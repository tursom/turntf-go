package turntf

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/tursom/turntf-go/internal/proto"
)

func newRelayID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("relay: rand.Read failed: %v", err))
	}
	return hex.EncodeToString(b[:])
}

// Relay 管理基于 Client 的 relay 连接，负责入站连接分发和出站连接创建。
type Relay struct {
	client *Client
	mu     sync.Mutex
	conns  map[string]*RelayConnection
	onConn func(*RelayConnection)
}

// Relay 返回 Client 关联的 Relay 管理器（懒初始化）。
func (c *Client) Relay() *Relay {
	c.relayOnce.Do(func() {
		c.relay = &Relay{
			client: c,
			conns:  make(map[string]*RelayConnection),
		}
	})
	return c.relay
}

// OnConnection 注册入站 relay 连接的处理器。每个新入站连接会调用 handler。
func (r *Relay) OnConnection(handler func(*RelayConnection)) {
	r.mu.Lock()
	r.onConn = handler
	r.mu.Unlock()
}

// Connect 向目标用户发起 relay 连接。自动解析目标用户的在线会话并选择支持瞬时消息的会话。
// config 为 nil 时使用 DefaultRelayConfig()。
func (r *Relay) Connect(ctx context.Context, target UserRef, config *RelayConfig) (*RelayConnection, error) {
	sessions, err := r.client.ResolveUserSessions(ctx, target)
	if err != nil {
		return nil, fmt.Errorf("relay: resolve sessions: %w", err)
	}

	var targetSession SessionRef
	for _, s := range sessions.Sessions {
		if s.TransientCapable {
			targetSession = s.Session
			break
		}
	}
	if targetSession.IsZero() {
		return nil, &RelayError{Code: RelayErrorNotConnected, Message: "no transient-capable session found for target user"}
	}

	cfg := DefaultRelayConfig()
	if config != nil {
		cfg = *config
	}

	relayID := newRelayID()
	conn := &RelayConnection{
		relay:         r,
		relayID:       relayID,
		state:         RelayStateOpening,
		config:        cfg,
		remotePeer:    target,
		remoteSession: targetSession,
		mySession:     r.client.loginInfo.SessionRef,
		sendCh:        make(chan relaySendItem, cfg.SendBufferSize/1024),
		recvCh:        make(chan []byte, 64),
		closeCh:       make(chan struct{}),
		openCh:        make(chan struct{}),
		flushCh:       make(chan struct{}),
		unacked:       make(map[uint64]unackedFrame),
		recvBuf:       make(map[uint64][]byte),
		expectedSeq:   1,
		sendBase:      1,
		nextSeq:       1,
	}
	conn.ctx, conn.cancel = context.WithCancel(context.Background())

	r.mu.Lock()
	r.conns[relayID] = conn
	r.mu.Unlock()

	openEnv := &RelayEnvelope{
		RelayID:       relayID,
		Kind:          RelayKindOpen,
		SenderSession: r.client.loginInfo.SessionRef,
		TargetSession: targetSession,
		SentAtMs:      time.Now().UnixMilli(),
	}
	if err := conn.sendRelayEnvelope(openEnv); err != nil {
		conn.abort(err)
		return nil, fmt.Errorf("relay: send OPEN: %w", err)
	}

	conn.wg.Add(1)
	go conn.sendLoop()

	openTimeout := time.Duration(cfg.OpenTimeoutMs) * time.Millisecond
	select {
	case <-conn.openCh:
		return conn, nil
	case <-conn.closeCh:
		return nil, conn.closeResult()
	case <-time.After(openTimeout):
		conn.abort(&RelayError{Code: RelayErrorOpenTimeout, Message: "OPEN timeout waiting for OPEN_ACK"})
		return nil, &RelayError{Code: RelayErrorOpenTimeout, Message: "OPEN timeout waiting for OPEN_ACK"}
	case <-ctx.Done():
		conn.abort(ctx.Err())
		return nil, ctx.Err()
	}
}

// acceptIncoming 将入站 OPEN 帧转换为新的 RelayConnection 并通知用户处理器。
func (r *Relay) acceptIncoming(env *RelayEnvelope, remotePeer UserRef) {
	cfg := DefaultRelayConfig()
	conn := &RelayConnection{
		relay:         r,
		relayID:       env.RelayID,
		state:         RelayStateOpen,
		config:        cfg,
		remotePeer:    remotePeer,
		remoteSession: env.SenderSession,
		mySession:     env.TargetSession,
		sendCh:        make(chan relaySendItem, cfg.SendBufferSize/1024),
		recvCh:        make(chan []byte, 64),
		closeCh:       make(chan struct{}),
		openCh:        make(chan struct{}),
		flushCh:       make(chan struct{}),
		unacked:       make(map[uint64]unackedFrame),
		recvBuf:       make(map[uint64][]byte),
		expectedSeq:   1,
		sendBase:      1,
		nextSeq:       1,
	}
	conn.ctx, conn.cancel = context.WithCancel(context.Background())
	close(conn.openCh)

	r.mu.Lock()
	existing, dup := r.conns[env.RelayID]
	if dup {
		r.mu.Unlock()
		if env.RelayID < existing.relayID {
			existing.abort(&RelayError{Code: RelayErrorDuplicateOpen, Message: "concurrent OPEN, keeping lower relay_id"})
		} else {
			conn.abort(&RelayError{Code: RelayErrorDuplicateOpen, Message: "concurrent OPEN, keeping lower relay_id"})
			return
		}
		r.mu.Lock()
	}
	r.conns[env.RelayID] = conn
	handler := r.onConn
	r.mu.Unlock()

	conn.wg.Add(1)
	go conn.sendLoop()

	openAckEnv := &RelayEnvelope{
		RelayID:       env.RelayID,
		Kind:          RelayKindOpenAck,
		SenderSession: conn.mySession,
		TargetSession: conn.remoteSession,
		SentAtMs:      time.Now().UnixMilli(),
	}
	go func() {
		_ = conn.sendRelayEnvelope(openAckEnv)
	}()

	if handler != nil {
		go handler(conn)
	}
}

// handlePacket 检查 packet body 是否为 relay 帧，是则分发到对应连接。
func (r *Relay) handlePacket(p Packet) bool {
	env, err := decodeRelayEnvelope(p.Body)
	if err != nil {
		return false
	}

	r.mu.Lock()
	conn, ok := r.conns[env.RelayID]
	r.mu.Unlock()

	switch env.Kind {
	case RelayKindOpen:
		if !ok {
			r.acceptIncoming(env, p.Sender)
		}
		return true

	default:
		if ok {
			conn.enqueueEnvelope(env)
		}
		return true
	}
}

// removeConnection 从管理器中移除连接。
func (r *Relay) removeConnection(relayID string) {
	r.mu.Lock()
	delete(r.conns, relayID)
	r.mu.Unlock()
}

func (r *Relay) closeAll(reason error) {
	r.mu.Lock()
	conns := make([]*RelayConnection, 0, len(r.conns))
	for _, conn := range r.conns {
		conns = append(conns, conn)
	}
	r.mu.Unlock()

	for _, conn := range conns {
		conn.closeConnection(reason, true)
	}
}

type unackedFrame struct {
	data       []byte
	retransmit int
}

// RelayConnection 表示一条 relay 点对点连接，提供可靠或尽力而为的数据传输。
type RelayConnection struct {
	relay     *Relay
	relayID   string
	enqueueMu cancelMutex
	mu        sync.Mutex
	state     RelayState
	config    RelayConfig

	remotePeer    UserRef
	remoteSession SessionRef
	mySession     SessionRef

	sendBase      uint64
	nextSeq       uint64
	unacked       map[uint64]unackedFrame
	expectedSeq   uint64
	recvBuf       map[uint64][]byte
	recvReady     int    // Frames removed from recvBuf but not yet delivered to recvCh.
	recvDelivered uint64 // Last cumulative receive ACK eligible after full batch delivery.
	retransCnt    int

	sendCh  chan relaySendItem
	recvCh  chan []byte
	closeCh chan struct{}
	openCh  chan struct{}
	flushCh chan struct{}
	ackCh   chan struct{} // Coalesced window-progress notification, guarded by mu.

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	onClose      []func(error)
	closeErr     error
	remoteClosed bool

	// Admission includes the frame currently blocked on delivery.
	inboxMu       sync.Mutex
	inbox         chan *RelayEnvelope
	inboxSeq      map[uint64]bool
	inboxData     int
	inboxFailed   bool
	inboxTerminal bool // ACK fast path stops at the first admitted CLOSE/ERROR.
	ackIdle       chan struct{}
	ackOut        chan struct{}
	ackPending    *RelayEnvelope
	ackGenerating int
	ackSending    bool
	ackOnce       sync.Once

	sendEnvelope func(*RelayEnvelope) error
}

// RelayID 返回连接的唯一标识。
func (c *RelayConnection) RelayID() string { return c.relayID }

// State 返回当前连接状态。
func (c *RelayConnection) State() RelayState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// RemotePeer 返回对端用户引用。
func (c *RelayConnection) RemotePeer() UserRef { return c.remotePeer }

// RemoteSession 返回对端会话引用。
func (c *RelayConnection) RemoteSession() SessionRef { return c.remoteSession }

// Send 发送数据。行为取决于配置的可靠性等级。
// 当发送窗口或缓冲区满时，若 SendTimeoutMs > 0 则最多等待该时长后返回错误。
func (c *RelayConnection) Send(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	c.enqueueMu.Lock()
	defer c.enqueueMu.Unlock()

	c.mu.Lock()
	state := c.state
	c.mu.Unlock()

	if state != RelayStateOpen {
		return &RelayError{Code: RelayErrorNotConnected, Message: "connection not open"}
	}

	owned := append([]byte(nil), data...)
	if c.config.SendTimeoutMs <= 0 {
		select {
		case c.sendCh <- relaySendItem{data: owned}:
			return nil
		case <-c.closeCh:
			return &RelayError{Code: RelayErrorClientClosed, Message: "connection closed"}
		case <-c.ctx.Done():
			return c.ctx.Err()
		}
	}

	timeout := time.Duration(c.config.SendTimeoutMs) * time.Millisecond
	select {
	case c.sendCh <- relaySendItem{data: owned}:
		return nil
	case <-time.After(timeout):
		return &RelayError{Code: RelayErrorSendTimeout, Message: "send timeout waiting for buffer space"}
	case <-c.closeCh:
		return &RelayError{Code: RelayErrorClientClosed, Message: "connection closed"}
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
}

// Receive 返回接收通道。从通道读取对端发送的数据。
func (c *RelayConnection) Receive() <-chan []byte { return c.recvCh }

// ReceiveTimeout 从连接读取数据，支持超时。timeout 为 0 时无限等待。
// 正常远端 CLOSE 后先排空已接受的数据；Abort 和其他错误不启用排空。
func (c *RelayConnection) ReceiveTimeout(timeout time.Duration) ([]byte, error) {
	if timeout <= 0 {
		select {
		case data, ok := <-c.recvCh:
			if !ok {
				return nil, &RelayError{Code: RelayErrorClientClosed, Message: "connection closed"}
			}
			return data, nil
		case <-c.closeCh:
			return c.receiveTerminal(&RelayError{Code: RelayErrorClientClosed, Message: "connection closed"})
		case <-c.ctx.Done():
			return c.receiveTerminal(c.ctx.Err())
		}
	}

	select {
	case data, ok := <-c.recvCh:
		if !ok {
			return nil, &RelayError{Code: RelayErrorClientClosed, Message: "connection closed"}
		}
		return data, nil
	case <-time.After(timeout):
		return nil, &RelayError{Code: RelayErrorReceiveTimeout, Message: "receive timeout"}
	case <-c.closeCh:
		return c.receiveTerminal(&RelayError{Code: RelayErrorClientClosed, Message: "connection closed"})
	case <-c.ctx.Done():
		return c.receiveTerminal(c.ctx.Err())
	}
}

func (c *RelayConnection) receiveTerminal(fallback error) ([]byte, error) {
	c.mu.Lock()
	remoteClosed := c.remoteClosed
	cause := c.closeErr
	c.mu.Unlock()
	// FIFO dispatch processes remote CLOSE only after publishing its DATA
	// prefix. No producer can add later DATA, so a nonblocking drain suffices.
	if remoteClosed {
		select {
		case data, ok := <-c.recvCh:
			if ok {
				return data, nil
			}
		default:
		}
	}
	if cause != nil {
		return nil, cause
	}
	return nil, fallback
}

// OnClose 注册连接关闭回调。
func (c *RelayConnection) OnClose(fn func(error)) {
	if fn == nil {
		return
	}
	c.mu.Lock()
	if c.state == RelayStateClosed {
		reason := c.closeErr
		c.mu.Unlock()
		fn(reason)
		return
	}
	c.onClose = append(c.onClose, fn)
	c.mu.Unlock()
}

// Close 优雅关闭连接，发送 CLOSE 帧并等待确认。
func (c *RelayConnection) Close() error {
	timeout := time.Duration(c.config.CloseTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	timeoutError := &RelayError{Code: RelayErrorCloseTimeout, Message: "close timeout waiting for queued data acknowledgement"}
	// The same deadline releases a Send holding enqueueMu and cancels ACK/CLOSE
	// acceptance RPCs. User callbacks cannot extend the deadline.
	deadline := time.AfterFunc(timeout, func() { c.closeConnection(timeoutError, true) })
	defer deadline.Stop()
	c.enqueueMu.Lock()
	c.mu.Lock()
	if c.state != RelayStateOpen {
		err := c.closeErr
		c.mu.Unlock()
		c.enqueueMu.Unlock()
		return err
	}
	c.state = RelayStateClosing
	c.mu.Unlock()

	select {
	case c.sendCh <- relaySendItem{}:
		c.enqueueMu.Unlock()
	case <-c.closeCh:
		c.enqueueMu.Unlock()
		return c.closeResult()
	case <-c.ctx.Done():
		c.enqueueMu.Unlock()
		return c.closeResult()
	}
	select {
	case <-c.flushCh:
	case <-c.closeCh:
		return c.closeResult()
	case <-c.ctx.Done():
		return c.closeResult()
	}

	if c.config.Reliability != ReliabilityBestEffort {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			c.mu.Lock()
			pending := len(c.unacked)
			c.mu.Unlock()
			if pending == 0 {
				break
			}
			select {
			case <-ticker.C:
			case <-c.closeCh:
				return c.closeResult()
			case <-c.ctx.Done():
				return c.closeResult()
			}
		}
	}

	c.waitReceiveACK()
	if c.ctx.Err() != nil {
		return c.closeResult()
	}
	closeEnv := &RelayEnvelope{
		RelayID:       c.relayID,
		Kind:          RelayKindClose,
		SenderSession: c.mySession,
		TargetSession: c.remoteSession,
		SentAtMs:      time.Now().UnixMilli(),
	}
	// The shared WS write cannot be interrupted by a Relay deadline. Keep this
	// single terminal RPC bounded by the client write budget without making
	// the caller wait beyond CloseTimeout. The buffered result cannot strand
	// the worker after the caller leaves on cancellation.
	closeSent := make(chan error, 1)
	c.mu.Lock()
	if c.state == RelayStateClosed {
		err := c.closeErr
		c.mu.Unlock()
		return err
	}
	c.wg.Add(1)
	c.mu.Unlock()
	go func() {
		defer c.wg.Done()
		closeSent <- c.sendRelayEnvelope(closeEnv)
	}()
	select {
	case err := <-closeSent:
		c.closeConnection(err, true)
	case <-c.closeCh:
	case <-c.ctx.Done():
	}
	return c.closeResult()
}

// Preserve the first failure instead of replacing a failed acceptance with the
// cancellation used internally to release the other pending operations.
func (c *RelayConnection) closeResult() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeErr
}

// Abort 强制关闭连接，不等待确认。
func (c *RelayConnection) Abort(reason error) {
	c.abort(reason)
}

func (c *RelayConnection) abort(reason error) {
	c.handleClose(reason)
}

func (c *RelayConnection) handleClose(reason error) {
	c.closeConnection(reason, false)
}

func (c *RelayConnection) closeConnection(reason error, asyncCallbacks bool) {
	c.closeWithSource(reason, asyncCallbacks, false)
}

func (c *RelayConnection) closeWithSource(reason error, asyncCallbacks, remoteClose bool) {
	c.mu.Lock()
	if c.state == RelayStateClosed {
		c.mu.Unlock()
		return
	}
	c.state = RelayStateClosed
	c.closeErr = reason
	c.remoteClosed = remoteClose
	callbacks := make([]func(error), len(c.onClose))
	copy(callbacks, c.onClose)
	c.cancel()
	close(c.closeCh)
	c.mu.Unlock()

	c.relay.removeConnection(c.relayID)
	notify := func() {
		for _, fn := range callbacks {
			fn(reason)
		}
	}
	if asyncCallbacks && len(callbacks) > 0 {
		go notify()
	} else {
		notify()
	}
}

func (c *RelayConnection) sendRelayEnvelope(env *RelayEnvelope) error {
	err := c.sendRelayEnvelopeOnce(env)
	// Reliable DATA stays in unacked until the peer ACKs it. A temporary
	// rejection must use the existing bounded retransmission loop, not tear
	// down the stream. Flush still waits for the end-to-end ACK.
	if env.Kind == RelayKindData && c.retryableSendError(err) {
		return nil
	}
	return err
}

func (c *RelayConnection) sendRelayEnvelopeOnce(env *RelayEnvelope) error {
	if c.sendEnvelope != nil {
		return c.sendEnvelope(env)
	}
	body, err := encodeRelayEnvelope(env)
	if err != nil {
		return err
	}

	mode := DeliveryModeBestEffort
	if c.config.DeliveryMode != "" {
		mode = c.config.DeliveryMode
	}

	ctx := c.ctx
	if timeout := c.relay.client.cfg.RequestTimeout; timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	_, err = c.relay.client.SendPacket(ctx, SendPacketInput{
		Target:        c.remotePeer,
		Body:          body,
		DeliveryMode:  mode,
		TargetSession: c.remoteSession,
	})
	return err
}

func (c *RelayConnection) handleEnvelope(env *RelayEnvelope) {
	switch env.Kind {
	case RelayKindOpenAck:
		c.mu.Lock()
		if c.state == RelayStateOpening {
			c.state = RelayStateOpen
			c.remoteSession = env.SenderSession
			close(c.openCh)
		}
		c.mu.Unlock()
	case RelayKindClose:
		c.closeWithSource(&RelayError{Code: RelayErrorRemoteClose, Message: "remote peer closed connection"}, false, true)
	case RelayKindError:
		c.handleClose(&RelayError{Code: RelayErrorProtocol, Message: "remote peer error: " + string(env.Payload)})
	case RelayKindData:
		c.handleData(env)
	case RelayKindAck:
		c.handleAck(env)
	case RelayKindPing:
		c.handlePing(env)
	}
}

func (c *RelayConnection) handleData(env *RelayEnvelope) {
	switch c.config.Reliability {
	case ReliabilityBestEffort:
		select {
		case c.recvCh <- env.Payload:
		case <-c.closeCh:
		}

	case ReliabilityAtLeastOnce:
		ackEnv := &RelayEnvelope{
			RelayID:       c.relayID,
			Kind:          RelayKindAck,
			SenderSession: c.mySession,
			TargetSession: c.remoteSession,
			AckSeq:        env.Seq,
			SentAtMs:      time.Now().UnixMilli(),
		}
		if !c.deliverOrdered(env.Payload) {
			return
		}
		c.queueACK(ackEnv)

	case ReliabilityReliableOrdered:
		var ready [][]byte
		c.mu.Lock()
		if env.Seq == c.expectedSeq {
			ready = append(ready, env.Payload)
			c.expectedSeq++
			for {
				if data, ok := c.recvBuf[c.expectedSeq]; ok {
					ready = append(ready, data)
					delete(c.recvBuf, c.expectedSeq)
					c.expectedSeq++
				} else {
					break
				}
			}
		} else if env.Seq > c.expectedSeq {
			if env.Seq-c.expectedSeq >= uint64(c.config.WindowSize) {
				// The peer's send window is not negotiated with our reorder window.
				// Leave this frame unacknowledged so a later retransmit can recover it.
				c.mu.Unlock()
				return
			}
			c.recvBuf[env.Seq] = env.Payload
		}
		c.recvReady += len(ready)
		ackSeq := c.expectedSeq - 1
		c.mu.Unlock()

		for _, data := range ready {
			if !c.deliverOrdered(data) {
				return
			}
			c.mu.Lock()
			c.recvReady--
			c.mu.Unlock()
		}
		c.mu.Lock()
		c.recvDelivered = ackSeq
		c.mu.Unlock()
		if ackSeq == 0 {
			return
		}

		ackEnv := &RelayEnvelope{
			RelayID:       c.relayID,
			Kind:          RelayKindAck,
			SenderSession: c.mySession,
			TargetSession: c.remoteSession,
			AckSeq:        ackSeq,
			SentAtMs:      time.Now().UnixMilli(),
		}
		c.queueACK(ackEnv)
	}
}

func (c *RelayConnection) deliverOrdered(data []byte) bool {
	select {
	case c.recvCh <- data:
		return true
	case <-c.closeCh:
		return false
	case <-c.ctx.Done():
		return false
	}
}

func (c *RelayConnection) handleAck(env *RelayEnvelope) {
	if c.config.Reliability == ReliabilityBestEffort {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if env.AckSeq >= c.sendBase && env.AckSeq < c.nextSeq {
		for seq := range c.unacked {
			if seq <= env.AckSeq {
				delete(c.unacked, seq)
			}
		}
		c.sendBase = env.AckSeq + 1
		c.retransCnt = 0
		select {
		case c.ackCh <- struct{}{}:
		default:
		}
	}
}

func (c *RelayConnection) handlePing(env *RelayEnvelope) {
	errEnv := &RelayEnvelope{
		RelayID:       c.relayID,
		Kind:          RelayKindError,
		SenderSession: c.mySession,
		TargetSession: c.remoteSession,
		Payload:       nil,
		SentAtMs:      time.Now().UnixMilli(),
	}
	_ = c.sendRelayEnvelope(errEnv)
}

func (c *RelayConnection) sendLoop() {
	defer c.wg.Done()

	c.mu.Lock()
	c.ackCh = make(chan struct{}, 1)
	c.mu.Unlock()

	var sends sync.WaitGroup
	defer sends.Wait()
	limit := c.config.WindowSize
	if limit < 1 {
		limit = 1
	}
	if limit > 16 {
		limit = 16
	}
	inflight := make(chan struct{}, limit)

	var ackTimer *time.Timer
	var ackTimerC <-chan time.Time
	if c.config.Reliability != ReliabilityBestEffort {
		ackTimer = time.NewTimer(time.Duration(c.config.AckTimeoutMs) * time.Millisecond)
		ackTimer.Stop()
		ackTimerC = ackTimer.C
	}
	defer func() {
		if ackTimer != nil {
			ackTimer.Stop()
		}
	}()

	retry := func() {
		// Retransmissions share the DATA acceptance budget, not an extra RPC.
		select {
		case inflight <- struct{}{}:
		case <-c.ctx.Done():
			return
		}
		defer func() { <-inflight }()
		c.retransmit()
		c.mu.Lock()
		pending := len(c.unacked) > 0 && c.state != RelayStateClosed
		c.mu.Unlock()
		if pending && ackTimer != nil {
			ackTimer.Reset(time.Duration(c.config.AckTimeoutMs) * time.Millisecond)
		}
	}

	for {
		select {
		case item, ok := <-c.sendCh:
			if !ok {
				return
			}
			if item.barrier != nil {
				sends.Wait()
				c.mu.Lock()
				target := c.nextSeq
				c.mu.Unlock()
				item.barrier <- target
				continue
			}
			data := item.data
			if data == nil {
				sends.Wait()
				close(c.flushCh)
				continue
			}
			c.mu.Lock()
			if c.config.Reliability == ReliabilityBestEffort {
				c.mu.Unlock()
				env := &RelayEnvelope{
					RelayID:       c.relayID,
					Kind:          RelayKindData,
					SenderSession: c.mySession,
					TargetSession: c.remoteSession,
					Payload:       data,
					SentAtMs:      time.Now().UnixMilli(),
				}
				if err := c.sendRelayEnvelope(env); err != nil {
					c.handleClose(err)
				}
				continue
			}

			for c.nextSeq-c.sendBase >= uint64(c.config.WindowSize) {
				c.mu.Unlock()
				select {
				case <-c.closeCh:
					return
				case <-c.ctx.Done():
					return
				case <-c.ackCh:
				case <-ackTimerC:
					retry()
				}
				c.mu.Lock()
			}

			seq := c.nextSeq
			c.nextSeq++
			c.unacked[seq] = unackedFrame{data: data}
			if c.sendBase == 0 {
				c.sendBase = seq
			}
			if ackTimer != nil && c.sendBase > 0 && len(c.unacked) > 0 {
				ackTimer.Reset(time.Duration(c.config.AckTimeoutMs) * time.Millisecond)
			}
			c.mu.Unlock()

			env := &RelayEnvelope{
				RelayID:       c.relayID,
				Kind:          RelayKindData,
				SenderSession: c.mySession,
				TargetSession: c.remoteSession,
				Seq:           seq,
				Payload:       data,
				SentAtMs:      time.Now().UnixMilli(),
			}
			if c.config.Reliability != ReliabilityReliableOrdered {
				if err := c.sendRelayEnvelope(env); err != nil {
					c.handleClose(err)
				}
				continue
			}
			// Bound outstanding acceptance RPCs separately from the Relay ACK
			// window: an ACK may arrive before its acceptance response. Ordered
			// delivery uses Seq, so concurrent RPC writes may safely reorder.
			select {
			case inflight <- struct{}{}:
			case <-c.ctx.Done():
				return
			}
			sends.Add(1)
			go func() {
				defer sends.Done()
				defer func() { <-inflight }()
				if err := c.sendRelayEnvelope(env); err != nil {
					c.handleClose(err)
				}
			}()

		case <-ackTimerC:
			retry()

		case <-c.closeCh:
			return

		case <-c.ctx.Done():
			return
		}
	}
}

func (c *RelayConnection) retransmit() {
	c.mu.Lock()
	if len(c.unacked) == 0 {
		c.mu.Unlock()
		return
	}

	c.retransCnt++
	if c.retransCnt > c.config.MaxRetransmits {
		c.mu.Unlock()
		c.handleClose(&RelayError{Code: RelayErrorMaxRetransmit, Message: "max retransmits exceeded"})
		return
	}

	// ACK dispatch on the shared readLoop also needs mu. Never hold it while
	// waiting for a SendPacket response from that same readLoop. Send owns
	// payload storage, so snapshots remain valid after cumulative ACK removal.
	frames := make([]*RelayEnvelope, 0, len(c.unacked))
	for seq := c.sendBase; seq < c.nextSeq; seq++ {
		frame, ok := c.unacked[seq]
		if !ok {
			continue
		}
		frames = append(frames, &RelayEnvelope{
			RelayID:       c.relayID,
			Kind:          RelayKindData,
			SenderSession: c.mySession,
			TargetSession: c.remoteSession,
			Seq:           seq,
			Payload:       frame.data,
			SentAtMs:      time.Now().UnixMilli(),
		})
	}
	c.mu.Unlock()
	for _, env := range frames {
		if err := c.sendRelayEnvelope(env); err != nil {
			c.handleClose(err)
			return
		}
	}
}

func encodeRelayEnvelope(env *RelayEnvelope) ([]byte, error) {
	pbEnv := &pb.RelayEnvelope{
		RelayId: env.RelayID,
		Kind:    relayKindToProto(env.Kind),
		SenderSession: &pb.SessionRef{
			ServingNodeId: env.SenderSession.ServingNodeID,
			SessionId:     env.SenderSession.SessionID,
		},
		TargetSession: &pb.SessionRef{
			ServingNodeId: env.TargetSession.ServingNodeID,
			SessionId:     env.TargetSession.SessionID,
		},
		Seq:      env.Seq,
		AckSeq:   env.AckSeq,
		Payload:  env.Payload,
		SentAtMs: env.SentAtMs,
	}
	return proto.Marshal(pbEnv)
}

func decodeRelayEnvelope(data []byte) (*RelayEnvelope, error) {
	var pbEnv pb.RelayEnvelope
	if err := proto.Unmarshal(data, &pbEnv); err != nil {
		return nil, err
	}
	return &RelayEnvelope{
		RelayID: pbEnv.RelayId,
		Kind:    relayKindFromProto(pbEnv.Kind),
		SenderSession: SessionRef{
			ServingNodeID: pbEnv.SenderSession.GetServingNodeId(),
			SessionID:     pbEnv.SenderSession.GetSessionId(),
		},
		TargetSession: SessionRef{
			ServingNodeID: pbEnv.TargetSession.GetServingNodeId(),
			SessionID:     pbEnv.TargetSession.GetSessionId(),
		},
		Seq:      pbEnv.Seq,
		AckSeq:   pbEnv.AckSeq,
		Payload:  pbEnv.Payload,
		SentAtMs: pbEnv.SentAtMs,
	}, nil
}

func relayKindToProto(k RelayKind) pb.RelayKind {
	switch k {
	case RelayKindOpen:
		return pb.RelayKind_RELAY_KIND_OPEN
	case RelayKindOpenAck:
		return pb.RelayKind_RELAY_KIND_OPEN_ACK
	case RelayKindData:
		return pb.RelayKind_RELAY_KIND_DATA
	case RelayKindAck:
		return pb.RelayKind_RELAY_KIND_ACK
	case RelayKindClose:
		return pb.RelayKind_RELAY_KIND_CLOSE
	case RelayKindPing:
		return pb.RelayKind_RELAY_KIND_PING
	case RelayKindError:
		return pb.RelayKind_RELAY_KIND_ERROR
	default:
		return pb.RelayKind_RELAY_KIND_UNSPECIFIED
	}
}

func relayKindFromProto(k pb.RelayKind) RelayKind {
	switch k {
	case pb.RelayKind_RELAY_KIND_OPEN:
		return RelayKindOpen
	case pb.RelayKind_RELAY_KIND_OPEN_ACK:
		return RelayKindOpenAck
	case pb.RelayKind_RELAY_KIND_DATA:
		return RelayKindData
	case pb.RelayKind_RELAY_KIND_ACK:
		return RelayKindAck
	case pb.RelayKind_RELAY_KIND_CLOSE:
		return RelayKindClose
	case pb.RelayKind_RELAY_KIND_PING:
		return RelayKindPing
	case pb.RelayKind_RELAY_KIND_ERROR:
		return RelayKindError
	default:
		return RelayKindUnspecified
	}
}

var errRelayIgnored = errors.New("relay: ignored")

func isRelayIgnored(err error) bool {
	return errors.Is(err, errRelayIgnored)
}
