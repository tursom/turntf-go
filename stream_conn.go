package turntf

import (
	"context"
	"errors"
	"io"
	"sync"
)

var (
	ErrStreamClosed  = errors.New("stream is closed")
	ErrStreamUnknown = errors.New("stream is not registered")
)

// StreamFrameSendFunc sends one frame over an application-provided transport.
// It may synchronously deliver frames back to StreamMux.HandleFrame.
type StreamFrameSendFunc func(context.Context, StreamFrame) error

// StreamMuxConfig controls the bounded resources used by a StreamMux.
// Window bounds each direction's unread or unacknowledged bytes.
type StreamMuxConfig struct {
	Window        uint64
	MaxFrameSize  int
	AcceptBacklog int
}

func (c StreamMuxConfig) normalized() (StreamMuxConfig, error) {
	if c.Window == 0 {
		c.Window = DefaultStreamWindow
	}
	if c.MaxFrameSize == 0 {
		c.MaxFrameSize = DefaultStreamMaxFrame
	}
	if c.MaxFrameSize < 1 || c.MaxFrameSize > DefaultStreamMaxFrame {
		return StreamMuxConfig{}, errors.New("stream max frame size is out of range")
	}
	if c.Window > uint64(maxInt()) {
		return StreamMuxConfig{}, errors.New("stream window is too large")
	}
	if c.AcceptBacklog == 0 {
		c.AcceptBacklog = 16
	}
	if c.AcceptBacklog < 1 {
		return StreamMuxConfig{}, errors.New("stream accept backlog must be positive")
	}
	return c, nil
}

// StreamMux turns an asynchronous stream-frame transport into dialed and
// accepted byte streams. HandleFrame may be called concurrently.
type StreamMux struct {
	send StreamFrameSendFunc
	cfg  StreamMuxConfig

	mu      sync.Mutex
	streams map[StreamID]*StreamConn
	accept  chan *StreamConn
	closed  chan struct{}
	once    sync.Once
}

func NewStreamMux(send StreamFrameSendFunc, cfg *StreamMuxConfig) (*StreamMux, error) {
	if send == nil {
		return nil, errors.New("stream frame sender is required")
	}
	var value StreamMuxConfig
	if cfg != nil {
		value = *cfg
	}
	normalized, err := value.normalized()
	if err != nil {
		return nil, err
	}
	return &StreamMux{
		send:    send,
		cfg:     normalized,
		streams: make(map[StreamID]*StreamConn),
		accept:  make(chan *StreamConn, normalized.AcceptBacklog),
		closed:  make(chan struct{}),
	}, nil
}

// Dial creates a stream ID, sends Open, and waits for OpenAck.
func (m *StreamMux) Dial(ctx context.Context) (*StreamConn, error) {
	id, err := NewStreamID()
	if err != nil {
		return nil, err
	}
	return m.DialID(ctx, id)
}

// DialID is Dial with a caller-supplied ID, primarily for persisted identities
// and deterministic tests. The ID must not already be registered.
func (m *StreamMux) DialID(ctx context.Context, id StreamID) (*StreamConn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	conn := newStreamConn(m, id, 1, m.cfg.Window, false)
	m.mu.Lock()
	select {
	case <-m.closed:
		m.mu.Unlock()
		return nil, ErrStreamClosed
	default:
	}
	if _, exists := m.streams[id]; exists {
		m.mu.Unlock()
		return nil, errors.New("stream ID is already registered")
	}
	m.streams[id] = conn
	m.mu.Unlock()

	open := StreamFrame{Kind: StreamFrameOpen, ID: id, Epoch: 1, Window: m.cfg.Window}
	if err := m.send(ctx, open); err != nil {
		conn.terminate(err)
		return nil, err
	}
	select {
	case <-conn.opened:
		return conn, nil
	case <-conn.done:
		return nil, conn.closeError()
	case <-ctx.Done():
		conn.terminate(ctx.Err())
		return nil, ctx.Err()
	}
}

// Accept waits for the next remotely opened stream.
func (m *StreamMux) Accept(ctx context.Context) (*StreamConn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		select {
		case conn := <-m.accept:
			if !conn.isClosed() {
				return conn, nil
			}
		case <-m.closed:
			return nil, ErrStreamClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// HandleFrame supplies one received frame to the mux.
func (m *StreamMux) HandleFrame(ctx context.Context, frame StreamFrame) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if frame.Kind < StreamFrameOpen || frame.Kind > StreamFrameClose {
		return errors.New("invalid stream frame kind")
	}
	if len(frame.Payload) > m.cfg.MaxFrameSize {
		return errors.New("stream frame payload is too large")
	}
	if frame.Kind != StreamFrameData && len(frame.Payload) != 0 {
		return errors.New("stream control frame contains payload")
	}
	if frame.Kind == StreamFrameData && len(frame.Payload) == 0 {
		return errors.New("stream data frame is empty")
	}
	if frame.Kind == StreamFrameOpen {
		return m.handleOpen(ctx, frame)
	}
	m.mu.Lock()
	conn := m.streams[frame.ID]
	m.mu.Unlock()
	if conn == nil {
		if frame.Kind == StreamFrameClose {
			return nil
		}
		return ErrStreamUnknown
	}
	return conn.handleFrame(ctx, frame)
}

func (m *StreamMux) handleOpen(ctx context.Context, frame StreamFrame) error {
	if frame.Epoch == 0 || len(frame.Payload) != 0 {
		return errors.New("invalid stream open frame")
	}
	window := frame.Window
	if window == 0 {
		window = DefaultStreamWindow
	}
	if window > uint64(maxInt()) {
		return ErrStreamCredit
	}

	m.mu.Lock()
	select {
	case <-m.closed:
		m.mu.Unlock()
		return ErrStreamClosed
	default:
	}
	conn := m.streams[frame.ID]
	created := false
	if conn == nil {
		conn = newStreamConn(m, frame.ID, frame.Epoch, window, true)
		m.streams[frame.ID] = conn
		created = true
	}
	m.mu.Unlock()

	ack := StreamFrame{Kind: StreamFrameOpenAck, ID: frame.ID, Epoch: frame.Epoch, Window: window}
	if err := m.send(ctx, ack); err != nil {
		if created {
			conn.terminate(err)
		}
		return err
	}
	if !created {
		return nil
	}
	select {
	case m.accept <- conn:
		return nil
	default:
		conn.terminate(errors.New("stream accept backlog is full"))
		_ = m.send(ctx, StreamFrame{Kind: StreamFrameClose, ID: frame.ID, Epoch: frame.Epoch})
		return errors.New("stream accept backlog is full")
	}
}

func (m *StreamMux) remove(id StreamID, conn *StreamConn) {
	m.mu.Lock()
	if m.streams[id] == conn {
		delete(m.streams, id)
	}
	m.mu.Unlock()
}

// Close terminates all streams and rejects future Dial, Accept, and HandleFrame calls.
func (m *StreamMux) Close() error {
	m.once.Do(func() {
		close(m.closed)
		m.mu.Lock()
		streams := make([]*StreamConn, 0, len(m.streams))
		for _, conn := range m.streams {
			streams = append(streams, conn)
		}
		m.streams = make(map[StreamID]*StreamConn)
		m.mu.Unlock()
		for _, conn := range streams {
			conn.terminate(ErrStreamClosed)
		}
	})
	return nil
}

// StreamConn is a concurrent, resumable byte stream. Its send and receive
// directions have independent epoch, offset, window, and buffering state.
type StreamConn struct {
	mux      *StreamMux
	id       StreamID
	sender   *StreamSenderState
	receiver *StreamReceiverState

	writeMu    sync.Mutex
	handleMu   sync.Mutex
	responseMu sync.Mutex
	mu         sync.Mutex
	readBuf    []byte
	window     uint64
	sendWin    uint64
	opened     chan struct{}
	openOnce   sync.Once
	done       chan struct{}
	doneOnce   sync.Once
	state      chan struct{}
	closeErr   error

	resumeEpoch uint64
	resumeReady chan struct{}
}

func newStreamConn(mux *StreamMux, id StreamID, epoch, window uint64, open bool) *StreamConn {
	conn := &StreamConn{
		mux:      mux,
		id:       id,
		sender:   NewStreamSenderState(id, epoch, window),
		receiver: NewStreamReceiverState(id, epoch, window),
		window:   window,
		sendWin:  window,
		opened:   make(chan struct{}),
		done:     make(chan struct{}),
		state:    make(chan struct{}),
	}
	if open {
		conn.markOpen()
	}
	return conn
}

func (c *StreamConn) ID() StreamID { return c.id }

// Read returns bytes accepted from the peer. Reading releases receive-window
// credit; a transport error while sending that update may accompany n > 0.
func (c *StreamConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		c.mu.Lock()
		if len(c.readBuf) != 0 {
			c.mu.Unlock()

			// Serialize consumption and its credit snapshot with Data/Resume.
			// Otherwise an arriving Data frame could pair a newer offset with
			// stale free-space credit and overrun the bounded read buffer.
			c.handleMu.Lock()
			c.mu.Lock()
			if len(c.readBuf) == 0 {
				c.mu.Unlock()
				c.handleMu.Unlock()
				continue
			}
			n := copy(p, c.readBuf)
			copy(c.readBuf, c.readBuf[n:])
			c.readBuf = c.readBuf[:len(c.readBuf)-n]
			free := c.window - uint64(len(c.readBuf))
			closed := c.closeErr != nil
			epoch, offset, _ := c.receiver.snapshot()
			c.mu.Unlock()
			if closed {
				c.handleMu.Unlock()
				return n, nil
			}
			c.responseMu.Lock()
			c.handleMu.Unlock()
			err := c.mux.send(context.Background(), StreamFrame{Kind: StreamFrameAck, ID: c.id, Epoch: epoch, Offset: offset, Window: free})
			c.responseMu.Unlock()
			if errors.Is(err, ErrStreamClosed) || errors.Is(err, ErrStreamUnknown) {
				err = nil
			}
			return n, err
		}
		if c.closeErr != nil {
			err := c.closeErr
			c.mu.Unlock()
			if errors.Is(err, io.EOF) {
				return 0, io.EOF
			}
			return 0, err
		}
		state := c.state
		c.mu.Unlock()
		<-state
	}
}

// Write splits p into frames and waits for peer-advertised credit. If the
// transport callback fails, bytes included in n remain pending for Resume and
// must not be written again.
func (c *StreamConn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	written := 0
	for written < len(p) {
		available, err := c.waitSendCredit()
		if err != nil {
			return written, err
		}
		chunk := len(p) - written
		if chunk > c.mux.cfg.MaxFrameSize {
			chunk = c.mux.cfg.MaxFrameSize
		}
		if uint64(chunk) > available {
			chunk = int(available)
		}
		frame, err := c.sender.Data(p[written : written+chunk])
		if err != nil {
			return written, err
		}
		written += chunk
		if err := c.mux.send(context.Background(), frame); err != nil {
			// Data is retained by sender state and Resume will retransmit it.
			return written, err
		}
	}
	return written, nil
}

func (c *StreamConn) waitSendCredit() (uint64, error) {
	for {
		_, next, acked, _ := c.sender.snapshot()
		c.mu.Lock()
		if c.closeErr != nil {
			err := c.closeErr
			c.mu.Unlock()
			return 0, err
		}
		outstanding := next - acked
		if c.sendWin > outstanding {
			available := c.sendWin - outstanding
			c.mu.Unlock()
			return available, nil
		}
		state := c.state
		c.mu.Unlock()
		<-state
	}
}

// Resume advances only the sending direction to a new epoch, waits for its
// Ack, and retransmits the unacknowledged suffix.
func (c *StreamConn) Resume(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	epoch, _, acked, _ := c.sender.snapshot()
	frames, err := c.sender.Resume(epoch+1, acked)
	if err != nil {
		return err
	}
	ready := make(chan struct{})
	c.mu.Lock()
	if c.closeErr != nil {
		err := c.closeErr
		c.mu.Unlock()
		return err
	}
	c.resumeEpoch = epoch + 1
	c.resumeReady = ready
	c.mu.Unlock()

	if err := c.mux.send(ctx, StreamFrame{Kind: StreamFrameResume, ID: c.id, Epoch: epoch + 1, Offset: acked}); err != nil {
		c.clearResume(ready)
		return err
	}
	select {
	case <-ready:
	case <-c.done:
		return c.closeError()
	case <-ctx.Done():
		c.clearResume(ready)
		return ctx.Err()
	}
	for _, frame := range frames {
		if err := c.mux.send(ctx, frame); err != nil {
			return err
		}
	}
	return nil
}

func (c *StreamConn) clearResume(ready chan struct{}) {
	c.mu.Lock()
	if c.resumeReady == ready {
		c.resumeReady = nil
		c.resumeEpoch = 0
	}
	c.mu.Unlock()
}

// Close locally terminates the stream and best-effort sends Close to the peer.
func (c *StreamConn) Close() error {
	if c.isClosed() {
		return nil
	}
	epoch, _, _, _ := c.sender.snapshot()
	c.terminate(ErrStreamClosed)
	err := c.mux.send(context.Background(), StreamFrame{Kind: StreamFrameClose, ID: c.id, Epoch: epoch})
	if errors.Is(err, ErrStreamClosed) || errors.Is(err, ErrStreamUnknown) {
		return nil
	}
	return err
}

func (c *StreamConn) handleFrame(ctx context.Context, frame StreamFrame) error {
	c.handleMu.Lock()

	switch frame.Kind {
	case StreamFrameOpenAck:
		if err := c.sender.Acknowledge(frame.Epoch, 0, frame.Window); err != nil {
			c.handleMu.Unlock()
			return err
		}
		c.mu.Lock()
		c.sendWin = frame.Window
		c.signalLocked()
		c.mu.Unlock()
		c.markOpen()
		c.handleMu.Unlock()
		return nil
	case StreamFrameData:
		ack, err := c.handleData(frame)
		if err != nil {
			c.handleMu.Unlock()
			return err
		}
		return c.sendResponseLocked(ctx, ack)
	case StreamFrameAck:
		if err := c.sender.Acknowledge(frame.Epoch, frame.Offset, frame.Window); err != nil {
			c.handleMu.Unlock()
			return err
		}
		c.mu.Lock()
		c.sendWin = frame.Window
		if c.resumeReady != nil && c.resumeEpoch == frame.Epoch {
			close(c.resumeReady)
			c.resumeReady = nil
			c.resumeEpoch = 0
		}
		c.signalLocked()
		c.mu.Unlock()
		c.handleMu.Unlock()
		return nil
	case StreamFrameResume:
		ack, err := c.receiver.Resume(frame.Epoch, frame.Offset)
		if err != nil {
			c.handleMu.Unlock()
			return err
		}
		c.mu.Lock()
		ack.Window = c.window - uint64(len(c.readBuf))
		c.mu.Unlock()
		return c.sendResponseLocked(ctx, ack)
	case StreamFrameClose:
		c.terminate(io.EOF)
		c.handleMu.Unlock()
		return nil
	default:
		c.handleMu.Unlock()
		return errors.New("unexpected stream frame")
	}
}

// sendResponseLocked preserves cumulative ACK order while ensuring an
// application callback is never invoked with the receive-state lock held.
func (c *StreamConn) sendResponseLocked(ctx context.Context, frame StreamFrame) error {
	c.responseMu.Lock()
	c.handleMu.Unlock()
	err := c.mux.send(ctx, frame)
	c.responseMu.Unlock()
	return err
}

func (c *StreamConn) handleData(frame StreamFrame) (StreamFrame, error) {
	_, offset, _ := c.receiver.snapshot()
	if frame.Offset > offset {
		return StreamFrame{}, ErrStreamGap
	}
	start := offset - frame.Offset
	if start > uint64(len(frame.Payload)) {
		start = uint64(len(frame.Payload))
	}
	unique := uint64(len(frame.Payload)) - start

	c.mu.Lock()
	if c.closeErr != nil {
		err := c.closeErr
		c.mu.Unlock()
		return StreamFrame{}, err
	}
	if uint64(len(c.readBuf))+unique > c.window {
		c.mu.Unlock()
		return StreamFrame{}, ErrStreamCredit
	}
	c.mu.Unlock()

	payload, ack, err := c.receiver.Accept(frame)
	if err != nil {
		return StreamFrame{}, err
	}
	c.mu.Lock()
	c.readBuf = append(c.readBuf, payload...)
	ack.Window = c.window - uint64(len(c.readBuf))
	c.signalLocked()
	c.mu.Unlock()
	return ack, nil
}

func (c *StreamConn) markOpen() {
	c.openOnce.Do(func() { close(c.opened) })
}

func (c *StreamConn) terminate(err error) {
	if err == nil {
		err = ErrStreamClosed
	}
	c.doneOnce.Do(func() {
		c.mu.Lock()
		c.closeErr = err
		close(c.done)
		c.signalLocked()
		c.mu.Unlock()
		c.mux.remove(c.id, c)
	})
}

func (c *StreamConn) signalLocked() {
	close(c.state)
	c.state = make(chan struct{})
}

func (c *StreamConn) closeError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeErr == nil {
		return ErrStreamClosed
	}
	return c.closeErr
}

func (c *StreamConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeErr != nil
}

func (s *StreamSenderState) snapshot() (epoch, next, acked, window uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epoch, s.next, s.acked, s.window
}

func (r *StreamReceiverState) snapshot() (epoch, offset, window uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.epoch, r.offset, r.window
}

func maxInt() int { return int(^uint(0) >> 1) }

var _ io.ReadWriteCloser = (*StreamConn)(nil)
