package turntf

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	pb "github.com/tursom/turntf-go/internal/proto"
	"google.golang.org/protobuf/proto"
)

type failNextWriteConn struct {
	net.Conn
	mu       sync.Mutex
	failNext bool
}

type blockNextWriteConn struct {
	net.Conn
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (c *blockNextWriteConn) Write(p []byte) (int, error) {
	if c.armed.CompareAndSwap(true, false) {
		close(c.entered)
		<-c.release
	}
	return c.Conn.Write(p)
}

func (c *failNextWriteConn) arm() {
	c.mu.Lock()
	c.failNext = true
	c.mu.Unlock()
}

func (c *failNextWriteConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	fail := c.failNext
	c.failNext = false
	c.mu.Unlock()
	if fail {
		return 0, syscall.EPIPE
	}
	return c.Conn.Write(p)
}

type writeFailureHandler struct {
	disconnects chan error
	streamSends chan StreamSendResult
}

func (h *writeFailureHandler) OnLogin(context.Context, LoginInfo) {}
func (h *writeFailureHandler) OnMessage(context.Context, Message) {}
func (h *writeFailureHandler) OnPacket(context.Context, Packet)   {}
func (h *writeFailureHandler) OnError(context.Context, error)     {}
func (h *writeFailureHandler) OnDisconnect(_ context.Context, err error) {
	h.disconnects <- err
}
func (h *writeFailureHandler) OnStreamSendResult(_ context.Context, result StreamSendResult) {
	h.streamSends <- result
}

func TestWriteFailureClosesCurrentTransportAndReconnects(t *testing.T) {
	var attempts atomic.Int32
	firstRequestsRead := make(chan struct{})
	firstConnectionClosed := make(chan struct{})
	secondLogin := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.CloseNow()

		attempt := attempts.Add(1)
		_ = mustReadClientEnvelope(t, conn).GetLogin()
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{Body: &pb.ServerEnvelope_LoginResponse{
			LoginResponse: &pb.LoginResponse{
				User:            &pb.User{NodeId: 4096, UserId: 1025, Username: "alice", Role: "user"},
				ProtocolVersion: clientProtocolVersion,
			},
		}})

		if attempt == 1 {
			if got := mustReadClientEnvelope(t, conn).GetStreamFrame(); got == nil {
				t.Error("first request is not a tracked stream frame")
			}
			if got := mustReadClientEnvelope(t, conn).GetPing(); got == nil {
				t.Error("second request is not ping")
			}
			close(firstRequestsRead)
			_, _, _ = conn.Read(context.Background())
			close(firstConnectionClosed)
			return
		}

		close(secondLogin)
		_, _, _ = conn.Read(context.Background())
	}))
	defer server.Close()

	sockets := make(chan *failNextWriteConn, 2)
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		wrapped := &failNextWriteConn{Conn: conn}
		sockets <- wrapped
		return wrapped, nil
	}}
	defer transport.CloseIdleConnections()

	handler := &writeFailureHandler{
		disconnects: make(chan error, 2),
		streamSends: make(chan StreamSendResult, 2),
	}
	client, err := NewClient(Config{
		BaseURL:               server.URL,
		Credentials:           Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("alice-password")},
		Handler:               handler,
		HTTPClient:            &http.Client{Transport: transport},
		Reconnect:             true,
		InitialReconnectDelay: 10 * time.Millisecond,
		MaxReconnectDelay:     20 * time.Millisecond,
		PingInterval:          time.Hour,
		RequestTimeout:        time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	firstSocket := <-sockets

	requestID, err := client.SendStreamFrameTracked(ctx, UserRef{NodeID: 4096, UserID: 2049}, SessionRef{}, StreamFrame{
		Kind: StreamFrameData,
		ID:   testStreamID(),
	}, DeliveryModeBestEffort)
	if err != nil {
		t.Fatalf("SendStreamFrameTracked: %v", err)
	}
	pingDone := make(chan error, 1)
	go func() { pingDone <- client.Ping(ctx) }()

	select {
	case <-firstRequestsRead:
	case <-ctx.Done():
		t.Fatal("server did not enter its blocking read")
	}
	firstSocket.arm()
	if _, err := client.SendStreamFrame(ctx, UserRef{NodeID: 4096, UserID: 2049}, SessionRef{}, StreamFrame{
		Kind: StreamFrameData,
		ID:   testStreamID(),
	}, DeliveryModeBestEffort); !errors.Is(err, syscall.EPIPE) {
		t.Fatalf("write error = %v, want broken pipe", err)
	}

	select {
	case <-firstConnectionClosed:
	case <-ctx.Done():
		t.Fatal("write failure did not close the current transport")
	}
	select {
	case err := <-pingDone:
		if !errors.Is(err, ErrDisconnected) {
			t.Fatalf("pending RPC error = %v, want ErrDisconnected", err)
		}
	case <-ctx.Done():
		t.Fatal("pending RPC was not released")
	}
	select {
	case result := <-handler.streamSends:
		if result.RequestID != requestID || !errors.Is(result.Err, ErrDisconnected) {
			t.Fatalf("tracked stream result = %+v, want request %d disconnected", result, requestID)
		}
	case <-ctx.Done():
		t.Fatal("tracked stream was not released")
	}
	select {
	case <-handler.streamSends:
		t.Fatal("tracked stream callback ran more than once")
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case <-handler.disconnects:
	case <-ctx.Done():
		t.Fatal("disconnect callback was not invoked")
	}
	select {
	case <-secondLogin:
	case <-ctx.Done():
		t.Fatal("client did not reconnect after write failure")
	}
	if _, ok := client.CurrentLogin(); !ok {
		t.Fatal("reconnected client is not authenticated")
	}
}

func TestInvalidateTransportIgnoresStaleConnection(t *testing.T) {
	current := new(websocket.Conn)
	stale := new(websocket.Conn)
	client := &Client{
		conn:          current,
		authenticated: true,
		loginInfo:     LoginInfo{User: User{Username: "current"}},
	}

	client.invalidateTransport(stale)

	if client.conn != current || !client.authenticated || client.loginInfo.User.Username != "current" {
		t.Fatalf("stale failure invalidated current transport: conn=%p authenticated=%v login=%+v", client.conn, client.authenticated, client.loginInfo)
	}
}

func TestQueuedWritesRejectStaleTransportAfterReconnect(t *testing.T) {
	const queued = 8
	type receivedEnvelope struct {
		generation int32
		envelope   *pb.ClientEnvelope
	}

	var attempts atomic.Int32
	received := make(chan receivedEnvelope, queued+4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.CloseNow()
		generation := attempts.Add(1)
		for {
			_, payload, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			var env pb.ClientEnvelope
			if err := proto.Unmarshal(payload, &env); err != nil {
				t.Errorf("decode generation %d envelope: %v", generation, err)
				return
			}
			received <- receivedEnvelope{generation: generation, envelope: &env}
		}
	}))
	defer server.Close()

	var oldSocket *blockNextWriteConn
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		oldSocket = &blockNextWriteConn{Conn: conn, entered: make(chan struct{}), release: make(chan struct{})}
		return oldSocket, nil
	}}
	defer transport.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	oldConn, _, err := websocket.Dial(ctx, server.URL, &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatalf("dial old websocket: %v", err)
	}
	defer oldConn.CloseNow()
	newConn, _, err := websocket.Dial(ctx, server.URL, nil)
	if err != nil {
		t.Fatalf("dial new websocket: %v", err)
	}
	defer newConn.CloseNow()

	handler := &writeFailureHandler{
		disconnects: make(chan error, 1),
		streamSends: make(chan StreamSendResult, 2),
	}
	clientCtx, cancelClient := context.WithCancel(context.Background())
	defer cancelClient()
	client := &Client{
		cfg: Config{
			Handler:                handler,
			RequestTimeout:         time.Second,
			StreamSendPendingLimit: DefaultStreamSendPendingLimit,
		},
		ctx:               clientCtx,
		conn:              oldConn,
		authenticated:     true,
		pending:           make(map[uint64]chan requestResult),
		streamSendPending: make(map[uint64]StreamSendMetadata),
	}

	pingDone := make(chan error, 1)
	go func() { pingDone <- client.Ping(ctx) }()
	select {
	case got := <-received:
		if got.generation != 1 || got.envelope.GetPing() == nil {
			t.Fatalf("old pending RPC reached generation %d as %T", got.generation, got.envelope.Body)
		}
	case <-ctx.Done():
		t.Fatal("old pending RPC was not written")
	}

	streamRequestID, err := client.SendStreamFrameTracked(ctx, UserRef{NodeID: 4096, UserID: 2049}, SessionRef{}, StreamFrame{
		Kind: StreamFrameData,
		ID:   testStreamID(),
	}, DeliveryModeBestEffort)
	if err != nil {
		t.Fatalf("write old tracked stream: %v", err)
	}
	select {
	case got := <-received:
		stream := got.envelope.GetStreamFrame()
		if got.generation != 1 || stream == nil || stream.RequestId != streamRequestID {
			t.Fatalf("old tracked stream reached generation %d as %T", got.generation, got.envelope.Body)
		}
	case <-ctx.Done():
		t.Fatal("old tracked stream was not written")
	}

	oldSocket.armed.Store(true)
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- client.writeProtoOnTransport(ctx, oldConn, &pb.ClientEnvelope{Body: &pb.ClientEnvelope_Ping{
			Ping: &pb.Ping{RequestId: 100},
		}})
	}()
	select {
	case <-oldSocket.entered:
	case <-ctx.Done():
		t.Fatal("first old transport write did not block")
	}

	queuedDone := make(chan error, queued)
	for i := 0; i < queued; i++ {
		requestID := uint64(200 + i)
		go func() {
			queuedDone <- client.writeProtoOnTransport(ctx, oldConn, &pb.ClientEnvelope{Body: &pb.ClientEnvelope_Ping{
				Ping: &pb.Ping{RequestId: requestID},
			}})
		}()
	}

	client.stateMu.Lock()
	client.conn = newConn
	client.authenticated = true
	client.stateMu.Unlock()
	client.failAllPending(ErrDisconnected)
	client.failAllStreamSends(ErrDisconnected)
	close(oldSocket.release)

	newWriteStarted := time.Now()
	newWriteDone := make(chan error, 1)
	go func() {
		newWriteDone <- client.sendEnvelope(ctx, &pb.ClientEnvelope{Body: &pb.ClientEnvelope_Ping{
			Ping: &pb.Ping{RequestId: 999},
		}})
	}()
	select {
	case err := <-newWriteDone:
		if err != nil {
			t.Fatalf("new transport write: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("new transport write starved behind stale queue")
	}
	if elapsed := time.Since(newWriteStarted); elapsed > 500*time.Millisecond {
		t.Fatalf("new transport write starved behind stale queue for %v", elapsed)
	}

	if err := <-firstDone; err != nil {
		t.Fatalf("in-flight old write: %v", err)
	}
	for i := 0; i < queued; i++ {
		if err := <-queuedDone; !errors.Is(err, ErrDisconnected) {
			t.Fatalf("queued old write %d = %v, want ErrDisconnected", i, err)
		}
	}

	want := map[uint64]int32{100: 1, 999: 2}
	for len(want) != 0 {
		select {
		case got := <-received:
			ping := got.envelope.GetPing()
			if ping == nil {
				t.Fatalf("generation %d received non-Ping envelope", got.generation)
			}
			generation, ok := want[ping.RequestId]
			if !ok {
				t.Fatalf("stale queued request %d reached generation %d", ping.RequestId, got.generation)
			}
			if got.generation != generation {
				t.Fatalf("request %d reached generation %d, want %d", ping.RequestId, got.generation, generation)
			}
			delete(want, ping.RequestId)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for expected writes: %v", want)
		}
	}
	select {
	case got := <-received:
		t.Fatalf("unexpected request %d reached generation %d", got.envelope.GetPing().GetRequestId(), got.generation)
	case <-time.After(100 * time.Millisecond):
	}

	select {
	case err := <-pingDone:
		if !errors.Is(err, ErrDisconnected) {
			t.Fatalf("pending RPC cleanup = %v, want ErrDisconnected", err)
		}
	case <-ctx.Done():
		t.Fatal("pending RPC was not cleaned")
	}
	client.pendingMu.Lock()
	pendingCount := len(client.pending)
	client.pendingMu.Unlock()
	if pendingCount != 0 {
		t.Fatalf("pending RPC entries = %d, want 0", pendingCount)
	}
	select {
	case result := <-handler.streamSends:
		if result.RequestID != streamRequestID || !errors.Is(result.Err, ErrDisconnected) {
			t.Fatalf("tracked stream cleanup = %+v", result)
		}
	default:
		t.Fatal("tracked stream was not cleaned")
	}
	select {
	case result := <-handler.streamSends:
		t.Fatalf("tracked stream was cleaned more than once: %+v", result)
	default:
	}
	if len(client.streamSendPending) != 0 {
		t.Fatalf("tracked stream pending entries = %d, want 0", len(client.streamSendPending))
	}
}

func TestPingTimeoutClosesHalfOpenTransportAndReconnects(t *testing.T) {
	var attempts atomic.Int32
	firstRequestsRead := make(chan struct{})
	firstConnectionClosed := make(chan struct{})
	secondLogin := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.CloseNow()

		attempt := attempts.Add(1)
		_ = mustReadClientEnvelope(t, conn).GetLogin()
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{Body: &pb.ServerEnvelope_LoginResponse{
			LoginResponse: &pb.LoginResponse{
				User:            &pb.User{NodeId: 4096, UserId: 1025, Username: "alice", Role: "user"},
				ProtocolVersion: clientProtocolVersion,
			},
		}})

		if attempt == 1 {
			var gotGetUser, gotStream, gotPing bool
			for !gotGetUser || !gotStream || !gotPing {
				env := mustReadClientEnvelope(t, conn)
				gotGetUser = gotGetUser || env.GetGetUser() != nil
				gotStream = gotStream || env.GetStreamFrame() != nil
				gotPing = gotPing || env.GetPing() != nil
			}
			close(firstRequestsRead)
			_, _, _ = conn.Read(context.Background())
			close(firstConnectionClosed)
			return
		}

		close(secondLogin)
		for {
			_, payload, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			var env pb.ClientEnvelope
			if proto.Unmarshal(payload, &env) != nil || env.GetPing() == nil {
				continue
			}
			response, err := proto.Marshal(&pb.ServerEnvelope{Body: &pb.ServerEnvelope_Pong{
				Pong: &pb.Pong{RequestId: env.GetPing().RequestId},
			}})
			if err != nil {
				return
			}
			if conn.Write(context.Background(), websocket.MessageBinary, response) != nil {
				return
			}
		}
	}))
	defer server.Close()

	handler := &writeFailureHandler{
		disconnects: make(chan error, 2),
		streamSends: make(chan StreamSendResult, 2),
	}
	client, err := NewClient(Config{
		BaseURL:               server.URL,
		Credentials:           Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("alice-password")},
		Handler:               handler,
		Reconnect:             true,
		InitialReconnectDelay: 10 * time.Millisecond,
		MaxReconnectDelay:     20 * time.Millisecond,
		PingInterval:          100 * time.Millisecond,
		RequestTimeout:        80 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	getUserDone := make(chan error, 1)
	go func() {
		_, err := client.GetUser(ctx, UserRef{NodeID: 4096, UserID: 1025})
		getUserDone <- err
	}()
	requestID, err := client.SendStreamFrameTracked(ctx, UserRef{NodeID: 4096, UserID: 2049}, SessionRef{}, StreamFrame{
		Kind: StreamFrameData,
		ID:   testStreamID(),
	}, DeliveryModeBestEffort)
	if err != nil {
		t.Fatalf("SendStreamFrameTracked: %v", err)
	}

	select {
	case <-firstRequestsRead:
	case <-ctx.Done():
		t.Fatal("server did not read pending requests and heartbeat Ping")
	}
	select {
	case <-firstConnectionClosed:
	case <-ctx.Done():
		t.Fatal("Ping timeout did not close the half-open transport")
	}
	select {
	case err := <-getUserDone:
		if !errors.Is(err, ErrDisconnected) {
			t.Fatalf("pending RPC error = %v, want ErrDisconnected", err)
		}
	case <-ctx.Done():
		t.Fatal("pending RPC was not released")
	}
	select {
	case result := <-handler.streamSends:
		if result.RequestID != requestID || !errors.Is(result.Err, ErrDisconnected) {
			t.Fatalf("tracked stream result = %+v, want request %d disconnected", result, requestID)
		}
	case <-ctx.Done():
		t.Fatal("tracked stream was not released")
	}
	select {
	case <-handler.streamSends:
		t.Fatal("tracked stream callback ran more than once")
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case <-handler.disconnects:
	case <-ctx.Done():
		t.Fatal("disconnect callback was not invoked")
	}
	select {
	case <-handler.disconnects:
		t.Fatal("disconnect callback ran more than once for one transport")
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case <-secondLogin:
	case <-ctx.Done():
		t.Fatal("client did not reauthenticate after Ping timeout")
	}

	client.pendingMu.Lock()
	pending := len(client.pending)
	client.pendingMu.Unlock()
	if pending != 0 {
		t.Fatalf("old pending RPCs leaked after reconnect: %d", pending)
	}
	if _, ok := client.CurrentLogin(); !ok {
		t.Fatal("reconnected client is not authenticated")
	}
}

func TestPingFailureDoesNotInvalidateNewTransportGeneration(t *testing.T) {
	current := new(websocket.Conn)
	stale := new(websocket.Conn)
	client := &Client{
		conn:          current,
		authenticated: true,
		loginInfo:     LoginInfo{User: User{Username: "current"}},
	}

	client.invalidatePingTransport(stale, context.DeadlineExceeded)

	if client.conn != current || !client.authenticated || client.loginInfo.User.Username != "current" {
		t.Fatalf("stale Ping invalidated current transport: conn=%p authenticated=%v login=%+v", client.conn, client.authenticated, client.loginInfo)
	}
}

func TestPingServerErrorKeepsTransport(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.CloseNow()

		_ = mustReadClientEnvelope(t, conn).GetLogin()
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{Body: &pb.ServerEnvelope_LoginResponse{
			LoginResponse: &pb.LoginResponse{
				User:            &pb.User{NodeId: 4096, UserId: 1025, Username: "alice", Role: "user"},
				ProtocolVersion: clientProtocolVersion,
			},
		}})
		first := mustReadClientEnvelope(t, conn).GetPing()
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{Body: &pb.ServerEnvelope_Error{
			Error: &pb.Error{RequestId: first.RequestId, Code: "rate_limited", Message: "try later"},
		}})
		second := mustReadClientEnvelope(t, conn).GetPing()
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{Body: &pb.ServerEnvelope_Pong{
			Pong: &pb.Pong{RequestId: second.RequestId},
		}})
		_, _, _ = conn.Read(context.Background())
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:        server.URL,
		Credentials:    Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("alice-password")},
		PingInterval:   time.Hour,
		RequestTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	var serverErr *ServerError
	if err := client.Ping(ctx); !errors.As(err, &serverErr) || serverErr.Code != "rate_limited" {
		t.Fatalf("first Ping error = %v, want rate_limited ServerError", err)
	}
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("second Ping on same transport: %v", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("websocket attempts = %d, want 1", got)
	}
}
