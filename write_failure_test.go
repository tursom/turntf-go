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
)

type failNextWriteConn struct {
	net.Conn
	mu       sync.Mutex
	failNext bool
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
