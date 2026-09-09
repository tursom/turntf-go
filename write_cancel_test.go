package turntf

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	pb "github.com/tursom/turntf-go/internal/proto"
)

// Gate the actual socket write after websocket has installed its cancellation
// callback. This pins the Relay ACK/handleClose race without a production core.
type gatedWriteConn struct {
	net.Conn
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (c *gatedWriteConn) Write(p []byte) (int, error) {
	if c.armed.CompareAndSwap(true, false) {
		close(c.entered)
		<-c.release
	}
	return c.Conn.Write(p)
}

func TestWriteCancellationDoesNotCloseSharedWebSocket(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			typ, body, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			if conn.Write(r.Context(), typ, body) != nil {
				return
			}
		}
	}))
	defer server.Close()
	var socket *gatedWriteConn
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		socket = &gatedWriteConn{Conn: conn, entered: make(chan struct{}), release: make(chan struct{})}
		return socket, nil
	}}
	defer transport.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, server.URL, &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	client := &Client{ctx: ctx, cfg: Config{RequestTimeout: time.Second}}
	requestCtx, cancelRequest := context.WithCancel(ctx)
	defer cancelRequest()
	socket.armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- client.writeProto(requestCtx, conn, &pb.ClientEnvelope{}) }()
	select {
	case <-socket.entered:
	case <-ctx.Done():
		t.Fatal("write did not start")
	}
	cancelRequest()
	time.Sleep(20 * time.Millisecond)
	close(socket.release)
	if err := <-done; err != nil {
		t.Fatalf("request cancellation killed in-flight shared WS write: %v", err)
	}
	if _, _, err := conn.Read(ctx); err != nil {
		t.Fatalf("first frame lost: %v", err)
	}
	if err := client.writeProto(ctx, conn, &pb.ClientEnvelope{}); err != nil {
		t.Fatalf("unrelated next write failed: %v", err)
	}
	if _, _, err := conn.Read(ctx); err != nil {
		t.Fatalf("shared WS closed: %v", err)
	}
	if err := client.writeProto(requestCtx, conn, &pb.ClientEnvelope{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("already canceled request = %v", err)
	}
	if err := client.writeProto(ctx, conn, &pb.ClientEnvelope{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := conn.Read(ctx); err != nil {
		t.Fatal(err)
	}
}
