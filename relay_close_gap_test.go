package turntf

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	pb "github.com/tursom/turntf-go/internal/proto"
)

func TestReReviewImmediateReceiveClose(t *testing.T) {
	old := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(old)
	for i := 0; i < 20000; i++ {
		c := newTestRelayConnection()
		c.sendBase, c.nextSeq = 1, 1
		var ack atomic.Uint64
		var overtook atomic.Bool
		c.sendEnvelope = func(e *RelayEnvelope) error {
			if e.Kind == RelayKindAck {
				ack.Store(e.AckSeq)
			}
			if e.Kind == RelayKindClose && ack.Load() < 1 {
				overtook.Store(true)
			}
			return nil
		}
		c.wg.Add(1)
		go c.sendLoop()
		done := make(chan error, 1)
		go func() { <-c.Receive(); done <- c.Close() }()
		c.enqueueEnvelope(&RelayEnvelope{Kind: RelayKindData, Seq: 1, Payload: []byte("half-close")})
		err := <-done
		c.wg.Wait()
		if err != nil {
			t.Fatal(err)
		}
		if overtook.Load() {
			t.Fatalf("iteration %d: application received DATA1, wire CLOSE preceded ACK1, Close returned nil; final ACK=%d", i, ack.Load())
		}
	}
}

func TestReReviewCloseCallbackDeadline(t *testing.T) {
	c := newTestRelayConnection()
	c.config.CloseTimeoutMs = 30
	c.sendEnvelope = func(*RelayEnvelope) error { return nil }
	entered, release := make(chan struct{}), make(chan struct{})
	c.OnClose(func(error) { close(entered); <-release })
	c.wg.Add(1)
	go c.sendLoop()
	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	<-entered
	select {
	case err := <-done:
		close(release)
		t.Logf("Close returned: %v", err)
	case <-time.After(150 * time.Millisecond):
		close(release)
		err := <-done
		c.wg.Wait()
		t.Fatalf("Close exceeded 30ms deadline while synchronous callback blocked; after release returns %v", err)
	}
}

func TestReReviewCloseWriteLockDeadline(t *testing.T) {
	client, c := perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
		send(perfAccepted(req))
	})
	c.config.CloseTimeoutMs = 30
	c.wg.Add(1)
	go c.sendLoop()
	client.writeMu.Lock()
	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	reviewWait(t, func() bool { client.pendingMu.Lock(); defer client.pendingMu.Unlock(); return len(client.pending) == 1 }, "CLOSE RPC not registered")
	select {
	case err := <-done:
		reviewWait(t, func() bool {
			client.pendingMu.Lock()
			defer client.pendingMu.Unlock()
			return len(client.pending) == 0
		}, "canceled CLOSE kept a pending slot behind the shared write lock")
		client.writeMu.Unlock()
		c.wg.Wait()
		t.Logf("Close returned: %v", err)
	case <-time.After(150 * time.Millisecond):
		client.pendingMu.Lock()
		pending := len(client.pending)
		client.pendingMu.Unlock()
		state := c.State()
		client.writeMu.Unlock()
		err := <-done
		t.Fatalf("Close exceeded 30ms deadline on shared writeMu: state=%v pending=%d; after release returns %v", state, pending, err)
	}
}

func TestCloseDeadlineDuringSocketWriteKeepsSharedWS(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		for {
			typ, data, err := ws.Read(r.Context())
			if err != nil {
				return
			}
			if ws.Write(r.Context(), typ, data) != nil {
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, server.URL, &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	defer ws.CloseNow()
	release := sync.OnceFunc(func() { close(socket.release) })
	defer release()
	client, err := NewClient(Config{BaseURL: server.URL, Credentials: Credentials{LoginName: "test", Password: HashedPassword("test")}, RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.cancel()
	client.conn = ws
	c := newTestRelayConnection()
	c.relay.client = client
	c.remotePeer = UserRef{NodeID: 8192, UserID: 1}
	c.remoteSession = SessionRef{ServingNodeID: 8192, SessionID: "remote"}
	c.config.CloseTimeoutMs = 30
	c.wg.Add(1)
	go c.sendLoop()
	socket.armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	select {
	case <-socket.entered:
	case <-ctx.Done():
		t.Fatal("CLOSE did not enter the socket write")
	}
	select {
	case err := <-done:
		var relayErr *RelayError
		if !errors.As(err, &relayErr) || relayErr.Code != RelayErrorCloseTimeout {
			t.Fatalf("Close error = %v", err)
		}
	case <-time.After(150 * time.Millisecond):
		t.Fatal("Close waited for the blocked socket")
	}
	reviewWait(t, func() bool {
		client.pendingMu.Lock()
		defer client.pendingMu.Unlock()
		return len(client.pending) == 0
	}, "blocked CLOSE retained pending RPC")
	release()
	c.wg.Wait()
	if _, _, err := ws.Read(ctx); err != nil {
		t.Fatalf("in-flight frame was canceled: %v", err)
	}
	if err := ws.Write(ctx, websocket.MessageBinary, []byte("still-open")); err != nil {
		t.Fatal(err)
	}
	_, data, err := ws.Read(ctx)
	if err != nil || string(data) != "still-open" {
		t.Fatalf("shared connection lost: %q %v", data, err)
	}
}
