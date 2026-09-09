package turntf

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/tursom/turntf-go/internal/proto"
)

func TestRelayWSLostACKUntilMaxRetransmits(t *testing.T) {
	var frames atomic.Int32
	client, c := perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
		if env.Kind == RelayKindData {
			frames.Add(1)
		}
		send(perfAccepted(req))
	})
	c.config.AckTimeoutMs = 15
	c.config.MaxRetransmits = 3
	c.wg.Add(1)
	go c.sendLoop()
	if err := c.Send([]byte("one")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.closeCh:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("lost ACK never terminated")
	}
	c.mu.Lock()
	reason := c.closeErr
	c.mu.Unlock()
	e, ok := reason.(*RelayError)
	if !ok || e.Code != RelayErrorMaxRetransmit {
		t.Fatalf("reason: %v", reason)
	}
	if n := frames.Load(); n != 4 {
		t.Fatalf("DATA attempts=%d want original + 3 retries", n)
	}
	dispatchPing(t, client)
}

func TestRelayPipelineAcceptanceFailureReconnect(t *testing.T) {
	for _, mode := range []string{"acceptance_error", "disconnect"} {
		t.Run(mode, func(t *testing.T) {
			full := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			var requests atomic.Int32
			var phase atomic.Int32
			var wireClose atomic.Int32
			client, c := perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
				if env.Kind == RelayKindClose {
					wireClose.Add(1)
					send(perfAccepted(req))
					return
				}
				if env.Kind != RelayKindData {
					send(perfAccepted(req))
					return
				}
				if phase.Load() == 1 {
					send(perfAccepted(req))
					send(perfAck(env))
					return
				}
				n := requests.Add(1)
				// Deliberately ACK before RPC acceptance. Close must still wait.
				send(perfAck(env))
				if n == 16 {
					close(full)
				}
				<-release
				if mode == "acceptance_error" {
					send(&pb.ServerEnvelope{Body: &pb.ServerEnvelope_Error{Error: &pb.Error{RequestId: req.RequestId, Code: "forbidden", Message: "pipeline rejected"}}})
				}
			})
			c.wg.Add(1)
			go c.sendLoop()
			for i := 0; i < 32; i++ {
				if err := c.Send([]byte("data")); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-full:
			case <-time.After(time.Second):
				t.Fatal("pipeline not full")
			}
			closed := make(chan error, 1)
			c.OnClose(func(err error) { closed <- err })
			closeDone := make(chan error, 1)
			go func() { closeDone <- c.Close() }()
			select {
			case err := <-closeDone:
				t.Fatalf("Close passed acceptance barrier: %v", err)
			case <-time.After(30 * time.Millisecond):
			}
			if n := requests.Load(); n != 16 {
				t.Fatalf("unaccepted DATA=%d", n)
			}
			// A blocked receiving Relay shares this old connection at disconnect time.
			slow := newTestRelayConnection()
			slow.relay, slow.relayID = client.Relay(), "old-slow"
			slow.sendEnvelope = func(*RelayEnvelope) error { return nil }
			slow.relay.mu.Lock()
			slow.relay.conns[slow.relayID] = slow
			slow.relay.mu.Unlock()
			defer func() { slow.Abort(nil); slow.wg.Wait() }()
			for seq := uint64(1); seq <= 80; seq++ {
				slow.enqueueEnvelope(&RelayEnvelope{Kind: RelayKindData, Seq: seq, Payload: []byte("slow")})
			}
			if mode == "acceptance_error" {
				releaseOnce.Do(func() { close(release) })
				select {
				case err := <-closed:
					var se *ServerError
					if !errors.As(err, &se) || se.Code != "forbidden" {
						t.Fatalf("acceptance error lost: %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("acceptance failure did not close Relay")
				}
				dispatchPing(t, client)
			}
			client.stateMu.RLock()
			oldWS := client.conn
			client.stateMu.RUnlock()
			if err := oldWS.CloseNow(); err != nil {
				t.Logf("CloseNow: %v", err)
			}
			releaseOnce.Do(func() { close(release) })
			phase.Store(1)
			select {
			case <-slow.closeCh:
			case <-time.After(time.Second):
				t.Fatal("disconnect left slow dispatcher alive")
			}
			select {
			case err := <-closeDone:
				if err == nil {
					t.Fatal("Close hid failed DATA acceptance")
				}
				if mode == "acceptance_error" {
					var se *ServerError
					if !errors.As(err, &se) || se.Code != "forbidden" {
						t.Fatalf("Close lost acceptance cause: %v", err)
					}
				}
			case <-time.After(time.Second):
				t.Fatal("Close blocked after failure")
			}
			c.wg.Wait()
			slow.wg.Wait()
			if wireClose.Load() != 0 {
				t.Fatal("failed pipeline sent CLOSE past barrier")
			}
			deadline := time.Now().Add(3 * time.Second)
			for {
				client.stateMu.RLock()
				fresh := client.conn != nil && client.conn != oldWS && client.authenticated
				client.stateMu.RUnlock()
				if fresh {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("WebSocket did not reconnect")
				}
				time.Sleep(5 * time.Millisecond)
			}
			client.pendingMu.Lock()
			pending := len(client.pending)
			client.pendingMu.Unlock()
			if pending != 0 {
				t.Fatalf("old pending RPC leaked: %d", pending)
			}
			dispatchPing(t, client)
			next := newTestRelayConnection()
			next.relay, next.relayID = client.Relay(), "fresh-relay"
			next.sendBase, next.nextSeq = 1, 1
			next.remotePeer = c.remotePeer
			next.remoteSession = c.remoteSession
			next.relay.mu.Lock()
			next.relay.conns[next.relayID] = next
			next.relay.mu.Unlock()
			defer func() { next.Abort(nil); next.wg.Wait() }()
			next.wg.Add(1)
			go next.sendLoop()
			if err := next.Send([]byte("fresh")); err != nil {
				t.Fatal(err)
			}
			if err := next.Close(); err != nil {
				t.Fatalf("fresh Relay: %v", err)
			}
			if wireClose.Load() != 1 {
				t.Fatalf("fresh CLOSE count=%d", wireClose.Load())
			}
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			if err := client.Ping(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
