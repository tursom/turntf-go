package turntf

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/tursom/turntf-go/internal/proto"
)

func dispatchPush(send func(*pb.ServerEnvelope), id string, kind RelayKind, seq uint64) {
	b, _ := encodeRelayEnvelope(&RelayEnvelope{RelayID: id, Kind: kind, Seq: seq, AckSeq: seq, Payload: []byte(fmt.Sprint(seq))})
	send(&pb.ServerEnvelope{Body: &pb.ServerEnvelope_PacketPushed{PacketPushed: &pb.PacketPushed{Packet: &pb.Packet{Body: b}}}})
}

func dispatchPing(t *testing.T, c *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("independent Ping: %v", err)
	}
}

func dispatchDrain(t *testing.T, c *RelayConnection, count int) {
	t.Helper()
	for i := 1; i <= count; i++ {
		select {
		case b := <-c.Receive():
			if string(b) != fmt.Sprint(i) {
				t.Fatalf("frame %d: %q", i, b)
			}
		case <-time.After(time.Second):
			t.Fatalf("missing frame %d", i)
		}
	}
}

func TestRelayWSMixedSlowFastOrdered(t *testing.T) {
	for _, terminal := range []RelayKind{RelayKindClose, RelayKindError} {
		t.Run(fmt.Sprint(terminal), func(t *testing.T) {
			injected := make(chan struct{})
			var ackMax atomic.Uint64
			client, slow := perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
				if env != nil && env.Kind == RelayKindAck {
					if env.RelayID == "test-relay" {
						ackMax.Store(env.AckSeq)
					}
					send(perfAccepted(req))
					return
				}
				// A legal sender window of 16, reordered in each window, with retransmits.
				for start := uint64(1); start <= 64; start += 16 {
					for seq := start + 15; seq >= start; seq-- {
						dispatchPush(send, "test-relay", RelayKindData, seq)
					}
					deadline := time.Now().Add(time.Second)
					for ackMax.Load() < start+15 && time.Now().Before(deadline) {
						time.Sleep(time.Millisecond)
					}
				}
				for seq := uint64(80); seq >= 65; seq-- {
					dispatchPush(send, "test-relay", RelayKindData, seq)
				}
				for round := 0; round < 8; round++ {
					for seq := uint64(65); seq <= 80; seq++ {
						dispatchPush(send, "test-relay", RelayKindData, seq)
					}
				}
				// Same-relay ACK must remain ahead of its terminal event.
				dispatchPush(send, "test-relay", RelayKindAck, 1)
				dispatchPush(send, "test-relay", terminal, 0)
				for seq := uint64(1); seq <= 32; seq++ {
					dispatchPush(send, "fast", RelayKindData, seq)
				}
				send(perfAccepted(req))
				close(injected)
			})
			fast := newTestRelayConnection()
			fast.relay, fast.relayID = client.Relay(), "fast"
			fast.sendEnvelope = func(*RelayEnvelope) error { return nil }
			fast.relay.mu.Lock()
			fast.relay.conns["fast"] = fast
			fast.relay.mu.Unlock()
			defer func() { fast.Abort(nil); fast.wg.Wait() }()
			slow.sendBase, slow.nextSeq = 1, 2
			slow.unacked[1] = unackedFrame{data: []byte("outgoing")}
			closed := make(chan error, 1)
			slow.OnClose(func(err error) { closed <- err })
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if _, err := client.SendPacket(ctx, SendPacketInput{Target: UserRef{NodeID: 2, UserID: 2}, Body: []byte("inject"), DeliveryMode: DeliveryModeBestEffort}); err != nil {
				t.Fatal(err)
			}
			<-injected
			dispatchPing(t, client)
			dispatchDrain(t, fast, 32)
			if n := ackMax.Load(); n > 64 {
				t.Fatalf("ACK %d passed blocked receive budget", n)
			}
			select {
			case err := <-closed:
				t.Fatalf("terminal overtook DATA: %v", err)
			default:
			}
			dispatchDrain(t, slow, 80)
			select {
			case err := <-closed:
				e, ok := err.(*RelayError)
				if !ok || (terminal == RelayKindClose && e.Code != RelayErrorRemoteClose) || (terminal == RelayKindError && e.Code != RelayErrorProtocol) {
					t.Fatalf("terminal: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("terminal not delivered")
			}
			slow.mu.Lock()
			base := slow.sendBase
			slow.mu.Unlock()
			if base != 2 {
				t.Fatalf("terminal overtook ACK: base=%d", base)
			}
			if ackMax.Load() != 80 {
				t.Fatalf("terminal passed receive ACK barrier: %d", ackMax.Load())
			}
			dispatchPing(t, client)
		})
	}
}

func TestRelayWSOverflowIsLocalWithBlockingCallback(t *testing.T) {
	client, slow := perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
		if env != nil && env.Kind == RelayKindAck {
			send(perfAccepted(req))
			return
		}
		for seq := uint64(1); seq <= 160; seq++ {
			dispatchPush(send, "test-relay", RelayKindData, seq)
		}
		dispatchPush(send, "fast", RelayKindData, 1)
		send(perfAccepted(req))
	})
	slow.config.Reliability = ReliabilityBestEffort
	fast := newTestRelayConnection()
	fast.relay, fast.relayID = client.Relay(), "fast"
	fast.sendEnvelope = func(*RelayEnvelope) error { return nil }
	fast.relay.mu.Lock()
	fast.relay.conns["fast"] = fast
	fast.relay.mu.Unlock()
	defer func() { fast.Abort(nil); fast.wg.Wait() }()
	release := make(chan struct{})
	defer close(release)
	reason := make(chan error, 1)
	slow.OnClose(func(err error) { reason <- err; <-release })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := client.SendPacket(ctx, SendPacketInput{Target: UserRef{NodeID: 2, UserID: 2}, Body: []byte("inject"), DeliveryMode: DeliveryModeBestEffort}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-reason:
		e, ok := err.(*RelayError)
		if !ok || e.Code != RelayErrorReceiveOverflow {
			t.Fatalf("overflow: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("overflow did not close slow Relay")
	}
	dispatchDrain(t, fast, 1)
	dispatchPing(t, client)
	if fast.State() != RelayStateOpen {
		t.Fatal("overflow closed fast Relay")
	}
}

func TestRelayDispatchAbortCloseRace(t *testing.T) {
	for i := 0; i < 50; i++ {
		c := newTestRelayConnection()
		c.sendEnvelope = func(*RelayEnvelope) error { return nil }
		var calls atomic.Int32
		c.OnClose(func(error) { calls.Add(1) })
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			for seq := uint64(1); seq <= 80; seq++ {
				c.enqueueEnvelope(&RelayEnvelope{Kind: RelayKindData, Seq: seq, Payload: []byte("x")})
			}
		}()
		go func() { defer wg.Done(); c.Abort(nil) }()
		go func() { defer wg.Done(); _ = c.Close() }()
		wg.Wait()
		c.wg.Wait()
		if calls.Load() != 1 {
			t.Fatalf("OnClose ran %d times", calls.Load())
		}
	}
}
