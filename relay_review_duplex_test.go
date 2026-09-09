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

// Both directions have more than recvCh's capacity in flight. The core holds
// reverse ACKs until both receive dispatchers block, then releases them without
// resuming either application reader.
func TestRelayWSReviewBidirectionalPausedReaders(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	routes := [2]chan func(*pb.ServerEnvelope){make(chan func(*pb.ServerEnvelope), 1), make(chan func(*pb.ServerEnvelope), 1)}
	var peerSend [2]func(*pb.ServerEnvelope)
	var ackSent [2]atomic.Uint64
	var clients [2]*Client
	var conns [2]*RelayConnection
	for i := 0; i < 2; i++ {
		side := i
		clients[i], conns[i] = perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
			if string(req.Body) == "route" {
				routes[side] <- send
				send(perfAccepted(req))
				return
			}
			send(perfAccepted(req))
			if env.Kind == RelayKindAck {
				for old := ackSent[side].Load(); env.AckSeq > old; old = ackSent[side].Load() {
					if ackSent[side].CompareAndSwap(old, env.AckSeq) {
						break
					}
				}
				<-release
			}
			body, _ := encodeRelayEnvelope(env)
			peerSend[1-side](&pb.ServerEnvelope{Body: &pb.ServerEnvelope_PacketPushed{PacketPushed: &pb.PacketPushed{Packet: &pb.Packet{Body: body}}}})
		})
		conns[i].config.WindowSize = 128
		conns[i].config.AckTimeoutMs = 2000
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := clients[i].SendPacket(ctx, SendPacketInput{Target: UserRef{NodeID: 2, UserID: 2}, Body: []byte("route"), DeliveryMode: DeliveryModeBestEffort})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		peerSend[i] = <-routes[i]
	}
	for _, c := range conns {
		c.wg.Add(1)
		go c.sendLoop()
	}
	for seq := 1; seq <= 96; seq++ {
		for _, c := range conns {
			if err := c.Send([]byte(fmt.Sprint(seq))); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, c := range conns {
		reviewWait(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return len(c.recvCh) == 64 && c.recvReady > 0 }, "receive dispatcher did not block")
	}
	once.Do(func() { close(release) })
	for i, c := range conns {
		reviewWait(t, func() bool {
			c.mu.Lock()
			defer c.mu.Unlock()
			return c.sendBase > 1 && c.sendBase == ackSent[1-i].Load()+1
		}, "reverse ACK blocked behind forward DATA")
		dispatchPing(t, clients[i])
	}
	for _, c := range conns {
		dispatchDrain(t, c, 96)
	}
	for _, c := range conns {
		reviewWait(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.sendBase == 97 && len(c.unacked) == 0 }, "final end-to-end ACK missing")
	}
}

func TestRelayWSReviewFullAcceptanceTimerPartialRelease(t *testing.T) {
	partial := make(chan struct{})
	rest := make(chan struct{})
	var oncePartial, onceRest sync.Once
	defer oncePartial.Do(func() { close(partial) })
	defer onceRest.Do(func() { close(rest) })
	var mu sync.Mutex
	seen := make(map[uint64]int)
	active, maxActive, originals, retries := 0, 0, 0, 0
	var closeWire atomic.Bool
	_, c := perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
		if env.Kind == RelayKindClose {
			closeWire.Store(true)
			send(perfAccepted(req))
			return
		}
		if env.Kind != RelayKindData {
			send(perfAccepted(req))
			return
		}
		mu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		seen[env.Seq]++
		first := seen[env.Seq] == 1
		if first && env.Seq <= 16 {
			originals++
		} else if !first {
			retries++
		}
		mu.Unlock()
		if first && env.Seq <= 16 {
			if env.Seq <= 4 {
				<-partial
			} else {
				<-rest
			}
		}
		mu.Lock()
		active--
		mu.Unlock()
		send(perfAccepted(req))
		if !first || env.Seq > 16 {
			// All initial 16 DATA have reached the peer; this ACK is deliberately
			// delayed until a timer-driven retransmission actually reaches the wire.
			ack := *env
			if ack.Seq <= 16 {
				ack.Seq = 16
			}
			send(perfAck(&ack))
		}
	})
	c.config.AckTimeoutMs = 20
	c.wg.Add(1)
	go c.sendLoop()
	for i := 0; i < 32; i++ {
		if err := c.Send([]byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	reviewWait(t, func() bool { mu.Lock(); defer mu.Unlock(); return originals == 16 }, "acceptance slots not full")
	time.Sleep(60 * time.Millisecond)
	mu.Lock()
	n := retries
	bound := maxActive
	mu.Unlock()
	if n != 0 || bound != 16 {
		t.Fatalf("timer bypassed full acceptance budget: retries=%d peak=%d", n, bound)
	}
	oncePartial.Do(func() { close(partial) })
	reviewWait(t, func() bool { mu.Lock(); defer mu.Unlock(); return retries > 0 }, "timer did not retry after partial release")
	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	select {
	case err := <-done:
		t.Fatalf("Close skipped remaining acceptance: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if closeWire.Load() {
		t.Fatal("wire CLOSE skipped acceptance")
	}
	onceRest.Do(func() { close(rest) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close stalled after release")
	}
	mu.Lock()
	bound = maxActive
	mu.Unlock()
	if bound > 16 {
		t.Fatalf("DATA acceptance budget regressed: %d", bound)
	}
}
