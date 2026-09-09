package turntf

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/tursom/turntf-go/internal/proto"
)

func reviewWait(t *testing.T, f func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatal(message)
		}
		time.Sleep(time.Millisecond)
	}
}

// The peer models a larger sender window, including pipeline write reordering
// and retransmission of all unacknowledged frames while the reader is paused.
func TestRelayWSReviewDifferentWindows(t *testing.T) {
	for _, pair := range [][2]int{{32, 16}, {16, 1}, {256, 16}} {
		t.Run(fmt.Sprint(pair), func(t *testing.T) {
			var ack atomic.Uint64
			client, c := perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
				if env != nil && env.Kind == RelayKindAck {
					ack.Store(env.AckSeq)
					send(perfAccepted(req))
					return
				}
				start := ack.Load() + 1
				// A historical retransmit must not consume receive credit.
				if start > 1 {
					dispatchPush(send, "test-relay", RelayKindData, 1)
				}
				for seq := start + uint64(pair[0]) - 1; seq >= start; seq-- {
					dispatchPush(send, "test-relay", RelayKindData, seq)
				}
				send(perfAccepted(req))
			})
			c.config.WindowSize = pair[1]
			inject := func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if _, err := client.SendPacket(ctx, SendPacketInput{Target: UserRef{NodeID: 2, UserID: 2}, Body: []byte("inject"), DeliveryMode: DeliveryModeBestEffort}); err != nil {
					t.Fatal(err)
				}
				dispatchPing(t, client)
				if c.State() != RelayStateOpen {
					t.Fatalf("legal window %v closed: %v", pair, c.closeResult())
				}
			}
			for ack.Load() < 64 {
				inject()
				time.Sleep(3 * time.Millisecond)
			}
			for i := 0; i < 4; i++ {
				inject()
			}
			if ack.Load() > 64 {
				t.Fatalf("ACK passed paused reader: %d", ack.Load())
			}
			for i := 1; i <= 96; i++ {
				deadline := time.Now().Add(2 * time.Second)
				for len(c.recvCh) == 0 {
					inject()
					if time.Now().After(deadline) {
						t.Fatalf("retransmit did not recover frame %d", i)
					}
					time.Sleep(time.Millisecond)
				}
				if b := <-c.recvCh; string(b) != fmt.Sprint(i) {
					t.Fatalf("frame %d: %q", i, b)
				}
			}
			reviewWait(t, func() bool { return ack.Load() >= 96 }, "final cumulative ACK missing")
			c.inboxMu.Lock()
			c.mu.Lock()
			owned := c.inboxData + len(c.recvCh) + len(c.recvBuf) + c.recvReady
			c.mu.Unlock()
			c.inboxMu.Unlock()
			if owned > 64+2*pair[1] {
				t.Fatalf("unbounded receive storage: %d", owned)
			}
		})
	}
}

func TestRelayWSReviewLocalCloseACKBarrier(t *testing.T) {
	for _, mode := range []string{"release", "failure", "timeout", "abort", "ack2-failure", "ack2-timeout", "ack2-abort", "close-timeout"} {
		t.Run(mode, func(t *testing.T) {
			held := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			held2 := make(chan struct{})
			release2 := make(chan struct{})
			var once2 sync.Once
			defer once2.Do(func() { close(release2) })
			var ackAccepted atomic.Uint64
			var wireClose atomic.Bool
			var orderViolation atomic.Bool
			client, c := perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
				if env != nil && env.Kind == RelayKindAck {
					if env.AckSeq == 1 {
						close(held)
						<-release
					}
					if env.AckSeq == 2 {
						close(held2)
						<-release2
					}
					if mode == "failure" || (mode == "ack2-failure" && env.AckSeq == 2) {
						send(&pb.ServerEnvelope{Body: &pb.ServerEnvelope_Error{Error: &pb.Error{RequestId: req.RequestId, Code: "ack_rejected", Message: "ACK acceptance rejected"}}})
						return
					}
					ackAccepted.Store(env.AckSeq)
					send(perfAccepted(req))
					return
				}
				if env != nil && env.Kind == RelayKindClose {
					wireClose.Store(true)
					if ackAccepted.Load() != 2 {
						orderViolation.Store(true)
					}
					if mode == "close-timeout" {
						<-release
						return
					}
					send(perfAccepted(req))
					return
				}
				seq := uint64(1)
				if string(req.Body) == "second" {
					seq = 2
				}
				dispatchPush(send, "test-relay", RelayKindData, seq)
				send(perfAccepted(req))
			})
			c.config.CloseTimeoutMs = 100
			c.wg.Add(1)
			go c.sendLoop()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := client.SendPacket(ctx, SendPacketInput{Target: UserRef{NodeID: 2, UserID: 2}, Body: []byte("inject"), DeliveryMode: DeliveryModeBestEffort})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-held:
			case <-ctx.Done():
				t.Fatal("ACK1 not held")
			}
			_, err = client.SendPacket(ctx, SendPacketInput{Target: UserRef{NodeID: 2, UserID: 2}, Body: []byte("second"), DeliveryMode: DeliveryModeBestEffort})
			if err != nil {
				t.Fatal(err)
			}
			reviewWait(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.ackPending != nil && c.ackPending.AckSeq == 2 }, "ACK2 not queued")
			done := make(chan error, 1)
			start := time.Now()
			go func() { done <- c.Close() }()
			select {
			case err := <-done:
				t.Fatalf("Close skipped ACK acceptance: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			if wireClose.Load() {
				t.Fatal("wire CLOSE passed pending receive ACK")
			}
			abortErr := errors.New("review abort")
			switch mode {
			case "release", "failure", "close-timeout", "ack2-failure", "ack2-timeout", "ack2-abort":
				once.Do(func() { close(release) })
			case "abort":
				c.Abort(abortErr)
			}
			if mode == "release" || mode == "close-timeout" || mode == "ack2-failure" || mode == "ack2-timeout" || mode == "ack2-abort" {
				select {
				case <-held2:
				case <-time.After(time.Second):
					t.Fatal("pending final ACK was canceled")
				}
				select {
				case err := <-done:
					t.Fatalf("Close skipped final ACK acceptance: %v", err)
				case <-time.After(10 * time.Millisecond):
				}
				if wireClose.Load() {
					t.Fatal("wire CLOSE preceded final ACK acceptance")
				}
				switch mode {
				case "release", "close-timeout", "ack2-failure":
					once2.Do(func() { close(release2) })
				case "ack2-abort":
					c.Abort(abortErr)
				}
			}
			select {
			case err := <-done:
				if mode == "release" {
					if err != nil {
						t.Fatal(err)
					}
				} else if mode == "abort" || mode == "ack2-abort" {
					if !errors.Is(err, abortErr) {
						t.Fatalf("abort cause: %v", err)
					}
				} else if mode == "failure" || mode == "ack2-failure" {
					var serverErr *ServerError
					if !errors.As(err, &serverErr) || serverErr.Code != "ack_rejected" {
						t.Fatalf("lost acceptance cause: %v", err)
					}
				} else {
					var relayErr *RelayError
					if !errors.As(err, &relayErr) || relayErr.Code != RelayErrorCloseTimeout {
						t.Fatalf("lost close timeout: %v", err)
					}
				}
				if time.Since(start) > 350*time.Millisecond {
					t.Fatal("CloseTimeout not overall bounded")
				}
			case <-time.After(400 * time.Millisecond):
				t.Fatal("Close exceeded total timeout")
			}
			if orderViolation.Load() {
				t.Fatal("last ACK acceptance did not precede wire CLOSE")
			}
			if mode != "release" && mode != "close-timeout" && wireClose.Load() {
				t.Fatal("failed ACK barrier emitted CLOSE")
			}
			once.Do(func() { close(release) })
			reviewWait(t, func() bool { client.pendingMu.Lock(); defer client.pendingMu.Unlock(); return len(client.pending) == 0 }, "pending RPC leak")
		})
	}
}
