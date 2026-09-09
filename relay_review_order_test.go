package turntf

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/tursom/turntf-go/internal/proto"
)

func TestRelayWSReviewACKCannotPassTerminal(t *testing.T) {
	for _, terminal := range []RelayKind{RelayKindClose, RelayKindError} {
		t.Run(fmt.Sprint(terminal), func(t *testing.T) {
			client, c := perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
				if env != nil && env.Kind == RelayKindAck {
					send(perfAccepted(req))
					return
				}
				for seq := uint64(1); seq <= 65; seq++ {
					dispatchPush(send, "test-relay", RelayKindData, seq)
				}
				dispatchPush(send, "test-relay", terminal, 0)
				dispatchPush(send, "test-relay", RelayKindAck, 1)
				send(perfAccepted(req))
			})
			c.sendBase, c.nextSeq = 1, 2
			c.unacked[1] = unackedFrame{data: []byte("outgoing")}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := client.SendPacket(ctx, SendPacketInput{Target: UserRef{NodeID: 2, UserID: 2}, Body: []byte("inject"), DeliveryMode: DeliveryModeBestEffort}); err != nil {
				t.Fatal(err)
			}
			dispatchPing(t, client)
			c.mu.Lock()
			base := c.sendBase
			c.mu.Unlock()
			if base != 1 {
				t.Fatal("ACK fast path passed admitted terminal")
			}
			dispatchDrain(t, c, 65)
			select {
			case <-c.closeCh:
			case <-ctx.Done():
				t.Fatal("terminal missing")
			}
			c.mu.Lock()
			base = c.sendBase
			c.mu.Unlock()
			if base != 1 {
				t.Fatal("post-terminal ACK applied")
			}
		})
	}
}

func TestRelayReviewCloseDeadlineIncludesBlockedSend(t *testing.T) {
	c := newTestRelayConnection()
	defer func() { c.Abort(nil); c.wg.Wait() }()
	c.config.CloseTimeoutMs = 50
	callbackRelease := make(chan struct{})
	defer close(callbackRelease)
	c.OnClose(func(error) { <-callbackRelease })
	c.config.SendTimeoutMs = 0
	c.sendCh = make(chan relaySendItem)
	sent := make(chan error, 1)
	go func() { sent <- c.Send([]byte("blocked")) }()
	reviewWait(t, func() bool {
		if c.enqueueMu.TryLock() {
			c.enqueueMu.Unlock()
			return false
		}
		return true
	}, "Send did not hold admission lock")
	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	select {
	case err := <-done:
		if e, ok := err.(*RelayError); !ok || e.Code != RelayErrorCloseTimeout {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Close deadline excluded blocked Send")
	}
	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("Send did not cancel")
	}
}
