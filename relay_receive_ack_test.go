package turntf

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/tursom/turntf-go/internal/proto"
)

func TestRelayWSCloseWaitsReceiveACKAcceptance(t *testing.T) {
	for _, abort := range []bool{false, true} {
		name := "release"
		if abort {
			name = "abort"
		}
		t.Run(name, func(t *testing.T) {
			held := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			var calls atomic.Int32
			client, c := perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
				if env != nil && env.Kind == RelayKindAck {
					if calls.Add(1) == 1 {
						close(held)
					}
					<-release
					send(perfAccepted(req))
					return
				}
				for seq := uint64(1); seq <= 32; seq++ {
					dispatchPush(send, "test-relay", RelayKindData, seq)
				}
				dispatchPush(send, "test-relay", RelayKindClose, 0)
				send(perfAccepted(req))
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := client.SendPacket(ctx, SendPacketInput{Target: UserRef{NodeID: 2, UserID: 2}, Body: []byte("inject"), DeliveryMode: DeliveryModeBestEffort}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-held:
			case <-ctx.Done():
				t.Fatal("no receive ACK")
			}
			dispatchDrain(t, c, 32)
			dispatchPing(t, client)
			select {
			case <-c.closeCh:
				t.Fatal("remote CLOSE skipped pending receive ACK acceptance")
			case <-time.After(20 * time.Millisecond):
			}
			if calls.Load() != 1 {
				t.Fatalf("unbounded ACK RPCs: %d", calls.Load())
			}
			if abort {
				c.Abort(context.Canceled)
			} else {
				once.Do(func() { close(release) })
			}
			select {
			case <-c.closeCh:
			case <-ctx.Done():
				t.Fatal("terminal/abort did not finish")
			}
			c.wg.Wait()
			dispatchPing(t, client)
		})
	}
}
