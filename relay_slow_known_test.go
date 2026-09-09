package turntf

import (
	"context"
	pb "github.com/tursom/turntf-go/internal/proto"
	"testing"
	"time"
)

// Regression: a slow Relay must not block the shared WebSocket reader.
func TestKnownSlowRelayBlocksSharedRPC(t *testing.T) {
	sent := make(chan struct{})
	client, conn := perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
		for i := uint64(1); i <= 65; i++ {
			b, _ := encodeRelayEnvelope(&RelayEnvelope{RelayID: "test-relay", Kind: RelayKindData, Seq: i, Payload: make([]byte, 32768)})
			send(&pb.ServerEnvelope{Body: &pb.ServerEnvelope_PacketPushed{PacketPushed: &pb.PacketPushed{Packet: &pb.Packet{Body: b}}}})
		}
		send(perfAccepted(req))
		close(sent)
	})
	// Avoid generating ACK RPCs back to this single-purpose injector.
	conn.sendEnvelope = func(*RelayEnvelope) error { return nil }
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		client.SendPacket(ctx, SendPacketInput{Target: UserRef{NodeID: 2, UserID: 2}, Body: []byte("inject"), DeliveryMode: DeliveryModeBestEffort})
	}()
	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("injection did not reach WebSocket")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	start := time.Now()
	err := client.Ping(ctx)
	cancel()
	t.Logf("65th undrained relay frame: Ping=%v elapsed=%s", err, time.Since(start))
	conn.Abort(nil)
	<-done
	ctx, cancel = context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if recovery := client.Ping(ctx); recovery != nil {
		t.Errorf("Ping failed after abort: %v", recovery)
	}
	if err != nil {
		t.Errorf("slow relay blocked independent control RPC: %v", err)
	}
}
