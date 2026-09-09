package turntf

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/tursom/turntf-go/internal/proto"
	"testing"
	"time"
)

func TestPerfWindowACKWakesSender(t *testing.T) {
	second := make(chan struct{})
	_, conn := perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
		send(perfAccepted(req))
		time.Sleep(20 * time.Millisecond)
		send(perfAck(env))
		if env.Seq == 2 {
			close(second)
		}
	})
	conn.config.WindowSize = 1
	conn.wg.Add(1)
	go conn.sendLoop()
	_ = conn.Send(make([]byte, 32768))
	_ = conn.Send(make([]byte, 32768))
	start := time.Now()
	select {
	case <-second:
		t.Logf("second frame after %s", time.Since(start))
	case <-time.After(150 * time.Millisecond):
		t.Error("ACK freed window but sender did not wake within 150ms")
	}
}

func TestPerfCanceledPendingCleanup(t *testing.T) {
	client, _ := perfPeer(t, func(*pb.SendMessageRequest, *RelayEnvelope, func(*pb.ServerEnvelope)) {})
	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		_, err := client.SendPacket(ctx, SendPacketInput{Target: UserRef{NodeID: 2, UserID: 2}, Body: []byte("ignored"), DeliveryMode: DeliveryModeBestEffort})
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected real RPC timeout, got %v", err)
		}
	}
	client.pendingMu.Lock()
	n := len(client.pending)
	client.pendingMu.Unlock()
	if n != 0 {
		t.Errorf("pending after 50 timeouts = %d", n)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	t.Log(fmt.Sprintf("pending after 50 timeouts=%d; shared Ping healthy", n))
}
