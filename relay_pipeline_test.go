package turntf

import (
	pb "github.com/tursom/turntf-go/internal/proto"
	"sync"
	"testing"
	"time"
)

func TestPerfWindowPipelinesAcceptance(t *testing.T) {
	var mu sync.Mutex
	seen := make(map[uint64]bool)
	var contiguous uint64
	done := make(chan struct{})
	_, conn := perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
		time.Sleep(80 * time.Millisecond)
		send(perfAccepted(req))
		mu.Lock()
		seen[env.Seq] = true
		for seen[contiguous+1] {
			contiguous++
		}
		ack := *env
		ack.Seq = contiguous
		send(perfAck(&ack))
		if contiguous == 32 {
			close(done)
			contiguous++
		}
		mu.Unlock()
	})
	conn.wg.Add(1)
	go conn.sendLoop()
	start := time.Now()
	for i := 0; i < 32; i++ {
		if err := conn.Send(make([]byte, 32768)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-done:
		t.Logf("32x32KiB accepted+ACK at 80ms RTT: %s", time.Since(start))
	case <-time.After(600 * time.Millisecond):
		t.Error("16-frame reliable window behaves as stop-and-wait (>600ms)")
	}
}
