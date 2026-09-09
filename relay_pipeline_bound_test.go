package turntf

import (
	pb "github.com/tursom/turntf-go/internal/proto"
	"sync"
	"testing"
	"time"
)

func TestPerfAcceptanceBarrierAndBound(t *testing.T) {
	var mu sync.Mutex
	seen := map[uint64]bool{}
	var contiguous uint64
	count := 0
	full := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	_, conn := perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
		if env.Kind != RelayKindData {
			send(perfAccepted(req))
			return
		}
		mu.Lock()
		count++
		n := count
		seen[env.Seq] = true
		for seen[contiguous+1] {
			contiguous++
		}
		ack := *env
		ack.Seq = contiguous
		send(perfAck(&ack))
		mu.Unlock()
		if n == 16 {
			close(full)
		}
		<-release
		send(perfAccepted(req))
	})
	conn.wg.Add(1)
	go conn.sendLoop()
	for i := 0; i < 32; i++ {
		if err := conn.Send(make([]byte, 32768)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-full:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("window did not fill")
	}
	done := make(chan error, 1)
	go func() { done <- conn.Close() }()
	select {
	case err := <-done:
		t.Fatalf("Close passed unaccepted DATA: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	mu.Lock()
	n := count
	mu.Unlock()
	if n != 16 {
		t.Errorf("outstanding acceptance count=%d, limit=16", n)
	}
	once.Do(func() { close(release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after all acceptance responses and ACKs")
	}
	t.Log("early ACK does not bypass acceptance bound or CLOSE barrier")
}
