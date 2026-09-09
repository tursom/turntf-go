package turntf

import (
	"testing"
	"time"
)

func TestRelayReadyBatchCountsAgainstReceiveBudget(t *testing.T) {
	c := newTestRelayConnection()
	defer func() { c.Abort(nil); c.wg.Wait() }()
	c.sendEnvelope = func(*RelayEnvelope) error { return nil }
	for seq := uint64(1); seq <= 64; seq++ {
		c.handleData(&RelayEnvelope{Kind: RelayKindData, Seq: seq, Payload: []byte{byte(seq)}})
	}
	for seq := uint64(80); seq >= 65; seq-- {
		c.enqueueEnvelope(&RelayEnvelope{Kind: RelayKindData, Seq: seq, Payload: []byte{byte(seq)}})
	}
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		ready := c.recvReady
		c.mu.Unlock()
		if ready == 16 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reordered batch did not reach blocked delivery")
		}
		time.Sleep(time.Millisecond)
	}
	// The ready batch still owns 16 frames even though recvBuf is now empty.
	// Without ready accounting these additional frames silently exceed the budget.
	for seq := uint64(81); seq <= 97; seq++ {
		c.enqueueEnvelope(&RelayEnvelope{Kind: RelayKindData, Seq: seq, Payload: []byte{byte(seq)}})
	}
	// Mix historical and ready-batch retransmits with a larger peer window.
	// They must not consume credit or ACK the blocked ready batch.
	for round := 0; round < 8; round++ {
		for seq := uint64(1); seq <= 97; seq++ {
			c.enqueueEnvelope(&RelayEnvelope{Kind: RelayKindData, Seq: seq, Payload: []byte{byte(seq)}})
		}
	}
	c.inboxMu.Lock()
	c.mu.Lock()
	owned := c.inboxData + len(c.recvCh) + len(c.recvBuf) + c.recvReady
	c.mu.Unlock()
	c.inboxMu.Unlock()
	if owned > 96 {
		t.Fatalf("active ready batch escaped receive accounting: %d", owned)
	}
	if c.State() != RelayStateOpen {
		t.Fatalf("unacknowledged reliable DATA must recover, not overflow: %v", c.closeResult())
	}
	for seq := uint64(1); seq <= 97; seq++ {
		deadline := time.Now().Add(time.Second)
		for len(c.recvCh) == 0 {
			c.enqueueEnvelope(&RelayEnvelope{Kind: RelayKindData, Seq: seq, Payload: []byte{byte(seq)}})
			if time.Now().After(deadline) {
				t.Fatalf("missing recovered frame %d", seq)
			}
			time.Sleep(time.Millisecond)
		}
		if b := <-c.recvCh; len(b) != 1 || b[0] != byte(seq) {
			t.Fatalf("frame %d: %v", seq, b)
		}
	}
}
