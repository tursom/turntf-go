package turntf

import (
	"testing"
	"time"
)

func TestRelayLostACKRearmsUntilMaxRetransmits(t *testing.T) {
	c := newTestRelayConnection()
	defer c.Abort(nil)
	c.sendBase, c.nextSeq = 1, 1
	c.config.AckTimeoutMs = 10
	c.config.MaxRetransmits = 2
	c.sendEnvelope = func(*RelayEnvelope) error { return nil }
	c.wg.Add(1)
	go c.sendLoop()
	if err := c.Send([]byte("lost ack")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.closeCh:
		c.mu.Lock()
		err := c.closeErr
		c.mu.Unlock()
		if e, ok := err.(*RelayError); !ok || e.Code != RelayErrorMaxRetransmit {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("lost ACK stalled after first retransmission")
	}
	c.wg.Wait()
}

func TestRelayRejectsFutureACK(t *testing.T) {
	c := newTestRelayConnection()
	defer c.Abort(nil)
	c.sendBase, c.nextSeq = 1, 3
	c.unacked[1] = unackedFrame{data: []byte("one")}
	c.unacked[2] = unackedFrame{data: []byte("two")}
	for _, ack := range []uint64{0, 99, ^uint64(0)} {
		c.handleAck(&RelayEnvelope{AckSeq: ack})
		if c.sendBase != 1 || len(c.unacked) != 2 {
			t.Fatalf("ACK %d moved window: base=%d pending=%d", ack, c.sendBase, len(c.unacked))
		}
	}
	c.handleAck(&RelayEnvelope{AckSeq: 2})
	if c.sendBase != 3 || len(c.unacked) != 0 {
		t.Fatal("valid cumulative ACK not applied")
	}
	c.handleAck(&RelayEnvelope{AckSeq: 1})
	if c.sendBase != 3 || len(c.unacked) != 0 {
		t.Fatal("stale ACK changed completed window")
	}
}

func TestRelayLostACKWithFullWindow(t *testing.T) {
	c := newTestRelayConnection()
	defer c.Abort(nil)
	c.sendBase, c.nextSeq = 1, 1
	c.config.WindowSize = 1
	c.config.AckTimeoutMs = 10
	c.config.MaxRetransmits = 2
	c.sendEnvelope = func(*RelayEnvelope) error { return nil }
	c.wg.Add(1)
	go c.sendLoop()
	if err := c.Send([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := c.Send([]byte("waiting window")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.closeCh:
		c.mu.Lock()
		err := c.closeErr
		c.mu.Unlock()
		if e, ok := err.(*RelayError); !ok || e.Code != RelayErrorMaxRetransmit {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("full-window retry timer stalled")
	}
	c.wg.Wait()
}
