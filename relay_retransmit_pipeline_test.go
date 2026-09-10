package turntf

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func retryTestConnection(t *testing.T, window int, hook func(*RelayConnection, *RelayEnvelope, int) error) *RelayConnection {
	t.Helper()
	c := newTestRelayConnection()
	c.sendBase, c.nextSeq = 1, 1
	c.config.WindowSize = window
	c.config.AckTimeoutMs = 20
	c.sendCh = make(chan relaySendItem, 128)
	var mu sync.Mutex
	attempts := make(map[uint64]int)
	c.sendEnvelope = func(e *RelayEnvelope) error {
		mu.Lock()
		attempts[e.Seq]++
		n := attempts[e.Seq]
		mu.Unlock()
		return hook(c, e, n)
	}
	c.wg.Add(1)
	go c.sendLoop()
	t.Cleanup(func() { c.Abort(errors.New("test finished")); c.wg.Wait() })
	return c
}
func queueRetryTestFrames(t *testing.T, c *RelayConnection, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := c.Send([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
}
func awaitRetryFrames(t *testing.T, ch <-chan uint64, n int) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for i := 0; i < n; i++ {
		select {
		case <-ch:
		case <-deadline.C:
			t.Fatalf("only %d/%d retransmissions started before acceptance was released", i, n)
		}
	}
}

func TestRelayRetransmissionsPipelineAcceptance(t *testing.T) {
	started := make(chan uint64, 16)
	c := retryTestConnection(t, 16, func(c *RelayConnection, e *RelayEnvelope, n int) error {
		if n == 1 {
			return nil
		}
		started <- e.Seq
		<-c.ctx.Done()
		return c.ctx.Err()
	})
	queueRetryTestFrames(t, c, 16)
	awaitRetryFrames(t, started, 16)
}

func TestRelayACKProgressStartsDataBeforeRetryBatchFinishes(t *testing.T) {
	retries := make(chan uint64, 16)
	releaseFirst := make(chan struct{})
	next := make(chan struct{}, 1)
	var active, maxActive atomic.Int32
	c := retryTestConnection(t, 16, func(c *RelayConnection, e *RelayEnvelope, n int) error {
		current := active.Add(1)
		defer active.Add(-1)
		for old := maxActive.Load(); current > old && !maxActive.CompareAndSwap(old, current); old = maxActive.Load() {
		}
		if e.Seq == 17 {
			next <- struct{}{}
			c.handleAck(&RelayEnvelope{AckSeq: 17})
			return nil
		}
		if n == 1 {
			return nil
		}
		retries <- e.Seq
		if e.Seq == 1 {
			select {
			case <-releaseFirst:
				return nil
			case <-c.ctx.Done():
				return c.ctx.Err()
			}
		}
		<-c.ctx.Done()
		return c.ctx.Err()
	})
	queueRetryTestFrames(t, c, 17)
	awaitRetryFrames(t, retries, 16)
	c.handleAck(&RelayEnvelope{AckSeq: 16})
	close(releaseFirst)
	select {
	case <-next:
	case <-time.After(time.Second):
		t.Fatal("new DATA waits for obsolete retry RPCs despite a free window and RPC slot")
	}
	if got := maxActive.Load(); got > 16 {
		t.Fatalf("initial DATA and retransmissions exceeded shared RPC budget: %d", got)
	}
}

func TestRelaySkipsACKedRetriesWaitingForRPCSlot(t *testing.T) {
	retries := make(chan uint64, 64)
	release := make(chan struct{})
	next := make(chan struct{}, 1)
	c := retryTestConnection(t, 32, func(c *RelayConnection, e *RelayEnvelope, n int) error {
		if e.Seq == 33 {
			next <- struct{}{}
			c.handleAck(&RelayEnvelope{AckSeq: 33})
			return nil
		}
		if n == 1 {
			return nil
		}
		retries <- e.Seq
		select {
		case <-release:
			return nil
		case <-c.ctx.Done():
			return c.ctx.Err()
		}
	})
	queueRetryTestFrames(t, c, 33)
	awaitRetryFrames(t, retries, 16)
	// The remainder of this snapshot is waiting for an RPC slot. A cumulative
	// ACK retires it before the slots become available, so none may be issued.
	c.handleAck(&RelayEnvelope{AckSeq: 32})
	close(release)
	select {
	case <-next:
	case <-time.After(time.Second):
		t.Fatal("next DATA did not start")
	}
	c.Abort(errors.New("test complete"))
	c.wg.Wait()
	select {
	case seq := <-retries:
		t.Fatalf("retransmitted ACKed snapshot frame %d", seq)
	default:
	}
}
