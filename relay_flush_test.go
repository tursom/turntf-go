package turntf

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestFlushCancelledQueueLock(t *testing.T) {
	c := newTestRelayConnection()
	c.enqueueMu.Lock()
	defer c.enqueueMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := c.Flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Flush: %v", err)
	}
}

func startFlushRelay(t *testing.T, hook func(*RelayConnection, *RelayEnvelope) error) *RelayConnection {
	t.Helper()
	c := newTestRelayConnection()
	c.nextSeq, c.sendBase = 1, 1
	c.config.AckTimeoutMs = 5000
	c.sendEnvelope = func(e *RelayEnvelope) error { return hook(c, e) }
	c.wg.Add(1)
	go c.sendLoop()
	t.Cleanup(func() { c.Abort(errors.New("test complete")); c.wg.Wait() })
	return c
}
func assertFlushWaiting(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("premature Flush: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
}
func awaitFlush(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("Flush stuck")
		return nil
	}
}

func TestFlushAcceptanceACKAndFutureSend(t *testing.T) {
	first, future := make(chan struct{}), make(chan struct{})
	releaseFirst, releaseFuture := make(chan struct{}), make(chan struct{})
	defer close(releaseFuture)
	c := startFlushRelay(t, func(c *RelayConnection, e *RelayEnvelope) error {
		if e.Kind != RelayKindData {
			return nil
		}
		if e.Seq == 1 {
			close(first)
			select {
			case <-releaseFirst:
			case <-c.ctx.Done():
			}
		} else {
			close(future)
			select {
			case <-releaseFuture:
			case <-c.ctx.Done():
			}
		}
		return nil
	})
	_ = c.Send([]byte("before"))
	<-first
	done := make(chan error, 1)
	go func() { done <- c.Flush(context.Background()) }()
	// Wait until the send loop has taken the barrier and is blocked on DATA1 acceptance.
	for {
		if c.enqueueMu.TryLock() {
			empty := len(c.sendCh) == 0
			c.enqueueMu.Unlock()
			if empty {
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	assertFlushWaiting(t, done)
	c.handleAck(&RelayEnvelope{AckSeq: 1})
	assertFlushWaiting(t, done)
	_ = c.Send([]byte("future"))
	close(releaseFirst)
	<-future
	if err := awaitFlush(t, done); err != nil {
		t.Fatal(err)
	}
	if c.State() != RelayStateOpen {
		t.Fatal("Flush closed relay")
	}
	// A second independent barrier must wait for DATA2, not reuse the first completion.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := c.Flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second Flush: %v", err)
	}
}

func TestFlushWaitsForACKAfterAcceptance(t *testing.T) {
	sent := make(chan struct{})
	c := startFlushRelay(t, func(_ *RelayConnection, e *RelayEnvelope) error {
		if e.Kind == RelayKindData {
			close(sent)
		}
		return nil
	})
	_ = c.Send([]byte("data"))
	<-sent
	done := make(chan error, 1)
	go func() { done <- c.Flush(context.Background()) }()
	assertFlushWaiting(t, done)
	c.handleAck(&RelayEnvelope{AckSeq: 1})
	if err := awaitFlush(t, done); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := c.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFlushFailureAndCancellation(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "abort", true: "acceptance"}[failure], func(t *testing.T) {
			gate := make(chan struct{})
			want := errors.New("original failure")
			c := startFlushRelay(t, func(c *RelayConnection, e *RelayEnvelope) error {
				if e.Kind == RelayKindData {
					select {
					case <-gate:
					case <-c.ctx.Done():
					}
					if failure {
						return want
					}
				}
				return nil
			})
			_ = c.Send([]byte("data"))
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			if err := c.Flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			if c.State() != RelayStateOpen {
				t.Fatal("cancelled Flush aborted relay")
			}
			done := make(chan error, 1)
			go func() { done <- c.Flush(context.Background()) }()
			if failure {
				close(gate)
			} else {
				c.Abort(want)
			}
			if err := awaitFlush(t, done); !errors.Is(err, want) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestConcurrentSendFlushClose(t *testing.T) {
	c := startFlushRelay(t, func(c *RelayConnection, e *RelayEnvelope) error {
		if e.Kind == RelayKindData {
			c.handleAck(&RelayEnvelope{AckSeq: e.Seq})
		}
		return nil
	})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 8; j++ {
				_ = c.Send([]byte("data"))
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_ = c.Flush(ctx)
				cancel()
			}
		}()
	}
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	wg.Wait()
	if err := awaitFlush(t, closed); err != nil {
		t.Fatal(err)
	}
}

func TestFlushFullQueueContext(t *testing.T) {
	c := newTestRelayConnection()
	defer c.Abort(errors.New("done"))
	_ = c.Send([]byte("full"))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := c.Flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
