package turntf

import (
	"context"
	"errors"
	pb "github.com/tursom/turntf-go/internal/proto"
	"sync"
	"testing"
	"time"
)

func TestFlushWSIndependentBarriersAndDisconnect(t *testing.T) {
	for _, disconnect := range []bool{false, true} {
		t.Run(map[bool]string{false: "ACK", true: "disconnect"}[disconnect], func(t *testing.T) {
			received := make(chan struct{})
			release := make(chan struct{})
			client, c := perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
				send(perfAccepted(req))
				close(received)
				<-release
				send(perfAck(env))
			})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			c.wg.Add(1)
			go c.sendLoop()
			_ = c.Send([]byte("flush over WS"))
			<-received
			results := make(chan error, 8)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			for i := 0; i < 8; i++ {
				go func() { results <- c.Flush(ctx) }()
			}
			assertFlushWaiting(t, results)
			if disconnect {
				client.Close()
				for i := 0; i < 8; i++ {
					if err := awaitFlush(t, results); err == nil || errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("disconnect: %v", err)
					}
				}
			} else {
				unblock()
				for i := 0; i < 8; i++ {
					if err := awaitFlush(t, results); err != nil {
						t.Fatal(err)
					}
				}
				if err := c.Flush(ctx); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestFlushBestEffortAndEmpty(t *testing.T) {
	c := startFlushRelay(t, func(_ *RelayConnection, _ *RelayEnvelope) error { return nil })
	if err := c.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Separate connection: configuration is immutable once workers start.
	b := newTestRelayConnection()
	b.config.Reliability = ReliabilityBestEffort
	b.sendEnvelope = func(*RelayEnvelope) error { return nil }
	b.wg.Add(1)
	go b.sendLoop()
	defer func() { b.Abort(nil); b.wg.Wait() }()
	_ = b.Send([]byte("no ACK in best effort"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}
