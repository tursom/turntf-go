package turntf

import (
	"context"
	"errors"
	pb "github.com/tursom/turntf-go/internal/proto"
	"sync/atomic"
	"testing"
	"time"
)

func TestRelayTransientAcceptanceRecovery(t *testing.T) {
	for _, kind := range []RelayKind{RelayKindData, RelayKindAck} {
		name := "data"
		if kind == RelayKindAck {
			name = "ack"
		}
		t.Run(name, func(t *testing.T) {
			var attempts atomic.Int32
			client, c := perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
				if env.Kind == kind && attempts.Add(1) == 1 {
					send(&pb.ServerEnvelope{Body: &pb.ServerEnvelope_Error{Error: &pb.Error{RequestId: req.RequestId, Code: "service_unavailable", Message: "session lookup timed out"}}})
					return
				}
				send(perfAccepted(req))
				if env.Kind == RelayKindData {
					send(perfAck(env))
				}
			})
			c.config.AckTimeoutMs = 15
			c.wg.Add(1)
			go c.sendLoop()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if kind == RelayKindData {
				if err := c.Send([]byte("must survive lookup timeout")); err != nil {
					t.Fatal(err)
				}
				if err := c.Flush(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				c.queueACK(&RelayEnvelope{RelayID: c.relayID, Kind: RelayKindAck, SenderSession: c.mySession, TargetSession: c.remoteSession, AckSeq: 1})
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if attempts.Load() != 2 {
				t.Fatalf("attempts=%d", attempts.Load())
			}
			dispatchPing(t, client)
		})
	}
}

func TestRelayACKRetryBoundAndFatalErrors(t *testing.T) {
	for _, code := range []string{"service_unavailable", "forbidden", "not_found"} {
		t.Run(code, func(t *testing.T) {
			c := newTestRelayConnection()
			defer c.Abort(nil)
			c.config.AckTimeoutMs = 10
			c.config.MaxRetransmits = 2
			var attempts atomic.Int32
			failure := &ServerError{Code: code}
			c.sendEnvelope = func(*RelayEnvelope) error { attempts.Add(1); return failure }
			c.queueACK(&RelayEnvelope{Kind: RelayKindAck, AckSeq: 1})
			select {
			case <-c.closeCh:
			case <-time.After(time.Second):
				t.Fatal("unbounded retry")
			}
			want := int32(1)
			if code == "service_unavailable" {
				want = 3
			}
			if attempts.Load() != want {
				t.Fatalf("attempts=%d want=%d", attempts.Load(), want)
			}
			if !errors.Is(c.closeResult(), failure) {
				t.Fatalf("lost cause: %v", c.closeResult())
			}
			c.wg.Wait()
		})
	}
}

func TestRelayACKRetryRespectsCloseDeadline(t *testing.T) {
	c := newTestRelayConnection()
	defer c.Abort(nil)
	c.config.AckTimeoutMs = 100
	c.config.CloseTimeoutMs = 20
	var attempts, closes atomic.Int32
	c.sendEnvelope = func(env *RelayEnvelope) error {
		if env.Kind == RelayKindClose {
			closes.Add(1)
			return nil
		}
		attempts.Add(1)
		return &ServerError{Code: "service_unavailable"}
	}
	c.wg.Add(1)
	go c.sendLoop()
	c.queueACK(&RelayEnvelope{Kind: RelayKindAck, AckSeq: 1})
	err := c.Close()
	var relayErr *RelayError
	if !errors.As(err, &relayErr) || relayErr.Code != RelayErrorCloseTimeout {
		t.Fatalf("close deadline: %v", err)
	}
	c.wg.Wait()
	if attempts.Load() != 1 || closes.Load() != 0 {
		t.Fatalf("retry escaped cancellation or CLOSE overtook ACK: %d/%d", attempts.Load(), closes.Load())
	}
}

func TestRelayDATAUnavailableRetryBudget(t *testing.T) {
	for _, bestEffort := range []bool{false, true} {
		c := newTestRelayConnection()
		c.sendBase, c.nextSeq = 1, 1
		defer c.Abort(nil)
		c.config.AckTimeoutMs = 10
		c.config.MaxRetransmits = 2
		if bestEffort {
			c.config.Reliability = ReliabilityBestEffort
		}
		var attempts atomic.Int32
		failure := &ServerError{Code: "service_unavailable"}
		c.sendEnvelope = func(*RelayEnvelope) error { attempts.Add(1); return failure }
		c.wg.Add(1)
		go c.sendLoop()
		if err := c.Send([]byte("bounded")); err != nil {
			t.Fatal(err)
		}
		select {
		case <-c.closeCh:
		case <-time.After(time.Second):
			t.Fatal("unbounded DATA retries")
		}
		c.wg.Wait()
		if bestEffort {
			if attempts.Load() != 1 || !errors.Is(c.closeResult(), failure) {
				t.Fatal("best effort retried or lost cause")
			}
		} else {
			var relayErr *RelayError
			if attempts.Load() != 3 || !errors.As(c.closeResult(), &relayErr) || relayErr.Code != RelayErrorMaxRetransmit {
				t.Fatalf("bad DATA budget: %d %v", attempts.Load(), c.closeResult())
			}
		}
	}
}

func TestRelayReceivePreservesCloseCause(t *testing.T) {
	for _, timeout := range []time.Duration{0, time.Second} {
		c := newTestRelayConnection()
		failure := &ServerError{Code: "service_unavailable"}
		c.Abort(failure)
		for i := 0; i < 50; i++ {
			_, err := c.ReceiveTimeout(timeout)
			if !errors.Is(err, failure) {
				t.Fatalf("lost cause: %v", err)
			}
		}
	}
}
