package turntf

import (
	"bytes"
	"sync/atomic"
	"testing"
	"time"
)

func TestAbortDoesNotEnableRemoteDrain(t *testing.T) {
	for _, reason := range []error{
		&RelayError{Code: RelayErrorRemoteClose, Message: "caller supplied reason"},
		&RelayError{Code: RelayErrorProtocol, Message: "abort"},
	} {
		c := newTestRelayConnection()
		c.recvCh <- []byte("buffered")
		c.Abort(reason)
		got, err := c.receiveTerminal(reason)
		if got != nil || err != reason || len(c.recvCh) != 1 {
			t.Fatalf("Abort changed terminal semantics: got=%q err=%v queued=%d", got, err, len(c.recvCh))
		}
	}
}

func TestRemoteCloseDrainsAcceptedPrefix(t *testing.T) {
	for _, timeout := range []time.Duration{0, time.Second} {
		c := newTestRelayConnection()
		var ack atomic.Uint64
		c.sendEnvelope = func(env *RelayEnvelope) error {
			if env.Kind == RelayKindAck {
				ack.Store(env.AckSeq)
			}
			return nil
		}
		defer func() { c.Abort(nil); c.wg.Wait() }()
		// The consumer starts only after FIFO dispatch has ACKed the entire prefix
		// and processed CLOSE. No scheduler timing is used to arrange the race.
		for i := 1; i <= 32; i++ {
			c.enqueueEnvelope(&RelayEnvelope{Kind: RelayKindData, Seq: uint64(i), Payload: []byte{byte(i)}})
		}
		c.enqueueEnvelope(&RelayEnvelope{Kind: RelayKindData, Seq: 33, Payload: []byte("half_close")})
		c.enqueueEnvelope(&RelayEnvelope{Kind: RelayKindClose})
		select {
		case <-c.closeCh:
		case <-time.After(time.Second):
			t.Fatal("CLOSE not processed")
		}
		if ack.Load() != 33 {
			t.Fatalf("ACK watermark = %d", ack.Load())
		}
		if len(c.recvCh) != 33 {
			t.Fatalf("accepted prefix = %d", len(c.recvCh))
		}
		for i := 1; i <= 33; i++ {
			want := []byte{byte(i)}
			if i == 33 {
				want = []byte("half_close")
			}
			got, err := c.ReceiveTimeout(timeout)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("timeout=%v frame=%d: got %q err=%v", timeout, i, got, err)
			}
		}
		if _, err := c.ReceiveTimeout(timeout); err == nil {
			t.Fatal("missing terminal error")
		}
	}
}
