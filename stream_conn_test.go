package turntf

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type streamMuxPair struct {
	a *StreamMux
	b *StreamMux
}

func newStreamMuxPair(t *testing.T, cfg *StreamMuxConfig) streamMuxPair {
	t.Helper()
	var a, b *StreamMux
	var err error
	a, err = NewStreamMux(func(ctx context.Context, frame StreamFrame) error {
		return b.HandleFrame(ctx, frame)
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, err = NewStreamMux(func(ctx context.Context, frame StreamFrame) error {
		return a.HandleFrame(ctx, frame)
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})
	return streamMuxPair{a: a, b: b}
}

func dialAndAcceptStream(t *testing.T, pair streamMuxPair) (*StreamConn, *StreamConn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	dialed, err := pair.a.DialID(ctx, testStreamID())
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := pair.b.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if dialed.ID() != accepted.ID() {
		t.Fatalf("stream IDs differ: %v != %v", dialed.ID(), accepted.ID())
	}
	return dialed, accepted
}

func TestStreamMuxDialAcceptAndDuplexIO(t *testing.T) {
	pair := newStreamMuxPair(t, nil)
	a, b := dialAndAcceptStream(t, pair)

	if n, err := a.Write([]byte("request")); err != nil || n != len("request") {
		t.Fatalf("Write request = (%d, %v)", n, err)
	}
	request := make([]byte, len("request"))
	if _, err := io.ReadFull(b, request); err != nil || string(request) != "request" {
		t.Fatalf("Read request = (%q, %v)", request, err)
	}
	if n, err := b.Write([]byte("response")); err != nil || n != len("response") {
		t.Fatalf("Write response = (%d, %v)", n, err)
	}
	response := make([]byte, len("response"))
	if _, err := io.ReadFull(a, response); err != nil || string(response) != "response" {
		t.Fatalf("Read response = (%q, %v)", response, err)
	}
}

func TestStreamConnReadReleasesZeroWindowWriter(t *testing.T) {
	pair := newStreamMuxPair(t, &StreamMuxConfig{Window: 4, MaxFrameSize: 4})
	a, b := dialAndAcceptStream(t, pair)

	writeDone := make(chan error, 1)
	go func() {
		_, err := a.Write([]byte("abcdefgh"))
		writeDone <- err
	}()

	first := make([]byte, 4)
	if _, err := io.ReadFull(b, first); err != nil || string(first) != "abcd" {
		t.Fatalf("first Read = (%q, %v)", first, err)
	}
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("writer remained blocked after Read advertised credit")
	}
	second := make([]byte, 4)
	if _, err := io.ReadFull(b, second); err != nil || string(second) != "efgh" {
		t.Fatalf("second Read = (%q, %v)", second, err)
	}
}

func TestStreamConnResumeRetransmitsFailedWrite(t *testing.T) {
	var a, b *StreamMux
	var drop atomic.Bool
	drop.Store(true)
	pathErr := errors.New("path failed")
	var err error
	a, err = NewStreamMux(func(ctx context.Context, frame StreamFrame) error {
		if frame.Kind == StreamFrameData && drop.CompareAndSwap(true, false) {
			return pathErr
		}
		return b.HandleFrame(ctx, frame)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err = NewStreamMux(func(ctx context.Context, frame StreamFrame) error {
		return a.HandleFrame(ctx, frame)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out, err := a.DialID(ctx, testStreamID())
	if err != nil {
		t.Fatal(err)
	}
	in, err := b.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("retained while path is down")
	if n, err := out.Write(payload); n != len(payload) || !errors.Is(err, pathErr) {
		t.Fatalf("failed Write = (%d, %v), want (%d, path error)", n, err, len(payload))
	}
	if err := out.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(in, got); err != nil || string(got) != string(payload) {
		t.Fatalf("resumed Read = (%q, %v)", got, err)
	}
}

func TestStreamConnDirectionsResumeIndependently(t *testing.T) {
	pair := newStreamMuxPair(t, nil)
	a, b := dialAndAcceptStream(t, pair)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := a.Resume(ctx); err != nil {
			t.Errorf("a Resume: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := b.Resume(ctx); err != nil {
			t.Errorf("b Resume: %v", err)
		}
	}()
	wg.Wait()

	if _, err := a.Write([]byte("a-to-b")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write([]byte("b-to-a")); err != nil {
		t.Fatal(err)
	}
	fromA := make([]byte, len("a-to-b"))
	fromB := make([]byte, len("b-to-a"))
	if _, err := io.ReadFull(b, fromA); err != nil || string(fromA) != "a-to-b" {
		t.Fatalf("b Read = (%q, %v)", fromA, err)
	}
	if _, err := io.ReadFull(a, fromB); err != nil || string(fromB) != "b-to-a" {
		t.Fatalf("a Read = (%q, %v)", fromB, err)
	}
}

func TestStreamConnDeduplicatesRetransmittedData(t *testing.T) {
	var a, b *StreamMux
	var captured StreamFrame
	var err error
	a, err = NewStreamMux(func(ctx context.Context, frame StreamFrame) error {
		if frame.Kind == StreamFrameData {
			captured = frame
		}
		return b.HandleFrame(ctx, frame)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err = NewStreamMux(func(ctx context.Context, frame StreamFrame) error {
		return a.HandleFrame(ctx, frame)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out, err := a.DialID(ctx, testStreamID())
	if err != nil {
		t.Fatal(err)
	}
	in, err := b.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := out.Write([]byte("once")); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleFrame(ctx, captured); err != nil {
		t.Fatal(err)
	}
	in.mu.Lock()
	buffered := string(in.readBuf)
	in.mu.Unlock()
	if buffered != "once" {
		t.Fatalf("buffered payload after duplicate = %q", buffered)
	}
}

func TestStreamConnResumeRecoversAfterLostAcknowledgement(t *testing.T) {
	var a, b *StreamMux
	var dropAck atomic.Bool
	dropAck.Store(true)
	var err error
	a, err = NewStreamMux(func(ctx context.Context, frame StreamFrame) error {
		return b.HandleFrame(ctx, frame)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err = NewStreamMux(func(ctx context.Context, frame StreamFrame) error {
		if frame.Kind == StreamFrameAck && frame.Offset > 0 && dropAck.CompareAndSwap(true, false) {
			return nil
		}
		return a.HandleFrame(ctx, frame)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out, err := a.DialID(ctx, testStreamID())
	if err != nil {
		t.Fatal(err)
	}
	in, err := b.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("ack was lost")
	if n, err := out.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write = (%d, %v)", n, err)
	}
	if err := out.Resume(ctx); err != nil {
		t.Fatalf("Resume after lost ACK: %v", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(in, got); err != nil || string(got) != string(payload) {
		t.Fatalf("Read = (%q, %v)", got, err)
	}
	in.mu.Lock()
	buffered := len(in.readBuf)
	in.mu.Unlock()
	if buffered != 0 {
		t.Fatalf("duplicate bytes remained buffered after resume: %d", buffered)
	}
}

func TestStreamConnIgnoresStalePathClose(t *testing.T) {
	pair := newStreamMuxPair(t, nil)
	a, b := dialAndAcceptStream(t, pair)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pair.b.HandleFrame(ctx, StreamFrame{Kind: StreamFrameClose, ID: a.ID(), Epoch: 1}); !errors.Is(err, ErrStreamEpoch) {
		t.Fatalf("stale Close error = %v, want ErrStreamEpoch", err)
	}
	if _, err := a.Write([]byte("still-open")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("still-open"))
	if _, err := io.ReadFull(b, got); err != nil || string(got) != "still-open" {
		t.Fatalf("Read after stale Close = (%q, %v)", got, err)
	}
}

func TestStreamConnRemoteCloseUnblocksRead(t *testing.T) {
	pair := newStreamMuxPair(t, nil)
	a, b := dialAndAcceptStream(t, pair)
	readDone := make(chan error, 1)
	go func() {
		var buf [1]byte
		_, err := b.Read(buf[:])
		readDone <- err
	}()
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	select {
	case err := <-readDone:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("Read error = %v, want EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Read was not released by remote Close")
	}
}

func TestStreamConnConcurrentWritersRace(t *testing.T) {
	pair := newStreamMuxPair(t, &StreamMuxConfig{Window: 256, MaxFrameSize: 31})
	a, b := dialAndAcceptStream(t, pair)

	const writers = 8
	const writesPerWriter = 40
	const bytesPerWrite = 17
	total := writers * writesPerWriter * bytesPerWrite
	readDone := make(chan []byte, 1)
	go func() {
		payload := make([]byte, total)
		_, err := io.ReadFull(b, payload)
		if err != nil {
			readDone <- nil
			return
		}
		readDone <- payload
	}()

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		value := byte('a' + i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := make([]byte, bytesPerWrite)
			for i := range payload {
				payload[i] = value
			}
			for j := 0; j < writesPerWriter; j++ {
				if n, err := a.Write(payload); err != nil || n != len(payload) {
					t.Errorf("Write = (%d, %v)", n, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	select {
	case payload := <-readDone:
		if len(payload) != total {
			t.Fatalf("received %d bytes, want %d", len(payload), total)
		}
		counts := make(map[byte]int)
		for _, value := range payload {
			counts[value]++
		}
		for i := 0; i < writers; i++ {
			value := byte('a' + i)
			if counts[value] != writesPerWriter*bytesPerWrite {
				t.Fatalf("received %d bytes of %q", counts[value], value)
			}
		}
	case <-time.After(3 * time.Second):
		t.Fatal("concurrent transfer timed out")
	}
}

func TestStreamMuxDialContextRemovesPendingStream(t *testing.T) {
	mux, err := NewStreamMux(func(context.Context, StreamFrame) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mux.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := mux.DialID(ctx, testStreamID()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Dial error = %v, want deadline", err)
	}
	if err := mux.HandleFrame(context.Background(), StreamFrame{Kind: StreamFrameAck, ID: testStreamID(), Epoch: 1}); !errors.Is(err, ErrStreamUnknown) {
		t.Fatalf("HandleFrame error = %v, want unknown stream", err)
	}
}
