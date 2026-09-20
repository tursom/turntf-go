package turntf

import (
	"bytes"
	"errors"
	"testing"
)

func testStreamID() StreamID {
	return StreamID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
}

func TestStreamFrameBinaryRoundTrip(t *testing.T) {
	want := StreamFrame{Kind: StreamFrameData, ID: testStreamID(), Epoch: 3, Offset: 17, Window: 4096, Payload: []byte("payload")}
	encoded, err := want.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalStreamFrame(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != want.Kind || got.ID != want.ID || got.Epoch != want.Epoch || got.Offset != want.Offset || got.Window != want.Window || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("unexpected round trip: %+v", got)
	}
}

func TestStreamSenderWindowAndCumulativeAck(t *testing.T) {
	s := NewStreamSenderState(testStreamID(), 1, 5)
	first, err := s.Data([]byte("abc"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Offset != 0 {
		t.Fatalf("unexpected first offset %d", first.Offset)
	}
	if _, err := s.Data([]byte("def")); !errors.Is(err, ErrStreamWindowFull) {
		t.Fatalf("expected full window, got %v", err)
	}
	if err := s.Acknowledge(1, 3, 8); err != nil {
		t.Fatal(err)
	}
	second, err := s.Data([]byte("def"))
	if err != nil {
		t.Fatal(err)
	}
	if second.Offset != 3 {
		t.Fatalf("unexpected second offset %d", second.Offset)
	}
}

func TestStreamReceiverDeduplicatesOverlapAndDrainsOldEpoch(t *testing.T) {
	id := testStreamID()
	r := NewStreamReceiverState(id, 1, 1024)
	payload, ack, err := r.Accept(StreamFrame{Kind: StreamFrameData, ID: id, Epoch: 1, Offset: 0, Payload: []byte("abcdef")})
	if err != nil || string(payload) != "abcdef" || ack.Offset != 6 {
		t.Fatalf("first accept: payload=%q ack=%+v err=%v", payload, ack, err)
	}
	payload, ack, err = r.Accept(StreamFrame{Kind: StreamFrameData, ID: id, Epoch: 1, Offset: 3, Payload: []byte("defghi")})
	if err != nil || string(payload) != "ghi" || ack.Offset != 9 {
		t.Fatalf("overlap accept: payload=%q ack=%+v err=%v", payload, ack, err)
	}
	resumeAck, err := r.Resume(2, 6)
	if err != nil {
		t.Fatal(err)
	}
	if resumeAck.Offset != 9 {
		t.Fatalf("resume acknowledgement offset = %d, want 9", resumeAck.Offset)
	}
	if _, _, err := r.Accept(StreamFrame{Kind: StreamFrameData, ID: id, Epoch: 1, Offset: 9, Payload: []byte("old")}); !errors.Is(err, ErrStreamEpoch) {
		t.Fatalf("expected old path drain, got %v", err)
	}
	payload, ack, err = r.Accept(StreamFrame{Kind: StreamFrameData, ID: id, Epoch: 2, Offset: 9, Payload: []byte("new")})
	if err != nil || string(payload) != "new" || ack.Offset != 12 || ack.Epoch != 2 {
		t.Fatalf("resumed accept: payload=%q ack=%+v err=%v", payload, ack, err)
	}
}

func TestStreamReceiverEnforcesCredit(t *testing.T) {
	id := testStreamID()
	r := NewStreamReceiverState(id, 1, 4)
	if _, _, err := r.Accept(StreamFrame{Kind: StreamFrameData, ID: id, Epoch: 1, Offset: 0, Payload: []byte("12345")}); !errors.Is(err, ErrStreamCredit) {
		t.Fatalf("expected credit error, got %v", err)
	}
}
func TestStreamReceiverRejectsResumeAheadOfAcceptedOffset(t *testing.T) {
	id := testStreamID()
	r := NewStreamReceiverState(id, 1, 1024)
	if _, err := r.Resume(2, 1); err == nil {
		t.Fatal("expected resume ahead of accepted offset to fail")
	}
}

func TestStreamSenderResumeRetransmitsUnacknowledgedSuffix(t *testing.T) {
	id := testStreamID()
	s := NewStreamSenderState(id, 1, 1024)
	if _, err := s.Data([]byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	frames, err := s.Resume(2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || frames[0].Epoch != 2 || frames[0].Offset != 3 || string(frames[0].Payload) != "def" {
		t.Fatalf("unexpected resume frames: %+v", frames)
	}
}

func TestStreamReceiverRejectsGapAndStaleEpoch(t *testing.T) {
	id := testStreamID()
	r := NewStreamReceiverState(id, 2, 1024)
	if _, _, err := r.Accept(StreamFrame{Kind: StreamFrameData, ID: id, Epoch: 2, Offset: 1, Payload: []byte("x")}); !errors.Is(err, ErrStreamGap) {
		t.Fatalf("expected gap, got %v", err)
	}
	if _, _, err := r.Accept(StreamFrame{Kind: StreamFrameData, ID: id, Epoch: 1, Offset: 0, Payload: []byte("x")}); !errors.Is(err, ErrStreamEpoch) {
		t.Fatalf("expected stale epoch, got %v", err)
	}
}
