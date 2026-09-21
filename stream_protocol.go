package turntf

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)

const (
	streamHeaderSize      = 49
	DefaultStreamWindow   = 2 << 20
	DefaultStreamMaxFrame = 128 << 10
)

var streamMagic = [4]byte{'T', 'T', 'S', 1}

var (
	ErrStreamWindowFull      = errors.New("stream send window is full")
	ErrStreamGap             = errors.New("stream data offset has a gap")
	ErrStreamEpoch           = errors.New("stream path epoch is stale or not resumed")
	ErrStreamCredit          = errors.New("stream receive window exceeded")
	ErrStreamSendPendingFull = errors.New("tracked stream send pending limit reached")
)

// StreamSendMetadata identifies the stream frame associated with a tracked send.
type StreamSendMetadata struct {
	Target        UserRef
	TargetSession SessionRef
	StreamID      StreamID
	Kind          StreamFrameKind
	Epoch         uint64
}

// StreamSendResult reports asynchronous completion of a tracked stream send.
type StreamSendResult struct {
	RequestID uint64
	Metadata  StreamSendMetadata
	Err       error
}

type StreamID [16]byte

func NewStreamID() (StreamID, error) {
	var id StreamID
	_, err := rand.Read(id[:])
	return id, err
}

type StreamFrameKind uint8

const (
	StreamFrameOpen StreamFrameKind = iota + 1
	StreamFrameOpenAck
	StreamFrameResume
	StreamFrameData
	StreamFrameAck
	StreamFrameClose
)

type StreamFrame struct {
	Kind    StreamFrameKind
	ID      StreamID
	Epoch   uint64
	Offset  uint64
	Window  uint64
	Payload []byte
}

func (f StreamFrame) MarshalBinary() ([]byte, error) {
	if f.Kind < StreamFrameOpen || f.Kind > StreamFrameClose {
		return nil, fmt.Errorf("invalid stream frame kind %d", f.Kind)
	}
	if len(f.Payload) > DefaultStreamMaxFrame {
		return nil, fmt.Errorf("stream payload exceeds %d bytes", DefaultStreamMaxFrame)
	}
	out := make([]byte, streamHeaderSize+len(f.Payload))
	copy(out[:4], streamMagic[:])
	out[4] = byte(f.Kind)
	copy(out[5:21], f.ID[:])
	binary.BigEndian.PutUint64(out[21:29], f.Epoch)
	binary.BigEndian.PutUint64(out[29:37], f.Offset)
	binary.BigEndian.PutUint64(out[37:45], f.Window)
	binary.BigEndian.PutUint32(out[45:49], uint32(len(f.Payload)))
	copy(out[49:], f.Payload)
	return out, nil
}

// IsStreamFrame reports whether data starts with the stream protocol magic.
func IsStreamFrame(data []byte) bool {
	return len(data) >= len(streamMagic) && string(data[:len(streamMagic)]) == string(streamMagic[:])
}

func UnmarshalStreamFrame(data []byte) (StreamFrame, error) {
	if len(data) < streamHeaderSize || !IsStreamFrame(data) {
		return StreamFrame{}, errors.New("invalid stream frame header")
	}
	kind := StreamFrameKind(data[4])
	if kind < StreamFrameOpen || kind > StreamFrameClose {
		return StreamFrame{}, fmt.Errorf("invalid stream frame kind %d", kind)
	}
	payloadLen := int(binary.BigEndian.Uint32(data[45:49]))
	if payloadLen > DefaultStreamMaxFrame || len(data) != streamHeaderSize+payloadLen {
		return StreamFrame{}, errors.New("invalid stream payload length")
	}
	var id StreamID
	copy(id[:], data[5:21])
	return StreamFrame{Kind: kind, ID: id, Epoch: binary.BigEndian.Uint64(data[21:29]), Offset: binary.BigEndian.Uint64(data[29:37]), Window: binary.BigEndian.Uint64(data[37:45]), Payload: append([]byte(nil), data[49:]...)}, nil
}

type StreamSenderState struct {
	mu      sync.Mutex
	id      StreamID
	epoch   uint64
	next    uint64
	acked   uint64
	window  uint64
	pending []StreamFrame
}

func NewStreamSenderState(id StreamID, epoch, window uint64) *StreamSenderState {
	if window == 0 {
		window = DefaultStreamWindow
	}
	return &StreamSenderState{id: id, epoch: epoch, window: window}
}

func (s *StreamSenderState) Data(payload []byte) (StreamFrame, error) {
	if len(payload) == 0 || len(payload) > DefaultStreamMaxFrame {
		return StreamFrame{}, errors.New("invalid stream data payload")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next-s.acked+uint64(len(payload)) > s.window {
		return StreamFrame{}, ErrStreamWindowFull
	}
	frame := StreamFrame{Kind: StreamFrameData, ID: s.id, Epoch: s.epoch, Offset: s.next, Payload: append([]byte(nil), payload...)}
	s.next += uint64(len(payload))
	s.pending = append(s.pending, frame)
	return frame, nil
}

func (s *StreamSenderState) Acknowledge(epoch, offset, window uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if epoch != s.epoch {
		return ErrStreamEpoch
	}
	if offset < s.acked || offset > s.next {
		return errors.New("invalid cumulative stream acknowledgement")
	}
	s.epoch, s.acked = epoch, offset
	if window > 0 {
		s.window = window
	}
	kept := s.pending[:0]
	for _, frame := range s.pending {
		if frame.Offset+uint64(len(frame.Payload)) > offset {
			kept = append(kept, frame)
		}
	}
	s.pending = kept
	return nil
}

func (s *StreamSenderState) Resume(epoch, acknowledgedOffset uint64) ([]StreamFrame, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if epoch <= s.epoch || acknowledgedOffset < s.acked || acknowledgedOffset > s.next {
		return nil, errors.New("invalid stream resume state")
	}
	s.epoch, s.acked = epoch, acknowledgedOffset
	frames := make([]StreamFrame, 0, len(s.pending))
	for _, old := range s.pending {
		end := old.Offset + uint64(len(old.Payload))
		if end <= acknowledgedOffset {
			continue
		}
		start := acknowledgedOffset
		if start < old.Offset {
			start = old.Offset
		}
		frames = append(frames, StreamFrame{Kind: StreamFrameData, ID: s.id, Epoch: epoch, Offset: start, Payload: append([]byte(nil), old.Payload[start-old.Offset:]...)})
	}
	s.pending = append(s.pending[:0], frames...)
	return append([]StreamFrame(nil), frames...), nil
}

type StreamReceiverState struct {
	mu                    sync.Mutex
	id                    StreamID
	epoch, offset, window uint64
}

func NewStreamReceiverState(id StreamID, epoch, window uint64) *StreamReceiverState {
	if window == 0 {
		window = DefaultStreamWindow
	}
	return &StreamReceiverState{id: id, epoch: epoch, window: window}
}

func (r *StreamReceiverState) Resume(epoch, offset uint64) (StreamFrame, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if epoch <= r.epoch || offset > r.offset {
		return StreamFrame{}, errors.New("invalid stream receiver resume state")
	}
	// The peer's cumulative acknowledgement may lag behind bytes already
	// accepted here when an ACK was lost with the old path. Keep the local
	// offset and acknowledge it so the sender can discard that pending prefix.
	r.epoch = epoch
	return StreamFrame{Kind: StreamFrameAck, ID: r.id, Epoch: r.epoch, Offset: r.offset, Window: r.window}, nil
}

func (r *StreamReceiverState) Accept(frame StreamFrame) ([]byte, StreamFrame, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if frame.ID != r.id || frame.Kind != StreamFrameData {
		return nil, StreamFrame{}, errors.New("stream frame does not belong to receiver")
	}
	if frame.Epoch != r.epoch {
		return nil, StreamFrame{}, ErrStreamEpoch
	}
	if frame.Offset > r.offset {
		return nil, StreamFrame{}, ErrStreamGap
	}
	end := frame.Offset + uint64(len(frame.Payload))
	if end > r.offset+r.window {
		return nil, StreamFrame{}, ErrStreamCredit
	}
	start := r.offset - frame.Offset
	if start > uint64(len(frame.Payload)) {
		start = uint64(len(frame.Payload))
	}
	payload := append([]byte(nil), frame.Payload[start:]...)
	r.offset += uint64(len(payload))
	return payload, StreamFrame{Kind: StreamFrameAck, ID: r.id, Epoch: r.epoch, Offset: r.offset, Window: r.window}, nil
}
