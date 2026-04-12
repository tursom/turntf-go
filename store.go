package turntf

import (
	"context"
	"sync"
)

type CursorStore interface {
	LoadSeenMessages(context.Context) ([]MessageCursor, error)
	SaveMessage(context.Context, Message) error
	SaveCursor(context.Context, MessageCursor) error
}

type MemoryCursorStore struct {
	mu       sync.Mutex
	messages map[MessageCursor]Message
	order    []MessageCursor
}

func NewMemoryCursorStore() *MemoryCursorStore {
	return &MemoryCursorStore{
		messages: make(map[MessageCursor]Message),
	}
}

func (s *MemoryCursorStore) LoadSeenMessages(context.Context) ([]MessageCursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]MessageCursor, 0, len(s.order))
	out = append(out, s.order...)
	return out, nil
}

func (s *MemoryCursorStore) SaveMessage(_ context.Context, msg Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages[msg.Cursor()] = msg
	return nil
}

func (s *MemoryCursorStore) SaveCursor(_ context.Context, cursor MessageCursor) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.messages[cursor]; !exists {
		s.messages[cursor] = Message{NodeID: cursor.NodeID, Seq: cursor.Seq}
	}
	for _, existing := range s.order {
		if existing == cursor {
			return nil
		}
	}
	s.order = append(s.order, cursor)
	return nil
}

func (s *MemoryCursorStore) HasCursor(cursor MessageCursor) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.messages[cursor]
	return ok
}

func (s *MemoryCursorStore) Message(cursor MessageCursor) (Message, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, ok := s.messages[cursor]
	return msg, ok
}
