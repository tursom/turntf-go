package turntf

import (
	"context"
	"sync"
)

// CursorStore 定义了消息游标持久化接口。
// 客户端在连接时会加载已确认的消息游标，用于消息去重和服务端恢复会话状态。
type CursorStore interface {
	// LoadSeenMessages 加载所有已确认收到的消息游标列表，用于在登录时告知服务端已收到的消息。
	LoadSeenMessages(context.Context) ([]MessageCursor, error)
	// SaveMessage 保存收到的消息内容到本地存储。
	SaveMessage(context.Context, Message) error
	// SaveCursor 保存消息游标到本地存储，标记该游标对应的消息已被确认接收。
	SaveCursor(context.Context, MessageCursor) error
}

// MemoryCursorStore 是基于内存的 CursorStore 实现，适用于单实例或测试场景。
// 消息和游标均存储在内存 map 中，程序重启后数据会丢失。
type MemoryCursorStore struct {
	mu       sync.Mutex
	messages map[MessageCursor]Message
	order    []MessageCursor
}

// NewMemoryCursorStore 创建并返回一个新的 MemoryCursorStore 实例。
func NewMemoryCursorStore() *MemoryCursorStore {
	return &MemoryCursorStore{
		messages: make(map[MessageCursor]Message),
	}
}

// LoadSeenMessages 返回所有已保存的游标列表，按首次记录的顺序排列。
func (s *MemoryCursorStore) LoadSeenMessages(context.Context) ([]MessageCursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]MessageCursor, 0, len(s.order))
	out = append(out, s.order...)
	return out, nil
}

// SaveMessage 保存消息内容到内存中，以游标为键。
func (s *MemoryCursorStore) SaveMessage(_ context.Context, msg Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages[msg.Cursor()] = msg
	return nil
}

// SaveCursor 保存游标到内存。如果该游标尚不存在于消息 map 中，则创建一个空的占位记录。
// 重复的游标不会重复添加。
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

// HasCursor 判断指定的游标是否已被记录（无论是否有完整的消息内容）。
func (s *MemoryCursorStore) HasCursor(cursor MessageCursor) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.messages[cursor]
	return ok
}

// Message 根据游标查询已保存的消息内容，第二个返回值为是否存在该消息。
func (s *MemoryCursorStore) Message(cursor MessageCursor) (Message, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, ok := s.messages[cursor]
	return msg, ok
}
