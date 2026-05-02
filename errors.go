package turntf

import (
	"errors"
	"fmt"
)

var (
	// ErrClosed 表示客户端已经被关闭，无法执行任何操作。
	ErrClosed = errors.New("turntf client is closed")
	// ErrNotConnected 表示客户端尚未建立 WebSocket 连接或已断开连接。
	ErrNotConnected = errors.New("turntf client is not connected")
	// ErrDisconnected 表示 WebSocket 连接已断开，客户端将尝试自动重连（如果配置了重连）。
	ErrDisconnected = errors.New("turntf websocket disconnected")
)

// ServerError 表示服务端返回的错误响应，包含错误码、描述信息以及关联的请求 ID。
type ServerError struct {
	Code      string
	Message   string
	RequestID uint64
}

// Error 返回 ServerError 的格式化错误字符串。
// 如果有关联的请求 ID，则一并包含在错误信息中。
func (e *ServerError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.RequestID == 0 {
		return fmt.Sprintf("turntf server error: %s (%s)", e.Code, e.Message)
	}
	return fmt.Sprintf("turntf server error: %s (%s), request_id=%d", e.Code, e.Message, e.RequestID)
}

// Unauthorized 判断该错误是否为"未授权"错误（错误码为 "unauthorized"）。
// 当返回 true 时，客户端不会自动重连，因为凭据已失效。
func (e *ServerError) Unauthorized() bool {
	return e != nil && e.Code == "unauthorized"
}

// ProtocolError 表示协议层错误，例如收到了无法识别的消息格式、字段缺失或非预期的响应。
type ProtocolError struct {
	Message string
}

// Error 返回 ProtocolError 的格式化错误字符串。
func (e *ProtocolError) Error() string {
	return "turntf protocol error: " + e.Message
}

// ConnectionError 表示网络连接层面的错误，包含操作名称和原始错误原因。
type ConnectionError struct {
	Op  string
	Err error
}

// Error 返回 ConnectionError 的格式化错误字符串，包含操作描述（Op）和底层错误。
func (e *ConnectionError) Error() string {
	return fmt.Sprintf("turntf connection error during %s: %v", e.Op, e.Err)
}

// Unwrap 返回底层的原始错误，支持 errors.Is / errors.As 链式错误检查。
func (e *ConnectionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
