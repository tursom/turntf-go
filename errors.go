package turntf

import (
	"errors"
	"fmt"
)

var (
	ErrClosed       = errors.New("turntf client is closed")
	ErrNotConnected = errors.New("turntf client is not connected")
	ErrDisconnected = errors.New("turntf websocket disconnected")
)

type ServerError struct {
	Code      string
	Message   string
	RequestID uint64
}

func (e *ServerError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.RequestID == 0 {
		return fmt.Sprintf("turntf server error: %s (%s)", e.Code, e.Message)
	}
	return fmt.Sprintf("turntf server error: %s (%s), request_id=%d", e.Code, e.Message, e.RequestID)
}

func (e *ServerError) Unauthorized() bool {
	return e != nil && e.Code == "unauthorized"
}

type ProtocolError struct {
	Message string
}

func (e *ProtocolError) Error() string {
	return "turntf protocol error: " + e.Message
}

type ConnectionError struct {
	Op  string
	Err error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("turntf connection error during %s: %v", e.Op, e.Err)
}

func (e *ConnectionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
