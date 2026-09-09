package turntf

import (
	"context"
	"sync"
	"time"
)

// cancelMutex keeps queue ordering without creating a goroutine per waiter.
type cancelMutex struct {
	once  sync.Once
	token chan struct{}
}

func (m *cancelMutex) init() { m.once.Do(func() { m.token = make(chan struct{}, 1) }) }
func (m *cancelMutex) LockContext(ctx context.Context) error {
	m.init()
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case m.token <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (m *cancelMutex) Lock()   { _ = m.LockContext(context.Background()) }
func (m *cancelMutex) Unlock() { <-m.token }
func (m *cancelMutex) TryLock() bool {
	m.init()
	select {
	case m.token <- struct{}{}:
		return true
	default:
		return false
	}
}

type relaySendItem struct {
	data    []byte
	barrier chan uint64
}

// Flush 等待本次屏障前入队 DATA 的受理及端到端 ACK，不关闭连接。
// BestEffort 没有端到端 ACK，仅等待受理。取消仅结束本次等待，已入队
// 数据不会撤销。并发 Send 以入队锁顺序界定是否属于本次屏障。
func (c *RelayConnection) Flush(ctx context.Context) error {
	if err := c.enqueueMu.LockContext(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	open := c.state == RelayStateOpen
	c.mu.Unlock()
	if !open {
		c.enqueueMu.Unlock()
		return c.flushError()
	}
	barrier := make(chan uint64, 1)
	select {
	case c.sendCh <- relaySendItem{barrier: barrier}:
		c.enqueueMu.Unlock()
	case <-ctx.Done():
		c.enqueueMu.Unlock()
		return ctx.Err()
	case <-c.ctx.Done():
		c.enqueueMu.Unlock()
		return c.flushError()
	}
	var target uint64
	select {
	case target = <-barrier:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.ctx.Done():
		return c.flushError()
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.ctx.Err() != nil {
			return c.flushError()
		}
		c.mu.Lock()
		done := true
		for seq := range c.unacked {
			if seq < target {
				done = false
				break
			}
		}
		c.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		case <-c.ctx.Done():
			return c.flushError()
		}
	}
}
func (c *RelayConnection) flushError() error {
	if err := c.closeResult(); err != nil {
		return err
	}
	return &RelayError{Code: RelayErrorNotConnected, Message: "connection not open"}
}
