package turntf

import (
	"errors"
	"time"
)

// Only an explicit temporary server rejection is retryable. Authentication,
// missing sessions, disconnects, and best-effort failures keep their semantics.
func (c *RelayConnection) retryableSendError(err error) bool {
	var serverErr *ServerError
	return c.config.Reliability != ReliabilityBestEffort && c.ctx.Err() == nil &&
		errors.As(err, &serverErr) && serverErr.Code == "service_unavailable"
}

func (c *RelayConnection) sendACKWithRetry(env *RelayEnvelope) error {
	for retries := 0; ; retries++ {
		err := c.sendRelayEnvelope(env)
		if !c.retryableSendError(err) || retries >= c.config.MaxRetransmits {
			return err
		}
		// ackSending remains set throughout retries: Close must not overtake a
		// failed ACK. The worker and pending cumulative ACK remain bounded.
		delay := time.Duration(c.config.AckTimeoutMs) * time.Millisecond
		if delay <= 0 {
			delay = time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-c.ctx.Done():
			timer.Stop()
			return c.ctx.Err()
		}
	}
}
