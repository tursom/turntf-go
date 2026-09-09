package turntf

// enqueueEnvelope is the only shared-reader entry into an existing Relay.
// The budget includes recvCh, reorder storage and admitted DATA (including the
// worker's active frame). One extra sender window accommodates retransmissions
// and scheduling skew; duplicate pending sequences consume no additional credit.
// Terminal frames retain FIFO order and eight reserved queue slots. ACK advances
// the reverse direction and bypasses slow DATA, but never an admitted terminal.
func (c *RelayConnection) enqueueEnvelope(env *RelayEnvelope) {
	c.inboxMu.Lock()
	defer c.inboxMu.Unlock()
	c.mu.Lock()
	closed := c.state == RelayStateClosed
	buffered := len(c.recvBuf) + c.recvReady
	_, reordered := c.recvBuf[env.Seq]
	expected, delivered := c.expectedSeq, c.recvDelivered
	c.mu.Unlock()
	if closed || c.inboxFailed {
		return
	}
	window := c.config.WindowSize
	if window < 1 {
		window = 1
	}
	budget := cap(c.recvCh) + 2*window
	if env.Kind == RelayKindAck && !c.inboxTerminal {
		c.handleAck(env)
		return
	}
	data := env.Kind == RelayKindData
	if data && c.inboxTerminal {
		return
	}
	dedup := data && c.config.Reliability == ReliabilityReliableOrdered
	if dedup {
		if c.inboxSeq[env.Seq] || reordered {
			return
		}
		if env.Seq < expected {
			// expectedSeq includes a ready batch that may still be blocked. Only
			// replay the delivered watermark, never ACK that undelivered batch.
			if delivered > 0 {
				c.queueACK(&RelayEnvelope{RelayID: c.relayID, Kind: RelayKindAck,
					SenderSession: c.mySession, TargetSession: c.remoteSession, AckSeq: delivered})
			}
			return
		}
		// Filter before admission: a larger peer window must not exhaust our
		// inbox while the dispatcher is blocked. Dropped frames remain unACKed.
		frontier := expected
		for c.inboxSeq[frontier] {
			frontier++
		}
		if env.Seq >= frontier && env.Seq-frontier >= uint64(window) {
			return
		}
	}
	if c.inbox == nil {
		c.mu.Lock()
		if c.state == RelayStateClosed {
			c.mu.Unlock()
			return
		}
		c.inbox = make(chan *RelayEnvelope, budget+8)
		c.inboxSeq = make(map[uint64]bool)
		c.wg.Add(1)
		go c.dispatchLoop()
		c.mu.Unlock()
	}
	if data && c.inboxData+len(c.recvCh)+buffered >= budget {
		if dedup {
			return
		}
		c.failAdmission()
		return
	}
	ackWork := data && c.config.Reliability != ReliabilityBestEffort
	if ackWork && !c.beginReceiveACK() {
		return
	}
	select {
	case c.inbox <- env:
		if env.Kind == RelayKindClose || env.Kind == RelayKindError {
			c.inboxTerminal = true
		}
		if data {
			c.inboxData++
		}
		if dedup {
			c.inboxSeq[env.Seq] = true
		}
	default:
		if ackWork {
			c.finishReceiveACK()
		}
		if !dedup {
			c.failAdmission()
		}
	}
}

// Called under inboxMu. Shutdown is immediate; at most one callback task exists
// per Relay, and user callbacks never run under an admission lock.
func (c *RelayConnection) failAdmission() {
	c.inboxFailed = true
	c.closeConnection(&RelayError{Code: RelayErrorReceiveOverflow, Message: "relay receive budget exceeded"}, true)
}

func (c *RelayConnection) dispatchLoop() {
	defer c.wg.Done()
	defer func() {
		c.inboxMu.Lock()
		c.inboxFailed = true
		for len(c.inbox) > 0 {
			<-c.inbox
		}
		c.inboxSeq = nil
		c.inboxData = 0
		c.inboxMu.Unlock()
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		case env := <-c.inbox:
			if c.ctx.Err() != nil {
				return
			}
			if env.Kind == RelayKindClose || env.Kind == RelayKindError {
				c.waitReceiveACK()
			}
			c.handleEnvelope(env)
			if env.Kind == RelayKindData && c.config.Reliability != ReliabilityBestEffort {
				c.finishReceiveACK()
			}
			c.inboxMu.Lock()
			if env.Kind == RelayKindData {
				c.inboxData--
				delete(c.inboxSeq, env.Seq)
			}
			c.inboxMu.Unlock()
		}
	}
}

// Register before the dispatcher can publish DATA to the application. Close
// must cover ACK generation as well as the eventual acceptance RPC.
func (c *RelayConnection) beginReceiveACK() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == RelayStateClosed {
		return false
	}
	c.ackGenerating++
	if c.ackIdle == nil {
		c.ackIdle = make(chan struct{})
	}
	return true
}

func (c *RelayConnection) finishReceiveACK() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ackGenerating--
	c.closeACKIdleLocked()
}

func (c *RelayConnection) closeACKIdleLocked() {
	if c.ackGenerating == 0 && !c.ackSending && c.ackPending == nil && c.ackIdle != nil {
		close(c.ackIdle)
		c.ackIdle = nil
	}
}

// ACKs are cumulative in the current Relay implementation. A single worker and
// one coalesced pending ACK avoid per-frame goroutines and ACK stop-and-wait on
// the receive dispatcher. queueACK is called only after bounded delivery.
func (c *RelayConnection) queueACK(env *RelayEnvelope) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == RelayStateClosed {
		return
	}
	c.ackOnce.Do(func() {
		c.ackOut = make(chan struct{}, 1)
		c.wg.Add(1)
		go c.receiveACKLoop()
	})
	if c.ackPending == nil || env.AckSeq > c.ackPending.AckSeq {
		c.ackPending = env
	}
	if c.ackIdle == nil {
		c.ackIdle = make(chan struct{})
	}
	select {
	case c.ackOut <- struct{}{}:
	default:
	}
}

func (c *RelayConnection) receiveACKLoop() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.ackOut:
			for {
				c.mu.Lock()
				env := c.ackPending
				c.ackPending = nil
				c.ackSending = env != nil
				c.closeACKIdleLocked()
				c.mu.Unlock()
				if env == nil {
					break
				}
				err := c.sendRelayEnvelope(env)
				c.mu.Lock()
				c.ackSending = false
				if err == nil {
					c.closeACKIdleLocked()
				}
				c.mu.Unlock()
				if err != nil {
					c.handleClose(err)
					return
				}
			}
		}
	}
}

func (c *RelayConnection) waitReceiveACK() {
	c.mu.Lock()
	idle := c.ackIdle
	c.mu.Unlock()
	if idle != nil {
		select {
		case <-idle:
		case <-c.ctx.Done():
		}
	}
}
