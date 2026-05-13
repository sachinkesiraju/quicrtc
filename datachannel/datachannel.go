// Package datachannel provides a generic reliable bidi message
// channel layered on the quicrtc control stream.
//
// Conceptually equivalent to a WebRTC ordered+reliable DataChannel:
// each Send delivers exactly one opaque message to the peer, in
// order, with no payload framing on top of what quicrtc's wire
// format already provides. Use it for input forwarding, app RPC,
// signaling-after-handshake — anything that's not media.
//
// One channel per session in v1. Future versions may multiplex
// several logical channels via a channel-id field in the DATA
// frame; today the wire is one DATA stream per session.
package datachannel

import (
	"context"
	"io"
	"sync"
)

// Channel is one reliable, ordered, bidi message channel. Safe for
// concurrent Send and Recv from different goroutines.
type Channel struct {
	send func(payload []byte) error
	in   chan []byte

	deliverMu sync.Mutex // guards Deliver vs Close to avoid send-on-closed-channel
	closeOnce sync.Once
	closed    chan struct{}
}

// New constructs a Channel given a send function (must be safe to
// call from any goroutine) and an inbound message channel that the
// session loop pushes received DATA payloads into. inboundBuffer is
// the capacity of the receive queue; messages beyond it are dropped
// silently — the channel is not lossy in transit but a stalled Recv
// caller can lose messages.
func New(send func(payload []byte) error, inboundBuffer int) *Channel {
	if inboundBuffer <= 0 {
		inboundBuffer = 64
	}
	return &Channel{
		send:   send,
		in:     make(chan []byte, inboundBuffer),
		closed: make(chan struct{}),
	}
}

// Send delivers one message to the peer. Returns io.ErrClosedPipe if
// the channel has been closed.
func (c *Channel) Send(msg []byte) error {
	select {
	case <-c.closed:
		return io.ErrClosedPipe
	default:
	}
	return c.send(msg)
}

// Recv blocks until the next message arrives, the context is done,
// or the channel is closed.
func (c *Channel) Recv(ctx context.Context) ([]byte, error) {
	select {
	case msg, ok := <-c.in:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	case <-c.closed:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Deliver pushes a received message into the inbound queue. Used by
// the session loop. If the inbound queue is full the message is
// dropped (see New for buffer semantics).
//
// deliverMu serializes Deliver vs Close so a Close in flight cannot
// close c.in between our closed-check and our send.
func (c *Channel) Deliver(msg []byte) {
	c.deliverMu.Lock()
	defer c.deliverMu.Unlock()
	select {
	case <-c.closed:
		return
	default:
	}
	select {
	case c.in <- msg:
	default:
		// inbound queue full; drop. Caller can monitor by adjusting
		// the buffer or polling Recv faster.
	}
}

// Close shuts the channel down. Idempotent. After Close, Recv
// returns io.EOF and Send returns io.ErrClosedPipe.
func (c *Channel) Close() {
	c.closeOnce.Do(func() {
		c.deliverMu.Lock()
		defer c.deliverMu.Unlock()
		close(c.closed)
		close(c.in)
	})
}
