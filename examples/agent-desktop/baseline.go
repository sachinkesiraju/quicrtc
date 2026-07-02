// Baseline transports and their shared plumbing.
//
// Two baseline arms, both deliberately STEELMANNED — the demo should
// beat the good version of the competition, not a strawman:
//
//	single WebSocket (this file)
//	  All four lanes multiplexed on one TCP stream with a lean
//	  13-byte binary envelope. What single-gateway agent products
//	  ship (noVNC-class stacks, firewall-friendly one-connection
//	  designs).
//
//	glued stack (glued.go)
//	  Frames on their own WebSocket, tokens on SSE (with seq-based
//	  replay on reconnect), tool calls + telemetry + actions on an
//	  events WebSocket. What well-engineered agent products ship
//	  today — three connections, hand-rolled resume for the one lane
//	  where it's idiomatic.
//
// Steelman details applied to BOTH arms:
//
//   - Frames are ack-paced, latest-only (noVNC flow control): at most
//     one frame is in flight per client; when the client acks, the
//     newest pending frame is sent and intermediate ones are dropped.
//     Without this, a naive FIFO buffers seconds of stale frames and
//     the comparison gets easy — and misleading.
//
//   - Small messages (tokens, tool calls, telemetry, action acks) go
//     through a bounded FIFO, drop-oldest on overflow, mirroring the
//     quicrtc side's bounded queues.
//
// What no steelman can remove: on the single-WS arm, a token behind
// the in-flight 70 KB frame on the same TCP stream still waits for
// it; and neither arm gets cross-lane replay on reconnect without
// building it by hand (the SSE lane gets it here because EventSource
// makes it idiomatic; frames/tools/telemetry don't, because in real
// products they rarely do).
package main

import (
	"encoding/binary"
	"net/http"
	"sync"

	"golang.org/x/net/websocket"

	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
)

// Envelope: [1B lane][4B BE seq][8B BE ptsMicro][payload], both
// directions. Client→server uses laneAction (payload = action JSON)
// and laneFrameAck (no payload).
const wsEnvelopeLen = 13

func encodeWSMessage(lane int, seq uint32, ptsMicro uint64, payload []byte) []byte {
	msg := make([]byte, wsEnvelopeLen+len(payload))
	msg[0] = byte(lane)
	binary.BigEndian.PutUint32(msg[1:5], seq)
	binary.BigEndian.PutUint64(msg[5:13], ptsMicro)
	copy(msg[wsEnvelopeLen:], payload)
	return msg
}

func encodeWSEvent(ev event) []byte {
	return encodeWSMessage(ev.lane, ev.seq, ev.ptsMicro, ev.payload)
}

// framePacer implements ack-paced latest-only frame delivery for one
// client: at most one frame in flight; newer frames replace the
// pending one instead of queueing behind it.
type framePacer struct {
	mu       sync.Mutex
	pending  []byte
	inFlight bool
	kick     chan struct{} // cap 1; signals the sender loop
}

func newFramePacer() *framePacer {
	return &framePacer{kick: make(chan struct{}, 1)}
}

func (f *framePacer) offer(msg []byte) {
	f.mu.Lock()
	f.pending = msg
	fire := !f.inFlight
	f.mu.Unlock()
	if fire {
		select {
		case f.kick <- struct{}{}:
		default:
		}
	}
}

// take returns the pending frame (marking it in flight) or nil.
func (f *framePacer) take() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pending == nil || f.inFlight {
		return nil
	}
	msg := f.pending
	f.pending = nil
	f.inFlight = true
	return msg
}

func (f *framePacer) ack() {
	f.mu.Lock()
	f.inFlight = false
	fire := f.pending != nil
	f.mu.Unlock()
	if fire {
		select {
		case f.kick <- struct{}{}:
		default:
		}
	}
}

// fifoQueue is the bounded drop-oldest queue for small messages.
type fifoQueue struct {
	ch      chan []byte
	onDrop  func()
	stopped chan struct{}
}

const fifoDepth = 1024

func newFifoQueue(onDrop func()) *fifoQueue {
	return &fifoQueue{ch: make(chan []byte, fifoDepth), onDrop: onDrop, stopped: make(chan struct{})}
}

func (q *fifoQueue) offer(msg []byte) {
	select {
	case q.ch <- msg:
		return
	default:
	}
	select {
	case <-q.ch:
		if q.onDrop != nil {
			q.onDrop()
		}
	default:
	}
	select {
	case q.ch <- msg:
	default:
	}
}

// wsPeer is one connected socket that may carry frames (ack-paced)
// and/or small messages (FIFO). The sender loop serializes both onto
// the socket; the reader loop dispatches inbound actions and frame
// acks.
type wsPeer struct {
	fifo   *fifoQueue
	frames *framePacer
	done   chan struct{}
}

func newWSPeer(onDrop func()) *wsPeer {
	return &wsPeer{
		fifo:   newFifoQueue(onDrop),
		frames: newFramePacer(),
		done:   make(chan struct{}),
	}
}

// runSender writes queued messages until the socket errors.
func (p *wsPeer) runSender(ws *websocket.Conn, st *status.Status, bytesKey string) {
	write := func(msg []byte) bool {
		if _, err := ws.Write(msg); err != nil {
			return false
		}
		st.Inc(bytesKey, int64(len(msg)))
		return true
	}
	for {
		select {
		case <-p.done:
			return
		case msg := <-p.fifo.ch:
			if !write(msg) {
				return
			}
		case <-p.frames.kick:
			if msg := p.frames.take(); msg != nil {
				if !write(msg) {
					return
				}
			}
		}
	}
}

// runReader parses inbound client messages until the socket errors.
// onAction receives the action envelope (seq, client ts, payload).
func (p *wsPeer) runReader(ws *websocket.Conn, onAction func(seq uint32, tsMicro uint64, payload []byte)) {
	for {
		var msg []byte
		if err := websocket.Message.Receive(ws, &msg); err != nil {
			_ = ws.Close()
			return
		}
		if len(msg) < wsEnvelopeLen {
			continue
		}
		lane := int(msg[0])
		seq := binary.BigEndian.Uint32(msg[1:5])
		ts := binary.BigEndian.Uint64(msg[5:13])
		switch lane {
		case laneAction:
			if onAction != nil {
				onAction(seq, ts, msg[wsEnvelopeLen:])
			}
		case laneFrameAck:
			p.frames.ack()
		}
	}
}

// ── single-WebSocket arm ────────────────────────────────────────────

// wsHub fans engine events out to every client of the single-WS arm.
//
// naiveFrames switches the frame path from the steelman (ack-paced
// latest-only) to a plain FIFO behind the tokens — the behavior of
// products that write every frame to the socket and let it buffer.
// Default is the steelman; -naive-frames shows what the naive design
// costs (spoiler: seconds of token lag under load).
type wsHub struct {
	st          *status.Status
	onAction    func(x, y int)
	naiveFrames bool

	mu      sync.Mutex
	clients map[*wsPeer]struct{}
}

func newWSHub(st *status.Status, onAction func(x, y int), naiveFrames bool) *wsHub {
	return &wsHub{st: st, onAction: onAction, naiveFrames: naiveFrames, clients: make(map[*wsPeer]struct{})}
}

// Deliver implements sink. Non-blocking: frames go to the ack pacer
// (or the FIFO in naive mode), everything else to the FIFO.
func (h *wsHub) Deliver(ev event) {
	msg := encodeWSEvent(ev)
	h.mu.Lock()
	defer h.mu.Unlock()
	for p := range h.clients {
		if ev.lane == laneScreen && !h.naiveFrames {
			p.frames.offer(msg)
		} else {
			p.fifo.offer(msg)
		}
	}
}

// Handler returns the http.Handler for the /ws endpoint.
func (h *wsHub) Handler() http.Handler {
	return websocket.Server{
		// Accept any origin: the demo page is served from a different
		// localhost port than the shaped WS listener.
		Handshake: func(*websocket.Config, *http.Request) error { return nil },
		Handler:   websocket.Handler(h.serve),
	}
}

func (h *wsHub) serve(ws *websocket.Conn) {
	ws.PayloadType = websocket.BinaryFrame
	p := newWSPeer(func() { h.st.Inc("ws_drops", 1) })
	h.mu.Lock()
	h.clients[p] = struct{}{}
	h.mu.Unlock()
	h.st.Inc("ws_sessions", 1)
	defer func() {
		h.mu.Lock()
		delete(h.clients, p)
		h.mu.Unlock()
		close(p.done)
		_ = ws.Close()
	}()

	go p.runReader(ws, func(seq uint32, ts uint64, payload []byte) {
		if h.onAction != nil {
			if x, y, ok := parseActionXY(payload); ok {
				h.onAction(x, y)
			}
		}
		// Ack the action straight back on the same socket (echoing
		// the client's timestamp so it can compute RTT).
		p.fifo.offer(encodeWSMessage(laneAction, seq, ts, nil))
	})
	p.runSender(ws, h.st, "ws_bytes")
}
