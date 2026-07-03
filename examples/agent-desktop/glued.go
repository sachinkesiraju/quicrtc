// The glued-stack arm: what a well-engineered agent product ships
// today. Three connections, each the idiomatic tool for its lane:
//
//	/frames  WebSocket — screen frames, ack-paced latest-only
//	/events  WebSocket — tool calls + telemetry out, actions in
//	/sse     SSE       — reasoning tokens, with ?from=<seq> replay
//	                     from a ring buffer on reconnect
//
// This is the fair fight: lanes on separate TCP connections don't
// share an application FIFO, so unlike the single-WS arm the tokens
// here never wait behind an in-flight frame in the send queue — they
// only contend with frame packets at the bottleneck router. What this
// arm still can't shed:
//
//   - three handshakes to open, three reconnect dances after a drop,
//     and replay for exactly one lane (the SSE one, because
//     Last-Event-ID semantics make it idiomatic; frames, tool calls,
//     and telemetry lose whatever was in flight);
//   - every lane is reliable-ordered TCP: telemetry that should be
//     fire-and-forget queues behind loss like everything else;
//   - the token ring buffer, the ack pacing, the reconnect logic —
//     all hand-rolled here, ~200 lines a real product must own.
//
// quicrtc's session gets the equivalents from the transport: one
// handshake, one re-dial with per-track replay, datagram telemetry,
// and stream-per-GOP frame freshness.
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"golang.org/x/net/websocket"

	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
)

// tokenRing buffers recent token events for SSE replay. 1024 tokens
// at ~50 tok/s ≈ 20 s of history — same order as quicrtc's built-in
// 256-AU replay window.
type tokenRing struct {
	mu   sync.Mutex
	buf  []event
	head int
}

const tokenRingCap = 1024

func (r *tokenRing) add(ev event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.buf) < tokenRingCap {
		r.buf = append(r.buf, ev)
		return
	}
	r.buf[r.head] = ev
	r.head = (r.head + 1) % tokenRingCap
}

// since returns buffered events with seq > from, oldest first.
func (r *tokenRing) since(from uint32) []event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]event, 0, len(r.buf))
	n := len(r.buf)
	for i := 0; i < n; i++ {
		ev := r.buf[(r.head+i)%n]
		if ev.seq > from {
			out = append(out, ev)
		}
	}
	return out
}

// gluedHub is the sink + HTTP handlers for the glued arm.
type gluedHub struct {
	st       *status.Status
	onAction func(x, y int)

	mu     sync.Mutex
	frames map[*wsPeer]struct{}    // /frames clients
	events map[*wsPeer]struct{}    // /events clients
	sse    map[*sseClient]struct{} // /sse clients

	ring tokenRing
}

type sseClient struct {
	ch   chan event
	done chan struct{}
}

func newGluedHub(st *status.Status, onAction func(x, y int)) *gluedHub {
	return &gluedHub{
		st:       st,
		onAction: onAction,
		frames:   make(map[*wsPeer]struct{}),
		events:   make(map[*wsPeer]struct{}),
		sse:      make(map[*sseClient]struct{}),
	}
}

// Deliver implements sink, routing each lane to its connection.
func (h *gluedHub) Deliver(ev event) {
	switch ev.lane {
	case laneScreen:
		msg := encodeWSEvent(ev)
		h.mu.Lock()
		for p := range h.frames {
			p.frames.offer(msg)
		}
		h.mu.Unlock()
	case laneReasoning:
		h.ring.add(ev)
		h.mu.Lock()
		for c := range h.sse {
			select {
			case c.ch <- ev:
			default:
				h.st.Inc("glued_drops", 1)
			}
		}
		h.mu.Unlock()
	case laneToolCalls, laneTelemetry:
		msg := encodeWSEvent(ev)
		h.mu.Lock()
		for p := range h.events {
			p.fifo.offer(msg)
		}
		h.mu.Unlock()
	}
}

// Mux returns the handler serving all three endpoints.
func (h *gluedHub) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	anyOrigin := func(fn func(*websocket.Conn)) http.Handler {
		return websocket.Server{
			Handshake: func(*websocket.Config, *http.Request) error { return nil },
			Handler:   websocket.Handler(fn),
		}
	}
	mux.Handle("/frames", anyOrigin(h.serveFrames))
	mux.Handle("/events", anyOrigin(h.serveEvents))
	mux.HandleFunc("/sse", h.serveSSE)
	return mux
}

func (h *gluedHub) serveFrames(ws *websocket.Conn) {
	ws.PayloadType = websocket.BinaryFrame
	p := newWSPeer(nil)
	h.mu.Lock()
	h.frames[p] = struct{}{}
	h.mu.Unlock()
	h.st.Inc("glued_sessions", 1)
	defer func() {
		h.mu.Lock()
		delete(h.frames, p)
		h.mu.Unlock()
		close(p.done)
		_ = ws.Close()
	}()
	go p.runReader(ws, nil) // frame acks only
	p.runSender(ws, h.st, "glued_bytes")
}

func (h *gluedHub) serveEvents(ws *websocket.Conn) {
	ws.PayloadType = websocket.BinaryFrame
	p := newWSPeer(func() { h.st.Inc("glued_drops", 1) })
	h.mu.Lock()
	h.events[p] = struct{}{}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.events, p)
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
		p.fifo.offer(encodeWSMessage(laneAction, seq, ts, nil))
	})
	p.runSender(ws, h.st, "glued_bytes")
}

// serveSSE streams token events as text/event-stream. ?from=<seq>
// replays buffered tokens with seq > from before going live — the
// hand-rolled equivalent of Last-Event-ID resume.
func (h *gluedHub) serveSSE(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)

	c := &sseClient{ch: make(chan event, 256), done: make(chan struct{})}
	h.mu.Lock()
	h.sse[c] = struct{}{}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.sse, c)
		h.mu.Unlock()
		close(c.done)
	}()

	write := func(ev event) bool {
		body, err := json.Marshal(struct {
			Seq  uint32 `json:"seq"`
			TsUs uint64 `json:"ts_us"`
			Text string `json:"text"`
		}{ev.seq, ev.ptsMicro, string(ev.payload)})
		if err != nil {
			return true
		}
		if _, err := fmt.Fprintf(w, "id: %d\ndata: %s\n\n", ev.seq, body); err != nil {
			return false
		}
		fl.Flush()
		h.st.Inc("glued_bytes", int64(len(body)+16))
		return true
	}

	// Replay before live. Live events that arrived while replaying
	// are already queued in c.ch; duplicates are possible at the
	// boundary and the client dedups by seq.
	if from := r.URL.Query().Get("from"); from != "" {
		if seq, err := strconv.ParseUint(from, 10, 32); err == nil {
			for _, ev := range h.ring.since(uint32(seq)) {
				if !write(ev) {
					return
				}
			}
		}
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-c.ch:
			if !write(ev) {
				return
			}
		}
	}
}

// parseActionXY extracts x/y from an action payload.
func parseActionXY(payload []byte) (x, y int, ok bool) {
	var a struct {
		X int `json:"x"`
		Y int `json:"y"`
	}
	if err := json.Unmarshal(payload, &a); err != nil {
		return 0, 0, false
	}
	return a.X, a.Y, true
}

// laneFrameAck / laneAction extend the lane space for client→server
// messages (and the action-ack echo back).
const (
	laneAction   = 5
	laneFrameAck = 6
)
