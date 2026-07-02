// The baseline transport: everything multiplexed on ONE WebSocket.
//
// This models what most agent products actually ship today — screen
// frames, model tokens, tool events, and metrics as messages on a
// single WebSocket (or an SSE/WS pair with the same single-TCP-stream
// property). The envelope is deliberately lean binary — 13 bytes, no
// base64, no JSON framing on the frame path — so the baseline is a
// GOOD single-socket implementation, not a strawman:
//
//	[1B lane][4B BE seq][8B BE ptsMicro][payload]
//
// The structural handicap it can't shed: one TCP stream is one FIFO
// byte queue. A 100 KB screen frame accepted by the socket ahead of a
// 10-byte token MUST finish transmitting before the token's first
// byte leaves — and under packet loss, TCP's in-order delivery holds
// back everything behind the gap. quicrtc's lanes are independent
// QUIC streams, so neither is true there.
//
// Sender-side queueing mirrors the quicrtc side's bounded buffers:
// per-client queue of 1024 messages, drop-oldest on overflow (drops
// are counted and reported in telemetry). Writes go straight to the
// socket; when the emulated link backs up, backpressure lands here
// exactly like a real deployment's kernel socket buffer.
package main

import (
	"encoding/binary"
	"net/http"
	"sync"

	"golang.org/x/net/websocket"

	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
)

const wsEnvelopeLen = 13

// encodeWSMessage builds the binary envelope for one event.
func encodeWSMessage(ev event) []byte {
	msg := make([]byte, wsEnvelopeLen+len(ev.payload))
	msg[0] = byte(ev.lane)
	binary.BigEndian.PutUint32(msg[1:5], ev.seq)
	binary.BigEndian.PutUint64(msg[5:13], ev.ptsMicro)
	copy(msg[wsEnvelopeLen:], ev.payload)
	return msg
}

// wsHub fans engine events out to every connected WebSocket client.
type wsHub struct {
	st *status.Status

	mu      sync.Mutex
	clients map[*wsClient]struct{}
}

type wsClient struct {
	queue chan []byte
	done  chan struct{}
}

const wsClientQueue = 1024

func newWSHub(st *status.Status) *wsHub {
	return &wsHub{st: st, clients: make(map[*wsClient]struct{})}
}

// Deliver implements sink. Non-blocking: enqueue per client,
// drop-oldest on overflow.
func (h *wsHub) Deliver(ev event) {
	msg := encodeWSMessage(ev)
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		select {
		case c.queue <- msg:
		default:
			select {
			case <-c.queue:
				h.st.Inc("ws_drops", 1)
			default:
			}
			select {
			case c.queue <- msg:
			default:
			}
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
	c := &wsClient{queue: make(chan []byte, wsClientQueue), done: make(chan struct{})}
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	h.st.Inc("ws_sessions", 1)
	defer func() {
		h.mu.Lock()
		delete(h.clients, c)
		h.mu.Unlock()
		close(c.done)
		_ = ws.Close()
	}()

	// Drain inbound (pings, close) so the connection state machine
	// runs; the demo's data flow is server → client only.
	go func() {
		var buf [512]byte
		for {
			if _, err := ws.Read(buf[:]); err != nil {
				_ = ws.Close()
				return
			}
		}
	}()

	for msg := range c.queue {
		if _, err := ws.Write(msg); err != nil {
			return
		}
		h.st.Inc("ws_bytes", int64(len(msg)))
	}
}
