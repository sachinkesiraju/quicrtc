// agent_pubsub/server — multi-channel quicrtc publisher.
//
// Stands up FOUR independent channels on one QUIC connection. The
// shape mirrors what an autonomous computer-use agent's runtime
// produces — large screenshot frames, small high-frequency tokens,
// sparse JSON tool-calls, and per-session telemetry — but the AGENT
// LOOP IS NOT REAL. The reasoning lines are hardcoded sentences and
// the tool calls cycle through a fixed list. This example exists to
// demonstrate the WIRE SHAPE (per-Kind dispatch + datagrams on one
// QUIC connection). For a deterministic latency comparison between
// naive and multistream dispatch see ./examples/cua. A real CUA
// agent (Puppeteer / Playwright + an LLM loop) is an application
// that builds on top of this library, not part of this repo.
//
//	┌──────────────┬─────────────────────┬───────────────────────────────────┐
//	│ Track        │ Kind                │ On-wire delivery (per pump.go)    │
//	├──────────────┼─────────────────────┼───────────────────────────────────┤
//	│ screen       │ KindVideo           │ stream-per-GOP uni stream         │
//	│ reasoning    │ KindTokens          │ persistent low-latency uni stream │
//	│ actions      │ KindTokens (*)      │ persistent low-latency uni stream │
//	│ telemetry    │ datagrams           │ unreliable QUIC datagrams         │
//	└──────────────┴─────────────────────┴───────────────────────────────────┘
//	(*) KindToolCalls is the natural fit for `actions` but routes via
//	    DeliveryBidiPerCall, which requires a client that accepts bidi
//	    streams. The Go client doesn't yet, so this example uses
//	    KindTokens for actions to keep BOTH the Go CLI viewer AND the
//	    browser viewer reachable. The per-Kind dispatch teaching point
//	    still lands — actions ride a persistent uni stream distinct
//	    from screen's per-GOP stream and reasoning's own stream.
//
// What this teaches (verified by reading the code, not assumed):
//
//   - AddTrackSpec with distinct Kinds so the per-Kind dispatch in
//     feed/pump.go picks the right wire pattern for each modality.
//   - SessionHandle.SendDatagram — fire-and-forget telemetry from
//     OnSession. The 4-byte trackId envelope is documented in
//     wire/datagram.go and reused by the browser viewer.
//   - client.RequestKeyframe → server's OnKeyframeRequest — the
//     loss-recovery handshake. The CLI viewer asks every 10 s; the
//     next screen frame is marked K.
//   - SessionHandle.InboundRecv — per-session receive of a track the
//     subscriber published. The browser viewer's click-canvas publishes
//     back on `user_actions`; this server drains and logs them, scoped
//     correctly to the originating session (no AnySession races under
//     multi-subscriber load).
//
// What you should NOT take away:
//   - That the per-Kind dispatch is MEASURABLY faster than a naive
//     single-track shape. That comparison lives in ./examples/cua,
//     which prints actual p50/p95/max numbers.
//
// Run:
//
//	go run ./examples/agent_pubsub/server
//
// Prints the share URL on stdout; paste it into
// ./examples/agent_pubsub/viewer or ts-sdk/examples/viewer/.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	mrand "math/rand"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
	"github.com/sachinkesiraju/quicrtc/examples/internal/synframe"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// Track names are constants so the viewer can match exactly.
const (
	trackScreen    = "screen"
	trackReasoning = "reasoning"
	trackActions   = "actions"
	trackUserActs  = "user_actions" // viewer → server publish-back

	// Datagram envelope demux key. The viewer parses this with
	// wire.DecodeDatagram and filters by trackID, so a future
	// second datagram-kind track wouldn't collide.
	telemetryTrackID uint8 = 1

	screenWidth  = 480
	screenHeight = 320
)

func main() {
	st := status.New("server")

	// Bumped to 1 by OnKeyframeRequest so the next screen frame
	// becomes a keyframe regardless of cadence — exactly what a real
	// encoder would do on receiving a PLI.
	var forceKeyframe atomic.Bool

	cfg := server.Config{
		Addr:         "127.0.0.1:0",
		CertExtraIPs: []net.IP{net.ParseIP("127.0.0.1")},
		ExternalHost: "127.0.0.1",
		// Codec is "image/png" because that's exactly what the
		// screen track publishes — real PNG bytes encoded by
		// examples/internal/synframe, not a placeholder label.
		SDP: wire.SDP{
			Codec: "image/png", Width: screenWidth, Height: screenHeight, FPS: 5,
		},
		OnKeyframeRequest: func(_, trackName string) {
			if trackName == trackScreen {
				forceKeyframe.Store(true)
			}
		},
		OnSession: func(h server.SessionHandle) {
			st.Inc("sessions", 1)
			// One goroutine per session for fire-and-forget telemetry
			// and one for draining user_actions. Both bound to the
			// session's context so they exit on disconnect.
			go pumpTelemetry(h, st)
			go drainUserActions(h, st)
		},
	}
	srv, err := server.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Register all three publish-out tracks BEFORE accepting
	// connections so they appear in the handshake's ANNOUNCE list.
	// Each gets an explicit Kind so the per-Kind dispatch picks
	// the right wire shape.
	screen := srv.AddTrackSpec(server.TrackSpec{Name: trackScreen, Kind: track.KindVideo})
	reasoning := srv.AddTrackSpec(server.TrackSpec{Name: trackReasoning, Kind: track.KindTokens})
	// See the table at top of file: actions wants KindToolCalls but
	// settles for KindTokens until the Go client supports bidi
	// streams. The per-Kind point still lands — actions ride a
	// distinct persistent uni stream, not the screen GOP stream.
	actions := srv.AddTrackSpec(server.TrackSpec{Name: trackActions, Kind: track.KindTokens})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	go func() {
		if err := srv.ListenAndServe(ctx); err != nil && ctx.Err() == nil {
			log.Printf("server: %v", err)
		}
	}()
	for srv.Addr() == "127.0.0.1:0" {
		time.Sleep(10 * time.Millisecond)
	}
	fmt.Println("share:", srv.ShareLink())
	fmt.Println("(Ctrl-C to stop)")

	// Three publishers, one goroutine each. Each writes at its own
	// natural cadence; they never coordinate. On the wire, each goes
	// onto its own QUIC stream — slow traffic on one cannot delay the
	// others.
	go pumpScreen(ctx, screen, &forceKeyframe, st)
	go pumpReasoning(ctx, reasoning, st)
	go pumpActions(ctx, actions, st)

	// Keep the status line fresh while the pumps run.
	stopTick := st.Tick(200*time.Millisecond, func(l *status.Line) {
		l.Field("sessions", "%d", st.Get("sessions"))
		l.Raw("screen=%d (K %d)", st.Get("screen_au"), st.Get("screen_k"))
		l.Raw("reasoning=%d toks", st.Get("reasoning_au"))
		l.Raw("actions=%d", st.Get("actions_au"))
		l.Raw("telemetry=%d", st.Get("telemetry_dg"))
		if u := st.Get("user_actions"); u > 0 {
			l.Raw("user_actions=%d", u)
		}
	})
	<-ctx.Done()
	stopTick()

	st.Done(
		"four channels delivered concurrently on one QUIC connection — see ./examples/cua for measured latency comparison",
		[]status.Field{
			{Key: "uptime", Value: st.Uptime().Truncate(time.Second).String()},
			{Key: "sessions", Value: fmt.Sprintf("%d", st.Get("sessions"))},
			{Key: "screen", Value: fmt.Sprintf("%s AU (K %d, P %d) %s",
				status.HumanCount(st.Get("screen_au")),
				st.Get("screen_k"),
				st.Get("screen_au")-st.Get("screen_k"),
				status.HumanBytes(st.Get("screen_bytes")))},
			{Key: "reasoning", Value: fmt.Sprintf("%s tokens  %s",
				status.HumanCount(st.Get("reasoning_au")),
				status.HumanBytes(st.Get("reasoning_bytes")))},
			{Key: "actions", Value: fmt.Sprintf("%s calls  %s",
				status.HumanCount(st.Get("actions_au")),
				status.HumanBytes(st.Get("actions_bytes")))},
			{Key: "telemetry", Value: fmt.Sprintf("%s datagrams  %s",
				status.HumanCount(st.Get("telemetry_dg")),
				status.HumanBytes(st.Get("telemetry_bytes")))},
			{Key: "user_actions", Value: fmt.Sprintf("%s recv",
				status.HumanCount(st.Get("user_actions")))},
		})
}

// ── screen: real PNG frames @ 5 Hz, keyframe every 5 s ──
//
// Each frame is encoded with examples/internal/synframe to actual
// image/png bytes (~50 KB) — the SDP's Codec claim matches the
// wire content, and the browser viewer can decode + display the
// frame on its canvas. PNG encode at 480x320 costs ~1 ms on a
// recent CPU.
func pumpScreen(ctx context.Context, pub *server.Publisher, force *atomic.Bool, st *status.Status) {
	enc := synframe.New(screenWidth, screenHeight)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	var seq uint32
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			keyframe := seq%25 == 0 || force.Swap(false)
			png, err := enc.Encode(seq, keyframe)
			if err != nil {
				continue
			}
			// Copy: synframe reuses its buffer per Encode, and
			// pubsub keeps the slice alive across fanout.
			payload := make([]byte, len(png))
			copy(payload, png)
			au := pubsub.AccessUnit{
				Bytes:    payload,
				Keyframe: keyframe,
				PTSMicro: uint64(time.Now().UnixMicro()),
				Seq:      seq,
			}
			if err := pub.Publish(ctx, au); err == nil {
				st.Inc("screen_au", 1)
				st.Inc("screen_bytes", int64(len(payload)))
				if keyframe {
					st.Inc("screen_k", 1)
				}
			}
			seq++
		}
	}
}

// ── reasoning: token-by-token stream of fake agent thoughts ─────
//
// HARDCODED CONTENT — the model isn't running. The wire-shape
// teaching point (10 Hz small AUs on a persistent uni stream) is
// real; the content is decorative.
func pumpReasoning(ctx context.Context, pub *server.Publisher, st *status.Status) {
	thoughts := [][]string{
		{"User", "wants", "me", "to", "summarize", "the", "open", "tabs.", "I'll", "start", "by", "taking", "a", "screenshot."},
		{"Detected", "a", "Stripe", "dashboard.", "I", "should", "click", "the", "Reports", "tab."},
		{"Form", "appeared.", "Filling", "email", "field", "with", "the", "value", "from", "the", "previous", "step."},
		{"Submit", "succeeded.", "Verifying", "confirmation", "banner", "is", "visible."},
	}
	tick := time.NewTicker(100 * time.Millisecond) // 10 Hz tokens
	defer tick.Stop()
	var seq uint32
	for {
		for _, sentence := range thoughts {
			for _, tok := range sentence {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
				payload := []byte(tok + " ")
				au := pubsub.AccessUnit{
					Bytes:    payload,
					Keyframe: true, // every token is independent
					PTSMicro: uint64(time.Now().UnixMicro()),
					Seq:      seq,
				}
				if err := pub.Publish(ctx, au); err != nil {
					return
				}
				st.Inc("reasoning_au", 1)
				st.Inc("reasoning_bytes", int64(len(payload)))
				seq++
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(800 * time.Millisecond):
			}
		}
	}
}

// ── actions: JSON tool-calls, ~0.5 Hz ─────────────────────────
//
// HARDCODED CONTENT — cycles through a fixed list of plausible
// CUA tool calls. Wire shape is real (persistent low-latency uni
// stream distinct from screen and reasoning); content is not.
func pumpActions(ctx context.Context, pub *server.Publisher, st *status.Status) {
	actions := []string{
		`{"tool":"screenshot"}`,
		`{"tool":"left_click","x":412,"y":318}`,
		`{"tool":"type","text":"sachin@example.com"}`,
		`{"tool":"key","key":"Tab"}`,
		`{"tool":"type","text":"hunter2"}`,
		`{"tool":"left_click","x":520,"y":402}`,
		`{"tool":"wait","ms":1500}`,
	}
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	var seq uint32
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			payload := []byte(actions[int(seq)%len(actions)])
			au := pubsub.AccessUnit{
				Bytes:    payload,
				Keyframe: true,
				PTSMicro: uint64(time.Now().UnixMicro()),
				Seq:      seq,
			}
			if err := pub.Publish(ctx, au); err != nil {
				return
			}
			st.Inc("actions_au", 1)
			st.Inc("actions_bytes", int64(len(payload)))
			seq++
		}
	}
}

// ── telemetry: per-session datagrams @ 2 Hz ───────────────────
//
// One goroutine per accepted session; bound to that session's
// lifetime via h.Context(). The payload uses a 4-byte envelope
// (TrackID + uint16 seq + reserved) so the receiver can filter
// by trackId and not accidentally consume datagrams from a
// future second telemetry-kind track.
func pumpTelemetry(h server.SessionHandle, st *status.Status) {
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	var seq uint16
	for {
		select {
		case <-h.Context().Done():
			return
		case <-tick.C:
			body := []byte(fmt.Sprintf(
				`{"cpu":%.2f,"mem_mb":%d,"inflight":%d,"t":%d}`,
				mrand.Float64()*0.8,
				200+mrand.Intn(800),
				mrand.Intn(5),
				time.Now().UnixMilli(),
			))
			payload := make([]byte, 4+len(body))
			payload[0] = telemetryTrackID
			binary.BigEndian.PutUint16(payload[1:3], seq)
			// payload[3] reserved
			copy(payload[4:], body)
			if err := h.SendDatagram(payload); err != nil {
				return
			}
			st.Inc("telemetry_dg", 1)
			st.Inc("telemetry_bytes", int64(len(payload)))
			seq++
		}
	}
}

// ── user_actions: drain THIS session's publish-back ───────────
//
// The browser viewer publishes click coordinates on a "user_actions"
// track. We don't act on them beyond logging — the point is that the
// wire is bidirectional and that a server can consume client-
// published tracks per-session via SessionHandle.InboundRecv.
//
// Per-session-correct: previous revisions used Server.AnySession()
// here, which mixes inbound AUs across subscribers when more than
// one viewer is connected. SessionHandle.InboundRecv routes only
// the THIS-session AUs.
func drainUserActions(h server.SessionHandle, st *status.Status) {
	for {
		au, err := h.InboundRecv(h.Context(), trackUserActs)
		if err != nil {
			return
		}
		st.Inc("user_actions", 1)
		// One human-readable line per event, above the live status.
		// The status renderer rewrites the status line on its next tick.
		fmt.Fprintf(os.Stderr, "\r\x1b[2K[user_action sid=%s] %s\n",
			short(h.SessionID()), string(au.Bytes))
	}
}

func short(sid string) string {
	if len(sid) > 8 {
		return sid[:8]
	}
	return sid
}
