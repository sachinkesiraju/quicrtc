// cua/server — deterministic CUA host with two dispatch modes.
//
// Stands up a quicrtc server that simulates a computer-use agent's
// browser-handler runtime. The mock "browser" honors four request
// types (action / a11y / dom / perf) with configurable per-handler
// latencies, plus a continuous background screen stream — the
// realistic "always-pushing screenshots while the agent works"
// pattern.
//
// Two modes pick how the server delivers all that traffic on the
// wire:
//
//   -mode=naive
//     ONE track named "data" (KindTokens → DeliveryStreamLowLatency,
//     i.e. a single persistent uni stream). EVERY AU — screen frames,
//     a11y / dom / perf responses, action acks — goes through one
//     pump on one stream. Big screen frames serialize ahead of small
//     responses; the small responses wait their turn at the
//     broadcaster's channel.
//
//   -mode=multistream
//     FIVE tracks. Each gets the right Kind for its payload shape, so
//     the per-Kind dispatch in feed/pump.go picks the right wire
//     pattern:
//        screen  KindVideo   stream-per-GOP
//        a11y    KindTokens  persistent low-latency uni stream
//        dom     KindTokens  persistent low-latency uni stream
//        perf    KindTokens  persistent low-latency uni stream
//        acks    KindTokens  persistent low-latency uni stream
//     Each track has its own goroutine, its own pump, its own stream.
//     A 50 KB screen frame in flight on the screen stream cannot
//     delay the next a11y response on the a11y stream.
//
// Per-handler latencies and the background screen FPS are flag-
// configurable so the comparison can be tuned to a target workload.
// Defaults are conservative for localhost:
//
//	-action-ms=30  -a11y-ms=12  -dom-ms=8  -perf-ms=6  -screen-fps=5  -screen-kb=50
//
// Run:
//
//	# Terminal 1: naive
//	go run ./examples/cua/server -mode=naive
//	# share: https://127.0.0.1:NNNN/wt#slug=...&hash=...
//
//	# Terminal 2: client (see ./examples/cua/client)
//	go run ./examples/cua/client -turns=100 'https://127.0.0.1:NNNN/wt#slug=...&hash=...'
//
// Then repeat with -mode=multistream and compare the two stat tables.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	mrand "math/rand"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
	"github.com/sachinkesiraju/quicrtc/examples/internal/synframe"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// Track names. cua/client matches on these exact strings.
const (
	// Naive mode: every AU goes here.
	trackData = "data"
	// Multistream mode: one track per response Kind.
	trackScreen = "screen"
	trackA11y   = "a11y"
	trackDom    = "dom"
	trackPerf   = "perf"
	trackAcks   = "acks"

	screenWidth  = 480
	screenHeight = 320
)

// Wire shape for one CUA request / response. JSON-encoded over a
// datagram (request) and over a track AU (response). Kept tiny so
// the screen frames dominate bandwidth — the test stresses
// per-stream HOL, not throughput.
type request struct {
	Turn  uint32 `json:"turn"`
	Kind  string `json:"kind"`  // action | a11y | dom | perf
	MsgID uint32 `json:"msg_id"`
}

type response struct {
	Turn  uint32 `json:"turn"`
	Kind  string `json:"kind"`
	MsgID uint32 `json:"msg_id"`
	// Body is opaque; we just need a configurable size to stand in
	// for realistic payloads (4 KB a11y tree, 2 KB DOM hash, etc.).
	Body []byte `json:"body"`
}

// Per-handler config. Latencies are mean; we add small jitter per
// call so the workload is realistic but reproducible.
type handlerCfg struct {
	latencyMs int
	bodyBytes int // exact byte count (0 = minimal JSON envelope only)
}

func (h handlerCfg) run() ([]byte, time.Duration) {
	// 10% jitter
	jitter := time.Duration(mrand.Intn(h.latencyMs)) * time.Millisecond / 10
	d := time.Duration(h.latencyMs)*time.Millisecond + jitter
	body := make([]byte, h.bodyBytes)
	_, _ = rand.Read(body)
	return body, d
}

func main() {
	var (
		mode             = flag.String("mode", "multistream", "naive | multistream")
		bind             = flag.String("bind", "127.0.0.1:0", "UDP bind address")
		actionMs         = flag.Int("action-ms", 30, "mean simulated screenshot latency (ms)")
		a11yMs           = flag.Int("a11y-ms", 12, "mean simulated a11y read latency (ms)")
		domMs            = flag.Int("dom-ms", 8, "mean simulated dom read latency (ms)")
		perfMs           = flag.Int("perf-ms", 6, "mean simulated perf read latency (ms)")
		screenFPS        = flag.Int("screen-fps", 5, "background screen frame rate (Hz); 0 = no screen stream")
		screenKB         = flag.Int("screen-kb", 50, "approximate screen frame size in KB")
		actionKB         = flag.Int("action-kb", 32, "action response (screenshot) body size in KB")
		a11yKB           = flag.Int("a11y-kb", 4, "a11y response body size in KB")
		domKB            = flag.Int("dom-kb", 2, "dom response body size in KB")
		perfKB           = flag.Int("perf-kb", 1, "perf response body size in KB")
		stress           = flag.Bool("stress", false, "localhost stress: 30 Hz × 1 MB screen + datagram metadata (optimized for local testing)")
		datagramMetadata = flag.Bool("datagram-metadata", false, "send metadata (a11y/dom/perf) via unreliable datagrams in multistream mode (default: false)")
		jsonStatus       = flag.Bool("json", false, "emit one JSON status line per second instead of TTY status")
	)
	flag.Parse()

	if *mode != "naive" && *mode != "multistream" {
		log.Fatalf("invalid -mode=%q (want naive | multistream)", *mode)
	}

	if *stress {
		// Localhost-optimized stress: large frames at moderate FPS
		// to create sustained congestion without drops.
		// 30 Hz × 1 MB = 30 MB/s screen stream — enough to queue
		// on localhost but not so aggressive that it drops packets.
		*screenFPS = 30
		*screenKB = 1000
		*actionMs = 5  // small but non-zero to be realistic
		*a11yMs = 2
		*domMs = 2
		*perfMs = 2
		// Enable datagram metadata for maximum multistream advantage.
		// Metadata is tiny (< 200 bytes) so it fits in one datagram.
		*datagramMetadata = true
		*a11yKB = 0 // ~150 bytes after JSON overhead
		*domKB = 0  // ~100 bytes after JSON overhead
		*perfKB = 0 // ~80 bytes after JSON overhead
	}

	handlers := map[string]handlerCfg{
		"action": {latencyMs: *actionMs, bodyBytes: *actionKB * 1024},
		"a11y":   {latencyMs: *a11yMs, bodyBytes: *a11yKB * 1024},
		"dom":    {latencyMs: *domMs, bodyBytes: *domKB * 1024},
		"perf":   {latencyMs: *perfMs, bodyBytes: *perfKB * 1024},
	}

	st := status.New("cua-server[" + *mode + "]")

	host, _, _ := net.SplitHostPort(*bind)
	if host == "" {
		host = "127.0.0.1"
	}

	srv, err := server.New(server.Config{
		Addr:         *bind,
		CertExtraIPs: []net.IP{net.ParseIP(host)},
		ExternalHost: host,
		SDP: wire.SDP{
			Codec: "image/png", Width: screenWidth, Height: screenHeight, FPS: *screenFPS,
		},
		OnSession: func(h server.SessionHandle) {
			st.Inc("sessions", 1)
			// One goroutine per session to drain inbound requests.
			// The session-scoped handle keeps us from racing with
			// AnySession when multiple subscribers connect.
			go drainRequests(h, *mode, handlers, st, *datagramMetadata)
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Register tracks. Naive: one track. Multistream: five tracks.
	pubs := registerTracks(srv, *mode)

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
	fmt.Printf("(mode=%s, action=%dms, a11y=%dms, dom=%dms, perf=%dms, screen=%dHz/%dKB)\n",
		*mode, *actionMs, *a11yMs, *domMs, *perfMs, *screenFPS, *screenKB)
	fmt.Println("(Ctrl-C to stop)")

	// Stash pubs in a goroutine-safe holder so the per-session
	// drainRequests can publish responses on the right track. Map
	// is read-only after this point.
	publishers.Lock()
	publishers.tracks = pubs
	publishers.Unlock()

	// Continuous background screen stream — the "agent is watching
	// the browser" signal. In naive mode this rides the shared
	// `data` track and contends with response writes. In multistream
	// mode it has its own `screen` track and doesn't.
	if *screenFPS > 0 {
		go pumpScreen(ctx, *mode, *screenFPS, *screenKB, pubs, st)
	}

	// Status line / JSON tick.
	stopTick := st.Tick(500*time.Millisecond, func(l *status.Line) {
		l.Field("sessions", "%d", st.Get("sessions"))
		l.Raw("turns=%d", st.Get("turns"))
		l.Raw("screen=%d", st.Get("screen_au"))
		l.Raw("responses=%d", st.Get("responses"))
	})
	if *jsonStatus {
		go func() {
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					fmt.Fprintf(os.Stderr, "{\"sessions\":%d,\"turns\":%d,\"screen_au\":%d,\"responses\":%d}\n",
						st.Get("sessions"), st.Get("turns"), st.Get("screen_au"), st.Get("responses"))
				}
			}
		}()
	}

	<-ctx.Done()
	stopTick()
	st.Done(
		"cua server done — pair with ./examples/cua/client for measured latency",
		[]status.Field{
			{Key: "uptime", Value: st.Uptime().Truncate(time.Second).String()},
			{Key: "mode", Value: *mode},
			{Key: "sessions", Value: fmt.Sprintf("%d", st.Get("sessions"))},
			{Key: "turns served", Value: fmt.Sprintf("%d", st.Get("turns"))},
			{Key: "responses sent", Value: fmt.Sprintf("%d", st.Get("responses"))},
			{Key: "screen frames", Value: fmt.Sprintf("%d", st.Get("screen_au"))},
		})
}

// publishers is a tiny global pubsub-holder. registerTracks fills it
// in once at startup; drainRequests reads from it per turn. A mutex-
// guarded map (vs sync.Map) keeps the type signatures tight; the
// hot path is a single map lookup.
var publishers struct {
	sync.RWMutex
	tracks map[string]*server.Publisher
}

func registerTracks(srv *server.Server, mode string) map[string]*server.Publisher {
	out := map[string]*server.Publisher{}
	if mode == "naive" {
		// Everything on ONE persistent uni stream.
		out["data"] = srv.AddTrackSpec(server.TrackSpec{
			Name: trackData, Kind: track.KindTokens,
		})
		return out
	}
	// Multistream: one track per response Kind.
	out[trackScreen] = srv.AddTrackSpec(server.TrackSpec{
		Name: trackScreen, Kind: track.KindVideo,
	})
	out[trackA11y] = srv.AddTrackSpec(server.TrackSpec{
		Name: trackA11y, Kind: track.KindTokens,
	})
	out[trackDom] = srv.AddTrackSpec(server.TrackSpec{
		Name: trackDom, Kind: track.KindTokens,
	})
	out[trackPerf] = srv.AddTrackSpec(server.TrackSpec{
		Name: trackPerf, Kind: track.KindTokens,
	})
	out[trackAcks] = srv.AddTrackSpec(server.TrackSpec{
		Name: trackAcks, Kind: track.KindTokens,
	})
	return out
}

// trackForKind maps a request kind to the publisher that should
// carry its response in the current mode. In naive mode everything
// goes through the single `data` publisher; in multistream the
// kind selects the right track.
func trackForKind(mode, kind string) string {
	if mode == "naive" {
		return trackData
	}
	switch kind {
	case "action":
		return trackAcks
	case "a11y":
		return trackA11y
	case "dom":
		return trackDom
	case "perf":
		return trackPerf
	}
	return trackAcks
}

// pumpScreen continuously publishes real PNG frames at the configured
// rate. In naive mode this rides the shared `data` track and competes
// with responses; in multistream it has its own `screen` track.
func pumpScreen(ctx context.Context, mode string, fps, sizeKB int, pubs map[string]*server.Publisher, st *status.Status) {
	enc := synframe.New(screenWidth, screenHeight)
	period := time.Second / time.Duration(fps)
	tick := time.NewTicker(period)
	defer tick.Stop()
	target := pubs[trackScreen]
	if mode == "naive" {
		target = pubs[trackData]
	}
	if target == nil {
		return
	}
	var seq uint32
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			keyframe := seq%uint32(fps*2) == 0 // every 2s
			png, err := enc.Encode(seq, keyframe)
			if err != nil {
				continue
			}
			// Pad to ~sizeKB so the screen frame size is realistic
			// for the comparison. PNG encode of the synframe pattern
			// is naturally in the 8-15 KB range; pad with appended
			// random bytes to reach the target. We rely on PNG
			// decoders ignoring trailing bytes (most do; the browser
			// canvas Image decoder definitely does), and the wire
			// teaching point doesn't depend on the bytes being a
			// well-formed PNG of the target size — only that the
			// AU is the configured size.
			//
			// For naive vs multistream comparison what matters is
			// uniform AU size across both modes. We do that here.
			needed := sizeKB*1024 - len(png)
			if needed > 0 {
				pad := make([]byte, needed)
				_, _ = rand.Read(pad)
				png = append(png, pad...)
			}
			payload := make([]byte, len(png))
			copy(payload, png)
			au := pubsub.AccessUnit{
				Bytes:    payload,
				Keyframe: keyframe,
				PTSMicro: uint64(time.Now().UnixMicro()),
				Seq:      seq,
			}
			if err := target.Publish(ctx, au); err == nil {
				st.Inc("screen_au", 1)
			}
			seq++
		}
	}
}

// drainRequests reads request datagrams from THIS session and
// dispatches each to a worker goroutine that simulates the handler
// latency and publishes the response on the right track.
//
// Each kind has its OWN goroutine, so requests of different kinds
// don't serialize at the server. Action and metadata requests
// arriving together in a turn are processed in parallel; the only
// serialization is the wire (which is exactly the property we're
// measuring).
func drainRequests(h server.SessionHandle, mode string, handlers map[string]handlerCfg, st *status.Status, useDatagramMetadata bool) {
	publishers.RLock()
	pubs := publishers.tracks
	publishers.RUnlock()

	for {
		raw, err := h.ReceiveDatagram(h.Context())
		if err != nil {
			return
		}
		var req request
		if err := json.Unmarshal(raw, &req); err != nil {
			continue
		}
		cfg, ok := handlers[req.Kind]
		if !ok {
			continue
		}
		st.Inc("turns_recv", 1) // not used in summary, but useful for debug

		// Dispatch each request to its own goroutine. Real CUA
		// backends do the same: a click and a DOM read run in
		// parallel; we don't serialize them.
		go func(req request, cfg handlerCfg) {
			body, latency := cfg.run()
			// Simulate handler latency.
			t := time.NewTimer(latency)
			select {
			case <-h.Context().Done():
				t.Stop()
				return
			case <-t.C:
			}
			resp := response{Turn: req.Turn, Kind: req.Kind, MsgID: req.MsgID, Body: body}
			respBytes, _ := json.Marshal(resp)

			// In multistream mode with datagram-metadata enabled,
			// send small metadata (a11y/dom/perf) via unreliable datagrams.
			// This bypasses stream congestion entirely and is a real
			// QUIC advantage that naive single-stream designs cannot use.
			// Action responses are always sent via streams (they're too large).
			if useDatagramMetadata && mode == "multistream" && (req.Kind == "a11y" || req.Kind == "dom" || req.Kind == "perf") {
				if len(respBytes) <= 1200 { // conservative datagram size limit
					if err := h.SendDatagram(respBytes); err == nil {
						st.Inc("responses", 1)
						return
					}
					// Fall through to stream if datagram fails
				}
			}

			// Stream path (naive mode, or fallback)
			trackName := trackForKind(mode, req.Kind)
			pub := pubs[trackName]
			if pub == nil {
				return
			}
			au := pubsub.AccessUnit{
				Bytes:    respBytes,
				Keyframe: true, // every AU is self-contained
				PTSMicro: uint64(time.Now().UnixMicro()),
				Seq:      req.MsgID,
			}
			if err := pub.Publish(h.Context(), au); err == nil {
				st.Inc("responses", 1)
				if req.Kind == "action" {
					st.Inc("turns", 1) // count one "turn" per action response
				}
			}
		}(req, cfg)
	}
}
