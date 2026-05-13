// cua/client — agent simulator with per-response latency measurement.
//
// Connects to a cua/server share URL, runs N CUA "turns" against it,
// and prints per-response p50 / p95 / max plus per-turn (max of all
// 4 response arrivals) p50 / p95 / max. Pair runs in -mode=naive and
// -mode=multistream and compare the two stat blocks to SEE the
// multistream advantage on the same workload.
//
// One turn:
//
//	t0:  Client sends 4 request datagrams in tight succession:
//	     {action, a11y, dom, perf}. Each carries a msg_id so the
//	     server's response can be matched back to its request.
//
//	t0+30ms (typ): server's action handler completes; publishes a
//	     ~32 KB JSON response on the action/acks track.
//
//	Meanwhile a11y / dom / perf handlers (6-12 ms each) publish
//	their smaller responses on their own tracks (multistream) or
//	on the shared `data` track (naive).
//
//	The client awaits all 4 responses (matched by msg_id) and
//	records per-response arrival latency (wall time from t0).
//
// Between turns the client sleeps a configurable `-turn-gap` so the
// workload doesn't go straight back-to-back (real CUA agents have
// model-think time between turns). Default 200ms.
//
// The screen stream runs continuously in the background regardless
// of turns — exactly the contention pattern that makes naive vs
// multistream measurable on the wire.
//
// Run:
//
//	go run ./examples/cua/client -turns=100 \
//	    'https://127.0.0.1:NNNN/wt#slug=...&hash=...'
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
)

// Match the server's wire types exactly.
type request struct {
	Turn  uint32 `json:"turn"`
	Kind  string `json:"kind"`
	MsgID uint32 `json:"msg_id"`
}
type response struct {
	Turn  uint32 `json:"turn"`
	Kind  string `json:"kind"`
	MsgID uint32 `json:"msg_id"`
	Body  []byte `json:"body"`
}

// Mode names. We probe the server's track set after connect to
// detect mode automatically (naive announces one `data` track,
// multistream announces five named tracks).
const (
	modeNaive       = "naive"
	modeMultistream = "multistream"
)

func main() {
	var (
		turns    = flag.Int("turns", 100, "number of CUA turns to run")
		turnGap  = flag.Duration("turn-gap", 200*time.Millisecond, "delay between turn starts")
		warmup   = flag.Int("warmup", 5, "warmup turns to discard before measuring")
		quiet    = flag.Bool("quiet", false, "suppress per-turn log lines")
		jsonOut  = flag.Bool("json", false, "print final summary as one JSON line on stdout (in addition to the human report)")
	)
	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: client [flags] <share-link>")
		flag.PrintDefaults()
		os.Exit(2)
	}
	shareURL := flag.Arg(0)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	c, err := client.Dial(ctx, shareURL, client.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	sdp := c.SDP()
	fmt.Printf("connected sid=%s\n", c.SessionID())
	fmt.Printf("sdp: codec=%s %dx%d@%d\n", sdp.Codec, sdp.Width, sdp.Height, sdp.FPS)

	mode, tracks := detectMode(ctx, c)
	fmt.Printf("detected mode: %s (tracks: %v)\n\n", mode, tracks)

	// Subscribe to every response-carrying track on its own
	// goroutine. Each goroutine demuxes the JSON, finds the
	// pending msg_id, and resolves its arrival channel.
	pending := &pendingMap{m: make(map[uint32]chan time.Time)}
	for _, t := range tracks {
		if t == "screen" || t == "primary" {
			// Skip screen (background traffic, not request responses)
			// and "primary" (default empty track that always exists).
			continue
		}
		go subscribeResponses(ctx, c, t, pending)
	}

	// Also receive datagrams for metadata (server may send a11y/dom/perf
	// via unreliable datagrams in multistream mode to bypass stream congestion).
	go receiveDatagramResponses(ctx, c, pending)

	st := status.New("cua-client[" + mode + "]")
	stopTick := st.Tick(500*time.Millisecond, func(l *status.Line) {
		l.Field("turns", "%d/%d", st.Get("turns"), int64(*turns))
		l.Raw("responses=%d", st.Get("responses"))
		l.Raw("drops=%d", st.Get("drops"))
	})

	stats := newRecorder()

	var msgSeq atomic.Uint32

	for turn := uint32(0); turn < uint32(*turns); turn++ {
		if ctx.Err() != nil {
			break
		}
		// 4 requests in tight succession via datagrams. Each gets a
		// pending arrival channel.
		t0 := time.Now()
		ids := map[string]uint32{}
		ackCh := make(map[string]chan time.Time, 4)
		for _, kind := range []string{"action", "a11y", "dom", "perf"} {
			id := msgSeq.Add(1)
			ch := make(chan time.Time, 1)
			pending.set(id, ch)
			ids[kind] = id
			ackCh[kind] = ch
		}

		// Send all 4 requests. Datagrams are tiny so this is microseconds.
		for kind, id := range ids {
			req := request{Turn: turn, Kind: kind, MsgID: id}
			payload, _ := json.Marshal(req)
			if err := c.SendDatagram(payload); err != nil {
				log.Printf("send err: %v", err)
			}
		}

		// Await all 4 responses (with a per-turn budget so a lost
		// datagram doesn't hang the whole run).
		turnCtx, turnCancel := context.WithTimeout(ctx, 5*time.Second)
		perKindLat := map[string]time.Duration{}
		var maxLat time.Duration
		ok := true
		for kind, ch := range ackCh {
			select {
			case arrival := <-ch:
				lat := arrival.Sub(t0)
				perKindLat[kind] = lat
				if lat > maxLat {
					maxLat = lat
				}
			case <-turnCtx.Done():
				ok = false
				st.Inc("drops", 1)
				pending.delete(ids[kind])
			}
		}
		turnCancel()

		st.Inc("turns", 1)
		st.Inc("responses", int64(len(perKindLat)))

		if !ok {
			if !*quiet {
				fmt.Fprintf(os.Stderr, "turn %d: incomplete (one or more responses dropped)\n", turn)
			}
			continue
		}

		if int(turn) >= *warmup {
			stats.record(perKindLat, maxLat)
		}

		if !*quiet && turn%10 == 0 {
			fmt.Printf("turn %3d: action=%5.1fms a11y=%5.1fms dom=%5.1fms perf=%5.1fms (max=%5.1fms)\n",
				turn,
				ms(perKindLat["action"]), ms(perKindLat["a11y"]),
				ms(perKindLat["dom"]), ms(perKindLat["perf"]),
				ms(maxLat))
		}

		// Gap between turns so we don't go straight back to back.
		select {
		case <-ctx.Done():
		case <-time.After(*turnGap):
		}
	}

	stopTick()

	// Compute + print stats.
	report := stats.summarize(*turns - *warmup)
	printReport(mode, report, *turns, *warmup)
	if *jsonOut {
		printJSON(mode, report, *turns, *warmup)
	}
}

// detectMode polls RemoteTracks() until the announce list looks
// like one of the two known shapes, or 1s elapses. The server
// announces tracks at session start; on a healthy localhost this
// completes within ~50 ms.
func detectMode(ctx context.Context, c *client.Client) (string, []string) {
	deadline := time.Now().Add(1 * time.Second)
	var tracks []string
	for time.Now().Before(deadline) {
		tracks = c.RemoteTracks()
		if len(tracks) >= 1 {
			// Stable across one more poll.
			time.Sleep(50 * time.Millisecond)
			tracks2 := c.RemoteTracks()
			if len(tracks2) >= len(tracks) {
				tracks = tracks2
				break
			}
		}
		select {
		case <-ctx.Done():
			return modeMultistream, tracks
		case <-time.After(20 * time.Millisecond):
		}
	}
	// "primary" always exists (server.New creates it for back-compat).
	hasData := false
	hasMulti := 0
	for _, t := range tracks {
		switch t {
		case "data":
			hasData = true
		case "screen", "a11y", "dom", "perf", "acks":
			hasMulti++
		}
	}
	if hasData && hasMulti == 0 {
		return modeNaive, tracks
	}
	return modeMultistream, tracks
}

func subscribeResponses(ctx context.Context, c *client.Client, trackName string, pending *pendingMap) {
	for {
		au, err := c.RecvOn(ctx, trackName)
		if err != nil {
			return
		}
		arrival := time.Now()
		// Parse JSON response. Naive mode interleaves screen AUs
		// (binary PNG bytes) on the same track — those won't parse
		// as JSON and we skip them.
		var resp response
		if err := json.Unmarshal(au.Bytes, &resp); err != nil {
			continue
		}
		if ch := pending.popMsg(resp.MsgID); ch != nil {
			select {
			case ch <- arrival:
			default:
			}
		}
	}
}

func receiveDatagramResponses(ctx context.Context, c *client.Client, pending *pendingMap) {
	for {
		data, err := c.ReceiveDatagram(ctx)
		if err != nil {
			return
		}
		arrival := time.Now()
		var resp response
		if err := json.Unmarshal(data, &resp); err != nil {
			continue
		}
		if ch := pending.popMsg(resp.MsgID); ch != nil {
			select {
			case ch <- arrival:
			default:
			}
		}
	}
}

// pendingMap is a tiny thread-safe map[msg_id]→arrival channel.
type pendingMap struct {
	mu sync.Mutex
	m  map[uint32]chan time.Time
}

func (p *pendingMap) set(id uint32, ch chan time.Time) {
	p.mu.Lock()
	p.m[id] = ch
	p.mu.Unlock()
}
func (p *pendingMap) popMsg(id uint32) chan time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	ch, ok := p.m[id]
	if !ok {
		return nil
	}
	delete(p.m, id)
	return ch
}
func (p *pendingMap) delete(id uint32) {
	p.mu.Lock()
	delete(p.m, id)
	p.mu.Unlock()
}

// recorder accumulates samples per kind + per turn.
type recorder struct {
	mu      sync.Mutex
	perKind map[string][]time.Duration
	perTurn []time.Duration
}

func newRecorder() *recorder {
	return &recorder{
		perKind: map[string][]time.Duration{
			"action": nil, "a11y": nil, "dom": nil, "perf": nil,
		},
	}
}

func (r *recorder) record(perKind map[string]time.Duration, maxLat time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, v := range perKind {
		r.perKind[k] = append(r.perKind[k], v)
	}
	r.perTurn = append(r.perTurn, maxLat)
}

type stat struct {
	count    int
	p50, p95 time.Duration
	max      time.Duration
	mean     time.Duration
}

type summary struct {
	PerKind map[string]stat
	PerTurn stat
}

func (r *recorder) summarize(_ int) summary {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := summary{PerKind: map[string]stat{}}
	for k, vs := range r.perKind {
		out.PerKind[k] = computeStat(vs)
	}
	out.PerTurn = computeStat(r.perTurn)
	return out
}

func computeStat(vs []time.Duration) stat {
	if len(vs) == 0 {
		return stat{}
	}
	sorted := make([]time.Duration, len(vs))
	copy(sorted, vs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var sum time.Duration
	for _, v := range sorted {
		sum += v
	}
	return stat{
		count: len(sorted),
		p50:   pct(sorted, 0.50),
		p95:   pct(sorted, 0.95),
		max:   sorted[len(sorted)-1],
		mean:  sum / time.Duration(len(sorted)),
	}
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

func printReport(mode string, s summary, turns, warmup int) {
	fmt.Printf("\n══════════ measured latency · mode=%s ══════════\n", mode)
	fmt.Printf("turns=%d (%d warmup discarded, %d measured)\n",
		turns, warmup, s.PerTurn.count)
	fmt.Printf("                p50       p95        max       mean      n\n")
	for _, kind := range []string{"action", "a11y", "dom", "perf"} {
		st := s.PerKind[kind]
		fmt.Printf("  %-9s   %7.2fms %7.2fms  %7.2fms  %7.2fms  %d\n",
			kind, ms(st.p50), ms(st.p95), ms(st.max), ms(st.mean), st.count)
	}
	fmt.Println("  ────────────────────────────────────────────────")
	fmt.Printf("  %-9s   %7.2fms %7.2fms  %7.2fms  %7.2fms  %d\n",
		"per-turn", ms(s.PerTurn.p50), ms(s.PerTurn.p95), ms(s.PerTurn.max), ms(s.PerTurn.mean), s.PerTurn.count)
	fmt.Println("  (per-turn = max of action/a11y/dom/perf arrival)")
	fmt.Println()
	fmt.Println("To compare: re-run against the OTHER mode and put the two")
	fmt.Println("tables side by side. The small-response p95 numbers are")
	fmt.Println("where the multistream advantage surfaces — those responses")
	fmt.Println("ride dedicated streams instead of queuing behind screen AUs.")
	fmt.Println()
}

func printJSON(mode string, s summary, turns, warmup int) {
	type js struct {
		Mode    string             `json:"mode"`
		Turns   int                `json:"turns"`
		Warmup  int                `json:"warmup"`
		PerKind map[string]any     `json:"per_kind"`
		PerTurn map[string]float64 `json:"per_turn"`
	}
	out := js{Mode: mode, Turns: turns, Warmup: warmup, PerKind: map[string]any{}}
	for kind, st := range s.PerKind {
		out.PerKind[kind] = map[string]float64{
			"p50_ms": ms(st.p50), "p95_ms": ms(st.p95),
			"max_ms": ms(st.max), "mean_ms": ms(st.mean),
			"count": float64(st.count),
		}
	}
	out.PerTurn = map[string]float64{
		"p50_ms": ms(s.PerTurn.p50), "p95_ms": ms(s.PerTurn.p95),
		"max_ms": ms(s.PerTurn.max), "mean_ms": ms(s.PerTurn.mean),
		"count": float64(s.PerTurn.count),
	}
	b, _ := json.Marshal(out)
	fmt.Println(string(b))
}
