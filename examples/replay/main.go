// replay — a session observability / replay viewer for quicrtc
// recordings.
//
// A recording (see the record package) is an append-only binary log of
// access units across tracks, each frame stamped with a capture time.
// This tool builds an in-memory index over that file and prints, on one
// shared clock, what the agent SAW (screen video), REASONED (token
// text), and DID (tool calls) — the three lanes you want aligned when
// an agent run goes sideways.
//
// What this teaches:
//   - record.BuildTimeline: one-pass index of every frame's metadata
//   - Timeline.Frames(): merged capture-time order across all tracks
//   - Timeline.SeekSnapshot(t): the cross-track state at a moment
//
// Run:
//
//	go run ./examples/replay -gen sample.qrtc               # write a sample recording to try
//	go run ./examples/replay -file session.qrtc
//	go run ./examples/replay -file session.qrtc -at 1500   # snapshot at 1500ms
//	go run ./examples/replay -file session.qrtc -json       # machine-readable
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/record"
	"github.com/sachinkesiraju/quicrtc/track"
)

func main() {
	file := flag.String("file", "", "path to a .qrtc recording (required unless -gen)")
	at := flag.Int64("at", -1, "print the cross-track snapshot at this capture-time, in ms from session start")
	asJSON := flag.Bool("json", false, "emit the timeline as JSON for tooling")
	payloads := flag.Bool("payloads", false, "with -json, base64-embed each frame's payload (feeds the browser scrubber)")
	gen := flag.String("gen", "", "synthesize a sample recording at this path and exit (try the tool with no real session)")
	imp := flag.String("import", "", "build a recording from a capture manifest.json; writes to the -gen path")
	flag.Parse()

	if *imp != "" {
		if *gen == "" {
			fmt.Fprintln(os.Stderr, "replay: -import needs -gen <out.qrtc> for the output path")
			os.Exit(2)
		}
		importManifest(*imp, *gen)
		return
	}
	if *gen != "" {
		generateSample(*gen)
		return
	}

	if *file == "" {
		fmt.Fprintln(os.Stderr, "replay: -file is required")
		flag.Usage()
		os.Exit(2)
	}

	tl, err := record.BuildTimeline(*file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay: %v\n", err)
		os.Exit(1)
	}

	frames := tl.Frames()
	if len(frames) == 0 {
		fmt.Fprintln(os.Stderr, "replay: recording contains no frames")
		os.Exit(1)
	}
	// The first captured frame is t=0 on our display clock. Capture
	// times are absolute wall-clock micros, so we rebase to the session
	// start for human-friendly relative timestamps.
	origin := frames[0].CapturedAtMicros

	switch {
	case *asJSON:
		emitJSON(tl, frames, origin, *payloads)
	case *at >= 0:
		printSnapshot(tl, origin, *at)
	default:
		printTimeline(tl, frames, origin)
	}
}

// printTimeline writes the human-readable, capture-time-ordered log:
// one line per frame with its relative time, track, kind, size,
// keyframe marker, and a short payload preview.
func printTimeline(tl *record.Timeline, frames []record.Entry, origin int64) {
	fmt.Printf("recording: %d frames across %d track(s)\n", tl.Len(), len(tl.Tracks()))
	for _, t := range tl.Tracks() {
		fmt.Printf("  track %-12s kind=%s\n", t.Name, t.Kind)
	}
	fmt.Println()
	fmt.Printf("%-10s  %-12s  %-9s  %8s  %s  %s\n", "TIME", "TRACK", "KIND", "SIZE", "KF", "PAYLOAD")
	for _, e := range frames {
		fmt.Printf("%-10s  %-12s  %-9s  %8d  %s  %s\n",
			relTime(e.CapturedAtMicros, origin),
			e.TrackName,
			e.Kind,
			e.PayloadLen,
			keyframeMark(e.Keyframe),
			preview(tl, e),
		)
	}
}

// printSnapshot reconstructs and prints the state of every track at the
// given relative capture-time (ms). This is the "what was the world at
// moment t" view: the screen frame on the wire, the last tokens, the
// last tool call.
func printSnapshot(tl *record.Timeline, origin, atMS int64) {
	target := origin + atMS*1000 // ms -> micros, rebased onto the absolute clock
	snap := tl.SeekSnapshot(target)
	fmt.Printf("snapshot at t=%dms (%d track(s) active)\n\n", atMS, len(snap))
	if len(snap) == 0 {
		fmt.Println("  (nothing had been recorded on any track yet)")
		return
	}
	// Sort the snapshot's tracks for stable output.
	keys := make([]record.Track, 0, len(snap))
	for k := range snap {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Name < keys[j].Name })
	for _, k := range keys {
		e := snap[k]
		fmt.Printf("  %-12s kind=%-9s  @%-9s seq=%-4d size=%-6d %s\n",
			e.TrackName, e.Kind,
			relTime(e.CapturedAtMicros, origin),
			e.Seq, e.PayloadLen, keyframeMark(e.Keyframe))
		fmt.Printf("    %s\n", preview(tl, e))
	}
}

// timelineJSON is the wire shape of the -json output. Times are emitted
// both as absolute micros and as relative milliseconds so downstream
// tools can pick whichever clock they prefer.
type timelineJSON struct {
	Tracks []trackJSON `json:"tracks"`
	Frames []frameJSON `json:"frames"`
}

type trackJSON struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

type frameJSON struct {
	Track            string `json:"track"`
	Kind             string `json:"kind"`
	RelMS            int64  `json:"rel_ms"`
	CapturedAtMicros int64  `json:"captured_at_micros"`
	PTSMicros        uint64 `json:"pts_micros"`
	Seq              uint32 `json:"seq"`
	Keyframe         bool   `json:"keyframe"`
	PayloadLen       int    `json:"payload_len"`
	Preview          string `json:"preview"`
	PayloadB64       string `json:"payload_b64,omitempty"`
}

func emitJSON(tl *record.Timeline, frames []record.Entry, origin int64, withPayloads bool) {
	out := timelineJSON{}
	for _, t := range tl.Tracks() {
		out.Tracks = append(out.Tracks, trackJSON{Name: t.Name, Kind: t.Kind})
	}
	for _, e := range frames {
		fj := frameJSON{
			Track:            e.TrackName,
			Kind:             e.Kind,
			RelMS:            (e.CapturedAtMicros - origin) / 1000,
			CapturedAtMicros: e.CapturedAtMicros,
			PTSMicros:        e.PTSMicros,
			Seq:              e.Seq,
			Keyframe:         e.Keyframe,
			PayloadLen:       e.PayloadLen,
			Preview:          preview(tl, e),
		}
		if withPayloads {
			if au, err := tl.ReadPayload(e); err == nil {
				fj.PayloadB64 = base64.StdEncoding.EncodeToString(au.Bytes)
			}
		}
		out.Frames = append(out.Frames, fj)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fmt.Fprintf(os.Stderr, "replay: %v\n", err)
		os.Exit(1)
	}
}

// preview renders a short, human-readable look at a frame's payload.
// Text-shaped lanes (tokens, tool calls) decode as UTF-8 and are
// truncated; opaque media lanes (video, telemetry, audio) render as a
// byte count to keep the timeline readable.
func preview(tl *record.Timeline, e record.Entry) string {
	switch track.Kind(e.Kind) {
	case track.KindTokens, track.KindToolCalls:
		au, err := tl.ReadPayload(e)
		if err != nil {
			return fmt.Sprintf("<read error: %v>", err)
		}
		return quoteShort(string(au.Bytes), 80)
	default:
		// Video, audio, telemetry, or unknown — don't dump binary.
		return fmt.Sprintf("<%d bytes>", e.PayloadLen)
	}
}

// quoteShort collapses newlines/tabs to spaces, trims, and truncates a
// text payload to max runes so a token batch stays on one log line.
func quoteShort(s string, max int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	s = strings.TrimSpace(s)
	r := []rune(s)
	if len(r) > max {
		return strconv.Quote(string(r[:max])) + "…"
	}
	return strconv.Quote(s)
}

// relTime formats a capture timestamp relative to the session origin as
// a short HH:MM:SS.mmm-style duration.
func relTime(capturedAt, origin int64) string {
	d := time.Duration(capturedAt-origin) * time.Microsecond
	ms := d.Milliseconds()
	return fmt.Sprintf("+%d.%03ds", ms/1000, ms%1000)
}

func keyframeMark(kf bool) string {
	if kf {
		return "K"
	}
	return "."
}

// generateSample writes a small computer-use session to path so the
// replay tooling can be tried without a real recording: a login task
// with screen frames interleaved with reasoning tokens, tool calls,
// and a couple of telemetry beats. Small sleeps give the frames
// distinct capture timestamps so the merged timeline reads naturally.
func generateSample(path string) {
	rec, err := record.NewFileRecorder(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay -gen: %v\n", err)
		os.Exit(1)
	}
	seq := map[string]uint32{}
	put := func(name, kind string, b []byte, kf bool, gap time.Duration) {
		s := seq[name]
		seq[name] = s + 1
		_ = rec.Write(name, kind, pubsub.AccessUnit{
			Bytes: b, Keyframe: kf, Seq: s, PTSMicro: uint64(time.Now().UnixMicro()),
		})
		time.Sleep(gap)
	}
	tok := func(text string, gap time.Duration) { put("reasoning", string(track.KindTokens), []byte(text), false, gap) }
	tool := func(j string, gap time.Duration) { put("toolcalls", string(track.KindToolCalls), []byte(j), true, gap) }
	tele := func(j string, gap time.Duration) { put("telemetry", string(track.KindTelemetry), []byte(j), false, gap) }
	scr := func(phase string, cx, cy int, kf bool, gap time.Duration) {
		put("screen", string(track.KindVideo), synthScreen(phase, cx, cy), kf, gap)
	}

	scr("login", 60, 70, true, 40*time.Millisecond)
	tok("The page is the demo dashboard login.", 30*time.Millisecond)
	tok("I'll click the email field and type the address.", 30*time.Millisecond)
	tool(`{"action":"left_click","x":190,"y":140}`, 50*time.Millisecond)
	scr("email_filled", 190, 140, false, 40*time.Millisecond)
	tool(`{"action":"type","text":"demo@quicrtc.dev"}`, 30*time.Millisecond)
	tele(`{"heap_mb":38,"goroutines":9,"frame":2}`, 20*time.Millisecond)
	scr("creds_filled", 200, 226, false, 40*time.Millisecond)
	tok("Credentials entered; submitting the form.", 30*time.Millisecond)
	tool(`{"action":"key","key":"Return"}`, 60*time.Millisecond)
	scr("dashboard", 320, 168, true, 40*time.Millisecond)
	tok("Dashboard loaded. The payout total reads $48,120.", 20*time.Millisecond)
	tele(`{"heap_mb":41,"goroutines":11,"frame":4}`, 0)
	if err := rec.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "replay -gen: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("wrote sample recording to %s — try: go run ./examples/replay -file %s\n", path, path)
}

// importManifest builds a real quicrtc recording from an external capture:
// a manifest.json listing frames (per frame: track, kind, rel_ms,
// keyframe, and either inline text or a file path to a payload — e.g. a
// PNG screenshot from a headless browser). It replays the capture's
// relative timing with sleeps so the recording's capture clock matches the
// original session. This is how a Playwright/CDP capture — or any external
// trace — becomes a scrubber-ready .qrtc through the real record pipeline.
func importManifest(manifestPath, out string) {
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay -import: %v\n", err)
		os.Exit(1)
	}
	var m struct {
		Frames []struct {
			Track    string `json:"track"`
			Kind     string `json:"kind"`
			RelMS    int64  `json:"rel_ms"`
			Keyframe bool   `json:"keyframe"`
			Text     string `json:"text"`
			File     string `json:"file"`
		} `json:"frames"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		fmt.Fprintf(os.Stderr, "replay -import: %v\n", err)
		os.Exit(1)
	}
	sort.SliceStable(m.Frames, func(i, j int) bool { return m.Frames[i].RelMS < m.Frames[j].RelMS })
	rec, err := record.NewFileRecorder(out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay -import: %v\n", err)
		os.Exit(1)
	}
	seq := map[string]uint32{}
	var last int64
	for _, f := range m.Frames {
		if f.RelMS > last {
			time.Sleep(time.Duration(f.RelMS-last) * time.Millisecond)
			last = f.RelMS
		}
		b := []byte(f.Text)
		if f.File != "" {
			if fb, e := os.ReadFile(f.File); e == nil {
				b = fb
			} else {
				fmt.Fprintf(os.Stderr, "replay -import: %v\n", e)
			}
		}
		s := seq[f.Track]
		seq[f.Track] = s + 1
		_ = rec.Write(f.Track, f.Kind, pubsub.AccessUnit{
			Bytes: b, Keyframe: f.Keyframe, Seq: s, PTSMicro: uint64(time.Now().UnixMicro()),
		})
	}
	if err := rec.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "replay -import: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("imported %d frames -> %s\n", len(m.Frames), out)
}

// synthScreen renders a tiny synthesized "browser window" PNG for one
// phase of the sample login flow, with a red cursor crosshair at (cx,cy).
// Real image bytes — so the browser scrubber can render the screen lane
// and you watch the page change as you drag the playhead.
func synthScreen(phase string, cx, cy int) []byte {
	const W, H = 480, 320
	img := image.NewRGBA(image.Rect(0, 0, W, H))
	fill := func(x0, y0, x1, y1 int, c color.RGBA) {
		if x0 < 0 {
			x0 = 0
		}
		if y0 < 0 {
			y0 = 0
		}
		if x1 > W {
			x1 = W
		}
		if y1 > H {
			y1 = H
		}
		for y := y0; y < y1; y++ {
			for x := x0; x < x1; x++ {
				img.SetRGBA(x, y, c)
			}
		}
	}
	white := color.RGBA{255, 255, 255, 255}
	chrome := color.RGBA{232, 234, 238, 255}
	field := color.RGBA{238, 240, 245, 255}
	filled := color.RGBA{198, 221, 255, 255}
	blue := color.RGBA{56, 121, 217, 255}
	green := color.RGBA{34, 170, 100, 255}
	dark := color.RGBA{70, 76, 88, 255}

	fill(0, 0, W, H, white)
	// window chrome: traffic lights + address bar with a lock + URL.
	fill(0, 0, W, 26, chrome)
	fill(12, 9, 22, 19, color.RGBA{237, 106, 94, 255})
	fill(28, 9, 38, 19, color.RGBA{245, 191, 79, 255})
	fill(44, 9, 54, 19, color.RGBA{98, 197, 84, 255})
	fill(40, 32, W-12, 52, field)
	fill(46, 37, 56, 47, green)
	fill(62, 40, 250, 44, dark)

	if phase == "dashboard" {
		fill(0, 60, W, 98, blue)     // header bar
		fill(18, 73, 130, 85, white) // logo
		for r := 0; r < 4; r++ {     // content rows
			fill(24, 122+r*32, 250, 122+r*32+20, field)
		}
		fill(280, 116, 456, 210, color.RGBA{226, 246, 233, 255}) // payout card
		fill(296, 130, 360, 138, dark)                           // label
		fill(296, 150, 420, 190, green)                          // "$48,120"
	} else {
		cx0, cy0, cx1 := 130, 84, 350
		fill(cx0, cy0, cx1, 256, color.RGBA{250, 251, 253, 255})
		fill(cx0, cy0, cx1, cy0+26, chrome)
		fill(cx0+20, cy0+10, cx0+110, cy0+18, dark) // "Sign in"
		efill := field
		if phase == "email_filled" || phase == "creds_filled" {
			efill = filled
		}
		fill(cx0+24, cy0+44, cx1-24, cy0+70, efill) // email field
		pfill := field
		if phase == "creds_filled" {
			pfill = filled
		}
		fill(cx0+24, cy0+84, cx1-24, cy0+110, pfill) // password field
		btn := blue
		if phase == "creds_filled" {
			btn = color.RGBA{38, 92, 184, 255}
		}
		fill(cx0+24, cy0+128, cx1-24, cy0+156, btn) // sign-in button
	}
	// red cursor crosshair
	red := color.RGBA{220, 40, 60, 255}
	fill(cx-9, cy-1, cx+10, cy+1, red)
	fill(cx-1, cy-9, cx+1, cy+10, red)

	var buf bytes.Buffer
	_ = png.Encode(&buf, img)
	return buf.Bytes()
}
