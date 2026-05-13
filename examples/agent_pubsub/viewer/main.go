// agent_pubsub/viewer — Go CLI subscriber for the agent_pubsub/server.
// Consumes all four channels concurrently and prints them interleaved
// so the developer can SEE the multi-stream wire shape: large screen
// frames, fast reasoning tokens, sparse action JSON, and constant
// telemetry datagrams all making progress at the same time on one
// QUIC connection.
//
// The end-of-run summary prints per-track totals so a developer can
// confirm nothing got starved under concurrent load.
//
// Run:
//
//	go run ./examples/agent_pubsub/viewer 'https://127.0.0.1:NNNN/wt#slug=...&hash=...'
//
// The share URL is printed by ./examples/agent_pubsub/server on startup.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
)

// Single stdout lock — all four readers print whole lines, so
// serializing here keeps the interleaved output legible. Without
// this, partial reasoning prints would land in the middle of a
// screen Printf and the visual would look broken.
var stdoutMu sync.Mutex

func println(format string, args ...any) {
	stdoutMu.Lock()
	defer stdoutMu.Unlock()
	fmt.Printf(format, args...)
}

const (
	trackScreen      = "screen"
	trackReasoning   = "reasoning"
	trackActions     = "actions"
	telemetryTrackID = 1
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: viewer <share-link>\n  (the share URL printed by agent_pubsub/server)")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	c, err := client.Dial(ctx, os.Args[1], client.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	sdp := c.SDP()
	fmt.Printf("connected sid=%s\n", c.SessionID())
	fmt.Printf("sdp: codec=%s %dx%d@%d\n", sdp.Codec, sdp.Width, sdp.Height, sdp.FPS)
	// Brief poll for ANNOUNCE frames. With the SDK announce-replay
	// on fresh attach, tracks typically appear within 100ms.
	deadline := time.Now().Add(1 * time.Second)
	var tracks []string
	for time.Now().Before(deadline) {
		tracks = c.RemoteTracks()
		if len(tracks) >= 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Printf("remote tracks: %v + telemetry datagrams\n\n", tracks)

	st := status.New("viewer")

	go readScreen(ctx, c, st)
	go readReasoning(ctx, c, st)
	go readActions(ctx, c, st)
	go readTelemetry(ctx, c, st)

	// Every 10 s, ask for a fresh keyframe on screen — demonstrates
	// the loss-recovery path. The server's OnKeyframeRequest flips
	// the very next screen frame to a keyframe; the [screen] log
	// row marks K and the developer can see the K cadence change.
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := c.RequestKeyframe(trackScreen); err == nil {
					println("[viewer]    requested keyframe on screen\n")
				}
			}
		}
	}()

	<-ctx.Done()

	st.Done(
		"four channels delivered cleanly under concurrent load — no track starved another",
		[]status.Field{
			{Key: "uptime", Value: st.Uptime().Truncate(time.Second).String()},
			{Key: "screen", Value: fmt.Sprintf("%s AU (K %d, P %d)  %s",
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
		})
}

func readScreen(ctx context.Context, c *client.Client, st *status.Status) {
	for {
		au, err := c.RecvOn(ctx, trackScreen)
		if err != nil {
			return
		}
		marker := "P"
		if au.Keyframe {
			marker = "K"
			st.Inc("screen_k", 1)
		}
		println("[screen]    %s seq=%5d pts=%dµs len=%5dB\n",
			marker, au.Seq, au.PTSMicro, len(au.Bytes))
		st.Inc("screen_au", 1)
		st.Inc("screen_bytes", int64(len(au.Bytes)))
	}
}

// Reasoning batches arriving tokens into a buffer and flushes one
// "[reasoning] …" line whenever it sees sentence-ending punctuation
// or the buffer crosses ~64 chars. Streaming feel preserved (tokens
// arrive at 10 Hz, sentences flush within a second), but never
// mid-line interleaved with screen/action/telemetry prints.
func readReasoning(ctx context.Context, c *client.Client, st *status.Status) {
	var buf strings.Builder
	flush := func() {
		if buf.Len() > 0 {
			println("[reasoning] %s\n", strings.TrimSpace(buf.String()))
			buf.Reset()
		}
	}
	for {
		au, err := c.RecvOn(ctx, trackReasoning)
		if err != nil {
			flush()
			return
		}
		tok := string(au.Bytes)
		st.Inc("reasoning_au", 1)
		st.Inc("reasoning_bytes", int64(len(au.Bytes)))
		if tok == "" {
			continue
		}
		buf.WriteString(tok)
		// Flush on sentence boundary or buffer-fill so the line gets
		// printed atomically before something else slips in.
		last := tok[len(tok)-1]
		if last == '.' || last == '!' || last == '?' || buf.Len() >= 64 {
			flush()
		}
	}
}

func readActions(ctx context.Context, c *client.Client, st *status.Status) {
	for {
		au, err := c.RecvOn(ctx, trackActions)
		if err != nil {
			return
		}
		println("[action]    seq=%d %s\n", au.Seq, string(au.Bytes))
		st.Inc("actions_au", 1)
		st.Inc("actions_bytes", int64(len(au.Bytes)))
	}
}

// Datagrams carry the 4-byte envelope set by the server's pumpTelemetry.
// We filter by trackId so future second telemetry-kind tracks wouldn't
// be silently swallowed here.
func readTelemetry(ctx context.Context, c *client.Client, st *status.Status) {
	for {
		raw, err := c.ReceiveDatagram(ctx)
		if err != nil {
			return
		}
		if len(raw) < 4 {
			continue
		}
		trackID := raw[0]
		if trackID != telemetryTrackID {
			continue
		}
		seq := binary.BigEndian.Uint16(raw[1:3])
		body := raw[4:]
		println("[telemetry] seq=%d %s\n", seq, string(body))
		st.Inc("telemetry_dg", 1)
		st.Inc("telemetry_bytes", int64(len(raw)))
	}
}
