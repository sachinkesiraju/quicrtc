// subscriber — the minimal "hello world" subscriber.
//
// Dials a quicrtc server using its share URL, prints the SDP, and
// reads AUs from the named track. Companion to ./examples/publisher.
//
// What this teaches:
//   - client.Dial — slug + cert hash are parsed from the URL fragment
//     by the SDK; no manual fragment parsing required
//   - RecvOn(track) — the multi-track-aware receive primitive
//     (preferred over Recv() which is the legacy "primary" shim)
//   - The shared live status + summary pattern
//
// Run:
//
//	go run ./examples/subscriber 'https://127.0.0.1:NNNN/wt#slug=...&hash=...'
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
)

const trackName = "video"

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: subscriber <share-link>\n  (the URL printed by ./examples/publisher)")
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
	fmt.Printf("subscribing to: %s\n\n", trackName)

	st := status.New("subscriber")
	stopTick := st.Tick(200*time.Millisecond, func(l *status.Line) {
		l.Raw("frames=%d", st.Get("frames"))
		if lastK := st.Get("last_k_seq"); lastK >= 0 {
			ageMs := st.Get("last_k_age_ms")
			l.Raw("last K @ seq=%d (%dms ago)", lastK, ageMs)
		}
		l.Raw("gaps=%d", st.Get("gaps"))
		l.Raw("%s", status.HumanBytes(st.Get("bytes")))
	})

	// Tick that recomputes last_k_age_ms every 100ms.
	go func() {
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				if lastKAt := st.Get("last_k_at_ms"); lastKAt > 0 {
					age := now.UnixMilli() - lastKAt
					st.Set("last_k_age_ms", age)
				}
			}
		}
	}()

	var lastSeq int64 = -1
	st.Set("last_k_seq", -1)

	for {
		au, err := c.RecvOn(ctx, trackName)
		if err != nil {
			stopTick()
			headline := "single-track receive complete — wire was clean"
			if st.Get("gaps") > 0 {
				headline = fmt.Sprintf(
					"single-track receive complete — %d gap(s) detected (likely from joining mid-stream)",
					st.Get("gaps"))
			}
			st.Done(
				headline,
				[]status.Field{
					{Key: "uptime", Value: st.Uptime().Truncate(time.Second).String()},
					{Key: "AU recv", Value: fmt.Sprintf("%s (K %d, P %d)",
						status.HumanCount(st.Get("frames")),
						st.Get("kframes"),
						st.Get("frames")-st.Get("kframes"))},
					{Key: "gaps", Value: fmt.Sprintf("%d", st.Get("gaps"))},
					{Key: "bytes", Value: status.HumanBytes(st.Get("bytes"))},
				})
			if ctx.Err() == nil {
				fmt.Println("end:", err)
			}
			return
		}
		// Gap detection (informational).
		if lastSeq >= 0 && int64(au.Seq) > lastSeq+1 {
			st.Inc("gaps", 1)
		}
		lastSeq = int64(au.Seq)
		st.Inc("frames", 1)
		st.Inc("bytes", int64(len(au.Bytes)))
		if au.Keyframe {
			st.Inc("kframes", 1)
			st.Set("last_k_seq", int64(au.Seq))
			st.Set("last_k_at_ms", time.Now().UnixMilli())
		}
	}
}
