// publisher — the minimal "hello world" of quicrtc.
//
// Stands up a server, registers ONE video track with an explicit
// track.Kind, and publishes 4 KB synthetic frames at 30 fps. Prints
// the share URL on stdout — paste it into ./examples/subscriber or
// ts-sdk/examples/viewer/ to consume.
//
// What this teaches:
//   - server.New + server.Config (cert auto-generated)
//   - AddTrackSpec with explicit Kind (the recommended path — never
//     the legacy Publisher() singular)
//   - OnSession to log when subscribers attach
//   - The live status line + end-of-run summary pattern used by
//     every example in this repo
//
// Run:
//
//	go run ./examples/publisher
package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

const (
	trackName = "video"
	gop       = 30 // keyframe every 30 frames at 30 fps = 1s
	frameSize = 4 * 1024
)

func main() {
	st := status.New("publisher")

	srv, err := server.New(server.Config{
		Addr:         "127.0.0.1:0",
		CertExtraIPs: []net.IP{net.ParseIP("127.0.0.1")},
		ExternalHost: "127.0.0.1",
		SDP: wire.SDP{
			Codec: "synthetic", Width: 64, Height: 64, FPS: 30,
		},
		OnSession: func(h server.SessionHandle) {
			st.Inc("sessions", 1)
			log.Printf("session connected")
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Register the track WITH an explicit Kind. Without this, the
	// track falls through to the legacy stream-per-GOP path — fine
	// for video, but a learning trap for token/telemetry tracks.
	pub := srv.AddTrackSpec(server.TrackSpec{
		Name: trackName,
		Kind: track.KindVideo,
	})

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

	// Live status — updates in place on a TTY, silent on pipe/file.
	stopTick := st.Tick(200*time.Millisecond, func(l *status.Line) {
		l.Field("sessions", "%d", st.Get("sessions"))
		l.Raw("frames=%d K=%d P=%d",
			st.Get("frames"),
			st.Get("kframes"),
			st.Get("frames")-st.Get("kframes"))
		l.Raw("%s", status.HumanBytes(st.Get("bytes")))
		secs := st.Uptime().Seconds()
		if secs > 0.1 {
			l.Raw("%.1f fps", float64(st.Get("frames"))/secs)
		}
	})

	tick := time.NewTicker(33 * time.Millisecond) // ~30 fps
	defer tick.Stop()
	var seq uint32
	buf := make([]byte, frameSize)

	for {
		select {
		case <-ctx.Done():
			stopTick()
			st.Done(
				"single-track delivery — wire is up, AUs flowed, no drops detected",
				[]status.Field{
					{Key: "uptime", Value: st.Uptime().Truncate(time.Second).String()},
					{Key: "sessions", Value: fmt.Sprintf("%d", st.Get("sessions"))},
					{Key: "AU sent", Value: fmt.Sprintf("%s (K %d, P %d)",
						status.HumanCount(st.Get("frames")),
						st.Get("kframes"),
						st.Get("frames")-st.Get("kframes"))},
					{Key: "bytes", Value: status.HumanBytes(st.Get("bytes"))},
					{Key: "avg rate", Value: fmt.Sprintf("%.1f fps",
						float64(st.Get("frames"))/st.Uptime().Seconds())},
				})
			return
		case <-tick.C:
			_, _ = rand.Read(buf)
			au := pubsub.AccessUnit{
				Bytes:    buf,
				Keyframe: seq%gop == 0,
				PTSMicro: uint64(seq) * 33333,
				Seq:      seq,
			}
			if err := pub.Publish(ctx, au); err == nil {
				st.Inc("frames", 1)
				st.Inc("bytes", int64(len(buf)))
				if au.Keyframe {
					st.Inc("kframes", 1)
				}
			}
			seq++
		}
	}
}
