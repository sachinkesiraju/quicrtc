// relay — 1:N fan-out example.
//
// Subscribes to one or more upstream quicrtc publishers and re-serves
// every received track to downstream subscribers. Wire format is the
// same on both sides — downstreams cannot tell they're talking to a
// relay vs a direct publisher (a teaching point: any of the existing
// publishers can be the upstream and any of the subscribers / the
// browser viewer can be downstream, unmodified).
//
// What this teaches:
//   - relay.New + Config { Server, Upstreams, ClientOptions }
//   - The relay is transparent: downstream view is indistinguishable
//     from a direct publisher
//   - SubscriberCount as a live observable so you can SEE fan-out
//     happen (open another subscriber, watch the count change)
//
// Run:
//
//	go run ./examples/relay \
//	    -listen 127.0.0.1:4445 \
//	    -upstream 'https://127.0.0.1:NNNN/wt#slug=...&hash=...'
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
	"github.com/sachinkesiraju/quicrtc/relay"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/wire"
)

func main() {
	var (
		listen    = flag.String("listen", "127.0.0.1:4445", "downstream listen addr (host:port)")
		upstreams multiFlag
	)
	flag.Var(&upstreams, "upstream", "upstream publisher share link (repeatable)")
	flag.Parse()

	if len(upstreams) == 0 {
		log.Fatal("usage: relay -listen <addr> -upstream <share-link> [-upstream ...]")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	host, _, err := net.SplitHostPort(*listen)
	if err != nil || host == "" {
		host = "127.0.0.1"
	}

	r, err := relay.New(relay.Config{
		Server: server.Config{
			Addr:         *listen,
			CertExtraIPs: []net.IP{net.ParseIP(host)},
			ExternalHost: host,
			SDP:          wire.SDP{Codec: "synthetic", Width: 64, Height: 64, FPS: 30},
		},
		Upstreams:     upstreams,
		ClientOptions: client.Options{HelloTimeout: 5 * time.Second},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Print the share link once the listener has bound a real port.
	go func() {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			addr := r.Server().Addr()
			if addr != "" && !strings.HasSuffix(addr, ":0") {
				fmt.Println("share:", r.Server().ShareLink())
				fmt.Printf("(open more subscribers against this URL to watch fan-out)\n")
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	st := status.New("relay")

	// Track peak subscriber count for the end-of-run summary.
	go func() {
		t := time.NewTicker(200 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n := int64(r.Server().SubscriberCount())
				if n > st.Get("peak_subs") {
					st.Set("peak_subs", n)
				}
			}
		}
	}()

	stopTick := st.Tick(200*time.Millisecond, func(l *status.Line) {
		l.Raw("upstreams=%d (configured)", len(upstreams))
		l.Raw("subs=%d", r.Server().SubscriberCount())
		l.Field("uptime", "%s", st.Uptime().Truncate(time.Second))
	})

	// ListenAndServe blocks until ctx is cancelled.
	runErr := r.ListenAndServe(ctx)
	stopTick()

	if runErr != nil && ctx.Err() == nil {
		log.Printf("relay: %v", runErr)
	}

	st.Done(
		"relay forwards transparently — downstream views are identical to direct publisher",
		[]status.Field{
			{Key: "uptime", Value: st.Uptime().Truncate(time.Second).String()},
			{Key: "upstreams", Value: fmt.Sprintf("%d", len(upstreams))},
			{Key: "peak subs", Value: fmt.Sprintf("%d", st.Get("peak_subs"))},
		})
}

// multiFlag accumulates repeated -upstream flags.
type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(s string) error { *m = append(*m, s); return nil }
