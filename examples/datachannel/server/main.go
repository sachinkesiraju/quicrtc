// datachannel/server — companion to ./examples/datachannel/client.
//
// Demonstrates the bidirectional, persistent control channel.
// For each connecting client, this server:
//
//   1. Echoes every message back with an "ack:" prefix.
//   2. Pushes a 1 Hz heartbeat carrying its uptime.
//
// Both directions run concurrently per session — the control channel
// is not request/reply. It's a long-lived bidi stream that either
// side can write to at any time.
//
// Run:
//
//	go run ./examples/datachannel/server
//
// Then in another terminal:
//
//	go run ./examples/datachannel/client '<share-link>'
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/sachinkesiraju/quicrtc/datachannel"
	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/wire"
)

func main() {
	st := status.New("server")
	var channels atomic.Int64

	srv, err := server.New(server.Config{
		Addr:         "127.0.0.1:0",
		CertExtraIPs: []net.IP{net.ParseIP("127.0.0.1")},
		ExternalHost: "127.0.0.1",
		// SDP is required even though this example doesn't publish a
		// media track — the field is part of the handshake. Width/
		// height/fps are cosmetic when no track is delivered.
		SDP: wire.SDP{Codec: "none", Width: 0, Height: 0, FPS: 0},
		OnDataChannel: func(dc *datachannel.Channel) {
			channels.Add(1)
			st.Set("channels", channels.Load())
			go handleChannel(dc, st)
		},
	})
	if err != nil {
		log.Fatal(err)
	}

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

	stopTick := st.Tick(200*time.Millisecond, func(l *status.Line) {
		l.Field("channels", "%d", st.Get("channels"))
		l.Raw("recv=%d", st.Get("recv"))
		l.Raw("sent=%d (%d echo + %d heartbeat)",
			st.Get("echo")+st.Get("heartbeat"),
			st.Get("echo"),
			st.Get("heartbeat"))
	})

	<-ctx.Done()
	stopTick()
	st.Done(
		"datachannel is bidi and persistent — server pushed heartbeats while echoing client commands",
		[]status.Field{
			{Key: "uptime", Value: st.Uptime().Truncate(time.Second).String()},
			{Key: "channels seen", Value: fmt.Sprintf("%d", st.Get("channels"))},
			{Key: "msgs received", Value: fmt.Sprintf("%d", st.Get("recv"))},
			{Key: "echoes sent", Value: fmt.Sprintf("%d", st.Get("echo"))},
			{Key: "heartbeats sent", Value: fmt.Sprintf("%d", st.Get("heartbeat"))},
		})
}

// handleChannel runs two goroutines:
//   - reader: drain client messages, send echo replies.
//   - heartbeat: every 1 s, send an uptime ping.
// Both share the same datachannel; the SDK's mutex makes concurrent
// Send safe.
func handleChannel(dc *datachannel.Channel, st *status.Status) {
	start := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				msg := fmt.Sprintf("heartbeat (uptime %ds)",
					int(now.Sub(start).Seconds()))
				if err := dc.Send([]byte(msg)); err != nil {
					return
				}
				st.Inc("heartbeat", 1)
			}
		}
	}()

	for {
		msg, err := dc.Recv(ctx)
		if err != nil {
			return
		}
		st.Inc("recv", 1)
		if err := dc.Send([]byte("ack:" + string(msg))); err != nil {
			return
		}
		st.Inc("echo", 1)
	}
}
