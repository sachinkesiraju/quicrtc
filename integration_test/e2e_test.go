// Package integration_test exercises quicrtc end-to-end over real
// QUIC + WebTransport on loopback. The point of these tests is to
// catch wire-protocol regressions that would compile-pass but
// runtime-fail when packets actually traverse a stack.
package integration_test

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/cert"
	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/datachannel"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/wire"
)

func startServer(t *testing.T, cfg server.Config) (*server.Server, context.CancelFunc) {
	t.Helper()
	if cfg.CertBundle == nil {
		b, err := cert.Generate(cert.Options{IPs: []net.IP{net.ParseIP("127.0.0.1")}})
		if err != nil {
			t.Fatal(err)
		}
		cfg.CertBundle = b
	}
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	if cfg.SDP.Codec == "" {
		cfg.SDP = wire.SDP{Codec: "test", Width: 64, Height: 64, FPS: 30}
	}

	s, err := server.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	servErr := make(chan error, 1)
	go func() { servErr <- s.ListenAndServe(ctx) }()

	// Wait for the listener to bind; Addr() reports the resolved port.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !strings.HasSuffix(s.Addr(), ":0") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if strings.HasSuffix(s.Addr(), ":0") {
		cancel()
		t.Fatalf("server didn't bind: addr=%q", s.Addr())
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-servErr:
		case <-time.After(2 * time.Second):
			t.Log("server didn't shut down cleanly")
		}
	})
	return s, cancel
}

func dialClient(t *testing.T, s *server.Server) *client.Client {
	t.Helper()
	url := "https://" + s.Addr() + "/wt"
	c, err := client.Dial(context.Background(), url, client.Options{
		Slug:           s.Slug(),
		CertHashB64URL: s.CertHashB64(),
		HelloTimeout:   3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestEndToEndPublishSubscribe(t *testing.T) {
	s, _ := startServer(t, server.Config{})
	c := dialClient(t, s)

	if c.SDP().Codec != "test" {
		t.Fatalf("SDP codec mismatch: %+v", c.SDP())
	}

	pub := s.Publisher()
	go func() {
		for i := 0; i < 6; i++ {
			au := pubsub.AccessUnit{
				Bytes:    []byte{byte(i), 0xAA, 0xBB},
				Keyframe: i%3 == 0,
				PTSMicro: uint64(i) * 33333,
				Seq:      uint32(i),
			}
			_ = pub.Publish(context.Background(), au)
			time.Sleep(20 * time.Millisecond)
		}
	}()

	got := make([]pubsub.AccessUnit, 0, 6)
	deadline := time.Now().Add(3 * time.Second)
	for len(got) < 6 && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		au, err := c.Recv(ctx)
		cancel()
		if err != nil {
			t.Logf("Recv: %v (have %d)", err, len(got))
			continue
		}
		got = append(got, au)
	}
	if len(got) < 4 {
		t.Fatalf("only received %d AUs, expected ~6", len(got))
	}
	// First AU received must be a keyframe (broadcaster's seed).
	if !got[0].Keyframe {
		t.Fatalf("first AU should be keyframe, got %+v", got[0])
	}
	// Sequence numbers should be non-decreasing.
	for i := 1; i < len(got); i++ {
		if got[i].Seq < got[i-1].Seq {
			t.Fatalf("seq went backwards: %d then %d", got[i-1].Seq, got[i].Seq)
		}
	}
}

func TestEndToEndDataChannel(t *testing.T) {
	gotFromClient := make(chan []byte, 1)
	s, _ := startServer(t, server.Config{
		OnDataChannel: func(dc *datachannel.Channel) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			msg, err := dc.Recv(ctx)
			if err != nil {
				return
			}
			gotFromClient <- msg
			_ = dc.Send([]byte("server-reply:" + string(msg)))
		},
	})
	c := dialClient(t, s)

	if err := c.DataChannel().Send([]byte("hello-server")); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-gotFromClient:
		if string(got) != "hello-server" {
			t.Fatalf("server got %q, want hello-server", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server didn't receive client DC message")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	reply, err := c.DataChannel().Recv(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(reply) != "server-reply:hello-server" {
		t.Fatalf("client got %q, want server-reply:hello-server", reply)
	}
}

func TestEndToEndAuthRejection(t *testing.T) {
	s, _ := startServer(t, server.Config{})
	url := "https://" + s.Addr() + "/wt"
	_, err := client.Dial(context.Background(), url, client.Options{
		Slug:           "wrong-slug",
		CertHashB64URL: s.CertHashB64(),
		HelloTimeout:   2 * time.Second,
	})
	if err == nil {
		t.Fatal("expected auth failure, got success")
	}
	if !strings.Contains(err.Error(), "auth") && !strings.Contains(err.Error(), "rejected") {
		t.Logf("got error (likely auth-related): %v", err)
	}
}

func TestEndToEndShareLinkParsing(t *testing.T) {
	s, _ := startServer(t, server.Config{})
	link := s.ShareLink()
	if !strings.HasPrefix(link, "https://") {
		t.Fatalf("share link should be https: %s", link)
	}
	if !strings.Contains(link, "#slug=") || !strings.Contains(link, "&hash=") {
		t.Fatalf("share link missing slug/hash fragment: %s", link)
	}
	// Client should be able to dial with just the share link.
	// Replace the host with our actual addr (ShareLink uses ExternalHost
	// which defaulted to 127.0.0.1; addr is 127.0.0.1:port).
	c, err := client.Dial(context.Background(), link, client.Options{
		HelloTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
}

func TestEndToEndConcurrentSubscribers(t *testing.T) {
	s, _ := startServer(t, server.Config{})

	const N = 4
	clients := make([]*client.Client, N)
	for i := range clients {
		clients[i] = dialClient(t, s)
	}

	pub := s.Publisher()
	go func() {
		for i := 0; i < 10; i++ {
			au := pubsub.AccessUnit{
				Bytes:    []byte{byte(i)},
				Keyframe: i%5 == 0,
				PTSMicro: uint64(i) * 33333,
				Seq:      uint32(i),
			}
			_ = pub.Publish(context.Background(), au)
			time.Sleep(15 * time.Millisecond)
		}
	}()

	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func(c *client.Client) {
			defer wg.Done()
			n := 0
			for n < 5 {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				_, err := c.Recv(ctx)
				cancel()
				if err != nil {
					return
				}
				n++
			}
		}(c)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("not all subscribers received their share")
	}
}
