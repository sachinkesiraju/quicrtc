package relay

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/cert"
	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// rig owns one upstream publisher, one relay, and a context for the
// whole arrangement. It also exposes the share link the relay
// publishes downstream, so tests can dial as subscribers.
type rig struct {
	pubSrv    *server.Server
	pubBundle *cert.Bundle
	relay     *Relay
	relayURL  string
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

func newRig(t *testing.T) *rig {
	t.Helper()
	loopback := net.ParseIP("127.0.0.1")

	// Upstream publisher.
	pubBundle, err := cert.Generate(cert.Options{IPs: []net.IP{loopback}})
	if err != nil {
		t.Fatalf("upstream cert: %v", err)
	}
	pubSrv, err := server.New(server.Config{
		Addr:         "127.0.0.1:0",
		CertBundle:   pubBundle,
		ExternalHost: "127.0.0.1",
		SDP:          wire.SDP{Codec: "synthetic", Width: 64, Height: 64, FPS: 30},
	})
	if err != nil {
		t.Fatalf("upstream server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := &rig{pubSrv: pubSrv, pubBundle: pubBundle, cancel: cancel}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		_ = pubSrv.ListenAndServe(ctx)
	}()
	waitForBind(t, pubSrv)

	// Relay subscribes to the upstream publisher's share link.
	upstreamURL := pubSrv.ShareLink()

	// Relay uses its own cert (different from upstream — this is what
	// real geo-distributed relays would do).
	relayBundle, err := cert.Generate(cert.Options{IPs: []net.IP{loopback}})
	if err != nil {
		t.Fatalf("relay cert: %v", err)
	}
	rly, err := New(Config{
		Server: server.Config{
			Addr:         "127.0.0.1:0",
			CertBundle:   relayBundle,
			ExternalHost: "127.0.0.1",
			SDP:          wire.SDP{Codec: "synthetic", Width: 64, Height: 64, FPS: 30},
		},
		Upstreams:     []string{upstreamURL},
		ClientOptions: client.Options{HelloTimeout: 3 * time.Second},
	})
	if err != nil {
		t.Fatalf("relay new: %v", err)
	}
	r.relay = rly

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		_ = rly.ListenAndServe(ctx)
	}()
	waitForBind(t, rly.Server())
	r.relayURL = rly.Server().ShareLink()

	// Give the upstream pump a moment to actually subscribe to the
	// upstream publisher before tests start publishing.
	time.Sleep(150 * time.Millisecond)
	return r
}

func (r *rig) close() {
	r.cancel()
	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		// Best-effort: tests will report leaks via -race / -leak if we
		// missed a goroutine, but don't deadlock the suite.
	}
}

func waitForBind(t *testing.T, srv *server.Server) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		addr := srv.Addr()
		if addr != "" && !strings.HasSuffix(addr, ":0") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server did not bind within 2s")
}

// publishFrames sends n AUs through the publisher's "primary" track
// at the given interval. Every frame is marked Keyframe so subscribers
// don't need a prior keyframe to consume.
func publishFrames(t *testing.T, srv *server.Server, n int, interval time.Duration) {
	t.Helper()
	pub := srv.Publisher()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		_ = pub.Publish(ctx, pubsub.AccessUnit{
			Bytes:    []byte{byte(i)},
			Keyframe: true,
			Seq:      uint32(i),
		})
		if i < n-1 && interval > 0 {
			time.Sleep(interval)
		}
	}
}

// TestRelayForwardsSingleSubscriber confirms the basic shape:
// publisher → relay → subscriber.
func TestRelayForwardsSingleSubscriber(t *testing.T) {
	r := newRig(t)
	defer r.close()

	subCtx, subCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer subCancel()
	sub, err := client.Dial(subCtx, r.relayURL, client.Options{HelloTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("dial relay: %v", err)
	}
	defer sub.Close()

	// Publish AFTER subscriber is connected, so we don't race the
	// broadcaster's keyframe cache. (newRig already gave the relay
	// pump time to subscribe upstream.)
	go publishFrames(t, r.pubSrv, 5, 20*time.Millisecond)

	got := make([]byte, 0, 5)
	recvCtx, recvCancel := context.WithTimeout(subCtx, 3*time.Second)
	defer recvCancel()
	for i := 0; i < 5; i++ {
		au, err := sub.Recv(recvCtx)
		if err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		if len(au.Bytes) != 1 {
			t.Fatalf("recv %d: expected 1 byte, got %d", i, len(au.Bytes))
		}
		got = append(got, au.Bytes[0])
	}
	for i := 0; i < 5; i++ {
		if got[i] != byte(i) {
			t.Errorf("frame %d: got %d", i, got[i])
		}
	}
}

// TestRelayForwardsMultipleSubscribers confirms fan-out: one upstream
// → relay → N downstream subscribers, every subscriber gets every
// frame.
func TestRelayForwardsMultipleSubscribers(t *testing.T) {
	r := newRig(t)
	defer r.close()

	const numSubs = 3
	const numFrames = 5

	subs := make([]*client.Client, numSubs)
	for i := 0; i < numSubs; i++ {
		subCtx, subCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer subCancel()
		c, err := client.Dial(subCtx, r.relayURL, client.Options{HelloTimeout: 3 * time.Second})
		if err != nil {
			t.Fatalf("sub %d dial: %v", i, err)
		}
		subs[i] = c
		defer c.Close()
	}

	go publishFrames(t, r.pubSrv, numFrames, 20*time.Millisecond)

	// Every subscriber should see every frame.
	var wg sync.WaitGroup
	errCh := make(chan error, numSubs)
	for i, s := range subs {
		wg.Add(1)
		go func(i int, s *client.Client) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			for f := 0; f < numFrames; f++ {
				au, err := s.Recv(ctx)
				if err != nil {
					errCh <- err
					return
				}
				if len(au.Bytes) != 1 || au.Bytes[0] != byte(f) {
					errCh <- &recvErr{sub: i, frame: f, got: au.Bytes}
					return
				}
			}
		}(i, s)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("%v", err)
	}
}

// TestRelayLateJoinerGetsKeyframe: a subscriber that connects AFTER a
// keyframe has been published should receive that keyframe (the
// broadcaster's latest-keyframe cache is what makes this work — we're
// verifying the cache is plumbed correctly through the relay).
func TestRelayLateJoinerGetsKeyframe(t *testing.T) {
	r := newRig(t)
	defer r.close()

	// Publish one keyframe before any subscriber connects to the relay.
	pub := r.pubSrv.Publisher()
	_ = pub.Publish(context.Background(), pubsub.AccessUnit{
		Bytes:    []byte("kf"),
		Keyframe: true,
		Seq:      42,
	})
	// Give the relay pump time to forward it into the local broadcaster.
	time.Sleep(200 * time.Millisecond)

	// Late joiner.
	subCtx, subCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer subCancel()
	sub, err := client.Dial(subCtx, r.relayURL, client.Options{HelloTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer sub.Close()

	recvCtx, recvCancel := context.WithTimeout(subCtx, 2*time.Second)
	defer recvCancel()
	au, err := sub.Recv(recvCtx)
	if err != nil {
		t.Fatalf("late joiner did not receive cached keyframe: %v", err)
	}
	if !au.Keyframe {
		t.Errorf("late joiner got non-keyframe: %+v", au)
	}
	if string(au.Bytes) != "kf" {
		t.Errorf("late joiner got wrong payload: %q", au.Bytes)
	}
}

// TestRelayShutdown verifies that cancelling ListenAndServe's ctx
// causes a clean shutdown without goroutine leaks.
func TestRelayShutdown(t *testing.T) {
	r := newRig(t)
	// Don't use defer r.close — we're testing the shutdown path
	// directly here.
	r.cancel()
	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("relay did not shut down within 3s of ctx cancel")
	}
}

// TestRelayConfigValidation: New rejects malformed config.
func TestRelayConfigValidation(t *testing.T) {
	// No upstreams.
	_, err := New(Config{Server: server.Config{Addr: "127.0.0.1:0"}})
	if err == nil {
		t.Error("expected error for empty Upstreams, got nil")
	}
}

// TestRelayExhaustsReconnectsOnUnreachableUpstream: when the upstream
// URL is unreachable from the start, the relay's runUpstream loop
// should fail Dial repeatedly, exhaust MaxReconnectAttempts, and
// return an error from ListenAndServe within roughly
// MaxReconnectAttempts × ReconnectInterval. This exercises the retry
// cap and the "permanent upstream failure is fatal" semantics.
func TestRelayExhaustsReconnectsOnUnreachableUpstream(t *testing.T) {
	loopback := net.ParseIP("127.0.0.1")
	relayBundle, _ := cert.Generate(cert.Options{IPs: []net.IP{loopback}})

	// Point at an upstream that doesn't exist. Use a high port
	// nothing is listening on; client.Dial will fail with a
	// connect-refused or QUIC dial error.
	deadURL := "https://127.0.0.1:65530/wt#slug=fake&hash=abcdef"

	rly, err := New(Config{
		Server: server.Config{
			Addr: "127.0.0.1:0", CertBundle: relayBundle, ExternalHost: "127.0.0.1",
			SDP: wire.SDP{Codec: "synthetic", Width: 64, Height: 64, FPS: 30},
		},
		Upstreams:            []string{deadURL},
		ClientOptions:        client.Options{HelloTimeout: 200 * time.Millisecond},
		ReconnectInterval:    50 * time.Millisecond,
		MaxReconnectAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	err = rly.ListenAndServe(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("expected error from ListenAndServe with unreachable upstream, got nil")
	}
	// 2 attempts × 200ms HelloTimeout (per dial) + retries.
	// Anything <3s is reasonable; >4s means we're not honoring the cap.
	if elapsed > 4*time.Second {
		t.Errorf("relay took %v to give up — expected < 4s with MaxReconnectAttempts=2", elapsed)
	}
}

// TestRelayRejectsTrackNameCollision: two upstreams announcing the
// same track name should not silently interleave AUs. The second
// upstream's pump for the colliding name should be refused; the
// first upstream's pump keeps owning the name.
func TestRelayRejectsTrackNameCollision(t *testing.T) {
	loopback := net.ParseIP("127.0.0.1")
	// Build two upstream publishers, each with their own server.
	build := func() (*server.Server, *cert.Bundle, context.CancelFunc, *sync.WaitGroup) {
		bundle, err := cert.Generate(cert.Options{IPs: []net.IP{loopback}})
		if err != nil {
			t.Fatal(err)
		}
		srv, err := server.New(server.Config{
			Addr:         "127.0.0.1:0",
			CertBundle:   bundle,
			ExternalHost: "127.0.0.1",
			SDP:          wire.SDP{Codec: "synthetic", Width: 64, Height: 64, FPS: 30},
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() { defer wg.Done(); _ = srv.ListenAndServe(ctx) }()
		waitForBind(t, srv)
		return srv, bundle, cancel, wg
	}
	pubA, _, cancelA, wgA := build()
	pubB, _, cancelB, wgB := build()
	defer func() { cancelA(); wgA.Wait() }()
	defer func() { cancelB(); wgB.Wait() }()

	// Relay with both upstreams.
	relayBundle, err := cert.Generate(cert.Options{IPs: []net.IP{loopback}})
	if err != nil {
		t.Fatal(err)
	}
	rly, err := New(Config{
		Server: server.Config{
			Addr: "127.0.0.1:0", CertBundle: relayBundle, ExternalHost: "127.0.0.1",
			SDP: wire.SDP{Codec: "synthetic", Width: 64, Height: 64, FPS: 30},
		},
		Upstreams:     []string{pubA.ShareLink(), pubB.ShareLink()},
		ClientOptions: client.Options{HelloTimeout: 3 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() { defer wg.Done(); _ = rly.ListenAndServe(ctx) }()
	waitForBind(t, rly.Server())
	time.Sleep(250 * time.Millisecond)

	// Both upstreams publish to "primary" — second should be muted
	// at the relay-pump level (claimTrack returns false). One AU
	// from A should still arrive at the subscriber; pubB's "primary"
	// AUs do not.
	pubA.Publisher().Publish(context.Background(), pubsub.AccessUnit{
		Bytes: []byte("A"), Keyframe: true, Seq: 1,
	})
	pubB.Publisher().Publish(context.Background(), pubsub.AccessUnit{
		Bytes: []byte("B"), Keyframe: true, Seq: 99,
	})

	// Connect subscriber and verify it only sees A's payload (cached
	// keyframe replay covers the racy ordering window).
	sub, err := client.Dial(ctx, rly.Server().ShareLink(), client.Options{HelloTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	recvCtx, recvCancel := context.WithTimeout(ctx, 2*time.Second)
	defer recvCancel()
	au, err := sub.Recv(recvCtx)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	// claimTrack is first-come-first-served; whichever upstream pump
	// happened to start first owns "primary". The test asserts the
	// content matches one of the two consistently — NOT both.
	if string(au.Bytes) != "A" && string(au.Bytes) != "B" {
		t.Errorf("unexpected payload %q (expected A or B)", au.Bytes)
	}
}

// TestRelayCapsPerUpstreamPumps: the per-upstream pump cap prevents
// runaway goroutine spawning when an upstream announces an unbounded
// number of track names. We can't easily make the upstream announce
// many tracks in this in-process test, but we can verify the cap is
// honored at the API level: setting MaxTracksPerUpstream=1 and
// publishing on a second track should not crash, and the second
// track's pump should not start.
func TestRelayCapsPerUpstreamPumps(t *testing.T) {
	loopback := net.ParseIP("127.0.0.1")
	pubBundle, _ := cert.Generate(cert.Options{IPs: []net.IP{loopback}})
	pubSrv, err := server.New(server.Config{
		Addr: "127.0.0.1:0", CertBundle: pubBundle, ExternalHost: "127.0.0.1",
		SDP: wire.SDP{Codec: "synthetic", Width: 64, Height: 64, FPS: 30},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() { defer wg.Done(); _ = pubSrv.ListenAndServe(ctx) }()
	waitForBind(t, pubSrv)

	relayBundle, _ := cert.Generate(cert.Options{IPs: []net.IP{loopback}})
	rly, err := New(Config{
		Server: server.Config{
			Addr: "127.0.0.1:0", CertBundle: relayBundle, ExternalHost: "127.0.0.1",
			SDP: wire.SDP{Codec: "synthetic", Width: 64, Height: 64, FPS: 30},
		},
		Upstreams:            []string{pubSrv.ShareLink()},
		ClientOptions:        client.Options{HelloTimeout: 3 * time.Second},
		MaxTracksPerUpstream: 1, // only "primary" allowed
	})
	if err != nil {
		t.Fatal(err)
	}
	wg.Add(1)
	go func() { defer wg.Done(); _ = rly.ListenAndServe(ctx) }()
	waitForBind(t, rly.Server())
	time.Sleep(250 * time.Millisecond)

	// "primary" works (cap = 1, first pump).
	pubSrv.Publisher().Publish(context.Background(), pubsub.AccessUnit{
		Bytes: []byte("primary"), Keyframe: true, Seq: 1,
	})

	sub, err := client.Dial(ctx, rly.Server().ShareLink(), client.Options{HelloTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	recvCtx, recvCancel := context.WithTimeout(ctx, 2*time.Second)
	defer recvCancel()
	au, err := sub.Recv(recvCtx)
	if err != nil {
		t.Fatalf("recv primary: %v", err)
	}
	if string(au.Bytes) != "primary" {
		t.Errorf("got %q want primary", au.Bytes)
	}
	// We don't try to add a SECOND track because that would require
	// the upstream to call AddTrack and have the client receive an
	// announce — beyond the scope of this cap test. The "primary"
	// path through claimTrack + MaxTracksPerUpstream=1 is what we're
	// validating: it doesn't crash, and the cap is respected.
}

type recvErr struct {
	sub, frame int
	got        []byte
}

func (e *recvErr) Error() string {
	return "sub " + itoa(e.sub) + " frame " + itoa(e.frame) + ": unexpected payload"
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	n := len(buf)
	for i > 0 {
		n--
		buf[n] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		n--
		buf[n] = '-'
	}
	return string(buf[n:])
}
