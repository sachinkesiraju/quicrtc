// Phase 1 e2e: drive the unified PeerConnection API end-to-end over
// real QUIC + WebTransport on loopback. Catches wire/handshake
// regressions that would runtime-fail despite compile-passing.
package integration_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/cert"
	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/datachannel"
	"github.com/sachinkesiraju/quicrtc/peerconnection"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/transport"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// startPCPublisher spins up a publisher-side PeerConnection on
// loopback and returns the PC plus the resolved share-link URL/slug/
// hash (so a subscriber-side PC can dial into it).
func startPCPublisher(t *testing.T) (pubPC *peerconnection.PeerConnection, shareURL, slug, hashB64 string) {
	t.Helper()

	bundle, err := cert.Generate(cert.Options{IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	if err != nil {
		t.Fatal(err)
	}

	pc, err := peerconnection.New(peerconnection.Config{
		Transport: peerconnection.TransportNativeOnly,
		Native: &peerconnection.NativeConfig{
			Role: transport.RolePublisher,
			PublisherConfig: server.Config{
				Addr:       "127.0.0.1:0",
				CertBundle: bundle,
				SDP:        wire.SDP{Codec: "test", Width: 64, Height: 64, FPS: 30},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := pc.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })

	// Get the share-link components via the underlying server.
	tx := pc.Transport()
	srvAdapter, ok := tx.(interface{ Server() *server.Server })
	if !ok {
		t.Fatal("publisher Transport does not expose underlying *server.Server")
	}
	s := srvAdapter.Server()

	addr := s.Addr()
	if strings.HasSuffix(addr, ":0") {
		t.Fatalf("server didn't bind: addr=%q", addr)
	}
	shareURL = "https://" + addr + "/wt"
	slug = s.Slug()
	hashB64 = s.CertHashB64()
	return pc, shareURL, slug, hashB64
}

func dialPCSubscriber(t *testing.T, shareURL, slug, hashB64 string) *peerconnection.PeerConnection {
	t.Helper()

	pc, err := peerconnection.New(peerconnection.Config{
		Transport: peerconnection.TransportNativeOnly,
		Native: &peerconnection.NativeConfig{
			Role:          transport.RoleSubscriber,
			SubscriberURL: shareURL,
			SubscriberOpts: client.Options{
				Slug:           slug,
				CertHashB64URL: hashB64,
				HelloTimeout:   3 * time.Second,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pc.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	return pc
}

func TestPCPublishSubscribe(t *testing.T) {
	pubPC, shareURL, slug, hashB64 := startPCPublisher(t)
	subPC := dialPCSubscriber(t, shareURL, slug, hashB64)

	// Subscriber should have RemoteTrack populated post-Connect.
	rt := subPC.RemoteTrack()
	if rt == nil {
		t.Fatal("subscriber RemoteTrack is nil after Connect")
	}
	if rt.Kind != track.KindVideo || rt.Codec != "test" {
		t.Fatalf("RemoteTrack mismatch: %+v", rt)
	}

	// Publisher AddTrack returns a Sender.
	sender, err := pubPC.AddTrack(context.Background(), track.Video("primary", "test"))
	if err != nil {
		t.Fatal(err)
	}

	// Push 6 AUs (mix of keyframes + P-frames).
	go func() {
		for i := 0; i < 6; i++ {
			au := pubsub.AccessUnit{
				Bytes:    []byte{byte(i), 0xAA, 0xBB},
				Keyframe: i%3 == 0,
				PTSMicro: uint64(i) * 33333,
				Seq:      uint32(i),
			}
			_ = sender.Send(context.Background(), au)
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// Subscriber Recv should yield AUs in order, starting with a keyframe.
	got := make([]pubsub.AccessUnit, 0, 6)
	deadline := time.Now().Add(3 * time.Second)
	for len(got) < 6 && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		au, err := subPC.Recv(ctx)
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
	if !got[0].Keyframe {
		t.Fatalf("first AU should be keyframe, got %+v", got[0])
	}
	for i := 1; i < len(got); i++ {
		if got[i].Seq < got[i-1].Seq {
			t.Fatalf("seq went backwards: %d then %d", got[i-1].Seq, got[i].Seq)
		}
	}
}

func TestPCDataChannel(t *testing.T) {
	gotFromClient := make(chan []byte, 1)
	bundle, err := cert.Generate(cert.Options{IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	if err != nil {
		t.Fatal(err)
	}

	pubPC, err := peerconnection.New(peerconnection.Config{
		Transport: peerconnection.TransportNativeOnly,
		Native: &peerconnection.NativeConfig{
			Role: transport.RolePublisher,
			PublisherConfig: server.Config{
				Addr:       "127.0.0.1:0",
				CertBundle: bundle,
				SDP:        wire.SDP{Codec: "test", Width: 64, Height: 64, FPS: 30},
				OnDataChannel: func(dc *datachannel.Channel) {
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()
					msg, e := dc.Recv(ctx)
					if e != nil {
						return
					}
					gotFromClient <- msg
					_ = dc.Send([]byte("server-reply:" + string(msg)))
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pubPC.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pubPC.Close() })

	tx := pubPC.Transport()
	srvAdapter := tx.(interface{ Server() *server.Server })
	s := srvAdapter.Server()
	shareURL := "https://" + s.Addr() + "/wt"

	subPC := dialPCSubscriber(t, shareURL, s.Slug(), s.CertHashB64())

	dc, err := subPC.DataChannel()
	if err != nil {
		t.Fatal(err)
	}
	if err := dc.Send([]byte("hello-server")); err != nil {
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

	rctx, rcancel := context.WithTimeout(context.Background(), 3*time.Second)
	reply, err := dc.Recv(rctx)
	rcancel()
	if err != nil {
		t.Fatal(err)
	}
	if string(reply) != "server-reply:hello-server" {
		t.Fatalf("client got %q, want server-reply:hello-server", reply)
	}
}

// TestAuthValidatorRejects: a custom AuthValidator that always
// rejects results in HELLO failure even when the slug matches.
// Confirms the pluggable auth path takes precedence.
func TestAuthValidatorRejects(t *testing.T) {
	bundle, err := cert.Generate(cert.Options{IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	if err != nil {
		t.Fatal(err)
	}
	pubPC, err := peerconnection.New(peerconnection.Config{
		Transport: peerconnection.TransportNativeOnly,
		Native: &peerconnection.NativeConfig{
			Role: transport.RolePublisher,
			PublisherConfig: server.Config{
				Addr:       "127.0.0.1:0",
				CertBundle: bundle,
				SDP:        wire.SDP{Codec: "test", Width: 64, Height: 64, FPS: 30},
				AuthValidator: func(cred string) (string, error) {
					return "", fmt.Errorf("rejected: %q not in allowlist", cred)
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pubPC.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pubPC.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	srv := pubPC.Transport().(interface{ Server() *server.Server }).Server()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && strings.HasSuffix(srv.Addr(), ":0") {
		time.Sleep(time.Millisecond)
	}

	// Try to dial; should fail at handshake.
	subPC, err := peerconnection.New(peerconnection.Config{
		Transport: peerconnection.TransportNativeOnly,
		Native: &peerconnection.NativeConfig{
			Role:          transport.RoleSubscriber,
			SubscriberURL: "https://" + srv.Addr() + "/wt",
			SubscriberOpts: client.Options{
				Slug:           srv.Slug(),
				CertHashB64URL: srv.CertHashB64(),
				HelloTimeout:   2 * time.Second,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subPC.Close()
	if err := subPC.Connect(ctx); err == nil {
		t.Fatal("expected Connect to fail when AuthValidator rejects, got nil error")
	}
}

// TestAuthValidatorAccepts: a custom AuthValidator that ignores the
// slug entirely (e.g., a bearer-token validator) accepts a connect
// regardless of slug.
func TestAuthValidatorAccepts(t *testing.T) {
	bundle, err := cert.Generate(cert.Options{IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	if err != nil {
		t.Fatal(err)
	}
	pubPC, err := peerconnection.New(peerconnection.Config{
		Transport: peerconnection.TransportNativeOnly,
		Native: &peerconnection.NativeConfig{
			Role: transport.RolePublisher,
			PublisherConfig: server.Config{
				Addr:       "127.0.0.1:0",
				CertBundle: bundle,
				SDP:        wire.SDP{Codec: "test", Width: 64, Height: 64, FPS: 30},
				AuthValidator: func(cred string) (string, error) {
					if cred == "valid-bearer-token" {
						return "tenant-a", nil
					}
					return "", fmt.Errorf("invalid token")
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pubPC.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pubPC.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	srv := pubPC.Transport().(interface{ Server() *server.Server }).Server()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && strings.HasSuffix(srv.Addr(), ":0") {
		time.Sleep(time.Millisecond)
	}

	subPC, err := peerconnection.New(peerconnection.Config{
		Transport: peerconnection.TransportNativeOnly,
		Native: &peerconnection.NativeConfig{
			Role:          transport.RoleSubscriber,
			SubscriberURL: "https://" + srv.Addr() + "/wt",
			SubscriberOpts: client.Options{
				Slug:           "valid-bearer-token", // smuggled as 'slug'
				CertHashB64URL: srv.CertHashB64(),
				HelloTimeout:   2 * time.Second,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subPC.Close()
	if err := subPC.Connect(ctx); err != nil {
		t.Fatalf("expected Connect to succeed with valid bearer token, got: %v", err)
	}
}

// TestCapabilityNegotiation verifies the HELLO/SDP feature
// intersection. Client declares {resume, future-x}; server supports
// {resume, backpressure}; the negotiated set on the client should
// be just {resume}.
func TestCapabilityNegotiation(t *testing.T) {
	bundle, err := cert.Generate(cert.Options{IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	if err != nil {
		t.Fatal(err)
	}
	pubPC, err := peerconnection.New(peerconnection.Config{
		Transport: peerconnection.TransportNativeOnly,
		Native: &peerconnection.NativeConfig{
			Role: transport.RolePublisher,
			PublisherConfig: server.Config{
				Addr:              "127.0.0.1:0",
				CertBundle:        bundle,
				SDP:               wire.SDP{Codec: "test", Width: 64, Height: 64, FPS: 30},
				SupportedFeatures: []string{"resume", "backpressure"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pubPC.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pubPC.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	srv := pubPC.Transport().(interface{ Server() *server.Server }).Server()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && strings.HasSuffix(srv.Addr(), ":0") {
		time.Sleep(time.Millisecond)
	}

	subPC, err := peerconnection.New(peerconnection.Config{
		Transport: peerconnection.TransportNativeOnly,
		Native: &peerconnection.NativeConfig{
			Role:          transport.RoleSubscriber,
			SubscriberURL: "https://" + srv.Addr() + "/wt",
			SubscriberOpts: client.Options{
				Slug:           srv.Slug(),
				CertHashB64URL: srv.CertHashB64(),
				HelloTimeout:   2 * time.Second,
				Features:       []string{"resume", "future-x"}, // future-x not on server
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subPC.Close()
	if err := subPC.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	c := subPC.Transport().(interface{ Client() *client.Client }).Client()
	negotiated := c.NegotiatedFeatures()
	if len(negotiated) != 1 || negotiated[0] != "resume" {
		t.Fatalf("expected negotiated = [resume], got %v", negotiated)
	}
}

// TestBackpressureSignaling verifies the subscriber → publisher
// backpressure path: client.SendBackpressure emits a TypeBackpressure
// control frame, the server decodes it, and Server.Config.OnBackpressure
// fires with the right track/level.
func TestBackpressureSignaling(t *testing.T) {
	type bpEvent struct {
		track string
		level uint8
	}
	bpCh := make(chan bpEvent, 4)

	bundle, err := cert.Generate(cert.Options{IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	if err != nil {
		t.Fatal(err)
	}
	pubPC, err := peerconnection.New(peerconnection.Config{
		Transport: peerconnection.TransportNativeOnly,
		Native: &peerconnection.NativeConfig{
			Role: transport.RolePublisher,
			PublisherConfig: server.Config{
				Addr:       "127.0.0.1:0",
				CertBundle: bundle,
				SDP:        wire.SDP{Codec: "test", Width: 64, Height: 64, FPS: 30},
				OnBackpressure: func(_, trackName string, level uint8) {
					bpCh <- bpEvent{track: trackName, level: level}
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pubPC.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pubPC.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	srv := pubPC.Transport().(interface{ Server() *server.Server }).Server()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && strings.HasSuffix(srv.Addr(), ":0") {
		time.Sleep(time.Millisecond)
	}
	subPC := dialPCSubscriber(t, "https://"+srv.Addr()+"/wt", srv.Slug(), srv.CertHashB64())
	defer subPC.Close()

	// Reach into the underlying client.
	type clientHaver interface{ Client() *client.Client }
	c := subPC.Transport().(clientHaver).Client()
	if c == nil {
		t.Fatal("no underlying client")
	}

	// Send a backpressure event.
	if err := c.SendBackpressure("video", 80); err != nil {
		t.Fatalf("SendBackpressure: %v", err)
	}

	select {
	case ev := <-bpCh:
		if ev.track != "video" || ev.level != 80 {
			t.Fatalf("got bp event %+v, want {video 80}", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnBackpressure callback never fired")
	}
}

// TestServerGracefulShutdown verifies Server.Shutdown drains live
// sessions cleanly: TypeClose is delivered to the subscriber, the
// drain returns nil within budget, and the server's session count
// reaches zero before the listener closes.
func TestServerGracefulShutdown(t *testing.T) {
	pubPC, shareURL, slug, hashB64 := startPCPublisher(t)
	subPC := dialPCSubscriber(t, shareURL, slug, hashB64)

	// Get the *server.Server out of the publisher PC's transport.
	type serverHaver interface {
		Server() *server.Server
	}
	srvHaver, ok := pubPC.Transport().(serverHaver)
	if !ok {
		t.Fatal("publisher Transport doesn't expose Server()")
	}
	srv := srvHaver.Server()

	// Confirm one session is live.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && srv.SubscriberCount() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if srv.SubscriberCount() == 0 {
		t.Fatal("subscriber didn't connect")
	}

	// Trigger graceful shutdown with a generous drain budget.
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	if err := srv.Shutdown(drainCtx); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}

	// Verify draining flag was set + count is now zero.
	if !srv.IsDraining() {
		t.Error("expected IsDraining() == true after Shutdown")
	}
	if srv.SubscriberCount() != 0 {
		t.Errorf("expected SubscriberCount=0 after drain, got %d", srv.SubscriberCount())
	}

	// New subscriber dial should fail (or get HTTP 503), since the
	// server is now draining and the listener is being torn down.
	_ = subPC.Close()
}

// TestPCDynamicTrackAddRemove exercises adding and removing tracks
// mid-session. After RemoveTrack, the subscriber's recv channel for
// that track should close cleanly (RecvOn returns an error). A new
// AddTrack with the same name should work and deliver fresh AUs.
func TestPCDynamicTrackAddRemove(t *testing.T) {
	pubPC, shareURL, slug, hashB64 := startPCPublisher(t)
	subPC := dialPCSubscriber(t, shareURL, slug, hashB64)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sender, err := pubPC.AddTrack(ctx, track.Video("alpha", "test"))
	if err != nil {
		t.Fatalf("AddTrack alpha: %v", err)
	}
	// Send a few AUs. First is keyframe (opens stream), rest are
	// P-frames so they ride the same stream rather than each one
	// causing a stream-reset of the prior frame.
	for i := 0; i < 5; i++ {
		_ = sender.Send(ctx, pubsub.AccessUnit{
			Bytes: []byte{byte(i)}, Keyframe: i == 0, Seq: uint32(i),
		})
	}
	// Subscriber receives.
	for i := 0; i < 5; i++ {
		rctx, rcancel := context.WithTimeout(ctx, 2*time.Second)
		au, err := subPC.RecvOn(rctx, "alpha")
		rcancel()
		if err != nil {
			t.Fatalf("RecvOn alpha[%d]: %v", i, err)
		}
		if len(au.Bytes) != 1 || au.Bytes[0] != byte(i) {
			t.Fatalf("got %v, want [%d]", au.Bytes, i)
		}
	}

	// Remove the track. Subscriber's recv channel should close.
	if err := pubPC.RemoveTrack("alpha"); err != nil {
		t.Fatalf("RemoveTrack: %v", err)
	}
	// Allow Unannounce to propagate.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rctx, rcancel := context.WithTimeout(ctx, 200*time.Millisecond)
		_, err := subPC.RecvOn(rctx, "alpha")
		rcancel()
		if err != nil {
			break // got expected error (channel closed or timeout)
		}
	}

	// Re-add a track with the same name; should work and deliver fresh AUs.
	sender2, err := pubPC.AddTrack(ctx, track.Video("alpha", "test"))
	if err != nil {
		t.Fatalf("re-AddTrack alpha: %v", err)
	}
	_ = sender2.Send(ctx, pubsub.AccessUnit{
		Bytes: []byte{99}, Keyframe: true, Seq: 0,
	})
	rctx, rcancel := context.WithTimeout(ctx, 2*time.Second)
	au, err := subPC.RecvOn(rctx, "alpha")
	rcancel()
	if err != nil {
		t.Fatalf("RecvOn after re-add: %v", err)
	}
	if len(au.Bytes) != 1 || au.Bytes[0] != 99 {
		t.Fatalf("got %v, want [99]", au.Bytes)
	}
}

// TestPCPublishBack exercises the subscriber→publisher track flow:
// the subscriber declares an "actions" track via Announce, opens uni
// streams toward the server, and the publisher reads the AUs back
// via RecvOn on its server-side PeerConnection.
func TestPCPublishBack(t *testing.T) {
	pubPC, shareURL, slug, hashB64 := startPCPublisher(t)
	subPC := dialPCSubscriber(t, shareURL, slug, hashB64)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sender, err := subPC.AddTrack(ctx, track.LocalTrack{
		Name: "actions", Kind: track.KindToolCalls,
	})
	if err != nil {
		t.Fatalf("subscriber AddTrack: %v", err)
	}

	const N = 20
	go func() {
		for i := 0; i < N; i++ {
			_ = sender.Send(ctx, pubsub.AccessUnit{
				Bytes:    []byte{byte(i)},
				Keyframe: i == 0,
				Seq:      uint32(i),
			})
			time.Sleep(2 * time.Millisecond)
		}
	}()

	for i := 0; i < N; i++ {
		recvCtx, rcancel := context.WithTimeout(ctx, 2*time.Second)
		au, err := pubPC.RecvOn(recvCtx, "actions")
		rcancel()
		if err != nil {
			t.Fatalf("publisher RecvOn[%d]: %v", i, err)
		}
		if len(au.Bytes) != 1 || au.Bytes[0] != byte(i) {
			t.Fatalf("publisher got AU %d = %v, want byte=%d", i, au.Bytes, i)
		}
	}
}

func TestPCConcurrentSubscribers(t *testing.T) {
	pubPC, shareURL, slug, hashB64 := startPCPublisher(t)
	sender, err := pubPC.AddTrack(context.Background(), track.Video("primary", "test"))
	if err != nil {
		t.Fatal(err)
	}

	const N = 4
	subs := make([]*peerconnection.PeerConnection, N)
	for i := range subs {
		subs[i] = dialPCSubscriber(t, shareURL, slug, hashB64)
	}

	go func() {
		for i := 0; i < 10; i++ {
			au := pubsub.AccessUnit{
				Bytes:    []byte{byte(i)},
				Keyframe: i%5 == 0,
				PTSMicro: uint64(i) * 33333,
				Seq:      uint32(i),
			}
			_ = sender.Send(context.Background(), au)
			time.Sleep(15 * time.Millisecond)
		}
	}()

	var wg sync.WaitGroup
	for _, s := range subs {
		wg.Add(1)
		go func(s *peerconnection.PeerConnection) {
			defer wg.Done()
			n := 0
			for n < 5 {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				_, err := s.Recv(ctx)
				cancel()
				if err != nil {
					return
				}
				n++
			}
		}(s)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("not all subscribers received their share")
	}
}
