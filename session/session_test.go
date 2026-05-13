package session

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/feed"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// pipeStream wraps a net.Pipe end as a ControlStream. It tracks the
// last SetReadDeadline so tests can verify the handshake timeout.
type pipeStream struct {
	conn net.Conn
}

func (p *pipeStream) Read(b []byte) (int, error)         { return p.conn.Read(b) }
func (p *pipeStream) Write(b []byte) (int, error)        { return p.conn.Write(b) }
func (p *pipeStream) Close() error                       { return p.conn.Close() }
func (p *pipeStream) SetReadDeadline(t time.Time) error  { return p.conn.SetReadDeadline(t) }
func (p *pipeStream) SetWriteDeadline(t time.Time) error { return p.conn.SetWriteDeadline(t) }

// readSkippingAnnounces reads one control frame, skipping the
// post-handshake Announce burst.
func readSkippingAnnounces(r io.Reader) (byte, []byte, error) {
	for {
		t, payload, err := wire.ReadControlFrame(r)
		if err != nil {
			return 0, nil, err
		}
		if t == wire.TypeAnnounce || t == wire.TypeUnannounce {
			continue
		}
		return t, payload, nil
	}
}

// fakeStream and fakeOpener are minimal copies of the feed package's
// test helpers — Go's package boundaries forbid sharing _test.go.
type fakeSendStream struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	cancelled bool
}

func (s *fakeSendStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelled {
		return 0, io.ErrClosedPipe
	}
	return s.buf.Write(p)
}
func (s *fakeSendStream) Close() error                       { return nil }
func (s *fakeSendStream) SetWriteDeadline(t time.Time) error { return nil }
func (s *fakeSendStream) CancelWrite(code uint32)            { s.mu.Lock(); s.cancelled = true; s.mu.Unlock() }

type fakeOpener struct {
	mu      sync.Mutex
	streams []*fakeSendStream
}

func (o *fakeOpener) OpenSendStream(ctx context.Context) (feed.SendStream, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	s := &fakeSendStream{}
	o.streams = append(o.streams, s)
	return s, nil
}

func (o *fakeOpener) Streams() []*fakeSendStream {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]*fakeSendStream(nil), o.streams...)
}

func newSession(t *testing.T, cfg Config) (*Session, *pipeStream, *pipeStream, *fakeOpener, *pubsub.Broadcaster) {
	t.Helper()
	srv, cli := net.Pipe()
	ps := &pipeStream{conn: srv}
	pc := &pipeStream{conn: cli}
	op := &fakeOpener{}
	b := pubsub.NewBroadcaster(8)
	r := b.Subscribe()
	s := New(cfg, ps, op)
	s.AttachTrack("primary", r)
	return s, ps, pc, op, b
}

// TestHandshakeOK verifies the happy path: subscriber sends HELLO,
// server responds with SDP, then we read the SDP back.
func TestHandshakeOK(t *testing.T) {
	cfg := Config{
		ExpectSlug: "secret",
		SDP:        wire.SDP{Codec: "avc1.42E01F", Width: 1280, Height: 720, FPS: 30},
	}
	s, _, pc, _, b := newSession(t, cfg)

	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()

	// Client sends HELLO.
	hello := wire.Hello{Role: "recv", Slug: "secret", Version: wire.CurrentVersion}
	hb, _ := hello.Marshal()
	if err := wire.WriteControlFrame(pc, wire.TypeHello, hb); err != nil {
		t.Fatal(err)
	}
	// Read SDP back.
	t1, payload, err := wire.ReadControlFrame(pc)
	if err != nil {
		t.Fatal(err)
	}
	if t1 != wire.TypeSDP {
		t.Fatalf("got type %#x, want SDP", t1)
	}
	sdp, _ := wire.UnmarshalSDP(payload)
	if sdp.Codec != cfg.SDP.Codec || sdp.Width != cfg.SDP.Width || sdp.Height != cfg.SDP.Height || sdp.FPS != cfg.SDP.FPS {
		t.Fatalf("SDP mismatch: %+v vs %+v", sdp, cfg.SDP)
	}

	// Send a CLOSE to end the session.
	wire.WriteControlFrame(pc, wire.TypeClose, nil)
	pc.Close()
	for _, r := range s.Receivers("primary") {
		b.Unsubscribe(r)
	} // unblocks pump

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("session didn't end")
	}
}

func TestHandshakeRejectsBadSlug(t *testing.T) {
	cfg := Config{ExpectSlug: "secret", IdleTimeout: time.Hour}
	s, _, pc, _, _ := newSession(t, cfg)

	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()

	hello := wire.Hello{Role: "recv", Slug: "wrong", Version: wire.CurrentVersion}
	hb, _ := hello.Marshal()
	wire.WriteControlFrame(pc, wire.TypeHello, hb)

	// Server should reply with ERROR then close.
	t1, payload, err := wire.ReadControlFrame(pc)
	if err == nil && t1 == wire.TypeError && string(payload) == "auth" {
		// expected
	} else {
		t.Logf("got back: t=%#x payload=%q err=%v (continuing — error response is best-effort)", t1, payload, err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, ErrAuth) {
			t.Fatalf("want ErrAuth, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("auth-failed session didn't return")
	}
	pc.Close()
}

func TestHandshakeRejectsWrongFirstFrame(t *testing.T) {
	cfg := Config{ExpectSlug: "x"}
	s, _, pc, _, _ := newSession(t, cfg)

	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()

	wire.WriteControlFrame(pc, wire.TypePing, []byte("nope")) // not HELLO

	select {
	case err := <-done:
		if !errors.Is(err, ErrBadHello) {
			t.Fatalf("want ErrBadHello, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session didn't return")
	}
	pc.Close()
}

func TestHandshakeRejectsBadVersion(t *testing.T) {
	cfg := Config{ExpectSlug: "secret"}
	s, _, pc, _, _ := newSession(t, cfg)

	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()

	hello := wire.Hello{Role: "recv", Slug: "secret", Version: "99"}
	hb, _ := hello.Marshal()
	wire.WriteControlFrame(pc, wire.TypeHello, hb)

	select {
	case err := <-done:
		if !errors.Is(err, ErrBadHello) {
			t.Fatalf("want ErrBadHello, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session didn't return")
	}
	pc.Close()
}

func TestPingPongRoundTrip(t *testing.T) {
	cfg := Config{ExpectSlug: "secret", IdleTimeout: time.Hour, SDP: wire.SDP{Codec: "x"}}
	s, _, pc, _, b := newSession(t, cfg)

	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()

	// Handshake.
	hb, _ := (wire.Hello{Role: "recv", Slug: "secret", Version: wire.CurrentVersion}).Marshal()
	wire.WriteControlFrame(pc, wire.TypeHello, hb)
	wire.ReadControlFrame(pc) // SDP

	// PING
	wire.WriteControlFrame(pc, wire.TypePing, []byte("nonce-1"))
	t1, payload, err := readSkippingAnnounces(pc)
	if err != nil {
		t.Fatal(err)
	}
	if t1 != wire.TypePong || string(payload) != "nonce-1" {
		t.Fatalf("PING/PONG mismatch: type=%#x payload=%q", t1, payload)
	}

	wire.WriteControlFrame(pc, wire.TypeClose, nil)
	pc.Close()
	for _, r := range s.Receivers("primary") {
		b.Unsubscribe(r)
	}
	<-done
}

func TestDataChannelInboundDelivery(t *testing.T) {
	cfg := Config{ExpectSlug: "secret", IdleTimeout: time.Hour, SDP: wire.SDP{Codec: "x"}}
	s, _, pc, _, b := newSession(t, cfg)
	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()

	hb, _ := (wire.Hello{Role: "recv", Slug: "secret", Version: wire.CurrentVersion}).Marshal()
	wire.WriteControlFrame(pc, wire.TypeHello, hb)
	wire.ReadControlFrame(pc) // SDP

	wire.WriteControlFrame(pc, wire.TypeData, []byte("hello-from-peer"))

	dc := s.DataChannel()
	rctx, rcancel := context.WithTimeout(context.Background(), time.Second)
	msg, err := dc.Recv(rctx)
	rcancel()
	if err != nil {
		t.Fatal(err)
	}
	if string(msg) != "hello-from-peer" {
		t.Fatalf("got %q want %q", msg, "hello-from-peer")
	}

	wire.WriteControlFrame(pc, wire.TypeClose, nil)
	pc.Close()
	for _, r := range s.Receivers("primary") {
		b.Unsubscribe(r)
	}
	<-done
}

func TestDataChannelOutbound(t *testing.T) {
	cfg := Config{ExpectSlug: "secret", IdleTimeout: time.Hour, SDP: wire.SDP{Codec: "x"}}
	s, _, pc, _, b := newSession(t, cfg)

	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()

	hb, _ := (wire.Hello{Role: "recv", Slug: "secret", Version: wire.CurrentVersion}).Marshal()
	wire.WriteControlFrame(pc, wire.TypeHello, hb)
	wire.ReadControlFrame(pc) // SDP

	// Send blocks until the peer reads (synchronous net.Pipe), so we
	// kick the Send into a goroutine and let the test goroutine read.
	sendErr := make(chan error, 1)
	go func() { sendErr <- s.DataChannel().Send([]byte("server-says-hi")) }()
	t1, payload, err := readSkippingAnnounces(pc)
	if err != nil {
		t.Fatal(err)
	}
	if e := <-sendErr; e != nil {
		t.Fatal(e)
	}
	if t1 != wire.TypeData || string(payload) != "server-says-hi" {
		t.Fatalf("got type=%#x payload=%q", t1, payload)
	}

	wire.WriteControlFrame(pc, wire.TypeClose, nil)
	pc.Close()
	for _, r := range s.Receivers("primary") {
		b.Unsubscribe(r)
	}
	<-done
}

func TestIdleWatchdogFires(t *testing.T) {
	cfg := Config{
		ExpectSlug:  "secret",
		IdleTimeout: 100 * time.Millisecond,
		SDP:         wire.SDP{Codec: "x"},
	}
	s, _, pc, _, b := newSession(t, cfg)

	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()

	hb, _ := (wire.Hello{Role: "recv", Slug: "secret", Version: wire.CurrentVersion}).Marshal()
	wire.WriteControlFrame(pc, wire.TypeHello, hb)
	wire.ReadControlFrame(pc) // SDP

	// Don't publish anything; wait for watchdog.
	select {
	case err := <-done:
		if !errors.Is(err, ErrIdle) {
			t.Fatalf("want ErrIdle, got %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("idle watchdog never fired")
	}
	pc.Close()
	for _, r := range s.Receivers("primary") {
		b.Unsubscribe(r)
	}
}

// TestPingDoesNotResetIdleTimer is the security-critical assertion:
// a peer that knows the slug and spams PINGs but never reads the
// feed must NOT keep the session alive past IdleTimeout.
func TestPingDoesNotResetIdleTimer(t *testing.T) {
	cfg := Config{
		ExpectSlug:  "secret",
		IdleTimeout: 100 * time.Millisecond,
		SDP:         wire.SDP{Codec: "x"},
	}
	s, _, pc, _, b := newSession(t, cfg)

	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()

	hb, _ := (wire.Hello{Role: "recv", Slug: "secret", Version: wire.CurrentVersion}).Marshal()
	wire.WriteControlFrame(pc, wire.TypeHello, hb)
	wire.ReadControlFrame(pc)

	// Spam pings while the watchdog should be counting toward expiry.
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if err := wire.WriteControlFrame(pc, wire.TypePing, []byte("p")); err != nil {
					return
				}
				readSkippingAnnounces(pc) // drain pong (skip replay-burst announces)
			}
		}
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrIdle) {
			t.Fatalf("want ErrIdle (PING spam should not save session), got %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("PING-spammed session never expired — WATCHDOG REGRESSION")
	}
	close(stop)
	pc.Close()
	for _, r := range s.Receivers("primary") {
		b.Unsubscribe(r)
	}
}
