package session

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/wire"
)

var _ io.Reader = (*pipeStream)(nil)

// TestOnAnnounceRejectsTrack confirms that an OnAnnounce validator
// returning a non-nil error causes the server to emit a TypeError
// carrying a JSON ErrorPayload{Code: "track_unauthorized"}.
//
// The companion stream-header race fix (a uni stream naming an
// unauthorized track is dropped before any AU is enqueued) is
// covered indirectly: drainInboundStream uses the same
// announced/authorized state set here.
func TestOnAnnounceRejectsTrack(t *testing.T) {
	cfg := Config{
		ExpectSlug:  "secret",
		IdleTimeout: time.Hour,
		SDP:         wire.SDP{Codec: "avc1.42E01F", Width: 1280, Height: 720, FPS: 30},
		OnAnnounce: func(tenant, sessionID, trackName, kind string) error {
			if strings.HasPrefix(trackName, "private/") {
				return errors.New("namespace not allowed")
			}
			return nil
		},
	}
	s, _, pc, _, b := newSession(t, cfg)

	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()

	// HELLO + SDP handshake.
	hello := wire.Hello{Role: "recv", Slug: "secret", Version: wire.CurrentVersion}
	hb, _ := hello.Marshal()
	if err := wire.WriteControlFrame(pc, wire.TypeHello, hb); err != nil {
		t.Fatal(err)
	}
	t1, _, err := wire.ReadControlFrame(pc)
	if err != nil || t1 != wire.TypeSDP {
		t.Fatalf("handshake failed: t=%#x err=%v", t1, err)
	}

	// Subscriber announces a rejected track. Server must emit TypeError.
	// Skip any post-handshake Announce echoes from the server (it
	// re-broadcasts the track set on connect) before checking for the
	// error frame.
	bad := wire.Announce{Name: "private/foo", Kind: "tokens"}
	bp, _ := bad.Marshal()
	if err := wire.WriteControlFrame(pc, wire.TypeAnnounce, bp); err != nil {
		t.Fatal(err)
	}
	t2, payload, err := readSkippingAnnounces(pc)
	if err != nil {
		t.Fatalf("read TypeError: %v", err)
	}
	if t2 != wire.TypeError {
		t.Fatalf("got frame type %#x, want TypeError", t2)
	}
	ep, err := wire.UnmarshalError(payload)
	if err != nil {
		t.Fatalf("error payload did not parse: %v (raw=%q)", err, payload)
	}
	if ep.Code != "track_unauthorized" {
		t.Fatalf("ErrorPayload.Code = %q, want 'track_unauthorized'", ep.Code)
	}
	if !strings.Contains(ep.Reason, "namespace") {
		t.Fatalf("ErrorPayload.Reason = %q, want substring 'namespace'", ep.Reason)
	}

	// Clean shutdown.
	_ = wire.WriteControlFrame(pc, wire.TypeClose, nil)
	_ = pc.Close()
	for _, r := range s.Receivers("primary") {
		b.Unsubscribe(r)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("session didn't end")
	}
}

// TestOnAnnounceAcceptsAllowedTrack mirrors the rejection test for
// the happy path: an OnAnnounce validator that returns nil leaves
// the track normally announced and authorized.
func TestOnAnnounceAcceptsAllowedTrack(t *testing.T) {
	cfg := Config{
		ExpectSlug:  "secret",
		IdleTimeout: time.Hour,
		SDP:         wire.SDP{Codec: "avc1.42E01F", Width: 1280, Height: 720, FPS: 30},
		OnAnnounce: func(tenant, sessionID, trackName, kind string) error {
			return nil // accept all
		},
	}
	s, _, pc, _, b := newSession(t, cfg)

	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()

	hello := wire.Hello{Role: "recv", Slug: "secret", Version: wire.CurrentVersion}
	hb, _ := hello.Marshal()
	if err := wire.WriteControlFrame(pc, wire.TypeHello, hb); err != nil {
		t.Fatal(err)
	}
	if _, _, err := wire.ReadControlFrame(pc); err != nil {
		t.Fatal(err)
	}

	ann := wire.Announce{Name: "public/foo", Kind: "tokens"}
	ap, _ := ann.Marshal()
	if err := wire.WriteControlFrame(pc, wire.TypeAnnounce, ap); err != nil {
		t.Fatal(err)
	}

	// Give the control reader a moment to process; no TypeError should arrive.
	time.Sleep(50 * time.Millisecond)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		tracks := s.InboundTracks()
		for _, name := range tracks {
			if name == "public/foo" {
				goto ok
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("public/foo not registered as announced")
ok:

	_ = wire.WriteControlFrame(pc, wire.TypeClose, nil)
	_ = pc.Close()
	for _, r := range s.Receivers("primary") {
		b.Unsubscribe(r)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("session didn't end")
	}
}
