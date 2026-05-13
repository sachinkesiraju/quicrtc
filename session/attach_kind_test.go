package session

import (
	"testing"

	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
)

// TestAttachTrackWithKindSetsDelivery is the regression test for the
// kind-aware-public-API bug. Before the fix, AttachTrackWithTrackID
// never assigned tp.cfg.Delivery, so every track ran on the legacy
// stream-per-GOP pump regardless of payload shape. This test pins the
// mapping: a token-kind track must land on StreamLowLatency, a
// telemetry-kind track on DatagramOrStream, a tool-call-kind on
// BidiPerCall, and an empty/video kind on the legacy GOP path.
func TestAttachTrackWithKindSetsDelivery(t *testing.T) {
	cases := []struct {
		name string
		kind track.Kind
		want track.DeliveryClass
	}{
		{"video", track.KindVideo, track.DeliveryStreamGOP},
		{"tokens", track.KindTokens, track.DeliveryStreamLowLatency},
		{"audio", track.KindAudio, track.DeliveryStreamLowLatency},
		{"toolcalls", track.KindToolCalls, track.DeliveryBidiPerCall},
		{"telemetry", track.KindTelemetry, track.DeliveryDatagramOrStream},
		{"empty", track.Kind(""), track.DeliveryStreamGOP},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _, _, _ := newSession(t, Config{})
			bc := pubsub.NewBroadcaster(8)
			s.AttachTrackWithKind(tc.name, tc.kind, bc.Subscribe(), 4, 0)

			s.trackMu.Lock()
			tp, ok := s.tracks[tc.name]
			s.trackMu.Unlock()
			if !ok {
				t.Fatalf("track %q not attached", tc.name)
			}
			if got := tp.cfg.Delivery; got != tc.want {
				t.Fatalf("kind=%q delivery=%v, want %v", tc.kind, got, tc.want)
			}
		})
	}
}

// TestAttachTrackWithTrackIDConsultsOnLookupKind is the regression
// test for the legacy attach path: when the caller has not migrated
// to AttachTrackWithKind but has set the OnLookupKind callback (as
// server.Server does), the legacy path must still arrive at the
// correct DeliveryClass by consulting the lookup. Before the fix,
// the legacy path ignored Kind entirely.
func TestAttachTrackWithTrackIDConsultsOnLookupKind(t *testing.T) {
	s, _, _, _, _ := newSession(t, Config{})
	s.OnLookupKind = func(name string) track.Kind {
		if name == "tokens" {
			return track.KindTokens
		}
		return track.KindVideo
	}

	bc1 := pubsub.NewBroadcaster(8)
	s.AttachTrackWithTrackID("tokens", bc1.Subscribe(), 4, 0)

	bc2 := pubsub.NewBroadcaster(8)
	s.AttachTrackWithTrackID("video", bc2.Subscribe(), 4, 0)

	s.trackMu.Lock()
	tpTokens := s.tracks["tokens"]
	tpVideo := s.tracks["video"]
	s.trackMu.Unlock()

	if tpTokens.cfg.Delivery != track.DeliveryStreamLowLatency {
		t.Fatalf("tokens: got %v, want StreamLowLatency", tpTokens.cfg.Delivery)
	}
	if tpVideo.cfg.Delivery != track.DeliveryStreamGOP {
		t.Fatalf("video: got %v, want StreamGOP", tpVideo.cfg.Delivery)
	}
}

// TestAttachTrackWithTrackIDLegacyFallback verifies that callers who
// haven't wired OnLookupKind (i.e. truly pre-fix usage) continue to
// get the legacy GOP path. This guarantees we have not broken any
// existing external caller that builds Session without populating
// the new callback.
func TestAttachTrackWithTrackIDLegacyFallback(t *testing.T) {
	s, _, _, _, _ := newSession(t, Config{})
	// OnLookupKind intentionally nil — simulates the pre-fix caller.

	bc := pubsub.NewBroadcaster(8)
	s.AttachTrackWithTrackID("legacy", bc.Subscribe(), 4, 0)

	s.trackMu.Lock()
	tp := s.tracks["legacy"]
	s.trackMu.Unlock()

	if tp.cfg.Delivery != track.DeliveryStreamGOP {
		t.Fatalf("legacy: got %v, want StreamGOP", tp.cfg.Delivery)
	}
}
