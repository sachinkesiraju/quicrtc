package client

import (
	"testing"

	"github.com/sachinkesiraju/quicrtc/track"
)

// TestClientPublishDeliveryAvoidsGOPChurn locks in the reliability fix
// for the computer-use action tail: a client-published non-video track
// must use a single persistent low-latency stream, NOT the stream-per-
// GOP pump. GOP opens a fresh uni stream per keyframe AU; a tool-call
// track marks every AU Keyframe=true, so at 100 actions/sec that churned
// ~100 stream opens/sec, exhausting the peer's uni-stream credit and
// producing multi-second open stalls on the action path.
func TestClientPublishDeliveryAvoidsGOPChurn(t *testing.T) {
	cases := []struct {
		kind track.Kind
		want track.DeliveryClass
	}{
		{track.KindVideo, track.DeliveryStreamGOP},
		{track.Kind(""), track.DeliveryStreamGOP}, // legacy unspecified default
		{track.KindTokens, track.DeliveryStreamLowLatency},
		{track.KindToolCalls, track.DeliveryStreamLowLatency},
		{track.KindTelemetry, track.DeliveryStreamLowLatency},
		{track.KindAudio, track.DeliveryStreamLowLatency},
	}
	for _, tc := range cases {
		if got := clientPublishDelivery(tc.kind); got != tc.want {
			t.Errorf("clientPublishDelivery(%q) = %v, want %v", tc.kind, got, tc.want)
		}
	}
}
