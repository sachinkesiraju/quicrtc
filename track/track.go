// Package track defines the LocalTrack/RemoteTrack abstraction
// shared across transports.
//
// A track is the unit of routing: each published stream of access
// units (AUs) has a Name, a Kind, and a DeliveryClass that selects
// transport behavior. Kinds map to default delivery classes so most
// callers never set Delivery directly — track.Video(...) gets
// stream-per-GOP video, track.Tokens(...) gets a low-latency
// persistent stream, etc.
package track

// Kind names a category of track. Each kind comes with sensible
// defaults (delivery class, priority) so AI-shaped consumers don't
// reinvent framing per app.
type Kind string

const (
	// KindVideo carries codec-agnostic video access units. One AU =
	// one decodable picture's worth of bytes. Per-stream FIFO
	// preserves decode order. Default delivery: stream-per-GOP.
	KindVideo Kind = "video"

	// KindAudio carries audio frames. Codec is whatever you put in
	// LocalTrack.Codec (Opus is the typical choice). Default
	// delivery: low-latency persistent stream. Note: this transport
	// does not implement FEC or a jitter buffer, so for real-time
	// conversational audio under loss, WebRTC remains the gold
	// standard — use this kind for non-conversational audio playback
	// or accept the quality tradeoff.
	KindAudio Kind = "audio"

	// KindTokens carries small structured AUs typical of LLM token
	// streams. Reliable + ordered; high priority; default delivery
	// is a persistent low-latency uni stream that avoids per-AU
	// stream churn.
	KindTokens Kind = "tokens"

	// KindToolCalls carries structured RPC-shaped events from agents
	// (function calls, tool invocations). Default delivery: one
	// bidi stream per call so parallel calls don't HOL-block each
	// other under packet loss.
	KindToolCalls Kind = "toolcalls"

	// KindTelemetry carries unreliable, fire-and-forget data. Default
	// delivery: WebTransport datagrams (or a low-priority stream
	// fallback when datagrams aren't available). Low priority;
	// payloads bounded by the path MTU.
	KindTelemetry Kind = "telemetry"
)

// Priority maps to QUIC stream priority hints (RFC 9218
// PRIORITY_UPDATE numeric scale: lower = more urgent). RFC 9000 has
// no native stream priority, so this is plumbed through the app-layer
// scheduler (see feed.Scheduler) rather than to QUIC directly.
type Priority uint8

const (
	PriorityCritical   Priority = 0
	PriorityHigh       Priority = 2
	PriorityNormal     Priority = 4
	PriorityBackground Priority = 7
)

// DeliveryClass selects the transport's stream/dispatch behavior for
// a track. Derived from Kind at track construction; internal routing
// concern — not user-facing in steady state. The four classes map to
// the four AI-workload shapes quicrtc optimizes for:
//
//   - StreamGOP: per-keyframe uni stream, P-frame-droppable, keyframe-
//     aware resync. The video path (current default behavior).
//
//   - StreamLowLatency: one persistent uni stream per track, no GOP
//     semantics, drop-oldest on bounded queue overflow. Tuned for
//     LLM tokens, terminal output, small frequent AUs where TTFT
//     and inter-arrival jitter matter more than throughput.
//
//   - BidiPerCall: one short-lived bidi stream per logical call,
//     reliable+ordered, never drops. Tuned for RPC-shaped tool
//     calls / computer-use actions where parallel calls must not
//     HOL-block each other under loss.
//
//   - DatagramOrStream: WebTransport datagram by default, falls back
//     to a low-priority uni stream if datagram queue saturates.
//     Tuned for fire-and-forget telemetry.
type DeliveryClass uint8

const (
	// DeliveryStreamGOP is the existing video-style stream-per-GOP
	// behavior. Default for KindVideo and the zero-value fallback for
	// tracks whose kind was not specified at AddTrack time.
	DeliveryStreamGOP DeliveryClass = iota

	// DeliveryStreamLowLatency uses one persistent uni stream per
	// track with app-layer bounded queue + write deadline as the
	// backpressure mechanism. No keyframe/Pframe distinction; every
	// AU is independent.
	DeliveryStreamLowLatency

	// DeliveryBidiPerCall opens one bidi stream per AU (treated as
	// a logical call) with FIN-on-request-complete and result on the
	// read side. Default for KindToolCalls.
	DeliveryBidiPerCall

	// DeliveryDatagramOrStream sends each AU as a WebTransport
	// datagram and falls back to a low-priority uni stream when
	// datagrams aren't available. Default for KindTelemetry.
	DeliveryDatagramOrStream
)

// String returns a stable string label for diagnostics and metrics.
func (c DeliveryClass) String() string {
	switch c {
	case DeliveryStreamGOP:
		return "stream-gop"
	case DeliveryStreamLowLatency:
		return "stream-low-latency"
	case DeliveryBidiPerCall:
		return "bidi-per-call"
	case DeliveryDatagramOrStream:
		return "datagram-or-stream"
	default:
		return "unknown"
	}
}

// DefaultPriority maps a Kind to its preferred Priority. Centralizes
// the per-Kind defaults so constructors in presets.go, server.AddTrackSpec,
// and any external caller stay in sync.
//
// Note that PriorityCritical is the zero value of Priority (0); callers
// that pass spec.Priority=0 with an explicit Kind get this default
// rather than being silently bumped to PriorityNormal.
func DefaultPriority(k Kind) Priority {
	switch k {
	case KindAudio:
		return PriorityCritical
	case KindVideo, KindTokens, KindToolCalls:
		return PriorityHigh
	case KindTelemetry:
		return PriorityBackground
	default:
		return PriorityNormal
	}
}

// DefaultDeliveryClass maps a Kind to its preferred DeliveryClass.
// Used at track construction to populate LocalTrack.Delivery and at
// session-attach time to populate feed.Config.Delivery.
func DefaultDeliveryClass(k Kind) DeliveryClass {
	switch k {
	case KindVideo:
		return DeliveryStreamGOP
	case KindTokens, KindAudio:
		return DeliveryStreamLowLatency
	case KindToolCalls:
		return DeliveryBidiPerCall
	case KindTelemetry:
		return DeliveryDatagramOrStream
	default:
		return DeliveryStreamGOP
	}
}

// LocalTrack is a track this peer is producing.
type LocalTrack struct {
	// Name uniquely identifies the track within its session. Used as
	// the demux key on the receiver side for multi-track sessions
	// (TypeStreamHeader carries the name on the wire).
	Name string

	// Kind selects the track category and its defaults.
	Kind Kind

	// Codec is the codec descriptor (e.g., "avc1.42E01F", "opus",
	// "" for non-codec kinds like Tokens).
	Codec string

	// Priority hints the transport's scheduler.
	Priority Priority

	// Delivery selects the transport's per-kind dispatch behavior.
	// Zero value is DeliveryStreamGOP (the safe default that matches
	// the legacy single-pump video path). Constructors in presets.go
	// populate this per Kind; users typically don't set it directly.
	Delivery DeliveryClass
}

// RemoteTrack is a track this peer is subscribed to.
type RemoteTrack struct {
	Name     string
	Kind     Kind
	Codec    string
	Priority Priority
	Delivery DeliveryClass
}
