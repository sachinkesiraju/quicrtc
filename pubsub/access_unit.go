// Package pubsub implements quicrtc's broadcaster: one publisher,
// many subscribers, with a drop policy tuned for live media.
//
// Drop policy summary:
//
//   - Keyframes never silently drop. If a subscriber's queue is full,
//     the oldest queued frame is evicted to make room. Reasoning:
//     baseline H.264 P-frame loss cascades visible garbage until the
//     next IDR, so losing a keyframe wastes a whole GOP for the
//     subscriber.
//
//   - P-frames drop silently when a subscriber is full, and the
//     subscriber is flagged with NeedsKeyframe(). Subsequent P-frames
//     are skipped at fanout time until the next keyframe arrives.
//
//   - On Subscribe, the latest cached keyframe (if any) is delivered
//     immediately so late joiners decode without waiting for the next
//     natural IDR.
//
// AccessUnit.Bytes is opaque — quicrtc does not parse codec payloads.
// Callers feed pre-framed bytes (Annex B for H.264, OBUs for AV1, etc.)
// and the same bytes flow through to subscribers.
package pubsub

// AccessUnit is one publishable media unit. For video this is a
// decodable picture's worth of bytes; for audio one packet; for
// tokens one batch of LLM output; for tool-calls one structured
// event; for telemetry one datagram-sized payload.
//
// Phase 1 fields (Bytes, Keyframe, PTSMicro, Seq, Discontinuity)
// are wire-compatible with v1. Phase 2 fields (Meta, SyncGroup,
// Priority) are populated only when wire format v2 is negotiated;
// v1 peers ignore them transparently because they live in a
// metadata block that v1 receivers don't read.
type AccessUnit struct {
	// Bytes is the codec payload. Treated as opaque by quicrtc.
	// Caller retains ownership conceptually but must not mutate after
	// Publish: the broadcaster fans the same slice out to every
	// subscriber.
	Bytes []byte

	// Keyframe marks an independently-decodable unit. For video,
	// literal keyframe (IDR). For non-video tracks (Tokens,
	// ToolCalls, Telemetry), abstract "self-contained checkpoint"
	// — receivers can decode/process from this AU without prior
	// context. Drop policy and stream-per-GOP wire pattern both
	// key off this flag.
	Keyframe bool

	// PTSMicro is the presentation timestamp in microseconds. quicrtc
	// does not consume it; it's serialized to the wire so subscribers
	// can pace rendering. Source-of-truth pacing convention is
	// frameIdx*(1e6/fps), which is monotonic regardless of pipeline
	// jitter.
	PTSMicro uint64

	// Seq is a monotonic per-publisher counter. Useful for diagnostics
	// and for detecting drops. Within a single feed stream, in-stream
	// FIFO order is the canonical decode order, so Seq is NOT load-
	// bearing for ordering.
	Seq uint32

	// Discontinuity is a hint that the decoder should reset state
	// before consuming this unit (e.g., after a long subscriber stall).
	Discontinuity bool

	// --- Phase 2 fields (wire format v2) ---

	// Meta is typed per-kind metadata. Examples by track kind:
	//   Tokens:    {"model":"claude-opus-4-7","step":"42",
	//               "logprob":"-1.23","finish":"false"}
	//   ToolCalls: {"request_id":"req_abc","function":"search",
	//               "args_schema":"v1"}
	//   Video:     {"layer":"1080p","sps":"<base64>"}
	// Wire-side: a varint-prefixed metadata block precedes the
	// payload (~5–20 bytes overhead per AU). v1 receivers ignore.
	// Nil or empty means "no metadata" — common for telemetry.
	Meta map[string]string

	// SyncGroup is a cross-track alignment reference. Two AUs from
	// different tracks with the same SyncGroup value are intended
	// to be presented together (e.g., video frame N and its
	// annotations track at frame N). Zero means "no sync group."
	// Used by multi-modal agent streams and annotated-video flows.
	SyncGroup uint32

	// Priority is a per-AU override of the track's priority hint.
	// Forwarded to QUIC stream priority (RFC 9218 PRIORITY_UPDATE)
	// in Phase 3. Zero means "use track default."
	Priority uint8
}
