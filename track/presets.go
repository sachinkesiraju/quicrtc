package track

// Video constructs a video track with sensible defaults: high
// priority, codec-defined keyframe semantics on the wire.
func Video(name, codec string) LocalTrack {
	return LocalTrack{
		Name:     name,
		Kind:     KindVideo,
		Codec:    codec,
		Priority: PriorityHigh,
		Delivery: DefaultDeliveryClass(KindVideo),
	}
}

// Audio constructs an audio track. codec defaults to "opus" when
// empty. Audio is PriorityCritical: voice agents tolerate dropped
// video frames but lose intelligibility quickly with audio drops.
func Audio(name, codec string) LocalTrack {
	if codec == "" {
		codec = "opus"
	}
	return LocalTrack{
		Name:     name,
		Kind:     KindAudio,
		Codec:    codec,
		Priority: PriorityCritical,
		Delivery: DefaultDeliveryClass(KindAudio),
	}
}

// Tokens constructs an LLM-token-stream track. Reliable + ordered
// (per-stream FIFO over QUIC); high priority because TTFT is the
// load-bearing UX KPI for chat / voice agents.
func Tokens(name string) LocalTrack {
	return LocalTrack{
		Name:     name,
		Kind:     KindTokens,
		Priority: PriorityHigh,
		Delivery: DefaultDeliveryClass(KindTokens),
	}
}

// ToolCalls constructs a structured tool-call event track for
// agent flows (function invocations, tool-use markers, etc.).
// Reliable + ordered; high priority because tool calls usually
// gate continued generation.
func ToolCalls(name string) LocalTrack {
	return LocalTrack{
		Name:     name,
		Kind:     KindToolCalls,
		Priority: PriorityHigh,
		Delivery: DefaultDeliveryClass(KindToolCalls),
	}
}

// Telemetry constructs an unreliable, fire-and-forget track. Phase
// 2 wires this to WebTransport datagrams so payloads beyond the
// datagram MTU are silently dropped rather than queued. Background
// priority — telemetry is the first thing to drop under contention.
func Telemetry(name string) LocalTrack {
	return LocalTrack{
		Name:     name,
		Kind:     KindTelemetry,
		Priority: PriorityBackground,
		Delivery: DefaultDeliveryClass(KindTelemetry),
	}
}
