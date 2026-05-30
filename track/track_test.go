package track

import "testing"

func TestVideoPreset(t *testing.T) {
	v := Video("primary", "avc1.42E01F")
	if v.Name != "primary" {
		t.Fatalf("name: got %q want primary", v.Name)
	}
	if v.Kind != KindVideo {
		t.Fatalf("kind: got %q want video", v.Kind)
	}
	if v.Codec != "avc1.42E01F" {
		t.Fatalf("codec: got %q", v.Codec)
	}
	if v.Priority != PriorityNormal {
		t.Fatalf("priority: got %d want %d", v.Priority, PriorityNormal)
	}
}

func TestPriorityOrdering(t *testing.T) {
	// Verify the priority constants are correctly ordered so that a
	// numeric "lower = more urgent" comparison works for QUIC stream
	// priority forwarding in Phase 3.
	if !(PriorityCritical < PriorityHigh && PriorityHigh < PriorityNormal && PriorityNormal < PriorityBackground) {
		t.Fatal("priority ordering broken")
	}
}

func TestAudioPreset(t *testing.T) {
	a := Audio("voice", "")
	if a.Kind != KindAudio || a.Codec != "opus" || a.Priority != PriorityCritical {
		t.Fatalf("audio defaults wrong: %+v", a)
	}
	b := Audio("voice", "g711")
	if b.Codec != "g711" {
		t.Fatalf("codec override not honored: %+v", b)
	}
}

func TestTokensPreset(t *testing.T) {
	tk := Tokens("output")
	if tk.Kind != KindTokens || tk.Priority != PriorityHigh {
		t.Fatalf("tokens defaults wrong: %+v", tk)
	}
	if tk.Codec != "" {
		t.Fatalf("tokens shouldn't carry a codec descriptor: %q", tk.Codec)
	}
}

func TestToolCallsPreset(t *testing.T) {
	tc := ToolCalls("functions")
	if tc.Kind != KindToolCalls || tc.Priority != PriorityHigh {
		t.Fatalf("toolcalls defaults wrong: %+v", tc)
	}
}

func TestTelemetryPreset(t *testing.T) {
	te := Telemetry("metrics")
	if te.Kind != KindTelemetry || te.Priority != PriorityBackground {
		t.Fatalf("telemetry defaults wrong: %+v", te)
	}
}
