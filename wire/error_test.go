package wire

import "testing"

func TestErrorPayloadRoundTrip(t *testing.T) {
	e := ErrorPayload{Code: "track_unauthorized", Reason: "private/foo blocked"}
	b, err := e.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalError(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Code != e.Code || got.Reason != e.Reason {
		t.Fatalf("round-trip mismatch: got %+v want %+v", got, e)
	}
}

func TestUnmarshalErrorLegacyText(t *testing.T) {
	got, err := UnmarshalError([]byte("auth"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Code != "" || got.Reason != "auth" {
		t.Fatalf("legacy fallback: got %+v", got)
	}
}

func TestUnmarshalErrorEmpty(t *testing.T) {
	got, err := UnmarshalError(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Code != "" || got.Reason != "" {
		t.Fatalf("empty payload: got %+v", got)
	}
}

func TestUnmarshalErrorMalformedJSON(t *testing.T) {
	got, _ := UnmarshalError([]byte("{not valid json"))
	if got.Reason != "{not valid json" {
		t.Fatalf("malformed JSON should fall back to Reason: got %+v", got)
	}
}
