package wire

import "testing"

func TestNegotiateVersion(t *testing.T) {
	cases := []struct {
		name                                       string
		clientMin, clientMax, serverMin, serverMax string
		want                                       string
		ok                                         bool
	}{
		{"exact match", "1", "1", "1", "1", "1", true},
		{"legacy single-version client", "1", "1", "1", "2", "1", true},
		{"legacy server, new client offers range", "1", "2", "1", "1", "1", true},
		{"both newer", "2", "2", "2", "2", "2", true},
		{"client below server min", "0", "0", "1", "2", "", false},
		{"client above server max", "3", "3", "1", "2", "", false},
		{"overlap picks highest", "1", "3", "2", "4", "3", true},
		{"multi-digit ordering", "9", "10", "9", "10", "10", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := NegotiateVersion(c.clientMin, c.clientMax, c.serverMin, c.serverMax)
			if ok != c.ok || got != c.want {
				t.Fatalf("got (%q, %v), want (%q, %v)", got, ok, c.want, c.ok)
			}
		})
	}
}

func TestHelloMarshalRoundTripWithVersionFields(t *testing.T) {
	h := Hello{
		Role:       "recv",
		Slug:       "abc",
		Version:    "1",
		MinVersion: "1",
		MaxVersion: "2",
	}
	b, err := h.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalHello(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.MinVersion != "1" || got.MaxVersion != "2" {
		t.Fatalf("min/max round-trip: got (%q, %q)", got.MinVersion, got.MaxVersion)
	}
}

func TestSDPRoundTripWithNegotiatedVersion(t *testing.T) {
	s := SDP{Codec: "avc1.42E01F", NegotiatedVersion: "1"}
	b, err := s.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalSDP(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.NegotiatedVersion != "1" {
		t.Fatalf("neg_ver round-trip: got %q", got.NegotiatedVersion)
	}
}
