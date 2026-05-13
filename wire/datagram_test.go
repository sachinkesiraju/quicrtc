package wire

import (
	"bytes"
	"errors"
	"testing"
)

// TestDatagramEncodeDecodeRoundTrip exercises the wire format on
// representative payload sizes and verifies header bytes are exactly
// where they should be.
func TestDatagramEncodeDecodeRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		trackID byte
		seq     uint16
		payload []byte
	}{
		{"empty payload", 0, 0, nil},
		{"typical telemetry", 0x42, 0x1337, []byte("event=action_start;t=12345")},
		{"max seq", 1, 0xFFFF, []byte{1, 2, 3, 4, 5}},
		{"max trackID", 0xFF, 0, []byte("xyz")},
		{"near-MTU payload", 0x10, 99, bytes.Repeat([]byte{0xAB}, MaxDatagramPayload-1)},
		{"max-size payload", 0x10, 99, bytes.Repeat([]byte{0xCD}, MaxDatagramPayload)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enc, err := EncodeDatagram(nil, tc.trackID, tc.seq, tc.payload)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if len(enc) != DatagramHeaderLen+len(tc.payload) {
				t.Fatalf("encoded length: got %d want %d", len(enc), DatagramHeaderLen+len(tc.payload))
			}
			if enc[0] != TypeDatagramAU {
				t.Fatalf("type byte: got 0x%02x want 0x%02x", enc[0], TypeDatagramAU)
			}
			if enc[1] != tc.trackID {
				t.Fatalf("trackID byte: got 0x%02x want 0x%02x", enc[1], tc.trackID)
			}
			id, seq, payload, err := DecodeDatagram(enc)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if id != tc.trackID || seq != tc.seq {
				t.Fatalf("decoded header: got id=%d seq=%d want id=%d seq=%d",
					id, seq, tc.trackID, tc.seq)
			}
			if !bytes.Equal(payload, tc.payload) {
				t.Fatalf("payload mismatch")
			}
		})
	}
}

// TestDatagramTooLarge confirms oversize payloads error rather than
// silently truncate.
func TestDatagramTooLarge(t *testing.T) {
	oversize := bytes.Repeat([]byte{0xFF}, MaxDatagramPayload+1)
	_, err := EncodeDatagram(nil, 0, 0, oversize)
	if err == nil {
		t.Fatal("expected ErrDatagramTooLarge, got nil")
	}
	if !errors.Is(err, ErrDatagramTooLarge) {
		t.Fatalf("expected ErrDatagramTooLarge, got %v", err)
	}
}

// TestDatagramDecodeRejectsShort and TestDatagramDecodeRejectsBadType
// guard the receive path against malformed input.
func TestDatagramDecodeRejectsShort(t *testing.T) {
	if _, _, _, err := DecodeDatagram([]byte{0x24, 0x00}); err == nil {
		t.Fatal("expected error on short buffer")
	}
}

func TestDatagramDecodeRejectsBadType(t *testing.T) {
	if _, _, _, err := DecodeDatagram([]byte{0x99, 0, 0, 0, 'x'}); err == nil {
		t.Fatal("expected error on unrecognized type byte")
	}
}

// ------ Benchmarks ------

// BenchmarkEncodeDatagram_Small mirrors the typical telemetry-sample
// payload (~64 bytes). Reuses dst to test the allocation-free path.
func BenchmarkEncodeDatagram_Small(b *testing.B) {
	payload := bytes.Repeat([]byte{0xAB}, 64)
	dst := make([]byte, 0, DatagramHeaderLen+64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = dst[:0]
		out, err := EncodeDatagram(dst, 0x42, uint16(i&0xFFFF), payload)
		if err != nil {
			b.Fatal(err)
		}
		dst = out
	}
}

// BenchmarkEncodeDatagram_NearMTU exercises the largest realistic
// payload (~1100 bytes), where the payload-copy dominates the cost.
func BenchmarkEncodeDatagram_NearMTU(b *testing.B) {
	payload := bytes.Repeat([]byte{0xCD}, MaxDatagramPayload)
	dst := make([]byte, 0, DatagramHeaderLen+MaxDatagramPayload)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = dst[:0]
		out, err := EncodeDatagram(dst, 0x10, uint16(i&0xFFFF), payload)
		if err != nil {
			b.Fatal(err)
		}
		dst = out
	}
}

// BenchmarkDecodeDatagram measures the receiver-side hot path. We
// encode once outside the timed loop, then decode repeatedly.
func BenchmarkDecodeDatagram(b *testing.B) {
	payload := bytes.Repeat([]byte{0xAB}, 64)
	enc, err := EncodeDatagram(nil, 0x42, 0x1337, payload)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, err := DecodeDatagram(enc)
		if err != nil {
			b.Fatal(err)
		}
	}
}
