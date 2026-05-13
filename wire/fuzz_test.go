package wire

import (
	"bytes"
	"testing"
)

// FuzzReadControlFrame ensures the control-frame parser never panics
// on adversarial input and never allocates more than MaxControlPayload
// bytes for the payload buffer.
func FuzzReadControlFrame(f *testing.F) {
	f.Add([]byte{0x01, 0x00, 0x00, 0x00})
	f.Add([]byte{0x07, 0x00, 0x00, 0x05, 'h', 'e', 'l', 'l', 'o'})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff}) // claims huge length
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, payload, _ := ReadControlFrame(bytes.NewReader(data))
		if len(payload) > MaxControlPayload {
			t.Fatalf("oversize payload returned: %d", len(payload))
		}
	})
}

// FuzzReadFeedFrame ensures the feed-frame parser never panics and
// never returns a payload larger than MaxFeedPayload.
func FuzzReadFeedFrame(f *testing.F) {
	f.Add([]byte{0x20, 0x00, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		ff, _ := ReadFeedFrame(bytes.NewReader(data))
		if len(ff.Payload) > MaxFeedPayload {
			t.Fatalf("oversize feed payload: %d", len(ff.Payload))
		}
	})
}

// FuzzControlRoundTripIdempotent: writing a frame then reading it
// must reproduce the original type/payload exactly.
func FuzzControlRoundTripIdempotent(f *testing.F) {
	f.Add(byte(0x01), []byte("hi"))
	f.Add(byte(0x07), []byte{})
	f.Add(byte(0xab), bytes.Repeat([]byte{0x55}, 1024))
	f.Fuzz(func(t *testing.T, typ byte, payload []byte) {
		if len(payload) > MaxControlPayload {
			t.Skip()
		}
		var buf bytes.Buffer
		if err := WriteControlFrame(&buf, typ, payload); err != nil {
			t.Fatalf("write: %v", err)
		}
		gotT, gotP, err := ReadControlFrame(&buf)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if gotT != typ {
			t.Fatalf("type drift: got %#x want %#x", gotT, typ)
		}
		if !bytes.Equal(gotP, payload) && !(len(gotP) == 0 && len(payload) == 0) {
			t.Fatalf("payload drift: %d vs %d", len(gotP), len(payload))
		}
	})
}
