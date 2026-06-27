package wire

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestControlFrameRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		typ     byte
		payload []byte
	}{
		{"hello", TypeHello, []byte(`{"role":"recv","slug":"abc","ver":"1"}`)},
		{"sdp", TypeSDP, []byte(`{"codec":"avc1.42E01F","width":1280,"height":720,"fps":30}`)},
		{"ping_empty", TypePing, nil},
		{"pong_empty", TypePong, []byte{}},
		{"close", TypeClose, nil},
		{"error", TypeError, []byte("auth")},
		{"data", TypeData, bytes.Repeat([]byte{0x42}, 1024)},
		{"data_max", TypeData, bytes.Repeat([]byte{0xab}, MaxControlPayload)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteControlFrame(&buf, tc.typ, tc.payload); err != nil {
				t.Fatalf("write: %v", err)
			}
			gotT, gotP, err := ReadControlFrame(&buf)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if gotT != tc.typ {
				t.Fatalf("type: got %#x want %#x", gotT, tc.typ)
			}
			if !bytes.Equal(gotP, tc.payload) && !(len(gotP) == 0 && len(tc.payload) == 0) {
				t.Fatalf("payload mismatch (len got=%d want=%d)", len(gotP), len(tc.payload))
			}
			if buf.Len() != 0 {
				t.Fatalf("trailing bytes after read: %d", buf.Len())
			}
		})
	}
}

func TestControlFrameTooLargeWrite(t *testing.T) {
	var buf bytes.Buffer
	err := WriteControlFrame(&buf, TypeData, make([]byte, MaxControlPayload+1))
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("want ErrFrameTooLarge, got %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("partial write on too-large: %d bytes", buf.Len())
	}
}

func TestControlFrameTooLargeRead(t *testing.T) {
	// Forge a header claiming a payload larger than MaxControlPayload.
	hdr := []byte{TypeData, 0xff, 0xff, 0xff} // length = 0xFFFFFF
	_, _, err := ReadControlFrame(bytes.NewReader(hdr))
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("want ErrFrameTooLarge, got %v", err)
	}
}

func TestControlFrameTruncated(t *testing.T) {
	// Length says 100 but only 4 bytes follow.
	bad := []byte{TypeData, 0x00, 0x00, 0x64, 'a', 'b', 'c', 'd'}
	_, _, err := ReadControlFrame(bytes.NewReader(bad))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want io.ErrUnexpectedEOF, got %v", err)
	}
}

func TestControlFrameEmptyReader(t *testing.T) {
	_, _, err := ReadControlFrame(strings.NewReader(""))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("want io.EOF, got %v", err)
	}
}

func TestFeedFrameRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		typ  byte
		pts  uint64
		seq  uint32
		flag byte
		pl   []byte
	}{
		{"keyframe_small", TypeKeyframe, 0, 0, FlagKeyframe, []byte{0x00, 0x00, 0x00, 0x01, 0x67}},
		{"pframe", TypePFrame, 33333, 1, 0, []byte{0x00, 0x00, 0x00, 0x01, 0x41}},
		{"keyframe_disco", TypeKeyframe, 1_000_000, 30, FlagKeyframe | FlagDiscontinuity, bytes.Repeat([]byte{0xff}, 4096)},
		{"max_pts", TypePFrame, ^uint64(0), ^uint32(0), 0, []byte{0xab}},
		{"empty", TypePFrame, 0, 0, 0, nil},
		{"generic_checkpoint", TypeFrame, 1234, 5, FlagKeyframe, []byte("token-batch-self-contained")},
		{"generic_continuation", TypeFrame, 1235, 6, 0, []byte("token-batch-continuation")},
		{"generic_discontinuity", TypeFrame, 1236, 7, FlagKeyframe | FlagDiscontinuity, []byte{0xab, 0xcd}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteFeedFrame(&buf, tc.typ, tc.pts, tc.seq, tc.flag, tc.pl); err != nil {
				t.Fatalf("write: %v", err)
			}
			f, err := ReadFeedFrame(&buf)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if f.Type != tc.typ || f.PTSMicro != tc.pts || f.Seq != tc.seq || f.Flags != tc.flag {
				t.Fatalf("hdr mismatch: %+v vs (typ=%#x pts=%d seq=%d flag=%#x)", f, tc.typ, tc.pts, tc.seq, tc.flag)
			}
			if !bytes.Equal(f.Payload, tc.pl) && !(len(f.Payload) == 0 && len(tc.pl) == 0) {
				t.Fatalf("payload mismatch")
			}
			if buf.Len() != 0 {
				t.Fatalf("trailing bytes: %d", buf.Len())
			}
		})
	}
}

func TestFeedFrameWallRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		flag    byte
		pubWall uint64
		pl      []byte
	}{
		{"keyframe_wall", FlagKeyframe, 1_700_000_000_000_000, []byte{0x67}},
		{"disco_wall", FlagKeyframe | FlagDiscontinuity, 42, bytes.Repeat([]byte{0xaa}, 2048)},
		{"empty_payload_wall", 0, ^uint64(0), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteFeedFrameWall(&buf, TypeKeyframe, 9999, 7, tc.flag, tc.pubWall, tc.pl); err != nil {
				t.Fatalf("write: %v", err)
			}
			f, err := ReadFeedFrame(&buf)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if f.Flags&FlagPublishWall == 0 {
				t.Fatalf("FlagPublishWall not set on read: flags=%#x", f.Flags)
			}
			if f.PubWallMicro != tc.pubWall {
				t.Fatalf("pubWall mismatch: got %d want %d", f.PubWallMicro, tc.pubWall)
			}
			if f.PTSMicro != 9999 || f.Seq != 7 {
				t.Fatalf("hdr mismatch: %+v", f)
			}
			if !bytes.Equal(f.Payload, tc.pl) && !(len(f.Payload) == 0 && len(tc.pl) == 0) {
				t.Fatalf("payload mismatch")
			}
			if buf.Len() != 0 {
				t.Fatalf("trailing bytes: %d", buf.Len())
			}
		})
	}
}

// TestFeedFramePlainHasNoWall confirms a v1-style frame (no
// FlagPublishWall) carries no extension and decodes with PubWallMicro
// == 0 — the backward-compatibility guarantee for peers that never
// negotiated "kind-stats".
func TestFeedFramePlainHasNoWall(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFeedFrame(&buf, TypeKeyframe, 1, 2, FlagKeyframe, []byte{0x01}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if buf.Len() != FeedHeaderLen+1 {
		t.Fatalf("plain frame should be header+payload (%d), got %d", FeedHeaderLen+1, buf.Len())
	}
	f, err := ReadFeedFrame(&buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if f.Flags&FlagPublishWall != 0 || f.PubWallMicro != 0 {
		t.Fatalf("plain frame should have no wall: flags=%#x wall=%d", f.Flags, f.PubWallMicro)
	}
}

func TestFeedFrameTooLarge(t *testing.T) {
	var buf bytes.Buffer
	err := WriteFeedFrame(&buf, TypeKeyframe, 0, 0, FlagKeyframe, make([]byte, MaxFeedPayload+1))
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("want ErrFrameTooLarge, got %v", err)
	}
	// MaxFeedPayload == 0xFFFFFF is the largest value the 3-byte length
	// field can express. A header at exactly that boundary should pass
	// the size check (and then fail with EOF because we didn't supply
	// the payload bytes).
	hdr := make([]byte, FeedHeaderLen)
	hdr[0] = TypeKeyframe
	hdr[1], hdr[2], hdr[3] = 0xff, 0xff, 0xff // 0xFFFFFF, exact cap
	_, err = ReadFeedFrame(bytes.NewReader(hdr))
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected truncation EOF for boundary length, got %v", err)
	}
}

func TestIsKeyframe(t *testing.T) {
	cases := []struct {
		f    FeedFrame
		want bool
	}{
		{FeedFrame{Type: TypeKeyframe, Flags: FlagKeyframe}, true},
		{FeedFrame{Type: TypePFrame, Flags: FlagKeyframe}, true}, // flag wins
		{FeedFrame{Type: TypeKeyframe, Flags: 0}, true},          // type wins
		{FeedFrame{Type: TypePFrame, Flags: 0}, false},
		{FeedFrame{Type: TypePFrame, Flags: FlagDiscontinuity}, false},
		{FeedFrame{Type: TypeFrame, Flags: FlagKeyframe}, true}, // generic + checkpoint flag
		{FeedFrame{Type: TypeFrame, Flags: 0}, false},           // generic + continuation
	}
	for i, tc := range cases {
		if got := tc.f.IsKeyframe(); got != tc.want {
			t.Errorf("case %d: got %v want %v (%+v)", i, got, tc.want, tc.f)
		}
	}
}

func TestHelloRoundTrip(t *testing.T) {
	h := Hello{Role: "recv", Slug: "abcdefg", Version: CurrentVersion, Features: []string{"resume", "backpressure"}}
	b, err := h.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalHello(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Role != h.Role || got.Slug != h.Slug || got.Version != h.Version || got.SessionID != h.SessionID {
		t.Fatalf("got %+v want %+v", got, h)
	}
	if len(got.Features) != len(h.Features) {
		t.Fatalf("Features round-trip mismatch: got %v want %v", got.Features, h.Features)
	}
	for i := range h.Features {
		if got.Features[i] != h.Features[i] {
			t.Fatalf("Features[%d]: got %q want %q", i, got.Features[i], h.Features[i])
		}
	}
}

func TestSDPRoundTrip(t *testing.T) {
	s := SDP{Codec: "avc1.42E01F", Width: 1280, Height: 720, FPS: 30, ScreenW: 1440, ScreenH: 900, Features: []string{"resume"}}
	b, err := s.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalSDP(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Codec != s.Codec || got.Width != s.Width || got.Height != s.Height || got.FPS != s.FPS ||
		got.ScreenW != s.ScreenW || got.ScreenH != s.ScreenH || got.SessionID != s.SessionID {
		t.Fatalf("got %+v want %+v", got, s)
	}
	if len(got.Features) != len(s.Features) || (len(got.Features) > 0 && got.Features[0] != s.Features[0]) {
		t.Fatalf("Features round-trip mismatch: got %v want %v", got.Features, s.Features)
	}
}

func TestAnnounceRoundTrip(t *testing.T) {
	a := Announce{Name: "actions", Kind: "toolcalls", Codec: ""}
	b, err := a.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalAnnounce(b)
	if err != nil {
		t.Fatal(err)
	}
	if got != a {
		t.Fatalf("got %+v want %+v", got, a)
	}
}

func TestUnannounceRoundTrip(t *testing.T) {
	u := Unannounce{Name: "screen-share"}
	b, err := u.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalUnannounce(b)
	if err != nil {
		t.Fatal(err)
	}
	if got != u {
		t.Fatalf("got %+v want %+v", got, u)
	}
}

func TestResumeRoundTrip(t *testing.T) {
	r := Resume{TrackName: "video", FromSeq: 4242}
	b, err := r.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalResume(b)
	if err != nil {
		t.Fatal(err)
	}
	if got != r {
		t.Fatalf("got %+v want %+v", got, r)
	}
}

// TestNoOverflowOnLargePayload verifies that MaxFeedPayload is exactly
// the maximum value representable in the 3-byte length field. Prior to
// the v0.1.1 fix, MaxFeedPayload was 16*1024*1024 (0x1000000) — one
// byte beyond the 3-byte cap — so a payload of exactly MaxFeedPayload
// bytes silently encoded length=0 and desynced the stream.
func TestNoOverflowOnLargePayload(t *testing.T) {
	const max3byte = 0xFFFFFF
	if max3byte != MaxFeedPayload {
		t.Fatalf("MaxFeedPayload (%d) must equal max-3-byte (%d) so length encoding never truncates", MaxFeedPayload, max3byte)
	}
}

// TestMaxFeedPayloadBoundaryRoundTrip verifies that a payload of exactly
// MaxFeedPayload bytes round-trips correctly — encoder writes the full
// 3-byte length, decoder reads it back, no truncation.
func TestMaxFeedPayloadBoundaryRoundTrip(t *testing.T) {
	payload := make([]byte, MaxFeedPayload)
	payload[0] = 0xAB
	payload[len(payload)-1] = 0xCD
	var buf bytes.Buffer
	if err := WriteFeedFrame(&buf, TypeKeyframe, 1, 2, FlagKeyframe, payload); err != nil {
		t.Fatalf("write at boundary: %v", err)
	}
	f, err := ReadFeedFrame(&buf)
	if err != nil {
		t.Fatalf("read at boundary: %v", err)
	}
	if len(f.Payload) != MaxFeedPayload {
		t.Fatalf("payload len got %d want %d", len(f.Payload), MaxFeedPayload)
	}
	if f.Payload[0] != 0xAB || f.Payload[MaxFeedPayload-1] != 0xCD {
		t.Fatalf("payload bytes corrupted at boundary")
	}
}
