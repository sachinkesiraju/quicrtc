// Package wire defines the on-the-wire frame format used on every
// QUIC/WebTransport stream a quicrtc session opens.
//
// Two stream shapes:
//
//   - Control stream (one bidi per session). Each frame is
//     [1B type][3B BE length][payload]. Used for HELLO, SDP,
//     heartbeat, errors, close, and DATA (datachannel) messages.
//
//   - Feed stream (server -> client, one uni per GOP for video).
//     Each frame is
//     [1B type][3B BE length][8B BE pts µs][4B BE seq][1B flags][payload].
//     Within a stream, QUIC's per-stream FIFO IS the decode order, so
//     receivers do not need a reorder buffer.
//
// The stream-per-GOP wire pattern is the load-bearing design choice
// (see package feed): each keyframe opens a fresh uni stream and the
// previous stream is RESET (not FIN'd) so any stale in-flight bytes
// from the prior GOP are dropped at the QUIC layer immediately. This
// bounds head-of-line blocking to a single GOP and gives the encoder
// a structural backpressure signal — faster-than-realtime encoding
// produces more keyframes which reset older streams which release
// flow-control credit.
//
// Inspired by IETF moq-lite's group-as-stream model. NOT wire-
// compatible with MoQ; see docs/spec.md.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Frame type bytes. The 0x01..0x0F range is reserved for built-in
// control frames; 0x20..0x2F for feed (media) frames. Application
// extensions should use 0x80..0xFF to avoid colliding with future
// quicrtc additions.
const (
	TypeHello byte = 0x01
	TypeSDP   byte = 0x02
	TypePing  byte = 0x03
	TypePong  byte = 0x04
	TypeError byte = 0x05
	TypeClose byte = 0x06
	TypeData  byte = 0x07 // datachannel message (control stream)

	// --- Phase 2 control frames (wire format v2) ---
	// These constants are reserved at the protocol layer; full
	// state-machine wiring lands when Phase 2 implements multi-
	// track and bidirectional native.

	// TypeSwitch (0x08) — subscriber requests a layer switch
	// (simulcast-lite, ROADMAP §2.5).
	TypeSwitch byte = 0x08

	// TypeSubscribe (0x09) — subscriber selects a remote track by
	// name (multi-track, ROADMAP §2.6).
	TypeSubscribe byte = 0x09

	// TypeAnnounce (0x0a) — peer announces an outbound track they
	// intend to publish on this session. Payload is a JSON
	// Announce{Name, Kind, Codec}. The server treats subscriber-
	// originated Announces as PublishBack declarations; subscribers
	// treat server-originated Announces as new server-side tracks
	// available for RecvOn.
	TypeAnnounce byte = 0x0a

	// TypeBackpressure (0x0b) — kind-aware slow-consumer signal
	// from subscriber back to publisher.
	TypeBackpressure byte = 0x0b

	// TypeUnannounce (0x0c) — peer that previously sent Announce
	// declares it will not publish more AUs on the named track.
	// Payload is JSON Unannounce{Name}. Receiver closes the per-
	// track recv queue after draining what's already buffered.
	TypeUnannounce byte = 0x0c

	// TypeResume (0x0d) — client requests resumption of a track
	// from a specific sequence number on reconnect. Payload is
	// JSON Resume{TrackName, FromSeq}. Server replays buffered AUs
	// from FromSeq onward (subject to replay-buffer capacity).
	TypeResume byte = 0x0d

	// TypeKindStats (0x0e) is the periodic per-Kind observability
	// report from subscriber back to publisher. Payload is JSON
	// KindStats{Kind, LastSeq, RecvP50Ms, RecvP99Ms, Dropped}. Sent
	// at ~1 Hz when the "kind-stats" feature is negotiated. Lets the
	// publisher see the true end-to-end p99 per lane without
	// guessing from send-side queue depth alone.
	TypeKindStats byte = 0x0e

	// Feed-frame range (0x20..0x2F): media-bearing AUs on uni feed
	// streams, plus stream-scoped envelopes.
	//   0x20 TypeKeyframe       — GOP-start frame
	//   0x21 TypePFrame         — subsequent in-GOP frame
	//   0x22 TypeFrame          — generic non-video AU (tokens, etc.)
	//   0x23 TypeStreamHeader   — first frame on a multi-track uni stream
	//   0x24 TypeDatagramAU     — datagram envelope (wire/datagram.go)
	TypeKeyframe byte = 0x20 // first frame of a new GOP on a feed stream
	TypePFrame   byte = 0x21 // subsequent in-GOP frame on a feed stream

	// TypeFrame is a generic AU type for non-video tracks (Phase 2
	// Tokens, ToolCalls, Telemetry; or any codec without keyframe/
	// P-frame distinction). The flag bits carry the abstract
	// equivalents:
	//   FlagKeyframe: this AU is a self-contained checkpoint —
	//     receivers can decode from here without prior context.
	//   FlagDiscontinuity: receivers should reset decoder state
	//     before this AU.
	// v1 receivers that don't recognize 0x22 silently drop frames
	// (per the existing forward-compat rule), so introducing the
	// type is non-breaking.
	TypeFrame byte = 0x22

	// TypeStreamHeader (0x23) is the FIRST frame on every uni feed
	// stream when multi-track is in use. It names the track this
	// stream carries, so the receiver can demux multiple concurrent
	// tracks. Wire layout matches a control frame:
	//   [1B 0x23][3B BE length][N bytes track name UTF-8]
	// Subsequent frames on the same stream are normal feed frames
	// (TypeKeyframe / TypePFrame / TypeFrame) for that track.
	//
	// Single-track senders MAY skip this frame and write feed frames
	// directly; receivers that see a feed-frame type byte first
	// fall back to the implicit "primary" track. This preserves
	// wire compatibility with v1 senders that pre-date multi-track.
	TypeStreamHeader byte = 0x23
)

// MaxStreamHeader caps the track name carried in a TypeStreamHeader
// frame. Track names are short identifiers; 1 KiB is plenty.
const MaxStreamHeader = 1024

// WriteStreamHeader writes a TypeStreamHeader frame announcing the
// track this uni stream carries. Must be the first frame written on
// the stream when multi-track is in use.
func WriteStreamHeader(w io.Writer, trackName string) error {
	if len(trackName) > MaxStreamHeader {
		return fmt.Errorf("%w: stream-header %d > %d", ErrFrameTooLarge, len(trackName), MaxStreamHeader)
	}
	hdr := [4]byte{TypeStreamHeader, byte(len(trackName) >> 16), byte(len(trackName) >> 8), byte(len(trackName))}
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(trackName) == 0 {
		return nil
	}
	_, err := w.Write([]byte(trackName))
	return err
}

// PeekStreamHeader looks at the first byte of a uni stream and, if it
// is TypeStreamHeader, reads the rest of the header and returns the
// track name plus a reader that yields the remaining bytes (the body
// of feed frames). If the first byte is anything else, the byte is
// returned in firstByte and the caller should treat the stream as a
// legacy single-track ("primary") stream whose first frame begins at
// firstByte.
//
// Returns:
//   - trackName != "" : header was present, body reader skips past it.
//   - trackName == "" && firstByte != 0 : no header, caller must
//     prepend firstByte before reading feed frames.
func PeekStreamHeader(r io.Reader) (trackName string, firstByte byte, err error) {
	var b [1]byte
	if _, err = io.ReadFull(r, b[:]); err != nil {
		return "", 0, err
	}
	if b[0] != TypeStreamHeader {
		return "", b[0], nil
	}
	var lenBuf [3]byte
	if _, err = io.ReadFull(r, lenBuf[:]); err != nil {
		return "", 0, err
	}
	length := uint32(lenBuf[0])<<16 | uint32(lenBuf[1])<<8 | uint32(lenBuf[2])
	if length > MaxStreamHeader {
		return "", 0, fmt.Errorf("%w: stream-header %d > %d", ErrFrameTooLarge, length, MaxStreamHeader)
	}
	if length == 0 {
		return "", 0, fmt.Errorf("quicrtc/wire: empty stream header")
	}
	name := make([]byte, length)
	if _, err = io.ReadFull(r, name); err != nil {
		return "", 0, err
	}
	return string(name), 0, nil
}

// Feed-frame flag bits. FlagKeyframe is redundant with TypeKeyframe
// but kept explicit so a transcoder/relay that switches stream
// boundaries doesn't have to re-derive keyframe-ness from type alone.
const (
	FlagKeyframe      byte = 1 << 0
	FlagDiscontinuity byte = 1 << 1
)

// MaxControlPayload caps a single control-stream frame. Hard limit
// before any allocation so an adversarial peer cannot trigger an
// OOM with a forged length prefix.
const MaxControlPayload = 64 * 1024

// MaxFeedPayload caps a single access unit. The on-the-wire length
// field is 3 bytes (24 bits), so the maximum encodable payload is
// 0xFFFFFF bytes (one byte short of 16 MiB). 1080p baseline H.264
// keyframes can run past 1 MiB at high bitrates; the ceiling is the
// largest value the length field can express without truncation.
const MaxFeedPayload = 0xFFFFFF

// FeedHeaderLen is the fixed-size feed-frame header in bytes:
// type(1) + len(3) + pts(8) + seq(4) + flags(1) = 17.
const FeedHeaderLen = 17

// ErrFrameTooLarge is returned when a length prefix exceeds the
// per-stream-shape maximum. It does NOT wrap a partial read — the
// caller can keep using the underlying stream only by closing it.
var ErrFrameTooLarge = errors.New("quicrtc/wire: frame too large")

// ReadControlFrame reads exactly one control-stream frame. It uses
// io.ReadFull on the raw reader; never wrap the underlying QUIC
// stream in bufio because over-reading would strand bytes in the
// buffer between calls.
func ReadControlFrame(r io.Reader) (typ byte, payload []byte, err error) {
	var hdr [4]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	typ = hdr[0]
	length := uint32(hdr[1])<<16 | uint32(hdr[2])<<8 | uint32(hdr[3])
	if length > MaxControlPayload {
		return 0, nil, fmt.Errorf("%w: control %d > %d", ErrFrameTooLarge, length, MaxControlPayload)
	}
	if length == 0 {
		return typ, nil, nil
	}
	payload = make([]byte, length)
	if _, err = io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return typ, payload, nil
}

// WriteControlFrame writes one typed framed message on a control
// stream. payload may be nil or empty.
func WriteControlFrame(w io.Writer, typ byte, payload []byte) error {
	if len(payload) > MaxControlPayload {
		return fmt.Errorf("%w: control %d > %d", ErrFrameTooLarge, len(payload), MaxControlPayload)
	}
	hdr := [4]byte{typ, byte(len(payload) >> 16), byte(len(payload) >> 8), byte(len(payload))}
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.Write(payload)
	return err
}

// FeedFrame is the parsed form of a feed-stream frame.
type FeedFrame struct {
	Type     byte
	PTSMicro uint64
	Seq      uint32
	Flags    byte
	Payload  []byte
}

// IsKeyframe is true if either the type or the keyframe flag bit
// indicates a keyframe (or, for TypeFrame, a self-contained
// checkpoint). Both are checked because relays may rewrite types
// but tend to preserve flag bits.
func (f *FeedFrame) IsKeyframe() bool {
	return f.Type == TypeKeyframe || (f.Flags&FlagKeyframe) != 0
}

// ReadFeedFrame reads exactly one feed-stream frame.
func ReadFeedFrame(r io.Reader) (FeedFrame, error) {
	var hdr [FeedHeaderLen]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return FeedFrame{}, err
	}
	length := uint32(hdr[1])<<16 | uint32(hdr[2])<<8 | uint32(hdr[3])
	if length > MaxFeedPayload {
		return FeedFrame{}, fmt.Errorf("%w: feed %d > %d", ErrFrameTooLarge, length, MaxFeedPayload)
	}
	f := FeedFrame{
		Type:     hdr[0],
		PTSMicro: binary.BigEndian.Uint64(hdr[4:12]),
		Seq:      binary.BigEndian.Uint32(hdr[12:16]),
		Flags:    hdr[16],
	}
	if length > 0 {
		f.Payload = make([]byte, length)
		if _, err := io.ReadFull(r, f.Payload); err != nil {
			return FeedFrame{}, err
		}
	}
	return f, nil
}

// WriteFeedFrame writes one feed-stream frame.
func WriteFeedFrame(w io.Writer, typ byte, ptsMicro uint64, seq uint32, flags byte, payload []byte) error {
	if len(payload) > MaxFeedPayload {
		return fmt.Errorf("%w: feed %d > %d", ErrFrameTooLarge, len(payload), MaxFeedPayload)
	}
	var hdr [FeedHeaderLen]byte
	hdr[0] = typ
	hdr[1] = byte(len(payload) >> 16)
	hdr[2] = byte(len(payload) >> 8)
	hdr[3] = byte(len(payload))
	binary.BigEndian.PutUint64(hdr[4:12], ptsMicro)
	binary.BigEndian.PutUint32(hdr[12:16], seq)
	hdr[16] = flags
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.Write(payload)
	return err
}
