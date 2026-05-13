package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Datagram envelope for telemetry / fire-and-forget AUs. Wire layout:
//
//	[1B type][1B trackID][2B BE seq][payload]
//
// 4 bytes total envelope overhead. Smaller than the 17-byte stream
// feed header because datagrams have no stream context — there's no
// per-stream length prefix (the datagram boundary IS the length),
// no PTS (apps put timestamps in payload if they want them), and no
// flags (datagrams are unreliable; FlagKeyframe/FlagDiscontinuity
// have no meaning).
//
// TrackID is a session-scoped 1-byte handle assigned by the publisher
// at Announce time. 256 distinct datagram tracks per session is more
// than enough for AI workloads (typically 1-5: one telemetry track,
// maybe a few event tracks).
//
// Datagram payloads are bounded by the QUIC path MTU minus envelope
// overhead. quic-go does not currently expose MaxDatagramSize; we use
// a conservative MaxDatagramPayload constant and trust the application
// or coalescer to keep below it. Oversized sends return
// ErrDatagramTooLarge.
const (
	// DatagramHeaderLen is the fixed-size envelope in bytes.
	// Untyped int constant — matches the convention of FeedHeaderLen
	// so slice-index sites don't need explicit conversions.
	DatagramHeaderLen = 4

	// MaxDatagramPayload caps a single datagram payload. Picked
	// conservatively to fit inside the typical Ethernet 1500-byte
	// MTU after QUIC packet overhead (~80 bytes) and our 4-byte
	// envelope. Adjust upward only if path MTU is known to be larger.
	MaxDatagramPayload = 1100
)

// TypeDatagramAU is the datagram envelope type byte. Reserves a
// distinct value so an out-of-band byte stream couldn't accidentally
// look like a datagram. Picked from the same range as TypeFrame (0x22)
// to keep AU-style types grouped.
const TypeDatagramAU byte = 0x24

// ErrDatagramTooLarge is returned by encode when the payload exceeds
// MaxDatagramPayload.
var ErrDatagramTooLarge = errors.New("quicrtc/wire: datagram payload too large")

// EncodeDatagram appends a 4-byte envelope and the payload into dst.
// dst can be reused across calls (returned as a slice with cap >= 4+len(payload)).
func EncodeDatagram(dst []byte, trackID byte, seq uint16, payload []byte) ([]byte, error) {
	if len(payload) > MaxDatagramPayload {
		return nil, fmt.Errorf("%w: %d > %d", ErrDatagramTooLarge, len(payload), MaxDatagramPayload)
	}
	dst = append(dst, TypeDatagramAU, trackID, byte(seq>>8), byte(seq))
	dst = append(dst, payload...)
	return dst, nil
}

// DecodeDatagram parses a 4-byte envelope and returns the components.
// Returns an error if the buffer is too short or the type byte is
// not TypeDatagramAU.
func DecodeDatagram(b []byte) (trackID byte, seq uint16, payload []byte, err error) {
	if len(b) < DatagramHeaderLen {
		return 0, 0, nil, fmt.Errorf("quicrtc/wire: datagram too short: %d", len(b))
	}
	if b[0] != TypeDatagramAU {
		return 0, 0, nil, fmt.Errorf("quicrtc/wire: unexpected datagram type 0x%02x", b[0])
	}
	trackID = b[1]
	seq = binary.BigEndian.Uint16(b[2:4])
	payload = b[DatagramHeaderLen:]
	return
}
