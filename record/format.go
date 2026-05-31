// Package record implements deterministic session recording and
// replay built on the Broadcaster interceptor primitive. A recorder
// taps every published AU on every track and writes a length-prefixed
// binary log; a replayer reads the log and re-publishes the AUs into a
// fresh server, preserving inter-frame timing.
//
// File format (no third-party dependencies; pure encoding/binary):
//
//	[8B magic "QRTCREC\x00"][4B BE format_version]
//	  per frame:
//	    [4B BE total_length]                 — bytes after this field
//	    [2B BE track_name_length][N bytes UTF-8]
//	    [1B kind_length][N bytes UTF-8]
//	    [8B BE pts_micros]
//	    [4B BE seq]
//	    [1B flags]                           — bit 0 keyframe, bit 1 discontinuity
//	    [8B BE captured_at_micros]           — wall clock at capture, for replay timing
//	    [4B BE payload_length][N bytes]
//	  [4B 0xFF 0xFF 0xFF 0xFF]               — EOF marker; readers stop here
//
// The recorder is mandatory-async: AUs are queued on a buffered
// channel and a single writer goroutine drains them to disk. The
// channel is sized so typical workloads don't block; overflow is
// counted (dropped) rather than blocking the publish hot path.
package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// FileMagic is the 8-byte header every recorder file begins with.
// "QRTCREC" + a NUL byte so a hex dump tool catches the format quickly.
const FileMagic = "QRTCREC\x00"

// FormatVersion is bumped whenever the on-disk layout changes in a
// way readers must distinguish. Old readers reject newer versions
// rather than silently misinterpreting fields.
const FormatVersion uint32 = 1

// Flag bits packed into each frame's flags byte.
const (
	flagKeyframe      byte = 1 << 0
	flagDiscontinuity byte = 1 << 1
)

// eofMarker is written after the last frame so partial-file readers
// can distinguish a clean shutdown from a truncation.
var eofMarker = []byte{0xFF, 0xFF, 0xFF, 0xFF}

// ErrCorruptHeader indicates the file's magic or version didn't match.
var ErrCorruptHeader = errors.New("record: corrupt header")

// ErrTrackNameTooLong is returned if a track name exceeds 16 bits.
var ErrTrackNameTooLong = errors.New("record: track name > 65535 bytes")

// ErrKindTooLong is returned if a kind string exceeds 8 bits.
var ErrKindTooLong = errors.New("record: kind > 255 bytes")

// frame is the on-disk shape of one recorded AU. Exposed for tests
// and for the replayer; users typically interact with AccessUnits.
type frame struct {
	TrackName        string
	Kind             string
	PTSMicros        uint64
	Seq              uint32
	Keyframe         bool
	Discontinuity    bool
	CapturedAtMicros int64
	Payload          []byte
}

func writeHeader(w io.Writer) error {
	var hdr [12]byte
	copy(hdr[:8], FileMagic)
	binary.BigEndian.PutUint32(hdr[8:12], FormatVersion)
	_, err := w.Write(hdr[:])
	return err
}

func readHeader(r io.Reader) error {
	var hdr [12]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return err
	}
	if string(hdr[:8]) != FileMagic {
		return ErrCorruptHeader
	}
	v := binary.BigEndian.Uint32(hdr[8:12])
	if v != FormatVersion {
		return fmt.Errorf("%w: format version %d (want %d)", ErrCorruptHeader, v, FormatVersion)
	}
	return nil
}

func encodeFrame(f frame) ([]byte, error) {
	if len(f.TrackName) > 0xFFFF {
		return nil, ErrTrackNameTooLong
	}
	if len(f.Kind) > 0xFF {
		return nil, ErrKindTooLong
	}
	body := make([]byte, 0,
		2+len(f.TrackName)+
			1+len(f.Kind)+
			8+4+1+8+
			4+len(f.Payload))
	body = appendU16(body, uint16(len(f.TrackName)))
	body = append(body, f.TrackName...)
	body = append(body, byte(len(f.Kind)))
	body = append(body, f.Kind...)
	body = appendU64(body, f.PTSMicros)
	body = appendU32(body, f.Seq)
	flags := byte(0)
	if f.Keyframe {
		flags |= flagKeyframe
	}
	if f.Discontinuity {
		flags |= flagDiscontinuity
	}
	body = append(body, flags)
	body = appendU64(body, uint64(f.CapturedAtMicros))
	body = appendU32(body, uint32(len(f.Payload)))
	body = append(body, f.Payload...)
	out := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(out[:4], uint32(len(body)))
	copy(out[4:], body)
	return out, nil
}

// maxFrameLen caps a single encoded frame body. No legitimate AU (video
// keyframe, screenshot, token batch) approaches this; it bounds the
// allocation decodeFrame will attempt on corrupt or hostile input.
const maxFrameLen = 64 << 20 // 64 MiB

func decodeFrame(r io.Reader) (frame, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return frame{}, err
	}
	if lenBuf == [4]byte{0xFF, 0xFF, 0xFF, 0xFF} {
		return frame{}, io.EOF
	}
	totalLen := binary.BigEndian.Uint32(lenBuf[:])
	// Guard against a corrupt/hostile length (replay -import reads external
	// files): refuse absurd sizes instead of pre-allocating up to ~4 GiB.
	if totalLen > maxFrameLen {
		return frame{}, ErrCorruptHeader
	}
	body := make([]byte, totalLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return frame{}, err
	}
	off := 0
	if len(body) < off+2 {
		return frame{}, ErrCorruptHeader
	}
	trackLen := binary.BigEndian.Uint16(body[off : off+2])
	off += 2
	if len(body) < off+int(trackLen) {
		return frame{}, ErrCorruptHeader
	}
	trackName := string(body[off : off+int(trackLen)])
	off += int(trackLen)
	if len(body) < off+1 {
		return frame{}, ErrCorruptHeader
	}
	kindLen := int(body[off])
	off++
	if len(body) < off+kindLen {
		return frame{}, ErrCorruptHeader
	}
	kind := string(body[off : off+kindLen])
	off += kindLen
	if len(body) < off+8+4+1+8+4 {
		return frame{}, ErrCorruptHeader
	}
	pts := binary.BigEndian.Uint64(body[off : off+8])
	off += 8
	seq := binary.BigEndian.Uint32(body[off : off+4])
	off += 4
	flags := body[off]
	off++
	capturedAt := int64(binary.BigEndian.Uint64(body[off : off+8]))
	off += 8
	payloadLen := binary.BigEndian.Uint32(body[off : off+4])
	off += 4
	if len(body) < off+int(payloadLen) {
		return frame{}, ErrCorruptHeader
	}
	payload := make([]byte, payloadLen)
	copy(payload, body[off:off+int(payloadLen)])
	return frame{
		TrackName:        trackName,
		Kind:             kind,
		PTSMicros:        pts,
		Seq:              seq,
		Keyframe:         flags&flagKeyframe != 0,
		Discontinuity:    flags&flagDiscontinuity != 0,
		CapturedAtMicros: capturedAt,
		Payload:          payload,
	}, nil
}

func writeEOF(w io.Writer) error {
	_, err := w.Write(eofMarker)
	return err
}

func appendU16(b []byte, v uint16) []byte {
	return append(b, byte(v>>8), byte(v))
}

func appendU32(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendU64(b []byte, v uint64) []byte {
	return append(b,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}
