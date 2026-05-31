package record

import (
	"errors"
	"io"
	"os"
	"sort"

	"github.com/sachinkesiraju/quicrtc/pubsub"
)

// Timeline is a read-only index over a recorded session file. It is
// built in a single forward pass (see BuildTimeline) and never holds
// payload bytes in memory — each entry stores only metadata plus the
// byte offset where the frame begins, so a developer can scrub a
// multi-hour recording without loading it all. Payloads are read
// lazily on demand via ReadPayload.
//
// The value over the streaming Replayer is alignment: every
// track is stamped on one capture clock, so SeekSnapshot can answer
// "what was on screen, what had the model just said, and what tool did
// it just call at moment t" — the three things you want when an agent
// run goes wrong.
type Timeline struct {
	path    string
	entries []Entry // in file order (== encode order)
}

// Entry is one frame's metadata plus its location in the file. It
// deliberately omits the payload: an index over a 4 GB recording must
// stay cheap. ReadPayload re-reads the bytes for a single entry when a
// caller actually needs them.
type Entry struct {
	// TrackName and Kind identify which lane this frame belongs to
	// (e.g. "screen"/"video", "reasoning"/"tokens").
	TrackName string
	Kind      string

	// CapturedAtMicros is the recorder's wall-clock capture time in
	// microseconds. This is the master clock the timeline sorts on —
	// PTSMicro is per-track presentation time and is NOT comparable
	// across tracks.
	CapturedAtMicros int64

	// PTSMicros is the per-track presentation timestamp, carried
	// through for callers that want intra-track pacing.
	PTSMicros uint64

	// Seq is the monotonic per-publisher counter from the AU.
	Seq uint32

	// Keyframe marks an independently-decodable / self-contained AU.
	Keyframe bool

	// Discontinuity flags a decoder-reset hint (rare; surfaced for
	// completeness).
	Discontinuity bool

	// PayloadLen is the size of the frame's payload in bytes. Lets a
	// CLI render "<N bytes>" for opaque media without reading it.
	PayloadLen int

	// Offset is the absolute byte offset in the file of this frame's
	// 4-byte total-length prefix — i.e. the position decodeFrame must
	// be at to read this frame. ReadPayload seeks here.
	Offset int64
}

// Track names a lane present in a recording: its name and kind.
type Track struct {
	Name string
	Kind string
}

// BuildTimeline opens path, validates the header, and walks every
// frame once to build the index. It returns an error for a corrupt
// header or a truncated frame; a clean EOF marker ends the scan
// normally. The file is closed before returning — Timeline reopens it
// briefly inside ReadPayload, so there is no long-lived descriptor to
// leak.
func BuildTimeline(path string) (*Timeline, error) {
	f, err := os.Open(path) // #nosec G304 -- path is a caller-supplied recording file, not untrusted input
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// We can't reuse the Replayer here because we need the byte offset
	// of each frame, which the bufio-wrapped reader hides. Instead we
	// track position explicitly with a counting reader so Offset is
	// exact regardless of buffering.
	cr := &countingReader{r: f}
	if err := readHeader(cr); err != nil {
		return nil, err
	}

	tl := &Timeline{path: path}
	for {
		// Snapshot the offset BEFORE decoding so it points at the
		// frame's length prefix, matching what decodeFrame expects to
		// see when ReadPayload seeks back here.
		off := cr.n
		f, err := decodeFrame(cr)
		if err != nil {
			// io.EOF is the normal terminator (EOF marker or file end).
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		tl.entries = append(tl.entries, Entry{
			TrackName:        f.TrackName,
			Kind:             f.Kind,
			CapturedAtMicros: f.CapturedAtMicros,
			PTSMicros:        f.PTSMicros,
			Seq:              f.Seq,
			Keyframe:         f.Keyframe,
			Discontinuity:    f.Discontinuity,
			PayloadLen:       len(f.Payload),
			Offset:           off,
		})
	}
	return tl, nil
}

// Tracks returns the distinct (name, kind) pairs present in the
// recording, in first-appearance order. Stable order makes CLI output
// and tests deterministic.
func (tl *Timeline) Tracks() []Track {
	seen := make(map[Track]bool)
	var out []Track
	for _, e := range tl.entries {
		t := Track{Name: e.TrackName, Kind: e.Kind}
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return out
}

// Len reports the number of indexed frames.
func (tl *Timeline) Len() int { return len(tl.entries) }

// Frames returns all entries merged in capture-time order across every
// track. Ties on CapturedAtMicros are broken by track name then Seq so
// the order is fully deterministic — important for tests and for
// reproducible eval runs. The returned slice is a fresh copy; callers
// may sort or filter it freely.
func (tl *Timeline) Frames() []Entry {
	out := make([]Entry, len(tl.entries))
	copy(out, tl.entries)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].CapturedAtMicros != out[j].CapturedAtMicros {
			return out[i].CapturedAtMicros < out[j].CapturedAtMicros
		}
		if out[i].TrackName != out[j].TrackName {
			return out[i].TrackName < out[j].TrackName
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}

// SeekSnapshot reconstructs the cross-track state at capture-time
// micros: for each track it returns the latest frame whose
// CapturedAtMicros is <= micros (the most recent frame at or before
// that moment). A track with no frame at or before t is absent from
// the result — there was nothing on that lane yet.
//
// This is the core observability primitive: pass the capture time of a
// failure and read back the screen frame the agent saw, the last token
// batch it had emitted, and the last tool call it had issued.
func (tl *Timeline) SeekSnapshot(micros int64) map[Track]Entry {
	out := make(map[Track]Entry)
	for _, e := range tl.entries {
		if e.CapturedAtMicros > micros {
			continue
		}
		t := Track{Name: e.TrackName, Kind: e.Kind}
		// Keep the latest qualifying frame per track. Compare on
		// capture time, then Seq, to stay deterministic when two
		// frames on one track share a capture timestamp.
		if prev, ok := out[t]; !ok ||
			e.CapturedAtMicros > prev.CapturedAtMicros ||
			(e.CapturedAtMicros == prev.CapturedAtMicros && e.Seq > prev.Seq) {
			out[t] = e
		}
	}
	return out
}

// ReadPayload re-opens the file and reads back the full frame at the
// given entry's offset, returning it as an AccessUnit. Payloads aren't
// cached in the index, so this is the path a viewer uses to fetch the
// bytes for a specific frame (a screen image to decode, a token batch
// to display) without holding the whole recording in RAM.
func (tl *Timeline) ReadPayload(e Entry) (pubsub.AccessUnit, error) {
	f, err := os.Open(tl.path)
	if err != nil {
		return pubsub.AccessUnit{}, err
	}
	defer f.Close()
	if _, err := f.Seek(e.Offset, io.SeekStart); err != nil {
		return pubsub.AccessUnit{}, err
	}
	fr, err := decodeFrame(f)
	if err != nil {
		return pubsub.AccessUnit{}, err
	}
	return pubsub.AccessUnit{
		Bytes:         fr.Payload,
		Keyframe:      fr.Keyframe,
		PTSMicro:      fr.PTSMicros,
		Seq:           fr.Seq,
		Discontinuity: fr.Discontinuity,
	}, nil
}

// countingReader wraps an io.Reader and tracks the total number of
// bytes consumed. We use it to recover each frame's byte offset during
// the index pass; decodeFrame reads exactly one frame's worth, so the
// counter lands on the next frame's length prefix after each call.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
