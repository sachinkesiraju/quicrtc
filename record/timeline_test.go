package record

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
)

// writeOrdered records a sequence of AUs to a fresh file, sleeping
// between each Write so the recorder's internal time.Now() stamp gives
// every frame a distinct, monotonically increasing capture time across
// tracks. Returns the file path. The small sleeps make the test a few
// dozen ms slow but guarantee deterministic ordering without reaching
// into the encoder.
func writeOrdered(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "session.qrtc")

	rec, err := NewFileRecorder(path)
	if err != nil {
		t.Fatal(err)
	}

	// A 3-track session: screen video, reasoning tokens, tool calls.
	// Written interleaved so the file order is NOT capture-time order —
	// that's the whole point of the merge.
	type write struct {
		track, kind string
		au          pubsub.AccessUnit
	}
	plan := []write{
		{"screen", "video", pubsub.AccessUnit{Bytes: make([]byte, 4096), Keyframe: true, Seq: 1, PTSMicro: 0}},
		{"reasoning", "tokens", pubsub.AccessUnit{Bytes: []byte("thinking about the page"), Keyframe: true, Seq: 1, PTSMicro: 0}},
		{"actions", "toolcalls", pubsub.AccessUnit{Bytes: []byte(`{"fn":"click","x":42}`), Keyframe: true, Seq: 1, PTSMicro: 0}},
		{"screen", "video", pubsub.AccessUnit{Bytes: make([]byte, 2048), Keyframe: false, Seq: 2, PTSMicro: 33000}},
		{"reasoning", "tokens", pubsub.AccessUnit{Bytes: []byte("done clicking"), Keyframe: false, Seq: 2, PTSMicro: 33000}},
	}

	for _, w := range plan {
		if err := rec.Write(w.track, w.kind, w.au); err != nil {
			t.Fatal(err)
		}
		// Distinct capture timestamps. The writer goroutine stamps
		// time.Now() when it drains; sleeping here spaces those out.
		time.Sleep(5 * time.Millisecond)
	}
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestTimelineIndexContents(t *testing.T) {
	path := writeOrdered(t)

	tl, err := BuildTimeline(path)
	if err != nil {
		t.Fatal(err)
	}

	if tl.Len() != 5 {
		t.Fatalf("indexed %d frames, want 5", tl.Len())
	}

	// Tracks: 3 distinct (name,kind) pairs in first-appearance order.
	tracks := tl.Tracks()
	want := []Track{
		{Name: "screen", Kind: "video"},
		{Name: "reasoning", Kind: "tokens"},
		{Name: "actions", Kind: "toolcalls"},
	}
	if len(tracks) != len(want) {
		t.Fatalf("got %d tracks, want %d: %+v", len(tracks), len(want), tracks)
	}
	for i := range want {
		if tracks[i] != want[i] {
			t.Fatalf("track %d = %+v, want %+v", i, tracks[i], want[i])
		}
	}

	// Spot-check that per-frame metadata survived the round trip and
	// that distinct, ascending offsets were recorded.
	frames := tl.Frames()
	var lastOff int64 = -1
	for i, e := range frames {
		if e.Offset <= lastOff {
			t.Fatalf("frame %d offset %d not strictly increasing (prev %d)", i, e.Offset, lastOff)
		}
		lastOff = e.Offset
		if e.PayloadLen <= 0 {
			t.Fatalf("frame %d has non-positive payload len %d", i, e.PayloadLen)
		}
	}
}

func TestTimelineMergedCaptureOrder(t *testing.T) {
	path := writeOrdered(t)

	tl, err := BuildTimeline(path)
	if err != nil {
		t.Fatal(err)
	}

	frames := tl.Frames()
	// Frames must come back in non-decreasing capture-time order even
	// though they were interleaved across tracks on disk.
	for i := 1; i < len(frames); i++ {
		if frames[i].CapturedAtMicros < frames[i-1].CapturedAtMicros {
			t.Fatalf("frame %d capture time %d < prev %d — not merged in order",
				i, frames[i].CapturedAtMicros, frames[i-1].CapturedAtMicros)
		}
	}

	// With 5ms spacing the recorded sequence of (track,seq) should match
	// the write plan exactly.
	type ts struct {
		track string
		seq   uint32
	}
	gotOrder := make([]ts, len(frames))
	for i, e := range frames {
		gotOrder[i] = ts{e.TrackName, e.Seq}
	}
	wantOrder := []ts{
		{"screen", 1}, {"reasoning", 1}, {"actions", 1},
		{"screen", 2}, {"reasoning", 2},
	}
	for i := range wantOrder {
		if gotOrder[i] != wantOrder[i] {
			t.Fatalf("merged order[%d] = %+v, want %+v (full: %+v)",
				i, gotOrder[i], wantOrder[i], gotOrder)
		}
	}
}

func TestTimelineSeekSnapshot(t *testing.T) {
	path := writeOrdered(t)

	tl, err := BuildTimeline(path)
	if err != nil {
		t.Fatal(err)
	}

	frames := tl.Frames()
	origin := frames[0].CapturedAtMicros

	// Before anything was recorded: empty snapshot.
	if snap := tl.SeekSnapshot(origin - 1); len(snap) != 0 {
		t.Fatalf("snapshot before origin should be empty, got %d entries", len(snap))
	}

	// At the third frame's capture time, exactly the first frame of each
	// of the three tracks should be visible (actions, reasoning #1,
	// screen #1) — the second screen/reasoning frames are still future.
	thirdT := frames[2].CapturedAtMicros
	snap := tl.SeekSnapshot(thirdT)
	if len(snap) != 3 {
		t.Fatalf("snapshot at frame[2] has %d tracks, want 3: %+v", len(snap), snap)
	}
	if e, ok := snap[Track{"screen", "video"}]; !ok || e.Seq != 1 {
		t.Fatalf("screen at frame[2] = %+v (ok=%v), want seq 1", e, ok)
	}
	if e, ok := snap[Track{"actions", "toolcalls"}]; !ok || e.Seq != 1 {
		t.Fatalf("actions at frame[2] = %+v (ok=%v), want seq 1", e, ok)
	}

	// At the very end: the latest frame of each track. screen and
	// reasoning advance to seq 2; actions stays at its only frame (1).
	lastT := frames[len(frames)-1].CapturedAtMicros
	snap = tl.SeekSnapshot(lastT)
	if e, ok := snap[Track{"screen", "video"}]; !ok || e.Seq != 2 {
		t.Fatalf("screen at end = %+v (ok=%v), want seq 2", e, ok)
	}
	if e, ok := snap[Track{"reasoning", "tokens"}]; !ok || e.Seq != 2 {
		t.Fatalf("reasoning at end = %+v (ok=%v), want seq 2", e, ok)
	}
	if e, ok := snap[Track{"actions", "toolcalls"}]; !ok || e.Seq != 1 {
		t.Fatalf("actions at end = %+v (ok=%v), want seq 1 (only one written)", e, ok)
	}

	// Snapshot far in the future is identical to the end snapshot.
	future := tl.SeekSnapshot(lastT + 1_000_000)
	if len(future) != len(snap) {
		t.Fatalf("future snapshot size %d != end snapshot size %d", len(future), len(snap))
	}
}

func TestTimelineReadPayload(t *testing.T) {
	path := writeOrdered(t)

	tl, err := BuildTimeline(path)
	if err != nil {
		t.Fatal(err)
	}

	// Find the first tool-call entry and confirm its bytes read back
	// intact from the recorded offset.
	var found bool
	for _, e := range tl.Frames() {
		if e.Kind == "toolcalls" {
			au, err := tl.ReadPayload(e)
			if err != nil {
				t.Fatal(err)
			}
			if string(au.Bytes) != `{"fn":"click","x":42}` {
				t.Fatalf("tool-call payload = %q, want the click JSON", au.Bytes)
			}
			if au.Seq != e.Seq {
				t.Fatalf("ReadPayload seq %d != index seq %d", au.Seq, e.Seq)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("no toolcalls frame found in timeline")
	}
}
