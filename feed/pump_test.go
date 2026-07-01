package feed

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// fakeStream is an in-memory SendStream that records writes,
// cancellation, and close. Its writeFunc lets tests inject failures.
type fakeStream struct {
	id        int
	mu        sync.Mutex
	buf       bytes.Buffer
	cancelled bool
	closed    bool
	writeErr  error
}

func (s *fakeStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelled || s.closed {
		return 0, io.ErrClosedPipe
	}
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	return s.buf.Write(p)
}

func (s *fakeStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *fakeStream) SetWriteDeadline(t time.Time) error { return nil }

func (s *fakeStream) CancelWrite(code uint32) {
	s.mu.Lock()
	s.cancelled = true
	s.mu.Unlock()
}

func (s *fakeStream) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.buf.Bytes()...)
}

func (s *fakeStream) Cancelled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancelled
}

// fakeOpener returns a fresh fakeStream on each call. If responses is
// non-empty, it consumes one entry per call (front of queue) and
// either returns its err or a fresh stream. When responses is
// empty, every call returns a fresh stream successfully.
type fakeOpener struct {
	mu        sync.Mutex
	streams   []*fakeStream
	responses []openResponse
}

type openResponse struct {
	err error // if non-nil, return this error and don't create a stream
}

func (o *fakeOpener) OpenSendStream(ctx context.Context) (SendStream, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.responses) > 0 {
		r := o.responses[0]
		o.responses = o.responses[1:]
		if r.err != nil {
			return nil, r.err
		}
	}
	s := &fakeStream{id: len(o.streams) + 1}
	o.streams = append(o.streams, s)
	return s, nil
}

func (o *fakeOpener) Streams() []*fakeStream {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]*fakeStream(nil), o.streams...)
}

func parseAllFeedFrames(t *testing.T, b []byte) []wire.FeedFrame {
	t.Helper()
	r := bytes.NewReader(b)
	var out []wire.FeedFrame
	for r.Len() > 0 {
		f, err := wire.ReadFeedFrame(r)
		if err != nil {
			t.Fatalf("read feed frame: %v (after %d frames)", err, len(out))
		}
		out = append(out, f)
	}
	return out
}

func keyframe(seq uint32) pubsub.AccessUnit {
	return pubsub.AccessUnit{Bytes: []byte{byte(seq)}, Keyframe: true, Seq: seq, PTSMicro: uint64(seq) * 33333}
}

func pframe(seq uint32) pubsub.AccessUnit {
	return pubsub.AccessUnit{Bytes: []byte{byte(seq)}, Keyframe: false, Seq: seq, PTSMicro: uint64(seq) * 33333}
}

func TestStreamPerGOPLifecycle(t *testing.T) {
	b := pubsub.NewBroadcaster(16)
	r := b.Subscribe()
	op := &fakeOpener{}
	p := New(op, Config{})

	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background(), r) }()

	// First GOP: keyframe + 2 P-frames.
	b.Publish(keyframe(0))
	b.Publish(pframe(1))
	b.Publish(pframe(2))
	// Second GOP: new keyframe + 1 P-frame. This MUST reset stream #1
	// and open stream #2.
	b.Publish(keyframe(3))
	b.Publish(pframe(4))

	// Allow the pump to drain.
	time.Sleep(50 * time.Millisecond)
	b.Unsubscribe(r)
	if err := <-done; err != nil {
		t.Fatalf("pump: %v", err)
	}

	streams := op.Streams()
	if len(streams) != 2 {
		t.Fatalf("expected 2 streams (one per keyframe), got %d", len(streams))
	}
	if !streams[0].Cancelled() {
		t.Fatal("first stream should be CancelWrite'd when second keyframe arrived")
	}
	// Second stream is closed via the deferred Close() at end of Run.
	frames1 := parseAllFeedFrames(t, streams[0].Bytes())
	frames2 := parseAllFeedFrames(t, streams[1].Bytes())
	if len(frames1) != 3 {
		t.Fatalf("stream 1 should have keyframe+2pframes, got %d frames", len(frames1))
	}
	if !frames1[0].IsKeyframe() || frames1[0].Seq != 0 {
		t.Fatalf("stream 1 frame 0 should be keyframe seq=0, got %+v", frames1[0])
	}
	if frames1[1].Seq != 1 || frames1[2].Seq != 2 {
		t.Fatalf("stream 1 P-frame ordering wrong: %d %d", frames1[1].Seq, frames1[2].Seq)
	}
	if len(frames2) != 2 || !frames2[0].IsKeyframe() || frames2[0].Seq != 3 {
		t.Fatalf("stream 2 should start with keyframe seq=3, got %+v", frames2)
	}
	if frames2[1].Seq != 4 {
		t.Fatalf("stream 2 P-frame should have seq=4, got %d", frames2[1].Seq)
	}
}

func TestKeyframeOpenFailureRecoversOnNextKeyframe(t *testing.T) {
	b := pubsub.NewBroadcaster(16)
	r := b.Subscribe()
	// Pre-program the opener: first call fails, second succeeds.
	op := &fakeOpener{responses: []openResponse{{err: errors.New("open fail")}}}
	p := New(op, Config{})

	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background(), r) }()

	b.Publish(keyframe(0)) // open fails → current=nil, RequestKeyframe set
	b.Publish(pframe(1))   // broadcaster skips (NeedsKeyframe is true)
	b.Publish(pframe(2))   // broadcaster skips
	b.Publish(keyframe(3)) // open succeeds → stream #1 with this keyframe

	time.Sleep(50 * time.Millisecond)
	b.Unsubscribe(r)
	if err := <-done; err != nil {
		t.Fatalf("pump: %v", err)
	}
	streams := op.Streams()
	if len(streams) != 1 {
		t.Fatalf("want 1 successful stream after recovery, got %d", len(streams))
	}
	frames := parseAllFeedFrames(t, streams[0].Bytes())
	if len(frames) != 1 || !frames[0].IsKeyframe() || frames[0].Seq != 3 {
		t.Fatalf("recovery stream should contain keyframe seq=3 only, got %+v", frames)
	}
}

func TestWriteFailureCancelsStreamAndDemandsKeyframe(t *testing.T) {
	b := pubsub.NewBroadcaster(16)
	r := b.Subscribe()
	op := &fakeOpener{}
	p := New(op, Config{})

	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background(), r) }()

	b.Publish(keyframe(0)) // opens stream #1, writes keyframe ok
	// Force write to fail on stream #1 mid-GOP.
	for {
		op.mu.Lock()
		ready := len(op.streams) > 0
		op.mu.Unlock()
		if ready {
			break
		}
		time.Sleep(time.Millisecond)
	}
	op.streams[0].mu.Lock()
	op.streams[0].writeErr = errors.New("conn drop")
	op.streams[0].mu.Unlock()

	b.Publish(pframe(1))   // write fails, current=nil, RequestKeyframe
	b.Publish(pframe(2))   // skipped at fanout
	b.Publish(keyframe(3)) // opens stream #2

	time.Sleep(50 * time.Millisecond)
	b.Unsubscribe(r)
	if err := <-done; err != nil {
		t.Fatalf("pump: %v", err)
	}

	streams := op.Streams()
	if len(streams) != 2 {
		t.Fatalf("want 2 streams (original + recovery), got %d", len(streams))
	}
	if !streams[0].Cancelled() {
		t.Fatal("first stream should be cancelled after write failure")
	}
	frames := parseAllFeedFrames(t, streams[1].Bytes())
	if len(frames) != 1 || frames[0].Seq != 3 {
		t.Fatalf("recovery stream frames wrong: %+v", frames)
	}
}

func TestContextCancelStopsPump(t *testing.T) {
	b := pubsub.NewBroadcaster(16)
	r := b.Subscribe()
	op := &fakeOpener{}
	p := New(op, Config{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, r) }()

	b.Publish(keyframe(0))
	time.Sleep(20 * time.Millisecond)
	cancel()
	// Publish more so the pump's loop has a chance to observe ctx.Err.
	b.Publish(pframe(1))

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pump did not stop on context cancel")
	}
	b.Unsubscribe(r)
}

func TestOnWriteBytesCallback(t *testing.T) {
	b := pubsub.NewBroadcaster(16)
	r := b.Subscribe()
	op := &fakeOpener{}
	var totalBytes int
	var cbMu sync.Mutex
	p := New(op, Config{OnWriteBytes: func(n int) {
		cbMu.Lock()
		totalBytes += n
		cbMu.Unlock()
	}})

	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background(), r) }()

	payload := bytes.Repeat([]byte{0xab}, 100)
	b.Publish(pubsub.AccessUnit{Bytes: payload, Keyframe: true, Seq: 0})

	time.Sleep(20 * time.Millisecond)
	b.Unsubscribe(r)
	<-done

	cbMu.Lock()
	got := totalBytes
	cbMu.Unlock()
	want := wire.FeedHeaderLen + len(payload)
	if got != want {
		t.Fatalf("OnWriteBytes total: got %d want %d", got, want)
	}
}

// TestAllowLeadingPFramesWritesResumeContinuation: with the resume
// flag set, a receiver whose queue starts with mid-GOP P-frames (the
// server's resume splice) gets that P-run written on a fresh stream
// instead of stalling until the next keyframe — the subscriber
// declared LastSeenSeq, so it holds the GOP head and can decode them.
func TestAllowLeadingPFramesWritesResumeContinuation(t *testing.T) {
	b := pubsub.NewBroadcaster(16)
	r := b.Subscribe()
	// Simulate the resume splice: mid-GOP P-frames already queued in
	// the receiver before the pump starts.
	r.SpliceFront([]pubsub.AccessUnit{pframe(5), pframe(6)})
	op := &fakeOpener{}
	p := New(op, Config{AllowLeadingPFrames: true})

	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background(), r) }()

	// Live keyframe after the continuation: must open a new stream.
	b.Publish(keyframe(7))

	time.Sleep(50 * time.Millisecond)
	b.Unsubscribe(r)
	if err := <-done; err != nil {
		t.Fatalf("pump: %v", err)
	}

	streams := op.Streams()
	if len(streams) != 2 {
		t.Fatalf("want 2 streams (continuation + new GOP), got %d", len(streams))
	}
	cont := parseAllFeedFrames(t, streams[0].Bytes())
	if len(cont) != 2 || cont[0].IsKeyframe() || cont[0].Seq != 5 || cont[1].Seq != 6 {
		t.Fatalf("continuation stream should carry P5,P6, got %+v", cont)
	}
	if !streams[0].Cancelled() {
		t.Fatal("continuation stream should be RESET when the keyframe opened a new stream")
	}
	gop := parseAllFeedFrames(t, streams[1].Bytes())
	if len(gop) != 1 || !gop[0].IsKeyframe() || gop[0].Seq != 7 {
		t.Fatalf("new GOP stream should carry keyframe 7, got %+v", gop)
	}
}

// TestLeadingPFramesDroppedWithoutFlag pins the default behavior:
// P-frames before any keyframe are dropped (no decodable reference),
// and delivery starts at the first keyframe.
func TestLeadingPFramesDroppedWithoutFlag(t *testing.T) {
	b := pubsub.NewBroadcaster(16)
	r := b.Subscribe()
	r.SpliceFront([]pubsub.AccessUnit{pframe(5), pframe(6)})
	op := &fakeOpener{}
	p := New(op, Config{})

	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background(), r) }()

	b.Publish(keyframe(7))

	time.Sleep(50 * time.Millisecond)
	b.Unsubscribe(r)
	if err := <-done; err != nil {
		t.Fatalf("pump: %v", err)
	}

	streams := op.Streams()
	if len(streams) != 1 {
		t.Fatalf("want 1 stream (leading P-frames dropped), got %d", len(streams))
	}
	frames := parseAllFeedFrames(t, streams[0].Bytes())
	if len(frames) != 1 || !frames[0].IsKeyframe() || frames[0].Seq != 7 {
		t.Fatalf("stream should carry only keyframe 7, got %+v", frames)
	}
}
