package record

import (
	"bufio"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
)

// Recorder writes recorded AUs to a backing store. Write is called
// from the broadcaster interceptor hot path and MUST NOT block: an
// implementation that can't accept an AU immediately should drop and
// increment its drop counter rather than synchronously do I/O.
type Recorder interface {
	// Write records one AU for the given track. The bytes slice
	// referenced by au.Bytes is copied internally before the call
	// returns — callers retain ownership and may reuse the underlying
	// buffer.
	Write(trackName, kind string, au pubsub.AccessUnit) error

	// Close flushes any buffered records and releases backend
	// resources. Idempotent.
	Close() error

	// Dropped returns the count of AUs dropped because the recorder's
	// internal channel was full. Useful for observability: when this
	// is non-zero, the recording is incomplete for that track.
	Dropped() uint64
}

// DefaultChannelDepth is the buffered channel size used by recorders.
// Sized to absorb ~5 seconds of typical video (30 fps * 5s = 150) plus
// burst headroom. Tune via WithChannelDepth if your workload differs.
const DefaultChannelDepth = 1024

// fileRecorder implements Recorder against a local append-only file.
// AUs queue on a buffered channel; a single writer goroutine drains
// them so the broadcaster hot path never blocks on disk I/O.
//
// closeMu is an RWMutex so concurrent Writes share the read lock
// (cheap) while Close takes the write lock exclusively around
// `close(ch)`. Without the read lock, a Write racing Close would
// panic with "send on closed channel" because select+default only
// guards a full channel, not a closed one.
type fileRecorder struct {
	ch       chan recordedAU
	f        *os.File
	buf      *bufio.Writer
	wg       sync.WaitGroup
	dropped  atomic.Uint64
	closeMu  sync.RWMutex
	closed   bool
	closeErr error
}

type recordedAU struct {
	track string
	kind  string
	au    pubsub.AccessUnit
	bytes []byte // owned copy; safe to retain
}

// NewFileRecorder opens path for append-only writing and starts the
// background writer goroutine. Uses DefaultChannelDepth.
func NewFileRecorder(path string) (Recorder, error) {
	return NewFileRecorderDepth(path, DefaultChannelDepth)
}

// NewFileRecorderDepth is NewFileRecorder with an explicit channel
// buffer size. Larger absorbs bigger bursts at the cost of more
// queued memory.
func NewFileRecorderDepth(path string, depth int) (Recorder, error) {
	if depth <= 0 {
		depth = DefaultChannelDepth
	}
	// 0600: a session recording may hold sensitive agent data, so keep it
	// owner-only. G304 here is a caller-supplied output path.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) // #nosec G304
	if err != nil {
		return nil, err
	}
	buf := bufio.NewWriter(f)
	if err := writeHeader(buf); err != nil {
		_ = f.Close()
		return nil, err
	}
	r := &fileRecorder{
		ch:  make(chan recordedAU, depth),
		f:   f,
		buf: buf,
	}
	r.wg.Add(1)
	go r.run()
	return r, nil
}

// Write enqueues the AU for asynchronous writing. Returns nil even
// when the queue is full or the recorder has been closed; overflow
// and post-close attempts are counted via Dropped() rather than
// surfaced as an error, so a recording lag never breaks the
// publisher.
func (r *fileRecorder) Write(trackName, kind string, au pubsub.AccessUnit) error {
	// Copy the bytes. The broadcaster contract is "caller may reuse
	// au.Bytes after Publish returns," so retaining the slice would
	// race with subsequent publishes that overwrite the backing array.
	cp := make([]byte, len(au.Bytes))
	copy(cp, au.Bytes)
	rec := recordedAU{track: trackName, kind: kind, au: au, bytes: cp}
	// Hold the read lock across the send so Close cannot close r.ch
	// while we're mid-send. RLock is fast and uncontended on the hot
	// path (only Close ever takes the write lock).
	r.closeMu.RLock()
	defer r.closeMu.RUnlock()
	if r.closed {
		r.dropped.Add(1)
		return nil
	}
	select {
	case r.ch <- rec:
		return nil
	default:
		r.dropped.Add(1)
		return nil
	}
}

func (r *fileRecorder) Dropped() uint64 {
	return r.dropped.Load()
}

// Close drains the queue, flushes the buffer, writes the EOF marker,
// and closes the file. Safe to call multiple times. Takes the write
// lock exclusively to fence concurrent Writes from racing the
// channel close.
func (r *fileRecorder) Close() error {
	r.closeMu.Lock()
	if r.closed {
		r.closeMu.Unlock()
		return r.closeErr
	}
	r.closed = true
	close(r.ch)
	r.closeMu.Unlock()
	r.wg.Wait()
	if err := writeEOF(r.buf); err != nil {
		r.closeErr = err
	}
	if err := r.buf.Flush(); err != nil && r.closeErr == nil {
		r.closeErr = err
	}
	if err := r.f.Close(); err != nil && r.closeErr == nil {
		r.closeErr = err
	}
	return r.closeErr
}

func (r *fileRecorder) run() {
	defer r.wg.Done()
	for rec := range r.ch {
		f := frame{
			TrackName:        rec.track,
			Kind:             rec.kind,
			PTSMicros:        rec.au.PTSMicro,
			Seq:              rec.au.Seq,
			Keyframe:         rec.au.Keyframe,
			Discontinuity:    rec.au.Discontinuity,
			CapturedAtMicros: time.Now().UnixMicro(),
			Payload:          rec.bytes,
		}
		buf, err := encodeFrame(f)
		if err != nil {
			continue
		}
		if _, err := r.buf.Write(buf); err != nil {
			// The writer goroutine doesn't surface per-write errors to
			// the publisher; recording is best-effort. The next Close
			// flush will surface the underlying error if it persists.
			continue
		}
	}
}

// Compile-time assertion that fileRecorder satisfies Recorder.
var _ Recorder = (*fileRecorder)(nil)

// ErrClosed is returned by Write after Close has been called. Currently
// unused (Write never errors), reserved for future strict-mode behavior.
var ErrClosed = errors.New("record: recorder closed")
