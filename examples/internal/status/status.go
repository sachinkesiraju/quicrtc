// Package status is a tiny dependency-free helper used by every
// example to give the developer a live, in-place "is anything
// happening?" view on stderr plus a boxed end-of-run summary on
// Ctrl-C. Both behaviors are deliberately minimal: no third-party
// libraries, no terminal-attribute fanciness — just \r and ANSI
// erase, gated on isatty so file redirects stay clean.
//
// Typical usage from an example:
//
//	st := status.New("publisher")
//	defer st.Done(map[string]string{
//	    "uptime":   uptime.String(),
//	    "AU sent":  fmt.Sprintf("%d", st.Get("frames")),
//	    "bytes":    humanBytes(st.Get("bytes")),
//	})
//	// in the hot loop:
//	st.Inc("frames", 1)
//	st.Inc("bytes", int64(len(au.Bytes)))
//	st.Render(func(s *status.Line) {
//	    s.Field("frames", "%d", st.Get("frames"))
//	    s.Field("bytes", "%s", humanBytes(st.Get("bytes")))
//	})
package status

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Status is the live counter set + status-line renderer.
type Status struct {
	tag    string
	out    io.Writer
	istty  bool
	start  time.Time
	render atomic.Pointer[string]

	mu       sync.Mutex
	counters map[string]*int64

	stopRender chan struct{}
	rendered   atomic.Bool
}

// New returns a Status with stderr as the output sink. tag is the
// short label printed at the start of each status line, e.g. "publisher".
func New(tag string) *Status {
	st := &Status{
		tag:      tag,
		out:      os.Stderr,
		istty:    isTerminal(os.Stderr),
		start:    time.Now(),
		counters: make(map[string]*int64),
	}
	return st
}

// counter returns (creating if needed) the *int64 for name.
func (s *Status) counter(name string) *int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.counters[name]
	if !ok {
		var v int64
		c = &v
		s.counters[name] = c
	}
	return c
}

// Inc adds n to counter name (creating it if missing).
func (s *Status) Inc(name string, n int64) {
	atomic.AddInt64(s.counter(name), n)
}

// Set sets counter name to v.
func (s *Status) Set(name string, v int64) {
	atomic.StoreInt64(s.counter(name), v)
}

// Get reads counter name (returns 0 if missing).
func (s *Status) Get(name string) int64 {
	return atomic.LoadInt64(s.counter(name))
}

// Render prints one status line. Compose by calling Field on the
// passed Line — the helper takes care of separators, erase, and the
// leading "[tag]". Safe to call concurrently.
//
// On non-tty sinks, suppressed entirely (avoids polluting log files).
func (s *Status) Render(compose func(*Line)) {
	if !s.istty {
		return
	}
	l := &Line{tag: s.tag}
	compose(l)
	// \r + ANSI EL (erase line) + line content. No trailing newline —
	// the next Render overwrites in place; Done prints \n before the
	// summary box.
	fmt.Fprintf(s.out, "\r\x1b[2K%s", l.String())
	s.rendered.Store(true)
}

// Tick starts a goroutine that calls compose() every d on a ticker
// until ctx-style stop is signaled via the returned cancel func.
// Convenient for "just keep the status line fresh while the main
// loop does work."
func (s *Status) Tick(d time.Duration, compose func(*Line)) (stop func()) {
	stopCh := make(chan struct{})
	go func() {
		t := time.NewTicker(d)
		defer t.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-t.C:
				s.Render(compose)
			}
		}
	}()
	return func() { close(stopCh) }
}

// Done prints a final \n (to land below the in-place status line) and
// a boxed summary. fields is rendered in insertion order — pass a
// slice of {Key,Value} so the caller controls layout.
func (s *Status) Done(headline string, fields []Field) {
	if s.rendered.Load() && s.istty {
		fmt.Fprint(s.out, "\n")
	}
	if headline != "" {
		fmt.Fprintf(s.out, "\n%s\n", headline)
	}
	fmt.Fprintln(s.out, "── summary ──────────────────────────")
	keyW := 0
	for _, f := range fields {
		if len(f.Key) > keyW {
			keyW = len(f.Key)
		}
	}
	for _, f := range fields {
		fmt.Fprintf(s.out, "  %-*s  %s\n", keyW, f.Key, f.Value)
	}
}

// Uptime returns wall-clock since New was called.
func (s *Status) Uptime() time.Duration { return time.Since(s.start) }

// Field is one labeled row in the summary box.
type Field struct{ Key, Value string }

// Line is a one-render builder.
type Line struct {
	tag    string
	fields []string
}

// Field appends "name=value" with formatted value.
func (l *Line) Field(name, format string, args ...any) {
	l.fields = append(l.fields, fmt.Sprintf("%s=%s", name, fmt.Sprintf(format, args...)))
}

// Raw appends a fully-formed segment (no name=value framing).
func (l *Line) Raw(format string, args ...any) {
	l.fields = append(l.fields, fmt.Sprintf(format, args...))
}

func (l *Line) String() string {
	return fmt.Sprintf("[%s] %s", l.tag, strings.Join(l.fields, " │ "))
}

// HumanBytes formats b as e.g. "1.2 MB" / "418 KB" / "12 B".
func HumanBytes(b int64) string {
	const k = 1024
	switch {
	case b < k:
		return fmt.Sprintf("%d B", b)
	case b < k*k:
		return fmt.Sprintf("%.1f KB", float64(b)/k)
	case b < k*k*k:
		return fmt.Sprintf("%.1f MB", float64(b)/(k*k))
	default:
		return fmt.Sprintf("%.1f GB", float64(b)/(k*k*k))
	}
}

// isTerminal returns true if w is an os.File backed by a character
// device (a TTY). Pipes, files, and non-*os.File writers all yield
// false — which is the right default for the status-line use case
// (don't write \r-overwriting lines into files or piped output).
func isTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

// HumanCount formats n with thousands separators when large.
func HumanCount(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	s := fmt.Sprintf("%d", n)
	var out strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out.WriteByte(',')
		}
		out.WriteRune(c)
	}
	return out.String()
}
