package record

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
)

// Replayer reads a recorded session and re-publishes AUs into fresh
// broadcasters. Use it for evals (replay a known-failing session
// against a new model) and debugging (step through what the agent
// saw frame by frame).
type Replayer struct {
	f   *os.File
	br  *bufio.Reader
	pos int64
}

// Open opens path for replay. The caller must call Close when done.
func Open(path string) (*Replayer, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	br := bufio.NewReader(f)
	if err := readHeader(br); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Replayer{f: f, br: br}, nil
}

// Close releases the underlying file.
func (r *Replayer) Close() error {
	return r.f.Close()
}

// Frame is one decoded record from the file, surfaced to PublishInto
// callers that want a custom routing hook.
type Frame struct {
	TrackName        string
	Kind             string
	CapturedAtMicros int64
	AU               pubsub.AccessUnit
}

// Next reads the next recorded frame. Returns io.EOF when the EOF
// marker is reached or the file ends.
func (r *Replayer) Next() (Frame, error) {
	f, err := decodeFrame(r.br)
	if err != nil {
		return Frame{}, err
	}
	return Frame{
		TrackName:        f.TrackName,
		Kind:             f.Kind,
		CapturedAtMicros: f.CapturedAtMicros,
		AU: pubsub.AccessUnit{
			Bytes:         f.Payload,
			Keyframe:      f.Keyframe,
			PTSMicro:      f.PTSMicros,
			Seq:           f.Seq,
			Discontinuity: f.Discontinuity,
		},
	}, nil
}

// PublishInto reads every remaining frame and invokes the routing
// callback for each. The caller decides where to send the AU — for
// example, calling Broadcaster.Publish on the appropriate track.
//
// Speed > 0 preserves inter-frame timing scaled by 1/speed (1.0 = real
// time, 2.0 = 2x faster, 0.5 = half-speed). Speed <= 0 plays as fast
// as the callback returns.
func (r *Replayer) PublishInto(ctx context.Context, speed float64, route func(Frame) error) error {
	var firstCaptured int64
	startWall := time.Now()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		f, err := r.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if speed > 0 {
			if firstCaptured == 0 {
				firstCaptured = f.CapturedAtMicros
			}
			elapsedRecorded := time.Duration(f.CapturedAtMicros-firstCaptured) * time.Microsecond
			targetWall := startWall.Add(time.Duration(float64(elapsedRecorded) / speed))
			wait := time.Until(targetWall)
			if wait > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(wait):
				}
			}
		}
		if err := route(f); err != nil {
			return err
		}
	}
}
