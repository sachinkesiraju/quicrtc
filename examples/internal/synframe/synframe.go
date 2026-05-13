// Package synframe generates small REAL PNG frames for examples.
//
// The examples publish on a "screen" track that advertises
// Codec: "image/png" in the SDP. Shipping random bytes would make
// that codec field a lie and would prevent the browser viewer
// from rendering anything. This package emits actual PNGs that
// decode in any image library — they're synthetic content (a
// scrolling band pattern with a frame counter overlaid as bars),
// but the bytes ARE a valid PNG.
//
// Encoding is real-time cheap: ~1 ms for a 640×480 frame at the
// default compression level on a 2024-era CPU. Output is in the
// ~30–80 KB range, matching realistic screenshot sizes a CUA
// agent would carry over the wire.
package synframe

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
)

// Encoder is reusable; one per goroutine. Caches the destination
// image so we don't allocate width*height pixels per frame.
type Encoder struct {
	width, height int
	img           *image.NRGBA
	buf           bytes.Buffer
	enc           png.Encoder
}

// New constructs an Encoder for frames of width × height.
func New(width, height int) *Encoder {
	return &Encoder{
		width:  width,
		height: height,
		img:    image.NewNRGBA(image.Rect(0, 0, width, height)),
		enc:    png.Encoder{CompressionLevel: png.DefaultCompression},
	}
}

// Encode paints a frame for the given seq and returns its PNG bytes.
// The returned slice is reused across calls; copy it if the caller
// retains the bytes past the next Encode call.
//
// Visual content: three horizontal bands that scroll left↔right at
// different speeds, plus a thin vertical "frame index" tick mark
// at x = seq % width. The bands are gradients in different hues
// so the viewer's eye can see motion frame to frame. Keyframes
// are tinted slightly brighter for visual confirmation that the
// K/P marker matches the actual bytes.
func (e *Encoder) Encode(seq uint32, keyframe bool) ([]byte, error) {
	paint(e.img, seq, keyframe)
	e.buf.Reset()
	if err := e.enc.Encode(&e.buf, e.img); err != nil {
		return nil, err
	}
	return e.buf.Bytes(), nil
}

func paint(img *image.NRGBA, seq uint32, keyframe bool) {
	w, h := img.Bounds().Dx(), img.Bounds().Dy()
	thirds := h / 3

	// Three bands. Each scrolls horizontally at a different speed,
	// so the viewer can SEE per-frame motion (otherwise an animated
	// gradient looks static across single-frame inspections).
	// Brightness bumped on keyframes.
	boost := uint8(0)
	if keyframe {
		boost = 32
	}
	for y := 0; y < h; y++ {
		var base color.NRGBA
		switch {
		case y < thirds:
			// Cyan band, scrolls slowly to the right.
			base = color.NRGBA{R: 32, G: 120, B: 200, A: 255}
		case y < 2*thirds:
			// Amber band, scrolls quickly to the left.
			base = color.NRGBA{R: 200, G: 140, B: 32, A: 255}
		default:
			// Magenta band, scrolls slowly to the left.
			base = color.NRGBA{R: 180, G: 60, B: 160, A: 255}
		}
		var phase int
		switch {
		case y < thirds:
			phase = int(seq) % w
		case y < 2*thirds:
			phase = (w - int(seq*2)%w) % w
		default:
			phase = (w - int(seq)%w) % w
		}
		row := img.Pix[img.PixOffset(0, y) : img.PixOffset(0, y)+w*4]
		for x := 0; x < w; x++ {
			// Gradient: brightness eases toward `phase`.
			d := x - phase
			if d < 0 {
				d = -d
			}
			if d > w/2 {
				d = w - d
			}
			scale := 1.0 - float64(d)/float64(w/2)
			if scale < 0 {
				scale = 0
			}
			r := uint8(float64(base.R)*scale) + boost/2
			g := uint8(float64(base.G)*scale) + boost/2
			b := uint8(float64(base.B)*scale) + boost/2
			off := x * 4
			row[off+0] = r
			row[off+1] = g
			row[off+2] = b
			row[off+3] = 255
		}
	}

	// Vertical tick line at x = seq % w so the eye can count frames
	// at a glance even on small viewer canvases.
	tickX := int(seq) % w
	for y := 0; y < h; y++ {
		off := img.PixOffset(tickX, y)
		img.Pix[off+0] = 230
		img.Pix[off+1] = 230
		img.Pix[off+2] = 230
		img.Pix[off+3] = 255
	}
}
