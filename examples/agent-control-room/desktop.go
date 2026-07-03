// Desktop painter for the agent-desktop demo.
//
// Renders the agent's "cloud desktop" — a window manager with a
// terminal, a code editor, and (late in the task) a CI browser window
// — into an NRGBA image, then PNG-encodes it. The pixels are honest:
// text is drawn with a real bitmap font (font.go), the wallpaper
// carries film-grain noise so frames compress like real screenshots
// instead of collapsing to a few KB of flat color, and every frame
// differs (clock, cursor blink, spinner, grain) the way a live screen
// capture would.
package main

import (
	"fmt"
	"image"
	"image/color"
	"image/png"
	"io"
	"strings"
	"time"
)

const (
	deskW = 1280
	deskH = 800

	topBarH  = 30
	lineH    = 16
	titleH   = 26
	statusH  = 38
	statusY  = deskH - statusH - 14
	grainAmp = 11 // wallpaper noise amplitude; keeps PNG size screenshot-realistic
)

// Palette.
var (
	colTopBar    = color.NRGBA{16, 19, 26, 255}
	colTitleBar  = color.NRGBA{31, 36, 48, 255}
	colWinBody   = color.NRGBA{11, 14, 20, 255}
	colEditorBg  = color.NRGBA{14, 17, 24, 255}
	colGutterBg  = color.NRGBA{18, 22, 30, 255}
	colBorder    = color.NRGBA{54, 62, 80, 255}
	colText      = color.NRGBA{201, 209, 217, 255}
	colDim       = color.NRGBA{110, 120, 134, 255}
	colPrompt    = color.NRGBA{86, 182, 194, 255}
	colGreen     = color.NRGBA{87, 200, 122, 255}
	colRed       = color.NRGBA{235, 100, 92, 255}
	colYellow    = color.NRGBA{229, 192, 90, 255}
	colBlue      = color.NRGBA{97, 158, 239, 255}
	colPurple    = color.NRGBA{178, 132, 234, 255}
	colOrange    = color.NRGBA{214, 143, 82, 255}
	colStr       = color.NRGBA{146, 200, 116, 255}
	colComment   = color.NRGBA{92, 110, 98, 255}
	colHighlight = color.NRGBA{42, 52, 38, 255}
	colModified  = color.NRGBA{229, 192, 90, 255}
	colAccent    = color.NRGBA{90, 150, 230, 255}
)

// termLine is one rendered terminal row.
type termLine struct {
	text  string
	color color.NRGBA
}

// scene is the mutable desktop state the script drives and the
// painter reads. Guarded by the engine's mutex.
type scene struct {
	stepTitle string
	stepIdx   int

	term []termLine

	editorFile      string
	editorLines     []string
	editorHighlight map[int]bool // 0-based changed/focused lines
	editorModified  bool

	ciVisible bool
	ciPassing bool

	prURL string

	// clicks are transient viewer-click ripples (interactive action
	// lane). Pruned by the painter as they age out.
	clicks []clickMark
}

// clickMark is one viewer click, aged by painter tick.
type clickMark struct {
	x, y int
	born uint32
}

const termMaxLines = 24

func (s *scene) appendTerm(l termLine) {
	s.term = append(s.term, l)
	if len(s.term) > termMaxLines {
		s.term = s.term[len(s.term)-termMaxLines:]
	}
}

// desktopPainter owns the reusable frame buffer + PNG encoder.
type desktopPainter struct {
	img  *image.NRGBA
	enc  png.Encoder
	buf  writerBuf
	tick uint32
}

func newDesktopPainter() *desktopPainter {
	return &desktopPainter{
		img: image.NewNRGBA(image.Rect(0, 0, deskW, deskH)),
		// BestSpeed keeps encode well under the frame budget and
		// compresses about like a real capture pipeline would.
		enc: png.Encoder{CompressionLevel: png.BestSpeed},
	}
}

// writerBuf is a minimal reusable byte sink for png.Encode.
type writerBuf struct{ b []byte }

func (w *writerBuf) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}

var _ io.Writer = (*writerBuf)(nil)

// Frame paints the scene and returns freshly-allocated PNG bytes
// (the broadcaster retains the slice across fanout).
func (p *desktopPainter) Frame(s *scene) ([]byte, error) {
	p.tick++
	p.paint(s)
	p.buf.b = p.buf.b[:0]
	if err := p.enc.Encode(&p.buf, p.img); err != nil {
		return nil, err
	}
	out := make([]byte, len(p.buf.b))
	copy(out, p.buf.b)
	return out, nil
}

func (p *desktopPainter) paint(s *scene) {
	img := p.img

	p.paintWallpaper()
	p.paintTopBar(s)

	// Terminal window (left).
	termX, termY, termW, termH := 20, 50, 610, 476
	p.paintWindow(termX, termY, termW, termH, "agent@vm-7c2f: ~/payments-service")
	ty := termY + titleH + 10
	for _, l := range s.term {
		drawText(img, termX+12, ty, clip(l.text, (termW-24)/glyphW), l.color)
		ty += lineH
	}
	// Blinking block cursor on the next prompt row.
	if p.tick%12 < 7 && ty < termY+termH-lineH {
		drawText(img, termX+12, ty, "$", colPrompt)
		fillRect(img, termX+12+2*glyphW, ty, termX+12+3*glyphW, ty+glyphH, colText)
	}

	// Editor window (right).
	edX, edY, edW, edH := 650, 50, 610, 560
	title := "agent editor"
	if s.editorFile != "" {
		title = s.editorFile + " — agent editor"
		if s.editorModified {
			title = "● " + title
		}
	}
	p.paintWindow(edX, edY, edW, edH, title)
	fillRect(img, edX+1, edY+titleH, edX+edW-1, edY+edH-1, colEditorBg)
	gutterW := 44
	fillRect(img, edX+1, edY+titleH, edX+gutterW, edY+edH-1, colGutterBg)
	if s.editorFile == "" {
		drawText(img, edX+edW/2-11*glyphW, edY+edH/2, "no file open", colDim)
	}
	ey := edY + titleH + 10
	for i, line := range s.editorLines {
		if ey > edY+edH-lineH {
			break
		}
		if s.editorHighlight[i] {
			fillRect(img, edX+gutterW+1, ey-2, edX+edW-1, ey+glyphH+1, colHighlight)
		}
		drawText(img, edX+8, ey, fmt.Sprintf("%3d", i+1), colDim)
		drawCode(img, edX+gutterW+10, ey, clip(line, (edW-gutterW-20)/glyphW))
		ey += lineH
	}

	// CI browser window (appears once the PR is open), overlapping
	// the terminal like a real stacked window manager.
	if s.ciVisible {
		cw, ch := 470, 176
		cx, cy := 140, 536
		p.paintWindow(cx, cy, cw, ch, "ci.acme.dev — pull request #2481")
		// URL pill.
		fillRect(img, cx+10, cy+titleH+8, cx+cw-10, cy+titleH+28, color.NRGBA{24, 29, 40, 255})
		drawText(img, cx+18, cy+titleH+12, "https://ci.acme.dev/acme/payments-service/pull/2481", colDim)
		if s.ciPassing {
			fillRect(img, cx+10, cy+titleH+40, cx+22, cy+titleH+52, colGreen)
			drawText(img, cx+32, cy+titleH+40, "All checks have passed", colGreen)
			drawText(img, cx+32, cy+titleH+40+lineH, "billing: apply discount at invoice level, round half-up", colText)
		} else {
			spin := []string{"|", "/", "-", "\\"}[int(p.tick/3)%4]
			fillRect(img, cx+10, cy+titleH+40, cx+22, cy+titleH+52, colYellow)
			drawText(img, cx+32, cy+titleH+40, "Checks running "+spin, colYellow)
			drawText(img, cx+32, cy+titleH+40+lineH, "go-test | go-vet | gosec", colDim)
		}
	}

	p.paintClicks(s)
	p.paintStatusBar(s)
}

// clickTTL is how many painter ticks a click ripple stays visible.
const clickTTL = 14

// paintClicks draws expanding rings at recent viewer-click positions
// and prunes expired ones. The visible ripple is the "screen response"
// to the interactive action lane.
func (p *desktopPainter) paintClicks(s *scene) {
	img := p.img
	keep := s.clicks[:0]
	for _, c := range s.clicks {
		age := p.tick - c.born
		if age > clickTTL {
			continue
		}
		keep = append(keep, c)
		col := color.NRGBA{R: 120, G: 190, B: 255, A: 255}
		r1 := 4 + int(age)*2
		for _, r := range []int{r1, r1 + 1} {
			for a := 0; a < 360; a += 4 {
				dx := r * cosDeg(a) / 100
				dy := r * sinDeg(a) / 100
				setPx(img, c.x+dx, c.y+dy, col)
			}
		}
		// Center dot.
		fillRect(img, c.x-2, c.y-2, c.x+2, c.y+2, col)
	}
	s.clicks = keep
}

func setPx(img *image.NRGBA, x, y int, c color.NRGBA) {
	b := img.Bounds()
	if x < 0 || y < 0 || x >= b.Dx() || y >= b.Dy() {
		return
	}
	off := img.PixOffset(x, y)
	img.Pix[off+0] = c.R
	img.Pix[off+1] = c.G
	img.Pix[off+2] = c.B
	img.Pix[off+3] = c.A
}

// cosDeg/sinDeg return cos/sin scaled by 100 — a coarse table is
// plenty for decorative rings (same approach as cua-live's painter).
func cosDeg(deg int) int { return trigTable[((deg%360)+360)%360][0] }
func sinDeg(deg int) int { return trigTable[((deg%360)+360)%360][1] }

var trigTable = func() [360][2]int {
	var t [360][2]int
	for d := 0; d < 360; d++ {
		q := d % 90
		cosQ := 100 - (q*q)/81
		sinQ := 100 - ((90-q)*(90-q))/81
		switch d / 90 {
		case 0:
			t[d] = [2]int{cosQ, sinQ}
		case 1:
			t[d] = [2]int{-sinQ, cosQ}
		case 2:
			t[d] = [2]int{-cosQ, -sinQ}
		default:
			t[d] = [2]int{sinQ, -cosQ}
		}
	}
	return t
}()

func (p *desktopPainter) paintWallpaper() {
	img := p.img
	// Slow-drifting film grain over a vertical navy→plum gradient.
	// The grain is coarse (4×4 blocks) on purpose: per-pixel noise
	// blows PNG frames past 300 KB, while 4×4 texture keeps them at
	// real-screenshot sizes (~100 KB) — enough entropy that the codec
	// can't flatten the wallpaper to nothing, like an actual desktop.
	t := p.tick / 4
	for y := 0; y < deskH; y++ {
		fy := float64(y) / deskH
		baseR := 15 + int(14*fy)
		baseG := 17 + int(10*fy)
		baseB := 30 + int(26*fy)
		row := img.Pix[img.PixOffset(0, y) : img.PixOffset(0, y)+deskW*4]
		for x := 0; x < deskW; x++ {
			n := int(hash3(uint32(x>>2), uint32(y>>2), t)%uint32(2*grainAmp+1)) - grainAmp
			off := x * 4
			row[off+0] = clamp8(baseR + n)
			row[off+1] = clamp8(baseG + n)
			row[off+2] = clamp8(baseB + n*2)
			row[off+3] = 255
		}
	}
}

func (p *desktopPainter) paintTopBar(s *scene) {
	img := p.img
	fillRect(img, 0, 0, deskW, topBarH, colTopBar)
	for i, c := range []color.NRGBA{
		{235, 90, 80, 255}, {240, 190, 70, 255}, {90, 200, 110, 255},
	} {
		cx := 14 + i*20
		fillRect(img, cx, 10, cx+11, 21, c)
	}
	title := "cloud agent — workspace vm-7c2f (live stream)"
	drawText(img, deskW/2-len(title)*glyphW/2, 8, title, colText)
	clock := time.Now().Format("15:04:05")
	drawText(img, deskW-len(clock)*glyphW-16, 8, clock, colDim)
}

func (p *desktopPainter) paintStatusBar(s *scene) {
	img := p.img
	fillRect(img, 20, statusY, deskW-20, statusY+statusH, colTopBar)
	fillRect(img, 20, statusY, 26, statusY+statusH, colAccent)
	spin := []string{"|", "/", "-", "\\"}[int(p.tick/3)%4]
	drawText(img, 38, statusY+11, fmt.Sprintf("%s step %d — %s", spin, s.stepIdx+1, s.stepTitle), colText)
	right := "task: fix invoice rounding + open PR"
	if s.prURL != "" {
		right = s.prURL
	}
	drawText(img, deskW-40-len(right)*glyphW, statusY+11, right, colDim)
}

func (p *desktopPainter) paintWindow(x, y, w, h int, title string) {
	img := p.img
	// Drop shadow, border, title bar, body.
	fillRect(img, x+5, y+5, x+w+5, y+h+5, color.NRGBA{0, 0, 0, 90})
	fillRect(img, x-1, y-1, x+w+1, y+h+1, colBorder)
	fillRect(img, x, y, x+w, y+titleH, colTitleBar)
	fillRect(img, x, y+titleH, x+w, y+h, colWinBody)
	drawText(img, x+10, y+6, clip(title, (w-20)/glyphW), colDim)
}

// ── code syntax highlighting (display candy only) ───────────────────

var goKeywords = map[string]bool{
	"package": true, "import": true, "func": true, "return": true,
	"var": true, "const": true, "for": true, "range": true, "if": true,
	"else": true, "type": true, "struct": true, "int64": true,
	"string": true, "bool": true, "float64": true,
}

// drawCode renders one line of Go-ish source with a tiny keyword /
// string / comment / number colorizer.
func drawCode(img *image.NRGBA, x, y int, line string) {
	if i := strings.Index(line, "//"); i >= 0 {
		drawCode(img, x, y, line[:i])
		drawText(img, x+i*glyphW, y, line[i:], colComment)
		return
	}
	col := x
	i := 0
	for i < len(line) {
		c := line[i]
		switch {
		case c == '"':
			j := i + 1
			for j < len(line) && line[j] != '"' {
				j++
			}
			if j < len(line) {
				j++
			}
			drawText(img, col, y, line[i:j], colStr)
			col += (j - i) * glyphW
			i = j
		case c >= '0' && c <= '9':
			j := i
			for j < len(line) && (line[j] >= '0' && line[j] <= '9' || line[j] == '.') {
				j++
			}
			drawText(img, col, y, line[i:j], colOrange)
			col += (j - i) * glyphW
			i = j
		case isIdent(c):
			j := i
			for j < len(line) && isIdent(line[j]) {
				j++
			}
			word := line[i:j]
			wc := colText
			if goKeywords[word] {
				wc = colPurple
			} else if j < len(line) && line[j] == '(' {
				wc = colBlue
			}
			drawText(img, col, y, word, wc)
			col += (j - i) * glyphW
			i = j
		default:
			drawText(img, col, y, string(c), colText)
			col += glyphW
			i++
		}
	}
}

func isIdent(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' || c >= '0' && c <= '9'
}

// ── low-level drawing ───────────────────────────────────────────────

// drawText blits s at (x, y) using the 7×13 bitmap font. Non-ASCII
// runes render as '?'.
func drawText(img *image.NRGBA, x, y int, s string, c color.NRGBA) {
	for _, r := range s {
		if r < 32 || r > 126 {
			r = '?'
		}
		g := &glyphs[r-32]
		for gy := 0; gy < glyphH; gy++ {
			bits := g[gy]
			if bits == 0 {
				continue
			}
			py := y + gy
			if py < 0 || py >= deskH {
				continue
			}
			for gx := 0; gx < glyphW; gx++ {
				if bits&(1<<(6-gx)) == 0 {
					continue
				}
				px := x + gx
				if px < 0 || px >= deskW {
					continue
				}
				off := img.PixOffset(px, py)
				img.Pix[off+0] = c.R
				img.Pix[off+1] = c.G
				img.Pix[off+2] = c.B
				img.Pix[off+3] = 255
			}
		}
		x += glyphW
	}
}

func fillRect(img *image.NRGBA, x0, y0, x1, y1 int, c color.NRGBA) {
	b := img.Bounds()
	if x0 < 0 {
		x0 = 0
	}
	if y0 < 0 {
		y0 = 0
	}
	if x1 > b.Dx() {
		x1 = b.Dx()
	}
	if y1 > b.Dy() {
		y1 = b.Dy()
	}
	// Alpha-blend only for the translucent shadow; everything else is
	// opaque and takes the fast path.
	if c.A == 255 {
		for y := y0; y < y1; y++ {
			row := img.Pix[img.PixOffset(x0, y):img.PixOffset(x1, y)]
			for i := 0; i < len(row); i += 4 {
				row[i+0] = c.R
				row[i+1] = c.G
				row[i+2] = c.B
				row[i+3] = 255
			}
		}
		return
	}
	a := int(c.A)
	for y := y0; y < y1; y++ {
		row := img.Pix[img.PixOffset(x0, y):img.PixOffset(x1, y)]
		for i := 0; i < len(row); i += 4 {
			row[i+0] = uint8((int(row[i+0])*(255-a) + int(c.R)*a) / 255)
			row[i+1] = uint8((int(row[i+1])*(255-a) + int(c.G)*a) / 255)
			row[i+2] = uint8((int(row[i+2])*(255-a) + int(c.B)*a) / 255)
			row[i+3] = 255
		}
	}
}

func clip(s string, maxChars int) string {
	if maxChars <= 0 {
		return ""
	}
	if len(s) <= maxChars {
		return s
	}
	return s[:maxChars]
}

func clamp8(v int) uint8 {
	if v < 0 {
		return 0
	}
	if v > 255 {
		return 255
	}
	return uint8(v)
}

// hash3 is a cheap integer hash for the wallpaper grain — decorrelated
// enough per (x, y, t) that PNG's filters can't flatten it, which is
// the point (real screenshots don't compress to nothing either).
func hash3(x, y, t uint32) uint32 {
	h := x*0x9E3779B1 ^ y*0x85EBCA77 ^ t*0xC2B2AE3D
	h ^= h >> 15
	h *= 0x2C1B3C6D
	h ^= h >> 12
	h *= 0x297A2D39
	h ^= h >> 15
	return h
}
