package main

import (
	"context"
	"image"
	"image/color"
	"image/png"
	"sync"
)

// FakeBrain is a scripted Brain that needs NO API key, NO Chrome, and
// NO network. It replays a believable computer-use session (a model
// working through a login + dashboard task) and synthesizes its own
// screen frames so the whole demo runs end-to-end locally. The point
// is to PROVE the transport wiring carries real agent-SHAPED traffic
// — reasoning tokens, RPC-shaped tool calls, and screen frames on
// their own lanes — without any external dependency.
//
// The synthesized screen frames are honest PNGs (so the viewer renders
// real pixels, and the SDP's "image/png" codec claim is true). Each
// frame paints a stylized "browser window" whose visible content
// matches the step's action — a click target highlights, typed text
// appears in a field — so a viewer SEES the scripted session progress.
type FakeBrain struct {
	w, h   int
	script []Step
	idx    int

	// img is reused across Frame calls to avoid per-frame allocation.
	imgMu sync.Mutex
	img   *image.NRGBA
}

// NewFakeBrain builds the scripted Brain. width/height set the
// synthesized screen dimensions (matched to the server's SDP).
func NewFakeBrain(width, height int) *FakeBrain {
	return &FakeBrain{
		w:      width,
		h:      height,
		script: fakeScript(),
		img:    image.NewNRGBA(image.Rect(0, 0, width, height)),
	}
}

// Goal returns the scripted task description.
func (b *FakeBrain) Goal() string {
	return "Log into the demo dashboard and read the latest payout total"
}

// NextStep returns the next scripted turn. screenshotPNG is ignored —
// the fake brain is open-loop (it doesn't actually look at pixels), it
// just walks its script. The very last scripted step has Done=true.
func (b *FakeBrain) NextStep(_ context.Context, _ []byte) (Step, error) {
	if b.idx >= len(b.script) {
		// Loop the script so a left-running demo keeps producing
		// traffic on every lane instead of going idle. Re-arm the
		// Done flag only on the script's own final step.
		b.idx = 0
	}
	step := b.script[b.idx]
	b.idx++
	return step, nil
}

// Close is a no-op for the fake brain (no resources held).
func (b *FakeBrain) Close() error { return nil }

// Frame synthesizes the screen PNG that an agent host WOULD have
// captured after executing the given action on the given step index.
// keyframe brightens the frame slightly for visual K/P confirmation in
// the viewer (matching examples/internal/synframe's convention).
//
// This is fake-mode only: in live mode the screen frame comes from a
// real chromedp screenshot, not from this painter.
func (b *FakeBrain) Frame(stepIdx int, act Action, keyframe bool) ([]byte, error) {
	b.imgMu.Lock()
	defer b.imgMu.Unlock()
	paintBrowser(b.img, stepIdx, act, keyframe)
	var buf pngBuffer
	enc := png.Encoder{CompressionLevel: png.DefaultCompression}
	if err := enc.Encode(&buf, b.img); err != nil {
		return nil, err
	}
	// Return a fresh copy: the caller (the broadcaster) keeps the slice
	// alive across fanout while img is reused next Frame.
	out := make([]byte, len(buf.b))
	copy(out, buf.b)
	return out, nil
}

// pngBuffer is a tiny bytes.Buffer-equivalent that avoids importing
// bytes solely for one Encode sink. png.Encode only needs io.Writer.
type pngBuffer struct{ b []byte }

func (p *pngBuffer) Write(b []byte) (int, error) {
	p.b = append(p.b, b...)
	return len(b), nil
}

// fakeScript is the hardcoded computer-use session the fake brain
// replays. Each entry is one turn: a model thought plus the action it
// decided to take. The sequence is a realistic CUA flow — open the
// site, log in, navigate, read a value, finish.
//
// HARDCODED CONTENT: the reasoning text and actions are scripted, no
// model is running. The transport behavior (token stream cadence,
// bidi-per-call tool calls, screen frames on a separate lane) is real.
func fakeScript() []Step {
	return []Step{
		{
			Reasoning: "The task is to log into the demo dashboard and read the latest payout total. I'll start by navigating to the login page.",
			Action:    Action{Type: ActionNavigate, URL: "https://dashboard.demo.example/login"},
		},
		{
			Reasoning: "The login form has loaded. I can see an email field near the top. I'll click it to focus, then type the demo account email.",
			Action:    Action{Type: ActionLeftClick, X: 240, Y: 150},
		},
		{
			Reasoning: "Email field focused. Typing the demo credentials.",
			Action:    Action{Type: ActionTypeText, Text: "agent@demo.example"},
		},
		{
			Reasoning: "Now I'll tab to the password field and enter the password.",
			Action:    Action{Type: ActionKey, Key: "Tab"},
		},
		{
			Reasoning: "Typing the password into the focused field.",
			Action:    Action{Type: ActionTypeText, Text: "•••••••••"},
		},
		{
			Reasoning: "Credentials entered. Clicking the Sign in button to submit.",
			Action:    Action{Type: ActionLeftClick, X: 240, Y: 250},
		},
		{
			Reasoning: "Waiting for the dashboard to load after authentication.",
			Action:    Action{Type: ActionWait, WaitMS: 800},
		},
		{
			Reasoning: "Dashboard loaded. The payout summary is below the fold, so I'll scroll down to bring it into view.",
			Action:    Action{Type: ActionScroll, X: 240, Y: 200, ScrollY: 300},
		},
		{
			Reasoning: "I can now see the payouts card. Taking a screenshot to read the latest total precisely.",
			Action:    Action{Type: ActionScreenshot},
		},
		{
			Reasoning: "The latest payout total reads $12,480.00, posted today. Task complete — I have the value the user asked for.",
			Action:    Action{Type: ActionScreenshot},
			Done:      true,
		},
	}
}

// ── synthesized browser painter ───────────────────────────────────
//
// Paints a stylized browser window so the viewer sees the scripted
// session progress visually. Not pixel-perfect Chrome — just enough
// shape (chrome bar, address field, page body, a click highlight or a
// typed-text caret) that the screen lane is obviously carrying
// step-correlated content rather than noise.

const (
	chromeBarH = 48
)

func paintBrowser(img *image.NRGBA, stepIdx int, act Action, keyframe bool) {
	w, h := img.Bounds().Dx(), img.Bounds().Dy()

	// Page background — light grey "browser content area". Keyframes
	// are tinted marginally brighter (same K/P convention as synframe).
	pageR, pageG, pageB := uint8(244), uint8(246), uint8(250)
	if keyframe {
		pageR, pageG, pageB = 250, 252, 255
	}
	fillRect(img, 0, 0, w, h, color.NRGBA{R: pageR, G: pageG, B: pageB, A: 255})

	// Chrome bar across the top.
	fillRect(img, 0, 0, w, chromeBarH, color.NRGBA{R: 36, G: 42, B: 54, A: 255})
	// Traffic-light dots.
	for i, c := range []color.NRGBA{
		{R: 235, G: 90, B: 80, A: 255},
		{R: 240, G: 190, B: 70, A: 255},
		{R: 90, G: 200, B: 110, A: 255},
	} {
		cx := 16 + i*22
		fillRect(img, cx, 18, cx+12, 30, c)
	}
	// Address bar pill.
	fillRect(img, 84, 12, w-16, 36, color.NRGBA{R: 58, G: 66, B: 80, A: 255})

	// Page body content depends on the step: login form for the early
	// steps, dashboard card for the later ones. We key off the action
	// rather than the index so the live-vs-fake split stays loose.
	switch {
	case act.Type == ActionScroll || stepIdx >= 7:
		paintDashboard(img, w, h)
	default:
		paintLoginForm(img, w, h, act)
	}

	// A pulsing crosshair at the click target so clicks are visible.
	if act.Type == ActionLeftClick {
		paintCrosshair(img, act.X, act.Y)
	}

	// A progress strip at the very bottom whose fill tracks step index
	// — gives the eye a per-frame motion cue even on static scenes.
	prog := (stepIdx*73 + 40) % w
	fillRect(img, 0, h-6, prog, h, color.NRGBA{R: 90, G: 150, B: 230, A: 255})
}

func paintLoginForm(img *image.NRGBA, w, _ int, act Action) {
	// Centered card.
	cardX0, cardX1 := w/2-160, w/2+160
	fillRect(img, cardX0, chromeBarH+24, cardX1, chromeBarH+24+220,
		color.NRGBA{R: 255, G: 255, B: 255, A: 255})
	// Title bar of the card.
	fillRect(img, cardX0, chromeBarH+24, cardX1, chromeBarH+24+8,
		color.NRGBA{R: 90, G: 150, B: 230, A: 255})

	// Highlight whichever field the current type action is filling: the
	// email field for the email string, the password field otherwise.
	typing := act.Type == ActionTypeText && len(act.Text) > 0
	typingEmail := typing && act.Text == "agent@demo.example"

	// Email field.
	emailY := chromeBarH + 78
	paintField(img, cardX0+24, emailY, cardX1-24, emailY+30, typingEmail)

	// Password field.
	pwY := chromeBarH + 128
	paintField(img, cardX0+24, pwY, cardX1-24, pwY+30, typing && !typingEmail)

	// Sign-in button.
	fillRect(img, cardX0+24, chromeBarH+178, cardX1-24, chromeBarH+178+34,
		color.NRGBA{R: 70, G: 130, B: 220, A: 255})
}

func paintDashboard(img *image.NRGBA, w, h int) {
	// Sidebar.
	fillRect(img, 0, chromeBarH, 120, h, color.NRGBA{R: 28, G: 34, B: 46, A: 255})
	// "Latest payout" card.
	fillRect(img, 160, chromeBarH+40, w-40, chromeBarH+40+120,
		color.NRGBA{R: 255, G: 255, B: 255, A: 255})
	// Accent header on the card.
	fillRect(img, 160, chromeBarH+40, w-40, chromeBarH+40+10,
		color.NRGBA{R: 90, G: 200, B: 130, A: 255})
	// A big "value" bar inside the card representing $12,480.00.
	fillRect(img, 184, chromeBarH+96, 380, chromeBarH+96+28,
		color.NRGBA{R: 40, G: 48, B: 60, A: 255})
	// A couple of supporting rows.
	for i := 0; i < 2; i++ {
		y := chromeBarH + 200 + i*40
		fillRect(img, 160, y, w-40, y+24, color.NRGBA{R: 230, G: 234, B: 240, A: 255})
	}
}

func paintField(img *image.NRGBA, x0, y0, x1, y1 int, filled bool) {
	border := color.NRGBA{R: 200, G: 206, B: 214, A: 255}
	fillRect(img, x0, y0, x1, y1, border)
	inner := color.NRGBA{R: 248, G: 250, B: 252, A: 255}
	fillRect(img, x0+2, y0+2, x1-2, y1-2, inner)
	if filled {
		// A "typed text" bar inside the field.
		fillRect(img, x0+8, y0+10, x0+8+140, y1-10,
			color.NRGBA{R: 60, G: 70, B: 84, A: 255})
	}
}

func paintCrosshair(img *image.NRGBA, cx, cy int) {
	c := color.NRGBA{R: 235, G: 70, B: 70, A: 255}
	for d := -10; d <= 10; d++ {
		setPx(img, cx+d, cy, c)
		setPx(img, cx, cy+d, c)
	}
	// A ring around the click point.
	for _, r := range []int{8, 9} {
		for a := 0; a < 360; a += 6 {
			// Cheap integer circle: step the angle in coarse increments.
			dx := r * cosDeg(a) / 100
			dy := r * sinDeg(a) / 100
			setPx(img, cx+dx, cy+dy, c)
		}
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
	for y := y0; y < y1; y++ {
		row := img.Pix[img.PixOffset(0, y) : img.PixOffset(0, y)+b.Dx()*4]
		for x := x0; x < x1; x++ {
			off := x * 4
			row[off+0] = c.R
			row[off+1] = c.G
			row[off+2] = c.B
			row[off+3] = c.A
		}
	}
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

// cosDeg/sinDeg return cos/sin scaled by 100 for the small set of
// angles the crosshair ring uses. A tiny lookup avoids importing math
// for a decorative ring.
func cosDeg(deg int) int { return trigTable[((deg%360)+360)%360][0] }
func sinDeg(deg int) int { return trigTable[((deg%360)+360)%360][1] }

// trigTable holds [cos*100, sin*100] for 0..359 degrees, filled once.
var trigTable = func() [360][2]int {
	var t [360][2]int
	// Approximate with a coarse piecewise from a few anchor points;
	// the ring only needs to look round, not be metrologically exact.
	for d := 0; d < 360; d++ {
		// Use a small rational approximation via repeated quarter
		// symmetry from a 0..90 quarter built from a parabola-ish
		// shape. Good enough for a decorative 9px ring.
		q := d % 90
		// cos drops 100→0 across a quarter, sin rises 0→100.
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
