//go:build cualive

// live.go is the REAL computer-use Brain: it calls Claude's
// computer-use API over raw net/http (no third-party SDK) and drives a
// real Chromium via github.com/chromedp/chromedp for screenshots and
// action execution.
//
// It is behind the `cualive` build tag so the default build — and the
// -fake demo — compile and run with ZERO external dependencies. Build
// and run live mode with:
//
//	go build -tags cualive ./examples/cua-live
//	ANTHROPIC_API_KEY=sk-... ./cua-live -live \
//	    -url https://news.ycombinator.com \
//	    -goal "Find the top story and tell me its title"
//
// Requirements (NOT available in CI): ANTHROPIC_API_KEY, a Chrome /
// Chromium binary on PATH, and outbound network to api.anthropic.com.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/chromedp/cdproto/input"
	"github.com/chromedp/chromedp"
)

const (
	anthropicURL     = "https://api.anthropic.com/v1/messages"
	anthropicVersion = "2023-06-01"
	// computerUseBeta is the beta header that unlocks the computer_use
	// tool. The tool type string below must match this beta revision.
	computerUseBeta = "computer-use-2025-01-24"
	computerToolTyp = "computer_20250124"
)

// liveBrain implements Brain by round-tripping the Anthropic Messages
// API with the computer-use tool. It owns the conversation history and
// the chromedp context (shared with liveScreen, which executes the
// actions and captures screenshots).
type liveBrain struct {
	cfg    liveConfig
	apiKey string
	http   *http.Client

	// browser is the shared chromedp context; liveScreen runs actions
	// against it. cancels tears down the browser + allocator.
	browser context.Context
	cancels []context.CancelFunc

	// history is the running Messages API conversation. Each NextStep
	// appends the user turn (tool_result with the latest screenshot)
	// and the assistant turn (the model's reasoning + tool_use).
	history []apiMessage

	// pendingToolUseID correlates the model's tool_use block with the
	// tool_result we must send back on the next turn. Empty before the
	// first action.
	pendingToolUseID string
	started          bool
}

// newLiveBrain constructs the live Brain + its screen source. Both
// share one chromedp browser context: the Brain reasons, the
// screenSource executes the chosen action and captures the result.
func newLiveBrain(cfg liveConfig) (Brain, screenSource, error) {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		return nil, nil, errors.New("ANTHROPIC_API_KEY is not set (required for -live)")
	}
	if cfg.Goal == "" {
		cfg.Goal = "Explore the page, then summarize what you see in one sentence."
	}

	// Launch headless Chromium sized to our screen track. headless=true
	// keeps the demo CI-/server-friendly; flip to false locally to
	// watch the real browser alongside the viewer.
	allocCtx, allocCancel := chromedp.NewExecAllocator(context.Background(),
		append(chromedp.DefaultExecAllocatorOptions[:],
			chromedp.WindowSize(cfg.Width, cfg.Height),
		)...)
	browserCtx, browserCancel := chromedp.NewContext(allocCtx)

	// Open the starting page and set the viewport so screenshots match
	// the advertised SDP dimensions.
	if err := chromedp.Run(browserCtx,
		chromedp.EmulateViewport(int64(cfg.Width), int64(cfg.Height)),
		chromedp.Navigate(cfg.StartURL),
		chromedp.Sleep(800*time.Millisecond),
	); err != nil {
		browserCancel()
		allocCancel()
		return nil, nil, fmt.Errorf("launch chromium: %w (is a Chrome binary installed?)", err)
	}

	b := &liveBrain{
		cfg:     cfg,
		apiKey:  apiKey,
		http:    &http.Client{Timeout: 120 * time.Second},
		browser: browserCtx,
		cancels: []context.CancelFunc{browserCancel, allocCancel},
	}
	scr := &liveScreen{browser: browserCtx, w: cfg.Width, h: cfg.Height}
	return b, scr, nil
}

func (b *liveBrain) Goal() string { return b.cfg.Goal }

func (b *liveBrain) Close() error {
	// Cancel in reverse order: browser context before its allocator.
	for i := len(b.cancels) - 1; i >= 0; i-- {
		b.cancels[i]()
	}
	return nil
}

// NextStep appends the latest screenshot as a tool_result (or the
// initial user prompt on the first call), calls the Messages API, and
// parses the model's reasoning + the single computer-use action it
// chose. Done is set when the model stops requesting tool use (i.e.
// it produced a final text answer with no tool_use block).
func (b *liveBrain) NextStep(ctx context.Context, screenshotPNG []byte) (Step, error) {
	// Build this turn's user message.
	if !b.started {
		// First turn: the goal plus the initial screenshot.
		content := []apiContent{{Type: "text", Text: b.cfg.Goal}}
		if len(screenshotPNG) > 0 {
			content = append(content, imageContent(screenshotPNG))
		}
		b.history = append(b.history, apiMessage{Role: "user", Content: content})
		b.started = true
	} else {
		// Subsequent turns: return the post-action screenshot as the
		// tool_result for the model's last tool_use.
		b.history = append(b.history, apiMessage{Role: "user", Content: []apiContent{
			{
				Type:      "tool_result",
				ToolUseID: b.pendingToolUseID,
				Content:   []apiContent{imageContent(screenshotPNG)},
			},
		}})
	}

	resp, err := b.callAPI(ctx)
	if err != nil {
		return Step{}, err
	}
	// Record the assistant turn verbatim so the next tool_result lines
	// up with the model's expectations.
	b.history = append(b.history, apiMessage{Role: "assistant", Content: resp.Content})

	// Extract the reasoning text and the (optional) tool_use action.
	var reasoning strings.Builder
	var act Action
	haveAction := false
	for _, c := range resp.Content {
		switch c.Type {
		case "text":
			reasoning.WriteString(c.Text)
		case "tool_use":
			b.pendingToolUseID = c.ID
			if a, ok := toolUseToAction(c.Input); ok {
				act = a
				haveAction = true
			}
		}
	}

	step := Step{Reasoning: strings.TrimSpace(reasoning.String())}
	if haveAction {
		step.Action = act
	} else {
		// No tool_use → the model gave a final answer. Emit a terminal
		// screenshot action so the loop captures the final state once
		// more, and mark the session done.
		step.Action = Action{Type: ActionScreenshot}
		step.Done = true
	}
	// stop_reason "end_turn" with no tool_use also signals completion.
	if resp.StopReason == "end_turn" && !haveAction {
		step.Done = true
	}
	return step, nil
}

// callAPI POSTs the current history to the Messages API with the
// computer-use tool declared, and returns the parsed response.
func (b *liveBrain) callAPI(ctx context.Context) (apiResponse, error) {
	reqBody := apiRequest{
		Model:     b.cfg.Model,
		MaxTokens: 1024,
		Tools: []apiTool{{
			Type:          computerToolTyp,
			Name:          "computer",
			DisplayWidth:  b.cfg.Width,
			DisplayHeight: b.cfg.Height,
		}},
		Messages: b.history,
	}
	raw, err := json.Marshal(reqBody)
	if err != nil {
		return apiResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, anthropicURL, bytes.NewReader(raw))
	if err != nil {
		return apiResponse{}, err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-api-key", b.apiKey)
	req.Header.Set("anthropic-version", anthropicVersion)
	req.Header.Set("anthropic-beta", computerUseBeta)

	httpResp, err := b.http.Do(req)
	if err != nil {
		return apiResponse{}, err
	}
	defer httpResp.Body.Close()
	body, _ := io.ReadAll(httpResp.Body)
	if httpResp.StatusCode != http.StatusOK {
		return apiResponse{}, fmt.Errorf("anthropic API %d: %s", httpResp.StatusCode, truncate(string(body), 400))
	}
	var resp apiResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return apiResponse{}, fmt.Errorf("decode response: %w", err)
	}
	return resp, nil
}

// ── Messages API wire types (raw net/http; no SDK) ─────────────────

type apiRequest struct {
	Model     string       `json:"model"`
	MaxTokens int          `json:"max_tokens"`
	Tools     []apiTool    `json:"tools"`
	Messages  []apiMessage `json:"messages"`
}

type apiTool struct {
	Type          string `json:"type"`
	Name          string `json:"name"`
	DisplayWidth  int    `json:"display_width_px"`
	DisplayHeight int    `json:"display_height_px"`
}

type apiMessage struct {
	Role    string       `json:"role"`
	Content []apiContent `json:"content"`
}

// apiContent is a polymorphic content block. The fields populated
// depend on Type; json omitempty keeps unused fields off the wire.
type apiContent struct {
	Type string `json:"type"`

	// text block
	Text string `json:"text,omitempty"`

	// tool_use block (assistant → us)
	ID    string          `json:"id,omitempty"`
	Name  string          `json:"name,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`

	// tool_result block (us → assistant)
	ToolUseID string       `json:"tool_use_id,omitempty"`
	Content   []apiContent `json:"content,omitempty"`

	// image block (inside tool_result / user content)
	Source *apiImageSource `json:"source,omitempty"`
}

type apiImageSource struct {
	Type      string `json:"type"`       // "base64"
	MediaType string `json:"media_type"` // "image/png"
	Data      string `json:"data"`       // base64 PNG
}

type apiResponse struct {
	Content    []apiContent `json:"content"`
	StopReason string       `json:"stop_reason"`
}

// imageContent wraps PNG bytes as a base64 image content block.
func imageContent(png []byte) apiContent {
	return apiContent{
		Type: "image",
		Source: &apiImageSource{
			Type:      "base64",
			MediaType: "image/png",
			Data:      base64.StdEncoding.EncodeToString(png),
		},
	}
}

// toolUseToAction maps the computer tool's JSON input to our Action.
// The computer_20250124 tool nests the concrete action under an
// "action" string plus action-specific fields ("coordinate", "text",
// etc.). Returns ok=false for actions we don't model (the loop then
// treats it as a screenshot no-op).
func toolUseToAction(input json.RawMessage) (Action, bool) {
	var in struct {
		Action     string `json:"action"`
		Coordinate []int  `json:"coordinate"`
		Text       string `json:"text"`
		ScrollDir  string `json:"scroll_direction"`
		ScrollAmt  int    `json:"scroll_amount"`
		Duration   int    `json:"duration"`
	}
	if err := json.Unmarshal(input, &in); err != nil {
		return Action{}, false
	}
	at := strings.ToLower(in.Action)
	switch {
	case at == "screenshot":
		return Action{Type: ActionScreenshot}, true
	case at == "left_click" || at == "mouse_click":
		x, y := coord(in.Coordinate)
		return Action{Type: ActionLeftClick, X: x, Y: y}, true
	case at == "type":
		return Action{Type: ActionTypeText, Text: in.Text}, true
	case at == "key":
		return Action{Type: ActionKey, Key: in.Text}, true
	case at == "scroll":
		dx, dy := scrollDelta(in.ScrollDir, in.ScrollAmt)
		x, y := coord(in.Coordinate)
		return Action{Type: ActionScroll, X: x, Y: y, ScrollX: dx, ScrollY: dy}, true
	case at == "wait":
		ms := in.Duration * 1000 // computer tool expresses wait in seconds
		if ms == 0 {
			ms = 500
		}
		return Action{Type: ActionWait, WaitMS: ms}, true
	default:
		return Action{}, false
	}
}

func coord(c []int) (int, int) {
	if len(c) >= 2 {
		return c[0], c[1]
	}
	return 0, 0
}

func scrollDelta(dir string, amt int) (int, int) {
	if amt == 0 {
		amt = 3
	}
	const px = 100 // CSS pixels per scroll "unit"
	switch strings.ToLower(dir) {
	case "up":
		return 0, -amt * px
	case "down":
		return 0, amt * px
	case "left":
		return -amt * px, 0
	case "right":
		return amt * px, 0
	default:
		return 0, amt * px
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// ── liveScreen: execute the action + capture a real screenshot ─────

// liveScreen runs the chosen Action against the shared chromedp
// browser, then captures a fresh full-viewport screenshot. It is the
// screenSource the agent loop calls every turn.
type liveScreen struct {
	browser context.Context
	w, h    int
}

// Frame executes act in Chromium and returns the post-action PNG. The
// keyframe flag is irrelevant to a real screenshot (every PNG is
// self-contained), so it is ignored here — the loop still marks the
// AU appropriately for the video lane's resync semantics.
func (s *liveScreen) Frame(ctx context.Context, _ int, act Action, _ bool) ([]byte, error) {
	if err := chromedp.Run(s.browser, actionTasks(act)...); err != nil {
		return nil, fmt.Errorf("execute %s: %w", act.Type, err)
	}
	// Let the page settle briefly so the screenshot reflects the action.
	_ = chromedp.Run(s.browser, chromedp.Sleep(250*time.Millisecond))

	var png []byte
	if err := chromedp.Run(s.browser, chromedp.CaptureScreenshot(&png)); err != nil {
		return nil, fmt.Errorf("screenshot: %w", err)
	}
	// Copy: chromedp may reuse the backing buffer; the broadcaster keeps
	// the slice alive across fanout.
	out := make([]byte, len(png))
	copy(out, png)
	_ = ctx // ctx is the loop's; chromedp uses its own browser context
	return out, nil
}

// actionTasks turns one Action into the chromedp actions that execute
// it against the page.
func actionTasks(act Action) []chromedp.Action {
	switch act.Type {
	case ActionNavigate:
		return []chromedp.Action{chromedp.Navigate(act.URL)}
	case ActionLeftClick:
		return []chromedp.Action{chromedp.MouseClickXY(float64(act.X), float64(act.Y))}
	case ActionTypeText:
		return []chromedp.Action{chromedp.KeyEvent(act.Text)}
	case ActionKey:
		return []chromedp.Action{keyChord(act.Key)}
	case ActionScroll:
		return []chromedp.Action{wheelScroll(act.X, act.Y, act.ScrollX, act.ScrollY)}
	case ActionWait:
		d := time.Duration(act.WaitMS) * time.Millisecond
		if d <= 0 {
			d = 300 * time.Millisecond
		}
		return []chromedp.Action{chromedp.Sleep(d)}
	default: // ActionScreenshot and unknown: nothing to execute
		return nil
	}
}

// wheelScroll dispatches a single mouse-wheel event at (x,y) with the
// given CSS-pixel deltas. cdproto's DispatchMouseEventParams exposes
// DeltaX/DeltaY as plain fields, so we set them directly.
func wheelScroll(x, y, dx, dy int) chromedp.Action {
	return chromedp.ActionFunc(func(ctx context.Context) error {
		p := input.DispatchMouseEvent(input.MouseWheel, float64(x), float64(y))
		p.DeltaX = float64(dx)
		p.DeltaY = float64(dy)
		return p.Do(ctx)
	})
}

// keyChord maps a key name / chord (e.g. "Return", "Tab", "ctrl+l")
// onto a chromedp KeyEvent. chromedp.KeyEvent accepts the literal
// runes; for named keys we translate to the corresponding control
// rune. Unknown names fall through as literal text.
func keyChord(key string) chromedp.Action {
	switch strings.ToLower(key) {
	case "return", "enter":
		return chromedp.KeyEvent("\r")
	case "tab":
		return chromedp.KeyEvent("\t")
	case "backspace":
		return chromedp.KeyEvent("\b")
	case "escape", "esc":
		return chromedp.KeyEvent("")
	default:
		// Chords like "ctrl+l" aren't expanded here (modifier handling
		// is non-trivial across platforms); send the trailing token as
		// a literal so at least simple named keys work.
		if i := strings.LastIndex(key, "+"); i >= 0 {
			key = key[i+1:]
		}
		return chromedp.KeyEvent(key)
	}
}
