// cua-live — a REAL, runnable computer-use agent whose screen,
// reasoning, tool calls, and telemetry stream through four quicrtc
// tracks on ONE QUIC connection to the browser viewer.
//
// This file defines the transport-agnostic agent contract. The agent
// loop in main.go drives a Brain step by step and fans each step's
// output onto the right track:
//
//	┌────────────┬───────────────┬───────────────────────────────────┐
//	│ Track      │ Kind          │ On-wire delivery (per feed/pump.go)│
//	├────────────┼───────────────┼───────────────────────────────────┤
//	│ screen     │ KindVideo     │ stream-per-GOP uni stream (PNG AUs)│
//	│ reasoning  │ KindTokens    │ persistent low-latency uni stream  │
//	│ toolcalls  │ KindToolCalls │ fresh bidi stream per AU           │
//	│ telemetry  │ datagrams     │ unreliable QUIC datagrams          │
//	└────────────┴───────────────┴───────────────────────────────────┘
//
// Two Brain implementations ship:
//
//   - FakeBrain (-fake): a scripted session that needs NO API key, NO
//     Chrome, and NO network. It synthesizes screen frames and replays
//     a believable reasoning + action sequence so the whole demo runs
//     end-to-end anywhere and PROVES the transport wiring.
//
//   - liveBrain (-live, build tag `cualive`): calls Claude's computer-
//     use API over raw net/http and drives a real Chromium via
//     chromedp. Requires ANTHROPIC_API_KEY + a Chrome binary + network;
//     does NOT run in CI.
package main

import "context"

// liveConfig carries the knobs newLiveBrain needs. Defined here (no
// build tag) so both the default-build stub (live_stub.go) and the
// real chromedp implementation (live.go, tag `cualive`) share one
// definition and main.go compiles identically regardless of tag.
type liveConfig struct {
	Goal     string // task description; empty uses a built-in default
	StartURL string // initial page to open
	Model    string // Anthropic model id (computer-use beta)
	Width    int    // browser viewport / screenshot width
	Height   int    // browser viewport / screenshot height
}

// ActionType enumerates the computer-use actions the agent can take.
// These mirror the action vocabulary of Anthropic's computer-use tool
// so the fake and live Brains speak the same language and main.go can
// render either identically on the toolcalls track.
type ActionType string

const (
	// ActionScreenshot asks the host to capture the current screen.
	// Every step implicitly returns a fresh screenshot, so an explicit
	// screenshot action is mostly a no-op the model emits to "look".
	ActionScreenshot ActionType = "screenshot"

	// ActionLeftClick clicks at (X, Y) in screen pixel coordinates.
	ActionLeftClick ActionType = "left_click"

	// ActionTypeText types Text at the current focus.
	ActionTypeText ActionType = "type"

	// ActionKey presses a key / chord (e.g. "Return", "ctrl+l").
	ActionKey ActionType = "key"

	// ActionScroll scrolls by (ScrollX, ScrollY) at (X, Y).
	ActionScroll ActionType = "scroll"

	// ActionNavigate loads URL in the browser (live mode only; the
	// fake brain treats it as a scripted scene change).
	ActionNavigate ActionType = "navigate"

	// ActionWait pauses WaitMS milliseconds (e.g. for a page to settle).
	ActionWait ActionType = "wait"
)

// Action is one computer-use action emitted by a Brain step. It is the
// payload of one AU on the toolcalls track. Only the fields relevant
// to Type are populated; the rest are zero. Marshaled to JSON for the
// wire so the viewer's ToolCalls panel can list it human-readably.
type Action struct {
	Type ActionType `json:"type"`

	// X, Y are screen-pixel coordinates for click / scroll actions.
	X int `json:"x,omitempty"`
	Y int `json:"y,omitempty"`

	// Text is the string to type for ActionTypeText.
	Text string `json:"text,omitempty"`

	// Key is the key / chord for ActionKey (e.g. "Return", "ctrl+l").
	Key string `json:"key,omitempty"`

	// ScrollX, ScrollY are scroll deltas for ActionScroll.
	ScrollX int `json:"scroll_x,omitempty"`
	ScrollY int `json:"scroll_y,omitempty"`

	// URL is the navigation target for ActionNavigate.
	URL string `json:"url,omitempty"`

	// WaitMS is the pause duration for ActionWait.
	WaitMS int `json:"wait_ms,omitempty"`
}

// Step is the full result of one Brain turn. The agent loop fans each
// field onto its own track:
//
//	Reasoning → reasoning (tokens), token by token
//	Action    → toolcalls (one bidi-per-call AU)
//	Screenshot is produced AFTER the action executes and goes on screen.
type Step struct {
	// Reasoning is the model's natural-language thought for this turn.
	// Streamed token-by-token on the reasoning track so the viewer's
	// Tokens panel fills in live.
	Reasoning string

	// Action is the computer-use action to execute this turn.
	Action Action

	// Done is true on the final step; the loop stops after fanning out
	// this step's reasoning + action.
	Done bool
}

// Brain is the agent loop's pluggable decision-maker. Given the
// current screenshot (PNG bytes), it decides the next reasoning +
// action. The loop calls NextStep repeatedly, executing each returned
// Action against the host (real browser for -live, scripted scene for
// -fake) and feeding the resulting screenshot back into the next call.
//
// Implementations must be safe to call from a single goroutine in a
// loop; the loop never calls NextStep concurrently.
type Brain interface {
	// NextStep returns the next agent turn. screenshotPNG is the most
	// recent screen capture (nil on the very first call). Returning a
	// Step with Done=true ends the session after that step is fanned
	// out. A non-nil error aborts the loop.
	NextStep(ctx context.Context, screenshotPNG []byte) (Step, error)

	// Goal returns a one-line description of the task the Brain is
	// pursuing, shown in the status line and the initial reasoning.
	Goal() string

	// Close releases any resources (browser, HTTP client). Safe to call
	// once after the loop exits.
	Close() error
}
