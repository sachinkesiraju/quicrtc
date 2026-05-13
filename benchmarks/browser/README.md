# benchmarks/browser

Chromium-driven browser benchmark comparing quicrtc-native against
pion WebRTC via real-browser JS receivers. Measures setup latency
and multi-modal stream behavior end-to-end through the browser
networking stack.

## Why this is a sub-module

This subdirectory has its own `go.mod` because it depends on
`github.com/chromedp/chromedp` and `chromedp/cdproto`, which require
a newer Go toolchain than the main `quicrtc` module. Keeping it
isolated lets the main module target the most-compatible Go version
while still exercising real-browser performance here.

## Requirements

- Go 1.26+ (for the chromedp dependency).
- Chromium or Google Chrome installed locally (chromedp launches it
  in headless mode).

## Running

```bash
cd benchmarks/browser
go test -v ./... -timeout=300s
```

## Contents

- `orchestrator.go` — drives the publisher (quicrtc + pion), serves
  the JS receiver page, collects timing via `/report`.
- `setup_test.go` — setup-latency benchmark (handshake + first
  frame).
- `browser_test.go` — Chromium driver wiring.
- `multimodal_test.go` — multi-track end-to-end test.
- `pages/` — static JS receiver pages.

## Output

Each test logs `[end-to-end ...]` and `[inter-arrival ...]` stats
plus comparison lines like `quicrtc-native faster by N×` so results
are diffable across runs.
