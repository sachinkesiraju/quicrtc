//go:build !cualive

package main

import "errors"

// newLiveBrain is the default-build stub. The real implementation that
// drives Chromium via chromedp and calls Claude's computer-use API
// lives in live.go behind the `cualive` build tag, so the default
// `go build ./examples/cua-live/...` (and the -fake demo) compile and
// run with ZERO external dependencies — no chromedp, no Chrome, no
// network.
//
// To use -live, build with the tag:
//
//	go build -tags cualive ./examples/cua-live
//	ANTHROPIC_API_KEY=sk-... ./cua-live -live -goal "..."
func newLiveBrain(_ liveConfig) (Brain, screenSource, error) {
	return nil, nil, errors.New(
		"live mode requires building with -tags cualive (and ANTHROPIC_API_KEY + a Chrome binary). " +
			"Build it: go build -tags cualive ./examples/cua-live  —  or run the no-deps demo: go run ./examples/cua-live -fake")
}
