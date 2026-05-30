// examples/cua-live is a sub-module of quicrtc. It hosts the live
// computer-use agent demo, whose -live mode drives a real Chromium via
// chromedp + cdproto and therefore needs a newer Go toolchain than the
// main module targets. Keeping it isolated — exactly like
// testing/benchmarks/browser — lets the main quicrtc module stay lean:
// library consumers don't transitively pull chromedp.
//
// Build / run independently:
//
//	cd examples/cua-live
//	go run . -fake            # zero external deps; runs anywhere
//	go build -tags cualive .  # the real Claude + chromedp path
//
// The replace directive points at the parent so the demo tracks
// quicrtc's public packages without needing a release.
module github.com/sachinkesiraju/quicrtc/examples/cua-live

go 1.26

replace github.com/sachinkesiraju/quicrtc => ../..

require (
	github.com/chromedp/cdproto v0.0.0-20260321001828-e3e3800016bc
	github.com/chromedp/chromedp v0.15.1
	github.com/sachinkesiraju/quicrtc v0.0.0-00010101000000-000000000000
)

require (
	github.com/chromedp/sysutil v1.1.0 // indirect
	github.com/dunglas/httpsfv v1.1.0 // indirect
	github.com/go-json-experiment/json v0.0.0-20260214004413-d219187c3433 // indirect
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/gobwas/ws v1.4.0 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.59.0 // indirect
	github.com/quic-go/webtransport-go v0.10.0 // indirect
	golang.org/x/crypto v0.48.0 // indirect
	golang.org/x/net v0.50.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.34.0 // indirect
)
