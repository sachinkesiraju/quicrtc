// benchmarks/browser is a sub-module of quicrtc. It hosts the
// Chromium-driven browser benchmark suite (setup latency + multimodal)
// that compares quicrtc-native against pion WebRTC via real-browser
// JS receivers.
//
// It's a separate Go module because the Chromium driver (chromedp +
// cdproto) requires a newer Go toolchain than the rest of quicrtc.
// Keeping it isolated lets the main module target the most-compatible
// Go version while this sub-module pins to the cdproto-required one.
//
// Build / run independently:
//
//	cd benchmarks/browser
//	go test -v ./...
//
// The replace directive points at the parent so changes to quicrtc's
// public packages are picked up without committing a release.
module github.com/sachinkesiraju/quicrtc/benchmarks/browser

go 1.26

replace github.com/sachinkesiraju/quicrtc => ../..

require (
	github.com/chromedp/cdproto v0.0.0-20260321001828-e3e3800016bc
	github.com/chromedp/chromedp v0.15.1
	github.com/pion/webrtc/v4 v4.2.12
	github.com/sachinkesiraju/quicrtc v0.0.0-00010101000000-000000000000
)
