//go:build browser

package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/cdproto/runtime"
	"github.com/chromedp/chromedp"
)

func chromeOpts() []chromedp.ExecAllocatorOption {
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", true),
		chromedp.Flag("disable-web-security", true),
		chromedp.Flag("ignore-certificate-errors", true),
		chromedp.Flag("allow-insecure-localhost", true),
		chromedp.Flag("autoplay-policy", "no-user-gesture-required"),
		chromedp.Flag("origin-to-force-quic-on", "127.0.0.1"),
		chromedp.Flag("enable-features", "WebTransport"),
	)
	for _, path := range []string{
		os.Getenv("CHROME_PATH"),
		"/usr/local/bin/google-chrome",
		"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
	} {
		if path == "" {
			continue
		}
		if _, err := os.Stat(path); err == nil {
			opts = append(opts, chromedp.ExecPath(path))
			break
		}
	}
	return opts
}

// TestBrowserConnectAndFork validates headless Chromium loads the UI,
// establishes WebTransport (connected pill), and fork works via HTTP.
// Steer publish-back is covered by TestWebTransportSteerCancel because
// some headless Chrome builds do not expose outbound uni streams reliably.
func TestBrowserConnectAndFork(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping browser test in short mode")
	}

	h, err := startControlRoomHarness(harnessOptions{FPS: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	allocCtx, cancel := chromedp.NewExecAllocator(context.Background(), chromeOpts()...)
	defer cancel()
	ctx, cancel2 := chromedp.NewContext(allocCtx, chromedp.WithLogf(t.Logf))
	defer cancel2()

	chromedp.ListenTarget(ctx, func(ev interface{}) {
		switch e := ev.(type) {
		case *runtime.EventConsoleAPICalled:
			args := make([]string, 0, len(e.Args))
			for _, a := range e.Args {
				if a.Value != nil {
					args = append(args, string(a.Value))
				} else {
					args = append(args, a.Description)
				}
			}
			t.Logf("[browser %s] %s", e.Type, strings.Join(args, " "))
		case *runtime.EventExceptionThrown:
			t.Logf("[browser exception] %s", e.ExceptionDetails.Error())
		}
	})

	var stateText string
	err = chromedp.Run(ctx,
		chromedp.Navigate(h.HTTPURL),
		chromedp.WaitVisible("#state", chromedp.ByID),
		chromedp.WaitReady("#state.ok", chromedp.ByQuery),
		chromedp.Text("#state", &stateText, chromedp.ByID),
	)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if stateText != "connected" {
		t.Fatalf("state=%q want connected", stateText)
	}

	time.Sleep(500 * time.Millisecond)

	var reasoning string
	err = chromedp.Run(ctx,
		chromedp.Click("#fork", chromedp.ByID),
		chromedp.PollFunction(`function(){
			return document.getElementById('reasoning').textContent.includes('[forked session');
		}`, nil,
			chromedp.WithPollingInterval(200*time.Millisecond), chromedp.WithPollingTimeout(5*time.Second)),
		chromedp.Text("#reasoning", &reasoning, chromedp.ByID),
	)
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	if !strings.Contains(reasoning, "[forked session") {
		t.Fatalf("reasoning missing fork marker: %q", reasoning)
	}
}
