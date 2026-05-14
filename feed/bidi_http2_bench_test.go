package feed

import (
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
)

// TestBidiPerCallVsHTTP2Multiplex is the steelman comparison for
// "8 tool calls, 1 stalled 200 ms": instead of pitting QUIC-stream-
// per-call against a strawman single-stream serial pump, we pit it
// against the configuration a sane non-QUIC deployment would actually
// use — 8 concurrent HTTP/2 requests over a single TLS connection,
// with HTTP/2's per-stream flow control providing in-protocol
// multiplexing.
//
// The honest question this benchmark asks: on a healthy loopback
// connection (no packet loss, no congestion), does QUIC's stream-
// per-call design beat HTTP/2 multiplexing at median, or does it
// roughly tie? If they tie at median, the structural win we point to
// in the README ("20× faster than serial") is real but uninteresting
// — what actually differentiates QUIC is its behavior under loss
// (no TCP head-of-line blocking), which the WAN row of the bench
// table covers separately.
//
// Run: go test -v -run TestBidiPerCallVsHTTP2Multiplex ./feed -timeout=120s
func TestBidiPerCallVsHTTP2Multiplex(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping HTTP/2-mux steelman benchmark in short mode")
	}

	const (
		nCalls       = 8
		fastDelay    = 5 * time.Millisecond
		slowDelay    = 200 * time.Millisecond
		stallStream  = 1 // 1-indexed; first call stalls
		outerRepeats = 20
	)

	// One handler: read body, sleep per X-Stall-Ms header, write reply.
	// Loopback + tiny payloads, so the work here is dominated by the
	// sleep, mirroring the existing fakeBidiStream behavior where the
	// "fast" calls sleep fastDelay and the "stalled" one sleeps
	// slowDelay.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()

		stallMs := 0
		if v := r.Header.Get("X-Stall-Ms"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				stallMs = n
			}
		}
		if stallMs > 0 {
			time.Sleep(time.Duration(stallMs) * time.Millisecond)
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// httptest.NewUnstartedServer + StartTLS + explicit http2.ConfigureServer
	// is the most portable way to guarantee HTTP/2 is actually negotiated.
	// (httptest.NewTLSServer also works but doesn't always enable h2 across
	// Go versions; being explicit removes that ambiguity.)
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{NextProtos: []string{"h2"}}
	if err := http2.ConfigureServer(srv.Config, &http2.Server{}); err != nil {
		t.Fatalf("http2.ConfigureServer: %v", err)
	}
	srv.StartTLS()
	defer srv.Close()

	// Client: explicit http2.Transport so all 8 requests share a single
	// HTTP/2 connection (per-stream multiplexing, the steelman config).
	// AllowHTTP=false; TLSClientConfig skips verify since httptest cert
	// is self-signed.
	transport := &http2.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{"h2"},
		},
	}
	defer transport.CloseIdleConnections()

	client := &http.Client{Transport: transport}

	// One warm-up request to establish the connection so the very first
	// measured request doesn't pay the TLS handshake cost. This matches
	// the existing bidi benchmark, which exercises a pre-configured
	// fakeBidiOpener (no per-iteration handshake).
	{
		req, err := http.NewRequest("POST", srv.URL+"/call", nil)
		if err != nil {
			t.Fatalf("warmup request: %v", err)
		}
		req.Header.Set("X-Stall-Ms", "0")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("warmup do: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.ProtoMajor != 2 {
			t.Fatalf("expected HTTP/2 negotiation, got %s", resp.Proto)
		}
	}

	// Run outerRepeats × nCalls. Each outer iteration fires nCalls
	// concurrent requests; one is the stalled call (skipped from
	// reported samples), the other (nCalls-1) feed the latency vector.
	nonStalled := make([]time.Duration, 0, outerRepeats*(nCalls-1))
	stalledSamples := make([]time.Duration, 0, outerRepeats)

	for iter := 0; iter < outerRepeats; iter++ {
		var wg sync.WaitGroup
		latencies := make([]time.Duration, nCalls)
		errs := make([]error, nCalls)
		wg.Add(nCalls)
		for i := 0; i < nCalls; i++ {
			i := i
			go func() {
				defer wg.Done()
				stall := int(fastDelay / time.Millisecond)
				if i+1 == stallStream {
					stall = int(slowDelay / time.Millisecond)
				}
				req, err := http.NewRequest("POST", srv.URL+"/call",
					reqBody("request-payload"))
				if err != nil {
					errs[i] = err
					return
				}
				req.Header.Set("X-Stall-Ms", strconv.Itoa(stall))
				t0 := time.Now()
				resp, err := client.Do(req)
				if err != nil {
					errs[i] = err
					return
				}
				_, err = io.Copy(io.Discard, resp.Body)
				if err != nil {
					_ = resp.Body.Close()
					errs[i] = err
					return
				}
				_ = resp.Body.Close()
				latencies[i] = time.Since(t0)
			}()
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("iter %d call %d: %v", iter, i, err)
			}
		}
		for i, d := range latencies {
			if i+1 == stallStream {
				stalledSamples = append(stalledSamples, d)
			} else {
				nonStalled = append(nonStalled, d)
			}
		}
	}

	nonStats := computeLatStats(nonStalled)
	stallStats := computeLatStats(stalledSamples)

	t.Logf("=== HTTP/2-multiplexed steelman, 8 calls, 1 stalled 200 ms, %d iters ===", outerRepeats)
	t.Logf("HTTP/2 non-stalled (7 of 8 per iter)  %s", nonStats)
	t.Logf("HTTP/2 stalled    (1 of 8 per iter)   %s", stallStats)
	t.Logf("=== headline ===")
	t.Logf("HTTP/2 mux, median (non-stalled): %v", nonStats.P50.Round(time.Millisecond))
	t.Logf("HTTP/2 mux, p99    (non-stalled): %v", nonStats.P99.Round(time.Millisecond))
	t.Logf("HTTP/2 mux, max    (non-stalled): %v", nonStats.Max.Round(time.Millisecond))
	t.Logf("Compare to BidiPerCall: ~13 ms median in TestBidiPerCallIsolation")
}

// reqBody returns an io.Reader for a fixed string body. Wrapped in a
// helper so each goroutine gets its own Reader (a shared bytes.Reader
// would be drained on first use and starve later requests).
func reqBody(s string) io.Reader {
	return &stringReader{s: s}
}

type stringReader struct {
	s string
	n int
}

func (r *stringReader) Read(p []byte) (int, error) {
	if r.n >= len(r.s) {
		return 0, io.EOF
	}
	n := copy(p, r.s[r.n:])
	r.n += n
	return n, nil
}

