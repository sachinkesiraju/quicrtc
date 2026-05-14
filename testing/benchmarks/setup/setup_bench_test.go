// Cold bring-up benchmark: time-to-ready for a session that can carry
// 4 logical streams (video + tokens + tool-calls + telemetry) at a
// ~50 ms RTT.
//
// Two arms:
//
//	quicrtc  — one QUIC/WebTransport session. Server pre-registers all
//	           four tracks. Client.Dial completes the QUIC + WT
//	           handshake + HELLO/SDP, then receives 4 ANNOUNCE frames
//	           on the control stream. Sample = Dial start → 4 tracks
//	           visible in RemoteTracks().
//
//	HTTP     — four independent httptest TLS servers (one per modality)
//	           wrapped by a per-connection delay shim that injects
//	           ~25 ms one-way latency on every Read and Write of the
//	           underlying TCP socket. That approximates 50 ms RTT for
//	           the SYN/SYN-ACK + ClientHello/ServerHello/Finished
//	           round trips. Two variants:
//	             serial     — issue 4 GETs back-to-back. Models the
//	                          naive "tokens stream, then tools call,
//	                          then telemetry beacon, then video peer
//	                          connection" status quo.
//	             concurrent — issue all 4 in parallel. Models a
//	                          sophisticated client that fans out the
//	                          handshakes.
//
// Routing for the quicrtc arm uses the existing loadgen UDP proxy with
// NetCond{OneWayDelay: 25ms} for symmetric ~50 ms RTT.
//
// n=30 per series; below the methodology floor of 100 because each
// trial spins up a fresh server + fresh proxy + four fresh TLS
// servers, which is expensive enough to make 100 a multi-minute run.
// The point of this bench is event-style honesty — the magnitudes are
// what matter, not the third decimal of the median.
package setup

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/cert"
	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/testing/benchmarks/internal/loadgen"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

const (
	setupN          = 30
	setupOneWay     = 25 * time.Millisecond // 50 ms RTT total
	setupDialTO     = 8 * time.Second
	setupReadyTO    = 3 * time.Second
	setupHTTPDialTO = 3 * time.Second
)

func TestColdBringUp(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cold bring-up in short mode")
	}

	t.Logf("=== Cold bring-up of 4 logical streams, ~%s RTT, n=%d ===",
		(2 * setupOneWay).String(), setupN)

	quicSamples := make([]time.Duration, 0, setupN)
	for i := 0; i < setupN; i++ {
		d, err := quicrtcBringUpOnce(t)
		if err != nil {
			t.Logf("quicrtc trial %d failed: %v", i, err)
			continue
		}
		quicSamples = append(quicSamples, d)
	}

	httpSerialSamples := make([]time.Duration, 0, setupN)
	httpConcSamples := make([]time.Duration, 0, setupN)
	for i := 0; i < setupN; i++ {
		serial, conc, err := httpBringUpOnce(t)
		if err != nil {
			t.Logf("http trial %d failed: %v", i, err)
			continue
		}
		httpSerialSamples = append(httpSerialSamples, serial)
		httpConcSamples = append(httpConcSamples, conc)
	}

	qs := summarize(quicSamples)
	hs := summarize(httpSerialSamples)
	hc := summarize(httpConcSamples)

	t.Logf("---")
	t.Logf("quicrtc:                     %s", qs)
	t.Logf("HTTP baseline (serial):      %s", hs)
	t.Logf("HTTP baseline (concurrent):  %s", hc)
	t.Logf("---")
	if qs.P50 > 0 && hs.P50 > 0 {
		t.Logf("speedup vs serial:     p50 %.2fx, p99 %.2fx",
			float64(hs.P50)/float64(qs.P50),
			float64(hs.P99)/float64(qs.P99))
	}
	if qs.P50 > 0 && hc.P50 > 0 {
		t.Logf("speedup vs concurrent: p50 %.2fx, p99 %.2fx",
			float64(hc.P50)/float64(qs.P50),
			float64(hc.P99)/float64(qs.P99))
	}
}

// quicrtcBringUpOnce stands up a fresh server, pre-registers 4 tracks
// of distinct Kinds, routes the client through a 25-ms-one-way UDP
// proxy, and measures Dial start → 4 ANNOUNCEs visible.
func quicrtcBringUpOnce(t *testing.T) (time.Duration, error) {
	t.Helper()
	bundle, err := cert.Generate(cert.Options{IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	if err != nil {
		return 0, fmt.Errorf("cert: %w", err)
	}
	srv, err := server.New(server.Config{
		Addr:         "127.0.0.1:0",
		ExternalHost: "127.0.0.1",
		CertBundle:   bundle,
		MaxSessions:  64,
		SDP:          wire.SDP{Codec: "noop", Width: 64, Height: 64, FPS: 30},
	})
	if err != nil {
		return 0, fmt.Errorf("server.New: %w", err)
	}

	srv.AddTrackSpec(server.TrackSpec{Name: "video", Kind: track.KindVideo})
	srv.AddTrackSpec(server.TrackSpec{Name: "tokens", Kind: track.KindTokens})
	srv.AddTrackSpec(server.TrackSpec{Name: "toolcalls", Kind: track.KindToolCalls})
	srv.AddTrackSpec(server.TrackSpec{Name: "telemetry", Kind: track.KindTelemetry})

	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer func() {
		srvCancel()
		_ = srv.Shutdown(context.Background())
	}()
	go func() { _ = srv.ListenAndServe(srvCtx) }()

	// Wait for server to bind.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		a := srv.Addr()
		if a != "" && a != "127.0.0.1:0" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	addr := srv.Addr()
	backend, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return 0, fmt.Errorf("resolve backend: %w", err)
	}

	proxyAddr, stopProxy, err := loadgen.StartUDPProxy(backend, loadgen.NetCond{
		OneWayDelay: setupOneWay,
	})
	if err != nil {
		return 0, fmt.Errorf("start proxy: %w", err)
	}
	defer stopProxy()

	shareURL := fmt.Sprintf("https://127.0.0.1:%d/wt", proxyAddr.Port)

	ctx, cancel := context.WithTimeout(context.Background(), setupDialTO)
	defer cancel()

	t0 := time.Now()
	cli, err := client.Dial(ctx, shareURL, client.Options{
		Slug:           srv.Slug(),
		CertHashB64URL: srv.CertHashB64(),
		HelloTimeout:   setupDialTO,
	})
	if err != nil {
		return 0, fmt.Errorf("dial: %w", err)
	}
	defer cli.Close()

	// Poll RemoteTracks() until 4 are present or we time out.
	readyDeadline := time.Now().Add(setupReadyTO)
	for time.Now().Before(readyDeadline) {
		if len(cli.RemoteTracks()) >= 4 {
			return time.Since(t0), nil
		}
		time.Sleep(time.Millisecond)
	}
	got := cli.RemoteTracks()
	return 0, fmt.Errorf("only %d/4 announces observed within %s: %v",
		len(got), setupReadyTO, got)
}

// delayConn wraps a net.Conn and inserts setupOneWay sleeps on every
// Read and Write boundary. Each Read or Write of a buffered chunk
// sleeps once, so a full TCP+TLS handshake (which exchanges several
// small frames) absorbs multiple one-way delays — close in shape to
// the 2 RTTs that a TCP+TLS handshake actually costs on a real path.
//
// This is the "per-connection net.Conn wrapper" approach. It's not
// perfect (it pessimizes large transfers and slightly overcounts the
// number of round trips for batched TLS records), but it's directional
// and same-shaped for both quicrtc (1 RTT QUIC handshake + HELLO) and
// the HTTP baseline.
type delayConn struct {
	net.Conn
	delay time.Duration
}

func (d *delayConn) Read(p []byte) (int, error) {
	time.Sleep(d.delay)
	return d.Conn.Read(p)
}

func (d *delayConn) Write(p []byte) (int, error) {
	time.Sleep(d.delay)
	return d.Conn.Write(p)
}

// httpBringUpOnce stands up 4 fresh httptest TLS servers (one per
// modality), then issues 4 GETs to "/ready" — serial and concurrent.
// All client TCP connections traverse a delayConn shim that injects
// setupOneWay on every Read and Write.
func httpBringUpOnce(t *testing.T) (time.Duration, time.Duration, error) {
	t.Helper()

	mkHandler := func(name string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("ready:" + name))
		})
	}

	servers := []*httptest.Server{
		httptest.NewTLSServer(mkHandler("video")),
		httptest.NewTLSServer(mkHandler("tokens")),
		httptest.NewTLSServer(mkHandler("toolcalls")),
		httptest.NewTLSServer(mkHandler("telemetry")),
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

	dialer := &net.Dialer{Timeout: setupHTTPDialTO}
	mkClient := func(s *httptest.Server) *http.Client {
		// Force a fresh TCP connection per request (no keep-alive
		// pooling) so each call pays the full handshake cost — the
		// thing we're trying to measure.
		tr := &http.Transport{
			DisableKeepAlives:   true,
			MaxIdleConnsPerHost: 0,
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				c, err := dialer.DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				return &delayConn{Conn: c, delay: setupOneWay}, nil
			},
		}
		return &http.Client{Transport: tr, Timeout: setupDialTO}
	}

	clients := make([]*http.Client, len(servers))
	for i, s := range servers {
		clients[i] = mkClient(s)
	}

	urls := make([]string, len(servers))
	for i, s := range servers {
		urls[i] = s.URL + "/ready"
	}

	// Serial.
	tSerial0 := time.Now()
	for i := range urls {
		if err := getOnce(clients[i], urls[i]); err != nil {
			return 0, 0, fmt.Errorf("serial[%d]: %w", i, err)
		}
	}
	serial := time.Since(tSerial0)

	// Concurrent. Fresh clients to avoid any residual state from
	// the serial pass. Each gets its own delayConn-wrapped Transport.
	concClients := make([]*http.Client, len(servers))
	for i, s := range servers {
		concClients[i] = mkClient(s)
	}
	tConc0 := time.Now()
	var wg sync.WaitGroup
	errs := make([]error, len(servers))
	for i := range urls {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = getOnce(concClients[i], urls[i])
		}(i)
	}
	wg.Wait()
	conc := time.Since(tConc0)
	for i, e := range errs {
		if e != nil {
			return 0, 0, fmt.Errorf("concurrent[%d]: %w", i, e)
		}
	}

	return serial, conc, nil
}

func getOnce(c *http.Client, url string) error {
	resp, err := c.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

// summary is a tiny stats helper. We deliberately don't pull in the
// loadgen.LatencyStats type because it formats with extra fields the
// headline doesn't need; this keeps the bench output narrow.
type summary struct {
	N    int
	Mean time.Duration
	P50  time.Duration
	P95  time.Duration
	P99  time.Duration
}

func (s summary) String() string {
	return fmt.Sprintf("n=%d  p50=%5s  p95=%5s  p99=%5s  mean=%5s",
		s.N,
		s.P50.Round(time.Millisecond),
		s.P95.Round(time.Millisecond),
		s.P99.Round(time.Millisecond),
		s.Mean.Round(time.Millisecond))
}

func summarize(xs []time.Duration) summary {
	if len(xs) == 0 {
		return summary{}
	}
	sorted := append([]time.Duration(nil), xs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pct := func(p float64) time.Duration {
		idx := int(float64(len(sorted)-1) * p)
		return sorted[idx]
	}
	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	return summary{
		N:    len(sorted),
		Mean: sum / time.Duration(len(sorted)),
		P50:  pct(0.50),
		P95:  pct(0.95),
		P99:  pct(0.99),
	}
}
