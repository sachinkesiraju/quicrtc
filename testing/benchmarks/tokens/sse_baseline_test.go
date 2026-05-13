// SSE-over-HTTP/2 token-streaming baseline. The industry standard
// for LLM token streaming (used by OpenAI, Anthropic, every major
// inference platform). We measure it head-to-head against quicrtc's
// StreamLowLatency path so the comparison is defensible.
//
// Methodology:
//   - HTTP/2 + TLS server (Go's net/http; HTTP/2 is default with TLS).
//   - Each event: "data: <8B BE timestamp><filler>\n\n" — same
//     payload size as the quicrtc token bench so wire-byte cost is
//     comparable.
//   - Client uses net/http with a TLS config that trusts the test
//     cert; reads the response body line-by-line.
//   - Same pace, same N, same payload size as the quicrtc real-QUIC
//     token bench.
package tokens

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/sachinkesiraju/quicrtc/testing/benchmarks/internal/loadgen"
	"github.com/sachinkesiraju/quicrtc/cert"
)

const (
	sseN       = 500
	ssePayload = 64
	ssePace    = 2 * time.Millisecond
	sseTimeout = 30 * time.Second
)

func runSSEStream(t *testing.T) loadgen.TokenStreamResult {
	t.Helper()

	bundle, err := cert.Generate(cert.Options{IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	if err != nil {
		t.Fatal(err)
	}

	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{bundle.TLS},
		NextProtos:   []string{"h2"},
	})
	if err != nil {
		t.Fatal(err)
	}

	serverReady := make(chan struct{})
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()

	mux := http.NewServeMux()
	mux.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		flusher.Flush()

		buf := make([]byte, ssePayload)
		for i := 8; i < ssePayload; i++ {
			buf[i] = 0xab
		}
		ticker := time.NewTicker(ssePace)
		defer ticker.Stop()
		for i := 0; i < sseN; i++ {
			binary.BigEndian.PutUint64(buf[:8], uint64(time.Now().UnixNano()))
			fmt.Fprintf(w, "data: %s\n\n", hex.EncodeToString(buf))
			flusher.Flush()
			if i < sseN-1 {
				select {
				case <-r.Context().Done():
					return
				case <-ticker.C:
				}
			}
		}
	})

	srv := &http.Server{Handler: mux}
	if err := http2.ConfigureServer(srv, &http2.Server{}); err != nil {
		t.Fatal(err)
	}

	go func() {
		close(serverReady)
		_ = srv.Serve(listener)
	}()
	<-serverReady
	defer srv.Shutdown(srvCtx)

	pool := x509.NewCertPool()
	if leafCert, err := x509.ParseCertificate(bundle.TLS.Certificate[0]); err == nil {
		pool.AddCert(leafCert)
	}
	clientTLS := &tls.Config{
		RootCAs:    pool,
		NextProtos: []string{"h2"},
		ServerName: "127.0.0.1",
	}
	transport := &http2.Transport{
		TLSClientConfig: clientTLS,
	}
	httpClient := &http.Client{
		Transport: transport,
		Timeout:   0,
	}

	addr := listener.Addr().String()
	reqCtx, reqCancel := context.WithTimeout(context.Background(), sseTimeout)
	defer reqCancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet,
		"https://"+addr+"/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	endToEnd := make([]time.Duration, 0, sseN)
	interArrival := make([]time.Duration, 0, sseN)
	var firstRecv, lastRecv, prev time.Time

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 16*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		hexData := line[6:]
		raw, err := hex.DecodeString(hexData)
		if err != nil || len(raw) < 8 {
			continue
		}
		arrived := time.Now()
		sentNs := binary.BigEndian.Uint64(raw[:8])
		sent := time.Unix(0, int64(sentNs))
		endToEnd = append(endToEnd, arrived.Sub(sent))
		if len(endToEnd) == 1 {
			firstRecv = arrived
		}
		if !prev.IsZero() {
			interArrival = append(interArrival, arrived.Sub(prev))
		}
		prev = arrived
		lastRecv = arrived
		if len(endToEnd) >= sseN {
			break
		}
	}

	return loadgen.TokenStreamResult{
		EndToEnd:     loadgen.ComputeStats(endToEnd),
		InterArrival: loadgen.ComputeStats(interArrival),
		TotalTime:    lastRecv.Sub(firstRecv),
	}
}

// TestTokenStreamingVsSSE is the competitor-baseline validation:
// quicrtc's StreamLowLatency token path head-to-head against
// SSE-over-HTTP/2 (the industry-standard LLM token streaming protocol)
// over real loopback TLS.
//
//	go test -v ./benchmarks/tokens -run TestTokenStreamingVsSSE
func TestTokenStreamingVsSSE(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SSE comparison in short mode")
	}

	t.Log("running SSE-over-HTTP/2 baseline...")
	sseRes := runSSEStream(t)

	t.Log("running quicrtc StreamLowLatency...")
	llRes := runRealTokenStreamMatching(t)

	t.Logf("=== SSE-over-HTTP/2 vs quicrtc StreamLowLatency, %d events, %v pace, loopback TLS ===",
		sseN, ssePace)
	t.Logf("SSE/HTTP-2 (industry baseline)  %s", sseRes.Summary())
	t.Logf("quicrtc StreamLowLatency        %s", llRes.Summary())

	improveMean := loadgen.PercentReduction(sseRes.EndToEnd.Mean, llRes.EndToEnd.Mean)
	improveP50 := loadgen.PercentReduction(sseRes.EndToEnd.P50, llRes.EndToEnd.P50)
	improveP99 := loadgen.PercentReduction(sseRes.EndToEnd.P99, llRes.EndToEnd.P99)
	t.Logf("=== headline (end-to-end latency, over real TLS) ===")
	t.Logf("mean:  %v (SSE)  ->  %v (quicrtc)   (%.1f%% %s)",
		sseRes.EndToEnd.Mean.Round(time.Microsecond),
		llRes.EndToEnd.Mean.Round(time.Microsecond),
		absPercent(improveMean), winLossLabel(improveMean))
	t.Logf("p50:   %v (SSE)  ->  %v (quicrtc)   (%.1f%% %s)",
		sseRes.EndToEnd.P50.Round(time.Microsecond),
		llRes.EndToEnd.P50.Round(time.Microsecond),
		absPercent(improveP50), winLossLabel(improveP50))
	t.Logf("p99:   %v (SSE)  ->  %v (quicrtc)   (%.1f%% %s)",
		sseRes.EndToEnd.P99.Round(time.Microsecond),
		llRes.EndToEnd.P99.Round(time.Microsecond),
		absPercent(improveP99), winLossLabel(improveP99))
}

func absPercent(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

func winLossLabel(v float64) string {
	if v > 0 {
		return "faster"
	}
	if v < 0 {
		return "slower"
	}
	return "tied"
}

func runRealTokenStreamMatching(t *testing.T) loadgen.TokenStreamResult {
	t.Helper()
	pair, err := loadgen.NewNativePair()
	if err != nil {
		t.Fatal(err)
	}
	defer pair.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := pair.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	pubDC, err := pair.PubPC.DataChannel()
	if err != nil {
		t.Fatal(err)
	}
	subDC, err := pair.SubPC.DataChannel()
	if err != nil {
		t.Fatal(err)
	}

	recvCh := make(chan []byte, sseN+16)
	recvCtx, recvCancel := context.WithCancel(context.Background())
	defer recvCancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			msg, err := subDC.Recv(recvCtx)
			if err != nil {
				return
			}
			recvCh <- msg
		}
	}()

	endToEndSamples := make([]time.Duration, 0, sseN)
	interArrivalSamples := make([]time.Duration, 0, sseN)
	var firstRecv, lastRecv time.Time
	collected := make(chan struct{})

	go func() {
		var prev time.Time
		for i := 0; i < sseN; i++ {
			select {
			case msg := <-recvCh:
				now := time.Now()
				sentAt := loadgen.DecodeStamp(msg)
				endToEndSamples = append(endToEndSamples, now.Sub(sentAt))
				if i == 0 {
					firstRecv = now
				}
				if !prev.IsZero() {
					interArrivalSamples = append(interArrivalSamples, now.Sub(prev))
				}
				prev = now
				lastRecv = now
			case <-time.After(sseTimeout):
				t.Errorf("matching-stream receiver timeout at %d", i)
				close(collected)
				return
			}
		}
		close(collected)
	}()

	ticker := time.NewTicker(ssePace)
	defer ticker.Stop()
	for i := 0; i < sseN; i++ {
		if err := pubDC.Send(loadgen.MakeTokenPayload(time.Now())); err != nil {
			t.Fatal(err)
		}
		if i < sseN-1 {
			<-ticker.C
		}
	}
	<-collected
	return loadgen.TokenStreamResult{
		EndToEnd:     loadgen.ComputeStats(endToEndSamples),
		InterArrival: loadgen.ComputeStats(interArrivalSamples),
		TotalTime:    lastRecv.Sub(firstRecv),
	}
}
