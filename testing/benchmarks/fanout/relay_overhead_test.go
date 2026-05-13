// Relay-overhead benchmark: isolates the latency cost of the
// native-wire relay path.
//
// Two parallel topologies receive the same workload:
//
//	NATIVE:  publisher  ──QUIC──►  subscriber_native
//
//	RELAYED: publisher  ──QUIC──►  relay  ──QUIC──►  subscriber_relay
//
// The publisher emits paced AUs. Both subscribers receive every AU
// (the relay subscribes upstream and republishes to its downstream
// server). Per-AU end-to-end latency is measured on each path. The
// overhead is the difference between the relayed and native paths
// for the same AU.
package fanout

import (
	"context"
	"encoding/binary"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/cert"
	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	relaylib "github.com/sachinkesiraju/quicrtc/relay"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/wire"
)

const (
	relayOverheadN       = 500
	relayOverheadPace    = 2 * time.Millisecond
	relayOverheadPayload = 256
	relayOverheadTimeout = 30 * time.Second
)

// TestRelayOverhead measures the extra latency a subscriber pays for
// going through the native relay vs connecting directly to the
// publisher. Reports per-path stats and the path-paired overhead.
func TestRelayOverhead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping relay overhead bench in short mode")
	}

	bundle, err := cert.Generate(cert.Options{IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	upstream, err := server.New(server.Config{
		Addr:         "127.0.0.1:0",
		ExternalHost: "127.0.0.1",
		CertBundle:   bundle,
		SDP:          wire.SDP{Codec: "test", Width: 64, Height: 64, FPS: 30},
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = upstream.ListenAndServe(ctx) }()
	for upstream.Addr() == "127.0.0.1:0" {
		time.Sleep(5 * time.Millisecond)
	}
	defer func() {
		sCtx, sCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer sCancel()
		_ = upstream.Shutdown(sCtx)
	}()

	upstreamPub := upstream.AddTrack("primary")
	upstreamShareLink := upstream.ShareLink()

	relayBundle, err := cert.Generate(cert.Options{IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	if err != nil {
		t.Fatal(err)
	}
	r, err := relaylib.New(relaylib.Config{
		Server: server.Config{
			Addr:         "127.0.0.1:0",
			ExternalHost: "127.0.0.1",
			CertBundle:   relayBundle,
			SDP:          wire.SDP{Codec: "test", Width: 64, Height: 64, FPS: 30},
		},
		Upstreams: []string{upstreamShareLink},
		ClientOptions: client.Options{
			HelloTimeout: 5 * time.Second,
		},
		ReconnectInterval:    1 * time.Second,
		MaxReconnectAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	relayDone := make(chan error, 1)
	go func() { relayDone <- r.ListenAndServe(ctx) }()
	for r.Server().Addr() == "127.0.0.1:0" {
		time.Sleep(5 * time.Millisecond)
	}
	defer func() {
		sCtx, sCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer sCancel()
		_ = r.Shutdown(sCtx)
	}()

	relayShareLink := r.Server().ShareLink()
	t.Logf("upstream=%s relay=%s", upstream.Addr(), r.Server().Addr())

	time.Sleep(200 * time.Millisecond)

	dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
	defer dialCancel()

	subNative, err := client.Dial(dialCtx, upstreamShareLink, client.Options{
		HelloTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal("native subscriber dial:", err)
	}
	defer subNative.Close()

	subRelay, err := client.Dial(dialCtx, relayShareLink, client.Options{
		HelloTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal("relay subscriber dial:", err)
	}
	defer subRelay.Close()

	type sample struct {
		seq uint32
		ms  float64
	}
	nativeCh := make(chan sample, relayOverheadN+16)
	relayCh := make(chan sample, relayOverheadN+16)

	recvCtx, recvCancel := context.WithCancel(ctx)
	defer recvCancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			au, err := subNative.Recv(recvCtx)
			if err != nil {
				return
			}
			if len(au.Bytes) < 12 {
				continue
			}
			seq := binary.BigEndian.Uint32(au.Bytes[:4])
			sentNs := int64(binary.BigEndian.Uint64(au.Bytes[4:12]))
			nativeCh <- sample{seq: seq, ms: float64(time.Now().UnixNano()-sentNs) / 1e6}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			au, err := subRelay.Recv(recvCtx)
			if err != nil {
				return
			}
			if len(au.Bytes) < 12 {
				continue
			}
			seq := binary.BigEndian.Uint32(au.Bytes[:4])
			sentNs := int64(binary.BigEndian.Uint64(au.Bytes[4:12]))
			relayCh <- sample{seq: seq, ms: float64(time.Now().UnixNano()-sentNs) / 1e6}
		}
	}()

	ticker := time.NewTicker(relayOverheadPace)
	defer ticker.Stop()
	for i := 0; i < relayOverheadN; i++ {
		payload := make([]byte, relayOverheadPayload)
		binary.BigEndian.PutUint32(payload[:4], uint32(i))
		binary.BigEndian.PutUint64(payload[4:12], uint64(time.Now().UnixNano()))
		_ = upstreamPub.Publish(ctx, pubsub.AccessUnit{
			Bytes:    payload,
			Keyframe: i == 0 || i%30 == 0,
			Seq:      uint32(i),
			PTSMicro: uint64(i) * 2000,
		})
		if i < relayOverheadN-1 {
			<-ticker.C
		}
	}

	nativeBySeq := map[uint32]float64{}
	relayBySeq := map[uint32]float64{}
	collectDeadline := time.After(relayOverheadTimeout)

collect:
	for len(nativeBySeq) < relayOverheadN || len(relayBySeq) < relayOverheadN {
		select {
		case s := <-nativeCh:
			if _, ok := nativeBySeq[s.seq]; !ok {
				nativeBySeq[s.seq] = s.ms
			}
		case s := <-relayCh:
			if _, ok := relayBySeq[s.seq]; !ok {
				relayBySeq[s.seq] = s.ms
			}
		case <-collectDeadline:
			t.Logf("relay-overhead collect timeout: native=%d/%d, relay=%d/%d",
				len(nativeBySeq), relayOverheadN, len(relayBySeq), relayOverheadN)
			break collect
		}
	}
	recvCancel()
	wg.Wait()

	var nativeLats, relayLats, perAUOverhead []float64
	for seq := uint32(0); seq < uint32(relayOverheadN); seq++ {
		n, hasN := nativeBySeq[seq]
		r, hasR := relayBySeq[seq]
		if hasN {
			nativeLats = append(nativeLats, n)
		}
		if hasR {
			relayLats = append(relayLats, r)
		}
		if hasN && hasR {
			perAUOverhead = append(perAUOverhead, r-n)
		}
	}

	if len(perAUOverhead) < relayOverheadN/2 {
		t.Fatalf("too few paired samples: %d (want >= %d)", len(perAUOverhead), relayOverheadN/2)
	}

	nativeStats := computeRelayStats(nativeLats)
	relayStats := computeRelayStats(relayLats)
	overheadStats := computeRelayStats(perAUOverhead)

	t.Logf("===== relay overhead =====")
	t.Logf("paired samples: %d / %d", len(perAUOverhead), relayOverheadN)
	t.Logf("native subscriber path:   %s", nativeStats)
	t.Logf("relayed subscriber path:  %s", relayStats)
	t.Logf("per-AU overhead (relay - native):")
	t.Logf("  %s", overheadStats)
	t.Logf("===== headline =====")
	t.Logf("mean overhead added by relay path: %.3fms", overheadStats.mean)
	t.Logf("p99  overhead added by relay path: %.3fms", overheadStats.p99)
}

type relayStats struct {
	n                       int
	min, p50, p95, p99, max float64
	mean, stddev            float64
}

func (s relayStats) String() string {
	return formatRelayStats(s)
}

func formatRelayStats(s relayStats) string {
	return "n=" + itoaRelay(s.n) +
		" min=" + ftoaRelay(s.min) +
		" p50=" + ftoaRelay(s.p50) +
		" p95=" + ftoaRelay(s.p95) +
		" p99=" + ftoaRelay(s.p99) +
		" max=" + ftoaRelay(s.max) +
		" mean=" + ftoaRelay(s.mean) +
		" stddev=" + ftoaRelay(s.stddev) + "ms"
}

func computeRelayStats(xs []float64) relayStats {
	if len(xs) == 0 {
		return relayStats{}
	}
	sorted := append([]float64(nil), xs...)
	sort.Float64s(sorted)
	pct := func(p float64) float64 { return sorted[int(float64(len(sorted)-1)*p)] }

	var sum float64
	for _, v := range sorted {
		sum += v
	}
	mean := sum / float64(len(sorted))
	var sq float64
	for _, v := range sorted {
		sq += (v - mean) * (v - mean)
	}
	sd := 0.0
	if len(sorted) > 1 {
		sd = sqrtRelay(sq / float64(len(sorted)-1))
	}
	return relayStats{
		n:   len(sorted),
		min: sorted[0], max: sorted[len(sorted)-1],
		p50: pct(0.5), p95: pct(0.95), p99: pct(0.99),
		mean: mean, stddev: sd,
	}
}

func sqrtRelay(x float64) float64 {
	if x <= 0 {
		return 0
	}
	z := x
	for i := 0; i < 8; i++ {
		z = z - (z*z-x)/(2*z)
	}
	return z
}

func itoaRelay(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func ftoaRelay(f float64) string {
	neg := ""
	if f < 0 {
		neg = "-"
		f = -f
	}
	whole := int(f)
	frac := int((f - float64(whole)) * 1000)
	return neg + itoaRelay(whole) + "." +
		paddedRelay(frac, 3)
}

func paddedRelay(n, w int) string {
	s := itoaRelay(n)
	for len(s) < w {
		s = "0" + s
	}
	return s
}
