// Real-QUIC datagram benchmark: proves the structural claim that
// kind-aware telemetry over datagrams doesn't queue behind concurrent
// video bytes on the same connection.
//
// Setup:
//   - One QUIC connection (server + client over loopback).
//   - Publisher streams large video frames over a reliable uni stream.
//     The stream saturates with chunky data, exactly what telemetry
//     would normally have to wait behind.
//   - Concurrently, publisher sends small telemetry payloads via TWO
//     paths and the test compares their arrival latency:
//     · BASELINE: same reliable uni-stream pipe as video
//     · PR3:      raw QUIC datagrams via SessionHandle.SendDatagram
//     and Client.ReceiveDatagram
//
// Expected result: the datagram path's per-AU latency is roughly
// constant under load; the reliable-stream path's latency rises in
// step with video buffer occupancy.
package telemetry

import (
	"context"
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/testing/benchmarks/internal/loadgen"
	"github.com/sachinkesiraju/quicrtc/cert"
	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/wire"
)

const (
	dgVideoFrames   = 60                    // ~2s @ 30fps
	dgVideoSize     = 50 * 1024             // 50 KB per frame
	dgVideoPace     = 33 * time.Millisecond // 30 fps
	dgTelemetryN    = 200                   // telemetry samples in the same window
	dgTelemetryPace = 10 * time.Millisecond // 100 Hz
	dgTelemetrySize = 48                    // 8B timestamp + 40B filler
	dgRealTimeout   = 30 * time.Second
)

func makeDgPayload(size int, t time.Time) []byte {
	b := make([]byte, size)
	binary.BigEndian.PutUint64(b[:8], uint64(t.UnixNano()))
	for i := 8; i < size; i++ {
		b[i] = 0xa5
	}
	return b
}

func decodeDgTimestamp(b []byte) time.Time {
	if len(b) < 8 {
		return time.Time{}
	}
	return time.Unix(0, int64(binary.BigEndian.Uint64(b[:8])))
}

func runDatagramContention(t *testing.T, useDatagrams bool) ([]time.Duration, int) {
	t.Helper()
	bundle, err := cert.Generate(cert.Options{IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	if err != nil {
		t.Fatal(err)
	}

	handleCh := make(chan server.SessionHandle, 1)
	srv, err := server.New(server.Config{
		Addr:       "127.0.0.1:0",
		CertBundle: bundle,
		SDP:        wire.SDP{Codec: "test", Width: 64, Height: 64, FPS: 30},
		OnSession: func(h server.SessionHandle) {
			select {
			case handleCh <- h:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	srvCtx, srvDone := context.WithCancel(context.Background())
	go func() { _ = srv.ListenAndServe(srvCtx) }()
	defer srvDone()
	for srv.Addr() == "" || srv.Addr()[len(srv.Addr())-2:] == ":0" {
		time.Sleep(time.Millisecond)
	}

	videoPub := srv.AddTrack("video")
	telemetryPub := srv.AddTrack("telemetry") // only used in baseline

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	cli, err := client.Dial(dialCtx, "https://"+srv.Addr()+"/wt", client.Options{
		Slug:           srv.Slug(),
		CertHashB64URL: srv.CertHashB64(),
		HelloTimeout:   3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	var handle server.SessionHandle
	select {
	case handle = <-handleCh:
	case <-time.After(5 * time.Second):
		t.Fatal("OnSession never fired")
	}

	recvCtx, recvCancel := context.WithCancel(context.Background())
	defer recvCancel()
	var videoWg sync.WaitGroup
	videoWg.Add(1)
	go func() {
		defer videoWg.Done()
		for {
			_, err := cli.RecvOn(recvCtx, "video")
			if err != nil {
				return
			}
		}
	}()

	telemetryLat := make([]time.Duration, 0, dgTelemetryN)
	var lMu sync.Mutex
	var receivedCount int
	done := make(chan struct{})

	go func() {
		defer close(done)
		for i := 0; i < dgTelemetryN; i++ {
			var payload []byte
			if useDatagrams {
				dg, err := cli.ReceiveDatagram(recvCtx)
				if err != nil {
					return
				}
				_, _, p, err := wire.DecodeDatagram(dg)
				if err != nil {
					continue
				}
				payload = p
			} else {
				au, err := cli.RecvOn(recvCtx, "telemetry")
				if err != nil {
					return
				}
				payload = au.Bytes
			}
			arrived := time.Now()
			sent := decodeDgTimestamp(payload)
			lMu.Lock()
			telemetryLat = append(telemetryLat, arrived.Sub(sent))
			receivedCount++
			lMu.Unlock()
		}
	}()

	videoDone := make(chan struct{})
	go func() {
		defer close(videoDone)
		ticker := time.NewTicker(dgVideoPace)
		defer ticker.Stop()
		for i := 0; i < dgVideoFrames; i++ {
			payload := makeDgPayload(dgVideoSize, time.Now())
			_ = videoPub.Publish(context.Background(), pubsub.AccessUnit{
				Bytes:    payload,
				Keyframe: i%30 == 0,
				Seq:      uint32(i),
				PTSMicro: uint64(i) * 33000,
			})
			if i < dgVideoFrames-1 {
				<-ticker.C
			}
		}
	}()

	telemetryProducerDone := make(chan struct{})
	go func() {
		defer close(telemetryProducerDone)
		ticker := time.NewTicker(dgTelemetryPace)
		defer ticker.Stop()
		for i := 0; i < dgTelemetryN; i++ {
			payload := makeDgPayload(dgTelemetrySize, time.Now())
			if useDatagrams {
				dg, err := wire.EncodeDatagram(nil, 0x42, uint16(i), payload)
				if err == nil {
					_ = handle.SendDatagram(dg)
				}
			} else {
				_ = telemetryPub.Publish(context.Background(), pubsub.AccessUnit{
					Bytes:    payload,
					Keyframe: true,
					Seq:      uint32(i),
					PTSMicro: uint64(i) * 1000,
				})
			}
			if i < dgTelemetryN-1 {
				<-ticker.C
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(dgRealTimeout):
		t.Errorf("telemetry receiver timeout: got %d of %d", receivedCount, dgTelemetryN)
	}
	<-telemetryProducerDone
	<-videoDone
	recvCancel()
	videoWg.Wait()

	return telemetryLat, receivedCount
}

// TestDatagramVsStreamContention is the real-QUIC validation for PR3.
// Compares telemetry latency under concurrent video load between two
// paths on the same connection: reliable stream vs raw datagrams.
//
//	go test -v ./benchmarks/telemetry -run TestDatagramVsStreamContention
func TestDatagramVsStreamContention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping datagram contention test in short mode")
	}

	t.Log("running BASELINE: telemetry over reliable stream, concurrent with video...")
	streamLat, streamRecv := runDatagramContention(t, false)
	streamStats := loadgen.ComputeStats(streamLat)

	t.Log("running PR3: telemetry over QUIC datagrams, concurrent with video...")
	dgLat, dgRecv := runDatagramContention(t, true)
	dgStats := loadgen.ComputeStats(dgLat)

	t.Logf("=== real-QUIC datagram-vs-stream contention, %d telemetry @ %v during %d video frames @ %v ===",
		dgTelemetryN, dgTelemetryPace, dgVideoFrames, dgVideoPace)
	t.Logf("BASELINE (reliable stream)  recv=%d/%d  %s", streamRecv, dgTelemetryN, streamStats)
	t.Logf("PR3 (raw QUIC datagrams)    recv=%d/%d  %s", dgRecv, dgTelemetryN, dgStats)

	if streamStats.Mean > 0 && dgStats.Mean > 0 {
		t.Logf("=== headline ===")
		t.Logf("mean telemetry latency:  %v  ->  %v   (%.1f%% reduction)",
			streamStats.Mean.Round(time.Microsecond),
			dgStats.Mean.Round(time.Microsecond),
			loadgen.PercentReduction(streamStats.Mean, dgStats.Mean))
		t.Logf("p99 telemetry latency:   %v  ->  %v   (%.1f%% reduction)",
			streamStats.P99.Round(time.Microsecond),
			dgStats.P99.Round(time.Microsecond),
			loadgen.PercentReduction(streamStats.P99, dgStats.P99))
	}
}
