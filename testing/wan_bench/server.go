package main

// WAN server: brings up quicrtc native, raw WebTransport, and Pion
// WebRTC signaling, all on well-known ports. Exposes /info on :8090
// returning JSON the client uses to bootstrap.
//
// Run on the "server" cloud VM:
//   ./wan_bench -mode=server -external-host=<server-public-ip>

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/pion/webrtc/v4/pkg/media/ivfreader"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"

	"github.com/sachinkesiraju/quicrtc/cert"
	"github.com/sachinkesiraju/quicrtc/datachannel"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	qserver "github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// runServer brings up all three protocol servers. blocks until ctx done.
func runServer(ctx context.Context, externalHost string, ivfPath string) error {
	hostIP := net.ParseIP(externalHost)
	certOpts := cert.Options{
		IPs: []net.IP{net.ParseIP("127.0.0.1")},
	}
	if hostIP != nil {
		certOpts.IPs = append(certOpts.IPs, hostIP)
	} else {
		certOpts.DNSNames = []string{externalHost}
	}
	bundle, err := cert.Generate(certOpts)
	if err != nil {
		return fmt.Errorf("cert: %w", err)
	}

	// 1. quicrtc native server on :4433
	qsrv, err := qserver.New(qserver.Config{
		Addr:         "0.0.0.0:4433",
		ExternalHost: externalHost,
		CertBundle:   bundle,
		MaxSessions:  10000,
		SDP:          wire.SDP{Codec: "vp8", Width: 1280, Height: 720, FPS: 30},
		OnDataChannel: func(dc *datachannel.Channel) {
			// Push a "hello" + a stream of timestamped tokens for the
			// multi-modal test. Client consumes via DataChannel.Recv.
			_ = dc.Send([]byte("hello"))
			go func() {
				tick := time.NewTicker(5 * time.Millisecond)
				defer tick.Stop()
				for i := 0; i < 400; i++ {
					select {
					case <-ctx.Done():
						return
					case <-tick.C:
					}
					p := make([]byte, 64)
					stampNs(p, time.Now().UnixNano())
					_ = dc.Send(p)
				}
			}()
		},
	})
	if err != nil {
		return fmt.Errorf("quicrtc server.New: %w", err)
	}
	go func() {
		if err := qsrv.ListenAndServe(ctx); err != nil {
			fmt.Println("quicrtc server:", err)
		}
	}()

	// quicrtc burst publisher (50KB AU at 30 fps, looping)
	go func() {
		pub := qsrv.Publisher()
		burst := make([]byte, 50*1024)
		for ctx.Err() == nil {
			tick := time.NewTicker(33 * time.Millisecond)
			for i := 0; i < 60 && ctx.Err() == nil; i++ {
				<-tick.C
				stampNs(burst, time.Now().UnixNano())
				_ = pub.Publish(ctx, pubsub.AccessUnit{
					Bytes:    burst,
					Keyframe: i == 0,
					PTSMicro: uint64(time.Now().UnixMicro()),
					Seq:      uint32(i),
				})
			}
			tick.Stop()
		}
	}()

	// 2. Raw WebTransport on :4434
	if err := startRawWT(ctx, externalHost, bundle); err != nil {
		return fmt.Errorf("raw wt: %w", err)
	}

	// 3. Pion WebRTC signaling on :8080
	startPionSignaling(ctx, ivfPath)

	// 4. /info endpoint on :8090
	mux := http.NewServeMux()
	mux.HandleFunc("/info", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"externalHost":      externalHost,
			"quicrtcShareLink":  qsrv.ShareLink(),
			"quicrtcCertHashB64": qsrv.CertHashB64(),
			"quicrtcSlug":       qsrv.Slug(),
			"webtransportURL":   fmt.Sprintf("https://%s:4434/wt", externalHost),
			"webtransportCertHashB64": qsrv.CertHashB64(),
			"webrtcSignaling":   fmt.Sprintf("http://%s:8080/sig", externalHost),
		})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	})
	infoLn, err := net.Listen("tcp", "0.0.0.0:8090")
	if err != nil {
		return fmt.Errorf("info listener: %w", err)
	}
	go http.Serve(infoLn, mux)
	fmt.Printf("server ready: ext=%s\n  quicrtc:      %s\n  webtransport: https://%s:4434/wt\n  webrtc-sig:   http://%s:8080/sig\n  info:         http://%s:8090/info\n",
		externalHost, qsrv.ShareLink(), externalHost, externalHost, externalHost)
	<-ctx.Done()
	return nil
}

// startRawWT serves a raw WebTransport server on :4434 that sends a
// length-prefixed "hello" via uni stream on every connect.
func startRawWT(ctx context.Context, externalHost string, bundle *cert.Bundle) error {
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{bundle.TLS},
		NextProtos:   []string{"h3"},
	}
	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 4434})
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	h3 := &http3.Server{
		Addr: "0.0.0.0:4434", Handler: mux, TLSConfig: tlsCfg,
		EnableDatagrams: true,
		QUICConfig:      &quic.Config{EnableDatagrams: true},
	}
	wts := &webtransport.Server{
		H3:          h3,
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	webtransport.ConfigureHTTP3Server(h3)

	mux.HandleFunc("/wt", func(w http.ResponseWriter, r *http.Request) {
		sess, err := wts.Upgrade(w, r)
		if err != nil {
			return
		}
		defer sess.CloseWithError(0, "")
		stream, err := sess.OpenUniStreamSync(r.Context())
		if err != nil {
			return
		}
		hello := []byte("hello")
		hdr := []byte{0, 0, 0, byte(len(hello))}
		if _, err := stream.Write(append(hdr, hello...)); err != nil {
			return
		}
		<-r.Context().Done()
		_ = stream.Close()
	})
	go func() { _ = wts.Serve(udpConn) }()
	go func() {
		<-ctx.Done()
		_ = wts.Close()
		_ = udpConn.Close()
	}()
	return nil
}

// startPionSignaling brings up the WebRTC signaling on :8080.
// Each /sig POST creates a new PC, optionally adds a video track
// pumping from input.ivf if ?video=1.
func startPionSignaling(ctx context.Context, ivfPath string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/sig", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.Method == "OPTIONS" {
			return
		}
		isVideo := r.URL.Query().Get("video") == "1"
		isMulti := r.URL.Query().Get("multi") == "1"
		var offer webrtc.SessionDescription
		if err := json.NewDecoder(r.Body).Decode(&offer); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		pc, err := webrtc.NewPeerConnection(webrtc.Configuration{ICEServers: []webrtc.ICEServer{}})
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		pc.OnDataChannel(func(dc *webrtc.DataChannel) {
			dc.OnOpen(func() {
				if isMulti {
					switch dc.Label() {
					case "burst":
						go func() {
							payload := make([]byte, 50*1024)
							tick := time.NewTicker(33 * time.Millisecond)
							defer tick.Stop()
							for i := 0; i < 60; i++ {
								<-tick.C
								stampNs(payload, time.Now().UnixNano())
								_ = dc.Send(payload)
							}
						}()
					case "tokens":
						go func() {
							tick := time.NewTicker(5 * time.Millisecond)
							defer tick.Stop()
							for i := 0; i < 400; i++ {
								<-tick.C
								p := make([]byte, 64)
								stampNs(p, time.Now().UnixNano())
								_ = dc.Send(p)
							}
						}()
					}
				} else {
					_ = dc.SendText("hello")
				}
			})
		})

		if isVideo {
			track, err := webrtc.NewTrackLocalStaticSample(
				webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8},
				"video", "wan-bench")
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			if _, err := pc.AddTrack(track); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			go pumpIVFToTrack(track, ivfPath)
		}

		if err := pc.SetRemoteDescription(offer); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		gather := webrtc.GatheringCompletePromise(pc)
		ans, err := pc.CreateAnswer(nil)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		_ = pc.SetLocalDescription(ans)
		<-gather
		_ = json.NewEncoder(w).Encode(*pc.LocalDescription())
	})
	go func() {
		ln, err := net.Listen("tcp", "0.0.0.0:8080")
		if err != nil {
			fmt.Println("pion signaling listen:", err)
			return
		}
		_ = http.Serve(ln, mux)
	}()
}

func pumpIVFToTrack(track *webrtc.TrackLocalStaticSample, path string) {
	for {
		f, err := os.Open(path)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		ivf, header, err := ivfreader.NewWith(f)
		if err != nil {
			f.Close()
			return
		}
		frameInterval := time.Duration(float64(header.TimebaseNumerator)/float64(header.TimebaseDenominator)*float64(time.Second))
		if frameInterval <= 0 {
			frameInterval = 33 * time.Millisecond
		}
		tick := time.NewTicker(frameInterval)
		for ; true; <-tick.C {
			frame, _, err := ivf.ParseNextFrame()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				break
			}
			if err := track.WriteSample(media.Sample{Data: frame, Duration: frameInterval}); err != nil {
				break
			}
		}
		tick.Stop()
		f.Close()
	}
}

func stampNs(buf []byte, ns int64) {
	if len(buf) < 8 {
		return
	}
	u := uint64(ns)
	buf[0] = byte(u >> 56)
	buf[1] = byte(u >> 48)
	buf[2] = byte(u >> 40)
	buf[3] = byte(u >> 32)
	buf[4] = byte(u >> 24)
	buf[5] = byte(u >> 16)
	buf[6] = byte(u >> 8)
	buf[7] = byte(u)
}

// avoid unused-import flags in narrow builds
var _ = bytes.NewReader
var _ sync.WaitGroup
