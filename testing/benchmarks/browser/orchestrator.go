// Browser benchmark orchestrator. Hosts:
//   - the quicrtc-native publisher (on its own UDP port)
//   - a pion WebRTC publisher (server-side PeerConnection)
//   - HTTP signaling for the WebRTC case (offer / answer / ICE)
//   - a static page server for the JS receivers
//   - a /report endpoint that browsers POST results to
//
// The orchestrator drives Chromium via chromedp to load the
// receiver page and collect timing.
package browser

import (
	"context"
	"crypto/tls"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/sachinkesiraju/quicrtc/cert"
	"github.com/sachinkesiraju/quicrtc/peerconnection"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/transport"
	"github.com/sachinkesiraju/quicrtc/wire"
)

//go:embed pages/quicrtc_receiver.html
var quicrtcHTML string

//go:embed pages/webrtc_receiver.html
var webrtcHTML string

//go:embed pages/quicrtc_multimodal.html
var quicrtcMMHTML string

//go:embed pages/webrtc_multimodal.html
var webrtcMMHTML string

// BrowserResult is the JSON the browser POSTs back at end-of-test.
// Single-stream tests use N/RecvAtMs/SentAtNs; multimodal tests use
// the Token*/Video* fields.
type BrowserResult struct {
	Protocol    string    `json:"protocol"`
	N           int       `json:"n"`
	RecvAtMs    []float64 `json:"recvAtMs"`
	SentAtNs    []float64 `json:"sentAtNs"`
	FirstRecvMs float64   `json:"firstRecvMs"`
	LastRecvMs  float64   `json:"lastRecvMs"`

	// Multimodal fields.
	TokenN      int       `json:"tokenN"`
	VideoN      int       `json:"videoN"`
	TokenRecvMs []float64 `json:"tokenRecvMs"`
	TokenSentNs []float64 `json:"tokenSentNs"`
	VideoRecvMs []float64 `json:"videoRecvMs"`
	VideoSentNs []float64 `json:"videoSentNs"`

	Error string `json:"error,omitempty"`
}

// Orchestrator wires together the publisher, signaling, page server,
// and result collection in one HTTP server.
type Orchestrator struct {
	mu sync.Mutex

	// Public assets URL: where the JS receivers fetch the HTML +
	// signaling endpoints + report endpoint.
	httpServer *http.Server
	httpAddr   string // resolved (with port)

	// quicrtc native publisher
	qPC     *peerconnection.PeerConnection
	qSender transport.Sender
	qServer *server.Server // exposed via the transport adapter's escape hatch
	qBundle *cert.Bundle

	// pion publisher (server-side PC). For single-channel tests use
	// pionDC. For multimodal tests, two extra DCs are created on
	// the same PC (canonical 1-PC + 2-DC pattern that hits SCTP HOL).
	pionPC      *webrtc.PeerConnection
	pionDC      *webrtc.DataChannel
	pionTokenDC *webrtc.DataChannel
	pionVideoDC *webrtc.DataChannel
	pionOffer   *webrtc.SessionDescription
	pionAnswer  atomic.Pointer[webrtc.SessionDescription]
	pionICEMu   sync.Mutex
	pionICEs    []webrtc.ICECandidate // candidates from server, queued for browser long-poll

	// Result collection
	resultCh chan *BrowserResult
}

// NewOrchestrator builds an orchestrator. Call Start, then GetURLs,
// drive the browser, await results.
func NewOrchestrator() (*Orchestrator, error) {
	o := &Orchestrator{
		resultCh: make(chan *BrowserResult, 4),
	}
	if err := o.startQuicrtc(); err != nil {
		return nil, fmt.Errorf("start quicrtc: %w", err)
	}
	if err := o.startPion(); err != nil {
		o.Close()
		return nil, fmt.Errorf("start pion: %w", err)
	}
	if err := o.startHTTP(); err != nil {
		o.Close()
		return nil, fmt.Errorf("start http: %w", err)
	}
	return o, nil
}

func (o *Orchestrator) startQuicrtc() error {
	bundle, err := cert.Generate(cert.Options{IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	if err != nil {
		return err
	}
	o.qBundle = bundle
	pc, err := peerconnection.New(peerconnection.Config{
		Transport: peerconnection.TransportNativeOnly,
		Native: &peerconnection.NativeConfig{
			Role: transport.RolePublisher,
			PublisherConfig: server.Config{
				Addr:       "127.0.0.1:0",
				CertBundle: bundle,
				SDP:        wire.SDP{Codec: "test", Width: 64, Height: 64, FPS: 30},
			},
		},
	})
	if err != nil {
		return err
	}
	o.qPC = pc
	if err := pc.Connect(context.Background()); err != nil {
		return err
	}
	if srvAdapter, ok := pc.Transport().(interface{ Server() *server.Server }); ok {
		o.qServer = srvAdapter.Server()
	} else {
		return errors.New("can't access underlying server")
	}
	for strings.HasSuffix(o.qServer.Addr(), ":0") {
		time.Sleep(time.Millisecond)
	}
	return nil
}

func (o *Orchestrator) startPion() error {
	cfg := webrtc.Configuration{ICEServers: []webrtc.ICEServer{}}
	pc, err := webrtc.NewPeerConnection(cfg)
	if err != nil {
		return err
	}
	dc, err := pc.CreateDataChannel("data", nil)
	if err != nil {
		pc.Close()
		return err
	}
	// Also create the multimodal pair (tokens + video) on the SAME
	// PC so the canonical 1-PC + 2-DC pattern is testable. SCTP
	// negotiation happens once for all DCs in the offer.
	tokenDC, err := pc.CreateDataChannel("tokens", nil)
	if err != nil {
		pc.Close()
		return err
	}
	videoDC, err := pc.CreateDataChannel("video", nil)
	if err != nil {
		pc.Close()
		return err
	}
	o.pionTokenDC = tokenDC
	o.pionVideoDC = videoDC
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		o.pionICEMu.Lock()
		o.pionICEs = append(o.pionICEs, *c)
		o.pionICEMu.Unlock()
	})
	o.pionPC = pc
	o.pionDC = dc

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		pc.Close()
		return err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		pc.Close()
		return err
	}
	o.pionOffer = &offer
	return nil
}

func (o *Orchestrator) startHTTP() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/quicrtc", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "text/html; charset=utf-8")
		w.Write([]byte(quicrtcHTML))
	})
	mux.HandleFunc("/webrtc", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "text/html; charset=utf-8")
		w.Write([]byte(webrtcHTML))
	})
	mux.HandleFunc("/quicrtc-mm", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "text/html; charset=utf-8")
		w.Write([]byte(quicrtcMMHTML))
	})
	mux.HandleFunc("/webrtc-mm", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "text/html; charset=utf-8")
		w.Write([]byte(webrtcMMHTML))
	})
	mux.HandleFunc("/sig", o.handleSig)
	mux.HandleFunc("/report", func(w http.ResponseWriter, r *http.Request) {
		var res BrowserResult
		if err := json.NewDecoder(r.Body).Decode(&res); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		select {
		case o.resultCh <- &res:
		default:
		}
		w.WriteHeader(http.StatusNoContent)
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	o.httpAddr = listener.Addr().String()
	o.httpServer = &http.Server{Handler: corsMW(mux)}
	go o.httpServer.Serve(listener)
	return nil
}

func corsMW(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("access-control-allow-origin", "*")
		w.Header().Set("access-control-allow-headers", "*")
		w.Header().Set("access-control-allow-methods", "*")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func (o *Orchestrator) handleSig(w http.ResponseWriter, r *http.Request) {
	op := r.URL.Query().Get("op")
	switch op {
	case "get-offer":
		json.NewEncoder(w).Encode(o.pionOffer)
	case "answer":
		var ans webrtc.SessionDescription
		if err := json.NewDecoder(r.Body).Decode(&ans); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		o.pionAnswer.Store(&ans)
		if err := o.pionPC.SetRemoteDescription(ans); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case "ice-from-browser":
		var c webrtc.ICECandidateInit
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		_ = o.pionPC.AddICECandidate(c)
		w.WriteHeader(http.StatusNoContent)
	case "ice-from-server":
		// Long-poll: pop one queued candidate. If none for ~250ms,
		// return 204 to let the browser loop continue.
		deadline := time.Now().Add(250 * time.Millisecond)
		for time.Now().Before(deadline) {
			o.pionICEMu.Lock()
			if len(o.pionICEs) > 0 {
				c := o.pionICEs[0]
				o.pionICEs = o.pionICEs[1:]
				o.pionICEMu.Unlock()
				cInit := c.ToJSON()
				json.NewEncoder(w).Encode(cInit)
				return
			}
			o.pionICEMu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "unknown op", 400)
	}
}

// QuicrtcURL returns the URL the browser-side JS should load to run
// the quicrtc receiver. Embeds connection params as query string.
func (o *Orchestrator) QuicrtcURL(n int) string {
	addr := o.qServer.Addr()
	wtURL := "https://" + addr + "/wt"
	return fmt.Sprintf("http://%s/quicrtc?url=%s&slug=%s&hash=%s&n=%d&report=http://%s/report",
		o.httpAddr, urlEncode(wtURL), o.qServer.Slug(), o.qServer.CertHashB64(), n, o.httpAddr)
}

// WebRTCURL returns the URL for the WebRTC receiver page.
func (o *Orchestrator) WebRTCURL(n int) string {
	return fmt.Sprintf("http://%s/webrtc?n=%d&sig=http://%s/sig&report=http://%s/report",
		o.httpAddr, n, o.httpAddr, o.httpAddr)
}

// QuicrtcMultimodalURL returns the URL for the multi-modal quicrtc
// receiver (tokens via DataChannel + video via uni streams).
func (o *Orchestrator) QuicrtcMultimodalURL(nt, nv int) string {
	addr := o.qServer.Addr()
	wtURL := "https://" + addr + "/wt"
	return fmt.Sprintf("http://%s/quicrtc-mm?url=%s&slug=%s&hash=%s&nt=%d&nv=%d&report=http://%s/report",
		o.httpAddr, urlEncode(wtURL), o.qServer.Slug(), o.qServer.CertHashB64(), nt, nv, o.httpAddr)
}

// WebRTCMultimodalURL returns the URL for the multi-modal WebRTC
// receiver (canonical 1-PC + tokens DC + video DC pattern).
func (o *Orchestrator) WebRTCMultimodalURL(nt, nv int) string {
	return fmt.Sprintf("http://%s/webrtc-mm?nt=%d&nv=%d&sig=http://%s/sig&report=http://%s/report",
		o.httpAddr, nt, nv, o.httpAddr, o.httpAddr)
}

// PionTokenDC and PionVideoDC return the per-channel DCs for the
// multi-modal canonical-PC pattern.
func (o *Orchestrator) PionTokenDC() *webrtc.DataChannel { return o.pionTokenDC }
func (o *Orchestrator) PionVideoDC() *webrtc.DataChannel { return o.pionVideoDC }

// QuicrtcSender returns the publisher's Sender. Caller drives
// Send() at whatever cadence the workload requires.
func (o *Orchestrator) QuicrtcSender() transport.Sender { return o.qSender }
func (o *Orchestrator) PionDC() *webrtc.DataChannel     { return o.pionDC }

// Init publishes the quicrtc track (must be done after page loads
// to avoid "no subscribers" drop policy on first frame). For pion,
// nothing to do here — DC is created in startPion.
func (o *Orchestrator) PrepareQuicrtcTrack(ctx context.Context) error {
	// AddTrack must be called before first send; the test's send
	// loop assumes the sender is ready.
	// Defensive: we use track.Video here just to satisfy the API.
	// The test workload is opaque AccessUnit bytes.
	if o.qSender != nil {
		return nil
	}
	// Lazy import to avoid cycle annoyance: resolve the sender on
	// first call rather than at startup.
	return errors.New("call SetQuicrtcSender before driving sends")
}

// SetQuicrtcSender installs the publisher's Sender (created by the
// caller via pc.AddTrack with an appropriate track.LocalTrack).
func (o *Orchestrator) SetQuicrtcSender(s transport.Sender) {
	o.qSender = s
}

// QuicrtcPC exposes the underlying PeerConnection so the test can
// AddTrack with a track of its choice.
func (o *Orchestrator) QuicrtcPC() *peerconnection.PeerConnection { return o.qPC }

// AwaitResult blocks until exactly one /report POST arrives.
func (o *Orchestrator) AwaitResult(ctx context.Context) (*BrowserResult, error) {
	select {
	case r := <-o.resultCh:
		return r, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close tears down everything.
func (o *Orchestrator) Close() {
	if o.httpServer != nil {
		o.httpServer.Close()
	}
	if o.qPC != nil {
		o.qPC.Close()
	}
	if o.pionPC != nil {
		o.pionPC.Close()
	}
}

// TLSConfig is a hint to TLS-terminating proxies.
func (o *Orchestrator) TLSConfig() *tls.Config {
	return &tls.Config{Certificates: []tls.Certificate{o.qBundle.TLS}}
}

// urlEncode URL-encodes a single value.
func urlEncode(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '~' {
			out = append(out, c)
		} else {
			out = append(out, '%')
			out = append(out, hexDigit(c>>4))
			out = append(out, hexDigit(c&0xf))
		}
	}
	return string(out)
}

func hexDigit(n byte) byte {
	if n < 10 {
		return '0' + n
	}
	return 'a' + n - 10
}

// pubsub.AccessUnit is exported for tests that need it.
var _ pubsub.AccessUnit
