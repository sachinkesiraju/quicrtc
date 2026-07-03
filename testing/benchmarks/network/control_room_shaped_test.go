// Validates agent-control-room WebTransport under shaped loopback
// network (UDP proxy netem) — the WAN-bench substitute when GCP is
// unavailable.
package network

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/testing/benchmarks/internal/loadgen"
)

type controlRoomConfig struct {
	WTURL    string `json:"wtUrl"`
	Slug     string `json:"slug"`
	CertHash string `json:"certHash"`
}

func controlRoomRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../../.."))
}

func startControlRoomForNetworkTest(t *testing.T) (cfg controlRoomConfig, quicUDP string, cleanup func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	cmd := exec.Command("go", "run", ".", "-http", fmt.Sprintf("127.0.0.1:%d", port))
	cmd.Dir = filepath.Join(controlRoomRepoRoot(t), "examples/agent-control-room")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/config")
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = json.NewDecoder(resp.Body).Decode(&cfg)
			resp.Body.Close()
			if cfg.WTURL != "" {
				host, _, err := net.SplitHostPort(cfg.WTURL[len("https://"):])
				if err == nil {
					// WTURL is https://host:port/wt — extract UDP quic port from config wt host
					_ = host
				}
				// Parse quic addr from wt URL
				u := cfg.WTURL
				if len(u) > 8 {
					addrPort := u[8 : len(u)-3] // strip https:// and /wt
					quicUDP = addrPort
				}
				return cfg, quicUDP, func() {
					_ = cmd.Process.Kill()
					_, _ = cmd.Process.Wait()
				}
			}
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	t.Fatal("control-room did not start")
	return controlRoomConfig{}, "", nil
}

func TestControlRoomWebTransportUnderShapedNetwork(t *testing.T) {
	cfg, quicUDP, stop := startControlRoomForNetworkTest(t)
	defer stop()

	backend, err := net.ResolveUDPAddr("udp", quicUDP)
	if err != nil {
		t.Fatal(err)
	}
	cond := loadgen.NetCond{OneWayDelay: 40 * time.Millisecond}
	proxyAddr, stopProxy, err := loadgen.StartUDPProxy(backend, cond)
	if err != nil {
		t.Fatal(err)
	}
	defer stopProxy()

	wtURL := fmt.Sprintf("https://127.0.0.1:%d/wt", proxyAddr.Port)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	cl, err := client.Dial(ctx, wtURL, client.Options{
		Slug:               cfg.Slug,
		CertHashB64URL:     cfg.CertHash,
		InsecureSkipVerify: true,
		HelloTimeout:       15 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	recvCtx, recvCancel := context.WithTimeout(ctx, 30*time.Second)
	defer recvCancel()
	start := time.Now()
	au, err := cl.RecvOn(recvCtx, "screen")
	if err != nil {
		t.Fatalf("recv screen through shaped link: %v", err)
	}
	elapsed := time.Since(start)
	if len(au.Bytes) == 0 {
		t.Fatal("empty screen frame")
	}
	// One-way 40ms proxy + server work — first frame should arrive well under 2s.
	if elapsed > 2*time.Second {
		t.Fatalf("first frame took %v through 40ms one-way netem", elapsed)
	}
	t.Logf("first screen frame through shaped link in %v (%d bytes)", elapsed, len(au.Bytes))
}
