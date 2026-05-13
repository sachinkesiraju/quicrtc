package cert

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestReloaderInitialLoad: NewReloader returns a usable cert for
// GetCertificate immediately after construction.
func TestReloaderInitialLoad(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writeTestCert(t, dir, "first")
	r, err := NewReloader(certPath, keyPath, ReloaderOptions{
		PollInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	c, err := r.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if c == nil || len(c.Certificate) == 0 {
		t.Fatal("nil or empty cert")
	}
}

// TestReloaderHotSwap: writing a new cert/key pair causes the
// reloader to swap the served cert within ~one poll interval, with
// no restart.
func TestReloaderHotSwap(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writeTestCert(t, dir, "first")

	var reloads atomic.Int32
	r, err := NewReloader(certPath, keyPath, ReloaderOptions{
		PollInterval: 50 * time.Millisecond,
		OnReload:     func(_ *tls.Certificate, _ time.Time) { reloads.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	first := r.Current()

	// Wait a bit so the next ModTime is observably newer (filesystem
	// resolution can be coarse), then write a fresh cert.
	time.Sleep(120 * time.Millisecond)
	writeTestCertOver(t, certPath, keyPath, "second")

	// Wait for poll to pick up the change.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if reloads.Load() >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if reloads.Load() < 2 {
		t.Fatalf("expected ≥2 reloads after rewrite, got %d", reloads.Load())
	}
	second := r.Current()
	if first == second {
		t.Error("expected new cert pointer after hot reload; got same pointer")
	}
}

// TestReloaderBadReloadKeepsOldCert: corrupting the cert file should
// NOT clobber the in-memory cert. The reloader logs (via OnReloadError
// callback) and keeps serving the old one.
func TestReloaderBadReloadKeepsOldCert(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writeTestCert(t, dir, "good")
	var errCount atomic.Int32
	r, err := NewReloader(certPath, keyPath, ReloaderOptions{
		PollInterval:  30 * time.Millisecond,
		OnReloadError: func(error) { errCount.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	good := r.Current()

	// Corrupt the cert file.
	time.Sleep(80 * time.Millisecond)
	if err := os.WriteFile(certPath, []byte("not a pem cert at all"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Wait for reload attempt.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) && errCount.Load() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if errCount.Load() == 0 {
		t.Fatal("expected reload error, got none")
	}
	if r.Current() != good {
		t.Error("bad reload should leave old cert in service")
	}
}

func writeTestCert(t *testing.T, dir, label string) (certPath, keyPath string) {
	t.Helper()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	writeTestCertOver(t, certPath, keyPath, label)
	return
}

func writeTestCertOver(t *testing.T, certPath, keyPath, label string) {
	t.Helper()
	bundle, err := Generate(Options{
		CommonName: "reloader-test-" + label,
		IPs:        []net.IP{net.ParseIP("127.0.0.1")},
	})
	if err != nil {
		t.Fatal(err)
	}
	// We have a tls.Certificate; encode back to PEM for disk.
	derCert := bundle.TLS.Certificate[0]
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: derCert,
	}), 0o600); err != nil {
		t.Fatal(err)
	}
	// Marshal the private key. Bundle stores tls.Certificate which
	// has PrivateKey; we re-marshal as PEM.
	keyDER, err := x509.MarshalPKCS8PrivateKey(bundle.TLS.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{
		Type: "PRIVATE KEY", Bytes: keyDER,
	}), 0o600); err != nil {
		t.Fatal(err)
	}
}
