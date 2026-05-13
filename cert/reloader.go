// Hot-reloading TLS cert source. Production deployments use cert-
// manager / certbot / Vault to rotate certs on disk; this lets the
// running server pick up the new cert without a restart.
//
// Usage:
//
//	reloader, err := cert.NewReloader(certFile, keyFile, cert.ReloaderOptions{
//	    PollInterval: 30 * time.Second,
//	})
//	tlsCfg := &tls.Config{
//	    GetCertificate: reloader.GetCertificate,
//	}
//
// The reloader stat()s both files at PollInterval and atomically
// swaps the served cert when ModTime changes. Bad reload (parse
// error, unreadable) leaves the previous cert in place — never
// serves a half-loaded state.
package cert

import (
	"crypto/tls"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ReloaderOptions tunes how often the reloader checks for changes.
type ReloaderOptions struct {
	// PollInterval is how often the reloader stat()s the cert/key
	// files. <=0 picks 60s — same as Caddy/Traefik defaults.
	PollInterval time.Duration

	// OnReload, if non-nil, is called every time a successful reload
	// happens. Useful for logging or metrics.
	OnReload func(newCert *tls.Certificate, modTime time.Time)

	// OnReloadError, if non-nil, is called when a reload attempt
	// fails. The previous cert remains in service.
	OnReloadError func(err error)
}

// Reloader watches a cert+key file pair on disk and atomically swaps
// the in-memory cert when either file's ModTime changes.
type Reloader struct {
	certFile string
	keyFile  string
	cfg      ReloaderOptions

	current  atomic.Pointer[tls.Certificate]
	lastMod  atomic.Int64 // unix nano
	stopOnce sync.Once
	stopCh   chan struct{}
}

// NewReloader loads the initial cert/key, then starts a background
// goroutine that polls for changes. Returns an error if the initial
// load fails.
func NewReloader(certFile, keyFile string, opts ReloaderOptions) (*Reloader, error) {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 60 * time.Second
	}
	r := &Reloader{
		certFile: certFile,
		keyFile:  keyFile,
		cfg:      opts,
		stopCh:   make(chan struct{}),
	}
	if err := r.loadOnce(); err != nil {
		return nil, fmt.Errorf("initial cert load: %w", err)
	}
	go r.pollLoop()
	return r, nil
}

// GetCertificate is suitable for tls.Config.GetCertificate.
// Always returns the most recently successfully-loaded cert; never
// returns nil after a successful initial load.
func (r *Reloader) GetCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	c := r.current.Load()
	if c == nil {
		return nil, fmt.Errorf("cert reloader: no cert loaded yet")
	}
	return c, nil
}

// Current returns a snapshot of the currently-served cert.
func (r *Reloader) Current() *tls.Certificate {
	return r.current.Load()
}

// Stop halts the polling goroutine. Safe to call multiple times.
func (r *Reloader) Stop() {
	r.stopOnce.Do(func() { close(r.stopCh) })
}

func (r *Reloader) loadOnce() error {
	certInfo, err := os.Stat(r.certFile)
	if err != nil {
		return fmt.Errorf("stat cert: %w", err)
	}
	keyInfo, err := os.Stat(r.keyFile)
	if err != nil {
		return fmt.Errorf("stat key: %w", err)
	}
	mod := certInfo.ModTime()
	if keyInfo.ModTime().After(mod) {
		mod = keyInfo.ModTime()
	}
	tlsCert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return fmt.Errorf("load keypair: %w", err)
	}
	r.current.Store(&tlsCert)
	r.lastMod.Store(mod.UnixNano())
	if r.cfg.OnReload != nil {
		r.cfg.OnReload(&tlsCert, mod)
	}
	return nil
}

func (r *Reloader) pollLoop() {
	t := time.NewTicker(r.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-t.C:
			if r.changed() {
				if err := r.loadOnce(); err != nil {
					if r.cfg.OnReloadError != nil {
						r.cfg.OnReloadError(err)
					}
					// Otherwise swallow — current cert stays in service.
				}
			}
		}
	}
}

// changed reports whether either file's ModTime has advanced past
// the last successful load.
func (r *Reloader) changed() bool {
	last := r.lastMod.Load()
	certInfo, err := os.Stat(r.certFile)
	if err != nil {
		return false
	}
	if certInfo.ModTime().UnixNano() > last {
		return true
	}
	keyInfo, err := os.Stat(r.keyFile)
	if err != nil {
		return false
	}
	return keyInfo.ModTime().UnixNano() > last
}
