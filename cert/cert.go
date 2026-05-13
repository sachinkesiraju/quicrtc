// Package cert generates ephemeral self-signed TLS certificates
// suitable for browser WebTransport.
//
// Browser WebTransport rejects:
//
//   - non-ECDSA P-256 / P-384 keys (RSA fails);
//   - validity > 14 days (browsers hard-cap at two weeks);
//   - certs missing SAN entries for the IPs being connected to.
//
// quicrtc generates certs that satisfy all three. The DER hash is
// emitted to the URL fragment so the browser can pin it via
// WebTransport's serverCertificateHashes option, which lets us avoid
// the public CA dance entirely.
package cert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"
)

// Bundle is the materials for a WebTransport listener: the TLS cert
// for the server, and the SHA-256(DER) hash for the browser to pin.
type Bundle struct {
	TLS      tls.Certificate
	DERHash  [32]byte // SHA-256 of full DER cert
	NotAfter time.Time
}

// HashB64URL returns the SHA-256(DER) as base64url with no padding.
// This is the wire format the browser expects in the URL fragment
// and the form quicrtc's JS client parses.
func (b *Bundle) HashB64URL() string {
	return base64.RawURLEncoding.EncodeToString(b.DERHash[:])
}

// Options control which SAN entries land in the cert.
type Options struct {
	// IPs is the list of IP literals to add to subjectAltName. The
	// loopback addresses 127.0.0.1 and ::1 are always added; pass any
	// LAN, public, Tailscale, or extra-SAN IPs the listener should be
	// reachable on.
	IPs []net.IP

	// DNSNames is the list of DNS names. "localhost" is always added.
	DNSNames []string

	// CommonName is what shows up as Subject CN. Cosmetic for
	// browsers that pin via hash; defaults to "quicrtc".
	CommonName string

	// Validity caps the cert's NotAfter relative to now. Hard-clamped
	// to <= 13d24h to stay under the browser 14-day ceiling. Zero
	// picks 13 days.
	Validity time.Duration
}

// Generate produces an ephemeral ECDSA P-256 cert per opts.
func Generate(opts Options) (*Bundle, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("ecdsa keygen: %w", err)
	}

	now := time.Now()
	notBefore := now.Add(-1 * time.Minute) // clock-skew slack
	validity := opts.Validity
	if validity <= 0 {
		validity = 13 * 24 * time.Hour
	}
	const browserCap = 13*24*time.Hour + 23*time.Hour // stay safely under 14d
	if validity > browserCap {
		validity = browserCap
	}
	notAfter := now.Add(validity)

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("serial: %w", err)
	}

	ips := []net.IP{
		net.ParseIP("127.0.0.1"),
		net.ParseIP("::1"),
	}
	for _, ip := range opts.IPs {
		if ip == nil {
			continue
		}
		ips = append(ips, ip)
	}

	dnsNames := []string{"localhost"}
	dnsNames = append(dnsNames, opts.DNSNames...)

	cn := opts.CommonName
	if cn == "" {
		cn = "quicrtc"
	}

	tmpl := x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              dnsNames,
		IPAddresses:           ips,
		SignatureAlgorithm:    x509.ECDSAWithSHA256,
	}

	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		return nil, fmt.Errorf("createcert: %w", err)
	}

	hash := sha256.Sum256(der)

	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("marshalkey: %w", err)
	}

	tlsCert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		return nil, fmt.Errorf("tls keypair: %w", err)
	}

	return &Bundle{TLS: tlsCert, DERHash: hash, NotAfter: notAfter}, nil
}
