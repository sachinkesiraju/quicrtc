package cert

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"net"
	"testing"
	"time"
)

func TestGenerateContainsLoopbackAndExtras(t *testing.T) {
	extras := []net.IP{
		net.ParseIP("192.168.1.42"),
		net.ParseIP("2001:db8::1"),
		net.ParseIP("100.64.0.1"), // Tailscale CGNAT
	}
	b, err := Generate(Options{IPs: extras, DNSNames: []string{"foo.example"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(b.TLS.Certificate) == 0 {
		t.Fatal("no DER cert")
	}
	leaf, err := x509.ParseCertificate(b.TLS.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"127.0.0.1":    false,
		"::1":          false,
		"192.168.1.42": false,
		"2001:db8::1":  false,
		"100.64.0.1":   false,
	}
	for _, ip := range leaf.IPAddresses {
		if _, ok := want[ip.String()]; ok {
			want[ip.String()] = true
		}
	}
	for ip, found := range want {
		if !found {
			t.Errorf("SAN missing IP %s", ip)
		}
	}
	dnsWant := map[string]bool{"localhost": false, "foo.example": false}
	for _, d := range leaf.DNSNames {
		if _, ok := dnsWant[d]; ok {
			dnsWant[d] = true
		}
	}
	for d, found := range dnsWant {
		if !found {
			t.Errorf("DNS SAN missing %s", d)
		}
	}
}

func TestHashMatchesDER(t *testing.T) {
	b, err := Generate(Options{})
	if err != nil {
		t.Fatal(err)
	}
	got := sha256.Sum256(b.TLS.Certificate[0])
	if got != b.DERHash {
		t.Fatal("DERHash != sha256(DER)")
	}
	// Hash must round-trip through base64url.
	dec, err := base64.RawURLEncoding.DecodeString(b.HashB64URL())
	if err != nil {
		t.Fatal(err)
	}
	if string(dec) != string(b.DERHash[:]) {
		t.Fatal("HashB64URL round-trip mismatch")
	}
}

func TestValidityClampedUnderBrowserCap(t *testing.T) {
	b, err := Generate(Options{Validity: 365 * 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(b.TLS.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if d := leaf.NotAfter.Sub(leaf.NotBefore); d > 14*24*time.Hour {
		t.Fatalf("validity not clamped: %s > 14d", d)
	}
}

func TestDefaultValidityIs13Days(t *testing.T) {
	b, err := Generate(Options{})
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(b.TLS.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	d := leaf.NotAfter.Sub(leaf.NotBefore)
	// 13 days + clock-skew slack ~= 13d1m
	if d < 12*24*time.Hour || d > 14*24*time.Hour {
		t.Fatalf("default validity %s not within 13d window", d)
	}
}

func TestECDSACurve(t *testing.T) {
	b, err := Generate(Options{})
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(b.TLS.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if leaf.SignatureAlgorithm != x509.ECDSAWithSHA256 {
		t.Fatalf("got SigAlg %v, want ECDSAWithSHA256 (browsers reject other algs)", leaf.SignatureAlgorithm)
	}
}

func TestNilIPsAreSkipped(t *testing.T) {
	b, err := Generate(Options{IPs: []net.IP{nil, net.ParseIP("10.0.0.1"), nil}})
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := x509.ParseCertificate(b.TLS.Certificate[0])
	for _, ip := range leaf.IPAddresses {
		if ip == nil {
			t.Fatal("nil IP leaked into SAN list")
		}
	}
}
