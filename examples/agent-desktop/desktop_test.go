package main

import "testing"

// TestFrameEncodes exercises the painter across a representative
// scene and reports the PNG size (run with -v) so the screen lane's
// bandwidth footprint can be tuned against the demo's default link.
func TestFrameEncodes(t *testing.T) {
	p := newDesktopPainter()
	s := &scene{}
	resetScene(s)
	s.stepTitle = "verify the fix"
	s.editorFile = "billing/invoice.go"
	s.editorLines = invoiceFixed
	s.ciVisible = true
	for i := 0; i < 12; i++ {
		s.appendTerm(termLine{text: "ok      acme/payments-service/billing    0.22s", color: colText})
	}
	total := 0
	const n = 5
	for i := 0; i < n; i++ {
		b, err := p.Frame(s)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) == 0 {
			t.Fatal("empty frame")
		}
		total += len(b)
	}
	avg := total / n
	t.Logf("avg frame %d KB → %.1f Mbps at 10 fps", avg/1024, float64(avg)*8*10/1e6)
}
