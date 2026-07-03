package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// workspace is a real on-disk Go module the LLM workers share.
type workspace struct {
	root string
	mu   sync.Mutex
}

func newWorkspace(dir string) (*workspace, error) {
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "control-room-ws-*")
		if err != nil {
			return nil, err
		}
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &workspace{root: dir}
	if err := w.seed(); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	return w, nil
}

func (w *workspace) Close() error {
	return os.RemoveAll(w.root)
}

func (w *workspace) seed() error {
	files := map[string]string{
		"go.mod": `module acme/payments-service

go 1.22
`,
		"billing/invoice.go": joinLines(invoiceBuggy),
		"billing/invoice_test.go": `package billing

import "testing"

func TestInvoiceTotalWithDiscount(t *testing.T) {
	inv := Invoice{
		DiscountPct: 10,
		Items:       make([]LineItem, 15),
	}
	for i := range inv.Items {
		inv.Items[i] = LineItem{UnitCents: 7999, Qty: 1}
	}
	got := inv.Total()
	want := int64(107987)
	if got != want {
		t.Fatalf("Total() = %d, want %d", got, want)
	}
}
`,
	}
	for rel, body := range files {
		path := filepath.Join(w.root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func joinLines(lines []string) string {
	out := ""
	for i, l := range lines {
		if i > 0 {
			out += "\n"
		}
		out += l
	}
	return out + "\n"
}

func (w *workspace) summary() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return fmt.Sprintf("workspace root: %s", w.root)
}
