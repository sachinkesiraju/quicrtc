// The scripted coding-agent session the desktop replays.
//
// A cloud dev agent (think Devin / Cursor cloud agent) works a
// realistic task: reproduce a failing test in a Go service, find the
// rounding bug, patch it, re-run the tests, and open a PR. Each step
// carries the reasoning the model would stream, the tool call it
// issues, and a sequence of timed desktop mutations (terminal output
// appearing line by line, editor edits, CI status) so the screen lane
// visibly tracks the work.
//
// The reasoning text and action sequence are scripted — no model is
// running. Everything on the wire is real: PNG screen frames, token
// cadence, tool-call AUs, telemetry.
package main

import (
	"image/color"
	"time"
)

// toolCall is the JSON payload published on the toolcalls lane —
// the same action vocabulary a Devin-style agent host exposes.
type toolCall struct {
	Tool string `json:"tool"`
	Cmd  string `json:"cmd,omitempty"`
	Path string `json:"path,omitempty"`
	Diff string `json:"diff,omitempty"`
	Note string `json:"note,omitempty"`
}

// mutation is one timed scene change within a step. After holds the
// delay since the previous mutation in the same step.
type mutation struct {
	after time.Duration
	apply func(*scene)
}

// step is one agent turn: stream the reasoning, publish the tool
// call, then play the desktop mutations.
type step struct {
	title     string
	reasoning string
	call      toolCall
	mutations []mutation
}

func term(text string, c colorRef) func(*scene) {
	return func(s *scene) { s.appendTerm(termLine{text: text, color: resolve(c)}) }
}

// colorRef keeps the script table readable.
type colorRef int

const (
	cText colorRef = iota
	cDim
	cPrompt
	cGreen
	cRed
	cYellow
)

func resolve(c colorRef) color.NRGBA {
	switch c {
	case cDim:
		return colDim
	case cPrompt:
		return colPrompt
	case cGreen:
		return colGreen
	case cRed:
		return colRed
	case cYellow:
		return colYellow
	default:
		return colText
	}
}

// The buggy file as first opened, and the fixed hunk. Line indices
// are 0-based into these slices.
var invoiceBuggy = []string{
	"package billing",
	"",
	"import \"math\"",
	"",
	"// Total returns the invoice total in cents, applying the",
	"// customer's percentage discount to each line item.",
	"func (inv *Invoice) Total() int64 {",
	"\tvar total int64",
	"\tfor _, item := range inv.Items {",
	"\t\tprice := float64(item.UnitCents) * float64(item.Qty)",
	"\t\tdiscounted := price * (1 - inv.DiscountPct/100)",
	"\t\ttotal += int64(discounted) // truncates per line item",
	"\t}",
	"\treturn total",
	"}",
}

var invoiceFixed = []string{
	"package billing",
	"",
	"import \"math\"",
	"",
	"// Total returns the invoice total in cents. The discount is",
	"// applied to the invoice subtotal and rounded half-up ONCE —",
	"// per-line truncation drifted a cent per item (see #2481).",
	"func (inv *Invoice) Total() int64 {",
	"\tvar subtotal int64",
	"\tfor _, item := range inv.Items {",
	"\t\tsubtotal += item.UnitCents * item.Qty",
	"\t}",
	"\tdiscounted := float64(subtotal) * (1 - inv.DiscountPct/100)",
	"\treturn int64(math.Round(discounted))",
	"}",
}

// script returns the full scripted session. Written as a function
// (not a package var) because mutations close over scene state.
func script() []step {
	return []step{
		{
			title:     "set up the workspace",
			reasoning: "Starting on the task: customers report invoice totals drifting by a few cents when a percentage discount is applied, and finance flagged it because the drift compounds across thousands of invoices a day. First I'll clone the repo into the workspace and run the full test suite to see the current state before touching anything — if the bug is real there should already be a failing test, and if there isn't, writing one is my first job.",
			call:      toolCall{Tool: "run_command", Cmd: "git clone git@github.com:acme/payments-service && go test ./..."},
			mutations: []mutation{
				{0, term("$ git clone git@github.com:acme/payments-service", cPrompt)},
				{400 * time.Millisecond, term("Cloning into 'payments-service'...", cDim)},
				{500 * time.Millisecond, term("remote: Enumerating objects: 4821, done.", cDim)},
				{300 * time.Millisecond, term("Receiving objects: 100% (4821/4821), 6.24 MiB | 18.3 MiB/s, done.", cDim)},
				{600 * time.Millisecond, term("$ cd payments-service && go test ./...", cPrompt)},
				{900 * time.Millisecond, term("ok      acme/payments-service/api        0.41s", cText)},
				{300 * time.Millisecond, term("ok      acme/payments-service/ledger     1.02s", cText)},
				{400 * time.Millisecond, term("--- FAIL: TestInvoiceTotalWithDiscount (0.00s)", cRed)},
				{200 * time.Millisecond, term("    invoice_test.go:74: Total() = 107987, want 107989", cRed)},
				{200 * time.Millisecond, term("FAIL    acme/payments-service/billing    0.18s", cRed)},
			},
		},
		{
			title:     "reproduce and localize the failure",
			reasoning: "One failing test: TestInvoiceTotalWithDiscount expects 107989 cents but Total() returns 107987 — two cents short on a 15-item invoice. A small negative drift that grows with the number of line items is the signature of per-item truncation: each cast throws away up to a cent, and the losses only ever point one direction. Let me run just that test with -v to confirm the drift scales with item count, then read the function itself before deciding on the fix.",
			call:      toolCall{Tool: "run_command", Cmd: "go test ./billing -run TestInvoiceTotalWithDiscount -v"},
			mutations: []mutation{
				{0, term("$ go test ./billing -run TestInvoiceTotalWithDiscount -v", cPrompt)},
				{700 * time.Millisecond, term("=== RUN   TestInvoiceTotalWithDiscount", cDim)},
				{250 * time.Millisecond, term("    invoice_test.go:74: Total() = 107987, want 107989", cRed)},
				{200 * time.Millisecond, term("    invoice_test.go:75: drift is -2 cents across 15 line items", cRed)},
				{200 * time.Millisecond, term("--- FAIL: TestInvoiceTotalWithDiscount (0.00s)", cRed)},
			},
		},
		{
			title:     "read the offending code",
			reasoning: "Opening billing/invoice.go. There it is: the discount is applied per line item and each discounted price is cast straight to int64, which truncates toward zero. Fifteen line items means fifteen truncations, up to a cent each, always in the customer's favor or against them depending on sign — never balancing out. The invoice-level contract in the billing spec is explicit about this exact case: apply the discount to the subtotal, then round half-up exactly once. The per-item approach can never satisfy that, no matter how the rounding mode is tweaked.",
			call:      toolCall{Tool: "open_file", Path: "billing/invoice.go"},
			mutations: []mutation{
				{0, func(s *scene) {
					s.editorFile = "billing/invoice.go"
					s.editorLines = invoiceBuggy
					s.editorHighlight = map[int]bool{10: true, 11: true}
					s.editorModified = false
				}},
			},
		},
		{
			title:     "patch the rounding bug",
			reasoning: "Rewriting Total(): accumulate the exact integer subtotal first — unit cents times quantity stays in int64, no floats anywhere near the loop — then apply the discount once at invoice level and round half-up with math.Round instead of truncating. That matches the billing spec, kills the accumulation drift for any item count, and as a bonus the function gets simpler: one conversion to float64 at the very end instead of one per line item.",
			call: toolCall{
				Tool: "edit_file", Path: "billing/invoice.go",
				Diff: "-\t\tdiscounted := price * (1 - inv.DiscountPct/100)\n-\t\ttotal += int64(discounted)\n+\tdiscounted := float64(subtotal) * (1 - inv.DiscountPct/100)\n+\treturn int64(math.Round(discounted))",
			},
			mutations: []mutation{
				{0, func(s *scene) {
					s.editorLines = invoiceFixed
					s.editorHighlight = map[int]bool{8: true, 9: true, 10: true, 11: true, 12: true, 13: true}
					s.editorModified = true
				}},
				{1200 * time.Millisecond, func(s *scene) { s.editorModified = false }},
			},
		},
		{
			title:     "verify the fix",
			reasoning: "Re-running the whole suite, not just the failing test — rounding changes love to break neighboring golden values, and the ledger package consumes Total() for reconciliation so it gets a close look. Everything is green, including the property test that sweeps invoices from 1 to 500 items with randomized prices and discount rates. The drift is gone at every size, and the reconciliation goldens didn't move, which confirms the old behavior was wrong only in the discounted path.",
			call:      toolCall{Tool: "run_command", Cmd: "go test ./..."},
			mutations: []mutation{
				{0, term("$ go test ./...", cPrompt)},
				{900 * time.Millisecond, term("ok      acme/payments-service/api        0.40s", cText)},
				{250 * time.Millisecond, term("ok      acme/payments-service/billing    0.22s", cGreen)},
				{250 * time.Millisecond, term("ok      acme/payments-service/ledger     1.01s", cText)},
				{300 * time.Millisecond, term("PASS: all packages (0 failed)", cGreen)},
			},
		},
		{
			title:     "commit and open the PR",
			reasoning: "Committing the fix with a message that explains the invoice-level rounding contract rather than just describing the diff — the next person to touch this function should learn the spec from the history. Pushing the branch and opening a pull request against main with the failing-then-fixed test called out in the description. I'll watch CI run the same suite on a clean runner before handing this back for review.",
			call:      toolCall{Tool: "git_commit", Cmd: "git checkout -b fix/invoice-rounding && git commit -am 'billing: apply discount at invoice level, round half-up' && gh pr create"},
			mutations: []mutation{
				{0, term("$ git checkout -b fix/invoice-rounding", cPrompt)},
				{300 * time.Millisecond, term("$ git commit -am 'billing: apply discount at invoice level, round half-up'", cPrompt)},
				{400 * time.Millisecond, term("[fix/invoice-rounding 9c41f2a] billing: apply discount at invoice level", cDim)},
				{200 * time.Millisecond, term(" 1 file changed, 8 insertions(+), 6 deletions(-)", cDim)},
				{500 * time.Millisecond, term("$ gh pr create --fill", cPrompt)},
				{800 * time.Millisecond, term("https://github.com/acme/payments-service/pull/2481", cText)},
				{400 * time.Millisecond, func(s *scene) {
					s.ciVisible = true
					s.ciPassing = false
					s.prURL = "PR #2481 open — CI running"
				}},
			},
		},
		{
			title:     "watch CI go green",
			reasoning: "CI is running go-test, go-vet, and gosec on the PR — the same suite that failed at the start of the session, now on a clean runner with no local state to flatter the result. All three checks passed. Task complete: the rounding drift is fixed at the invoice level where the spec says it belongs, the property test covers every invoice size that mattered, and the PR is ready for human review with the reasoning documented in the commit message.",
			call:      toolCall{Tool: "wait_for_ci", Note: "pull #2481: go-test, go-vet, gosec"},
			mutations: []mutation{
				{2500 * time.Millisecond, func(s *scene) {
					s.ciPassing = true
					s.prURL = "PR #2481 — all checks passed"
				}},
				{300 * time.Millisecond, term("CI: all checks passed on fix/invoice-rounding", cGreen)},
			},
		},
	}
}

// resetScene returns the scene to its start-of-script state (the demo
// loops forever so a left-running server keeps producing traffic).
func resetScene(s *scene) {
	s.term = s.term[:0]
	s.editorFile = ""
	s.editorLines = nil
	s.editorHighlight = nil
	s.editorModified = false
	s.ciVisible = false
	s.ciPassing = false
	s.prURL = ""
	s.appendTerm(termLine{text: "cloud agent session — workspace vm-7c2f", color: colDim})
	s.appendTerm(termLine{text: "", color: colDim})
}
