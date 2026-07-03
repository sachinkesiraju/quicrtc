package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestWorkspaceRunCommand(t *testing.T) {
	ws, err := newWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()

	out, err := executeTool(context.Background(), ws, toolCall{
		Tool: "run_command",
		Cmd:  "go test ./billing -run TestInvoiceTotalWithDiscount -count=1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.output == "" {
		t.Fatal("expected test output")
	}
	if !strings.Contains(out.output, "FAIL") && !strings.Contains(out.output, "want") {
		t.Logf("output: %s", out.output)
	}
}

func TestWorkspaceFixAndPass(t *testing.T) {
	ws, err := newWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()

	_, err = executeTool(context.Background(), ws, toolCall{
		Tool: "edit_file", Path: "billing/invoice.go", Note: joinLines(invoiceFixed),
	})
	if err != nil {
		t.Fatal(err)
	}
	out, err := executeTool(context.Background(), ws, toolCall{
		Tool: "run_command",
		Cmd:  "go test ./billing -run TestInvoiceTotalWithDiscount -count=1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.output, "ok") && !strings.Contains(out.output, "PASS") {
		t.Fatalf("expected pass, got: %s", out.output)
	}
}

func TestLLMAgentsIntegration(t *testing.T) {
	if os.Getenv("ANTHROPIC_API_KEY") == "" {
		t.Skip("ANTHROPIC_API_KEY not set")
	}
	h, err := startControlRoomHarness(harnessOptions{
		FPS:      8,
		LLM:      true,
		LLMTurns: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	time.Sleep(30 * time.Second)
	if h.orch.st.Get("tool_au") == 0 {
		t.Fatal("expected tool calls from real agents")
	}
}
