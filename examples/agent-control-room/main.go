package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
)

func main() {
	var (
		httpAddr    = flag.String("http", "127.0.0.1:8430", "demo page address")
		fps         = flag.Int("fps", 12, "screen frame rate")
		recordPath  = flag.String("record", "", "optional .qrtc recording path")
		thesisBench = flag.Bool("thesis-bench", false, "run thesis validation benchmarks and exit")
		useLLM      = flag.Bool("llm", false, "use real Anthropic agents (requires ANTHROPIC_API_KEY)")
		llmModel    = flag.String("model", defaultLLMModel, "Anthropic model for -llm")
		llmTurns    = flag.Int("llm-turns", 8, "max tool turns per worker per cycle")
		workspace   = flag.String("workspace", "", "workspace directory for -llm (temp if empty)")
	)
	flag.Parse()

	if *thesisBench {
		if err := runThesisBench(); err != nil {
			log.Fatal(err)
		}
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	h, err := startControlRoomHarness(harnessOptions{
		HTTPAddr:   *httpAddr,
		FPS:        *fps,
		RecordPath: *recordPath,
		LLM:        *useLLM,
		LLMModel:   *llmModel,
		LLMTurns:   *llmTurns,
		Workspace:  *workspace,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()

	mode := "scripted"
	if *useLLM {
		mode = "real agents (Anthropic)"
		if h.orch.workspace != nil {
			fmt.Printf("  workspace %s\n", h.orch.workspace.root)
		}
	}
	fmt.Println("agent-control-room — parallel steerable session on quicrtc")
	fmt.Printf("  mode   %s\n", mode)
	fmt.Printf("  open   %s\n", h.HTTPURL)
	fmt.Printf("  share  %s\n", h.ShareURL)
	if *recordPath != "" {
		fmt.Printf("  record %s\n", *recordPath)
	}
	fmt.Println("(Ctrl-C to stop)")

	<-ctx.Done()
}
