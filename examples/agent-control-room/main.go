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
	})
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()

	fmt.Println("agent-control-room — parallel steerable session on quicrtc")
	fmt.Printf("  open   %s\n", h.HTTPURL)
	fmt.Printf("  share  %s\n", h.ShareURL)
	if *recordPath != "" {
		fmt.Printf("  record %s\n", *recordPath)
	}
	fmt.Println("(Ctrl-C to stop)")

	<-ctx.Done()
}
