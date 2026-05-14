// wan_bench: split-mode benchmark for WAN testing.
//
// On the SERVER VM (e.g., us-east1):
//   ./wan_bench -mode=server -external-host=$(curl -s ifconfig.me) -ivf=input.ivf
//
// On the CLIENT VM (e.g., us-west1):
//   ./wan_bench -mode=client -server=<server-public-ip> -test=setup -n=20 -csv=setup.csv
//   ./wan_bench -mode=client -server=<server-public-ip> -test=multi -runs=5 -csv=multi.csv
//   ./wan_bench -mode=client -server=<server-public-ip> -test=resume -n=20 -csv=resume.csv
//   ./wan_bench -mode=client -server=<server-public-ip> -test=scale -total=200 -csv=scale.csv
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	flagMode         = flag.String("mode", "server", "server | client")
	flagExternalHost = flag.String("external-host", "", "public IP/host the server should advertise")
	flagIVF          = flag.String("ivf", "input.ivf", "VP8 .ivf path (server mode)")
	flagServer       = flag.String("server", "", "server host (client mode)")
	flagTest         = flag.String("test", "setup", "setup | multi | resume | scale | computer_use (client mode)")
	flagN            = flag.Int("n", 100, "iterations (setup, resume) - increased from 20 for better statistical power")
	flagRuns         = flag.Int("runs", 20, "runs (multi) - increased from 10 for better statistical power")
	flagTotal        = flag.Int("total", 200, "total sessions (scale)")
	flagBatch        = flag.Int("batch", 25, "concurrent dial batch (scale)")
	flagInterval     = flag.Duration("interval", time.Second, "interval between scale batches")
	flagCSV          = flag.String("csv", "results.csv", "output CSV path (client mode)")

	// computer_use mode flags. -mode-name is an alias for -test that
	// matches the prompt's documented invocation; either form selects
	// the test.
	flagModeName    = flag.String("mode-name", "", "alias for -test; if non-empty, overrides -test (e.g. computer_use)")
	flagScreenMbps  = flag.Float64("screen-mbps", 12, "computer_use: target screen bitrate in Mbps (default 12; try 60 for stress)")
	flagTrials      = flag.Int("trials", 3, "computer_use: number of trials per arm")
	flagTrialSec    = flag.Int("trial-sec", 12, "computer_use: seconds per trial")
	flagLossPct     = flag.Float64("loss-pct", 0, "computer_use server: if >0, apply tc netem loss% to -loss-iface (Linux only; no-op if tc unavailable)")
	flagLossIface   = flag.String("loss-iface", "eth0", "computer_use server: interface for -loss-pct (e.g. eth0, ens4)")
	flagTCPBaseAddr = flag.String("tcp-baseline-addr", ":4435", "computer_use server: TCP-mux baseline TLS listen addr")
)

func main() {
	flag.Parse()

	if *flagMode == "server" {
		host := *flagExternalHost
		if host == "" {
			host = "127.0.0.1"
		}
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			sig := make(chan os.Signal, 1)
			signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
			<-sig
			cancel()
		}()
		// Apply optional synthetic loss for the computer_use mode.
		// Linux-only; on other OSes or when tc is missing this is a
		// logged no-op. Cleanup runs on SIGTERM via the ctx-cancel path.
		cleanupNetem := startComputerUseNetem(*flagLossPct, *flagLossIface)
		defer cleanupNetem()
		if err := runServer(ctx, host, *flagIVF); err != nil {
			fmt.Fprintln(os.Stderr, "server:", err)
			os.Exit(1)
		}
		return
	}

	// Client mode
	if *flagServer == "" {
		fmt.Fprintln(os.Stderr, "-server required for client mode")
		os.Exit(2)
	}
	info, err := fetchInfo(*flagServer)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fetch /info:", err)
		os.Exit(1)
	}
	fmt.Printf("connected to server %s\n", info.ExternalHost)

	// -mode-name overrides -test when set. This matches the prompt's
	// documented "-mode-name=computer_use" invocation while remaining
	// back-compatible with the existing -test=setup|multi|resume|scale
	// dispatch path.
	testName := *flagTest
	if *flagModeName != "" {
		testName = *flagModeName
	}

	// computer_use writes its own per-action CSV format and emits its
	// own headline line; the other modes flow through the shared
	// writeCSV/summarize pipeline so their existing CSVs are
	// byte-compatible with the wan_results/ outputs the README's
	// numbers came from.
	if testName == "computer_use" {
		runComputerUseTest(info, *flagServer)
		return
	}

	var samples []sample
	switch testName {
	case "setup":
		samples = runSetupTest(info, *flagN)
	case "multi":
		samples = runMultiTest(info, *flagRuns)
	case "resume":
		samples = runResumeTest(info, *flagN)
	case "scale":
		samples = runScaleTest(info, *flagTotal, *flagBatch, *flagInterval)
	default:
		fmt.Fprintln(os.Stderr, "unknown -test:", testName)
		os.Exit(2)
	}

	writeCSV(*flagCSV, samples)
	summarize(samples)
	fmt.Printf("\nwrote %s (%d samples)\n", *flagCSV, len(samples))
}
