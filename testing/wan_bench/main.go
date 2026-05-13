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
	flagTest         = flag.String("test", "setup", "setup | multi | resume | scale (client mode)")
	flagN            = flag.Int("n", 100, "iterations (setup, resume) - increased from 20 for better statistical power")
	flagRuns         = flag.Int("runs", 20, "runs (multi) - increased from 10 for better statistical power")
	flagTotal        = flag.Int("total", 200, "total sessions (scale)")
	flagBatch        = flag.Int("batch", 25, "concurrent dial batch (scale)")
	flagInterval     = flag.Duration("interval", time.Second, "interval between scale batches")
	flagCSV          = flag.String("csv", "results.csv", "output CSV path (client mode)")
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

	var samples []sample
	switch *flagTest {
	case "setup":
		samples = runSetupTest(info, *flagN)
	case "multi":
		samples = runMultiTest(info, *flagRuns)
	case "resume":
		samples = runResumeTest(info, *flagN)
	case "scale":
		samples = runScaleTest(info, *flagTotal, *flagBatch, *flagInterval)
	default:
		fmt.Fprintln(os.Stderr, "unknown -test:", *flagTest)
		os.Exit(2)
	}

	writeCSV(*flagCSV, samples)
	summarize(samples)
	fmt.Printf("\nwrote %s (%d samples)\n", *flagCSV, len(samples))
}
