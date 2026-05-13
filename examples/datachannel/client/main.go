// datachannel/client — companion to ./examples/datachannel/server.
//
// Dials the server, then runs a simple chat loop: type a line to
// send a message, the server echoes "ack:<your message>", and the
// server's 1 Hz heartbeat lines interleave naturally. Demonstrates
// that the datachannel is a true bidi long-lived stream — not
// request/reply.
//
// Transcript prefixes:
//   client>  — what you typed
//   server<  — echo reply
//   server*  — server-initiated heartbeat
//
// Run:
//
//	go run ./examples/datachannel/server          # in one terminal
//	go run ./examples/datachannel/client '...'    # in another
//
// Ctrl-D (EOF) on stdin or Ctrl-C exits cleanly.
package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
)

var stdoutMu sync.Mutex

func println(format string, args ...any) {
	stdoutMu.Lock()
	defer stdoutMu.Unlock()
	fmt.Printf(format, args...)
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: client <share-link>\n  (the URL printed by ./examples/datachannel/server)")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	c, err := client.Dial(ctx, os.Args[1], client.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	fmt.Printf("connected sid=%s\n", c.SessionID())
	fmt.Println("type a line + Enter to send. Ctrl-D / Ctrl-C to exit.")
	fmt.Println()

	dc := c.DataChannel()
	if dc == nil {
		log.Fatal("server didn't expose a datachannel")
	}

	st := status.New("client")

	// Reader goroutine — drain server messages into the transcript.
	// Each message is either an echo ("ack:...") or a heartbeat
	// ("heartbeat (uptime Ns)"). We tag by prefix so the developer
	// can SEE that one side talks while the other listens.
	go func() {
		for {
			msg, err := dc.Recv(ctx)
			if err != nil {
				return
			}
			text := string(msg)
			if strings.HasPrefix(text, "ack:") {
				println("server< %s\n", strings.TrimPrefix(text, "ack:"))
				st.Inc("acks", 1)
			} else if strings.HasPrefix(text, "heartbeat") {
				println("server* %s\n", text)
				st.Inc("heartbeats", 1)
			} else {
				println("server? %s\n", text)
			}
		}
	}()

	// Stdin chat loop. Each line becomes a Send.
	scanner := bufio.NewScanner(os.Stdin)
	stdinDone := make(chan struct{})
	go func() {
		defer close(stdinDone)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}
			if err := dc.Send([]byte(line)); err != nil {
				println("send error: %v\n", err)
				return
			}
			println("client> %s\n", line)
			st.Inc("sent", 1)
		}
	}()

	select {
	case <-ctx.Done():
	case <-stdinDone:
		// Give the reader ~300ms to drain any in-flight server replies.
		time.Sleep(300 * time.Millisecond)
	}

	st.Done(
		"control channel is bidi and persistent — your sends and server heartbeats interleaved freely",
		[]status.Field{
			{Key: "uptime", Value: st.Uptime().Truncate(time.Second).String()},
			{Key: "msgs sent", Value: fmt.Sprintf("%d", st.Get("sent"))},
			{Key: "echoes recv", Value: fmt.Sprintf("%d", st.Get("acks"))},
			{Key: "heartbeats", Value: fmt.Sprintf("%d", st.Get("heartbeats"))},
		})
}
