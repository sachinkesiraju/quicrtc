package datachannel

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func TestSendRecvRoundTrip(t *testing.T) {
	var sent [][]byte
	var mu sync.Mutex
	c := New(func(p []byte) error {
		mu.Lock()
		sent = append(sent, append([]byte(nil), p...))
		mu.Unlock()
		return nil
	}, 4)

	if err := c.Send([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if len(sent) != 1 || string(sent[0]) != "hello" {
		t.Fatalf("send didn't reach transport: %v", sent)
	}
	mu.Unlock()

	c.Deliver([]byte("from peer"))
	got, err := c.Recv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "from peer" {
		t.Fatalf("got %q want %q", got, "from peer")
	}
}

func TestRecvRespectsContext(t *testing.T) {
	c := New(func(p []byte) error { return nil }, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := c.Recv(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
}

func TestCloseStopsSendAndRecv(t *testing.T) {
	c := New(func(p []byte) error { return nil }, 4)
	c.Close()
	if err := c.Send([]byte("hi")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Send after close: want ErrClosedPipe, got %v", err)
	}
	_, err := c.Recv(context.Background())
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after close: want EOF, got %v", err)
	}
	// Idempotent.
	c.Close()
}

func TestDeliverAfterClose(t *testing.T) {
	c := New(func(p []byte) error { return nil }, 4)
	c.Close()
	c.Deliver([]byte("ignored")) // must not panic on closed channel
}

func TestInboundBackpressureDropsOldest(t *testing.T) {
	c := New(func(p []byte) error { return nil }, 2)
	c.Deliver([]byte("a"))
	c.Deliver([]byte("b"))
	c.Deliver([]byte("c")) // dropped — buffer full

	got1, err := c.Recv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got2, err := c.Recv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(got1) != "a" || string(got2) != "b" {
		t.Fatalf("expected a,b (drop newest); got %s,%s", got1, got2)
	}
}
