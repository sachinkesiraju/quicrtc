// Network shaper for the agent-desktop demo.
//
// Both transports in the comparison are routed through an in-process
// proxy that emulates the SAME bottleneck link: a fixed one-way
// propagation delay (RTT/2 per direction) plus a single-server
// serialization queue at the configured bandwidth. The QUIC side goes
// through a UDP proxy; the WebSocket side goes through a TCP proxy.
// Identical Shape, identical pacing math — the only difference is
// what each transport does with the link it's given, which is exactly
// the thing the demo measures.
//
// Modeling notes (kept deliberately simple and symmetric):
//
//   - Serialization: packets/chunks pass through one queue per
//     direction; each occupies the "wire" for len*8/bandwidth. This is
//     the classic single-bottleneck router model and is what produces
//     head-of-line queueing when a burst (a big screen frame) lands in
//     front of small messages (tokens).
//
//   - Queue overflow: the UDP queue tail-drops (router behavior; QUIC
//     recovers via retransmission). The TCP queue blocks the reader
//     instead (backpressure propagates into the kernel socket buffer
//     and ultimately the sender, which is how a real TCP bottleneck
//     behaves from the application's point of view).
//
//   - Loss: applied to the UDP path only — a userspace TCP proxy
//     cannot meaningfully drop "packets" (retransmission happens in
//     the kernel below it). A non-zero -loss therefore impairs ONLY
//     the quicrtc path, i.e. it stacks the deck against quicrtc, never
//     for it.
package main

import (
	"math/rand"
	"net"
	"sync"
	"time"
)

// Shape describes the emulated network both transports run through.
type Shape struct {
	RTT  time.Duration // added round-trip time; each direction adds RTT/2
	Mbps float64       // per-direction bottleneck bandwidth; 0 = unlimited
	Loss float64       // packet drop probability [0,1); UDP/QUIC path only
}

// zero reports whether the shape is a no-op (clean loopback); callers
// skip the proxies entirely in that case.
func (s Shape) zero() bool { return s.RTT == 0 && s.Mbps == 0 && s.Loss == 0 }

// pacerQueueLen bounds each direction's bottleneck queue. At ~1300 B
// per QUIC packet this is ~330 KB of buffer — a few hundred ms at the
// demo's default bandwidth, i.e. a realistic (slightly bloated) home
// router, not an infinite queue that would hide loss entirely.
const pacerQueueLen = 256

// pacer is one direction of one emulated link, split into two stages
// so propagation is pipelined the way a real wire is:
//
//	wire goroutine     — owns serialization. Each packet occupies the
//	                     wire for len*8/bandwidth; the next packet
//	                     can't start until the previous one finished
//	                     serializing. This is where queueing delay
//	                     builds when a burst lands.
//	delivery goroutine — owns propagation. Each packet arrives RTT/2
//	                     after it finished serializing. Packets overlap
//	                     in flight; since serialization completions are
//	                     monotonic and the delay is constant, sleeping
//	                     sequentially preserves order.
type pacer struct {
	shape   Shape
	ch      chan []byte
	inFlight chan timedPacket
	write   func([]byte)
	done    chan struct{}
}

type timedPacket struct {
	deliverAt time.Time
	data      []byte
}

func newPacer(shape Shape, write func([]byte)) *pacer {
	p := &pacer{
		shape:   shape,
		ch:      make(chan []byte, pacerQueueLen),
		inFlight: make(chan timedPacket, pacerQueueLen),
		write:   write,
		done:    make(chan struct{}),
	}
	go p.runWire()
	go p.runDelivery()
	return p
}

func (p *pacer) runWire() {
	oneWay := p.shape.RTT / 2
	var nextFree time.Time
	for {
		var data []byte
		select {
		case <-p.done:
			return
		case data = <-p.ch:
		}
		start := time.Now()
		if nextFree.After(start) {
			start = nextFree
		}
		var tx time.Duration
		if p.shape.Mbps > 0 {
			tx = time.Duration(float64(len(data)) * 8 / (p.shape.Mbps * 1e6) * float64(time.Second))
		}
		sent := start.Add(tx)
		nextFree = sent
		if d := time.Until(sent); d > 0 {
			time.Sleep(d)
		}
		select {
		case p.inFlight <- timedPacket{deliverAt: sent.Add(oneWay), data: data}:
		case <-p.done:
			return
		}
	}
}

func (p *pacer) runDelivery() {
	for {
		var pkt timedPacket
		select {
		case <-p.done:
			return
		case pkt = <-p.inFlight:
		}
		if d := time.Until(pkt.deliverAt); d > 0 {
			time.Sleep(d)
		}
		select {
		case <-p.done:
			return
		default:
		}
		p.write(pkt.data)
	}
}

// enqueueDrop offers data to the queue, tail-dropping when full
// (UDP/router semantics).
func (p *pacer) enqueueDrop(data []byte) {
	select {
	case p.ch <- data:
	default:
	}
}

// enqueueBlock blocks until the queue accepts data (TCP backpressure
// semantics) or the pacer is stopped.
func (p *pacer) enqueueBlock(data []byte) {
	select {
	case p.ch <- data:
	case <-p.done:
	}
}

func (p *pacer) stop() {
	select {
	case <-p.done:
	default:
		close(p.done)
	}
}

// lossyRand is a mutex-guarded rand shared by the UDP shaper's two
// directions.
type lossyRand struct {
	mu  sync.Mutex
	rng *rand.Rand
}

func (l *lossyRand) drop(rate float64) bool {
	if rate <= 0 {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rng.Float64() < rate
}

// ── UDP shaper (QUIC path) ──────────────────────────────────────────

type udpShaper struct {
	shape    Shape
	backend  *net.UDPAddr
	listener *net.UDPConn
	rng      *lossyRand
	stopped  chan struct{}

	mu      sync.Mutex
	clients map[string]*udpClient
}

type udpClient struct {
	upstream *net.UDPConn
	toBack   *pacer // client → backend
	toClient *pacer // backend → client
}

// startUDPShaper listens on 127.0.0.1:0 and forwards datagrams to
// backend through the shaped link. Returns the public address and a
// stop func.
func startUDPShaper(backend *net.UDPAddr, shape Shape) (*net.UDPAddr, func(), error) {
	l, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		return nil, nil, err
	}
	s := &udpShaper{
		shape:    shape,
		backend:  backend,
		listener: l,
		rng:      &lossyRand{rng: rand.New(rand.NewSource(time.Now().UnixNano()))},
		stopped:  make(chan struct{}),
		clients:  make(map[string]*udpClient),
	}
	go s.acceptLoop()
	stop := func() {
		close(s.stopped)
		_ = l.Close()
		s.mu.Lock()
		for _, c := range s.clients {
			c.toBack.stop()
			c.toClient.stop()
			_ = c.upstream.Close()
		}
		s.mu.Unlock()
	}
	return l.LocalAddr().(*net.UDPAddr), stop, nil
}

func (s *udpShaper) acceptLoop() {
	buf := make([]byte, 64*1024)
	for {
		n, src, err := s.listener.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if s.rng.drop(s.shape.Loss) {
			continue
		}
		c := s.client(src)
		if c == nil {
			continue
		}
		data := make([]byte, n)
		copy(data, buf[:n])
		c.toBack.enqueueDrop(data)
	}
}

func (s *udpShaper) client(src *net.UDPAddr) *udpClient {
	key := src.String()
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.clients[key]; ok {
		return c
	}
	select {
	case <-s.stopped:
		return nil
	default:
	}
	up, err := net.DialUDP("udp", nil, s.backend)
	if err != nil {
		return nil
	}
	clientAddr := &net.UDPAddr{IP: src.IP, Port: src.Port}
	c := &udpClient{upstream: up}
	c.toBack = newPacer(s.shape, func(b []byte) { _, _ = up.Write(b) })
	c.toClient = newPacer(s.shape, func(b []byte) { _, _ = s.listener.WriteToUDP(b, clientAddr) })
	s.clients[key] = c
	go s.upstreamLoop(c)
	return c
}

func (s *udpShaper) upstreamLoop(c *udpClient) {
	buf := make([]byte, 64*1024)
	for {
		n, err := c.upstream.Read(buf)
		if err != nil {
			return
		}
		if s.rng.drop(s.shape.Loss) {
			continue
		}
		data := make([]byte, n)
		copy(data, buf[:n])
		c.toClient.enqueueDrop(data)
	}
}

// ── TCP shaper (WebSocket path) ─────────────────────────────────────

// tcpChunk is the read granularity for the TCP shaper — one MTU-ish
// payload, so serialization is modeled at the same packet size the
// UDP path sees.
const tcpChunk = 1460

// startTCPShaper listens on 127.0.0.1:0 and forwards each accepted
// connection to backend through the shaped link. Returns the public
// address and a stop func.
func startTCPShaper(backend string, shape Shape) (net.Addr, func(), error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	stopped := make(chan struct{})
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go shapeTCPConn(conn, backend, shape, stopped)
		}
	}()
	stop := func() {
		close(stopped)
		_ = l.Close()
	}
	return l.Addr(), stop, nil
}

func shapeTCPConn(client net.Conn, backend string, shape Shape, stopped chan struct{}) {
	up, err := net.Dial("tcp", backend)
	if err != nil {
		_ = client.Close()
		return
	}
	var wg sync.WaitGroup
	pipe := func(src, dst net.Conn) {
		defer wg.Done()
		p := newPacer(shape, func(b []byte) { _, _ = dst.Write(b) })
		defer p.stop()
		buf := make([]byte, tcpChunk)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				data := make([]byte, n)
				copy(data, buf[:n])
				p.enqueueBlock(data)
			}
			if err != nil {
				// Give the pacer a moment to drain in-flight chunks
				// before the writes start failing; then close both
				// sides so the peer sees EOF.
				time.Sleep(shape.RTT + 50*time.Millisecond)
				_ = client.Close()
				_ = up.Close()
				return
			}
			select {
			case <-stopped:
				_ = client.Close()
				_ = up.Close()
				return
			default:
			}
		}
	}
	wg.Add(2)
	go pipe(client, up)
	go pipe(up, client)
	wg.Wait()
}
