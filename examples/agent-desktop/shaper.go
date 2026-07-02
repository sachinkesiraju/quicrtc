// Network shaper for the agent-desktop demo.
//
// Every transport arm in the comparison is routed through an
// in-process proxy that emulates the SAME bottleneck link: a fixed
// one-way propagation delay (RTT/2 per direction) plus a single-server
// serialization queue at the configured bandwidth. The quicrtc arm
// goes through a UDP proxy; each WebSocket/SSE arm goes through a TCP
// proxy. Identical Shape, identical pacing math.
//
// Crucially, ALL connections belonging to one arm share ONE link —
// the glued-stack arm doesn't get 8 Mbps per connection, it gets
// 8 Mbps total across its frame socket, event socket, and SSE stream,
// exactly like one user's wifi. (Corollary: the demo compares fairly
// for one viewer per arm; a second browser tab shares that arm's
// link like a second device on the same network.)
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

// Shape describes the emulated network every arm runs through.
type Shape struct {
	RTT  time.Duration // added round-trip time; each direction adds RTT/2
	Mbps float64       // per-direction bottleneck bandwidth; 0 = unlimited
	Loss float64       // packet drop probability [0,1); UDP/QUIC path only
}

// zero reports whether the shape is a no-op (clean loopback); callers
// skip the proxies entirely in that case.
func (s Shape) zero() bool { return s.RTT == 0 && s.Mbps == 0 && s.Loss == 0 }

// pacerQueueLen bounds each direction's bottleneck queue. At ~1300 B
// per QUIC packet this is ~85 KB of buffer — ~85 ms at the demo's
// default 8 Mbps, a realistic access-point queue. The size matters:
// a deep queue would itself add hundreds of ms of FIFO bufferbloat to
// EVERY flow and mask the transports' own behavior; a shallow one
// lets congestion control (QUIC's and TCP's alike) find the link rate
// and keep the standing queue small, so the latency the demo measures
// is the transports' queueing, not the router's.
const pacerQueueLen = 64

// packet is one queued unit on the emulated wire. deliver is bound to
// the destination socket/conn, so one shared pacer can serve many
// connections (the "all conns share one link" property).
type packet struct {
	data    []byte
	deliver func([]byte)
}

type timedPacket struct {
	deliverAt time.Time
	data      []byte
	deliver   func([]byte)
}

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
	shape    Shape
	ch       chan packet
	inFlight chan timedPacket
	done     chan struct{}
}

func newPacer(shape Shape) *pacer {
	p := &pacer{
		shape:    shape,
		ch:       make(chan packet, pacerQueueLen),
		inFlight: make(chan timedPacket, pacerQueueLen),
		done:     make(chan struct{}),
	}
	go p.runWire()
	go p.runDelivery()
	return p
}

func (p *pacer) runWire() {
	oneWay := p.shape.RTT / 2
	var nextFree time.Time
	for {
		var pkt packet
		select {
		case <-p.done:
			return
		case pkt = <-p.ch:
		}
		start := time.Now()
		if nextFree.After(start) {
			start = nextFree
		}
		var tx time.Duration
		if p.shape.Mbps > 0 {
			tx = time.Duration(float64(len(pkt.data)) * 8 / (p.shape.Mbps * 1e6) * float64(time.Second))
		}
		sent := start.Add(tx)
		nextFree = sent
		if d := time.Until(sent); d > 0 {
			time.Sleep(d)
		}
		select {
		case p.inFlight <- timedPacket{deliverAt: sent.Add(oneWay), data: pkt.data, deliver: pkt.deliver}:
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
		pkt.deliver(pkt.data)
	}
}

// enqueueDrop offers a packet to the queue, tail-dropping when full
// (UDP/router semantics).
func (p *pacer) enqueueDrop(pkt packet) {
	select {
	case p.ch <- pkt:
	default:
	}
}

// enqueueBlock blocks until the queue accepts the packet (TCP
// backpressure semantics) or the pacer is stopped.
func (p *pacer) enqueueBlock(pkt packet) {
	select {
	case p.ch <- pkt:
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

// ── UDP shaper (QUIC arm) ───────────────────────────────────────────

type udpShaper struct {
	shape    Shape
	backend  *net.UDPAddr
	listener *net.UDPConn
	rng      *lossyRand
	up       *pacer // clients → backend, one shared wire
	down     *pacer // backend → clients, one shared wire
	stopped  chan struct{}

	mu      sync.Mutex
	clients map[string]*udpClient
}

type udpClient struct {
	upstream *net.UDPConn
	addr     *net.UDPAddr
}

// startUDPShaper listens on 127.0.0.1:0 and forwards datagrams to
// backend through one shared shaped link. Returns the public address
// and a stop func.
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
		up:       newPacer(shape),
		down:     newPacer(shape),
		stopped:  make(chan struct{}),
		clients:  make(map[string]*udpClient),
	}
	go s.acceptLoop()
	stop := func() {
		close(s.stopped)
		_ = l.Close()
		s.up.stop()
		s.down.stop()
		s.mu.Lock()
		for _, c := range s.clients {
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
		up := c.upstream
		s.up.enqueueDrop(packet{data: data, deliver: func(b []byte) { _, _ = up.Write(b) }})
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
	c := &udpClient{
		upstream: up,
		addr:     &net.UDPAddr{IP: src.IP, Port: src.Port},
	}
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
		addr := c.addr
		s.down.enqueueDrop(packet{data: data, deliver: func(b []byte) { _, _ = s.listener.WriteToUDP(b, addr) }})
	}
}

// ── TCP shaper (WebSocket / SSE arms) ───────────────────────────────

// tcpChunk is the read granularity for the TCP shaper — one MTU-ish
// payload, so serialization is modeled at the same packet size the
// UDP path sees.
const tcpChunk = 1460

// startTCPShaper listens on 127.0.0.1:0 and forwards every accepted
// connection to backend through ONE shared shaped link (all
// connections of the arm contend for the same wire). Returns the
// public address and a stop func.
func startTCPShaper(backend string, shape Shape) (net.Addr, func(), error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	up := newPacer(shape)
	down := newPacer(shape)
	stopped := make(chan struct{})
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go shapeTCPConn(conn, backend, up, down, stopped, shape)
		}
	}()
	stop := func() {
		close(stopped)
		_ = l.Close()
		up.stop()
		down.stop()
	}
	return l.Addr(), stop, nil
}

func shapeTCPConn(client net.Conn, backend string, up, down *pacer, stopped chan struct{}, shape Shape) {
	upstream, err := net.Dial("tcp", backend)
	if err != nil {
		_ = client.Close()
		return
	}
	var wg sync.WaitGroup
	pipe := func(src, dst net.Conn, wire *pacer) {
		defer wg.Done()
		buf := make([]byte, tcpChunk)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				data := make([]byte, n)
				copy(data, buf[:n])
				wire.enqueueBlock(packet{data: data, deliver: func(b []byte) { _, _ = dst.Write(b) }})
			}
			if err != nil {
				// Give the shared wire a moment to flush this conn's
				// in-flight chunks, then close both sides so the peer
				// sees EOF.
				time.Sleep(shape.RTT + 50*time.Millisecond)
				_ = client.Close()
				_ = upstream.Close()
				return
			}
			select {
			case <-stopped:
				_ = client.Close()
				_ = upstream.Close()
				return
			default:
			}
		}
	}
	wg.Add(2)
	go pipe(client, upstream, up)
	go pipe(upstream, client, down)
	wg.Wait()
}
