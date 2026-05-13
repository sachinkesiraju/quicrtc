// UDP proxy for injecting realistic network conditions (delay,
// jitter, packet loss) between a quicrtc client and server. Both
// endpoints run on loopback; the proxy sits between them at a
// different UDP port and forwards each datagram through a configured
// NetCond filter.
//
// This is a Go-level approximation of `tc qdisc add dev lo netem`.
// It's not perfect — proxy goroutine scheduling adds its own jitter,
// and we can't model bottleneck bandwidth queuing — but it captures
// the dominant effects (RTT, loss) needed to validate quicrtc's
// architectural advantages.

package loadgen

import (
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// NetCond configures the proxy's forwarding behavior.
type NetCond struct {
	// OneWayDelay is added to each forwarded datagram in each
	// direction. Total RTT contributed by the proxy = 2× OneWayDelay.
	OneWayDelay time.Duration
	// LossRate is the probability [0,1) that a datagram is dropped.
	// Applied independently in each direction.
	LossRate float64
	// JitterPlusMinus is added/subtracted uniformly to OneWayDelay.
	// Total spread = 2× JitterPlusMinus per direction.
	JitterPlusMinus time.Duration
}

// udpProxy listens on a local UDP port and forwards datagrams to a
// fixed backend address with the configured NetCond filter.
type udpProxy struct {
	cond     NetCond
	backend  *net.UDPAddr
	listener *net.UDPConn
	stopped  atomic.Bool
	wg       sync.WaitGroup
	rng      *rand.Rand
	rngMu    sync.Mutex

	clientsMu sync.Mutex
	clients   map[string]*proxyClient
}

type proxyClient struct {
	addr     *net.UDPAddr
	upstream *net.UDPConn
}

// StartUDPProxy binds a listener on 127.0.0.1:0 and forwards all
// inbound datagrams to backend with the given NetCond. Returns the
// proxy's listen address and a stop function.
func StartUDPProxy(backend *net.UDPAddr, cond NetCond) (*net.UDPAddr, func(), error) {
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		return nil, nil, err
	}
	p := &udpProxy{
		cond:     cond,
		backend:  backend,
		listener: listener,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
		clients:  make(map[string]*proxyClient),
	}
	p.wg.Add(1)
	go p.runDownstream()
	stop := func() {
		p.stopped.Store(true)
		_ = listener.Close()
		p.clientsMu.Lock()
		for _, c := range p.clients {
			_ = c.upstream.Close()
		}
		p.clientsMu.Unlock()
		p.wg.Wait()
	}
	return listener.LocalAddr().(*net.UDPAddr), stop, nil
}

func (p *udpProxy) runDownstream() {
	defer p.wg.Done()
	buf := make([]byte, 64*1024)
	for {
		n, src, err := p.listener.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if p.shouldDrop() {
			continue
		}
		client := p.getOrCreateClient(src)
		if client == nil {
			continue
		}
		data := make([]byte, n)
		copy(data, buf[:n])
		delay := p.delayWithJitter()
		go func() {
			if delay > 0 {
				time.Sleep(delay)
			}
			if p.stopped.Load() {
				return
			}
			_, _ = client.upstream.Write(data)
		}()
	}
}

func (p *udpProxy) runUpstream(client *proxyClient) {
	defer p.wg.Done()
	buf := make([]byte, 64*1024)
	for {
		n, err := client.upstream.Read(buf)
		if err != nil {
			return
		}
		if p.shouldDrop() {
			continue
		}
		data := make([]byte, n)
		copy(data, buf[:n])
		delay := p.delayWithJitter()
		addr := client.addr
		go func() {
			if delay > 0 {
				time.Sleep(delay)
			}
			if p.stopped.Load() {
				return
			}
			_, _ = p.listener.WriteToUDP(data, addr)
		}()
	}
}

func (p *udpProxy) getOrCreateClient(src *net.UDPAddr) *proxyClient {
	key := src.String()
	p.clientsMu.Lock()
	defer p.clientsMu.Unlock()
	if c, ok := p.clients[key]; ok {
		return c
	}
	upstream, err := net.DialUDP("udp", nil, p.backend)
	if err != nil {
		return nil
	}
	c := &proxyClient{
		addr:     &net.UDPAddr{IP: src.IP, Port: src.Port},
		upstream: upstream,
	}
	p.clients[key] = c
	p.wg.Add(1)
	go p.runUpstream(c)
	return c
}

func (p *udpProxy) shouldDrop() bool {
	if p.cond.LossRate <= 0 {
		return false
	}
	p.rngMu.Lock()
	defer p.rngMu.Unlock()
	return p.rng.Float64() < p.cond.LossRate
}

func (p *udpProxy) delayWithJitter() time.Duration {
	d := p.cond.OneWayDelay
	if p.cond.JitterPlusMinus > 0 {
		p.rngMu.Lock()
		jitter := time.Duration(p.rng.Int63n(int64(p.cond.JitterPlusMinus*2))) - p.cond.JitterPlusMinus
		p.rngMu.Unlock()
		d += jitter
	}
	if d < 0 {
		return 0
	}
	return d
}
