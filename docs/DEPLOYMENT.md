# Production Deployment Guide

Resource requirements, scaling, monitoring, and security for running quicrtc in production.

## Resource Requirements

### Per-Connection Baseline

On a typical modern server (4-8 cores, 16-32GB RAM):

| Resource | Baseline per connection | Notes |
|----------|----------------------|-------|
| Memory   | ~1-2 MB              | QUIC connection state + track buffers |
| CPU      | ~0.1-0.5%            | Varies with track count and data rate |
| Network  | Application-specific | Depends on video bitrate, token rate, etc. |

### Scaling Factors

- **Video bitrate**: 1 Mbps video uses ~10× more CPU/bandwidth than 100 kbps
- **Track count**: Each additional track adds memory and scheduling overhead
- **Subscriber count**: Per-subscriber queues scale linearly in the broadcaster
- **Datagram rate**: High-frequency telemetry (60Hz+) increases CPU load

### Capacity Planning Example

100 concurrent subscribers, each with 1 video track @ 2 Mbps + 1 token track @ 200 tok/s + 1 telemetry track @ 10 Hz:

- **Memory**: 100-200 MB for connections + overhead
- **CPU**: 10-50% utilization (depends on encoding/decoding)
- **Network**: ~200 Mbps outbound (100 × 2 Mbps)

## Horizontal Scaling

### Stateless Design

Sessions are stateful within a single process. For horizontal scaling: use layer 4 (UDP) load balancing with consistent hashing, keep each established session on the same server (affinity), and run each instance independently with no shared state.

### Load Balancer Configuration

```nginx
# Example nginx UDP load balancer
stream {
    upstream quicrtc_backend {
        least_conn;
        server 10.0.1.10:4433;
        server 10.0.1.11:4433;
        server 10.0.1.12:4433;
    }

    server {
        listen 4433 udp;
        proxy_pass quicrtc_backend;
        proxy_timeout 1s;
        proxy_responses 1;
    }
}
```

### Relay for 1:N Fan-Out

For large-scale broadcasts, use the native relay to offload the origin publisher and distribute geographically:

```
Publisher → Relay Server → Multiple Downstream Servers → Subscribers
```

## TLS Certificate Management

### Production Certificates

Use a proper CA-signed certificate in production:

```go
reloader, _ := cert.NewReloader("fullchain.pem", "privkey.pem", cert.ReloaderOptions{})
srv, _ := server.New(server.Config{
    Addr:       ":443",
    CertGetter: reloader.GetCertificate,
    Slug:       os.Getenv("QUICRTC_SLUG"),
    SDP:        sdp,
})
```

### Let's Encrypt with cert-manager

For Kubernetes deployments, use cert-manager:

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: quicrtc-cert
spec:
  secretName: quicrtc-tls
  issuerRef:
    name: letsencrypt-prod
  dnsNames:
    - quicrtc.example.com
```

Mount the secret and use `cert.Bundle`:

```go
certBytes, _ := os.ReadFile("/path/to/tls.crt")
keyBytes, _ := os.ReadFile("/path/to/tls.key")
tlsCert, _ := tls.X509KeyPair(certBytes, keyBytes)
srv, _ := server.New(server.Config{
    Addr:       ":443",
    CertBundle: &cert.Bundle{TLS: tlsCert},
    Slug:       os.Getenv("QUICRTC_SLUG"),
    SDP:        sdp,
})
```

### Certificate Rotation

`cert.Reloader` picks up certificate changes without a restart. For zero-downtime rotation, update the cert files on disk: the reloader detects the change, new connections use the new cert, and existing sessions continue on their original one.

## Rate Limiting and DoS Protection

quicrtc exposes two integration points; per-IP throttling belongs upstream at the load balancer.

### Per-session bandwidth: `Config.InboundRateLimit`

Bounds inbound (PublishBack) traffic per session via a token bucket. Set this in production.

```go
srv, _ := server.New(server.Config{
    Addr: ":4433",
    InboundRateLimit: session.RateLimit{
        MaxAUsPerSecond:   200,
        MaxBytesPerSecond: 5 << 20, // 5 MiB/s
    },
    // ... other fields
})
```

### Per-tenant admission cap: `Config.AuthValidator` + `Config.OnSession`

Validator runs per HELLO and can refuse before any data flows. Pair it with `OnSession` to decrement on disconnect via `SessionHandle.Context().Done()`:

```go
var inFlight sync.Map // tenant -> *atomic.Int64
const maxPerTenant = 100

srv, _ := server.New(server.Config{
    AuthValidator: func(credential string) (string, error) {
        claims, err := jwt.ParseAndVerify(credential, jwks)
        if err != nil {
            return "", err
        }
        v, _ := inFlight.LoadOrStore(claims.TenantID, new(atomic.Int64))
        if v.(*atomic.Int64).Add(1) > maxPerTenant {
            v.(*atomic.Int64).Add(-1)
            return "", errors.New("tenant limit reached")
        }
        return claims.TenantID, nil
    },
    OnSession: func(h server.SessionHandle) {
        go func() {
            <-h.Context().Done()
            if v, ok := inFlight.Load(/* tenant */); ok {
                v.(*atomic.Int64).Add(-1)
            }
        }()
    },
})
```

### Per-IP throttling: at the LB

quicrtc has no per-IP hook by design. Do this at your UDP load balancer (nginx stream, HAProxy, or a `net.PacketConn` wrapper).

### QUIC-Specific Protections

quic-go provides built-in protections:
- Connection ID limits
- Stream limits per connection
- Handshake timeout

quicrtc currently ships with tuned defaults (receive windows 4/16 MiB per-stream, 8/32 MiB per-connection, datagrams enabled, partial-delivery on reset) and does not expose `quic.Config` for caller-side tuning. If you need to override `MaxIdleTimeout`, `KeepAlivePeriod`, or stream caps (for example, for mobile sessions where the radio sleeps), file an issue and we'll add the hook.

## Monitoring and Observability

### Key Metrics

Monitor these metrics to understand system health:

| Metric | Type | Question it answers |
|--------|------|---------------------|
| `connections_total` | Counter | Total connections established |
| `connections_active` | Gauge | Currently active connections |
| `handshake_duration_seconds` | Histogram | Time to establish connection |
| `frames_published_total` | Counter | Frames sent per track |
| `frames_dropped_total` | Counter | Frames dropped (per reason) |
| `bytes_sent_total` | Counter | Total bytes transmitted |
| `subscribers_per_track` | Gauge | Active subscribers per track |
| `errors_total` | Counter | Connection/session errors |

### Prometheus Integration

Implement the `metrics.Metrics` interface (13 methods; see `metrics/metrics.go`) as a Prometheus adapter:

```go
type promMetrics struct {
    sessions   prometheus.Counter
    auSent     *prometheus.CounterVec
    auDropped  *prometheus.CounterVec
    handshake  prometheus.Histogram
    authFailed *prometheus.CounterVec
    // ... add the rest of the 13
}

func newPromMetrics(reg prometheus.Registerer) *promMetrics {
    m := &promMetrics{
        sessions: prometheus.NewCounter(prometheus.CounterOpts{Name: "quicrtc_sessions_started_total"}),
        auSent:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "quicrtc_au_sent_total"}, []string{"kind"}),
        auDropped: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "quicrtc_au_dropped_total"}, []string{"kind", "reason"}),
        handshake: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "quicrtc_handshake_duration_seconds"}),
        authFailed: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "quicrtc_auth_failed_total"}, []string{"reason"}),
    }
    reg.MustRegister(m.sessions, m.auSent, m.auDropped, m.handshake, m.authFailed)
    return m
}

func (m *promMetrics) SessionStarted()                        { m.sessions.Inc() }
func (m *promMetrics) AUSent(kind string, _ int)              { m.auSent.WithLabelValues(kind).Inc() }
func (m *promMetrics) AUDropped(kind, reason string)          { m.auDropped.WithLabelValues(kind, reason).Inc() }
func (m *promMetrics) HandshakeDuration(d time.Duration)      { m.handshake.Observe(d.Seconds()) }
func (m *promMetrics) AuthFailed(reason string)               { m.authFailed.WithLabelValues(reason).Inc() }
// ... implement the remaining methods similarly

// Wire it in:
srv, _ := server.New(server.Config{
    Metrics: newPromMetrics(prometheus.DefaultRegisterer),
    // ...
})
```

quicrtc deliberately does not depend on `prometheus/client_golang`; keep the adapter in your application.

### Logging Best Practices

- Log connection establishment and termination
- Log authentication failures (without sensitive data)
- Log track attachment/detachment events
- Use structured logging (JSON format)
- Include correlation IDs for session tracking

```go
log.Printf("[session] id=%s remote=%s tracks=%v", sessionID, remoteAddr, trackNames)
```

### Alerting

Set up alerts for:
- Connection error rate > 5%
- Frame drop rate > 1%
- Active connections near capacity
- High memory usage (> 80%)
- Certificate expiry (< 7 days)

## Security Considerations

### Authentication in Production

Never use the auto-generated slug in production. Use `AuthValidator` with proper JWT verification, short-lived tokens (5-15 min expiry) plus a refresh mechanism, and ensure the validator returns non-empty tenant IDs for isolation.

```go
AuthValidator: func(credential string) (tenant string, err error) {
    claims, err := jwt.ParseAndVerify(credential, jwks)
    if err != nil {
        return "", err
    }
    if !claims.Scopes.Includes("quicrtc:subscribe") {
        return "", errors.New("missing scope")
    }
    return claims.TenantID, nil
},
```

### Network Security

- **Firewall rules**: Only allow UDP 443 from trusted sources
- **DDoS protection**: Use cloud DDoS protection (Cloudflare, AWS Shield)
- **Private networks**: Deploy behind VPN/private network for internal use
- **IP whitelisting**: For internal tools, whitelist specific IP ranges

### Data Protection

- **Encryption in transit**: TLS 1.3 is mandatory
- **Sensitive data**: Don't put sensitive data in track payloads
- **Logging**: Don't log auth slugs, tokens, or user data
- **Retention**: Implement data retention policies for recorded streams

## High Availability

### Graceful Shutdown

`Server.Shutdown(ctx)` flips draining mode (the `/wt` upgrade handler returns 503 + `Retry-After: 5` to new clients), sends CLOSE to live sessions, and polls until they terminate or `ctx` expires.

```go
srv, _ := server.New(config)
errCh := make(chan error, 1)
go func() { errCh <- srv.ListenAndServe(ctx) }()

<-shutdownSignal // SIGTERM, etc.
drainCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := srv.Shutdown(drainCtx); err != nil {
    log.Printf("drain timed out: %v", err)
}
```

For readiness probes, poll `srv.IsDraining()` and return non-200 while draining so the LB stops routing new connections to this instance.

### Health Checks

Implement health check endpoints:

```go
http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
    // Check: can we accept new connections?
    // Check: are we near resource limits?
    w.WriteHeader(http.StatusOK)
})
```

### Multi-Instance Session Resume: `SessionStore`

`server.SessionStore` is the interface that holds parked session state across socket disconnects:

```go
type SessionStore interface {
    AllocateID() string
    Park(tenant, sessionID string, receivers map[string]*pubsub.Receiver, onEvict func())
    Resume(tenant, sessionID string) map[string]*pubsub.Receiver
}
```

**Important caveat.** A parked session holds in-process `*pubsub.Receiver` channels into broadcasters. These cannot be serialized across processes. So a "shared" backend can hold session metadata (which instance owns the session, expiresAt), but the actual replay path requires either:

1. **Sticky LB routing** (recommended where the LB supports it): reconnect goes back to the same instance that holds the receivers. Use the HELLO/SDP `SessionID` as the affinity key. Pair sticky routing with the default `NewMemorySessionStore()`. No additional configuration needed.
2. **Pre-Upgrade redirect** (added in v1.0.2): instances behind a non-sticky LB cooperate via a `DistributedSessionStore` (e.g. Redis). On reconnect, the `/wt` handler reads the `?session=<id>` query param, asks the store which instance owns the session, and returns a `307 Temporary Redirect` to that instance's `InstanceAddr`. The redirect happens before `wts.Upgrade` hijacks the response; that hand-off is the only place an HTTP redirect can still be issued.
3. **Active session migration** (future): ship replay buffers + reattach to the broadcaster on the new instance. Not implemented today.

#### Pre-Upgrade redirect configuration

Each instance declares its externally-reachable URL plus an
instance ID. Plug a `DistributedSessionStore` implementation into
`Config.SessionStore`; the handler will return 307 redirects to
the instance owning each parked session.

```go
import "github.com/sachinkesiraju/quicrtc/server"

// store implements server.SessionStore + server.DistributedSessionStore.
// A Redis-backed reference implementation is on the roadmap as the
// quicrtc-redis sibling repo; until that ships, write a thin adapter
// against the interfaces in server/session_store.go.
srv, _ := server.New(server.Config{
    Addr:             ":4433",
    SessionStore:     store,
    InstanceID:       hostname,
    InstanceAddr:     "https://agent-1.us-east.example.com:4433",
    EvictionInterval: 30 * time.Second,
    SDP:              sdp,
})
```

The client appends `?session=<id>` to its reconnect URL; the
redirect is transparent to the application code that calls
`client.Dial`. Single-instance deployments don't need to do
anything. Without a `DistributedSessionStore` plugged in, the
redirect path is dormant and behavior matches v0.1.

#### Background eviction loop

When the configured store implements `EvictableStore` (the
in-memory store does), the server runs a periodic sweep at
`Config.EvictionInterval` (default 30 s). Idle servers reclaim
parked sessions whose TTL elapsed without waiting for new
traffic. Remote stores that lean on backend TTLs (Redis with `EX`,
etcd leases) typically don't implement `EvictableStore`; the
backend handles expiry, and the loop never runs for them.

## Deployment Patterns

### Single Server (Small Scale)

For < 1000 concurrent connections:

```
[Load Balancer] → [quicrtc Server] → [Subscribers]
```

### Multi-Server (Medium Scale)

For 1000-10000 concurrent connections:

```
[Load Balancer] → [quicrtc Server 1, 2, 3] → [Subscribers]
```

Use consistent hashing for session affinity.

### Relay Architecture (Large Scale)

For > 10000 concurrent connections or global distribution:

```
[Publisher] → [Origin Server] → [Relay Servers] → [Edge Servers] → [Subscribers]
```

Relays provide:
- Geographic distribution
- Load distribution
- Cached content (if implementing recording)

## Kubernetes Deployment

Example deployment manifest:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: quicrtc
spec:
  replicas: 3
  selector:
    matchLabels:
      app: quicrtc
  template:
    metadata:
      labels:
        app: quicrtc
    spec:
      containers:
      - name: quicrtc
        image: your-registry/quicrtc:latest
        ports:
        - containerPort: 4433
          protocol: UDP
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
        env:
        - name: QUICRTC_SLUG
          valueFrom:
            secretKeyRef:
              name: quicrtc-secrets
              key: slug
---
apiVersion: v1
kind: Service
metadata:
  name: quicrtc
spec:
  selector:
    app: quicrtc
  ports:
  - port: 4433
    protocol: UDP
  type: LoadBalancer
```

## Troubleshooting Production Issues

See [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) for common production issues and solutions.

## Performance Tuning

### QUIC Configuration

QUIC parameters are not currently tunable; quicrtc ships with defaults chosen for multi-modal AI workloads (receive windows 4/16 MiB per-stream, 8/32 MiB per-connection, datagrams enabled). If your workload needs `MaxIdleTimeout` / `KeepAlivePeriod` / stream-cap overrides (most common for long-lived mobile sessions), file an issue.

### GOMAXPROCS

Set GOMAXPROCS to match your CPU cores:

```go
runtime.GOMAXPROCS(runtime.NumCPU())
```

### Memory Limits

Set appropriate memory limits to prevent OOM:

```bash
ulimit -v 2097152  # 2GB limit
```

Or in container orchestration:
```yaml
resources:
  limits:
    memory: "2Gi"
```
