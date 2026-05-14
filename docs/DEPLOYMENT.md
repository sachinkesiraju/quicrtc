# Production Deployment Guide

This guide covers deploying quicrtc in production environments, including resource requirements, scaling strategies, monitoring, and security considerations.

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

For a server handling 100 concurrent subscribers with:
- 1 video track @ 2 Mbps
- 1 token track @ 200 tokens/sec
- 1 telemetry track @ 10 Hz

Estimated requirements:
- **Memory**: 100-200 MB for connections + overhead
- **CPU**: 10-50% utilization (depends on encoding/decoding)
- **Network**: ~200 Mbps outbound (100 × 2 Mbps)

## Horizontal Scaling

### Stateless Design

quicrtc sessions are stateful within a single server process. For horizontal scaling:

1. **Load balancing**: Use layer 4 (UDP) load balancing with consistent hashing
2. **Session affinity**: Once a session is established, it must stay on the same server
3. **No shared state**: Each server instance is independent

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

For large-scale broadcasts, use the native relay to distribute load:

```
Publisher → Relay Server → Multiple Downstream Servers → Subscribers
```

This reduces the load on the origin publisher and allows geographic distribution.

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

The `cert.Reloader` automatically picks up certificate changes without restart. For zero-downtime rotation:

1. Update the certificate files on disk
2. The reloader detects the change
3. New connections use the new certificate
4. Existing sessions continue on their original certificate

## Rate Limiting and DoS Protection

quicrtc exposes two integration points; per-IP throttling belongs upstream at the load balancer.

### Per-session bandwidth — `Config.InboundRateLimit`

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

### Per-tenant admission cap — `Config.AuthValidator` + `Config.OnSession`

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

### Per-IP throttling — at the LB

quicrtc has no per-IP hook by design. Do this at your UDP load balancer (nginx stream, HAProxy, or a `net.PacketConn` wrapper).

### QUIC-Specific Protections

quic-go provides built-in protections:
- Connection ID limits
- Stream limits per connection
- Handshake timeout

quicrtc currently ships with tuned defaults (receive windows 4/16 MiB per-stream, 8/32 MiB per-connection, datagrams enabled, partial-delivery on reset) and does not expose `quic.Config` for caller-side tuning. If you need to override `MaxIdleTimeout`, `KeepAlivePeriod`, or stream caps — for example for mobile sessions where the radio sleeps — file an issue and we'll add the hook.

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

Implement the `metrics.Metrics` interface (13 methods — see `metrics/metrics.go`) as a Prometheus adapter:

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

quicrtc deliberately does not depend on `prometheus/client_golang` — keep the adapter in your application.

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

Never use the auto-generated slug in production. Always use:

1. **JWT validation**: Implement `AuthValidator` with proper JWT verification
2. **Short-lived tokens**: Use JWTs with short expiration (5-15 minutes)
3. **Token refresh**: Implement a refresh token mechanism
4. **Tenant isolation**: Ensure `AuthValidator` returns non-empty tenant IDs

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

For readiness probes, poll `srv.IsDraining()` — return non-200 while draining so the LB stops routing new connections to this instance.

### Health Checks

Implement health check endpoints:

```go
http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
    // Check: can we accept new connections?
    // Check: are we near resource limits?
    w.WriteHeader(http.StatusOK)
})
```

### Multi-Instance Session Resume — `SessionStore`

`server.SessionStore` is the interface that holds parked session state across socket disconnects:

```go
type SessionStore interface {
    AllocateID() string
    Park(tenant, sessionID string, receivers map[string]*pubsub.Receiver, onEvict func())
    Resume(tenant, sessionID string) map[string]*pubsub.Receiver
}
```

**Important caveat.** A parked session holds in-process `*pubsub.Receiver` channels into broadcasters. These cannot be serialized across processes. So a "shared" backend can hold session metadata (which instance owns the session, expiresAt) — but the actual replay path requires:

1. **Sticky LB routing** (recommended): reconnect goes back to the same instance that holds the receivers. Use the HELLO/SDP `SessionID` as the affinity key. Pair sticky routing with the default `NewMemorySessionStore()`.
2. **Active session migration** (future): ship replay buffers + reattach to the broadcaster on the new instance. Not implemented today.

A shared store is useful for telling the LB which instance to route to; it does not let any instance serve any session.

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

QUIC parameters are not currently tunable; quicrtc ships with defaults chosen for multi-modal AI workloads (receive windows 4/16 MiB per-stream, 8/32 MiB per-connection, datagrams enabled). If your workload needs `MaxIdleTimeout` / `KeepAlivePeriod` / stream-cap overrides — most common for long-lived mobile sessions — file an issue.

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
