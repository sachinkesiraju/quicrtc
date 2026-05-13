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
tlsCert, _ := tls.LoadX509KeyPair(certBytes, keyBytes)
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

### Per-IP Rate Limiting

Implement rate limiting at the connection establishment layer:

```go
import "golang.org/x/time/rate"

var ipLimiter = sync.Map{} // map[string]*rate.Limiter

func getLimiter(ip string) *rate.Limiter {
    limiter, _ := ipLimiter.LoadOrStore(ip, rate.NewLimiter(10, 5))
    return limiter.(*rate.Limiter)
}

// In your connection handler:
if !getLimiter(clientIP).Allow() {
    // Reject connection
}
```

### Per-Slug Session Limits

Limit concurrent sessions per authentication slug:

```go
type SessionLimiter struct {
    maxSessions int
    counts      map[string]int
    mu          sync.Mutex
}

func (l *SessionLimiter) Allow(slug string) bool {
    l.mu.Lock()
    defer l.mu.Unlock()
    if l.counts[slug] >= l.maxSessions {
        return false
    }
    l.counts[slug]++
    return true
}
```

### QUIC-Specific Protections

quic-go provides built-in protections:
- Connection ID limits
- Stream limits per connection
- Handshake timeout

Configure these in your QUIC config:

```go
quicConfig := &quic.Config{
    MaxIdleTimeout:        30 * time.Second,
    MaxIncomingStreams:   1000,
    MaxIncomingUniStreams: 1000,
}
```

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

Use the built-in metrics:

```go
import "github.com/prometheus/client_golang/prometheus"

registry := prometheus.NewRegistry()
// Register quicrtc metrics (if exposed)
```

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

Implement graceful shutdown to complete in-flight sessions:

```go
srv, _ := server.New(config)
go srv.ListenAndServe(ctx)

// On shutdown signal:
shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
defer shutdownCancel()

// Stop accepting new connections
// Send CLOSE frames to existing sessions
// Wait for sessions to terminate or timeout
```

### Health Checks

Implement health check endpoints:

```go
http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
    // Check: can we accept new connections?
    // Check: are we near resource limits?
    w.WriteHeader(http.StatusOK)
})
```

### Database Backing (Optional)

For session resume across server restarts, consider:

1. **Redis**: Store session state for fast resume
2. **PostgreSQL**: For durable session storage
3. **Etcd**: For distributed configuration

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

Tune QUIC parameters for your workload:

```go
quicConfig := &quic.Config{
    MaxIdleTimeout:        60 * time.Second,  // Longer for mobile
    MaxIncomingStreams:   100,               // Limit per connection
    KeepAlivePeriod:       10 * time.Second,
}
```

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
