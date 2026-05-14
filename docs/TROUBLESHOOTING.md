# Troubleshooting Guide

This guide covers common issues, debugging techniques, and solutions for quicrtc deployments.

## Connection Issues

### Client Cannot Connect

**Symptoms:**
- Connection timeout
- "connection refused" errors
- Immediate disconnection after connection

**Possible Causes and Solutions:**

1. **Wrong URL or port**
   ```bash
   # Check the server is listening
   netstat -an | grep 4433
   # or
   lsof -i :4433
   ```

2. **Firewall blocking UDP**
   ```bash
   # Allow UDP traffic
   sudo ufw allow 4433/udp
   # or for iptables
   sudo iptables -A INPUT -p udp --dport 4433 -j ACCEPT
   ```

3. **TLS certificate issues**
   - For dev certs: ensure `certHash` matches the server's certificate
   - For production certs: ensure CA chain is properly configured
   ```go
   // Debug: print the certificate hash
   hash := cert.Bundle.DERHash
   fmt.Printf("Cert hash: %s\n", base64url.EncodeToString(hash))
   ```

4. **Slug mismatch**
   - Verify the slug in the URL fragment matches the server's configured slug
   - For multi-tenant: verify JWT validation is working

### Connection Drops Intermittently

**Symptoms:**
- Connection works initially but drops after a few seconds/minutes
- Frequent reconnections

**Possible Causes and Solutions:**

1. **Idle timeout too short**
   ```go
   quicConfig := &quic.Config{
       MaxIdleTimeout: 5 * time.Minute,  // Increase from default
   }
   ```

2. **Network issues**
   - Check for packet loss: `ping -c 100 server`
   - Check RTT: `ping server`
   - Use network monitoring tools

3. **Resource exhaustion**
   - Check server memory: `free -h`
   - Check CPU: `top`
   - Check file descriptors: `ulimit -n`

## Performance Issues

### High Latency

**Symptoms:**
- Tokens/frames arriving slowly
- High p99 latency metrics

**Possible Causes and Solutions:**

1. **Network congestion**
   - Check bandwidth: `iperf3 -c server`
   - Check for competing traffic on the network

2. **Server overload**
   - Check CPU usage: `top`
   - Check connection count
   - Scale horizontally if needed

3. **Incorrect delivery class**
   - Ensure you're using the right `Kind` for your workload
   - Video should use `KindVideo` (stream-per-GOP)
   - Tokens should use `KindTokens` (persistent stream)

4. **Backpressure**
   - Check if subscribers are slow to consume
   - Monitor frame drop rates
   - Implement publisher-side backpressure handling

### Low Frame Rate

**Symptoms:**
- Video frames arriving at < expected FPS
- Choppy video playback

**Possible Causes and Solutions:**

1. **Encoder bottleneck**
   - Check encoder CPU usage
   - Reduce resolution or bitrate
   - Use hardware acceleration if available

2. **Network bandwidth**
   - Check available bandwidth
   - Reduce video bitrate
   - Implement adaptive bitrate

3. **Subscriber processing**
   - Check client-side decoding performance
   - Use WebCodecs for hardware-accelerated decoding
   - Implement frame skipping on slow clients

## Browser-Specific Issues

### WebTransport Not Available

**Symptoms:**
- "WebTransport is not defined" errors
- Connection fails in browser console

**Solutions:**

1. **Check browser support**
   ```javascript
   if (!('WebTransport' in window)) {
       console.error('WebTransport not supported');
   }
   ```

2. **Enable flag in Firefox**
   - Go to `about:config`
   - Set `network.webtransport.enabled` to `true`

3. **Enable in Safari**
   - Safari > Develop > Experimental Features > WebTransport

### Safari Connection Issues

**Symptoms:**
- Connection fails only in Safari
- Works in Chrome/Edge

**Solutions:**

1. **Check Safari version**
   - Requires Safari 18.2+ (GA December 2024; no flag needed)
   - Older 17.x builds need the WebTransport flag in Develop → Experimental Features

2. **Certificate issues**
   - Safari has stricter certificate validation
   - Ensure proper CA chain for production certs

### CORS Errors

**Symptoms:**
- CORS errors in browser console
- Connection blocked by cross-origin policy

**Solutions:**

1. **Configure CORS headers**
   ```go
   http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
       w.Header().Set("Access-Control-Allow-Origin", "*")
       // ... rest of handler
   })
   ```

2. **Use same origin**
   - Serve from the same domain as the API
   - Or configure proper CORS policies

## Authentication Issues

### Auth Failed Errors

**Symptoms:**
- `ErrAuth` returned on connection
- Connection rejected immediately

**Possible Causes and Solutions:**

1. **Slug mismatch**
   ```go
   // Debug: log the slug being compared
   log.Printf("Expected slug: %s, Got: %s", srv.Slug(), receivedSlug)
   ```

2. **JWT validation failure**
   - Check JWT signature verification
   - Check token expiration
   - Check required scopes

3. **Constant-time comparison issues**
   - Ensure you're using `subtle.ConstantTimeCompare`
   - Don't use `==` for slug comparison

### Multi-Tenant Session Resume Issues

**Symptoms:**
- Session resume fails across tenants
- Wrong session resumed

**Solutions:**

1. **Ensure tenant isolation**
   ```go
   AuthValidator: func(credential string) (tenant string, err error) {
       // MUST return non-empty tenant for multi-tenant
       return claims.TenantID, nil
   },
   ```

2. **Check session store keys**
   - Sessions should be keyed by `(tenant, sessionID)`
   - Not just `sessionID`

## Memory Issues

### High Memory Usage

**Symptoms:**
- Server process using excessive memory
- OOM kills

**Possible Causes and Solutions:**

1. **Too many buffered frames**
   - Reduce per-subscriber queue sizes
   - Implement more aggressive drop policies

2. **Memory leaks**
   - Check for goroutine leaks: `runtime.NumGoroutine()`
   - Use pprof to profile memory
   ```bash
   go tool pprof http://localhost:6060/debug/pprof/heap
   ```

3. **Large frames**
   - Check frame sizes in metrics
   - Implement frame size limits
   - Use appropriate codecs

### Memory Growing Over Time

**Symptoms:**
- Memory usage increases steadily
- No memory leaks in code

**Possible Causes and Solutions:**

1. **Session not cleaned up**
   - Ensure proper session cleanup on disconnect
   - Implement session timeout

2. **Metric collection overhead**
   - Reduce metric granularity
   - Sample metrics instead of collecting all

## Debugging Techniques

### Enable QUIC debug logging (qlog)

quic-go writes per-connection [qlog](https://github.com/quicwg/qlog) files when the `QLOGDIR` environment variable is set. No code change required.

```bash
QLOGDIR=/tmp/qlogs ./your-quicrtc-server
```

Each connection produces one `.qlog` file. Open with [qvis.edm.uhasselt.be](https://qvis.edm.uhasselt.be/) for a timeline view, or [qlog-tools](https://github.com/quic-tracker/qlog-tools) for grep-friendly text.

For application-level logging, pass a `*slog.Logger` via `server.Config.Logger` — server events (session start/end, auth failures, track lifecycle) go through it.

### Use pprof for Profiling

```go
import (
    _ "net/http/pprof"
    "net/http"
)

go func() {
    log.Println(http.ListenAndServe("localhost:6060", nil))
}()
```

Then:
```bash
# CPU profile
go tool pprof http://localhost:6060/debug/pprof/profile

# Memory profile
go tool pprof http://localhost:6060/debug/pprof/heap

# Goroutine profile
go tool pprof http://localhost:6060/debug/pprof/goroutine
```

### Network Debugging

```bash
# Capture QUIC traffic
tcpdump -i any -w quic.pcap udp port 4433

# Analyze with Wireshark
wireshark quic.pcap
```

### Client-Side Debugging

```javascript
const client = new QuicRTCClient(/* options */);

// Connection state transitions
client.onStateChange((state) => {
    console.log('[quicrtc state]', state);
});

// Server-signaled per-track backpressure (subscriber slow-consumer hints)
client.onBackpressure((track, level) => {
    console.log('[quicrtc backpressure]', track, level);
});

// Periodic introspection: poll getStats() at whatever cadence you want
setInterval(() => {
    const stats = client.getStats();
    console.log('[quicrtc stats]', stats); // auSent, sessionId, connectionState, trackCount, remoteTrackCount
}, 5000);
```

## Common Error Messages

### "ErrAuth"

Authentication failed. Check:
- Slug matches server configuration
- JWT is valid and not expired
- AuthValidator is correctly implemented

### "ErrTimeout"

Operation timed out. Check:
- Network connectivity
- Server load
- Timeout configuration

### "ErrStreamReset"

Stream was reset. Check:
- Publisher stopped publishing
- Network issues caused stream reset
- Flow control blocked stream

### "ErrBufferFull"

Buffer is full. Check:
- Subscriber not consuming fast enough
- Queue sizes too small
- Need more aggressive drop policy

## Getting Help

If you're still stuck:

1. **Check the logs**
   - Server logs for connection errors
   - Client console for browser errors
   - Network logs for transport issues

2. **Search existing issues**
   - GitHub Issues: https://github.com/sachinkesiraju/quicrtc/issues
   - Check for similar problems

3. **Create a minimal reproduction**
   - Simplify your code to the minimum that shows the issue
   - Test with the example applications
   - Document your environment (OS, browser versions, etc.)

4. **File an issue**
   - Include error messages
   - Include minimal reproduction code
   - Include environment details
   - Include logs (redact sensitive data)

## Performance Baselines

Expected performance under good conditions:

| Metric | Expected Range |
|--------|----------------|
| Setup latency | < 2 ms (loopback), 10-30 ms (LAN) |
| Token latency | < 1 ms (loopback), < 10 ms (LAN) |
| Video latency | < 5 ms (loopback), < 30 ms (LAN) |
| Session resume | < 2 ms (loopback), < 20 ms (LAN) |

If you're seeing significantly worse performance, see the Performance Issues section above.
