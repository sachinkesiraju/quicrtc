# Authentication

Two layers of subscriber authentication plus one mechanism for
trusting the server's TLS certificate. None is a session-token
system in the JWT sense: auth happens at the HELLO handshake; once
the session is established, TLS provides confidentiality and
integrity. This doc covers what each layer does, when to use which,
and what they don't protect against.

- [The handshake credential (slug)](#the-handshake-credential-slug)
- [Single-tenant: shared slug](#single-tenant-shared-slug)
- [Multi-tenant: AuthValidator + tenant scoping](#multi-tenant-authvalidator--tenant-scoping)
- [Per-track authorization (PublishBack)](#per-track-authorization-publishback)
- [Server identity: real CA cert vs. cert-hash TOFU](#server-identity-real-ca-cert-vs-cert-hash-tofu)
- [Threat model](#threat-model)

## The handshake credential (slug)

Every subscriber sends a HELLO frame as the first message on the
control stream. HELLO carries a `slug` field, an opaque string
the server inspects to decide whether to admit the session. The
field is called "slug" for historical reasons; it can hold a
shared secret, a JWT, an opaque bearer token, or anything else
your application produces.

The server compares the slug **before** any track state is
attached or any data is exchanged. A bad slug returns `ErrAuth`
and the connection is dropped.

Two server-side modes:

| Mode | Configured via | Use when |
|---|---|---|
| Shared slug | `server.Config.Slug` | Single-tenant, internal, demo, or development |
| Validator callback | `server.Config.AuthValidator` | Multi-tenant SaaS, JWT-based identity, anything that needs per-request authz |

If both are set, `AuthValidator` wins.

## Single-tenant: shared slug

The simplest mode. The server holds one secret; every subscriber
must echo it.

```go
srv, _ := server.New(server.Config{
    Addr: ":4433",
    Slug: os.Getenv("QUICRTC_SLUG"), // e.g. a random 128-bit base64url
    SDP:  wire.SDP{Codec: "avc1.42E01F", Width: 1280, Height: 720, FPS: 30},
})
```

If `Slug` is empty, the server generates a fresh 128-bit
base64url slug at startup. Read it back with `srv.Slug()` for
logging, or use `srv.ShareLink()` to get a complete URL with the
slug pre-filled in the fragment.

Comparison is constant-time
([`subtle.ConstantTimeCompare`](https://pkg.go.dev/crypto/subtle#ConstantTimeCompare))
so an attacker cannot mount a timing attack against the slug.

The Go client passes the slug via `client.Options{Slug: ...}` or
parses it from the URL fragment automatically:

```go
c, err := client.Dial(ctx, "https://host:4433/wt#slug=...&hash=...", client.Options{})
```

The TypeScript client takes it the same way:

```typescript
await client.connect('https://host:4433/wt', {
  slug: 'shared-secret-here',
  certHash: '...',
});
```

**Operational notes:**

- Rotate the slug by restarting the server with a new value. Live
  sessions are not invalidated; only new connections require the
  new slug.
- Don't put the slug in URL query strings, server logs, or
  Sentry/etc. payloads. The fragment is fine because browsers
  don't transmit fragments to servers; an `https://...#slug=`
  link is safe to share over a confidential channel.

## Multi-tenant: AuthValidator + tenant scoping

For anything beyond a single shared secret, plug a validator:

```go
srv, _ := server.New(server.Config{
    Addr: ":4433",
    AuthValidator: func(credential string) (tenant string, err error) {
        // credential is the raw HELLO.slug field.
        claims, err := jwt.ParseAndVerify(credential, jwks)
        if err != nil {
            return "", err // any non-nil err rejects the session
        }
        if !claims.Scopes.Includes("quicrtc:subscribe") {
            return "", errors.New("missing scope")
        }
        return claims.TenantID, nil // tenant scope returned to the session
    },
    SDP: wire.SDP{Codec: "avc1.42E01F", Width: 1280, Height: 720, FPS: 30},
})
```

The returned `tenant` string flows into the session as
`session.Tenant` and **namespaces the session store**. Resume
requests carrying a `SessionID` from tenant A cannot match a
parked session belonging to tenant B; the lookup key is
`(tenant, sessionID)`. This is the only cross-tenant isolation
guarantee the library provides; everything else (billing, rate
limiting, per-track ACLs) is the validator and application's
responsibility.

**The validator is on the handshake hot path.** It runs
synchronously inside the per-connection goroutine, before any
data flows. Keep it fast. JWT verification is fine; a database
round-trip per HELLO is not. Cache aggressively (the JWT signing
key, any per-tenant config) outside the callback.

**Constant-time comparison is your responsibility.** The slug
fallback compares constant-time; a JWT validator that does an
internal `==` against a stored token is timing-vulnerable. Use a
proper library or `subtle.ConstantTimeCompare`.

**Empty tenant is allowed but discouraged.** Returning `("",
nil)` lets the session resume across what would otherwise be
isolated boundaries. Acceptable for single-tenant deployments
that just want to validate JWTs centrally; never acceptable for
multi-tenant.

## Per-track authorization (PublishBack)

`AuthValidator` decides who can open a session. It does not decide
what tracks an authenticated subscriber is allowed to publish on
once the session is open. In a single-tenant deployment this is
fine: every authenticated subscriber is trusted. In a multi-tenant
SaaS it isn't. A subscriber authenticated as tenant A should not
be able to call `client.AddTrack({Name: "tenantB/private"})` and
land AUs in tenant B's namespace.

`server.Config.OnAnnounce`, added in v1.0.2, is the hook that
gates inbound `TypeAnnounce` frames per track:

```go
srv, _ := server.New(server.Config{
    Addr:          ":443",
    AuthValidator: validateJWT, // returns tenant
    OnAnnounce: func(tenant, sessionID, trackName, kind string) error {
        if !strings.HasPrefix(trackName, tenant+"/") {
            return fmt.Errorf("track %q not under tenant %q", trackName, tenant)
        }
        return nil
    },
    SDP: sdp,
})
```

Returning a non-nil error rejects the announce. The server emits
a `TypeError` frame carrying a structured
`wire.ErrorPayload{Code: "track_unauthorized", Reason: <err>}`
payload (new in v1.0.2; the legacy plain-bytes payload format
still decodes correctly on older clients via
`wire.UnmarshalError`).

The rejected track is recorded as `announced=true, authorized=false`
in the session's inbound track set. A retry under the same name
lands in the same rejected state, so a misbehaving client cannot
accumulate Announce attempts to confuse server-side accounting.

**Stream-header race fix.** A subscriber-opened uni stream
carries a stream-header naming the track it publishes on
(`wire.TypeStreamHeader`). Without per-track auth, the receiver
queue is created lazily the first time the stream header (or the
matching Announce) names a track. That created a small window
where a subscriber could open the uni stream first, pump AUs into
the queue, and only afterward send the Announce that
`AuthValidator` would have rejected. With `OnAnnounce` set, the
demuxer requires `announced=true && authorized=true` before it
creates the receive queue. Streams naming an unauthorized or
not-yet-announced track are dropped at the header check; no AUs
are enqueued and no inbound state is created.

**The hook is on the control-frame hot path.** `handleAnnounce`
runs from the session goroutine; long-running work (database
lookups, RPCs) should run in a goroutine kicked off from the
callback. The callback itself should be cache-driven.

**Default behavior is unchanged when `OnAnnounce` is nil.** All
announces are accepted, matching v0.1 single-tenant behavior. The
stream-header gate is also a no-op in that case, so legacy
PublishBack workloads keep working.

## Server identity: real CA cert vs. cert-hash TOFU

The slug authenticates the *subscriber*. The TLS certificate
authenticates the *server*. quicrtc supports both standard CA
verification (production) and SHA-256 cert hash pinning (dev,
LAN, ephemeral certs).

### Production: standard TLS chain

For a CA-issued cert (Let's Encrypt, internal PKI, your vendor of
choice) point `Config.CertGetter` at a `*cert.Reloader`. The
reloader watches the cert files on disk and picks up rotations
without a server restart:

```go
reloader, _ := cert.NewReloader("fullchain.pem", "privkey.pem", cert.ReloaderOptions{})
srv, _ := server.New(server.Config{
    Addr:       ":443",
    CertGetter: reloader.GetCertificate,
    Slug:       os.Getenv("QUICRTC_SLUG"),
    SDP:        sdp,
})
```

Clients connect normally; the standard system CA pool validates
the chain. No hash needed in the share link; omit the `#hash=`
fragment entirely.

If you need to supply a static cert (no rotation), construct a
`cert.Bundle` directly with `tls.LoadX509KeyPair`:

```go
tlsCert, _ := tls.LoadX509KeyPair("fullchain.pem", "privkey.pem")
srv, _ := server.New(server.Config{
    Addr:       ":443",
    CertBundle: &cert.Bundle{TLS: tlsCert},
    Slug:       os.Getenv("QUICRTC_SLUG"),
    SDP:        sdp,
})
```

(`Bundle.DERHash` is only populated by `cert.Generate`; production
deployments using a CA-issued cert validate via the chain, so the
hash field is unused.)

### Dev / LAN: cert-hash TOFU pinning

When the server has no CA-signed cert (the auto-generated ECDSA
P-256 cert covers loopback + `Config.CertExtraIPs` for 13 days),
clients pin the cert by SHA-256 hash. The hash is part of the
share link fragment:

```
https://host:4433/wt#slug=...&hash=<base64url-sha256-of-DER>
```

Both Go and TypeScript clients verify the server's presented
certificate matches the pinned hash. Mismatch → connection
refused. The pin works for both fresh and resumed TLS handshakes
(the verifier covers `VerifyPeerCertificate` and
`VerifyConnection`).

This is **trust-on-first-use**: the pin is only as safe as the
channel that delivered the share link. A signed Slack DM is fine;
a public webpage is not.

## Threat model

quicrtc auth is designed for these scenarios:

- An attacker on the same network cannot establish a session
  without the slug. The slug is constant-time compared.
- A subscriber from tenant A cannot resume tenant B's parked
  session, provided `AuthValidator` returns a non-empty tenant.
- An attacker cannot impersonate the server, provided the client
  validates against either a real CA chain or a pre-shared cert
  hash.

quicrtc auth is **not** designed for these:

- **Slug-as-session-token.** The slug is checked once at HELLO.
  If you need to revoke an in-flight session, terminate the
  connection server-side; there is no per-request re-auth.
- **Per-track / per-frame ACLs.** Authorization is binary at
  the session level: either you got in or you didn't. If
  different tracks need different access, run them on separate
  servers or check in your `OnSession` / `OnDataChannel` logic
  before exposing data.
- **Replay protection beyond TLS.** TLS prevents on-path replay
  of frames. quicrtc does not prevent a leaked slug from being
  used to open a *new* session; that's the JWT validator's job
  (use short-lived tokens, check `nbf`/`exp`, etc.).
- **Audit logging.** The library emits an `AuthFailed` metric
  with a short reason; full audit logs (who connected, from
  where, when) are the application's responsibility. Wire them
  in via `OnSession` and your existing logging stack.

## Quick reference

| Field | Where | Purpose |
|---|---|---|
| `server.Config.Slug` | server | Shared secret for single-tenant |
| `server.Config.AuthValidator` | server | Validator callback for multi-tenant |
| `server.Config.CertBundle` | server | Static TLS cert |
| `server.Config.CertGetter` | server | Hot-reloadable TLS cert (e.g. cert-manager) |
| `client.Options.Slug` | Go client | Credential to send in HELLO |
| `client.Options.CertHashB64URL` | Go client | TOFU pin for self-signed certs |
| `client.Options.InsecureSkipVerify` | Go client | Disables verification (**dev only**) |
| `Server.ShareLink()` | server | Returns `https://host/wt#slug=...&hash=...` |
| `session.Session.Tenant` | runtime | Tenant scope returned by AuthValidator |
| `metrics.Metrics.AuthFailed` | runtime | Per-failure counter for observability |
