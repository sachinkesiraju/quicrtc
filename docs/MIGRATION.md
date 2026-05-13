# Migration Guide

This guide helps you migrate from legacy quicrtc APIs to the current recommended patterns.

## Overview

quicrtc v0.1.0 introduced `AddTrackSpec` and kind-aware delivery classes. The legacy `AddTrack*` methods still work for backward compatibility, but we recommend migrating to the new APIs for better performance and clearer semantics.

## Migrating from `AddTrack` to `AddTrackSpec`

### Legacy Pattern

```go
// OLD: Legacy AddTrack
srv, _ := server.New(server.Config{
    Addr: ":4433",
    SDP:  wire.SDP{Codec: "avc1.42E01F", Width: 1280, Height: 720, FPS: 30},
})

// Legacy method - no Kind specified
pub := srv.AddTrack("screen", track.KindVideo, 4)
```

### New Pattern

```go
// NEW: AddTrackSpec with explicit Kind
srv, _ := server.New(server.Config{
    Addr: ":4433",
    SDP:  wire.SDP{Codec: "avc1.42E01F", Width: 1280, Height: 720, FPS: 30},
})

// New method - explicit TrackSpec with Kind
pub := srv.AddTrackSpec(server.TrackSpec{
    Name:     "screen",
    Kind:     track.KindVideo,
    Priority: 4,
})
```

### Benefits of `AddTrackSpec`

- **Explicit Kind**: No ambiguity about which delivery class to use
- **Consistent API**: Single method instead of multiple `AddTrack*` variants
- **Future-proof**: Can add fields to `TrackSpec` without breaking existing code
- **Type safety**: Compile-time checking of track configuration

## Complete Migration Example

### Before (Legacy)

```go
package main

import (
    "github.com/sachinkesiraju/quicrtc/server"
    "github.com/sachinkesiraju/quicrtc/track"
    "github.com/sachinkesiraju/quicrtc/wire"
)

func main() {
    srv, _ := server.New(server.Config{
        Addr: ":4433",
        SDP:  wire.SDP{Codec: "avc1.42E01F", Width: 1280, Height: 720, FPS: 30},
    })

    // Legacy methods
    videoPub := srv.AddTrack("screen", track.KindVideo, 4)
    tokenPub := srv.AddTrack("reasoning", track.KindTokens, 2)
    telePub := srv.AddTrackWithTrackID("telemetry", track.KindTelemetry, 7, 0x42)

    go srv.ListenAndServe(ctx)

    // Publishing...
    videoPub.Publish(ctx, pubsub.AccessUnit{Bytes: frame, Keyframe: true})
}
```

### After (New)

```go
package main

import (
    "github.com/sachinkesiraju/quicrtc/server"
    "github.com/sachinkesiraju/quicrtc/track"
    "github.com/sachinkesiraju/quicrtc/wire"
)

func main() {
    srv, _ := server.New(server.Config{
        Addr: ":4433",
        SDP:  wire.SDP{Codec: "avc1.42E01F", Width: 1280, Height: 720, FPS: 30},
    })

    // New method - consistent TrackSpec for all tracks
    videoPub := srv.AddTrackSpec(server.TrackSpec{
        Name:     "screen",
        Kind:     track.KindVideo,
        Priority: 4,
    })
    tokenPub := srv.AddTrackSpec(server.TrackSpec{
        Name:     "reasoning",
        Kind:     track.KindTokens,
        Priority: 2,
    })
    telePub := srv.AddTrackSpec(server.TrackSpec{
        Name:     "telemetry",
        Kind:     track.KindTelemetry,
        Priority: 7,
        TrackID:  0x42,
    })

    go srv.ListenAndServe(ctx)

    // Publishing (unchanged)
    videoPub.Publish(ctx, pubsub.AccessUnit{Bytes: frame, Keyframe: true})
}
```

## Migrating Client APIs

### From `Recv()` to `RecvOn()`

#### Legacy Pattern

```go
// OLD: Recv() on the "primary" track
c, _ := client.Dial(ctx, shareURL, opts)
au, err := c.Recv(ctx)
```

#### New Pattern

```go
// NEW: RecvOn() for explicit track selection
c, _ := client.Dial(ctx, shareURL, opts)
au, err := c.RecvOn(ctx, "reasoning")
```

### Benefits of `RecvOn()`

- **Explicit track selection**: No ambiguity about which track you're reading
- **Multi-track support**: Easy to read from multiple tracks
- **Better error messages**: Clearer errors when track doesn't exist

## Delivery Class Migration

The main benefit of migrating is getting the correct delivery class for your workload. Here's how the mapping works:

### Kind to Delivery Class Mapping

| Kind            | Delivery Class    | Stream Shape                          |
|-----------------|-------------------|---------------------------------------|
| `KindVideo`     | `StreamGOP`       | One uni stream per keyframe           |
| `KindTokens`    | `StreamLowLatency`| One persistent uni stream per track    |
| `KindToolCalls` | `BidiPerCall`     | One bidi stream per call              |
| `KindTelemetry` | `DatagramOrStream`| QUIC datagrams with stream fallback   |

### Legacy Behavior

Without explicit `Kind`, tracks defaulted to `KindVideo` (stream-per-GOP):

```go
// Legacy: This would use StreamGOP even for tokens
pub := srv.AddTrack("tokens", track.KindVideo, 2)  // Wrong delivery class!
```

### New Behavior

With explicit `Kind`, you get the right delivery class:

```go
// New: This uses StreamLowLatency for tokens
pub := srv.AddTrackSpec(server.TrackSpec{
    Name: "tokens",
    Kind: track.KindTokens,  // Correct delivery class
})
```

## Breaking Changes

### v0.1.0 Breaking Changes

There are **no breaking changes** in v0.1.0. All legacy APIs continue to work:

- `AddTrack()` - still works
- `AddTrackWithPriority()` - still works
- `AddTrackWithTrackID()` - still works
- `Recv()` - still works

However, we recommend migrating to:
- `AddTrackSpec()` - for new track registration
- `RecvOn()` - for explicit track reading

### Future Breaking Changes

When quicrtc reaches v1.0, we may:
- Deprecate legacy `AddTrack*` methods
- Deprecate `Recv()` in favor of `RecvOn()`
- Require explicit `Kind` for all tracks

We will provide at least 6 months notice before any breaking changes.

## Testing Your Migration

### 1. Update Your Code

Replace legacy APIs with new ones:

```bash
# Find usages of AddTrack
grep -r "AddTrack" ./your-code

# Find usages of Recv()
grep -r "\.Recv(" ./your-code
```

### 2. Run Tests

```bash
go test ./...
```

### 3. Run Benchmarks

```bash
go test -v -p 1 ./benchmarks/... -timeout=600s
```

### 4. Test with Examples

```bash
# Test the updated examples
go run ./examples/publisher
go run ./examples/subscriber 'share-url'
```

### 5. Monitor Performance

After migration, monitor:
- Token latency (should improve if using `KindTokens`)
- Video latency (should remain similar)
- Frame drop rates (should decrease with proper delivery classes)

## Rollback Plan

If you encounter issues after migration:

1. **Revert to legacy APIs**
   ```go
   // Rollback to legacy
   pub := srv.AddTrack("screen", track.KindVideo, 4)
   ```

2. **Check delivery class**
   ```go
   // Verify the delivery class being used
   fmt.Printf("Delivery class: %v\n", pub.DeliveryClass())
   ```

3. **Report issues**
   - File a GitHub issue with details
   - Include before/after performance metrics
   - Include minimal reproduction code

## Migration Checklist

- [ ] Replace `AddTrack()` with `AddTrackSpec()`
- [ ] Replace `AddTrackWithPriority()` with `AddTrackSpec()`
- [ ] Replace `AddTrackWithTrackID()` with `AddTrackSpec()`
- [ ] Replace `Recv()` with `RecvOn()` where appropriate
- [ ] Add explicit `Kind` to all track specifications
- [ ] Verify delivery classes match your workload
- [ ] Run tests
- [ ] Run benchmarks
- [ ] Monitor performance in production
- [ ] Update documentation

## Need Help?

If you need help migrating:

1. **Check the examples** - See `examples/publisher` for the current pattern
2. **Read the API docs** - pkg.go.dev has the latest API documentation
3. **File an issue** - https://github.com/sachinkesiraju/quicrtc/issues
4. **Ask in discussions** - https://github.com/sachinkesiraju/quicrtc/discussions
