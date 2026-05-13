# Contributing to quicrtc

Wire format is stable; public APIs may shift before v1.0.

## Development setup

```bash
git clone https://github.com/sachinkesiraju/quicrtc.git
cd quicrtc
go build ./...           # Go 1.26+ recommended (1.25+ works for core library)
cd ts-sdk && npm install # Node 18+ required
```

## Running the tests

Every change should pass the full suite plus the race detector on
the concurrency-heavy packages.

```bash
# Go
go test ./... -count=1 -short -timeout=180s
go test -race ./feed ./session ./wire ./track ./pubsub
go vet ./...

# TypeScript SDK
cd ts-sdk
npx tsc --noEmit
npm test
npm run bench       # smoke-test the wire benchmarks
```

End-to-end real-QUIC benchmarks live in `benchmarks/`:

```bash
go test -v -p 1 ./benchmarks/... -timeout=600s
```

`-p 1` keeps the heavy benchmark subpackages from competing for CPU.

CI runs all of the above on every PR (see
[`.github/workflows/test.yml`](../.github/workflows/test.yml)).

## What we accept

- **Bug fixes** — any race, leak, or correctness issue. Please
  include a regression test.
- **Performance improvements** — must come with a `Benchmark*`
  function. Speed-claim PRs without numbers don't land.
- **New delivery classes / kinds** — discuss in an issue first; the
  abstraction was designed for four classes and adding a fifth has
  protocol-level implications.
- **Documentation and example improvements** — always welcome.
- **TypeScript SDK additions** — must be wire-compatible with the
  Go server (verified by the wire test suite on both sides).

## What we don't accept

- Style-only changes without behavior or readability impact.
- Breaking wire format changes outside an explicit v2 design
  discussion. v1 receivers must continue to interop.
- Code that bypasses `DeliveryClass` dispatch (e.g., direct
  `Stream.Write` from outside the pump).

## Pull request norms

1. **One concern per PR.** Big diffs are hard to review.
2. **Tests pass locally** before opening.
3. **Public API changes** include updated doc comments and a
   CHANGELOG entry.
4. **Wire format additions** include a corresponding TypeScript
   wire-test case so the JS side stays in sync.

## Wire format guarantees

- v1 (the current format) is **frozen** as of v0.1.0. Receivers
  ignore unknown control frame types, so additive changes are
  forward-compatible.
- v2 (reserved fields in `pubsub.AccessUnit` and the metadata
  block) is **opt-in via capability negotiation** — not a breaking
  upgrade.
- Bumping the wire major version (v1 → v2) is a major version bump
  in semver.

## Code style

- Go: standard `gofmt`. No additional linters required, but
  `go vet ./...` must pass.
- TypeScript: TypeScript strict mode. The existing eslint disable
  comments mark intentional snake_case wire fields (`needs_keyframe`,
  `from_seq`) that must match Go's JSON tags.

## Where to ask

Open an issue for design questions before sending large PRs. For
small bug fixes, the PR description is enough.

## License

By contributing, you agree your contribution is licensed under the
Apache License 2.0, the same as the rest of the project. See
[`LICENSE`](../LICENSE).
