# AGENTS.md

## Cursor Cloud specific instructions

quicrtc is a **Go library + server/client** (root module `github.com/sachinkesiraju/quicrtc`, Go 1.25) plus a zero-dependency **TypeScript browser SDK** in `ts-sdk/`. There is no database, broker, or hosted backend — "running it" means starting an example Go server and consuming it with a Go client or the browser viewer. Canonical build/lint/test commands live in `.github/workflows/test.yml`; the TS scripts live in `ts-sdk/package.json`.

### Services / how to run
- **Go example servers** (e.g. `go run ./examples/publisher`, `go run ./examples/agent_pubsub/server`): bind to an ephemeral `127.0.0.1:0` port and print a `share: https://127.0.0.1:NNNN/wt#slug=...&hash=...` URL on stdout. The TLS cert is auto-generated per run and its hash is pinned in the URL fragment — always copy the exact printed URL.
- **Go client**: `go run ./examples/subscriber '<share-url>'` (or `./examples/agent_pubsub/viewer`).
- **Browser viewer** (flagship visual demo, all four lanes): build the SDK once with `npm --prefix ts-sdk run build` (writes `ts-sdk/dist/`), serve the SDK dir with `python3 -m http.server 5173` from `ts-sdk/`, then open `http://localhost:5173/examples/viewer/`, paste the server's share URL, and click connect. WebTransport with the self-signed cert works in Chrome because the viewer passes the pinned cert hash from the URL fragment.

### Non-obvious gotchas
- **Example status lines are TTY-gated.** The live `[subscriber] frames=…` counter and the end-of-run `── summary ──` only render when stdout is a real TTY. Piping through `tee`/a file shows nothing after the initial `connected`/`sdp` lines. To capture progress, run inside a tmux pane *without* piping and use `capture-pane`, or send SIGINT (Ctrl-C, not SIGTERM/`timeout`) to get the summary — the examples only trap `os.Interrupt`.
- **`failed to sufficiently increase receive buffer size` is benign.** quic-go wants a 7 MiB UDP buffer; the sandbox kernel caps it lower. Short runs and tests still pass.
- **Nested Go modules need Go 1.26**, which is not installed here. `examples/cua-live/` and `testing/benchmarks/browser/` have their own `go.mod` requiring Go 1.26 and are NOT covered by a root `go build ./...`. They are optional and CI-excluded (`cua-live -live` also needs `ANTHROPIC_API_KEY` + Chromium). The main library builds/tests fine on Go 1.25.
- **Benchmark tests must run serially** (`-p 1`) — they share CPU via real QUIC loopback and inflate each other's latencies otherwise. See the `go test ./testing/benchmarks/...` step in CI.

### Commands (run Go from repo root, npm from `ts-sdk/`)
- Go build/vet: `go build ./...` · `go vet ./...`
- Go library tests: `go test $(go list ./... | grep -v '/testing/benchmarks/') -count=1 -short -timeout=180s`
- Go benchmark tests (serial): `go test ./testing/benchmarks/... -count=1 -short -timeout=300s -p 1`
- Go race: `go test -race ./feed ./wire ./session ./track ./pubsub -count=1 -timeout=240s`
- TS: `npm --prefix ts-sdk run build` · `npm --prefix ts-sdk test` · `npx --prefix ts-sdk tsc --noEmit`
