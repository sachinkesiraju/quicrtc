# agent-control-room — parallel, steerable, forkable agent session

The **10× product thesis** on quicrtc: the cloud agent desktop is a **live
control room**, not a video feed you babysit.

| Capability | What it proves |
|---|---|
| **Parallel sub-agents** | Three specialists (`tests`, `code`, `ship`) run concurrently; each tool call is its own bidi stream |
| **Steer** | Cancel one worker or all via publish-back `steer` track — without stopping the screen |
| **Fork** | Checkpoint embedded in `.qrtc` recording; restore session state from any moment |

One QUIC/WebTransport session carries screen, multiplexed reasoning, parallel
tool calls, telemetry datagrams, and client steer input.

## Run

```bash
go run ./examples/agent-control-room
# open http://127.0.0.1:8430
```

With recording (enables fork-from-recording API):

```bash
go run ./examples/agent-control-room -record /tmp/session.qrtc
```

### Real agents (Anthropic)

Three parallel specialists call Claude with real tool execution in a shared Go workspace (`go test`, `edit_file`, `git commit`):

```bash
export ANTHROPIC_API_KEY=sk-ant-...
go run ./examples/agent-control-room -llm
# optional: -model claude-sonnet-4-20250514 -llm-turns 8 -workspace /tmp/ws
```

| Worker | Tools | Role |
|--------|-------|------|
| `tests` | run_command, finish_task | Reproduce failing test |
| `code` | open_file, edit_file, run_command, finish_task | Patch `billing/invoice.go` |
| `ship` | run_command, git_commit, finish_task | Commit + PR |

Steer inject is fed back into each worker's next API turn. Cancel aborts in-flight API calls via context.

Without `-llm`, workers use the scripted session (CI-safe, no API key).

## Validate the thesis (CI)

```bash
go run ./examples/agent-control-room -thesis-bench
go test ./examples/agent-control-room/ -count=1 -v
go test -tags browser ./examples/agent-control-room/ -run TestBrowser -count=1 -v
go test ./testing/benchmarks/network/ -run TestControlRoom -count=1 -v
```

**Pass criteria:**

| Experiment | Target | Test |
|---|---|---|
| E1 parallel vs serial (3×250ms tools) | ≥2.5× wall-clock speedup | `TestParallelVsSerialThesis`, `TestFullScriptParallelSpeedup` |
| E2 steer cancel during active tool work | ≤150ms to stop | `TestSteerCancelUnderLoadThesis`, `TestWebTransportSteerCancel` |
| E3 fork from recording checkpoint | ≤500ms restore | `TestForkRestoreThesis`, `TestForkFromRecordingHTTP`, `TestRecordingSeekSnapshot` |
| HTTP fork API | checkpoint round-trip | `TestHTTPCheckpointAndFork` |
| WebTransport screen recv | first frame | `TestWebTransportRecvScreen` |
| Shaped network (40ms one-way) | screen through proxy | `TestControlRoomWebTransportUnderShapedNetwork` |
| Browser e2e | connect + fork UI | `TestBrowserConnectAndFork` (`-tags browser`) |

## UI

- **Screen** — shared desktop (invoice-fixing task from agent-desktop script)
- **Reasoning** — tokens prefixed `[tests]` / `[code]` / `[ship]`
- **Tool columns** — per-worker call state: `running` / `done` / `cancelled`
- **Steer** — cancel all, cancel per worker, inject context, fork from checkpoint

## Architecture

```
Viewer (1 WebTransport session)
  ├─ screen        KindVideo
  ├─ reasoning     KindTokens
  ├─ toolcalls     KindToolCalls (parallel bidi)
  ├─ telemetry     datagrams
  └─ steer         publish-back (cancel / inject)
```

This is the production-shaped layer above transport benchmarks
(`agent-desktop` proves ~2× latency) — **new capability**, not faster pixels.
