# durable-go

[![CI](https://github.com/agenticenv/durable-go/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/agenticenv/durable-go/actions/workflows/ci.yml)
[![Security](https://github.com/agenticenv/durable-go/actions/workflows/security.yml/badge.svg?branch=main)](https://github.com/agenticenv/durable-go/actions/workflows/security.yml)
[![Release](https://img.shields.io/github/v/release/agenticenv/durable-go?label=Release)](https://github.com/agenticenv/durable-go/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/agenticenv/durable-go.svg)](https://pkg.go.dev/github.com/agenticenv/durable-go)
[![License](https://img.shields.io/github/license/agenticenv/durable-go?label=License)](LICENSE)

**Lightweight, embeddable durable task execution for Go.**

**durable-go** lets you define typed tasks, run memoized steps, and persist progress so work can resume safely after failures or restarts. Useful for any Go app that needs reliable, resumable workflows without a heavy orchestration framework.

> Releases follow [Semantic Versioning](https://semver.org/); see the [latest release](https://github.com/agenticenv/durable-go/releases/latest).

## Features

- **Engine API** — `NewEngine`, `RegisterTask`, `RunTask`, `RunStep` in a single package.
- **Async fan-out** — `RunStep` starts work and returns immediately, matching `RunTask`; call several before `Get`-ing any of them.
- **First-of-N** — `StepRun.Done()` exposes a read-only channel so you can `select` across handles and react to whichever finishes first.
- **Memoized steps** — completed steps replay from the journal; they are not run again. Failed steps replay their stored error instead of re-running `fn`.
- **Pending steps** — return `ErrStepPending` and complete later via `CompleteStep` (human approval, webhooks). Suspends only that step's `Get`, not the whole task — sibling steps keep running.
- **Cancellation** — `CancelRun` persists a durable cancel signal and cancels the run's `ctx` immediately if it is executing in this process; every `RunStep` call, including on a later resume, fails fast with `ErrRunCancelled` instead of re-running `fn`.
- **Fire-and-forget runs** — `RunTask` returns immediately; `TaskRun.Get` waits; `RunID()` is available at once.
- **Step observability** — `GetStep` / `LoadSteps` for current state, `WatchSteps` for a live/historical event stream (including STARTED).
- **Timeouts and retries** — engine / task / run / step options. Retries default to 0 (opt-in).
- **Panic recovery** — task and step panics are recorded and returned as errors.
- **Auto-purge** — optional background cleanup of old completed and failed runs.
- **Flexible execution** — tasks as `durable.Func` closures or structs with `Exec`.
- **Payload privacy** — optional `PayloadCodec` (built-in AES-GCM); owner-only journal modes; inspect `--redact`.

## Why durable-go

Most durable-execution frameworks require external infrastructure—such as a dedicated workflow server or a Postgres database—and enforce strict code execution models like replay determinism.

`durable-go` takes a zero-infra, in-process approach: a single Go library with a filesystem journal running inside your application process. Instead of replaying entire function call graphs from an external orchestrator, `durable-go` memoizes individual step results. On resume the task runs again from the top; completed steps return the cached result. There is no replay-determinism sandbox. The journal is files, not SQLite: no schema migrations when the library changes, and no connection/busy-lock handling for a single-process writer. See [Use cases](#use-cases) for more places where durable-go is a perfect fit.

> **One writer per dataDir.** `NewEngine` takes an exclusive OS flock on `<dataDir>/.lock`. Do not open the same directory from two writer processes. Another process can open the same directory with `NewReadOnlyEngine` (shared lock).

## Install

```bash
go get github.com/agenticenv/durable-go@latest
```

Go 1.26.5+. No infrastructure required.

## Quick Start

```go
e, err := durable.NewEngine(ctx, "./data", durable.WithLogger(logger))
if err != nil { ... }
defer e.Close()

err = durable.RegisterTask(e, "process-order", durable.Func(
    func(ctx context.Context, s *durable.StepRunner, in OrderInput) (OrderOutput, error) {
        charged, err := durable.RunStep(ctx, s, "charge", in, func(ctx context.Context, in OrderInput) (string, error) {
            return chargeCard(in)
        }).Get(ctx)
        if err != nil {
            return OrderOutput{}, err
        }
        shipped, err := durable.RunStep(ctx, s, "ship", charged, func(ctx context.Context, charged string) (string, error) {
            return scheduleShip(charged)
        }).Get(ctx)
        if err != nil {
            return OrderOutput{}, err
        }
        return OrderOutput{Result: shipped}, nil
    },
))

run := durable.RunTask[OrderInput, OrderOutput](ctx, e, "process-order", "", input)
storeRunID(run.RunID())      // available immediately before Get
output, err := run.Get(ctx)  // block for result
```

Full example: [`examples/func-task/`](examples/func-task/).

### Struct-based tasks

For services with injected dependencies, implement `Exec` on a struct and pass it to `RegisterTask`:

```go
type Job struct {
    DB   *Database
    Mail Mailer
}

func (j *Job) Exec(ctx context.Context, s *durable.StepRunner, id string) (string, error) {
    return durable.RunStep(ctx, s, "notify", id, func(ctx context.Context, id string) (string, error) {
        return j.Mail.Send(ctx, id)
    }).Get(ctx)
}

durable.RegisterTask(e, "notify", &Job{DB: db, Mail: mailer})
run := durable.RunTask[string, string](ctx, e, "notify", "", "42")
out, err := run.Get(ctx)
```

Full example: [`examples/struct-task/`](examples/struct-task/).

### Pending steps

A step suspends itself by returning `ErrStepPending`. This blocks only that step's `Get` — sibling steps started before it keep running. An external caller completes it with the token from `StepToken(ctx)`:

```go
approval, err := durable.RunStep(ctx, s, "approve", struct{}{}, func(ctx context.Context, _ struct{}) (Approval, error) {
    token := s.StepToken(ctx)
    sendEmail("manager@co.com", token)
    return Approval{}, durable.ErrStepPending
}).Get(ctx)

// webhook / CLI / another goroutine:
durable.CompleteStep(ctx, e, token, Approval{By: "manager@co.com"})
```

### Fan-out and first-of-N

`RunStep` starts work and returns immediately — the same shape as `RunTask`. Start several steps before calling `Get` on any of them to run them concurrently, then join:

```go
charge := durable.RunStep(ctx, s, "charge", order, func(ctx context.Context, order Order) (string, error) {
    return chargeCard(order)
})
notify := durable.RunStep(ctx, s, "notify", order, func(ctx context.Context, order Order) (string, error) {
    return sendReceipt(order)
})
chargeResult, err := charge.Get(ctx)
if err != nil {
    return Output{}, err
}
notifyResult, err := notify.Get(ctx)
```

To react to whichever of several steps finishes first, `select` on `Done()` instead of blocking on `Get`:

```go
a := durable.RunStep(ctx, s, "provider-a", req, callProviderA)
b := durable.RunStep(ctx, s, "provider-b", req, callProviderB)
select {
case <-a.Done():
    result, err := a.Get(ctx)
case <-b.Done():
    result, err := b.Get(ctx)
}
```

Full example: [`examples/fanout/`](examples/fanout/).

### Cancellation

`CancelRun` persists a durable cancel signal (reusing the same journal plumbing as `CompleteStep`) and, if the run is executing in this process, cancels the `ctx` delivered to that run's `Task.Exec` and every in-flight `RunStep` call:

```go
run := durable.RunTask[OrderInput, OrderOutput](ctx, e, "process-order", "", input)
// ... later, from another goroutine or request:
if err := e.CancelRun(ctx, "process-order", run.RunID()); err != nil {
    // ErrRunAlreadyFinished if the run already completed or failed.
}
_, err := run.Get(ctx) // errors.Is(err, durable.ErrRunCancelled)
```

- **Immediate for a running process.** A step already blocked on `ctx.Done()` (e.g. inside `waitForSignal` after `ErrStepPending`, or a step function that itself selects on `ctx`) unblocks right away.
- **Durable across a crash.** The cancel signal is written to the journal before this call returns. If the process crashes before the in-process cancellation above takes effect, the next `RunTask` for that taskID/runID cancels `ctx` before `Task.Exec` is invoked at all — every subsequent `RunStep` call, including a cached/replayed one, returns `ErrRunCancelled` immediately without running `fn`.
- **Cooperative, like any Go `ctx`.** `RunStep.Get`/`RunTask.Get` return promptly regardless of whether the step's goroutine has exited, but the engine's drain (`Close`, and any run reaching a terminal state) waits for it to actually return — see rule 4 in [Writing tasks](#writing-tasks).
- The run ends up `StatusFailed` (the same terminal status used for `Close` and timeouts) with `TaskInfo.Error` equal to `durable.ErrRunCancelled.Error()`, so callers can tell a deliberate cancel apart from another failure.

### Resume

Register tasks after every `NewEngine`, then resume active runs. Pass the saved runID (or `""` to resume the oldest Running/Waiting run for that taskID). Completed steps replay from the journal.

```go
durable.RegisterTask(e, "process-order", ...)
pending, _ := e.ListTasks(ctx, durable.StatusRunning, durable.StatusWaiting)
for _, t := range pending {
    run := durable.RunTask[OrderInput, OrderOutput](ctx, e, t.TaskID, t.RunID, OrderInput{})
    go func() { _, _ = run.Get(ctx) }()
}
```

`RunTask` writes `input.json` on first start. The same runID reloads it; the input argument is ignored.

`GetStep` and `LoadSteps` return current state — the latest record per step, in-memory-map order (not sorted). `WatchSteps` returns history and live updates instead: every STARTED, WAITING, COMPLETED, and FAILED event in the order it was written, either from the beginning (`fromOffset` 0) or resuming past a previously-seen `Offset` / `ByteOffset`. Cancelling the watch does not stop the run. A watch that falls behind may have events dropped (logged as a warning) rather than blocking the run — reconnect with the last `Offset`/`ByteOffset` you saw to catch up.

Full example: [`examples/resume/`](examples/resume/).

## Writing tasks

Follow these when you write a task. On resume, the task runs again from the top; completed steps are reused, not re-executed.

1. **Side effects in `RunStep`.** Do not call an API, write to a database, or publish to a queue in the task body. Wrap that work in `durable.RunStep`.
2. **Non-deterministic values in `RunStep`.** Do not use `time.Now()`, UUIDs, or random values in the task body to choose a step ID or a branch. Generate them inside a `RunStep` so resume sees the same result.
3. **Idempotent steps.** A crash can re-run a step after the side effect already happened. Charging a card or sending mail must be safe to do twice (or no-op).
4. **Check `ctx` to be cancellable.** `CancelRun`, `WithStepTimeout`, and engine `Close` only cancel `ctx` — they cannot forcibly stop a step function. Select on `ctx.Done()` in any loop or long-running step body, and pass `ctx` to ctx-aware calls (`http.NewRequestWithContext`, a `database/sql` `*Context` method, etc.). A step that never checks `ctx` keeps running in the background past cancellation/timeout/Close, and the engine still waits for it to actually return before the run reaches a terminal state.

Also:

- **Unique step IDs** — one stable string per step (literals or `fmt.Sprintf("step-%d", i)`). Reusing an ID panics.
- **Bound the steps in one run.** Resume scans that run’s `journal.log`. A fixed list of steps (including `fmt.Sprintf("step-%d", i)` with a known N) is fine. Do not put an unbounded loop of new step IDs in one run. Start a new `RunTask` when the work is a new unit (new agent session, next batch). Large LLM/tool payloads in every step also grow the file — keep stored results small when you can. Use `WithAutoPurge` so finished runs do not pile up.
- **Step IDs are the resume key — never rename one.** The journal matches records by stepID string only. Renaming a step between deploys orphans the old result: on the next run `fn` executes again under the new name as if it had never run. Treat a stepID like a database column name, not a display label.
- **Concurrent `RunStep` calls are safe.** Start several steps before `Get`-ing any of them to fan out; join with `Get` or `select` on `Done()`. A duplicate stepID within one run still panics.
- **One stepID is reserved.** `RunStep` panics if `stepID` is `"\x00cancel"` — it is reserved internally for `CancelRun`'s durable signal. Any human-readable stepID you would actually choose is unaffected.
- **JSON results** — step and task outputs must be JSON-marshalable. Step input `in` must be too.
- **No secrets or PII in I/O or errors.** Task input, step `in`, and results are persisted. err.Error() and panic values are stored in the journal (plaintext even with WithPayloadCodec) and may be logged. Pass IDs; load credentials and personal data inside `fn` from env or a secret manager. Enabling a codec or journal MAC later is not a migrate — see [Data privacy](#data-privacy--sensitive-payloads).
- **One task input** — `I` is a single value, not variadic args. Bundle multiple fields in one struct; a task with no payload uses `struct{}` and `struct{}{}`.
- **One step input** — `in` is a single value, not variadic args. Bundle multiple fields in one struct. No payload: `struct{}` and `struct{}{}`. Stored for inspect.
- **Bump step version on the next deploy** — resume returns the cached result if `stepID` is unchanged, even when `fn` or `in` changed. If this step’s **code or params** change and in-flight runs must re-execute it, set `WithStepVersion` to a new string (`"1"` → `"2"`) or rename the stepID (`charge` → `charge-v2`). Same version (or no version) = cache. Side effects on re-run are the caller’s problem (idempotent steps).
- **Same runID to resume** — `input.json` is reloaded; you do not need to pass the original input again.

## Examples

Runnable examples in [examples/](examples/) — see [examples/README.md](examples/README.md) for setup and run instructions.

| Example | What it shows |
|---------|----------------|
| [`examples/resume/`](examples/resume/) | One `go run`: crash after step 2, resume from cache |
| [`examples/func-task/`](examples/func-task/) | Closure-style `durable.Func` |
| [`examples/struct-task/`](examples/struct-task/) | Struct task with injected deps, retries, timeout |
| [`examples/fanout/`](examples/fanout/) | Concurrent `RunStep`, `Get`-all join, `ErrStepPending` |
| [`examples/yaml-task/`](examples/yaml-task/) | YAML file as one task; each YAML step is a `RunStep` |
| [`examples/payload-codec/`](examples/payload-codec/) | Plaintext vs AES-GCM vs custom codec, HMAC tokens, journal MAC, inspect flags |

```bash
# from repo root
go run ./examples/resume/
go run ./examples/func-task/
go run ./examples/struct-task/
go run ./examples/fanout/
go run ./examples/yaml-task/
go run ./examples/payload-codec/
```

## Inspect CLI

`durable-inspect` is a read-only viewer for a journal (`task list` / `task get` / `step list` / `step get`). `--dir` / `-d` wins over `DURABLE_DIR`. Set `DURABLE_PAYLOAD_KEY` and `DURABLE_JOURNAL_MAC_KEY` in the environment (do not pass keys on the command line). Hex is tried first for both. `--redact` hides INPUT and RESULT.

```bash
go install github.com/agenticenv/durable-go/cmd/durable-inspect@latest
durable-inspect -d ./data task list
```

Commands, flags, lookup by name or ID, and the writer-lock behavior: see **[`cmd/durable-inspect/README.md`](cmd/durable-inspect/README.md)**.

## Data privacy & sensitive payloads

The journal is files on disk. **Default persist is plaintext JSON** (`input.json`, `output.json`, step Input/Result, `CompleteStep` payloads). Do not treat `dataDir` as a secret store.

**Keep secrets out of I/O.** Pass order IDs, user IDs, or blob handles. Fetch credentials and PII inside the step `fn` from the environment or a secret manager.

**Optional at-rest codec.** `WithPayloadCodec` wraps those blobs after JSON marshal. Built-in AES-GCM (16/24/32-byte key; never written under `dataDir`):

```go
key, err := hex.DecodeString(os.Getenv("DURABLE_PAYLOAD_KEY"))
codec, err := durable.NewAESGCMCodec(key)
e, err := durable.NewEngine(ctx, "./data", durable.WithPayloadCodec(codec))
```

Open the same journal with `WithPayloadCodec` or `durable-inspect --payload-key`. Resume without the matching codec+key fails closed. Inspect without a key prints stored ciphertext; a wrong `--payload-key` fails closed. Any reversible `Encode`/`Decode` works; AAD binds each blob to kind/task/run/step so ciphertext cannot be copied between fields. `--redact` is inspect-only — it is not a codec.

```go
type kmsCodec struct{ client KMS }

func (c kmsCodec) Encode(plaintext, aad []byte) ([]byte, error) {
    return c.client.Encrypt(plaintext, aad)
}
func (c kmsCodec) Decode(ciphertext, aad []byte) ([]byte, error) {
    return c.client.Decrypt(ciphertext, aad)
}

e, err := durable.NewEngine(ctx, "./data", durable.WithPayloadCodec(kmsCodec{client: kms}))
```

**File modes.** Unix directories are `0700` and files `0600`. `NewEngine` chmods an existing `dataDir` and warns if group/world bits remain. Windows chmod is best-effort; encryption still helps there.

**Step tokens.** `CompleteStep` tokens are unsigned with no expiry unless you set `WithStepTokenKey` (HMAC, default 24h TTL). Override with `WithDefaultStepTokenTTL` or per-step `WithStepTokenTTL`. The key is not stored in `dataDir`.

**Journal MAC.** Default frames end in CRC32 (torn-write detection only). `WithJournalMACKey` replaces that trailer with HMAC-SHA256 and also appends a 32-byte HMAC to `input.json`, `output.json`, and `meta.json` (bound to task/run) so those files cannot be rewritten either. The key is not stored in `dataDir`. Inspect reads `DURABLE_JOURNAL_MAC_KEY` (hex first, else raw). Deleting files is still possible.

**Same `dataDir`, same options.** One directory, one codec, one journal MAC key (or none). Those options apply to every run in that tree. Do not turn them on later against an existing plaintext/CRC directory — resume and inspect fail closed.

To add AES and/or a journal MAC and **keep the old journal**: open a **second** `NewEngine` on a new `dataDir` and send new work there. Finish or cancel in-flight runs on the old engine. Wipe the old tree only if you do not need it.

```go
plain, err := durable.NewEngine(ctx, "./data-plain")
secure, err := durable.NewEngine(ctx, "./data-secure",
    durable.WithPayloadCodec(codec),
    durable.WithJournalMACKey(macKey),
)
```

Inspect: one `-d` per directory; set `DURABLE_PAYLOAD_KEY` / `DURABLE_JOURNAL_MAC_KEY` in the environment (not flags). Walkthrough: [`examples/payload-codec/`](examples/payload-codec/). `WithStepTokenKey` does not change the journal; already-issued unsigned tokens are rejected.

**Inspect.** Set `DURABLE_PAYLOAD_KEY` / `DURABLE_JOURNAL_MAC_KEY` (not flags). `--redact` prints `[redacted]` for INPUT/RESULT even after decrypt. Status, IDs, ERROR, and PANIC stay visible. Details: [`cmd/durable-inspect/README.md`](cmd/durable-inspect/README.md).

**Limits.** Encryption is at rest versus other local users of the machine — not versus this process or root. Step IDs, status, timestamps, `Error`, and `PanicTrace` stay plaintext. Compact copies ciphertext as-is.

Runnable walkthrough: [`examples/payload-codec/`](examples/payload-codec/).

## Use cases

Match this table to your app. If your work is one process plus a local journal, durable-go is a fit.

| Use case | Why this library |
|---|---|
| **Single-process agent** (in-process loop, agent CLI) | Memoize each LLM / tool step so a crash resumes the same run; gate tools with `ErrStepPending`; inspect progress with `WatchSteps` / `NewReadOnlyEngine`. This is how [agent-sdk-go](https://github.com/agenticenv/agent-sdk-go) uses it by default. |
| **Ops CLI** (migrate, import, backup, deploy) | Re-run the same command after a crash — completed steps skip; a second process can inspect the journal read-only while the job runs. |
| **Daemon / sidecar** on one box | On boot, `ListTasks` + `RunTask(runID)` resumes in-flight work; webhooks call `CompleteStep`; `CancelRun` stops a live run and survives a crash. |
| **Cron / batch / ETL** on one machine | Skip already-fetched extracts; fan-out parallel source/transform steps and join with `Get`, or take the first success with `select` on `Done()`. |
| **Provisioning / install scripts** | Sequence create/configure/verify as steps so a failed run does not recreate what already succeeded. |
| **Single-process service** (orders, payments, reports) | Charge, ship, notify as memoized steps; wait on manager approval or a webhook without blocking sibling work; race multiple providers with first-of-N. |

## Performance

Persistence is a local `journal.log` append plus `fsync` — no extra server. On a MacBook Pro (M2 Pro, Apple NVMe SSD) that is ~4 ms per append, well under **1%** of a typical LLM call (~1 s). Replay is a file read; `fn` does not run again.

These figures are the **default persist path**: plaintext JSON (no `PayloadCodec` / AES-GCM), unsigned `CompleteStep` tokens (no `WithStepTokenKey`), and CRC32 journal frames (no `WithJournalMACKey`).

| Operation | Latency | Memory / op | Allocations |
| :--- | :--- | :--- | :--- |
| Journal append+sync | `4.1 ms/op` | `328 B/op` | `7 allocs/op` |
| Journal replay (100 steps) | `272 µs/op` | `269 KB/op` | `919 allocs/op` |
| Completed-run recovery | `33 µs/op` | `3.6 KB/op` | `31 allocs/op` |

Same machine and ops with **AES-GCM + journal MAC** (`NewAESGCMCodec` AES-256 and `WithJournalMACKey`). Append stays fsync-bound; replay and Get pay decrypt/MAC CPU and extra allocations. `WithStepTokenKey` is not in this table (`CompleteStep` only).

| Operation | Latency | Memory / op | Allocations |
| :--- | :--- | :--- | :--- |
| Journal append+sync | `4.1 ms/op` | `1140 B/op` | `19 allocs/op` |
| Journal replay (100 steps) | `337 µs/op` | `450 KB/op` | `1919 allocs/op` |
| Completed-run recovery | `33 µs/op` | `4.9 KB/op` | `57 allocs/op` |

HDD/NFS will differ. Two ways to measure (not the same command):

1. **Your disk, with vs without the engine** — start here: `go run ./benchmarks/` — [`benchmarks/README.md`](benchmarks/README.md)
2. **Per-op `ns/op` (this table)** — `go test -run=^$ -bench=. -benchmem .` — [`journal_bench_test.go`](journal_bench_test.go)

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, workflow, and guidelines.
Project policies: [SECURITY.md](SECURITY.md) · [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

Quick commands (requires [Task](https://taskfile.dev)): `task check` | `task test` | `task lint` | `task fmt` | `task tidy` | `task test-coverage` | `task bench` | `task bench-test`

Coverage reports (PR and default branch) are on **[Codecov](https://app.codecov.io/gh/agenticenv/durable-go)**. Run `task test-coverage` locally to produce `coverage.out` and `coverage.html`.

## License

[Apache 2.0](LICENSE)

## Disclaimer

This project is provided "as is" under the Apache License 2.0. You are responsible for how you persist and handle task data, including secrets and personally identifiable information in step outputs. See [Data privacy](#data-privacy--sensitive-payloads). For security issues, follow [SECURITY.md](SECURITY.md).
