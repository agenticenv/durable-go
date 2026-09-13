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

> **Pre-1.0.** This is the first fully worked-through release of the step model (see [Resume](#resume) and [Writing tasks](#writing-tasks)); the on-disk journal format changed in this release and is not compatible with runs written by `v0.1.0`–`v0.1.2`. Upgrade only between clean runs.

## Why durable-go

Most durable-execution frameworks require external infrastructure—such as a dedicated workflow server or a Postgres database—and enforce strict code execution models like replay determinism.

`durable-go` takes a zero-infra, in-process approach: a single Go library with a filesystem journal running inside your application process. Instead of replaying entire function call graphs from an external orchestrator, `durable-go` memoizes individual step results. On resume the task runs again from the top; completed steps return the cached result. There is no replay-determinism sandbox.

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
        charged, err := durable.RunStep(ctx, s, "charge", func(ctx context.Context) (string, error) {
            return chargeCard(in)
        }).Get(ctx)
        if err != nil {
            return OrderOutput{}, err
        }
        shipped, err := durable.RunStep(ctx, s, "ship", func(ctx context.Context) (string, error) {
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
    return durable.RunStep(ctx, s, "notify", func(ctx context.Context) (string, error) {
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
approval, err := durable.RunStep(ctx, s, "approve", func(ctx context.Context) (Approval, error) {
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
charge := durable.RunStep(ctx, s, "charge", func(ctx context.Context) (string, error) {
    return chargeCard(order)
})
notify := durable.RunStep(ctx, s, "notify", func(ctx context.Context) (string, error) {
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
a := durable.RunStep(ctx, s, "provider-a", callProviderA)
b := durable.RunStep(ctx, s, "provider-b", callProviderB)
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

## Resume

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
- **Step IDs are the resume key — never rename one.** The journal matches records by stepID string only. Renaming a step between deploys orphans the old result: on the next run `fn` executes again under the new name as if it had never run. Treat a stepID like a database column name, not a display label.
- **Concurrent `RunStep` calls are safe.** Start several steps before `Get`-ing any of them to fan out; join with `Get` or `select` on `Done()`. A duplicate stepID within one run still panics.
- **One stepID is reserved.** `RunStep` panics if `stepID` is `"\x00cancel"` — it is reserved internally for `CancelRun`'s durable signal. Any human-readable stepID you would actually choose is unaffected.
- **JSON results** — step and task outputs must be JSON-marshalable.
- **Same runID to resume** — `input.json` is reloaded; you do not need to pass the original input again.

## Examples

Runnable examples in [examples/](examples/) — see [examples/README.md](examples/README.md) for setup and run instructions.

| Example | What it shows |
|---------|----------------|
| [`examples/resume/`](examples/resume/) | Crash after step 2, resume from cache |
| [`examples/func-task/`](examples/func-task/) | Closure-style `durable.Func` |
| [`examples/struct-task/`](examples/struct-task/) | Struct task with injected deps, retries, timeout |
| [`examples/fanout/`](examples/fanout/) | Concurrent `RunStep`, `Get`-all join, `ErrStepPending` |

```bash
# from repo root
go run ./examples/resume/
go run ./examples/func-task/
go run ./examples/struct-task/
go run ./examples/fanout/
```

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, workflow, and guidelines.
Project policies: [SECURITY.md](SECURITY.md) · [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

Quick commands (requires [Task](https://taskfile.dev)): `task check` | `task test` | `task lint` | `task fmt` | `task tidy` | `task test-coverage`

Coverage reports (PR and default branch) are on **[Codecov](https://app.codecov.io/gh/agenticenv/durable-go)**. Run `task test-coverage` locally to produce `coverage.out` and `coverage.html`.

## License

[Apache 2.0](LICENSE)

## Disclaimer

This project is provided "as is" under the Apache License 2.0. You are responsible for how you persist and handle task data, including secrets and personally identifiable information in step outputs. For security issues, follow [SECURITY.md](SECURITY.md).
