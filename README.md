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
- **Memoized steps** — completed steps replay from the journal; they are not run again.
- **Pending steps** — return `ErrStepPending` and complete later via `CompleteStep` (human approval, webhooks).
- **Fire-and-forget runs** — `RunTask` returns immediately; `TaskRun.Get` waits; `RunID()` is available at once.
- **Timeouts and retries** — engine / task / run / step options. Retries default to 0 (opt-in).
- **Panic recovery** — task and step panics are recorded and returned as errors.
- **Auto-purge** — optional background cleanup of old completed and failed runs.
- **Flexible execution** — tasks as `durable.Func` closures or structs with `Exec`.

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

A step suspends itself by returning `ErrStepPending`. An external caller completes it with the token from `StepToken()`:

```go
approval, err := durable.RunStep(ctx, s, "approve", func(ctx context.Context) (Approval, error) {
    token := s.StepToken()
    sendEmail("manager@co.com", token)
    return Approval{}, durable.ErrStepPending
}).Get(ctx)

// webhook / CLI / another goroutine:
durable.CompleteStep(ctx, e, token, Approval{By: "manager@co.com"})
```

## Resume

Register tasks after every `NewEngine`, then resume active runs. Pass the saved runID (or `""` to resume the oldest Running/Waiting run for that taskID). Completed steps replay from the journal.

```go
durable.RegisterTask(e, "process-order", ...)
pending, _ := e.ListTasks(ctx, durable.StatusRunning, durable.StatusWaiting)
for _, t := range pending {
    run := durable.RunTask[OrderInput, OrderOutput](ctx, e, t.TaskID, t.RunID, reloadInput(t))
    go func() { _, _ = run.Get(ctx) }()
}
```

Task inputs are not persisted. Pass the same input when resuming.

Full example: [`examples/resume/`](examples/resume/).

## Writing tasks

Follow these when you write a task. On resume, the task runs again from the top; completed steps are reused, not re-executed.

1. **Side effects in `RunStep`.** Do not call an API, write to a database, or publish to a queue in the task body. Wrap that work in `durable.RunStep`.
2. **Non-deterministic values in `RunStep`.** Do not use `time.Now()`, UUIDs, or random values in the task body to choose a step ID or a branch. Generate them inside a `RunStep` so resume sees the same result.
3. **Idempotent steps.** A crash can re-run a step after the side effect already happened. Charging a card or sending mail must be safe to do twice (or no-op).

Also:

- **Unique step IDs** — one stable string per step (literals or `fmt.Sprintf("step-%d", i)`). Reusing an ID panics.
- **Sequential `RunStep` calls** — concurrent calls on the same `StepRunner` panic. Fan out work, then persist results sequentially.
- **JSON results** — step and task outputs must be JSON-marshalable.
- **Same runID to resume** — inputs are not persisted; pass the same input to `RunTask` when resuming.

## Examples

Runnable examples in [examples/](examples/) — see [examples/README.md](examples/README.md) for setup and run instructions.

| Example | What it shows |
|---------|----------------|
| [`examples/resume/`](examples/resume/) | Crash after step 2, resume from cache |
| [`examples/func-task/`](examples/func-task/) | Closure-style `durable.Func` |
| [`examples/struct-task/`](examples/struct-task/) | Struct task with injected deps, retries, timeout |

```bash
# from repo root
go run ./examples/resume/
go run ./examples/func-task/
go run ./examples/struct-task/
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
