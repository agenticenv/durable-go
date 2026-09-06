# durable-go

[![CI](https://github.com/agenticenv/durable-go/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/agenticenv/durable-go/actions/workflows/ci.yml)
[![Security](https://github.com/agenticenv/durable-go/actions/workflows/security.yml/badge.svg?branch=main)](https://github.com/agenticenv/durable-go/actions/workflows/security.yml)
[![Release](https://img.shields.io/github/v/release/agenticenv/durable-go?label=Release)](https://github.com/agenticenv/durable-go/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/agenticenv/durable-go.svg)](https://pkg.go.dev/github.com/agenticenv/durable-go)
[![License](https://img.shields.io/github/license/agenticenv/durable-go?label=License)](LICENSE)

**Durable task execution for Go with memoized steps and pluggable persistence.**

**durable-go** is a Go library for durable task execution. Define typed tasks, run memoized steps, and persist progress so work can resume safely after failures or restarts. Useful for any Go app that needs reliable, resumable workflows without a heavy orchestration framework.

> Releases follow [Semantic Versioning](https://semver.org/); see the [latest release](https://github.com/agenticenv/durable-go/releases/latest).

## Features

- **Typed tasks** — generic `Run` / `Task` with input and output types
- **Memoized steps** — completed steps replay from the store; they are not run again
- **In-process** — no cluster or workflow server; one process, one store
- **Pluggable persistence** — `Store` interface; SQLite driver included
- **Timeouts and retries** — `WithTimeout`, `WithMaxRetries` on the task handle
- **Panic recovery** — task and step panics are recorded and returned as errors
- **Auto-purge** — optional background cleanup of old completed and failed records
- **Closure or struct** — `durable.Func` for inline tasks, or a type with `Exec`

## Install

```bash
go get github.com/agenticenv/durable-go@latest
```

Go 1.26.5+. No infrastructure required. The included SQLite driver writes a local file.

## Quick Start

```go
import (
    "context"

    durable "github.com/agenticenv/durable-go"
    "github.com/agenticenv/durable-go/store/sqlite"
)

// errors omitted for brevity
store, _ := sqlite.NewSQLiteStore("app.db")
defer store.Close()

client, _ := durable.NewClient(context.Background(), store)
defer client.Close()

handle := client.NewTask("job-42", durable.WithName("Example job"))

out, _ := durable.Run(context.Background(), handle, "hello", durable.Func(
    func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
        greet, err := durable.Step(ctx, s, "greet", func(ctx context.Context) (string, error) {
            return in + " world", nil
        })
        if err != nil {
            return "", err
        }
        return durable.Step(ctx, s, "upper", func(ctx context.Context) (string, error) {
            return greet, nil
        })
    },
))
_ = out
```

Re-running the same task identity replays completed steps from the store.

## Examples

Runnable examples in [examples/](examples/) — see [examples/README.md](examples/README.md) for setup and run instructions.

| Example | What it shows |
|---------|----------------|
| [`examples/resume/`](examples/resume/) | Crash after step 2, resume from cache |
| [`examples/payment/`](examples/payment/) | Closure-style `durable.Func` |
| [`examples/agent/`](examples/agent/) | Struct task with injected deps |

```bash
# from repo root
go run ./examples/resume/
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
