# Durability benchmark

This directory is a runner (`go run ./benchmarks/`). It measures what durable-go adds on top of calling step functions directly: journal persist on first run, step replay from the journal, and the filesystem operations underneath (append+sync, atomic write, journal read).

The step body is a no-op that returns a byte blob. Work is the journal, not your business logic. Run it on the disk you will actually use — fsync cost is not portable.

## Run

From the **repo root**:

```bash
go run ./benchmarks/ --
go run ./benchmarks/ -- -steps 20 -payload 1kb
go run ./benchmarks/ -- -steps 100 -payload 64kb -iters 10
go run ./benchmarks/ -- -dir /path/to/disk
```

`go run` needs `--` before flags so they are not eaten by `go run`.

Or: `task bench` / `task bench -- -steps 100`.

## Flags

| Flag | Default | What it changes |
|---|---|---|
| `-steps` | `20` | `RunStep` count in the synthetic task |
| `-payload` | `1kb` | Size of each step **output** (`512`, `1kb`, `64kb`, `1mb`) |
| `-iters` | `5` | Timed iterations per mode (report p50 / p95 / avg) |
| `-warmup` | `1` | Untimed iterations discarded first |
| `-dir` | temp dir | Journal disk to measure (use this for a real SSD / NFS) |

## What it measures

Same N-step loop, three ways:

1. **no engine** — call `fn` N times. Baseline.
2. **first durable** — `NewEngine` + `RunTask` with those N steps. Persist cost (STARTED + COMPLETED journal append+sync per step, plus `input.json` / `meta.json` / `output.json`).
3. **step replay** — resume the same run so completed steps return from the journal; `fn` does not run. Fails the process if a step function is invoked.

Filesystem probe (same disk, isolated — not engine hooks):

- journal **append+sync** (2× steps, matching STARTED + COMPLETED)
- **atomic write** (tmp+sync+rename, same pattern as meta/input/output)
- **journal read** of the `journal.log` produced by the last first-run

Bump `-steps` and `-payload` to see how latency grows.
