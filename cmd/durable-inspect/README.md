# durable-inspect

Read-only CLI for a durable-go journal. Lists tasks and steps; it does not start, cancel, or complete runs.

Opens the journal with `NewReadOnlyEngine`. `--dir` / `-d` wins over `DURABLE_DIR`. If a writer still holds the exclusive lock, the command fails — stop that process or inspect a copy of the directory.

## Install

```bash
go install github.com/agenticenv/durable-go/cmd/durable-inspect@latest
durable-inspect -h
```

Puts the binary on `$(go env GOPATH)/bin` (keep that on your `PATH`).

From a **checkout** (repo root):

```bash
go build -o bin/durable-inspect ./cmd/durable-inspect
# or: task build

./bin/durable-inspect -h
go run ./cmd/durable-inspect -- -h
```

`go run` needs `--` before flags so they are not eaten by `go run`.

## Point at a journal

The path is the engine `dataDir` (the folder that contains `.lock` and `tasks/`), not the repo root.

| Source | When it is used |
|---|---|
| `--dir` / `-d` | If passed — always wins |
| `DURABLE_DIR` | If `--dir` / `-d` is omitted |
| neither | Error: directory required |

```bash
export DURABLE_DIR=./data
./bin/durable-inspect task list

./bin/durable-inspect -d ./data task list
./bin/durable-inspect --dir=./data task list
```

Example journals after `go run ./examples/<name>/` live under `examples/<name>/.data/<journal>/` (gitignored).

```bash
go run ./examples/yaml-task/
./bin/durable-inspect -d examples/yaml-task/.data/yaml-journal task list
```

## Commands

```text
durable-inspect [flags] task list [--status STATUS]
durable-inspect [flags] task get <taskID|runID|name> [runID]
durable-inspect [flags] step list <taskID> <runID>
durable-inspect [flags] step get <taskID> <runID> <stepID>
```

Flags can appear anywhere (`-d` after `list` is fine). `--status` is valid only on `task list`.

### `task list`

All runs in the journal, newest first.

```bash
./bin/durable-inspect -d ./data task list
./bin/durable-inspect -d ./data task list --status running
```

`--status` is one of: `running`, `waiting`, `completed`, `failed`.

Columns: `TASK_ID`, `RUN_ID`, `NAME`, `STATUS`, `CREATED`.

`NAME` is the optional `WithName` label, not the task ID.

### `task get`

One run: metadata, **task input** (`input.json`), then its steps. Step inputs are not stored (v1); only step results.

```bash
# task ID — if several runs share it, the table is printed and the command exits 1
./bin/durable-inspect -d ./data task get echo

# run ID
./bin/durable-inspect -d ./data task get run-1

# WithName label
./bin/durable-inspect -d ./data task get "Echo Task"

# exact run
./bin/durable-inspect -d ./data task get echo run-1
```

With one argument, the value is matched against **task ID**, **run ID**, or **name**. Two arguments are always `taskID` then `runID`.

### `step list`

Steps for one run (`STEP_ID`, `STATUS`, `STARTED`, `COMPLETED`).

```bash
./bin/durable-inspect -d ./data step list echo run-1
```

### `step get`

One step, including `RESULT` / `ERROR` when present.

```bash
./bin/durable-inspect -d ./data step get echo run-1 say
```

## Typical flow

1. Set `DURABLE_DIR` or pass `-d` at the journal root.
2. `task list` to find `TASK_ID` / `RUN_ID`.
3. `task get <taskID> <runID>` for status and every step.
4. `step get …` if you need a step result.

## Lock / “journal is locked by a writer”

A live `NewEngine` on the same directory takes an exclusive flock. Inspect cannot open that dir until the writer `Close`s (process exit after `defer e.Close()`, or a copy of the folder). Multiple inspect processes can share a journal when no writer is present.

## Help

```bash
./bin/durable-inspect -h
./bin/durable-inspect --help
```
