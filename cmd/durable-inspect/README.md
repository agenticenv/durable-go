# durable-inspect

Read-only CLI for a durable-go journal. Lists tasks and steps; it does not start, cancel, or complete runs.

Opens the journal with `NewReadOnlyEngine`. `--dir` / `-d` wins over `DURABLE_DIR`. Put `DURABLE_PAYLOAD_KEY` and `DURABLE_JOURNAL_MAC_KEY` in the environment (flags put secrets on the command line). Hex is tried first for both keys; otherwise the string is raw bytes. `--redact` hides INPUT and RESULT. One `-d` and one key pair per command — plaintext and AES (or CRC and HMAC) journals belong in separate directories (second `NewEngine` on a new `dataDir`; do not mix in one tree). If a writer still holds the exclusive lock, the command fails — stop that process or inspect a copy of the directory.

Payloads, file modes, and tokens: [README — Data privacy](../../README.md#data-privacy--sensitive-payloads).

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

go run ./examples/payload-codec/
export DURABLE_PAYLOAD_KEY="$KEY"
export DURABLE_JOURNAL_MAC_KEY="$MAC"
./bin/durable-inspect -d examples/payload-codec/.data/aes --redact task get echo run-1
./bin/durable-inspect -d examples/payload-codec/.data/journal-mac step get echo run-1 echo
```

## Decrypt and redact

**Prefer env vars.** `--payload-key` and `--journal-mac-key` appear in `ps`. Use `DURABLE_PAYLOAD_KEY` and `DURABLE_JOURNAL_MAC_KEY`. Flags override env when you pass them.

`DURABLE_PAYLOAD_KEY` supplies the AES-GCM key so `task get` / `step get` / `step list` can decode encrypted INPUT and RESULT. Hex (32/48/64 chars) is tried first; otherwise the string must be 16, 24, or 32 raw bytes. Omit it to print stored bytes (quoted ciphertext if the writer used `NewAESGCMCodec`). A wrong key fails closed.

`DURABLE_JOURNAL_MAC_KEY` is required to read a journal written with `WithJournalMACKey`. Hex is tried first; otherwise the string is raw key bytes.

`--redact` is display-only — it does not change the journal or replace `PayloadCodec`. It replaces non-empty task/step **INPUT** and **RESULT** with `[redacted]`, including after a successful decrypt. Status, IDs, timestamps, ERROR, and PANIC are still shown.

| Source | When it is used |
|---|---|
| `--payload-key` | If passed — always wins |
| `DURABLE_PAYLOAD_KEY` | If `--payload-key` is omitted |
| neither | Stored bytes (plaintext JSON, or ciphertext) |
| `--journal-mac-key` | If passed — always wins |
| `DURABLE_JOURNAL_MAC_KEY` | If `--journal-mac-key` is omitted |
| neither (MAC journal) | HMAC frames look empty / not found |

```bash
export DURABLE_PAYLOAD_KEY="$KEY"
export DURABLE_JOURNAL_MAC_KEY="$MAC"
./bin/durable-inspect -d ./data task get echo run-1
./bin/durable-inspect -d ./data --redact step get echo run-1 say
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

One run: metadata, **task input** (`input.json` — one JSON value for `I`; `{}` when the task used `struct{}`), then its steps. Step input and version are stored on each step record when set.

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

Steps for one run (`STEP_ID`, `STATUS`, `VERSION`, `INPUT`, `STARTED`, `COMPLETED`).

```bash
./bin/durable-inspect -d ./data step list echo run-1
```

### `step get`

One step, including `VERSION` / `INPUT` / `RESULT` / `ERROR` when present.

```bash
./bin/durable-inspect -d ./data step get echo run-1 say
```

## Typical flow

1. Set `DURABLE_DIR` or pass `-d` at the journal root.
2. `task list` to find `TASK_ID` / `RUN_ID`.
3. `task get <taskID> <runID>` for status and every step. Set `DURABLE_PAYLOAD_KEY` if the writer used AES-GCM; set `DURABLE_JOURNAL_MAC_KEY` if it used `WithJournalMACKey`; add `--redact` before pasting output.
4. `step get …` if you need a step result.

## Lock / “journal is locked by a writer”

A live `NewEngine` on the same directory takes an exclusive flock. Inspect cannot open that dir until the writer `Close`s (process exit after `defer e.Close()`, or a copy of the folder). Multiple inspect processes can share a journal when no writer is present.

## Help

```bash
./bin/durable-inspect -h
./bin/durable-inspect --help
```
