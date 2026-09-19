# yaml-task — YAML workflow as one task

One YAML file is one durable-go task. Each `steps[].id` is a `RunStep`.
No if/else, loops, or parallel DSL — just a list of commands.

Re-running with the same `taskID` and `runID` skips already-completed steps.

## Run

From this directory:

```bash
go run .
```

Crash after step 2, then resume:

```bash
CRASH_AFTER=2 go run .
go run .
```

On the second run, `clone` and `build` replay from the journal; only `test` executes.

## Reset

```bash
rm -rf .data/
```
