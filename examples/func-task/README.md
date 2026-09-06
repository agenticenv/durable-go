# func-task — Closure / Func Style

Demonstrates `durable.Func(...)` for inline closure tasks — no struct required.
Models a payment flow: **validate → charge → send receipt**.

Re-running with the same `taskID` skips already-completed steps; the charge step won't fire twice.

## Run

```bash
go run .
```

## Reset

```bash
rm -rf examples/func-task/.data/
```
