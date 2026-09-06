# Payment Example — Functional / Closure Style

Demonstrates `durable.Func(...)` for inline closure tasks — no struct required.
Models a payment flow: **validate → charge → send receipt**.

Re-running with the same `taskID` skips the charge step; it won't fire twice.

## Run

```bash
go run .
```

## Reset

```bash
rm examples/payment/.data/payment.db
```
