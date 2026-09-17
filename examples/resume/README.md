# Resume Example — Crash Recovery

One `go run` proves crash recovery: persist two steps, exit without `Close`, reopen the same journal. Completed steps are not re-executed.

## Run

From the repo root, or from this directory:

```bash
go run ./examples/resume/
# or: go run .
```

## Expected output

```
── RUN 1 — persist steps 1–2, then crash ──
  → [fetch-data] executed
  ✓ [fetch-data] persisted
  → [run-computation] executed
  ✓ [run-computation] persisted
💥  simulated crash (COMPLETED is on disk; step 3 never ran)

── RUN 2 — new Engine, same dir ──
  · [fetch-data] replayed (fn not called)
  · [run-computation] replayed (fn not called)
  → [deliver-report] executed
  ✓ [deliver-report] persisted

✅  delivered=true
```

The crash happens **after** `Get` returns, so steps 1–2 are already in the journal. A crash *inside* a step function re-runs that step (at-least-once), same as a Temporal activity.
