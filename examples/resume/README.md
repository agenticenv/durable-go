# Resume Example — Crash Recovery

The core durable-go demo. Shows that completed steps are **never re-executed** after a crash.

## Run

```bash
# Run 1 — crashes intentionally after step 2
CRASH_AFTER=2 go run .

# Run 2 — resumes; steps 1 & 2 replayed from cache, only step 3 executes
go run .
```

## Expected output

**Run 1**
```
  → [fetch-data]       querying database …  ✓
  → [run-computation]  running model …      ✓
💥  SIMULATED CRASH after step 2
```

**Run 2**
```
  → [deliver-report]   sending report …     ✓
✅  Report delivered: true
```

On run 2, `fetch-data` and `run-computation` return from the journal without calling the step functions.

## Reset

```bash
rm -rf examples/resume/.data/
```
