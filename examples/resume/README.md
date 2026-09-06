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
  ↩ [fetch-data]       replayed from cache
  ↩ [run-computation]  replayed from cache
  → [deliver-report]   sending report …     ✓
✅  Report delivered: true
```

## Reset

```bash
rm examples/resume/.data/resume-demo.db
```
