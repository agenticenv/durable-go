# Fan-out Example — Concurrent Steps, First-of-N, Pending Approval

Shows the async `RunStep` model: `check-inventory`, `quote-price`, and
`manager-approval` all start before anything is `Get`-ed. `manager-approval`
suspends with `ErrStepPending`; `main` completes it from a separate
goroutine, simulating a webhook. A `select` on `Done()` reacts to whichever
of `check-inventory` / `quote-price` finishes first.

## Run

```bash
go run .
```

## Expected output

```
  → [check-inventory]   checking stock for order 7 …
  → [quote-price]       pricing order 7 …
  ⏸ [manager-approval]  suspended — waiting for CompleteStep
  ✓ [quote-price]       done
  ⚑ quote-price finished first

⏸  manager-approval is pending — completing it now (simulating a webhook) …
  ✓ [check-inventory]   in stock

✅  order processed: inventory=in-stock pricing=$42.00 approval=approved-by-manager
```

Run it again and the run is already `StatusCompleted` — `RunTask` returns
the stored output without invoking `processOrder` at all, and there is
nothing to approve.

## Reset

```bash
rm -rf .data/
```
