// Package main demonstrates durable-go's async RunStep fan-out: several
// steps run concurrently, joined with Get; a first-of-N select reacts to
// whichever of two steps finishes first; one step suspends with
// ErrStepPending and is resumed by an external CompleteStep call.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	durable "github.com/agenticenv/durable-go"
)

type OrderInput struct {
	OrderID string
}

type OrderOutput struct {
	Inventory string
	Pricing   string
	Approval  string
}

// tokenCh hands the pending step's token to main so it can simulate an
// external caller (webhook, CLI, another process) delivering the result.
var tokenCh = make(chan string, 1)

var processOrder = durable.Func(func(ctx context.Context, s *durable.StepRunner, in OrderInput) (OrderOutput, error) {
	inventory := durable.RunStep(ctx, s, "check-inventory", func(ctx context.Context) (string, error) {
		log.Printf("  → [check-inventory]   checking stock for order %s …", in.OrderID)
		time.Sleep(150 * time.Millisecond)
		log.Printf("  ✓ [check-inventory]   in stock")
		return "in-stock", nil
	})
	pricing := durable.RunStep(ctx, s, "quote-price", func(ctx context.Context) (string, error) {
		log.Printf("  → [quote-price]       pricing order %s …", in.OrderID)
		time.Sleep(60 * time.Millisecond)
		log.Printf("  ✓ [quote-price]       done")
		return "$42.00", nil
	})
	approval := durable.RunStep(ctx, s, "manager-approval", func(ctx context.Context) (string, error) {
		token := s.StepToken(ctx)
		log.Printf("  ⏸ [manager-approval]  suspended — waiting for CompleteStep")
		tokenCh <- token
		return "", durable.ErrStepPending
	})

	// First-of-N: react to whichever of inventory/pricing finishes first.
	// Both are still Get-ed below regardless of which one wins the select.
	select {
	case <-inventory.Done():
		log.Println("  ⚑ check-inventory finished first")
	case <-pricing.Done():
		log.Println("  ⚑ quote-price finished first")
	}

	inv, err := inventory.Get(ctx)
	if err != nil {
		return OrderOutput{}, err
	}
	price, err := pricing.Get(ctx)
	if err != nil {
		return OrderOutput{}, err
	}
	appr, err := approval.Get(ctx)
	if err != nil {
		return OrderOutput{}, err
	}
	return OrderOutput{Inventory: inv, Pricing: price, Approval: appr}, nil
})

func main() {
	ctx := context.Background()

	e, err := durable.NewEngine(ctx, "examples/fanout/.data/fanout-journal")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	if err := durable.RegisterTask(e, "process-order", processOrder,
		durable.WithName("Fan-out Order 7"),
	); err != nil {
		log.Fatal(err)
	}

	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  check-inventory, quote-price, manager-approval")
	fmt.Println("  all start concurrently — no Get until all three")
	fmt.Println("  are in flight.")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	run := durable.RunTask[OrderInput, OrderOutput](ctx, e, "process-order", "order-7", OrderInput{OrderID: "7"})

	// Get runs on its own goroutine so main can race it against tokenCh:
	// on a first run manager-approval suspends and needs completing; on a
	// resumed, already-completed run (e.g. running this example a second
	// time against the same .data dir) it never will, and that is fine.
	done := make(chan struct{})
	var out OrderOutput
	var runErr error
	go func() {
		out, runErr = run.Get(ctx)
		close(done)
	}()

	select {
	case token := <-tokenCh:
		fmt.Println("\n⏸  manager-approval is pending — completing it now (simulating a webhook) …")
		if err := durable.CompleteStep(ctx, e, token, "approved-by-manager"); err != nil {
			log.Fatal(err)
		}
	case <-done:
		fmt.Println("\n(run already completed from a previous execution — nothing to approve)")
	}

	<-done
	if runErr != nil {
		log.Fatalf("task failed: %v", runErr)
	}

	fmt.Printf("\n✅  order processed: inventory=%s pricing=%s approval=%s\n", out.Inventory, out.Pricing, out.Approval)
}
