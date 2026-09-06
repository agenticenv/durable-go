// Package main demonstrates the functional / closure style of durable-go.
// A payment task runs three steps: validate, charge, and send receipt.
// If the process crashes after "charge-stripe" succeeds, re-running with the
// same taskID replays the cached result and only executes "send-receipt".
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	durable "github.com/agenticenv/durable-go"
	"github.com/agenticenv/durable-go/store/sqlite"
)

// --- domain types ---

type PaymentRequest struct {
	OrderID string
	UserID  string
	Amount  float64
	Email   string
}

type ChargeResult struct {
	ChargeID string
	Status   string
}

type PaymentReceipt struct {
	ChargeID string
	Email    string
	Sent     bool
}

// --- task definition (inline closure, no struct) ---

var processPayment = durable.Func(func(
	ctx context.Context,
	s *durable.StepRunner,
	req PaymentRequest,
) (PaymentReceipt, error) {

	// Step 1 – validate input.
	// Returns nothing meaningful; panics or errors abort the task.
	_, err := durable.Step(ctx, s, "validate-input", func(ctx context.Context) (struct{}, error) {
		if req.Amount <= 0 {
			return struct{}{}, fmt.Errorf("invalid amount: %.2f", req.Amount)
		}
		if req.Email == "" {
			return struct{}{}, fmt.Errorf("email is required")
		}
		log.Printf("[validate-input] order=%s amount=%.2f ✓", req.OrderID, req.Amount)
		return struct{}{}, nil
	})
	if err != nil {
		return PaymentReceipt{}, err
	}

	// Step 2 – charge via Stripe (simulated).
	// On replay this result is returned from cache; Stripe is NOT called again.
	charge, err := durable.Step(ctx, s, "charge-stripe", func(ctx context.Context) (ChargeResult, error) {
		log.Printf("[charge-stripe] charging %.2f for order=%s …", req.Amount, req.OrderID)
		// simulate Stripe call
		return ChargeResult{ChargeID: "ch_" + req.OrderID, Status: "succeeded"}, nil
	})
	if err != nil {
		return PaymentReceipt{}, err
	}

	// Step 3 – send receipt email.
	receipt, err := durable.Step(ctx, s, "send-receipt", func(ctx context.Context) (PaymentReceipt, error) {
		log.Printf("[send-receipt] emailing %s for charge=%s …", req.Email, charge.ChargeID)
		// simulate email send
		return PaymentReceipt{ChargeID: charge.ChargeID, Email: req.Email, Sent: true}, nil
	})
	if err != nil {
		return PaymentReceipt{}, err
	}

	return receipt, nil
})

func main() {
	ctx := context.Background()

	if err := os.MkdirAll("examples/payment/.data", 0o755); err != nil {
		log.Fatal(err)
	}
	store, err := sqlite.NewSQLiteStore("examples/payment/.data/payment.db")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	client, err := durable.NewClient(ctx, store)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	// taskID is stable per order; re-running with the same ID replays completed steps.
	handle := client.NewTask(
		"payment-order-42",
		durable.WithName("Process Payment – Order 42"),
		durable.WithTag("team", "payments"),
	)

	receipt, err := durable.Run(ctx, handle, PaymentRequest{
		OrderID: "42",
		UserID:  "usr_abc",
		Amount:  99.99,
		Email:   "alice@example.com",
	}, processPayment)
	if err != nil {
		log.Fatalf("task failed: %v", err)
	}

	log.Printf("done: chargeID=%s sent=%v", receipt.ChargeID, receipt.Sent)
}
