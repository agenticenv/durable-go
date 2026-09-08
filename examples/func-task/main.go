// Package main demonstrates the functional / closure style of durable-go.
// A payment task runs three steps: validate, charge, and send receipt.
// If the process crashes after "charge-stripe" succeeds, re-running with the
// same taskID and runID replays the cached result and only executes "send-receipt".
package main

import (
	"context"
	"fmt"
	"log"

	durable "github.com/agenticenv/durable-go"
)

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

var processPayment = durable.Func(func(
	ctx context.Context,
	s *durable.StepRunner,
	req PaymentRequest,
) (PaymentReceipt, error) {

	_, err := durable.RunStep(ctx, s, "validate-input", func(ctx context.Context) (struct{}, error) {
		if req.Amount <= 0 {
			return struct{}{}, fmt.Errorf("invalid amount: %.2f", req.Amount)
		}
		if req.Email == "" {
			return struct{}{}, fmt.Errorf("email is required")
		}
		log.Printf("[validate-input] order=%s amount=%.2f ✓", req.OrderID, req.Amount)
		return struct{}{}, nil
	}).Get(ctx)
	if err != nil {
		return PaymentReceipt{}, err
	}

	charge, err := durable.RunStep(ctx, s, "charge-stripe", func(ctx context.Context) (ChargeResult, error) {
		log.Printf("[charge-stripe] charging %.2f for order=%s …", req.Amount, req.OrderID)
		return ChargeResult{ChargeID: "ch_" + req.OrderID, Status: "succeeded"}, nil
	}).Get(ctx)
	if err != nil {
		return PaymentReceipt{}, err
	}

	receipt, err := durable.RunStep(ctx, s, "send-receipt", func(ctx context.Context) (PaymentReceipt, error) {
		log.Printf("[send-receipt] emailing %s for charge=%s …", req.Email, charge.ChargeID)
		return PaymentReceipt{ChargeID: charge.ChargeID, Email: req.Email, Sent: true}, nil
	}).Get(ctx)
	if err != nil {
		return PaymentReceipt{}, err
	}

	return receipt, nil
})

func main() {
	ctx := context.Background()

	e, err := durable.NewEngine(ctx, "examples/func-task/.data/payment-journal")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	if err := durable.RegisterTask(e, "process-payment", processPayment,
		durable.WithName("Process Payment – Order 42"),
		durable.WithTag("team", "payments"),
	); err != nil {
		log.Fatal(err)
	}

	run := durable.RunTask[PaymentRequest, PaymentReceipt](ctx, e, "process-payment", "payment-order-42", PaymentRequest{
		OrderID: "42",
		UserID:  "usr_abc",
		Amount:  99.99,
		Email:   "alice@example.com",
	})
	log.Printf("runID=%s", run.RunID())

	receipt, err := run.Get(ctx)
	if err != nil {
		log.Fatalf("task failed: %v", err)
	}

	log.Printf("done: chargeID=%s sent=%v", receipt.ChargeID, receipt.Sent)
}
