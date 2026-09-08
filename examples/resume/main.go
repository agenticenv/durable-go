// Package main demonstrates durable-go's core value: crash recovery and step replay.
//
// # How to run this demo
//
//  1. First run  – crashes intentionally after step 2:
//     CRASH_AFTER=2 go run .
//
//  2. Second run – resumes from step 3 (steps 1 & 2 are replayed from cache):
//     go run .
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	durable "github.com/agenticenv/durable-go"
)

var crashAfter = os.Getenv("CRASH_AFTER")

type ReportInput struct {
	ReportID string
}

type ReportOutput struct {
	ReportID  string
	Delivered bool
}

var generateReport = durable.Func(func(
	ctx context.Context,
	s *durable.StepRunner,
	in ReportInput,
) (ReportOutput, error) {

	_, err := durable.RunStep(ctx, s, "fetch-data", func(ctx context.Context) (struct{}, error) {
		log.Printf("  → [fetch-data]   querying database for report %s …", in.ReportID)
		time.Sleep(200 * time.Millisecond)
		log.Printf("  ✓ [fetch-data]   done")
		return struct{}{}, nil
	}).Get(ctx)
	if err != nil {
		return ReportOutput{}, err
	}

	_, err = durable.RunStep(ctx, s, "run-computation", func(ctx context.Context) (struct{}, error) {
		log.Printf("  → [run-computation]  running expensive model inference …")
		time.Sleep(300 * time.Millisecond)
		log.Printf("  ✓ [run-computation]  done")

		if crashAfter == "2" {
			log.Println()
			log.Println("💥  SIMULATED CRASH after step 2 (CRASH_AFTER=2)")
			log.Println("    Re-run without CRASH_AFTER to resume from step 3.")
			log.Println()
			os.Exit(1)
		}

		return struct{}{}, nil
	}).Get(ctx)
	if err != nil {
		return ReportOutput{}, err
	}

	out, err := durable.RunStep(ctx, s, "deliver-report", func(ctx context.Context) (ReportOutput, error) {
		log.Printf("  → [deliver-report]  sending report %s …", in.ReportID)
		time.Sleep(100 * time.Millisecond)
		log.Printf("  ✓ [deliver-report]  sent")
		return ReportOutput{ReportID: in.ReportID, Delivered: true}, nil
	}).Get(ctx)
	if err != nil {
		return ReportOutput{}, err
	}

	return out, nil
})

func main() {
	ctx := context.Background()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	e, err := durable.NewEngine(ctx, "examples/resume/.data/resume-journal", durable.WithLogger(logger))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	if err := durable.RegisterTask(e, "weekly-report", generateReport,
		durable.WithName("Weekly Report – Sep 2026"),
	); err != nil {
		log.Fatal(err)
	}

	const runID = "report-weekly-2026-09"

	if crashAfter == "2" {
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("  RUN 1 — will crash after step 2")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	} else {
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("  RUN 2 — resuming; steps 1 & 2 replayed from cache")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	}

	run := durable.RunTask[ReportInput, ReportOutput](ctx, e, "weekly-report", runID, ReportInput{ReportID: runID})
	out, err := run.Get(ctx)
	if err != nil {
		log.Fatalf("task failed: %v", err)
	}

	fmt.Printf("\n✅  Report delivered: %v\n", out.Delivered)
}
