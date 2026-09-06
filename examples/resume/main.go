// Package main demonstrates durable-go's core value: crash recovery and step replay.
//
// # How to run this demo
//
//  1. First run  – crashes intentionally after step 2:
//     CRASH_AFTER=2 go run .
//
//  2. Second run – resumes from step 3 (steps 1 & 2 are replayed from cache):
//     go run .
//
// Watch the logs: on the second run you will see "↩ replayed from cache" for the
// first two steps, proving they were never re-executed.
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	durable "github.com/agenticenv/durable-go"
	"github.com/agenticenv/durable-go/store/sqlite"
)

// crashAfter reads the CRASH_AFTER env var. When set to "2", the process
// exits hard after step 2 to simulate a mid-run crash.
var crashAfter = os.Getenv("CRASH_AFTER")

// --- domain types ---

type ReportInput struct {
	ReportID string
}

type ReportOutput struct {
	ReportID  string
	Delivered bool
}

// --- task ---

var generateReport = durable.Func(func(
	ctx context.Context,
	s *durable.StepRunner,
	in ReportInput,
) (ReportOutput, error) {

	// Step 1 – fetch raw data from the database.
	_, err := durable.Step(ctx, s, "fetch-data", func(ctx context.Context) (struct{}, error) {
		log.Printf("  → [fetch-data]   querying database for report %s …", in.ReportID)
		time.Sleep(200 * time.Millisecond) // simulate DB query
		log.Printf("  ✓ [fetch-data]   done")
		return struct{}{}, nil
	})
	if err != nil {
		return ReportOutput{}, err
	}

	// Step 2 – run a heavy computation / ML inference.
	_, err = durable.Step(ctx, s, "run-computation", func(ctx context.Context) (struct{}, error) {
		log.Printf("  → [run-computation]  running expensive model inference …")
		time.Sleep(300 * time.Millisecond) // simulate expensive work
		log.Printf("  ✓ [run-computation]  done")

		// ── Simulate a crash immediately after this step succeeds ──
		// In reality this could be: OOM kill, power loss, os.Exit from a signal handler, etc.
		if crashAfter == "2" {
			log.Println()
			log.Println("💥  SIMULATED CRASH after step 2 (CRASH_AFTER=2)")
			log.Println("    Re-run without CRASH_AFTER to resume from step 3.")
			log.Println()
			os.Exit(1)
		}

		return struct{}{}, nil
	})
	if err != nil {
		return ReportOutput{}, err
	}

	// Step 3 – deliver the report via email / webhook.
	out, err := durable.Step(ctx, s, "deliver-report", func(ctx context.Context) (ReportOutput, error) {
		log.Printf("  → [deliver-report]  sending report %s …", in.ReportID)
		time.Sleep(100 * time.Millisecond) // simulate HTTP call
		log.Printf("  ✓ [deliver-report]  sent")
		return ReportOutput{ReportID: in.ReportID, Delivered: true}, nil
	})
	if err != nil {
		return ReportOutput{}, err
	}

	return out, nil
})

func main() {
	ctx := context.Background()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	if err := os.MkdirAll("examples/resume/.data", 0o755); err != nil {
		log.Fatal(err)
	}
	store, err := sqlite.NewSQLiteStore("examples/resume/.data/resume-demo.db", sqlite.WithLogger(logger))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	client, err := durable.NewClient(ctx, store, durable.WithLogger(logger))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	// Same taskID on every run — this is what enables replay.
	// Changing it starts a fresh execution with no cached steps.
	const taskID = "report-weekly-2026-09"

	handle := client.NewTask(taskID, durable.WithName("Weekly Report – Sep 2026"))

	if crashAfter == "2" {
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("  RUN 1 — will crash after step 2")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	} else {
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("  RUN 2 — resuming; steps 1 & 2 replayed from cache")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	}

	out, err := durable.Run(ctx, handle, ReportInput{ReportID: taskID}, generateReport)
	if err != nil {
		log.Fatalf("task failed: %v", err)
	}

	fmt.Printf("\n✅  Report delivered: %v\n", out.Delivered)
}
