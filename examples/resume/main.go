// Package main is a self-contained crash/resume demo.
//
//	go run .
//
// One process starts a run, persists steps 1–2, then exits without Close
// (simulated crash). A second Engine opens the same dataDir and resumes:
// steps 1–2 replay from the journal; only step 3 executes.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync/atomic"

	durable "github.com/agenticenv/durable-go"
)

const (
	taskID = "weekly-report"
	runID  = "report-weekly-2026-09"
)

type ReportInput struct {
	ReportID string
}

type ReportOutput struct {
	ReportID  string
	Delivered bool
}

func main() {
	log.SetFlags(0)

	crash := flag.Bool("crash", false, "internal: persist two steps then exit 1")
	flag.Parse()

	if *crash {
		dir := os.Getenv("DURABLE_DIR")
		if dir == "" {
			log.Fatal("DURABLE_DIR is required with -crash")
		}
		runPhase(dir, true)
		log.Fatal("crash phase returned; expected os.Exit(1)")
	}

	dir, err := os.MkdirTemp("", "durable-resume-*")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	self, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("durable-go crash/resume")
	fmt.Printf("journal: %s\n\n", dir)

	fmt.Println("── RUN 1 — persist steps 1–2, then crash ──")
	cmd := exec.Command(self, "-crash")
	cmd.Env = append(os.Environ(), "DURABLE_DIR="+dir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 1 {
		log.Fatalf("crash phase: want exit 1, got %v", err)
	}

	fmt.Println()
	fmt.Println("── RUN 2 — new Engine, same dir ──")
	runPhase(dir, false)
}

func runPhase(dir string, crashAfter2 bool) {
	ctx := context.Background()
	e, err := durable.NewEngine(ctx, dir)
	if err != nil {
		log.Fatal(err)
	}
	if !crashAfter2 {
		defer func() { _ = e.Close() }()
	}

	if err := durable.RegisterTask(e, taskID, reportTask(crashAfter2),
		durable.WithName("Weekly Report"),
	); err != nil {
		log.Fatal(err)
	}

	run := durable.RunTask[ReportInput, ReportOutput](ctx, e, taskID, runID, ReportInput{ReportID: runID})
	out, err := run.Get(ctx)
	if err != nil {
		log.Fatalf("task failed: %v", err)
	}
	fmt.Printf("\n✅  delivered=%v\n", out.Delivered)
}

func reportTask(crashAfter2 bool) durable.TaskFunc[ReportInput, ReportOutput] {
	return durable.Func(func(ctx context.Context, s *durable.StepRunner, in ReportInput) (ReportOutput, error) {
		if err := step(ctx, s, "fetch-data", in, func(context.Context, ReportInput) (struct{}, error) {
			return struct{}{}, nil
		}); err != nil {
			return ReportOutput{}, err
		}

		if err := step(ctx, s, "run-computation", struct{}{}, func(context.Context, struct{}) (struct{}, error) {
			return struct{}{}, nil
		}); err != nil {
			return ReportOutput{}, err
		}

		if crashAfter2 {
			log.Println("💥  simulated crash (COMPLETED is on disk; step 3 never ran)")
			os.Exit(1)
		}

		var out ReportOutput
		if err := step(ctx, s, "deliver-report", struct{}{}, func(context.Context, struct{}) (ReportOutput, error) {
			return ReportOutput{ReportID: in.ReportID, Delivered: true}, nil
		}, &out); err != nil {
			return ReportOutput{}, err
		}
		return out, nil
	})
}

func step[I, O any](ctx context.Context, s *durable.StepRunner, id string, in I, fn func(context.Context, I) (O, error), dest ...*O) error {
	var executed atomic.Bool
	got, err := durable.RunStep(ctx, s, id, in, func(ctx context.Context, in I) (O, error) {
		executed.Store(true)
		log.Printf("  → [%s] executed", id)
		return fn(ctx, in)
	}).Get(ctx)
	if err != nil {
		return err
	}
	if !executed.Load() {
		log.Printf("  · [%s] replayed (fn not called)", id)
	} else {
		log.Printf("  ✓ [%s] persisted", id)
	}
	if len(dest) > 0 {
		*dest[0] = got
	}
	return nil
}
