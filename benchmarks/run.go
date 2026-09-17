package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	durable "github.com/agenticenv/durable-go"
)

const benchTaskID = "bench-task"

type benchInput struct {
	Steps int `json:"steps"`
}

func nStepTask(steps int, payload string, execs *atomic.Int64) durable.Task[benchInput, string] {
	return durable.Func(func(ctx context.Context, s *durable.StepRunner, in benchInput) (string, error) {
		return runNSteps(ctx, s, in, steps, payload, execs)
	})
}

func nStepThenPending(steps int, payload string, execs *atomic.Int64) durable.Task[benchInput, string] {
	return durable.Func(func(ctx context.Context, s *durable.StepRunner, in benchInput) (string, error) {
		if _, err := runNSteps(ctx, s, in, steps, payload, execs); err != nil {
			return "", err
		}
		return durable.RunStep(ctx, s, "replay-gate", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return "", durable.ErrStepPending
		}).Get(ctx)
	})
}

func runNSteps(ctx context.Context, s *durable.StepRunner, in benchInput, steps int, payload string, execs *atomic.Int64) (string, error) {
	n := in.Steps
	if n <= 0 {
		n = steps
	}
	var last string
	for i := 0; i < n; i++ {
		out, err := durable.RunStep(ctx, s, fmt.Sprintf("step-%d", i), struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			if execs != nil {
				execs.Add(1)
			}
			return payload, nil
		}).Get(ctx)
		if err != nil {
			return "", err
		}
		last = out
	}
	return last, nil
}

var plainSink string

func runPlain(steps int, payload string) {
	for i := 0; i < steps; i++ {
		plainSink = payload
	}
}

func registerTask(e *durable.Engine, steps int, payload string, execs *atomic.Int64) error {
	return durable.RegisterTask(e, benchTaskID, nStepTask(steps, payload, execs))
}

func registerReplayTask(e *durable.Engine, steps int, payload string, execs *atomic.Int64) error {
	return durable.RegisterTask(e, benchTaskID, nStepThenPending(steps, payload, execs))
}

func runDurable(ctx context.Context, e *durable.Engine, runID string, steps int) error {
	run := durable.RunTask[benchInput, string](ctx, e, benchTaskID, runID, benchInput{Steps: steps})
	out, err := run.Get(ctx)
	if err != nil {
		return err
	}
	if len(out) == 0 {
		return fmt.Errorf("empty task output")
	}
	return nil
}

func runDirPath(dataDir, runID string) string {
	return filepath.Join(dataDir, "tasks", benchTaskID, runID)
}

func journalFilePath(dataDir, runID string) string {
	return filepath.Join(runDirPath(dataDir, runID), "journal.log")
}

// prepareReplayDir copies one completed run into a fresh dataDir and rewrites
// meta.json to StatusRunning so the next RunTask re-executes the task body and
// replays completed steps from the journal (fn must not run).
func prepareReplayDir(srcDataDir, runID, dstDataDir string) error {
	src := runDirPath(srcDataDir, runID)
	dst := runDirPath(dstDataDir, runID)
	if err := copyDir(src, dst); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(dst, "output.json")); err != nil && !os.IsNotExist(err) {
		return err
	}
	return markRunning(filepath.Join(dst, "meta.json"))
}

func markRunning(metaPath string) error {
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		return err
	}
	var info durable.TaskInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return fmt.Errorf("decode meta: %w", err)
	}
	info.Status = durable.StatusRunning
	info.Error = ""
	info.PanicTrace = ""
	info.CompletedAt = time.Time{}
	raw, err = json.Marshal(info)
	if err != nil {
		return err
	}
	return os.WriteFile(metaPath, raw, 0o644)
}

func waitStatus(run *durable.TaskRun[string], want durable.TaskStatus, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if run.Status() == want {
			return nil
		}
		time.Sleep(50 * time.Microsecond)
	}
	return fmt.Errorf("timed out waiting for status %s (last %s)", want, run.Status())
}

func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() { _ = in.Close() }()
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode())
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(out, in)
		closeErr := out.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
}
