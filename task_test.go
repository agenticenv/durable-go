package durable_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	durable "github.com/agenticenv/durable-go"
)

func identityTask() durable.TaskFunc[string, string] {
	return durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "echo", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return in, nil
		}).Get(ctx)
	})
}

func TestRegisterTask_Success(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "echo", identityTask(), durable.WithName("Echo"), durable.WithTag("k", "v")); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterTask_Duplicate(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "echo", identityTask()); err != nil {
		t.Fatal(err)
	}
	err := durable.RegisterTask(e, "echo", identityTask())
	if !errors.Is(err, durable.ErrTaskAlreadyRegistered) {
		t.Fatalf("got %v", err)
	}
}

func TestRegisterTask_InvalidTaskID(t *testing.T) {
	e := newTestEngine(t)
	for _, id := range []string{"", "a/b", `a\b`, "a..b", "a:b", "..", "."} {
		err := durable.RegisterTask(e, id, identityTask())
		if err == nil {
			t.Fatalf("id %q: expected error", id)
		}
	}
}

func TestRunTask_NotRegistered(t *testing.T) {
	e := newTestEngine(t)
	run := durable.RunTask[string, string](context.Background(), e, "missing", "", "x")
	if run.RunID() != "" {
		t.Fatalf("RunID should be empty, got %q", run.RunID())
	}
	_, err := run.Get(context.Background())
	if !errors.Is(err, durable.ErrTaskNotRegistered) {
		t.Fatalf("got %v", err)
	}
}

func TestRunTask_InvalidTaskID(t *testing.T) {
	e := newTestEngine(t)
	run := durable.RunTask[string, string](context.Background(), e, "bad/id", "", "x")
	_, err := run.Get(context.Background())
	if err == nil || !strings.Contains(err.Error(), "invalid task ID") {
		t.Fatalf("got %v", err)
	}
}

func TestRunTask_NewRun(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "echo", identityTask(), durable.WithName("Echo")); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "echo", "", "hello")
	if run.RunID() == "" {
		t.Fatal("RunID should be available immediately")
	}
	out, err := run.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out != "hello" {
		t.Fatalf("got %q", out)
	}
	if run.Status() != durable.StatusCompleted {
		t.Fatalf("status %s", run.Status())
	}

	info, ok, err := e.GetTask(context.Background(), "echo", run.RunID())
	if err != nil || !ok {
		t.Fatalf("GetTask: ok=%v err=%v", ok, err)
	}
	if info.Name != "Echo" || info.Status != durable.StatusCompleted {
		t.Fatalf("info %+v", info)
	}
}

func TestRunTask_RunIDAvailableBeforeGet(t *testing.T) {
	e := newTestEngine(t)
	started := make(chan struct{})
	if err := durable.RegisterTask(e, "slow", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	}), durable.WithTaskTimeout(50*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "slow", "", "x")
	if run.RunID() == "" {
		t.Fatal("RunID empty before Get")
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("task did not start")
	}
	_, _ = run.Get(context.Background())
}

func TestRunTask_GetBlocksThenReturns(t *testing.T) {
	e := newTestEngine(t)
	release := make(chan struct{})
	if err := durable.RegisterTask(e, "block", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		<-release
		return in, nil
	})); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "block", "", "done")
	done := make(chan struct{})
	var out string
	var err error
	go func() {
		out, err = run.Get(context.Background())
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("Get returned before release")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Get did not return")
	}
	if err != nil || out != "done" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestRunTask_StatusPolling(t *testing.T) {
	e := newTestEngine(t)
	release := make(chan struct{})
	if err := durable.RegisterTask(e, "poll", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		<-release
		return "ok", nil
	})); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "poll", "", "")
	deadline := time.Now().Add(2 * time.Second)
	for run.Status() != durable.StatusRunning && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if run.Status() != durable.StatusRunning {
		t.Fatalf("status %s", run.Status())
	}
	close(release)
	if _, err := run.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	if run.Status() != durable.StatusCompleted {
		t.Fatalf("status %s", run.Status())
	}
}

func TestRunTask_IdempotentRunID(t *testing.T) {
	e := newTestEngine(t)
	var n atomic.Int32
	if err := durable.RegisterTask(e, "once", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		n.Add(1)
		return durable.RunStep(ctx, s, "s", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return in, nil
		}).Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}
	const runID = "req-42"
	r1 := durable.RunTask[string, string](context.Background(), e, "once", runID, "a")
	out1, err := r1.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r2 := durable.RunTask[string, string](context.Background(), e, "once", runID, "b")
	out2, err := r2.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out1 != "a" || out2 != "a" {
		t.Fatalf("out1=%q out2=%q", out1, out2)
	}
	if n.Load() != 1 {
		t.Fatalf("closure invoked %d times", n.Load())
	}
}

func TestRunTask_CompletedReturnsStoredOutput(t *testing.T) {
	e := newTestEngine(t)
	var n atomic.Int32
	if err := durable.RegisterTask(e, "store", durable.Func(func(ctx context.Context, s *durable.StepRunner, in int) (int, error) {
		n.Add(1)
		return in + 1, nil
	})); err != nil {
		t.Fatal(err)
	}
	r1 := durable.RunTask[int, int](context.Background(), e, "store", "r1", 10)
	out, err := r1.Get(context.Background())
	if err != nil || out != 11 {
		t.Fatalf("out=%d err=%v", out, err)
	}
	r2 := durable.RunTask[int, int](context.Background(), e, "store", "r1", 99)
	out, err = r2.Get(context.Background())
	if err != nil || out != 11 {
		t.Fatalf("stored out=%d err=%v", out, err)
	}
	if n.Load() != 1 {
		t.Fatalf("invocations %d", n.Load())
	}
}

func TestRunTask_ResumeReplaysSteps(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	var steps atomic.Int32
	task := durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		a, err := durable.RunStep(ctx, s, "one", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			steps.Add(1)
			return "A", nil
		}).Get(ctx)
		if err != nil {
			return "", err
		}
		b, err := durable.RunStep(ctx, s, "two", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			steps.Add(1)
			return a + "B", nil
		}).Get(ctx)
		if err != nil {
			return "", err
		}
		return b, nil
	})

	e1, err := durable.NewEngine(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := durable.RegisterTask(e1, "resume", task); err != nil {
		t.Fatal(err)
	}
	r1 := durable.RunTask[string, string](ctx, e1, "resume", "run-1", "")
	out, err := r1.Get(ctx)
	if err != nil || out != "AB" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := durable.NewEngine(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e2.Close() }()
	if err := durable.RegisterTask(e2, "resume", task); err != nil {
		t.Fatal(err)
	}
	r2 := durable.RunTask[string, string](ctx, e2, "resume", "run-1", "")
	out, err = r2.Get(ctx)
	if err != nil || out != "AB" {
		t.Fatalf("resume out=%q err=%v", out, err)
	}
	if steps.Load() != 2 {
		t.Fatalf("steps executed %d, want 2 (no re-exec on resume of completed run)", steps.Load())
	}
}

func TestRunTask_ResumeUsesStoredInput(t *testing.T) {
	src := t.TempDir()
	ctx := context.Background()
	e1, err := durable.NewEngine(ctx, src, durable.WithUnsignedStepTokens())
	if err != nil {
		t.Fatal(err)
	}
	if err := durable.RegisterTask(e1, "echo-in", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		_, err := durable.RunStep(ctx, s, "wait", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return "", durable.ErrStepPending
		}).Get(ctx)
		return in, err
	})); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](ctx, e1, "echo-in", "r1", "keep-me")
	waitUntil(t, 2*time.Second, func() bool { return run.Status() == durable.StatusWaiting })

	dst := t.TempDir()
	if err := copyDir(src, dst); err != nil {
		t.Fatal(err)
	}
	_ = e1.Close()

	e2, err := durable.NewEngine(ctx, dst, durable.WithUnsignedStepTokens())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e2.Close() }()
	if err := durable.RegisterTask(e2, "echo-in", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		_, err := durable.RunStep(ctx, s, "wait", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return "", durable.ErrStepPending
		}).Get(ctx)
		return in, err
	})); err != nil {
		t.Fatal(err)
	}

	token := encodeTestToken("echo-in", "r1", "wait")
	if err := durable.CompleteStep(ctx, e2, token, "ok"); err != nil {
		t.Fatal(err)
	}
	out, err := durable.RunTask[string, string](ctx, e2, "echo-in", "r1", "ignore-me").Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out != "keep-me" {
		t.Fatalf("stored input not used: got %q", out)
	}
}

func TestRunTask_EmptyRunIDResumesOldestActive(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "single", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		token := ""
		_, err := durable.RunStep(ctx, s, "wait", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			token = s.StepToken(ctx)
			return "", durable.ErrStepPending
		}).Get(ctx)
		return token, err
	})); err != nil {
		t.Fatal(err)
	}
	r1 := durable.RunTask[string, string](context.Background(), e, "single", "", "")
	deadline := time.Now().Add(2 * time.Second)
	for r1.Status() != durable.StatusWaiting && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if r1.Status() != durable.StatusWaiting {
		t.Fatalf("status %s", r1.Status())
	}
	r2 := durable.RunTask[string, string](context.Background(), e, "single", "", "")
	if r2.RunID() != r1.RunID() {
		t.Fatalf("empty runID should resume singleton: %q vs %q", r2.RunID(), r1.RunID())
	}
	info, ok, err := e.GetTask(context.Background(), "single", r1.RunID())
	if err != nil || !ok {
		t.Fatal(err)
	}
	_ = info
}

func TestRunTask_InvalidRunID(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "echo", identityTask()); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "echo", "a/b", "x")
	_, err := run.Get(context.Background())
	if !errors.Is(err, durable.ErrInvalidRunID) {
		t.Fatalf("got %v", err)
	}
}

func TestRunTask_PurgeThenRetry(t *testing.T) {
	e := newTestEngine(t)
	var n atomic.Int32
	if err := durable.RegisterTask(e, "p", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		n.Add(1)
		return in, nil
	})); err != nil {
		t.Fatal(err)
	}
	r1 := durable.RunTask[string, string](context.Background(), e, "p", "rid", "first")
	if _, err := r1.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.DeleteTaskRun(context.Background(), "p", "rid"); err != nil {
		t.Fatal(err)
	}
	r2 := durable.RunTask[string, string](context.Background(), e, "p", "rid", "second")
	out, err := r2.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out != "second" {
		t.Fatalf("got %q", out)
	}
	if n.Load() != 2 {
		t.Fatalf("invocations %d", n.Load())
	}
}

func TestRunTask_StaleDetectionViaListTasks(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "stale", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		_, err := durable.RunStep(ctx, s, "wait", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return "", durable.ErrStepPending
		}).Get(ctx)
		return "", err
	})); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "stale", "s1", "")
	deadline := time.Now().Add(2 * time.Second)
	for run.Status() != durable.StatusWaiting && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	pending, err := e.ListTasks(context.Background(), durable.StatusRunning, durable.StatusWaiting)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].RunID != "s1" {
		t.Fatalf("pending %+v", pending)
	}
}

func TestRunTask_GetCancelled(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "hang", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}), durable.WithTaskTimeout(time.Hour)); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "hang", "", "")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := run.Get(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
}

func TestRunTask_FailedReturnsError(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "fail", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return "", errors.New("boom")
	})); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "fail", "f1", "")
	_, err := run.Get(context.Background())
	if err == nil || err.Error() != "boom" {
		t.Fatalf("got %v", err)
	}
	run2 := durable.RunTask[string, string](context.Background(), e, "fail", "f1", "")
	_, err = run2.Get(context.Background())
	if err == nil || err.Error() != "boom" {
		t.Fatalf("stored fail: %v", err)
	}
}

func TestOptionResolution_RunOverridesEngine(t *testing.T) {
	e := newTestEngine(t, durable.WithMaxRetries(3))
	var n atomic.Int32
	if err := durable.RegisterTask(e, "opt", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		n.Add(1)
		return "", errors.New("non-step")
	})); err != nil {
		t.Fatal(err)
	}
	_, err := durable.RunTask[string, string](context.Background(), e, "opt", "r", "", durable.WithRunMaxRetries(0)).Get(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if n.Load() != 1 {
		t.Fatalf("explicit 0 should disable engine retries, got %d", n.Load())
	}
}

func TestTaskMaxRetries_RetriesPanic(t *testing.T) {
	e := newTestEngine(t)
	var n atomic.Int32
	if err := durable.RegisterTask(e, "tr", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		if n.Add(1) == 1 {
			panic("once")
		}
		return "ok", nil
	}), durable.WithTaskMaxRetries(1)); err != nil {
		t.Fatal(err)
	}
	out, err := durable.RunTask[string, string](context.Background(), e, "tr", "", "").Get(context.Background())
	if err != nil || out != "ok" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	if n.Load() != 2 {
		t.Fatalf("attempts %d", n.Load())
	}
}

func TestTaskRetries_DoNotRetryStepError(t *testing.T) {
	e := newTestEngine(t)
	var n atomic.Int32
	if err := durable.RegisterTask(e, "sr", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		n.Add(1)
		return durable.RunStep(ctx, s, "f", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return "", errors.New("step-fail")
		}).Get(ctx)
	}), durable.WithTaskMaxRetries(3)); err != nil {
		t.Fatal(err)
	}
	_, err := durable.RunTask[string, string](context.Background(), e, "sr", "", "").Get(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if n.Load() != 1 {
		t.Fatalf("task retried a step error: %d", n.Load())
	}
}

func TestWithRunTimeout(t *testing.T) {
	e := newTestEngine(t, durable.WithTimeout(time.Hour))
	if err := durable.RegisterTask(e, "to", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})); err != nil {
		t.Fatal(err)
	}
	_, err := durable.RunTask[string, string](context.Background(), e, "to", "", "", durable.WithRunTimeout(20*time.Millisecond)).Get(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
}

func TestListTasks_FilterAndAll(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "ok", identityTask()); err != nil {
		t.Fatal(err)
	}
	if err := durable.RegisterTask(e, "fail", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return "", errors.New("no")
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.RunTask[string, string](context.Background(), e, "ok", "a", "x").Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, _ = durable.RunTask[string, string](context.Background(), e, "fail", "b", "x").Get(context.Background())

	all, err := e.ListTasks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("all=%d", len(all))
	}
	completed, err := e.ListTasks(context.Background(), durable.StatusCompleted)
	if err != nil {
		t.Fatal(err)
	}
	if len(completed) != 1 || completed[0].RunID != "a" {
		t.Fatalf("%+v", completed)
	}
	failed, err := e.ListTasks(context.Background(), durable.StatusFailed)
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 1 || failed[0].RunID != "b" {
		t.Fatalf("%+v", failed)
	}
}

func TestGetTask_NotFound(t *testing.T) {
	e := newTestEngine(t)
	_, ok, err := e.GetTask(context.Background(), "missing", "r")
	if err != nil || ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}

func TestLoadSteps_ContainsAllStepsAnyOrder(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "seq", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		if _, err := durable.RunStep(ctx, s, "a", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) { return "1", nil }).Get(ctx); err != nil {
			return "", err
		}
		if _, err := durable.RunStep(ctx, s, "b", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) { return "2", nil }).Get(ctx); err != nil {
			return "", err
		}
		return "ok", nil
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.RunTask[string, string](context.Background(), e, "seq", "r", "").Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	steps, err := e.LoadSteps(context.Background(), "seq", "r")
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 2 {
		t.Fatalf("%+v", steps)
	}
	byID := make(map[string]durable.StepRecord, len(steps))
	for _, s := range steps {
		byID[s.StepID] = s
	}
	if byID["a"].Status != durable.StepStatusCompleted || byID["b"].Status != durable.StepStatusCompleted {
		t.Fatalf("%+v", byID)
	}
}

func TestGetStep_ByID(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "gs", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "only", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return "v", nil
		}).Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.RunTask[string, string](context.Background(), e, "gs", "r1", "").Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec, ok, err := e.GetStep(context.Background(), "gs", "r1", "only")
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if rec.Status != durable.StepStatusCompleted {
		t.Fatalf("status %s", rec.Status)
	}
	_, ok, err = e.GetStep(context.Background(), "gs", "r1", "missing")
	if err != nil || ok {
		t.Fatalf("missing step should not be found: ok=%v err=%v", ok, err)
	}
}

func TestDeleteTaskRun(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "echo", identityTask()); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.RunTask[string, string](context.Background(), e, "echo", "r1", "x").Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.DeleteTaskRun(context.Background(), "echo", "r1"); err != nil {
		t.Fatal(err)
	}
	_, ok, err := e.GetTask(context.Background(), "echo", "r1")
	if err != nil || ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if err := e.DeleteTaskRun(context.Background(), "echo", "r1"); err != nil {
		t.Fatalf("no-op delete: %v", err)
	}
}

func TestDeleteTask(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "echo", identityTask()); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.RunTask[string, string](context.Background(), e, "echo", "r1", "x").Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.RunTask[string, string](context.Background(), e, "echo", "r2", "y").Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.DeleteTask(context.Background(), "echo"); err != nil {
		t.Fatal(err)
	}
	all, err := e.ListTasks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 0 {
		t.Fatalf("left %+v", all)
	}
}

func TestDeleteTask_ActiveReturnsErrRunActive(t *testing.T) {
	e := newTestEngine(t)
	registerApproval(t, e, nil)
	run := durable.RunTask[string, approval](context.Background(), e, "approve", "r1", "")
	waitUntil(t, 2*time.Second, func() bool { return run.Status() == durable.StatusWaiting })
	err := e.DeleteTask(context.Background(), "approve")
	if !errors.Is(err, durable.ErrRunActive) {
		t.Fatalf("got %v", err)
	}
}

func TestListTasksPage(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "echo", identityTask()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"r1", "r2", "r3"} {
		if _, err := durable.RunTask[string, string](context.Background(), e, "echo", id, "x").Get(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	page, err := e.ListTasksPage(context.Background(), 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Tasks) != 2 || !page.HasMore {
		t.Fatalf("page %+v", page)
	}
	rest, err := e.ListTasksPage(context.Background(), 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest.Tasks) != 1 || rest.HasMore {
		t.Fatalf("rest %+v", rest)
	}
}

func TestListTasks_Empty(t *testing.T) {
	e := newTestEngine(t)
	all, err := e.ListTasks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 0 {
		t.Fatalf("%+v", all)
	}
}

func TestGetTask_InvalidIDs(t *testing.T) {
	e := newTestEngine(t)
	if _, _, err := e.GetTask(context.Background(), "a/b", "r"); err == nil {
		t.Fatal("expected invalid taskID")
	}
	if _, _, err := e.GetTask(context.Background(), "t", "a/b"); !errors.Is(err, durable.ErrInvalidRunID) {
		t.Fatalf("got %v", err)
	}
}

func recvWatchSteps(t *testing.T, ch <-chan durable.StepEvent, n int, timeout time.Duration) []durable.StepEvent {
	t.Helper()
	out := make([]durable.StepEvent, 0, n)
	deadline := time.After(timeout)
	for len(out) < n {
		select {
		case rec, ok := <-ch:
			if !ok {
				t.Fatalf("watch closed after %d records, want %d", len(out), n)
			}
			out = append(out, rec)
		case <-deadline:
			t.Fatalf("timeout waiting for %d watch records, got %d", n, len(out))
		}
	}
	return out
}

// TestWatchSteps_Live gates step execution on a release channel so the test
// can confirm the run exists (meta.json written) and attach the watch
// before any step runs — proving events are delivered live, not just
// replayed from a post-hoc snapshot.
func TestWatchSteps_Live(t *testing.T) {
	e := newTestEngine(t)
	release := make(chan struct{})
	if err := durable.RegisterTask(e, "seq", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		<-release
		if _, err := durable.RunStep(ctx, s, "a", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) { return "1", nil }).Get(ctx); err != nil {
			return "", err
		}
		if _, err := durable.RunStep(ctx, s, "b", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) { return "2", nil }).Get(ctx); err != nil {
			return "", err
		}
		return "ok", nil
	})); err != nil {
		t.Fatal(err)
	}

	run := durable.RunTask[string, string](context.Background(), e, "seq", "r", "")
	waitUntil(t, 2*time.Second, func() bool {
		_, ok, _ := e.GetTask(context.Background(), "seq", "r")
		return ok
	})
	ch, err := e.WatchSteps(context.Background(), "seq", "r", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	if _, err := run.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Each step emits STARTED then COMPLETED, so 4 events total, in order.
	got := recvWatchSteps(t, ch, 4, 2*time.Second)
	wantIDs := []string{"a", "a", "b", "b"}
	wantStatus := []durable.StepStatus{durable.StepStatusRunning, durable.StepStatusCompleted, durable.StepStatusRunning, durable.StepStatusCompleted}
	for i, ev := range got {
		if ev.StepID != wantIDs[i] || ev.Status != wantStatus[i] {
			t.Fatalf("event %d: got %+v, want stepID=%s status=%s", i, ev, wantIDs[i], wantStatus[i])
		}
		if i > 0 && ev.Offset <= got[i-1].Offset {
			t.Fatalf("offsets must strictly increase: %+v", got)
		}
	}
}

// TestWatchSteps_FromOffset and TestWatchSteps_FromByteOffset use a run
// that ends on a still-pending step, so the journal is never compacted —
// compactJournal (terminal runs only) collapses each step to its latest
// record and drops STARTED entries, which would otherwise make history
// unavailable to a reconnecting watcher.
func TestWatchSteps_FromOffset(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "seq", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		if _, err := durable.RunStep(ctx, s, "a", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) { return "1", nil }).Get(ctx); err != nil {
			return "", err
		}
		if _, err := durable.RunStep(ctx, s, "b", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) { return "2", nil }).Get(ctx); err != nil {
			return "", err
		}
		_, err := durable.RunStep(ctx, s, "wait", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return "", durable.ErrStepPending
		}).Get(ctx)
		return "ok", err
	})); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "seq", "r", "")
	waitUntil(t, 2*time.Second, func() bool { return run.Status() == durable.StatusWaiting })

	// Skip the first two events (a STARTED, a COMPLETED) via fromOffset.
	ch, err := e.WatchSteps(context.Background(), "seq", "r", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := recvWatchSteps(t, ch, 2, 2*time.Second)
	if got[0].StepID != "b" || got[0].Status != durable.StepStatusRunning {
		t.Fatalf("%+v", got[0])
	}
	if got[1].StepID != "b" || got[1].Status != durable.StepStatusCompleted {
		t.Fatalf("%+v", got[1])
	}
}

func TestWatchSteps_FromByteOffset(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "seq", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		if _, err := durable.RunStep(ctx, s, "a", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) { return "1", nil }).Get(ctx); err != nil {
			return "", err
		}
		if _, err := durable.RunStep(ctx, s, "b", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) { return "2", nil }).Get(ctx); err != nil {
			return "", err
		}
		_, err := durable.RunStep(ctx, s, "wait", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return "", durable.ErrStepPending
		}).Get(ctx)
		return "ok", err
	})); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "seq", "r", "")
	waitUntil(t, 2*time.Second, func() bool { return run.Status() == durable.StatusWaiting })

	// Full history first, to learn the byte offset right after "a" COMPLETED.
	full, err := e.WatchSteps(context.Background(), "seq", "r", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	firstTwo := recvWatchSteps(t, full, 2, 2*time.Second)
	resumeAt := firstTwo[1].ByteOffset

	ch, err := e.WatchSteps(context.Background(), "seq", "r", 0, resumeAt)
	if err != nil {
		t.Fatal(err)
	}
	got := recvWatchSteps(t, ch, 2, 2*time.Second)
	if got[0].StepID != "b" || got[0].Status != durable.StepStatusRunning {
		t.Fatalf("fromByteOffset should skip straight past %q, got %+v", "a", got[0])
	}
}

func TestWatchSteps_RunNotFound(t *testing.T) {
	e := newTestEngine(t)
	_, err := e.WatchSteps(context.Background(), "missing", "r1", 0, 0)
	if err == nil {
		t.Fatal("expected error for non-existent run")
	}
}

func TestWatchSteps_CancelClosesChannel(t *testing.T) {
	e := newTestEngine(t)
	registerApproval(t, e, nil)
	run := durable.RunTask[string, approval](context.Background(), e, "approve", "r1", "")
	waitUntil(t, 2*time.Second, func() bool { return run.Status() == durable.StatusWaiting })

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := e.WatchSteps(ctx, "approve", "r1", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// STARTED then WAITING — drain both before cancelling so the close
	// check below cannot instead observe an already-buffered event.
	_ = recvWatchSteps(t, ch, 2, 2*time.Second)
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected watch channel to close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watch did not close after cancel")
	}
	if run.Status() != durable.StatusWaiting {
		t.Fatalf("run stopped: %s", run.Status())
	}
}

func TestWatchSteps_WaitingThenCompleted(t *testing.T) {
	e := newTestEngine(t)
	registerApproval(t, e, nil)
	run := durable.RunTask[string, approval](context.Background(), e, "approve", "r1", "")
	waitUntil(t, 2*time.Second, func() bool { return run.Status() == durable.StatusWaiting })

	ch, err := e.WatchSteps(context.Background(), "approve", "r1", 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	// STARTED, then WAITING.
	first := recvWatchSteps(t, ch, 2, 2*time.Second)
	if first[0].StepID != "approve" || first[0].Status != durable.StepStatusRunning {
		t.Fatalf("started %+v", first[0])
	}
	if first[1].StepID != "approve" || first[1].Status != durable.StepStatusWaiting {
		t.Fatalf("waiting %+v", first[1])
	}

	token := encodeTestToken("approve", "r1", "approve")
	if err := durable.CompleteStep(context.Background(), e, token, approval{By: "ok"}); err != nil {
		t.Fatal(err)
	}
	second := recvWatchSteps(t, ch, 1, 2*time.Second)
	if second[0].StepID != "approve" || second[0].Status != durable.StepStatusCompleted {
		t.Fatalf("completed %+v", second[0])
	}
	if _, err := run.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWatchSteps_EngineCloseClosesChannel(t *testing.T) {
	e, err := durable.NewEngine(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := durable.RegisterTask(e, "t", identityTask()); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.RunTask[string, string](context.Background(), e, "t", "r", "x").Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	ch, err := e.WatchSteps(context.Background(), "t", "r", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Drain the one buffered snapshot event (the completed "echo" step)
	// before checking that Close terminates the watch.
	_ = recvWatchSteps(t, ch, 1, 2*time.Second)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected watch channel to close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watch did not close after Engine.Close")
	}
}

// TestWatchSteps_SlowConsumerDropLogsWarning fills the watch buffer past
// capacity with a consumer that never reads, then verifies the run still
// completes (notifyStepWatchers must not block on a full channel) and that
// a drop warning was logged.
func TestWatchSteps_SlowConsumerDropLogsWarning(t *testing.T) {
	var logBuf syncBuffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))
	e := newTestEngine(t, durable.WithLogger(logger))

	const nSteps = 200 // watchStepsBuf is 64; each step emits 2 events.
	release := make(chan struct{})
	if err := durable.RegisterTask(e, "slowwatch", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		<-release
		for i := 0; i < nSteps; i++ {
			id := fmt.Sprintf("s%d", i)
			if _, err := durable.RunStep(ctx, s, id, struct{}{}, func(ctx context.Context, _ struct{}) (string, error) { return "ok", nil }).Get(ctx); err != nil {
				return "", err
			}
		}
		return "done", nil
	})); err != nil {
		t.Fatal(err)
	}

	run := durable.RunTask[string, string](context.Background(), e, "slowwatch", "r1", "")
	waitUntil(t, 2*time.Second, func() bool {
		_, ok, _ := e.GetTask(context.Background(), "slowwatch", "r1")
		return ok
	})
	// Register the watch but never read from it — a slow/absent consumer.
	if _, err := e.WatchSteps(context.Background(), "slowwatch", "r1", 0, 0); err != nil {
		t.Fatal(err)
	}
	close(release)

	out, err := run.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out != "done" {
		t.Fatalf("got %q", out)
	}
	if !strings.Contains(logBuf.String(), "consumer too slow") {
		t.Fatal("expected a slow-consumer drop warning to be logged")
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
