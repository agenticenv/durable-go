package durable_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	durable "github.com/agenticenv/durable-go"
)

func identityTask() durable.TaskFunc[string, string] {
	return durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "echo", func(ctx context.Context) (string, error) {
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
		return durable.RunStep(ctx, s, "s", func(ctx context.Context) (string, error) {
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
		a, err := durable.RunStep(ctx, s, "one", func(ctx context.Context) (string, error) {
			steps.Add(1)
			return "A", nil
		}).Get(ctx)
		if err != nil {
			return "", err
		}
		b, err := durable.RunStep(ctx, s, "two", func(ctx context.Context) (string, error) {
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

func TestRunTask_EmptyRunIDResumesOldestActive(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "single", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		token := ""
		_, err := durable.RunStep(ctx, s, "wait", func(ctx context.Context) (string, error) {
			token = s.StepToken()
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
		_, err := durable.RunStep(ctx, s, "wait", func(ctx context.Context) (string, error) {
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
	e := newTestEngine(t, durable.WithMaxRetries(3), durable.WithLogger(slog.Default()))
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
		return durable.RunStep(ctx, s, "f", func(ctx context.Context) (string, error) {
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

func TestLoadSteps_OrderedBySeq(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "seq", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		if _, err := durable.RunStep(ctx, s, "a", func(ctx context.Context) (string, error) { return "1", nil }).Get(ctx); err != nil {
			return "", err
		}
		if _, err := durable.RunStep(ctx, s, "b", func(ctx context.Context) (string, error) { return "2", nil }).Get(ctx); err != nil {
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
	if len(steps) != 2 || steps[0].StepID != "a" || steps[1].StepID != "b" {
		t.Fatalf("%+v", steps)
	}
	if steps[0].Seq != 1 || steps[1].Seq != 2 {
		t.Fatalf("seq %+v", steps)
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
