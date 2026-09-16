package durable_test

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	durable "github.com/agenticenv/durable-go"
)

func newTestEngine(t *testing.T, opts ...durable.EngineOption) *durable.Engine {
	t.Helper()
	e, err := durable.NewEngine(context.Background(), t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func TestNewEngine_EmptyDataDir(t *testing.T) {
	_, err := durable.NewEngine(context.Background(), "")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewEngine_NilContext(t *testing.T) {
	var nilCtx context.Context
	_, err := durable.NewEngine(nilCtx, t.TempDir())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewEngine_DoubleOpen(t *testing.T) {
	dir := t.TempDir()
	e1, err := durable.NewEngine(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e1.Close() }()

	_, err = durable.NewEngine(context.Background(), dir, durable.WithLockTimeout(200*time.Millisecond))
	if !errors.Is(err, durable.ErrEngineLocked) {
		t.Fatalf("got %v, want ErrEngineLocked", err)
	}
}

func TestNewEngine_ReopenAfterClose(t *testing.T) {
	dir := t.TempDir()
	e1, err := durable.NewEngine(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}
	e2, err := durable.NewEngine(context.Background(), dir)
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestEngine_CloseDrainsInFlightSteps starts a task with two fanned-out
// steps and closes the engine while one is still running. Close must not
// return until that step's goroutine has observed ctx cancellation and
// exited — otherwise compactJournal or a second Engine on the same dataDir
// could race an in-flight append.
func TestEngine_CloseDrainsInFlightSteps(t *testing.T) {
	e := newTestEngine(t)
	started := make(chan struct{})
	var finished atomic.Bool
	if err := durable.RegisterTask(e, "drain", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		x := durable.RunStep(ctx, s, "fast", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) { return "ok", nil })
		y := durable.RunStep(ctx, s, "slow", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			close(started)
			<-ctx.Done()
			finished.Store(true)
			return "", ctx.Err()
		})
		if _, err := x.Get(ctx); err != nil {
			return "", err
		}
		_, err := y.Get(ctx)
		return "", err
	}), durable.WithTaskTimeout(time.Hour)); err != nil {
		t.Fatal(err)
	}

	run := durable.RunTask[string, string](context.Background(), e, "drain", "", "")
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("step did not start")
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if !finished.Load() {
		t.Fatal("Close returned before the in-flight step observed cancellation")
	}
	_, _ = run.Get(context.Background())
}

func TestEngine_CloseIdempotent(t *testing.T) {
	e := newTestEngine(t)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestNewEngine_ExclusiveBlocksReadOnly(t *testing.T) {
	dir := t.TempDir()
	e, err := durable.NewEngine(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	_, err = durable.NewReadOnlyEngine(dir, durable.WithROLockTimeout(200*time.Millisecond))
	if !errors.Is(err, durable.ErrEngineLocked) {
		t.Fatalf("got %v, want ErrEngineLocked", err)
	}
}

func TestReadOnlyEngine_SharedCoexistence(t *testing.T) {
	dir := t.TempDir()
	e, err := durable.NewEngine(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := durable.RegisterTask(e, "echo", identityTask()); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "echo", "r1", "hi")
	if _, err := run.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	r1, err := durable.NewReadOnlyEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r1.Close() }()
	r2, err := durable.NewReadOnlyEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r2.Close() }()

	tasks, err := r1.ListTasks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].RunID != "r1" {
		t.Fatalf("%+v", tasks)
	}
	info, ok, err := r2.GetTask(context.Background(), "echo", "r1")
	if err != nil || !ok || info.Status != durable.StatusCompleted {
		t.Fatalf("ok=%v info=%+v err=%v", ok, info, err)
	}
	steps, err := r1.LoadSteps(context.Background(), "echo", "r1")
	if err != nil || len(steps) != 1 {
		t.Fatalf("steps=%v err=%v", steps, err)
	}
	in, ok, err := r1.LoadInput(context.Background(), "echo", "r1")
	if err != nil || !ok || string(in) != `"hi"` {
		t.Fatalf("input ok=%v %s err=%v", ok, in, err)
	}
}

func TestReadOnlyEngine_BlockedWhileWriterHoldsLock(t *testing.T) {
	dir := t.TempDir()
	e, err := durable.NewEngine(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	_, err = durable.NewReadOnlyEngine(dir, durable.WithROLockTimeout(150*time.Millisecond))
	if !errors.Is(err, durable.ErrEngineLocked) {
		t.Fatalf("got %v, want ErrEngineLocked", err)
	}
}

func TestReadOnlyEngine_BlocksWriter(t *testing.T) {
	dir := t.TempDir()
	e, err := durable.NewEngine(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := durable.NewReadOnlyEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	_, err = durable.NewEngine(context.Background(), dir, durable.WithLockTimeout(150*time.Millisecond))
	if !errors.Is(err, durable.ErrEngineLocked) {
		t.Fatalf("got %v, want ErrEngineLocked", err)
	}
}

func TestReadOnlyEngine_EmptyDataDir(t *testing.T) {
	_, err := durable.NewReadOnlyEngine("")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestReadOnlyEngine_CloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	e, err := durable.NewEngine(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := durable.NewReadOnlyEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReadOnlyEngine_MissingDir(t *testing.T) {
	_, err := durable.NewReadOnlyEngine(t.TempDir() + "/nope")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWithAutoPurge(t *testing.T) {
	e := newTestEngine(t, durable.WithAutoPurge(time.Millisecond, 20*time.Millisecond))
	if err := durable.RegisterTask(e, "echo", identityTask()); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.RunTask[string, string](context.Background(), e, "echo", "old", "x").Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		all, err := e.ListTasks(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(all) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("auto-purge did not remove completed run")
}

// TestCancelRun_LiveInFlightStepObservesCancellation starts a task with one
// step blocked on <-ctx.Done(), calls CancelRun while it is running, and
// checks the step's ctx fires immediately and the run ends up StatusFailed
// with the ErrRunCancelled message.
func TestCancelRun_LiveInFlightStepObservesCancellation(t *testing.T) {
	e := newTestEngine(t)
	started := make(chan struct{})
	var observedCancel atomic.Bool

	if err := durable.RegisterTask(e, "cancel-live", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "block", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			close(started)
			<-ctx.Done()
			observedCancel.Store(true)
			return "", ctx.Err()
		}).Get(ctx)
	}), durable.WithTaskTimeout(time.Hour)); err != nil {
		t.Fatal(err)
	}

	run := durable.RunTask[string, string](context.Background(), e, "cancel-live", "r1", "")
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("step did not start")
	}

	if err := e.CancelRun(context.Background(), "cancel-live", "r1"); err != nil {
		t.Fatal(err)
	}

	_, err := run.Get(context.Background())
	if !errors.Is(err, durable.ErrRunCancelled) {
		t.Fatalf("got %v, want ErrRunCancelled", err)
	}
	if !observedCancel.Load() {
		t.Fatal("in-flight step never observed ctx cancellation")
	}

	info, ok, err := e.GetTask(context.Background(), "cancel-live", "r1")
	if err != nil || !ok {
		t.Fatalf("GetTask: ok=%v err=%v", ok, err)
	}
	if info.Status != durable.StatusFailed || info.Error != durable.ErrRunCancelled.Error() {
		t.Fatalf("info=%+v", info)
	}
}

// TestCancelRun_SubsequentRunStepFailsFast proves RunStep is checked before
// running fn, not just at the step that was in flight when CancelRun fired:
// once the first step observes cancellation and returns, a second RunStep
// call must fail immediately without ever invoking its fn.
func TestCancelRun_SubsequentRunStepFailsFast(t *testing.T) {
	e := newTestEngine(t)
	started := make(chan struct{})
	var secondStepCalled atomic.Bool

	if err := durable.RegisterTask(e, "cancel-chain", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		_, err := durable.RunStep(ctx, s, "block", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			close(started)
			<-ctx.Done()
			return "", ctx.Err()
		}).Get(ctx)
		if err == nil {
			t.Error("expected first step to fail")
		}
		_, err2 := durable.RunStep(ctx, s, "second", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			secondStepCalled.Store(true)
			return "should not run", nil
		}).Get(ctx)
		return "", err2
	}), durable.WithTaskTimeout(time.Hour)); err != nil {
		t.Fatal(err)
	}

	run := durable.RunTask[string, string](context.Background(), e, "cancel-chain", "r1", "")
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("step did not start")
	}

	if err := e.CancelRun(context.Background(), "cancel-chain", "r1"); err != nil {
		t.Fatal(err)
	}

	_, err := run.Get(context.Background())
	if !errors.Is(err, durable.ErrRunCancelled) {
		t.Fatalf("got %v, want ErrRunCancelled", err)
	}
	if secondStepCalled.Load() {
		t.Fatal("second RunStep's fn ran after cancellation")
	}
}

// TestCancelRun_SurvivesCrashResume persists a cancel signal on a copy of
// the data directory that has no live goroutine for the run (simulating a
// process that crashed while the step was durably StatusWaiting), then
// resumes on a fresh engine and checks the run fails fast with
// ErrRunCancelled without ever re-invoking the step's fn.
func TestCancelRun_SurvivesCrashResume(t *testing.T) {
	src := t.TempDir()
	ctx := context.Background()
	var calls atomic.Int32

	e1, err := durable.NewEngine(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	if err := durable.RegisterTask(e1, "cancel-resume", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "work", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			calls.Add(1)
			return "", durable.ErrStepPending
		}).Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](ctx, e1, "cancel-resume", "r1", "")
	waitUntil(t, 2*time.Second, func() bool { return run.Status() == durable.StatusWaiting })
	if calls.Load() != 1 {
		t.Fatalf("fn calls %d", calls.Load())
	}

	dst := t.TempDir()
	if err := copyDir(src, dst); err != nil {
		t.Fatal(err)
	}
	_ = e1.Close()

	e2, err := durable.NewEngine(ctx, dst)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e2.Close() }()
	if err := durable.RegisterTask(e2, "cancel-resume", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "work", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			calls.Add(1)
			return "", durable.ErrStepPending
		}).Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}

	// Nobody has called RunTask on e2 yet, so this only appends the durable
	// signal — there is no in-process ctx to cancel live.
	if err := e2.CancelRun(ctx, "cancel-resume", "r1"); err != nil {
		t.Fatal(err)
	}

	r2 := durable.RunTask[string, string](ctx, e2, "cancel-resume", "r1", "")
	_, err = r2.Get(ctx)
	if !errors.Is(err, durable.ErrRunCancelled) {
		t.Fatalf("got %v, want ErrRunCancelled", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("step fn re-ran on a cancelled resume: calls=%d", calls.Load())
	}

	info, ok, err := e2.GetTask(ctx, "cancel-resume", "r1")
	if err != nil || !ok {
		t.Fatalf("GetTask: ok=%v err=%v", ok, err)
	}
	if info.Status != durable.StatusFailed || info.Error != durable.ErrRunCancelled.Error() {
		t.Fatalf("info=%+v", info)
	}
}

func TestCancelRun_NotFound(t *testing.T) {
	e := newTestEngine(t)
	err := e.CancelRun(context.Background(), "nope", "r1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCancelRun_AlreadyFinished(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "done", identityTask()); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "done", "r1", "hi")
	if _, err := run.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	err := e.CancelRun(context.Background(), "done", "r1")
	if !errors.Is(err, durable.ErrRunAlreadyFinished) {
		t.Fatalf("got %v, want ErrRunAlreadyFinished", err)
	}
}

// TestCancelRun_Idempotent checks a second CancelRun call on an already
// cancel-requested, still-running run is a harmless no-op rather than an
// error or a duplicate signal that breaks resume.
func TestCancelRun_Idempotent(t *testing.T) {
	e := newTestEngine(t)
	started := make(chan struct{})

	if err := durable.RegisterTask(e, "cancel-twice", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "block", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			close(started)
			<-ctx.Done()
			return "", ctx.Err()
		}).Get(ctx)
	}), durable.WithTaskTimeout(time.Hour)); err != nil {
		t.Fatal(err)
	}

	run := durable.RunTask[string, string](context.Background(), e, "cancel-twice", "r1", "")
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("step did not start")
	}

	if err := e.CancelRun(context.Background(), "cancel-twice", "r1"); err != nil {
		t.Fatal(err)
	}
	if err := e.CancelRun(context.Background(), "cancel-twice", "r1"); err != nil {
		t.Fatalf("second CancelRun: %v", err)
	}

	_, err := run.Get(context.Background())
	if !errors.Is(err, durable.ErrRunCancelled) {
		t.Fatalf("got %v, want ErrRunCancelled", err)
	}
}

func TestReadOnly_WithROLogger(t *testing.T) {
	dir := t.TempDir()
	e, err := durable.NewEngine(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := durable.NewReadOnlyEngine(dir, durable.WithROLogger(slog.Default()), durable.WithROLockTimeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
}
