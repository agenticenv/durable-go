package durable_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	durable "github.com/agenticenv/durable-go"
)

func skipIfWindows(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("file permission bits are not enforced on Windows")
	}
}

func assertPerm(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Mode().Perm(); got != want {
		t.Fatalf("%s mode %04o, want %04o", path, got, want)
	}
}

func newTestEngine(t *testing.T, opts ...durable.Option) *durable.Engine {
	t.Helper()
	// Tests that mint encodeTestToken values need unsigned tokens. HMAC
	// tests pass WithStepTokenKey, which takes precedence.
	opts = append([]durable.Option{durable.WithUnsignedStepTokens()}, opts...)
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

func TestNewEngine_RestrictsDataDirPerms(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	e, err := durable.NewEngine(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	assertPerm(t, dir, 0o700)
	assertPerm(t, filepath.Join(dir, "tasks"), 0o700)
	assertPerm(t, filepath.Join(dir, ".lock"), 0o600)
}

func TestNewEngine_TightensExistingJournalFiles(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	run := filepath.Join(dir, "tasks", "echo", "run-1")
	if err := os.MkdirAll(run, 0o755); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(run, "journal.log")
	if err := os.WriteFile(journal, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	e, err := durable.NewEngine(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	assertPerm(t, dir, 0o700)
	assertPerm(t, filepath.Join(dir, "tasks", "echo"), 0o700)
	assertPerm(t, run, 0o700)
	assertPerm(t, journal, 0o600)
}

func TestRunTask_PersistedFilesAreOwnerOnly(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	e, err := durable.NewEngine(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	if err := durable.RegisterTask(e, "echo", identityTask()); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "echo", "run-1", "hello")
	if _, err := run.Get(context.Background()); err != nil {
		t.Fatal(err)
	}

	runDir := filepath.Join(dir, "tasks", "echo", "run-1")
	assertPerm(t, runDir, 0o700)
	for _, name := range []string{"journal.log", "input.json", "output.json", "meta.json"} {
		assertPerm(t, filepath.Join(runDir, name), 0o600)
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

	_, err = durable.NewReadOnlyEngine(dir, durable.WithLockTimeout(200*time.Millisecond))
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

	_, err = durable.NewReadOnlyEngine(dir, durable.WithLockTimeout(150*time.Millisecond))
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

func TestReadOnly_WithLogger(t *testing.T) {
	dir := t.TempDir()
	e, err := durable.NewEngine(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := durable.NewReadOnlyEngine(dir,
		durable.WithLogger(slog.Default()),
		durable.WithLockTimeout(2*time.Second),
		durable.WithAutoPurge(time.Hour),
		durable.WithStepTokenKey([]byte("ignored")),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
}

func TestJournalMACKey_ReplayAndRejectTamper(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	macKey := []byte("journal-mac-key")
	e, err := durable.NewEngine(ctx, dir, durable.WithJournalMACKey(macKey))
	if err != nil {
		t.Fatal(err)
	}
	task := durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "echo", in, func(_ context.Context, in string) (string, error) {
			return in, nil
		}).Get(ctx)
	})
	if err := durable.RegisterTask(e, "echo", task); err != nil {
		t.Fatal(err)
	}
	out, err := durable.RunTask[string, string](ctx, e, "echo", "run-1", "hello").Get(ctx)
	if err != nil || out != "hello" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := durable.NewEngine(ctx, dir, durable.WithJournalMACKey(macKey))
	if err != nil {
		t.Fatal(err)
	}
	if err := durable.RegisterTask(e2, "echo", task); err != nil {
		t.Fatal(err)
	}
	out, err = durable.RunTask[string, string](ctx, e2, "echo", "run-1", "ignored").Get(ctx)
	if err != nil || out != "hello" {
		t.Fatalf("replay out=%q err=%v", out, err)
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}

	runDir := filepath.Join(dir, "tasks", "echo", "run-1")
	raw, err := os.ReadFile(filepath.Join(runDir, "journal.log"))
	if err != nil {
		t.Fatal(err)
	}
	// Flip a protobuf byte (not the length prefix). A midpoint XOR can
	// land on a later frame's length and look like a torn tail, which
	// scanJournal would skip instead of fail-closed.
	if len(raw) < 4+1+32 {
		t.Fatal("journal too small for an HMAC frame")
	}
	raw[4] ^= 0xff
	if err := os.WriteFile(filepath.Join(runDir, "journal.log"), raw, 0o600); err != nil {
		t.Fatal(err)
	}

	e3, err := durable.NewEngine(ctx, dir, durable.WithJournalMACKey(macKey))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e3.Close() }()
	if _, err := e3.LoadSteps(ctx, "echo", "run-1"); err == nil || !strings.Contains(err.Error(), "journal MAC mismatch") {
		t.Fatalf("got %v, want journal MAC mismatch", err)
	}
}

func TestJournalMACKey_RejectSidecarTamper(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	macKey := []byte("journal-mac-key")
	e, err := durable.NewEngine(ctx, dir, durable.WithJournalMACKey(macKey))
	if err != nil {
		t.Fatal(err)
	}
	task := durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "echo", in, func(_ context.Context, in string) (string, error) {
			return in, nil
		}).Get(ctx)
	})
	if err := durable.RegisterTask(e, "echo", task); err != nil {
		t.Fatal(err)
	}
	out, err := durable.RunTask[string, string](ctx, e, "echo", "run-1", "hello").Get(ctx)
	if err != nil || out != "hello" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	outPath := filepath.Join(dir, "tasks", "echo", "run-1", "output.json")
	raw, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] ^= 0xff
	if err := os.WriteFile(outPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	e2, err := durable.NewEngine(ctx, dir, durable.WithJournalMACKey(macKey))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e2.Close() }()
	if err := durable.RegisterTask(e2, "echo", task); err != nil {
		t.Fatal(err)
	}
	_, err = durable.RunTask[string, string](ctx, e2, "echo", "run-1", "ignored").Get(ctx)
	if err == nil || !strings.Contains(err.Error(), "file MAC mismatch") {
		t.Fatalf("got %v, want file MAC mismatch", err)
	}
}

func TestRunTask_AfterCloseReturnsErrEngineClosed(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "echo", identityTask()); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := durable.RunTask[string, string](context.Background(), e, "echo", "r", "x").Get(context.Background())
	if !errors.Is(err, durable.ErrEngineClosed) {
		t.Fatalf("got %v, want ErrEngineClosed", err)
	}
}

func TestRunTask_DuplicateInFlightSharesHandle(t *testing.T) {
	e := newTestEngine(t)
	started := make(chan struct{})
	release := make(chan struct{})
	if err := durable.RegisterTask(e, "slow", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "block", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			close(started)
			<-release
			return in, nil
		}).Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}
	first := durable.RunTask[string, string](context.Background(), e, "slow", "shared", "ok")
	<-started
	second := durable.RunTask[string, string](context.Background(), e, "slow", "shared", "ignored")
	if first.RunID() != second.RunID() {
		t.Fatalf("run IDs %s vs %s", first.RunID(), second.RunID())
	}
	close(release)
	out1, err := first.Get(context.Background())
	if err != nil || out1 != "ok" {
		t.Fatalf("first %q %v", out1, err)
	}
	out2, err := second.Get(context.Background())
	if err != nil || out2 != "ok" {
		t.Fatalf("second %q %v", out2, err)
	}
}

func TestNewEngine_DefaultStepTokensAreHMAC(t *testing.T) {
	e, err := durable.NewEngine(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if err := durable.RegisterTask(e, "approve", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "wait", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return "", durable.ErrStepPending
		}).Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "approve", "r1", "")
	waitUntil(t, 2*time.Second, func() bool { return run.Status() == durable.StatusWaiting })
	err = durable.CompleteStep(context.Background(), e, encodeTestToken("approve", "r1", "wait"), "nope")
	if !errors.Is(err, durable.ErrInvalidToken) {
		t.Fatalf("unsigned token accepted by default HMAC engine: %v", err)
	}
}
