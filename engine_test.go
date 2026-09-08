package durable_test

import (
	"context"
	"errors"
	"log/slog"
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
