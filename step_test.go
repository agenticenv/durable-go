package durable_test

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	durable "github.com/agenticenv/durable-go"
)

func TestRunStep_CacheHitMiss(t *testing.T) {
	e := newTestEngine(t)
	var n atomic.Int32
	if err := durable.RegisterTask(e, "cache", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "work", func(ctx context.Context) (string, error) {
			n.Add(1)
			return "result", nil
		}).Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}
	r1 := durable.RunTask[string, string](context.Background(), e, "cache", "r", "")
	if _, err := r1.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	r2 := durable.RunTask[string, string](context.Background(), e, "cache", "r", "")
	if _, err := r2.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n.Load() != 1 {
		t.Fatalf("fn called %d times", n.Load())
	}
}

func TestRunStep_PanicRecovery(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "panic", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "boom", func(ctx context.Context) (string, error) {
			panic("kaboom")
		}).Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "panic", "", "")
	_, err := run.Get(context.Background())
	if err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("got %v", err)
	}
	info, ok, err := e.GetTask(context.Background(), "panic", run.RunID())
	if err != nil || !ok {
		t.Fatal(err)
	}
	if info.Status != durable.StatusFailed {
		t.Fatalf("status %s", info.Status)
	}
	steps, err := e.LoadSteps(context.Background(), "panic", run.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 1 || steps[0].Status != durable.StepStatusFailed || steps[0].PanicTrace == "" {
		t.Fatalf("steps %+v", steps)
	}
}

func TestRunStep_ConcurrentCallPanic(t *testing.T) {
	e := newTestEngine(t)
	started := make(chan struct{})
	block := make(chan struct{})
	var recovered atomic.Bool
	if err := durable.RegisterTask(e, "conc", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = durable.RunStep(ctx, s, "a", func(ctx context.Context) (string, error) {
				close(started)
				<-block
				return "a", nil
			}).Get(ctx)
		}()
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					if strings.Contains(fmtPanic(r), "concurrent RunStep") {
						recovered.Store(true)
					}
				}
				close(block)
			}()
			<-started
			_, _ = durable.RunStep(ctx, s, "b", func(ctx context.Context) (string, error) {
				return "b", nil
			}).Get(ctx)
		}()
		wg.Wait()
		return "ok", nil
	})); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "conc", "", "")
	if _, err := run.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !recovered.Load() {
		t.Fatal("expected concurrent RunStep panic")
	}
}

func fmtPanic(r any) string {
	if r == nil {
		return ""
	}
	if s, ok := r.(string); ok {
		return s
	}
	if e, ok := r.(error); ok {
		return e.Error()
	}
	return ""
}

func TestRunStep_DuplicateStepIDPanic(t *testing.T) {
	e := newTestEngine(t)
	var recovered atomic.Bool
	if err := durable.RegisterTask(e, "dup", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		if _, err := durable.RunStep(ctx, s, "same", func(ctx context.Context) (string, error) {
			return "1", nil
		}).Get(ctx); err != nil {
			return "", err
		}
		defer func() {
			if r := recover(); r != nil {
				if strings.Contains(fmtPanic(r), "duplicate step ID") {
					recovered.Store(true)
				} else {
					panic(r)
				}
			}
		}()
		_, _ = durable.RunStep(ctx, s, "same", func(ctx context.Context) (string, error) {
			return "2", nil
		}).Get(ctx)
		return "ok", nil
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.RunTask[string, string](context.Background(), e, "dup", "", "").Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !recovered.Load() {
		t.Fatal("expected duplicate stepID panic")
	}
}

func TestRunStep_Timeout(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "to", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "slow", func(ctx context.Context) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		}, durable.WithStepTimeout(20*time.Millisecond)).Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}
	_, err := durable.RunTask[string, string](context.Background(), e, "to", "", "").Get(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
}

func TestRunStep_StepSeq(t *testing.T) {
	e := newTestEngine(t)
	var before, mid, after int
	if err := durable.RegisterTask(e, "seq", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		before = s.StepSeq()
		if _, err := durable.RunStep(ctx, s, "one", func(ctx context.Context) (string, error) {
			return "1", nil
		}).Get(ctx); err != nil {
			return "", err
		}
		mid = s.StepSeq()
		if _, err := durable.RunStep(ctx, s, "two", func(ctx context.Context) (string, error) {
			return "2", nil
		}).Get(ctx); err != nil {
			return "", err
		}
		after = s.StepSeq()
		return "ok", nil
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.RunTask[string, string](context.Background(), e, "seq", "", "").Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	if before != 0 || mid != 1 || after != 2 {
		t.Fatalf("seq before=%d mid=%d after=%d", before, mid, after)
	}
}

func TestRunStep_RetriesOnErrorNotPanic(t *testing.T) {
	e := newTestEngine(t)
	var n atomic.Int32
	if err := durable.RegisterTask(e, "retry", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "flaky", func(ctx context.Context) (string, error) {
			if n.Add(1) < 3 {
				return "", errors.New("transient")
			}
			return "ok", nil
		}, durable.WithStepMaxRetries(2)).Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}
	out, err := durable.RunTask[string, string](context.Background(), e, "retry", "", "").Get(context.Background())
	if err != nil || out != "ok" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	if n.Load() != 3 {
		t.Fatalf("attempts %d", n.Load())
	}
}

func TestRunStep_NoRetryOnPanic(t *testing.T) {
	e := newTestEngine(t)
	var n atomic.Int32
	if err := durable.RegisterTask(e, "nr", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "p", func(ctx context.Context) (string, error) {
			n.Add(1)
			panic("no")
		}, durable.WithStepMaxRetries(5)).Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}
	_, err := durable.RunTask[string, string](context.Background(), e, "nr", "", "").Get(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if n.Load() != 1 {
		t.Fatalf("panic retried: %d", n.Load())
	}
}

func TestStepRunner_Accessors(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "acc", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		if s.TaskID() != "acc" || s.RunID() != "rid" {
			t.Errorf("ids %s %s", s.TaskID(), s.RunID())
		}
		if s.Logger() == nil {
			t.Error("nil logger")
		}
		return "ok", nil
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.RunTask[string, string](context.Background(), e, "acc", "rid", "").Get(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStepToken_PanicsOutsideStep(t *testing.T) {
	e := newTestEngine(t)
	var recovered atomic.Bool
	if err := durable.RegisterTask(e, "tok", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		defer func() {
			if r := recover(); r != nil {
				if strings.Contains(fmtPanic(r), "StepToken must be called") {
					recovered.Store(true)
				} else {
					panic(r)
				}
			}
		}()
		_ = s.StepToken()
		return "ok", nil
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.RunTask[string, string](context.Background(), e, "tok", "", "").Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !recovered.Load() {
		t.Fatal("expected StepToken panic")
	}
}

type approval struct {
	By string `json:"by"`
}

func waitUntil(t *testing.T, timeout time.Duration, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout waiting for condition")
}

func encodeTestToken(taskID, runID, stepID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(taskID + ":" + runID + ":" + stepID))
}

func registerApproval(t *testing.T, e *durable.Engine, calls *atomic.Int32) {
	t.Helper()
	if err := durable.RegisterTask(e, "approve", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (approval, error) {
		return durable.RunStep(ctx, s, "approve", func(ctx context.Context) (approval, error) {
			if calls != nil {
				calls.Add(1)
			}
			_ = s.StepToken()
			return approval{}, durable.ErrStepPending
		}).Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}
}

func TestCompleteStep_ResumesPending(t *testing.T) {
	e := newTestEngine(t)
	registerApproval(t, e, nil)
	run := durable.RunTask[string, approval](context.Background(), e, "approve", "r1", "")
	waitUntil(t, 2*time.Second, func() bool { return run.Status() == durable.StatusWaiting })

	pending, err := e.ListTasks(context.Background(), durable.StatusWaiting)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("waiting count %d", len(pending))
	}

	token := encodeTestToken("approve", "r1", "approve")
	if err := durable.CompleteStep(context.Background(), e, token, approval{By: "manager@co.com"}); err != nil {
		t.Fatal(err)
	}
	out, err := run.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.By != "manager@co.com" {
		t.Fatalf("got %+v", out)
	}
}

func TestCompleteStep_Idempotent(t *testing.T) {
	e := newTestEngine(t)
	registerApproval(t, e, nil)
	run := durable.RunTask[string, approval](context.Background(), e, "approve", "r1", "")
	waitUntil(t, 2*time.Second, func() bool { return run.Status() == durable.StatusWaiting })

	token := encodeTestToken("approve", "r1", "approve")
	if err := durable.CompleteStep(context.Background(), e, token, approval{By: "a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := run.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := durable.CompleteStep(context.Background(), e, token, approval{By: "b"}); !errors.Is(err, durable.ErrRunAlreadyFinished) {
		t.Fatalf("second call after completion: %v", err)
	}
}

func TestCompleteStep_IdempotentWhileWaiting(t *testing.T) {
	e := newTestEngine(t)
	registerApproval(t, e, nil)
	run := durable.RunTask[string, approval](context.Background(), e, "approve", "r1", "")
	waitUntil(t, 2*time.Second, func() bool {
		info, ok, _ := e.GetTask(context.Background(), "approve", "r1")
		return ok && info.Status == durable.StatusWaiting
	})

	token := encodeTestToken("approve", "r1", "approve")
	if err := durable.CompleteStep(context.Background(), e, token, approval{By: "a"}); err != nil {
		t.Fatal(err)
	}
	err := durable.CompleteStep(context.Background(), e, token, approval{By: "b"})
	if err != nil && !errors.Is(err, durable.ErrRunAlreadyFinished) {
		t.Fatalf("idempotent while signal present: %v", err)
	}
	if _, err := run.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCompleteStep_FinishedRun(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "done", identityTask()); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "done", "r1", "x")
	if _, err := run.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	err := durable.CompleteStep(context.Background(), e, encodeTestToken("done", "r1", "echo"), "nope")
	if !errors.Is(err, durable.ErrRunAlreadyFinished) {
		t.Fatalf("got %v", err)
	}
}

func TestCompleteStep_InvalidToken(t *testing.T) {
	e := newTestEngine(t)
	err := durable.CompleteStep(context.Background(), e, "not-a-token", approval{})
	if !errors.Is(err, durable.ErrInvalidToken) {
		t.Fatalf("got %v", err)
	}
}

func TestDeleteTaskRun_WaitingReturnsErrRunActive(t *testing.T) {
	e := newTestEngine(t)
	registerApproval(t, e, nil)
	run := durable.RunTask[string, approval](context.Background(), e, "approve", "r1", "")
	waitUntil(t, 2*time.Second, func() bool { return run.Status() == durable.StatusWaiting })

	err := e.DeleteTaskRun(context.Background(), "approve", "r1")
	if !errors.Is(err, durable.ErrRunActive) {
		t.Fatalf("got %v", err)
	}
}

func TestCompleteStep_CrashRestartReplay(t *testing.T) {
	src := t.TempDir()
	ctx := context.Background()
	var calls atomic.Int32

	e1, err := durable.NewEngine(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	registerApproval(t, e1, &calls)
	run := durable.RunTask[string, approval](ctx, e1, "approve", "r1", "")
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
	registerApproval(t, e2, &calls)

	token := encodeTestToken("approve", "r1", "approve")
	if err := durable.CompleteStep(ctx, e2, token, approval{By: "after-crash"}); err != nil {
		t.Fatal(err)
	}

	r2 := durable.RunTask[string, approval](ctx, e2, "approve", "r1", "")
	out, err := r2.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out.By != "after-crash" {
		t.Fatalf("got %+v", out)
	}
	if calls.Load() != 1 {
		t.Fatalf("pending fn re-executed after crash+signal: %d", calls.Load())
	}
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

func TestStepIDAccessor(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "sid", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		sr := durable.RunStep(ctx, s, "named", func(ctx context.Context) (string, error) {
			return "v", nil
		})
		if sr.StepID() != "named" {
			t.Errorf("StepID %q", sr.StepID())
		}
		return sr.Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.RunTask[string, string](context.Background(), e, "sid", "", "").Get(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCompleteStep_TokenWithColonInStepID(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "c", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "kind:approve", func(ctx context.Context) (string, error) {
			return "", durable.ErrStepPending
		}).Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "c", "r1", "")
	waitUntil(t, 2*time.Second, func() bool { return run.Status() == durable.StatusWaiting })
	if err := durable.CompleteStep(context.Background(), e, encodeTestToken("c", "r1", "kind:approve"), "yes"); err != nil {
		t.Fatal(err)
	}
	out, err := run.Get(context.Background())
	if err != nil || out != "yes" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}
