package durable_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
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
		return durable.RunStep(ctx, s, "work", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
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
		return durable.RunStep(ctx, s, "boom", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
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

// TestRunStep_Fanout starts two independent steps before Get-ing either,
// proving concurrent RunStep calls on the same StepRunner no longer panic
// and both genuinely run in parallel (the fast step finishes while the
// slow one is still sleeping).
func TestRunStep_Fanout(t *testing.T) {
	e := newTestEngine(t)
	var calls atomic.Int32
	if err := durable.RegisterTask(e, "fanout", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		x := durable.RunStep(ctx, s, "slow", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			calls.Add(1)
			time.Sleep(50 * time.Millisecond)
			return "A", nil
		})
		y := durable.RunStep(ctx, s, "fast", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			calls.Add(1)
			return "B", nil
		})
		// Get the fast one first — it must not block on the slow one.
		rb, errb := y.Get(ctx)
		if errb != nil {
			return "", errb
		}
		ra, erra := x.Get(ctx)
		if erra != nil {
			return "", erra
		}
		return ra + rb, nil
	})); err != nil {
		t.Fatal(err)
	}

	run := durable.RunTask[string, string](context.Background(), e, "fanout", "", "")
	out, err := run.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out != "AB" {
		t.Fatalf("got %q", out)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d", calls.Load())
	}
}

// TestRunStep_UserGoroutineFanoutNoPanic covers calling RunStep from two
// explicit user goroutines (not just sequential calls before Get) — the
// library does not police how the caller fans out.
func TestRunStep_UserGoroutineFanoutNoPanic(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "usergo", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		var a, b *durable.StepRun[string]
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			a = durable.RunStep(ctx, s, "a", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) { return "A", nil })
		}()
		go func() {
			defer wg.Done()
			b = durable.RunStep(ctx, s, "b", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) { return "B", nil })
		}()
		wg.Wait()
		ra, erra := a.Get(ctx)
		if erra != nil {
			return "", erra
		}
		rb, errb := b.Get(ctx)
		if errb != nil {
			return "", errb
		}
		return ra + rb, nil
	})); err != nil {
		t.Fatal(err)
	}

	out, err := durable.RunTask[string, string](context.Background(), e, "usergo", "", "").Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out != "AB" {
		t.Fatalf("got %q", out)
	}
}

// TestStepRun_DoneSelectFirstOfN covers the first-of-N pattern: select on
// Done across two handles and react to whichever finishes first.
func TestStepRun_DoneSelectFirstOfN(t *testing.T) {
	e := newTestEngine(t)
	var firstWinner atomic.Value
	if err := durable.RegisterTask(e, "race", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		x := durable.RunStep(ctx, s, "slow", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			time.Sleep(150 * time.Millisecond)
			return "slow", nil
		})
		y := durable.RunStep(ctx, s, "fast", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return "fast", nil
		})
		select {
		case <-x.Done():
			firstWinner.Store("slow")
		case <-y.Done():
			firstWinner.Store("fast")
		}
		// Drain both so the run completes cleanly.
		if _, err := x.Get(ctx); err != nil {
			return "", err
		}
		if _, err := y.Get(ctx); err != nil {
			return "", err
		}
		return "ok", nil
	})); err != nil {
		t.Fatal(err)
	}

	if _, err := durable.RunTask[string, string](context.Background(), e, "race", "", "").Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := firstWinner.Load(); got != "fast" {
		t.Fatalf("expected fast step to win the select, got %v", got)
	}
}

func TestRunStep_DuplicateStepIDPanic(t *testing.T) {
	e := newTestEngine(t)
	var recovered atomic.Bool
	if err := durable.RegisterTask(e, "dup", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		if _, err := durable.RunStep(ctx, s, "same", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
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
		_, _ = durable.RunStep(ctx, s, "same", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
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

func TestRunStep_Timeout(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "to", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "slow", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
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

func TestRunStep_RetriesOnErrorNotPanic(t *testing.T) {
	e := newTestEngine(t)
	var n atomic.Int32
	if err := durable.RegisterTask(e, "retry", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "flaky", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
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
		return durable.RunStep(ctx, s, "p", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
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

// TestRunStep_FailedReplayDoesNotRerunFn crashes a run with a step already
// persisted FAILED (but the run itself still non-terminal, suspended on a
// sibling pending step), reopens the engine, and verifies the failed step's
// fn is not invoked again — only the stored error is replayed.
func TestRunStep_FailedReplayDoesNotRerunFn(t *testing.T) {
	src := t.TempDir()
	ctx := context.Background()
	var flakyCalls atomic.Int32

	newTask := func() durable.TaskFunc[string, string] {
		return durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
			_, ferr := durable.RunStep(ctx, s, "flaky", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
				flakyCalls.Add(1)
				return "", errors.New("boom")
			}).Get(ctx)
			errMsg := ""
			if ferr != nil {
				errMsg = ferr.Error()
			}
			_, werr := durable.RunStep(ctx, s, "wait", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
				return "", durable.ErrStepPending
			}).Get(ctx)
			if werr != nil {
				return "", werr
			}
			return errMsg, nil
		})
	}

	e1, err := durable.NewEngine(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	if err := durable.RegisterTask(e1, "flakytest", newTask()); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](ctx, e1, "flakytest", "r1", "")
	waitUntil(t, 2*time.Second, func() bool { return run.Status() == durable.StatusWaiting })
	if flakyCalls.Load() != 1 {
		t.Fatalf("flaky calls before crash: %d", flakyCalls.Load())
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
	if err := durable.RegisterTask(e2, "flakytest", newTask()); err != nil {
		t.Fatal(err)
	}

	run2 := durable.RunTask[string, string](ctx, e2, "flakytest", "r1", "")
	waitUntil(t, 2*time.Second, func() bool { return run2.Status() == durable.StatusWaiting })
	if flakyCalls.Load() != 1 {
		t.Fatalf("flaky step re-ran fn on resume: calls=%d", flakyCalls.Load())
	}

	token := encodeTestToken("flakytest", "r1", "wait")
	if err := durable.CompleteStep(ctx, e2, token, "done"); err != nil {
		t.Fatal(err)
	}
	out, err := run2.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out != "boom" {
		t.Fatalf("expected replayed error message %q, got %q", "boom", out)
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
		_ = s.StepToken(ctx)
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

// TestStepToken_TwoConcurrentStepsEachGetOwnToken proves StepToken reads
// the stepID off ctx (not off shared StepRunner state) so two concurrent
// steps do not clobber each other's token.
func TestStepToken_TwoConcurrentStepsEachGetOwnToken(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "twotok", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		x := durable.RunStep(ctx, s, "a", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return s.StepToken(ctx), durable.ErrStepPending
		})
		y := durable.RunStep(ctx, s, "b", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return s.StepToken(ctx), durable.ErrStepPending
		})
		_, _ = x.Get(ctx)
		_, _ = y.Get(ctx)
		return "unreachable", nil
	})); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "twotok", "r1", "")
	waitUntil(t, 2*time.Second, func() bool {
		ra, oka, _ := e.GetStep(context.Background(), "twotok", "r1", "a")
		rb, okb, _ := e.GetStep(context.Background(), "twotok", "r1", "b")
		return oka && ra.Status == durable.StepStatusWaiting && okb && rb.Status == durable.StepStatusWaiting
	})
	tokenA := encodeTestToken("twotok", "r1", "a")
	tokenB := encodeTestToken("twotok", "r1", "b")
	if err := durable.CompleteStep(context.Background(), e, tokenA, "resumed-a"); err != nil {
		t.Fatal(err)
	}
	if err := durable.CompleteStep(context.Background(), e, tokenB, "resumed-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := run.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestRunStep_DualPendingBothComplete covers two steps suspended at once:
// completing one must not clear StatusWaiting while the sibling is still
// pending, and completing both resolves the run.
func TestRunStep_DualPendingBothComplete(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "dual", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		x := durable.RunStep(ctx, s, "a", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return "", durable.ErrStepPending
		})
		y := durable.RunStep(ctx, s, "b", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return "", durable.ErrStepPending
		})
		ra, erra := x.Get(ctx)
		if erra != nil {
			return "", erra
		}
		rb, errb := y.Get(ctx)
		if errb != nil {
			return "", errb
		}
		return ra + rb, nil
	})); err != nil {
		t.Fatal(err)
	}

	run := durable.RunTask[string, string](context.Background(), e, "dual", "r1", "")
	waitUntil(t, 2*time.Second, func() bool {
		ra, oka, _ := e.GetStep(context.Background(), "dual", "r1", "a")
		rb, okb, _ := e.GetStep(context.Background(), "dual", "r1", "b")
		return oka && ra.Status == durable.StepStatusWaiting && okb && rb.Status == durable.StepStatusWaiting
	})

	tokenA := encodeTestToken("dual", "r1", "a")
	if err := durable.CompleteStep(context.Background(), e, tokenA, "A"); err != nil {
		t.Fatal(err)
	}

	// b is still pending — the run must remain Waiting.
	time.Sleep(50 * time.Millisecond)
	if run.Status() != durable.StatusWaiting {
		t.Fatalf("run left Waiting while sibling still pending: %s", run.Status())
	}

	tokenB := encodeTestToken("dual", "r1", "b")
	if err := durable.CompleteStep(context.Background(), e, tokenB, "B"); err != nil {
		t.Fatal(err)
	}

	out, err := run.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out != "AB" {
		t.Fatalf("got %q", out)
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
		return durable.RunStep(ctx, s, "approve", struct{}{}, func(ctx context.Context, _ struct{}) (approval, error) {
			if calls != nil {
				calls.Add(1)
			}
			_ = s.StepToken(ctx)
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

// TestCompleteStep_ConcurrentSameTokenNoDoubleDeliver fires several
// concurrent CompleteStep calls for the same token — exercises the
// signalLocks race fix. Run with -race.
func TestCompleteStep_ConcurrentSameTokenNoDoubleDeliver(t *testing.T) {
	e := newTestEngine(t)
	registerApproval(t, e, nil)
	run := durable.RunTask[string, approval](context.Background(), e, "approve", "r1", "")
	waitUntil(t, 2*time.Second, func() bool { return run.Status() == durable.StatusWaiting })

	token := encodeTestToken("approve", "r1", "approve")
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = durable.CompleteStep(context.Background(), e, token, approval{By: fmt.Sprintf("caller-%d", i)})
		}(i)
	}
	wg.Wait()

	out, err := run.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.By == "" {
		t.Fatal("expected a delivered result")
	}
}

// TestRunStep_ConcurrentCompleteStepNoRace fans out N pending steps and
// completes all of them concurrently from separate goroutines — exercises
// both the setRunStatus race fix (concurrent leaveWaiting) and appendSignal
// under concurrent CompleteStep calls for distinct stepIDs. Run with -race.
func TestRunStep_ConcurrentCompleteStepNoRace(t *testing.T) {
	e := newTestEngine(t)
	const n = 8
	if err := durable.RegisterTask(e, "raceN", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		runs := make([]*durable.StepRun[string], n)
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("s%d", i)
			runs[i] = durable.RunStep(ctx, s, id, struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
				return "", durable.ErrStepPending
			})
		}
		for i := 0; i < n; i++ {
			if _, err := runs[i].Get(ctx); err != nil {
				return "", err
			}
		}
		return "ok", nil
	})); err != nil {
		t.Fatal(err)
	}

	run := durable.RunTask[string, string](context.Background(), e, "raceN", "r1", "")
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("s%d", i)
		waitUntil(t, 2*time.Second, func() bool {
			rec, ok, _ := e.GetStep(context.Background(), "raceN", "r1", id)
			return ok && rec.Status == durable.StepStatusWaiting
		})
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			token := encodeTestToken("raceN", "r1", fmt.Sprintf("s%d", i))
			_ = durable.CompleteStep(context.Background(), e, token, "done")
		}(i)
	}
	wg.Wait()

	out, err := run.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out != "ok" {
		t.Fatalf("got %q", out)
	}
	info, ok, err := e.GetTask(context.Background(), "raceN", "r1")
	if err != nil || !ok {
		t.Fatal(err)
	}
	if info.Status != durable.StatusCompleted {
		t.Fatalf("final status %s", info.Status)
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
		sr := durable.RunStep(ctx, s, "named", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
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

// TestRunStep_ReservedCancelIDPanics locks in that the sentinel signal ID
// CancelRun uses can never be a real stepID, so it can never collide with a
// legitimate CompleteStep-addressed step.
func TestRunStep_ReservedCancelIDPanics(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "reserved", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for reserved step ID")
			}
		}()
		return durable.RunStep(ctx, s, "\x00cancel", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return "", nil
		}).Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "reserved", "", "")
	_, _ = run.Get(context.Background())
}

func TestCompleteStep_TokenWithColonInStepID(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "c", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "kind:approve", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
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

func TestCompleteStep_HMACToken(t *testing.T) {
	e := newTestEngine(t, durable.WithStepTokenKey([]byte("secret")))
	var token string
	if err := durable.RegisterTask(e, "approve", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "wait", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			token = s.StepToken(ctx)
			return "", durable.ErrStepPending
		}).Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "approve", "r1", "")
	waitUntil(t, 2*time.Second, func() bool { return run.Status() == durable.StatusWaiting && token != "" })
	if !strings.HasPrefix(token, "v1.") {
		t.Fatalf("expected HMAC token, got %s", token)
	}
	if err := durable.CompleteStep(context.Background(), e, encodeTestToken("approve", "r1", "wait"), "nope"); !errors.Is(err, durable.ErrInvalidToken) {
		t.Fatalf("unsigned token: %v", err)
	}
	if err := durable.CompleteStep(context.Background(), e, token, "yes"); err != nil {
		t.Fatal(err)
	}
	out, err := run.Get(context.Background())
	if err != nil || out != "yes" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestCompleteStep_StepTokenTTLOverride(t *testing.T) {
	e := newTestEngine(t, durable.WithStepTokenKey([]byte("secret")), durable.WithDefaultStepTokenTTL(time.Hour))
	var token string
	if err := durable.RegisterTask(e, "approve", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "wait", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			token = s.StepToken(ctx)
			return "", durable.ErrStepPending
		}, durable.WithStepTokenTTL(7*24*time.Hour)).Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "approve", "r1", "")
	waitUntil(t, 2*time.Second, func() bool { return token != "" })
	if err := durable.CompleteStep(context.Background(), e, token, "yes"); err != nil {
		t.Fatal(err)
	}
	out, err := run.Get(context.Background())
	if err != nil || out != "yes" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func runUntilWaiting(t *testing.T, dir, ver string, n *atomic.Int32) *durable.Engine {
	t.Helper()
	e, err := durable.NewEngine(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := durable.RegisterTask(e, "v", versionedWaitTask(ver, n)); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "v", "r1", "")
	waitUntil(t, 2*time.Second, func() bool { return run.Status() == durable.StatusWaiting })
	return e
}

func versionedWaitTask(ver string, n *atomic.Int32) durable.TaskFunc[string, string] {
	return durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		var opts []durable.StepOption
		if ver != "" {
			opts = append(opts, durable.WithStepVersion(ver))
		}
		out, err := durable.RunStep(ctx, s, "work", "payload", func(ctx context.Context, in string) (string, error) {
			n.Add(1)
			return in + "-ok", nil
		}, opts...).Get(ctx)
		if err != nil {
			return "", err
		}
		_, err = durable.RunStep(ctx, s, "wait", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return "", durable.ErrStepPending
		}).Get(ctx)
		return out, err
	})
}

func TestRunStep_StoresInputAndVersion(t *testing.T) {
	dir := t.TempDir()
	var n atomic.Int32
	e, err := durable.NewEngine(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	if err := durable.RegisterTask(e, "v", versionedWaitTask("1", &n)); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "v", "r1", "")
	waitUntil(t, 2*time.Second, func() bool { return run.Status() == durable.StatusWaiting })
	rec, ok, err := e.GetStep(context.Background(), "v", "r1", "work")
	if err != nil || !ok {
		t.Fatalf("GetStep: ok=%v err=%v", ok, err)
	}
	if rec.Version != "1" {
		t.Fatalf("version %q", rec.Version)
	}
	if string(rec.Input) != `"payload"` {
		t.Fatalf("input %s", rec.Input)
	}
	if n.Load() != 1 {
		t.Fatalf("calls %d", n.Load())
	}
}

func TestRunStep_VersionSameCaches(t *testing.T) {
	src := t.TempDir()
	var n atomic.Int32
	e1 := runUntilWaiting(t, src, "1", &n)
	dst := t.TempDir()
	if err := copyDir(src, dst); err != nil {
		t.Fatal(err)
	}
	_ = e1.Close()
	e2 := runUntilWaiting(t, dst, "1", &n)
	_ = e2.Close()
	if n.Load() != 1 {
		t.Fatalf("fn called %d, want 1", n.Load())
	}
}

func TestRunStep_VersionBumpReruns(t *testing.T) {
	src := t.TempDir()
	var n atomic.Int32
	e1 := runUntilWaiting(t, src, "1", &n)
	dst := t.TempDir()
	if err := copyDir(src, dst); err != nil {
		t.Fatal(err)
	}
	_ = e1.Close()
	e2 := runUntilWaiting(t, dst, "2", &n)
	_ = e2.Close()
	if n.Load() != 2 {
		t.Fatalf("fn called %d, want 2", n.Load())
	}
}

func TestRunStep_EmptyVersionThenSetReruns(t *testing.T) {
	src := t.TempDir()
	var n atomic.Int32
	e1 := runUntilWaiting(t, src, "", &n)
	dst := t.TempDir()
	if err := copyDir(src, dst); err != nil {
		t.Fatal(err)
	}
	_ = e1.Close()
	e2 := runUntilWaiting(t, dst, "1", &n)
	_ = e2.Close()
	if n.Load() != 2 {
		t.Fatalf("fn called %d, want 2", n.Load())
	}
}

func TestRunStep_NoVersionCachesByStepID(t *testing.T) {
	src := t.TempDir()
	var n atomic.Int32
	e1 := runUntilWaiting(t, src, "", &n)
	dst := t.TempDir()
	if err := copyDir(src, dst); err != nil {
		t.Fatal(err)
	}
	_ = e1.Close()
	e2 := runUntilWaiting(t, dst, "", &n)
	_ = e2.Close()
	if n.Load() != 1 {
		t.Fatalf("fn called %d, want 1", n.Load())
	}
}

func TestRunStep_FailedStoresVersion(t *testing.T) {
	e := newTestEngine(t)
	if err := durable.RegisterTask(e, "failv", durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "boom", "x", func(ctx context.Context, in string) (string, error) {
			return "", errors.New("nope")
		}, durable.WithStepVersion("1")).Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](context.Background(), e, "failv", "r1", "")
	if _, err := run.Get(context.Background()); err == nil {
		t.Fatal("expected error")
	}
	rec, ok, err := e.GetStep(context.Background(), "failv", "r1", "boom")
	if err != nil || !ok {
		t.Fatal(err)
	}
	if rec.Status != durable.StepStatusFailed || rec.Version != "1" || string(rec.Input) != `"x"` {
		t.Fatalf("%+v", rec)
	}
}
