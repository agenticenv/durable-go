package durable_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	durable "github.com/agenticenv/durable-go"
)

// ---- in-memory Store for unit tests (no SQLite dependency) ----

type memStore struct {
	tasks map[string]durable.TaskInfo
	steps map[string]map[string]durable.StepRecord // taskID -> stepID -> record
}

func newMemStore() *memStore {
	return &memStore{
		tasks: make(map[string]durable.TaskInfo),
		steps: make(map[string]map[string]durable.StepRecord),
	}
}

func (m *memStore) SaveTask(_ context.Context, t durable.TaskInfo) error {
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now()
	}
	m.tasks[t.ID] = t
	return nil
}

func (m *memStore) GetTask(_ context.Context, id string) (durable.TaskInfo, bool, error) {
	t, ok := m.tasks[id]
	return t, ok, nil
}

func (m *memStore) ListTasks(_ context.Context) ([]durable.TaskInfo, error) {
	out := make([]durable.TaskInfo, 0, len(m.tasks))
	for _, t := range m.tasks {
		out = append(out, t)
	}
	return out, nil
}

func (m *memStore) DeleteTask(_ context.Context, id string) error {
	delete(m.tasks, id)
	delete(m.steps, id)
	return nil
}

func (m *memStore) PurgeTasks(_ context.Context, status durable.TaskStatus, before time.Time) (int64, error) {
	var n int64
	for id, t := range m.tasks {
		if t.Status == status && t.UpdatedAt.Before(before) {
			delete(m.tasks, id)
			delete(m.steps, id)
			n++
		}
	}
	return n, nil
}

func (m *memStore) SaveStep(_ context.Context, taskID string, step durable.StepRecord) error {
	if m.steps[taskID] == nil {
		m.steps[taskID] = make(map[string]durable.StepRecord)
	}
	m.steps[taskID][step.StepID] = step
	return nil
}

func (m *memStore) LoadStep(_ context.Context, taskID, stepID string) (durable.StepRecord, bool, error) {
	s, ok := m.steps[taskID][stepID]
	return s, ok, nil
}

func (m *memStore) LoadSteps(_ context.Context, taskID string) ([]durable.StepRecord, error) {
	out := make([]durable.StepRecord, 0)
	for _, s := range m.steps[taskID] {
		out = append(out, s)
	}
	return out, nil
}

func (m *memStore) ListStepIDs(_ context.Context, taskID string) ([]string, error) {
	var ids []string
	for id := range m.steps[taskID] {
		ids = append(ids, id)
	}
	return ids, nil
}

// ---- helpers ----

func newClient(t *testing.T, store durable.Store, opts ...durable.Option) *durable.Client {
	t.Helper()
	c, err := durable.NewClient(context.Background(), store, opts...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// ================================================================
// NewClient
// ================================================================

func TestNewClient_NilStoreReturnsError(t *testing.T) {
	_, err := durable.NewClient(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil store, got nil")
	}
}

func TestNewClient_ValidStore(t *testing.T) {
	c, err := durable.NewClient(context.Background(), newMemStore())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = c.Close()
}

func TestClient_CloseIdempotent(t *testing.T) {
	c, _ := durable.NewClient(context.Background(), newMemStore())
	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close should be no-op: %v", err)
	}
}

// ================================================================
// TaskHandle / task options
// ================================================================

func TestNewTask_ID(t *testing.T) {
	c := newClient(t, newMemStore())
	h := c.NewTask("my-task")
	if h.ID() != "my-task" {
		t.Fatalf("got ID %q, want %q", h.ID(), "my-task")
	}
}

func TestNewTask_WithName(t *testing.T) {
	store := newMemStore()
	c := newClient(t, store)
	h := c.NewTask("t1", durable.WithName("My Task"))

	ctx := context.Background()
	_, _ = durable.Run(ctx, h, struct{}{}, durable.Func(func(ctx context.Context, s *durable.StepRunner, _ struct{}) (struct{}, error) {
		return struct{}{}, nil
	}))

	info, ok, _ := store.GetTask(ctx, "t1")
	if !ok {
		t.Fatal("task not found in store")
	}
	if info.Name != "My Task" {
		t.Fatalf("got name %q, want %q", info.Name, "My Task")
	}
}

func TestNewTask_WithTag(t *testing.T) {
	store := newMemStore()
	c := newClient(t, store)
	h := c.NewTask("t2", durable.WithTag("env", "prod"), durable.WithTag("team", "ops"))

	ctx := context.Background()
	_, _ = durable.Run(ctx, h, struct{}{}, durable.Func(func(ctx context.Context, s *durable.StepRunner, _ struct{}) (struct{}, error) {
		return struct{}{}, nil
	}))

	info, _, _ := store.GetTask(ctx, "t2")
	if info.Tags["env"] != "prod" || info.Tags["team"] != "ops" {
		t.Fatalf("unexpected tags: %v", info.Tags)
	}
}

// ================================================================
// Run — happy path
// ================================================================

func TestRun_HappyPath(t *testing.T) {
	store := newMemStore()
	c := newClient(t, store)
	ctx := context.Background()
	h := c.NewTask("run-happy")

	out, err := durable.Run(ctx, h, 42, durable.Func(func(ctx context.Context, s *durable.StepRunner, in int) (int, error) {
		return in * 2, nil
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != 84 {
		t.Fatalf("got %d, want 84", out)
	}
}

func TestRun_PersistsCompletedStatus(t *testing.T) {
	store := newMemStore()
	c := newClient(t, store)
	ctx := context.Background()
	h := c.NewTask("run-status")

	_, _ = durable.Run(ctx, h, struct{}{}, durable.Func(func(ctx context.Context, s *durable.StepRunner, _ struct{}) (struct{}, error) {
		return struct{}{}, nil
	}))

	info, ok, _ := store.GetTask(ctx, "run-status")
	if !ok {
		t.Fatal("task not in store")
	}
	if info.Status != durable.StatusCompleted {
		t.Fatalf("got status %q, want completed", info.Status)
	}
}

// ================================================================
// Run — failure / panic
// ================================================================

func TestRun_TaskError_PersistsFailedStatus(t *testing.T) {
	store := newMemStore()
	c := newClient(t, store)
	ctx := context.Background()
	h := c.NewTask("run-fail")

	_, err := durable.Run(ctx, h, struct{}{}, durable.Func(func(ctx context.Context, s *durable.StepRunner, _ struct{}) (struct{}, error) {
		return struct{}{}, errors.New("boom")
	}))
	if err == nil {
		t.Fatal("expected error")
	}

	info, _, _ := store.GetTask(ctx, "run-fail")
	if info.Status != durable.StatusFailed {
		t.Fatalf("got status %q, want failed", info.Status)
	}
	if info.Error == "" {
		t.Fatal("expected Error field to be set")
	}
}

func TestRun_TaskPanic_RecoveredAndPersisted(t *testing.T) {
	store := newMemStore()
	c := newClient(t, store)
	ctx := context.Background()
	h := c.NewTask("run-panic")

	_, err := durable.Run(ctx, h, struct{}{}, durable.Func(func(ctx context.Context, s *durable.StepRunner, _ struct{}) (struct{}, error) {
		panic("something went wrong")
	}))
	if err == nil {
		t.Fatal("expected error from panic")
	}
	if !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("error should mention panic, got: %v", err)
	}

	info, _, _ := store.GetTask(ctx, "run-panic")
	if info.Status != durable.StatusFailed {
		t.Fatalf("got status %q, want failed", info.Status)
	}
	if info.PanicTrace == "" {
		t.Fatal("PanicTrace should be populated after a panic")
	}
}

// ================================================================
// Run — timeout
// ================================================================

func TestRun_WithTimeout_CancelsTask(t *testing.T) {
	store := newMemStore()
	c := newClient(t, store)
	ctx := context.Background()
	h := c.NewTask("run-timeout", durable.WithTimeout(20*time.Millisecond))

	_, err := durable.Run(ctx, h, struct{}{}, durable.Func(func(ctx context.Context, s *durable.StepRunner, _ struct{}) (struct{}, error) {
		select {
		case <-ctx.Done():
			return struct{}{}, ctx.Err()
		case <-time.After(5 * time.Second):
			return struct{}{}, nil
		}
	}))
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

// ================================================================
// Run — retries
// ================================================================

func TestRun_WithMaxRetries_RetriesOnFailure(t *testing.T) {
	store := newMemStore()
	c := newClient(t, store)
	ctx := context.Background()
	h := c.NewTask("run-retry", durable.WithMaxRetries(2))

	var calls int32
	_, err := durable.Run(ctx, h, struct{}{}, durable.Func(func(ctx context.Context, s *durable.StepRunner, _ struct{}) (struct{}, error) {
		atomic.AddInt32(&calls, 1)
		return struct{}{}, errors.New("transient")
	}))
	if err == nil {
		t.Fatal("expected error after retries")
	}
	// initial attempt + 2 retries = 3 total
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRun_WithMaxRetries_SucceedsOnSecondAttempt(t *testing.T) {
	store := newMemStore()
	c := newClient(t, store)
	ctx := context.Background()
	h := c.NewTask("run-retry-ok", durable.WithMaxRetries(2))

	var calls int32
	out, err := durable.Run(ctx, h, struct{}{}, durable.Func(func(ctx context.Context, s *durable.StepRunner, _ struct{}) (string, error) {
		n := atomic.AddInt32(&calls, 1)
		if n < 2 {
			return "", errors.New("not yet")
		}
		return "ok", nil
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "ok" {
		t.Fatalf("got %q, want %q", out, "ok")
	}
}

// ================================================================
// Step — happy path
// ================================================================

func TestStep_ExecutesFnAndReturnsResult(t *testing.T) {
	store := newMemStore()
	c := newClient(t, store)
	ctx := context.Background()
	h := c.NewTask("step-basic")

	out, err := durable.Run(ctx, h, struct{}{}, durable.Func(func(ctx context.Context, s *durable.StepRunner, _ struct{}) (string, error) {
		return durable.Step(ctx, s, "greet", func(ctx context.Context) (string, error) {
			return "hello", nil
		})
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "hello" {
		t.Fatalf("got %q, want %q", out, "hello")
	}
}

func TestStep_PersistsCompletedRecord(t *testing.T) {
	store := newMemStore()
	c := newClient(t, store)
	ctx := context.Background()
	h := c.NewTask("step-persist")

	_, _ = durable.Run(ctx, h, struct{}{}, durable.Func(func(ctx context.Context, s *durable.StepRunner, _ struct{}) (struct{}, error) {
		_, err := durable.Step(ctx, s, "do-work", func(ctx context.Context) (int, error) {
			return 99, nil
		})
		return struct{}{}, err
	}))

	rec, ok, _ := store.LoadStep(ctx, "step-persist", "do-work")
	if !ok {
		t.Fatal("step record not found")
	}
	if rec.Status != durable.StepStatusCompleted {
		t.Fatalf("got step status %q, want completed", rec.Status)
	}
}

// ================================================================
// Step — replay (cache hit)
// ================================================================

func TestStep_ReplayDoesNotReExecuteFn(t *testing.T) {
	store := newMemStore()
	c := newClient(t, store)
	ctx := context.Background()

	var callCount int32

	runTask := func(taskID string) (string, error) {
		h := c.NewTask(taskID)
		return durable.Run(ctx, h, struct{}{}, durable.Func(func(ctx context.Context, s *durable.StepRunner, _ struct{}) (string, error) {
			return durable.Step(ctx, s, "expensive", func(ctx context.Context) (string, error) {
				atomic.AddInt32(&callCount, 1)
				return "result", nil
			})
		}))
	}

	// First run — fn executes.
	out1, err := runTask("replay-task")
	if err != nil || out1 != "result" {
		t.Fatalf("run1: err=%v out=%q", err, out1)
	}

	// Second run with same taskID — fn must NOT be called again.
	out2, err := runTask("replay-task")
	if err != nil || out2 != "result" {
		t.Fatalf("run2: err=%v out=%q", err, out2)
	}

	if callCount != 1 {
		t.Fatalf("step fn called %d times, expected exactly 1 (replay on run 2)", callCount)
	}
}

// ================================================================
// Step — error and panic
// ================================================================

func TestStep_FnError_PersistsFailedRecord(t *testing.T) {
	store := newMemStore()
	c := newClient(t, store)
	ctx := context.Background()
	h := c.NewTask("step-err")

	_, err := durable.Run(ctx, h, struct{}{}, durable.Func(func(ctx context.Context, s *durable.StepRunner, _ struct{}) (struct{}, error) {
		return struct{}{}, func() error {
			_, e := durable.Step(ctx, s, "bad-step", func(ctx context.Context) (struct{}, error) {
				return struct{}{}, errors.New("step failed")
			})
			return e
		}()
	}))
	if err == nil {
		t.Fatal("expected error")
	}

	rec, ok, _ := store.LoadStep(ctx, "step-err", "bad-step")
	if !ok {
		t.Fatal("step record not found")
	}
	if rec.Status != durable.StepStatusFailed {
		t.Fatalf("got %q, want failed", rec.Status)
	}
	if rec.Error == "" {
		t.Fatal("step Error field should be set")
	}
}

func TestStep_FnPanic_RecoveredAndPersisted(t *testing.T) {
	store := newMemStore()
	c := newClient(t, store)
	ctx := context.Background()
	h := c.NewTask("step-panic")

	_, err := durable.Run(ctx, h, struct{}{}, durable.Func(func(ctx context.Context, s *durable.StepRunner, _ struct{}) (struct{}, error) {
		return struct{}{}, func() error {
			_, e := durable.Step(ctx, s, "panic-step", func(ctx context.Context) (struct{}, error) {
				panic("step boom")
			})
			return e
		}()
	}))
	if err == nil {
		t.Fatal("expected error from step panic")
	}

	rec, _, _ := store.LoadStep(ctx, "step-panic", "panic-step")
	if rec.Status != durable.StepStatusFailed {
		t.Fatalf("got %q, want failed", rec.Status)
	}
	if rec.PanicTrace == "" {
		t.Fatal("step PanicTrace should be set")
	}
}

// ================================================================
// Step — chaining (output of step 1 flows into step 2)
// ================================================================

func TestStep_Chaining(t *testing.T) {
	store := newMemStore()
	c := newClient(t, store)
	ctx := context.Background()
	h := c.NewTask("step-chain")

	out, err := durable.Run(ctx, h, 3, durable.Func(func(ctx context.Context, s *durable.StepRunner, n int) (int, error) {
		doubled, err := durable.Step(ctx, s, "double", func(ctx context.Context) (int, error) {
			return n * 2, nil
		})
		if err != nil {
			return 0, err
		}
		return durable.Step(ctx, s, "add-ten", func(ctx context.Context) (int, error) {
			return doubled + 10, nil
		})
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != 16 { // 3*2+10
		t.Fatalf("got %d, want 16", out)
	}
}

// ================================================================
// Step — seq counter increments
// ================================================================

func TestStep_SeqIncrements(t *testing.T) {
	store := newMemStore()
	c := newClient(t, store)
	ctx := context.Background()
	h := c.NewTask("step-seq")

	_, _ = durable.Run(ctx, h, struct{}{}, durable.Func(func(ctx context.Context, s *durable.StepRunner, _ struct{}) (struct{}, error) {
		for i := range 3 {
			_, _ = durable.Step(ctx, s, fmt.Sprintf("step-%d", i), func(ctx context.Context) (struct{}, error) {
				return struct{}{}, nil
			})
		}
		return struct{}{}, nil
	}))

	for i := range 3 {
		rec, ok, _ := store.LoadStep(ctx, "step-seq", fmt.Sprintf("step-%d", i))
		if !ok {
			t.Fatalf("step-%d not found", i)
		}
		if rec.Seq != i+1 {
			t.Fatalf("step-%d: got seq %d, want %d", i, rec.Seq, i+1)
		}
	}
}

// ================================================================
// Func helper
// ================================================================

func TestFunc_AdaptsClosureToTask(t *testing.T) {
	task := durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return in + "!", nil
	})
	out, err := task.Exec(context.Background(), nil, "hello")
	if err != nil || out != "hello!" {
		t.Fatalf("got %q, %v", out, err)
	}
}

// ================================================================
// WithAutoPurge option (smoke test — verifies no panic on construction)
// ================================================================

func TestWithAutoPurge_DoesNotPanic(t *testing.T) {
	c, err := durable.NewClient(context.Background(), newMemStore(),
		durable.WithAutoPurge(time.Hour, 10*time.Minute),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = c.Close()
}
