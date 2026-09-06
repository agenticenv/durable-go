package sqlite_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	durable "github.com/agenticenv/durable-go"
	"github.com/agenticenv/durable-go/store/sqlite"
)

// newTestStore creates a SQLiteStore backed by a temporary file and registers
// cleanup to close the store and remove the file after the test completes.
func newTestStore(t *testing.T) *sqlite.SQLiteStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	store, err := sqlite.NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// ================================================================
// NewSQLiteStore
// ================================================================

func TestNewSQLiteStore_CreatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "new.db")
	store, err := sqlite.NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = store.Close() }()

	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("expected db file to be created on disk")
	}
}

func TestNewSQLiteStore_IdempotentSchema(t *testing.T) {
	// Opening the same file twice should not fail (CREATE TABLE IF NOT EXISTS).
	path := filepath.Join(t.TempDir(), "idempotent.db")
	s1, err := sqlite.NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	_ = s1.Close()

	s2, err := sqlite.NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	_ = s2.Close()
}

// ================================================================
// SaveTask / GetTask
// ================================================================

func TestSaveTask_GetTask_RoundTrip(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	now := time.Now().UTC().Truncate(time.Second)
	task := durable.TaskInfo{
		ID:        "task-1",
		Name:      "My Task",
		Tags:      map[string]string{"env": "test"},
		Status:    durable.StatusRunning,
		CreatedAt: now,
		StartedAt: now,
		UpdatedAt: now,
	}

	if err := store.SaveTask(ctx, task); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}

	got, ok, err := store.GetTask(ctx, "task-1")
	if err != nil || !ok {
		t.Fatalf("GetTask: ok=%v err=%v", ok, err)
	}
	if got.ID != "task-1" || got.Name != "My Task" {
		t.Fatalf("unexpected task: %+v", got)
	}
	if got.Tags["env"] != "test" {
		t.Fatalf("tags not round-tripped: %v", got.Tags)
	}
	if got.Status != durable.StatusRunning {
		t.Fatalf("got status %q, want running", got.Status)
	}
}

func TestGetTask_NotFound(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	_, ok, err := store.GetTask(ctx, "missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for missing task")
	}
}

func TestSaveTask_Upsert(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	now := time.Now().UTC().Truncate(time.Second)
	task := durable.TaskInfo{ID: "upsert-task", Status: durable.StatusRunning, CreatedAt: now, UpdatedAt: now}
	_ = store.SaveTask(ctx, task)

	// Update status to completed.
	task.Status = durable.StatusCompleted
	task.CompletedAt = now.Add(time.Second)
	if err := store.SaveTask(ctx, task); err != nil {
		t.Fatalf("upsert SaveTask: %v", err)
	}

	got, _, _ := store.GetTask(ctx, "upsert-task")
	if got.Status != durable.StatusCompleted {
		t.Fatalf("got %q, want completed", got.Status)
	}
}

func TestSaveTask_PanicTrace(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	now := time.Now().UTC().Truncate(time.Second)
	task := durable.TaskInfo{
		ID:         "panic-task",
		Status:     durable.StatusFailed,
		PanicTrace: "goroutine 1 [running]:\nmain.main()",
		Error:      "panicked",
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	_ = store.SaveTask(ctx, task)

	got, _, _ := store.GetTask(ctx, "panic-task")
	if got.PanicTrace == "" {
		t.Fatal("PanicTrace should be persisted")
	}
}

// ================================================================
// ListTasks
// ================================================================

func TestListTasks_Empty(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	tasks, err := store.ListTasks(ctx)
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("expected 0 tasks, got %d", len(tasks))
	}
}

func TestListTasks_ReturnsSavedTasks(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	now := time.Now().UTC().Truncate(time.Second)
	for i := range 3 {
		_ = store.SaveTask(ctx, durable.TaskInfo{
			ID:        fmt.Sprintf("lt-%d", i),
			Status:    durable.StatusCompleted,
			CreatedAt: now.Add(time.Duration(i) * time.Second),
			UpdatedAt: now,
		})
	}

	tasks, err := store.ListTasks(ctx)
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 3 {
		t.Fatalf("expected 3 tasks, got %d", len(tasks))
	}
}

// ================================================================
// DeleteTask
// ================================================================

func TestDeleteTask_RemovesTask(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	now := time.Now().UTC()
	_ = store.SaveTask(ctx, durable.TaskInfo{ID: "del-task", Status: durable.StatusCompleted, CreatedAt: now, UpdatedAt: now})
	_ = store.DeleteTask(ctx, "del-task")

	_, ok, _ := store.GetTask(ctx, "del-task")
	if ok {
		t.Fatal("task should be deleted")
	}
}

func TestDeleteTask_CascadesToSteps(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	now := time.Now().UTC()
	_ = store.SaveTask(ctx, durable.TaskInfo{ID: "cascade-task", Status: durable.StatusCompleted, CreatedAt: now, UpdatedAt: now})
	_ = store.SaveStep(ctx, "cascade-task", durable.StepRecord{
		StepID: "s1", Seq: 1, Status: durable.StepStatusCompleted, Result: []byte(`"ok"`),
		StartedAt: now, CompletedAt: now,
	})

	_ = store.DeleteTask(ctx, "cascade-task")

	_, ok, _ := store.LoadStep(ctx, "cascade-task", "s1")
	if ok {
		t.Fatal("steps should be cascade-deleted with the task")
	}
}

func TestDeleteTask_NoopForMissingTask(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	// Should not error.
	if err := store.DeleteTask(ctx, "no-such-task"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ================================================================
// PurgeTasks
// ================================================================

func TestPurgeTasks_DeletesOldCompleted(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	old := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Second)
	recent := time.Now().UTC().Truncate(time.Second)

	_ = store.SaveTask(ctx, durable.TaskInfo{ID: "old-done", Status: durable.StatusCompleted, CreatedAt: old, UpdatedAt: old})
	_ = store.SaveTask(ctx, durable.TaskInfo{ID: "new-done", Status: durable.StatusCompleted, CreatedAt: recent, UpdatedAt: recent})

	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	n, err := store.PurgeTasks(ctx, durable.StatusCompleted, cutoff)
	if err != nil {
		t.Fatalf("PurgeTasks: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 deleted, got %d", n)
	}

	_, ok, _ := store.GetTask(ctx, "old-done")
	if ok {
		t.Fatal("old task should be purged")
	}
	_, ok, _ = store.GetTask(ctx, "new-done")
	if !ok {
		t.Fatal("recent task should not be purged")
	}
}

func TestPurgeTasks_DoesNotDeleteRunning(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	old := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Second)
	_ = store.SaveTask(ctx, durable.TaskInfo{ID: "old-running", Status: durable.StatusRunning, CreatedAt: old, UpdatedAt: old})

	cutoff := time.Now().UTC()
	n, _ := store.PurgeTasks(ctx, durable.StatusCompleted, cutoff)
	if n != 0 {
		t.Fatalf("running task should not be purged, got n=%d", n)
	}
}

// ================================================================
// SaveStep / LoadStep
// ================================================================

func TestSaveStep_LoadStep_RoundTrip(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	now := time.Now().UTC().Truncate(time.Second)
	// Task must exist for foreign key.
	_ = store.SaveTask(ctx, durable.TaskInfo{ID: "t1", Status: durable.StatusRunning, CreatedAt: now, UpdatedAt: now})

	step := durable.StepRecord{
		StepID:      "step-a",
		Seq:         1,
		InputHash:   "abc123",
		Result:      []byte(`"hello"`),
		Status:      durable.StepStatusCompleted,
		StartedAt:   now,
		CompletedAt: now.Add(time.Second),
	}
	if err := store.SaveStep(ctx, "t1", step); err != nil {
		t.Fatalf("SaveStep: %v", err)
	}

	got, ok, err := store.LoadStep(ctx, "t1", "step-a")
	if err != nil || !ok {
		t.Fatalf("LoadStep: ok=%v err=%v", ok, err)
	}
	if got.StepID != "step-a" || got.Seq != 1 || got.InputHash != "abc123" {
		t.Fatalf("unexpected step: %+v", got)
	}
	if got.Status != durable.StepStatusCompleted {
		t.Fatalf("got status %q, want completed", got.Status)
	}
	if string(got.Result) != `"hello"` {
		t.Fatalf("got result %q, want %q", got.Result, `"hello"`)
	}
}

func TestLoadStep_NotFound(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	_, ok, err := store.LoadStep(ctx, "no-task", "no-step")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for missing step")
	}
}

func TestSaveStep_Upsert(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	now := time.Now().UTC().Truncate(time.Second)
	_ = store.SaveTask(ctx, durable.TaskInfo{ID: "t-upsert", Status: durable.StatusRunning, CreatedAt: now, UpdatedAt: now})

	// Save as pending first.
	_ = store.SaveStep(ctx, "t-upsert", durable.StepRecord{
		StepID: "s1", Seq: 1, Status: durable.StepStatusPending, StartedAt: now,
	})
	// Upsert to completed.
	_ = store.SaveStep(ctx, "t-upsert", durable.StepRecord{
		StepID: "s1", Seq: 1, Status: durable.StepStatusCompleted,
		Result: []byte(`42`), StartedAt: now, CompletedAt: now.Add(time.Second),
	})

	got, _, _ := store.LoadStep(ctx, "t-upsert", "s1")
	if got.Status != durable.StepStatusCompleted {
		t.Fatalf("got %q, want completed", got.Status)
	}
	if string(got.Result) != "42" {
		t.Fatalf("got result %q", got.Result)
	}
}

func TestSaveStep_PanicTrace(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	now := time.Now().UTC().Truncate(time.Second)
	_ = store.SaveTask(ctx, durable.TaskInfo{ID: "t-panic", Status: durable.StatusFailed, CreatedAt: now, UpdatedAt: now})
	_ = store.SaveStep(ctx, "t-panic", durable.StepRecord{
		StepID:     "bad-step",
		Seq:        1,
		Status:     durable.StepStatusFailed,
		Error:      "panic: boom",
		PanicTrace: "goroutine 1...",
		StartedAt:  now,
	})

	got, _, _ := store.LoadStep(ctx, "t-panic", "bad-step")
	if got.PanicTrace == "" {
		t.Fatal("PanicTrace should be persisted for steps")
	}
}

// ================================================================
// LoadSteps
// ================================================================

func TestLoadSteps_Empty(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	steps, err := store.LoadSteps(ctx, "no-task")
	if err != nil {
		t.Fatalf("LoadSteps: %v", err)
	}
	if steps == nil {
		t.Fatal("LoadSteps should return non-nil empty slice")
	}
	if len(steps) != 0 {
		t.Fatalf("expected 0, got %d", len(steps))
	}
}

func TestLoadSteps_OrderedBySeq(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	now := time.Now().UTC().Truncate(time.Second)
	_ = store.SaveTask(ctx, durable.TaskInfo{ID: "t-order", Status: durable.StatusRunning, CreatedAt: now, UpdatedAt: now})

	// Insert out of order.
	for _, seq := range []int{3, 1, 2} {
		_ = store.SaveStep(ctx, "t-order", durable.StepRecord{
			StepID:    fmt.Sprintf("step-%d", seq),
			Seq:       seq,
			Status:    durable.StepStatusCompleted,
			Result:    []byte(`"x"`),
			StartedAt: now,
		})
	}

	steps, err := store.LoadSteps(ctx, "t-order")
	if err != nil {
		t.Fatalf("LoadSteps: %v", err)
	}
	if len(steps) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(steps))
	}
	for i, s := range steps {
		if s.Seq != i+1 {
			t.Fatalf("step[%d] has seq %d, want %d", i, s.Seq, i+1)
		}
	}
}

// ================================================================
// ListStepIDs
// ================================================================

func TestListStepIDs_ReturnsIDsOrderedBySeq(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	now := time.Now().UTC().Truncate(time.Second)
	_ = store.SaveTask(ctx, durable.TaskInfo{ID: "t-ids", Status: durable.StatusRunning, CreatedAt: now, UpdatedAt: now})

	_ = store.SaveStep(ctx, "t-ids", durable.StepRecord{StepID: "b", Seq: 2, Status: durable.StepStatusCompleted, Result: []byte(`1`), StartedAt: now})
	_ = store.SaveStep(ctx, "t-ids", durable.StepRecord{StepID: "a", Seq: 1, Status: durable.StepStatusCompleted, Result: []byte(`1`), StartedAt: now})

	ids, err := store.ListStepIDs(ctx, "t-ids")
	if err != nil {
		t.Fatalf("ListStepIDs: %v", err)
	}
	if len(ids) != 2 || ids[0] != "a" || ids[1] != "b" {
		t.Fatalf("unexpected ids: %v", ids)
	}
}
