package journal_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	durable "github.com/agenticenv/durable-go"
	"github.com/agenticenv/durable-go/store/journal"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func newTestStore(t *testing.T) *journal.JournalStore {
	t.Helper()
	root := filepath.Join(t.TempDir(), "journal")
	s, err := journal.NewJournalStore(root)
	if err != nil {
		t.Fatalf("NewJournalStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

var ctx = context.Background()

func taskInfo(id string, status durable.TaskStatus) durable.TaskInfo {
	now := time.Now().UTC()
	return durable.TaskInfo{
		ID:        id,
		Name:      "test-" + id,
		Status:    status,
		CreatedAt: now,
		UpdatedAt: now,
		StartedAt: now,
	}
}

func stepRecord(stepID string, seq int, status durable.StepStatus) durable.StepRecord {
	now := time.Now().UTC()
	r := durable.StepRecord{
		StepID:    stepID,
		Seq:       seq,
		Status:    status,
		StartedAt: now,
	}
	if status == durable.StepStatusCompleted {
		r.Result = []byte(`"ok"`)
		r.CompletedAt = now
	}
	return r
}

// ── NewJournalStore ───────────────────────────────────────────────────────────

func TestNewJournalStore_CreatesRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "new-root")
	s, err := journal.NewJournalStore(root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = s.Close() }()
	if _, err := os.Stat(filepath.Join(root, "tasks")); err != nil {
		t.Errorf("tasks dir not created: %v", err)
	}
}

func TestNewJournalStore_DuplicateRootReturnsError(t *testing.T) {
	root := filepath.Join(t.TempDir(), "dup-root")
	s1, err := journal.NewJournalStore(root)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	defer func() { _ = s1.Close() }()

	_, err = journal.NewJournalStore(root)
	if err == nil {
		t.Fatal("expected error opening same root twice, got nil")
	}
}

func TestNewJournalStore_AfterCloseCanReopen(t *testing.T) {
	root := filepath.Join(t.TempDir(), "reopen")
	s, err := journal.NewJournalStore(root)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_ = s.Close()

	s2, err := journal.NewJournalStore(root)
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	defer func() { _ = s2.Close() }()
}

// ── SaveTask / GetTask ────────────────────────────────────────────────────────

func TestSaveTask_GetTask_RoundTrip(t *testing.T) {
	s := newTestStore(t)
	task := taskInfo("task-1", durable.StatusRunning)
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}
	got, ok, err := s.GetTask(ctx, "task-1")
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if !ok {
		t.Fatal("expected task to exist")
	}
	if got.ID != "task-1" || got.Status != durable.StatusRunning {
		t.Errorf("unexpected task: %+v", got)
	}
}

func TestGetTask_NotFound(t *testing.T) {
	s := newTestStore(t)
	_, ok, err := s.GetTask(ctx, "missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for missing task")
	}
}

func TestSaveTask_Upsert(t *testing.T) {
	s := newTestStore(t)
	task := taskInfo("upsert-task", durable.StatusRunning)
	_ = s.SaveTask(ctx, task)

	task.Status = durable.StatusCompleted
	now := time.Now().UTC()
	task.CompletedAt = now
	task.UpdatedAt = now
	if err := s.SaveTask(ctx, task); err != nil {
		t.Fatalf("SaveTask upsert: %v", err)
	}

	got, _, _ := s.GetTask(ctx, "upsert-task")
	if got.Status != durable.StatusCompleted {
		t.Errorf("expected completed, got %s", got.Status)
	}
}

// ── ListTasks ─────────────────────────────────────────────────────────────────

func TestListTasks_OrderedByCreatedAtDesc(t *testing.T) {
	s := newTestStore(t)

	t1 := taskInfo("t1", durable.StatusCompleted)
	t1.CreatedAt = time.Now().UTC().Add(-2 * time.Hour)
	t1.UpdatedAt = t1.CreatedAt

	t2 := taskInfo("t2", durable.StatusRunning)
	t2.CreatedAt = time.Now().UTC().Add(-1 * time.Hour)
	t2.UpdatedAt = t2.CreatedAt

	_ = s.SaveTask(ctx, t1)
	_ = s.SaveTask(ctx, t2)

	tasks, err := s.ListTasks(ctx)
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(tasks))
	}
	if tasks[0].ID != "t2" || tasks[1].ID != "t1" {
		t.Errorf("wrong order: %v %v", tasks[0].ID, tasks[1].ID)
	}
}

// ── DeleteTask ────────────────────────────────────────────────────────────────

func TestDeleteTask_RemovesTask(t *testing.T) {
	s := newTestStore(t)
	_ = s.SaveTask(ctx, taskInfo("del-me", durable.StatusCompleted))
	if err := s.DeleteTask(ctx, "del-me"); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}
	_, ok, _ := s.GetTask(ctx, "del-me")
	if ok {
		t.Error("expected task to be deleted")
	}
}

func TestDeleteTask_NoOpForMissing(t *testing.T) {
	s := newTestStore(t)
	if err := s.DeleteTask(ctx, "never-existed"); err != nil {
		t.Errorf("expected no-op, got: %v", err)
	}
}

// ── PurgeTasks ────────────────────────────────────────────────────────────────

func TestPurgeTasks_RemovesMatchingTasks(t *testing.T) {
	s := newTestStore(t)

	old := taskInfo("old-completed", durable.StatusCompleted)
	old.UpdatedAt = time.Now().UTC().Add(-48 * time.Hour)
	_ = s.SaveTask(ctx, old)

	recent := taskInfo("recent-completed", durable.StatusCompleted)
	recent.UpdatedAt = time.Now().UTC()
	_ = s.SaveTask(ctx, recent)

	running := taskInfo("running-task", durable.StatusRunning)
	running.UpdatedAt = time.Now().UTC().Add(-48 * time.Hour)
	_ = s.SaveTask(ctx, running)

	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	n, err := s.PurgeTasks(ctx, durable.StatusCompleted, cutoff)
	if err != nil {
		t.Fatalf("PurgeTasks: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 purged, got %d", n)
	}

	_, ok, _ := s.GetTask(ctx, "old-completed")
	if ok {
		t.Error("old-completed should be purged")
	}
	_, ok, _ = s.GetTask(ctx, "recent-completed")
	if !ok {
		t.Error("recent-completed should survive")
	}
	_, ok, _ = s.GetTask(ctx, "running-task")
	if !ok {
		t.Error("running-task should survive (wrong status)")
	}
}

// ── SaveStep / LoadSteps ──────────────────────────────────────────────────────

func TestSaveStep_LoadSteps_RoundTrip(t *testing.T) {
	s := newTestStore(t)
	_ = s.SaveTask(ctx, taskInfo("step-task", durable.StatusRunning))

	steps := []durable.StepRecord{
		stepRecord("step-a", 1, durable.StepStatusCompleted),
		stepRecord("step-b", 2, durable.StepStatusCompleted),
	}
	for _, r := range steps {
		if err := s.SaveStep(ctx, "step-task", r); err != nil {
			t.Fatalf("SaveStep %q: %v", r.StepID, err)
		}
	}

	got, err := s.LoadSteps(ctx, "step-task")
	if err != nil {
		t.Fatalf("LoadSteps: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(got))
	}
	if got[0].StepID != "step-a" || got[1].StepID != "step-b" {
		t.Errorf("unexpected step IDs: %v %v", got[0].StepID, got[1].StepID)
	}
}

func TestSaveStep_UpsertSemantics(t *testing.T) {
	// Append pending then completed for same stepID — LoadSteps returns completed (last-write-wins).
	s := newTestStore(t)
	_ = s.SaveTask(ctx, taskInfo("upsert-steps", durable.StatusRunning))

	pending := stepRecord("s1", 1, durable.StepStatusPending)
	completed := stepRecord("s1", 1, durable.StepStatusCompleted)

	_ = s.SaveStep(ctx, "upsert-steps", pending)
	_ = s.SaveStep(ctx, "upsert-steps", completed)

	got, _ := s.LoadSteps(ctx, "upsert-steps")
	if len(got) != 1 {
		t.Fatalf("expected 1 deduplicated step, got %d", len(got))
	}
	if got[0].Status != durable.StepStatusCompleted {
		t.Errorf("expected completed status, got %s", got[0].Status)
	}
}

func TestLoadSteps_EmptyForNewTask(t *testing.T) {
	s := newTestStore(t)
	_ = s.SaveTask(ctx, taskInfo("no-steps", durable.StatusRunning))
	steps, err := s.LoadSteps(ctx, "no-steps")
	if err != nil {
		t.Fatalf("LoadSteps: %v", err)
	}
	if steps == nil {
		t.Error("expected non-nil empty slice")
	}
	if len(steps) != 0 {
		t.Errorf("expected 0 steps, got %d", len(steps))
	}
}

// ── Kill-9 partial-write recovery ─────────────────────────────────────────────

func TestLoadSteps_PartialFrameAtTailIsDiscarded(t *testing.T) {
	root := filepath.Join(t.TempDir(), "crash-test")
	s, _ := journal.NewJournalStore(root)
	defer func() { _ = s.Close() }()

	taskID := "crash-task"
	_ = s.SaveTask(ctx, taskInfo(taskID, durable.StatusRunning))

	// Write two valid steps.
	_ = s.SaveStep(ctx, taskID, stepRecord("s1", 1, durable.StepStatusCompleted))
	_ = s.SaveStep(ctx, taskID, stepRecord("s2", 2, durable.StepStatusCompleted))

	// Simulate a partial write: append garbage bytes to the journal.
	logPath := filepath.Join(root, "tasks", taskID, "journal.log")
	f, _ := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o644)
	_, _ = f.Write([]byte{0x00, 0x00, 0x00, 0x20, 0xDE, 0xAD}) // truncated frame
	_ = f.Close()

	// Reload — should recover both valid steps and discard the corrupt tail.
	steps, err := s.LoadSteps(ctx, taskID)
	if err != nil {
		t.Fatalf("LoadSteps after partial write: %v", err)
	}
	if len(steps) != 2 {
		t.Errorf("expected 2 steps after partial-write recovery, got %d", len(steps))
	}
}

// ── LoadStep ──────────────────────────────────────────────────────────────────

func TestLoadStep_Found(t *testing.T) {
	s := newTestStore(t)
	_ = s.SaveTask(ctx, taskInfo("ls-task", durable.StatusRunning))
	_ = s.SaveStep(ctx, "ls-task", stepRecord("s1", 1, durable.StepStatusCompleted))

	rec, ok, err := s.LoadStep(ctx, "ls-task", "s1")
	if err != nil || !ok {
		t.Fatalf("LoadStep: ok=%v err=%v", ok, err)
	}
	if rec.StepID != "s1" {
		t.Errorf("unexpected step: %+v", rec)
	}
}

func TestLoadStep_NotFound(t *testing.T) {
	s := newTestStore(t)
	_ = s.SaveTask(ctx, taskInfo("ls-task2", durable.StatusRunning))

	_, ok, err := s.LoadStep(ctx, "ls-task2", "missing-step")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected ok=false")
	}
}

// ── ListStepIDs ───────────────────────────────────────────────────────────────

func TestListStepIDs(t *testing.T) {
	s := newTestStore(t)
	_ = s.SaveTask(ctx, taskInfo("ids-task", durable.StatusRunning))
	_ = s.SaveStep(ctx, "ids-task", stepRecord("alpha", 1, durable.StepStatusCompleted))
	_ = s.SaveStep(ctx, "ids-task", stepRecord("beta", 2, durable.StepStatusCompleted))

	ids, err := s.ListStepIDs(ctx, "ids-task")
	if err != nil {
		t.Fatalf("ListStepIDs: %v", err)
	}
	if len(ids) != 2 || ids[0] != "alpha" || ids[1] != "beta" {
		t.Errorf("unexpected ids: %v", ids)
	}
}

// ── ListStaleTasks ────────────────────────────────────────────────────────────

func TestListStaleTasks(t *testing.T) {
	s := newTestStore(t)

	staleRunning := taskInfo("stale-run", durable.StatusRunning)
	staleRunning.UpdatedAt = time.Now().UTC().Add(-30 * time.Minute)
	_ = s.SaveTask(ctx, staleRunning)

	freshRunning := taskInfo("fresh-run", durable.StatusRunning)
	freshRunning.UpdatedAt = time.Now().UTC()
	_ = s.SaveTask(ctx, freshRunning)

	completed := taskInfo("done", durable.StatusCompleted)
	completed.UpdatedAt = time.Now().UTC().Add(-30 * time.Minute)
	_ = s.SaveTask(ctx, completed)

	stale, err := durable.ListStaleTasks(ctx, s, 10*time.Minute)
	if err != nil {
		t.Fatalf("ListStaleTasks: %v", err)
	}
	if len(stale) != 1 || stale[0].ID != "stale-run" {
		t.Errorf("expected [stale-run], got %v", stale)
	}
}
