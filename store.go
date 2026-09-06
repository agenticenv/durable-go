package durable

import (
	"context"
	"time"
)

// TaskStatus represents the lifecycle state of a Task.
type TaskStatus string

const (
	// StatusPending indicates the task has been registered but not yet started.
	StatusPending TaskStatus = "pending"
	// StatusRunning indicates the task is actively executing.
	StatusRunning TaskStatus = "running"
	// StatusCompleted indicates the task finished successfully.
	StatusCompleted TaskStatus = "completed"
	// StatusFailed indicates the task terminated with an error or recovered panic.
	StatusFailed TaskStatus = "failed"
)

// StepStatus represents the lifecycle state of a single Step within a Task.
type StepStatus string

const (
	// StepStatusPending indicates the step has been registered but not yet executed.
	StepStatusPending StepStatus = "pending"
	// StepStatusCompleted indicates the step executed successfully and its result is cached.
	StepStatusCompleted StepStatus = "completed"
	// StepStatusFailed indicates the step execution returned an error or panicked.
	StepStatusFailed StepStatus = "failed"
)

// TaskInfo is the persistent metadata record for a single task execution.
// It is written on task start and updated on completion or failure.
type TaskInfo struct {
	// ID is the unique, caller-supplied task identifier.
	ID string
	// Name is a human-readable label set via WithName. May be empty.
	Name string
	// Tags are arbitrary key-value annotations for filtering and observability.
	Tags map[string]string
	// Status is the current lifecycle state of the task.
	Status TaskStatus
	// Error is the serialised error message when Status == StatusFailed.
	Error string
	// PanicTrace holds the recovered panic value and stack when the task panicked.
	// Empty string if no panic occurred.
	PanicTrace string
	// CreatedAt is the wall-clock time when the task record was first persisted.
	CreatedAt time.Time
	// StartedAt is the wall-clock time when Run began executing the task closure.
	StartedAt time.Time
	// CompletedAt is the wall-clock time when the task reached a terminal state.
	CompletedAt time.Time
	// UpdatedAt is the wall-clock time of the most recent record mutation.
	UpdatedAt time.Time
}

// StepRecord is the persistent checkpoint for a single memoized step.
// Once Status == StepStatusCompleted, Result is immutable and will be replayed
// verbatim on subsequent task runs without re-executing the step function.
type StepRecord struct {
	// StepID is the caller-supplied, task-scoped identifier for this step.
	StepID string
	// Seq is the zero-based execution order of this step within its parent task.
	// Used to validate replay ordering and detect sequence divergence.
	Seq int
	// InputHash is an optional hash of the step's input used for cache-busting
	// when the same StepID is reused with different inputs across runs.
	InputHash string
	// Result is the JSON-marshalled output value of a successfully completed step.
	// Non-nil only when Status == StepStatusCompleted.
	Result []byte
	// Error is the serialised error string when Status == StepStatusFailed.
	Error string
	// PanicTrace holds the recovered panic value and stack when the step panicked.
	PanicTrace string
	// Status is the current lifecycle state of this step.
	Status StepStatus
	// StartedAt is the wall-clock time when the step function was invoked.
	StartedAt time.Time
	// CompletedAt is the wall-clock time when the step reached a terminal state.
	CompletedAt time.Time
}

// ListStaleTasks returns all tasks with StatusRunning whose UpdatedAt is older
// than staleSince ago. These tasks were most likely left in the running state
// by a previous process crash and were never resumed.
//
// Callers can inspect the returned tasks and decide whether to re-run them
// (by calling Run with the same task ID, which will replay completed steps)
// or mark them failed via DeleteTask and starting fresh.
//
//	stale, err := durable.ListStaleTasks(ctx, store, 10*time.Minute)
func ListStaleTasks(ctx context.Context, store Store, staleSince time.Duration) ([]TaskInfo, error) {
	tasks, err := store.ListTasks(ctx)
	if err != nil {
		return nil, err
	}
	cutoff := time.Now().UTC().Add(-staleSince)
	var stale []TaskInfo
	for _, t := range tasks {
		if t.Status == StatusRunning && t.UpdatedAt.Before(cutoff) {
			stale = append(stale, t)
		}
	}
	return stale, nil
}

// Store is the persistence contract for durable task and step state.
// All implementations must be safe for concurrent use by multiple goroutines.
// Implementations must treat writes as upserts (idempotent on ID conflict).
//
// durable-go's built-in store (store/journal) is designed for single-process
// use only. Do not share a store across multiple OS processes or pods.
type Store interface {
	// SaveTask persists or updates a TaskInfo record.
	// Must be an upsert: if a record with task.ID already exists, all mutable
	// fields are overwritten. Returns an error if the store is unavailable.
	SaveTask(ctx context.Context, task TaskInfo) error

	// GetTask retrieves a TaskInfo by its ID.
	// Returns (info, true, nil) when found, (zero, false, nil) when not found,
	// and (zero, false, err) on store errors.
	GetTask(ctx context.Context, taskID string) (TaskInfo, bool, error)

	// ListTasks returns all recorded TaskInfo records ordered by CreatedAt descending.
	ListTasks(ctx context.Context) ([]TaskInfo, error)

	// DeleteTask removes a task record and all associated step records atomically.
	// Must be a no-op (not an error) when the task does not exist.
	DeleteTask(ctx context.Context, taskID string) error

	// PurgeTasks deletes all task records (and their steps) with the given status
	// whose UpdatedAt is strictly before the cutoff time.
	// Returns the number of task records deleted.
	PurgeTasks(ctx context.Context, status TaskStatus, before time.Time) (int64, error)

	// SaveStep persists or updates a StepRecord under the given taskID.
	// Must be an upsert keyed on (taskID, step.StepID).
	SaveStep(ctx context.Context, taskID string, step StepRecord) error

	// LoadStep retrieves a single StepRecord by (taskID, stepID).
	// Returns (record, true, nil) when found, (zero, false, nil) when not found,
	// and (zero, false, err) on store errors.
	LoadStep(ctx context.Context, taskID, stepID string) (StepRecord, bool, error)

	// LoadSteps returns all StepRecords for a task ordered by Seq ascending.
	// Used during task replay to pre-populate the step cache in a single round-trip.
	// Must return a non-nil empty slice when no steps exist.
	LoadSteps(ctx context.Context, taskID string) ([]StepRecord, error)

	// ListStepIDs returns only the step IDs for a task ordered by Seq ascending.
	// Prefer LoadSteps when full records are needed.
	ListStepIDs(ctx context.Context, taskID string) ([]string, error)
}
