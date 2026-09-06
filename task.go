package durable

import (
	"context"
	"log/slog"
	"time"
)

// taskConfig holds the resolved configuration for a single TaskHandle.
// Constructed via functional options passed to Client.NewTask.
type taskConfig struct {
	id         string
	name       string
	tags       map[string]string
	timeout    time.Duration // zero means no timeout
	maxRetries int           // 0 means no retries beyond the initial attempt
}

// TaskOption is a functional option applied when constructing a TaskHandle.
type TaskOption func(*taskConfig)

// WithName sets a human-readable display name on the task record.
// Does not affect uniqueness or execution behaviour.
func WithName(name string) TaskOption { return func(c *taskConfig) { c.name = name } }

// WithTag attaches an arbitrary key-value annotation to the task record.
// Call multiple times to set multiple tags. Useful for grouping and observability.
func WithTag(k, v string) TaskOption { return func(c *taskConfig) { c.tags[k] = v } }

// WithTimeout sets a hard execution deadline for the task. The context passed
// to the task closure and all child steps is cancelled after this duration.
// Zero (default) means no timeout is applied.
func WithTimeout(d time.Duration) TaskOption { return func(c *taskConfig) { c.timeout = d } }

// WithMaxRetries sets the number of additional execution attempts on failure.
// Defaults to 0 (no retries). On each retry the task closure is re-invoked;
// already-completed steps are replayed from cache without re-executing.
func WithMaxRetries(n int) TaskOption { return func(c *taskConfig) { c.maxRetries = n } }

// TaskHandle is a bound execution handle for a uniquely identified task.
// Created via Client.NewTask and passed to Run. Safe to store and reuse
// across Run calls; a non-terminal task reuses its existing step cache.
type TaskHandle struct {
	cfg    taskConfig
	store  Store
	logger *slog.Logger
	client *Client // back-reference for per-task mutex in Run
}

// ID returns the unique task identifier associated with this handle.
func (h *TaskHandle) ID() string { return h.cfg.id }

// NewTask creates a TaskHandle bound to this client's store.
// id must be unique per logical task execution; reusing the same id on a
// subsequent Run replays previously completed steps from the store.
func (c *Client) NewTask(id string, opts ...TaskOption) *TaskHandle {
	cfg := taskConfig{id: id, tags: make(map[string]string)}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &TaskHandle{cfg: cfg, store: c.store, logger: c.cfg.logger, client: c}
}

// Task is the execution contract for a durable task.
// Implementations should treat any work with external side effects as a Step;
// non-deterministic logic outside of Step calls may not be replayed correctly.
// I is the input type; O is the output type.
type Task[I, O any] interface {
	// Exec performs the task logic. ctx is cancelled when the task timeout elapses.
	// s is the StepRunner bound to this execution; wrap all memoised work in Step calls.
	// Panics are recovered by the engine, recorded in TaskInfo.PanicTrace, and
	// surfaced as an error to the Run caller.
	Exec(ctx context.Context, s *StepRunner, input I) (O, error)
}

// TaskFunc adapts a plain function to the Task[I, O] interface, enabling inline closures.
type TaskFunc[I, O any] func(ctx context.Context, s *StepRunner, input I) (O, error)

// Exec implements Task[I, O] for TaskFunc.
func (f TaskFunc[I, O]) Exec(ctx context.Context, s *StepRunner, input I) (O, error) {
	return f(ctx, s, input)
}

// Func wraps a plain function as a Task, triggering Go's generic type inference
// so callers do not need to specify type parameters explicitly.
func Func[I, O any](fn func(ctx context.Context, s *StepRunner, in I) (O, error)) TaskFunc[I, O] {
	return TaskFunc[I, O](fn)
}
