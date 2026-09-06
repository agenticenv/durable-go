package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"
)

// config is the internal resolved configuration for a Client.
// Users never construct this directly; it is built from Option values inside NewClient.
type config struct {
	// autoPurgeAfter triggers background purging of completed and failed
	// task records older than this duration. Zero disables purging.
	autoPurgeAfter time.Duration
	// autoPurgeInterval controls how often the background purger runs.
	// Defaults to 1 hour when autoPurgeAfter is set.
	autoPurgeInterval time.Duration
	// logger is the custom slog.Logger to use for logging.
	logger *slog.Logger
}

// Option is a functional option applied when constructing a Client via NewClient.
type Option func(*config)

// WithAutoPurge enables periodic purging of completed and failed task records
// older than age. The optional interval controls how frequently the purger runs
// (defaults to 1 hour). Pass a single duration for the most common case:
//
//	durable.WithAutoPurge(24 * time.Hour)
func WithAutoPurge(age time.Duration, interval ...time.Duration) Option {
	return func(c *config) {
		c.autoPurgeAfter = age
		if len(interval) > 0 {
			c.autoPurgeInterval = interval[0]
		}
	}
}

// WithLogger sets a custom slog.Logger for the Client.
// All task and step lifecycle events are emitted through this logger at the
// appropriate level (Debug for normal flow, Info for completions, Warn for
// retries, Error for failures and panics).
// The log level is determined entirely by the handler attached to logger —
// pass a handler with a higher min-level to suppress verbose output.
// If nil or not provided, a no-op logger is used and nothing is emitted.
func WithLogger(logger *slog.Logger) Option {
	return func(c *config) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// Client is the process-level entry point for durable task execution.
// Create one per process via NewClient and share it across goroutines.
// Client is safe for concurrent use.
//
// Always call Close (or defer it) when the Client is no longer needed to stop
// the background auto-purger goroutine and release associated resources.
type Client struct {
	store  Store
	cfg    config
	stopCh chan struct{} // closed by Close to signal the purger goroutine to exit
}

// NewClient initialises a Client bound to the given store.
// Returns an error if store is nil.
// If WithAutoPurge is provided, a background goroutine is started that
// periodically purges old completed/failed task records. Call Close to stop it.
//
//	client, err := durable.NewClient(ctx, store)
//	// with auto-purge:
//	client, err := durable.NewClient(ctx, store, durable.WithAutoPurge(24*time.Hour))
func NewClient(ctx context.Context, store Store, opts ...Option) (*Client, error) {
	if store == nil {
		return nil, errors.New("durable: store must not be nil")
	}
	cfg := config{
		logger: slog.New(slog.DiscardHandler),
	}
	for _, o := range opts {
		o(&cfg)
	}
	c := &Client{
		store:  store,
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}
	if c.cfg.autoPurgeAfter > 0 {
		go c.runAutoPurge()
	}
	return c, nil
}

// runAutoPurge is a long-lived goroutine that purges stale task records on a
// configurable interval. It stops when stopCh is closed (via Close).
func (c *Client) runAutoPurge() {
	interval := c.cfg.autoPurgeInterval
	if interval <= 0 {
		interval = time.Hour
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cutoff := time.Now().UTC().Add(-c.cfg.autoPurgeAfter)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			c.cfg.logger.Debug("auto-purge running", "cutoff", cutoff)
			// Purge both terminal statuses; failed tasks older than the cutoff
			// are equally stale and safe to remove.
			nCompleted, err1 := c.store.PurgeTasks(ctx, StatusCompleted, cutoff)
			nFailed, err2 := c.store.PurgeTasks(ctx, StatusFailed, cutoff)
			cancel()
			if err1 != nil {
				c.cfg.logger.Error("auto-purge: purge completed tasks failed", "error", err1)
			}
			if err2 != nil {
				c.cfg.logger.Error("auto-purge: purge failed tasks failed", "error", err2)
			}
			if err1 == nil && err2 == nil {
				c.cfg.logger.Info("auto-purge completed", "purged_completed", nCompleted, "purged_failed", nFailed, "cutoff", cutoff)
			}
		case <-c.stopCh:
			return
		}
	}
}

// Close stops the background auto-purger goroutine if it is running.
// It is safe to call Close more than once. Idiomatic usage:
//
//	client, err := durable.NewClient(ctx, cfg)
//	if err != nil { ... }
//	defer client.Close()
func (c *Client) Close() error {
	select {
	case <-c.stopCh:
		// Already closed; no-op.
	default:
		close(c.stopCh)
	}
	return nil
}

// Run executes the given task using the execution handle h.
//
// On first run, a new TaskInfo record is persisted and the task closure is executed.
// On a subsequent run with the same h.ID(), all previously completed steps are
// loaded from the store in a single batch and replayed from cache without re-execution.
//
// The engine recovers panics from the task closure, records the panic value and stack
// in TaskInfo.PanicTrace, marks the task StatusFailed, and returns an error.
//
// Concurrent Run calls for different task IDs are safe. Concurrent calls for the
// same task ID result in undefined behaviour; callers must serialise per task ID.
func Run[I, O any](ctx context.Context, h *TaskHandle, input I, task Task[I, O]) (O, error) {
	var zero O

	// 1. Apply timeout if configured.
	if h.cfg.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.cfg.timeout)
		defer cancel()
	}

	now := time.Now().UTC()

	// 2. Upsert TaskInfo as running.
	info := TaskInfo{
		ID:        h.cfg.id,
		Name:      h.cfg.name,
		Tags:      h.cfg.tags,
		Status:    StatusRunning,
		StartedAt: now,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := h.store.SaveTask(ctx, info); err != nil {
		return zero, fmt.Errorf("durable: save task start: %w", err)
	}
	h.logger.Debug("task started", "task_id", h.cfg.id, "name", h.cfg.name)

	// 3. Pre-load all existing step records for O(1) replay lookup.
	steps, err := h.store.LoadSteps(ctx, h.cfg.id)
	if err != nil {
		return zero, fmt.Errorf("durable: load steps: %w", err)
	}
	cache := make(map[string]StepRecord, len(steps))
	for _, s := range steps {
		cache[s.StepID] = s
	}
	h.logger.Debug("task step cache loaded", "task_id", h.cfg.id, "cached_steps", len(cache))

	// 4. Construct the StepRunner.
	runner := &StepRunner{
		taskID: h.cfg.id,
		store:  h.store,
		cache:  cache,
		logger: h.logger,
	}

	// execOnce runs the task closure with panic recovery.
	// Returns (output, panicTrace, taskErr) — error is last per ST1008.
	execOnce := func() (result O, panicTrace string, taskErr error) {
		defer func() {
			if r := recover(); r != nil {
				panicTrace = fmt.Sprintf("%v\n%s", r, debug.Stack())
				taskErr = fmt.Errorf("durable: task panicked: %v", r)
			}
		}()
		result, taskErr = task.Exec(ctx, runner, input)
		return
	}

	var (
		out        O
		taskErr    error
		panicTrace string
	)

	// 9. Retry loop (initial attempt + up to maxRetries more).
	attempts := 1 + h.cfg.maxRetries
	for attempt := 0; attempt < attempts; attempt++ {
		// Reset the seq counter on each attempt; cache persists so completed
		// steps are still replayed.
		runner.seq = 0
		if attempt > 0 {
			h.logger.Warn("task retrying", "task_id", h.cfg.id, "attempt", attempt, "error", taskErr)
		}

		out, panicTrace, taskErr = execOnce()
		if taskErr == nil {
			break
		}
		// Don't retry if the context is cancelled/timed out.
		if ctx.Err() != nil {
			taskErr = ctx.Err()
			break
		}
	}

	// 7 & 8. Persist final task state.
	info.UpdatedAt = time.Now().UTC()
	info.CompletedAt = info.UpdatedAt
	info.PanicTrace = panicTrace

	if taskErr != nil {
		info.Status = StatusFailed
		info.Error = taskErr.Error()
		_ = h.store.SaveTask(ctx, info)
		if panicTrace != "" {
			h.logger.Error("task panicked", "task_id", h.cfg.id, "name", h.cfg.name, "panic_trace", panicTrace)
		} else {
			h.logger.Error("task failed", "task_id", h.cfg.id, "name", h.cfg.name, "error", taskErr)
		}
		return zero, taskErr
	}

	info.Status = StatusCompleted
	if err := h.store.SaveTask(ctx, info); err != nil {
		return zero, fmt.Errorf("durable: save task completion: %w", err)
	}
	h.logger.Info("task completed", "task_id", h.cfg.id, "name", h.cfg.name, "duration", info.CompletedAt.Sub(info.StartedAt))

	return out, nil
}

// StepRunner is scoped to a single task execution and provides the Step primitive.
// It is not safe for concurrent use; do not share a StepRunner across goroutines.
type StepRunner struct {
	// taskID is the parent task's unique identifier.
	taskID string
	// store is the persistence backend shared with the parent Client.
	store Store
	// seq is the monotonically increasing step sequence counter for this execution.
	// Incremented before each step invocation; used to detect replay divergence.
	seq int
	// cache holds pre-loaded StepRecords keyed by StepID, populated at run start.
	// A step whose cached Status == StepStatusCompleted is replayed without re-execution.
	cache map[string]StepRecord
	// logger is inherited from the Client that created the TaskHandle.
	logger *slog.Logger
}

// Step executes fn as a memoised, fault-tolerant step within the parent task.
//
// stepID must be unique and stable within the task across runs. Derive it from
// static literals or a deterministic counter — never from runtime data that may
// change between executions (e.g. timestamps, random values).
//
// Cache hit: if a StepRecord with StepStatusCompleted exists for (taskID, stepID),
// its Result is unmarshalled and returned immediately without calling fn.
//
// Cache miss: fn is executed. On success the result is marshalled, persisted as
// StepStatusCompleted, and returned. On error the step is persisted as StepStatusFailed
// and the error is returned; subsequent replays will re-execute fn.
//
// Panics in fn are recovered, persisted as StepStatusFailed with PanicTrace populated,
// and returned as a non-nil error to the caller.
//
// Step is not safe for concurrent use within a single StepRunner.
func Step[O any](ctx context.Context, s *StepRunner, stepID string, fn func(ctx context.Context) (O, error)) (O, error) {
	var zero O

	// 1. Advance sequence counter.
	s.seq++
	seq := s.seq

	// 2. Cache hit — replay completed step without calling fn.
	if rec, ok := s.cache[stepID]; ok && rec.Status == StepStatusCompleted {
		var out O
		if err := json.Unmarshal(rec.Result, &out); err != nil {
			return zero, fmt.Errorf("durable: replay step %q: unmarshal: %w", stepID, err)
		}
		s.logger.Debug("step replayed from cache", "task_id", s.taskID, "step_id", stepID, "seq", seq)
		return out, nil
	}

	// 3. Persist pending record before execution so a crash mid-step is observable.
	pending := StepRecord{
		StepID:    stepID,
		Seq:       seq,
		Status:    StepStatusPending,
		StartedAt: time.Now().UTC(),
	}
	if err := s.store.SaveStep(ctx, s.taskID, pending); err != nil {
		return zero, fmt.Errorf("durable: save step pending %q: %w", stepID, err)
	}
	s.logger.Debug("step executing", "task_id", s.taskID, "step_id", stepID, "seq", seq)

	// 4 & 5. Execute fn with panic recovery.
	var (
		out        O
		fnErr      error
		panicTrace string
	)

	func() {
		defer func() {
			if r := recover(); r != nil {
				panicTrace = fmt.Sprintf("%v\n%s", r, debug.Stack())
				fnErr = fmt.Errorf("durable: step %q panicked: %v", stepID, r)
			}
		}()
		out, fnErr = fn(ctx)
	}()

	completedAt := time.Now().UTC()

	if fnErr != nil || panicTrace != "" {
		failed := StepRecord{
			StepID:      stepID,
			Seq:         seq,
			Status:      StepStatusFailed,
			Error:       fnErr.Error(),
			PanicTrace:  panicTrace,
			StartedAt:   pending.StartedAt,
			CompletedAt: completedAt,
		}
		_ = s.store.SaveStep(ctx, s.taskID, failed)
		if panicTrace != "" {
			s.logger.Error("step panicked", "task_id", s.taskID, "step_id", stepID, "seq", seq, "panic_trace", panicTrace)
		} else {
			s.logger.Error("step failed", "task_id", s.taskID, "step_id", stepID, "seq", seq, "error", fnErr)
		}
		return zero, fnErr
	}

	// 6. Marshal and persist completed result.
	raw, err := json.Marshal(out)
	if err != nil {
		return zero, fmt.Errorf("durable: step %q: marshal result: %w", stepID, err)
	}

	completed := StepRecord{
		StepID:      stepID,
		Seq:         seq,
		Status:      StepStatusCompleted,
		Result:      raw,
		StartedAt:   pending.StartedAt,
		CompletedAt: completedAt,
	}
	if err := s.store.SaveStep(ctx, s.taskID, completed); err != nil {
		return zero, fmt.Errorf("durable: save step completed %q: %w", stepID, err)
	}
	s.logger.Debug("step completed", "task_id", s.taskID, "step_id", stepID, "seq", seq, "duration", completedAt.Sub(pending.StartedAt))

	// Update local cache so subsequent steps in the same run see the result.
	s.cache[stepID] = completed

	return out, nil
}
