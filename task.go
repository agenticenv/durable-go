package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"
)

// TaskStatus is the lifecycle state of a run.
type TaskStatus string

const (
	// StatusRunning means the run is actively executing.
	StatusRunning TaskStatus = "running"
	// StatusWaiting means at least one step is awaiting CompleteStep.
	StatusWaiting TaskStatus = "waiting"
	// StatusCompleted means the run finished successfully.
	StatusCompleted TaskStatus = "completed"
	// StatusFailed means the run terminated with an error or recovered panic.
	StatusFailed TaskStatus = "failed"
)

// TaskInfo is the persistent metadata for a single run. The task input is
// stored separately in input.json (see RunTask), not on this struct.
type TaskInfo struct {
	TaskID      string            `json:"task_id"`
	RunID       string            `json:"run_id"`
	Name        string            `json:"name"`
	Tags        map[string]string `json:"tags"`
	Status      TaskStatus        `json:"status"`
	Error       string            `json:"error"`
	PanicTrace  string            `json:"panic_trace"`
	CreatedAt   time.Time         `json:"created_at"`
	StartedAt   time.Time         `json:"started_at"`
	CompletedAt time.Time         `json:"completed_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// Task is the execution contract for a durable task.
// Implementations should treat any work with external side effects as a
// RunStep; non-deterministic logic outside of RunStep calls may not be
// replayed correctly. I is the input type; O is the output type.
type Task[I, O any] interface {
	// Exec performs the task logic. ctx is cancelled when the task timeout
	// elapses or the engine is closed. s is the StepRunner bound to this
	// run; wrap all memoised work in RunStep calls. Panics are recovered
	// by the engine, recorded in TaskInfo.PanicTrace, and surfaced to Get.
	Exec(ctx context.Context, s *StepRunner, input I) (O, error)
}

// TaskFunc adapts a plain function to Task[I, O].
type TaskFunc[I, O any] func(ctx context.Context, s *StepRunner, input I) (O, error)

// Exec implements Task[I, O] for TaskFunc.
func (f TaskFunc[I, O]) Exec(ctx context.Context, s *StepRunner, input I) (O, error) {
	return f(ctx, s, input)
}

// Func wraps a plain function as a Task, triggering Go's generic type
// inference so callers do not need to specify type parameters explicitly.
func Func[I, O any](fn func(ctx context.Context, s *StepRunner, in I) (O, error)) TaskFunc[I, O] {
	return TaskFunc[I, O](fn)
}

type taskConfig struct {
	name       string
	tags       map[string]string
	maxRetries *int
	timeout    *time.Duration
}

// TaskOption configures RegisterTask.
type TaskOption func(*taskConfig)

// WithName sets a human-readable label stored on TaskInfo. It does not
// affect uniqueness or execution.
func WithName(name string) TaskOption {
	return func(c *taskConfig) { c.name = name }
}

// WithTag attaches an arbitrary key-value annotation to TaskInfo. Call
// multiple times to set multiple tags.
func WithTag(k, v string) TaskOption {
	return func(c *taskConfig) { c.tags[k] = v }
}

// WithTaskMaxRetries overrides the engine default for task-level retries
// (re-invoking the whole closure). Nil-vs-set is tracked so an explicit 0
// disables a non-zero engine default.
func WithTaskMaxRetries(n int) TaskOption {
	return func(c *taskConfig) { c.maxRetries = &n }
}

// WithTaskTimeout overrides the engine default task deadline. An explicit
// 0 disables a non-zero engine default.
func WithTaskTimeout(d time.Duration) TaskOption {
	return func(c *taskConfig) { c.timeout = &d }
}

// taskEntry is the type-erased registry value. Input and output are
// JSON-encoded at the RunTask / Get boundary so the registry can store
// heterogeneous tasks in one map.
type taskEntry struct {
	cfg  taskConfig
	exec func(ctx context.Context, s *StepRunner, input []byte) ([]byte, error)
}

// RegisterTask stores taskID → (closure + config) in the in-memory registry.
// Must be called before RunTask. Re-registering the same taskID returns
// ErrTaskAlreadyRegistered. The registry is not persisted; call this again
// after every NewEngine. taskID must not contain path separators or ':'.
func RegisterTask[I, O any](e *Engine, taskID string, task Task[I, O], opts ...TaskOption) error {
	if err := validateTaskID(taskID); err != nil {
		return err
	}

	cfg := taskConfig{tags: make(map[string]string)}
	for _, o := range opts {
		o(&cfg)
	}

	entry := taskEntry{
		cfg: cfg,
		exec: func(ctx context.Context, s *StepRunner, input []byte) ([]byte, error) {
			var in I
			if len(input) > 0 {
				if err := json.Unmarshal(input, &in); err != nil {
					return nil, fmt.Errorf("durable: unmarshal task input: %w", err)
				}
			}
			out, err := task.Exec(ctx, s, in)
			if err != nil {
				return nil, err
			}
			raw, err := json.Marshal(out)
			if err != nil {
				return nil, fmt.Errorf("durable: marshal task output: %w", err)
			}
			return raw, nil
		},
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.registry[taskID]; exists {
		return ErrTaskAlreadyRegistered
	}
	e.registry[taskID] = entry
	return nil
}

func cloneTags(tags map[string]string) map[string]string {
	if tags == nil {
		return map[string]string{}
	}
	return maps.Clone(tags)
}

type runConfig struct {
	maxRetries *int
	timeout    *time.Duration
}

// RunOption configures a single RunTask call.
type RunOption func(*runConfig)

// WithRunTimeout overrides the task and engine timeout for this run.
// An explicit 0 disables a non-zero parent timeout.
func WithRunTimeout(d time.Duration) RunOption {
	return func(c *runConfig) { c.timeout = &d }
}

// WithRunMaxRetries overrides the task and engine task-level retry count
// for this run. An explicit 0 disables a non-zero parent default.
func WithRunMaxRetries(n int) RunOption {
	return func(c *runConfig) { c.maxRetries = &n }
}

func resolveMaxRetries(run, task *int, engine int) int {
	if run != nil {
		return *run
	}
	if task != nil {
		return *task
	}
	return engine
}

func resolveTimeout(run, task *time.Duration, engine time.Duration) time.Duration {
	if run != nil {
		return *run
	}
	if task != nil {
		return *task
	}
	return engine
}

// runHandle is the type-erased state shared by TaskRun and the background
// executor. Status is stored atomically so TaskRun.Status can be polled
// without blocking on Get.
type runHandle struct {
	runID      string
	status     atomic.Value
	done       chan struct{}
	output     []byte
	err        error
	finishOnce sync.Once

	// cancel is the CancelFunc for this run's ctx (the one delivered to
	// Task.Exec/RunStep). Set by RunTask; called by CancelRun when the run
	// is executing in this process. cancelRequested distinguishes a
	// deliberate CancelRun cancellation from engine Close or a task/run
	// timeout so executeRun can report ErrRunCancelled instead of a plain
	// ctx error.
	cancel          context.CancelFunc
	cancelRequested atomic.Bool
}

func newRunHandle(runID string, status TaskStatus) *runHandle {
	h := &runHandle{
		runID: runID,
		done:  make(chan struct{}),
	}
	h.setStatus(status)
	return h
}

func (h *runHandle) setStatus(s TaskStatus) {
	h.status.Store(s)
}

func (h *runHandle) currentStatus() TaskStatus {
	if v := h.status.Load(); v != nil {
		return v.(TaskStatus)
	}
	return ""
}

func (h *runHandle) finish(output []byte, err error) {
	h.finishOnce.Do(func() {
		h.output = output
		h.err = err
		if err != nil {
			h.setStatus(StatusFailed)
		} else {
			h.setStatus(StatusCompleted)
		}
		close(h.done)
	})
}

// TaskRun is the handle returned by RunTask. RunID is available immediately;
// Get blocks until the run reaches a terminal state.
type TaskRun[O any] struct {
	h      *runHandle
	once   sync.Once
	result O
	err    error
}

// RunID returns the resolved runID. Empty if the taskID was not registered
// or the runID was invalid — check Get for the error.
func (r *TaskRun[O]) RunID() string {
	if r.h == nil {
		return ""
	}
	return r.h.runID
}

// Status returns the current run status without blocking.
func (r *TaskRun[O]) Status() TaskStatus {
	if r.h == nil {
		return ""
	}
	return r.h.currentStatus()
}

// Get blocks until the run completes or ctx is cancelled. The typed result
// is cached after the first successful wait so later calls do not re-decode.
func (r *TaskRun[O]) Get(ctx context.Context) (O, error) {
	var zero O
	if r.h == nil {
		return zero, fmt.Errorf("durable: invalid task run")
	}
	select {
	case <-r.h.done:
	case <-ctx.Done():
		return zero, ctx.Err()
	}
	r.once.Do(func() {
		if r.h.err != nil {
			r.err = r.h.err
			return
		}
		if len(r.h.output) == 0 {
			return
		}
		if err := json.Unmarshal(r.h.output, &r.result); err != nil {
			r.err = fmt.Errorf("durable: unmarshal task output: %w", err)
		}
	})
	return r.result, r.err
}

func failedTaskRun[O any](err error) *TaskRun[O] {
	h := newRunHandle("", "")
	h.finish(nil, err)
	return &TaskRun[O]{h: h}
}

func finishedTaskRun[O any](runID string, status TaskStatus, output []byte, err error) *TaskRun[O] {
	h := newRunHandle(runID, status)
	h.finish(output, err)
	return &TaskRun[O]{h: h}
}

// RunTask starts or resumes a run in a background goroutine and returns
// immediately. On first start the input is written to input.json. The same
// runID reloads that file and ignores the input argument. Pass an empty
// runID to resume the oldest active run for taskID, or to generate a new
// ULID if none is active. A completed or failed run returns the stored
// result without spawning a goroutine.
func RunTask[I, O any](ctx context.Context, e *Engine, taskID string, runID string, input I, opts ...RunOption) *TaskRun[O] {
	if err := ctx.Err(); err != nil {
		return failedTaskRun[O](err)
	}
	if err := validateTaskID(taskID); err != nil {
		return failedTaskRun[O](err)
	}
	entry, ok := e.lookupTask(taskID)
	if !ok {
		return failedTaskRun[O](ErrTaskNotRegistered)
	}
	if runID != "" {
		if err := validateRunID(runID); err != nil {
			return failedTaskRun[O](err)
		}
	}

	resolved, err := e.resolveRunID(ctx, taskID, runID)
	if err != nil {
		return failedTaskRun[O](err)
	}

	info, exists, err := e.loadMeta(taskID, resolved)
	if err != nil {
		return failedTaskRun[O](err)
	}
	if exists && info.Status == StatusCompleted {
		out, err := e.loadOutput(taskID, resolved)
		if err != nil {
			return finishedTaskRun[O](resolved, StatusCompleted, nil, err)
		}
		return finishedTaskRun[O](resolved, StatusCompleted, out, nil)
	}
	if exists && info.Status == StatusFailed {
		return finishedTaskRun[O](resolved, StatusFailed, nil, errors.New(info.Error))
	}

	rc := runConfig{}
	for _, o := range opts {
		o(&rc)
	}
	timeout := resolveTimeout(rc.timeout, entry.cfg.timeout, e.cfg.timeout)
	maxRetries := resolveMaxRetries(rc.maxRetries, entry.cfg.maxRetries, e.cfg.maxRetries)
	if maxRetries < 0 {
		maxRetries = 0
	}

	inputBytes, err := json.Marshal(input)
	if err != nil {
		return failedTaskRun[O](fmt.Errorf("durable: marshal task input: %w", err))
	}

	h := newRunHandle(resolved, StatusRunning)
	runCtx, cancel := context.WithCancel(context.Background())
	if timeout > 0 {
		runCtx, cancel = context.WithTimeout(context.Background(), timeout)
	}
	h.cancel = cancel
	runKey := taskID + "/" + resolved
	e.runCancels.Store(runKey, h)
	stopWatchDone := make(chan struct{})
	go func() {
		select {
		case <-e.stopCh:
			cancel()
		case <-stopWatchDone:
		}
	}()
	e.runs.Add(1)
	go func() {
		defer e.runs.Done()
		defer close(stopWatchDone)
		defer cancel()
		defer e.runCancels.Delete(runKey)
		mu := e.lockRun(taskID, resolved)
		defer mu.Unlock()
		e.executeRun(runCtx, cancel, taskID, resolved, entry, inputBytes, maxRetries, h)
	}()
	return &TaskRun[O]{h: h}
}

// resolveRunID implements the singleton-active-run rule: an empty runID
// resumes the oldest Running or Waiting run so two callers cannot silently
// start two charges for the same task. A provided runID that is not on
// disk is accepted as-is so callers can use an API request ID as the runID.
func (e *Engine) resolveRunID(ctx context.Context, taskID, runID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if runID != "" {
		return runID, nil
	}
	ids, err := e.scanRuns(taskID)
	if err != nil {
		return "", err
	}
	for _, id := range ids {
		info, ok, err := e.loadMeta(taskID, id)
		if err != nil {
			return "", err
		}
		if ok && (info.Status == StatusRunning || info.Status == StatusWaiting) {
			return id, nil
		}
	}
	return ulid.Make().String(), nil
}

func (e *Engine) executeRun(ctx context.Context, cancel context.CancelFunc, taskID, runID string, entry taskEntry, input []byte, maxRetries int, h *runHandle) {
	info, exists, err := e.loadMeta(taskID, runID)
	if err != nil {
		h.finish(nil, err)
		return
	}
	if exists && info.Status == StatusCompleted {
		out, err := e.loadOutput(taskID, runID)
		h.finish(out, err)
		return
	}
	if exists && info.Status == StatusFailed {
		h.finish(nil, errors.New(info.Error))
		return
	}

	input, err = e.resolveRunInput(taskID, runID, input)
	if err != nil {
		h.finish(nil, err)
		return
	}

	now := time.Now().UTC()
	if !exists {
		info = TaskInfo{
			TaskID:    taskID,
			RunID:     runID,
			Name:      entry.cfg.name,
			Tags:      cloneTags(entry.cfg.tags),
			Status:    StatusRunning,
			CreatedAt: now,
			StartedAt: now,
			UpdatedAt: now,
		}
		if err := e.saveMeta(taskID, runID, info); err != nil {
			h.finish(nil, err)
			return
		}
	} else {
		info.Status = StatusRunning
		info.UpdatedAt = now
		if err := e.saveMeta(taskID, runID, info); err != nil {
			h.finish(nil, err)
			return
		}
	}
	h.setStatus(StatusRunning)

	steps, signals, err := e.loadJournal(taskID, runID)
	if err != nil {
		h.finish(nil, err)
		return
	}

	// A CancelRun call before this process last exited (or before this
	// resume) persisted a cancel signal. Cancel ctx now, before Task.Exec
	// is ever invoked, so every RunStep call fails fast with
	// ErrRunCancelled instead of re-running fn.
	if _, cancelled := signals[cancelSignalID]; cancelled {
		h.cancelRequested.Store(true)
		cancel()
	}

	cache := make(map[string]StepRecord, len(steps))
	for id, rec := range steps {
		switch rec.Status {
		case StepStatusCompleted, StepStatusFailed:
			// FAILED must replay here too — a step that failed before a
			// crash must not re-run fn on resume; RunStep returns the
			// stored error from cache instead.
			cache[id] = rec
		case StepStatusWaiting:
			if payload, ok := signals[id]; ok {
				rec.Status = StepStatusCompleted
				rec.Result = payload
				if rec.CompletedAt.IsZero() {
					rec.CompletedAt = time.Now().UTC()
				}
			}
			cache[id] = rec
		}
	}

	logger := e.cfg.logger.With("task_id", taskID, "run_id", runID)
	logger.Debug("run started", "name", entry.cfg.name, "cached_steps", len(cache))

	var (
		output     []byte
		taskErr    error
		panicTrace string
	)
	for attempt := 0; attempt <= maxRetries; attempt++ {
		runner := &StepRunner{
			taskID:    taskID,
			runID:     runID,
			cache:     cache,
			seenSteps: make(map[string]struct{}),
			logger:    logger,
			engine:    e,
			handle:    h,
		}
		if attempt > 0 {
			logger.Warn("task retrying", "attempt", attempt, "error", taskErr)
		}
		output, panicTrace, taskErr = invokeTask(ctx, runner, entry, input)
		// The task closure has returned, but sibling RunStep goroutines it
		// started and never Get-ed (or started fanning out but returned
		// early on one error) may still be running. Wait for them before
		// deciding the attempt's outcome or retrying — otherwise a retry's
		// fresh StepRunner could race the previous attempt's stragglers on
		// the same journal, and Close/compactJournal could run concurrently
		// with an in-flight append.
		runner.waitInFlight()
		if taskErr == nil {
			break
		}
		if ctx.Err() != nil {
			if h.cancelRequested.Load() {
				taskErr = ErrRunCancelled
			} else {
				taskErr = ctx.Err()
			}
			break
		}
		var stepErr *stepFailedError
		if errors.As(taskErr, &stepErr) {
			break
		}
	}

	now = time.Now().UTC()
	info, _, _ = e.loadMeta(taskID, runID)
	info.TaskID = taskID
	info.RunID = runID
	info.UpdatedAt = now
	info.CompletedAt = now
	info.PanicTrace = panicTrace

	if taskErr != nil {
		info.Status = StatusFailed
		info.Error = taskErr.Error()
		_ = e.saveMeta(taskID, runID, info)
		_ = e.compactJournal(taskID, runID)
		if panicTrace != "" {
			logger.Error("task panicked", "panic_trace", panicTrace)
		} else {
			logger.Error("task failed", "error", taskErr)
		}
		h.finish(nil, taskErr)
		return
	}

	if err := e.saveOutput(taskID, runID, output); err != nil {
		h.finish(nil, err)
		return
	}
	info.Status = StatusCompleted
	info.Error = ""
	info.PanicTrace = ""
	if err := e.saveMeta(taskID, runID, info); err != nil {
		h.finish(nil, err)
		return
	}
	if err := e.compactJournal(taskID, runID); err != nil {
		logger.Error("compact journal failed", "error", err)
	}
	logger.Info("task completed", "duration", info.CompletedAt.Sub(info.StartedAt))
	h.finish(output, nil)
}

func invokeTask(ctx context.Context, s *StepRunner, entry taskEntry, input []byte) (output []byte, panicTrace string, taskErr error) {
	defer func() {
		if r := recover(); r != nil {
			panicTrace = fmt.Sprintf("%v\n%s", r, debug.Stack())
			taskErr = fmt.Errorf("durable: task panicked: %v", r)
		}
	}()
	output, taskErr = entry.exec(ctx, s, input)
	return output, panicTrace, taskErr
}

// ListTasks returns TaskInfo records. Zero args returns every status.
// Pass explicit statuses to filter, e.g. ListTasks(ctx, StatusRunning,
// StatusWaiting) for recovery after a restart.
func (e *Engine) ListTasks(ctx context.Context, statuses ...TaskStatus) ([]TaskInfo, error) {
	return listTasks(ctx, e.dataDir, statuses...)
}

// GetTask returns a single run's metadata. (zero, false, nil) if not found.
func (e *Engine) GetTask(ctx context.Context, taskID, runID string) (TaskInfo, bool, error) {
	if err := ctx.Err(); err != nil {
		return TaskInfo{}, false, err
	}
	if err := validateTaskID(taskID); err != nil {
		return TaskInfo{}, false, err
	}
	if err := validateRunID(runID); err != nil {
		return TaskInfo{}, false, err
	}
	return loadMeta(e.dataDir, taskID, runID)
}

// LoadInput returns the JSON-encoded task input written on first RunTask.
// (nil, false, nil) if input.json is missing.
func (e *Engine) LoadInput(ctx context.Context, taskID, runID string) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := validateTaskID(taskID); err != nil {
		return nil, false, err
	}
	if err := validateRunID(runID); err != nil {
		return nil, false, err
	}
	return loadInput(e.dataDir, taskID, runID)
}

// LoadSteps returns all StepRecords for a run. Used to inspect progress.
// Includes waiting, completed, and failed steps. Order is not sorted or
// otherwise guaranteed — it reflects map iteration order internally. Use
// WatchSteps if you need append order or history.
func (e *Engine) LoadSteps(ctx context.Context, taskID, runID string) ([]StepRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateTaskID(taskID); err != nil {
		return nil, err
	}
	if err := validateRunID(runID); err != nil {
		return nil, err
	}
	return loadStepRecords(e.dataDir, taskID, runID)
}

// GetStep returns the latest StepRecord for one stepID in O(1) after one
// journal load — (zero, false, nil) if the run has no record for that
// stepID yet (including one stuck mid-execution: an unfinished STARTED
// step never appears here — see loadJournal).
func (e *Engine) GetStep(ctx context.Context, taskID, runID, stepID string) (StepRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return StepRecord{}, false, err
	}
	if err := validateTaskID(taskID); err != nil {
		return StepRecord{}, false, err
	}
	if err := validateRunID(runID); err != nil {
		return StepRecord{}, false, err
	}
	if stepID == "" {
		return StepRecord{}, false, fmt.Errorf("durable: step ID must not be empty")
	}
	steps, _, err := e.loadJournal(taskID, runID)
	if err != nil {
		return StepRecord{}, false, err
	}
	rec, ok := steps[stepID]
	return rec, ok, nil
}

// GetStep returns the latest StepRecord for one stepID. Same semantics as
// Engine.GetStep.
func (r *ReadOnlyEngine) GetStep(ctx context.Context, taskID, runID, stepID string) (StepRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return StepRecord{}, false, err
	}
	if err := validateTaskID(taskID); err != nil {
		return StepRecord{}, false, err
	}
	if err := validateRunID(runID); err != nil {
		return StepRecord{}, false, err
	}
	if stepID == "" {
		return StepRecord{}, false, fmt.Errorf("durable: step ID must not be empty")
	}
	steps, _, err := loadJournal(r.dataDir, taskID, runID)
	if err != nil {
		return StepRecord{}, false, err
	}
	rec, ok := steps[stepID]
	return rec, ok, nil
}

const watchStepsBuf = 64

// StepEvent is one journal entry delivered by WatchSteps: STARTED (running),
// WAITING, COMPLETED, or FAILED — every append, in journal order (never
// last-write-wins). Offset is the 1-based position of this event across the
// run's whole journal (steps and signals share one counter); ByteOffset is
// the file position immediately after it. Reconnect with either value as
// fromOffset/fromByteOffset to resume a watch without missing or repeating
// events; byteOffset skips a full rescan, offset does not.
type StepEvent struct {
	StepRecord
	Offset     int
	ByteOffset int64
}

// WatchSteps streams step lifecycle events for a run: STARTED, WAITING,
// COMPLETED, FAILED — every append, in journal order. fromOffset (event
// count) or fromByteOffset (file position) skip already-seen history;
// fromByteOffset takes priority when non-zero (pass 0 for fromOffset if you
// only have a byte offset). Already-written events after that point are
// sent first, then each new event as it is persisted. The channel closes
// when ctx is cancelled or the engine closes; cancelling the watch does not
// stop the run. A slow consumer may miss live events — a warning is logged
// and delivery continues; reconnect with the last Offset/ByteOffset seen to
// catch up from the journal.
func (e *Engine) WatchSteps(ctx context.Context, taskID, runID string, fromOffset int, fromByteOffset int64) (<-chan StepEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateTaskID(taskID); err != nil {
		return nil, err
	}
	if err := validateRunID(runID); err != nil {
		return nil, err
	}
	if _, ok, err := e.loadMeta(taskID, runID); err != nil {
		return nil, err
	} else if !ok {
		return nil, fmt.Errorf("durable: run %s/%s not found", taskID, runID)
	}

	notify := make(chan StepEvent, watchStepsBuf)
	e.addStepWatcher(taskID, runID, notify)
	events, err := e.loadStepEvents(taskID, runID)
	if err != nil {
		e.removeStepWatcher(taskID, runID, notify)
		return nil, err
	}
	out := make(chan StepEvent, watchStepsBuf)
	go e.runStepWatch(ctx, taskID, runID, fromOffset, fromByteOffset, events, notify, out)
	return out, nil
}

// watchStartOffset resolves the initial cursor from either an event-count
// offset or a byte offset (byte offset wins when non-zero): the returned
// cursor is the Offset of the last event the caller has already seen.
func watchStartOffset(fromOffset int, fromByteOffset int64, events []StepEvent) int {
	if fromByteOffset <= 0 {
		return fromOffset
	}
	cursor := 0
	for _, ev := range events {
		if ev.ByteOffset <= fromByteOffset {
			cursor = ev.Offset
		}
	}
	return cursor
}

func (e *Engine) runStepWatch(ctx context.Context, taskID, runID string, fromOffset int, fromByteOffset int64, events []StepEvent, notify, out chan StepEvent) {
	defer close(out)
	defer e.removeStepWatcher(taskID, runID, notify)

	cursor := watchStartOffset(fromOffset, fromByteOffset, events)
	for _, ev := range events {
		if ev.Offset <= cursor {
			continue
		}
		cursor = ev.Offset
		if !sendStepWatch(ctx, e.stopCh, out, ev) {
			return
		}
	}

	for {
		select {
		case ev := <-notify:
			if ev.Offset <= cursor {
				continue
			}
			cursor = ev.Offset
			if !sendStepWatch(ctx, e.stopCh, out, ev) {
				return
			}
		case <-ctx.Done():
			return
		case <-e.stopCh:
			return
		}
	}
}

func sendStepWatch(ctx context.Context, stopCh <-chan struct{}, out chan StepEvent, ev StepEvent) bool {
	select {
	case out <- ev:
		return true
	case <-ctx.Done():
		return false
	case <-stopCh:
		return false
	}
}

func (e *Engine) addStepWatcher(taskID, runID string, ch chan StepEvent) {
	key := taskID + "/" + runID
	actual, _ := e.watchers.LoadOrStore(key, &stepWatchSet{chs: make(map[chan StepEvent]struct{})})
	s := actual.(*stepWatchSet)
	s.mu.Lock()
	s.chs[ch] = struct{}{}
	s.mu.Unlock()
}

func (e *Engine) removeStepWatcher(taskID, runID string, ch chan StepEvent) {
	key := taskID + "/" + runID
	v, ok := e.watchers.Load(key)
	if !ok {
		return
	}
	s := v.(*stepWatchSet)
	s.mu.Lock()
	delete(s.chs, ch)
	empty := len(s.chs) == 0
	s.mu.Unlock()
	if empty {
		e.watchers.Delete(key)
	}
}

// notifyStepWatchers fans ev out to every live watcher for this run. A
// watcher whose buffer is full (a slow consumer) is skipped rather than
// blocked — the drop is logged so the consumer knows to reconnect with the
// last Offset/ByteOffset it did see; the channel itself is left open so a
// momentary burst does not force a reconnect.
func (e *Engine) notifyStepWatchers(taskID, runID string, ev StepEvent) {
	v, ok := e.watchers.Load(taskID + "/" + runID)
	if !ok {
		return
	}
	s := v.(*stepWatchSet)
	s.mu.Lock()
	chs := make([]chan StepEvent, 0, len(s.chs))
	for ch := range s.chs {
		chs = append(chs, ch)
	}
	s.mu.Unlock()
	for _, ch := range chs {
		select {
		case ch <- ev:
		default:
			e.cfg.logger.Warn("watch step dropped: consumer too slow",
				"task_id", taskID, "run_id", runID, "step_id", ev.StepID, "offset", ev.Offset)
		}
	}
}

// DeleteTaskRun removes one run directory and its journal. No-op if not found.
// Returns ErrRunActive if the run is currently executing, including while
// blocked in StatusWaiting — runLocks is held for that entire duration.
func (e *Engine) DeleteTaskRun(ctx context.Context, taskID, runID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateTaskID(taskID); err != nil {
		return err
	}
	if err := validateRunID(runID); err != nil {
		return err
	}
	mu, ok := e.tryLockRun(taskID, runID)
	if !ok {
		return ErrRunActive
	}
	defer mu.Unlock()

	e.closeJournal(taskID, runID)
	if err := os.RemoveAll(e.runDir(taskID, runID)); err != nil {
		return err
	}
	e.runLocks.Delete(taskID + "/" + runID)
	e.cfg.logger.Debug("run deleted", "task_id", taskID, "run_id", runID)
	return nil
}

// DeleteTask removes all runs under taskID. Destructive — use for a full
// wipe only. Returns ErrRunActive if any run under taskID is currently
// executing (Running or Waiting in-process).
func (e *Engine) DeleteTask(ctx context.Context, taskID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateTaskID(taskID); err != nil {
		return err
	}

	ids, err := e.scanRuns(taskID)
	if err != nil {
		return err
	}

	held := make([]*sync.Mutex, 0, len(ids))
	for _, id := range ids {
		mu, ok := e.tryLockRun(taskID, id)
		if !ok {
			for _, m := range held {
				m.Unlock()
			}
			return ErrRunActive
		}
		held = append(held, mu)
	}

	for _, id := range ids {
		e.closeJournal(taskID, id)
	}
	if err := os.RemoveAll(e.taskDir(taskID)); err != nil {
		for _, m := range held {
			m.Unlock()
		}
		return err
	}
	for i, id := range ids {
		e.runLocks.Delete(taskID + "/" + id)
		held[i].Unlock()
	}
	e.cfg.logger.Debug("task deleted", "task_id", taskID)
	return nil
}

func listTasks(ctx context.Context, dataDir string, statuses ...TaskStatus) ([]TaskInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	want := make(map[TaskStatus]struct{}, len(statuses))
	for _, s := range statuses {
		want[s] = struct{}{}
	}

	taskEntries, err := os.ReadDir(filepath.Join(dataDir, tasksDirName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var out []TaskInfo
	for _, te := range taskEntries {
		if !te.IsDir() {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		taskID := te.Name()
		runs, err := scanRuns(dataDir, taskID)
		if err != nil {
			return nil, err
		}
		for _, runID := range runs {
			info, ok, err := loadMeta(dataDir, taskID, runID)
			if err != nil || !ok {
				continue
			}
			if len(want) > 0 {
				if _, match := want[info.Status]; !match {
					continue
				}
			}
			out = append(out, info)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		if out[i].TaskID != out[j].TaskID {
			return out[i].TaskID < out[j].TaskID
		}
		return out[i].RunID < out[j].RunID
	})
	return out, nil
}

// ListTasks returns TaskInfo records with the same filter semantics as Engine.ListTasks.
func (r *ReadOnlyEngine) ListTasks(ctx context.Context, statuses ...TaskStatus) ([]TaskInfo, error) {
	return listTasks(ctx, r.dataDir, statuses...)
}

// GetTask returns a single run's metadata. (zero, false, nil) if not found.
func (r *ReadOnlyEngine) GetTask(ctx context.Context, taskID, runID string) (TaskInfo, bool, error) {
	if err := ctx.Err(); err != nil {
		return TaskInfo{}, false, err
	}
	if err := validateTaskID(taskID); err != nil {
		return TaskInfo{}, false, err
	}
	if err := validateRunID(runID); err != nil {
		return TaskInfo{}, false, err
	}
	return loadMeta(r.dataDir, taskID, runID)
}

// LoadInput returns the JSON-encoded task input written on first RunTask.
// Same semantics as Engine.LoadInput.
func (r *ReadOnlyEngine) LoadInput(ctx context.Context, taskID, runID string) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := validateTaskID(taskID); err != nil {
		return nil, false, err
	}
	if err := validateRunID(runID); err != nil {
		return nil, false, err
	}
	return loadInput(r.dataDir, taskID, runID)
}

// LoadSteps returns all StepRecords for a run ordered by Seq.
func (r *ReadOnlyEngine) LoadSteps(ctx context.Context, taskID, runID string) ([]StepRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateTaskID(taskID); err != nil {
		return nil, err
	}
	if err := validateRunID(runID); err != nil {
		return nil, err
	}
	return loadStepRecords(r.dataDir, taskID, runID)
}

func loadStepRecords(dataDir, taskID, runID string) ([]StepRecord, error) {
	steps, _, err := loadJournal(dataDir, taskID, runID)
	if err != nil {
		return nil, err
	}
	recs := make([]StepRecord, 0, len(steps))
	for _, rec := range steps {
		recs = append(recs, rec)
	}
	return recs, nil
}
