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

// TaskInfo is the persistent metadata for a single run. Input is intentionally
// omitted — callers own input persistence and reload it on recovery.
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
// immediately. Pass an empty runID to resume the oldest active run for
// taskID, or to generate a new ULID if none is active. Pass an existing
// runID to resume; a completed or failed run returns the stored result
// without spawning a goroutine.
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
		mu := e.lockRun(taskID, resolved)
		defer mu.Unlock()
		e.executeRun(runCtx, taskID, resolved, entry, inputBytes, maxRetries, h)
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

func (e *Engine) executeRun(ctx context.Context, taskID, runID string, entry taskEntry, input []byte, maxRetries int, h *runHandle) {
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
	cache := make(map[string]StepRecord, len(steps))
	for id, rec := range steps {
		if rec.Status == StepStatusCompleted {
			cache[id] = rec
			continue
		}
		if rec.Status == StepStatusWaiting {
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
		if taskErr == nil {
			break
		}
		if ctx.Err() != nil {
			taskErr = ctx.Err()
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

// LoadSteps returns all StepRecords for a run ordered by Seq. Used to
// inspect progress. Includes waiting, completed, and failed steps.
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
	sort.Slice(recs, func(i, j int) bool { return recs[i].Seq < recs[j].Seq })
	return recs, nil
}
