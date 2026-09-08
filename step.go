package durable

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"
)

// StepStatus is the lifecycle state of a single step checkpoint.
type StepStatus string

const (
	// StepStatusWaiting means the step is suspended and awaiting CompleteStep.
	StepStatusWaiting StepStatus = "waiting"
	// StepStatusCompleted means the step succeeded and its result is cached.
	StepStatusCompleted StepStatus = "completed"
	// StepStatusFailed means the step returned an error or panicked.
	StepStatusFailed StepStatus = "failed"
)

// StepRecord is the persistent checkpoint for one memoised step.
type StepRecord struct {
	StepID      string
	Seq         int
	InputHash   string // unused in v1; RunStep has no input parameter
	Result      []byte
	Error       string
	PanicTrace  string
	Status      StepStatus
	StartedAt   time.Time
	CompletedAt time.Time
}

type stepConfig struct {
	timeout    *time.Duration
	maxRetries *int
}

// StepOption configures a single RunStep call.
type StepOption func(*stepConfig)

// WithStepTimeout is an inner bound: the step deadline is
// min(task deadline, step timeout). It cannot extend past the task deadline.
func WithStepTimeout(d time.Duration) StepOption {
	return func(c *stepConfig) { c.timeout = &d }
}

// WithStepMaxRetries sets how many times this step's function is re-invoked
// on a non-panic, non-ErrStepPending error. Default is 0 (one attempt).
// Does not inherit from task or engine retry settings.
func WithStepMaxRetries(n int) StepOption {
	return func(c *stepConfig) { c.maxRetries = &n }
}

// StepRun is the handle returned by RunStep. Get returns immediately —
// RunStep itself is synchronous (including the wait on ErrStepPending).
type StepRun[O any] struct {
	stepID string
	result O
	err    error
}

// StepID returns the stepID this handle corresponds to.
func (r *StepRun[O]) StepID() string { return r.stepID }

// Get returns the step result. ctx is reserved for future async support;
// the result is already available because RunStep waits before returning.
func (r *StepRun[O]) Get(ctx context.Context) (O, error) {
	// ctx is reserved for future async step execution. RunStep is
	// synchronous today, so the result is already available.
	_ = ctx
	return r.result, r.err
}

// StepRunner is scoped to a single run and provides RunStep, StepToken, and
// run-context accessors. It is not safe for concurrent use.
type StepRunner struct {
	taskID        string
	runID         string
	cache         map[string]StepRecord
	seenSteps     map[string]struct{}
	seq           int
	currentStepID string
	inUse         atomic.Bool
	logger        *slog.Logger
	engine        *Engine
	handle        *runHandle
}

// TaskID returns the taskID of the current run.
func (s *StepRunner) TaskID() string { return s.taskID }

// RunID returns the runID of the current run.
func (s *StepRunner) RunID() string { return s.runID }

// StepSeq returns the number of steps executed so far. Zero before the first
// RunStep call; incremented after each RunStep returns (including cache hits).
func (s *StepRunner) StepSeq() int { return s.seq }

// Logger returns the engine logger pre-scoped with taskID and runID.
func (s *StepRunner) Logger() *slog.Logger { return s.logger }

// StepToken returns an opaque token encoding taskID, runID, and the current
// stepID. Must be called inside a RunStep function — panics if currentStepID
// is empty. Pass the token to an external caller (DB, email, webhook URL)
// so they can later call CompleteStep.
func (s *StepRunner) StepToken() string {
	if s.currentStepID == "" {
		panic("durable: StepToken must be called inside a RunStep function")
	}
	return encodeStepToken(s.taskID, s.runID, s.currentStepID)
}

// stepFailedError marks an error that originated from a step so the task
// retry loop does not re-invoke the whole closure — the step already spent
// its own retry budget.
type stepFailedError struct {
	stepID string
	err    error
}

func (e *stepFailedError) Error() string { return e.err.Error() }
func (e *stepFailedError) Unwrap() error { return e.err }

// RunStep executes fn as a memoised checkpoint. On a completed cache hit,
// fn is not called. On a miss, fn runs synchronously and the result is
// persisted. stepID must be unique within the run — a duplicate panics.
// Concurrent calls on the same StepRunner panic.
func RunStep[O any](ctx context.Context, s *StepRunner, stepID string, fn func(ctx context.Context) (O, error), opts ...StepOption) *StepRun[O] {
	failed := func(err error) *StepRun[O] {
		return &StepRun[O]{stepID: stepID, err: err}
	}

	if stepID == "" {
		panic("durable: step ID must not be empty")
	}
	if !s.inUse.CompareAndSwap(false, true) {
		panic("durable: concurrent RunStep calls on the same StepRunner; collect results then call RunStep sequentially")
	}
	defer s.inUse.Store(false)

	if _, dup := s.seenSteps[stepID]; dup {
		panic(fmt.Sprintf("durable: duplicate step ID %q in run %s/%s", stepID, s.taskID, s.runID))
	}
	s.seenSteps[stepID] = struct{}{}

	cfg := stepConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	if rec, ok := s.cache[stepID]; ok && rec.Status == StepStatusCompleted {
		var out O
		if err := json.Unmarshal(rec.Result, &out); err != nil {
			s.finishStep()
			return failed(fmt.Errorf("durable: replay step %q: unmarshal: %w", stepID, err))
		}
		s.logger.Debug("step replayed from cache", "step_id", stepID, "seq", s.seq+1)
		s.finishStep()
		return &StepRun[O]{stepID: stepID, result: out}
	}

	if rec, ok := s.cache[stepID]; ok && rec.Status == StepStatusWaiting {
		s.currentStepID = stepID
		out, err := waitForSignal[O](ctx, s, stepID, rec.StartedAt)
		s.currentStepID = ""
		s.finishStep()
		if err != nil {
			return failed(err)
		}
		return &StepRun[O]{stepID: stepID, result: out}
	}

	s.currentStepID = stepID
	defer func() { s.currentStepID = "" }()

	stepCtx := ctx
	var cancel context.CancelFunc
	if cfg.timeout != nil && *cfg.timeout > 0 {
		stepCtx, cancel = context.WithTimeout(ctx, *cfg.timeout)
		defer cancel()
	}

	maxRetries := 0
	if cfg.maxRetries != nil && *cfg.maxRetries > 0 {
		maxRetries = *cfg.maxRetries
	}

	startedAt := time.Now().UTC()
	var (
		out        O
		fnErr      error
		panicTrace string
	)

	for attempt := 0; attempt <= maxRetries; attempt++ {
		out, panicTrace, fnErr = invokeStep(stepCtx, stepID, fn)
		if panicTrace != "" {
			s.persistFailed(stepID, startedAt, fnErr, panicTrace)
			s.finishStep()
			return failed(&stepFailedError{stepID: stepID, err: fnErr})
		}
		if errors.Is(fnErr, ErrStepPending) {
			out, fnErr = enterPending[O](stepCtx, s, stepID, startedAt)
			s.finishStep()
			if fnErr != nil {
				return failed(fnErr)
			}
			return &StepRun[O]{stepID: stepID, result: out}
		}
		if fnErr != nil {
			if stepCtx.Err() != nil || attempt == maxRetries {
				s.persistFailed(stepID, startedAt, fnErr, "")
				s.finishStep()
				return failed(&stepFailedError{stepID: stepID, err: fnErr})
			}
			s.logger.Warn("step retrying", "step_id", stepID, "attempt", attempt+1, "error", fnErr)
			continue
		}
		break
	}

	raw, err := json.Marshal(out)
	if err != nil {
		s.finishStep()
		return failed(fmt.Errorf("durable: step %q: marshal result: %w", stepID, err))
	}
	completedAt := time.Now().UTC()
	rec := StepRecord{
		StepID:      stepID,
		Seq:         s.seq + 1,
		Status:      StepStatusCompleted,
		Result:      raw,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
	}
	if err := s.engine.appendStep(s.taskID, s.runID, rec); err != nil {
		s.finishStep()
		return failed(err)
	}
	s.cache[stepID] = rec
	s.logger.Debug("step completed", "step_id", stepID, "seq", rec.Seq, "duration", completedAt.Sub(startedAt))
	s.finishStep()
	return &StepRun[O]{stepID: stepID, result: out}
}

func (s *StepRunner) finishStep() {
	s.seq++
}

func invokeStep[O any](ctx context.Context, stepID string, fn func(context.Context) (O, error)) (out O, panicTrace string, fnErr error) {
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicTrace = fmt.Sprintf("%v\n%s", r, debug.Stack())
				fnErr = fmt.Errorf("durable: step %q panicked: %v", stepID, r)
			}
		}()
		out, fnErr = fn(ctx)
	}()
	return out, panicTrace, fnErr
}

func (s *StepRunner) persistFailed(stepID string, startedAt time.Time, fnErr error, panicTrace string) {
	rec := StepRecord{
		StepID:      stepID,
		Seq:         s.seq + 1,
		Status:      StepStatusFailed,
		Error:       fnErr.Error(),
		PanicTrace:  panicTrace,
		StartedAt:   startedAt,
		CompletedAt: time.Now().UTC(),
	}
	if err := s.engine.appendStep(s.taskID, s.runID, rec); err != nil {
		s.logger.Error("save failed step failed", "step_id", stepID, "error", err)
	}
	if panicTrace != "" {
		s.logger.Error("step panicked", "step_id", stepID, "panic_trace", panicTrace)
	} else {
		s.logger.Error("step failed", "step_id", stepID, "error", fnErr)
	}
}

func enterPending[O any](ctx context.Context, s *StepRunner, stepID string, startedAt time.Time) (O, error) {
	var zero O
	waiting := StepRecord{
		StepID:    stepID,
		Seq:       s.seq + 1,
		Status:    StepStatusWaiting,
		StartedAt: startedAt,
	}
	if err := s.engine.appendStep(s.taskID, s.runID, waiting); err != nil {
		return zero, err
	}
	s.cache[stepID] = waiting
	if err := s.setRunStatus(StatusWaiting); err != nil {
		return zero, err
	}
	s.logger.Info("step waiting", "step_id", stepID)
	return waitForSignal[O](ctx, s, stepID, startedAt)
}

func waitForSignal[O any](ctx context.Context, s *StepRunner, stepID string, startedAt time.Time) (O, error) {
	var zero O
	key := s.taskID + "/" + s.runID + "/" + stepID
	ch := make(chan []byte, 1)
	s.engine.signals.Store(key, ch)
	defer s.engine.signals.Delete(key)

	// CompleteStep may have written the SignalEntry between our WAITING
	// append and channel registration. Re-read the journal before blocking
	// so that payload is not lost.
	_, signals, err := s.engine.loadJournal(s.taskID, s.runID)
	if err != nil {
		return zero, err
	}
	payload, ok := signals[stepID]
	if !ok {
		select {
		case payload = <-ch:
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-s.engine.stopCh:
			return zero, fmt.Errorf("durable: engine closed")
		}
	}

	var out O
	if err := json.Unmarshal(payload, &out); err != nil {
		return zero, fmt.Errorf("durable: unmarshal signal for step %q: %w", stepID, err)
	}

	completedAt := time.Now().UTC()
	rec := StepRecord{
		StepID:      stepID,
		Seq:         s.seq + 1,
		Status:      StepStatusCompleted,
		Result:      payload,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
	}
	if err := s.engine.appendStep(s.taskID, s.runID, rec); err != nil {
		return zero, err
	}
	s.cache[stepID] = rec
	if err := s.setRunStatus(StatusRunning); err != nil {
		return zero, err
	}
	s.logger.Info("step resumed", "step_id", stepID)
	return out, nil
}

func (s *StepRunner) setRunStatus(status TaskStatus) error {
	info, ok, err := s.engine.loadMeta(s.taskID, s.runID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("durable: meta missing for %s/%s", s.taskID, s.runID)
	}
	info.Status = status
	info.UpdatedAt = time.Now().UTC()
	if err := s.engine.saveMeta(s.taskID, s.runID, info); err != nil {
		return err
	}
	if s.handle != nil {
		s.handle.setStatus(status)
	}
	return nil
}

// encodeStepToken uses base64.RawURLEncoding of "taskID:runID:stepID".
// runID is a ULID (no colons); taskID is rejected if it contains ':'.
// stepID may contain colons because decode uses SplitN(..., 3).
func encodeStepToken(taskID, runID, stepID string) string {
	raw := taskID + ":" + runID + ":" + stepID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeStepToken(token string) (taskID, runID, stepID string, err error) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", "", "", ErrInvalidToken
	}
	parts := strings.SplitN(string(b), ":", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", ErrInvalidToken
	}
	return parts[0], parts[1], parts[2], nil
}

// CompleteStep delivers result to a suspended step identified by token.
// The payload is appended as a SignalEntry (durable) before the in-process
// waiter is signalled, so a crash after this call still resumes on the next
// RunTask. A second call with the same token is a no-op once the SignalEntry
// is on disk. Returns ErrRunAlreadyFinished if the step or run is already
// terminal, and ErrInvalidToken if the token cannot be decoded.
func CompleteStep[O any](ctx context.Context, e *Engine, token string, result O) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	taskID, runID, stepID, err := decodeStepToken(token)
	if err != nil {
		return err
	}

	info, ok, err := e.loadMeta(taskID, runID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("durable: run %s/%s not found", taskID, runID)
	}
	if info.Status == StatusCompleted || info.Status == StatusFailed {
		return ErrRunAlreadyFinished
	}

	cache, signals, err := e.loadJournal(taskID, runID)
	if err != nil {
		return err
	}
	if rec, exists := cache[stepID]; exists && rec.Status == StepStatusCompleted {
		return ErrRunAlreadyFinished
	}
	if _, exists := signals[stepID]; exists {
		return nil
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if err := e.appendSignal(taskID, runID, stepID, raw); err != nil {
		return err
	}

	info.Status = StatusRunning
	info.UpdatedAt = time.Now().UTC()
	if err := e.saveMeta(taskID, runID, info); err != nil {
		return err
	}

	key := taskID + "/" + runID + "/" + stepID
	if v, ok := e.signals.Load(key); ok {
		ch := v.(chan []byte)
		select {
		case ch <- raw:
		default:
		}
	}
	return nil
}
