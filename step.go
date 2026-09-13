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
	"sync"
	"time"
)

// StepStatus is the lifecycle state of a single step checkpoint.
type StepStatus string

const (
	// StepStatusRunning means the step function is currently executing.
	// Watch-only: it is superseded by a terminal or waiting status once the
	// step finishes, and is never the record used to replay RunStep/GetStep —
	// an unfinished StepStatusRunning after a crash is treated as missing
	// (fn re-runs), not stuck.
	StepStatusRunning StepStatus = "running"
	// StepStatusWaiting means the step is suspended and awaiting CompleteStep.
	StepStatusWaiting StepStatus = "waiting"
	// StepStatusCompleted means the step succeeded and its result is cached.
	StepStatusCompleted StepStatus = "completed"
	// StepStatusFailed means the step returned an error or panicked.
	StepStatusFailed StepStatus = "failed"
)

// StepRecord is the latest persistent checkpoint for one memoised step.
type StepRecord struct {
	StepID      string
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

type stepIDKey struct{}

func withStepID(ctx context.Context, stepID string) context.Context {
	return context.WithValue(ctx, stepIDKey{}, stepID)
}

func stepIDFromCtx(ctx context.Context) string {
	s, _ := ctx.Value(stepIDKey{}).(string)
	return s
}

// StepRun is the handle returned by RunStep. RunStep starts work and
// returns immediately; Get waits for the result, Done reports readiness
// without blocking so callers can select across several handles (first-of-N).
type StepRun[O any] struct {
	stepID string
	done   chan struct{}
	stopCh <-chan struct{}
	result O
	err    error
}

// StepID returns the stepID this handle corresponds to.
func (r *StepRun[O]) StepID() string { return r.stepID }

// Done reports readiness. It is closed once the step completes or fails —
// on a cache hit it is already closed when RunStep returns. Use it in a
// select across multiple StepRun handles to react to whichever finishes
// first, then call Get to retrieve that handle's result or error.
func (r *StepRun[O]) Done() <-chan struct{} { return r.done }

// Get blocks until the step completes or ctx is cancelled. The result is
// already available on a cache hit.
func (r *StepRun[O]) Get(ctx context.Context) (O, error) {
	var zero O
	if r == nil || r.done == nil {
		return zero, fmt.Errorf("durable: invalid step run")
	}
	select {
	case <-r.done:
		return r.result, r.err
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-r.stopCh:
		return zero, fmt.Errorf("durable: engine closed")
	}
}

func readyStepRun[O any](stepID string, stopCh <-chan struct{}, result O, err error) *StepRun[O] {
	r := &StepRun[O]{stepID: stepID, done: make(chan struct{}), stopCh: stopCh, result: result, err: err}
	close(r.done)
	return r
}

// StepRunner is scoped to a single run and provides RunStep, StepToken, and
// run-context accessors. Concurrent RunStep calls are safe: fan out work by
// calling RunStep multiple times before Get-ing any handle.
type StepRunner struct {
	taskID    string
	runID     string
	mu        sync.Mutex
	cache     map[string]StepRecord
	seenSteps map[string]struct{}
	waiting   int
	inFlight  sync.WaitGroup
	logger    *slog.Logger
	engine    *Engine
	handle    *runHandle
}

// TaskID returns the taskID of the current run.
func (s *StepRunner) TaskID() string { return s.taskID }

// RunID returns the runID of the current run.
func (s *StepRunner) RunID() string { return s.runID }

// Logger returns the engine logger pre-scoped with taskID and runID.
func (s *StepRunner) Logger() *slog.Logger { return s.logger }

// StepToken returns an opaque token encoding taskID, runID, and the current
// stepID. Must be called inside a RunStep function with that function's ctx
// (the stepID is carried on ctx, not on the StepRunner, so concurrent steps
// each get their own token). Pass the token to an external caller so they
// can later call CompleteStep.
func (s *StepRunner) StepToken(ctx context.Context) string {
	stepID := stepIDFromCtx(ctx)
	if stepID == "" {
		panic("durable: StepToken must be called inside a RunStep function")
	}
	return encodeStepToken(s.taskID, s.runID, stepID)
}

func (s *StepRunner) waitInFlight() {
	s.inFlight.Wait()
}

func (s *StepRunner) storeCache(rec StepRecord) {
	s.mu.Lock()
	s.cache[rec.StepID] = rec
	s.mu.Unlock()
}

func (s *StepRunner) persistRecord(rec StepRecord) error {
	if err := s.engine.appendStep(s.taskID, s.runID, rec); err != nil {
		return err
	}
	s.storeCache(rec)
	return nil
}

// persistStarted is best-effort: a failure to write the watch-only STARTED
// event must not stop the step from executing fn, and it is never placed in
// s.cache (loadJournal's map skips StepStatusRunning entirely — see journal.go).
func (s *StepRunner) persistStarted(stepID string, startedAt time.Time) {
	rec := StepRecord{
		StepID:    stepID,
		Status:    StepStatusRunning,
		StartedAt: startedAt,
	}
	if err := s.engine.appendStep(s.taskID, s.runID, rec); err != nil {
		s.logger.Warn("persist step started failed", "step_id", stepID, "error", err)
	}
}

func (s *StepRunner) addWaiting() error {
	s.mu.Lock()
	s.waiting++
	s.mu.Unlock()
	return s.setRunStatus(StatusWaiting)
}

// leaveWaiting decrements the waiting count and flips the run back to
// StatusRunning only once no step is left waiting — a sibling still waiting
// keeps the run's status as Waiting so CompleteStep for one step does not
// mask the fact that another is still pending.
func (s *StepRunner) leaveWaiting() error {
	s.mu.Lock()
	if s.waiting > 0 {
		s.waiting--
	}
	n := s.waiting
	s.mu.Unlock()
	if n == 0 {
		return s.setRunStatus(StatusRunning)
	}
	return nil
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

// RunStep starts fn as a memoised checkpoint and returns immediately.
// Get waits for the result. On a completed or failed cache hit, fn is not
// called. On a miss, fn runs on a goroutine and the result is persisted.
// stepID must be unique within the run — a duplicate panics. Concurrent
// calls on the same StepRunner are safe: start several steps, then Get them
// in any order, or select on their Done channels for first-of-N.
//
// Step IDs are the resume key: never rename a stepID once a run has
// started. A renamed step is treated as a new, unrelated step — the old
// result is orphaned and fn runs again under the new name.
func RunStep[O any](ctx context.Context, s *StepRunner, stepID string, fn func(ctx context.Context) (O, error), opts ...StepOption) *StepRun[O] {
	if stepID == "" {
		panic("durable: step ID must not be empty")
	}

	s.mu.Lock()
	if _, dup := s.seenSteps[stepID]; dup {
		s.mu.Unlock()
		panic(fmt.Sprintf("durable: duplicate step ID %q in run %s/%s", stepID, s.taskID, s.runID))
	}
	s.seenSteps[stepID] = struct{}{}
	rec, has := s.cache[stepID]
	s.mu.Unlock()

	stopCh := s.engine.stopCh
	var zero O

	if has && rec.Status == StepStatusCompleted {
		var out O
		if err := json.Unmarshal(rec.Result, &out); err != nil {
			return readyStepRun(stepID, stopCh, zero, fmt.Errorf("durable: replay step %q: unmarshal: %w", stepID, err))
		}
		s.logger.Debug("step replayed from cache", "step_id", stepID)
		return readyStepRun(stepID, stopCh, out, nil)
	}
	if has && rec.Status == StepStatusFailed {
		err := errors.New(rec.Error)
		if rec.Error == "" {
			err = fmt.Errorf("durable: step %q failed", stepID)
		}
		s.logger.Debug("step replayed failed from cache", "step_id", stepID)
		return readyStepRun(stepID, stopCh, zero, &stepFailedError{stepID: stepID, err: err})
	}

	cfg := stepConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	run := &StepRun[O]{stepID: stepID, done: make(chan struct{}), stopCh: stopCh}
	s.inFlight.Add(1)
	go func() {
		defer s.inFlight.Done()
		defer close(run.done)
		run.result, run.err = execStep[O](ctx, s, stepID, fn, cfg, rec, has)
	}()
	return run
}

func execStep[O any](ctx context.Context, s *StepRunner, stepID string, fn func(context.Context) (O, error), cfg stepConfig, rec StepRecord, has bool) (O, error) {
	var zero O
	stepCtx := withStepID(ctx, stepID)
	var cancel context.CancelFunc
	if cfg.timeout != nil && *cfg.timeout > 0 {
		stepCtx, cancel = context.WithTimeout(stepCtx, *cfg.timeout)
		defer cancel()
	}

	if has && rec.Status == StepStatusWaiting {
		if err := s.addWaiting(); err != nil {
			return zero, err
		}
		return waitForSignal[O](stepCtx, s, stepID, rec.StartedAt)
	}

	maxRetries := 0
	if cfg.maxRetries != nil && *cfg.maxRetries > 0 {
		maxRetries = *cfg.maxRetries
	}

	startedAt := time.Now().UTC()
	s.persistStarted(stepID, startedAt)

	var (
		out        O
		fnErr      error
		panicTrace string
	)

	for attempt := 0; attempt <= maxRetries; attempt++ {
		out, panicTrace, fnErr = invokeStep(stepCtx, stepID, fn)
		if panicTrace != "" {
			s.persistFailed(stepID, startedAt, fnErr, panicTrace)
			return zero, &stepFailedError{stepID: stepID, err: fnErr}
		}
		if errors.Is(fnErr, ErrStepPending) {
			return enterPending[O](stepCtx, s, stepID, startedAt)
		}
		if fnErr != nil {
			if stepCtx.Err() != nil || attempt == maxRetries {
				s.persistFailed(stepID, startedAt, fnErr, "")
				return zero, &stepFailedError{stepID: stepID, err: fnErr}
			}
			s.logger.Warn("step retrying", "step_id", stepID, "attempt", attempt+1, "error", fnErr)
			continue
		}
		break
	}

	raw, err := json.Marshal(out)
	if err != nil {
		return zero, fmt.Errorf("durable: step %q: marshal result: %w", stepID, err)
	}
	completedAt := time.Now().UTC()
	completed := StepRecord{
		StepID:      stepID,
		Status:      StepStatusCompleted,
		Result:      raw,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
	}
	if err := s.persistRecord(completed); err != nil {
		return zero, err
	}
	s.logger.Debug("step completed", "step_id", stepID, "duration", completedAt.Sub(startedAt))
	return out, nil
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
		Status:      StepStatusFailed,
		Error:       fnErr.Error(),
		PanicTrace:  panicTrace,
		StartedAt:   startedAt,
		CompletedAt: time.Now().UTC(),
	}
	if err := s.persistRecord(rec); err != nil {
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
		Status:    StepStatusWaiting,
		StartedAt: startedAt,
	}
	if err := s.addWaiting(); err != nil {
		return zero, err
	}
	if err := s.persistRecord(waiting); err != nil {
		_ = s.leaveWaiting()
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
		_ = s.leaveWaiting()
		return zero, err
	}
	payload, ok := signals[stepID]
	if !ok {
		select {
		case payload = <-ch:
		case <-ctx.Done():
			_ = s.leaveWaiting()
			return zero, ctx.Err()
		case <-s.engine.stopCh:
			_ = s.leaveWaiting()
			return zero, fmt.Errorf("durable: engine closed")
		}
	}

	var out O
	if err := json.Unmarshal(payload, &out); err != nil {
		_ = s.leaveWaiting()
		return zero, fmt.Errorf("durable: unmarshal signal for step %q: %w", stepID, err)
	}

	completedAt := time.Now().UTC()
	rec := StepRecord{
		StepID:      stepID,
		Status:      StepStatusCompleted,
		Result:      payload,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
	}
	if err := s.persistRecord(rec); err != nil {
		_ = s.leaveWaiting()
		return zero, err
	}
	if err := s.leaveWaiting(); err != nil {
		return zero, err
	}
	s.logger.Info("step resumed", "step_id", stepID)
	return out, nil
}

// setRunStatus is serialised on s.mu (in addition to guarding cache/seenSteps)
// so concurrent addWaiting/leaveWaiting calls from sibling steps cannot
// interleave their load-modify-save of meta.json and lose an update.
func (s *StepRunner) setRunStatus(status TaskStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
//
// The run's status transition back to StatusRunning is owned by the waiting
// step itself (see leaveWaiting), not by CompleteStep — with several steps
// possibly waiting at once, only the step that actually resumes knows
// whether a sibling is still pending.
//
// Concurrent CompleteStep calls for the same token are serialised on a
// per-(taskID,runID,stepID) lock distinct from the run-execution lock (which
// is held for the entire run, including while blocked waiting — locking it
// here would deadlock). A CompleteStep that loses a race with the run
// reaching a terminal state may append an orphan SignalEntry that is never
// read; this is harmless and does not corrupt the journal.
func CompleteStep[O any](ctx context.Context, e *Engine, token string, result O) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	taskID, runID, stepID, err := decodeStepToken(token)
	if err != nil {
		return err
	}

	mu := e.lockSignal(taskID, runID, stepID)
	defer mu.Unlock()

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
