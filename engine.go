// Package durable provides embeddable durable task execution for a single OS
// process. Register typed tasks, run memoised steps, and resume from a local
// journal after a crash. A step may return ErrStepPending and complete later
// via CompleteStep.
//
//	e, err := durable.NewEngine(ctx, "./data")
//	if err != nil { ... }
//	defer e.Close()
//
//	_ = durable.RegisterTask(e, "process-order", durable.Func(
//	    func(ctx context.Context, s *durable.StepRunner, in OrderInput) (OrderOutput, error) {
//	        charged, err := durable.RunStep(ctx, s, "charge", in, func(ctx context.Context, in OrderInput) (string, error) {
//	            return chargeCard(in)
//	        }).Get(ctx)
//	        if err != nil {
//	            return OrderOutput{}, err
//	        }
//	        return OrderOutput{Result: charged}, nil
//	    },
//	))
//
//	run := durable.RunTask[OrderInput, OrderOutput](ctx, e, "process-order", "", input)
//	output, err := run.Get(ctx)
package durable

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/flock"
)

const (
	defaultLockTimeout       = 2 * time.Second
	defaultAutoPurgeInterval = time.Hour
	maxOpenJournals          = 256
)

type engineConfig struct {
	autoPurgeAge      time.Duration
	autoPurgeInterval time.Duration
	autoPurgeMaxRuns  int
	autoPurgeMaxBytes int64
	maxRetries        int
	timeout           time.Duration
	logger            *slog.Logger
	lockTimeout       time.Duration
	codec             PayloadCodec
	tokenSecret       []byte
	tokenTTL          *time.Duration
	unsignedTokens    bool
	journalMACKey     []byte
}

// Option configures NewEngine and NewReadOnlyEngine.
// Writer-only options (WithAutoPurge, WithMaxRetries, WithTimeout,
// WithStepTokenKey, WithDefaultStepTokenTTL) are ignored by
// NewReadOnlyEngine.
type Option func(*engineConfig, *readOnlyConfig)

// WithAutoPurge starts a background goroutine that deletes Completed and
// Failed runs whose UpdatedAt is older than age. One pass runs at
// NewEngine (so a process that restarts more often than interval still
// purges). The optional interval controls later ticks; it defaults to one
// hour. Running and Waiting runs are never purged. Combine with
// WithAutoPurgeMaxRuns / WithAutoPurgeMaxBytes to cap disk. Ignored by
// NewReadOnlyEngine.
func WithAutoPurge(age time.Duration, interval ...time.Duration) Option {
	return func(c *engineConfig, _ *readOnlyConfig) {
		if c == nil {
			return
		}
		c.autoPurgeAge = age
		if len(interval) > 0 {
			c.autoPurgeInterval = interval[0]
		}
	}
}

// WithAutoPurgeMaxRuns deletes the oldest Completed/Failed runs when the
// number of those terminal runs exceeds n. Running and Waiting runs are
// never counted or removed. Zero (default) means no count cap. Starts the
// purger even when WithAutoPurge is omitted. Ignored by NewReadOnlyEngine.
func WithAutoPurgeMaxRuns(n int) Option {
	return func(c *engineConfig, _ *readOnlyConfig) {
		if c != nil && n > 0 {
			c.autoPurgeMaxRuns = n
		}
	}
}

// WithAutoPurgeMaxBytes deletes the oldest Completed/Failed runs when
// terminal-run directories exceed n bytes on disk. Zero (default) means no
// size cap. Starts the purger even when WithAutoPurge is omitted. Ignored
// by NewReadOnlyEngine.
func WithAutoPurgeMaxBytes(n int64) Option {
	return func(c *engineConfig, _ *readOnlyConfig) {
		if c != nil && n > 0 {
			c.autoPurgeMaxBytes = n
		}
	}
}

// WithMaxRetries sets the engine-wide default for task-level retries
// (re-invoking the task closure). Default is 0 — retries are opt-in so
// non-idempotent work is not silently repeated. Overridden by
// WithTaskMaxRetries and WithRunMaxRetries. Ignored by NewReadOnlyEngine.
func WithMaxRetries(n int) Option {
	return func(c *engineConfig, _ *readOnlyConfig) {
		if c != nil {
			c.maxRetries = n
		}
	}
}

// WithTimeout sets the engine-wide default task deadline. Zero (default)
// means no timeout. Overridden by WithTaskTimeout and WithRunTimeout.
// Ignored by NewReadOnlyEngine.
func WithTimeout(d time.Duration) Option {
	return func(c *engineConfig, _ *readOnlyConfig) {
		if c != nil {
			c.timeout = d
		}
	}
}

// WithLogger sets the slog.Logger for NewEngine and NewReadOnlyEngine.
// If nil or omitted, a discard logger is used.
func WithLogger(l *slog.Logger) Option {
	return func(e *engineConfig, r *readOnlyConfig) {
		if l == nil {
			return
		}
		if e != nil {
			e.logger = l
		}
		if r != nil {
			r.logger = l
		}
	}
}

// WithLockTimeout sets how long NewEngine waits for the exclusive flock
// and NewReadOnlyEngine waits for the shared flock. Default is 2 seconds.
func WithLockTimeout(d time.Duration) Option {
	return func(e *engineConfig, r *readOnlyConfig) {
		if e != nil {
			e.lockTimeout = d
		}
		if r != nil {
			r.lockTimeout = d
		}
	}
}

// WithJournalMACKey signs journal.log frames and the run sidecar files
// (input.json, output.json, meta.json) with HMAC-SHA256. Frame MACs are
// bound to taskID, runID, and the 1-based frame index (journal v2) so a
// journal.log cannot be copied between runs or have its frames reordered.
// Sidecar MACs are bound to taskID/runID. Omit it (or pass nil/empty) to
// keep CRC32 journal trailers and unsigned JSON sidecars, the default.
// Enabling a MAC on an existing CRC tree is not a migrate. The key is
// copied and never written under dataDir. Required on NewReadOnlyEngine
// to read a MAC-signed tree.
func WithJournalMACKey(key []byte) Option {
	return func(e *engineConfig, r *readOnlyConfig) {
		if len(key) == 0 {
			return
		}
		owned := append([]byte(nil), key...)
		if e != nil {
			e.journalMACKey = owned
		}
		if r != nil {
			r.journalMACKey = owned
		}
	}
}

// Engine is the process-level entry point for durable task execution.
// Create one per dataDir via NewEngine. Safe for concurrent use.
//
// Always call Close to cancel in-flight runs, release the exclusive flock,
// stop the purger, and close journal file handles.
type Engine struct {
	dataDir     string
	cfg         engineConfig
	lockFile    *flock.Flock
	registry    map[string]taskEntry
	mu          sync.RWMutex
	runLocks    sync.Map // "taskID/runID" → *runLock, held for a run's whole execution
	signalLocks sync.Map // "taskID/runID/stepID" → *sync.Mutex, held only inside CompleteStep and CancelRun
	openFiles   sync.Map // "taskID/runID" → *journalFile
	signals     sync.Map // "taskID/runID/stepID" → chan []byte
	watchers    sync.Map // "taskID/runID" → *stepWatchSet
	runCancels  sync.Map // "taskID/runID" → *runHandle, present only while the run executes in this process
	openCount   atomic.Int32
	stopCh      chan struct{}
	closed      atomic.Bool
	runs        sync.WaitGroup // in-flight RunTask executors and auto-purge
	closeOnce   sync.Once
}

// runLock is a refcounted mutex with a tombstone so DeleteTaskRun cannot
// drop the map entry while another goroutine is blocked on the same lock —
// that would let LoadOrStore hand out a second mutex and two owners.
type runLock struct {
	mu      sync.Mutex
	refs    atomic.Int32
	deleted atomic.Bool
}

// cancelSignalID is a reserved SignalEntry ID used by CancelRun to persist a
// durable cancel intent via the same journal plumbing as CompleteStep. It
// starts with a NUL byte so it can never collide with a real, user-supplied
// stepID (RunStep panics if a caller tries to use it). The payload content
// is never read back — only the signal's presence in the journal matters.
const cancelSignalID = "\x00cancel"

var cancelSignalPayload = []byte("true")

// stepWatchSet is the in-process fan-out for WatchSteps. appendStep wakes
// every subscriber after the journal write succeeds.
type stepWatchSet struct {
	mu  sync.Mutex
	chs map[chan StepEvent]struct{}
}

// NewEngine opens or creates dataDir, acquires an exclusive flock on
// <dataDir>/.lock, and initialises the in-memory task registry. Fails with
// ErrEngineLocked if another Engine or ReadOnlyEngine holds the directory.
func NewEngine(ctx context.Context, dataDir string, opts ...Option) (*Engine, error) {
	if ctx == nil {
		return nil, fmt.Errorf("durable: context must not be nil")
	}
	if dataDir == "" {
		return nil, fmt.Errorf("durable: dataDir must not be empty")
	}

	cfg := engineConfig{
		logger:      slog.New(slog.DiscardHandler),
		lockTimeout: defaultLockTimeout,
	}
	for _, o := range opts {
		o(&cfg, nil)
	}
	if err := cfg.resolveStepTokenKey(); err != nil {
		return nil, err
	}

	absDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("durable: resolve dataDir: %w", err)
	}
	if err := mkdirAllSecure(filepath.Join(absDir, tasksDirName)); err != nil {
		return nil, fmt.Errorf("durable: create dataDir %q: %w", absDir, err)
	}

	if err := occupyExclusive(absDir); err != nil {
		return nil, err
	}

	lockFile, err := acquireLock(ctx, lockPath(absDir), true, cfg.lockTimeout)
	if err != nil {
		releaseOccupancy(absDir, true)
		return nil, err
	}
	chmodBestEffort(lockPath(absDir), filePerm)

	// Lock first so a concurrent writer cannot race the chmod walk or the
	// writable probe. The recursive walk is skipped once .perms_ok exists.
	ensureDataDirPerms(absDir)
	if err := ensureWritable(absDir); err != nil {
		_ = releaseLock(lockFile)
		releaseOccupancy(absDir, true)
		return nil, err
	}
	warnIfInsecure(absDir, cfg.logger)
	sweepTmpFiles(absDir, cfg.logger)
	sweepOrphanRunDirs(absDir, cfg.logger)

	e := &Engine{
		dataDir:  absDir,
		cfg:      cfg,
		lockFile: lockFile,
		registry: make(map[string]taskEntry),
		stopCh:   make(chan struct{}),
	}
	if e.shouldAutoPurge() {
		e.runs.Add(1)
		go e.runAutoPurge()
	}
	e.cfg.logger.Debug("engine opened", "data_dir", absDir)
	return e, nil
}

func ensureWritable(dir string) error {
	path := filepath.Join(dir, fmt.Sprintf(".perm_test.%d", os.Getpid()))
	f, err := openFileSecure(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return fmt.Errorf("durable: dataDir %q is not writable: %w", dir, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("durable: dataDir %q is not writable: %w", dir, err)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("durable: dataDir %q is not writable (cleanup failed): %w", dir, err)
	}
	return nil
}

// Close cancels in-flight runs, waits for them and the auto-purger to
// finish writing, then releases the exclusive flock and journal handles.
// Safe to call more than once.
//
// Cancelling ctx only signals — it does not forcibly stop a step function.
// Close waits for every in-flight RunStep goroutine to actually return
// (see StepRunner.waitInFlight) so a step's own journal append can never
// race compactJournal or a second Engine opening the same dataDir. If a
// step function never checks ctx and never returns, Close blocks forever
// and the exclusive flock on dataDir is never released. Write step
// functions so they select on ctx (or pass it to ctx-aware calls like
// http.NewRequestWithContext) instead of running unconditionally to
// completion.
func (e *Engine) Close() error {
	var err error
	e.closeOnce.Do(func() {
		e.mu.Lock()
		e.closed.Store(true)
		close(e.stopCh)
		e.mu.Unlock()
		e.runs.Wait()
		e.closeAllJournals()
		err = releaseLock(e.lockFile)
		releaseOccupancy(e.dataDir, true)
		e.cfg.logger.Debug("engine closed", "data_dir", e.dataDir)
	})
	return err
}

func (e *Engine) shouldAutoPurge() bool {
	return e.cfg.autoPurgeAge > 0 || e.cfg.autoPurgeMaxRuns > 0 || e.cfg.autoPurgeMaxBytes > 0
}

// runAutoPurge is the long-lived goroutine started when WithAutoPurge is set.
// It exits when stopCh is closed by Close so the process can shut down
// without leaking the ticker.
func (e *Engine) runAutoPurge() {
	defer e.runs.Done()
	e.runPurgePass()
	interval := e.cfg.autoPurgeInterval
	if interval <= 0 {
		interval = defaultAutoPurgeInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.runPurgePass()
		case <-e.stopCh:
			return
		}
	}
}

func (e *Engine) runPurgePass() {
	if err := e.purge(); err != nil {
		e.cfg.logger.Error("auto-purge failed", "error", err)
	}
}

// purge deletes Completed and Failed runs that are older than autoPurgeAge
// and, if caps are set, the oldest terminal runs until the remaining
// terminal-run count and on-disk size are under the configured limits.
// Running and Waiting runs are never purged. Walks the tree once instead
// of materialising a fully sorted ListTasks snapshot.
func (e *Engine) purge() error {
	age := e.cfg.autoPurgeAge
	var cutoff time.Time
	if age > 0 {
		cutoff = time.Now().UTC().Add(-age)
	}
	e.cfg.logger.Debug("auto-purge running", "cutoff", cutoff)

	type termRun struct {
		info TaskInfo
		size int64
	}
	var terminal []termRun
	var termBytes int64
	err := walkRuns(e.dataDir, e.journalMACKey(), func(info TaskInfo, size int64) error {
		if info.Status != StatusCompleted && info.Status != StatusFailed {
			return nil
		}
		terminal = append(terminal, termRun{info: info, size: size})
		termBytes += size
		return nil
	})
	if err != nil {
		return err
	}

	sort.Slice(terminal, func(i, j int) bool {
		if !terminal[i].info.UpdatedAt.Equal(terminal[j].info.UpdatedAt) {
			return terminal[i].info.UpdatedAt.Before(terminal[j].info.UpdatedAt)
		}
		return terminal[i].info.RunID < terminal[j].info.RunID
	})

	remain := len(terminal)
	var n int
	for _, t := range terminal {
		drop := age > 0 && t.info.UpdatedAt.Before(cutoff)
		if !drop && e.cfg.autoPurgeMaxRuns > 0 && remain > e.cfg.autoPurgeMaxRuns {
			drop = true
		}
		if !drop && e.cfg.autoPurgeMaxBytes > 0 && termBytes > e.cfg.autoPurgeMaxBytes {
			drop = true
		}
		if !drop {
			continue
		}
		if err := e.DeleteTaskRun(context.Background(), t.info.TaskID, t.info.RunID); err != nil {
			if errors.Is(err, ErrRunActive) {
				continue
			}
			e.cfg.logger.Error("auto-purge delete failed", "task_id", t.info.TaskID, "run_id", t.info.RunID, "error", err)
			continue
		}
		n++
		remain--
		termBytes -= t.size
	}
	e.cfg.logger.Info("auto-purge completed", "purged", n, "cutoff", cutoff)
	return nil
}

func (e *Engine) lockRun(taskID, runID string) (*runLock, error) {
	l, ok := e.acquireRunLock(taskID, runID, false)
	if !ok {
		return nil, ErrRunActive
	}
	return l, nil
}

func (e *Engine) tryLockRun(taskID, runID string) (*runLock, bool) {
	return e.acquireRunLock(taskID, runID, true)
}

func (e *Engine) acquireRunLock(taskID, runID string, try bool) (*runLock, bool) {
	key := taskID + "/" + runID
	actual, _ := e.runLocks.LoadOrStore(key, &runLock{})
	l := actual.(*runLock)
	l.refs.Add(1)
	if try {
		if !l.mu.TryLock() {
			e.releaseRunLockRef(key, l)
			return nil, false
		}
	} else {
		l.mu.Lock()
	}
	if l.deleted.Load() {
		l.mu.Unlock()
		e.releaseRunLockRef(key, l)
		return nil, false
	}
	return l, true
}

func (e *Engine) unlockRun(taskID, runID string, l *runLock) {
	key := taskID + "/" + runID
	l.mu.Unlock()
	e.releaseRunLockRef(key, l)
}

func (e *Engine) markRunDeleted(l *runLock) {
	l.deleted.Store(true)
}

func (e *Engine) releaseRunLockRef(key string, l *runLock) {
	if l.refs.Add(-1) == 0 && l.deleted.Load() {
		e.runLocks.Delete(key)
	}
}

func (e *Engine) dropIdleRunLock(taskID, runID string) {
	key := taskID + "/" + runID
	v, ok := e.runLocks.Load(key)
	if !ok {
		return
	}
	l := v.(*runLock)
	if l.refs.Load() != 0 {
		return
	}
	if !l.mu.TryLock() {
		return
	}
	if l.refs.Load() != 0 {
		l.mu.Unlock()
		return
	}
	e.runLocks.Delete(key)
	l.mu.Unlock()
}

func (e *Engine) pruneSignalLocks(taskID, runID string) {
	prefix := taskID + "/" + runID + "/"
	e.signalLocks.Range(func(k, _ any) bool {
		if s, ok := k.(string); ok && strings.HasPrefix(s, prefix) {
			e.signalLocks.Delete(k)
		}
		return true
	})
}

// lockSignal serialises CompleteStep calls for one (taskID,runID,stepID),
// distinct from lockRun. lockRun is held for a run's entire execution,
// including while a step is blocked waiting — CompleteStep must never wait
// on it, or a CompleteStep call for the very run it is trying to resume
// would deadlock against itself.
func (e *Engine) lockSignal(taskID, runID, stepID string) *sync.Mutex {
	actual, _ := e.signalLocks.LoadOrStore(taskID+"/"+runID+"/"+stepID, &sync.Mutex{})
	mu := actual.(*sync.Mutex)
	mu.Lock()
	return mu
}

// CancelRun requests cancellation of a run. It persists a durable cancel
// signal — reusing the SignalEntry/CompleteStep journal plumbing via a
// reserved signal ID — so the intent survives a crash: on the next RunTask
// for this taskID/runID, the run's ctx is cancelled before Task.Exec is
// invoked, and every RunStep call fails fast with ErrRunCancelled instead
// of re-running fn. If the run is currently executing in this process, its
// ctx is also cancelled immediately.
//
// Cancelling ctx only signals a step function; it cannot forcibly stop
// one. RunStep.Get and RunTask.Get both return promptly regardless — they
// select on ctx.Done() independently of whether the step's goroutine has
// exited. But the engine still waits for that goroutine to actually return
// before the run reaches a terminal state or Close/compactJournal can
// safely proceed (see Close). If the step function never checks ctx and
// never returns, that goroutine runs to completion in the background and
// the run stays non-terminal until it does — Close called afterward would
// block on it too. Write step functions so they select on ctx instead of
// running unconditionally to completion.
//
// The run ends up StatusFailed (the same terminal status used for engine
// Close and task/run timeouts) with TaskInfo.Error set to
// ErrRunCancelled.Error(), so callers can distinguish a deliberate cancel
// from another failure by comparing that string.
//
// Returns ErrRunAlreadyFinished if the run is already StatusCompleted or
// StatusFailed, or an error if taskID/runID is invalid or the run does not
// exist. Idempotent: calling it more than once on the same run is a no-op
// after the first call's signal is durably written.
func (e *Engine) CancelRun(ctx context.Context, taskID, runID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateTaskID(taskID); err != nil {
		return err
	}
	if err := validateRunID(runID); err != nil {
		return err
	}

	// Serialised on the same per-(taskID,runID,stepID) lock CompleteStep
	// uses (with stepID = the reserved cancelSignalID) so two concurrent
	// CancelRun calls for the same run cannot both decide the signal is
	// missing and double-append it.
	mu := e.lockSignal(taskID, runID, cancelSignalID)
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

	_, signals, err := e.loadJournal(taskID, runID)
	if err != nil {
		return err
	}
	if _, exists := signals[cancelSignalID]; !exists {
		if err := e.appendSignal(taskID, runID, cancelSignalID, cancelSignalPayload); err != nil {
			return err
		}
	}

	if v, ok := e.runCancels.Load(taskID + "/" + runID); ok {
		h := v.(*runHandle)
		h.cancelRequested.Store(true)
		h.cancel()
	}
	e.cfg.logger.Info("run cancel requested", "task_id", taskID, "run_id", runID)
	return nil
}

// log returns the engine logger, never nil. Internal tests construct a bare
// &Engine{dataDir: ...} with no config, so callers cannot assume one is set.
func (e *Engine) log() *slog.Logger { return orDiscard(e.cfg.logger) }

// log returns the read-only engine logger, never nil.
func (r *ReadOnlyEngine) log() *slog.Logger { return orDiscard(r.cfg.logger) }

func (e *Engine) lookupTask(taskID string) (taskEntry, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	entry, ok := e.registry[taskID]
	return entry, ok
}

type readOnlyConfig struct {
	lockTimeout   time.Duration
	logger        *slog.Logger
	codec         PayloadCodec
	journalMACKey []byte
}

// ReadOnlyEngine is a compile-time-restricted view of a dataDir. It acquires
// a shared flock so multiple readers can coexist. It cannot register or run
// tasks. Safe to open from a separate CLI process with no task registration.
type ReadOnlyEngine struct {
	dataDir   string
	cfg       readOnlyConfig
	lockFile  *flock.Flock
	closeOnce sync.Once
}

// NewReadOnlyEngine acquires a shared flock on <dataDir>/.lock. Multiple
// readers coexist. Returns ErrEngineLocked after the lock timeout if a
// writer holds the exclusive lock.
func NewReadOnlyEngine(dataDir string, opts ...Option) (*ReadOnlyEngine, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("durable: dataDir must not be empty")
	}
	cfg := readOnlyConfig{
		logger:      slog.New(slog.DiscardHandler),
		lockTimeout: defaultLockTimeout,
	}
	for _, o := range opts {
		o(nil, &cfg)
	}

	absDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("durable: resolve dataDir: %w", err)
	}
	if _, err := os.Stat(absDir); err != nil {
		return nil, fmt.Errorf("durable: dataDir %q: %w", absDir, err)
	}

	if err := occupyShared(absDir); err != nil {
		return nil, err
	}

	lockFile, err := acquireLock(context.Background(), lockPath(absDir), false, cfg.lockTimeout)
	if err != nil {
		releaseOccupancy(absDir, false)
		return nil, err
	}
	chmodBestEffort(lockPath(absDir), filePerm)

	r := &ReadOnlyEngine{
		dataDir:  absDir,
		cfg:      cfg,
		lockFile: lockFile,
	}
	r.cfg.logger.Debug("read-only engine opened", "data_dir", absDir)
	return r, nil
}

// Close releases the shared flock. Safe to call more than once.
func (r *ReadOnlyEngine) Close() error {
	var err error
	r.closeOnce.Do(func() {
		err = releaseLock(r.lockFile)
		releaseOccupancy(r.dataDir, false)
		r.cfg.logger.Debug("read-only engine closed", "data_dir", r.dataDir)
	})
	return err
}
