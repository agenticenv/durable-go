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
//	        charged, err := durable.RunStep(ctx, s, "charge", func(ctx context.Context) (string, error) {
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
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

const (
	defaultLockTimeout       = 2 * time.Second
	defaultAutoPurgeInterval = time.Hour
	permTestFileName         = ".perm_test"
)

type engineConfig struct {
	autoPurgeAge      time.Duration
	autoPurgeInterval time.Duration
	maxRetries        int
	timeout           time.Duration
	logger            *slog.Logger
	lockTimeout       time.Duration
}

// EngineOption configures NewEngine.
type EngineOption func(*engineConfig)

// WithAutoPurge starts a background goroutine that deletes Completed and
// Failed runs whose UpdatedAt is older than age. The optional interval
// controls how often the purger runs; it defaults to one hour. Running and
// Waiting runs are never purged.
func WithAutoPurge(age time.Duration, interval ...time.Duration) EngineOption {
	return func(c *engineConfig) {
		c.autoPurgeAge = age
		if len(interval) > 0 {
			c.autoPurgeInterval = interval[0]
		}
	}
}

// WithMaxRetries sets the engine-wide default for task-level retries
// (re-invoking the task closure). Default is 0 — retries are opt-in so
// non-idempotent work is not silently repeated. Overridden by
// WithTaskMaxRetries and WithRunMaxRetries.
func WithMaxRetries(n int) EngineOption {
	return func(c *engineConfig) { c.maxRetries = n }
}

// WithTimeout sets the engine-wide default task deadline. Zero (default)
// means no timeout. Overridden by WithTaskTimeout and WithRunTimeout.
func WithTimeout(d time.Duration) EngineOption {
	return func(c *engineConfig) { c.timeout = d }
}

// WithLogger sets the slog.Logger used for task and step lifecycle events.
// If nil or omitted, a discard logger is used.
func WithLogger(l *slog.Logger) EngineOption {
	return func(c *engineConfig) {
		if l != nil {
			c.logger = l
		}
	}
}

// WithLockTimeout sets how long NewEngine waits for the exclusive flock.
// Default is 2 seconds.
func WithLockTimeout(d time.Duration) EngineOption {
	return func(c *engineConfig) { c.lockTimeout = d }
}

// Engine is the process-level entry point for durable task execution.
// Create one per dataDir via NewEngine. Safe for concurrent use.
//
// Always call Close to cancel in-flight runs, release the exclusive flock,
// stop the purger, and close journal file handles.
type Engine struct {
	dataDir   string
	cfg       engineConfig
	lockFile  *flock.Flock
	registry  map[string]taskEntry
	mu        sync.RWMutex
	runLocks  sync.Map // "taskID/runID" → *sync.Mutex
	openFiles sync.Map // "taskID/runID" → *journalFile
	signals   sync.Map // "taskID/runID/stepID" → chan []byte
	stopCh    chan struct{}
	runs      sync.WaitGroup // in-flight RunTask executors and auto-purge
	closeOnce sync.Once
}

// NewEngine opens or creates dataDir, acquires an exclusive flock on
// <dataDir>/.lock, and initialises the in-memory task registry. Fails with
// ErrEngineLocked if another Engine or ReadOnlyEngine holds the directory.
func NewEngine(ctx context.Context, dataDir string, opts ...EngineOption) (*Engine, error) {
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
		o(&cfg)
	}

	absDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("durable: resolve dataDir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(absDir, tasksDirName), 0o755); err != nil {
		return nil, fmt.Errorf("durable: create dataDir %q: %w", absDir, err)
	}
	if err := ensureWritable(absDir); err != nil {
		return nil, err
	}

	if err := occupyExclusive(absDir); err != nil {
		return nil, err
	}

	lockFile, err := acquireLock(ctx, lockPath(absDir), true, cfg.lockTimeout)
	if err != nil {
		releaseOccupancy(absDir, true)
		return nil, err
	}

	e := &Engine{
		dataDir:  absDir,
		cfg:      cfg,
		lockFile: lockFile,
		registry: make(map[string]taskEntry),
		stopCh:   make(chan struct{}),
	}
	if e.cfg.autoPurgeAge > 0 {
		e.runs.Add(1)
		go e.runAutoPurge()
	}
	e.cfg.logger.Debug("engine opened", "data_dir", absDir)
	return e, nil
}

func ensureWritable(dir string) error {
	path := filepath.Join(dir, permTestFileName)
	f, err := os.Create(path)
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
func (e *Engine) Close() error {
	var err error
	e.closeOnce.Do(func() {
		close(e.stopCh)
		e.runs.Wait()
		e.closeAllJournals()
		err = releaseLock(e.lockFile)
		releaseOccupancy(e.dataDir, true)
		e.cfg.logger.Debug("engine closed", "data_dir", e.dataDir)
	})
	return err
}

// runAutoPurge is the long-lived goroutine started when WithAutoPurge is set.
// It exits when stopCh is closed by Close so the process can shut down
// without leaking the ticker.
func (e *Engine) runAutoPurge() {
	defer e.runs.Done()
	interval := e.cfg.autoPurgeInterval
	if interval <= 0 {
		interval = defaultAutoPurgeInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := e.purgeOlderThan(e.cfg.autoPurgeAge); err != nil {
				e.cfg.logger.Error("auto-purge failed", "error", err)
			}
		case <-e.stopCh:
			return
		}
	}
}

// purgeOlderThan deletes only Completed and Failed runs whose UpdatedAt is
// strictly older than now-age. Running and Waiting runs are skipped even if
// they look old — they may be blocked on a human approval.
func (e *Engine) purgeOlderThan(age time.Duration) error {
	cutoff := time.Now().UTC().Add(-age)
	e.cfg.logger.Debug("auto-purge running", "cutoff", cutoff)

	tasks, err := listTasks(context.Background(), e.dataDir)
	if err != nil {
		return err
	}

	var n int
	for _, t := range tasks {
		if t.Status != StatusCompleted && t.Status != StatusFailed {
			continue
		}
		if !t.UpdatedAt.Before(cutoff) {
			continue
		}
		if err := e.DeleteTaskRun(context.Background(), t.TaskID, t.RunID); err != nil {
			if err == ErrRunActive {
				continue
			}
			e.cfg.logger.Error("auto-purge delete failed", "task_id", t.TaskID, "run_id", t.RunID, "error", err)
			continue
		}
		n++
	}
	e.cfg.logger.Info("auto-purge completed", "purged", n, "cutoff", cutoff)
	return nil
}

func (e *Engine) lockRun(taskID, runID string) *sync.Mutex {
	actual, _ := e.runLocks.LoadOrStore(taskID+"/"+runID, &sync.Mutex{})
	mu := actual.(*sync.Mutex)
	mu.Lock()
	return mu
}

func (e *Engine) tryLockRun(taskID, runID string) (*sync.Mutex, bool) {
	actual, _ := e.runLocks.LoadOrStore(taskID+"/"+runID, &sync.Mutex{})
	mu := actual.(*sync.Mutex)
	if mu.TryLock() {
		return mu, true
	}
	return nil, false
}

func (e *Engine) lookupTask(taskID string) (taskEntry, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	entry, ok := e.registry[taskID]
	return entry, ok
}

type readOnlyConfig struct {
	lockTimeout time.Duration
	logger      *slog.Logger
}

// ReadOnlyOption configures NewReadOnlyEngine.
type ReadOnlyOption func(*readOnlyConfig)

// WithROLockTimeout sets how long NewReadOnlyEngine waits for the shared flock.
// Default is 2 seconds. Named distinctly from WithLockTimeout because both
// option types live in the same package.
func WithROLockTimeout(d time.Duration) ReadOnlyOption {
	return func(c *readOnlyConfig) { c.lockTimeout = d }
}

// WithROLogger sets the slog.Logger for the read-only engine. Named
// distinctly from WithLogger because both option types live in the same package.
func WithROLogger(l *slog.Logger) ReadOnlyOption {
	return func(c *readOnlyConfig) {
		if l != nil {
			c.logger = l
		}
	}
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
func NewReadOnlyEngine(dataDir string, opts ...ReadOnlyOption) (*ReadOnlyEngine, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("durable: dataDir must not be empty")
	}
	cfg := readOnlyConfig{
		logger:      slog.New(slog.DiscardHandler),
		lockTimeout: defaultLockTimeout,
	}
	for _, o := range opts {
		o(&cfg)
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
