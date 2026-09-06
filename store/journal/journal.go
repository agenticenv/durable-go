// Package journal provides a filesystem-backed, journal-per-task implementation
// of durable.Store. Each task gets its own directory containing an atomic
// meta.json file and an append-only journal.log that records step checkpoints.
//
// # Single-process constraint
//
// JournalStore is designed for use within a single OS process. Opening the same
// root directory from two processes simultaneously will corrupt journal.log
// (concurrent appends produce interleaved bytes). The package-level registry
// prevents two JournalStore instances from opening the same root within one
// process.
//
// # Crash safety
//
// Each entry written to journal.log is framed:
//
//	[ 4 bytes: uint32 payload length ][ N bytes: JSON ][ 4 bytes: CRC32 checksum ]
//
// On replay, entries with a mismatched checksum or a truncated tail (from a
// kill -9 or power loss mid-write) are detected and discarded. The preceding
// fully-written entries are replayed normally.
//
// TaskInfo is written to meta.json.tmp and then renamed into place. os.Rename
// is atomic on POSIX systems, so a crash mid-write always leaves either the
// old or the new meta.json intact — never a partial file.
package journal

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	durable "github.com/agenticenv/durable-go"
)

// frame layout constants
const (
	frameLenSize  = 4 // uint32 big-endian: payload byte length
	frameCRCSize  = 4 // uint32 big-endian: CRC32-IEEE of payload
	frameOverhead = frameLenSize + frameCRCSize
)

// ── registry ──────────────────────────────────────────────────────────────────

var (
	registryMu sync.Mutex
	registry   = map[string]*JournalStore{}
)

func registerStore(absRoot string, s *JournalStore) error {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := registry[absRoot]; exists {
		return fmt.Errorf("journal: store already open for root %q; share the existing instance instead of opening a second one", absRoot)
	}
	registry[absRoot] = s
	return nil
}

func unregisterStore(absRoot string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	delete(registry, absRoot)
}

// ── options ───────────────────────────────────────────────────────────────────

// JournalOption is a functional option for configuring a JournalStore.
type JournalOption func(*JournalStore)

// WithLogger sets an slog.Logger on the JournalStore. All store operations
// emit structured log events through this logger at the appropriate level.
// If not set, a no-op logger is used and nothing is emitted.
func WithLogger(logger *slog.Logger) JournalOption {
	return func(s *JournalStore) {
		if logger != nil {
			s.logger = logger
		}
	}
}

// ── JournalStore ──────────────────────────────────────────────────────────────

// JournalStore is a durable.Store implementation backed by the local filesystem.
// Each task is stored in its own subdirectory under root/tasks/<task-id>/.
//
// JournalStore is safe for concurrent use by multiple goroutines within a single
// OS process. It must not be shared across processes (see package doc).
//
// Always call Close when the store is no longer needed.
type JournalStore struct {
	root      string
	logger    *slog.Logger
	taskLocks sync.Map // taskID → *sync.Mutex
}

// NewJournalStore opens (or creates) a journal store rooted at root.
// root is created if it does not exist. Returns an error if root is already
// open in this process — share the existing instance instead.
func NewJournalStore(root string, opts ...JournalOption) (*JournalStore, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("journal: resolve root path: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(absRoot, "tasks"), 0o755); err != nil {
		return nil, fmt.Errorf("journal: create tasks dir: %w", err)
	}

	s := &JournalStore{
		root:   absRoot,
		logger: slog.New(slog.DiscardHandler),
	}
	for _, o := range opts {
		o(s)
	}

	if err := registerStore(absRoot, s); err != nil {
		return nil, err
	}

	s.logger.Debug("journal: store opened", "root", absRoot)
	return s, nil
}

// Close releases the store from the process-level registry.
// After Close, NewJournalStore may be called again for the same root.
// Always defer Close to ensure the registry entry is cleaned up.
func (s *JournalStore) Close() error {
	unregisterStore(s.root)
	s.logger.Debug("journal: store closed", "root", s.root)
	return nil
}

// ── path helpers ──────────────────────────────────────────────────────────────

func (s *JournalStore) taskDir(taskID string) string {
	return filepath.Join(s.root, "tasks", taskID)
}

func (s *JournalStore) metaPath(taskID string) string {
	return filepath.Join(s.taskDir(taskID), "meta.json")
}

func (s *JournalStore) journalPath(taskID string) string {
	return filepath.Join(s.taskDir(taskID), "journal.log")
}

// ── per-task locking ──────────────────────────────────────────────────────────

func (s *JournalStore) lockTask(taskID string) func() {
	actual, _ := s.taskLocks.LoadOrStore(taskID, &sync.Mutex{})
	mu := actual.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// ── SaveTask ──────────────────────────────────────────────────────────────────

// SaveTask implements durable.Store. Atomically writes task metadata to
// meta.json via a write-to-tmp-then-rename pattern.
func (s *JournalStore) SaveTask(ctx context.Context, task durable.TaskInfo) error {
	unlock := s.lockTask(task.ID)
	defer unlock()

	dir := s.taskDir(task.ID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		s.logger.Error("journal: create task dir failed", "task_id", task.ID, "error", err)
		return fmt.Errorf("journal: create task dir %q: %w", task.ID, err)
	}

	raw, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("journal: marshal task %q: %w", task.ID, err)
	}

	tmp := s.metaPath(task.ID) + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		s.logger.Error("journal: write meta tmp failed", "task_id", task.ID, "error", err)
		return fmt.Errorf("journal: write meta tmp %q: %w", task.ID, err)
	}
	if err := os.Rename(tmp, s.metaPath(task.ID)); err != nil {
		s.logger.Error("journal: rename meta failed", "task_id", task.ID, "error", err)
		_ = os.Remove(tmp)
		return fmt.Errorf("journal: rename meta %q: %w", task.ID, err)
	}

	s.logger.Debug("journal: task saved", "task_id", task.ID, "status", task.Status)
	return nil
}

// ── GetTask ───────────────────────────────────────────────────────────────────

// GetTask implements durable.Store.
func (s *JournalStore) GetTask(ctx context.Context, taskID string) (durable.TaskInfo, bool, error) {
	raw, err := os.ReadFile(s.metaPath(taskID))
	if errors.Is(err, os.ErrNotExist) {
		return durable.TaskInfo{}, false, nil
	}
	if err != nil {
		return durable.TaskInfo{}, false, fmt.Errorf("journal: read meta %q: %w", taskID, err)
	}

	var t durable.TaskInfo
	if err := json.Unmarshal(raw, &t); err != nil {
		return durable.TaskInfo{}, false, fmt.Errorf("journal: unmarshal meta %q: %w", taskID, err)
	}
	return t, true, nil
}

// ── ListTasks ─────────────────────────────────────────────────────────────────

// ListTasks implements durable.Store. Walks the tasks directory and reads each
// meta.json. Results are sorted by CreatedAt descending.
func (s *JournalStore) ListTasks(ctx context.Context) ([]durable.TaskInfo, error) {
	entries, err := os.ReadDir(filepath.Join(s.root, "tasks"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("journal: read tasks dir: %w", err)
	}

	var tasks []durable.TaskInfo
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		taskID := e.Name()
		t, ok, err := s.GetTask(ctx, taskID)
		if err != nil || !ok {
			continue // skip unreadable / incomplete task dirs
		}
		tasks = append(tasks, t)
	}

	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].CreatedAt.After(tasks[j].CreatedAt)
	})
	return tasks, nil
}

// ── DeleteTask ────────────────────────────────────────────────────────────────

// DeleteTask implements durable.Store. Removes the task directory and all its
// contents (meta.json + journal.log). No-op if the task does not exist.
func (s *JournalStore) DeleteTask(ctx context.Context, taskID string) error {
	unlock := s.lockTask(taskID)
	defer unlock()

	if err := os.RemoveAll(s.taskDir(taskID)); err != nil {
		s.logger.Error("journal: delete task failed", "task_id", taskID, "error", err)
		return fmt.Errorf("journal: delete task %q: %w", taskID, err)
	}
	// Clean up the in-memory lock entry so it doesn't leak indefinitely.
	s.taskLocks.Delete(taskID)
	s.logger.Debug("journal: task deleted", "task_id", taskID)
	return nil
}

// ── PurgeTasks ────────────────────────────────────────────────────────────────

// PurgeTasks implements durable.Store. Deletes task directories whose status
// matches and whose UpdatedAt is strictly before the cutoff time.
func (s *JournalStore) PurgeTasks(ctx context.Context, status durable.TaskStatus, before time.Time) (int64, error) {
	s.logger.Debug("journal: purging tasks", "status", status, "before", before)

	tasks, err := s.ListTasks(ctx)
	if err != nil {
		return 0, err
	}

	var n int64
	for _, t := range tasks {
		if t.Status != status || !t.UpdatedAt.Before(before) {
			continue
		}
		if err := s.DeleteTask(ctx, t.ID); err != nil {
			s.logger.Error("journal: purge delete failed", "task_id", t.ID, "error", err)
			continue
		}
		n++
	}

	s.logger.Info("journal: tasks purged", "status", status, "count", n, "before", before)
	return n, nil
}

// ── SaveStep ──────────────────────────────────────────────────────────────────

// SaveStep implements durable.Store. Appends a framed JSON entry to
// journal.log and calls Sync to flush to disk before returning.
//
// Upsert semantics are achieved by appending — LoadSteps returns the last
// written entry per stepID (last-write-wins).
func (s *JournalStore) SaveStep(ctx context.Context, taskID string, step durable.StepRecord) error {
	unlock := s.lockTask(taskID)
	defer unlock()

	// Ensure task directory exists (task may not have been saved yet in tests).
	if err := os.MkdirAll(s.taskDir(taskID), 0o755); err != nil {
		return fmt.Errorf("journal: create task dir for step %q/%q: %w", taskID, step.StepID, err)
	}

	payload, err := json.Marshal(step)
	if err != nil {
		return fmt.Errorf("journal: marshal step %q/%q: %w", taskID, step.StepID, err)
	}

	frame := makeFrame(payload)

	f, err := os.OpenFile(s.journalPath(taskID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		s.logger.Error("journal: open log for append failed", "task_id", taskID, "step_id", step.StepID, "error", err)
		return fmt.Errorf("journal: open log %q: %w", taskID, err)
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Write(frame); err != nil {
		s.logger.Error("journal: append frame failed", "task_id", taskID, "step_id", step.StepID, "error", err)
		return fmt.Errorf("journal: write frame %q/%q: %w", taskID, step.StepID, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("journal: sync log %q: %w", taskID, err)
	}

	s.logger.Debug("journal: step saved", "task_id", taskID, "step_id", step.StepID, "status", step.Status)
	return nil
}

// ── LoadStep ──────────────────────────────────────────────────────────────────

// LoadStep implements durable.Store.
func (s *JournalStore) LoadStep(ctx context.Context, taskID, stepID string) (durable.StepRecord, bool, error) {
	steps, err := s.LoadSteps(ctx, taskID)
	if err != nil {
		return durable.StepRecord{}, false, err
	}
	for _, rec := range steps {
		if rec.StepID == stepID {
			return rec, true, nil
		}
	}
	return durable.StepRecord{}, false, nil
}

// ── LoadSteps ─────────────────────────────────────────────────────────────────

// LoadSteps implements durable.Store. Reads journal.log and replays all valid
// framed entries. Partial or corrupt tail entries (from kill -9) are silently
// discarded. The last written entry per stepID is returned (upsert semantics).
// Results are ordered by Seq ascending.
func (s *JournalStore) LoadSteps(ctx context.Context, taskID string) ([]durable.StepRecord, error) {
	s.logger.Debug("journal: loading steps", "task_id", taskID)

	path := s.journalPath(taskID)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return make([]durable.StepRecord, 0), nil
	}
	if err != nil {
		return nil, fmt.Errorf("journal: read log %q: %w", taskID, err)
	}

	// last-write-wins map: stepID → StepRecord
	seen := make(map[string]durable.StepRecord)

	buf := data
	for len(buf) > 0 {
		rec, remaining, ok := readFrame(buf)
		if !ok {
			// Partial or corrupt tail — stop here (kill-9 safe).
			s.logger.Debug("journal: partial/corrupt frame detected, stopping replay", "task_id", taskID, "remaining_bytes", len(buf))
			break
		}
		seen[rec.StepID] = rec
		buf = remaining
	}

	// Collect and sort by Seq ascending.
	steps := make([]durable.StepRecord, 0, len(seen))
	for _, rec := range seen {
		steps = append(steps, rec)
	}
	sort.Slice(steps, func(i, j int) bool {
		return steps[i].Seq < steps[j].Seq
	})

	s.logger.Debug("journal: steps loaded", "task_id", taskID, "count", len(steps))
	return steps, nil
}

// ── ListStepIDs ───────────────────────────────────────────────────────────────

// ListStepIDs implements durable.Store.
func (s *JournalStore) ListStepIDs(ctx context.Context, taskID string) ([]string, error) {
	steps, err := s.LoadSteps(ctx, taskID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(steps))
	for i, rec := range steps {
		ids[i] = rec.StepID
	}
	return ids, nil
}

// ── frame encoding ────────────────────────────────────────────────────────────

// makeFrame encodes payload as:
//
//	[ 4B uint32 BE: len(payload) ][ payload ][ 4B uint32 BE: CRC32-IEEE(payload) ]
func makeFrame(payload []byte) []byte {
	frame := make([]byte, frameOverhead+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	copy(frame[4:], payload)
	checksum := crc32.ChecksumIEEE(payload)
	binary.BigEndian.PutUint32(frame[4+len(payload):], checksum)
	return frame
}

// readFrame reads one frame from buf. Returns the decoded StepRecord, the
// remaining bytes after the frame, and ok=true on success.
// Returns ok=false if buf is too short or the CRC32 does not match —
// indicating a partial write from a crash.
func readFrame(buf []byte) (rec durable.StepRecord, remaining []byte, ok bool) {
	if len(buf) < frameOverhead {
		return rec, buf, false
	}

	payloadLen := int(binary.BigEndian.Uint32(buf[0:4]))
	totalLen := frameOverhead + payloadLen
	if len(buf) < totalLen {
		return rec, buf, false // truncated payload
	}

	payload := buf[4 : 4+payloadLen]
	storedCRC := binary.BigEndian.Uint32(buf[4+payloadLen : totalLen])
	computedCRC := crc32.ChecksumIEEE(payload)
	if storedCRC != computedCRC {
		return rec, buf, false // corrupt entry
	}

	if err := json.Unmarshal(payload, &rec); err != nil {
		return rec, buf, false
	}

	return rec, buf[totalLen:], true
}

// Ensure JournalStore satisfies durable.Store at compile time.
var _ durable.Store = (*JournalStore)(nil)
