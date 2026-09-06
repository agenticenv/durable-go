package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	durable "github.com/agenticenv/durable-go"
	_ "modernc.org/sqlite"
)

// SQLiteOption is a functional option for configuring a SQLiteStore.
type SQLiteOption func(*SQLiteStore)

// WithLogger sets an slog.Logger on the SQLiteStore. All store operations
// emit structured log events through this logger. If not set, a no-op logger
// is used and nothing is emitted.
func WithLogger(logger *slog.Logger) SQLiteOption {
	return func(s *SQLiteStore) {
		if logger != nil {
			s.logger = logger
		}
	}
}

// SQLiteStore is a durable.Store implementation backed by a local SQLite database.
// It is safe for concurrent use; the connection pool is limited to one connection
// to avoid SQLite write-lock contention.
type SQLiteStore struct {
	db     *sql.DB
	logger *slog.Logger
}

// NewSQLiteStore opens (or creates) a SQLite database at dbPath and initialises
// the schema. WAL journal mode, a 5 s busy timeout, NORMAL synchronous writes,
// foreign key enforcement, and WAL auto-checkpoint every 1000 pages are enabled
// for reliable concurrent read access alongside single-writer semantics.
func NewSQLiteStore(dbPath string, opts ...SQLiteOption) (*SQLiteStore, error) {
	dsn := fmt.Sprintf(
		"%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)&_pragma=wal_autocheckpoint(1000)",
		dbPath,
	)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open db: %w", err)
	}
	// Single writer connection prevents file-lock contention.
	db.SetMaxOpenConns(1)

	store := &SQLiteStore{
		db:     db,
		logger: slog.New(slog.DiscardHandler),
	}
	for _, o := range opts {
		o(store)
	}

	store.logger.Debug("sqlite: opening database", "path", dbPath)
	if err := store.initSchema(context.Background()); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite: init schema: %w", err)
	}
	store.logger.Debug("sqlite: schema initialised", "path", dbPath)
	return store, nil
}

// Close closes the underlying SQLite database handle.
// Must be called when the store is no longer needed to release file locks.
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) initSchema(ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS durable_tasks (
		id           TEXT PRIMARY KEY,
		name         TEXT NOT NULL DEFAULT '',
		tags         TEXT NOT NULL DEFAULT '{}',
		status       TEXT NOT NULL,
		error        TEXT NOT NULL DEFAULT '',
		panic_trace  TEXT NOT NULL DEFAULT '',
		created_at   DATETIME NOT NULL,
		started_at   DATETIME,
		completed_at DATETIME,
		updated_at   DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS durable_steps (
		task_id      TEXT    NOT NULL,
		step_id      TEXT    NOT NULL,
		seq          INTEGER NOT NULL DEFAULT 0,
		input_hash   TEXT    NOT NULL DEFAULT '',
		result       BLOB,
		error        TEXT    NOT NULL DEFAULT '',
		panic_trace  TEXT    NOT NULL DEFAULT '',
		status       TEXT    NOT NULL,
		started_at   DATETIME,
		completed_at DATETIME,
		PRIMARY KEY (task_id, step_id),
		FOREIGN KEY (task_id) REFERENCES durable_tasks(id) ON DELETE CASCADE
	);
	`
	_, err := s.db.ExecContext(ctx, query)
	return err
}

// SaveTask implements durable.Store. Performs an upsert keyed on task.ID.
func (s *SQLiteStore) SaveTask(ctx context.Context, task durable.TaskInfo) error {
	tagsJSON, err := json.Marshal(task.Tags)
	if err != nil {
		return fmt.Errorf("sqlite: marshal tags: %w", err)
	}
	now := time.Now().UTC()
	createdAt := task.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	updatedAt := task.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = now
	}
	query := `
	INSERT INTO durable_tasks
		(id, name, tags, status, error, panic_trace, created_at, started_at, completed_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(id) DO UPDATE SET
		name         = excluded.name,
		tags         = excluded.tags,
		status       = excluded.status,
		error        = excluded.error,
		panic_trace  = excluded.panic_trace,
		started_at   = excluded.started_at,
		completed_at = excluded.completed_at,
		updated_at   = excluded.updated_at;
	`
	_, err = s.db.ExecContext(ctx, query,
		task.ID,
		task.Name,
		string(tagsJSON),
		string(task.Status),
		task.Error,
		task.PanicTrace,
		createdAt,
		nullTime(task.StartedAt),
		nullTime(task.CompletedAt),
		updatedAt,
	)
	if err != nil {
		s.logger.Error("sqlite: save task failed", "task_id", task.ID, "status", task.Status, "error", err)
		return err
	}
	s.logger.Debug("sqlite: task saved", "task_id", task.ID, "status", task.Status)
	return nil
}

// GetTask implements durable.Store.
func (s *SQLiteStore) GetTask(ctx context.Context, taskID string) (durable.TaskInfo, bool, error) {
	query := `
	SELECT id, name, tags, status, error, panic_trace,
	       created_at, started_at, completed_at, updated_at
	FROM durable_tasks WHERE id = ?`
	row := s.db.QueryRowContext(ctx, query, taskID)

	var t durable.TaskInfo
	var tagsStr, statusStr string
	var startedAt, completedAt sql.NullTime

	err := row.Scan(
		&t.ID, &t.Name, &tagsStr, &statusStr, &t.Error, &t.PanicTrace,
		&t.CreatedAt, &startedAt, &completedAt, &t.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return durable.TaskInfo{}, false, nil
	}
	if err != nil {
		return durable.TaskInfo{}, false, fmt.Errorf("sqlite: scan task: %w", err)
	}

	t.Status = durable.TaskStatus(statusStr)
	if startedAt.Valid {
		t.StartedAt = startedAt.Time
	}
	if completedAt.Valid {
		t.CompletedAt = completedAt.Time
	}
	if tagsStr != "" && tagsStr != "{}" {
		if err := json.Unmarshal([]byte(tagsStr), &t.Tags); err != nil {
			return durable.TaskInfo{}, false, fmt.Errorf("sqlite: unmarshal tags: %w", err)
		}
	}
	return t, true, nil
}

// ListTasks implements durable.Store.
func (s *SQLiteStore) ListTasks(ctx context.Context) ([]durable.TaskInfo, error) {
	query := `
	SELECT id, name, tags, status, error, panic_trace,
	       created_at, started_at, completed_at, updated_at
	FROM durable_tasks ORDER BY created_at DESC`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list tasks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tasks []durable.TaskInfo
	for rows.Next() {
		var t durable.TaskInfo
		var tagsStr, statusStr string
		var startedAt, completedAt sql.NullTime
		if err := rows.Scan(
			&t.ID, &t.Name, &tagsStr, &statusStr, &t.Error, &t.PanicTrace,
			&t.CreatedAt, &startedAt, &completedAt, &t.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("sqlite: scan task row: %w", err)
		}
		t.Status = durable.TaskStatus(statusStr)
		if startedAt.Valid {
			t.StartedAt = startedAt.Time
		}
		if completedAt.Valid {
			t.CompletedAt = completedAt.Time
		}
		if tagsStr != "" && tagsStr != "{}" {
			_ = json.Unmarshal([]byte(tagsStr), &t.Tags)
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

// DeleteTask implements durable.Store. Cascades to associated step records.
func (s *SQLiteStore) DeleteTask(ctx context.Context, taskID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM durable_tasks WHERE id = ?`, taskID)
	return err
}

// PurgeTasks implements durable.Store.
func (s *SQLiteStore) PurgeTasks(ctx context.Context, status durable.TaskStatus, before time.Time) (int64, error) {
	s.logger.Debug("sqlite: purging tasks", "status", status, "before", before)
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM durable_tasks WHERE status = ? AND updated_at < ?`,
		string(status), before,
	)
	if err != nil {
		s.logger.Error("sqlite: purge tasks failed", "status", status, "error", err)
		return 0, err
	}
	n, _ := res.RowsAffected()
	s.logger.Info("sqlite: tasks purged", "status", status, "count", n, "before", before)
	return n, nil
}

// SaveStep implements durable.Store. Performs an upsert keyed on (taskID, step.StepID).
func (s *SQLiteStore) SaveStep(ctx context.Context, taskID string, step durable.StepRecord) error {
	query := `
	INSERT INTO durable_steps
		(task_id, step_id, seq, input_hash, result, error, panic_trace, status, started_at, completed_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(task_id, step_id) DO UPDATE SET
		seq          = excluded.seq,
		input_hash   = excluded.input_hash,
		result       = excluded.result,
		error        = excluded.error,
		panic_trace  = excluded.panic_trace,
		status       = excluded.status,
		started_at   = excluded.started_at,
		completed_at = excluded.completed_at;
	`
	_, err := s.db.ExecContext(ctx, query,
		taskID,
		step.StepID,
		step.Seq,
		step.InputHash,
		step.Result,
		step.Error,
		step.PanicTrace,
		string(step.Status),
		nullTime(step.StartedAt),
		nullTime(step.CompletedAt),
	)
	if err != nil {
		s.logger.Error("sqlite: save step failed", "task_id", taskID, "step_id", step.StepID, "status", step.Status, "error", err)
		return err
	}
	s.logger.Debug("sqlite: step saved", "task_id", taskID, "step_id", step.StepID, "status", step.Status)
	return nil
}

// LoadStep implements durable.Store.
func (s *SQLiteStore) LoadStep(ctx context.Context, taskID, stepID string) (durable.StepRecord, bool, error) {
	query := `
	SELECT step_id, seq, input_hash, result, error, panic_trace, status, started_at, completed_at
	FROM durable_steps WHERE task_id = ? AND step_id = ?`
	row := s.db.QueryRowContext(ctx, query, taskID, stepID)

	var rec durable.StepRecord
	var statusStr string
	var startedAt, completedAt sql.NullTime

	err := row.Scan(
		&rec.StepID, &rec.Seq, &rec.InputHash, &rec.Result,
		&rec.Error, &rec.PanicTrace, &statusStr, &startedAt, &completedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return durable.StepRecord{}, false, nil
	}
	if err != nil {
		return durable.StepRecord{}, false, fmt.Errorf("sqlite: scan step: %w", err)
	}

	rec.Status = durable.StepStatus(statusStr)
	if startedAt.Valid {
		rec.StartedAt = startedAt.Time
	}
	if completedAt.Valid {
		rec.CompletedAt = completedAt.Time
	}
	return rec, true, nil
}

// LoadSteps implements durable.Store. Returns all step records ordered by Seq ascending.
func (s *SQLiteStore) LoadSteps(ctx context.Context, taskID string) ([]durable.StepRecord, error) {
	s.logger.Debug("sqlite: loading steps", "task_id", taskID)
	query := `
	SELECT step_id, seq, input_hash, result, error, panic_trace, status, started_at, completed_at
	FROM durable_steps WHERE task_id = ? ORDER BY seq ASC`
	rows, err := s.db.QueryContext(ctx, query, taskID)
	if err != nil {
		s.logger.Error("sqlite: load steps failed", "task_id", taskID, "error", err)
		return nil, fmt.Errorf("sqlite: load steps: %w", err)
	}
	defer func() { _ = rows.Close() }()

	steps := make([]durable.StepRecord, 0)
	for rows.Next() {
		var rec durable.StepRecord
		var statusStr string
		var startedAt, completedAt sql.NullTime
		if err := rows.Scan(
			&rec.StepID, &rec.Seq, &rec.InputHash, &rec.Result,
			&rec.Error, &rec.PanicTrace, &statusStr, &startedAt, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("sqlite: scan step row: %w", err)
		}
		rec.Status = durable.StepStatus(statusStr)
		if startedAt.Valid {
			rec.StartedAt = startedAt.Time
		}
		if completedAt.Valid {
			rec.CompletedAt = completedAt.Time
		}
		steps = append(steps, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	s.logger.Debug("sqlite: steps loaded", "task_id", taskID, "count", len(steps))
	return steps, nil
}

// ListStepIDs implements durable.Store. Returns step IDs ordered by Seq ascending.
func (s *SQLiteStore) ListStepIDs(ctx context.Context, taskID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT step_id FROM durable_steps WHERE task_id = ? ORDER BY seq ASC`, taskID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// nullTime converts a zero time.Time to a sql.NullTime that will be stored as NULL.
func nullTime(t time.Time) sql.NullTime {
	return sql.NullTime{Time: t, Valid: !t.IsZero()}
}
