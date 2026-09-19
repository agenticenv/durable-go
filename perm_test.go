package durable

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func skipIfWindows(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("file permission bits are not enforced on Windows")
	}
}

func assertPerm(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Mode().Perm(); got != want {
		t.Fatalf("%s mode %04o, want %04o", path, got, want)
	}
}

func TestMkdirAllSecure_DirPerm(t *testing.T) {
	skipIfWindows(t)
	dir := filepath.Join(t.TempDir(), "nested", "run")
	if err := mkdirAllSecure(dir); err != nil {
		t.Fatal(err)
	}
	assertPerm(t, dir, dirPerm)
}

func TestOpenFileSecure_FilePerm(t *testing.T) {
	skipIfWindows(t)
	path := filepath.Join(t.TempDir(), "journal.log")
	f, err := openFileSecure(path, os.O_CREATE|os.O_WRONLY)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	assertPerm(t, path, filePerm)
}

func TestOpenFileSecure_TightensExisting(t *testing.T) {
	skipIfWindows(t)
	path := filepath.Join(t.TempDir(), "journal.log")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	assertPerm(t, path, 0o644)
	f, err := openFileSecure(path, os.O_APPEND|os.O_WRONLY)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	assertPerm(t, path, filePerm)
}

func TestTightenDataDir_ExistingWorldReadable(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	tasks := filepath.Join(dir, tasksDirName, "echo", "run-1")
	if err := os.MkdirAll(tasks, 0o755); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(tasks, journalFileName)
	if err := os.WriteFile(journal, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	tightenDataDir(dir)

	assertPerm(t, dir, dirPerm)
	assertPerm(t, filepath.Join(dir, tasksDirName), dirPerm)
	assertPerm(t, filepath.Join(dir, tasksDirName, "echo"), dirPerm)
	assertPerm(t, tasks, dirPerm)
	assertPerm(t, journal, filePerm)
}

func TestWarnIfInsecure_LogsWhenGroupOrWorldBitsRemain(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	orig := chmodFn
	chmodFn = func(string, os.FileMode) error { return nil }
	t.Cleanup(func() { chmodFn = orig })

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	warnIfInsecure(dir, logger)
	if !strings.Contains(buf.String(), "still accessible by group or others") {
		t.Fatalf("expected warn log, got %q", buf.String())
	}
}

func TestWarnIfInsecure_SilentWhenOwnerOnly(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	if err := os.Chmod(dir, dirPerm); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	warnIfInsecure(dir, logger)
	if buf.Len() != 0 {
		t.Fatalf("unexpected log: %s", buf.String())
	}
}

func TestNewEngine_WarnsWhenChmodDoesNotStick(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	orig := chmodFn
	chmodFn = func(string, os.FileMode) error { return nil }
	t.Cleanup(func() { chmodFn = orig })

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	e, err := NewEngine(context.Background(), dir, WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	_ = e.Close()
	if !strings.Contains(buf.String(), "still accessible by group or others") {
		t.Fatalf("expected warn log, got %q", buf.String())
	}
}

func TestSweepOrphanRunDirs_RemovesMetaLess(t *testing.T) {
	dir := t.TempDir()
	orphan := runDir(dir, "t", "r")
	if err := os.MkdirAll(orphan, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(inputPath(dir, "t", "r"), []byte(`"x"`), 0o600); err != nil {
		t.Fatal(err)
	}
	// no journal.log — the first-start crash window listTasks cannot see
	sweepOrphanRunDirs(dir, nil)
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan dir still present: %v", err)
	}
}

func TestSweepTmpFiles_RemovesStale(t *testing.T) {
	dir := t.TempDir()
	run := runDir(dir, "t", "r")
	if err := os.MkdirAll(run, 0o700); err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(run, "meta.json.tmp")
	if err := os.WriteFile(tmp, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	sweepTmpFiles(dir, nil)
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("tmp still present: %v", err)
	}
}

func TestIsGroupOrWorldAccessible(t *testing.T) {
	if !isGroupOrWorldAccessible(0o755) || !isGroupOrWorldAccessible(0o644) {
		t.Fatal("expected 0755 and 0644 to be group/world accessible")
	}
	if isGroupOrWorldAccessible(dirPerm) || isGroupOrWorldAccessible(filePerm) {
		t.Fatal("expected 0700 and 0600 to be owner-only")
	}
}
