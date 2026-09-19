package durable

import (
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
)

const (
	dirPerm   os.FileMode = 0o700
	filePerm  os.FileMode = 0o600
	worldBits os.FileMode = 0o077
)

// chmodFn is os.Chmod. Tests stub it to simulate platforms where mode bits
// do not stick (Windows) so warnIfInsecure can be exercised.
var chmodFn = os.Chmod

func chmodBestEffort(path string, perm os.FileMode) {
	_ = chmodFn(path, perm)
}

func mkdirAllSecure(path string) error {
	if err := os.MkdirAll(path, dirPerm); err != nil {
		return err
	}
	chmodBestEffort(path, dirPerm)
	return nil
}

func openFileSecure(path string, flag int) (*os.File, error) {
	f, err := os.OpenFile(path, flag, filePerm)
	if err != nil {
		return nil, err
	}
	chmodBestEffort(path, filePerm)
	return f, nil
}

// ensureDataDirPerms sets owner-only modes on dataDir, tasks/, and .lock.
// The recursive walk of existing files runs only until .perms_ok is written,
// so later NewEngine calls are O(1) in the number of runs.
func ensureDataDirPerms(dir string) {
	chmodBestEffort(dir, dirPerm)
	tasks := filepath.Join(dir, tasksDirName)
	chmodBestEffort(tasks, dirPerm)
	chmodBestEffort(lockPath(dir), filePerm)
	sentinel := filepath.Join(dir, permsSentinelName)
	if _, err := os.Stat(sentinel); err == nil {
		return
	}
	tightenDataDir(dir)
	_ = os.WriteFile(sentinel, []byte("ok\n"), filePerm)
	chmodBestEffort(sentinel, filePerm)
}

// tightenDataDir sets owner-only modes on dataDir, tasks/, .lock, and every
// existing file under tasks/. Chmod failures are ignored (Windows).
func tightenDataDir(dir string) {
	chmodBestEffort(dir, dirPerm)
	tasks := filepath.Join(dir, tasksDirName)
	chmodBestEffort(tasks, dirPerm)
	_ = filepath.WalkDir(tasks, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			chmodBestEffort(path, dirPerm)
		} else {
			chmodBestEffort(path, filePerm)
		}
		return nil
	})
	chmodBestEffort(lockPath(dir), filePerm)
}

func sweepTmpFiles(dataDir string, logger *slog.Logger) {
	tasks := filepath.Join(dataDir, tasksDirName)
	_ = filepath.WalkDir(tasks, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if filepath.Ext(d.Name()) == ".tmp" || len(d.Name()) > 4 && d.Name()[len(d.Name())-4:] == ".tmp" {
			if err := os.Remove(path); err == nil {
				orDiscard(logger).Warn("removed stale tmp file", "path", path)
			}
		}
		return nil
	})
}

func sweepOrphanRunDirs(dataDir string, logger *slog.Logger) {
	tasks := filepath.Join(dataDir, tasksDirName)
	taskEntries, err := os.ReadDir(tasks)
	if err != nil {
		return
	}
	for _, te := range taskEntries {
		if !te.IsDir() {
			continue
		}
		runs, err := os.ReadDir(filepath.Join(tasks, te.Name()))
		if err != nil {
			continue
		}
		for _, re := range runs {
			if !re.IsDir() {
				continue
			}
			dir := runDir(dataDir, te.Name(), re.Name())
			if _, err := os.Stat(metaPath(dataDir, te.Name(), re.Name())); err == nil {
				continue
			}
			// A journal without meta is still a real run (lost sidecar).
			// Only drop the first-start crash window: input.json and no
			// journal, which listTasks cannot see.
			if _, err := os.Stat(journalPath(dataDir, te.Name(), re.Name())); err == nil {
				continue
			}
			if err := os.RemoveAll(dir); err == nil {
				orDiscard(logger).Warn("removed run directory with no meta.json",
					"task_id", te.Name(), "run_id", re.Name())
			}
		}
	}
}

func isGroupOrWorldAccessible(mode os.FileMode) bool {
	return mode.Perm()&worldBits != 0
}

func warnIfInsecure(dir string, logger *slog.Logger) {
	if logger == nil {
		return
	}
	st, err := os.Stat(dir)
	if err != nil {
		return
	}
	if isGroupOrWorldAccessible(st.Mode()) {
		logger.Warn("dataDir is still accessible by group or others",
			"data_dir", dir,
			"mode", st.Mode().Perm().String(),
		)
	}
}
