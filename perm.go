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
