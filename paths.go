package durable

import (
	"fmt"
	"path/filepath"
	"strings"
)

const (
	tasksDirName    = "tasks"
	metaFileName    = "meta.json"
	journalFileName = "journal.log"
	outputFileName  = "output.json"
	lockFileName    = ".lock"
)

func taskDir(dataDir, taskID string) string {
	return filepath.Join(dataDir, tasksDirName, taskID)
}

func runDir(dataDir, taskID, runID string) string {
	return filepath.Join(taskDir(dataDir, taskID), runID)
}

func metaPath(dataDir, taskID, runID string) string {
	return filepath.Join(runDir(dataDir, taskID, runID), metaFileName)
}

func journalPath(dataDir, taskID, runID string) string {
	return filepath.Join(runDir(dataDir, taskID, runID), journalFileName)
}

func outputPath(dataDir, taskID, runID string) string {
	return filepath.Join(runDir(dataDir, taskID, runID), outputFileName)
}

func lockPath(dataDir string) string {
	return filepath.Join(dataDir, lockFileName)
}

func (e *Engine) taskDir(taskID string) string {
	return taskDir(e.dataDir, taskID)
}

func (e *Engine) runDir(taskID, runID string) string {
	return runDir(e.dataDir, taskID, runID)
}

func (e *Engine) journalPath(taskID, runID string) string {
	return journalPath(e.dataDir, taskID, runID)
}

// validateTaskID rejects empty IDs and any value that could escape dataDir
// when used as a path segment. Colon is rejected because StepToken encodes
// taskID:runID:stepID and decode splits on the first two colons — a colon
// inside taskID would shift the fields.
func validateTaskID(taskID string) error {
	if err := validatePathID(taskID); err != nil {
		return fmt.Errorf("durable: invalid task ID %q: %w", taskID, err)
	}
	return nil
}

// validateRunID applies the same path-safety rules as validateTaskID.
// An empty runID is not validated here — RunTask treats "" as "resolve
// singleton / generate ULID" before this is called on a concrete value.
func validateRunID(runID string) error {
	if err := validatePathID(runID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRunID, err)
	}
	return nil
}

func validatePathID(id string) error {
	if id == "" {
		return fmt.Errorf("must not be empty")
	}
	if id == "." || id == ".." {
		return fmt.Errorf("must not be %q", id)
	}
	if strings.Contains(id, "..") {
		return fmt.Errorf("must not contain %q", "..")
	}
	if strings.ContainsAny(id, `/\`) {
		return fmt.Errorf("must not contain path separators")
	}
	if strings.Contains(id, ":") {
		return fmt.Errorf("must not contain ':' (reserved for step-token encoding)")
	}
	if filepath.Base(id) != id {
		return fmt.Errorf("must be a single path segment")
	}
	return nil
}
