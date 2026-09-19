// Package exdir locates an example directory whether you run from the repo
// root, examples/, or the example folder itself.
package exdir

import (
	"os"
	"path/filepath"
)

// Dir returns the example named name (e.g. "yaml-task").
func Dir(name string) string {
	for _, c := range []string{".", name, filepath.Join("examples", name)} {
		if isExample(c, name) {
			return c
		}
	}
	return "."
}

// Data is Dir(name)/.data/journal.
func Data(name, journal string) string {
	return filepath.Join(Dir(name), ".data", journal)
}

func isExample(dir, name string) bool {
	if _, err := os.Stat(filepath.Join(dir, "main.go")); err != nil {
		return false
	}
	base := filepath.Base(dir)
	if dir == "." {
		wd, err := os.Getwd()
		if err != nil {
			return false
		}
		base = filepath.Base(wd)
	}
	return base == name
}
