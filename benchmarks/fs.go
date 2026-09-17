package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type fsResult struct {
	appendOps      int
	appendTotal    time.Duration
	atomicOps      int
	atomicTotal    time.Duration
	journalRead    time.Duration
	journalBytes   int64
	journalReadErr string
}

func probeFS(dir string, steps, payloadBytes int, journalPath string) (fsResult, error) {
	var r fsResult
	probeDir := filepath.Join(dir, ".fsprobe")
	if err := os.MkdirAll(probeDir, 0o755); err != nil {
		return r, err
	}

	frame := make([]byte, payloadBytes)
	for i := range frame {
		frame[i] = 'x'
	}
	appendPath := filepath.Join(probeDir, "append.log")
	f, err := os.OpenFile(appendPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return r, err
	}
	// STARTED + COMPLETED per step, matching journal append+sync.
	r.appendOps = steps * 2
	start := time.Now()
	for i := 0; i < r.appendOps; i++ {
		if _, err := f.Write(frame); err != nil {
			_ = f.Close()
			return r, err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return r, err
		}
	}
	r.appendTotal = time.Since(start)
	if err := f.Close(); err != nil {
		return r, err
	}

	// One run writes input.json, several meta.json updates, and output.json.
	atomicOps := 5
	metaLike := make([]byte, 256)
	for i := range metaLike {
		metaLike[i] = 'm'
	}
	r.atomicOps = atomicOps
	start = time.Now()
	for i := 0; i < atomicOps; i++ {
		path := filepath.Join(probeDir, fmt.Sprintf("atomic-%d.json", i))
		data := metaLike
		if i == atomicOps-1 {
			data = frame // last write sized like output.json
		}
		if err := writeFileAtomic(path, data); err != nil {
			return r, err
		}
	}
	r.atomicTotal = time.Since(start)

	if journalPath != "" {
		if st, err := os.Stat(journalPath); err == nil {
			r.journalBytes = st.Size()
		}
		start = time.Now()
		_, err := os.ReadFile(journalPath)
		r.journalRead = time.Since(start)
		if err != nil {
			r.journalReadErr = err.Error()
		}
	}
	return r, nil
}

func writeFileAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
