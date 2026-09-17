package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	durable "github.com/agenticenv/durable-go"
)

type config struct {
	steps        int
	payload      string
	payloadBytes int
	iters        int
	warmup       int
	dir          string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "durable-go benchmark: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := parseFlags(args)
	if err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}

	dir := cfg.dir
	cleanup := false
	if dir == "" {
		tmp, err := os.MkdirTemp("", "durable-bench-*")
		if err != nil {
			return err
		}
		dir = tmp
		cleanup = true
	} else {
		abs, err := filepath.Abs(dir)
		if err != nil {
			return err
		}
		dir = abs
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	if cleanup {
		defer func() { _ = os.RemoveAll(dir) }()
	}

	buf := make([]byte, cfg.payloadBytes)
	for i := range buf {
		buf[i] = 'x'
	}
	payload := string(buf)

	ctx := context.Background()
	e, err := durable.NewEngine(ctx, dir)
	if err != nil {
		return fmt.Errorf("NewEngine: %w", err)
	}
	defer func() { _ = e.Close() }()

	var firstExecs atomic.Int64
	if err := registerTask(e, cfg.steps, payload, &firstExecs); err != nil {
		return err
	}

	// Warmup (discarded).
	for i := 0; i < cfg.warmup; i++ {
		_ = runPlain(cfg.steps, payload)
		runID := fmt.Sprintf("warmup-%d", i)
		if err := runDurable(ctx, e, runID, cfg.steps); err != nil {
			return fmt.Errorf("warmup durable: %w", err)
		}
		if _, err := replayOnce(ctx, dir, runID, cfg.steps, payload); err != nil {
			return fmt.Errorf("warmup replay: %w", err)
		}
	}

	var plain, first, replay stats
	var sink string
	for i := 0; i < cfg.iters; i++ {
		start := time.Now()
		sink = runPlain(cfg.steps, payload)
		plain.add(time.Since(start))
	}
	if cfg.steps > 0 && sink != payload {
		return fmt.Errorf("plain run produced unexpected sink")
	}

	var lastRunID string
	beforeFirst := firstExecs.Load()
	for i := 0; i < cfg.iters; i++ {
		runID := fmt.Sprintf("first-%d", i)
		start := time.Now()
		if err := runDurable(ctx, e, runID, cfg.steps); err != nil {
			return fmt.Errorf("first durable %s: %w", runID, err)
		}
		first.add(time.Since(start))
		lastRunID = runID
	}
	gotFirst := firstExecs.Load() - beforeFirst
	wantFirst := int64(cfg.iters * cfg.steps)
	if gotFirst != wantFirst {
		return fmt.Errorf("first durable: step fn ran %d times, want %d", gotFirst, wantFirst)
	}

	for i := 0; i < cfg.iters; i++ {
		d, err := replayOnce(ctx, dir, lastRunID, cfg.steps, payload)
		if err != nil {
			return fmt.Errorf("step replay: %w", err)
		}
		replay.add(d)
	}

	fs, err := probeFS(dir, cfg.steps, cfg.payloadBytes, journalFilePath(dir, lastRunID))
	if err != nil {
		return fmt.Errorf("filesystem probe: %w", err)
	}

	if err := writeReport(os.Stdout, cfg, dir, plain, first, replay, fs); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}

func replayOnce(ctx context.Context, srcDataDir, runID string, steps int, payload string) (time.Duration, error) {
	dst, err := os.MkdirTemp("", "durable-replay-*")
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.RemoveAll(dst) }()
	if err := prepareReplayDir(srcDataDir, runID, dst); err != nil {
		return 0, err
	}
	start := time.Now()
	e, err := durable.NewEngine(ctx, dst)
	if err != nil {
		return 0, err
	}
	defer func() { _ = e.Close() }()
	var execs atomic.Int64
	if err := registerReplayTask(e, steps, payload, &execs); err != nil {
		return 0, err
	}
	run := durable.RunTask[benchInput, string](ctx, e, benchTaskID, runID, benchInput{Steps: steps})
	if err := waitStatus(run, durable.StatusWaiting, 30*time.Second); err != nil {
		return 0, err
	}
	elapsed := time.Since(start)
	if n := execs.Load(); n != 0 {
		return 0, fmt.Errorf("step fn re-ran on replay (%d calls); want cache hit", n)
	}
	return elapsed, nil
}

func parseFlags(args []string) (config, error) {
	if len(args) > 0 && args[0] == "--" {
		args = args[1:]
	}
	fs := flag.NewFlagSet("durable-go benchmark", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var cfg config
	fs.IntVar(&cfg.steps, "steps", 20, "number of RunStep calls in the synthetic task")
	fs.StringVar(&cfg.payload, "payload", "1kb", "size of each step output (e.g. 512, 1kb, 64kb)")
	fs.IntVar(&cfg.iters, "iters", 5, "timed iterations per mode")
	fs.IntVar(&cfg.warmup, "warmup", 1, "untimed iterations discarded before measuring")
	fs.StringVar(&cfg.dir, "dir", "", "journal disk to measure (default: temp dir)")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.steps < 1 || cfg.steps > maxSteps {
		return config{}, fmt.Errorf("-steps must be 1..%d", maxSteps)
	}
	if cfg.iters < 1 {
		return config{}, fmt.Errorf("-iters must be >= 1")
	}
	if cfg.warmup < 0 {
		return config{}, fmt.Errorf("-warmup must be >= 0")
	}
	n, err := parsePayload(cfg.payload)
	if err != nil {
		return config{}, fmt.Errorf("-payload: %w", err)
	}
	cfg.payloadBytes = n
	return cfg, nil
}
