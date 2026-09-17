package main

import (
	"fmt"
	"io"
	"math"
	"sort"
	"time"
)

type stats struct {
	samples []time.Duration
}

func (s *stats) add(d time.Duration) {
	s.samples = append(s.samples, d)
}

func (s stats) sorted() []time.Duration {
	out := append([]time.Duration(nil), s.samples...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (s stats) pct(p float64) time.Duration {
	vals := s.sorted()
	if len(vals) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(vals)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(vals) {
		idx = len(vals) - 1
	}
	return vals[idx]
}

func (s stats) avg() time.Duration {
	if len(s.samples) == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range s.samples {
		sum += d
	}
	return sum / time.Duration(len(s.samples))
}

func formatDur(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.1fµs", float64(d.Nanoseconds())/1e3)
	}
	if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

func writeReport(w io.Writer, cfg config, disk string, plain, first, replay stats, fs fsResult) error {
	overhead := first.pct(50) - plain.pct(50)
	if overhead < 0 {
		overhead = 0
	}

	p := func(format string, args ...any) error {
		_, err := fmt.Fprintf(w, format, args...)
		return err
	}
	if err := p("=== durable-go benchmark ===\n"); err != nil {
		return err
	}
	if err := p("disk     %s\n", disk); err != nil {
		return err
	}
	if err := p("steps    %d\n", cfg.steps); err != nil {
		return err
	}
	if err := p("payload  %s per step output\n", formatBytes(int64(cfg.payloadBytes))); err != nil {
		return err
	}
	if err := p("iters    %d  (warmup %d discarded)\n\n", cfg.iters, cfg.warmup); err != nil {
		return err
	}

	if err := p("Task latency\n"); err != nil {
		return err
	}
	if err := p("  %-18s %10s %10s %10s\n", "mode", "p50", "p95", "avg"); err != nil {
		return err
	}
	if err := printMode(w, "no engine", plain); err != nil {
		return err
	}
	if err := printMode(w, "first durable", first); err != nil {
		return err
	}
	if err := printMode(w, "step replay", replay); err != nil {
		return err
	}
	if err := p("  %-18s %10s\n", "overhead (first)", formatDur(overhead)); err != nil {
		return err
	}
	if err := p("      first durable p50 − no engine p50\n"); err != nil {
		return err
	}
	if err := p("      step replay = journal load + cached steps (fn does not re-run)\n\n"); err != nil {
		return err
	}

	if err := p("Filesystem (isolated, same disk — not engine instrumentation)\n"); err != nil {
		return err
	}
	if err := p("  journal append+sync  %4d ops  %10s   (2 per step: STARTED + COMPLETED)\n",
		fs.appendOps, formatDur(fs.appendTotal)); err != nil {
		return err
	}
	if err := p("  atomic write         %4d ops  %10s   (tmp+sync+rename+dirsync; meta/input/output)\n",
		fs.atomicOps, formatDur(fs.atomicTotal)); err != nil {
		return err
	}
	if fs.journalReadErr != "" {
		return p("  journal read                %10s   (%s)\n", "n/a", fs.journalReadErr)
	}
	return p("  journal read          1 file  %10s   (actual journal.log, %s)\n",
		formatDur(fs.journalRead), formatBytes(fs.journalBytes))
}

func printMode(w io.Writer, name string, s stats) error {
	_, err := fmt.Fprintf(w, "  %-18s %10s %10s %10s\n", name, formatDur(s.pct(50)), formatDur(s.pct(95)), formatDur(s.avg()))
	return err
}
