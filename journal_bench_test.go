package durable

import (
	"context"
	"fmt"
	"testing"
)

// Journal microbenchmarks (go test -bench). This file is not the
// benchmarks/ runner.
//
//	go test -run=^$ -bench=. -benchmem .
//	go test -run=^$ -bench=BenchmarkJournalAppend -benchmem .
//
// Names: BenchmarkJournalAppend, BenchmarkJournalAppend1KiB,
// BenchmarkJournalLoad, BenchmarkJournalLoad100, BenchmarkWriteFileAtomic,
// BenchmarkRunTaskFirst, BenchmarkRunTaskCompletedGet, BenchmarkStepFnNoEngine.

const (
	benchTaskID = "bench-task"
	benchRunID  = "bench-run"
)

func newBenchEngine(b *testing.B) *Engine {
	b.Helper()
	e, err := NewEngine(context.Background(), b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = e.Close() })
	return e
}

func completedRecord(stepID string, payload []byte) StepRecord {
	return StepRecord{
		StepID: stepID,
		Status: StepStatusCompleted,
		Result: payload,
	}
}

func seedAppends(b *testing.B, e *Engine, n int, payload []byte) {
	b.Helper()
	for i := 0; i < n; i++ {
		if err := e.appendStep(benchTaskID, benchRunID, completedRecord(fmt.Sprintf("s-%d", i), payload)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkJournalAppend times one journal append+sync with a small result.
func BenchmarkJournalAppend(b *testing.B) {
	benchmarkJournalAppend(b, []byte(`"ok"`))
}

// BenchmarkJournalAppend1KiB times one journal append+sync with a 1 KiB payload.
func BenchmarkJournalAppend1KiB(b *testing.B) {
	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = 'x'
	}
	benchmarkJournalAppend(b, payload)
}

func benchmarkJournalAppend(b *testing.B, payload []byte) {
	e := newBenchEngine(b)
	if err := e.appendStep(benchTaskID, benchRunID, completedRecord("warm", payload)); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := completedRecord(fmt.Sprintf("s-%d", i), payload)
		if err := e.appendStep(benchTaskID, benchRunID, rec); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkJournalLoad times loadJournal of a 20-step 1 KiB journal.
func BenchmarkJournalLoad(b *testing.B) {
	benchmarkJournalLoad(b, 20)
}

// BenchmarkJournalLoad100 times loadJournal of a 100-step 1 KiB journal.
func BenchmarkJournalLoad100(b *testing.B) {
	benchmarkJournalLoad(b, 100)
}

func benchmarkJournalLoad(b *testing.B, steps int) {
	e := newBenchEngine(b)
	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = 'x'
	}
	seedAppends(b, e, steps, payload)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := e.loadJournal(benchTaskID, benchRunID); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWriteFileAtomic times tmp+sync+rename (meta/input/output).
func BenchmarkWriteFileAtomic(b *testing.B) {
	dir := b.TempDir()
	path := metaPath(dir, benchTaskID, benchRunID)
	data := []byte(`{"task_id":"bench-task","status":"running"}`)
	if err := writeFileAtomic(path, data); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := writeFileAtomic(path, data); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRunTaskFirst times a first RunTask with one RunStep.
func BenchmarkRunTaskFirst(b *testing.B) {
	ctx := context.Background()
	e := newBenchEngine(b)
	if err := RegisterTask(e, benchTaskID, Func(func(ctx context.Context, s *StepRunner, in string) (string, error) {
		return RunStep(ctx, s, "s", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return in, nil
		}).Get(ctx)
	})); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		run := RunTask[string, string](ctx, e, benchTaskID, fmt.Sprintf("r-%d", i), "x")
		if _, err := run.Get(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRunTaskCompletedGet times RunTask on an already-completed run (output.json).
func BenchmarkRunTaskCompletedGet(b *testing.B) {
	ctx := context.Background()
	e := newBenchEngine(b)
	if err := RegisterTask(e, benchTaskID, Func(func(ctx context.Context, s *StepRunner, in string) (string, error) {
		return RunStep(ctx, s, "s", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return in, nil
		}).Get(ctx)
	})); err != nil {
		b.Fatal(err)
	}
	run := RunTask[string, string](ctx, e, benchTaskID, "done", "x")
	if _, err := run.Get(ctx); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		run := RunTask[string, string](ctx, e, benchTaskID, "done", "x")
		if _, err := run.Get(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStepFnNoEngine times calling fn with no engine (baseline).
func BenchmarkStepFnNoEngine(b *testing.B) {
	fn := func() string { return "x" }
	b.ReportAllocs()
	b.ResetTimer()
	var sink string
	for i := 0; i < b.N; i++ {
		sink = fn()
	}
	_ = sink
}
