package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	durable "github.com/agenticenv/durable-go"
)

func TestParseArgs_DirAndStatus(t *testing.T) {
	p, err := parseArgs([]string{"-d", "./data", "task", "list", "--status", "running"})
	if err != nil {
		t.Fatal(err)
	}
	if p.dir != "./data" || p.status != "running" {
		t.Fatalf("%+v", p)
	}
	if got := strings.Join(p.rest, " "); got != "task list" {
		t.Fatalf("rest=%q", got)
	}

	p, err = parseArgs([]string{"--dir=./j", "task", "get", "echo"})
	if err != nil {
		t.Fatal(err)
	}
	if p.dir != "./j" {
		t.Fatalf("dir=%q", p.dir)
	}
}

func TestResolveDir_FlagWinsEnv(t *testing.T) {
	dir, err := resolveDir("/flag", func(k string) string {
		if k == "DURABLE_DIR" {
			return "/env"
		}
		return ""
	})
	if err != nil || dir != "/flag" {
		t.Fatalf("dir=%q err=%v", dir, err)
	}

	dir, err = resolveDir("", func(k string) string {
		if k == "DURABLE_DIR" {
			return "/env"
		}
		return ""
	})
	if err != nil || dir != "/env" {
		t.Fatalf("dir=%q err=%v", dir, err)
	}

	_, err = resolveDir("", func(string) string { return "" })
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestInspect_ListAndGet(t *testing.T) {
	dir := t.TempDir()
	seedJournal(t, dir)

	env := func(string) string { return "" }
	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	if code := run([]string{"-d", dir, "task", "list"}, stdout, stderr, env); code != 0 {
		t.Fatalf("list exit=%d stderr=%s", code, stderr)
	}
	out := stdout.String()
	if !strings.Contains(out, "echo") || !strings.Contains(out, "run-1") || !strings.Contains(out, "Echo Task") {
		t.Fatalf("list output:\n%s", out)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-d", dir, "task", "get", "echo", "run-1"}, stdout, stderr, env); code != 0 {
		t.Fatalf("get exit=%d stderr=%s", code, stderr)
	}
	out = stdout.String()
	if !strings.Contains(out, "TASK_ID:") || !strings.Contains(out, "say") || !strings.Contains(out, "hello") {
		t.Fatalf("get output:\n%s", out)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"--dir", dir, "task", "get", "Echo Task"}, stdout, stderr, env); code != 0 {
		t.Fatalf("get by name exit=%d stderr=%s", code, stderr)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-d", dir, "task", "get", "run-1"}, stdout, stderr, env); code != 0 {
		t.Fatalf("get by runID exit=%d stderr=%s", code, stderr)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-d", dir, "step", "list", "echo", "run-1"}, stdout, stderr, env); code != 0 {
		t.Fatalf("step list exit=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout.String(), "say") {
		t.Fatalf("step list:\n%s", stdout)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-d", dir, "step", "get", "echo", "run-1", "say"}, stdout, stderr, env); code != 0 {
		t.Fatalf("step get exit=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout.String(), "hello") {
		t.Fatalf("step get:\n%s", stdout)
	}
}

func TestInspect_DURABLE_DIR(t *testing.T) {
	dir := t.TempDir()
	seedJournal(t, dir)

	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	getenv := func(k string) string {
		if k == "DURABLE_DIR" {
			return dir
		}
		return ""
	}
	if code := run([]string{"task", "list"}, stdout, stderr, getenv); code != 0 {
		t.Fatalf("exit=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout.String(), "echo") {
		t.Fatalf("output:\n%s", stdout)
	}
}

func TestInspect_StepInputAndVersion(t *testing.T) {
	dir := t.TempDir()
	seedJournal(t, dir)

	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	if code := run([]string{"-d", dir, "step", "get", "echo", "run-1", "say"}, stdout, stderr, func(string) string { return "" }); code != 0 {
		t.Fatalf("exit=%d stderr=%s", code, stderr)
	}
	out := stdout.String()
	if !strings.Contains(out, "VERSION:") || !strings.Contains(out, "1") {
		t.Fatalf("missing version:\n%s", out)
	}
	if !strings.Contains(out, "INPUT:") || !strings.Contains(out, "hello") {
		t.Fatalf("missing input:\n%s", out)
	}

	stdout.Reset()
	if code := run([]string{"-d", dir, "step", "list", "echo", "run-1"}, stdout, stderr, func(string) string { return "" }); code != 0 {
		t.Fatalf("list exit=%d stderr=%s", code, stderr)
	}
	list := stdout.String()
	if !strings.Contains(list, "VERSION") || !strings.Contains(list, "INPUT") {
		t.Fatalf("list:\n%s", list)
	}
}

func TestInspect_MissingDir(t *testing.T) {
	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	code := run([]string{"task", "list"}, stdout, stderr, func(string) string { return "" })
	if code != 1 {
		t.Fatalf("exit=%d", code)
	}
	if !strings.Contains(stderr.String(), "DURABLE_DIR") {
		t.Fatalf("stderr=%s", stderr)
	}
}

func TestInspect_WriterLocked(t *testing.T) {
	dir := t.TempDir()
	e, err := durable.NewEngine(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })

	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	code := run([]string{"-d", dir, "task", "list"}, stdout, stderr, func(string) string { return "" })
	if code != 1 {
		t.Fatalf("exit=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr.String(), "locked by a writer") {
		t.Fatalf("stderr=%s", stderr)
	}
}

func seedJournal(t *testing.T, dir string) {
	t.Helper()
	ctx := context.Background()
	e, err := durable.NewEngine(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	task := durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "say", in, func(_ context.Context, in string) (string, error) {
			return in, nil
		}, durable.WithStepVersion("1")).Get(ctx)
	})
	if err := durable.RegisterTask(e, "echo", task, durable.WithName("Echo Task")); err != nil {
		t.Fatal(err)
	}
	run := durable.RunTask[string, string](ctx, e, "echo", "run-1", "hello")
	if _, err := run.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "tasks")); err != nil {
		t.Fatal(err)
	}
}
