package main

import (
	"bytes"
	"context"
	"encoding/hex"
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

	p, err = parseArgs([]string{"--payload-key", "abc", "--redact", "task", "get", "echo"})
	if err != nil {
		t.Fatal(err)
	}
	if p.payloadKey != "abc" || !p.redact {
		t.Fatalf("%+v", p)
	}

	p, err = parseArgs([]string{"--payload-key=deadbeef", "step", "get"})
	if err != nil {
		t.Fatal(err)
	}
	if p.payloadKey != "deadbeef" {
		t.Fatalf("payloadKey=%q", p.payloadKey)
	}

	_, err = parseArgs([]string{"--payload-key"})
	if err == nil {
		t.Fatal("expected error")
	}

	p, err = parseArgs([]string{"--", "-d", "./data", "task", "list"})
	if err != nil {
		t.Fatal(err)
	}
	if p.dir != "./data" || strings.Join(p.rest, " ") != "task list" {
		t.Fatalf("%+v", p)
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

func TestResolvePayloadKey_FlagWinsEnv(t *testing.T) {
	got := resolvePayloadKey("flag", func(k string) string {
		if k == "DURABLE_PAYLOAD_KEY" {
			return "env"
		}
		return ""
	})
	if got != "flag" {
		t.Fatalf("got=%q", got)
	}

	got = resolvePayloadKey("", func(k string) string {
		if k == "DURABLE_PAYLOAD_KEY" {
			return "env"
		}
		return ""
	})
	if got != "env" {
		t.Fatalf("got=%q", got)
	}

	got = resolvePayloadKey("", func(string) string { return "" })
	if got != "" {
		t.Fatalf("got=%q", got)
	}
}

func TestParseAESKey(t *testing.T) {
	raw := bytes.Repeat([]byte{'k'}, 32)
	got, err := parseAESKey(string(raw))
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("raw key: got=%q err=%v", got, err)
	}

	hx := hex.EncodeToString(raw)
	got, err = parseAESKey(hx)
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("hex key: got=%q err=%v", got, err)
	}

	if _, err := parseAESKey("short"); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseHMACKey(t *testing.T) {
	raw := []byte("demo-journal-mac-key")
	if got := parseHMACKey(string(raw)); !bytes.Equal(got, raw) {
		t.Fatalf("raw: %q", got)
	}
	hx := hex.EncodeToString(raw)
	if got := parseHMACKey(hx); !bytes.Equal(got, raw) {
		t.Fatalf("hex: %q", got)
	}
	if got := parseHMACKey(""); got != nil {
		t.Fatalf("empty: %q", got)
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

func TestInspect_Redact(t *testing.T) {
	dir := t.TempDir()
	seedJournal(t, dir)
	env := func(string) string { return "" }

	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	if code := run([]string{"-d", dir, "--redact", "task", "get", "echo", "run-1"}, stdout, stderr, env); code != 0 {
		t.Fatalf("exit=%d stderr=%s", code, stderr)
	}
	out := stdout.String()
	if !strings.Contains(out, "[redacted]") {
		t.Fatalf("missing [redacted]:\n%s", out)
	}
	if strings.Contains(out, `"hello"`) {
		t.Fatalf("plaintext leaked:\n%s", out)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-d", dir, "--redact", "step", "get", "echo", "run-1", "say"}, stdout, stderr, env); code != 0 {
		t.Fatalf("step get exit=%d stderr=%s", code, stderr)
	}
	step := stdout.String()
	if !strings.Contains(step, "[redacted]") || strings.Contains(step, `"hello"`) {
		t.Fatalf("step get:\n%s", step)
	}
}

func TestInspect_PayloadKeyDecryptAndRedact(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{'k'}, 32)
	seedEncryptedJournal(t, dir, key)
	env := func(string) string { return "" }

	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	if code := run([]string{"-d", dir, "task", "get", "echo", "run-1"}, stdout, stderr, env); code != 0 {
		t.Fatalf("no-key exit=%d stderr=%s", code, stderr)
	}
	cipherOut := stdout.String()
	if strings.Contains(cipherOut, `"hello"`) {
		t.Fatalf("ciphertext inspect leaked plaintext:\n%s", cipherOut)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-d", dir, "--payload-key", string(key), "task", "get", "echo", "run-1"}, stdout, stderr, env); code != 0 {
		t.Fatalf("decrypt exit=%d stderr=%s", code, stderr)
	}
	plain := stdout.String()
	if !strings.Contains(plain, `"hello"`) {
		t.Fatalf("decrypt missing plaintext:\n%s", plain)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-d", dir, "--payload-key", hex.EncodeToString(key), "step", "get", "echo", "run-1", "say"}, stdout, stderr, env); code != 0 {
		t.Fatalf("hex key exit=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout.String(), `"hello"`) {
		t.Fatalf("hex decrypt:\n%s", stdout)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-d", dir, "--payload-key", string(key), "--redact", "task", "get", "echo", "run-1"}, stdout, stderr, env); code != 0 {
		t.Fatalf("redact+decrypt exit=%d stderr=%s", code, stderr)
	}
	redactedOut := stdout.String()
	if !strings.Contains(redactedOut, "[redacted]") {
		t.Fatalf("missing [redacted]:\n%s", redactedOut)
	}
	if strings.Contains(redactedOut, `"hello"`) {
		t.Fatalf("redact leaked plaintext:\n%s", redactedOut)
	}

	stdout.Reset()
	stderr.Reset()
	getenv := func(k string) string {
		if k == "DURABLE_PAYLOAD_KEY" {
			return string(key)
		}
		return ""
	}
	if code := run([]string{"-d", dir, "task", "get", "echo", "run-1"}, stdout, stderr, getenv); code != 0 {
		t.Fatalf("env key exit=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout.String(), `"hello"`) {
		t.Fatalf("env decrypt:\n%s", stdout)
	}

	stdout.Reset()
	stderr.Reset()
	wrong := bytes.Repeat([]byte{'x'}, 32)
	getenvWrong := func(k string) string {
		if k == "DURABLE_PAYLOAD_KEY" {
			return string(wrong)
		}
		return ""
	}
	if code := run([]string{"-d", dir, "--payload-key", string(key), "task", "get", "echo", "run-1"}, stdout, stderr, getenvWrong); code != 0 {
		t.Fatalf("flag-wins exit=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout.String(), `"hello"`) {
		t.Fatalf("flag should win env:\n%s", stdout)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-d", dir, "--payload-key", "short", "task", "get", "echo", "run-1"}, stdout, stderr, env); code != 1 {
		t.Fatalf("bad key exit=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr.String(), "payload key") {
		t.Fatalf("stderr=%s", stderr)
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

func seedEncryptedJournal(t *testing.T, dir string, key []byte) {
	t.Helper()
	ctx := context.Background()
	codec, err := durable.NewAESGCMCodec(key)
	if err != nil {
		t.Fatal(err)
	}
	e, err := durable.NewEngine(ctx, dir, durable.WithPayloadCodec(codec))
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
}

func TestInspect_JournalMACKey(t *testing.T) {
	dir := t.TempDir()
	macKey := "inspect-mac-key"
	ctx := context.Background()
	e, err := durable.NewEngine(ctx, dir, durable.WithJournalMACKey([]byte(macKey)))
	if err != nil {
		t.Fatal(err)
	}
	task := durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
		return durable.RunStep(ctx, s, "say", in, func(_ context.Context, in string) (string, error) {
			return in, nil
		}).Get(ctx)
	})
	if err := durable.RegisterTask(e, "echo", task); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.RunTask[string, string](ctx, e, "echo", "run-1", "hello").Get(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	env := func(string) string { return "" }
	if code := run([]string{"-d", dir, "step", "get", "echo", "run-1", "say"}, stdout, stderr, env); code == 0 {
		if strings.Contains(stdout.String(), `"hello"`) {
			t.Fatalf("inspect without MAC key leaked/replayed plaintext:\n%s", stdout)
		}
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-d", dir, "--journal-mac-key", macKey, "step", "get", "echo", "run-1", "say"}, stdout, stderr, env); code != 0 {
		t.Fatalf("exit=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout.String(), `"hello"`) {
		t.Fatalf("missing decrypt:\n%s", stdout)
	}

	stdout.Reset()
	stderr.Reset()
	hx := hex.EncodeToString([]byte(macKey))
	envHex := func(k string) string {
		if k == "DURABLE_JOURNAL_MAC_KEY" {
			return hx
		}
		return ""
	}
	if code := run([]string{"-d", dir, "step", "get", "echo", "run-1", "say"}, stdout, stderr, envHex); code != 0 {
		t.Fatalf("hex env exit=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout.String(), `"hello"`) {
		t.Fatalf("hex env missing decrypt:\n%s", stdout)
	}
}
