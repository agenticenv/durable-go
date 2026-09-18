package durable

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

type prefixCodec struct{ p string }

func (c prefixCodec) Encode(plain, _ []byte) ([]byte, error) {
	out := make([]byte, 0, len(c.p)+len(plain))
	out = append(out, c.p...)
	out = append(out, plain...)
	return out, nil
}

func (c prefixCodec) Decode(ct, _ []byte) ([]byte, error) {
	if !bytes.HasPrefix(ct, []byte(c.p)) {
		return nil, errors.New("missing prefix")
	}
	out := make([]byte, len(ct)-len(c.p))
	copy(out, ct[len(c.p):])
	return out, nil
}

type aadCodec struct{}

func (aadCodec) Encode(plain, aad []byte) ([]byte, error) {
	buf := make([]byte, 4+len(aad)+len(plain))
	binary.BigEndian.PutUint32(buf, uint32(len(aad)))
	copy(buf[4:], aad)
	copy(buf[4+len(aad):], plain)
	return buf, nil
}

func (aadCodec) Decode(ct, aad []byte) ([]byte, error) {
	if len(ct) < 4 {
		return nil, errors.New("short")
	}
	n := int(binary.BigEndian.Uint32(ct[:4]))
	if n < 0 || 4+n > len(ct) {
		return nil, errors.New("short aad")
	}
	if !bytes.Equal(ct[4:4+n], aad) {
		return nil, errors.New("aad mismatch")
	}
	out := make([]byte, len(ct)-4-n)
	copy(out, ct[4+n:])
	return out, nil
}

func echoPayloadTask() Task[string, string] {
	return Func(func(ctx context.Context, s *StepRunner, in string) (string, error) {
		return RunStep(ctx, s, "echo", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return in, nil
		}).Get(ctx)
	})
}

func waitPayload(t *testing.T, timeout time.Duration, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout waiting for condition")
}

func TestPayloadAAD_Layout(t *testing.T) {
	got := payloadAAD(aadKindStepResult, "t", "r", "s")
	want := []byte("step-result\x00t\x00r\x00s")
	if !bytes.Equal(got, want) {
		t.Fatalf("got %q", got)
	}
}

func TestEncodeDecodePayload_EmptyAndNilCodec(t *testing.T) {
	aad := payloadAAD(aadKindTaskInput, "t", "r", "")
	got, err := encodePayload(nil, []byte("hi"), aad)
	if err != nil || string(got) != "hi" {
		t.Fatalf("nil codec encode: %q err=%v", got, err)
	}
	got, err = decodePayload(nil, []byte("hi"), aad)
	if err != nil || string(got) != "hi" {
		t.Fatalf("nil codec decode: %q err=%v", got, err)
	}
	got, err = encodePayload(prefixCodec{p: "ENC:"}, nil, aad)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty encode: %q err=%v", got, err)
	}
	got, err = decodePayload(prefixCodec{p: "ENC:"}, nil, aad)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty decode: %q err=%v", got, err)
	}
}

func TestAADCodec_RejectsSwappedAAD(t *testing.T) {
	c := aadCodec{}
	enc, err := c.Encode([]byte(`"x"`), payloadAAD(aadKindStepResult, "t", "r", "a"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Decode(enc, payloadAAD(aadKindStepResult, "t", "r", "b")); err == nil {
		t.Fatal("expected aad mismatch")
	}
	got, err := c.Decode(enc, payloadAAD(aadKindStepResult, "t", "r", "a"))
	if err != nil || string(got) != `"x"` {
		t.Fatalf("got %q err=%v", got, err)
	}
}

func TestAppendStep_EncodesAndLoadJournalDecodes(t *testing.T) {
	dir := t.TempDir()
	e := &Engine{dataDir: dir, cfg: engineConfig{codec: prefixCodec{p: "ENC:"}}}
	rec := StepRecord{
		StepID: "echo",
		Status: StepStatusCompleted,
		Input:  []byte(`"in"`),
		Result: []byte(`"out"`),
	}
	if err := e.appendStep("t", "r", rec); err != nil {
		t.Fatal(err)
	}
	e.closeJournal("t", "r")

	rawSteps, _, err := readJournalFrames(dir, "t", "r", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(rawSteps["echo"].Result, []byte("ENC:")) {
		t.Fatalf("disk result %q", rawSteps["echo"].Result)
	}

	steps, _, err := loadJournal(dir, "t", "r", e.codec(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(steps["echo"].Input) != `"in"` || string(steps["echo"].Result) != `"out"` {
		t.Fatalf("decoded %+v", steps["echo"])
	}

	if _, _, err := loadJournal(dir, "t", "r", prefixCodec{p: "NOPE:"}, nil); err == nil {
		t.Fatal("expected fail-closed decode")
	}
}

func TestAppendSignal_EncodesAndLoadJournalDecodes(t *testing.T) {
	dir := t.TempDir()
	e := &Engine{dataDir: dir, cfg: engineConfig{codec: prefixCodec{p: "ENC:"}}}
	if err := e.appendSignal("t", "r", "wait", []byte(`"ok"`)); err != nil {
		t.Fatal(err)
	}
	e.closeJournal("t", "r")

	_, sigs, err := readJournalFrames(dir, "t", "r", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(sigs) != 1 || !bytes.HasPrefix(sigs[0].GetPayload(), []byte("ENC:")) {
		t.Fatalf("disk signal %+v", sigs)
	}

	_, payloads, err := loadJournal(dir, "t", "r", e.codec(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(payloads["wait"]) != `"ok"` {
		t.Fatalf("decoded %q", payloads["wait"])
	}
}

func TestCompactJournal_CopiesCiphertextAsIs(t *testing.T) {
	dir := t.TempDir()
	e := &Engine{dataDir: dir, cfg: engineConfig{codec: prefixCodec{p: "ENC:"}}}
	if err := e.appendStep("t", "r", StepRecord{
		StepID: "s",
		Status: StepStatusCompleted,
		Result: []byte(`"done"`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.appendSignal("t", "r", "s", []byte(`"sig"`)); err != nil {
		t.Fatal(err)
	}
	if err := e.compactJournal("t", "r"); err != nil {
		t.Fatal(err)
	}

	steps, sigs, err := readJournalFrames(dir, "t", "r", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := steps["s"].Result; string(got) != `ENC:"done"` {
		t.Fatalf("compact step result %q", got)
	}
	if len(sigs) != 1 || string(sigs[0].GetPayload()) != `ENC:"sig"` {
		t.Fatalf("compact signal %+v", sigs)
	}

	decoded, payloads, err := loadJournal(dir, "t", "r", e.codec(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(decoded["s"].Result) != `"done"` || string(payloads["s"]) != `"sig"` {
		t.Fatal("decode after compact failed")
	}
}

func TestSaveLoadInputOutput_WithCodec(t *testing.T) {
	dir := t.TempDir()
	c := prefixCodec{p: "ENC:"}
	if err := saveInput(dir, "t", "r", []byte(`{"goal":"x"}`), c, nil); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(inputPath(dir, "t", "r"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(raw, []byte("ENC:")) {
		t.Fatalf("disk input %q", raw)
	}
	got, ok, err := loadInput(dir, "t", "r", c, nil)
	if err != nil || !ok || string(got) != `{"goal":"x"}` {
		t.Fatalf("load input %q ok=%v err=%v", got, ok, err)
	}

	if err := saveOutput(dir, "t", "r", []byte(`{"x":1}`), c, nil); err != nil {
		t.Fatal(err)
	}
	out, err := loadOutput(dir, "t", "r", c, nil)
	if err != nil || string(out) != `{"x":1}` {
		t.Fatalf("load output %q err=%v", out, err)
	}
}

func TestPayloadCodec_DefaultIsPlaintext(t *testing.T) {
	dir := t.TempDir()
	e, err := NewEngine(context.Background(), dir, WithPayloadCodec(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	if err := RegisterTask(e, "echo", echoPayloadTask()); err != nil {
		t.Fatal(err)
	}
	run := RunTask[string, string](context.Background(), e, "echo", "run-1", "hello")
	if _, err := run.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "tasks", "echo", "run-1", "input.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `"hello"` {
		t.Fatalf("default disk input %s", raw)
	}
}

func TestPayloadCodec_RoundTripReplayAndFailClosed(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	codec := prefixCodec{p: "ENC:"}
	var execs atomic.Int32
	task := Func(func(ctx context.Context, s *StepRunner, in string) (string, error) {
		return RunStep(ctx, s, "echo", in, func(ctx context.Context, in string) (string, error) {
			execs.Add(1)
			return in, nil
		}).Get(ctx)
	})

	e, err := NewEngine(ctx, dir, WithPayloadCodec(codec))
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterTask(e, "echo", task); err != nil {
		t.Fatal(err)
	}
	run := RunTask[string, string](ctx, e, "echo", "run-1", "hello")
	out, err := run.Get(ctx)
	if err != nil || out != "hello" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	if execs.Load() != 1 {
		t.Fatalf("execs %d", execs.Load())
	}

	raw, err := os.ReadFile(filepath.Join(dir, "tasks", "echo", "run-1", "input.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(raw, []byte("ENC:")) {
		t.Fatalf("expected encoded input, got %s", raw)
	}

	rec, ok, err := e.GetStep(ctx, "echo", "run-1", "echo")
	if err != nil || !ok {
		t.Fatalf("GetStep: ok=%v err=%v", ok, err)
	}
	if string(rec.Result) != `"hello"` || string(rec.Input) != `"hello"` {
		t.Fatalf("GetStep decoded input=%s result=%s", rec.Input, rec.Result)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := NewEngine(ctx, dir, WithPayloadCodec(codec))
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterTask(e2, "echo", task); err != nil {
		_ = e2.Close()
		t.Fatal(err)
	}
	run2 := RunTask[string, string](ctx, e2, "echo", "run-1", "ignored")
	out, err = run2.Get(ctx)
	if err != nil || out != "hello" {
		t.Fatalf("replay out=%q err=%v", out, err)
	}
	if execs.Load() != 1 {
		t.Fatalf("step re-ran on replay: execs %d", execs.Load())
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}

	e3, err := NewEngine(ctx, dir, WithPayloadCodec(prefixCodec{p: "NOPE:"}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e3.Close() }()
	if err := RegisterTask(e3, "echo", task); err != nil {
		t.Fatal(err)
	}
	_, err = RunTask[string, string](ctx, e3, "echo", "run-1", "hello").Get(ctx)
	if err == nil {
		t.Fatal("expected fail-closed resume with wrong codec")
	}
}

func TestPayloadCodec_ReadOnlyEngineDecodes(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	codec := prefixCodec{p: "ENC:"}
	e, err := NewEngine(ctx, dir, WithPayloadCodec(codec))
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterTask(e, "echo", echoPayloadTask()); err != nil {
		t.Fatal(err)
	}
	run := RunTask[string, string](ctx, e, "echo", "run-1", "hello")
	if _, err := run.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReadOnlyEngine(dir, WithPayloadCodec(codec))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	in, ok, err := r.LoadInput(ctx, "echo", "run-1")
	if err != nil || !ok || string(in) != `"hello"` {
		t.Fatalf("LoadInput %s ok=%v err=%v", in, ok, err)
	}
	rec, ok, err := r.GetStep(ctx, "echo", "run-1", "echo")
	if err != nil || !ok || string(rec.Result) != `"hello"` {
		t.Fatalf("GetStep result=%s ok=%v err=%v", rec.Result, ok, err)
	}

	plain, err := NewReadOnlyEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = plain.Close() }()
	in, ok, err = plain.LoadInput(ctx, "echo", "run-1")
	if err != nil || !ok {
		t.Fatalf("plaintext reader LoadInput err=%v ok=%v", err, ok)
	}
	if !bytes.HasPrefix(in, []byte("ENC:")) {
		t.Fatalf("reader without codec should see ciphertext, got %s", in)
	}
}

func TestPayloadCodec_CompleteStep(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	codec := prefixCodec{p: "ENC:"}
	e, err := NewEngine(ctx, dir, WithPayloadCodec(codec))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	if err := RegisterTask(e, "approve", Func(func(ctx context.Context, s *StepRunner, in string) (string, error) {
		return RunStep(ctx, s, "wait", struct{}{}, func(ctx context.Context, _ struct{}) (string, error) {
			return "", ErrStepPending
		}).Get(ctx)
	})); err != nil {
		t.Fatal(err)
	}
	run := RunTask[string, string](ctx, e, "approve", "r1", "x")
	waitPayload(t, 2*time.Second, func() bool { return run.Status() == StatusWaiting })
	if err := CompleteStep(ctx, e, encodeStepToken("approve", "r1", "wait"), "yes"); err != nil {
		t.Fatal(err)
	}
	out, err := run.Get(ctx)
	if err != nil || out != "yes" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func testAESKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	return key
}

func TestNewAESGCMCodec_RejectsBadKeySize(t *testing.T) {
	if _, err := NewAESGCMCodec(nil); err == nil {
		t.Fatal("expected error for empty key")
	}
	if _, err := NewAESGCMCodec(make([]byte, 15)); err == nil {
		t.Fatal("expected error for 15-byte key")
	}
}

func TestAESGCMCodec_RoundTripAndAAD(t *testing.T) {
	c, err := NewAESGCMCodec(testAESKey(t))
	if err != nil {
		t.Fatal(err)
	}
	aad := payloadAAD(aadKindStepResult, "t", "r", "echo")
	plain := []byte(`{"ok":true}`)
	ct, err := c.Encode(plain, aad)
	if err != nil {
		t.Fatal(err)
	}
	if ct[0] != aesgcmVersion || bytes.Equal(ct, plain) {
		t.Fatalf("expected AES-GCM envelope, got %q", ct)
	}
	got, err := c.Decode(ct, aad)
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("round-trip %q err=%v", got, err)
	}
	if _, err := c.Decode(ct, payloadAAD(aadKindStepResult, "t", "r", "other")); err == nil {
		t.Fatal("expected fail-closed on aad mismatch")
	}
	other, err := NewAESGCMCodec(bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Decode(ct, aad); err == nil {
		t.Fatal("expected fail-closed on wrong key")
	}
}

func TestAESGCMCodec_RejectsBadEnvelope(t *testing.T) {
	c, err := NewAESGCMCodec(testAESKey(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Decode([]byte{aesgcmVersion, 1, 2, 3}, nil); err == nil {
		t.Fatal("expected short ciphertext error")
	}
	ct, err := c.Encode([]byte("x"), nil)
	if err != nil {
		t.Fatal(err)
	}
	bad := bytes.Clone(ct)
	bad[0] = 0x02
	if _, err := c.Decode(bad, nil); err == nil {
		t.Fatal("expected unsupported version")
	}
}

func TestAESGCMCodec_EnginePersistsCiphertext(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	codec, err := NewAESGCMCodec(testAESKey(t))
	if err != nil {
		t.Fatal(err)
	}
	var execs atomic.Int32
	task := Func(func(ctx context.Context, s *StepRunner, in string) (string, error) {
		return RunStep(ctx, s, "echo", in, func(ctx context.Context, in string) (string, error) {
			execs.Add(1)
			return in, nil
		}).Get(ctx)
	})

	e, err := NewEngine(ctx, dir, WithPayloadCodec(codec))
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterTask(e, "echo", task); err != nil {
		t.Fatal(err)
	}
	run := RunTask[string, string](ctx, e, "echo", "run-1", "hello")
	out, err := run.Get(ctx)
	if err != nil || out != "hello" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "tasks", "echo", "run-1", "input.json"))
	if err != nil {
		t.Fatal(err)
	}
	if raw[0] != aesgcmVersion || bytes.Equal(raw, []byte(`"hello"`)) {
		t.Fatalf("expected AES-GCM envelope on disk, got %q", raw)
	}
	if _, err := os.Stat(filepath.Join(dir, "aes.key")); !os.IsNotExist(err) {
		t.Fatal("key must not be written under dataDir")
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	codec2, err := NewAESGCMCodec(testAESKey(t))
	if err != nil {
		t.Fatal(err)
	}
	e2, err := NewEngine(ctx, dir, WithPayloadCodec(codec2))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e2.Close() }()
	if err := RegisterTask(e2, "echo", task); err != nil {
		t.Fatal(err)
	}
	out, err = RunTask[string, string](ctx, e2, "echo", "run-1", "ignored").Get(ctx)
	if err != nil || out != "hello" {
		t.Fatalf("replay out=%q err=%v", out, err)
	}
	if execs.Load() != 1 {
		t.Fatalf("step re-ran on replay: %d", execs.Load())
	}
}
