package durable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	durablepb "github.com/agenticenv/durable-go/pb"
)

func TestMakeReadFrame_RoundTrip(t *testing.T) {
	rec := StepRecord{
		StepID:      "charge",
		Seq:         1,
		Status:      StepStatusCompleted,
		Result:      []byte(`"ok"`),
		StartedAt:   time.Unix(0, 1_700_000_000_000_000_000).UTC(),
		CompletedAt: time.Unix(0, 1_700_000_000_100_000_000).UTC(),
	}
	entry := &durablepb.JournalEntry{
		Entry: &durablepb.JournalEntry_Step{Step: stepToProto(rec)},
	}
	payload, err := proto.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	frame := makeFrame(payload)

	got, err := readFrame(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if got.GetStep() == nil {
		t.Fatal("expected step entry")
	}
	back := protoToStep(got.GetStep())
	if back.StepID != rec.StepID || back.Seq != rec.Seq || back.Status != rec.Status {
		t.Fatalf("round-trip mismatch: got %+v want %+v", back, rec)
	}
	if !bytes.Equal(back.Result, rec.Result) {
		t.Fatalf("result mismatch: %q vs %q", back.Result, rec.Result)
	}
}

func TestReadFrame_EOFAtEnd(t *testing.T) {
	_, err := readFrame(bytes.NewReader(nil))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("empty reader: got %v, want io.EOF", err)
	}
}

func TestReadFrame_CRCCorruption(t *testing.T) {
	entry := &durablepb.JournalEntry{
		Entry: &durablepb.JournalEntry_Step{Step: stepToProto(StepRecord{StepID: "a", Seq: 1, Status: StepStatusCompleted})},
	}
	payload, err := proto.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	frame := makeFrame(payload)
	frame[len(frame)-1] ^= 0xff

	_, err = readFrame(bytes.NewReader(frame))
	if !errors.Is(err, errCorruptFrame) {
		t.Fatalf("corrupt CRC: got %v, want errCorruptFrame", err)
	}
}

func TestReadFrame_PartialTail(t *testing.T) {
	entry := &durablepb.JournalEntry{
		Entry: &durablepb.JournalEntry_Step{Step: stepToProto(StepRecord{StepID: "a", Seq: 1, Status: StepStatusCompleted})},
	}
	payload, err := proto.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	frame := makeFrame(payload)
	truncated := frame[:len(frame)/2]

	_, err = readFrame(bytes.NewReader(truncated))
	if !errors.Is(err, errCorruptFrame) && !errors.Is(err, io.EOF) {
		t.Fatalf("partial tail: got %v, want corrupt or EOF", err)
	}
}

func TestReadFrame_HugeLengthRejected(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], maxFramePayload+1)
	_, err := readFrame(bytes.NewReader(hdr[:]))
	if !errors.Is(err, errCorruptFrame) {
		t.Fatalf("huge length: got %v, want errCorruptFrame", err)
	}
}

func TestLoadJournal_CacheAndSignals(t *testing.T) {
	dir := t.TempDir()
	taskID, runID := "t1", "r1"
	if err := os.MkdirAll(runDir(dir, taskID, runID), 0o755); err != nil {
		t.Fatal(err)
	}

	e := &Engine{dataDir: dir}
	if err := e.appendStep(taskID, runID, StepRecord{
		StepID: "one", Seq: 1, Status: StepStatusCompleted, Result: []byte(`"a"`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.appendStep(taskID, runID, StepRecord{
		StepID: "two", Seq: 2, Status: StepStatusWaiting,
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.appendSignal(taskID, runID, "two", []byte(`"sig"`)); err != nil {
		t.Fatal(err)
	}

	cache, signals, err := loadJournal(dir, taskID, runID)
	if err != nil {
		t.Fatal(err)
	}
	if cache["one"].Status != StepStatusCompleted {
		t.Fatalf("one: %+v", cache["one"])
	}
	if cache["two"].Status != StepStatusWaiting {
		t.Fatalf("two status: %s", cache["two"].Status)
	}
	if string(signals["two"]) != `"sig"` {
		t.Fatalf("signal payload: %s", signals["two"])
	}
}

func TestLoadJournal_PartialTailStopsReplay(t *testing.T) {
	dir := t.TempDir()
	taskID, runID := "t1", "r1"
	if err := os.MkdirAll(runDir(dir, taskID, runID), 0o755); err != nil {
		t.Fatal(err)
	}
	e := &Engine{dataDir: dir}
	if err := e.appendStep(taskID, runID, StepRecord{
		StepID: "good", Seq: 1, Status: StepStatusCompleted, Result: []byte(`"ok"`),
	}); err != nil {
		t.Fatal(err)
	}
	e.closeJournal(taskID, runID)

	path := journalPath(dir, taskID, runID)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x00, 0x00, 0x00, 0x20, 0x01, 0x02}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	cache, _, err := loadJournal(dir, taskID, runID)
	if err != nil {
		t.Fatalf("partial tail must not fail load: %v", err)
	}
	if cache["good"].Status != StepStatusCompleted {
		t.Fatalf("expected intact prefix, got %+v", cache)
	}
}

func TestCompactJournal_PreservesLatestStepAndAllSignals(t *testing.T) {
	dir := t.TempDir()
	taskID, runID := "t1", "r1"
	e := &Engine{dataDir: dir}

	if err := e.appendStep(taskID, runID, StepRecord{StepID: "s", Seq: 1, Status: StepStatusWaiting}); err != nil {
		t.Fatal(err)
	}
	if err := e.appendStep(taskID, runID, StepRecord{StepID: "s", Seq: 1, Status: StepStatusCompleted, Result: []byte(`"done"`)}); err != nil {
		t.Fatal(err)
	}
	if err := e.appendSignal(taskID, runID, "s", []byte(`"first"`)); err != nil {
		t.Fatal(err)
	}
	if err := e.appendSignal(taskID, runID, "s", []byte(`"second"`)); err != nil {
		t.Fatal(err)
	}

	if err := e.compactJournal(taskID, runID); err != nil {
		t.Fatal(err)
	}

	steps, sigs, err := readJournalFrames(dir, taskID, runID)
	if err != nil {
		t.Fatal(err)
	}
	if steps["s"].Status != StepStatusCompleted {
		t.Fatalf("compact should keep latest step, got %s", steps["s"].Status)
	}
	if len(sigs) != 2 {
		t.Fatalf("compact should keep all SignalEntry records, got %d", len(sigs))
	}
	if string(sigs[0].GetPayload()) != `"first"` || string(sigs[1].GetPayload()) != `"second"` {
		t.Fatalf("signals: %+v", sigs)
	}
}

func TestMakeFrame_CRCMatchesPayload(t *testing.T) {
	payload := []byte("hello")
	frame := makeFrame(payload)
	if int(binary.BigEndian.Uint32(frame[:4])) != len(payload) {
		t.Fatal("length prefix mismatch")
	}
	got := binary.BigEndian.Uint32(frame[4+len(payload):])
	want := crc32.ChecksumIEEE(payload)
	if got != want {
		t.Fatalf("crc %d != %d", got, want)
	}
}

func TestWriteFileAtomic_ReplacesAndCleansTmp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.json")
	if err := writeFileAtomic(path, []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(path, []byte(`{"a":2}`)); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"a":2}` {
		t.Fatalf("got %s", raw)
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("tmp file left behind")
	}
}

func TestScanRuns_SortedAscending(t *testing.T) {
	dir := t.TempDir()
	taskID := "task"
	for _, id := range []string{"01JZZZ", "01JAAA", "01JMMM"} {
		if err := os.MkdirAll(runDir(dir, taskID, id), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	ids, err := scanRuns(dir, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 || ids[0] != "01JAAA" || ids[2] != "01JZZZ" {
		t.Fatalf("order: %v", ids)
	}
}

func TestSaveLoadMeta(t *testing.T) {
	dir := t.TempDir()
	info := TaskInfo{
		TaskID:    "t",
		RunID:     "r",
		Name:      "n",
		Tags:      map[string]string{"k": "v"},
		Status:    StatusRunning,
		CreatedAt: time.Unix(100, 0).UTC(),
		UpdatedAt: time.Unix(200, 0).UTC(),
	}
	if err := saveMeta(dir, "t", "r", info); err != nil {
		t.Fatal(err)
	}
	got, ok, err := loadMeta(dir, "t", "r")
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if got.Name != "n" || got.Tags["k"] != "v" || got.Status != StatusRunning {
		t.Fatalf("got %+v", got)
	}
	_, ok, err = loadMeta(dir, "t", "missing")
	if err != nil || ok {
		t.Fatalf("missing: ok=%v err=%v", ok, err)
	}
}

func TestSaveLoadOutput(t *testing.T) {
	dir := t.TempDir()
	if err := saveOutput(dir, "t", "r", []byte(`{"x":1}`)); err != nil {
		t.Fatal(err)
	}
	got, err := loadOutput(dir, "t", "r")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"x":1}` {
		t.Fatalf("got %s", got)
	}
}
