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
		Version:     "2",
		Input:       []byte(`{"amount":10}`),
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

	got, size, err := readFrame(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if size != int64(len(frame)) {
		t.Fatalf("frame size %d, want %d", size, len(frame))
	}
	if got.GetStep() == nil {
		t.Fatal("expected step entry")
	}
	back := protoToStep(got.GetStep())
	if back.StepID != rec.StepID || back.Status != rec.Status || back.Version != rec.Version {
		t.Fatalf("round-trip mismatch: got %+v want %+v", back, rec)
	}
	if !bytes.Equal(back.Result, rec.Result) {
		t.Fatalf("result mismatch: %q vs %q", back.Result, rec.Result)
	}
	if !bytes.Equal(back.Input, rec.Input) {
		t.Fatalf("input mismatch: %q vs %q", back.Input, rec.Input)
	}
}

func TestReadFrame_EOFAtEnd(t *testing.T) {
	_, _, err := readFrame(bytes.NewReader(nil))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("empty reader: got %v, want io.EOF", err)
	}
}

func TestReadFrame_CRCCorruption(t *testing.T) {
	entry := &durablepb.JournalEntry{
		Entry: &durablepb.JournalEntry_Step{Step: stepToProto(StepRecord{StepID: "a", Status: StepStatusCompleted})},
	}
	payload, err := proto.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	frame := makeFrame(payload)
	frame[len(frame)-1] ^= 0xff

	_, _, err = readFrame(bytes.NewReader(frame))
	if !errors.Is(err, errCorruptFrame) {
		t.Fatalf("corrupt CRC: got %v, want errCorruptFrame", err)
	}
}

func TestReadFrame_PartialTail(t *testing.T) {
	entry := &durablepb.JournalEntry{
		Entry: &durablepb.JournalEntry_Step{Step: stepToProto(StepRecord{StepID: "a", Status: StepStatusCompleted})},
	}
	payload, err := proto.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	frame := makeFrame(payload)
	truncated := frame[:len(frame)/2]

	_, _, err = readFrame(bytes.NewReader(truncated))
	if !errors.Is(err, errCorruptFrame) && !errors.Is(err, io.EOF) {
		t.Fatalf("partial tail: got %v, want corrupt or EOF", err)
	}
}

func TestReadFrame_HugeLengthRejected(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], maxFramePayload+1)
	_, _, err := readFrame(bytes.NewReader(hdr[:]))
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
		StepID: "one", Status: StepStatusCompleted, Result: []byte(`"a"`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.appendStep(taskID, runID, StepRecord{
		StepID: "two", Status: StepStatusWaiting,
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

func TestLoadJournal_RunningStatusExcludedFromCache(t *testing.T) {
	dir := t.TempDir()
	taskID, runID := "t1", "r1"
	if err := os.MkdirAll(runDir(dir, taskID, runID), 0o755); err != nil {
		t.Fatal(err)
	}
	e := &Engine{dataDir: dir}
	if err := e.appendStep(taskID, runID, StepRecord{
		StepID: "stuck", Status: StepStatusRunning,
	}); err != nil {
		t.Fatal(err)
	}

	cache, _, err := loadJournal(dir, taskID, runID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cache["stuck"]; ok {
		t.Fatalf("StepStatusRunning must not appear in the replay map, got %+v", cache["stuck"])
	}
}

func TestLoadStepEvents_IncludesRunningInOrderWithOffsets(t *testing.T) {
	dir := t.TempDir()
	taskID, runID := "t1", "r1"
	if err := os.MkdirAll(runDir(dir, taskID, runID), 0o755); err != nil {
		t.Fatal(err)
	}
	e := &Engine{dataDir: dir}
	if err := e.appendStep(taskID, runID, StepRecord{StepID: "a", Status: StepStatusRunning}); err != nil {
		t.Fatal(err)
	}
	if err := e.appendStep(taskID, runID, StepRecord{StepID: "a", Status: StepStatusCompleted, Result: []byte(`"1"`)}); err != nil {
		t.Fatal(err)
	}
	if err := e.appendSignal(taskID, runID, "a", []byte(`"sig"`)); err != nil {
		t.Fatal(err)
	}
	if err := e.appendStep(taskID, runID, StepRecord{StepID: "b", Status: StepStatusRunning}); err != nil {
		t.Fatal(err)
	}

	events, err := loadStepEvents(dir, taskID, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("want 3 step events (signal excluded), got %d: %+v", len(events), events)
	}
	if events[0].StepID != "a" || events[0].Status != StepStatusRunning || events[0].Offset != 1 {
		t.Fatalf("event 0: %+v", events[0])
	}
	if events[1].StepID != "a" || events[1].Status != StepStatusCompleted || events[1].Offset != 2 {
		t.Fatalf("event 1: %+v", events[1])
	}
	// event index 3 is the signal (skipped in output); "b" STARTED is frame 4.
	if events[2].StepID != "b" || events[2].Status != StepStatusRunning || events[2].Offset != 4 {
		t.Fatalf("event 2: %+v", events[2])
	}
	if events[0].ByteOffset <= 0 || events[1].ByteOffset <= events[0].ByteOffset || events[2].ByteOffset <= events[1].ByteOffset {
		t.Fatalf("byte offsets must strictly increase: %+v", events)
	}
}

func TestJournalTail_ReflectsExistingFrames(t *testing.T) {
	dir := t.TempDir()
	taskID, runID := "t1", "r1"
	if err := os.MkdirAll(runDir(dir, taskID, runID), 0o755); err != nil {
		t.Fatal(err)
	}
	e := &Engine{dataDir: dir}
	if err := e.appendStep(taskID, runID, StepRecord{StepID: "a", Status: StepStatusCompleted, Result: []byte(`"x"`)}); err != nil {
		t.Fatal(err)
	}
	e.closeJournal(taskID, runID)

	count, size, err := journalTail(dir, taskID, runID)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || size <= 0 {
		t.Fatalf("count=%d size=%d", count, size)
	}

	// Reopening must pick up the existing tail rather than resetting to 0.
	h, err := e.getOrOpenJournal(taskID, runID)
	if err != nil {
		t.Fatal(err)
	}
	if h.count != 1 || h.size != size {
		t.Fatalf("reopened handle count=%d size=%d, want count=1 size=%d", h.count, h.size, size)
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
		StepID: "good", Status: StepStatusCompleted, Result: []byte(`"ok"`),
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

	if err := e.appendStep(taskID, runID, StepRecord{StepID: "s", Status: StepStatusWaiting}); err != nil {
		t.Fatal(err)
	}
	if err := e.appendStep(taskID, runID, StepRecord{StepID: "s", Status: StepStatusCompleted, Result: []byte(`"done"`)}); err != nil {
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

func TestCompactJournal_DropsRunningEntries(t *testing.T) {
	dir := t.TempDir()
	taskID, runID := "t1", "r1"
	e := &Engine{dataDir: dir}

	if err := e.appendStep(taskID, runID, StepRecord{StepID: "s", Status: StepStatusRunning}); err != nil {
		t.Fatal(err)
	}
	if err := e.appendStep(taskID, runID, StepRecord{StepID: "s", Status: StepStatusCompleted, Result: []byte(`"done"`)}); err != nil {
		t.Fatal(err)
	}
	if err := e.compactJournal(taskID, runID); err != nil {
		t.Fatal(err)
	}

	events, err := loadStepEvents(dir, taskID, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Status != StepStatusCompleted {
		t.Fatalf("compact must drop STARTED entries, got %+v", events)
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

func TestSaveLoadInput(t *testing.T) {
	dir := t.TempDir()
	if err := saveInput(dir, "t", "r", []byte(`{"goal":"x"}`)); err != nil {
		t.Fatal(err)
	}
	got, ok, err := loadInput(dir, "t", "r")
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if string(got) != `{"goal":"x"}` {
		t.Fatalf("got %s", got)
	}
	_, ok, err = loadInput(dir, "t", "missing")
	if err != nil || ok {
		t.Fatalf("missing: ok=%v err=%v", ok, err)
	}
}

func TestResolveRunInput_StoredWins(t *testing.T) {
	dir := t.TempDir()
	first, err := resolveRunInput(dir, "t", "r", []byte(`"keep"`))
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != `"keep"` {
		t.Fatalf("first %s", first)
	}
	second, err := resolveRunInput(dir, "t", "r", []byte(`"ignore"`))
	if err != nil {
		t.Fatal(err)
	}
	if string(second) != `"keep"` {
		t.Fatalf("stored-wins %s", second)
	}
}
