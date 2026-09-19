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
	frame := makeFrame(payload, nil, "", "", 1)

	got, size, err := readFrame(bytes.NewReader(frame), nil, "", "", 1)
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
	_, _, err := readFrame(bytes.NewReader(nil), nil, "", "", 1)
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
	frame := makeFrame(payload, nil, "", "", 1)
	frame[len(frame)-1] ^= 0xff

	_, _, err = readFrame(bytes.NewReader(frame), nil, "", "", 1)
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
	frame := makeFrame(payload, nil, "", "", 1)
	truncated := frame[:len(frame)/2]

	_, _, err = readFrame(bytes.NewReader(truncated), nil, "", "", 1)
	if !errors.Is(err, errCorruptFrame) && !errors.Is(err, io.EOF) {
		t.Fatalf("partial tail: got %v, want corrupt or EOF", err)
	}
}

func TestReadFrame_HugeLengthRejected(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], maxFramePayload+1)
	_, _, err := readFrame(bytes.NewReader(hdr[:]), nil, "", "", 1)
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

	cache, signals, err := loadJournal(dir, taskID, runID, nil, nil, nil)
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

	cache, _, err := loadJournal(dir, taskID, runID, nil, nil, nil)
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

	events, err := loadStepEvents(dir, taskID, runID, nil, nil, nil)
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

	count, size, err := journalTail(dir, taskID, runID, nil, nil)
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

	cache, _, err := loadJournal(dir, taskID, runID, nil, nil, nil)
	if err != nil {
		t.Fatalf("partial tail must not fail load: %v", err)
	}
	if cache["good"].Status != StepStatusCompleted {
		t.Fatalf("expected intact prefix, got %+v", cache)
	}
}

// A torn write must not strand every later append behind it: the frames
// after the tear are unreachable to replay, so the tear has to be cut off
// before the journal is reopened for append.
func TestGetOrOpenJournal_DiscardsTornTailBeforeAppending(t *testing.T) {
	dir := t.TempDir()
	taskID, runID := "t1", "r1"
	e := &Engine{dataDir: dir}
	if err := e.appendStep(taskID, runID, StepRecord{
		StepID: "first", Status: StepStatusCompleted, Result: []byte(`"1"`),
	}); err != nil {
		t.Fatal(err)
	}
	e.closeJournal(taskID, runID)

	path := journalPath(dir, taskID, runID)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	// A frame header claiming 0x40 payload bytes, with only two of them on disk.
	if _, err := f.Write([]byte{0x00, 0x00, 0x00, 0x40, 0xde, 0xad}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if err := e.appendStep(taskID, runID, StepRecord{
		StepID: "second", Status: StepStatusCompleted, Result: []byte(`"2"`),
	}); err != nil {
		t.Fatal(err)
	}

	cache, _, err := loadJournal(dir, taskID, runID, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cache["first"].Status != StepStatusCompleted {
		t.Fatalf("lost the frame before the tear: %+v", cache)
	}
	if cache["second"].Status != StepStatusCompleted {
		t.Fatalf("append after a torn tail is unreachable to replay: %+v", cache)
	}

	// The cached handle's byte position must match the file, or the
	// ByteOffset handed to WatchSteps stops lining up with the journal.
	h, err := e.getOrOpenJournal(taskID, runID)
	if err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if h.size != st.Size() {
		t.Fatalf("handle size %d, file size %d", h.size, st.Size())
	}
	e.closeJournal(taskID, runID)
}

// A payload readFrame would reject must fail the append instead of writing a
// frame that replay and compaction cannot read past.
func TestAppendStep_RejectsOversizePayload(t *testing.T) {
	dir := t.TempDir()
	e := &Engine{dataDir: dir}
	huge := make([]byte, maxFramePayload+1)
	for i := range huge {
		huge[i] = 'x'
	}
	err := e.appendStep("t1", "r1", StepRecord{
		StepID: "big", Status: StepStatusCompleted, Result: huge,
	})
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("got %v, want ErrPayloadTooLarge", err)
	}
	cache, loadErr := func() (map[string]StepRecord, error) {
		c, _, err := loadJournal(dir, "t1", "r1", nil, nil, nil)
		return c, err
	}()
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if len(cache) != 0 {
		t.Fatalf("rejected frame was still persisted: %+v", cache)
	}
}

// Corruption with intact frames after it is real damage, not an interrupted
// write. Stopping there silently would discard every later step.
func TestLoadJournal_InteriorCorruptionFailsClosed(t *testing.T) {
	dir := t.TempDir()
	taskID, runID := "t1", "r1"
	e := &Engine{dataDir: dir}
	for _, id := range []string{"a", "b", "c"} {
		if err := e.appendStep(taskID, runID, StepRecord{
			StepID: id, Status: StepStatusCompleted, Result: []byte(`"x"`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	e.closeJournal(taskID, runID)

	path := journalPath(dir, taskID, runID)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[frameLenSize+1] ^= 0xff // inside the first frame's payload
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, _, err := loadJournal(dir, taskID, runID, nil, nil, nil); !errors.Is(err, errCorruptInterior) {
		t.Fatalf("got %v, want errCorruptInterior", err)
	}
}

// The tail stays lenient: a corrupt final frame is what a crash mid-append
// looks like, so the intact prefix must still replay.
func TestLoadJournal_CorruptFinalFrameStillRecovers(t *testing.T) {
	dir := t.TempDir()
	taskID, runID := "t1", "r1"
	e := &Engine{dataDir: dir}
	for _, id := range []string{"a", "b"} {
		if err := e.appendStep(taskID, runID, StepRecord{
			StepID: id, Status: StepStatusCompleted, Result: []byte(`"x"`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	e.closeJournal(taskID, runID)

	path := journalPath(dir, taskID, runID)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xff // last frame's CRC trailer
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	cache, _, err := loadJournal(dir, taskID, runID, nil, nil, nil)
	if err != nil {
		t.Fatalf("corrupt final frame must not fail replay: %v", err)
	}
	if cache["a"].Status != StepStatusCompleted {
		t.Fatalf("intact prefix lost: %+v", cache)
	}
	if _, ok := cache["b"]; ok {
		t.Fatalf("corrupt final frame must be discarded: %+v", cache)
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

	steps, sigs, err := readJournalFrames(dir, taskID, runID, nil, nil)
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

	events, err := loadStepEvents(dir, taskID, runID, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Status != StepStatusCompleted {
		t.Fatalf("compact must drop STARTED entries, got %+v", events)
	}
}

func TestMakeFrame_CRCMatchesPayload(t *testing.T) {
	payload := []byte("hello")
	frame := makeFrame(payload, nil, "", "", 1)
	if int(binary.BigEndian.Uint32(frame[:4])) != len(payload) {
		t.Fatal("length prefix mismatch")
	}
	got := binary.BigEndian.Uint32(frame[4+len(payload):])
	want := crc32.ChecksumIEEE(payload)
	if got != want {
		t.Fatalf("crc %d != %d", got, want)
	}
}

func TestMakeReadFrame_HMACRoundTrip(t *testing.T) {
	key := []byte("journal-mac-key")
	rec := StepRecord{StepID: "echo", Status: StepStatusCompleted, Result: []byte(`"ok"`)}
	entry := &durablepb.JournalEntry{
		Entry: &durablepb.JournalEntry_Step{Step: stepToProto(rec)},
	}
	payload, err := proto.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	frame := makeFrame(payload, key, "t", "r", 1)
	if len(frame) != frameLenSize+len(payload)+frameHMACSize {
		t.Fatalf("frame len %d", len(frame))
	}
	got, size, err := readFrame(bytes.NewReader(frame), key, "t", "r", 1)
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(frame)) {
		t.Fatalf("size %d want %d", size, len(frame))
	}
	if got.GetStep().GetStepId() != "echo" {
		t.Fatalf("step %s", got.GetStep().GetStepId())
	}
}

func TestReadFrame_HMACMismatch(t *testing.T) {
	key := []byte("journal-mac-key")
	entry := &durablepb.JournalEntry{
		Entry: &durablepb.JournalEntry_Step{Step: stepToProto(StepRecord{StepID: "a", Status: StepStatusCompleted, Result: []byte(`"ok"`)})},
	}
	payload, err := proto.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	frame := makeFrame(payload, key, "t", "r", 1)
	frame[4] ^= 0xff
	_, _, err = readFrame(bytes.NewReader(frame), key, "t", "r", 1)
	if !errors.Is(err, errJournalMAC) {
		t.Fatalf("got %v, want errJournalMAC", err)
	}

	_, _, err = readFrame(bytes.NewReader(frame), []byte("other-key"), "t", "r", 1)
	if !errors.Is(err, errJournalMAC) {
		t.Fatalf("wrong key: got %v, want errJournalMAC", err)
	}
}

func TestReadFrame_HMACBoundToTaskRunAndIndex(t *testing.T) {
	key := []byte("journal-mac-key")
	entry := &durablepb.JournalEntry{
		Entry: &durablepb.JournalEntry_Step{Step: stepToProto(StepRecord{StepID: "a", Status: StepStatusCompleted, Result: []byte(`"ok"`)})},
	}
	payload, err := proto.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	frame := makeFrame(payload, key, "task-a", "run-a", 1)

	if _, _, err := readFrame(bytes.NewReader(frame), key, "task-b", "run-a", 1); !errors.Is(err, errJournalMAC) {
		t.Fatalf("copied to another task: got %v, want errJournalMAC", err)
	}
	if _, _, err := readFrame(bytes.NewReader(frame), key, "task-a", "run-b", 1); !errors.Is(err, errJournalMAC) {
		t.Fatalf("copied to another run: got %v, want errJournalMAC", err)
	}
	if _, _, err := readFrame(bytes.NewReader(frame), key, "task-a", "run-a", 2); !errors.Is(err, errJournalMAC) {
		t.Fatalf("reordered index: got %v, want errJournalMAC", err)
	}
	if _, _, err := readFrame(bytes.NewReader(frame), key, "task-a", "run-a", 1); err != nil {
		t.Fatalf("matching location: %v", err)
	}
}

func TestWriteFileAtomic_RestrictsMode(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "run", "meta.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(path, []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	assertPerm(t, filepath.Dir(path), dirPerm)
	assertPerm(t, path, filePerm)
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

func TestSyncDir(t *testing.T) {
	dir := t.TempDir()
	if err := syncDir(dir); err != nil {
		t.Fatal(err)
	}
	if err := syncDir(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("expected error for missing dir")
	}
}

func TestGetOrOpenJournal_CreatesJournal(t *testing.T) {
	dir := t.TempDir()
	e := &Engine{dataDir: dir}
	h, err := e.getOrOpenJournal("t1", "r1")
	if err != nil {
		t.Fatal(err)
	}
	if h.count != 0 || h.size != 0 {
		t.Fatalf("new journal count=%d size=%d", h.count, h.size)
	}
	path := journalPath(dir, "t1", "r1")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("journal.log missing after create: %v", err)
	}
	e.closeJournal("t1", "r1")
}

func TestGetOrOpenJournal_RestrictsMode(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	e := &Engine{dataDir: dir}
	if _, err := e.getOrOpenJournal("t1", "r1"); err != nil {
		t.Fatal(err)
	}
	assertPerm(t, runDir(dir, "t1", "r1"), dirPerm)
	assertPerm(t, journalPath(dir, "t1", "r1"), filePerm)
	e.closeJournal("t1", "r1")
}

func withSyncDirFn(t *testing.T, fn func(string) error) {
	t.Helper()
	orig := syncDirFn
	syncDirFn = fn
	t.Cleanup(func() { syncDirFn = orig })
}

func TestWriteFileAtomic_SyncsParentDir(t *testing.T) {
	dir := t.TempDir()
	var synced []string
	withSyncDirFn(t, func(d string) error {
		synced = append(synced, d)
		return syncDir(d)
	})

	path := filepath.Join(dir, "tasks", "t", "r", "meta.json")
	if err := writeFileAtomic(path, []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if len(synced) != 1 || synced[0] != filepath.Dir(path) {
		t.Fatalf("synced %v, want parent of %s", synced, path)
	}
}

func TestWriteFileAtomic_DirSyncError(t *testing.T) {
	withSyncDirFn(t, func(string) error {
		return errors.New("dir sync failed")
	})
	path := filepath.Join(t.TempDir(), "meta.json")
	if err := writeFileAtomic(path, []byte(`{}`)); err == nil {
		t.Fatal("expected dir sync error")
	}
}

func TestGetOrOpenJournal_SyncsDirOnCreateOnly(t *testing.T) {
	dir := t.TempDir()
	var n int
	withSyncDirFn(t, func(d string) error {
		n++
		return syncDir(d)
	})

	e := &Engine{dataDir: dir}
	if _, err := e.getOrOpenJournal("t1", "r1"); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("create dirsync count=%d, want 1", n)
	}
	e.closeJournal("t1", "r1")

	if _, err := e.getOrOpenJournal("t1", "r1"); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("reopen dirsync count=%d, want 1", n)
	}
	e.closeJournal("t1", "r1")
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
	if err := saveMeta(dir, "t", "r", info, nil); err != nil {
		t.Fatal(err)
	}
	got, ok, err := loadMeta(dir, "t", "r", nil)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if got.Name != "n" || got.Tags["k"] != "v" || got.Status != StatusRunning {
		t.Fatalf("got %+v", got)
	}
	_, ok, err = loadMeta(dir, "t", "missing", nil)
	if err != nil || ok {
		t.Fatalf("missing: ok=%v err=%v", ok, err)
	}
}

func TestSaveLoadOutput(t *testing.T) {
	dir := t.TempDir()
	if err := saveOutput(dir, "t", "r", []byte(`{"x":1}`), nil, nil); err != nil {
		t.Fatal(err)
	}
	got, err := loadOutput(dir, "t", "r", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"x":1}` {
		t.Fatalf("got %s", got)
	}
}

func TestSaveLoadInput(t *testing.T) {
	dir := t.TempDir()
	if err := saveInput(dir, "t", "r", []byte(`{"goal":"x"}`), nil, nil); err != nil {
		t.Fatal(err)
	}
	got, ok, err := loadInput(dir, "t", "r", nil, nil)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if string(got) != `{"goal":"x"}` {
		t.Fatalf("got %s", got)
	}
	_, ok, err = loadInput(dir, "t", "missing", nil, nil)
	if err != nil || ok {
		t.Fatalf("missing: ok=%v err=%v", ok, err)
	}
}

func TestResolveRunInput_StoredWins(t *testing.T) {
	dir := t.TempDir()
	first, err := resolveRunInput(dir, "t", "r", []byte(`"keep"`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != `"keep"` {
		t.Fatalf("first %s", first)
	}
	second, err := resolveRunInput(dir, "t", "r", []byte(`"ignore"`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(second) != `"keep"` {
		t.Fatalf("stored-wins %s", second)
	}
}

func TestSidecarMAC_RoundTripAndRejectTamper(t *testing.T) {
	dir := t.TempDir()
	key := []byte("sidecar-mac-key")
	info := TaskInfo{TaskID: "t", RunID: "r", Status: StatusCompleted}
	if err := saveMeta(dir, "t", "r", info, key); err != nil {
		t.Fatal(err)
	}
	got, ok, err := loadMeta(dir, "t", "r", key)
	if err != nil || !ok || got.Status != StatusCompleted {
		t.Fatalf("meta ok=%v err=%v %+v", ok, err, got)
	}
	if _, _, err := loadMeta(dir, "t", "r", []byte("other")); !errors.Is(err, errFileMAC) {
		t.Fatalf("wrong key: %v", err)
	}

	p := metaPath(dir, "t", "r")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] ^= 0xff
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadMeta(dir, "t", "r", key); !errors.Is(err, errFileMAC) {
		t.Fatalf("tamper: %v", err)
	}

	if err := saveOutput(dir, "t", "r", []byte(`"ok"`), nil, key); err != nil {
		t.Fatal(err)
	}
	if err := saveOutput(dir, "t", "other", []byte(`"ok"`), nil, key); err != nil {
		t.Fatal(err)
	}
	stolen, err := os.ReadFile(outputPath(dir, "t", "other"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outputPath(dir, "t", "r"), stolen, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadOutput(dir, "t", "r", nil, key); !errors.Is(err, errFileMAC) {
		t.Fatalf("copied output: %v", err)
	}

	if err := saveInput(dir, "t", "r", []byte(`"in"`), nil, key); err != nil {
		t.Fatal(err)
	}
	in, ok, err := loadInput(dir, "t", "r", nil, key)
	if err != nil || !ok || string(in) != `"in"` {
		t.Fatalf("input %q ok=%v err=%v", in, ok, err)
	}
}
