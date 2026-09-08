package durable

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	durablepb "github.com/agenticenv/durable-go/pb"
)

// Frame layout: [4B uint32 BE payload length][protobuf JournalEntry][4B CRC32-IEEE].
// Length-prefixing lets a reader skip unknown future entry types. CRC32 detects
// a torn write from kill -9 or power loss so replay can stop at the last
// intact frame instead of treating garbage as data.
const (
	frameLenSize    = 4
	frameCRCSize    = 4
	frameOverhead   = frameLenSize + frameCRCSize
	maxFramePayload = 32 << 20 // 32 MiB — reject corrupt lengths that would OOM
)

var errCorruptFrame = errors.New("durable: corrupt or truncated journal frame")

// journalFile is a cached append handle. CompleteStep writes SignalEntry
// without holding runLocks (the waiter already holds that lock), so each
// handle has its own mutex to serialise appends from the runner and from
// CompleteStep.
type journalFile struct {
	mu sync.Mutex
	f  *os.File
}

func (e *Engine) saveMeta(taskID, runID string, info TaskInfo) error {
	return saveMeta(e.dataDir, taskID, runID, info)
}

func (e *Engine) loadMeta(taskID, runID string) (TaskInfo, bool, error) {
	return loadMeta(e.dataDir, taskID, runID)
}

func (e *Engine) saveOutput(taskID, runID string, output []byte) error {
	return saveOutput(e.dataDir, taskID, runID, output)
}

func (e *Engine) loadOutput(taskID, runID string) ([]byte, error) {
	return loadOutput(e.dataDir, taskID, runID)
}

func (e *Engine) loadJournal(taskID, runID string) (map[string]StepRecord, map[string][]byte, error) {
	return loadJournal(e.dataDir, taskID, runID)
}

func (e *Engine) scanRuns(taskID string) ([]string, error) {
	return scanRuns(e.dataDir, taskID)
}

func saveMeta(dataDir, taskID, runID string, info TaskInfo) error {
	raw, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("durable: marshal meta %s/%s: %w", taskID, runID, err)
	}
	if err := writeFileAtomic(metaPath(dataDir, taskID, runID), raw); err != nil {
		return fmt.Errorf("durable: write meta %s/%s: %w", taskID, runID, err)
	}
	return nil
}

func loadMeta(dataDir, taskID, runID string) (TaskInfo, bool, error) {
	raw, err := os.ReadFile(metaPath(dataDir, taskID, runID))
	if errors.Is(err, os.ErrNotExist) {
		return TaskInfo{}, false, nil
	}
	if err != nil {
		return TaskInfo{}, false, fmt.Errorf("durable: read meta %s/%s: %w", taskID, runID, err)
	}
	var info TaskInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return TaskInfo{}, false, fmt.Errorf("durable: unmarshal meta %s/%s: %w", taskID, runID, err)
	}
	return info, true, nil
}

func saveOutput(dataDir, taskID, runID string, output []byte) error {
	if err := writeFileAtomic(outputPath(dataDir, taskID, runID), output); err != nil {
		return fmt.Errorf("durable: write output %s/%s: %w", taskID, runID, err)
	}
	return nil
}

func loadOutput(dataDir, taskID, runID string) ([]byte, error) {
	raw, err := os.ReadFile(outputPath(dataDir, taskID, runID))
	if err != nil {
		return nil, fmt.Errorf("durable: read output %s/%s: %w", taskID, runID, err)
	}
	return raw, nil
}

// writeFileAtomic persists data via tmp+sync+rename so a crash mid-write
// leaves either the previous file or the new one — never a half-written
// meta.json or output.json.
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
	return nil
}

func (e *Engine) appendStep(taskID, runID string, step StepRecord) error {
	entry := &durablepb.JournalEntry{
		Entry: &durablepb.JournalEntry_Step{Step: stepToProto(step)},
	}
	if err := e.appendFrame(taskID, runID, entry); err != nil {
		return fmt.Errorf("durable: append step %s/%s/%s: %w", taskID, runID, step.StepID, err)
	}
	return nil
}

func (e *Engine) appendSignal(taskID, runID, stepID string, payload []byte) error {
	entry := &durablepb.JournalEntry{
		Entry: &durablepb.JournalEntry_Signal{
			Signal: &durablepb.SignalEntry{
				SignalId: stepID,
				Payload:  payload,
				SentAtNs: time.Now().UTC().UnixNano(),
			},
		},
	}
	if err := e.appendFrame(taskID, runID, entry); err != nil {
		return fmt.Errorf("durable: append signal %s/%s/%s: %w", taskID, runID, stepID, err)
	}
	return nil
}

func (e *Engine) appendFrame(taskID, runID string, entry *durablepb.JournalEntry) error {
	payload, err := proto.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal journal entry: %w", err)
	}
	frame := makeFrame(payload)

	h, err := e.getOrOpenJournal(taskID, runID)
	if err != nil {
		return err
	}
	key := taskID + "/" + runID
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, err := h.f.Write(frame); err != nil {
		_ = h.f.Close()
		e.openFiles.Delete(key)
		return fmt.Errorf("write frame: %w", err)
	}
	if err := h.f.Sync(); err != nil {
		_ = h.f.Close()
		e.openFiles.Delete(key)
		return fmt.Errorf("sync journal: %w", err)
	}
	return nil
}

func (e *Engine) getOrOpenJournal(taskID, runID string) (*journalFile, error) {
	key := taskID + "/" + runID
	if v, ok := e.openFiles.Load(key); ok {
		return v.(*journalFile), nil
	}
	if err := os.MkdirAll(e.runDir(taskID, runID), 0o755); err != nil {
		return nil, fmt.Errorf("create run dir: %w", err)
	}
	f, err := os.OpenFile(e.journalPath(taskID, runID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}
	h := &journalFile{f: f}
	actual, loaded := e.openFiles.LoadOrStore(key, h)
	if loaded {
		_ = f.Close()
		return actual.(*journalFile), nil
	}
	return h, nil
}

func (e *Engine) closeJournal(taskID, runID string) {
	key := taskID + "/" + runID
	if v, ok := e.openFiles.LoadAndDelete(key); ok {
		h := v.(*journalFile)
		h.mu.Lock()
		_ = h.f.Close()
		h.mu.Unlock()
	}
}

func (e *Engine) closeAllJournals() {
	e.openFiles.Range(func(key, value any) bool {
		h := value.(*journalFile)
		h.mu.Lock()
		_ = h.f.Close()
		h.mu.Unlock()
		e.openFiles.Delete(key)
		return true
	})
}

// loadJournal replays journal.log and returns the latest StepRecord per stepID
// plus the latest SignalEntry payload per stepID. A torn or CRC-mismatched
// tail is discarded so a crash mid-append cannot poison replay.
func loadJournal(dataDir, taskID, runID string) (map[string]StepRecord, map[string][]byte, error) {
	steps, sigs, err := readJournalFrames(dataDir, taskID, runID)
	if err != nil {
		return nil, nil, err
	}
	payloads := make(map[string][]byte, len(sigs))
	for _, s := range sigs {
		payloads[s.GetSignalId()] = s.GetPayload()
	}
	return steps, payloads, nil
}

func readJournalFrames(dataDir, taskID, runID string) (map[string]StepRecord, []*durablepb.SignalEntry, error) {
	path := journalPath(dataDir, taskID, runID)
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return make(map[string]StepRecord), nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("durable: open journal %s/%s: %w", taskID, runID, err)
	}
	defer func() { _ = f.Close() }()

	steps := make(map[string]StepRecord)
	var signals []*durablepb.SignalEntry
	for {
		entry, err := readFrame(f)
		if err != nil {
			// io.EOF is a clean end. Corrupt/truncated tail is also treated
			// as end-of-valid-data so a kill -9 mid-write cannot fail resume.
			if errors.Is(err, io.EOF) || errors.Is(err, errCorruptFrame) {
				break
			}
			return nil, nil, fmt.Errorf("durable: read journal %s/%s: %w", taskID, runID, err)
		}
		switch e := entry.Entry.(type) {
		case *durablepb.JournalEntry_Step:
			if e.Step != nil {
				rec := protoToStep(e.Step)
				steps[rec.StepID] = rec
			}
		case *durablepb.JournalEntry_Signal:
			if e.Signal != nil {
				signals = append(signals, e.Signal)
			}
		}
	}
	return steps, signals, nil
}

// compactJournal rewrites journal.log after a run reaches a terminal state so
// the file does not grow without bound across retries. Latest StepEntry per
// stepID is kept (last-write-wins already applied by replay) plus every
// SignalEntry — signals are an audit of external completions and must not
// be collapsed away.
func (e *Engine) compactJournal(taskID, runID string) error {
	steps, signals, err := readJournalFrames(e.dataDir, taskID, runID)
	if err != nil {
		return fmt.Errorf("durable: compact read %s/%s: %w", taskID, runID, err)
	}

	recs := make([]StepRecord, 0, len(steps))
	for _, rec := range steps {
		recs = append(recs, rec)
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].Seq < recs[j].Seq })

	var buf []byte
	for _, rec := range recs {
		entry := &durablepb.JournalEntry{
			Entry: &durablepb.JournalEntry_Step{Step: stepToProto(rec)},
		}
		payload, err := proto.Marshal(entry)
		if err != nil {
			return fmt.Errorf("durable: compact marshal step %s/%s: %w", taskID, runID, err)
		}
		buf = append(buf, makeFrame(payload)...)
	}
	for _, sig := range signals {
		entry := &durablepb.JournalEntry{
			Entry: &durablepb.JournalEntry_Signal{Signal: sig},
		}
		payload, err := proto.Marshal(entry)
		if err != nil {
			return fmt.Errorf("durable: compact marshal signal %s/%s: %w", taskID, runID, err)
		}
		buf = append(buf, makeFrame(payload)...)
	}

	e.closeJournal(taskID, runID)
	if err := writeFileAtomic(e.journalPath(taskID, runID), buf); err != nil {
		return fmt.Errorf("durable: compact write %s/%s: %w", taskID, runID, err)
	}
	return nil
}

// scanRuns lists runID directories under taskID, sorted ascending. ULID
// strings are lexicographically time-ordered, so the first element is the
// oldest run — resolveRunID uses that to drain a stale singleton first.
func scanRuns(dataDir, taskID string) ([]string, error) {
	entries, err := os.ReadDir(taskDir(dataDir, taskID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("durable: scan runs %s: %w", taskID, err)
	}
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			ids = append(ids, e.Name())
		}
	}
	sort.Strings(ids)
	return ids, nil
}

func stepStatusToProto(s StepStatus) durablepb.StepStatus {
	switch s {
	case StepStatusWaiting:
		return durablepb.StepStatus_STEP_STATUS_WAITING
	case StepStatusCompleted:
		return durablepb.StepStatus_STEP_STATUS_COMPLETED
	case StepStatusFailed:
		return durablepb.StepStatus_STEP_STATUS_FAILED
	default:
		return durablepb.StepStatus_STEP_STATUS_UNSPECIFIED
	}
}

func protoStatusToStep(s durablepb.StepStatus) StepStatus {
	switch s {
	case durablepb.StepStatus_STEP_STATUS_WAITING:
		return StepStatusWaiting
	case durablepb.StepStatus_STEP_STATUS_COMPLETED:
		return StepStatusCompleted
	case durablepb.StepStatus_STEP_STATUS_FAILED:
		return StepStatusFailed
	default:
		return ""
	}
}

func stepToProto(r StepRecord) *durablepb.StepEntry {
	e := &durablepb.StepEntry{
		StepId:     r.StepID,
		Seq:        int32(r.Seq),
		Status:     stepStatusToProto(r.Status),
		Result:     r.Result,
		Error:      r.Error,
		PanicTrace: r.PanicTrace,
		InputHash:  r.InputHash,
	}
	if !r.StartedAt.IsZero() {
		e.StartedAtNs = r.StartedAt.UnixNano()
	}
	if !r.CompletedAt.IsZero() {
		e.CompletedAtNs = r.CompletedAt.UnixNano()
	}
	return e
}

func protoToStep(e *durablepb.StepEntry) StepRecord {
	r := StepRecord{
		StepID:     e.GetStepId(),
		Seq:        int(e.GetSeq()),
		Status:     protoStatusToStep(e.GetStatus()),
		Result:     e.GetResult(),
		Error:      e.GetError(),
		PanicTrace: e.GetPanicTrace(),
		InputHash:  e.GetInputHash(),
	}
	if e.GetStartedAtNs() != 0 {
		r.StartedAt = time.Unix(0, e.GetStartedAtNs()).UTC()
	}
	if e.GetCompletedAtNs() != 0 {
		r.CompletedAt = time.Unix(0, e.GetCompletedAtNs()).UTC()
	}
	return r
}

func makeFrame(payload []byte) []byte {
	frame := make([]byte, frameOverhead+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	copy(frame[4:], payload)
	checksum := crc32.ChecksumIEEE(payload)
	binary.BigEndian.PutUint32(frame[4+len(payload):], checksum)
	return frame
}

// readFrame reads one CRC-framed JournalEntry. It returns io.EOF at a clean
// end of file, and errCorruptFrame when the next bytes are a torn write or
// CRC mismatch so the caller can stop without failing the whole replay.
func readFrame(r io.Reader) (*durablepb.JournalEntry, error) {
	var lenBuf [frameLenSize]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, io.EOF
		}
		return nil, err
	}
	payloadLen := binary.BigEndian.Uint32(lenBuf[:])
	if payloadLen > maxFramePayload {
		return nil, errCorruptFrame
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, errCorruptFrame
	}

	var crcBuf [frameCRCSize]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return nil, errCorruptFrame
	}
	storedCRC := binary.BigEndian.Uint32(crcBuf[:])
	if storedCRC != crc32.ChecksumIEEE(payload) {
		return nil, errCorruptFrame
	}

	var entry durablepb.JournalEntry
	if err := proto.Unmarshal(payload, &entry); err != nil {
		return nil, errCorruptFrame
	}
	return &entry, nil
}
