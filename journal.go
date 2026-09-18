package durable

import (
	"crypto/hmac"
	"crypto/sha256"
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

// Frame layout: [4B uint32 BE payload length][protobuf JournalEntry][trailer].
// Trailer is 4B CRC32-IEEE by default, or 32B HMAC-SHA256 when
// WithJournalMACKey is set. Length-prefixing lets a reader skip unknown
// future entry types. CRC32 detects a torn write from kill -9 or power
// loss so replay can stop at the last intact frame. HMAC also detects
// that, and rejects an editor who rewrites the protobuf and recomputes CRC32.
const (
	frameLenSize        = 4
	frameCRCSize        = 4
	frameHMACSize       = sha256.Size
	frameOverhead       = frameLenSize + frameCRCSize // default (CRC) overhead
	maxFramePayload     = 32 << 20                    // 32 MiB — reject corrupt lengths that would OOM
	journalMACDomain    = "durable.journal.v1"
	fileMACDomainMeta   = "durable.meta.v1"
	fileMACDomainInput  = "durable.input.v1"
	fileMACDomainOutput = "durable.output.v1"
)

var (
	errCorruptFrame = errors.New("durable: corrupt or truncated journal frame")
	errJournalMAC   = errors.New("durable: journal MAC mismatch")
	errFileMAC      = errors.New("durable: file MAC mismatch")
)

// journalFile is a cached append handle. CompleteStep writes SignalEntry
// without holding runLocks (the waiter already holds that lock), so each
// handle has its own mutex to serialise appends from the runner and from
// CompleteStep. count and size track the running frame index and byte
// position so appendFrame can report an Offset/ByteOffset for WatchSteps
// without rescanning the file on every write.
type journalFile struct {
	mu    sync.Mutex
	f     *os.File
	count int
	size  int64
}

func (e *Engine) saveMeta(taskID, runID string, info TaskInfo) error {
	return saveMeta(e.dataDir, taskID, runID, info, e.journalMACKey())
}

func (e *Engine) loadMeta(taskID, runID string) (TaskInfo, bool, error) {
	return loadMeta(e.dataDir, taskID, runID, e.journalMACKey())
}

func (e *Engine) saveOutput(taskID, runID string, output []byte) error {
	return saveOutput(e.dataDir, taskID, runID, output, e.codec(), e.journalMACKey())
}

func (e *Engine) loadOutput(taskID, runID string) ([]byte, error) {
	return loadOutput(e.dataDir, taskID, runID, e.codec(), e.journalMACKey())
}

func (e *Engine) resolveRunInput(taskID, runID string, caller []byte) ([]byte, error) {
	return resolveRunInput(e.dataDir, taskID, runID, caller, e.codec(), e.journalMACKey())
}

func (e *Engine) loadJournal(taskID, runID string) (map[string]StepRecord, map[string][]byte, error) {
	return loadJournal(e.dataDir, taskID, runID, e.codec(), e.journalMACKey())
}

func (e *Engine) loadStepEvents(taskID, runID string) ([]StepEvent, error) {
	return loadStepEvents(e.dataDir, taskID, runID, e.codec(), e.journalMACKey())
}

func (e *Engine) scanRuns(taskID string) ([]string, error) {
	return scanRuns(e.dataDir, taskID)
}

func saveMeta(dataDir, taskID, runID string, info TaskInfo, macKey []byte) error {
	raw, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("durable: marshal meta %s/%s: %w", taskID, runID, err)
	}
	raw = wrapSidecarMAC(macKey, fileMACDomainMeta, taskID, runID, raw)
	if err := writeFileAtomic(metaPath(dataDir, taskID, runID), raw); err != nil {
		return fmt.Errorf("durable: write meta %s/%s: %w", taskID, runID, err)
	}
	return nil
}

func loadMeta(dataDir, taskID, runID string, macKey []byte) (TaskInfo, bool, error) {
	raw, err := os.ReadFile(metaPath(dataDir, taskID, runID))
	if errors.Is(err, os.ErrNotExist) {
		return TaskInfo{}, false, nil
	}
	if err != nil {
		return TaskInfo{}, false, fmt.Errorf("durable: read meta %s/%s: %w", taskID, runID, err)
	}
	raw, err = unwrapSidecarMAC(macKey, fileMACDomainMeta, taskID, runID, raw)
	if err != nil {
		return TaskInfo{}, false, fmt.Errorf("durable: verify meta %s/%s: %w", taskID, runID, err)
	}
	var info TaskInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return TaskInfo{}, false, fmt.Errorf("durable: unmarshal meta %s/%s: %w", taskID, runID, err)
	}
	return info, true, nil
}

func saveOutput(dataDir, taskID, runID string, output []byte, c PayloadCodec, macKey []byte) error {
	stored, err := encodePayload(c, output, payloadAAD(aadKindTaskOutput, taskID, runID, ""))
	if err != nil {
		return err
	}
	stored = wrapSidecarMAC(macKey, fileMACDomainOutput, taskID, runID, stored)
	if err := writeFileAtomic(outputPath(dataDir, taskID, runID), stored); err != nil {
		return fmt.Errorf("durable: write output %s/%s: %w", taskID, runID, err)
	}
	return nil
}

func loadOutput(dataDir, taskID, runID string, c PayloadCodec, macKey []byte) ([]byte, error) {
	raw, err := os.ReadFile(outputPath(dataDir, taskID, runID))
	if err != nil {
		return nil, fmt.Errorf("durable: read output %s/%s: %w", taskID, runID, err)
	}
	raw, err = unwrapSidecarMAC(macKey, fileMACDomainOutput, taskID, runID, raw)
	if err != nil {
		return nil, fmt.Errorf("durable: verify output %s/%s: %w", taskID, runID, err)
	}
	return decodePayload(c, raw, payloadAAD(aadKindTaskOutput, taskID, runID, ""))
}

func saveInput(dataDir, taskID, runID string, input []byte, c PayloadCodec, macKey []byte) error {
	stored, err := encodePayload(c, input, payloadAAD(aadKindTaskInput, taskID, runID, ""))
	if err != nil {
		return err
	}
	stored = wrapSidecarMAC(macKey, fileMACDomainInput, taskID, runID, stored)
	if err := writeFileAtomic(inputPath(dataDir, taskID, runID), stored); err != nil {
		return fmt.Errorf("durable: write input %s/%s: %w", taskID, runID, err)
	}
	return nil
}

func loadInput(dataDir, taskID, runID string, c PayloadCodec, macKey []byte) ([]byte, bool, error) {
	raw, err := os.ReadFile(inputPath(dataDir, taskID, runID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("durable: read input %s/%s: %w", taskID, runID, err)
	}
	raw, err = unwrapSidecarMAC(macKey, fileMACDomainInput, taskID, runID, raw)
	if err != nil {
		return nil, false, fmt.Errorf("durable: verify input %s/%s: %w", taskID, runID, err)
	}
	decoded, err := decodePayload(c, raw, payloadAAD(aadKindTaskInput, taskID, runID, ""))
	if err != nil {
		return nil, false, err
	}
	return decoded, true, nil
}

// resolveRunInput returns the stored input when input.json exists (resume).
// Otherwise it persists caller bytes and returns them (first start, or a
// pre-v1 run that has no input file).
func resolveRunInput(dataDir, taskID, runID string, caller []byte, c PayloadCodec, macKey []byte) ([]byte, error) {
	stored, ok, err := loadInput(dataDir, taskID, runID, c, macKey)
	if err != nil {
		return nil, err
	}
	if ok {
		return stored, nil
	}
	if err := saveInput(dataDir, taskID, runID, caller, c, macKey); err != nil {
		return nil, err
	}
	return caller, nil
}

// writeFileAtomic persists data via tmp+sync+rename+dirsync so a crash
// mid-write leaves either the previous file or the new one — never a
// half-written meta.json, input.json, or output.json. The parent
// directory is synced after rename so the directory entry survives
// power loss.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := mkdirAllSecure(dir); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := openFileSecure(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
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
	chmodBestEffort(path, filePerm)
	return syncDirFn(dir)
}

// syncDirFn is the directory fsync used after rename and after first
// journal.log create. Tests swap it to assert those calls.
var syncDirFn = syncDir

// syncDir fsyncs a directory so a preceding create or rename is durable
// across power loss. File-only Sync is not enough: the directory entry
// can still be in the page cache.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

func (e *Engine) appendStep(taskID, runID string, step StepRecord) error {
	stored, err := encodeStepRecord(e.codec(), taskID, runID, step)
	if err != nil {
		return fmt.Errorf("durable: append step %s/%s/%s: %w", taskID, runID, step.StepID, err)
	}
	entry := &durablepb.JournalEntry{
		Entry: &durablepb.JournalEntry_Step{Step: stepToProto(stored)},
	}
	offset, byteOffset, err := e.appendFrame(taskID, runID, entry)
	if err != nil {
		return fmt.Errorf("durable: append step %s/%s/%s: %w", taskID, runID, step.StepID, err)
	}
	e.notifyStepWatchers(taskID, runID, StepEvent{StepRecord: step, Offset: offset, ByteOffset: byteOffset})
	return nil
}

func (e *Engine) appendSignal(taskID, runID, stepID string, payload []byte) error {
	stored, err := encodePayload(e.codec(), payload, payloadAAD(aadKindSignal, taskID, runID, stepID))
	if err != nil {
		return fmt.Errorf("durable: append signal %s/%s/%s: %w", taskID, runID, stepID, err)
	}
	entry := &durablepb.JournalEntry{
		Entry: &durablepb.JournalEntry_Signal{
			Signal: &durablepb.SignalEntry{
				SignalId: stepID,
				Payload:  stored,
				SentAtNs: time.Now().UTC().UnixNano(),
			},
		},
	}
	if _, _, err := e.appendFrame(taskID, runID, entry); err != nil {
		return fmt.Errorf("durable: append signal %s/%s/%s: %w", taskID, runID, stepID, err)
	}
	return nil
}

// appendFrame writes entry and returns the frame's 1-based index and the
// byte offset immediately after it — both counted across every frame in
// the journal (steps and signals share one counter), so a caller that
// reconnects with either value skips exactly what it has already seen.
func (e *Engine) appendFrame(taskID, runID string, entry *durablepb.JournalEntry) (int, int64, error) {
	payload, err := proto.Marshal(entry)
	if err != nil {
		return 0, 0, fmt.Errorf("marshal journal entry: %w", err)
	}
	frame := makeFrame(payload, e.journalMACKey())

	h, err := e.getOrOpenJournal(taskID, runID)
	if err != nil {
		return 0, 0, err
	}
	key := taskID + "/" + runID
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, err := h.f.Write(frame); err != nil {
		_ = h.f.Close()
		e.openFiles.Delete(key)
		return 0, 0, fmt.Errorf("write frame: %w", err)
	}
	if err := h.f.Sync(); err != nil {
		_ = h.f.Close()
		e.openFiles.Delete(key)
		return 0, 0, fmt.Errorf("sync journal: %w", err)
	}
	h.count++
	h.size += int64(len(frame))
	return h.count, h.size, nil
}

func (e *Engine) getOrOpenJournal(taskID, runID string) (*journalFile, error) {
	key := taskID + "/" + runID
	if v, ok := e.openFiles.Load(key); ok {
		return v.(*journalFile), nil
	}
	if err := mkdirAllSecure(e.runDir(taskID, runID)); err != nil {
		return nil, fmt.Errorf("create run dir: %w", err)
	}
	count, size, err := journalTail(e.dataDir, taskID, runID, e.journalMACKey())
	if err != nil {
		return nil, fmt.Errorf("scan journal tail: %w", err)
	}
	path := e.journalPath(taskID, runID)
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !created {
		return nil, fmt.Errorf("stat journal: %w", statErr)
	}
	f, err := openFileSecure(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY)
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}
	h := &journalFile{f: f, count: count, size: size}
	actual, loaded := e.openFiles.LoadOrStore(key, h)
	if loaded {
		_ = f.Close()
		return actual.(*journalFile), nil
	}
	if created {
		if err := syncDirFn(filepath.Dir(path)); err != nil {
			e.openFiles.Delete(key)
			_ = f.Close()
			return nil, fmt.Errorf("sync journal dir: %w", err)
		}
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

// frameVisit is one intact frame seen by scanJournal: its 1-based index
// across the whole journal, the byte offset immediately after it, and the
// decoded entry.
type frameVisit struct {
	index      int
	byteOffset int64
	entry      *durablepb.JournalEntry
}

// scanJournal opens the journal (a no-op if it does not exist yet) and
// calls visit for every intact frame in append order. A torn or
// CRC-mismatched tail stops the scan without error so a crash mid-append
// cannot poison replay or resume.
func scanJournal(dataDir, taskID, runID string, macKey []byte, visit func(frameVisit)) error {
	path := journalPath(dataDir, taskID, runID)
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("durable: open journal %s/%s: %w", taskID, runID, err)
	}
	defer func() { _ = f.Close() }()

	var idx int
	var offset int64
	for {
		entry, frameLen, err := readFrame(f, macKey)
		if err != nil {
			// io.EOF is a clean end. Corrupt/truncated tail is also treated
			// as end-of-valid-data so a kill -9 mid-write cannot fail resume.
			// HMAC mismatch is fail-closed (tamper or wrong key).
			if errors.Is(err, io.EOF) || errors.Is(err, errCorruptFrame) {
				break
			}
			return fmt.Errorf("durable: read journal %s/%s: %w", taskID, runID, err)
		}
		idx++
		offset += frameLen
		visit(frameVisit{index: idx, byteOffset: offset, entry: entry})
	}
	return nil
}

// journalTail scans the whole journal purely to find the current frame
// count and byte size, used to seed a freshly opened journalFile handle so
// appendFrame can report correct Offset/ByteOffset without rescanning on
// every write.
func journalTail(dataDir, taskID, runID string, macKey []byte) (int, int64, error) {
	var count int
	var size int64
	err := scanJournal(dataDir, taskID, runID, macKey, func(v frameVisit) {
		count = v.index
		size = v.byteOffset
	})
	return count, size, err
}

// loadJournal replays journal.log and returns the latest StepRecord per stepID
// plus the latest SignalEntry payload per stepID. StepStatusRunning (STARTED)
// entries are watch-only and never enter this map: an unfinished STARTED
// step with no later WAITING/COMPLETED/FAILED record is treated as missing
// on replay, so RunStep re-runs fn rather than getting stuck. A torn or
// CRC-mismatched tail is discarded so a crash mid-append cannot poison replay.
func loadJournal(dataDir, taskID, runID string, c PayloadCodec, macKey []byte) (map[string]StepRecord, map[string][]byte, error) {
	steps, sigs, err := readJournalFrames(dataDir, taskID, runID, macKey)
	if err != nil {
		return nil, nil, err
	}
	for id, rec := range steps {
		decoded, err := decodeStepRecord(c, taskID, runID, rec)
		if err != nil {
			return nil, nil, fmt.Errorf("durable: decode step %s/%s/%s: %w", taskID, runID, id, err)
		}
		steps[id] = decoded
	}
	payloads := make(map[string][]byte, len(sigs))
	for _, s := range sigs {
		id := s.GetSignalId()
		decoded, err := decodePayload(c, s.GetPayload(), payloadAAD(aadKindSignal, taskID, runID, id))
		if err != nil {
			return nil, nil, fmt.Errorf("durable: decode signal %s/%s/%s: %w", taskID, runID, id, err)
		}
		payloads[id] = decoded
	}
	return steps, payloads, nil
}

func readJournalFrames(dataDir, taskID, runID string, macKey []byte) (map[string]StepRecord, []*durablepb.SignalEntry, error) {
	steps := make(map[string]StepRecord)
	var signals []*durablepb.SignalEntry
	err := scanJournal(dataDir, taskID, runID, macKey, func(v frameVisit) {
		switch e := v.entry.Entry.(type) {
		case *durablepb.JournalEntry_Step:
			if e.Step == nil {
				return
			}
			rec := protoToStep(e.Step)
			if rec.Status == StepStatusRunning {
				return
			}
			steps[rec.StepID] = rec
		case *durablepb.JournalEntry_Signal:
			if e.Signal != nil {
				signals = append(signals, e.Signal)
			}
		}
	})
	if err != nil {
		return nil, nil, err
	}
	return steps, signals, nil
}

// loadStepEvents replays every StepEntry frame in append order for
// WatchSteps — STARTED, WAITING, COMPLETED, FAILED as separate events,
// never last-write-wins. Offset and ByteOffset match what appendFrame
// reported when each frame was written (SignalEntry frames advance the
// shared counters but are not emitted here).
func loadStepEvents(dataDir, taskID, runID string, c PayloadCodec, macKey []byte) ([]StepEvent, error) {
	var events []StepEvent
	err := scanJournal(dataDir, taskID, runID, macKey, func(v frameVisit) {
		step, ok := v.entry.Entry.(*durablepb.JournalEntry_Step)
		if !ok || step.Step == nil {
			return
		}
		events = append(events, StepEvent{
			StepRecord: protoToStep(step.Step),
			Offset:     v.index,
			ByteOffset: v.byteOffset,
		})
	})
	if err != nil {
		return nil, err
	}
	for i, ev := range events {
		decoded, err := decodeStepRecord(c, taskID, runID, ev.StepRecord)
		if err != nil {
			return nil, fmt.Errorf("durable: decode step event %s/%s/%s: %w", taskID, runID, ev.StepID, err)
		}
		events[i].StepRecord = decoded
	}
	return events, nil
}

// compactJournal rewrites journal.log after a run reaches a terminal state so
// the file does not grow without bound across retries. Latest StepEntry per
// stepID is kept (last-write-wins already applied by replay; STARTED entries
// are dropped since they never entered the map) plus every SignalEntry —
// signals are an audit of external completions and must not be collapsed
// away. Order is not preserved or sorted; only the map's current state matters.
func (e *Engine) compactJournal(taskID, runID string) error {
	steps, signals, err := readJournalFrames(e.dataDir, taskID, runID, e.journalMACKey())
	if err != nil {
		return fmt.Errorf("durable: compact read %s/%s: %w", taskID, runID, err)
	}

	var buf []byte
	for _, rec := range steps {
		entry := &durablepb.JournalEntry{
			Entry: &durablepb.JournalEntry_Step{Step: stepToProto(rec)},
		}
		payload, err := proto.Marshal(entry)
		if err != nil {
			return fmt.Errorf("durable: compact marshal step %s/%s: %w", taskID, runID, err)
		}
		buf = append(buf, makeFrame(payload, e.journalMACKey())...)
	}
	for _, sig := range signals {
		entry := &durablepb.JournalEntry{
			Entry: &durablepb.JournalEntry_Signal{Signal: sig},
		}
		payload, err := proto.Marshal(entry)
		if err != nil {
			return fmt.Errorf("durable: compact marshal signal %s/%s: %w", taskID, runID, err)
		}
		buf = append(buf, makeFrame(payload, e.journalMACKey())...)
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
	case StepStatusRunning:
		return durablepb.StepStatus_STEP_STATUS_RUNNING
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
	case durablepb.StepStatus_STEP_STATUS_RUNNING:
		return StepStatusRunning
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
		Status:     stepStatusToProto(r.Status),
		Result:     r.Result,
		Error:      r.Error,
		PanicTrace: r.PanicTrace,
		Version:    r.Version,
		Input:      r.Input,
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
		Status:     protoStatusToStep(e.GetStatus()),
		Result:     e.GetResult(),
		Error:      e.GetError(),
		PanicTrace: e.GetPanicTrace(),
		Version:    e.GetVersion(),
		Input:      e.GetInput(),
	}
	if e.GetStartedAtNs() != 0 {
		r.StartedAt = time.Unix(0, e.GetStartedAtNs()).UTC()
	}
	if e.GetCompletedAtNs() != 0 {
		r.CompletedAt = time.Unix(0, e.GetCompletedAtNs()).UTC()
	}
	return r
}

func frameTrailerSize(macKey []byte) int {
	if len(macKey) == 0 {
		return frameCRCSize
	}
	return frameHMACSize
}

func journalFrameMAC(macKey, lenPrefix, payload []byte) []byte {
	mac := hmac.New(sha256.New, macKey)
	_, _ = mac.Write([]byte(journalMACDomain))
	_, _ = mac.Write(lenPrefix)
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}

func sidecarMAC(macKey []byte, domain, taskID, runID string, body []byte) []byte {
	mac := hmac.New(sha256.New, macKey)
	_, _ = mac.Write([]byte(domain))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(taskID))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(runID))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write(body)
	return mac.Sum(nil)
}

func wrapSidecarMAC(macKey []byte, domain, taskID, runID string, body []byte) []byte {
	if len(macKey) == 0 {
		return body
	}
	mac := sidecarMAC(macKey, domain, taskID, runID, body)
	out := make([]byte, len(body)+len(mac))
	copy(out, body)
	copy(out[len(body):], mac)
	return out
}

func unwrapSidecarMAC(macKey []byte, domain, taskID, runID string, raw []byte) ([]byte, error) {
	if len(macKey) == 0 {
		return raw, nil
	}
	if len(raw) < sha256.Size {
		return nil, errFileMAC
	}
	body := raw[:len(raw)-sha256.Size]
	if !hmac.Equal(raw[len(raw)-sha256.Size:], sidecarMAC(macKey, domain, taskID, runID, body)) {
		return nil, errFileMAC
	}
	return body, nil
}

func makeFrame(payload, macKey []byte) []byte {
	trail := frameTrailerSize(macKey)
	frame := make([]byte, frameLenSize+len(payload)+trail)
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	copy(frame[4:], payload)
	if len(macKey) == 0 {
		checksum := crc32.ChecksumIEEE(payload)
		binary.BigEndian.PutUint32(frame[4+len(payload):], checksum)
		return frame
	}
	copy(frame[4+len(payload):], journalFrameMAC(macKey, frame[0:4], payload))
	return frame
}

// readFrame reads one framed JournalEntry and returns it along with the
// total on-disk size of the frame. It returns io.EOF at a clean end of
// file, errCorruptFrame for a torn write or CRC mismatch, and errJournalMAC
// when WithJournalMACKey is set and the HMAC does not match.
func readFrame(r io.Reader, macKey []byte) (*durablepb.JournalEntry, int64, error) {
	var lenBuf [frameLenSize]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, 0, io.EOF
		}
		return nil, 0, err
	}
	payloadLen := binary.BigEndian.Uint32(lenBuf[:])
	if payloadLen > maxFramePayload {
		return nil, 0, errCorruptFrame
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, 0, errCorruptFrame
	}

	trail := frameTrailerSize(macKey)
	trailer := make([]byte, trail)
	if _, err := io.ReadFull(r, trailer); err != nil {
		return nil, 0, errCorruptFrame
	}
	if len(macKey) == 0 {
		storedCRC := binary.BigEndian.Uint32(trailer)
		if storedCRC != crc32.ChecksumIEEE(payload) {
			return nil, 0, errCorruptFrame
		}
	} else if !hmac.Equal(trailer, journalFrameMAC(macKey, lenBuf[:], payload)) {
		return nil, 0, errJournalMAC
	}

	var entry durablepb.JournalEntry
	if err := proto.Unmarshal(payload, &entry); err != nil {
		return nil, 0, errCorruptFrame
	}
	total := int64(frameLenSize+trail) + int64(payloadLen)
	return &entry, total, nil
}
