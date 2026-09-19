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
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	frameLenSize    = 4
	frameCRCSize    = 4
	frameHMACSize   = sha256.Size
	frameOverhead   = frameLenSize + frameCRCSize // default (CRC) overhead
	maxFramePayload = 32 << 20                    // 32 MiB — reject corrupt lengths that would OOM
	// v2 binds each frame MAC to taskID, runID, and the 1-based frame
	// index so a journal.log cannot be copied between runs or have its
	// frames reordered/duplicated. v1 (domain + length + payload only)
	// is not read back — enabling a journal MAC is not a migrate.
	journalMACDomain    = "durable.journal.v2"
	fileMACDomainMeta   = "durable.meta.v1"
	fileMACDomainInput  = "durable.input.v1"
	fileMACDomainOutput = "durable.output.v1"
)

var (
	errCorruptFrame = errors.New("durable: corrupt or truncated journal frame")
	// errTruncatedFrame is the subset of errCorruptFrame where the file ended
	// part-way through a frame. That can only happen at the tail, so it is
	// always recoverable — unlike a frame that decodes badly while intact
	// frames follow it.
	errTruncatedFrame = fmt.Errorf("%w (file ended mid-frame)", errCorruptFrame)
	errJournalMAC     = errors.New("durable: journal MAC mismatch")
	errFileMAC        = errors.New("durable: file MAC mismatch")
	// errCorruptInterior is returned when an undecodable frame is followed by
	// more bytes. Treating that as end-of-journal would silently discard every
	// step after it, so replay fails closed instead.
	errCorruptInterior = errors.New("durable: corrupt journal frame with data after it")
)

// orDiscard makes a logger optional at every call site. Internal tests build
// a bare &Engine{dataDir: ...} with no logger configured.
func orDiscard(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.New(slog.DiscardHandler)
	}
	return l
}

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
	return loadJournal(e.dataDir, taskID, runID, e.codec(), e.journalMACKey(), e.log())
}

func (e *Engine) loadStepEvents(taskID, runID string) ([]StepEvent, error) {
	return loadStepEvents(e.dataDir, taskID, runID, e.codec(), e.journalMACKey(), e.log())
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
	// Refuse a payload readFrame would later reject. Writing one succeeds at
	// the OS level but makes the frame — and everything appended after it —
	// unreadable on replay and in compaction, so the step must fail here
	// instead.
	if err := checkFramePayload(payload); err != nil {
		return 0, 0, err
	}
	h, err := e.getOrOpenJournal(taskID, runID)
	if err != nil {
		return 0, 0, err
	}
	key := taskID + "/" + runID
	h.mu.Lock()
	defer h.mu.Unlock()
	frame := makeFrame(payload, e.journalMACKey(), taskID, runID, h.count+1)
	if _, err := h.f.Write(frame); err != nil {
		e.discardTornFrame(h, taskID, runID, key)
		return 0, 0, fmt.Errorf("write frame: %w", err)
	}
	if err := h.f.Sync(); err != nil {
		e.discardTornFrame(h, taskID, runID, key)
		return 0, 0, fmt.Errorf("sync journal: %w", err)
	}
	h.count++
	h.size += int64(len(frame))
	return h.count, h.size, nil
}

// checkFramePayload rejects a payload that would not survive a round trip:
// readFrame refuses anything over maxFramePayload, and the 4-byte length
// prefix cannot describe more than a uint32 in the first place.
func checkFramePayload(payload []byte) error {
	if len(payload) > maxFramePayload {
		return fmt.Errorf("%w: journal frame payload is %d bytes, limit is %d",
			ErrPayloadTooLarge, len(payload), maxFramePayload)
	}
	return nil
}

// discardTornFrame rolls the journal back to the last fully written frame
// after a failed append, then drops the cached handle. Leaving the partial
// bytes in place would strand every later append behind a frame that replay
// cannot read past — the appends would keep succeeding while resume silently
// lost all of them. Caller holds h.mu.
func (e *Engine) discardTornFrame(h *journalFile, taskID, runID, key string) {
	if err := h.f.Truncate(h.size); err != nil {
		orDiscard(e.log()).Error("journal rollback failed after a torn append",
			"task_id", taskID, "run_id", runID, "last_good_offset", h.size, "error", err)
	} else {
		_ = h.f.Sync()
	}
	_ = h.f.Close()
	if _, loaded := e.openFiles.LoadAndDelete(key); loaded {
		e.openCount.Add(-1)
	}
}

func (e *Engine) getOrOpenJournal(taskID, runID string) (*journalFile, error) {
	key := taskID + "/" + runID
	if v, ok := e.openFiles.Load(key); ok {
		return v.(*journalFile), nil
	}
	if err := mkdirAllSecure(e.runDir(taskID, runID)); err != nil {
		return nil, fmt.Errorf("create run dir: %w", err)
	}
	count, size, err := journalTail(e.dataDir, taskID, runID, e.journalMACKey(), e.log())
	if err != nil {
		return nil, fmt.Errorf("scan journal tail: %w", err)
	}
	path := e.journalPath(taskID, runID)
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !created {
		return nil, fmt.Errorf("stat journal: %w", statErr)
	}
	// Drop anything past the last intact frame before reopening for append.
	// journalTail stops at a torn tail, so without this the next O_APPEND
	// write would land after bytes replay can never read past: every later
	// frame would be persisted and then silently lost on resume, and h.size
	// would drift from the real file size so WatchSteps byte offsets stop
	// matching the file.
	if !created {
		if err := truncateJournalTo(path, size, taskID, runID, e.log()); err != nil {
			return nil, err
		}
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
	e.openCount.Add(1)
	e.evictOpenJournals(key)
	if created {
		if err := syncDirFn(filepath.Dir(path)); err != nil {
			e.openFiles.Delete(key)
			e.openCount.Add(-1)
			_ = f.Close()
			return nil, fmt.Errorf("sync journal dir: %w", err)
		}
	}
	return h, nil
}

// truncateJournalTo drops everything past keep bytes and fsyncs, so the next
// O_APPEND write lands exactly where replay stops reading. A no-op when the
// file is already that size, which is the common case.
func truncateJournalTo(path string, keep int64, taskID, runID string, logger *slog.Logger) error {
	st, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("durable: stat journal %s/%s: %w", taskID, runID, err)
	}
	if st.Size() <= keep {
		return nil
	}
	f, err := os.OpenFile(path, os.O_WRONLY, filePerm)
	if err != nil {
		return fmt.Errorf("durable: open journal %s/%s to truncate: %w", taskID, runID, err)
	}
	err = f.Truncate(keep)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("durable: truncate journal %s/%s: %w", taskID, runID, err)
	}
	orDiscard(logger).Warn("journal rolled back to last intact frame",
		"task_id", taskID, "run_id", runID,
		"kept_bytes", keep, "discarded_bytes", st.Size()-keep)
	return nil
}

func (e *Engine) closeJournal(taskID, runID string) {
	key := taskID + "/" + runID
	if v, ok := e.openFiles.LoadAndDelete(key); ok {
		h := v.(*journalFile)
		h.mu.Lock()
		_ = h.f.Close()
		h.mu.Unlock()
		e.openCount.Add(-1)
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
	e.openCount.Store(0)
}

func (e *Engine) evictOpenJournals(keep string) {
	if e.openCount.Load() <= maxOpenJournals {
		return
	}
	e.openFiles.Range(func(key, _ any) bool {
		if e.openCount.Load() <= maxOpenJournals {
			return false
		}
		k, ok := key.(string)
		if !ok || k == keep {
			return true
		}
		taskID, runID, ok := strings.Cut(k, "/")
		if !ok {
			return true
		}
		e.closeJournal(taskID, runID)
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

// journalRecovery reports what a scan discarded at the tail of the journal.
// Zero means the file ended cleanly on a frame boundary.
type journalRecovery struct {
	frames int
	bytes  int64
}

// scanJournal opens the journal (a no-op if it does not exist yet) and
// calls visit for every intact frame in append order.
//
// A torn tail stops the scan without error so a crash mid-append cannot
// poison replay or resume, and what was dropped is logged. That leniency is
// deliberately limited to the tail: a frame that fails to decode while
// intact bytes still follow it is real damage, not an interrupted write, and
// silently stopping there would discard every step after it. That case
// returns errCorruptInterior instead. An HMAC mismatch always fails closed
// (tamper or wrong key), wherever it occurs.
func scanJournal(dataDir, taskID, runID string, macKey []byte, logger *slog.Logger, visit func(frameVisit)) (journalRecovery, error) {
	path := journalPath(dataDir, taskID, runID)
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return journalRecovery{}, nil
	}
	if err != nil {
		return journalRecovery{}, fmt.Errorf("durable: open journal %s/%s: %w", taskID, runID, err)
	}
	defer func() { _ = f.Close() }()

	st, err := f.Stat()
	if err != nil {
		return journalRecovery{}, fmt.Errorf("durable: stat journal %s/%s: %w", taskID, runID, err)
	}
	size := st.Size()

	var idx int
	var offset int64
	for {
		entry, frameLen, err := readFrame(f, macKey, taskID, runID, idx+1)
		if err == nil {
			idx++
			offset += frameLen
			visit(frameVisit{index: idx, byteOffset: offset, entry: entry})
			continue
		}
		if errors.Is(err, io.EOF) {
			return journalRecovery{}, nil
		}
		if !errors.Is(err, errCorruptFrame) {
			return journalRecovery{}, fmt.Errorf("durable: read journal %s/%s: %w", taskID, runID, err)
		}
		// Recoverable only when nothing intact can follow: either the file
		// ended mid-frame, or the frame's own claimed length runs to or past
		// EOF. Anything else has readable bytes after it.
		if !errors.Is(err, errTruncatedFrame) && offset+frameLen < size {
			return journalRecovery{}, fmt.Errorf("durable: read journal %s/%s at byte %d: %w",
				taskID, runID, offset, errCorruptInterior)
		}
		rec := journalRecovery{frames: 1, bytes: size - offset}
		orDiscard(logger).Warn("journal tail discarded: torn final frame",
			"task_id", taskID, "run_id", runID,
			"last_good_offset", offset, "discarded_bytes", rec.bytes)
		return rec, nil
	}
}

// journalTail scans the whole journal purely to find the current frame
// count and byte size, used to seed a freshly opened journalFile handle so
// appendFrame can report correct Offset/ByteOffset without rescanning on
// every write. size is the offset of the last intact frame's end, which is
// also where a torn tail must be truncated back to.
func journalTail(dataDir, taskID, runID string, macKey []byte, logger *slog.Logger) (int, int64, error) {
	var count int
	var size int64
	_, err := scanJournal(dataDir, taskID, runID, macKey, logger, func(v frameVisit) {
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
func loadJournal(dataDir, taskID, runID string, c PayloadCodec, macKey []byte, logger *slog.Logger) (map[string]StepRecord, map[string][]byte, error) {
	steps, sigs, err := readJournalFrames(dataDir, taskID, runID, macKey, logger)
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

func readJournalFrames(dataDir, taskID, runID string, macKey []byte, logger *slog.Logger) (map[string]StepRecord, []*durablepb.SignalEntry, error) {
	steps := make(map[string]StepRecord)
	var signals []*durablepb.SignalEntry
	_, err := scanJournal(dataDir, taskID, runID, macKey, logger, func(v frameVisit) {
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
func loadStepEvents(dataDir, taskID, runID string, c PayloadCodec, macKey []byte, logger *slog.Logger) ([]StepEvent, error) {
	var events []StepEvent
	_, err := scanJournal(dataDir, taskID, runID, macKey, logger, func(v frameVisit) {
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
	info, _, metaErr := loadMeta(dataDir, taskID, runID, macKey)
	if metaErr != nil {
		return nil, metaErr
	}
	for i, ev := range events {
		decoded, err := decodeStepRecord(c, taskID, runID, ev.StepRecord)
		if err != nil {
			return nil, fmt.Errorf("durable: decode step event %s/%s/%s: %w", taskID, runID, ev.StepID, err)
		}
		events[i].StepRecord = decoded
		events[i].Generation = info.JournalGeneration
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
	steps, signals, err := readJournalFrames(e.dataDir, taskID, runID, e.journalMACKey(), e.log())
	if err != nil {
		return fmt.Errorf("durable: compact read %s/%s: %w", taskID, runID, err)
	}

	var buf []byte
	var idx int
	for _, rec := range steps {
		entry := &durablepb.JournalEntry{
			Entry: &durablepb.JournalEntry_Step{Step: stepToProto(rec)},
		}
		payload, err := proto.Marshal(entry)
		if err != nil {
			return fmt.Errorf("durable: compact marshal step %s/%s: %w", taskID, runID, err)
		}
		if err := checkFramePayload(payload); err != nil {
			return fmt.Errorf("durable: compact step %s/%s/%s: %w", taskID, runID, rec.StepID, err)
		}
		idx++
		buf = append(buf, makeFrame(payload, e.journalMACKey(), taskID, runID, idx)...)
	}
	for _, sig := range signals {
		entry := &durablepb.JournalEntry{
			Entry: &durablepb.JournalEntry_Signal{Signal: sig},
		}
		payload, err := proto.Marshal(entry)
		if err != nil {
			return fmt.Errorf("durable: compact marshal signal %s/%s: %w", taskID, runID, err)
		}
		if err := checkFramePayload(payload); err != nil {
			return fmt.Errorf("durable: compact signal %s/%s/%s: %w", taskID, runID, sig.GetSignalId(), err)
		}
		idx++
		buf = append(buf, makeFrame(payload, e.journalMACKey(), taskID, runID, idx)...)
	}

	e.closeJournal(taskID, runID)
	if err := writeFileAtomic(e.journalPath(taskID, runID), buf); err != nil {
		return fmt.Errorf("durable: compact write %s/%s: %w", taskID, runID, err)
	}
	info, ok, err := e.loadMeta(taskID, runID)
	if err == nil && ok {
		info.JournalGeneration++
		if err := e.saveMeta(taskID, runID, info); err != nil {
			return fmt.Errorf("durable: compact generation %s/%s: %w", taskID, runID, err)
		}
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

func journalFrameMAC(macKey []byte, taskID, runID string, index int, lenPrefix, payload []byte) []byte {
	mac := hmac.New(sha256.New, macKey)
	_, _ = mac.Write([]byte(journalMACDomain))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(taskID))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(runID))
	_, _ = mac.Write([]byte{0})
	var idx [8]byte
	binary.BigEndian.PutUint64(idx[:], uint64(index))
	_, _ = mac.Write(idx[:])
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

// makeFrame builds the on-disk frame. payload must already have passed
// checkFramePayload — otherwise the uint32 length prefix below cannot
// describe it and the frame would be unreadable.
func makeFrame(payload, macKey []byte, taskID, runID string, index int) []byte {
	if len(payload) > maxFramePayload {
		panic(fmt.Sprintf("durable: makeFrame called with %d-byte payload past the %d limit; call checkFramePayload first",
			len(payload), maxFramePayload))
	}
	trail := frameTrailerSize(macKey)
	frame := make([]byte, frameLenSize+len(payload)+trail)
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	copy(frame[4:], payload)
	if len(macKey) == 0 {
		checksum := crc32.ChecksumIEEE(payload)
		binary.BigEndian.PutUint32(frame[4+len(payload):], checksum)
		return frame
	}
	copy(frame[4+len(payload):], journalFrameMAC(macKey, taskID, runID, index, frame[0:4], payload))
	return frame
}

// readFrame reads one framed JournalEntry and returns it along with the
// total on-disk size of the frame. It returns io.EOF at a clean end of
// file, errTruncatedFrame when the file ends mid-frame, errCorruptFrame for
// a CRC mismatch or an undecodable payload, and errJournalMAC when
// WithJournalMACKey is set and the HMAC does not match.
//
// The size is reported even on a corrupt frame — it is the size the frame
// claims, from its own length prefix. scanJournal compares that against the
// remaining file to tell a torn tail from damage in the middle.
func readFrame(r io.Reader, macKey []byte, taskID, runID string, index int) (*durablepb.JournalEntry, int64, error) {
	var lenBuf [frameLenSize]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, 0, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, 0, errTruncatedFrame
		}
		return nil, 0, err
	}
	payloadLen := binary.BigEndian.Uint32(lenBuf[:])
	trail := frameTrailerSize(macKey)
	claimed := int64(frameLenSize+trail) + int64(payloadLen)
	if payloadLen > maxFramePayload {
		// appendFrame rejects oversize payloads, so a prefix this large means
		// the prefix itself is damaged. claimed is still returned: it is
		// normally far past EOF, which marks this as garbage at the tail.
		return nil, claimed, errCorruptFrame
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, claimed, errTruncatedFrame
	}

	trailer := make([]byte, trail)
	if _, err := io.ReadFull(r, trailer); err != nil {
		return nil, claimed, errTruncatedFrame
	}
	if len(macKey) == 0 {
		storedCRC := binary.BigEndian.Uint32(trailer)
		if storedCRC != crc32.ChecksumIEEE(payload) {
			return nil, claimed, errCorruptFrame
		}
	} else if !hmac.Equal(trailer, journalFrameMAC(macKey, taskID, runID, index, lenBuf[:], payload)) {
		return nil, claimed, errJournalMAC
	}

	var entry durablepb.JournalEntry
	if err := proto.Unmarshal(payload, &entry); err != nil {
		return nil, claimed, errCorruptFrame
	}
	return &entry, claimed, nil
}
