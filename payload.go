package durable

import "fmt"

// PayloadCodec transforms task and step payload bytes at the persistence
// boundary. Encode runs after JSON marshal and before the bytes are written;
// Decode runs after a read and before JSON unmarshal. Implementations must be
// reversible — hashing or redaction here would break resume.
//
// aad binds the blob to its location (kind, taskID, runID, stepID) so a
// ciphertext cannot be copied between fields or steps. Omit WithPayloadCodec
// (or pass nil) to persist JSON plaintext, the default.
type PayloadCodec interface {
	Encode(plaintext, aad []byte) ([]byte, error)
	Decode(ciphertext, aad []byte) ([]byte, error)
}

const (
	aadKindTaskInput  = "task-input"
	aadKindTaskOutput = "task-output"
	aadKindStepInput  = "step-input"
	aadKindStepResult = "step-result"
	aadKindSignal     = "signal"
)

// WithPayloadCodec sets the codec for task input/output, step input/result,
// and CompleteStep signal payloads on NewEngine and NewReadOnlyEngine.
// nil is identity (plaintext).
func WithPayloadCodec(c PayloadCodec) Option {
	return func(e *engineConfig, r *readOnlyConfig) {
		if e != nil {
			e.codec = c
		}
		if r != nil {
			r.codec = c
		}
	}
}

func (e *Engine) codec() PayloadCodec { return e.cfg.codec }

func (r *ReadOnlyEngine) codec() PayloadCodec { return r.cfg.codec }

func (e *Engine) journalMACKey() []byte { return e.cfg.journalMACKey }

func (r *ReadOnlyEngine) journalMACKey() []byte { return r.cfg.journalMACKey }

func payloadAAD(kind, taskID, runID, stepID string) []byte {
	n := len(kind) + 1 + len(taskID) + 1 + len(runID) + 1 + len(stepID)
	buf := make([]byte, 0, n)
	buf = append(buf, kind...)
	buf = append(buf, 0)
	buf = append(buf, taskID...)
	buf = append(buf, 0)
	buf = append(buf, runID...)
	buf = append(buf, 0)
	buf = append(buf, stepID...)
	return buf
}

func encodePayload(c PayloadCodec, plaintext, aad []byte) ([]byte, error) {
	if len(plaintext) == 0 || c == nil {
		return plaintext, nil
	}
	out, err := c.Encode(plaintext, aad)
	if err != nil {
		return nil, fmt.Errorf("durable: encode payload: %w", err)
	}
	return out, nil
}

func decodePayload(c PayloadCodec, ciphertext, aad []byte) ([]byte, error) {
	if len(ciphertext) == 0 || c == nil {
		return ciphertext, nil
	}
	out, err := c.Decode(ciphertext, aad)
	if err != nil {
		return nil, fmt.Errorf("durable: decode payload: %w", err)
	}
	return out, nil
}

func encodeStepRecord(c PayloadCodec, taskID, runID string, rec StepRecord) (StepRecord, error) {
	var err error
	rec.Input, err = encodePayload(c, rec.Input, payloadAAD(aadKindStepInput, taskID, runID, rec.StepID))
	if err != nil {
		return rec, err
	}
	rec.Result, err = encodePayload(c, rec.Result, payloadAAD(aadKindStepResult, taskID, runID, rec.StepID))
	return rec, err
}

func decodeStepRecord(c PayloadCodec, taskID, runID string, rec StepRecord) (StepRecord, error) {
	var err error
	rec.Input, err = decodePayload(c, rec.Input, payloadAAD(aadKindStepInput, taskID, runID, rec.StepID))
	if err != nil {
		return rec, err
	}
	rec.Result, err = decodePayload(c, rec.Result, payloadAAD(aadKindStepResult, taskID, runID, rec.StepID))
	return rec, err
}
