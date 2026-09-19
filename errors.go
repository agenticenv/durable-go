package durable

import "errors"

var (
	// ErrTaskNotRegistered is returned by RunTask.Get when the taskID has not
	// been registered on this Engine. The registry is in-memory only and must
	// be rebuilt after every NewEngine call.
	ErrTaskNotRegistered = errors.New("durable: task not registered")

	// ErrTaskAlreadyRegistered is returned by RegisterTask when the same
	// taskID is registered twice on one Engine.
	ErrTaskAlreadyRegistered = errors.New("durable: task already registered")

	// ErrRunAlreadyFinished is returned by CompleteStep when the target step
	// is already completed or the run is in a terminal state (completed or failed).
	ErrRunAlreadyFinished = errors.New("durable: run already completed or failed")

	// ErrRunActive is returned by DeleteTaskRun and DeleteTask when the target run
	// (or any run under the task) is currently executing, including while
	// blocked in StatusWaiting.
	ErrRunActive = errors.New("durable: run is currently executing")

	// ErrInvalidRunID is returned when a runID contains path separators or
	// parent-directory references that would escape the data directory.
	ErrInvalidRunID = errors.New("durable: invalid run ID")

	// ErrEngineLocked is returned by NewEngine and NewReadOnlyEngine when
	// dataDir is already held by another engine instance (same process or
	// another OS process) and the lock cannot be acquired before the timeout.
	ErrEngineLocked = errors.New("durable: dataDir locked by another engine")

	// ErrStepPending is returned from a step function to suspend that step
	// until CompleteStep delivers a result for it. The engine writes
	// StepStatusWaiting and blocks that step's Get — not the whole task.
	// Sibling steps started before this one keep running; call Get on them
	// independently or select on their Done channels.
	ErrStepPending = errors.New("durable: step pending external completion")

	// ErrInvalidToken is returned by CompleteStep when the token cannot be
	// decoded into taskID, runID, and stepID, or when an HMAC token has a
	// missing or wrong MAC (including unsigned tokens while a secret is set).
	ErrInvalidToken = errors.New("durable: invalid step token")

	// ErrTokenExpired is returned by CompleteStep when an HMAC step token is
	// past its TTL.
	ErrTokenExpired = errors.New("durable: step token expired")

	// ErrPayloadTooLarge is returned when a step's persisted record does not
	// fit in one journal frame (32 MiB of encoded input plus result). The step
	// fails rather than writing a frame that replay and compaction could not
	// read back. Keep large blobs out of step results — store a handle and
	// fetch the payload inside fn.
	ErrPayloadTooLarge = errors.New("durable: payload too large for one journal frame")

	// ErrEngineClosed is returned by RunTask when the Engine is already
	// closing or closed, so no new run is started after the exclusive flock
	// on dataDir has been released.
	ErrEngineClosed = errors.New("durable: engine is closed")

	// ErrRunCancelled is the error stored on a run (TaskInfo.Error, as its
	// string form) and returned by RunStep/Get after CancelRun. It marks
	// the run as StatusFailed for the same reason engine Close and task/run
	// timeouts already do — but with a distinct message so callers can
	// tell a deliberate CancelRun apart from a generic ctx cancellation or
	// deadline. Because TaskInfo.Error is a plain string, matching after a
	// resume requires comparing against ErrRunCancelled.Error(), not
	// errors.Is.
	ErrRunCancelled = errors.New("durable: run was cancelled")
)
