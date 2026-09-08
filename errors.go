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

	// ErrStepPending is returned from a step function to suspend the run
	// until CompleteStep delivers a result for that step. The engine writes
	// StepStatusWaiting and blocks the run goroutine.
	ErrStepPending = errors.New("durable: step pending external completion")

	// ErrInvalidToken is returned by CompleteStep when the token cannot be
	// decoded into taskID, runID, and stepID.
	ErrInvalidToken = errors.New("durable: invalid step token")
)
