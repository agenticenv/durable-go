package durable

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

// dirOccupancy tracks in-process Engine / ReadOnlyEngine opens for a dataDir.
// OS flock is associated with a file description and can succeed twice in the
// same process on some platforms; this map is what makes a second NewEngine
// on the same dataDir fail inside one process. Flock still serialises
// distinct OS processes.
type dirOccupancy struct {
	writers int
	readers int
}

var (
	occupancyMu sync.Mutex
	occupancy   = map[string]*dirOccupancy{}
)

func occupyExclusive(absDir string) error {
	occupancyMu.Lock()
	defer occupancyMu.Unlock()
	o := occupancy[absDir]
	if o == nil {
		occupancy[absDir] = &dirOccupancy{writers: 1}
		return nil
	}
	if o.writers > 0 || o.readers > 0 {
		return ErrEngineLocked
	}
	o.writers = 1
	return nil
}

func occupyShared(absDir string) error {
	occupancyMu.Lock()
	defer occupancyMu.Unlock()
	o := occupancy[absDir]
	if o == nil {
		occupancy[absDir] = &dirOccupancy{readers: 1}
		return nil
	}
	if o.writers > 0 {
		return ErrEngineLocked
	}
	o.readers++
	return nil
}

func releaseOccupancy(absDir string, exclusive bool) {
	occupancyMu.Lock()
	defer occupancyMu.Unlock()
	o := occupancy[absDir]
	if o == nil {
		return
	}
	if exclusive {
		o.writers--
	} else {
		o.readers--
	}
	if o.writers <= 0 && o.readers <= 0 {
		delete(occupancy, absDir)
	}
}

// acquireLock takes an exclusive or shared flock on path, waiting up to
// timeout (or ctx, whichever ends first). Exclusive is used by NewEngine so
// a writer never shares the journal with another writer or a reader.
// Shared is used by NewReadOnlyEngine so multiple CLI readers can coexist.
func acquireLock(ctx context.Context, path string, exclusive bool, timeout time.Duration) (*flock.Flock, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	lockCtx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		lockCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	const retryDelay = 50 * time.Millisecond
	f := flock.New(path)
	var (
		ok  bool
		err error
	)
	if exclusive {
		ok, err = f.TryLockContext(lockCtx, retryDelay)
	} else {
		ok, err = f.TryRLockContext(lockCtx, retryDelay)
	}
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, fmt.Errorf("durable: lock cancelled: %w", ctx.Err())
		}
		return nil, fmt.Errorf("%w: %v", ErrEngineLocked, err)
	}
	if !ok {
		return nil, ErrEngineLocked
	}
	return f, nil
}

func releaseLock(f *flock.Flock) error {
	if f == nil {
		return nil
	}
	return f.Unlock()
}
