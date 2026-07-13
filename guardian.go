/*
Real-time Online/Offline Charging System (OCS) for Telecom & ISP environments
Copyright (C) ITsysCOM GmbH

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>
*/

package guardian

import (
	"fmt"
	"sync"
	"time"

	"github.com/cgrates/birpc/context"
	"github.com/google/uuid"
)

// Option configures a GuardianLocker.
type Option func(*GuardianLocker)

// logger defines the logging interface required.
type logger interface {
	Alert(string) error
	Close() error
	Crit(string) error
	Debug(string) error
	Emerg(string) error
	Err(string) error
	Info(string) error
	Notice(string) error
	Warning(string) error
}

// GuardianLocker is an optimized locking system that manages locks by string keys.
type GuardianLocker struct {
	lkMux   sync.Mutex // protects the locks
	locks   map[string]*itemLock
	refsMux sync.RWMutex       // protects the map
	refs    map[string]*refObj // used in case of remote locks
	logger  logger
}

// itemLock holds the channel used for locking and a counter for tracking lock references.
type itemLock struct {
	lk  chan struct{}
	cnt int64
}

// refObj tracks a group of locks with an optional timer for auto-unlocking.
type refObj struct {
	refs []string
	tm   *time.Timer
}

// Guardian is the global package variable.
var Guardian = New()

// New creates a GuardianLocker with the provided options.
func New(opts ...Option) *GuardianLocker {
	gl := &GuardianLocker{
		locks:  make(map[string]*itemLock),
		refs:   make(map[string]*refObj),
		logger: nopLogger{},
	}

	for _, opt := range opts {
		opt(gl)
	}

	return gl
}

// WithLogger sets a custom logger for the GuardianLocker.
func WithLogger(l logger) Option {
	return func(gl *GuardianLocker) {
		if l != nil {
			gl.logger = l
		}
	}
}

// Guard locks the specified IDs, executes the handler, and then unlocks the IDs.
// Returns the error from handler or nil if it times out/gets cancelled.
func (gl *GuardianLocker) Guard(ctx *context.Context, handler func(*context.Context) error,
	timeout time.Duration, lockIDs ...string) (err error) {
	for _, lockID := range lockIDs {
		gl.lockItem(lockID)
	}
	defer func() {
		for _, lockID := range lockIDs {
			gl.unlockItem(lockID)
		}
	}()
	if timeout <= 0 && ctx.Done() == nil {
		return handler(ctx)
	}
	errChan := make(chan error, 1)

	// Apply timeout if specified.
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	go func() {
		errChan <- handler(ctx)
	}()

	select {
	case err = <-errChan:
		close(errChan)
	case <-ctx.Done(): // ignore context error but log it
		gl.logger.Warning(fmt.Sprintf(
			"<Guardian> force timing-out locks: <%+v> because: <%s> ", lockIDs, ctx.Err()))
	}
	return
}

// GuardIDs acquires a lock for the specified duration and returns the
// reference ID for the lock group acquired.
func (gl *GuardianLocker) GuardIDs(refID string, timeout time.Duration, lkIDs ...string) string {
	return gl.lockWithReference(refID, timeout, lkIDs...)
}

// UnguardIDs unlocks all locks associated with the given reference ID.
// Returns the list of unlocked item IDs or nil if refID is empty.
func (gl *GuardianLocker) UnguardIDs(refID string) []string {
	if refID != "" {
		return gl.unlockWithReference(refID)
	}
	return nil
}

// lockItem acquires a lock for the given item ID.
func (gl *GuardianLocker) lockItem(itmID string) {
	if itmID == "" {
		return
	}
	gl.lkMux.Lock()
	itmLock, exists := gl.locks[itmID]
	if !exists {
		gl.locks[itmID] = &itemLock{lk: make(chan struct{}, 1), cnt: 1}
		gl.lkMux.Unlock()
		return
	}
	itmLock.cnt++
	gl.lkMux.Unlock()
	<-itmLock.lk
}

// unlockItem releases a lock for the given item ID.
func (gl *GuardianLocker) unlockItem(itmID string) {
	gl.lkMux.Lock()
	itmLock, exists := gl.locks[itmID]
	if !exists {
		gl.lkMux.Unlock()
		return
	}
	itmLock.cnt--
	if itmLock.cnt == 0 {
		delete(gl.locks, itmID)
	}
	gl.lkMux.Unlock()
	itmLock.lk <- struct{}{}
}

// lockWithReference acquires locks for the given IDs and associates them with
// a reference ID.
// If refID is empty, it generates a new UUID. If timeout is positive, it
// automatically unlocks after the timeout duration.
// Returns the reference ID on success or empty string if the reference ID is
// already in use.
func (gl *GuardianLocker) lockWithReference(refID string, timeout time.Duration, lkIDs ...string) string {
	var refEmpty bool
	if refID == "" {
		refEmpty = true
		refID = uuid.NewString()
	}

	// Lock the refID first to ensure only one process can check or use it at a time.
	gl.lockItem(refID)

	gl.refsMux.Lock()
	if !refEmpty {
		if _, has := gl.refs[refID]; has {
			gl.refsMux.Unlock()
			gl.unlockItem(refID)
			return "" // refID already in use, abort without locking
		}
	}
	var tm *time.Timer
	if timeout != 0 {
		// Set up auto-unlock after timeout period.
		tm = time.AfterFunc(timeout, func() {
			if lkIDs := gl.unlockWithReference(refID); len(lkIDs) != 0 {
				gl.logger.Warning(fmt.Sprintf("<Guardian> force timing-out locks: %+v", lkIDs))
			}
		})
	}
	gl.refs[refID] = &refObj{
		refs: lkIDs,
		tm:   tm,
	}
	gl.refsMux.Unlock()
	// execute the real locks
	for _, lk := range lkIDs {
		gl.lockItem(lk)
	}
	gl.unlockItem(refID)
	return refID
}

// unlockWithReference releases all locks associated with the given reference
// ID and returns the unlocked item IDs.
func (gl *GuardianLocker) unlockWithReference(refID string) (lkIDs []string) {
	gl.lockItem(refID)
	gl.refsMux.Lock()
	ref, has := gl.refs[refID]
	if !has {
		gl.refsMux.Unlock()
		gl.unlockItem(refID)
		return
	}
	if ref.tm != nil {
		ref.tm.Stop()
	}
	delete(gl.refs, refID)
	gl.refsMux.Unlock()
	lkIDs = ref.refs
	for _, lk := range lkIDs {
		gl.unlockItem(lk)
	}
	gl.unlockItem(refID)
	return
}

type nopLogger struct{}

func (nopLogger) Alert(string) error   { return nil }
func (nopLogger) Close() error         { return nil }
func (nopLogger) Crit(string) error    { return nil }
func (nopLogger) Debug(string) error   { return nil }
func (nopLogger) Emerg(string) error   { return nil }
func (nopLogger) Err(string) error     { return nil }
func (nopLogger) Info(string) error    { return nil }
func (nopLogger) Notice(string) error  { return nil }
func (nopLogger) Warning(string) error { return nil }
