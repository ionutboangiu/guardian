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
	"slices"
	"sync"
	"time"

	"github.com/cgrates/birpc/context"
)

// Option configures a GuardianLocker.
type Option func(*GuardianLocker)

type logger interface {
	Warning(string) error
}

// GuardianLocker is an optimized locking system that manages locks by string keys.
type GuardianLocker struct {
	timeout time.Duration
	lkMux   sync.Mutex // protects the locks
	locks   map[string]*itemLock
	logger  logger
}

type itemLock struct {
	mu  sync.Mutex
	cnt int64
}

// Guardian is the global package variable.
var Guardian = New()

// New creates a GuardianLocker with the provided options.
func New(opts ...Option) *GuardianLocker {
	gl := &GuardianLocker{
		locks:  make(map[string]*itemLock),
		logger: nopLogger{},
	}

	for _, opt := range opts {
		opt(gl)
	}

	return gl
}

// WithTimeout sets the timeout for Guard and Lock.
// Non-positive durations disable the timeout.
func WithTimeout(d time.Duration) Option {
	return func(gl *GuardianLocker) {
		gl.timeout = d
	}
}

// WithLogger sets a custom logger for the GuardianLocker.
func WithLogger(l logger) Option {
	return func(gl *GuardianLocker) {
		if l != nil {
			gl.logger = l
		}
	}
}

// Lock locks keys and returns an unlock function.
// The timeout starts once all keys are locked. Calling unlock after timeout is
// safe.
func (gl *GuardianLocker) Lock(keys ...string) func() {
	unlock := gl.lockKeys(keys)
	if gl.timeout <= 0 {
		return unlock
	}
	warningKeys := slices.Clone(keys)
	var once sync.Once
	timer := time.AfterFunc(gl.timeout, func() {
		once.Do(func() {
			unlock()
			if len(warningKeys) != 0 {
				_ = gl.logger.Warning(fmt.Sprintf("<Guardian> force timing-out locks: %+v", warningKeys))
			}
		})
	})
	return func() {
		timer.Stop()
		once.Do(unlock)
	}
}

// Guard runs handler while holding keys.
// On timeout or cancellation, it unlocks and returns nil while handler may
// still run.
func (gl *GuardianLocker) Guard(ctx *context.Context, handler func(*context.Context) error,
	keys ...string) error {
	if len(keys) == 1 {
		key := keys[0]
		gl.lockItem(key)
		defer gl.unlockItem(key)
	} else {
		unlock := gl.lockKeys(keys)
		defer unlock()
	}
	if gl.timeout <= 0 && ctx.Done() == nil {
		return handler(ctx)
	}
	errCh := make(chan error, 1)

	if gl.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, gl.timeout)
		defer cancel()
	}
	go func() {
		errCh <- handler(ctx)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		_ = gl.logger.Warning(fmt.Sprintf(
			"<Guardian> force timing-out locks: <%+v> because: <%s> ", keys, ctx.Err()))
		return nil
	}
}

func (gl *GuardianLocker) lockKeys(keys []string) func() {
	switch len(keys) {
	case 0:
		return func() {}
	case 1:
		key := keys[0]
		gl.lockItem(key)
		return func() { gl.unlockItem(key) }
	}
	keys = slices.Clone(keys)
	slices.Sort(keys)
	keys = slices.Compact(keys)
	for _, key := range keys {
		gl.lockItem(key)
	}
	return func() {
		for _, key := range keys {
			gl.unlockItem(key)
		}
	}
}

// lockItem acquires a lock for the given item ID.
func (gl *GuardianLocker) lockItem(itmID string) {
	if itmID == "" {
		return
	}
	gl.lkMux.Lock()
	itmLock, exists := gl.locks[itmID]
	if !exists {
		itmLock = &itemLock{}
		gl.locks[itmID] = itmLock
	}
	itmLock.cnt++
	gl.lkMux.Unlock()
	itmLock.mu.Lock()
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
	itmLock.mu.Unlock()
}

type nopLogger struct{}

func (nopLogger) Warning(string) error { return nil }
