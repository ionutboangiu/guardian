// Copyright (C) ITsysCOM GmbH
// SPDX-License-Identifier: MIT

package guardian

import (
	"crypto/rand"
	"fmt"
	"slices"
	"sync"
	"time"
)

// ReferenceLocker locks keys and unlocks them by reference.
type ReferenceLocker struct {
	locker *Locker

	refsMu sync.Mutex
	refs   map[string]*referenceLock
}

type referenceLock struct {
	unlock func()
	timer  *time.Timer
}

// NewReferenceLocker returns a ReferenceLocker using locker.
func NewReferenceLocker(locker *Locker) *ReferenceLocker {
	return &ReferenceLocker{
		locker: locker,
		refs:   make(map[string]*referenceLock),
	}
}

// Lock locks keys and returns a reference for Unlock.
// A non-positive timeout uses the Locker timeout. The timeout starts after all
// keys are locked.
func (l *ReferenceLocker) Lock(timeout time.Duration, keys ...string) string {
	unlock := l.locker.lockKeys(keys)
	if timeout <= 0 {
		timeout = l.locker.timeout
	}

	ref := rand.Text()
	l.refsMu.Lock()
	lock := &referenceLock{unlock: unlock}
	if timeout > 0 {
		warningKeys := slices.Clone(keys)
		lock.timer = time.AfterFunc(timeout, func() {
			if l.Unlock(ref) && len(warningKeys) != 0 {
				_ = l.locker.logger.Warning(fmt.Sprintf(
					"<Guardian> force timing-out locks: %+v", warningKeys))
			}
		})
	}
	l.refs[ref] = lock
	l.refsMu.Unlock()
	return ref
}

// Unlock unlocks the keys for ref and reports whether ref was active.
func (l *ReferenceLocker) Unlock(ref string) bool {
	l.refsMu.Lock()
	lock, exists := l.refs[ref]
	delete(l.refs, ref)
	l.refsMu.Unlock()
	if !exists {
		return false
	}
	if lock.timer != nil {
		lock.timer.Stop()
	}
	lock.unlock()
	return true
}
