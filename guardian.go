// Copyright (C) ITsysCOM GmbH
// SPDX-License-Identifier: MIT

package guardian

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/cgrates/birpc/context"
)

// Option configures a Locker.
type Option func(*Locker)

type logger interface {
	Warning(string) error
}

// Locker is an optimized locking system that manages locks by string keys.
type Locker struct {
	timeout time.Duration
	logger  logger

	locksMu sync.Mutex
	locks   map[string]*itemLock
}

type itemLock struct {
	mu    sync.Mutex
	count int
}

// New creates a Locker with the provided options.
func New(opts ...Option) *Locker {
	l := &Locker{
		locks:  make(map[string]*itemLock),
		logger: nopLogger{},
	}

	for _, opt := range opts {
		opt(l)
	}

	return l
}

// WithTimeout sets the timeout for Guard and Lock.
// Non-positive durations disable the timeout.
func WithTimeout(d time.Duration) Option {
	return func(l *Locker) {
		l.timeout = d
	}
}

// WithLogger sets a custom logger for the Locker.
func WithLogger(logger logger) Option {
	return func(l *Locker) {
		if logger != nil {
			l.logger = logger
		}
	}
}

// Lock locks keys and returns an unlock function.
// The timeout starts once all keys are locked. Calling unlock after timeout is
// safe.
func (l *Locker) Lock(keys ...string) func() {
	unlock := l.lockKeys(keys)
	if l.timeout <= 0 {
		return unlock
	}
	warningKeys := slices.Clone(keys)
	var once sync.Once
	timer := time.AfterFunc(l.timeout, func() {
		once.Do(func() {
			unlock()
			if len(warningKeys) != 0 {
				_ = l.logger.Warning(fmt.Sprintf("<Guardian> force timing-out locks: %+v", warningKeys))
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
func (l *Locker) Guard(ctx *context.Context, handler func(*context.Context) error,
	keys ...string) error {
	if len(keys) == 1 {
		key := keys[0]
		l.lockItem(key)
		defer l.unlockItem(key)
	} else {
		unlock := l.lockKeys(keys)
		defer unlock()
	}
	if l.timeout <= 0 && ctx.Done() == nil {
		return handler(ctx)
	}
	errCh := make(chan error, 1)

	if l.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, l.timeout)
		defer cancel()
	}
	go func() {
		errCh <- handler(ctx)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		_ = l.logger.Warning(fmt.Sprintf(
			"<Guardian> force timing-out locks: <%+v> because: <%s> ", keys, ctx.Err()))
		return nil
	}
}

func (l *Locker) lockKeys(keys []string) func() {
	switch len(keys) {
	case 0:
		return func() {}
	case 1:
		key := keys[0]
		l.lockItem(key)
		return func() { l.unlockItem(key) }
	}
	keys = slices.Clone(keys)
	slices.Sort(keys)
	keys = slices.Compact(keys)
	for _, key := range keys {
		l.lockItem(key)
	}
	return func() {
		for _, key := range keys {
			l.unlockItem(key)
		}
	}
}

// lockItem acquires a lock for the given item ID.
func (l *Locker) lockItem(itemID string) {
	if itemID == "" {
		return
	}
	l.locksMu.Lock()
	lock, exists := l.locks[itemID]
	if !exists {
		lock = &itemLock{}
		l.locks[itemID] = lock
	}
	lock.count++
	l.locksMu.Unlock()
	lock.mu.Lock()
}

// unlockItem releases a lock for the given item ID.
func (l *Locker) unlockItem(itemID string) {
	l.locksMu.Lock()
	lock, exists := l.locks[itemID]
	if !exists {
		l.locksMu.Unlock()
		return
	}
	lock.count--
	if lock.count == 0 {
		delete(l.locks, itemID)
	}
	l.locksMu.Unlock()
	lock.mu.Unlock()
}

type nopLogger struct{}

func (nopLogger) Warning(string) error { return nil }
