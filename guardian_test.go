// Copyright (C) ITsysCOM GmbH
// SPDX-License-Identifier: MIT

package guardian

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cgrates/birpc/context"
)

type warningLogger struct {
	warnings chan string
}

func (l *warningLogger) Warning(msg string) error {
	l.warnings <- msg
	return nil
}

func (l *Locker) lockEntryCount() int {
	l.locksMu.Lock()
	defer l.locksMu.Unlock()
	return len(l.locks)
}

func (l *Locker) lockCount(key string) int {
	l.locksMu.Lock()
	defer l.locksMu.Unlock()
	if lock := l.locks[key]; lock != nil {
		return lock.count
	}
	return 0
}

func waitFor(t *testing.T, d time.Duration, label string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", label)
		}
		time.Sleep(time.Millisecond)
	}
}

func noopHandler(*context.Context) error { return nil }

func TestLockBlocksSameKey(t *testing.T) {
	locker := New()
	unlock := locker.Lock("k")
	done := make(chan struct{})
	go func() {
		unlock := locker.Lock("k")
		unlock()
		close(done)
	}()
	waitFor(t, time.Second, "second Lock", func() bool {
		return locker.lockCount("k") == 2
	})
	select {
	case <-done:
		t.Fatal("second Lock acquired before unlock")
	default:
	}
	unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("second Lock did not acquire after unlock")
	}
	if n := locker.lockEntryCount(); n != 0 {
		t.Errorf("live locks = %d, want 0", n)
	}
}

func TestLockDuplicateKeys(t *testing.T) {
	locker := New()
	done := make(chan struct{})
	go func() {
		unlock := locker.Lock("b", "a", "b")
		unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("duplicate keys deadlocked")
	}
	if n := locker.lockEntryCount(); n != 0 {
		t.Errorf("live locks = %d, want 0", n)
	}
}

func TestLockOppositeKeyOrder(t *testing.T) {
	locker := New()
	locker.lockItem("a")
	locker.lockItem("b")
	done := make(chan struct{}, 2)
	for _, keys := range [][]string{{"a", "b"}, {"b", "a"}} {
		go func(keys []string) {
			unlock := locker.Lock(keys...)
			unlock()
			done <- struct{}{}
		}(keys)
	}
	waitFor(t, time.Second, "both lock calls queued", func() bool {
		return locker.lockCount("a")+locker.lockCount("b") == 4
	})
	locker.unlockItem("a")
	locker.unlockItem("b")
	for range 2 {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("opposite key order deadlocked")
		}
	}
	if n := locker.lockEntryCount(); n != 0 {
		t.Errorf("live locks = %d, want 0", n)
	}
}

func TestLockEmptyKeys(t *testing.T) {
	locker := New()
	locker.Lock()()
	locker.Lock("")()
	if n := locker.lockEntryCount(); n != 0 {
		t.Errorf("live locks = %d, want 0", n)
	}
}

func TestLockCopiesKeys(t *testing.T) {
	for _, original := range [][]string{{"a"}, {"b", "a"}} {
		locker := New()
		keys := append([]string(nil), original...)
		unlock := locker.Lock(keys...)
		for i := range keys {
			keys[i] = "changed"
		}
		unlock()
		if n := locker.lockEntryCount(); n != 0 {
			t.Errorf("Lock(%v) left %d live locks after input changed", original, n)
		}
	}
}

func TestLockTimeoutUnlocksKey(t *testing.T) {
	logger := &warningLogger{warnings: make(chan string, 2)}
	locker := New(WithTimeout(20*time.Millisecond), WithLogger(logger))
	unlock := locker.Lock("k")
	done := make(chan struct{})
	go func() {
		unlock := locker.Lock("k")
		unlock()
		close(done)
	}()
	waitFor(t, time.Second, "second Lock", func() bool {
		return locker.lockCount("k") == 2
	})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("second Lock stayed blocked after timeout")
	}
	unlock()
	select {
	case warning := <-logger.warnings:
		if !strings.Contains(warning, "force timing-out locks: [k]") {
			t.Errorf("unexpected warning: %q", warning)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout warning was not logged")
	}
	if n := locker.lockEntryCount(); n != 0 {
		t.Errorf("live locks = %d, want 0", n)
	}
}

func TestGuardHandlerError(t *testing.T) {
	locker := New()
	mockErr := errors.New("mock error")
	err := locker.Guard(context.TODO(), func(*context.Context) error {
		return mockErr
	}, "b", "a", "b")
	if !errors.Is(err, mockErr) {
		t.Errorf("Guard returned %v, want %v", err, mockErr)
	}
	if n := locker.lockEntryCount(); n != 0 {
		t.Errorf("live locks = %d, want 0", n)
	}
}

func TestGuardTimeoutReleasesLock(t *testing.T) {
	locker := New(WithTimeout(20 * time.Millisecond))
	started := make(chan struct{})
	release := make(chan struct{})
	handlerDone := make(chan struct{})
	guardDone := make(chan error, 1)
	go func() {
		guardDone <- locker.Guard(context.TODO(), func(*context.Context) error {
			close(started)
			<-release
			close(handlerDone)
			return nil
		}, "k")
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	select {
	case err := <-guardDone:
		if err != nil {
			t.Errorf("Guard returned %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Guard did not return after timeout")
	}
	lockDone := make(chan struct{})
	go func() {
		unlock := locker.Lock("k")
		unlock()
		close(lockDone)
	}()
	select {
	case <-lockDone:
	case <-time.After(time.Second):
		t.Fatal("lock was not released after Guard timeout")
	}
	select {
	case <-handlerDone:
		t.Fatal("handler returned before release")
	default:
	}
	close(release)
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("handler did not return")
	}
	if n := locker.lockEntryCount(); n != 0 {
		t.Errorf("live locks = %d, want 0", n)
	}
}

func BenchmarkLockUncontended(b *testing.B) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
	}{
		{"without timeout", 0},
		{"with timeout", time.Hour},
	} {
		b.Run(tc.name, func(b *testing.B) {
			locker := New(WithTimeout(tc.timeout))
			for b.Loop() {
				unlock := locker.Lock("k")
				unlock()
			}
		})
	}
}

func BenchmarkGuardUncontended(b *testing.B) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
	}{
		{"without timeout", 0},
		{"with timeout", time.Hour},
	} {
		b.Run(tc.name, func(b *testing.B) {
			locker := New(WithTimeout(tc.timeout))
			ctx := context.TODO()
			for b.Loop() {
				if err := locker.Guard(ctx, noopHandler, "k"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkGuardContended(b *testing.B) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
	}{
		{"without timeout", 0},
		{"with timeout", time.Hour},
	} {
		b.Run(tc.name, func(b *testing.B) {
			locker := New(WithTimeout(tc.timeout))
			b.RunParallel(func(pb *testing.PB) {
				ctx := context.TODO()
				for pb.Next() {
					if err := locker.Guard(ctx, noopHandler, "k"); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
