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

func (gl *GuardianLocker) lockEntryCount() int {
	gl.lkMux.Lock()
	defer gl.lkMux.Unlock()
	return len(gl.locks)
}

func (gl *GuardianLocker) lockRefCount(key string) int64 {
	gl.lkMux.Lock()
	defer gl.lkMux.Unlock()
	if lock := gl.locks[key]; lock != nil {
		return lock.cnt
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
	gl := New()
	unlock := gl.Lock("k")
	done := make(chan struct{})
	go func() {
		unlock := gl.Lock("k")
		unlock()
		close(done)
	}()
	waitFor(t, time.Second, "second Lock", func() bool {
		return gl.lockRefCount("k") == 2
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
	if n := gl.lockEntryCount(); n != 0 {
		t.Errorf("live locks = %d, want 0", n)
	}
}

func TestLockDuplicateKeys(t *testing.T) {
	gl := New()
	done := make(chan struct{})
	go func() {
		unlock := gl.Lock("b", "a", "b")
		unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("duplicate keys deadlocked")
	}
	if n := gl.lockEntryCount(); n != 0 {
		t.Errorf("live locks = %d, want 0", n)
	}
}

func TestLockOppositeKeyOrder(t *testing.T) {
	gl := New()
	gl.lockItem("a")
	gl.lockItem("b")
	done := make(chan struct{}, 2)
	for _, keys := range [][]string{{"a", "b"}, {"b", "a"}} {
		go func(keys []string) {
			unlock := gl.Lock(keys...)
			unlock()
			done <- struct{}{}
		}(keys)
	}
	waitFor(t, time.Second, "both lock calls queued", func() bool {
		return gl.lockRefCount("a")+gl.lockRefCount("b") == 4
	})
	gl.unlockItem("a")
	gl.unlockItem("b")
	for range 2 {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("opposite key order deadlocked")
		}
	}
	if n := gl.lockEntryCount(); n != 0 {
		t.Errorf("live locks = %d, want 0", n)
	}
}

func TestLockEmptyKeys(t *testing.T) {
	gl := New()
	gl.Lock()()
	gl.Lock("")()
	if n := gl.lockEntryCount(); n != 0 {
		t.Errorf("live locks = %d, want 0", n)
	}
}

func TestLockCopiesKeys(t *testing.T) {
	for _, original := range [][]string{{"a"}, {"b", "a"}} {
		gl := New()
		keys := append([]string(nil), original...)
		unlock := gl.Lock(keys...)
		for i := range keys {
			keys[i] = "changed"
		}
		unlock()
		if n := gl.lockEntryCount(); n != 0 {
			t.Errorf("Lock(%v) left %d live locks after input changed", original, n)
		}
	}
}

func TestLockTimeoutUnlocksKey(t *testing.T) {
	logger := &warningLogger{warnings: make(chan string, 2)}
	gl := New(WithTimeout(20*time.Millisecond), WithLogger(logger))
	unlock := gl.Lock("k")
	done := make(chan struct{})
	go func() {
		unlock := gl.Lock("k")
		unlock()
		close(done)
	}()
	waitFor(t, time.Second, "second Lock", func() bool {
		return gl.lockRefCount("k") == 2
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
	if n := gl.lockEntryCount(); n != 0 {
		t.Errorf("live locks = %d, want 0", n)
	}
}

func TestGuardHandlerError(t *testing.T) {
	gl := New()
	mockErr := errors.New("mock error")
	err := gl.Guard(context.TODO(), func(*context.Context) error {
		return mockErr
	}, "b", "a", "b")
	if !errors.Is(err, mockErr) {
		t.Errorf("Guard returned %v, want %v", err, mockErr)
	}
	if n := gl.lockEntryCount(); n != 0 {
		t.Errorf("live locks = %d, want 0", n)
	}
}

func TestGuardTimeoutReleasesLock(t *testing.T) {
	gl := New(WithTimeout(20 * time.Millisecond))
	started := make(chan struct{})
	release := make(chan struct{})
	handlerDone := make(chan struct{})
	guardDone := make(chan error, 1)
	go func() {
		guardDone <- gl.Guard(context.TODO(), func(*context.Context) error {
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
		unlock := gl.Lock("k")
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
	if n := gl.lockEntryCount(); n != 0 {
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
			gl := New(WithTimeout(tc.timeout))
			for b.Loop() {
				unlock := gl.Lock("k")
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
			gl := New(WithTimeout(tc.timeout))
			ctx := context.TODO()
			for b.Loop() {
				if err := gl.Guard(ctx, noopHandler, "k"); err != nil {
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
			gl := New(WithTimeout(tc.timeout))
			b.RunParallel(func(pb *testing.PB) {
				ctx := context.TODO()
				for pb.Next() {
					if err := gl.Guard(ctx, noopHandler, "k"); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
