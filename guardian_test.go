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
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cgrates/birpc/context"
	"github.com/google/uuid"
)

func delayHandler(_ *context.Context) error {
	time.Sleep(100 * time.Millisecond)
	return nil
}

// Forks 3 groups of workers and makes sure that the time for execution is the one we expect for all 15 goroutines (with 100ms )
func TestGuardianMultipleKeys(t *testing.T) {
	gl := New()
	tStart := time.Now()
	maxIter := 5
	var wg sync.WaitGroup
	keys := []string{"test1", "test2", "test3"}
	for range maxIter {
		for _, key := range keys {
			wg.Add(1)
			go func(key string) {
				gl.Guard(context.TODO(), delayHandler, 0, key)
				wg.Done()
			}(key)
		}
	}
	wg.Wait()
	mustExecDur := time.Duration(maxIter*100) * time.Millisecond
	if execTime := time.Since(tStart); execTime < mustExecDur ||
		execTime > mustExecDur+100*time.Millisecond {
		t.Errorf("Execution took: %v", execTime)
	}
	for _, key := range keys {
		if _, hasKey := gl.locks[key]; hasKey {
			t.Errorf("Possible memleak for key: %s", key)
		}
	}
}

func TestGuardianTimeout(t *testing.T) {
	gl := New()
	tStart := time.Now()
	maxIter := 5
	var wg sync.WaitGroup
	keys := []string{"test1", "test2", "test3"}
	for range maxIter {
		for _, key := range keys {
			wg.Add(1)
			go func(key string) {
				gl.Guard(context.TODO(), delayHandler, 10*time.Millisecond, key)
				wg.Done()
			}(key)
		}
	}
	wg.Wait()
	mustExecDur := time.Duration(maxIter*10) * time.Millisecond
	if execTime := time.Since(tStart); execTime < mustExecDur ||
		execTime > mustExecDur+100*time.Millisecond {
		t.Errorf("Execution took: %v", execTime)
	}
	for _, key := range keys {
		if _, hasKey := gl.locks[key]; hasKey {
			t.Error("Possible memleak")
		}
	}
}

func TestGuardianGuardIDs(t *testing.T) {
	gl := New()

	//lock with 3 keys
	lockIDs := []string{"test1", "test2", "test3"}

	// lock 3 items
	tStart := time.Now()
	lockDur := 2 * time.Millisecond
	gl.GuardIDs("", lockDur, lockIDs...)
	for _, lockID := range lockIDs {
		if itmLock, hasKey := gl.locks[lockID]; !hasKey {
			t.Errorf("Cannot find lock for lockID: %s", lockID)
		} else if itmLock.cnt != 1 {
			t.Errorf("Unexpected itmLock found: %+v", itmLock)
		}
	}
	secLockDur := time.Millisecond
	// second lock to test counter
	go gl.GuardIDs("", secLockDur, lockIDs[1:]...)
	time.Sleep(time.Millisecond) // give time for goroutine to lock
	// check if counters were properly increased
	gl.lkMux.Lock()
	lkID := lockIDs[0]
	eCnt := int64(1)
	if itmLock, hasKey := gl.locks[lkID]; !hasKey {
		t.Errorf("Cannot find lock for lockID: %s", lkID)
	} else if itmLock.cnt != eCnt {
		t.Errorf("itemLock %q counter=%d, want %d", lkID, itmLock.cnt, eCnt)
	}
	lkID = lockIDs[1]
	eCnt = int64(2)
	if itmLock, hasKey := gl.locks[lkID]; !hasKey {
		t.Errorf("Cannot find lock for lockID: %s", lkID)
	} else if itmLock.cnt != eCnt {
		t.Errorf("itemLock %q counter=%d, want %d", lkID, itmLock.cnt, eCnt)
	}
	lkID = lockIDs[2]
	eCnt = int64(1) // we did not manage to increase it yet since it did not pass first lock
	if itmLock, hasKey := gl.locks[lkID]; !hasKey {
		t.Errorf("Cannot find lock for lockID: %s", lkID)
	} else if itmLock.cnt != eCnt {
		t.Errorf("itemLock %q counter=%d, want %d", lkID, itmLock.cnt, eCnt)
	}
	gl.lkMux.Unlock()
	time.Sleep(lockDur + secLockDur + 50*time.Millisecond) // give time to unlock before proceeding

	// make sure all counters were removed
	gl.lkMux.Lock()
	for _, lockID := range lockIDs {
		if _, hasKey := gl.locks[lockID]; hasKey {
			t.Errorf("Unexpected lockID found: %s", lockID)
		}
	}
	gl.lkMux.Unlock()

	// test lock  without timer
	refID := gl.GuardIDs("", 0, lockIDs...)

	if totalLockDur := time.Since(tStart); totalLockDur < lockDur {
		t.Errorf("Lock duration too small")
	}
	time.Sleep(30 * time.Millisecond)
	// making sure the items stay locked
	gl.lkMux.Lock()
	if len(gl.locks) != 3 {
		t.Errorf("locks should have 3 elements, have: %+v", gl.locks)
	}
	for _, lkID := range lockIDs {
		if itmLock, hasKey := gl.locks[lkID]; !hasKey {
			t.Errorf("Cannot find lock for lockID: %s", lkID)
		} else if itmLock.cnt != 1 {
			t.Errorf("itemLock %q counter=%d, want %d", lkID, itmLock.cnt, 1)
		}
	}
	gl.lkMux.Unlock()
	gl.UnguardIDs(refID)
	// make sure items were unlocked
	gl.lkMux.Lock()
	if len(gl.locks) != 0 {
		t.Errorf("locks should have 0 elements, has: %+v", gl.locks)
	}
	gl.lkMux.Unlock()
}

// TestGuardianGuardIDsConcurrent executes GuardIDs concurrently
func TestGuardianGuardIDsConcurrent(t *testing.T) {
	gl := New()
	maxIter := 500
	var wg sync.WaitGroup
	keys := []string{"test1", "test2", "test3"}
	refID := uuid.NewString()
	for range maxIter {
		wg.Add(1)
		go func() {
			if retRefID := gl.GuardIDs(refID, 0, keys...); retRefID != "" {
				if lkIDs := gl.UnguardIDs(refID); !reflect.DeepEqual(keys, lkIDs) {
					t.Errorf("expecting: %+v, received: %+v", keys, lkIDs)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if len(gl.locks) != 0 {
		t.Errorf("Possible memleak for locks: %+v", gl.locks)
	}
	if len(gl.refs) != 0 {
		t.Errorf("Possible memleak for refs: %+v", gl.refs)
	}
}

func TestGuardianGuardIDsTimeoutConcurrent(t *testing.T) {
	gl := New()
	maxIter := 50
	var wg sync.WaitGroup
	keys := []string{"test1", "test2", "test3"}
	refID := uuid.NewString()
	for range maxIter {
		wg.Add(1)
		go func() {
			gl.GuardIDs(refID, time.Microsecond, keys...)
			wg.Done()
		}()
	}
	wg.Wait()
	time.Sleep(10 * time.Millisecond)
	gl.lkMux.Lock()
	if len(gl.locks) != 0 {
		t.Errorf("Possible memleak for locks: %+v", gl.locks)
	}
	gl.lkMux.Unlock()
	gl.refsMux.Lock()
	if len(gl.refs) != 0 {
		t.Errorf("Possible memleak for refs: %+v", gl.refs)
	}
	gl.refsMux.Unlock()
}

func noopHandler(*context.Context) error { return nil }

func BenchmarkGuardUncontended(b *testing.B) {
	gl := New()
	for _, tc := range []struct {
		name    string
		timeout time.Duration
	}{
		{"without timeout", 0},
		{"with timeout", time.Hour},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.TODO()
			for b.Loop() {
				if err := gl.Guard(ctx, noopHandler, tc.timeout, "k"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkGuardContended(b *testing.B) {
	gl := New()
	for _, tc := range []struct {
		name    string
		timeout time.Duration
	}{
		{"without timeout", 0},
		{"with timeout", time.Hour},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				ctx := context.TODO()
				for pb.Next() {
					if err := gl.Guard(ctx, noopHandler, tc.timeout, "k"); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func TestGuardianLockItemUnlockItem(t *testing.T) {
	gl := New()
	itemID := ""
	gl.lockItem(itemID)
	gl.unlockItem(itemID)
	if itemID != "" {
		t.Errorf("\nExpected <%+v>, \nReceived <%+v>", "", itemID)
	}
}

func TestGuardianLockUnlockWithReference(t *testing.T) {
	gl := New()
	refID := ""
	gl.lockWithReference(refID, 0, []string{}...)
	gl.unlockWithReference(refID)
	if refID != "" {
		t.Errorf("\nExpected <%+v>, \nReceived <%+v>", "", refID)
	}
}

func TestGuardianGuardUnguardIDs(t *testing.T) {
	gl := New()
	refID := ""
	lkIDs := []string{"test1", "test2", "test3"}
	gl.GuardIDs(refID, time.Second, lkIDs...)
	gl.UnguardIDs(refID)
	if refID != "" {
		t.Errorf("\nExpected <%+v>, \nReceived <%+v>", "", refID)
	}
}

func TestGuardianGuardUnguardIDsCase2(t *testing.T) {
	gl := New()
	mockErr := errors.New("mock_error")
	lkIDs := []string{"test1", "test2", "test3"}
	err := gl.Guard(context.TODO(), func(_ *context.Context) error {
		return mockErr
	}, 10*time.Millisecond, lkIDs...)
	if err == nil || err != mockErr {
		t.Errorf("\nExpected <%+v>, \nReceived <%+v>", mockErr, err)
	}
}
