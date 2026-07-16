// Copyright (C) ITsysCOM GmbH
// SPDX-License-Identifier: MIT

package guardian_test

import (
	"testing"
	"time"

	"github.com/cgrates/guardian"
)

func TestReferenceLocker(t *testing.T) {
	locker := guardian.New()
	refLocker := guardian.NewReferenceLocker(locker)
	ref := refLocker.Lock(0, "key")

	started := make(chan struct{})
	acquired := make(chan struct{})
	go func() {
		close(started)
		unlock := locker.Lock("key")
		unlock()
		close(acquired)
	}()
	<-started
	select {
	case <-acquired:
		t.Fatal("Locker acquired before reference was unlocked")
	case <-time.After(20 * time.Millisecond):
	}

	if !refLocker.Unlock(ref) {
		t.Fatal("failed to unlock reference")
	}
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("Locker did not acquire after Unlock")
	}
}
