// Copyright (c) 2025 Visvasity LLC

package kvhttp

import (
	"errors"
	"os"
	"testing"
	"time"
)

// TestLockExistingConcurrentClose verifies that when a name is closed while a
// LockExisting call is blocked waiting for its lock, the waiter observes the
// terminal state (ErrClosed) instead of proceeding with a stale reference
// (spec §7.3).
func TestLockExistingConcurrentClose(t *testing.T) {
	s := &server{}

	// Create and hold the lock for "n" (LockCreate returns with it locked).
	if _, exists := s.LockCreate("n"); exists {
		t.Fatal("unexpected pre-existing name")
	}

	done := make(chan error, 1)
	go func() {
		_, err := s.LockExisting("n")
		done <- err
	}()

	// Let the goroutine reach v.mu.Lock() and block on the held lock. It cannot
	// proceed until unlockTx releases the lock below, so this deterministically
	// exercises the post-lock re-check path.
	time.Sleep(50 * time.Millisecond)

	// Close the name (as commit/rollback/discard would) and release the lock.
	s.unlockTx("n", outcomeRolledBack)

	select {
	case err := <-done:
		if !errors.Is(err, os.ErrClosed) {
			t.Errorf("LockExisting after concurrent close = %v, want os.ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("LockExisting did not return")
	}
}
