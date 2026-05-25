package flow

import (
	"sync"
	"testing"
	"time"
)

func TestWindow_AcquireRelease(t *testing.T) {
	w := NewWindow(3)

	// Acquire should succeed immediately when window > 0
	w.Acquire()
	if w.value != 2 {
		t.Fatalf("expected value 2 after acquire, got %d", w.value)
	}

	w.Acquire()
	if w.value != 1 {
		t.Fatalf("expected value 1 after second acquire, got %d", w.value)
	}

	// Release adds back
	w.Release(1)
	if w.value != 2 {
		t.Fatalf("expected value 2 after release, got %d", w.value)
	}
}

func TestWindow_AcquireBlocksAtZero(t *testing.T) {
	w := NewWindow(1)

	// Use up the window
	w.Acquire()

	done := make(chan struct{})
	go func() {
		w.Acquire() // should block
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("expected Acquire to block when window is zero")
	case <-time.After(50 * time.Millisecond):
		// Expected: still blocked
	}

	// Release to unblock
	w.Release(1)

	select {
	case <-done:
		// Expected: unblocked
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected Acquire to unblock after Release")
	}
}

func TestWindow_ReleaseWakesBlocked(t *testing.T) {
	w := NewWindow(0)

	blocked := make(chan struct{})
	acquired := make(chan struct{})

	go func() {
		close(blocked)
		w.Acquire()
		close(acquired)
	}()

	// Wait for goroutine to start blocking
	<-blocked
	time.Sleep(20 * time.Millisecond)

	select {
	case <-acquired:
		t.Fatal("expected Acquire to still be blocked")
	default:
	}

	// Release should wake it
	w.Release(1)

	select {
	case <-acquired:
		// Success
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected Acquire to complete after Release")
	}
}

func TestWindow_ConcurrentAcquireRelease(t *testing.T) {
	w := NewWindow(10)
	var wg sync.WaitGroup

	// 20 goroutines each try to acquire
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.Acquire()
			// Hold briefly
			time.Sleep(5 * time.Millisecond)
			w.Release(1)
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines completed
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent acquire/release timed out")
	}

	// Window should be back to original value
	if w.value != 10 {
		t.Fatalf("expected window value 10 after all releases, got %d", w.value)
	}
}

func TestWindow_ReleaseMultiple(t *testing.T) {
	w := NewWindow(0)

	// Release 5 at once
	w.Release(5)
	if w.value != 5 {
		t.Fatalf("expected value 5, got %d", w.value)
	}

	// Now 5 acquires should work without blocking
	for i := range 5 {
		done := make(chan struct{})
		go func() {
			w.Acquire()
			close(done)
		}()
		select {
		case <-done:
			// Good
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("acquire %d blocked unexpectedly", i)
		}
	}
}

func TestWindow_ZeroInit(t *testing.T) {
	w := NewWindow(0)

	if w.value != 0 {
		t.Fatalf("expected initial value 0, got %d", w.value)
	}

	// Acquire on zero-window should block
	done := make(chan struct{})
	go func() {
		w.Acquire()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("expected Acquire to block on zero window")
	case <-time.After(50 * time.Millisecond):
		// Expected
	}
}
