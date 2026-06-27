package flow

import (
	"context"
	"sync"
)

type Window struct {
	mu    sync.Mutex
	cond  *sync.Cond
	value int
}

func NewWindow(init int) *Window {
	w := &Window{value: init}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func (w *Window) Acquire() {
	w.mu.Lock()
	for w.value <= 0 {
		w.cond.Wait()
	}
	w.value--
	w.mu.Unlock()
}

// AcquireContext blocks until a slot is available or the context is cancelled.
// Returns ctx.Err() if the context is cancelled while waiting.
func (w *Window) AcquireContext(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for w.value <= 0 {
		// Wait with context cancellation support.
		if ctx.Done() == nil {
			w.cond.Wait()
			continue
		}

		// Spawn a goroutine to broadcast when context is done.
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				w.cond.Broadcast()
			case <-done:
			}
		}()

		w.cond.Wait()
		close(done)

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	w.value--
	return nil
}

func (w *Window) Release(n int) {
	if n <= 0 {
		return
	}
	w.mu.Lock()
	w.value += n
	w.mu.Unlock()
	w.cond.Broadcast()
}
