package flow

import "sync"

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

func (w *Window) Release(n int) {
	w.mu.Lock()
	w.value += n
	w.mu.Unlock()
	w.cond.Broadcast()
}
