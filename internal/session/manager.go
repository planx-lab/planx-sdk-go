package session

import "sync"

type Manager[T any] struct {
	mu sync.RWMutex
	m  map[string]T
}

func NewManager[T any]() *Manager[T] {
	return &Manager[T]{m: make(map[string]T)}
}

func (m *Manager[T]) Add(id string, v T) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.m[id] = v
}

func (m *Manager[T]) Get(id string) (T, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.m[id]
	return v, ok
}

func (m *Manager[T]) Remove(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.m, id)
}
func (m *Manager[T]) All() []T {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]T, 0, len(m.m))
	for _, v := range m.m {
		result = append(result, v)
	}
	return result
}
