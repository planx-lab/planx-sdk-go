// Package session provides multi-session management for Planx plugins.
// Every plugin process MUST support multiple concurrent sessions.
package session

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

// ErrSessionNotFound is returned when a session ID is not found.
var ErrSessionNotFound = errors.New("session not found")

// ErrSessionClosed is returned when operating on a closed session.
var ErrSessionClosed = errors.New("session is closed")

// Session represents a single tenant session within a plugin.
// Session MUST maintain: configuration, tenant context, connection pool,
// flow control window, and metrics.
type Session struct {
	ID       string
	TenantID string
	Config   []byte

	// Flow control
	WindowSize int32
	window     int32

	// User data (connection pools, caches, etc.)
	data   map[string]interface{}
	dataMu sync.RWMutex

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	closed atomic.Bool
}

// NewSession creates a new session with the given parameters.
func NewSession(tenantID string, config []byte) *Session {
	ctx, cancel := context.WithCancel(context.Background())
	return &Session{
		ID:         uuid.New().String(),
		TenantID:   tenantID,
		Config:     config,
		WindowSize: 20, // default window
		window:     20,
		data:       make(map[string]interface{}),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Context returns the session's context, which is cancelled when the session is closed.
func (s *Session) Context() context.Context {
	return s.ctx
}

// Close marks the session as closed and cancels its context.
func (s *Session) Close() error {
	if s.closed.CompareAndSwap(false, true) {
		s.cancel()
	}
	return nil
}

// IsClosed returns whether the session is closed.
func (s *Session) IsClosed() bool {
	return s.closed.Load()
}

// SetData stores user data in the session.
func (s *Session) SetData(key string, value interface{}) {
	s.dataMu.Lock()
	defer s.dataMu.Unlock()
	s.data[key] = value
}

// GetData retrieves user data from the session.
func (s *Session) GetData(key string) (interface{}, bool) {
	s.dataMu.RLock()
	defer s.dataMu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Window returns the current flow control window.
func (s *Session) Window() int32 {
	return atomic.LoadInt32(&s.window)
}

// ConsumeWindow decrements the window by 1. Returns false if window is 0.
func (s *Session) ConsumeWindow() bool {
	for {
		current := atomic.LoadInt32(&s.window)
		if current <= 0 {
			return false
		}
		if atomic.CompareAndSwapInt32(&s.window, current, current-1) {
			return true
		}
	}
}

// UpdateWindow sets a new window size.
func (s *Session) UpdateWindow(newWindow int32) {
	atomic.StoreInt32(&s.window, newWindow)
}

// Manager manages multiple sessions for a plugin.
type Manager struct {
	sessions sync.Map // map[string]*Session
	count    atomic.Int32
}

// NewManager creates a new session manager.
func NewManager() *Manager {
	return &Manager{}
}

// Create creates a new session and registers it.
func (m *Manager) Create(tenantID string, config []byte) *Session {
	s := NewSession(tenantID, config)
	m.sessions.Store(s.ID, s)
	m.count.Add(1)
	return s
}

// Get retrieves a session by ID.
func (m *Manager) Get(sessionID string) (*Session, error) {
	v, ok := m.sessions.Load(sessionID)
	if !ok {
		return nil, ErrSessionNotFound
	}
	s := v.(*Session)
	if s.IsClosed() {
		return nil, ErrSessionClosed
	}
	return s, nil
}

// Close closes and removes a session.
func (m *Manager) Close(sessionID string) error {
	v, ok := m.sessions.LoadAndDelete(sessionID)
	if !ok {
		return ErrSessionNotFound
	}
	m.count.Add(-1)
	return v.(*Session).Close()
}

// Count returns the number of active sessions.
func (m *Manager) Count() int {
	return int(m.count.Load())
}

// CloseAll closes all sessions.
func (m *Manager) CloseAll() {
	m.sessions.Range(func(key, value interface{}) bool {
		s := value.(*Session)
		s.Close()
		m.sessions.Delete(key)
		m.count.Add(-1)
		return true
	})
}
