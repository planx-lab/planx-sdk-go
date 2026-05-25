package util

import (
	"strings"
	"testing"
)

func TestNewSessionID_NotEmpty(t *testing.T) {
	id := NewSessionID()
	if id == "" {
		t.Fatal("session ID should not be empty")
	}
}

func TestNewSessionID_UUIDFormat(t *testing.T) {
	id := NewSessionID()
	if len(id) != 36 {
		t.Fatalf("UUID length: got %d, want 36", len(id))
	}
	parts := strings.Split(id, "-")
	if len(parts) != 5 {
		t.Fatalf("UUID parts: got %d, want 5", len(parts))
	}
	expected := []int{8, 4, 4, 4, 12}
	for i, p := range parts {
		if len(p) != expected[i] {
			t.Fatalf("part %d: got len %d, want %d", i, len(p), expected[i])
		}
	}
}

func TestNewSessionID_Unique(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := NewSessionID()
		if ids[id] {
			t.Fatalf("duplicate ID: %s", id)
		}
		ids[id] = true
	}
}
