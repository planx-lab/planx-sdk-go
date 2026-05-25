package session

import (
	"sync"
	"testing"
)

func TestManager_AddAndGet(t *testing.T) {
	m := NewManager[string]()
	m.Add("key1", "value1")

	got, ok := m.Get("key1")
	if !ok {
		t.Fatal("expected key1 to exist")
	}
	if got != "value1" {
		t.Fatalf("expected value1, got %q", got)
	}
}

func TestManager_GetNonExistent(t *testing.T) {
	m := NewManager[int]()

	_, ok := m.Get("missing")
	if ok {
		t.Fatal("expected missing key to not be found")
	}
}

func TestManager_AddOverwrites(t *testing.T) {
	m := NewManager[string]()
	m.Add("k", "first")
	m.Add("k", "second")

	got, ok := m.Get("k")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if got != "second" {
		t.Fatalf("expected second, got %q", got)
	}
}

func TestManager_Remove(t *testing.T) {
	m := NewManager[string]()
	m.Add("key1", "value1")
	m.Remove("key1")

	_, ok := m.Get("key1")
	if ok {
		t.Fatal("expected key1 to be removed")
	}
}

func TestManager_RemoveNonExistent(t *testing.T) {
	m := NewManager[string]()
	// Should not panic
	m.Remove("nonexistent")
}

func TestManager_All(t *testing.T) {
	m := NewManager[int]()
	m.Add("a", 1)
	m.Add("b", 2)
	m.Add("c", 3)

	all := m.All()
	if len(all) != 3 {
		t.Fatalf("expected 3 items, got %d", len(all))
	}

	// All values should be present (order not guaranteed)
	found := map[int]bool{}
	for _, v := range all {
		found[v] = true
	}
	for _, expected := range []int{1, 2, 3} {
		if !found[expected] {
			t.Errorf("expected to find %d in All() result", expected)
		}
	}
}

func TestManager_AllEmpty(t *testing.T) {
	m := NewManager[string]()
	all := m.All()
	if all == nil {
		t.Fatal("expected non-nil slice for empty manager")
	}
	if len(all) != 0 {
		t.Fatalf("expected 0 items, got %d", len(all))
	}
}

func TestManager_ConcurrentAccess(t *testing.T) {
	m := NewManager[int]()
	var wg sync.WaitGroup

	// Concurrent writers
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.Add(string(rune('a'+i%26)), i)
		}(i)
	}

	// Concurrent readers
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = m.Get(string(rune('a' + i%26)))
		}(i)
	}

	// Concurrent removers
	for i := range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.Remove(string(rune('A' + i%26)))
		}(i)
	}

	// Concurrent All()
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.All()
		}()
	}

	wg.Wait()
}

func TestManager_DeleteAndGet_Existing(t *testing.T) {
	m := NewManager[string]()
	m.Add("k1", "v1")

	v, ok := m.DeleteAndGet("k1")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if v != "v1" {
		t.Fatalf("got %q, want %q", v, "v1")
	}

	_, ok = m.Get("k1")
	if ok {
		t.Fatal("key should be removed after DeleteAndGet")
	}
}

func TestManager_DeleteAndGet_Missing(t *testing.T) {
	m := NewManager[int]()

	v, ok := m.DeleteAndGet("nope")
	if ok {
		t.Fatal("expected ok=false for missing key")
	}
	if v != 0 {
		t.Fatalf("expected zero value, got %d", v)
	}
}

func TestManager_DeleteAndGet_Concurrent(t *testing.T) {
	m := NewManager[int]()
	for i := range 100 {
		m.Add(string(rune('A'+i%26)), i)
	}

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.DeleteAndGet(string(rune('A' + i%26)))
		}(i)
	}
	wg.Wait()
}

func TestManager_GenericType(t *testing.T) {
	// Verify Manager works with different types
	intMgr := NewManager[int]()
	intMgr.Add("x", 42)
	v, ok := intMgr.Get("x")
	if !ok || v != 42 {
		t.Fatalf("expected 42, got %d, ok=%v", v, ok)
	}

	type item struct {
		Name  string
		Value int
	}
	structMgr := NewManager[item]()
	structMgr.Add("s", item{Name: "test", Value: 99})
	sv, ok := structMgr.Get("s")
	if !ok || sv.Name != "test" || sv.Value != 99 {
		t.Fatalf("expected {test 99}, got %+v, ok=%v", sv, ok)
	}
}
