package sdk

import (
	"testing"
)

// ToRows is the canonical batch normalization at the source boundary. Every
// source emits []Row; processors read/write only []Row. This makes processor
// chains unbreakable: there is exactly one in-process batch type. (Logstash
// Event / NiFi Record / Kafka Connect ConnectRecord-value analog.)
func TestToRows_CSVStringGrid(t *testing.T) {
	// CSV source: [][]string where row 0 is the header.
	in := [][]string{
		{"name", "ssn", "city"},
		{"alice", "123", "NYC"},
		{"bob", "987", "LA"},
	}
	rows, err := ToRows(in)
	if err != nil {
		t.Fatalf("ToRows: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 data rows (header consumed), got %d", len(rows))
	}
	if rows[0]["name"] != "alice" || rows[0]["ssn"] != "123" || rows[0]["city"] != "NYC" {
		t.Errorf("row 0 = %v, want alice/123/NYC keyed by header", rows[0])
	}
	if rows[1]["name"] != "bob" {
		t.Errorf("row 1 name = %v, want bob", rows[1]["name"])
	}
}

func TestToRows_NativeRows(t *testing.T) {
	in := []Row{{"id": float64(1), "name": "x"}}
	rows, err := ToRows(in)
	if err != nil || len(rows) != 1 || rows[0]["id"] != float64(1) {
		t.Fatalf("native []Row passthrough: %v %v", rows, err)
	}
}

func TestToRows_SingleRow(t *testing.T) {
	in := Row{"k": "v"}
	rows, err := ToRows(in)
	if err != nil || len(rows) != 1 || rows[0]["k"] != "v" {
		t.Fatalf("single Row wrap: %v %v", rows, err)
	}
}

func TestToRows_Empty(t *testing.T) {
	rows, err := ToRows([][]string{})
	if err != nil || len(rows) != 0 {
		t.Fatalf("empty: %v %v, want empty/no-error", rows, err)
	}
}

func TestToRows_HeaderOnly(t *testing.T) {
	rows, err := ToRows([][]string{{"a", "b"}})
	if err != nil || len(rows) != 0 {
		t.Fatalf("header-only: %v %v, want 0 rows", rows, err)
	}
}

func TestToRows_Unsupported(t *testing.T) {
	_, err := ToRows(42)
	if err == nil {
		t.Fatal("expected error for int")
	}
}

// FromRowsToStringGrid is the canonical→CSV sink boundary: rebuilds a
// [][]string (header + stringified data rows) from []Row, using the provided
// column order.
func TestFromRowsToStringGrid_Basic(t *testing.T) {
	rows := []Row{
		{"name": "alice", "ssn": "***", "city": "NYC"},
		{"name": "bob", "ssn": "***", "city": "LA"},
	}
	grid, err := FromRowsToStringGrid(rows, []string{"name", "ssn", "city"})
	if err != nil {
		t.Fatalf("FromRowsToStringGrid: %v", err)
	}
	if len(grid) != 3 { // header + 2 rows
		t.Fatalf("expected 3 rows (header+data), got %d", len(grid))
	}
	if len(grid[0]) != 3 || grid[0][0] != "name" || grid[0][1] != "ssn" || grid[0][2] != "city" {
		t.Errorf("header = %v, want [name ssn city]", grid[0])
	}
	if grid[1][0] != "alice" || grid[1][1] != "***" || grid[1][2] != "NYC" {
		t.Errorf("row 0 = %v, want alice/*** /NYC", grid[1])
	}
}

func TestFromRowsToStringGrid_MissingFieldBecomesEmpty(t *testing.T) {
	rows := []Row{{"name": "alice"}} // missing ssn, city
	grid, err := FromRowsToStringGrid(rows, []string{"name", "ssn", "city"})
	if err != nil {
		t.Fatalf("FromRowsToStringGrid: %v", err)
	}
	if grid[1][1] != "" {
		t.Errorf("missing field should be empty string, got %q", grid[1][1])
	}
}

func TestFromRowsToStringGrid_EmptyRows(t *testing.T) {
	grid, err := FromRowsToStringGrid([]Row{}, []string{"a", "b"})
	if err != nil {
		t.Fatalf("FromRowsToStringGrid: %v", err)
	}
	if len(grid) != 1 { // just header
		t.Fatalf("expected 1 row (header only), got %d", len(grid))
	}
}

func TestFromRowsToStringGrid_NonStringValueStringified(t *testing.T) {
	rows := []Row{{"count": float64(42), "active": true}}
	grid, err := FromRowsToStringGrid(rows, []string{"count", "active"})
	if err != nil {
		t.Fatalf("FromRowsToStringGrid: %v", err)
	}
	if grid[1][0] != "42" {
		t.Errorf("float64 42 -> %q, want \"42\"", grid[1][0])
	}
	if grid[1][1] != "true" {
		t.Errorf("bool true -> %q, want \"true\"", grid[1][1])
	}
}
