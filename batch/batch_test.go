package batch

import (
	"bytes"
	"testing"
)

func TestPackUnpackRoundTrip(t *testing.T) {
	original := Batch{
		Records: []Record{
			{
				Metadata: map[string]string{"source": "mysql", "table": "users"},
				Payload:  []byte(`{"id":1,"name":"Alice"}`),
			},
			{
				Metadata: map[string]string{"source": "mysql", "table": "users"},
				Payload:  []byte(`{"id":2,"name":"Bob"}`),
			},
		},
		Context: map[string]string{
			"tenant_id": "tenant-123",
			"batch_id":  "batch-456",
		},
	}

	packed, err := PackBatch(original)
	if err != nil {
		t.Fatalf("PackBatch failed: %v", err)
	}

	unpacked, err := UnpackBatch(packed)
	if err != nil {
		t.Fatalf("UnpackBatch failed: %v", err)
	}

	// Verify records
	if len(unpacked.Records) != len(original.Records) {
		t.Fatalf("record count mismatch: got %d, want %d", len(unpacked.Records), len(original.Records))
	}

	for i, r := range unpacked.Records {
		orig := original.Records[i]
		if !bytes.Equal(r.Payload, orig.Payload) {
			t.Errorf("record %d payload mismatch", i)
		}
		for k, v := range orig.Metadata {
			if r.Metadata[k] != v {
				t.Errorf("record %d metadata[%s] mismatch: got %s, want %s", i, k, r.Metadata[k], v)
			}
		}
	}

	// Verify context
	for k, v := range original.Context {
		if unpacked.Context[k] != v {
			t.Errorf("context[%s] mismatch: got %s, want %s", k, unpacked.Context[k], v)
		}
	}
}

func TestEmptyBatch(t *testing.T) {
	original := Batch{
		Records: []Record{},
		Context: map[string]string{},
	}

	packed, err := PackBatch(original)
	if err != nil {
		t.Fatalf("PackBatch failed: %v", err)
	}

	unpacked, err := UnpackBatch(packed)
	if err != nil {
		t.Fatalf("UnpackBatch failed: %v", err)
	}

	if len(unpacked.Records) != 0 {
		t.Errorf("expected 0 records, got %d", len(unpacked.Records))
	}
}

func TestInvalidMagicNumber(t *testing.T) {
	data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	_, err := UnpackBatch(data)
	if err == nil {
		t.Error("expected error for invalid magic number")
	}
}

func BenchmarkPackBatch(b *testing.B) {
	batch := Batch{
		Records: make([]Record, 100),
		Context: map[string]string{"tenant": "test"},
	}
	for i := range batch.Records {
		batch.Records[i] = Record{
			Metadata: map[string]string{"idx": "test"},
			Payload:  make([]byte, 1024),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = PackBatch(batch)
	}
}
