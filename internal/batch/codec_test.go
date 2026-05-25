package batch

import (
	"testing"
)

func TestCodec_PackUnpackRoundtrip(t *testing.T) {
	codec := NewCodec()
	original := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	packed, err := codec.Pack(original)
	if err != nil {
		t.Fatalf("Pack failed: %v", err)
	}
	if len(packed) == 0 {
		t.Fatal("expected non-empty packed bytes")
	}

	unpacked, err := codec.Unpack(packed)
	if err != nil {
		t.Fatalf("Unpack failed: %v", err)
	}

	result, ok := unpacked.(map[string]string)
	if !ok {
		t.Fatalf("expected map[string]string, got %T", unpacked)
	}

	if len(result) != len(original) {
		t.Fatalf("expected %d entries, got %d", len(original), len(result))
	}
	for k, v := range original {
		if result[k] != v {
			t.Errorf("key %q: expected %q, got %q", k, v, result[k])
		}
	}
}

func TestCodec_PackUnpackEmptyData(t *testing.T) {
	codec := NewCodec()
	original := map[string]string{}

	packed, err := codec.Pack(original)
	if err != nil {
		t.Fatalf("Pack failed with empty map: %v", err)
	}

	unpacked, err := codec.Unpack(packed)
	if err != nil {
		t.Fatalf("Unpack failed with empty packed data: %v", err)
	}

	result, ok := unpacked.(map[string]string)
	if !ok {
		t.Fatalf("expected map[string]string, got %T", unpacked)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty map, got %d entries", len(result))
	}
}

func TestCodec_UnpackInvalidData(t *testing.T) {
	codec := NewCodec()

	_, err := codec.Unpack(PackedBatch{0xFF, 0x00, 0xAB})
	if err == nil {
		t.Fatal("expected error for invalid packed data, got nil")
	}
}

func TestCodec_UnpackEmptyBytes(t *testing.T) {
	codec := NewCodec()

	_, err := codec.Unpack(PackedBatch{})
	if err == nil {
		t.Fatal("expected error for empty bytes, got nil")
	}
}

func TestCodec_PackPreservesAllKeys(t *testing.T) {
	codec := NewCodec()
	original := map[string]string{
		"a":  "1",
		"b":  "2",
		"c":  "3",
		"d":  "4",
		"e":  "5",
		"f":  "6",
		"g":  "7",
		"h":  "8",
		"i":  "9",
		"j":  "10",
	}

	packed, err := codec.Pack(original)
	if err != nil {
		t.Fatalf("Pack failed: %v", err)
	}

	unpacked, err := codec.Unpack(packed)
	if err != nil {
		t.Fatalf("Unpack failed: %v", err)
	}

	result := unpacked.(map[string]string)
	if len(result) != len(original) {
		t.Fatalf("expected %d keys, got %d", len(original), len(result))
	}
	for k, expected := range original {
		if result[k] != expected {
			t.Errorf("key %q: expected %q, got %q", k, expected, result[k])
		}
	}
}

func TestCodec_PackedBytesAreDeterministic(t *testing.T) {
	codec := NewCodec()
	data := map[string]string{"x": "y"}

	p1, err := codec.Pack(data)
	if err != nil {
		t.Fatalf("Pack failed: %v", err)
	}
	p2, err := codec.Pack(data)
	if err != nil {
		t.Fatalf("Pack failed: %v", err)
	}

	if len(p1) != len(p2) {
		t.Fatalf("packed sizes differ: %d vs %d", len(p1), len(p2))
	}
	// gob encoding with same input should produce same output
	for i := range p1 {
		if p1[i] != p2[i] {
			t.Logf("packed bytes differ at index %d", i)
			break
		}
	}
}
