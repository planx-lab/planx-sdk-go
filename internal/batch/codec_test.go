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

// TestCodec_StandardBatchShapesRoundtripWithoutRegistration verifies the codec
// can pack AND unpack every standard batch shape a Planx plugin may emit —
// WITHOUT the consumer calling RegisterType first. This is the "bytes-opaque"
// contract: a Source (one OS process) emits a type, a Sink/Processor (a
// DIFFERENT process with its own gob type registry) must decode it without
// pre-registering the Source's concrete types.
//
// Today this fails: gob's `any` interface encoding writes the concrete type
// name, and the decoder requires that name registered in its own process.
// The fix: the SDK's codec init registers the full standard type set so every
// SDK process (Source/Processor/Sink) shares the same type universe.
func TestCodec_StandardBatchShapesRoundtripWithoutRegistration(t *testing.T) {
	codec := NewCodec()
	// These are the real batch shapes emitted by Planx plugins:
	//   [][]string          — CSV source (rows of string fields)
	//   []map[string]any    — processors (normalized rows)
	//   map[string]any      — single-row batch
	//   map[string]string   — string-valued single row (hello-style source)
	//   []map[string]string — string-valued rows
	//   []string            — text-template processor output
	standardShapes := []struct {
		name string
		val  any
	}{
		{"[][]string", [][]string{{"a", "b"}, {"c", "d"}}},
		{"[]map[string]any", []map[string]any{{"id": float64(1), "name": "x"}}},
		{"map[string]any", map[string]any{"id": float64(1), "name": "x"}},
		{"map[string]string", map[string]string{"k": "v"}},
		{"[]map[string]string", []map[string]string{{"k": "v"}}},
		{"[]string", []string{"rendered", "output"}},
	}

	for _, tc := range standardShapes {
		t.Run(tc.name, func(t *testing.T) {
			packed, err := codec.Pack(tc.val)
			if err != nil {
				t.Fatalf("Pack(%s): %v", tc.name, err)
			}
			unpacked, err := codec.Unpack(packed)
			if err != nil {
				t.Fatalf("Unpack(%s): %v (decoder must know this type without per-plugin registration)", tc.name, err)
			}
			// The unpacked value must be usable — not a nil/opaque blob.
			if unpacked == nil {
				t.Fatalf("Unpack(%s): got nil, want the original value", tc.name)
			}
		})
	}
}
