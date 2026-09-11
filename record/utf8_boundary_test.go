package record

import (
	"bytes"
	"testing"
)

func TestInvalidUTF8StringRejectedAtValueBatchAndEncode(t *testing.T) {
	invalid := string([]byte{'a', 0xff, 'b'})
	value := Value{Kind: String, Present: true, Data: invalid}
	batch := Batch{
		Schema:  Schema{Fields: []Field{{Name: "text", Kind: String}}},
		Records: []Record{{"text": value}},
	}
	for _, boundary := range []struct {
		name string
		call func() error
	}{
		{"Value.Validate", value.Validate},
		{"Batch.Validate", batch.Validate},
		{"Encode", func() error { _, err := Encode(batch); return err }},
	} {
		t.Run(boundary.name, func(t *testing.T) {
			if err := boundary.call(); err == nil {
				t.Fatal("invalid UTF-8 String was accepted")
			} else {
				t.Logf("rejected invalid UTF-8: %v", err)
			}
		})
	}
	if value.Data.(string) != invalid || batch.Records[0]["text"].Data.(string) != invalid {
		t.Fatal("rejection replaced invalid bytes or mutated the input")
	}
}

func TestDecodeRejectsInvalidUTF8RawTypedJSON(t *testing.T) {
	valid := Batch{
		Schema:  Schema{Fields: []Field{{Name: "text", Kind: String}}},
		Records: []Record{{"text": {Kind: String, Present: true, Data: "utf8-marker"}}},
	}
	wire, err := Encode(valid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(wire); err != nil {
		t.Fatalf("valid control failed: %v", err)
	}
	marker := []byte(`"utf8-marker"`)
	if bytes.Count(wire, marker) != 1 {
		t.Fatal("expected exactly one JSON string value to replace")
	}
	invalid := bytes.Replace(wire, marker, []byte{'"', 'a', 0xff, 'b', '"'}, 1)
	if _, err := Decode(invalid); err == nil {
		t.Fatal("Decode accepted raw invalid UTF-8, potentially replacing it with U+FFFD")
	} else {
		t.Logf("rejected raw invalid UTF-8: %v", err)
	}
}

func TestBinaryArbitraryBytesRoundTripWithUTF8Guards(t *testing.T) {
	want := []byte{0, 255, 1}
	value := Value{Kind: Binary, Present: true, Data: want}
	batch := Batch{
		Schema:  Schema{Fields: []Field{{Name: "binary", Kind: Binary}}},
		Records: []Record{{"binary": value}},
	}
	if err := value.Validate(); err != nil {
		t.Fatalf("Binary must not require UTF-8: %v", err)
	}
	if err := batch.Validate(); err != nil {
		t.Fatal(err)
	}
	wire, err := Encode(batch)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(wire)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := decoded.Records[0]["binary"].Data.([]byte)
	if !ok || !bytes.Equal(got, want) {
		t.Fatalf("Binary bytes changed: got %v, want %v", got, want)
	}
	t.Logf("Binary retained exact bytes: %v", got)
}
