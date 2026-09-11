package record

import (
	"encoding/json"
	"math"
	"testing"
)

func TestPublicJSONNames(t *testing.T) {
	b := Batch{
		Schema: Schema{Fields: []Field{{Name: "nested", Kind: Struct, Fields: []Field{
			{Name: "items", Kind: List, Element: &Field{Kind: String}},
		}}}},
		Records: []Record{{"nested": {Kind: Struct, Present: true, Data: Record{
			"items": {Kind: List, Present: true, Data: []Value{{Kind: String, Present: true, Data: ""}}},
		}}}},
	}
	p, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(p, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["schema"] == nil || got["records"] == nil {
		t.Fatalf("batch JSON keys: %s", p)
	}
	var schema struct {
		Fields []map[string]json.RawMessage `json:"fields"`
	}
	if err := json.Unmarshal(got["schema"], &schema); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"name", "kind", "nullable", "precision", "scale", "fields"} {
		if schema.Fields[0][key] == nil {
			t.Fatalf("missing lowerCamel field key %q: %s", key, p)
		}
	}
	var fields []map[string]json.RawMessage
	if err := json.Unmarshal(schema.Fields[0]["fields"], &fields); err != nil {
		t.Fatal(err)
	}
	if fields[0]["element"] == nil {
		t.Fatalf("missing element key: %s", p)
	}
	var rows []map[string]map[string]json.RawMessage
	if err := json.Unmarshal(got["records"], &rows); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"kind", "present", "null", "data"} {
		if rows[0]["nested"][key] == nil {
			t.Fatalf("missing lowerCamel value key %q: %s", key, p)
		}
	}
}

func TestFloat64EncodingPreservesBits(t *testing.T) {
	for _, want := range []float64{math.Pi, math.SmallestNonzeroFloat64, math.MaxFloat64, math.Copysign(0, -1), 1.0000000000000002} {
		b := Batch{Schema: Schema{Fields: []Field{{Name: "f", Kind: Float64}}}, Records: []Record{
			{"f": {Kind: Float64, Present: true, Data: want}},
		}}
		p, err := Encode(b)
		if err != nil {
			t.Fatal(err)
		}
		got, err := Decode(p)
		if err != nil {
			t.Fatal(err)
		}
		if math.Float64bits(got.Records[0]["f"].Data.(float64)) != math.Float64bits(want) {
			t.Fatalf("float bits changed: %s", p)
		}
	}
}
