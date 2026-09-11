package record

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func temporalPrecision(p int32) *int32 { return &p }

func temporalBatch(kind Kind, precision *int32, nanosecond int) Batch {
	var data any = time.Date(2024, time.February, 29, 12, 34, 56, nanosecond, time.UTC)
	if kind == DateTime {
		data = DateTimeValue{DateValue: DateValue{Year: 2024, Month: time.February, Day: 29}, Hour: 12, Minute: 34, Second: 56, Nanosecond: nanosecond}
	}
	return Batch{
		Schema:  Schema{Fields: []Field{{Name: "time", Kind: kind, Nullable: true, TimePrecision: precision}}},
		Records: []Record{{"time": {Kind: kind, Present: true, Data: data}}},
	}
}

func TestTemporalPrecisionSchema(t *testing.T) {
	for _, kind := range []Kind{DateTime, Timestamp} {
		for _, p := range []*int32{nil, temporalPrecision(0), temporalPrecision(6), temporalPrecision(9)} {
			b := temporalBatch(kind, p, 0)
			b.Records = nil
			if err := b.Validate(); err != nil {
				t.Fatalf("%s precision %v: %v", kind, p, err)
			}
		}
		for _, p := range []int32{-1, 10} {
			b := temporalBatch(kind, temporalPrecision(p), 0)
			b.Records = nil
			if err := b.Validate(); err == nil || !strings.Contains(strings.ToLower(err.Error()), "precision") {
				t.Fatalf("%s precision %d: expected precision error, got %v", kind, p, err)
			}
		}
	}
	for _, kind := range []Kind{Bool, Int64, Uint64, Decimal, Float64, String, Binary, Date, UUID, List, Struct} {
		f := Field{Name: "value", Kind: kind, TimePrecision: temporalPrecision(6)}
		if kind == Decimal {
			f.Precision, f.Scale = 38, 18
		}
		if kind == List {
			f.Element = &Field{Kind: String}
		}
		if kind == Struct {
			f.Fields = []Field{{Name: "text", Kind: String}}
		}
		if err := (Batch{Schema: Schema{Fields: []Field{f}}}).Validate(); err == nil || !strings.Contains(strings.ToLower(err.Error()), "precision") {
			t.Fatalf("%s: expected precision error, got %v", kind, err)
		}
	}
}

func TestTemporalPrecisionAlignment(t *testing.T) {
	for _, kind := range []Kind{DateTime, Timestamp} {
		for _, tc := range []struct {
			name string
			p    *int32
			ns   int
			ok   bool
		}{
			{"unknown", nil, 123456789, true},
			{"whole_seconds", temporalPrecision(0), 0, true},
			{"whole_seconds_rejects_fraction", temporalPrecision(0), 1, false},
			{"microseconds", temporalPrecision(6), 123456000, true},
			{"microseconds_rejects_nanoseconds", temporalPrecision(6), 123456001, false},
			{"nanoseconds", temporalPrecision(9), 123456789, true},
		} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				b := temporalBatch(kind, tc.p, tc.ns)
				before := b.Records[0]["time"]
				if err := b.Validate(); (err == nil) != tc.ok {
					t.Fatalf("Validate() = %v, want success %t", err, tc.ok)
				}
				if b.Records[0]["time"] != before {
					t.Fatal("validation rounded or mutated input")
				}
				if !tc.ok {
					if _, err := Encode(b); err == nil {
						t.Fatal("encoder accepted misaligned value")
					}
				}
			})
		}
		for _, value := range []Value{NullValue(kind), MissingValue(kind)} {
			b := temporalBatch(kind, temporalPrecision(0), 0)
			b.Records[0]["time"] = value
			if err := b.Validate(); err != nil {
				t.Fatalf("%s null/missing rejected: %v", kind, err)
			}
		}
	}
}

func TestTemporalPrecisionNestedValidation(t *testing.T) {
	for _, kind := range []Kind{Struct, List} {
		child := Field{Name: "time", Kind: Timestamp, TimePrecision: temporalPrecision(6)}
		value := temporalBatch(Timestamp, nil, 123456001).Records[0]["time"]
		outer := Field{Name: "nested", Kind: kind}
		var data any
		if kind == Struct {
			outer.Fields, data = []Field{child}, Record{"time": value}
		} else {
			child.Name = ""
			outer.Element, data = &child, []Value{value}
		}
		b := Batch{Schema: Schema{Fields: []Field{outer}}, Records: []Record{{"nested": {Kind: kind, Present: true, Data: data}}}}
		if err := b.Validate(); err == nil {
			t.Fatalf("%s accepted nested misalignment", kind)
		}
		*child.TimePrecision = 10
		b.Records = nil
		if err := b.Validate(); err == nil {
			t.Fatalf("%s accepted invalid nested metadata without records", kind)
		}
	}
}

func TestTemporalPrecisionJSONAndCodec(t *testing.T) {
	for _, kind := range []Kind{DateTime, Timestamp} {
		for _, p := range []*int32{nil, temporalPrecision(0), temporalPrecision(6), temporalPrecision(9)} {
			b := temporalBatch(kind, p, 0)
			wire, err := Encode(b)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(wire, []byte(`"timePrecision"`)) != (p != nil) {
				t.Fatalf("incorrect omitted/known precision: %s", wire)
			}
			decoded, err := Decode(wire)
			if err != nil {
				t.Fatal(err)
			}
			got := decoded.Schema.Fields[0].TimePrecision
			if (got == nil) != (p == nil) || (p != nil && *got != *p) {
				t.Fatalf("codec lost precision: got %v, want %v", got, p)
			}
			if decoded.Records[0]["time"] != b.Records[0]["time"] {
				t.Fatal("codec changed temporal value")
			}
			encodedField, err := json.Marshal(b.Schema.Fields[0])
			if err != nil {
				t.Fatal(err)
			}
			if p != nil && !bytes.Contains(encodedField, []byte(fmt.Sprintf(`"timePrecision":%d`, *p))) {
				t.Fatalf("public JSON lost known precision: %s", encodedField)
			}
		}
	}
	for _, text := range []string{`{"name":"time","kind":"timestamp"}`, `{"name":"time","kind":"timestamp","timePrecision":null}`} {
		var f Field
		if err := json.Unmarshal([]byte(text), &f); err != nil || f.TimePrecision != nil {
			t.Fatalf("absent/null must decode as unknown: %s: %v", text, err)
		}
		encoded, err := json.Marshal(f)
		if err != nil || bytes.Contains(encoded, []byte(`"timePrecision"`)) {
			t.Fatalf("unknown must be omitted: %s: %v", encoded, err)
		}
	}

	b := temporalBatch(Timestamp, temporalPrecision(9), 123456789)
	wire, err := Encode(b)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []int32{6, -1, 10} {
		invalid := bytes.ReplaceAll(wire, []byte(`"timePrecision":9`), []byte(fmt.Sprintf(`"timePrecision":%d`, p)))
		if _, err := Decode(invalid); err == nil {
			t.Fatalf("decoder accepted misaligned or invalid precision %d", p)
		}
	}
}

func TestTemporalPrecisionCloneAndAppend(t *testing.T) {
	p := temporalPrecision(6)
	schema := Schema{Fields: []Field{
		{Name: "time", Kind: Timestamp, TimePrecision: p},
		{Name: "object", Kind: Struct, Fields: []Field{{Name: "civil", Kind: DateTime, TimePrecision: p}}},
		{Name: "list", Kind: List, Element: &Field{Kind: Timestamp, TimePrecision: p}},
	}}
	cloned := (Batch{Schema: schema}).Clone()
	for _, ptr := range []*int32{cloned.Schema.Fields[0].TimePrecision, cloned.Schema.Fields[1].Fields[0].TimePrecision, cloned.Schema.Fields[2].Element.TimePrecision} {
		if ptr == p || ptr == nil || *ptr != 6 {
			t.Fatal("clone lost or aliased temporal precision")
		}
		*ptr = 0
		if *p != 6 {
			t.Fatal("clone mutation changed shared input")
		}
	}

	left := temporalBatch(Timestamp, temporalPrecision(6), 1000)
	right := temporalBatch(Timestamp, temporalPrecision(6), 2000)
	joined, err := Append(left, right)
	if err != nil || len(joined.Records) != 2 {
		t.Fatalf("equal precision at distinct addresses must append: %v", err)
	}
	if joined.Records[0]["time"] != left.Records[0]["time"] || joined.Records[1]["time"] != right.Records[0]["time"] {
		t.Fatal("append reordered supplied edges")
	}
	*joined.Schema.Fields[0].TimePrecision = 9
	if *left.Schema.Fields[0].TimePrecision != 6 || *right.Schema.Fields[0].TimePrecision != 6 {
		t.Fatal("append aliased schema precision")
	}
	for _, pair := range [][2]*int32{{nil, temporalPrecision(0)}, {temporalPrecision(6), temporalPrecision(9)}} {
		if _, err := Append(temporalBatch(Timestamp, pair[0], 0), temporalBatch(Timestamp, pair[1], 0)); err == nil {
			t.Fatal("append accepted different precision metadata")
		}
	}
}

func TestTemporalPrecisionEstimatedBytes(t *testing.T) {
	unknown := temporalBatch(Timestamp, nil, 0)
	base := EstimatedBytes(unknown)
	var delta int64
	for _, p := range []int32{0, 6, 9} {
		known := temporalBatch(Timestamp, temporalPrecision(p), 0)
		n := EstimatedBytes(known) - base
		if n <= 0 || n > 128 || (delta != 0 && n != delta) {
			t.Fatalf("precision storage must have bounded constant cost: p=%d delta=%d", p, n)
		}
		delta = n
		if *known.Schema.Fields[0].TimePrecision != p {
			t.Fatal("size accounting mutated precision")
		}
	}
	nested := Batch{Schema: Schema{Fields: []Field{
		{Name: "object", Kind: Struct, Fields: []Field{{Name: "time", Kind: DateTime}}},
		{Name: "list", Kind: List, Element: &Field{Kind: Timestamp}},
	}}}
	before := EstimatedBytes(nested)
	nested.Schema.Fields[0].Fields[0].TimePrecision = temporalPrecision(6)
	nested.Schema.Fields[1].Element.TimePrecision = temporalPrecision(9)
	if got := EstimatedBytes(nested) - before; got != 2*delta {
		t.Fatalf("nested precision storage = %d, want %d", got, 2*delta)
	}
}

func TestTemporalPrecisionNestedCodecAndEquality(t *testing.T) {
	civil := temporalBatch(DateTime, temporalPrecision(6), 123456000)
	instant := temporalBatch(Timestamp, temporalPrecision(9), 123456789)
	element := instant.Schema.Fields[0]
	element.Name = ""
	b := Batch{
		Schema: Schema{Fields: []Field{
			{Name: "object", Kind: Struct, Fields: civil.Schema.Fields},
			{Name: "list", Kind: List, Element: &element},
		}},
		Records: []Record{{
			"object": {Kind: Struct, Present: true, Data: civil.Records[0]},
			"list":   {Kind: List, Present: true, Data: []Value{instant.Records[0]["time"]}},
		}},
	}
	wire, err := Encode(b)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decode(wire)
	if err != nil {
		t.Fatal(err)
	}
	again, err := Encode(out)
	if err != nil || !bytes.Equal(wire, again) {
		t.Fatalf("nested codec changed metadata or nanoseconds: %v", err)
	}
	if _, err := Append(b, out); err != nil {
		t.Fatalf("nested decoded metadata must be compatible: %v", err)
	}
	for _, field := range []*Field{&out.Schema.Fields[0].Fields[0], out.Schema.Fields[1].Element} {
		original := field.TimePrecision
		field.TimePrecision = nil
		if _, err := Append(b, out); err == nil {
			t.Fatal("append ignored a nested known/unknown difference")
		}
		field.TimePrecision = original
	}
}
