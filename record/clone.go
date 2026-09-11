package record

import (
	"fmt"
	"slices"

	"github.com/shopspring/decimal"
)

// Clone gives a validated batch independent mutable schema and data storage.
// As with other data operations, callers must Validate untrusted input first.
func (b Batch) Clone() Batch {
	out := Batch{Schema: Schema{Fields: cloneFields(b.Schema.Fields)}}
	if b.Records != nil {
		out.Records = make([]Record, len(b.Records))
		for i, r := range b.Records {
			out.Records[i] = r.Clone()
		}
	}
	return out
}

func (r Record) Clone() Record {
	if r == nil {
		return nil
	}
	out := make(Record, len(r))
	for name, v := range r {
		out[name] = v.Clone()
	}
	return out
}

func (v Value) Clone() Value {
	switch data := v.Data.(type) {
	case []byte:
		v.Data = slices.Clone(data)
	case []Value:
		if data != nil {
			out := make([]Value, len(data))
			for i, item := range data {
				out[i] = item.Clone()
			}
			v.Data = out
		}
	case Record:
		v.Data = data.Clone()
	case decimal.Decimal:
		v.Data = decimal.NewFromBigInt(data.Coefficient(), data.Exponent())
	}
	return v
}

func cloneFields(fields []Field) []Field {
	if fields == nil {
		return nil
	}
	out := slices.Clone(fields)
	for i := range out {
		if fields[i].TimePrecision != nil {
			precision := *fields[i].TimePrecision
			out[i].TimePrecision = &precision
		}
		out[i].Fields = cloneFields(fields[i].Fields)
		if fields[i].Element != nil {
			item := cloneFields([]Field{*fields[i].Element})
			out[i].Element = &item[0]
		}
	}
	return out
}

// Append concatenates compatible batches in supplied edge order. It neither
// sorts nor zips records, and its output owns all mutable storage.
func Append(batches ...Batch) (Batch, error) {
	if len(batches) == 0 {
		return Batch{}, fmt.Errorf("Append requires at least one batch with a schema")
	}
	limits := &budget{}
	for i, b := range batches {
		if err := validateBatch(b, limits); err != nil {
			return Batch{}, fmt.Errorf("batch %d: %w", i, err)
		}
		if !sameFields(batches[0].Schema.Fields, b.Schema.Fields) {
			return Batch{}, fmt.Errorf("batch %d has an incompatible schema", i)
		}
	}
	out := Batch{Schema: Schema{Fields: cloneFields(batches[0].Schema.Fields)}}
	for _, b := range batches {
		for _, r := range b.Records {
			out.Records = append(out.Records, r.Clone())
		}
	}
	return out, nil
}

func (b Batch) Append(other Batch) (Batch, error) { return Append(b, other) }

func sameFields(a, b []Field) bool {
	if len(a) != len(b) {
		return false
	}
	for i, x := range a {
		y := b[i]
		if x.Name != y.Name || x.Kind != y.Kind || x.Nullable != y.Nullable || x.Precision != y.Precision || x.Scale != y.Scale || !sameFields(x.Fields, y.Fields) {
			return false
		}
		if (x.TimePrecision == nil) != (y.TimePrecision == nil) {
			return false
		}
		if x.TimePrecision != nil && *x.TimePrecision != *y.TimePrecision {
			return false
		}
		if (x.Element == nil) != (y.Element == nil) {
			return false
		}
		if x.Element != nil && !sameFields([]Field{*x.Element}, []Field{*y.Element}) {
			return false
		}
	}
	return true
}
