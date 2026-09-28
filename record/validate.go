package record

import (
	"fmt"
	"math"
	"time"
	"unicode/utf8"

	"github.com/shopspring/decimal"
)

// Limits bound validation, nested values and the typed external representation.
// A transport may impose a smaller frame limit.
const (
	MaxDepth         = 32
	MaxValues        = 100000
	MaxEncodedBytes  = 16 << 20
	MaxDecimalDigits = 4096
)

type budget struct{ values, bytes int }

func (b *budget) add(n int) error {
	b.values++
	if n > MaxEncodedBytes-b.bytes || b.values > MaxValues {
		return fmt.Errorf("record resource limit exceeded")
	}
	b.bytes += n
	return nil
}

func validKind(k Kind) bool {
	switch k {
	case Bool, Int64, Uint64, Decimal, Float64, String, Binary, Date, DateTime, Timestamp, UUID, List, Struct:
		return true
	}
	return false
}

// NewValue validates a concrete value without coercion. After publication,
// callers must not mutate its binary, list or record storage.
func NewValue(kind Kind, data any) (Value, error) {
	v := Value{Kind: kind, Present: true, Data: data}
	if err := checkValue(v, nil, 0, &budget{}); err != nil {
		return Value{}, err
	}
	return v, nil
}

// Validate checks a value without schema metadata. Use Batch.Validate to also
// enforce field nullability, decimal bounds and temporal precision alignment.
func (v Value) Validate() error { return checkValue(v, nil, 0, &budget{}) }

// Validate checks schema, presence and exact concrete types. Missing fields are
// permitted independently of nullability; target required/default policies are
// domain concerns. Precision=0, Scale=0 means unconstrained Decimal metadata.
// A positive precision enforces exact representability at the declared scale.
// A known TimePrecision requires nanosecond alignment without rounding.
func (b Batch) Validate() error { return validateBatch(b, &budget{}) }

func validateBatch(b Batch, limits *budget) error {
	if err := checkFields(b.Schema.Fields, 0, limits); err != nil {
		return fmt.Errorf("schema: %w", err)
	}
	if len(b.Records) > MaxValues {
		return fmt.Errorf("too many records")
	}
	for i, r := range b.Records {
		if err := checkRecord(r, b.Schema.Fields, 0, limits); err != nil {
			return fmt.Errorf("record %d: %w", i, err)
		}
	}
	return nil
}

func checkFields(fields []Field, depth int, limits *budget) error {
	seen := make(map[string]bool)
	for i := range fields {
		f := &fields[i]
		if f.Name == "" || !utf8.ValidString(f.Name) || seen[f.Name] {
			return fmt.Errorf("field names must be nonempty, unique UTF-8 strings")
		}
		seen[f.Name] = true
		if err := checkField(f, depth, limits); err != nil {
			return fmt.Errorf("field %q: %w", f.Name, err)
		}
	}
	return nil
}

func checkField(f *Field, depth int, limits *budget) error {
	if depth > MaxDepth {
		return fmt.Errorf("schema nesting exceeds %d", MaxDepth)
	}
	if err := limits.add(len(f.Name)); err != nil {
		return err
	}
	if !validKind(f.Kind) {
		return fmt.Errorf("unsupported kind %q", f.Kind)
	}
	if f.Kind != Struct && len(f.Fields) != 0 {
		return fmt.Errorf("fields require Struct")
	}
	if f.Kind != List && f.Element != nil {
		return fmt.Errorf("element requires List")
	}
	if f.Kind != Decimal && (f.Precision != 0 || f.Scale != 0) {
		return fmt.Errorf("precision/scale require Decimal")
	}
	if f.TimePrecision != nil {
		if f.Kind != DateTime && f.Kind != Timestamp {
			return fmt.Errorf("timePrecision requires DateTime or Timestamp")
		}
		if *f.TimePrecision < 0 || *f.TimePrecision > 9 {
			return fmt.Errorf("timePrecision must be in 0..9")
		}
	}
	switch f.Kind {
	case Decimal:
		if f.Precision < 0 || f.Precision > MaxDecimalDigits || f.Scale < -MaxDecimalDigits || f.Scale > MaxDecimalDigits {
			return fmt.Errorf("decimal metadata exceeds supported bounds")
		}
		if f.Precision == 0 && f.Scale != 0 {
			return fmt.Errorf("decimal scale requires a declared precision")
		}
	case List:
		if f.Element == nil {
			return fmt.Errorf("list requires an element field")
		}
		return checkField(f.Element, depth+1, limits)
	case Struct:
		return checkFields(f.Fields, depth+1, limits)
	}
	return nil
}

func checkRecord(r Record, fields []Field, depth int, limits *budget) error {
	if depth > MaxDepth {
		return fmt.Errorf("record nesting exceeds %d", MaxDepth)
	}
	if err := limits.add(0); err != nil {
		return err
	}
	matched := 0
	for i := range fields {
		f := &fields[i]
		if v, ok := r[f.Name]; ok {
			matched++
			if err := limits.add(len(f.Name)); err != nil {
				return err
			}
			if err := checkValue(v, f, depth, limits); err != nil {
				return fmt.Errorf("field %q: %w", f.Name, err)
			}
		}
	}
	if matched != len(r) {
		return fmt.Errorf("record contains a field outside its schema")
	}
	return nil
}

func checkValue(v Value, field *Field, depth int, limits *budget) error {
	if depth > MaxDepth {
		return fmt.Errorf("value nesting exceeds %d", MaxDepth)
	}
	if err := limits.add(0); err != nil {
		return err
	}
	if !validKind(v.Kind) {
		return fmt.Errorf("unsupported kind %q", v.Kind)
	}
	if field != nil && field.Kind != v.Kind {
		return fmt.Errorf("value kind %q does not match %q", v.Kind, field.Kind)
	}
	if !v.Present {
		if v.Null || v.Data != nil {
			return fmt.Errorf("missing value cannot be null or carry data")
		}
		return nil
	}
	if v.Null {
		if v.Data != nil {
			return fmt.Errorf("null value cannot carry data")
		}
		if field != nil && !field.Nullable {
			return fmt.Errorf("null is not allowed")
		}
		return nil
	}
	wrong := func() error { return fmt.Errorf("invalid concrete data type %T for %s", v.Data, v.Kind) }
	switch v.Kind {
	case Bool:
		if _, ok := v.Data.(bool); !ok {
			return wrong()
		}
	case Int64:
		if _, ok := v.Data.(int64); !ok {
			return wrong()
		}
	case Uint64:
		if _, ok := v.Data.(uint64); !ok {
			return wrong()
		}
	case Float64:
		n, ok := v.Data.(float64)
		if !ok {
			return wrong()
		}
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return fmt.Errorf("non-finite float is unsupported")
		}
	case Decimal:
		d, ok := v.Data.(decimal.Decimal)
		if !ok {
			return wrong()
		}
		coefficient := d.Coefficient()
		if d.Exponent() < -MaxDecimalDigits || d.Exponent() > MaxDecimalDigits || coefficient.BitLen() > MaxDecimalDigits*4 {
			return fmt.Errorf("decimal representation exceeds supported bounds")
		}
		digits := coefficient.String()
		count := len(digits)
		if digits[0] == '-' {
			count--
		}
		if count > MaxDecimalDigits {
			return fmt.Errorf("decimal coefficient is too large")
		}
		if err := limits.add(len(digits)); err != nil {
			return err
		}
		if field != nil && field.Precision > 0 {
			if !d.Round(field.Scale).Equal(d) {
				return fmt.Errorf("decimal is inexact at the declared scale")
			}
			if d.Abs().Cmp(decimal.New(1, field.Precision-field.Scale)) >= 0 {
				return fmt.Errorf("decimal exceeds the declared precision")
			}
		}
	case String:
		s, ok := v.Data.(string)
		if !ok {
			return wrong()
		}
		if !utf8.ValidString(s) {
			return fmt.Errorf("string is not valid UTF-8")
		}
		return limits.add(len(s))
	case Binary:
		data, ok := v.Data.([]byte)
		if !ok {
			return wrong()
		}
		return limits.add(len(data))
	case Date:
		d, ok := v.Data.(DateValue)
		if !ok {
			return wrong()
		}
		return checkDate(d)
	case DateTime:
		d, ok := v.Data.(DateTimeValue)
		if !ok {
			return wrong()
		}
		if err := checkDate(d.DateValue); err != nil {
			return err
		}
		if d.Hour < 0 || d.Hour > 23 || d.Minute < 0 || d.Minute > 59 || d.Second < 0 || d.Second > 59 || d.Nanosecond < 0 || d.Nanosecond >= 1000000000 {
			return fmt.Errorf("invalid civil time")
		}
		return checkTimePrecision(d.Nanosecond, field)
	case Timestamp:
		ts, ok := v.Data.(time.Time)
		if !ok {
			return wrong()
		}
		if ts.UTC().Year() < 1 || ts.UTC().Year() > 9999 {
			return fmt.Errorf("timestamp year is outside 1..9999")
		}
		return checkTimePrecision(ts.Nanosecond(), field)
	case UUID:
		if _, ok := v.Data.([16]byte); !ok {
			return wrong()
		}
	case List:
		items, ok := v.Data.([]Value)
		if !ok {
			return wrong()
		}
		var element *Field
		if field != nil {
			element = field.Element
		}
		for i, item := range items {
			if err := checkValue(item, element, depth+1, limits); err != nil {
				return fmt.Errorf("element %d: %w", i, err)
			}
		}
	case Struct:
		r, ok := v.Data.(Record)
		if !ok {
			return wrong()
		}
		if field != nil {
			return checkRecord(r, field.Fields, depth+1, limits)
		}
		for name, item := range r {
			if !utf8.ValidString(name) {
				return fmt.Errorf("invalid struct field name")
			}
			if err := limits.add(len(name)); err != nil {
				return err
			}
			if err := checkValue(item, nil, depth+1, limits); err != nil {
				return fmt.Errorf("field %q: %w", name, err)
			}
		}
	}
	return nil
}

func checkTimePrecision(nanosecond int, field *Field) error {
	if field == nil || field.TimePrecision == nil {
		return nil
	}
	// validateBatch checks the bound before any value reaches this helper.
	quantum := 1
	for p := *field.TimePrecision; p < 9; p++ {
		quantum *= 10
	}
	if nanosecond%quantum != 0 {
		return fmt.Errorf("temporal value is inexact at timePrecision %d", *field.TimePrecision)
	}
	return nil
}

func checkDate(d DateValue) error {
	if d.Year < 1 || d.Year > 9999 {
		return fmt.Errorf("date year is outside 1..9999")
	}
	t := time.Date(d.Year, d.Month, d.Day, 0, 0, 0, 0, time.UTC)
	if t.Year() != d.Year || t.Month() != d.Month || t.Day() != d.Day {
		return fmt.Errorf("invalid Gregorian date")
	}
	return nil
}
