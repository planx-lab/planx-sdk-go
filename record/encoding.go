package record

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/shopspring/decimal"
)

// WirePrefix identifies this encoding family; the header versions its contract.
// JSON is used only at external boundaries. Integers use decimal strings and
// Decimal uses its exact coefficient/exponent, never a floating point number.
const WirePrefix = "planx-record/"
const wireHeader = WirePrefix + "1\n"
const localLayout = "2006-01-02T15:04:05.999999999"

type wireValue struct {
	Kind    Kind            `json:"kind"`
	Present bool            `json:"present"`
	Null    bool            `json:"null"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type wireDecimal struct {
	Coefficient string `json:"coefficient"`
	Exponent    int32  `json:"exponent"`
}

// Encode validates and serializes a batch. Runtime identity is not encoded.
func Encode(b Batch) ([]byte, error) {
	if err := b.Validate(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(b)
	if err != nil {
		return nil, err
	}
	if len(data) > MaxEncodedBytes-len(wireHeader) {
		return nil, fmt.Errorf("typed batch exceeds encoded byte limit")
	}
	return append([]byte(wireHeader), data...), nil
}

// Decode rejects unsupported versions, excessive bytes/depth, duplicate keys,
// unknown fields and invalid data. Each call returns independently owned data.
func Decode(data []byte) (Batch, error) {
	if len(data) > MaxEncodedBytes {
		return Batch{}, fmt.Errorf("typed batch exceeds encoded byte limit")
	}
	if !bytes.HasPrefix(data, []byte(wireHeader)) {
		return Batch{}, fmt.Errorf("unsupported typed batch encoding/version")
	}
	payload := data[len(wireHeader):]
	if err := checkJSON(payload); err != nil {
		return Batch{}, err
	}
	if len(bytes.TrimSpace(payload)) == 0 || bytes.TrimSpace(payload)[0] != '{' {
		return Batch{}, fmt.Errorf("typed batch must be an object")
	}
	var b Batch
	if err := readJSON(payload, &b); err != nil {
		return Batch{}, fmt.Errorf("typed batch: %w", err)
	}
	if err := b.Validate(); err != nil {
		return Batch{}, err
	}
	return b, nil
}

func (v Value) MarshalJSON() ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	w := wireValue{Kind: v.Kind, Present: v.Present, Null: v.Null}
	if !v.Present || v.Null {
		return json.Marshal(w)
	}
	data := v.Data
	switch v.Kind {
	case Int64:
		data = strconv.FormatInt(v.Data.(int64), 10)
	case Uint64:
		data = strconv.FormatUint(v.Data.(uint64), 10)
	case Decimal:
		d := v.Data.(decimal.Decimal)
		data = wireDecimal{Coefficient: d.Coefficient().String(), Exponent: d.Exponent()}
	case Binary:
		data = base64.StdEncoding.EncodeToString(v.Data.([]byte))
	case UUID:
		id := v.Data.([16]byte)
		data = hex.EncodeToString(id[:])
	case Date:
		d := v.Data.(DateValue)
		data = fmt.Sprintf("%04d-%02d-%02d", d.Year, d.Month, d.Day)
	case DateTime:
		d := v.Data.(DateTimeValue)
		data = time.Date(d.Year, d.Month, d.Day, d.Hour, d.Minute, d.Second, d.Nanosecond, time.UTC).Format(localLayout)
	case Timestamp:
		data = v.Data.(time.Time).UTC().Format(time.RFC3339Nano)
	case List:
		if v.Data.([]Value) == nil {
			data = []Value{}
		}
	case Struct:
		if v.Data.(Record) == nil {
			data = Record{}
		}
	}
	encoded, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	w.Data = encoded
	return json.Marshal(w)
}

func (v *Value) UnmarshalJSON(data []byte) error {
	var w wireValue
	if err := readJSON(data, &w); err != nil {
		return err
	}
	out := Value{Kind: w.Kind, Present: w.Present, Null: w.Null}
	if !w.Present || w.Null {
		if len(w.Data) != 0 && !bytes.Equal(bytes.TrimSpace(w.Data), []byte("null")) {
			return fmt.Errorf("missing/null value carries data")
		}
	} else {
		if len(w.Data) == 0 || bytes.Equal(bytes.TrimSpace(w.Data), []byte("null")) {
			return fmt.Errorf("present value requires non-null data")
		}
		decoded, err := decodeData(w.Kind, w.Data)
		if err != nil {
			return fmt.Errorf("%s data: %w", w.Kind, err)
		}
		out.Data = decoded
	}
	if err := out.Validate(); err != nil {
		return err
	}
	*v = out
	return nil
}

func decodeData(kind Kind, data []byte) (any, error) {
	var text string
	switch kind {
	case Int64, Uint64, String, Binary, Date, DateTime, Timestamp, UUID:
		if err := readJSON(data, &text); err != nil {
			return nil, err
		}
	}
	switch kind {
	case Bool:
		var v bool
		err := readJSON(data, &v)
		return v, err
	case Int64:
		return strconv.ParseInt(text, 10, 64)
	case Uint64:
		return strconv.ParseUint(text, 10, 64)
	case Float64:
		var v float64
		err := readJSON(data, &v)
		return v, err
	case String:
		return text, nil
	case Binary:
		return base64.StdEncoding.DecodeString(text)
	case Decimal:
		var d wireDecimal
		if err := readJSON(data, &d); err != nil {
			return nil, err
		}
		if len(d.Coefficient) > MaxDecimalDigits+1 {
			return nil, fmt.Errorf("decimal coefficient exceeds limit")
		}
		coefficient, ok := new(big.Int).SetString(d.Coefficient, 10)
		if !ok {
			return nil, fmt.Errorf("invalid decimal coefficient")
		}
		return decimal.NewFromBigInt(coefficient, d.Exponent), nil
	case UUID:
		var id [16]byte
		decoded, err := hex.DecodeString(text)
		if err != nil || len(decoded) != len(id) {
			return nil, fmt.Errorf("UUID must contain 32 hexadecimal digits")
		}
		copy(id[:], decoded)
		return id, nil
	case Date, DateTime:
		layout := "2006-01-02"
		if kind == DateTime {
			layout = localLayout
		}
		t, err := time.Parse(layout, text)
		if err != nil || t.Format(layout) != text {
			return nil, fmt.Errorf("invalid canonical civil date/time")
		}
		d := DateValue{Year: t.Year(), Month: t.Month(), Day: t.Day()}
		if kind == Date {
			return d, nil
		}
		return DateTimeValue{DateValue: d, Hour: t.Hour(), Minute: t.Minute(), Second: t.Second(), Nanosecond: t.Nanosecond()}, nil
	case Timestamp:
		t, err := time.Parse(time.RFC3339Nano, text)
		return t.UTC(), err
	case List:
		var v []Value
		err := readJSON(data, &v)
		return v, err
	case Struct:
		var v Record
		err := readJSON(data, &v)
		return v, err
	}
	return nil, fmt.Errorf("unsupported kind %q", kind)
}

func readJSON(data []byte, into any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(into); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("unexpected trailing JSON data")
	}
	return nil
}

// Preflight before allocating the typed graph. Token decoding retains numeric
// literals and rejects repeated object keys instead of silently overwriting.
func checkJSON(data []byte) error {
	if !utf8.Valid(data) {
		return fmt.Errorf("wire data is not UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	tokens := 0
	var walk func(int) error
	walk = func(depth int) error {
		tokens++
		if depth > 4*MaxDepth+8 || tokens > 20*MaxValues {
			return fmt.Errorf("wire structure exceeds limit")
		}
		tok, err := d.Token()
		if err != nil {
			return err
		}
		delim, ok := tok.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			keys := make(map[string]bool)
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok || keys[name] {
					return fmt.Errorf("duplicate or invalid JSON object key")
				}
				keys[name] = true
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unexpected JSON delimiter")
		}
		_, err = d.Token()
		return err
	}
	if err := walk(0); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("unexpected trailing JSON data")
	}
	return nil
}
