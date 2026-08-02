package sdk

import (
	"fmt"
	"strconv"
)

// Row is the canonical in-process batch row: a field-name → value map.
// Every source emits []Row (via ToRows at the source boundary); every
// processor reads and writes []Row; every sink receives []Row (converting at
// the sink boundary). This single canonical type makes processor chains
// unbreakable by construction — there is only one type, so no processor can
// emit a type a downstream consumer doesn't expect.
//
// Industry analog: Logstash Event, NiFi Record, Kafka Connect record value.
type Row = map[string]any

// Rows is a batch of Row.
type Rows = []Row

// ToRows normalizes any source-emitted batch into Rows. Sources call this in
// ReadBatch so downstream processors/sinks always receive Rows.
//
// Supported input shapes:
//   - Rows ([]map[string]any): identity.
//   - Row (map[string]any): wrapped into a single-element Rows.
//   - [][]string: CSV grid where row 0 is the header; each subsequent row is
//     mapped to a Row keyed by the header names.
func ToRows(b any) (Rows, error) {
	switch v := b.(type) {
	case nil:
		return nil, fmt.Errorf("batch is nil")
	case Rows:
		return v, nil
	case Row:
		return Rows{v}, nil
	case [][]string:
		if len(v) == 0 {
			return Rows{}, nil
		}
		header := v[0]
		out := make(Rows, 0, len(v)-1)
		for _, row := range v[1:] {
			m := make(Row, len(header))
			for i, col := range header {
				if i < len(row) {
					m[col] = row[i]
				}
			}
			out = append(out, m)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported batch type %T", b)
	}
}

// FromRowsToStringGrid converts Rows back into a [][]string CSV-style grid
// (header row first, then one stringified row each). Sinks that need a column
// grid (e.g. CSV sink) call this at the sink boundary. Missing fields become
// empty strings; non-string values are stringified via fmt.
func FromRowsToStringGrid(rows Rows, columns []string) ([][]string, error) {
	grid := make([][]string, 0, len(rows)+1)
	grid = append(grid, columns)
	for _, row := range rows {
		rec := make([]string, len(columns))
		for i, col := range columns {
			rec[i] = stringify(row[col])
		}
		grid = append(grid, rec)
	}
	return grid, nil
}

func stringify(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(val)
	default:
		return fmt.Sprintf("%v", v)
	}
}
