package batch

import (
	"bytes"
	"encoding/gob"
)

func init() {
	// Register every standard batch shape a Planx plugin may emit so the codec
	// is bytes-opaque across process boundaries: a Source (one OS process)
	// emits one of these types, and a Sink/Processor (a DIFFERENT process with
	// its own gob type registry) can decode it WITHOUT calling RegisterType.
	//
	// Before this, each plugin had to pre-register every other plugin's batch
	// type (e.g. sink-stdout mirrored postgres's DBBatch; CSV's [][]string
	// wasn't registered anywhere and crashed any non-CSV sink). Centralizing
	// the standard type universe in the SDK removes that coupling.
	//
	// Plugins with CUSTOM (non-standard) batch types (e.g. DBBatch under a
	// shared gob wire name) still call RegisterType / gob.RegisterName for
	// their domain types — that remains a deliberate cross-connector contract.
	gob.Register(map[string]string{})
	gob.Register([]map[string]any{})
	gob.Register(map[string]any{})
	gob.Register([][]string{})
	gob.Register([]map[string]string{})
	gob.Register([]string{})
}

// RegisterType registers a custom type with the gob codec so it can be
// serialized in plugin batch data. Plugins that return custom types from
// ReadBatch/Process must call this before serving.
//
// Standard batch shapes ([][]string, []map[string]any, map[string]any,
// map[string]string, []map[string]string, []string) are registered
// automatically by the SDK — most plugins do NOT need to call this. It is only
// needed for genuinely custom domain types (e.g. a connector's DBBatch).
func RegisterType(value any) {
	gob.Register(value)
}

type batchWrapper struct {
	Batch any
}

type Codec interface {
	Pack(batch any) (PackedBatch, error)
	Unpack(p PackedBatch) (any, error)
}

type gobCodec struct{}

func NewCodec() Codec {
	return &gobCodec{}
}

func (c *gobCodec) Pack(b any) (PackedBatch, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(batchWrapper{Batch: b})
	return buf.Bytes(), err
}

func (c *gobCodec) Unpack(p PackedBatch) (any, error) {
	var w batchWrapper
	err := gob.NewDecoder(bytes.NewReader(p)).Decode(&w)
	return w.Batch, err
}
