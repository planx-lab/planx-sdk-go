package sdk

import "github.com/planx-lab/planx-sdk-go/internal/batch"

type Batch = any

// RegisterType registers a custom batch data type for gob serialization.
// Plugins that return custom types from ReadBatch/Process must call this
// before ServeSource/ServeProcessor/ServeSink.
func RegisterType(value any) {
	batch.RegisterType(value)
}
