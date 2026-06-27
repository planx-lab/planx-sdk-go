package runtime

import (
	"context"

	"github.com/planx-lab/planx-sdk-go/internal/flow"
)

// SPI interfaces — runtime-internal. The sdk package defines interfaces with
// identical method sets (sdk.SourceSPI etc.); a plugin value satisfies these
// via structural typing, so runtime never imports sdk (no cycle).

// SourceSPI is implemented by SOURCE components.
type SourceSPI interface {
	Init(ctx context.Context, config []byte) error
	ReadBatch() (any, error)
	Close() error
}

// ProcessorSPI is implemented by PROCESSOR components.
type ProcessorSPI interface {
	Init(ctx context.Context, config []byte) error
	Process(batch any) (any, error)
	Close() error
}

// SinkSPI is implemented by SINK components.
type SinkSPI interface {
	Init(ctx context.Context, config []byte) error
	WriteBatch(batch any) error
	Close() error
}

// liveSession is the runtime payload of one created session. Exactly one of
// source/processor/sink is non-nil, matching the bound component's kind.
// window is non-nil only for SOURCE sessions (stream flow control).
type liveSession struct {
	source    SourceSPI
	processor ProcessorSPI
	sink      SinkSPI
	window    *flow.Window
}

// close calls Close on whichever SPI owns this session. Idempotent-safe:
// only one SPI field is ever set.
func (s *liveSession) close() error {
	switch {
	case s.source != nil:
		return s.source.Close()
	case s.processor != nil:
		return s.processor.Close()
	case s.sink != nil:
		return s.sink.Close()
	}
	return nil
}
