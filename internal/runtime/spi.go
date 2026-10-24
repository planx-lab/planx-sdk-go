package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

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
	closing   atomic.Bool
	closeOnce sync.Once
	closeErr  error
	source    SourceSPI
	processor ProcessorSPI
	sink      SinkSPI
	window    *flow.Window
}

// close runs SPI.Close once. A failed or panicking close remains failed on
// repeated requests; closing sessions cannot accept new business operations.
func (s *liveSession) close() error {
	s.closing.Store(true)
	s.closeOnce.Do(func() {
		// The existing RPC interceptor reports panics. Keep a failure here too so
		// a later CloseSession cannot manufacture success after a panic.
		s.closeErr = errors.New("plugin cleanup did not complete")
		switch {
		case s.source != nil:
			s.closeErr = s.source.Close()
		case s.processor != nil:
			s.closeErr = s.processor.Close()
		case s.sink != nil:
			s.closeErr = s.sink.Close()
		default:
			s.closeErr = nil
		}
	})
	return s.closeErr
}
