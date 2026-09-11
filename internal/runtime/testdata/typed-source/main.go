// This executable is an external-process test fixture, not a connector.
package main

import (
	"context"
	"io"

	"github.com/planx-lab/planx-sdk-go/record"
	"github.com/planx-lab/planx-sdk-go/sdk"
)

type source struct {
	batch record.Batch
	next  int
}

func (s *source) Init(_ context.Context, config []byte) error {
	batch, err := record.Decode(config)
	s.batch = batch
	s.next = 0
	return err
}

func (s *source) ReadBatch() (sdk.Batch, error) {
	s.next++
	switch s.next {
	case 1:
		return s.batch.Clone(), nil
	case 2:
		return record.Batch{Schema: s.batch.Clone().Schema, Records: []record.Record{}}, nil
	default:
		return nil, io.EOF
	}
}

func (s *source) Close() error { return nil }

func main() {
	sdk.Serve(sdk.Plugin{
		ID: "typed-source-fixture", Version: "1.0.0",
		Components: []sdk.ComponentSpec{{
			ID: "source", Kind: sdk.KindSource,
			Source: func() sdk.SourceSPI { return &source{} },
		}},
	})
}
