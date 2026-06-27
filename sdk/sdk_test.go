package sdk

import (
	"context"
	"testing"
)

// TestBatchIsAny verifies that sdk.Batch values are assignable to any.
// After the type alias change (Batch = any), this remains trivially true.
func TestBatchIsAny(t *testing.T) {
	var _ any = Batch("hello")
	var _ any = Batch(42)
	var _ any = Batch(nil)
}

// testSource implements SourceSPI for testing.
type testSource struct{}

func (s *testSource) Init(ctx context.Context, config []byte) error { return nil }
func (s *testSource) ReadBatch() (Batch, error)                     { return "test", nil }
func (s *testSource) Close() error                                  { return nil }

func TestSourceSPIInterface(t *testing.T) {
	var spi SourceSPI = &testSource{}
	batch, err := spi.ReadBatch()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batch != "test" {
		t.Fatalf("expected 'test', got %v", batch)
	}
}

// testSink implements SinkSPI for testing.
type testSink struct{}

func (s *testSink) Init(ctx context.Context, config []byte) error { return nil }
func (s *testSink) WriteBatch(batch Batch) error                  { return nil }
func (s *testSink) Close() error                                  { return nil }

func TestSinkSPIInterface(t *testing.T) {
	var spi SinkSPI = &testSink{}
	if err := spi.WriteBatch("data"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// testProcessor implements ProcessorSPI for testing.
type testProcessor struct{}

func (p *testProcessor) Init(ctx context.Context, config []byte) error { return nil }
func (p *testProcessor) Process(batch Batch) (Batch, error)            { return batch, nil }
func (p *testProcessor) Close() error                                  { return nil }

func TestProcessorSPIInterface(t *testing.T) {
	var spi ProcessorSPI = &testProcessor{}
	out, err := spi.Process("input")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "input" {
		t.Fatalf("expected 'input', got %v", out)
	}
}
