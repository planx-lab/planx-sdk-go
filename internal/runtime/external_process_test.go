//go:build integration

package runtime_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"github.com/planx-lab/planx-sdk-go/record"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Run with -tags=integration in the multi-repository workspace. Missing sibling
// repositories, failed builds, and unavailable sockets are failures, not skips.
func TestExternalProcessesTypedChain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	t.Cleanup(cancel)
	t.Setenv("PLANX_SDK_METRICS_ADDR", "")
	_, file, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatal("cannot locate SDK workspace")
	}
	workspace := filepath.Clean(filepath.Join(filepath.Dir(file), "../../.."))
	dir, err := os.MkdirTemp("/tmp", "planx-typed-external-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	specs := []struct {
		module, target, id string
		components         int
	}{
		{"planx-sdk-go", "./internal/runtime/testdata/typed-source", "typed-source-fixture", 1},
		{"planx-plugin-template-go", "./cmd/plugin", "template", 3},
		{"planx-plugin-processor-passthrough", "./cmd/plugin", "processor-passthrough", 1},
		{"planx-plugin-sink-stdout", "./cmd/plugin", "sink-stdout", 1},
	}
	processes := make(map[string]externalProcess, len(specs))
	for _, spec := range specs {
		binary := filepath.Join(dir, spec.id)
		build := exec.CommandContext(ctx, "go", "build", "-mod=readonly", "-o", binary, spec.target)
		build.Dir = filepath.Join(workspace, spec.module)
		t.Logf("COMMAND: cd %s && %s", build.Dir, strings.Join(build.Args, " "))
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", spec.module, err, out)
		}
		t.Logf("EXIT build %s: 0", spec.module)
		process := startExternalProcess(t, ctx, binary, dir, spec.id)
		desc, err := pb.NewPluginServiceClient(process.conn).Discover(ctx, &pb.Empty{}, grpc.WaitForReady(true))
		if err != nil {
			t.Fatalf("Discover %s: %v", spec.id, err)
		}
		if desc.GetId() != spec.id || len(desc.GetComponents()) != spec.components {
			t.Fatalf("unexpected descriptor for %s: %v", spec.id, desc)
		}
		t.Logf("Discover %s: %d components", spec.id, len(desc.GetComponents()))
		processes[spec.id] = process
	}
	template := processes["template"]
	// The real template deliberately leaves source business logic as an EOF
	// skeleton. Verify it, then use the separate SDK fixture as the typed source.
	templateSource := externalSession(t, ctx, template.conn, "source", nil)
	empty, err := pb.NewSourceServiceClient(template.conn).OpenStream(ctx, &pb.StreamOpenRequest{
		SessionId: templateSource, InitialWindow: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := empty.Recv(); err != io.EOF {
		t.Fatalf("real template source must remain EOF-only: %v", err)
	}
	t.Log("actual template source: EOF skeleton verified")

	want := externalTypedFixture()
	config, err := record.Encode(want)
	if err != nil {
		t.Fatal(err)
	}
	source := processes["typed-source-fixture"]
	sourceSession := externalSession(t, ctx, source.conn, "source", config)
	processorSession := externalSession(t, ctx, template.conn, "processor", nil)
	templateSinkSession := externalSession(t, ctx, template.conn, "sink", nil)
	passthrough := processes["processor-passthrough"]
	passthroughSession := externalSession(t, ctx, passthrough.conn, "processor", nil)
	stdout := processes["sink-stdout"]
	stdoutSession := externalSession(t, ctx, stdout.conn, "sink", nil)
	stream, err := pb.NewSourceServiceClient(source.conn).OpenStream(ctx, &pb.StreamOpenRequest{
		SessionId: sourceSession, InitialWindow: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	lineage := []byte("runtime-owned-lineage-not-record-data")
	for i := 0; i < 2; i++ {
		batch, err := stream.Recv()
		if err != nil {
			t.Fatalf("typed source batch %d: %v", i, err)
		}
		expected := want
		if i == 1 {
			expected = record.Batch{Schema: want.Schema, Records: []record.Record{}}
		}
		assertExternalBatch(t, batch, expected)
		batch.Lineage = append([]byte(nil), lineage...)
		for _, processor := range []struct {
			conn *grpc.ClientConn
			id   string
		}{{template.conn, processorSession}, {passthrough.conn, passthroughSession}} {
			batch, err = pb.NewProcessorServiceClient(processor.conn).Process(
				metadata.AppendToOutgoingContext(ctx, "x-planx-session-id", processor.id), batch)
			if err != nil {
				t.Fatalf("Process batch %d: %v", i, err)
			}
			assertExternalBatch(t, batch, expected)
			if !bytes.Equal(batch.GetLineage(), lineage) {
				t.Fatal("processor discarded runtime lineage")
			}
		}
		for _, sink := range []struct {
			process externalProcess
			id      string
		}{{template, templateSinkSession}, {stdout, stdoutSession}} {
			if _, err := pb.NewSinkServiceClient(sink.process.conn).WriteBatch(
				metadata.AppendToOutgoingContext(ctx, "x-planx-session-id", sink.id), batch); err != nil {
				t.Fatalf("WriteBatch %d: %v", i, err)
			}
			line := externalOutput(t, ctx, sink.process.lines)
			if !strings.Contains(line, "[SINK]") {
				t.Fatalf("missing actual sink output: %q", line)
			}
			if i == 0 {
				for _, exact := range []string{
					"123456789012345678901234567890123456.78",
					want.Records[0]["fraction"].Data.(decimal.Decimal).String(),
					"-9223372036854775808", "18446744073709551615",
				} {
					if !strings.Contains(line, exact) {
						t.Fatalf("sink rounded or omitted exact value %q: %s", exact, line)
					}
				}
				instant := want.Records[0]["timestamp_microseconds"].Data.(time.Time).UTC()
				if !strings.Contains(line, "timestamp_microseconds") ||
					(!strings.Contains(line, instant.Format(time.RFC3339Nano)) && !strings.Contains(line, instant.String())) {
					t.Fatalf("sink lost exact p6 timestamp: %s", line)
				}
				t.Logf("external sink: exact p6 timestamp_microseconds=%s verified", instant.Format(time.RFC3339Nano))
			}
		}
		if _, err := pb.NewSessionServiceClient(source.conn).Ack(ctx, &pb.AckRequest{
			SessionId: sourceSession, Processed: 1, NewWindow: 1,
		}); err != nil {
			t.Fatalf("source credit ACK: %v", err)
		}
		t.Logf("typed batch %d: %d records, two processors, two sinks, exact values and lineage preserved", i, len(expected.Records))
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("typed source exhaustion: %v", err)
	}
}

type externalProcess struct {
	conn  *grpc.ClientConn
	lines <-chan string
}

func startExternalProcess(t *testing.T, ctx context.Context, binary, dir, id string) externalProcess {
	t.Helper()
	cwd := filepath.Join(dir, id+"-cwd")
	if err := os.Mkdir(cwd, 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, binary)
	cmd.Dir = cwd
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	lines := make(chan string, 16)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), record.MaxEncodedBytes)
		for scanner.Scan() {
			select {
			case lines <- scanner.Text():
			case <-ctx.Done():
				return
			}
		}
	}()
	first := externalOutput(t, ctx, lines)
	var hs struct {
		Protocol string
		Address  string
	}
	if err := json.Unmarshal([]byte(first), &hs); err != nil || hs.Protocol != "v4" {
		t.Fatalf("%s first stdout line is not a v4 handshake: %q (%v)", id, first, err)
	}
	host, port, err := net.SplitHostPort(hs.Address)
	if err != nil || host != "127.0.0.1" || port == "" || port == "0" {
		t.Fatalf("invalid loopback handshake: %s", first)
	}
	persisted, err := os.ReadFile(filepath.Join(cwd, "planx.handshake"))
	if err != nil || !bytes.Equal(persisted, []byte(first)) {
		t.Fatalf("%s persisted handshake mismatch: %v", id, err)
	}
	t.Logf("HANDSHAKE %s: %s", id, first)
	conn, err := grpc.NewClient(hs.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return externalProcess{conn: conn, lines: lines}
}

func externalOutput(t *testing.T, ctx context.Context, lines <-chan string) string {
	t.Helper()
	select {
	case line, ok := <-lines:
		if !ok {
			t.Fatal("external process stdout closed unexpectedly")
		}
		return line
	case <-ctx.Done():
		t.Fatalf("external process output: %v", ctx.Err())
		return ""
	}
}

func externalSession(t *testing.T, ctx context.Context, conn *grpc.ClientConn, component string, config []byte) string {
	t.Helper()
	client := pb.NewSessionServiceClient(conn)
	created, err := client.CreateSession(ctx, &pb.SessionCreateRequest{ComponentId: component, Config: config})
	if err != nil || created.GetSessionId() == "" {
		t.Fatalf("CreateSession %s: %v", component, err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := client.CloseSession(closeCtx, &pb.SessionCloseRequest{SessionId: created.GetSessionId()}); err != nil {
			t.Errorf("CloseSession %s: %v", component, err)
		}
	})
	return created.GetSessionId()
}

func assertExternalBatch(t *testing.T, wire *pb.Batch, want record.Batch) {
	t.Helper()
	got, err := record.Decode(wire.GetPayload())
	if err != nil {
		t.Fatalf("canonical external Decode: %v", err)
	}
	assertExternalMicroseconds(t, got)
	t.Logf("external p6: timePrecision=6 preserved with %d records; nonempty rows retain 2024-02-29T04:00:00.123456Z", len(got.Records))
	gotWire, err := record.Encode(got)
	if err != nil {
		t.Fatal(err)
	}
	wantWire, err := record.Encode(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotWire, wantWire) {
		t.Fatalf("typed batch changed\n got %s\nwant %s", gotWire, wantWire)
	}
}

func externalTypedFixture() record.Batch {
	p6 := int32(6)
	return record.Batch{Schema: record.Schema{Fields: []record.Field{
		{Name: "amount", Kind: record.Decimal, Precision: 38, Scale: 2},
		{Name: "fraction", Kind: record.Decimal},
		{Name: "signed", Kind: record.Int64},
		{Name: "unsigned", Kind: record.Uint64},
		{Name: "missing", Kind: record.String, Nullable: true},
		{Name: "null", Kind: record.String, Nullable: true},
		{Name: "empty", Kind: record.String},
		{Name: "binary", Kind: record.Binary},
		{Name: "float", Kind: record.Float64},
		{Name: "date", Kind: record.Date},
		{Name: "datetime", Kind: record.DateTime},
		{Name: "timestamp", Kind: record.Timestamp},
		{Name: "timestamp_microseconds", Kind: record.Timestamp, TimePrecision: &p6},
		{Name: "uuid", Kind: record.UUID},
		{Name: "list", Kind: record.List, Element: &record.Field{Kind: record.Binary}},
		{Name: "object", Kind: record.Struct, Fields: []record.Field{{Name: "flag", Kind: record.Bool}}},
	}}, Records: []record.Record{{
		"amount":                 {Kind: record.Decimal, Present: true, Data: decimal.RequireFromString("123456789012345678901234567890123456.78")},
		"fraction":               {Kind: record.Decimal, Present: true, Data: decimal.RequireFromString("0." + strings.Repeat("1234567890", 32))},
		"signed":                 {Kind: record.Int64, Present: true, Data: int64(math.MinInt64)},
		"unsigned":               {Kind: record.Uint64, Present: true, Data: uint64(math.MaxUint64)},
		"missing":                record.MissingValue(record.String),
		"null":                   record.NullValue(record.String),
		"empty":                  {Kind: record.String, Present: true, Data: ""},
		"binary":                 {Kind: record.Binary, Present: true, Data: []byte{0, 255, 1}},
		"float":                  {Kind: record.Float64, Present: true, Data: 1.0000000000000002},
		"date":                   {Kind: record.Date, Present: true, Data: record.DateValue{Year: 2024, Month: 2, Day: 29}},
		"datetime":               {Kind: record.DateTime, Present: true, Data: record.DateTimeValue{DateValue: record.DateValue{Year: 2024, Month: 2, Day: 29}, Hour: 12, Nanosecond: 123456789}},
		"timestamp":              {Kind: record.Timestamp, Present: true, Data: time.Date(2024, 2, 29, 12, 0, 0, 123456789, time.FixedZone("UTC+8", 8*3600))},
		"timestamp_microseconds": {Kind: record.Timestamp, Present: true, Data: time.Date(2024, 2, 29, 12, 0, 0, 123456000, time.FixedZone("UTC+8", 8*3600))},
		"uuid":                   {Kind: record.UUID, Present: true, Data: [16]byte{1, 2, 3, 4}},
		"list":                   {Kind: record.List, Present: true, Data: []record.Value{{Kind: record.Binary, Present: true, Data: []byte{2, 3}}}},
		"object":                 {Kind: record.Struct, Present: true, Data: record.Record{"flag": {Kind: record.Bool, Present: true, Data: false}}},
	}}}
}
