package lockbox

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// helper to create a parquet file with given record
func writeParquet(path string, rec arrow.Record) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w, err := pqarrow.NewFileWriter(rec.Schema(), f, nil, pqarrow.ArrowWriterProperties{})
	if err != nil {
		return err
	}
	if err := w.Write(rec); err != nil {
		return err
	}
	return w.Close()
}

func TestIngestParquet(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	mem := memory.NewGoAllocator()
	idb := array.NewInt64Builder(mem)
	nameb := array.NewStringBuilder(mem)
	idb.AppendValues([]int64{1, 2}, nil)
	nameb.AppendValues([]string{"a", "b"}, nil)
	rec := array.NewRecord(schema, []arrow.Array{idb.NewArray(), nameb.NewArray()}, 2)
	idb.Release()
	nameb.Release()

	tmpParquet := "/tmp/test_ingest.parquet"
	if err := writeParquet(tmpParquet, rec); err != nil {
		t.Fatalf("write parquet: %v", err)
	}
	defer os.Remove(tmpParquet)

	tmpFile := "/tmp/test_ingest.lbx"
	defer os.Remove(tmpFile)
	lb, err := Create(tmpFile, schema, WithPassword("pass"), WithCreatedBy("t"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer lb.Close()

	if err := lb.IngestParquet(context.Background(), tmpParquet, WithPassword("pass")); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	recOut, err := lb.Read(context.Background(), WithPassword("pass"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if recOut.NumRows() != 2 {
		t.Fatalf("rows: %d", recOut.NumRows())
	}
	recOut.Release()
}
