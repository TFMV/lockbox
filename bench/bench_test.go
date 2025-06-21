package bench

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	lb "github.com/TFMV/lockbox/pkg/lockbox"
)

var schema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
}, nil)

func largeRecord(rows int) arrow.Record {
	mem := memory.NewGoAllocator()
	idb := array.NewInt64Builder(mem)
	nameb := array.NewStringBuilder(mem)
	for i := 0; i < rows; i++ {
		idb.Append(int64(i))
		nameb.Append(fmt.Sprintf("user_%d", i))
	}
	idArr := idb.NewArray()
	nameArr := nameb.NewArray()
	idb.Release()
	nameb.Release()
	rec := array.NewRecord(schema, []arrow.Array{idArr, nameArr}, int64(rows))
	idArr.Release()
	nameArr.Release()
	return rec
}

// Benchmark writing a large record to a new lockbox each iteration.
func BenchmarkWriteLarge(b *testing.B) {
	rows := 100000
	for i := 0; i < b.N; i++ {
		tmp := filepath.Join(os.TempDir(), fmt.Sprintf("bench_write_%d.lbx", i))
		lbx, err := lb.Create(tmp, schema, lb.WithPassword("bench"))
		if err != nil {
			b.Fatalf("create: %v", err)
		}
		record := largeRecord(rows)
		if err := lbx.Write(context.Background(), record, lb.WithPassword("bench")); err != nil {
			b.Fatalf("write: %v", err)
		}
		lbx.Close()
		os.Remove(tmp)
	}
}

// Benchmark reading a large record from an existing lockbox.
func BenchmarkReadLarge(b *testing.B) {
	rows := 100000
	tmp := filepath.Join(os.TempDir(), "bench_read.lbx")
	lbx, err := lb.Create(tmp, schema, lb.WithPassword("bench"))
	if err != nil {
		b.Fatalf("create: %v", err)
	}
	record := largeRecord(rows)
	if err := lbx.Write(context.Background(), record, lb.WithPassword("bench")); err != nil {
		b.Fatalf("write: %v", err)
	}
	lbx.Close()

	lbx, err = lb.Open(tmp, lb.WithPassword("bench"))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer func() {
		lbx.Close()
		os.Remove(tmp)
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec, err := lbx.Read(context.Background(), lb.WithPassword("bench"))
		if err != nil {
			b.Fatalf("read: %v", err)
		}
		rec.Release()
	}
}
