package lockbox

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestCreateAndWrite(t *testing.T) {
	// Create a test schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	// Create temporary file
	tmpFile := "/tmp/test_lockbox.lbx"
	defer os.Remove(tmpFile)

	password := "test_password_123"

	// Create lockbox
	lb, err := Create(
		tmpFile,
		schema,
		WithPassword(password),
		WithCreatedBy("test"),
	)
	if err != nil {
		t.Fatalf("Failed to create lockbox: %v", err)
	}

	// Create test data
	mem := memory.NewGoAllocator()

	idBuilder := array.NewInt64Builder(mem)
	nameBuilder := array.NewStringBuilder(mem)

	idBuilder.Append(1)
	idBuilder.Append(2)
	nameBuilder.Append("Alice")
	nameBuilder.Append("Bob")

	idArray := idBuilder.NewArray()
	nameArray := nameBuilder.NewArray()

	record := array.NewRecord(schema, []arrow.Array{idArray, nameArray}, 2)

	// Write data
	ctx := context.Background()
	err = lb.Write(ctx, record, WithPassword(password))
	if err != nil {
		record.Release()
		idArray.Release()
		nameArray.Release()
		idBuilder.Release()
		nameBuilder.Release()
		lb.Close()
		t.Fatalf("Failed to write data: %v", err)
	}

	// Clean up
	record.Release()
	idArray.Release()
	nameArray.Release()
	idBuilder.Release()
	nameBuilder.Release()
	lb.Close()

	t.Logf("Successfully created and wrote to lockbox: %s", tmpFile)
}

func TestOpenAndRead(t *testing.T) {
	// This test depends on TestCreateAndWrite having run first
	// In a real test suite, we'd set up the file in this test

	tmpFile := "/tmp/test_lockbox_read.lbx"
	defer os.Remove(tmpFile)

	// First create a file to read from
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	password := "test_password_123"

	// Create and populate the file
	lb, err := Create(tmpFile, schema, WithPassword(password), WithCreatedBy("test"))
	if err != nil {
		t.Fatalf("Failed to create lockbox for read test: %v", err)
	}

	// Write some data
	mem := memory.NewGoAllocator()
	idBuilder := array.NewInt64Builder(mem)
	nameBuilder := array.NewStringBuilder(mem)

	idBuilder.Append(1)
	nameBuilder.Append("Test User")

	idArray := idBuilder.NewArray()
	nameArray := nameBuilder.NewArray()
	record := array.NewRecord(schema, []arrow.Array{idArray, nameArray}, 1)

	ctx := context.Background()
	err = lb.Write(ctx, record, WithPassword(password))
	if err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}

	// Clean up write resources
	record.Release()
	idArray.Release()
	nameArray.Release()
	idBuilder.Release()
	nameBuilder.Release()
	lb.Close()

	// Now test reading
	lb2, err := Open(tmpFile, WithPassword(password))
	if err != nil {
		t.Fatalf("Failed to open lockbox: %v", err)
	}
	defer lb2.Close()

	// Read data
	readRecord, err := lb2.Read(ctx, WithPassword(password))
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}
	defer readRecord.Release()

	if readRecord.NumRows() != 1 {
		t.Errorf("Expected 1 row, got %d", readRecord.NumRows())
	}

	if len(readRecord.Columns()) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(readRecord.Columns()))
	}

	t.Logf("Successfully read %d rows with %d columns", readRecord.NumRows(), len(readRecord.Columns()))
}

func TestInfo(t *testing.T) {
	tmpFile := "/tmp/test_lockbox_info.lbx"
	defer os.Remove(tmpFile)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "test_field", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	password := "test_password_123"
	creator := "test_creator"

	// Create lockbox
	lb, err := Create(tmpFile, schema, WithPassword(password), WithCreatedBy(creator))
	if err != nil {
		t.Fatalf("Failed to create lockbox: %v", err)
	}
	defer lb.Close()

	// Get info
	info, err := lb.Info()
	if err != nil {
		t.Fatalf("Failed to get info: %v", err)
	}

	if info.CreatedBy != creator {
		t.Errorf("Expected creator %s, got %s", creator, info.CreatedBy)
	}

	if info.Schema == nil {
		t.Error("Expected schema to be present")
	} else if len(info.Schema.Fields()) != 1 {
		t.Errorf("Expected 1 field, got %d", len(info.Schema.Fields()))
	}

	t.Logf("Info test passed: created by %s, %d fields", info.CreatedBy, len(info.Schema.Fields()))
}
