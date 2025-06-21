package cmd

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/TFMV/lockbox/pkg/lockbox"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var writeCmd = &cobra.Command{
	Use:   "write [lockbox-file]",
	Short: "Write data to a lockbox file",
	Long: `Write data to a lockbox file from various input sources.

Supported input formats:
- CSV files
- JSON files  
- Parquet files (future)
- Sample data generation`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		filename := args[0]

		inputFile, _ := cmd.Flags().GetString("input")
		password, _ := cmd.Flags().GetString("password")
		sampleData, _ := cmd.Flags().GetBool("sample")
		format, _ := cmd.Flags().GetString("format")
		blobArgs, _ := cmd.Flags().GetStringArray("blob")

		// Get password if not provided
		if password == "" {
			fmt.Print("Enter password: ")
			passwordBytes, err := term.ReadPassword(int(syscall.Stdin))
			if err != nil {
				return fmt.Errorf("failed to read password: %w", err)
			}
			password = string(passwordBytes)
			fmt.Println() // New line after password input
		}

		// Open the lockbox
		lb, err := lockbox.Open(filename, lockbox.WithPassword(password))
		if err != nil {
			return fmt.Errorf("failed to open lockbox: %w", err)
		}
		defer lb.Close()

		blobMap := parseBlobArgs(blobArgs)

		ctx := context.Background()

		var record arrow.Record

		if len(blobMap) > 0 {
			record, err = loadBlobRecord(blobMap, lb.Schema())
			if err != nil {
				return fmt.Errorf("failed to load blob data: %w", err)
			}
		} else if sampleData {
			// Generate sample data
			record, err = generateSampleData(lb.Schema())
			if err != nil {
				return fmt.Errorf("failed to generate sample data: %w", err)
			}
		} else if inputFile != "" && format == "csv" {
			// Load data from file
			record, err = loadDataFromFile(inputFile, lb.Schema())
			if err != nil {
				return fmt.Errorf("failed to load data from file: %w", err)
			}
		} else if inputFile != "" && format == "json" {
			// Load data from file
			record, err = loadDataFromJSON(inputFile, lb.Schema())
			if err != nil {
				return fmt.Errorf("failed to load data from file: %w", err)
			}
		} else {
			return fmt.Errorf("either --input or --sample must be specified")
		}

		// Write the data
		if err := lb.Write(ctx, record, lockbox.WithPassword(password)); err != nil {
			record.Release()
			return fmt.Errorf("failed to write data: %w", err)
		}

		record.Release()
		fmt.Printf("Successfully wrote %d rows to %s\n", record.NumRows(), filename)

		return nil
	},
}

func init() {
	rootCmd.AddCommand(writeCmd)

	writeCmd.Flags().StringP("input", "i", "", "Input data file (CSV, JSON)")
	writeCmd.Flags().StringP("format", "f", "", "Input data format (csv, json)")
	writeCmd.Flags().StringP("password", "p", "", "Password for encryption")
	writeCmd.Flags().Bool("sample", false, "Generate sample data")
	writeCmd.Flags().StringArray("blob", []string{}, "Blob field mapping field=file")
}

// generateSampleData creates sample Arrow data matching the schema
func generateSampleData(schema *arrow.Schema) (arrow.Record, error) {
	mem := memory.NewGoAllocator()

	// Create arrays for each field
	var arrays []arrow.Array

	numRows := 5 // Generate 5 sample rows

	for _, field := range schema.Fields() {
		switch field.Type {
		case arrow.PrimitiveTypes.Int64:
			builder := array.NewInt64Builder(mem)
			for i := 0; i < numRows; i++ {
				builder.Append(int64(i + 1))
			}
			arrays = append(arrays, builder.NewArray())
			builder.Release()

		case arrow.PrimitiveTypes.Int32:
			builder := array.NewInt32Builder(mem)
			for i := 0; i < numRows; i++ {
				builder.Append(int32(20 + i))
			}
			arrays = append(arrays, builder.NewArray())
			builder.Release()

		case arrow.BinaryTypes.String:
			builder := array.NewStringBuilder(mem)
			for i := 0; i < numRows; i++ {
				if field.Name == "name" {
					builder.Append(fmt.Sprintf("User%d", i+1))
				} else if field.Name == "email" {
					builder.Append(fmt.Sprintf("user%d@example.com", i+1))
				} else {
					builder.Append(fmt.Sprintf("sample_%s_%d", field.Name, i+1))
				}
			}
			arrays = append(arrays, builder.NewArray())
			builder.Release()

		case arrow.PrimitiveTypes.Float64:
			builder := array.NewFloat64Builder(mem)
			for i := 0; i < numRows; i++ {
				builder.Append(float64(i) * 1.5)
			}
			arrays = append(arrays, builder.NewArray())
			builder.Release()

		default:
			// Default to string for unsupported types
			builder := array.NewStringBuilder(mem)
			for i := 0; i < numRows; i++ {
				builder.Append(fmt.Sprintf("default_%d", i+1))
			}
			arrays = append(arrays, builder.NewArray())
			builder.Release()
		}
	}
	record := array.NewRecord(schema, arrays, int64(numRows))

	// Release individual arrays
	for _, arr := range arrays {
		arr.Release()
	}

	return record, nil
}

// loadDataFromFile loads data from various file formats
// This is a simplified implementation for MVP
func loadDataFromFile(filename string, schema *arrow.Schema) (arrow.Record, error) {
	// For MVP, we'll just generate sample data regardless of input file
	// In a full implementation, this would parse CSV, JSON, Parquet, etc.
	mem := memory.NewGoAllocator()
	numFields := len(schema.Fields())

	// Create array builders for each column
	builders := make([]array.Builder, numFields)
	for i, field := range schema.Fields() {
		switch typ := field.Type.(type) {
		case *arrow.Int64Type:
			builders[i] = array.NewInt64Builder(mem)
		case *arrow.Int32Type:
			builders[i] = array.NewInt32Builder(mem)
		case *arrow.Float64Type:
			builders[i] = array.NewFloat64Builder(mem)
		case *arrow.StringType:
			builders[i] = array.NewStringBuilder(mem)
		case *arrow.TimestampType:
			builders[i] = array.NewTimestampBuilder(mem, typ)
		default:
			return nil, fmt.Errorf("unsupported type: %v", field.Type)
		}
	}

	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	rdr := csv.NewReader(f)

	// skip the header row
	_, err = rdr.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	for rowNum := 2; ; rowNum++ { // Start from 2 since header was row 1
		row, err := rdr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) { // EOF check
				break
			}
			return nil, fmt.Errorf("error reading row %d: %w", rowNum, err)
		}
		if len(row) != numFields {
			return nil, fmt.Errorf("row %d: expected %d fields, got %d", rowNum, numFields, len(row))
		}

		for i, val := range row {
			field := schema.Field(i)
			switch typ := field.Type.(type) {
			case *arrow.Int64Type:
				if val == "" && field.Nullable {
					builders[i].(*array.Int64Builder).AppendNull()
					continue
				}
				v, err := strconv.ParseInt(val, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("row %d, col %s: invalid int64: %s", rowNum, field.Name, val)
				}
				builders[i].(*array.Int64Builder).Append(v)
			case *arrow.Int32Type:
				if val == "" && field.Nullable {
					builders[i].(*array.Int32Builder).AppendNull()
					continue
				}
				v, err := strconv.ParseInt(val, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("row %d, col %s: invalid int32: %s", rowNum, field.Name, val)
				}
				builders[i].(*array.Int32Builder).Append(int32(v))
			case *arrow.Float64Type:
				if val == "" && field.Nullable {
					builders[i].(*array.Float64Builder).AppendNull()
					continue
				}
				v, err := strconv.ParseFloat(val, 64)
				if err != nil {
					return nil, fmt.Errorf("row %d, col %s: invalid float64: %s", rowNum, field.Name, val)
				}
				builders[i].(*array.Float64Builder).Append(v)
			case *arrow.StringType:
				if val == "" && field.Nullable {
					builders[i].(*array.StringBuilder).AppendNull()
					continue
				}
				builders[i].(*array.StringBuilder).Append(val)
			case *arrow.TimestampType:
				if val == "" && field.Nullable {
					builders[i].(*array.TimestampBuilder).AppendNull()
					continue
				}
				tm, err := time.Parse(time.RFC3339, val)
				if err != nil {
					return nil, fmt.Errorf("row %d, col %s: invalid timestamp: %s", rowNum, field.Name, val)
				}
				var epoch int64
				switch typ.Unit {
				case arrow.Second:
					epoch = tm.Unix()
				case arrow.Millisecond:
					epoch = tm.UnixMilli()
				case arrow.Microsecond:
					epoch = tm.UnixMicro()
				case arrow.Nanosecond:
					epoch = tm.UnixNano()
				default:
					return nil, fmt.Errorf("unknown timestamp unit: %v", typ.Unit)
				}
				builders[i].(*array.TimestampBuilder).Append(arrow.Timestamp(epoch))
			default:
				return nil, fmt.Errorf("unsupported type in row %d, col %s: %v", rowNum, field.Name, field.Type)
			}
		}
	}

	// Build Arrow arrays and record
	arrays := make([]arrow.Array, numFields)
	for i, b := range builders {
		arrays[i] = b.NewArray()
		b.Release()
	}

	numRows := int64(arrays[0].Len())
	record := array.NewRecord(schema, arrays, numRows)

	// Clean up arrays
	for _, arr := range arrays {
		arr.Release()
	}

	return record, nil
}

func loadDataFromJSON(filename string, schema *arrow.Schema) (arrow.Record, error) {
	mem := memory.NewGoAllocator()
	numFields := len(schema.Fields())

	// Create builders for each column
	builders := make([]array.Builder, numFields)
	for i, field := range schema.Fields() {
		switch typ := field.Type.(type) {
		case *arrow.Int64Type:
			builders[i] = array.NewInt64Builder(mem)
		case *arrow.Int32Type:
			builders[i] = array.NewInt32Builder(mem)
		case *arrow.Float64Type:
			builders[i] = array.NewFloat64Builder(mem)
		case *arrow.StringType:
			builders[i] = array.NewStringBuilder(mem)
		case *arrow.TimestampType:
			builders[i] = array.NewTimestampBuilder(mem, typ)
		default:
			return nil, fmt.Errorf("unsupported type: %v", field.Type)
		}
	}

	// Open JSON file
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	// Read JSON: try as array, else NDJSON fallback
	dec := json.NewDecoder(f)
	var records []map[string]interface{}
	// Try to decode as array of objects
	if err := dec.Decode(&records); err != nil {
		// Reset file pointer and try NDJSON (one object per line)
		if _, err2 := f.Seek(0, io.SeekStart); err2 != nil {
			return nil, fmt.Errorf("invalid JSON format, and seek failed: %w", err)
		}
		dec = json.NewDecoder(f)
		records = []map[string]interface{}{}
		for {
			var row map[string]interface{}
			if err := dec.Decode(&row); err != nil {
				if err == io.EOF {
					break
				}
				return nil, fmt.Errorf("JSON decode error: %w", err)
			}
			records = append(records, row)
		}
	}

	// Process records
	for rowNum, rec := range records {
		for i, field := range schema.Fields() {
			val, ok := rec[field.Name]
			if !ok || val == nil {
				if field.Nullable {
					builders[i].AppendNull()
					continue
				}
				return nil, fmt.Errorf("row %d: missing non-nullable field '%s'", rowNum+1, field.Name)
			}
			switch typ := field.Type.(type) {
			case *arrow.Int64Type:
				switch v := val.(type) {
				case float64: // json.Unmarshal converts numbers to float64
					builders[i].(*array.Int64Builder).Append(int64(v))
				case string:
					if v == "" && field.Nullable {
						builders[i].(*array.Int64Builder).AppendNull()
					} else {
						num, err := strconv.ParseInt(v, 10, 64)
						if err != nil {
							return nil, fmt.Errorf("row %d, col %s: invalid int64: %v", rowNum+1, field.Name, v)
						}
						builders[i].(*array.Int64Builder).Append(num)
					}
				default:
					return nil, fmt.Errorf("row %d, col %s: expected int64, got %T", rowNum+1, field.Name, val)
				}
			case *arrow.Int32Type:
				switch v := val.(type) {
				case float64:
					builders[i].(*array.Int32Builder).Append(int32(v))
				case string:
					if v == "" && field.Nullable {
						builders[i].(*array.Int32Builder).AppendNull()
					} else {
						num, err := strconv.ParseInt(v, 10, 32)
						if err != nil {
							return nil, fmt.Errorf("row %d, col %s: invalid int32: %v", rowNum+1, field.Name, v)
						}
						builders[i].(*array.Int32Builder).Append(int32(num))
					}
				default:
					return nil, fmt.Errorf("row %d, col %s: expected int32, got %T", rowNum+1, field.Name, val)
				}
			case *arrow.Float64Type:
				switch v := val.(type) {
				case float64:
					builders[i].(*array.Float64Builder).Append(v)
				case string:
					if v == "" && field.Nullable {
						builders[i].(*array.Float64Builder).AppendNull()
					} else {
						num, err := strconv.ParseFloat(v, 64)
						if err != nil {
							return nil, fmt.Errorf("row %d, col %s: invalid float64: %v", rowNum+1, field.Name, v)
						}
						builders[i].(*array.Float64Builder).Append(num)
					}
				default:
					return nil, fmt.Errorf("row %d, col %s: expected float64, got %T", rowNum+1, field.Name, val)
				}
			case *arrow.StringType:
				switch v := val.(type) {
				case string:
					if v == "" && field.Nullable {
						builders[i].(*array.StringBuilder).AppendNull()
					} else {
						builders[i].(*array.StringBuilder).Append(v)
					}
				default:
					builders[i].(*array.StringBuilder).Append(fmt.Sprintf("%v", val))
				}
			case *arrow.TimestampType:
				switch v := val.(type) {
				case string:
					if v == "" && field.Nullable {
						builders[i].(*array.TimestampBuilder).AppendNull()
						continue
					}
					tm, err := time.Parse(time.RFC3339, v)
					if err != nil {
						return nil, fmt.Errorf("row %d, col %s: invalid timestamp: %v", rowNum+1, field.Name, v)
					}
					var epoch int64
					switch typ.Unit {
					case arrow.Second:
						epoch = tm.Unix()
					case arrow.Millisecond:
						epoch = tm.UnixMilli()
					case arrow.Microsecond:
						epoch = tm.UnixMicro()
					case arrow.Nanosecond:
						epoch = tm.UnixNano()
					default:
						return nil, fmt.Errorf("unknown timestamp unit: %v", typ.Unit)
					}
					builders[i].(*array.TimestampBuilder).Append(arrow.Timestamp(epoch))
				default:
					return nil, fmt.Errorf("row %d, col %s: invalid timestamp type: %T", rowNum+1, field.Name, val)
				}
			default:
				return nil, fmt.Errorf("unsupported type: %v", field.Type)
			}
		}
	}

	// Build Arrow arrays and record
	arrays := make([]arrow.Array, numFields)
	for i, b := range builders {
		arrays[i] = b.NewArray()
		b.Release()
	}
	numRows := int64(arrays[0].Len())
	record := array.NewRecord(schema, arrays, numRows)
	for _, arr := range arrays {
		arr.Release()
	}

	return record, nil
}

func parseBlobArgs(args []string) map[string]string {
	m := make(map[string]string)
	for _, a := range args {
		parts := strings.SplitN(a, "=", 2)
		if len(parts) == 2 {
			m[parts[0]] = parts[1]
		}
	}
	return m
}

func loadBlobRecord(blobs map[string]string, schema *arrow.Schema) (arrow.Record, error) {
	mem := memory.NewGoAllocator()

	builders := make([]array.Builder, len(schema.Fields()))
	for i, f := range schema.Fields() {
		switch f.Type.(type) {
		case *arrow.BinaryType, *arrow.LargeBinaryType:
			builders[i] = array.NewBinaryBuilder(mem, f.Type)
		case *arrow.StringType:
			builders[i] = array.NewStringBuilder(mem)
		default:
			builders[i] = array.NewStringBuilder(mem)
		}
	}

	for i, f := range schema.Fields() {
		if path, ok := blobs[f.Name]; ok {
			data, err := os.ReadFile(path)
			if err != nil {
				return nil, fmt.Errorf("read blob %s: %w", f.Name, err)
			}
			switch b := builders[i].(type) {
			case *array.BinaryBuilder:
				b.Append(data)
			case *array.LargeBinaryBuilder:
				b.Append(data)
			case *array.StringBuilder:
				b.Append(string(data))
			}
		} else {
			builders[i].AppendNull()
		}
	}

	arrays := make([]arrow.Array, len(schema.Fields()))
	for i, b := range builders {
		arrays[i] = b.NewArray()
		b.Release()
	}

	rec := array.NewRecord(schema, arrays, 1)
	for _, arr := range arrays {
		arr.Release()
	}
	return rec, nil
}
