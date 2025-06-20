package cmd

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
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

		ctx := context.Background()

		var record arrow.Record

		if sampleData {
			// Generate sample data
			record, err = generateSampleData(lb.Schema())
			if err != nil {
				return fmt.Errorf("failed to generate sample data: %w", err)
			}
		} else if inputFile != "" {
			// Load data from file
			record, err = loadDataFromFile(inputFile, lb.Schema())
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
	writeCmd.Flags().StringP("password", "p", "", "Password for encryption")
	writeCmd.Flags().Bool("sample", false, "Generate sample data")
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
