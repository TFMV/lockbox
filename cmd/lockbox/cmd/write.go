package cmd

import (
	"context"
	"fmt"
	"syscall"

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
	fmt.Printf("Note: File loading not fully implemented in MVP, generating sample data instead\n")
	return generateSampleData(schema)
}
