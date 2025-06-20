package cmd

import (
	"context"
	"fmt"
	"strings"
	"syscall"
	"time"

	"github.com/TFMV/lockbox/pkg/lockbox"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var queryCmd = &cobra.Command{
	Use:   "query [lockbox-file]",
	Short: "Query data from a lockbox file",
	Long: `Query data from a lockbox file using SQL-like syntax.

This command provides basic querying capabilities to retrieve and filter data
from encrypted lockbox files.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		filename := args[0]

		sqlQuery, _ := cmd.Flags().GetString("sql")
		columnsFlag, _ := cmd.Flags().GetString("columns")
		password, _ := cmd.Flags().GetString("password")
		output, _ := cmd.Flags().GetString("output")

		if columnsFlag != "" {
			cols := strings.Split(columnsFlag, ",")
			for i, c := range cols {
				cols[i] = strings.TrimSpace(c)
			}
			if sqlQuery == "" || sqlQuery == "SELECT * FROM data" {
				sqlQuery = fmt.Sprintf("SELECT %s FROM data", strings.Join(cols, ", "))
			}
		}

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

		// Execute query
		result, err := lb.Query(ctx, sqlQuery, lockbox.WithPassword(password))
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}
		defer result.Release()

		// Output results
		switch output {
		case "json":
			return outputJSON(result)
		case "csv":
			return outputCSV(result)
		case "table":
			return outputTable(result)
		default:
			return outputTable(result)
		}
	},
}

func init() {
	rootCmd.AddCommand(queryCmd)

	queryCmd.Flags().StringP("sql", "q", "SELECT * FROM data", "SQL query to execute")
	queryCmd.Flags().String("columns", "", "Column projection shorthand")
	queryCmd.Flags().StringP("password", "p", "", "Password for decryption")
	queryCmd.Flags().StringP("output", "o", "table", "Output format (table, json, csv)")
}

func outputTable(rec arrow.Record) error {
	schema := rec.Schema()

	// Print header
	for i, field := range schema.Fields() {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print(field.Name)
	}
	fmt.Println()

	// Print separator
	for i, field := range schema.Fields() {
		if i > 0 {
			fmt.Print("\t")
		}
		for range len(field.Name) {
			fmt.Print("-")
		}
	}
	fmt.Println()

	// Print data rows
	for i := int64(0); i < rec.NumRows(); i++ {
		for j, col := range rec.Columns() {
			if j > 0 {
				fmt.Print("\t")
			}
			fmt.Printf("%v", getValue(col, int(i))) // ⬅️ THIS LINE is mandatory!
		}
		fmt.Println()
	}

	return nil
}

// func outputTable(rec arrow.Record) error {
// 	schema := rec.Schema()

// 	// Print header
// 	for i, field := range schema.Fields() {
// 		if i > 0 {
// 			fmt.Print("\t")
// 		}
// 		fmt.Print(field.Name)
// 	}
// 	fmt.Println()

// 	// Print separator
// 	for i, field := range schema.Fields() {
// 		if i > 0 {
// 			fmt.Print("\t")
// 		}
// 		for range len(field.Name) {
// 			fmt.Print("-")
// 		}
// 	}
// 	fmt.Println()

// 	// Print data rows
// 	for i := int64(0); i < rec.NumRows(); i++ {
// 		for j, col := range rec.Columns() {
// 			if j > 0 {
// 				fmt.Print("\t")
// 			}
// 			fmt.Printf("%v", getValue(col, int(i)))
// 		}
// 		fmt.Println()
// 	}

// 	return nil
// }

func outputJSON(rec arrow.Record) error {
	schema := rec.Schema()
	fmt.Print("[")
	for i := int64(0); i < rec.NumRows(); i++ {
		if i > 0 {
			fmt.Print(",")
		}
		fmt.Print("{")
		for j, col := range rec.Columns() {
			if j > 0 {
				fmt.Print(",")
			}
			fmt.Printf("\"%s\":\"%v\"", schema.Field(j).Name, getValue(col, int(i)))
		}
		fmt.Print("}")
	}
	fmt.Println("]")

	return nil
}

func outputCSV(rec arrow.Record) error {
	schema := rec.Schema()

	// Print header
	for i, field := range schema.Fields() {
		if i > 0 {
			fmt.Print(",")
		}
		fmt.Print(field.Name)
	}
	fmt.Println()

	// Print data - simplified for MVP
	for i := int64(0); i < rec.NumRows(); i++ {
		for j, col := range rec.Columns() {
			if j > 0 {
				fmt.Print(",")
			}
			fmt.Printf("%v", getValue(col, int(i)))
		}
		fmt.Println()
	}

	return nil
}

func getValue(col arrow.Array, row int) interface{} {
	if col.IsNull(row) {
		return "NULL"
	}
	switch c := col.(type) {
	case *array.Int64:
		val := c.Value(row)
		return val
	case *array.Float64:
		val := c.Value(row)
		return val
	case *array.String:
		val := c.Value(row)
		return val
	case *array.Timestamp:
		ts := c.Value(row)
		switch typ := c.DataType().(*arrow.TimestampType); typ.Unit {
		case arrow.Second:
			return time.Unix(int64(ts), 0).UTC().Format(time.RFC3339)
		case arrow.Millisecond:
			return time.UnixMilli(int64(ts)).UTC().Format(time.RFC3339)
		case arrow.Microsecond:
			return time.UnixMicro(int64(ts)).UTC().Format(time.RFC3339)
		case arrow.Nanosecond:
			return time.Unix(0, int64(ts)).UTC().Format(time.RFC3339)
		default:
			return ts
		}
	default:
		return "NULL"
	}
}
