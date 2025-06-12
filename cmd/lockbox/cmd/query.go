package cmd

import (
	"context"
	"fmt"
	"syscall"

	"github.com/TFMV/lockbox/pkg/lockbox"
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
		password, _ := cmd.Flags().GetString("password")
		output, _ := cmd.Flags().GetString("output")

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
	queryCmd.Flags().StringP("password", "p", "", "Password for decryption")
	queryCmd.Flags().StringP("output", "o", "table", "Output format (table, json, csv)")
}

func outputTable(result *lockbox.QueryResult) error {
	schema := result.Schema()

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
	for result.Next() {
		record := result.Record()
		if record != nil {
			for i := int64(0); i < record.NumRows(); i++ {
				for j, col := range record.Columns() {
					if j > 0 {
						fmt.Print("\t")
					}

					// Simple value extraction - in a full implementation
					// this would handle all Arrow types properly
					switch col := col.(type) {
					default:
						fmt.Printf("%v", col)
					}
				}
				fmt.Println()
			}
		}
	}

	return nil
}

func outputJSON(result *lockbox.QueryResult) error {
	fmt.Println("{")
	fmt.Printf("  \"schema\": %v,\n", result.Schema())
	fmt.Println("  \"data\": [")

	first := true
	for result.Next() {
		if !first {
			fmt.Println(",")
		}
		fmt.Print("    {}")
		first = false
	}

	fmt.Println()
	fmt.Println("  ]")
	fmt.Println("}")

	return nil
}

func outputCSV(result *lockbox.QueryResult) error {
	schema := result.Schema()

	// Print header
	for i, field := range schema.Fields() {
		if i > 0 {
			fmt.Print(",")
		}
		fmt.Print(field.Name)
	}
	fmt.Println()

	// Print data - simplified for MVP
	for result.Next() {
		record := result.Record()
		if record != nil {
			for i := int64(0); i < record.NumRows(); i++ {
				for j := range record.Columns() {
					if j > 0 {
						fmt.Print(",")
					}
					fmt.Print("value") // Simplified for MVP
				}
				fmt.Println()
			}
		}
	}

	return nil
}
