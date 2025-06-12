package cmd

import (
	"encoding/json"
	"fmt"
	"syscall"

	"github.com/TFMV/lockbox/pkg/lockbox"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var infoCmd = &cobra.Command{
	Use:   "info [lockbox-file]",
	Short: "Display information about a lockbox file",
	Long: `Display detailed information about a lockbox file including:
- Schema information
- Metadata details
- Creation and modification timestamps
- Block information
- Access audit logs`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		filename := args[0]

		password, _ := cmd.Flags().GetString("password")
		outputFormat, _ := cmd.Flags().GetString("output")

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

		// Get file info
		info, err := lb.Info()
		if err != nil {
			return fmt.Errorf("failed to get file info: %w", err)
		}

		// Display information
		switch outputFormat {
		case "json":
			return displayInfoJSON(info)
		default:
			return displayInfoTable(info, filename)
		}
	},
}

func init() {
	rootCmd.AddCommand(infoCmd)

	infoCmd.Flags().StringP("password", "p", "", "Password for decryption")
	infoCmd.Flags().StringP("output", "o", "table", "Output format (table, json)")
}

func displayInfoTable(info *lockbox.Info, filename string) error {
	fmt.Printf("Lockbox File Information\n")
	fmt.Printf("========================\n\n")

	fmt.Printf("File: %s\n", filename)
	fmt.Printf("Version: %d\n", info.Version)
	fmt.Printf("Created By: %s\n", info.CreatedBy)
	fmt.Printf("Created At: %v\n", info.CreatedAt)
	fmt.Printf("Modified By: %s\n", info.ModifiedBy)
	fmt.Printf("Modified At: %v\n", info.ModifiedAt)
	fmt.Printf("Block Count: %d\n", info.BlockCount)
	fmt.Printf("Access Count: %d\n", info.AccessCount)

	fmt.Printf("\nSchema Information\n")
	fmt.Printf("------------------\n")

	if info.Schema != nil {
		fmt.Printf("Fields: %d\n", len(info.Schema.Fields()))
		for i, field := range info.Schema.Fields() {
			nullable := ""
			if field.Nullable {
				nullable = " (nullable)"
			}
			fmt.Printf("  %d. %s: %s%s\n", i+1, field.Name, field.Type, nullable)
		}
	} else {
		fmt.Printf("Schema: Not available\n")
	}

	return nil
}

func displayInfoJSON(info *lockbox.Info) error {
	// Convert schema to a JSON-serializable format
	type SchemaField struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		Nullable bool   `json:"nullable"`
	}

	var fields []SchemaField
	if info.Schema != nil {
		for _, field := range info.Schema.Fields() {
			fields = append(fields, SchemaField{
				Name:     field.Name,
				Type:     field.Type.String(),
				Nullable: field.Nullable,
			})
		}
	}

	output := map[string]interface{}{
		"version":     info.Version,
		"createdBy":   info.CreatedBy,
		"createdAt":   info.CreatedAt,
		"modifiedBy":  info.ModifiedBy,
		"modifiedAt":  info.ModifiedAt,
		"blockCount":  info.BlockCount,
		"accessCount": info.AccessCount,
		"schema": map[string]interface{}{
			"fields": fields,
		},
	}

	jsonData, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	fmt.Println(string(jsonData))
	return nil
}
