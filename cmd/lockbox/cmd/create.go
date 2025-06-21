package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/TFMV/lockbox/pkg/lockbox"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var createCmd = &cobra.Command{
	Use:   "create [lockbox-file]",
	Short: "Create a new lockbox file",
	Long: `Create a new lockbox file with the specified schema.

The schema can be provided as a JSON file or generated from sample data.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		filename := args[0]

		schemaFile, _ := cmd.Flags().GetString("schema")
		password, _ := cmd.Flags().GetString("password")
		createdBy, _ := cmd.Flags().GetString("created-by")

		if password == "" {
			return fmt.Errorf("password is required")
		}

		var schema *arrow.Schema
		var err error

		if schemaFile != "" {
			schema, err = loadSchemaFromFile(schemaFile)
			if err != nil {
				return fmt.Errorf("failed to load schema: %w", err)
			}
		} else {
			// Default schema for demonstration
			schema = arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
				{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
				{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			}, nil)
			log.Info().Msg("Using default schema (id, name, email, age)")
		}

		// Create the lockbox
		lb, err := lockbox.Create(
			filename,
			schema,
			lockbox.WithPassword(password),
			lockbox.WithCreatedBy(createdBy),
		)
		if err != nil {
			return fmt.Errorf("failed to create lockbox: %w", err)
		}
		defer lb.Close()

		fmt.Printf("Successfully created lockbox: %s\n", filename)
		fmt.Printf("Schema fields: %d\n", len(schema.Fields()))
		for i, field := range schema.Fields() {
			fmt.Printf("  %d. %s (%s)\n", i+1, field.Name, field.Type)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(createCmd)

	createCmd.Flags().StringP("schema", "s", "", "JSON schema file")
	createCmd.Flags().StringP("password", "p", "", "Password for encryption (required)")
	createCmd.Flags().String("created-by", "system", "Creator name")

	if err := createCmd.MarkFlagRequired("password"); err != nil {
		log.Fatal().Err(err).Msg("Failed to mark password flag as required")
	}
}

// loadSchemaFromFile loads an Arrow schema from a JSON file
func loadSchemaFromFile(filename string) (*arrow.Schema, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Simple JSON schema format
	type SchemaField struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		Nullable bool   `json:"nullable"`
		Mime     string `json:"mime,omitempty"`
	}

	type SchemaJSON struct {
		Fields []SchemaField `json:"fields"`
	}

	var schemaJSON SchemaJSON
	if err := json.Unmarshal(data, &schemaJSON); err != nil {
		return nil, fmt.Errorf("failed to parse schema JSON: %w", err)
	}

	var fields []arrow.Field
	for _, field := range schemaJSON.Fields {
		var dataType arrow.DataType

		switch field.Type {
		case "int64":
			dataType = arrow.PrimitiveTypes.Int64
		case "int32":
			dataType = arrow.PrimitiveTypes.Int32
		case "float64":
			dataType = arrow.PrimitiveTypes.Float64
		case "float32":
			dataType = arrow.PrimitiveTypes.Float32
		case "string":
			dataType = arrow.BinaryTypes.String
		case "binary", "blob":
			dataType = arrow.BinaryTypes.Binary
		case "date":
			dataType = arrow.FixedWidthTypes.Date32
		case "timestamp":
			dataType = arrow.FixedWidthTypes.Timestamp_s
		case "time":
			dataType = arrow.FixedWidthTypes.Time32ms
		case "duration":
			dataType = arrow.FixedWidthTypes.Duration_s
		case "bool":
			dataType = arrow.FixedWidthTypes.Boolean
		default:
			return nil, fmt.Errorf("unsupported type: %s", field.Type)
		}

		var md arrow.Metadata
		if field.Mime != "" {
			md = arrow.NewMetadata([]string{"mime"}, []string{field.Mime})
		}

		fields = append(fields, arrow.Field{
			Name:     field.Name,
			Type:     dataType,
			Nullable: field.Nullable,
			Metadata: md,
		})
	}

	return arrow.NewSchema(fields, nil), nil
}
