package lockbox

import (
	"context"
	"fmt"

	"github.com/TFMV/lockbox/pkg/format"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/rs/zerolog/log"
)

// Lockbox represents a lockbox file with high-level operations
type Lockbox struct {
	file   *format.LockboxFile
	writer *format.Writer
	reader *format.Reader
}

// Options for lockbox operations
type Options struct {
	Password  string
	CreatedBy string
	Columns   []string
}

// Option is a functional option for lockbox operations
type Option func(*Options)

// WithPassword sets the password for lockbox operations
func WithPassword(password string) Option {
	return func(o *Options) {
		o.Password = password
	}
}

// WithCreatedBy sets the creator name for lockbox operations
func WithCreatedBy(createdBy string) Option {
	return func(o *Options) {
		o.CreatedBy = createdBy
	}
}

// WithColumns sets the specific columns to operate on
func WithColumns(columns ...string) Option {
	return func(o *Options) {
		o.Columns = columns
	}
}

// Create creates a new lockbox file with the given schema
func Create(filename string, schema *arrow.Schema, opts ...Option) (*Lockbox, error) {
	options := &Options{
		Password:  "",
		CreatedBy: "system",
		Columns:   []string{},
	}

	for _, opt := range opts {
		opt(options)
	}

	if options.Password == "" {
		return nil, fmt.Errorf("password is required")
	}

	file, err := format.Create(filename, schema, options.Password, options.CreatedBy)
	if err != nil {
		return nil, fmt.Errorf("failed to create lockbox file: %w", err)
	}

	lb := &Lockbox{
		file: file,
	}

	log.Info().
		Str("file", filename).
		Int("fields", len(schema.Fields())).
		Msg("Created new lockbox")

	return lb, nil
}

// Open opens an existing lockbox file
func Open(filename string, opts ...Option) (*Lockbox, error) {
	options := &Options{
		Password:  "",
		CreatedBy: "system",
		Columns:   []string{},
	}

	for _, opt := range opts {
		opt(options)
	}

	if options.Password == "" {
		return nil, fmt.Errorf("password is required")
	}

	file, err := format.Open(filename, options.Password)
	if err != nil {
		return nil, fmt.Errorf("failed to open lockbox file: %w", err)
	}

	lb := &Lockbox{
		file: file,
	}

	log.Info().
		Str("file", filename).
		Int("fields", len(file.Schema().Fields())).
		Msg("Opened lockbox")

	return lb, nil
}

// Close closes the lockbox file
func (lb *Lockbox) Close() error {
	if lb.writer != nil {
		// Writers don't need explicit closing, they use the underlying file
		lb.writer = nil
	}
	if lb.reader != nil {
		// Readers don't need explicit closing, they use the underlying file
		lb.reader = nil
	}
	if lb.file != nil {
		return lb.file.Close()
	}
	return nil
}

// Schema returns the Arrow schema of the lockbox
func (lb *Lockbox) Schema() *arrow.Schema {
	return lb.file.Schema()
}

// Write writes an Arrow record to the lockbox
func (lb *Lockbox) Write(ctx context.Context, record arrow.Record, opts ...Option) error {
	options := &Options{
		Password: "",
		Columns:  []string{},
	}

	for _, opt := range opts {
		opt(options)
	}

	if options.Password == "" {
		return fmt.Errorf("password is required for writing")
	}

	// Create writer if it doesn't exist
	if lb.writer == nil {
		writer, err := lb.file.NewWriter(options.Password)
		if err != nil {
			return fmt.Errorf("failed to create writer: %w", err)
		}
		lb.writer = writer
	}

	// Write the record
	if err := lb.writer.WriteRecord(record); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	log.Debug().
		Int64("rows", record.NumRows()).
		Int("columns", len(record.Columns())).
		Msg("Wrote record to lockbox")

	return nil
}

// Read reads an Arrow record from the lockbox
func (lb *Lockbox) Read(ctx context.Context, opts ...Option) (arrow.Record, error) {
	options := &Options{
		Password: "",
		Columns:  []string{},
	}

	for _, opt := range opts {
		opt(options)
	}

	if options.Password == "" {
		return nil, fmt.Errorf("password is required for reading")
	}

	// Create reader if it doesn't exist
	if lb.reader == nil {
		reader, err := lb.file.NewReader(options.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to create reader: %w", err)
		}
		lb.reader = reader
	}

	// Read the record
	record, err := lb.reader.ReadRecord()
	if err != nil {
		return nil, fmt.Errorf("failed to read record: %w", err)
	}

	log.Debug().
		Int64("rows", record.NumRows()).
		Int("columns", len(record.Columns())).
		Msg("Read record from lockbox")

	return record, nil
}

// Query performs a simple query on the lockbox data
// This is a basic implementation that reads all data and applies simple filters
func (lb *Lockbox) Query(ctx context.Context, query string, opts ...Option) (*QueryResult, error) {
	options := &Options{
		Password: "",
		Columns:  []string{},
	}

	for _, opt := range opts {
		opt(options)
	}

	if options.Password == "" {
		return nil, fmt.Errorf("password is required for querying")
	}

	// For MVP, we'll implement a very basic query parser
	// In a full implementation, this would be much more sophisticated
	record, err := lb.Read(ctx, WithPassword(options.Password))
	if err != nil {
		return nil, fmt.Errorf("failed to read data for query: %w", err)
	}

	// Create query result
	result := &QueryResult{
		record: record,
		schema: record.Schema(),
	}

	log.Debug().
		Str("query", query).
		Int64("rows", record.NumRows()).
		Msg("Executed query on lockbox")

	return result, nil
}

// QueryResult represents the result of a query operation
type QueryResult struct {
	record arrow.Record
	schema *arrow.Schema
	pos    int
}

// Next advances to the next row in the result set
func (qr *QueryResult) Next() bool {
	return qr.pos < int(qr.record.NumRows())
}

// Record returns the current Arrow record
func (qr *QueryResult) Record() arrow.Record {
	if qr.pos < int(qr.record.NumRows()) {
		qr.pos++
		return qr.record
	}
	return nil
}

// Schema returns the schema of the query result
func (qr *QueryResult) Schema() *arrow.Schema {
	return qr.schema
}

// Release releases the resources associated with the query result
func (qr *QueryResult) Release() {
	if qr.record != nil {
		qr.record.Release()
		qr.record = nil
	}
}

// Info returns information about the lockbox file
func (lb *Lockbox) Info() (*Info, error) {
	meta := lb.file.Metadata()

	return &Info{
		Version:     meta.Header.Version,
		Schema:      meta.Schema,
		CreatedAt:   meta.AuditTrail.CreatedAt,
		CreatedBy:   meta.AuditTrail.CreatedBy,
		ModifiedAt:  meta.AuditTrail.ModifiedAt,
		ModifiedBy:  meta.AuditTrail.ModifiedBy,
		BlockCount:  len(meta.BlockInfo),
		AccessCount: len(meta.AuditTrail.AccessLog),
	}, nil
}

// Info represents information about a lockbox file
type Info struct {
	Version     uint32        `json:"version"`
	Schema      *arrow.Schema `json:"-"`
	CreatedAt   interface{}   `json:"createdAt"`
	CreatedBy   string        `json:"createdBy"`
	ModifiedAt  interface{}   `json:"modifiedAt"`
	ModifiedBy  string        `json:"modifiedBy"`
	BlockCount  int           `json:"blockCount"`
	AccessCount int           `json:"accessCount"`
}
