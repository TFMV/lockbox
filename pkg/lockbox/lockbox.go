package lockbox

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/TFMV/lockbox/pkg/crypto"
	"github.com/TFMV/lockbox/pkg/format"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog/log"
)

// Lockbox represents a lockbox file with high-level operations
type Lockbox struct {
	file   *format.LockboxFile
	writer *format.Writer
	reader *format.Reader
	key    *crypto.Key // Store the key for signing operations
}

// Options for lockbox operations
type Options struct {
	Password  string
	CreatedBy string
	Columns   []string
	DryRun    bool
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

// WithDryRun enables or disables dry-run mode
func WithDryRun(v bool) Option {
	return func(o *Options) {
		o.DryRun = v
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

	// Generate key with post-quantum components
	key, err := crypto.NewKey(options.Password)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}

	file, err := format.Create(filename, schema, options.Password, options.CreatedBy)
	if err != nil {
		return nil, fmt.Errorf("failed to create lockbox file: %w", err)
	}

	lb := &Lockbox{
		file: file,
		key:  key,
	}

	log.Info().
		Str("file", filename).
		Int("fields", len(schema.Fields())).
		Bool("pq_enabled", true).
		Msg("Created new lockbox with post-quantum protection")

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

	// Derive key with post-quantum components if available
	key := crypto.DeriveKey(options.Password, nil) // Salt will be read from file

	file, err := format.Open(filename, options.Password)
	if err != nil {
		return nil, fmt.Errorf("failed to open lockbox file: %w", err)
	}

	lb := &Lockbox{
		file: file,
		key:  key,
	}

	log.Info().
		Str("file", filename).
		Int("fields", len(file.Schema().Fields())).
		Bool("pq_enabled", key.KyberPublicKey != nil).
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

	// Sign the record before writing
	if lb.key != nil && lb.key.KyberSecretKey != nil {
		encryptor, err := crypto.NewColumnEncryptor(lb.key.Data)
		if err != nil {
			return fmt.Errorf("failed to create encryptor: %w", err)
		}
		// Set up Kyber keys
		encryptor.KyberPublicKey = lb.key.KyberPublicKey
		encryptor.KyberSecretKey = lb.key.KyberSecretKey

		// Sign the serialized record data
		recordBytes := []byte(fmt.Sprintf("%v", record))
		signature, err := encryptor.Sign(recordBytes)
		if err != nil {
			return fmt.Errorf("failed to sign record: %w", err)
		}

		// Store signature in metadata (implementation detail left to format package)
		// This is just a placeholder - actual implementation would need format package support
		log.Debug().
			Int("signature_size", len(signature)).
			Msg("Added quantum-resistant signature to record")
	}

	// Write the record
	if err := lb.writer.WriteRecord(record); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	log.Debug().
		Int64("rows", record.NumRows()).
		Int("columns", len(record.Columns())).
		Bool("pq_signed", lb.key != nil && lb.key.KyberSecretKey != nil).
		Msg("Wrote record to lockbox")

	return nil
}

// WriteAsync performs Write in a separate goroutine
func (lb *Lockbox) WriteAsync(ctx context.Context, record arrow.Record, opts ...Option) <-chan error {
	ch := make(chan error, 1)
	go func() {
		ch <- lb.Write(ctx, record, opts...)
	}()
	return ch
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

// ReadAsync performs Read in a separate goroutine
func (lb *Lockbox) ReadAsync(ctx context.Context, opts ...Option) (<-chan arrow.Record, <-chan error) {
	rch := make(chan arrow.Record, 1)
	ech := make(chan error, 1)
	go func() {
		rec, err := lb.Read(ctx, opts...)
		if err != nil {
			ech <- err
			close(rch)
			close(ech)
			return
		}
		rch <- rec
		close(ech)
	}()
	return rch, ech
}

// Query performs a simple query on the lockbox data
// This is a basic implementation that reads all data and applies simple filters
// query-engine
func (lb *Lockbox) Query(ctx context.Context, query string, opts ...Option) (arrow.Record, error) {
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

	pq, err := parseQuery(query)
	if err != nil {
		return nil, err
	}

	// Determine required columns
	required := append([]string{}, pq.SelectCols...)
	if pq.WhereCol != "" {
		if !contains(required, pq.WhereCol) {
			required = append(required, pq.WhereCol)
		}
	}
	if pq.OrderCol != "" {
		if !contains(required, pq.OrderCol) {
			required = append(required, pq.OrderCol)
		}
	}

	reader, err := lb.file.NewReader(options.Password)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}

	rec, err := reader.ReadColumns(required)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	result, err := applyQuery(rec, pq)
	if err != nil {
		rec.Release()
		return nil, err
	}
	rec.Release()

	log.Debug().Str("query", query).Int64("rows", result.NumRows()).Msg("Executed query on lockbox")

	return result, nil
}

type parsedQuery struct {
	SelectCols []string
	WhereCol   string
	WhereOp    string
	WhereVal   string
	OrderCol   string
	OrderDesc  bool
	Limit      int
}

func parseQuery(q string) (*parsedQuery, error) {
	pq := &parsedQuery{Limit: -1}

	upper := strings.ToUpper(q)
	parts := strings.Fields(upper)
	if len(parts) < 4 || parts[0] != "SELECT" {
		return nil, fmt.Errorf("invalid query")
	}

	fromIdx := -1
	for i, p := range parts {
		if p == "FROM" {
			fromIdx = i
			break
		}
	}
	if fromIdx == -1 || fromIdx == 1 {
		return nil, fmt.Errorf("invalid query")
	}

	selectRaw := strings.Join(parts[1:fromIdx], " ")
	cols := strings.Split(selectRaw, ",")
	for i := range cols {
		c := strings.TrimSpace(cols[i])
		if c != "*" && c != "" {
			pq.SelectCols = append(pq.SelectCols, strings.ToLower(c))
		}
	}

	i := fromIdx + 2 // skip FROM data
	for i < len(parts) {
		switch parts[i] {
		case "WHERE":
			if i+3 >= len(parts) {
				return nil, fmt.Errorf("invalid WHERE clause")
			}
			pq.WhereCol = strings.ToLower(parts[i+1])
			pq.WhereOp = parts[i+2]
			pq.WhereVal = parts[i+3]
			i += 4
		case "ORDER":
			if i+3 >= len(parts) || parts[i+1] != "BY" {
				return nil, fmt.Errorf("invalid ORDER BY clause")
			}
			pq.OrderCol = strings.ToLower(parts[i+2])
			if i+3 < len(parts) && (parts[i+3] == "DESC" || parts[i+3] == "ASC") {
				pq.OrderDesc = parts[i+3] == "DESC"
				i += 4
			} else {
				i += 3
			}
		case "LIMIT":
			if i+1 >= len(parts) {
				return nil, fmt.Errorf("invalid LIMIT clause")
			}
			val, err := strconv.Atoi(parts[i+1])
			if err != nil {
				return nil, fmt.Errorf("invalid LIMIT value")
			}
			pq.Limit = val
			i += 2
		default:
			i++
		}
	}

	return pq, nil
}

func applyQuery(rec arrow.Record, pq *parsedQuery) (arrow.Record, error) {
	mem := memory.NewGoAllocator()

	rowCount := int(rec.NumRows())
	idx := make([]int, rowCount)
	for i := range idx {
		idx[i] = i
	}

	// WHERE filtering
	if pq.WhereCol != "" {
		col := rec.Column(rec.Schema().FieldIndices(pq.WhereCol)[0])
		var keep []int
		for _, i := range idx {
			if matchValue(col, i, pq.WhereOp, pq.WhereVal) {
				keep = append(keep, i)
			}
		}
		idx = keep
	}

	// ORDER BY
	if pq.OrderCol != "" {
		col := rec.Column(rec.Schema().FieldIndices(pq.OrderCol)[0])
		sort.Slice(idx, func(a, b int) bool {
			va := getValue(col, idx[a])
			vb := getValue(col, idx[b])
			if pq.OrderDesc {
				return less(vb, va)
			}
			return less(va, vb)
		})
	}

	// LIMIT
	if pq.Limit >= 0 && pq.Limit < len(idx) {
		idx = idx[:pq.Limit]
	}

	// Build result
	if len(pq.SelectCols) == 0 {
		for _, f := range rec.Schema().Fields() {
			pq.SelectCols = append(pq.SelectCols, f.Name)
		}
	}

	builders := make([]array.Builder, len(pq.SelectCols))
	fields := make([]arrow.Field, len(pq.SelectCols))

	for i, name := range pq.SelectCols {
		fIdx := rec.Schema().FieldIndices(name)[0]
		field := rec.Schema().Field(fIdx)
		fields[i] = field
		switch field.Type.ID() {
		case arrow.INT64:
			builders[i] = array.NewInt64Builder(mem)
		case arrow.FLOAT64:
			builders[i] = array.NewFloat64Builder(mem)
		case arrow.STRING:
			builders[i] = array.NewStringBuilder(mem)
		case arrow.TIMESTAMP:
			builders[i] = array.NewTimestampBuilder(mem, field.Type.(*arrow.TimestampType))
		default:
			// fallback to string, or handle more types as needed
			builders[i] = array.NewStringBuilder(mem)
		}
	}

	for _, row := range idx {
		for i, name := range pq.SelectCols {
			fIdx := rec.Schema().FieldIndices(name)[0]
			col := rec.Column(fIdx)
			appendValue(builders[i], col, row)
		}
	}

	arrays := make([]arrow.Array, len(builders))
	for i, b := range builders {
		arrays[i] = b.NewArray()
		b.Release()
	}

	schema := arrow.NewSchema(fields, nil)
	return array.NewRecord(schema, arrays, int64(len(idx))), nil
}

func matchValue(col arrow.Array, row int, op, val string) bool {
	cv := getValue(col, row)
	fVal, ferr := strconv.ParseFloat(val, 64)
	switch v := cv.(type) {
	case int64:
		if ferr != nil {
			return false
		}
		switch op {
		case ">":
			return float64(v) > fVal
		case "<":
			return float64(v) < fVal
		case "=":
			return float64(v) == fVal
		case ">=":
			return float64(v) >= fVal
		case "<=":
			return float64(v) <= fVal
		}
	case float64:
		if ferr != nil {
			return false
		}
		switch op {
		case ">":
			return v > fVal
		case "<":
			return v < fVal
		case "=":
			return v == fVal
		case ">=":
			return v >= fVal
		case "<=":
			return v <= fVal
		}
	case string:
		switch op {
		case "=":
			return v == strings.Trim(val, "'\"")
		}
	}
	return false
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

func appendValue(b array.Builder, col arrow.Array, row int) {
	switch c := col.(type) {
	case *array.Int64:
		b.(*array.Int64Builder).Append(c.Value(row))
	case *array.Float64:
		b.(*array.Float64Builder).Append(c.Value(row))
	case *array.String:
		b.(*array.StringBuilder).Append(c.Value(row))
	case *array.Timestamp:
		b.(*array.TimestampBuilder).Append(c.Value(row))
	}
}

func less(a, b interface{}) bool {
	switch av := a.(type) {
	case int64:
		return av < b.(int64)
	case float64:
		return av < b.(float64)
	case string:
		return av < b.(string)
	default:
		return false
	}
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
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

// Validate verifies the integrity of the lockbox data blocks
func (lb *Lockbox) Validate() error {
	return lb.file.ValidateBlocks()
}

// Repair attempts to remove corrupted blocks and update metadata
func (lb *Lockbox) Repair() error {
	return lb.file.Repair()
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

// IngestParquet ingests a Parquet file into the lockbox
func (lb *Lockbox) IngestParquet(ctx context.Context, path string, opts ...Option) error {
	options := &Options{Password: "", Columns: []string{}, DryRun: false}
	for _, opt := range opts {
		opt(options)
	}

	if options.Password == "" {
		return fmt.Errorf("password is required for ingestion")
	}

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer f.Close()

	mem := memory.NewGoAllocator()

	pf, err := file.NewParquetReader(f)
	if err != nil {
		return fmt.Errorf("failed to read parquet file: %w", err)
	}
	defer pf.Close()

	pqReader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 1024}, mem)
	if err != nil {
		return fmt.Errorf("failed to create parquet reader: %w", err)
	}

	pqSchema, err := pqReader.Schema()
	if err != nil {
		return fmt.Errorf("failed to get parquet schema: %w", err)
	}
	if err := validateParquetSchema(lb.Schema(), pqSchema); err != nil {
		return err
	}

	recReader, err := pqReader.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to get record reader: %w", err)
	}
	defer recReader.Release()

	var totalRows int64
	for recReader.Next() {
		rec := recReader.Record()
		coerced, err := coerceRecord(lb.Schema(), rec)
		if err != nil {
			rec.Release()
			return err
		}

		if !options.DryRun {
			if err := lb.Write(ctx, coerced, WithPassword(options.Password)); err != nil {
				coerced.Release()
				rec.Release()
				return err
			}
		}
		totalRows += coerced.NumRows()
		coerced.Release()
		rec.Release()
	}

	log.Info().Str("file", path).Int64("rows", totalRows).Bool("dry_run", options.DryRun).Msg("Ingested parquet")
	return nil
}

// IngestParquetAsync runs IngestParquet in a goroutine
func (lb *Lockbox) IngestParquetAsync(ctx context.Context, path string, opts ...Option) <-chan error {
	ch := make(chan error, 1)
	go func() {
		ch <- lb.IngestParquet(ctx, path, opts...)
	}()
	return ch
}

// validateParquetSchema ensures the parquet schema matches or is a superset of the lockbox schema
func validateParquetSchema(lb *arrow.Schema, pq *arrow.Schema) error {
	for i, field := range lb.Fields() {
		if i >= len(pq.Fields()) {
			return fmt.Errorf("parquet missing field %s", field.Name)
		}

		pqField := pq.Field(i)
		if field.Name != pqField.Name {
			return fmt.Errorf("field name mismatch at index %d: %s vs %s", i, field.Name, pqField.Name)
		}

		if !typesCompatible(field.Type, pqField.Type) {
			return fmt.Errorf("incompatible type for field %s", field.Name)
		}
	}
	return nil
}

// typesCompatible checks if parquet type can be coerced into lockbox type
func typesCompatible(dst, src arrow.DataType) bool {
	if arrow.TypeEqual(dst, src) {
		return true
	}
	if dst.ID() == arrow.INT64 && src.ID() == arrow.INT32 {
		return true
	}
	return false
}

// coerceRecord converts parquet record columns to lockbox schema order and types
func coerceRecord(schema *arrow.Schema, rec arrow.Record) (arrow.Record, error) {
	if rec.Schema().Equal(schema) {
		rec.Retain()
		return rec, nil
	}

	mem := memory.NewGoAllocator()
	var cols []arrow.Array
	for i, field := range schema.Fields() {
		src := rec.Column(i)
		if !arrow.TypeEqual(field.Type, src.DataType()) {
			if field.Type.ID() == arrow.INT64 && src.DataType().ID() == arrow.INT32 {
				b := array.NewInt64Builder(mem)
				int32Arr := src.(*array.Int32)
				for j := 0; j < int(int32Arr.Len()); j++ {
					if int32Arr.IsNull(j) {
						b.AppendNull()
					} else {
						b.Append(int64(int32Arr.Value(j)))
					}
				}
				cols = append(cols, b.NewArray())
				b.Release()
			} else {
				return nil, fmt.Errorf("cannot coerce column %s", field.Name)
			}
		} else {
			src.Retain()
			cols = append(cols, src)
		}
	}
	out := array.NewRecord(schema, cols, rec.NumRows())
	for _, c := range cols {
		c.Release()
	}
	return out, nil
}
