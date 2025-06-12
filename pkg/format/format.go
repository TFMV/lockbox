package format

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/TFMV/lockbox/pkg/crypto"
	"github.com/TFMV/lockbox/pkg/metadata"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog/log"
)

// LockboxFile represents a lockbox file handle
type LockboxFile struct {
	file     *os.File
	metadata *metadata.Metadata
	readonly bool
}

// Writer handles writing encrypted Arrow data to lockbox files
type Writer struct {
	file       *LockboxFile
	encryptors map[string]*crypto.ColumnEncryptor
	masterKey  []byte
}

// Reader handles reading encrypted Arrow data from lockbox files
type Reader struct {
	file       *LockboxFile
	encryptors map[string]*crypto.ColumnEncryptor
	masterKey  []byte
}

// Create creates a new lockbox file
func Create(filename string, schema *arrow.Schema, password string, createdBy string) (*LockboxFile, error) {
	// Generate master key
	masterKey, err := crypto.NewKey(password)
	if err != nil {
		return nil, fmt.Errorf("failed to generate master key: %w", err)
	}

	// Create metadata
	meta, err := metadata.NewMetadata(schema, masterKey.Salt, createdBy)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata: %w", err)
	}

	// Ensure schema is properly set
	meta.Schema = schema

	// Create file
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	lbf := &LockboxFile{
		file:     file,
		metadata: meta,
		readonly: false,
	}

	// Write header and metadata
	if err := lbf.writeHeader(); err != nil {
		file.Close()
		os.Remove(filename)
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	// Write initial metadata with schema
	if err := lbf.updateMetadata(); err != nil {
		file.Close()
		os.Remove(filename)
		return nil, fmt.Errorf("failed to write initial metadata: %w", err)
	}

	log.Info().Str("file", filename).Msg("Created lockbox file")
	return lbf, nil
}

// Open opens an existing lockbox file
func Open(filename string, password string) (*LockboxFile, error) {
	file, err := os.OpenFile(filename, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	lbf := &LockboxFile{
		file:     file,
		readonly: false,
	}

	// Read header and metadata
	if err := lbf.readHeader(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Verify password by attempting to derive key
	derivedKey := crypto.DeriveKey(password, lbf.metadata.Encryption.MasterSalt)
	if derivedKey == nil {
		file.Close()
		return nil, fmt.Errorf("invalid password")
	}

	log.Info().Str("file", filename).Msg("Opened lockbox file")
	return lbf, nil
}

// Close closes the lockbox file
func (lbf *LockboxFile) Close() error {
	if lbf.file != nil {
		return lbf.file.Close()
	}
	return nil
}

// Schema returns the Arrow schema
func (lbf *LockboxFile) Schema() *arrow.Schema {
	return lbf.metadata.Schema
}

// Metadata returns the file metadata
func (lbf *LockboxFile) Metadata() *metadata.Metadata {
	return lbf.metadata
}

// NewWriter creates a new writer for the lockbox file
func (lbf *LockboxFile) NewWriter(password string) (*Writer, error) {
	if lbf.readonly {
		return nil, fmt.Errorf("file is read-only")
	}

	// Derive master key
	masterKey := crypto.DeriveKey(password, lbf.metadata.Encryption.MasterSalt)
	if masterKey == nil {
		return nil, fmt.Errorf("failed to derive master key")
	}

	// Create column encryptors
	encryptors := make(map[string]*crypto.ColumnEncryptor)
	for i, field := range lbf.metadata.Schema.Fields() {
		columnKey := crypto.DeriveColumnKey(masterKey.Data, field.Name, lbf.metadata.Encryption.MasterSalt)
		encryptor, err := crypto.NewColumnEncryptor(columnKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create encryptor for column %s: %w", field.Name, err)
		}
		encryptors[field.Name] = encryptor
		log.Debug().Str("column", field.Name).Int("index", i).Msg("Created column encryptor")
	}

	return &Writer{
		file:       lbf,
		encryptors: encryptors,
		masterKey:  masterKey.Data,
	}, nil
}

// NewReader creates a new reader for the lockbox file
func (lbf *LockboxFile) NewReader(password string) (*Reader, error) {
	// Derive master key
	masterKey := crypto.DeriveKey(password, lbf.metadata.Encryption.MasterSalt)
	if masterKey == nil {
		return nil, fmt.Errorf("failed to derive master key")
	}

	// Create column encryptors
	encryptors := make(map[string]*crypto.ColumnEncryptor)
	for i, field := range lbf.metadata.Schema.Fields() {
		columnKey := crypto.DeriveColumnKey(masterKey.Data, field.Name, lbf.metadata.Encryption.MasterSalt)
		encryptor, err := crypto.NewColumnEncryptor(columnKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create encryptor for column %s: %w", field.Name, err)
		}
		encryptors[field.Name] = encryptor
		log.Debug().Str("column", field.Name).Int("index", i).Msg("Created column encryptor")
	}

	return &Reader{
		file:       lbf,
		encryptors: encryptors,
		masterKey:  masterKey.Data,
	}, nil
}

// WriteRecord writes an encrypted Arrow record to the file
func (w *Writer) WriteRecord(record arrow.Record) error {
	mem := memory.NewGoAllocator()
	defer record.Release()

	// Get current file position for offset tracking
	_, err := w.file.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get file position: %w", err)
	}

	// Write each column as a separate encrypted block
	for i, col := range record.Columns() {
		field := record.Schema().Field(i)

		// Serialize column to Arrow IPC format
		var buf bytes.Buffer
		batch := array.NewRecord(
			arrow.NewSchema([]arrow.Field{field}, nil),
			[]arrow.Array{col},
			record.NumRows(),
		)

		writer := ipc.NewWriter(&buf, ipc.WithSchema(batch.Schema()), ipc.WithAllocator(mem))
		if err := writer.Write(batch); err != nil {
			batch.Release()
			return fmt.Errorf("failed to serialize column %s: %w", field.Name, err)
		}
		writer.Close()
		batch.Release()

		// Encrypt the serialized data
		encryptor, exists := w.encryptors[field.Name]
		if !exists {
			return fmt.Errorf("no encryptor for column %s", field.Name)
		}

		encryptedData, err := encryptor.Encrypt(buf.Bytes())
		if err != nil {
			return fmt.Errorf("failed to encrypt column %s: %w", field.Name, err)
		}

		// Calculate checksum
		checksum := sha256.Sum256(encryptedData)

		// Write encrypted data
		blockStart, err := w.file.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("failed to get block start position: %w", err)
		}

		if _, err := w.file.file.Write(encryptedData); err != nil {
			return fmt.Errorf("failed to write encrypted data: %w", err)
		}

		// Update metadata with block info
		w.file.metadata.AddBlockInfo(
			field.Name,
			blockStart,
			int64(len(encryptedData)),
			record.NumRows(),
			checksum[:],
		)

		log.Debug().
			Str("column", field.Name).
			Int64("offset", blockStart).
			Int("size", len(encryptedData)).
			Msg("Wrote encrypted column block")
	}

	// Log access
	w.file.metadata.LogAccess("system", "write", "record", true, fmt.Sprintf("wrote %d rows", record.NumRows()))

	// Update metadata in file
	if err := w.file.updateMetadata(); err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	return nil
}

// ReadRecord reads and decrypts an Arrow record from the file
func (r *Reader) ReadRecord() (arrow.Record, error) {
	mem := memory.NewGoAllocator()

	// Read all column blocks for this record
	var columns []arrow.Array
	schema := r.file.metadata.Schema

	for i, field := range schema.Fields() {
		// Find block info for this column
		var blockInfo *metadata.BlockInfo
		for _, block := range r.file.metadata.BlockInfo {
			if block.ColumnName == field.Name {
				blockInfo = &block
				break
			}
		}

		if blockInfo == nil {
			return nil, fmt.Errorf("no block info for column %s", field.Name)
		}

		// Seek to block position
		if _, err := r.file.file.Seek(blockInfo.Offset, io.SeekStart); err != nil {
			return nil, fmt.Errorf("failed to seek to block for column %s: %w", field.Name, err)
		}

		// Read encrypted data
		encryptedData := make([]byte, blockInfo.Length)
		if _, err := io.ReadFull(r.file.file, encryptedData); err != nil {
			return nil, fmt.Errorf("failed to read encrypted data for column %s: %w", field.Name, err)
		}

		// Verify checksum
		checksum := sha256.Sum256(encryptedData)
		if !bytes.Equal(checksum[:], blockInfo.Checksum) {
			return nil, fmt.Errorf("checksum mismatch for column %s", field.Name)
		}

		// Decrypt data
		encryptor, exists := r.encryptors[field.Name]
		if !exists {
			return nil, fmt.Errorf("no encryptor for column %s", field.Name)
		}

		decryptedData, err := encryptor.Decrypt(encryptedData)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt column %s: %w", field.Name, err)
		}

		// Deserialize Arrow data
		reader, err := ipc.NewReader(bytes.NewReader(decryptedData), ipc.WithAllocator(mem))
		if err != nil {
			return nil, fmt.Errorf("failed to create reader for column %s: %w", field.Name, err)
		}

		record, err := reader.Read()
		if err != nil {
			reader.Release()
			return nil, fmt.Errorf("failed to read record for column %s: %w", field.Name, err)
		}

		if record.Column(0) == nil {
			record.Release()
			reader.Release()
			return nil, fmt.Errorf("nil column data for %s", field.Name)
		}

		// Retain the column before releasing the record
		col := record.Column(0)
		col.Retain()
		columns = append(columns, col)
		record.Release()
		reader.Release()

		log.Debug().
			Str("column", field.Name).
			Int("index", i).
			Msg("Read and decrypted column")
	}

	// Create combined record
	result := array.NewRecord(schema, columns, -1)

	// Release individual columns since NewRecord retains them
	for _, col := range columns {
		col.Release()
	}

	// Log access
	r.file.metadata.LogAccess("system", "read", "record", true, fmt.Sprintf("read %d rows", result.NumRows()))

	return result, nil
}

// writeHeader writes the file header and initial metadata
func (lbf *LockboxFile) writeHeader() error {
	// Write file header with placeholder for metadata offset
	header := lbf.metadata.Header
	if err := binary.Write(lbf.file, binary.LittleEndian, header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write placeholder for metadata offset (will be updated later)
	metadataOffset := uint64(0)
	if err := binary.Write(lbf.file, binary.LittleEndian, metadataOffset); err != nil {
		return fmt.Errorf("failed to write metadata offset placeholder: %w", err)
	}

	return nil
}

// readHeader reads the file header and metadata
func (lbf *LockboxFile) readHeader() error {
	// Read file header
	var header metadata.FileHeader
	if err := binary.Read(lbf.file, binary.LittleEndian, &header); err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	// Verify magic bytes
	if string(header.Magic[:]) != metadata.MagicBytes {
		return fmt.Errorf("invalid magic bytes")
	}

	// Check version
	if header.Version != metadata.FileFormatVersion {
		return fmt.Errorf("unsupported file version: %d", header.Version)
	}

	// Read metadata offset
	var metadataOffset uint64
	if err := binary.Read(lbf.file, binary.LittleEndian, &metadataOffset); err != nil {
		return fmt.Errorf("failed to read metadata offset: %w", err)
	}

	// If metadata offset is 0, metadata hasn't been written yet (new file)
	if metadataOffset == 0 {
		return fmt.Errorf("file has no metadata - file may be corrupted or incomplete")
	}

	// Seek to metadata position
	if _, err := lbf.file.Seek(int64(metadataOffset), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to metadata: %w", err)
	}

	// Read metadata length
	var metadataLen uint32
	if err := binary.Read(lbf.file, binary.LittleEndian, &metadataLen); err != nil {
		return fmt.Errorf("failed to read metadata length: %w", err)
	}

	// Read metadata
	metadataBytes := make([]byte, metadataLen)
	if _, err := io.ReadFull(lbf.file, metadataBytes); err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}

	// Deserialize metadata
	meta, err := metadata.Deserialize(metadataBytes)
	if err != nil {
		return fmt.Errorf("failed to deserialize metadata: %w", err)
	}

	meta.Header = header
	lbf.metadata = meta
	return nil
}

// updateMetadata writes the current metadata to the end of the file
func (lbf *LockboxFile) updateMetadata() error {
	if lbf.readonly {
		return fmt.Errorf("file is read-only")
	}

	// Seek to end of file to write metadata
	metadataPos, err := lbf.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek to end of file: %w", err)
	}

	// Serialize and write metadata
	metadataBytes, err := lbf.metadata.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize metadata: %w", err)
	}

	// Write metadata length
	metadataLen := uint32(len(metadataBytes))
	if err := binary.Write(lbf.file, binary.LittleEndian, metadataLen); err != nil {
		return fmt.Errorf("failed to write metadata length: %w", err)
	}

	// Write metadata
	if _, err := lbf.file.Write(metadataBytes); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// Update metadata offset in header
	if _, err := lbf.file.Seek(20, io.SeekStart); err != nil { // After FileHeader
		return fmt.Errorf("failed to seek to metadata offset position: %w", err)
	}

	if err := binary.Write(lbf.file, binary.LittleEndian, uint64(metadataPos)); err != nil {
		return fmt.Errorf("failed to write metadata offset: %w", err)
	}

	// Seek back to end for any future writes
	if _, err := lbf.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek to end: %w", err)
	}

	return nil
}
