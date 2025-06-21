package metadata

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

const (
	// FileFormatVersion is the current version of the lockbox file format
	FileFormatVersion = 1
	// MagicBytes identifies a lockbox file
	MagicBytes = "LOCKBOX\x00"
)

// FileHeader represents the lockbox file header
type FileHeader struct {
	Magic    [8]byte `json:"magic"`
	Version  uint32  `json:"version"`
	Flags    uint32  `json:"flags"`
	Reserved uint32  `json:"reserved"`
}

// EncryptionParams holds encryption configuration
type EncryptionParams struct {
	Algorithm     string            `json:"algorithm"`     // "AES-256-GCM"
	KeyDerivation string            `json:"keyDerivation"` // "PBKDF2"
	Iterations    int               `json:"iterations"`
	SaltSize      int               `json:"saltSize"`
	ColumnSalts   map[string][]byte `json:"columnSalts"` // Column name -> salt
	MasterSalt    []byte            `json:"masterSalt"`
}

// AccessPolicy represents access control rules
type AccessPolicy struct {
	Version    int         `json:"version"`
	Principals []Principal `json:"principals"`
	Resources  []Resource  `json:"resources"`
	Actions    []string    `json:"actions"`
	Conditions []Condition `json:"conditions"`
	CreatedAt  time.Time   `json:"createdAt"`
	ModifiedAt time.Time   `json:"modifiedAt"`
}

// Principal represents a user, role, or service account
type Principal struct {
	Type string `json:"type"` // "user", "role", "service"
	Name string `json:"name"`
}

// Resource represents a column, table, or dataset
type Resource struct {
	Type string `json:"type"` // "column", "table", "dataset"
	Name string `json:"name"`
}

// Condition represents access conditions
type Condition struct {
	Type  string      `json:"type"` // "time", "ip", "custom"
	Value interface{} `json:"value"`
}

// AuditTrail holds audit information
type AuditTrail struct {
	CreatedAt  time.Time     `json:"createdAt"`
	CreatedBy  string        `json:"createdBy"`
	ModifiedAt time.Time     `json:"modifiedAt"`
	ModifiedBy string        `json:"modifiedBy"`
	AccessLog  []AccessEntry `json:"accessLog"`
	Version    int           `json:"version"`
}

// AccessEntry represents a single access event
type AccessEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Principal string    `json:"principal"`
	Action    string    `json:"action"`
	Resource  string    `json:"resource"`
	Success   bool      `json:"success"`
	Details   string    `json:"details,omitempty"`
}

// Metadata represents the complete lockbox metadata
type Metadata struct {
	Header       FileHeader       `json:"header"`
	Schema       *arrow.Schema    `json:"-"` // Serialized separately
	SchemaBytes  []byte           `json:"schemaBytes"`
	Encryption   EncryptionParams `json:"encryption"`
	AccessPolicy *AccessPolicy    `json:"accessPolicy,omitempty"`
	AuditTrail   AuditTrail       `json:"auditTrail"`
	BlockInfo    []BlockInfo      `json:"blockInfo"`
}

// BlockInfo describes an encrypted data block
type BlockInfo struct {
	ColumnName string `json:"columnName"`
	Offset     int64  `json:"offset"`
	Length     int64  `json:"length"`
	RowCount   int64  `json:"rowCount"`
	Compressed bool   `json:"compressed"`
	Checksum   []byte `json:"checksum"`
	OrigSize   int64  `json:"origSize,omitempty"`
	MimeType   string `json:"mimeType,omitempty"`
}

// NewMetadata creates new metadata for a lockbox file
func NewMetadata(schema *arrow.Schema, masterSalt []byte, createdBy string) (*Metadata, error) {
	// Serialize schema
	var buf []byte
	writer := ipc.NewWriter(&writeBuffer{data: &buf}, ipc.WithSchema(schema))
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to serialize schema: %w", err)
	}

	// Create file header
	header := FileHeader{
		Version:  FileFormatVersion,
		Flags:    0,
		Reserved: 0,
	}
	copy(header.Magic[:], MagicBytes)

	// Create encryption params
	encryption := EncryptionParams{
		Algorithm:     "AES-256-GCM",
		KeyDerivation: "PBKDF2",
		Iterations:    100000,
		SaltSize:      32,
		ColumnSalts:   make(map[string][]byte),
		MasterSalt:    masterSalt,
	}

	// Create audit trail
	now := time.Now()
	auditTrail := AuditTrail{
		CreatedAt:  now,
		CreatedBy:  createdBy,
		ModifiedAt: now,
		ModifiedBy: createdBy,
		AccessLog:  []AccessEntry{},
		Version:    1,
	}

	return &Metadata{
		Header:       header,
		Schema:       schema,
		SchemaBytes:  buf,
		Encryption:   encryption,
		AccessPolicy: nil,
		AuditTrail:   auditTrail,
		BlockInfo:    []BlockInfo{},
	}, nil
}

// Serialize serializes metadata to JSON
func (m *Metadata) Serialize() ([]byte, error) {
	// Update schema bytes if schema exists
	if m.Schema != nil {
		var buf []byte
		writer := ipc.NewWriter(&writeBuffer{data: &buf}, ipc.WithSchema(m.Schema))
		if err := writer.Close(); err != nil {
			return nil, fmt.Errorf("failed to serialize schema: %w", err)
		}
		m.SchemaBytes = buf
	}

	return json.MarshalIndent(m, "", "  ")
}

// Deserialize deserializes metadata from JSON
func Deserialize(data []byte) (*Metadata, error) {
	var m Metadata
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("failed to deserialize metadata: %w", err)
	}

	// Deserialize schema
	if len(m.SchemaBytes) > 0 {
		reader, err := ipc.NewReader(&readBuffer{data: m.SchemaBytes})
		if err != nil {
			return nil, fmt.Errorf("failed to create schema reader: %w", err)
		}
		m.Schema = reader.Schema()
		reader.Release()
	}

	return &m, nil
}

// AddBlockInfo adds information about an encrypted block
func (m *Metadata) AddBlockInfo(columnName string, offset, length, rowCount int64, checksum []byte, origSize int64, mime string) {
	m.BlockInfo = append(m.BlockInfo, BlockInfo{
		ColumnName: columnName,
		Offset:     offset,
		Length:     length,
		RowCount:   rowCount,
		Compressed: false,
		Checksum:   checksum,
		OrigSize:   origSize,
		MimeType:   mime,
	})
}

// LogAccess logs an access event
func (m *Metadata) LogAccess(principal, action, resource string, success bool, details string) {
	entry := AccessEntry{
		Timestamp: time.Now(),
		Principal: principal,
		Action:    action,
		Resource:  resource,
		Success:   success,
		Details:   details,
	}
	m.AuditTrail.AccessLog = append(m.AuditTrail.AccessLog, entry)
}

// writeBuffer is a helper for writing schema bytes
type writeBuffer struct {
	data *[]byte
}

func (wb *writeBuffer) Write(p []byte) (n int, err error) {
	*wb.data = append(*wb.data, p...)
	return len(p), nil
}

// readBuffer is a helper for reading schema bytes
type readBuffer struct {
	data []byte
	pos  int
}

func (rb *readBuffer) Read(p []byte) (n int, err error) {
	if rb.pos >= len(rb.data) {
		return 0, io.EOF
	}
	n = copy(p, rb.data[rb.pos:])
	rb.pos += n
	return n, nil
}
