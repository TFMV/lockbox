# Lockbox

**Secure, high-performance columnar data storage with Apache Arrow**

Lockbox is a secure data storage system that combines Apache Arrow's zero-copy columnar data structures with enterprise-grade encryption. It provides developers with a "fast data, under lock and key" paradigm that doesn't compromise on performance, security, or developer experience.

## Features

### 🔐 MVP Features (v0.1)

- **Arrow I/O**: Read/write Arrow IPC and Feather formats with full type support
- **Column-Level Encryption**: AES-256-GCM encryption with individual column keys
- **Password-Based Authentication**: PBKDF2 key derivation with configurable iterations
- **CLI Interface**: Complete command-line tools for common operations
- **Go SDK**: Programmatic API for Go applications
- **Metadata Management**: Comprehensive schema and encryption metadata storage
- **Audit Trail**: Basic access logging and file metadata tracking

### 🚀 Core Value Proposition

- **Performance**: Zero-copy Arrow operations with selective decryption
- **Security**: AES-256-GCM encryption with fine-grained access controls
- **Ergonomics**: Simple CLI and Go SDK with intuitive APIs
- **Local-First**: Operates without cloud dependencies
- **Extensible**: Platform for secure query patterns and data workflows

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/TFMV/lockbox
cd lockbox

# Build the CLI
go build ./cmd/lockbox

# Run tests
go test ./pkg/lockbox -v
```

### Basic Usage

```bash
# Create a new lockbox with default schema
./lockbox create mydata.lbx --password mypassword123

# View file information
./lockbox info mydata.lbx --password mypassword123

# Write sample data
./lockbox write mydata.lbx --sample --password mypassword123

# Query data (basic implementation)
./lockbox query mydata.lbx --password mypassword123
```

### Using Custom Schema

Create a schema file (`schema.json`):

```json
{
  "fields": [
    {"name": "user_id", "type": "int64", "nullable": false},
    {"name": "username", "type": "string", "nullable": false},
    {"name": "email", "type": "string", "nullable": true},
    {"name": "created_at", "type": "timestamp", "nullable": false},
    {"name": "score", "type": "float64", "nullable": true}
  ]
}
```

```bash
# Create lockbox with custom schema
./lockbox create userdata.lbx --schema schema.json --password mypassword123
```

## Go SDK Usage

```go
package main

import (
    "context"
    "log"

    "github.com/TFMV/lockbox/pkg/lockbox"
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

func main() {
    // Define schema
    schema := arrow.NewSchema([]arrow.Field{
        {Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
        {Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
    }, nil)

    // Create lockbox
    lb, err := lockbox.Create(
        "data.lbx",
        schema,
        lockbox.WithPassword("mypassword123"),
        lockbox.WithCreatedBy("myapp"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer lb.Close()

    // Create sample data
    mem := memory.NewGoAllocator()
    idBuilder := array.NewInt64Builder(mem)
    nameBuilder := array.NewStringBuilder(mem)
    
    idBuilder.Append(1)
    nameBuilder.Append("Alice")
    
    idArray := idBuilder.NewArray()
    nameArray := nameBuilder.NewArray()
    record := array.NewRecord(schema, []arrow.Array{idArray, nameArray}, 1)

    // Write data
    ctx := context.Background()
    err = lb.Write(ctx, record, lockbox.WithPassword("mypassword123"))
    if err != nil {
        log.Fatal(err)
    }

    // Read data back
    readRecord, err := lb.Read(ctx, lockbox.WithPassword("mypassword123"))
    if err != nil {
        log.Fatal(err)
    }
    defer readRecord.Release()

    log.Printf("Read %d rows with %d columns", readRecord.NumRows(), len(readRecord.Columns()))
}
```

## Architecture

### File Format (.lbx)

```
┌─────────────────────────────────────────────────────────────┐
│                      Lockbox Header                         │
├─────────────────────────────────────────────────────────────┤
│ Magic Bytes (8)  │ Version (4)  │ Flags (4)  │ Reserved (4) │
├─────────────────────────────────────────────────────────────┤
│                 Metadata Offset (8)                         │
├─────────────────────────────────────────────────────────────┤
│                   Encrypted Data Blocks                     │
│  ┌─────────────────────────────────────────────────────────┐│
│  │ Block 1: Column A (Encrypted Arrow RecordBatch)         ││
│  ├─────────────────────────────────────────────────────────┤│
│  │ Block 2: Column B (Encrypted Arrow RecordBatch)         ││
│  ├─────────────────────────────────────────────────────────┤│
│  │ Block N: Column N (Encrypted Arrow RecordBatch)         ││
│  └─────────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────────┤
│                    Metadata Block                           │
│  - Schema Information                                       │
│  - Encryption Parameters                                    │
│  - Block Information & Checksums                            │
│  - Audit Trail                                              │
└─────────────────────────────────────────────────────────────┘
```

### Security Model

**Encryption**:

- **Algorithm**: AES-256-GCM for authenticated encryption
- **Key Derivation**: PBKDF2 with 100,000 iterations
- **Column-Level Keys**: Each column encrypted with derived keys
- **Integrity**: SHA-256 checksums for all encrypted blocks

**Key Management**:

```
Master Key = PBKDF2(Password, Salt, 100000 iterations)
Column Key = PBKDF2(Master Key + Column Name, Salt, 100000 iterations)
```

### Performance Characteristics

The MVP implementation focuses on correctness and security while maintaining reasonable performance:

- **Encryption Overhead**: ~15-20% compared to unencrypted Arrow
- **Column Selectivity**: Only decrypt requested columns
- **Memory Efficiency**: Zero-copy Arrow operations where possible
- **File Size**: ~5-10% overhead for metadata and encryption

## CLI Reference

### Commands

#### `create`

Create a new lockbox file with specified schema.

```bash
lockbox create [file] --password [password] [options]

Options:
  -s, --schema string      JSON schema file
  -p, --password string    Password for encryption (required)
      --created-by string  Creator name (default "system")
```

#### `write`

Write data to an existing lockbox file.

```bash
lockbox write [file] --password [password] [options]

Options:
  -i, --input string     Input data file (CSV, JSON)
  -p, --password string  Password for encryption
      --sample           Generate sample data
```

#### `query`

Query data from a lockbox file.

```bash
lockbox query [file] --password [password] [options]

Options:
  -q, --sql string       SQL query to execute (default "SELECT * FROM data")
  -p, --password string  Password for decryption
  -o, --output string    Output format (table, json, csv) (default "table")
```

#### `info`

Display information about a lockbox file.

```bash
lockbox info [file] --password [password] [options]

Options:
  -p, --password string  Password for decryption
  -o, --output string    Output format (table, json) (default "table")
```

### Global Options

```bash
Options:
  -v, --verbose          Enable verbose output
      --config string    Config file (default is $HOME/.lockbox.yaml)
```

## Supported Data Types

### Arrow Types

- **Integers**: int8, int16, int32, int64, uint8, uint16, uint32, uint64
- **Floating Point**: float32, float64
- **Strings**: utf8, binary
- **Boolean**: bool
- **Temporal**: date32, timestamp, time32ms, duration
- **Complex**: Coming in future versions

### Schema Definition

Schemas are defined in JSON format with the following structure:

```json
{
  "fields": [
    {
      "name": "column_name",
      "type": "arrow_type",
      "nullable": true|false
    }
  ]
}
```

## Testing

Run the test suite:

```bash
# Run all tests
go test ./...

# Run with verbose output
go test -v ./pkg/lockbox

# Run specific test
go test -v ./pkg/lockbox -run TestCreateAndWrite
```

The test suite includes:

- Basic create/write/read operations
- Encryption/decryption verification
- File format validation
- Error handling scenarios

## Development Status

- Core file format and encryption
- Arrow integration with column-level encryption
- CLI with essential commands
- Go SDK with functional options
- Basic testing framework
- Password-based authentication

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Lockbox** - Fast data, under lock and key. 🔐⚡
