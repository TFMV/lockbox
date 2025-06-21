# Lockbox

Lockbox combines the speed of Apache Arrow with transparent encryption so you can store columnar data securely on disk. It is written in Go and exposes both a command line interface and a Go SDK.

## Key Features

- **Arrow Based Storage** – Records are stored as Arrow IPC blocks for fast columnar access.
- **Hybrid Encryption** – Each column is encrypted with AES‑256‑GCM. A Kyber key pair is used to add post‑quantum protection.
- **Extensible Crypto Modules** – Additional encryption schemes can be plugged in via Go plugins.
- **Audit Friendly Metadata** – File metadata tracks creation details, access events and block checksums.
- **CLI and Go SDK** – Create, write, query and inspect `.lbx` files from the terminal or directly from Go.
- **Parquet Ingestion** – Library helpers allow importing Parquet files into a lockbox.

## The `.lbx` Format

An `.lbx` file starts with a small header followed by encrypted column blocks and a JSON metadata section.

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
│  │ ...                                                     ││
│  └─────────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────────┤
│                    Metadata Block                           │
│  - Schema information                                       │
│  - Encryption parameters                                    │
│  - Post‑quantum key material                                │
│  - Block information & checksums                            │
│  - Audit trail                                              │
└─────────────────────────────────────────────────────────────┘
```

The metadata keeps the Arrow schema, salts for each column and an audit log so the file can be validated and repaired if needed.

## Getting Started

### Build and Test

```bash
# Build the CLI
go build ./cmd/lockbox

# Run unit tests
go test ./...
```

### Creating and Querying a Lockbox

```bash
# Create a new file with the default schema
./lockbox create mydata.lbx --password secret

# Write some example rows
./lockbox write mydata.lbx --sample --password secret

# Write some CSV data
./lockbox write mydata.lbx --input <csv_data_file_path> --format csv --password secret

# Write some JSON data
./lockbox write mydata.lbx --input <json_data_file_path> --format json --password secret

# Inspect the file
./lockbox info mydata.lbx --password secret

# Run a simple query
./lockbox query mydata.lbx --password secret
```

### Custom Schemas

You can pass a JSON schema when creating a file.

```json
{
  "fields": [
    {"name": "user_id", "type": "int64", "nullable": false},
    {"name": "username", "type": "string", "nullable": false},
    {"name": "created_at", "type": "timestamp", "nullable": false}
  ]
}
```

```bash
./lockbox create users.lbx --schema schema.json --password secret
```

### Go SDK Example

```go
lb, err := lockbox.Create("data.lbx", schema,
    lockbox.WithPassword("secret"),
    lockbox.WithCreatedBy("example"),
)
if err != nil {
    log.Fatal(err)
}
defer lb.Close()

// Write and read records just like with the CLI
```

## Security Overview

- AES‑256‑GCM for column encryption
- Kyber based key exchange for post‑quantum protection
- PBKDF2‑derived master key and column keys
- Optional signatures using the Kyber key pair

Only the columns needed for a query are decrypted which keeps operations fast.

## CLI Reference

- `create` – create a new lockbox file
- `write` – append data to an existing file
- `query` – run a basic SQL‑like query against the data
- `info` – display schema and audit information

Run any command with `--help` for detailed flags.

