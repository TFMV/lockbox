# Lockbox Design Document

**Version:** 1.0  
**Date:** 20245
**Status:** Draft

---

## Executive Summary

Lockbox is a secure, high-performance data storage system that combines Apache Arrow's zero-copy columnar data structures with enterprise-grade encryption. It provides developers with a "fast data, under lock and key" paradigm that doesn't compromise on performance, security, or developer experience.

### Core Value Proposition

- **Performance**: Zero-copy Arrow operations with selective decryption
- **Security**: AES-GCM encryption with fine-grained access controls
- **Ergonomics**: Simple CLI and Go SDK with intuitive APIs
- **Local-First**: Operates without cloud dependencies
- **Extensible**: Platform for secure query patterns and data workflows

---

## Architecture Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Lockbox Platform                         │
├─────────────────────────────────────────────────────────────┤
│  CLI Interface          │  Go SDK           │  HTTP API     │
├─────────────────────────────────────────────────────────────┤
│                    Core Engine                              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐  │
│  │ Query Processor │  │ Access Control  │  │ Crypto Core │  │
│  └─────────────────┘  └─────────────────┘  └─────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                 Storage Layer                               │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐  │
│  │ Arrow Store     │  │ Metadata Store  │  │ Key Store   │  │
│  └─────────────────┘  └─────────────────┘  └─────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Key Design Principles

1. **Security by Design**: All data encrypted at rest, keys managed separately
2. **Zero-Copy Performance**: Leverage Arrow's memory layout for speed
3. **Selective Decryption**: Only decrypt data that needs to be accessed
4. **Minimal Dependencies**: Reduce attack surface and deployment complexity
5. **Composable Architecture**: Each component can be used independently

---

## Core Components

### 1. Lockbox File Format (.lbx)

```
┌─────────────────────────────────────────────────────────────┐
│                      Lockbox Header                         │
├─────────────────────────────────────────────────────────────┤
│ Magic Bytes (8)  │ Version (4)  │ Flags (4)  │ Reserved (4) │
├─────────────────────────────────────────────────────────────┤
│                    Metadata Block                           │
│  - Schema Information                                       │
│  - Encryption Parameters                                    │
│  - Access Control Policies                                  │
│  - Audit Trail Headers                                      │
├─────────────────────────────────────────────────────────────┤
│                   Encrypted Data Blocks                     │
│  ┌────────────────────────────────────────────────────-─────┤
│  │ Block 1: Column A (Encrypted Arrow RecordBatch)          │
│  ├─────────────────────────────────────────────────────-────┤
│  │ Block 2: Column B (Encrypted Arrow RecordBatch)          │
│  ├──────────────────────────────────────────────────────-───┤
│  │ Block N: Column N (Encrypted Arrow RecordBatch)          │
│  └───────────────────────────────────────────────────────-──┘
└─────────────────────────────────────────────────────────────┘
```

### 2. Crypto Core

**Encryption Strategy**:

- **Algorithm**: AES-256-GCM for authenticated encryption
- **Key Derivation**: PBKDF2 with salt for password-based keys
- **Key Management**: Separate key store with key rotation support
- **Granularity**: Column-level encryption with shared row-level keys

**Key Features**:

- **Column-Level Encryption**: Different columns can have different encryption keys
- **Key Rotation**: Re-encrypt data with new keys without service interruption
- **Multiple Key Sources**: Support for passwords, key files, and HSM integration

### 3. Access Control Engine

**Role-Based Access Control (RBAC)**:

```go
type AccessPolicy struct {
    Principals []Principal     // Users, roles, or service accounts
    Resources  []Resource      // Columns, tables, or datasets
    Actions    []Action        // Read, write, query, admin
    Conditions []Condition     // Time-based, IP-based, etc.
}
```

**Built-in Roles**:

- `reader`: Can read decrypted data
- `writer`: Can write new encrypted data
- `admin`: Can manage keys and policies
- `auditor`: Can view audit logs only

### 4. Query Processor

**Selective Decryption Query Engine**:

- Parse queries to determine required columns
- Only decrypt columns needed for the operation
- Support for common operations: filter, select, aggregate
- Push-down predicates to minimize decryption overhead

---

## Feature Specifications

### MVP Features (v0.1)

| Feature | Description | Priority |
|---------|-------------|----------|
| Arrow I/O | Read/write Arrow IPC and Feather formats | P0 |
| Column Encryption | Encrypt individual columns with AES-256-GCM | P0 |
| Basic Auth | Password-based authentication | P0 |
| CLI Interface | Command-line tools for common operations | P0 |
| Go SDK | Programmatic API for Go applications | P0 |
| Metadata Management | Store schema and encryption metadata | P0 |

### Enhanced Features (v0.2)

| Feature | Description | Priority |
|---------|-------------|----------|
| RBAC | Role-based access control system | P1 |
| Audit Logging | Track all data access and modifications | P1 |
| Key Rotation | Rotate encryption keys without downtime | P1 |
| Query Optimization | Smart query planning for encrypted data | P1 |
| Streaming Queries | Process large datasets without full decryption | P1 |

### Advanced Features (v0.3+)

| Feature | Description | Priority |
|---------|-------------|----------|
| Multi-User Support | Concurrent access with isolation | P2 |
| Arrow Flight Integration | Network transport for Arrow data | P2 |
| Schema Evolution | Handle schema changes over time | P2 |
| Backup/Recovery | Secure backup and disaster recovery | P2 |
| Performance Monitoring | Metrics and observability | P2 |
| HSM Integration | Hardware security module support | P3 |

---

## API Design

### Command Line Interface

```bash
# Create a new lockbox
lockbox create mydata.lbx --schema schema.json

# Encrypt and store data
lockbox write mydata.lbx --input data.parquet --key-file key.json

# Query encrypted data
lockbox query mydata.lbx --sql "SELECT name, age FROM users WHERE age > 25"

# Manage access policies
lockbox policy add mydata.lbx --user alice --role reader --columns "name,email"

# Rotate encryption keys
lockbox rotate-key mydata.lbx --new-key-file new-key.json
```

### Go SDK

```go
package main

import (
    "github.com/yourusername/lockbox"
)

func main() {
    // Open a lockbox
    lb, err := lockbox.Open("mydata.lbx", lockbox.WithPassword("secret"))
    if err != nil {
        panic(err)
    }
    defer lb.Close()
    
    // Write data
    err = lb.Write(ctx, arrowRecord, lockbox.WithColumns("name", "email"))
    if err != nil {
        panic(err)
    }
    
    // Query data
    result, err := lb.Query(ctx, "SELECT name FROM users WHERE age > 25")
    if err != nil {
        panic(err)
    }
    
    // Process results
    for result.Next() {
        record := result.Record()
        // Process Arrow record
    }
}
```

### HTTP API (Future)

```
POST /lockbox/{id}/query
GET  /lockbox/{id}/schema
PUT  /lockbox/{id}/data
GET  /lockbox/{id}/audit
POST /lockbox/{id}/keys/rotate
```

---

## Security Model

### Threat Model

**Protected Against**:

- Data at rest compromise
- Unauthorized access to sensitive columns
- Insider threats with partial access
- Accidental data exposure

**Assumptions**:

- Application memory is trusted during processing
- Encryption keys are managed securely
- Authentication mechanisms are properly implemented

### Encryption Details

**Column-Level Encryption**:

```
Column Key = PBKDF2(Master Key, Column Salt, 100000 iterations)
Encrypted Data = AES-256-GCM(Column Data, Column Key, Random Nonce)
```

**Key Management**:

- Master keys stored separately from data
- Per-column keys derived from master key
- Support for key rotation without re-encrypting all data
- Integration with external key management systems

### Access Control

**Policy Enforcement**:

- Policies evaluated before any decryption
- Fine-grained column and row-level permissions
- Time-based and condition-based access
- Comprehensive audit trail

---

## Performance Considerations

### Optimization Strategies

1. **Lazy Decryption**: Only decrypt data when explicitly accessed
2. **Batch Processing**: Process multiple records together to amortize crypto overhead
3. **Memory Pooling**: Reuse Arrow memory allocations to reduce GC pressure
4. **Predicate Pushdown**: Apply filters before decryption when possible
5. **Columnar Operations**: Leverage Arrow's columnar format for vectorized operations

### Benchmarking Targets

| Operation | Target Performance | Notes |
|-----------|-------------------|-------|
| Read (encrypted) | 90% of unencrypted Arrow | Selective decryption |
| Write (encrypted) | 85% of unencrypted Arrow | Encryption overhead |
| Query (selective) | 95% of unencrypted Arrow | Column-level filtering |
| Key rotation | < 1s for 1GB file | Background operation |

---

## Implementation Roadmap

### Phase 1: Core Foundation (Months 1-2)

- [ ] Lockbox file format specification
- [ ] Basic Arrow I/O with encryption
- [ ] CLI MVP with essential commands
- [ ] Go SDK core functionality
- [ ] Basic authentication and key management

### Phase 2: Security & Access Control (Months 3-4)

- [ ] RBAC implementation
- [ ] Audit logging system
- [ ] Advanced key management
- [ ] Column-level encryption
- [ ] Query optimization engine

### Phase 3: Advanced Features (Months 5-6)

- [ ] Multi-user concurrent access
- [ ] Arrow Flight integration
- [ ] Schema evolution support
- [ ] Performance monitoring
- [ ] Backup and recovery tools

### Phase 4: Enterprise Features (Months 7+)

- [ ] HSM integration
- [ ] Advanced compliance features
- [ ] Multi-region support
- [ ] Enterprise authentication (LDAP, SAML)
- [ ] Advanced analytics and reporting

---

## High-Value Feature Additions

Based on the concept review, here are additional high-value features that would significantly enhance Lockbox:

### 1. Smart Query Optimization

**Value**: Automatically optimize queries for encrypted data
**Implementation**:

- Analyze query patterns to pre-decrypt frequently accessed columns
- Cache decrypted data temporarily for repeated queries
- Use Arrow's compute kernels for vectorized operations

### 2. Data Lineage and Provenance

**Value**: Track data origins and transformations for compliance
**Implementation**:

- Embed lineage metadata in lockbox headers
- Track data transformations and access patterns
- Integration with data governance tools

### 3. Compression Integration

**Value**: Combine encryption with compression for efficiency
**Implementation**:

- Compress data before encryption
- Support for various compression algorithms (LZ4, ZSTD, Snappy)
- Automatic compression selection based on data characteristics

### 4. Zero-Knowledge Queries

**Value**: Enable queries without exposing raw data
**Implementation**:

- Homomorphic encryption for specific operations
- Secure multi-party computation for aggregations
- Privacy-preserving analytics

### 5. Arrow Flight Encryption

**Value**: Secure network transport for Arrow data
**Implementation**:

- End-to-end encryption for Arrow Flight
- Mutual TLS authentication
- Streaming encryption for large datasets

---

## Conclusion

Lockbox represents a significant advancement in secure data processing, combining the performance benefits of Apache Arrow with enterprise-grade security. The modular architecture allows for incremental adoption while the comprehensive feature set addresses real-world needs for secure, performant data storage and processing.

The project's focus on developer ergonomics, combined with its local-first approach, positions it well for adoption in scenarios where data sovereignty and performance are critical requirements.
