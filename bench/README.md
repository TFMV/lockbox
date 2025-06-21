# Benchmarks

This directory contains performance benchmarks for LockBox.

## Running

Execute the benchmarks with:

```bash
go test ./bench -bench=. -benchmem
```

The benchmarks create temporary lockbox files and exercise large record
writes and reads (100k rows) to gauge performance with sizable datasets.
