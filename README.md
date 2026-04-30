# 🌀 LumeDB

A high-performance NoSQL document database built from scratch in Rust.

**ACID Compliant** • **LSM-Tree Storage** • **Bloom Filters** • **LZ4 Compression** • **B-Tree Indexes** • **MVCC Transactions**

## Quick Start

```bash
source "$HOME/.cargo/env"
cargo build --release

# Terminal 1 — Start server
cargo run --release --bin lumedb-server

# Terminal 2 — Start CLI
cargo run --release --bin vortex-cli
```

## Usage

```
vortex> db.users.insert({"name": "Alice", "age": 30, "city": "NYC"})
vortex> db.users.find({"age": {"$gte": 25}})
vortex> db.users.update({"name": "Alice"}, {"$set": {"age": 31}})
vortex> db.users.delete({"name": "Alice"})
vortex> stats
```

## Features

- **Document Model** — Flexible JSON documents with auto-generated UUIDs
- **Query DSL** — `$eq` `$gt` `$lt` `$in` `$or` `$and` `$not` `$exists` and more
- **Update Operators** — `$set` `$unset` `$inc` `$push` `$pull`
- **Secondary Indexes** — B-Tree indexes with unique constraints
- **ACID Transactions** — MVCC with snapshot isolation
- **Durability** — Write-Ahead Log with CRC32 checksums
- **Compression** — LZ4 on all SSTables
- **Bloom Filters** — O(1) negative key lookups (1% FP rate)
- **TCP Server** — JSON wire protocol on port 7070
- **Interactive CLI** — Colorized REPL with MongoDB-style syntax

## Docs

See [DEVELOPER_GUIDE.md](./DEVELOPER_GUIDE.md) for full API reference, architecture, and internals.

## Tests

```bash
cargo test    # 24 tests across all modules
bash demo.sh  # Integration test against running server
```

## License

MIT
