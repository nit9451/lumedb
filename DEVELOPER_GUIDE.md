# 🌀 LumeDB Developer Guide

> **Version**: 0.1.0 | **Language**: Rust | **License**: MIT

A high-performance NoSQL document database with ACID transactions, LSM-Tree storage, and a MongoDB-style query DSL.

---

## Table of Contents

1. [Quick Start](#-quick-start)
2. [Architecture](#-architecture)
3. [API Reference](#-api-reference)
4. [Query Operators](#-query-operators)
5. [Update Operators](#-update-operators)
6. [Indexing](#-indexing)
7. [Transactions](#-transactions)
8. [Storage Internals](#-storage-internals)
9. [Configuration](#%EF%B8%8F-configuration)
10. [Testing](#-testing)
11. [Extending LumeDB](#-extending-lumedb)

---

## 🚀 Quick Start

### Prerequisites
- Rust 1.70+ (`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)

### Build & Run

```bash
cd lumedb
source "$HOME/.cargo/env"

# Build release binaries
cargo build --release

# Start the server (Terminal 1)
cargo run --release --bin lumedb-server

# Start the CLI (Terminal 2)
cargo run --release --bin vortex-cli
```

### Server Options

```bash
lumedb-server [OPTIONS]
  --host <HOST>        Listen address (default: 127.0.0.1)
  --port <PORT>        Listen port (default: 7070)
  --data-dir <PATH>    Data directory (default: ./lumedb_data)
```

### First Commands

```
vortex> db.users.insert({"name": "Alice", "age": 30})
vortex> db.users.find({})
vortex> stats
```

---

## 🏗 Architecture

```
┌──────────────────────────────────────────────────────┐
│                   CLIENT LAYER                        │
│   vortex-cli (REPL)  |  TCP Client  |  Raw JSON      │
└────────────────────────┬─────────────────────────────┘
                         │ TCP :7070 (JSON wire protocol)
┌────────────────────────▼─────────────────────────────┐
│                  SERVER (server.rs)                    │
│   Parse JSON command → Route to Engine → Return JSON  │
└────────────────────────┬─────────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────────┐
│                  ENGINE (engine.rs)                    │
│   Orchestrates all components. Exposes CRUD API.      │
│                                                       │
│  ┌─────────────┐  ┌────────────┐  ┌───────────────┐  │
│  │ Query Engine│  │ Index Mgr  │  │  Transaction  │  │
│  │ (query.rs)  │  │ (index.rs) │  │  (txn.rs)     │  │
│  └──────┬──────┘  └─────┬──────┘  └───────┬───────┘  │
│         └───────────────┼─────────────────┘           │
│                         │                             │
│  ┌──────────────────────▼────────────────────────┐    │
│  │           STORAGE ENGINE (LSM-Tree)           │    │
│  │                                               │    │
│  │  Write Path:  Client → WAL → MemTable         │    │
│  │  Flush:       MemTable → SSTable (on disk)     │    │
│  │  Read Path:   MemTable → SSTables (newest 1st) │    │
│  │                                               │    │
│  │  ┌──────────┐ ┌──────┐ ┌────────────────────┐│    │
│  │  │ MemTable │ │ WAL  │ │ SSTables (L0→L6)   ││    │
│  │  │(BTreeMap)│ │(disk)│ │ + Bloom Filters    ││    │
│  │  └──────────┘ └──────┘ └────────────────────┘│    │
│  └───────────────────────────────────────────────┘    │
└───────────────────────────────────────────────────────┘
```

### Source Files

| File | Purpose | Lines |
|---|---|---|
| `src/main.rs` | Server entry point, CLI args | 63 |
| `src/cli.rs` | Interactive REPL client | 448 |
| `src/lib.rs` | Module declarations | 12 |
| `src/engine.rs` | Core database engine | 742 |
| `src/document.rs` | Document model & operators | 340 |
| `src/query.rs` | Query parser & evaluator | 435 |
| `src/index.rs` | B-Tree index manager | 276 |
| `src/transaction.rs` | MVCC transaction manager | 217 |
| `src/server.rs` | TCP server & command router | 333 |
| `src/wal.rs` | Write-Ahead Log | 343 |
| `src/storage/memtable.rs` | In-memory sorted store | 173 |
| `src/storage/sstable.rs` | On-disk sorted tables + bloom | 474 |
| `src/error.rs` | Error types | 50 |

---

## 📘 API Reference

### Wire Protocol

LumeDB uses a **line-delimited JSON** protocol over TCP. Each command is a JSON object sent as one line, and each response is a JSON object returned as one line.

```
Client → Server:  {"action":"find","collection":"users","query":{"age":30}}\n
Server → Client:  {"status":"ok","count":1,"documents":[...]}\n
```

### CLI Syntax

The CLI supports two modes:

**MongoDB-style:**
```
db.<collection>.<method>(<json>)
```

**Raw JSON:**
```json
{"action":"<action>","collection":"<name>", ...}
```

---

### Collection Operations

#### Create Collection
```json
{"action": "createCollection", "collection": "users"}
```
Response: `{"status":"ok","message":"Collection 'users' created"}`

#### Drop Collection
```json
{"action": "dropCollection", "collection": "users"}
```

#### List Collections
```json
{"action": "listCollections"}
```
Response:
```json
{
  "status": "ok",
  "collections": [
    {"name": "users", "docCount": 5, "indexCount": 1}
  ]
}
```

---

### Document Operations

#### Insert One
```json
{
  "action": "insert",
  "collection": "users",
  "document": {"name": "Alice", "age": 30, "tags": ["dev"]}
}
```
Response:
```json
{
  "status": "ok",
  "insertedId": "uuid-here",
  "document": {"_id": "uuid-here", "name": "Alice", "age": 30}
}
```
CLI: `db.users.insert({"name": "Alice", "age": 30})`

#### Insert Many
```json
{
  "action": "insertMany",
  "collection": "users",
  "documents": [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25}
  ]
}
```

#### Find (with options)
```json
{
  "action": "find",
  "collection": "users",
  "query": {"age": {"$gte": 25}},
  "sort": {"age": 1},
  "skip": 0,
  "limit": 10,
  "projection": ["name", "age"]
}
```
- `sort`: `1` = ascending, `-1` = descending
- `projection`: array of field names to return
- `skip` / `limit`: pagination

CLI: `db.users.find({"age": {"$gte": 25}})`

#### Find One
```json
{"action": "findOne", "collection": "users", "query": {"name": "Alice"}}
```
CLI: `db.users.findOne({"name": "Alice"})`

#### Update
```json
{
  "action": "update",
  "collection": "users",
  "query": {"name": "Alice"},
  "update": {"$set": {"age": 31}, "$inc": {"loginCount": 1}}
}
```
Response: `{"status":"ok","modifiedCount":1}`

#### Delete
```json
{
  "action": "delete",
  "collection": "users",
  "query": {"age": {"$lt": 18}}
}
```
Response: `{"status":"ok","deletedCount":3}`

CLI: `db.users.delete({"age": {"$lt": 18}})`

#### Count
```json
{"action": "count", "collection": "users", "query": {"role": "engineer"}}
```
CLI: `db.users.count({"role": "engineer"})`

---

## 🔍 Query Operators

### Comparison

| Operator | Description | Example |
|---|---|---|
| `$eq` | Equal (implicit) | `{"age": 30}` or `{"age": {"$eq": 30}}` |
| `$ne` | Not equal | `{"status": {"$ne": "inactive"}}` |
| `$gt` | Greater than | `{"age": {"$gt": 18}}` |
| `$gte` | Greater or equal | `{"price": {"$gte": 9.99}}` |
| `$lt` | Less than | `{"age": {"$lt": 65}}` |
| `$lte` | Less or equal | `{"score": {"$lte": 100}}` |

### Array

| Operator | Description | Example |
|---|---|---|
| `$in` | Value in array | `{"city": {"$in": ["NYC", "LA"]}}` |
| `$nin` | Value not in array | `{"status": {"$nin": ["banned"]}}` |

### Element

| Operator | Description | Example |
|---|---|---|
| `$exists` | Field exists | `{"email": {"$exists": true}}` |

### Logical

| Operator | Description | Example |
|---|---|---|
| `$and` | All conditions match | `{"$and": [{"age": {"$gte": 18}}, {"age": {"$lt": 65}}]}` |
| `$or` | Any condition matches | `{"$or": [{"city": "NYC"}, {"city": "LA"}]}` |
| `$not` | Negate a condition | `{"$not": {"status": "banned"}}` |

### Combining Operators

```json
{
  "age": {"$gte": 18, "$lte": 65},
  "city": {"$in": ["NYC", "SF", "LA"]},
  "$or": [
    {"role": "engineer"},
    {"role": "manager"}
  ]
}
```

---

## ✏️ Update Operators

| Operator | Description | Example |
|---|---|---|
| `$set` | Set field values | `{"$set": {"name": "Bob", "active": true}}` |
| `$unset` | Remove fields | `{"$unset": {"tempField": ""}}` |
| `$inc` | Increment number | `{"$inc": {"views": 1, "score": -5}}` |
| `$push` | Append to array | `{"$push": {"tags": "featured"}}` |
| `$pull` | Remove from array | `{"$pull": {"tags": "deprecated"}}` |

Combine multiple operators in one update:
```json
{
  "$set": {"status": "premium"},
  "$inc": {"loginCount": 1},
  "$push": {"roles": "admin"}
}
```

---

## 📇 Indexing

### Create Index
```json
{"action": "createIndex", "collection": "users", "field": "email", "unique": true}
{"action": "createIndex", "collection": "users", "field": "age", "unique": false}
```

### List Indexes
```json
{"action": "listIndexes", "collection": "users"}
```

### How Indexes Work
- **B-Tree** structure maps field values → document IDs
- **Unique indexes** reject duplicate values at insert time
- Numbers use **IEEE 754 sortable encoding** for correct ordering
- Indexes are updated automatically on insert/update/delete

### When to Use Indexes
- Fields used frequently in `$eq`, `$gt`, `$lt` queries
- Fields with unique constraints (email, username)
- Fields used in sorting

---

## 🔒 Transactions

### Begin / Commit / Rollback

```json
{"action": "beginTransaction"}
→ {"status": "ok", "transactionId": 1}

{"action": "commitTransaction", "transactionId": 1}
→ {"status": "ok"}

{"action": "rollbackTransaction", "transactionId": 1}
→ {"status": "ok"}
```

### MVCC Implementation

- Each transaction gets a **snapshot version** at start time
- Operations are buffered until commit
- On commit: all operations apply atomically
- On rollback: all operations are discarded
- **Snapshot isolation** prevents dirty reads

---

## 💾 Storage Internals

### Write Path
```
Client Insert
    → WAL (append entry with CRC32 checksum)
    → MemTable (in-memory BTreeMap)
    → if MemTable > 4MB → flush to SSTable on disk
```

### Read Path
```
Client Query
    → Search MemTable (newest data)
    → Search SSTables L0 → L6 (newest first)
    → Bloom filter rejects non-existent keys in O(1)
    → Merge results, return to client
```

### WAL (Write-Ahead Log)
- **Format**: `[4-byte length][4-byte CRC32][data bytes]`
- **Segmented**: Rotates at 64MB per segment file
- **Crash recovery**: Replays all entries on startup
- **Location**: `<data_dir>/wal/wal_00000000.log`

### MemTable
- In-memory **BTreeMap** (sorted by key)
- Thread-safe via `parking_lot::RwLock`
- Flushes to SSTable when size exceeds threshold (default 4MB)
- Tombstones (value = None) represent deletes

### SSTable Format
```
┌─────────────────────────────────────────┐
│ Header Length (4 bytes)                  │
├─────────────────────────────────────────┤
│ Header (bincode serialized)             │
│   magic: "VORTEXST"                     │
│   version, entry_count, min/max key     │
│   offsets for index & bloom sections    │
├─────────────────────────────────────────┤
│ Data Blocks (LZ4 compressed)            │
│   Block 0: [entry, entry, ...]          │
│   Block 1: [entry, entry, ...]          │
│   ...                                   │
├─────────────────────────────────────────┤
│ Sparse Index (bincode serialized)       │
│   [first_key → block_offset] × N       │
├─────────────────────────────────────────┤
│ Bloom Filter (bincode serialized)       │
│   Bit array + hash count               │
└─────────────────────────────────────────┘
```

### Bloom Filter
- **False positive rate**: 1%
- **Hash function**: xxHash3 with multiple seeds
- Used to skip SSTables that definitely don't contain a key
- Eliminates unnecessary disk reads

---

## ⚙️ Configuration

### EngineConfig (in `engine.rs`)

| Parameter | Default | Description |
|---|---|---|
| `data_dir` | `./lumedb_data` | Base directory for all data files |
| `memtable_size` | 4 MB | Flush threshold for MemTable |
| `wal_enabled` | true | Enable write-ahead logging |
| `compression_enabled` | true | LZ4 compress SSTables |

### Data Directory Structure

```
lumedb_data/
├── wal/
│   ├── wal_00000000.log
│   └── wal_00000001.log
└── sstables/
    ├── users_00000000.sst
    └── products_00000001.sst
```

---

## 🧪 Testing

### Run All Tests
```bash
cargo test
```

### Run Specific Module Tests
```bash
cargo test document::tests
cargo test query::tests
cargo test storage::memtable::tests
cargo test storage::sstable::tests
cargo test engine::tests
cargo test wal::tests
cargo test index::tests
cargo test transaction::tests
```

### Test Coverage Summary

| Module | Tests | What's Tested |
|---|---|---|
| document | 5 | creation, update, $inc, nested fields, serialization |
| query | 4 | $eq, comparisons, $in, $and/$or, sort+limit |
| memtable | 4 | put/get, tombstones, sorted iteration, flush trigger |
| sstable | 2 | bloom filter, write+read+lookup |
| engine | 3 | insert+find, update+delete, collections |
| wal | 1 | write + crash recovery replay |
| index | 2 | B-Tree operations, unique constraint |
| transaction | 2 | begin+commit, rollback |

### Integration Test (against running server)
```bash
bash demo.sh
```

---

## 🔧 Extending LumeDB

### Adding a New Query Operator

1. Add variant to `QueryFilter` enum in `src/query.rs`:
```rust
// In the QueryFilter enum
Contains { field: String, value: String },
```

2. Add parsing in `parse_field_filter()`:
```rust
"$contains" => filters.push(QueryFilter::Contains {
    field: field.to_string(),
    value: val.as_str().unwrap_or("").to_string(),
}),
```

3. Add evaluation in `matches()`:
```rust
QueryFilter::Contains { field, value } => {
    doc.get_field_value(field)
        .and_then(|v| v.as_str().map(|s| s.contains(value.as_str())))
        .unwrap_or(false)
}
```

### Adding a New Server Action

1. Add handler in `process_command()` in `src/server.rs`:
```rust
"myAction" => {
    // Your logic here
    json!({"status": "ok", "result": "..."})
}
```

### Adding a New Update Operator

Add handling in `Document::apply_update()` in `src/document.rs`:
```rust
// Handle $rename operator
if let Some(Value::Object(rename_fields)) = updates.get("$rename") {
    for (old_key, new_key) in rename_fields {
        if let Some(val) = self.data.remove(old_key) {
            self.data.insert(new_key.as_str().unwrap().to_string(), val);
        }
    }
}
```

---

## 📊 CLI Quick Reference

| Command | Description |
|---|---|
| `db.<coll>.insert({...})` | Insert document |
| `db.<coll>.find({...})` | Find documents |
| `db.<coll>.findOne({...})` | Find one document |
| `db.<coll>.delete({...})` | Delete documents |
| `db.<coll>.count({...})` | Count documents |
| `db.<coll>.drop()` | Drop collection |
| `use <collection>` | Switch default collection |
| `collections` | List all collections |
| `stats` | Database statistics |
| `ping` | Test connection |
| `help` | Show help |
| `clear` | Clear screen |
| `quit` | Exit CLI |

---

## 🐛 Troubleshooting

| Problem | Solution |
|---|---|
| `Connection refused` | Make sure server is running: `cargo run --release --bin lumedb-server` |
| `command not found: cargo` | Run `source "$HOME/.cargo/env"` |
| Port already in use | Use `--port 7071` or kill existing process: `lsof -i :7070` |
| Data corruption | Delete `lumedb_data/` directory and restart |
| Slow builds | Use `cargo build --release` for optimized binary |

---

*Built with ❤️ in Rust — LumeDB v0.1.0*
