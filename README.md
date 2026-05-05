# 🌀 LumeDB

A high-performance, AI-native NoSQL document database built from scratch in Rust.

**Vector Search** • **ACID Compliant** • **LSM-Tree Storage** • **Bloom Filters** • **MVCC Transactions**

![LumeDB Banner](https://img.shields.io/badge/Written_in-Rust-FA4F28?style=for-the-badge&logo=rust)
![Docker Ready](https://img.shields.io/badge/Docker-Ready-2496ED?style=for-the-badge&logo=docker)
![Frontend](https://img.shields.io/badge/LumeDB_Studio-Next.js-black?style=for-the-badge&logo=next.js)

LumeDB combines traditional JSON document storage with high-dimensional AI Vector Embeddings. It is designed to be the only database you need when building modern Retrieval-Augmented Generation (RAG) applications.

<p align="center">
  <i>(📸 Place a screenshot of your LumeDB Studio UI here: <code>docs/studio-screenshot.png</code>)</i>
  <br/>
  <img src="docs/studio-screenshot.png" width="800" alt="LumeDB Studio Screenshot" onerror="this.style.display='none'">
</p>

## ✨ Features

- **Hybrid AI Search** — Filter documents using standard operators and rank by vector cosine similarity.
- **Document Model** — Flexible JSON documents with auto-generated UUIDs.
- **Query DSL** — `$eq`, `$gt`, `$lt`, `$in`, `$or`, `$and`, and more.
- **ACID Transactions** — MVCC with snapshot isolation.
- **Durability** — Write-Ahead Log (WAL) with CRC32 checksums.
- **LumeDB Studio** — A beautiful, interactive web-based GUI for data management.
- **Dockerized Deployment** — Production-ready containerized environment.

---

## 🚀 Quick Start (Docker)

The fastest way to get started is using the provided setup script, which pulls the Docker image and starts the server on port `7070`.

```bash
chmod +x setup-lumedb.sh
./setup-lumedb.sh
```

## 💻 LumeDB Studio (Web GUI)

We have a dedicated Next.js graphical interface to manage your database and visualize vector embeddings.

**Live Demo:** 🌍 [https://lumedb-8ivc.vercel.app/](https://lumedb-8ivc.vercel.app/)

**Run Locally:**
If you prefer to run the Studio locally on your own machine:
```bash
cd lumedb-studio
npm install
npm run dev
```
Navigate to `http://localhost:3000` and connect to your local engine at `127.0.0.1:7070`.

---

## 📖 Usage Examples

LumeDB communicates over TCP using JSON. You can execute these in LumeDB Studio's Query Editor or via a TCP client.

### Standard CRUD
```json
{
  "action": "insert",
  "collection": "users",
  "document": { "name": "Alice", "age": 30, "role": "engineer" }
}
```

```json
{
  "action": "find",
  "collection": "users",
  "query": { "age": { "$gte": 25 } }
}
```

### AI Vector Search
Create a vector index on a collection to enable similarity search:
```json
{
  "action": "createVectorIndex",
  "collection": "articles",
  "field": "embedding",
  "dimensions": 1536,
  "metric": "cosine"
}
```

Filter by category AND find the closest vector simultaneously:
```json
{
  "action": "vectorSearch",
  "collection": "articles",
  "vector": [0.11, -0.42, 0.89, 0.12],
  "k": 5,
  "filter": { "category": "programming" }
}
```

---

## 📚 Documentation
- [Learning Guide](./LEARNING_LUMEDB.md) - Deep dive into usage and vector search.
- [Developer Guide](./DEVELOPER_GUIDE.md) - API reference, internals, and architecture.

## 🛠️ Building from Source (Local Engine)

```bash
source "$HOME/.cargo/env"
cargo build --release

# Start server
cargo run --release --bin lumedb-server
```

## License
MIT
