# LumeDB Learning Guide

Welcome to LumeDB! This guide will take you from absolute beginner to advanced user. By the end of this document, you will understand how LumeDB works under the hood and how to use it for standard document storage and AI vector search.

---

## 1. What is LumeDB?
LumeDB is a **high-performance, AI-native document database** written from scratch in Rust. 

Traditionally, developers building AI applications (like RAG - Retrieval-Augmented Generation) have to use **two** databases:
1. A standard database (like MongoDB or Postgres) to store user data and text.
2. A vector database (like Pinecone or Qdrant) to store AI embeddings for semantic search.

**LumeDB solves this by combining both into a single engine.** It stores native JSON documents *and* high-dimensional vectors, allowing you to seamlessly filter data using standard operators (e.g., "age > 25") while simultaneously ranking results by vector similarity (cosine distance).

## 2. Core Concepts
- **Collections:** Similar to tables in SQL or collections in MongoDB. A collection holds many documents.
- **Documents:** JSON objects containing your data. Every document automatically gets a unique `_id`.
- **Vector Index:** A mathematical map that allows LumeDB to quickly find similar embeddings without scanning every single document.
- **LSM-Tree Storage:** LumeDB uses an Append-Only architecture (Log-Structured Merge Tree). It writes data extremely quickly to an in-memory `MemTable`, which is eventually flushed to disk as an immutable `SSTable`. 

---

## 3. Getting Started (The CLI)

LumeDB communicates over TCP using raw JSON strings. The easiest way to interact with it is using the built-in Lume CLI Repl.

When you start the CLI, you will see the `lume>` prompt.

### 3.1 Basic CRUD Operations

**Create a Collection**
```json
{
  "action": "createCollection",
  "name": "users"
}
```

**Insert a Document**
```json
{
  "action": "insert",
  "collection": "users",
  "document": {
    "name": "Alice",
    "age": 30,
    "role": "engineer"
  }
}
```

**Find Documents**
You can use MongoDB-like operators to filter data (`$eq`, `$gt`, `$gte`, `$in`, etc).
```json
{
  "action": "find",
  "collection": "users",
  "query": {
    "age": { "$gte": 25 }
  }
}
```

**Update a Document**
```json
{
  "action": "update",
  "collection": "users",
  "query": { "name": "Alice" },
  "update": {
    "$set": { "role": "lead engineer" },
    "$inc": { "age": 1 }
  }
}
```

---

## 4. Vector Search (AI Capabilities)

To use LumeDB for AI, you first need to generate an "embedding" (an array of floats) using an AI model like OpenAI `text-embedding-3-small`. 

### 4.1 Create a Vector Index
Before inserting vectors, tell LumeDB which field will hold the vector, how many dimensions it has, and how to measure distance (cosine, euclidean, or dot product).

```json
{
  "action": "createVectorIndex",
  "collection": "articles",
  "field": "embedding",
  "dimensions": 1536,
  "metric": "cosine"
}
```

### 4.2 Insert Documents with Embeddings
```json
{
  "action": "insert",
  "collection": "articles",
  "document": {
    "title": "Introduction to Rust",
    "category": "programming",
    "embedding": [0.12, -0.45, 0.88, ...] 
  }
}
```

### 4.3 Filtered Vector Search
This is LumeDB's superpower. You can pre-filter documents before running the heavy math for vector similarity.
```json
{
  "action": "vectorSearch",
  "collection": "articles",
  "vector": [0.11, -0.42, 0.89, ...],
  "k": 5,
  "filter": {
    "category": "programming"
  }
}
```
LumeDB will *only* search for similar vectors within articles that have the category "programming", returning the top 5 closest matches!

---

## 5. Administrative Commands
You can check the health and stats of your database at any time:

**Get Server Stats:**
```json
{
  "action": "stats"
}
```

**List Collections:**
```json
{
  "action": "listCollections"
}
```

## Next Steps
Now that you know the commands, try opening **LumeDB Studio** (the web GUI) and pasting these JSON snippets into the Query Editor!
