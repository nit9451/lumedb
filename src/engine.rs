// LumeDB Engine
// The main database engine that orchestrates all components:
// MemTable, WAL, SSTables, Indexes, Transactions, and Query execution

use crate::document::{CollectionMeta, Document};
use crate::error::{LumeError, LumeResult};
use crate::index::{IndexDef, IndexManager};
use crate::query::{apply_options, QueryFilter, QueryOptions};
use crate::storage::memtable::MemTable;
use crate::storage::sstable::{SSTable, SSTableWriter};
use crate::transaction::TransactionManager;
use crate::vector::{VectorIndexConfig, VectorIndexManager, VectorSearchResult, DistanceMetric};
use crate::wal::{Wal, WalOperation};
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Database configuration
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Base data directory
    pub data_dir: PathBuf,
    /// Maximum MemTable size before flush (bytes)
    pub memtable_size: u64,
    /// Enable WAL for durability
    pub wal_enabled: bool,
    /// Enable compression for SSTables
    pub compression_enabled: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            data_dir: PathBuf::from("./lumedb_data"),
            memtable_size: 4 * 1024 * 1024, // 4MB
            wal_enabled: true,
            compression_enabled: true,
        }
    }
}

/// Statistics for the database
#[derive(Debug, Clone, Default)]
pub struct EngineStats {
    pub total_documents: u64,
    pub total_collections: u64,
    pub total_indexes: u64,
    pub total_vectors: u64,
    pub memtable_size_bytes: u64,
    pub sstable_count: u64,
    pub sstable_total_bytes: u64,
    pub wal_sequence: u64,
    pub active_transactions: u64,
}

/// The main LumeDB engine
pub struct Engine {
    config: EngineConfig,
    /// Collection name -> MemTable
    memtables: Arc<RwLock<HashMap<String, MemTable>>>,
    /// Collection name -> Vec<SSTable>
    sstables: Arc<RwLock<HashMap<String, Vec<SSTable>>>>,
    /// Collection metadata
    collections: Arc<RwLock<HashMap<String, CollectionMeta>>>,
    /// In-memory document cache (collection:id -> Document)
    doc_cache: Arc<RwLock<HashMap<String, Document>>>,
    /// Write-Ahead Log
    wal: Arc<RwLock<Wal>>,
    /// Index Manager
    index_manager: Arc<IndexManager>,
    /// Vector Index Manager
    vector_manager: Arc<VectorIndexManager>,
    /// Transaction Manager
    txn_manager: Arc<TransactionManager>,
    /// SSTable counter
    sstable_counter: AtomicU64,
}

impl Engine {
    /// Create or open a LumeDB engine
    pub fn new(config: EngineConfig) -> LumeResult<Self> {
        // Create data directories
        fs::create_dir_all(&config.data_dir)?;
        fs::create_dir_all(config.data_dir.join("wal"))?;
        fs::create_dir_all(config.data_dir.join("sstables"))?;

        let wal = Wal::new(&config.data_dir.join("wal"))?;

        let engine = Engine {
            config,
            memtables: Arc::new(RwLock::new(HashMap::new())),
            sstables: Arc::new(RwLock::new(HashMap::new())),
            collections: Arc::new(RwLock::new(HashMap::new())),
            doc_cache: Arc::new(RwLock::new(HashMap::new())),
            wal: Arc::new(RwLock::new(wal)),
            index_manager: Arc::new(IndexManager::new()),
            vector_manager: Arc::new(VectorIndexManager::new()),
            txn_manager: Arc::new(TransactionManager::new()),
            sstable_counter: AtomicU64::new(0),
        };

        // Replay WAL for recovery
        engine.recover()?;

        Ok(engine)
    }

    /// Open with default config
    pub fn open(data_dir: &Path) -> LumeResult<Self> {
        let config = EngineConfig {
            data_dir: data_dir.to_path_buf(),
            ..Default::default()
        };
        Self::new(config)
    }

    /// Recover state from WAL
    fn recover(&self) -> LumeResult<()> {
        let wal = self.wal.read();
        let entries = wal.replay()?;

        for entry in entries {
            match entry.operation {
                WalOperation::CreateCollection { name } => {
                    let mut collections = self.collections.write();
                    if !collections.contains_key(&name) {
                        collections.insert(name.clone(), CollectionMeta::new(&name));
                        self.memtables.write().insert(name, MemTable::new(self.config.memtable_size));
                    }
                }
                WalOperation::Insert {
                    collection,
                    key,
                    value,
                } => {
                    if let Ok(doc) = Document::from_bytes(&value) {
                        let cache_key = format!("{}:{}", collection, key);
                        self.doc_cache.write().insert(cache_key, doc);
                    }
                }
                WalOperation::Update {
                    collection,
                    key,
                    value,
                } => {
                    if let Ok(doc) = Document::from_bytes(&value) {
                        let cache_key = format!("{}:{}", collection, key);
                        self.doc_cache.write().insert(cache_key, doc);
                    }
                }
                WalOperation::Delete { collection, key } => {
                    let cache_key = format!("{}:{}", collection, key);
                    self.doc_cache.write().remove(&cache_key);
                }
                _ => {}
            }
        }

        Ok(())
    }

    // ===== Collection Operations =====

    /// Create a new collection
    pub fn create_collection(&self, name: &str) -> LumeResult<()> {
        let mut collections = self.collections.write();
        if collections.contains_key(name) {
            return Err(LumeError::CollectionAlreadyExists(name.to_string()));
        }

        // WAL log
        self.wal.write().append(
            WalOperation::CreateCollection {
                name: name.to_string(),
            },
            0,
        )?;

        collections.insert(name.to_string(), CollectionMeta::new(name));
        self.memtables
            .write()
            .insert(name.to_string(), MemTable::new(self.config.memtable_size));

        Ok(())
    }

    /// Drop a collection
    pub fn drop_collection(&self, name: &str) -> LumeResult<()> {
        let mut collections = self.collections.write();
        if !collections.contains_key(name) {
            return Err(LumeError::CollectionNotFound(name.to_string()));
        }

        // WAL log
        self.wal.write().append(
            WalOperation::DropCollection {
                name: name.to_string(),
            },
            0,
        )?;

        collections.remove(name);
        self.memtables.write().remove(name);
        self.sstables.write().remove(name);

        // Remove documents from cache
        let prefix = format!("{}:", name);
        self.doc_cache
            .write()
            .retain(|k, _| !k.starts_with(&prefix));

        Ok(())
    }

    /// List all collections
    pub fn list_collections(&self) -> Vec<CollectionMeta> {
        self.collections.read().values().cloned().collect()
    }

    /// Ensure collection exists, create if not
    fn ensure_collection(&self, name: &str) -> LumeResult<()> {
        if !self.collections.read().contains_key(name) {
            self.create_collection(name)?;
        }
        Ok(())
    }

    // ===== Document Operations =====

    /// Insert a document into a collection
    pub fn insert(&self, collection: &str, data: Value) -> LumeResult<Document> {
        self.ensure_collection(collection)?;

        let doc = Document::new(data);
        let doc_bytes = doc.to_bytes();

        // WAL log
        self.wal.write().append(
            WalOperation::Insert {
                collection: collection.to_string(),
                key: doc.id.clone(),
                value: doc_bytes.clone(),
            },
            0,
        )?;

        // Write to MemTable
        let memtables = self.memtables.read();
        if let Some(mt) = memtables.get(collection) {
            let key = format!("{}:{}", collection, doc.id);
            mt.put(key, doc_bytes, self.wal.read().sequence());
        }

        // Index the document
        self.index_manager
            .index_document(collection, &doc.id, &doc.data)?;

        // Auto-index embeddings if vector index exists
        self.vector_manager
            .auto_index_document(collection, &doc.id, &doc.data);

        // Cache the document
        let cache_key = format!("{}:{}", collection, doc.id);
        self.doc_cache.write().insert(cache_key, doc.clone());

        // Update collection stats
        if let Some(meta) = self.collections.write().get_mut(collection) {
            meta.doc_count += 1;
        }

        // Check if memtable needs flushing
        if let Some(mt) = memtables.get(collection) {
            if mt.should_flush() {
                drop(memtables); // Release read lock
                self.flush_memtable(collection)?;
            }
        }

        Ok(doc)
    }

    /// Insert multiple documents
    pub fn insert_many(&self, collection: &str, docs: Vec<Value>) -> LumeResult<Vec<Document>> {
        let mut results = Vec::with_capacity(docs.len());
        for data in docs {
            results.push(self.insert(collection, data)?);
        }
        Ok(results)
    }

    /// Find documents matching a query
    pub fn find(
        &self,
        collection: &str,
        query: &Value,
        options: Option<QueryOptions>,
    ) -> LumeResult<Vec<Document>> {
        if !self.collections.read().contains_key(collection) {
            return Ok(Vec::new());
        }

        let filter = QueryFilter::from_json(query)?;
        let options = options.unwrap_or_default();

        // Collect all documents from cache
        let cache = self.doc_cache.read();
        let prefix = format!("{}:", collection);
        let mut matching_docs: Vec<Document> = cache
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, doc)| doc.clone())
            .filter(|doc| !doc.meta.deleted && !doc.is_expired())
            .filter(|doc| filter.matches(doc))
            .collect();

        // Apply sort, skip, limit
        matching_docs = apply_options(matching_docs, &options);

        // Apply projection
        if let Some(ref proj_fields) = options.projection {
            matching_docs = matching_docs
                .into_iter()
                .map(|doc| {
                    let mut new_data = std::collections::BTreeMap::new();
                    for field in proj_fields {
                        if let Some(val) = doc.data.get(field) {
                            new_data.insert(field.clone(), val.clone());
                        }
                    }
                    Document {
                        id: doc.id,
                        data: new_data,
                        meta: doc.meta,
                    }
                })
                .collect();
        }

        Ok(matching_docs)
    }

    /// Find a single document
    pub fn find_one(&self, collection: &str, query: &Value) -> LumeResult<Option<Document>> {
        let options = QueryOptions {
            limit: Some(1),
            ..Default::default()
        };
        let results = self.find(collection, query, Some(options))?;
        Ok(results.into_iter().next())
    }

    /// Find by document ID
    pub fn find_by_id(&self, collection: &str, id: &str) -> LumeResult<Option<Document>> {
        let cache_key = format!("{}:{}", collection, id);
        let cache = self.doc_cache.read();

        match cache.get(&cache_key) {
            Some(doc) if !doc.meta.deleted && !doc.is_expired() => Ok(Some(doc.clone())),
            _ => Ok(None),
        }
    }

    /// Update documents matching a query
    pub fn update(
        &self,
        collection: &str,
        query: &Value,
        update: &Value,
    ) -> LumeResult<u64> {
        if !self.collections.read().contains_key(collection) {
            return Err(LumeError::CollectionNotFound(collection.to_string()));
        }

        let filter = QueryFilter::from_json(query)?;

        // Find matching documents
        let cache = self.doc_cache.read();
        let prefix = format!("{}:{}", collection, "");
        let matching_ids: Vec<String> = cache
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .filter(|(_, doc)| !doc.meta.deleted && filter.matches(doc))
            .map(|(_, doc)| doc.id.clone())
            .collect();
        drop(cache);

        let mut count = 0u64;

        for doc_id in matching_ids {
            let cache_key = format!("{}:{}", collection, doc_id);

            // Remove from indexes
            if let Some(doc) = self.doc_cache.read().get(&cache_key) {
                self.index_manager
                    .unindex_document(collection, &doc_id, &doc.data);
            }

            // Apply update
            let mut cache = self.doc_cache.write();
            if let Some(doc) = cache.get_mut(&cache_key) {
                doc.apply_update(update);
                let doc_bytes = doc.to_bytes();

                // WAL log
                self.wal.write().append(
                    WalOperation::Update {
                        collection: collection.to_string(),
                        key: doc_id.clone(),
                        value: doc_bytes.clone(),
                    },
                    0,
                )?;

                // Write to MemTable
                let memtables = self.memtables.read();
                if let Some(mt) = memtables.get(collection) {
                    mt.put(cache_key.clone(), doc_bytes, self.wal.read().sequence());
                }

                // Re-index
                self.index_manager
                    .index_document(collection, &doc_id, &doc.data)?;

                count += 1;
            }
        }

        Ok(count)
    }

    /// Delete documents matching a query
    pub fn delete(&self, collection: &str, query: &Value) -> LumeResult<u64> {
        if !self.collections.read().contains_key(collection) {
            return Err(LumeError::CollectionNotFound(collection.to_string()));
        }

        let filter = QueryFilter::from_json(query)?;

        // Find matching documents
        let cache = self.doc_cache.read();
        let prefix = format!("{}:{}", collection, "");
        let matching: Vec<(String, String)> = cache
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .filter(|(_, doc)| !doc.meta.deleted && filter.matches(doc))
            .map(|(_, doc)| (doc.id.clone(), format!("{}:{}", collection, doc.id)))
            .collect();
        drop(cache);

        let mut count = 0u64;

        for (doc_id, cache_key) in matching {
            // Remove from indexes
            if let Some(doc) = self.doc_cache.read().get(&cache_key) {
                self.index_manager
                    .unindex_document(collection, &doc_id, &doc.data);
            }

            // WAL log
            self.wal.write().append(
                WalOperation::Delete {
                    collection: collection.to_string(),
                    key: doc_id.clone(),
                },
                0,
            )?;

            // Mark as tombstone in MemTable
            let memtables = self.memtables.read();
            if let Some(mt) = memtables.get(collection) {
                mt.delete(&cache_key, self.wal.read().sequence());
            }

            // Remove from cache
            self.doc_cache.write().remove(&cache_key);

            // Update stats
            if let Some(meta) = self.collections.write().get_mut(collection) {
                meta.doc_count = meta.doc_count.saturating_sub(1);
            }

            count += 1;
        }

        Ok(count)
    }

    /// Count documents matching a query
    pub fn count(&self, collection: &str, query: &Value) -> LumeResult<u64> {
        let docs = self.find(collection, query, None)?;
        Ok(docs.len() as u64)
    }

    // ===== Index Operations =====

    /// Create an index on a field
    pub fn create_index(
        &self,
        collection: &str,
        field: &str,
        unique: bool,
    ) -> LumeResult<()> {
        self.ensure_collection(collection)?;

        let def = IndexDef {
            name: format!("idx_{}_{}", collection, field),
            field: field.to_string(),
            unique,
            collection: collection.to_string(),
        };

        self.index_manager.create_index(def)?;

        // Index existing documents
        let cache = self.doc_cache.read();
        let prefix = format!("{}:", collection);
        for (key, doc) in cache.iter() {
            if key.starts_with(&prefix) && !doc.meta.deleted {
                self.index_manager
                    .index_document(collection, &doc.id, &doc.data)?;
            }
        }

        // Update stats
        if let Some(meta) = self.collections.write().get_mut(collection) {
            meta.index_count += 1;
        }

        Ok(())
    }

    /// List indexes for a collection
    pub fn list_indexes(&self, collection: &str) -> Vec<IndexDef> {
        self.index_manager.get_indexes(collection)
    }

    // ===== Vector Search Operations =====

    /// Create a vector index on a collection field
    pub fn create_vector_index(
        &self,
        collection: &str,
        field: &str,
        dimensions: usize,
        metric: DistanceMetric,
    ) -> LumeResult<()> {
        self.ensure_collection(collection)?;

        let config = VectorIndexConfig {
            collection: collection.to_string(),
            field: field.to_string(),
            dimensions,
            metric,
        };

        self.vector_manager
            .create_index(config)
            .map_err(|e| LumeError::VectorError(e))?;

        // Index existing documents
        let cache = self.doc_cache.read();
        let prefix = format!("{}:", collection);
        for (key, doc) in cache.iter() {
            if key.starts_with(&prefix) && !doc.meta.deleted {
                self.vector_manager
                    .auto_index_document(collection, &doc.id, &doc.data);
            }
        }

        Ok(())
    }

    /// Search for nearest neighbors using vector similarity
    pub fn vector_search(
        &self,
        collection: &str,
        field: &str,
        query_vector: Vec<f32>,
        k: usize,
        filter: Option<&Value>,
    ) -> LumeResult<Vec<(Document, VectorSearchResult)>> {
        // If there's a pre-filter, get matching doc IDs first
        let filter_ids: Option<Vec<String>> = if let Some(filter_query) = filter {
            let qf = QueryFilter::from_json(filter_query)?;
            let cache = self.doc_cache.read();
            let prefix = format!("{}:", collection);
            let ids: Vec<String> = cache
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .filter(|(_, doc)| !doc.meta.deleted && qf.matches(doc))
                .map(|(_, doc)| doc.id.clone())
                .collect();
            Some(ids)
        } else {
            None
        };

        let results = self
            .vector_manager
            .search(
                collection,
                field,
                &query_vector,
                k,
                filter_ids.as_deref(),
            )
            .map_err(|e| LumeError::VectorError(e))?;

        // Attach full documents to results
        let mut docs_with_scores = Vec::new();
        let cache = self.doc_cache.read();
        for result in results {
            let cache_key = format!("{}:{}", collection, result.doc_id);
            if let Some(doc) = cache.get(&cache_key) {
                docs_with_scores.push((doc.clone(), result));
            }
        }

        Ok(docs_with_scores)
    }

    /// List vector indexes for a collection
    pub fn list_vector_indexes(&self, collection: &str) -> Vec<VectorIndexConfig> {
        self.vector_manager.get_indexes(collection)
    }

    // ===== Transaction Operations =====

    /// Begin a transaction
    pub fn begin_transaction(&self) -> u64 {
        self.txn_manager.begin()
    }

    /// Commit a transaction
    pub fn commit_transaction(&self, txn_id: u64) -> LumeResult<()> {
        let _ops = self.txn_manager.commit(txn_id)?;
        // Operations are already applied in the engine
        Ok(())
    }

    /// Rollback a transaction
    pub fn rollback_transaction(&self, txn_id: u64) -> LumeResult<()> {
        let _ops = self.txn_manager.rollback(txn_id)?;
        // TODO: Undo operations from the cache
        Ok(())
    }

    // ===== SSTable Operations =====

    /// Flush a MemTable to an SSTable
    fn flush_memtable(&self, collection: &str) -> LumeResult<()> {
        let memtables = self.memtables.read();
        let mt = memtables.get(collection).ok_or_else(|| {
            LumeError::CollectionNotFound(collection.to_string())
        })?;

        if mt.is_empty() {
            return Ok(());
        }

        let entries = mt.sorted_entries();
        let sst_id = self.sstable_counter.fetch_add(1, Ordering::SeqCst);
        let sst_path = self
            .config
            .data_dir
            .join("sstables")
            .join(format!("{}_{:08}.sst", collection, sst_id));

        let mut writer = SSTableWriter::new(&sst_path, 0);

        for (key, entry) in &entries {
            writer.add(
                key.clone(),
                entry.value.clone(),
                entry.sequence,
                entry.timestamp,
            );
        }

        let info = writer.finish()?;

        // Open the SSTable for reading
        let sst = SSTable::open(&info.path)?;

        // Add to sstables list
        let mut sstables = self.sstables.write();
        sstables
            .entry(collection.to_string())
            .or_insert_with(Vec::new)
            .push(sst);

        // Clear the MemTable
        mt.clear();

        Ok(())
    }

    // ===== Statistics =====

    /// Get database statistics
    pub fn stats(&self) -> EngineStats {
        let collections = self.collections.read();
        let sstables = self.sstables.read();

        let total_docs = collections.values().map(|m| m.doc_count).sum();
        let total_indexes = collections.values().map(|m| m.index_count).sum();

        let mut sst_count = 0u64;
        let mut sst_bytes = 0u64;
        for tables in sstables.values() {
            for sst in tables {
                sst_count += 1;
                sst_bytes += sst.info.file_size;
            }
        }

        let mut mt_size = 0u64;
        for mt in self.memtables.read().values() {
            mt_size += mt.size();
        }

        let mut total_vectors = 0u64;
        for coll in collections.keys() {
            total_vectors += self.vector_manager.vector_count(coll) as u64;
        }

        EngineStats {
            total_documents: total_docs,
            total_collections: collections.len() as u64,
            total_indexes,
            total_vectors,
            memtable_size_bytes: mt_size,
            sstable_count: sst_count,
            sstable_total_bytes: sst_bytes,
            wal_sequence: self.wal.read().sequence(),
            active_transactions: self.txn_manager.active_count() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_engine() -> Engine {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data_engine");
        let _ = fs::remove_dir_all(&dir);
        let config = EngineConfig {
            data_dir: dir,
            memtable_size: 1024 * 1024,
            wal_enabled: true,
            compression_enabled: true,
        };
        Engine::new(config).unwrap()
    }

    #[test]
    fn test_insert_and_find() {
        let engine = test_engine();

        engine
            .insert("users", json!({"name": "Alice", "age": 30}))
            .unwrap();
        engine
            .insert("users", json!({"name": "Bob", "age": 25}))
            .unwrap();
        engine
            .insert("users", json!({"name": "Charlie", "age": 35}))
            .unwrap();

        // Find all
        let all = engine.find("users", &json!({}), None).unwrap();
        assert_eq!(all.len(), 3);

        // Find by query
        let adults = engine
            .find("users", &json!({"age": {"$gte": 30}}), None)
            .unwrap();
        assert_eq!(adults.len(), 2);

        // Find one
        let alice = engine.find_one("users", &json!({"name": "Alice"})).unwrap();
        assert!(alice.is_some());
        assert_eq!(alice.unwrap().data.get("name").unwrap(), &json!("Alice"));

        let _ = fs::remove_dir_all(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data_engine"),
        );
    }

    #[test]
    fn test_update_and_delete() {
        let engine = test_engine();

        engine
            .insert("users", json!({"name": "Alice", "age": 30}))
            .unwrap();

        // Update
        let count = engine
            .update(
                "users",
                &json!({"name": "Alice"}),
                &json!({"$set": {"age": 31}}),
            )
            .unwrap();
        assert_eq!(count, 1);

        let alice = engine.find_one("users", &json!({"name": "Alice"})).unwrap().unwrap();
        assert_eq!(alice.data.get("age").unwrap(), &json!(31));

        // Delete
        let count = engine.delete("users", &json!({"name": "Alice"})).unwrap();
        assert_eq!(count, 1);

        let result = engine.find_one("users", &json!({"name": "Alice"})).unwrap();
        assert!(result.is_none());

        let _ = fs::remove_dir_all(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data_engine"),
        );
    }

    #[test]
    fn test_collections() {
        let engine = test_engine();

        engine.create_collection("users").unwrap();
        engine.create_collection("posts").unwrap();

        let collections = engine.list_collections();
        assert_eq!(collections.len(), 2);

        engine.drop_collection("posts").unwrap();
        let collections = engine.list_collections();
        assert_eq!(collections.len(), 1);

        let _ = fs::remove_dir_all(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data_engine"),
        );
    }
}
