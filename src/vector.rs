// LumeDB Vector Search Engine
// Enables AI-native RAG workflows by storing embeddings alongside documents
// Supports cosine similarity, euclidean distance, and dot product metrics

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use hnsw_rs::prelude::*;

/// Distance metric for vector comparisons
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum DistanceMetric {
    Cosine,
    Euclidean,
    DotProduct,
}

/// A single vector entry linked to a document
#[derive(Debug, Clone)]
struct VectorEntry {
    doc_id: String,
    vector: Vec<f32>,
    norm: f32, // pre-computed L2 norm for cosine similarity
}

/// Result from a vector similarity search
#[derive(Debug, Clone, Serialize)]
pub struct VectorSearchResult {
    pub doc_id: String,
    pub score: f64,
    pub distance: f64,
}

/// Configuration for a vector index on a collection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexConfig {
    pub collection: String,
    pub field: String,
    pub dimensions: usize,
    pub metric: DistanceMetric,
}

/// HNSW wrapper for fast approximate vector search
pub struct HnswVectorIndex {
    pub config: VectorIndexConfig,
    hnsw_cosine: Option<Hnsw<'static, f32, DistCosine>>,
    hnsw_l2: Option<Hnsw<'static, f32, DistL2>>,
    hnsw_dot: Option<Hnsw<'static, f32, DistDot>>,
    doc_to_id: HashMap<String, usize>,
    id_to_doc: HashMap<usize, String>,
    deleted_ids: HashSet<usize>,
    next_id: usize,
    count: usize,
}

impl HnswVectorIndex {
    fn new(config: VectorIndexConfig) -> Self {
        let max_nb_connection = 16;
        let max_elements = 1000000;
        let max_layer = 16;
        let ef_construction = 200;

        let mut hnsw_cosine = None;
        let mut hnsw_l2 = None;
        let mut hnsw_dot = None;

        match config.metric {
            DistanceMetric::Cosine => {
                hnsw_cosine = Some(Hnsw::<f32, DistCosine>::new(max_nb_connection, max_elements, max_layer, ef_construction, DistCosine));
            }
            DistanceMetric::Euclidean => {
                hnsw_l2 = Some(Hnsw::<f32, DistL2>::new(max_nb_connection, max_elements, max_layer, ef_construction, DistL2));
            }
            DistanceMetric::DotProduct => {
                hnsw_dot = Some(Hnsw::<f32, DistDot>::new(max_nb_connection, max_elements, max_layer, ef_construction, DistDot));
            }
        }

        Self {
            config,
            hnsw_cosine,
            hnsw_l2,
            hnsw_dot,
            doc_to_id: HashMap::new(),
            id_to_doc: HashMap::new(),
            deleted_ids: HashSet::new(),
            next_id: 1,
            count: 0,
        }
    }

    fn insert(&mut self, doc_id: &str, vector: Vec<f32>) {
        let data_id = if let Some(&id) = self.doc_to_id.get(doc_id) {
            self.deleted_ids.remove(&id); // un-delete if it was deleted
            id
        } else {
            let id = self.next_id;
            self.next_id += 1;
            self.doc_to_id.insert(doc_id.to_string(), id);
            self.id_to_doc.insert(id, doc_id.to_string());
            self.count += 1;
            id
        };

        if let Some(h) = &mut self.hnsw_cosine { h.insert((&vector, data_id)); }
        if let Some(h) = &mut self.hnsw_l2 { h.insert((&vector, data_id)); }
        if let Some(h) = &mut self.hnsw_dot { h.insert((&vector, data_id)); }
    }

    fn remove(&mut self, doc_id: &str) {
        if let Some(&id) = self.doc_to_id.get(doc_id) {
            self.deleted_ids.insert(id);
            self.doc_to_id.remove(doc_id);
            self.count = self.count.saturating_sub(1);
            // hnsw_rs does not support hard delete efficiently, so we just track it in deleted_ids
        }
    }

    fn search(&self, query: &[f32], k: usize, filter_ids: Option<&[String]>) -> Vec<VectorSearchResult> {
        let ef_search = (k * 10).max(100); // over-fetch for post-filtering
        
        let neighbors = if let Some(h) = &self.hnsw_cosine {
            h.search(query, ef_search, ef_search)
        } else if let Some(h) = &self.hnsw_l2 {
            h.search(query, ef_search, ef_search)
        } else if let Some(h) = &self.hnsw_dot {
            h.search(query, ef_search, ef_search)
        } else {
            vec![]
        };

        let mut scored = Vec::new();
        for neighbor in neighbors {
            if self.deleted_ids.contains(&neighbor.d_id) {
                continue;
            }
            if let Some(doc_id) = self.id_to_doc.get(&neighbor.d_id) {
                if let Some(filters) = filter_ids {
                    if !filters.contains(doc_id) {
                        continue;
                    }
                }

                // Convert distance to similarity score
                let dist = neighbor.distance;
                let score = match self.config.metric {
                    DistanceMetric::Cosine => 1.0 - (dist as f64), // dist is 1 - cos_sim
                    DistanceMetric::Euclidean => 1.0 / (1.0 + dist as f64),
                    DistanceMetric::DotProduct => -(dist as f64), // dist is -dot
                };

                scored.push(VectorSearchResult {
                    doc_id: doc_id.clone(),
                    score,
                    distance: dist as f64,
                });
            }

            if scored.len() >= k {
                break;
            }
        }

        scored
    }

    fn len(&self) -> usize {
        self.count
    }
}

// ═══ Math Functions ═══

fn l2_norm(v: &[f32]) -> f32 {
    v.iter().map(|x| x * x).sum::<f32>().sqrt()
}

fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

fn cosine_similarity(a: &[f32], b: &[f32], norm_a: f32, norm_b: f32) -> f32 {
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    dot_product(a, b) / (norm_a * norm_b)
}

fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}

// ═══ Vector Index Manager ═══

/// Manages all vector indexes across collections
pub struct VectorIndexManager {
    /// collection_name -> VectorIndex
    indexes: Arc<RwLock<HashMap<String, HnswVectorIndex>>>,
}

impl VectorIndexManager {
    pub fn new() -> Self {
        VectorIndexManager {
            indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a vector index on a collection field
    pub fn create_index(&self, config: VectorIndexConfig) -> Result<(), String> {
        let mut indexes = self.indexes.write();
        let key = format!("{}:{}", config.collection, config.field);
        if indexes.contains_key(&key) {
            return Err(format!(
                "Vector index already exists on '{}.{}'",
                config.collection, config.field
            ));
        }
        indexes.insert(key, HnswVectorIndex::new(config));
        Ok(())
    }

    /// Insert or update a vector for a document
    pub fn upsert_vector(
        &self,
        collection: &str,
        field: &str,
        doc_id: &str,
        vector: Vec<f32>,
    ) -> Result<(), String> {
        let key = format!("{}:{}", collection, field);
        let mut indexes = self.indexes.write();
        let index = indexes
            .get_mut(&key)
            .ok_or_else(|| format!("No vector index on '{}.{}'", collection, field))?;

        // Validate dimensions
        if vector.len() != index.config.dimensions {
            return Err(format!(
                "Vector dimension mismatch: expected {}, got {}",
                index.config.dimensions,
                vector.len()
            ));
        }

        index.insert(doc_id, vector);
        Ok(())
    }

    /// Remove a document from vector indexes by field
    pub fn remove_vector(&self, collection: &str, field: &str, doc_id: &str) {
        let key = format!("{}:{}", collection, field);
        let mut indexes = self.indexes.write();
        if let Some(index) = indexes.get_mut(&key) {
            index.remove(doc_id);
        }
    }

    /// Remove a document from all vector indexes in a collection
    pub fn unindex_document(&self, collection: &str, doc_id: &str) {
        let mut indexes = self.indexes.write();
        let prefix = format!("{}:", collection);
        for (key, index) in indexes.iter_mut() {
            if key.starts_with(&prefix) {
                index.remove(doc_id);
            }
        }
    }

    /// Search for nearest neighbors
    pub fn search(
        &self,
        collection: &str,
        field: &str,
        query_vector: &[f32],
        k: usize,
        filter_ids: Option<&[String]>,
    ) -> Result<Vec<VectorSearchResult>, String> {
        let key = format!("{}:{}", collection, field);
        let indexes = self.indexes.read();
        let index = indexes
            .get(&key)
            .ok_or_else(|| format!("No vector index on '{}.{}'", collection, field))?;

        if query_vector.len() != index.config.dimensions {
            return Err(format!(
                "Query vector dimension mismatch: expected {}, got {}",
                index.config.dimensions,
                query_vector.len()
            ));
        }

        Ok(index.search(query_vector, k, filter_ids))
    }

    /// Auto-detect and index embedding fields from a document's data
    pub fn auto_index_document(
        &self,
        collection: &str,
        doc_id: &str,
        data: &std::collections::BTreeMap<String, serde_json::Value>,
    ) {
        // Phase 1: Read lock — find matching indexes and extract vectors
        let to_insert: Vec<(String, Vec<f32>)> = {
            let indexes = self.indexes.read();
            let mut results = Vec::new();
            for (key, index) in indexes.iter() {
                if key.starts_with(&format!("{}:", collection)) {
                    if let Some(serde_json::Value::Array(arr)) = data.get(&index.config.field) {
                        let vector: Vec<f32> = arr
                            .iter()
                            .filter_map(|v| v.as_f64().map(|f| f as f32))
                            .collect();
                        if vector.len() == index.config.dimensions {
                            results.push((key.clone(), vector));
                        }
                    }
                }
            }
            results
        };

        // Phase 2: Write lock — insert the vectors
        if !to_insert.is_empty() {
            let mut indexes = self.indexes.write();
            for (key, vector) in to_insert {
                if let Some(idx) = indexes.get_mut(&key) {
                    idx.insert(doc_id, vector);
                }
            }
        }
    }

    /// Get index info for a collection
    pub fn get_indexes(&self, collection: &str) -> Vec<VectorIndexConfig> {
        let indexes = self.indexes.read();
        indexes
            .iter()
            .filter(|(k, _)| k.starts_with(&format!("{}:", collection)))
            .map(|(_, idx)| idx.config.clone())
            .collect()
    }

    /// Get total vector count for a collection
    pub fn vector_count(&self, collection: &str) -> usize {
        let indexes = self.indexes.read();
        indexes
            .iter()
            .filter(|(k, _)| k.starts_with(&format!("{}:", collection)))
            .map(|(_, idx)| idx.len())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_vector(base: f32, dims: usize) -> Vec<f32> {
        (0..dims).map(|i| base + (i as f32) * 0.01).collect()
    }

    #[test]
    fn test_cosine_similarity_search() {
        let mgr = VectorIndexManager::new();
        mgr.create_index(VectorIndexConfig {
            collection: "docs".to_string(),
            field: "embedding".to_string(),
            dimensions: 4,
            metric: DistanceMetric::Cosine,
        })
        .unwrap();

        // Insert vectors
        mgr.upsert_vector("docs", "embedding", "doc1", vec![1.0, 0.0, 0.0, 0.0])
            .unwrap();
        mgr.upsert_vector("docs", "embedding", "doc2", vec![0.9, 0.1, 0.0, 0.0])
            .unwrap();
        mgr.upsert_vector("docs", "embedding", "doc3", vec![0.0, 1.0, 0.0, 0.0])
            .unwrap();
        mgr.upsert_vector("docs", "embedding", "doc4", vec![0.0, 0.0, 1.0, 0.0])
            .unwrap();

        // Search for vector closest to [1, 0, 0, 0]
        let results = mgr
            .search("docs", "embedding", &[1.0, 0.0, 0.0, 0.0], 2, None)
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].doc_id, "doc1"); // Exact match
        assert_eq!(results[1].doc_id, "doc2"); // Very similar
        assert!((results[0].score - 1.0).abs() < 0.001); // cosine = 1.0
    }

    #[test]
    fn test_hnsw_api() {
        use hnsw_rs::prelude::*;
        let _ = Hnsw::<f32, DistCosine>::new(16, 1000, 16, 200, DistCosine);
        let _ = Hnsw::<f32, DistL2>::new(16, 1000, 16, 200, DistL2);
        let _ = Hnsw::<f32, DistDot>::new(16, 1000, 16, 200, DistDot);
    }

    #[test]
    fn test_euclidean_search() {
        let mgr = VectorIndexManager::new();
        mgr.create_index(VectorIndexConfig {
            collection: "items".to_string(),
            field: "vec".to_string(),
            dimensions: 3,
            metric: DistanceMetric::Euclidean,
        })
        .unwrap();

        mgr.upsert_vector("items", "vec", "a", vec![0.0, 0.0, 0.0])
            .unwrap();
        mgr.upsert_vector("items", "vec", "b", vec![1.0, 0.0, 0.0])
            .unwrap();
        mgr.upsert_vector("items", "vec", "c", vec![10.0, 10.0, 10.0])
            .unwrap();

        let results = mgr
            .search("items", "vec", &[0.0, 0.0, 0.0], 2, None)
            .unwrap();

        assert_eq!(results[0].doc_id, "a"); // distance = 0
        assert_eq!(results[1].doc_id, "b"); // distance = 1
    }

    #[test]
    fn test_dimension_mismatch_rejected() {
        let mgr = VectorIndexManager::new();
        mgr.create_index(VectorIndexConfig {
            collection: "test".to_string(),
            field: "emb".to_string(),
            dimensions: 4,
            metric: DistanceMetric::Cosine,
        })
        .unwrap();

        let result = mgr.upsert_vector("test", "emb", "doc1", vec![1.0, 2.0]); // wrong dims
        assert!(result.is_err());
    }

    #[test]
    fn test_filtered_search() {
        let mgr = VectorIndexManager::new();
        mgr.create_index(VectorIndexConfig {
            collection: "docs".to_string(),
            field: "embedding".to_string(),
            dimensions: 3,
            metric: DistanceMetric::Cosine,
        })
        .unwrap();

        mgr.upsert_vector("docs", "embedding", "doc1", vec![1.0, 0.0, 0.0])
            .unwrap();
        mgr.upsert_vector("docs", "embedding", "doc2", vec![0.9, 0.1, 0.0])
            .unwrap();
        mgr.upsert_vector("docs", "embedding", "doc3", vec![0.0, 1.0, 0.0])
            .unwrap();

        // Only search within doc2 and doc3
        let filter = vec!["doc2".to_string(), "doc3".to_string()];
        let results = mgr
            .search(
                "docs",
                "embedding",
                &[1.0, 0.0, 0.0],
                10,
                Some(&filter),
            )
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].doc_id, "doc2"); // doc1 was filtered out
    }

    #[test]
    fn test_remove_vector() {
        let mgr = VectorIndexManager::new();
        mgr.create_index(VectorIndexConfig {
            collection: "test".to_string(),
            field: "emb".to_string(),
            dimensions: 3,
            metric: DistanceMetric::Cosine,
        })
        .unwrap();

        mgr.upsert_vector("test", "emb", "doc1", vec![1.0, 0.0, 0.0])
            .unwrap();
        mgr.upsert_vector("test", "emb", "doc2", vec![0.0, 1.0, 0.0])
            .unwrap();

        assert_eq!(mgr.vector_count("test"), 2);

        mgr.remove_vector("test", "emb", "doc1");
        assert_eq!(mgr.vector_count("test"), 1);

        let results = mgr
            .search("test", "emb", &[1.0, 0.0, 0.0], 10, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, "doc2");
    }
}
