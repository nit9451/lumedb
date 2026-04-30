// LumeDB Vector Search Engine
// Enables AI-native RAG workflows by storing embeddings alongside documents
// Supports cosine similarity, euclidean distance, and dot product metrics

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

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

/// A flat vector index — exact search via brute-force scan
/// Practical and correct for up to ~100K vectors
#[derive(Debug)]
struct FlatVectorIndex {
    config: VectorIndexConfig,
    vectors: Vec<VectorEntry>,
}

impl FlatVectorIndex {
    fn new(config: VectorIndexConfig) -> Self {
        FlatVectorIndex {
            config,
            vectors: Vec::new(),
        }
    }

    /// Insert a vector for a document
    fn insert(&mut self, doc_id: &str, vector: Vec<f32>) {
        let norm = l2_norm(&vector);
        // Remove existing entry for this doc if updating
        self.vectors.retain(|e| e.doc_id != doc_id);
        self.vectors.push(VectorEntry {
            doc_id: doc_id.to_string(),
            vector,
            norm,
        });
    }

    /// Remove a document's vector
    fn remove(&mut self, doc_id: &str) {
        self.vectors.retain(|e| e.doc_id != doc_id);
    }

    /// Search for the K nearest neighbors
    fn search(&self, query: &[f32], k: usize, filter_ids: Option<&[String]>) -> Vec<VectorSearchResult> {
        let query_norm = l2_norm(query);
        let mut scored: Vec<VectorSearchResult> = self
            .vectors
            .iter()
            .filter(|entry| {
                filter_ids
                    .map(|ids| ids.contains(&entry.doc_id))
                    .unwrap_or(true)
            })
            .map(|entry| {
                let (score, distance) = match self.config.metric {
                    DistanceMetric::Cosine => {
                        let sim = cosine_similarity(query, &entry.vector, query_norm, entry.norm);
                        (sim as f64, (1.0 - sim) as f64)
                    }
                    DistanceMetric::Euclidean => {
                        let dist = euclidean_distance(query, &entry.vector);
                        // Convert distance to a similarity score (higher = more similar)
                        let sim = 1.0 / (1.0 + dist);
                        (sim as f64, dist as f64)
                    }
                    DistanceMetric::DotProduct => {
                        let dp = dot_product(query, &entry.vector);
                        (dp as f64, -(dp as f64))
                    }
                };
                VectorSearchResult {
                    doc_id: entry.doc_id.clone(),
                    score,
                    distance,
                }
            })
            .collect();

        // Sort by score descending (highest similarity first)
        scored.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);
        scored
    }

    fn len(&self) -> usize {
        self.vectors.len()
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
    indexes: Arc<RwLock<HashMap<String, FlatVectorIndex>>>,
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
        indexes.insert(key, FlatVectorIndex::new(config));
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

    /// Remove a document from vector indexes
    pub fn remove_vector(&self, collection: &str, field: &str, doc_id: &str) {
        let key = format!("{}:{}", collection, field);
        let mut indexes = self.indexes.write();
        if let Some(index) = indexes.get_mut(&key) {
            index.remove(doc_id);
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
