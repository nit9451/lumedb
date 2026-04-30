// LumeDB Index Manager
// B-Tree secondary indexes for accelerating queries

use crate::error::{LumeError, LumeResult};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

/// Index definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    /// Name of the index
    pub name: String,
    /// Field path to index (e.g., "age" or "address.city")
    pub field: String,
    /// Whether this index enforces uniqueness
    pub unique: bool,
    /// Collection this index belongs to
    pub collection: String,
}

/// A B-Tree index mapping field values to document IDs
#[derive(Debug)]
pub struct BTreeIndex {
    pub def: IndexDef,
    /// Maps serialized field values to sets of document IDs
    tree: Arc<RwLock<BTreeMap<String, BTreeSet<String>>>>,
}

impl BTreeIndex {
    pub fn new(def: IndexDef) -> Self {
        BTreeIndex {
            def,
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Insert a document into the index
    pub fn insert(&self, doc_id: &str, value: &Value) -> LumeResult<()> {
        let key = value_to_index_key(value);
        let mut tree = self.tree.write();

        if self.def.unique {
            if let Some(existing) = tree.get(&key) {
                if !existing.is_empty() && !existing.contains(doc_id) {
                    return Err(LumeError::DuplicateKey {
                        field: self.def.field.clone(),
                        value: key,
                        index: self.def.name.clone(),
                    });
                }
            }
        }

        tree.entry(key)
            .or_insert_with(BTreeSet::new)
            .insert(doc_id.to_string());

        Ok(())
    }

    /// Remove a document from the index
    pub fn remove(&self, doc_id: &str, value: &Value) {
        let key = value_to_index_key(value);
        let mut tree = self.tree.write();
        if let Some(set) = tree.get_mut(&key) {
            set.remove(doc_id);
            if set.is_empty() {
                tree.remove(&key);
            }
        }
    }

    /// Find document IDs that match an exact value
    pub fn find_eq(&self, value: &Value) -> Vec<String> {
        let key = value_to_index_key(value);
        let tree = self.tree.read();
        tree.get(&key)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Find document IDs in a range
    pub fn find_range(&self, start: Option<&Value>, end: Option<&Value>) -> Vec<String> {
        let tree = self.tree.read();
        let mut results = Vec::new();

        let start_key = start.map(value_to_index_key);
        let end_key = end.map(value_to_index_key);

        for (key, doc_ids) in tree.iter() {
            let after_start = start_key.as_ref().map_or(true, |s| key >= s);
            let before_end = end_key.as_ref().map_or(true, |e| key <= e);

            if after_start && before_end {
                results.extend(doc_ids.iter().cloned());
            }
        }

        results
    }

    /// Get index size (number of unique values)
    pub fn size(&self) -> usize {
        self.tree.read().len()
    }
}

/// Convert a JSON value to a sortable string key for the B-Tree
fn value_to_index_key(value: &Value) -> String {
    match value {
        Value::Null => "\x00null".to_string(),
        Value::Bool(b) => format!("\x01{}", if *b { "1" } else { "0" }),
        Value::Number(n) => {
            // IEEE 754 sortable encoding for numbers
            let f = n.as_f64().unwrap_or(0.0);
            let bits = f.to_bits();
            // Transform so that negative numbers sort correctly
            let sortable = if f.is_sign_negative() {
                !bits
            } else {
                bits ^ (1u64 << 63)
            };
            format!("\x02{:020}", sortable)
        }
        Value::String(s) => format!("\x03{}", s),
        Value::Array(arr) => {
            let parts: Vec<String> = arr.iter().map(value_to_index_key).collect();
            format!("\x04[{}]", parts.join(","))
        }
        Value::Object(_) => format!("\x05{}", serde_json::to_string(value).unwrap_or_default()),
    }
}

/// Manages all indexes for a collection
#[derive(Debug)]
pub struct IndexManager {
    /// Collection name -> field name -> Index
    indexes: Arc<RwLock<HashMap<String, HashMap<String, BTreeIndex>>>>,
}

impl IndexManager {
    pub fn new() -> Self {
        IndexManager {
            indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new index
    pub fn create_index(&self, def: IndexDef) -> LumeResult<()> {
        let mut indexes = self.indexes.write();
        let collection_indexes = indexes
            .entry(def.collection.clone())
            .or_insert_with(HashMap::new);

        if collection_indexes.contains_key(&def.field) {
            return Err(LumeError::IndexError(format!(
                "Index on field '{}' already exists for collection '{}'",
                def.field, def.collection
            )));
        }

        collection_indexes.insert(def.field.clone(), BTreeIndex::new(def));
        Ok(())
    }

    /// Drop an index
    pub fn drop_index(&self, collection: &str, field: &str) -> LumeResult<()> {
        let mut indexes = self.indexes.write();
        if let Some(collection_indexes) = indexes.get_mut(collection) {
            collection_indexes.remove(field);
            Ok(())
        } else {
            Err(LumeError::IndexError("Index not found".to_string()))
        }
    }

    /// Get indexes for a collection
    pub fn get_indexes(&self, collection: &str) -> Vec<IndexDef> {
        let indexes = self.indexes.read();
        indexes
            .get(collection)
            .map(|coll_indexes| {
                coll_indexes.values().map(|idx| idx.def.clone()).collect()
            })
            .unwrap_or_default()
    }

    /// Index a document (insert into all relevant indexes)
    pub fn index_document(
        &self,
        collection: &str,
        doc_id: &str,
        doc_data: &BTreeMap<String, Value>,
    ) -> LumeResult<()> {
        let indexes = self.indexes.read();
        if let Some(collection_indexes) = indexes.get(collection) {
            for (field, index) in collection_indexes {
                if let Some(value) = doc_data.get(field) {
                    index.insert(doc_id, value)?;
                }
            }
        }
        Ok(())
    }

    /// Remove a document from all indexes
    pub fn unindex_document(
        &self,
        collection: &str,
        doc_id: &str,
        doc_data: &BTreeMap<String, Value>,
    ) {
        let indexes = self.indexes.read();
        if let Some(collection_indexes) = indexes.get(collection) {
            for (field, index) in collection_indexes {
                if let Some(value) = doc_data.get(field) {
                    index.remove(doc_id, value);
                }
            }
        }
    }

    /// Query an index for exact match
    pub fn find_by_index(&self, collection: &str, field: &str, value: &Value) -> Option<Vec<String>> {
        let indexes = self.indexes.read();
        indexes
            .get(collection)
            .and_then(|coll_indexes| coll_indexes.get(field))
            .map(|index| index.find_eq(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_btree_index() {
        let def = IndexDef {
            name: "idx_age".to_string(),
            field: "age".to_string(),
            unique: false,
            collection: "users".to_string(),
        };

        let index = BTreeIndex::new(def);
        index.insert("doc1", &json!(25)).unwrap();
        index.insert("doc2", &json!(30)).unwrap();
        index.insert("doc3", &json!(25)).unwrap();

        let results = index.find_eq(&json!(25));
        assert_eq!(results.len(), 2);
        assert!(results.contains(&"doc1".to_string()));
        assert!(results.contains(&"doc3".to_string()));
    }

    #[test]
    fn test_unique_index() {
        let def = IndexDef {
            name: "idx_email".to_string(),
            field: "email".to_string(),
            unique: true,
            collection: "users".to_string(),
        };

        let index = BTreeIndex::new(def);
        index.insert("doc1", &json!("alice@test.com")).unwrap();

        let result = index.insert("doc2", &json!("alice@test.com"));
        assert!(result.is_err());
    }
}
