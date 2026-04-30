// LumeDB Document Model
// JSON-based documents with auto-generated IDs, TTL, and metadata

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use uuid::Uuid;

/// A document in LumeDB — the fundamental unit of data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// Unique document identifier (auto-generated UUID)
    #[serde(rename = "_id")]
    pub id: String,

    /// The actual document data as JSON
    #[serde(flatten)]
    pub data: BTreeMap<String, Value>,

    /// Document metadata
    #[serde(rename = "_meta")]
    pub meta: DocumentMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentMeta {
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Last modified timestamp
    pub updated_at: DateTime<Utc>,
    /// Version number for MVCC (increments on each update)
    pub version: u64,
    /// Time-to-live in seconds (None = never expires)
    pub ttl: Option<u64>,
    /// Soft-deleted flag
    pub deleted: bool,
}

impl Document {
    /// Create a new document from a JSON value
    pub fn new(data: Value) -> Self {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now();

        let map = match data {
            Value::Object(obj) => obj.into_iter().collect(),
            _ => {
                let mut m = BTreeMap::new();
                m.insert("value".to_string(), data);
                m
            }
        };

        Document {
            id,
            data: map,
            meta: DocumentMeta {
                created_at: now,
                updated_at: now,
                version: 1,
                ttl: None,
                deleted: false,
            },
        }
    }

    /// Create a document with a specific ID
    pub fn with_id(id: String, data: Value) -> Self {
        let now = Utc::now();
        let map = match data {
            Value::Object(obj) => obj.into_iter().collect(),
            _ => {
                let mut m = BTreeMap::new();
                m.insert("value".to_string(), data);
                m
            }
        };

        Document {
            id,
            data: map,
            meta: DocumentMeta {
                created_at: now,
                updated_at: now,
                version: 1,
                ttl: None,
                deleted: false,
            },
        }
    }

    /// Get a field value by dot-notation path (e.g., "address.city")
    pub fn get_field(&self, path: &str) -> Option<&Value> {
        let parts: Vec<&str> = path.split('.').collect();
        if parts.is_empty() {
            return None;
        }

        // Check special fields
        if path == "_id" {
            return Some(&Value::String(self.id.clone())).map(|_| {
                // Return a reference - we need to store it
                // For _id we'll handle it differently
                &Value::Null
            });
        }

        let mut current: Option<&Value> = self.data.get(parts[0]);

        for part in &parts[1..] {
            match current {
                Some(Value::Object(map)) => {
                    current = map.get(*part);
                }
                _ => return None,
            }
        }

        current
    }

    /// Get the field value, with special handling for _id
    pub fn get_field_value(&self, path: &str) -> Option<Value> {
        if path == "_id" {
            return Some(Value::String(self.id.clone()));
        }

        self.get_field(path).cloned()
    }

    /// Convert document to a full JSON Value (including _id and _meta)
    pub fn to_json(&self) -> Value {
        let mut map = serde_json::Map::new();
        map.insert("_id".to_string(), Value::String(self.id.clone()));

        for (k, v) in &self.data {
            map.insert(k.clone(), v.clone());
        }

        map.insert(
            "_meta".to_string(),
            serde_json::to_value(&self.meta).unwrap_or(Value::Null),
        );

        Value::Object(map)
    }

    /// Convert to JSON without metadata
    pub fn to_json_clean(&self) -> Value {
        let mut map = serde_json::Map::new();
        map.insert("_id".to_string(), Value::String(self.id.clone()));

        for (k, v) in &self.data {
            map.insert(k.clone(), v.clone());
        }

        Value::Object(map)
    }

    /// Apply an update to the document
    pub fn apply_update(&mut self, update: &Value) {
        if let Value::Object(updates) = update {
            // Handle $set operator
            if let Some(Value::Object(set_fields)) = updates.get("$set") {
                for (key, value) in set_fields {
                    self.data.insert(key.clone(), value.clone());
                }
            }

            // Handle $unset operator
            if let Some(Value::Object(unset_fields)) = updates.get("$unset") {
                for (key, _) in unset_fields {
                    self.data.remove(key);
                }
            }

            // Handle $inc operator
            if let Some(Value::Object(inc_fields)) = updates.get("$inc") {
                for (key, inc_value) in inc_fields {
                    if let Some(current) = self.data.get(key) {
                        let new_val = match (current, inc_value) {
                            (Value::Number(a), Value::Number(b)) => {
                                if let (Some(a), Some(b)) = (a.as_f64(), b.as_f64()) {
                                    serde_json::json!(a + b)
                                } else {
                                    continue;
                                }
                            }
                            _ => continue,
                        };
                        self.data.insert(key.clone(), new_val);
                    }
                }
            }

            // Handle $push operator (append to array)
            if let Some(Value::Object(push_fields)) = updates.get("$push") {
                for (key, value) in push_fields {
                    let entry = self.data.entry(key.clone()).or_insert(Value::Array(vec![]));
                    if let Value::Array(arr) = entry {
                        arr.push(value.clone());
                    }
                }
            }

            // Handle $pull operator (remove from array)
            if let Some(Value::Object(pull_fields)) = updates.get("$pull") {
                for (key, value) in pull_fields {
                    if let Some(Value::Array(arr)) = self.data.get_mut(key) {
                        arr.retain(|item| item != value);
                    }
                }
            }

            // If no operators, treat as full replacement (minus _id)
            if !updates.keys().any(|k| k.starts_with('$')) {
                for (key, value) in updates {
                    if key != "_id" {
                        self.data.insert(key.clone(), value.clone());
                    }
                }
            }
        }

        self.meta.updated_at = Utc::now();
        self.meta.version += 1;
    }

    /// Check if document has expired based on TTL
    pub fn is_expired(&self) -> bool {
        if let Some(ttl_secs) = self.meta.ttl {
            let expires_at = self.meta.created_at
                + chrono::Duration::seconds(ttl_secs as i64);
            Utc::now() > expires_at
        } else {
            false
        }
    }

    /// Serialize document to bytes for storage (uses JSON for Value compatibility)
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Failed to serialize document")
    }

    /// Deserialize document from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// Represents a collection of documents (like a table in SQL)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionMeta {
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub doc_count: u64,
    pub index_count: u64,
}

impl CollectionMeta {
    pub fn new(name: &str) -> Self {
        CollectionMeta {
            name: name.to_string(),
            created_at: Utc::now(),
            doc_count: 0,
            index_count: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_document_creation() {
        let doc = Document::new(json!({
            "name": "Alice",
            "age": 30,
            "city": "NYC"
        }));
        assert!(!doc.id.is_empty());
        assert_eq!(doc.data.get("name").unwrap(), &json!("Alice"));
        assert_eq!(doc.meta.version, 1);
    }

    #[test]
    fn test_document_update() {
        let mut doc = Document::new(json!({
            "name": "Alice",
            "age": 30
        }));

        doc.apply_update(&json!({
            "$set": { "age": 31, "city": "LA" },
        }));

        assert_eq!(doc.data.get("age").unwrap(), &json!(31));
        assert_eq!(doc.data.get("city").unwrap(), &json!("LA"));
        assert_eq!(doc.meta.version, 2);
    }

    #[test]
    fn test_document_inc() {
        let mut doc = Document::new(json!({
            "balance": 100.0
        }));

        doc.apply_update(&json!({
            "$inc": { "balance": 50.0 }
        }));

        assert_eq!(doc.data.get("balance").unwrap(), &json!(150.0));
    }

    #[test]
    fn test_nested_field_access() {
        let doc = Document::new(json!({
            "address": {
                "city": "NYC",
                "zip": "10001"
            }
        }));

        assert_eq!(
            doc.get_field("address.city"),
            Some(&json!("NYC"))
        );
    }

    #[test]
    fn test_serialization() {
        let doc = Document::new(json!({"name": "test"}));
        let bytes = doc.to_bytes();
        let doc2 = Document::from_bytes(&bytes).unwrap();
        assert_eq!(doc.id, doc2.id);
    }
}
