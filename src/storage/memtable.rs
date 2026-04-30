// LumeDB MemTable
// In-memory sorted key-value store backed by a BTreeMap
// This is the "write buffer" — all writes go here first before being flushed to SSTables

use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A single entry in the MemTable
#[derive(Debug, Clone)]
pub struct MemTableEntry {
    pub value: Option<Vec<u8>>, // None = tombstone (deleted)
    pub sequence: u64,
    pub timestamp: u64,
}

/// Thread-safe in-memory sorted store
pub struct MemTable {
    /// The actual data store — BTreeMap gives us sorted iteration for free
    data: Arc<RwLock<BTreeMap<String, MemTableEntry>>>,
    /// Current size in bytes (approximate)
    size: AtomicU64,
    /// Maximum size before flush (default 4MB)
    max_size: u64,
}

impl MemTable {
    pub fn new(max_size: u64) -> Self {
        MemTable {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            size: AtomicU64::new(0),
            max_size,
        }
    }

    /// Default 4MB memtable
    pub fn default_size() -> Self {
        Self::new(4 * 1024 * 1024)
    }

    /// Insert a key-value pair
    pub fn put(&self, key: String, value: Vec<u8>, sequence: u64) {
        let entry_size = key.len() as u64 + value.len() as u64 + 16;
        let entry = MemTableEntry {
            value: Some(value),
            sequence,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
        };

        let mut data = self.data.write();
        data.insert(key, entry);
        self.size.fetch_add(entry_size, Ordering::Relaxed);
    }

    /// Mark a key as deleted (tombstone)
    pub fn delete(&self, key: &str, sequence: u64) {
        let entry = MemTableEntry {
            value: None, // Tombstone
            sequence,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
        };

        let mut data = self.data.write();
        data.insert(key.to_string(), entry);
        self.size
            .fetch_add(key.len() as u64 + 16, Ordering::Relaxed);
    }

    /// Get a value by key
    pub fn get(&self, key: &str) -> Option<MemTableEntry> {
        let data = self.data.read();
        data.get(key).cloned()
    }

    /// Check if the memtable should be flushed
    pub fn should_flush(&self) -> bool {
        self.size.load(Ordering::Relaxed) >= self.max_size
    }

    /// Get current size
    pub fn size(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }

    /// Get number of entries
    pub fn len(&self) -> usize {
        self.data.read().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.data.read().is_empty()
    }

    /// Get all entries sorted by key (for flushing to SSTable)
    pub fn sorted_entries(&self) -> Vec<(String, MemTableEntry)> {
        let data = self.data.read();
        data.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Scan a key range [start, end)
    pub fn scan(&self, start: &str, end: &str) -> Vec<(String, MemTableEntry)> {
        let data = self.data.read();
        data.range(start.to_string()..end.to_string())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Get all entries (unordered for iteration)
    pub fn entries(&self) -> Vec<(String, MemTableEntry)> {
        self.sorted_entries()
    }

    /// Clear the memtable
    pub fn clear(&self) {
        self.data.write().clear();
        self.size.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let mt = MemTable::new(1024);
        mt.put("key1".to_string(), b"value1".to_vec(), 1);
        mt.put("key2".to_string(), b"value2".to_vec(), 2);

        let entry = mt.get("key1").unwrap();
        assert_eq!(entry.value.unwrap(), b"value1");
        assert_eq!(entry.sequence, 1);
    }

    #[test]
    fn test_delete_tombstone() {
        let mt = MemTable::new(1024);
        mt.put("key1".to_string(), b"value1".to_vec(), 1);
        mt.delete("key1", 2);

        let entry = mt.get("key1").unwrap();
        assert!(entry.value.is_none()); // tombstone
        assert_eq!(entry.sequence, 2);
    }

    #[test]
    fn test_sorted_entries() {
        let mt = MemTable::new(1024);
        mt.put("c".to_string(), b"3".to_vec(), 3);
        mt.put("a".to_string(), b"1".to_vec(), 1);
        mt.put("b".to_string(), b"2".to_vec(), 2);

        let entries = mt.sorted_entries();
        assert_eq!(entries[0].0, "a");
        assert_eq!(entries[1].0, "b");
        assert_eq!(entries[2].0, "c");
    }

    #[test]
    fn test_should_flush() {
        let mt = MemTable::new(100); // Tiny max size
        mt.put(
            "key".to_string(),
            vec![0u8; 200], // Exceeds max
            1,
        );
        assert!(mt.should_flush());
    }
}
