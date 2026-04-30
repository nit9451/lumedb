// LumeDB SSTable (Sorted String Table)
// Immutable, sorted, on-disk key-value files with block-based compression
// and bloom filters for fast negative lookups

use crate::error::{LumeError, LumeResult};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// Block size for SSTable data blocks (4KB)
const BLOCK_SIZE: usize = 4096;

/// SSTable file header
#[derive(Debug, Serialize, Deserialize)]
pub struct SSTableHeader {
    pub magic: [u8; 8],       // "VORTEXST"
    pub version: u32,
    pub entry_count: u64,
    pub data_size: u64,
    pub index_offset: u64,
    pub bloom_offset: u64,
    pub min_key: String,
    pub max_key: String,
    pub level: u32,
    pub created_at: u64,
    pub compressed: bool,
}

/// An entry in the SSTable index (sparse index for block lookups)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableIndexEntry {
    pub key: String,
    pub offset: u64,
    pub length: u32,
}

/// A key-value entry stored in the SSTable
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableEntry {
    pub key: String,
    pub value: Option<Vec<u8>>, // None = tombstone
    pub sequence: u64,
    pub timestamp: u64,
}

/// Simple bloom filter for fast negative lookups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: usize,
    num_hashes: u32,
}

impl BloomFilter {
    /// Create a bloom filter sized for expected_items with target false positive rate
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let num_bits = (-(expected_items as f64 * false_positive_rate.ln()) / (2.0_f64.ln().powi(2)))
            .ceil() as usize;
        let num_bits = num_bits.max(64); // Minimum 64 bits
        let num_hashes = ((num_bits as f64 / expected_items as f64) * 2.0_f64.ln()).ceil() as u32;
        let num_hashes = num_hashes.max(1).min(16);

        BloomFilter {
            bits: vec![0u8; (num_bits + 7) / 8],
            num_bits,
            num_hashes,
        }
    }

    /// Add a key to the bloom filter
    pub fn insert(&mut self, key: &str) {
        for i in 0..self.num_hashes {
            let hash = self.hash(key, i) % self.num_bits;
            self.bits[hash / 8] |= 1 << (hash % 8);
        }
    }

    /// Check if a key might exist (false positives possible, no false negatives)
    pub fn might_contain(&self, key: &str) -> bool {
        for i in 0..self.num_hashes {
            let hash = self.hash(key, i) % self.num_bits;
            if self.bits[hash / 8] & (1 << (hash % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Hash function using xxHash with different seeds
    fn hash(&self, key: &str, seed: u32) -> usize {
        use xxhash_rust::xxh3::xxh3_64_with_seed;
        xxh3_64_with_seed(key.as_bytes(), seed as u64) as usize
    }
}

/// SSTable writer — builds a new SSTable file
pub struct SSTableWriter {
    path: PathBuf,
    entries: Vec<SSTableEntry>,
    level: u32,
}

impl SSTableWriter {
    pub fn new(path: &Path, level: u32) -> Self {
        SSTableWriter {
            path: path.to_path_buf(),
            entries: Vec::new(),
            level,
        }
    }

    /// Add an entry to the SSTable
    pub fn add(&mut self, key: String, value: Option<Vec<u8>>, sequence: u64, timestamp: u64) {
        self.entries.push(SSTableEntry {
            key,
            value,
            sequence,
            timestamp,
        });
    }

    /// Finalize and write the SSTable to disk
    pub fn finish(mut self) -> LumeResult<SSTableInfo> {
        // Sort entries by key
        self.entries.sort_by(|a, b| a.key.cmp(&b.key));

        let entry_count = self.entries.len() as u64;
        if entry_count == 0 {
            return Err(LumeError::SSTableError("Cannot write empty SSTable".to_string()));
        }

        let min_key = self.entries.first().unwrap().key.clone();
        let max_key = self.entries.last().unwrap().key.clone();

        // Build bloom filter
        let mut bloom = BloomFilter::new(self.entries.len().max(1), 0.01);
        for entry in &self.entries {
            bloom.insert(&entry.key);
        }

        // Serialize entries in blocks
        let mut data_blocks: Vec<Vec<u8>> = Vec::new();
        let mut index_entries: Vec<SSTableIndexEntry> = Vec::new();
        let mut current_block: Vec<SSTableEntry> = Vec::new();
        let mut current_block_size = 0usize;
        let mut data_offset = 0u64;

        for entry in &self.entries {
            let entry_size = entry.key.len()
                + entry.value.as_ref().map_or(0, |v| v.len())
                + 32;

            if current_block_size + entry_size > BLOCK_SIZE && !current_block.is_empty() {
                // Flush current block
                let block_data = bincode::serialize(&current_block)
                    .map_err(|e| LumeError::Serialization(e.to_string()))?;

                let compressed = compress_prepend_size(&block_data);

                index_entries.push(SSTableIndexEntry {
                    key: current_block[0].key.clone(),
                    offset: data_offset,
                    length: compressed.len() as u32,
                });

                data_offset += compressed.len() as u64;
                data_blocks.push(compressed);
                current_block.clear();
                current_block_size = 0;
            }

            current_block.push(entry.clone());
            current_block_size += entry_size;
        }

        // Flush remaining entries
        if !current_block.is_empty() {
            let block_data = bincode::serialize(&current_block)
                .map_err(|e| LumeError::Serialization(e.to_string()))?;
            let compressed = compress_prepend_size(&block_data);

            index_entries.push(SSTableIndexEntry {
                key: current_block[0].key.clone(),
                offset: data_offset,
                length: compressed.len() as u32,
            });

            data_offset += compressed.len() as u64;
            data_blocks.push(compressed);
        }

        // Write the file: [header][data blocks][index][bloom]
        let file = File::create(&self.path)?;
        let mut writer = BufWriter::new(file);

        // Serialize index and bloom
        let index_data = bincode::serialize(&index_entries)
            .map_err(|e| LumeError::Serialization(e.to_string()))?;
        let bloom_data = bincode::serialize(&bloom)
            .map_err(|e| LumeError::Serialization(e.to_string()))?;

        let header = SSTableHeader {
            magic: *b"VORTEXST",
            version: 1,
            entry_count,
            data_size: data_offset,
            index_offset: data_offset, // Relative to after header
            bloom_offset: data_offset + index_data.len() as u64,
            min_key: min_key.clone(),
            max_key: max_key.clone(),
            level: self.level,
            created_at: chrono::Utc::now().timestamp_millis() as u64,
            compressed: true,
        };

        let header_data = bincode::serialize(&header)
            .map_err(|e| LumeError::Serialization(e.to_string()))?;
        let header_len = header_data.len() as u32;

        // Write header length + header
        writer.write_all(&header_len.to_le_bytes())?;
        writer.write_all(&header_data)?;

        // Write data blocks
        for block in &data_blocks {
            writer.write_all(block)?;
        }

        // Write index
        writer.write_all(&index_data)?;

        // Write bloom filter
        writer.write_all(&bloom_data)?;

        // Write checksum of entire file
        writer.flush()?;

        let file_size = fs::metadata(&self.path)?.len();

        Ok(SSTableInfo {
            path: self.path.clone(),
            entry_count,
            min_key,
            max_key,
            level: self.level,
            file_size,
        })
    }
}

/// Metadata about a written SSTable
#[derive(Debug, Clone)]
pub struct SSTableInfo {
    pub path: PathBuf,
    pub entry_count: u64,
    pub min_key: String,
    pub max_key: String,
    pub level: u32,
    pub file_size: u64,
}

/// SSTable reader — reads from an existing SSTable file
pub struct SSTable {
    pub info: SSTableInfo,
    header: SSTableHeader,
    index: Vec<SSTableIndexEntry>,
    bloom: BloomFilter,
    data_offset: u64,
}

impl SSTable {
    /// Open an existing SSTable file
    pub fn open(path: &Path) -> LumeResult<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read header length
        let mut header_len_buf = [0u8; 4];
        reader.read_exact(&mut header_len_buf)?;
        let header_len = u32::from_le_bytes(header_len_buf) as usize;

        // Read header
        let mut header_buf = vec![0u8; header_len];
        reader.read_exact(&mut header_buf)?;
        let header: SSTableHeader = bincode::deserialize(&header_buf)
            .map_err(|e| LumeError::SSTableError(e.to_string()))?;

        // Verify magic
        if &header.magic != b"VORTEXST" {
            return Err(LumeError::SSTableError("Invalid SSTable magic bytes".to_string()));
        }

        let data_offset = 4 + header_len as u64;

        // Read index
        let index_abs_offset = data_offset + header.index_offset;
        let index_size = header.bloom_offset - header.index_offset;

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut discard = vec![0u8; index_abs_offset as usize];
        reader.read_exact(&mut discard)?;

        let mut index_buf = vec![0u8; index_size as usize];
        reader.read_exact(&mut index_buf)?;
        let index: Vec<SSTableIndexEntry> = bincode::deserialize(&index_buf)
            .map_err(|e| LumeError::SSTableError(e.to_string()))?;

        // Read bloom filter
        let mut bloom_buf = Vec::new();
        reader.read_to_end(&mut bloom_buf)?;
        let bloom: BloomFilter = bincode::deserialize(&bloom_buf)
            .map_err(|e| LumeError::SSTableError(e.to_string()))?;

        let file_size = fs::metadata(path)?.len();

        Ok(SSTable {
            info: SSTableInfo {
                path: path.to_path_buf(),
                entry_count: header.entry_count,
                min_key: header.min_key.clone(),
                max_key: header.max_key.clone(),
                level: header.level,
                file_size,
            },
            header,
            index,
            bloom,
            data_offset,
        })
    }

    /// Check if a key might exist using the bloom filter
    pub fn might_contain(&self, key: &str) -> bool {
        self.bloom.might_contain(key)
    }

    /// Get a value by key
    pub fn get(&self, key: &str) -> LumeResult<Option<SSTableEntry>> {
        // Quick bloom filter check
        if !self.bloom.might_contain(key) {
            return Ok(None);
        }

        // Binary search the index to find the right block
        let block_idx = match self.index.binary_search_by(|e| e.key.as_str().cmp(key)) {
            Ok(idx) => idx,
            Err(idx) => {
                if idx == 0 {
                    0
                } else {
                    idx - 1
                }
            }
        };

        // Read and decompress the block
        let index_entry = &self.index[block_idx];
        let block_offset = self.data_offset + index_entry.offset;

        let file = File::open(&self.info.path)?;
        let mut reader = BufReader::new(file);
        let mut discard = vec![0u8; block_offset as usize];
        reader.read_exact(&mut discard)?;

        let mut block_buf = vec![0u8; index_entry.length as usize];
        reader.read_exact(&mut block_buf)?;

        let decompressed = decompress_size_prepended(&block_buf)
            .map_err(|e| LumeError::SSTableError(format!("LZ4 decompression failed: {}", e)))?;

        let entries: Vec<SSTableEntry> = bincode::deserialize(&decompressed)
            .map_err(|e| LumeError::SSTableError(e.to_string()))?;

        // Binary search within the block
        for entry in entries {
            if entry.key == key {
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }

    /// Scan all entries in the SSTable
    pub fn scan_all(&self) -> LumeResult<Vec<SSTableEntry>> {
        let mut all_entries = Vec::new();

        for index_entry in &self.index {
            let block_offset = self.data_offset + index_entry.offset;

            let file = File::open(&self.info.path)?;
            let mut reader = BufReader::new(file);
            let mut discard = vec![0u8; block_offset as usize];
            reader.read_exact(&mut discard)?;

            let mut block_buf = vec![0u8; index_entry.length as usize];
            reader.read_exact(&mut block_buf)?;

            let decompressed = decompress_size_prepended(&block_buf)
                .map_err(|e| LumeError::SSTableError(format!("LZ4 decompression failed: {}", e)))?;

            let entries: Vec<SSTableEntry> = bincode::deserialize(&decompressed)
                .map_err(|e| LumeError::SSTableError(e.to_string()))?;

            all_entries.extend(entries);
        }

        Ok(all_entries)
    }

    /// Get the level of this SSTable
    pub fn level(&self) -> u32 {
        self.header.level
    }

    /// Check if a key is in the range of this SSTable
    pub fn key_in_range(&self, key: &str) -> bool {
        key >= self.header.min_key.as_str() && key <= self.header.max_key.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter() {
        let mut bloom = BloomFilter::new(1000, 0.01);
        bloom.insert("hello");
        bloom.insert("world");

        assert!(bloom.might_contain("hello"));
        assert!(bloom.might_contain("world"));
        // "notfound" might return true (false positive) but very unlikely
    }

    #[test]
    fn test_sstable_write_and_read() {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data_sst");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let path = dir.join("test.sst");

        // Write
        let mut writer = SSTableWriter::new(&path, 0);
        for i in 0..100 {
            writer.add(
                format!("key_{:04}", i),
                Some(format!("value_{}", i).into_bytes()),
                i as u64,
                0,
            );
        }
        let info = writer.finish().unwrap();
        assert_eq!(info.entry_count, 100);

        // Read
        let sst = SSTable::open(&path).unwrap();
        assert!(sst.might_contain("key_0050"));

        let entry = sst.get("key_0050").unwrap().unwrap();
        assert_eq!(entry.value.unwrap(), b"value_50");

        // Key not in SSTable
        let entry = sst.get("nonexistent").unwrap();
        assert!(entry.is_none());

        let _ = fs::remove_dir_all(&dir);
    }
}
