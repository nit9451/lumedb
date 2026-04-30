// LumeDB Write-Ahead Log (WAL)
// Ensures durability — all mutations are logged before being applied
// On crash recovery, replay the WAL to restore state

use crate::error::{LumeError, LumeResult};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// A single WAL entry representing one mutation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Monotonically increasing sequence number
    pub sequence: u64,
    /// The operation performed
    pub operation: WalOperation,
    /// Timestamp of the operation
    pub timestamp: u64,
    /// Transaction ID (0 = no transaction)
    pub txn_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalOperation {
    /// Insert a document into a collection
    Insert {
        collection: String,
        key: String,
        value: Vec<u8>,
    },
    /// Update a document
    Update {
        collection: String,
        key: String,
        value: Vec<u8>,
    },
    /// Delete a document
    Delete {
        collection: String,
        key: String,
    },
    /// Create a new collection
    CreateCollection {
        name: String,
    },
    /// Drop a collection
    DropCollection {
        name: String,
    },
    /// Begin transaction
    BeginTransaction {
        txn_id: u64,
    },
    /// Commit transaction
    CommitTransaction {
        txn_id: u64,
    },
    /// Rollback transaction
    RollbackTransaction {
        txn_id: u64,
    },
    /// Checkpoint marker — all data before this is flushed to SSTables
    Checkpoint {
        sstable_id: u64,
    },
}

/// The Write-Ahead Log writer
pub struct Wal {
    /// Path to the WAL directory
    dir: PathBuf,
    /// Current WAL file writer
    writer: Option<BufWriter<File>>,
    /// Current sequence number
    sequence: u64,
    /// Current WAL segment number
    segment: u64,
    /// Maximum WAL segment size in bytes (default 64MB)
    max_segment_size: u64,
    /// Current segment size
    current_size: u64,
    /// Whether WAL is enabled
    enabled: bool,
}

impl Wal {
    /// Create a new WAL in the given directory
    pub fn new(dir: &Path) -> LumeResult<Self> {
        fs::create_dir_all(dir)?;

        let mut wal = Wal {
            dir: dir.to_path_buf(),
            writer: None,
            sequence: 0,
            segment: 0,
            max_segment_size: 64 * 1024 * 1024, // 64MB
            current_size: 0,
            enabled: true,
        };

        // Find the latest segment
        wal.segment = wal.find_latest_segment();
        wal.open_segment()?;

        Ok(wal)
    }

    /// Find the latest WAL segment number
    fn find_latest_segment(&self) -> u64 {
        let mut max_segment = 0u64;
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if let Some(num_str) = name.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log")) {
                    if let Ok(num) = num_str.parse::<u64>() {
                        max_segment = max_segment.max(num);
                    }
                }
            }
        }
        max_segment
    }

    /// Open a new WAL segment file
    fn open_segment(&mut self) -> LumeResult<()> {
        let path = self.segment_path(self.segment);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        self.writer = Some(BufWriter::new(file));
        self.current_size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        Ok(())
    }

    /// Get the path for a segment number
    fn segment_path(&self, segment: u64) -> PathBuf {
        self.dir.join(format!("wal_{:08}.log", segment))
    }

    /// Rotate to a new segment if current is too large
    fn maybe_rotate(&mut self) -> LumeResult<()> {
        if self.current_size >= self.max_segment_size {
            // Flush current segment
            if let Some(ref mut writer) = self.writer {
                writer.flush()?;
            }
            // Start a new segment
            self.segment += 1;
            self.open_segment()?;
        }
        Ok(())
    }

    /// Append an entry to the WAL
    pub fn append(&mut self, operation: WalOperation, txn_id: u64) -> LumeResult<u64> {
        if !self.enabled {
            self.sequence += 1;
            return Ok(self.sequence);
        }

        self.maybe_rotate()?;
        self.sequence += 1;

        let entry = WalEntry {
            sequence: self.sequence,
            operation,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            txn_id,
        };

        // Serialize the entry
        let data = bincode::serialize(&entry)
            .map_err(|e| LumeError::Serialization(e.to_string()))?;

        // Calculate checksum
        let mut hasher = Hasher::new();
        hasher.update(&data);
        let checksum = hasher.finalize();

        // Write: [length: u32][checksum: u32][data: bytes]
        let writer = self.writer.as_mut().ok_or_else(|| {
            LumeError::Internal("WAL writer not initialized".to_string())
        })?;

        let len = data.len() as u32;
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(&checksum.to_le_bytes())?;
        writer.write_all(&data)?;
        writer.flush()?;

        self.current_size += 4 + 4 + data.len() as u64;

        Ok(self.sequence)
    }

    /// Replay all WAL entries for crash recovery
    pub fn replay(&self) -> LumeResult<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let mut segments: Vec<u64> = Vec::new();

        // Find all WAL segments
        if let Ok(dir_entries) = fs::read_dir(&self.dir) {
            for entry in dir_entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if let Some(num_str) = name.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log")) {
                    if let Ok(num) = num_str.parse::<u64>() {
                        segments.push(num);
                    }
                }
            }
        }

        segments.sort();

        // Replay each segment in order
        for seg in segments {
            let path = self.segment_path(seg);
            let file = File::open(&path)?;
            let mut reader = BufReader::new(file);

            loop {
                // Read length
                let mut len_buf = [0u8; 4];
                match reader.read_exact(&mut len_buf) {
                    Ok(_) => {}
                    Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(LumeError::Io(e)),
                }
                let len = u32::from_le_bytes(len_buf) as usize;

                // Read checksum
                let mut checksum_buf = [0u8; 4];
                reader.read_exact(&mut checksum_buf)?;
                let expected_checksum = u32::from_le_bytes(checksum_buf);

                // Read data
                let mut data = vec![0u8; len];
                reader.read_exact(&mut data)?;

                // Verify checksum
                let mut hasher = Hasher::new();
                hasher.update(&data);
                let actual_checksum = hasher.finalize();

                if actual_checksum != expected_checksum {
                    return Err(LumeError::ChecksumMismatch {
                        expected: expected_checksum,
                        actual: actual_checksum,
                    });
                }

                // Deserialize entry
                let entry: WalEntry = bincode::deserialize(&data)
                    .map_err(|e| LumeError::WalCorruption(e.to_string()))?;

                entries.push(entry);
            }
        }

        Ok(entries)
    }

    /// Truncate WAL files up to a checkpoint
    pub fn truncate_before(&mut self, segment: u64) -> LumeResult<()> {
        if let Ok(dir_entries) = fs::read_dir(&self.dir) {
            for entry in dir_entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if let Some(num_str) = name_str.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log")) {
                    if let Ok(num) = num_str.parse::<u64>() {
                        if num < segment {
                            fs::remove_file(entry.path())?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Get current sequence number
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Sync WAL to disk
    pub fn sync(&mut self) -> LumeResult<()> {
        if let Some(ref mut writer) = self.writer {
            writer.flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn temp_dir() -> PathBuf {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data_wal");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_wal_write_and_replay() {
        let dir = temp_dir();
        {
            let mut wal = Wal::new(&dir).unwrap();
            wal.append(
                WalOperation::Insert {
                    collection: "users".to_string(),
                    key: "doc1".to_string(),
                    value: b"hello".to_vec(),
                },
                0,
            )
            .unwrap();

            wal.append(
                WalOperation::Insert {
                    collection: "users".to_string(),
                    key: "doc2".to_string(),
                    value: b"world".to_vec(),
                },
                0,
            )
            .unwrap();
        }

        // Replay
        let wal = Wal::new(&dir).unwrap();
        let entries = wal.replay().unwrap();
        assert_eq!(entries.len(), 2);

        let _ = fs::remove_dir_all(&dir);
    }
}
