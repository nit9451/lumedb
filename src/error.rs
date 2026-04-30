// LumeDB Error Types
use thiserror::Error;

/// Custom result type for LumeDB operations
pub type LumeResult<T> = std::result::Result<T, LumeError>;

#[derive(Error, Debug)]
pub enum LumeError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    #[error("Transaction error: {0}")]
    TransactionError(String),

    #[error("WAL corruption detected: {0}")]
    WalCorruption(String),

    #[error("Checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("Collection not found: {0}")]
    CollectionNotFound(String),

    #[error("Collection already exists: {0}")]
    CollectionAlreadyExists(String),

    #[error("Duplicate key: field '{field}' with value '{value}' already exists in index '{index}'")]
    DuplicateKey {
        field: String,
        value: String,
        index: String,
    },

    #[error("SSTable error: {0}")]
    SSTableError(String),

    #[error("Index error: {0}")]
    IndexError(String),

    #[error("Vector error: {0}")]
    VectorError(String),

    #[error("Internal error: {0}")]
    Internal(String),
}
