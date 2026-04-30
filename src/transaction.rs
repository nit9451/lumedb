// LumeDB Transaction Manager
// MVCC with snapshot isolation for ACID compliance

use crate::error::{LumeError, LumeResult};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Transaction state
#[derive(Debug, Clone, PartialEq)]
pub enum TxnState {
    Active,
    Committed,
    RolledBack,
}

/// A single operation within a transaction
#[derive(Debug, Clone)]
pub enum TxnOperation {
    Insert {
        collection: String,
        doc_id: String,
        data: Vec<u8>,
    },
    Update {
        collection: String,
        doc_id: String,
        old_data: Vec<u8>,
        new_data: Vec<u8>,
    },
    Delete {
        collection: String,
        doc_id: String,
        old_data: Vec<u8>,
    },
}

/// A transaction context
#[derive(Debug)]
pub struct Transaction {
    pub id: u64,
    pub state: TxnState,
    pub operations: Vec<TxnOperation>,
    pub started_at: u64,
    pub snapshot_version: u64,
}

impl Transaction {
    pub fn new(id: u64, snapshot_version: u64) -> Self {
        Transaction {
            id,
            state: TxnState::Active,
            operations: Vec::new(),
            started_at: chrono::Utc::now().timestamp_millis() as u64,
            snapshot_version,
        }
    }

    pub fn add_operation(&mut self, op: TxnOperation) {
        self.operations.push(op);
    }
}

/// Transaction Manager - handles MVCC and snapshot isolation
pub struct TransactionManager {
    /// Next transaction ID
    next_txn_id: AtomicU64,
    /// Global version counter
    global_version: AtomicU64,
    /// Active transactions
    active_txns: Arc<RwLock<HashMap<u64, Transaction>>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            next_txn_id: AtomicU64::new(1),
            global_version: AtomicU64::new(0),
            active_txns: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Begin a new transaction
    pub fn begin(&self) -> u64 {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let snapshot = self.global_version.load(Ordering::SeqCst);

        let txn = Transaction::new(txn_id, snapshot);
        self.active_txns.write().insert(txn_id, txn);

        txn_id
    }

    /// Add an operation to a transaction
    pub fn add_operation(&self, txn_id: u64, op: TxnOperation) -> LumeResult<()> {
        let mut txns = self.active_txns.write();
        let txn = txns.get_mut(&txn_id).ok_or_else(|| {
            LumeError::TransactionError(format!("Transaction {} not found", txn_id))
        })?;

        if txn.state != TxnState::Active {
            return Err(LumeError::TransactionError(format!(
                "Transaction {} is not active",
                txn_id
            )));
        }

        txn.add_operation(op);
        Ok(())
    }

    /// Commit a transaction — returns the operations to be applied
    pub fn commit(&self, txn_id: u64) -> LumeResult<Vec<TxnOperation>> {
        let mut txns = self.active_txns.write();
        let txn = txns.get_mut(&txn_id).ok_or_else(|| {
            LumeError::TransactionError(format!("Transaction {} not found", txn_id))
        })?;

        if txn.state != TxnState::Active {
            return Err(LumeError::TransactionError(format!(
                "Transaction {} is not active",
                txn_id
            )));
        }

        txn.state = TxnState::Committed;
        self.global_version.fetch_add(1, Ordering::SeqCst);

        let ops = txn.operations.clone();
        txns.remove(&txn_id);

        Ok(ops)
    }

    /// Rollback a transaction — returns the operations to undo
    pub fn rollback(&self, txn_id: u64) -> LumeResult<Vec<TxnOperation>> {
        let mut txns = self.active_txns.write();
        let txn = txns.get_mut(&txn_id).ok_or_else(|| {
            LumeError::TransactionError(format!("Transaction {} not found", txn_id))
        })?;

        if txn.state != TxnState::Active {
            return Err(LumeError::TransactionError(format!(
                "Transaction {} is not active",
                txn_id
            )));
        }

        txn.state = TxnState::RolledBack;
        let ops = txn.operations.clone();
        txns.remove(&txn_id);

        Ok(ops)
    }

    /// Get current global version
    pub fn current_version(&self) -> u64 {
        self.global_version.load(Ordering::SeqCst)
    }

    /// Get number of active transactions
    pub fn active_count(&self) -> usize {
        self.active_txns.read().len()
    }

    /// Check if a transaction is active
    pub fn is_active(&self, txn_id: u64) -> bool {
        self.active_txns.read().contains_key(&txn_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_begin_and_commit() {
        let tm = TransactionManager::new();
        let txn_id = tm.begin();
        assert!(tm.is_active(txn_id));

        tm.add_operation(
            txn_id,
            TxnOperation::Insert {
                collection: "users".to_string(),
                doc_id: "doc1".to_string(),
                data: b"hello".to_vec(),
            },
        )
        .unwrap();

        let ops = tm.commit(txn_id).unwrap();
        assert_eq!(ops.len(), 1);
        assert!(!tm.is_active(txn_id));
    }

    #[test]
    fn test_rollback() {
        let tm = TransactionManager::new();
        let txn_id = tm.begin();

        tm.add_operation(
            txn_id,
            TxnOperation::Insert {
                collection: "users".to_string(),
                doc_id: "doc1".to_string(),
                data: b"hello".to_vec(),
            },
        )
        .unwrap();

        let ops = tm.rollback(txn_id).unwrap();
        assert_eq!(ops.len(), 1);
        assert!(!tm.is_active(txn_id));
    }
}
