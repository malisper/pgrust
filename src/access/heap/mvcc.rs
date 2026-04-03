use std::collections::{BTreeMap, BTreeSet};

use crate::access::heap::tuple::HeapTuple;

pub type TransactionId = u32;
pub const INVALID_TRANSACTION_ID: TransactionId = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    InProgress,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MvccError {
    UnknownTransactionId(TransactionId),
    TransactionNotInProgress(TransactionId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub current_xid: TransactionId,
    committed: BTreeSet<TransactionId>,
    in_progress: BTreeSet<TransactionId>,
}

#[derive(Debug, Default, Clone)]
pub struct TransactionManager {
    next_xid: TransactionId,
    statuses: BTreeMap<TransactionId, TransactionStatus>,
}

impl TransactionManager {
    pub fn begin(&mut self) -> TransactionId {
        self.next_xid += 1;
        let xid = self.next_xid;
        self.statuses.insert(xid, TransactionStatus::InProgress);
        xid
    }

    pub fn commit(&mut self, xid: TransactionId) -> Result<(), MvccError> {
        match self.statuses.get_mut(&xid) {
            Some(status @ TransactionStatus::InProgress) => {
                *status = TransactionStatus::Committed;
                Ok(())
            }
            Some(_) => Err(MvccError::TransactionNotInProgress(xid)),
            None => Err(MvccError::UnknownTransactionId(xid)),
        }
    }

    pub fn abort(&mut self, xid: TransactionId) -> Result<(), MvccError> {
        match self.statuses.get_mut(&xid) {
            Some(status @ TransactionStatus::InProgress) => {
                *status = TransactionStatus::Aborted;
                Ok(())
            }
            Some(_) => Err(MvccError::TransactionNotInProgress(xid)),
            None => Err(MvccError::UnknownTransactionId(xid)),
        }
    }

    pub fn status(&self, xid: TransactionId) -> Option<TransactionStatus> {
        self.statuses.get(&xid).copied()
    }

    pub fn snapshot(&self, current_xid: TransactionId) -> Result<Snapshot, MvccError> {
        if current_xid != INVALID_TRANSACTION_ID && !self.statuses.contains_key(&current_xid) {
            return Err(MvccError::UnknownTransactionId(current_xid));
        }

        let mut committed = BTreeSet::new();
        let mut in_progress = BTreeSet::new();
        for (&xid, &status) in &self.statuses {
            match status {
                TransactionStatus::Committed => {
                    committed.insert(xid);
                }
                TransactionStatus::InProgress => {
                    if xid != current_xid {
                        in_progress.insert(xid);
                    }
                }
                TransactionStatus::Aborted => {}
            }
        }

        Ok(Snapshot {
            current_xid,
            committed,
            in_progress,
        })
    }
}

impl Snapshot {
    pub fn bootstrap() -> Self {
        Self {
            current_xid: INVALID_TRANSACTION_ID,
            committed: BTreeSet::new(),
            in_progress: BTreeSet::new(),
        }
    }

    pub fn transaction_visible(&self, xid: TransactionId) -> bool {
        xid == INVALID_TRANSACTION_ID || xid == self.current_xid || self.committed.contains(&xid)
    }

    pub fn transaction_in_progress(&self, xid: TransactionId) -> bool {
        xid != INVALID_TRANSACTION_ID && xid != self.current_xid && self.in_progress.contains(&xid)
    }

    pub fn tuple_visible(&self, tuple: &HeapTuple) -> bool {
        let xmin = tuple.header.xmin;
        if !self.transaction_visible(xmin) {
            return false;
        }

        let xmax = tuple.header.xmax;
        if xmax == INVALID_TRANSACTION_ID {
            return true;
        }
        if xmax == self.current_xid {
            return false;
        }
        if self.transaction_in_progress(xmax) {
            return true;
        }

        !self.transaction_visible(xmax)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::heap::tuple::HeapTuple;

    #[test]
    fn snapshot_hides_in_progress_inserts_from_other_transactions() {
        let mut txns = TransactionManager::default();
        let writer = txns.begin();
        let reader = txns.begin();

        let mut tuple = HeapTuple::new_raw(1, b"row".to_vec());
        tuple.header.xmin = writer;

        let snapshot = txns.snapshot(reader).unwrap();
        assert!(!snapshot.tuple_visible(&tuple));
    }

    #[test]
    fn snapshot_hides_committed_delete() {
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();

        let deleter = txns.begin();
        txns.commit(deleter).unwrap();

        let mut tuple = HeapTuple::new_raw(1, b"row".to_vec());
        tuple.header.xmin = inserter;
        tuple.header.xmax = deleter;

        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        assert!(!snapshot.tuple_visible(&tuple));
    }
}
