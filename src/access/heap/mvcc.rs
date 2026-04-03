use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use crate::access::heap::tuple::HeapTuple;

pub type TransactionId = u32;
pub type CommandId = u32;
pub const INVALID_TRANSACTION_ID: TransactionId = 0;
const TXN_STATUS_FORMAT_VERSION: u32 = 1;

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
    Io(String),
    CorruptStatusFile(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub current_xid: TransactionId,
    pub current_cid: CommandId,
    pub xmin: TransactionId,
    pub xmax: TransactionId,
    in_progress: BTreeSet<TransactionId>,
}

#[derive(Debug, Default, Clone)]
pub struct TransactionManager {
    next_xid: TransactionId,
    statuses: BTreeMap<TransactionId, TransactionStatus>,
    status_path: Option<PathBuf>,
}

impl TransactionManager {
    pub fn new_durable(base_dir: impl Into<PathBuf>) -> Result<Self, MvccError> {
        let path = Self::status_path(base_dir.into());
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| MvccError::Io(e.to_string()))?;
        }

        if path.exists() {
            let (next_xid, statuses) = load_status_file(&path)?;
            Ok(Self {
                next_xid,
                statuses,
                status_path: Some(path),
            })
        } else {
            let manager = Self {
                next_xid: INVALID_TRANSACTION_ID,
                statuses: BTreeMap::new(),
                status_path: Some(path),
            };
            manager.persist()?;
            Ok(manager)
        }
    }

    pub fn begin(&mut self) -> TransactionId {
        self.next_xid += 1;
        let xid = self.next_xid;
        self.statuses.insert(xid, TransactionStatus::InProgress);
        self.persist()
            .expect("persisting transaction status must succeed");
        xid
    }

    pub fn commit(&mut self, xid: TransactionId) -> Result<(), MvccError> {
        match self.statuses.get_mut(&xid) {
            Some(status @ TransactionStatus::InProgress) => {
                *status = TransactionStatus::Committed;
                self.persist()?;
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
                self.persist()?;
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
        self.snapshot_for_command(current_xid, CommandId::MAX)
    }

    pub fn snapshot_for_command(
        &self,
        current_xid: TransactionId,
        current_cid: CommandId,
    ) -> Result<Snapshot, MvccError> {
        if current_xid != INVALID_TRANSACTION_ID && !self.statuses.contains_key(&current_xid) {
            return Err(MvccError::UnknownTransactionId(current_xid));
        }

        let mut in_progress = BTreeSet::new();
        for (&xid, &status) in &self.statuses {
            match status {
                TransactionStatus::Committed => {}
                TransactionStatus::InProgress => {
                    if xid != current_xid {
                        in_progress.insert(xid);
                    }
                }
                TransactionStatus::Aborted => {}
            }
        }

        let xmax = self.next_xid.saturating_add(1).max(1);
        let xmin = in_progress
            .iter()
            .copied()
            .chain((current_xid != INVALID_TRANSACTION_ID).then_some(current_xid))
            .min()
            .unwrap_or(xmax);

        Ok(Snapshot {
            current_xid,
            current_cid,
            xmin,
            xmax,
            in_progress,
        })
    }

    fn status_path(base_dir: PathBuf) -> PathBuf {
        base_dir.join("pg_xact").join("status")
    }

    fn persist(&self) -> Result<(), MvccError> {
        let Some(path) = &self.status_path else {
            return Ok(());
        };

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&TXN_STATUS_FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&self.next_xid.to_le_bytes());
        bytes.extend_from_slice(&(self.statuses.len() as u32).to_le_bytes());
        for (&xid, &status) in &self.statuses {
            bytes.extend_from_slice(&xid.to_le_bytes());
            bytes.push(match status {
                TransactionStatus::InProgress => 0,
                TransactionStatus::Committed => 1,
                TransactionStatus::Aborted => 2,
            });
        }

        fs::write(path, bytes).map_err(|e| MvccError::Io(e.to_string()))
    }
}

impl Snapshot {
    pub fn bootstrap() -> Self {
        Self {
            current_xid: INVALID_TRANSACTION_ID,
            current_cid: CommandId::MAX,
            xmin: 1,
            xmax: 1,
            in_progress: BTreeSet::new(),
        }
    }

    pub fn transaction_active_in_snapshot(&self, xid: TransactionId) -> bool {
        xid != INVALID_TRANSACTION_ID
            && xid != self.current_xid
            && xid >= self.xmin
            && xid < self.xmax
            && self.in_progress.contains(&xid)
    }

    pub fn tuple_visible(&self, txns: &TransactionManager, tuple: &HeapTuple) -> bool {
        let xmin = tuple.header.xmin;
        if xmin == INVALID_TRANSACTION_ID {
            return true;
        }
        if xmin == self.current_xid {
            return tuple.header.cid_or_xvac < self.current_cid;
        }
        if xmin >= self.xmax {
            return false;
        }
        if self.transaction_active_in_snapshot(xmin) {
            return false;
        }
        match txns.status(xmin) {
            Some(TransactionStatus::Committed) => {}
            Some(TransactionStatus::Aborted) | Some(TransactionStatus::InProgress) | None => {
                return false;
            }
        }

        let xmax = tuple.header.xmax;
        if xmax == INVALID_TRANSACTION_ID {
            return true;
        }
        if xmax == self.current_xid {
            return false;
        }
        if xmax >= self.xmax {
            return true;
        }
        if self.transaction_active_in_snapshot(xmax) {
            return true;
        }
        match txns.status(xmax) {
            Some(TransactionStatus::Committed) => false,
            Some(TransactionStatus::Aborted) | Some(TransactionStatus::InProgress) | None => true,
        }
    }
}

fn load_status_file(
    path: &Path,
) -> Result<(TransactionId, BTreeMap<TransactionId, TransactionStatus>), MvccError> {
    let bytes = fs::read(path).map_err(|e| MvccError::Io(e.to_string()))?;
    if bytes.len() < 12 {
        return Err(MvccError::CorruptStatusFile("header too short"));
    }

    let version = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if version != TXN_STATUS_FORMAT_VERSION {
        return Err(MvccError::CorruptStatusFile("unknown format version"));
    }

    let next_xid = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let count = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let expected_len = 12 + count * 5;
    if bytes.len() != expected_len {
        return Err(MvccError::CorruptStatusFile(
            "entry count does not match file length",
        ));
    }

    let mut statuses = BTreeMap::new();
    let mut offset = 12;
    for _ in 0..count {
        let xid = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let status = match bytes[offset + 4] {
            0 => TransactionStatus::InProgress,
            1 => TransactionStatus::Committed,
            2 => TransactionStatus::Aborted,
            _ => {
                return Err(MvccError::CorruptStatusFile(
                    "invalid transaction status byte",
                ));
            }
        };
        statuses.insert(xid, status);
        offset += 5;
    }

    Ok((next_xid, statuses))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::heap::tuple::HeapTuple;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "pgrust_mvcc_{}_{}_{}",
            label,
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn snapshot_hides_in_progress_inserts_from_other_transactions() {
        let mut txns = TransactionManager::default();
        let writer = txns.begin();
        let reader = txns.begin();

        let mut tuple = HeapTuple::new_raw(1, b"row".to_vec());
        tuple.header.xmin = writer;

        let snapshot = txns.snapshot(reader).unwrap();
        assert!(!snapshot.tuple_visible(&txns, &tuple));
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
        assert!(!snapshot.tuple_visible(&txns, &tuple));
    }

    #[test]
    fn durable_status_survives_reopen() {
        let base = temp_dir("durable_reopen");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let committed = txns.begin();
        txns.commit(committed).unwrap();
        let aborted = txns.begin();
        txns.abort(aborted).unwrap();
        let in_progress = txns.begin();
        drop(txns);

        let reopened = TransactionManager::new_durable(&base).unwrap();
        assert_eq!(
            reopened.status(committed),
            Some(TransactionStatus::Committed)
        );
        assert_eq!(reopened.status(aborted), Some(TransactionStatus::Aborted));
        assert_eq!(
            reopened.status(in_progress),
            Some(TransactionStatus::InProgress)
        );

        let snapshot = reopened.snapshot(INVALID_TRANSACTION_ID).unwrap();
        assert!(snapshot.xmin <= in_progress);
        assert!(snapshot.xmax > in_progress);
        assert!(snapshot.transaction_active_in_snapshot(in_progress));

        let mut committed_tuple = HeapTuple::new_raw(1, b"committed".to_vec());
        committed_tuple.header.xmin = committed;
        assert!(snapshot.tuple_visible(&reopened, &committed_tuple));

        let mut aborted_tuple = HeapTuple::new_raw(1, b"aborted".to_vec());
        aborted_tuple.header.xmin = aborted;
        assert!(!snapshot.tuple_visible(&reopened, &aborted_tuple));
    }

    #[test]
    fn snapshot_uses_xmax_boundary_for_future_xids() {
        let mut txns = TransactionManager::default();
        let committed = txns.begin();
        txns.commit(committed).unwrap();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        let mut tuple = HeapTuple::new_raw(1, b"future".to_vec());
        tuple.header.xmin = snapshot.xmax;

        assert!(!snapshot.tuple_visible(&txns, &tuple));
    }
}
