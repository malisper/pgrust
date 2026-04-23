use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write as _};
use std::path::PathBuf;

use crate::backend::access::transam::clog::{
    CLOG_BITS_PER_XACT, CLOG_XACT_BITMASK, CLOG_XACTS_PER_BYTE, STATUS_FILE_HEADER_SIZE,
    load_status_file_from_bytes, status_to_bits,
};
pub use crate::backend::utils::time::snapmgr::Snapshot;

pub type TransactionId = u32;
pub type CommandId = u32;
pub const INVALID_TRANSACTION_ID: TransactionId = 0;
pub const BOOTSTRAP_TRANSACTION_ID: TransactionId = 1;
pub const FROZEN_TRANSACTION_ID: TransactionId = 2;
pub const FIRST_NORMAL_TRANSACTION_ID: TransactionId = 3;

pub const fn transaction_id_is_normal(xid: TransactionId) -> bool {
    xid >= FIRST_NORMAL_TRANSACTION_ID
}

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

#[derive(Debug)]
pub struct TransactionManager {
    next_xid: TransactionId,
    statuses: BTreeMap<TransactionId, TransactionStatus>,
    /// Dense list of currently in-progress transaction IDs.
    /// Maintained alongside `statuses` so snapshot creation is O(active_txns)
    /// instead of O(all_txns).
    in_progress: Vec<TransactionId>,
    status_path: Option<PathBuf>,
    /// Open file handle for CLOG flushes.
    status_file: Option<File>,
    /// In-memory CLOG page buffer, matching PostgreSQL's SLRU approach.
    /// All reads/writes go through this buffer; flushed to disk on checkpoint.
    clog_buf: Vec<u8>,
}

impl Clone for TransactionManager {
    fn clone(&self) -> Self {
        Self {
            next_xid: self.next_xid,
            statuses: self.statuses.clone(),
            in_progress: self.in_progress.clone(),
            status_path: self.status_path.clone(),
            status_file: self.status_file.as_ref().and_then(|f| f.try_clone().ok()),
            clog_buf: self.clog_buf.clone(),
        }
    }
}

impl TransactionManager {
    pub fn new_ephemeral() -> Self {
        let initial_next_xid = FIRST_NORMAL_TRANSACTION_ID - 1;
        let clog_buf = initial_next_xid.to_le_bytes().to_vec();
        Self {
            next_xid: initial_next_xid,
            statuses: BTreeMap::new(),
            in_progress: Vec::new(),
            status_path: None,
            status_file: None,
            clog_buf,
        }
    }

    pub fn new_durable(base_dir: impl Into<PathBuf>) -> Result<Self, MvccError> {
        let path = Self::status_path(base_dir.into());
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| MvccError::Io(e.to_string()))?;
        }

        if path.exists() {
            let raw = fs::read(&path).map_err(|e| MvccError::Io(e.to_string()))?;
            let (next_xid, statuses) = load_status_file_from_bytes(&raw)?;
            let in_progress = statuses
                .iter()
                .filter(|(_, s)| **s == TransactionStatus::InProgress)
                .map(|(xid, _)| *xid)
                .collect();
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|e| MvccError::Io(e.to_string()))?;
            Ok(Self {
                next_xid: next_xid.max(FIRST_NORMAL_TRANSACTION_ID - 1),
                statuses,
                in_progress,
                status_path: Some(path),
                status_file: Some(file),
                clog_buf: raw,
            })
        } else {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .map_err(|e| MvccError::Io(e.to_string()))?;
            let initial_next_xid = FIRST_NORMAL_TRANSACTION_ID - 1;
            let clog_buf = initial_next_xid.to_le_bytes().to_vec();
            Self::write_initial_status_file(&mut file, &clog_buf)?;
            Ok(Self {
                next_xid: initial_next_xid,
                statuses: BTreeMap::new(),
                in_progress: Vec::new(),
                status_path: Some(path),
                status_file: Some(file),
                clog_buf,
            })
        }
    }

    fn write_initial_status_file(file: &mut File, clog_buf: &[u8]) -> Result<(), MvccError> {
        file.write_all(clog_buf)
            .map_err(|e| MvccError::Io(e.to_string()))?;
        crate::backend::storage::sync_file_data(file).map_err(|e| MvccError::Io(e.to_string()))?;
        file.seek(SeekFrom::Start(0))
            .map_err(|e| MvccError::Io(e.to_string()))?;
        Ok(())
    }

    pub fn begin(&mut self) -> TransactionId {
        self.next_xid += 1;
        let xid = self.next_xid;
        self.statuses.insert(xid, TransactionStatus::InProgress);
        self.in_progress.push(xid);
        self.write_status_bits(xid, TransactionStatus::InProgress);
        self.write_next_xid();
        xid
    }

    pub fn commit(&mut self, xid: TransactionId) -> Result<(), MvccError> {
        match self.statuses.get_mut(&xid) {
            Some(status @ TransactionStatus::InProgress) => {
                *status = TransactionStatus::Committed;
                self.in_progress.retain(|&x| x != xid);
                self.write_status_bits(xid, TransactionStatus::Committed);
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
                self.in_progress.retain(|&x| x != xid);
                self.write_status_bits(xid, TransactionStatus::Aborted);
                Ok(())
            }
            Some(_) => Err(MvccError::TransactionNotInProgress(xid)),
            None => Err(MvccError::UnknownTransactionId(xid)),
        }
    }

    /// Mark a transaction as committed during WAL replay.
    /// Unlike `commit()`, this does not require a prior `begin()` — the
    /// transaction may not be in our in-memory state if it was started
    /// before the crash.
    pub fn replay_commit(&mut self, xid: TransactionId) {
        if xid >= self.next_xid {
            self.next_xid = xid + 1;
            self.write_next_xid();
        }
        self.statuses.insert(xid, TransactionStatus::Committed);
        self.in_progress.retain(|&x| x != xid);
        self.write_status_bits(xid, TransactionStatus::Committed);
    }

    /// Mark a transaction as aborted during WAL replay cleanup.
    /// Called for any xid that appears in WAL records but has no
    /// corresponding commit record.
    pub fn replay_abort(&mut self, xid: TransactionId) {
        if xid >= self.next_xid {
            self.next_xid = xid + 1;
            self.write_next_xid();
        }
        self.statuses.insert(xid, TransactionStatus::Aborted);
        self.in_progress.retain(|&x| x != xid);
        self.write_status_bits(xid, TransactionStatus::Aborted);
    }

    pub fn status(&self, xid: TransactionId) -> Option<TransactionStatus> {
        if matches!(xid, BOOTSTRAP_TRANSACTION_ID | FROZEN_TRANSACTION_ID) {
            return Some(TransactionStatus::Committed);
        }
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
        for &xid in &self.in_progress {
            if xid != current_xid {
                in_progress.insert(xid);
            }
        }

        let xmax = self
            .next_xid
            .saturating_add(1)
            .max(FIRST_NORMAL_TRANSACTION_ID);
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

    pub fn oldest_active_xid(&self) -> TransactionId {
        self.in_progress.iter().copied().min().unwrap_or_else(|| {
            self.next_xid
                .saturating_add(1)
                .max(FIRST_NORMAL_TRANSACTION_ID)
        })
    }

    pub fn next_xid(&self) -> TransactionId {
        self.next_xid
    }

    fn status_path(base_dir: PathBuf) -> PathBuf {
        base_dir.join("pg_xact").join("status")
    }

    /// Write 2-bit status for `xid` into the in-memory CLOG buffer.
    /// No disk I/O — flushed to disk on checkpoint via `flush_clog()`.
    fn write_status_bits(&mut self, xid: TransactionId, status: TransactionStatus) {
        let byte_idx = STATUS_FILE_HEADER_SIZE + (xid / CLOG_XACTS_PER_BYTE) as usize;
        let bshift = (xid % CLOG_XACTS_PER_BYTE) * CLOG_BITS_PER_XACT;

        // Extend buffer if needed.
        if byte_idx >= self.clog_buf.len() {
            self.clog_buf.resize(byte_idx + 1, 0);
        }

        self.clog_buf[byte_idx] &= !(CLOG_XACT_BITMASK << bshift);
        self.clog_buf[byte_idx] |= status_to_bits(status) << bshift;
    }

    /// Update next_xid in the in-memory CLOG header.
    fn write_next_xid(&mut self) {
        if self.clog_buf.len() < STATUS_FILE_HEADER_SIZE {
            self.clog_buf.resize(STATUS_FILE_HEADER_SIZE, 0);
        }
        self.clog_buf[0..4].copy_from_slice(&self.next_xid.to_le_bytes());
    }

    /// Flush in-memory CLOG buffer to disk (like PostgreSQL's SLRU writeback).
    pub fn flush_clog(&mut self) -> Result<(), MvccError> {
        let Some(ref mut file) = self.status_file else {
            return Ok(());
        };
        file.seek(SeekFrom::Start(0))
            .map_err(|e| MvccError::Io(e.to_string()))?;
        file.write_all(&self.clog_buf)
            .map_err(|e| MvccError::Io(e.to_string()))?;
        crate::backend::storage::sync_file_data(file).map_err(|e| MvccError::Io(e.to_string()))?;
        file.seek(SeekFrom::Start(0))
            .map_err(|e| MvccError::Io(e.to_string()))?;
        Ok(())
    }
}

impl Drop for TransactionManager {
    fn drop(&mut self) {
        let _ = self.flush_clog();
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new_ephemeral()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::access::htup::HeapTuple;
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
    fn snapshot_without_xid_hides_future_lazy_xid_insert() {
        let mut txns = TransactionManager::default();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        let writer = txns.begin();
        txns.commit(writer).unwrap();

        let mut tuple = HeapTuple::new_raw(1, b"row".to_vec());
        tuple.header.xmin = writer;

        assert!(writer >= snapshot.xmax);
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

    /// The in_progress Vec must stay in sync with statuses after a
    /// sequence of begin/commit/abort operations.
    #[test]
    fn in_progress_vec_tracks_statuses() {
        let mut txns = TransactionManager::default();

        let a = txns.begin();
        let b = txns.begin();
        let c = txns.begin();
        assert_eq!(txns.in_progress, vec![a, b, c]);

        txns.commit(b).unwrap();
        assert_eq!(txns.in_progress, vec![a, c]);

        txns.abort(a).unwrap();
        assert_eq!(txns.in_progress, vec![c]);

        let d = txns.begin();
        assert_eq!(txns.in_progress, vec![c, d]);

        txns.commit(c).unwrap();
        txns.commit(d).unwrap();
        assert!(txns.in_progress.is_empty());
    }

    /// Snapshot after many committed transactions should be O(active)
    /// not O(total). Verify correctness at scale.
    #[test]
    fn snapshot_correct_after_many_committed_transactions() {
        let mut txns = TransactionManager::default();

        // Commit 1000 transactions to build up history.
        for _ in 0..1000 {
            let xid = txns.begin();
            txns.commit(xid).unwrap();
        }

        let inserter = txns.begin();
        txns.commit(inserter).unwrap();

        let updater = txns.begin();
        txns.commit(updater).unwrap();

        let mut old_tuple = HeapTuple::new_raw(1, b"old".to_vec());
        old_tuple.header.xmin = inserter;
        old_tuple.header.xmax = updater;

        let mut new_tuple = HeapTuple::new_raw(2, b"new".to_vec());
        new_tuple.header.xmin = updater;

        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        assert!(
            !snapshot.tuple_visible(&txns, &old_tuple),
            "old tuple invisible after 1000+ txns"
        );
        assert!(
            snapshot.tuple_visible(&txns, &new_tuple),
            "new tuple visible after 1000+ txns"
        );
    }

    /// 2-bit CLOG format: durable status survives reopen and correctly
    /// packs 4 transactions per byte.
    #[test]
    fn clog_2bit_format_roundtrips() {
        let base = temp_dir("clog_2bit");
        let mut txns = TransactionManager::new_durable(&base).unwrap();

        // Create 8 transactions (2 full bytes) with mixed statuses.
        let xids: Vec<_> = (0..8).map(|_| txns.begin()).collect();
        txns.commit(xids[0]).unwrap();
        txns.abort(xids[1]).unwrap();
        // xids[2] stays in progress
        txns.commit(xids[3]).unwrap();
        txns.commit(xids[4]).unwrap();
        txns.abort(xids[5]).unwrap();
        txns.commit(xids[6]).unwrap();
        // xids[7] stays in progress
        drop(txns);

        // Reopen and verify all statuses survived.
        let txns2 = TransactionManager::new_durable(&base).unwrap();
        assert_eq!(txns2.status(xids[0]), Some(TransactionStatus::Committed));
        assert_eq!(txns2.status(xids[1]), Some(TransactionStatus::Aborted));
        assert_eq!(txns2.status(xids[2]), Some(TransactionStatus::InProgress));
        assert_eq!(txns2.status(xids[3]), Some(TransactionStatus::Committed));
        assert_eq!(txns2.status(xids[4]), Some(TransactionStatus::Committed));
        assert_eq!(txns2.status(xids[5]), Some(TransactionStatus::Aborted));
        assert_eq!(txns2.status(xids[6]), Some(TransactionStatus::Committed));
        assert_eq!(txns2.status(xids[7]), Some(TransactionStatus::InProgress));

        // in_progress should be rebuilt from the file.
        let mut expected_in_progress = vec![xids[2], xids[7]];
        expected_in_progress.sort();
        let mut actual = txns2.in_progress.clone();
        actual.sort();
        assert_eq!(actual, expected_in_progress);
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
    fn durable_status_file_is_initialized_immediately() {
        let base = temp_dir("durable_status_header_written");
        let _txns = TransactionManager::new_durable(&base).unwrap();
        let raw = std::fs::read(base.join("pg_xact").join("status")).unwrap();
        assert_eq!(raw.len(), STATUS_FILE_HEADER_SIZE);
        assert_eq!(raw, (FIRST_NORMAL_TRANSACTION_ID - 1).to_le_bytes());
    }

    /// Build raw tuple bytes for hint bit testing.
    /// Layout: xmin(4) xmax(4) cid(4) ctid(6) infomask2(2) infomask(2) hoff(1) = 23 bytes header + 1 byte data
    fn make_tuple_bytes(xmin: u32, xmax: u32, cid: u32, extra_infomask: u16) -> Vec<u8> {
        let mut bytes = vec![0u8; 24]; // 23 header + 1 data
        bytes[0..4].copy_from_slice(&xmin.to_le_bytes());
        bytes[4..8].copy_from_slice(&xmax.to_le_bytes());
        bytes[8..12].copy_from_slice(&cid.to_le_bytes());
        // ctid at 12..18 = zeros
        let infomask2: u16 = 0; // 0 attributes
        bytes[18..20].copy_from_slice(&infomask2.to_le_bytes());
        bytes[20..22].copy_from_slice(&extra_infomask.to_le_bytes());
        bytes[22] = 23; // hoff
        bytes[23] = 0xFF; // dummy data byte
        bytes
    }

    fn set_infomask(bytes: &mut [u8], bits: u16) {
        let current = u16::from_le_bytes([bytes[20], bytes[21]]);
        let updated = (current | bits).to_le_bytes();
        bytes[20] = updated[0];
        bytes[21] = updated[1];
    }

    // ---- Hint bit fast path tests ----

    #[test]
    fn hint_fast_path_xmin_committed_xmax_invalid_is_visible() {
        let mut txns = TransactionManager::default();
        // Begin and commit a transaction so that xmin=1 is within the snapshot's range.
        let xid = txns.begin();
        txns.commit(xid).unwrap();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let bytes = make_tuple_bytes(
            1,
            0,
            0,
            crate::include::access::htup::HEAP_XMIN_COMMITTED
                | crate::include::access::htup::HEAP_XMAX_INVALID,
        );
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(visible);
        assert_eq!(hints, 0); // no new hints needed
    }

    #[test]
    fn hint_fast_path_xmin_invalid_is_not_visible() {
        let txns = TransactionManager::default();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let bytes = make_tuple_bytes(
            1,
            0,
            0,
            crate::include::access::htup::HEAP_XMIN_INVALID
                | crate::include::access::htup::HEAP_XMAX_INVALID,
        );
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(!visible);
        assert_eq!(hints, 0);
    }

    #[test]
    fn hint_fast_path_xmin_committed_xmax_committed_is_not_visible() {
        // Tuple was inserted then deleted — both committed.
        let txns = TransactionManager::default();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let bytes = make_tuple_bytes(
            1,
            2,
            0,
            crate::include::access::htup::HEAP_XMIN_COMMITTED
                | crate::include::access::htup::HEAP_XMAX_COMMITTED,
        );
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(!visible); // deleted
        assert_eq!(hints, 0);
    }

    // ---- Hint bit computation tests ----

    #[test]
    fn hints_set_xmin_committed_for_committed_insert() {
        use crate::include::access::htup::{HEAP_XMAX_INVALID, HEAP_XMIN_COMMITTED};
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        // INSERT sets HEAP_XMAX_INVALID on the tuple (xmax=0 means not deleted).
        let bytes = make_tuple_bytes(inserter, 0, 0, HEAP_XMAX_INVALID);
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(visible);
        assert!(
            hints & HEAP_XMIN_COMMITTED != 0,
            "should set XMIN_COMMITTED"
        );
        // HEAP_XMAX_INVALID was already set by INSERT — no new hint needed.
    }

    #[test]
    fn hints_set_xmin_invalid_for_aborted_insert() {
        use crate::include::access::htup::HEAP_XMIN_INVALID;
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.abort(inserter).unwrap();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        let bytes = make_tuple_bytes(inserter, 0, 0, 0);
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(!visible);
        assert!(hints & HEAP_XMIN_INVALID != 0, "should set XMIN_INVALID");
    }

    #[test]
    fn hints_not_set_for_in_progress_xmin() {
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        // Don't commit or abort — still in progress.
        let reader = txns.begin();
        let snapshot = txns.snapshot(reader).unwrap();

        let bytes = make_tuple_bytes(inserter, 0, 0, 0);
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(!visible);
        assert_eq!(hints, 0, "should not set any hints for in-progress xmin");
    }

    #[test]
    fn hints_set_xmax_committed_for_committed_delete() {
        use crate::include::access::htup::{HEAP_XMAX_COMMITTED, HEAP_XMIN_COMMITTED};
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();
        let deleter = txns.begin();
        txns.commit(deleter).unwrap();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        let bytes = make_tuple_bytes(inserter, deleter, 0, 0);
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(!visible); // deleted
        assert!(hints & HEAP_XMIN_COMMITTED != 0);
        assert!(hints & HEAP_XMAX_COMMITTED != 0);
    }

    #[test]
    fn hints_set_xmax_invalid_for_aborted_delete() {
        use crate::include::access::htup::{HEAP_XMAX_INVALID, HEAP_XMIN_COMMITTED};
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();
        let deleter = txns.begin();
        txns.abort(deleter).unwrap(); // rollback the delete
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        let bytes = make_tuple_bytes(inserter, deleter, 0, 0);
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(visible); // delete was rolled back, so still visible
        assert!(hints & HEAP_XMIN_COMMITTED != 0);
        assert!(
            hints & HEAP_XMAX_INVALID != 0,
            "aborted xmax should set XMAX_INVALID"
        );
    }

    #[test]
    fn hints_not_set_for_in_progress_xmax() {
        use crate::include::access::htup::HEAP_XMIN_COMMITTED;
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();
        let deleter = txns.begin();
        // deleter still in progress
        let reader = txns.begin();
        let snapshot = txns.snapshot(reader).unwrap();

        let bytes = make_tuple_bytes(inserter, deleter, 0, 0);
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(visible); // delete not committed yet
        assert!(hints & HEAP_XMIN_COMMITTED != 0, "xmin is committed");
        // Should NOT set xmax hints — transaction still in progress
        assert_eq!(
            hints & 0x0C00,
            0,
            "should not set xmax hints for in-progress xmax"
        );
    }

    #[test]
    fn hints_not_set_for_own_transaction_xmin() {
        let mut txns = TransactionManager::default();
        let my_xid = txns.begin();
        let snapshot = txns.snapshot(my_xid).unwrap();

        // Tuple inserted by our own transaction, cid=0 < current_cid=MAX
        let bytes = make_tuple_bytes(my_xid, 0, 0, 0);
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(visible);
        // Should not set XMIN_COMMITTED — our own txn isn't committed yet
        assert_eq!(
            hints & 0x0300,
            0,
            "should not set xmin hints for own transaction"
        );
    }

    #[test]
    fn hints_not_set_for_own_transaction_xmax() {
        use crate::include::access::htup::HEAP_XMIN_COMMITTED;
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();
        let my_xid = txns.begin();
        let snapshot = txns.snapshot(my_xid).unwrap();

        // Tuple deleted by our own transaction
        let bytes = make_tuple_bytes(inserter, my_xid, 0, 0);
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(!visible); // we deleted it
        assert!(hints & HEAP_XMIN_COMMITTED != 0);
        // Should not set xmax hints — our own txn isn't committed yet
        assert_eq!(
            hints & 0x0C00,
            0,
            "should not set xmax hints for own transaction"
        );
    }

    // ---- Round-trip: hints computed then applied then fast path ----

    #[test]
    fn hint_roundtrip_committed_insert_becomes_fast_path() {
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        let mut bytes = make_tuple_bytes(inserter, 0, 0, 0);
        // First check — slow path, computes hints
        let (visible1, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(visible1);
        assert_ne!(hints, 0);
        // Apply hints
        set_infomask(&mut bytes, hints);
        // Second check — should hit fast path
        let (visible2, hints2) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(visible2);
        assert_eq!(hints2, 0, "fast path should return no new hints");
    }

    #[test]
    fn hint_roundtrip_deleted_tuple_stays_invisible() {
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();
        let deleter = txns.begin();
        txns.commit(deleter).unwrap();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        let mut bytes = make_tuple_bytes(inserter, deleter, 0, 0);
        let (visible1, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(!visible1);
        set_infomask(&mut bytes, hints);
        let (visible2, _) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(
            !visible2,
            "deleted tuple must stay invisible after hint bits set"
        );
    }

    #[test]
    fn hint_roundtrip_aborted_insert_stays_invisible() {
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.abort(inserter).unwrap();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        let mut bytes = make_tuple_bytes(inserter, 0, 0, 0);
        let (visible1, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(!visible1);
        set_infomask(&mut bytes, hints);
        let (visible2, _) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(
            !visible2,
            "aborted insert must stay invisible after hint bits set"
        );
    }

    #[test]
    fn hint_roundtrip_aborted_delete_stays_visible() {
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();
        let deleter = txns.begin();
        txns.abort(deleter).unwrap(); // rollback delete
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        let mut bytes = make_tuple_bytes(inserter, deleter, 0, 0);
        let (visible1, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(visible1);
        set_infomask(&mut bytes, hints);
        let (visible2, _) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(
            visible2,
            "rolled-back delete must stay visible after hint bits set"
        );
    }

    // ---- Update scenario (old + new tuple versions) ----

    #[test]
    fn hint_roundtrip_update_old_version_invisible_new_version_visible() {
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();
        let updater = txns.begin();
        txns.commit(updater).unwrap();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        // Old version: xmin=inserter(committed), xmax=updater(committed)
        let mut old_bytes = make_tuple_bytes(inserter, updater, 0, 0);
        let (old_vis1, old_hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &old_bytes);
        assert!(!old_vis1, "old version should be invisible");
        set_infomask(&mut old_bytes, old_hints);
        let (old_vis2, _) = snapshot.tuple_bytes_visible_with_hints(&txns, &old_bytes);
        assert!(!old_vis2, "old version must stay invisible after hints");

        // New version: xmin=updater(committed), xmax=0
        let mut new_bytes = make_tuple_bytes(updater, 0, 0, 0);
        let (new_vis1, new_hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &new_bytes);
        assert!(new_vis1, "new version should be visible");
        set_infomask(&mut new_bytes, new_hints);
        let (new_vis2, _) = snapshot.tuple_bytes_visible_with_hints(&txns, &new_bytes);
        assert!(new_vis2, "new version must stay visible after hints");
    }

    #[test]
    fn hint_roundtrip_update_in_progress_old_still_visible() {
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();
        let updater = txns.begin();
        // updater still in progress
        let reader = txns.begin();
        let snapshot = txns.snapshot(reader).unwrap();

        // Old version: xmin=inserter(committed), xmax=updater(in-progress)
        let mut old_bytes = make_tuple_bytes(inserter, updater, 0, 0);
        let (old_vis, old_hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &old_bytes);
        assert!(old_vis, "old version visible while update in progress");
        // xmax hints should NOT be set (updater is in-progress)
        assert_eq!(
            old_hints & 0x0C00,
            0,
            "no xmax hints for in-progress updater"
        );
        set_infomask(&mut old_bytes, old_hints);

        // Now updater commits
        txns.commit(updater).unwrap();
        let snapshot2 = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        // With new snapshot, old version should be invisible
        let (old_vis2, old_hints2) = snapshot2.tuple_bytes_visible_with_hints(&txns, &old_bytes);
        assert!(!old_vis2, "old version invisible after update commits");
        // Now xmax hints should be set
        assert!(old_hints2 & crate::include::access::htup::HEAP_XMAX_COMMITTED != 0);
    }

    // ---- Fast path must still respect snapshot boundaries ----

    #[test]
    fn hint_fast_path_must_check_snapshot_for_xmin() {
        // A tuple's xmin committed AFTER snapshot was taken. Another scan sets
        // HEAP_XMIN_COMMITTED. The original snapshot must NOT see the tuple
        // as visible, even though the hint bit says "committed".
        use crate::include::access::htup::{HEAP_XMAX_INVALID, HEAP_XMIN_COMMITTED};
        let mut txns = TransactionManager::default();
        let inserter = txns.begin(); // xid=1
        let reader = txns.begin(); // xid=2

        // Reader takes snapshot BEFORE inserter commits.
        let old_snapshot = txns.snapshot(reader).unwrap();

        // Inserter commits.
        txns.commit(inserter).unwrap();

        // A different scan sets hint bits on the tuple (as would happen in practice).
        let mut bytes = make_tuple_bytes(inserter, 0, 0, HEAP_XMAX_INVALID);
        let new_snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let (vis_new, hints) = new_snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(vis_new, "new snapshot should see committed insert");
        assert!(hints & HEAP_XMIN_COMMITTED != 0);
        // Apply hints — simulating what the scan would do.
        set_infomask(&mut bytes, hints);

        // Now the OLD snapshot scans. The fast path sees XMIN_COMMITTED | XMAX_INVALID.
        // But xmin=inserter is in old_snapshot's in-progress set — must NOT be visible.
        let (vis_old, _) = old_snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(
            !vis_old,
            "old snapshot must NOT see tuple whose xmin committed after snapshot was taken, \
             even if HEAP_XMIN_COMMITTED hint bit is set"
        );
    }

    #[test]
    fn hint_fast_path_must_check_snapshot_for_xmax() {
        // A tuple's xmax (delete) committed AFTER snapshot was taken. Another scan
        // sets HEAP_XMAX_COMMITTED. The original snapshot should still see the tuple
        // (delete not yet visible to it).
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();
        let deleter = txns.begin(); // xid=2
        let reader = txns.begin(); // xid=3

        // Reader takes snapshot BEFORE deleter commits.
        let old_snapshot = txns.snapshot(reader).unwrap();

        // Deleter commits.
        txns.commit(deleter).unwrap();

        // Simulate: tuple was inserted (committed), then deleted (committed).
        // A new scan sets both XMIN_COMMITTED and XMAX_COMMITTED.
        let mut bytes = make_tuple_bytes(inserter, deleter, 0, 0);
        let new_snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let (vis_new, hints) = new_snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(
            !vis_new,
            "new snapshot should see deleted tuple as invisible"
        );
        set_infomask(&mut bytes, hints);

        // Old snapshot: delete committed after snapshot, so tuple should still be visible.
        let (vis_old, _) = old_snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(
            vis_old,
            "old snapshot must still see tuple whose xmax committed after snapshot was taken, \
             even if HEAP_XMAX_COMMITTED hint bit is set"
        );
    }

    // ---- Tests for xmax >= snapshot.xmax in hint fast path ----

    #[test]
    fn hint_fast_path_xmax_committed_after_snapshot_still_visible() {
        // Regression: if XMAX_COMMITTED is set but xmax >= snapshot.xmax,
        // the deleter started after our snapshot. The tuple should still be
        // visible. Without the xmax >= self.xmax check in the fast path,
        // transaction_active_in_snapshot returns false (out of range) and the
        // tuple is incorrectly hidden.
        use crate::include::access::htup::{HEAP_XMAX_COMMITTED, HEAP_XMIN_COMMITTED};
        let mut txns = TransactionManager::default();
        let inserter = txns.begin(); // xid=1
        txns.commit(inserter).unwrap();

        // Take snapshot before any delete happens.
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        // A deleter starts AFTER the snapshot.
        let deleter = txns.begin(); // xid=2, >= snapshot.xmax
        txns.commit(deleter).unwrap();

        // Another reader sets both XMIN_COMMITTED and XMAX_COMMITTED hints.
        let bytes = make_tuple_bytes(
            inserter,
            deleter,
            0,
            HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED,
        );

        // The original snapshot should still see the tuple (delete not yet visible).
        let result = snapshot.tuple_bytes_try_visible_from_hints(&bytes);
        assert_eq!(
            result,
            Some(true),
            "hint-only fast path must return visible when xmax >= snapshot.xmax"
        );

        let (visible, _) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(
            visible,
            "full check must also return visible when xmax >= snapshot.xmax"
        );
    }

    #[test]
    fn hint_fast_path_xmin_after_snapshot_not_visible() {
        // Regression: if XMIN_COMMITTED is set but xmin >= snapshot.xmax,
        // the inserter started after our snapshot. The tuple should NOT be
        // visible. Without the xmin >= self.xmax check, the fast path
        // incorrectly returns visible when XMAX_INVALID is also set.
        use crate::include::access::htup::{HEAP_XMAX_INVALID, HEAP_XMIN_COMMITTED};
        let mut txns = TransactionManager::default();
        let inserter = txns.begin(); // xid=FIRST_NORMAL_TRANSACTION_ID
        txns.commit(inserter).unwrap();

        // Take a snapshot after the first normal transaction commits.
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        // A new transaction inserts AFTER the snapshot.
        let new_inserter = txns.begin(); // xid=FIRST_NORMAL_TRANSACTION_ID + 1
        txns.commit(new_inserter).unwrap();

        // Another reader sets XMIN_COMMITTED on the new tuple.
        let bytes = make_tuple_bytes(new_inserter, 0, 0, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID);

        // The original snapshot must NOT see this tuple.
        let result = snapshot.tuple_bytes_try_visible_from_hints(&bytes);
        assert_eq!(
            result,
            Some(false),
            "hint-only fast path must return not-visible when xmin >= snapshot.xmax"
        );

        let (visible, _) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(
            !visible,
            "full check must also return not-visible when xmin >= snapshot.xmax"
        );
    }

    #[test]
    fn hint_only_path_resolves_common_cases() {
        // Verify that tuple_bytes_try_visible_from_hints resolves visibility
        // without needing a CLOG lookup for the common cases.
        use crate::include::access::htup::{
            HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMIN_COMMITTED, HEAP_XMIN_INVALID,
        };
        let mut txns = TransactionManager::default();
        let xid1 = txns.begin();
        txns.commit(xid1).unwrap();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        // Case 1: XMIN_COMMITTED | XMAX_INVALID → visible (no CLOG needed)
        let bytes = make_tuple_bytes(xid1, 0, 0, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID);
        assert_eq!(
            snapshot.tuple_bytes_try_visible_from_hints(&bytes),
            Some(true)
        );

        // Case 2: XMIN_COMMITTED | XMAX_COMMITTED → not visible (no CLOG needed)
        let xid2 = txns.begin();
        txns.commit(xid2).unwrap();
        let snapshot2 = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let bytes = make_tuple_bytes(xid1, xid2, 0, HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED);
        assert_eq!(
            snapshot2.tuple_bytes_try_visible_from_hints(&bytes),
            Some(false)
        );

        // Case 3: XMIN_INVALID → not visible (no CLOG needed)
        let bytes = make_tuple_bytes(xid1, 0, 0, HEAP_XMIN_INVALID);
        assert_eq!(
            snapshot.tuple_bytes_try_visible_from_hints(&bytes),
            Some(false)
        );

        // Case 4: No hint bits → needs CLOG (returns None)
        let bytes = make_tuple_bytes(xid1, 0, 0, 0);
        assert_eq!(snapshot.tuple_bytes_try_visible_from_hints(&bytes), None);

        // Case 5: XMIN_COMMITTED but no xmax hint → needs CLOG for xmax
        let bytes = make_tuple_bytes(xid1, xid2, 0, HEAP_XMIN_COMMITTED);
        assert_eq!(snapshot2.tuple_bytes_try_visible_from_hints(&bytes), None);
    }

    // ---- Tests for update/delete AFTER hint bits are already set ----

    #[test]
    fn hint_bits_cleared_on_delete_old_version_becomes_invisible() {
        // Regression test: INSERT sets HEAP_XMAX_INVALID. After hint bits are
        // fully set (XMIN_COMMITTED | XMAX_INVALID), a DELETE must clear
        // HEAP_XMAX_INVALID so the fast path doesn't incorrectly return visible.
        use crate::include::access::htup::{
            HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMIN_COMMITTED,
        };
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();

        // Tuple with full hint bits set (as if scanned after insert committed).
        let mut bytes = make_tuple_bytes(inserter, 0, 0, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID);

        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(
            visible,
            "tuple with XMIN_COMMITTED | XMAX_INVALID should be visible"
        );
        assert_eq!(hints, 0, "no new hints needed");

        // Now simulate DELETE: set xmax to a new transaction and clear HEAP_XMAX_INVALID.
        let deleter = txns.begin();
        bytes[4..8].copy_from_slice(&deleter.to_le_bytes());
        // Clear HEAP_XMAX_INVALID (as heap_delete does).
        let infomask = u16::from_le_bytes([bytes[20], bytes[21]]);
        let cleared = (infomask & !HEAP_XMAX_INVALID).to_le_bytes();
        bytes[20] = cleared[0];
        bytes[21] = cleared[1];

        // Before commit: tuple still visible (delete in progress).
        let snapshot2 = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let (vis2, _) = snapshot2.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(vis2, "tuple visible while delete is in progress");

        // After commit: tuple invisible.
        txns.commit(deleter).unwrap();
        let snapshot3 = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let (vis3, hints3) = snapshot3.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(!vis3, "tuple invisible after delete committed");
        assert!(
            hints3 & HEAP_XMAX_COMMITTED != 0,
            "should set XMAX_COMMITTED"
        );
    }

    #[test]
    fn hint_bits_cleared_on_update_old_version_invisible_new_visible() {
        use crate::include::access::htup::{
            HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMIN_COMMITTED,
        };
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();

        // Old version with full hint bits.
        let mut old_bytes =
            make_tuple_bytes(inserter, 0, 0, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID);

        // Simulate UPDATE: old version gets xmax, HEAP_XMAX_INVALID cleared.
        let updater = txns.begin();
        old_bytes[4..8].copy_from_slice(&updater.to_le_bytes());
        let infomask = u16::from_le_bytes([old_bytes[20], old_bytes[21]]);
        let cleared = (infomask & !HEAP_XMAX_INVALID).to_le_bytes();
        old_bytes[20] = cleared[0];
        old_bytes[21] = cleared[1];

        // New version: INSERT sets HEAP_XMAX_INVALID.
        let new_bytes = make_tuple_bytes(updater, 0, 0, HEAP_XMAX_INVALID);

        txns.commit(updater).unwrap();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        let (old_vis, old_hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &old_bytes);
        assert!(!old_vis, "old version invisible after update");
        assert!(old_hints & HEAP_XMAX_COMMITTED != 0);

        let (new_vis, new_hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &new_bytes);
        assert!(new_vis, "new version visible after update");
        assert!(new_hints & HEAP_XMIN_COMMITTED != 0);
    }

    #[test]
    fn hint_bits_after_aborted_delete_tuple_stays_visible() {
        use crate::include::access::htup::{HEAP_XMAX_INVALID, HEAP_XMIN_COMMITTED};
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();

        // Tuple with full hints.
        let mut bytes = make_tuple_bytes(inserter, 0, 0, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID);

        // DELETE starts then aborts.
        let deleter = txns.begin();
        bytes[4..8].copy_from_slice(&deleter.to_le_bytes());
        let infomask = u16::from_le_bytes([bytes[20], bytes[21]]);
        let cleared = (infomask & !HEAP_XMAX_INVALID).to_le_bytes();
        bytes[20] = cleared[0];
        bytes[21] = cleared[1];

        txns.abort(deleter).unwrap();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(visible, "tuple visible after aborted delete");
        assert!(
            hints & HEAP_XMAX_INVALID != 0,
            "should set XMAX_INVALID for aborted xmax"
        );

        // Apply hints and re-check — fast path should work.
        set_infomask(&mut bytes, hints);
        let (vis2, _) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(
            vis2,
            "tuple still visible after hint bits updated for aborted delete"
        );
    }

    #[test]
    fn hint_xmax_invalid_not_set_for_xmax_zero_without_insert_flag() {
        // Regression test for the bug: visibility check was setting
        // HEAP_XMAX_INVALID for xmax=0 tuples. This is wrong because
        // INSERT should set that flag. If a tuple somehow has xmax=0
        // without the flag, the visibility check should not add it.
        use crate::include::access::htup::{HEAP_XMAX_INVALID, HEAP_XMIN_COMMITTED};
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        // Tuple WITHOUT HEAP_XMAX_INVALID (simulating a bug or old tuple format).
        let bytes = make_tuple_bytes(inserter, 0, 0, 0);
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(visible);
        assert!(hints & HEAP_XMIN_COMMITTED != 0);
        // Should NOT set HEAP_XMAX_INVALID — that's INSERT's job.
        assert_eq!(
            hints & HEAP_XMAX_INVALID,
            0,
            "visibility check must not set XMAX_INVALID for xmax=0; INSERT sets it"
        );
    }

    // ---- Exhaustive permutation test ----

    /// Test ALL combinations of (xmin status, xmax status, hint bits) to ensure
    /// that tuple_bytes_visible_with_hints matches check_visibility and that
    /// applying hint bits doesn't change the visibility answer.
    #[test]
    fn hint_bits_exhaustive_permutations() {
        use crate::include::access::htup::{
            HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMIN_COMMITTED, HEAP_XMIN_INVALID,
        };

        // xmin states: committed, aborted, in-progress, own-txn, bootstrap(0)
        // xmax states: 0 (with XMAX_INVALID), committed, aborted, in-progress, own-txn
        // For each combo: check visibility matches, hints are correct, round-trip is safe.

        let mut txns = TransactionManager::default();
        let committed_xid = txns.begin();
        txns.commit(committed_xid).unwrap();
        let aborted_xid = txns.begin();
        txns.abort(aborted_xid).unwrap();
        let in_progress_xid = txns.begin();
        let my_xid = txns.begin();
        let snapshot = txns.snapshot(my_xid).unwrap();

        let xmin_cases = [
            ("xmin=0(bootstrap)", INVALID_TRANSACTION_ID),
            ("xmin=committed", committed_xid),
            ("xmin=aborted", aborted_xid),
            ("xmin=in_progress", in_progress_xid),
            ("xmin=own_txn", my_xid),
        ];

        let xmax_cases: Vec<(&str, u32, u16)> = vec![
            ("xmax=0+INVALID", 0, HEAP_XMAX_INVALID), // normal insert
            ("xmax=committed", committed_xid, 0),
            ("xmax=aborted", aborted_xid, 0),
            ("xmax=in_progress", in_progress_xid, 0),
            ("xmax=own_txn", my_xid, 0),
        ];

        for (xmin_label, xmin) in &xmin_cases {
            for (xmax_label, xmax, xmax_infomask) in &xmax_cases {
                let label = format!("{xmin_label}, {xmax_label}");

                // Build tuple bytes with no hint bits (except XMAX_INVALID from insert).
                let bytes_no_hints = make_tuple_bytes(*xmin, *xmax, 0, *xmax_infomask);

                // Get the ground truth from check_visibility.
                let cid = 0u32;
                let expected = snapshot.check_visibility(&txns, *xmin, *xmax, cid);

                // tuple_bytes_visible_with_hints should agree.
                let (actual, hints) =
                    snapshot.tuple_bytes_visible_with_hints(&txns, &bytes_no_hints);
                assert_eq!(
                    actual, expected,
                    "visibility mismatch for {label}: expected={expected}, got={actual}"
                );

                // Apply hints and re-check — result must be the same.
                if hints != 0 {
                    let mut bytes_with_hints = bytes_no_hints.clone();
                    set_infomask(&mut bytes_with_hints, hints);
                    let (after_hints, _) =
                        snapshot.tuple_bytes_visible_with_hints(&txns, &bytes_with_hints);
                    assert_eq!(
                        after_hints, expected,
                        "visibility changed after applying hints for {label}: \
                         expected={expected}, got={after_hints}, hints=0x{hints:04x}"
                    );
                }

                // Also test with ALL hint bits pre-set (simulate a fully-hinted tuple).
                // This tests the fast path exhaustively.
                let all_xmin_hints = [0, HEAP_XMIN_COMMITTED, HEAP_XMIN_INVALID];
                let all_xmax_hints = [0, HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID];
                for xmin_hint in &all_xmin_hints {
                    for xmax_hint in &all_xmax_hints {
                        let pre_hints = xmax_infomask | xmin_hint | xmax_hint;
                        let bytes_pre = make_tuple_bytes(*xmin, *xmax, 0, pre_hints);
                        let (vis_pre, _) =
                            snapshot.tuple_bytes_visible_with_hints(&txns, &bytes_pre);

                        // If the hint bits are CORRECT (matching actual status), result must match.
                        // If hint bits are WRONG (e.g. XMIN_COMMITTED but xmin is aborted),
                        // that's an invalid state that wouldn't happen in practice — skip.
                        let xmin_hint_correct = match txns.status(*xmin) {
                            Some(TransactionStatus::Committed) => {
                                *xmin_hint == HEAP_XMIN_COMMITTED || *xmin_hint == 0
                            }
                            Some(TransactionStatus::Aborted) => {
                                *xmin_hint == HEAP_XMIN_INVALID || *xmin_hint == 0
                            }
                            _ => *xmin_hint == 0, // in-progress/own/bootstrap: no hints
                        };
                        let xmax_hint_correct = if *xmax == 0 {
                            *xmax_hint == 0 // XMAX_INVALID comes from base_infomask
                        } else {
                            match txns.status(*xmax) {
                                Some(TransactionStatus::Committed) => {
                                    *xmax_hint == HEAP_XMAX_COMMITTED || *xmax_hint == 0
                                }
                                Some(TransactionStatus::Aborted) => {
                                    *xmax_hint == HEAP_XMAX_INVALID || *xmax_hint == 0
                                }
                                _ => *xmax_hint == 0,
                            }
                        };

                        if xmin_hint_correct && xmax_hint_correct {
                            assert_eq!(
                                vis_pre, expected,
                                "fast path mismatch for {label} with hints \
                                 xmin=0x{xmin_hint:04x} xmax=0x{xmax_hint:04x}: \
                                 expected={expected}, got={vis_pre}"
                            );
                        }
                    }
                }
            }
        }
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
