use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write as _};
use std::path::{Path, PathBuf};

use crate::access::heap::tuple::HeapTuple;

pub type TransactionId = u32;
pub type CommandId = u32;
pub const INVALID_TRANSACTION_ID: TransactionId = 0;
/// Header: next_xid(4). Status bytes start at offset 4.
const STATUS_FILE_HEADER_SIZE: usize = 4;

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

#[derive(Debug, Default)]
pub struct TransactionManager {
    next_xid: TransactionId,
    statuses: BTreeMap<TransactionId, TransactionStatus>,
    status_path: Option<PathBuf>,
    /// Open file handle for single-byte status writes.
    /// Avoids open/close per commit — just seek + write 1 byte.
    status_file: Option<File>,
}

impl Clone for TransactionManager {
    fn clone(&self) -> Self {
        Self {
            next_xid: self.next_xid,
            statuses: self.statuses.clone(),
            status_path: self.status_path.clone(),
            status_file: self.status_file.as_ref().and_then(|f| f.try_clone().ok()),
        }
    }
}

fn status_to_byte(status: TransactionStatus) -> u8 {
    match status {
        TransactionStatus::InProgress => 1,
        TransactionStatus::Committed => 2,
        TransactionStatus::Aborted => 3,
    }
}

fn byte_to_status(b: u8) -> Option<TransactionStatus> {
    match b {
        1 => Some(TransactionStatus::InProgress),
        2 => Some(TransactionStatus::Committed),
        3 => Some(TransactionStatus::Aborted),
        _ => None,
    }
}

impl TransactionManager {
    pub fn new_durable(base_dir: impl Into<PathBuf>) -> Result<Self, MvccError> {
        let path = Self::status_path(base_dir.into());
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| MvccError::Io(e.to_string()))?;
        }

        if path.exists() {
            let (next_xid, statuses) = load_status_file(&path)?;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|e| MvccError::Io(e.to_string()))?;
            Ok(Self {
                next_xid,
                statuses,
                status_path: Some(path),
                status_file: Some(file),
            })
        } else {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .map_err(|e| MvccError::Io(e.to_string()))?;
            // Write header: next_xid = 0
            file.write_all(&INVALID_TRANSACTION_ID.to_le_bytes())
                .map_err(|e| MvccError::Io(e.to_string()))?;
            Ok(Self {
                next_xid: INVALID_TRANSACTION_ID,
                statuses: BTreeMap::new(),
                status_path: Some(path),
                status_file: Some(file),
            })
        }
    }

    pub fn begin(&mut self) -> TransactionId {
        self.next_xid += 1;
        let xid = self.next_xid;
        self.statuses.insert(xid, TransactionStatus::InProgress);
        self.write_status_byte(xid, TransactionStatus::InProgress);
        self.write_next_xid();
        xid
    }

    pub fn commit(&mut self, xid: TransactionId) -> Result<(), MvccError> {
        match self.statuses.get_mut(&xid) {
            Some(status @ TransactionStatus::InProgress) => {
                *status = TransactionStatus::Committed;
                self.write_status_byte(xid, TransactionStatus::Committed);
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
                self.write_status_byte(xid, TransactionStatus::Aborted);
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

    /// Write one status byte at offset `HEADER_SIZE + xid`. No fsync.
    fn write_status_byte(&mut self, xid: TransactionId, status: TransactionStatus) {
        let Some(ref mut file) = self.status_file else { return };
        let offset = STATUS_FILE_HEADER_SIZE as u64 + xid as u64;
        let _ = file.seek(SeekFrom::Start(offset));
        let _ = file.write_all(&[status_to_byte(status)]);
    }

    /// Update next_xid in the header (first 4 bytes).
    fn write_next_xid(&mut self) {
        let Some(ref mut file) = self.status_file else { return };
        let _ = file.seek(SeekFrom::Start(0));
        let _ = file.write_all(&self.next_xid.to_le_bytes());
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

    /// Check visibility from raw on-page tuple bytes without parsing.
    #[inline(always)]
    pub fn tuple_bytes_visible(&self, txns: &TransactionManager, bytes: &[u8]) -> bool {
        use crate::access::heap::tuple::{
            HEAP_XMIN_COMMITTED, HEAP_XMIN_INVALID, HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID,
            INFOMASK_OFFSET,
        };
        let infomask = u16::from_le_bytes([bytes[INFOMASK_OFFSET], bytes[INFOMASK_OFFSET + 1]]);
        let xmin = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);

        // Fast path: hint bits let us skip txns.status() lookups, but we
        // still must check the snapshot (a committed xid might be in our
        // in-progress set if it committed after our snapshot was taken).
        if infomask & HEAP_XMIN_COMMITTED != 0 {
            // xmin committed — but maybe not according to our snapshot.
            if self.transaction_active_in_snapshot(xmin) {
                return false;
            }
            if infomask & HEAP_XMAX_INVALID != 0 {
                return true; // inserted, not deleted
            }
            if infomask & HEAP_XMAX_COMMITTED != 0 {
                let xmax = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
                // xmax committed — but maybe not according to our snapshot.
                return self.transaction_active_in_snapshot(xmax);
            }
            // xmax has no hint — fall through to full check.
        }
        if infomask & HEAP_XMIN_INVALID != 0 {
            return false;
        }

        let xmax = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        let cid = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        self.check_visibility(txns, xmin, xmax, cid)
    }

    /// Check visibility and return which hint bits should be set on the tuple.
    /// Returns (visible, hints_to_or_into_infomask).
    /// Following PostgreSQL's approach: hint bits are set based on the
    /// *definitive* transaction status (committed or aborted), never for
    /// in-progress or snapshot-relative decisions.
    #[inline(always)]
    pub fn tuple_bytes_visible_with_hints(&self, txns: &TransactionManager, bytes: &[u8]) -> (bool, u16) {
        use crate::access::heap::tuple::{
            HEAP_XMIN_COMMITTED, HEAP_XMIN_INVALID, HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID,
            INFOMASK_OFFSET,
        };
        let infomask = u16::from_le_bytes([bytes[INFOMASK_OFFSET], bytes[INFOMASK_OFFSET + 1]]);

        let xmin = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);

        // Fast path: hint bits let us skip txns.status() lookups, but we
        // still must check the snapshot (a committed xid might be in our
        // in-progress set if it committed after our snapshot was taken).
        if infomask & HEAP_XMIN_COMMITTED != 0 {
            if self.transaction_active_in_snapshot(xmin) {
                return (false, 0);
            }
            if infomask & HEAP_XMAX_INVALID != 0 {
                return (true, 0); // inserted, not deleted
            }
            if infomask & HEAP_XMAX_COMMITTED != 0 {
                let xmax = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
                return (self.transaction_active_in_snapshot(xmax), 0);
            }
            // xmax has no hint — fall through to full check.
        }
        if infomask & HEAP_XMIN_INVALID != 0 {
            return (false, 0);
        }

        // Slow path: do the full visibility check, collecting hints along the way.
        let xmax = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        let cid = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);

        let visible = self.check_visibility(txns, xmin, xmax, cid);

        // Set hint bits only for definitive, snapshot-independent facts:
        // a committed or aborted transaction will never change status.
        let mut hints: u16 = 0;

        // Determine xmin hint.
        let _xmin_settled = if infomask & (HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID) != 0 {
            true // already has hints
        } else if xmin == INVALID_TRANSACTION_ID || xmin == self.current_xid {
            false // can't set hints for bootstrap or own txn
        } else {
            match txns.status(xmin) {
                Some(TransactionStatus::Committed) => { hints |= HEAP_XMIN_COMMITTED; true }
                Some(TransactionStatus::Aborted) => { hints |= HEAP_XMIN_INVALID; true }
                _ => false,
            }
        };

        // Only set xmax hints if xmin is definitively committed.
        // HEAP_XMAX_INVALID for xmax=0 is set during INSERT, not here.
        let xmin_known_committed = (infomask & HEAP_XMIN_COMMITTED != 0)
            || (hints & HEAP_XMIN_COMMITTED != 0);
        if xmin_known_committed
            && infomask & (HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID) == 0
            && xmax != INVALID_TRANSACTION_ID
            && xmax != self.current_xid
        {
            match txns.status(xmax) {
                Some(TransactionStatus::Committed) => hints |= HEAP_XMAX_COMMITTED,
                Some(TransactionStatus::Aborted) => hints |= HEAP_XMAX_INVALID,
                _ => {}
            }
        }

        (visible, hints)
    }

    pub fn tuple_visible(&self, txns: &TransactionManager, tuple: &HeapTuple) -> bool {
        self.check_visibility(txns, tuple.header.xmin, tuple.header.xmax, tuple.header.cid_or_xvac)
    }

    #[inline(always)]
    fn check_visibility(&self, txns: &TransactionManager, xmin: u32, xmax: u32, cid: u32) -> bool {
        if xmin == INVALID_TRANSACTION_ID {
            return true;
        }
        if xmin == self.current_xid {
            return cid < self.current_cid;
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

/// Load status file: header is next_xid (4 bytes), then 1 byte per xid
/// at offset HEADER_SIZE + xid.
fn load_status_file(
    path: &Path,
) -> Result<(TransactionId, BTreeMap<TransactionId, TransactionStatus>), MvccError> {
    let bytes = fs::read(path).map_err(|e| MvccError::Io(e.to_string()))?;
    if bytes.len() < STATUS_FILE_HEADER_SIZE {
        return Err(MvccError::CorruptStatusFile("header too short"));
    }

    let next_xid = u32::from_le_bytes(bytes[0..4].try_into().unwrap());

    let mut statuses = BTreeMap::new();
    for (i, &b) in bytes[STATUS_FILE_HEADER_SIZE..].iter().enumerate() {
        if let Some(status) = byte_to_status(b) {
            statuses.insert(i as TransactionId, status);
        }
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
        let txns = TransactionManager::default();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let bytes = make_tuple_bytes(1, 0, 0,
            crate::access::heap::tuple::HEAP_XMIN_COMMITTED | crate::access::heap::tuple::HEAP_XMAX_INVALID);
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(visible);
        assert_eq!(hints, 0); // no new hints needed
    }

    #[test]
    fn hint_fast_path_xmin_invalid_is_not_visible() {
        let txns = TransactionManager::default();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let bytes = make_tuple_bytes(1, 0, 0,
            crate::access::heap::tuple::HEAP_XMIN_INVALID | crate::access::heap::tuple::HEAP_XMAX_INVALID);
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(!visible);
        assert_eq!(hints, 0);
    }

    #[test]
    fn hint_fast_path_xmin_committed_xmax_committed_is_not_visible() {
        // Tuple was inserted then deleted — both committed.
        let txns = TransactionManager::default();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let bytes = make_tuple_bytes(1, 2, 0,
            crate::access::heap::tuple::HEAP_XMIN_COMMITTED | crate::access::heap::tuple::HEAP_XMAX_COMMITTED);
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(!visible); // deleted
        assert_eq!(hints, 0);
    }

    // ---- Hint bit computation tests ----

    #[test]
    fn hints_set_xmin_committed_for_committed_insert() {
        use crate::access::heap::tuple::{HEAP_XMIN_COMMITTED, HEAP_XMAX_INVALID};
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        // INSERT sets HEAP_XMAX_INVALID on the tuple (xmax=0 means not deleted).
        let bytes = make_tuple_bytes(inserter, 0, 0, HEAP_XMAX_INVALID);
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(visible);
        assert!(hints & HEAP_XMIN_COMMITTED != 0, "should set XMIN_COMMITTED");
        // HEAP_XMAX_INVALID was already set by INSERT — no new hint needed.
    }

    #[test]
    fn hints_set_xmin_invalid_for_aborted_insert() {
        use crate::access::heap::tuple::HEAP_XMIN_INVALID;
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
        use crate::access::heap::tuple::{HEAP_XMIN_COMMITTED, HEAP_XMAX_COMMITTED};
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
        use crate::access::heap::tuple::{HEAP_XMIN_COMMITTED, HEAP_XMAX_INVALID};
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
        assert!(hints & HEAP_XMAX_INVALID != 0, "aborted xmax should set XMAX_INVALID");
    }

    #[test]
    fn hints_not_set_for_in_progress_xmax() {
        use crate::access::heap::tuple::HEAP_XMIN_COMMITTED;
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
        assert_eq!(hints & 0x0C00, 0, "should not set xmax hints for in-progress xmax");
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
        assert_eq!(hints & 0x0300, 0, "should not set xmin hints for own transaction");
    }

    #[test]
    fn hints_not_set_for_own_transaction_xmax() {
        use crate::access::heap::tuple::HEAP_XMIN_COMMITTED;
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
        assert_eq!(hints & 0x0C00, 0, "should not set xmax hints for own transaction");
    }

    // ---- Round-trip: hints computed then applied then fast path ----

    #[test]
    fn hint_roundtrip_committed_insert_becomes_fast_path() {
        use crate::access::heap::tuple::{HEAP_XMIN_COMMITTED, HEAP_XMAX_INVALID};
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
        use crate::access::heap::tuple::{HEAP_XMIN_COMMITTED, HEAP_XMAX_COMMITTED};
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
        assert!(!visible2, "deleted tuple must stay invisible after hint bits set");
    }

    #[test]
    fn hint_roundtrip_aborted_insert_stays_invisible() {
        use crate::access::heap::tuple::HEAP_XMIN_INVALID;
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.abort(inserter).unwrap();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        let mut bytes = make_tuple_bytes(inserter, 0, 0, 0);
        let (visible1, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(!visible1);
        set_infomask(&mut bytes, hints);
        let (visible2, _) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(!visible2, "aborted insert must stay invisible after hint bits set");
    }

    #[test]
    fn hint_roundtrip_aborted_delete_stays_visible() {
        use crate::access::heap::tuple::{HEAP_XMIN_COMMITTED, HEAP_XMAX_INVALID};
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
        assert!(visible2, "rolled-back delete must stay visible after hint bits set");
    }

    // ---- Update scenario (old + new tuple versions) ----

    #[test]
    fn hint_roundtrip_update_old_version_invisible_new_version_visible() {
        use crate::access::heap::tuple::{HEAP_XMIN_COMMITTED, HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID};
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
        use crate::access::heap::tuple::HEAP_XMIN_COMMITTED;
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
        assert_eq!(old_hints & 0x0C00, 0, "no xmax hints for in-progress updater");
        set_infomask(&mut old_bytes, old_hints);

        // Now updater commits
        txns.commit(updater).unwrap();
        let snapshot2 = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();

        // With new snapshot, old version should be invisible
        let (old_vis2, old_hints2) = snapshot2.tuple_bytes_visible_with_hints(&txns, &old_bytes);
        assert!(!old_vis2, "old version invisible after update commits");
        // Now xmax hints should be set
        assert!(old_hints2 & crate::access::heap::tuple::HEAP_XMAX_COMMITTED != 0);
    }

    // ---- Fast path must still respect snapshot boundaries ----

    #[test]
    fn hint_fast_path_must_check_snapshot_for_xmin() {
        // A tuple's xmin committed AFTER snapshot was taken. Another scan sets
        // HEAP_XMIN_COMMITTED. The original snapshot must NOT see the tuple
        // as visible, even though the hint bit says "committed".
        use crate::access::heap::tuple::{HEAP_XMIN_COMMITTED, HEAP_XMAX_INVALID};
        let mut txns = TransactionManager::default();
        let inserter = txns.begin(); // xid=1
        let reader = txns.begin();   // xid=2

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
        assert!(!vis_old,
            "old snapshot must NOT see tuple whose xmin committed after snapshot was taken, \
             even if HEAP_XMIN_COMMITTED hint bit is set");
    }

    #[test]
    fn hint_fast_path_must_check_snapshot_for_xmax() {
        // A tuple's xmax (delete) committed AFTER snapshot was taken. Another scan
        // sets HEAP_XMAX_COMMITTED. The original snapshot should still see the tuple
        // (delete not yet visible to it).
        use crate::access::heap::tuple::{HEAP_XMIN_COMMITTED, HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID};
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();
        let deleter = txns.begin();   // xid=2
        let reader = txns.begin();    // xid=3

        // Reader takes snapshot BEFORE deleter commits.
        let old_snapshot = txns.snapshot(reader).unwrap();

        // Deleter commits.
        txns.commit(deleter).unwrap();

        // Simulate: tuple was inserted (committed), then deleted (committed).
        // A new scan sets both XMIN_COMMITTED and XMAX_COMMITTED.
        let mut bytes = make_tuple_bytes(inserter, deleter, 0, 0);
        let new_snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let (vis_new, hints) = new_snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(!vis_new, "new snapshot should see deleted tuple as invisible");
        set_infomask(&mut bytes, hints);

        // Old snapshot: delete committed after snapshot, so tuple should still be visible.
        let (vis_old, _) = old_snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(vis_old,
            "old snapshot must still see tuple whose xmax committed after snapshot was taken, \
             even if HEAP_XMAX_COMMITTED hint bit is set");
    }

    // ---- Tests for update/delete AFTER hint bits are already set ----

    #[test]
    fn hint_bits_cleared_on_delete_old_version_becomes_invisible() {
        // Regression test: INSERT sets HEAP_XMAX_INVALID. After hint bits are
        // fully set (XMIN_COMMITTED | XMAX_INVALID), a DELETE must clear
        // HEAP_XMAX_INVALID so the fast path doesn't incorrectly return visible.
        use crate::access::heap::tuple::{
            HEAP_XMIN_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMAX_COMMITTED,
        };
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();

        // Tuple with full hint bits set (as if scanned after insert committed).
        let mut bytes = make_tuple_bytes(inserter, 0, 0, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID);

        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let (visible, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(visible, "tuple with XMIN_COMMITTED | XMAX_INVALID should be visible");
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
        assert!(hints3 & HEAP_XMAX_COMMITTED != 0, "should set XMAX_COMMITTED");
    }

    #[test]
    fn hint_bits_cleared_on_update_old_version_invisible_new_visible() {
        use crate::access::heap::tuple::{
            HEAP_XMIN_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMAX_COMMITTED,
        };
        let mut txns = TransactionManager::default();
        let inserter = txns.begin();
        txns.commit(inserter).unwrap();

        // Old version with full hint bits.
        let mut old_bytes = make_tuple_bytes(inserter, 0, 0, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID);

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
        use crate::access::heap::tuple::{
            HEAP_XMIN_COMMITTED, HEAP_XMAX_INVALID,
        };
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
        assert!(hints & HEAP_XMAX_INVALID != 0, "should set XMAX_INVALID for aborted xmax");

        // Apply hints and re-check — fast path should work.
        set_infomask(&mut bytes, hints);
        let (vis2, _) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes);
        assert!(vis2, "tuple still visible after hint bits updated for aborted delete");
    }

    #[test]
    fn hint_xmax_invalid_not_set_for_xmax_zero_without_insert_flag() {
        // Regression test for the bug: visibility check was setting
        // HEAP_XMAX_INVALID for xmax=0 tuples. This is wrong because
        // INSERT should set that flag. If a tuple somehow has xmax=0
        // without the flag, the visibility check should not add it.
        use crate::access::heap::tuple::{HEAP_XMIN_COMMITTED, HEAP_XMAX_INVALID};
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
        assert_eq!(hints & HEAP_XMAX_INVALID, 0,
            "visibility check must not set XMAX_INVALID for xmax=0; INSERT sets it");
    }

    #[test]
    // ---- Exhaustive permutation test ----

    /// Test ALL combinations of (xmin status, xmax status, hint bits) to ensure
    /// that tuple_bytes_visible_with_hints matches check_visibility and that
    /// applying hint bits doesn't change the visibility answer.
    #[test]
    fn hint_bits_exhaustive_permutations() {
        use crate::access::heap::tuple::{
            HEAP_XMIN_COMMITTED, HEAP_XMIN_INVALID, HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID,
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

        struct Case {
            label: &'static str,
            xmin: u32,
            xmax: u32,
            base_infomask: u16,
        }

        let xmin_cases = [
            ("xmin=0(bootstrap)", INVALID_TRANSACTION_ID),
            ("xmin=committed", committed_xid),
            ("xmin=aborted", aborted_xid),
            ("xmin=in_progress", in_progress_xid),
            ("xmin=own_txn", my_xid),
        ];

        let xmax_cases: Vec<(&str, u32, u16)> = vec![
            ("xmax=0+INVALID", 0, HEAP_XMAX_INVALID),       // normal insert
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
                let (actual, hints) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes_no_hints);
                assert_eq!(actual, expected,
                    "visibility mismatch for {label}: expected={expected}, got={actual}");

                // Apply hints and re-check — result must be the same.
                if hints != 0 {
                    let mut bytes_with_hints = bytes_no_hints.clone();
                    set_infomask(&mut bytes_with_hints, hints);
                    let (after_hints, _) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes_with_hints);
                    assert_eq!(after_hints, expected,
                        "visibility changed after applying hints for {label}: \
                         expected={expected}, got={after_hints}, hints=0x{hints:04x}");
                }

                // Also test with ALL hint bits pre-set (simulate a fully-hinted tuple).
                // This tests the fast path exhaustively.
                let all_xmin_hints = [0, HEAP_XMIN_COMMITTED, HEAP_XMIN_INVALID];
                let all_xmax_hints = [0, HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID];
                for xmin_hint in &all_xmin_hints {
                    for xmax_hint in &all_xmax_hints {
                        let pre_hints = xmax_infomask | xmin_hint | xmax_hint;
                        let bytes_pre = make_tuple_bytes(*xmin, *xmax, 0, pre_hints);
                        let (vis_pre, _) = snapshot.tuple_bytes_visible_with_hints(&txns, &bytes_pre);

                        // If the hint bits are CORRECT (matching actual status), result must match.
                        // If hint bits are WRONG (e.g. XMIN_COMMITTED but xmin is aborted),
                        // that's an invalid state that wouldn't happen in practice — skip.
                        let xmin_hint_correct = match txns.status(*xmin) {
                            Some(TransactionStatus::Committed) => *xmin_hint == HEAP_XMIN_COMMITTED || *xmin_hint == 0,
                            Some(TransactionStatus::Aborted) => *xmin_hint == HEAP_XMIN_INVALID || *xmin_hint == 0,
                            _ => *xmin_hint == 0, // in-progress/own/bootstrap: no hints
                        };
                        let xmax_hint_correct = if *xmax == 0 {
                            *xmax_hint == 0 // XMAX_INVALID comes from base_infomask
                        } else {
                            match txns.status(*xmax) {
                                Some(TransactionStatus::Committed) => *xmax_hint == HEAP_XMAX_COMMITTED || *xmax_hint == 0,
                                Some(TransactionStatus::Aborted) => *xmax_hint == HEAP_XMAX_INVALID || *xmax_hint == 0,
                                _ => *xmax_hint == 0,
                            }
                        };

                        if xmin_hint_correct && xmax_hint_correct {
                            assert_eq!(vis_pre, expected,
                                "fast path mismatch for {label} with hints \
                                 xmin=0x{xmin_hint:04x} xmax=0x{xmax_hint:04x}: \
                                 expected={expected}, got={vis_pre}");
                        }
                    }
                }
            }
        }
    }

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
