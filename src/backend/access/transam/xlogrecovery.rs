// :HACK: Recovery needs root AM redo callbacks until the redo modules move
// into `pgrust_access` with the rest of WAL/recovery.
use std::collections::HashSet;
use std::path::Path;

use pgrust_access::transam::xact::TransactionManager;
use pgrust_access::transam::xlog::{INVALID_LSN, Lsn, WalError};
use pgrust_access::transam::xlogreader::DecodedXLogRecord;
use pgrust_access::transam::xlogrecovery as access_xlogrecovery;
use pgrust_storage::smgr::md::MdStorageManager;

struct RootRedoServices;

impl access_xlogrecovery::AccessRedoServices for RootRedoServices {
    fn btree_redo(
        &self,
        smgr: &mut MdStorageManager,
        record_lsn: Lsn,
        record: &DecodedXLogRecord,
    ) -> Result<(), WalError> {
        crate::backend::access::nbtree::nbtxlog::btree_redo(smgr, record_lsn, record)
    }

    fn gist_redo(
        &self,
        smgr: &mut MdStorageManager,
        record_lsn: Lsn,
        record: &DecodedXLogRecord,
    ) -> Result<(), WalError> {
        crate::backend::access::gist::wal::gist_redo(smgr, record_lsn, record)
    }

    fn gin_redo(
        &self,
        smgr: &mut MdStorageManager,
        record_lsn: Lsn,
        record: &DecodedXLogRecord,
    ) -> Result<(), WalError> {
        crate::backend::access::gin::wal::gin_redo(smgr, record_lsn, record)
    }

    fn hash_redo(
        &self,
        smgr: &mut MdStorageManager,
        record_lsn: Lsn,
        record: &DecodedXLogRecord,
    ) -> Result<(), WalError> {
        crate::backend::access::hash::wal::hash_redo(smgr, record_lsn, record)
    }
}

pub use pgrust_access::transam::xlogrecovery::{AccessRedoServices, RecoveryStats};

pub fn perform_wal_recovery(
    wal_dir: &Path,
    smgr: &mut MdStorageManager,
    txns: &mut TransactionManager,
) -> Result<RecoveryStats, WalError> {
    perform_wal_recovery_from(wal_dir, smgr, txns, INVALID_LSN)
}

pub fn perform_wal_recovery_from(
    wal_dir: &Path,
    smgr: &mut MdStorageManager,
    txns: &mut TransactionManager,
    start_lsn: Lsn,
) -> Result<RecoveryStats, WalError> {
    let redo = RootRedoServices;
    access_xlogrecovery::perform_wal_recovery_from_with_redo(wal_dir, smgr, txns, start_lsn, &redo)
}

pub fn perform_wal_recovery_from_preserving_xids(
    wal_dir: &Path,
    smgr: &mut MdStorageManager,
    txns: &mut TransactionManager,
    start_lsn: Lsn,
    preserve_in_progress_xids: &HashSet<u32>,
) -> Result<RecoveryStats, WalError> {
    let redo = RootRedoServices;
    access_xlogrecovery::perform_wal_recovery_from_preserving_xids_with_redo(
        wal_dir,
        smgr,
        txns,
        start_lsn,
        preserve_in_progress_xids,
        &redo,
    )
}
