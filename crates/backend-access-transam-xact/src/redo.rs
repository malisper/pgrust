//! Transaction WAL redo: `xact_redo` (xact.c:6363), `xact_redo_commit`
//! (xact.c:6130), and `xact_redo_abort` (xact.c:6284), plus the record
//! parsers `ParseCommitRecord` / `ParseAbortRecord` (xactdesc.c) which pair
//! with the writers in `wal.rs` (the rmgr-desc unit reuses these exports).

use crate::*;
use types_core::RepOriginId;
use types_error::PANIC;
use types_storage::{RelFileLocator, SharedInvalidationMessage, SHARED_INVALIDATION_MESSAGE_SIZE};
use types_wal::{
    xact_completion_apply_feedback, xact_completion_force_sync_commit,
    xact_completion_relcache_init_file_inval, ParsedAbort, ParsedCommit, XlXactStatsItem,
    STANDBY_DISABLED, STANDBY_INITIALIZED, XACT_XINFO_HAS_AE_LOCKS, XACT_XINFO_HAS_DBINFO,
    XACT_XINFO_HAS_DROPPED_STATS, XACT_XINFO_HAS_GID, XACT_XINFO_HAS_INVALS,
    XACT_XINFO_HAS_ORIGIN, XACT_XINFO_HAS_RELFILELOCATORS, XACT_XINFO_HAS_SUBXACTS,
    XACT_XINFO_HAS_TWOPHASE, XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED, XLOG_XACT_ASSIGNMENT,
    XLOG_XACT_COMMIT, XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_HAS_INFO, XLOG_XACT_INVALIDATIONS,
    XLOG_XACT_OPMASK, XLOG_XACT_PREPARE,
};

use backend_access_transam_commit_ts_seams as commit_ts_seams;
use backend_access_transam_transam_seams as transam_seams;
use backend_access_transam_twophase_seams as twophase_seams;
use backend_access_transam_xlogutils_seams as xlogutils_seams;
use backend_replication_logical_origin_seams as origin_seams;
use backend_access_transam_xlogrecovery_seams as xlogrecovery_seams;
use backend_storage_ipc_procarray_seams as procarray_seams;
use backend_storage_ipc_standby_seams as standby_seams;
use backend_storage_smgr_seams as smgr_seams;
use backend_utils_activity_xact_seams as pgstat_xact_seams;

/// Everything `xact_redo` reads from the `XLogReaderState` it gets in C
/// (XLogRecGetInfo / Xid / Origin, Read/EndRecPtr, XLogRecGetData).
#[derive(Clone, Copy, Debug)]
pub struct XactRedoInfo<'a> {
    /// `XLogRecGetInfo(record)` (the full xl_info byte).
    pub info: u8,
    /// `XLogRecGetXid(record)`
    pub xid: TransactionId,
    /// `XLogRecGetOrigin(record)`
    pub origin_id: RepOriginId,
    /// `record->ReadRecPtr`
    pub read_rec_ptr: XLogRecPtr,
    /// `record->EndRecPtr`
    pub end_rec_ptr: XLogRecPtr,
    /// `XLogRecGetData(record)` (the record body, sans the WAL header).
    pub data: &'a [u8],
}

fn truncated() -> PgError {
    PgError::error("truncated transaction WAL record")
}

// Bounds-checked native-endian cursor: a malformed on-disk record surfaces a
// recoverable error, and embedded element counts are validated against the
// bytes that remain before any collection grows.
struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn u32(&mut self) -> PgResult<u32> {
        let end = self.pos + 4;
        let bytes = self.data.get(self.pos..end).ok_or_else(truncated)?;
        self.pos = end;
        Ok(u32::from_ne_bytes(bytes.try_into().unwrap()))
    }

    fn i32(&mut self) -> PgResult<i32> {
        self.u32().map(|v| v as i32)
    }

    fn i64(&mut self) -> PgResult<i64> {
        let end = self.pos + 8;
        let bytes = self.data.get(self.pos..end).ok_or_else(truncated)?;
        self.pos = end;
        Ok(i64::from_ne_bytes(bytes.try_into().unwrap()))
    }

    fn u64(&mut self) -> PgResult<u64> {
        self.i64().map(|v| v as u64)
    }

    fn take(&mut self, n: usize) -> PgResult<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or_else(truncated)?;
        let s = self.data.get(self.pos..end).ok_or_else(truncated)?;
        self.pos = end;
        Ok(s)
    }

    /// Take a NUL-terminated C string (the twophase_gid).
    fn cstr(&mut self) -> PgResult<Vec<u8>> {
        let start = self.pos;
        while self.data.get(self.pos).copied().ok_or_else(truncated)? != 0 {
            self.pos += 1;
        }
        let mut s = Vec::new();
        s.try_reserve(self.pos - start)
            .map_err(|_| PgError::error("out of memory parsing transaction WAL record"))?;
        s.extend_from_slice(&self.data[start..self.pos]);
        self.pos += 1; // consume the NUL
        Ok(s)
    }

    /// Read a count, validate it against remaining bytes (each element at
    /// least `min_elem_bytes`), and fallibly reserve.
    fn read_count<T>(&mut self, min_elem_bytes: usize, into: &mut Vec<T>) -> PgResult<i32> {
        let n = self.i32()?;
        if n < 0 {
            return Err(PgError::error(
                "negative element count in transaction WAL record",
            ));
        }
        let count = n as usize;
        if min_elem_bytes != 0 && count.saturating_mul(min_elem_bytes) > self.remaining() {
            return Err(truncated());
        }
        into.try_reserve(count)
            .map_err(|_| PgError::error("out of memory parsing transaction WAL record"))?;
        Ok(n)
    }
}

fn parse_rel(c: &mut Cursor<'_>) -> PgResult<RelFileLocator> {
    let spc = c.u32()?;
    let db = c.u32()?;
    let rel = c.u32()?;
    Ok(RelFileLocator {
        spcOid: spc,
        dbOid: db,
        relNumber: rel,
    })
}

/// Decode an `xl_xact_stats_item` (16 bytes); the 64-bit objid reassembles
/// from its two halves as in `xact_desc_stats` (xactdesc.c).
fn parse_stat(c: &mut Cursor<'_>) -> PgResult<XlXactStatsItem> {
    let kind = c.i32()?;
    let dboid = c.u32()?;
    let objid_lo = c.u32()?;
    let objid_hi = c.u32()?;
    Ok(XlXactStatsItem {
        kind,
        dboid,
        objid: ((objid_hi as u64) << 32) | (objid_lo as u64),
    })
}

/// `ParseCommitRecord` (xactdesc.c) — decode a commit record body. `info` is
/// the full xl_info byte (`XLOG_XACT_HAS_INFO` controls the xinfo field).
pub fn parse_commit_record(info: u8, data: &[u8]) -> PgResult<ParsedCommit> {
    let mut c = Cursor::new(data);
    let mut parsed = ParsedCommit {
        xact_time: c.i64()?,
        ..Default::default()
    };

    let xinfo = if (info & XLOG_XACT_HAS_INFO) != 0 {
        c.u32()?
    } else {
        0
    };
    parsed.xinfo = xinfo;

    if (xinfo & XACT_XINFO_HAS_DBINFO) != 0 {
        parsed.db_id = c.u32()?;
        parsed.ts_id = c.u32()?;
    }

    if (xinfo & XACT_XINFO_HAS_SUBXACTS) != 0 {
        let n = c.read_count(4, &mut parsed.subxacts)?;
        for _ in 0..n {
            parsed.subxacts.push(c.u32()?);
        }
    }

    if (xinfo & XACT_XINFO_HAS_RELFILELOCATORS) != 0 {
        let n = c.read_count(12, &mut parsed.xlocators)?;
        for _ in 0..n {
            parsed.xlocators.push(parse_rel(&mut c)?);
        }
    }

    if (xinfo & XACT_XINFO_HAS_DROPPED_STATS) != 0 {
        let n = c.read_count(16, &mut parsed.stats)?;
        for _ in 0..n {
            parsed.stats.push(parse_stat(&mut c)?);
        }
    }

    if (xinfo & XACT_XINFO_HAS_INVALS) != 0 {
        let n = c.read_count(SHARED_INVALIDATION_MESSAGE_SIZE, &mut parsed.msgs)?;
        for _ in 0..n {
            let bytes: [u8; SHARED_INVALIDATION_MESSAGE_SIZE] =
                c.take(SHARED_INVALIDATION_MESSAGE_SIZE)?.try_into().unwrap();
            let msg = SharedInvalidationMessage::from_wire_bytes(bytes).ok_or_else(|| {
                PgError::error("invalid shared-invalidation message in transaction WAL record")
            })?;
            parsed.msgs.push(msg);
        }
    }

    if (xinfo & XACT_XINFO_HAS_TWOPHASE) != 0 {
        parsed.twophase_xid = c.u32()?;
        if (xinfo & XACT_XINFO_HAS_GID) != 0 {
            parsed.twophase_gid = c.cstr()?;
        }
    }

    if (xinfo & XACT_XINFO_HAS_ORIGIN) != 0 {
        parsed.origin_lsn = c.u64()?;
        parsed.origin_timestamp = c.i64()?;
    }

    Ok(parsed)
}

/// `ParseAbortRecord` (xactdesc.c) — decode an abort record body.
pub fn parse_abort_record(info: u8, data: &[u8]) -> PgResult<ParsedAbort> {
    let mut c = Cursor::new(data);
    let mut parsed = ParsedAbort {
        xact_time: c.i64()?,
        ..Default::default()
    };

    let xinfo = if (info & XLOG_XACT_HAS_INFO) != 0 {
        c.u32()?
    } else {
        0
    };
    parsed.xinfo = xinfo;

    if (xinfo & XACT_XINFO_HAS_DBINFO) != 0 {
        parsed.db_id = c.u32()?;
        parsed.ts_id = c.u32()?;
    }

    if (xinfo & XACT_XINFO_HAS_SUBXACTS) != 0 {
        let n = c.read_count(4, &mut parsed.subxacts)?;
        for _ in 0..n {
            parsed.subxacts.push(c.u32()?);
        }
    }

    if (xinfo & XACT_XINFO_HAS_RELFILELOCATORS) != 0 {
        let n = c.read_count(12, &mut parsed.xlocators)?;
        for _ in 0..n {
            parsed.xlocators.push(parse_rel(&mut c)?);
        }
    }

    if (xinfo & XACT_XINFO_HAS_DROPPED_STATS) != 0 {
        let n = c.read_count(16, &mut parsed.stats)?;
        for _ in 0..n {
            parsed.stats.push(parse_stat(&mut c)?);
        }
    }

    if (xinfo & XACT_XINFO_HAS_TWOPHASE) != 0 {
        parsed.twophase_xid = c.u32()?;
        if (xinfo & XACT_XINFO_HAS_GID) != 0 {
            parsed.twophase_gid = c.cstr()?;
        }
    }

    if (xinfo & XACT_XINFO_HAS_ORIGIN) != 0 {
        parsed.origin_lsn = c.u64()?;
        parsed.origin_timestamp = c.i64()?;
    }

    Ok(parsed)
}

/// `xact_redo_commit` (xact.c:6130) — order of execution is critical.
fn xact_redo_commit(
    parsed: &ParsedCommit,
    xid: TransactionId,
    lsn: XLogRecPtr,
    origin_id: RepOriginId,
) -> PgResult<()> {
    debug_assert!(xid != InvalidTransactionId);

    let max_xid = transam_seams::transaction_id_latest::call(xid, &parsed.subxacts);

    // Make sure nextXid is beyond any XID mentioned in the record.
    varsup_seams::advance_next_full_transaction_id_past_xid::call(max_xid);

    debug_assert_eq!(
        (parsed.xinfo & XACT_XINFO_HAS_ORIGIN) == 0,
        origin_id == types_core::InvalidRepOriginId
    );

    let commit_time = if (parsed.xinfo & XACT_XINFO_HAS_ORIGIN) != 0 {
        parsed.origin_timestamp
    } else {
        parsed.xact_time
    };

    // Set the transaction commit timestamp and metadata.
    commit_ts_seams::transaction_tree_set_commit_ts_data::call(
        xid,
        &parsed.subxacts,
        commit_time,
        origin_id,
    )?;

    if xlogutils_seams::standby_state::call() == STANDBY_DISABLED {
        // Mark the transaction committed in pg_xact.
        transam_seams::transaction_id_commit_tree::call(xid, &parsed.subxacts)?;
    } else {
        // As-yet-unobserved subtransactions need bookkeeping again here
        // (RecordKnownAssignedTransactionIds in the main loop doesn't cover
        // this case — easy to think this call is irrelevant; it isn't).
        procarray_seams::record_known_assigned_transaction_ids::call(max_xid)?;

        // Mark committed using the async protocol during recovery: hint bits
        // must not be set until minRecoveryPoint passes this commit record.
        transam_seams::transaction_id_async_commit_tree::call(xid, &parsed.subxacts, lsn)?;

        // We must mark clog before we update the ProcArray.
        procarray_seams::expire_tree_known_assigned_transaction_ids::call(
            xid,
            &parsed.subxacts,
            max_xid,
        )?;

        // Send cache invalidations attached to the commit (same inval-then-
        // release-locks order as CommitTransaction).
        inval_seams::process_committed_invalidation_messages::call(
            &parsed.msgs,
            xact_completion_relcache_init_file_inval(parsed.xinfo),
            parsed.db_id,
            parsed.ts_id,
        )?;

        // Release locks, if any (both 2PC and normal transactions: in effect
        // we skip the prepare phase and go straight to lock release).
        if (parsed.xinfo & XACT_XINFO_HAS_AE_LOCKS) != 0 {
            standby_seams::standby_release_lock_tree::call(xid, &parsed.subxacts);
        }
    }

    if (parsed.xinfo & XACT_XINFO_HAS_ORIGIN) != 0 {
        // recover apply progress
        origin_seams::replorigin_advance::call(
            origin_id,
            parsed.origin_lsn,
            lsn,
            false, /* backward */
            false, /* WAL */
        )?;
    }

    // Make sure files supposed to be dropped are dropped: first update the
    // minimum recovery point past this record (we bypass the buffer manager,
    // so we enforce the WAL-first rule ourselves), then drop.
    if !parsed.xlocators.is_empty() {
        xlog_seams::xlog_flush::call(lsn)?;
        smgr_seams::drop_relation_files::call(&parsed.xlocators, true)?;
    }

    if !parsed.stats.is_empty() {
        // see equivalent call for relations above
        xlog_seams::xlog_flush::call(lsn)?;
        pgstat_xact_seams::pgstat_execute_transactional_drops::call(&parsed.stats, true)?;
    }

    // XLogFlush for the same reason ForceSyncCommit exists in normal
    // operation (e.g. CREATE DATABASE's window between file copy and commit).
    if xact_completion_force_sync_commit(parsed.xinfo) {
        xlog_seams::xlog_flush::call(lsn)?;
    }

    // If asked by the primary (synchronous_commit = remote_apply), ask
    // walreceiver to send a reply immediately.
    if xact_completion_apply_feedback(parsed.xinfo) {
        xlogrecovery_seams::xlog_request_wal_receiver_reply::call();
    }

    Ok(())
}

/// `xact_redo_abort` (xact.c:6284). An abort may be for a subtransaction and
/// its children (topxid != xid), unlike commit.
fn xact_redo_abort(
    parsed: &ParsedAbort,
    xid: TransactionId,
    lsn: XLogRecPtr,
    origin_id: RepOriginId,
) -> PgResult<()> {
    debug_assert!(xid != InvalidTransactionId);

    // Make sure nextXid is beyond any XID mentioned in the record.
    let max_xid = transam_seams::transaction_id_latest::call(xid, &parsed.subxacts);
    varsup_seams::advance_next_full_transaction_id_past_xid::call(max_xid);

    if xlogutils_seams::standby_state::call() == STANDBY_DISABLED {
        // Mark the transaction aborted in pg_xact, no need for async stuff.
        transam_seams::transaction_id_abort_tree::call(xid, &parsed.subxacts)?;
    } else {
        // See xact_redo_commit about this call.
        procarray_seams::record_known_assigned_transaction_ids::call(max_xid)?;

        // Mark the transaction aborted in pg_xact, no need for async stuff.
        transam_seams::transaction_id_abort_tree::call(xid, &parsed.subxacts)?;

        // We must update the ProcArray after we have marked clog.
        procarray_seams::expire_tree_known_assigned_transaction_ids::call(
            xid,
            &parsed.subxacts,
            max_xid,
        )?;

        // There are no invalidation messages to send or undo.

        // Release locks, if any.
        if (parsed.xinfo & XACT_XINFO_HAS_AE_LOCKS) != 0 {
            standby_seams::standby_release_lock_tree::call(xid, &parsed.subxacts);
        }
    }

    if (parsed.xinfo & XACT_XINFO_HAS_ORIGIN) != 0 {
        // recover apply progress
        origin_seams::replorigin_advance::call(
            origin_id,
            parsed.origin_lsn,
            lsn,
            false, /* backward */
            false, /* WAL */
        )?;
    }

    // Make sure files supposed to be dropped are dropped.
    if !parsed.xlocators.is_empty() {
        // See comments about the minimum recovery point in xact_redo_commit.
        xlog_seams::xlog_flush::call(lsn)?;
        smgr_seams::drop_relation_files::call(&parsed.xlocators, true)?;
    }

    if !parsed.stats.is_empty() {
        // see equivalent call for relations above
        xlog_seams::xlog_flush::call(lsn)?;
        pgstat_xact_seams::pgstat_execute_transactional_drops::call(&parsed.stats, true)?;
    }

    Ok(())
}

/// `xact_redo` (xact.c:6363) — the redo dispatcher. PANIC on an unknown op
/// code. (Backup blocks are not used in xact records.)
pub fn xact_redo(record: XactRedoInfo<'_>) -> PgResult<()> {
    let info = record.info & XLOG_XACT_OPMASK;

    match info {
        XLOG_XACT_COMMIT => {
            let parsed = parse_commit_record(record.info, record.data)?;
            xact_redo_commit(&parsed, record.xid, record.end_rec_ptr, record.origin_id)
        }
        XLOG_XACT_COMMIT_PREPARED => {
            let parsed = parse_commit_record(record.info, record.data)?;
            xact_redo_commit(
                &parsed,
                parsed.twophase_xid,
                record.end_rec_ptr,
                record.origin_id,
            )?;
            // Delete TwoPhaseState gxact entry and/or 2PC file (the C caller
            // holds TwoPhaseStateLock around this; the installed impl carries
            // the lock until the lwlock surface lands).
            twophase_seams::prepare_redo_remove::call(parsed.twophase_xid, false)
        }
        XLOG_XACT_ABORT => {
            let parsed = parse_abort_record(record.info, record.data)?;
            xact_redo_abort(&parsed, record.xid, record.end_rec_ptr, record.origin_id)
        }
        XLOG_XACT_ABORT_PREPARED => {
            let parsed = parse_abort_record(record.info, record.data)?;
            xact_redo_abort(
                &parsed,
                parsed.twophase_xid,
                record.end_rec_ptr,
                record.origin_id,
            )?;
            // Delete TwoPhaseState gxact entry and/or 2PC file.
            twophase_seams::prepare_redo_remove::call(parsed.twophase_xid, false)
        }
        XLOG_XACT_PREPARE => {
            // Store xid and start/end pointers of the WAL record in the
            // TwoPhaseState gxact entry.
            twophase_seams::prepare_redo_add::call(
                record.data,
                record.read_rec_ptr,
                record.end_rec_ptr,
                record.origin_id,
            )
        }
        XLOG_XACT_ASSIGNMENT => {
            // xl_xact_assignment { TransactionId xtop; int nsubxacts;
            //                      TransactionId xsub[]; }
            if xlogutils_seams::standby_state::call() >= STANDBY_INITIALIZED {
                let mut c = Cursor::new(record.data);
                let xtop = c.u32()?;
                let mut subxids: Vec<TransactionId> = Vec::new();
                let nsub = c.read_count(4, &mut subxids)?;
                for _ in 0..nsub {
                    subxids.push(c.u32()?);
                }
                procarray_seams::proc_array_apply_xid_assignment::call(xtop, &subxids)?;
            }
            Ok(())
        }
        XLOG_XACT_INVALIDATIONS => {
            // We ignore this for now: what matters are the invalidations
            // written into the commit record.
            Ok(())
        }
        other => Err(PgError::new(
            PANIC,
            format!("xact_redo: unknown op code {other}"),
        )),
    }
}
