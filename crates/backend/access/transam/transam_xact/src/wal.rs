//! WAL-record builders for transaction commit/abort: `XactLogCommitRecord`
//! (xact.c:5814) and `XactLogAbortRecord` (xact.c:5986).
//!
//! The xinfo derivation, opcode selection, and record-body assembly all live
//! here; each `XLogRegisterData` call in C becomes a `xlog_register_data`
//! seam call with the same bytes in the same order, followed by
//! `xlog_set_record_flags(XLOG_INCLUDE_ORIGIN)` + `xlog_insert`.

use crate::*;
use types_core::{Oid, TransactionId};
use types_storage::{RelFileLocator, SharedInvalidationMessage, SHARED_INVALIDATION_MESSAGE_SIZE};
use ::wal::{
    XlXactStatsItem, RM_XACT_ID, XACT_COMPLETION_APPLY_FEEDBACK,
    XACT_COMPLETION_FORCE_SYNC_COMMIT, XACT_COMPLETION_UPDATE_RELCACHE_FILE,
    XACT_XINFO_HAS_AE_LOCKS, XACT_XINFO_HAS_DBINFO, XACT_XINFO_HAS_DROPPED_STATS,
    XACT_XINFO_HAS_GID, XACT_XINFO_HAS_INVALS, XACT_XINFO_HAS_ORIGIN,
    XACT_XINFO_HAS_RELFILELOCATORS, XACT_XINFO_HAS_SUBXACTS, XACT_XINFO_HAS_TWOPHASE,
    XLOG_INCLUDE_ORIGIN, XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED, XLOG_XACT_COMMIT,
    XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_HAS_INFO, XLR_SPECIAL_REL_UPDATE,
};

use origin_seams as origin_seams;
use init_small_seams as globals_seams;

fn oom() -> PgError {
    PgError::error("out of memory building transaction WAL record")
}

/// Serialize a `RelFileLocator` array (`{ Oid spcOid; Oid dbOid;
/// RelFileNumber relNumber; }` each).
pub(crate) fn rels_bytes(rels: &[RelFileLocator]) -> PgResult<Vec<u8>> {
    let mut buf = Vec::new();
    buf.try_reserve(rels.len() * 12).map_err(|_| oom())?;
    for rel in rels {
        buf.extend_from_slice(&rel.spcOid.to_ne_bytes());
        buf.extend_from_slice(&rel.dbOid.to_ne_bytes());
        buf.extend_from_slice(&rel.relNumber.to_ne_bytes());
    }
    Ok(buf)
}

/// Serialize an `xl_xact_stats_item` array. The struct is exactly 16 bytes:
/// `{ int kind; Oid dboid; uint32 objid_lo; uint32 objid_hi; }`.
pub(crate) fn stats_bytes(items: &[XlXactStatsItem]) -> PgResult<Vec<u8>> {
    let mut buf = Vec::new();
    buf.try_reserve(items.len() * 16).map_err(|_| oom())?;
    for item in items {
        buf.extend_from_slice(&item.kind.to_ne_bytes());
        buf.extend_from_slice(&item.dboid.to_ne_bytes());
        buf.extend_from_slice(&((item.objid & 0xFFFF_FFFF) as u32).to_ne_bytes());
        buf.extend_from_slice(&((item.objid >> 32) as u32).to_ne_bytes());
    }
    Ok(buf)
}

/// Serialize the `SharedInvalidationMessage` array to its raw C-union form
/// (the byte layout the redo side and xactdesc expect).
pub(crate) fn inval_msgs_bytes(msgs: &[SharedInvalidationMessage]) -> PgResult<Vec<u8>> {
    let mut buf = Vec::new();
    buf.try_reserve(msgs.len() * SHARED_INVALIDATION_MESSAGE_SIZE)
        .map_err(|_| oom())?;
    for msg in msgs {
        buf.extend_from_slice(&msg.to_wire_bytes());
    }
    Ok(buf)
}

fn xids_bytes(xids: &[TransactionId]) -> PgResult<Vec<u8>> {
    let mut buf = Vec::new();
    buf.try_reserve(xids.len() * 4).map_err(|_| oom())?;
    for x in xids {
        buf.extend_from_slice(&x.to_ne_bytes());
    }
    Ok(buf)
}

/// `XactLogCommitRecord` (xact.c:5814) — log the commit record for a plain or
/// twophase commit (2PC when `twophase_xid` is valid).
pub fn XactLogCommitRecord(
    commit_time: TimestampTz,
    subxacts: &[TransactionId],
    rels: &[RelFileLocator],
    dropped_stats: &[XlXactStatsItem],
    msgs: &[SharedInvalidationMessage],
    relcache_inval: bool,
    xactflags: i32,
    twophase_xid: TransactionId,
    twophase_gid: Option<&str>,
) -> PgResult<XLogRecPtr> {
    let mut xinfo: u32 = 0;

    // decide between a plain and 2pc commit
    let mut info: u8 = if twophase_xid == InvalidTransactionId {
        XLOG_XACT_COMMIT
    } else {
        XLOG_XACT_COMMIT_PREPARED
    };

    // First figure out and collect all the information needed.

    if relcache_inval {
        xinfo |= XACT_COMPLETION_UPDATE_RELCACHE_FILE;
    }
    if xs(|s| s.force_sync_commit) {
        xinfo |= XACT_COMPLETION_FORCE_SYNC_COMMIT;
    }
    if (xactflags & XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK) != 0 {
        xinfo |= XACT_XINFO_HAS_AE_LOCKS;
    }

    // Ask standbys for immediate feedback once this commit is applied?
    if xs(|s| s.synchronous_commit) >= SYNCHRONOUS_COMMIT_REMOTE_APPLY {
        xinfo |= XACT_COMPLETION_APPLY_FEEDBACK;
    }

    // Relcache invalidations require information about the current database,
    // and so does logical decoding.
    let logical_info = xlog_seams::xlog_logical_info_active::call();
    let mut db_id: Oid = 0;
    let mut ts_id: Oid = 0;
    if !msgs.is_empty() || logical_info {
        xinfo |= XACT_XINFO_HAS_DBINFO;
        db_id = globals_seams::my_database_id::call();
        ts_id = globals_seams::my_database_table_space::call();
    }

    if !subxacts.is_empty() {
        xinfo |= XACT_XINFO_HAS_SUBXACTS;
    }
    if !rels.is_empty() {
        xinfo |= XACT_XINFO_HAS_RELFILELOCATORS;
        info |= XLR_SPECIAL_REL_UPDATE;
    }
    if !dropped_stats.is_empty() {
        xinfo |= XACT_XINFO_HAS_DROPPED_STATS;
    }
    if !msgs.is_empty() {
        xinfo |= XACT_XINFO_HAS_INVALS;
    }

    if twophase_xid != InvalidTransactionId {
        xinfo |= XACT_XINFO_HAS_TWOPHASE;
        debug_assert!(twophase_gid.is_some());
        if logical_info {
            xinfo |= XACT_XINFO_HAS_GID;
        }
    }

    // dump transaction origin information
    let session_origin = origin_seams::replorigin_session_origin::call();
    if session_origin != ::types_core::InvalidRepOriginId {
        xinfo |= XACT_XINFO_HAS_ORIGIN;
    }

    if xinfo != 0 {
        info |= XLOG_XACT_HAS_INFO;
    }

    // Then include all the collected data into the commit record. Each C
    // `XLogRegisterData(data, len)` becomes one fragment, registered in order
    // by the `xlog_insert` seam (which also does the begin/set-flags/insert).
    let mut frags: Vec<Vec<u8>> = Vec::new();

    // xl_xact_commit { TimestampTz xact_time; }
    frags.push(commit_time.to_ne_bytes().to_vec());

    if xinfo != 0 {
        frags.push(xinfo.to_ne_bytes().to_vec());
    }

    if (xinfo & XACT_XINFO_HAS_DBINFO) != 0 {
        // xl_xact_dbinfo { Oid dbId; Oid tsId; }
        let mut dbinfo = [0u8; 8];
        dbinfo[0..4].copy_from_slice(&db_id.to_ne_bytes());
        dbinfo[4..8].copy_from_slice(&ts_id.to_ne_bytes());
        frags.push(dbinfo.to_vec());
    }

    if (xinfo & XACT_XINFO_HAS_SUBXACTS) != 0 {
        // xl_xact_subxacts { int nsubxacts; TransactionId subxacts[]; }
        frags.push((subxacts.len() as i32).to_ne_bytes().to_vec());
        frags.push(xids_bytes(subxacts)?);
    }

    if (xinfo & XACT_XINFO_HAS_RELFILELOCATORS) != 0 {
        // xl_xact_relfilelocators { int nrels; RelFileLocator xlocators[]; }
        frags.push((rels.len() as i32).to_ne_bytes().to_vec());
        frags.push(rels_bytes(rels)?);
    }

    if (xinfo & XACT_XINFO_HAS_DROPPED_STATS) != 0 {
        // xl_xact_stats_items { int nitems; xl_xact_stats_item items[]; }
        frags.push((dropped_stats.len() as i32).to_ne_bytes().to_vec());
        frags.push(stats_bytes(dropped_stats)?);
    }

    if (xinfo & XACT_XINFO_HAS_INVALS) != 0 {
        // xl_xact_invals { int nmsgs; SharedInvalidationMessage msgs[]; }
        frags.push((msgs.len() as i32).to_ne_bytes().to_vec());
        frags.push(inval_msgs_bytes(msgs)?);
    }

    if (xinfo & XACT_XINFO_HAS_TWOPHASE) != 0 {
        // xl_xact_twophase { TransactionId xid; }
        frags.push(twophase_xid.to_ne_bytes().to_vec());
        if (xinfo & XACT_XINFO_HAS_GID) != 0 {
            let gid = twophase_gid.expect("HAS_GID implies a gid");
            let mut gid_bytes = Vec::new();
            gid_bytes.try_reserve(gid.len() + 1).map_err(|_| oom())?;
            gid_bytes.extend_from_slice(gid.as_bytes());
            gid_bytes.push(0); // the trailing NUL of the C string
            frags.push(gid_bytes);
        }
    }

    if (xinfo & XACT_XINFO_HAS_ORIGIN) != 0 {
        // xl_xact_origin { XLogRecPtr origin_lsn; TimestampTz origin_timestamp; }
        let mut origin = [0u8; 16];
        origin[0..8]
            .copy_from_slice(&origin_seams::replorigin_session_origin_lsn::call().to_ne_bytes());
        origin[8..16].copy_from_slice(
            &origin_seams::replorigin_session_origin_timestamp::call().to_ne_bytes(),
        );
        frags.push(origin.to_vec());
    }

    // we allow filtering by xacts: XLOG_INCLUDE_ORIGIN flags the record.
    let fragments: Vec<&[u8]> = frags.iter().map(|f| f.as_slice()).collect();
    xloginsert_seams::xlog_insert::call(RM_XACT_ID, info, XLOG_INCLUDE_ORIGIN, &fragments)
}

/// `XactLogAbortRecord` (xact.c:5986) — log the abort record for a plain or
/// twophase abort (2PC when `twophase_xid` is valid).
pub fn XactLogAbortRecord(
    abort_time: TimestampTz,
    subxacts: &[TransactionId],
    rels: &[RelFileLocator],
    dropped_stats: &[XlXactStatsItem],
    xactflags: i32,
    twophase_xid: TransactionId,
    twophase_gid: Option<&str>,
) -> PgResult<XLogRecPtr> {
    let mut xinfo: u32 = 0;

    // decide between a plain and 2pc abort
    let mut info: u8 = if twophase_xid == InvalidTransactionId {
        XLOG_XACT_ABORT
    } else {
        XLOG_XACT_ABORT_PREPARED
    };

    // First figure out and collect all the information needed.

    if (xactflags & XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK) != 0 {
        xinfo |= XACT_XINFO_HAS_AE_LOCKS;
    }
    if !subxacts.is_empty() {
        xinfo |= XACT_XINFO_HAS_SUBXACTS;
    }
    if !rels.is_empty() {
        xinfo |= XACT_XINFO_HAS_RELFILELOCATORS;
        info |= XLR_SPECIAL_REL_UPDATE;
    }
    if !dropped_stats.is_empty() {
        xinfo |= XACT_XINFO_HAS_DROPPED_STATS;
    }

    let logical_info = xlog_seams::xlog_logical_info_active::call();
    if twophase_xid != InvalidTransactionId {
        xinfo |= XACT_XINFO_HAS_TWOPHASE;
        debug_assert!(twophase_gid.is_some());
        if logical_info {
            xinfo |= XACT_XINFO_HAS_GID;
        }
    }

    let mut db_id: Oid = 0;
    let mut ts_id: Oid = 0;
    if twophase_xid != InvalidTransactionId && logical_info {
        xinfo |= XACT_XINFO_HAS_DBINFO;
        db_id = globals_seams::my_database_id::call();
        ts_id = globals_seams::my_database_table_space::call();
    }

    // Dump transaction origin information (needed during recovery to update
    // the replication origin progress).
    let session_origin = origin_seams::replorigin_session_origin::call();
    if session_origin != ::types_core::InvalidRepOriginId {
        xinfo |= XACT_XINFO_HAS_ORIGIN;
    }

    if xinfo != 0 {
        info |= XLOG_XACT_HAS_INFO;
    }

    // Then include all the collected data into the abort record. Each C
    // `XLogRegisterData(data, len)` becomes one fragment, registered in order
    // by the `xlog_insert` seam (which also does the begin/set-flags/insert).
    let mut frags: Vec<Vec<u8>> = Vec::new();

    // xl_xact_abort { TimestampTz xact_time; ... } (MinSizeOfXactAbort)
    frags.push(abort_time.to_ne_bytes().to_vec());

    if xinfo != 0 {
        frags.push(xinfo.to_ne_bytes().to_vec());
    }

    if (xinfo & XACT_XINFO_HAS_DBINFO) != 0 {
        let mut dbinfo = [0u8; 8];
        dbinfo[0..4].copy_from_slice(&db_id.to_ne_bytes());
        dbinfo[4..8].copy_from_slice(&ts_id.to_ne_bytes());
        frags.push(dbinfo.to_vec());
    }

    if (xinfo & XACT_XINFO_HAS_SUBXACTS) != 0 {
        frags.push((subxacts.len() as i32).to_ne_bytes().to_vec());
        frags.push(xids_bytes(subxacts)?);
    }

    if (xinfo & XACT_XINFO_HAS_RELFILELOCATORS) != 0 {
        frags.push((rels.len() as i32).to_ne_bytes().to_vec());
        frags.push(rels_bytes(rels)?);
    }

    if (xinfo & XACT_XINFO_HAS_DROPPED_STATS) != 0 {
        frags.push((dropped_stats.len() as i32).to_ne_bytes().to_vec());
        frags.push(stats_bytes(dropped_stats)?);
    }

    if (xinfo & XACT_XINFO_HAS_TWOPHASE) != 0 {
        frags.push(twophase_xid.to_ne_bytes().to_vec());
        if (xinfo & XACT_XINFO_HAS_GID) != 0 {
            let gid = twophase_gid.expect("HAS_GID implies a gid");
            let mut gid_bytes = Vec::new();
            gid_bytes.try_reserve(gid.len() + 1).map_err(|_| oom())?;
            gid_bytes.extend_from_slice(gid.as_bytes());
            gid_bytes.push(0);
            frags.push(gid_bytes);
        }
    }

    if (xinfo & XACT_XINFO_HAS_ORIGIN) != 0 {
        let mut origin = [0u8; 16];
        origin[0..8]
            .copy_from_slice(&origin_seams::replorigin_session_origin_lsn::call().to_ne_bytes());
        origin[8..16].copy_from_slice(
            &origin_seams::replorigin_session_origin_timestamp::call().to_ne_bytes(),
        );
        frags.push(origin.to_vec());
    }

    // Include the replication origin: XLOG_INCLUDE_ORIGIN flags the record.
    let fragments: Vec<&[u8]> = frags.iter().map(|f| f.as_slice()).collect();
    xloginsert_seams::xlog_insert::call(RM_XACT_ID, info, XLOG_INCLUDE_ORIGIN, &fragments)
}
