// XactLogCommitRecord / XactLogAbortRecord: xinfo derivation, opcode
// selection, and body assembly; each C XLogRegisterData is one fragment, in
// C's order, handed to the xlog_insert_with_flags seam (XLOG_INCLUDE_ORIGIN).
// Fabled #396 endpoint: fixed fragment array + stack locals for the
// fixed-size pieces; owned buffers only for the variable-length arrays (all
// empty on a plain commit), so the hot plain-commit path allocates nothing.

use crate::*;
use types_core::{Oid, TransactionId};
use types_core::xact::XlXactStatsItem;
use types_storage::{RelFileLocator, SharedInvalidationMessage, SHARED_INVALIDATION_MESSAGE_SIZE};

// C default: replorigin_session_origin = InvalidRepOriginId (origin.c);
// seam uninstalled until replication origins land.
pub(crate) fn session_origin_or_default() -> types_core::RepOriginId {
    if origin_seams::replorigin_session_origin::is_installed() {
        origin_seams::replorigin_session_origin::call()
    } else {
        types_core::InvalidRepOriginId
    }
}

fn oom() -> Box<PgError> {
    Box::new(PgError::error("out of memory building transaction WAL record"))
}

/// `RelFileLocator` array as `{ Oid spcOid; Oid dbOid; RelFileNumber relNumber; }` each.
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

/// `xl_xact_stats_item` array; 16 bytes each, objid split into lo/hi words.
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

/// `XactLogCommitRecord` (xact.c) — plain or twophase commit (2PC when
/// `twophase_xid` is valid).
#[allow(clippy::too_many_arguments)]
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

    let mut info: u8 = if twophase_xid == InvalidTransactionId {
        XLOG_XACT_COMMIT
    } else {
        XLOG_XACT_COMMIT_PREPARED
    };

    if relcache_inval {
        xinfo |= XACT_COMPLETION_UPDATE_RELCACHE_FILE;
    }
    if xs(|s| s.force_sync_commit) {
        xinfo |= XACT_COMPLETION_FORCE_SYNC_COMMIT;
    }
    if (xactflags & XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK) != 0 {
        xinfo |= XACT_XINFO_HAS_AE_LOCKS;
    }
    if xs(|s| s.synchronous_commit) >= SYNCHRONOUS_COMMIT_REMOTE_APPLY {
        xinfo |= XACT_COMPLETION_APPLY_FEEDBACK;
    }

    // Relcache invalidations and logical decoding both need dbinfo.
    let logical_info = xlog_seams::xlog_logical_info_active::call();
    let mut db_id: Oid = 0;
    let mut ts_id: Oid = 0;
    if !msgs.is_empty() || logical_info {
        xinfo |= XACT_XINFO_HAS_DBINFO;
        db_id = init_small::globals::MyDatabaseId();
        ts_id = init_small::globals::MyDatabaseTableSpace();
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

    let session_origin = session_origin_or_default();
    if session_origin != types_core::InvalidRepOriginId {
        xinfo |= XACT_XINFO_HAS_ORIGIN;
    }

    if xinfo != 0 {
        info |= XLOG_XACT_HAS_INFO;
    }

    let mut fragments: [&[u8]; 13] = [&[]; 13];
    let mut nfrags: usize = 0;

    // xl_xact_commit { TimestampTz xact_time; }
    let commit_time_b = commit_time.to_ne_bytes();
    fragments[nfrags] = &commit_time_b;
    nfrags += 1;

    let xinfo_b = xinfo.to_ne_bytes();
    if xinfo != 0 {
        fragments[nfrags] = &xinfo_b;
        nfrags += 1;
    }

    // xl_xact_dbinfo { Oid dbId; Oid tsId; }
    let mut dbinfo = [0u8; 8];
    if (xinfo & XACT_XINFO_HAS_DBINFO) != 0 {
        dbinfo[0..4].copy_from_slice(&db_id.to_ne_bytes());
        dbinfo[4..8].copy_from_slice(&ts_id.to_ne_bytes());
        fragments[nfrags] = &dbinfo;
        nfrags += 1;
    }

    // xl_xact_subxacts { int nsubxacts; TransactionId subxacts[]; }
    let nsubxacts_b = (subxacts.len() as i32).to_ne_bytes();
    let subxacts_bytes: Vec<u8>;
    if (xinfo & XACT_XINFO_HAS_SUBXACTS) != 0 {
        subxacts_bytes = xids_bytes(subxacts)?;
        fragments[nfrags] = &nsubxacts_b;
        fragments[nfrags + 1] = &subxacts_bytes;
        nfrags += 2;
    }

    // xl_xact_relfilelocators { int nrels; RelFileLocator xlocators[]; }
    let nrels_b = (rels.len() as i32).to_ne_bytes();
    let rels_b: Vec<u8>;
    if (xinfo & XACT_XINFO_HAS_RELFILELOCATORS) != 0 {
        rels_b = rels_bytes(rels)?;
        fragments[nfrags] = &nrels_b;
        fragments[nfrags + 1] = &rels_b;
        nfrags += 2;
    }

    // xl_xact_stats_items { int nitems; xl_xact_stats_item items[]; }
    let nstats_b = (dropped_stats.len() as i32).to_ne_bytes();
    let stats_b: Vec<u8>;
    if (xinfo & XACT_XINFO_HAS_DROPPED_STATS) != 0 {
        stats_b = stats_bytes(dropped_stats)?;
        fragments[nfrags] = &nstats_b;
        fragments[nfrags + 1] = &stats_b;
        nfrags += 2;
    }

    // xl_xact_invals { int nmsgs; SharedInvalidationMessage msgs[]; }
    let nmsgs_b = (msgs.len() as i32).to_ne_bytes();
    let msgs_b: Vec<u8>;
    if (xinfo & XACT_XINFO_HAS_INVALS) != 0 {
        msgs_b = inval_msgs_bytes(msgs)?;
        fragments[nfrags] = &nmsgs_b;
        fragments[nfrags + 1] = &msgs_b;
        nfrags += 2;
    }

    // xl_xact_twophase { TransactionId xid; } + the gid C string
    let twophase_xid_b = twophase_xid.to_ne_bytes();
    let gid_bytes: Vec<u8>;
    if (xinfo & XACT_XINFO_HAS_TWOPHASE) != 0 {
        fragments[nfrags] = &twophase_xid_b;
        nfrags += 1;
        if (xinfo & XACT_XINFO_HAS_GID) != 0 {
            let gid = twophase_gid.expect("HAS_GID implies a gid");
            let mut b = Vec::new();
            b.try_reserve(gid.len() + 1).map_err(|_| oom())?;
            b.extend_from_slice(gid.as_bytes());
            b.push(0);
            gid_bytes = b;
            fragments[nfrags] = &gid_bytes;
            nfrags += 1;
        }
    }

    // xl_xact_origin { XLogRecPtr origin_lsn; TimestampTz origin_timestamp; }
    let mut origin = [0u8; 16];
    if (xinfo & XACT_XINFO_HAS_ORIGIN) != 0 {
        origin[0..8]
            .copy_from_slice(&origin_seams::replorigin_session_origin_lsn::call().to_ne_bytes());
        origin[8..16].copy_from_slice(
            &origin_seams::replorigin_session_origin_timestamp::call().to_ne_bytes(),
        );
        fragments[nfrags] = &origin;
        nfrags += 1;
    }

    xloginsert_seams::xlog_insert_with_flags::call(
        RM_XACT_ID,
        info,
        XLOG_INCLUDE_ORIGIN,
        &fragments[..nfrags],
    )
}

/// `XactLogAbortRecord` (xact.c) — plain or twophase abort.
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

    let mut info: u8 = if twophase_xid == InvalidTransactionId {
        XLOG_XACT_ABORT
    } else {
        XLOG_XACT_ABORT_PREPARED
    };

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
        db_id = init_small::globals::MyDatabaseId();
        ts_id = init_small::globals::MyDatabaseTableSpace();
    }

    let session_origin = session_origin_or_default();
    if session_origin != types_core::InvalidRepOriginId {
        xinfo |= XACT_XINFO_HAS_ORIGIN;
    }

    if xinfo != 0 {
        info |= XLOG_XACT_HAS_INFO;
    }

    let mut fragments: [&[u8]; 12] = [&[]; 12];
    let mut nfrags: usize = 0;

    // xl_xact_abort { TimestampTz xact_time; }
    let abort_time_b = abort_time.to_ne_bytes();
    fragments[nfrags] = &abort_time_b;
    nfrags += 1;

    let xinfo_b = xinfo.to_ne_bytes();
    if xinfo != 0 {
        fragments[nfrags] = &xinfo_b;
        nfrags += 1;
    }

    let mut dbinfo = [0u8; 8];
    if (xinfo & XACT_XINFO_HAS_DBINFO) != 0 {
        dbinfo[0..4].copy_from_slice(&db_id.to_ne_bytes());
        dbinfo[4..8].copy_from_slice(&ts_id.to_ne_bytes());
        fragments[nfrags] = &dbinfo;
        nfrags += 1;
    }

    let nsubxacts_b = (subxacts.len() as i32).to_ne_bytes();
    let subxacts_bytes: Vec<u8>;
    if (xinfo & XACT_XINFO_HAS_SUBXACTS) != 0 {
        subxacts_bytes = xids_bytes(subxacts)?;
        fragments[nfrags] = &nsubxacts_b;
        fragments[nfrags + 1] = &subxacts_bytes;
        nfrags += 2;
    }

    let nrels_b = (rels.len() as i32).to_ne_bytes();
    let rels_b: Vec<u8>;
    if (xinfo & XACT_XINFO_HAS_RELFILELOCATORS) != 0 {
        rels_b = rels_bytes(rels)?;
        fragments[nfrags] = &nrels_b;
        fragments[nfrags + 1] = &rels_b;
        nfrags += 2;
    }

    let nstats_b = (dropped_stats.len() as i32).to_ne_bytes();
    let stats_b: Vec<u8>;
    if (xinfo & XACT_XINFO_HAS_DROPPED_STATS) != 0 {
        stats_b = stats_bytes(dropped_stats)?;
        fragments[nfrags] = &nstats_b;
        fragments[nfrags + 1] = &stats_b;
        nfrags += 2;
    }

    let twophase_xid_b = twophase_xid.to_ne_bytes();
    let gid_bytes: Vec<u8>;
    if (xinfo & XACT_XINFO_HAS_TWOPHASE) != 0 {
        fragments[nfrags] = &twophase_xid_b;
        nfrags += 1;
        if (xinfo & XACT_XINFO_HAS_GID) != 0 {
            let gid = twophase_gid.expect("HAS_GID implies a gid");
            let mut b = Vec::new();
            b.try_reserve(gid.len() + 1).map_err(|_| oom())?;
            b.extend_from_slice(gid.as_bytes());
            b.push(0);
            gid_bytes = b;
            fragments[nfrags] = &gid_bytes;
            nfrags += 1;
        }
    }

    let mut origin = [0u8; 16];
    if (xinfo & XACT_XINFO_HAS_ORIGIN) != 0 {
        origin[0..8]
            .copy_from_slice(&origin_seams::replorigin_session_origin_lsn::call().to_ne_bytes());
        origin[8..16].copy_from_slice(
            &origin_seams::replorigin_session_origin_timestamp::call().to_ne_bytes(),
        );
        fragments[nfrags] = &origin;
        nfrags += 1;
    }

    xloginsert_seams::xlog_insert_with_flags::call(
        RM_XACT_ID,
        info,
        XLOG_INCLUDE_ORIGIN,
        &fragments[..nfrags],
    )
}
