// xact_redo / xact_redo_commit / xact_redo_abort, plus the record parsers
// ParseCommitRecord / ParseAbortRecord (xactdesc.c pairs with wal.rs; the
// rmgrdesc unit reuses these exports). Order of execution in the redo
// bodies is critical and mirrors the C.

use crate::*;
use types_core::{Oid, RepOriginId};
use types_error::PANIC;
use types_storage::{RelFileLocator, SharedInvalidationMessage, SHARED_INVALIDATION_MESSAGE_SIZE};
use types_core::xact::XlXactStatsItem;
use xlogutils::{STANDBY_DISABLED, STANDBY_INITIALIZED};

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

/// `xl_xact_parsed_commit` (recovery-only: std collections stage the decode).
#[derive(Clone, Debug, Default)]
pub struct ParsedCommit {
    pub xact_time: TimestampTz,
    pub xinfo: u32,
    pub db_id: Oid,
    pub ts_id: Oid,
    pub subxacts: Vec<TransactionId>,
    pub xlocators: Vec<RelFileLocator>,
    pub stats: Vec<XlXactStatsItem>,
    pub msgs: Vec<SharedInvalidationMessage>,
    pub twophase_xid: TransactionId,
    pub twophase_gid: Vec<u8>,
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

#[derive(Clone, Debug, Default)]
pub struct ParsedAbort {
    pub xact_time: TimestampTz,
    pub xinfo: u32,
    pub db_id: Oid,
    pub ts_id: Oid,
    pub subxacts: Vec<TransactionId>,
    pub xlocators: Vec<RelFileLocator>,
    pub stats: Vec<XlXactStatsItem>,
    pub twophase_xid: TransactionId,
    pub twophase_gid: Vec<u8>,
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

/// `xl_xact_parsed_prepare` (the fields two-phase decoding consumes; the
/// delete-on-commit/abort rel and stats arrays stay unparsed as decode.c
/// never reads them from a PREPARE record).
#[derive(Clone, Debug, Default)]
pub struct ParsedPrepare {
    pub xact_time: TimestampTz,
    pub db_id: Oid,
    pub subxacts: Vec<TransactionId>,
    pub twophase_xid: TransactionId,
    /// GID bytes without the on-disk NUL terminator.
    pub twophase_gid: Vec<u8>,
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

fn truncated() -> Box<PgError> {
    Box::new(PgError::error("truncated transaction WAL record"))
}

// Bounds-checked native-endian cursor: a malformed on-disk record surfaces a
// recoverable error; element counts are validated against the remaining
// bytes before any collection grows.
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

    fn cstr(&mut self) -> PgResult<Vec<u8>> {
        let start = self.pos;
        while self.data.get(self.pos).copied().ok_or_else(truncated)? != 0 {
            self.pos += 1;
        }
        let mut s = Vec::new();
        s.try_reserve(self.pos - start)
            .map_err(|_| PgError::error("out of memory parsing transaction WAL record"))?;
        s.extend_from_slice(&self.data[start..self.pos]);
        self.pos += 1;
        Ok(s)
    }

    fn read_count<T>(&mut self, min_elem_bytes: usize, into: &mut Vec<T>) -> PgResult<i32> {
        let n = self.i32()?;
        if n < 0 {
            return Err(Box::new(PgError::error(
                "negative element count in transaction WAL record",
            )));
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
    Ok(RelFileLocator { spcOid: spc, dbOid: db, relNumber: rel })
}

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

/// `ParseCommitRecord` (xactdesc.c); `info` is the full xl_info byte.
pub fn parse_commit_record(info: u8, data: &[u8]) -> PgResult<ParsedCommit> {
    let mut c = Cursor::new(data);
    let mut parsed = ParsedCommit {
        xact_time: c.i64()?,
        ..Default::default()
    };

    let xinfo = if (info & XLOG_XACT_HAS_INFO) != 0 { c.u32()? } else { 0 };
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

/// `ParseAbortRecord` (xactdesc.c).
pub fn parse_abort_record(info: u8, data: &[u8]) -> PgResult<ParsedAbort> {
    let mut c = Cursor::new(data);
    let mut parsed = ParsedAbort {
        xact_time: c.i64()?,
        ..Default::default()
    };

    let xinfo = if (info & XLOG_XACT_HAS_INFO) != 0 { c.u32()? } else { 0 };
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

/// `ParsePrepareRecord` (xactdesc.c): decode the xl_xact_prepare header plus
/// the GID and subxact array that follow it. The on-disk header layout
/// (twophase.c's TwoPhaseFileHeader) is fixed:
///   magic@0 total_len@4 xid@8 database@12 prepared_at@16 owner@24
///   nsubxacts@28 ncommitrels@32 nabortrels@36 ncommitstats@40
///   nabortstats@44 ninvalmsgs@48 initfileinval@52 gidlen@54
///   origin_lsn@56 origin_timestamp@64  -> 72 bytes = MAXALIGN(72).
/// Payload: gid (gidlen bytes incl. NUL), MAXALIGNed; subxacts; then the
/// commit/abort rels, stats and inval arrays (unread here).
pub fn parse_prepare_record(_info: u8, data: &[u8]) -> PgResult<ParsedPrepare> {
    const HDR: usize = 72;
    const MAXALIGN: usize = 8;
    fn maxalign(n: usize) -> usize {
        (n + (MAXALIGN - 1)) & !(MAXALIGN - 1)
    }

    let mut c = Cursor::new(data);
    let _magic = c.u32()?;
    let _total_len = c.u32()?;
    let xid = c.u32()?;
    let database = c.u32()?;
    let prepared_at = c.i64()?;
    let _owner = c.u32()?;
    let nsubxacts = c.i32()?;
    let _ncommitrels = c.i32()?;
    let _nabortrels = c.i32()?;
    let _ncommitstats = c.i32()?;
    let _nabortstats = c.i32()?;
    let _ninvalmsgs = c.i32()?;
    let _initfileinval = c.take(1)?;
    let _pad = c.take(1)?;
    let gidlen = {
        let b = c.take(2)?;
        u16::from_ne_bytes(b.try_into().unwrap()) as usize
    };
    let origin_lsn = c.u64()?;
    let origin_timestamp = c.i64()?;
    debug_assert_eq!(c.pos, HDR);

    if nsubxacts < 0 {
        return Err(Box::new(PgError::error(
            "negative subxact count in prepare WAL record",
        )));
    }

    let mut off = HDR;
    let gid_bytes = data.get(off..off + gidlen).ok_or_else(truncated)?;
    // gidlen counts the NUL terminator (twophase.c: strlen(gid) + 1).
    let twophase_gid = gid_bytes
        .split(|&b| b == 0)
        .next()
        .unwrap_or(&[])
        .to_vec();
    off = maxalign(off + gidlen);

    let mut subxacts = Vec::new();
    subxacts
        .try_reserve(nsubxacts as usize)
        .map_err(|_| PgError::error("out of memory parsing prepare WAL record"))?;
    for i in 0..nsubxacts as usize {
        let b = data.get(off + i * 4..off + i * 4 + 4).ok_or_else(truncated)?;
        subxacts.push(u32::from_ne_bytes(b.try_into().unwrap()));
    }

    Ok(ParsedPrepare {
        xact_time: prepared_at,
        db_id: database,
        subxacts,
        twophase_xid: xid,
        twophase_gid,
        origin_lsn,
        origin_timestamp,
    })
}

fn xact_redo_commit(
    parsed: &ParsedCommit,
    xid: TransactionId,
    lsn: XLogRecPtr,
    origin_id: RepOriginId,
) -> PgResult<()> {
    debug_assert!(xid != InvalidTransactionId);

    let max_xid = transam_seams::transaction_id_latest::call(xid, &parsed.subxacts);

    // Make sure nextXid is beyond any XID mentioned in the record.
    varsup::AdvanceNextFullTransactionIdPastXid(max_xid)?;

    debug_assert_eq!(
        (parsed.xinfo & XACT_XINFO_HAS_ORIGIN) == 0,
        origin_id == types_core::InvalidRepOriginId
    );

    let commit_time = if (parsed.xinfo & XACT_XINFO_HAS_ORIGIN) != 0 {
        parsed.origin_timestamp
    } else {
        parsed.xact_time
    };

    // is_installed guard survives seam-free unit tests; installed = live.
    if commit_ts_seams::transaction_tree_set_commit_ts_data::is_installed() {
        commit_ts_seams::transaction_tree_set_commit_ts_data::call(
            xid,
            &parsed.subxacts,
            commit_time,
            origin_id,
        )?;
    }

    if xlogutils::standby_state() == STANDBY_DISABLED {
        transam_seams::transaction_id_commit_tree::call(xid, &parsed.subxacts)?;
    } else {
        procarray_seams::record_known_assigned_transaction_ids::call(max_xid)?;

        transam_seams::transaction_id_async_commit_tree::call(xid, &parsed.subxacts, lsn)?;

        procarray_seams::expire_tree_known_assigned_transaction_ids::call(
            xid,
            &parsed.subxacts,
            max_xid,
        )?;

        inval::eoxact::ProcessCommittedInvalidationMessages(
            &parsed.msgs,
            XactCompletionRelcacheInitFileInval(parsed.xinfo),
            parsed.db_id,
            parsed.ts_id,
        )?;

        if (parsed.xinfo & XACT_XINFO_HAS_AE_LOCKS) != 0 {
            standby_seams::standby_release_lock_tree::call(xid, &parsed.subxacts)?;
        }
    }

    if (parsed.xinfo & XACT_XINFO_HAS_ORIGIN) != 0 {
        origin_seams::replorigin_advance::call(
            origin_id,
            parsed.origin_lsn,
            lsn,
            false, // backward
            false, // WAL
        )?;
    }

    if !parsed.xlocators.is_empty() {
        xlog_seams::xlog_flush::call(lsn)?;
        catalog_storage_seams::drop_relation_files::call(&parsed.xlocators, true)?;
    }

    if !parsed.stats.is_empty() {
        xlog_seams::xlog_flush::call(lsn)?;
        pgstat::xact::pgstat_execute_transactional_drops(&parsed.stats, true)?;
    }

    if XactCompletionForceSyncCommit(parsed.xinfo) {
        xlog_seams::xlog_flush::call(lsn)?;
    }

    if XactCompletionApplyFeedback(parsed.xinfo) {
        xlogrecovery_seams::xlog_request_wal_receiver_reply::call();
    }

    Ok(())
}

fn xact_redo_abort(
    parsed: &ParsedAbort,
    xid: TransactionId,
    lsn: XLogRecPtr,
    origin_id: RepOriginId,
) -> PgResult<()> {
    debug_assert!(xid != InvalidTransactionId);

    let max_xid = transam_seams::transaction_id_latest::call(xid, &parsed.subxacts);
    varsup::AdvanceNextFullTransactionIdPastXid(max_xid)?;

    if xlogutils::standby_state() == STANDBY_DISABLED {
        transam_seams::transaction_id_abort_tree::call(xid, &parsed.subxacts)?;
    } else {
        procarray_seams::record_known_assigned_transaction_ids::call(max_xid)?;

        transam_seams::transaction_id_abort_tree::call(xid, &parsed.subxacts)?;

        procarray_seams::expire_tree_known_assigned_transaction_ids::call(
            xid,
            &parsed.subxacts,
            max_xid,
        )?;


        if (parsed.xinfo & XACT_XINFO_HAS_AE_LOCKS) != 0 {
            standby_seams::standby_release_lock_tree::call(xid, &parsed.subxacts)?;
        }
    }

    if (parsed.xinfo & XACT_XINFO_HAS_ORIGIN) != 0 {
        origin_seams::replorigin_advance::call(
            origin_id,
            parsed.origin_lsn,
            lsn,
            false, // backward
            false, // WAL
        )?;
    }

    if !parsed.xlocators.is_empty() {
        xlog_seams::xlog_flush::call(lsn)?;
        catalog_storage_seams::drop_relation_files::call(&parsed.xlocators, true)?;
    }

    if !parsed.stats.is_empty() {
        xlog_seams::xlog_flush::call(lsn)?;
        pgstat::xact::pgstat_execute_transactional_drops(&parsed.stats, true)?;
    }

    Ok(())
}

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
            // Delete the TwoPhaseState gxact entry and/or 2PC file (C holds
            // TwoPhaseStateLock around this; the installed impl carries it).
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
            twophase_seams::prepare_redo_remove::call(parsed.twophase_xid, false)
        }
        XLOG_XACT_PREPARE => {
            twophase_seams::prepare_redo_add::call(
                record.data,
                record.read_rec_ptr,
                record.end_rec_ptr,
                record.origin_id,
            )
        }
        XLOG_XACT_ASSIGNMENT => {
            if xlogutils::standby_state() >= STANDBY_INITIALIZED {
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
            Ok(())
        }
        other => Err(Box::new(PgError::new(
            PANIC,
            format!("xact_redo: unknown op code {other}"),
        ))),
    }
}
