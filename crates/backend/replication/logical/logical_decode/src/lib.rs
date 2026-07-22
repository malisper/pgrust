#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use elog::elog;
use logical::{filter_by_origin_cb_wrapper, LogicalDecodingContext};
use mcx::{Mcx, MemoryContext, PgVec};
use reorderbuffer::{ReorderBufferChange, ReorderBufferChangeData, ReorderBufferChangeType};
use snapbuild::SnapBuildState;
use types_core::{
    InvalidOid, Oid, RepOriginId, TimestampTz, TransactionId, TransactionIdIsValid, XLogRecPtr,
};
use types_error::{PgResult, ERROR};
use types_storage::{RelFileLocator, SharedInvalidationMessage};
use types_tuple::{BlockIdData, ItemPointerData, SizeofHeapTupleHeader};
use xact::{
    parse_abort_record, parse_commit_record, XACT_XINFO_HAS_ORIGIN, XLOG_XACT_ABORT,
    XLOG_XACT_ABORT_PREPARED, XLOG_XACT_ASSIGNMENT, XLOG_XACT_COMMIT, XLOG_XACT_COMMIT_PREPARED,
    XLOG_XACT_INVALIDATIONS, XLOG_XACT_OPMASK, XLOG_XACT_PREPARE,
};
use xlogreader::LocalPageRead;

const InvalidXLogRecPtr: XLogRecPtr = 0;

const RM_XLOG_ID: u8 = 0;
const RM_XACT_ID: u8 = 1;
const RM_STANDBY_ID: u8 = 8;
const RM_HEAP2_ID: u8 = 9;
const RM_HEAP_ID: u8 = 10;
const RM_LOGICALMSG_ID: u8 = 21;

const XLR_INFO_MASK: u8 = 0x0F;

// heapam_xlog.h layout constants (private in the heapam_xlog crate).
const XLOG_HEAP_OPMASK: u8 = 0x70;
const XLOG_HEAP_INSERT: u8 = 0x00;
const XLOG_HEAP_DELETE: u8 = 0x10;
const XLOG_HEAP_UPDATE: u8 = 0x20;
const XLOG_HEAP_TRUNCATE: u8 = 0x30;
const XLOG_HEAP_HOT_UPDATE: u8 = 0x40;
const XLOG_HEAP_CONFIRM: u8 = 0x50;
const XLOG_HEAP_LOCK: u8 = 0x60;
const XLOG_HEAP_INPLACE: u8 = 0x70;
const XLOG_HEAP2_REWRITE: u8 = 0x00;
const XLOG_HEAP2_PRUNE_ON_ACCESS: u8 = 0x10;
const XLOG_HEAP2_PRUNE_VACUUM_SCAN: u8 = 0x20;
const XLOG_HEAP2_PRUNE_VACUUM_CLEANUP: u8 = 0x30;
const XLOG_HEAP2_VISIBLE: u8 = 0x40;
const XLOG_HEAP2_MULTI_INSERT: u8 = 0x50;
const XLOG_HEAP2_LOCK_UPDATED: u8 = 0x60;
const XLOG_HEAP2_NEW_CID: u8 = 0x70;

const XLH_INSERT_LAST_IN_MULTI: u8 = 1 << 1;
const XLH_INSERT_IS_SPECULATIVE: u8 = 1 << 2;
const XLH_INSERT_CONTAINS_NEW_TUPLE: u8 = 1 << 3;
const XLH_INSERT_ON_TOAST_RELATION: u8 = 1 << 4;
const XLH_UPDATE_CONTAINS_OLD_TUPLE: u8 = 1 << 2;
const XLH_UPDATE_CONTAINS_OLD_KEY: u8 = 1 << 3;
const XLH_UPDATE_CONTAINS_NEW_TUPLE: u8 = 1 << 4;
const XLH_DELETE_CONTAINS_OLD_TUPLE: u8 = 1 << 1;
const XLH_DELETE_CONTAINS_OLD_KEY: u8 = 1 << 2;
const XLH_DELETE_IS_SUPER: u8 = 1 << 3;
const XLH_TRUNCATE_CASCADE: u8 = 1 << 0;
const XLH_TRUNCATE_RESTART_SEQS: u8 = 1 << 1;

const SizeOfHeapDelete: usize = 8;
const SizeOfHeapUpdate: usize = 14;
const SizeOfHeapHeader: usize = 5;
const SizeOfHeapTruncate: usize = 12;
const SizeOfMultiInsertTuple: usize = 7;

const XLOG_LOGICAL_MESSAGE: u8 = 0x00;
const XLOG_STANDBY_LOCK: u8 = 0x00;
const XLOG_RUNNING_XACTS: u8 = 0x10;
const XLOG_INVALIDATIONS: u8 = 0x20;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported callee reached from decode.c: {what}")
}

thread_local! {
    // Truncate relid arrays outlive the decode call inside ReorderBuffer changes.
    static DECODE_CTX: &'static MemoryContext =
        ::mcx::session_root("LogicalDecode");
}

fn decode_mcx() -> Mcx<'static> {
    DECODE_CTX.with(|c| c.mcx())
}

#[derive(Clone, Copy)]
struct XLogRecordBuffer {
    origptr: XLogRecPtr,
    endptr: XLogRecPtr,
}

fn u16_at(data: &[u8], off: usize) -> u16 {
    u16::from_ne_bytes(data[off..off + 2].try_into().expect("in bounds"))
}

fn u32_at(data: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(data[off..off + 4].try_into().expect("in bounds"))
}

fn u64_at(data: &[u8], off: usize) -> u64 {
    u64::from_ne_bytes(data[off..off + 8].try_into().expect("in bounds"))
}

pub fn LogicalDecodingProcessRecord(ctx: &mut LogicalDecodingContext) -> PgResult<()> {
    let buf = XLogRecordBuffer {
        origptr: ctx.reader.v.ReadRecPtr,
        endptr: ctx.reader.v.EndRecPtr,
    };

    // Mirror ReorderBufferCanStartStreaming's decoding-context half into the
    // buffer before dispatch (reorderbuffer.c:4285): a consistent snapshot
    // that does not skip the record being decoded. Eviction mid-record reads
    // this instead of reaching the builder across the crate boundary.
    ctx.reorder.streaming_ready = ctx.snapshot_builder.current_state()
        == snapbuild::SnapBuildState::Consistent
        && !ctx.snapshot_builder.xact_needs_skip(buf.origptr);

    let txid = ctx.reader.XLogRecGetTopXid();
    if TransactionIdIsValid(txid) {
        let xid = ctx.reader.XLogRecGetXid();
        ctx.reorder.assign_child(txid, xid, buf.origptr);
    }

    // C 18.3 dispatches via the rmgr table's rm_decode; matching on rmid here
    // avoids an rmgr -> logical_decode dependency inversion.
    match ctx.reader.XLogRecGetRmid() {
        RM_XLOG_ID => xlog_decode(ctx, buf),
        RM_XACT_ID => xact_decode(ctx, buf),
        RM_STANDBY_ID => standby_decode(ctx, buf),
        RM_HEAP2_ID => heap2_decode(ctx, buf),
        RM_HEAP_ID => heap_decode(ctx, buf),
        RM_LOGICALMSG_ID => logicalmsg_decode(ctx, buf),
        _ => {
            let xid = ctx.reader.XLogRecGetXid();
            ctx.reorder.process_xid(xid, buf.origptr);
            Ok(())
        }
    }
}

fn xlog_decode(ctx: &mut LogicalDecodingContext, buf: XLogRecordBuffer) -> PgResult<()> {
    let info = ctx.reader.XLogRecGetInfo() & !XLR_INFO_MASK;
    let xid = ctx.reader.XLogRecGetXid();
    ctx.reorder.process_xid(xid, buf.origptr);

    match info {
        transam_xlog::XLOG_CHECKPOINT_SHUTDOWN | transam_xlog::XLOG_END_OF_RECOVERY => {
            ctx.snapshot_builder
                .serialization_point(&mut ctx.reorder, buf.origptr)?;
        }
        transam_xlog::XLOG_CHECKPOINT_ONLINE => {}
        transam_xlog::XLOG_PARAMETER_CHANGE => {
            // xl_parameter_change.wal_level is at offset 20.
            let wal_level = u32_at(ctx.reader.XLogRecGetData(), 20) as i32;
            if wal_level < transam_xlog::WAL_LEVEL_LOGICAL {
                unported("xlog_decode: wal_level dropped below logical (standby-only)");
            }
        }
        transam_xlog::XLOG_NOOP
        | transam_xlog::XLOG_NEXTOID
        | transam_xlog::XLOG_SWITCH
        | transam_xlog::XLOG_BACKUP_END
        | transam_xlog::XLOG_RESTORE_POINT
        | transam_xlog::XLOG_FPW_CHANGE
        | transam_xlog::XLOG_FPI_FOR_HINT
        | transam_xlog::XLOG_FPI
        | transam_xlog::XLOG_OVERWRITE_CONTRECORD
        | transam_xlog::XLOG_CHECKPOINT_REDO => {}
        _ => return elog(ERROR, format!("unexpected RM_XLOG_ID record type: {info}")),
    }
    Ok(())
}

fn xact_decode(ctx: &mut LogicalDecodingContext, buf: XLogRecordBuffer) -> PgResult<()> {
    let info = ctx.reader.XLogRecGetInfo() & XLOG_XACT_OPMASK;

    if ctx.snapshot_builder.current_state() < SnapBuildState::FullSnapshot {
        return Ok(());
    }

    match info {
        XLOG_XACT_COMMIT | XLOG_XACT_COMMIT_PREPARED => {
            let parsed =
                parse_commit_record(ctx.reader.XLogRecGetInfo(), ctx.reader.XLogRecGetData())?;
            let xid = if !TransactionIdIsValid(parsed.twophase_xid) {
                ctx.reader.XLogRecGetXid()
            } else {
                parsed.twophase_xid
            };
            let two_phase = if info == XLOG_XACT_COMMIT_PREPARED {
                !FilterPrepare(ctx, xid, &parsed.twophase_gid)?
            } else {
                false
            };
            DecodeCommit(ctx, buf, &parsed, xid, two_phase)?;
        }
        XLOG_XACT_ABORT | XLOG_XACT_ABORT_PREPARED => {
            let parsed =
                parse_abort_record(ctx.reader.XLogRecGetInfo(), ctx.reader.XLogRecGetData())?;
            let xid = if !TransactionIdIsValid(parsed.twophase_xid) {
                ctx.reader.XLogRecGetXid()
            } else {
                parsed.twophase_xid
            };
            let two_phase = if info == XLOG_XACT_ABORT_PREPARED {
                !FilterPrepare(ctx, xid, &parsed.twophase_gid)?
            } else {
                false
            };
            DecodeAbort(ctx, buf, &parsed, xid, two_phase)?;
        }
        XLOG_XACT_ASSIGNMENT => {}
        XLOG_XACT_INVALIDATIONS => {
            let xid = ctx.reader.XLogRecGetXid();
            let msgs = parse_xact_invals(ctx.reader.XLogRecGetData());
            if TransactionIdIsValid(xid) {
                if !ctx.fast_forward {
                    ctx.reorder.add_invalidations(xid, buf.origptr, &msgs)?;
                }
                ctx.reorder.xid_set_catalog_changes(xid, buf.origptr);
            } else if !ctx.fast_forward {
                ctx.reorder.immediate_invalidation(&msgs)?;
            }
        }
        XLOG_XACT_PREPARE => {
            let parsed =
                xact::parse_prepare_record(ctx.reader.XLogRecGetInfo(), ctx.reader.XLogRecGetData())?;

            // Process the transaction in a two-phase manner iff the output
            // plugin supports two-phase commits and doesn't filter the
            // transaction at prepare time.
            if FilterPrepare(ctx, parsed.twophase_xid, &parsed.twophase_gid)? {
                ctx.reorder.process_xid(parsed.twophase_xid, buf.origptr);
            } else {
                DecodePrepare(ctx, buf, &parsed)?;
            }
        }
        _ => return elog(ERROR, format!("unexpected RM_XACT_ID record type: {info}")),
    }
    Ok(())
}

// xl_xact_invals: int nmsgs; SharedInvalidationMessage msgs[].
fn parse_xact_invals(data: &[u8]) -> Vec<SharedInvalidationMessage> {
    const MSG_SIZE: usize = types_storage::SHARED_INVALIDATION_MESSAGE_SIZE;
    let nmsgs = u32_at(data, 0) as usize;
    let mut msgs = Vec::with_capacity(nmsgs);
    let mut off = 4;
    for _ in 0..nmsgs {
        let bytes: [u8; MSG_SIZE] = data[off..off + MSG_SIZE].try_into().expect("in bounds");
        let msg = SharedInvalidationMessage::from_wire_bytes(bytes)
            .expect("valid shared-invalidation message in XLOG_XACT_INVALIDATIONS");
        msgs.push(msg);
        off += MSG_SIZE;
    }
    msgs
}

fn standby_decode(ctx: &mut LogicalDecodingContext, buf: XLogRecordBuffer) -> PgResult<()> {
    let info = ctx.reader.XLogRecGetInfo() & !XLR_INFO_MASK;
    let xid = ctx.reader.XLogRecGetXid();
    ctx.reorder.process_xid(xid, buf.origptr);

    match info {
        XLOG_RUNNING_XACTS => {
            // xl_running_xacts main-data layout (standbydefs.h).
            let data = ctx.reader.XLogRecGetData();
            let xcnt = u32_at(data, 0);
            let subxcnt = u32_at(data, 4);
            let subxid_overflow = data[8] != 0;
            let next_xid = u32_at(data, 12);
            let oldest_running_xid = u32_at(data, 16);
            let latest_completed_xid = u32_at(data, 20);
            let total = (xcnt + subxcnt) as usize;
            let mut xids: Vec<TransactionId> = Vec::with_capacity(total);
            for i in 0..total {
                xids.push(u32_at(data, 24 + i * 4));
            }
            let running = snapbuild::XlRunningXacts {
                xcnt,
                subxcnt,
                subxid_overflow,
                next_xid,
                oldest_running_xid,
                latest_completed_xid,
                xids: &xids,
            };
            ctx.snapshot_builder
                .process_running_xacts(&mut ctx.reorder, buf.origptr, &running)?;
            ctx.reorder.abort_old(oldest_running_xid)?;
        }
        XLOG_STANDBY_LOCK => {}
        XLOG_INVALIDATIONS => {}
        _ => {
            return elog(
                ERROR,
                format!("unexpected RM_STANDBY_ID record type: {info}"),
            )
        }
    }
    Ok(())
}

fn heap2_decode(ctx: &mut LogicalDecodingContext, buf: XLogRecordBuffer) -> PgResult<()> {
    let info = ctx.reader.XLogRecGetInfo() & XLOG_HEAP_OPMASK;
    let xid = ctx.reader.XLogRecGetXid();
    ctx.reorder.process_xid(xid, buf.origptr);

    if ctx.snapshot_builder.current_state() < SnapBuildState::FullSnapshot {
        return Ok(());
    }

    match info {
        XLOG_HEAP2_MULTI_INSERT => {
            if ctx
                .snapshot_builder
                .process_change(&mut ctx.reorder, xid, buf.origptr)
                && !ctx.fast_forward
            {
                DecodeMultiInsert(ctx, buf)?;
            }
        }
        XLOG_HEAP2_NEW_CID => {
            if !ctx.fast_forward {
                let xlrec = parse_new_cid(ctx.reader.XLogRecGetData());
                ctx.snapshot_builder
                    .process_new_cid(&mut ctx.reorder, xid, buf.origptr, &xlrec)?;
            }
        }
        XLOG_HEAP2_REWRITE
        | XLOG_HEAP2_PRUNE_ON_ACCESS
        | XLOG_HEAP2_PRUNE_VACUUM_SCAN
        | XLOG_HEAP2_PRUNE_VACUUM_CLEANUP
        | XLOG_HEAP2_VISIBLE
        | XLOG_HEAP2_LOCK_UPDATED => {}
        _ => return elog(ERROR, format!("unexpected RM_HEAP2_ID record type: {info}")),
    }
    Ok(())
}

// xl_heap_new_cid main-data layout (34 bytes, unpadded on disk).
fn parse_new_cid(data: &[u8]) -> heapam_xlog::XlHeapNewCid {
    heapam_xlog::XlHeapNewCid {
        top_xid: u32_at(data, 0),
        cmin: u32_at(data, 4),
        cmax: u32_at(data, 8),
        combocid: u32_at(data, 12),
        target_locator: RelFileLocator {
            spcOid: u32_at(data, 16),
            dbOid: u32_at(data, 20),
            relNumber: u32_at(data, 24),
        },
        target_tid: ItemPointerData {
            ip_blkid: BlockIdData {
                bi_hi: u16_at(data, 28),
                bi_lo: u16_at(data, 30),
            },
            ip_posid: u16_at(data, 32),
        },
    }
}

fn heap_decode(ctx: &mut LogicalDecodingContext, buf: XLogRecordBuffer) -> PgResult<()> {
    let info = ctx.reader.XLogRecGetInfo() & XLOG_HEAP_OPMASK;
    let xid = ctx.reader.XLogRecGetXid();
    ctx.reorder.process_xid(xid, buf.origptr);

    if ctx.snapshot_builder.current_state() < SnapBuildState::FullSnapshot {
        return Ok(());
    }

    match info {
        XLOG_HEAP_INSERT => {
            if ctx
                .snapshot_builder
                .process_change(&mut ctx.reorder, xid, buf.origptr)
                && !ctx.fast_forward
            {
                DecodeInsert(ctx, buf)?;
            }
        }
        XLOG_HEAP_HOT_UPDATE | XLOG_HEAP_UPDATE => {
            if ctx
                .snapshot_builder
                .process_change(&mut ctx.reorder, xid, buf.origptr)
                && !ctx.fast_forward
            {
                DecodeUpdate(ctx, buf)?;
            }
        }
        XLOG_HEAP_DELETE => {
            if ctx
                .snapshot_builder
                .process_change(&mut ctx.reorder, xid, buf.origptr)
                && !ctx.fast_forward
            {
                DecodeDelete(ctx, buf)?;
            }
        }
        XLOG_HEAP_TRUNCATE => {
            if ctx
                .snapshot_builder
                .process_change(&mut ctx.reorder, xid, buf.origptr)
                && !ctx.fast_forward
            {
                DecodeTruncate(ctx, buf)?;
            }
        }
        XLOG_HEAP_INPLACE => {}
        XLOG_HEAP_CONFIRM => {
            if ctx
                .snapshot_builder
                .process_change(&mut ctx.reorder, xid, buf.origptr)
                && !ctx.fast_forward
            {
                DecodeSpecConfirm(ctx, buf)?;
            }
        }
        XLOG_HEAP_LOCK => {}
        _ => return elog(ERROR, format!("unexpected RM_HEAP_ID record type: {info}")),
    }
    Ok(())
}

// Ask the output plugin whether we want to skip this PREPARE and send this
// transaction as a regular commit later (decode.c:551).
fn FilterPrepare(
    ctx: &mut LogicalDecodingContext,
    xid: TransactionId,
    gid: &[u8],
) -> PgResult<bool> {
    // Skip if decoding of two-phase transactions at PREPARE time is not
    // enabled. In that case, all two-phase transactions are considered
    // filtered out and will be applied as regular transactions at COMMIT
    // PREPARED.
    if !ctx.opc().twophase {
        return Ok(true);
    }

    // The filter_prepare callback is optional. When not supplied, all
    // prepared transactions should go through.
    if ctx.opc().callbacks.filter_prepare_cb.is_none() {
        return Ok(false);
    }

    let gid = String::from_utf8_lossy(gid).into_owned();
    logical::filter_prepare_cb_wrapper(ctx.opc(), xid, &gid)
}

fn FilterByOrigin(ctx: &mut LogicalDecodingContext, origin_id: RepOriginId) -> PgResult<bool> {
    if ctx.opc().callbacks.filter_by_origin_cb.is_none() {
        return Ok(false);
    }
    filter_by_origin_cb_wrapper(ctx.opc(), origin_id)
}

fn logicalmsg_decode(ctx: &mut LogicalDecodingContext, buf: XLogRecordBuffer) -> PgResult<()> {
    let info = ctx.reader.XLogRecGetInfo() & !XLR_INFO_MASK;
    if info != XLOG_LOGICAL_MESSAGE {
        return elog(
            ERROR,
            format!("unexpected RM_LOGICALMSG_ID record type: {info}"),
        );
    }

    let xid = ctx.reader.XLogRecGetXid();
    let origin_id = ctx.reader.XLogRecGetOrigin();
    ctx.reorder.process_xid(xid, buf.origptr);

    if ctx.snapshot_builder.current_state() < SnapBuildState::FullSnapshot {
        return Ok(());
    }

    // xl_logical_message: dbId@0, transactional@4, prefix_size@8,
    // message_size@16, payload@24 (NUL-terminated prefix, then message bytes).
    let data = ctx.reader.XLogRecGetData();
    let db_id = u32_at(data, 0);
    let transactional = data[4] != 0;
    let prefix_size = u64_at(data, 8) as usize;
    let message_size = u64_at(data, 16) as usize;

    if db_id != ctx.slot.data.get().database || FilterByOrigin(ctx, origin_id)? {
        return Ok(());
    }

    if transactional {
        if !ctx
            .snapshot_builder
            .process_change(&mut ctx.reorder, xid, buf.origptr)
        {
            return Ok(());
        }
    } else if ctx.snapshot_builder.current_state() != SnapBuildState::Consistent
        || ctx.snapshot_builder.xact_needs_skip(buf.origptr)
    {
        return Ok(());
    }

    if ctx.fast_forward {
        if !transactional {
            ctx.processing_required = true;
        }
        return Ok(());
    }

    let snapshot = if !transactional {
        Some(ctx.snapshot_builder.get_or_build_snapshot())
    } else {
        None
    };

    let data = ctx.reader.XLogRecGetData();
    let prefix = std::str::from_utf8(&data[24..24 + prefix_size - 1])
        .expect("message prefix is utf8")
        .to_string();
    let message = data[24 + prefix_size..24 + prefix_size + message_size].to_vec();
    ctx.reorder
        .queue_message(xid, snapshot, buf.endptr, transactional, &prefix, &message)
}

fn DecodeCommit(
    ctx: &mut LogicalDecodingContext,
    buf: XLogRecordBuffer,
    parsed: &xact::ParsedCommit,
    xid: TransactionId,
    two_phase: bool,
) -> PgResult<()> {
    let mut origin_lsn = InvalidXLogRecPtr;
    let mut commit_time: TimestampTz = parsed.xact_time;
    let origin_id = ctx.reader.XLogRecGetOrigin();

    if parsed.xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        origin_lsn = parsed.origin_lsn;
        commit_time = parsed.origin_timestamp;
    }

    ctx.snapshot_builder.commit_txn(
        &mut ctx.reorder,
        buf.origptr,
        xid,
        &parsed.subxacts,
        parsed.xinfo,
    )?;

    if DecodeTXNNeedSkip(ctx, buf, parsed.db_id, origin_id)? {
        for &subxid in &parsed.subxacts {
            ctx.reorder.forget(subxid, buf.origptr)?;
        }
        ctx.reorder.forget(xid, buf.origptr)?;
        return Ok(());
    }

    for &subxid in &parsed.subxacts {
        ctx.reorder
            .commit_child(xid, subxid, buf.origptr, buf.endptr);
    }

    // Send the final commit record if the transaction data is already
    // decoded, otherwise process the entire transaction.
    if two_phase {
        let two_phase_at = ctx.snapshot_builder.get_two_phase_at();
        let gid = String::from_utf8_lossy(&parsed.twophase_gid).into_owned();
        ctx.reorder.finish_prepared(
            xid,
            buf.origptr,
            buf.endptr,
            two_phase_at,
            commit_time,
            origin_id,
            origin_lsn,
            &gid,
            true,
        )?;
    } else {
        ctx.reorder.commit(
            xid,
            buf.origptr,
            buf.endptr,
            commit_time,
            origin_id,
            origin_lsn,
        )?;
    }

    logical::UpdateDecodingStats(ctx);
    Ok(())
}

// Decode PREPARE record (decode.c:763). Similar logic as in DecodeCommit.
//
// Note that we don't skip prepare even if we have detected a concurrent
// abort, because we may have already sent some changes that the subscriber
// must be able to roll back via prepare + rollback prepared.
fn DecodePrepare(
    ctx: &mut LogicalDecodingContext,
    buf: XLogRecordBuffer,
    parsed: &xact::ParsedPrepare,
) -> PgResult<()> {
    let origin_lsn = parsed.origin_lsn;
    let mut prepare_time: TimestampTz = parsed.xact_time;
    let origin_id = ctx.reader.XLogRecGetOrigin();
    let xid = parsed.twophase_xid;

    if parsed.origin_timestamp != 0 {
        prepare_time = parsed.origin_timestamp;
    }

    // Remember the prepare info for the txn so that it can be used later in
    // commit prepared if required. See ReorderBufferFinishPrepared.
    if !ctx.reorder.remember_prepare_info(
        xid,
        buf.origptr,
        buf.endptr,
        prepare_time,
        origin_id,
        origin_lsn,
    ) {
        return Ok(());
    }

    // We can't start streaming unless a consistent state is reached.
    if ctx.snapshot_builder.current_state() < SnapBuildState::Consistent {
        ctx.reorder.skip_prepare(xid);
        return Ok(());
    }

    // Check whether we need to process this transaction. We can't call
    // ReorderBufferForget as in DecodeCommit: the txn hasn't committed yet
    // and removing it early could produce an incorrect restart_lsn (see
    // SnapBuildProcessRunningXacts) — but cache invalidations must run.
    if DecodeTXNNeedSkip(ctx, buf, parsed.db_id, origin_id)? {
        ctx.reorder.skip_prepare(xid);
        ctx.reorder.invalidate(xid, buf.origptr)?;
        return Ok(());
    }

    // Tell the reorderbuffer about the surviving subtransactions.
    for &subxid in &parsed.subxacts {
        ctx.reorder
            .commit_child(xid, subxid, buf.origptr, buf.endptr);
    }

    // Replay actions of all transaction + subtransactions in order.
    let gid = String::from_utf8_lossy(&parsed.twophase_gid).into_owned();
    ctx.reorder.prepare(xid, &gid)?;

    logical::UpdateDecodingStats(ctx);
    Ok(())
}

fn DecodeAbort(
    ctx: &mut LogicalDecodingContext,
    buf: XLogRecordBuffer,
    parsed: &xact::ParsedAbort,
    xid: TransactionId,
    two_phase: bool,
) -> PgResult<()> {
    let mut origin_lsn = InvalidXLogRecPtr;
    let mut abort_time: TimestampTz = parsed.xact_time;
    let origin_id = ctx.reader.XLogRecGetOrigin();

    if parsed.xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        origin_lsn = parsed.origin_lsn;
        abort_time = parsed.origin_timestamp;
    }

    let skip_xact = DecodeTXNNeedSkip(ctx, buf, parsed.db_id, origin_id)?;

    // Send the final rollback record for a prepared transaction unless we
    // need to skip it. For non-two-phase xacts, simply forget the xact.
    if two_phase && !skip_xact {
        let gid = String::from_utf8_lossy(&parsed.twophase_gid).into_owned();
        ctx.reorder.finish_prepared(
            xid,
            buf.origptr,
            buf.endptr,
            InvalidXLogRecPtr,
            abort_time,
            origin_id,
            origin_lsn,
            &gid,
            false,
        )?;
    } else {
        let end = ctx.reader.v.EndRecPtr;
        for &subxid in &parsed.subxacts {
            ctx.reorder.abort(subxid, end, abort_time)?;
        }
        ctx.reorder.abort(xid, end, abort_time)?;
    }

    logical::UpdateDecodingStats(ctx);
    Ok(())
}

fn block0_locator(ctx: &LogicalDecodingContext) -> RelFileLocator {
    let (locator, _, _, _) = ctx
        .reader
        .XLogRecGetBlockTagExtended(0)
        .expect("heap record has block 0");
    locator
}

fn DecodeInsert(ctx: &mut LogicalDecodingContext, buf: XLogRecordBuffer) -> PgResult<()> {
    // xl_heap_insert.flags is at offset 2.
    let flags = ctx.reader.XLogRecGetData()[2];

    if flags & XLH_INSERT_CONTAINS_NEW_TUPLE == 0 {
        return Ok(());
    }

    let target_locator = block0_locator(ctx);
    if target_locator.dbOid != ctx.slot.data.get().database {
        return Ok(());
    }

    let origin = ctx.reader.XLogRecGetOrigin();
    if FilterByOrigin(ctx, origin)? {
        return Ok(());
    }

    let action = if flags & XLH_INSERT_IS_SPECULATIVE == 0 {
        ReorderBufferChangeType::Insert
    } else {
        ReorderBufferChangeType::InternalSpecInsert
    };

    let datalen = ctx
        .reader
        .XLogRecGetBlockData(0)
        .expect("insert block data present")
        .len();
    let tuplelen = datalen - SizeOfHeapHeader;

    let mut newtuple = ctx.reorder.alloc_tuple_buf(tuplelen)?;
    let tupledata = ctx
        .reader
        .XLogRecGetBlockData(0)
        .expect("insert block data present");
    DecodeXLogTuple(tupledata, &mut newtuple);

    let mut change = ReorderBufferChange::new(
        action,
        ReorderBufferChangeData::Tp {
            rlocator: target_locator,
            clear_toast_afterwards: true,
            oldtuple: None,
            newtuple: Some(newtuple),
        },
    );
    change.origin_id = origin;

    let xid = ctx.reader.XLogRecGetXid();
    ctx.reorder.queue_change(
        xid,
        buf.origptr,
        change,
        flags & XLH_INSERT_ON_TOAST_RELATION != 0,
    )
}

fn DecodeUpdate(ctx: &mut LogicalDecodingContext, buf: XLogRecordBuffer) -> PgResult<()> {
    // xl_heap_update.flags is at offset 7.
    let flags = ctx.reader.XLogRecGetData()[7];

    let target_locator = block0_locator(ctx);
    if target_locator.dbOid != ctx.slot.data.get().database {
        return Ok(());
    }

    let origin = ctx.reader.XLogRecGetOrigin();
    if FilterByOrigin(ctx, origin)? {
        return Ok(());
    }

    let mut newtuple = None;
    if flags & XLH_UPDATE_CONTAINS_NEW_TUPLE != 0 {
        let datalen = ctx
            .reader
            .XLogRecGetBlockData(0)
            .expect("update block data present")
            .len();
        let mut t = ctx.reorder.alloc_tuple_buf(datalen - SizeOfHeapHeader)?;
        let data = ctx
            .reader
            .XLogRecGetBlockData(0)
            .expect("update block data present");
        DecodeXLogTuple(data, &mut t);
        newtuple = Some(t);
    }

    let mut oldtuple = None;
    if flags & (XLH_UPDATE_CONTAINS_OLD_TUPLE | XLH_UPDATE_CONTAINS_OLD_KEY) != 0 {
        let datalen = ctx.reader.XLogRecGetDataLen() as usize - SizeOfHeapUpdate;
        let mut t = ctx.reorder.alloc_tuple_buf(datalen - SizeOfHeapHeader)?;
        let rec = ctx.reader.XLogRecGetData();
        DecodeXLogTuple(&rec[SizeOfHeapUpdate..], &mut t);
        oldtuple = Some(t);
    }

    let mut change = ReorderBufferChange::new(
        ReorderBufferChangeType::Update,
        ReorderBufferChangeData::Tp {
            rlocator: target_locator,
            clear_toast_afterwards: true,
            oldtuple,
            newtuple,
        },
    );
    change.origin_id = origin;

    let xid = ctx.reader.XLogRecGetXid();
    ctx.reorder.queue_change(xid, buf.origptr, change, false)
}

fn DecodeDelete(ctx: &mut LogicalDecodingContext, buf: XLogRecordBuffer) -> PgResult<()> {
    // xl_heap_delete.flags is at offset 7.
    let flags = ctx.reader.XLogRecGetData()[7];

    let target_locator = block0_locator(ctx);
    if target_locator.dbOid != ctx.slot.data.get().database {
        return Ok(());
    }

    let origin = ctx.reader.XLogRecGetOrigin();
    if FilterByOrigin(ctx, origin)? {
        return Ok(());
    }

    let action = if flags & XLH_DELETE_IS_SUPER != 0 {
        ReorderBufferChangeType::InternalSpecAbort
    } else {
        ReorderBufferChangeType::Delete
    };

    let mut oldtuple = None;
    if flags & (XLH_DELETE_CONTAINS_OLD_TUPLE | XLH_DELETE_CONTAINS_OLD_KEY) != 0 {
        let datalen = ctx.reader.XLogRecGetDataLen() as usize - SizeOfHeapDelete;
        debug_assert!(ctx.reader.XLogRecGetDataLen() as usize > SizeOfHeapDelete + SizeOfHeapHeader);
        let mut t = ctx.reorder.alloc_tuple_buf(datalen - SizeOfHeapHeader)?;
        let rec = ctx.reader.XLogRecGetData();
        DecodeXLogTuple(&rec[SizeOfHeapDelete..], &mut t);
        oldtuple = Some(t);
    }

    let mut change = ReorderBufferChange::new(
        action,
        ReorderBufferChangeData::Tp {
            rlocator: target_locator,
            clear_toast_afterwards: true,
            oldtuple,
            newtuple: None,
        },
    );
    change.origin_id = origin;

    let xid = ctx.reader.XLogRecGetXid();
    ctx.reorder.queue_change(xid, buf.origptr, change, false)
}

fn DecodeTruncate(ctx: &mut LogicalDecodingContext, buf: XLogRecordBuffer) -> PgResult<()> {
    // xl_heap_truncate: dbId@0, nrelids@4, flags@8, relids@12.
    let data = ctx.reader.XLogRecGetData();
    let db_id = u32_at(data, 0);
    let nrelids = u32_at(data, 4) as usize;
    let flags = data[8];

    if db_id != ctx.slot.data.get().database {
        return Ok(());
    }

    let origin = ctx.reader.XLogRecGetOrigin();
    if FilterByOrigin(ctx, origin)? {
        return Ok(());
    }

    let data = ctx.reader.XLogRecGetData();
    let mut relids: PgVec<'static, Oid> = PgVec::new_in(decode_mcx());
    for i in 0..nrelids {
        relids.push(u32_at(data, SizeOfHeapTruncate + i * 4));
    }

    let mut change = ReorderBufferChange::new(
        ReorderBufferChangeType::Truncate,
        ReorderBufferChangeData::Truncate {
            cascade: flags & XLH_TRUNCATE_CASCADE != 0,
            restart_seqs: flags & XLH_TRUNCATE_RESTART_SEQS != 0,
            relids,
        },
    );
    change.origin_id = origin;

    let xid = ctx.reader.XLogRecGetXid();
    ctx.reorder.queue_change(xid, buf.origptr, change, false)
}

fn DecodeMultiInsert(ctx: &mut LogicalDecodingContext, buf: XLogRecordBuffer) -> PgResult<()> {
    // xl_heap_multi_insert: flags@0, ntuples@2.
    let rec = ctx.reader.XLogRecGetData();
    let flags = rec[0];
    let ntuples = u16_at(rec, 2) as usize;

    if flags & XLH_INSERT_CONTAINS_NEW_TUPLE == 0 {
        return Ok(());
    }

    let rlocator = block0_locator(ctx);
    if rlocator.dbOid != ctx.slot.data.get().database {
        return Ok(());
    }

    let origin = ctx.reader.XLogRecGetOrigin();
    if FilterByOrigin(ctx, origin)? {
        return Ok(());
    }

    let xid = ctx.reader.XLogRecGetXid();
    let tuplelen = ctx
        .reader
        .XLogRecGetBlockData(0)
        .expect("multi-insert block data present")
        .len();

    let mut off = 0usize;
    for i in 0..ntuples {
        // xl_multi_insert_tuple entries are SHORTALIGNed relative to the
        // block-data start.
        off = (off + 1) & !1;
        let tupledata = ctx
            .reader
            .XLogRecGetBlockData(0)
            .expect("multi-insert block data present");
        let datalen = u16_at(tupledata, off) as usize;
        let t_infomask2 = u16_at(tupledata, off + 2);
        let t_infomask = u16_at(tupledata, off + 4);
        let t_hoff = tupledata[off + 6];
        let payload_start = off + SizeOfMultiInsertTuple;
        let payload_end = payload_start + datalen;

        let mut tuple = ctx.reorder.alloc_tuple_buf(datalen)?;
        {
            let tupledata = ctx
                .reader
                .XLogRecGetBlockData(0)
                .expect("multi-insert block data present");
            tuple.t_self = ItemPointerData::default();
            tuple.t_tableOid = InvalidOid;
            tuple.image_mut()[SizeofHeapTupleHeader..]
                .copy_from_slice(&tupledata[payload_start..payload_end]);
            let hdr = tuple.t_data_mut();
            hdr.t_infomask = t_infomask;
            hdr.t_infomask2 = t_infomask2;
            hdr.t_hoff = t_hoff;
        }

        let clear_toast_afterwards = flags & XLH_INSERT_LAST_IN_MULTI != 0 && (i + 1) == ntuples;

        let mut change = ReorderBufferChange::new(
            ReorderBufferChangeType::Insert,
            ReorderBufferChangeData::Tp {
                rlocator,
                clear_toast_afterwards,
                oldtuple: None,
                newtuple: Some(tuple),
            },
        );
        change.origin_id = origin;
        ctx.reorder.queue_change(xid, buf.origptr, change, false)?;

        off = payload_end;
    }
    debug_assert_eq!(off, tuplelen);
    Ok(())
}

fn DecodeSpecConfirm(ctx: &mut LogicalDecodingContext, buf: XLogRecordBuffer) -> PgResult<()> {
    let target_locator = block0_locator(ctx);
    if target_locator.dbOid != ctx.slot.data.get().database {
        return Ok(());
    }

    let origin = ctx.reader.XLogRecGetOrigin();
    if FilterByOrigin(ctx, origin)? {
        return Ok(());
    }

    let mut change = ReorderBufferChange::new(
        ReorderBufferChangeType::InternalSpecConfirm,
        ReorderBufferChangeData::Tp {
            rlocator: target_locator,
            clear_toast_afterwards: true,
            oldtuple: None,
            newtuple: None,
        },
    );
    change.origin_id = origin;

    let xid = ctx.reader.XLogRecGetXid();
    ctx.reorder.queue_change(xid, buf.origptr, change, false)
}

// `data` covers xl_heap_header followed by the tuple payload.
fn DecodeXLogTuple(data: &[u8], tuple: &mut heaptuple::HeapTuple<'static>) {
    let datalen = data.len() - SizeOfHeapHeader;
    debug_assert_eq!(tuple.t_len as usize, datalen + SizeofHeapTupleHeader);

    let t_infomask2 = u16_at(data, 0);
    let t_infomask = u16_at(data, 2);
    let t_hoff = data[4];

    tuple.t_self = ItemPointerData::default();
    tuple.t_tableOid = InvalidOid;
    tuple.image_mut()[SizeofHeapTupleHeader..].copy_from_slice(&data[SizeOfHeapHeader..]);
    let hdr = tuple.t_data_mut();
    hdr.t_infomask = t_infomask;
    hdr.t_infomask2 = t_infomask2;
    hdr.t_hoff = t_hoff;
}

fn DecodeTXNNeedSkip(
    ctx: &mut LogicalDecodingContext,
    buf: XLogRecordBuffer,
    txn_dbid: Oid,
    origin_id: RepOriginId,
) -> PgResult<bool> {
    if ctx.snapshot_builder.xact_needs_skip(buf.origptr)
        || (txn_dbid != InvalidOid && txn_dbid != ctx.slot.data.get().database)
        || FilterByOrigin(ctx, origin_id)?
    {
        return Ok(true);
    }

    if ctx.fast_forward {
        ctx.processing_required = true;
        return Ok(true);
    }

    Ok(false)
}

// C keeps this in logical.c; it lives here because it drives the record loop
// through LogicalDecodingProcessRecord (avoids a logical <-> logical_decode
// dependency cycle).
pub fn DecodingContextFindStartpoint(ctx: &mut LogicalDecodingContext) -> PgResult<()> {
    let slot = ctx.slot;

    ctx.reader.XLogBeginRead(slot.data.get().restart_lsn);
    let mut routine = LocalPageRead { wait_for_wal: true };

    loop {
        let record = ctx.reader.XLogReadRecord(&mut routine)?;
        if record.is_none() {
            return match ctx.reader.errormsg() {
                Some(err) => elog(
                    ERROR,
                    format!("could not find logical decoding starting point: {err}"),
                ),
                None => elog(ERROR, "could not find logical decoding starting point"),
            };
        }

        LogicalDecodingProcessRecord(ctx)?;

        if logical::DecodingContextReady(ctx) {
            break;
        }
    }

    let end = ctx.reader.v.EndRecPtr;
    slot.with_mutex(|| {
        let mut d = slot.data.get();
        d.confirmed_flush = end;
        if d.two_phase {
            d.two_phase_at = end;
        }
        slot.data.set(d);
    });
    Ok(())
}
