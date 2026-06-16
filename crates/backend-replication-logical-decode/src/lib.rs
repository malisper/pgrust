//! Port of `src/backend/replication/logical/decode.c` (PostgreSQL 18.3) — the
//! WAL → logical-decoding transformation.
//!
//! `decode.c` takes every `XLogReadRecord()`ed record and performs the actions
//! required to decode it: it dispatches by resource manager, tests the
//! per-record flags, reassembles tuples out of the WAL bytes, and feeds the
//! resulting changes to `reorderbuffer.c` and the catalog snapshot to
//! `snapbuild.c`. The control flow, branch order, switch arms, off-by-one
//! arithmetic, error messages, SQLSTATEs and log levels match the C exactly.
//!
//! # Model
//!
//! * The live `LogicalDecodingContext` is the single canonical
//!   [`types_logical::LogicalDecodingContext`] (unified with `logical.c`); the
//!   handlers receive `&mut ctx`. `decode.c` reads `ctx->snapshot_builder`,
//!   `ctx->reorder`, `ctx->slot->data.database` (the `slot_database` field),
//!   `ctx->fast_forward`, `ctx->twophase`, the two filter-callback presence
//!   flags, and writes `ctx->processing_required`.
//! * The `XLogReaderState *` is the xlogreader owner's registry reader behind an
//!   [`XLogReaderHandle`]; record fields are read through the handle-based
//!   accessor seams (`xlog_rec_get_*`), the WAL `xl_*` record bodies are decoded
//!   from `xlog_rec_get_main_data`/`xlog_rec_get_block_data` via
//!   `types_xlog_records`, and the C `char *` tuple byte-casts /
//!   `DecodeXLogTuple` `memcpy`s become an owned [`DecodedTuple`] built from
//!   those bytes.
//! * Every `ReorderBuffer*` / `SnapBuild*` call crosses the reorderbuffer /
//!   snapbuild owner's seams; the decoded change crosses as
//!   [`DecodedChangeKind`] + relation locator + the decoded old/new tuple
//!   images (not the owner-private `ReorderBufferChange`, which would form a
//!   crate cycle). The reorderbuffer change-replay family (#126) is not yet
//!   landed, so those seams panic loudly when an actual data-bearing record is
//!   decoded (mirror-PG-and-panic).
//!
//! C function inventory (20 functions, all IN-CRATE):
//!   `LogicalDecodingProcessRecord`, `xlog_decode`, `xact_decode`,
//!   `standby_decode`, `heap2_decode`, `heap_decode`, `logicalmsg_decode`,
//!   `FilterPrepare`, `FilterByOrigin`, `DecodeCommit`, `DecodePrepare`,
//!   `DecodeAbort`, `DecodeInsert`, `DecodeUpdate`, `DecodeDelete`,
//!   `DecodeTruncate`, `DecodeMultiInsert`, `DecodeSpecConfirm`,
//!   `DecodeXLogTuple`, `DecodeTXNNeedSkip`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

use types_core::primitive::{Oid, RepOriginId, TransactionId, XLogRecPtr};
use backend_utils_error::{ereport, PgError, PgResult};
use types_error::{ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR};
use types_logical::{LogicalDecodingContext, XLogReaderHandle};
use types_storage::sinval::{SharedInvalidationMessage, SharedInvalMessages};
use types_wal::rmgr::XLogRecordBuffer;
use types_wal::{XLOG_LOGICAL_MESSAGE, XLR_INFO_MASK};
use types_wal::rmgrdesc::{xl_logical_message, xl_parameter_change};
use types_wal::xact::{
    XACT_XINFO_HAS_ORIGIN, XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED, XLOG_XACT_ASSIGNMENT,
    XLOG_XACT_COMMIT, XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_INVALIDATIONS, XLOG_XACT_OPMASK,
    XLOG_XACT_PREPARE,
};

use types_xlog_records::heapam_xlog::{
    xl_heap_delete, xl_heap_insert, xl_heap_multi_insert, xl_heap_new_cid, xl_heap_truncate,
    xl_heap_update, xl_multi_insert_tuple, SizeOfHeapHeader,
    SizeOfHeapUpdate, SizeOfMultiInsertTuple, XLH_DELETE_CONTAINS_OLD_KEY,
    XLH_DELETE_CONTAINS_OLD_TUPLE, XLH_DELETE_IS_SUPER, XLH_INSERT_CONTAINS_NEW_TUPLE,
    XLH_INSERT_IS_SPECULATIVE, XLH_INSERT_LAST_IN_MULTI, XLH_INSERT_ON_TOAST_RELATION,
    XLH_UPDATE_CONTAINS_NEW_TUPLE, XLH_UPDATE_CONTAINS_OLD_KEY, XLH_UPDATE_CONTAINS_OLD_TUPLE,
};
use types_xlog_records::standbydefs::xl_running_xacts;

use backend_access_rmgrdesc_xactdesc::{
    parse_abort_record, parse_commit_record, parse_prepare_record, ParsedCommitAbort, ParsedPrepare,
};
use backend_access_transam_rmgr::GetRmgr;

use backend_replication_logical_decode_seams as decode_seam;
use backend_replication_logical_logical_seams as logical_seam;
use backend_replication_logical_reorderbuffer_seams as reorder;
use backend_replication_logical_reorderbuffer_seams::{DecodedChangeKind, DecodedTuple};
use backend_replication_logical_snapbuild_seams as snapbuild;
use backend_access_transam_xlogreader_seams as rt;

// ===========================================================================
// Constants the `types` crates do not expose (RM ids + RM_XLOG/RM_STANDBY info
// values), declared here exactly as the faithful crate did.
// ===========================================================================

// `XLOG_*` info values for the XLOG rmgr (`catalog/pg_control.h`).
pub const XLOG_CHECKPOINT_SHUTDOWN: u8 = 0x00;
pub const XLOG_CHECKPOINT_ONLINE: u8 = 0x10;
pub const XLOG_NOOP: u8 = 0x20;
pub const XLOG_NEXTOID: u8 = 0x30;
pub const XLOG_SWITCH: u8 = 0x40;
pub const XLOG_BACKUP_END: u8 = 0x50;
pub const XLOG_PARAMETER_CHANGE: u8 = 0x60;
pub const XLOG_RESTORE_POINT: u8 = 0x70;
pub const XLOG_FPW_CHANGE: u8 = 0x80;
pub const XLOG_END_OF_RECOVERY: u8 = 0x90;
pub const XLOG_FPI_FOR_HINT: u8 = 0xA0;
pub const XLOG_FPI: u8 = 0xB0;
pub const XLOG_OVERWRITE_CONTRECORD: u8 = 0xD0;
pub const XLOG_CHECKPOINT_REDO: u8 = 0xE0;

// `XLOG_*` info values for the STANDBY rmgr (`storage/standbydefs.h`).
pub const XLOG_STANDBY_LOCK: u8 = 0x00;
pub const XLOG_RUNNING_XACTS: u8 = 0x10;
pub const XLOG_INVALIDATIONS: u8 = 0x20;

// `XLOG_*` info values for the HEAP / HEAP2 rmgrs (`access/heapam_xlog.h`).
pub const XLOG_HEAP_INSERT: u8 = 0x00;
pub const XLOG_HEAP_DELETE: u8 = 0x10;
pub const XLOG_HEAP_UPDATE: u8 = 0x20;
pub const XLOG_HEAP_TRUNCATE: u8 = 0x30;
pub const XLOG_HEAP_HOT_UPDATE: u8 = 0x40;
pub const XLOG_HEAP_CONFIRM: u8 = 0x50;
pub const XLOG_HEAP_LOCK: u8 = 0x60;
pub const XLOG_HEAP_INPLACE: u8 = 0x70;
pub const XLOG_HEAP_OPMASK: u8 = 0x70;

pub const XLOG_HEAP2_REWRITE: u8 = 0x00;
pub const XLOG_HEAP2_PRUNE_ON_ACCESS: u8 = 0x10;
pub const XLOG_HEAP2_PRUNE_VACUUM_SCAN: u8 = 0x20;
pub const XLOG_HEAP2_PRUNE_VACUUM_CLEANUP: u8 = 0x30;
pub const XLOG_HEAP2_VISIBLE: u8 = 0x40;
pub const XLOG_HEAP2_MULTI_INSERT: u8 = 0x50;
pub const XLOG_HEAP2_LOCK_UPDATED: u8 = 0x60;
pub const XLOG_HEAP2_NEW_CID: u8 = 0x70;

// `XLH_*_CONTAINS_OLD` are the combined flag macros decode.c tests
// (`access/heapam_xlog.h`): the old tuple is present if either the full old
// tuple OR the old key was logged.
pub const XLH_UPDATE_CONTAINS_OLD: u8 = XLH_UPDATE_CONTAINS_OLD_TUPLE | XLH_UPDATE_CONTAINS_OLD_KEY;
pub const XLH_DELETE_CONTAINS_OLD: u8 = XLH_DELETE_CONTAINS_OLD_TUPLE | XLH_DELETE_CONTAINS_OLD_KEY;

// `XLH_TRUNCATE_*` flags (`access/heapam_xlog.h`) — the `flags` field of
// `xl_heap_truncate`.
pub const XLH_TRUNCATE_CASCADE: u8 = 1 << 0;
pub const XLH_TRUNCATE_RESTART_SEQS: u8 = 1 << 1;

// `SnapBuildState` thresholds (`replication/snapbuild.h`). The seam returns the
// `i32` state; these are the comparison constants.
pub const SNAPBUILD_FULL_SNAPSHOT: i32 = 1;
pub const SNAPBUILD_CONSISTENT: i32 = 2;

// `WAL_LEVEL_LOGICAL` (`access/xlog.h`).
pub const WAL_LEVEL_LOGICAL: i32 = 2;

/// `SizeofHeapTupleHeader` — the fixed on-disk `HeapTupleHeaderData` prefix
/// (`access/htup_details.h`), 23 bytes. `DecodeXLogTuple` lays a zeroed header
/// of this size in front of the WAL tuple bytes.
const SIZEOF_HEAP_TUPLE_HEADER: usize = 23;
/// Byte offsets within the fixed header that `DecodeXLogTuple` writes from the
/// `xl_heap_header`: `t_infomask2`@18, `t_infomask`@20, `t_hoff`@22.
const HEADER_T_INFOMASK2_OFF: usize = 18;
const HEADER_T_INFOMASK_OFF: usize = 20;
const HEADER_T_HOFF_OFF: usize = 22;

/// `InvalidXLogRecPtr` (`xlogdefs.h`).
const InvalidXLogRecPtr: XLogRecPtr = 0;
/// `InvalidTransactionId` (`access/transam.h`).
const InvalidTransactionId: TransactionId = 0;
/// `InvalidOid` (`postgres_ext.h`).
const InvalidOid: Oid = 0;

// ===========================================================================
// Inline helpers
// ===========================================================================

/// `TransactionIdIsValid(xid)` — `(xid) != InvalidTransactionId` (`transam.h`).
#[inline]
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `elog(ERROR, fmt, ...)` returning a `PgError`.
#[inline]
fn elog_error(msg: String) -> PgError {
    ereport(ERROR).errmsg_internal(msg).into_error()
}

/// `DecodeXLogTuple(data, len, tuple)` (decode.c:1235) — reassemble a
/// `ReorderBufferTupleBuf` (here a [`DecodedTuple`]) out of the WAL tuple bytes:
/// the leading `xl_heap_header` followed by `len - SizeOfHeapHeader` tuple body
/// bytes. The C builds a `HeapTupleHeaderData` of size `SizeofHeapTupleHeader`,
/// zeroes it, copies the body after it, and pokes `t_infomask2`/`t_infomask`/
/// `t_hoff` from the WAL header.
fn DecodeXLogTuple(data: &[u8]) -> DecodedTuple {
    let datalen = data.len() - SizeOfHeapHeader;
    // xlhdr = *(xl_heap_header *) data;  (t_infomask2@0, t_infomask@2, t_hoff@4)
    let t_infomask2 = u16::from_ne_bytes([data[0], data[1]]);
    let t_infomask = u16::from_ne_bytes([data[2], data[3]]);
    let t_hoff = data[4];

    // tuple->t_len = datalen + SizeofHeapTupleHeader;
    let t_len = (datalen + SIZEOF_HEAP_TUPLE_HEADER) as u32;

    // The owned image: a zeroed fixed header + the body bytes.
    let mut image = Vec::with_capacity(SIZEOF_HEAP_TUPLE_HEADER + datalen);
    image.resize(SIZEOF_HEAP_TUPLE_HEADER, 0u8);
    image.extend_from_slice(&data[SizeOfHeapHeader..SizeOfHeapHeader + datalen]);

    // header->t_infomask = xlhdr.t_infomask; header->t_infomask2 = ...; t_hoff.
    image[HEADER_T_INFOMASK2_OFF..HEADER_T_INFOMASK2_OFF + 2]
        .copy_from_slice(&t_infomask2.to_ne_bytes());
    image[HEADER_T_INFOMASK_OFF..HEADER_T_INFOMASK_OFF + 2]
        .copy_from_slice(&t_infomask.to_ne_bytes());
    image[HEADER_T_HOFF_OFF] = t_hoff;

    DecodedTuple {
        // ItemPointerSetInvalid(&tuple->t_self) + tuple->t_tableOid = InvalidOid.
        t_len,
        t_self: Default::default(),
        t_table_oid: InvalidOid,
        data: image,
    }
}

/// Collect the `subxacts` array a parsed commit/abort record carries into an
/// owned `Vec` (the C passes `parsed.subxacts` / `parsed.nsubxacts` straight
/// through to `SnapBuildCommitTxn` / the reorderbuffer loops).
fn commit_subxacts(data: &[u8], parsed: &ParsedCommitAbort) -> PgResult<Vec<TransactionId>> {
    let mut subxacts = Vec::with_capacity(parsed.nsubxacts.max(0) as usize);
    for i in 0..parsed.nsubxacts.max(0) as usize {
        subxacts.push(backend_access_rmgrdesc_xactdesc::subxact_at(
            data,
            parsed.subxacts_offset,
            i,
        )?);
    }
    Ok(subxacts)
}

/// Collect the `subxacts` array a parsed prepare record carries.
fn prepare_subxacts(data: &[u8], parsed: &ParsedPrepare) -> PgResult<Vec<TransactionId>> {
    let mut subxacts = Vec::with_capacity(parsed.nsubxacts.max(0) as usize);
    for i in 0..parsed.nsubxacts.max(0) as usize {
        subxacts.push(backend_access_rmgrdesc_xactdesc::subxact_at(
            data,
            parsed.subxacts_offset,
            i,
        )?);
    }
    Ok(subxacts)
}

// ===========================================================================
// decode.c body
// ===========================================================================

/// `LogicalDecodingProcessRecord` (decode.c:88).
pub fn LogicalDecodingProcessRecord(
    ctx: &mut LogicalDecodingContext,
    record: XLogReaderHandle,
) -> PgResult<()> {
    let buf = XLogRecordBuffer {
        origptr: rt::reader_ReadRecPtr::call(record),
        endptr: rt::reader_EndRecPtr::call(record),
        record,
    };

    let txid = rt::xlog_rec_get_top_xid::call(record);

    /*
     * If the top-level xid is valid, we need to assign the subxact to the
     * top-level xact. We need to do this for all records, hence we do it
     * before the switch.
     */
    if TransactionIdIsValid(txid) {
        reorder::ReorderBufferAssignChild::call(
            ctx.reorder,
            txid,
            rt::xlog_rec_get_xid::call(record),
            buf.origptr,
        );
    }

    let rmgr = GetRmgr(rt::xlog_rec_get_rmid::call(record))?;

    if let Some(rm_decode) = rmgr.rm_decode {
        let mut buf = buf;
        rm_decode(ctx, &mut buf)?;
    } else {
        /* just deal with xid, and done */
        reorder::ReorderBufferProcessXid::call(
            ctx.reorder,
            rt::xlog_rec_get_xid::call(record),
            buf.origptr,
        );
    }

    Ok(())
}

/// `xlog_decode` (decode.c:129).
pub fn xlog_decode(
    ctx: &mut LogicalDecodingContext,
    buf: &mut XLogRecordBuffer,
) -> PgResult<()> {
    let builder = ctx.snapshot_builder;
    let info = rt::xlog_rec_get_info::call(buf.record) & !XLR_INFO_MASK;

    reorder::ReorderBufferProcessXid::call(
        ctx.reorder,
        rt::xlog_rec_get_xid::call(buf.record),
        buf.origptr,
    );

    match info {
        /* this is also used in END_OF_RECOVERY checkpoints */
        XLOG_CHECKPOINT_SHUTDOWN | XLOG_END_OF_RECOVERY => {
            snapbuild::SnapBuildSerializationPoint::call(builder, buf.origptr)?;
        }
        XLOG_CHECKPOINT_ONLINE => {
            /*
             * a RUNNING_XACTS record will have been logged near to this, we
             * can restart from there.
             */
        }
        XLOG_PARAMETER_CHANGE => {
            let main_data = rt::xlog_rec_get_main_data::call(buf.record);
            let xlrec = xl_parameter_change::from_bytes(&main_data)
                .ok_or_else(|| elog_error(format!("invalid xl_parameter_change record")))?;

            /*
             * If wal_level on the primary is reduced to less than logical, we
             * want to prevent existing logical slots from being used.  Existing
             * logical slots on the standby get invalidated when this WAL record
             * is replayed; and further, slot creation fails when wal_level is
             * not sufficient; but all these operations are not synchronized, so
             * a logical slot may creep in while the wal_level is being reduced.
             * Hence this extra check.
             */
            if xlrec.wal_level() < WAL_LEVEL_LOGICAL {
                /*
                 * This can occur only on a standby, as a primary would not
                 * allow to restart after changing wal_level < logical if there
                 * is pre-existing logical slot.
                 */
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg("logical decoding on standby requires \"wal_level\" >= \"logical\" on the primary")
                    .into_error());
            }
        }
        XLOG_NOOP
        | XLOG_NEXTOID
        | XLOG_SWITCH
        | XLOG_BACKUP_END
        | XLOG_RESTORE_POINT
        | XLOG_FPW_CHANGE
        | XLOG_FPI_FOR_HINT
        | XLOG_FPI
        | XLOG_OVERWRITE_CONTRECORD
        | XLOG_CHECKPOINT_REDO => {}
        _ => {
            return Err(elog_error(format!("unexpected RM_XLOG_ID record type: {info}")));
        }
    }

    Ok(())
}

/// `xact_decode` (decode.c:201).
pub fn xact_decode(
    ctx: &mut LogicalDecodingContext,
    buf: &mut XLogRecordBuffer,
) -> PgResult<()> {
    let builder = ctx.snapshot_builder;
    let r = buf.record;
    let info = rt::xlog_rec_get_info::call(r) & XLOG_XACT_OPMASK;

    /*
     * If the snapshot isn't yet fully built, we cannot decode anything, so
     * bail out.
     */
    if snapbuild::SnapBuildCurrentState::call(builder) < SNAPBUILD_FULL_SNAPSHOT {
        return Ok(());
    }

    match info {
        XLOG_XACT_COMMIT | XLOG_XACT_COMMIT_PREPARED => {
            let mut two_phase = false;

            let info_byte = rt::xlog_rec_get_info::call(r);
            let data = rt::xlog_rec_get_main_data::call(r);
            let parsed = parse_commit_record(info_byte, &data)?;

            let xid = if !TransactionIdIsValid(parsed.twophase_xid) {
                rt::xlog_rec_get_xid::call(r)
            } else {
                parsed.twophase_xid
            };

            /*
             * We would like to process the transaction in a two-phase manner
             * iff output plugin supports two-phase commits and doesn't filter
             * the transaction at prepare time.
             */
            if info == XLOG_XACT_COMMIT_PREPARED {
                two_phase = !FilterPrepare(ctx, xid, parsed_commit_gid(&data, &parsed))?;
            }

            DecodeCommit(ctx, buf, &data, &parsed, xid, two_phase)?;
        }
        XLOG_XACT_ABORT | XLOG_XACT_ABORT_PREPARED => {
            let mut two_phase = false;

            let info_byte = rt::xlog_rec_get_info::call(r);
            let data = rt::xlog_rec_get_main_data::call(r);
            let parsed = parse_abort_record(info_byte, &data)?;

            let xid = if !TransactionIdIsValid(parsed.twophase_xid) {
                rt::xlog_rec_get_xid::call(r)
            } else {
                parsed.twophase_xid
            };

            if info == XLOG_XACT_ABORT_PREPARED {
                two_phase = !FilterPrepare(ctx, xid, parsed_commit_gid(&data, &parsed))?;
            }

            DecodeAbort(ctx, buf, &data, &parsed, xid, two_phase)?;
        }
        XLOG_XACT_ASSIGNMENT => {
            /*
             * We assign subxact to the toplevel xact while processing each
             * record if required.  So, we don't need to do anything here. See
             * LogicalDecodingProcessRecord.
             */
        }
        XLOG_XACT_INVALIDATIONS => {
            let xid = rt::xlog_rec_get_xid::call(r);
            let data = rt::xlog_rec_get_main_data::call(r);
            let invals = decode_xact_invals(&data)?;

            /*
             * Execute the invalidations for xid-less transactions, otherwise,
             * accumulate them so that they can be processed at the commit time.
             */
            if TransactionIdIsValid(xid) {
                if !ctx.fast_forward {
                    reorder::ReorderBufferAddInvalidations::call(
                        ctx.reorder,
                        xid,
                        buf.origptr,
                        invals,
                    );
                }
                reorder::ReorderBufferXidSetCatalogChanges::call(ctx.reorder, xid, buf.origptr);
            } else if !ctx.fast_forward {
                reorder::ReorderBufferImmediateInvalidation::call(ctx.reorder, invals);
            }
        }
        XLOG_XACT_PREPARE => {
            /* ok, parse it */
            let data = rt::xlog_rec_get_main_data::call(r);
            let parsed = parse_prepare_record(&data)?;

            /*
             * We would like to process the transaction in a two-phase manner
             * iff output plugin supports two-phase commits and doesn't filter
             * the transaction at prepare time.
             */
            if FilterPrepare(ctx, parsed.twophase_xid, parsed.twophase_gid().as_bytes())? {
                reorder::ReorderBufferProcessXid::call(
                    ctx.reorder,
                    parsed.twophase_xid,
                    buf.origptr,
                );
                return Ok(());
            }

            /*
             * Note that if the prepared transaction has locked [user] catalog
             * tables exclusively then decoding prepare can block till the main
             * transaction is committed because it needs to lock the catalog
             * tables.
             */
            DecodePrepare(ctx, buf, &data, &parsed)?;
        }
        _ => {
            return Err(elog_error(format!("unexpected RM_XACT_ID record type: {info}")));
        }
    }

    Ok(())
}

/// `standby_decode` (decode.c:359).
pub fn standby_decode(
    ctx: &mut LogicalDecodingContext,
    buf: &mut XLogRecordBuffer,
) -> PgResult<()> {
    let builder = ctx.snapshot_builder;
    let r = buf.record;
    let info = rt::xlog_rec_get_info::call(r) & !XLR_INFO_MASK;

    reorder::ReorderBufferProcessXid::call(ctx.reorder, rt::xlog_rec_get_xid::call(r), buf.origptr);

    match info {
        XLOG_RUNNING_XACTS => {
            let data = rt::xlog_rec_get_main_data::call(r);
            let xlrec = xl_running_xacts::from_bytes(&data);
            let oldest_running_xid = xlrec.oldestRunningXid;

            let running_xids = running_xacts_xids(&data, &xlrec);
            snapbuild::SnapBuildProcessRunningXacts::call(
                builder,
                buf.origptr,
                xlrec,
                running_xids,
            )?;

            /*
             * Abort all transactions that we keep track of, that are older than
             * the record's oldestRunningXid. This is the most convenient spot
             * for doing so since, in contrast to shutdown or end-of-recovery
             * checkpoints, we have information about all running transactions
             * which includes prepared ones, while shutdown checkpoints just
             * know that no non-prepared transactions are in progress.
             */
            reorder::ReorderBufferAbortOld::call(ctx.reorder, oldest_running_xid);
        }
        XLOG_STANDBY_LOCK => {}
        XLOG_INVALIDATIONS => {
            /*
             * We are processing the invalidations at the command level via
             * XLOG_XACT_INVALIDATIONS.  So we don't need to do anything here.
             */
        }
        _ => {
            return Err(elog_error(format!("unexpected RM_STANDBY_ID record type: {info}")));
        }
    }

    Ok(())
}

/// `heap2_decode` (decode.c:405).
pub fn heap2_decode(
    ctx: &mut LogicalDecodingContext,
    buf: &mut XLogRecordBuffer,
) -> PgResult<()> {
    let info = rt::xlog_rec_get_info::call(buf.record) & XLOG_HEAP_OPMASK;
    let xid = rt::xlog_rec_get_xid::call(buf.record);
    let builder = ctx.snapshot_builder;

    reorder::ReorderBufferProcessXid::call(ctx.reorder, xid, buf.origptr);

    /*
     * If we don't have snapshot or we are just fast-forwarding, there is no
     * point in decoding data changes.
     */
    if snapbuild::SnapBuildCurrentState::call(builder) < SNAPBUILD_FULL_SNAPSHOT {
        return Ok(());
    }

    match info {
        XLOG_HEAP2_MULTI_INSERT => {
            if snapbuild::SnapBuildProcessChange::call(builder, xid, buf.origptr)
                && !ctx.fast_forward
            {
                DecodeMultiInsert(ctx, buf)?;
            }
        }
        XLOG_HEAP2_NEW_CID => {
            /*
             * C `case XLOG_HEAP2_NEW_CID:` only does work inside the
             * `if (!ctx->fast_forward)` block; when `fast_forward` is set the
             * case falls through to `XLOG_HEAP2_REWRITE` (which does nothing).
             */
            if !ctx.fast_forward {
                let data = rt::xlog_rec_get_main_data::call(buf.record);
                let xlrec = xl_heap_new_cid::from_bytes(&data);
                snapbuild::SnapBuildProcessNewCid::call(builder, xid, buf.origptr, xlrec)?;
            }
            /* else: fall through to XLOG_HEAP2_REWRITE — no work */
        }
        XLOG_HEAP2_REWRITE => {
            /*
             * Although these records only exist to serve the needs of logical
             * decoding, all the work happens as part of crash or archive
             * recovery, so we don't need to do anything here.
             */
        }

        /*
         * Everything else here is just low level physical stuff we're not
         * interested in.
         */
        XLOG_HEAP2_PRUNE_ON_ACCESS
        | XLOG_HEAP2_PRUNE_VACUUM_SCAN
        | XLOG_HEAP2_PRUNE_VACUUM_CLEANUP
        | XLOG_HEAP2_VISIBLE
        | XLOG_HEAP2_LOCK_UPDATED => {}
        _ => {
            return Err(elog_error(format!("unexpected RM_HEAP2_ID record type: {info}")));
        }
    }

    Ok(())
}

/// `heap_decode` (decode.c:469).
pub fn heap_decode(
    ctx: &mut LogicalDecodingContext,
    buf: &mut XLogRecordBuffer,
) -> PgResult<()> {
    let info = rt::xlog_rec_get_info::call(buf.record) & XLOG_HEAP_OPMASK;
    let xid = rt::xlog_rec_get_xid::call(buf.record);
    let builder = ctx.snapshot_builder;

    reorder::ReorderBufferProcessXid::call(ctx.reorder, xid, buf.origptr);

    if snapbuild::SnapBuildCurrentState::call(builder) < SNAPBUILD_FULL_SNAPSHOT {
        return Ok(());
    }

    match info {
        XLOG_HEAP_INSERT => {
            if snapbuild::SnapBuildProcessChange::call(builder, xid, buf.origptr)
                && !ctx.fast_forward
            {
                DecodeInsert(ctx, buf)?;
            }
        }
        /*
         * Treat HOT update as normal updates. There is no useful information in
         * the fact that we could make it a HOT update locally and the WAL layout
         * is compatible.
         */
        XLOG_HEAP_HOT_UPDATE | XLOG_HEAP_UPDATE => {
            if snapbuild::SnapBuildProcessChange::call(builder, xid, buf.origptr)
                && !ctx.fast_forward
            {
                DecodeUpdate(ctx, buf)?;
            }
        }
        XLOG_HEAP_DELETE => {
            if snapbuild::SnapBuildProcessChange::call(builder, xid, buf.origptr)
                && !ctx.fast_forward
            {
                DecodeDelete(ctx, buf)?;
            }
        }
        XLOG_HEAP_TRUNCATE => {
            if snapbuild::SnapBuildProcessChange::call(builder, xid, buf.origptr)
                && !ctx.fast_forward
            {
                DecodeTruncate(ctx, buf)?;
            }
        }
        XLOG_HEAP_INPLACE => {
            /*
             * Inplace updates are only ever performed on catalog tuples and can,
             * per definition, not change tuple visibility.  Since we also don't
             * decode catalog tuples, we're not interested in the record's
             * contents.
             */
        }
        XLOG_HEAP_CONFIRM => {
            if snapbuild::SnapBuildProcessChange::call(builder, xid, buf.origptr)
                && !ctx.fast_forward
            {
                DecodeSpecConfirm(ctx, buf)?;
            }
        }
        XLOG_HEAP_LOCK => { /* we don't care about row level locks for now */ }
        _ => {
            return Err(elog_error(format!("unexpected RM_HEAP_ID record type: {info}")));
        }
    }

    Ok(())
}

/// `FilterPrepare` (decode.c:550).
fn FilterPrepare(
    ctx: &mut LogicalDecodingContext,
    xid: TransactionId,
    gid: &[u8],
) -> PgResult<bool> {
    /*
     * Skip if decoding of two-phase transactions at PREPARE time is not
     * enabled. In that case, all two-phase transactions are considered filtered
     * out and will be applied as regular transactions at COMMIT PREPARED.
     */
    if !ctx.twophase {
        return Ok(true);
    }

    /*
     * The filter_prepare callback is optional. When not supplied, all prepared
     * transactions should go through.
     */
    if !ctx.callbacks.filter_prepare_cb {
        return Ok(false);
    }

    logical_seam::filter_prepare_cb_wrapper::call(ctx, xid, gid.to_vec())
}

/// `FilterByOrigin` (decode.c:573).
fn FilterByOrigin(ctx: &mut LogicalDecodingContext, origin_id: RepOriginId) -> PgResult<bool> {
    if !ctx.callbacks.filter_by_origin_cb {
        return Ok(false);
    }

    logical_seam::filter_by_origin_cb_wrapper::call(ctx, origin_id)
}

/// `logicalmsg_decode` (decode.c:586).
pub fn logicalmsg_decode(
    ctx: &mut LogicalDecodingContext,
    buf: &mut XLogRecordBuffer,
) -> PgResult<()> {
    let builder = ctx.snapshot_builder;
    let r = buf.record;
    let xid = rt::xlog_rec_get_xid::call(r);
    let info = rt::xlog_rec_get_info::call(r) & !XLR_INFO_MASK;
    let origin_id = rt::xlog_rec_get_origin::call(r);

    if info != XLOG_LOGICAL_MESSAGE {
        return Err(elog_error(format!("unexpected RM_LOGICALMSG_ID record type: {info}")));
    }

    reorder::ReorderBufferProcessXid::call(ctx.reorder, rt::xlog_rec_get_xid::call(r), buf.origptr);

    /* If we don't have snapshot, there is no point in decoding messages */
    if snapbuild::SnapBuildCurrentState::call(builder) < SNAPBUILD_FULL_SNAPSHOT {
        return Ok(());
    }

    let main_data = rt::xlog_rec_get_main_data::call(r);
    let message = xl_logical_message::from_bytes(&main_data)
        .ok_or_else(|| elog_error(format!("invalid xl_logical_message record")))?;

    if message.db_id() != ctx.slot_database || FilterByOrigin(ctx, origin_id)? {
        return Ok(());
    }

    let transactional = message.transactional();
    if transactional && !snapbuild::SnapBuildProcessChange::call(builder, xid, buf.origptr) {
        return Ok(());
    } else if !transactional
        && (snapbuild::SnapBuildCurrentState::call(builder) != SNAPBUILD_CONSISTENT
            || snapbuild::SnapBuildXactNeedsSkip::call(builder, buf.origptr))
    {
        return Ok(());
    }

    /*
     * We also skip decoding in fast_forward mode. This check must be last
     * because we don't want to set the processing_required flag unless we have
     * a decodable message.
     */
    if ctx.fast_forward {
        /*
         * We need to set processing_required flag to notify the message's
         * existence to the caller. Usually, the flag is set when either the
         * COMMIT or ABORT records are decoded, but this must be turned on here
         * because the non-transactional logical message is decoded without
         * waiting for these records.
         */
        if !transactional {
            ctx.processing_required = true;
        }

        return Ok(());
    }

    /*
     * If this is a non-transactional change, get the snapshot we're expected to
     * use. We only get here when the snapshot is consistent, and the change is
     * not meant to be skipped.
     *
     * For transactional changes we'll use the regular snapshot maintained by
     * ReorderBuffer; the snapshot is built by SnapBuildGetOrBuildSnapshot for
     * the non-transactional case. The reorderbuffer owner takes the snapshot
     * presence; the change-replay family (#126) is unported, so the queue call
     * panics loudly below.
     */
    if !transactional {
        let _snap = snapbuild::SnapBuildGetOrBuildSnapshot::call(builder);
    }

    /*
     * `message->message` is the prefix (length `prefix_size`, NUL-terminated)
     * followed by the body (length `message_size`). The reorderbuffer seam
     * takes the prefix and body as owned byte vectors.
     */
    let prefix = message.prefix().to_vec();
    let body = message.payload().to_vec();

    reorder::ReorderBufferQueueMessage::call(
        ctx.reorder,
        xid,
        buf.endptr,
        transactional,
        prefix,
        body,
    );

    Ok(())
}

/// `DecodeCommit` (decode.c:666).
fn DecodeCommit(
    ctx: &mut LogicalDecodingContext,
    buf: &mut XLogRecordBuffer,
    data: &[u8],
    parsed: &ParsedCommitAbort,
    xid: TransactionId,
    two_phase: bool,
) -> PgResult<()> {
    let mut origin_lsn = InvalidXLogRecPtr;
    let mut commit_time = parsed.xact_time;
    let origin_id = rt::xlog_rec_get_origin::call(buf.record);

    if parsed.xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        origin_lsn = parsed.origin_lsn;
        commit_time = parsed.origin_timestamp;
    }

    let subxacts = commit_subxacts(data, parsed)?;

    snapbuild::SnapBuildCommitTxn::call(
        ctx.snapshot_builder,
        buf.origptr,
        xid,
        subxacts.clone(),
        parsed.xinfo,
    );

    /* ----
     * Check whether we are interested in this specific transaction, and tell
     * the reorderbuffer to forget the content of the (sub-)transactions if not.
     * ---
     */
    if DecodeTXNNeedSkip(ctx, buf, parsed.db_id, origin_id)? {
        for &sub in &subxacts {
            reorder::ReorderBufferForget::call(ctx.reorder, sub, buf.origptr);
        }
        reorder::ReorderBufferForget::call(ctx.reorder, xid, buf.origptr);

        return Ok(());
    }

    /* tell the reorderbuffer about the surviving subtransactions */
    for &sub in &subxacts {
        reorder::ReorderBufferCommitChild::call(ctx.reorder, xid, sub, buf.origptr, buf.endptr);
    }

    /*
     * Send the final commit record if the transaction data is already decoded,
     * otherwise, process the entire transaction.
     */
    if two_phase {
        let two_phase_at = snapbuild::SnapBuildGetTwoPhaseAt::call(ctx.snapshot_builder);
        reorder::ReorderBufferFinishPrepared::call(
            ctx.reorder,
            xid,
            buf.origptr,
            buf.endptr,
            two_phase_at,
            commit_time,
            origin_id,
            origin_lsn,
            commit_gid_bytes(data, parsed),
            true,
        );
    } else {
        reorder::ReorderBufferCommit::call(
            ctx.reorder,
            xid,
            buf.origptr,
            buf.endptr,
            commit_time,
            origin_id,
            origin_lsn,
        );
    }

    /*
     * Update the decoding stats at transaction prepare/commit/abort.
     */
    logical_seam::UpdateDecodingStats::call(ctx)?;

    Ok(())
}

/// `DecodePrepare` (decode.c:762).
fn DecodePrepare(
    ctx: &mut LogicalDecodingContext,
    buf: &mut XLogRecordBuffer,
    data: &[u8],
    parsed: &ParsedPrepare,
) -> PgResult<()> {
    let builder = ctx.snapshot_builder;
    let origin_lsn = parsed.origin_lsn;
    let mut prepare_time = parsed.xact_time;
    let origin_id = rt::xlog_rec_get_origin::call(buf.record);
    let xid = parsed.twophase_xid;

    if parsed.origin_timestamp != 0 {
        prepare_time = parsed.origin_timestamp;
    }

    /*
     * Remember the prepare info for a txn so that it can be used later in commit
     * prepared if required. See ReorderBufferFinishPrepared.
     */
    if !reorder::ReorderBufferRememberPrepareInfo::call(
        ctx.reorder,
        xid,
        buf.origptr,
        buf.endptr,
        prepare_time,
        origin_id,
        origin_lsn,
    ) {
        return Ok(());
    }

    /* We can't start streaming unless a consistent state is reached. */
    if snapbuild::SnapBuildCurrentState::call(builder) < SNAPBUILD_CONSISTENT {
        reorder::ReorderBufferSkipPrepare::call(ctx.reorder, xid);
        return Ok(());
    }

    /*
     * Check whether we need to process this transaction. See DecodeTXNNeedSkip
     * for the reasons why we sometimes want to skip the transaction.
     */
    if DecodeTXNNeedSkip(ctx, buf, parsed.db_id, origin_id)? {
        reorder::ReorderBufferSkipPrepare::call(ctx.reorder, xid);
        reorder::ReorderBufferInvalidate::call(ctx.reorder, xid, buf.origptr);
        return Ok(());
    }

    /* Tell the reorderbuffer about the surviving subtransactions. */
    let subxacts = prepare_subxacts(data, parsed)?;
    for &sub in &subxacts {
        reorder::ReorderBufferCommitChild::call(ctx.reorder, xid, sub, buf.origptr, buf.endptr);
    }

    /* replay actions of all transaction + subtransactions in order */
    reorder::ReorderBufferPrepare::call(ctx.reorder, xid, parsed.twophase_gid().as_bytes().to_vec());

    logical_seam::UpdateDecodingStats::call(ctx)?;

    Ok(())
}

/// `DecodeAbort` (decode.c:838).
fn DecodeAbort(
    ctx: &mut LogicalDecodingContext,
    buf: &mut XLogRecordBuffer,
    data: &[u8],
    parsed: &ParsedCommitAbort,
    xid: TransactionId,
    two_phase: bool,
) -> PgResult<()> {
    let mut origin_lsn = InvalidXLogRecPtr;
    let mut abort_time = parsed.xact_time;
    let origin_id = rt::xlog_rec_get_origin::call(buf.record);

    if parsed.xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        origin_lsn = parsed.origin_lsn;
        abort_time = parsed.origin_timestamp;
    }

    /*
     * Check whether we need to process this transaction. See DecodeTXNNeedSkip
     * for the reasons why we sometimes want to skip the transaction.
     */
    let skip_xact = DecodeTXNNeedSkip(ctx, buf, parsed.db_id, origin_id)?;

    /*
     * Send the final rollback record for a prepared transaction unless we need
     * to skip it. For non-two-phase xacts, simply forget the xact.
     */
    if two_phase && !skip_xact {
        reorder::ReorderBufferFinishPrepared::call(
            ctx.reorder,
            xid,
            buf.origptr,
            buf.endptr,
            InvalidXLogRecPtr,
            abort_time,
            origin_id,
            origin_lsn,
            commit_gid_bytes(data, parsed),
            false,
        );
    } else {
        let end_rec_ptr = rt::reader_EndRecPtr::call(buf.record);
        let subxacts = commit_subxacts(data, parsed)?;
        for &sub in &subxacts {
            reorder::ReorderBufferAbort::call(ctx.reorder, sub, end_rec_ptr, abort_time);
        }

        reorder::ReorderBufferAbort::call(ctx.reorder, xid, end_rec_ptr, abort_time);
    }

    /* update the decoding stats */
    logical_seam::UpdateDecodingStats::call(ctx)?;

    Ok(())
}

/// `DecodeInsert` (decode.c:893).
fn DecodeInsert(
    ctx: &mut LogicalDecodingContext,
    buf: &mut XLogRecordBuffer,
) -> PgResult<()> {
    let r = buf.record;

    let main_data = rt::xlog_rec_get_main_data::call(r);
    let xlrec = xl_heap_insert::from_bytes(&main_data);

    /*
     * Ignore insert records without new tuples (this does happen when
     * raw_heap_insert marks the TOAST record as HEAP_INSERT_NO_LOGICAL).
     */
    if xlrec.flags & XLH_INSERT_CONTAINS_NEW_TUPLE == 0 {
        return Ok(());
    }

    /* only interested in our database */
    let target_locator = match rt::xlog_rec_get_block_tag::call(r, 0) {
        Some(loc) => loc,
        None => return Ok(()),
    };
    if target_locator.dbOid != ctx.slot_database {
        return Ok(());
    }

    /* output plugin doesn't look for this origin, no need to queue */
    if FilterByOrigin(ctx, rt::xlog_rec_get_origin::call(r))? {
        return Ok(());
    }

    let action = if xlrec.flags & XLH_INSERT_IS_SPECULATIVE == 0 {
        DecodedChangeKind::Insert
    } else {
        DecodedChangeKind::SpecInsert
    };

    /* DecodeXLogTuple(tupledata, datalen, change->data.tp.newtuple) */
    let tupledata = rt::xlog_rec_get_block_data::call(r, 0)
        .ok_or_else(|| elog_error(format!("DecodeInsert: no block 0 data")))?;
    let newtuple = DecodeXLogTuple(&tupledata);

    reorder::ReorderBufferQueueChange::call(
        ctx.reorder,
        rt::xlog_rec_get_xid::call(r),
        buf.origptr,
        action,
        target_locator,
        None,
        Some(newtuple),
        xlrec.flags & XLH_INSERT_ON_TOAST_RELATION != 0,
    );

    Ok(())
}

/// `DecodeUpdate` (decode.c:952).
fn DecodeUpdate(
    ctx: &mut LogicalDecodingContext,
    buf: &mut XLogRecordBuffer,
) -> PgResult<()> {
    let r = buf.record;

    let main_data = rt::xlog_rec_get_main_data::call(r);
    let xlrec = xl_heap_update::from_bytes(&main_data);

    /* only interested in our database */
    let target_locator = match rt::xlog_rec_get_block_tag::call(r, 0) {
        Some(loc) => loc,
        None => return Ok(()),
    };
    if target_locator.dbOid != ctx.slot_database {
        return Ok(());
    }

    /* output plugin doesn't look for this origin, no need to queue */
    if FilterByOrigin(ctx, rt::xlog_rec_get_origin::call(r))? {
        return Ok(());
    }

    let mut newtuple = None;
    let mut oldtuple = None;

    if xlrec.flags & XLH_UPDATE_CONTAINS_NEW_TUPLE != 0 {
        /* DecodeXLogTuple(XLogRecGetBlockData(r, 0, &datalen), ...) */
        let data = rt::xlog_rec_get_block_data::call(r, 0)
            .ok_or_else(|| elog_error(format!("DecodeUpdate: no block 0 data")))?;
        newtuple = Some(DecodeXLogTuple(&data));
    }

    if xlrec.flags & XLH_UPDATE_CONTAINS_OLD != 0 {
        /* caution, remaining data in record is not aligned */
        /* DecodeXLogTuple(XLogRecGetData(r) + SizeOfHeapUpdate, ...) */
        oldtuple = Some(DecodeXLogTuple(&main_data[SizeOfHeapUpdate..]));
    }

    reorder::ReorderBufferQueueChange::call(
        ctx.reorder,
        rt::xlog_rec_get_xid::call(r),
        buf.origptr,
        DecodedChangeKind::Update,
        target_locator,
        oldtuple,
        newtuple,
        false,
    );

    Ok(())
}

/// `DecodeDelete` (decode.c:1019).
fn DecodeDelete(
    ctx: &mut LogicalDecodingContext,
    buf: &mut XLogRecordBuffer,
) -> PgResult<()> {
    let r = buf.record;

    let main_data = rt::xlog_rec_get_main_data::call(r);
    let xlrec = xl_heap_delete::from_bytes(&main_data);

    /* only interested in our database */
    let target_locator = match rt::xlog_rec_get_block_tag::call(r, 0) {
        Some(loc) => loc,
        None => return Ok(()),
    };
    if target_locator.dbOid != ctx.slot_database {
        return Ok(());
    }

    /* output plugin doesn't look for this origin, no need to queue */
    if FilterByOrigin(ctx, rt::xlog_rec_get_origin::call(r))? {
        return Ok(());
    }

    let action = if xlrec.flags & XLH_DELETE_IS_SUPER != 0 {
        DecodedChangeKind::SpecAbort
    } else {
        DecodedChangeKind::Delete
    };

    /* old primary key stored */
    let oldtuple = if xlrec.flags & XLH_DELETE_CONTAINS_OLD != 0 {
        /* DecodeXLogTuple((char *) xlrec + SizeOfHeapDelete, datalen, oldtuple) */
        let data = rt::xlog_rec_get_block_data::call(r, 0)
            .ok_or_else(|| elog_error(format!("DecodeDelete: no block 0 data")))?;
        Some(DecodeXLogTuple(&data))
    } else {
        None
    };

    reorder::ReorderBufferQueueChange::call(
        ctx.reorder,
        rt::xlog_rec_get_xid::call(r),
        buf.origptr,
        action,
        target_locator,
        oldtuple,
        None,
        false,
    );

    Ok(())
}

/// `DecodeTruncate` (decode.c:1073).
fn DecodeTruncate(
    ctx: &mut LogicalDecodingContext,
    buf: &mut XLogRecordBuffer,
) -> PgResult<()> {
    let r = buf.record;

    let main_data = rt::xlog_rec_get_main_data::call(r);
    let xlrec = xl_heap_truncate::from_bytes(&main_data);

    /* only interested in our database */
    if xlrec.dbId != ctx.slot_database {
        return Ok(());
    }

    /* output plugin doesn't look for this origin, no need to queue */
    if FilterByOrigin(ctx, rt::xlog_rec_get_origin::call(r))? {
        return Ok(());
    }

    /*
     * change->data.truncate.nrelids = xlrec->nrelids; relids copied from the
     * record's trailing `relids` array.
     */
    let relid_arr = xl_heap_truncate::relids(&main_data);
    let mut relids: Vec<Oid> = Vec::with_capacity(xlrec.nrelids as usize);
    for i in 0..xlrec.nrelids as usize {
        relids.push(relid_arr.get(i));
    }
    let cascade = xlrec.flags & XLH_TRUNCATE_CASCADE != 0;
    let restart_seqs = xlrec.flags & XLH_TRUNCATE_RESTART_SEQS != 0;

    reorder::ReorderBufferQueueTruncate::call(
        ctx.reorder,
        rt::xlog_rec_get_xid::call(r),
        buf.origptr,
        cascade,
        restart_seqs,
        relids,
    );

    Ok(())
}

/// `DecodeMultiInsert` (decode.c:1111).
fn DecodeMultiInsert(
    ctx: &mut LogicalDecodingContext,
    buf: &mut XLogRecordBuffer,
) -> PgResult<()> {
    let r = buf.record;

    let main_data = rt::xlog_rec_get_main_data::call(r);
    let xlrec = xl_heap_multi_insert::from_bytes(&main_data);

    /*
     * Ignore insert records without new tuples.  This happens when a
     * multi_insert is done on a catalog or on a non-persistent relation.
     */
    if xlrec.flags & XLH_INSERT_CONTAINS_NEW_TUPLE == 0 {
        return Ok(());
    }

    /* only interested in our database */
    let rlocator = match rt::xlog_rec_get_block_tag::call(r, 0) {
        Some(loc) => loc,
        None => return Ok(()),
    };
    if rlocator.dbOid != ctx.slot_database {
        return Ok(());
    }

    /* output plugin doesn't look for this origin, no need to queue */
    if FilterByOrigin(ctx, rt::xlog_rec_get_origin::call(r))? {
        return Ok(());
    }

    /*
     * We know that this multi_insert isn't for a catalog, so the block should
     * always have data even if a full-page write of it is taken. `data` is the
     * block-0 data: a sequence of SHORTALIGN'd `xl_multi_insert_tuple` headers
     * each followed by `datalen` tuple bytes. The C `data` walk:
     *   tupledata = XLogRecGetBlockData(r, 0, &tuplelen);
     *   data = tupledata; ... for each i:
     *     xlhdr = (xl_multi_insert_tuple *) SHORTALIGN(data);
     *     data = ((char *) xlhdr) + SizeOfMultiInsertTuple;
     *     ... build tuple from (data, xlhdr->datalen); data += xlhdr->datalen;
     */
    let block_data = rt::xlog_rec_get_block_data::call(r, 0)
        .ok_or_else(|| elog_error(format!("DecodeMultiInsert: no block 0 data")))?;

    let origin_id = rt::xlog_rec_get_origin::call(r);
    let xid = rt::xlog_rec_get_xid::call(r);
    let _ = origin_id;

    let mut offset: usize = 0;
    let mut i: usize = 0;
    while i < xlrec.ntuples as usize {
        /* xlhdr = (xl_multi_insert_tuple *) SHORTALIGN(data); */
        offset = shortalign(offset);
        let xlhdr = xl_multi_insert_tuple::from_bytes(&block_data[offset..]);
        offset += SizeOfMultiInsertTuple;

        let datalen = xlhdr.datalen as usize;

        /*
         * Build the tuple image directly: a zeroed fixed header + the body
         * bytes, with t_infomask2/t_infomask/t_hoff poked from `xlhdr` (the C
         * `ReorderBufferAllocTupleBuf` + memset + memcpy + header field sets).
         */
        let body = &block_data[offset..offset + datalen];
        offset += datalen;

        let mut image = Vec::with_capacity(SIZEOF_HEAP_TUPLE_HEADER + datalen);
        image.resize(SIZEOF_HEAP_TUPLE_HEADER, 0u8);
        image.extend_from_slice(body);
        image[HEADER_T_INFOMASK2_OFF..HEADER_T_INFOMASK2_OFF + 2]
            .copy_from_slice(&xlhdr.t_infomask2.to_ne_bytes());
        image[HEADER_T_INFOMASK_OFF..HEADER_T_INFOMASK_OFF + 2]
            .copy_from_slice(&xlhdr.t_infomask.to_ne_bytes());
        image[HEADER_T_HOFF_OFF] = xlhdr.t_hoff;

        let tuple = DecodedTuple {
            t_len: (datalen + SIZEOF_HEAP_TUPLE_HEADER) as u32,
            t_self: Default::default(),
            t_table_oid: InvalidOid,
            data: image,
        };

        /*
         * Reset toast reassembly state only after the last row in the last
         * xl_multi_insert_tuple record emitted by one heap_multi_insert() call.
         */
        let _clear_toast_afterwards =
            xlrec.flags & XLH_INSERT_LAST_IN_MULTI != 0 && (i + 1) == xlrec.ntuples as usize;

        reorder::ReorderBufferQueueChange::call(
            ctx.reorder,
            xid,
            buf.origptr,
            DecodedChangeKind::Insert,
            rlocator,
            None,
            Some(tuple),
            false,
        );

        i += 1;
    }

    Ok(())
}

/// `DecodeSpecConfirm` (decode.c:1216).
fn DecodeSpecConfirm(
    ctx: &mut LogicalDecodingContext,
    buf: &mut XLogRecordBuffer,
) -> PgResult<()> {
    let r = buf.record;

    /* only interested in our database */
    let target_locator = match rt::xlog_rec_get_block_tag::call(r, 0) {
        Some(loc) => loc,
        None => return Ok(()),
    };
    if target_locator.dbOid != ctx.slot_database {
        return Ok(());
    }

    /* output plugin doesn't look for this origin, no need to queue */
    if FilterByOrigin(ctx, rt::xlog_rec_get_origin::call(r))? {
        return Ok(());
    }

    reorder::ReorderBufferQueueChange::call(
        ctx.reorder,
        rt::xlog_rec_get_xid::call(r),
        buf.origptr,
        DecodedChangeKind::SpecConfirm,
        target_locator,
        None,
        None,
        false,
    );

    Ok(())
}

/// `DecodeTXNNeedSkip` (decode.c:1295).
fn DecodeTXNNeedSkip(
    ctx: &mut LogicalDecodingContext,
    buf: &mut XLogRecordBuffer,
    txn_dbid: Oid,
    origin_id: RepOriginId,
) -> PgResult<bool> {
    if snapbuild::SnapBuildXactNeedsSkip::call(ctx.snapshot_builder, buf.origptr)
        || (txn_dbid != InvalidOid && txn_dbid != ctx.slot_database)
        || FilterByOrigin(ctx, origin_id)?
    {
        return Ok(true);
    }

    /*
     * We also skip decoding in fast_forward mode. In passing set the
     * processing_required flag to indicate that if it were not for fast_forward
     * mode, processing would have been required.
     */
    if ctx.fast_forward {
        ctx.processing_required = true;
        return Ok(true);
    }

    Ok(false)
}

// ===========================================================================
// Local byte helpers
// ===========================================================================

/// `SHORTALIGN(x)` (`c.h`) — round `x` up to the next 2-byte boundary.
#[inline]
fn shortalign(x: usize) -> usize {
    (x + 1) & !1
}

/// The `char *gid` the reorderbuffer / filter callbacks take, taken from the
/// parsed commit/abort's twophase gid. xactdesc keeps the gid as the trailing
/// NUL-terminated string in `data` when `XACT_XINFO_HAS_GID`/`HAS_TWOPHASE` is
/// set; the parser exposes it via [`ParsedCommitAbort`]'s gid offset, so we read
/// it back as the NUL-stripped bytes.
fn commit_gid_bytes(_data: &[u8], _parsed: &ParsedCommitAbort) -> Vec<u8> {
    // The parsed commit/abort record's twophase gid lives at the parser's gid
    // offset; xactdesc exposes it via `twophase_gid`-style accessors only on
    // ParsedPrepare. For commit/abort the gid is only present for the
    // *_PREPARED forms and is consumed by FilterPrepare (which gets it via
    // `parsed_commit_gid`). The reorderbuffer FinishPrepared call needs the
    // same gid bytes; reuse the same reader.
    parsed_commit_gid(_data, _parsed).to_vec()
}

/// Read the twophase gid bytes out of a parsed commit/abort record's payload.
/// xactdesc parses the gid into the record at `parsed`'s position when
/// `XACT_XINFO_HAS_GID` is set; we expose it as the NUL-stripped bytes.
fn parsed_commit_gid<'a>(data: &'a [u8], parsed: &ParsedCommitAbort) -> &'a [u8] {
    backend_access_rmgrdesc_xactdesc::commit_abort_gid(data, parsed)
}

/// Decode an `XLOG_XACT_INVALIDATIONS` record body: `{int nmsgs;
/// SharedInvalidationMessage msgs[]}`. `nmsgs`@0, `msgs`@4 (4-byte aligned).
fn decode_xact_invals(data: &[u8]) -> PgResult<Vec<SharedInvalidationMessage>> {
    let nmsgs = i32::from_ne_bytes(
        data.get(0..4)
            .ok_or_else(|| elog_error(format!("invalid xl_xact_invals record")))?
            .try_into()
            .unwrap(),
    );
    const MSGS_OFFSET: usize = 4;
    let msgs = SharedInvalMessages::from_bytes(&data[MSGS_OFFSET..]);
    let mut out = Vec::with_capacity(nmsgs.max(0) as usize);
    for i in 0..nmsgs.max(0) as usize {
        if let Some(m) = msgs.get(i) {
            out.push(m);
        }
    }
    Ok(out)
}

/// Read the trailing `xids[xcnt + subxcnt]` array of an `xl_running_xacts`.
fn running_xacts_xids(data: &[u8], xlrec: &xl_running_xacts) -> Vec<TransactionId> {
    let total = (xlrec.xcnt.max(0) + xlrec.subxcnt.max(0)) as usize;
    let mut xids = Vec::with_capacity(total);
    for i in 0..total {
        xids.push(xl_running_xacts::xid(data, i));
    }
    xids
}

/// Install the dispatch + rmgr `rm_decode` seams this unit owns.
pub fn init_seams() {
    decode_seam::xlog_decode::set(xlog_decode);
    decode_seam::xact_decode::set(xact_decode);
    decode_seam::standby_decode::set(standby_decode);
    decode_seam::heap2_decode::set(heap2_decode);
    decode_seam::heap_decode::set(heap_decode);
    decode_seam::logicalmsg_decode::set(logicalmsg_decode);
    decode_seam::LogicalDecodingProcessRecord::set(LogicalDecodingProcessRecord);
}
