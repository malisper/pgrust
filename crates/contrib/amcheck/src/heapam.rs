#![allow(non_camel_case_types)]

use std::sync::atomic::Ordering::Relaxed;

use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::{
    Buffer, ForkNumber, MultiXactId, Oid, OffsetNumber, TransactionId, BLCKSZ,
    C_COLLATION_OID,
};
use types_core::xact::{
    BootstrapTransactionId, FirstNormalFullTransactionId, FirstNormalTransactionId,
    FrozenTransactionId, FullTransactionId, InvalidTransactionId, MultiXactIdPrecedes,
    MultiXactIdPrecedesOrEquals, TransactionIdEquals, TransactionIdIsNormal, TransactionIdIsValid,
    TransactionIdPrecedes,
};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_WRONG_OBJECT_TYPE,
};
use types_fmgr::{
    varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
};
use types_rel::{AccessShareLock, Relation};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_scan::sdir::ForwardScanDirection;
use types_storage::buf::{BufferAccessStrategy, BufferAccessStrategyType, BUFFER_LOCK_SHARE};
use types_storage::bufpage::{MaxOffsetNumber, PageRef};
use types_storage::storage::ReadBufferMode;
use types_tuple::htup::{
    BITMAPLEN, HEAP_HASEXTERNAL, HEAP_HASNULL, HEAP_HOT_UPDATED, HEAP_MOVED_IN, HEAP_MOVED_OFF,
    HEAP_UPDATED, HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI,
    HEAP_XMAX_IS_LOCKED_ONLY, HeapTupleHeaderData, SizeofHeapTupleHeader,
};
use types_tuple::itemptr::{ItemPointerGetBlockNumber, ItemPointerGetOffsetNumberNoCheck};
use types_tuple::varatt::{
    varatt_is_1b, varatt_is_1b_e, varatt_is_4b_u, varsize_1b, varsize_4b, VARHDRSZ,
    VARHDRSZ_EXTERNAL, VARHDRSZ_SHORT, VARTAG_ONDISK,
};

use funcapi::{InitMaterializedSRF, MaterializedSRF};
use heaptoast::{toast_close_indexes, toast_open_indexes, TOAST_MAX_CHUNK_SIZE};
use toastdesc::{
    VarattExternal, TOAST_LZ4_COMPRESSION_ID, TOAST_PGLZ_COMPRESSION_ID,
};
use visibilitymap::{
    visibilitymap_get_status, VmBuffer, VISIBILITYMAP_ALL_FROZEN, VISIBILITYMAP_ALL_VISIBLE,
};

const HEAPCHECK_RELATION_COLS: usize = 4;
const VARLENA_SIZE_LIMIT: i32 = 0x3FFF_FFFF;
const HEAP_TABLE_AM_OID: Oid = 2;
const FIRST_OFFSET_NUMBER: OffsetNumber = 1;
const INVALID_OFFSET_NUMBER: OffsetNumber = 0;
const NSLOTS: usize = MaxOffsetNumber as usize + 1;

#[inline]
const fn maxalign(len: u32) -> u32 {
    (len + 7) & !7
}

#[inline]
fn offset_number_next(offnum: OffsetNumber) -> OffsetNumber {
    offnum + 1
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum XidBoundsViolation {
    XID_INVALID,
    XID_IN_FUTURE,
    XID_PRECEDES_CLUSTERMIN,
    XID_PRECEDES_RELMIN,
    XID_BOUNDS_OK,
}
use XidBoundsViolation::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum XidCommitStatus {
    XID_COMMITTED,
    XID_IS_CURRENT_XID,
    XID_IN_PROGRESS,
    XID_ABORTED,
}
use XidCommitStatus::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum SkipPages {
    SKIP_PAGES_ALL_FROZEN,
    SKIP_PAGES_ALL_VISIBLE,
    SKIP_PAGES_NONE,
}
use SkipPages::*;

struct ToastedAttribute {
    toast_pointer: VarattExternal,
    blkno: u32,
    offnum: OffsetNumber,
    attnum: i16,
}

struct HeapCheckContext<'mcx> {
    mcx: Mcx<'mcx>,

    next_fxid: FullTransactionId,
    next_xid: TransactionId,
    oldest_xid: TransactionId,
    oldest_fxid: FullTransactionId,
    safe_xmin: TransactionId,

    next_mxact: MultiXactId,
    oldest_mxact: MultiXactId,

    cached_xid: TransactionId,
    cached_status: XidCommitStatus,

    relfrozenxid: TransactionId,
    relfrozenfxid: FullTransactionId,
    relminmxid: MultiXactId,
    toast_rel_present: bool,

    blkno: u32,
    offnum: OffsetNumber,
    attnum: i16,

    lp_len: u16,
    natts: i32,

    offset: u32,

    tuple_could_be_pruned: bool,

    toasted_attributes: PgVec<'mcx, ToastedAttribute>,

    is_corrupt: bool,
}

#[inline]
unsafe fn header_at<'p>(page: PageRef<'p>, lp_off: u16) -> &'p HeapTupleHeaderData {
    unsafe { &*page.as_ptr().add(lp_off as usize).cast::<HeapTupleHeaderData>() }
}

#[inline]
unsafe fn att_isnull(att: i32, bits: *const u8) -> bool {
    let byte = unsafe { *bits.add((att >> 3) as usize) };
    (byte & (1 << (att & 0x07))) == 0
}

#[inline]
fn att_nominal_alignby(cur_offset: u32, attalignby: u8) -> u32 {
    let a = attalignby as u32;
    (cur_offset + (a - 1)) & !(a - 1)
}

#[inline]
unsafe fn att_pointer_alignby(cur_offset: u32, attalignby: u8, attptr: *const u8) -> u32 {
    if unsafe { *attptr } != 0 {
        cur_offset
    } else {
        att_nominal_alignby(cur_offset, attalignby)
    }
}

#[inline]
unsafe fn att_addlength_pointer(cur_offset: u32, attlen: i16, attptr: *const u8) -> u32 {
    if attlen > 0 {
        cur_offset + attlen as u32
    } else if attlen == -1 {
        cur_offset + unsafe { varsize_any(attptr) } as u32
    } else {
        debug_assert_eq!(attlen, -2);
        let mut len = 0usize;
        while unsafe { *attptr.add(len) } != 0 {
            len += 1;
        }
        cur_offset + len as u32 + 1
    }
}

#[inline]
unsafe fn varsize_any(p: *const u8) -> usize {
    unsafe { types_tuple::varatt::varsize_any(p) }
}

#[inline]
unsafe fn varatt_is_extended(p: *const u8) -> bool {
    !unsafe { varatt_is_4b_u(p) }
}

fn full_xid_from_xid_and_ctx(xid: TransactionId, ctx: &HeapCheckContext) -> FullTransactionId {
    debug_assert!(TransactionIdIsNormal(ctx.next_xid));
    debug_assert!(ctx.next_fxid.is_normal());
    debug_assert!(ctx.next_fxid.xid() == ctx.next_xid);

    if !TransactionIdIsNormal(xid) {
        return FullTransactionId::from_epoch_and_xid(0, xid);
    }

    let nextfxid_i = ctx.next_fxid.to_u64();
    let diff = ctx.next_xid.wrapping_sub(xid) as i32;

    let fxid = if diff > 0 && (nextfxid_i - FirstNormalTransactionId as u64) < diff as i64 as u64 {
        debug_assert!(ctx.next_fxid.epoch() == 0);
        FirstNormalFullTransactionId
    } else {
        FullTransactionId::from_u64(nextfxid_i.wrapping_sub(diff as i64 as u64))
    };

    debug_assert!(fxid.is_normal());
    fxid
}

// DIVERGENCE: C reads nextXid/oldestXid under XidGenLock; here relaxed unlocked atomics (advisory; the caller rechecks the range).
fn update_cached_xid_range(ctx: &mut HeapCheckContext) {
    let tv = varsup::TransamVariables();
    ctx.next_fxid = FullTransactionId::from_u64(tv.nextXid.load(Relaxed));
    ctx.oldest_xid = tv.oldestXid.load(Relaxed);
    ctx.next_xid = ctx.next_fxid.xid();
    ctx.oldest_fxid = full_xid_from_xid_and_ctx(ctx.oldest_xid, ctx);
}

fn update_cached_mxid_range(ctx: &mut HeapCheckContext) -> PgResult<()> {
    let (oldest, next) = multixact::ReadMultiXactIdRange()?;
    ctx.oldest_mxact = oldest;
    ctx.next_mxact = next;
    Ok(())
}

fn fxid_in_cached_range(fxid: FullTransactionId, ctx: &HeapCheckContext) -> bool {
    ctx.oldest_fxid.precedes_or_equals(fxid) && fxid.precedes(ctx.next_fxid)
}

fn check_mxid_in_range(mxid: MultiXactId, ctx: &HeapCheckContext) -> XidBoundsViolation {
    if !TransactionIdIsValid(mxid) {
        return XID_INVALID;
    }
    if MultiXactIdPrecedes(mxid, ctx.relminmxid) {
        return XID_PRECEDES_RELMIN;
    }
    if MultiXactIdPrecedes(mxid, ctx.oldest_mxact) {
        return XID_PRECEDES_CLUSTERMIN;
    }
    if MultiXactIdPrecedesOrEquals(ctx.next_mxact, mxid) {
        return XID_IN_FUTURE;
    }
    XID_BOUNDS_OK
}

fn check_mxid_valid_in_rel(mxid: MultiXactId, ctx: &mut HeapCheckContext) -> PgResult<XidBoundsViolation> {
    let result = check_mxid_in_range(mxid, ctx);
    if result == XID_BOUNDS_OK {
        return Ok(XID_BOUNDS_OK);
    }
    update_cached_mxid_range(ctx)?;
    Ok(check_mxid_in_range(mxid, ctx))
}

// DIVERGENCE: C reads oldestClogXid under XactTruncationLock; here a relaxed unlocked atomic read (see update_cached_xid_range).
fn get_xid_status(
    xid: TransactionId,
    ctx: &mut HeapCheckContext,
    want_status: bool,
) -> PgResult<(XidBoundsViolation, Option<XidCommitStatus>)> {
    if !TransactionIdIsValid(xid) {
        return Ok((XID_INVALID, None));
    } else if xid == BootstrapTransactionId || xid == FrozenTransactionId {
        return Ok((XID_BOUNDS_OK, Some(XID_COMMITTED)));
    }

    let mut fxid = full_xid_from_xid_and_ctx(xid, ctx);
    if !fxid_in_cached_range(fxid, ctx) {
        update_cached_xid_range(ctx);
        fxid = full_xid_from_xid_and_ctx(xid, ctx);
    }

    if ctx.next_fxid.precedes_or_equals(fxid) {
        return Ok((XID_IN_FUTURE, None));
    }
    if fxid.precedes(ctx.oldest_fxid) {
        return Ok((XID_PRECEDES_CLUSTERMIN, None));
    }
    if fxid.precedes(ctx.relfrozenfxid) {
        return Ok((XID_PRECEDES_RELMIN, None));
    }

    if !want_status {
        return Ok((XID_BOUNDS_OK, None));
    }

    if xid == ctx.cached_xid {
        return Ok((XID_BOUNDS_OK, Some(ctx.cached_status)));
    }

    let clog_horizon =
        full_xid_from_xid_and_ctx(varsup::TransamVariables().oldestClogXid.load(Relaxed), ctx);
    let mut status = XID_COMMITTED;
    if clog_horizon.precedes_or_equals(fxid) {
        if xact::TransactionIdIsCurrentTransactionId(xid) {
            status = XID_IS_CURRENT_XID;
        } else if procarray::TransactionIdIsInProgress(xid)? {
            status = XID_IN_PROGRESS;
        } else if transam::TransactionIdDidCommit(xid)? {
            status = XID_COMMITTED;
        } else {
            status = XID_ABORTED;
        }
    }
    ctx.cached_xid = xid;
    ctx.cached_status = status;
    Ok((XID_BOUNDS_OK, Some(status)))
}

fn report_internal<'mcx>(
    mcx: Mcx<'mcx>,
    srf: &mut MaterializedSRF<'mcx>,
    blkno: u32,
    offnum: OffsetNumber,
    attnum: i16,
    msg: String,
) -> PgResult<()> {
    let mut nulls = [false; HEAPCHECK_RELATION_COLS];
    nulls[2] = attnum < 0;
    let values = [
        Datum::from_i64(blkno as i64),
        Datum::from_i32(offnum as i32),
        Datum::from_i32(attnum as i32),
        varlena_result(varlena::cstring_to_text(mcx, msg.as_bytes())?),
    ];
    srf.putvalues(&values, &nulls)
}

fn report<'mcx>(
    ctx: &mut HeapCheckContext<'mcx>,
    srf: &mut MaterializedSRF<'mcx>,
    msg: String,
) -> PgResult<()> {
    report_internal(ctx.mcx, srf, ctx.blkno, ctx.offnum, ctx.attnum, msg)?;
    ctx.is_corrupt = true;
    Ok(())
}

fn report_toast<'mcx>(
    ctx: &mut HeapCheckContext<'mcx>,
    srf: &mut MaterializedSRF<'mcx>,
    ta: &ToastedAttribute,
    msg: String,
) -> PgResult<()> {
    report_internal(ctx.mcx, srf, ta.blkno, ta.offnum, ta.attnum, msg)?;
    ctx.is_corrupt = true;
    Ok(())
}

fn check_tuple_header<'mcx>(
    ctx: &mut HeapCheckContext<'mcx>,
    srf: &mut MaterializedSRF<'mcx>,
    tuphdr: &HeapTupleHeaderData,
) -> PgResult<bool> {
    let infomask = tuphdr.t_infomask;
    let curr_xmax = heapam::HeapTupleHeaderGetUpdateXid(tuphdr)?;
    let mut result = true;

    if tuphdr.t_hoff as u16 > ctx.lp_len {
        report(ctx, srf, format!(
            "data begins at offset {} beyond the tuple length {}",
            tuphdr.t_hoff, ctx.lp_len
        ))?;
        result = false;
    }

    if (infomask & HEAP_XMAX_COMMITTED) != 0 && (infomask & HEAP_XMAX_IS_MULTI) != 0 {
        report(ctx, srf, "multixact should not be marked committed".to_string())?;
    }

    if !TransactionIdIsValid(curr_xmax) && tuphdr.is_hot_updated() {
        report(ctx, srf, "tuple has been HOT updated, but xmax is 0".to_string())?;
    }

    if tuphdr.is_heap_only() && (infomask & HEAP_UPDATED) == 0 {
        report(ctx, srf, "tuple is heap only, but not the result of an update".to_string())?;
    }

    let expected_hoff = if (infomask & HEAP_HASNULL) != 0 {
        maxalign(SizeofHeapTupleHeader as u32 + BITMAPLEN(ctx.natts) as u32)
    } else {
        maxalign(SizeofHeapTupleHeader as u32)
    };
    if tuphdr.t_hoff as u32 != expected_hoff {
        let hoff = tuphdr.t_hoff as u32;
        let natts = ctx.natts as u32;
        let msg = if (infomask & HEAP_HASNULL) != 0 && ctx.natts == 1 {
            format!("tuple data should begin at byte {expected_hoff}, but actually begins at byte {hoff} (1 attribute, has nulls)")
        } else if (infomask & HEAP_HASNULL) != 0 {
            format!("tuple data should begin at byte {expected_hoff}, but actually begins at byte {hoff} ({natts} attributes, has nulls)")
        } else if ctx.natts == 1 {
            format!("tuple data should begin at byte {expected_hoff}, but actually begins at byte {hoff} (1 attribute, no nulls)")
        } else {
            format!("tuple data should begin at byte {expected_hoff}, but actually begins at byte {hoff} ({natts} attributes, no nulls)")
        };
        report(ctx, srf, msg)?;
        result = false;
    }

    Ok(result)
}

fn check_tuple_visibility<'mcx>(
    ctx: &mut HeapCheckContext<'mcx>,
    srf: &mut MaterializedSRF<'mcx>,
    tuphdr: &HeapTupleHeaderData,
) -> PgResult<(bool, bool, XidCommitStatus)> {
    ctx.tuple_could_be_pruned = true;
    #[allow(unused_assignments)]
    let mut xmin_commit_status_ok = false;
    #[allow(unused_assignments)]
    let mut xmin_commit_status = XID_COMMITTED;

    let xmin = tuphdr.xmin();
    let (xmin_bound, xmin_status_opt) = get_xid_status(xmin, ctx, true)?;
    match xmin_bound {
        XID_INVALID => {
            return Ok((false, false, XID_COMMITTED));
        }
        XID_BOUNDS_OK => {
            xmin_commit_status_ok = true;
            xmin_commit_status = xmin_status_opt.expect("XID_BOUNDS_OK carries a status");
        }
        XID_IN_FUTURE => {
            report(ctx, srf, format!(
                "xmin {xmin} equals or exceeds next valid transaction ID {}:{}",
                ctx.next_fxid.epoch(), ctx.next_fxid.xid()
            ))?;
            return Ok((false, false, XID_COMMITTED));
        }
        XID_PRECEDES_CLUSTERMIN => {
            report(ctx, srf, format!(
                "xmin {xmin} precedes oldest valid transaction ID {}:{}",
                ctx.oldest_fxid.epoch(), ctx.oldest_fxid.xid()
            ))?;
            return Ok((false, false, XID_COMMITTED));
        }
        XID_PRECEDES_RELMIN => {
            report(ctx, srf, format!(
                "xmin {xmin} precedes relation freeze threshold {}:{}",
                ctx.relfrozenfxid.epoch(), ctx.relfrozenfxid.xid()
            ))?;
            return Ok((false, false, XID_COMMITTED));
        }
    }
    let xmin_status = xmin_status_opt.expect("XID_BOUNDS_OK carries a status");

    if !tuphdr.xmin_committed() {
        if tuphdr.xmin_invalid() {
            return Ok((false, xmin_commit_status_ok, xmin_commit_status));
        } else if (tuphdr.t_infomask & HEAP_MOVED_OFF) != 0 {
            let xvac = tuphdr.xvac();
            let (bound, xvac_status) = get_xid_status(xvac, ctx, true)?;
            match bound {
                XID_INVALID => {
                    report(ctx, srf, "old-style VACUUM FULL transaction ID for moved off tuple is invalid".to_string())?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_IN_FUTURE => {
                    report(ctx, srf, format!("old-style VACUUM FULL transaction ID {xvac} for moved off tuple equals or exceeds next valid transaction ID {}:{}", ctx.next_fxid.epoch(), ctx.next_fxid.xid()))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_PRECEDES_RELMIN => {
                    report(ctx, srf, format!("old-style VACUUM FULL transaction ID {xvac} for moved off tuple precedes relation freeze threshold {}:{}", ctx.relfrozenfxid.epoch(), ctx.relfrozenfxid.xid()))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_PRECEDES_CLUSTERMIN => {
                    report(ctx, srf, format!("old-style VACUUM FULL transaction ID {xvac} for moved off tuple precedes oldest valid transaction ID {}:{}", ctx.oldest_fxid.epoch(), ctx.oldest_fxid.xid()))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_BOUNDS_OK => {}
            }
            match xvac_status.expect("XID_BOUNDS_OK carries a status") {
                XID_IS_CURRENT_XID => {
                    report(ctx, srf, format!("old-style VACUUM FULL transaction ID {xvac} for moved off tuple matches our current transaction ID"))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_IN_PROGRESS => {
                    report(ctx, srf, format!("old-style VACUUM FULL transaction ID {xvac} for moved off tuple appears to be in progress"))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_COMMITTED => {
                    return Ok((true, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_ABORTED => {}
            }
        } else if (tuphdr.t_infomask & HEAP_MOVED_IN) != 0 {
            let xvac = tuphdr.xvac();
            let (bound, xvac_status) = get_xid_status(xvac, ctx, true)?;
            match bound {
                XID_INVALID => {
                    report(ctx, srf, "old-style VACUUM FULL transaction ID for moved in tuple is invalid".to_string())?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_IN_FUTURE => {
                    report(ctx, srf, format!("old-style VACUUM FULL transaction ID {xvac} for moved in tuple equals or exceeds next valid transaction ID {}:{}", ctx.next_fxid.epoch(), ctx.next_fxid.xid()))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_PRECEDES_RELMIN => {
                    report(ctx, srf, format!("old-style VACUUM FULL transaction ID {xvac} for moved in tuple precedes relation freeze threshold {}:{}", ctx.relfrozenfxid.epoch(), ctx.relfrozenfxid.xid()))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_PRECEDES_CLUSTERMIN => {
                    report(ctx, srf, format!("old-style VACUUM FULL transaction ID {xvac} for moved in tuple precedes oldest valid transaction ID {}:{}", ctx.oldest_fxid.epoch(), ctx.oldest_fxid.xid()))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_BOUNDS_OK => {}
            }
            match xvac_status.expect("XID_BOUNDS_OK carries a status") {
                XID_IS_CURRENT_XID => {
                    report(ctx, srf, format!("old-style VACUUM FULL transaction ID {xvac} for moved in tuple matches our current transaction ID"))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_IN_PROGRESS => {
                    report(ctx, srf, format!("old-style VACUUM FULL transaction ID {xvac} for moved in tuple appears to be in progress"))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_COMMITTED => {}
                XID_ABORTED => {
                    return Ok((true, xmin_commit_status_ok, xmin_commit_status));
                }
            }
        } else if xmin_status != XID_COMMITTED {
            return Ok((false, xmin_commit_status_ok, xmin_commit_status));
        }
    }

    if (tuphdr.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        let xmax = tuphdr.xmax_raw();
        match check_mxid_valid_in_rel(xmax, ctx)? {
            XID_INVALID => {
                report(ctx, srf, "multitransaction ID is invalid".to_string())?;
                return Ok((true, xmin_commit_status_ok, xmin_commit_status));
            }
            XID_PRECEDES_RELMIN => {
                report(ctx, srf, format!("multitransaction ID {xmax} precedes relation minimum multitransaction ID threshold {}", ctx.relminmxid))?;
                return Ok((true, xmin_commit_status_ok, xmin_commit_status));
            }
            XID_PRECEDES_CLUSTERMIN => {
                report(ctx, srf, format!("multitransaction ID {xmax} precedes oldest valid multitransaction ID threshold {}", ctx.oldest_mxact))?;
                return Ok((true, xmin_commit_status_ok, xmin_commit_status));
            }
            XID_IN_FUTURE => {
                report(ctx, srf, format!("multitransaction ID {xmax} equals or exceeds next valid multitransaction ID {}", ctx.next_mxact))?;
                return Ok((true, xmin_commit_status_ok, xmin_commit_status));
            }
            XID_BOUNDS_OK => {}
        }
    }

    if (tuphdr.t_infomask & HEAP_XMAX_INVALID) != 0 {
        ctx.tuple_could_be_pruned = false;
        return Ok((true, xmin_commit_status_ok, xmin_commit_status));
    }

    if HEAP_XMAX_IS_LOCKED_ONLY(tuphdr.t_infomask) {
        ctx.tuple_could_be_pruned = false;
        return Ok((true, xmin_commit_status_ok, xmin_commit_status));
    }

    if (tuphdr.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        let xmax = heapam::HeapTupleGetUpdateXid(tuphdr)?;
        let (bound, xmax_status) = get_xid_status(xmax, ctx, true)?;
        match bound {
            XID_INVALID => {
                report(ctx, srf, "update xid is invalid".to_string())?;
                return Ok((true, xmin_commit_status_ok, xmin_commit_status));
            }
            XID_IN_FUTURE => {
                report(ctx, srf, format!("update xid {xmax} equals or exceeds next valid transaction ID {}:{}", ctx.next_fxid.epoch(), ctx.next_fxid.xid()))?;
                return Ok((true, xmin_commit_status_ok, xmin_commit_status));
            }
            XID_PRECEDES_RELMIN => {
                report(ctx, srf, format!("update xid {xmax} precedes relation freeze threshold {}:{}", ctx.relfrozenfxid.epoch(), ctx.relfrozenfxid.xid()))?;
                return Ok((true, xmin_commit_status_ok, xmin_commit_status));
            }
            XID_PRECEDES_CLUSTERMIN => {
                report(ctx, srf, format!("update xid {xmax} precedes oldest valid transaction ID {}:{}", ctx.oldest_fxid.epoch(), ctx.oldest_fxid.xid()))?;
                return Ok((true, xmin_commit_status_ok, xmin_commit_status));
            }
            XID_BOUNDS_OK => {}
        }
        match xmax_status.expect("XID_BOUNDS_OK carries a status") {
            XID_IS_CURRENT_XID | XID_IN_PROGRESS => {
                ctx.tuple_could_be_pruned = false;
            }
            XID_COMMITTED => {
                ctx.tuple_could_be_pruned = TransactionIdPrecedes(xmax, ctx.safe_xmin);
            }
            XID_ABORTED => {
                ctx.tuple_could_be_pruned = false;
            }
        }
        return Ok((true, xmin_commit_status_ok, xmin_commit_status));
    }

    let xmax = tuphdr.xmax_raw();
    let (bound, xmax_status) = get_xid_status(xmax, ctx, true)?;
    match bound {
        XID_INVALID => {
            ctx.tuple_could_be_pruned = false;
            return Ok((true, xmin_commit_status_ok, xmin_commit_status));
        }
        XID_IN_FUTURE => {
            report(ctx, srf, format!("xmax {xmax} equals or exceeds next valid transaction ID {}:{}", ctx.next_fxid.epoch(), ctx.next_fxid.xid()))?;
            return Ok((false, xmin_commit_status_ok, xmin_commit_status));
        }
        XID_PRECEDES_RELMIN => {
            report(ctx, srf, format!("xmax {xmax} precedes relation freeze threshold {}:{}", ctx.relfrozenfxid.epoch(), ctx.relfrozenfxid.xid()))?;
            return Ok((false, xmin_commit_status_ok, xmin_commit_status));
        }
        XID_PRECEDES_CLUSTERMIN => {
            report(ctx, srf, format!("xmax {xmax} precedes oldest valid transaction ID {}:{}", ctx.oldest_fxid.epoch(), ctx.oldest_fxid.xid()))?;
            return Ok((false, xmin_commit_status_ok, xmin_commit_status));
        }
        XID_BOUNDS_OK => {}
    }

    match xmax_status.expect("XID_BOUNDS_OK carries a status") {
        XID_IS_CURRENT_XID | XID_IN_PROGRESS => {
            ctx.tuple_could_be_pruned = false;
        }
        XID_COMMITTED => {
            ctx.tuple_could_be_pruned = TransactionIdPrecedes(xmax, ctx.safe_xmin);
        }
        XID_ABORTED => {
            ctx.tuple_could_be_pruned = false;
        }
    }

    Ok((true, xmin_commit_status_ok, xmin_commit_status))
}

fn check_tuple_attribute<'mcx>(
    ctx: &mut HeapCheckContext<'mcx>,
    srf: &mut MaterializedSRF<'mcx>,
    rel: &Relation<'mcx>,
    tuphdr: &HeapTupleHeaderData,
    item: *const u8,
) -> PgResult<bool> {
    let infomask = tuphdr.t_infomask;
    let thisatt = &rel.rd_att.compact_attrs[ctx.attnum as usize];
    let attlen = thisatt.attlen;
    let attalignby = thisatt.attalignby;
    let t_hoff = tuphdr.t_hoff as u32;

    if t_hoff + ctx.offset > ctx.lp_len as u32 {
        report(ctx, srf, format!(
            "attribute with length {attlen} starts at offset {} beyond total tuple length {}",
            t_hoff + ctx.offset, ctx.lp_len
        ))?;
        return Ok(false);
    }

    if (infomask & HEAP_HASNULL) != 0
        && unsafe { att_isnull(ctx.attnum as i32, item.add(SizeofHeapTupleHeader)) }
    {
        return Ok(true);
    }

    let tp = unsafe { item.add(t_hoff as usize) };

    if attlen != -1 {
        ctx.offset = att_nominal_alignby(ctx.offset, attalignby);
        ctx.offset = unsafe { att_addlength_pointer(ctx.offset, attlen, tp.add(ctx.offset as usize)) };
        if t_hoff + ctx.offset > ctx.lp_len as u32 {
            report(ctx, srf, format!(
                "attribute with length {attlen} ends at offset {} beyond total tuple length {}",
                t_hoff + ctx.offset, ctx.lp_len
            ))?;
            return Ok(false);
        }
        return Ok(true);
    }

    ctx.offset = unsafe { att_pointer_alignby(ctx.offset, attalignby, tp.add(ctx.offset as usize)) };

    let attr = unsafe { tp.add(ctx.offset as usize) };

    if unsafe { varatt_is_1b_e(attr) } {
        let va_tag = unsafe { *attr.add(1) };
        if va_tag != VARTAG_ONDISK {
            report(ctx, srf, format!("toasted attribute has unexpected TOAST tag {va_tag}"))?;
            return Ok(false);
        }
    }

    ctx.offset = unsafe { att_addlength_pointer(ctx.offset, attlen, tp.add(ctx.offset as usize)) };

    if t_hoff + ctx.offset > ctx.lp_len as u32 {
        report(ctx, srf, format!(
            "attribute with length {attlen} ends at offset {} beyond total tuple length {}",
            t_hoff + ctx.offset, ctx.lp_len
        ))?;
        return Ok(false);
    }

    if !unsafe { varatt_is_1b_e(attr) } {
        return Ok(true);
    }

    let mut payload = [0u8; 16];
    unsafe { core::ptr::copy_nonoverlapping(attr.add(VARHDRSZ_EXTERNAL), payload.as_mut_ptr(), 16) };
    let toast_pointer = VarattExternal::from_payload(payload);

    if toast_pointer.va_rawsize > VARLENA_SIZE_LIMIT {
        report(ctx, srf, format!(
            "toast value {} rawsize {} exceeds limit {}",
            toast_pointer.va_valueid, toast_pointer.va_rawsize, VARLENA_SIZE_LIMIT
        ))?;
    }

    if toast_pointer.is_compressed() {
        let cmid = toast_pointer.compress_method();
        let valid = matches!(cmid, TOAST_PGLZ_COMPRESSION_ID | TOAST_LZ4_COMPRESSION_ID);
        if !valid {
            report(ctx, srf, format!(
                "toast value {} has invalid compression method id {cmid}",
                toast_pointer.va_valueid
            ))?;
        }
    }

    if (infomask & HEAP_HASEXTERNAL) == 0 {
        report(ctx, srf, format!(
            "toast value {} is external but tuple header flag HEAP_HASEXTERNAL not set",
            toast_pointer.va_valueid
        ))?;
        return Ok(true);
    }

    if rel.rd_rel.reltoastrelid == 0 {
        report(ctx, srf, format!(
            "toast value {} is external but relation has no toast relation",
            toast_pointer.va_valueid
        ))?;
        return Ok(true);
    }

    if !ctx.toast_rel_present {
        return Ok(true);
    }

    if !ctx.tuple_could_be_pruned {
        ctx.toasted_attributes.push(ToastedAttribute {
            toast_pointer,
            blkno: ctx.blkno,
            offnum: ctx.offnum,
            attnum: ctx.attnum,
        });
    }

    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn check_toast_tuple<'mcx>(
    chunk_seq_col: (Datum, bool),
    chunk_data_col: (Datum, bool),
    ctx: &mut HeapCheckContext<'mcx>,
    srf: &mut MaterializedSRF<'mcx>,
    ta: &ToastedAttribute,
    expected_chunk_seq: i32,
    extsize: u32,
) -> PgResult<i32> {
    let last_chunk_seq: i32 = ((extsize - 1) / TOAST_MAX_CHUNK_SIZE as u32) as i32;

    let (chunk_seq_val, seq_isnull) = chunk_seq_col;
    if seq_isnull {
        report_toast(ctx, srf, ta, format!(
            "toast value {} has toast chunk with null sequence number",
            ta.toast_pointer.va_valueid
        ))?;
        return Ok(expected_chunk_seq);
    }
    let chunk_seq = chunk_seq_val.as_i32();
    if chunk_seq != expected_chunk_seq {
        report_toast(ctx, srf, ta, format!(
            "toast value {} index scan returned chunk {chunk_seq} when expecting chunk {expected_chunk_seq}",
            ta.toast_pointer.va_valueid
        ))?;
    }
    let next_expected = chunk_seq + 1;

    let (chunk_val, data_isnull) = chunk_data_col;
    if data_isnull {
        report_toast(ctx, srf, ta, format!(
            "toast value {} chunk {chunk_seq} has null data",
            ta.toast_pointer.va_valueid
        ))?;
        return Ok(next_expected);
    }
    let chunk = chunk_val.as_usize() as *const u8;

    let chunksize: i32 = if !unsafe { varatt_is_extended(chunk) } {
        (unsafe { varsize_4b(chunk) }) as i32 - VARHDRSZ as i32
    } else if unsafe { varatt_is_1b(chunk) } {
        (unsafe { varsize_1b(chunk) }) as i32 - VARHDRSZ_SHORT as i32
    } else {
        let header = unsafe { chunk.cast::<u32>().read_unaligned() };
        report_toast(ctx, srf, ta, format!(
            "toast value {} chunk {chunk_seq} has invalid varlena header {header:x}",
            ta.toast_pointer.va_valueid
        ))?;
        return Ok(next_expected);
    };

    if chunk_seq > last_chunk_seq {
        report_toast(ctx, srf, ta, format!(
            "toast value {} chunk {chunk_seq} follows last expected chunk {last_chunk_seq}",
            ta.toast_pointer.va_valueid
        ))?;
        return Ok(next_expected);
    }

    let expected_size: i32 = if chunk_seq < last_chunk_seq {
        TOAST_MAX_CHUNK_SIZE as i32
    } else {
        extsize as i32 - (last_chunk_seq * TOAST_MAX_CHUNK_SIZE as i32)
    };

    if chunksize != expected_size {
        report_toast(ctx, srf, ta, format!(
            "toast value {} chunk {chunk_seq} has size {chunksize}, but expected size {expected_size}",
            ta.toast_pointer.va_valueid
        ))?;
    }

    Ok(next_expected)
}

fn valueid_scan_key(valueid: Oid) -> ScanKeyData {
    let mut k = ScanKeyData::empty();
    k.sk_attno = 1;
    k.sk_strategy = BTEqualStrategyNumber;
    k.sk_collation = C_COLLATION_OID;
    k.sk_func = FmgrInfo::new(
        adt_scalar::builtins::fc_oideq,
        types_core::fmgr::F_OIDEQ,
        2,
        true,
        false,
    );
    k.sk_argument = Datum::from_oid(valueid);
    k
}

fn check_toasted_attribute<'mcx>(
    ctx: &mut HeapCheckContext<'mcx>,
    srf: &mut MaterializedSRF<'mcx>,
    ta: &ToastedAttribute,
    toast_rel: &Relation<'mcx>,
    valid_toast_index: &Relation<'mcx>,
    have_snapshot: bool,
) -> PgResult<()> {
    let mcx = ctx.mcx;
    let extsize = ta.toast_pointer.extsize();
    let last_chunk_seq: i32 = ((extsize - 1) / TOAST_MAX_CHUNK_SIZE as u32) as i32;

    let toastkey = valueid_scan_key(ta.toast_pointer.va_valueid);
    let snapshot = std::rc::Rc::new(toastdesc::get_toast_snapshot(mcx, have_snapshot)?);
    let mut toastscan = genam::systable_beginscan_ordered(
        mcx,
        toast_rel,
        valid_toast_index,
        Some(snapshot),
        core::slice::from_ref(&toastkey),
    )?;

    let toastdesc_td = &*toast_rel.rd_att;
    let mut found_toasttup = false;
    let mut expected_chunk_seq: i32 = 0;
    while let Some(toasttup) =
        genam::systable_getnext_ordered(mcx, &mut toastscan, ForwardScanDirection)?
    {
        found_toasttup = true;
        let mut seq_isnull = false;
        // SAFETY: toast tuples match the 3-column toast descriptor.
        let seq_datum = unsafe { types_tuple::heap_getattr(toasttup, 2, toastdesc_td, &mut seq_isnull) };
        let mut data_isnull = false;
        let data_datum = unsafe { types_tuple::heap_getattr(toasttup, 3, toastdesc_td, &mut data_isnull) };
        expected_chunk_seq = check_toast_tuple(
            (seq_datum, seq_isnull),
            (data_datum, data_isnull),
            ctx,
            srf,
            ta,
            expected_chunk_seq,
            extsize,
        )?;
    }
    genam::systable_endscan_ordered(mcx, toastscan)?;

    if !found_toasttup {
        report_toast(ctx, srf, ta, format!(
            "toast value {} not found in toast table",
            ta.toast_pointer.va_valueid
        ))?;
    } else if expected_chunk_seq <= last_chunk_seq {
        report_toast(ctx, srf, ta, format!(
            "toast value {} was expected to end at chunk {last_chunk_seq}, but ended while expecting chunk {expected_chunk_seq}",
            ta.toast_pointer.va_valueid
        ))?;
    }

    Ok(())
}

fn check_tuple<'mcx>(
    ctx: &mut HeapCheckContext<'mcx>,
    srf: &mut MaterializedSRF<'mcx>,
    rel: &Relation<'mcx>,
    tuphdr: &HeapTupleHeaderData,
    item: *const u8,
) -> PgResult<(bool, XidCommitStatus)> {
    if !check_tuple_header(ctx, srf, tuphdr)? {
        return Ok((false, XID_COMMITTED));
    }

    let (checkable, xmin_ok, xmin_status) = check_tuple_visibility(ctx, srf, tuphdr)?;
    if !checkable {
        return Ok((xmin_ok, xmin_status));
    }

    if rel.rd_att.natts < ctx.natts {
        report(ctx, srf, format!(
            "number of attributes {} exceeds maximum expected for table {}",
            ctx.natts, rel.rd_att.natts
        ))?;
        return Ok((xmin_ok, xmin_status));
    }

    ctx.offset = 0;
    let natts = ctx.natts;
    let mut attnum = 0;
    while attnum < natts {
        ctx.attnum = attnum as i16;
        if !check_tuple_attribute(ctx, srf, rel, tuphdr, item)? {
            break;
        }
        attnum += 1;
    }

    ctx.attnum = -1;
    Ok((xmin_ok, xmin_status))
}

#[track_caller]
#[cold]
fn param_err(msg: impl Into<String>) -> Box<PgError> {
    Box::new(PgError::error(msg.into()).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
}

// DIVERGENCE from verify_heapam.c: C's read_stream is replaced by a per-block ReadBufferExtended loop; skippability is checked per block against the visibility map.
pub(crate) fn verify_heapam(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("verify_heapam: resolved FmgrInfo required");

    if fcinfo.argisnull(0) {
        return Err(param_err("relation cannot be null"));
    }
    let relid = fcinfo.arg_oid(0);

    if fcinfo.argisnull(1) {
        return Err(param_err("on_error_stop cannot be null"));
    }
    let on_error_stop = fcinfo.arg_bool(1);

    if fcinfo.argisnull(2) {
        return Err(param_err("check_toast cannot be null"));
    }
    let check_toast = fcinfo.arg_bool(2);

    if fcinfo.argisnull(3) {
        return Err(param_err("skip cannot be null"));
    }
    // SAFETY: arg 3 was null-checked above.
    let skip_bytes = unsafe { fcinfo.arg_varlena_packed(3)? };
    let skip = String::from_utf8_lossy(skip_bytes.data());
    let skip_option = if skip.eq_ignore_ascii_case("all-visible") {
        SKIP_PAGES_ALL_VISIBLE
    } else if skip.eq_ignore_ascii_case("all-frozen") {
        SKIP_PAGES_ALL_FROZEN
    } else if skip.eq_ignore_ascii_case("none") {
        SKIP_PAGES_NONE
    } else {
        return Err(Box::new(
            PgError::error("invalid skip option")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_hint("Valid skip options are \"all-visible\", \"all-frozen\", and \"none\"."),
        ));
    };
    let _ = skip_bytes;

    let startblock: Option<i64> = if fcinfo.argisnull(4) { None } else { Some(fcinfo.arg_i64(4)) };
    let endblock: Option<i64> = if fcinfo.argisnull(5) { None } else { Some(fcinfo.arg_i64(5)) };

    let safe_xmin = snapmgr::GetTransactionSnapshot()?.xmin;

    // SAFETY: the result context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    let rel = relation::relation_open(mcx, relid, AccessShareLock)?;

    if !types_rel::pg_class::RELKIND_HAS_TABLE_AM(rel.rd_rel.relkind)
        && rel.rd_rel.relkind != types_rel::pg_class::RELKIND_SEQUENCE
    {
        let name = rel.name().to_string();
        let detail = pg_class_seams::errdetail_relkind_not_supported::call(rel.rd_rel.relkind)?;
        rel.close(AccessShareLock)?;
        return Err(Box::new(
            PgError::error(format!("cannot check relation \"{name}\""))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
                .with_detail(detail),
        ));
    }

    if rel.rd_rel.relkind != types_rel::pg_class::RELKIND_SEQUENCE
        && rel.rd_rel.relam != HEAP_TABLE_AM_OID
    {
        rel.close(AccessShareLock)?;
        return Err(Box::new(
            PgError::error("only heap AM is supported").with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    if rel.rd_rel.relpersistence == types_core::catalog::RELPERSISTENCE_UNLOGGED
        && transam_xlog::RecoveryInProgress()
    {
        rel.close(AccessShareLock)?;
        return Ok(srf.finish(fcinfo));
    }

    let nblocks = bufmgr::RelationGetNumberOfBlocksInFork(&rel, ForkNumber::MAIN_FORKNUM)?;
    if nblocks == 0 {
        rel.close(AccessShareLock)?;
        return Ok(srf.finish(fcinfo));
    }

    let bstrategy: BufferAccessStrategy =
        bufmgr::GetAccessStrategy(BufferAccessStrategyType::BasBulkread);

    let first_block: u32 = match startblock {
        None => 0,
        Some(fb) => {
            if fb < 0 || fb >= nblocks as i64 {
                rel.close(AccessShareLock)?;
                return Err(param_err(format!(
                    "starting block number must be between 0 and {}",
                    nblocks - 1
                )));
            }
            fb as u32
        }
    };
    let last_block: u32 = match endblock {
        None => nblocks - 1,
        Some(lb) => {
            if lb < 0 || lb >= nblocks as i64 {
                rel.close(AccessShareLock)?;
                return Err(param_err(format!(
                    "ending block number must be between 0 and {}",
                    nblocks - 1
                )));
            }
            lb as u32
        }
    };

    let have_snapshot = snapmgr::HaveRegisteredOrActiveSnapshot();
    let (toast_rel, toast_indexes): (Option<Relation>, Option<(PgVec<Relation>, usize)>) =
        if rel.rd_rel.reltoastrelid != 0 && check_toast {
            let tr = table::table_open(mcx, rel.rd_rel.reltoastrelid, AccessShareLock)?;
            let idxs = toast_open_indexes(mcx, &tr, AccessShareLock)?;
            (Some(tr), Some(idxs))
        } else {
            (None, None)
        };

    let mut ctx = HeapCheckContext {
        mcx,
        next_fxid: FullTransactionId::from_u64(0),
        next_xid: 0,
        oldest_xid: 0,
        oldest_fxid: FullTransactionId::from_u64(0),
        safe_xmin,
        next_mxact: 0,
        oldest_mxact: 0,
        cached_xid: InvalidTransactionId,
        cached_status: XID_COMMITTED,
        relfrozenxid: 0,
        relfrozenfxid: FullTransactionId::from_u64(0),
        relminmxid: 0,
        toast_rel_present: toast_rel.is_some(),
        blkno: 0,
        offnum: 0,
        attnum: -1,
        lp_len: 0,
        natts: 0,
        offset: 0,
        tuple_could_be_pruned: true,
        toasted_attributes: PgVec::new_in(mcx),
        is_corrupt: false,
    };

    update_cached_xid_range(&mut ctx);
    update_cached_mxid_range(&mut ctx)?;
    ctx.relfrozenxid = rel.rd_rel.relfrozenxid;
    ctx.relfrozenfxid = full_xid_from_xid_and_ctx(ctx.relfrozenxid, &ctx);
    ctx.relminmxid = rel.rd_rel.relminmxid;
    if TransactionIdIsNormal(ctx.relfrozenxid) {
        ctx.oldest_xid = ctx.relfrozenxid;
    }

    let mut predecessor = [0 as OffsetNumber; NSLOTS];
    let mut successor = [0 as OffsetNumber; NSLOTS];
    let mut lp_valid = [false; NSLOTS];
    let mut xmin_commit_status_ok = [false; NSLOTS];
    let mut xmin_commit_status = [XID_COMMITTED; NSLOTS];
    let mut pending: PgVec<ToastedAttribute> = PgVec::new_in(mcx);
    let mut vmbuf = VmBuffer::new();

    let mut blkno = first_block;
    'block: while blkno <= last_block {
        if skip_option != SKIP_PAGES_NONE {
            let mapbits = visibilitymap_get_status(&rel, blkno, &mut vmbuf)?;
            if skip_option == SKIP_PAGES_ALL_FROZEN && (mapbits & VISIBILITYMAP_ALL_FROZEN) != 0 {
                blkno += 1;
                continue;
            }
            if skip_option == SKIP_PAGES_ALL_VISIBLE && (mapbits & VISIBILITYMAP_ALL_VISIBLE) != 0 {
                blkno += 1;
                continue;
            }
        }

        postgres_seams::check_for_interrupts::call()?;

        let buffer: Buffer = bufmgr::ReadBufferExtended(
            &rel,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            ReadBufferMode::Normal,
            bstrategy.clone(),
        )?;
        bufmgr::LockBuffer(buffer, BUFFER_LOCK_SHARE)?;
        ctx.blkno = bufmgr::BufferGetBlockNumber(buffer);
        let page = bufmgr::buffer_page_ref(buffer);
        let maxoff = page.max_offset_number();

        for slot in predecessor.iter_mut().take(maxoff as usize + 1) {
            *slot = 0;
        }

        let mut off = FIRST_OFFSET_NUMBER;
        while off <= maxoff {
            ctx.offnum = off;
            successor[off as usize] = INVALID_OFFSET_NUMBER;
            lp_valid[off as usize] = false;
            xmin_commit_status_ok[off as usize] = false;
            let lp = page.item_id(off);

            if !lp.is_used() || lp.is_dead() {
                off = offset_number_next(off);
                continue;
            }

            if lp.is_redirected() {
                let rdoffnum = lp.lp_off();
                if rdoffnum < FIRST_OFFSET_NUMBER {
                    report(&mut ctx, &mut srf, format!(
                        "line pointer redirection to item at offset {rdoffnum} precedes minimum offset {FIRST_OFFSET_NUMBER}"
                    ))?;
                    off = offset_number_next(off);
                    continue;
                }
                if rdoffnum > maxoff {
                    report(&mut ctx, &mut srf, format!(
                        "line pointer redirection to item at offset {rdoffnum} exceeds maximum offset {maxoff}"
                    ))?;
                    off = offset_number_next(off);
                    continue;
                }
                let rditem = page.item_id(rdoffnum);
                if !rditem.is_used() {
                    report(&mut ctx, &mut srf, format!(
                        "redirected line pointer points to an unused item at offset {rdoffnum}"
                    ))?;
                    off = offset_number_next(off);
                    continue;
                } else if rditem.is_dead() {
                    report(&mut ctx, &mut srf, format!(
                        "redirected line pointer points to a dead item at offset {rdoffnum}"
                    ))?;
                    off = offset_number_next(off);
                    continue;
                } else if rditem.is_redirected() {
                    report(&mut ctx, &mut srf, format!(
                        "redirected line pointer points to another redirected line pointer at offset {rdoffnum}"
                    ))?;
                    off = offset_number_next(off);
                    continue;
                }
                lp_valid[off as usize] = true;
                successor[off as usize] = rdoffnum;
                off = offset_number_next(off);
                continue;
            }

            ctx.lp_len = lp.lp_len();
            let lp_off = lp.lp_off();
            let lp_len = lp.lp_len();

            if lp_off as u32 != maxalign(lp_off as u32) {
                report(&mut ctx, &mut srf, format!(
                    "line pointer to page offset {lp_off} is not maximally aligned"
                ))?;
                off = offset_number_next(off);
                continue;
            }
            if (lp_len as u32) < maxalign(SizeofHeapTupleHeader as u32) {
                report(&mut ctx, &mut srf, format!(
                    "line pointer length {lp_len} is less than the minimum tuple header size {}",
                    maxalign(SizeofHeapTupleHeader as u32)
                ))?;
                off = offset_number_next(off);
                continue;
            }
            if lp_off as usize + lp_len as usize > BLCKSZ {
                report(&mut ctx, &mut srf, format!(
                    "line pointer to page offset {lp_off} with length {lp_len} ends beyond maximum page offset {BLCKSZ}"
                ))?;
                off = offset_number_next(off);
                continue;
            }

            lp_valid[off as usize] = true;
            // SAFETY: lp_off/lp_len validated above; header within the page image.
            let tuphdr = unsafe { header_at(page, lp_off) };
            let item = unsafe { page.as_ptr().add(lp_off as usize) };
            ctx.natts = tuphdr.natts() as i32;

            let (ok, status) = check_tuple(&mut ctx, &mut srf, &rel, tuphdr, item)?;
            xmin_commit_status_ok[off as usize] = ok;
            xmin_commit_status[off as usize] = status;

            let nextblkno = ItemPointerGetBlockNumber(&tuphdr.t_ctid);
            let nextoffnum = ItemPointerGetOffsetNumberNoCheck(&tuphdr.t_ctid);
            if nextblkno == ctx.blkno
                && nextoffnum != ctx.offnum
                && nextoffnum >= FIRST_OFFSET_NUMBER
                && nextoffnum <= maxoff
            {
                successor[off as usize] = nextoffnum;
            }

            off = offset_number_next(off);
        }

        ctx.attnum = -1;
        let mut off = FIRST_OFFSET_NUMBER;
        while off <= maxoff {
            ctx.offnum = off;
            let nextoffnum = successor[off as usize];

            if nextoffnum == INVALID_OFFSET_NUMBER || !lp_valid[nextoffnum as usize] {
                off = offset_number_next(off);
                continue;
            }

            let curr_lp = page.item_id(off);
            let next_lp = page.item_id(nextoffnum);

            if curr_lp.is_redirected() {
                debug_assert!(next_lp.is_normal());
                // SAFETY: next_lp is a validated LP_NORMAL line pointer.
                let next_htup = unsafe { header_at(page, next_lp.lp_off()) };
                if !next_htup.is_heap_only() {
                    report(&mut ctx, &mut srf, format!(
                        "redirected line pointer points to a non-heap-only tuple at offset {nextoffnum}"
                    ))?;
                }
                if predecessor[nextoffnum as usize] != INVALID_OFFSET_NUMBER {
                    report(&mut ctx, &mut srf, format!(
                        "redirect line pointer points to offset {nextoffnum}, but offset {} also points there",
                        predecessor[nextoffnum as usize]
                    ))?;
                    off = offset_number_next(off);
                    continue;
                }
                predecessor[nextoffnum as usize] = off;
                off = offset_number_next(off);
                continue;
            }

            if next_lp.is_redirected() {
                off = offset_number_next(off);
                continue;
            }
            // SAFETY: curr_lp/next_lp are validated LP_NORMAL line pointers.
            let curr_htup = unsafe { header_at(page, curr_lp.lp_off()) };
            let next_htup = unsafe { header_at(page, next_lp.lp_off()) };
            let curr_xmax = heapam::HeapTupleHeaderGetUpdateXid(curr_htup)?;
            let next_xmin = next_htup.xmin();
            if !TransactionIdIsValid(curr_xmax) || !TransactionIdEquals(curr_xmax, next_xmin) {
                off = offset_number_next(off);
                continue;
            }

            if predecessor[nextoffnum as usize] != INVALID_OFFSET_NUMBER {
                report(&mut ctx, &mut srf, format!(
                    "tuple points to new version at offset {nextoffnum}, but offset {} also points there",
                    predecessor[nextoffnum as usize]
                ))?;
                off = offset_number_next(off);
                continue;
            }
            predecessor[nextoffnum as usize] = off;

            if (curr_htup.t_infomask2 & HEAP_HOT_UPDATED) == 0 && next_htup.is_heap_only() {
                report(&mut ctx, &mut srf, format!(
                    "non-heap-only update produced a heap-only tuple at offset {nextoffnum}"
                ))?;
            }
            if (curr_htup.t_infomask2 & HEAP_HOT_UPDATED) != 0 && !next_htup.is_heap_only() {
                report(&mut ctx, &mut srf, format!(
                    "heap-only update produced a non-heap only tuple at offset {nextoffnum}"
                ))?;
            }

            let curr_xmin = curr_htup.xmin();
            if xmin_commit_status_ok[off as usize]
                && xmin_commit_status[off as usize] == XID_IN_PROGRESS
                && xmin_commit_status_ok[nextoffnum as usize]
                && xmin_commit_status[nextoffnum as usize] == XID_COMMITTED
                && procarray::TransactionIdIsInProgress(curr_xmin)?
            {
                report(&mut ctx, &mut srf, format!(
                    "tuple with in-progress xmin {curr_xmin} was updated to produce a tuple at offset {off} with committed xmin {next_xmin}"
                ))?;
            }

            if xmin_commit_status_ok[off as usize]
                && xmin_commit_status[off as usize] == XID_ABORTED
                && xmin_commit_status_ok[nextoffnum as usize]
            {
                if xmin_commit_status[nextoffnum as usize] == XID_IN_PROGRESS {
                    report(&mut ctx, &mut srf, format!(
                        "tuple with aborted xmin {curr_xmin} was updated to produce a tuple at offset {off} with in-progress xmin {next_xmin}"
                    ))?;
                } else if xmin_commit_status[nextoffnum as usize] == XID_COMMITTED {
                    report(&mut ctx, &mut srf, format!(
                        "tuple with aborted xmin {curr_xmin} was updated to produce a tuple at offset {off} with committed xmin {next_xmin}"
                    ))?;
                }
            }

            off = offset_number_next(off);
        }

        let mut off = FIRST_OFFSET_NUMBER;
        while off <= maxoff {
            ctx.offnum = off;
            if xmin_commit_status_ok[off as usize]
                && (xmin_commit_status[off as usize] == XID_COMMITTED
                    || xmin_commit_status[off as usize] == XID_IN_PROGRESS)
                && predecessor[off as usize] == INVALID_OFFSET_NUMBER
            {
                let curr_lp = page.item_id(off);
                if !curr_lp.is_redirected() {
                    // SAFETY: curr_lp is a validated LP_NORMAL line pointer.
                    let curr_htup = unsafe { header_at(page, curr_lp.lp_off()) };
                    if curr_htup.is_heap_only() {
                        report(&mut ctx, &mut srf, "tuple is root of chain but is marked as heap-only tuple".to_string())?;
                    }
                }
            }
            off = offset_number_next(off);
        }

        bufmgr::UnlockReleaseBuffer(buffer)?;

        if !ctx.toasted_attributes.is_empty() {
            pending.clear();
            core::mem::swap(&mut pending, &mut ctx.toasted_attributes);
            if let (Some(tr), Some((idxs, vi))) = (toast_rel.as_ref(), toast_indexes.as_ref()) {
                for ta in pending.iter() {
                    check_toasted_attribute(&mut ctx, &mut srf, ta, tr, &idxs[*vi], have_snapshot)?;
                }
            }
            pending.clear();
        }

        if on_error_stop && ctx.is_corrupt {
            break 'block;
        }

        blkno += 1;
    }

    vmbuf.release();

    if let Some((idxs, _)) = toast_indexes {
        toast_close_indexes(idxs, AccessShareLock)?;
    }
    if let Some(tr) = toast_rel {
        table::table_close(tr, AccessShareLock)?;
    }
    rel.close(AccessShareLock)?;

    Ok(srf.finish(fcinfo))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_mcx() -> mcx::Mcx<'static> {
        Box::leak(Box::new(mcx::MemoryContext::new("amcheck-test"))).mcx()
    }

    fn synthetic_ctx(next: u32, oldest: u32, relfrozen: u32) -> HeapCheckContext<'static> {
        let mut ctx = HeapCheckContext {
            mcx: test_mcx(),
            next_fxid: FullTransactionId::from_epoch_and_xid(0, next),
            next_xid: next,
            oldest_xid: oldest,
            oldest_fxid: FullTransactionId::from_epoch_and_xid(0, oldest),
            safe_xmin: oldest,
            next_mxact: 100,
            oldest_mxact: 5,
            cached_xid: InvalidTransactionId,
            cached_status: XID_COMMITTED,
            relfrozenxid: relfrozen,
            relfrozenfxid: FullTransactionId::from_epoch_and_xid(0, relfrozen),
            relminmxid: 5,
            toast_rel_present: false,
            blkno: 0,
            offnum: 0,
            attnum: -1,
            lp_len: 0,
            natts: 0,
            offset: 0,
            tuple_could_be_pruned: true,
            toasted_attributes: PgVec::new_in(test_mcx()),
            is_corrupt: false,
        };
        ctx.oldest_fxid = full_xid_from_xid_and_ctx(oldest, &ctx);
        ctx
    }

    #[test]
    fn toast_max_chunk_size_is_1996() {
        assert_eq!(TOAST_MAX_CHUNK_SIZE, 1996);
    }

    #[test]
    fn xid_range_classification() {
        let mut ctx = synthetic_ctx(1000, 100, 200);

        let (b, s) = get_xid_status(500, &mut ctx, false).unwrap();
        assert!(b == XID_BOUNDS_OK);
        assert!(s.is_none());
        let (b, _) = get_xid_status(200, &mut ctx, false).unwrap();
        assert!(b == XID_BOUNDS_OK);

        let (b, _) = get_xid_status(InvalidTransactionId, &mut ctx, true).unwrap();
        assert!(b == XID_INVALID);
        let (b, s) = get_xid_status(FrozenTransactionId, &mut ctx, true).unwrap();
        assert!(b == XID_BOUNDS_OK && s == Some(XID_COMMITTED));
        let (b, s) = get_xid_status(BootstrapTransactionId, &mut ctx, true).unwrap();
        assert!(b == XID_BOUNDS_OK && s == Some(XID_COMMITTED));

        assert!(check_mxid_in_range(0, &ctx) == XID_INVALID);
        assert!(check_mxid_in_range(3, &ctx) == XID_PRECEDES_RELMIN);
        ctx.relminmxid = 10;
        assert!(check_mxid_in_range(7, &ctx) == XID_PRECEDES_RELMIN);
        ctx.relminmxid = 5;
        ctx.oldest_mxact = 20;
        assert!(check_mxid_in_range(7, &ctx) == XID_PRECEDES_CLUSTERMIN);
        ctx.oldest_mxact = 5;
        assert!(check_mxid_in_range(100, &ctx) == XID_IN_FUTURE);
        assert!(check_mxid_in_range(50, &ctx) == XID_BOUNDS_OK);
    }

    #[test]
    fn mxid_range_classification() {
        let ctx = synthetic_ctx(1000, 100, 200);
        assert!(check_mxid_in_range(0, &ctx) == XID_INVALID);
        assert!(check_mxid_in_range(3, &ctx) == XID_PRECEDES_RELMIN);
        assert!(check_mxid_in_range(100, &ctx) == XID_IN_FUTURE);
        assert!(check_mxid_in_range(50, &ctx) == XID_BOUNDS_OK);
    }

    #[test]
    fn skip_option_parsing() {
        let parse = |s: &str| -> Option<SkipPages> {
            if s.eq_ignore_ascii_case("all-visible") {
                Some(SKIP_PAGES_ALL_VISIBLE)
            } else if s.eq_ignore_ascii_case("all-frozen") {
                Some(SKIP_PAGES_ALL_FROZEN)
            } else if s.eq_ignore_ascii_case("none") {
                Some(SKIP_PAGES_NONE)
            } else {
                None
            }
        };
        assert!(parse("all-visible") == Some(SKIP_PAGES_ALL_VISIBLE));
        assert!(parse("ALL-FROZEN") == Some(SKIP_PAGES_ALL_FROZEN));
        assert!(parse("None") == Some(SKIP_PAGES_NONE));
        assert!(parse("bogus").is_none());
    }
}
