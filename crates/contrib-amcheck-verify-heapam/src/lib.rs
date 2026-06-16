#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

//! `contrib-amcheck-verify-heapam` — port of `contrib/amcheck/verify_heapam.c`.
//!
//! amcheck's heap verifier: the SQL-callable `verify_heapam` function scans a
//! heap relation block by block (under a shared content lock on each page),
//! checks every line pointer, tuple header, tuple visibility (xmin/xmax
//! against clog / multixact / the cached valid-xid range), HOT/update-chain
//! linkage, and per-attribute layout, and — optionally — reconciles each
//! external (TOASTed) attribute against the chunks stored in the TOAST table.
//! Every corruption found is appended to a materialized set-returning result
//! of `(blkno int8, offnum int4, attnum int4, msg text)` rows.
//!
//! The page is read as a private byte copy (`BufferGetPage`); page-format /
//! tuple-header / varlena fields are decoded with byte arithmetic mirroring
//! the C inline macros, exactly as the sibling `verify_nbtree` verifier does.

use mcx::Mcx;

use types_core::primitive::{
    AttrNumber, BlockNumber, ForkNumber, OffsetNumber, Oid,
};
use types_core::xact::{
    BootstrapTransactionId, FrozenTransactionId, FullTransactionId, MultiXactIdPrecedes,
    MultiXactIdPrecedesOrEquals, TransactionIdEquals, TransactionIdIsNormal,
    TransactionIdIsValid, TransactionIdPrecedes,
};
use types_error::error::{
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_READ_ONLY_SQL_TRANSACTION, ERRCODE_WRONG_OBJECT_TYPE,
};
use types_error::{DEBUG1, PgError, PgResult};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_rel::Relation;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::buf::BufferAccessStrategyType;
use types_storage::lock::AccessShareLock;
use types_storage::storage::{Buffer, InvalidBuffer};
use types_tuple::access::RELKIND_SEQUENCE;
use types_tuple::Datum;
use types_tuple::heaptuple::{
    HeapTupleHeaderData, HeapTupleHeaderGetNatts, HeapTupleHeaderGetXmin,
    HeapTupleHeaderXminCommitted, BITMAPLEN, HEAP_HASEXTERNAL, HEAP_HASNULL,
    HEAP_HOT_UPDATED, HEAP_MOVED_IN, HEAP_MOVED_OFF, HEAP_ONLY_TUPLE, HEAP_UPDATED,
    HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI, HEAP_XMAX_LOCK_ONLY,
};

use backend_access_common_scankey::ScanKeyInit;
use backend_access_heap_heapam_visibility::htup::{
    HeapTupleHeaderGetRawXmax, HeapTupleHeaderXminInvalid, HEAP_XMAX_IS_LOCKED_ONLY,
};

use backend_access_common_heaptuple as heaptuple;
use backend_access_common_toast_internals_seams as toast_internals;
use backend_access_index_genam_seams as genam;
use backend_access_transam_multixact_seams as multixact;
use backend_access_transam_transam_seams as transam;
use backend_access_transam_varsup_seams as varsup;
use backend_access_transam_xact_seams as xact;
use backend_access_transam_xlog_seams as xlog;
use backend_access_heap_visibilitymap_seams as visibilitymap;
use backend_storage_aio_read_stream as read_stream;
use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_storage_buffer_support as buffer_support;
use backend_storage_ipc_procarray_seams as procarray;
use backend_utils_adt_varlena_seams as varlena;
use backend_utils_error_elog_seams as elog;
use backend_utils_fmgr_funcapi_seams as funcapi;
use backend_utils_time_snapmgr_seams as snapmgr;

// ===========================================================================
// Constants (verify_heapam.c, and inlined macros it depends on)
// ===========================================================================

/// `HEAPCHECK_RELATION_COLS` — columns in tuples returned by verify_heapam.
const HEAPCHECK_RELATION_COLS: usize = 4;

/// `VARLENA_SIZE_LIMIT` — the largest valid toast `va_rawsize`.
const VARLENA_SIZE_LIMIT: i32 = 0x3FFFFFFF;

/// `TOAST_MAX_CHUNK_SIZE` (`access/heaptoast.h`): the largest chunk payload a
/// single toast-relation tuple holds. On the default 8 KB BLCKSZ with 4 toast
/// tuples per page this evaluates to 1996 (the same value the heaptoast unit
/// computes from `EXTERN_TUPLE_MAX_SIZE - MAXALIGN(SizeofHeapTupleHeader) -
/// sizeof(Oid) - sizeof(int32) - VARHDRSZ`).
const TOAST_MAX_CHUNK_SIZE: i32 = {
    // MAXALIGN_DOWN(LEN) = LEN & ~7.
    const fn maxalign_down(len: i32) -> i32 {
        len & !7
    }
    // EXTERN_TUPLES_PER_PAGE = 4; sizeof(ItemIdData) = 4; SizeOfPageHeaderData = 24.
    // EXTERN_TUPLE_MAX_SIZE = MaximumBytesPerTuple(4)
    //   = MAXALIGN_DOWN((BLCKSZ - MAXALIGN(SizeOfPageHeaderData + 4*sizeof(ItemIdData))) / 4)
    const EXTERN_TUPLE_MAX_SIZE: i32 =
        maxalign_down((BLCKSZ as i32 - maxalign(24 + 4 * 4) as i32) / 4);
    // TOAST_MAX_CHUNK_SIZE = EXTERN_TUPLE_MAX_SIZE - MAXALIGN(SizeofHeapTupleHeader)
    //   - sizeof(Oid) - sizeof(int32) - VARHDRSZ  (= 1996 on the default page).
    EXTERN_TUPLE_MAX_SIZE
        - maxalign(SizeofHeapTupleHeader) as i32
        - 4 // sizeof(Oid)
        - 4 // sizeof(int32)
        - VARHDRSZ as i32
};

/// `BLCKSZ` (`pg_config.h`).
const BLCKSZ: u32 = 8192;
/// `SizeOfPageHeaderData` (`storage/bufpage.h`).
const SIZE_OF_PAGE_HEADER_DATA: usize = 24;
/// `sizeof(ItemIdData)`.
const SIZEOF_ITEM_ID: usize = 4;

/// `MaxOffsetNumber` (`storage/off.h`).
const MaxOffsetNumber: usize = (BLCKSZ as usize) / SIZEOF_ITEM_ID;
/// `FirstOffsetNumber` (`storage/off.h`).
const FirstOffsetNumber: OffsetNumber = 1;
/// `InvalidOffsetNumber` (`storage/off.h`).
const InvalidOffsetNumber: OffsetNumber = 0;

/// `SizeofHeapTupleHeader` (`access/htup_details.h`).
const SizeofHeapTupleHeader: u32 = 23;

/// `HEAP_TABLE_AM_OID` (`catalog/pg_am_d.h`).
const HEAP_TABLE_AM_OID: Oid = 2;

/// `F_OIDEQ` (`catalog/fmgroids.h`).
const F_OIDEQ: types_core::primitive::RegProcedure = types_core::fmgr::F_OIDEQ;

/// `BUFFER_LOCK_SHARE` (`storage/bufmgr.h`).
const BUFFER_LOCK_SHARE: i32 = types_storage::buf::BUFFER_LOCK_SHARE;

/// `VISIBILITYMAP_ALL_VISIBLE` / `VISIBILITYMAP_ALL_FROZEN`.
const VISIBILITYMAP_ALL_VISIBLE: u8 = 0x01;
const VISIBILITYMAP_ALL_FROZEN: u8 = 0x02;

/// `VARHDRSZ` (`c.h`).
const VARHDRSZ: usize = 4;
/// `VARHDRSZ_EXTERNAL` (`varatt.h`).
const VARHDRSZ_EXTERNAL: usize = 2;
/// `VARHDRSZ_SHORT` (`varatt.h`).
const VARHDRSZ_SHORT: usize = 1;
/// `VARTAG_ONDISK` (`varatt.h`).
const VARTAG_ONDISK: u8 = 18;

const VARLENA_EXTSIZE_BITS: u32 = 30;
const VARLENA_EXTSIZE_MASK: u32 = (1 << VARLENA_EXTSIZE_BITS) - 1;

/// Toast compression method IDs (`access/toast_compression.h`).
const TOAST_PGLZ_COMPRESSION_ID: u32 = 0;
const TOAST_LZ4_COMPRESSION_ID: u32 = 1;
const TOAST_INVALID_COMPRESSION_ID: u32 = 2;

/// `RELKIND_HAS_TABLE_AM(relkind)` (`catalog/pg_class.h`).
fn relkind_has_table_am(relkind: u8) -> bool {
    use types_tuple::access::{RELKIND_MATVIEW, RELKIND_RELATION, RELKIND_TOASTVALUE};
    relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW || relkind == RELKIND_TOASTVALUE
}

/// `MAXALIGN(LEN)` (`c.h`).
#[inline]
const fn maxalign(len: u32) -> u32 {
    (len + 7) & !7
}

// ===========================================================================
// XID-bounds / commit-status / skip enums (verify_heapam.c)
// ===========================================================================

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

// ===========================================================================
// varatt_external + ToastedAttribute (verify_heapam.c)
// ===========================================================================

/// `struct varatt_external` (`varatt.h`) — the on-disk TOAST-pointer payload.
#[derive(Clone, Copy, Default)]
struct VarattExternal {
    va_rawsize: i32,
    va_extinfo: u32,
    va_valueid: Oid,
    #[allow(dead_code)]
    va_toastrelid: Oid,
}

impl VarattExternal {
    /// `VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr)` — copy the 16-byte
    /// struct out of an external datum's payload (after the 2-byte external
    /// `va_header` + `va_tag`).
    fn from_external(attr: &[u8]) -> Self {
        let p = &attr[VARHDRSZ_EXTERNAL..];
        let rd32 = |o: usize| u32::from_ne_bytes([p[o], p[o + 1], p[o + 2], p[o + 3]]);
        VarattExternal {
            va_rawsize: rd32(0) as i32,
            va_extinfo: rd32(4),
            va_valueid: rd32(8),
            va_toastrelid: rd32(12),
        }
    }

    /// `VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer)`.
    fn extsize(&self) -> u32 {
        self.va_extinfo & VARLENA_EXTSIZE_MASK
    }

    /// `VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer)`.
    fn is_compressed(&self) -> bool {
        self.extsize() < (self.va_rawsize as u32 - VARHDRSZ as u32)
    }

    /// `TOAST_COMPRESS_METHOD(&toast_pointer)` /
    /// `VARATT_EXTERNAL_GET_COMPRESS_METHOD`.
    fn compress_method(&self) -> u32 {
        self.va_extinfo >> VARLENA_EXTSIZE_BITS
    }
}

/// `struct ToastedAttribute` — info about a toasted attribute sufficient to
/// both check it and report where it was found in the main table.
struct ToastedAttribute {
    toast_pointer: VarattExternal,
    blkno: BlockNumber,
    offnum: OffsetNumber,
    attnum: AttrNumber,
}

// ===========================================================================
// HeapCheckContext (verify_heapam.c)
// ===========================================================================

/// `struct HeapCheckContext` — running context for a verify_heapam execution.
/// The C struct's tuplestore/tupdesc live in the `ReturnSetInfo` here; result
/// rows are appended through `report_corruption`, which takes `&mut
/// ReturnSetInfo` separately so the borrow checker permits it alongside `ctx`.
struct HeapCheckContext<'mcx> {
    mcx: Mcx<'mcx>,

    // Cached transaction-id range.
    next_fxid: FullTransactionId,
    next_xid: types_core::TransactionId,
    oldest_xid: types_core::TransactionId,
    oldest_fxid: FullTransactionId,
    safe_xmin: types_core::TransactionId,

    // Cached multixact range.
    next_mxact: types_core::MultiXactId,
    oldest_mxact: types_core::MultiXactId,

    // Most recently checked xid + status.
    cached_xid: types_core::TransactionId,
    cached_status: XidCommitStatus,

    // The heap relation being checked. Stored as an owned alias handle (shares
    // the relcache cell, releases nothing on drop) so the context is not tied
    // to a borrow of the function-local `rel`.
    rel: Relation<'mcx>,
    relfrozenxid: types_core::TransactionId,
    relfrozenfxid: FullTransactionId,
    relminmxid: types_core::MultiXactId,
    toast_rel: Option<Relation<'mcx>>,

    // Page iteration. `page` is a private byte copy of the locked buffer.
    blkno: BlockNumber,
    page: alloc::vec::Vec<u8>,

    // Tuple-within-page iteration.
    offnum: OffsetNumber,
    lp_len: u16,
    lp_off: u16,
    tuphdr: HeapTupleHeaderData<'mcx>,
    natts: i32,

    // Attribute-within-tuple iteration.
    offset: u32,
    attnum: AttrNumber,

    // True if tuple's xmax makes it eligible for pruning.
    tuple_could_be_pruned: bool,

    // Toasted attributes pending check.
    toasted_attributes: alloc::vec::Vec<ToastedAttribute>,

    // Whether any corruption has been seen.
    is_corrupt: bool,
}

extern crate alloc;

// ===========================================================================
// Page / item-id / tuple-header byte decode
// ===========================================================================

/// `PageGetMaxOffsetNumber(page)` (bufpage.h).
fn page_get_max_offset_number(page: &[u8]) -> OffsetNumber {
    let pd_lower = u16::from_ne_bytes([page[12], page[13]]) as usize;
    if pd_lower <= SIZE_OF_PAGE_HEADER_DATA {
        0
    } else {
        ((pd_lower - SIZE_OF_PAGE_HEADER_DATA) / SIZEOF_ITEM_ID) as OffsetNumber
    }
}

/// The raw 32-bit `ItemIdData` word for the 1-based `offnum`
/// (`PageGetItemId(page, offnum)`); the line-pointer array starts at
/// `SizeOfPageHeaderData`.
fn item_id_word(page: &[u8], offnum: OffsetNumber) -> u32 {
    let o = SIZE_OF_PAGE_HEADER_DATA + (offnum as usize - 1) * SIZEOF_ITEM_ID;
    u32::from_ne_bytes([page[o], page[o + 1], page[o + 2], page[o + 3]])
}

const LP_UNUSED: u8 = 0;
const LP_NORMAL: u8 = 1;
const LP_REDIRECT: u8 = 2;
const LP_DEAD: u8 = 3;

/// `ItemIdData` bit-fields (`storage/itemid.h`): `lp_off:15, lp_flags:2,
/// lp_len:15`.
struct ItemId {
    lp_off: u16,
    lp_flags: u8,
    lp_len: u16,
}

fn item_id(page: &[u8], offnum: OffsetNumber) -> ItemId {
    let w = item_id_word(page, offnum);
    ItemId {
        lp_off: (w & 0x7FFF) as u16,
        lp_flags: ((w >> 15) & 0x3) as u8,
        lp_len: ((w >> 17) & 0x7FFF) as u16,
    }
}

impl ItemId {
    fn is_used(&self) -> bool {
        self.lp_flags != LP_UNUSED
    }
    fn is_normal(&self) -> bool {
        self.lp_flags == LP_NORMAL
    }
    fn is_redirected(&self) -> bool {
        self.lp_flags == LP_REDIRECT
    }
    fn is_dead(&self) -> bool {
        self.lp_flags == LP_DEAD
    }
    /// `ItemIdGetRedirect(itemId)`.
    fn get_redirect(&self) -> OffsetNumber {
        self.lp_off
    }
}

/// `OffsetNumberNext(offsetNumber)`.
#[inline]
fn offset_number_next(offnum: OffsetNumber) -> OffsetNumber {
    offnum + 1
}

/// `(HeapTupleHeader) PageGetItem(page, itemid)` — the tuple's on-page bytes.
fn page_get_item(page: &[u8], lp_off: u16, lp_len: u16) -> &[u8] {
    &page[lp_off as usize..(lp_off as usize + lp_len as usize)]
}

/// `HeapTupleHeaderIsHeapOnly(tup)` — `t_infomask2 & HEAP_ONLY_TUPLE`.
fn is_heap_only(tuphdr: &HeapTupleHeaderData) -> bool {
    (tuphdr.t_infomask2 & HEAP_ONLY_TUPLE) != 0
}

/// `HeapTupleHeaderIsHotUpdated(tup)` (htup_details.h).
fn is_hot_updated(tuphdr: &HeapTupleHeaderData) -> bool {
    (tuphdr.t_infomask2 & HEAP_HOT_UPDATED) != 0
        && (tuphdr.t_infomask & HEAP_XMAX_INVALID) == 0
        && !HeapTupleHeaderXminInvalid(tuphdr)
}

/// `HeapTupleHeaderGetUpdateXid(tup)` (htup_details.h) — the update xid,
/// resolving a multixact xmax via the multixact owner.
fn header_get_update_xid(
    tuphdr: &HeapTupleHeaderData,
) -> PgResult<types_core::TransactionId> {
    if (tuphdr.t_infomask & HEAP_XMAX_INVALID) == 0
        && (tuphdr.t_infomask & HEAP_XMAX_IS_MULTI) != 0
        && (tuphdr.t_infomask & HEAP_XMAX_LOCK_ONLY) == 0
    {
        multixact::multi_xact_id_get_update_xid::call(
            HeapTupleHeaderGetRawXmax(tuphdr),
            tuphdr.t_infomask,
        )
    } else {
        Ok(HeapTupleHeaderGetRawXmax(tuphdr))
    }
}

/// `HeapTupleGetUpdateXid(tup)` — `MultiXactIdGetUpdateXid(GetRawXmax,
/// t_infomask)`.
fn tuple_get_update_xid(
    tuphdr: &HeapTupleHeaderData,
) -> PgResult<types_core::TransactionId> {
    multixact::multi_xact_id_get_update_xid::call(
        HeapTupleHeaderGetRawXmax(tuphdr),
        tuphdr.t_infomask,
    )
}

/// `HeapTupleHeaderGetXvac(tup)`.
fn header_get_xvac(tuphdr: &HeapTupleHeaderData) -> types_core::TransactionId {
    backend_access_heap_heapam_visibility::htup::HeapTupleHeaderGetXvac(tuphdr)
}

/// `ItemPointerGetBlockNumber(&t_ctid)`.
fn ctid_block(tuphdr: &HeapTupleHeaderData) -> BlockNumber {
    let bi = &tuphdr.t_ctid.ip_blkid;
    ((bi.bi_hi as u32) << 16) | bi.bi_lo as u32
}

/// `ItemPointerGetOffsetNumber(&t_ctid)`.
fn ctid_offset(tuphdr: &HeapTupleHeaderData) -> OffsetNumber {
    tuphdr.t_ctid.ip_posid
}

/// `att_isnull(ATT, BITS)` (`access/tupmacs.h`): a clear bit means NULL.
#[inline]
fn att_isnull(att: usize, bits: &[u8]) -> bool {
    (bits[att >> 3] & (1 << (att & 7))) == 0
}

// ===========================================================================
// Epoch helpers for FullTransactionId
// ===========================================================================

#[inline]
fn epoch_of(f: FullTransactionId) -> u32 {
    f.epoch()
}
#[inline]
fn xid_of(f: FullTransactionId) -> types_core::TransactionId {
    f.xid()
}

/// `FullTransactionIdFromXidAndCtx(xid, ctx)` — convert a 32-bit xid to full,
/// tolerating an xid before epoch 0 (a form of corruption).
fn full_xid_from_xid_and_ctx(
    xid: types_core::TransactionId,
    ctx: &HeapCheckContext,
) -> FullTransactionId {
    debug_assert!(TransactionIdIsNormal(ctx.next_xid));
    debug_assert!(ctx.next_fxid.is_normal());
    debug_assert!(ctx.next_fxid.xid() == ctx.next_xid);

    if !TransactionIdIsNormal(xid) {
        return FullTransactionId::from_epoch_and_xid(0, xid);
    }

    let nextfxid_i = ctx.next_fxid.to_u64();
    let diff = ctx.next_xid.wrapping_sub(xid) as i32;

    let fxid = if diff > 0
        && (nextfxid_i - types_core::xact::FirstNormalTransactionId as u64)
            < diff as i64 as u64
    {
        debug_assert!(ctx.next_fxid.epoch() == 0);
        types_core::xact::FirstNormalFullTransactionId
    } else {
        FullTransactionId::from_u64(nextfxid_i.wrapping_sub(diff as i64 as u64))
    };

    debug_assert!(fxid.is_normal());
    fxid
}

/// `update_cached_xid_range(ctx)`.
fn update_cached_xid_range(ctx: &mut HeapCheckContext) {
    // XidGenLock is held inside the varsup getters (TransamVariables reads).
    ctx.next_fxid = varsup::read_next_full_transaction_id::call();
    ctx.oldest_xid = varsup::get_oldest_xid::call();
    ctx.next_xid = ctx.next_fxid.xid();
    ctx.oldest_fxid = full_xid_from_xid_and_ctx(ctx.oldest_xid, ctx);
}

/// `update_cached_mxid_range(ctx)`.
fn update_cached_mxid_range(ctx: &mut HeapCheckContext) -> PgResult<()> {
    let (oldest, next) = multixact::read_multi_xact_id_range::call()?;
    ctx.oldest_mxact = oldest;
    ctx.next_mxact = next;
    Ok(())
}

/// `fxid_in_cached_range(fxid, ctx)`.
fn fxid_in_cached_range(fxid: FullTransactionId, ctx: &HeapCheckContext) -> bool {
    ctx.oldest_fxid.precedes_or_equals(fxid) && fxid.precedes(ctx.next_fxid)
}

/// `check_mxid_in_range(mxid, ctx)`.
fn check_mxid_in_range(
    mxid: types_core::MultiXactId,
    ctx: &HeapCheckContext,
) -> XidBoundsViolation {
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

/// `check_mxid_valid_in_rel(mxid, ctx)`.
fn check_mxid_valid_in_rel(
    mxid: types_core::MultiXactId,
    ctx: &mut HeapCheckContext,
) -> PgResult<XidBoundsViolation> {
    let result = check_mxid_in_range(mxid, ctx);
    if result == XID_BOUNDS_OK {
        return Ok(XID_BOUNDS_OK);
    }
    update_cached_mxid_range(ctx)?;
    Ok(check_mxid_in_range(mxid, ctx))
}

/// `get_xid_status(xid, ctx, status)`.
fn get_xid_status(
    xid: types_core::TransactionId,
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

    // XactTruncationLock is held inside get_oldest_clog_xid against concurrent
    // clog truncation.
    let clog_horizon = full_xid_from_xid_and_ctx(varsup::get_oldest_clog_xid::call(), ctx);
    let mut status = XID_COMMITTED;
    if clog_horizon.precedes_or_equals(fxid) {
        if xact::transaction_id_is_current_transaction_id::call(xid) {
            status = XID_IS_CURRENT_XID;
        } else if procarray::transaction_id_is_in_progress::call(xid)? {
            status = XID_IN_PROGRESS;
        } else if transam::transaction_id_did_commit::call(xid, ctx.safe_xmin)? {
            status = XID_COMMITTED;
        } else {
            status = XID_ABORTED;
        }
    }
    ctx.cached_xid = xid;
    ctx.cached_status = status;
    Ok((XID_BOUNDS_OK, Some(status)))
}

// ===========================================================================
// Corruption reporting
// ===========================================================================

/// `report_corruption(ctx, msg)` / `report_corruption_internal(...)`. Append a
/// `(blkno, offnum, attnum, msg)` row to the materialized result set and mark
/// the context corrupt.
fn report_corruption<'mcx>(
    rsinfo: &mut types_nodes::funcapi::ReturnSetInfo<'mcx>,
    is_corrupt: &mut bool,
    mcx: Mcx<'mcx>,
    blkno: BlockNumber,
    offnum: OffsetNumber,
    attnum: AttrNumber,
    msg: String,
) -> PgResult<()> {
    report_corruption_internal(rsinfo, mcx, blkno, offnum, attnum, msg)?;
    *is_corrupt = true;
    Ok(())
}

fn report_corruption_internal<'mcx>(
    rsinfo: &mut types_nodes::funcapi::ReturnSetInfo<'mcx>,
    mcx: Mcx<'mcx>,
    blkno: BlockNumber,
    offnum: OffsetNumber,
    attnum: AttrNumber,
    msg: String,
) -> PgResult<()> {
    let mut nulls = [false; HEAPCHECK_RELATION_COLS];
    // values[2] = Int32GetDatum(attnum); nulls[2] = (attnum < 0)
    nulls[2] = attnum < 0;
    let values = [
        Datum::from_i64(blkno as i64),                  // [0] blkno
        Datum::from_i32(offnum as i32),                 // [1] offnum
        Datum::from_i32(attnum as i32),                 // [2] attnum
        varlena::cstring_to_text_v::call(mcx, &msg)?,   // [3] msg (text)
    ];
    funcapi::materialized_srf_putvalues::call(rsinfo, &values, &nulls)
}

/// `report_corruption(ctx, psprintf(...))` — the main-table reporting wrapper:
/// reads the current location off `ctx`, appends the row, and marks `ctx`
/// corrupt. The `mcx` and the location fields are `Copy`, so this does not
/// conflict with the `&mut ctx` callers also hold.
fn report<'mcx>(ctx: &mut HeapCheckContext<'mcx>, rsinfo: &mut types_nodes::funcapi::ReturnSetInfo<'mcx>, msg: String) -> PgResult<()> {
    let (mcx, blkno, offnum, attnum) = (ctx.mcx, ctx.blkno, ctx.offnum, ctx.attnum);
    report_corruption(rsinfo, &mut ctx.is_corrupt, mcx, blkno, offnum, attnum, msg)
}

/// `report_toast_corruption(ctx, ta, msg)` — like [`report`] but the reported
/// location is the main-table site recorded in the [`ToastedAttribute`].
fn report_toast<'mcx>(
    ctx: &mut HeapCheckContext<'mcx>,
    rsinfo: &mut types_nodes::funcapi::ReturnSetInfo<'mcx>,
    ta: &ToastedAttribute,
    msg: String,
) -> PgResult<()> {
    let mcx = ctx.mcx;
    report_corruption(rsinfo, &mut ctx.is_corrupt, mcx, ta.blkno, ta.offnum, ta.attnum, msg)
}

// ===========================================================================
// check_tuple_header (verify_heapam.c)
// ===========================================================================

/// `check_tuple_header(ctx)` — header corruption checks. Returns whether the
/// tuple is sufficiently sensible to undergo visibility and attribute checks.
fn check_tuple_header<'mcx>(
    ctx: &mut HeapCheckContext<'mcx>,
    rsinfo: &mut types_nodes::funcapi::ReturnSetInfo<'mcx>,
) -> PgResult<bool> {
    let tuphdr = ctx.tuphdr.clone_in(ctx.mcx)?;
    let infomask = tuphdr.t_infomask;
    let curr_xmax = header_get_update_xid(&tuphdr)?;
    let mut result = true;

    if tuphdr.t_hoff as u16 > ctx.lp_len {
        report(ctx, rsinfo, format!(
            "data begins at offset {} beyond the tuple length {}",
            tuphdr.t_hoff, ctx.lp_len
        ))?;
        result = false;
    }

    if (infomask & HEAP_XMAX_COMMITTED) != 0 && (infomask & HEAP_XMAX_IS_MULTI) != 0 {
        report(ctx, rsinfo, "multixact should not be marked committed".to_string())?;
        // Clearly wrong, but not enough to skip further checks.
    }

    if !TransactionIdIsValid(curr_xmax) && is_hot_updated(&tuphdr) {
        report(ctx, rsinfo, "tuple has been HOT updated, but xmax is 0".to_string())?;
    }

    if is_heap_only(&tuphdr) && (infomask & HEAP_UPDATED) == 0 {
        report(ctx, rsinfo, "tuple is heap only, but not the result of an update".to_string())?;
    }

    let expected_hoff = if (infomask & HEAP_HASNULL) != 0 {
        maxalign(SizeofHeapTupleHeader + BITMAPLEN(ctx.natts) as u32)
    } else {
        maxalign(SizeofHeapTupleHeader)
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
        report(ctx, rsinfo, msg)?;
        result = false;
    }

    Ok(result)
}

// ===========================================================================
// check_tuple_visibility (verify_heapam.c)
// ===========================================================================

/// `check_tuple_visibility(ctx, xmin_commit_status_ok, xmin_commit_status)` —
/// returns `(checkable, xmin_status_ok, xmin_status)`.
fn check_tuple_visibility<'mcx>(
    ctx: &mut HeapCheckContext<'mcx>,
    rsinfo: &mut types_nodes::funcapi::ReturnSetInfo<'mcx>,
) -> PgResult<(bool, bool, XidCommitStatus)> {
    let tuphdr = ctx.tuphdr.clone_in(ctx.mcx)?;

    ctx.tuple_could_be_pruned = true; // have not yet proven otherwise
    let mut xmin_commit_status_ok = false;
    let mut xmin_commit_status = XID_COMMITTED;

    // If xmin is normal, it should be within valid range.
    let xmin = HeapTupleHeaderGetXmin(&tuphdr);
    let (xmin_bound, xmin_status) = get_xid_status(xmin, ctx, true)?;
    match xmin_bound {
        XID_INVALID => {
            // Could be the result of a speculative insertion that aborted.
            return Ok((false, false, XID_COMMITTED));
        }
        XID_BOUNDS_OK => {
            xmin_commit_status_ok = true;
            xmin_commit_status = xmin_status.expect("XID_BOUNDS_OK with status request");
        }
        XID_IN_FUTURE => {
            report(ctx, rsinfo, format!(
                "xmin {xmin} equals or exceeds next valid transaction ID {}:{}",
                epoch_of(ctx.next_fxid), xid_of(ctx.next_fxid)
            ))?;
            return Ok((false, false, XID_COMMITTED));
        }
        XID_PRECEDES_CLUSTERMIN => {
            report(ctx, rsinfo, format!(
                "xmin {xmin} precedes oldest valid transaction ID {}:{}",
                epoch_of(ctx.oldest_fxid), xid_of(ctx.oldest_fxid)
            ))?;
            return Ok((false, false, XID_COMMITTED));
        }
        XID_PRECEDES_RELMIN => {
            report(ctx, rsinfo, format!(
                "xmin {xmin} precedes relation freeze threshold {}:{}",
                epoch_of(ctx.relfrozenfxid), xid_of(ctx.relfrozenfxid)
            ))?;
            return Ok((false, false, XID_COMMITTED));
        }
    }
    let xmin_status = xmin_status.expect("XID_BOUNDS_OK with status request");

    // Has inserting transaction committed?
    if !HeapTupleHeaderXminCommitted(&tuphdr) {
        if HeapTupleHeaderXminInvalid(&tuphdr) {
            return Ok((false, xmin_commit_status_ok, xmin_commit_status)); // inserter aborted
        } else if (tuphdr.t_infomask & HEAP_MOVED_OFF) != 0 {
            // Used by pre-9.0 binary upgrades.
            let xvac = header_get_xvac(&tuphdr);
            let (bound, xvac_status) = get_xid_status(xvac, ctx, true)?;
            match bound {
                XID_INVALID => {
                    report(ctx, rsinfo, "old-style VACUUM FULL transaction ID for moved off tuple is invalid".to_string())?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_IN_FUTURE => {
                    report(ctx, rsinfo, format!("old-style VACUUM FULL transaction ID {xvac} for moved off tuple equals or exceeds next valid transaction ID {}:{}", epoch_of(ctx.next_fxid), xid_of(ctx.next_fxid)))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_PRECEDES_RELMIN => {
                    report(ctx, rsinfo, format!("old-style VACUUM FULL transaction ID {xvac} for moved off tuple precedes relation freeze threshold {}:{}", epoch_of(ctx.relfrozenfxid), xid_of(ctx.relfrozenfxid)))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_PRECEDES_CLUSTERMIN => {
                    report(ctx, rsinfo, format!("old-style VACUUM FULL transaction ID {xvac} for moved off tuple precedes oldest valid transaction ID {}:{}", epoch_of(ctx.oldest_fxid), xid_of(ctx.oldest_fxid)))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_BOUNDS_OK => {}
            }
            match xvac_status.expect("XID_BOUNDS_OK status") {
                XID_IS_CURRENT_XID => {
                    report(ctx, rsinfo, format!("old-style VACUUM FULL transaction ID {xvac} for moved off tuple matches our current transaction ID"))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_IN_PROGRESS => {
                    report(ctx, rsinfo, format!("old-style VACUUM FULL transaction ID {xvac} for moved off tuple appears to be in progress"))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_COMMITTED => {
                    // The tuple is dead (xvac moved it off and committed):
                    // checkable, but also prunable.
                    return Ok((true, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_ABORTED => {
                    // Original xmin must have committed; aliveness depends on xmax.
                }
            }
        } else if (tuphdr.t_infomask & HEAP_MOVED_IN) != 0 {
            // Used by pre-9.0 binary upgrades.
            let xvac = header_get_xvac(&tuphdr);
            let (bound, xvac_status) = get_xid_status(xvac, ctx, true)?;
            match bound {
                XID_INVALID => {
                    report(ctx, rsinfo, "old-style VACUUM FULL transaction ID for moved in tuple is invalid".to_string())?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_IN_FUTURE => {
                    report(ctx, rsinfo, format!("old-style VACUUM FULL transaction ID {xvac} for moved in tuple equals or exceeds next valid transaction ID {}:{}", epoch_of(ctx.next_fxid), xid_of(ctx.next_fxid)))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_PRECEDES_RELMIN => {
                    report(ctx, rsinfo, format!("old-style VACUUM FULL transaction ID {xvac} for moved in tuple precedes relation freeze threshold {}:{}", epoch_of(ctx.relfrozenfxid), xid_of(ctx.relfrozenfxid)))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_PRECEDES_CLUSTERMIN => {
                    report(ctx, rsinfo, format!("old-style VACUUM FULL transaction ID {xvac} for moved in tuple precedes oldest valid transaction ID {}:{}", epoch_of(ctx.oldest_fxid), xid_of(ctx.oldest_fxid)))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_BOUNDS_OK => {}
            }
            match xvac_status.expect("XID_BOUNDS_OK status") {
                XID_IS_CURRENT_XID => {
                    report(ctx, rsinfo, format!("old-style VACUUM FULL transaction ID {xvac} for moved in tuple matches our current transaction ID"))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_IN_PROGRESS => {
                    report(ctx, rsinfo, format!("old-style VACUUM FULL transaction ID {xvac} for moved in tuple appears to be in progress"))?;
                    return Ok((false, xmin_commit_status_ok, xmin_commit_status));
                }
                XID_COMMITTED => {
                    // Original xmin must have committed; aliveness depends on xmax.
                }
                XID_ABORTED => {
                    // Tuple is dead (xvac moved it off and committed):
                    // checkable, but also prunable.
                    return Ok((true, xmin_commit_status_ok, xmin_commit_status));
                }
            }
        } else if xmin_status != XID_COMMITTED {
            // Inserting transaction not in progress and not committed: it might
            // have changed the TupleDesc, so don't check the tuple structure.
            return Ok((false, xmin_commit_status_ok, xmin_commit_status));
        }
    }

    // Inserter committed. What about the deleting transaction?
    if (tuphdr.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        let xmax = HeapTupleHeaderGetRawXmax(&tuphdr);
        match check_mxid_valid_in_rel(xmax, ctx)? {
            XID_INVALID => {
                report(ctx, rsinfo, "multitransaction ID is invalid".to_string())?;
                return Ok((true, xmin_commit_status_ok, xmin_commit_status));
            }
            XID_PRECEDES_RELMIN => {
                report(ctx, rsinfo, format!("multitransaction ID {xmax} precedes relation minimum multitransaction ID threshold {}", ctx.relminmxid))?;
                return Ok((true, xmin_commit_status_ok, xmin_commit_status));
            }
            XID_PRECEDES_CLUSTERMIN => {
                report(ctx, rsinfo, format!("multitransaction ID {xmax} precedes oldest valid multitransaction ID threshold {}", ctx.oldest_mxact))?;
                return Ok((true, xmin_commit_status_ok, xmin_commit_status));
            }
            XID_IN_FUTURE => {
                report(ctx, rsinfo, format!("multitransaction ID {xmax} equals or exceeds next valid multitransaction ID {}", ctx.next_mxact))?;
                return Ok((true, xmin_commit_status_ok, xmin_commit_status));
            }
            XID_BOUNDS_OK => {}
        }
    }

    if (tuphdr.t_infomask & HEAP_XMAX_INVALID) != 0 {
        // Live tuple; a concurrent deleter is surely >= safe_xmin, so the toast
        // cannot be vacuumed out from under us.
        ctx.tuple_could_be_pruned = false;
        return Ok((true, xmin_commit_status_ok, xmin_commit_status));
    }

    if HEAP_XMAX_IS_LOCKED_ONLY(tuphdr.t_infomask) {
        ctx.tuple_could_be_pruned = false;
        return Ok((true, xmin_commit_status_ok, xmin_commit_status));
    }

    if (tuphdr.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        // We already checked the multixact is in range. Check the update xid.
        let xmax = tuple_get_update_xid(&tuphdr)?;
        let (bound, xmax_status) = get_xid_status(xmax, ctx, true)?;
        match bound {
            XID_INVALID => {
                report(ctx, rsinfo, "update xid is invalid".to_string())?;
                return Ok((true, xmin_commit_status_ok, xmin_commit_status));
            }
            XID_IN_FUTURE => {
                report(ctx, rsinfo, format!("update xid {xmax} equals or exceeds next valid transaction ID {}:{}", epoch_of(ctx.next_fxid), xid_of(ctx.next_fxid)))?;
                return Ok((true, xmin_commit_status_ok, xmin_commit_status));
            }
            XID_PRECEDES_RELMIN => {
                report(ctx, rsinfo, format!("update xid {xmax} precedes relation freeze threshold {}:{}", epoch_of(ctx.relfrozenfxid), xid_of(ctx.relfrozenfxid)))?;
                return Ok((true, xmin_commit_status_ok, xmin_commit_status));
            }
            XID_PRECEDES_CLUSTERMIN => {
                report(ctx, rsinfo, format!("update xid {xmax} precedes oldest valid transaction ID {}:{}", epoch_of(ctx.oldest_fxid), xid_of(ctx.oldest_fxid)))?;
                return Ok((true, xmin_commit_status_ok, xmin_commit_status));
            }
            XID_BOUNDS_OK => {}
        }
        match xmax_status.expect("XID_BOUNDS_OK status") {
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
        // Tuple itself is checkable even if dead.
        return Ok((true, xmin_commit_status_ok, xmin_commit_status));
    }

    // xmax is an XID, not a MXID. Sanity check it.
    let xmax = HeapTupleHeaderGetRawXmax(&tuphdr);
    let (bound, xmax_status) = get_xid_status(xmax, ctx, true)?;
    match bound {
        XID_INVALID => {
            ctx.tuple_could_be_pruned = false;
            return Ok((true, xmin_commit_status_ok, xmin_commit_status));
        }
        XID_IN_FUTURE => {
            report(ctx, rsinfo, format!("xmax {xmax} equals or exceeds next valid transaction ID {}:{}", epoch_of(ctx.next_fxid), xid_of(ctx.next_fxid)))?;
            return Ok((false, xmin_commit_status_ok, xmin_commit_status)); // corrupt
        }
        XID_PRECEDES_RELMIN => {
            report(ctx, rsinfo, format!("xmax {xmax} precedes relation freeze threshold {}:{}", epoch_of(ctx.relfrozenfxid), xid_of(ctx.relfrozenfxid)))?;
            return Ok((false, xmin_commit_status_ok, xmin_commit_status));
        }
        XID_PRECEDES_CLUSTERMIN => {
            report(ctx, rsinfo, format!("xmax {xmax} precedes oldest valid transaction ID {}:{}", epoch_of(ctx.oldest_fxid), xid_of(ctx.oldest_fxid)))?;
            return Ok((false, xmin_commit_status_ok, xmin_commit_status));
        }
        XID_BOUNDS_OK => {}
    }

    match xmax_status.expect("XID_BOUNDS_OK status") {
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

// ===========================================================================
// varlena / attribute byte decode (varatt.h, tupmacs.h)
// ===========================================================================

/// `TYPEALIGN(ALIGNVAL, LEN)`.
#[inline]
fn type_align(alignval: usize, len: usize) -> usize {
    (len + (alignval - 1)) & !(alignval - 1)
}

/// `att_nominal_alignby(cur_offset, attalignby)`.
#[inline]
fn att_nominal_alignby(cur_offset: usize, attalignby: u8) -> usize {
    type_align(attalignby as usize, cur_offset)
}

#[inline]
fn varatt_is_1b_e(b: &[u8]) -> bool {
    b[0] == 0x01
}
#[inline]
fn varatt_is_1b(b: &[u8]) -> bool {
    (b[0] & 0x01) == 0x01
}
/// `VARATT_IS_SHORT(PTR)` == `VARATT_IS_1B(PTR)`.
#[inline]
fn varatt_is_short(b: &[u8]) -> bool {
    varatt_is_1b(b)
}
/// `VARATT_IS_EXTENDED(PTR)` — NOT a plain 4-byte uncompressed datum.
#[inline]
fn varatt_is_extended(b: &[u8]) -> bool {
    !varatt_is_4b_u(b)
}
/// `VARATT_IS_4B_U(PTR)` — 4-byte header, low two bits 00.
#[inline]
fn varatt_is_4b_u(b: &[u8]) -> bool {
    (b[0] & 0x03) == 0x00
}
/// `VARATT_IS_EXTERNAL(PTR)` == `VARATT_IS_1B_E(PTR)`.
#[inline]
fn varatt_is_external(b: &[u8]) -> bool {
    varatt_is_1b_e(b)
}
/// `VARTAG_EXTERNAL(PTR)` == `VARTAG_1B_E(PTR)` — the tag byte.
#[inline]
fn vartag_external(b: &[u8]) -> u8 {
    b[1]
}
#[inline]
fn varsize_1b(b: &[u8]) -> u32 {
    ((b[0] >> 1) & 0x7F) as u32
}
#[inline]
fn varsize_4b(b: &[u8]) -> u32 {
    let hdr = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
    (hdr >> 2) & 0x3FFF_FFFF
}
/// `VARSIZE(PTR)` — 4-byte-header total length.
#[inline]
fn varsize(b: &[u8]) -> u32 {
    varsize_4b(b)
}
/// `VARSIZE_SHORT(PTR)` — 1-byte-header total length.
#[inline]
fn varsize_short(b: &[u8]) -> u32 {
    varsize_1b(b)
}
/// `va_4byte.va_header` of a 4-byte-header varlena (for an error message).
#[inline]
fn va_4byte_header(b: &[u8]) -> u32 {
    u32::from_ne_bytes([b[0], b[1], b[2], b[3]])
}

/// `VARTAG_SIZE(tag)`.
fn vartag_size(tag: u8) -> usize {
    const VARTAG_INDIRECT: u8 = 1;
    const VARTAG_EXPANDED_RO: u8 = 2;
    const VARTAG_EXPANDED_RW: u8 = 3;
    if tag == VARTAG_INDIRECT {
        8
    } else if tag == VARTAG_EXPANDED_RO || tag == VARTAG_EXPANDED_RW {
        8
    } else {
        debug_assert_eq!(tag, VARTAG_ONDISK);
        16
    }
}
#[inline]
fn varsize_external(b: &[u8]) -> usize {
    VARHDRSZ_EXTERNAL + vartag_size(vartag_external(b))
}
/// `VARSIZE_ANY(ptr)` for a varlena starting at `b[0]`.
#[inline]
fn varsize_any(b: &[u8]) -> usize {
    if varatt_is_1b_e(b) {
        varsize_external(b)
    } else if varatt_is_1b(b) {
        varsize_1b(b) as usize
    } else {
        varsize_4b(b) as usize
    }
}

/// `att_pointer_alignby(cur_offset, attalignby, attlen, attptr)`.
#[inline]
fn att_pointer_alignby(cur_offset: usize, attalignby: u8, attlen: i16, data: &[u8], off: usize) -> usize {
    if attlen == -1 && data[off] != 0 {
        cur_offset
    } else {
        att_nominal_alignby(cur_offset, attalignby)
    }
}

/// `att_addlength_pointer(cur_offset, attlen, attptr)`.
#[inline]
fn att_addlength_pointer(cur_offset: usize, attlen: i16, data: &[u8], off: usize) -> usize {
    if attlen > 0 {
        cur_offset + attlen as usize
    } else if attlen == -1 {
        cur_offset + varsize_any(&data[off..])
    } else {
        debug_assert_eq!(attlen, -2);
        let mut len = 0usize;
        while data[off + len] != 0 {
            len += 1;
        }
        cur_offset + len + 1
    }
}

// ===========================================================================
// check_tuple_attribute / check_toasted_attribute / check_toast_tuple
// ===========================================================================

/// `check_tuple_attribute(ctx)` — check the current attribute (`ctx.attnum`),
/// following heap_deform_tuple / detoast_external_attr logic with extra
/// corruption checks. Returns whether processing can continue to the next
/// attribute.
fn check_tuple_attribute<'mcx>(
    ctx: &mut HeapCheckContext<'mcx>,
    rsinfo: &mut types_nodes::funcapi::ReturnSetInfo<'mcx>,
) -> PgResult<bool> {
    let tuphdr = ctx.tuphdr.clone_in(ctx.mcx)?;
    let infomask = tuphdr.t_infomask;

    // thisatt = TupleDescCompactAttr(RelationGetDescr(ctx->rel), ctx->attnum)
    let thisatt = ctx.rel.rd_att.compact_attrs[ctx.attnum as usize];
    let attlen = thisatt.attlen;
    let attalignby = thisatt.attalignby;
    let attbyval = thisatt.attbyval;

    // tp = (char *) tuphdr + tuphdr->t_hoff  — the tuple data area on the page.
    // The tuple's on-page bytes are page[lp_off .. lp_off+lp_len]; tp begins at
    // t_hoff within that. Copy out so `ctx` can be borrowed mutably for reports.
    let item: alloc::vec::Vec<u8> =
        page_get_item(&ctx.page, ctx.lp_off, ctx.lp_len).to_vec();
    let item: &[u8] = &item;
    let tp_start = tuphdr.t_hoff as usize;

    if tuphdr.t_hoff as u32 + ctx.offset > ctx.lp_len as u32 {
        report(ctx, rsinfo, format!(
            "attribute with length {attlen} starts at offset {} beyond total tuple length {}",
            tuphdr.t_hoff as u32 + ctx.offset, ctx.lp_len
        ))?;
        return Ok(false);
    }

    // Skip null values.
    if (infomask & HEAP_HASNULL) != 0 && att_isnull(ctx.attnum as usize, &tuphdr.t_bits) {
        return Ok(true);
    }

    // Skip non-varlena values, but update offset first.
    if attlen != -1 {
        ctx.offset = att_nominal_alignby(ctx.offset as usize, attalignby) as u32;
        ctx.offset = att_addlength_pointer(
            ctx.offset as usize,
            attlen,
            item,
            tp_start + ctx.offset as usize,
        ) as u32;
        if tuphdr.t_hoff as u32 + ctx.offset > ctx.lp_len as u32 {
            report(ctx, rsinfo, format!(
                "attribute with length {attlen} ends at offset {} beyond total tuple length {}",
                tuphdr.t_hoff as u32 + ctx.offset, ctx.lp_len
            ))?;
            return Ok(false);
        }
        return Ok(true);
    }

    // A varlena attribute.
    ctx.offset = att_pointer_alignby(
        ctx.offset as usize,
        attalignby,
        -1,
        item,
        tp_start + ctx.offset as usize,
    ) as u32;

    let attr_off = tp_start + ctx.offset as usize;

    // Check VARTAG won't Assert on a corrupt va_tag before att_addlength.
    if varatt_is_external(&item[attr_off..]) {
        let va_tag = vartag_external(&item[attr_off..]);
        if va_tag != VARTAG_ONDISK {
            report(ctx, rsinfo, format!("toasted attribute has unexpected TOAST tag {va_tag}"))?;
            return Ok(false); // can't know where the next attribute begins
        }
    }

    ctx.offset = att_addlength_pointer(
        ctx.offset as usize,
        attlen,
        item,
        tp_start + ctx.offset as usize,
    ) as u32;

    if tuphdr.t_hoff as u32 + ctx.offset > ctx.lp_len as u32 {
        report(ctx, rsinfo, format!(
            "attribute with length {attlen} ends at offset {} beyond total tuple length {}",
            tuphdr.t_hoff as u32 + ctx.offset, ctx.lp_len
        ))?;
        return Ok(false);
    }

    // attr = the varlena datum bytes (we don't copy; read from page).
    let attr = &item[attr_off..];
    let _ = attbyval;

    // detoast_external_attr logic: skip values that are not external.
    if !varatt_is_external(attr) {
        return Ok(true);
    }

    // It is external, on a page on disk. Copy the toast pointer for alignment.
    let toast_pointer = VarattExternal::from_external(attr);

    // Toasted attributes too large to be untoasted should never be stored.
    if toast_pointer.va_rawsize > VARLENA_SIZE_LIMIT {
        report(ctx, rsinfo, format!(
            "toast value {} rawsize {} exceeds limit {}",
            toast_pointer.va_valueid, toast_pointer.va_rawsize, VARLENA_SIZE_LIMIT
        ))?;
    }

    if toast_pointer.is_compressed() {
        let cmid = toast_pointer.compress_method();
        let valid = matches!(cmid, TOAST_PGLZ_COMPRESSION_ID | TOAST_LZ4_COMPRESSION_ID);
        let _ = TOAST_INVALID_COMPRESSION_ID;
        if !valid {
            report(ctx, rsinfo, format!(
                "toast value {} has invalid compression method id {cmid}",
                toast_pointer.va_valueid
            ))?;
        }
    }

    // The tuple header better claim to contain toasted values.
    if (infomask & HEAP_HASEXTERNAL) == 0 {
        report(ctx, rsinfo, format!(
            "toast value {} is external but tuple header flag HEAP_HASEXTERNAL not set",
            toast_pointer.va_valueid
        ))?;
        return Ok(true);
    }

    // The relation better have a toast table.
    if ctx.rel.rd_rel.reltoastrelid == 0 {
        report(ctx, rsinfo, format!(
            "toast value {} is external but relation has no toast relation",
            toast_pointer.va_valueid
        ))?;
        return Ok(true);
    }

    // If we were told to skip toast checking, we're done.
    if ctx.toast_rel.is_none() {
        return Ok(true);
    }

    // If this tuple is eligible to be pruned we cannot check the toast.
    // Otherwise push a copy of the toast pointer to check after releasing the
    // buffer lock.
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

/// `check_toast_tuple(toasttup, ctx, ta, expected_chunk_seq, extsize)` — check
/// one toast chunk tuple against the running sequence. Returns the updated
/// `expected_chunk_seq`.
fn check_toast_tuple<'mcx>(
    chunk_seq_col: (Datum<'mcx>, bool),
    chunk_data_col: (Datum<'mcx>, bool),
    ctx: &mut HeapCheckContext<'mcx>,
    rsinfo: &mut types_nodes::funcapi::ReturnSetInfo<'mcx>,
    ta: &ToastedAttribute,
    expected_chunk_seq: i32,
    extsize: u32,
) -> PgResult<i32> {
    let last_chunk_seq: i32 = ((extsize - 1) / TOAST_MAX_CHUNK_SIZE as u32) as i32;

    // Sanity-check the sequence number (attr 2).
    let (chunk_seq_val, seq_isnull) = chunk_seq_col;
    if seq_isnull {
        report_toast(ctx, rsinfo, ta, format!(
            "toast value {} has toast chunk with null sequence number",
            ta.toast_pointer.va_valueid
        ))?;
        return Ok(expected_chunk_seq);
    }
    let chunk_seq = chunk_seq_val.as_i32();
    if chunk_seq != expected_chunk_seq {
        report_toast(ctx, rsinfo, ta, format!(
            "toast value {} index scan returned chunk {chunk_seq} when expecting chunk {expected_chunk_seq}",
            ta.toast_pointer.va_valueid
        ))?;
    }
    let next_expected = chunk_seq + 1;

    // Sanity-check the chunk data (attr 3).
    let (chunk_val, data_isnull) = chunk_data_col;
    if data_isnull {
        report_toast(ctx, rsinfo, ta, format!(
            "toast value {} chunk {chunk_seq} has null data",
            ta.toast_pointer.va_valueid
        ))?;
        return Ok(next_expected);
    }
    let chunk: &[u8] = match &chunk_val {
        Datum::ByRef(b) => b,
        // bytea is by reference; any other arm here is itself corruption-shaped.
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => {
            report_toast(ctx, rsinfo, ta, format!(
                "toast value {} chunk {chunk_seq} has invalid varlena header",
                ta.toast_pointer.va_valueid
            ))?;
            return Ok(next_expected);
        }
    };

    let chunksize: i32 = if !varatt_is_extended(chunk) {
        varsize(chunk) as i32 - VARHDRSZ as i32
    } else if varatt_is_short(chunk) {
        // could happen due to heap_form_tuple doing its thing
        varsize_short(chunk) as i32 - VARHDRSZ_SHORT as i32
    } else {
        // should never happen
        let header = va_4byte_header(chunk);
        report_toast(ctx, rsinfo, ta, format!(
            "toast value {} chunk {chunk_seq} has invalid varlena header {header:0x}",
            ta.toast_pointer.va_valueid
        ))?;
        return Ok(next_expected);
    };

    if chunk_seq > last_chunk_seq {
        report_toast(ctx, rsinfo, ta, format!(
            "toast value {} chunk {chunk_seq} follows last expected chunk {last_chunk_seq}",
            ta.toast_pointer.va_valueid
        ))?;
        return Ok(next_expected);
    }

    let expected_size: i32 = if chunk_seq < last_chunk_seq {
        TOAST_MAX_CHUNK_SIZE
    } else {
        extsize as i32 - (last_chunk_seq * TOAST_MAX_CHUNK_SIZE)
    };

    if chunksize != expected_size {
        report_toast(ctx, rsinfo, ta, format!(
            "toast value {} chunk {chunk_seq} has size {chunksize}, but expected size {expected_size}",
            ta.toast_pointer.va_valueid
        ))?;
    }

    Ok(next_expected)
}

/// `check_toasted_attribute(ctx, ta)` — look up `ta` in the toast table and
/// check its chunk sequence. Only called for toast pointers that cannot be
/// vacuumed away during processing.
fn check_toasted_attribute<'mcx>(
    ctx: &mut HeapCheckContext<'mcx>,
    rsinfo: &mut types_nodes::funcapi::ReturnSetInfo<'mcx>,
    ta: &ToastedAttribute,
    toast_rel: &Relation,
    valid_toast_index: &types_rel::RelationData,
    have_registered_or_active_snapshot: bool,
) -> PgResult<()> {
    let mcx = ctx.mcx;
    let extsize = ta.toast_pointer.extsize();
    let last_chunk_seq: i32 = ((extsize - 1) / TOAST_MAX_CHUNK_SIZE as u32) as i32;

    // ScanKeyInit on va_valueid (attr 1).
    let mut toastkey = ScanKeyData::empty();
    ScanKeyInit(
        &mut toastkey,
        1 as AttrNumber,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(ta.toast_pointer.va_valueid),
    )?;

    let snapshot = toast_internals::get_toast_snapshot::call(have_registered_or_active_snapshot)?;
    let mut toastscan = genam::systable_beginscan_ordered::call(
        toast_rel,
        valid_toast_index,
        Some(&snapshot),
        core::slice::from_ref(&toastkey),
    )?;

    let toastdesc = toast_rel.rd_att.clone_in(mcx)?;
    let mut found_toasttup = false;
    let mut expected_chunk_seq: i32 = 0;
    while let Some(toasttup) = genam::systable_getnext_ordered::call(
        mcx,
        toastscan.desc_mut(),
        types_scan::sdir::ScanDirection::ForwardScanDirection,
    )? {
        found_toasttup = true;
        // fastgetattr(toasttup, 2/3, toastdesc): deform and read columns.
        let cols = heaptuple::heap_deform_tuple(mcx, &toasttup.tuple, &toastdesc, &toasttup.data)?;
        let seq_col = cols.get(1).cloned().unwrap_or((Datum::null(), true));
        let data_col = cols.get(2).cloned().unwrap_or((Datum::null(), true));
        expected_chunk_seq =
            check_toast_tuple(seq_col, data_col, ctx, rsinfo, ta, expected_chunk_seq, extsize)?;
    }
    toastscan.end()?;

    if !found_toasttup {
        report_toast(ctx, rsinfo, ta, format!(
            "toast value {} not found in toast table",
            ta.toast_pointer.va_valueid
        ))?;
    } else if expected_chunk_seq <= last_chunk_seq {
        report_toast(ctx, rsinfo, ta, format!(
            "toast value {} was expected to end at chunk {last_chunk_seq}, but ended while expecting chunk {expected_chunk_seq}",
            ta.toast_pointer.va_valueid
        ))?;
    }

    Ok(())
}

/// `check_tuple(ctx, xmin_commit_status_ok, xmin_commit_status)` — the
/// per-tuple driver. Returns `(xmin_status_ok, xmin_status)`.
fn check_tuple<'mcx>(
    ctx: &mut HeapCheckContext<'mcx>,
    rsinfo: &mut types_nodes::funcapi::ReturnSetInfo<'mcx>,
) -> PgResult<(bool, XidCommitStatus)> {
    // Header checks; bail if too corrupt.
    if !check_tuple_header(ctx, rsinfo)? {
        return Ok((false, XID_COMMITTED));
    }

    // Visibility; if the inserting transaction aborted we cannot trust the
    // relation descriptor matches the tuple structure.
    let (checkable, xmin_ok, xmin_status) = check_tuple_visibility(ctx, rsinfo)?;
    if !checkable {
        return Ok((xmin_ok, xmin_status));
    }

    // The tuple is visible, so it must be compatible with the current relation
    // descriptor: it may have fewer columns, never more.
    if ctx.rel.rd_att.natts < ctx.natts {
        report(ctx, rsinfo, format!(
            "number of attributes {} exceeds maximum expected for table {}",
            ctx.natts, ctx.rel.rd_att.natts
        ))?;
        return Ok((xmin_ok, xmin_status));
    }

    // Check each attribute until corruption confuses what to do next.
    ctx.offset = 0;
    let natts = ctx.natts;
    let mut attnum = 0;
    while attnum < natts {
        ctx.attnum = attnum as AttrNumber;
        if !check_tuple_attribute(ctx, rsinfo)? {
            break; // cannot continue
        }
        attnum += 1;
    }

    // Revert attnum to -1 until we again examine individual attributes.
    ctx.attnum = -1;
    Ok((xmin_ok, xmin_status))
}

// ===========================================================================
// verify_heapam (SQL entry point) — verify_heapam.c
// ===========================================================================

/// `verify_heapam(relation, on_error_stop, check_toast, skip, startblock,
/// endblock)` — the SQL-callable heap verifier. `startblock`/`endblock` are the
/// nullable args 4 and 5 (`Option<i64>`); args 0-3 are required (the C errors on
/// NULL for them, which the typed dispatch enforces).
pub fn verify_heapam<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    relid: Oid,
    on_error_stop: bool,
    check_toast: bool,
    skip: &str,
    startblock: Option<i64>,
    endblock: Option<i64>,
) -> PgResult<()> {
    // Parse the skip option.
    let skip_option = if skip.eq_ignore_ascii_case("all-visible") {
        SKIP_PAGES_ALL_VISIBLE
    } else if skip.eq_ignore_ascii_case("all-frozen") {
        SKIP_PAGES_ALL_FROZEN
    } else if skip.eq_ignore_ascii_case("none") {
        SKIP_PAGES_NONE
    } else {
        return Err(PgError::error("invalid skip option")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_hint("Valid skip options are \"all-visible\", \"all-frozen\", and \"none\"."));
    };

    // Any xmin newer than our snapshot's xmin can't become all-visible while
    // we run.
    let safe_xmin = snapmgr::get_transaction_snapshot::call()?.xmin;

    // Construct the materialized SRF (tuplestore + descriptor in resultinfo).
    funcapi::InitMaterializedSRF::call(fcinfo, 0)?;

    // Open the relation; check relkind and access method.
    let rel = backend_access_common_relation_seams::relation_open::call(mcx, relid, AccessShareLock)?;

    if !relkind_has_table_am(rel.rd_rel.relkind) && rel.rd_rel.relkind != RELKIND_SEQUENCE {
        let name = rel_name(&rel);
        rel.close(AccessShareLock)?;
        return Err(PgError::error(format!("cannot check relation \"{name}\""))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE));
    }

    // Sequences always use heap AM but don't show it in the catalogs.
    if rel.rd_rel.relkind != RELKIND_SEQUENCE && rel.rd_rel.relam != HEAP_TABLE_AM_OID {
        rel.close(AccessShareLock)?;
        return Err(PgError::error("only heap AM is supported")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    // Early exit for unlogged relations during recovery (no fork to check).
    if rel.rd_rel.relpersistence == types_tuple::access::RELPERSISTENCE_UNLOGGED
        && xlog::recovery_in_progress::call()
    {
        let _ = ERRCODE_READ_ONLY_SQL_TRANSACTION;
        elog::ereport_msg::call(
            DEBUG1,
            format!(
                "cannot verify unlogged relation \"{}\" during recovery, skipping",
                rel_name(&rel)
            ),
            None,
        )?;
        rel.close(AccessShareLock)?;
        return Ok(());
    }

    // Early exit if the relation is empty.
    let nblocks = backend_access_heap_hio_seams::relation_get_number_of_blocks::call(relid)?;
    if nblocks == 0 {
        rel.close(AccessShareLock)?;
        return Ok(());
    }

    let bstrategy = buffer_support::get_access_strategy(BufferAccessStrategyType::BasBulkread)?;

    // Validate block numbers, or handle nulls.
    let first_block: BlockNumber = match startblock {
        None => 0,
        Some(fb) => {
            if fb < 0 || fb >= nblocks as i64 {
                rel.close(AccessShareLock)?;
                return Err(PgError::error(format!(
                    "starting block number must be between 0 and {}",
                    nblocks - 1
                ))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
            fb as BlockNumber
        }
    };
    let last_block: BlockNumber = match endblock {
        None => nblocks - 1,
        Some(lb) => {
            if lb < 0 || lb >= nblocks as i64 {
                rel.close(AccessShareLock)?;
                return Err(PgError::error(format!(
                    "ending block number must be between 0 and {}",
                    nblocks - 1
                ))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
            lb as BlockNumber
        }
    };

    // Optionally open the toast relation, if any.
    let have_snapshot = snapmgr::have_registered_or_active_snapshot::call();
    let (toast_rel, toast_indexes_guard): (
        Option<Relation>,
        Option<toast_internals::ToastIndexesGuard>,
    ) = if rel.rd_rel.reltoastrelid != 0 && check_toast {
        let tr = backend_access_table_table_seams::table_open::call(
            mcx,
            rel.rd_rel.reltoastrelid,
            AccessShareLock,
        )?;
        let guard = toast_internals::toast_open_indexes::call(mcx, &tr, AccessShareLock)?;
        (Some(tr), Some(guard))
    } else {
        (None, None)
    };

    // The scan borrows `rel` / `toast_rel` / `toast_indexes_guard`; run it in a
    // block so `ctx` + `stream` (and those borrows) drop before the relations
    // are closed (consumed) below.
    {
    // Cache the valid xid/mxid ranges + relation thresholds.
    let mut ctx = HeapCheckContext {
        mcx,
        next_fxid: FullTransactionId::from_u64(0),
        next_xid: 0,
        oldest_xid: 0,
        oldest_fxid: FullTransactionId::from_u64(0),
        safe_xmin,
        next_mxact: 0,
        oldest_mxact: 0,
        cached_xid: types_core::xact::InvalidTransactionId,
        cached_status: XID_COMMITTED,
        rel: rel.alias(),
        relfrozenxid: 0,
        relfrozenfxid: FullTransactionId::from_u64(0),
        relminmxid: 0,
        toast_rel: toast_rel.as_ref().map(|r| r.alias()),
        blkno: 0,
        page: alloc::vec::Vec::new(),
        offnum: 0,
        lp_len: 0,
        lp_off: 0,
        tuphdr: HeapTupleHeaderData::read_on_page(mcx, &[0u8; 23])?,
        natts: 0,
        offset: 0,
        attnum: -1,
        tuple_could_be_pruned: true,
        toasted_attributes: alloc::vec::Vec::new(),
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

    // Set up the read stream. For SKIP_PAGES_NONE the general block-range
    // callback is used; otherwise the unskippable-block callback reads the VM.
    let range = alloc::rc::Rc::new(core::cell::RefCell::new(
        read_stream::BlockRangeReadStreamPrivate {
            current_blocknum: first_block,
            last_exclusive: last_block + 1,
        },
    ));

    let (stream_flags, stream_cb): (i32, read_stream::ReadStreamBlockNumberCB) =
        if skip_option == SKIP_PAGES_NONE {
            (
                read_stream::READ_STREAM_SEQUENTIAL
                    | read_stream::READ_STREAM_FULL
                    | read_stream::READ_STREAM_USE_BATCHING,
                read_stream::block_range_read_stream_cb(range.clone()),
            )
        } else {
            (
                read_stream::READ_STREAM_DEFAULT,
                heapcheck_read_stream_next_unskippable(&rel, skip_option, range.clone()),
            )
        };

    let mut stream = read_stream::read_stream_begin_relation(
        stream_flags,
        bstrategy,
        &rel,
        ForkNumber::MAIN_FORKNUM,
        stream_cb,
        0,
    )?;

    // The pending toasted attributes for the page whose lock we just released.
    let mut pending_toasted: alloc::vec::Vec<ToastedAttribute> = alloc::vec::Vec::new();

    loop {
        let (buffer, _) = stream.read_stream_next_buffer()?;
        if buffer == InvalidBuffer {
            break;
        }

        backend_tcop_postgres_seams::check_for_interrupts::call()?;

        // predecessor/successor/lp_valid/xmin_commit_status arrays (1-based).
        let mut predecessor = alloc::vec![InvalidOffsetNumber; MaxOffsetNumber + 1];
        let mut successor = alloc::vec![InvalidOffsetNumber; MaxOffsetNumber + 1];
        let mut lp_valid = alloc::vec![false; MaxOffsetNumber + 1];
        let mut xmin_commit_status_ok = alloc::vec![false; MaxOffsetNumber + 1];
        let mut xmin_commit_status = alloc::vec![XID_COMMITTED; MaxOffsetNumber + 1];

        // Lock the page and snapshot it.
        bufmgr::lock_buffer::call(buffer, BUFFER_LOCK_SHARE)?;
        ctx.blkno = bufmgr::buffer_get_block_number::call(buffer);
        ctx.page = bufmgr::buffer_get_page::call(mcx, buffer)?.to_vec();

        let rsinfo = fcinfo
            .resultinfo
            .as_mut()
            .expect("InitMaterializedSRF establishes fcinfo->resultinfo");

        let maxoff = page_get_max_offset_number(&ctx.page);

        // --- per-tuple checks ----------------------------------------------
        let mut off = FirstOffsetNumber;
        while off <= maxoff {
            ctx.offnum = off;
            successor[off as usize] = InvalidOffsetNumber;
            lp_valid[off as usize] = false;
            xmin_commit_status_ok[off as usize] = false;
            let lp = item_id(&ctx.page, off);

            // Skip over unused/dead line pointers.
            if !lp.is_used() || lp.is_dead() {
                off = offset_number_next(off);
                continue;
            }

            // A redirect: check it points to a valid LP_NORMAL offset.
            if lp.is_redirected() {
                let rdoffnum = lp.get_redirect();
                if rdoffnum < FirstOffsetNumber {
                    report(&mut ctx, rsinfo, format!(
                        "line pointer redirection to item at offset {rdoffnum} precedes minimum offset {FirstOffsetNumber}"
                    ))?;
                    off = offset_number_next(off);
                    continue;
                }
                if rdoffnum as usize > maxoff as usize {
                    report(&mut ctx, rsinfo, format!(
                        "line pointer redirection to item at offset {rdoffnum} exceeds maximum offset {maxoff}"
                    ))?;
                    off = offset_number_next(off);
                    continue;
                }
                let rditem = item_id(&ctx.page, rdoffnum);
                if !rditem.is_used() {
                    report(&mut ctx, rsinfo, format!(
                        "redirected line pointer points to an unused item at offset {rdoffnum}"
                    ))?;
                    off = offset_number_next(off);
                    continue;
                } else if rditem.is_dead() {
                    report(&mut ctx, rsinfo, format!(
                        "redirected line pointer points to a dead item at offset {rdoffnum}"
                    ))?;
                    off = offset_number_next(off);
                    continue;
                } else if rditem.is_redirected() {
                    report(&mut ctx, rsinfo, format!(
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

            // Sanity-check the line pointer's offset and length values.
            ctx.lp_len = lp.lp_len;
            ctx.lp_off = lp.lp_off;
            let (lp_off, lp_len) = (ctx.lp_off, ctx.lp_len);

            if lp_off as u32 != maxalign(lp_off as u32) {
                report(&mut ctx, rsinfo, format!(
                    "line pointer to page offset {lp_off} is not maximally aligned"
                ))?;
                off = offset_number_next(off);
                continue;
            }
            if (lp_len as u32) < maxalign(SizeofHeapTupleHeader) {
                report(&mut ctx, rsinfo, format!(
                    "line pointer length {lp_len} is less than the minimum tuple header size {}",
                    maxalign(SizeofHeapTupleHeader)
                ))?;
                off = offset_number_next(off);
                continue;
            }
            if lp_off as u32 + lp_len as u32 > BLCKSZ {
                report(&mut ctx, rsinfo, format!(
                    "line pointer to page offset {lp_off} with length {lp_len} ends beyond maximum page offset {BLCKSZ}"
                ))?;
                off = offset_number_next(off);
                continue;
            }

            // Safe to examine the tuple header.
            lp_valid[off as usize] = true;
            let item = page_get_item(&ctx.page, ctx.lp_off, ctx.lp_len).to_vec();
            ctx.tuphdr = HeapTupleHeaderData::read_on_page(mcx, &item)?;
            // t_bits stays on the page; read_on_page leaves it empty, so attach
            // the null bitmap bytes from the item for att_isnull / fastgetattr.
            attach_tbits(&mut ctx.tuphdr, &item, mcx)?;
            ctx.natts = HeapTupleHeaderGetNatts(&ctx.tuphdr) as i32;

            // Check this tuple.
            let (ok, status) = check_tuple(&mut ctx, rsinfo)?;
            xmin_commit_status_ok[off as usize] = ok;
            xmin_commit_status[off as usize] = status;

            // If the CTID points to another tuple on the same page, record the
            // successor.
            let nextblkno = ctid_block(&ctx.tuphdr);
            let nextoffnum = ctid_offset(&ctx.tuphdr);
            if nextblkno == ctx.blkno
                && nextoffnum != ctx.offnum
                && nextoffnum >= FirstOffsetNumber
                && nextoffnum as usize <= maxoff as usize
            {
                successor[off as usize] = nextoffnum;
            }

            off = offset_number_next(off);
        }

        // --- update-chain validation ---------------------------------------
        ctx.attnum = -1;
        let mut off = FirstOffsetNumber;
        while off <= maxoff {
            ctx.offnum = off;
            let nextoffnum = successor[off as usize];

            if nextoffnum == InvalidOffsetNumber || !lp_valid[nextoffnum as usize] {
                off = offset_number_next(off);
                continue;
            }

            let curr_lp = item_id(&ctx.page, off);
            let next_lp = item_id(&ctx.page, nextoffnum);

            // Current line pointer is a redirect.
            if curr_lp.is_redirected() {
                debug_assert!(next_lp.is_normal());
                let next_item = page_get_item(&ctx.page, next_lp.lp_off, next_lp.lp_len).to_vec();
                let next_htup = HeapTupleHeaderData::read_on_page(mcx, &next_item)?;
                if !is_heap_only(&next_htup) {
                    report(&mut ctx, rsinfo, format!(
                        "redirected line pointer points to a non-heap-only tuple at offset {nextoffnum}"
                    ))?;
                }
                if predecessor[nextoffnum as usize] != InvalidOffsetNumber {
                    report(&mut ctx, rsinfo, format!(
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

            // If the next is a redirect, or xmax(curr) != xmin(next), not a chain.
            if next_lp.is_redirected() {
                off = offset_number_next(off);
                continue;
            }
            let curr_item = page_get_item(&ctx.page, curr_lp.lp_off, curr_lp.lp_len).to_vec();
            let curr_htup = HeapTupleHeaderData::read_on_page(mcx, &curr_item)?;
            let next_item = page_get_item(&ctx.page, next_lp.lp_off, next_lp.lp_len).to_vec();
            let next_htup = HeapTupleHeaderData::read_on_page(mcx, &next_item)?;
            let curr_xmax = header_get_update_xid(&curr_htup)?;
            let next_xmin = HeapTupleHeaderGetXmin(&next_htup);
            if !TransactionIdIsValid(curr_xmax) || !TransactionIdEquals(curr_xmax, next_xmin) {
                off = offset_number_next(off);
                continue;
            }

            // HOT chains should not intersect.
            if predecessor[nextoffnum as usize] != InvalidOffsetNumber {
                report(&mut ctx, rsinfo, format!(
                    "tuple points to new version at offset {nextoffnum}, but offset {} also points there",
                    predecessor[nextoffnum as usize]
                ))?;
                off = offset_number_next(off);
                continue;
            }
            predecessor[nextoffnum as usize] = off;

            // HOT-update flag consistency. (Can't use IsHotUpdated — it checks
            // hint bits indicating xmin/xmax aborted.)
            if (curr_htup.t_infomask2 & HEAP_HOT_UPDATED) == 0 && is_heap_only(&next_htup) {
                report(&mut ctx, rsinfo, format!(
                    "non-heap-only update produced a heap-only tuple at offset {nextoffnum}"
                ))?;
            }
            if (curr_htup.t_infomask2 & HEAP_HOT_UPDATED) != 0 && !is_heap_only(&next_htup) {
                report(&mut ctx, rsinfo, format!(
                    "heap-only update produced a non-heap only tuple at offset {nextoffnum}"
                ))?;
            }

            // In-progress xmin updated to a committed xmin: corruption.
            let curr_xmin = HeapTupleHeaderGetXmin(&curr_htup);
            if xmin_commit_status_ok[off as usize]
                && xmin_commit_status[off as usize] == XID_IN_PROGRESS
                && xmin_commit_status_ok[nextoffnum as usize]
                && xmin_commit_status[nextoffnum as usize] == XID_COMMITTED
                && procarray::transaction_id_is_in_progress::call(curr_xmin)?
            {
                report(&mut ctx, rsinfo, format!(
                    "tuple with in-progress xmin {curr_xmin} was updated to produce a tuple at offset {} with committed xmin {next_xmin}",
                    off
                ))?;
            }

            // Aborted xmin updated to in-progress/committed xmin: corruption.
            if xmin_commit_status_ok[off as usize]
                && xmin_commit_status[off as usize] == XID_ABORTED
                && xmin_commit_status_ok[nextoffnum as usize]
            {
                if xmin_commit_status[nextoffnum as usize] == XID_IN_PROGRESS {
                    report(&mut ctx, rsinfo, format!(
                        "tuple with aborted xmin {curr_xmin} was updated to produce a tuple at offset {} with in-progress xmin {next_xmin}",
                        off
                    ))?;
                } else if xmin_commit_status[nextoffnum as usize] == XID_COMMITTED {
                    report(&mut ctx, rsinfo, format!(
                        "tuple with aborted xmin {curr_xmin} was updated to produce a tuple at offset {} with committed xmin {next_xmin}",
                        off
                    ))?;
                }
            }

            off = offset_number_next(off);
        }

        // --- chain-root check ----------------------------------------------
        let mut off = FirstOffsetNumber;
        while off <= maxoff {
            ctx.offnum = off;
            if xmin_commit_status_ok[off as usize]
                && (xmin_commit_status[off as usize] == XID_COMMITTED
                    || xmin_commit_status[off as usize] == XID_IN_PROGRESS)
                && predecessor[off as usize] == InvalidOffsetNumber
            {
                let curr_lp = item_id(&ctx.page, off);
                if !curr_lp.is_redirected() {
                    let item = page_get_item(&ctx.page, curr_lp.lp_off, curr_lp.lp_len).to_vec();
                    let curr_htup = HeapTupleHeaderData::read_on_page(mcx, &item)?;
                    if is_heap_only(&curr_htup) {
                        report(&mut ctx, rsinfo, "tuple is root of chain but is marked as heap-only tuple".to_string())?;
                    }
                }
            }
            off = offset_number_next(off);
        }

        // Release the page lock.
        bufmgr::unlock_release_buffer::call(buffer);

        // Check any toast pointers collected from this page now that the lock
        // is released. Move them out of ctx so we can borrow ctx mutably.
        if !ctx.toasted_attributes.is_empty() {
            pending_toasted.clear();
            core::mem::swap(&mut pending_toasted, &mut ctx.toasted_attributes);
            if let (Some(tr), Some(guard)) = (toast_rel.as_ref(), toast_indexes_guard.as_ref()) {
                let valid_index = guard.valid_index();
                let rsinfo = fcinfo
                    .resultinfo
                    .as_mut()
                    .expect("InitMaterializedSRF establishes fcinfo->resultinfo");
                for ta in &pending_toasted {
                    check_toasted_attribute(&mut ctx, rsinfo, ta, tr, valid_index, have_snapshot)?;
                }
            }
            pending_toasted.clear();
        }

        if on_error_stop && ctx.is_corrupt {
            break;
        }
    }

    read_stream::read_stream_end(stream)?;
    } // end scan block: ctx + stream dropped, borrows of rel/toast released.

    // Close the toast indexes / relation, then the main relation. The
    // ToastIndexesGuard releases the indexes; table_close releases the toast
    // relation; the main relation releases its lock + relcache ref.
    if let Some(guard) = toast_indexes_guard {
        guard.close()?;
    }
    if let Some(tr) = toast_rel {
        tr.close(AccessShareLock)?;
    }
    rel.close(AccessShareLock)?;

    Ok(())
}

/// `RelationGetRelationName(rel)`.
fn rel_name(rel: &Relation) -> alloc::string::String {
    rel.name().to_string()
}

/// `read_on_page` leaves `t_bits` empty; re-attach the on-page null bitmap so
/// `att_isnull` works. The bitmap immediately follows the fixed header
/// (`SizeofHeapTupleHeader`) and spans `BITMAPLEN(natts)` bytes when
/// `HEAP_HASNULL` is set.
fn attach_tbits<'mcx>(
    tuphdr: &mut HeapTupleHeaderData<'mcx>,
    item: &[u8],
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    if (tuphdr.t_infomask & HEAP_HASNULL) != 0 {
        let natts = HeapTupleHeaderGetNatts(tuphdr) as i32;
        let len = BITMAPLEN(natts) as usize;
        let start = SizeofHeapTupleHeader as usize;
        if item.len() >= start + len {
            let mut bits = mcx::PgVec::new_in(mcx);
            for &b in &item[start..start + len] {
                bits.push(b);
            }
            tuphdr.t_bits = bits;
        }
    }
    Ok(())
}

/// `heapcheck_read_stream_next_unskippable` — the read-stream callback for the
/// `all-visible` / `all-frozen` skip options. Walks `[current_blocknum,
/// last_exclusive)` returning the next block not skippable per the VM, or
/// `InvalidBlockNumber` at end. Takes a VM-buffer lock (so the stream uses the
/// default, non-batching mode).
fn heapcheck_read_stream_next_unskippable<'mcx>(
    rel: &'mcx Relation<'mcx>,
    skip_option: SkipPages,
    range: alloc::rc::Rc<core::cell::RefCell<read_stream::BlockRangeReadStreamPrivate>>,
) -> read_stream::ReadStreamBlockNumberCB<'mcx> {
    let mut vmbuffer: Buffer = InvalidBuffer;
    alloc::boxed::Box::new(move |_per_buffer_data: &mut [u8]| {
        loop {
            let i = {
                let mut p = range.borrow_mut();
                let cur = p.current_blocknum;
                if cur >= p.last_exclusive {
                    break;
                }
                p.current_blocknum = cur + 1;
                cur
            };
            // visibilitymap_get_status can ereport; the callback signature is
            // infallible, so a hard failure here surfaces as a panic (matching
            // the C behaviour of an unrecoverable smgr error). In practice the
            // VM read does not fail on a healthy cluster.
            let (mapbits, newvm) =
                visibilitymap::visibilitymap_get_status::call(rel.alias(), i, vmbuffer)
                    .expect("visibilitymap_get_status failed in heap amcheck");
            vmbuffer = newvm;

            if skip_option == SKIP_PAGES_ALL_FROZEN && (mapbits & VISIBILITYMAP_ALL_FROZEN) != 0 {
                continue;
            }
            if skip_option == SKIP_PAGES_ALL_VISIBLE && (mapbits & VISIBILITYMAP_ALL_VISIBLE) != 0 {
                continue;
            }
            return i;
        }
        types_core::primitive::InvalidBlockNumber
    })
}

/// This crate registers `verify_heapam` through the SQL function-manager (it is
/// a contrib SQL function, not a cross-crate seam), so it owns no inward seams
/// and has no seams to install.
pub fn init_seams() {}
