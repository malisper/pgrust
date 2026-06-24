//! Port of `src/backend/access/nbtree/nbtutils.c` (PostgreSQL 18.3) — utility
//! code for the Postgres btree implementation.
//!
//! All top-level functions of `nbtutils.c` (C names preserved) are ported here,
//! operating on the repo's owned runtime structs: real [`Relation`],
//! `&mut BTScanOpaqueData<'mcx>`, [`ScanKeyData`], [`BTArrayKeyInfo`],
//! [`BTReadPageState`], over `'mcx`, with canonical
//! [`Datum`](::types_tuple::heaptuple::Datum) values.
//!
//! # Chosen signatures (no `IndexScanDesc` in this repo)
//!
//! The C entry points take `IndexScanDesc scan` and reach `scan->opaque`,
//! `scan->indexRelation`, `scan->keyData[]`, etc. Here those are passed as
//! explicit `rel: &Relation<'mcx>` + `so: &mut BTScanOpaqueData<'mcx>`
//! arguments, exactly like the `nbtpreprocesskeys` sibling module and as
//! demanded by the `backend-access-nbtree-core-seams` decls this crate installs
//! (`bt_killitems(rel,&mut so)`, `bt_start_array_keys(rel,&mut so,dir)`,
//! `bt_start_prim_scan(rel,&mut so,dir)->bool`, `bt_mkscankey(rel,Option<&[u8]>)`,
//! `bt_keep_natts_fast`, `bt_check_natts`, `bt_allequalimage`/`_dbg`,
//! `bt_freestack`, `bt_start_vacuum`, `bt_end_vacuum`).
//!
//! # In-crate vs. seam
//!
//! Faithfully ported in-crate: the entire SK_SEARCHARRAY array-advancement
//! state machine (`_bt_advance_array_keys` + `_bt_array_increment/decrement` +
//! `_bt_array_set_low_or_high` + `_bt_skiparray_*` + `_bt_binsrch_*` +
//! `_bt_tuple_before_array_skeys` + `_bt_advance_array_keys_increment`), the
//! per-tuple qual engine (`_bt_checkkeys` / `_bt_check_compare` /
//! `_bt_check_rowcompare` / `_bt_oppodir_checkkeys` / `_bt_scanbehind_checkkeys`
//! / `_bt_checkkeys_look_ahead` / `_bt_set_startikey`), the truncation/keep-natts
//! arithmetic, the byte-codec page reads, the VACUUM cycle-id assignment
//! arithmetic, `btoptions`/`btproperty`/`btbuildphasename`.
//!
//! Wired to real seams: fmgr comparison dispatch
//! (`function_call2_coll`/`function_call1_coll`), `get_opfamily_proc`,
//! `get_typlenbyvalalign`, `index_truncate_tuple`, the bufmgr/`_bt_lockbuf`/
//! `_bt_relbuf` page protocol for `_bt_killitems`.
//!
//! Genuinely-unported callees (no producer exists in this repo yet) reach an
//! honest `panic!` (never `todo!`/`unimplemented!`):
//!   * `index_getprocinfo` / `fmgr_info` materialisation into the `u64` ORDER
//!     proc handles `so->orderProcs[]` carries — no handle producer exists.
//!   * `rd_indoption[]` / `rd_indcollation[]` per-column arrays — the trimmed
//!     `RelationData` does carry them as `PgVec`, but the 1-based-attno relcache
//!     slot contract is honoured (mirrors preprocesskeys).
//!   * `datumCopy` / `pfree` of by-ref skip-array `sk_argument`s, and the
//!     opclass skip-support increment/decrement callbacks — no producer.
//!   * the `btvacinfo` shared-memory array (only the surrounding cycle-id
//!     arithmetic is portable); the parallel-scan `_bt_parallel_*` helpers
//!     (owned by nbtree.c).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::needless_range_loop)]
// Faithful C translations whose call sites are gated out in this repo's
// single-process / non-debug model (the parallel-scan helpers, the
// assert-checking verifier, the InvalidAttrNumber sentinel) and the `result`
// default-assignment pattern mirroring the C locals.
#![allow(dead_code)]
#![allow(unused_assignments)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;

use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_core::fmgr::FmgrInfo;
use ::types_core::primitive::{AttrNumber, BlockNumber, OffsetNumber, Oid, Size};
use ::types_error::{PgError, PgResult};
use ::types_error::error::{ERRCODE_PROGRAM_LIMIT_EXCEEDED, DEBUG1, ERROR};
use ::utils_error::ereport;

use ::types_nbtree::{
    BTArrayKeyInfo, BTCycleId, BTReadPageState, BTScanInsert, BTScanInsertData, BTScanOpaqueData,
    BTScanPosIsPinned, BTScanPosIsValid, BTStack, BT_IS_POSTING, BT_OFFSET_MASK,
    BT_PIVOT_HEAP_TID_ATTR, BTMaxItemSize, BTORDER_PROC, BTP_DELETED,
    BTP_HALF_DEAD, BTP_HAS_GARBAGE, BTP_LEAF, BTREE_NOVAC_VERSION, BTREE_VERSION, INDEX_ALT_TID_MASK,
    MAX_BT_CYCLE_ID, MaxIndexTuplesPerPage, P_FIRSTKEY, P_HIKEY, P_NONE,
};
use ::rel::Relation;
use ::types_scan::scankey::{
    ScanKeyData, StrategyNumber, BTEqualStrategyNumber, BTGreaterEqualStrategyNumber,
    BTGreaterStrategyNumber, BTLessEqualStrategyNumber, BTLessStrategyNumber, InvalidStrategy,
    SK_BT_DESC, SK_BT_MAXVAL, SK_BT_MINVAL, SK_BT_NULLS_FIRST, SK_BT_SKIP, SK_ISNULL, SK_ROW_END,
    SK_ROW_HEADER, SK_ROW_MEMBER, SK_SEARCHARRAY, SK_SEARCHNOTNULL, SK_SEARCHNULL,
};
use ::types_scan::sdir::{
    ScanDirection, ScanDirectionIsBackward, ScanDirectionIsForward, ScanDirectionIsNoMovement,
};
use ::types_storage::storage::Buffer;
use ::types_tuple::heaptuple::Datum;
use ::types_tuple::heaptuple::{IndexTupleData, IndexTupleSize, ItemPointerData};

use ::page::{
    ItemIdIsDead, ItemPointerCompare, ItemPointerEquals, ItemPointerGetBlockNumber,
    ItemPointerGetBlockNumberNoCheck, ItemPointerGetOffsetNumber,
    ItemPointerGetOffsetNumberNoCheck, ItemPointerSetOffsetNumber, PageGetItem, PageGetItemId,
    PageGetMaxOffsetNumber, PageGetSpecialPointer, PageRef,
};

use bufmgr_seams as bufmgr;
use compare_seams as nbtcompare;
use lsyscache_seams as lsyscache;
use fmgr_seams as fmgr;
use indextuple_seams as indextuple;
use indexam_seams as indexam;
use nbtree_core_seams as core_seams;

// ===========================================================================
// Constants (access/nbtree.h, access/skey.h, commands/progress.h, etc.).
// ===========================================================================

/// `LOOK_AHEAD_REQUIRED_RECHECKS` (nbtutils.c).
const LOOK_AHEAD_REQUIRED_RECHECKS: i16 = 3;
/// `LOOK_AHEAD_DEFAULT_DISTANCE` (nbtutils.c).
const LOOK_AHEAD_DEFAULT_DISTANCE: i16 = 5;
/// `NSKIPADVANCES_THRESHOLD` (nbtutils.c).
const NSKIPADVANCES_THRESHOLD: i16 = 3;

/// Support-function (amproc) numbers, `access/nbtree.h`.
const BTEQUALIMAGE_PROC: i16 = 4;

/// `InvalidOid`.
const InvalidOid: Oid = 0;
/// `InvalidStrategy` (`access/stratnum.h`).
const InvalidStrategyConst: StrategyNumber = InvalidStrategy;
/// `InvalidAttrNumber` (`access/attnum.h`).
const InvalidAttrNumber: AttrNumber = 0;

/// `SK_BT_INDOPTION_SHIFT` (`access/nbtree.h`).
const SK_BT_INDOPTION_SHIFT: i32 = 24;
/// nbtree-private `sk_flags` bits (`access/nbtree.h`).
const SK_BT_NEXT: i32 = 0x00200000;
const SK_BT_PRIOR: i32 = 0x00400000;
/// `SK_BT_REQFWD` — required to continue a forward scan.
const SK_BT_REQFWD: i32 = 0x00010000;
/// `SK_BT_REQBKWD` — required to continue a backward scan.
const SK_BT_REQBKWD: i32 = 0x00020000;

/// `FirstOffsetNumber` (`storage/off.h`).
const FirstOffsetNumber: OffsetNumber = 1;

/// `BTP_*` page status flags (`access/nbtree.h`).
const BTP_LEAF_FLAG: u16 = BTP_LEAF;

/// `INDEX_SIZE_MASK` (`access/itup.h`) — t_info bits holding the tuple size.
const INDEX_SIZE_MASK: u16 = 0x1FFF;

/// `MAXIMUM_ALIGNOF` (`pg_config.h`).
const MAXIMUM_ALIGNOF: usize = 8;

/// `BTMaxItemSizeNoHeapTid` (`access/nbtree.h`): the older (v2/v3) per-item
/// limit, which does not reserve room for a tiebreaker heap-TID attribute.
///
/// `MAXALIGN_DOWN((BLCKSZ - MAXALIGN(SizeOfPageHeaderData + 3*sizeof(ItemIdData))`
/// ` - MAXALIGN(sizeof(BTPageOpaqueData))) / 3)`
///   = `MAXALIGN_DOWN((8192 - 40 - 16) / 3)` = 2712.
const BTMaxItemSizeNoHeapTid: Size = 2712;

/// `BT_READ` (`access/nbtree.h`) — share-lock a buffer (the `lock_buffer`
/// seam's `mode` arg).
const BT_READ: i32 = 1; // BUFFER_LOCK_SHARE

// commands/progress.h index-build phase constants.
const PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE: i64 = 1;
const PROGRESS_BTREE_PHASE_INDEXBUILD_TABLESCAN: i64 = 2;
const PROGRESS_BTREE_PHASE_PERFORMSORT_1: i64 = 3;
const PROGRESS_BTREE_PHASE_PERFORMSORT_2: i64 = 4;
const PROGRESS_BTREE_PHASE_LEAF_LOAD: i64 = 5;

// ===========================================================================
// Small inline helpers (c.h / common/int.h / access/nbtree.h macros).
// ===========================================================================

/// `MAXALIGN(len)` (`c.h`).
#[inline]
const fn maxalign(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `OidIsValid(oid)`.
#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// `INVERT_COMPARE_RESULT(var)` (c.h): `var = (var < 0) ? 1 : -(var)`.
#[inline]
fn invert_compare_result(var: i32) -> i32 {
    if var < 0 {
        1
    } else {
        -var
    }
}

/// `-dir` (sdir.h `ScanDirectionIsForward(d) ? Backward : Forward`, with
/// NoMovement self-inverse). `ScanDirection` has no `Neg` impl.
#[inline]
fn neg_dir(dir: ScanDirection) -> ScanDirection {
    match dir {
        ScanDirection::ForwardScanDirection => ScanDirection::BackwardScanDirection,
        ScanDirection::BackwardScanDirection => ScanDirection::ForwardScanDirection,
        ScanDirection::NoMovementScanDirection => ScanDirection::NoMovementScanDirection,
    }
}

/// `OffsetNumberNext(offsetNumber)`.
#[inline]
fn offset_number_next(offset_number: OffsetNumber) -> OffsetNumber {
    offset_number + 1
}

/// `DatumGetBool(d)` — low bit of the fmgr result word.
#[inline]
fn datum_get_bool(d: datum::Datum) -> bool {
    (d.as_usize() & 1) != 0
}


/// The `fn_oid` carried by an ORDER-proc fmgr handle (low 32 bits). The
/// `so->orderProcs[]` slots are `u64` handles whose low word is the proc OID
/// (mirrors `nbtpreprocesskeys::handle_fn_oid`).
#[inline]
fn handle_fn_oid(handle: u64) -> Oid {
    handle as u32
}

// ===========================================================================
// Relcache index-metadata reads (the per-column arrays nbtutils reads).
// ===========================================================================

/// `IndexRelationGetNumberOfKeyAttributes(rel)` (`utils/rel.h`).
#[inline]
fn rel_nkeyatts(rel: &Relation) -> i32 {
    rel.indnkeyatts()
}

/// `IndexRelationGetNumberOfAttributes(rel)` (`utils/rel.h`).
#[inline]
fn rel_natts(rel: &Relation) -> i32 {
    rel.rd_att.natts
}

/// `RelationGetRelationName(rel)`.
#[inline]
fn rel_name<'a>(rel: &'a Relation<'a>) -> &'a str {
    rel.name()
}

/// `rel->rd_indoption[attno - 1]` (`utils/rel.h`).
#[inline]
fn rd_indoption(rel: &Relation, attno: AttrNumber) -> i16 {
    rel.rd_indoption[(attno - 1) as usize]
}

/// `rel->rd_opfamily[attno - 1]` (`utils/rel.h`).
#[inline]
fn rd_opfamily(rel: &Relation, attno: AttrNumber) -> Oid {
    rel.rd_opfamily[(attno - 1) as usize]
}

/// `rel->rd_opcintype[attno - 1]` (`utils/rel.h`).
#[inline]
fn rd_opcintype(rel: &Relation, attno: AttrNumber) -> Oid {
    rel.rd_opcintype[(attno - 1) as usize]
}

/// `rel->rd_indcollation[attno - 1]` (`utils/rel.h`).
#[inline]
fn rd_indcollation(rel: &Relation, attno: AttrNumber) -> Oid {
    rel.rd_indcollation[(attno - 1) as usize]
}

/// `RelationGetDescr(rel)` per-attribute `(attbyval, attlen)` projection
/// (`TupleDescCompactAttr`).
#[inline]
fn compact_attr(rel: &Relation, attoff: i32) -> (bool, i16) {
    let att = rel.rd_att.attr(attoff as usize);
    (att.attbyval, att.attlen)
}

// ===========================================================================
// On-disk byte codec (IndexTuple header / BTPageOpaqueData / heap-TID / posting
// list). Decoded with safe field-by-field reads, the same idiomatic style as
// the nbtdedup / nbtsplitloc crates.
// ===========================================================================

/// `BTPageOpaqueData` flag/link fields decoded from a page's special area.
#[derive(Clone, Copy, Debug, Default)]
struct PageOpaque {
    btpo_next: BlockNumber,
    btpo_flags: u16,
}

/// Read an [`ItemPointerData`] (6 `#[repr(C)]` bytes) from the start of `bytes`.
fn read_ipd(bytes: &[u8]) -> ItemPointerData {
    debug_assert!(bytes.len() >= 6);
    ItemPointerData {
        ip_blkid: ::types_tuple::heaptuple::BlockIdData {
            bi_hi: u16::from_ne_bytes([bytes[0], bytes[1]]),
            bi_lo: u16::from_ne_bytes([bytes[2], bytes[3]]),
        },
        ip_posid: u16::from_ne_bytes([bytes[4], bytes[5]]),
    }
}

/// Write an [`ItemPointerData`] (6 bytes) into `bytes` at byte offset `off`.
fn write_ipd(bytes: &mut [u8], off: usize, ipd: &ItemPointerData) {
    bytes[off..off + 2].copy_from_slice(&ipd.ip_blkid.bi_hi.to_ne_bytes());
    bytes[off + 2..off + 4].copy_from_slice(&ipd.ip_blkid.bi_lo.to_ne_bytes());
    bytes[off + 4..off + 6].copy_from_slice(&ipd.ip_posid.to_ne_bytes());
}

/// Decode the leading bytes of a page item as an [`IndexTupleData`] header.
fn index_tuple_header(tuple: &[u8]) -> IndexTupleData {
    debug_assert!(tuple.len() >= 8);
    let t_tid = read_ipd(&tuple[0..6]);
    let t_info = u16::from_ne_bytes([tuple[6], tuple[7]]);
    IndexTupleData { t_tid, t_info }
}

/// Write an [`IndexTupleData`] header back into the leading bytes of `tuple`.
fn write_index_tuple_header(tuple: &mut [u8], hdr: &IndexTupleData) {
    write_ipd(tuple, 0, &hdr.t_tid);
    tuple[6..8].copy_from_slice(&hdr.t_info.to_ne_bytes());
}

/// `BTPageGetOpaque(page)` — decode `(btpo_next, btpo_flags)` from the page
/// special area (the nbtree special area is 16 bytes).
fn bt_page_get_opaque(page: &PageRef<'_>) -> PgResult<PageOpaque> {
    let special = PageGetSpecialPointer(page)?;
    let rd_u32 = |off: usize| -> u32 {
        u32::from_ne_bytes([
            special[off],
            special[off + 1],
            special[off + 2],
            special[off + 3],
        ])
    };
    let rd_u16 = |off: usize| -> u16 { u16::from_ne_bytes([special[off], special[off + 1]]) };
    Ok(PageOpaque {
        btpo_next: rd_u32(4),
        btpo_flags: rd_u16(12),
    })
}

/// `P_RIGHTMOST(opaque)`.
#[inline]
fn p_rightmost(opaque: &PageOpaque) -> bool {
    opaque.btpo_next == P_NONE
}

/// `P_ISLEAF(opaque)`.
#[inline]
fn p_isleaf(opaque: &PageOpaque) -> bool {
    (opaque.btpo_flags & BTP_LEAF_FLAG) != 0
}

/// `P_IGNORE(opaque)`.
#[inline]
fn p_ignore(opaque: &PageOpaque) -> bool {
    (opaque.btpo_flags & (BTP_DELETED | BTP_HALF_DEAD)) != 0
}

/// `P_FIRSTDATAKEY(opaque)`.
#[inline]
fn p_firstdatakey(opaque: &PageOpaque) -> OffsetNumber {
    if p_rightmost(opaque) {
        P_HIKEY
    } else {
        P_FIRSTKEY
    }
}

/// `BTreeTupleIsPivot(itup)`.
#[inline]
fn bt_tuple_is_pivot(itup: &IndexTupleData) -> bool {
    if (itup.t_info & INDEX_ALT_TID_MASK) == 0 {
        return false;
    }
    (ItemPointerGetOffsetNumberNoCheck(&itup.t_tid) & BT_IS_POSTING) == 0
}

/// `BTreeTupleIsPosting(itup)`.
#[inline]
fn bt_tuple_is_posting(itup: &IndexTupleData) -> bool {
    if (itup.t_info & INDEX_ALT_TID_MASK) == 0 {
        return false;
    }
    (ItemPointerGetOffsetNumberNoCheck(&itup.t_tid) & BT_IS_POSTING) != 0
}

/// `BTreeTupleGetNPosting(posting)`.
#[inline]
fn bt_tuple_get_nposting(posting: &IndexTupleData) -> u16 {
    debug_assert!(bt_tuple_is_posting(posting));
    ItemPointerGetOffsetNumberNoCheck(&posting.t_tid) & BT_OFFSET_MASK
}

/// `BTreeTupleGetPostingOffset(posting)`.
#[inline]
fn bt_tuple_get_posting_offset(posting: &IndexTupleData) -> u32 {
    debug_assert!(bt_tuple_is_posting(posting));
    ItemPointerGetBlockNumberNoCheck(&posting.t_tid)
}

/// `BTreeTupleGetNAtts(itup, rel)`.
#[inline]
fn bt_tuple_get_natts(itup: &IndexTupleData, indnatts: u16) -> u16 {
    if bt_tuple_is_pivot(itup) {
        ItemPointerGetOffsetNumberNoCheck(&itup.t_tid) & BT_OFFSET_MASK
    } else {
        indnatts
    }
}

/// `BTreeTupleSetNAtts(itup, nkeyatts, heaptid)`.
#[inline]
fn bt_tuple_set_natts(itup: &mut IndexTupleData, nkeyatts: u16, heaptid: bool) {
    debug_assert!(!bt_tuple_is_pivot(itup) || nkeyatts == 0);
    itup.t_info |= INDEX_ALT_TID_MASK;
    let mut nkeyatts = nkeyatts;
    if heaptid {
        nkeyatts |= BT_PIVOT_HEAP_TID_ATTR;
    }
    ItemPointerSetOffsetNumber(&mut itup.t_tid, nkeyatts);
    debug_assert!(bt_tuple_is_pivot(itup));
}

/// `BTreeTupleGetPostingN(posting, n)` — the `n`-th heap TID of a posting list.
fn posting_list_n(tuple: &[u8], n: usize) -> ItemPointerData {
    let hdr = index_tuple_header(tuple);
    let off = bt_tuple_get_posting_offset(&hdr) as usize;
    let item_off = off + n * core::mem::size_of::<ItemPointerData>();
    read_ipd(&tuple[item_off..])
}

/// `BTreeTupleGetHeapTID(itup)` — first/lowest heap TID, or `None` when a pivot
/// tuple's heap-TID attribute was truncated.
fn heap_tid(tuple: &[u8]) -> Option<ItemPointerData> {
    let itup = index_tuple_header(tuple);
    if bt_tuple_is_pivot(&itup) {
        if (ItemPointerGetOffsetNumberNoCheck(&itup.t_tid) & BT_PIVOT_HEAP_TID_ATTR) != 0 {
            let sz = IndexTupleSize(&itup);
            let off = sz - core::mem::size_of::<ItemPointerData>();
            return Some(read_ipd(&tuple[off..]));
        }
        None
    } else if bt_tuple_is_posting(&itup) {
        Some(posting_list_n(tuple, 0))
    } else {
        Some(itup.t_tid)
    }
}

/// `BTreeTupleGetMaxHeapTID(itup)` (non-pivot tuples only).
fn max_heap_tid(tuple: &[u8]) -> ItemPointerData {
    let itup = index_tuple_header(tuple);
    debug_assert!(!bt_tuple_is_pivot(&itup));
    if bt_tuple_is_posting(&itup) {
        let nposting = bt_tuple_get_nposting(&itup) as usize;
        posting_list_n(tuple, nposting - 1)
    } else {
        itup.t_tid
    }
}

// ===========================================================================
// Genuinely-unported callees (no producer in this repo yet). These are honest
// seam-and-panic boundaries -- NOT stubs wired to nothing -- exactly mirroring
// the blockers that `nbtpreprocesskeys` documented.
// ===========================================================================

/// `index_getattr(tuple, attno, tupdesc, &isnull)` (access/itup.h) — deform a
/// single attribute out of an index tuple's on-disk byte image, against
/// `RelationGetDescr(rel)`. Backed by the now-ported
/// `backend-access-common-indextuple` `nocache_index_getattr` seam (the
/// byte-slice variant); the scan-lifetime `Mcx` (into which a by-ref value is
/// copied) is the index relation's allocator, exactly as `rel_mcx` and the
/// `index_deform_tuple` seam thread it elsewhere. `Err` propagates the
/// detoast / `ereport(ERROR)` surface.
fn index_getattr<'mcx>(
    tuple: &[u8],
    attno: AttrNumber,
    rel: &Relation<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    let mcx = *rel.rd_opcintype.allocator();
    indextuple::nocache_index_getattr::call(mcx, tuple, attno as i32, rel.rd_att.as_ref())
}

/// `index_getprocinfo(rel, attno, BTORDER_PROC)` — the cached `FmgrInfo` for
/// the index AM's ORDER support procedure for the given attribute. Delegates to
/// the installed `indexam::index_getprocinfo` seam, which resolves the proc OID
/// off the relcache `rd_support` array and lazily materialises the `FmgrInfo`
/// (`rd_supportinfo`), exactly as search.rs's `_bt_preprocess_keys` path does.
fn index_getprocinfo<'mcx>(
    rel: &Relation<'mcx>,
    attno: AttrNumber,
    procnum: u16,
) -> PgResult<FmgrInfo> {
    indexam::index_getprocinfo::call(rel, attno, procnum)
}

/// `datumCopy(value, attbyval, attlen)` (utils/datum.c). For by-value datums
/// this is a plain word copy; for by-ref datums it allocates a fresh copy of the
/// bytes in `mcx` (here the index relation's scan-lifetime allocator, the same
/// `Mcx` `index_getattr` threads for materialised by-ref tuple datums). The
/// canonical `Datum::clone_in` is exactly `datumCopy`: it word-copies `ByVal`,
/// deep-copies `ByRef` into `mcx`, and flattens an `Expanded` datum into its
/// `ByRef` varlena image (C: `EOH_flatten_into`). `attbyval`/`attlen` are
/// encoded in the arm itself, so the explicit flags are unused (the C signature
/// is preserved for fidelity). `clone_in` propagates the `ereport(ERROR)`
/// (out-of-memory) surface that C's `palloc` would raise.
fn datum_copy<'mcx>(
    mcx: Mcx<'mcx>,
    value: &Datum<'mcx>,
    _attbyval: bool,
    _attlen: i16,
) -> PgResult<Datum<'mcx>> {
    value.clone_in(mcx)
}

/// `pfree(DatumGetPointer(sk_argument))` of a by-ref skip-array `sk_argument`.
/// In the arena model the by-reference bytes are owned by the scan-lifetime
/// `Mcx`; replacing `sk_argument` drops the old value and the arena reclaims it
/// when the context is reset/deleted, so the explicit `pfree` is a behaviour-
/// preserving no-op (C frees eagerly; the arena frees in bulk). No pointer lane
/// to free by hand.
fn pfree_datum_if_byref(_skey: &ScanKeyData, _attbyval: bool) {
    // No-op: arena-owned by-ref bytes are reclaimed with the memory context.
}

/// `array->sksup->decrement(rel, sk_argument, &underflow)` — opclass skip
/// support decrement. Dispatched through the skip-support substrate's
/// `run_skip_decrement` seam, keyed by the `SkipSupportIncDecId` token the
/// skip array recorded in `sksup_data.decrement` during
/// `PrepareSkipSupportFromOpclass` (mirrors the `nbtpreprocesskeys` sibling).
/// The `rel` argument is dropped (the in-core trivial-type kernels never read
/// it). The boundary `Datum` is the pass-by-value scalar the skip-support
/// types operate on.
fn skip_decrement(
    _rel: &Relation,
    decrement: Option<types_sortsupport::SkipSupportIncDecId>,
    arg: &Datum,
) -> PgResult<(Datum<'static>, bool)> {
    let id = decrement
        .expect("_bt_array_decrement: skip array's decrement callback must be set");
    let (res, underflow) = nbtcompare::run_skip_decrement::call(id, datum_to_word(arg)?);
    Ok((word_to_datum(res), underflow))
}

/// `array->sksup->increment(rel, sk_argument, &overflow)` — opclass skip
/// support increment. Dispatched through `run_skip_increment` keyed by the
/// `sksup_data.increment` token.
fn skip_increment(
    _rel: &Relation,
    increment: Option<types_sortsupport::SkipSupportIncDecId>,
    arg: &Datum,
) -> PgResult<(Datum<'static>, bool)> {
    let id = increment
        .expect("_bt_array_increment: skip array's increment callback must be set");
    let (res, overflow) = nbtcompare::run_skip_increment::call(id, datum_to_word(arg)?);
    Ok((word_to_datum(res), overflow))
}

/// Convert a canonical by-value `Datum` into the bare-word fmgr-seam `Datum`
/// the skip-support kernels operate on (the trivial skip-support types are all
/// pass-by-value). A by-reference argument is not produced for these types.
#[inline]
fn datum_to_word(d: &Datum) -> PgResult<datum::Datum> {
    match d {
        Datum::ByVal(w) => Ok(datum::Datum::from_usize(*w)),
        // Unreachable for the registered (all pass-by-value) skip-support types;
        // a by-ref argument would only arise once uuid/date/timestamp skip
        // support is registered (a separate keystone). Degrade to a clean query
        // error rather than killing the backend.
        _ => Err(PgError::error(
            "_bt_array_increment/decrement: by-reference skip-support argument \
             not supported for a trivial skip-support type",
        )),
    }
}

/// Project a bare-word fmgr-seam `Datum` returned by the skip-support kernels
/// back into the canonical by-value `Datum`.
#[inline]
fn word_to_datum<'mcx>(w: datum::Datum) -> Datum<'mcx> {
    Datum::ByVal(w.as_usize())
}

/// `_bt_parallel_done(scan)` (nbtree.c). nbtree.c is ported and exposes a
/// `bt_parallel_done` seam, but it needs the scan's `parallel_scan` handle, which
/// the core split does not carry on `BTScanOpaqueData` (only the full `NbtScan`
/// in nbtree.c has it); the parallel branch is unreachable from this layer.
fn bt_parallel_done(_so: &mut BTScanOpaqueData) {
    panic!("_bt_start_prim_scan: _bt_parallel_done needs the NbtScan parallel_scan handle, unreachable from BTScanOpaqueData in the core split")
}

/// `_bt_parallel_primscan_schedule(scan, currPage)` (nbtree.c). Same blocker as
/// `bt_parallel_done`: the nbtree.c seam needs the `parallel_scan` handle (+rel)
/// the core split's `BTScanOpaqueData` does not carry.
fn bt_parallel_primscan_schedule(_so: &mut BTScanOpaqueData, _curr_page: BlockNumber) {
    panic!("_bt_advance_array_keys: _bt_parallel_primscan_schedule needs the NbtScan parallel_scan handle, unreachable from BTScanOpaqueData in the core split")
}

/// `_bt_getbuf(rel, blkno, BT_READ)` (nbtpage.c) — pin+lock a block. Delegates
/// to the in-crate `page::_bt_getbuf` (nbtpage.c lives in this same crate); the
/// scan-lifetime `Mcx` is the index relation's allocator (same source as
/// `rel_mcx`). The `so->dropPin` `_bt_killitems` path re-pins the current page
/// by block number through it.
fn bt_getbuf<'mcx>(rel: &Relation<'mcx>, blkno: BlockNumber, access: i32) -> PgResult<Buffer> {
    crate::page::_bt_getbuf(rel_mcx(rel), rel, blkno, access)
}

// ===========================================================================
// ScanKeyEntryInitializeWithInfo (access/skey.c).
// ===========================================================================

/// `ScanKeyEntryInitializeWithInfo()` — initialise an insertion scankey. The
/// resolved comparison proc handle is recorded separately (see `_bt_mkscankey`),
/// so only the scalar fields are filled, matching the C struct field-for-field.
fn scan_key_entry_initialize_with_info<'mcx>(
    entry: &mut ScanKeyData<'mcx>,
    flags: i32,
    attribute_number: AttrNumber,
    strategy: StrategyNumber,
    subtype: Oid,
    collation: Oid,
    procinfo: FmgrInfo,
    argument: Datum<'mcx>,
) {
    entry.sk_flags = flags;
    entry.sk_attno = attribute_number;
    entry.sk_strategy = strategy;
    entry.sk_subtype = subtype;
    entry.sk_collation = collation;
    // C: `fmgr_info_copy(&entry->sk_func, finfo, CurrentMemoryContext)`. The
    // resolved ORDER-proc FmgrInfo is recorded directly.
    entry.sk_func = procinfo;
    entry.sk_argument = argument;
}

// ===========================================================================
// _bt_mkscankey
// ===========================================================================

/// `_bt_mkscankey()` — Build an insertion scan key that contains comparison
/// data from `itup` as well as comparator routines appropriate to the key
/// datatypes. Pass `itup == None` to build an "all truncated" template scankey.
pub fn bt_mkscankey<'mcx>(
    rel: &Relation<'mcx>,
    itup: Option<&[u8]>,
) -> PgResult<BTScanInsert<'mcx>> {
    let indnkeyatts = rel_nkeyatts(rel);
    let tupnatts = match itup {
        Some(t) => bt_tuple_get_natts(&index_tuple_header(t), rel_natts(rel) as u16) as i32,
        None => 0,
    };

    debug_assert!(tupnatts <= rel_natts(rel));

    // We'll execute search using a scan key constructed on key columns.
    // Truncated attributes and non-key attributes are omitted from the final
    // scan key.
    let mut scankeys: alloc::vec::Vec<ScanKeyData<'mcx>> =
        alloc::vec::Vec::with_capacity(indnkeyatts as usize);
    for _ in 0..indnkeyatts {
        scankeys.push(ScanKeyData::empty());
    }

    let (heapkeyspace, allequalimage) = if itup.is_some() {
        crate::page::bt_metaversion(rel)?
    } else {
        // Utility statement callers can set these fields themselves.
        (true, false)
    };

    let mut key = BTScanInsertData {
        heapkeyspace,
        allequalimage,
        anynullkeys: false, // initial assumption
        nextkey: false,     // usual case, required by btinsert
        backward: false,    // usual case, required by btinsert
        scantid: if heapkeyspace {
            itup.and_then(heap_tid)
        } else {
            None
        },
        keysz: indnkeyatts.min(tupnatts),
        scankeys,
    };

    for i in 0..indnkeyatts {
        // We can use the cached (default) support procs since no cross-type
        // comparison can be needed.
        let procinfo = index_getprocinfo(rel, (i + 1) as AttrNumber, BTORDER_PROC as u16)?;

        // Key arguments built from truncated attributes (or when caller
        // provides no tuple) are defensively represented as NULL values.
        let (arg, null) = if i < tupnatts {
            // SAFETY: itup is Some here, since tupnatts == 0 when None.
            let (d, n) = index_getattr(itup.unwrap(), (i + 1) as AttrNumber, rel)?;
            (d, n)
        } else {
            (Datum::null(), true)
        };
        let flags = (if null { SK_ISNULL } else { 0 })
            | ((rd_indoption(rel, (i + 1) as AttrNumber) as i32) << SK_BT_INDOPTION_SHIFT);
        scan_key_entry_initialize_with_info(
            &mut key.scankeys[i as usize],
            flags,
            (i + 1) as AttrNumber,
            InvalidStrategyConst,
            InvalidOid,
            rd_indcollation(rel, (i + 1) as AttrNumber),
            procinfo,
            arg,
        );
        // Record if any key attribute is NULL (or truncated).
        if null {
            key.anynullkeys = true;
        }
    }

    // In NULLS NOT DISTINCT mode, we pretend that there are no null keys, so
    // that full uniqueness check is done (C: nbtutils.c:175-176).
    if rel
        .rd_index
        .as_ref()
        .map(|ix| ix.indnullsnotdistinct)
        .unwrap_or(false)
    {
        key.anynullkeys = false;
    }

    Ok(Some(Box::new(key)))
}

// ===========================================================================
// _bt_freestack
// ===========================================================================

/// `_bt_freestack()` — free a retracement stack made by `_bt_search`. With the
/// owned boxed `BTStack` model this is a drop; the seam exists to mirror the C
/// call site exactly.
pub fn bt_freestack(stack: BTStack) {
    let mut cur = stack;
    while let Some(mut node) = cur {
        cur = node.bts_parent.take();
        // `node` (C's `ostack`) is dropped here, freeing it.
    }
}

// ===========================================================================
// _bt_compare_array_skey
// ===========================================================================

/// `_bt_compare_array_skey()` — apply array comparison function. Compares
/// caller's tuple attribute value to a scan key/array element. Returns `<0`,
/// `0`, `>0`.
fn bt_compare_array_skey<'mcx>(
    mcx: Mcx<'mcx>,
    orderproc_handle: u64,
    tupdatum: &Datum<'mcx>,
    tupnull: bool,
    arrdatum: &Datum<'mcx>,
    cur: &ScanKeyData<'mcx>,
) -> PgResult<i32> {
    debug_assert!(cur.sk_strategy == BTEqualStrategyNumber);
    debug_assert!((cur.sk_flags & (SK_BT_MINVAL | SK_BT_MAXVAL)) == 0);

    let mut result: i32 = 0;

    if tupnull {
        // NULL tupdatum
        if (cur.sk_flags & SK_ISNULL) != 0 {
            result = 0; // NULL "=" NULL
        } else if (cur.sk_flags & SK_BT_NULLS_FIRST) != 0 {
            result = -1; // NULL "<" NOT_NULL
        } else {
            result = 1; // NULL ">" NOT_NULL
        }
    } else if (cur.sk_flags & SK_ISNULL) != 0 {
        // NOT_NULL tupdatum, NULL arrdatum
        if (cur.sk_flags & SK_BT_NULLS_FIRST) != 0 {
            result = 1; // NOT_NULL ">" NULL
        } else {
            result = -1; // NOT_NULL "<" NULL
        }
    } else {
        // Like _bt_compare, the left value must come from the index tuple.
        // Canonical `Datum` lane so by-reference array element types reach the
        // ordering proc (bare-word dispatch panics on a by-reference value).
        result = fmgr::function_call2_coll_datum::call(
            mcx,
            handle_fn_oid(orderproc_handle),
            cur.sk_collation,
            tupdatum.clone(),
            arrdatum.clone(),
        )?
        .as_i32();

        // Flip the sign whenever the column is a DESC column.
        if (cur.sk_flags & SK_BT_DESC) != 0 {
            result = invert_compare_result(result);
        }
    }

    Ok(result)
}

// ===========================================================================
// _bt_binsrch_array_skey
// ===========================================================================

/// `_bt_binsrch_array_skey()` — Binary search for next matching array key.
/// Returns an index to the first array element >= caller's tupdatum, and sets
/// `*set_elem_result` to the comparison of that element against tupdatum.
pub(crate) fn bt_binsrch_array_skey<'mcx>(
    mcx: Mcx<'mcx>,
    orderproc_handle: u64,
    cur_elem_trig: bool,
    dir: ScanDirection,
    tupdatum: &Datum<'mcx>,
    tupnull: bool,
    array: &BTArrayKeyInfo<'mcx>,
    cur: &ScanKeyData<'mcx>,
) -> PgResult<(i32, i32)> {
    let mut low_elem: i32 = 0;
    let mut mid_elem: i32 = -1;
    let mut high_elem: i32 = array.num_elems - 1;
    let mut result: i32 = 0;

    debug_assert!((cur.sk_flags & SK_SEARCHARRAY) != 0);
    debug_assert!((cur.sk_flags & SK_BT_SKIP) == 0);
    debug_assert!((cur.sk_flags & SK_ISNULL) == 0); // SAOP arrays never have NULLs
    debug_assert!(cur.sk_strategy == BTEqualStrategyNumber);

    if cur_elem_trig {
        debug_assert!(!ScanDirectionIsNoMovement(dir));
        debug_assert!((cur.sk_flags & SK_BT_REQFWD) != 0);

        if ScanDirectionIsForward(dir) {
            low_elem = array.cur_elem + 1; // old cur_elem exhausted

            // Compare prospective new cur_elem (also the new lower bound).
            if high_elem >= low_elem {
                result = bt_compare_array_skey(
                    mcx,
                    orderproc_handle,
                    tupdatum,
                    tupnull,
                    &array.elem_values[low_elem as usize],
                    cur,
                )?;

                if result <= 0 {
                    // Optimistic comparison optimization worked out.
                    return Ok((low_elem, result));
                }
                mid_elem = low_elem;
                low_elem += 1; // this cur_elem exhausted, too
            }

            if high_elem < low_elem {
                // Caller needs to perform "beyond end" array advancement.
                return Ok((high_elem, 1));
            }
        } else {
            high_elem = array.cur_elem - 1; // old cur_elem exhausted

            // Compare prospective new cur_elem (also the new upper bound).
            if high_elem >= low_elem {
                result = bt_compare_array_skey(
                    mcx,
                    orderproc_handle,
                    tupdatum,
                    tupnull,
                    &array.elem_values[high_elem as usize],
                    cur,
                )?;

                if result >= 0 {
                    // Optimistic comparison optimization worked out.
                    return Ok((high_elem, result));
                }
                mid_elem = high_elem;
                high_elem -= 1; // this cur_elem exhausted, too
            }

            if high_elem < low_elem {
                // Caller needs to perform "beyond end" array advancement.
                return Ok((low_elem, -1));
            }
        }
    }

    while high_elem > low_elem {
        mid_elem = low_elem + ((high_elem - low_elem) / 2);
        result = bt_compare_array_skey(
            mcx,
            orderproc_handle,
            tupdatum,
            tupnull,
            &array.elem_values[mid_elem as usize],
            cur,
        )?;

        if result == 0 {
            // Safe to quit as soon as we see an equal array element.
            low_elem = mid_elem;
            break;
        }

        if result > 0 {
            low_elem = mid_elem + 1;
        } else {
            high_elem = mid_elem;
        }
    }

    // Caller also cares about how its tuple datum compares to the low_elem
    // datum: always set *set_elem_result with that comparison specifically.
    if low_elem != mid_elem {
        result = bt_compare_array_skey(
            mcx,
            orderproc_handle,
            tupdatum,
            tupnull,
            &array.elem_values[low_elem as usize],
            cur,
        )?;
    }

    Ok((low_elem, result))
}

// ===========================================================================
// _bt_binsrch_skiparray_skey
// ===========================================================================

/// `_bt_binsrch_skiparray_skey()` — "Binary search" within a skip array. Sets
/// `*set_elem_result` (0 = within range, -1 = below, 1 = above).
fn bt_binsrch_skiparray_skey<'mcx>(
    mcx: Mcx<'mcx>,
    cur_elem_trig: bool,
    dir: ScanDirection,
    tupdatum: &Datum<'mcx>,
    tupnull: bool,
    array: &BTArrayKeyInfo<'mcx>,
    cur: &ScanKeyData<'mcx>,
) -> PgResult<i32> {
    debug_assert!((cur.sk_flags & SK_BT_SKIP) != 0);
    debug_assert!((cur.sk_flags & SK_SEARCHARRAY) != 0);
    debug_assert!((cur.sk_flags & SK_BT_REQFWD) != 0);
    debug_assert!(array.num_elems == -1);
    debug_assert!(!ScanDirectionIsNoMovement(dir));

    if array.null_elem {
        debug_assert!(array.low_compare.is_none() && array.high_compare.is_none());
        return Ok(0);
    }

    if tupnull {
        // NULL tupdatum
        if (cur.sk_flags & SK_BT_NULLS_FIRST) != 0 {
            return Ok(-1); // NULL "<" NOT_NULL
        } else {
            return Ok(1); // NULL ">" NOT_NULL
        }
    }

    // Array inequalities determine whether tupdatum is within range.
    let mut set_elem_result = 0;
    if ScanDirectionIsForward(dir) {
        if !cur_elem_trig
            && array.low_compare.is_some()
            && !skipcmp(mcx, array.low_compare.as_ref().unwrap(), tupdatum)?
        {
            set_elem_result = -1;
        } else if array.high_compare.is_some()
            && !skipcmp(mcx, array.high_compare.as_ref().unwrap(), tupdatum)?
        {
            set_elem_result = 1;
        }
    } else {
        if !cur_elem_trig
            && array.high_compare.is_some()
            && !skipcmp(mcx, array.high_compare.as_ref().unwrap(), tupdatum)?
        {
            set_elem_result = 1;
        } else if array.low_compare.is_some()
            && !skipcmp(mcx, array.low_compare.as_ref().unwrap(), tupdatum)?
        {
            set_elem_result = -1;
        }
    }

    Ok(set_elem_result)
}

/// `DatumGetBool(FunctionCall2Coll(&cmp->sk_func, cmp->sk_collation, tupdatum,
/// cmp->sk_argument))` — evaluate a skip array's low/high boundary comparison.
fn skipcmp<'mcx>(mcx: Mcx<'mcx>, cmp: &ScanKeyData<'mcx>, tupdatum: &Datum<'mcx>) -> PgResult<bool> {
    Ok(fmgr::function_call2_coll_datum::call(
        mcx,
        cmp.sk_func.fn_oid,
        cmp.sk_collation,
        tupdatum.clone(),
        cmp.sk_argument.clone(),
    )?
    .as_bool())
}

// ===========================================================================
// _bt_skiparray_set_element / _bt_skiparray_set_isnull
// ===========================================================================

/// `_bt_skiparray_set_element()` — set a skip array scan key's `sk_argument`.
fn bt_skiparray_set_element<'mcx>(
    rel: &Relation<'mcx>,
    skey: &mut ScanKeyData<'mcx>,
    array: &BTArrayKeyInfo<'mcx>,
    set_elem_result: i32,
    tupdatum: &Datum<'mcx>,
    tupnull: bool,
) -> PgResult<()> {
    debug_assert!((skey.sk_flags & SK_BT_SKIP) != 0);
    debug_assert!((skey.sk_flags & SK_SEARCHARRAY) != 0);

    if set_elem_result != 0 {
        // tupdatum/tupnull is out of the range of the skip array.
        debug_assert!(!array.null_elem);
        bt_array_set_low_or_high(rel, skey, array, set_elem_result < 0);
        return Ok(());
    }

    // Advance skip array to tupdatum (or tupnull) value.
    if tupnull {
        bt_skiparray_set_isnull(rel, skey, array);
        return Ok(());
    }

    // Free memory previously allocated for sk_argument if needed.
    pfree_datum_if_byref(skey, array.attbyval);

    // tupdatum becomes new sk_argument/new current element.
    skey.sk_flags &= !(SK_SEARCHNULL
        | SK_ISNULL
        | SK_BT_MINVAL
        | SK_BT_MAXVAL
        | SK_BT_NEXT
        | SK_BT_PRIOR);
    skey.sk_argument = datum_copy(
        *rel.rd_opcintype.allocator(),
        tupdatum,
        array.attbyval,
        array.attlen,
    )?;
    Ok(())
}

/// `_bt_skiparray_set_isnull()` — set skip array scan key to NULL.
fn bt_skiparray_set_isnull<'mcx>(
    _rel: &Relation<'mcx>,
    skey: &mut ScanKeyData<'mcx>,
    array: &BTArrayKeyInfo<'mcx>,
) {
    debug_assert!((skey.sk_flags & SK_BT_SKIP) != 0);
    debug_assert!((skey.sk_flags & SK_SEARCHARRAY) != 0);
    debug_assert!(array.null_elem && array.low_compare.is_none() && array.high_compare.is_none());

    // Free memory previously allocated for sk_argument if needed.
    pfree_datum_if_byref(skey, array.attbyval);

    // NULL becomes new sk_argument/new current element.
    skey.sk_argument = Datum::null();
    skey.sk_flags &= !(SK_BT_MINVAL | SK_BT_MAXVAL | SK_BT_NEXT | SK_BT_PRIOR);
    skey.sk_flags |= SK_SEARCHNULL | SK_ISNULL;
}

// ===========================================================================
// _bt_start_array_keys
// ===========================================================================

/// `_bt_start_array_keys()` — Initialize array keys at start of a scan.
pub fn bt_start_array_keys<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    dir: ScanDirection,
) {
    debug_assert!(so.numArrayKeys != 0);
    debug_assert!(so.qual_ok);

    for i in 0..so.numArrayKeys as usize {
        let scan_key = so.arrayKeys[i].scan_key as usize;
        // Split the borrow: copy the array, mutate the key, then write back.
        let mut array = so.arrayKeys[i].clone();
        debug_assert!((so.keyData[scan_key].sk_flags & SK_SEARCHARRAY) != 0);
        let low_not_high = ScanDirectionIsForward(dir);
        bt_array_set_low_or_high(rel, &mut so.keyData[scan_key], &array, low_not_high);
        // C sets the SAOP array's cur_elem inside _bt_array_set_low_or_high; the
        // borrow split means the array's cur_elem is written here (see
        // array_set_cur_elem). Without this, cur_elem desyncs from sk_argument
        // on every scan start / array roll-over.
        array_set_cur_elem(&mut array, low_not_high);
        so.arrayKeys[i] = array;
    }
    so.scanBehind = false;
    so.oppositeDirCheck = false; // reset
}

// ===========================================================================
// _bt_array_set_low_or_high
// ===========================================================================

/// `_bt_array_set_low_or_high()` — Set array scan key to lowest/highest element.
///
/// Both the scankey and (for SAOP arrays) the array's `cur_elem` are updated.
/// Because the repo splits the `(skey, array)` borrow at every call site, the
/// SAOP `cur_elem` write is handled by the caller via [`array_set_cur_elem`];
/// here we set the scankey's `sk_argument`/flags and report the new cur_elem.
fn bt_array_set_low_or_high<'mcx>(
    rel: &Relation<'mcx>,
    skey: &mut ScanKeyData<'mcx>,
    array: &BTArrayKeyInfo<'mcx>,
    low_not_high: bool,
) {
    debug_assert!((skey.sk_flags & SK_SEARCHARRAY) != 0);

    if array.num_elems != -1 {
        // set low or high element for SAOP array
        debug_assert!((skey.sk_flags & SK_BT_SKIP) == 0);
        let set_elem = if low_not_high {
            0
        } else {
            array.num_elems - 1
        };
        // Just copy over array datum (only skip arrays need freeing/allocating).
        skey.sk_argument = array.elem_values[set_elem as usize].clone();
        // The caller is responsible for writing array.cur_elem = set_elem (see
        // array_set_cur_elem); record it on the scankey's collation-free path by
        // having every caller pair this with array_set_cur_elem.
        return;
    }

    // set low or high element for skip array
    debug_assert!((skey.sk_flags & SK_BT_SKIP) != 0);
    debug_assert!(array.num_elems == -1);

    // Free memory previously allocated for sk_argument if needed.
    pfree_datum_if_byref(skey, array.attbyval);

    // Reset flags.
    skey.sk_argument = Datum::null();
    skey.sk_flags &= !(SK_SEARCHNULL
        | SK_ISNULL
        | SK_BT_MINVAL
        | SK_BT_MAXVAL
        | SK_BT_NEXT
        | SK_BT_PRIOR);

    let _ = rel; // rel currently only needed for the by-ref free path above

    if array.null_elem && (low_not_high == ((skey.sk_flags & SK_BT_NULLS_FIRST) != 0)) {
        // Requested element (either lowest or highest) has the value NULL.
        skey.sk_flags |= SK_SEARCHNULL | SK_ISNULL;
    } else if low_not_high {
        // Setting array to lowest element (according to low_compare).
        skey.sk_flags |= SK_BT_MINVAL;
    } else {
        // Setting array to highest element (according to high_compare).
        skey.sk_flags |= SK_BT_MAXVAL;
    }
}

/// Companion to [`bt_array_set_low_or_high`]: write the SAOP `cur_elem` that the
/// C code sets inside `_bt_array_set_low_or_high` (the repo splits the borrow,
/// so the array's `cur_elem` is written by the caller).
#[inline]
fn array_set_cur_elem(array: &mut BTArrayKeyInfo, low_not_high: bool) {
    if array.num_elems != -1 {
        array.cur_elem = if low_not_high {
            0
        } else {
            array.num_elems - 1
        };
    }
}

// ===========================================================================
// _bt_array_decrement
// ===========================================================================

/// `_bt_array_decrement()` — decrement array scan key's `sk_argument`. Returns
/// whether the array was successfully decremented.
fn bt_array_decrement<'mcx>(
    rel: &Relation<'mcx>,
    skey: &mut ScanKeyData<'mcx>,
    array: &mut BTArrayKeyInfo<'mcx>,
) -> PgResult<bool> {
    debug_assert!((skey.sk_flags & SK_SEARCHARRAY) != 0);
    debug_assert!((skey.sk_flags & (SK_BT_MAXVAL | SK_BT_NEXT | SK_BT_PRIOR)) == 0);

    // SAOP array?
    if array.num_elems != -1 {
        debug_assert!((skey.sk_flags & (SK_BT_SKIP | SK_BT_MINVAL | SK_BT_MAXVAL)) == 0);
        if array.cur_elem > 0 {
            array.cur_elem -= 1;
            skey.sk_argument = array.elem_values[array.cur_elem as usize].clone();
            return Ok(true);
        }
        // Cannot decrement to before first array element.
        return Ok(false);
    }

    // Nope, this is a skip array.
    debug_assert!((skey.sk_flags & SK_BT_SKIP) != 0);

    // The MINVAL sentinel is never decrementable.
    if (skey.sk_flags & SK_BT_MINVAL) != 0 {
        return Ok(false);
    }

    // When current element is NULL and the lowest sorting value is also NULL,
    // we cannot decrement before the first array element.
    if (skey.sk_flags & SK_ISNULL) != 0 && (skey.sk_flags & SK_BT_NULLS_FIRST) != 0 {
        return Ok(false);
    }

    // Opclasses without skip support "decrement" by setting the PRIOR flag.
    if array.sksup.is_none() {
        skey.sk_flags |= SK_BT_PRIOR;
        return Ok(true);
    }

    // Opclasses with skip support directly decrement sk_argument.
    if (skey.sk_flags & SK_ISNULL) != 0 {
        debug_assert!((skey.sk_flags & SK_BT_NULLS_FIRST) == 0);
        // "Decrement" from NULL to the high_elem value (skip support).
        skey.sk_flags &= !(SK_SEARCHNULL | SK_ISNULL);
        let high_elem = &array.sksup_data.as_ref().unwrap().high_elem;
        skey.sk_argument = datum_copy(
            *rel.rd_opcintype.allocator(),
            high_elem,
            array.attbyval,
            array.attlen,
        )?;
        return Ok(true);
    }

    // Ask opclass support routine for a decremented copy of sk_argument.
    let decrement = array.sksup_data.as_ref().and_then(|d| d.decrement);
    let (dec_sk_argument, uflow) = skip_decrement(rel, decrement, &skey.sk_argument)?;
    if uflow {
        // dec_sk_argument has undefined value (so no pfree).
        if array.null_elem && (skey.sk_flags & SK_BT_NULLS_FIRST) != 0 {
            bt_skiparray_set_isnull(rel, skey, array);
            return Ok(true);
        }
        return Ok(false);
    }

    // Make sure the decremented value is still within the array's range.
    if let Some(low_compare) = &array.low_compare {
        if !skipcmp_arg(*rel.rd_opcintype.allocator(), low_compare, &dec_sk_argument)? {
            // Keep existing sk_argument after all.
            // (by-ref dec_sk_argument would be pfree'd here; unmodelled.)
            return Ok(false);
        }
    }

    // Accept value returned by opclass decrement callback.
    pfree_datum_if_byref(skey, array.attbyval);
    // dec_sk_argument is 'static; project it to 'mcx (only meaningful for the
    // by-value lane, which is all the canonical Datum models).
    skey.sk_argument = clone_static_into(&dec_sk_argument);
    Ok(true)
}

// ===========================================================================
// _bt_array_increment
// ===========================================================================

/// `_bt_array_increment()` — increment array scan key's `sk_argument`. Returns
/// whether the array was successfully incremented.
fn bt_array_increment<'mcx>(
    rel: &Relation<'mcx>,
    skey: &mut ScanKeyData<'mcx>,
    array: &mut BTArrayKeyInfo<'mcx>,
) -> PgResult<bool> {
    debug_assert!((skey.sk_flags & SK_SEARCHARRAY) != 0);
    debug_assert!((skey.sk_flags & (SK_BT_MINVAL | SK_BT_NEXT | SK_BT_PRIOR)) == 0);

    // SAOP array?
    if array.num_elems != -1 {
        debug_assert!((skey.sk_flags & (SK_BT_SKIP | SK_BT_MINVAL | SK_BT_MAXVAL)) == 0);
        if array.cur_elem < array.num_elems - 1 {
            array.cur_elem += 1;
            skey.sk_argument = array.elem_values[array.cur_elem as usize].clone();
            return Ok(true);
        }
        // Cannot increment past final array element.
        return Ok(false);
    }

    // Nope, this is a skip array.
    debug_assert!((skey.sk_flags & SK_BT_SKIP) != 0);

    // The MAXVAL sentinel is never incrementable.
    if (skey.sk_flags & SK_BT_MAXVAL) != 0 {
        return Ok(false);
    }

    // When current element is NULL and the highest sorting value is also NULL,
    // we cannot increment past the final element.
    if (skey.sk_flags & SK_ISNULL) != 0 && (skey.sk_flags & SK_BT_NULLS_FIRST) == 0 {
        return Ok(false);
    }

    // Opclasses without skip support "increment" by setting the NEXT flag.
    if array.sksup.is_none() {
        skey.sk_flags |= SK_BT_NEXT;
        return Ok(true);
    }

    // Opclasses with skip support directly increment sk_argument.
    if (skey.sk_flags & SK_ISNULL) != 0 {
        debug_assert!((skey.sk_flags & SK_BT_NULLS_FIRST) != 0);
        // "Increment" from NULL to the low_elem value (skip support).
        skey.sk_flags &= !(SK_SEARCHNULL | SK_ISNULL);
        let low_elem = &array.sksup_data.as_ref().unwrap().low_elem;
        skey.sk_argument = datum_copy(
            *rel.rd_opcintype.allocator(),
            low_elem,
            array.attbyval,
            array.attlen,
        )?;
        return Ok(true);
    }

    // Ask opclass support routine for an incremented copy of sk_argument.
    let increment = array.sksup_data.as_ref().and_then(|d| d.increment);
    let (inc_sk_argument, oflow) = skip_increment(rel, increment, &skey.sk_argument)?;
    if oflow {
        if array.null_elem && (skey.sk_flags & SK_BT_NULLS_FIRST) == 0 {
            bt_skiparray_set_isnull(rel, skey, array);
            return Ok(true);
        }
        return Ok(false);
    }

    // Make sure the incremented value is still within the array's range.
    if let Some(high_compare) = &array.high_compare {
        if !skipcmp_arg(*rel.rd_opcintype.allocator(), high_compare, &inc_sk_argument)? {
            return Ok(false);
        }
    }

    // Accept value returned by opclass increment callback.
    pfree_datum_if_byref(skey, array.attbyval);
    skey.sk_argument = clone_static_into(&inc_sk_argument);
    Ok(true)
}

/// `DatumGetBool(FunctionCall2Coll(&cmp->sk_func, cmp->sk_collation, value,
/// cmp->sk_argument))` for the array increment/decrement range checks.
fn skipcmp_arg<'mcx>(mcx: Mcx<'mcx>, cmp: &ScanKeyData<'mcx>, value: &Datum<'mcx>) -> PgResult<bool> {
    Ok(fmgr::function_call2_coll_datum::call(
        mcx,
        cmp.sk_func.fn_oid,
        cmp.sk_collation,
        value.clone(),
        cmp.sk_argument.clone(),
    )?
    .as_bool())
}

/// Project a `'static` (by-value) datum produced by the skip-support callbacks
/// into the scan context lifetime. Only the by-value word is meaningful in this
/// repo's canonical `Datum`, so this is a word copy.
#[inline]
fn clone_static_into<'mcx>(d: &Datum<'static>) -> Datum<'mcx> {
    Datum::from_usize(d.as_usize())
}

// ===========================================================================
// _bt_advance_array_keys_increment
// ===========================================================================

/// `_bt_advance_array_keys_increment()` — Advance to next set of array elements
/// by a single increment in the current scan direction (rolling over to
/// higher-order arrays as needed). Returns whether another set exists.
fn bt_advance_array_keys_increment<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    dir: ScanDirection,
    skip_array_set: &mut bool,
) -> PgResult<bool> {
    // Advance the last array key most quickly (lowest-order index column).
    for i in (0..so.numArrayKeys as usize).rev() {
        let scan_key = so.arrayKeys[i].scan_key as usize;
        let mut array = so.arrayKeys[i].clone();
        let mut skey = so.keyData[scan_key].clone();

        if array.num_elems == -1 {
            *skip_array_set = true;
        }

        let advanced = if ScanDirectionIsForward(dir) {
            bt_array_increment(rel, &mut skey, &mut array)?
        } else {
            bt_array_decrement(rel, &mut skey, &mut array)?
        };

        if advanced {
            so.keyData[scan_key] = skey;
            so.arrayKeys[i] = array;
            return Ok(true);
        }

        // Couldn't increment/decrement: handle array roll over. Start over at
        // the array's lowest (or highest, for backward scans) sorting value...
        let low_not_high = ScanDirectionIsForward(dir);
        bt_array_set_low_or_high(rel, &mut skey, &array, low_not_high);
        array_set_cur_elem(&mut array, low_not_high);
        so.keyData[scan_key] = skey;
        so.arrayKeys[i] = array;
        // ...then increment/decrement the next most significant array.
    }

    // The array keys are now exhausted. Restore them to the state they were in
    // immediately before we were called (ratchet only in current scan dir).
    bt_start_array_keys(rel, so, neg_dir(dir));

    Ok(false)
}

// ===========================================================================
// _bt_tuple_before_array_skeys
// ===========================================================================

/// `_bt_tuple_before_array_skeys()` — is it too early to advance required
/// arrays? See the long C comment for the readpagetup / scanBehind contract.
fn bt_tuple_before_array_skeys<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    dir: ScanDirection,
    tuple: &[u8],
    tupnatts: i32,
    readpagetup: bool,
    sktrig: i32,
    scan_behind: Option<&mut bool>,
) -> PgResult<bool> {
    debug_assert!(so.numArrayKeys != 0);
    debug_assert!(so.numberOfKeys != 0);
    debug_assert!(sktrig == 0 || readpagetup);
    debug_assert!(!readpagetup || scan_behind.is_none());

    let mut scan_behind = scan_behind;
    if let Some(sb) = scan_behind.as_deref_mut() {
        *sb = false;
    }

    let mut ikey = sktrig;
    while ikey < so.numberOfKeys {
        let cur_flags = so.keyData[ikey as usize].sk_flags;
        let cur_attno = so.keyData[ikey as usize].sk_attno;
        let cur_strategy = so.keyData[ikey as usize].sk_strategy;

        debug_assert!(!readpagetup || ikey == sktrig);

        // Once we reach a non-required scan key, we're completely done.
        if (cur_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) == 0 {
            debug_assert!(!readpagetup);
            debug_assert!(ikey > sktrig || ikey == 0);
            return Ok(false);
        }

        if cur_attno as i32 > tupnatts {
            debug_assert!(!readpagetup);
            // High key truncated attribute: assume tuple value >= equality
            // constraints, but record scanBehind.
            if let Some(sb) = scan_behind.as_deref_mut() {
                *sb = true;
            }
            return Ok(false);
        }

        // Inequality strategy keys that _bt_check_compare set continuescan=false.
        if cur_strategy != BTEqualStrategyNumber {
            if readpagetup {
                return Ok(false);
            }
            // Otherwise we must check all required scan keys to track scanBehind.
            ikey += 1;
            continue;
        }

        let (tupdatum, tupnull) = index_getattr(tuple, cur_attno, rel)?;
        let result: i32;

        if (cur_flags & (SK_BT_MINVAL | SK_BT_MAXVAL)) == 0 {
            // Scankey has a valid/comparable sk_argument value.
            let orderproc = so.orderProcs[ikey as usize];
            let arg = so.keyData[ikey as usize].sk_argument.clone();
            let mut r = bt_compare_array_skey(
                *rel.rd_opcintype.allocator(),
                orderproc,
                &tupdatum,
                tupnull,
                &arg,
                &so.keyData[ikey as usize],
            )?;

            if r == 0 {
                // Interpret result in a way that takes NEXT/PRIOR into account.
                if (cur_flags & SK_BT_NEXT) != 0 {
                    r = -1;
                } else if (cur_flags & SK_BT_PRIOR) != 0 {
                    r = 1;
                }
                debug_assert!(r == 0 || (cur_flags & SK_BT_SKIP) != 0);
            }
            result = r;
        } else {
            // MINVAL/MAXVAL sentinel: check if tupdatum is within skip range.
            debug_assert!(if ScanDirectionIsForward(dir) {
                (cur_flags & SK_BT_MAXVAL) == 0
            } else {
                (cur_flags & SK_BT_MINVAL) == 0
            });

            // Find the array matching this scan key.
            let mut arr_idx = 0usize;
            for arrayidx in 0..so.numArrayKeys as usize {
                if so.arrayKeys[arrayidx].scan_key == ikey {
                    arr_idx = arrayidx;
                    break;
                }
            }
            result = bt_binsrch_skiparray_skey(
                *rel.rd_opcintype.allocator(),
                false,
                dir,
                &tupdatum,
                tupnull,
                &so.arrayKeys[arr_idx],
                &so.keyData[ikey as usize],
            )?;

            if result == 0 {
                // tupdatum satisfies both low_compare and high_compare, so it's
                // time to advance the array keys.
                return Ok(false);
            }
        }

        // Does this comparison indicate caller must NOT advance arrays yet?
        if (ScanDirectionIsForward(dir) && result < 0)
            || (ScanDirectionIsBackward(dir) && result > 0)
        {
            return Ok(true);
        }

        // Does this comparison indicate caller should now advance arrays?
        if readpagetup || result != 0 {
            debug_assert!(result != 0);
            return Ok(false);
        }

        // Inconclusive -- check later scan keys (finaltup precheck / assertion).
        debug_assert!(result == 0);
        ikey += 1;
    }

    debug_assert!(!readpagetup);
    Ok(false)
}

// ===========================================================================
// _bt_start_prim_scan
// ===========================================================================

/// `_bt_start_prim_scan()` — start scheduled primitive index scan? Returns true
/// if `_bt_checkkeys` scheduled another primitive index scan.
pub fn bt_start_prim_scan<'mcx>(
    _rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    _dir: ScanDirection,
) -> bool {
    debug_assert!(so.numArrayKeys != 0);

    so.scanBehind = false;
    so.oppositeDirCheck = false; // reset

    if so.needPrimScan {
        // Flag was set -- must call _bt_first again (which resets needPrimScan).
        return true;
    }

    // The top-level index scan ran out of tuples in this scan direction.
    // (Parallel scans notify _bt_parallel_done; the parallel_scan handle isn't
    // carried across this seam, so this repo's single-process path is a no-op,
    // mirroring scan->parallel_scan == NULL.)
    false
}

// ===========================================================================
// _bt_advance_array_keys
// ===========================================================================

/// `_bt_advance_array_keys()` — Advance array elements using a tuple. Works as
/// a wrapper around `_bt_check_compare`; sets `pstate.continuescan` and
/// `so->needPrimScan`, and returns whether caller's tuple satisfies the new qual.
fn bt_advance_array_keys<'mcx, 'p>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    pstate: Option<&mut BTReadPageState<'mcx, 'p>>,
    tuple: &[u8],
    tupnatts: i32,
    sktrig: i32,
    sktrig_required: bool,
) -> PgResult<bool> {
    let dir = so.currPos.dir;
    let mut arrayidx = 0i32;
    let mut beyond_end_advance = false;
    let mut skip_array_advanced = false;
    let mut has_required_opposite_direction_only = false;
    let mut all_required_satisfied = true;
    let mut all_satisfied = true;

    debug_assert!(!so.needPrimScan && !so.scanBehind && !so.oppositeDirCheck);

    let mut pstate = pstate;

    if sktrig_required {
        // Once we return we'll have a new set of required array keys, so reset
        // state used by the "look ahead" optimization.
        if let Some(ps) = pstate.as_deref_mut() {
            ps.rechecks = 0;
            ps.targetdistance = 0;
        }
    } else if sktrig < so.numberOfKeys - 1
        && (so.keyData[(so.numberOfKeys - 1) as usize].sk_flags & SK_SEARCHARRAY) == 0
    {
        let mut least_sign_ikey = so.numberOfKeys - 1;
        let mut continuescan = false;

        // Optimization: precheck the least significant key during
        // !sktrig_required calls when it isn't already our sktrig.
        debug_assert!((so.keyData[sktrig as usize].sk_flags & SK_SEARCHARRAY) != 0);
        if !bt_check_compare(
            rel,
            so,
            dir,
            tuple,
            tupnatts,
            false,
            false,
            &mut continuescan,
            &mut least_sign_ikey,
        )? {
            return Ok(false);
        }
    }

    let mut ikey = 0i32;
    while ikey < so.numberOfKeys {
        let cur_flags = so.keyData[ikey as usize].sk_flags;
        let cur_strategy = so.keyData[ikey as usize].sk_strategy;
        let cur_attno = so.keyData[ikey as usize].sk_attno;
        let mut has_array = false;
        let mut required = false;
        let mut result: i32 = 0;
        let mut set_elem: i32 = 0;

        if cur_strategy == BTEqualStrategyNumber {
            // Manage array state.
            if (cur_flags & SK_SEARCHARRAY) != 0 {
                debug_assert!(so.arrayKeys[arrayidx as usize].scan_key == ikey);
                has_array = true;
            }
        } else {
            // Inequalities required in the opposite direction only?
            if (ScanDirectionIsForward(dir) && (cur_flags & SK_BT_REQBKWD) != 0)
                || (ScanDirectionIsBackward(dir) && (cur_flags & SK_BT_REQFWD) != 0)
            {
                has_required_opposite_direction_only = true;
            }
        }

        // Bump arrayidx exactly when the C `array = &so->arrayKeys[arrayidx++]`
        // executes (only for equality SEARCHARRAY keys).
        let this_array_idx = arrayidx;
        if has_array {
            arrayidx += 1;
        }

        // Optimization: skip over known-satisfied scan keys.
        if ikey < sktrig {
            ikey += 1;
            continue;
        }

        if (cur_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) != 0 {
            required = true;
            if cur_attno as i32 > tupnatts {
                debug_assert!(sktrig < ikey);
                so.scanBehind = true;
            }
        }

        // Handle a required non-array scan key that the initial _bt_check_compare
        // indicated triggered array advancement.
        if ikey == sktrig && !has_array {
            debug_assert!(sktrig_required && required && all_required_satisfied);
            // Use "beyond end" advancement.
            beyond_end_advance = true;
            all_satisfied = false;
            all_required_satisfied = false;
            ikey += 1;
            continue;
        }
        // Nothing to do with an inequality key that wasn't the sktrig.
        else if cur_strategy != BTEqualStrategyNumber {
            ikey += 1;
            continue;
        }
        // Nothing to do with a non-required, non-array equality key.
        else if !required && !has_array {
            ikey += 1;
            continue;
        }

        // Steps for all array keys after a required array whose binary search
        // triggered "beyond end" advancement.
        if beyond_end_advance {
            if has_array {
                let low_not_high = ScanDirectionIsBackward(dir);
                let array = so.arrayKeys[this_array_idx as usize].clone();
                bt_array_set_low_or_high(rel, &mut so.keyData[ikey as usize], &array, low_not_high);
                array_set_cur_elem(&mut so.arrayKeys[this_array_idx as usize], low_not_high);
            }
            ikey += 1;
            continue;
        }

        // Steps for all array keys after a required array whose tuple attribute
        // was < the closest matching key (or > for backwards scans), or for
        // truncated high key attributes.
        if !all_required_satisfied || cur_attno as i32 > tupnatts {
            if has_array {
                let low_not_high = ScanDirectionIsForward(dir);
                let array = so.arrayKeys[this_array_idx as usize].clone();
                bt_array_set_low_or_high(rel, &mut so.keyData[ikey as usize], &array, low_not_high);
                array_set_cur_elem(&mut so.arrayKeys[this_array_idx as usize], low_not_high);
            }
            ikey += 1;
            continue;
        }

        // Search in the scankey's array for the tuple attribute value.
        let (tupdatum, tupnull) = index_getattr(tuple, cur_attno, rel)?;

        if has_array {
            let cur_elem_trig = sktrig_required && ikey == sktrig;

            if so.arrayKeys[this_array_idx as usize].num_elems == -1 {
                // Skip array binary search.
                result = bt_binsrch_skiparray_skey(
                    *rel.rd_opcintype.allocator(),
                    cur_elem_trig,
                    dir,
                    &tupdatum,
                    tupnull,
                    &so.arrayKeys[this_array_idx as usize],
                    &so.keyData[ikey as usize],
                )?;
            } else {
                // SAOP array binary search.
                let orderproc = so.orderProcs[ikey as usize];
                let (se, r) = bt_binsrch_array_skey(
                    *rel.rd_opcintype.allocator(),
                    orderproc,
                    cur_elem_trig,
                    dir,
                    &tupdatum,
                    tupnull,
                    &so.arrayKeys[this_array_idx as usize],
                    &so.keyData[ikey as usize],
                )?;
                set_elem = se;
                result = r;
            }
        } else {
            debug_assert!(required);
            // Required non-array equality key treated as a degenerate array.
            let orderproc = so.orderProcs[ikey as usize];
            let arg = so.keyData[ikey as usize].sk_argument.clone();
            result = bt_compare_array_skey(
                *rel.rd_opcintype.allocator(),
                orderproc,
                &tupdatum,
                tupnull,
                &arg,
                &so.keyData[ikey as usize],
            )?;
        }

        // Consider "beyond end of array element" array advancement.
        if sktrig_required
            && required
            && ((ScanDirectionIsForward(dir) && result > 0)
                || (ScanDirectionIsBackward(dir) && result < 0))
        {
            beyond_end_advance = true;
        }

        debug_assert!(all_required_satisfied && all_satisfied);
        if result != 0 {
            all_satisfied = false;
            if sktrig_required && required {
                all_required_satisfied = false;
            } else {
                // No need to advance arrays for a non-required array. Give up.
                break;
            }
        }

        // Advance array keys, even when we don't have an exact match.
        if has_array {
            if so.arrayKeys[this_array_idx as usize].num_elems == -1 {
                // Skip array's new element is tupdatum (or MINVAL/MAXVAL).
                let array = so.arrayKeys[this_array_idx as usize].clone();
                bt_skiparray_set_element(
                    rel,
                    &mut so.keyData[ikey as usize],
                    &array,
                    result,
                    &tupdatum,
                    tupnull,
                )?;
                // If it became a valid datum, sync cur_elem via set_low_or_high
                // path is not needed (skip arrays don't track cur_elem index).
                skip_array_advanced = true;
            } else if so.arrayKeys[this_array_idx as usize].cur_elem != set_elem {
                // SAOP array's new element is set_elem datum.
                so.arrayKeys[this_array_idx as usize].cur_elem = set_elem;
                so.keyData[ikey as usize].sk_argument =
                    so.arrayKeys[this_array_idx as usize].elem_values[set_elem as usize].clone();
            }
        }

        ikey += 1;
    }

    // Advance the array keys incrementally whenever "beyond end of array
    // element" advancement happens, carrying to higher-order arrays.
    if beyond_end_advance
        && !bt_advance_array_keys_increment(rel, so, dir, &mut skip_array_advanced)?
    {
        // end_toplevel_scan
        if let Some(ps) = pstate.as_deref_mut() {
            ps.continuescan = false;
        }
        so.needPrimScan = false;
        return Ok(false);
    }

    // Maintain a page-level count of skip-array advancements.
    if sktrig_required && skip_array_advanced {
        if let Some(ps) = pstate.as_deref_mut() {
            ps.nskipadvances += 1;
        }
    }

    // Does tuple now satisfy our new qual? Recheck with _bt_check_compare.
    if (sktrig_required && all_required_satisfied) || (!sktrig_required && all_satisfied) {
        let mut nsktrig = sktrig + 1;
        let mut continuescan = false;

        debug_assert!(all_required_satisfied);

        let recheck = bt_check_compare(
            rel,
            so,
            dir,
            tuple,
            tupnatts,
            false,
            !sktrig_required,
            &mut continuescan,
            &mut nsktrig,
        )?;

        if recheck && !so.scanBehind {
            // This tuple satisfies the new qual.
            debug_assert!(all_satisfied && continuescan);
            if let Some(ps) = pstate.as_deref_mut() {
                ps.continuescan = true;
            }
            return Ok(true);
        }

        // Consider "second pass" handling of required inequalities.
        if !continuescan {
            debug_assert!(sktrig_required);
            debug_assert!(so.keyData[nsktrig as usize].sk_strategy != BTEqualStrategyNumber);
            debug_assert!(!beyond_end_advance);

            // Advance the array keys a second time using the same tuple.
            let satisfied = bt_advance_array_keys(
                rel, so, pstate, tuple, tupnatts, nsktrig, true,
            )?;
            debug_assert!(!satisfied);
            let _ = satisfied;
            return Ok(false);
        }
        // Else: some non-required key still unsatisfied; required keys remain
        // satisfied, so all_required_satisfied holds below.
    }

    // When called just to "advance" non-required arrays, this is as far as we
    // can go (cannot stop the scan for these callers).
    if !sktrig_required {
        return Ok(false);
    }

    // From here, we have a pstate (sktrig_required callers always pass one).
    // Determine the new-prim-scan / continue-scan disposition.
    enum Disp {
        NewPrimScan,
        ContinueScan,
    }

    let mut disp = Disp::ContinueScan;

    {
        let ps = pstate
            .as_deref_mut()
            .expect("_bt_advance_array_keys: sktrig_required call must carry pstate");

        // finaltup == tuple && still unsatisfied -> new primitive scan.
        let finaltup_is_tuple = ps
            .finaltup
            .map(|f| f == tuple)
            .unwrap_or(false);

        if !all_required_satisfied && finaltup_is_tuple {
            disp = Disp::NewPrimScan;
        }
    }

    if matches!(disp, Disp::ContinueScan) {
        // Proactively check finaltup. `finaltup` borrows the live page (`'p`),
        // which outlives `pstate`, so copy the slice reference out (no heap copy)
        // to release the `pstate` borrow before the `&mut so` calls below.
        let finaltup_bytes: Option<&[u8]> =
            pstate.as_deref().and_then(|ps| ps.finaltup);

        if !all_required_satisfied {
            if let Some(ft) = finaltup_bytes {
                let nfatts = bt_tuple_get_natts(&index_tuple_header(ft), rel_natts(rel) as u16)
                    as i32;
                let mut sb = so.scanBehind;
                let before = bt_tuple_before_array_skeys(
                    rel,
                    so,
                    dir,
                    ft,
                    nfatts,
                    false,
                    0,
                    Some(&mut sb),
                )?;
                so.scanBehind = sb;
                if before {
                    disp = Disp::NewPrimScan;
                }
            }
        }
    }

    if matches!(disp, Disp::ContinueScan) {
        if so.scanBehind {
            // Truncated high key -- _bt_scanbehind_checkkeys recheck scheduled.
        } else if has_required_opposite_direction_only {
            let finaltup_bytes: Option<&[u8]> =
                pstate.as_deref().and_then(|ps| ps.finaltup);
            if let Some(ft) = finaltup_bytes {
                if !bt_oppodir_checkkeys(rel, so, dir, ft)? {
                    disp = Disp::NewPrimScan;
                }
            }
        }
    }

    // new_prim_scan: consider continuing the current primscan via heuristics.
    if matches!(disp, Disp::NewPrimScan) {
        let (firstpage, nskipadvances) = {
            let ps = pstate.as_deref().unwrap();
            (ps.firstpage, ps.nskipadvances)
        };
        if !firstpage || nskipadvances > NSKIPADVANCES_THRESHOLD {
            // Schedule a recheck once on the next (or previous) page.
            so.scanBehind = true;
            // Continue the current primitive scan after all.
            disp = Disp::ContinueScan;
        } else {
            // End this primitive index scan, but schedule another.
            if let Some(ps) = pstate.as_deref_mut() {
                ps.continuescan = false; // Tell _bt_readpage we're done...
            }
            so.needPrimScan = true; // ...but call _bt_first again
            // (parallel_scan primscan scheduling: single-process no-op.)
            return Ok(false);
        }
    }

    // continue_scan:
    {
        let ps = pstate.as_deref_mut().unwrap();
        ps.continuescan = true; // Override _bt_check_compare
        so.needPrimScan = false; // _bt_readpage has more tuples to check

        if so.scanBehind {
            // Remember if recheck needs _bt_oppodir_checkkeys for next finaltup.
            so.oppositeDirCheck = has_required_opposite_direction_only;
            // skip by setting "look ahead" offnum for forwards scans.
            if ScanDirectionIsForward(dir) {
                ps.skip = ps.maxoff + 1;
            }
        }
    }

    Ok(false)
}

// ===========================================================================
// _bt_verify_keys_with_arraykeys  (USE_ASSERT_CHECKING)
// ===========================================================================

/// `_bt_verify_keys_with_arraykeys()` — verify that `so->keyData[]` agrees with
/// the array key state. Only meaningful under assert-enabled builds; provided so
/// the C call sites translate, but invoked only behind `debug_assert!`.
#[cfg(debug_assertions)]
fn bt_verify_keys_with_arraykeys(so: &BTScanOpaqueData) -> bool {
    let mut last_sk_attno: AttrNumber = InvalidAttrNumber;
    let mut arrayidx = 0i32;
    let mut nonrequiredseen = false;

    if !so.qual_ok {
        return false;
    }

    for ikey in 0..so.numberOfKeys as usize {
        let cur = &so.keyData[ikey];
        if cur.sk_strategy != BTEqualStrategyNumber || (cur.sk_flags & SK_SEARCHARRAY) == 0 {
            continue;
        }

        let array = &so.arrayKeys[arrayidx as usize];
        arrayidx += 1;
        if array.scan_key != ikey as i32 {
            return false;
        }

        if array.num_elems == 0 || array.num_elems < -1 {
            return false;
        }

        if array.num_elems != -1
            && cur.sk_argument.as_usize()
                != array.elem_values[array.cur_elem as usize].as_usize()
        {
            return false;
        }
        if (cur.sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) != 0 {
            if last_sk_attno > cur.sk_attno {
                return false;
            }
            if nonrequiredseen {
                return false;
            }
        } else {
            nonrequiredseen = true;
        }

        last_sk_attno = cur.sk_attno;
    }

    arrayidx == so.numArrayKeys
}

// ===========================================================================
// _bt_checkkeys
// ===========================================================================

/// `_bt_checkkeys()` — Test whether an index tuple satisfies all the scankey
/// conditions. Advances array keys and stops/starts primitive index scans for
/// `array_keys=true` callers. (Public: the per-page scan engine in `search.rs`
/// and amcheck call here.)
pub fn bt_checkkeys<'mcx, 'p>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    pstate: &mut BTReadPageState<'mcx, 'p>,
    array_keys: bool,
    tuple: &[u8],
    tupnatts: i32,
) -> PgResult<bool> {
    let dir = so.currPos.dir;
    let mut ikey = pstate.startikey;

    debug_assert!(!so.needPrimScan && !so.scanBehind && !so.oppositeDirCheck);
    debug_assert!(array_keys || so.numArrayKeys == 0);

    let forcenonrequired = pstate.forcenonrequired;
    let mut continuescan = false;
    let res = bt_check_compare(
        rel,
        so,
        dir,
        tuple,
        tupnatts,
        array_keys,
        forcenonrequired,
        &mut continuescan,
        &mut ikey,
    )?;
    pstate.continuescan = continuescan;

    debug_assert!(!pstate.forcenonrequired || array_keys);

    // Only one _bt_check_compare call is required when there are no equality
    // array keys. Otherwise we can only accept it unreservedly when it didn't
    // set continuescan=false.
    if !array_keys || pstate.continuescan {
        return Ok(res);
    }

    // _bt_check_compare set continuescan=false with equality array keys. The
    // tuple might be just past the end of matches; or still before the start.
    debug_assert!(!pstate.forcenonrequired);
    if bt_tuple_before_array_skeys(rel, so, dir, tuple, tupnatts, true, ikey, None)? {
        // Override _bt_check_compare, continue primitive scan.
        pstate.continuescan = true;

        pstate.rechecks += 1;
        if pstate.rechecks >= LOOK_AHEAD_REQUIRED_RECHECKS {
            // See if we should skip ahead within the current leaf page.
            bt_checkkeys_look_ahead(rel, so, pstate, tupnatts)?;
        }

        // This indextuple doesn't match the current qual, in any case.
        return Ok(false);
    }

    // Caller's tuple is >= the current array keys: must advance required arrays.
    bt_advance_array_keys(rel, so, Some(pstate), tuple, tupnatts, ikey, true)
}

// ===========================================================================
// _bt_scanbehind_checkkeys
// ===========================================================================

/// `_bt_scanbehind_checkkeys()` — Test whether caller's `finaltup` is still
/// before the start of matches for the current array keys. (Public; called by
/// the scan engine when so->scanBehind was set on the prior page.)
pub fn bt_scanbehind_checkkeys<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    dir: ScanDirection,
    finaltup: &[u8],
) -> PgResult<bool> {
    let nfinaltupatts =
        bt_tuple_get_natts(&index_tuple_header(finaltup), rel_natts(rel) as u16) as i32;

    debug_assert!(so.numArrayKeys != 0);

    let mut scan_behind = false;
    if bt_tuple_before_array_skeys(
        rel,
        so,
        dir,
        finaltup,
        nfinaltupatts,
        false,
        0,
        Some(&mut scan_behind),
    )? {
        return Ok(false);
    }

    if scan_behind {
        // Cut our losses and start a new primscan.
        return Ok(false);
    }

    if !so.oppositeDirCheck {
        return Ok(true);
    }

    bt_oppodir_checkkeys(rel, so, dir, finaltup)
}

// ===========================================================================
// _bt_oppodir_checkkeys
// ===========================================================================

/// `_bt_oppodir_checkkeys()` — Test whether an index tuple fails to satisfy an
/// inequality required in the opposite direction only.
fn bt_oppodir_checkkeys<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    dir: ScanDirection,
    finaltup: &[u8],
) -> PgResult<bool> {
    let nfinaltupatts =
        bt_tuple_get_natts(&index_tuple_header(finaltup), rel_natts(rel) as u16) as i32;
    let flipped = neg_dir(dir);
    let mut ikey = 0i32;
    let mut continuescan = false;

    debug_assert!(so.numArrayKeys != 0);

    bt_check_compare(
        rel,
        so,
        flipped,
        finaltup,
        nfinaltupatts,
        false,
        false,
        &mut continuescan,
        &mut ikey,
    )?;

    if !continuescan && so.keyData[ikey as usize].sk_strategy != BTEqualStrategyNumber {
        return Ok(false);
    }

    Ok(true)
}

// ===========================================================================
// _bt_set_startikey
// ===========================================================================

/// `_bt_set_startikey()` — Determine an offset to the first scan key that is not
/// guaranteed to be satisfied by every tuple from `pstate.page`. Sets
/// `pstate.startikey` and `pstate.forcenonrequired`. (Public; the scan engine
/// calls here at the start of reading each non-first leaf page.)
pub fn bt_set_startikey<'mcx, 'p>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    pstate: &mut BTReadPageState<'mcx, 'p>,
) -> PgResult<()> {
    let mut startikey = 0i32;
    let mut arrayidx = 0i32;
    let mut start_past_saop_eq = false;

    debug_assert!(!so.scanBehind);
    debug_assert!(pstate.minoff < pstate.maxoff);
    debug_assert!(!pstate.firstpage);
    debug_assert!(pstate.startikey == 0);

    if so.numberOfKeys == 0 {
        return Ok(());
    }

    // The page being read (owned bytes); decode line pointers from it.
    let page = PageRef::new(pstate.page)?;

    // minoff is the lowest non-pivot tuple; maxoff the highest.
    let firstiid = PageGetItemId(&page, pstate.minoff)?;
    let firsttup = PageGetItem(&page, &firstiid)?;
    let lastiid = PageGetItemId(&page, pstate.maxoff)?;
    let lasttup = PageGetItem(&page, &lastiid)?;

    // Determine the first attribute whose values change on caller's page.
    let firstchangingattnum = bt_keep_natts_fast_inner(rel, firsttup, lasttup)?;

    'keys: while startikey < so.numberOfKeys {
        let key_flags = so.keyData[startikey as usize].sk_flags;
        let key_strategy = so.keyData[startikey as usize].sk_strategy;
        let key_attno = so.keyData[startikey as usize].sk_attno as i32;

        if (key_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) == 0 {
            break; // unsafe (not marked required)
        }
        if (key_flags & SK_ROW_HEADER) != 0 {
            break; // "unsafe" (RowCompare inequalities not supported)
        }
        if key_strategy != BTEqualStrategyNumber {
            // Scalar inequality key.
            if key_attno > firstchangingattnum {
                break; // unsafe, preceding attr has multiple distinct values
            }

            let (firstdatum, firstnull) =
                index_getattr(firsttup, key_attno as AttrNumber, rel)?;
            let (lastdatum, lastnull) = index_getattr(lasttup, key_attno as AttrNumber, rel)?;

            if (key_flags & SK_ISNULL) != 0 {
                // IS NOT NULL key.
                debug_assert!((key_flags & SK_SEARCHNOTNULL) != 0);
                if firstnull || lastnull {
                    break; // unsafe
                }
                startikey += 1;
                continue;
            }

            // Test firsttup.
            if firstnull
                || !scankey_apply(rel_mcx(rel), &so.keyData[startikey as usize], &firstdatum)?
            {
                break; // unsafe
            }
            // Test lasttup.
            if lastnull || !scankey_apply(rel_mcx(rel), &so.keyData[startikey as usize], &lastdatum)? {
                break; // unsafe
            }
            startikey += 1;
            continue;
        }

        // Some = key.
        debug_assert!(key_strategy == BTEqualStrategyNumber);

        if (key_flags & SK_SEARCHARRAY) == 0 {
            // Scalar = key (possibly IS NULL).
            if key_attno >= firstchangingattnum {
                break; // unsafe, multiple distinct attr values
            }

            let (firstdatum, firstnull) =
                index_getattr(firsttup, key_attno as AttrNumber, rel)?;
            if (key_flags & SK_ISNULL) != 0 {
                // IS NULL key.
                debug_assert!((key_flags & SK_SEARCHNULL) != 0);
                if !firstnull {
                    break; // unsafe
                }
                startikey += 1;
                continue;
            }
            if firstnull || !scankey_apply(rel_mcx(rel), &so.keyData[startikey as usize], &firstdatum)? {
                break; // unsafe
            }
            startikey += 1;
            continue;
        }

        // = array key.
        let this_array_idx = arrayidx;
        arrayidx += 1;
        debug_assert!(so.arrayKeys[this_array_idx as usize].scan_key == startikey);
        if so.arrayKeys[this_array_idx as usize].num_elems != -1 {
            // SAOP array = key.
            if key_attno >= firstchangingattnum {
                break; // unsafe
            }
            let (firstdatum, firstnull) =
                index_getattr(firsttup, key_attno as AttrNumber, rel)?;
            let orderproc = so.orderProcs[startikey as usize];
            let (_se, result) = bt_binsrch_array_skey(
                *rel.rd_opcintype.allocator(),
                orderproc,
                false,
                ScanDirection::NoMovementScanDirection,
                &firstdatum,
                firstnull,
                &so.arrayKeys[this_array_idx as usize],
                &so.keyData[startikey as usize],
            )?;
            if result != 0 {
                break; // unsafe
            }
            start_past_saop_eq = true;
            startikey += 1;
            continue;
        }

        // Skip array = key.
        debug_assert!((key_flags & SK_BT_SKIP) != 0);
        if so.arrayKeys[this_array_idx as usize].null_elem {
            // Non-range skip array = key: "satisfied" by every tuple.
            startikey += 1;
            continue;
        }

        // Range skip array = key (like scalar inequality).
        if key_attno > firstchangingattnum {
            break; // unsafe
        }
        let (firstdatum, firstnull) = index_getattr(firsttup, key_attno as AttrNumber, rel)?;
        let (lastdatum, lastnull) = index_getattr(lasttup, key_attno as AttrNumber, rel)?;

        let r1 = bt_binsrch_skiparray_skey(
            *rel.rd_opcintype.allocator(),
            false,
            ScanDirection::ForwardScanDirection,
            &firstdatum,
            firstnull,
            &so.arrayKeys[this_array_idx as usize],
            &so.keyData[startikey as usize],
        )?;
        if r1 != 0 {
            break; // unsafe
        }
        let r2 = bt_binsrch_skiparray_skey(
            *rel.rd_opcintype.allocator(),
            false,
            ScanDirection::ForwardScanDirection,
            &lastdatum,
            lastnull,
            &so.arrayKeys[this_array_idx as usize],
            &so.keyData[startikey as usize],
        )?;
        if r2 != 0 {
            break 'keys; // unsafe
        }
        // Safe, range skip array satisfied by every tuple on page.
        startikey += 1;
    }

    pstate.forcenonrequired = start_past_saop_eq || so.skipScan;
    pstate.startikey = startikey;

    debug_assert!(!pstate.forcenonrequired || so.numArrayKeys != 0);
    if pstate.forcenonrequired && pstate.finaltup.is_none() {
        pstate.forcenonrequired = false;
        pstate.startikey = 0;
    }

    Ok(())
}

/// `DatumGetBool(FunctionCall2Coll(&key->sk_func, key->sk_collation, datum,
/// key->sk_argument))` for an ordinary scankey applied to a tuple value.
///
/// Uses the canonical per-attribute `Datum` lane so by-reference index column
/// types (`name`/`text`/`varlena`) pass their payloads to the operator proc;
/// the bare-word dispatch cannot carry them.
fn scankey_apply<'mcx>(
    mcx: Mcx<'mcx>,
    key: &ScanKeyData<'mcx>,
    datum: &Datum<'mcx>,
) -> PgResult<bool> {
    Ok(fmgr::function_call2_coll_datum::call(
        mcx,
        key.sk_func.fn_oid,
        key.sk_collation,
        datum.clone(),
        key.sk_argument.clone(),
    )?
    .as_bool())
}

// ===========================================================================
// _bt_check_compare
// ===========================================================================

/// `_bt_check_compare()` — Test whether an index tuple satisfies the current
/// scan condition. Subroutine for `_bt_checkkeys`. Sets `*continuescan` and
/// `*ikey`.
fn bt_check_compare<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    dir: ScanDirection,
    tuple: &[u8],
    tupnatts: i32,
    advancenonrequired: bool,
    forcenonrequired: bool,
    continuescan: &mut bool,
    ikey: &mut i32,
) -> PgResult<bool> {
    *continuescan = true; // default assumption

    while *ikey < so.numberOfKeys {
        let key_flags = so.keyData[*ikey as usize].sk_flags;
        let key_attno = so.keyData[*ikey as usize].sk_attno;
        let key_strategy = so.keyData[*ikey as usize].sk_strategy;

        let mut required_same_dir = false;
        let mut required_opposite_dir_only = false;

        if forcenonrequired {
            // treating scan's keys as non-required
        } else if ((key_flags & SK_BT_REQFWD) != 0 && ScanDirectionIsForward(dir))
            || ((key_flags & SK_BT_REQBKWD) != 0 && ScanDirectionIsBackward(dir))
        {
            required_same_dir = true;
        } else if ((key_flags & SK_BT_REQFWD) != 0 && ScanDirectionIsBackward(dir))
            || ((key_flags & SK_BT_REQBKWD) != 0 && ScanDirectionIsForward(dir))
        {
            required_opposite_dir_only = true;
        }

        if key_attno as i32 > tupnatts {
            // Truncated attribute (must be high key): assume it passes.
            debug_assert!(bt_tuple_is_pivot(&index_tuple_header(tuple)));
            *ikey += 1;
            continue;
        }

        // Skip array sentinel value: fall back on _bt_tuple_before_array_skeys.
        if (key_flags & (SK_BT_MINVAL | SK_BT_MAXVAL | SK_BT_NEXT | SK_BT_PRIOR)) != 0 {
            debug_assert!((key_flags & SK_SEARCHARRAY) != 0);
            debug_assert!((key_flags & SK_BT_SKIP) != 0);
            debug_assert!(required_same_dir || forcenonrequired);

            if forcenonrequired {
                return bt_advance_array_keys(rel, so, None, tuple, tupnatts, *ikey, false);
            }

            *continuescan = false;
            return Ok(false);
        }

        // Row-comparison keys need special processing.
        if (key_flags & SK_ROW_HEADER) != 0 {
            let key = so.keyData[*ikey as usize].clone();
            if bt_check_rowcompare(&key, tuple, tupnatts, rel, dir, forcenonrequired, continuescan)?
            {
                *ikey += 1;
                continue;
            }
            return Ok(false);
        }

        let (datum, is_null) = index_getattr(tuple, key_attno, rel)?;

        if (key_flags & SK_ISNULL) != 0 {
            // Handle IS NULL/NOT NULL tests.
            if (key_flags & SK_SEARCHNULL) != 0 {
                if is_null {
                    *ikey += 1;
                    continue; // tuple satisfies this qual
                }
            } else {
                debug_assert!((key_flags & SK_SEARCHNOTNULL) != 0);
                if !is_null {
                    *ikey += 1;
                    continue; // tuple satisfies this qual
                }
            }

            // Tuple fails this qual.
            if required_same_dir {
                *continuescan = false;
            } else if (key_flags & SK_BT_SKIP) != 0 {
                // Non-range skip array NULL element: satisfied, no advance.
                debug_assert!(forcenonrequired && *ikey > 0);
                *ikey += 1;
                continue;
            }
            return Ok(false);
        }

        if is_null {
            // Scalar scan key isn't satisfied by NULL tuple value.
            if forcenonrequired && (key_flags & SK_BT_SKIP) != 0 {
                return bt_advance_array_keys(rel, so, None, tuple, tupnatts, *ikey, false);
            }

            if (key_flags & SK_BT_NULLS_FIRST) != 0 {
                if (required_same_dir || required_opposite_dir_only)
                    && ScanDirectionIsBackward(dir)
                {
                    *continuescan = false;
                }
            } else {
                if (required_same_dir || required_opposite_dir_only)
                    && ScanDirectionIsForward(dir)
                {
                    *continuescan = false;
                }
            }
            return Ok(false);
        }

        if !scankey_apply(rel_mcx(rel), &so.keyData[*ikey as usize], &datum)? {
            // Tuple fails this qual.
            if required_same_dir {
                *continuescan = false;
            } else if advancenonrequired
                && key_strategy == BTEqualStrategyNumber
                && (key_flags & SK_SEARCHARRAY) != 0
            {
                return bt_advance_array_keys(rel, so, None, tuple, tupnatts, *ikey, false);
            }
            return Ok(false);
        }

        *ikey += 1;
    }

    // The tuple passes all index quals.
    Ok(true)
}

// ===========================================================================
// _bt_check_rowcompare
// ===========================================================================

/// `_bt_check_rowcompare()` — Test whether an index tuple satisfies a
/// row-comparison scan condition. Subroutine for `_bt_check_compare`. The
/// subsidiary keys live in `header.sk_subkeys` (the owned model of C's
/// `DatumGetPointer(sk_argument)` chain).
fn bt_check_rowcompare<'mcx>(
    header: &ScanKeyData<'mcx>,
    tuple: &[u8],
    tupnatts: i32,
    rel: &Relation<'mcx>,
    dir: ScanDirection,
    forcenonrequired: bool,
    continuescan: &mut bool,
) -> PgResult<bool> {
    debug_assert!((header.sk_flags & SK_ROW_HEADER) != 0);

    let subkeys = header
        .sk_subkeys
        .as_ref()
        .ok_or_else(|| PgError::error("_bt_check_rowcompare: row header lacks subkeys"))?;

    debug_assert!(subkeys[0].sk_attno == header.sk_attno);
    debug_assert!(subkeys[0].sk_strategy == header.sk_strategy);

    let mut idx = 0usize;
    let mut cmpresult: i32 = 0;

    // Loop over columns of the row condition.
    loop {
        let subkey = &subkeys[idx];
        debug_assert!((subkey.sk_flags & SK_ROW_MEMBER) != 0);

        // When a NULL row member is compared, the row never matches.
        if (subkey.sk_flags & SK_ISNULL) != 0 {
            debug_assert!(idx != 0);
            let prev = &subkeys[idx - 1];
            if forcenonrequired {
                // treating scan's keys as non-required
            } else if (prev.sk_flags & SK_BT_REQFWD) != 0 && ScanDirectionIsForward(dir) {
                *continuescan = false;
            } else if (prev.sk_flags & SK_BT_REQBKWD) != 0 && ScanDirectionIsBackward(dir) {
                *continuescan = false;
            }
            return Ok(false);
        }

        if subkey.sk_attno as i32 > tupnatts {
            // Truncated attribute (must be high key): assume it passes.
            debug_assert!(bt_tuple_is_pivot(&index_tuple_header(tuple)));
            return Ok(true);
        }

        let (datum, is_null) = index_getattr(tuple, subkey.sk_attno, rel)?;

        if is_null {
            if forcenonrequired {
                // treating scan's keys as non-required
            } else if (subkey.sk_flags & SK_BT_NULLS_FIRST) != 0 {
                let mut reqflags = SK_BT_REQBKWD;
                if idx == 0 {
                    reqflags |= SK_BT_REQFWD; // safe, first row member
                }
                if (subkey.sk_flags & reqflags) != 0 && ScanDirectionIsBackward(dir) {
                    *continuescan = false;
                }
            } else {
                let mut reqflags = SK_BT_REQFWD;
                if idx == 0 {
                    reqflags |= SK_BT_REQBKWD; // safe, first row member
                }
                if (subkey.sk_flags & reqflags) != 0 && ScanDirectionIsForward(dir) {
                    *continuescan = false;
                }
            }
            return Ok(false);
        }

        // Three-way comparison, not bool operator. Canonical `Datum` lane so
        // by-reference column types reach the support proc (bare-word dispatch
        // cannot carry a by-reference value and would panic).
        cmpresult = fmgr::function_call2_coll_datum::call(
            *rel.rd_opcintype.allocator(),
            subkey.sk_func.fn_oid,
            subkey.sk_collation,
            datum.clone(),
            subkey.sk_argument.clone(),
        )?
        .as_i32();

        if (subkey.sk_flags & SK_BT_DESC) != 0 {
            cmpresult = invert_compare_result(cmpresult);
        }

        // Done comparing if unequal, else advance to next column.
        if cmpresult != 0 {
            break;
        }
        if (subkey.sk_flags & SK_ROW_END) != 0 {
            break;
        }
        idx += 1;
    }

    // cmpresult is the overall result; subkeys[idx] is the deciding column.
    let deciding = &subkeys[idx];
    let result = match deciding.sk_strategy {
        s if s == BTLessStrategyNumber => cmpresult < 0,
        s if s == BTLessEqualStrategyNumber => cmpresult <= 0,
        s if s == BTGreaterEqualStrategyNumber => cmpresult >= 0,
        s if s == BTGreaterStrategyNumber => cmpresult > 0,
        other => {
            return Err(PgError::error(format!(
                "unexpected strategy number {}",
                other as i32
            )));
        }
    };

    if !result && !forcenonrequired {
        if (deciding.sk_flags & SK_BT_REQFWD) != 0 && ScanDirectionIsForward(dir) {
            *continuescan = false;
        } else if (deciding.sk_flags & SK_BT_REQBKWD) != 0 && ScanDirectionIsBackward(dir) {
            *continuescan = false;
        }
    }

    Ok(result)
}

// ===========================================================================
// _bt_checkkeys_look_ahead
// ===========================================================================

/// `_bt_checkkeys_look_ahead()` — Determine if a scan with array keys should
/// skip over uninteresting tuples; sets `pstate.skip` on success.
fn bt_checkkeys_look_ahead<'mcx, 'p>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    pstate: &mut BTReadPageState<'mcx, 'p>,
    tupnatts: i32,
) -> PgResult<()> {
    let dir = so.currPos.dir;

    debug_assert!(!pstate.forcenonrequired);

    // Avoid looking ahead when comparing the page high key.
    if pstate.offnum < pstate.minoff {
        return Ok(());
    }

    // Don't look ahead when there aren't enough tuples remaining. In C the
    // OffsetNumber operands are promoted to int before the subtract/add, so
    // `maxoff - LOOK_AHEAD_DEFAULT_DISTANCE` is signed and may go negative
    // (small page) without wrapping; mirror that with i32 arithmetic.
    if ScanDirectionIsForward(dir)
        && pstate.offnum as i32 >= pstate.maxoff as i32 - LOOK_AHEAD_DEFAULT_DISTANCE as i32
    {
        return Ok(());
    } else if ScanDirectionIsBackward(dir)
        && pstate.offnum as i32 <= pstate.minoff as i32 + LOOK_AHEAD_DEFAULT_DISTANCE as i32
    {
        return Ok(());
    }

    // The look ahead distance starts small and ramps up.
    if pstate.targetdistance == 0 {
        pstate.targetdistance = LOOK_AHEAD_DEFAULT_DISTANCE;
    } else if (pstate.targetdistance as usize) < MaxIndexTuplesPerPage / 2 {
        pstate.targetdistance *= 2;
    }

    // Don't read past the end (or before the start) of the page.
    let aheadoffnum: OffsetNumber = if ScanDirectionIsForward(dir) {
        (pstate.maxoff as i32).min(pstate.offnum as i32 + pstate.targetdistance as i32)
            as OffsetNumber
    } else {
        (pstate.minoff as i32).max(pstate.offnum as i32 - pstate.targetdistance as i32)
            as OffsetNumber
    };

    let page = PageRef::new(pstate.page)?;
    let iid = PageGetItemId(&page, aheadoffnum)?;
    let ahead = PageGetItem(&page, &iid)?.to_vec();

    if bt_tuple_before_array_skeys(rel, so, dir, &ahead, tupnatts, false, 0, None)? {
        // Success -- instruct _bt_readpage to skip ahead.
        if ScanDirectionIsForward(dir) {
            pstate.skip = aheadoffnum + 1;
        } else {
            pstate.skip = aheadoffnum - 1;
        }
    } else {
        // Failure -- "ahead" tuple is too far ahead; reset and reduce distance.
        pstate.rechecks = 0;
        pstate.targetdistance = (pstate.targetdistance / 8).max(1);
    }

    Ok(())
}

// ===========================================================================
// _bt_killitems
// ===========================================================================

/// `_bt_killitems()` — set `LP_DEAD` state for items an indexscan caller has
/// told us were killed.
pub fn bt_killitems<'mcx>(rel: &Relation<'mcx>, so: &mut BTScanOpaqueData<'mcx>) {
    // The C signature is infallible (it `Assert`s on the page protocol and gives
    // up silently on contention). The bufmgr/page protocol can return PgResult;
    // any error is unrecoverable hinting, so it just stops (matching "give up").
    if let Err(_e) = bt_killitems_inner(rel, so) {
        // best-effort hinting; LP_DEAD setting is always optional/redoable.
    }
}

fn bt_killitems_inner<'mcx>(rel: &Relation<'mcx>, so: &mut BTScanOpaqueData<'mcx>) -> PgResult<()> {
    let num_killed = so.numKilled;
    let mut killedsomething = false;

    debug_assert!(num_killed > 0);
    debug_assert!(BTScanPosIsValid(&so.currPos));

    // Always invalidate so->killedItems[] before leaving so->currPos.
    so.numKilled = 0;

    let buf: Buffer;
    if !so.dropPin {
        // We have held the pin since reading the index tuples; just lock it.
        debug_assert!(BTScanPosIsPinned(&so.currPos));
        buf = so.currPos.buf;
        core_seams::bt_lockbuf::call(rel, buf);
    } else {
        debug_assert!(!BTScanPosIsPinned(&so.currPos));
        buf = bt_getbuf(rel, so.currPos.currPage, BT_READ)?;

        let latestlsn = bufmgr::buffer_get_lsn_atomic::call(buf)?;
        debug_assert!(so.currPos.lsn <= latestlsn);
        if so.currPos.lsn != latestlsn {
            // Modified, give up on hinting.
            core_seams::bt_relbuf::call(rel, buf);
            return Ok(());
        }
        // Unmodified, hinting is safe.
    }

    // Read the page (immutable decode for offset/tuple matching). We mutate the
    // line pointers via with_buffer_page below.
    let (maxoff, kill_offsets) = bufmgr::buffer_with_page(buf, |page_bytes| {
        let opaque = {
            let page = PageRef::new(page_bytes)?;
            bt_page_get_opaque(&page)?
        };
        let minoff = p_firstdatakey(&opaque);
        let page = PageRef::new(page_bytes)?;
        let maxoff = PageGetMaxOffsetNumber(&page);

        // First pass: determine which line-pointer offsets to mark dead, by
        // matching killed kitems' heap TIDs against on-page tuples. This is read
        // only; the actual ItemIdMarkDead happens under with_buffer_page below.
        let mut kill_offsets: alloc::vec::Vec<OffsetNumber> = alloc::vec::Vec::new();

        for i in 0..num_killed as usize {
            let item_index = so.killedItems[i] as usize;
            let kitem_heaptid = so.currPos.items[item_index].heapTid;
            let mut offnum = so.currPos.items[item_index].indexOffset;

            debug_assert!(
                item_index >= so.currPos.firstItem as usize
                    && item_index <= so.currPos.lastItem as usize
            );
            if offnum < minoff {
                continue; // pure paranoia
            }
            // Track a moving kitem for posting-list read-ahead.
            let mut kitem = kitem_heaptid;
            let mut pi = i + 1;

            while offnum <= maxoff {
                let iid = PageGetItemId(&page, offnum)?;
                let ituple = PageGetItem(&page, &iid)?;
                let ithdr = index_tuple_header(ituple);
                let mut killtuple = false;

                if bt_tuple_is_posting(&ithdr) {
                    let nposting = bt_tuple_get_nposting(&ithdr) as usize;
                    let mut j = 0usize;
                    while j < nposting {
                        let item = posting_list_n(ituple, j);
                        if !ItemPointerEquals(&item, &kitem) {
                            break; // out of posting list loop
                        }
                        // Read-ahead to later kitems.
                        if pi < num_killed as usize {
                            let next_index = so.killedItems[pi] as usize;
                            pi += 1;
                            kitem = so.currPos.items[next_index].heapTid;
                        }
                        j += 1;
                    }
                    if j == nposting {
                        killtuple = true;
                    }
                } else if ItemPointerEquals(&ithdr.t_tid, &kitem) {
                    // C (nbtutils.c:3429) compares against the moving `kitem`,
                    // which posting-list read-ahead may have advanced — not the
                    // fixed original heap TID.
                    killtuple = true;
                }

                if killtuple && !ItemIdIsDead(&iid) {
                    kill_offsets.push(offnum);
                    break; // out of inner search loop
                }
                offnum = offset_number_next(offnum);
            }
        }
        Ok((maxoff, kill_offsets))
    })?;
    let _ = maxoff;

    if !kill_offsets.is_empty() {
        // Apply the LP_DEAD marks + BTP_HAS_GARBAGE flag under the buffer lock.
        bufmgr::with_buffer_page::call(buf, &mut |page_mut: &mut [u8]| -> PgResult<()> {
            for &offnum in &kill_offsets {
                let mut iid = page_item_id(page_mut, offnum)?;
                if !ItemIdIsDead(&iid) {
                    iid.mark_dead(); // ItemIdMarkDead
                    set_page_item_id(page_mut, offnum, &iid)?;
                }
            }
            // opaque->btpo_flags |= BTP_HAS_GARBAGE
            set_page_btpo_flags_garbage(page_mut)?;
            Ok(())
        })?;
        killedsomething = true;
    }

    // Mark as dirty hint (redoable).
    if killedsomething {
        bufmgr::mark_buffer_dirty_hint::call(buf, true);
    }

    if !so.dropPin {
        // _bt_unlockbuf(rel, buf): release the content lock kept by bt_lockbuf
        // without dropping the pin (bt_relbuf would drop both, but the !dropPin
        // caller keeps its pin). nbtpage.c is in this same crate, so this calls
        // the in-crate page::_bt_unlockbuf directly.
        bt_unlockbuf(rel, buf);
    } else {
        core_seams::bt_relbuf::call(rel, buf);
    }

    Ok(())
}

/// `BufferGetPage` memory context source: the scan/index workspace context.
/// The `Relation` does not carry an explicit `mcx`, but its `'mcx` `PgVec`
/// metadata is allocated in the scan-lifetime context, so its allocator is the
/// genuine context (identical to the `rel_mcx` search.rs threads for page
/// reads). Page snapshots read through it are owned `PgVec`s freed at the end
/// of the call, mirroring C reading pages into `CurrentMemoryContext`.
fn rel_mcx<'mcx>(rel: &Relation<'mcx>) -> Mcx<'mcx> {
    *rel.rd_opcintype.allocator()
}

/// `_bt_unlockbuf(rel, buf)` (nbtpage.c) — release the content lock kept by a
/// prior `_bt_lockbuf`, without dropping the pin. Delegates to the in-crate
/// `page::_bt_unlockbuf` (nbtpage.c is in this same crate).
fn bt_unlockbuf<'mcx>(rel: &Relation<'mcx>, buf: Buffer) {
    crate::page::_bt_unlockbuf(rel, buf);
}

/// `PageGetItemId(page, offnum)` reading from a mutable byte buffer.
fn page_item_id(
    page: &[u8],
    offnum: OffsetNumber,
) -> PgResult<::types_storage::bufpage::ItemIdData> {
    let p = PageRef::new(page)?;
    PageGetItemId(&p, offnum)
}

/// Write a (modified) `ItemIdData` back into the page's line-pointer array.
/// Reconstructs the 4-byte packed line pointer from its public bit fields (the
/// same packing as `ItemIdData::new`: `lp_off | lp_flags<<15 | lp_len<<17`).
fn set_page_item_id(
    page: &mut [u8],
    offnum: OffsetNumber,
    iid: &::types_storage::bufpage::ItemIdData,
) -> PgResult<()> {
    // SizeOfPageHeaderData == 24; line pointers are 4-byte structs packed after
    // the header, 1-based by offnum.
    const SIZE_OF_PAGE_HEADER_DATA: usize = 24;
    const SIZE_OF_ITEM_ID: usize = 4;
    let off = SIZE_OF_PAGE_HEADER_DATA + (offnum as usize - 1) * SIZE_OF_ITEM_ID;
    if off + SIZE_OF_ITEM_ID > page.len() {
        return Err(PgError::error("nbtutils: page item-id offset out of range"));
    }
    let raw: u32 = (iid.lp_off() as u32 & 0x7fff)
        | ((iid.lp_flags() & 0x0003) << 15)
        | ((iid.lp_len() as u32 & 0x7fff) << 17);
    page[off..off + SIZE_OF_ITEM_ID].copy_from_slice(&raw.to_ne_bytes());
    Ok(())
}

/// `BTPageGetOpaque(page)->btpo_flags |= BTP_HAS_GARBAGE` on a mutable page.
fn set_page_btpo_flags_garbage(page: &mut [u8]) -> PgResult<()> {
    // pd_special holds the byte offset of the special area; read it then OR the
    // flag word (at special + 12, a u16) with BTP_HAS_GARBAGE.
    if page.len() < 24 {
        return Err(PgError::error("nbtutils: page too small for special pointer"));
    }
    let pd_special = u16::from_ne_bytes([page[16], page[17]]) as usize;
    let flags_off = pd_special + 12;
    if flags_off + 2 > page.len() {
        return Err(PgError::error("nbtutils: special area out of range"));
    }
    let cur = u16::from_ne_bytes([page[flags_off], page[flags_off + 1]]);
    let new = cur | BTP_HAS_GARBAGE;
    page[flags_off..flags_off + 2].copy_from_slice(&new.to_ne_bytes());
    Ok(())
}

// ===========================================================================
// VACUUM cycle-ID shmem helpers (BTVacInfo).
//
// `btvacinfo` is the genuinely-shared btree vacuum cycle-id registry: the
// cycle-id counter plus the array of `(LockRelId, BTCycleId)` entries for the
// currently active VACUUMs. In C it lives in main shared memory, carved by
// `ShmemInitStruct` and interlocked by `BtreeVacuumLock`. This engine is
// thread-per-backend; per AGENTS.md "Backend-global state" the genuinely-shared
// payload is modelled as a process-local view (the same posture procarray uses
// for the ProcArray header + KnownAssignedXids ring). `BTreeShmemInit` still
// calls `ShmemInitStruct` so the cross-backend allocate-or-attach + `found`
// bookkeeping (and the `pg_get_shmem_allocations` index entry) is honoured, and
// `BtreeVacuumLock` is taken exactly where C takes it.
// ===========================================================================

/// `BTOneVacInfo` (nbtutils.c) — one active-VACUUM registry entry.
#[derive(Clone, Copy)]
struct BTOneVacInfo {
    /// `LockRelId relid` — global identifier of an index.
    relid: ::types_storage::lock::LockRelId,
    /// `BTCycleId cycleid` — cycle ID for its active VACUUM.
    cycleid: BTCycleId,
}

/// `BTVacInfo` (nbtutils.c) — the btree vacuum cycle-id shared state.
struct BTVacInfo {
    /// `BTCycleId cycle_ctr` — cycle ID most recently assigned.
    cycle_ctr: BTCycleId,
    /// `int num_vacuums` — number of currently active VACUUMs.
    num_vacuums: i32,
    /// `int max_vacuums` — allocated length of `vacuums[]`.
    max_vacuums: i32,
    /// `BTOneVacInfo vacuums[FLEXIBLE_ARRAY_MEMBER]` — the active-VACUUM array,
    /// modelled as an owned `Vec` (the C flexible member).
    vacuums: alloc::vec::Vec<BTOneVacInfo>,
}

std::thread_local! {
    /// `static BTVacInfo *btvacinfo` — this backend's view of the shared btree
    /// vacuum registry, established by [`bt_shmem_init`].
    static BTVACINFO: core::cell::RefCell<Option<BTVacInfo>> =
        const { core::cell::RefCell::new(None) };
}

/// `rel->rd_lockInfo.lockRelId` (`RelationInitLockInfo`): `relId = rd_id`,
/// `dbId = rd_locator.dbOid` (`InvalidOid` for a shared relation).
fn lock_rel_id(rel: &Relation<'_>) -> ::types_storage::lock::LockRelId {
    ::types_storage::lock::LockRelId {
        relId: rel.rd_id,
        dbId: rel.rd_locator.dbOid,
    }
}

/// Acquire `BtreeVacuumLock` in the given mode (RAII release on drop / explicit
/// `release()`), keyed by this backend's `ProcNumber`.
fn acquire_btree_vacuum_lock(
    mode: ::types_storage::LWLockMode,
) -> PgResult<lwlock::MainLWLockGuard> {
    lwlock::LWLockAcquireMain(
        ::types_storage::storage::BTREE_VACUUM_LOCK,
        mode,
        init_small_seams::my_proc_number::call(),
    )
}

/// `_bt_vacuum_cycleid()` — get the active vacuum cycle ID for an index, or 0
/// if there is no active VACUUM. (Reads the `btvacinfo` shmem array.)
pub fn bt_vacuum_cycleid(rel: &Relation) -> PgResult<BTCycleId> {
    let mut result: BTCycleId = 0;

    // Share lock is enough since this is a read-only operation.
    let guard = acquire_btree_vacuum_lock(::types_storage::LW_SHARED)?;

    let target = lock_rel_id(rel);
    BTVACINFO.with(|cell| {
        let borrow = cell.borrow();
        let info = borrow
            .as_ref()
            .expect("btvacinfo accessed before BTreeShmemInit");
        for i in 0..info.num_vacuums as usize {
            let vac = &info.vacuums[i];
            if vac.relid.relId == target.relId && vac.relid.dbId == target.dbId {
                result = vac.cycleid;
                break;
            }
        }
    });

    guard.release()?;
    Ok(result)
}

/// `_bt_start_vacuum()` — assign a cycle ID to a just-starting VACUUM operation.
/// Returns the cycle ID it was assigned.
pub fn bt_start_vacuum(rel: &Relation) -> PgResult<BTCycleId> {
    let guard = acquire_btree_vacuum_lock(::types_storage::LW_EXCLUSIVE)?;

    let target = lock_rel_id(rel);

    // The body needs to release the lock explicitly before erroring (the C
    // comment: _bt_end_vacuum must run before abort cleanup releases LWLocks),
    // so collect the outcome under the borrow and act afterwards.
    enum Outcome {
        Ok(BTCycleId),
        DuplicateVacuum,
        OutOfSlots,
    }

    let outcome = BTVACINFO.with(|cell| {
        let mut borrow = cell.borrow_mut();
        let info = borrow
            .as_mut()
            .expect("btvacinfo accessed before BTreeShmemInit");

        // Assign the next cycle ID, avoiding zero and the reserved high values.
        let mut result = info.cycle_ctr.wrapping_add(1);
        info.cycle_ctr = result;
        if result == 0 || result > MAX_BT_CYCLE_ID {
            result = 1;
            info.cycle_ctr = 1;
        }

        // Make sure there's no entry already for this index.
        for i in 0..info.num_vacuums as usize {
            let vac = &info.vacuums[i];
            if vac.relid.relId == target.relId && vac.relid.dbId == target.dbId {
                return Outcome::DuplicateVacuum;
            }
        }

        // Add an entry.
        if info.num_vacuums >= info.max_vacuums {
            return Outcome::OutOfSlots;
        }
        let idx = info.num_vacuums as usize;
        debug_assert!(idx < info.vacuums.len());
        info.vacuums[idx] = BTOneVacInfo {
            relid: target,
            cycleid: result,
        };
        info.num_vacuums += 1;
        Outcome::Ok(result)
    });

    match outcome {
        Outcome::Ok(result) => {
            guard.release()?;
            Ok(result)
        }
        Outcome::DuplicateVacuum => {
            // Unlike most places, we must release the LWLock before erroring.
            guard.release()?;
            Err(ereport(ERROR)
                .errmsg_internal(format!(
                    "multiple active vacuums for index \"{}\"",
                    rel_name(rel)
                ))
                .into_error())
        }
        Outcome::OutOfSlots => {
            guard.release()?;
            Err(ereport(ERROR)
                .errmsg_internal("out of btvacinfo slots")
                .into_error())
        }
    }
}

/// `_bt_end_vacuum()` — mark a btree VACUUM operation as done (deregister it
/// from the `btvacinfo` shmem array). Deliberately silent if no entry is found.
pub fn bt_end_vacuum(rel: &Relation) {
    let guard = acquire_btree_vacuum_lock(::types_storage::LW_EXCLUSIVE)
        .expect("BtreeVacuumLock acquisition failed in _bt_end_vacuum");

    let target = lock_rel_id(rel);
    BTVACINFO.with(|cell| {
        let mut borrow = cell.borrow_mut();
        let info = borrow
            .as_mut()
            .expect("btvacinfo accessed before BTreeShmemInit");
        for i in 0..info.num_vacuums as usize {
            let vac = info.vacuums[i];
            if vac.relid.relId == target.relId && vac.relid.dbId == target.dbId {
                // Remove it by shifting down the last entry.
                let last = (info.num_vacuums - 1) as usize;
                info.vacuums[i] = info.vacuums[last];
                info.num_vacuums -= 1;
                break;
            }
        }
    });

    guard
        .release()
        .expect("BtreeVacuumLock release failed in _bt_end_vacuum");
}

/// `sizeof(BTOneVacInfo)` (nbtutils.c) — `LockRelId` (`relId` 4 + `dbId` 4) +
/// `BTCycleId` (2), padded to the 4-byte alignment of `Oid` = 12 bytes.
const SIZEOF_BT_ONE_VAC_INFO: Size = 12;
/// `offsetof(BTVacInfo, vacuums)` (nbtutils.c) — `BTCycleId cycle_ctr` (2) +
/// `int num_vacuums` (4) + `int max_vacuums` (4), with `vacuums[]` aligned to
/// the 4-byte alignment of `Oid` inside `BTOneVacInfo` = 12.
const OFFSETOF_BT_VAC_INFO_VACUUMS: Size = 12;

/// `BTreeShmemSize()` — report shared memory space needed. Mirrors
/// `offsetof(BTVacInfo, vacuums) + MaxBackends * sizeof(BTOneVacInfo)`.
pub fn bt_shmem_size() -> PgResult<Size> {
    use ipc_shmem_seams as shmem;

    let max_backends = init_small_seams::max_backends::call() as Size;
    let size = OFFSETOF_BT_VAC_INFO_VACUUMS;
    shmem::add_size::call(
        size,
        shmem::mul_size::call(max_backends, SIZEOF_BT_ONE_VAC_INFO)?,
    )
}

/// `BTreeShmemInit()` — initialize this module's shared memory area.
pub fn bt_shmem_init() -> PgResult<()> {
    use ipc_shmem_seams as shmem;

    // Carve (or attach to) the shared "BTree Vacuum State" region so the
    // cross-backend allocate-or-attach + shmem-index bookkeeping matches C.
    let (_addr, found) = shmem::shmem_init_struct::call("BTree Vacuum State", bt_shmem_size()?)?;

    let max_vacuums = init_small_seams::max_backends::call();

    if !init_small_seams::is_under_postmaster::call() {
        // Initialize the shared memory area. Assert(!found).
        debug_assert!(!found);

        // Seed the cycle counter with low-order bits of time(), as C does.
        // SAFETY: `time(NULL)` is always safe.
        let cycle_ctr = (unsafe { libc::time(core::ptr::null_mut()) } as u64) as BTCycleId;

        BTVACINFO.with(|cell| {
            *cell.borrow_mut() = Some(BTVacInfo {
                cycle_ctr,
                num_vacuums: 0,
                max_vacuums,
                vacuums: alloc::vec![
                    BTOneVacInfo {
                        relid: ::types_storage::lock::LockRelId {
                            relId: ::types_core::primitive::InvalidOid,
                            dbId: ::types_core::primitive::InvalidOid,
                        },
                        cycleid: 0,
                    };
                    max_vacuums as usize
                ],
            });
        });
    } else {
        // Attaching backend: the genuinely-shared payload is already
        // initialised; re-publish this backend's process-local view. Assert(found).
        debug_assert!(found);
        BTVACINFO.with(|cell| {
            if cell.borrow().is_none() {
                *cell.borrow_mut() = Some(BTVacInfo {
                    cycle_ctr: 0,
                    num_vacuums: 0,
                    max_vacuums,
                    vacuums: alloc::vec![
                        BTOneVacInfo {
                            relid: ::types_storage::lock::LockRelId {
                                relId: ::types_core::primitive::InvalidOid,
                                dbId: ::types_core::primitive::InvalidOid,
                            },
                            cycleid: 0,
                        };
                        max_vacuums as usize
                    ],
                });
            }
        });
    }

    Ok(())
}

// ===========================================================================
// btoptions / btproperty / btbuildphasename
// ===========================================================================

/// `btoptions()` — parse and validate the reloptions of a btree index.
///
/// Delegates to `build_reloptions` for `RELOPT_KIND_BTREE` against the
/// `BTOptions` struct (`fillfactor`, `vacuum_cleanup_index_scale_factor`,
/// `deduplicate_items`). The reloptions parse-table assembly + `build_reloptions`
/// dispatch is owned by the reloptions module; we cross to it via the
/// `build_reloptions_btree` seam. The verbatim `reloptions` varlena bytes are
/// passed (`None` for a NULL datum) and the serialized `BTOptions` `bytea` is
/// returned (`None` when no options apply).
pub fn btoptions(
    reloptions: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<alloc::vec::Vec<u8>>> {
    reloptions_seams::build_reloptions_btree::call(reloptions, validate)
}

/// `IndexAMProperty` (`access/amapi.h`) — the boolean property `btproperty`
/// answers. Only `AMPROP_RETURNABLE` is handled here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexAMProperty {
    AmpropReturnable,
    Other,
}

/// `btproperty()` — Check boolean properties of indexes. Handles
/// `AMPROP_RETURNABLE`. On `true`, writes `res` (and clears `isnull`).
pub fn btproperty(
    _index_oid: Oid,
    attno: i32,
    prop: IndexAMProperty,
    _propname: &str,
    res: &mut bool,
    isnull: &mut bool,
) -> bool {
    match prop {
        IndexAMProperty::AmpropReturnable => {
            // answer only for columns, not AM or whole index
            if attno == 0 {
                return false;
            }
            // otherwise, btree can always return data
            *res = true;
            *isnull = false;
            true
        }
        _ => false, // punt to generic code
    }
}

/// `btbuildphasename()` — Return name of index build phase, or `None` (C NULL).
pub fn btbuildphasename(phasenum: i64) -> Option<&'static str> {
    match phasenum {
        PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE => Some("initializing"),
        PROGRESS_BTREE_PHASE_INDEXBUILD_TABLESCAN => Some("scanning table"),
        PROGRESS_BTREE_PHASE_PERFORMSORT_1 => Some("sorting live tuples"),
        PROGRESS_BTREE_PHASE_PERFORMSORT_2 => Some("sorting dead tuples"),
        PROGRESS_BTREE_PHASE_LEAF_LOAD => Some("loading tuples in tree"),
        _ => None,
    }
}

// ===========================================================================
// _bt_truncate / _bt_keep_natts / _bt_keep_natts_fast
// ===========================================================================

/// `_bt_truncate()` — create a tuple without unneeded suffix attributes.
/// Returns a truncated pivot index tuple (owned bytes over `mcx`).
pub fn bt_truncate<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    lastleft: &[u8],
    firstright: &[u8],
    itup_key: &BTScanInsertData<'mcx>,
) -> PgResult<PgVec<'mcx, u8>> {
    let nkeyatts = rel_nkeyatts(rel) as i16;

    // We should only ever truncate non-pivot tuples from leaf pages.
    debug_assert!(
        !bt_tuple_is_pivot(&index_tuple_header(lastleft))
            && !bt_tuple_is_pivot(&index_tuple_header(firstright))
    );

    // Determine how many attributes must be kept in the truncated tuple.
    let keepnatts = bt_keep_natts(rel, lastleft, firstright, itup_key)?;

    // index_truncate_tuple(itupdesc, firstright, Min(keepnatts, nkeyatts)).
    // The seam takes a FormedIndexTuple; this byte-sliced index tuple has no
    // FormedIndexTuple producer in scope. This is a genuine gap in the byte
    // lane (the same FormedTuple/byte-slice divide flagged repo-wide).
    let mut pivot = index_truncate_tuple_bytes(
        mcx,
        rel,
        firstright,
        keepnatts.min(nkeyatts as i32),
    )?;

    if bt_tuple_is_posting(&index_tuple_header(&pivot)) {
        // index_truncate_tuple() returns a straight copy of firstright when it
        // has nothing to truncate; truncate the posting list here instead.
        debug_assert!(keepnatts == nkeyatts as i32 || keepnatts == nkeyatts as i32 + 1);
        debug_assert!(rel_natts(rel) == nkeyatts as i32);
        let postingoff = bt_tuple_get_posting_offset(&index_tuple_header(firstright));
        let mut pivothdr = index_tuple_header(&pivot);
        pivothdr.t_info &= !INDEX_SIZE_MASK;
        pivothdr.t_info |= maxalign(postingoff as usize) as u16;
        write_index_tuple_header(&mut pivot, &pivothdr);
    }

    // If there's a distinguishing key attribute within pivot, we're done.
    if keepnatts <= nkeyatts as i32 {
        let mut pivothdr = index_tuple_header(&pivot);
        bt_tuple_set_natts(&mut pivothdr, keepnatts as u16, false);
        write_index_tuple_header(&mut pivot, &pivothdr);
        return Ok(pivot);
    }

    // We have to store a heap TID in the new pivot tuple.
    let pivotsz = maxalign(IndexTupleSize(&index_tuple_header(&pivot)));
    let newsize = pivotsz + maxalign(core::mem::size_of::<ItemPointerData>());
    let mut tidpivot = vec_with_capacity_in(mcx, newsize)?;
    tidpivot.resize(newsize, 0u8);
    // memcpy(tidpivot, pivot, MAXALIGN(IndexTupleSize(pivot))).
    let copylen = pivotsz.min(pivot.len());
    tidpivot[..copylen].copy_from_slice(&pivot[..copylen]);
    drop(pivot);

    // Store key attrs + a tiebreaker heap TID in the enlarged pivot tuple.
    {
        let mut tidhdr = index_tuple_header(&tidpivot);
        tidhdr.t_info &= !INDEX_SIZE_MASK;
        tidhdr.t_info |= newsize as u16;
        write_index_tuple_header(&mut tidpivot, &tidhdr);
    }
    {
        let mut tidhdr = index_tuple_header(&tidpivot);
        bt_tuple_set_natts(&mut tidhdr, nkeyatts as u16, true);
        write_index_tuple_header(&mut tidpivot, &tidhdr);
    }

    // Use lastleft's max heap TID (closest legal value to negative infinity).
    let ll_maxtid = max_heap_tid(lastleft);
    let tid_off =
        IndexTupleSize(&index_tuple_header(&tidpivot)) - core::mem::size_of::<ItemPointerData>();
    write_ipd(&mut tidpivot, tid_off, &ll_maxtid);

    // Assert heap TID invariants before returning.
    #[cfg(debug_assertions)]
    {
        let pivotheaptid = read_ipd(&tidpivot[tid_off..]);
        debug_assert!(
            ItemPointerCompare(&max_heap_tid(lastleft), &heap_tid(firstright).unwrap()) < 0
        );
        debug_assert!(ItemPointerCompare(&pivotheaptid, &heap_tid(lastleft).unwrap()) >= 0);
        debug_assert!(ItemPointerCompare(&pivotheaptid, &heap_tid(firstright).unwrap()) < 0);
    }

    Ok(tidpivot)
}

/// `index_truncate_tuple(itupdesc, source, leavenatts)` over byte-sliced index
/// tuples, via the `backend-access-common-indextuple` seam (which parses the
/// on-page byte image into a `FormedIndexTuple`, truncates against the index's
/// `rd_att`, and serializes back).
fn index_truncate_tuple_bytes<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    source: &[u8],
    leavenatts: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    indextuple::index_truncate_tuple::call(mcx, rel, source, leavenatts)
}

/// `_bt_keep_natts()` — how many key attributes to keep when truncating.
fn bt_keep_natts<'mcx>(
    rel: &Relation<'mcx>,
    lastleft: &[u8],
    firstright: &[u8],
    itup_key: &BTScanInsertData<'mcx>,
) -> PgResult<i32> {
    let nkeyatts = rel_nkeyatts(rel);

    // _bt_compare() treats truncated key attributes as minus infinity, which
    // would break searches within !heapkeyspace indexes; but we must still
    // truncate away non-key attribute values.
    if !itup_key.heapkeyspace {
        return Ok(nkeyatts);
    }

    let mut keepnatts = 1;
    for attnum in 1..=nkeyatts {
        let scankey = &itup_key.scankeys[(attnum - 1) as usize];

        let (datum1, is_null1) = index_getattr(lastleft, attnum as AttrNumber, rel)?;
        let (datum2, is_null2) = index_getattr(firstright, attnum as AttrNumber, rel)?;

        if is_null1 != is_null2 {
            break;
        }

        // Use the canonical per-attribute `Datum` lane so by-reference index
        // column types (`name`/`text`/`varlena`) pass their payloads to the
        // ordering support proc — a bare-word dispatch cannot carry them (it
        // would read a by-reference value as a scalar word and panic).
        if !is_null1
            && fmgr::function_call2_coll_datum::call(
                *rel.rd_opcintype.allocator(),
                scankey.sk_func.fn_oid,
                scankey.sk_collation,
                datum1.clone(),
                datum2.clone(),
            )?
            .as_i32()
                != 0
        {
            break;
        }

        keepnatts += 1;
    }

    // Assert that _bt_keep_natts_fast() agrees with us (allequalimage indexes).
    debug_assert!(
        !itup_key.allequalimage
            || keepnatts == bt_keep_natts_fast_inner(rel, lastleft, firstright)?
    );

    Ok(keepnatts)
}

/// `_bt_keep_natts_fast()` — fast bitwise variant of `_bt_keep_natts` (installs
/// the `bt_keep_natts_fast` seam). Exported for nbtsplitloc.c.
pub fn bt_keep_natts_fast<'mcx>(
    rel: &Relation<'mcx>,
    lastleft: &[u8],
    firstright: &[u8],
) -> PgResult<i32> {
    bt_keep_natts_fast_inner(rel, lastleft, firstright)
}

/// The body of `_bt_keep_natts_fast`, shared by the seam wrapper and the
/// in-crate callers (`_bt_keep_natts` assert, `_bt_set_startikey`).
fn bt_keep_natts_fast_inner<'mcx>(
    rel: &Relation<'mcx>,
    lastleft: &[u8],
    firstright: &[u8],
) -> PgResult<i32> {
    let keysz = rel_nkeyatts(rel);
    let mut keepnatts = 1;
    for attnum in 1..=keysz {
        let (datum1, is_null1) = index_getattr(lastleft, attnum as AttrNumber, rel)?;
        let (datum2, is_null2) = index_getattr(firstright, attnum as AttrNumber, rel)?;
        let (attbyval, attlen) = compact_attr(rel, attnum - 1);

        if is_null1 != is_null2 {
            break;
        }

        if !is_null1 && !datum_image_eq(&datum1, &datum2, attbyval, attlen)? {
            break;
        }

        keepnatts += 1;
    }
    Ok(keepnatts)
}

/// `datum_image_eq(datum1, datum2, attbyval, attlen)` (utils/datum.c) — whether
/// the in-memory images of two index-tuple datums are bit-for-bit equal.
/// Delegates to datum.c's owner through the value-model `datum_image_eq_v` seam,
/// which operates directly on the canonical `Datum` enum: by-value word
/// equality (`ByVal`), `attlen`-byte `memcmp` for fixed-length by-ref, logical
/// varlena-payload compare for `attlen == -1`, and `strlen + 1` compare for a
/// cstring (`attlen == -2`). The `index_getattr` above already detoasts into the
/// `ByRef` byte image the seam reads.
fn datum_image_eq(d1: &Datum, d2: &Datum, attbyval: bool, attlen: i16) -> PgResult<bool> {
    datum_seams::datum_image_eq_v::call(d1, d2, attbyval, attlen)
}

// ===========================================================================
// _bt_check_natts
// ===========================================================================

/// `_bt_check_natts()` — Verify a tuple has the expected number of attributes
/// for `offnum` on `page` (installs the `bt_check_natts` seam).
pub fn bt_check_natts<'mcx>(
    rel: &Relation<'mcx>,
    heapkeyspace: bool,
    page: &[u8],
    offnum: OffsetNumber,
) -> PgResult<bool> {
    let natts = rel_natts(rel) as i16;
    let nkeyatts = rel_nkeyatts(rel) as i16;
    let pageref = PageRef::new(page)?;
    let opaque = bt_page_get_opaque(&pageref)?;

    // We cannot reliably test a deleted or half-dead page (dummy high keys).
    if p_ignore(&opaque) {
        return Ok(true);
    }

    debug_assert!(offnum >= FirstOffsetNumber && offnum <= PageGetMaxOffsetNumber(&pageref));

    let itemid = PageGetItemId(&pageref, offnum)?;
    let itup = PageGetItem(&pageref, &itemid)?;
    let ituphdr = index_tuple_header(itup);
    let tupnatts = bt_tuple_get_natts(&ituphdr, natts as u16) as i32;

    // !heapkeyspace indexes do not support deduplication.
    if !heapkeyspace && bt_tuple_is_posting(&ituphdr) {
        return Ok(false);
    }

    // Posting list tuples should never have "pivot heap TID" bit set.
    if bt_tuple_is_posting(&ituphdr)
        && (ItemPointerGetOffsetNumberNoCheck(&ituphdr.t_tid) & BT_PIVOT_HEAP_TID_ATTR) != 0
    {
        return Ok(false);
    }

    // INCLUDE indexes do not support deduplication.
    if natts != nkeyatts && bt_tuple_is_posting(&ituphdr) {
        return Ok(false);
    }

    if p_isleaf(&opaque) {
        if offnum >= p_firstdatakey(&opaque) {
            // Non-pivot tuple should never be explicitly marked as pivot.
            if bt_tuple_is_pivot(&ituphdr) {
                return Ok(false);
            }
            // Non-pivot leaf tuples should never be truncated.
            return Ok(tupnatts == natts as i32);
        } else {
            debug_assert!(!p_rightmost(&opaque));
            if !heapkeyspace {
                return Ok(tupnatts == nkeyatts as i32);
            }
            // Use generic heapkeyspace pivot tuple handling.
        }
    } else {
        // !P_ISLEAF(opaque)
        if offnum == p_firstdatakey(&opaque) {
            // Negative infinity tuple: truncated to zero attributes.
            if heapkeyspace {
                return Ok(tupnatts == 0);
            }
            // Pre-v11 negative infinity tuples could have P_HIKEY offset.
            return Ok(
                tupnatts == 0 || ItemPointerGetOffsetNumber(&ituphdr.t_tid) == P_HIKEY
            );
        } else {
            if !heapkeyspace {
                return Ok(tupnatts == nkeyatts as i32);
            }
            // Use generic heapkeyspace pivot tuple handling.
        }
    }

    // Handle heapkeyspace pivot tuples (excluding minus infinity items).
    debug_assert!(heapkeyspace);

    // Explicit representation of natts is mandatory for heapkeyspace pivots.
    if !bt_tuple_is_pivot(&ituphdr) {
        return Ok(false);
    }
    // Pivot tuple should not use posting list representation (redundant).
    if bt_tuple_is_posting(&ituphdr) {
        return Ok(false);
    }
    // Heap TID can't be untruncated when another key attribute is truncated.
    if heap_tid(itup).is_some() && tupnatts != nkeyatts as i32 {
        return Ok(false);
    }
    // Pivot tuple must have at least one untruncated key attribute.
    Ok(tupnatts > 0 && tupnatts <= nkeyatts as i32)
}

// ===========================================================================
// _bt_load comparison inner loop (build-time SortSupport)
// ===========================================================================

/// The `_bt_load` unique-index merge comparison (nbtsort.c:1201-1235): compare
/// two build-sorted index tuples in the index's sort order across all key
/// attributes, returning `<0`/`0`/`>0` (heap-TID tiebreak is applied by the
/// caller).
///
/// C builds a per-column `SortSupport` array with `PrepareSortSupportFromIndexRel`
/// (off `wstate->index`) once at `_bt_load` entry, then inlines `index_getattr` +
/// `ApplySortComparator` per key. That build-time SortSupport substrate
/// (`PrepareSortSupportFromIndexRel`) is not ported in this repo — no producer
/// exists — so this comparison cannot be carried out faithfully and reaches an
/// honest panic, the sanctioned mirror-and-panic for a genuinely-unported
/// callee (never `todo!`/`unimplemented!`).
pub fn bt_load_compare_index_tuples<'mcx>(
    _rel: &Relation<'mcx>,
    _itup_key: &BTScanInsert<'mcx>,
    _itup1: &[u8],
    _itup2: &[u8],
) -> PgResult<i32> {
    panic!(
        "_bt_load comparison: PrepareSortSupportFromIndexRel build-time \
         SortSupport substrate is not yet ported"
    );
}

// ===========================================================================
// _bt_check_third_page
// ===========================================================================

/// `_bt_check_third_page()` — check whether a tuple fits on a btree page at all
/// (restrict any one item to 1/3 of the per-page space). (Public.)
///
/// The `errtableconstraint(heap, relname)` context-attach of the C original is a
/// project-wide error-context gap; the user-visible message/detail/hint are
/// reproduced verbatim.
pub fn bt_check_third_page<'mcx>(
    rel: &Relation<'mcx>,
    heap: &Relation<'mcx>,
    needheaptidspace: bool,
    page: &[u8],
    newtup: &[u8],
) -> PgResult<()> {
    let itemsz = maxalign(IndexTupleSize(&index_tuple_header(newtup)));

    // Double check item size against limit.
    if itemsz <= BTMaxItemSize {
        return Ok(());
    }

    // version 2/3 or internal page: a slightly higher limit applies.
    if !needheaptidspace && itemsz <= BTMaxItemSizeNoHeapTid {
        return Ok(());
    }

    // Internal page insertions cannot fail here.
    let pageref = PageRef::new(page)?;
    let opaque = bt_page_get_opaque(&pageref)?;
    if !p_isleaf(&opaque) {
        return Err(ereport(ERROR)
            .errmsg_internal(format!(
                "cannot insert oversized tuple of size {} on internal page of index \"{}\"",
                itemsz,
                rel_name(rel),
            ))
            .into_error());
    }

    let htid = heap_tid(newtup);
    let (htid_blk, htid_off) = match htid {
        Some(t) => (
            ItemPointerGetBlockNumber(&t),
            ItemPointerGetOffsetNumber(&t),
        ),
        None => (0, 0),
    };
    Err(ereport(ERROR)
        .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .errmsg(format!(
            "index row size {} exceeds btree version {} maximum {} for index \"{}\"",
            itemsz,
            if needheaptidspace {
                BTREE_VERSION
            } else {
                BTREE_NOVAC_VERSION
            },
            if needheaptidspace {
                BTMaxItemSize
            } else {
                BTMaxItemSizeNoHeapTid
            },
            rel_name(rel),
        ))
        .errdetail(format!(
            "Index row references tuple ({},{}) in relation \"{}\".",
            htid_blk,
            htid_off,
            rel_name(heap),
        ))
        .errhint(
            "Values larger than 1/3 of a buffer page cannot be indexed.\nConsider a function index of an MD5 hash of the value, or use full text indexing.",
        )
        .into_error())
}

// ===========================================================================
// _bt_allequalimage
// ===========================================================================

/// `_bt_allequalimage()` — Are all attributes in `rel` "equality is image
/// equality" attributes? Installs `bt_allequalimage_dbg` (this variant carries
/// the `debugmessage` flag). [`bt_allequalimage`] is the no-debug wrapper that
/// installs the `bt_allequalimage` seam.
pub fn bt_allequalimage_dbg<'mcx>(
    rel: &Relation<'mcx>,
    debugmessage: bool,
) -> PgResult<bool> {
    let mut allequalimage = true;

    // INCLUDE indexes can never support deduplication.
    if rel_natts(rel) != rel_nkeyatts(rel) {
        return Ok(false);
    }

    for i in 0..rel_nkeyatts(rel) {
        let opfamily = rd_opfamily(rel, (i + 1) as AttrNumber);
        let opcintype = rd_opcintype(rel, (i + 1) as AttrNumber);
        let collation = rd_indcollation(rel, (i + 1) as AttrNumber);

        let equalimageproc =
            lsyscache::get_opfamily_proc::call(opfamily, opcintype, opcintype, BTEQUALIMAGE_PROC)?;

        // No BTEQUALIMAGE_PROC -> assumed unsafe; else call the proc.
        if !oid_is_valid(equalimageproc)
            || !datum_get_bool(fmgr::function_call1_coll::call(
                equalimageproc,
                collation,
                datum::Datum::from_oid(opcintype),
            )?)
        {
            allequalimage = false;
            break;
        }
    }

    if debugmessage {
        if allequalimage {
            let _ = ::utils_error::elog(
                DEBUG1,
                format!("index \"{}\" can safely use deduplication", rel_name(rel)),
            );
        } else {
            let _ = ::utils_error::elog(
                DEBUG1,
                format!("index \"{}\" cannot use deduplication", rel_name(rel)),
            );
        }
    }

    Ok(allequalimage)
}

/// `_bt_allequalimage(index, debugmessage = false)` — installs the no-debug
/// `bt_allequalimage` seam consumed by `btbuildempty`.
pub fn bt_allequalimage<'mcx>(rel: &Relation<'mcx>) -> PgResult<bool> {
    bt_allequalimage_dbg(rel, false)
}
