//! Port of `src/backend/access/nbtree/nbtsearch.c` (PostgreSQL 18.3) — the
//! B-tree search / scan-positioning core: tree descent (`_bt_search` /
//! `_bt_moveright`), the per-page binary searches (`_bt_binsrch` /
//! `_bt_binsrch_insert` / `_bt_binsrch_posting`), the 3-way insertion-scankey
//! comparison (`_bt_compare`), and the whole forward/backward scan runtime
//! (`_bt_first` / `_bt_next` / `_bt_readpage` / `_bt_steppage` /
//! `_bt_readfirstpage` / `_bt_readnextpage` / `_bt_endpoint` /
//! `_bt_get_endpoint`, plus the `_bt_saveitem*` / `_bt_returnitem` machinery).
//!
//! # Repo model
//!
//! Like the sibling modules (`page.rs`, `utils.rs`, `preprocesskeys.rs`), this
//! file uses real [`Relation<'mcx>`], reads pages through
//! `bufmgr::buffer_get_page` into an owned `PgVec<u8>` decoded via [`PageRef`],
//! and threads scan state on the owned `&mut BTScanOpaqueData<'mcx>`. Tree
//! descent follows downlinks lock-coupling one buffer at a time (parent
//! released before child), exactly as in C.
//!
//! The `bt_first`/`bt_next` seams carry only `(rel, &mut so, dir)` — the C
//! `IndexScanDesc` is not passed. Scan-level inputs the runtime needs are
//! resolved as follows:
//!   * `scan->xs_want_itup` — encoded by `so.currTuples.is_some()` (the AM
//!     driver allocates `currTuples` iff `xs_want_itup`).
//!   * `scan->ignore_killed_tuples` — mirrored onto `so.ignore_killed_tuples`
//!     (additive field; the AM driver sets it before each call).
//!   * `scan->xs_heaptid` / `scan->xs_itup` — written into `so.currPos` (the AM
//!     reads the current position back via `current_heaptid` / `so.currTuples`).
//!   * `scan->parallel_scan` — parallel scans are deferred honestly: every
//!     parallel branch `panic!`s (mirrors the `nbtree.c` crate, which defers
//!     parallel). The serial path is fully ported.
//!   * `scan->xs_snapshot` predicate locking + `pgstat_count_index_scan` +
//!     `scan->instrument` — see the no-op rationale on the helper fns below.
//!
//! # Genuinely-unported callees (honest seam-and-panic)
//!
//!   * `_bt_parallel_*` (nbtree.c): parallel-scan branches only.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::collapsible_else_if)]
#![allow(dead_code)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{AttrNumber, BlockNumber, OffsetNumber, Oid};
use types_error::{PgError, PgResult};
use types_nbtree::{
    BTScanInsert, BTScanInsertData, BTScanOpaqueData, BTScanPosInvalidate, BTScanPosIsPinned,
    BTScanPosIsValid, BTScanPosItem, BTStack, BTStackData, BT_IS_POSTING, BT_OFFSET_MASK,
    BT_PIVOT_HEAP_TID_ATTR, BTORDER_PROC, BTP_DELETED, BTP_HALF_DEAD, BTP_INCOMPLETE_SPLIT,
    BTP_LEAF, INDEX_ALT_TID_MASK, MaxTIDsPerBTreePage, P_FIRSTKEY, P_HIKEY, P_NONE,
};
use types_rel::Relation;
use types_scan::scankey::{
    ScanKeyData, StrategyNumber, BTEqualStrategyNumber, BTGreaterEqualStrategyNumber,
    BTGreaterStrategyNumber, BTLessEqualStrategyNumber, BTLessStrategyNumber, InvalidStrategy,
    SK_BT_DESC, SK_BT_NULLS_FIRST, SK_ISNULL, SK_ROW_END, SK_ROW_HEADER, SK_ROW_MEMBER,
    SK_SEARCHNOTNULL, SK_BT_MINVAL, SK_BT_MAXVAL,
};
use types_scan::sdir::{ScanDirection, ScanDirectionIsBackward, ScanDirectionIsForward};
use types_storage::buf::{BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE};
use types_storage::storage::Buffer;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{IndexTupleData, IndexTupleSize, ItemPointerData};

use backend_storage_page::{
    ItemIdIsDead, ItemPointerCompare, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
    PageGetSpecialPointer, PageRef,
};

use backend_access_common_indextuple_seams as indextuple;
use backend_access_index_indexam_seams as indexam;
use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_fmgr_fmgr_seams::function_call2_coll;

use crate::page::{
    bt_metaversion, bt_relbuf as page_bt_relbuf, _bt_getbuf, _bt_getroot, _bt_gettrueroot,
    _bt_lockbuf, _bt_relandgetbuf, _bt_unlockbuf,
};
use crate::utils::{
    bt_checkkeys, bt_freestack, bt_killitems, bt_scanbehind_checkkeys, bt_set_startikey,
    bt_start_array_keys,
};

// ===========================================================================
// Constants (c.h / access/nbtree.h / access/skey.h).
// ===========================================================================

/// `BT_READ` (`access/nbtree.h`) — `BUFFER_LOCK_SHARE`.
const BT_READ: i32 = BUFFER_LOCK_SHARE;
/// `BT_WRITE` (`access/nbtree.h`) — `BUFFER_LOCK_EXCLUSIVE`.
const BT_WRITE: i32 = BUFFER_LOCK_EXCLUSIVE;

/// `InvalidOffsetNumber` (`storage/off.h`).
const InvalidOffsetNumber: OffsetNumber = 0;
/// `InvalidBlockNumber` (`storage/block.h`).
const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;
/// `InvalidBuffer` (`storage/buf.h`).
const InvalidBuffer: Buffer = 0;
/// `InvalidOid`.
const InvalidOid: Oid = 0;

/// `INDEX_MAX_KEYS` (`pg_config_manual.h`).
const INDEX_MAX_KEYS: usize = 32;

/// `MAXIMUM_ALIGNOF`.
const MAXIMUM_ALIGNOF: usize = 8;
/// `INDEX_SIZE_MASK` (`access/itup.h`) — t_info bits holding the tuple size.
const INDEX_SIZE_MASK: u16 = 0x1FFF;

/// nbtree-private `sk_flags` bits (`access/nbtree.h`) not exported by types-scan.
const SK_BT_REQFWD: i32 = 0x00010000;
const SK_BT_REQBKWD: i32 = 0x00020000;
const SK_BT_NEXT: i32 = 0x00200000;
const SK_BT_PRIOR: i32 = 0x00400000;
const SK_BT_SKIP: i32 = 0x00040000;

/// `SK_BT_INDOPTION_SHIFT` (`access/nbtree.h`).
const SK_BT_INDOPTION_SHIFT: i32 = 24;

// ===========================================================================
// Small inline helpers (c.h / common/int.h / storage/off.h macros).
// ===========================================================================

/// `MAXALIGN(len)` (`c.h`).
#[inline]
const fn maxalign(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `INVERT_COMPARE_RESULT(var)` (`c.h`): `var = (var < 0) ? 1 : -(var)`.
#[inline]
fn invert_compare_result(var: i32) -> i32 {
    if var < 0 {
        1
    } else {
        -var
    }
}

/// `OffsetNumberNext(offsetNumber)` (`storage/off.h`).
#[inline]
const fn OffsetNumberNext(o: OffsetNumber) -> OffsetNumber {
    o + 1
}

/// `OffsetNumberPrev(offsetNumber)` (`storage/off.h`).
#[inline]
const fn OffsetNumberPrev(o: OffsetNumber) -> OffsetNumber {
    o.wrapping_sub(1)
}

/// `OffsetNumberIsValid(offsetNumber)`.
#[inline]
fn offset_number_is_valid(o: OffsetNumber) -> bool {
    o != InvalidOffsetNumber
}

/// `RelationGetRelationName(rel)`.
#[inline]
fn rel_name<'a>(rel: &'a Relation<'a>) -> &'a str {
    rel.name()
}

/// `IndexRelationGetNumberOfKeyAttributes(rel)`.
#[inline]
fn rel_nkeyatts(rel: &Relation) -> i32 {
    rel.indnkeyatts()
}

/// `IndexRelationGetNumberOfAttributes(rel)`.
#[inline]
fn rel_natts(rel: &Relation) -> i32 {
    rel.rd_att.natts
}

/// `DatumGetInt32(d)` — a btree ORDER support proc returns an int32.
#[inline]
fn datum_get_int32(d: types_datum::Datum) -> i32 {
    d.as_i32()
}

/// Convert a canonical `Datum<'mcx>` argument into the bare-word
/// `types_datum::Datum` the fmgr seam dispatches on (mirrors `utils::to_word`).
#[inline]
fn to_word(d: &Datum) -> types_datum::Datum {
    types_datum::Datum::from_usize(d.as_usize())
}

/// The scan/index memory context for `BufferGetPage` reads + `'mcx` scan-state
/// allocations. The `bt_*` seams do not carry an explicit `Mcx`; the index
/// `Relation` does carry `'mcx` `PgVec` metadata, so its allocator is the genuine
/// scan-lifetime context. Page snapshots read through it are owned `PgVec`s that
/// are dropped (freed) at the end of each call, mirroring C reading pages into
/// `CurrentMemoryContext`; scan-state stored into `so->currPos` correctly lives
/// for `'mcx`.
#[inline]
fn rel_mcx<'mcx>(rel: &Relation<'mcx>) -> Mcx<'mcx> {
    *rel.rd_opcintype.allocator()
}

// ===========================================================================
// On-page byte codec (IndexTuple header / BTPageOpaqueData / posting list).
// Same idiomatic style as page.rs / utils.rs.
// ===========================================================================

/// `BTPageOpaqueData` link/flag/level fields decoded from a page special area.
#[derive(Clone, Copy, Debug, Default)]
struct PageOpaque {
    btpo_prev: BlockNumber,
    btpo_next: BlockNumber,
    btpo_level: u32,
    btpo_flags: u16,
}

fn read_ipd(bytes: &[u8]) -> ItemPointerData {
    debug_assert!(bytes.len() >= 6);
    ItemPointerData {
        ip_blkid: types_tuple::heaptuple::BlockIdData {
            bi_hi: u16::from_ne_bytes([bytes[0], bytes[1]]),
            bi_lo: u16::from_ne_bytes([bytes[2], bytes[3]]),
        },
        ip_posid: u16::from_ne_bytes([bytes[4], bytes[5]]),
    }
}

fn index_tuple_header(tuple: &[u8]) -> IndexTupleData {
    debug_assert!(tuple.len() >= 8);
    IndexTupleData {
        t_tid: read_ipd(&tuple[0..6]),
        t_info: u16::from_ne_bytes([tuple[6], tuple[7]]),
    }
}

/// `ItemPointerGetBlockNumberNoCheck(&t_tid)`.
#[inline]
fn ipd_block_number(t: &ItemPointerData) -> BlockNumber {
    ((t.ip_blkid.bi_hi as u32) << 16) | (t.ip_blkid.bi_lo as u32)
}
/// `ItemPointerGetOffsetNumberNoCheck(&t_tid)`.
#[inline]
fn ipd_offset(t: &ItemPointerData) -> u16 {
    t.ip_posid
}

/// `BTPageGetOpaque(page)` — decode the 16-byte nbtree special area.
fn bt_page_get_opaque(page: &PageRef<'_>) -> PgResult<PageOpaque> {
    let special = PageGetSpecialPointer(page)?;
    if special.len() < 16 {
        return Err(PgError::error("BTPageGetOpaque: special area too small"));
    }
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
        btpo_prev: rd_u32(0),
        btpo_next: rd_u32(4),
        btpo_level: rd_u32(8),
        btpo_flags: rd_u16(12),
    })
}

/// `BTPageGetOpaque(page)` from a raw byte slice.
fn opaque_from_bytes(page_bytes: &[u8]) -> PgResult<PageOpaque> {
    let page = PageRef::new(page_bytes)?;
    bt_page_get_opaque(&page)
}

#[inline]
fn P_RIGHTMOST(o: &PageOpaque) -> bool {
    o.btpo_next == P_NONE
}
#[inline]
fn P_LEFTMOST(o: &PageOpaque) -> bool {
    o.btpo_prev == P_NONE
}
#[inline]
fn P_ISLEAF(o: &PageOpaque) -> bool {
    (o.btpo_flags & BTP_LEAF) != 0
}
#[inline]
fn P_ISDELETED(o: &PageOpaque) -> bool {
    (o.btpo_flags & BTP_DELETED) != 0
}
#[inline]
fn P_IGNORE(o: &PageOpaque) -> bool {
    (o.btpo_flags & (BTP_DELETED | BTP_HALF_DEAD)) != 0
}
#[inline]
fn P_INCOMPLETE_SPLIT(o: &PageOpaque) -> bool {
    (o.btpo_flags & BTP_INCOMPLETE_SPLIT) != 0
}
/// `P_FIRSTDATAKEY(opaque)`.
#[inline]
fn P_FIRSTDATAKEY(o: &PageOpaque) -> OffsetNumber {
    if P_RIGHTMOST(o) {
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
    (ipd_offset(&itup.t_tid) & BT_IS_POSTING) == 0
}

/// `BTreeTupleIsPosting(itup)`.
#[inline]
fn bt_tuple_is_posting(itup: &IndexTupleData) -> bool {
    if (itup.t_info & INDEX_ALT_TID_MASK) == 0 {
        return false;
    }
    (ipd_offset(&itup.t_tid) & BT_IS_POSTING) != 0
}

/// `BTreeTupleGetNPosting(posting)`.
#[inline]
fn bt_tuple_get_nposting(posting: &IndexTupleData) -> u16 {
    debug_assert!(bt_tuple_is_posting(posting));
    ipd_offset(&posting.t_tid) & BT_OFFSET_MASK
}

/// `BTreeTupleGetPostingOffset(posting)`.
#[inline]
fn bt_tuple_get_posting_offset(posting: &IndexTupleData) -> u32 {
    debug_assert!(bt_tuple_is_posting(posting));
    ipd_block_number(&posting.t_tid)
}

/// `BTreeTupleGetNAtts(itup, rel)`.
#[inline]
fn bt_tuple_get_natts(itup: &IndexTupleData, indnatts: u16) -> u16 {
    if bt_tuple_is_pivot(itup) {
        ipd_offset(&itup.t_tid) & BT_OFFSET_MASK
    } else {
        indnatts
    }
}

/// `BTreeTupleGetPostingN(posting, n)` — n-th heap TID of a posting list tuple.
fn posting_list_n(tuple: &[u8], n: usize) -> ItemPointerData {
    let hdr = index_tuple_header(tuple);
    let off = bt_tuple_get_posting_offset(&hdr) as usize;
    read_ipd(&tuple[off + n * core::mem::size_of::<ItemPointerData>()..])
}

/// `BTreeTupleGetDownLink(pivot)`.
fn bt_tuple_get_downlink(pivot: &[u8]) -> BlockNumber {
    ipd_block_number(&index_tuple_header(pivot).t_tid)
}

/// `BTreeTupleGetHeapTID(itup)` — first/lowest heap TID, or `None` when a pivot
/// tuple's heap-TID attribute was truncated.
fn heap_tid(tuple: &[u8]) -> Option<ItemPointerData> {
    let itup = index_tuple_header(tuple);
    if bt_tuple_is_pivot(&itup) {
        if (ipd_offset(&itup.t_tid) & BT_PIVOT_HEAP_TID_ATTR) != 0 {
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
// Genuinely-unported callees (no producer in this repo yet). Honest
// seam-and-panic boundaries -- NOT stubs wired to nothing -- mirroring exactly
// the blockers utils.rs / preprocesskeys.rs documented.
// ===========================================================================

/// `index_getattr(itup, attno, tupdesc, &isnull)` (access/itup.h): deform a
/// single attribute out of an index tuple's on-disk byte image, against
/// `RelationGetDescr(rel)`. Backed by the now-ported
/// `backend-access-common-indextuple` `nocache_index_getattr` seam (byte-slice
/// variant); the scan-lifetime `Mcx` (into which a by-ref value is copied) is
/// the index relation's allocator, exactly as `rel_mcx` threads it for page
/// reads. `Err` propagates the detoast / `ereport(ERROR)` surface.
fn index_getattr<'mcx>(
    tuple: &[u8],
    attno: AttrNumber,
    rel: &Relation<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    let mcx = rel_mcx(rel);
    indextuple::nocache_index_getattr::call(mcx, tuple, attno as i32, rel.rd_att.as_ref())
}

// --- Predicate locking / pgstat / instrument: behaviour-preserving no-ops ---
//
// The bt_first/bt_next seams do not carry the IndexScanDesc, so `xs_snapshot`,
// the SSI isolation level, `rel->pgstat_info`, and `scan->instrument` are not
// reachable from this layer (BTScanOpaqueData carries none of them, and this
// crate does not depend on the predicate/pgstat seam crates).
//
// All three are behaviour-preserving when skipped on the common path, exactly
// like page.rs's rd_amcache no-op:
//   * `PredicateLockPage` / `PredicateLockRelation` are internally no-ops unless
//     SSI is active for the read; SSI is not plumbed to this layer, so this
//     matches the (overwhelmingly common) non-serializable case. Serializable
//     correctness for nbtree scans is a genuine SSI-plumbing gap, not a stub.
//   * `pgstat_count_index_scan` is a macro that no-ops when
//     `rel->pgstat_info == NULL`; there is no pgstat_info at this layer.
//   * `scan->instrument` is NULL-guarded in C; there is no instrument here.

/// `PredicateLockRelation(rel, scan->xs_snapshot)` — see module note.
#[inline]
fn predicate_lock_relation<'mcx>(_rel: &Relation<'mcx>) {}

/// `PredicateLockPage(rel, blkno, scan->xs_snapshot)` — see module note.
#[inline]
fn predicate_lock_page<'mcx>(_rel: &Relation<'mcx>, _blkno: BlockNumber) {}

/// `IsolationIsSerializable()` — SSI not plumbed to this layer (see module
/// note); the relation-lock retry in `_bt_first` is therefore never taken here.
#[inline]
fn isolation_is_serializable() -> bool {
    false
}

/// `pgstat_count_index_scan(rel)` — see module note.
#[inline]
fn pgstat_count_index_scan<'mcx>(_rel: &Relation<'mcx>) {}

/// `CHECK_FOR_INTERRUPTS()` — query-cancel / die check. Modelled as a no-op at
/// this layer (the interrupt machinery is process-global; the loops it guards
/// against unbounded right/left walks remain bounded by the index size).
#[inline]
fn check_for_interrupts() {}

// ===========================================================================
// _bt_parallel_* (nbtree.c) — parallel scan is deferred honestly.
// ===========================================================================

fn bt_parallel_done(_so: &mut BTScanOpaqueData) {
    // Serial path: nothing to release. (Parallel branches panic before this.)
}

// ===========================================================================
// _bt_drop_lock_and_maybe_pin
// ===========================================================================

/// `_bt_drop_lock_and_maybe_pin()` — Unlock `so->currPos.buf`; for `so->dropPin`
/// scans drop the pin too (so VACUUM never blocks on a cleanup lock).
fn _bt_drop_lock_and_maybe_pin<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
) -> PgResult<()> {
    if !so.dropPin {
        /* Just drop the lock (not the pin) */
        _bt_unlockbuf(rel, so.currPos.buf);
        return Ok(());
    }

    /*
     * Drop both the lock and the pin.
     *
     * Have to set so->currPos.lsn so that _bt_killitems has a way to detect
     * when concurrent heap TID recycling by VACUUM might have taken place.
     */
    so.currPos.lsn = bufmgr::buffer_get_lsn_atomic::call(so.currPos.buf)?;
    page_bt_relbuf(rel, so.currPos.buf);
    so.currPos.buf = InvalidBuffer;
    Ok(())
}

// ===========================================================================
// _bt_search
// ===========================================================================

/// `_bt_search()` — Search the tree for a particular scankey, or more precisely
/// for the first leaf page it could be on. Returns the parent-page `BTStack`
/// and the located leaf buffer (locked + pinned), mirroring the C out-param
/// `*bufP`.
pub fn bt_search<'mcx>(
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    key: &BTScanInsert<'mcx>,
    access_write: bool,
) -> PgResult<(BTStack, Buffer)> {
    let access = if access_write { BT_WRITE } else { BT_READ };
    {
        let mcx = rel_mcx(rel);
        _bt_search_inner(mcx, rel, heaprel, key, access)
    }
}

fn _bt_search_inner<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    key: &BTScanInsert<'mcx>,
    access: i32,
) -> PgResult<(BTStack, Buffer)> {
    let keyd = key.as_ref().expect("_bt_search: NULL key");

    let mut stack_in: BTStack = None;
    let mut page_access = BT_READ;

    /* Get the root page to start with */
    let mut bufp = _bt_getroot(mcx, rel, heaprel, access)?;

    /* If index is empty and access = BT_READ, no root page is created. */
    if bufp == InvalidBuffer {
        return Ok((None, InvalidBuffer));
    }

    /* Loop iterates once per level descended in the tree */
    loop {
        /*
         * Race -- the page we just grabbed may have split since we read its
         * downlink in its parent page (or the metapage). Move right if needed.
         * In write-mode, allow _bt_moveright to finish incomplete splits.
         */
        bufp = _bt_moveright_inner(
            mcx,
            rel,
            heaprel,
            key,
            bufp,
            access == BT_WRITE,
            &stack_in,
            page_access,
        )?;

        /* if this is a leaf page, we're done */
        let (opaque, level) = {
            let page_bytes = bufmgr::buffer_get_page::call(mcx, bufp)?;
            let o = opaque_from_bytes(&page_bytes)?;
            (o, o.btpo_level)
        };
        if P_ISLEAF(&opaque) {
            break;
        }

        /*
         * Find the appropriate pivot tuple on this page. Its downlink points to
         * the child page that we're about to descend to.
         */
        let offnum = _bt_binsrch_inner(mcx, rel, key, bufp)?;
        let child = {
            let page_bytes = bufmgr::buffer_get_page::call(mcx, bufp)?;
            let page = PageRef::new(&page_bytes)?;
            let iid = PageGetItemId(&page, offnum)?;
            let itup = PageGetItem(&page, &iid)?;
            debug_assert!(bt_tuple_is_pivot(&index_tuple_header(itup)) || !keyd.heapkeyspace);
            bt_tuple_get_downlink(itup)
        };

        /*
         * Save the location of the pivot tuple we chose in a new stack entry.
         */
        let new_stack = Box::new(BTStackData {
            bts_blkno: bufmgr::buffer_get_block_number::call(bufp),
            bts_offset: offnum,
            bts_parent: stack_in.take(),
        });

        /*
         * Page level 1 is lowest non-leaf level prior to leaves. If we're on
         * level 1 and asked to lock leaf in write mode, lock the next page in
         * write mode (it must be a leaf).
         */
        if level == 1 && access == BT_WRITE {
            page_access = BT_WRITE;
        }

        /* drop the read lock on the page, then acquire one on its child */
        bufp = _bt_relandgetbuf(mcx, rel, bufp, child, page_access)?;

        /* okay, all set to move down a level */
        stack_in = Some(new_stack);
    }

    /*
     * If we're asked to lock leaf in write mode, but didn't manage to, relock.
     * This should only happen when the root page is a leaf page.
     */
    if access == BT_WRITE && page_access == BT_READ {
        /* trade in our read lock for a write lock */
        _bt_unlockbuf(rel, bufp);
        _bt_lockbuf(rel, bufp, BT_WRITE);

        /*
         * Race -- the leaf page may have split. Move right if needed.
         */
        bufp = _bt_moveright_inner(mcx, rel, heaprel, key, bufp, true, &stack_in, BT_WRITE)?;
    }

    Ok((stack_in, bufp))
}

// ===========================================================================
// _bt_moveright
// ===========================================================================

/// `_bt_moveright()` — move right in the btree if necessary (handling
/// concurrent splits, and, when `forupdate`, completing incomplete splits).
pub fn bt_moveright<'mcx>(
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    key: &BTScanInsert<'mcx>,
    buf: Buffer,
    forupdate: bool,
    access_write: bool,
) -> PgResult<Buffer> {
    let access = if access_write { BT_WRITE } else { BT_READ };
    {
        let mcx = rel_mcx(rel);
        _bt_moveright_inner(mcx, rel, heaprel, key, buf, forupdate, &None, access)
    }
}

fn _bt_moveright_inner<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    key: &BTScanInsert<'mcx>,
    mut buf: Buffer,
    forupdate: bool,
    stack: &BTStack,
    mut access: i32,
) -> PgResult<Buffer> {
    let keyd = key.as_ref().expect("_bt_moveright: NULL key");

    /*
     * nextkey = false: move right if scan key > page high key.
     * nextkey = true: move right if scan key >= page high key.
     * Also move right if we followed a link to a dead page.
     */
    let cmpval: i32 = if keyd.nextkey { 0 } else { 1 };

    let mut opaque;
    loop {
        opaque = {
            let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
            opaque_from_bytes(&page_bytes)?
        };

        if P_RIGHTMOST(&opaque) {
            break;
        }

        /* Finish any incomplete splits we encounter along the way. */
        if forupdate && P_INCOMPLETE_SPLIT(&opaque) {
            let blkno = bufmgr::buffer_get_block_number::call(buf);

            /* upgrade our lock if necessary */
            if access == BT_READ {
                _bt_unlockbuf(rel, buf);
                _bt_lockbuf(rel, buf, BT_WRITE);
            }

            let still_incomplete = {
                let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
                P_INCOMPLETE_SPLIT(&opaque_from_bytes(&page_bytes)?)
            };
            if still_incomplete {
                crate::insert::_bt_finish_split(
                    mcx,
                    rel,
                    heaprel,
                    buf,
                    crate::insert::clone_stack(stack),
                )?;
            } else {
                page_bt_relbuf(rel, buf);
            }

            /* re-acquire the lock in the right mode, and re-check */
            buf = _bt_getbuf(mcx, rel, blkno, access)?;
            continue;
        }

        let move_right = if P_IGNORE(&opaque) {
            true
        } else {
            _bt_compare_inner(mcx, rel, key, buf, P_HIKEY)? >= cmpval
        };

        if move_right {
            /* step right one page */
            buf = _bt_relandgetbuf(mcx, rel, buf, opaque.btpo_next, access)?;
            continue;
        } else {
            break;
        }
    }

    if P_IGNORE(&opaque) {
        return Err(PgError::error(format!(
            "fell off the end of index \"{}\"",
            rel_name(rel)
        )));
    }

    /* keep `access` "used" for the upgraded-path symmetry with C */
    let _ = &mut access;
    Ok(buf)
}

// ===========================================================================
// _bt_binsrch
// ===========================================================================

/// `_bt_binsrch()` — Binary search for a key on a particular page (the bare,
/// non-insertion variant).
pub fn bt_binsrch<'mcx>(
    rel: &Relation<'mcx>,
    key: &BTScanInsert<'mcx>,
    buf: Buffer,
) -> PgResult<OffsetNumber> {
    {
        let mcx = rel_mcx(rel);
        _bt_binsrch_inner(mcx, rel, key, buf)
    }
}

fn _bt_binsrch_inner<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    key: &BTScanInsert<'mcx>,
    buf: Buffer,
) -> PgResult<OffsetNumber> {
    let keyd = key.as_ref().expect("_bt_binsrch: NULL key");

    let opaque = {
        let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
        opaque_from_bytes(&page_bytes)?
    };

    /* Requesting nextkey semantics while using scantid is nonsensical */
    debug_assert!(!keyd.nextkey || keyd.scantid.is_none());
    /* scantid-set callers must use _bt_binsrch_insert() on leaf pages */
    debug_assert!(!P_ISLEAF(&opaque) || keyd.scantid.is_none());

    let mut low = P_FIRSTDATAKEY(&opaque);
    let mut high = {
        let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
        let page = PageRef::new(&page_bytes)?;
        PageGetMaxOffsetNumber(&page)
    };

    /*
     * If there are no keys on the page, return the first available slot.
     */
    if high < low {
        return Ok(low);
    }

    high += 1; /* establish the loop invariant for high */

    let cmpval: i32 = if keyd.nextkey { 0 } else { 1 };

    while high > low {
        let mid = low + ((high - low) / 2);
        let result = _bt_compare_inner(mcx, rel, key, buf, mid)?;
        if result >= cmpval {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    /* At this point we have high == low. */
    if P_ISLEAF(&opaque) {
        if keyd.backward {
            return Ok(OffsetNumberPrev(low));
        }
        return Ok(low);
    }

    /* On a non-leaf page, return the last key < scan key (resp. <= scan key). */
    debug_assert!(low > P_FIRSTDATAKEY(&opaque));
    Ok(OffsetNumberPrev(low))
}

// ===========================================================================
// _bt_binsrch_insert
// ===========================================================================

/// `_bt_binsrch_insert()` — Cacheable, incremental leaf page binary search,
/// only used during insertion. Caches `low`/`stricthigh` in `insertstate`.
pub fn bt_binsrch_insert<'mcx>(
    rel: &Relation<'mcx>,
    insertstate: &mut types_nbtree::BTInsertStateData<'mcx>,
) -> PgResult<OffsetNumber> {
    {
        let mcx = rel_mcx(rel);
        _bt_binsrch_insert_inner(mcx, rel, insertstate)
    }
}

fn _bt_binsrch_insert_inner<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    insertstate: &mut types_nbtree::BTInsertStateData<'mcx>,
) -> PgResult<OffsetNumber> {
    let buf = insertstate.buf;
    let opaque = {
        let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
        opaque_from_bytes(&page_bytes)?
    };

    {
        let key = insertstate.itup_key.as_ref().expect("_bt_binsrch_insert: NULL key");
        debug_assert!(P_ISLEAF(&opaque));
        debug_assert!(!key.nextkey);
        debug_assert!(insertstate.postingoff == 0);
    }

    let maxoff = {
        let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
        let page = PageRef::new(&page_bytes)?;
        PageGetMaxOffsetNumber(&page)
    };

    let (mut low, mut high) = if !insertstate.bounds_valid {
        /* Start new binary search */
        (P_FIRSTDATAKEY(&opaque), maxoff)
    } else {
        /* Restore result of previous binary search against same page */
        (insertstate.low, insertstate.stricthigh)
    };

    /* If there are no keys on the page, return the first available slot */
    if high < low {
        /* Caller can't reuse bounds */
        insertstate.low = InvalidOffsetNumber;
        insertstate.stricthigh = InvalidOffsetNumber;
        insertstate.bounds_valid = false;
        return Ok(low);
    }

    if !insertstate.bounds_valid {
        high += 1; /* establish the loop invariant for high */
    }
    let mut stricthigh = high; /* high initially strictly higher */

    let cmpval: i32 = 1; /* !nextkey comparison value */

    while high > low {
        let mid = low + ((high - low) / 2);
        let result = _bt_compare_inner(mcx, rel, &insertstate.itup_key, buf, mid)?;

        if result >= cmpval {
            low = mid + 1;
        } else {
            high = mid;
            if result != 0 {
                stricthigh = high;
            }
        }

        /*
         * Posting list overlap with scantid: set postingoff for caller.
         */
        let has_scantid = insertstate
            .itup_key
            .as_ref()
            .map(|k| k.scantid.is_some())
            .unwrap_or(false);
        if result == 0 && has_scantid {
            if insertstate.postingoff != 0 {
                let key = insertstate.itup_key.as_ref().unwrap();
                let scantid = key.scantid.as_ref().unwrap();
                return Err(PgError::error(format!(
                    "table tid from new index tuple ({},{}) cannot find insert offset between offsets {} and {} of block {} in index \"{}\"",
                    ipd_block_number(scantid),
                    ipd_offset(scantid),
                    low,
                    stricthigh,
                    bufmgr::buffer_get_block_number::call(buf),
                    rel_name(rel)
                )));
            }
            insertstate.postingoff = _bt_binsrch_posting(mcx, &insertstate.itup_key, buf, mid)?;
        }
    }

    insertstate.low = low;
    insertstate.stricthigh = stricthigh;
    insertstate.bounds_valid = true;

    Ok(low)
}

// ===========================================================================
// _bt_binsrch_posting
// ===========================================================================

/// `_bt_binsrch_posting()` — posting list binary search; returns the offset into
/// the posting list where caller's scantid belongs (or -1 for an LP_DEAD tuple).
fn _bt_binsrch_posting<'mcx>(
    mcx: Mcx<'mcx>,
    key: &BTScanInsert<'mcx>,
    buf: Buffer,
    offnum: OffsetNumber,
) -> PgResult<i32> {
    let keyd = key.as_ref().expect("_bt_binsrch_posting: NULL key");

    let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
    let page = PageRef::new(&page_bytes)?;
    let itemid = PageGetItemId(&page, offnum)?;
    let itup = PageGetItem(&page, &itemid)?;
    let hdr = index_tuple_header(itup);

    if !bt_tuple_is_posting(&hdr) {
        return Ok(0);
    }

    debug_assert!(keyd.heapkeyspace && keyd.allequalimage);

    /* LP_DEAD posting list tuple: signal caller with -1 */
    if ItemIdIsDead(&itemid) {
        return Ok(-1);
    }

    let scantid = keyd.scantid.as_ref().expect("_bt_binsrch_posting: scantid");

    /* "high" is past end of posting list for loop invariant */
    let mut low: i32 = 0;
    let mut high: i32 = bt_tuple_get_nposting(&hdr) as i32;
    debug_assert!(high >= 2);

    while high > low {
        let mid = low + ((high - low) / 2);
        let res = ItemPointerCompare(scantid, &posting_list_n(itup, mid as usize));
        if res > 0 {
            low = mid + 1;
        } else if res < 0 {
            high = mid;
        } else {
            return Ok(mid);
        }
    }

    /* Exact match not found */
    Ok(low)
}

// ===========================================================================
// _bt_compare
// ===========================================================================

/// `_bt_compare()` — Compare insertion-type scankey to the tuple at `offnum`.
/// Returns `<0` / `0` / `>0`.
pub fn bt_compare<'mcx>(
    rel: &Relation<'mcx>,
    key: &BTScanInsert<'mcx>,
    page: &[u8],
    offnum: OffsetNumber,
) -> PgResult<i32> {
    let pref = PageRef::new(page)?;
    let opaque = bt_page_get_opaque(&pref)?;

    let keyd = key.as_ref().expect("_bt_compare: NULL key");

    debug_assert!(keyd.keysz <= rel_nkeyatts(rel));
    debug_assert!(keyd.heapkeyspace || keyd.scantid.is_none());

    /*
     * Force result ">" if target item is first data item on an internal page.
     */
    if !P_ISLEAF(&opaque) && offnum == P_FIRSTDATAKEY(&opaque) {
        return Ok(1);
    }

    let itemid = PageGetItemId(&pref, offnum)?;
    let itup = PageGetItem(&pref, &itemid)?;
    let hdr = index_tuple_header(itup);
    let ntupatts = bt_tuple_get_natts(&hdr, rel_natts(rel) as u16) as i32;

    let ncmpkey = ntupatts.min(keyd.keysz);
    debug_assert!(keyd.heapkeyspace || ncmpkey == keyd.keysz);
    debug_assert!(!bt_tuple_is_posting(&hdr) || keyd.allequalimage);

    for i in 0..ncmpkey as usize {
        let scankey = &keyd.scankeys[i];

        let (datum, is_null) = index_getattr(itup, scankey.sk_attno, rel)?;

        let result: i32 = if (scankey.sk_flags & SK_ISNULL) != 0 {
            /* key is NULL */
            if is_null {
                0 /* NULL "=" NULL */
            } else if (scankey.sk_flags & SK_BT_NULLS_FIRST) != 0 {
                -1 /* NULL "<" NOT_NULL */
            } else {
                1 /* NULL ">" NOT_NULL */
            }
        } else if is_null {
            /* key is NOT_NULL and item is NULL */
            if (scankey.sk_flags & SK_BT_NULLS_FIRST) != 0 {
                1 /* NOT_NULL ">" NULL */
            } else {
                -1 /* NOT_NULL "<" NULL */
            }
        } else {
            /*
             * sk_func compares the index value (left) to sk_argument (right).
             * Flip the sign unless it's a DESC column.
             */
            let mut r = datum_get_int32(function_call2_coll::call(
                scankey.sk_func.fn_oid,
                scankey.sk_collation,
                to_word(&datum),
                to_word(&scankey.sk_argument),
            )?);
            if (scankey.sk_flags & SK_BT_DESC) == 0 {
                r = invert_compare_result(r);
            }
            r
        };

        /* if the keys are unequal, return the difference */
        if result != 0 {
            return Ok(result);
        }
    }

    /*
     * All non-truncated attributes (other than heap TID) were found equal.
     * Treat truncated attributes as minus infinity.
     */
    if keyd.keysz > ntupatts {
        return Ok(1);
    }

    /*
     * Use the heap TID attribute and scantid to try to break the tie.
     */
    let heap_tid_opt = heap_tid(itup);
    if keyd.scantid.is_none() {
        if !keyd.backward
            && keyd.keysz == ntupatts
            && heap_tid_opt.is_none()
            && keyd.heapkeyspace
        {
            return Ok(1);
        }
        /* All provided scankey arguments found to be equal */
        return Ok(0);
    }

    /*
     * Treat truncated heap TID as minus infinity, since scankey has scantid.
     */
    debug_assert!(keyd.keysz == rel_nkeyatts(rel));
    let heap_tid_val = match heap_tid_opt {
        None => return Ok(1),
        Some(h) => h,
    };

    /*
     * Scankey is equal to a posting list tuple if scantid falls within its
     * posting list range; otherwise compare scantid directly.
     */
    debug_assert!(ntupatts >= rel_nkeyatts(rel));
    let scantid = keyd.scantid.as_ref().unwrap();
    let result = ItemPointerCompare(scantid, &heap_tid_val);
    if result <= 0 || !bt_tuple_is_posting(&hdr) {
        return Ok(result);
    } else {
        let result = ItemPointerCompare(scantid, &max_heap_tid(itup));
        if result > 0 {
            return Ok(1);
        }
    }

    Ok(0)
}

/// `_bt_compare(rel, key, BufferGetPage(buf), offnum)` over a buffer — reads the
/// page bytes and delegates to [`bt_compare`].
fn _bt_compare_inner<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    key: &BTScanInsert<'mcx>,
    buf: Buffer,
    offnum: OffsetNumber,
) -> PgResult<i32> {
    let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
    bt_compare(rel, key, &page_bytes, offnum)
}

// ===========================================================================
// _bt_first
// ===========================================================================

/// `_bt_first()` — Find the first item in a scan. On success exit, data about
/// the matching tuple(s) on the page has been loaded into `so->currPos`.
pub fn bt_first<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    dir: ScanDirection,
) -> PgResult<bool> {
    let mcx = rel_mcx(rel);

    debug_assert!(!BTScanPosIsValid(&so.currPos));

    /*
     * Examine the scan keys and eliminate any redundant keys; also mark the
     * keys that must be matched to continue the scan.
     */
    crate::preprocesskeys::_bt_preprocess_keys(mcx, rel, so, /*parallel_scan=*/ false)?;

    /*
     * Quit now if preprocessing discovered that the keys can never be satisfied.
     */
    if !so.qual_ok {
        debug_assert!(!so.needPrimScan);
        bt_parallel_done(so);
        return Ok(false);
    }

    /*
     * Parallel scan: serial path is fully ported; the parallel seize is
     * deferred honestly.
     */
    // (so->currPos parallel seize branch omitted: parallel_scan is never set at
    // this layer; the AM driver panics before reaching here for parallel scans.)

    /*
     * Initialize the scan's arrays (if any) for the current scan direction
     * (unless they were already set as part of scheduling the primscan now
     * underway).
     */
    if so.numArrayKeys != 0 && !so.needPrimScan {
        bt_start_array_keys(rel, so, dir);
    }

    /*
     * Count an indexscan for stats, now that we know we'll call
     * _bt_search/_bt_endpoint below.
     */
    pgstat_count_index_scan(rel);

    /*----------
     * Examine the scan keys to discover where we need to start the scan.
     *----------
     */
    let mut start_keys: [Option<ScanKeyData<'mcx>>; INDEX_MAX_KEYS] =
        core::array::from_fn(|_| None);
    let mut keysz: usize = 0;
    let mut strat_total: StrategyNumber = BTEqualStrategyNumber;

    if so.numberOfKeys > 0 {
        let mut curattr: AttrNumber = 1;
        let mut bkey: Option<usize> = None; /* index into so.keyData */
        /*
         * Skip-array MINVAL/MAXVAL substitution: when bkey is a skip array's
         * boundary key, C reassigns its local `bkey` pointer to the array's
         * separately-stored low_compare/high_compare ScanKey without ever
         * touching so->keyData. We mirror that with a local override that holds
         * the substituted compare key; so.keyData is left untouched (clobbering
         * it would corrupt the key set _bt_checkkeys reads for the whole scan).
         */
        let mut bkey_override: Option<ScanKeyData<'mcx>> = None;
        let mut implies_nn: Option<usize> = None;
        /* A NOT NULL key materialised from impliesNN, kept alive in start_keys */

        let mut i: usize = 0;
        loop {
            let cur_attno = if i < so.numberOfKeys as usize {
                so.keyData[i].sk_attno
            } else {
                0
            };

            if i >= so.numberOfKeys as usize || cur_attno != curattr {
                /* Done looking for the curattr boundary key */

                /*
                 * Skip-array MINVAL/MAXVAL handling: choose low_compare /
                 * high_compare. Skip arrays carry these on BTArrayKeyInfo; the
                 * boundary key may then become NULL.
                 */
                if let Some(bk) = bkey {
                    if (so.keyData[bk].sk_flags & (SK_BT_MINVAL | SK_BT_MAXVAL)) != 0 {
                        let ikey = bk as i32;
                        let mut arr_idx: Option<usize> = None;
                        for arridx in 0..so.numArrayKeys as usize {
                            if so.arrayKeys[arridx].scan_key == ikey {
                                arr_idx = Some(arridx);
                                break;
                            }
                        }
                        let array = arr_idx.map(|x| so.arrayKeys[x].clone());
                        let skipequalitykey_flags = so.keyData[bk].sk_flags;

                        let new_bkey: Option<ScanKeyData<'mcx>> = if let Some(arr) = &array {
                            if ScanDirectionIsForward(dir) {
                                debug_assert!((skipequalitykey_flags & SK_BT_MAXVAL) == 0);
                                arr.low_compare.as_ref().map(|b| (**b).clone())
                            } else {
                                debug_assert!((skipequalitykey_flags & SK_BT_MINVAL) == 0);
                                arr.high_compare.as_ref().map(|b| (**b).clone())
                            }
                        } else {
                            None
                        };

                        if let Some(arr) = &array {
                            if !arr.null_elem {
                                implies_nn = Some(bk);
                            }
                        }

                        /*
                         * Reassign the (local) boundary key to the (possibly
                         * NULL) compare key, mirroring C's `bkey = ...` pointer
                         * reassignment. so.keyData is NOT modified.
                         */
                        if let Some(nb) = new_bkey {
                            bkey_override = Some(nb);
                            /* bkey stays Some(bk) so bkey.is_some() holds below */
                        } else {
                            bkey = None;
                            bkey_override = None;
                        }
                    }
                }

                /*
                 * If no usable boundary key, see if we can deduce a NOT NULL key.
                 */
                let mut deduced_notnull: Option<ScanKeyData<'mcx>> = None;
                if bkey.is_none() {
                    if let Some(inn) = implies_nn {
                        let inn_flags = so.keyData[inn].sk_flags;
                        let use_it = if (inn_flags & SK_BT_NULLS_FIRST) != 0 {
                            ScanDirectionIsForward(dir)
                        } else {
                            ScanDirectionIsBackward(dir)
                        };
                        if use_it {
                            let mut nn = ScanKeyData::empty();
                            nn.sk_flags = SK_SEARCHNOTNULL
                                | SK_ISNULL
                                | (inn_flags & (SK_BT_DESC | SK_BT_NULLS_FIRST));
                            nn.sk_attno = curattr;
                            nn.sk_strategy = if (inn_flags & SK_BT_NULLS_FIRST) != 0 {
                                BTGreaterStrategyNumber
                            } else {
                                BTLessStrategyNumber
                            };
                            nn.sk_subtype = InvalidOid;
                            nn.sk_collation = InvalidOid;
                            nn.sk_func = FmgrInfo::empty();
                            nn.sk_argument = Datum::null();
                            deduced_notnull = Some(nn);
                        }
                    }
                }

                /*
                 * If preprocessing didn't leave a usable boundary key, quit;
                 * else save the boundary key in start_keys[].
                 */
                let chosen: Option<ScanKeyData<'mcx>> = if let Some(nn) = deduced_notnull {
                    Some(nn)
                } else if let Some(ov) = bkey_override.take() {
                    /* skip-array compare key substituted in for so.keyData[bk] */
                    Some(ov)
                } else if let Some(bk) = bkey {
                    Some(so.keyData[bk].clone())
                } else {
                    None
                };

                let chosen = match chosen {
                    None => break,
                    Some(c) => c,
                };

                strat_total = chosen.sk_strategy;
                let chosen_flags = chosen.sk_flags;
                start_keys[keysz] = Some(chosen);
                keysz += 1;

                /*
                 * Only continue adding boundary keys when this one is = or >=
                 * (forwards) / = or <= (backwards).
                 */
                if strat_total == BTGreaterStrategyNumber || strat_total == BTLessStrategyNumber {
                    break;
                }

                /*
                 * Skip-array = key whose element is NEXT/PRIOR: make strat_total
                 * > or < and stop.
                 */
                if (chosen_flags & (SK_BT_NEXT | SK_BT_PRIOR)) != 0 {
                    debug_assert!((chosen_flags & SK_BT_SKIP) != 0);
                    debug_assert!(strat_total == BTEqualStrategyNumber);
                    if ScanDirectionIsForward(dir) {
                        debug_assert!((chosen_flags & SK_BT_PRIOR) == 0);
                        strat_total = BTGreaterStrategyNumber;
                    } else {
                        debug_assert!((chosen_flags & SK_BT_NEXT) == 0);
                        strat_total = BTLessStrategyNumber;
                    }
                    break;
                }

                /*
                 * Done if that was the last preprocessing key, or we've examined
                 * all required keys.
                 */
                if i >= so.numberOfKeys as usize
                    || (so.keyData[i].sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) == 0
                {
                    break;
                }

                /* Reset for next attr. */
                debug_assert!(so.keyData[i].sk_attno == curattr + 1);
                curattr = so.keyData[i].sk_attno;
                bkey = None;
                bkey_override = None;
                implies_nn = None;
            }

            /*
             * If we've located the starting boundary key for curattr, ignore
             * curattr's other required key.
             */
            if bkey.is_some() {
                i += 1;
                continue;
            }

            /*
             * Is this key the starting boundary key for curattr? If not, does it
             * imply a NOT NULL constraint?
             */
            match so.keyData[i].sk_strategy {
                s if s == BTLessStrategyNumber || s == BTLessEqualStrategyNumber => {
                    if ScanDirectionIsBackward(dir) {
                        bkey = Some(i);
                    } else if implies_nn.is_none() {
                        implies_nn = Some(i);
                    }
                }
                s if s == BTEqualStrategyNumber => {
                    bkey = Some(i);
                }
                s if s == BTGreaterEqualStrategyNumber || s == BTGreaterStrategyNumber => {
                    if ScanDirectionIsForward(dir) {
                        bkey = Some(i);
                    } else if implies_nn.is_none() {
                        implies_nn = Some(i);
                    }
                }
                _ => {}
            }

            i += 1;
        }
    }

    /*
     * If no usable boundary keys, start from one end of the tree.
     */
    if keysz == 0 {
        return _bt_endpoint(mcx, rel, so, dir);
    }

    /*
     * Build the insertion scankey from start_keys[].
     */
    debug_assert!(keysz <= INDEX_MAX_KEYS);
    let mut inskeys: Vec<ScanKeyData<'mcx>> = Vec::with_capacity(keysz);
    for _ in 0..keysz {
        inskeys.push(ScanKeyData::empty());
    }

    let mut i: usize = 0;
    while i < keysz {
        let bkey = start_keys[i].as_ref().unwrap().clone();
        debug_assert!(bkey.sk_attno == (i + 1) as AttrNumber);

        if (bkey.sk_flags & SK_ROW_HEADER) != 0 {
            /*
             * Row comparison header: the member scankeys are already in
             * insertion format. Look to the first / later row members.
             */
            let subkeys = bkey.sk_subkeys.as_ref().expect("row header sk_subkeys");
            debug_assert!(!subkeys.is_empty());
            let subkey0 = &subkeys[0];
            debug_assert!((subkey0.sk_flags & SK_ROW_MEMBER) != 0);
            debug_assert!((subkey0.sk_flags & SK_ISNULL) == 0);

            inskeys[i] = subkey0.clone();

            let mut loosen_strat = false;
            let mut tighten_strat = false;
            let mut sidx = 1usize;
            debug_assert!((subkey0.sk_flags & SK_ROW_END) == 0);
            loop {
                let subkey = &subkeys[sidx];
                debug_assert!((subkey.sk_flags & SK_ROW_MEMBER) != 0);

                if (subkey.sk_flags & SK_ISNULL) != 0 {
                    /* NULL member key: can only use earlier keys. */
                    tighten_strat = true;
                    break;
                }
                if (subkey.sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) == 0 {
                    /* nonrequired member key: can only use earlier keys. */
                    loosen_strat = true;
                    break;
                }

                debug_assert!(keysz < INDEX_MAX_KEYS);
                inskeys.push(subkey.clone());
                keysz += 1;

                if (subkey.sk_flags & SK_ROW_END) != 0 {
                    break;
                }
                sidx += 1;
            }
            debug_assert!(!(loosen_strat && tighten_strat));
            if loosen_strat {
                strat_total = match strat_total {
                    s if s == BTLessStrategyNumber => BTLessEqualStrategyNumber,
                    s if s == BTGreaterStrategyNumber => BTGreaterEqualStrategyNumber,
                    s => s,
                };
            }
            if tighten_strat {
                strat_total = match strat_total {
                    s if s == BTLessEqualStrategyNumber => BTLessStrategyNumber,
                    s if s == BTGreaterEqualStrategyNumber => BTGreaterStrategyNumber,
                    s => s,
                };
            }
            /* Row compare header key is always last. */
            break;
        }

        /*
         * Ordinary comparison key: transform the search-style scan key to an
         * insertion scan key by replacing sk_func with the ORDER proc.
         */
        let opcintype = rel.rd_opcintype[i];
        if bkey.sk_subtype == opcintype || bkey.sk_subtype == InvalidOid {
            /* cached (default) support proc */
            let procinfo =
                indexam::index_getprocinfo::call(rel, bkey.sk_attno, BTORDER_PROC as u16)?;
            let mut e = ScanKeyData::empty();
            e.sk_flags = bkey.sk_flags;
            e.sk_attno = bkey.sk_attno;
            e.sk_strategy = InvalidStrategy;
            e.sk_subtype = bkey.sk_subtype;
            e.sk_collation = bkey.sk_collation;
            e.sk_func = procinfo;
            e.sk_argument = bkey.sk_argument.clone();
            inskeys[i] = e;
        } else {
            /* cross-type: look up the ORDER proc in the catalogs */
            let cmp_proc = lsyscache::get_opfamily_proc::call(
                rel.rd_opfamily[i],
                opcintype,
                bkey.sk_subtype,
                BTORDER_PROC,
            )?;
            if cmp_proc == InvalidOid {
                return Err(PgError::error(format!(
                    "missing support function {}({},{}) for attribute {} of index \"{}\"",
                    BTORDER_PROC, opcintype, bkey.sk_subtype, bkey.sk_attno, rel_name(rel)
                )));
            }
            let mut e = ScanKeyData::empty();
            e.sk_flags = bkey.sk_flags;
            e.sk_attno = bkey.sk_attno;
            e.sk_strategy = InvalidStrategy;
            e.sk_subtype = bkey.sk_subtype;
            e.sk_collation = bkey.sk_collation;
            e.sk_func = FmgrInfo::empty();
            e.sk_func.fn_oid = cmp_proc;
            e.sk_argument = bkey.sk_argument.clone();
            inskeys[i] = e;
        }

        i += 1;
    }

    /*----------
     * Examine the selected initial-positioning strategy.
     *----------
     */
    let (heapkeyspace, allequalimage) = bt_metaversion(rel)?;
    let mut inskey = BTScanInsertData {
        heapkeyspace,
        allequalimage,
        anynullkeys: false, /* unused */
        nextkey: false,
        backward: false,
        scantid: None,
        keysz: keysz as i32,
        scankeys: inskeys,
    };

    match strat_total {
        s if s == BTLessStrategyNumber => {
            inskey.nextkey = false;
            inskey.backward = true;
        }
        s if s == BTLessEqualStrategyNumber => {
            inskey.nextkey = true;
            inskey.backward = true;
        }
        s if s == BTEqualStrategyNumber => {
            if ScanDirectionIsBackward(dir) {
                /* same as <= */
                inskey.nextkey = true;
                inskey.backward = true;
            } else {
                /* same as >= */
                inskey.nextkey = false;
                inskey.backward = false;
            }
        }
        s if s == BTGreaterEqualStrategyNumber => {
            inskey.nextkey = false;
            inskey.backward = false;
        }
        s if s == BTGreaterStrategyNumber => {
            inskey.nextkey = true;
            inskey.backward = false;
        }
        other => {
            return Err(PgError::error(format!(
                "unrecognized strat_total: {}",
                other
            )));
        }
    }

    /*
     * Use the manufactured insertion scan key to descend the tree and position
     * on the target leaf page.
     */
    debug_assert!(ScanDirectionIsBackward(dir) == inskey.backward);
    let inskey_boxed: BTScanInsert<'mcx> = Some(Box::new(inskey));

    let (stack, found_buf) = _bt_search_inner(mcx, rel, rel, &inskey_boxed, BT_READ)?;
    so.currPos.buf = found_buf;
    /* don't need to keep the stack around... */
    bt_freestack(stack);

    if !BTScanPosIsBuf(so.currPos.buf) {
        debug_assert!(!so.needPrimScan);

        /*
         * Only get here if the index is completely empty. Take a relation-level
         * predicate lock under serializable isolation and retry. SSI is not
         * plumbed to this layer (isolation_is_serializable() is always false
         * here), so this branch is never taken; left in place for fidelity.
         */
        if isolation_is_serializable() {
            predicate_lock_relation(rel);
            let (stack2, found_buf2) = _bt_search_inner(mcx, rel, rel, &inskey_boxed, BT_READ)?;
            so.currPos.buf = found_buf2;
            bt_freestack(stack2);
        }

        if !BTScanPosIsBuf(so.currPos.buf) {
            bt_parallel_done(so);
            return Ok(false);
        }
    }

    /* position to the precise item on the page */
    let offnum = _bt_binsrch_inner(mcx, rel, &inskey_boxed, so.currPos.buf)?;

    /*
     * Now load data from the first page of the scan.
     */
    if !_bt_readfirstpage(mcx, rel, so, offnum, dir)? {
        return Ok(false);
    }

    _bt_returnitem(so);
    Ok(true)
}

/// `BufferIsValid(buf)`.
#[inline]
fn BTScanPosIsBuf(buf: Buffer) -> bool {
    buf != InvalidBuffer
}

// ===========================================================================
// _bt_next
// ===========================================================================

/// `_bt_next()` — Get the next item in a scan.
pub fn bt_next<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    dir: ScanDirection,
) -> PgResult<bool> {
    debug_assert!(BTScanPosIsValid(&so.currPos));

    /*
     * Advance to next tuple on current page; or if there's no more, try to step
     * to the next page with data.
     */
    if ScanDirectionIsForward(dir) {
        so.currPos.itemIndex += 1;
        if so.currPos.itemIndex > so.currPos.lastItem {
            if !_bt_steppage(rel, so, dir)? {
                return Ok(false);
            }
        }
    } else {
        so.currPos.itemIndex -= 1;
        if so.currPos.itemIndex < so.currPos.firstItem {
            if !_bt_steppage(rel, so, dir)? {
                return Ok(false);
            }
        }
    }

    _bt_returnitem(so);
    Ok(true)
}

// ===========================================================================
// _bt_readpage
// ===========================================================================

/// `_bt_readpage()` — Load data from the current index page into `so->currPos`.
/// Caller must have pinned and read-locked `so->currPos.buf`. Returns true if
/// any matching items were found.
fn _bt_readpage<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    dir: ScanDirection,
    mut offnum: OffsetNumber,
    firstpage: bool,
) -> PgResult<bool> {
    /* Snapshot the page bytes once (read under the held lock). */
    let page_vec: PgVec<'mcx, u8> = bufmgr::buffer_get_page::call(mcx, so.currPos.buf)?;
    let opaque = opaque_from_bytes(&page_vec)?;

    so.currPos.currPage = bufmgr::buffer_get_block_number::call(so.currPos.buf);
    so.currPos.prevPage = opaque.btpo_prev;
    so.currPos.nextPage = opaque.btpo_next;
    /* delay setting so->currPos.lsn until _bt_drop_lock_and_maybe_pin */
    so.currPos.dir = dir;
    so.currPos.nextTupleOffset = 0;

    debug_assert!(if ScanDirectionIsForward(dir) {
        so.currPos.moreRight
    } else {
        so.currPos.moreLeft
    });
    debug_assert!(!P_IGNORE(&opaque));
    debug_assert!(BTScanPosIsPinned(&so.currPos));
    debug_assert!(!so.needPrimScan);

    predicate_lock_page(rel, so.currPos.currPage);

    /* initialize local variables */
    let indnatts = rel_natts(rel);
    let array_keys = so.numArrayKeys != 0;
    let page = PageRef::new(&page_vec)?;
    let minoff = P_FIRSTDATAKEY(&opaque);
    let maxoff = PageGetMaxOffsetNumber(&page);

    /* initialize page-level state that we'll pass to _bt_checkkeys */
    let mut pstate = types_nbtree::BTReadPageState::new(mcx);
    pstate.minoff = minoff;
    pstate.maxoff = maxoff;
    pstate.finaltup = None;
    pstate.page = clone_pgvec(mcx, &page_vec);
    pstate.firstpage = firstpage;
    pstate.forcenonrequired = false;
    pstate.startikey = 0;
    pstate.offnum = InvalidOffsetNumber;
    pstate.skip = InvalidOffsetNumber;
    pstate.continuescan = true; /* default assumption */
    pstate.rechecks = 0;
    pstate.targetdistance = 0;
    pstate.nskipadvances = 0;

    if ScanDirectionIsForward(dir) {
        /* SK_SEARCHARRAY forward scans must provide high key up front */
        if array_keys {
            if !P_RIGHTMOST(&opaque) {
                let iid = PageGetItemId(&page, P_HIKEY)?;
                let hikey = PageGetItem(&page, &iid)?;
                pstate.finaltup = Some(to_pgvec(mcx, hikey));

                if so.scanBehind
                    && !bt_scanbehind_checkkeys(rel, so, dir, hikey)?
                {
                    /* Schedule another primitive index scan after all */
                    so.currPos.moreRight = false;
                    so.needPrimScan = true;
                    /* parallel primscan schedule omitted (serial path) */
                    return Ok(false);
                }
            }
            so.scanBehind = false;
            so.oppositeDirCheck = false; /* reset */
        }

        /* pstate.startikey optimization once the primscan read >= 1 page */
        if !pstate.firstpage && minoff < maxoff {
            bt_set_startikey(rel, so, &mut pstate)?;
        }

        /* load items[] in ascending order */
        let mut item_index: i32 = 0;
        ensure_items_capacity(mcx, so);

        offnum = offnum.max(minoff);

        while offnum <= maxoff {
            let iid = PageGetItemId(&page, offnum)?;

            /* killed tuple treated as not passing the qual */
            if so.ignore_killed_tuples && ItemIdIsDead(&iid) {
                offnum = OffsetNumberNext(offnum);
                continue;
            }

            let itup = PageGetItem(&page, &iid)?;
            let hdr = index_tuple_header(itup);
            debug_assert!(!bt_tuple_is_pivot(&hdr));

            pstate.offnum = offnum;
            let passes_quals = bt_checkkeys(rel, so, &mut pstate, array_keys, itup, indnatts)?;

            /* skip ahead to a later tuple (array keys only) */
            if array_keys && offset_number_is_valid(pstate.skip) {
                debug_assert!(!passes_quals && pstate.continuescan);
                debug_assert!(offnum < pstate.skip);
                debug_assert!(!pstate.forcenonrequired);
                offnum = pstate.skip;
                pstate.skip = InvalidOffsetNumber;
                continue;
            }

            if passes_quals {
                if !bt_tuple_is_posting(&hdr) {
                    _bt_saveitem(so, item_index, offnum, itup);
                    item_index += 1;
                } else {
                    let tuple_offset =
                        _bt_setuppostingitems(so, item_index, offnum, &posting_list_n(itup, 0), itup);
                    item_index += 1;
                    for k in 1..bt_tuple_get_nposting(&hdr) as usize {
                        _bt_savepostingitem(so, item_index, offnum, &posting_list_n(itup, k), tuple_offset);
                        item_index += 1;
                    }
                }
            }
            /* When !continuescan, there can't be any more matches, so stop */
            if !pstate.continuescan {
                break;
            }
            offnum = OffsetNumberNext(offnum);
        }

        /*
         * Don't visit the page to the right when the high key indicates no more
         * matches will be found there.
         */
        if pstate.continuescan && !so.scanBehind && !P_RIGHTMOST(&opaque) {
            let iid = PageGetItemId(&page, P_HIKEY)?;
            let itup = PageGetItem(&page, &iid)?;

            /* Reset arrays, per _bt_set_startikey contract */
            if pstate.forcenonrequired {
                bt_start_array_keys(rel, so, dir);
            }
            pstate.forcenonrequired = false;
            pstate.startikey = 0; /* _bt_set_startikey ignores P_HIKEY */

            let truncatt = bt_tuple_get_natts(&index_tuple_header(itup), indnatts as u16) as i32;
            bt_checkkeys(rel, so, &mut pstate, array_keys, itup, truncatt)?;
        }

        if !pstate.continuescan {
            so.currPos.moreRight = false;
        }

        debug_assert!(item_index <= MaxTIDsPerBTreePage as i32);
        so.currPos.firstItem = 0;
        so.currPos.lastItem = item_index - 1;
        so.currPos.itemIndex = 0;
    } else {
        /* SK_SEARCHARRAY backward scans must provide final tuple up front */
        if array_keys {
            if minoff <= maxoff && !P_LEFTMOST(&opaque) {
                let iid = PageGetItemId(&page, minoff)?;
                let ftup = PageGetItem(&page, &iid)?;
                pstate.finaltup = Some(to_pgvec(mcx, ftup));

                if so.scanBehind && !bt_scanbehind_checkkeys(rel, so, dir, ftup)? {
                    so.currPos.moreLeft = false;
                    so.needPrimScan = true;
                    return Ok(false);
                }
            }
            so.scanBehind = false;
            so.oppositeDirCheck = false; /* reset */
        }

        if !pstate.firstpage && minoff < maxoff {
            bt_set_startikey(rel, so, &mut pstate)?;
        }

        /* load items[] in descending order */
        let mut item_index: i32 = MaxTIDsPerBTreePage as i32;
        ensure_items_capacity(mcx, so);

        offnum = offnum.min(maxoff);

        while offnum >= minoff {
            let iid = PageGetItemId(&page, offnum)?;

            let tuple_alive: bool;
            if so.ignore_killed_tuples && ItemIdIsDead(&iid) {
                if offnum > minoff {
                    offnum = OffsetNumberPrev(offnum);
                    continue;
                }
                tuple_alive = false;
            } else {
                tuple_alive = true;
            }

            let itup = PageGetItem(&page, &iid)?;
            let hdr = index_tuple_header(itup);
            debug_assert!(!bt_tuple_is_pivot(&hdr));

            pstate.offnum = offnum;
            if array_keys && offnum == minoff && pstate.forcenonrequired {
                /* Reset arrays, per _bt_set_startikey contract */
                pstate.forcenonrequired = false;
                pstate.startikey = 0;
                bt_start_array_keys(rel, so, dir);
            }
            let passes_quals = bt_checkkeys(rel, so, &mut pstate, array_keys, itup, indnatts)?;

            if array_keys && so.scanBehind {
                /* Done scanning this page, but not done with current primscan. */
                debug_assert!(!passes_quals && pstate.continuescan);
                debug_assert!(!pstate.forcenonrequired);
                break;
            }

            if array_keys && offset_number_is_valid(pstate.skip) {
                debug_assert!(!passes_quals && pstate.continuescan);
                debug_assert!(offnum > pstate.skip);
                debug_assert!(!pstate.forcenonrequired);
                offnum = pstate.skip;
                pstate.skip = InvalidOffsetNumber;
                continue;
            }

            if passes_quals && tuple_alive {
                if !bt_tuple_is_posting(&hdr) {
                    item_index -= 1;
                    _bt_saveitem(so, item_index, offnum, itup);
                } else {
                    /*
                     * Save/return posting list TIDs in ascending heap TID order
                     * for backwards scans (so _bt_killitems can assume order).
                     */
                    item_index -= 1;
                    let tuple_offset =
                        _bt_setuppostingitems(so, item_index, offnum, &posting_list_n(itup, 0), itup);
                    for k in 1..bt_tuple_get_nposting(&hdr) as usize {
                        item_index -= 1;
                        _bt_savepostingitem(so, item_index, offnum, &posting_list_n(itup, k), tuple_offset);
                    }
                }
            }
            if !pstate.continuescan {
                break;
            }
            offnum = OffsetNumberPrev(offnum);
        }

        /* Don't visit the page to the left when no more matches found there */
        if !pstate.continuescan {
            so.currPos.moreLeft = false;
        }

        debug_assert!(item_index >= 0);
        so.currPos.firstItem = item_index;
        so.currPos.lastItem = MaxTIDsPerBTreePage as i32 - 1;
        so.currPos.itemIndex = MaxTIDsPerBTreePage as i32 - 1;
    }

    debug_assert!(!pstate.forcenonrequired);

    Ok(so.currPos.firstItem <= so.currPos.lastItem)
}

/// Ensure `so.currPos.items` has `MaxTIDsPerBTreePage` slots (the C flexible
/// array is fixed-size; here it's a `PgVec`). Idempotent.
fn ensure_items_capacity<'mcx>(mcx: Mcx<'mcx>, so: &mut BTScanOpaqueData<'mcx>) {
    if so.currPos.items.len() < MaxTIDsPerBTreePage {
        let mut v: PgVec<'mcx, BTScanPosItem> =
            vec_with_capacity_in(mcx, MaxTIDsPerBTreePage).expect("items alloc");
        for _ in 0..MaxTIDsPerBTreePage {
            v.push(BTScanPosItem::default());
        }
        so.currPos.items = v;
    }
}

/// Copy a byte slice into an owned `PgVec<u8>`.
fn to_pgvec<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgVec<'mcx, u8> {
    let mut v: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, bytes.len()).expect("pgvec alloc");
    v.extend_from_slice(bytes);
    v
}

/// Clone a `PgVec<u8>` into a fresh `PgVec<u8>` over `mcx`.
fn clone_pgvec<'mcx>(mcx: Mcx<'mcx>, src: &PgVec<'mcx, u8>) -> PgVec<'mcx, u8> {
    to_pgvec(mcx, src.as_slice())
}

// ===========================================================================
// _bt_saveitem / _bt_setuppostingitems / _bt_savepostingitem
// ===========================================================================

/// `_bt_saveitem()` — Save an index item into `so->currPos.items[itemIndex]`.
fn _bt_saveitem<'mcx>(
    so: &mut BTScanOpaqueData<'mcx>,
    item_index: i32,
    offnum: OffsetNumber,
    itup: &[u8],
) {
    let hdr = index_tuple_header(itup);
    debug_assert!(!bt_tuple_is_pivot(&hdr) && !bt_tuple_is_posting(&hdr));

    let heap_tid = hdr.t_tid;
    let next_off = so.currPos.nextTupleOffset;

    let tuple_offset = if so.currTuples.is_some() {
        let itupsz = IndexTupleSize(&hdr);
        let curr = so.currTuples.as_mut().unwrap();
        let dst = next_off as usize;
        store_tuple(curr, dst, &itup[..itupsz]);
        so.currPos.nextTupleOffset = next_off + maxalign(itupsz) as i32;
        next_off
    } else {
        0
    };

    let curr_item = &mut so.currPos.items[item_index as usize];
    curr_item.heapTid = heap_tid;
    curr_item.indexOffset = offnum;
    if so.currTuples.is_some() {
        curr_item.tupleOffset = tuple_offset as u16;
    }
}

/// `_bt_setuppostingitems()` — Set up state to save TIDs from one posting list
/// tuple. Returns the tuple storage offset of the saved base tuple (or 0).
fn _bt_setuppostingitems<'mcx>(
    so: &mut BTScanOpaqueData<'mcx>,
    item_index: i32,
    offnum: OffsetNumber,
    heap_tid: &ItemPointerData,
    itup: &[u8],
) -> i32 {
    let hdr = index_tuple_header(itup);
    debug_assert!(bt_tuple_is_posting(&hdr));

    let next_off = so.currPos.nextTupleOffset;
    let mut ret_offset = 0i32;

    if so.currTuples.is_some() {
        /* Save base IndexTuple (truncate posting list) */
        let itupsz = maxalign(bt_tuple_get_posting_offset(&hdr) as usize);
        let dst = next_off as usize;
        let curr = so.currTuples.as_mut().unwrap();
        store_tuple(curr, dst, &itup[..itupsz]);
        /* Defensively reduce work area index tuple header size (t_info @ +6) */
        let mut new_info = u16::from_ne_bytes([curr[dst + 6], curr[dst + 7]]);
        new_info &= !INDEX_SIZE_MASK;
        new_info |= itupsz as u16;
        let nb = new_info.to_ne_bytes();
        curr[dst + 6] = nb[0];
        curr[dst + 7] = nb[1];
        so.currPos.nextTupleOffset = next_off + itupsz as i32;
        ret_offset = next_off;
    }

    let curr_item = &mut so.currPos.items[item_index as usize];
    curr_item.heapTid = *heap_tid;
    curr_item.indexOffset = offnum;
    if so.currTuples.is_some() {
        curr_item.tupleOffset = ret_offset as u16;
    }

    ret_offset
}

/// `_bt_savepostingitem()` — Save an item for the current posting tuple.
fn _bt_savepostingitem<'mcx>(
    so: &mut BTScanOpaqueData<'mcx>,
    item_index: i32,
    offnum: OffsetNumber,
    heap_tid: &ItemPointerData,
    tuple_offset: i32,
) {
    let curr_item = &mut so.currPos.items[item_index as usize];
    curr_item.heapTid = *heap_tid;
    curr_item.indexOffset = offnum;
    /* index-only scans return the same base IndexTuple for every posting TID */
    if so.currTuples.is_some() {
        curr_item.tupleOffset = tuple_offset as u16;
    }
}

/// Write `src` into `dst[at..at+src.len()]`, growing `dst` with zeros as needed
/// (the C work area is pre-sized to `BLCKSZ`; the `PgVec` grows on demand).
fn store_tuple(dst: &mut PgVec<'_, u8>, at: usize, src: &[u8]) {
    let need = at + src.len();
    while dst.len() < need {
        dst.push(0);
    }
    dst[at..need].copy_from_slice(src);
}

// ===========================================================================
// _bt_returnitem
// ===========================================================================

/// `_bt_returnitem()` — Return the current item to the scan.
///
/// C sets `scan->xs_heaptid` and (for index-only scans) `scan->xs_itup`. At this
/// seam there is no `IndexScanDesc`: the AM driver reads the current position
/// back from `so->currPos` (via the `current_heaptid` seam) and reads
/// `so->currTuples` for the index-only tuple. So this only validates state.
fn _bt_returnitem<'mcx>(so: &BTScanOpaqueData<'mcx>) {
    debug_assert!(BTScanPosIsValid(&so.currPos));
    debug_assert!(so.currPos.itemIndex >= so.currPos.firstItem);
    debug_assert!(so.currPos.itemIndex <= so.currPos.lastItem);
    /* xs_heaptid / xs_itup are read back from so->currPos by the AM driver. */
}

// ===========================================================================
// _bt_steppage
// ===========================================================================

/// `_bt_steppage()` — Step to the next page containing valid data for the scan.
fn _bt_steppage<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    dir: ScanDirection,
) -> PgResult<bool> {
    debug_assert!(BTScanPosIsValid(&so.currPos));

    /* Before leaving current page, deal with any killed items */
    if so.numKilled > 0 {
        bt_killitems(rel, so);
    }

    /*
     * Copy page data into markPos if there's a mark position that needs it.
     */
    if so.markItemIndex >= 0 {
        /* bump pin on current buffer for assignment to mark buffer */
        if BTScanPosIsPinned(&so.currPos) {
            bufmgr::incr_buffer_ref_count::call(so.currPos.buf);
        }
        /* memcpy(&so->markPos, &so->currPos, ...up to lastItem) */
        so.markPos = so.currPos.clone();
        if so.markTuples.is_some() && so.currTuples.is_some() {
            let n = so.currPos.nextTupleOffset as usize;
            let src: Vec<u8> = so.currTuples.as_ref().unwrap().as_slice()[..n].to_vec();
            let mt = so.markTuples.as_mut().unwrap();
            store_tuple(mt, 0, &src);
        }
        so.markPos.itemIndex = so.markItemIndex;
        so.markItemIndex = -1;

        /*
         * If about to start the next primitive index scan, moreLeft/moreRight
         * only indicate the end of the current primscan, not the top-level scan.
         */
        if so.needPrimScan {
            if ScanDirectionIsForward(so.currPos.dir) {
                so.markPos.moreRight = true;
            } else {
                so.markPos.moreLeft = true;
            }
        }
        /* mark/restore not supported by parallel scans (serial path here) */
    }

    /* BTScanPosUnpinIfPinned(so->currPos) */
    if BTScanPosIsPinned(&so.currPos) {
        bufmgr::release_buffer::call(so.currPos.buf);
        so.currPos.buf = InvalidBuffer;
    }

    /* Walk to the next page with data */
    let blkno = if ScanDirectionIsForward(dir) {
        so.currPos.nextPage
    } else {
        so.currPos.prevPage
    };
    let lastcurrblkno = so.currPos.currPage;

    /*
     * Cancel primscans scheduled when currPos was read in the opposite
     * direction to the one we're now stepping in.
     */
    if so.currPos.dir != dir {
        so.needPrimScan = false;
    }

    {
        let mcx = rel_mcx(rel);
        _bt_readnextpage(mcx, rel, so, blkno, lastcurrblkno, dir, false)
    }
}

// ===========================================================================
// _bt_readfirstpage
// ===========================================================================

/// `_bt_readfirstpage()` — Read the first page containing valid data for
/// `_bt_first`. On entry `so->currPos` must be pinned and locked.
fn _bt_readfirstpage<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    offnum: OffsetNumber,
    dir: ScanDirection,
) -> PgResult<bool> {
    so.numKilled = 0; /* just paranoia */
    so.markItemIndex = -1; /* ditto */

    /* Initialize so->currPos for the first page */
    if so.needPrimScan {
        debug_assert!(so.numArrayKeys != 0);
        so.currPos.moreLeft = true;
        so.currPos.moreRight = true;
        so.needPrimScan = false;
    } else if ScanDirectionIsForward(dir) {
        so.currPos.moreLeft = false;
        so.currPos.moreRight = true;
    } else {
        so.currPos.moreLeft = true;
        so.currPos.moreRight = false;
    }

    /*
     * Attempt to load matching tuples from the first page.
     */
    if _bt_readpage(mcx, rel, so, dir, offnum, true)? {
        /*
         * _bt_readpage succeeded. Drop the lock (and maybe the pin) on
         * so->currPos.buf in preparation for btgettuple returning tuples.
         */
        debug_assert!(BTScanPosIsPinned(&so.currPos));
        _bt_drop_lock_and_maybe_pin(rel, so)?;
        return Ok(true);
    }

    /* No actually-matching data on the page in so->currPos.buf */
    _bt_unlockbuf(rel, so.currPos.buf);

    /* Call _bt_readnextpage using its _bt_steppage wrapper */
    if !_bt_steppage(rel, so, dir)? {
        return Ok(false);
    }

    Ok(true)
}

// ===========================================================================
// _bt_readnextpage
// ===========================================================================

/// `_bt_readnextpage()` — Read the next page containing valid data for the scan.
fn _bt_readnextpage<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    mut blkno: BlockNumber,
    mut lastcurrblkno: BlockNumber,
    dir: ScanDirection,
    mut seized: bool,
) -> PgResult<bool> {
    debug_assert!(so.currPos.currPage == lastcurrblkno || seized);
    debug_assert!(!(blkno == P_NONE && seized));
    debug_assert!(!BTScanPosIsPinned(&so.currPos));

    /*
     * Remember that the scan already read lastcurrblkno (to the left of blkno
     * for forward scans, to the right for backwards scans).
     */
    if ScanDirectionIsForward(dir) {
        so.currPos.moreLeft = true;
    } else {
        so.currPos.moreRight = true;
    }

    loop {
        if blkno == P_NONE
            || (if ScanDirectionIsForward(dir) {
                !so.currPos.moreRight
            } else {
                !so.currPos.moreLeft
            })
        {
            /* most recent _bt_readpage call (for lastcurrblkno) ended scan */
            debug_assert!(so.currPos.currPage == lastcurrblkno && !seized);
            BTScanPosInvalidate(&mut so.currPos);
            bt_parallel_done(so); /* iff !so->needPrimScan */
            return Ok(false);
        }

        debug_assert!(!so.needPrimScan);

        /* parallel scan seize omitted (serial path) */

        let opaque;
        if ScanDirectionIsForward(dir) {
            /* read blkno, but check for interrupts first */
            check_for_interrupts();
            so.currPos.buf = _bt_getbuf(mcx, rel, blkno, BT_READ)?;
        } else {
            /* read blkno, avoiding race (also checks for interrupts) */
            so.currPos.buf = _bt_lock_and_validate_left(mcx, rel, &mut blkno, lastcurrblkno)?;
            if so.currPos.buf == InvalidBuffer {
                /* concurrent deletion of leftmost page */
                BTScanPosInvalidate(&mut so.currPos);
                bt_parallel_done(so);
                return Ok(false);
            }
        }

        opaque = {
            let page_bytes = bufmgr::buffer_get_page::call(mcx, so.currPos.buf)?;
            opaque_from_bytes(&page_bytes)?
        };
        lastcurrblkno = blkno;

        let mut found = false;
        if !P_IGNORE(&opaque) {
            /* see if there are any matches on this page */
            if ScanDirectionIsForward(dir) {
                if _bt_readpage(mcx, rel, so, dir, P_FIRSTDATAKEY(&opaque), seized)? {
                    found = true;
                } else {
                    blkno = so.currPos.nextPage;
                }
            } else {
                let maxoff = {
                    let page_bytes = bufmgr::buffer_get_page::call(mcx, so.currPos.buf)?;
                    let page = PageRef::new(&page_bytes)?;
                    PageGetMaxOffsetNumber(&page)
                };
                if _bt_readpage(mcx, rel, so, dir, maxoff, seized)? {
                    found = true;
                } else {
                    blkno = so.currPos.prevPage;
                }
            }
        } else {
            /* _bt_readpage not called, do the link-walk ourselves */
            if ScanDirectionIsForward(dir) {
                blkno = opaque.btpo_next;
            } else {
                blkno = opaque.btpo_prev;
            }
            /* parallel release omitted (serial path) */
        }

        if found {
            break;
        }

        /* no matching tuples on this page */
        page_bt_relbuf(rel, so.currPos.buf);
        so.currPos.buf = InvalidBuffer;
        seized = false; /* released by _bt_readpage (or by us) */
    }

    /*
     * _bt_readpage succeeded. Drop the lock (and maybe the pin) on
     * so->currPos.buf in preparation for btgettuple returning tuples.
     */
    debug_assert!(so.currPos.currPage == blkno);
    debug_assert!(BTScanPosIsPinned(&so.currPos));
    _bt_drop_lock_and_maybe_pin(rel, so)?;

    Ok(true)
}

// ===========================================================================
// _bt_lock_and_validate_left
// ===========================================================================

/// `_bt_lock_and_validate_left()` — lock caller's left sibling `blkno`,
/// recovering from concurrent splits/deletions. Returns InvalidBuffer if there
/// is no page to the left.
fn _bt_lock_and_validate_left<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    blkno: &mut BlockNumber,
    mut lastcurrblkno: BlockNumber,
) -> PgResult<Buffer> {
    let mut origblkno = *blkno; /* detects circular links */

    loop {
        /* check for interrupts while not holding any buffer lock */
        check_for_interrupts();
        let mut buf = _bt_getbuf(mcx, rel, *blkno, BT_READ)?;
        let mut opaque = {
            let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
            opaque_from_bytes(&page_bytes)?
        };

        /*
         * If this isn't the page we want, walk right (max four hops).
         * Test P_ISDELETED not P_IGNORE: half-dead pages are still in the chain.
         */
        let mut tries = 0;
        loop {
            if !P_ISDELETED(&opaque) && opaque.btpo_next == lastcurrblkno {
                /* Found desired page, return it */
                return Ok(buf);
            }
            if P_RIGHTMOST(&opaque) || {
                tries += 1;
                tries > 4
            } {
                break;
            }
            /* step right */
            *blkno = opaque.btpo_next;
            buf = _bt_relandgetbuf(mcx, rel, buf, *blkno, BT_READ)?;
            opaque = {
                let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
                opaque_from_bytes(&page_bytes)?
            };
        }

        /*
         * Return to lastcurrblkno to see what's up with its prev sibling link.
         */
        buf = _bt_relandgetbuf(mcx, rel, buf, lastcurrblkno, BT_READ)?;
        opaque = {
            let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
            opaque_from_bytes(&page_bytes)?
        };
        if P_ISDELETED(&opaque) {
            /*
             * It was deleted. Move right to first nondeleted page; that one
             * acquired the deleted page's keyspace.
             */
            loop {
                if P_RIGHTMOST(&opaque) {
                    return Err(PgError::error(format!(
                        "fell off the end of index \"{}\"",
                        rel_name(rel)
                    )));
                }
                lastcurrblkno = opaque.btpo_next;
                buf = _bt_relandgetbuf(mcx, rel, buf, lastcurrblkno, BT_READ)?;
                opaque = {
                    let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
                    opaque_from_bytes(&page_bytes)?
                };
                if !P_ISDELETED(&opaque) {
                    break;
                }
            }
        } else {
            /*
             * lastcurrblkno wasn't deleted; the page to the left got split or
             * deleted. Guard against an infinite loop.
             */
            if opaque.btpo_prev == origblkno {
                return Err(PgError::error(format!(
                    "could not find left sibling of block {} in index \"{}\"",
                    lastcurrblkno,
                    rel_name(rel)
                )));
            }
            /* Okay to try again, since left sibling link changed */
        }

        /*
         * Found a non-deleted page that should now act as lastcurrblkno.
         */
        if P_LEFTMOST(&opaque) {
            /* New lastcurrblkno has no left sibling (concurrently deleted) */
            page_bt_relbuf(rel, buf);
            break;
        }

        /* Start from scratch with new lastcurrblkno's blkno/prev link */
        *blkno = opaque.btpo_prev;
        origblkno = opaque.btpo_prev;
        page_bt_relbuf(rel, buf);
    }

    Ok(InvalidBuffer)
}

// ===========================================================================
// _bt_get_endpoint
// ===========================================================================

/// `_bt_get_endpoint()` — Find the first or last page on a given tree level.
/// Returns InvalidBuffer if the index is empty. The returned buffer is pinned
/// and read-locked.
pub fn _bt_get_endpoint<'mcx>(
    rel: &Relation<'mcx>,
    level: u32,
    rightmost: bool,
) -> PgResult<Buffer> {
    {
        let mcx = rel_mcx(rel);
        _bt_get_endpoint_inner(mcx, rel, level, rightmost)
    }
}

fn _bt_get_endpoint_inner<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    level: u32,
    rightmost: bool,
) -> PgResult<Buffer> {
    /*
     * For a leaf page, descend from the fast root; otherwise from the true root.
     */
    let mut buf = if level == 0 {
        _bt_getroot(mcx, rel, rel, BT_READ)?
    } else {
        _bt_gettrueroot(mcx, rel)?
    };

    if buf == InvalidBuffer {
        return Ok(InvalidBuffer);
    }

    let mut opaque = {
        let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
        opaque_from_bytes(&page_bytes)?
    };

    loop {
        /*
         * If we landed on a deleted page, step right to a live page. Also step
         * right to reach the rightmost page if requested.
         */
        while P_IGNORE(&opaque) || (rightmost && !P_RIGHTMOST(&opaque)) {
            let blkno = opaque.btpo_next;
            if blkno == P_NONE {
                return Err(PgError::error(format!(
                    "fell off the end of index \"{}\"",
                    rel_name(rel)
                )));
            }
            buf = _bt_relandgetbuf(mcx, rel, buf, blkno, BT_READ)?;
            opaque = {
                let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
                opaque_from_bytes(&page_bytes)?
            };
        }

        /* Done? */
        if opaque.btpo_level == level {
            break;
        }
        if opaque.btpo_level < level {
            return Err(PgError::error(format!(
                "btree level {} not found in index \"{}\"",
                level,
                rel_name(rel)
            )));
        }

        /* Descend to leftmost or rightmost child page */
        let blkno = {
            let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
            let page = PageRef::new(&page_bytes)?;
            let offnum = if rightmost {
                PageGetMaxOffsetNumber(&page)
            } else {
                P_FIRSTDATAKEY(&opaque)
            };
            let iid = PageGetItemId(&page, offnum)?;
            let itup = PageGetItem(&page, &iid)?;
            bt_tuple_get_downlink(itup)
        };

        buf = _bt_relandgetbuf(mcx, rel, buf, blkno, BT_READ)?;
        opaque = {
            let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
            opaque_from_bytes(&page_bytes)?
        };
    }

    Ok(buf)
}

// ===========================================================================
// _bt_endpoint
// ===========================================================================

/// `_bt_endpoint()` — Find the first/last page in the index and scan from there
/// to the first key satisfying all quals (used by `_bt_first` when starting at
/// one end of the tree).
fn _bt_endpoint<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    dir: ScanDirection,
) -> PgResult<bool> {
    debug_assert!(!BTScanPosIsValid(&so.currPos));
    debug_assert!(!so.needPrimScan);

    /*
     * Scan down to the leftmost or rightmost leaf page (simplified _bt_search).
     */
    so.currPos.buf = _bt_get_endpoint_inner(mcx, rel, 0, ScanDirectionIsBackward(dir))?;

    if !BTScanPosIsBuf(so.currPos.buf) {
        /*
         * Empty index. Lock the whole relation (nothing finer exists).
         */
        predicate_lock_relation(rel);
        bt_parallel_done(so);
        return Ok(false);
    }

    let (opaque, maxoff) = {
        let page_bytes = bufmgr::buffer_get_page::call(mcx, so.currPos.buf)?;
        let o = opaque_from_bytes(&page_bytes)?;
        let page = PageRef::new(&page_bytes)?;
        (o, PageGetMaxOffsetNumber(&page))
    };
    debug_assert!(P_ISLEAF(&opaque));

    let start = if ScanDirectionIsForward(dir) {
        /* There could be dead pages to the left, so not Assert(P_LEFTMOST). */
        P_FIRSTDATAKEY(&opaque)
    } else if ScanDirectionIsBackward(dir) {
        debug_assert!(P_RIGHTMOST(&opaque));
        maxoff
    } else {
        return Err(PgError::error(format!("invalid scan direction: {}", dir as i32)));
    };

    /* Now load data from the first page of the scan. */
    if !_bt_readfirstpage(mcx, rel, so, start, dir)? {
        return Ok(false);
    }

    _bt_returnitem(so);
    Ok(true)
}

// ===========================================================================
// current_heaptid (seam) — read by the AM driver after _bt_first / _bt_next.
// ===========================================================================

/// `so->currPos.items[so->currPos.itemIndex].heapTid`.
pub fn current_heaptid<'mcx>(so: &BTScanOpaqueData<'mcx>) -> ItemPointerData {
    so.currPos.items[so.currPos.itemIndex as usize].heapTid
}

/// `(IndexTuple) (so->currTuples + currItem->tupleOffset)` — the current index
/// tuple bytes for an index-only scan. `_bt_returnitem` only sets
/// `scan->xs_itup` when `so->currTuples != NULL`; otherwise this returns `None`.
/// The on-disk size is read from the tuple header (`IndexTupleSize`); the
/// contiguous image is copied out of the workspace into `mcx`.
pub fn current_itup<'mcx>(
    mcx: Mcx<'mcx>,
    so: &BTScanOpaqueData<'mcx>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let Some(curr_tuples) = so.currTuples.as_ref() else {
        return Ok(None);
    };
    let off = so.currPos.items[so.currPos.itemIndex as usize].tupleOffset as usize;
    // IndexTupleSize(itup) reads the t_info length bits from the header.
    let hdr = index_tuple_header(&curr_tuples[off..]);
    let sz = IndexTupleSize(&hdr);
    let mut out = vec_with_capacity_in(mcx, sz)?;
    out.extend_from_slice(&curr_tuples[off..off + sz]);
    Ok(Some(out))
}
