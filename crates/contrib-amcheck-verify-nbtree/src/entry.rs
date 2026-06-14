//! F1 — entry points and verification harness (verify_nbtree.c).
//!
//! The SQL-callable entry points (`bt_index_check` / `bt_index_parent_check`),
//! the level-by-level driver (`bt_check_every_level` /
//! `bt_check_level_from_leftmost`), the heapallindexed Bloom-filter feed
//! (`bt_tuple_present_callback` / `bt_normalize_tuple` / `bt_report_duplicate`
//! / `bt_entry_unique_check`), the heap-visibility probe
//! (`heap_entry_is_visible`), and the careful-read page / line-pointer helpers
//! (`palloc_btree_page` / `PageGetItemIdCareful` / `BTreeTupleGetHeapTIDCareful`
//! / `bt_mkscankey_pivotsearch`).
//!
//! Every body is a decomposition placeholder: `panic!("decomp: <fn> not yet
//! filled")`. The signatures are C-faithful so the fill stage can drop bodies
//! in without churning call sites.

use types_core::primitive::{BlockNumber, OffsetNumber, Oid};
use types_error::PgResult;
use types_nbtree::BTScanInsert;
use types_rel::Relation;
use types_tuple::heaptuple::{IndexTuple, ItemPointerData};

use amcheck_verify_common_seams::BTCallbackState;

use crate::{BtreeCheckState, BtreeLastVisibleEntry, BtreeLevel, Page};

/// `bt_index_check(index regclass, heapallindexed boolean, checkunique
/// boolean)` — light-weight verification under AccessShareLock.
pub fn bt_index_check(indrelid: Oid, heapallindexed: bool, checkunique: bool) -> PgResult<()> {
    let _ = (indrelid, heapallindexed, checkunique);
    panic!("decomp: bt_index_check not yet filled")
}

/// `bt_index_parent_check(index regclass, heapallindexed boolean, rootdescend
/// boolean, checkunique boolean)` — thorough verification under ShareLock,
/// including parent/child downlink invariants.
pub fn bt_index_parent_check(
    indrelid: Oid,
    heapallindexed: bool,
    rootdescend: bool,
    checkunique: bool,
) -> PgResult<()> {
    let _ = (indrelid, heapallindexed, rootdescend, checkunique);
    panic!("decomp: bt_index_parent_check not yet filled")
}

/// `bt_index_check_callback(indrel, heaprel, state, readonly)` — the
/// `IndexDoCheckCallback` the common driver invokes once it holds the locks:
/// extract + sanitize metapage metadata, then run `bt_check_every_level`.
pub fn bt_index_check_callback<'mcx>(
    indrel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    state: &BTCallbackState,
    readonly: bool,
) -> PgResult<()> {
    let _ = (indrel, heaprel, state, readonly);
    panic!("decomp: bt_index_check_callback not yet filled")
}

/// `bt_check_every_level(rel, heaprel, heapkeyspace, readonly, heapallindexed,
/// rootdescend, checkunique)` — walk the whole index, level by level, top to
/// bottom. Acquires the snapshot, sets up the per-page context and the Bloom
/// filter, reads the metapage, and drives `bt_check_level_from_leftmost`.
pub fn bt_check_every_level<'mcx>(
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    heapkeyspace: bool,
    readonly: bool,
    heapallindexed: bool,
    rootdescend: bool,
    checkunique: bool,
) -> PgResult<()> {
    let _ = (
        rel,
        heaprel,
        heapkeyspace,
        readonly,
        heapallindexed,
        rootdescend,
        checkunique,
    );
    panic!("decomp: bt_check_every_level not yet filled")
}

/// `bt_check_level_from_leftmost(state, level)` — verify one entire level by
/// walking its pages left-to-right via right-links, returning the descent
/// point for the next level down.
pub fn bt_check_level_from_leftmost<'mcx>(
    state: &mut BtreeCheckState<'mcx>,
    level: BtreeLevel,
) -> PgResult<BtreeLevel> {
    let _ = (state, level);
    panic!("decomp: bt_check_level_from_leftmost not yet filled")
}

/// `heap_entry_is_visible(state, tid)` — is the heap tuple at `tid` visible to
/// the check's snapshot (used by the uniqueness check)?
pub fn heap_entry_is_visible<'mcx>(
    state: &BtreeCheckState<'mcx>,
    tid: &ItemPointerData,
) -> PgResult<bool> {
    let _ = (state, tid);
    panic!("decomp: heap_entry_is_visible not yet filled")
}

/// `bt_report_duplicate(state, lVis, nexttid, nblock, noffset, nposting)` —
/// build the uniqueness-violation error message and `ereport(ERROR)`.
pub fn bt_report_duplicate<'mcx>(
    state: &BtreeCheckState<'mcx>,
    l_vis: &BtreeLastVisibleEntry,
    nexttid: &ItemPointerData,
    nblock: BlockNumber,
    noffset: OffsetNumber,
    nposting: i32,
) -> PgResult<()> {
    let _ = (state, l_vis, nexttid, nblock, noffset, nposting);
    panic!("decomp: bt_report_duplicate not yet filled")
}

/// `bt_entry_unique_check(state, itup, targetblock, offset, lVis)` — check
/// that the current leaf entry complies with the UNIQUE constraint, updating
/// the last-visible-entry tracker.
pub fn bt_entry_unique_check<'mcx>(
    state: &mut BtreeCheckState<'mcx>,
    itup: &IndexTuple<'mcx>,
    targetblock: BlockNumber,
    offset: OffsetNumber,
    l_vis: &mut BtreeLastVisibleEntry,
) -> PgResult<()> {
    let _ = (state, itup, targetblock, offset, l_vis);
    panic!("decomp: bt_entry_unique_check not yet filled")
}

/// `bt_tuple_present_callback(index, tid, values, isnull, tupleIsAlive,
/// checkstate)` — the `table_index_build_scan` callback for heapallindexed:
/// form the index tuple from the heap datums, normalize it, and probe the
/// Bloom filter for its fingerprint.
pub fn bt_tuple_present_callback<'mcx>(
    state: &mut BtreeCheckState<'mcx>,
    index: &Relation<'mcx>,
    tid: &ItemPointerData,
    values: &[types_datum::datum::Datum],
    isnull: &[bool],
    tuple_is_alive: bool,
) -> PgResult<()> {
    let _ = (state, index, tid, values, isnull, tuple_is_alive);
    panic!("decomp: bt_tuple_present_callback not yet filled")
}

/// `bt_normalize_tuple(state, itup)` — normalize a (possibly toasted /
/// posting-list) index tuple to the canonical form fingerprinted by the Bloom
/// filter, so heap-derived and index-derived tuples compare bit-for-bit.
pub fn bt_normalize_tuple<'mcx>(
    state: &BtreeCheckState<'mcx>,
    itup: &IndexTuple<'mcx>,
) -> PgResult<IndexTuple<'mcx>> {
    let _ = (state, itup);
    panic!("decomp: bt_normalize_tuple not yet filled")
}

/// `palloc_btree_page(state, blocknum)` — read `blocknum` into a private
/// `palloc(BLCKSZ)` page copy, running the careful-read sanity checks
/// (`_bt_checkpage` and friends) before returning it.
pub fn palloc_btree_page<'mcx>(
    state: &BtreeCheckState<'mcx>,
    blocknum: BlockNumber,
) -> PgResult<Page<'mcx>> {
    let _ = (state, blocknum);
    panic!("decomp: palloc_btree_page not yet filled")
}

/// `PageGetItemIdCareful(state, block, page, offset)` — fetch the line pointer
/// at `offset`, validating that it points within the page's bounds (raises a
/// corruption ereport otherwise). Returns the `(off, len)` line-pointer pair.
pub fn PageGetItemIdCareful<'mcx>(
    state: &BtreeCheckState<'mcx>,
    block: BlockNumber,
    page: &Page<'mcx>,
    offset: OffsetNumber,
) -> PgResult<(u32, u32)> {
    let _ = (state, block, page, offset);
    panic!("decomp: PageGetItemIdCareful not yet filled")
}

/// `BTreeTupleGetHeapTIDCareful(state, itup, nonpivot)` — fetch the heap TID
/// of `itup`, validating that the tuple's pivot/non-pivot shape matches the
/// caller's expectation (raises a corruption ereport otherwise).
pub fn BTreeTupleGetHeapTIDCareful<'mcx>(
    state: &BtreeCheckState<'mcx>,
    itup: &IndexTuple<'mcx>,
    nonpivot: bool,
) -> PgResult<Option<ItemPointerData>> {
    let _ = (state, itup, nonpivot);
    panic!("decomp: BTreeTupleGetHeapTIDCareful not yet filled")
}

/// `bt_mkscankey_pivotsearch(rel, itup)` — build an insertion scankey via
/// `_bt_mkscankey` and flip it into pivot-search (backward) mode, as used for
/// the cross-page boundary checks.
pub fn bt_mkscankey_pivotsearch<'mcx>(
    rel: &Relation<'mcx>,
    itup: Option<&IndexTuple<'mcx>>,
) -> PgResult<BTScanInsert> {
    let _ = (rel, itup);
    panic!("decomp: bt_mkscankey_pivotsearch not yet filled")
}
