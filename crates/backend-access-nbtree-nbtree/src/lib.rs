//! Idiomatic port of `src/backend/access/nbtree/nbtree.c` (PostgreSQL 18.3) —
//! the B-tree access-method handler and the AM-API callback implementations.
//!
//! The dispatch-table assembly, scan-state bookkeeping, and VACUUM page-walk
//! orchestration are ported in-crate over the runtime structs in
//! [`types_nbtree`]. The cross-module nbtree functions owned by the
//! `nbtree-core` unit (`_bt_first` / `_bt_next` / `_bt_doinsert` /
//! `_bt_killitems` / `_bt_pagedel` / …), the buffer-manager / smgr / FSM /
//! read-stream / lmgr substrate, the index-vacuum tuple-is-dead callback,
//! `index_form_tuple`, and `tbm_add_tuples` are reached through their owners'
//! `-seams` crates and panic until those owners land.
//!
//! Parallel build / parallel scan is deferred honestly: the parallel
//! coordination seams loud-panic by default; the serial path never reaches
//! them.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

use mcx::{slice_in, vec_with_capacity_in, Mcx, MemoryContext, PgVec};
use types_amapi::{
    CompareType, IndexAmRoutine, COMPARE_EQ, COMPARE_GE, COMPARE_GT, COMPARE_INVALID, COMPARE_LE,
    COMPARE_LT, T_IndexAmRoutine,
};
use types_core::primitive::{BlockNumber, OffsetNumber, Oid, Size};
use types_core::InvalidOid;
use types_error::PgResult;
use types_nbtree::{
    BTParallelScanDescData, BTPS_State, BTScanOpaqueData, BTScanPosInvalidate, BTScanPosIsPinned,
    BTScanPosIsValid, BTVacState, BTVacuumPosting, BTCycleId, IndexBulkDeleteResult,
    IndexUniqueCheck, BTMaxStrategyNumber, BTNProcs, BTOPTIONS_PROC, BTP_DELETED, BTP_HALF_DEAD,
    BTP_LEAF, BTP_SPLIT_END, BTREE_METAPAGE, MaxIndexTuplesPerPage, MaxTIDsPerBTreePage, P_FIRSTKEY,
    P_HIKEY, P_NONE,
};
use types_rel::Relation;
use types_scan::scankey::{
    ScanKeyData, StrategyNumber, BTEqualStrategyNumber, BTGreaterEqualStrategyNumber,
    BTGreaterStrategyNumber, BTLessEqualStrategyNumber, BTLessStrategyNumber, InvalidStrategy,
    SK_BT_MAXVAL, SK_BT_MINVAL, SK_BT_SKIP, SK_ISNULL, SK_SEARCHNULL,
};
use types_storage::storage::LWLockMode;
use types_scan::sdir::ScanDirection;
use types_storage::storage::{Buffer, BufferIsValid, InvalidBuffer};
use types_tuple::heaptuple::ItemPointerData;
// The canonical unified value type (Datum-unification keystone). Used for
// own-logic scalar construction at the value-consuming `datum_*_v` seams.
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_access_common_indextuple_seams::index_form_tuple;
use backend_access_index_indexam_seams as parallel;
use backend_access_nbtree_core_seams as core;
use backend_storage_lmgr_condition_variable_seams as condvar;
use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_utils_adt_datum_seams as datumser;
use backend_nodes_core_seams::tbm_add_tuple;
use backend_storage_aio_seams as readstream;
use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_storage_freespace_seams as indexfsm;
use backend_storage_lmgr_lmgr_seams as lmgr;
use backend_utils_cache_relcache_seams as relcache;

// VACUUM parallel-option flags (commands/vacuum.h).
/// `VACUUM_OPTION_PARALLEL_BULKDEL`.
const VACUUM_OPTION_PARALLEL_BULKDEL: u8 = 1 << 0;
/// `VACUUM_OPTION_PARALLEL_COND_CLEANUP`.
const VACUUM_OPTION_PARALLEL_COND_CLEANUP: u8 = 1 << 1;

// ===========================================================================
// IndexScanDescData view + IndexVacuumInfo subset (idiomatic, see module docs).
// ===========================================================================

/// The `IndexScanDescData` subset the `nbtree.c` AM entry points manipulate.
/// The index relation is held as the open [`Relation`] handle for the scan's
/// lifetime (C: `scan->indexRelation`).
#[derive(Debug)]
pub struct NbtScan<'mcx> {
    /// `scan->indexRelation`.
    pub indexRelation: Relation<'mcx>,
    /// `scan->opaque` — the btree-private scan state.
    pub opaque: BTScanOpaqueData<'mcx>,
    /// `scan->parallel_scan != NULL` (carries the DSM handle when parallel).
    pub parallel_scan: Option<u64>,
    /// `scan->heapRelation != NULL` — false for bitmap index scans.
    pub heapRelation: bool,
    /// `scan->xs_snapshot != NULL && IsMVCCSnapshot(...)` presence flag.
    pub xs_snapshot_is_valid: bool,
    /// `scan->xs_want_itup`.
    pub xs_want_itup: bool,
    /// `scan->xs_recheck`.
    pub xs_recheck: bool,
    /// `scan->kill_prior_tuple`.
    pub kill_prior_tuple: bool,
    /// `scan->xs_heaptid`.
    pub xs_heaptid: ItemPointerData,
}

/// `IndexVacuumInfo` (`access/genam.h`) — the subset `btvacuumscan` /
/// `btvacuumpage` read directly.
#[derive(Clone, Copy, Debug, Default)]
pub struct NbtVacuumInfo {
    /// `analyze_only`.
    pub analyze_only: bool,
    /// `report_progress`.
    pub report_progress: bool,
    /// `estimated_count`.
    pub estimated_count: bool,
    /// `num_heap_tuples`.
    pub num_heap_tuples: f64,
}

// `pgstat_progress_update_param` index codes (commands/progress.h).
const PROGRESS_SCAN_BLOCKS_TOTAL: i32 = 15;
const PROGRESS_SCAN_BLOCKS_DONE: i32 = 16;

// ===========================================================================
// bthandler
// ===========================================================================

/// The non-pure AM callbacks of nbtree, named so a caller that cannot install
/// the raw fmgr-pointer ABI can still reach them by name. Mirrors the
/// `amroutine->amXXX = btXXX` assignments for the callbacks that are this
/// crate's own functions (rather than pure translate/validate functions).
pub const BT_AM_CALLBACKS: &[&str] = &[
    "btbuild",
    "btbuildempty",
    "btinsert",
    "btbulkdelete",
    "btvacuumcleanup",
    "btcanreturn",
    "btcostestimate",
    "btgettreeheight",
    "btoptions",
    "btproperty",
    "btbuildphasename",
    "btvalidate",
    "btadjustmembers",
    "btbeginscan",
    "btrescan",
    "btgettuple",
    "btgetbitmap",
    "btendscan",
    "btmarkpos",
    "btrestrpos",
    "btestimateparallelscan",
    "btinitparallelscan",
    "btparallelrescan",
];

/// `bthandler()` — return [`IndexAmRoutine`] with AM parameters and callbacks.
pub fn bthandler() -> IndexAmRoutine {
    IndexAmRoutine {
        type_: T_IndexAmRoutine,
        amstrategies: BTMaxStrategyNumber,
        amsupport: BTNProcs,
        amoptsprocnum: BTOPTIONS_PROC,
        amcanorder: true,
        amcanorderbyop: false,
        amcanhash: false,
        amconsistentequality: true,
        amconsistentordering: true,
        amcanbackward: true,
        amcanunique: true,
        amcanmulticol: true,
        amoptionalkey: true,
        amsearcharray: true,
        amsearchnulls: true,
        amstorage: false,
        amclusterable: true,
        ampredlocks: true,
        amcanparallel: true,
        amcanbuildparallel: true,
        amcaninclude: true,
        amusemaintenanceworkmem: false,
        amsummarizing: false,
        amparallelvacuumoptions: VACUUM_OPTION_PARALLEL_BULKDEL
            | VACUUM_OPTION_PARALLEL_COND_CLEANUP,
        amkeytype: InvalidOid,
        amtranslatestrategy: Some(bttranslatestrategy),
        amtranslatecmptype: Some(bttranslatecmptype),
        // btvalidate (nbtvalidate.c) returns a soft-error result and so cannot
        // be the raw `fn(Oid) -> bool` ABI pointer; it is reached by name.
        amvalidate: None,
    }
}

// ===========================================================================
// btbuildempty
// ===========================================================================

/// `btbuildempty()` — build an empty btree index in the initialization fork.
pub fn btbuildempty(index: &Relation) -> PgResult<()> {
    let allequalimage = core::bt_allequalimage::call(index)?;
    // smgr_bulk_start_rel(index, INIT_FORKNUM); _bt_initmetapage at
    // BTREE_METAPAGE with P_NONE/level 0/allequalimage; smgr_bulk_write/finish.
    core::build_empty_metapage::call(index, allequalimage)
}

// ===========================================================================
// btinsert
// ===========================================================================

/// `btinsert()` — insert an index tuple into a btree.
pub fn btinsert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    values: &[Datum<'mcx>],
    isnull: &[bool],
    ht_ctid: ItemPointerData,
    heap_rel: &Relation<'mcx>,
    check_unique: IndexUniqueCheck,
    index_unchanged: bool,
) -> PgResult<bool> {
    // generate an index tuple; itup->t_tid = *ht_ctid.
    let itup = index_form_tuple::call(mcx, rel, values, isnull, ht_ctid)?;
    let result = core::bt_doinsert::call(rel, &itup, check_unique, index_unchanged, heap_rel)?;
    // pfree(itup): itup is dropped here.
    Ok(result)
}

// ===========================================================================
// btgettuple
// ===========================================================================

/// `btgettuple()` — Get the next tuple in the scan.
pub fn btgettuple<'mcx>(mcx: Mcx<'mcx>, scan: &mut NbtScan<'mcx>, dir: ScanDirection) -> PgResult<bool> {
    debug_assert!(scan.heapRelation);

    // btree indexes are never lossy
    scan.xs_recheck = false;

    let rel = scan.indexRelation.alias();
    let mut res;
    // Each loop iteration performs another primitive index scan
    loop {
        // If we've already initialized this scan, advance it; otherwise call
        // _bt_first() to get the first item.
        if !BTScanPosIsValid(&scan.opaque.currPos) {
            res = core::bt_first::call(&rel, &mut scan.opaque, dir)?;
        } else {
            // Check whether to kill the previously-fetched tuple.
            if scan.kill_prior_tuple {
                // Remember it for later. Test for numKilled overrun is not just
                // paranoia: reversing direction can re-enter the same item.
                if scan.opaque.killedItems.is_empty() {
                    let mut v = vec_with_capacity_in(mcx, MaxTIDsPerBTreePage)?;
                    v.resize(MaxTIDsPerBTreePage, 0i32);
                    scan.opaque.killedItems = v;
                }
                if (scan.opaque.numKilled as usize) < MaxTIDsPerBTreePage {
                    let n = scan.opaque.numKilled as usize;
                    scan.opaque.killedItems[n] = scan.opaque.currPos.itemIndex;
                    scan.opaque.numKilled += 1;
                }
            }

            // Now continue the scan.
            res = core::bt_next::call(&rel, &mut scan.opaque, dir)?;
        }

        // If we have a tuple, return it ...
        if res {
            break;
        }
        // ... otherwise see if we need another primitive index scan
        if !(scan.opaque.numArrayKeys != 0
            && core::bt_start_prim_scan::call(&rel, &mut scan.opaque, dir))
        {
            break;
        }
    }

    Ok(res)
}

// ===========================================================================
// btgetbitmap
// ===========================================================================

/// `btgetbitmap()` — gather all matching tuples into a bitmap; returns the
/// number of TIDs added.
pub fn btgetbitmap<'mcx>(
    scan: &mut NbtScan<'mcx>,
    tbm: &mut types_tidbitmap::TIDBitmap,
) -> PgResult<i64> {
    debug_assert!(!scan.heapRelation);

    let rel = scan.indexRelation.alias();
    let mut ntids: i64 = 0;

    // Each loop iteration performs another primitive index scan
    loop {
        // Fetch the first page & tuple
        if core::bt_first::call(&rel, &mut scan.opaque, ScanDirection::ForwardScanDirection)? {
            // Save tuple ID, and continue scanning
            let mut heap_tid = core::current_heaptid::call(&scan.opaque);
            tbm_add_tuple::call(&mut *tbm, heap_tid)?;
            ntids += 1;

            loop {
                // Advance to next tuple within page (same as the easy case in
                // _bt_next()).
                scan.opaque.currPos.itemIndex += 1;
                if scan.opaque.currPos.itemIndex > scan.opaque.currPos.lastItem {
                    // let _bt_next do the heavy lifting
                    if !core::bt_next::call(
                        &rel,
                        &mut scan.opaque,
                        ScanDirection::ForwardScanDirection,
                    )? {
                        break;
                    }
                }

                // Save tuple ID, and continue scanning
                let idx = scan.opaque.currPos.itemIndex as usize;
                heap_tid = scan.opaque.currPos.items[idx].heapTid;
                tbm_add_tuple::call(&mut *tbm, heap_tid)?;
                ntids += 1;
            }
        }
        // Now see if we need another primitive index scan
        if !(scan.opaque.numArrayKeys != 0
            && core::bt_start_prim_scan::call(
                &rel,
                &mut scan.opaque,
                ScanDirection::ForwardScanDirection,
            ))
        {
            break;
        }
    }

    Ok(ntids)
}

// ===========================================================================
// btbeginscan
// ===========================================================================

/// `btbeginscan()` — start a scan on a btree index.
pub fn btbeginscan<'mcx>(
    mcx: Mcx<'mcx>,
    rel: Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<NbtScan<'mcx>> {
    // no order by operators allowed
    debug_assert!(norderbys == 0);

    let mut so = BTScanOpaqueData::new(mcx);
    BTScanPosInvalidate(&mut so.currPos);
    BTScanPosInvalidate(&mut so.markPos);
    if nkeys > 0 {
        let mut kd = vec_with_capacity_in(mcx, nkeys as usize)?;
        for _ in 0..nkeys {
            kd.push(ScanKeyData::empty());
        }
        so.keyData = kd;
    }

    so.skipScan = false;
    so.needPrimScan = false;
    so.scanBehind = false;
    so.oppositeDirCheck = false;
    // so->arrayKeys / orderProcs / arrayContext stay empty until btrescan.

    so.numKilled = 0;

    // We don't know yet whether the scan is index-only, so the tuple workspace
    // arrays are not allocated until btrescan.
    so.currTuples = None;
    so.markTuples = None;

    Ok(NbtScan {
        indexRelation: rel,
        opaque: so,
        parallel_scan: None,
        heapRelation: false,
        xs_snapshot_is_valid: false,
        xs_want_itup: false,
        xs_recheck: false,
        kill_prior_tuple: false,
        xs_heaptid: ItemPointerData::default(),
    })
}

// ===========================================================================
// btrescan
// ===========================================================================

/// `btrescan()` — rescan an index relation. `scankey == None` (the C NULL
/// `scankey`) leaves the keys unchanged.
pub fn btrescan<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut NbtScan<'mcx>,
    scankey: Option<&[ScanKeyData<'mcx>]>,
    norderbys: i32,
) -> PgResult<()> {
    let _ = norderbys;
    let rel = scan.indexRelation.alias();

    // we aren't holding any read locks, but gotta drop the pins
    if BTScanPosIsValid(&scan.opaque.currPos) {
        // Before leaving current page, deal with any killed items
        if scan.opaque.numKilled > 0 {
            core::bt_killitems::call(&rel, &mut scan.opaque);
        }
        bt_scan_pos_unpin_if_pinned_curr(&mut scan.opaque);
        BTScanPosInvalidate(&mut scan.opaque.currPos);
    }

    // We prefer to eagerly drop leaf page pins before btgettuple returns, but
    // not for index-only scans, non-MVCC plain index scans, bitmap scans, or
    // unlogged relation scans (see nbtree/README). dropPin never changes across
    // rescans.
    scan.opaque.dropPin = !scan.xs_want_itup
        && scan.xs_snapshot_is_valid
        && relcache::relation_needs_wal::call(&scan.indexRelation)
        && scan.heapRelation;

    scan.opaque.markItemIndex = -1;
    scan.opaque.needPrimScan = false;
    scan.opaque.scanBehind = false;
    scan.opaque.oppositeDirCheck = false;
    bt_scan_pos_unpin_if_pinned_mark(&mut scan.opaque);
    BTScanPosInvalidate(&mut scan.opaque.markPos);

    // Allocate tuple workspace arrays, if needed for an index-only scan and not
    // already done in a previous rescan call.
    if scan.xs_want_itup && scan.opaque.currTuples.is_none() {
        let mut curr = vec_with_capacity_in(mcx, types_core::primitive::BLCKSZ)?;
        curr.resize(types_core::primitive::BLCKSZ, 0u8);
        let mut mark = vec_with_capacity_in(mcx, types_core::primitive::BLCKSZ)?;
        mark.resize(types_core::primitive::BLCKSZ, 0u8);
        scan.opaque.currTuples = Some(curr);
        scan.opaque.markTuples = Some(mark);
    }

    // Reset the scan keys. (C copies numberOfKeys ScanKeyData from scankey into
    // scan->keyData only when scankey != NULL && numberOfKeys > 0.)
    if let Some(sk) = scankey {
        if !sk.is_empty() {
            scan.opaque.keyData = slice_in(mcx, sk)?;
        }
    }
    scan.opaque.numberOfKeys = 0; // until _bt_preprocess_keys sets it
    scan.opaque.numArrayKeys = 0; // ditto
    Ok(())
}

// ===========================================================================
// btendscan
// ===========================================================================

/// `btendscan()` — close down a scan.
pub fn btendscan(scan: &mut NbtScan) {
    let rel = scan.indexRelation.alias();

    // we aren't holding any read locks, but gotta drop the pins
    if BTScanPosIsValid(&scan.opaque.currPos) {
        // Before leaving current page, deal with any killed items
        if scan.opaque.numKilled > 0 {
            core::bt_killitems::call(&rel, &mut scan.opaque);
        }
        bt_scan_pos_unpin_if_pinned_curr(&mut scan.opaque);
    }

    scan.opaque.markItemIndex = -1;
    bt_scan_pos_unpin_if_pinned_mark(&mut scan.opaque);

    // No need to invalidate positions; the storage is about to be freed
    // (keyData/arrayKeys/orderProcs/killedItems/currTuples/markTuples are owned
    // PgVecs dropped with `scan`).
}

// ===========================================================================
// btmarkpos
// ===========================================================================

/// `btmarkpos()` — save current scan position.
pub fn btmarkpos(scan: &mut NbtScan) {
    // There may be an old mark with a pin (but no lock).
    bt_scan_pos_unpin_if_pinned_mark(&mut scan.opaque);

    // Just record the current itemIndex. _bt_steppage makes a full copy into
    // markPos if we later step to the next page before releasing the mark.
    if BTScanPosIsValid(&scan.opaque.currPos) {
        scan.opaque.markItemIndex = scan.opaque.currPos.itemIndex;
    } else {
        BTScanPosInvalidate(&mut scan.opaque.markPos);
        scan.opaque.markItemIndex = -1;
    }
}

// ===========================================================================
// btrestrpos
// ===========================================================================

/// `btrestrpos()` — restore scan to last saved position.
pub fn btrestrpos(scan: &mut NbtScan) {
    let rel = scan.indexRelation.alias();

    if scan.opaque.markItemIndex >= 0 {
        // The scan never moved to a new page since the last mark. Just restore
        // itemIndex (so->markPos is not reliable in this case).
        scan.opaque.currPos.itemIndex = scan.opaque.markItemIndex;
    } else {
        // The scan moved to a new page after the last mark/restore, and we are
        // now restoring to the marked page. Drop the pin for the current
        // position if we still hold one.
        if BTScanPosIsValid(&scan.opaque.currPos) {
            // Before leaving current page, deal with any killed items
            if scan.opaque.numKilled > 0 {
                core::bt_killitems::call(&rel, &mut scan.opaque);
            }
            bt_scan_pos_unpin_if_pinned_curr(&mut scan.opaque);
        }

        if BTScanPosIsValid(&scan.opaque.markPos) {
            // bump pin on mark buffer for assignment to current buffer
            if BTScanPosIsPinned(&scan.opaque.markPos) {
                bufmgr::incr_buffer_ref_count::call(scan.opaque.markPos.buf);
            }
            // memcpy(&so->currPos, &so->markPos, ... markPos.lastItem ...)
            scan.opaque.currPos = scan.opaque.markPos.clone();
            if scan.opaque.currTuples.is_some() {
                // memcpy(so->currTuples, so->markTuples, markPos.nextTupleOffset)
                let n = scan.opaque.markPos.nextTupleOffset as usize;
                let mark_copy: Option<PgVec<u8>> = scan.opaque.markTuples.clone();
                if let (Some(mark), Some(curr)) = (mark_copy, scan.opaque.currTuples.as_mut()) {
                    curr[..n].copy_from_slice(&mark[..n]);
                }
            }
            // Reset the scan's array keys (see _bt_steppage for why)
            if scan.opaque.numArrayKeys != 0 {
                let dir = scan.opaque.currPos.dir;
                core::bt_start_array_keys::call(&rel, &mut scan.opaque, dir);
                scan.opaque.needPrimScan = false;
            }
        } else {
            BTScanPosInvalidate(&mut scan.opaque.currPos);
        }
    }
}

// ---------------------------------------------------------------------------
// BTScanPosUnpinIfPinned helpers (nbtree.h inline).
// ---------------------------------------------------------------------------

fn bt_scan_pos_unpin_if_pinned_curr(so: &mut BTScanOpaqueData) {
    if BTScanPosIsPinned(&so.currPos) {
        bufmgr::release_buffer::call(so.currPos.buf);
        so.currPos.buf = InvalidBuffer;
    }
}

fn bt_scan_pos_unpin_if_pinned_mark(so: &mut BTScanOpaqueData) {
    if BTScanPosIsPinned(&so.markPos) {
        bufmgr::release_buffer::call(so.markPos.buf);
        so.markPos.buf = InvalidBuffer;
    }
}

// ===========================================================================
// btestimateparallelscan / btinitparallelscan / btparallelrescan
// ===========================================================================

/// `BTMaxItemSize` (`access/nbtree.h`):
/// `MAXALIGN_DOWN((BLCKSZ - MAXALIGN(SizeOfPageHeaderData + 3*sizeof(ItemIdData)) -
///   MAXALIGN(sizeof(BTPageOpaqueData))) / 3) - MAXALIGN(sizeof(ItemPointerData))`
///   = `MAXALIGN_DOWN((8192 - 40 - 16) / 3) - 8` = `2712 - 8` = `2704`.
const BTMaxItemSize: Size = 2704;

/// `WAIT_EVENT_BTREE_PAGE` (`PG_WAIT_IPC | 7`) — verified `= 134217735`.
const WAIT_EVENT_BTREE_PAGE: u32 = 0x0800_0000 | 7;

/// `IndexRelationGetNumberOfKeyAttributes(rel)` — `rel->rd_index->indnkeyatts`.
fn index_relation_get_number_of_key_attributes(rel: &Relation) -> i16 {
    rel.rd_index
        .as_ref()
        .expect("nbtree relation has no rd_index")
        .indnkeyatts
}

/// Resolve the DSM handle to the `BTParallelScanDescData` it carries
/// (`(BTParallelScanDesc) OffsetToPointer(parallel_scan, ps_offset_am)`), then
/// run `f` over the resolved struct. The pointer aliases shared memory the
/// (unported) parallel index-scan infrastructure owns; until it lands the
/// resolver panics, so a serial scan never reaches this.
fn with_btscan<R>(parallel_handle: u64, f: impl FnOnce(&mut BTParallelScanDescData) -> R) -> R {
    let ptr = parallel::bt_resolve_parallel_scan::call(parallel_handle);
    // SAFETY: the resolver returns the live DSM pointer for this scan, exactly
    // as the C `OffsetToPointer` macro does; the embedded `btps_lock` serializes
    // concurrent access at the call sites below.
    let btscan = unsafe { &mut *ptr };
    f(btscan)
}

/// `btestimateparallelscan` — estimate storage for `BTParallelScanDescData`.
pub fn btestimateparallelscan(rel: &Relation, nkeys: i32, _norderbys: i32) -> PgResult<Size> {
    let nkeyatts = index_relation_get_number_of_key_attributes(rel);

    // Pessimistically assume that every input scan key will be output with its
    // own SAOP array.
    let mut estnbtreeshared: Size =
        BTParallelScanDescData_offsetof_btps_arrElems() + ::core::mem::size_of::<i32>() * nkeys as usize;

    // Single column indexes cannot possibly use a skip array.
    if nkeyatts == 1 {
        return Ok(estnbtreeshared);
    }

    // Pessimistically assume that all attributes prior to the least significant
    // attribute require a skip array (and an associated key).
    let genericattrspace = datumser::datum_estimate_space_v::call(
        &Datum::null(),
        false,
        true,
        // C: `sizeof(Datum)` — the machine word that backs a by-value scalar.
        ::core::mem::size_of::<usize>() as i32,
    );
    for attnum in 1..nkeyatts {
        // Every skip array must have space to store its scan key's sk_flags.
        estnbtreeshared = add_size(estnbtreeshared, ::core::mem::size_of::<i32>())?;

        // Consider space required to store a datum of opclass input type.
        let attr = &rel
            .rd_att
            .compact_attrs[(attnum - 1) as usize];
        if attr.attbyval {
            // This index attribute stores pass-by-value datums.
            let estfixed =
                datumser::datum_estimate_space_v::call(&Datum::null(), false, true, attr.attlen as i32);
            estnbtreeshared = add_size(estnbtreeshared, estfixed)?;
            continue;
        }

        // Pass-by-reference: assume a pass-by-value datum's worth plus the
        // largest possible whole index tuple.
        estnbtreeshared = add_size(estnbtreeshared, genericattrspace)?;
        estnbtreeshared = add_size(estnbtreeshared, BTMaxItemSize)?;
    }

    Ok(estnbtreeshared)
}

/// `offsetof(BTParallelScanDescData, btps_arrElems)`.
#[inline]
fn BTParallelScanDescData_offsetof_btps_arrElems() -> Size {
    types_nbtree::BTPARALLEL_HEADER_SIZE
}

/// `add_size(s1, s2)` (shmem.c) — overflow-checked size addition. The C version
/// `ereport(ERROR)`s on overflow; here that surfaces as the context OOM-shaped
/// error via a checked add.
fn add_size(s1: Size, s2: Size) -> PgResult<Size> {
    s1.checked_add(s2)
        .ok_or_else(|| types_error::PgError::error("requested shared memory size overflows size_t"))
}

/// `_bt_parallel_serialize_arrays()` — serialize parallel array state. Caller
/// must hold `btscan->btps_lock` exclusively.
fn _bt_parallel_serialize_arrays(btscan: &mut BTParallelScanDescData, so: &mut BTScanOpaqueData) {
    let numkeys = so.numArrayKeys as usize;
    // Space for serialized datums begins immediately after btps_arrElems[].
    // SAFETY: numkeys == so->numArrayKeys, the FAM sizing assumption.
    let mut datumshared = unsafe { btscan.btps_datumshared(numkeys) };
    for i in 0..so.numArrayKeys as usize {
        let array = &so.arrayKeys[i];
        let skey = &so.keyData[array.scan_key as usize];

        if array.num_elems != -1 {
            // Save SAOP array's cur_elem (no need to copy key/datum).
            debug_assert!((skey.sk_flags & SK_BT_SKIP) == 0);
            // SAFETY: i < numkeys.
            unsafe { btscan.set_btps_arr_elem(i, array.cur_elem) };
            continue;
        }

        // Save all mutable state associated with skip array's key.
        debug_assert!((skey.sk_flags & SK_BT_SKIP) != 0);
        // memcpy(datumshared, &skey->sk_flags, sizeof(int)); datumshared += 4.
        // SAFETY: datumshared points into the DSM datum region sized by estimate.
        unsafe {
            ::core::ptr::write_unaligned(datumshared as *mut i32, skey.sk_flags);
            datumshared = datumshared.add(::core::mem::size_of::<i32>());
        }

        if (skey.sk_flags & (SK_BT_MINVAL | SK_BT_MAXVAL)) != 0 {
            // No sk_argument datum to serialize (C: `Assert(skey->sk_argument == 0)`).
            // The canonical NULL word is `Datum::null()` (`ByVal(0)`); the value
            // compare avoids `as_usize()` on a `ByRef`.
            debug_assert!(skey.sk_argument == Datum::null());
            continue;
        }

        datumshared = datumser::datum_serialize_v::call(
            &skey.sk_argument,
            (skey.sk_flags & SK_ISNULL) != 0,
            array.attbyval,
            array.attlen as i32,
            datumshared,
        );
    }
}

/// `_bt_parallel_restore_arrays()` — restore serialized parallel array state.
/// Caller must hold `btscan->btps_lock` exclusively.
fn _bt_parallel_restore_arrays(btscan: &mut BTParallelScanDescData, so: &mut BTScanOpaqueData) {
    let numkeys = so.numArrayKeys as usize;
    // Space for serialized datums begins immediately after btps_arrElems[].
    // SAFETY: numkeys == so->numArrayKeys.
    let mut datumshared = unsafe { btscan.btps_datumshared(numkeys) };
    for i in 0..so.numArrayKeys as usize {
        let num_elems;
        let attbyval;
        let scan_key;
        let cur_elem_saved;
        {
            let array = &so.arrayKeys[i];
            num_elems = array.num_elems;
            attbyval = array.attbyval;
            scan_key = array.scan_key as usize;
        }
        // SAFETY: i < numkeys.
        cur_elem_saved = unsafe { btscan.btps_arr_elem(i) };

        if num_elems != -1 {
            // Restore SAOP array using its saved cur_elem.
            debug_assert!((so.keyData[scan_key].sk_flags & SK_BT_SKIP) == 0);
            so.arrayKeys[i].cur_elem = cur_elem_saved;
            let ce = so.arrayKeys[i].cur_elem as usize;
            // `elem_values` is now the canonical `Datum<'mcx>` (types-nbtree
            // migrated): copy the saved array element straight into
            // `sk_argument: Datum<'mcx>` (C: `skey->sk_argument = array->elem_values[curelem]`).
            so.keyData[scan_key].sk_argument = so.arrayKeys[i].elem_values[ce].clone();
            continue;
        }

        // Restore skip array by restoring its key directly.
        {
            let skey = &mut so.keyData[scan_key];
            if !attbyval && skey.sk_argument != Datum::null() {
                // pfree(DatumGetPointer(skey->sk_argument)): the C frees the old
                // pass-by-ref datum; under mcx ownership it is released when the
                // owning context resets. (Pointer is into another context.)
                //
                // C's `if (!array->attbyval && skey->sk_argument)` is a non-NULL
                // pointer test. On the canonical `Datum<'mcx>` a NULL pass-by-ref
                // word is `ByVal(0)` (== `Datum::null()`) and a live by-reference
                // image is a `ByRef`, so this value compare is the faithful
                // non-NULL test and never calls the by-value-only `as_usize()` on
                // a `ByRef` (which would panic).
            }
            skey.sk_argument = Datum::null();
            // memcpy(&skey->sk_flags, datumshared, sizeof(int)); datumshared += 4.
            // SAFETY: datumshared in the DSM datum region.
            unsafe {
                skey.sk_flags = ::core::ptr::read_unaligned(datumshared as *const i32);
                datumshared = datumshared.add(::core::mem::size_of::<i32>());
            }
            debug_assert!((skey.sk_flags & SK_BT_SKIP) != 0);
        }

        if (so.keyData[scan_key].sk_flags & (SK_BT_MINVAL | SK_BT_MAXVAL)) != 0 {
            // No sk_argument datum to restore.
            continue;
        }

        // `datum_restore` is the transitional bare-word seam (no `*_v` form
        // exists yet); wrap its scalar word into the canonical by-value arm.
        let (val, isnull, adv) = datumser::datum_restore::call(datumshared);
        datumshared = adv;
        so.keyData[scan_key].sk_argument = Datum::ByVal(val.as_usize());
        if isnull {
            debug_assert!(so.keyData[scan_key].sk_argument == Datum::null());
            debug_assert!((so.keyData[scan_key].sk_flags & SK_SEARCHNULL) != 0);
            debug_assert!((so.keyData[scan_key].sk_flags & SK_ISNULL) != 0);
        }
    }
}

/// `btinitparallelscan` — initialize `BTParallelScanDesc` for parallel scan.
pub fn btinitparallelscan(target_handle: u64) {
    with_btscan(target_handle, |bt_target| {
        lwlock::lwlock_initialize::call(
            &mut bt_target.btps_lock,
            types_storage::storage::LWTRANCHE_PARALLEL_BTREE_SCAN,
        );
        bt_target.btps_nextScanPage = types_core::primitive::InvalidBlockNumber;
        bt_target.btps_lastCurrPage = types_core::primitive::InvalidBlockNumber;
        bt_target.btps_pageStatus = BTPS_State::BTPARALLEL_NOT_INITIALIZED;
        condvar::condition_variable_init::call(&mut bt_target.btps_cv);
    });
}

/// `btparallelrescan()` — reset parallel scan.
pub fn btparallelrescan(scan: &mut NbtScan) -> PgResult<()> {
    let parallel_handle = scan.parallel_scan.expect("btparallelrescan: parallel_scan");
    // In theory we don't need the LWLock here (no other workers yet), but take
    // it for consistency.
    with_btscan(parallel_handle, |btscan| -> PgResult<()> {
        let guard = acquire_btps_lock(btscan)?;
        btscan.btps_nextScanPage = types_core::primitive::InvalidBlockNumber;
        btscan.btps_lastCurrPage = types_core::primitive::InvalidBlockNumber;
        btscan.btps_pageStatus = BTPS_State::BTPARALLEL_NOT_INITIALIZED;
        guard.release()
    })
}

// ===========================================================================
// _bt_parallel_seize / _release / _done / _primscan_schedule
// ===========================================================================

/// `MyProcNumber` — the caller's per-backend proc number, read through the
/// init-small seam (passed explicitly to the LWLock seam per the no-ambient
/// rule).
fn my_proc_number() -> types_core::ProcNumber {
    backend_utils_init_small_seams::my_proc_number::call()
}

/// `LWLockAcquire(&btscan->btps_lock, LW_EXCLUSIVE)` — the parallel-scan lock
/// guards a field disjoint from the rest of `BTParallelScanDescData`, but a
/// borrow of `&btscan.btps_lock` would (per Rust's whole-struct view) block the
/// mutations the critical section performs. The guard is taken over a raw
/// reborrow of the lock field so it does not freeze `btscan`; the lock object
/// outlives the guard (it lives in the DSM struct), so this is sound and the
/// `Drop` backstop still fires.
fn acquire_btps_lock(
    btscan: &BTParallelScanDescData,
) -> PgResult<lwlock::LWLockGuard<'static>> {
    let lock_ptr: *const types_storage::storage::LWLock = &btscan.btps_lock;
    // SAFETY: the lock lives in the DSM struct for the whole scan; the guard's
    // synthetic 'static borrow never outlives `btscan` in practice (every call
    // site releases it before the closure returns).
    let lock: &'static types_storage::storage::LWLock = unsafe { &*lock_ptr };
    lwlock::lwlock_acquire::call(lock, LWLockMode::LW_EXCLUSIVE, my_proc_number())
}

/// `_bt_parallel_seize()` — begin advancing the parallel scan to a new page.
/// AM-facing wrapper over the projected core (`scan->indexRelation`,
/// `scan->opaque`, `scan->parallel_scan`).
pub fn _bt_parallel_seize(scan: &mut NbtScan, first: bool) -> PgResult<(bool, BlockNumber, BlockNumber)> {
    let rel = scan.indexRelation.alias();
    let parallel_handle = scan.parallel_scan.expect("_bt_parallel_seize: parallel_scan");
    bt_parallel_seize_core(&rel, &mut scan.opaque, parallel_handle, first)
}

/// Projected `_bt_parallel_seize` core over `(rel, so, parallel_handle)`.
/// Returns `(status, next_scan_page, last_curr_page)`.
fn bt_parallel_seize_core<'mcx>(
    _rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    parallel_handle: u64,
    first: bool,
) -> PgResult<(bool, BlockNumber, BlockNumber)> {
    let mut next_scan_page = types_core::primitive::InvalidBlockNumber;
    let mut last_curr_page = types_core::primitive::InvalidBlockNumber;

    // Reset so->currPos, and initialize moreLeft/moreRight.
    BTScanPosInvalidate(&mut so.currPos);
    so.currPos.moreLeft = true;
    so.currPos.moreRight = true;

    if first {
        // Initialize array state when called from _bt_first.
        so.needPrimScan = false;
        so.scanBehind = false;
        so.oppositeDirCheck = false;
    } else {
        // Don't seize when another primitive index scan is required.
        if so.needPrimScan {
            return Ok((false, next_scan_page, last_curr_page));
        }
    }

    let mut status = true;
    let mut endscan = false;

    with_btscan(parallel_handle, |btscan| -> PgResult<()> {
        let mut exit_loop = false;
        loop {
            let guard = acquire_btps_lock(btscan)?;

            if btscan.btps_pageStatus == BTPS_State::BTPARALLEL_DONE {
                // We're done with this parallel index scan.
                status = false;
            } else if btscan.btps_pageStatus == BTPS_State::BTPARALLEL_IDLE
                && btscan.btps_nextScanPage == P_NONE
            {
                // End this parallel index scan.
                status = false;
                endscan = true;
            } else if btscan.btps_pageStatus == BTPS_State::BTPARALLEL_NEED_PRIMSCAN {
                debug_assert!(so.numArrayKeys != 0);

                if first {
                    // Can start scheduled primitive scan right away.
                    btscan.btps_pageStatus = BTPS_State::BTPARALLEL_ADVANCING;
                    // Restore scan's array keys from serialized values.
                    _bt_parallel_restore_arrays(btscan, so);
                    exit_loop = true;
                } else {
                    // Can't start it right now.
                    status = false;
                }

                // Either way, mark that a pending primitive scan is required.
                so.needPrimScan = true;
                so.scanBehind = false;
                so.oppositeDirCheck = false;
            } else if btscan.btps_pageStatus != BTPS_State::BTPARALLEL_ADVANCING {
                // We have seized control of the scan to advance it.
                btscan.btps_pageStatus = BTPS_State::BTPARALLEL_ADVANCING;
                debug_assert!(btscan.btps_nextScanPage != P_NONE);
                next_scan_page = btscan.btps_nextScanPage;
                last_curr_page = btscan.btps_lastCurrPage;
                exit_loop = true;
            }
            guard.release()?;
            if exit_loop || !status {
                break;
            }
            condvar::condition_variable_sleep::call(&btscan.btps_cv, WAIT_EVENT_BTREE_PAGE)?;
        }
        condvar::condition_variable_cancel_sleep::call();
        Ok(())
    })?;

    // When the scan has reached the rightmost (or leftmost) page, end it.
    if endscan {
        bt_parallel_done_core(so, Some(parallel_handle))?;
    }

    Ok((status, next_scan_page, last_curr_page))
}

/// `_bt_parallel_release()` — publish the new `btps_nextScanPage`. AM-facing
/// wrapper over the projected core.
pub fn _bt_parallel_release(
    scan: &mut NbtScan,
    next_scan_page: BlockNumber,
    curr_page: BlockNumber,
) -> PgResult<()> {
    let parallel_handle = scan.parallel_scan.expect("_bt_parallel_release: parallel_scan");
    bt_parallel_release_core(&mut scan.opaque, parallel_handle, next_scan_page, curr_page)
}

/// Projected `_bt_parallel_release` core.
fn bt_parallel_release_core<'mcx>(
    _so: &mut BTScanOpaqueData<'mcx>,
    parallel_handle: u64,
    next_scan_page: BlockNumber,
    curr_page: BlockNumber,
) -> PgResult<()> {
    debug_assert!(next_scan_page != types_core::primitive::InvalidBlockNumber);

    with_btscan(parallel_handle, |btscan| -> PgResult<()> {
        let guard = acquire_btps_lock(btscan)?;
        btscan.btps_nextScanPage = next_scan_page;
        btscan.btps_lastCurrPage = curr_page;
        btscan.btps_pageStatus = BTPS_State::BTPARALLEL_IDLE;
        guard.release()?;
        condvar::condition_variable_signal::call(&btscan.btps_cv);
        Ok(())
    })
}

/// `_bt_parallel_done()` — mark the parallel scan as complete. For non-parallel
/// scans the in-crate guard short-circuits without touching the resolver.
pub fn _bt_parallel_done(scan: &mut NbtScan) -> PgResult<()> {
    bt_parallel_done_core(&mut scan.opaque, scan.parallel_scan)
}

/// Projected `_bt_parallel_done` core. `parallel_handle == None` is the C
/// non-parallel-scan short-circuit (`scan->parallel_scan == NULL`).
fn bt_parallel_done_core<'mcx>(
    so: &mut BTScanOpaqueData<'mcx>,
    parallel_handle: Option<u64>,
) -> PgResult<()> {
    debug_assert!(!BTScanPosIsValid(&so.currPos));

    // Do nothing, for non-parallel scans.
    let parallel_handle = match parallel_handle {
        Some(h) => h,
        None => return Ok(()),
    };

    // Should not mark done when a primitive index scan is still pending.
    if so.needPrimScan {
        return Ok(());
    }

    with_btscan(parallel_handle, |btscan| -> PgResult<()> {
        // Mark the parallel scan as done, unless some other process did already.
        let guard = acquire_btps_lock(btscan)?;
        debug_assert!(btscan.btps_pageStatus != BTPS_State::BTPARALLEL_NEED_PRIMSCAN);
        let mut status_changed = false;
        if btscan.btps_pageStatus != BTPS_State::BTPARALLEL_DONE {
            btscan.btps_pageStatus = BTPS_State::BTPARALLEL_DONE;
            status_changed = true;
        }
        guard.release()?;

        // Wake up all the workers associated with this parallel scan.
        if status_changed {
            condvar::condition_variable_broadcast::call(&btscan.btps_cv);
        }
        Ok(())
    })
}

/// `_bt_parallel_primscan_schedule()` — schedule another primitive index scan.
/// AM-facing wrapper over the projected core.
pub fn _bt_parallel_primscan_schedule(scan: &mut NbtScan, curr_page: BlockNumber) -> PgResult<()> {
    let rel = scan.indexRelation.alias();
    let parallel_handle = scan
        .parallel_scan
        .expect("_bt_parallel_primscan_schedule: parallel_scan");
    bt_parallel_primscan_schedule_core(&rel, &mut scan.opaque, parallel_handle, curr_page)
}

/// Projected `_bt_parallel_primscan_schedule` core.
fn bt_parallel_primscan_schedule_core<'mcx>(
    _rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    parallel_handle: u64,
    curr_page: BlockNumber,
) -> PgResult<()> {
    debug_assert!(so.numArrayKeys != 0);

    with_btscan(parallel_handle, |btscan| -> PgResult<()> {
        let guard = acquire_btps_lock(btscan)?;
        if btscan.btps_lastCurrPage == curr_page
            && btscan.btps_pageStatus == BTPS_State::BTPARALLEL_IDLE
        {
            btscan.btps_nextScanPage = types_core::primitive::InvalidBlockNumber;
            btscan.btps_lastCurrPage = types_core::primitive::InvalidBlockNumber;
            btscan.btps_pageStatus = BTPS_State::BTPARALLEL_NEED_PRIMSCAN;

            // Serialize scan's current array keys.
            _bt_parallel_serialize_arrays(btscan, so);
        }
        guard.release()
    })
}

// ===========================================================================
// btbulkdelete
// ===========================================================================

/// `btbulkdelete` — bulk deletion of all index entries pointing to a set of
/// heap tuples. `has_callback` mirrors a non-NULL callback; the target tuples
/// are consulted through the `vacuum_tid_is_dead` seam carried as
/// `callback_state_handle`.
pub fn btbulkdelete<'mcx>(
    mcx: Mcx<'mcx>,
    info: &NbtVacuumInfo,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    stats: Option<IndexBulkDeleteResult>,
    has_callback: bool,
    callback_state_handle: u64,
) -> PgResult<IndexBulkDeleteResult> {
    // allocate stats if first time through, else re-use existing struct
    let mut stats = stats.unwrap_or_default();

    // Establish the vacuum cycle ID. The ENSURE stuff (owned by the seam)
    // cleans up shared memory on failure.
    let cycleid = core::bt_start_vacuum::call(rel)?;

    btvacuumscan(
        mcx,
        info,
        rel,
        heaprel,
        &mut stats,
        has_callback,
        callback_state_handle,
        cycleid,
    )?;

    // _bt_end_vacuum (the seam owns the PG_ENSURE_ERROR_CLEANUP wrapper so the
    // shmem slot is released even on error).
    core::bt_end_vacuum::call(rel);

    Ok(stats)
}

// ===========================================================================
// btvacuumcleanup
// ===========================================================================

/// `btvacuumcleanup` — post-VACUUM cleanup. `None` mirrors a NULL return (no
/// scan needed).
pub fn btvacuumcleanup<'mcx>(
    mcx: Mcx<'mcx>,
    info: &NbtVacuumInfo,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    stats: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    // No-op in ANALYZE ONLY mode
    if info.analyze_only {
        return Ok(stats);
    }

    // If btbulkdelete was called we only maintain num_delpages via
    // _bt_set_cleanup_info below. Otherwise decide whether a btvacuumscan() is
    // needed now via _bt_vacuum_needs_cleanup().
    let mut stats = match stats {
        Some(s) => s,
        None => {
            if !core::bt_vacuum_needs_cleanup::call(rel)? {
                return Ok(None);
            }
            // No leaf items will be deleted, so skip the vacuum-cycle-ID
            // pushups. num_index_tuples is an estimate for cleanup-only scans.
            let mut s = IndexBulkDeleteResult::default();
            btvacuumscan(mcx, info, rel, heaprel, &mut s, false, 0, 0)?;
            s.estimated_count = true;
            s
        }
    };

    // Maintain num_delpages value in metapage for _bt_vacuum_needs_cleanup().
    debug_assert!(stats.pages_deleted >= stats.pages_free);
    let num_delpages = stats.pages_deleted - stats.pages_free;
    core::bt_set_cleanup_info::call(rel, num_delpages)?;

    // Disbelieve any total exceeding the underlying heap's count, if accurate.
    if !info.estimated_count && stats.num_index_tuples > info.num_heap_tuples {
        stats.num_index_tuples = info.num_heap_tuples;
    }

    Ok(Some(stats))
}

// ===========================================================================
// btvacuumscan
// ===========================================================================

/// `btvacuumscan` — scan the index for VACUUMing purposes.
#[allow(unused_assignments)]
fn btvacuumscan<'mcx>(
    mcx: Mcx<'mcx>,
    info: &NbtVacuumInfo,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    stats: &mut IndexBulkDeleteResult,
    has_callback: bool,
    callback_state_handle: u64,
    cycleid: BTCycleId,
) -> PgResult<()> {
    // Reset fields tracking the whole index (avoid double-counting across
    // multiple scans in a single VACUUM). tuples_removed and
    // pages_newly_deleted track the whole command and are not reset here.
    stats.num_pages = 0;
    stats.num_index_tuples = 0.0;
    stats.pages_deleted = 0;
    stats.pages_free = 0;

    // Set up info to pass down to btvacuumpage.
    let mut vstate = BTVacState::new(mcx, cycleid);
    vstate.stats = *stats;

    // Temp context to run _bt_pagedel in (C: AllocSetContextCreate "_bt_pagedel").
    let mut pagedelcontext = MemoryContext::new("_bt_pagedel");

    // Consider applying _bt_pendingfsm_finalize optimization
    // (cleanuponly == !has_callback).
    core::bt_pendingfsm_init::call(rel, &mut vstate, !has_callback)?;

    // The outer loop iterates over all index pages except the metapage, in
    // physical order. We must visit all leaf pages, including ones added after
    // the scan starts, so re-check the relation length each time. We acquire
    // the relation-extension lock while doing so (skipped for new/temp rels).
    let needlock = !relcache::relation_is_local::call(rel);

    // It is safe to use batchmode as block_range_read_stream_cb takes no locks.
    let stream = readstream::read_stream_begin::call(rel, BTREE_METAPAGE + 1)?;

    let mut num_pages: BlockNumber = 0;
    loop {
        // Get the current relation length.
        let guard = if needlock {
            Some(lmgr::lock_relation_for_extension::call(rel)?)
        } else {
            None
        };
        num_pages = bufmgr::relation_get_number_of_blocks_in_fork::call(
            rel.rd_id,
            types_core::primitive::ForkNumber::MAIN_FORKNUM,
        )?;
        if let Some(g) = guard {
            g.release()?;
        }

        if info.report_progress {
            backend_utils_activity_small::backend_progress::pgstat_progress_update_param(
                PROGRESS_SCAN_BLOCKS_TOTAL,
                num_pages as i64,
            );
        }

        // Quit if we've scanned the whole relation.
        if readstream::read_stream_current_blocknum::call(stream) >= num_pages {
            break;
        }

        readstream::read_stream_set_last_exclusive::call(stream, num_pages);

        // Iterate over pages, then loop back to recheck relation length.
        loop {
            // call vacuum_delay_point while not holding any buffer lock
            backend_commands_vacuum_seams::vacuum_delay_point::call()?;

            let buf = readstream::read_stream_next_buffer::call(stream)?;
            if !BufferIsValid(buf) {
                break;
            }

            let current_block = btvacuumpage(
                mcx,
                info,
                rel,
                heaprel,
                &mut vstate,
                &mut pagedelcontext,
                buf,
                has_callback,
                callback_state_handle,
            )?;

            if info.report_progress {
                backend_utils_activity_small::backend_progress::pgstat_progress_update_param(
                    PROGRESS_SCAN_BLOCKS_DONE,
                    current_block as i64,
                );
            }
        }

        // Reset the read stream to use it again.
        readstream::read_stream_reset::call(stream);
    }

    readstream::read_stream_end::call(stream);

    // Set statistics num_pages field to final size of index.
    vstate.stats.num_pages = num_pages;

    // pagedelcontext is dropped here (C: MemoryContextDelete).
    drop(pagedelcontext);

    // Place now-safe deleted pages in the FSM; force upper FSM pages up to date
    // if any were placed.
    core::bt_pendingfsm_finalize::call(rel, heaprel, &mut vstate)?;
    if vstate.stats.pages_free > 0 {
        indexfsm::index_free_space_map_vacuum::call(rel)?;
    }

    *stats = vstate.stats;
    Ok(())
}

// ===========================================================================
// btvacuumpage
// ===========================================================================

/// `btvacuumpage` — VACUUM one page. Returns the BlockNumber of the scanned
/// page (not the backtracked one).
fn btvacuumpage<'mcx>(
    mcx: Mcx<'mcx>,
    info: &NbtVacuumInfo,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    vstate: &mut BTVacState<'mcx>,
    pagedelcontext: &mut MemoryContext,
    mut buf: Buffer,
    has_callback: bool,
    callback_state_handle: u64,
) -> PgResult<BlockNumber> {
    let scanblkno = bufmgr::buffer_get_block_number::call(buf);
    let mut blkno = scanblkno;

    // backtrack:
    loop {
        let mut attempt_pagedel = false;
        let mut backtrack_to = P_NONE;

        core::bt_lockbuf::call(rel, buf);
        let page = bufmgr::buffer_get_page::call(mcx, buf)?;
        // opaque == NULL when PageIsNew(page); else (btpo_flags, cycleid, next).
        let mut opaque: Option<(u16, BTCycleId, BlockNumber)> = None;
        if !core::page_is_new::call(&page) {
            core::bt_checkpage::call(rel, buf)?;
            opaque = Some(core::page_opaque::call(&page));
        }

        debug_assert!(blkno <= scanblkno);
        if blkno != scanblkno {
            // We're backtracking. The only case we want to do anything with is
            // a live leaf page having the current vacuum cycle ID.
            let bad = match opaque {
                None => true,
                Some((flags, _, _)) => !P_ISLEAF(flags) || P_ISHALFDEAD(flags),
            };
            if bad {
                debug_assert!(false);
                // ereport(LOG, ERRCODE_INDEX_CORRUPTED, ...); emitted by the
                // buffer substrate. Bail out as C does after the report.
                core::bt_relbuf::call(rel, buf);
                return Ok(scanblkno);
            }

            // The page may have been processed in an earlier call (split after
            // the scan began) or deleted by this btvacuumpage() call.
            let (flags, cycleid, _) = opaque.unwrap();
            if cycleid != vstate.cycleid || P_ISDELETED(flags) {
                core::bt_relbuf::call(rel, buf);
                return Ok(scanblkno);
            }
        }

        let recyclable = match opaque {
            None => true,
            Some(_) => core::bt_page_is_recyclable::call(&page, heaprel),
        };
        if recyclable {
            // Okay to recycle this page (leaf or internal).
            indexfsm::record_free_index_page::call(rel, blkno)?;
            vstate.stats.pages_deleted += 1;
            vstate.stats.pages_free += 1;
        } else {
            let (flags, _, _) = opaque.unwrap();
            if P_ISDELETED(flags) {
                // Already deleted page (leaf or internal). Can't recycle yet.
                vstate.stats.pages_deleted += 1;
            } else if P_ISHALFDEAD(flags) {
                // Half-dead leaf page (from interrupted VACUUM) — finish
                // deleting. _bt_pagedel() maintains both stats.
                attempt_pagedel = true;
            } else if P_ISLEAF(flags) {
                let (leaf_flags, btpo_cycleid, btpo_next) = opaque.unwrap();
                let (ad, bt) = btvacuumpage_leaf(
                    mcx,
                    rel,
                    vstate,
                    &page,
                    leaf_flags,
                    btpo_cycleid,
                    btpo_next,
                    buf,
                    scanblkno,
                    blkno,
                    has_callback,
                    callback_state_handle,
                )?;
                attempt_pagedel = ad;
                backtrack_to = bt;
            }
        }

        if attempt_pagedel {
            // Run pagedel in a temp context to avoid memory leakage.
            pagedelcontext.reset();

            // _bt_pagedel maintains the bulk delete stats; pages_newly_deleted
            // and pages_deleted are likely incremented during the call.
            debug_assert!(blkno == scanblkno);
            core::bt_pagedel::call(rel, heaprel, buf, vstate)?;
            // pagedel released the buffer, so we shouldn't.
        } else {
            core::bt_relbuf::call(rel, buf);
        }

        if backtrack_to != P_NONE {
            blkno = backtrack_to;

            // check for vacuum delay while not holding any buffer lock
            backend_commands_vacuum_seams::vacuum_delay_point::call()?;

            // We can't use _bt_getbuf() here because it applies _bt_checkpage(),
            // which barfs on an all-zero page; we want to recycle all-zero
            // pages, not fail. Also we want the nondefault buffer access
            // strategy (info->strategy is consumed by the seam).
            let _ = info;
            buf = bufmgr::read_buffer_extended::call(rel, blkno)?;
            continue; // goto backtrack
        }

        return Ok(scanblkno);
    }
}

/// The leaf-page branch of `btvacuumpage` (the `else if (P_ISLEAF(opaque))`
/// block). Returns `(attempt_pagedel, backtrack_to)`.
fn btvacuumpage_leaf<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    vstate: &mut BTVacState<'mcx>,
    page: &[u8],
    flags: u16,
    btpo_cycleid: BTCycleId,
    btpo_next: BlockNumber,
    buf: Buffer,
    scanblkno: BlockNumber,
    blkno: BlockNumber,
    has_callback: bool,
    callback_state_handle: u64,
) -> PgResult<(bool, BlockNumber)> {
    let mut deletable: PgVec<OffsetNumber> = vec_with_capacity_in(mcx, MaxIndexTuplesPerPage)?;
    let mut updatable: PgVec<BTVacuumPosting> = vec_with_capacity_in(mcx, MaxIndexTuplesPerPage)?;
    let mut nhtidsdead: i32 = 0;
    let mut nhtidslive: i32 = 0;
    let mut attempt_pagedel = false;
    let mut backtrack_to = P_NONE;

    // Trade the read lock for a cleanup lock on this leaf page. We must obtain
    // it on every leaf page over the scan, whether or not it has deletable
    // tuples. The buffer stays pinned, so the `page`/opaque already read in
    // btvacuumpage remain valid under the upgraded lock (as in C, which reuses
    // the same `page`/`opaque`).
    core::bt_upgradelockbufcleanup::call(rel, buf);

    // Check whether we must backtrack to earlier pages: a split since the scan
    // began that moved tuples onto a block we already passed over. (Must do
    // this before clearing btpo_cycleid or deleting scanblkno below.)
    if vstate.cycleid != 0
        && btpo_cycleid == vstate.cycleid
        && !P_SPLIT_END(flags)
        && !P_RIGHTMOST(btpo_next)
        && btpo_next < scanblkno
    {
        backtrack_to = btpo_next;
    }

    let minoff = P_FIRSTDATAKEY(btpo_next);
    let mut maxoff = core::page_get_max_offset_number::call(page);

    if has_callback {
        // btbulkdelete callback tells us what to delete (or update).
        let mut offnum = minoff;
        while offnum <= maxoff {
            let itup = core::page_get_item::call(mcx, page, offnum)?;

            debug_assert!(!core::tuple_is_pivot::call(&itup));
            if !core::tuple_is_posting::call(&itup) {
                // Regular tuple, standard table TID representation.
                let t_tid = core::tuple_heap_tid::call(&itup);
                if vacuum_tid_is_dead(t_tid, callback_state_handle) {
                    deletable.push(offnum);
                    nhtidsdead += 1;
                } else {
                    nhtidslive += 1;
                }
            } else {
                // Posting list tuple.
                let (vacposting, nremaining) =
                    btreevacuumposting(mcx, &itup, offnum, callback_state_handle)?;
                let nposting = core::tuple_n_posting::call(&itup);
                match vacposting {
                    None => {
                        // All TIDs remain: no delete or update required.
                        debug_assert!(nremaining == nposting);
                    }
                    Some(vp) => {
                        if nremaining > 0 {
                            // Some TIDs remain: update during _bt_delitems_vacuum().
                            debug_assert!(nremaining < nposting);
                            updatable.push(vp);
                            nhtidsdead += nposting - nremaining;
                        } else {
                            // All TIDs deleted: delete the index tuple completely.
                            debug_assert!(nremaining == 0);
                            deletable.push(offnum);
                            nhtidsdead += nposting;
                            // pfree(vacposting): vp dropped here.
                        }
                    }
                }
                nhtidslive += nremaining;
            }
            offnum += 1; // OffsetNumberNext
        }
    }

    // Apply any needed deletes/updates in a single _bt_delitems_vacuum() call.
    let ndeletable = deletable.len();
    let nupdatable = updatable.len();
    if ndeletable > 0 || nupdatable > 0 {
        debug_assert!(nhtidsdead >= (ndeletable + nupdatable) as i32);
        core::bt_delitems_vacuum::call(rel, buf, deletable, updatable)?;

        vstate.stats.tuples_removed += nhtidsdead as f64;
        // must recompute maxoff
        let page3 = bufmgr::buffer_get_page::call(mcx, buf)?;
        maxoff = core::page_get_max_offset_number::call(&page3);
        // updatable was consumed by _bt_delitems_vacuum.
    } else {
        // If the leaf split during this cycle, clear btpo_cycleid (a hint-bit
        // style update, no WAL needed) so we won't process the page again.
        debug_assert!(nhtidsdead == 0);
        if vstate.cycleid != 0 && btpo_cycleid == vstate.cycleid {
            core::page_clear_cycleid::call(buf);
            bufmgr::mark_buffer_dirty_hint::call(buf, false);
        }
    }

    // If the leaf page is now empty try to delete it (not when backtracking);
    // else count the live tuples. For cleanup-only calls count index tuples
    // directly instead of live TIDs.
    if minoff > maxoff {
        attempt_pagedel = blkno == scanblkno;
    } else if has_callback {
        vstate.stats.num_index_tuples += nhtidslive as f64;
    } else {
        vstate.stats.num_index_tuples += (maxoff - minoff + 1) as f64;
    }

    debug_assert!(!attempt_pagedel || nhtidslive == 0);
    Ok((attempt_pagedel, backtrack_to))
}

// ===========================================================================
// btreevacuumposting
// ===========================================================================

/// `btreevacuumposting` — determine TIDs still needed in a posting list.
/// Returns `(metadata, nremaining)`; `metadata` is `None` in the common case
/// where no changes are needed (avoiding an allocation).
fn btreevacuumposting<'mcx>(
    mcx: Mcx<'mcx>,
    posting: &[u8],
    updatedoffset: OffsetNumber,
    callback_state_handle: u64,
) -> PgResult<(Option<BTVacuumPosting<'mcx>>, i32)> {
    let mut live = 0;
    let nitem = core::tuple_n_posting::call(posting);
    let mut vacposting: Option<BTVacuumPosting> = None;

    for i in 0..nitem {
        let item = core::tuple_posting_tid::call(posting, i);
        if !vacuum_tid_is_dead(item, callback_state_handle) {
            // Live table TID
            live += 1;
        } else if vacposting.is_none() {
            // First dead table TID: start the replacement-tuple metadata.
            let itup = slice_in(mcx, posting)?;
            let mut deletetids = vec_with_capacity_in(mcx, 1)?;
            deletetids.push(i as u16);
            vacposting = Some(BTVacuumPosting {
                itup,
                updatedoffset,
                deletetids,
            });
        } else {
            // Second or subsequent dead table TID
            let vp = vacposting.as_mut().unwrap();
            vp.deletetids.try_reserve(1).map_err(|_| mcx.oom(2))?;
            vp.deletetids.push(i as u16);
        }
    }

    Ok((vacposting, live))
}

/// `callback(htup, callback_state)` — the bulk-delete callback that decides
/// whether a heap TID is dead. The callback lives in another subsystem
/// (`commands/vacuum.c`); a `0` handle is the cleanup-only NULL callback,
/// which is never consulted (guarded by `has_callback`).
fn vacuum_tid_is_dead(tid: ItemPointerData, callback_state_handle: u64) -> bool {
    backend_commands_vacuum_seams::vacuum_tid_is_dead::call(tid, callback_state_handle)
}

// ===========================================================================
// btcanreturn / btgettreeheight
// ===========================================================================

/// `btcanreturn()` — btrees always support index-only scans.
pub fn btcanreturn(_index: &Relation, _attno: i32) -> bool {
    true
}

/// `btgettreeheight()` — compute tree height for `btcostestimate()`.
pub fn btgettreeheight(rel: &Relation) -> PgResult<i32> {
    core::bt_getrootheight::call(rel)
}

// ===========================================================================
// bttranslatestrategy / bttranslatecmptype
// ===========================================================================

/// `bttranslatestrategy()` — btree strategy number to a CompareType.
pub fn bttranslatestrategy(strategy: StrategyNumber, _opfamily: Oid) -> CompareType {
    match strategy {
        s if s == BTLessStrategyNumber => COMPARE_LT,
        s if s == BTLessEqualStrategyNumber => COMPARE_LE,
        s if s == BTEqualStrategyNumber => COMPARE_EQ,
        s if s == BTGreaterEqualStrategyNumber => COMPARE_GE,
        s if s == BTGreaterStrategyNumber => COMPARE_GT,
        _ => COMPARE_INVALID,
    }
}

/// `bttranslatecmptype()` — CompareType to a btree strategy number.
pub fn bttranslatecmptype(cmptype: CompareType, _opfamily: Oid) -> StrategyNumber {
    match cmptype {
        COMPARE_LT => BTLessStrategyNumber,
        COMPARE_LE => BTLessEqualStrategyNumber,
        COMPARE_EQ => BTEqualStrategyNumber,
        COMPARE_GE => BTGreaterEqualStrategyNumber,
        COMPARE_GT => BTGreaterStrategyNumber,
        _ => InvalidStrategy,
    }
}

// ===========================================================================
// Page-opaque flag/state predicates (access/nbtree.h).
// ===========================================================================

/// `P_ISLEAF(opaque)`.
#[inline]
fn P_ISLEAF(flags: u16) -> bool {
    (flags & BTP_LEAF) != 0
}

/// `P_ISDELETED(opaque)`.
#[inline]
fn P_ISDELETED(flags: u16) -> bool {
    (flags & BTP_DELETED) != 0
}

/// `P_ISHALFDEAD(opaque)`.
#[inline]
fn P_ISHALFDEAD(flags: u16) -> bool {
    (flags & BTP_HALF_DEAD) != 0
}

/// `P_SPLIT_END(opaque)`.
#[inline]
fn P_SPLIT_END(flags: u16) -> bool {
    (flags & BTP_SPLIT_END) != 0
}

/// `P_RIGHTMOST(opaque)` — `opaque->btpo_next == P_NONE`.
#[inline]
fn P_RIGHTMOST(btpo_next: BlockNumber) -> bool {
    btpo_next == P_NONE
}

/// `P_FIRSTDATAKEY(opaque)` — `P_RIGHTMOST(opaque) ? P_HIKEY : P_FIRSTKEY`.
#[inline]
fn P_FIRSTDATAKEY(btpo_next: BlockNumber) -> OffsetNumber {
    if P_RIGHTMOST(btpo_next) {
        P_HIKEY
    } else {
        P_FIRSTKEY
    }
}

// ===========================================================================
// Seam installation (inward parallel seams).
// ===========================================================================

/// Install every seam in `backend-access-nbtree-nbtree-seams` (the inward
/// parallel-scan coordination entry points the sibling nbtree modules call).
pub fn init_seams() {
    backend_access_nbtree_nbtree_seams::bt_parallel_seize::set(seam_bt_parallel_seize);
    backend_access_nbtree_nbtree_seams::bt_parallel_release::set(seam_bt_parallel_release);
    backend_access_nbtree_nbtree_seams::bt_parallel_done::set(seam_bt_parallel_done);
    backend_access_nbtree_nbtree_seams::bt_parallel_primscan_schedule::set(
        seam_bt_parallel_primscan_schedule,
    );
}

fn seam_bt_parallel_seize<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    parallel_handle: u64,
    first: bool,
) -> PgResult<(bool, BlockNumber, BlockNumber)> {
    bt_parallel_seize_core(rel, so, parallel_handle, first)
}

fn seam_bt_parallel_release<'mcx>(
    so: &mut BTScanOpaqueData<'mcx>,
    parallel_handle: u64,
    next_scan_page: BlockNumber,
    curr_page: BlockNumber,
) -> PgResult<()> {
    bt_parallel_release_core(so, parallel_handle, next_scan_page, curr_page)
}

fn seam_bt_parallel_done<'mcx>(
    so: &mut BTScanOpaqueData<'mcx>,
    parallel_handle: Option<u64>,
) -> PgResult<()> {
    bt_parallel_done_core(so, parallel_handle)
}

fn seam_bt_parallel_primscan_schedule<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    parallel_handle: u64,
    curr_page: BlockNumber,
) -> PgResult<()> {
    bt_parallel_primscan_schedule_core(rel, so, parallel_handle, curr_page)
}

#[cfg(test)]
mod tests;
