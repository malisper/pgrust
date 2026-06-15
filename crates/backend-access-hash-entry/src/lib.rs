//! Idiomatic port of `src/backend/access/hash/hash.c` (PostgreSQL 18.3) — the
//! hash index access-method handler and the AM-API callback implementations —
//! plus `hashsort.c` (the build-time tuplesort driver, in [`hashsort`]).
//!
//! The cross-module hash internals owned by the `hash-core` unit (`_hash_first`
//! / `_hash_next` / `_hash_doinsert` / `_hash_kill_items` / `_hash_init` /
//! `_hash_getcachedmetap` / the buffer / overflow / util helpers) are called
//! directly (acyclic sibling dependency). The buffer-manager, tuplesort,
//! relcache, table-AM `table_index_build_scan`, plancat `estimate_rel_size`,
//! progress, interrupt, WAL-insert, and vacuum-callback substrate is reached
//! through the relevant `-seams` crates and panics loudly until those owners
//! land.
//!
//! `hashbucketcleanup` lives here (hash.c) and is reached *back* by `hash-core`
//! during `_hash_expandtable` split cleanup via the one inward seam this crate
//! owns (`hashbucketcleanup_split_cleanup`), installed by [`init_seams`].

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::vec::Vec;

use mcx::Mcx;
use types_amapi::{
    CompareType, IndexAmRoutine, IndexBuildResult, COMPARE_EQ, COMPARE_INVALID, T_IndexAmRoutine,
};
// Vtable-facing types (F2/F3): unified descriptor + erased AM-opaque carrier (A0).
use types_tableam::amapi::{
    IndexInfo, IndexUniqueCheck as AmIndexUniqueCheck, TIDBitmap as AmTIDBitmap,
};
use types_tableam::genam::{
    IndexBulkDeleteResult as AmIndexBulkDeleteResult, IndexVacuumInfo,
};
use types_tableam::relscan::{IndexScanDesc, IndexScanDescData};
use types_core::primitive::{BlockNumber, ForkNumber, OffsetNumber, Oid};
use types_core::catalog::RELPERSISTENCE_TEMP;
use types_core::INT4OID;
use types_error::PgResult;
use types_hash::hash::{HTEqualStrategyNumber, HTMaxStrategyNumber, HASHNProcs, HASHOPTIONS_PROC};
use types_hash::hashpage::{
    Bucket, HashScanPosIsValid, InvalidBucket, H_BUCKET_BEING_SPLIT, H_HAS_DEAD_TUPLES,
    H_NEEDS_SPLIT_CLEANUP, LH_BUCKET_NEEDS_SPLIT_CLEANUP, LH_BUCKET_PAGE, LH_OVERFLOW_PAGE,
    LH_PAGE_HAS_DEAD_TUPLES, MaxIndexTuplesPerPage, HASH_METAPAGE, HASH_NOLOCK, HASH_WRITE,
};
use types_scan::scankey::{ScanKeyData, StrategyNumber, InvalidStrategy};
use types_scan::sdir::ScanDirection;
use types_storage::buf::BufferAccessStrategy;
use types_storage::storage::{Buffer, BufferIsValid, InvalidBuffer};
use types_storage::buf::{BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;

use backend_access_common_indextuple_seams::index_form_tuple;
use backend_access_hash_core as core;
use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_storage_page::{
    PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageIndexMultiDelete, PageMut, PageRef,
};

pub mod hashsort;

// VACUUM parallel-option flags (commands/vacuum.h).
/// `VACUUM_OPTION_PARALLEL_BULKDEL`.
const VACUUM_OPTION_PARALLEL_BULKDEL: u8 = 1 << 0;

// `pgstat_progress_update_param` index codes (commands/progress.h).
const PROGRESS_CREATEIDX_TUPLES_TOTAL: i32 = 8;

// WAL: hash rmgr id + op codes (access/rmgrlist.h, access/hash_xlog.h).
const RM_HASH_ID: types_core::RmgrId = 12;
const XLOG_HASH_DELETE: u8 = 0x90;
const XLOG_HASH_SPLIT_CLEANUP: u8 = 0xA0;
const XLOG_HASH_UPDATE_META_PAGE: u8 = 0xB0;

// XLogRegisterBuffer flags (access/xloginsert.h): REGBUF_STANDARD = 0x08,
// REGBUF_NO_IMAGE = 0x02, REGBUF_NO_CHANGE = 0x20;
// see backend-access-hash-core/src/hashpage.rs and types-wal/src/xloginsert.rs.
const REGBUF_STANDARD: u8 = 0x08;
const REGBUF_NO_IMAGE: u8 = 0x02;
const REGBUF_NO_CHANGE: u8 = 0x20;

// SizeOf for the WAL records constructed here.
const SIZE_OF_HASH_DELETE: usize = 2; // {bool clear_dead_marking; bool is_primary_bucket_page;}
const SIZE_OF_HASH_UPDATE_META_PAGE: usize = 8; // {double ntuples;}

/// `MaxOffsetNumber` (`storage/off.h`) — the `deletable[]` array bound.
const MaxOffsetNumber: usize = (8192 - 24) / 4; // BLCKSZ-SizeOfPageHeader / sizeof(ItemIdData)
/// `FirstOffsetNumber` (`storage/off.h`).
const FirstOffsetNumber: OffsetNumber = 1;

// ===========================================================================
// IndexScanDescData view (HashScan, from hash-core) + IndexVacuumInfo subset.
// ===========================================================================

pub use core::HashScan;

/// `IndexVacuumInfo` (`access/genam.h`) — the subset `hashbulkdelete` /
/// `hashvacuumcleanup` read directly. The vacuum buffer-access ring crosses as
/// the `BufferAccessStrategy` handle (`None` is the C `NULL`).
///
/// Not `Copy`: `strategy` is the backend-private ring handed out by pointer
/// (`Rc<RefCell<_>>` / `None`), which is not `Copy` — mirroring C's
/// `BufferAccessStrategy` pointer.
#[derive(Clone, Debug, Default)]
pub struct HashVacuumInfo {
    /// `analyze_only` — used only via `stats == None` handling in cleanup.
    pub analyze_only: bool,
    /// `BufferAccessStrategy strategy`.
    pub strategy: BufferAccessStrategy,
}

// ===========================================================================
// hashhandler
// ===========================================================================

/// The non-pure AM callbacks of hash, named so a caller that cannot install
/// the raw fmgr-pointer ABI can reach them by name (mirrors nbtree's
/// `BT_AM_CALLBACKS`).
pub const HASH_AM_CALLBACKS: &[&str] = &[
    "hashbuild",
    "hashbuildempty",
    "hashinsert",
    "hashbulkdelete",
    "hashvacuumcleanup",
    "hashcostestimate",
    "hashoptions",
    "hashvalidate",
    "hashadjustmembers",
    "hashbeginscan",
    "hashrescan",
    "hashgettuple",
    "hashgetbitmap",
    "hashendscan",
];

/// `hashhandler()` — return [`IndexAmRoutine`] with AM parameters and
/// callbacks.
pub fn hashhandler() -> IndexAmRoutine {
    IndexAmRoutine {
        type_: T_IndexAmRoutine,
        amstrategies: HTMaxStrategyNumber,
        amsupport: HASHNProcs,
        amoptsprocnum: HASHOPTIONS_PROC,
        amcanorder: false,
        amcanorderbyop: false,
        amcanhash: true,
        amconsistentequality: true,
        amconsistentordering: false,
        amcanbackward: true,
        amcanunique: false,
        amcanmulticol: false,
        amoptionalkey: false,
        amsearcharray: false,
        amsearchnulls: false,
        amstorage: false,
        amclusterable: false,
        ampredlocks: true,
        amcanparallel: false,
        amcanbuildparallel: false,
        amcaninclude: false,
        amusemaintenanceworkmem: false,
        amsummarizing: false,
        amparallelvacuumoptions: VACUUM_OPTION_PARALLEL_BULKDEL,
        amkeytype: INT4OID,
        amtranslatestrategy: Some(hashtranslatestrategy),
        amtranslatecmptype: Some(hashtranslatecmptype),
        // hashvalidate (hashvalidate.c) returns a soft-error result and so
        // cannot be the raw `fn(Oid) -> bool` ABI pointer; it is reached by
        // name (mirrors nbtree).
        amvalidate: None,

        // Scan / insert / vacuum callbacks (F3): thin adapters translating the
        // unified descriptor <-> hash's `HashScan` working state (downcast from
        // `scan.opaque`). Hash has no index-only scans (amcanreturn = NULL), no
        // mark/restore, and no parallel scan.
        aminsert: hashinsert_am,
        ambulkdelete: hashbulkdelete_am,
        amvacuumcleanup: hashvacuumcleanup_am,
        ambeginscan: hashbeginscan_am,
        amrescan: hashrescan_am,
        amendscan: hashendscan_am,
        aminsertcleanup: None,
        amcanreturn: None,
        amgettuple: Some(hashgettuple_am),
        amgetbitmap: Some(hashgetbitmap_am),
        ammarkpos: None,
        amrestrpos: None,
        amestimateparallelscan: None,
        aminitparallelscan: None,
        amparallelrescan: None,
    }
}

// ===========================================================================
// AM-vtable adapters (F3): unified IndexScanDescData <-> HashScan
// ===========================================================================

/// Downcast `scan.opaque` to hash's `HashScan` working state (A0 downcast).
fn hsh<'a, 'mcx>(scan: &'a mut IndexScanDescData<'mcx>) -> &'a mut core::HashScan<'mcx> {
    scan.opaque
        .as_deref_mut()
        .expect("hash scan descriptor has no opaque (not built by hashbeginscan)")
        .downcast_mut::<core::HashScan<'mcx>>()
        .expect("hash scan opaque is not a HashScan")
}

/// Sync the descriptor's IN boundary fields into the `HashScan` working state
/// before each scan callback (C: the AM reads these straight off `scan`).
fn sync_in(scan: &mut IndexScanDescData<'_>) {
    let kill = scan.kill_prior_tuple;
    let ignore_killed = scan.ignore_killed_tuples;
    let instrument = scan.instrument.is_some();
    let snap = scan.xs_snapshot.clone();
    let h = hsh(scan);
    h.kill_prior_tuple = kill;
    h.ignore_killed_tuples = ignore_killed;
    h.instrument = instrument;
    h.xs_snapshot = snap.map(std::rc::Rc::new);
}

/// `aminsert` adapter.
#[allow(clippy::too_many_arguments)]
fn hashinsert_am<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &types_rel::Relation<'mcx>,
    values: &[Datum<'mcx>],
    isnull: &[bool],
    heap_tid: &ItemPointerData,
    heap_relation: &types_rel::Relation<'mcx>,
    _check_unique: AmIndexUniqueCheck,
    _index_unchanged: bool,
    _index_info: &mut IndexInfo,
) -> PgResult<bool> {
    hashinsert(mcx, index_relation, values, isnull, *heap_tid, heap_relation)
}

/// `ambulkdelete` adapter — folds the split args onto the unified shape.
fn hashbulkdelete_am<'mcx>(
    _mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    stats: Option<AmIndexBulkDeleteResult>,
    callback_state: Option<u64>,
) -> PgResult<Option<AmIndexBulkDeleteResult>> {
    // C `info->strategy` is a `BufferAccessStrategy` (the backend-private ring
    // pointer, modeled as `Option<Rc<RefCell<_>>>`). The unified
    // `IndexVacuumInfo` carries it erased; downcast it back, cloning the handle
    // (an `Rc` bump, like aliasing C's pointer), or `None` for the C `NULL`
    // strategy.
    let strategy: BufferAccessStrategy = info
        .strategy
        .as_ref()
        .and_then(|s| s.payload.as_ref())
        .and_then(|p| p.downcast_ref::<BufferAccessStrategy>())
        .cloned()
        .unwrap_or(None);
    let hinfo = HashVacuumInfo {
        analyze_only: info.analyze_only,
        strategy,
    };
    let res = hashbulkdelete(
        &hinfo,
        &info.index,
        stats,
        callback_state.is_some(),
        callback_state.unwrap_or(0),
    )?;
    Ok(Some(res))
}

/// `amvacuumcleanup` adapter.
fn hashvacuumcleanup_am<'mcx>(
    _mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    stats: Option<AmIndexBulkDeleteResult>,
) -> PgResult<Option<AmIndexBulkDeleteResult>> {
    hashvacuumcleanup(&info.index, stats)
}

/// `ambeginscan` adapter — build the unified descriptor with `opaque` holding a
/// freshly-erased `HashScan` (the A0 erase pattern).
fn hashbeginscan_am<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &types_rel::Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDesc<'mcx>> {
    let hashscan = hashbeginscan(index_relation.alias(), nkeys, norderbys)?;
    let mut desc = relation_get_index_scan(mcx, index_relation, nkeys, norderbys)?;
    desc.opaque = Some(erase_hashscan(mcx, hashscan)?);
    Ok(desc)
}

/// `amrescan` adapter.
fn hashrescan_am<'mcx>(
    _mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    keys: &[ScanKeyData<'mcx>],
    _orderbys: &[ScanKeyData<'mcx>],
) -> PgResult<()> {
    sync_in(scan);
    let norderbys = scan.number_of_order_bys;
    let scankey = if keys.is_empty() { None } else { Some(keys) };
    hashrescan(hsh(scan), scankey, norderbys)
}

/// `amendscan` adapter.
fn hashendscan_am<'mcx>(_mcx: Mcx<'mcx>, scan: &mut IndexScanDescData<'mcx>) -> PgResult<()> {
    hashendscan(hsh(scan))
}

/// `amgettuple` adapter.
fn hashgettuple_am<'mcx>(
    _mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    direction: ScanDirection,
) -> PgResult<bool> {
    sync_in(scan);
    let found = hashgettuple(hsh(scan), direction)?;
    scan.xs_recheck = hsh(scan).xs_recheck;
    if found {
        scan.xs_heaptid = hsh(scan).xs_heaptid;
    }
    Ok(found)
}

/// `amgetbitmap` adapter.
fn hashgetbitmap_am<'mcx>(
    _mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    tbm: &mut AmTIDBitmap,
) -> PgResult<i64> {
    sync_in(scan);
    let tbm_concrete = tbm
        .payload
        .as_mut()
        .and_then(|p| p.downcast_mut::<types_tidbitmap::TIDBitmap>())
        .expect("amgetbitmap TIDBitmap payload is not a types_tidbitmap::TIDBitmap");
    hashgetbitmap(hsh(scan), tbm_concrete)
}

/// `RelationGetIndexScan(indexRelation, nkeys, norderbys)` (genam.c) — allocate
/// the generic descriptor the hash AM extends via `opaque`.
fn relation_get_index_scan<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &types_rel::Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDesc<'mcx>> {
    let mut key_data = Vec::with_capacity(nkeys as usize);
    for _ in 0..nkeys {
        key_data.push(ScanKeyData::empty());
    }
    let mut order_by_data = Vec::with_capacity(norderbys as usize);
    for _ in 0..norderbys {
        order_by_data.push(ScanKeyData::empty());
    }
    let _ = mcx;
    Ok(std::boxed::Box::new(IndexScanDescData {
        heap_relation: None,
        index_relation: index_relation.alias(),
        xs_snapshot: None,
        number_of_keys: nkeys,
        number_of_order_bys: norderbys,
        key_data,
        order_by_data,
        xs_want_itup: false,
        xs_temp_snap: false,
        kill_prior_tuple: false,
        ignore_killed_tuples: true,
        xact_started_in_recovery: false,
        opaque: None,
        instrument: None,
        xs_itup: None,
        xs_itupdesc: None,
        xs_hitup: None,
        xs_hitupdesc: None,
        xs_heaptid: ItemPointerData::default(),
        xs_heap_continue: false,
        xs_heapfetch: None,
        xs_recheck: false,
        xs_orderbyvals: std::vec::from_elem(Datum::null(), norderbys as usize),
        xs_orderbynulls: std::vec![false; norderbys as usize],
        xs_recheckorderby: false,
        parallel_scan: None,
    }))
}

/// Erase a `HashScan` into the A0 AM-opaque carrier for storage in
/// `IndexScanDescData.opaque`.
fn erase_hashscan<'mcx>(
    mcx: Mcx<'mcx>,
    hashscan: core::HashScan<'mcx>,
) -> PgResult<mcx::PgBox<'mcx, dyn types_tableam::amopaque::AmOpaque<'mcx> + 'mcx>> {
    let boxed: mcx::PgBox<'mcx, core::HashScan<'mcx>> = mcx::alloc_in(mcx, hashscan)?;
    let (ptr, alloc) = mcx::PgBox::into_raw_with_allocator(boxed);
    // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast only
    // attaches the `dyn AmOpaque` vtable (the A0 erase pattern).
    Ok(unsafe {
        mcx::PgBox::from_raw_in(
            ptr as *mut (dyn types_tableam::amopaque::AmOpaque<'mcx> + 'mcx),
            alloc,
        )
    })
}

// ===========================================================================
// hashbuild
// ===========================================================================

/// Working state for `hashbuild` and its callback (hash.c `HashBuildState`).
struct HashBuildState<'mcx> {
    /// `HSpool *spool` — `None` if not using spooling.
    spool: Option<hashsort::HSpool<'mcx>>,
    /// `double indtuples` — # tuples accepted into the index.
    indtuples: f64,
    /// `Relation heapRel` — the heap relation descriptor.
    heap_rel: types_rel::Relation<'mcx>,
}

/// `hashbuild()` — build a new hash index.
pub fn hashbuild<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &types_rel::Relation<'mcx>,
    index: &types_rel::Relation<'mcx>,
    index_info: &mut types_tableam::amapi::IndexInfo,
) -> PgResult<IndexBuildResult> {
    // We expect to be called exactly once for any index relation. If that's
    // not the case, big trouble's what we have.
    if bufmgr::relation_get_number_of_blocks_in_fork::call(index, ForkNumber::MAIN_FORKNUM)?
        != 0
    {
        return Err(types_error::PgError::error(
            "index already contains data",
        ));
    }

    // Estimate the number of rows currently present in the table.
    let (_relpages, reltuples, _allvisfrac) =
        backend_optimizer_util_plancat_seams::estimate_rel_size::call(heap)?;

    // Initialize the hash index metadata page and initial buckets.
    let num_buckets = core::_hash_init(index, reltuples, ForkNumber::MAIN_FORKNUM)?;

    // If we just insert the tuples into the index in scan order, then there
    // will be no locality of access. To prevent thrashing we sort the tuples by
    // (expected) bucket number when the initial index size exceeds
    // maintenance_work_mem, or the number of usable buffers, whichever is less.
    let mut sort_threshold: usize = (backend_utils_misc_guc_seams::maintenance_work_mem::call()
        as usize
        * 1024)
        / types_core::primitive::BLCKSZ;
    if index.rd_rel.relpersistence != RELPERSISTENCE_TEMP {
        // Min(sort_threshold, NBuffers).
        let nbuffers = backend_utils_init_small_seams::nbuffers::call() as usize;
        sort_threshold = sort_threshold.min(nbuffers);
    } else {
        // Min(sort_threshold, NLocBuffer).
        let nloc = backend_utils_init_small_seams::nloc_buffer::call() as usize;
        sort_threshold = sort_threshold.min(nloc);
    }

    let spool = if (num_buckets as usize) >= sort_threshold {
        Some(hashsort::_h_spoolinit(mcx, heap, index, num_buckets)?)
    } else {
        None
    };

    // prepare to build the index
    let mut buildstate = HashBuildState {
        spool,
        indtuples: 0.0,
        heap_rel: heap.alias(),
    };

    // do the heap scan
    let reltuples = {
        let bs = &mut buildstate;
        let index_alias = index.alias();
        backend_access_table_tableam_seams::table_index_build_scan::call(
            heap,
            index,
            index_info,
            true,
            true,
            &mut |tid: ItemPointerData,
                  values: &[Datum<'mcx>],
                  isnull: &[bool],
                  _tuple_is_alive: bool|
                  -> PgResult<()> {
                hashbuildCallback(mcx, &index_alias, tid, values, isnull, bs)
            },
        )?
    };

    backend_utils_activity_small::backend_progress::pgstat_progress_update_param(
        PROGRESS_CREATEIDX_TUPLES_TOTAL,
        buildstate.indtuples as i64,
    );

    if let Some(mut spool) = buildstate.spool.take() {
        // sort the tuples and insert them into the index
        hashsort::_h_indexbuild(&mut spool, &buildstate.heap_rel)?;
        hashsort::_h_spooldestroy(spool)?;
    }

    // Return statistics.
    Ok(IndexBuildResult {
        heap_tuples: reltuples,
        index_tuples: buildstate.indtuples,
    })
}

/// `hashbuildempty()` — build an empty hash index in the initialization fork.
pub fn hashbuildempty(index: &types_rel::Relation) -> PgResult<()> {
    core::_hash_init(index, 0.0, ForkNumber::INIT_FORKNUM)?;
    Ok(())
}

/// `hashbuildCallback()` — per-tuple callback for `table_index_build_scan`.
fn hashbuildCallback<'mcx>(
    mcx: Mcx<'mcx>,
    index: &types_rel::Relation<'mcx>,
    tid: ItemPointerData,
    values: &[Datum<'mcx>],
    isnull: &[bool],
    buildstate: &mut HashBuildState<'mcx>,
) -> PgResult<()> {
    let mut index_values: [Datum<'mcx>; 1] = [Datum::null()];
    let mut index_isnull: [bool; 1] = [false];

    // convert data to a hash key; on failure, do not insert anything
    if !core::_hash_convert_tuple(index, values, isnull, &mut index_values, &mut index_isnull)? {
        return Ok(());
    }

    // Either spool the tuple for sorting, or just put it into the index.
    if let Some(spool) = buildstate.spool.as_mut() {
        hashsort::_h_spool(spool, tid, &index_values, &index_isnull)?;
    } else {
        // form an index tuple and point it at the heap tuple
        let itup = index_form_tuple::call(mcx, index, &index_values, &index_isnull, tid)?;
        core::_hash_doinsert(index, &itup, &buildstate.heap_rel, false)?;
        // pfree(itup): itup is dropped here.
    }

    buildstate.indtuples += 1.0;
    Ok(())
}

// ===========================================================================
// hashinsert
// ===========================================================================

/// `hashinsert()` — insert an index tuple into a hash table. Always returns
/// `false` (the C return value, which hash does not use for uniqueness).
pub fn hashinsert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    values: &[Datum<'mcx>],
    isnull: &[bool],
    ht_ctid: ItemPointerData,
    heap_rel: &types_rel::Relation<'mcx>,
) -> PgResult<bool> {
    let mut index_values: [Datum<'mcx>; 1] = [Datum::null()];
    let mut index_isnull: [bool; 1] = [false];

    // convert data to a hash key; on failure, do not insert anything
    if !core::_hash_convert_tuple(rel, values, isnull, &mut index_values, &mut index_isnull)? {
        return Ok(false);
    }

    // form an index tuple and point it at the heap tuple
    let itup = index_form_tuple::call(mcx, rel, &index_values, &index_isnull, ht_ctid)?;

    core::_hash_doinsert(rel, &itup, heap_rel, false)?;

    // pfree(itup): itup is dropped here.
    Ok(false)
}

// ===========================================================================
// hashgettuple
// ===========================================================================

/// `hashgettuple()` — get the next tuple in the scan.
pub fn hashgettuple<'mcx>(scan: &mut HashScan<'mcx>, dir: ScanDirection) -> PgResult<bool> {
    // Hash indexes are always lossy since we store only the hash code.
    scan.xs_recheck = true;

    // If we've already initialized this scan, advance it; otherwise call a
    // routine to get the first item in the scan.
    let res = if !HashScanPosIsValid(&scan.opaque.currPos) {
        core::_hash_first(scan, dir)?
    } else {
        // Check to see if we should kill the previously-fetched tuple.
        if scan.kill_prior_tuple {
            // Remember it for later. We'll deal with all such tuples at once
            // right after leaving the index page or at end of scan. Reversing
            // direction can re-enter the same item; we just forget any excess.
            if scan.opaque.killedItems.is_empty() {
                // palloc(MaxIndexTuplesPerPage * sizeof(int)).
                let mut v: Vec<i32> = Vec::new();
                v.resize(MaxIndexTuplesPerPage, 0i32);
                scan.opaque.killedItems = v;
            }

            if (scan.opaque.numKilled as usize) < MaxIndexTuplesPerPage {
                let n = scan.opaque.numKilled as usize;
                scan.opaque.killedItems[n] = scan.opaque.currPos.itemIndex;
                scan.opaque.numKilled += 1;
            }
        }

        // Now continue the scan.
        core::_hash_next(scan, dir)?
    };

    Ok(res)
}

// ===========================================================================
// hashgetbitmap
// ===========================================================================

/// `hashgetbitmap()` — get all matching tuples at once into a TIDBitmap;
/// returns the number of TIDs added.
pub fn hashgetbitmap<'mcx>(
    scan: &mut HashScan<'mcx>,
    tbm: &mut types_tidbitmap::TIDBitmap,
) -> PgResult<i64> {
    let mut ntids: i64 = 0;

    let mut res = core::_hash_first(scan, ScanDirection::ForwardScanDirection)?;

    while res {
        let idx = scan.opaque.currPos.itemIndex as usize;
        let heap_tid = scan.opaque.currPos.items[idx].heapTid;

        // _hash_first and _hash_next eliminate dead index entries whenever
        // scan->ignore_killed_tuples is true; nothing to do here except add the
        // results to the TIDBitmap.
        backend_nodes_core_seams::tbm_add_tuples::call(&mut *tbm, &[heap_tid], true)?;
        ntids += 1;

        res = core::_hash_next(scan, ScanDirection::ForwardScanDirection)?;
    }

    Ok(ntids)
}

// ===========================================================================
// hashbeginscan
// ===========================================================================

/// `hashbeginscan()` — start a scan on a hash index.
pub fn hashbeginscan<'mcx>(
    rel: types_rel::Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<HashScan<'mcx>> {
    // no order by operators allowed
    debug_assert!(norderbys == 0);

    let mut so = types_hash::hashpage::HashScanOpaqueData::default();
    types_hash::hashpage::HashScanPosInvalidate(&mut so.currPos);
    so.hashso_bucket_buf = InvalidBuffer;
    so.hashso_split_bucket_buf = InvalidBuffer;

    so.hashso_buc_populated = false;
    so.hashso_buc_split = false;

    so.numKilled = 0;
    // so.killedItems stays empty until the first kill (C: NULL).

    Ok(HashScan {
        indexRelation: rel,
        opaque: so,
        xs_recheck: false,
        kill_prior_tuple: false,
        xs_heaptid: ItemPointerData::default(),
        numberOfKeys: nkeys,
        keyData: Vec::new(),
        xs_snapshot: None,
        ignore_killed_tuples: false,
        instrument: false,
        nsearches: 0,
    })
}

// ===========================================================================
// hashrescan
// ===========================================================================

/// `hashrescan()` — rescan an index relation. `scankey == None` (the C NULL
/// `scankey`) leaves the keys unchanged.
pub fn hashrescan<'mcx>(
    scan: &mut HashScan<'mcx>,
    scankey: Option<&[ScanKeyData<'mcx>]>,
    _norderbys: i32,
) -> PgResult<()> {
    let rel = scan.indexRelation.alias();

    if HashScanPosIsValid(&scan.opaque.currPos) {
        // Before leaving current page, deal with any killed items.
        if scan.opaque.numKilled > 0 {
            core::_hash_kill_items(scan)?;
        }
    }

    core::_hash_dropscanbuf(&rel, &mut scan.opaque);

    // set position invalid (this will cause a _hash_first call)
    types_hash::hashpage::HashScanPosInvalidate(&mut scan.opaque.currPos);

    // Update scan key, if a new one is given.
    if let Some(sk) = scankey {
        if scan.numberOfKeys > 0 {
            let n = scan.numberOfKeys as usize;
            // memcpy(scan->keyData, scankey, numberOfKeys * sizeof(ScanKeyData))
            scan.keyData.clear();
            scan.keyData.extend_from_slice(&sk[..n]);
        }
    }

    scan.opaque.hashso_buc_populated = false;
    scan.opaque.hashso_buc_split = false;
    Ok(())
}

// ===========================================================================
// hashendscan
// ===========================================================================

/// `hashendscan()` — close down a scan.
pub fn hashendscan(scan: &mut HashScan) -> PgResult<()> {
    let rel = scan.indexRelation.alias();

    if HashScanPosIsValid(&scan.opaque.currPos) {
        // Before leaving current page, deal with any killed items.
        if scan.opaque.numKilled > 0 {
            core::_hash_kill_items(scan)?;
        }
    }

    core::_hash_dropscanbuf(&rel, &mut scan.opaque);

    // pfree(so->killedItems) / pfree(so): the owned storage drops with `scan`.
    Ok(())
}

// ===========================================================================
// hashbulkdelete
// ===========================================================================

/// `hashbulkdelete()` — bulk deletion of all index entries pointing to a set of
/// heap tuples (also deletes tuples moved by split). `has_callback` mirrors a
/// non-NULL callback; the target tuples are consulted through the
/// `vacuum_tid_is_dead` seam keyed by `callback_state_handle`.
pub fn hashbulkdelete<'mcx>(
    info: &HashVacuumInfo,
    rel: &types_rel::Relation<'mcx>,
    stats: Option<types_tableam::genam::IndexBulkDeleteResult>,
    has_callback: bool,
    callback_state_handle: u64,
) -> PgResult<types_tableam::genam::IndexBulkDeleteResult> {
    let mut tuples_removed: f64 = 0.0;
    let mut num_index_tuples: f64 = 0.0;

    let mut metabuf: Buffer = InvalidBuffer;

    // We need a copy of the metapage so that we can use its hashm_spares[]
    // values to compute bucket page addresses; a cached copy should be good
    // enough (refreshed below if a concurrent split is detected).
    let mut cachedmetap = core::_hash_getcachedmetap(rel, &mut metabuf, false)?;

    let orig_maxbucket = cachedmetap.hashm_maxbucket;
    let orig_ntuples = cachedmetap.hashm_ntuples;

    // Scan the buckets that we know exist.
    let mut cur_bucket: Bucket = 0;
    let mut cur_maxbucket: Bucket = orig_maxbucket;

    // loop_top:
    loop {
        while cur_bucket <= cur_maxbucket {
            // Get address of bucket's start page.
            let bucket_blkno = core::bucket_to_blkno(&cachedmetap, cur_bucket);
            let blkno = bucket_blkno;
            let mut split_cleanup = false;

            // Acquire a cleanup lock on the primary bucket page to out-wait
            // concurrent scans before deleting the dead tuples.
            let buf =
                bufmgr::read_buffer_with_strategy::call(rel, blkno, info.strategy.clone())?;
            bufmgr::lock_buffer_for_cleanup::call(buf)?;
            core::_hash_checkpage(rel, buf, LH_BUCKET_PAGE as i32)?;

            let (bucket_flag, bucket_prevblkno) = with_page_ref(buf, |p| {
                let bytes = p.as_bytes();
                Ok((hasho_flag(bytes), hasho_prevblkno(bytes)))
            })?;

            // If the bucket contains tuples moved by split, we need to delete
            // such tuples (only once the split is finished).
            if !H_BUCKET_BEING_SPLIT(bucket_flag) && H_NEEDS_SPLIT_CLEANUP(bucket_flag) {
                split_cleanup = true;

                // This bucket might have been split since we last held the
                // metapage lock. Now that the primary page is locked (and thus
                // can't be split further), refresh the cached metapage if its
                // mapping data is too old to remove tuples left by the most
                // recent split.
                debug_assert!(bucket_prevblkno != types_core::primitive::InvalidBlockNumber);
                if bucket_prevblkno > cachedmetap.hashm_maxbucket {
                    cachedmetap = core::_hash_getcachedmetap(rel, &mut metabuf, true)?;
                }
            }

            let bucket_buf = buf;

            hashbucketcleanup(
                rel,
                cur_bucket,
                bucket_buf,
                blkno,
                Some(info.strategy.clone()),
                cachedmetap.hashm_maxbucket,
                cachedmetap.hashm_highmask,
                cachedmetap.hashm_lowmask,
                Some(&mut tuples_removed),
                Some(&mut num_index_tuples),
                split_cleanup,
                has_callback,
                callback_state_handle,
            )?;

            core::_hash_dropbuf(rel, bucket_buf);

            // Advance to next bucket.
            cur_bucket += 1;
        }

        if !BufferIsValid(metabuf) {
            metabuf = core::_hash_getbuf(rel, HASH_METAPAGE, HASH_NOLOCK, types_hash::hashpage::LH_META_PAGE as i32)?;
        }

        // Write-lock metapage and check for a split since we started.
        bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_EXCLUSIVE)?;
        let (metap_maxbucket, _) = core::metap_maxbucket_ntuples(metabuf)?;

        if cur_maxbucket != metap_maxbucket {
            // There's been a split, so process the additional bucket(s).
            bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_UNLOCK)?;
            cachedmetap = core::_hash_getcachedmetap(rel, &mut metabuf, true)?;
            cur_maxbucket = cachedmetap.hashm_maxbucket;
            continue; // goto loop_top
        }

        break;
    }

    // Okay, we're really done. Update the tuple count in the metapage.
    // START_CRIT_SECTION
    let (metap_maxbucket, metap_ntuples) = core::metap_maxbucket_ntuples(metabuf)?;
    let new_ntuples;
    if orig_maxbucket == metap_maxbucket && orig_ntuples == metap_ntuples {
        // No one has split or inserted anything since start of scan, so believe
        // our count as gospel.
        new_ntuples = num_index_tuples;
    } else {
        // Otherwise, our count is untrustworthy since we may have
        // double-scanned tuples in split buckets. Proceed by dead-reckoning.
        new_ntuples = if metap_ntuples > tuples_removed {
            metap_ntuples - tuples_removed
        } else {
            0.0
        };
        num_index_tuples = new_ntuples;
    }
    core::set_metap_ntuples(metabuf, new_ntuples)?;

    bufmgr::mark_buffer_dirty::call(metabuf);

    // XLOG stuff
    if backend_utils_cache_relcache_seams::relation_needs_wal::call(rel) {
        // xl_hash_update_meta_page { double ntuples; }
        let mut xlrec = [0u8; SIZE_OF_HASH_UPDATE_META_PAGE];
        xlrec.copy_from_slice(&new_ntuples.to_ne_bytes());

        backend_access_transam_xloginsert_seams::xlog_begin_insert::call()?;
        backend_access_transam_xloginsert_seams::xlog_register_data::call(&xlrec)?;
        backend_access_transam_xloginsert_seams::xlog_register_buffer::call(
            0, metabuf, REGBUF_STANDARD,
        )?;

        let recptr = backend_access_transam_xloginsert_seams::xlog_insert_record::call(
            RM_HASH_ID,
            XLOG_HASH_UPDATE_META_PAGE,
        )?;
        bufmgr::page_set_lsn::call(metabuf, recptr)?;
    }
    // END_CRIT_SECTION

    core::_hash_relbuf(rel, metabuf);

    // return statistics
    let mut stats = stats.unwrap_or_default();
    stats.estimated_count = false;
    stats.num_index_tuples = num_index_tuples;
    stats.tuples_removed += tuples_removed;
    // hashvacuumcleanup will fill in num_pages.

    Ok(stats)
}

// ===========================================================================
// hashvacuumcleanup
// ===========================================================================

/// `hashvacuumcleanup()` — post-VACUUM cleanup. `None` mirrors a NULL return
/// (no change; covers the analyze-only case too).
pub fn hashvacuumcleanup<'mcx>(
    rel: &types_rel::Relation<'mcx>,
    stats: Option<types_tableam::genam::IndexBulkDeleteResult>,
) -> PgResult<Option<types_tableam::genam::IndexBulkDeleteResult>> {
    // If hashbulkdelete wasn't called, return NULL signifying no change.
    let mut stats = match stats {
        Some(s) => s,
        None => return Ok(None),
    };

    // update statistics
    let num_pages =
        bufmgr::relation_get_number_of_blocks_in_fork::call(rel, ForkNumber::MAIN_FORKNUM)?;
    stats.num_pages = num_pages;

    Ok(Some(stats))
}

// ===========================================================================
// hashbucketcleanup
// ===========================================================================

/// `hashbucketcleanup()` — perform deletion of index entries from a bucket.
///
/// The caller must hold a cleanup lock on the primary bucket page; this returns
/// with a write lock held again on the primary bucket page (not necessarily
/// continuously — released while visiting overflow pages). A pin is retained on
/// the primary bucket to ensure no concurrent split can start.
///
/// `bstrategy == None` is the C `NULL` strategy (the split-cleanup call from
/// `_hash_expandtable`). The bulk-delete out-params (`tuples_removed`,
/// `num_index_tuples`) are `None` for the split-cleanup call; `has_callback`
/// mirrors a non-NULL `IndexBulkDeleteCallback`.
pub fn hashbucketcleanup<'mcx>(
    rel: &types_rel::Relation<'mcx>,
    cur_bucket: Bucket,
    bucket_buf: Buffer,
    bucket_blkno: BlockNumber,
    bstrategy: Option<BufferAccessStrategy>,
    maxbucket: u32,
    highmask: u32,
    lowmask: u32,
    mut tuples_removed: Option<&mut f64>,
    mut num_index_tuples: Option<&mut f64>,
    split_cleanup: bool,
    has_callback: bool,
    callback_state_handle: u64,
) -> PgResult<()> {
    // Resolve the optional caller-supplied ring handle to the C
    // `BufferAccessStrategy` (the by-pointer ring / `None`); it is read at the
    // two `_hash_getbuf_with_strategy` / `_hash_squeezebucket` call sites by
    // reference (cloning the `Rc` at the seam, like aliasing C's pointer).
    let bstrategy_ring: BufferAccessStrategy = bstrategy.unwrap_or(None);

    let mut blkno;
    let mut buf;
    let mut new_bucket: Bucket = InvalidBucket;
    let mut bucket_dirty = false;

    blkno = bucket_blkno;
    buf = bucket_buf;

    if split_cleanup {
        new_bucket =
            core::_hash_get_newbucket_from_oldbucket(rel, cur_bucket, lowmask, maxbucket)?;
    }

    // Scan each page in the bucket.
    loop {
        let mut deletable: Vec<OffsetNumber> = Vec::with_capacity(MaxOffsetNumber);
        let mut clear_dead_marking = false;

        backend_commands_vacuum_seams::vacuum_delay_point::call()?;

        // Read this page's opaque (next blkno + flag) and scan its tuples.
        let (page_nextblkno, page_flag) = with_page_ref(buf, |p| {
            let bytes = p.as_bytes();
            Ok((hasho_nextblkno(bytes), hasho_flag(bytes)))
        })?;

        // Scan each tuple in page.
        with_page_ref(buf, |page| {
            let maxoffno = PageGetMaxOffsetNumber(page);
            let mut offno = FirstOffsetNumber;
            while offno <= maxoffno {
                let item_id = PageGetItemId(page, offno)?;
                let itup = PageGetItem(page, &item_id)?;
                // htup = &itup->t_tid
                let htup = index_tuple_tid(itup);
                let mut kill_tuple = false;

                // To remove the dead tuples, rely strictly on the callback
                // function (refer btvacuumpage for the detailed reason).
                if has_callback && vacuum_tid_is_dead(htup, callback_state_handle) {
                    kill_tuple = true;
                    if let Some(tr) = tuples_removed.as_deref_mut() {
                        *tr += 1.0;
                    }
                } else if split_cleanup {
                    // delete the tuples that are moved by split.
                    let bucket = core::_hash_hashkey2bucket(
                        core::_hash_get_indextuple_hashkey(itup),
                        maxbucket,
                        highmask,
                        lowmask,
                    );
                    // mark the item for deletion
                    if bucket != cur_bucket {
                        // We expect tuples to either belong to current bucket
                        // or new_bucket (no further splits from a bucket with
                        // garbage; see _hash_expandtable).
                        debug_assert!(bucket == new_bucket);
                        kill_tuple = true;
                    }
                }

                if kill_tuple {
                    // mark the item for deletion
                    deletable.push(offno);
                } else {
                    // we're keeping it, so count it
                    if let Some(nit) = num_index_tuples.as_deref_mut() {
                        *nit += 1.0;
                    }
                }

                offno += 1; // OffsetNumberNext
            }
            Ok(())
        })?;
        let _ = new_bucket; // used only under debug_assert above

        // retain the pin on the primary bucket page till end of bucket scan
        let retain_pin = blkno == bucket_blkno;

        blkno = page_nextblkno;

        // Apply deletions, advance to next page and write page if needed.
        let ndeletable = deletable.len();
        if ndeletable > 0 {
            // No ereport(ERROR) until changes are logged.
            // START_CRIT_SECTION

            with_page_ref(buf, |_p| Ok(()))?; // (page borrow scope marker)
            bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
                let mut pmut = PageMut::new(page)?;
                PageIndexMultiDelete(&mut pmut, &deletable)?;
                Ok(())
            })?;
            bucket_dirty = true;

            // Mark the page as clean (clear LH_PAGE_HAS_DEAD_TUPLES) if vacuum
            // removed DEAD tuples from this index page.
            let tuples_removed_pos = tuples_removed
                .as_deref()
                .map(|&tr| tr > 0.0)
                .unwrap_or(false);
            if tuples_removed_pos && H_HAS_DEAD_TUPLES(page_flag) {
                bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
                    let f = hasho_flag(page) & !LH_PAGE_HAS_DEAD_TUPLES;
                    set_hasho_flag(page, f);
                    Ok(())
                })?;
                clear_dead_marking = true;
            }

            bufmgr::mark_buffer_dirty::call(buf);

            // XLOG stuff
            if backend_utils_cache_relcache_seams::relation_needs_wal::call(rel) {
                let is_primary_bucket_page = buf == bucket_buf;
                // xl_hash_delete { bool clear_dead_marking; bool
                //                  is_primary_bucket_page; }
                let mut xlrec = [0u8; SIZE_OF_HASH_DELETE];
                xlrec[0] = clear_dead_marking as u8;
                xlrec[1] = is_primary_bucket_page as u8;

                backend_access_transam_xloginsert_seams::xlog_begin_insert::call()?;
                backend_access_transam_xloginsert_seams::xlog_register_data::call(&xlrec)?;

                // The bucket buffer was not changed, but still needs to be
                // registered to ensure we can acquire a cleanup lock on it
                // during replay.
                if !is_primary_bucket_page {
                    let flags = REGBUF_STANDARD | REGBUF_NO_IMAGE | REGBUF_NO_CHANGE;
                    backend_access_transam_xloginsert_seams::xlog_register_buffer::call(
                        0, bucket_buf, flags,
                    )?;
                }

                backend_access_transam_xloginsert_seams::xlog_register_buffer::call(
                    1, buf, REGBUF_STANDARD,
                )?;
                let mut offs = Vec::with_capacity(ndeletable * 2);
                for &o in &deletable {
                    offs.extend_from_slice(&o.to_ne_bytes());
                }
                backend_access_transam_xloginsert_seams::xlog_register_buf_data::call(1, &offs)?;

                let recptr = backend_access_transam_xloginsert_seams::xlog_insert_record::call(
                    RM_HASH_ID,
                    XLOG_HASH_DELETE,
                )?;
                bufmgr::page_set_lsn::call(buf, recptr)?;
            }
            // END_CRIT_SECTION
        }

        // bail out if there are no more pages to scan.
        if !block_number_is_valid(blkno) {
            break;
        }

        let next_buf = core::_hash_getbuf_with_strategy(
            rel,
            blkno,
            HASH_WRITE,
            LH_OVERFLOW_PAGE as i32,
            &bstrategy_ring,
        )?;

        // release the lock on previous page after acquiring the lock on next.
        if retain_pin {
            bufmgr::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
        } else {
            core::_hash_relbuf(rel, buf);
        }

        buf = next_buf;
    }

    // Lock the bucket page to clear the garbage flag and squeeze the bucket. If
    // the current buffer is the same as the bucket buffer, we already hold the
    // lock on the bucket page.
    if buf != bucket_buf {
        core::_hash_relbuf(rel, buf);
        bufmgr::lock_buffer::call(bucket_buf, BUFFER_LOCK_EXCLUSIVE)?;
    }

    // Clear the garbage flag from the bucket after deleting the moved-by-split
    // tuples. Clear it before squeezing so vacuum won't again try to delete the
    // moved-by-split tuples after a restart.
    if split_cleanup {
        // START_CRIT_SECTION
        bufmgr::with_buffer_page::call(bucket_buf, &mut |page: &mut [u8]| {
            let f = hasho_flag(page) & !LH_BUCKET_NEEDS_SPLIT_CLEANUP;
            set_hasho_flag(page, f);
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(bucket_buf);

        // XLOG stuff
        if backend_utils_cache_relcache_seams::relation_needs_wal::call(rel) {
            backend_access_transam_xloginsert_seams::xlog_begin_insert::call()?;
            backend_access_transam_xloginsert_seams::xlog_register_buffer::call(
                0, bucket_buf, REGBUF_STANDARD,
            )?;

            let recptr = backend_access_transam_xloginsert_seams::xlog_insert_record::call(
                RM_HASH_ID,
                XLOG_HASH_SPLIT_CLEANUP,
            )?;
            bufmgr::page_set_lsn::call(bucket_buf, recptr)?;
        }
        // END_CRIT_SECTION
    }

    // If we deleted anything, try to compact free space. For squeezing we must
    // have a cleanup lock, else it can impact the ordering of tuples for a scan
    // that started before it.
    if bucket_dirty && bufmgr::is_buffer_cleanup_ok::call(bucket_buf)? {
        core::_hash_squeezebucket(
            rel,
            cur_bucket,
            bucket_blkno,
            bucket_buf,
            &bstrategy_ring,
        )?;
    } else {
        bufmgr::lock_buffer::call(bucket_buf, BUFFER_LOCK_UNLOCK)?;
    }

    Ok(())
}

/// `callback(htup, callback_state)` — the bulk-delete callback deciding whether
/// a heap TID is dead. The callback lives in the vacuum subsystem; reached via
/// the seam keyed by `callback_state_handle` (never consulted unless
/// `has_callback`).
fn vacuum_tid_is_dead(tid: ItemPointerData, callback_state_handle: u64) -> bool {
    backend_commands_vacuum_seams::vacuum_tid_is_dead::call(tid, callback_state_handle)
}

// ===========================================================================
// hashtranslatestrategy / hashtranslatecmptype
// ===========================================================================

/// `hashtranslatestrategy()` — hash strategy number to a CompareType.
pub fn hashtranslatestrategy(strategy: StrategyNumber, _opfamily: Oid) -> CompareType {
    if strategy == HTEqualStrategyNumber {
        return COMPARE_EQ;
    }
    COMPARE_INVALID
}

/// `hashtranslatecmptype()` — CompareType to a hash strategy number.
pub fn hashtranslatecmptype(cmptype: CompareType, _opfamily: Oid) -> StrategyNumber {
    if cmptype == COMPARE_EQ {
        return HTEqualStrategyNumber;
    }
    InvalidStrategy
}

// ===========================================================================
// Page-byte helpers (HashPageOpaque field access + IndexTuple t_tid), local
// to this crate. The on-disk layout mirrors hash-core's `pagebytes` module
// (the canonical hash page-opaque accessors); duplicated here because they are
// `pub(crate)` to hash-core. The opaque is the last `HashPageOpaqueData` at the
// page's special area; offsets are computed from BLCKSZ minus the special-area
// size, matching backend-storage-page's pd_special.
// ===========================================================================

/// Byte size of `HashPageOpaqueData` (prevblkno u32, nextblkno u32, bucket u32,
/// flag u16, page_id u16 = 16 bytes).
const SIZE_OF_HASH_PAGE_OPAQUE: usize = 16;

#[inline]
fn opaque_off(page: &[u8]) -> usize {
    page.len() - SIZE_OF_HASH_PAGE_OPAQUE
}

#[inline]
fn hasho_prevblkno(page: &[u8]) -> BlockNumber {
    let o = opaque_off(page);
    u32::from_ne_bytes([page[o], page[o + 1], page[o + 2], page[o + 3]])
}

#[inline]
fn hasho_nextblkno(page: &[u8]) -> BlockNumber {
    let o = opaque_off(page) + 4;
    u32::from_ne_bytes([page[o], page[o + 1], page[o + 2], page[o + 3]])
}

#[inline]
fn hasho_flag(page: &[u8]) -> u16 {
    let o = opaque_off(page) + 12;
    u16::from_ne_bytes([page[o], page[o + 1]])
}

#[inline]
fn set_hasho_flag(page: &mut [u8], flag: u16) {
    let o = opaque_off(page) + 12;
    page[o..o + 2].copy_from_slice(&flag.to_ne_bytes());
}

/// `&itup->t_tid` — the heap TID at the start of an `IndexTupleData`
/// (`ItemPointerData { BlockIdData ip_blkid; uint16 ip_posid; }` = 6 bytes:
/// block-hi u16, block-lo u16, posid u16).
#[inline]
fn index_tuple_tid(itup: &[u8]) -> ItemPointerData {
    let bi_hi = u16::from_ne_bytes([itup[0], itup[1]]);
    let bi_lo = u16::from_ne_bytes([itup[2], itup[3]]);
    let posid = u16::from_ne_bytes([itup[4], itup[5]]);
    ItemPointerData {
        ip_blkid: types_tuple::heaptuple::BlockIdData { bi_hi, bi_lo },
        ip_posid: posid,
    }
}

/// `BlockNumberIsValid(blkno)` (`storage/block.h`).
#[inline]
fn block_number_is_valid(blkno: BlockNumber) -> bool {
    blkno != types_core::primitive::InvalidBlockNumber
}

/// `with_page_ref` over the bufmgr seam (mirrors hash-core's private helper).
fn with_page_ref<R>(buf: Buffer, f: impl FnOnce(&PageRef<'_>) -> PgResult<R>) -> PgResult<R> {
    let mut out: Option<R> = None;
    let mut f = Some(f);
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        let pref = PageRef::new(page)?;
        out = Some((f.take().unwrap())(&pref)?);
        Ok(())
    })?;
    Ok(out.expect("with_page_ref closure ran"))
}

// ===========================================================================
// init_seams
// ===========================================================================

/// Install the one inward seam this crate owns: `hash-core` calls
/// `hashbucketcleanup` back during `_hash_expandtable` split cleanup.
pub fn init_seams() {
    backend_access_hash_entry_seams::hashbucketcleanup_split_cleanup::set(
        |rel, cur_bucket, bucket_buf, bucket_blkno, maxbucket, highmask, lowmask| {
            // hashbucketcleanup(rel, cur_bucket, bucket_buf, bucket_blkno, NULL,
            //   maxbucket, highmask, lowmask, NULL, NULL, true, NULL, NULL).
            hashbucketcleanup(
                rel,
                cur_bucket,
                bucket_buf,
                bucket_blkno,
                None,
                maxbucket,
                highmask,
                lowmask,
                None,
                None,
                true,
                false,
                0,
            )
        },
    );
}
