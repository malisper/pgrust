//! Port of `src/backend/access/index/indexam.c` — the general index
//! access-method dispatch layer (the `index_*` interface routines).
//!
//! The dispatch model mirrors `backend-access-table-tableam`: an index
//! relation's `rd_indam` is an [`types_tableam::IndexAmRoutine`] vtable whose
//! callbacks (`am*`) the per-AM implementation (nbtree / hash / gist / gin /
//! spgist / brin) installs; this layer reads the property flags
//! (`ampredlocks`, `amsupport`, `amoptsprocnum`) and invokes the callbacks
//! through the vtable, fetched per relation through the relcache owner's seam.
//! `RELATION_CHECKS` (the reindex guard) and `CHECK_REL_PROCEDURE` /
//! `CHECK_SCAN_PROCEDURE` (the missing-callback error) are this layer's logic.
//!
//! The open index/heap relation crosses as a [`types_rel::Relation`] handle.
//! `IndexScanDescData` is the generic scan descriptor (the AM extends it via
//! `opaque`); the AM allocates it in `ambeginscan`. `IndexScanEnd` is the
//! `Drop` of the owned `Box<IndexScanDescData>`. The table-AM heap-fetch
//! (`table_index_fetch_*`) is a direct dependency (`tableam.c` is ported); the
//! predicate-lock manager, snapshot manager, pgstat counters, the relcache
//! refcount + index support cache, and `ReindexIsProcessingIndex` cross seams
//! to their (as-yet-unported) owners.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use std::vec::Vec;

use mcx::Mcx;
// The canonical unified value type (Datum-unification). The tableam contracts
// this layer forwards to — the `aminsert` vtable `values: &[Datum<'_>]`, the
// `IndexScanDesc.xs_orderbyvals` slots, and the opclass-options word forwarded
// verbatim to the reloptions seam — all carry it.
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INTERNAL_ERROR,
    ERRCODE_WRONG_OBJECT_TYPE};
use types_rel::Relation;
use types_scan::sdir::ScanDirection;
use types_scan::scankey::ScanKeyData;
use types_snapshot::snapshot::{IsMVCCSnapshot, SnapshotData};
use types_storage::lock::{LOCKMODE, NoLock};
use types_tableam::amapi::{IndexAmRoutine, IndexInfo, IndexUniqueCheck, TIDBitmap};
use types_tableam::genam::{
    IndexBulkDeleteResult, IndexOrderByDistance, IndexScanInstrumentation, IndexVacuumInfo,
    SharedIndexScanInstrumentation,
};
use types_tableam::relscan::{IndexScanDesc, IndexScanDescData, ParallelIndexScanDescData};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{AttrNumber, InvalidOid, Oid, RegProcedure};
use types_tuple::access::{RELKIND_INDEX, RELKIND_PARTITIONED_INDEX};
use types_tuple::heaptuple::ItemPointerData;

use backend_access_table_tableam as tableam;
use backend_catalog_index_seams as catalog_index;
use backend_storage_lmgr_predicate_seams as predicate;
use backend_utils_activity_pgstat_seams as pgstat;
use backend_utils_cache_relcache_seams as relcache;
use backend_utils_time_snapmgr_seams as snapmgr;

/// Install this crate's seam implementations: every seam declared in
/// `backend-access-index-indexam-seams`.
pub fn init_seams() {
    backend_access_index_indexam_seams::index_open::set(index_open);
    backend_access_index_indexam_seams::index_getprocinfo::set(index_getprocinfo);
}

// ===========================================================================
// RELATION_CHECKS / SCAN_CHECKS / CHECK_*_PROCEDURE macros
// ===========================================================================

/// `RELATION_CHECKS` — the reindex guard (the C relcache-validity asserts are
/// debug-only). The reindex state is owned by catalog/index.c.
fn relation_checks(index_relation: &Relation<'_>) -> PgResult<()> {
    if catalog_index::reindex_is_processing_index::call(index_relation.rd_id) {
        return Err(PgError::error(format!(
            "cannot access index \"{}\" while it is being reindexed",
            index_relation.name()
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }
    Ok(())
}

/// `relation->rd_indam` where C dereferences it unconditionally (after the
/// `PointerIsValid` assert): a missing vtable is the C NULL-pointer crash, so
/// panic loudly.
fn indam(index_relation: &Relation<'_>) -> IndexAmRoutine {
    relcache::relation_rd_indam::call(index_relation.rd_id)
        .expect("index relation has no rd_indam (C would dereference NULL)")
}

// ===========================================================================
// index_open / try_index_open / index_close / validate_relation_kind
// ===========================================================================

/// `index_open(relationId, lockmode)` — open an index relation by OID, taking
/// `lockmode` (unless `NoLock`) and verifying it is an index.
pub fn index_open<'mcx>(
    mcx: Mcx<'mcx>,
    relation_id: Oid,
    lockmode: LOCKMODE,
) -> PgResult<Relation<'mcx>> {
    let r = backend_access_common_relation_seams::relation_open::call(mcx, relation_id, lockmode)?;
    validate_relation_kind(&r)?;
    Ok(r)
}

/// `try_index_open(relationId, lockmode)` — like [`index_open`] but returns
/// `None` instead of erroring when the relation does not exist.
pub fn try_index_open<'mcx>(
    mcx: Mcx<'mcx>,
    relation_id: Oid,
    lockmode: LOCKMODE,
) -> PgResult<Option<Relation<'mcx>>> {
    let r = backend_access_common_relation_seams::try_relation_open::call(mcx, relation_id, lockmode)?;

    // leave if index does not exist
    let Some(r) = r else {
        return Ok(None);
    };

    validate_relation_kind(&r)?;
    Ok(Some(r))
}

/// `index_close(relation, lockmode)` — close an index relation and, unless
/// `NoLock`, release the lock. The relcache refcount decrement + the
/// conditional `UnlockRelationId` are both the [`Relation::close`] of the
/// open handle.
pub fn index_close(relation: Relation<'_>, lockmode: LOCKMODE) -> PgResult<()> {
    // Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES); NoLock == 0 is
    // the floor, the upper bound a debug-only relcache invariant.
    debug_assert!(lockmode >= NoLock);
    relation.close(lockmode)
}

/// `validate_relation_kind(r)` — error unless the relation is an index or a
/// partitioned index.
fn validate_relation_kind(r: &Relation<'_>) -> PgResult<()> {
    let relkind = r.rd_rel.relkind;
    if relkind != RELKIND_INDEX && relkind != RELKIND_PARTITIONED_INDEX {
        return Err(PgError::error(format!("\"{}\" is not an index", r.name()))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE));
    }
    Ok(())
}

// ===========================================================================
// index_insert / index_insert_cleanup
// ===========================================================================

/// `index_insert(indexRelation, values, isnull, heap_t_ctid, heapRelation,
/// checkUnique, indexUnchanged, indexInfo)` — insert an index tuple.
pub fn index_insert(
    index_relation: &Relation<'_>,
    values: &[DatumV<'_>],
    isnull: &[bool],
    heap_t_ctid: &ItemPointerData,
    heap_relation: &Relation<'_>,
    check_unique: IndexUniqueCheck,
    index_unchanged: bool,
    index_info: &mut IndexInfo,
) -> PgResult<bool> {
    relation_checks(index_relation)?;
    let am = indam(index_relation);
    // CHECK_REL_PROCEDURE(aminsert): aminsert is a required (non-Option)
    // callback in the vtable.

    if !am.ampredlocks {
        // CheckForSerializableConflictIn(indexRelation, NULL, InvalidBlockNumber)
        predicate::check_for_serializable_conflict_in::call(index_relation.rd_id)?;
    }

    (am.aminsert)(
        index_relation,
        values,
        isnull,
        heap_t_ctid,
        heap_relation,
        check_unique,
        index_unchanged,
        index_info,
    )
}

/// `index_insert_cleanup(indexRelation, indexInfo)` — clean up after all index
/// inserts are done. `aminsertcleanup` is optional.
pub fn index_insert_cleanup(
    index_relation: &Relation<'_>,
    index_info: &mut IndexInfo,
) -> PgResult<()> {
    relation_checks(index_relation)?;

    if let Some(aminsertcleanup) = indam(index_relation).aminsertcleanup {
        aminsertcleanup(index_relation, index_info)?;
    }
    Ok(())
}

// ===========================================================================
// Scan lifecycle: begin / rescan / endscan / markpos / restrpos
// ===========================================================================

/// `index_beginscan(heapRelation, indexRelation, snapshot, instrument, nkeys,
/// norderbys)` — start a scan of an index with `amgettuple`.
pub fn index_beginscan<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relation: &Relation<'mcx>,
    index_relation: &Relation<'mcx>,
    snapshot: SnapshotData,
    instrument: Option<IndexScanInstrumentation>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDesc<'mcx>> {
    // Assert(snapshot != InvalidSnapshot) — modeled by SnapshotData being a
    // value, not the C NULL/Invalid pointer.

    let mut scan =
        index_beginscan_internal(index_relation, nkeys, norderbys, snapshot.clone(), None, false)?;

    // Save additional parameters into the scandesc; everything else was set up
    // by RelationGetIndexScan (inside ambeginscan).
    scan.heap_relation = Some(heap_relation.alias());
    scan.xs_snapshot = Some(snapshot);
    scan.instrument = instrument;

    // prepare to fetch index matches from table
    scan.xs_heapfetch = Some(tableam::table_index_fetch_begin(mcx, heap_relation)?);

    Ok(scan)
}

/// `index_beginscan_bitmap(indexRelation, snapshot, instrument, nkeys)` —
/// start a scan of an index with `amgetbitmap`.
pub fn index_beginscan_bitmap<'mcx>(
    index_relation: &Relation<'mcx>,
    snapshot: SnapshotData,
    instrument: Option<IndexScanInstrumentation>,
    nkeys: i32,
) -> PgResult<IndexScanDesc<'mcx>> {
    // Assert(snapshot != InvalidSnapshot).

    let mut scan =
        index_beginscan_internal(index_relation, nkeys, 0, snapshot.clone(), None, false)?;

    // scan->xs_snapshot = snapshot; scan->instrument = instrument; (no heap rel)
    scan.xs_snapshot = Some(snapshot);
    scan.instrument = instrument;

    Ok(scan)
}

/// `index_beginscan_internal` — common code for the begin variants.
fn index_beginscan_internal<'mcx>(
    index_relation: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
    snapshot: SnapshotData,
    pscan: Option<std::sync::Arc<ParallelIndexScanDescData>>,
    temp_snap: bool,
) -> PgResult<IndexScanDesc<'mcx>> {
    relation_checks(index_relation)?;
    let am = indam(index_relation);
    // CHECK_REL_PROCEDURE(ambeginscan): ambeginscan is a required callback.

    if !am.ampredlocks {
        predicate::predicate_lock_relation::call(index_relation.rd_id, &snapshot)?;
    }

    // We hold a reference count to the relcache entry throughout the scan.
    relcache::relation_increment_reference_count::call(index_relation.rd_id)?;

    // Tell the AM to open a scan.
    let mut scan = (am.ambeginscan)(index_relation, nkeys, norderbys)?;

    // Initialize information for parallel scan.
    scan.parallel_scan = pscan;
    scan.xs_temp_snap = temp_snap;

    Ok(scan)
}

/// `index_rescan(scan, keys, nkeys, orderbys, norderbys)` — (re)start a scan.
/// The key counts must equal what `index_beginscan` was told. To restart
/// without changing keys, pass empty key arrays (the C `NULL`).
pub fn index_rescan(
    scan: &mut IndexScanDescData<'_>,
    keys: &[ScanKeyData],
    nkeys: i32,
    orderbys: &[ScanKeyData],
    norderbys: i32,
) -> PgResult<()> {
    // SCAN_CHECKS + CHECK_SCAN_PROCEDURE(amrescan): amrescan is required.
    let am = indam(&scan.index_relation);

    debug_assert!(nkeys == scan.number_of_keys);
    debug_assert!(norderbys == scan.number_of_order_bys);

    // Release resources (like buffer pins) from table accesses.
    if let Some(heapfetch) = scan.xs_heapfetch.as_deref_mut() {
        tableam::table_index_fetch_reset(heapfetch)?;
    }

    scan.kill_prior_tuple = false; // for safety
    scan.xs_heap_continue = false;

    (am.amrescan)(scan, keys, orderbys)
}

/// `index_endscan(scan)` — end a scan.
pub fn index_endscan(mut scan: IndexScanDesc<'_>) -> PgResult<()> {
    // SCAN_CHECKS + CHECK_SCAN_PROCEDURE(amendscan): amendscan is required.
    let am = indam(&scan.index_relation);

    // Release resources (like buffer pins) from table accesses, then NULL the
    // heap-fetch pointer.
    if let Some(heapfetch) = scan.xs_heapfetch.take() {
        tableam::table_index_fetch_end(heapfetch)?;
    }

    // End the AM's scan.
    (am.amendscan)(&mut scan)?;
    // (note: `scan` is `Box<IndexScanDescData>`; `&mut scan` auto-derefs to
    // `&mut IndexScanDescData` via deref coercion at the call.)

    // Release index refcount acquired by index_beginscan.
    relcache::relation_decrement_reference_count::call(scan.index_relation.rd_id)?;

    if scan.xs_temp_snap {
        if let Some(snap) = scan.xs_snapshot.take() {
            snapmgr::unregister_snapshot::call(snap);
        }
    }

    // Release the scan data structure itself (IndexScanEnd): drop the box.
    drop(scan);
    Ok(())
}

/// `index_markpos(scan)` — mark a scan position.
pub fn index_markpos(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    // CHECK_SCAN_PROCEDURE(ammarkpos): optional callback, error if absent.
    let am = indam(&scan.index_relation);
    let ammarkpos = check_scan_procedure(am.ammarkpos, "ammarkpos", &scan.index_relation)?;
    ammarkpos(scan)
}

/// `index_restrpos(scan)` — restore a scan position. Only restores the index
/// AM's internal state (see C comments on HOT chains + MVCC snapshots).
pub fn index_restrpos(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    // Assert(IsMVCCSnapshot(scan->xs_snapshot)).
    debug_assert!(scan
        .xs_snapshot
        .as_ref()
        .map(IsMVCCSnapshot)
        .unwrap_or(false));

    // CHECK_SCAN_PROCEDURE(amrestrpos): optional callback, error if absent.
    let am = indam(&scan.index_relation);
    let amrestrpos = check_scan_procedure(am.amrestrpos, "amrestrpos", &scan.index_relation)?;

    // release resources (like buffer pins) from table accesses
    if let Some(heapfetch) = scan.xs_heapfetch.as_deref_mut() {
        tableam::table_index_fetch_reset(heapfetch)?;
    }

    scan.kill_prior_tuple = false; // for safety
    scan.xs_heap_continue = false;

    amrestrpos(scan)
}

// ===========================================================================
// Parallel scan
// ===========================================================================

/// `offsetof(ParallelIndexScanDescData, ps_snapshot_data)` — the fixed header
/// of `ParallelIndexScanDescData` (`{ RelFileLocator ps_locator;
/// RelFileLocator ps_indexlocator; Size ps_offset_ins; Size ps_offset_am;
/// char ps_snapshot_data[FLEXIBLE_ARRAY_MEMBER]; }`). The flexible `char`
/// array follows the two locators and two `Size`s at 1-byte alignment, so the
/// offset is simply their summed size.
#[inline]
fn parallel_index_scan_desc_header_size() -> usize {
    2 * core::mem::size_of::<types_storage::RelFileLocator>() + 2 * core::mem::size_of::<usize>()
}

/// `offsetof(SharedIndexScanInstrumentation, winstrument)` — the fixed header
/// of `SharedIndexScanInstrumentation` (`{ int num_workers;
/// IndexScanInstrumentation winstrument[FLEXIBLE_ARRAY_MEMBER]; }`), aligned
/// for the array element.
#[inline]
fn shared_index_scan_instrumentation_header_size() -> usize {
    let align = core::mem::align_of::<IndexScanInstrumentation>();
    (core::mem::size_of::<i32>() + (align - 1)) & !(align - 1)
}

/// `sizeof(IndexScanInstrumentation)`.
#[inline]
fn size_of_index_scan_instrumentation() -> usize {
    core::mem::size_of::<IndexScanInstrumentation>()
}

/// `index_parallelscan_estimate(indexRelation, nkeys, norderbys, snapshot,
/// instrument, parallel_aware, nworkers)` — estimate shared memory for a
/// parallel scan.
pub fn index_parallelscan_estimate(
    index_relation: &Relation<'_>,
    nkeys: i32,
    norderbys: i32,
    snapshot: &SnapshotData,
    instrument: bool,
    parallel_aware: bool,
    nworkers: i32,
) -> PgResult<usize> {
    debug_assert!(instrument || parallel_aware);

    relation_checks(index_relation)?;

    let mut nbytes = parallel_index_scan_desc_header_size();
    nbytes = add_size(nbytes, snapmgr::estimate_snapshot_space::call(snapshot));
    nbytes = maxalign(nbytes);

    if instrument {
        let sharedinfosz = add_size(
            shared_index_scan_instrumentation_header_size(),
            (nworkers as usize) * size_of_index_scan_instrumentation(),
        );
        nbytes = add_size(nbytes, sharedinfosz);
        nbytes = maxalign(nbytes);
    }

    // If the parallel-scan index AM interface can't be used (or the AM
    // provides no such interface), assume there is no AM-specific data needed.
    if parallel_aware {
        if let Some(amestimateparallelscan) = indam(index_relation).amestimateparallelscan {
            nbytes = add_size(
                nbytes,
                amestimateparallelscan(index_relation, nkeys, norderbys)?,
            );
        }
    }

    Ok(nbytes)
}

/// `index_parallelscan_initialize(heapRelation, indexRelation, snapshot,
/// instrument, parallel_aware, nworkers, sharedinfo, target)` — initialize the
/// `ParallelIndexScanDesc` proper and the AM-specific info following it. Call
/// once in the leader; workers then attach via [`index_beginscan_parallel`].
///
/// The leader's `*sharedinfo` in C points into `target` at `ps_offset_ins`;
/// the owned model stores the zeroed `SharedIndexScanInstrumentation` region
/// in `target.shared_instrument`.
pub fn index_parallelscan_initialize(
    heap_relation: &Relation<'_>,
    index_relation: &Relation<'_>,
    snapshot: &SnapshotData,
    instrument: bool,
    parallel_aware: bool,
    nworkers: i32,
    target: &mut ParallelIndexScanDescData,
) -> PgResult<()> {
    debug_assert!(instrument || parallel_aware);

    relation_checks(index_relation)?;

    let mut offset = add_size(
        parallel_index_scan_desc_header_size(),
        snapmgr::estimate_snapshot_space::call(snapshot),
    );
    offset = maxalign(offset);

    target.ps_locator = heap_relation.rd_locator;
    target.ps_indexlocator = index_relation.rd_locator;
    target.ps_offset_ins = 0;
    target.ps_offset_am = 0;
    // SerializeSnapshot(snapshot, target->ps_snapshot_data).
    target.ps_snapshot_data = snapmgr::serialize_snapshot::call(snapshot)?;

    if instrument {
        target.ps_offset_ins = offset;
        let sharedinfosz = add_size(
            shared_index_scan_instrumentation_header_size(),
            (nworkers as usize) * size_of_index_scan_instrumentation(),
        );
        offset = add_size(offset, sharedinfosz);
        offset = maxalign(offset);

        // Set leader's *sharedinfo pointer (into the DSM at ps_offset_ins),
        // memset it to zero, and initialize num_workers.
        target.shared_instrument = Some(SharedIndexScanInstrumentation {
            num_workers: nworkers,
            winstrument: std::vec![IndexScanInstrumentation::default(); nworkers as usize],
        });
    }

    // aminitparallelscan is optional; assume no-op if not provided by the AM.
    if parallel_aware {
        if let Some(aminitparallelscan) = indam(index_relation).aminitparallelscan {
            target.ps_offset_am = offset;
            let mut amtarget = Vec::new();
            aminitparallelscan(&mut amtarget)?;
            target.am_specific = Some(amtarget);
        }
    }

    Ok(())
}

/// `index_parallelrescan(scan)` — (re)start a parallel scan of an index.
pub fn index_parallelrescan(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    // SCAN_CHECKS.
    if let Some(heapfetch) = scan.xs_heapfetch.as_deref_mut() {
        tableam::table_index_fetch_reset(heapfetch)?;
    }

    // amparallelrescan is optional; assume no-op if not provided by the AM.
    if let Some(amparallelrescan) = indam(&scan.index_relation).amparallelrescan {
        amparallelrescan(scan)?;
    }
    Ok(())
}

/// `index_beginscan_parallel(heaprel, indexrel, instrument, nkeys, norderbys,
/// pscan)` — join a parallel index scan.
pub fn index_beginscan_parallel<'mcx>(
    mcx: Mcx<'mcx>,
    heaprel: &Relation<'mcx>,
    indexrel: &Relation<'mcx>,
    instrument: Option<IndexScanInstrumentation>,
    nkeys: i32,
    norderbys: i32,
    pscan: std::sync::Arc<ParallelIndexScanDescData>,
) -> PgResult<IndexScanDesc<'mcx>> {
    // Assert(RelFileLocatorEquals(heaprel->rd_locator, pscan->ps_locator)) and
    // Assert(RelFileLocatorEquals(indexrel->rd_locator, pscan->ps_indexlocator)).
    debug_assert!(types_storage::RelFileLocatorEquals(
        &heaprel.rd_locator,
        &pscan.ps_locator
    ));
    debug_assert!(types_storage::RelFileLocatorEquals(
        &indexrel.rd_locator,
        &pscan.ps_indexlocator
    ));

    let restored = snapmgr::restore_snapshot::call(&pscan.ps_snapshot_data)?;
    let snapshot = snapmgr::register_snapshot::call(restored)?;
    let mut scan =
        index_beginscan_internal(indexrel, nkeys, norderbys, snapshot.clone(), Some(pscan), true)?;

    // Save additional parameters into the scandesc.
    scan.heap_relation = Some(heaprel.alias());
    scan.xs_snapshot = Some(snapshot);
    scan.instrument = instrument;

    // prepare to fetch index matches from table
    scan.xs_heapfetch = Some(tableam::table_index_fetch_begin(mcx, heaprel)?);

    Ok(scan)
}

// ===========================================================================
// Scan tuple retrieval
// ===========================================================================

/// `index_getnext_tid(scan, direction)` — get the next TID satisfying the scan
/// keys, or `None` when exhausted. On success the TID is `scan->xs_heaptid`
/// (returned here by value once located).
pub fn index_getnext_tid(
    scan: &mut IndexScanDescData<'_>,
    direction: ScanDirection,
) -> PgResult<Option<ItemPointerData>> {
    // CHECK_SCAN_PROCEDURE(amgettuple): optional callback, error if absent.
    let am = indam(&scan.index_relation);
    let amgettuple = check_scan_procedure(am.amgettuple, "amgettuple", &scan.index_relation)?;

    // XXX: we should assert that a snapshot is pushed or registered.
    // Assert(TransactionIdIsValid(RecentXmin)) — RecentXmin is a snapmgr
    // per-backend global; the debug-only assert is omitted rather than
    // modeled with a forbidden ambient-global seam.

    // The AM's amgettuple proc finds the next matching index entry and puts
    // the TID into scan->xs_heaptid (plus xs_recheck/xs_itup/xs_hitup, which
    // we ignore here).
    let found = amgettuple(scan, direction)?;

    // Reset kill flag immediately for safety.
    scan.kill_prior_tuple = false;
    scan.xs_heap_continue = false;

    // If we're out of index entries, we're done.
    if !found {
        // release resources (like buffer pins) from table accesses
        if let Some(heapfetch) = scan.xs_heapfetch.as_deref_mut() {
            tableam::table_index_fetch_reset(heapfetch)?;
        }
        return Ok(None);
    }
    // Assert(ItemPointerIsValid(&scan->xs_heaptid)) — debug-only.

    pgstat::pgstat_count_index_tuples::call(scan.index_relation.rd_id, 1);

    // Return the TID of the tuple we found.
    Ok(Some(scan.xs_heaptid))
}

/// `index_fetch_heap(scan, slot)` — get the scan's next heap tuple for the
/// index TID most recently fetched by [`index_getnext_tid`]. On success the
/// slot holds a visible heap tuple (its buffer pinned by the table AM).
pub fn index_fetch_heap<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    slot: &mut types_nodes::TupleTableSlot<'mcx>,
) -> PgResult<bool> {
    let mut all_dead = false;

    // table_index_fetch_tuple(scan->xs_heapfetch, &scan->xs_heaptid,
    //   scan->xs_snapshot, slot, &scan->xs_heap_continue, &all_dead).
    let heaptid = scan.xs_heaptid;
    let snapshot: types_tableam::Snapshot = scan.xs_snapshot.clone();
    let mut heap_continue = scan.xs_heap_continue;
    let heapfetch = scan
        .xs_heapfetch
        .as_deref_mut()
        .expect("index_fetch_heap with no xs_heapfetch (C would dereference NULL)");
    let found = tableam::table_index_fetch_tuple(
        mcx,
        heapfetch,
        &heaptid,
        &snapshot,
        slot,
        &mut heap_continue,
        Some(&mut all_dead),
    )?;
    scan.xs_heap_continue = heap_continue;

    if found {
        pgstat::pgstat_count_heap_fetch::call(scan.index_relation.rd_id);
    }

    // If we scanned a whole HOT chain and found only dead tuples, tell the
    // index AM to kill its entry (effective next amgettuple). We do not do
    // this in recovery because it may violate MVCC.
    if !scan.xact_started_in_recovery {
        scan.kill_prior_tuple = all_dead;
    }

    Ok(found)
}

/// `index_getnext_slot(scan, direction, slot)` — get the next tuple from a
/// scan into `slot`, returning whether one satisfying the keys + snapshot was
/// found.
pub fn index_getnext_slot<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    direction: ScanDirection,
    slot: &mut types_nodes::TupleTableSlot<'mcx>,
) -> PgResult<bool> {
    loop {
        if !scan.xs_heap_continue {
            // Time to fetch the next TID from the index.
            let tid = index_getnext_tid(scan, direction)?;

            // If we're out of index entries, we're done.
            if tid.is_none() {
                break;
            }
            // Assert(ItemPointerEquals(tid, &scan->xs_heaptid)) — debug-only.
        }

        // Fetch the next (or only) visible heap tuple for this index entry. If
        // we don't find anything, loop around and grab the next TID.
        // Assert(ItemPointerIsValid(&scan->xs_heaptid)) — debug-only.
        if index_fetch_heap(mcx, scan, slot)? {
            return Ok(true);
        }
    }

    Ok(false)
}

/// `index_getbitmap(scan, bitmap)` — add the TIDs of all heap tuples
/// satisfying the scan keys to a bitmap; returns the (possibly approximate)
/// match count.
pub fn index_getbitmap(scan: &mut IndexScanDescData<'_>, bitmap: &mut TIDBitmap) -> PgResult<i64> {
    // CHECK_SCAN_PROCEDURE(amgetbitmap): optional callback, error if absent.
    let am = indam(&scan.index_relation);
    let amgetbitmap = check_scan_procedure(am.amgetbitmap, "amgetbitmap", &scan.index_relation)?;

    // just make sure this is false...
    scan.kill_prior_tuple = false;

    // have the am's getbitmap proc do all the work.
    let ntids = amgetbitmap(scan, bitmap)?;

    pgstat::pgstat_count_index_tuples::call(scan.index_relation.rd_id, ntids);

    Ok(ntids)
}

// ===========================================================================
// Bulk delete / vacuum cleanup
// ===========================================================================

/// `index_bulk_delete(info, istat, callback, callback_state)` — mass deletion
/// of index entries; `info->index` is the index relation. The deletion
/// callback + its state live in the vacuum substrate, so the whole AM call is
/// owned by the AM.
pub fn index_bulk_delete(
    info: &IndexVacuumInfo<'_>,
    istat: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    let index_relation = &info.index;
    relation_checks(index_relation)?;
    // CHECK_REL_PROCEDURE(ambulkdelete): ambulkdelete is a required callback.
    (indam(index_relation).ambulkdelete)(info, istat)
}

/// `index_vacuum_cleanup(info, istat)` — post-deletion cleanup of an index.
pub fn index_vacuum_cleanup(
    info: &IndexVacuumInfo<'_>,
    istat: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    let index_relation = &info.index;
    relation_checks(index_relation)?;
    // CHECK_REL_PROCEDURE(amvacuumcleanup): required callback.
    (indam(index_relation).amvacuumcleanup)(info, istat)
}

// ===========================================================================
// index_can_return
// ===========================================================================

/// `index_can_return(indexRelation, attno)` — does the AM support index-only
/// scans for the given column? `amcanreturn` is optional; absent means false.
pub fn index_can_return(index_relation: &Relation<'_>, attno: i32) -> PgResult<bool> {
    relation_checks(index_relation)?;

    // amcanreturn is optional; assume false if not provided by the AM.
    match indam(index_relation).amcanreturn {
        None => Ok(false),
        Some(amcanreturn) => amcanreturn(index_relation, attno),
    }
}

// ===========================================================================
// index_getprocid / index_getprocinfo
// ===========================================================================

/// `index_getprocid(irel, attnum, procnum)` — the requested default support
/// procedure OID for an indexed attribute.
pub fn index_getprocid(
    irel: &Relation<'_>,
    attnum: AttrNumber,
    procnum: u16,
) -> PgResult<RegProcedure> {
    let nproc = indam(irel).amsupport;

    debug_assert!(procnum > 0 && procnum <= nproc);

    let procindex = (nproc as i32) * ((attnum as i32) - 1) + ((procnum as i32) - 1);

    // loc = irel->rd_support; Assert(loc != NULL); return loc[procindex];
    relcache::rd_support_at::call(irel.rd_id, procindex)
}

/// `index_getprocinfo(irel, attnum, procnum)` — the cached fmgr lookup info
/// for a support procedure (only the default functions are cached). The C
/// returns a pointer into the relcache `rd_supportinfo` cache (lazily
/// initialized on first use); the relcache owner holds and lazily initializes
/// that cache, so the lookup (and its `fmgr_info_cxt` /
/// `set_fn_opclass_options` init) crosses one seam. The procindex arithmetic
/// and the `procnum` range assert are this layer's logic.
pub fn index_getprocinfo(
    irel: &Relation<'_>,
    attnum: AttrNumber,
    procnum: u16,
) -> PgResult<FmgrInfo> {
    let am = indam(irel);
    let nproc = am.amsupport;
    let optsproc = am.amoptsprocnum;

    debug_assert!(procnum > 0 && procnum <= nproc);

    let procindex = (nproc as i32) * ((attnum as i32) - 1) + ((procnum as i32) - 1);

    // locinfo = irel->rd_supportinfo + procindex. The relcache lazily fills
    // the slot (fmgr_info_cxt; plus set_fn_opclass_options when procnum !=
    // optsproc) on first use, complaining (`missing support function ...`) if
    // rd_support[procindex] is invalid; that complaint is part of the seam's
    // error surface.
    relcache::index_getprocinfo::call(irel.rd_id, attnum, procnum, optsproc, procindex)
}

// ===========================================================================
// index_store_float8_orderby_distances / index_opclass_options
// ===========================================================================

/// `index_store_float8_orderby_distances(scan, orderByTypes, distances,
/// recheckOrderBy)` — convert the AM distance function's (possibly inexact)
/// results to the ORDER BY types and save them into the scan's
/// `xs_orderbyvals` / `xs_orderbynulls` for a possible recheck. `distances` is
/// `None` to model the C `NULL` (only valid when `!recheckOrderBy`).
pub fn index_store_float8_orderby_distances(
    scan: &mut IndexScanDescData<'_>,
    order_by_types: &[Oid],
    distances: Option<&[IndexOrderByDistance]>,
    recheck_orderby: bool,
) -> PgResult<()> {
    // Assert(distances || !recheckOrderBy).
    debug_assert!(distances.is_some() || !recheck_orderby);

    scan.xs_recheckorderby = recheck_orderby;

    for i in 0..scan.number_of_order_bys {
        let idx = i as usize;
        let typ = order_by_types[idx];
        let d = distances.map(|ds| ds[idx]);
        if typ == types_tuple::heaptuple::FLOAT8OID {
            // USE_FLOAT8_BYVAL is defined on all supported 64-bit platforms, so
            // the C `#ifndef USE_FLOAT8_BYVAL` pfree branch is compiled out;
            // the owned descriptor's Datum slots hold no allocation either.
            if let Some(d) = d {
                if !d.isnull {
                    scan.xs_orderbyvals[idx] = DatumV::from_f64(d.value);
                    scan.xs_orderbynulls[idx] = false;
                    continue;
                }
            }
            scan.xs_orderbyvals[idx] = DatumV::null();
            scan.xs_orderbynulls[idx] = true;
        } else if typ == types_tuple::heaptuple::FLOAT4OID {
            // convert distance function's result to ORDER BY type
            if let Some(d) = d {
                if !d.isnull {
                    scan.xs_orderbyvals[idx] = DatumV::from_f32(d.value as f32);
                    scan.xs_orderbynulls[idx] = false;
                    continue;
                }
            }
            scan.xs_orderbyvals[idx] = DatumV::null();
            scan.xs_orderbynulls[idx] = true;
        } else {
            // We don't know how to convert the float8 bound to this type. The
            // executor won't need these values unless there are lossy results,
            // so only insist on converting if the recheck flag is set.
            if scan.xs_recheckorderby {
                return Err(PgError::error(
                    "ORDER BY operator must return float8 or float4 if the distance function is lossy",
                )
                .with_sqlstate(ERRCODE_INTERNAL_ERROR));
            }
            scan.xs_orderbynulls[idx] = true;
        }
    }

    Ok(())
}

/// `index_opclass_options(indrel, attnum, attoptions, validate)` — parse
/// opclass-specific options for an index column. The `amoptsprocnum` fetch is
/// this layer's logic; the local-reloptions machinery (`init_local_reloptions`
/// / `FunctionCall1(procinfo)` / `build_local_reloptions`) and the
/// missing-procedure error (which reaches the syscache + ruleutils) cross to
/// their owners.
pub fn index_opclass_options<'mcx>(
    indrel: &Relation<'_>,
    attnum: AttrNumber,
    attoptions: DatumV<'mcx>,
    validate: bool,
) -> PgResult<Option<Vec<u8>>> {
    let amoptsprocnum = indam(indrel).amoptsprocnum;
    let mut procid = InvalidOid;

    // fetch options support procedure if specified
    if amoptsprocnum != 0 {
        procid = index_getprocid(indrel, attnum, amoptsprocnum)?;
    }

    if procid == InvalidOid {
        if attoptions.as_u64() == 0 {
            return Ok(None); // ok, no options, no procedure
        }

        // Report an error if the opclass's options-parsing procedure does not
        // exist but the opclass options are specified. The opclass name comes
        // from indrel->rd_indextuple's indclass[attnum-1], reached through the
        // relcache + generate_opclass_name.
        return Err(relcache::index_opclass_missing_options_error::call(
            indrel.rd_id,
            attnum,
        )?);
    }

    // init_local_reloptions(&relopts, 0); procinfo =
    // index_getprocinfo(indrel, attnum, amoptsprocnum);
    // FunctionCall1(procinfo, PointerGetDatum(&relopts));
    // return build_local_reloptions(&relopts, attoptions, validate).
    let procinfo = index_getprocinfo(indrel, attnum, amoptsprocnum)?;
    // The reloptions seam takes the canonical unified value; `attoptions` is
    // already that type (the `text[]` pointer word travels in its by-value arm)
    // so it forwards verbatim.
    backend_access_common_reloptions_seams::index_build_local_reloptions::call(
        procinfo, attoptions, validate,
    )
}

// ===========================================================================
// Helpers
// ===========================================================================

/// `CHECK_SCAN_PROCEDURE(pname)` for an optional callback: a `None` slot is the
/// C `elog(ERROR, "function \"%s\" is not defined for index \"%s\"")`.
fn check_scan_procedure<F>(
    slot: Option<F>,
    pname: &str,
    index_relation: &Relation<'_>,
) -> PgResult<F> {
    slot.ok_or_else(|| {
        PgError::error(format!(
            "function \"{pname}\" is not defined for index \"{}\"",
            index_relation.name()
        ))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
    })
}

/// `add_size(s1, s2)`.
#[inline]
fn add_size(s1: usize, s2: usize) -> usize {
    s1 + s2
}

/// `MAXALIGN(len)` — round up to `MAXIMUM_ALIGNOF` (8 on supported platforms).
#[inline]
fn maxalign(len: usize) -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

#[cfg(test)]
mod tests;
