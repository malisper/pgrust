#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

pub use types_relscan::*;

mod prefetch;

#[cfg(test)]
mod tests;

use core::mem::MaybeUninit;
use std::rc::Rc;

use datum::Datum;
use tableam::{BatchFetch, INDEX_FETCH_BATCH_MAX};
use mcx::Mcx;
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_WRONG_OBJECT_TYPE,
};
use types_nbtree::{IndexBulkDeleteResult, IndexUniqueCheck};
use types_rel::{
    MaxLockMode, NoLock, Relation, RelationData, LOCKMODE, RELKIND_INDEX,
    RELKIND_PARTITIONED_INDEX,
};
use types_scan::scankey::ScanKeyData;
use types_scan::sdir::ScanDirection;
use types_slot::SlotData;
use types_snapshot::{IsMVCCSnapshot, SnapshotData};
use types_tuple::itemptr::{ItemPointerData, ItemPointerEquals, ItemPointerIsValid};

pub fn init_seams() {
    indexam_seams::index_open::set(index_open);
    indexam_seams::try_index_open::set(try_index_open);
}

pub fn index_open<'mcx>(
    mcx: Mcx<'mcx>,
    relationId: Oid,
    lockmode: LOCKMODE,
) -> PgResult<Relation<'mcx>> {
    let r = relation_seams::relation_open::call(mcx, relationId, lockmode)?;

    validate_relation_kind(&r)?;

    Ok(r)
}

pub fn try_index_open<'mcx>(
    mcx: Mcx<'mcx>,
    relationId: Oid,
    lockmode: LOCKMODE,
) -> PgResult<Option<Relation<'mcx>>> {
    let Some(r) = relation_seams::try_relation_open::call(mcx, relationId, lockmode)? else {
        return Ok(None);
    };

    validate_relation_kind(&r)?;

    Ok(Some(r))
}

/// Consumes the handle; a lock held past close is released at xact end, as in C.
pub fn index_close(relation: Relation<'_>, lockmode: LOCKMODE) -> PgResult<()> {
    debug_assert!(lockmode >= NoLock && lockmode <= MaxLockMode);
    relation.close(lockmode)
}

fn validate_relation_kind(r: &Relation<'_>) -> PgResult<()> {
    let relkind = r.rd_rel.relkind;

    if relkind != RELKIND_INDEX && relkind != RELKIND_PARTITIONED_INDEX {
        return Err(not_an_index(r));
    }

    Ok(())
}

#[cold]
#[inline(never)]
fn not_an_index(r: &RelationData<'_>) -> Box<PgError> {
    Box::new(
        PgError::error(format!("\"{}\" is not an index", r.name()))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
    )
}

// RELATION_CHECKS: the compiled-hot reindex ereport (the validity Asserts live in the types).
fn relation_checks(indexRelation: &Relation<'_>) -> PgResult<()> {
    if reindex_is_processing_index(indexRelation.rd_id) {
        return Err(reindex_in_progress(indexRelation));
    }
    Ok(())
}

#[inline]
fn reindex_is_processing_index(indexId: Oid) -> bool {
    types_rel::reindex::ReindexIsProcessingIndex(indexId)
}

#[cold]
#[inline(never)]
fn reindex_in_progress(r: &RelationData<'_>) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "cannot access index \"{}\" while it is being reindexed",
            r.name()
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

// CHECK_*_PROCEDURE: required callbacks exist per IndexAmKind by construction; optional ones keep the C elog.
#[cold]
#[inline(never)]
fn missing_procedure(pname: &str, r: &RelationData<'_>) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "function \"{pname}\" is not defined for index \"{}\"",
        r.name()
    )))
}

#[cold]
#[inline(never)]
fn mock_outside_tests() -> ! {
    unreachable!("mock index AM outside indexam's own tests")
}

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: {what}")
}

// C divergence: IndexInfo is not threaded; only its ii_AmCache slot is passed.
pub fn index_insert<'mcx>(
    mcx: Mcx<'mcx>,
    indexRelation: &Relation<'mcx>,
    values: &[Datum],
    isnull: &[bool],
    heap_t_ctid: &ItemPointerData,
    heapRelation: &Relation<'mcx>,
    checkUnique: IndexUniqueCheck,
    indexUnchanged: bool,
    // C indexInfo->ii_AmCache (per-statement; gist stores its GISTSTATE,
    // brin its BrinInsertState)
    am_cache: &mut Option<Box<dyn core::any::Any>>,
) -> PgResult<bool> {
    relation_checks(indexRelation)?;
    let kind = IndexAmKind::from_relam(indexRelation.rd_rel.relam);

    if !kind.ampredlocks() {
        predicate::CheckForSerializableConflictIn(
            heapRelation,
            None,
            types_core::InvalidBlockNumber,
        )?;
    }

    am_insert(
        mcx,
        kind,
        indexRelation,
        values,
        isnull,
        heap_t_ctid,
        heapRelation,
        checkUnique,
        indexUnchanged,
        am_cache,
    )
}

/// index_bulk_delete. C divergence (recorded): the callback is monomorphized
/// to the sorted dead-TID slice — vac_tid_reaped is its only producer.
pub fn index_bulk_delete<'mcx>(
    mcx: Mcx<'mcx>,
    info: &nbtree::IndexVacuumInfo<'_, 'mcx>,
    istat: Option<IndexBulkDeleteResult>,
    dead_items: &[ItemPointerData],
) -> PgResult<IndexBulkDeleteResult> {
    relation_checks(info.index)?;
    match IndexAmKind::from_relam(info.index.rd_rel.relam) {
        IndexAmKind::Btree => nbtree::btbulkdelete(mcx, info, istat, dead_items),
        IndexAmKind::Hash => panic!("unported: hashbulkdelete (hash vacuum lane)"),
        IndexAmKind::Gin => gin::ginbulkdelete(),
        IndexAmKind::Gist => {
            gist::gistbulkdelete(info.index)?;
            Ok(istat.unwrap_or_default())
        }
        IndexAmKind::Spgist => {
            spgist::spgbulkdelete(info.index)?;
            Ok(istat.unwrap_or_default())
        }
        IndexAmKind::Brin => brin::brinbulkdelete(),
        #[cfg(test)]
        IndexAmKind::Mock => Ok(istat.unwrap_or_default()),
        #[allow(unreachable_patterns)]
        _ => mock_outside_tests(),
    }
}

/// index_bulk_delete with C's collect-only callback shape (validate_index).
pub fn index_bulk_delete_collect<'mcx>(
    mcx: Mcx<'mcx>,
    info: &nbtree::IndexVacuumInfo<'_, 'mcx>,
    callback: &mut (dyn FnMut(&ItemPointerData) -> PgResult<()> + '_),
) -> PgResult<IndexBulkDeleteResult> {
    relation_checks(info.index)?;
    match IndexAmKind::from_relam(info.index.rd_rel.relam) {
        IndexAmKind::Btree => nbtree::btbulkdelete_collect(mcx, info, callback),
        _ => panic!("unported: ambulkdelete TID-collect beyond btree (validate_index)"),
    }
}

/// index_vacuum_cleanup.
pub fn index_vacuum_cleanup<'mcx>(
    mcx: Mcx<'mcx>,
    info: &nbtree::IndexVacuumInfo<'_, 'mcx>,
    istat: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    relation_checks(info.index)?;
    match IndexAmKind::from_relam(info.index.rd_rel.relam) {
        IndexAmKind::Btree => nbtree::btvacuumcleanup(mcx, info, istat),
        // hashvacuumcleanup: C returns NULL when stats is NULL (the ANALYZE
        // call); the stats-bearing path is the hash vacuum lane.
        IndexAmKind::Hash => {
            if istat.is_none() {
                Ok(None)
            } else {
                panic!("unported: hashvacuumcleanup with stats (hash vacuum lane)")
            }
        }
        // ginvacuumcleanup's analyze_only arm is a C no-op; the rest is the
        // loud vacuum lane. brinvacuumcleanup: same analyze_only early return.
        IndexAmKind::Gin => {
            if info.analyze_only {
                Ok(istat)
            } else {
                gin::ginvacuumcleanup()
            }
        }
        IndexAmKind::Gist => {
            gist::gistvacuumcleanup(info.index, info.analyze_only)?;
            Ok(istat)
        }
        IndexAmKind::Spgist => {
            spgist::spgvacuumcleanup(info.index, info.analyze_only)?;
            Ok(istat)
        }
        IndexAmKind::Brin => {
            if info.analyze_only {
                Ok(istat)
            } else {
                brin::brinvacuumcleanup()
            }
        }
        #[cfg(test)]
        IndexAmKind::Mock => Ok(istat),
        #[allow(unreachable_patterns)]
        _ => mock_outside_tests(),
    }
}

pub fn index_insert_cleanup(
    indexRelation: &Relation<'_>,
    am_cache: &mut Option<Box<dyn core::any::Any>>,
) -> PgResult<()> {
    relation_checks(indexRelation)?;
    let kind = IndexAmKind::from_relam(indexRelation.rd_rel.relam);

    if kind.has_aminsertcleanup() {
        am_insert_cleanup(kind, indexRelation, am_cache)?;
    }
    Ok(())
}

/// Caller must be holding suitable locks on the heap and the index.
pub fn index_beginscan<'mcx>(
    mcx: Mcx<'mcx>,
    heapRelation: &Relation<'mcx>,
    indexRelation: &Relation<'mcx>,
    snapshot: Rc<SnapshotData<'mcx>>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDescData<'mcx>> {
    let mut scan =
        index_beginscan_internal(mcx, indexRelation, nkeys, norderbys, false, Some(&snapshot))?;

    // Everything else was set up by ambeginscan (C RelationGetIndexScan).
    scan.heapRelation = Some(heapRelation.alias());
    scan.xs_snapshot = Some(snapshot);

    scan.xs_heapfetch = Some(fetch::begin(heapRelation));

    Ok(scan)
}

/// `index_beginscan_bitmap`: no heap relation, no heap fetch — the bitmap
/// heap scan node owns heap access.
pub fn index_beginscan_bitmap<'mcx>(
    mcx: Mcx<'mcx>,
    indexRelation: &Relation<'mcx>,
    snapshot: Rc<SnapshotData<'mcx>>,
    nkeys: i32,
) -> PgResult<IndexScanDescData<'mcx>> {
    let mut scan =
        index_beginscan_internal(mcx, indexRelation, nkeys, 0, false, Some(&snapshot))?;
    scan.xs_snapshot = Some(snapshot);
    Ok(scan)
}

/// `index_parallelscan_estimate` + `index_parallelscan_initialize`,
/// thread-native: DSM sizing/offsets collapse to typed Arc construction; the
/// shared-instrumentation arm rides execParallel's collapsed retrieval.
pub fn index_parallelscan_initialize<'mcx>(
    heapRelation: &Relation<'mcx>,
    indexRelation: &Relation<'mcx>,
    snapshot: &::snapmgr::Snapshot,
) -> PgResult<std::sync::Arc<ParallelIndexScanDescShared>> {
    relation_checks(indexRelation)?;
    let am = match IndexAmKind::from_relam(indexRelation.rd_rel.relam) {
        IndexAmKind::Btree => ParallelIndexAmShared::Btree(BTParallelScanShared::new()),
        other => panic!(
            "index_parallelscan_initialize (indexam.c): {other:?} aminitparallelscan unported"
        ),
    };
    Ok(std::sync::Arc::new(ParallelIndexScanDescShared {
        ps_locator: heapRelation.rd_locator.get(),
        ps_indexlocator: indexRelation.rd_locator.get(),
        snapshot: ::snapmgr::SerializeSnapshot(snapshot),
        am,
    }))
}

/// `index_parallelrescan`.
pub fn index_parallelrescan(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    if let Some(heapfetch) = scan.xs_heapfetch.as_mut() {
        fetch::reset(heapfetch);
    }
    let pscan = scan
        .parallel_scan
        .as_deref()
        .expect("index_parallelrescan without parallel_scan");
    match &pscan.am {
        ParallelIndexAmShared::Btree(shared) => nbtree::btparallelrescan(shared),
    }
    Ok(())
}

/// `index_beginscan_parallel`.
pub fn index_beginscan_parallel<'mcx>(
    mcx: Mcx<'mcx>,
    heaprel: &Relation<'mcx>,
    indexrel: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
    pscan: std::sync::Arc<ParallelIndexScanDescShared>,
) -> PgResult<IndexScanDescData<'mcx>> {
    debug_assert!(heaprel.rd_locator.get() == pscan.ps_locator);
    debug_assert!(indexrel.rd_locator.get() == pscan.ps_indexlocator);

    let snapshot = ::snapmgr::RestoreSnapshot(&pscan.snapshot);
    let snapshot = ::snapmgr::RegisterSnapshot(Some(&snapshot))?.expect("registered a snapshot");

    let mut scan =
        index_beginscan_internal(mcx, indexrel, nkeys, norderbys, true, Some(&snapshot))?;
    scan.parallel_scan = Some(pscan);
    scan.heapRelation = Some(heaprel.alias());
    scan.xs_snapshot = Some(snapshot.clone());
    scan.xs_temp_snapshot = Some(snapshot);

    scan.xs_heapfetch = Some(fetch::begin(heaprel));

    Ok(scan)
}

/// `index_getbitmap`: drop all matching TIDs into the bitmap; returns ntids.
pub fn index_getbitmap<'mcx>(
    scan: &mut IndexScanDescData<'mcx>,
    bitmap: &mut tidbitmap::TIDBitmap<'_>,
) -> PgResult<i64> {
    scan.kill_prior_tuple = false;
    let ntids = am_getbitmap(scan, bitmap)?;
    scan.xs_pgstat_index_tuples += ntids as u64;
    Ok(ntids)
}

// CHECK_SCAN_PROCEDURE(amgetbitmap): a tuple-only AM would get an error arm here.
fn am_getbitmap(
    scan: &mut IndexScanDescData<'_>,
    bitmap: &mut tidbitmap::TIDBitmap<'_>,
) -> PgResult<i64> {
    match scan.opaque {
        IndexScanOpaque::Btree(_) => nbtree::btgetbitmap(scan, bitmap),
        IndexScanOpaque::Hash(_) => hash::hashgetbitmap(scan, bitmap),
        IndexScanOpaque::Gin(_) => gin::gingetbitmap(scan, bitmap),
        IndexScanOpaque::Gist(_) => gist::gistgetbitmap(scan, bitmap),
        IndexScanOpaque::Spgist(_) => spgist::spggetbitmap(scan, bitmap),
        IndexScanOpaque::Brin(_) => brin::bringetbitmap(scan, bitmap),
        #[cfg(test)]
        IndexScanOpaque::Mock(_) => unreachable!("Mock lacks amgetbitmap"),
        #[allow(unreachable_patterns)]
        _ => mock_outside_tests(),
    }
}

fn index_beginscan_internal<'mcx>(
    mcx: Mcx<'mcx>,
    indexRelation: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
    temp_snap: bool,
    snapshot: Option<&SnapshotData<'mcx>>,
) -> PgResult<IndexScanDescData<'mcx>> {
    relation_checks(indexRelation)?;
    let kind = IndexAmKind::from_relam(indexRelation.rd_rel.relam);

    if !kind.ampredlocks() {
        predicate::PredicateLockRelation(
            indexRelation,
            snapshot.expect("index_beginscan_internal without snapshot"),
        )?;
    }

    // RelationIncrementReferenceCount: the scan's alias Rc is rd_refcnt, held throughout.
    let mut scan = am_beginscan(mcx, kind, indexRelation, nkeys, norderbys)?;

    scan.xs_temp_snap = temp_snap;

    Ok(scan)
}

/// Key counts must equal what `index_beginscan` was told; `None` restarts the
/// scan without changing keys (the C NULL).
pub fn index_rescan<'mcx>(
    scan: &mut IndexScanDescData<'mcx>,
    keys: Option<&[ScanKeyData]>,
    orderbys: Option<&[ScanKeyData]>,
) -> PgResult<()> {
    debug_assert!(keys.map_or(true, |k| k.len() as i32 == scan.numberOfKeys));
    debug_assert!(orderbys.map_or(true, |k| k.len() as i32 == scan.numberOfOrderBys));

    if let Some(heapfetch) = scan.xs_heapfetch.as_mut() {
        fetch::reset(heapfetch);
    }

    scan.kill_prior_tuple = false; // for safety
    scan.xs_heap_continue = false;
    scan.xs_prefetch = types_relscan::IndexPrefetchState::reset();

    am_rescan(scan, keys, orderbys)
}

pub fn index_endscan(mut scan: IndexScanDescData<'_>) -> PgResult<()> {
    if let Some(heapfetch) = scan.xs_heapfetch.take() {
        fetch::end(heapfetch);
    }

    am_endscan(&mut scan)?;

    if scan.xs_temp_snap {
        ::snapmgr::UnregisterSnapshot(scan.xs_temp_snapshot.take().as_ref());
    }

    // RelationDecrementReferenceCount + IndexScanEnd: the drop of the scan value.
    Ok(())
}

pub fn index_markpos(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    let kind = scan.opaque.kind();
    if !kind.has_ammarkpos() {
        return Err(missing_procedure("ammarkpos", &scan.indexRelation));
    }

    am_markpos(scan)
}

pub fn index_restrpos(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    debug_assert!(scan.xs_snapshot.as_ref().is_some_and(|s| IsMVCCSnapshot(s)));

    let kind = scan.opaque.kind();
    if !kind.has_amrestrpos() {
        return Err(missing_procedure("amrestrpos", &scan.indexRelation));
    }

    if let Some(heapfetch) = scan.xs_heapfetch.as_mut() {
        fetch::reset(heapfetch);
    }

    scan.kill_prior_tuple = false; // for safety
    scan.xs_heap_continue = false;

    am_restrpos(scan)
}

/// Next TID satisfying the scan keys, or `None` when exhausted. On success the
/// TID is also `scan.xs_heaptid`.
pub fn index_getnext_tid(
    scan: &mut IndexScanDescData<'_>,
    direction: ScanDirection,
) -> PgResult<Option<ItemPointerData>> {
    // amgettuple sets xs_heaptid/xs_recheck, reading kill_prior_tuple before the reset below.
    let found = am_gettuple(scan, direction)?;

    // Reset kill flag immediately for safety
    scan.kill_prior_tuple = false;
    scan.xs_heap_continue = false;

    if !found {
        // release resources (like buffer pins) from table accesses
        if let Some(heapfetch) = scan.xs_heapfetch.as_mut() {
            fetch::reset(heapfetch);
        }
        return Ok(None);
    }
    debug_assert!(ItemPointerIsValid(&scan.xs_heaptid));

    pgstat_count_index_tuples(scan, 1);

    Ok(Some(scan.xs_heaptid))
}

/// Fetch the visible heap tuple for the TID from the last `index_getnext_tid`
/// into `slot`. Caller must check `scan.xs_recheck`. `mcx` is the slot's
/// owning context. On success `xs_heaptid` is updated to the resolved
/// HOT-chain member (C mutates the tid through the fetch callback).
pub fn index_fetch_heap<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    // Serve from an active same-page run first; Miss falls through with the
    // batch invalidated.
    let outcome = {
        let IndexScanDescData {
            xs_heapfetch,
            xs_heaptid,
            ..
        } = scan;
        match xs_heapfetch.as_mut() {
            Some(heapfetch) => fetch::batch_next(mcx, heapfetch, xs_heaptid, slot),
            None => BatchFetch::Miss,
        }
    };
    if let Some(found) = batch_outcome(scan, outcome) {
        return Ok(found);
    }

    // New heap block (or non-batch row): keep the readahead window primed.
    prefetch::on_heap_fetch(scan)?;

    // Start a run when the index holds more TIDs for this heap page. MVCC
    // verdicts are fill-time-independent; non-MVCC keeps the per-tuple path.
    if !scan.xs_heap_continue && scan.xs_snapshot.as_deref().is_some_and(IsMVCCSnapshot) {
        if let IndexScanOpaque::Btree(so) = &scan.opaque {
            let mut run: [MaybeUninit<ItemPointerData>; INDEX_FETCH_BATCH_MAX] =
                [const { MaybeUninit::uninit() }; INDEX_FETCH_BATCH_MAX];
            let n = nbtree::bt_peek_same_block_tids(so, &mut run[..INDEX_FETCH_BATCH_MAX - 1]);
            if n > 0 {
                // SAFETY: prefix written by the peek.
                let rest = unsafe {
                    core::slice::from_raw_parts(run.as_ptr() as *const ItemPointerData, n)
                };
                let outcome = {
                    let IndexScanDescData {
                        xs_heapfetch,
                        xs_heaptid,
                        xs_snapshot,
                        ..
                    } = scan;
                    let heapfetch = xs_heapfetch
                        .as_mut()
                        .expect("index_fetch_heap: xs_heapfetch not armed (C would dereference NULL)");
                    fetch::batch_fill(mcx, heapfetch, xs_heaptid, rest, xs_snapshot)?;
                    fetch::batch_next(mcx, heapfetch, xs_heaptid, slot)
                };
                debug_assert!(!matches!(outcome, BatchFetch::Miss));
                if let Some(found) = batch_outcome(scan, outcome) {
                    return Ok(found);
                }
            }
        }
    }

    let mut all_dead = false;

    // Disjoint field borrows: the fetch mutates xs_heaptid/xs_heap_continue
    // while holding the descriptor.
    let IndexScanDescData {
        xs_heapfetch,
        xs_heaptid,
        xs_snapshot,
        xs_heap_continue,
        ..
    } = scan;
    let heapfetch = xs_heapfetch
        .as_mut()
        .expect("index_fetch_heap: xs_heapfetch not armed (C would dereference NULL)");
    let found = fetch::tuple(
        mcx,
        heapfetch,
        xs_heaptid,
        xs_snapshot,
        slot,
        xs_heap_continue,
        &mut all_dead,
    )?;

    if found {
        pgstat_count_heap_fetch(scan);
    }

    // A fully-dead HOT chain kills the AM's entry on the next amgettuple — never in recovery (MVCC hazard).
    if !scan.xactStartedInRecovery {
        scan.kill_prior_tuple = all_dead;
    }

    Ok(found)
}

#[inline]
fn batch_outcome(scan: &mut IndexScanDescData<'_>, outcome: BatchFetch) -> Option<bool> {
    match outcome {
        BatchFetch::Stored => {
            pgstat_count_heap_fetch(scan);
            if !scan.xactStartedInRecovery {
                scan.kill_prior_tuple = false;
            }
            Some(true)
        }
        BatchFetch::NotVisible { all_dead } => {
            if !scan.xactStartedInRecovery {
                scan.kill_prior_tuple = all_dead;
            }
            Some(false)
        }
        BatchFetch::Miss => None,
    }
}

/// True when a tuple satisfying the scan keys and snapshot landed in `slot`.
/// Caller must check `scan.xs_recheck`.
pub fn index_getnext_slot<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    direction: ScanDirection,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    loop {
        if !scan.xs_heap_continue {
            let Some(tid) = index_getnext_tid(scan, direction)? else {
                return Ok(false);
            };
            debug_assert!(ItemPointerEquals(&tid, &scan.xs_heaptid));
        }

        // No visible tuple in this HOT chain: loop for the next TID.
        debug_assert!(ItemPointerIsValid(&scan.xs_heaptid));
        if index_fetch_heap(mcx, scan, slot)? {
            return Ok(true);
        }
    }
}

/// Fused-drive support: advance to the next TID and stage its same-block
/// heap-fetch run (batch_fill under one lock). Returns TIDs staged, first
/// already current in `xs_heaptid`; 0 = scan exhausted. Consumers drain via
/// `index_getnext_tid` + `index_fetch_heap` in fill order (batch hits).
pub fn index_getnext_tidrun<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    direction: ScanDirection,
) -> PgResult<u32> {
    if index_getnext_tid(scan, direction)?.is_none() {
        return Ok(0);
    }
    prefetch::on_heap_fetch(scan)?;
    if !scan.xs_heap_continue && scan.xs_snapshot.as_deref().is_some_and(IsMVCCSnapshot) {
        if let IndexScanOpaque::Btree(so) = &scan.opaque {
            let mut run: [MaybeUninit<ItemPointerData>; INDEX_FETCH_BATCH_MAX] =
                [const { MaybeUninit::uninit() }; INDEX_FETCH_BATCH_MAX];
            let n = nbtree::bt_peek_same_block_tids(so, &mut run[..INDEX_FETCH_BATCH_MAX - 1]);
            if n > 0 {
                // SAFETY: prefix written by the peek.
                let rest = unsafe {
                    core::slice::from_raw_parts(run.as_ptr() as *const ItemPointerData, n)
                };
                let IndexScanDescData {
                    xs_heapfetch,
                    xs_heaptid,
                    xs_snapshot,
                    ..
                } = scan;
                let heapfetch = xs_heapfetch
                    .as_mut()
                    .expect("index_getnext_tidrun: xs_heapfetch not armed");
                fetch::batch_fill(mcx, heapfetch, xs_heaptid, rest, xs_snapshot)?;
                return Ok(1 + n as u32);
            }
        }
    }
    Ok(1)
}

// One probe on the enabled flag then one add: C's pgstat_should_count_relation shape.
#[inline]
fn pgstat_count_index_tuples(scan: &mut IndexScanDescData<'_>, n: u64) {
    if scan.indexRelation.pgstat_enabled.get() {
        scan.xs_pgstat_index_tuples += n;
    }
}

#[inline]
fn pgstat_count_heap_fetch(scan: &mut IndexScanDescData<'_>) {
    if scan.indexRelation.pgstat_enabled.get() {
        scan.xs_pgstat_heap_fetches += 1;
    }
}

// IndexAmRoutine dispatch (rule-4 enum arms; direct calls, no seams).
#[allow(unused_variables)]
fn am_beginscan<'mcx>(
    mcx: Mcx<'mcx>,
    kind: IndexAmKind,
    indexRelation: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDescData<'mcx>> {
    match kind {
        IndexAmKind::Btree => nbtree::btbeginscan(mcx, indexRelation, nkeys, norderbys),
        IndexAmKind::Hash => hash::hashbeginscan(mcx, indexRelation, nkeys, norderbys),
        IndexAmKind::Gin => gin::ginbeginscan(mcx, indexRelation, nkeys, norderbys),
        IndexAmKind::Gist => gist::gistbeginscan(mcx, indexRelation, nkeys, norderbys),
        IndexAmKind::Spgist => spgist::spgbeginscan(mcx, indexRelation, nkeys, norderbys),
        IndexAmKind::Brin => brin::brinbeginscan(mcx, indexRelation, nkeys, norderbys),
        #[cfg(test)]
        IndexAmKind::Mock => Ok(mock::beginscan(mcx, indexRelation, nkeys, norderbys)),
        #[allow(unreachable_patterns)]
        _ => mock_outside_tests(),
    }
}

#[allow(unused_variables)]
fn am_rescan(
    scan: &mut IndexScanDescData<'_>,
    keys: Option<&[ScanKeyData]>,
    orderbys: Option<&[ScanKeyData]>,
) -> PgResult<()> {
    match scan.opaque {
        IndexScanOpaque::Btree(_) => nbtree::btrescan(scan, keys),
        IndexScanOpaque::Hash(_) => hash::hashrescan(scan, keys),
        IndexScanOpaque::Gin(_) => gin::ginrescan(scan, keys),
        IndexScanOpaque::Gist(_) => gist::gistrescan(scan, keys, orderbys),
        IndexScanOpaque::Spgist(_) => spgist::spgrescan(scan, keys, orderbys),
        IndexScanOpaque::Brin(_) => brin::brinrescan(scan, keys),
        #[cfg(test)]
        IndexScanOpaque::Mock(_) => Ok(mock::rescan(scan)),
        #[allow(unreachable_patterns)]
        _ => mock_outside_tests(),
    }
}

fn am_endscan(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    match scan.opaque {
        IndexScanOpaque::Btree(_) => nbtree::btendscan(scan),
        IndexScanOpaque::Hash(_) => hash::hashendscan(scan),
        IndexScanOpaque::Gin(_) => gin::ginendscan(scan),
        IndexScanOpaque::Gist(_) => gist::gistendscan(scan),
        IndexScanOpaque::Spgist(_) => spgist::spgendscan(scan),
        IndexScanOpaque::Brin(_) => brin::brinendscan(scan),
        #[cfg(test)]
        IndexScanOpaque::Mock(_) => Ok(()),
        #[allow(unreachable_patterns)]
        _ => mock_outside_tests(),
    }
}

fn am_markpos(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    match scan.opaque {
        IndexScanOpaque::Btree(_) => nbtree::btmarkpos(scan),
        IndexScanOpaque::Hash(_) => unreachable!("hash lacks ammarkpos (guarded by has_ammarkpos)"),
        IndexScanOpaque::Gin(_) => unreachable!("gin lacks ammarkpos (guarded by has_ammarkpos)"),
        IndexScanOpaque::Gist(_) => Err(missing_procedure("ammarkpos", &scan.indexRelation)),
        IndexScanOpaque::Spgist(_) => unreachable!("has_ammarkpos gate"),
        IndexScanOpaque::Brin(_) => unreachable!("has_ammarkpos gate"),
        #[cfg(test)]
        IndexScanOpaque::Mock(_) => Ok(mock::markpos(scan)),
        #[allow(unreachable_patterns)]
        _ => mock_outside_tests(),
    }
}

fn am_restrpos(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    match scan.opaque {
        IndexScanOpaque::Btree(_) => nbtree::btrestrpos(scan),
        IndexScanOpaque::Hash(_) => unreachable!("hash lacks amrestrpos (guarded by has_amrestrpos)"),
        IndexScanOpaque::Gin(_) => unreachable!("gin lacks amrestrpos (guarded by has_amrestrpos)"),
        IndexScanOpaque::Gist(_) => Err(missing_procedure("amrestrpos", &scan.indexRelation)),
        IndexScanOpaque::Spgist(_) => unreachable!("has_amrestrpos gate"),
        IndexScanOpaque::Brin(_) => unreachable!("has_amrestrpos gate"),
        #[cfg(test)]
        IndexScanOpaque::Mock(_) => unreachable!("Mock lacks amrestrpos"),
        #[allow(unreachable_patterns)]
        _ => mock_outside_tests(),
    }
}

// CHECK_SCAN_PROCEDURE(amgettuple) folds in: a bitmap-only AM would get an error arm here.
#[allow(unused_variables)]
fn am_gettuple(scan: &mut IndexScanDescData<'_>, direction: ScanDirection) -> PgResult<bool> {
    match scan.opaque {
        IndexScanOpaque::Btree(_) => nbtree::btgettuple(scan, direction),
        IndexScanOpaque::Hash(_) => hash::hashgettuple(scan, direction),
        IndexScanOpaque::Gin(_) => panic!(
            "index \"{}\" does not support amgettuple (bitmap-only AM)",
            scan.indexRelation.name()
        ),
        IndexScanOpaque::Gist(_) => gist::gistgettuple(scan, direction),
        IndexScanOpaque::Spgist(_) => spgist::spggettuple(scan, direction),
        // CHECK_SCAN_PROCEDURE(amgettuple): BRIN is bitmap-only.
        IndexScanOpaque::Brin(_) => Err(missing_procedure("amgettuple", &scan.indexRelation)),
        #[cfg(test)]
        IndexScanOpaque::Mock(_) => Ok(mock::gettuple(scan)),
        #[allow(unreachable_patterns)]
        _ => mock_outside_tests(),
    }
}

#[allow(unused_variables)]
fn am_insert<'mcx>(
    mcx: Mcx<'mcx>,
    kind: IndexAmKind,
    indexRelation: &Relation<'mcx>,
    values: &[Datum],
    isnull: &[bool],
    heap_t_ctid: &ItemPointerData,
    heapRelation: &Relation<'mcx>,
    checkUnique: IndexUniqueCheck,
    indexUnchanged: bool,
    am_cache: &mut Option<Box<dyn core::any::Any>>,
) -> PgResult<bool> {
    match kind {
        IndexAmKind::Btree => nbtree::btinsert(
            mcx,
            indexRelation,
            values,
            isnull,
            heap_t_ctid,
            heapRelation,
            checkUnique,
            indexUnchanged,
        ),
        IndexAmKind::Hash => hash::hashinsert(
            mcx,
            indexRelation,
            values,
            isnull,
            heap_t_ctid,
            heapRelation,
        ),
        IndexAmKind::Gin => gin::gininsert(
            mcx,
            indexRelation,
            values,
            isnull,
            heap_t_ctid,
            heapRelation,
        ),
        IndexAmKind::Gist => {
            // gistinsert ignores checkUnique (temporal PK/UNIQUE indexes are
            // indisunique gist; enforcement runs via the exclusion recheck).
            let _ = checkUnique;
            // downcast once per row on a cache slot C derefs as void*; the
            // resolve-once state lives inside (rule-5 ii_AmCache mirror).
            if am_cache.is_none() {
                *am_cache = Some(Box::new(
                    None::<gist::GistInsertAmCache<'static>>,
                ));
            }
            let slot = am_cache
                .as_mut()
                .expect("just filled")
                .downcast_mut::<Option<gist::GistInsertAmCache<'static>>>()
                .expect("gist ii_AmCache slot type");
            // SAFETY: the cache's tupdesc Rcs alias the open index relation's
            // relcache entry, which outlives the per-statement cache slot
            // (ResultRelIndexState holds the relation open).
            let slot: &mut Option<gist::GistInsertAmCache<'mcx>> =
                unsafe { core::mem::transmute(slot) };
            gist::gistinsert(mcx, indexRelation, values, isnull, heap_t_ctid, heapRelation, slot)
        }
        IndexAmKind::Spgist => {
            debug_assert!(checkUnique == IndexUniqueCheck::UNIQUE_CHECK_NO);
            if am_cache.is_none() {
                *am_cache = Some(Box::new(
                    None::<spgist::SpgInsertAmCache<'static>>,
                ));
            }
            let slot = am_cache
                .as_mut()
                .expect("just filled")
                .downcast_mut::<Option<spgist::SpgInsertAmCache<'static>>>()
                .expect("spgist ii_AmCache slot type");
            // SAFETY: same relcache-outlives-slot argument as the gist arm.
            let slot: &mut Option<spgist::SpgInsertAmCache<'mcx>> =
                unsafe { core::mem::transmute(slot) };
            spgist::spginsert(mcx, indexRelation, values, isnull, heap_t_ctid, slot)
        }
        IndexAmKind::Brin => {
            if am_cache.is_none() {
                *am_cache = Some(Box::new(
                    None::<types_brin::BrinInsertState<'static>>,
                ));
            }
            let slot = am_cache
                .as_mut()
                .expect("just filled")
                .downcast_mut::<Option<types_brin::BrinInsertState<'static>>>()
                .expect("brin ii_AmCache slot type");
            // SAFETY: same relcache-outlives-slot argument as the gist arm.
            let slot: &mut Option<types_brin::BrinInsertState<'mcx>> =
                unsafe { core::mem::transmute(slot) };
            brin::brininsert(mcx, indexRelation, values, isnull, heap_t_ctid, slot)
        }
        #[cfg(test)]
        IndexAmKind::Mock => Ok(true),
        #[allow(unreachable_patterns)]
        _ => mock_outside_tests(),
    }
}

#[allow(unused_variables)]
fn am_insert_cleanup(
    kind: IndexAmKind,
    indexRelation: &Relation<'_>,
    am_cache: &mut Option<Box<dyn core::any::Any>>,
) -> PgResult<()> {
    match kind {
        IndexAmKind::Btree => unported("nbtree aminsertcleanup (insert lane is phase 2)"),
        IndexAmKind::Hash => unreachable!("hash lacks aminsertcleanup (guarded)"),
        IndexAmKind::Gin => unreachable!("gin lacks aminsertcleanup (guarded)"),
        IndexAmKind::Gist => Ok(()),
        IndexAmKind::Spgist => unreachable!("spgist lacks aminsertcleanup (guarded)"),
        IndexAmKind::Brin => {
            let Some(boxed) = am_cache else { return Ok(()) };
            let slot = boxed
                .downcast_mut::<Option<types_brin::BrinInsertState<'static>>>()
                .expect("brin ii_AmCache slot type");
            brin::brininsertcleanup(slot)
        }
        #[cfg(test)]
        IndexAmKind::Mock => Ok(()),
        #[allow(unreachable_patterns)]
        _ => mock_outside_tests(),
    }
}

// The table-AM fetch boundary: direct tableam calls (heapam_handler landed);
// unit tests substitute a scripted mock via the relscan wrapper's Mock arm.
mod fetch {
    use super::*;

    #[cfg(not(test))]
    pub fn begin<'mcx>(heapRelation: &Relation<'mcx>) -> IndexFetchTableData<'mcx> {
        IndexFetchTableData::Table(tableam::table_index_fetch_begin(heapRelation))
    }

    #[cfg(test)]
    pub fn begin<'mcx>(heapRelation: &Relation<'mcx>) -> IndexFetchTableData<'mcx> {
        IndexFetchTableData::Mock(types_relscan::MockFetch {
            rel: heapRelation.alias(),
            mock_fetch: Vec::new(),
            resets: 0,
        })
    }

    pub fn reset(heapfetch: &mut IndexFetchTableData<'_>) {
        match heapfetch {
            IndexFetchTableData::Table(t) => tableam::table_index_fetch_reset(t),
            #[allow(unreachable_patterns)]
            other => mock::reset(other),
        }
    }

    pub fn end(heapfetch: IndexFetchTableData<'_>) {
        match heapfetch {
            IndexFetchTableData::Table(t) => tableam::table_index_fetch_end(t),
            #[allow(unreachable_patterns)]
            _ => {}
        }
    }

    pub fn tuple<'mcx>(
        mcx: Mcx<'mcx>,
        heapfetch: &mut IndexFetchTableData<'mcx>,
        tid: &mut ItemPointerData,
        snapshot: &mut Option<Rc<SnapshotData<'mcx>>>,
        slot: &mut SlotData<'mcx>,
        call_again: &mut bool,
        all_dead: &mut bool,
    ) -> PgResult<bool> {
        match heapfetch {
            IndexFetchTableData::Table(t) => tableam::table_index_fetch_tuple(
                mcx,
                t,
                tid,
                snapshot,
                slot,
                call_again,
                Some(all_dead),
            ),
            #[allow(unreachable_patterns)]
            other => mock::tuple(other, tid, slot, call_again, all_dead),
        }
    }

    pub fn batch_fill<'mcx>(
        mcx: Mcx<'mcx>,
        heapfetch: &mut IndexFetchTableData<'mcx>,
        first_tid: &ItemPointerData,
        rest: &[ItemPointerData],
        snapshot: &Option<Rc<SnapshotData<'mcx>>>,
    ) -> PgResult<()> {
        match heapfetch {
            IndexFetchTableData::Table(t) => {
                tableam::table_index_fetch_batch_fill(mcx, t, first_tid, rest, snapshot)
            }
            #[allow(unreachable_patterns)]
            _ => unreachable!("batch fill on a mock table fetch"),
        }
    }

    pub fn batch_next<'mcx>(
        mcx: Mcx<'mcx>,
        heapfetch: &mut IndexFetchTableData<'mcx>,
        tid: &mut ItemPointerData,
        slot: &mut SlotData<'mcx>,
    ) -> BatchFetch {
        match heapfetch {
            IndexFetchTableData::Table(t) => {
                tableam::table_index_fetch_batch_next(mcx, t, tid, slot)
            }
            #[allow(unreachable_patterns)]
            _ => BatchFetch::Miss,
        }
    }

    // The Mock arm exists only under the relscan "mock" feature (test builds);
    // a non-test build reaching here would be a wiring bug.
    #[cfg(test)]
    mod mock {
        use super::*;

        pub fn reset(heapfetch: &mut IndexFetchTableData<'_>) {
            heapfetch.mock_mut().resets += 1;
        }

        pub fn tuple<'mcx>(
            heapfetch: &mut IndexFetchTableData<'mcx>,
            tid: &mut ItemPointerData,
            slot: &mut SlotData<'mcx>,
            call_again: &mut bool,
            all_dead: &mut bool,
        ) -> PgResult<bool> {
            let (found, cont, dead) = heapfetch.mock_mut().mock_fetch.remove(0);
            *call_again = cont;
            *all_dead = dead;
            if found {
                slot.base_mut().tts_tid = *tid;
            }
            Ok(found)
        }
    }

    #[cfg(not(test))]
    mod mock {
        use super::*;

        pub fn reset(_heapfetch: &mut IndexFetchTableData<'_>) {
            unreachable!("mock table fetch outside tests")
        }

        pub fn tuple<'mcx>(
            _heapfetch: &mut IndexFetchTableData<'mcx>,
            _tid: &mut ItemPointerData,
            _slot: &mut SlotData<'mcx>,
            _call_again: &mut bool,
            _all_dead: &mut bool,
        ) -> PgResult<bool> {
            unreachable!("mock table fetch outside tests")
        }
    }
}

#[cfg(test)]
mod mock {
    use super::*;
    use mcx::PgVec;

    pub fn beginscan<'mcx>(
        mcx: Mcx<'mcx>,
        indexRelation: &Relation<'mcx>,
        nkeys: i32,
        norderbys: i32,
    ) -> IndexScanDescData<'mcx> {
        IndexScanDescData {
            heapRelation: None,
            indexRelation: indexRelation.alias(),
            xs_snapshot: None,
            numberOfKeys: nkeys,
            numberOfOrderBys: norderbys,
            keyData: PgVec::new_in(mcx),
            orderByData: PgVec::new_in(mcx),
            parallel_scan: None,
            xs_want_itup: false,
            xs_itup: None,
            xs_itupdesc: None,
            xs_temp_snap: false,
            xs_temp_snapshot: None,
            kill_prior_tuple: false,
            ignore_killed_tuples: true,
            xactStartedInRecovery: false,
            opaque: IndexScanOpaque::Mock(MockOpaque::default()),
            xs_heaptid: ItemPointerData::invalid(),
            xs_heap_continue: false,
            xs_heapfetch: None,
            xs_recheck: false,
            xs_prefetch: types_relscan::IndexPrefetchState::reset(),
            xs_pgstat_index_tuples: 0,
            xs_pgstat_heap_fetches: 0,
            xs_pgstat_index_scans: 0,
            xs_nsearches: 0,
        }
    }

    pub fn gettuple(scan: &mut IndexScanDescData<'_>) -> bool {
        let kill = scan.kill_prior_tuple;
        let IndexScanOpaque::Mock(m) = &mut scan.opaque else {
            unreachable!()
        };
        m.kill_seen.push(kill);
        if m.next < m.tids.len() {
            let tid = m.tids[m.next];
            m.next += 1;
            scan.xs_heaptid = tid;
            true
        } else {
            false
        }
    }

    pub fn rescan(scan: &mut IndexScanDescData<'_>) {
        let IndexScanOpaque::Mock(m) = &mut scan.opaque else {
            unreachable!()
        };
        m.rescans += 1;
        m.next = 0;
    }

    pub fn markpos(scan: &mut IndexScanDescData<'_>) {
        let IndexScanOpaque::Mock(m) = &mut scan.opaque else {
            unreachable!()
        };
        m.markpos_calls += 1;
    }
}
