#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

pub use types_relscan::*;

#[cfg(test)]
mod tests;

use std::rc::Rc;

use datum::Datum;
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

// REINDEX is unimplemented repo-wide (C's list statically empty); reroute via catalog/index.c when it lands.
#[inline]
fn reindex_is_processing_index(_indexId: Oid) -> bool {
    false
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

// C divergence: IndexInfo not threaded (execnodes unported; btree ignores it).
pub fn index_insert<'mcx>(
    mcx: Mcx<'mcx>,
    indexRelation: &Relation<'mcx>,
    values: &[Datum],
    isnull: &[bool],
    heap_t_ctid: &ItemPointerData,
    heapRelation: &Relation<'mcx>,
    checkUnique: IndexUniqueCheck,
    indexUnchanged: bool,
) -> PgResult<bool> {
    relation_checks(indexRelation)?;
    let kind = IndexAmKind::from_relam(indexRelation.rd_rel.relam);

    if !kind.ampredlocks() {
        unported("predicate CheckForSerializableConflictIn (backend/storage/lmgr/predicate)");
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
        IndexAmKind::Gin => gin::ginbulkdelete(),
        #[cfg(test)]
        IndexAmKind::Mock => Ok(istat.unwrap_or_default()),
        #[allow(unreachable_patterns)]
        _ => mock_outside_tests(),
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
        // ginvacuumcleanup's analyze_only arm is a C no-op; the rest is the
        // loud vacuum lane.
        IndexAmKind::Gin => {
            if info.analyze_only {
                Ok(istat)
            } else {
                gin::ginvacuumcleanup()
            }
        }
        #[cfg(test)]
        IndexAmKind::Mock => Ok(istat),
        #[allow(unreachable_patterns)]
        _ => mock_outside_tests(),
    }
}

pub fn index_insert_cleanup(indexRelation: &Relation<'_>) -> PgResult<()> {
    relation_checks(indexRelation)?;
    let kind = IndexAmKind::from_relam(indexRelation.rd_rel.relam);

    if kind.has_aminsertcleanup() {
        am_insert_cleanup(kind, indexRelation)?;
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
    let mut scan = index_beginscan_internal(mcx, indexRelation, nkeys, norderbys, false)?;

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
    let mut scan = index_beginscan_internal(mcx, indexRelation, nkeys, 0, false)?;
    scan.xs_snapshot = Some(snapshot);
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
) -> PgResult<IndexScanDescData<'mcx>> {
    relation_checks(indexRelation)?;
    let kind = IndexAmKind::from_relam(indexRelation.rd_rel.relam);

    if !kind.ampredlocks() {
        unported("predicate PredicateLockRelation (backend/storage/lmgr/predicate)");
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

    am_rescan(scan, keys, orderbys)
}

pub fn index_endscan(mut scan: IndexScanDescData<'_>) -> PgResult<()> {
    if let Some(heapfetch) = scan.xs_heapfetch.take() {
        fetch::end(heapfetch);
    }

    am_endscan(&mut scan)?;

    if scan.xs_temp_snap {
        unported("snapmgr UnregisterSnapshot (backend/utils/time/snapmgr)");
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
        #[cfg(test)]
        IndexAmKind::Mock => Ok(true),
        #[allow(unreachable_patterns)]
        _ => mock_outside_tests(),
    }
}

#[allow(unused_variables)]
fn am_insert_cleanup(kind: IndexAmKind, indexRelation: &Relation<'_>) -> PgResult<()> {
    match kind {
        IndexAmKind::Btree => unported("nbtree aminsertcleanup (insert lane is phase 2)"),
        IndexAmKind::Hash => unreachable!("hash lacks aminsertcleanup (guarded)"),
        IndexAmKind::Gin => unreachable!("gin lacks aminsertcleanup (guarded)"),
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
            xs_want_itup: false,
            xs_itup: None,
            xs_itupdesc: None,
            xs_temp_snap: false,
            kill_prior_tuple: false,
            ignore_killed_tuples: true,
            xactStartedInRecovery: false,
            opaque: IndexScanOpaque::Mock(MockOpaque::default()),
            xs_heaptid: ItemPointerData::invalid(),
            xs_heap_continue: false,
            xs_heapfetch: None,
            xs_recheck: false,
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
