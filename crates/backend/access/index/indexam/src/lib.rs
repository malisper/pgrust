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
//! The open index/heap relation crosses as a [`rel::Relation`] handle.
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
use types_tuple::heaptuple::Datum as DatumV;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INTERNAL_ERROR,
    ERRCODE_WRONG_OBJECT_TYPE};
use rel::Relation;
use types_scan::sdir::ScanDirection;
use types_scan::scankey::ScanKeyData;
use snapshot::snapshot::{IsMVCCSnapshot, SnapshotData};
use types_storage::lock::{LOCKMODE, NoLock};
use types_tableam::amapi::{IndexAmRoutine, IndexUniqueCheck, TIDBitmap};
use types_tableam::index_info_carrier::IndexInfoCarrier;
use types_tableam::genam::{
    IndexBulkDeleteResult, IndexOrderByDistance, IndexScanInstrumentation, IndexVacuumInfo,
    SharedIndexScanInstrumentation,
};
use types_tableam::relscan::{
    IndexScanDesc, IndexScanDescData, ParallelIndexScanDescData, ParallelIndexScanDescHandle,
    PARALLEL_INDEX_SCAN_DESC_HEADER_SIZE,
};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{AttrNumber, InvalidOid, Oid, RegProcedure};
use types_tuple::access::{RELKIND_INDEX, RELKIND_PARTITIONED_INDEX};
use types_tuple::heaptuple::ItemPointerData;

use table_tableam as tableam;
use index_seams as catalog_index;
use predicate_seams as predicate;
use pgstat_seams as pgstat;
use relcache_seams as relcache;
use snapmgr_seams as snapmgr;

/// Install this crate's seam implementations: every seam declared in
/// `backend-access-index-indexam-seams`.
pub fn init_seams() {
    use indexam_seams as seams;
    seams::index_open::set(index_open);
    seams::try_index_open::set(try_index_open);
    seams::index_getprocinfo::set(index_getprocinfo);
    seams::index_getprocid::set(index_getprocid);
    seams::index_opclass_options::set(index_opclass_options);
    seams::index_insert::set(index_insert);

    // Scan lifecycle + retrieval seams. The seam decls carry node-/SlotId-shaped
    // params (so the executor consumers barely change); each `seam_*` wrapper
    // adapts to the C-faithful `index_*` implementation above.
    seams::index_beginscan::set(seam_index_beginscan);
    seams::index_beginscan_bitmap::set(seam_index_beginscan_bitmap);
    seams::index_beginscan_parallel::set(seam_index_beginscan_parallel);
    seams::index_rescan::set(seam_index_rescan_ios);
    seams::index_rescan_is::set(seam_index_rescan_is);
    seams::index_rescan_bis::set(seam_index_rescan_bis);
    seams::index_endscan::set(seam_index_endscan);
    seams::index_markpos::set(seam_index_markpos);
    seams::index_restrpos::set(seam_index_restrpos);
    seams::index_getnext_tid::set(seam_index_getnext_tid);
    seams::index_fetch_heap::set(seam_index_fetch_heap);
    seams::index_getnext_slot::set(seam_index_getnext_slot);
    seams::get_actual_variable_endpoint::set(get_actual_variable_endpoint);
    seams::index_getbitmap::set(seam_index_getbitmap);
    seams::index_parallelscan_estimate::set(seam_index_parallelscan_estimate);
    seams::index_parallelscan_initialize::set(seam_index_parallelscan_initialize);
    seams::index_parallelrescan::set(seam_index_parallelrescan);
    seams::index_scan_resolve_shared_info::set(seam_index_scan_resolve_shared_info);
    seams::bt_resolve_parallel_scan::set(seam_bt_resolve_parallel_scan);

    // AM-vacuum dispatch consumed by vacuum.c (`vac_bulkdel_one_index` /
    // `vac_cleanup_one_index`). These seams are declared by the vacuum owner
    // (`backend-commands-vacuum-seams`) but their bodies are indexam's
    // `index_bulk_delete` / `index_vacuum_cleanup` — so indexam installs them,
    // adapting the Oid-shaped `vacuumparallel::IndexVacuumInfo` (which crosses
    // the seam) to the Relation-shaped `genam::IndexVacuumInfo` the AM wants.
    {
        use vacuum_seams as vac;
        vac::index_bulk_delete::set(seam_vac_index_bulk_delete);
        vac::index_vacuum_cleanup::set(seam_vac_index_vacuum_cleanup);
    }

    // ANALYZE-only index cleanup (`do_analyze_rel`'s `index_vacuum_cleanup`
    // call with `ivinfo.analyze_only == true`). The analyze owner declares the
    // seam (`backend-commands-analyze-rt-seams`) but cannot install it — its
    // body is indexam's `index_vacuum_cleanup`, which lives here. indexam owns
    // and installs it, mirroring the AM-vacuum dispatch above.
    analyze_rt_seams::index_vacuum_cleanup_analyze::set(
        seam_analyze_index_vacuum_cleanup,
    );
}

/// Build the Relation-shaped `genam::IndexVacuumInfo<'mcx>` the AM dispatch
/// expects from the Oid-shaped `vacuumparallel::IndexVacuumInfo` that crosses
/// the vacuum seam. The index and heap relations are already open and locked by
/// `vac_open_indexes` (RowExclusiveLock held for the duration of the vacuum), so
/// re-opening with `NoLock` just fetches the live relcache entries.
fn build_genam_ivinfo<'mcx>(
    mcx: Mcx<'mcx>,
    ivinfo: &types_vacuum::vacuumparallel::IndexVacuumInfo,
) -> PgResult<IndexVacuumInfo<'mcx>> {
    let index = index_open(mcx, ivinfo.index, NoLock)?;
    let heaprel =
        common_relation_seams::relation_open::call(mcx, ivinfo.heaprel, NoLock)?;
    Ok(IndexVacuumInfo {
        index,
        heaprel,
        analyze_only: ivinfo.analyze_only,
        report_progress: ivinfo.report_progress,
        estimated_count: ivinfo.estimated_count,
        message_level: ivinfo.message_level,
        num_heap_tuples: ivinfo.num_heap_tuples,
        // The access strategy lives in the vacuum substrate (carried by handle);
        // the AM-vacuum bodies don't consult it through this struct.
        strategy: None,
    })
}

/// `index_bulk_delete` seam body (vacuum owner's decl). The seam is mcx-free
/// (like vacuum's own `index_open` body), so a short-lived context is created to
/// hold the re-opened relations for the duration of the AM call. The vacuum side
/// passes the dead-items `TidStore` as the callback state; the AM consults
/// membership through the `vacuum_tid_is_dead` callback keyed by the store's
/// `id`, so the `callback_state` handle here is exactly that `id`.
fn seam_vac_index_bulk_delete(
    ivinfo: types_vacuum::vacuumparallel::IndexVacuumInfo,
    istat: Option<IndexBulkDeleteResult>,
    dead_items: types_vacuum::vacuumlazy::TidStore,
) -> PgResult<IndexBulkDeleteResult> {
    let cx = mcx::MemoryContext::new("index_bulk_delete");
    let mcx = cx.mcx();
    let info = build_genam_ivinfo(mcx, &ivinfo)?;
    let res = index_bulk_delete(mcx, &info, istat, Some(dead_items.id))?;
    // The AM (btbulkdelete) always returns stats; mirror C's non-NULL result.
    Ok(res.unwrap_or_default())
}

/// `index_vacuum_cleanup` seam body (vacuum owner's decl).
fn seam_vac_index_vacuum_cleanup(
    ivinfo: types_vacuum::vacuumparallel::IndexVacuumInfo,
    istat: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    let cx = mcx::MemoryContext::new("index_vacuum_cleanup");
    let mcx = cx.mcx();
    let info = build_genam_ivinfo(mcx, &ivinfo)?;
    index_vacuum_cleanup(mcx, &info, istat)
}

/// `index_vacuum_cleanup_analyze` seam body (analyze owner's decl). Mirrors the
/// `do_analyze_rel` call site (analyze.c:714-726): for one index of the
/// just-analyzed relation, build the ANALYZE-only `IndexVacuumInfo` and let the
/// AM do post-analyze cleanup via `amvacuumcleanup` (a no-op for every core AM
/// except GIN). The C passes `stats = NULL` (no prior bulk-delete result) and
/// `pfree`s any returned stats; the owned model drops the `Option` return.
///
/// The `index` / `heaprel` relations are already open and locked by analyze's
/// `vac_open_indexes` (RowExclusiveLock held for the duration of ANALYZE), so
/// re-opening by OID with `NoLock` just fetches the live relcache entries — the
/// same posture `build_genam_ivinfo` uses for the vacuum path. The fixed
/// `ivinfo` fields (`analyze_only = true`, `estimated_count = true`, and the
/// `message_level` / `num_heap_tuples` carried across the seam) match the C
/// call site verbatim; `vac_strategy` is the vacuum substrate's access strategy
/// (not consulted by the AM cleanup bodies through this struct, so `None`).
fn seam_analyze_index_vacuum_cleanup<'mcx>(
    index: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    message_level: i32,
    num_heap_tuples: f64,
) -> PgResult<()> {
    let cx = mcx::MemoryContext::new("index_vacuum_cleanup_analyze");
    let mcx = cx.mcx();
    let index_rel = index_open(mcx, index.rd_id, NoLock)?;
    let heap_rel =
        common_relation_seams::relation_open::call(mcx, heaprel.rd_id, NoLock)?;
    let info = IndexVacuumInfo {
        index: index_rel,
        heaprel: heap_rel,
        analyze_only: true,
        report_progress: false,
        estimated_count: true,
        message_level,
        num_heap_tuples,
        strategy: None,
    };
    // C: stats = index_vacuum_cleanup(&ivinfo, NULL); if (stats) pfree(stats);
    let _stats = index_vacuum_cleanup(mcx, &info, None)?;
    Ok(())
}

// ===========================================================================
// Seam wrappers — adapt the node-/SlotId-shaped seam decls (consumer-friendly)
// to the C-faithful `index_*` implementations above.
// ===========================================================================

/// `index_beginscan` seam wrapper: the consumer passes the node-driven
/// snapshot (`Option<Rc<SnapshotData>>`) and instrument by value; the C-faithful
/// `index_beginscan` takes a `SnapshotData` value and `Option<IndexScanInstrumentation>`.
fn seam_index_beginscan<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relation: Relation<'mcx>,
    index_relation: Relation<'mcx>,
    snapshot: Option<std::rc::Rc<SnapshotData>>,
    instrument: IndexScanInstrumentation,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDesc<'mcx>> {
    let snapshot = snapshot
        .map(|rc| (*rc).clone())
        .expect("index_beginscan requires a snapshot (C Assert(snapshot != InvalidSnapshot))");
    index_beginscan(
        mcx,
        &heap_relation,
        &index_relation,
        snapshot,
        Some(instrument),
        nkeys,
        norderbys,
    )
}

/// `index_beginscan_bitmap` seam wrapper.
fn seam_index_beginscan_bitmap<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: Relation<'mcx>,
    snapshot: Option<std::rc::Rc<SnapshotData>>,
    instrument: IndexScanInstrumentation,
    nkeys: i32,
) -> PgResult<IndexScanDesc<'mcx>> {
    let snapshot = snapshot
        .map(|rc| (*rc).clone())
        .expect("index_beginscan_bitmap requires a snapshot (C Assert(snapshot != InvalidSnapshot))");
    index_beginscan_bitmap(mcx, &index_relation, snapshot, Some(instrument), nkeys)
}

/// `index_beginscan_parallel` seam wrapper. The seam passes the `Copy` in-DSM
/// pointer handle (C's bare `ParallelIndexScanDesc`); no per-process copy — the
/// scan attaches to the SAME shared descriptor in leader and every worker.
fn seam_index_beginscan_parallel<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relation: Relation<'mcx>,
    index_relation: Relation<'mcx>,
    instrument: IndexScanInstrumentation,
    nkeys: i32,
    norderbys: i32,
    pscan: nodes::nodeindexonlyscan::ParallelIndexScanDesc<'mcx>,
) -> PgResult<IndexScanDesc<'mcx>> {
    index_beginscan_parallel(
        mcx,
        &heap_relation,
        &index_relation,
        Some(instrument),
        nkeys,
        norderbys,
        pscan,
    )
}

/// `index_rescan` (index-only scan node) seam wrapper: read the node's
/// `ioss_ScanKeys`/`ioss_OrderByKeys` + counts, then drive the C-faithful rescan.
fn seam_index_rescan_ios<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut nodes::IndexOnlyScanState<'mcx>,
) -> PgResult<()> {
    // Clone the key arrays so the `&mut ioss_ScanDesc` borrow is disjoint from
    // the `&ioss_ScanKeys`/`&ioss_OrderByKeys` reads.
    let keys: Vec<ScanKeyData> = node.ioss_ScanKeys.iter().cloned().collect();
    let nkeys = node.ioss_NumScanKeys;
    let orderbys: Vec<ScanKeyData> = node.ioss_OrderByKeys.iter().cloned().collect();
    let norderbys = node.ioss_NumOrderByKeys;
    let scan = node
        .ioss_ScanDesc
        .as_mut()
        .expect("index_rescan: ioss_ScanDesc not set (C would dereference NULL)");
    index_rescan(mcx, scan, &keys, nkeys, &orderbys, norderbys)
}

/// `index_rescan` (plain index scan node) seam wrapper: `iss_*` arrays.
fn seam_index_rescan_is<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut nodes::IndexScanState<'mcx>,
) -> PgResult<()> {
    let keys: Vec<ScanKeyData> = node.iss_ScanKeys.iter().cloned().collect();
    let nkeys = node.iss_NumScanKeys;
    let orderbys: Vec<ScanKeyData> = node.iss_OrderByKeys.iter().cloned().collect();
    let norderbys = node.iss_NumOrderByKeys;
    let scan = node
        .iss_ScanDesc
        .as_mut()
        .expect("index_rescan: iss_ScanDesc not set (C would dereference NULL)");
    index_rescan(mcx, scan, &keys, nkeys, &orderbys, norderbys)
}

/// `index_rescan` (bitmap index scan node) seam wrapper: `biss_ScanKeys` +
/// empty order-bys (C `NULL, 0`).
fn seam_index_rescan_bis<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut nodes::nodebitmapindexscan::BitmapIndexScanState<'mcx>,
) -> PgResult<()> {
    let keys: Vec<ScanKeyData> = node.biss_ScanKeys.iter().cloned().collect();
    let nkeys = node.biss_NumScanKeys;
    let scan = node
        .biss_ScanDesc
        .as_mut()
        .expect("index_rescan: biss_ScanDesc not set (C would dereference NULL)");
    index_rescan(mcx, scan, &keys, nkeys, &[], 0)
}

/// `index_endscan` seam wrapper.
fn seam_index_endscan<'mcx>(mcx: Mcx<'mcx>, scan: IndexScanDesc<'mcx>) -> PgResult<()> {
    index_endscan(mcx, scan)
}

/// `index_markpos` seam wrapper.
fn seam_index_markpos<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
) -> PgResult<()> {
    index_markpos(mcx, scan)
}

/// `index_restrpos` seam wrapper.
fn seam_index_restrpos<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
) -> PgResult<()> {
    index_restrpos(mcx, scan)
}

/// `index_getnext_tid` seam wrapper.
fn seam_index_getnext_tid<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    direction: ScanDirection,
) -> PgResult<Option<ItemPointerData>> {
    index_getnext_tid(mcx, scan, direction)
}

/// `index_fetch_heap` seam wrapper: resolve `mcx`/`slot` from the EState pool.
fn seam_index_fetch_heap<'mcx>(
    scan: &mut IndexScanDescData<'mcx>,
    estate: &mut nodes::EStateData<'mcx>,
    slot: nodes::SlotId,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let slot = estate.slot_data_mut(slot);
    index_fetch_heap(mcx, scan, slot)
}

/// `index_getnext_slot` seam wrapper: resolve `mcx`/`slot` from the EState pool.
fn seam_index_getnext_slot<'mcx>(
    scan: &mut IndexScanDescData<'mcx>,
    direction: ScanDirection,
    estate: &mut nodes::EStateData<'mcx>,
    slot: nodes::SlotId,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let slot = estate.slot_data_mut(slot);
    index_getnext_slot(mcx, scan, direction, slot)
}

/// `index_getbitmap` seam wrapper. The seam carries the concrete
/// `tidbitmap::TIDBitmap`; the C-faithful impl forwards the payload-erased
/// `types_tableam::amapi::TIDBitmap` to the AM. Round-trip the concrete bitmap
/// through the erased carrier.
fn seam_index_getbitmap<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    bitmap: &mut tidbitmap::TIDBitmap,
) -> PgResult<i64> {
    let owned = core::mem::take(bitmap);
    let mut am = TIDBitmap {
        payload: Some(std::boxed::Box::new(owned)),
    };
    let r = index_getbitmap(mcx, scan, &mut am)?;
    *bitmap = *am
        .payload
        .expect("index_getbitmap: AM dropped the TIDBitmap payload")
        .downcast::<tidbitmap::TIDBitmap>()
        .expect("index_getbitmap: AM returned a foreign TIDBitmap payload");
    Ok(r)
}

/// `index_parallelscan_estimate` seam wrapper: unwrap the node's
/// `Option<Rc<SnapshotData>>` to a `&SnapshotData`.
fn seam_index_parallelscan_estimate<'mcx>(
    index_relation: Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
    snapshot: Option<std::rc::Rc<SnapshotData>>,
    instrument: bool,
    parallel_aware: bool,
    nworkers: i32,
) -> PgResult<usize> {
    let snapshot = snapshot
        .map(|rc| (*rc).clone())
        .expect("index_parallelscan_estimate requires a snapshot");
    index_parallelscan_estimate(
        &index_relation,
        nkeys,
        norderbys,
        &snapshot,
        instrument,
        parallel_aware,
        nworkers,
    )
}

/// `index_parallelscan_initialize` seam wrapper: initialize the descriptor IN
/// PLACE at the in-DSM chunk base `target_addr` and return the `Copy` handle.
fn seam_index_parallelscan_initialize<'mcx>(
    _mcx: Mcx<'mcx>,
    heap_relation: Relation<'mcx>,
    index_relation: Relation<'mcx>,
    snapshot: Option<std::rc::Rc<SnapshotData>>,
    instrument: bool,
    parallel_aware: bool,
    nworkers: i32,
    target_addr: usize,
) -> PgResult<nodes::nodeindexonlyscan::ParallelIndexScanDesc<'mcx>> {
    let snapshot = snapshot
        .map(|rc| (*rc).clone())
        .expect("index_parallelscan_initialize requires a snapshot");
    // SAFETY: `target_addr` is the executor's freshly-`shm_toc_allocate`'d chunk
    // base, sized by `index_parallelscan_estimate` with the same arguments, not
    // yet aliased by any worker (the leader is the sole writer pre-launch).
    unsafe {
        index_parallelscan_initialize(
            &heap_relation,
            &index_relation,
            &snapshot,
            instrument,
            parallel_aware,
            nworkers,
            target_addr,
        )
    }
}

/// `index_parallelrescan` seam wrapper.
fn seam_index_parallelrescan<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
) -> PgResult<()> {
    index_parallelrescan(mcx, scan)
}

/// `index_scan_resolve_shared_info` seam wrapper —
/// `(SharedIndexScanInstrumentation *) OffsetToPointer(piscan, piscan->ps_offset_ins)`
/// (nodeIndexscan.c / nodeIndexonlyscan.c `Exec*ScanInitializeWorker`).
///
/// In C the worker resolves the `SharedIndexScanInstrumentation` that the
/// leader's `index_parallelscan_initialize` memset/initialized inside the
/// DSM-resident `ParallelIndexScanDesc` chunk at byte offset `ps_offset_ins`.
/// This reads `num_workers` + the `IndexScanInstrumentation[num_workers]` tail
/// straight out of the in-chunk region (`OffsetToPointer(piscan,
/// ps_offset_ins)`) into an owned struct the worker node holds.
fn seam_index_scan_resolve_shared_info(
    piscan: ParallelIndexScanDescHandle,
) -> PgResult<SharedIndexScanInstrumentation> {
    let off = piscan.ps_offset_ins();
    if off == 0 {
        return Err(PgError::error(
            "index parallel scan has no shared instrumentation region".to_string(),
        )
        .with_sqlstate(ERRCODE_INTERNAL_ERROR));
    }
    let base = piscan.base_addr() + off;
    // SAFETY: `base` is the real in-segment address of the leader-zeroed
    // `SharedIndexScanInstrumentation` region (`int num_workers` + the FAM of
    // `IndexScanInstrumentation`), written once before the launch barrier and
    // read-only here; the chunk was sized for `num_workers` slots.
    unsafe {
        let num_workers = core::ptr::read(base as *const i32);
        let win_base = (base + shared_index_scan_instrumentation_header_size())
            as *const IndexScanInstrumentation;
        let mut winstrument = Vec::with_capacity(num_workers.max(0) as usize);
        for i in 0..num_workers.max(0) as usize {
            winstrument.push(core::ptr::read(win_base.add(i)));
        }
        Ok(SharedIndexScanInstrumentation {
            num_workers,
            winstrument,
        })
    }
}

/// `bt_resolve_parallel_scan(parallel_handle)` seam wrapper —
/// `(BTParallelScanDesc) OffsetToPointer(parallel_scan, parallel_scan->ps_offset_am)`.
///
/// `parallel_handle` is the in-DSM base address of the `ParallelIndexScanDesc`
/// chunk (the same shared address in leader and every worker, returned by
/// `shm_toc_allocate` / `shm_toc_lookup`). Reading `ps_offset_am` from the
/// in-chunk header and adding it yields the in-DSM address of the AM-specific
/// `BTParallelScanDescData` tail — the exact pointer C's macro computes. The
/// nbtree state machine dereferences it under the tail's embedded `btps_lock`.
fn seam_bt_resolve_parallel_scan(parallel_handle: u64) -> *mut types_nbtree::BTParallelScanDescData {
    // SAFETY: `parallel_handle` is the real in-segment base of a leader-
    // initialized `ParallelIndexScanDescData` live for the DSM segment; the
    // handle exposes its `ps_offset_am` field (write-once-pre-launch).
    let base = parallel_handle as usize;
    let handle = unsafe { ParallelIndexScanDescHandle::from_raw(base) };
    let amtarget = base + handle.ps_offset_am();
    amtarget as *mut types_nbtree::BTParallelScanDescData
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
    let r = common_relation_seams::relation_open::call(mcx, relation_id, lockmode)?;
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
    let r = common_relation_seams::try_relation_open::call(mcx, relation_id, lockmode)?;

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
pub fn index_insert<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    values: &[DatumV<'mcx>],
    isnull: &[bool],
    heap_t_ctid: &ItemPointerData,
    heap_relation: &Relation<'mcx>,
    check_unique: IndexUniqueCheck,
    index_unchanged: bool,
    index_info: &mut IndexInfoCarrier<'_, 'mcx>,
) -> PgResult<bool> {
    relation_checks(index_relation)?;
    let am = indam(index_relation);
    // CHECK_REL_PROCEDURE(aminsert): aminsert is a required (non-Option)
    // callback in the vtable.

    if !am.ampredlocks {
        // CheckForSerializableConflictIn(indexRelation, NULL, InvalidBlockNumber)
        predicate::check_for_serializable_conflict_in::call(
            index_relation.rd_id,
            None,
            types_core::primitive::InvalidBlockNumber,
        )?;
    }

    (am.aminsert)(
        mcx,
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
pub fn index_insert_cleanup<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    index_info: &mut IndexInfoCarrier<'_, 'mcx>,
) -> PgResult<()> {
    relation_checks(index_relation)?;

    if let Some(aminsertcleanup) = indam(index_relation).aminsertcleanup {
        aminsertcleanup(mcx, index_relation, index_info)?;
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
        index_beginscan_internal(mcx, index_relation, nkeys, norderbys, snapshot.clone(), None, false)?;

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
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    snapshot: SnapshotData,
    instrument: Option<IndexScanInstrumentation>,
    nkeys: i32,
) -> PgResult<IndexScanDesc<'mcx>> {
    // Assert(snapshot != InvalidSnapshot).

    let mut scan =
        index_beginscan_internal(mcx, index_relation, nkeys, 0, snapshot.clone(), None, false)?;

    // scan->xs_snapshot = snapshot; scan->instrument = instrument; (no heap rel)
    scan.xs_snapshot = Some(snapshot);
    scan.instrument = instrument;

    Ok(scan)
}

/// `index_beginscan_internal` — common code for the begin variants.
fn index_beginscan_internal<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
    snapshot: SnapshotData,
    pscan: Option<ParallelIndexScanDescHandle>,
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
    let mut scan = (am.ambeginscan)(mcx, index_relation, nkeys, norderbys)?;

    // Initialize information for parallel scan.
    scan.parallel_scan = pscan;
    scan.xs_temp_snap = temp_snap;

    Ok(scan)
}

/// `index_rescan(scan, keys, nkeys, orderbys, norderbys)` — (re)start a scan.
/// The key counts must equal what `index_beginscan` was told. To restart
/// without changing keys, pass empty key arrays (the C `NULL`).
pub fn index_rescan<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    keys: &[ScanKeyData<'mcx>],
    nkeys: i32,
    orderbys: &[ScanKeyData<'mcx>],
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

    (am.amrescan)(mcx, scan, keys, orderbys)
}

/// `index_endscan(scan)` — end a scan.
pub fn index_endscan<'mcx>(mcx: Mcx<'mcx>, mut scan: IndexScanDesc<'mcx>) -> PgResult<()> {
    // SCAN_CHECKS + CHECK_SCAN_PROCEDURE(amendscan): amendscan is required.
    let am = indam(&scan.index_relation);

    // Release resources (like buffer pins) from table accesses, then NULL the
    // heap-fetch pointer.
    if let Some(heapfetch) = scan.xs_heapfetch.take() {
        tableam::table_index_fetch_end(heapfetch)?;
    }

    // End the AM's scan.
    (am.amendscan)(mcx, &mut scan)?;
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
pub fn index_markpos<'mcx>(mcx: Mcx<'mcx>, scan: &mut IndexScanDescData<'mcx>) -> PgResult<()> {
    // CHECK_SCAN_PROCEDURE(ammarkpos): optional callback, error if absent.
    let am = indam(&scan.index_relation);
    let ammarkpos = check_scan_procedure(am.ammarkpos, "ammarkpos", &scan.index_relation)?;
    ammarkpos(mcx, scan)
}

/// `index_restrpos(scan)` — restore a scan position. Only restores the index
/// AM's internal state (see C comments on HOT chains + MVCC snapshots).
pub fn index_restrpos<'mcx>(mcx: Mcx<'mcx>, scan: &mut IndexScanDescData<'mcx>) -> PgResult<()> {
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

    amrestrpos(mcx, scan)
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
    // The flat `#[repr(C)]` header size (== `offsetof(.., ps_snapshot_data)`,
    // where the in-chunk serialized-snapshot tail begins).
    PARALLEL_INDEX_SCAN_DESC_HEADER_SIZE
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
/// `ParallelIndexScanDesc` proper and the AM-specific info following it, IN
/// PLACE at the `shm_toc_allocate`'d chunk whose real in-segment base address is
/// `target_addr` (the C `ParallelIndexScanDesc target` pointer). Call once in
/// the leader; workers then attach via [`index_beginscan_parallel`].
///
/// This is the C body verbatim: it writes the flat header at `target`,
/// serializes the snapshot into the in-chunk `ps_snapshot_data` tail, zeroes the
/// `SharedIndexScanInstrumentation` region at `OffsetToPointer(target,
/// ps_offset_ins)`, and calls `aminitparallelscan(OffsetToPointer(target,
/// ps_offset_am))` so the AM places its `repr(C)` shared state IN the chunk. The
/// leader is the sole writer pre-launch, so the unique writes through `*mut` are
/// valid. Returns the `Copy` in-DSM handle.
///
/// # Safety
/// `target_addr` must be the real in-segment base of a chunk sized by
/// [`index_parallelscan_estimate`] with the same arguments, freshly allocated
/// and not yet aliased by any worker.
pub unsafe fn index_parallelscan_initialize(
    heap_relation: &Relation<'_>,
    index_relation: &Relation<'_>,
    snapshot: &SnapshotData,
    instrument: bool,
    parallel_aware: bool,
    nworkers: i32,
    target_addr: usize,
) -> PgResult<ParallelIndexScanDescHandle> {
    debug_assert!(instrument || parallel_aware);

    relation_checks(index_relation)?;

    let mut offset = add_size(
        parallel_index_scan_desc_header_size(),
        snapmgr::estimate_snapshot_space::call(snapshot),
    );
    offset = maxalign(offset);

    // target->ps_locator = ...; target->ps_indexlocator = ...; offsets = 0.
    //
    // SAFETY: `target_addr` is the leader's freshly-`shm_toc_allocate`'d,
    // not-yet-aliased chunk base (the function's safety contract); the leader is
    // the sole writer pre-launch, so this unique `&mut` over the header bytes is
    // valid.
    let target = unsafe { &mut *(target_addr as *mut ParallelIndexScanDescData) };
    target.ps_locator = heap_relation.rd_locator;
    target.ps_indexlocator = index_relation.rd_locator;
    target.ps_offset_ins = 0;
    target.ps_offset_am = 0;

    // SerializeSnapshot(snapshot, target->ps_snapshot_data) — into the in-chunk
    // tail immediately after the header.
    let snap_bytes = snapmgr::serialize_snapshot::call(snapshot)?;
    // SAFETY: the chunk was sized with `EstimateSnapshotSpace(snapshot)` past
    // the header (see `index_parallelscan_estimate`); the leader is the sole
    // writer.
    unsafe {
        let dst = (target_addr + parallel_index_scan_desc_header_size()) as *mut u8;
        core::ptr::copy_nonoverlapping(snap_bytes.as_ptr(), dst, snap_bytes.len());
    }

    if instrument {
        target.ps_offset_ins = offset;
        let sharedinfosz = add_size(
            shared_index_scan_instrumentation_header_size(),
            (nworkers as usize) * size_of_index_scan_instrumentation(),
        );
        offset = add_size(offset, sharedinfosz);
        offset = maxalign(offset);

        // *sharedinfo = OffsetToPointer(target, ps_offset_ins); memset(0);
        // (*sharedinfo)->num_workers = nworkers — written in place in the chunk.
        // SAFETY: chunk sized for `sharedinfosz` at `ps_offset_ins`; sole writer.
        unsafe {
            let ins = (target_addr + target.ps_offset_ins) as *mut u8;
            core::ptr::write_bytes(ins, 0, sharedinfosz);
            // num_workers is the first `i32` field of SharedIndexScanInstrumentation.
            core::ptr::write(ins as *mut i32, nworkers);
        }
    }

    // aminitparallelscan is optional; assume no-op if not provided by the AM.
    if parallel_aware {
        if let Some(aminitparallelscan) = indam(index_relation).aminitparallelscan {
            target.ps_offset_am = offset;
            // amtarget = OffsetToPointer(target, target->ps_offset_am).
            let amtarget = target_addr + target.ps_offset_am;
            aminitparallelscan(amtarget)?;
        }
    }

    // SAFETY: `target_addr` is the real in-segment base of the leader-initialized
    // descriptor.
    Ok(unsafe { ParallelIndexScanDescHandle::from_raw(target_addr) })
}

/// `index_parallelrescan(scan)` — (re)start a parallel scan of an index.
pub fn index_parallelrescan<'mcx>(mcx: Mcx<'mcx>, scan: &mut IndexScanDescData<'mcx>) -> PgResult<()> {
    // SCAN_CHECKS.
    if let Some(heapfetch) = scan.xs_heapfetch.as_deref_mut() {
        tableam::table_index_fetch_reset(heapfetch)?;
    }

    // amparallelrescan is optional; assume no-op if not provided by the AM.
    if let Some(amparallelrescan) = indam(&scan.index_relation).amparallelrescan {
        amparallelrescan(mcx, scan)?;
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
    pscan: ParallelIndexScanDescHandle,
) -> PgResult<IndexScanDesc<'mcx>> {
    // Assert(RelFileLocatorEquals(heaprel->rd_locator, pscan->ps_locator)) and
    // Assert(RelFileLocatorEquals(indexrel->rd_locator, pscan->ps_indexlocator)).
    debug_assert!(types_storage::RelFileLocatorEquals(
        &heaprel.rd_locator,
        &pscan.ps_locator()
    ));
    debug_assert!(types_storage::RelFileLocatorEquals(
        &indexrel.rd_locator,
        &pscan.ps_indexlocator()
    ));

    // snapshot = RestoreSnapshot(pscan->ps_snapshot_data); RegisterSnapshot(...).
    let restored = snapmgr::restore_snapshot::call(pscan.ps_snapshot_data())?;
    let snapshot = snapmgr::register_snapshot::call(restored)?;
    // scan = index_beginscan_internal(indexrel, nkeys, norderbys, snapshot,
    //                                 pscan, true) — `scan->parallel_scan = pscan`
    // is the SAME shared in-DSM pointer (NO copy).
    let mut scan =
        index_beginscan_internal(mcx, indexrel, nkeys, norderbys, snapshot.clone(), Some(pscan), true)?;

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
pub fn index_getnext_tid<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
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
    let found = amgettuple(mcx, scan, direction)?;

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

    pgstat::pgstat_count_index_tuples::call(
        scan.index_relation.rd_id,
        scan.index_relation.rd_rel.relisshared,
        scan.index_relation.pgstat_enabled,
        1,
    );

    // Return the TID of the tuple we found.
    Ok(Some(scan.xs_heaptid))
}

/// `index_fetch_heap(scan, slot)` — get the scan's next heap tuple for the
/// index TID most recently fetched by [`index_getnext_tid`]. On success the
/// slot holds a visible heap tuple (its buffer pinned by the table AM).
pub fn index_fetch_heap<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    slot: &mut nodes::tuptable::SlotData<'mcx>,
) -> PgResult<bool> {
    let mut all_dead = false;

    // table_index_fetch_tuple(scan->xs_heapfetch, &scan->xs_heaptid,
    //   scan->xs_snapshot, slot, &scan->xs_heap_continue, &all_dead).
    //
    // The snapshot crosses by `&mut`, not a clone: C passes `scan->xs_snapshot`
    // by pointer, and for a dirty (non-MVCC) snapshot the visibility check
    // (HeapTupleSatisfiesDirty) writes the in-progress inserter/deleter's
    // xmin/xmax/speculativeToken back into it. The scan's owner
    // (`_bt_check_unique` / `check_exclusion_or_unique_constraint`) reads those
    // out of `scan->xs_snapshot` to decide whether to wait on the conflicting
    // xact. Cloning here (the previous behaviour) silently discarded the
    // write-back, so unique/exclusion conflict-wait never fired.
    // C passes `&scan->xs_heaptid` directly; the heap AM mutates it in place to
    // the resolved live HOT-chain member's offset, and the next continuation
    // call resumes the walk from there. Disjoint field borrows force a local
    // copy here, so we write the (possibly mutated) tid back into the scan
    // afterward to preserve that effect.
    let mut heaptid = scan.xs_heaptid;
    let mut heap_continue = scan.xs_heap_continue;
    // Disjoint field borrows: `xs_snapshot` (&mut) and `xs_heapfetch` (&mut).
    let snapshot = &mut scan.xs_snapshot;
    let heapfetch = scan
        .xs_heapfetch
        .as_deref_mut()
        .expect("index_fetch_heap with no xs_heapfetch (C would dereference NULL)");
    let found = tableam::table_index_fetch_tuple(
        mcx,
        heapfetch,
        &mut heaptid,
        snapshot,
        slot,
        &mut heap_continue,
        Some(&mut all_dead),
    )?;
    scan.xs_heaptid = heaptid;
    scan.xs_heap_continue = heap_continue;

    if found {
        pgstat::pgstat_count_heap_fetch::call(
            scan.index_relation.rd_id,
            scan.index_relation.rd_rel.relisshared,
            scan.index_relation.pgstat_enabled,
        );
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
    slot: &mut nodes::tuptable::SlotData<'mcx>,
) -> PgResult<bool> {
    loop {
        if !scan.xs_heap_continue {
            // Time to fetch the next TID from the index.
            let tid = index_getnext_tid(mcx, scan, direction)?;

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

/// `get_actual_variable_endpoint(heapRel, indexRel, indexscandir, scankeys,
/// typLen, typByVal, tableslot, outercontext, &endpointDatum)` (selfuncs.c:6770)
/// — fetch one endpoint datum (min or max, per `indexscandir`) from the index's
/// first column, using the index-only-scan machinery under a transient
/// `SnapshotNonVacuumable`. Returns `Some(value)` on success (C `true` +
/// `*endpointDatum`) or `None` (C `false`: empty index, or gave up after too
/// many heap-page visits).
///
/// The bare-scan probe lives here because indexam owns the scan-descriptor
/// primitives (`index_beginscan`/`index_rescan`/`index_getnext_tid`/
/// `index_fetch_heap`); the driver that picks a suitable btree index and chooses
/// the scan direction (`get_actual_variable_range`) lives in the optimizer's
/// selfuncs unit and reaches this through the `get_actual_variable_endpoint`
/// seam. The scankey and table slot that C builds once in
/// `get_actual_variable_range` (to share across the min and max invocations) are
/// rebuilt per call here, since the seam is invoked once per endpoint — harmless
/// (the `IS NOT NULL` key and the slot are cheap) and keeps the cross-crate
/// surface minimal.
pub fn get_actual_variable_endpoint<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relation: Relation<'mcx>,
    index_relation: Relation<'mcx>,
    indexscandir: ScanDirection,
    typ_len: i16,
    typ_byval: bool,
) -> PgResult<Option<DatumV<'mcx>>> {
    use types_scan::scankey::{InvalidStrategy, ScanKeyData, SK_ISNULL, SK_SEARCHNOTNULL};
    use snapshot::snapshot::SnapshotType;
    use types_storage::{Buffer, InvalidBuffer};
    use types_core::primitive::{BlockNumber, InvalidBlockNumber};

    const VISITED_PAGES_LIMIT: i32 = 100;

    // build some stuff needed for indexscan execution (the C caller's job; here
    // per-call). table_slot_create(heapRel, NULL).
    let mut slot = tableam::table_slot_create(mcx, &heap_relation)?;

    // set up an IS NOT NULL scan key so that we ignore nulls
    // ScanKeyEntryInitialize(&scankeys[0], SK_ISNULL | SK_SEARCHNOTNULL, 1,
    //   InvalidStrategy, InvalidOid, InvalidOid, InvalidOid, (Datum) 0).
    let mut scankey = ScanKeyData::empty();
    scankey::ScanKeyEntryInitialize(
        &mut scankey,
        SK_ISNULL | SK_SEARCHNOTNULL,
        1, // index col to scan
        InvalidStrategy,
        InvalidOid, // no strategy subtype
        InvalidOid, // no collation
        InvalidOid, // no reg proc for this
        DatumV::ByVal(0),
    )?;
    let scankeys = [scankey];

    // InitNonVacuumableSnapshot(SnapshotNonVacuumable, GlobalVisTestFor(heapRel)).
    let vistest =
        vacuumlazy_seams::global_vis_test_for::call(&heap_relation)?;
    let mut snapshot = SnapshotData::sentinel(SnapshotType::SNAPSHOT_NON_VACUUMABLE);
    snapshot.vistest = vistest;

    // index_beginscan(heapRel, indexRel, &SnapshotNonVacuumable, NULL, 1, 0).
    let mut scan = index_beginscan(
        mcx,
        &heap_relation,
        &index_relation,
        snapshot,
        None,
        1,
        0,
    )?;

    // Set it up for index-only scan; index_rescan(scan, scankeys, 1, NULL, 0).
    scan.xs_want_itup = true;
    index_rescan(mcx, &mut scan, &scankeys, 1, &[], 0)?;

    let mut have_data = false;
    let mut result: Option<DatumV<'mcx>> = None;
    let mut vmbuffer: Buffer = InvalidBuffer;
    let mut last_heap_block: BlockNumber = InvalidBlockNumber;
    let mut n_visited_heap_pages: i32 = 0;

    // Fetch first/next tuple in specified direction.
    while let Some(tid) = index_getnext_tid(mcx, &mut scan, indexscandir)? {
        let block = tid.ip_blkid.block_number();

        // !VM_ALL_VISIBLE(heapRel, block, &vmbuffer)
        let (status, vmbuf) = visibilitymap_seams::visibilitymap_get_status::call(
            heap_relation.alias(),
            block,
            vmbuffer,
        )?;
        vmbuffer = vmbuf;
        let all_visible = status
            & visibilitymap_seams::VISIBILITYMAP_ALL_VISIBLE
            != 0;

        if !all_visible {
            // Rats, we have to visit the heap to check visibility.
            if !index_fetch_heap(mcx, &mut scan, &mut slot)? {
                // No visible tuple for this index entry; advance to the next.
                // Count heap-page fetches and give up if we've done too many.
                // We don't charge a page fetch if this is the same heap page as
                // the previous tuple.
                if block != last_heap_block {
                    last_heap_block = block;
                    n_visited_heap_pages += 1;
                    if n_visited_heap_pages > VISITED_PAGES_LIMIT {
                        break;
                    }
                }
                continue; // no visible tuple, try next index entry
            }
            // We don't actually need the heap tuple for anything; the slot's
            // contents (and its buffer pin) are released when the slot is
            // dropped below, since we break out of the loop right after reading
            // the index tuple. (C calls ExecClearTuple(tableslot) here.)
        }

        // We expect that the index will return data in IndexTuple not HeapTuple
        // format.
        let itup = scan.xs_itup.as_ref().ok_or_else(|| {
            PgError::error("no data returned for index-only scan".to_string())
                .with_sqlstate(ERRCODE_INTERNAL_ERROR)
        })?;

        // We do not yet support recheck here.
        if scan.xs_recheck {
            break;
        }

        // OK to deconstruct the index tuple.
        let itupdesc = scan.xs_itupdesc.as_deref().ok_or_else(|| {
            PgError::error("index-only scan has no index tuple descriptor".to_string())
                .with_sqlstate(ERRCODE_INTERNAL_ERROR)
        })?;
        let columns = indextuple_seams::index_deform_tuple::call(
            mcx,
            itup.as_slice(),
            itupdesc,
        )?;

        // Shouldn't have got a null, but be careful.
        let (value0, isnull0) = &columns[0];
        if *isnull0 {
            return Err(PgError::error(format!(
                "found unexpected null value in index \"{}\"",
                index_relation.name()
            ))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR));
        }

        // Copy the index column value out to caller's context.
        // *endpointDatum = datumCopy(values[0], typByVal, typLen). In the arena
        // model the deformed value already carries its referent; clone_in places
        // it in the caller's `mcx` (C copies into `outercontext`).
        let _ = (typ_byval, typ_len); // describe the value's storage; modeled by DatumV.
        result = Some(value0.clone_in(mcx)?);
        have_data = true;
        break;
    }

    if vmbuffer != InvalidBuffer {
        bufmgr_seams::release_buffer::call(vmbuffer);
    }
    index_endscan(mcx, scan)?;

    // Clean up the standalone table slot.
    execTuples_seams::exec_drop_single_tuple_table_slot::call(slot)?;

    Ok(if have_data { result } else { None })
}

/// `index_getbitmap(scan, bitmap)` — add the TIDs of all heap tuples
/// satisfying the scan keys to a bitmap; returns the (possibly approximate)
/// match count.
pub fn index_getbitmap<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    bitmap: &mut TIDBitmap,
) -> PgResult<i64> {
    // CHECK_SCAN_PROCEDURE(amgetbitmap): optional callback, error if absent.
    let am = indam(&scan.index_relation);
    let amgetbitmap = check_scan_procedure(am.amgetbitmap, "amgetbitmap", &scan.index_relation)?;

    // just make sure this is false...
    scan.kill_prior_tuple = false;

    // have the am's getbitmap proc do all the work.
    let ntids = amgetbitmap(mcx, scan, bitmap)?;

    pgstat::pgstat_count_index_tuples::call(
        scan.index_relation.rd_id,
        scan.index_relation.rd_rel.relisshared,
        scan.index_relation.pgstat_enabled,
        ntids,
    );

    Ok(ntids)
}

// ===========================================================================
// Bulk delete / vacuum cleanup
// ===========================================================================

/// `index_bulk_delete(info, istat, callback, callback_state)` — mass deletion
/// of index entries; `info->index` is the index relation. The deletion
/// callback + its state live in the vacuum substrate, so the whole AM call is
/// owned by the AM.
pub fn index_bulk_delete<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    istat: Option<IndexBulkDeleteResult>,
    callback_state: Option<u64>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    let index_relation = &info.index;
    relation_checks(index_relation)?;
    // CHECK_REL_PROCEDURE(ambulkdelete): ambulkdelete is a required callback.
    (indam(index_relation).ambulkdelete)(mcx, info, istat, callback_state)
}

/// `index_vacuum_cleanup(info, istat)` — post-deletion cleanup of an index.
pub fn index_vacuum_cleanup<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    istat: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    let index_relation = &info.index;
    relation_checks(index_relation)?;
    // CHECK_REL_PROCEDURE(amvacuumcleanup): required callback.
    (indam(index_relation).amvacuumcleanup)(mcx, info, istat)
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
        // C: `if (!PointerIsValid(DatumGetPointer(attoptions)))`. The unified
        // Datum carries the `(Datum) 0` SQL-NULL as `ByVal(0)` and a real
        // `text[]` attoptions image as `ByRef(..)`; `as_byref_word()` reads the
        // pointer word for both arms (0 for the null, the non-zero byte-image
        // address for a by-ref value) — `as_u64()` would panic on the by-ref
        // arm ("scalar accessor called on a by-reference value").
        if attoptions.as_byref_word() == 0 {
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
    reloptions_seams::index_build_local_reloptions::call(
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
