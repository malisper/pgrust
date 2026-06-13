//! Seam declarations for the `backend-access-index-indexam` unit
//! (`access/index/indexam.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use types_nodes::nodeindexonlyscan::{
    IndexScanDesc, IndexScanInstrumentation, ParallelIndexScanDesc,
};
use types_nodes::{IndexOnlyScanState, ParallelIndexScanDescData};
use types_scan::sdir::ScanDirection;
use types_tuple::heaptuple::ItemPointerData;

seam_core::seam!(
    /// `index_beginscan(heapRelation, indexRelation, snapshot, instrument,
    /// nkeys, norderbys)` (indexam.c): begin a scan of an index for the given
    /// relations and snapshot, returning a fresh `IndexScanDesc` allocated in
    /// `mcx`. The instrumentation counter is the node's `ioss_Instrument`
    /// (its initial value); `instrument` carries it in. Fallible on OOM /
    /// `ereport(ERROR)`.
    pub fn index_beginscan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        heap_relation: types_rel::Relation<'mcx>,
        index_relation: types_rel::Relation<'mcx>,
        snapshot: Option<std::rc::Rc<types_snapshot::SnapshotData>>,
        instrument: IndexScanInstrumentation,
        nkeys: i32,
        norderbys: i32,
    ) -> types_error::PgResult<IndexScanDesc<'mcx>>
);

seam_core::seam!(
    /// `index_beginscan_parallel(heaprel, indexrel, instrument, nkeys,
    /// norderbys, pscan)` (indexam.c): begin a parallel index scan attached to
    /// the shared `ParallelIndexScanDesc`. Fallible on OOM / `ereport(ERROR)`.
    pub fn index_beginscan_parallel<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        heap_relation: types_rel::Relation<'mcx>,
        index_relation: types_rel::Relation<'mcx>,
        instrument: IndexScanInstrumentation,
        nkeys: i32,
        norderbys: i32,
        pscan: ParallelIndexScanDesc<'mcx>,
    ) -> types_error::PgResult<IndexScanDesc<'mcx>>
);

seam_core::seam!(
    /// `index_rescan(scan, keys, nkeys, orderbys, norderbys)` (indexam.c):
    /// (re)start a scan with the given (possibly recomputed) scan keys. The
    /// owned model takes the node so the AM can read its `ioss_ScanKeys` /
    /// `ioss_OrderByKeys` arrays and `ioss_ScanDesc`. Fallible on
    /// `ereport(ERROR)`.
    pub fn index_rescan<'mcx>(
        node: &mut IndexOnlyScanState<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `index_getnext_tid(scan, direction)` (indexam.c): fetch the next TID in
    /// the given direction; `Ok(None)` at end of scan (the C `NULL`). Fills
    /// the scan descriptor's per-tuple result fields as a side effect.
    /// Fallible on `ereport(ERROR)`.
    pub fn index_getnext_tid<'mcx>(
        scan: &mut types_nodes::IndexScanDescData<'mcx>,
        direction: ScanDirection,
    ) -> types_error::PgResult<Option<ItemPointerData>>
);

seam_core::seam!(
    /// `index_fetch_heap(scan, slot)` (indexam.c): fetch the heap tuple the
    /// current TID points at into the given table slot (id into the EState
    /// pool), returning whether a visible tuple was found. Fallible on
    /// `ereport(ERROR)`; pins a heap buffer recorded in the scan descriptor.
    pub fn index_fetch_heap<'mcx>(
        scan: &mut types_nodes::IndexScanDescData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `index_endscan(scan)` (indexam.c): end an index scan, releasing its
    /// resources (pins, AM scan state). Fallible on `ereport(ERROR)`.
    pub fn index_endscan<'mcx>(
        scan: types_nodes::IndexScanDesc<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `index_markpos(scan)` (indexam.c): mark the current scan position.
    pub fn index_markpos<'mcx>(
        scan: &mut types_nodes::IndexScanDescData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `index_restrpos(scan)` (indexam.c): restore the marked scan position.
    pub fn index_restrpos<'mcx>(
        scan: &mut types_nodes::IndexScanDescData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `index_parallelscan_estimate(indexrel, nkeys, norderbys, snapshot,
    /// instrument, parallel_aware, nworkers)` (indexam.c): the DSM space the
    /// parallel index scan descriptor needs (`ioss_PscanLen`). Fallible on
    /// `ereport(ERROR)`.
    pub fn index_parallelscan_estimate<'mcx>(
        index_relation: types_rel::Relation<'mcx>,
        nkeys: i32,
        norderbys: i32,
        snapshot: Option<std::rc::Rc<types_snapshot::SnapshotData>>,
        instrument: bool,
        parallel_aware: bool,
        nworkers: i32,
    ) -> types_error::PgResult<usize>
);

seam_core::seam!(
    /// `index_parallelscan_initialize(heaprel, indexrel, snapshot, instrument,
    /// parallel_aware, nworkers, &sharedinfo, target)` (indexam.c): initialize
    /// the shared parallel index-scan descriptor in DSM, wiring its
    /// `SharedIndexScanInstrumentation` offset. Returns the initialized
    /// descriptor. Fallible on OOM / `ereport(ERROR)`.
    pub fn index_parallelscan_initialize<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        heap_relation: types_rel::Relation<'mcx>,
        index_relation: types_rel::Relation<'mcx>,
        snapshot: Option<std::rc::Rc<types_snapshot::SnapshotData>>,
        instrument: bool,
        parallel_aware: bool,
        nworkers: i32,
        target: ParallelIndexScanDescData,
    ) -> types_error::PgResult<ParallelIndexScanDesc<'mcx>>
);

seam_core::seam!(
    /// `index_parallelrescan(scan)` (indexam.c): reset shared parallel scan
    /// state before beginning a fresh scan. Fallible on `ereport(ERROR)`.
    pub fn index_parallelrescan<'mcx>(
        scan: &mut types_nodes::IndexScanDescData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `index_open(relationId, lockmode)` (indexam.c): open an index relation
    /// by OID — `relation_open` plus the not-an-index `ereport(ERROR)` check.
    /// The consumed slice of the relcache entry is copied into `mcx`. The
    /// owner installs the handle's closer, so `index_close(rel, lockmode)` is
    /// the returned handle's [`types_rel::Relation::close`] and drop is the
    /// abort-path `index_close(rel, NoLock)`.
    pub fn index_open<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation_id: types_core::Oid,
        lockmode: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<types_rel::Relation<'mcx>>
);

// === parallel btree scan DSM pointer resolution ============================
//
// The `BTParallelScanDescData` lives in the DSM region the parallel
// index-scan infrastructure (indexam.c `ParallelIndexScanDesc` +
// `OffsetToPointer(parallel_scan, ps_offset_am)`) sets up. The
// `_bt_parallel_*` *logic* (the LWLock-protected page-status state machine,
// the array serialize/restore, the init/rescan field writes) lives in the
// owning `backend-access-nbtree-nbtree` crate; only the DSM-pointer
// resolution itself is foreign and reached through this seam. Until the
// parallel index-scan infrastructure lands, the resolver panics loudly — a
// serial scan never reaches it.

seam_core::seam!(
    /// `(BTParallelScanDesc) OffsetToPointer(parallel_scan, parallel_scan->ps_offset_am)`
    /// — resolve the DSM handle for a parallel index scan to the AM-specific
    /// `BTParallelScanDescData` that lives within it. Returns the raw DSM
    /// pointer exactly as the C macro does; the nbtree state machine
    /// dereferences it under the descriptor's embedded LWLock.
    pub fn bt_resolve_parallel_scan(
        parallel_handle: u64,
    ) -> *mut types_nbtree::BTParallelScanDescData
);

seam_core::seam!(
    /// `(SharedIndexScanInstrumentation *) OffsetToPointer(piscan, piscan->ps_offset_ins)`
    /// (nodeIndexonlyscan.c `ExecIndexOnlyScanInitializeWorker`) — resolve the
    /// `SharedIndexScanInstrumentation` that `index_parallelscan_initialize`
    /// placed inside the DSM-resident `ParallelIndexScanDesc` blob, at offset
    /// `ps_offset_ins`. The blob layout / offset arithmetic into shared memory
    /// is owned by the parallel index-scan infrastructure (indexam/genam); the
    /// worker node only assigns the result to `node->ioss_SharedInfo`. The
    /// owned model returns the live shared struct. Fallible on `ereport(ERROR)`.
    pub fn index_scan_resolve_shared_info<'mcx>(
        piscan: &types_nodes::ParallelIndexScanDescData,
    ) -> types_error::PgResult<types_nodes::SharedIndexScanInstrumentation>
);
