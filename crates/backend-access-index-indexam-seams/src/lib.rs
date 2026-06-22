//! Seam declarations for the `backend-access-index-indexam` unit
//! (`access/index/indexam.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use types_nodes::nodeindexonlyscan::{
    IndexScanDesc, IndexScanInstrumentation, ParallelIndexScanDesc,
};
use types_nodes::IndexOnlyScanState;
use types_scan::sdir::ScanDirection;
use types_tuple::heaptuple::ItemPointerData;

seam_core::seam!(
    /// `get_actual_variable_endpoint(heapRel, indexRel, indexscandir, scankeys,
    /// typLen, typByVal, slot, outercontext, &endpointDatum)` (selfuncs.c:6770) —
    /// fetch one endpoint (min or max, per `indexscandir`) of an index's first
    /// column using the index-only-scan machinery under a transient
    /// `SnapshotNonVacuumable` snapshot. Returns the endpoint as its canonical
    /// value image (`Some`, C `true` + `*endpointDatum`) or `None` (C `false`:
    /// empty index, or gave up after visiting `VISITED_PAGES_LIMIT` heap pages).
    ///
    /// The driver logic (`get_actual_variable_range`, which picks a suitable
    /// btree index and translates the sortop into a scan direction) lives in the
    /// optimizer's selfuncs unit; only this bare-scan-descriptor probe — which
    /// drives `index_beginscan`/`index_rescan`/`index_getnext_tid`/
    /// `index_fetch_heap` directly on a raw `IndexScanDesc` (no executor
    /// plan-state node), builds the `IS NOT NULL` `ScanKeyData`, sets up the
    /// `SnapshotNonVacuumable` from `GlobalVisTestFor(heapRel)`, consults the
    /// visibility map, and deforms `xs_itup` — belongs to indexam, which owns
    /// those primitives. The owner installs it from `init_seams()`. `Err` carries
    /// the probe's `ereport(ERROR)` surface.
    pub fn get_actual_variable_endpoint<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        heap_relation: types_rel::Relation<'mcx>,
        index_relation: types_rel::Relation<'mcx>,
        indexscandir: ScanDirection,
        typ_len: i16,
        typ_byval: bool,
    ) -> types_error::PgResult<
        Option<types_tuple::backend_access_common_heaptuple::Datum<'mcx>>,
    >
);

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
    /// `index_insert(indexRelation, values, isnull, heap_t_ctid, heapRelation,
    /// checkUnique, indexUnchanged, indexInfo)` (indexam.c): insert one index
    /// tuple, dispatching to the index AM's `aminsert`. Returns the `aminsert`
    /// boolean. The `IndexInfoCarrier` carries the live executor
    /// `IndexInfo<'mcx>` for the AM. Reached cross-crate (the heap AM's
    /// `index_validate_scan` inserts missing entries during CREATE INDEX
    /// CONCURRENTLY's validation phase); owned by
    /// `backend-access-index-indexam`, which installs it from `init_seams()`.
    /// `Err` carries the AM's `ereport(ERROR)` (incl. unique violation) surface.
    #[allow(clippy::type_complexity)]
    pub fn index_insert<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        index_relation: &types_rel::Relation<'mcx>,
        values: &[types_tuple::backend_access_common_heaptuple::Datum<'mcx>],
        isnull: &[bool],
        heap_t_ctid: &types_tuple::heaptuple::ItemPointerData,
        heap_relation: &types_rel::Relation<'mcx>,
        check_unique: types_tableam::amapi::IndexUniqueCheck,
        index_unchanged: bool,
        index_info: &mut types_tableam::index_info_carrier::IndexInfoCarrier<'_, 'mcx>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `index_getprocinfo(irel, attnum, procnum)` (indexam.c): the cached fmgr
    /// lookup info for a support procedure (only the default functions are
    /// cached). The relcache owner holds + lazily initializes the
    /// `rd_supportinfo` cache; the procindex arithmetic + `procnum` range
    /// assert are indexam's logic, which this seam encapsulates. `Err` carries
    /// the `missing support function ...` ereport.
    pub fn index_getprocinfo<'mcx>(
        irel: &types_rel::Relation<'mcx>,
        attnum: types_core::primitive::AttrNumber,
        procnum: u16,
    ) -> types_error::PgResult<types_core::fmgr::FmgrInfo>
);

seam_core::seam!(
    /// `index_getprocid(irel, attnum, procnum)` (indexam.c): the requested
    /// default support-procedure OID for an indexed attribute, read from the
    /// relcache `rd_support` array (`InvalidOid` when the opclass does not define
    /// the optional procedure). Unlike [`index_getprocinfo`] it does not resolve
    /// or complain on a missing procedure — the BRIN inclusion opclass uses it to
    /// test for optional support procedures (`missing_ok`).
    pub fn index_getprocid<'mcx>(
        irel: &types_rel::Relation<'mcx>,
        attnum: types_core::primitive::AttrNumber,
        procnum: u16,
    ) -> types_error::PgResult<types_core::primitive::RegProcedure>
);

seam_core::seam!(
    /// `index_opclass_options(indrel, attnum, attoptions, validate)` (indexam.c):
    /// parse (and optionally validate) the opclass-specific per-column options
    /// for an index column, returning the packed `local_relopts` bytea image
    /// (`None` when the column has no options and the opclass defines no options
    /// support procedure). `attoptions` is the canonical attoptions `Datum`
    /// (`Datum::null()` ⇒ no options). Reached from `index_create` to validate a
    /// CREATE INDEX's per-column opclass options. The owner
    /// (`backend-access-index-indexam`) installs it from `init_seams()`; until
    /// then a call panics loudly. `Err` carries the opclass-options
    /// `ereport(ERROR)` surface (including the "operator class %s has no options"
    /// error).
    pub fn index_opclass_options<'mcx>(
        indrel: &types_rel::Relation<'mcx>,
        attnum: types_core::primitive::AttrNumber,
        attoptions: types_tuple::Datum<'mcx>,
        validate: bool,
    ) -> types_error::PgResult<std::option::Option<std::vec::Vec<u8>>>
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
        mcx: mcx::Mcx<'mcx>,
        node: &mut IndexOnlyScanState<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `index_rescan(node->iss_ScanDesc, node->iss_ScanKeys,
    /// node->iss_NumScanKeys, node->iss_OrderByKeys, node->iss_NumOrderByKeys)`
    /// (indexam.c), for a plain index scan node: restart the scan with the
    /// node's (possibly recomputed) scan + order-by keys. Fallible on
    /// `ereport(ERROR)`.
    pub fn index_rescan_is<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        node: &mut types_nodes::IndexScanState<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `index_beginscan_bitmap(indexRelation, snapshot, instrument, nkeys)`
    /// (indexam.c): begin a scan of an index for a bitmap index scan
    /// (`amgetbitmap`-style; no heap relation, no order-by keys), returning a
    /// fresh `IndexScanDesc` allocated in `mcx`. Fallible on OOM /
    /// `ereport(ERROR)`.
    pub fn index_beginscan_bitmap<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        index_relation: types_rel::Relation<'mcx>,
        snapshot: Option<std::rc::Rc<types_snapshot::SnapshotData>>,
        instrument: IndexScanInstrumentation,
        nkeys: i32,
    ) -> types_error::PgResult<IndexScanDesc<'mcx>>
);

seam_core::seam!(
    /// `index_getbitmap(scan, bitmap)` (indexam.c): the `amgetbitmap` access
    /// method — fetch the TIDs of all heap tuples satisfying the scan keys into
    /// `bitmap`, returning the number of TIDs inserted (a double in the caller,
    /// `int64` from the AM). Fallible on `ereport(ERROR)`.
    pub fn index_getbitmap<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        scan: &mut types_nodes::IndexScanDescData<'mcx>,
        bitmap: &mut types_tidbitmap::TIDBitmap,
    ) -> types_error::PgResult<i64>
);

seam_core::seam!(
    /// `index_rescan(node->biss_ScanDesc, node->biss_ScanKeys,
    /// node->biss_NumScanKeys, NULL, 0)` (indexam.c), driven from a bitmap index
    /// scan node: (re)start the scan with the node's current (possibly
    /// recomputed) scan keys. The owned model takes the node so the AM reads its
    /// `biss_ScanKeys` array and `biss_ScanDesc`. Fallible on `ereport(ERROR)`.
    pub fn index_rescan_bis<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        node: &mut types_nodes::nodebitmapindexscan::BitmapIndexScanState<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `index_getnext_tid(scan, direction)` (indexam.c): fetch the next TID in
    /// the given direction; `Ok(None)` at end of scan (the C `NULL`). Fills
    /// the scan descriptor's per-tuple result fields as a side effect.
    /// Fallible on `ereport(ERROR)`.
    pub fn index_getnext_tid<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        scan: &mut types_nodes::IndexScanDescData<'mcx>,
        direction: ScanDirection,
    ) -> types_error::PgResult<Option<ItemPointerData>>
);

seam_core::seam!(
    /// `index_getnext_slot(scan, direction, slot)` (indexam.c): fetch the next
    /// tuple satisfying the scan keys + snapshot into the table slot (id into
    /// the EState pool), returning whether one was found (the C `bool`; `false`
    /// at end of scan). Loops `index_getnext_tid` + `index_fetch_heap`
    /// internally, walking HOT chains. Fallible on `ereport(ERROR)`; pins a
    /// heap buffer recorded in the scan descriptor on success.
    pub fn index_getnext_slot<'mcx>(
        scan: &mut types_nodes::IndexScanDescData<'mcx>,
        direction: ScanDirection,
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
    ) -> types_error::PgResult<bool>
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
        mcx: mcx::Mcx<'mcx>,
        scan: types_nodes::IndexScanDesc<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `index_markpos(scan)` (indexam.c): mark the current scan position.
    pub fn index_markpos<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        scan: &mut types_nodes::IndexScanDescData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `index_restrpos(scan)` (indexam.c): restore the marked scan position.
    pub fn index_restrpos<'mcx>(
        mcx: mcx::Mcx<'mcx>,
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
    /// the shared parallel index-scan descriptor IN PLACE at the
    /// `shm_toc_allocate`'d chunk whose real in-segment base address is
    /// `target_addr` (the C `ParallelIndexScanDesc target` pointer). Writes the
    /// flat header (`ps_locator`/`ps_indexlocator`/`ps_offset_ins`/
    /// `ps_offset_am`), serializes the snapshot into the in-chunk
    /// `ps_snapshot_data` tail, zeroes the `SharedIndexScanInstrumentation`
    /// region at `ps_offset_ins`, and calls `aminitparallelscan` on the AM tail
    /// at `ps_offset_am`. The leader is the sole writer pre-launch. Returns the
    /// `Copy` in-DSM handle. Fallible on OOM / `ereport(ERROR)`.
    pub fn index_parallelscan_initialize<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        heap_relation: types_rel::Relation<'mcx>,
        index_relation: types_rel::Relation<'mcx>,
        snapshot: Option<std::rc::Rc<types_snapshot::SnapshotData>>,
        instrument: bool,
        parallel_aware: bool,
        nworkers: i32,
        target_addr: usize,
    ) -> types_error::PgResult<ParallelIndexScanDesc<'mcx>>
);

seam_core::seam!(
    /// `index_parallelrescan(scan)` (indexam.c): reset shared parallel scan
    /// state before beginning a fresh scan. Fallible on `ereport(ERROR)`.
    pub fn index_parallelrescan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
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

seam_core::seam!(
    /// `try_index_open(relationId, lockmode)` (indexam.c): like [`index_open`],
    /// but returns `Ok(None)` (the C `NULL`) when the index has disappeared
    /// rather than raising. Used by the REINDEX MISSING_OK path.
    pub fn try_index_open<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation_id: types_core::Oid,
        lockmode: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<Option<types_rel::Relation<'mcx>>>
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
    /// `SharedIndexScanInstrumentation` lives in-chunk at `piscan->ps_offset_ins`
    /// (the leader's `index_parallelscan_initialize` zeroed it there); this reads
    /// `num_workers` + the `IndexScanInstrumentation[]` tail back out of DSM into
    /// an owned struct the worker node holds. Fallible on `ereport(ERROR)`.
    pub fn index_scan_resolve_shared_info<'mcx>(
        piscan: types_nodes::ParallelIndexScanDescHandle,
    ) -> types_error::PgResult<types_nodes::SharedIndexScanInstrumentation>
);
