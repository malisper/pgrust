//! Seam declarations for table-AM dispatch helpers in
//! `access/table/tableam.c` / `access/tableam.h` inline wrappers that dispatch
//! through a relation's `rd_tableam` vtable into its access method:
//! the TOAST helpers (`table_relation_toast_am` /
//! `table_relation_needs_toast_table`) and the sequential-scan entry points
//! (`table_beginscan` / `table_scan_getnextslot` / `table_endscan`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. (The heap AM implementations these dispatch to —
//! `heapam_relation_toast_am` / `heapam_relation_needs_toast_table` /
//! `scan_begin` / `scan_getnextslot` / `scan_end` — are
//! `access/heap/heapam_handler.c`, also unported, so the call panics until
//! both land.)
//!
//! The scan descriptor crosses as the C-faithful value-typed
//! [`TableScanDesc`](types_tableam::relscan::TableScanDesc) (an owned
//! `Box<TableScanDescData>`), matching the bodies the tableam.c owner was
//! ported with (and the value-typed bitmap-scan seams in
//! `backend-access-table-tableam-bm-seams`).

use std::rc::Rc;

use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::tuptable::SlotData;
use types_rel::Relation;
use types_snapshot::SnapshotData;
use types_tableam::relscan::{TableScanDesc, TableScanDescData};

seam_core::seam!(
    /// `table_index_build_scan(table_rel, index_rel, index_info, allow_sync,
    /// progress, callback, callback_state, scan=NULL)` (tableam.h): scan the
    /// table once for an index build, invoking `callback` per live tuple with
    /// `(heap_tid, values, isnull, tuple_is_alive)`. The index AM's per-tuple
    /// callback (`hashbuildCallback`) crosses the seam as a closure the heap AM
    /// invokes (mirroring the C `IndexBuildCallback` function pointer +
    /// `state`). Returns the number of heap tuples scanned (C `double`). `Err`
    /// carries the scan / callback `ereport(ERROR)` surface.
    ///
    /// In C this inline wrapper just forwards to `index_build_range_scan` with
    /// `anyvisible = false`, `start_blockno = 0`, `numblocks =
    /// InvalidBlockNumber` (the whole relation); the heap AM owner installs it
    /// that way, dispatching to the same `heapam_index_build_range_scan`
    /// provider. The `index_info` is the real executor `IndexInfo<'mcx>` (so
    /// the AM build driver can read `ii_Unique` / `ii_NullsNotDistinct` and the
    /// scan can set `ii_BrokenHotChain`), matching the
    /// [`table_index_build_range_scan`] contract.
    #[allow(clippy::type_complexity)]
    pub fn table_index_build_scan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        table_rel: &types_rel::Relation<'mcx>,
        index_rel: &types_rel::Relation<'mcx>,
        index_info: &mut types_nodes::execnodes::IndexInfo<'mcx>,
        allow_sync: bool,
        progress: bool,
        callback: &mut dyn FnMut(
            types_tuple::heaptuple::ItemPointerData,
            &[types_tuple::backend_access_common_heaptuple::Datum<'mcx>],
            &[bool],
            bool,
        ) -> PgResult<()>,
    ) -> PgResult<f64>
);

seam_core::seam!(
    /// `table_index_build_range_scan(table_rel, index_rel, index_info,
    /// allow_sync, anyvisible, progress, start_blockno, numblocks, callback,
    /// callback_state, scan=NULL)` (tableam.h): scan a `[start_blockno,
    /// start_blockno + numblocks)` block range of the table once during a BRIN
    /// range summarization, invoking `callback` per qualifying tuple with
    /// `(heap_tid, values, isnull, tuple_is_alive)`. `anyvisible` selects the
    /// "any visible" snapshot mode BRIN requires (so in-progress inserts are
    /// summarized). The per-tuple callback (`brinbuildCallback`) crosses the
    /// seam as a closure the heap AM invokes (mirroring the C
    /// `IndexBuildCallback` function pointer + `state`). Returns the number of
    /// heap tuples scanned (C `double`). `Err` carries the scan / callback
    /// `ereport(ERROR)` surface.
    ///
    /// Owned by the heap AM (`access/heap/heapam_handler.c`, unported); a call
    /// panics until the heap scan layer lands — exactly like
    /// [`table_index_build_scan`], the sanctioned build-path seam.
    #[allow(clippy::type_complexity)]
    pub fn table_index_build_range_scan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        table_rel: &types_rel::Relation<'mcx>,
        index_rel: &types_rel::Relation<'mcx>,
        index_info: &mut types_nodes::execnodes::IndexInfo<'mcx>,
        allow_sync: bool,
        anyvisible: bool,
        progress: bool,
        start_blockno: types_core::primitive::BlockNumber,
        numblocks: types_core::primitive::BlockNumber,
        callback: &mut dyn FnMut(
            types_tuple::heaptuple::ItemPointerData,
            &[types_tuple::backend_access_common_heaptuple::Datum<'mcx>],
            &[bool],
            bool,
        ) -> PgResult<()>,
    ) -> PgResult<f64>
);

seam_core::seam!(
    /// `GetTableAmRoutine(amhandler)` (access/table/tableamapi.c): call the
    /// table AM's handler function (`OidFunctionCall0(amhandler)` returning a
    /// `const TableAmRoutine*`) and hand back the vtable for the relcache to
    /// cache in `rd_tableam`. `Err` carries the handler's `ereport(ERROR)`
    /// (wrong magic number / NULL routine).
    pub fn get_table_am_routine(
        amhandler: Oid,
    ) -> PgResult<types_tableam::TableAmRoutine>
);

seam_core::seam!(
    /// `table_relation_toast_am(rel)` (access/tableam.h, static inline):
    /// `rel->rd_tableam->relation_toast_am(rel)` — the OID of the AM that
    /// should implement the TOAST table for `rel`. Infallible.
    pub fn table_relation_toast_am(rel: &Relation<'_>) -> Oid
);

seam_core::seam!(
    /// `table_relation_needs_toast_table(rel)` (access/tableam.h, static
    /// inline): `rel->rd_tableam->relation_needs_toast_table(rel)` — does the
    /// relation need a TOAST table? Infallible.
    pub fn table_relation_needs_toast_table(rel: &Relation<'_>) -> bool
);

seam_core::seam!(
    /// `table_relation_set_new_filelocator(rel, newrnode, persistence,
    /// &freezeXid, &minmulti)` (access/tableam.h, static inline):
    /// `rel->rd_tableam->relation_set_new_filelocator(...)` — create storage for
    /// the new relfilelocator of a table-AM relation (also its init fork if
    /// unlogged), and hand back the AM-chosen `relfrozenxid`/`relminmxid` to
    /// store in pg_class. Dispatch is keyed by the relation OID (the relcache
    /// entry owns the `rd_tableam` vtable, which can't cross this boundary).
    /// Returns `(freeze_xid, minmulti)`. `Err` carries its `ereport(ERROR)`s.
    pub fn table_relation_set_new_filelocator(
        relid: Oid,
        newrlocator: types_storage::RelFileLocator,
        relpersistence: i8,
    ) -> PgResult<(u32, u32)>
);

seam_core::seam!(
    /// `table_beginscan(relation, snapshot, 0, NULL)` (copyto.c:1076): start a
    /// forward sequential scan of `relation` under `snapshot`. The COPY driver
    /// passes `GetActiveSnapshot()` explicitly (the C call's second argument);
    /// the snapshot crosses as a shared `Rc<SnapshotData>` rather than being
    /// read ambiently inside the AM. Returns the AM-owned scan descriptor (the
    /// C-faithful value `TableScanDesc`). `Err` carries the AM's
    /// `ereport(ERROR)`.
    pub fn table_beginscan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation: &Relation<'mcx>,
        snapshot: Rc<SnapshotData>,
    ) -> PgResult<TableScanDesc<'mcx>>
);

seam_core::seam!(
    /// `table_scan_getnextslot(scan, ForwardScanDirection, slot)`
    /// (copyto.c:1080): fetch the next tuple into `slot`, returning `true` if a
    /// tuple was produced (`false` at end of scan). `Err` carries the AM's
    /// `ereport(ERROR)`.
    pub fn table_scan_getnextslot<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        scan: &mut TableScanDescData<'mcx>,
        slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `table_scan_getnextslot(scan, direction, slot)` (access/tableam.h,
    /// static inline) — the direction-carrying form `nodeSeqscan.c`'s `SeqNext`
    /// uses (`estate->es_direction`). Fetches the next tuple into `slot`,
    /// returning `true` if a tuple was produced (`false` at end of scan). `Err`
    /// carries the AM's `ereport(ERROR)`.
    pub fn table_scan_getnextslot_direction<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        scan: &mut TableScanDescData<'mcx>,
        direction: types_scan::sdir::ScanDirection,
        slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `table_rescan(scan, NULL)` (access/tableam.h, static inline) — restart
    /// the scan `scan` with no new scan keys (`nodeSeqscan.c`'s
    /// `ExecReScanSeqScan`). The leading `mcx` (convention A) is the arena the
    /// AM reinitializes the scan in. `Err` carries the AM's `ereport(ERROR)`.
    pub fn table_rescan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        scan: &mut TableScanDescData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `table_parallelscan_reinitialize(rel, pscan)` (tableam.c): reset the
    /// shared parallel-scan state in `pscan` before a re-scan
    /// (`nodeSeqscan.c`'s `ExecSeqScanReInitializeDSM`). `Err` carries the AM's
    /// `ereport(ERROR)`.
    pub fn table_parallelscan_reinitialize(
        rel: &Relation<'_>,
        pscan: &mut types_tableam::relscan::ParallelTableScanDescData,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `table_beginscan_analyze(rel)` (access/tableam.h, static inline) — the
    /// alternative scan-begin `acquire_sample_rows` uses for ANALYZE: dispatch
    /// the AM's `scan_begin` with `flags = SO_TYPE_ANALYZE` (no snapshot, no scan
    /// keys, no parallel descriptor). Returns the AM-owned scan descriptor (the
    /// C-faithful value `TableScanDesc`). `Err` carries the AM's
    /// `ereport(ERROR)`.
    pub fn table_beginscan_analyze<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation: &Relation<'mcx>,
    ) -> PgResult<TableScanDesc<'mcx>>
);

seam_core::seam!(
    /// `table_scan_analyze_next_block(scan, stream)` (access/tableam.h, static
    /// inline): `scan->rs_rd->rd_tableam->scan_analyze_next_block(scan, stream)`
    /// — the outer ANALYZE-sampling loop callback. Pin + share-lock the next
    /// block to be sampled, leaving it the scan's current page; returns `false`
    /// when the read stream is exhausted.
    ///
    /// In C the stream argument is `ReadStream *`, and the heap AM's only use of
    /// it is `read_stream_next_buffer(stream, NULL)`. The read stream lives in
    /// `commands/analyze.c` (above this layer), so it crosses the seam as the
    /// `next_buffer` closure the caller builds over its stream — the same
    /// closure-across-layers technique the index-build callback uses. The
    /// closure returns the next pinned `Buffer` (or `InvalidBuffer` at end of
    /// stream). `Err` carries the AM's `ereport(ERROR)`.
    pub fn table_scan_analyze_next_block<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        scan: &mut TableScanDescData<'mcx>,
        next_buffer: &mut dyn FnMut() -> PgResult<types_storage::buf::Buffer>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `table_scan_analyze_next_tuple(scan, OldestXmin, liverows, deadrows,
    /// slot)` (access/tableam.h, static inline):
    /// `scan->rs_rd->rd_tableam->scan_analyze_next_tuple(...)` — the inner
    /// ANALYZE-sampling loop callback. Advance over the current block's tuples,
    /// updating the live/dead counters, and store the next sampleable tuple into
    /// `slot`, returning `true` (buffer left locked) or `false` at end of block
    /// (buffer unlocked + released, slot cleared). `Err` carries the AM's
    /// `ereport(ERROR)`.
    pub fn table_scan_analyze_next_tuple<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        scan: &mut TableScanDescData<'mcx>,
        oldest_xmin: types_core::TransactionId,
        liverows: &mut f64,
        deadrows: &mut f64,
        slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `table_endscan(scan)` (copyto.c:1099): close the scan and release its
    /// resources. Consumes the descriptor. `Err` carries the AM's
    /// `ereport(ERROR)`.
    pub fn table_endscan<'mcx>(scan: TableScanDesc<'mcx>) -> PgResult<()>
);
