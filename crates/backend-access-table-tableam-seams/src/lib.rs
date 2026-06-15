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
use types_nodes::TupleTableSlot;
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
    #[allow(clippy::type_complexity)]
    pub fn table_index_build_scan<'mcx>(
        table_rel: &types_rel::Relation<'mcx>,
        index_rel: &types_rel::Relation<'mcx>,
        index_info: &mut types_tableam::amapi::IndexInfo,
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
        slot: &mut TupleTableSlot<'mcx>,
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
        slot: &mut TupleTableSlot<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `table_rescan(scan, NULL)` (access/tableam.h, static inline) — restart
    /// the scan `scan` with no new scan keys (`nodeSeqscan.c`'s
    /// `ExecReScanSeqScan`). `Err` carries the AM's `ereport(ERROR)`.
    pub fn table_rescan<'mcx>(scan: &mut TableScanDescData<'mcx>) -> PgResult<()>
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
    /// `table_endscan(scan)` (copyto.c:1099): close the scan and release its
    /// resources. Consumes the descriptor. `Err` carries the AM's
    /// `ereport(ERROR)`.
    pub fn table_endscan<'mcx>(scan: TableScanDesc<'mcx>) -> PgResult<()>
);
