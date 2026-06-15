//! Seam declarations for the bitmap-scan table-AM entry points
//! (`access/tableam.h` inline wrappers around the AM's `scan_begin` /
//! `scan_bitmap_next_tuple` / `scan_rescan` / `scan_end` callbacks).
//!
//! These dispatch through `rel->rd_tableam` to the concrete access method
//! (heapam), which the `backend-access-table-tableam` / heap AM units own and
//! have not ported the bitmap path of yet. Consumers reach them here; a call
//! panics loudly until the heap AM installs them.

#![allow(non_snake_case)]

use types_error::PgResult;
use types_nodes::executor::TupleTableSlot;
use types_rel::Relation;
use types_tableam::relscan::{TableScanDesc, TableScanDescData};

seam_core::seam!(
    /// `table_beginscan_bm(rel, snapshot, 0, NULL)` (access/tableam.h): set up
    /// a `TableScanDesc` for a bitmap heap scan
    /// (`SO_TYPE_BITMAPSCAN | SO_ALLOW_PAGEMODE`). The descriptor is allocated
    /// by the AM (`palloc`), so the call is fallible on OOM.
    pub fn table_beginscan_bm<'mcx>(
        rel: Relation<'mcx>,
        snapshot: Option<std::rc::Rc<types_snapshot::SnapshotData>>,
    ) -> PgResult<TableScanDesc<'mcx>>
);

seam_core::seam!(
    /// `table_scan_bitmap_next_tuple(scan, slot, &recheck, &lossy_pages,
    /// &exact_pages)` (access/tableam.h): fetch the next visible tuple of a
    /// bitmap table scan into `slot`. Returns `Some((recheck, lossy_inc,
    /// exact_inc))` when a tuple was stored (the C `true`), `None` at end of
    /// scan (the C `false`). `recheck` is the AM's per-tuple recheck flag;
    /// `lossy_inc`/`exact_inc` are the per-block bumps the C applies to the
    /// node's `lossy_pages`/`exact_pages` counters. Fallible — the AM
    /// `ereport`s (e.g. unexpected call during logical decoding) and the heap
    /// fetch can error.
    pub fn table_scan_bitmap_next_tuple<'mcx>(
        scan: &mut TableScanDescData<'mcx>,
        slot: &mut TupleTableSlot<'mcx>,
    ) -> PgResult<Option<(bool, u64, u64)>>
);

seam_core::seam!(
    /// `table_rescan(scan, NULL)` (access/tableam.h): restart the scan,
    /// releasing any page pin. Fallible — the AM's `scan_rescan` can error.
    pub fn table_rescan<'mcx>(scan: &mut TableScanDescData<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `table_endscan(scan)` (access/tableam.h): close the scan, releasing the
    /// descriptor and its resources. Consumes the descriptor box. Fallible —
    /// the AM's `scan_end` runs cleanup that can error.
    pub fn table_endscan<'mcx>(scan: TableScanDesc<'mcx>) -> PgResult<()>
);
