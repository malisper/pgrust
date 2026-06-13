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
//! The scan descriptor (`TableScanDesc`) is owned by the AM; it crosses as an
//! opaque [`ScanToken`].

use std::rc::Rc;

use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::TupleTableSlot;
use types_rel::Relation;
use types_snapshot::SnapshotData;

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

/// An open `TableScanDesc` (the AM-owned scan state). C's pointer is opaque to
/// the COPY driver, which only threads it back into `getnextslot`/`endscan`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanToken(pub u64);

seam_core::seam!(
    /// `table_beginscan(relation, snapshot, 0, NULL)` (copyto.c:1076): start a
    /// forward sequential scan of `relation` under `snapshot`. The COPY driver
    /// passes `GetActiveSnapshot()` explicitly (the C call's second argument);
    /// the snapshot crosses as a shared `Rc<SnapshotData>` rather than being
    /// read ambiently inside the AM. Returns the AM-owned scan token. `Err`
    /// carries the AM's `ereport(ERROR)`.
    pub fn table_beginscan(
        relation: &Relation<'_>,
        snapshot: Rc<SnapshotData>,
    ) -> PgResult<ScanToken>
);

seam_core::seam!(
    /// `table_scan_getnextslot(scan, ForwardScanDirection, slot)`
    /// (copyto.c:1080): fetch the next tuple into `slot`, returning `true` if a
    /// tuple was produced (`false` at end of scan). `Err` carries the AM's
    /// `ereport(ERROR)`.
    pub fn table_scan_getnextslot(
        scan: ScanToken,
        slot: &mut TupleTableSlot,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `table_endscan(scan)` (copyto.c:1099): close the scan and release its
    /// resources. `Err` carries the AM's `ereport(ERROR)`.
    pub fn table_endscan(scan: ScanToken) -> PgResult<()>
);
