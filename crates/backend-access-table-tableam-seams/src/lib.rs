//! Seam declarations for the table-AM sequential-scan entry points
//! (`access/tableam.h` inline wrappers `table_beginscan` /
//! `table_scan_getnextslot` / `table_endscan`) that dispatch through the
//! relation's AM vtable (the heap AM `scan_begin` / `scan_getnextslot` /
//! `scan_end`). The heap AM is not ported yet, so these panic until it lands.
//!
//! The scan descriptor (`TableScanDesc`) is owned by the AM; it crosses as an
//! opaque [`ScanToken`].

use types_error::PgResult;
use types_nodes::TupleTableSlot;
use types_rel::Relation;

/// An open `TableScanDesc` (the AM-owned scan state). C's pointer is opaque to
/// the COPY driver, which only threads it back into `getnextslot`/`endscan`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanToken(pub u64);

seam_core::seam!(
    /// `table_beginscan(relation, GetActiveSnapshot(), 0, NULL)`
    /// (copyto.c:1076): start a forward sequential scan of `relation` under the
    /// active snapshot. Returns the AM-owned scan token. `Err` carries the
    /// AM's `ereport(ERROR)`.
    pub fn table_beginscan(relation: &Relation<'_>) -> PgResult<ScanToken>
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
