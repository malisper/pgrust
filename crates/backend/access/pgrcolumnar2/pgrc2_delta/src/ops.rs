//! Trickle-DML composition over the delta store (the write faces M5-M's
//! ModifyTable binding drives; frozen donor surface at M5-B's merge).
//!
//! The heap side is a seam ([`DeltaWrite`]): at product grain the
//! implementor is real heapam over the table's delta pair (WAL inherited
//! from heap — this crate builds none); at crate grain it is
//! [`crate::testkit::SimHeap`]. All laws live HERE so both implementors
//! inherit them:
//!
//! - **delete_sealed** writes a tombstone (the frozen §15 payload) into
//!   the tombstone relation; a delta-tagged rowid is refused typed
//!   (delta rows die by heap delete — crate doc law).
//! - **delete_delta** is an ordinary heap delete on the delta relation.
//! - **update_sealed** = tombstone THEN delta-insert, one transaction.
//!   The order is pinned for determinism only; atomicity is the
//!   transaction's (both effects share commit/abort by heap MVCC).
//! - **update_delta** is an ordinary heap update on the delta relation.
//! - **trickle_insert** appends to the delta relation.
//!
//! Nothing here touches commit choreography: trickle transactions commit
//! through the ordinary xact engine, and the delta writes carry heap WAL
//! (`wrote_xlog = true`), which is what makes the #253 hazard class
//! structurally unreachable for them (crate doc; the crash battery is the
//! witness).

use crate::feed::DeltaCell;
use crate::tombstone::tombstone_payload;
use crate::DeltaResult;

/// The heap-side write seam over a table's delta pair. Implementors
/// persist through their own WAL story (product: heap WAL; testkit: the
/// sim WAL model). All faces are transactional in the implementor's
/// current transaction — this crate never begins or ends one.
pub trait DeltaWrite {
    /// Append `row` (the table's storage columns, feed currency) to the
    /// delta relation; returns the new tuple's (block, offnum).
    fn insert_delta_row(&mut self, row: &[DeltaCell]) -> DeltaResult<(u32, u16)>;

    /// Append a tombstone row (single int8 `payload`) to the tombstone
    /// relation; returns the new tuple's (block, offnum).
    fn insert_tombstone_row(&mut self, payload: i64) -> DeltaResult<(u32, u16)>;

    /// Heap-delete the delta-relation tuple at `tid` (MVCC delete: xmax
    /// stamped, visibility decided by snapshots as for any heap row).
    fn delete_delta_row(&mut self, tid: (u32, u16)) -> DeltaResult<()>;

    /// Heap-update the delta-relation tuple at `tid` to `row`; returns
    /// the new version's (block, offnum).
    fn update_delta_row(&mut self, tid: (u32, u16), row: &[DeltaCell])
        -> DeltaResult<(u32, u16)>;
}

/// Trickle INSERT: the row lands in the delta relation.
pub fn trickle_insert<W: DeltaWrite>(w: &mut W, row: &[DeltaCell]) -> DeltaResult<(u32, u16)> {
    w.insert_delta_row(row)
}

/// DELETE of a sealed row: write its tombstone. Refuses delta-tagged
/// rowids typed (the encode guard in [`crate::tombstone`]).
pub fn delete_sealed<W: DeltaWrite>(w: &mut W, sealed_rowid: u64) -> DeltaResult<(u32, u16)> {
    let payload = tombstone_payload(sealed_rowid)?;
    w.insert_tombstone_row(payload)
}

/// DELETE of a delta-resident row: ordinary heap delete, no tombstone.
pub fn delete_delta<W: DeltaWrite>(w: &mut W, tid: (u32, u16)) -> DeltaResult<()> {
    w.delete_delta_row(tid)
}

/// UPDATE of a sealed row: tombstone THEN delta-insert (pinned order, one
/// transaction). Returns the delta-resident new version's tid.
pub fn update_sealed<W: DeltaWrite>(
    w: &mut W,
    sealed_rowid: u64,
    new_row: &[DeltaCell],
) -> DeltaResult<(u32, u16)> {
    delete_sealed(w, sealed_rowid)?;
    w.insert_delta_row(new_row)
}

/// UPDATE of a delta-resident row: ordinary heap update.
pub fn update_delta<W: DeltaWrite>(
    w: &mut W,
    tid: (u32, u16),
    new_row: &[DeltaCell],
) -> DeltaResult<(u32, u16)> {
    w.update_delta_row(tid, new_row)
}
