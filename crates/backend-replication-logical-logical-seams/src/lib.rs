//! Seam declarations for the `backend-replication-logical-logical` unit
//! (`replication/logical/logical.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::primitive::XLogRecPtr;
use types_error::PgResult;

seam_core::seam!(
    /// `LogicalSlotAdvanceAndCheckSnapState(moveto, found_consistent_snapshot)`
    /// (logical.c) — advance `MyReplicationSlot` by decoding up to `moveto`,
    /// returning the resulting `confirmed_flush`. `found_consistent_snapshot`
    /// (when `Some`) is set true if a consistent decoding snapshot was reached.
    /// The C signature threads the out-pointer verbatim (NULL stays NULL).
    pub fn logical_slot_advance_and_check_snap_state(
        moveto: XLogRecPtr,
        found_consistent_snapshot: Option<&mut bool>,
    ) -> PgResult<XLogRecPtr>
);
