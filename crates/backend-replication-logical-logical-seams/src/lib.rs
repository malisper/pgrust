//! Inward seam declarations for the `backend-replication-logical-logical` unit
//! (`replication/logical/logical.c`).
//!
//! These are the entry points other (cyclic-partner) subsystems call back into
//! logical decoding through. `logical.c` installs them from its `init_seams()`.
//! Until then a call panics loudly.

#![allow(non_snake_case)]

use types_core::primitive::{Oid, XLogRecPtr};
use types_error::PgResult;
use types_logical::ReorderBufferCallback;

seam_core::seam!(
    /// `ResetLogicalStreamingState()` — reset logical streaming state on
    /// abort.
    pub fn reset_logical_streaming_state()
);

seam_core::seam!(
    /// Re-enter the crate's ReorderBuffer-driven `*_cb_wrapper` selected by
    /// `cb`, with `ctx == cache->private_data` (the runtime resolves the live
    /// decoding context). The reorderbuffer owner's trampolines call this.
    /// Mirrors the C wrapper failure surface: any wrapper can `ereport`.
    pub fn dispatch_reorderbuffer_callback(cb: ReorderBufferCallback) -> PgResult<()>
);

seam_core::seam!(
    /// `LogicalSlotAdvanceAndCheckSnapState(moveto, found_consistent_snapshot)`
    /// (logical.c:2083) — advance `MyReplicationSlot` by decoding up to
    /// `moveto`, returning the resulting `confirmed_flush`.
    /// `found_consistent_snapshot` (when `Some`) is set true if a consistent
    /// decoding snapshot was reached. `wal_segment_size`/`my_database_id` are
    /// the caller's `wal_segment_size` GUC and `MyDatabaseId` (no ambient
    /// globals at the seam).
    pub fn logical_slot_advance_and_check_snap_state(
        moveto: XLogRecPtr,
        found_consistent_snapshot: Option<&mut bool>,
        wal_segment_size: i32,
        my_database_id: Oid,
    ) -> PgResult<XLogRecPtr>
);
