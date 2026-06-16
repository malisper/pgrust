//! Inward seam declarations for the `backend-replication-logical-logical` unit
//! (`replication/logical/logical.c`).
//!
//! These are the entry points other (cyclic-partner) subsystems call back into
//! logical decoding through. `logical.c` installs them from its `init_seams()`.
//! Until then a call panics loudly.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;
use types_core::primitive::{Oid, RepOriginId, TransactionId, XLogRecPtr};
use types_error::PgResult;
use types_logical::{LogicalDecodingContext, ReorderBufferCallback};

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

seam_core::seam!(
    /// `LogicalIncreaseXminForSlot(current_lsn, xmin)` (logical.c:1678) — record
    /// a new candidate catalog xmin for `MyReplicationSlot`. snapbuild.c calls
    /// this from `SnapBuildProcessRunningXacts`.
    pub fn logical_increase_xmin_for_slot(
        current_lsn: XLogRecPtr,
        xmin: TransactionId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `LogicalIncreaseRestartDecodingForSlot(current_lsn, restart_lsn)`
    /// (logical.c:1746) — record a new candidate restart LSN for
    /// `MyReplicationSlot`. snapbuild.c calls this from
    /// `SnapBuildProcessRunningXacts`.
    pub fn logical_increase_restart_decoding_for_slot(
        current_lsn: XLogRecPtr,
        restart_lsn: XLogRecPtr,
    ) -> PgResult<()>
);

// ---------------------------------------------------------------------------
// Output-plugin filter wrappers + stats update consumed by decode.c.
//
// `decode.c` calls these `logical.c`-owned wrappers (`filter_prepare_cb_wrapper`
// / `filter_by_origin_cb_wrapper` / `UpdateDecodingStats`) directly, passing the
// live decoding context it received. They set the ctx output state and invoke
// the loaded output plugin's optional callbacks (panicking until the plugin
// loader lands) / report the reorder-buffer decoding stats to pgstat.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `filter_prepare_cb_wrapper(ctx, xid, gid)` (logical.c:1169) — ask the
    /// output plugin whether to skip a 2PC at PREPARE time. `gid` is the real
    /// (NUL-stripped) global-transaction-id bytes.
    pub fn filter_prepare_cb_wrapper(
        ctx: &mut LogicalDecodingContext,
        xid: TransactionId,
        gid: Vec<u8>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `filter_by_origin_cb_wrapper(ctx, origin_id)` (logical.c:1201) — ask the
    /// output plugin whether it is interested in changes from `origin_id`.
    pub fn filter_by_origin_cb_wrapper(
        ctx: &mut LogicalDecodingContext,
        origin_id: RepOriginId,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `UpdateDecodingStats(ctx)` (logical.c:1954) — report the reorder
    /// buffer's spill/stream/total decoding stats to pgstat.
    pub fn UpdateDecodingStats(ctx: &mut LogicalDecodingContext) -> PgResult<()>
);
