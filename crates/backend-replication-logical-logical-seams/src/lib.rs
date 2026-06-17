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

// -------------------------------------------------------------------------
// `rb->private_data` bridge.
//
// In C the ReorderBuffer holds a back-pointer `rb->private_data == ctx`, set in
// StartupDecodingContext, so the file-static `*_cb_wrapper` functions reach the
// live `LogicalDecodingContext` when ReorderBuffer drives them. The reorder
// buffer is owned (by handle) in its own crate and re-enters logical decoding
// through the `dispatch_reorderbuffer_callback(cb)` seam below, which carries
// only the callback variant. The live `ctx` is owned as a `Box` by the
// walsender and is on the stack when decode.c invokes a ReorderBuffer entry
// point (ReorderBufferCommit / Prepare / FinishPrepared / …). decode.c parks a
// raw pointer to it here for exactly the dynamic extent of that call — the
// faithful, single-threaded analog of `rb->private_data` — and the logical.c
// dispatch-seam installer dereferences it to recover `&mut ctx`.
//
// This lives in the seams crate (not the logical owner) so both decode.c (the
// parker) and the logical owner (the reader) reach it without a dependency
// cycle.
::std::thread_local! {
    static CURRENT_DECODING_CTX: core::cell::Cell<*mut LogicalDecodingContext> =
        const { core::cell::Cell::new(core::ptr::null_mut()) };
}

/// Park `ctx` as the live decoding context (`rb->private_data`) for the dynamic
/// extent of `f`, so the `dispatch_reorderbuffer_callback` seam can resolve it
/// while `f` drives the reorder buffer. Restores the previous parked pointer on
/// exit (supporting nested call shapes), even on unwind.
///
/// `f` takes no `ctx` argument by design: while it runs, the only access to the
/// ctx is through the parked raw pointer (which the seam dereferences as the
/// sole `&mut`). Handing `f` a live `&mut ctx` *and* parking a raw pointer to
/// the same object would alias two `&mut`s; instead the caller reads any ctx
/// fields it needs (e.g. `ctx.reorder`) into locals *before* the call, and `f`
/// closes over those. The borrow of `ctx` taken here ends before `f` is invoked
/// (only the raw pointer survives into `f`), so there is no live `&mut ctx`
/// when the seam reconstructs one.
pub fn with_parked_decoding_ctx<R>(
    ctx: &mut LogicalDecodingContext,
    f: impl FnOnce() -> R,
) -> R {
    struct Restore(*mut LogicalDecodingContext);
    impl Drop for Restore {
        fn drop(&mut self) {
            CURRENT_DECODING_CTX.with(|c| c.set(self.0));
        }
    }
    let ptr = ctx as *mut LogicalDecodingContext;
    // The `&mut ctx` borrow is no longer used past this point (we hold only the
    // raw pointer), so it is sound for the seam to reconstruct the unique `&mut`
    // from `ptr` while `f` runs.
    let prev = CURRENT_DECODING_CTX.with(|c| c.replace(ptr));
    let _restore = Restore(prev);
    f()
}

/// Resolve the live decoding context parked by [`with_parked_decoding_ctx`] and
/// run `f` against it. Panics if none is parked (the C `rb->private_data ==
/// NULL` programming error — the reorder buffer was driven outside a decode
/// scope).
///
/// # Safety contract
/// The caller (the logical.c dispatch-seam installer) must hold no other `&mut`
/// to the same ctx; see that installer for the full argument.
pub fn with_current_decoding_ctx<R>(
    f: impl FnOnce(&mut LogicalDecodingContext) -> R,
) -> R {
    let ptr = CURRENT_DECODING_CTX.with(|c| c.get());
    assert!(
        !ptr.is_null(),
        "dispatch_reorderbuffer_callback: no live LogicalDecodingContext parked \
         (rb->private_data == NULL); the reorder buffer must be driven inside \
         with_parked_decoding_ctx"
    );
    // SAFETY: see the function doc / the dispatch-seam installer.
    let ctx: &mut LogicalDecodingContext = unsafe { &mut *ptr };
    f(ctx)
}

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
