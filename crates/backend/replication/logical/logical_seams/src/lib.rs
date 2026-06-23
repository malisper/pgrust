//! Inward seam declarations for the `backend-replication-logical-logical` unit
//! (`replication/logical/logical.c`).
//!
//! These are the entry points other (cyclic-partner) subsystems call back into
//! logical decoding through. `logical.c` installs them from its `init_seams()`.
//! Until then a call panics loudly.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;
use ::types_core::primitive::{Oid, RepOriginId, TransactionId, XLogRecPtr};
use ::types_error::PgResult;
use ::types_logical::{LogicalDecodingContext, ReorderBufferCallback};

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

seam_core::seam!(
    /// `LogicalOutputWrite(ctx, lsn, xid, last_write)` (logicalfuncs.c:61) — the
    /// SQL-function (#351 `OutputWriter::SqlSrf`) write callback. Reads the
    /// finished `ctx->out` bytes, builds an `(lsn pg_lsn, xid xid, data text)`
    /// row, and `tuplestore_putvalues`es it into the `ctx.output_writer_private`
    /// `DecodingOutputState` tuplestore. Owned by `logicalfuncs` (the tuplestore /
    /// funcapi Datum boundary lives there); `OutputPluginWrite` routes here when
    /// the context's writer is `SqlSrf`. `lsn`/`xid` are `ctx.write_location` /
    /// `ctx.write_xid`.
    pub fn sql_srf_output_write(
        ctx: &mut LogicalDecodingContext,
        lsn: XLogRecPtr,
        xid: TransactionId,
    ) -> PgResult<()>
);

// -------------------------------------------------------------------------
// SQL-function (`OutputWriter::SqlSrf`) output-row collector.
//
// `LogicalOutputWrite` (logicalfuncs.c) builds one `(lsn, xid, data text)` row
// per decoded change and `tuplestore_putvalues`es it into the SRF result. The
// put needs the live `fcinfo->resultinfo` ReturnSetInfo (an `'mcx` value owned
// by the SRF function's frame), which `OutputPluginWrite` — buried in the
// decode loop — cannot reach. We collect the owned, lifetime-free row tuples in
// a backend-local stack here for the dynamic extent of the decode loop; the SRF
// function (`pg_logical_slot_get_changes_guts`) drains them into its tuplestore
// after the loop. The collected payload is `(lsn: u64, xid: u32, data: Vec<u8>)`
// — exactly the three result columns, no `'mcx` value crosses the thread-local.
::std::thread_local! {
    static SQL_SRF_ROWS: core::cell::RefCell<Vec<(u64, u32, alloc::vec::Vec<u8>)>> =
        const { core::cell::RefCell::new(Vec::new()) };
}

/// Append one decoded output row (the `LogicalOutputWrite` body's
/// `tuplestore_putvalues` of `(lsn, xid, text(ctx->out))`). Returns the running
/// `returned_rows` count (C's `p->returned_rows++`).
pub fn sql_srf_push_row(lsn: u64, xid: u32, data: alloc::vec::Vec<u8>) -> i64 {
    SQL_SRF_ROWS.with(|r| {
        let mut r = r.borrow_mut();
        r.push((lsn, xid, data));
        r.len() as i64
    })
}

/// The current number of collected rows (C's `p->returned_rows`, read by the
/// decode loop's `upto_nchanges` bound).
pub fn sql_srf_returned_rows() -> i64 {
    SQL_SRF_ROWS.with(|r| r.borrow().len() as i64)
}

/// Drain the collected rows (the SRF function rebuilds them into its tuplestore
/// after the decode loop). Also clears the collector for the next call.
pub fn sql_srf_take_rows() -> Vec<(u64, u32, alloc::vec::Vec<u8>)> {
    SQL_SRF_ROWS.with(|r| core::mem::take(&mut *r.borrow_mut()))
}

/// Clear the collector (the SRF function's PG_CATCH cleanup, so a failed decode
/// does not leak rows into the next call).
pub fn sql_srf_clear_rows() {
    SQL_SRF_ROWS.with(|r| r.borrow_mut().clear());
}

// -------------------------------------------------------------------------
// `ctx->output_plugin_options` (List* of DefElem) backing store.
//
// In C the decode caller (walsender START_REPLICATION / logicalfuncs
// pg_logical_slot_get_changes_guts) deconstructs the `text[]` options array
// into a `List *` of `DefElem`, stored as `ctx->output_plugin_options` and
// forwarded verbatim to the plugin's startup callback. The repo carries
// `output_plugin_options` as an opaque `OutputPluginOptionsHandle`; this store
// backs each handle with the parsed `(key, value)` pairs. The default handle
// (`0`, NIL) is the empty list — every current caller that passes no options.
// -------------------------------------------------------------------------

::std::thread_local! {
    static OPTION_LISTS: core::cell::RefCell<Vec<Vec<(alloc::string::String, Option<alloc::string::String>)>>> =
        const { core::cell::RefCell::new(Vec::new()) };
}

/// Register a parsed output-plugin option list and return its handle. The
/// caller (logicalfuncs / walsender) builds this from the `text[]` options.
pub fn register_output_plugin_options(
    opts: Vec<(alloc::string::String, Option<alloc::string::String>)>,
) -> ::types_logical::OutputPluginOptionsHandle {
    OPTION_LISTS.with(|s| {
        let mut s = s.borrow_mut();
        if s.is_empty() {
            // Reserve slot 0 as the NIL (empty) list.
            s.push(Vec::new());
        }
        s.push(opts);
        ::types_logical::OutputPluginOptionsHandle(s.len() - 1)
    })
}

/// The `(key, value)` DefElem pairs of an `OutputPluginOptionsHandle`. The
/// default handle (`0`) is the empty list.
pub fn output_plugin_options_list(
    handle: ::types_logical::OutputPluginOptionsHandle,
) -> Vec<(alloc::string::String, Option<alloc::string::String>)> {
    OPTION_LISTS.with(|s| {
        let s = s.borrow();
        s.get(handle.0).cloned().unwrap_or_default()
    })
}
