//! Seam declarations for the `backend-replication-walsender` unit
//! (`replication/walsender.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use types_core::primitive::{TransactionId, XLogRecPtr};

seam_core::seam!(
    /// `HandleWalSndInitStopping()` (walsender.c) — the
    /// PROCSIG_WALSND_INIT_STOPPING arm of `procsignal_sigusr1_handler`.
    /// Signal-handler-safe flag flipping; infallible.
    pub fn handle_wal_snd_init_stopping()
);

seam_core::seam!(
    /// `WaitForStandbyConfirmation(moveto)` (walsender.c): wait for the
    /// synchronous standbys to confirm `moveto`. Can `ereport(ERROR)` on
    /// interrupt, carried on `Err`.
    pub fn WaitForStandbyConfirmation(moveto: XLogRecPtr) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ctx->prepare_write(ctx, write_location, write_xid, last_write)` — the
    /// `LogicalOutputPluginWriterPrepareWrite` callback the decoding caller
    /// (walsender / logicalfuncs) installs. Can `ereport(ERROR)`.
    pub fn call_prepare_write(write_location: XLogRecPtr, write_xid: TransactionId, last_write: bool) -> types_error::PgResult<()>
);
seam_core::seam!(
    /// `ctx->write(ctx, write_location, write_xid, last_write)`. Can
    /// `ereport(ERROR)`.
    pub fn call_write(write_location: XLogRecPtr, write_xid: TransactionId, last_write: bool) -> types_error::PgResult<()>
);
seam_core::seam!(
    /// `ctx->update_progress(ctx, write_location, write_xid, skipped_xact)`.
    /// Can `ereport(ERROR)`.
    pub fn call_update_progress(write_location: XLogRecPtr, write_xid: TransactionId, skipped_xact: bool) -> types_error::PgResult<()>
);
