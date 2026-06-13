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

seam_core::seam!(
    /// `bool am_walsender` (walsender.c) — true if this process is a WAL
    /// sender. A backend-local global read.
    pub fn am_walsender() -> bool
);

seam_core::seam!(
    /// `bool log_replication_commands` (walsender.c) — the GUC controlling
    /// whether replication commands are logged at LOG (vs DEBUG1).
    pub fn log_replication_commands() -> bool
);

seam_core::seam!(
    /// Run `f` with `&WalSndCtl->wal_confirm_rcv_cv`, the shared condition
    /// variable logical WAL senders wait on for physical-standby confirmation
    /// (`WaitForStandbyConfirmation`). `WalSndCtl` lives in shared memory owned
    /// by walsender; the CV protocol functions are reached via the
    /// condition-variable seams, so only a borrow is handed out here.
    pub fn with_wal_confirm_rcv_cv(f: &mut dyn FnMut(&types_condvar::ConditionVariable))
);
