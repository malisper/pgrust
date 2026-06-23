//! Seam declarations for the `backend-replication-walsender` unit
//! (`replication/walsender.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use types_core::primitive::{TransactionId, XLogRecPtr};
use types_replication::walsender::WalSndState;

seam_core::seam!(
    /// `WalSndSetState(state)` (walsender.c) — set this walsender's advertised
    /// state in shared memory (e.g. `WALSNDSTATE_BACKUP` at the start of a
    /// `BASE_BACKUP`). Touches the per-slot spinlock-protected `WalSnd`.
    pub fn wal_snd_set_state(state: WalSndState)
);

seam_core::seam!(
    /// `bool exec_replication_command(const char *cmd_string)` (walsender.c) —
    /// the WAL-sender replication-command entry reached from `PostgresMain`'s
    /// simple-Query (`'Q'`) arm when `am_walsender`. Returns `false` if the
    /// string was not a replication command (the SQL path then takes over).
    /// Can `ereport(ERROR)`.
    pub fn exec_replication_command(cmd_string: &str) -> bool
);

seam_core::seam!(
    /// `WalSndErrorCleanup(void)` (walsender.c) — the WAL-sender error-recovery
    /// cleanup reached from `PostgresMain`'s error handler when `am_walsender`.
    /// Releases LWLocks, cancels condition-variable sleeps, closes the
    /// xlogreader, and frees / cleans up the active replication slot.
    pub fn wal_snd_error_cleanup()
);

seam_core::seam!(
    /// `InitWalSender(void)` (walsender.c) — initialize a WAL sender before the
    /// command loop, reached from `PostgresMain` after authentication when
    /// `am_walsender`. Claims this backend's per-walsender shmem `WalSnd` slot,
    /// creates the aux-process resource owner, advertises WAL-sender status to
    /// the postmaster, and sets up the lag-tracking buffer.
    pub fn init_wal_sender()
);

seam_core::seam!(
    /// `WalSndSignals(void)` (walsender.c) — install the WAL-sender signal
    /// handlers, reached from `PostgresMain`'s signal setup when `am_walsender`
    /// (in place of the regular-backend `pqsignal` block).
    pub fn wal_snd_signals()
);

seam_core::seam!(
    /// `HandleWalSndInitStopping()` (walsender.c) — the
    /// PROCSIG_WALSND_INIT_STOPPING arm of `procsignal_sigusr1_handler`.
    /// Signal-handler-safe flag flipping; infallible.
    pub fn handle_wal_snd_init_stopping()
);

seam_core::seam!(
    /// `am_db_walsender` (walsender.c global) — true in a database-connected
    /// (logical-replication) WAL sender. Pure read.
    pub fn am_db_walsender() -> bool
);

seam_core::seam!(
    /// `am_walsender = value` (walsender.c global) — set by the startup-packet
    /// `replication` parameter handling.
    pub fn set_am_walsender(value: bool)
);

seam_core::seam!(
    /// `am_db_walsender = value` (walsender.c global).
    pub fn set_am_db_walsender(value: bool)
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

seam_core::seam!(
    /// `if (AllowCascadeReplication()) WalSndWakeup(true, false)` — wake
    /// cascading walsenders after the walreceiver flushes new WAL.
    pub fn walsnd_wakeup_if_cascading()
);

seam_core::seam!(
    /// `max_wal_senders` (walsender.c GUC).
    pub fn max_wal_senders() -> i32
);

seam_core::seam!(
    /// `GetStandbyFlushRecPtr(NULL)` (walsender.c): on a standby, the most
    /// recent WAL position known to be safely flushed/replayed (the max of the
    /// receiver's flushed and the startup process's replayed LSN). Callers
    /// that don't need the timeline pass `NULL`; this seam discards it.
    pub fn get_standby_flush_rec_ptr() -> XLogRecPtr
);

seam_core::seam!(
    /// `WalSndShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn wal_snd_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `WalSndShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn wal_snd_shmem_init() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `WalSndRqstFileReload()` (walsender.c) — set `needreload` on every active
    /// walsender so each reopens its currently-open WAL segment (used after a
    /// segment is replaced from the archive). Touches shared memory.
    pub fn wal_snd_rqst_file_reload() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `WalSndWakeup(physical, logical)` (walsender.c) — set the latch of every
    /// walsender of the requested kind so they notice newly-arrived WAL.
    pub fn wal_snd_wakeup(physical: bool, logical: bool) -> types_error::PgResult<()>
);
