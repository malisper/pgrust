//! Seam declarations for the `backend-commands-async` unit
//! (`commands/async.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_core::TransactionId;
use types_error::PgResult;

seam_core::seam!(
    /// `HandleNotifyInterrupt()` (async.c) — the PROCSIG_NOTIFY_INTERRUPT
    /// arm of `procsignal_sigusr1_handler`. Signal-handler-safe flag
    /// flipping; infallible.
    pub fn handle_notify_interrupt()
);

seam_core::seam!(
    /// `PreCommit_Notify()` — insert pending notifications into the queue
    /// (may create a snapshot; can `ereport(ERROR)`).
    pub fn pre_commit_notify() -> PgResult<()>
);

seam_core::seam!(
    /// `AtCommit_Notify()` — signal listening backends after commit.
    pub fn at_commit_notify() -> PgResult<()>
);

seam_core::seam!(
    /// `AtAbort_Notify()`.
    pub fn at_abort_notify() -> PgResult<()>
);

seam_core::seam!(
    /// `AtSubCommit_Notify()` — reparent the subxact's pending notifies.
    pub fn at_subcommit_notify() -> PgResult<()>
);

seam_core::seam!(
    /// `AtSubAbort_Notify()`.
    pub fn at_subabort_notify()
);

seam_core::seam!(
    /// `AtPrepare_Notify()` — errors out for transactions that sent NOTIFY /
    /// LISTEN / UNLISTEN (2PC restriction).
    pub fn at_prepare_notify() -> PgResult<()>
);

seam_core::seam!(
    /// `AsyncShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn async_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `AsyncShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn async_shmem_init() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `Async_UnlistenAll()` (async.c) — `DISCARD ALL` / session reset: remove
    /// all of this backend's LISTEN registrations. May `ereport(ERROR)`.
    pub fn async_unlisten_all() -> PgResult<()>
);

seam_core::seam!(
    /// `AsyncNotifyFreezeXids(newFrozenXid)` (async.c) — VACUUM hook, called by
    /// `vac_truncate_clog()` before advancing `datfrozenxid`. Scans the async
    /// notification queue and replaces XIDs `< newFrozenXid` (whose CLOG pages
    /// are about to be truncated) with `FrozenTransactionId` (committed) or
    /// `InvalidTransactionId` (aborted/crashed). May `ereport(ERROR)`.
    pub fn async_notify_freeze_xids(new_frozen_xid: TransactionId) -> PgResult<()>
);
