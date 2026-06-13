//! Seam declarations for `backend-replication-logical-launcher`
//! (`replication/logical/launcher.c`) — the launcher's own functions that
//! other logical-replication units (tablesync, apply worker, parallel-apply
//! worker, subscriptioncmds, pgstatfuncs) call across a dependency cycle.
//!
//! The owner installs every one of these from its `init_seams()`. The launcher
//! owns the shared worker-slot array, so worker lookups return the slot index
//! (`i32`) the C code carries as a `LogicalRepWorker *`; functions documented
//! "caller holds LogicalRepWorkerLock" assume the consumer acquired the lock.

#![allow(non_snake_case)]

use types_core::primitive::Oid;
use types_error::PgResult;
use types_replication_applyparallel::ParallelApplyWorkerInfo;
use types_replication_launcher::LogicalRepWorkerType;

seam_core::seam!(
    /// `AtEOXact_ApplyLauncher(isCommit)` — wake/forget logical-rep launcher
    /// work queued in this transaction. (Void in C; the launcher's fallible
    /// internal returns are absorbed at install time since the only side effect
    /// — `SetLatch` via `kill(2)` — is infallible.)
    pub fn at_eoxact_apply_launcher(is_commit: bool)
);

seam_core::seam!(
    /// `logicalrep_worker_launch(wtype, dbid, subid, subname, userid, relid,
    /// subworker_dsm)` — start a logical-replication worker for the
    /// subscription; returns whether it attached successfully.
    pub fn logicalrep_worker_launch(
        wtype: LogicalRepWorkerType,
        dbid: Oid,
        subid: Oid,
        subname: &str,
        userid: Oid,
        relid: Oid,
        subworker_dsm: u32,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `logicalrep_worker_stop(subid, relid)` — stop the apply/tablesync worker
    /// for the given subscription (and relation, for tablesync).
    pub fn logicalrep_worker_stop(subid: Oid, relid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `logicalrep_pa_worker_stop(winfo)` — stop a parallel apply worker
    /// (sends SIGUSR2 for a clean exit).
    pub fn logicalrep_pa_worker_stop(winfo: &mut ParallelApplyWorkerInfo) -> PgResult<()>
);

seam_core::seam!(
    /// `logicalrep_worker_wakeup(subid, relid)` — wake (via latch) the worker
    /// for the given sub/rel.
    pub fn logicalrep_worker_wakeup(subid: Oid, relid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `logicalrep_worker_wakeup_ptr(worker)` — wake the worker at slot
    /// `slot`. Caller must hold `LogicalRepWorkerLock`.
    pub fn logicalrep_worker_wakeup_ptr(slot: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `logicalrep_worker_attach(slot)` — attach this backend to the worker
    /// slot `slot` (sets `MyLogicalRepWorker`).
    pub fn logicalrep_worker_attach(slot: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `logicalrep_worker_find(subid, relid, only_running)` — the leader apply
    /// or tablesync worker slot for sub/rel, or `None` (C NULL). Caller must
    /// hold `LogicalRepWorkerLock`.
    pub fn logicalrep_worker_find(subid: Oid, relid: Oid, only_running: bool) -> Option<i32>
);

seam_core::seam!(
    /// `logicalrep_sync_worker_count(subid)` — number of registered tablesync
    /// workers for the subscription. Caller must hold `LogicalRepWorkerLock`.
    pub fn logicalrep_sync_worker_count(subid: Oid) -> i32
);

seam_core::seam!(
    /// `GetLeaderApplyWorkerPid(pid)` — leader apply worker PID if `pid` is a
    /// parallel apply worker, else `InvalidPid`.
    pub fn GetLeaderApplyWorkerPid(pid: i32) -> PgResult<i32>
);

seam_core::seam!(
    /// `IsLogicalLauncher()` — is the current process the logical-replication
    /// launcher?
    pub fn IsLogicalLauncher() -> bool
);

seam_core::seam!(
    /// `ApplyLauncherWakeupAtCommit()` — request a launcher wakeup at the next
    /// transaction commit.
    pub fn ApplyLauncherWakeupAtCommit()
);

seam_core::seam!(
    /// `ApplyLauncherForgetWorkerStartTime(subid)` — drop the subscription's
    /// last-start-time entry so its apply worker can restart immediately.
    pub fn ApplyLauncherForgetWorkerStartTime(subid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `ApplyLauncherShmemSize()` (ipci.c `CalculateShmemSize` accumulator) —
    /// shared-memory bytes this subsystem needs. Infallible in C, so the seam
    /// returns a bare `Size`.
    pub fn apply_launcher_shmem_size() -> types_core::Size
);

seam_core::seam!(
    /// `ApplyLauncherShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn apply_launcher_shmem_init() -> types_error::PgResult<()>
);
