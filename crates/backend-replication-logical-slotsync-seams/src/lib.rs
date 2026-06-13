//! Seam declarations for the `backend-replication-logical-slotsync` unit
//! (`replication/logical/slotsync.c`) — slotsync's external surface, called
//! across dependency cycles by its (not-yet-ported) cycle partners:
//! xlogrecovery.c (`ShutDownSlotSync`), the postmaster (`ValidateSlotSyncParams`,
//! `SlotSyncWorkerCanRestart`, `ReplSlotSyncWorkerMain`, `SlotSyncShmem*`),
//! walsender/logical.c (`IsSyncingReplicationSlots`), and slotfuncs.c
//! (`SyncReplicationSlots`, `CheckAndGetDbnameFromConninfo`).
//!
//! The slotsync crate installs every one of these from its `init_seams()`.

#![allow(non_snake_case)]

use types_core::primitive::Size;
use types_error::PgResult;
use types_replication::WrConnHandle;

seam_core::seam!(
    /// `ShutDownSlotSync()` — stop the slot-sync worker / SQL sync during
    /// standby promotion and update synced slots' inactive_since.
    pub fn shut_down_slot_sync() -> PgResult<()>
);

seam_core::seam!(
    /// `ValidateSlotSyncParams(elevel)` — check the GUCs required for slot
    /// synchronization. `Ok(true)` when all are set; `Ok(false)` when a report
    /// was emitted at a sub-ERROR `elevel`; `Err` when `elevel >= ERROR`.
    pub fn validate_slot_sync_params(elevel: i32) -> PgResult<bool>
);

seam_core::seam!(
    /// `SlotSyncWorkerCanRestart()` — throttle worker restarts to once per
    /// `SLOTSYNC_RESTART_INTERVAL_SEC`.
    pub fn slot_sync_worker_can_restart() -> PgResult<bool>
);

seam_core::seam!(
    /// `IsSyncingReplicationSlots()` — is the current process performing slot
    /// synchronization? Infallible.
    pub fn is_syncing_replication_slots() -> bool
);

seam_core::seam!(
    /// `SyncReplicationSlots(wrconn)` — synchronize failover slots over the
    /// given primary connection (the SQL `pg_sync_replication_slots()` path).
    pub fn sync_replication_slots(wrconn: WrConnHandle) -> PgResult<()>
);

seam_core::seam!(
    /// `CheckAndGetDbnameFromConninfo()` — extract and require `dbname` from
    /// `primary_conninfo`.
    pub fn check_and_get_dbname_from_conninfo() -> PgResult<String>
);

seam_core::seam!(
    /// `ReplSlotSyncWorkerMain(startup_data, startup_data_len)` — the slot-sync
    /// worker entry point. `startup_data_len` must be 0.
    pub fn repl_slot_sync_worker_main(startup_data_len: usize) -> PgResult<()>
);

seam_core::seam!(
    /// `SlotSyncShmemSize()` — bytes of shared memory the control area needs.
    pub fn slot_sync_shmem_size() -> Size
);

seam_core::seam!(
    /// `SlotSyncShmemInit()` — allocate and initialize the control area.
    pub fn slot_sync_shmem_init() -> PgResult<()>
);
