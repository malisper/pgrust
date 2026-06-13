//! Seam declarations for the `backend-replication-logical-slotsync` unit
//! (`src/backend/replication/logical/slotsync.c`). The owning unit installs
//! these from its `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `ReplSlotSyncWorkerMain(startup_data, startup_data_len)` (slotsync.c):
    /// child entry point invoked by `postmaster_child_launch`; never returns.
    pub fn repl_slot_sync_worker_main(startup_data: &types_startup::StartupData) -> !
);

seam_core::seam!(
    /// `bool IsSyncingReplicationSlots(void)` (slotsync.c) — true while the
    /// slot sync worker (or `pg_sync_replication_slots`) is creating slots.
    pub fn is_syncing_replication_slots() -> bool
);
