//! Seam declarations for the `backend-replication-logical-slotsync` unit
//! (`src/backend/replication/logical/slotsync.c`). The owning unit installs these from its `init_seams()`;
//! until then a call panics loudly.

seam_core::seam!(
    /// `ReplSlotSyncWorkerMain(startup_data, startup_data_len)` (`src/backend/replication/logical/slotsync.c`): child entry
    /// point invoked by `postmaster_child_launch`; never returns.
    pub fn repl_slot_sync_worker_main(startup_data: &types_startup::StartupData) -> !
);

// --- backend-utils-init-postinit consumer (slotsync.c) ---

seam_core::seam!(
    /// `AmLogicalSlotSyncWorkerProcess()` (slotsync.c / miscadmin.h): is this
    /// the logical-slot-sync worker?
    pub fn am_logical_slot_sync_worker_process() -> bool
);
