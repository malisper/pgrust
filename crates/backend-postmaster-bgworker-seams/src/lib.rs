//! Seam declarations for the `backend-postmaster-bgworker` unit
//! (`src/backend/postmaster/bgworker.c`). The owning unit installs these from its
//! `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `BackgroundWorkerMain(startup_data, startup_data_len)` (`src/backend/postmaster/bgworker.c`): child entry
    /// point invoked by `postmaster_child_launch`; never returns.
    pub fn background_worker_main(startup_data: &types_startup::StartupData) -> !
);

seam_core::seam!(
    /// `GetBackgroundWorkerPid(handle, &pid)` (`postmaster/bgworker.c`) —
    /// report the state of the worker named by `handle`; the returned pid is
    /// meaningful only when the status is `Started` (C writes `*pidp` for
    /// `Started`/`Stopped` only). Infallible in C (slot read under the slot
    /// spinlock; no ereport path).
    pub fn get_background_worker_pid(
        handle: types_bgworker::BackgroundWorkerHandle,
    ) -> (types_bgworker::BgwHandleStatus, i32)
);
