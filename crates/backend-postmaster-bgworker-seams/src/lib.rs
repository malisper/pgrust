//! Seam declarations for the `backend-postmaster-bgworker` unit
//! (`postmaster/bgworker.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

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
