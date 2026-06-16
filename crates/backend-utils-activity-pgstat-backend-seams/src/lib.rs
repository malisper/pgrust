//! Seam declarations for the subset of the `backend-utils-activity-pgstat-backend`
//! unit (`utils/activity/pgstat_backend.c`) that `backend_status.c` calls from
//! `pgstat_bestart_final`. The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `pgstat_create_backend(ProcNumber procnum)` (pgstat_backend.c): create
    /// the per-backend cumulative-statistics entry for the backend at
    /// `procnum`. Called once from `pgstat_bestart_final` for backend types
    /// that track per-backend statistics.
    pub fn pgstat_create_backend(procnum: types_core::ProcNumber)
);

seam_core::seam!(
    /// `pgstat_tracks_backend_bktype(BackendType bktype)` (pgstat_backend.c):
    /// whether the given backend type has per-backend statistics tracked.
    pub fn pgstat_tracks_backend_bktype(bktype: types_core::init::BackendType) -> bool
);
