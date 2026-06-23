//! Seam declarations for the `backend-utils-activity-pgstat-wal` unit
//! (`utils/activity/pgstat_wal.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `pgstat_report_wal(force)` — flush pending WAL statistics to the
    /// cumulative stats system.
    pub fn pgstat_report_wal(force: bool)
);

seam_core::seam!(
    /// `pgstat_flush_backend(nowait, PGSTAT_BACKEND_FLUSH_WAL)` (pgstat_backend.c)
    /// — flush this backend's per-backend WAL stats. Returns whether some were
    /// left unflushed. Owned by `pgstat_backend.c` (unported); seam-and-panic.
    pub fn pgstat_flush_backend_wal(nowait: bool) -> bool
);
