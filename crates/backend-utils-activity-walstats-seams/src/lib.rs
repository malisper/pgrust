//! Seam declarations for the `backend-utils-activity-walstats` unit
//! (`utils/activity/pgstat_wal.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `pgstat_report_wal(force)` (pgstat_wal.c) — flush this backend's
    /// pending WAL / WAL-IO statistics to the cumulative stats system. The
    /// summarizer calls it as `pgstat_report_wal(false)`. The flush takes a
    /// shared lock and can `ereport` on stats-file trouble, carried on `Err`.
    pub fn pgstat_report_wal(force: bool) -> types_error::PgResult<()>
);
