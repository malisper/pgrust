//! Seam declarations for the `backend-utils-activity-stat` unit (the per-kind
//! stats implementation files `pgstat_io.c`, `pgstat_database.c`, ... bundled
//! by the catalog).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `pgstat_flush_io(bool nowait)` (`utils/activity/pgstat_io.c`) — flush
    /// the backend's pending IO statistics. Returns true if some stats could
    /// not be flushed because of contention.
    pub fn pgstat_flush_io(nowait: bool) -> bool
);
