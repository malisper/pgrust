//! Seam declarations for the `backend-access-transam-xlogarchive` unit
//! (`access/transam/xlogarchive.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `XLogArchiveForceDone(fname)` — create a `.done` file to prevent the
    /// segment from being archived later; `ereport(ERROR)` on I/O failure.
    pub fn xlog_archive_force_done(fname: String) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `XLogArchiveNotify(fname)` — create the `.ready` archive-notification
    /// file; `ereport(ERROR)` on I/O failure.
    pub fn xlog_archive_notify(fname: String) -> types_error::PgResult<()>
);
