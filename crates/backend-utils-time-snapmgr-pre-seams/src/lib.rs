//! PREPARE/EXECUTE's consumer slice of `utils/time/snapmgr.c`.
//!
//! The base `backend-utils-time-snapmgr-seams` crate is portalcmds' slice (it
//! models the active-snapshot stack against the real shared
//! `Rc<types_snapshot::SnapshotData>`). PREPARE/EXECUTE carries the live
//! `Snapshot` it hands to `PortalStart` as the opaque
//! [`types_scan::snapshot::SnapshotHandle`] (inherited opacity,
//! docs/types.md rule 6), so it gets its own slice here.
//!
//! The owning unit installs this from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `GetActiveSnapshot()` (snapmgr.c): the topmost active snapshot, or the
    /// C `NULL` (`None`) if none is set. The live `Snapshot` belongs to the
    /// snapmgr active-snapshot stack, so it crosses as the opaque
    /// [`types_scan::snapshot::SnapshotHandle`] the consumer hands to
    /// `PortalStart`.
    pub fn get_active_snapshot() -> Option<types_scan::snapshot::SnapshotHandle>
);
