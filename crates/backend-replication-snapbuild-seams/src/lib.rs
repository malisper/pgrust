//! Seam declarations for the `backend-replication-logical-snapbuild` unit
//! (`replication/logical/snapbuild.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::primitive::XLogRecPtr;

seam_core::seam!(
    /// `SnapBuildSnapshotExists(lsn)` (snapbuild.c) — does a serialized
    /// historic snapshot exist on disk for `lsn`? Pure on-disk existence check
    /// (errors are downgraded internally), so infallible here.
    pub fn snap_build_snapshot_exists(lsn: XLogRecPtr) -> bool
);
