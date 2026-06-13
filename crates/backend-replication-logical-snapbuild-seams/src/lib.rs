//! Seam declarations for the `backend-replication-logical-snapbuild` unit
//! (`replication/logical/snapbuild.c`). The owning unit installs these from
//! its `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `SnapBuildResetExportedSnapshotState()` — reset snapshot-export state
    /// on abort.
    pub fn snap_build_reset_exported_snapshot_state()
);
