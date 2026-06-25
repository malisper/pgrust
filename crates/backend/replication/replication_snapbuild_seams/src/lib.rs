//! Seam declarations for the `backend-replication-logical-snapbuild` unit
//! (`replication/logical/snapbuild.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use ::types_core::primitive::XLogRecPtr;
use ::types_error::PgResult;

seam_core::seam!(
    /// `SnapBuildSnapshotExists(lsn)` (snapbuild.c) — does a serialized
    /// historic snapshot exist on disk for `lsn`? Pure on-disk existence check
    /// (errors are downgraded internally), so infallible here.
    pub fn snap_build_snapshot_exists(lsn: XLogRecPtr) -> bool
);

seam_core::seam!(
    /// `CheckPointSnapBuild()` (snapbuild.c:1969) — remove serialized historic
    /// snapshots from `pg_logical/snapshots` that no slot needs anymore. Called
    /// from `CheckPointGuts` (xlog.c:7578) even when logical decoding is
    /// disabled, so stale snapshots are eventually reclaimed. Dir scan / unlink
    /// can `ereport`, carried on `Err`.
    pub fn check_point_snap_build() -> PgResult<()>
);

seam_core::seam!(
    /// `SnapBuildClearExportedSnapshot()` (snapbuild.c) — drop any snapshot
    /// exported by a previous `CREATE_REPLICATION_SLOT ... LOGICAL` command
    /// (cleaned up before the next replication command). Can `ereport` if it
    /// must abort the snapshot-exporting transaction.
    pub fn snap_build_clear_exported_snapshot() -> PgResult<()>
);
