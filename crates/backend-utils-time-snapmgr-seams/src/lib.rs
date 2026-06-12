//! Seam declarations for the `backend-utils-time-snapmgr` unit
//! (`utils/time/snapmgr.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::CommandId;
use types_error::PgResult;

seam_core::seam!(
    /// `SnapshotSetCommandId(curcid)` — propagate the new command id into the
    /// static snapshots. Pure field updates; cannot `ereport`.
    pub fn snapshot_set_command_id(curcid: CommandId)
);

seam_core::seam!(
    /// `AtEOXact_Snapshot(isCommit, resetXmin)` — snapshot cleanup at
    /// transaction end (WARNs about leaks at commit; can `ereport(ERROR)` on
    /// exported-snapshot file cleanup).
    pub fn at_eoxact_snapshot(is_commit: bool, reset_xmin: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `AtSubCommit_Snapshot(level)`.
    pub fn at_subcommit_snapshot(level: i32)
);

seam_core::seam!(
    /// `AtSubAbort_Snapshot(level)`.
    pub fn at_subabort_snapshot(level: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `XactHasExportedSnapshots()` — true after `pg_export_snapshot`, which
    /// forbids PREPARE.
    pub fn xact_has_exported_snapshots() -> bool
);
