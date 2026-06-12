//! Seam declarations for the `backend-utils-activity-status` unit
//! (`utils/activity/backend_status.c`): the process-global backend status
//! entry (`MyBEEntry`), the `pgstat_track_activities` GUC, and the
//! `st_changecount` write-activity protocol.
//!
//! `backend_progress.c` writes the `st_progress_*` fields of its own backend
//! entry between `PGSTAT_BEGIN_WRITE_ACTIVITY` / `PGSTAT_END_WRITE_ACTIVITY`;
//! the entry itself (and the macros) are owned by `backend_status.c`, so the
//! field operations are reached through these per-field slots rather than by
//! copying the whole `PgBackendStatus` struct. The owning unit installs them
//! from its `init_seams()` when it lands; until then a call panics loudly.

use types_core::{int64, Oid};
use types_pgstat::backend_progress::ProgressCommandType;

seam_core::seam!(
    /// `MyBEEntry != NULL` — is the backend status entry initialized?
    pub fn my_be_entry_present() -> bool
);

seam_core::seam!(
    /// The `pgstat_track_activities` GUC (`backend_status.c`).
    pub fn track_activities() -> bool
);

seam_core::seam!(
    /// `PGSTAT_BEGIN_WRITE_ACTIVITY(MyBEEntry)` (`utils/backend_status.h`).
    pub fn begin_write_activity()
);

seam_core::seam!(
    /// `PGSTAT_END_WRITE_ACTIVITY(MyBEEntry)` (`utils/backend_status.h`).
    pub fn end_write_activity()
);

seam_core::seam!(
    /// `MyBEEntry->st_progress_command = cmdtype`.
    pub fn set_progress_command(cmdtype: ProgressCommandType)
);

seam_core::seam!(
    /// `MyBEEntry->st_progress_command_target = relid`.
    pub fn set_progress_command_target(relid: Oid)
);

seam_core::seam!(
    /// Read `MyBEEntry->st_progress_command`.
    pub fn progress_command() -> ProgressCommandType
);

seam_core::seam!(
    /// `MemSet(&MyBEEntry->st_progress_param, 0, sizeof(...))`.
    pub fn zero_progress_param()
);

seam_core::seam!(
    /// `MyBEEntry->st_progress_param[index] = val`.
    pub fn set_progress_param(index: i32, val: int64)
);

seam_core::seam!(
    /// `MyBEEntry->st_progress_param[index] += incr`.
    pub fn incr_progress_param(index: i32, incr: int64)
);
