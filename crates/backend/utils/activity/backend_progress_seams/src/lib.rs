//! Seam declarations for the `backend-utils-activity-backend-progress` unit
//! (`utils/activity/backend_progress.c`): the command-progress reporting
//! the COPY / VACUUM / CLUSTER / etc. drivers call to publish
//! `pg_stat_progress_*` view rows.
//!
//! These write the reporting backend's own `MyBEEntry->st_progress_*` fields
//! (through `backend_status.c`'s entry) and never `ereport` at ERROR, so they
//! are infallible. The owning unit installs them from its `init_seams()` when
//! it lands; until then a call panics loudly.

use ::types_core::Oid;
use ::types_pgstat::backend_progress::ProgressCommandType;

seam_core::seam!(
    /// `pgstat_progress_start_command(cmdtype, relid)` (backend_progress.c):
    /// begin progress reporting for `cmdtype` against `relid`, clearing the
    /// progress parameter array.
    pub fn pgstat_progress_start_command(cmdtype: ProgressCommandType, relid: Oid)
);

seam_core::seam!(
    /// `pgstat_progress_update_param(index, val)` (backend_progress.c): set one
    /// progress parameter (`st_progress_param[index] = val`).
    pub fn pgstat_progress_update_param(index: i32, val: i64)
);

seam_core::seam!(
    /// `pgstat_progress_update_multi_param(nparam, index, val)`
    /// (backend_progress.c): set several progress parameters at once. The two
    /// slices are parallel (`index[i] -> val[i]`).
    pub fn pgstat_progress_update_multi_param(index: &[i32], val: &[i64])
);

seam_core::seam!(
    /// `pgstat_progress_end_command()` (backend_progress.c): stop progress
    /// reporting for this backend (`st_progress_command = PROGRESS_COMMAND_INVALID`).
    pub fn pgstat_progress_end_command()
);
