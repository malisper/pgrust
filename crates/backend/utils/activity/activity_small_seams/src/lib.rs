//! Seam declarations for the `backend-utils-activity-small` unit
//! (`utils/activity/backend_progress.c`). The owning unit installs these from
//! its `init_seams()` when the cross-crate-cycle paths land; until then a call
//! panics loudly.

#![allow(non_snake_case)]

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `pgstat_progress_start_command(cmdtype, relid)`.
    pub fn pgstat_progress_start_command(cmdtype: i32, relid: Oid) -> PgResult<()>
);
seam_core::seam!(
    /// `pgstat_progress_update_param(index, val)`.
    pub fn pgstat_progress_update_param(index: i32, val: i64) -> PgResult<()>
);
seam_core::seam!(
    /// `pgstat_progress_end_command()`.
    pub fn pgstat_progress_end_command() -> PgResult<()>
);
