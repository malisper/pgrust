//! Seam declarations for the `backend-utils-activity-pgstat` unit
//! (`utils/activity/pgstat.c`): the `pgStatLocal` shared-memory control block
//! and per-backend snapshot, plus the cross-kind helpers the per-kind stats
//! files call.
//!
//! The `shmem_*`/`snapshot_*` slots hand out the live shmem-resident /
//! snapshot-resident per-kind structs, mirroring C's
//! `&pgStatLocal.shmem-><kind>` / `&pgStatLocal.snapshot.<kind>` pointers.
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_pgstat::activity_pgstat::{
    PgStatShared_Archiver, PgStatShared_Checkpointer, PgStat_ArchiverStats,
    PgStat_CheckpointerStats,
};
use types_pgstat::backend_utils_activity_pgstat_bgwriter::{
    PgStatShared_BgWriter, PgStat_BgWriterStats,
};

seam_core::seam!(
    /// `&pgStatLocal.shmem->archiver`.
    pub fn shmem_archiver() -> &'static mut PgStatShared_Archiver
);

seam_core::seam!(
    /// `&pgStatLocal.snapshot.archiver`.
    pub fn snapshot_archiver() -> &'static mut PgStat_ArchiverStats
);

seam_core::seam!(
    /// `&pgStatLocal.shmem->bgwriter`.
    pub fn shmem_bgwriter() -> &'static mut PgStatShared_BgWriter
);

seam_core::seam!(
    /// `&pgStatLocal.snapshot.bgwriter`.
    pub fn snapshot_bgwriter() -> &'static mut PgStat_BgWriterStats
);

seam_core::seam!(
    /// `&pgStatLocal.shmem->checkpointer`.
    pub fn shmem_checkpointer() -> &'static mut PgStatShared_Checkpointer
);

seam_core::seam!(
    /// `&pgStatLocal.snapshot.checkpointer`.
    pub fn snapshot_checkpointer() -> &'static mut PgStat_CheckpointerStats
);

seam_core::seam!(
    /// `pgStatLocal.shmem->is_shutdown` (read for the `Assert` in the report
    /// paths).
    pub fn shmem_is_shutdown() -> bool
);

seam_core::seam!(
    /// `pgstat_assert_is_up()` (`utils/pgstat_internal.h` / `pgstat.c`).
    pub fn assert_is_up()
);

seam_core::seam!(
    /// `pgstat_snapshot_fixed(PgStat_Kind kind)` (`pgstat.c`).
    pub fn snapshot_fixed(kind: u32)
);
