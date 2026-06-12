//! Seam declarations for the `backend-utils-activity-pgstat` unit
//! (`utils/activity/pgstat.c`): the `pgStatLocal` shared-memory control block
//! and per-backend snapshot, plus the cross-kind helpers the per-kind stats
//! files call.
//!
//! The `with_shmem_*`/`with_snapshot_*` slots run a caller-supplied callback
//! against the live shmem-resident / snapshot-resident per-kind structs,
//! mirroring C's `&pgStatLocal.shmem-><kind>` / `&pgStatLocal.snapshot.<kind>`
//! pointers. (A callback rather than a returned `&'static mut`: aliasable
//! mutable statics are unsound in Rust.) The owning unit installs these from
//! its `init_seams()` when it lands; until then a call panics loudly.

use types_pgstat::activity_pgstat::{
    PgStatShared_Archiver, PgStatShared_Checkpointer, PgStat_ArchiverStats,
    PgStat_CheckpointerStats,
};
use types_pgstat::backend_utils_activity_pgstat_bgwriter::{
    PgStatShared_BgWriter, PgStat_BgWriterStats,
};

seam_core::seam!(
    /// Run `f` on `&pgStatLocal.shmem->archiver`.
    pub fn with_shmem_archiver(f: &mut dyn FnMut(&mut PgStatShared_Archiver))
);

seam_core::seam!(
    /// Run `f` on `&pgStatLocal.snapshot.archiver`.
    pub fn with_snapshot_archiver(f: &mut dyn FnMut(&mut PgStat_ArchiverStats))
);

seam_core::seam!(
    /// Run `f` on `&pgStatLocal.shmem->bgwriter`.
    pub fn with_shmem_bgwriter(f: &mut dyn FnMut(&mut PgStatShared_BgWriter))
);

seam_core::seam!(
    /// Run `f` on `&pgStatLocal.snapshot.bgwriter`.
    pub fn with_snapshot_bgwriter(f: &mut dyn FnMut(&mut PgStat_BgWriterStats))
);

seam_core::seam!(
    /// Run `f` on `&pgStatLocal.shmem->checkpointer`.
    pub fn with_shmem_checkpointer(f: &mut dyn FnMut(&mut PgStatShared_Checkpointer))
);

seam_core::seam!(
    /// Run `f` on `&pgStatLocal.snapshot.checkpointer`.
    pub fn with_snapshot_checkpointer(f: &mut dyn FnMut(&mut PgStat_CheckpointerStats))
);

seam_core::seam!(
    /// `pgStatLocal.shmem->is_shutdown` (read for the `Assert` in the report
    /// paths).
    pub fn shmem_is_shutdown() -> bool
);

seam_core::seam!(
    /// `pgstat_assert_is_up()` (`utils/pgstat_internal.h` / `pgstat.c`) — a
    /// no-op macro outside `USE_ASSERT_CHECKING`; infallible.
    pub fn assert_is_up()
);

seam_core::seam!(
    /// `pgstat_snapshot_fixed(PgStat_Kind kind)` (`pgstat.c`). `Err` carries
    /// the `ereport(ERROR)`s reachable through `pgstat_build_snapshot`
    /// (palloc / dsa out-of-memory) and the per-kind `snapshot_cb`s'
    /// `LWLockAcquire` (`too many LWLocks taken`).
    pub fn snapshot_fixed(kind: u32) -> types_error::PgResult<()>
);
