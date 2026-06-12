//! Seam declarations for the `backend-utils-time-snapmgr` unit
//! (`utils/time/snapmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! Note: per `docs/query-lifecycle-raii.md` the ActiveSnapshot stack
//! ultimately ports as an owned `SnapshotStack` facet. These two seams mirror
//! the single C call pair in `RemoveTempRelationsCallback`
//! (`PushActiveSnapshot(GetTransactionSnapshot())` / `PopActiveSnapshot()`)
//! until the snapmgr port lands and the callback is restructured around the
//! owned stack.

use types_error::PgResult;

seam_core::seam!(
    /// `PushActiveSnapshot(GetTransactionSnapshot())` (snapmgr.c). Allocates
    /// and can `ereport(ERROR)`, carried on `Err`.
    pub fn push_active_snapshot_for_transaction() -> PgResult<()>
);

seam_core::seam!(
    /// `PopActiveSnapshot()` (snapmgr.c).
    pub fn pop_active_snapshot() -> PgResult<()>
);
