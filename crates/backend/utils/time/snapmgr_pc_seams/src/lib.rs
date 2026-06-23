//! plancache's slice of the snapshot manager (`utils/time/snapmgr.c`). The
//! `ActiveSnapshot` stack is the `SnapshotStack` facet in this repo's
//! query-lifecycle model (`docs/query-lifecycle-raii.md`); the snapmgr port
//! installs these. Until then a call panics loudly.

use ::types_error::PgResult;

seam_core::seam!(
    /// `ActiveSnapshotSet()`.
    pub fn active_snapshot_set() -> PgResult<bool>
);

seam_core::seam!(
    /// `PushActiveSnapshot(GetTransactionSnapshot())`.
    pub fn push_active_snapshot_transaction() -> PgResult<()>
);

seam_core::seam!(
    /// `PopActiveSnapshot()`.
    pub fn pop_active_snapshot() -> PgResult<()>
);

seam_core::seam!(
    /// `TransactionXmin` (the backend-global).
    pub fn transaction_xmin() -> PgResult<u32>
);

seam_core::seam!(
    /// `RecentXmin` (snapmgr.c) — the xmin of the most recently taken snapshot.
    /// Read by procarray.c's `GlobalVisTestShouldUpdate` / `GlobalVisUpdateApply`
    /// to decide whether a horizon recompute is worthwhile and to record the
    /// last-update xmin. Pure backend-global read; cannot `ereport`.
    pub fn recent_xmin() -> u32
);

seam_core::seam!(
    /// `TransactionXmin = xmin;` — store the backend-global oldest-snapshot xmin
    /// (snapmgr.c owns it). Written by procarray.c's
    /// `ProcArrayInstall{Imported,Restored}Xmin` atomically with `MyProc->xmin`.
    pub fn set_transaction_xmin(xmin: u32)
);
