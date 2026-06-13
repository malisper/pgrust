//! plancache's slice of the snapshot manager (`utils/time/snapmgr.c`). The
//! `ActiveSnapshot` stack is the `SnapshotStack` facet in this repo's
//! query-lifecycle model (`docs/query-lifecycle-raii.md`); the snapmgr port
//! installs these. Until then a call panics loudly.

use types_error::PgResult;

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
