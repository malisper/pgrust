use types_core::{RepOriginId, TimestampTz, TransactionId};
use types_error::PgResult;

seam_core::seam!(
    pub fn transaction_tree_set_commit_ts_data<'a>(
        xid: TransactionId,
        children: &'a [TransactionId],
        timestamp: TimestampTz,
        node_id: RepOriginId,
    ) -> PgResult<()>
);

seam_core::seam!(
    // StartupCommitTs() (commit_ts.c).
    pub fn startup_commit_ts() -> PgResult<()>
);

seam_core::seam!(
    // CompleteCommitTsInitialization() (commit_ts.c).
    pub fn complete_commit_ts_initialization() -> PgResult<()>
);

seam_core::seam!(
    // CheckPointCommitTs() (commit_ts.c).
    pub fn check_point_commit_ts() -> PgResult<()>
);

seam_core::seam!(
    // SetCommitTsLimit(oldestXact, newestXact) (commit_ts.c).
    pub fn set_commit_ts_limit(
        oldest_xact: TransactionId,
        newest_xact: TransactionId,
    ) -> PgResult<()>
);

seam_core::seam!(
    // CommitTsParameterChange(newvalue, oldvalue) (commit_ts.c); xlog_redo's
    // XLOG_PARAMETER_CHANGE arm.
    pub fn commit_ts_parameter_change(newvalue: bool, oldvalue: bool) -> PgResult<()>
);

seam_core::seam!(
    // ExtendCommitTs (commit_ts.c); GetNewTransactionId's per-assignment call.
    pub fn extend_commit_ts(newest_xact: TransactionId) -> PgResult<()>
);
