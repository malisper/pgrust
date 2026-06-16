//! Seam declarations for the subset of the `backend-utils-activity-pgstat-database`
//! unit (`utils/activity/pgstat_database.c`) that `backend_status.c` calls when
//! a backend transitions out of an "active"/"idle in transaction" state. The
//! owning unit installs these from its `init_seams()` when it lands; until then
//! a call panics loudly.

seam_core::seam!(
    /// `pgstat_count_conn_active_time(PgStat_Counter usecs)` (pgstat_database.c):
    /// accumulate time spent actively running queries (`STATE_RUNNING` /
    /// `STATE_FASTPATH`) into this backend's pending per-database stats.
    pub fn pgstat_count_conn_active_time(usecs: i64)
);

seam_core::seam!(
    /// `pgstat_count_conn_txn_idle_time(PgStat_Counter usecs)`
    /// (pgstat_database.c): accumulate time spent idle in a transaction
    /// (`STATE_IDLEINTRANSACTION` / `..._ABORTED`) into this backend's pending
    /// per-database stats.
    pub fn pgstat_count_conn_txn_idle_time(usecs: i64)
);
