//! Seam declarations for the `backend-replication-logical-tablesync` unit
//! (`replication/logical/tablesync.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `AllTablesyncsReady()` (tablesync.c): true iff the subscription has
    /// tables and every one is in the READY sync state. Refreshes table-state
    /// info and may `CommitTransactionCommand()` / `pgstat_report_stat()`
    /// internally, so it can `ereport(ERROR)` — carried on `Err`.
    pub fn all_tablesyncs_ready() -> types_error::PgResult<bool>
);
