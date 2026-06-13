//! Seam declarations for the `backend-utils-activity-stat` unit (the per-kind
//! stats implementation files `pgstat_io.c`, `pgstat_database.c`,
//! `pgstat_relation.c`, ... bundled by the catalog).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `pgstat_flush_io(bool nowait)` (`utils/activity/pgstat_io.c`) — flush
    /// the backend's pending IO statistics. Returns true if some stats could
    /// not be flushed because of contention (`pgstat_io_flush_cb`'s result).
    /// `Err` carries `LWLockAcquire`'s `elog(ERROR, "too many LWLocks
    /// taken")` on the blocking (`!nowait`) path.
    pub fn pgstat_flush_io(nowait: bool) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `pgstat_twophase_postcommit(xid, info, recdata, len)` — apply the
    /// prepared transaction's per-table stats deltas on COMMIT PREPARED (slot
    /// `TWOPHASE_RM_PGSTAT_ID` of `twophase_postcommit_callbacks`).
    pub fn pgstat_twophase_postcommit(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgstat_twophase_postabort(xid, info, recdata, len)` — apply the
    /// prepared transaction's per-table stats deltas on ROLLBACK PREPARED
    /// (slot `TWOPHASE_RM_PGSTAT_ID` of `twophase_postabort_callbacks`).
    pub fn pgstat_twophase_postabort(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `AtEOXact_PgStat_Database(isCommit, parallel)` (`pgstat_database.c`) —
    /// count one transaction commit/abort in the backend-local
    /// `pgStatXactCommit`/`pgStatXactRollback` counters (skipped for parallel
    /// workers). Pure counter bumps; infallible.
    pub fn at_eoxact_pgstat_database(is_commit: bool, parallel: bool)
);

seam_core::seam!(
    /// `AtEOXact_PgStat_Relations(xact_state, isCommit)` (`pgstat_relation.c`)
    /// — fold each top-level `PgStat_TableXactStatus` node on `xact_state`'s
    /// `first` chain into its table's pending stats. Counter arithmetic over
    /// existing nodes; infallible.
    pub fn at_eoxact_pgstat_relations(
        xact_state: &mut types_pgstat::activity_pgstat::PgStat_SubXactStatus,
        is_commit: bool,
    )
);

seam_core::seam!(
    /// `AtEOSubXact_PgStat_Relations(xact_state, isCommit, nestDepth)`
    /// (`pgstat_relation.c`) — merge the subtransaction's table-stats nodes
    /// (`xact_state`'s `first` chain) into the parent level (commit) or fold
    /// them back into the tables' pending stats (abort). `Err` carries the
    /// out-of-memory `ereport(ERROR)` reachable through
    /// `pgstat_get_xact_stack_level`'s `MemoryContextAlloc` on the
    /// relink-to-missing-parent path.
    pub fn at_eosubxact_pgstat_relations(
        xact_state: &mut types_pgstat::activity_pgstat::PgStat_SubXactStatus,
        is_commit: bool,
        nest_depth: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `AtPrepare_PgStat_Relations(xact_state)` (`pgstat_relation.c`) —
    /// serialize each level-1 table-stats node on `xact_state`'s `first`
    /// chain into a `TwoPhasePgStatRecord` via `RegisterTwoPhaseRecord`.
    /// `Err` carries the palloc/repalloc out-of-memory `ereport(ERROR)`
    /// reachable through `RegisterTwoPhaseRecord`'s records-buffer growth.
    pub fn at_prepare_pgstat_relations(
        xact_state: &mut types_pgstat::activity_pgstat::PgStat_SubXactStatus,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// elog.c's FATAL path lets the cumulative stats system know the session
    /// terminated abnormally: `if (pgStatSessionEndCause == DISCONNECT_NORMAL)
    /// pgStatSessionEndCause = DISCONNECT_FATAL;` (the `pgStatSessionEndCause`
    /// global lives in `pgstat_database.c:32`). Only marks the session as
    /// terminated by fatal error if there is no other known cause.
    pub fn pgstat_set_session_end_cause_fatal()
);

seam_core::seam!(
    /// `PostPrepare_PgStat_Relations(xact_state)` (`pgstat_relation.c`) —
    /// detach and free the level-1 table-stats nodes on `xact_state`'s
    /// `first` chain after a successful PREPARE (the prepared xact's effects
    /// now live in the 2PC records). Frees only; infallible.
    pub fn post_prepare_pgstat_relations(
        xact_state: &mut types_pgstat::activity_pgstat::PgStat_SubXactStatus,
    )
);

seam_core::seam!(
    /// `pgstat_execute_transactional_drops(ndrops, items, is_commit)`
    /// (`utils/activity/pgstat.c`) — drop the cumulative-stats entries a
    /// finishing prepared transaction recorded. `items` is the raw on-disk
    /// `xl_xact_stats_item[]` slice from the 2PC buffer (the owner decodes it).
    pub fn pgstat_execute_transactional_drops(items: &[u8], nitems: i32) -> types_error::PgResult<()>
);
seam_core::seam!(
    /// `AtEOXact_PgStat(isCommit, parallel=false)` (`pgstat.c`) — end-of-xact
    /// cumulative-stats cleanup for a finished prepared transaction.
    pub fn at_eoxact_pgstat(is_commit: bool) -> types_error::PgResult<()>
);
