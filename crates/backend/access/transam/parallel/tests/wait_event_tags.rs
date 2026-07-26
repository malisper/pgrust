//! GL-SYNCWEDGE-1: the parallel leader's park quantum must publish itself
//! under its OWN name.
//!
//! `WAIT_EVENT_PARALLEL_FINISH` was `PG_WAIT_IPC + 32`, and 32 is
//! `LOGICAL_SYNC_STATE_CHANGE` in wait_event_names.txt's IPC section
//! (`PARALLEL_FINISH` is 40). Every leader parked in the finish quantum —
//! which in this tree is every lanev2 runtime engine's submit-and-park loop,
//! not just `WaitForParallelWorkersToFinish` — therefore showed up in
//! `pg_stat_activity` as `IPC / LogicalSyncStateChange`: a logical-replication
//! wait event on a plain analytic query. A production wedge investigation
//! started from that reading and had to be walked back.
//!
//! The value is only ever read back through a name table, so no other test in
//! the tree could see it. This one closes it at the constant, and
//! `scripts/lint-waitevent-tags.sh` closes it for the whole family.

#[test]
fn parallel_finish_quantum_reports_as_parallel_finish() {
    assert_eq!(
        waitevent::pgstat_get_wait_event_type(parallel::WAIT_EVENT_PARALLEL_FINISH),
        Some("IPC"),
    );
    assert_eq!(
        waitevent::pgstat_get_wait_event(parallel::WAIT_EVENT_PARALLEL_FINISH),
        Some("ParallelFinish"),
        "the leader park quantum is publishing some other event's name",
    );
}
