use super::*;
use std::sync::atomic::Ordering::Relaxed;
use types_storage::latch::Latch;

fn test_latches() -> &'static [Latch] {
    static L: std::sync::OnceLock<Vec<Latch>> = std::sync::OnceLock::new();
    L.get_or_init(|| (0..4).map(|_| Latch::new(false, 0)).collect())
}

fn clear_latches() {
    for l in test_latches() {
        l.is_set.store(0, Relaxed);
    }
}

// AtEOXact_LogicalRepWorkers / LogicalRepWorkersWakeupAtCommit (worker.c):
// commit of a transaction that altered a subscription wakes that
// subscription's running workers; abort (and PREPARE, which xact treats as
// rollback for worker wakeups) discards the queued requests.
#[test]
fn at_eoxact_logicalrep_workers_wakes_queued_subscriptions_on_commit() {
    // Proc latches resolve through the lmgr_proc seam; back procnos 0..3
    // with test-owned latches (no ProcGlobal needed).
    lmgr_proc_seams::proc_latch::set(|procno| &test_latches()[procno as usize]);

    ApplyLauncherShmemInit();
    with_ctx(|ctx| {
        // Running apply worker of subscription 100.
        ctx.workers[0].in_use = true;
        ctx.workers[0].subid = 100;
        ctx.workers[0].proc_pid = 11;
        ctx.workers[0].proc_no = Some(0);
        // Running apply worker of subscription 200.
        ctx.workers[1].in_use = true;
        ctx.workers[1].subid = 200;
        ctx.workers[1].proc_pid = 12;
        ctx.workers[1].proc_no = Some(1);
        // Launched-but-unattached worker of subscription 100 (pid 0):
        // only_running excludes it.
        ctx.workers[2].in_use = true;
        ctx.workers[2].subid = 100;
        ctx.workers[2].proc_pid = 0;
        ctx.workers[2].proc_no = Some(2);
    });

    // The installer publishes the xact-engine seam.
    init_seams();
    assert!(logical_worker_seams::at_eoxact_logical_rep_workers::is_installed());

    // Queued requests deduplicate (C list_append_unique_oid).
    LogicalRepWorkersWakeupAtCommit(100);
    LogicalRepWorkersWakeupAtCommit(100);
    ON_COMMIT_WAKEUP_WORKERS_SUBIDS.with(|l| assert_eq!(l.borrow().len(), 1));

    // Commit wakes subscription 100's running worker only.
    AtEOXact_LogicalRepWorkers(true);
    assert!(test_latches()[0].is_set());
    assert!(!test_latches()[1].is_set());
    assert!(!test_latches()[2].is_set());
    // The list was consumed.
    ON_COMMIT_WAKEUP_WORKERS_SUBIDS.with(|l| assert!(l.borrow().is_empty()));

    // Abort discards the queued request: nothing wakes, now or at the next
    // commit.
    clear_latches();
    LogicalRepWorkersWakeupAtCommit(200);
    AtEOXact_LogicalRepWorkers(false);
    assert!(!test_latches()[1].is_set());
    AtEOXact_LogicalRepWorkers(true);
    assert!(!test_latches()[1].is_set());

    // Two queued subscriptions wake both running workers at commit.
    clear_latches();
    LogicalRepWorkersWakeupAtCommit(100);
    LogicalRepWorkersWakeupAtCommit(200);
    AtEOXact_LogicalRepWorkers(true);
    assert!(test_latches()[0].is_set());
    assert!(test_latches()[1].is_set());
    assert!(!test_latches()[2].is_set());
}
