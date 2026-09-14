use super::*;
use std::sync::atomic::Ordering::Relaxed;
use types_storage::latch::Latch;

// The LogicalRepCtx is process-global: tests that (re)initialize or inspect
// it run one at a time.
static CTX_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
fn ctx_test_guard() -> std::sync::MutexGuard<'static, ()> {
    CTX_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// lock_release_all seam probe: records every (lockmethodid, allLocks) call.
static LOCK_RELEASE_ALL_CALLS: std::sync::Mutex<Vec<(u8, bool)>> = std::sync::Mutex::new(Vec::new());
fn install_lock_release_all_probe() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        lock_seams::lock_release_all::set(|lockmethodid, all_locks| {
            LOCK_RELEASE_ALL_CALLS
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((lockmethodid, all_locks));
            Ok(())
        });
    });
    LOCK_RELEASE_ALL_CALLS.lock().unwrap_or_else(|e| e.into_inner()).clear();
}
fn lock_release_all_calls() -> Vec<(u8, bool)> {
    LOCK_RELEASE_ALL_CALLS.lock().unwrap_or_else(|e| e.into_inner()).clone()
}

// An attached apply worker of subscription 100 in slot 0 (this thread is the
// worker: MY_WORKER_SLOT set, exit callback registered on this thread's
// ipc exit stacks).
fn attach_apply_worker_in_slot0() {
    ApplyLauncherShmemInit();
    with_ctx(|ctx| {
        ctx.workers[0].in_use = true;
        ctx.workers[0].wtype = LogicalRepWorkerType::Apply;
        ctx.workers[0].subid = 100;
    });
    logicalrep_worker_attach(0).unwrap();
    assert_eq!(my_worker_slot(), Some(0));
}

// Whether slot 0 was still in use when the on_shmem_exit probe ran.
static SLOT0_IN_USE_AT_ON_SHMEM_EXIT: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
fn on_shmem_exit_probe(_code: i32, _arg: usize) {
    let in_use = worker_snapshot(0).map(|w| w.in_use).unwrap_or(false);
    SLOT0_IN_USE_AT_ON_SHMEM_EXIT.store(in_use, Relaxed);
}

// logicalrep_worker_onexit (launcher.c:838-848): after detaching, the worker
// releases every session-level lock — LockReleaseAll(DEFAULT_LOCKMETHOD,
// true) — since parallel apply mode takes session locks outside any
// transaction and nothing else would release them.
#[test]
fn worker_exit_releases_all_default_lockmethod_locks() {
    let _g = ctx_test_guard();
    install_lock_release_all_probe();
    attach_apply_worker_in_slot0();

    ipc::shmem_exit(0).unwrap();

    assert_eq!(
        lock_release_all_calls(),
        vec![(types_storage::lock::DEFAULT_LOCKMETHOD, true)],
        "logicalrep_worker_onexit must call LockReleaseAll(DEFAULT_LOCKMETHOD, true) (launcher.c:848)"
    );
    assert_eq!(my_worker_slot(), None);
    assert!(!worker_snapshot(0).unwrap().in_use);
}

// logicalrep_worker_attach registers the exit callback with
// before_shmem_exit (launcher.c:744): it runs BEFORE the on_shmem_exit
// stage (ReplicationOriginExitCleanup, ProcKill, ...), so an on_shmem_exit
// callback registered later already sees the slot released.
#[test]
fn worker_exit_callback_runs_in_the_before_shmem_exit_stage() {
    let _g = ctx_test_guard();
    install_lock_release_all_probe();
    attach_apply_worker_in_slot0();
    SLOT0_IN_USE_AT_ON_SHMEM_EXIT.store(true, Relaxed);
    ipc::on_shmem_exit(on_shmem_exit_probe, 0);

    ipc::shmem_exit(0).unwrap();

    assert!(
        !SLOT0_IN_USE_AT_ON_SHMEM_EXIT.load(Relaxed),
        "the slot must already be detached when on_shmem_exit callbacks run (before_shmem_exit, launcher.c:744)"
    );
    assert_eq!(my_worker_slot(), None);
}

// tablesync.c's last_start_times is a private HTAB keyed by relid
// (tablesync.c:425/618), distinct from the launcher's subid-keyed dshash
// (launcher.c:89): a relation whose OID equals a subscription's OID neither
// reads nor overwrites the apply worker's start time, and
// ApplyLauncherForgetWorkerStartTime never wipes a tablesync throttle entry.
#[test]
fn tablesync_start_times_are_a_separate_table_from_apply_start_times() {
    let _g = ctx_test_guard();
    ApplyLauncherShmemInit();
    // Subscription 100's apply worker started at t = 1s.
    ApplyLauncherSetWorkerStartTime(100, 1_000_000);
    // Relation OID 100: the tablesync table has no entry -> due now (C:
    // !found), and the launcher's entry is untouched.
    assert!(
        tablesync_start_time_check_and_set(100, 100, 2_000_000, 5000),
        "tablesync start time of relid 100 must not read subscription 100's apply start time"
    );
    assert_eq!(ApplyLauncherGetWorkerStartTime(100), 1_000_000);
    // ALTER SUBSCRIPTION forgets the APPLY entry only; the tablesync
    // throttle for relid 100 (set 0.5s ago) still holds.
    ApplyLauncherForgetWorkerStartTime(100);
    assert_eq!(ApplyLauncherGetWorkerStartTime(100), 0);
    assert!(
        !tablesync_start_time_check_and_set(100, 100, 2_500_000, 5000),
        "ApplyLauncherForgetWorkerStartTime(100) must not wipe the tablesync throttle of relid 100"
    );
}

// ApplyLauncherMain's throttle uses TimestampDifferenceMilliseconds
// (launcher.c:1207; timestamp.c:1761): a clock step backwards counts as 0
// elapsed (wait the full interval, never longer), and fractional
// milliseconds round UP.
#[test]
fn apply_worker_restart_wait_follows_timestamp_difference_milliseconds() {
    // now < last_start: elapsed 0 -> wait exactly wal_retrieve_retry_interval.
    assert_eq!(apply_worker_restart_wait_ms(10_000_000, 4_000_000, 5000), Some(5000));
    // 4999.001 ms elapsed rounds up to 5000 -> eligible now.
    assert_eq!(apply_worker_restart_wait_ms(1_000_000, 1_000_000 + 4_999_001, 5000), None);
    // 4998.5 ms rounds up to 4999 -> wait 1 ms.
    assert_eq!(apply_worker_restart_wait_ms(1_000_000, 1_000_000 + 4_998_500, 5000), Some(1));
    // Never started -> eligible.
    assert_eq!(apply_worker_restart_wait_ms(0, 123, 5000), None);
}

// logicalrep_worker_launch's slot GC uses TimestampDifferenceExceeds
// (launcher.c:388; timestamp.c:1785: diff >= msec * 1000): a slot launched
// exactly wal_receiver_timeout ago is collected on this cycle.
#[test]
fn stuck_worker_slot_gc_fires_at_exactly_wal_receiver_timeout() {
    assert!(worker_attach_timed_out(1_000_000, 1_000_000 + 60_000_000, 60_000));
    assert!(!worker_attach_timed_out(1_000_000, 1_000_000 + 59_999_999, 60_000));
}

// launcher.c:506: WORKERTYPE_UNKNOWN is elog(ERROR, "unknown worker type"),
// a catchable error, not a process abort.
#[test]
fn unknown_worker_type_is_an_error_not_a_panic() {
    let r = std::panic::catch_unwind(|| worker_bgw_identity(LogicalRepWorkerType::Unknown, 1, 0));
    let Ok(Err(e)) = r else {
        panic!("WORKERTYPE_UNKNOWN must raise elog(ERROR, \"unknown worker type\") (launcher.c:506)");
    };
    assert_eq!(e.message(), "unknown worker type");
}

// logicalrep_worker_wakeup (launcher.c:686) over C's zeroed shmem struct
// finds no worker and returns; a context without ApplyLauncherShmemInit
// (single-user mode) must not panic.
#[test]
fn worker_wakeup_without_launcher_shmem_is_a_noop() {
    let _g = ctx_test_guard();
    *CTX.lock().unwrap_or_else(|e| e.into_inner()) = None;
    let r = std::panic::catch_unwind(|| logicalrep_worker_wakeup(100, InvalidOid));
    ApplyLauncherShmemInit();
    assert!(r.is_ok(), "logicalrep_worker_wakeup panicked without launcher shmem");
}

// ApplyLauncherShmemInit registers "Logical Replication Launcher Data" in the
// shmem index (launcher.c:969), sized ApplyLauncherShmemSize(): 528 bytes at
// max_logical_replication_workers = 4 (pg_shmem_allocations on C 18.6).
#[test]
fn launcher_shmem_is_registered_in_the_shmem_index() {
    let _g = ctx_test_guard();
    assert_eq!(max_logical_replication_workers(), 4);
    ApplyLauncherShmemInit();
    let (_, found) = shmem::ShmemInitStruct("Logical Replication Launcher Data", 528).unwrap();
    assert!(found, "\"Logical Replication Launcher Data\" (528 bytes) missing from the shmem index");
}

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
    let _g = ctx_test_guard();
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

// logicalrep_worker_stop_internal (launcher.c:544) stops the worker
// generation captured together with the lookup: when the slot has since been
// handed to another worker, it returns without signalling or waiting.
#[test]
fn worker_stop_leaves_a_reused_slot_alone() {
    let _g = ctx_test_guard();
    ApplyLauncherShmemInit();
    with_ctx(|ctx| {
        ctx.workers[0].in_use = true;
        ctx.workers[0].wtype = LogicalRepWorkerType::Apply;
        ctx.workers[0].subid = 200;
        ctx.workers[0].generation = 8;
        ctx.workers[0].proc_pid = 4711;
    });
    let stopper = std::thread::spawn(|| {
        logicalrep_worker_stop_internal(0, 7, procsignal::signums::SIGTERM)
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !stopper.is_finished() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(stopper.is_finished(), "stop of a stale generation must return at once");
    stopper.join().unwrap().unwrap();
    let w = worker_snapshot(0).unwrap();
    assert!(
        w.in_use && w.generation == 8 && w.proc_pid == 4711,
        "the replacement worker was disturbed"
    );
    with_ctx(|ctx| logicalrep_worker_cleanup_locked(&mut ctx.workers[0]));
}

// SetupApplyOrSyncWorker (worker.c:4809): last_send_time, last_recv_time and
// reply_time start at the current timestamp, so pg_stat_subscription shows
// them (not NULL) while the worker is still connecting.
#[test]
fn worker_setup_initialises_stats_times_to_now() {
    let _g = ctx_test_guard();
    attach_apply_worker_in_slot0();
    let before = worker_snapshot(0).unwrap();
    assert_eq!((before.last_send_time, before.last_recv_time, before.reply_time), (0, 0, 0));
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| timestamp_seams::get_current_timestamp::set(|| 777_000_000));
    my_worker_init_stats_times();
    let w = worker_snapshot(0).unwrap();
    assert_eq!((w.last_send_time, w.last_recv_time, w.reply_time), (777_000_000, 777_000_000, 777_000_000));
    ipc::shmem_exit(0).unwrap();
}
