// SlotSyncCtx is process-global: the tests below serialize on TEST_LOCK and
// reset it at entry.
use super::*;

static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn reset_ctx() {
    with_ctx(|ctx| {
        ctx.pid = InvalidPid;
        ctx.stop_signaled = false;
        ctx.syncing = false;
        ctx.last_start_time = 0;
    });
    slot::set_syncing_replication_slots(false);
}

fn install_seams() {
    static SEAMS: std::sync::Once = std::sync::Once::new();
    SEAMS.call_once(|| {
        if !postgres_seams::check_for_interrupts::is_installed() {
            postgres_seams::check_for_interrupts::set(|| Ok(()));
        }
    });
}

// upstream 94efd308bcec (18.4): the pg_sync_replication_slots() backend
// advertises its own pid (so the startup process can wake it on promotion),
// and reset_syncing_flag clears the pid with the syncing flag.
#[test]
fn sql_function_advertises_its_pid_and_reset_clears_it() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_ctx();

    check_and_set_sync_info(777).unwrap();
    assert!(with_ctx(|ctx| ctx.syncing && ctx.pid == 777));
    assert!(slot::syncing_replication_slots());

    reset_syncing_flag();
    assert!(with_ctx(|ctx| !ctx.syncing));
    assert_eq!(with_ctx(|ctx| ctx.pid), InvalidPid);
    assert!(!slot::syncing_replication_slots());

    // A second sync while one is advertised is the concurrent-sync error.
    check_and_set_sync_info(778).unwrap();
    let err = check_and_set_sync_info(779).unwrap_err();
    assert_eq!(err.message(), "cannot synchronize replication slots concurrently");
    reset_syncing_flag();
}

// upstream 58c1188a3eaa (18.4): the PROCSIG_SLOTSYNC_MESSAGE handler arms
// CHECK_FOR_INTERRUPTS, and ProcessSlotSyncMessage errors the SQL-function
// backend out (55000) only while its sync is still running.
#[test]
fn slotsync_message_interrupts_a_running_sql_sync() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    install_seams();
    reset_ctx();

    HandleSlotSyncMessageInterrupt();
    assert!(init_small::globals::InterruptPending());
    assert!(init_small::globals::SlotSyncShutdownPending());

    // Sync already finished: the flag clears and the caller is left alone.
    ProcessSlotSyncMessage().unwrap();
    assert!(!init_small::globals::SlotSyncShutdownPending());

    // Mid-sync: the interrupt is the promotion error.
    check_and_set_sync_info(777).unwrap();
    HandleSlotSyncMessageInterrupt();
    let err = ProcessSlotSyncMessage().unwrap_err();
    assert!(!init_small::globals::SlotSyncShutdownPending());
    assert_eq!(err.sqlstate(), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
    assert_eq!(
        err.message(),
        "replication slot synchronization will stop because promotion is triggered"
    );
    init_small::globals::SetInterruptPending(false);
    reset_ctx();
}

// upstream 58c1188a3eaa (18.4): a sync starting after ShutDownSlotSync set
// stopSignaled must not begin (the SQL-function arm: 55000).
#[test]
fn sql_function_does_not_start_after_promotion_was_triggered() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_ctx();
    with_ctx(|ctx| ctx.stop_signaled = true);

    let err = check_and_set_sync_info(777).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
    assert_eq!(
        err.message(),
        "replication slot synchronization will not start because promotion was triggered"
    );
    assert!(with_ctx(|ctx| !ctx.syncing && ctx.pid == InvalidPid));
    assert!(!slot::syncing_replication_slots());
    reset_ctx();
}

// ---------------------------------------------------------------------------
// Slot-array harness (the slot crate's shmem_setup, minus its own state) for
// the tests that drive update_local_synced_slot / drop_local_obsolete_slots.
// Slot state files live under a private cwd (pg_replslot, pg_logical).
// ---------------------------------------------------------------------------
static INJECT_CONCURRENT_DROP: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

fn slot_harness() {
    use std::sync::atomic::Ordering::{Relaxed, SeqCst};
    static SETUP: std::sync::Once = std::sync::Once::new();
    SETUP.call_once(|| {
        install_seams();
        init_small::globals::SetMaxConnections(8);
        init_small::globals::set_max_worker_processes(2);
        init_small::globals::SetMaxBackends(17);
        init_small::globals::SetMyProcPid(4242);
        init_small::globals::SetMyDatabaseId(5);

        pg_sema_seams::pg_semaphore_create::set(|_| {});
        pg_sema_seams::pg_semaphore_reset::set(|_| {});
        pg_sema_seams::pg_semaphore_lock::set(|_| {});
        pg_sema_seams::pg_semaphore_unlock::set(|_| {});
        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        s_lock_seams::set_spins_per_delay::set(|_| {});
        s_lock_seams::update_spins_per_delay::set(|v| v);
        latch_seams::own_latch::set(|_| {});
        latch_seams::disown_latch::set(|_| {});
        latch_seams::set_latch::set(|_| {});
        latch_seams::set_latch_my_latch::set(|| {});
        latch_seams::wait_latch_my_latch::set(|_, _, _| 0);
        latch_seams::reset_latch_my_latch::set(|| {});
        miscinit_seams::switch_to_shared_latch::set(|| {});
        miscinit_seams::switch_back_to_local_latch::set(|| {});
        waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});
        ipc_seams::on_shmem_exit::set(|_, _| {});
        deadlock_seams::init_dead_lock_checking::set(|| Ok(()));
        pmsignal_seams::register_postmaster_child_active::set(|| {});
        syncrep_seams::sync_rep_cleanup_at_proc_exit::set(|| {});
        condition_variable_seams::condition_variable_cancel_sleep::set(|| false);
        autovacuum_seams::wake_autovacuum_launcher::set(|| {});
        lock_seams::abort_strong_lock_acquire::set(|| {});
        lock_seams::get_awaited_lock_hashcode::set(|| None);
        lock_seams::lock_release_all::set(|_, _| Ok(()));
        timeout_seams::disable_timeouts::set(|_| {});
        shmem_seams::add_size::set(|a, b| Ok(a.checked_add(b).expect("size overflow")));
        shmem_seams::mul_size::set(|a, b| Ok(a.checked_mul(b).expect("size overflow")));
        shmem_seams::shmem_alloc::set(|size| {
            Ok(Box::leak(vec![0u8; size].into_boxed_slice()).as_mut_ptr())
        });
        xact_seams::transaction_id_is_current_transaction_id::set(|_| false);
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        xlog_seams::recovery_in_progress::set(|| false);
        transam_seams::transaction_id_did_abort::set(|_| Ok(false));
        subtrans_seams::sub_trans_get_topmost_transaction::set(Ok);
        superuser_seams::superuser_arg::set(|_| Ok(false));

        // drop_local_obsolete_slots' database lock: granted outright; the
        // post-acquire AcceptInvalidationMessages is where a concurrent
        // drop (and reuse of the freed entry) lands when a test asks for it.
        lock_seams::lock_acquire_extended::set(|_, _, _, _, _, _| {
            Ok(types_storage::lock::LOCKACQUIRE_OK)
        });
        lock_seams::lock_release::set(|_, _, _| Ok(true));
        lock_seams::mark_lock_clear::set(|_, _| {});
        inval_seams::accept_invalidation_messages::set(|| {
            if INJECT_CONCURRENT_DROP.swap(false, SeqCst) {
                let s = &ReplicationSlotCtl()[0];
                let mut d = unsafe { s.data.get() };
                d.name.namestrcpy("reused_by_another_backend");
                unsafe {
                    s.data.set(d);
                    s.in_use.set(false);
                }
            }
            Ok(())
        });
        // A no-op advance: the slot already sits at (or cannot reach) the
        // requested point.
        logical_slot_advance_and_check_snap_state::set(|_| Ok((0, false)));

        // Promotion / clock / conninfo inputs the tests below steer through
        // statics (a seam installs exactly once per process).
        if !xlogrecovery_seams::standby_mode::is_installed() {
            xlogrecovery_seams::standby_mode::set(|| STANDBY_MODE.load(SeqCst));
        }
        if !timestamp_seams::get_current_timestamp::is_installed() {
            timestamp_seams::get_current_timestamp::set(|| {
                TIMESTAMP_SEEN_CONTROL_LOCK_SHARED.store(
                    lwlock::LWLockHeldByMeInMode(
                        lwlock::main_lock(types_storage::storage::REPLICATION_SLOT_CONTROL_LOCK),
                        lwlock::LW_SHARED,
                    ),
                    SeqCst,
                );
                TEST_TIMESTAMP
            });
        }
        guc_tables::vars::PrimaryConnInfo.install_if_absent(guc_tables::GucVarAccessors {
            get: || PRIMARY_CONNINFO.lock().unwrap_or_else(|e| e.into_inner()).clone(),
            set: |v| *PRIMARY_CONNINFO.lock().unwrap_or_else(|e| e.into_inner()) = v,
        });

        walsender_config::init_seams();
        guc_tables::vars::max_replication_slots.write(2);

        lwlock::CreateLWLocks(false).unwrap();
        lmgr_proc::init_seams();
        lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
            autovacuum_worker_slots: 3,
            max_wal_senders: 2,
            max_prepared_xacts: 2,
            fastpath_lock_groups_per_backend: 1,
        });
        procarray::init_seams();
        procarray::ProcArrayShmemInit();
        static XLOG_BUFFERS: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(64);
        guc_tables::vars::XLOGbuffers.install_if_absent(guc_tables::GucVarAccessors {
            get: || XLOG_BUFFERS.load(Relaxed),
            set: |v| XLOG_BUFFERS.store(v, Relaxed),
        });
        transam_xlog::XLOGShmemInit();
        slot::ReplicationSlotsShmemInit();

        lmgr_proc::InitProcess(types_core::BackendType::Backend).expect("InitProcess");
        procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).expect("ProcArrayAdd self");

        let dir = std::env::temp_dir().join(format!("pgrust-slotsync-tests-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("pg_replslot")).unwrap();
        std::fs::create_dir_all(dir.join("pg_logical/snapshots")).unwrap();
        std::env::set_current_dir(&dir).unwrap();
    });
    become_backend();
}

static STANDBY_MODE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static TIMESTAMP_SEEN_CONTROL_LOCK_SHARED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
const TEST_TIMESTAMP: i64 = 123_456_789;
static PRIMARY_CONNINFO: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

// MyProc is per thread (as C's MyProc is per process) and every #[test] runs
// on its own thread, so the PGPROC bound inside the Once above belongs to
// whichever test won it; bind one to the calling thread instead, once per
// thread (the slot crate's become_backend idiom). ReplicationSlotControlLock
// waits queue MyProc.
fn become_backend() {
    if lmgr_proc::MyProc().is_none() {
        init_small::globals::SetMyProcPid(4242);
        lmgr_proc::InitProcess(types_core::BackendType::Backend).expect("InitProcess");
        procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).expect("ProcArrayAdd self");
    }
}

fn synced_slot_entry(name: &str, persistency: slot::ReplicationSlotPersistency) -> &'static ReplicationSlot {
    let s = &ReplicationSlotCtl()[0];
    let mut d = slot::ReplicationSlotPersistentData::default();
    d.name.namestrcpy(name);
    d.database = 5;
    d.persistency = persistency;
    d.synced = 1;
    d.failover = true;
    d.plugin.namestrcpy("test_decoding");
    d.restart_lsn = 0x1000;
    d.confirmed_flush = 0x2000;
    d.catalog_xmin = 700;
    // SAFETY: serialized tests; nobody else references the entry.
    unsafe {
        s.data.set(d);
        s.in_use.set(true);
        s.active_pid.set(0);
        s.dirty.set(false);
        s.just_dirtied.set(false);
        s.effective_catalog_xmin.set(700);
    }
    s
}

// upstream 540fe8fb5c22 (18.4): Fix excessive logging in idle slotsync worker.
// A remote-ahead slot whose advance moves nothing (the synced slot already
// reached, or cannot reach, a consistent point) is not an update: nothing is
// saved and the caller keeps backing off its nap instead of re-syncing (and
// re-logging) every 200 ms.
#[test]
fn noop_advance_is_not_an_update() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    slot_harness();
    let s = synced_slot_entry("noop_sync", slot::RS_TEMPORARY);
    // SAFETY: as in synced_slot_entry.
    unsafe { s.active_pid.set(init_small::globals::MyProcPid()) };
    slot::SetMyReplicationSlot(Some(s));
    std::fs::create_dir_all("pg_replslot/noop_sync").unwrap();

    let remote = RemoteSlot {
        name: "noop_sync".to_string(),
        plugin: "test_decoding".to_string(),
        database: "postgres".to_string(),
        two_phase: false,
        failover: true,
        restart_lsn: 0x1800, // ahead of the local 0x1000
        confirmed_lsn: 0x2000,
        two_phase_at: 0,
        catalog_xmin: 700,
        invalidated: RS_INVAL_NONE,
    };
    let mut found_consistent_snapshot = true;
    let updated =
        update_local_synced_slot(&remote, 5, Some(&mut found_consistent_snapshot), None).unwrap();

    assert!(!updated, "a no-op advance was reported as an update");
    assert!(!found_consistent_snapshot);
    // SAFETY: as above.
    assert!(!unsafe { s.dirty.get() }, "nothing changed, nothing to save");
    slot::SetMyReplicationSlot(None);
    // SAFETY: as above.
    unsafe { s.in_use.set(false) };
}

// upstream 08458bcaea5b (18.6): Avoid stale slot access after dropping obsolete synced slots.
// A slot that another backend dropped while we waited for the database lock
// (and whose freed entry was reused under a new name) is not ours to drop:
// no "dropped replication slot" line at all, let alone one naming the reuse.
#[test]
fn obsolete_slot_dropped_concurrently_is_not_logged_as_dropped() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    slot_harness();
    synced_slot_entry("obsolete_sync", slot::RS_PERSISTENT);

    LOGS.lock().unwrap_or_else(|e| e.into_inner()).clear();
    let prior = elog::sink::set_emit_log_hook(Some(capture_log));
    let _ = elog(LOG, "hook probe".to_string());
    INJECT_CONCURRENT_DROP.store(true, std::sync::atomic::Ordering::SeqCst);
    let r = drop_local_obsolete_slots(&[]);
    elog::sink::set_emit_log_hook(prior);
    r.unwrap();

    let logs = LOGS.lock().unwrap_or_else(|e| e.into_inner()).clone();
    assert!(logs.iter().any(|m| m == "hook probe"), "log capture is not wired: {logs:?}");
    assert!(
        !INJECT_CONCURRENT_DROP.load(std::sync::atomic::Ordering::SeqCst),
        "the concurrent drop was not injected"
    );
    assert!(
        !logs.iter().any(|m| m.starts_with("dropped replication slot")),
        "logged a drop that did not happen: {logs:?}"
    );
    // SAFETY: as in synced_slot_entry.
    unsafe { ReplicationSlotCtl()[0].in_use.set(false) };
}

static LOGS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

fn capture_log(error: &types_error::PgError, output_to_server: &mut bool) {
    LOGS.lock().unwrap_or_else(|e| e.into_inner()).push(error.message().to_string());
    *output_to_server = false;
}

// ---------------------------------------------------------------------------
// audit-18.6 b174 witnesses.
// ---------------------------------------------------------------------------

// slotsync.c:1771 SlotSyncWorkerCanRestart: the elapsed time is compared as
// (unsigned int)(curtime - last_start_time). A clock stepped backwards (the
// last start stamp lies in the future) wraps to a huge value and the
// postmaster may restart the worker at once; it never waits for the wall
// clock to catch up with the stale stamp.
#[test]
fn worker_can_restart_after_clock_step_backwards() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_ctx();

    // SAFETY: time(2) with a NULL argument.
    let now = unsafe { libc::time(std::ptr::null_mut()) } as i64;
    with_ctx(|ctx| ctx.last_start_time = now + 60);
    assert!(
        SlotSyncWorkerCanRestart(),
        "a last_start_time in the future (clock stepped back) must not block the restart"
    );
    // The restart stamped now: a second attempt inside the interval is refused.
    assert!(!SlotSyncWorkerCanRestart());
    // A stale stamp beyond the interval allows it again.
    with_ctx(|ctx| ctx.last_start_time = now - SLOTSYNC_RESTART_INTERVAL_SEC);
    assert!(SlotSyncWorkerCanRestart());
    reset_ctx();
}

// slotsync.c:377 get_local_synced_slots walks ReplicationSlotCtl under
// ReplicationSlotControlLock (LW_SHARED): with another backend holding the
// lock exclusively (slot creation / drop), the walk waits for the release
// instead of reading in_use / synced mid-update.
#[test]
fn get_local_synced_slots_waits_for_control_lock() {
    use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
    use std::sync::Arc;

    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    slot_harness();
    synced_slot_entry("locked_sync", slot::RS_PERSISTENT);

    let held = Arc::new(AtomicBool::new(false));
    let walk_started = Arc::new(AtomicBool::new(false));
    let releasing = Arc::new(AtomicBool::new(false));
    let holder = {
        let (held, walk_started, releasing) =
            (held.clone(), walk_started.clone(), releasing.clone());
        std::thread::spawn(move || {
            become_backend();
            slot::with_control_lock_exclusive(|| {
                held.store(true, SeqCst);
                while !walk_started.load(SeqCst) {
                    std::thread::yield_now();
                }
                // Give a lockless walk every chance to finish first.
                std::thread::sleep(std::time::Duration::from_millis(200));
                releasing.store(true, SeqCst);
                Ok(())
            })
            .unwrap();
        })
    };
    while !held.load(SeqCst) {
        std::thread::yield_now();
    }
    walk_started.store(true, SeqCst);

    let slots = get_local_synced_slots().unwrap();

    assert!(
        releasing.load(SeqCst),
        "get_local_synced_slots walked the slot array while another backend held \
         ReplicationSlotControlLock exclusively"
    );
    holder.join().unwrap();
    assert_eq!(slots.len(), 1);
    assert!(!lwlock::LWLockHeldByMe(lwlock::main_lock(
        types_storage::storage::REPLICATION_SLOT_CONTROL_LOCK
    )));
    // SAFETY: as in synced_slot_entry.
    unsafe { ReplicationSlotCtl()[0].in_use.set(false) };
}

// slotsync.c:1659 update_synced_slots_inactive_since stamps the synced slots
// under ReplicationSlotControlLock (LW_SHARED); GetCurrentTimestamp is called
// inside the walk, so it observes the lock.
#[test]
fn update_synced_slots_inactive_since_holds_control_lock_shared() {
    use std::sync::atomic::Ordering::SeqCst;

    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    slot_harness();
    reset_ctx();
    let s = synced_slot_entry("promoted_sync", slot::RS_PERSISTENT);
    // SAFETY: as in synced_slot_entry.
    unsafe { s.inactive_since.set(0) };
    TIMESTAMP_SEEN_CONTROL_LOCK_SHARED.store(false, SeqCst);

    STANDBY_MODE.store(true, SeqCst);
    let r = update_synced_slots_inactive_since();
    STANDBY_MODE.store(false, SeqCst);
    r.unwrap();

    // SAFETY: as above.
    assert_eq!(unsafe { s.inactive_since.get() }, TEST_TIMESTAMP, "the synced slot was not stamped");
    assert!(
        TIMESTAMP_SEEN_CONTROL_LOCK_SHARED.load(SeqCst),
        "update_synced_slots_inactive_since stamped the slots without ReplicationSlotControlLock"
    );
    assert!(!lwlock::LWLockHeldByMe(lwlock::main_lock(
        types_storage::storage::REPLICATION_SLOT_CONTROL_LOCK
    )));
    // SAFETY: as above.
    unsafe { s.in_use.set(false) };
}

// libpqwalreceiver.c:525 libpqrcv_get_option_from_conninfo: PQconninfoParse
// keeps the LAST value of a repeated keyword, and an empty value counts as
// absent. The port's parse_conninfo carries the same last-wins rule, and the
// same value scan (fe-connect.c:6355: blanks after '=' are skipped, the
// value runs to the next blank, so `dbname= dbname=postgres` is ONE option
// whose value is `dbname=postgres` for libpq and for the port alike).
#[test]
fn dbname_from_conninfo_takes_the_last_occurrence() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    slot_harness();

    guc_tables::vars::PrimaryConnInfo.write(Some("dbname= dbname=postgres".to_string()));
    assert_eq!(CheckAndGetDbnameFromConninfo().unwrap(), "dbname=postgres");

    guc_tables::vars::PrimaryConnInfo.write(Some("dbname='' dbname=postgres".to_string()));
    assert_eq!(CheckAndGetDbnameFromConninfo().unwrap(), "postgres");

    guc_tables::vars::PrimaryConnInfo.write(Some("dbname=first host=x dbname=second".to_string()));
    assert_eq!(CheckAndGetDbnameFromConninfo().unwrap(), "second");

    guc_tables::vars::PrimaryConnInfo.write(Some("dbname=postgres dbname=".to_string()));
    let err = CheckAndGetDbnameFromConninfo().unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    assert_eq!(
        err.message(),
        "replication slot synchronization requires \"dbname\" to be specified in \"primary_conninfo\""
    );
    guc_tables::vars::PrimaryConnInfo.write(Some(String::new()));
}
