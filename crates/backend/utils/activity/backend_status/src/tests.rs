use std::sync::{Mutex, MutexGuard, Once};

use super::*;

static TEST_LOCK: Mutex<()> = Mutex::new(());

const MAX_CONNECTIONS: i32 = 16;
const MAX_WORKER_PROCESSES: i32 = 2;
const NUM_SPECIAL: i32 = types_storage::storage::NUM_SPECIAL_WORKER_PROCS;
const MAX_BACKENDS: i32 = MAX_CONNECTIONS + 3 + MAX_WORKER_PROCESSES + 2 + NUM_SPECIAL;

fn set_globals() {
    g::SetMaxConnections(MAX_CONNECTIONS);
    g::set_max_worker_processes(MAX_WORKER_PROCESSES);
    g::SetMaxBackends(MAX_BACKENDS);
    // Per-session backing: every thread that reports activity must enable
    // tracking for itself (a real backend inherits it via GUC bring-up).
    guc_tables::vars::pgstat_track_activities.write(true);
}

fn bringup() -> MutexGuard<'static, ()> {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        init_seams();
        shmem::init_seams();
        ipc_seams::on_shmem_exit::set(|_cb, _arg| {});
        guc_seams::set_config_option_internal_dynamic_default::set(|_, _| Ok(()));
        superuser_seams::superuser::set(|| Ok(true));
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
        miscinit_seams::switch_to_shared_latch::set(|| {});
        miscinit_seams::switch_back_to_local_latch::set(|| {});
        waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});
        deadlock_seams::init_dead_lock_checking::set(|| Ok(()));
        pmsignal_seams::register_postmaster_child_active::set(|| {});
        syncrep_seams::sync_rep_cleanup_at_proc_exit::set(|| {});
        condition_variable_seams::condition_variable_cancel_sleep::set(|| false);
        autovacuum_seams::wake_autovacuum_launcher::set(|| {});
        lock_seams::abort_strong_lock_acquire::set(|| {});
        lock_seams::get_awaited_lock_hashcode::set(|| None);
        lock_seams::lock_release_all::set(|_, _| Ok(()));
        timeout_seams::disable_timeouts::set(|_| {});
        set_globals();
        g::SetMyProcPid(4242);
        lwlock::CreateLWLocks(false).unwrap();
        lmgr_proc::init_seams();
        lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
            autovacuum_worker_slots: 3,
            max_wal_senders: 2,
            max_prepared_xacts: 2,
            fastpath_lock_groups_per_backend: 1,
        });
        procarray::ProcArrayShmemInit();
        backend_status_seams::backend_status_shmem_init::call().unwrap();
    });
    set_globals();
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn my_pgproc(pid: i32) -> ProcNumber {
    g::SetMyProcPid(pid);
    if lmgr_proc::MyProc().is_none() {
        lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    }
    lmgr_proc::MyProc().unwrap()
}

fn start_backend(procno: ProcNumber, pid: i32) {
    g::SetMyProcNumber(procno);
    g::SetMyProcPid(pid);
    g::SetMyStartTimestamp(777);
    miscinit::SetMyBackendType(BackendType::Backend);
    miscinit::InitializeSessionUserIdStandalone().unwrap();
    backend_status_seams::pgstat_beinit::call().unwrap();
    backend_status_seams::pgstat_bestart_initial::call().unwrap();
    backend_status_seams::pgstat_bestart_final::call().unwrap();
}

#[test]
fn shmem_size_matches_c_formula() {
    let _g = bringup();
    let slots = (g::MaxBackends() + NUM_AUXILIARY_PROCS) as usize;
    let expected = slots * std::mem::size_of::<PgBackendStatus>()
        + 2 * slots * NAMELEN
        + slots * 1024;
    assert_eq!(BackendStatusShmemSize().unwrap(), expected);
}

#[test]
fn bestart_lifecycle_reaches_undefined_state() {
    let _g = bringup();
    start_backend(0, 900001);

    let e = MyBEEntry().expect("beinit ran");
    assert_eq!(e.st_procpid.get(), 900001);
    assert_eq!(e.st_backendType.get(), BackendType::Backend);
    assert_eq!(e.st_proc_start_timestamp.get(), 777);
    assert_eq!(e.st_state.get(), BackendState::STATE_UNDEFINED);
    assert_ne!(e.st_userid.get(), InvalidOid);
    assert_eq!(pgstat_get_backend_type_by_proc_number(0), BackendType::Backend);

    pgstat_beshutdown_hook(0, 0);
    assert!(MyBEEntry().is_none());
    let arr = backend_status_array();
    assert_eq!(arr[0].st_procpid.get(), 0);
}

#[test]
fn report_activity_stores_and_resets_ids() {
    let _g = bringup();
    start_backend(1, 900002);

    backend_status_seams::pgstat_report_query_id::call(42, false);
    backend_status_seams::pgstat_report_plan_id::call(43, false);
    assert_eq!(pgstat_get_my_query_id(), 42);
    backend_status_seams::pgstat_report_query_id::call(77, false);
    assert_eq!(pgstat_get_my_query_id(), 42);

    backend_status_seams::pgstat_report_activity::call(
        BackendState::STATE_RUNNING,
        Some("SELECT 1"),
    );
    let e = MyBEEntry().unwrap();
    assert_eq!(e.st_state.get(), BackendState::STATE_RUNNING);
    assert_eq!(pgstat_get_my_query_id(), 0);
    assert_eq!(pgstat_get_my_plan_id(), 0);
    assert_eq!(read_activity(e.slot), b"SELECT 1");

    let activity = pgstat_get_backend_current_activity(900002, false).unwrap();
    assert_eq!(activity, "SELECT 1");
    assert_eq!(pgstat_get_crashed_backend_activity(900002).unwrap(), "SELECT 1");

    let long = "x".repeat(5000);
    backend_status_seams::pgstat_report_activity::call(
        BackendState::STATE_RUNNING,
        Some(&long),
    );
    assert_eq!(read_activity(e.slot).len(), 1023);

    backend_status_seams::pgstat_report_activity::call(BackendState::STATE_IDLE, None);
    assert_eq!(e.st_state.get(), BackendState::STATE_IDLE);
    pgstat_beshutdown_hook(0, 0);
}

#[test]
fn track_activities_off_reports_disabled_once() {
    let _g = bringup();
    start_backend(2, 900003);

    guc_tables::vars::pgstat_track_activities.write(false);
    backend_status_seams::pgstat_report_activity::call(
        BackendState::STATE_RUNNING,
        Some("SELECT 2"),
    );
    let e = MyBEEntry().unwrap();
    assert_eq!(e.st_state.get(), BackendState::STATE_DISABLED);
    assert_eq!(read_activity(e.slot), b"");
    assert_eq!(e.st_query_id.get(), 0);

    backend_status_seams::pgstat_report_query_id::call(5, true);
    assert_eq!(pgstat_get_my_query_id(), 0);
    guc_tables::vars::pgstat_track_activities.write(true);
    pgstat_beshutdown_hook(0, 0);
}

#[test]
fn appname_and_xact_timestamp_roundtrip() {
    let _g = bringup();
    start_backend(3, 900004);

    pgstat_report_appname("psql");
    let e = MyBEEntry().unwrap();
    assert_eq!(appname_of(e), "psql");

    backend_status_seams::pgstat_report_xact_timestamp::call(123456);
    assert_eq!(e.st_xact_start_timestamp.get(), 123456);
    pgstat_beshutdown_hook(0, 0);
}

#[test]
fn reset_after_crash_restores_boot_image() {
    let _g = bringup();
    start_backend(5, 900006);
    backend_status_seams::pgstat_report_activity::call(
        BackendState::STATE_RUNNING,
        Some("CRASHING QUERY"),
    );
    assert_eq!(MyBEEntry().unwrap().st_procpid.get(), 900006);

    BackendStatusShmemResetAfterCrash();

    for e in backend_status_array() {
        assert_eq!(e.st_changecount.load(Relaxed), 0);
        assert_eq!(e.st_procpid.get(), 0);
        assert_eq!(e.st_backendType.get(), BackendType::Invalid);
        assert_eq!(e.st_state.get(), BackendState::STATE_UNDEFINED);
        assert_eq!(e.st_databaseid.get(), InvalidOid);
        assert_eq!(e.st_userid.get(), InvalidOid);
        assert_eq!(e.st_query_id.get(), 0);
        assert_eq!(read_activity(e.slot), b"");
    }
    MY_BE_ENTRY.set(None);
}

#[test]
fn cross_thread_entry_is_readable() {
    let _g = bringup();
    let handle = std::thread::spawn(|| {
        set_globals();
        start_backend(4, 900005);
        backend_status_seams::pgstat_report_activity::call(
            BackendState::STATE_RUNNING,
            Some("CROSS THREAD QUERY"),
        );
    });
    handle.join().unwrap();

    assert_eq!(pgstat_get_backend_type_by_proc_number(4), BackendType::Backend);
    assert_eq!(
        pgstat_get_backend_current_activity(900005, false).unwrap(),
        "CROSS THREAD QUERY"
    );
}

#[test]
fn local_snapshot_reads_entries_with_transaction_ids() {
    let _g = bringup();
    let me = my_pgproc(910001);
    start_backend(me, 910001);
    backend_status_seams::pgstat_report_activity::call(
        BackendState::STATE_RUNNING,
        Some("SELECT 42"),
    );
    pgstat_report_appname("snaptest");
    let proc = lmgr_proc::GetPGProcByNumber(me);
    proc.xid.value.store(555, Relaxed);
    proc.xmin.value.store(550, Relaxed);

    pgstat_clear_backend_status_snapshot();
    let n = pgstat_fetch_stat_numbackends();
    assert!(n >= 1);

    let mut mine = None;
    let mut prev = INVALID_PROC_NUMBER;
    for i in 1..=n {
        let e = pgstat_get_local_beentry_by_index(i).unwrap();
        assert!(e.proc_number > prev);
        prev = e.proc_number;
        assert!(e.st_procpid > 0);
        if e.st_procpid == 910001 {
            mine = Some(e);
        }
    }
    let e = mine.expect("own entry in snapshot");
    assert_eq!(e.proc_number, me);
    assert_eq!(e.st_backendType, BackendType::Backend);
    assert_eq!(e.st_state, BackendState::STATE_RUNNING);
    assert_eq!(e.st_activity_raw, "SELECT 42");
    assert_eq!(e.st_appname, "snaptest");
    assert_eq!(e.st_proc_start_timestamp, 777);
    assert_eq!(e.backend_xid, 555);
    assert_eq!(e.backend_xmin, 550);
    assert_eq!(e.backend_subxact_count, 0);
    assert!(!e.backend_subxact_overflowed);

    assert!(pgstat_get_local_beentry_by_index(0).is_none());
    assert!(pgstat_get_local_beentry_by_index(n + 1).is_none());
    let by_procno = pgstat_get_beentry_by_proc_number(me).unwrap();
    assert_eq!(by_procno.st_procpid, 910001);
    assert!(pgstat_get_beentry_by_proc_number(INVALID_PROC_NUMBER).is_none());

    proc.xid.value.store(0, Relaxed);
    proc.xmin.value.store(0, Relaxed);
    pgstat_beshutdown_hook(0, 0);
    // The snapshot is stable until explicitly cleared.
    assert!(pgstat_get_beentry_by_proc_number(me).is_some());
    pgstat_clear_backend_status_snapshot();
    assert!(pgstat_get_beentry_by_proc_number(me).is_none());
    pgstat_clear_backend_status_snapshot();
}
