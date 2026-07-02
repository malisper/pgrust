use std::sync::{Mutex, MutexGuard, Once};

use super::*;

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn bringup() -> MutexGuard<'static, ()> {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        init_seams();
        ipc_seams::on_shmem_exit::set(|_cb, _arg| {});
        guc_seams::set_config_option_internal_dynamic_default::set(|_, _| Ok(()));
        superuser_seams::superuser::set(|| Ok(true));
        g::SetMaxBackends(8);
        guc_tables::vars::pgstat_track_activities.write(true);
        backend_status_seams::backend_status_shmem_init::call().unwrap();
    });
    g::SetMaxBackends(8);
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
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
fn cross_thread_entry_is_readable() {
    let _g = bringup();
    let handle = std::thread::spawn(|| {
        g::SetMaxBackends(8);
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
