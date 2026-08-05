use std::sync::{Mutex, MutexGuard, Once};

use super::*;

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn bringup() -> MutexGuard<'static, ()> {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        postmaster_seams::signal_postmaster_sigusr1::set(|| {});
    });
    let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    g::set_max_worker_processes(4);
    g::set_max_parallel_workers(2);
    g::SetIsUnderPostmaster(true);
    g::SetMaxBackends(16);
    pmsignal::PMSignalShmemInit(8);
    procsignal::ProcSignalShmemInit();
    *REGISTRY.lock().unwrap_or_else(|e| e.into_inner()) = None;
    BackgroundWorkerShmemInit();
    guard
}

fn wmain(_arg: u64) -> PgResult<()> {
    Ok(())
}

fn mk_worker(name: &str, flags: i32) -> BackgroundWorker {
    BackgroundWorker {
        bgw_name: name.to_string(),
        bgw_type: String::new(),
        bgw_flags: flags,
        bgw_start_time: BgWorkerStartTime::ConsistentState,
        bgw_restart_time: BGW_NEVER_RESTART,
        bgw_main: wmain,
        bgw_main_arg: 0,
        bgw_extra: [0; BGW_EXTRALEN],
        bgw_notify_pid: 0,
    }
}

fn register(name: &str, flags: i32) -> Option<BackgroundWorkerHandle> {
    RegisterDynamicBackgroundWorker(mk_worker(name, flags)).expect("register")
}

fn registered_idx_for(handle: &BackgroundWorkerHandle) -> usize {
    with_registry(|reg| find_rw_by_slot(reg, handle.slot)).expect("registered")
}

#[test]
fn register_to_exhaustion_of_max_worker_processes() {
    let _g = bringup();
    let mut handles = Vec::new();
    for i in 0..4 {
        let h = register(&format!("w{i}"), BGWORKER_SHMEM_ACCESS).expect("free slot");
        handles.push(h);
    }
    assert!(register("w4", BGWORKER_SHMEM_ACCESS).is_none());

    BackgroundWorkerStateChange(true);
    let idx = registered_idx_for(&handles[0]);
    ForgetBackgroundWorker(idx);
    let h = register("w5", BGWORKER_SHMEM_ACCESS).expect("freed slot reusable");
    assert_eq!(h.slot, handles[0].slot);
    assert_eq!(h.generation, handles[0].generation + 1);
}

#[test]
fn parallel_class_capped_by_max_parallel_workers() {
    let _g = bringup();
    let flags = BGWORKER_SHMEM_ACCESS | BGWORKER_CLASS_PARALLEL;
    let p0 = register("p0", flags).expect("under cap");
    let _p1 = register("p1", flags).expect("under cap");
    assert!(register("p2", flags).is_none());
    assert!(register("np", BGWORKER_SHMEM_ACCESS).is_some());

    BackgroundWorkerStateChange(true);
    let idx = registered_idx_for(&p0);
    ForgetBackgroundWorker(idx);
    with_registry(|reg| {
        assert_eq!(reg.parallel_register_count, 2);
        assert_eq!(reg.parallel_terminate_count, 1);
    });
    assert!(register("p3", flags).is_some());
}

#[test]
fn handle_status_transitions() {
    let _g = bringup();
    let h = register("t0", BGWORKER_SHMEM_ACCESS).expect("slot");
    assert_eq!(GetBackgroundWorkerPid(&h), (BgwHandleStatus::BGWH_NOT_YET_STARTED, 0));

    BackgroundWorkerStateChange(true);
    let idx = registered_idx_for(&h);
    assert_eq!(rw_type(idx), "t0"); // bgw_type defaulted from bgw_name

    set_rw_pid(idx, 1234);
    ReportBackgroundWorkerPID(idx);
    assert_eq!(GetBackgroundWorkerPid(&h), (BgwHandleStatus::BGWH_STARTED, 1234));
    assert_eq!(GetBackgroundWorkerTypeByPid(1234).as_deref(), Some("t0"));

    set_rw_pid(idx, 0);
    ReportBackgroundWorkerExit(idx);
    assert_eq!(GetBackgroundWorkerPid(&h), (BgwHandleStatus::BGWH_STOPPED, 0));
    with_registry(|reg| {
        assert!(!reg.slots[h.slot as usize].in_use);
        assert!(reg.registered.iter().all(|rw| rw.is_none()));
    });

    let stale = BackgroundWorkerHandle { slot: h.slot, generation: h.generation + 7 };
    assert_eq!(GetBackgroundWorkerPid(&stale), (BgwHandleStatus::BGWH_STOPPED, 0));
}

#[test]
fn terminate_before_start_frees_slot_and_counts_parallel() {
    let _g = bringup();
    let flags = BGWORKER_SHMEM_ACCESS | BGWORKER_CLASS_PARALLEL;
    let h = register("pt", flags).expect("slot");
    TerminateBackgroundWorker(&h);

    BackgroundWorkerStateChange(true);
    with_registry(|reg| {
        assert!(!reg.slots[h.slot as usize].in_use);
        assert_eq!(reg.parallel_register_count, 1);
        assert_eq!(reg.parallel_terminate_count, 1);
        assert!(reg.registered.iter().all(|rw| rw.is_none()));
    });
    assert_eq!(GetBackgroundWorkerPid(&h), (BgwHandleStatus::BGWH_STOPPED, 0));
}

#[test]
fn state_change_disallowing_new_workers_terminates_pending() {
    let _g = bringup();
    let h = register("dn", BGWORKER_SHMEM_ACCESS).expect("slot");
    BackgroundWorkerStateChange(false);
    with_registry(|reg| assert!(!reg.slots[h.slot as usize].in_use));
    assert_eq!(GetBackgroundWorkerPid(&h), (BgwHandleStatus::BGWH_STOPPED, 0));
}

#[test]
fn running_terminated_worker_marks_rw_terminate() {
    let _g = bringup();
    let h = register("rt", BGWORKER_SHMEM_ACCESS).expect("slot");
    BackgroundWorkerStateChange(true);
    let idx = registered_idx_for(&h);
    set_rw_pid(idx, 4321);
    ReportBackgroundWorkerPID(idx);

    TerminateBackgroundWorker(&h);
    BackgroundWorkerStateChange(true);
    assert!(rw_terminate(idx));

    set_rw_pid(idx, 0);
    ReportBackgroundWorkerExit(idx);
    assert_eq!(GetBackgroundWorkerPid(&h), (BgwHandleStatus::BGWH_STOPPED, 0));
}

#[test]
fn forget_unstarted_only_zaps_waited_on_workers() {
    let _g = bringup();
    let mut w = mk_worker("fu", BGWORKER_SHMEM_ACCESS);
    w.bgw_notify_pid = 7777;
    let h = RegisterDynamicBackgroundWorker(w).expect("register").expect("slot");
    let h2 = register("fu2", BGWORKER_SHMEM_ACCESS).expect("slot");

    // notify-pid validation needs the pmchild seam; simulate the postmaster
    // knowing pid 7777 by patching after registration instead.
    with_registry(|reg| {
        reg.slots[h.slot as usize].worker.as_mut().unwrap().bgw_notify_pid = 0;
    });
    BackgroundWorkerStateChange(true);
    let idx = registered_idx_for(&h);
    with_registry(|reg| rw_mut(reg, idx).worker.bgw_notify_pid = 7777);

    ForgetUnstartedBackgroundWorkers();
    assert_eq!(GetBackgroundWorkerPid(&h), (BgwHandleStatus::BGWH_STOPPED, 0));
    assert_eq!(GetBackgroundWorkerPid(&h2), (BgwHandleStatus::BGWH_NOT_YET_STARTED, 0));
}

#[test]
fn shmem_reinit_zeroes_parallel_counts() {
    let _g = bringup();
    let flags = BGWORKER_SHMEM_ACCESS | BGWORKER_CLASS_PARALLEL;
    let _ = register("z0", flags).expect("slot");
    with_registry(|reg| assert_eq!(reg.parallel_register_count, 1));
    ResetBackgroundWorkerCrashTimes();
    BackgroundWorkerShmemInit();
    with_registry(|reg| {
        assert_eq!(reg.parallel_register_count, 0);
        assert_eq!(reg.parallel_terminate_count, 0);
        assert!(reg.slots.iter().all(|s| !s.in_use));
    });
}

#[test]
fn name_and_type_truncate_at_bgw_maxlen() {
    let _g = bringup();
    let long = "x".repeat(200);
    let h = register(&long, BGWORKER_SHMEM_ACCESS).expect("slot");
    with_registry(|reg| {
        let w = reg.slots[h.slot as usize].worker.as_ref().unwrap();
        assert_eq!(w.bgw_name.len(), BGW_MAXLEN - 1);
    });
}

#[test]
fn get_background_worker_type_by_pid() {
    let _g = bringup();
    let mut w = mk_worker("typed", BGWORKER_SHMEM_ACCESS);
    w.bgw_type = "test worker".to_string();
    let h = RegisterDynamicBackgroundWorker(w).expect("register").expect("slot");
    BackgroundWorkerStateChange(true);
    let idx = registered_idx_for(&h);
    with_registry(|reg| rw_mut(reg, idx).pid = 4242);
    ReportBackgroundWorkerPID(idx);
    assert_eq!(GetBackgroundWorkerTypeByPid(4242).as_deref(), Some("test worker"));
    assert_eq!(GetBackgroundWorkerTypeByPid(4243), None);
}

#[test]
fn static_registration_takes_slot_at_shmem_init() {
    let _g = bringup();
    // Static registration happens in the postmaster, before shmem init.
    *REGISTRY.lock().unwrap_or_else(|e| e.into_inner()) = None;
    STATIC_PENDING.lock().unwrap_or_else(|e| e.into_inner()).clear();
    g::SetIsUnderPostmaster(false);
    let mut w = mk_worker("static launcher", BGWORKER_SHMEM_ACCESS);
    w.bgw_restart_time = 5;
    RegisterBackgroundWorker(&w);
    // Too many: silently rejected at LOG (max_worker_processes = 4).
    for i in 0..5 {
        RegisterBackgroundWorker(&mk_worker(&format!("s{i}"), BGWORKER_SHMEM_ACCESS));
    }
    BackgroundWorkerShmemInit();
    g::SetIsUnderPostmaster(true);

    with_registry(|reg| {
        let n_used = reg.slots.iter().filter(|s| s.in_use).count();
        assert_eq!(n_used, 4, "4 slots (cap), 5th static registration rejected");
        let s0 = &reg.slots[0];
        assert_eq!(s0.worker.as_ref().unwrap().bgw_name, "static launcher");
        assert_eq!(s0.pid, InvalidPid);
        let idx = find_rw_by_slot(reg, 0).expect("registered entry");
        assert_eq!(rw_ref(reg, idx).worker.bgw_restart_time, 5);
    });

    // Registration after shmem init in a backend is refused at LOG, not panic.
    RegisterBackgroundWorker(&mk_worker("late", BGWORKER_SHMEM_ACCESS));
    with_registry(|reg| {
        assert_eq!(reg.slots.iter().filter(|s| s.in_use).count(), 4);
    });
}
