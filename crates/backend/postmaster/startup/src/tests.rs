#[test]
fn main_fn_matches_child_main_shape() {
    let f: fn(&types_startup::StartupData) -> ! = super::StartupProcessMain;
    let _ = f;
}

#[test]
fn promote_flag_roundtrip() {
    assert!(!super::IsPromoteSignaled());
    super::PROMOTE_SIGNALED.store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(super::IsPromoteSignaled());
    super::ResetPromoteSignaled();
    assert!(!super::IsPromoteSignaled());
}

// startup.c:207 StartupProcExit + elog.c:375-381/587: an ERROR raised by
// ShutdownRecoveryTransactionEnvironment inside the on_shmem_exit callback
// is promoted to FATAL (no exception stack, proc_exit in progress) and
// errfinish re-enters proc_exit(1), so the startup process finishes its exit
// with code 1 — never a panic, never the original code.
#[test]
fn startup_proc_exit_error_is_fatal_and_reenters_proc_exit_like_c() {
    use std::panic::{catch_unwind, AssertUnwindSafe};

    ipc::init_seams();
    init_small::init_seams();
    init_small::globals::SetMyProcPid(4242);
    xlogutils::set_standby_state(xlogutils::STANDBY_INITIALIZED);
    standby_seams::shutdown_recovery_transaction_environment::set(|| {
        Err(Box::new(types_error::PgError::error("lock table corrupted")))
    });

    ipc::on_shmem_exit(super::StartupProcExit, 0);
    let payload = catch_unwind(AssertUnwindSafe(|| ipc::proc_exit(0, 4242))).unwrap_err();
    let code = payload
        .downcast_ref::<ipc::ProcExitThread>()
        .expect("unwind payload is ProcExitThread")
        .code;
    assert_eq!(code, 1, "C: FATAL inside the exit callback re-enters proc_exit(1)");
}

// startup.c:371 has_startup_progress_timeout_expired -> TimestampDifference
// (timestamp.c:1730): a stop time at or before the phase start time yields
// (0, 0), never negative seconds/microseconds.
#[test]
fn startup_progress_elapsed_clamps_to_zero_when_clock_steps_back() {
    timestamp_seams::get_current_timestamp::set(|| 1_000_000);

    super::STARTUP_PROGRESS_PHASE_START_TIME.set(5_500_000);
    super::STARTUP_PROGRESS_TIMER_EXPIRED.store(true, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(super::has_startup_progress_timeout_expired(), Some((0, 0)));
    assert_eq!(super::has_startup_progress_timeout_expired(), None, "flag consumed");

    super::STARTUP_PROGRESS_PHASE_START_TIME.set(1_000_000);
    super::STARTUP_PROGRESS_TIMER_EXPIRED.store(true, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(super::has_startup_progress_timeout_expired(), Some((0, 0)), "diff == 0 arm");

    super::STARTUP_PROGRESS_PHASE_START_TIME.set(-2_500_000);
    super::STARTUP_PROGRESS_TIMER_EXPIRED.store(true, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(super::has_startup_progress_timeout_expired(), Some((3, 500_000)));
}
