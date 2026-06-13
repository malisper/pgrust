//! The flag state machine and the startup-progress cadence are exercised
//! against deterministic in-process mocks of the outward seams. Seam slots
//! install once per process, so the mocks are installed by a `Once` and read
//! their programmable state (and write their call log) through a shared
//! `Mutex`; `TEST_LOCK` serializes the tests around it (the thread-local
//! flags also require the tests to share one thread, which the lock
//! enforces by construction only if tests run on the locked path — so each
//! test resets the flags it uses).

use super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Mutex, MutexGuard, Once};

static TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Debug, PartialEq, Eq)]
enum Call {
    WakeupRecovery,
    ProcessConfigFile,
    RequestWalRcvRestart,
    ProcSignalBarrier,
    LogMemoryContext,
    ShutdownRecoveryTxnEnv,
    DisableTimeout { id: types_timeout::TimeoutId, keep_indicator: bool },
    EnableTimeoutEvery { id: types_timeout::TimeoutId, fin_time: i64, delay_ms: i32 },
    SetBackendType(types_core::init::BackendType),
    AuxProcessMainCommon,
    OnShmemExit,
    InitializeTimeouts,
    RegisterTimeout { id: types_timeout::TimeoutId },
    Pqsignal { signo: i32 },
    StartupXlog,
}

struct MockState {
    calls: Vec<Call>,
    conninfo: String,
    slotname: String,
    temp_slot: bool,
    /// Values the mocked `ProcessConfigFile` applies, modelling a config
    /// reload that changes the GUCs mid-`StartupRereadConfig`.
    reload_sets_conninfo: Option<String>,
    reload_sets_slotname: Option<String>,
    reload_sets_temp_slot: Option<bool>,
    is_under_postmaster: bool,
    postmaster_alive: bool,
    barrier_pending: bool,
    log_memctx_pending: bool,
    standby_state: types_wal::HotStandbyState,
    now: i64,
}

static MOCK: Mutex<Option<MockState>> = Mutex::new(None);

fn with_mock<R>(f: impl FnOnce(&mut MockState) -> R) -> R {
    let mut guard = MOCK.lock().unwrap();
    f(guard.as_mut().expect("mock not installed"))
}

fn record(call: Call) {
    with_mock(|m| m.calls.push(call));
}

const EXIT_TAG: &str = "TEST_SEAM_PROC_EXIT:";

fn mock_proc_exit(code: i32, _my_pid: i32) -> ! {
    panic!("{EXIT_TAG}{code}");
}

fn install_mocks() -> MutexGuard<'static, ()> {
    let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    *MOCK.lock().unwrap() = Some(MockState {
        calls: Vec::new(),
        conninfo: String::new(),
        slotname: String::new(),
        temp_slot: false,
        reload_sets_conninfo: None,
        reload_sets_slotname: None,
        reload_sets_temp_slot: None,
        is_under_postmaster: false,
        postmaster_alive: true,
        barrier_pending: false,
        log_memctx_pending: false,
        standby_state: types_wal::STANDBY_DISABLED,
        now: 0,
    });

    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        backend_access_transam_xlogrecovery_seams::wakeup_recovery::set(|| {
            record(Call::WakeupRecovery)
        });
        backend_access_transam_xlogrecovery_seams::primary_conninfo::set(|mcx| {
            with_mock(|m| mcx::PgString::from_str_in(&m.conninfo, mcx))
        });
        backend_access_transam_xlogrecovery_seams::primary_slot_name::set(|mcx| {
            with_mock(|m| mcx::PgString::from_str_in(&m.slotname, mcx))
        });
        backend_access_transam_xlogrecovery_seams::wal_receiver_create_temp_slot::set(|| {
            with_mock(|m| m.temp_slot)
        });
        backend_access_transam_xlogrecovery_seams::startup_request_wal_receiver_restart::set(
            || record(Call::RequestWalRcvRestart),
        );
        backend_utils_misc_guc_file_seams::process_config_file::set(|_context| {
            with_mock(|m| {
                if let Some(v) = m.reload_sets_conninfo.take() {
                    m.conninfo = v;
                }
                if let Some(v) = m.reload_sets_slotname.take() {
                    m.slotname = v;
                }
                if let Some(v) = m.reload_sets_temp_slot.take() {
                    m.temp_slot = v;
                }
            });
            record(Call::ProcessConfigFile);
            Ok(())
        });
        backend_storage_ipc_seams::proc_exit::set(mock_proc_exit);
        backend_utils_init_small_seams::my_proc_pid::set(|| 12345);
        backend_storage_ipc_seams::on_shmem_exit::set(|_f, _arg| {
            record(Call::OnShmemExit);
            Ok(())
        });
        backend_utils_init_small_seams::is_under_postmaster::set(|| {
            with_mock(|m| m.is_under_postmaster)
        });
        backend_utils_init_small_seams::set_my_backend_type::set(|t| {
            record(Call::SetBackendType(t))
        });
        backend_storage_ipc_pmsignal_seams::postmaster_is_alive::set(|| {
            with_mock(|m| m.postmaster_alive)
        });
        backend_storage_ipc_procsignal_seams::proc_signal_barrier_pending::set(|| {
            with_mock(|m| m.barrier_pending)
        });
        backend_storage_ipc_procsignal_seams::process_proc_signal_barrier::set(|| {
            with_mock(|m| m.barrier_pending = false);
            record(Call::ProcSignalBarrier);
            Ok(())
        });
        backend_storage_ipc_procsignal_seams::procsignal_sigusr1_handler::set(|_signo| {});
        backend_utils_mmgr_mcxt_seams::log_memory_context_pending::set(|| {
            with_mock(|m| m.log_memctx_pending)
        });
        backend_utils_mmgr_mcxt_seams::process_log_memory_context_interrupt::set(|| {
            with_mock(|m| m.log_memctx_pending = false);
            record(Call::LogMemoryContext);
            Ok(())
        });
        backend_access_transam_xlogutils_seams::standby_state::set(|| {
            with_mock(|m| m.standby_state)
        });
        backend_storage_ipc_standby_seams::shutdown_recovery_transaction_environment::set(
            || {
                record(Call::ShutdownRecoveryTxnEnv);
                Ok(())
            },
        );
        backend_storage_ipc_standby_seams::standby_dead_lock_handler::set(|| {});
        backend_storage_ipc_standby_seams::standby_timeout_handler::set(|| {});
        backend_storage_ipc_standby_seams::standby_lock_timeout_handler::set(|| {});
        backend_utils_adt_timestamp_seams::get_current_timestamp::set(|| with_mock(|m| m.now));
        backend_utils_adt_timestamp_seams::timestamp_difference::set(|start, stop| {
            let diff = (stop - start).max(0);
            ((diff / 1_000_000), (diff % 1_000_000) as i32)
        });
        backend_utils_misc_timeout_seams::initialize_timeouts::set(|| {
            record(Call::InitializeTimeouts)
        });
        backend_utils_misc_timeout_seams::register_timeout::set(|id, _handler| {
            record(Call::RegisterTimeout { id });
            id
        });
        backend_utils_misc_timeout_seams::disable_timeout::set(|id, keep_indicator| {
            record(Call::DisableTimeout { id, keep_indicator })
        });
        backend_utils_misc_timeout_seams::enable_timeout_every::set(|id, fin_time, delay_ms| {
            record(Call::EnableTimeoutEvery {
                id,
                fin_time,
                delay_ms,
            })
        });
        backend_postmaster_auxprocess_seams::auxiliary_process_main_common::set(|| {
            record(Call::AuxProcessMainCommon);
            Ok(())
        });
        port_pqsignal_seams::pqsignal::set(|signo, _func| {
            record(Call::Pqsignal { signo });
        });
        backend_access_transam_xlog_seams::startup_xlog::set(|| {
            record(Call::StartupXlog);
            Ok(())
        });
    });

    // Reset the crate's thread-local state.
    GOT_SIGHUP.set(false);
    SHUTDOWN_REQUESTED.set(false);
    PROMOTE_SIGNALED.set(false);
    IN_RESTORE_COMMAND.set(false);
    STARTUP_PROGRESS_PHASE_START_TIME.set(0);
    STARTUP_PROGRESS_TIMER_EXPIRED.set(false);
    set_log_startup_progress_interval(10000);

    guard
}

fn calls() -> Vec<Call> {
    with_mock(|m| m.calls.clone())
}

fn assert_exits_with(code: i32, f: impl FnOnce()) {
    let err = catch_unwind(AssertUnwindSafe(f)).expect_err("expected proc_exit");
    let msg = err
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| err.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default();
    assert_eq!(msg, format!("{EXIT_TAG}{code}"));
}

#[test]
fn sighup_handler_sets_flag_and_wakes_recovery() {
    let _guard = install_mocks();
    StartupProcSigHupHandler(libc::SIGHUP);
    assert!(GOT_SIGHUP.get());
    assert_eq!(calls(), vec![Call::WakeupRecovery]);
}

#[test]
fn trigger_handler_sets_promote_flag() {
    let _guard = install_mocks();
    assert!(!IsPromoteSignaled());
    StartupProcTriggerHandler(libc::SIGUSR2);
    assert!(IsPromoteSignaled());
    assert_eq!(calls(), vec![Call::WakeupRecovery]);
    ResetPromoteSignaled();
    assert!(!IsPromoteSignaled());
}

#[test]
fn shutdown_handler_outside_restore_sets_flag_then_interrupts_exit() {
    let _guard = install_mocks();
    StartupProcShutdownHandler(libc::SIGTERM);
    assert!(SHUTDOWN_REQUESTED.get());
    assert_eq!(calls(), vec![Call::WakeupRecovery]);
    let ctx = mcx::MemoryContext::new("test");
    assert_exits_with(1, || {
        let _ = ProcessStartupProcInterrupts(ctx.mcx());
    });
}

#[test]
fn shutdown_handler_in_restore_command_exits_immediately() {
    let _guard = install_mocks();
    PreRestoreCommand();
    assert_exits_with(1, || StartupProcShutdownHandler(libc::SIGTERM));
    PostRestoreCommand();
    assert!(!IN_RESTORE_COMMAND.get());
}

#[test]
fn pre_restore_command_exits_if_shutdown_already_requested() {
    let _guard = install_mocks();
    SHUTDOWN_REQUESTED.set(true);
    assert_exits_with(1, PreRestoreCommand);
}

#[test]
fn sighup_reload_without_changes_does_not_restart_walreceiver() {
    let _guard = install_mocks();
    with_mock(|m| {
        m.conninfo = "host=primary".to_string();
        m.slotname = "slot".to_string();
    });
    GOT_SIGHUP.set(true);
    let ctx = mcx::MemoryContext::new("test");
    ProcessStartupProcInterrupts(ctx.mcx()).unwrap();
    assert!(!GOT_SIGHUP.get());
    assert_eq!(calls(), vec![Call::ProcessConfigFile]);
}

#[test]
fn sighup_reload_with_conninfo_change_restarts_walreceiver() {
    let _guard = install_mocks();
    with_mock(|m| {
        m.conninfo = "host=old".to_string();
        m.reload_sets_conninfo = Some("host=new".to_string());
    });
    GOT_SIGHUP.set(true);
    let ctx = mcx::MemoryContext::new("test");
    ProcessStartupProcInterrupts(ctx.mcx()).unwrap();
    assert_eq!(
        calls(),
        vec![Call::ProcessConfigFile, Call::RequestWalRcvRestart]
    );
}

#[test]
fn sighup_reload_with_slotname_change_restarts_walreceiver() {
    let _guard = install_mocks();
    with_mock(|m| {
        m.slotname = "old_slot".to_string();
        m.reload_sets_slotname = Some("new_slot".to_string());
    });
    GOT_SIGHUP.set(true);
    let ctx = mcx::MemoryContext::new("test");
    ProcessStartupProcInterrupts(ctx.mcx()).unwrap();
    assert_eq!(
        calls(),
        vec![Call::ProcessConfigFile, Call::RequestWalRcvRestart]
    );
}

#[test]
fn temp_slot_change_with_empty_slot_name_restarts_walreceiver() {
    let _guard = install_mocks();
    // Snapshot temp_slot=false; the reload flips it to true; the slot name
    // is empty and unchanged => walreceiver restart.
    with_mock(|m| m.reload_sets_temp_slot = Some(true));
    GOT_SIGHUP.set(true);
    let ctx = mcx::MemoryContext::new("test");
    ProcessStartupProcInterrupts(ctx.mcx()).unwrap();
    assert_eq!(
        calls(),
        vec![Call::ProcessConfigFile, Call::RequestWalRcvRestart]
    );
}

#[test]
fn temp_slot_change_is_ignored_when_slot_name_configured() {
    let _guard = install_mocks();
    // wal_receiver_create_temp_slot only matters with no slot configured.
    with_mock(|m| {
        m.slotname = "slot".to_string();
        m.reload_sets_temp_slot = Some(true);
    });
    GOT_SIGHUP.set(true);
    let ctx = mcx::MemoryContext::new("test");
    ProcessStartupProcInterrupts(ctx.mcx()).unwrap();
    assert_eq!(calls(), vec![Call::ProcessConfigFile]);
}

#[test]
fn barrier_and_memory_context_interrupts_are_serviced() {
    let _guard = install_mocks();
    with_mock(|m| {
        m.barrier_pending = true;
        m.log_memctx_pending = true;
    });
    let ctx = mcx::MemoryContext::new("test");
    ProcessStartupProcInterrupts(ctx.mcx()).unwrap();
    assert_eq!(calls(), vec![Call::ProcSignalBarrier, Call::LogMemoryContext]);
}

#[test]
fn startup_proc_exit_shuts_down_recovery_env_only_when_standby_active() {
    let _guard = install_mocks();
    StartupProcExit(0, types_datum::Datum::null());
    assert_eq!(calls(), vec![]);

    with_mock(|m| m.standby_state = types_wal::STANDBY_SNAPSHOT_READY);
    StartupProcExit(0, types_datum::Datum::null());
    assert_eq!(calls(), vec![Call::ShutdownRecoveryTxnEnv]);
}

#[test]
fn progress_phase_disables_then_enables_with_computed_fin_time() {
    let _guard = install_mocks();
    with_mock(|m| m.now = 5_000_000);
    set_log_startup_progress_interval(10000);
    begin_startup_progress_phase();
    assert_eq!(
        calls(),
        vec![
            Call::DisableTimeout {
                id: STARTUP_PROGRESS_TIMEOUT,
                keep_indicator: false
            },
            Call::EnableTimeoutEvery {
                id: STARTUP_PROGRESS_TIMEOUT,
                fin_time: 5_000_000 + 10_000 * 1000,
                delay_ms: 10000
            },
        ]
    );
    assert_eq!(STARTUP_PROGRESS_PHASE_START_TIME.get(), 5_000_000);
}

#[test]
fn progress_feature_disabled_when_interval_zero() {
    let _guard = install_mocks();
    set_log_startup_progress_interval(0);
    begin_startup_progress_phase();
    disable_startup_progress_timeout();
    enable_startup_progress_timeout();
    assert_eq!(calls(), vec![]);
}

#[test]
fn progress_timeout_expiry_reports_elapsed_and_resets() {
    let _guard = install_mocks();
    assert_eq!(has_startup_progress_timeout_expired(), None);

    with_mock(|m| m.now = 1_000_000);
    enable_startup_progress_timeout();
    with_mock(|m| m.now = 4_500_000);
    startup_progress_timeout_handler();
    assert_eq!(has_startup_progress_timeout_expired(), Some((3, 500_000)));
    // Flag is reset.
    assert_eq!(has_startup_progress_timeout_expired(), None);
}

#[test]
fn disable_progress_timeout_clears_expiry_flag() {
    let _guard = install_mocks();
    startup_progress_timeout_handler();
    disable_startup_progress_timeout();
    assert_eq!(has_startup_progress_timeout_expired(), None);
}

#[test]
fn startup_process_main_runs_boot_sequence_and_exits_zero() {
    let _guard = install_mocks();
    assert_exits_with(0, || {
        let _ = StartupProcessMain(&[]);
    });
    let seq = calls();
    assert_eq!(
        seq,
        vec![
            Call::SetBackendType(types_core::init::BackendType::Startup),
            Call::AuxProcessMainCommon,
            Call::OnShmemExit,
            Call::Pqsignal { signo: libc::SIGHUP },
            Call::Pqsignal { signo: libc::SIGINT },
            Call::Pqsignal { signo: libc::SIGTERM },
            Call::InitializeTimeouts,
            Call::Pqsignal { signo: libc::SIGPIPE },
            Call::Pqsignal { signo: libc::SIGUSR1 },
            Call::Pqsignal { signo: libc::SIGUSR2 },
            Call::Pqsignal { signo: libc::SIGCHLD },
            Call::RegisterTimeout {
                id: types_timeout::STANDBY_DEADLOCK_TIMEOUT
            },
            Call::RegisterTimeout {
                id: types_timeout::STANDBY_TIMEOUT
            },
            Call::RegisterTimeout {
                id: types_timeout::STANDBY_LOCK_TIMEOUT
            },
            Call::StartupXlog,
        ]
    );
}
