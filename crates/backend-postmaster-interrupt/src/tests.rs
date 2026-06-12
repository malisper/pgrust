//! The two owned flags are exercised directly. To verify that
//! [`ProcessMainLoopInterrupts`] and the signal-handler bodies hit the
//! correct external seams in the correct order, these tests install
//! deterministic in-process mocks of the outward seams and assert the exact
//! call sequence.
//!
//! Seam slots install once per process, so the mocks are installed by a
//! `Once` and read their programmable state (and write their call log)
//! through a shared `Mutex`; `TEST_LOCK` serializes the tests around it.

use super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Mutex, Once};

static TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Debug, PartialEq, Eq)]
enum Call {
    SetLatchMyLatch,
    ProcExit { code: i32 },
    ProcessConfigFile { context: types_guc::GucContext },
    ProcessProcSignalBarrier,
    ProcessLogMemoryContextInterrupt,
}

struct MockState {
    calls: Vec<Call>,
    barrier_pending: bool,
    log_memctx_pending: bool,
}

static MOCK: Mutex<Option<MockState>> = Mutex::new(None);

fn with_mock<R>(f: impl FnOnce(&mut MockState) -> R) -> R {
    let mut guard = MOCK.lock().unwrap();
    f(guard.as_mut().expect("mock not installed"))
}

fn record(call: Call) {
    with_mock(|m| m.calls.push(call));
}

fn install_mock(barrier_pending: bool, log_memctx_pending: bool) {
    *MOCK.lock().unwrap() = Some(MockState {
        calls: Vec::new(),
        barrier_pending,
        log_memctx_pending,
    });

    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        backend_storage_ipc_latch_seams::set_latch_my_latch::set(|| {
            record(Call::SetLatchMyLatch)
        });
        backend_utils_misc_guc_seams::process_config_file::set(|context| {
            record(Call::ProcessConfigFile { context });
            Ok(())
        });
        backend_storage_ipc_procsignal_seams::proc_signal_barrier_pending::set(|| {
            with_mock(|m| m.barrier_pending)
        });
        backend_storage_ipc_procsignal_seams::process_proc_signal_barrier::set(|| {
            record(Call::ProcessProcSignalBarrier);
            Ok(())
        });
        backend_utils_mmgr_mcxt_seams::log_memory_context_pending::set(|| {
            with_mock(|m| m.log_memctx_pending)
        });
        backend_utils_mmgr_mcxt_seams::process_log_memory_context_interrupt::set(|| {
            record(Call::ProcessLogMemoryContextInterrupt);
            Ok(())
        });
        backend_storage_ipc_seams::proc_exit::set(|code| -> ! {
            record(Call::ProcExit { code });
            panic!("test-proc-exit");
        });
    });
}

fn recorded_calls() -> Vec<Call> {
    with_mock(|m| m.calls.clone())
}

fn reset_flags() {
    SetConfigReloadPending(false);
    SetShutdownRequestPending(false);
}

#[test]
fn config_reload_flag_round_trips() {
    let _guard = TEST_LOCK.lock().unwrap();
    reset_flags();

    assert!(!ConfigReloadPending());
    SetConfigReloadPending(true);
    assert!(ConfigReloadPending());
    SetConfigReloadPending(false);
    assert!(!ConfigReloadPending());
}

#[test]
fn shutdown_request_flag_round_trips() {
    let _guard = TEST_LOCK.lock().unwrap();
    reset_flags();

    assert!(!ShutdownRequestPending());
    SetShutdownRequestPending(true);
    assert!(ShutdownRequestPending());
    SetShutdownRequestPending(false);
    assert!(!ShutdownRequestPending());
}

#[test]
fn config_reload_handler_sets_flag_and_latch() {
    let _guard = TEST_LOCK.lock().unwrap();
    reset_flags();
    install_mock(false, false);

    SignalHandlerForConfigReload();

    assert!(ConfigReloadPending());
    assert_eq!(recorded_calls(), vec![Call::SetLatchMyLatch]);
}

#[test]
fn shutdown_request_handler_sets_flag_and_latch() {
    let _guard = TEST_LOCK.lock().unwrap();
    reset_flags();
    install_mock(false, false);

    SignalHandlerForShutdownRequest();

    assert!(ShutdownRequestPending());
    assert_eq!(recorded_calls(), vec![Call::SetLatchMyLatch]);
}

#[test]
fn main_loop_does_nothing_when_all_flags_clear() {
    let _guard = TEST_LOCK.lock().unwrap();
    reset_flags();
    install_mock(false, false);

    ProcessMainLoopInterrupts().unwrap();

    assert!(recorded_calls().is_empty());
}

#[test]
fn main_loop_processes_barrier_when_pending() {
    let _guard = TEST_LOCK.lock().unwrap();
    reset_flags();
    install_mock(true, false);

    ProcessMainLoopInterrupts().unwrap();

    assert_eq!(recorded_calls(), vec![Call::ProcessProcSignalBarrier]);
}

#[test]
fn main_loop_reloads_config_and_clears_flag() {
    let _guard = TEST_LOCK.lock().unwrap();
    reset_flags();
    install_mock(false, false);
    SetConfigReloadPending(true);

    ProcessMainLoopInterrupts().unwrap();

    // The flag is cleared before ProcessConfigFile runs.
    assert!(!ConfigReloadPending());
    assert_eq!(
        recorded_calls(),
        vec![Call::ProcessConfigFile {
            context: types_guc::PGC_SIGHUP
        }]
    );
}

#[test]
fn main_loop_logs_memory_context_when_pending() {
    let _guard = TEST_LOCK.lock().unwrap();
    reset_flags();
    install_mock(false, true);

    ProcessMainLoopInterrupts().unwrap();

    assert_eq!(
        recorded_calls(),
        vec![Call::ProcessLogMemoryContextInterrupt]
    );
}

#[test]
fn main_loop_exits_on_shutdown_request_before_memctx_check() {
    let _guard = TEST_LOCK.lock().unwrap();
    reset_flags();
    // Even with the log-memctx flag set, proc_exit(0) must short-circuit it.
    install_mock(false, true);
    SetShutdownRequestPending(true);

    let result = catch_unwind(AssertUnwindSafe(|| ProcessMainLoopInterrupts()));
    assert!(result.is_err(), "proc_exit(0) must not return");

    assert_eq!(recorded_calls(), vec![Call::ProcExit { code: 0 }]);
}

#[test]
fn main_loop_runs_all_arms_in_order() {
    let _guard = TEST_LOCK.lock().unwrap();
    reset_flags();
    install_mock(true, true);
    SetConfigReloadPending(true);

    ProcessMainLoopInterrupts().unwrap();

    assert!(!ConfigReloadPending());
    assert_eq!(
        recorded_calls(),
        vec![
            Call::ProcessProcSignalBarrier,
            Call::ProcessConfigFile {
                context: types_guc::PGC_SIGHUP
            },
            Call::ProcessLogMemoryContextInterrupt,
        ]
    );
}
