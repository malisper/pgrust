// Seam slots are set-once per process: mocks install under a Once, read
// programmable state and write a call log through a Mutex, and TEST_LOCK
// serializes the tests around that shared state.

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
        latch_seams::set_latch_my_latch::set(|| record(Call::SetLatchMyLatch));
        guc_file_seams::process_config_file::set(|context| {
            record(Call::ProcessConfigFile { context });
            Ok(())
        });
        procsignal_seams::proc_signal_barrier_pending::set(|| with_mock(|m| m.barrier_pending));
        procsignal_seams::process_proc_signal_barrier::set(|| {
            record(Call::ProcessProcSignalBarrier);
            Ok(())
        });
        mcxt_seams::log_memory_context_pending::set(|| with_mock(|m| m.log_memctx_pending));
        mcxt_seams::process_log_memory_context_interrupt::set(|| {
            record(Call::ProcessLogMemoryContextInterrupt);
            Ok(())
        });
        init_small_seams::my_proc_pid::set(|| std::process::id() as i32);
        ipc_seams::proc_exit::set(|code, _my_pid| -> ! {
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
fn flags_round_trip() {
    let _guard = TEST_LOCK.lock().unwrap();
    reset_flags();

    assert!(!ConfigReloadPending());
    SetConfigReloadPending(true);
    assert!(ConfigReloadPending());
    SetConfigReloadPending(false);
    assert!(!ConfigReloadPending());

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
fn main_loop_reloads_config_and_clears_flag_first() {
    let _guard = TEST_LOCK.lock().unwrap();
    reset_flags();
    install_mock(false, false);
    SetConfigReloadPending(true);

    ProcessMainLoopInterrupts().unwrap();

    assert!(!ConfigReloadPending());
    assert_eq!(
        recorded_calls(),
        vec![Call::ProcessConfigFile { context: types_guc::PGC_SIGHUP }]
    );
}

#[test]
fn main_loop_logs_memory_contexts_when_pending() {
    let _guard = TEST_LOCK.lock().unwrap();
    reset_flags();
    install_mock(false, true);

    ProcessMainLoopInterrupts().unwrap();

    assert_eq!(recorded_calls(), vec![Call::ProcessLogMemoryContextInterrupt]);
}

#[test]
fn main_loop_exits_on_shutdown_request_before_memctx_check() {
    let _guard = TEST_LOCK.lock().unwrap();
    reset_flags();
    install_mock(false, true);
    SetShutdownRequestPending(true);

    let result = catch_unwind(AssertUnwindSafe(ProcessMainLoopInterrupts));
    assert!(result.is_err(), "proc_exit(0) must not return");
    assert_eq!(recorded_calls(), vec![Call::ProcExit { code: 0 }]);
}

#[test]
fn main_loop_runs_all_arms_in_c_order() {
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
            Call::ProcessConfigFile { context: types_guc::PGC_SIGHUP },
            Call::ProcessLogMemoryContextInterrupt,
        ]
    );
}
