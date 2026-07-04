use super::*;
use types_error::ERRCODE_INVALID_TRANSACTION_STATE;

fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

struct ParallelModeGuard;
impl ParallelModeGuard {
    fn enter() -> Self {
        xact::EnterParallelMode();
        ParallelModeGuard
    }
}
impl Drop for ParallelModeGuard {
    fn drop(&mut self) {
        xact::ExitParallelMode();
    }
}

// Fence surface: texts and SQLSTATEs must match live C 18.3
// (scripts/parallel-fence-probe-e2e.sh captures the C side).
#[test]
fn fence_prevent_command_if_parallel_mode() {
    let _s = serial();
    let _g = ParallelModeGuard::enter();
    let err = xact::PreventCommandIfParallelMode("INSERT").unwrap_err();
    assert_eq!(err.message(), "cannot execute INSERT during a parallel operation");
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TRANSACTION_STATE);
}

#[test]
fn fence_query_snapshot_in_parallel_mode() {
    let _s = serial();
    xact::init_seams();
    let _g = ParallelModeGuard::enter();
    let err = snapmgr::GetTransactionSnapshot().unwrap_err();
    assert_eq!(err.message(), "cannot take query snapshot during a parallel operation");
}

#[test]
fn entrypoint_lookup_and_registration() {
    let _s = serial();
    fn entry(_: &ParallelShared) -> PgResult<()> {
        Ok(())
    }
    register_parallel_worker_entrypoint("substrate_test_entry", entry);
    assert!(LookupParallelWorkerFunction("postgres", "substrate_test_entry").is_ok());
    let err = LookupParallelWorkerFunction("postgres", "no_such_entry").unwrap_err();
    assert_eq!(err.message(), "internal function \"no_such_entry\" not found");
}

#[test]
fn worker_error_clamps_level_and_appends_context() {
    let _s = serial();
    let mut e = PgError::new(FATAL, "worker blew up").with_context("inner frame");
    if e.level > ERROR {
        e.level = ERROR;
    }
    append_parallel_worker_context(&mut e);
    assert_eq!(e.level, ERROR);
    assert_eq!(e.context(), Some("inner frame\nparallel worker"));
}

#[test]
fn parallel_context_requires_parallel_mode_and_lists() {
    let _s = serial();
    assert!(!ParallelContextActive());
    let _g = ParallelModeGuard::enter();
    let id = CreateParallelContext("postgres", "substrate_test_entry", 0).unwrap();
    assert!(ParallelContextActive());
    DestroyParallelContext(id).unwrap();
    assert!(!ParallelContextActive());
}
