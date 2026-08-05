use super::*;
use types_error::ERRCODE_INVALID_TRANSACTION_STATE;

fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// Caller must hold serial(): installed-check and install are two steps.
fn xact_seams_boot() {
    if !xact_seams::is_in_parallel_mode::is_installed() {
        xact::init_seams();
    }
}

fn guc_boot() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        guc_tables::init_seams();
        guc::init_seams();
        adt_bool::init_seams();
        adt_float::init_seams();
        install_debug_parallel_query_accessor();
    });
    xact_seams_boot();
    std::thread_local! {
        static ARMED: Cell<bool> = const { Cell::new(false) };
    }
    ARMED.with(|armed| {
        if !armed.get() {
            guc::store::initialize_guc_options().unwrap();
            armed.set(true);
        }
    });
}

// The planner owns this accessor in production; tests install a stand-in.
pub(crate) fn install_debug_parallel_query_accessor() {
    use std::sync::atomic::{AtomicI32, Ordering::Relaxed};
    static DPQ: AtomicI32 = AtomicI32::new(0);
    guc_tables::vars::debug_parallel_query.install_if_absent(guc_tables::GucVarAccessors {
        get: || DPQ.load(Relaxed),
        set: |v| DPQ.store(v, Relaxed),
    });
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
    xact_seams_boot();
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
    guc_boot();
    let mut e = PgError::new(FATAL, "worker blew up").with_context("inner frame");
    if e.level > ERROR {
        e.level = ERROR;
    }
    append_parallel_worker_context(&mut e);
    assert_eq!(e.level, ERROR);
    assert_eq!(e.context(), Some("inner frame\nparallel worker"));
}

#[test]
fn frontend_redirect_captures_sub_error_reports() {
    let _s = serial();
    guc_boot();
    let (tx, rx) = std::sync::mpsc::sync_channel::<Box<PgError>>(4);
    let prev = elog::set_frontend_redirect(Some(Box::new(move |e: &PgError| {
        if e.level < ERROR {
            let _ = tx.send(Box::new(e.clone()));
        }
    })));
    let prev_dest = elog::config::where_to_send_output();
    elog::config::set_where_to_send_output(types_dest::CommandDest::Remote);
    let _ = elog::elog(types_error::NOTICE, "worker says hi");
    elog::config::set_where_to_send_output(prev_dest);
    elog::set_frontend_redirect(prev);
    let captured = rx.try_recv().expect("notice did not reach the redirect");
    assert_eq!(captured.message(), "worker says hi");
    assert_eq!(captured.level, types_error::NOTICE);
}

// C dlist_push_head: AtEOSubXact scans newest-first or inner-subxact
// contexts leak past subxact end.
#[test]
fn pcxt_list_is_lifo() {
    let _s = serial();
    let _g = ParallelModeGuard::enter();
    let a = CreateParallelContext("postgres", "substrate_test_entry", 0).unwrap();
    let b = CreateParallelContext("postgres", "substrate_test_entry", 0).unwrap();
    let front = PCXT_LIST.with(|l| l.borrow().first().map(|p| ParallelContextId(p.id)));
    assert_eq!(front, Some(b));
    DestroyParallelContext(b).unwrap();
    DestroyParallelContext(a).unwrap();
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

// PHASE3-CLOSE WS-WIDTH §2.2 clamp-seam unit (band 99001; fence
// escalation EX-WIDTH-1, notes/se-p3close-width.md §1.5 — addition-only,
// this crate's product code is untouched): the width lease clamps launch
// counts ONLY through C's own lower-the-launch mechanism. Plan width N,
// grant k<N ⇒ nworkers_to_launch == k while pcxt.nworkers (the DSM/queue
// sizing input) stays N; grant 0 ⇒ launch count 0; a grant ABOVE plan
// width never raises the launch (ReinitializeParallelWorkers mins with
// nworkers); a fresh startup (ExecParallelReinitialize path) restores
// plan width before the next lease clamps.
#[test]
fn reinitialize_clamps_launch_count_only() {
    let _s = serial();
    xact_seams_boot();
    let _g = ParallelModeGuard::enter();
    let id = CreateParallelContext("postgres", "substrate_test_entry", 4).unwrap();
    assert_eq!(nworkers(id), 4);
    assert_eq!(nworkers_to_launch(id), 4, "launch count starts at plan width");

    // Grant k < N: launched < planned, DSM sizing (nworkers) untouched.
    ReinitializeParallelWorkers(id, 2);
    assert_eq!(nworkers_to_launch(id), 2, "clamped to the grant");
    assert_eq!(nworkers(id), 4, "DSM/queue sizing stays at plan width");

    // Grant 0: launches NOTHING (the leader-local serial path serves).
    ReinitializeParallelWorkers(id, 0);
    assert_eq!(nworkers_to_launch(id), 0);
    assert_eq!(nworkers(id), 4);

    // A grant above plan width never raises the launch (min with plan).
    ReinitializeParallelWorkers(id, 9);
    assert_eq!(nworkers_to_launch(id), 4, "never above plan width");

    DestroyParallelContext(id).unwrap();
}
