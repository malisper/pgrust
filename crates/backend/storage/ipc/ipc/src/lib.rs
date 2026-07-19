//! ipc.c: the proc_exit/shmem_exit exit-callback stacks. One backend = one
//! thread: the arrays and flags are TLS, and proc_exit ends the backend
//! THREAD by unwinding a [`ProcExitThread`] payload — never the process
//! (design + atexit divergence: notes/ipc-proc-exit-threads.md).

#![allow(non_snake_case)]

use std::cell::Cell;

use datum::Datum;
use types_error::{ErrorLocation, PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED, FATAL};

#[cfg(test)]
mod tests;

pub const MAX_ON_EXITS: usize = 20;

type BeforeShmemExitCallback = fn(code: i32, arg: Datum) -> PgResult<()>;
type OnExitCallback = fn(code: i32, arg: usize);

#[derive(Clone, Copy)]
struct BeforeOnExit {
    function: BeforeShmemExitCallback,
    arg: Datum,
}

#[derive(Clone, Copy)]
struct OnExit {
    function: OnExitCallback,
    arg: usize,
}

const NO_BEFORE: Option<BeforeOnExit> = None;
const NO_ON: Option<OnExit> = None;

thread_local! {
    static ON_PROC_EXIT_LIST: Cell<[Option<OnExit>; MAX_ON_EXITS]> = const { Cell::new([NO_ON; MAX_ON_EXITS]) };
    static ON_SHMEM_EXIT_LIST: Cell<[Option<OnExit>; MAX_ON_EXITS]> = const { Cell::new([NO_ON; MAX_ON_EXITS]) };
    static BEFORE_SHMEM_EXIT_LIST: Cell<[Option<BeforeOnExit>; MAX_ON_EXITS]> = const { Cell::new([NO_BEFORE; MAX_ON_EXITS]) };
    static ON_PROC_EXIT_INDEX: Cell<usize> = const { Cell::new(0) };
    static ON_SHMEM_EXIT_INDEX: Cell<usize> = const { Cell::new(0) };
    static BEFORE_SHMEM_EXIT_INDEX: Cell<usize> = const { Cell::new(0) };
    static PROC_EXIT_INPROGRESS: Cell<bool> = const { Cell::new(false) };
    static SHMEM_EXIT_INPROGRESS: Cell<bool> = const { Cell::new(false) };
    static EXIT_CALLBACKS_DEFERRED: Cell<bool> = const { Cell::new(false) };
}

/// Unwind payload of a normal backend-thread exit; the joiner recovers the C
/// exit code via downcast. Any other payload at the thread top is a crash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcExitThread {
    pub code: i32,
}

#[inline]
pub fn proc_exit_inprogress() -> bool {
    PROC_EXIT_INPROGRESS.with(Cell::get)
}

#[inline]
pub fn shmem_exit_inprogress() -> bool {
    SHMEM_EXIT_INPROGRESS.with(Cell::get)
}

/// Retention park (wretain): commit_to_exit committed this thread to
/// "exiting" (exit flags, ERROR->FATAL promotion, held interrupts, suppressed
/// statement logging); a parked standby lives on and must return to the
/// fresh-thread baseline before its next claim. Exit-callback lists were
/// fully drained by the teardown, so only the flags need resetting.
pub fn reset_exit_state_for_retained_park() {
    debug_assert_eq!(ON_PROC_EXIT_INDEX.with(Cell::get), 0);
    debug_assert_eq!(ON_SHMEM_EXIT_INDEX.with(Cell::get), 0);
    debug_assert_eq!(BEFORE_SHMEM_EXIT_INDEX.with(Cell::get), 0);
    debug_assert!(!EXIT_CALLBACKS_DEFERRED.with(Cell::get));
    PROC_EXIT_INPROGRESS.with(|c| c.set(false));
    SHMEM_EXIT_INPROGRESS.with(|c| c.set(false));
    elog::config::set_proc_exit_inprogress(false);
    init_small::globals::SetInterruptHoldoffCount(0);
    init_small::globals::SetCritSectionCount(0);
    elog::config::set_crit_section_count(0);
    elog::reset_statement_suppressed();
}

pub fn proc_exit(code: i32, my_pid: i32) -> ! {
    // C's `MyProcPid != getpid()` guard, thread-model form.
    if my_pid != init_small::globals::MyProcPid() {
        panic!("proc_exit() called in child process");
    }

    commit_to_exit();

    // Exit-callback ordering vs the unwind: a deep FATAL (die() inside a
    // scan) unwinds through frames whose Drop glue releases resources via
    // the resource owner; ShutdownPostgres must therefore run AFTER those
    // drops — the ERROR-path order — or every guard drop hits a torn-down
    // owner (the cancelled-parallel-query server abort, resowner bad_owner).
    // C never unwinds, so its callbacks-at-raise order has no such
    // constraint. Child threads defer the drain to the thread top
    // (launch_backend::run_child_task); the postmaster thread and bare test
    // threads have no run_child_task above them and drain here.
    if init_small::globals::IsUnderPostmaster() {
        EXIT_CALLBACKS_DEFERRED.with(|c| c.set(true));
    } else {
        drain_exit_callbacks(code);
    }

    std::panic::resume_unwind(Box::new(ProcExitThread { code }));
}

/// True while this thread's proc_exit callback drain is still owed (set by
/// proc_exit under postmaster, consumed by run_deferred_exit_callbacks).
/// The session-memory teardown gate (launch_backend, FPBUDGET-1 v2) reads it
/// BEFORE the drain to tell a clean proc_exit apart from a quickdie-class
/// exit_thread_raw: both unwind ProcExitThread, but only proc_exit owes the
/// callback ceremony (ShutdownPostgres -> AbortOutOfAnyTransaction ->
/// portal cleanup). Estate teardown without that ceremony is what aborted
/// train-29 (a FAILED portal's drop deallocated into a freed context), so
/// crash-class exits must skip the drain like C's _exit(2) skips atexit.
pub fn exit_callbacks_pending() -> bool {
    EXIT_CALLBACKS_DEFERRED.with(Cell::get)
}

/// Thread-top half of proc_exit (run_child_task): if this thread's unwind
/// deferred the callback drain, run the exit stacks now — after every stack
/// frame's Drop glue, in C's callback order. exit_thread_raw (quickdie)
/// never defers, so the crash class stays drain-free. A proc_exit
/// re-entered from inside a callback (C's recursion arm) unwinds back into
/// the loop and the drain resumes where it left off with the new code —
/// last code wins, as in C. Returns the final exit code.
pub fn run_deferred_exit_callbacks(mut code: i32) -> i32 {
    loop {
        if !EXIT_CALLBACKS_DEFERRED.with(|c| c.replace(false)) {
            return code;
        }
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drain_exit_callbacks(code)
        }));
        match outcome {
            Ok(()) => return code,
            Err(payload) => match payload.downcast_ref::<ProcExitThread>() {
                Some(p) => code = p.code,
                None => std::panic::resume_unwind(payload),
            },
        }
    }
}

// _exit(code)'s thread rendering: end this backend thread without running the
// exit-callback stacks (quickdie/SignalHandlerForCrashExit contract). Stack
// Drop glue still runs, unlike C's _exit — a documented divergence
// (notes/crash-restart-design.md).
pub fn exit_thread_raw(code: i32) -> ! {
    PROC_EXIT_INPROGRESS.with(|c| c.set(true));
    elog::config::set_proc_exit_inprogress(true);
    std::panic::resume_unwind(Box::new(ProcExitThread { code }));
}

/// Unwind payload of a SIGKILL-style abrupt death (crash-test injection): the
/// joiner reports WTERMSIG(signo) instead of the generic SIGABRT crash class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KilledBySignal {
    pub signo: i32,
}

// kill -9's thread rendering: no exit callbacks, no client message; the
// postmaster reaps it as "terminated by signal {signo}" and runs the crash
// cycle. Stack Drop glue still runs (same divergence as exit_thread_raw).
pub fn exit_thread_killed(signo: i32) -> ! {
    PROC_EXIT_INPROGRESS.with(|c| c.set(true));
    elog::config::set_proc_exit_inprogress(true);
    std::panic::resume_unwind(Box::new(KilledBySignal { signo }));
}

fn commit_to_exit() {
    // Committed to exit: elog now promotes ERROR to FATAL.
    PROC_EXIT_INPROGRESS.with(|c| c.set(true));
    elog::config::set_proc_exit_inprogress(true);

    init_small::globals::SetInterruptPending(false);
    init_small::globals::SetProcDiePending(false);
    init_small::globals::SetQueryCancelPending(false);
    init_small::globals::SetInterruptHoldoffCount(1);
    init_small::globals::SetCritSectionCount(0);
    elog::config::set_crit_section_count(0);

    elog::clear_emit_context_callbacks();
    elog::suppress_statement();
}

fn drain_exit_callbacks(code: i32) {
    shmem_exit_internal(code);

    while ON_PROC_EXIT_INDEX.with(Cell::get) > 0 {
        let i = ON_PROC_EXIT_INDEX.with(Cell::get) - 1;
        ON_PROC_EXIT_INDEX.with(|c| c.set(i));
        let entry = ON_PROC_EXIT_LIST
            .with(|l| l.get()[i].expect("on_proc_exit slot below index is filled"));
        run_callback_guarded("on_proc_exit", || (entry.function)(code, entry.arg));
    }
}

// A panic escaping an exit callback is announced SIGABRT (cluster-wide
// crash-restart); degrade to WARNING and keep draining callbacks.
fn run_callback_guarded(what: &str, f: impl FnOnce()) {
    let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) else {
        return;
    };
    if payload.is::<ProcExitThread>() || payload.is::<types_error::PanicExitThread>() {
        std::panic::resume_unwind(payload);
    }
    let msg = payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
        .or_else(|| payload.downcast_ref::<PgError>().map(|e| e.message().to_string()))
        .unwrap_or_else(|| "unknown panic".to_string());
    let what = what.to_string();
    let _ = std::panic::catch_unwind(move || {
        let _ = elog::elog(
            types_error::WARNING,
            format!("{what} callback failed during exit: {msg}"),
        );
    });
}

pub fn shmem_exit(code: i32) -> PgResult<()> {
    shmem_exit_internal(code);
    Ok(())
}

fn shmem_exit_internal(code: i32) {
    SHMEM_EXIT_INPROGRESS.with(|c| c.set(true));

    lwlock::LWLockReleaseAll().expect("LWLockReleaseAll failed in shmem_exit");

    while BEFORE_SHMEM_EXIT_INDEX.with(Cell::get) > 0 {
        let i = BEFORE_SHMEM_EXIT_INDEX.with(Cell::get) - 1;
        BEFORE_SHMEM_EXIT_INDEX.with(|c| c.set(i));
        let entry = BEFORE_SHMEM_EXIT_LIST
            .with(|l| l.get()[i].expect("before_shmem_exit slot below index is filled"));
        run_callback_guarded("before_shmem_exit", || {
            if let Err(e) = (entry.function)(code, entry.arg) {
                rethrow_callback_error(*e);
            }
        });
    }

    // Explicit call, not an on_shmem_exit entry (C comment: progressive logic).
    if let Err(e) = dsm_core::dsm::dsm_backend_shutdown() {
        rethrow_callback_error(*e);
    }

    while ON_SHMEM_EXIT_INDEX.with(Cell::get) > 0 {
        let i = ON_SHMEM_EXIT_INDEX.with(Cell::get) - 1;
        ON_SHMEM_EXIT_INDEX.with(|c| c.set(i));
        let entry = ON_SHMEM_EXIT_LIST
            .with(|l| l.get()[i].expect("on_shmem_exit slot below index is filled"));
        run_callback_guarded("on_shmem_exit", || (entry.function)(code, entry.arg));
    }

    SHMEM_EXIT_INPROGRESS.with(|c| c.set(false));
}

// C's ereport-inside-exit-callback flow: errstart promotes to FATAL (flag),
// errfinish emits and re-enters proc_exit(1), finishing the remaining
// already-decremented callbacks before unwinding with code 1.
#[cold]
fn rethrow_callback_error(e: PgError) {
    let _ = elog::ThrowErrorData(e);
    // Outside proc_exit ThrowErrorData returns the ERROR (C longjmps).
    panic!("shmem_exit callback failed outside proc_exit");
}

#[cold]
fn out_of_slots(which: &str) -> ! {
    // FATAL never returns: errfinish exits the thread via the proc_exit seam.
    let _ = elog::ereport(FATAL)
        .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .errmsg_internal(format!("out of {which} slots"))
        .finish(ErrorLocation { filename: None, lineno: 0, funcname: None });
    unreachable!("ereport(FATAL) returned");
}

pub fn on_proc_exit(function: OnExitCallback, arg: usize) {
    let i = ON_PROC_EXIT_INDEX.with(Cell::get);
    if i >= MAX_ON_EXITS {
        out_of_slots("on_proc_exit");
    }
    ON_PROC_EXIT_LIST.with(|l| {
        let mut arr = l.get();
        arr[i] = Some(OnExit { function, arg });
        l.set(arr);
    });
    ON_PROC_EXIT_INDEX.with(|c| c.set(i + 1));
}

pub fn before_shmem_exit(function: BeforeShmemExitCallback, arg: Datum) -> PgResult<()> {
    let i = BEFORE_SHMEM_EXIT_INDEX.with(Cell::get);
    if i >= MAX_ON_EXITS {
        out_of_slots("before_shmem_exit");
    }
    BEFORE_SHMEM_EXIT_LIST.with(|l| {
        let mut arr = l.get();
        arr[i] = Some(BeforeOnExit { function, arg });
        l.set(arr);
    });
    BEFORE_SHMEM_EXIT_INDEX.with(|c| c.set(i + 1));
    Ok(())
}

pub fn on_shmem_exit(function: OnExitCallback, arg: usize) {
    let i = ON_SHMEM_EXIT_INDEX.with(Cell::get);
    if i >= MAX_ON_EXITS {
        out_of_slots("on_shmem_exit");
    }
    ON_SHMEM_EXIT_LIST.with(|l| {
        let mut arr = l.get();
        arr[i] = Some(OnExit { function, arg });
        l.set(arr);
    });
    ON_SHMEM_EXIT_INDEX.with(|c| c.set(i + 1));
}

pub fn cancel_before_shmem_exit(function: BeforeShmemExitCallback, arg: Datum) -> PgResult<()> {
    let i = BEFORE_SHMEM_EXIT_INDEX.with(Cell::get);
    let latest = (i > 0)
        .then(|| BEFORE_SHMEM_EXIT_LIST.with(|l| l.get()[i - 1]))
        .flatten();
    match latest {
        Some(e) if e.function as usize == function as usize && e.arg == arg => {
            BEFORE_SHMEM_EXIT_INDEX.with(|c| c.set(i - 1));
            Ok(())
        }
        _ => Err(Box::new(PgError::error(format!(
            "before_shmem_exit callback ({:#x},{:#x}) is not the latest entry",
            function as usize,
            arg.as_i32() as usize
        )))),
    }
}

pub fn on_exit_reset() {
    BEFORE_SHMEM_EXIT_INDEX.with(|c| c.set(0));
    ON_SHMEM_EXIT_INDEX.with(|c| c.set(0));
    ON_PROC_EXIT_INDEX.with(|c| c.set(0));
    dsm_core::dsm::reset_on_dsm_detach();
}

/// M4 bgjobs multi-job seat (docs/design/m4-bgjobs.md §3.2): one dispatcher
/// thread hosts several migrated daemons; each job's exit-callback stacks
/// (registered during its aux prelude, drained by its teardown shmem_exit)
/// are stashed here while the job is not the thread's bound identity.
/// Without the swap, teardown of job A would run job B's aux exit chain,
/// and job B's startup `on_exit_reset` would wipe job A's registrations.
pub struct ExitCallbackLists {
    before_shmem: [Option<BeforeOnExit>; MAX_ON_EXITS],
    on_shmem: [Option<OnExit>; MAX_ON_EXITS],
    on_proc: [Option<OnExit>; MAX_ON_EXITS],
    before_shmem_index: usize,
    on_shmem_index: usize,
    on_proc_index: usize,
}

impl ExitCallbackLists {
    pub const fn empty() -> ExitCallbackLists {
        ExitCallbackLists {
            before_shmem: [NO_BEFORE; MAX_ON_EXITS],
            on_shmem: [NO_ON; MAX_ON_EXITS],
            on_proc: [NO_ON; MAX_ON_EXITS],
            before_shmem_index: 0,
            on_shmem_index: 0,
            on_proc_index: 0,
        }
    }
}

impl Default for ExitCallbackLists {
    fn default() -> Self {
        ExitCallbackLists::empty()
    }
}

/// Exchange the calling thread's exit-callback stacks with `saved`.
/// Self-inverse (the seat swap discipline). Must not be called while an
/// exit is in progress on this thread (the drain loops walk the live TLS).
pub fn swap_exit_callback_lists(saved: &mut ExitCallbackLists) {
    debug_assert!(
        !PROC_EXIT_INPROGRESS.with(Cell::get) && !SHMEM_EXIT_INPROGRESS.with(Cell::get),
        "exit-callback seat swap during an in-progress exit drain"
    );
    BEFORE_SHMEM_EXIT_LIST.with(|l| {
        let cur = l.get();
        l.set(saved.before_shmem);
        saved.before_shmem = cur;
    });
    ON_SHMEM_EXIT_LIST.with(|l| {
        let cur = l.get();
        l.set(saved.on_shmem);
        saved.on_shmem = cur;
    });
    ON_PROC_EXIT_LIST.with(|l| {
        let cur = l.get();
        l.set(saved.on_proc);
        saved.on_proc = cur;
    });
    let cur = BEFORE_SHMEM_EXIT_INDEX.with(Cell::get);
    BEFORE_SHMEM_EXIT_INDEX.with(|c| c.set(saved.before_shmem_index));
    saved.before_shmem_index = cur;
    let cur = ON_SHMEM_EXIT_INDEX.with(Cell::get);
    ON_SHMEM_EXIT_INDEX.with(|c| c.set(saved.on_shmem_index));
    saved.on_shmem_index = cur;
    let cur = ON_PROC_EXIT_INDEX.with(Cell::get);
    ON_PROC_EXIT_INDEX.with(|c| c.set(saved.on_proc_index));
    saved.on_proc_index = cur;
}

pub fn check_on_shmem_exit_lists_are_empty() -> PgResult<()> {
    if BEFORE_SHMEM_EXIT_INDEX.with(Cell::get) != 0 {
        return Err(Box::new(PgError::error(
            "before_shmem_exit has been called prematurely",
        )));
    }
    if ON_SHMEM_EXIT_INDEX.with(Cell::get) != 0 {
        return Err(Box::new(PgError::error(
            "on_shmem_exit has been called prematurely",
        )));
    }
    Ok(())
}

pub fn init_seams() {
    ipc_seams::proc_exit::set(proc_exit);
    ipc_seams::before_shmem_exit::set(before_shmem_exit);
    ipc_seams::on_shmem_exit::set(on_shmem_exit);
    ipc_seams::on_proc_exit::set(on_proc_exit);
    ipc_seams::check_on_shmem_exit_lists_are_empty::set(check_on_shmem_exit_lists_are_empty);
    ipc_portal_seams::shmem_exit_inprogress::set(shmem_exit_inprogress);
}
