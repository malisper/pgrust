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

/// Retention park (wretain): proc_exit_prepare committed this thread to
/// "exiting" (exit flags, ERROR->FATAL promotion, held interrupts, suppressed
/// statement logging); a parked standby lives on and must return to the
/// fresh-thread baseline before its next claim. Exit-callback lists were
/// fully drained by the teardown, so only the flags need resetting.
pub fn reset_exit_state_for_retained_park() {
    debug_assert_eq!(ON_PROC_EXIT_INDEX.with(Cell::get), 0);
    debug_assert_eq!(ON_SHMEM_EXIT_INDEX.with(Cell::get), 0);
    debug_assert_eq!(BEFORE_SHMEM_EXIT_INDEX.with(Cell::get), 0);
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

    proc_exit_prepare(code);

    std::panic::resume_unwind(Box::new(ProcExitThread { code }));
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

fn proc_exit_prepare(code: i32) {
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
