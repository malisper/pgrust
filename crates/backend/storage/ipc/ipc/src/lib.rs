//! ipc.c: exit-time cleanup — the proc_exit/shmem_exit callback stacks.
//! Thread model: one backend = one thread, so the three callback arrays and
//! both in-progress flags are per-thread TLS, and proc_exit ends the backend
//! THREAD, never the process — after the callbacks it unwinds with a
//! [`ProcExitThread`] payload that the postmaster-side reaper downcasts at
//! join (design: notes/ipc-proc-exit-threads.md). C's atexit backstop has no
//! per-thread analogue: a backend thread must never call process exit(); only
//! postmaster paths may end the process.

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

/// The unwind payload of a normal backend-thread exit; the joiner recovers
/// the C exit code via `downcast_ref::<ProcExitThread>()`. Any other panic
/// payload reaching the thread top is a backend crash.
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

pub fn proc_exit(code: i32, my_pid: i32) -> ! {
    // C guards `MyProcPid != getpid()` (fork by system()); the thread-model
    // hazard is a callable migrating to a thread that isn't its backend.
    if my_pid != init_small::globals::MyProcPid() {
        panic!("proc_exit() called in child process");
    }

    proc_exit_prepare(code);

    std::panic::resume_unwind(Box::new(ProcExitThread { code }));
}

fn proc_exit_prepare(code: i32) {
    // Committed to exit: ereport(ERROR) now promotes to FATAL (elog reads
    // this flag) and lands back here instead of the idle loop.
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
        (entry.function)(code, entry.arg);
    }
}

pub fn shmem_exit(code: i32) -> PgResult<()> {
    shmem_exit_internal(code);
    Ok(())
}

fn shmem_exit_internal(code: i32) {
    SHMEM_EXIT_INPROGRESS.with(|c| c.set(true));

    // Release LWLocks before callbacks run (they may acquire new ones).
    lwlock::LWLockReleaseAll().expect("LWLockReleaseAll failed in shmem_exit");

    while BEFORE_SHMEM_EXIT_INDEX.with(Cell::get) > 0 {
        let i = BEFORE_SHMEM_EXIT_INDEX.with(Cell::get) - 1;
        BEFORE_SHMEM_EXIT_INDEX.with(|c| c.set(i));
        let entry = BEFORE_SHMEM_EXIT_LIST
            .with(|l| l.get()[i].expect("before_shmem_exit slot below index is filled"));
        if let Err(e) = (entry.function)(code, entry.arg) {
            rethrow_callback_error(*e);
        }
    }

    // Explicit call, not an on_shmem_exit entry: dsm's own progressive logic
    // must keep running the remaining dsm callbacks after one errors.
    if let Err(e) = dsm_core::dsm::dsm_backend_shutdown() {
        rethrow_callback_error(*e);
    }

    while ON_SHMEM_EXIT_INDEX.with(Cell::get) > 0 {
        let i = ON_SHMEM_EXIT_INDEX.with(Cell::get) - 1;
        ON_SHMEM_EXIT_INDEX.with(|c| c.set(i));
        let entry = ON_SHMEM_EXIT_LIST
            .with(|l| l.get()[i].expect("on_shmem_exit slot below index is filled"));
        (entry.function)(code, entry.arg);
    }

    SHMEM_EXIT_INPROGRESS.with(|c| c.set(false));
}

// The C control flow for an ereport inside an exit callback: errstart sees
// proc_exit_inprogress, promotes ERROR to FATAL, errfinish emits and calls
// proc_exit(1) — which re-enters here and finishes the remaining (already
// decremented) callbacks before unwinding with code 1.
#[cold]
fn rethrow_callback_error(e: PgError) {
    let _ = elog::ThrowErrorData(e);
    // Not in proc_exit (postmaster shmem_exit): ThrowErrorData returned the
    // ERROR instead of exiting; C would longjmp to the caller's handler.
    panic!("shmem_exit callback failed outside proc_exit");
}

#[cold]
fn out_of_slots(which: &str) -> ! {
    // ereport(FATAL, ERRCODE_PROGRAM_LIMIT_EXCEEDED); FATAL never returns
    // (errfinish exits the thread through the proc_exit seam).
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
