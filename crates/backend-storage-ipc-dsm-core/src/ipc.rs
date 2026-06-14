//! `storage/ipc/ipc.c` — exit-time cleanup for a postmaster or backend:
//! `proc_exit`/`shmem_exit` and the on-exit callback lists.
//!
//! `proc_exit_inprogress` itself lives in `backend-utils-error`'s config
//! (the elog port owns the ERROR->FATAL promotion that reads it); this
//! module sets it via that crate's setter. `shmem_exit_inprogress` is owned
//! here.

use std::cell::{Cell, RefCell};

use backend_utils_error::{config, elog, ereport};
// The exit-callback `arg` is the canonical unified `Datum` (Datum-unification);
// the `on_proc_exit`/`on_shmem_exit`/`before_shmem_exit` seam contract carries
// `types_tuple::Datum<'static>`. It is the machine word the C `Datum arg`
// holds, stored by value in the registration list for the process lifetime.
use types_tuple::Datum;
use types_error::{
    ErrorLocation, PgResult, DEBUG3, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR, FATAL, PANIC,
};

use crate::dsm;

fn loc(funcname: &str) -> ErrorLocation {
    ErrorLocation::new("ipc.c", 0, funcname)
}

/// `pg_on_exit_callback` (`storage/ipc.h`): callbacks take the exit code and
/// the Datum supplied at registration. They may `ereport(ERROR/FATAL)`,
/// hence the `PgResult` (the C longjmp surface).
pub type PgOnExitCallback = fn(code: i32, arg: Datum<'static>) -> PgResult<()>;

/// `MAX_ON_EXITS`.
const MAX_ON_EXITS: usize = 20;

/// `struct ONEXIT`.
///
/// `arg` is the canonical unified `Datum<'static>`, which is not `Copy` (it has
/// a `ByRef` arm), so `OnExit` is `Clone` rather than `Copy`. The exit-callback
/// arg in practice is always a registered machine word (the `ByVal` arm), as in
/// C's `uintptr_t`-valued `Datum`.
#[derive(Clone)]
struct OnExit {
    function: PgOnExitCallback,
    arg: Datum<'static>,
}

/// One of the three fixed-size callback lists (`on_proc_exit_list` etc. with
/// their `*_index` counters). Fixed arrays as in C — registration has no
/// allocation (and no OOM path).
struct OnExitList {
    items: [Option<OnExit>; MAX_ON_EXITS],
    index: usize,
}

impl OnExitList {
    const fn new() -> Self {
        // `OnExit` is not `Copy` (its `Datum<'static>` arg has a `ByRef` arm),
        // so the array-repeat operand must be a named const, not an inline
        // `None`, to build the fixed-size list in const context.
        const NONE: Option<OnExit> = None;
        Self {
            items: [NONE; MAX_ON_EXITS],
            index: 0,
        }
    }
}

thread_local! {
    /// `shmem_exit_inprogress`.
    static SHMEM_EXIT_INPROGRESS: Cell<bool> = const { Cell::new(false) };
    /// `atexit_callback_setup`.
    static ATEXIT_CALLBACK_SETUP: Cell<bool> = const { Cell::new(false) };
    static ON_PROC_EXIT_LIST: RefCell<OnExitList> = const { RefCell::new(OnExitList::new()) };
    static ON_SHMEM_EXIT_LIST: RefCell<OnExitList> = const { RefCell::new(OnExitList::new()) };
    static BEFORE_SHMEM_EXIT_LIST: RefCell<OnExitList> =
        const { RefCell::new(OnExitList::new()) };
}

/// `shmem_exit_inprogress` — true while [`shmem_exit`] runs.
pub fn shmem_exit_inprogress() -> bool {
    SHMEM_EXIT_INPROGRESS.with(|c| c.get())
}

/// Pop the most recently registered callback (the C `--index` walk).
fn pop_callback(list: &'static std::thread::LocalKey<RefCell<OnExitList>>) -> Option<OnExit> {
    list.with(|cell| {
        let mut list = cell.borrow_mut();
        if list.index == 0 {
            None
        } else {
            list.index -= 1;
            let index = list.index;
            list.items[index].take()
        }
    })
}

/// `proc_exit(int code)` — run all the registered callbacks and then exit.
/// This should be the only function that calls `exit()`. `my_pid` is the
/// caller's `MyProcPid` (globals.c), passed explicitly per the
/// no-ambient-global rule; the caller reads it off its own state when the
/// miscinit owner lands.
pub fn proc_exit(code: i32, my_pid: i32) -> ! {
    // Not safe if forked by system(), etc.
    if my_pid != unsafe { libc::getpid() } as i32 {
        // PANIC aborts the process inside the report cycle.
        let _ = elog(PANIC, "proc_exit() called in child process");
    }

    // Clean up everything that must be cleaned up.
    proc_exit_prepare(code);

    // The PROFILE_PID_DIR gprof block is not ported (profiling build only).

    let _ = elog(DEBUG3, format!("exit({code})"));

    std::process::exit(code)
}

/// `proc_exit_prepare(int code)` — code shared between [`proc_exit`] and the
/// atexit handler; on a normal `proc_exit` it actually runs twice, the
/// second time with nothing to do.
fn proc_exit_prepare(code: i32) {
    // Once this flag is set we are committed to exit: ereport() will NOT
    // return control to the main loop (ERROR is promoted to FATAL), so an
    // error in an on_proc_exit routine comes right back here. Because of
    // that promotion, the callbacks' Err results below cannot actually
    // surface; they are dropped rather than propagated.
    config::set_proc_exit_inprogress(true);

    // Forget any pending cancel or die requests; we're closing up shop. The
    // signal handlers won't set these again now that proc_exit_inprogress
    // is set.
    backend_utils_init_small_seams::set_interrupt_pending::call(false);
    backend_utils_init_small_seams::set_proc_die_pending::call(false);
    backend_utils_init_small_seams::set_query_cancel_pending::call(false);
    backend_utils_init_small_seams::set_interrupt_holdoff_count::call(1);
    config::set_crit_section_count(0);

    // (C clears error_context_stack here; that chain is retired in favor of
    // attach-on-propagation, so there is no ambient state to clear.)
    // Reset debug_query_string before it's clobbered.
    backend_tcop_postgres_seams::reset_debug_query_string::call();

    // Do our shared memory exits first.
    let _ = shmem_exit(code);

    let n = ON_PROC_EXIT_LIST.with(|cell| cell.borrow().index);
    let _ = elog(
        DEBUG3,
        format!("proc_exit({code}): {n} callbacks to make"),
    );

    // Call all the registered callbacks, decrementing the index each time so
    // an erroring callback isn't invoked again when control comes back here
    // — no infinite loop is possible.
    while let Some(cb) = pop_callback(&ON_PROC_EXIT_LIST) {
        let _ = (cb.function)(code, cb.arg);
    }

    ON_PROC_EXIT_LIST.with(|cell| cell.borrow_mut().index = 0);
}

/// `shmem_exit(int code)` — run all of the on_shmem_exit routines but don't
/// actually exit. Used by the postmaster to re-initialize shared memory and
/// semaphores after a backend dies horribly.
///
/// The `Err` surface is a `before_shmem_exit`/`on_shmem_exit`/DSM-detach
/// callback raising ERROR (the C longjmp out of this function); each
/// callback is unregistered before being invoked, so re-entry resumes with
/// the remaining ones.
pub fn shmem_exit(code: i32) -> PgResult<()> {
    SHMEM_EXIT_INPROGRESS.with(|c| c.set(true));

    // Release any LWLocks we might be holding before callbacks run: this
    // prevents accessing locks in detached DSM segments and lets callbacks
    // acquire new locks. (Infallible in C; the Rust surface's only Err is an
    // internal held-lock-table invariant, folded into this function's
    // callback-error surface.)
    backend_storage_lmgr_lwlock::LWLockReleaseAll()?;

    // Call before_shmem_exit callbacks: things that need most of the system
    // still up, such as cleanup of temp relations (catalog access).
    let n = BEFORE_SHMEM_EXIT_LIST.with(|cell| cell.borrow().index);
    elog(
        DEBUG3,
        format!("shmem_exit({code}): {n} before_shmem_exit callbacks to make"),
    )?;
    while let Some(cb) = pop_callback(&BEFORE_SHMEM_EXIT_LIST) {
        (cb.function)(code, cb.arg)?;
    }
    BEFORE_SHMEM_EXIT_LIST.with(|cell| cell.borrow_mut().index = 0);

    // Call dynamic shared memory callbacks. Hard-coding this call (rather
    // than registering it as an on_shmem_exit callback) puts DSM segments on
    // an equal footing with the main segment: dsm_backend_shutdown
    // unregisters each callback before invoking it, so an erroring callback
    // doesn't stop the remaining ones from eventually running.
    dsm::dsm_backend_shutdown()?;

    // Call on_shmem_exit callbacks: generally, releasing low-level shared
    // memory resources; partly a backstop in case the early callbacks fail
    // and re-enter this routine.
    let n = ON_SHMEM_EXIT_LIST.with(|cell| cell.borrow().index);
    elog(
        DEBUG3,
        format!("shmem_exit({code}): {n} on_shmem_exit callbacks to make"),
    )?;
    while let Some(cb) = pop_callback(&ON_SHMEM_EXIT_LIST) {
        (cb.function)(code, cb.arg)?;
    }
    ON_SHMEM_EXIT_LIST.with(|cell| cell.borrow_mut().index = 0);

    SHMEM_EXIT_INPROGRESS.with(|c| c.set(false));
    Ok(())
}

/// `atexit_callback` — backstop so direct calls of `exit()` don't skip
/// cleanup. (For a really uncooperative `_exit()` there's the pmsignal.c
/// dead man switch.)
extern "C" fn atexit_callback() {
    // Clean up everything that must be cleaned up.
    // ... too bad we don't know the real exit code ...
    proc_exit_prepare(-1);
}

fn setup_atexit_callback() {
    if !ATEXIT_CALLBACK_SETUP.with(|c| c.get()) {
        unsafe {
            libc::atexit(atexit_callback);
        }
        ATEXIT_CALLBACK_SETUP.with(|c| c.set(true));
    }
}

fn register(
    list: &'static std::thread::LocalKey<RefCell<OnExitList>>,
    function: PgOnExitCallback,
    arg: Datum<'static>,
    overflow_msg: &str,
    funcname: &str,
) -> PgResult<()> {
    let full = list.with(|cell| cell.borrow().index >= MAX_ON_EXITS);
    if full {
        ereport(FATAL)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg_internal(overflow_msg)
            .finish(loc(funcname))?;
    }

    list.with(|cell| {
        let mut list = cell.borrow_mut();
        let index = list.index;
        list.items[index] = Some(OnExit { function, arg });
        list.index += 1;
    });

    setup_atexit_callback();
    Ok(())
}

/// `on_proc_exit(pg_on_exit_callback function, Datum arg)` — add a callback
/// to the list invoked by [`proc_exit`].
pub fn on_proc_exit(function: PgOnExitCallback, arg: Datum<'static>) -> PgResult<()> {
    register(
        &ON_PROC_EXIT_LIST,
        function,
        arg,
        "out of on_proc_exit slots",
        "on_proc_exit",
    )
}

/// `before_shmem_exit(pg_on_exit_callback function, Datum arg)` — register an
/// early callback for user-level cleanup (e.g. transaction abort) before
/// low-level subsystems shut down.
pub fn before_shmem_exit(function: PgOnExitCallback, arg: Datum<'static>) -> PgResult<()> {
    register(
        &BEFORE_SHMEM_EXIT_LIST,
        function,
        arg,
        "out of before_shmem_exit slots",
        "before_shmem_exit",
    )
}

/// `on_shmem_exit(pg_on_exit_callback function, Datum arg)` — register an
/// ordinary callback for low-level shutdown (e.g. releasing our PGPROC);
/// runs after before_shmem_exit callbacks and before on_proc_exit ones.
pub fn on_shmem_exit(function: PgOnExitCallback, arg: Datum<'static>) -> PgResult<()> {
    register(
        &ON_SHMEM_EXIT_LIST,
        function,
        arg,
        "out of on_shmem_exit slots",
        "on_shmem_exit",
    )
}

/// `cancel_before_shmem_exit(pg_on_exit_callback function, Datum arg)` —
/// remove a previously-registered before_shmem_exit callback. Only the
/// latest entry is considered: callers are expected to add and remove
/// temporary callbacks in strict LIFO order.
pub fn cancel_before_shmem_exit(function: PgOnExitCallback, arg: Datum<'static>) -> PgResult<()> {
    let removed = BEFORE_SHMEM_EXIT_LIST.with(|cell| {
        let mut list = cell.borrow_mut();
        if list.index > 0 {
            let last = list.items[list.index - 1].as_ref();
            if let Some(cb) = last {
                if cb.function as usize == function as usize && cb.arg == arg {
                    list.index -= 1;
                    let index = list.index;
                    list.items[index] = None;
                    return true;
                }
            }
        }
        false
    });
    if !removed {
        elog(
            ERROR,
            format!(
                "before_shmem_exit callback ({:#x},{:#x}) is not the latest entry",
                function as usize,
                arg.as_usize()
            ),
        )?;
    }
    Ok(())
}

/// `on_exit_reset` — clear all registered exit callbacks; used just after
/// forking a backend so it doesn't run the postmaster's on-exit routines
/// when it exits.
pub fn on_exit_reset() {
    BEFORE_SHMEM_EXIT_LIST.with(|cell| {
        let mut list = cell.borrow_mut();
        list.items = std::array::from_fn(|_| None);
        list.index = 0;
    });
    ON_SHMEM_EXIT_LIST.with(|cell| {
        let mut list = cell.borrow_mut();
        list.items = std::array::from_fn(|_| None);
        list.index = 0;
    });
    ON_PROC_EXIT_LIST.with(|cell| {
        let mut list = cell.borrow_mut();
        list.items = std::array::from_fn(|_| None);
        list.index = 0;
    });
    dsm::reset_on_dsm_detach();
}

/// `check_on_shmem_exit_lists_are_empty` — debugging check that no shmem
/// cleanup handlers have been registered prematurely in this process.
pub fn check_on_shmem_exit_lists_are_empty() -> PgResult<()> {
    if BEFORE_SHMEM_EXIT_LIST.with(|cell| cell.borrow().index) != 0 {
        elog(FATAL, "before_shmem_exit has been called prematurely")?;
    }
    if ON_SHMEM_EXIT_LIST.with(|cell| cell.borrow().index) != 0 {
        elog(FATAL, "on_shmem_exit has been called prematurely")?;
    }
    // Checking DSM detach state seems unnecessary given the above.
    Ok(())
}
