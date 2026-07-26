//! PostgresSingleUserMain (postgres.c:4055): the --single entry. No
//! postmaster, no auxiliary threads, exactly one session; stdin/stdout
//! protocol (InteractiveBackend + DestDebug live in main_loop/dest). The C
//! call order below is the deliverable — do not reorder.
//!
//! Thread-model exit frame: C's proc_exit _exit(2)s the process; here
//! proc_exit drains the exit-callback stacks inline (ipc::proc_exit's
//! !IsUnderPostmaster arm — that drain includes InitPostgres's
//! shutdown_xlog_cb, i.e. the shutdown checkpoint) and unwinds
//! ProcExitThread; the frame in PostgresSingleUserMain catches it and
//! std::process::exit()s with the C exit code.

use ::elog::ereport;
use ::types_error::{PgResult, ERRCODE_INVALID_PARAMETER_VALUE, FATAL};
use ::types_guc::GucContext;

use crate::{loc, switches};

const PROGNAME: &str = "postgres";

pub fn PostgresSingleUserMain(argv: &[String], username: &str) -> ! {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || -> core::convert::Infallible {
        let err = match single_user_main_inner(argv, username) {
            Ok(never) => match never {},
            Err(err) => err,
        };
        // A FATAL that unwound as Err (rather than ereport-finish): report
        // it, then take C's proc_exit(1).
        elog::emit_error_report_for(&err);
        ipc_seams::proc_exit::call(1, init_small::globals::MyProcPid())
    },
    ));
    let payload = match outcome {
        Ok(never) => match never {},
        Err(payload) => payload,
    };
    match payload.downcast_ref::<ipc::ProcExitThread>() {
        // C main() returns proc_exit's status; exit callbacks already ran
        // (ipc::proc_exit drains inline when !IsUnderPostmaster).
        Some(p) => std::process::exit(p.code),
        // Anything else is a crash; let it abort the process as a panic.
        None => std::panic::resume_unwind(payload),
    }
}

// Never returns Ok: the tail call is PostgresMain (-> !); every early exit is
// a FATAL ereport (which proc_exits via unwind) or an Err.
fn single_user_main_inner(argv: &[String], username: &str) -> PgResult<core::convert::Infallible> {
    assert!(!init_small::globals::IsUnderPostmaster());

    /* Initialize startup process environment. */
    miscinit::InitStandaloneProcess(argv.first().map(|s| s.as_str()).unwrap_or(PROGNAME))?;

    /* Set default values for command-line options. */
    guc_seams::initialize_guc_options::call()?;
    // Hoisted from C's InitializeGUCOptionsFromEnvironment, as PostmasterMain
    // does (postmaster/main_entry.rs convention).
    stack_depth::adjust_max_stack_depth_from_rlimit()?;

    /* Parse command-line options. */
    let mut dbname: Option<String> = None;
    switches::process_postgres_switches_dbname(
        argv,
        GucContext::PGC_POSTMASTER as i32 as u8,
        &mut dbname,
    )?;

    /* Must have gotten a database name, or have a default (the username) */
    let dbname = match dbname {
        Some(d) => d,
        None if !username.is_empty() => username.to_string(),
        None => {
            return Err(ereport(FATAL)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("{PROGNAME}: no database nor user name specified"))
                .into_error()
                .with_error_location(loc(4083, "PostgresSingleUserMain"))
                .into());
        }
    };

    /* Acquire configuration parameters */
    if !guc_seams::select_config_files::call(switches::user_d_option().as_deref(), PROGNAME)? {
        ipc_seams::proc_exit::call(1, init_small::globals::MyProcPid());
    }

    /*
     * Validate we have been given a reasonable-looking DataDir and change
     * into it.
     */
    miscinit_seams::check_data_dir::call()?;
    miscinit::ChangeToDataDir()?;

    /* Create lockfile for data directory. */
    miscinit::CreateDataDirLockFile(false)?;

    /* read control file (error checking and contains config ) */
    transam_xlog::LocalProcessControlFile(false)?;

    /* process any libraries that should be preloaded at postmaster start */
    miscinit_seams::process_shared_preload_libraries::call()?;

    /* Initialize MaxBackends */
    postinit::InitializeMaxBackends()?;

    /*
     * We don't need postmaster child slots in single-user mode, but
     * initialize them anyway to avoid having special handling.
     */
    pmchild_seams::init_postmaster_child_slots::call();

    /* Initialize size of fast-path lock cache. */
    let fastpath_groups = postinit::InitializeFastPathLocks();

    /* Give preloaded libraries a chance to request additional shared memory. */
    miscinit_seams::process_shmem_requests::call()?;

    /*
     * Now that loadable modules have had their chance to request additional
     * shared memory, determine the value of any runtime-computed GUCs that
     * depend on the amount of shared memory required.
     */
    ipci_seams::initialize_shmem_gucs::call(fastpath_groups)?;

    /*
     * Now that modules have been loaded, we can process any custom resource
     * managers specified in the wal_consistency_checking GUC.
     */
    transam_xlog_seams::initialize_wal_consistency_checking::call()?;

    /*
     * Create shared memory etc.  (Nothing's really "shared" in single-user
     * mode, but we must have these data structures anyway.)
     */
    ipci_seams::create_shared_memory_and_semaphores::call(fastpath_groups)?;

    /*
     * Estimate number of openable files.  This must happen after setting up
     * semaphores, because on some platforms semaphores count as open files.
     */
    // One process, one thread of execution: the whole descriptor budget is
    // this thread's (C's per-backend arithmetic, unshared).
    fd::set_max_safe_fds(1, 1)?;

    /*
     * Remember stand-alone backend startup time, roughly at the same point
     * during startup that postmaster does so.
     */
    postmaster_seams::set_pg_start_time::call(timestamp_seams::get_current_timestamp::call());

    /*
     * Create a per-backend PGPROC struct in shared memory. We must do this
     * before we can use LWLocks.
     */
    lmgr_proc::InitProcess(miscinit::GetMyBackendType())?;

    // Real-signal bridge + C's PostgresMain-time unblock: PostgresMain does
    // sigprocmask(SIG_SETMASK, &UnBlockSig) after installing handlers
    // (postgres.c:4276). Under the thread model, dispositions are the
    // pqsignal_thread table drained at interrupt points; the process-level
    // handlers here only pend the bit (SendThreadSignal) so a real SIGTERM/
    // SIGINT reaches die()/StatementCancelHandler at the next drain — and
    // interrupts a blocked stdin read (no SA_RESTART), which is exactly C's
    // die()-during-getc single-user hack.
    install_single_user_signal_bridge();
    libpq_pqsignal::unblock_signals();

    /*
     * Now that sufficient infrastructure has been initialized, PostgresMain()
     * can do the rest.
     */
    crate::PostgresMain(&dbname, username)
}

#[cfg(not(target_family = "wasm"))]
extern "C" fn bridge_signal_handler(signo: i32) {
    let saved = get_errno();
    // Pends this thread's disposition bit; drained at the next interrupt
    // point (ProcessClientReadInterrupt / CHECK_FOR_INTERRUPTS). Before
    // ProcSignalInit registers the slot this is ESRCH — DROPPED. That is a
    // known parity gap vs C, which pends via PostgresMain's pqsignal-installed
    // handlers (just before the postgres.c:4276 unblock): our unhandled
    // window spans InitPostgres
    // (including StartupXLOG crash recovery), where C would honor SIGTERM at
    // the next CFI. Ledgered in notes/wasm-p5-single-mode-worklog.md.
    //
    // NOT fully async-signal-safe: SendThreadSignal spin-acquires the slot's
    // pss_mutex, and the SetLatch path can take the waiter slot's Mutex +
    // Condvar. A signal landing while this (single) thread holds one of
    // those locks (a latch park/unpark transition, ProcSignalInit/Release)
    // would deadlock. Tolerated for now because the dominant delivery point
    // — a blocked stdin read in interactive_getc — holds no locks; the
    // proper fix is a sig_atomic staging flag drained at interrupt points.
    let _ = procsignal::SendThreadSignal(init_small::globals::MyProcPid(), signo);
    set_errno(saved);
}

// wasm32: WASI p1 delivers no signals — there is nothing to bridge. The
// thread-signal dispositions installed by PostgresMain still work for
// self-pended interrupts (statement_timeout, die() via SendThreadSignal);
// an operator Ctrl-C simply kills the wasmtime process, like SIGKILL would
// a native --single (the shutdown checkpoint is then skipped — crash
// recovery on next boot, C's own no-graceful-signal story).
#[cfg(target_family = "wasm")]
pub(crate) fn install_single_user_signal_bridge() {}

#[cfg(not(target_family = "wasm"))]
pub(crate) fn install_single_user_signal_bridge() {
    for signo in [libc::SIGHUP, libc::SIGINT, libc::SIGTERM, libc::SIGQUIT] {
        // SAFETY: standard sigaction install; the handler restores errno.
        // (It is not strictly async-signal-safe — see bridge_signal_handler.)
        unsafe {
            let mut sa: libc::sigaction = std::mem::zeroed();
            sa.sa_sigaction = bridge_signal_handler as *const () as usize;
            libc::sigfillset(&mut sa.sa_mask);
            sa.sa_flags = 0; /* no SA_RESTART: a blocked stdin read must EINTR */
            libc::sigaction(signo, &sa, std::ptr::null_mut());
        }
    }
    // SAFETY: SIG_IGN install, no handler code runs.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
}

#[cfg(not(target_family = "wasm"))]
fn get_errno() -> i32 {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    // SAFETY: __error returns this thread's valid errno location.
    unsafe {
        *libc::__error()
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    // SAFETY: __errno_location returns this thread's valid errno location.
    unsafe {
        *libc::__errno_location()
    }
}

#[cfg(not(target_family = "wasm"))]
fn set_errno(value: i32) {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    // SAFETY: __error returns this thread's valid errno location.
    unsafe {
        *libc::__error() = value;
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    // SAFETY: __errno_location returns this thread's valid errno location.
    unsafe {
        *libc::__errno_location() = value;
    }
}
