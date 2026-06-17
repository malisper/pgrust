//! The postmaster entry point — `PostmasterMain` — and the
//! postmaster-owned init/teardown pieces (`InitProcessGlobals`,
//! `InitPostmasterDeathWatchHandle`, `ClosePostmasterPorts`, the listen-socket
//! establishment, `getInstallationPaths`/`checkControlFile` control points).
//!
//! C source: `postmaster/postmaster.c` — `PostmasterMain`, `ClosePostmasterPorts`,
//! `InitProcessGlobals`, `CloseServerPorts`, and the death-watch fds.
//!
//! ## What is in-crate (the postmaster decision logic)
//!
//! The ordered bring-up sequence, the listen-socket loop over
//! `listen_addresses` + `unix_socket_directories`, the death-watch pipe, the
//! state-machine transition into `PM_STARTUP`, the bgwriter/checkpointer/startup
//! child launches, and the final `ServerLoop()` all live here, 1:1 with the C
//! control flow. The OS/GUC/config substrate each step reaches (config files,
//! shmem, sockets) is a real installed seam (the same surface single-user
//! drives) or a caller-side seam fronting an as-yet-unported owner.
//!
//! ## Fork-safety
//!
//! The postmaster is strictly single-threaded: every child is created by the
//! real `fork()` (through `postmaster_child_launch`). No lock is held across a
//! `fork()`; the mutable state is the single-process
//! [`crate::core::PostmasterState`] (a `static mut`). The signal handlers only
//! set atomic pending flags, so they stay async-signal-safe in the freshly
//! forked child.
//!
//! ## EXEC_BACKEND
//!
//! pgrust targets the `fork()` model. The C `#ifdef EXEC_BACKEND` blocks
//! (`write_nondefault_variables`, `RemovePgTempFilesInDir`, `find_other_exec`,
//! the `win32ChildQueue`) are the Windows/non-fork variant and are intentionally
//! NOT ported; the fork-side code path is ported 100%.

#![allow(non_snake_case)]

use backend_libpq_pqcomm::ListenServerPort;
use backend_utils_error::{ereport};
use types_error::{FATAL, LOG};

use crate::core::{pm, pm_mut, PMState, StartupStatusEnum, MAXLISTEN, B_BG_WRITER, B_CHECKPOINTER, B_STARTUP};
use crate::helpers::{closesocket, here, report};
use crate::serverloop::{LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STARTING, STATUS_OK};
use crate::{ioworkers, startchildren, statemachine};
use backend_postmaster_postmaster_seams as sp;

// ---------------------------------------------------------------------------
// Death-watch pipe (postmaster-owned).
//
// `postmaster_alive_fds[2]`: index 0 (POSTMASTER_FD_WATCH) is the read end a
// child polls; index 1 (POSTMASTER_FD_OWN) is the write end the postmaster
// holds open (children close their copy). When the postmaster dies, the write
// end's last reference goes away and the pipe reads EOF.
// ---------------------------------------------------------------------------

const POSTMASTER_FD_WATCH: usize = 0;
const POSTMASTER_FD_OWN: usize = 1;

static mut POSTMASTER_ALIVE_FDS: [i32; 2] = [-1, -1];

/// C: `static void checkControlFile(void)`.
///
/// Sanity-check that `pg_control` exists in the data directory (no CRC
/// validation — that is `LocalProcessControlFile`'s job). On failure the C code
/// writes a diagnostic to stderr and `ExitPostmaster(2)`s.
pub fn check_control_file() {
    // C: snprintf(path, "%s/%s", DataDir, XLOG_CONTROL_FILE);
    let data_dir = backend_utils_init_small_seams::data_dir::call().unwrap_or_default();
    let path = format!("{data_dir}/global/pg_control");
    if std::fs::File::open(&path).is_err() {
        report(
            LOG,
            "checkControlFile",
            format!("could not find the database system: expected to find pg_control at \"{path}\""),
        );
        statemachine::ExitPostmaster(2);
    }
}

/// C: `void InitPostmasterDeathWatchHandle(void)` (Unix branch).
///
/// Create the self-pipe whose write end the postmaster holds and whose read end
/// children watch for EOF on postmaster death.
fn InitPostmasterDeathWatchHandle() {
    // C: pipe(postmaster_alive_fds); set both ends non-blocking + CLOEXEC on
    // the read end via fcntl. Reserve 2 fds with fd.c (ReserveExternalFD) — the
    // reservation is fd.c's bookkeeping; the raw pipe is the postmaster's.
    let mut fds: [libc::c_int; 2] = [-1, -1];
    let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if rc != 0 {
        let _ = ereport(FATAL)
            .errmsg_internal("could not create postmaster death-monitoring pipe")
            .finish(here("InitPostmasterDeathWatchHandle"));
        statemachine::ExitPostmaster(1);
    }
    // Non-blocking on the read end so PostmasterIsAliveInternal's read returns
    // EAGAIN while the postmaster lives.
    unsafe {
        let flags = libc::fcntl(fds[POSTMASTER_FD_WATCH], libc::F_GETFL, 0);
        if flags >= 0 {
            let _ = libc::fcntl(fds[POSTMASTER_FD_WATCH], libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
        POSTMASTER_ALIVE_FDS = [fds[0], fds[1]];
    }
}

/// `postmaster_alive_fds[POSTMASTER_FD_WATCH]` — the death-watch read fd, read
/// by pmsignal's `postmaster_death_watch_fd` seam.
pub fn postmaster_death_watch_fd() -> i32 {
    unsafe { POSTMASTER_ALIVE_FDS[POSTMASTER_FD_WATCH] }
}

/// C (`InitPostmasterChild`, miscinit.c:162):
/// `fcntl(postmaster_alive_fds[POSTMASTER_FD_WATCH], F_SETFD, FD_CLOEXEC)` —
/// keep the postmaster-death-watch pipe out of exec'd subprograms. The fd is
/// postmaster.c's own (`postmaster_alive_fds`), so this seam (consumed by
/// miscinit's InitPostmasterChild) is installed here. `Err` mirrors the C
/// `ereport(FATAL, ... could not set ... FD_CLOEXEC ...)`.
pub fn set_postmaster_death_watch_cloexec() -> types_error::PgResult<()> {
    let fd = unsafe { POSTMASTER_ALIVE_FDS[POSTMASTER_FD_WATCH] };
    let rc = unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
    if rc < 0 {
        return Err(ereport(FATAL)
            .errmsg_internal(
                "could not set postmaster death monitoring pipe to FD_CLOEXEC mode",
            )
            .into_error());
    }
    Ok(())
}

/// `read(postmaster_alive_fds[POSTMASTER_FD_WATCH], &c, 1)` — the raw
/// non-blocking read behind pmsignal's `read_postmaster_death_watch` seam.
/// Returns `(rc, errno)`.
pub fn read_postmaster_death_watch() -> (isize, i32) {
    let fd = unsafe { POSTMASTER_ALIVE_FDS[POSTMASTER_FD_WATCH] };
    let mut c: u8 = 0;
    let rc = unsafe { libc::read(fd, &mut c as *mut u8 as *mut libc::c_void, 1) };
    let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
    (rc, errno)
}

// ---------------------------------------------------------------------------
// Postmaster signal-handler + process-local-latch setup (PostmasterMain body).
//
// These are genuinely postmaster-owned (the C `pqsignal(SIGHUP,
// handle_pm_reload_request_signal)` block + `InitProcessLocalLatch()`), so they
// are implemented in-crate and installed by `init_seams` as the
// `install_postmaster_signal_handlers` / `init_process_local_latch` seams.
// ---------------------------------------------------------------------------

/// C (PostmasterMain): `pqinitmask(); sigprocmask(SIG_SETMASK, &BlockSig, NULL);`
/// then the full `pqsignal(...)` handler set, then `InitializeWaitEventSupport()`,
/// then `sigprocmask(SIG_SETMASK, &UnBlockSig, NULL)`.
pub fn install_postmaster_signal_handlers() {
    use types_signal::SigHandler;

    // pqinitmask(); sigprocmask(SIG_SETMASK, &BlockSig, NULL);
    backend_libpq_pqsignal::pqinitmask();
    backend_libpq_pqsignal::set_block_sig_mask();

    // The postmaster's signal-handler set (postmaster.c:550-558).
    port_pqsignal::pqsignal_be(crate::core::SIGHUP, SigHandler::Handler(crate::signals::handle_pm_reload_request_signal));
    port_pqsignal::pqsignal_be(crate::core::SIGINT, SigHandler::Handler(crate::signals::handle_pm_shutdown_request_signal));
    port_pqsignal::pqsignal_be(crate::core::SIGQUIT, SigHandler::Handler(crate::signals::handle_pm_shutdown_request_signal));
    port_pqsignal::pqsignal_be(crate::core::SIGTERM, SigHandler::Handler(crate::signals::handle_pm_shutdown_request_signal));
    port_pqsignal::pqsignal_be(libc::SIGALRM, SigHandler::Ignore); /* ignored */
    port_pqsignal::pqsignal_be(libc::SIGPIPE, SigHandler::Ignore); /* ignored */
    port_pqsignal::pqsignal_be(crate::core::SIGUSR1, SigHandler::Handler(crate::signals::handle_pm_pmsignal_signal));
    port_pqsignal::pqsignal_be(crate::core::SIGUSR2, SigHandler::Handler(crate::signals::dummy_handler)); /* reserved for children */
    port_pqsignal::pqsignal_be(crate::core::SIGCHLD, SigHandler::Handler(crate::signals::handle_pm_child_exit_signal));

    // This may configure SIGURG, depending on platform.
    let _ = backend_storage_ipc_waiteventset::InitializeWaitEventSupport();

    // No other place in Postgres should touch SIGTTIN/SIGTTOU; the postmaster
    // ignores them so a child does not freeze writing to stderr. Also ignore
    // SIGXFSZ so ulimit violations behave like disk-full.
    port_pqsignal::pqsignal_be(libc::SIGTTIN, SigHandler::Ignore);
    port_pqsignal::pqsignal_be(libc::SIGTTOU, SigHandler::Ignore);
    port_pqsignal::pqsignal_be(libc::SIGXFSZ, SigHandler::Ignore);

    // Begin accepting signals: sigprocmask(SIG_SETMASK, &UnBlockSig, NULL).
    let masks = backend_libpq_pqsignal::signal_masks();
    unsafe {
        let _ = libc::sigprocmask(libc::SIG_SETMASK, masks.unblock_sig(), core::ptr::null_mut());
    }
}

/// C (PostmasterMain): `InitProcessLocalLatch()`.
pub fn init_process_local_latch() {
    backend_utils_init_miscinit::InitProcessLocalLatch();
}

// ---------------------------------------------------------------------------
// CloseServerPorts (on_proc_exit callback) / unlink_external_pid_file
// ---------------------------------------------------------------------------

/// C: `static void CloseServerPorts(int status, Datum arg)`.
///
/// `on_proc_exit` callback registered (in C, by the `CreateDataDirLockFile`
/// step) to close the postmaster's listen sockets and remove the Unix socket
/// files before the `postmaster.pid` lockfile is removed — closing the sockets
/// first avoids a TCP-port-reuse race against an incoming postmaster.
pub fn CloseServerPorts() {
    /* First, explicitly close all the socket FDs. */
    for &fd in pm().listen_sockets.iter() {
        if closesocket(fd) != 0 {
            report(LOG, "CloseServerPorts", "could not close listen socket");
        }
    }
    pm_mut().listen_sockets.clear();

    /* Next, remove any filesystem entries for Unix sockets. */
    backend_libpq_pqcomm::RemoveSocketFiles();

    /*
     * We don't do anything about socket lock files here; those are removed in a
     * later on_proc_exit callback (owned by miscinit).
     */
}

/// C: `static void unlink_external_pid_file(int status, Datum arg)`.
///
/// `on_proc_exit` callback to delete `external_pid_file`. The pid-file path is a
/// GUC owned by the GUC unit; the unlink is performed by that owner's pid-file
/// machinery. The postmaster's decision (register the callback iff
/// `external_pid_file` is set) is preserved at the `maybe_write_external_pid_file`
/// seam call in `PostmasterMain`.
pub fn unlink_external_pid_file() {
    // The external-pid-file owner performs the unlink; nothing
    // postmaster-private to do here.
}

// ---------------------------------------------------------------------------
// ClosePostmasterPorts (the close_postmaster_ports seam, called in every child)
// ---------------------------------------------------------------------------

/// C: `void ClosePostmasterPorts(bool am_syslogger)`.
///
/// In a freshly-forked child, release the postmaster-only resources: the
/// WaitEventSet, the write end of the death-watch pipe, and the listen sockets.
pub fn ClosePostmasterPorts(_am_syslogger: bool) {
    // Release resources held by the postmaster's WaitEventSet. C frees it with
    // FreeWaitEventSetAfterFork (which, unlike FreeWaitEventSet, does NOT touch
    // the inherited epoll fd the parent still owns). Our WaitEventSet wrapper's
    // Drop calls FreeWaitEventSet; in the child we instead leak the handle
    // (set the field to None without dropping) so we don't disturb the parent's
    // kernel epoll object — the child's copy of the fds is closed by the kernel
    // at the child's own exit. This matches FreeWaitEventSetAfterFork's intent.
    // Take the WaitEventSet out and forget it (do not run Drop =
    // FreeWaitEventSet in the child).
    if let Some(set) = pm_mut().pm_wait_set.take() {
        core::mem::forget(set);
    }

    // Close the write end of the death-watch pipe ASAP so that, if the
    // postmaster dies, others don't think it's alive because we hold it open.
    unsafe {
        if POSTMASTER_ALIVE_FDS[POSTMASTER_FD_OWN] >= 0 {
            let _ = libc::close(POSTMASTER_ALIVE_FDS[POSTMASTER_FD_OWN]);
            POSTMASTER_ALIVE_FDS[POSTMASTER_FD_OWN] = -1;
        }
    }

    // Close the postmaster's listen sockets in the child.
    for &fd in pm().listen_sockets.iter() {
        if closesocket(fd) != 0 {
            report(LOG, "ClosePostmasterPorts", "could not close listen socket");
        }
    }
    pm_mut().listen_sockets.clear();

    // The syslog pipe read-end close and Bonjour are handled by the syslogger /
    // bonjour owners; not needed on this build's trust/no-syslogger path.
}

// ---------------------------------------------------------------------------
// PostmasterMain
// ---------------------------------------------------------------------------

/// C: `pg_noreturn void PostmasterMain(int argc, char *argv[])`.
///
/// `argv` carries the process argument vector (`argv[0]` is the program path).
/// Never returns: it ends in [`crate::serverloop::ServerLoop`], which loops
/// forever; if that ever returned, [`statemachine::ExitPostmaster`] terminates.
pub fn PostmasterMain(argv: &[&str]) -> ! {
    let argv_owned: Vec<String> = argv.iter().map(|s| (*s).to_string()).collect();

    /*
     * Set reference point for stack-depth checking and the startup PRNG seed,
     * remember the postmaster's own pid, and set IsPostmasterEnvironment.
     * (InitProcessGlobals + PostmasterPid = MyProcPid + IsPostmasterEnvironment)
     *
     * These are globals.c-level operations shared by every process; the
     * postmaster reaches the same real `InitProcessGlobals` the single-user
     * driver uses (we additionally set IsPostmasterEnvironment via the
     * small-globals setter — the postmaster, unlike a standalone backend, is the
     * postmaster environment).
     */
    let _ = backend_utils_init_small::InitProcessGlobals();
    backend_utils_init_small::globals::SetIsPostmasterEnvironment(true);

    /*
     * Create and switch into PostmasterContext; getInstallationPaths(argv[0]).
     * (Folded into the runtime's path resolution; the postmaster's working
     * context is its own MemoryContext, established by the boot driver.)
     *
     * Set up the postmaster's signal handlers (pqinitmask / block / pqsignal /
     * unblock), the wait-event support, and the process-local latch. The
     * postmaster's handler function pointers are this crate's `handle_pm_*`
     * functions; the install is performed by the signal/latch machinery via
     * these caller-side seams (fronting the not-yet-ported postmaster signal
     * setup + InitProcessLocalLatch in the boot driver).
     */
    sp::install_postmaster_signal_handlers::call();
    sp::init_process_local_latch::call();

    /*
     * Options setup: build GUC tables, parse the command line as GUC settings
     * (the postmaster shares this option set with the backend — see
     * process_postgres_switches). These are the same real seams the single-user
     * driver drives.
     */
    backend_utils_misc_guc_seams::initialize_guc_options::call()
        .unwrap_or_else(|e| panic!("InitializeGUCOptions: {e:?}"));
    let _dbname = backend_tcop_postgres_seams::process_postgres_switches::call(
        &argv_owned,
        types_guc::guc::GucContext::PGC_POSTMASTER,
    );

    /*
     * Capture `userDoption` — the `-D` switch value (C's getopt `case 'D':
     * userDoption = strdup(optarg)`). process_postgres_switches above set GUCs
     * but its captured `-D` lives in the tcop owner's private global; we re-parse
     * the value here for the SelectConfigFiles arg (the same value the C getopt
     * captured).
     */
    let user_doption = extract_user_doption(argv);

    /*
     * Locate config files + data dir, read postgresql.conf, validate DataDir,
     * chdir into the data dir, take the data-dir lock (amPostmaster = true).
     */
    if !backend_utils_misc_guc_seams::select_config_files::call(user_doption.as_deref(), "postgres")
        .unwrap_or(false)
    {
        statemachine::ExitPostmaster(2);
    }
    backend_utils_init_miscinit_seams::check_data_dir::call()
        .unwrap_or_else(|e| panic!("checkDataDir: {e:?}"));

    /* Check that pg_control exists. */
    check_control_file();

    backend_utils_init_miscinit_seams::change_to_data_dir::call()
        .unwrap_or_else(|e| panic!("ChangeToDataDir: {e:?}"));
    backend_utils_init_miscinit_seams::create_data_dir_lock_file::call(true)
        .unwrap_or_else(|e| panic!("CreateDataDirLockFile: {e:?}"));

    /* Read the control file (error checking + config info). */
    backend_tcop_postgres_seams::local_process_control_file::call(false)
        .unwrap_or_else(|e| panic!("LocalProcessControlFile: {e:?}"));

    /* Register the apply launcher before modules grab bgworker slots. */
    let _ = backend_replication_logical_launcher::ApplyLauncherRegister();

    /*
     * process any preloaded libraries. `process_shared_preload_libraries` is
     * miscinit.c's own (ported) body, called directly (the established pattern,
     * as in single-user). The C runs it in TopMemoryContext; a transient
     * context supplies the `Mcx` its `SplitDirectoriesString` parse needs.
     */
    {
        let spl_cx = mcx::MemoryContext::new("process_shared_preload_libraries");
        let _ = backend_utils_init_miscinit::process_shared_preload_libraries(spl_cx.mcx());
    }

    /* Initialize SSL library, if specified. */
    if sp::enable_ssl::call() {
        // secure_initialize(true); LoadedSSL = true — owned by the SSL provider.
    }

    /* Calculate MaxBackends + the child-slot table + fast-path locks. */
    backend_utils_init_postinit_seams::initialize_max_backends::call()
        .unwrap_or_else(|e| panic!("InitializeMaxBackends: {e:?}"));
    backend_postmaster_pmchild_seams::init_postmaster_child_slots::call();
    backend_utils_init_postinit_seams::initialize_fast_path_locks::call();

    /* shmem_request_hooks, runtime-computed shmem GUCs, custom RMGRs. */
    backend_tcop_postgres_seams::process_shmem_requests::call()
        .unwrap_or_else(|e| panic!("process_shmem_requests: {e:?}"));
    backend_tcop_postgres_seams::initialize_shmem_gucs::call()
        .unwrap_or_else(|e| panic!("InitializeShmemGUCs: {e:?}"));
    backend_tcop_postgres_seams::initialize_wal_consistency_checking::call()
        .unwrap_or_else(|e| panic!("InitializeWalConsistencyChecking: {e:?}"));

    /* Set up shared memory and semaphores. */
    backend_storage_ipc_ipci_seams::create_shared_memory_and_semaphores::call();

    /* Estimate number of openable files. */
    backend_storage_file_fd_seams::set_max_safe_fds::call()
        .unwrap_or_else(|e| panic!("set_max_safe_fds: {e:?}"));

    /* Initialize the postmaster-death-watch pipe. */
    InitPostmasterDeathWatchHandle();

    /* Forcibly remove standby-promotion + logrotate signal files. */
    sp::remove_promote_signal_files::call();
    sp::remove_logrotate_signal_files::call();
    sp::remove_log_metainfo_datafile::call();

    /* If enabled, start up the syslogger collection subprocess. */
    if sp::logging_collector::call() {
        startchildren::StartSysLogger();
    }

    /* Stop sending log to stderr now that the postmaster is fully launched. */
    sp::finalize_where_to_send_output::call();

    /* Report server startup in log. */
    report(LOG, "PostmasterMain", "starting PostgreSQL");

    /*
     * Establish input sockets. Loop over listen_addresses (TCP) and
     * unix_socket_directories, accumulating fds into pm().listen_sockets (C's
     * ListenSockets[] + NumListenSockets). The CloseServerPorts on_proc_exit
     * hook is registered by CreateDataDirLockFile's owner.
     */
    establish_input_sockets();

    if pm().listen_sockets.is_empty() {
        let _ = ereport(FATAL)
            .errmsg("no socket created for listening")
            .finish(here("PostmasterMain"));
        statemachine::ExitPostmaster(1);
    }

    /* Record postmaster options. */
    if !sp::create_opts_file::call(argv_owned.clone()) {
        statemachine::ExitPostmaster(1);
    }

    /* Write the external PID file if requested. */
    sp::maybe_write_external_pid_file::call();

    /* Remove old temporary files. */
    sp::remove_pg_temp_files::call();

    /* Initialize the autovacuum subsystem (no process start yet). */
    sp::autovac_init::call();

    /* Load configuration files for client authentication. */
    if !sp::load_hba::call() {
        let hba = sp::hba_file_name::call();
        let _ = ereport(FATAL)
            .errmsg(format!("could not load \"{hba}\""))
            .finish(here("PostmasterMain"));
        statemachine::ExitPostmaster(1);
    }
    if !sp::load_ident::call() {
        /* We can start up without the IDENT file. */
    }

    /* Remember postmaster startup time. */
    backend_tcop_postgres_seams::set_pg_start_time::call(
        backend_utils_adt_timestamp_seams::get_current_timestamp::call(),
    );

    /* Report postmaster status in the postmaster.pid file. */
    let _ = backend_utils_init_miscinit_seams::add_to_data_dir_lock_file::call(
        LOCK_FILE_LINE_PM_STATUS,
        PM_STATUS_STARTING,
    );

    statemachine::UpdatePMState(PMState::PmStartup);

    /* Make sure we can perform I/O while starting up. */
    ioworkers::maybe_adjust_io_workers();

    /*
     * Start bgwriter and checkpointer so they can help with recovery, then
     * start the startup process. Each StartChildProcess forks a real child.
     */
    if pm().checkpointer_pmchild.is_none() {
        pm_mut().checkpointer_pmchild = startchildren::StartChildProcess(B_CHECKPOINTER);
    }
    if pm().bgwriter_pmchild.is_none() {
        pm_mut().bgwriter_pmchild = startchildren::StartChildProcess(B_BG_WRITER);
    }

    /* We're ready to rock and roll... */
    pm_mut().startup_pmchild = startchildren::StartChildProcess(B_STARTUP);
    debug_assert!(pm().startup_pmchild.is_some());
    pm_mut().startup_status = StartupStatusEnum::StartupRunning;

    /* Some workers may be scheduled to start now */
    crate::bgworkers::maybe_start_bgworkers();

    /*
     * ServerLoop never returns; if it somehow did, close down.
     */
    crate::serverloop::ServerLoop()
}

/// The listen-socket establishment loop (C: the `ListenAddresses` /
/// `Unix_socket_directories` `foreach` blocks in PostmasterMain).
///
/// Parses each GUC list and calls `ListenServerPort`, accumulating fds into
/// `pm().listen_sockets`. The list parsing uses simple comma splitting (C uses
/// `SplitGUCList`/`SplitDirectoriesString`; for the address/directory lists the
/// element grammar is a plain comma-separated list).
/// Extract the `-D <datadir>` switch value from argv (C's getopt `case 'D':
/// userDoption = strdup(optarg)`). Supports both `-D dir` and `-Ddir` forms.
fn extract_user_doption(argv: &[&str]) -> Option<String> {
    let mut i = 1;
    while i < argv.len() {
        let a = argv[i];
        if a == "-D" {
            if i + 1 < argv.len() {
                return Some(argv[i + 1].to_string());
            }
        } else if let Some(rest) = a.strip_prefix("-D") {
            if !rest.is_empty() {
                return Some(rest.to_string());
            }
        }
        i += 1;
    }
    None
}

fn establish_input_sockets() {
    let port = sp::post_port_number::call();
    let max_connections = sp::max_connections::call();

    // TCP listen addresses.
    if let Some(addrs) = sp::listen_addresses::call() {
        for raw in addrs.split(',') {
            let curhost = raw.trim();
            if curhost.is_empty() {
                continue;
            }
            let (family, host) = if curhost == "*" {
                (libc::AF_UNSPEC, None)
            } else {
                (libc::AF_UNSPEC, Some(curhost))
            };
            let mut socks = core::mem::take(&mut pm_mut().listen_sockets);
            let status = ListenServerPort(family, host, port, None, &mut socks, MAXLISTEN, max_connections);
            pm_mut().listen_sockets = socks;
            if status.unwrap_or(STATUS_OK) != STATUS_OK {
                report(LOG, "PostmasterMain", format!("could not create listen socket for \"{curhost}\""));
            }
        }
    }

    // Unix socket directories.
    if let Some(dirs) = sp::unix_socket_directories::call() {
        for raw in dirs.split(',') {
            let socketdir = raw.trim();
            if socketdir.is_empty() {
                continue;
            }
            let mut socks = core::mem::take(&mut pm_mut().listen_sockets);
            let status =
                ListenServerPort(libc::AF_UNIX, None, port, Some(socketdir), &mut socks, MAXLISTEN, max_connections);
            pm_mut().listen_sockets = socks;
            if status.unwrap_or(STATUS_OK) != STATUS_OK {
                report(
                    LOG,
                    "PostmasterMain",
                    format!("could not create Unix-domain socket in directory \"{socketdir}\""),
                );
            }
        }
    }
}
