//! PostmasterMain — the boot sequence. C order is the deliverable; every
//! unported callee is a loud seam or a named panic.

use types_core::init::BackendType;
use types_error::{PgResult, DEBUG3, ERROR, FATAL, LOG, WARNING};
use types_guc::{GucContext, GucSource};

use crate::statemachine::{ExitPostmaster, StartChildProcess, StartSysLogger, UpdatePMState};
use crate::{
    loc, report, try_with_pm, with_pm, PMState, LOCK_FILE_LINE_LISTEN_ADDR, LOCK_FILE_LINE_PM_STATUS,
    LOCK_FILE_LINE_SOCKET_DIR, MAXLISTEN, PM_STATUS_STARTING,
};

const PROGNAME: &str = "postgres";
// PG_VERSION_STR renders compiler/platform detail; the version core is the
// parity-relevant fragment. Kept in step with adt_misc::introspect's
// PG_VERSION_STR (the version() banner) — see the rationale there.
const PG_VERSION_STR: &str = "pgrust 0.2 (PostgreSQL 18.3 compatible)";
const PG_MODE_MASK_OWNER: libc::mode_t = 0o077;
const LOG_METAINFO_DATAFILE: &str = "current_logfiles";

const WAL_LEVEL_MINIMAL: i32 = 0;
const ARCHIVE_MODE_OFF: i32 = 0;

pub fn InitProcessGlobals() {
    // miscinit's InitProcessGlobals carries C's whole body, including the
    // strong-seed of the (thread-local) global PRNG.
    miscinit::InitProcessGlobals(init_small::globals::process_id() as i32);
}

fn getInstallationPaths(argv0: &str) {
    let exe = match pg_path::find_my_exec(argv0, |m| {
        let _ = elog::ereport(LOG)
            .errmsg(m)
            .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "find_my_exec"));
    }) {
        Ok(exe) => exe,
        Err(_) => {
            write_stderr(format!(
                "FATAL:  {argv0}: could not locate my own executable path\n"
            ));
            ExitPostmaster(1);
        }
    };
    let mut buf = [0u8; types_core::MAXPGPATH];
    let n = exe.len().min(types_core::MAXPGPATH - 1);
    buf[..n].copy_from_slice(&exe.as_bytes()[..n]);
    init_small::globals::set_my_exec_path(buf);

    let pkglib = pg_path::get_pkglib_path(&exe);
    let mut buf = [0u8; types_core::MAXPGPATH];
    let n = pkglib.len().min(types_core::MAXPGPATH - 1);
    buf[..n].copy_from_slice(&pkglib.as_bytes()[..n]);
    init_small::globals::set_pkglib_path(buf);
    // DIVERGENCE: C verifies pkglib_path is a readable dir; skipped while
    // extension loading is unported (the boot must not require an installed
    // lib/ tree).
}

fn checkControlFile() {
    // C: snprintf(path, "%s/%s", DataDir, XLOG_CONTROL_FILE) — absolute, this
    // runs before ChangeToDataDir.
    let data_dir = init_small::globals::DataDir().unwrap_or("");
    let path = format!("{data_dir}/global/pg_control");
    match std::fs::File::open(&path) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            write_stderr(format!(
                "{PROGNAME}: could not find the database system\nExpected to find it in the directory \"{data_dir}\",\nbut could not open file \"{path}\": {e}\n"
            ));
            ExitPostmaster(2);
        }
        Err(e) => {
            write_stderr(format!("{PROGNAME}: could not open file \"{path}\": {e}\n"));
            ExitPostmaster(2);
        }
    }
}

fn write_stderr(msg: String) {
    elog::write_stderr(&msg);
}

extern "C" fn c_handle_pm_reload(sig: i32) {
    crate::handle_pm_reload_request_signal(sig);
}
extern "C" fn c_handle_pm_shutdown(sig: i32) {
    crate::handle_pm_shutdown_request_signal(sig);
}
extern "C" fn c_handle_pm_pmsignal(sig: i32) {
    crate::handle_pm_pmsignal_signal(sig);
}
extern "C" fn c_dummy_handler(_sig: i32) {}

// wasm32: WASI p1 delivers no signals; the postmaster's process-signal
// installs are no-ops (the postmaster is unreachable on wasm anyway —
// no listen sockets — but the crate must compile).
#[cfg(target_family = "wasm")]
fn pqsignal(_signum: i32, _handler: extern "C" fn(i32)) {}

#[cfg(not(target_family = "wasm"))]
fn pqsignal(signum: i32, handler: extern "C" fn(i32)) {
    // SAFETY: standard sigaction install; handlers only touch atomics + the
    // signal-safe SetLatch path.
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = handler as usize;
        libc::sigfillset(&mut sa.sa_mask);
        sa.sa_flags = libc::SA_RESTART;
        libc::sigaction(signum, &sa, std::ptr::null_mut());
    }
}

#[cfg(target_family = "wasm")]
fn pqsignal_ignore(_signum: i32) {}

#[cfg(not(target_family = "wasm"))]
fn pqsignal_ignore(signum: i32) {
    // SAFETY: SIG_IGN install, no handler code runs.
    unsafe {
        libc::signal(signum, libc::SIG_IGN);
    }
}

fn parse_long_option(optarg: &str) -> (String, Option<String>) {
    let (name, value) = match optarg.split_once('=') {
        Some((n, v)) => (n, Some(v.to_string())),
        None => (optarg, None),
    };
    (name.replace('-', "_"), value)
}

fn set_config_argv(name: &str, value: &str) -> PgResult<()> {
    guc::SetConfigOption(name, Some(value), GucContext::PGC_POSTMASTER, GucSource::PGC_S_ARGV)
}

// SplitGUCList reduced to the unquoted comma form; quoted list items arrive
// with the guc list-parsing owner.
fn split_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

pub fn PostmasterMain(argv: &[String]) -> PgResult<()> {
    let mut user_d_option: Option<String> = None;
    let mut output_config_variable: Option<String> = None;
    let mut listen_addr_saved = false;

    InitProcessGlobals();

    init_small::globals::SetPostmasterPid(init_small::globals::MyProcPid());
    init_small::globals::SetIsPostmasterEnvironment(true);

    // SAFETY: umask is async-signal-safe and process-global by design here.
    // wasm32: no umask on WASI (files carry no mode bits) — no-op.
    #[cfg(not(target_family = "wasm"))]
    unsafe {
        libc::umask(PG_MODE_MASK_OWNER);
    }

    let postmaster_context = mcx::MemoryContext::new("Postmaster");
    let _mcx = postmaster_context.mcx();

    getInstallationPaths(argv.first().map(|s| s.as_str()).unwrap_or(PROGNAME));

    crate::crash_signals::install_crash_signal_reporter();

    libpq_pqsignal::pqinitmask();
    libpq_pqsignal::block_signals();

    pqsignal(procsignal::signums::SIGHUP, c_handle_pm_reload);
    pqsignal(procsignal::signums::SIGINT, c_handle_pm_shutdown);
    pqsignal(procsignal::signums::SIGQUIT, c_handle_pm_shutdown);
    pqsignal(procsignal::signums::SIGTERM, c_handle_pm_shutdown);
    pqsignal_ignore(procsignal::signums::SIGALRM);
    pqsignal_ignore(procsignal::signums::SIGPIPE);
    pqsignal(procsignal::signums::SIGUSR1, c_handle_pm_pmsignal);
    pqsignal(procsignal::signums::SIGUSR2, c_dummy_handler);
    // SIGCHLD: no forked children under the thread model; child-exit
    // notification is pmchild's channel (handle_pm_child_exit_signal kept
    // for it to invoke).

    waiteventset::InitializeWaitEventSupport()?;
    miscinit::InitProcessLocalLatch();
    if let Some(l) = init_small::globals::MyLatch() {
        crate::publish_pm_latch(l);
    }

    // Terminal/job-control signals: real-kernel-only names (the emulation
    // never delivers them); wasm32 has no signals at all — no-op there.
    #[cfg(not(target_family = "wasm"))]
    {
        pqsignal_ignore(libc::SIGTTIN);
        pqsignal_ignore(libc::SIGTTOU);
        pqsignal_ignore(libc::SIGXFSZ);
    }

    libpq_pqsignal::unblock_signals();

    guc_seams::initialize_guc_options::call()?;
    stack_depth::adjust_max_stack_depth_from_rlimit()?;

    let mut args = argv.iter().skip(1).peekable();
    while let Some(arg) = args.next() {
        let Some(rest) = arg.strip_prefix('-') else {
            write_stderr(format!("{PROGNAME}: invalid argument: \"{arg}\"\nTry \"{PROGNAME} --help\" for more information.\n"));
            ExitPostmaster(1);
        };
        let (opt, mut inline_val) = if let Some(long) = rest.strip_prefix('-') {
            ('-', Some(long.to_string()))
        } else {
            let mut cs = rest.chars();
            let o = cs.next().unwrap_or('\0');
            let tail: String = cs.collect();
            (o, if tail.is_empty() { None } else { Some(tail) })
        };
        let mut take_val = |args: &mut std::iter::Peekable<std::iter::Skip<std::slice::Iter<String>>>| {
            inline_val.take().or_else(|| args.next().cloned()).unwrap_or_else(|| {
                write_stderr(format!("{PROGNAME}: option requires an argument -- {opt}\n"));
                ExitPostmaster(1);
            })
        };
        match opt {
            'B' => set_config_argv("shared_buffers", &take_val(&mut args))?,
            'b' => init_small::globals::SetIsBinaryUpgrade(true),
            'C' => output_config_variable = Some(take_val(&mut args)),
            '-' | 'c' => {
                let optarg = take_val(&mut args);
                let (name, value) = parse_long_option(&optarg);
                let Some(value) = value else {
                    return elog::ereport(ERROR)
                        .errcode(types_error::ERRCODE_SYNTAX_ERROR)
                        .errmsg(if opt == '-' {
                            format!("--{optarg} requires a value")
                        } else {
                            format!("-c {optarg} requires a value")
                        })
                        .finish(loc(646, "PostmasterMain"));
                };
                set_config_argv(&name, &value)?;
            }
            'D' => user_d_option = Some(take_val(&mut args)),
            'd' => {
                let v: i32 = take_val(&mut args).parse().unwrap_or(0);
                postgres_seams::set_debug_options::call(v, GucContext::PGC_POSTMASTER as i32 as u8)?;
            }
            'E' => set_config_argv("log_statement", "all")?,
            'e' => set_config_argv("datestyle", "euro")?,
            'F' => set_config_argv("fsync", "false")?,
            'f' => {
                let v = take_val(&mut args);
                if !postgres_seams::set_plan_disabling_options::call(
                    &v,
                    GucContext::PGC_POSTMASTER as i32 as u8,
                )? {
                    write_stderr(format!("{PROGNAME}: invalid argument for option -f: \"{v}\"\n"));
                    ExitPostmaster(1);
                }
            }
            'h' => set_config_argv("listen_addresses", &take_val(&mut args))?,
            'i' => set_config_argv("listen_addresses", "*")?,
            'j' => {}
            'k' => set_config_argv("unix_socket_directories", &take_val(&mut args))?,
            'l' => set_config_argv("ssl", "true")?,
            'N' => set_config_argv("max_connections", &take_val(&mut args))?,
            'O' => set_config_argv("allow_system_table_mods", "true")?,
            'P' => set_config_argv("ignore_system_indexes", "true")?,
            'p' => set_config_argv("port", &take_val(&mut args))?,
            'r' => {
                let _ = take_val(&mut args);
            }
            'S' => set_config_argv("work_mem", &take_val(&mut args))?,
            's' => set_config_argv("log_statement_stats", "true")?,
            'T' => set_config_argv("send_abort_for_crash", "true")?,
            't' => {
                let v = take_val(&mut args);
                match postgres_seams::get_stats_option_name::call(&v) {
                    Some(name) => set_config_argv(name, "true")?,
                    None => {
                        write_stderr(format!("{PROGNAME}: invalid argument for option -t: \"{v}\"\n"));
                        ExitPostmaster(1);
                    }
                }
            }
            'W' => set_config_argv("post_auth_delay", &take_val(&mut args))?,
            _ => {
                write_stderr(format!("Try \"{PROGNAME} --help\" for more information.\n"));
                ExitPostmaster(1);
            }
        }
    }

    // §9 provider seam (dst-p3-scheduler; COMPOSE FINDING 1): under sim cfg
    // the WHOLE binary sees SimVfs, whose namespace starts empty. Compose
    // the world — the initdb'd datadir snapshot plus the manifest's share
    // asset trees — BEFORE the first vfs read: SelectConfigFiles below
    // already validates timezone GUCs through the pgtz vfs directory scan.
    // Boot-installer shape (the pqcomm init_seams idiom): one cfg-gated
    // call, no runtime knob; product builds compile none of this. The
    // datadir is resolved exactly the way SelectConfigFiles resolves it
    // (-D else PGDATA, make_absolute_path) so the sim namespace and the
    // product's DataDir string name the same tree.
    #[cfg(pgrust_sim)]
    {
        match user_d_option
            .clone()
            .or_else(|| std::env::var("PGDATA").ok())
            .map(|d| miscinit::make_absolute_path(&d))
        {
            Some(dd) => match vfs::sim_boot::compose_boot_namespace(&dd) {
                Ok(line) => write_stderr(format!("{line}\n")),
                Err(e) => {
                    write_stderr(format!("{PROGNAME}: sim asset ingest failed: {e}\n"));
                    ExitPostmaster(2);
                }
            },
            None => {
                write_stderr(format!(
                    "{PROGNAME}: sim boot requires -D or PGDATA (the asset-manifest datadir root)\n"
                ));
                ExitPostmaster(2);
            }
        }
    }

    if !guc_seams::select_config_files::call(user_d_option.as_deref(), PROGNAME)? {
        ExitPostmaster(2);
    }

    // pgrust public-release memory auto-tune (PGRUST_MEM_AUTOTUNE, default OFF):
    // install machine-scaled shared_buffers / work_mem / effective_cache_size /
    // maintenance_work_mem / parallel-worker defaults at PGC_S_DYNAMIC_DEFAULT.
    // Runs after the config file (so an explicit setting still wins) and before
    // shmem sizing locks in NBuffers. No-op unless PGRUST_MEM_AUTOTUNE is set,
    // keeping the byte-identical pg_settings/SHOW ALL conformance output.
    guc::autotune::apply_memory_autotune()?;

    if let Some(name) = output_config_variable.as_deref() {
        // GUC_RUNTIME_COMPUTED split: runtime-computed -C values print after
        // shmem sizing in C; the flags probe joins with the guc-funcs unit.
        let config_val = guc::GetConfigOption(name, false, false)?;
        println!("{}", config_val.as_deref().unwrap_or(""));
        ExitPostmaster(0);
    }

    miscinit_seams::check_data_dir::call()?;
    checkControlFile();
    miscinit::ChangeToDataDir()?;

    let su_reserved = guc_tables::vars::SuperuserReservedConnections.read();
    let reserved = guc_tables::vars::ReservedConnections.read();
    let max_connections = init_small::globals::MaxConnections();
    if su_reserved + reserved >= max_connections {
        write_stderr(format!(
            "{PROGNAME}: \"superuser_reserved_connections\" ({su_reserved}) plus \"reserved_connections\" ({reserved}) must be less than \"max_connections\" ({max_connections})\n"
        ));
        ExitPostmaster(1);
    }
    let wal_level = guc_tables::vars::wal_level.read();
    if guc_tables::vars::XLogArchiveMode.read() > ARCHIVE_MODE_OFF && wal_level == WAL_LEVEL_MINIMAL
    {
        return elog::ereport(ERROR)
            .errmsg("WAL archival cannot be enabled when \"wal_level\" is \"minimal\"")
            .finish(loc(851, "PostmasterMain"));
    }
    if guc_tables::vars::max_wal_senders.read() > 0 && wal_level == WAL_LEVEL_MINIMAL {
        return elog::ereport(ERROR)
            .errmsg("WAL streaming (\"max_wal_senders\" > 0) requires \"wal_level\" to be \"replica\" or \"logical\"")
            .finish(loc(854, "PostmasterMain"));
    }
    if guc_tables::vars::summarize_wal.read() && wal_level == WAL_LEVEL_MINIMAL {
        return elog::ereport(ERROR)
            .errmsg("WAL cannot be summarized when \"wal_level\" is \"minimal\"")
            .finish(loc(857, "PostmasterMain"));
    }

    // CheckDateTokenTables: a static-order assertion over datetime.c tables;
    // that unit (and its tables) is unported — nothing to check yet.

    if elog::message_level_is_interesting(DEBUG3) {
        let mut si = String::from("initial environment dump:");
        for (k, v) in std::env::vars() {
            si.push_str(&format!("\n{k}={v}"));
        }
        crate::report_internal(DEBUG3, si, 891, "PostmasterMain");
    }

    miscinit::CreateDataDirLockFile(true)?;

    transam_xlog::LocalProcessControlFile(false)?;

    launcher_seams::apply_launcher_register::call();

    miscinit_seams::process_shared_preload_libraries::call()?;
    miscinit_seams::process_preload_contrib::call()?;

    if guc_tables::vars::EnableSSL.read() {
        be_secure::secure_initialize(true)?;
        backend_startup::loaded_ssl::set(true);
    }

    // M2 pool-binding: reserve PGPROCs for the standing runtime executor
    // gang — and, under PGRUST_RUNTIME_POOLDB=1 (M2 inc-2), one per pool
    // thread — BEFORE MaxBackends is computed (0 unless PGRUST_RUNTIME=1 —
    // byte-identical sizing with the runtime off).
    init_small::globals::SetRuntimeGangProcs(launch_backend::rtgang::runtime_reserved_procs());
    postinit::InitializeMaxBackends()?;
    pmchild_seams::init_postmaster_child_slots::call();
    // C runs this inside CreateSharedMemoryAndSemaphores; hoisted next to the
    // slot-pool init (plain statics, no shmem placement here).
    bgworker::BackgroundWorkerShmemInit();
    if launcher_seams::apply_launcher_shmem_init::is_installed() {
        launcher_seams::apply_launcher_shmem_init::call();
    }

    let fastpath_groups = postinit::InitializeFastPathLocks();

    miscinit_seams::process_shmem_requests::call()?;

    ipci_seams::initialize_shmem_gucs::call(fastpath_groups)?;

    transam_xlog_seams::initialize_wal_consistency_checking::call()?;

    ipci_seams::create_shared_memory_and_semaphores::call(fastpath_groups)?;

    // C sizes the fd budget for ONE backend process; every child gets its own
    // copy by fork. Here every child is a thread in the postmaster's own fd
    // table, so the budget is shared and has to be divided: live children
    // (each holds a socket, a wake pipe and its wait sets) and the backends
    // among them that also hold a file-descriptor cache.
    fd::set_max_safe_fds(
        pmchild_seams::max_live_postmaster_children::call(),
        init_small::globals::MaxBackends(),
    )?;

    // InitPostmasterDeathWatchHandle: one address space — children observing
    // postmaster death is the pmchild/waiteventset redesign (WL_POSTMASTER_DEATH
    // panics there by design).

    xlogrecovery_seams::remove_promote_signal_files::call();
    syslogger_seams::remove_logrotate_signal_files::call();

    match std::fs::remove_file(LOG_METAINFO_DATAFILE) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            let _ = elog::ereport(LOG)
                .errcode_for_file_access()
                .errmsg(format!("could not remove file \"{LOG_METAINFO_DATAFILE}\": {e}"))
                .finish(loc(1068, "PostmasterMain"));
        }
    }

    if guc_tables::vars::Logging_collector.read() {
        StartSysLogger();
    }

    elog::config::set_where_to_send_output(types_dest_none());

    report(LOG, format!("starting {PG_VERSION_STR}"), 1105, "PostmasterMain");

    // pgrust extension (GL-STRDEFECTS-1 witness): the regexp engine tiers
    // are a build property (RE2 links only where libre2 existed at build) —
    // name them once at startup so a Spencer-only binary can never run
    // silently. pg_settings twin: pgrust.regex_re2_linked.
    if regexp_alt::re2_available() {
        report(
            LOG,
            "regexp engines: re2+spencer (regex_engine=auto dispatches compatible \
             patterns to re2)"
                .into(),
            0,
            "PostmasterMain",
        );
    } else {
        report(
            WARNING,
            "regexp engines: spencer only — RE2 was not linked into this build \
             (regex_engine=auto has no re2 tier; SET regex_engine=re2 will error)"
                .into(),
            0,
            "PostmasterMain",
        );
    }

    ipc_seams::on_proc_exit::call(close_server_ports_cb, 0);

    let listen_addresses = guc_tables::vars::ListenAddresses.read();
    if let Some(listen_addresses) = listen_addresses.filter(|s| !s.is_empty()) {
        let mut success = 0;
        let elems = split_list(&listen_addresses);
        for curhost in &elems {
            let host = if curhost == "*" { None } else { Some(curhost.as_str()) };
            let port = guc_tables::vars::PostPortNumber.read() as u16;
            let status = with_pm(|pm| {
                pqcomm_seams::listen_server_port::call(
                    host,
                    port,
                    None,
                    &mut pm.listen_sockets,
                    MAXLISTEN,
                )
            });
            match status {
                Ok(()) => {
                    success += 1;
                    if !listen_addr_saved {
                        miscinit::AddToDataDirLockFile(LOCK_FILE_LINE_LISTEN_ADDR, curhost)?;
                        listen_addr_saved = true;
                    }
                }
                Err(_) => {
                    let _ = elog::ereport(WARNING)
                        .errmsg(format!("could not create listen socket for \"{curhost}\""))
                        .finish(loc(1205, "PostmasterMain"));
                }
            }
        }
        if success == 0 && !elems.is_empty() {
            return elog::ereport(FATAL)
                .errmsg("could not create any TCP/IP sockets")
                .finish(loc(1212, "PostmasterMain"));
        }
    }

    let unix_dirs = guc_tables::vars::Unix_socket_directories.read();
    if let Some(unix_dirs) = unix_dirs.filter(|s| !s.is_empty()) {
        let mut success = 0;
        let elems = split_list(&unix_dirs);
        for socketdir in &elems {
            let port = guc_tables::vars::PostPortNumber.read() as u16;
            let status = with_pm(|pm| {
                pqcomm_seams::listen_server_port::call(
                    None,
                    port,
                    Some(socketdir.as_str()),
                    &mut pm.listen_sockets,
                    MAXLISTEN,
                )
            });
            match status {
                Ok(()) => {
                    success += 1;
                    if success == 1 {
                        miscinit::AddToDataDirLockFile(LOCK_FILE_LINE_SOCKET_DIR, socketdir)?;
                    }
                }
                Err(_) => {
                    let _ = elog::ereport(WARNING)
                        .errmsg(format!(
                            "could not create Unix-domain socket in directory \"{socketdir}\""
                        ))
                        .finish(loc(1255, "PostmasterMain"));
                }
            }
        }
        if success == 0 && !elems.is_empty() {
            return elog::ereport(FATAL)
                .errmsg("could not create any Unix-domain sockets")
                .finish(loc(1261, "PostmasterMain"));
        }
    }

    if with_pm(|pm| pm.listen_sockets.is_empty()) {
        return elog::ereport(FATAL)
            .errmsg("no socket created for listening")
            .finish(loc(1271, "PostmasterMain"));
    }

    if !listen_addr_saved {
        miscinit::AddToDataDirLockFile(LOCK_FILE_LINE_LISTEN_ADDR, "")?;
    }

    if !CreateOptsFile(argv) {
        ExitPostmaster(1);
    }

    if let Some(pidfile) = guc_tables::vars::external_pid_file.read().filter(|s| !s.is_empty()) {
        match std::fs::write(&pidfile, format!("{}\n", init_small::globals::MyProcPid())) {
            Ok(()) => {
                // wasm32: no unix mode bits on WASI; the chmod is a no-op
                // (Ok(()) keeps the C control flow: only failures report).
                #[cfg(not(target_family = "wasm"))]
                let perm_result = {
                    let perms = std::os::unix::fs::PermissionsExt::from_mode(0o644);
                    std::fs::set_permissions(&pidfile, perms)
                };
                #[cfg(target_family = "wasm")]
                let perm_result: std::io::Result<()> = Ok(());
                if perm_result.is_err() {
                    write_stderr(format!(
                        "{PROGNAME}: could not change permissions of external PID file \"{pidfile}\"\n"
                    ));
                }
            }
            Err(_) => {
                write_stderr(format!(
                    "{PROGNAME}: could not write external PID file \"{pidfile}\"\n"
                ));
            }
        }
        ipc_seams::on_proc_exit::call(unlink_external_pid_file_cb, 0);
    }

    fd::RemovePgTempFiles()?;

    autovacuum_seams::autovac_init::call();

    if !auth_seams::load_hba::call() {
        // translator: %s is a configuration file (C prints HbaFileName)
        return elog::ereport(FATAL)
            .errmsg(format!(
                "could not load {}",
                guc_tables::vars::HbaFileName.read().unwrap_or_default()
            ))
            .finish(loc(1336, "PostmasterMain"));
    }
    let _ = auth_seams::load_ident::call();

    // pthread_is_threaded_np check: inverted by design — this postmaster IS
    // multithreaded (thread-model children).

    set_pg_start_time(timestamp_seams::get_current_timestamp::call());

    miscinit::AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STARTING)?;

    // Last single-threaded instant: StartChildProcess below spawns the first
    // backend thread. Any tap install after this point is a bug, not a
    // deferred contrib load (hook-surface.md's `tap!` boot-phase gate).
    seam_core::close_tap_boot_phase();

    // Same instant: the process-global libc locale is now final. setlocale()
    // is process-global and not thread-safe — a concurrent transition frees
    // locale storage other threads may be mid-read on (the diesel parallel
    // suite SIGABRT). From here on pg_locale validates with newlocale() and
    // keeps per-thread records instead of touching the global.
    pg_locale::freeze_global_locale();

    UpdatePMState(PMState::PM_STARTUP);

    crate::serverloop::maybe_adjust_io_workers();

    if with_pm(|pm| pm.checkpointer.is_none()) {
        let c = StartChildProcess(BackendType::Checkpointer);
        with_pm(|pm| pm.checkpointer = c);
    }
    if with_pm(|pm| pm.bgwriter.is_none()) {
        let c = StartChildProcess(BackendType::BgWriter);
        with_pm(|pm| pm.bgwriter = c);
    }

    let startup = StartChildProcess(BackendType::Startup);
    debug_assert!(startup.is_some());
    with_pm(|pm| {
        pm.startup = startup;
        pm.startup_status = crate::StartupStatusEnum::Running;
    });

    let status = crate::serverloop::ServerLoop()?;

    ExitPostmaster(if status == types_core::STATUS_OK { 0 } else { 1 });
}

fn types_dest_none() -> types_dest::CommandDest {
    types_dest::CommandDest::None
}

// PgStartTime home is globals.c (unhosted in init_small yet); postmaster is
// the writer, xlog/pgstat the readers via this accessor pair.
static PG_START_TIME: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

pub fn set_pg_start_time(ts: i64) {
    PG_START_TIME.store(ts, std::sync::atomic::Ordering::Relaxed);
}

pub fn pg_start_time() -> i64 {
    PG_START_TIME.load(std::sync::atomic::Ordering::Relaxed)
}

fn close_server_ports_cb(_code: i32, _arg: usize) {
    // try_with_pm, not with_pm: this on_proc_exit callback can run while a
    // with_pm borrow is still on the stack (a FATAL raised from inside
    // listen_server_port, itself called under with_pm, drains callbacks
    // synchronously before unwinding). C has no aliasing check here and
    // just closes the fds; skip silently on the reentrant case instead of
    // panicking — the process is exiting, so the OS closes the fds anyway.
    let _ = try_with_pm(|pm| {
        for fd in pm.listen_sockets.drain(..) {
            // SAFETY: closing listen fds owned by the postmaster.
            unsafe {
                libc::close(fd);
            }
        }
    });
    // Unix-socket file unlinking rides with the pqcomm socket-half owner.
}

fn unlink_external_pid_file_cb(_code: i32, _arg: usize) {
    if let Some(pidfile) = guc_tables::vars::external_pid_file.read() {
        if !pidfile.is_empty() {
            let _ = std::fs::remove_file(pidfile);
        }
    }
}

fn CreateOptsFile(argv: &[String]) -> bool {
    use std::io::Write;
    let fullprogname = String::from_utf8_lossy(
        &init_small::globals::my_exec_path()
            .iter()
            .copied()
            .take_while(|b| *b != 0)
            .collect::<Vec<u8>>(),
    )
    .into_owned();
    let mut line = format!("{fullprogname}");
    for arg in argv.iter().skip(1) {
        line.push_str(" \"");
        line.push_str(arg);
        line.push('"');
    }
    line.push('\n');
    // vfs-routed (provider-seam reroute): postmaster.opts lives in the
    // datadir domain; std::fs would bypass the sim namespace.
    match fd::write_whole_file("postmaster.opts", line.as_bytes(), false) {
        Ok(()) => true,
        Err(en) => {
            let _ = elog::ereport(LOG)
                .with_saved_errno(en)
                .errcode_for_file_access()
                .errmsg("could not create file \"postmaster.opts\": %m".to_string())
                .finish(loc(3862, "CreateOptsFile"));
            false
        }
    }
}
