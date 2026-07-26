//! PostgresStdioWireMain (pgrust extension, no C counterpart): the
//! --stdio-wire entry. No postmaster, no auxiliary threads, exactly ONE
//! wire-protocol session over the stdio transport provider that
//! seams_init::init_all_with_transport installed at boot (§2.4 transport
//! seam, docs/design/dst-and-wasm.md) — stdin carries frontend pgwire
//! bytes, stdout carries backend pgwire bytes, stderr stays the log.
//!
//! This is the wasm32-wasip1 client-server story (WASI p1 has no socket();
//! the wasmtime host pumps the pipes) and doubles as the native
//! differential arm. Single-threaded by construction: the process boots,
//! serves the one session, and exits at Terminate/EOF through the same
//! ProcExitThread frame as --single (shutdown checkpoint included).
//!
//! Boot ladder = PostgresSingleUserMain's, verbatim; the session half =
//! backend_startup::wire_session_initialize (startup packet + trust
//! AuthenticationOk) + InitProcess + PostgresMain with
//! whereToSendOutput = Remote. Identity: the startup packet names
//! user/database on the wire, but the standalone InitPostgres arm
//! (!IsUnderPostmaster) runs the session as the bootstrap superuser —
//! trust-by-construction, like --single; hba enforcement stays with the
//! postmaster path.

use ::types_error::PgResult;
use ::types_guc::GucContext;

use crate::switches;

const PROGNAME: &str = "postgres";

pub fn PostgresStdioWireMain(argv: &[String], username: &str) -> ! {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || -> core::convert::Infallible {
            let err = match stdio_wire_main_inner(argv, username) {
                Ok(never) => match never {},
                Err(err) => err,
            };
            // A FATAL that unwound as Err (rather than ereport-finish):
            // report it, then take C's proc_exit(1).
            elog::emit_error_report_for(&err);
            ipc_seams::proc_exit::call(1, init_small::globals::MyProcPid())
        },
    ));
    let payload = match outcome {
        Ok(never) => match never {},
        Err(payload) => payload,
    };
    match payload.downcast_ref::<ipc::ProcExitThread>() {
        // Exit callbacks already ran (ipc::proc_exit drains inline when
        // !IsUnderPostmaster — the shutdown checkpoint among them).
        Some(p) => std::process::exit(p.code),
        None => std::panic::resume_unwind(payload),
    }
}

// Never returns Ok: the tail call is PostgresMain (-> !); every early exit
// is a FATAL ereport (which proc_exits via unwind) or an Err.
// pub(crate): the ladder + session half are TRANSPORT-BLIND (everything
// provider-specific lives behind the §2.4 seam slots installed at boot), so
// the P4 sim-net mode (sim_net.rs) runs this exact fn over its provider.
pub(crate) fn stdio_wire_main_inner(
    argv: &[String],
    username: &str,
) -> PgResult<core::convert::Infallible> {
    // Mechanical split (PERMIT-S1 WS-DEMO): boot half + session half compose
    // to the original body VERBATIM. The sim-net two-session mode runs the
    // boot half once on the main thread and the session half once per
    // session thread; this single-session path is unchanged.
    stdio_wire_boot_half(argv, username)?;
    stdio_wire_session_half()
}

// The boot ladder half: everything through set_pg_start_time. Process-wide
// state (shmem analogs, GUC store, lockfile, control file) — run ONCE per
// process, on the thread that owns the boot.
pub(crate) fn stdio_wire_boot_half(argv: &[String], username: &str) -> PgResult<()> {
    assert!(!init_small::globals::IsUnderPostmaster());

    // ---- PostgresSingleUserMain's boot ladder (single_user.rs), verbatim
    // through set_pg_start_time. C order is the deliverable — do not
    // reorder.
    miscinit::InitStandaloneProcess(argv.first().map(|s| s.as_str()).unwrap_or(PROGNAME))?;

    guc_seams::initialize_guc_options::call()?;
    stack_depth::adjust_max_stack_depth_from_rlimit()?;

    // The trailing-DBNAME pickup is single-user vocabulary; on the wire the
    // startup packet names the database, so a bare argument is parsed (and
    // accepted) but only used as documentation of intent.
    let mut dbname_arg: Option<String> = None;
    switches::process_postgres_switches_dbname(
        argv,
        GucContext::PGC_POSTMASTER as i32 as u8,
        &mut dbname_arg,
    )?;
    let _ = username; // identity comes from the startup packet / standalone arm

    if !guc_seams::select_config_files::call(switches::user_d_option().as_deref(), PROGNAME)? {
        ipc_seams::proc_exit::call(1, init_small::globals::MyProcPid());
    }

    miscinit_seams::check_data_dir::call()?;
    miscinit::ChangeToDataDir()?;

    miscinit::CreateDataDirLockFile(false)?;

    transam_xlog::LocalProcessControlFile(false)?;

    miscinit_seams::process_shared_preload_libraries::call()?;

    postinit::InitializeMaxBackends()?;

    pmchild_seams::init_postmaster_child_slots::call();

    let fastpath_groups = postinit::InitializeFastPathLocks();

    miscinit_seams::process_shmem_requests::call()?;

    ipci_seams::initialize_shmem_gucs::call(fastpath_groups)?;

    transam_xlog_seams::initialize_wal_consistency_checking::call()?;

    ipci_seams::create_shared_memory_and_semaphores::call(fastpath_groups)?;

    // One process, one thread of execution: the whole descriptor budget is
    // this thread's (C's per-backend arithmetic, unshared).
    fd::set_max_safe_fds(1, 1)?;

    postmaster_seams::set_pg_start_time::call(timestamp_seams::get_current_timestamp::call());
    Ok(())
}

// ---- The wire session half (BackendMain's order: connection first,
// InitProcess second). Backend-local: safe to run on a spawned session
// thread whose thread-local globals were snapshotted from the booted thread
// (sim_net.rs two-session mode); the transport provider state it reaches is
// itself thread-local.
pub(crate) fn stdio_wire_session_half() -> PgResult<core::convert::Infallible> {
    {
        let startup = mcx::MemoryContext::new("WireStartup");
        backend_startup::wire_session_initialize(startup.mcx())?;
    }

    lmgr_proc::InitProcess(miscinit::GetMyBackendType())?;

    // Same real-signal bridge as --single: a native SIGTERM/SIGINT pends the
    // thread-signal bit and interrupts a blocked stdin read (no SA_RESTART).
    // wasm arm: no signals exist; Ctrl-C kills the runtime (crash recovery
    // next boot), C's own no-graceful-signal story.
    crate::single_user::install_single_user_signal_bridge();
    libpq_pqsignal::unblock_signals();

    let (dbname, username) = init_small::globals::WithMyProcPort(|p| {
        (
            p.database_name.clone().unwrap_or_default(),
            p.user_name.clone().unwrap_or_default(),
        )
    });
    crate::PostgresMain(&dbname, &username)
}
