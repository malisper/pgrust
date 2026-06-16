//! `PostgresSingleUserMain` (`tcop/postgres.c:4055`, PostgreSQL 18.3) — the
//! standalone single-user backend entry, reached from `main()` for the
//! `DISPATCH_SINGLE` (`--single`) case.
//!
//! Single-user mode runs the whole server in one process: there is no
//! postmaster, so this driver performs the standalone equivalent of what the
//! postmaster does once at startup (read the config files, take the data-dir
//! lock, size and create shared memory + semaphores itself) and the per-backend
//! setup the postmaster would do per child (`InitProcess`), then hands off to
//! [`crate::main_loop::PostgresMain`], which does `BaseInit()` /
//! `InitPostgres()` / `SetProcessingMode(NormalProcessing)` and runs the
//! `InteractiveBackend` read loop.
//!
//! `pg_noreturn` in C: this never returns (it ends through `proc_exit` or runs
//! `PostgresMain` forever), so the Rust signature is `-> !`.

use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, FATAL};
use types_guc::guc::GucContext;

use backend_tcop_postgres_seams as s;

/// `pg_noreturn void PostgresSingleUserMain(int argc, char *argv[], const char
/// *username)` (postgres.c:4055).
///
/// `argv` is the full process argv (including the `--single` flag, which
/// `process_postgres_switches` skips); `username` is the OS user name resolved
/// by `main()` via `GetUserNameOrExit`. Never returns.
pub fn PostgresSingleUserMain(argv: &[&str], username: &str) -> ! {
    match run(argv, username) {
        Ok(()) => {
            // `run` only returns `Ok` after `PostgresMain` (which is `-> !`),
            // so this is unreachable; mirror the C `pg_noreturn` contract.
            unreachable!("PostgresSingleUserMain returned without diverging")
        }
        Err(err) => {
            // A standalone-bootstrap FATAL: report it and exit, mirroring the C
            // where the ereport(FATAL) longjmps to the top and the process dies.
            backend_utils_error::emit_error_report_for(&err);
            backend_storage_ipc_ipc_seams::proc_exit::call(1)
        }
    }
}

/// The body of [`PostgresSingleUserMain`], returning `PgResult` so a
/// bootstrap-phase FATAL is reported by the `!`-returning wrapper. Returns
/// `Ok` only after `PostgresMain` (which never returns), so in practice it
/// diverges.
fn run(argv: &[&str], username: &str) -> PgResult<()> {
    // Assert(!IsUnderPostmaster): single-user mode has no postmaster parent.
    debug_assert!(!backend_utils_init_small::globals::IsUnderPostmaster());

    // Initialize startup process environment (sets MyProcPid, MyStartTime,
    // application_name, the standalone-process latch, etc.).
    backend_utils_init_miscinit_seams::init_standalone_process::call(
        argv.first().copied().unwrap_or(""),
    )?;

    // Set default values for command-line options. SetProcessingMode is already
    // InitProcessing by default; the C re-asserts it here. InitializeGUCOptions
    // builds the GUC tables so process_postgres_switches can set values.
    backend_utils_init_miscinit::SetProcessingMode(
        types_core::init::ProcessingMode::InitProcessing,
    );
    backend_utils_misc_guc_seams::initialize_guc_options::call()?;

    // Parse command-line options. The seam's C `*dbname` out-parameter is
    // returned by the in-crate parser as the captured database name; the
    // `--single` flag is skipped inside it. PGC_POSTMASTER: switches are
    // "secure" (came from the command line).
    let argv_owned: alloc::vec::Vec<alloc::string::String> =
        argv.iter().map(|s| (*s).to_owned()).collect();
    let mut dbname = crate::guc::process_postgres_switches(&argv_owned, GucContext::PGC_POSTMASTER)?;

    // Must have gotten a database name, or have a default (the username).
    if dbname.is_none() {
        // dbname = username; (username is never NULL here — main() resolves it
        // via GetUserNameOrExit before dispatching, so the inner FATAL for a
        // NULL username is dead in this configuration, but kept faithfully.)
        if username.is_empty() {
            return Err(PgError::new(
                FATAL,
                alloc::format!("{}: no database nor user name specified", progname()),
            )
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        }
        dbname = Some(username.to_owned());
    }
    let dbname = dbname.expect("dbname defaulted to username above");

    // Acquire configuration parameters. userDoption is the -D switch captured
    // by process_postgres_switches; progname is the program name.
    if !backend_utils_misc_guc_seams::select_config_files::call(
        crate::globals::user_doption(),
        &progname(),
    )? {
        backend_storage_ipc_ipc_seams::proc_exit::call(1)
    }

    // Validate the data directory, chdir into it, and take the data-dir lock.
    backend_utils_init_miscinit_seams::check_data_dir::call()?;
    backend_utils_init_miscinit_seams::change_to_data_dir::call()?;

    // Create lockfile for data directory (amPostmaster = false).
    backend_utils_init_miscinit_seams::create_data_dir_lock_file::call(false)?;

    // Read the control file (error checking + carries config). Owned by the
    // unported xlog unit — seam-and-panics (boot gap #5).
    s::local_process_control_file::call(false)?;

    // Give preloaded libraries a chance to request additional shared memory.
    // `process_shared_preload_libraries` is miscinit.c's own (ported) body; it
    // is called directly here (the established single-user pattern, like
    // `SetProcessingMode` above) rather than through the boot-driver seam. The
    // C runs it in `TopMemoryContext`; a transient context supplies the `Mcx`
    // its `SplitDirectoriesString` parse needs.
    {
        let spl_cx = mcx::MemoryContext::new("process_shared_preload_libraries");
        backend_utils_init_miscinit::process_shared_preload_libraries(spl_cx.mcx())?;
    }

    // Initialize MaxBackends, the postmaster child-slot table, and the
    // fast-path lock cache (sized from the GUCs).
    backend_utils_init_postinit_seams::initialize_max_backends::call()?;
    backend_postmaster_pmchild_seams::init_postmaster_child_slots::call();
    backend_utils_init_postinit_seams::initialize_fast_path_locks::call();

    // Now that loadable modules have had their chance, run their
    // shmem_request_hooks. Owned by the unported ipci unit — seam-and-panics
    // (boot gap #4: AIO/shmem sizing).
    s::process_shmem_requests::call()?;

    // Determine the value of any runtime-computed GUCs (shared_memory_size,
    // shared_memory_size_in_huge_pages). Owned by the unported GUC-funcs unit.
    s::initialize_shmem_gucs::call()?;

    // Process custom resource managers named in wal_consistency_checking.
    // Owned by the unported xlog unit — seam-and-panics (boot gap #5).
    s::initialize_wal_consistency_checking::call()?;

    // Single-user creates + inits shared memory itself (no postmaster did it).
    backend_storage_ipc_ipci_seams::create_shared_memory_and_semaphores::call();

    // Set the maximum number of safely-openable file descriptors.
    backend_storage_file_fd_seams::set_max_safe_fds::call()?;

    // Remember stand-alone backend startup time, roughly where the postmaster
    // records it. PgStartTime lives in the unported globals.c — fronted by a
    // seam.
    s::set_pg_start_time::call(backend_utils_adt_timestamp_seams::get_current_timestamp::call());

    // Create a per-backend PGPROC in shared memory; required before LWLocks.
    backend_utils_init_miscinit_seams::init_process::call()?;

    // Now that sufficient infrastructure is initialized, PostgresMain() does the
    // rest: BaseInit, InitPostgres(dbname, ..., username), SetProcessingMode,
    // and the InteractiveBackend read loop. PostgresMain is `-> !`.
    crate::main_loop::PostgresMain(Some(&dbname), Some(username))
}

/// `progname` (main.c global), via the seam (same accessor `guc.rs` uses).
fn progname() -> alloc::string::String {
    s::progname::call()
}
