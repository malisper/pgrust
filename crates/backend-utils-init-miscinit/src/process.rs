//! Common process startup, data-directory checks, and version validation
//! (`miscinit.c`): `InitPostmasterChild`, `InitStandaloneProcess`,
//! `SwitchToSharedLatch`, `InitProcessLocalLatch`, `SwitchBackToLocalLatch`,
//! `checkDataDir`, `ChangeToDataDir`, `ValidatePgVersion`.
//!
//! These are call sequences into other subsystems; the genuine externals cross
//! their owners' seams. `LocalLatchData` is `miscinit.c`'s own backend-private
//! latch, owned here.

use std::cell::Cell;

use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, FATAL};
use types_storage::latch::LatchHandle;

use crate::{SetMyBackendType, PG_VERSION};
use types_core::BackendType;

thread_local! {
    /// `static Latch LocalLatchData;` — the process-local latch the backend uses
    /// before it owns a PGPROC. Allocated once (via the latch registry) and
    /// reused, matching the C file-scope storage.
    static LOCAL_LATCH: Cell<Option<LatchHandle>> = const { Cell::new(None) };
}

/// The handle for this backend's `LocalLatchData`, allocating it on first use.
fn local_latch() -> LatchHandle {
    LOCAL_LATCH.with(|c| {
        if let Some(h) = c.get() {
            return h;
        }
        let h = backend_storage_ipc_latch::allocate_latch();
        c.set(Some(h));
        h
    })
}

/// `InitPostmasterChild()` (`miscinit.c:95`): initialization common to all
/// postmaster children. WIN32/EXEC_BACKEND arms are compiled out of this build.
pub fn InitPostmasterChild() -> PgResult<()> {
    // we are a postmaster subprocess now
    backend_utils_init_small::globals::SetIsUnderPostmaster(true);

    backend_utils_init_small_seams::init_process_globals::call()?;

    // We don't want the postmaster's proc_exit() handlers.
    backend_storage_ipc_seams::on_exit_reset::call();

    // Initialize process-local latch support.
    backend_storage_ipc_waiteventset_seams::initialize_wait_event_support::call()?;
    InitProcessLocalLatch();
    backend_storage_ipc_latch::InitializeLatchWaitSet()?;

    // Make this process a group leader (setsid), so the postmaster can signal
    // child process groups too.
    if unsafe { libc::setsid() } < 0 {
        return Err(PgError::new(
            FATAL,
            format!("setsid() failed: {}", std::io::Error::last_os_error()),
        ));
    }

    // Every postmaster child responds promptly to SIGQUIT: remove it from
    // BlockSig and install SignalHandlerForCrashExit.
    backend_postmaster_interrupt_seams::install_crash_exit_sigquit_handler::call()?;

    // Request a signal if the postmaster dies, if possible.
    backend_storage_ipc_pmsignal_seams::postmaster_death_signal_init::call()?;

    // Don't give the postmaster-death pipe to subprograms we execute.
    backend_storage_ipc_pmsignal_seams::set_postmaster_death_watch_cloexec::call()?;

    Ok(())
}

/// `InitStandaloneProcess(argv0)` (`miscinit.c:174`): initialization for a
/// standalone (bootstrap / single-user) process.
pub fn InitStandaloneProcess(argv0: &str) -> PgResult<()> {
    debug_assert!(!backend_utils_init_small::globals::IsPostmasterEnvironment());

    SetMyBackendType(BackendType::StandaloneBackend);

    backend_utils_init_small_seams::init_process_globals::call()?;

    // Initialize process-local latch support.
    backend_storage_ipc_waiteventset_seams::initialize_wait_event_support::call()?;
    InitProcessLocalLatch();
    backend_storage_ipc_latch::InitializeLatchWaitSet()?;

    // Initialize signal mask (no SIGQUIT unblock or default handler here).
    backend_postmaster_interrupt_seams::pqinitmask_set_blocksig::call()?;

    // Compute paths, no postmaster to inherit from.
    backend_common_exec_seams::resolve_standalone_paths::call(argv0)
}

/// `SwitchToSharedLatch()` (`miscinit.c:214`): switch `MyLatch` from the
/// process-local latch to the PGPROC's shared latch.
pub fn SwitchToSharedLatch() -> PgResult<()> {
    debug_assert_eq!(
        backend_storage_ipc_latch::my_latch(),
        Some(local_latch())
    );
    let proc_latch = backend_storage_lmgr_proc_seams::my_proc_latch::call();
    backend_storage_ipc_latch::set_my_latch(Some(proc_latch));

    // If FeBeWaitSet exists, repoint its latch event at the new MyLatch.
    backend_libpq_pqcomm_seams::modify_fe_be_wait_set_latch::call(proc_latch)?;

    // Set the shared latch (a bit of care can't hurt).
    backend_storage_ipc_latch::SetLatch(proc_latch);
    Ok(())
}

/// `InitProcessLocalLatch()` (`miscinit.c:234`): point `MyLatch` at the
/// process-local `LocalLatchData` and initialize it.
pub fn InitProcessLocalLatch() {
    let latch = local_latch();
    backend_storage_ipc_latch::set_my_latch(Some(latch));
    backend_storage_ipc_latch::InitLatch(latch);
}

/// `SwitchBackToLocalLatch()` (`miscinit.c:242`): switch `MyLatch` back to the
/// process-local latch.
pub fn SwitchBackToLocalLatch() -> PgResult<()> {
    let latch = local_latch();
    debug_assert!(backend_storage_ipc_latch::my_latch() != Some(latch));
    backend_storage_ipc_latch::set_my_latch(Some(latch));

    backend_libpq_pqcomm_seams::modify_fe_be_wait_set_latch::call(latch)?;

    backend_storage_ipc_latch::SetLatch(latch);
    Ok(())
}

/// `checkDataDir()` (`miscinit.c:346`): validate the data directory and set the
/// file/directory create modes. WIN32/CYGWIN ownership/permission checks are
/// retained (this build targets Unix).
pub fn checkDataDir() -> PgResult<()> {
    let data_dir = backend_utils_init_small::globals::DataDir().expect("DataDir set");

    let stat_buf = match std::fs::metadata(&data_dir) {
        Ok(m) => m,
        Err(e) => {
            if e.raw_os_error() == Some(2) {
                // ENOENT
                return Err(PgError::new(
                    FATAL,
                    format!("data directory \"{data_dir}\" does not exist"),
                ));
            }
            return Err(PgError::new(
                FATAL,
                format!("could not read permissions of directory \"{data_dir}\": {e}"),
            ));
        }
    };

    // eventual chdir would fail anyway, but let's test ...
    if !stat_buf.is_dir() {
        return Err(PgError::new(
            FATAL,
            format!("specified data directory \"{data_dir}\" is not a directory"),
        )
        .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    use std::os::unix::fs::MetadataExt;
    use std::os::unix::fs::PermissionsExt;

    // Check that the directory belongs to my userid; if not, reject. This is an
    // essential part of the interlock that prevents two postmasters from
    // starting in the same directory (see CreateLockFile). Do not weaken it.
    let geteuid = unsafe { libc::geteuid() };
    if stat_buf.uid() != geteuid {
        return Err(PgError::new(
            FATAL,
            format!("data directory \"{data_dir}\" has wrong ownership"),
        )
        .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    // Check permissions. Only 0700 and 0750 are allowed; 0750 grants group
    // read/execute. `PG_MODE_MASK_GROUP = S_IWGRP | S_IRWXO` (file_perm.h): any
    // group-write or world bit makes the directory invalid.
    const PG_MODE_MASK_GROUP: u32 = 0o027;
    let mode = stat_buf.permissions().mode();
    if mode & PG_MODE_MASK_GROUP != 0 {
        return Err(PgError::new(
            FATAL,
            format!("data directory \"{data_dir}\" has invalid permissions"),
        )
        .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    // Reset creation modes and mask based on the data directory mode, set the
    // data_directory_mode GUC (file_perm.c owns the create-mode globals).
    backend_storage_file_fileperm_seams::set_data_directory_create_perm::call(mode)?;

    // Check for PG_VERSION.
    ValidatePgVersion(&data_dir)
}

/// `ChangeToDataDir()` (`miscinit.c:459`): chdir into `DataDir`.
pub fn ChangeToDataDir() -> PgResult<()> {
    let data_dir = backend_utils_init_small::globals::DataDir().expect("DataDir set");
    if std::env::set_current_dir(&data_dir).is_err() {
        let e = std::io::Error::last_os_error();
        return Err(PgError::new(
            FATAL,
            format!("could not change directory to \"{data_dir}\": {e}"),
        ));
    }
    Ok(())
}

/// `ValidatePgVersion(path)` (`miscinit.c:1769`): verify the `PG_VERSION` file
/// in `path` indicates a data version compatible with this program.
pub fn ValidatePgVersion(path: &str) -> PgResult<()> {
    let my_version_string = PG_VERSION;
    // my_major = strtol(my_version_string, ...)
    let my_major = leading_i64(my_version_string);

    let full_path = format!("{path}/PG_VERSION");

    let contents = match std::fs::read_to_string(&full_path) {
        Ok(s) => s,
        Err(e) => {
            if e.raw_os_error() == Some(2) {
                // ENOENT
                return Err(PgError::new(
                    FATAL,
                    format!("\"{path}\" is not a valid data directory"),
                )
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
            return Err(PgError::new(
                FATAL,
                format!("could not open file \"{full_path}\": {e}"),
            ));
        }
    };

    // fscanf(file, "%63s", ...) — first whitespace-delimited token, max 63 chars.
    let file_version_string: String = contents
        .split_whitespace()
        .next()
        .unwrap_or("")
        .chars()
        .take(63)
        .collect();

    // ret != 1 || endptr == file_version_string: no leading digit parsed.
    let has_leading_digit = file_version_string
        .trim_start()
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_digit() || c == '+' || c == '-');
    if file_version_string.is_empty() || !has_leading_digit {
        return Err(PgError::new(
            FATAL,
            format!("\"{path}\" is not a valid data directory"),
        )
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }
    let file_major = leading_i64(&file_version_string);

    if my_major != file_major {
        return Err(PgError::new(FATAL, "database files are incompatible with server")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }
    Ok(())
}

/// `strtol(s, &endptr, 10)` for the leading integer.
fn leading_i64(s: &str) -> i64 {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut sign = 1i64;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        if bytes[i] == b'-' {
            sign = -1;
        }
        i += 1;
    }
    let mut val = 0i64;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        val = val.saturating_mul(10).saturating_add((bytes[i] - b'0') as i64);
        i += 1;
    }
    sign.saturating_mul(val)
}
