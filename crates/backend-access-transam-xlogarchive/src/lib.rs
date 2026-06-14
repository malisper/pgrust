//! Port of `src/backend/access/transam/xlogarchive.c` (PostgreSQL 18.3).
//!
//! xlogarchive.c implements two related jobs:
//!
//!   * **restoring WAL from the archive** during recovery
//!     ([`RestoreArchivedFile`], [`ExecuteRecoveryCommand`],
//!     [`KeepFileRestoredFromArchive`]), and
//!   * **maintaining the `archive_status/` notification files**
//!     (`<XLOG>.ready` / `<XLOG>.done`) that drive the archiver
//!     ([`XLogArchiveNotify`], [`XLogArchiveNotifySeg`],
//!     [`XLogArchiveForceDone`], [`XLogArchiveCheckDone`],
//!     [`XLogArchiveIsBusy`], [`XLogArchiveIsReadyOrDone`],
//!     [`XLogArchiveIsReady`], [`XLogArchiveCleanup`]).
//!
//! All decision logic — the restore-command control flow with its
//! `not_available` fall-through, the archived-file size cross-check and its
//! standby-mode `DEBUG1`-vs-`FATAL` `elevel` choice, the signal interpretation
//! of `system()` results, the `.ready`/`.done` state machine, the archive
//! cutoff-point computation, and the status-file path/name construction — is
//! grounded here. Genuine externals cross loud-panic seams in their owners'
//! `-seams` crates; the raw POSIX `stat`/`unlink`/`rename`/`system`/`fflush`
//! syscalls (no PG TU owns them) go through `libc`.
//!
//! The `xlog_internal.h` filename/path helpers (`XLByteToSeg`, `XLogFileName`,
//! `StatusFilePath`, `IsTLHistoryFileName`, `XLOGDIR`) are pure
//! string/arithmetic and are grounded here, identical to the C macros.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use core::ffi::c_char;

use backend_utils_error::errno::current_errno;
use backend_utils_error::ereport;
use mcx::{Mcx, PgString};
use types_error::{ErrorLocation, PgResult};
use types_error::{DEBUG1, DEBUG2, DEBUG3, ERROR, FATAL, LOG, WARNING};
use types_core::primitive::{TimeLineID, XLogRecPtr, XLogSegNo};
use types_wal::xlog_consts::{ArchiveMode, RecoveryState, XLOGDIR};

use backend_access_transam_xlog_seams as xlog;
use backend_access_transam_xlogarchive_seams as inward;
use backend_access_transam_xlogrecovery_seams as xlogrecovery;
use backend_postmaster_pgarch_seams as pgarch;
use backend_postmaster_startup_seams as startup;
use backend_replication_walsender_seams as walsender;
use backend_storage_file_fd_seams as fd;
use backend_storage_file_fd_seams::CreateEmptyFileOutcome;
use backend_storage_file_seams as file;
use backend_storage_ipc_ipc_seams as ipc;
use backend_utils_activity_waitevent_seams as waitevent;
use backend_utils_init_small_seams as init_small;
use common_archive_seams as common_archive;
use common_wait_error_seams as wait_error;

/// `WAIT_EVENT_RESTORE_COMMAND` (pgstat wait-event id) — 0x08000032.
const WAIT_EVENT_RESTORE_COMMAND: u32 = 0x0800_0032;

/// `SIGTERM` from `<signal.h>` (15 on every PostgreSQL non-Windows target).
const SIGTERM: i32 = 15;

/// `ENOENT` from `<errno.h>` (2 on every PostgreSQL target).
const ENOENT: i32 = 2;

/// `.history` timeline-history filename suffix (`xlog_internal.h`).
const TL_HISTORY_SUFFIX: &str = ".history";

// ----- filename / path helpers (xlog_internal.h macros) -----------------

/// `XLogSegmentsPerXLogId(wal_segsz_bytes)` — `0x100000000 / wal_segsz_bytes`.
fn XLogSegmentsPerXLogId(wal_segsz_bytes: i32) -> u64 {
    0x1_0000_0000_u64 / wal_segsz_bytes as u64
}

/// `XLByteToSeg(xlrp, logSegNo, wal_segsz_bytes)`.
fn XLByteToSeg(xlrp: XLogRecPtr, wal_segsz_bytes: i32) -> XLogSegNo {
    xlrp / wal_segsz_bytes as u64
}

/// `XLogFileName(fname, tli, logSegNo, wal_segsz_bytes)`.
fn XLogFileName(tli: TimeLineID, logSegNo: XLogSegNo, wal_segsz_bytes: i32) -> String {
    let segments = XLogSegmentsPerXLogId(wal_segsz_bytes);
    format!(
        "{tli:08X}{:08X}{:08X}",
        logSegNo / segments,
        logSegNo % segments
    )
}

/// `StatusFilePath(path, xlog, suffix)`.
fn StatusFilePath(xlog: &str, suffix: &str) -> String {
    format!("{XLOGDIR}/archive_status/{xlog}{suffix}")
}

/// `is_upper_hex` — `[0-9A-F]` (the WAL filename alphabet).
fn is_upper_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'A'..=b'F').contains(&byte)
}

/// `IsTLHistoryFileName(fname)` — an 8-hex-digit timeline id followed by the
/// `.history` suffix.
fn IsTLHistoryFileName(fname: &str) -> bool {
    fname.len() == 8 + TL_HISTORY_SUFFIX.len()
        && fname.as_bytes()[..8].iter().copied().all(is_upper_hex)
        && &fname[8..] == TL_HISTORY_SUFFIX
}

// ----- raw POSIX syscall helpers ----------------------------------------

/// The result of a `stat(2)` probe — mirrors how xlogarchive.c distinguishes a
/// present file (and its `st_size`), an absent one (`errno == ENOENT`), and a
/// genuine stat error.
enum StatOutcome {
    Exists { st_size: i64 },
    NotFound,
    Error(i32),
}

/// `stat(path, &stat_buf)` (genuine POSIX syscall, no PG TU owns it).
fn raw_stat(path: &str) -> StatOutcome {
    let Ok(cpath) = std_cstring(path) else {
        return StatOutcome::Error(libc::ENAMETOOLONG);
    };
    let mut sb: libc::stat = unsafe { core::mem::zeroed() };
    let rc = unsafe { libc::stat(cpath.as_ptr(), &mut sb) };
    if rc == 0 {
        StatOutcome::Exists {
            st_size: sb.st_size as i64,
        }
    } else {
        let e = current_errno();
        if e == ENOENT {
            StatOutcome::NotFound
        } else {
            StatOutcome::Error(e)
        }
    }
}

/// `unlink(path)` — `Ok(())` on success, `Err(errno)` on failure.
fn raw_unlink(path: &str) -> Result<(), i32> {
    let cpath = std_cstring(path).map_err(|_| libc::ENAMETOOLONG)?;
    if unsafe { libc::unlink(cpath.as_ptr()) } == 0 {
        Ok(())
    } else {
        Err(current_errno())
    }
}

/// `system(cmd)` — run a shell command, returning the raw `wait(2)` status.
fn raw_system(cmd: &str) -> i32 {
    let Ok(ccmd) = std_cstring(cmd) else {
        // A NUL in the command can't form a C string; treat as shell failure.
        return -1;
    };
    unsafe { libc::system(ccmd.as_ptr()) }
}

/// `fflush(NULL)` — flush all stdio streams before forking a child.
fn raw_fflush_all() {
    unsafe {
        libc::fflush(core::ptr::null_mut());
    }
}

/// Build a NUL-terminated C string from a Rust `&str` without pulling in `std`'s
/// `CString` (we are `no_std`-friendly except for `alloc`). Returns `Err` if the
/// string contains an interior NUL.
fn std_cstring(s: &str) -> Result<Vec<c_char>, ()> {
    let bytes = s.as_bytes();
    if bytes.iter().any(|&b| b == 0) {
        return Err(());
    }
    let mut v: Vec<c_char> = Vec::with_capacity(bytes.len() + 1);
    for &b in bytes {
        v.push(b as c_char);
    }
    v.push(0);
    Ok(v)
}

// ----- error-helper plumbing --------------------------------------------

/// Source location stamped onto raised/emitted reports, mirroring the C
/// `__FILE__`/line macro expansion. (Message text, SQLSTATE, and level are
/// load-bearing for behavioral parity; the exact line number is not.)
fn here() -> ErrorLocation {
    ErrorLocation::new("../src/backend/access/transam/xlogarchive.c", 0, "")
}

/// Emit a non-throwing report (`ereport` at `< ERROR`).
fn emit(builder: backend_utils_error::ErrorBuilder) {
    let _ = builder.finish(here());
}

/// Attempt to retrieve the specified file from off-line archival storage.
///
/// If successful, returns `Ok(true)` with `path` set to the complete (temp)
/// path of the restored file; otherwise returns `Ok(false)` with `path` set to
/// the canonical on-line `pg_wal` name (which may or may not exist).
///
/// For fixed-size files the caller may pass the expected size as a crosscheck;
/// pass `expectedSize == 0` when unknown. When `cleanupEnabled` is false we
/// refrain from telling the restore command it may delete old segments.
///
/// `recovername`/`path` model the C in/out `char *` buffers. `path` is charged
/// to `mcx` on the archived-file success path; on the not-available fall-through
/// it is the canonical `pg_wal/<xlogfname>` name. Port of `RestoreArchivedFile`.
pub fn RestoreArchivedFile<'mcx>(
    mcx: Mcx<'mcx>,
    path: &mut Option<PgString<'mcx>>,
    xlogfname: &str,
    recovername: &str,
    expectedSize: i64,
    cleanupEnabled: bool,
) -> PgResult<bool> {
    'not_available: {
        // Ignore restore_command when not in archive recovery (crash recovery).
        if !xlogrecovery::archive_recovery_requested::call() {
            break 'not_available;
        }

        // In standby mode, restore_command might not be supplied.
        let recoveryRestoreCommand = match xlogrecovery::recovery_restore_command::call(mcx) {
            None => break 'not_available,
            Some(cmd) if cmd.as_str().is_empty() => break 'not_available,
            Some(cmd) => cmd,
        };

        // When doing archive recovery, we always prefer an archived log file
        // even if a same-named file exists in XLOGDIR. The copy-from-archive
        // filename is always the same, so long recoveries don't run out of disk.
        let xlogpath = format!("{XLOGDIR}/{recovername}");

        // Make sure there is no existing file named recovername.
        match raw_stat(&xlogpath) {
            StatOutcome::Exists { .. } => {
                if let Err(errno) = raw_unlink(&xlogpath) {
                    return Err(ereport(FATAL)
                        .with_saved_errno(errno)
                        .errcode_for_file_access()
                        .errmsg(format!("could not remove file \"{xlogpath}\": %m"))
                        .into_error()
                        .with_error_location(here()));
                }
            }
            StatOutcome::NotFound => {}
            StatOutcome::Error(errno) => {
                return Err(ereport(FATAL)
                    .with_saved_errno(errno)
                    .errcode_for_file_access()
                    .errmsg(format!("could not stat file \"{xlogpath}\": %m"))
                    .into_error()
                    .with_error_location(here()));
            }
        }

        // Calculate the archive file cutoff point for log-shipping replication.
        // If cleanup is disabled, initialise with InvalidXLogRecPtr's filename,
        // preventing deletion of any WAL files from the archive (alphabetic
        // sorting property of WAL names).
        let wal_segment_size = xlog::wal_segment_size::call();
        let lastRestartPointFname = if cleanupEnabled {
            let (restartRedoPtr, restartTli) = xlogrecovery::get_oldest_restart_point::call()?;
            let restartSegNo: XLogSegNo = XLByteToSeg(restartRedoPtr, wal_segment_size);
            let name = XLogFileName(restartTli, restartSegNo, wal_segment_size);
            // we shouldn't need anything earlier than last restart point
            debug_assert!(name.as_str() <= xlogfname);
            name
        } else {
            XLogFileName(0, 0, wal_segment_size)
        };

        // Build the restore command to execute.
        let xlogRestoreCmd = common_archive::build_restore_command::call(
            mcx,
            recoveryRestoreCommand.as_str(),
            Some(&xlogpath),
            Some(xlogfname),
            Some(&lastRestartPointFname),
        )?;

        emit(ereport(DEBUG3).errmsg_internal(format!(
            "executing restore command \"{}\"",
            xlogRestoreCmd.as_str()
        )));

        raw_fflush_all();
        waitevent::pgstat_report_wait_start::call(WAIT_EVENT_RESTORE_COMMAND);

        // PreRestoreCommand() informs the SIGTERM handler for the startup
        // process that it should proc_exit() right away.
        startup::pre_restore_command::call();

        // Copy xlog from archival storage to XLOGDIR.
        let rc = raw_system(xlogRestoreCmd.as_str());

        startup::post_restore_command::call();

        waitevent::pgstat_report_wait_end::call();
        // pfree(xlogRestoreCmd) — owned PgString dropped here.

        if rc == 0 {
            // Command apparently succeeded; make sure the file is really there
            // now and has the correct size.
            match raw_stat(&xlogpath) {
                StatOutcome::Exists { st_size } => {
                    if expectedSize > 0 && st_size != expectedSize {
                        // A partial file in standby mode is assumed to be still
                        // being copied to the archive — keep trying. Otherwise a
                        // wrong-sized file is FATAL.
                        let elevel = if xlog::standby_mode::call() && st_size < expectedSize {
                            DEBUG1
                        } else {
                            FATAL
                        };
                        ereport(elevel)
                            .errmsg(format!(
                                "archive file \"{xlogfname}\" has wrong size: {st_size} instead of {expectedSize}"
                            ))
                            .finish(here())?;
                        return Ok(false);
                    } else {
                        emit(ereport(LOG).errmsg(format!(
                            "restored log file \"{xlogfname}\" from archive"
                        )));
                        *path = Some(PgString::from_str_in(&xlogpath, mcx)?);
                        return Ok(true);
                    }
                }
                StatOutcome::NotFound => {
                    // stat failed (ENOENT)
                    ereport(LOG)
                        .with_saved_errno(ENOENT)
                        .errcode_for_file_access()
                        .errmsg(format!("could not stat file \"{xlogpath}\": %m"))
                        .errdetail(
                            "\"restore_command\" returned a zero exit status, but stat() failed.",
                        )
                        .finish(here())?;
                }
                StatOutcome::Error(errno) => {
                    // stat failed
                    ereport(FATAL)
                        .with_saved_errno(errno)
                        .errcode_for_file_access()
                        .errmsg(format!("could not stat file \"{xlogpath}\": %m"))
                        .errdetail(
                            "\"restore_command\" returned a zero exit status, but stat() failed.",
                        )
                        .finish(here())?;
                }
            }
        }

        // We rollforward UNTIL the restore fails, so failure here is part of the
        // process. If the failure was due to any signal, punt and abort
        // recovery. On SIGTERM, assume a fast-shutdown request and exit cleanly.
        // Hard shell errors such as "command not found" are fatal, too.
        if wait_error::wait_result_is_signal::call(rc, SIGTERM) {
            ipc::proc_exit::call(1);
        }

        let elevel = if wait_error::wait_result_is_any_signal::call(rc, true) {
            FATAL
        } else {
            DEBUG2
        };
        ereport(elevel)
            .errmsg(format!(
                "could not restore file \"{xlogfname}\" from archive: {}",
                wait_error::wait_result_to_str::call(rc)
            ))
            .finish(here())?;
    }

    // not_available:
    // If an archived file is not available, there might still be a version of
    // this file in XLOGDIR, so return that as the filename to open.
    *path = Some(PgString::from_str_in(&format!("{XLOGDIR}/{xlogfname}"), mcx)?);
    Ok(false)
}

/// Attempt to execute an external shell command during recovery.
///
/// `command` is the shell command, `commandName` a human-readable name used in
/// the logs. If `failOnSignal` is true and the command is killed by a signal a
/// `FATAL` error is raised; otherwise a `WARNING` is emitted. Used for
/// `recovery_end_command` and `archive_cleanup_command`.
///
/// Port of `ExecuteRecoveryCommand`.
pub fn ExecuteRecoveryCommand<'mcx>(
    mcx: Mcx<'mcx>,
    command: &str,
    commandName: &str,
    failOnSignal: bool,
    wait_event_info: u32,
) -> PgResult<()> {
    // Assert(command && commandName) — &str args are always non-NULL.

    // Calculate the archive file cutoff point for log-shipping replication.
    let wal_segment_size = xlog::wal_segment_size::call();
    let (restartRedoPtr, restartTli) = xlogrecovery::get_oldest_restart_point::call()?;
    let restartSegNo: XLogSegNo = XLByteToSeg(restartRedoPtr, wal_segment_size);
    let lastRestartPointFname = XLogFileName(restartTli, restartSegNo, wal_segment_size);

    // Construct the command to be executed.
    let xlogRecoveryCmd = common_percentrepl::replace_percent_placeholders(
        mcx,
        command,
        commandName,
        &[('r', Some(&lastRestartPointFname))],
    )?;

    emit(ereport(DEBUG3).errmsg_internal(format!("executing {commandName} \"{command}\"")));

    // Execute the constructed command.
    raw_fflush_all();
    waitevent::pgstat_report_wait_start::call(wait_event_info);
    let rc = raw_system(xlogRecoveryCmd.as_str());
    waitevent::pgstat_report_wait_end::call();

    // pfree(xlogRecoveryCmd) — owned PgString dropped here.

    if rc != 0 {
        // If the failure was due to any signal, punt and abort recovery. See
        // comments in RestoreArchivedFile().
        let elevel = if failOnSignal && wait_error::wait_result_is_any_signal::call(rc, true) {
            FATAL
        } else {
            WARNING
        };
        ereport(elevel)
            .errmsg(format!(
                "{commandName} \"{command}\": {}",
                wait_error::wait_result_to_str::call(rc)
            ))
            .finish(here())?;
    }

    Ok(())
}

/// A file was restored from the archive under a temporary filename (`path`), and
/// now we want to keep it. Rename it under the permanent filename in `pg_wal`
/// (`xlogfname`), replacing any existing file with the same name.
///
/// Port of `KeepFileRestoredFromArchive`.
pub fn KeepFileRestoredFromArchive(path: &str, xlogfname: &str) -> PgResult<()> {
    let mut reload = false;

    let xlogfpath = format!("{XLOGDIR}/{xlogfname}");

    if let StatOutcome::Exists { .. } = raw_stat(&xlogfpath) {
        // On non-Windows the old file is unlinked in place; same-size buffers,
        // so this never truncates. (The WIN32 rename-to-.deletedN dance is
        // omitted: this is a Unix build.)
        let oldpath = xlogfpath.clone();

        if let Err(errno) = raw_unlink(&oldpath) {
            return Err(ereport(FATAL)
                .with_saved_errno(errno)
                .errcode_for_file_access()
                .errmsg(format!("could not remove file \"{xlogfpath}\": %m"))
                .into_error()
                .with_error_location(here()));
        }
        reload = true;
    }

    file::durable_rename::call(path, &xlogfpath, ERROR)?;

    // Create .done file forcibly to prevent the restored segment from being
    // archived again later.
    if xlog::xlog_archive_mode::call() != ArchiveMode::Always {
        XLogArchiveForceDone(xlogfname)?;
    } else {
        XLogArchiveNotify(xlogfname)?;
    }

    // If the existing file was replaced, since walsenders might have it open,
    // request them to reload a currently-open segment.
    if reload {
        walsender::wal_snd_rqst_file_reload::call()?;
    }

    // Signal walsender that new WAL has arrived.
    walsender::wal_snd_wakeup::call(true, false)?;

    Ok(())
}

/// `XLogArchiveNotify` — create an archive notification file.
///
/// The name of the notification file is the message that will be picked up by
/// the archiver, e.g. we write `0000000100000001000000C6.ready` and the
/// archiver then knows to archive `XLOGDIR/0000000100000001000000C6`, then when
/// complete, rename it to `0000000100000001000000C6.done`.
///
/// Port of `XLogArchiveNotify`.
pub fn XLogArchiveNotify(xlog: &str) -> PgResult<()> {
    // Insert an otherwise empty file called <XLOG>.ready.
    let archiveStatusPath = StatusFilePath(xlog, ".ready");
    match fd::create_empty_file::call(&archiveStatusPath) {
        CreateEmptyFileOutcome::CreateFailed(errno) => {
            ereport(LOG)
                .with_saved_errno(errno)
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not create archive status file \"{archiveStatusPath}\": %m"
                ))
                .finish(here())?;
            return Ok(());
        }
        CreateEmptyFileOutcome::WriteFailed(errno) => {
            ereport(LOG)
                .with_saved_errno(errno)
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not write archive status file \"{archiveStatusPath}\": %m"
                ))
                .finish(here())?;
            return Ok(());
        }
        CreateEmptyFileOutcome::Ok => {}
    }

    // Timeline history files are given the highest archival priority. To ensure
    // the archiver picks them up as soon as possible, force a directory scan
    // next time it looks for a file to archive.
    if IsTLHistoryFileName(xlog) {
        pgarch::pg_arch_force_dir_scan::call();
    }

    // Notify archiver that it's got something to do.
    if init_small::is_under_postmaster::call() {
        pgarch::pg_arch_wakeup::call();
    }

    Ok(())
}

/// Convenience routine to notify using segment number representation of
/// filename. Port of `XLogArchiveNotifySeg`.
pub fn XLogArchiveNotifySeg(segno: XLogSegNo, tli: TimeLineID) -> PgResult<()> {
    debug_assert!(tli != 0);

    let xlog = XLogFileName(tli, segno, xlog::wal_segment_size::call());
    XLogArchiveNotify(&xlog)
}

/// `XLogArchiveForceDone` — emit notification forcibly that an XLOG segment file
/// has been successfully archived, by creating `<XLOG>.done` regardless of
/// whether `<XLOG>.ready` exists or not. Port of `XLogArchiveForceDone`.
pub fn XLogArchiveForceDone(xlog: &str) -> PgResult<()> {
    // Exit if already known done.
    let archiveDone = StatusFilePath(xlog, ".done");
    if let StatOutcome::Exists { .. } = raw_stat(&archiveDone) {
        return Ok(());
    }

    // If .ready exists, rename it to .done.
    let archiveReady = StatusFilePath(xlog, ".ready");
    if let StatOutcome::Exists { .. } = raw_stat(&archiveReady) {
        file::durable_rename::call(&archiveReady, &archiveDone, WARNING)?;
        return Ok(());
    }

    // Insert an otherwise empty file called <XLOG>.done.
    match fd::create_empty_file::call(&archiveDone) {
        CreateEmptyFileOutcome::CreateFailed(errno) => {
            ereport(LOG)
                .with_saved_errno(errno)
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not create archive status file \"{archiveDone}\": %m"
                ))
                .finish(here())?;
            Ok(())
        }
        CreateEmptyFileOutcome::WriteFailed(errno) => {
            ereport(LOG)
                .with_saved_errno(errno)
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not write archive status file \"{archiveDone}\": %m"
                ))
                .finish(here())?;
            Ok(())
        }
        CreateEmptyFileOutcome::Ok => Ok(()),
    }
}

/// `XLogArchiveCheckDone` — called when we are ready to delete or recycle an old
/// XLOG segment file or backup history file. Returns true if it is okay to
/// delete it. If not, make sure a `.ready` file exists and return false.
///
/// Port of `XLogArchiveCheckDone`.
pub fn XLogArchiveCheckDone(xlog: &str) -> PgResult<bool> {
    // The file is always deletable if archive_mode is "off".
    if !xlog_archiving_active() {
        return Ok(true);
    }

    // During archive recovery, the file is deletable if archive_mode is not
    // "always".
    if !xlog_archiving_always()
        && xlogrecovery::get_recovery_state::call()? == RecoveryState::Archive
    {
        return Ok(true);
    }

    // At this point we are either a primary with archive_mode "on"/"always", or
    // a standby with archive_mode "always".

    // First check for .done --- this means archiver is done with it.
    let archiveStatusPath = StatusFilePath(xlog, ".done");
    if let StatOutcome::Exists { .. } = raw_stat(&archiveStatusPath) {
        return Ok(true);
    }

    // Check for .ready --- this means archiver is still busy with it.
    let archiveStatusPath = StatusFilePath(xlog, ".ready");
    if let StatOutcome::Exists { .. } = raw_stat(&archiveStatusPath) {
        return Ok(false);
    }

    // Race condition --- maybe archiver just finished, so recheck.
    let archiveStatusPath = StatusFilePath(xlog, ".done");
    if let StatOutcome::Exists { .. } = raw_stat(&archiveStatusPath) {
        return Ok(true);
    }

    // Retry creation of the .ready file.
    XLogArchiveNotify(xlog)?;
    Ok(false)
}

/// `XLogArchiveIsBusy` — check to see if an XLOG segment file is still
/// unarchived. Almost the inverse of [`XLogArchiveCheckDone`]: we don't recreate
/// the `.ready` file, and if the file is already gone then it's not busy.
///
/// Port of `XLogArchiveIsBusy`.
pub fn XLogArchiveIsBusy(xlog: &str) -> bool {
    // First check for .done --- this means archiver is done with it.
    let archiveStatusPath = StatusFilePath(xlog, ".done");
    if let StatOutcome::Exists { .. } = raw_stat(&archiveStatusPath) {
        return false;
    }

    // Check for .ready --- this means archiver is still busy with it.
    let archiveStatusPath = StatusFilePath(xlog, ".ready");
    if let StatOutcome::Exists { .. } = raw_stat(&archiveStatusPath) {
        return true;
    }

    // Race condition --- maybe archiver just finished, so recheck.
    let archiveStatusPath = StatusFilePath(xlog, ".done");
    if let StatOutcome::Exists { .. } = raw_stat(&archiveStatusPath) {
        return false;
    }

    // Check whether the WAL file has been removed by checkpoint, which implies
    // it has already been archived and explains why we can't see a status file.
    let archiveStatusPath = format!("{XLOGDIR}/{xlog}");
    if let StatOutcome::NotFound = raw_stat(&archiveStatusPath) {
        return false;
    }

    true
}

/// `XLogArchiveIsReadyOrDone` — check to see if an XLOG segment file has a
/// `.ready` or `.done` file. Similar to [`XLogArchiveIsBusy`], but returns true
/// if the file is already archived or is about to be archived. Only used at
/// recovery. Port of `XLogArchiveIsReadyOrDone`.
pub fn XLogArchiveIsReadyOrDone(xlog: &str) -> bool {
    // First check for .done --- this means archiver is done with it.
    let archiveStatusPath = StatusFilePath(xlog, ".done");
    if let StatOutcome::Exists { .. } = raw_stat(&archiveStatusPath) {
        return true;
    }

    // Check for .ready --- this means archiver is still busy with it.
    let archiveStatusPath = StatusFilePath(xlog, ".ready");
    if let StatOutcome::Exists { .. } = raw_stat(&archiveStatusPath) {
        return true;
    }

    // Race condition --- maybe archiver just finished, so recheck.
    let archiveStatusPath = StatusFilePath(xlog, ".done");
    if let StatOutcome::Exists { .. } = raw_stat(&archiveStatusPath) {
        return true;
    }

    false
}

/// `XLogArchiveIsReady` — check to see if an XLOG segment file has an archive
/// notification (`.ready`) file. Port of `XLogArchiveIsReady`.
pub fn XLogArchiveIsReady(xlog: &str) -> bool {
    let archiveStatusPath = StatusFilePath(xlog, ".ready");
    matches!(raw_stat(&archiveStatusPath), StatOutcome::Exists { .. })
}

/// `XLogArchiveCleanup` — cleanup archive notification file(s) for a particular
/// xlog segment. Port of `XLogArchiveCleanup`.
pub fn XLogArchiveCleanup(xlog: &str) {
    // Remove the .done file (failure ignored, as in C).
    let archiveStatusPath = StatusFilePath(xlog, ".done");
    let _ = raw_unlink(&archiveStatusPath);

    // Remove the .ready file if present --- normally it shouldn't be.
    let archiveStatusPath = StatusFilePath(xlog, ".ready");
    let _ = raw_unlink(&archiveStatusPath);
}

/// `XLogArchivingActive()` — `XLogArchiveMode > ARCHIVE_MODE_OFF`.
fn xlog_archiving_active() -> bool {
    xlog::xlog_archive_mode::call() > ArchiveMode::Off
}

/// `XLogArchivingAlways()` — `XLogArchiveMode == ARCHIVE_MODE_ALWAYS`.
fn xlog_archiving_always() -> bool {
    xlog::xlog_archive_mode::call() == ArchiveMode::Always
}

// ----- inward seam wrappers ----------------------------------------------

/// `restore_archived_history_file(mcx, xlogfname)` inward seam — the
/// `RestoreArchivedFile(path, xlogfname, "RECOVERYHISTORY", 0, false)` call used
/// by timeline.c. Returns the restored temp path on success, `None` otherwise.
fn restore_archived_history_file<'mcx>(
    mcx: Mcx<'mcx>,
    xlogfname: &str,
) -> PgResult<Option<PgString<'mcx>>> {
    let mut path: Option<PgString<'mcx>> = None;
    if RestoreArchivedFile(mcx, &mut path, xlogfname, "RECOVERYHISTORY", 0, false)? {
        Ok(path)
    } else {
        Ok(None)
    }
}

/// `keep_file_restored_from_archive(path, xlogfname)` inward seam.
fn keep_file_restored_from_archive(path: &str, xlogfname: &str) -> PgResult<()> {
    KeepFileRestoredFromArchive(path, xlogfname)
}

/// `xlog_archive_force_done(fname)` inward seam.
fn xlog_archive_force_done(fname: String) -> PgResult<()> {
    XLogArchiveForceDone(&fname)
}

/// `xlog_archive_notify(fname)` inward seam.
fn xlog_archive_notify(fname: String) -> PgResult<()> {
    XLogArchiveNotify(&fname)
}

/// Install this crate's inward seams. Wired into `seams-init::init_all`.
pub fn init_seams() {
    inward::restore_archived_history_file::set(restore_archived_history_file);
    inward::keep_file_restored_from_archive::set(keep_file_restored_from_archive);
    inward::xlog_archive_force_done::set(xlog_archive_force_done);
    inward::xlog_archive_notify::set(xlog_archive_notify);
}
