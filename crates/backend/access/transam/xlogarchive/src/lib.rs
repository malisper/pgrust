//! xlogarchive.c. Recovery-side entry points (RestoreArchivedFile,
//! ExecuteRecoveryCommand, KeepFileRestoredFromArchive) are callable but have
//! no in-tree caller yet (recovery core owns them); Windows-only arms and
//! walsender wakeups (walsender unported: no walsender thread can exist, so
//! the wakeups are exact no-ops) are absent. BuildRestoreCommand is re-exported
//! from the `archive` crate (C home src/common/archive.c).

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use elog::ereport;
use init_small::globals as g;
use transam_xlog::{
    IsTLHistoryFileName, StatusFilePath, XLByteToSeg, XLogArchivingActive, XLogArchivingAlways,
    XLogFileName, GetRecoveryState, RECOVERY_STATE_ARCHIVE, XLOGDIR,
};
use types_error::{ErrorLocation, PgResult, DEBUG2, DEBUG3, ERROR, FATAL, LOG, WARNING};

#[cfg(test)]
mod tests;

const PG_WAIT_IPC: u32 = 0x0800_0000;
const WAIT_EVENT_RESTORE_COMMAND: u32 = PG_WAIT_IPC + 50;

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("xlogarchive.c", 0, funcname)
}

fn saved_errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

fn io_errno(e: &std::io::Error) -> i32 {
    e.raw_os_error().unwrap_or(0)
}

pub use archive::BuildRestoreCommand;

pub fn RestoreArchivedFile(
    xlogfname: &str,
    recovername: &str,
    expected_size: i64,
    cleanup_enabled: bool,
) -> PgResult<Option<String>> {
    if !xlogrecovery_seams::archive_recovery_requested::call() {
        return Ok(None);
    }

    // Owner-unported guard mirrors C's NULL restore_command.
    let restore_command = if guc_tables::vars::recoveryRestoreCommand.installed() {
        guc_tables::vars::recoveryRestoreCommand.read().unwrap_or_default()
    } else {
        String::new()
    };
    if restore_command.is_empty() {
        return Ok(None);
    }

    let xlogpath = format!("{XLOGDIR}/{recovername}");

    match std::fs::metadata(&xlogpath) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            ereport(FATAL)
                .with_saved_errno(io_errno(&e))
                .errcode_for_file_access()
                .errmsg(format!("could not stat file \"{xlogpath}\": %m"))
                .finish(loc("RestoreArchivedFile"))?;
        }
        Ok(_) => {
            if let Err(e) = std::fs::remove_file(&xlogpath) {
                ereport(FATAL)
                    .with_saved_errno(io_errno(&e))
                    .errcode_for_file_access()
                    .errmsg(format!("could not remove file \"{xlogpath}\": %m"))
                    .finish(loc("RestoreArchivedFile"))?;
            }
        }
    }

    let wal_segsz = transam_xlog::wal_segment_size();
    let last_restart_point_fname = if cleanup_enabled {
        let (restart_redo_ptr, restart_tli) = transam_xlog::GetOldestRestartPoint()?;
        let restart_seg_no = XLByteToSeg(restart_redo_ptr, wal_segsz);
        let fname = XLogFileName(restart_tli, restart_seg_no, wal_segsz);
        debug_assert!(fname.as_str() <= xlogfname);
        fname
    } else {
        XLogFileName(0, 0, wal_segsz)
    };

    let xlog_restore_cmd =
        BuildRestoreCommand(&restore_command, &xlogpath, xlogfname, &last_restart_point_fname)?;

    ereport(DEBUG3)
        .errmsg_internal(format!("executing restore command \"{xlog_restore_cmd}\""))
        .finish(loc("RestoreArchivedFile"))?;

    waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_RESTORE_COMMAND);
    postmaster_startup::PreRestoreCommand();
    let rc = wait_error::system(&xlog_restore_cmd);
    postmaster_startup::PostRestoreCommand();
    waitevent_seams::pgstat_report_wait_end::call();

    if rc == 0 {
        match std::fs::metadata(&xlogpath) {
            Ok(st) => {
                if expected_size > 0 && st.len() as i64 != expected_size {
                    // StandbyMode is unported (always false): C's partial-file
                    // DEBUG1 arm is unreachable, wrong size is FATAL.
                    ereport(FATAL)
                        .errmsg(format!(
                            "archive file \"{xlogfname}\" has wrong size: {} instead of {expected_size}",
                            st.len()
                        ))
                        .finish(loc("RestoreArchivedFile"))?;
                    return Ok(None);
                }
                ereport(LOG)
                    .errmsg(format!("restored log file \"{xlogfname}\" from archive"))
                    .finish(loc("RestoreArchivedFile"))?;
                return Ok(Some(xlogpath));
            }
            Err(e) => {
                let elevel =
                    if e.kind() == std::io::ErrorKind::NotFound { LOG } else { FATAL };
                ereport(elevel)
                    .with_saved_errno(io_errno(&e))
                    .errcode_for_file_access()
                    .errmsg(format!("could not stat file \"{xlogpath}\": %m"))
                    .errdetail(
                        "\"restore_command\" returned a zero exit status, but stat() failed.",
                    )
                    .finish(loc("RestoreArchivedFile"))?;
            }
        }
    }

    if wait_error::wait_result_is_signal(rc, libc::SIGTERM) {
        ipc::proc_exit(1, g::MyProcPid());
    }

    let elevel = if wait_error::wait_result_is_any_signal(rc, true) { FATAL } else { DEBUG2 };
    ereport(elevel)
        .errmsg(format!(
            "could not restore file \"{xlogfname}\" from archive: {}",
            wait_error::wait_result_to_str(rc)
        ))
        .finish(loc("RestoreArchivedFile"))?;

    Ok(None)
}

pub fn ExecuteRecoveryCommand(
    command: &str,
    command_name: &str,
    fail_on_signal: bool,
    wait_event_info: u32,
) -> PgResult<()> {
    debug_assert!(!command.is_empty() && !command_name.is_empty());

    let wal_segsz = transam_xlog::wal_segment_size();
    let (restart_redo_ptr, restart_tli) = transam_xlog::GetOldestRestartPoint()?;
    let restart_seg_no = XLByteToSeg(restart_redo_ptr, wal_segsz);
    let last_restart_point_fname = XLogFileName(restart_tli, restart_seg_no, wal_segsz);

    let xlog_recovery_cmd = percentrepl::replace_percent_placeholders(
        command,
        command_name,
        &[('r', Some(&last_restart_point_fname))],
    )?;

    ereport(DEBUG3)
        .errmsg_internal(format!("executing {command_name} \"{command}\""))
        .finish(loc("ExecuteRecoveryCommand"))?;

    waitevent_seams::pgstat_report_wait_start::call(wait_event_info);
    let rc = wait_error::system(&xlog_recovery_cmd);
    waitevent_seams::pgstat_report_wait_end::call();

    if rc != 0 {
        let elevel = if fail_on_signal && wait_error::wait_result_is_any_signal(rc, true) {
            FATAL
        } else {
            WARNING
        };
        ereport(elevel)
            .errmsg(format!(
                "{command_name} \"{command}\": {}",
                wait_error::wait_result_to_str(rc)
            ))
            .finish(loc("ExecuteRecoveryCommand"))?;
    }
    Ok(())
}

pub fn KeepFileRestoredFromArchive(path: &str, xlogfname: &str) -> PgResult<()> {
    let xlogfpath = format!("{XLOGDIR}/{xlogfname}");

    if std::fs::metadata(&xlogfpath).is_ok() {
        if let Err(e) = std::fs::remove_file(&xlogfpath) {
            ereport(FATAL)
                .with_saved_errno(io_errno(&e))
                .errcode_for_file_access()
                .errmsg(format!("could not remove file \"{xlogfpath}\": %m"))
                .finish(loc("KeepFileRestoredFromArchive"))?;
        }
    }

    fd::durable_rename(path, &xlogfpath, ERROR)?;

    if !XLogArchivingAlways() {
        XLogArchiveForceDone(xlogfname)?;
    } else {
        XLogArchiveNotify(xlogfname)?;
    }

    // WalSndWakeup(true, false): walsender unported (see module doc).
    Ok(())
}

pub fn XLogArchiveNotify(xlog: &str) -> PgResult<()> {
    let archive_status_path = StatusFilePath(xlog, ".ready");
    let fdnum = fd::AllocateFile(&archive_status_path, "w")?;
    if fdnum < 0 {
        ereport(LOG)
            .with_saved_errno(saved_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not create archive status file \"{archive_status_path}\": %m"))
            .finish(loc("XLogArchiveNotify"))?;
        return Ok(());
    }
    if fd::FreeFile(fdnum)? != 0 {
        ereport(LOG)
            .with_saved_errno(saved_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not write archive status file \"{archive_status_path}\": %m"))
            .finish(loc("XLogArchiveNotify"))?;
        return Ok(());
    }

    if IsTLHistoryFileName(xlog) {
        pgarch::PgArchForceDirScan();
    }

    if g::IsUnderPostmaster() {
        pgarch::PgArchWakeup();
    }
    Ok(())
}

pub fn XLogArchiveNotifySeg(segno: types_core::XLogSegNo, tli: types_core::TimeLineID) -> PgResult<()> {
    debug_assert!(tli != 0);
    XLogArchiveNotify(&XLogFileName(tli, segno, transam_xlog::wal_segment_size()))
}

pub fn XLogArchiveForceDone(xlog: &str) -> PgResult<()> {
    let archive_done = StatusFilePath(xlog, ".done");
    if std::fs::metadata(&archive_done).is_ok() {
        return Ok(());
    }

    let archive_ready = StatusFilePath(xlog, ".ready");
    if std::fs::metadata(&archive_ready).is_ok() {
        fd::durable_rename(&archive_ready, &archive_done, WARNING)?;
        return Ok(());
    }

    let fdnum = fd::AllocateFile(&archive_done, "w")?;
    if fdnum < 0 {
        ereport(LOG)
            .with_saved_errno(saved_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not create archive status file \"{archive_done}\": %m"))
            .finish(loc("XLogArchiveForceDone"))?;
        return Ok(());
    }
    if fd::FreeFile(fdnum)? != 0 {
        ereport(LOG)
            .with_saved_errno(saved_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not write archive status file \"{archive_done}\": %m"))
            .finish(loc("XLogArchiveForceDone"))?;
        return Ok(());
    }
    Ok(())
}

pub fn XLogArchiveCheckDone(xlog: &str) -> PgResult<bool> {
    if !XLogArchivingActive() {
        return Ok(true);
    }

    if !XLogArchivingAlways() && GetRecoveryState() == RECOVERY_STATE_ARCHIVE {
        return Ok(true);
    }

    if std::fs::metadata(StatusFilePath(xlog, ".done")).is_ok() {
        return Ok(true);
    }

    if std::fs::metadata(StatusFilePath(xlog, ".ready")).is_ok() {
        return Ok(false);
    }

    if std::fs::metadata(StatusFilePath(xlog, ".done")).is_ok() {
        return Ok(true);
    }

    XLogArchiveNotify(xlog)?;
    Ok(false)
}

pub fn XLogArchiveIsBusy(xlog: &str) -> bool {
    if std::fs::metadata(StatusFilePath(xlog, ".done")).is_ok() {
        return false;
    }
    if std::fs::metadata(StatusFilePath(xlog, ".ready")).is_ok() {
        return true;
    }
    if std::fs::metadata(StatusFilePath(xlog, ".done")).is_ok() {
        return false;
    }

    match std::fs::metadata(format!("{XLOGDIR}/{xlog}")) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        _ => true,
    }
}

pub fn XLogArchiveIsReadyOrDone(xlog: &str) -> bool {
    if std::fs::metadata(StatusFilePath(xlog, ".done")).is_ok() {
        return true;
    }
    if std::fs::metadata(StatusFilePath(xlog, ".ready")).is_ok() {
        return true;
    }
    std::fs::metadata(StatusFilePath(xlog, ".done")).is_ok()
}

pub fn XLogArchiveIsReady(xlog: &str) -> bool {
    std::fs::metadata(StatusFilePath(xlog, ".ready")).is_ok()
}

pub fn XLogArchiveCleanup(xlog: &str) {
    let _ = std::fs::remove_file(StatusFilePath(xlog, ".done"));
    let _ = std::fs::remove_file(StatusFilePath(xlog, ".ready"));
}

pub fn init_seams() {
    use xlogarchive_seams as s;
    s::xlog_archive_notify::set(XLogArchiveNotify);
    s::xlog_archive_notify_seg::set(XLogArchiveNotifySeg);
    s::xlog_archiving_active::set(XLogArchivingActive);
    s::restore_archived_file::set(RestoreArchivedFile);
    s::xlog_archive_check_done::set(XLogArchiveCheckDone);
    s::xlog_archive_is_busy::set(XLogArchiveIsBusy);
    s::keep_file_restored_from_archive::set(KeepFileRestoredFromArchive);
    s::xlog_archive_cleanup::set(XLogArchiveCleanup);
    s::xlog_archive_is_ready::set(XLogArchiveIsReady);
    s::xlog_archive_is_ready_or_done::set(XLogArchiveIsReadyOrDone);
    s::execute_recovery_command::set(ExecuteRecoveryCommand);
}
