//! Online base-backup control: do_pg_backup_start / do_pg_backup_stop and the
//! abort/cleanup family from access/transam/xlog.c ("Online backup" section,
//! xlog.c:8814-9505). The backup_label / tablespace_map formatting lives in the
//! xlogbackup crate (build_backup_content); this module is the control flow.

#![allow(non_snake_case)]

use std::io::Write;
use std::sync::atomic::Ordering::Relaxed;

use elog::ereport;
use types_core::{TimeLineID, XLogRecPtr, XLogSegNo};
use types_error::{
    ErrorLocation, PgResult, DEBUG2, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, NOTICE, WARNING,
};
use lwlock::LW_SHARED;
use xlogbackup::BackupState;

use crate::control_file::control_file;
use crate::ctl::{ControlFileLock, XLogCtl};
use crate::insert::{RecoveryInProgress, WALInsertLockAcquireExclusive, WALInsertLockRelease};
use crate::{
    wal_segment_size, RequestXLogSwitch, XLByteToSeg, XLogIsNeeded, XLogSegmentOffset,
    XLogSegmentsPerXLogId, CHECKPOINT_FORCE, CHECKPOINT_IMMEDIATE, CHECKPOINT_WAIT, RM_XLOG_ID,
    XLOGDIR, XLOG_BACKUP_END,
};

/// MAXPGPATH (pg_config_manual.h).
const MAXPGPATH: usize = 1024;

/// PG_TBLSPC_DIR (storage/fd.h) — the per-cluster tablespace symlink dir.
const PG_TBLSPC_DIR: &str = "pg_tblspc";

/// ARCHIVE_MODE_OFF / ARCHIVE_MODE_ALWAYS (access/xlog.h) — archive_mode enum GUC.
const ARCHIVE_MODE_OFF: i32 = 0;
const ARCHIVE_MODE_ALWAYS: i32 = 2;

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

/// `enum SessionBackupState` (xlog.h) — backend-local backup status.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SessionBackupState {
    None,
    Running,
}

// tablespaceinfo (basebackup.h) — homed in xlogbackup (shared with the
// base-backup driver + sink chain, below the backup layer).
pub use xlogbackup::TablespaceInfo;

// static SessionBackupState sessionBackupState = SESSION_BACKUP_NONE (xlog.c:416).
std::thread_local! {
    static SESSION_BACKUP_STATE: core::cell::Cell<SessionBackupState> =
        const { core::cell::Cell::new(SessionBackupState::None) };
}

// static bool already_done in register_persistent_abort_backup_handler (xlog.c).
std::thread_local! {
    static ABORT_HANDLER_REGISTERED: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
}

/// (pg_time_t) time(NULL) — wall-clock seconds for starttime / stoptime.
fn wallclock_time() -> types_core::pg_time_t {
    // DST P2 (contract §1.2): SystemTime -> pg_clock::wall_secs().
    pg_clock::wall_secs() as types_core::pg_time_t
}

/// get_backup_status(void) (xlog.c:9175).
pub fn get_backup_status() -> SessionBackupState {
    SESSION_BACKUP_STATE.with(core::cell::Cell::get)
}

// ===========================================================================
// do_pg_backup_start — xlog.c:8842.
// ===========================================================================

/// do_pg_backup_start(backupidstr, fast, tablespaces, state, tblspcmapfile)
/// (xlog.c:8842). Forces a checkpoint, fills `state` with the start metadata,
/// enumerates auxiliary tablespaces into `tablespaces` (when Some, matching C's
/// non-NULL `List **`), and appends the tablespace_map lines to `tblspcmapfile`.
pub fn do_pg_backup_start(
    backupidstr: &str,
    fast: bool,
    tablespaces: Option<&mut Vec<TablespaceInfo>>,
    state: &mut BackupState,
    tblspcmapfile: &mut Vec<u8>,
) -> PgResult<()> {
    let backup_started_in_recovery = RecoveryInProgress();

    // During recovery, WAL level need not be checked: it's impossible to get
    // here during recovery with an insufficient level.
    if !backup_started_in_recovery && !XLogIsNeeded() {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("WAL level not sufficient for making an online backup")
            .errhint("\"wal_level\" must be set to \"replica\" or \"logical\" at server start.")
            .finish(loc("do_pg_backup_start"));
    }

    if backupidstr.len() > MAXPGPATH {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("backup label too long (max {MAXPGPATH} bytes)"))
            .finish(loc("do_pg_backup_start"));
    }

    state.set_name(backupidstr.as_bytes());

    // Mark backup active. Full-page writes during the backup are forced
    // implicitly: XLogInsertRecord observes Insert.runningBackups > 0. All
    // insertion locks are held to change runningBackups, interlocking against
    // XLogInsertRecord.
    WALInsertLockAcquireExclusive();
    XLogCtl().Insert.runningBackups.fetch_add(1, Relaxed);
    WALInsertLockRelease();

    // Ensure runningBackups is decremented if the body fails. Critical: session
    // state is only updated after this block. (C: PG_ENSURE_ERROR_CLEANUP with
    // do_pg_abort_backup(during_backup_start = true).)
    let result = do_pg_backup_start_body(
        backup_started_in_recovery,
        fast,
        tablespaces,
        state,
        tblspcmapfile,
    );
    if result.is_err() {
        let _ = do_pg_abort_backup_impl(true);
        return result;
    }

    state.started_in_recovery = backup_started_in_recovery;

    // Mark that the start phase has correctly finished.
    SESSION_BACKUP_STATE.with(|c| c.set(SessionBackupState::Running));

    Ok(())
}

fn do_pg_backup_start_body(
    backup_started_in_recovery: bool,
    fast: bool,
    tablespaces: Option<&mut Vec<TablespaceInfo>>,
    state: &mut BackupState,
    tblspcmapfile: &mut Vec<u8>,
) -> PgResult<()> {
    let mut got_unique_startpoint = false;

    // Force an XLOG file switch before the checkpoint so the checkpoint's WAL
    // segment has no pages with old timeline IDs. Skipped during recovery.
    if !backup_started_in_recovery {
        RequestXLogSwitch(false)?;
    }

    loop {
        // Force a CHECKPOINT (immediate only when fast).
        checkpointer_seams::request_checkpoint::call(
            CHECKPOINT_FORCE | CHECKPOINT_WAIT | if fast { CHECKPOINT_IMMEDIATE } else { 0 },
        )?;

        // Fetch the checkpoint record location and its REDO pointer.
        let checkpointfpw = {
            lwlock::LWLockAcquire(ControlFileLock(), LW_SHARED, init_small::globals::MyProcNumber())?;
            let cf = control_file();
            state.checkpointloc = cf.checkPoint;
            state.startpoint = cf.checkPointCopy.redo;
            state.starttli = cf.checkPointCopy.ThisTimeLineID;
            let fpw = cf.checkPointCopy.fullPageWrites;
            lwlock::LWLockRelease(ControlFileLock())?;
            fpw
        };

        if backup_started_in_recovery {
            // Check that all WAL replayed since the last restartpoint contains
            // full-page writes.
            let recptr = {
                let ctl = XLogCtl();
                ctl.info_lck.with(|| ctl.lastFpwDisableRecPtr.load(Relaxed))
            };

            if !checkpointfpw || state.startpoint <= recptr {
                return ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(
                        "WAL generated with \"full_page_writes=off\" was replayed \
                         since last restartpoint",
                    )
                    .errhint(
                        "This means that the backup being taken on the standby is corrupt \
                         and should not be used. Enable \"full_page_writes\" and run \
                         CHECKPOINT on the primary, and then try an online backup again.",
                    )
                    .finish(loc("do_pg_backup_start"));
            }

            // During recovery the starting WAL location need not be unique.
            got_unique_startpoint = true;
        }

        // Two concurrent base backups must use distinct checkpoints as starting
        // locations (the starting WAL location is the backup's unique id).
        WALInsertLockAcquireExclusive();
        let ins = &XLogCtl().Insert;
        if ins.lastBackupStart.load(Relaxed) < state.startpoint {
            ins.lastBackupStart.store(state.startpoint, Relaxed);
            got_unique_startpoint = true;
        }
        WALInsertLockRelease();

        if got_unique_startpoint {
            break;
        }
    }

    // Construct tablespace_map file (and the tablespace list, if requested).
    let mut collected: Vec<TablespaceInfo> = Vec::new();
    collect_tablespaces(&mut collected, tblspcmapfile)?;
    if let Some(out) = tablespaces {
        out.extend(collected);
    }

    state.starttime = wallclock_time();

    Ok(())
}

/// The tablespace-enumeration leg of do_pg_backup_start (xlog.c:9019-9138):
/// walk pg_tblspc, appending one TablespaceInfo per tablespace and the matching
/// `<oid> <escaped-link-path>` line to tblspcmapfile.
fn collect_tablespaces(
    tablespaces: &mut Vec<TablespaceInfo>,
    tblspcmapfile: &mut Vec<u8>,
) -> PgResult<()> {
    let datadir = init_small::globals::DataDir().unwrap_or_default();
    let datadirpathlen = datadir.len();

    fd::with_allocated_dir(PG_TBLSPC_DIR, &mut |d_name: &str| {
        let bytes = d_name.as_bytes();

        // Tablespace directory names are positive 32-bit integers, no leading
        // zeroes or trailing garbage. C: if (d_name[0] < '1' || d_name[1] > '9').
        if bytes.is_empty() || bytes[0] < b'1' || (bytes.len() > 1 && bytes[1] > b'9') {
            return Ok(false);
        }
        let tsoid: u32 = match d_name.parse::<u32>() {
            Ok(v) => v,
            Err(_) => return Ok(false),
        };

        let fullpath = format!("{PG_TBLSPC_DIR}/{d_name}");

        // get_dirent_type(look_through_symlinks = false): lstat.
        let mut md = fd::FileInfo::zeroed();
        if fd::pg_lstat(&fullpath, &mut md) != 0 {
            return Ok(false);
        }

        let linkpath: String;
        let mut relpath: Option<String> = None;

        if md.is_symlink() {
            // readlink(2), MAXPGPATH-bounded like C's buffer.
            let mut buf = [0u8; MAXPGPATH];
            let n = fd::pg_readlink(&fullpath, &mut buf);
            if n < 0 {
                ereport(WARNING)
                    .errmsg(format!("could not read symbolic link \"{fullpath}\": %m"))
                    .finish(loc("do_pg_backup_start"))
                    .ok();
                return Ok(false);
            }
            let target = String::from_utf8_lossy(&buf[..n as usize]).into_owned();
            if target.len() >= MAXPGPATH {
                ereport(WARNING)
                    .errmsg(format!("symbolic link \"{fullpath}\" target is too long"))
                    .finish(loc("do_pg_backup_start"))
                    .ok();
                return Ok(false);
            }
            linkpath = target;

            // relpath: PGDATA-relative path when the tablespace lives in PGDATA.
            let lp = linkpath.as_bytes();
            if linkpath.len() > datadirpathlen
                && lp.starts_with(datadir.as_bytes())
                && lp[datadirpathlen] == b'/'
            {
                relpath = Some(linkpath[datadirpathlen + 1..].to_string());
            }

            // Backslash-escaped link path into the tablespace map.
            let mut escapedpath = String::new();
            for &c in lp {
                if c == b'\n' || c == b'\r' || c == b'\\' {
                    escapedpath.push('\\');
                }
                escapedpath.push(c as char);
            }
            tblspcmapfile.extend_from_slice(format!("{d_name} {escapedpath}\n").as_bytes());
        } else if md.is_dir() {
            // allow_in_place_tablespaces: a directory directly under pg_tblspc.
            // Store a relative path.
            linkpath = format!("{PG_TBLSPC_DIR}/{d_name}");
            relpath = Some(linkpath.clone());
        } else {
            return Ok(false);
        }

        tablespaces.push(TablespaceInfo {
            oid: tsoid,
            path: Some(linkpath),
            rpath: relpath,
            size: -1,
        });
        Ok(false)
    })?;

    Ok(())
}

// ===========================================================================
// do_pg_backup_stop — xlog.c:9170.
// ===========================================================================

/// do_pg_backup_stop(state, waitforarchive) (xlog.c:9170). Writes the
/// end-of-backup WAL record (when not in recovery), the backup history file,
/// resets sessionBackupState, and fills `state`'s stop fields.
pub fn do_pg_backup_stop(state: &mut BackupState, waitforarchive: bool) -> PgResult<()> {
    let wal_segment_size = wal_segment_size();
    let backup_stopped_in_recovery = RecoveryInProgress();

    if !backup_stopped_in_recovery && !XLogIsNeeded() {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("WAL level not sufficient for making an online backup")
            .errhint("\"wal_level\" must be set to \"replica\" or \"logical\" at server start.")
            .finish(loc("do_pg_backup_stop"));
    }

    // Update the backup counter and session-level lock. No CHECK_FOR_INTERRUPTS
    // may occur while updating them.
    WALInsertLockAcquireExclusive();

    // Each do_pg_backup_start() is matched by exactly one do_pg_backup_stop().
    let ins = &XLogCtl().Insert;
    debug_assert!(ins.runningBackups.load(Relaxed) > 0);
    ins.runningBackups.fetch_sub(1, Relaxed);

    // Session-level lock must be cleared before WALInsertLockRelease (which can
    // CHECK_FOR_INTERRUPTS).
    SESSION_BACKUP_STATE.with(|c| c.set(SessionBackupState::None));

    WALInsertLockRelease();

    // A standby must not have been promoted during a backup taken on it.
    if state.started_in_recovery && !backup_stopped_in_recovery {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("the standby was promoted during online backup")
            .errhint(
                "This means that the backup being taken is corrupt and should not be used. \
                 Try taking another online backup.",
            )
            .finish(loc("do_pg_backup_stop"));
    }

    if backup_stopped_in_recovery {
        // No end-of-backup record during recovery; pg_control's minRecoveryPoint
        // is the backup end location.
        let recptr = {
            let ctl = XLogCtl();
            ctl.info_lck.with(|| ctl.lastFpwDisableRecPtr.load(Relaxed))
        };

        if state.startpoint <= recptr {
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(
                    "WAL generated with \"full_page_writes=off\" was replayed \
                     during online backup",
                )
                .errhint(
                    "This means that the backup being taken on the standby is corrupt \
                     and should not be used. Enable \"full_page_writes\" and run \
                     CHECKPOINT on the primary, and then try an online backup again.",
                )
                .finish(loc("do_pg_backup_stop"));
        }

        lwlock::LWLockAcquire(ControlFileLock(), LW_SHARED, init_small::globals::MyProcNumber())?;
        let cf = control_file();
        state.stoppoint = cf.minRecoveryPoint;
        state.stoptli = cf.minRecoveryPointTLI;
        lwlock::LWLockRelease(ControlFileLock())?;
    } else {
        // Write the backup-end xlog record.
        let startpoint_bytes = state.startpoint.to_ne_bytes();
        state.stoppoint =
            xloginsert_seams::xlog_insert::call(RM_XLOG_ID, XLOG_BACKUP_END, &[&startpoint_bytes[..]])?;

        // Not in recovery, so InsertTimeLineID is set and stable (read lock-free).
        state.stoptli = XLogCtl().InsertTimeLineID.load(Relaxed);

        // Switch to a new xlog segment so the backup is valid once the archiver
        // moves out the current segment.
        RequestXLogSwitch(false)?;

        state.stoptime = wallclock_time();

        // Write the backup history file.
        let seg_no = XLByteToSeg(state.startpoint, wal_segment_size);
        let histfilepath =
            BackupHistoryFilePath(state.stoptli, seg_no, state.startpoint, wal_segment_size);
        write_backup_history_file(&histfilepath, state, wal_segment_size)?;

        // Clean out no-longer-needed history files (posts a .ready for the new one).
        CleanupBackupHistory()?;
    }

    // Wait for required WAL files to be archived, if archiving is enabled.
    let archive_mode = guc_tables::vars::XLogArchiveMode.read();
    let xlog_archiving_active = archive_mode > ARCHIVE_MODE_OFF;
    let xlog_archiving_always = archive_mode == ARCHIVE_MODE_ALWAYS;
    let should_wait = waitforarchive
        && ((!backup_stopped_in_recovery && xlog_archiving_active)
            || (backup_stopped_in_recovery && xlog_archiving_always));

    if should_wait {
        const WAIT_EVENT_BACKUP_WAIT_WAL_ARCHIVE: u32 = 0x0800_0000 + 4;
        use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};
        let seg_no = crate::XLByteToPrevSeg(state.stoppoint, wal_segment_size);
        let lastxlogfilename = crate::XLogFileName(state.stoptli, seg_no, wal_segment_size);
        let seg_no = XLByteToSeg(state.startpoint, wal_segment_size);
        let histfilename =
            BackupHistoryFileName(state.stoptli, seg_no, state.startpoint, wal_segment_size);

        let mut seconds_before_warning = 60;
        let mut waits = 0;
        let mut reported_waiting = false;
        while xlogarchive_seams::xlog_archive_is_busy::call(&lastxlogfilename)
            || xlogarchive_seams::xlog_archive_is_busy::call(&histfilename)
        {
            postgres_seams::check_for_interrupts::call()?;
            if !reported_waiting && waits > 5 {
                ereport(NOTICE)
                    .errmsg(
                        "base backup done, waiting for required WAL segments to be archived",
                    )
                    .finish(loc("do_pg_backup_stop"))
                    .ok();
                reported_waiting = true;
            }
            let _ = latch::WaitLatch(
                init_small::globals::MyLatch(),
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                1000,
                WAIT_EVENT_BACKUP_WAIT_WAL_ARCHIVE,
            )?;
            if let Some(l) = init_small::globals::MyLatch() {
                latch::ResetLatch(l);
            }
            waits += 1;
            if waits >= seconds_before_warning {
                seconds_before_warning *= 2;
                ereport(WARNING)
                    .errmsg(format!(
                        "still waiting for all required WAL segments to be archived ({waits} seconds elapsed)"
                    ))
                    .errhint(
                        "Check that your \"archive_command\" is executing properly.  You can safely cancel this backup, but the database backup will not be usable without all the WAL segments.",
                    )
                    .finish(loc("do_pg_backup_stop"))
                    .ok();
            }
        }
        ereport(NOTICE)
            .errmsg("all required WAL segments have been archived")
            .finish(loc("do_pg_backup_stop"))
            .ok();
    } else if waitforarchive {
        ereport(NOTICE)
            .errmsg(
                "WAL archiving is not enabled; you must ensure that all required WAL segments \
                 are copied through other means to complete the backup",
            )
            .finish(loc("do_pg_backup_stop"))
            .ok();
    }

    Ok(())
}

/// AllocateFile(histfilepath, "w") + fprintf(build_backup_content(state, true)).
fn write_backup_history_file(
    histfilepath: &str,
    state: &BackupState,
    wal_segment_size: i32,
) -> PgResult<()> {
    let ctx = mcx::MemoryContext::new_bump("on-line backup history");
    let content = xlogbackup::build_backup_content(ctx.mcx(), state, true, wal_segment_size)?;

    let idx = fd::AllocateFile(histfilepath, "w")?;
    if idx < 0 {
        return ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not create file \"{histfilepath}\": %m"))
            .finish(loc("do_pg_backup_stop"));
    }

    let wrote = fd::with_allocated_stdio(idx, |f| {
        f.write_all(&content).and_then(|()| f.flush())
    });
    let write_ok = matches!(wrote, Some(Ok(())));
    let freed = fd::FreeFile(idx)?;

    // content (borrowing ctx) drops before ctx at scope end — correct order.
    if !write_ok || freed != 0 {
        return ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not write file \"{histfilepath}\": %m"))
            .finish(loc("do_pg_backup_stop"));
    }
    Ok(())
}

// ===========================================================================
// do_pg_abort_backup / register_persistent_abort_backup_handler — xlog.c:9444.
// ===========================================================================

/// do_pg_abort_backup(during_backup_start) (xlog.c:9444) — take the system out
/// of backup mode. `during_backup_start` == C's DatumGetBool(arg).
pub fn do_pg_abort_backup(during_backup_start: bool) -> PgResult<()> {
    do_pg_abort_backup_impl(during_backup_start)
}

fn do_pg_abort_backup_impl(during_backup_start: bool) -> PgResult<()> {
    debug_assert!(!during_backup_start || get_backup_status() == SessionBackupState::None);

    if during_backup_start || get_backup_status() != SessionBackupState::None {
        WALInsertLockAcquireExclusive();
        let ins = &XLogCtl().Insert;
        debug_assert!(ins.runningBackups.load(Relaxed) > 0);
        ins.runningBackups.fetch_sub(1, Relaxed);

        SESSION_BACKUP_STATE.with(|c| c.set(SessionBackupState::None));
        WALInsertLockRelease();

        if !during_backup_start {
            ereport(WARNING)
                .errmsg("aborting backup due to backend exiting before pg_backup_stop was called")
                .finish(loc("do_pg_abort_backup"))
                .ok();
        }
    }
    Ok(())
}

/// The before_shmem_exit callback shape (xlog.c:9444): (code, arg) where
/// arg carries during_backup_start.
fn do_pg_abort_backup_callback(_code: i32, arg: datum::Datum) -> PgResult<()> {
    do_pg_abort_backup_impl(arg.as_bool())
}

/// register_persistent_abort_backup_handler(void) (xlog.c:9471) — register the
/// before_shmem_exit cleanup that aborts an in-progress backup if the session
/// ends without pg_backup_stop(), unless already registered.
pub fn register_persistent_abort_backup_handler() -> PgResult<()> {
    if ABORT_HANDLER_REGISTERED.with(core::cell::Cell::get) {
        return Ok(());
    }
    ipc_seams::before_shmem_exit::call(do_pg_abort_backup_callback, datum::Datum::from_bool(false))?;
    ABORT_HANDLER_REGISTERED.with(|c| c.set(true));
    Ok(())
}

// ===========================================================================
// CleanupBackupHistory + backup-history filename helpers — xlog.c:8745,
// xlog_internal.h.
// ===========================================================================

/// CleanupBackupHistory(void) (xlog.c:8745) — remove backup history files that
/// have already been archived (or whose archiving is not required).
fn CleanupBackupHistory() -> PgResult<()> {
    let mut to_remove: Vec<String> = Vec::new();
    fd::with_allocated_dir(XLOGDIR, &mut |d_name: &str| {
        if IsBackupHistoryFileName(d_name) && xlogarchive_seams::xlog_archive_check_done::call(d_name)? {
            to_remove.push(d_name.to_string());
        }
        Ok(false)
    })?;

    for d_name in to_remove {
        ereport(DEBUG2)
            .errmsg(format!("removing WAL backup history file \"{d_name}\""))
            .finish(loc("CleanupBackupHistory"))
            .ok();
        let path = format!("{XLOGDIR}/{d_name}");
        let _ = fd::pg_unlink(&path);
        xlogarchive_seams::xlog_archive_cleanup::call(&d_name);
    }
    Ok(())
}

/// BackupHistoryFilePath (xlog_internal.h): XLOGDIR/<tli><log><seg>.<off>.backup.
fn BackupHistoryFilePath(
    tli: TimeLineID,
    log_seg_no: XLogSegNo,
    startpoint: XLogRecPtr,
    wal_segsz: i32,
) -> String {
    format!("{XLOGDIR}/{}", BackupHistoryFileName(tli, log_seg_no, startpoint, wal_segsz))
}

/// BackupHistoryFileName (xlog_internal.h): <tli><log><seg>.<startoff>.backup.
fn BackupHistoryFileName(
    tli: TimeLineID,
    log_seg_no: XLogSegNo,
    startpoint: XLogRecPtr,
    wal_segsz: i32,
) -> String {
    let per_id = XLogSegmentsPerXLogId(wal_segsz);
    format!(
        "{tli:08X}{:08X}{:08X}.{:08X}.backup",
        log_seg_no / per_id,
        log_seg_no % per_id,
        XLogSegmentOffset(startpoint, wal_segsz),
    )
}

/// IsBackupHistoryFileName (xlog_internal.h): the leading run is exactly
/// XLOG_FNAME_LEN hex chars (char 24 is the '.' separator) and the tail is
/// ".backup".
fn IsBackupHistoryFileName(fname: &str) -> bool {
    const XLOG_FNAME_LEN: usize = 24;
    let hex_run = fname.bytes().take_while(|b| b.is_ascii_digit() || (b'A'..=b'F').contains(b)).count();
    fname.len() > XLOG_FNAME_LEN && hex_run == XLOG_FNAME_LEN && fname.ends_with(".backup")
}
