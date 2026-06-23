//! Online base-backup control: the `do_pg_backup_start` / `do_pg_backup_stop` /
//! `do_pg_abort_backup` / `register_persistent_abort_backup_handler` /
//! `get_backup_status` family from `access/transam/xlog.c` (the
//! "Online backup" section, xlog.c:8866-9505), plus `CleanupBackupHistory`.
//!
//! These are reached through the `backend-access-transam-xlog-seams`
//! (`do_pg_backup_start`, `do_pg_backup_stop`,
//! `register_persistent_abort_backup_handler`, `get_backup_status`) and the
//! `backend-backup-basebackup-seams` (`do_pg_backup_start_for_basebackup`,
//! `do_pg_abort_backup`) owner seams, installed in this crate's `init_seams`.
//!
//! The single C `do_pg_backup_start(backupidstr, fast, List **tablespaces,
//! BackupState *state, StringInfo tblspcmapfile)` is split here into a core that
//! always builds the tablespace list (the basebackup variant needs it) and two
//! thin wrappers: the `pg_backup_start()` SQL path drops the list (C passes
//! `tablespaces = NULL`), the `basebackup.c` path keeps it.

#![allow(non_snake_case)]

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;

use utils_error::{ereport, PgResult};
use types_core::{TimeLineID, XLogRecPtr, MAXPGPATH};
use types_error::{
    ErrorLocation, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERROR, LOG, NOTICE, WARNING,
};
use types_storage::storage::{LWLockMode, LW_SHARED};
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};
use types_tuple::Datum;
use wal::wal::{BackupState, SessionBackupState, RM_XLOG_ID};
use wal::xlog_consts::{
    CHECKPOINT_FORCE, CHECKPOINT_IMMEDIATE, CHECKPOINT_WAIT, MAXFNAMELEN,
};

use xloginsert_seams as xloginsert;
use checkpointer_seams as checkpointer;
use dsm_core_seams as ipc;
use latch_seams as latch;
use lwlock as lwlock;
use postgres_seams as tcop;
use init_small::globals;

use sink::TablespaceInfo;

use crate::control_funcs::RequestXLogSwitch;
use crate::insert::{WALInsertLockAcquireExclusive, WALInsertLockRelease};
use crate::shmem::{self, control_file_mut, spin_lock_acquire, spin_lock_release, xlog_ctl};
use crate::{
    BackupHistoryFilePath, IsBackupHistoryFileName, XLByteToPrevSeg, XLByteToSeg, XLogFileName,
};

/// `XLOG_BACKUP_END` (xlog.h) — the end-of-backup WAL record info byte.
const XLOG_BACKUP_END: u8 = 0x50;

/// `ControlFileLock` — offset 9 in the `MainLWLockArray` (`lwlocklist.h`).
const CONTROL_FILE_LOCK: usize = 9;

/// `PG_TBLSPC_DIR` (`storage/fd.h`) — the per-cluster tablespace symlink dir.
const PG_TBLSPC_DIR: &str = "pg_tblspc";

/// `XLOGDIR` (`access/xlog_internal.h`) — the WAL directory, relative to PGDATA.
const XLOGDIR: &str = "pg_wal";

/// `PGFILETYPE_DIR` / `PGFILETYPE_LNK` (`storage/fd.h`) — `get_dirent_type` codes.
const PGFILETYPE_DIR: i32 = 3;
const PGFILETYPE_LNK: i32 = 4;

/// `ARCHIVE_MODE_OFF` / `ARCHIVE_MODE_ALWAYS` (`access/xlog.h`) — the
/// `archive_mode` enum GUC values.
const ARCHIVE_MODE_OFF: i32 = 0;
const ARCHIVE_MODE_ALWAYS: i32 = 2;

/// `PG_WAIT_IPC` (`utils/wait_event.h`) — the IPC wait-event class base.
const PG_WAIT_IPC: u32 = 0x0800_0000;

/// `WAIT_EVENT_BACKUP_WAIT_WAL_ARCHIVE` — 5th entry (index 4) of the IPC section
/// of `wait_event_names.txt` (APPEND_READY, ARCHIVE_CLEANUP_COMMAND,
/// ARCHIVE_COMMAND, BACKEND_TERMINATION, BACKUP_WAIT_WAL_ARCHIVE, ...).
const WAIT_EVENT_BACKUP_WAIT_WAL_ARCHIVE: u32 = PG_WAIT_IPC + 4;

/// `static SessionBackupState sessionBackupState = SESSION_BACKUP_NONE`
/// (xlog.c:416) — backend-local session-level backup status.
std::thread_local! {
    static SESSION_BACKUP_STATE: core::cell::Cell<SessionBackupState> =
        const { core::cell::Cell::new(SessionBackupState::None) };
}

/// `static bool already_done` in `register_persistent_abort_backup_handler`
/// (xlog.c) — the once-guard for the before_shmem_exit registration.
std::thread_local! {
    static ABORT_HANDLER_REGISTERED: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
}

#[inline]
fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new("xlog.c", line, func)
}

/// `(pg_time_t) time(NULL)` — wall-clock seconds, as `do_pg_backup_start` /
/// `do_pg_backup_stop` record in `state->starttime` / `state->stoptime`.
fn wallclock_time() -> types_core::pg_time_t {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as types_core::pg_time_t)
        .unwrap_or(0)
}

/// `XLogIsNeeded()` (xlog.h): `wal_level >= WAL_LEVEL_REPLICA`.
fn xlog_is_needed() -> bool {
    guc_tables::vars::wal_level.read()
        >= wal::xlog_consts::WalLevel::Replica as i32
}

/// Acquire `ControlFileLock` in the given mode, run `f`, release.
fn with_control_file_lock<R>(mode: LWLockMode, f: impl FnOnce() -> R) -> PgResult<R> {
    let lock = lwlock::main_lock_ref(CONTROL_FILE_LOCK);
    lwlock::LWLockAcquire(lock, mode, globals::MyProcNumber())?;
    let r = f();
    lwlock::LWLockRelease(lock)?;
    Ok(r)
}

/// `get_backup_status(void)` (xlog.c:9175) — the session-level status of a
/// running backup.
pub fn get_backup_status() -> SessionBackupState {
    SESSION_BACKUP_STATE.with(core::cell::Cell::get)
}

// ===========================================================================
// do_pg_backup_start — xlog.c:8866.
// ===========================================================================

/// `do_pg_backup_start(backupidstr, fast, &tablespaces, state, tblspcmapfile)`
/// (xlog.c:8866) — the workhorse of `pg_backup_start()` and base backup: writes
/// a checkpoint, fills `state` with the backup-start metadata, enumerates the
/// auxiliary tablespaces, and renders the `tablespace_map` file contents.
///
/// Returns the populated [`BackupState`], the tablespace list (the C
/// `tablespaces` out-param), and the `tablespace_map` bytes (the C `StringInfo
/// tblspcmapfile`).
pub fn do_pg_backup_start(
    backupidstr: &str,
    fast: bool,
) -> PgResult<(BackupState, Vec<TablespaceInfo>, Vec<u8>)> {
    let mut state = BackupState::zeroed();
    let mut tablespaces: Vec<TablespaceInfo> = Vec::new();
    let mut tblspcmapfile: Vec<u8> = Vec::new();

    let backup_started_in_recovery = shmem::RecoveryInProgress();

    // During recovery, we don't need to check WAL level. Because, if WAL level
    // is not sufficient, it's impossible to get here during recovery.
    if !backup_started_in_recovery && !xlog_is_needed() {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("WAL level not sufficient for making an online backup")
            .errhint("\"wal_level\" must be set to \"replica\" or \"logical\" at server start.")
            .finish(loc(8878, "do_pg_backup_start"))
            .map(|_| unreachable!());
    }

    if backupidstr.len() > MAXPGPATH {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("backup label too long (max {MAXPGPATH} bytes)"))
            .finish(loc(8884, "do_pg_backup_start"))
            .map(|_| unreachable!());
    }

    // strlcpy(state->name, backupidstr, sizeof(state->name));
    state.set_name(backupidstr.as_bytes());

    // Mark backup active in shared memory. We must hold all the insertion locks
    // to change the value of runningBackups, to ensure adequate interlocking
    // against XLogInsertRecord().
    WALInsertLockAcquireExclusive()?;
    // SAFETY: live shmem region; all insertion locks held.
    unsafe {
        (*xlog_ctl()).Insert.runningBackups += 1;
    }
    WALInsertLockRelease()?;

    // Ensure we decrement runningBackups if we fail below. NB -- for this to
    // work correctly, it is critical that sessionBackupState is only updated
    // after this block is over.
    //   PG_ENSURE_ERROR_CLEANUP(do_pg_abort_backup, DatumGetBool(true));
    let result = do_pg_backup_start_body(
        backup_started_in_recovery,
        fast,
        &mut state,
        &mut tablespaces,
        &mut tblspcmapfile,
    );
    if result.is_err() {
        // The cleanup handler with `during_backup_start = true`.
        let _ = do_pg_abort_backup_impl(true);
        result?;
    }

    state.set_started_in_recovery(backup_started_in_recovery);

    // Mark that the start phase has correctly finished for the backup.
    SESSION_BACKUP_STATE.with(|c| c.set(SessionBackupState::Running));

    Ok((state, tablespaces, tblspcmapfile))
}

/// The body of `do_pg_backup_start` guarded by `PG_ENSURE_ERROR_CLEANUP`
/// (xlog.c:8923-9165).
fn do_pg_backup_start_body(
    backup_started_in_recovery: bool,
    fast: bool,
    state: &mut BackupState,
    tablespaces: &mut Vec<TablespaceInfo>,
    tblspcmapfile: &mut Vec<u8>,
) -> PgResult<()> {
    let mut got_unique_startpoint = false;

    // Force an XLOG file switch before the checkpoint, to ensure that the WAL
    // segment the checkpoint is written to doesn't contain pages with old
    // timeline IDs. During recovery, we skip forcing XLOG file switch.
    if !backup_started_in_recovery {
        RequestXLogSwitch(false)?;
    }

    loop {
        // Force a CHECKPOINT. We use CHECKPOINT_IMMEDIATE only if requested by
        // user (via passing fast = true).
        checkpointer::request_checkpoint::call(
            CHECKPOINT_FORCE | CHECKPOINT_WAIT | if fast { CHECKPOINT_IMMEDIATE } else { 0 },
        );

        // Now we need to fetch the checkpoint record location, and also its
        // REDO pointer. The oldest point in WAL that would be needed to restore
        // starting from the checkpoint is precisely the REDO pointer.
        let checkpointfpw = with_control_file_lock(LW_SHARED, || {
            let cf = control_file_mut();
            state.set_checkpointloc(cf.checkPoint);
            state.set_startpoint(cf.checkPointCopy.redo);
            state.set_starttli(cf.checkPointCopy.ThisTimeLineID);
            cf.checkPointCopy.fullPageWrites
        })?;

        if backup_started_in_recovery {
            // Check to see if all WAL replayed during online backup (i.e., since
            // last restartpoint used as backup starting checkpoint) contain
            // full-page writes.
            let recptr = {
                // SAFETY: live shmem region; info_lck serializes the read.
                let ctl = xlog_ctl();
                unsafe {
                    let ctl = &*ctl;
                    spin_lock_acquire(&ctl.info_lck);
                    let r = ctl.lastFpwDisableRecPtr;
                    spin_lock_release(&ctl.info_lck);
                    r
                }
            };

            if !checkpointfpw || state.startpoint() <= recptr {
                return ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(
                        "WAL generated with \"full_page_writes=off\" was replayed \
                         since last restartpoint",
                    )
                    .errhint(
                        "This means that the backup being taken on the standby \
                         is corrupt and should not be used. \
                         Enable \"full_page_writes\" and run CHECKPOINT on the primary, \
                         and then try an online backup again.",
                    )
                    .finish(loc(8998, "do_pg_backup_start"))
                    .map(|_| ());
            }

            // During recovery, since we don't use the end-of-backup WAL record
            // and don't write the backup history file, the starting WAL location
            // doesn't need to be unique.
            got_unique_startpoint = true;
        }

        // If two base backups are started at the same time (in WAL sender
        // processes), we need to make sure that they use different checkpoints
        // as starting locations, because we use the starting WAL location as a
        // unique identifier for the base backup in the end-of-backup WAL record
        // and when we write the backup history file.
        WALInsertLockAcquireExclusive()?;
        // SAFETY: live shmem region; all insertion locks held.
        unsafe {
            let ctl = &mut *xlog_ctl();
            if ctl.Insert.lastBackupStart < state.startpoint() {
                ctl.Insert.lastBackupStart = state.startpoint();
                got_unique_startpoint = true;
            }
        }
        WALInsertLockRelease()?;

        if got_unique_startpoint {
            break;
        }
    }

    // Construct tablespace_map file.
    collect_tablespaces(tablespaces, tblspcmapfile)?;

    // state->starttime = (pg_time_t) time(NULL);
    state.set_starttime(wallclock_time());

    Ok(())
}

/// The tablespace-enumeration leg of `do_pg_backup_start` (xlog.c:9043-9162):
/// walk `pg_tblspc`, appending one [`TablespaceInfo`] per tablespace and the
/// matching `<oid> <escaped-link-path>` line to `tblspcmapfile`.
fn collect_tablespaces(
    tablespaces: &mut Vec<TablespaceInfo>,
    tblspcmapfile: &mut Vec<u8>,
) -> PgResult<()> {
    // datadirpathlen = strlen(DataDir);
    let datadir = globals::DataDir().unwrap_or_default();
    let datadirpathlen = datadir.len();

    // Collect information about all tablespaces.
    for d_name in fd_seams::read_dir_names::call(PG_TBLSPC_DIR)? {
        let bytes = d_name.as_bytes();

        // Try to parse the directory name as an unsigned integer. Tablespace
        // directories should be positive integers representable in 32 bits, with
        // no leading zeroes or trailing garbage.
        //
        // C: `if (de->d_name[0] < '1' || de->d_name[1] > '9') continue;`
        if bytes.is_empty() || bytes[0] < b'1' || (bytes.len() > 1 && bytes[1] > b'9') {
            continue;
        }
        let tsoid: u32 = match d_name.parse::<u32>() {
            Ok(v) => v,
            Err(_) => continue,
        };

        let fullpath = format!("{PG_TBLSPC_DIR}/{d_name}");
        let de_type = fd_seams::get_dirent_type::call(&fullpath);

        let linkpath: String;
        let mut relpath: Option<String> = None;

        if de_type == PGFILETYPE_LNK {
            // rllen = readlink(fullpath, linkpath, sizeof(linkpath));
            let target = match fd_seams::read_link::call(&fullpath)? {
                Some(t) => t,
                None => {
                    ereport(WARNING)
                        .errmsg(format!("could not read symbolic link \"{fullpath}\""))
                        .finish(loc(9091, "do_pg_backup_start"))
                        .ok();
                    continue;
                }
            };
            // C bails if the target is >= sizeof(linkpath) (MAXPGPATH).
            if target.len() >= MAXPGPATH {
                ereport(WARNING)
                    .errmsg(format!("symbolic link \"{fullpath}\" target is too long"))
                    .finish(loc(9099, "do_pg_backup_start"))
                    .ok();
                continue;
            }
            linkpath = target;

            // Relpath holds the relative path of the tablespace directory when
            // it's located within PGDATA, or NULL if it's located elsewhere.
            let lp = linkpath.as_bytes();
            if linkpath.len() > datadirpathlen
                && lp.starts_with(datadir.as_bytes())
                && is_dir_sep(lp[datadirpathlen])
            {
                relpath = Some(linkpath[datadirpathlen + 1..].to_string());
            }

            // Add a backslash-escaped version of the link path to the tablespace
            // map file.
            let mut escapedpath = String::new();
            for &c in lp {
                if c == b'\n' || c == b'\r' || c == b'\\' {
                    escapedpath.push('\\');
                }
                escapedpath.push(c as char);
            }
            tblspcmapfile.extend_from_slice(format!("{d_name} {escapedpath}\n").as_bytes());
        } else if de_type == PGFILETYPE_DIR {
            // It's possible to use allow_in_place_tablespaces to create
            // directories directly under pg_tblspc, for testing purposes only.
            // In this case, we store a relative path.
            linkpath = format!("{PG_TBLSPC_DIR}/{d_name}");
            relpath = Some(linkpath.clone());
        } else {
            // Skip any other file type that appears here.
            continue;
        }

        // ti = palloc(sizeof(tablespaceinfo)); ... *tablespaces = lappend(...);
        tablespaces.push(TablespaceInfo {
            oid: tsoid,
            path: Some(linkpath),
            rpath: relpath,
            size: None, // C: ti->size = -1.
        });
    }

    Ok(())
}

/// `IS_DIR_SEP(ch)` (`port.h`) — on non-Windows, just `'/'`.
#[inline]
fn is_dir_sep(ch: u8) -> bool {
    ch == b'/'
}

// ===========================================================================
// do_pg_backup_stop — xlog.c:9194.
// ===========================================================================

/// `do_pg_backup_stop(state, waitforarchive)` (xlog.c:9194) — finish an online
/// backup: write the end-of-backup WAL record (when not in recovery), create the
/// backup history file, reset `sessionBackupState`, and optionally wait for WAL
/// segments to be archived. Returns the updated [`BackupState`] with its stop
/// fields filled.
pub fn do_pg_backup_stop(
    mut state: BackupState,
    waitforarchive: bool,
) -> PgResult<BackupState> {
    let wal_segment_size = shmem::wal_segment_size();

    let backup_stopped_in_recovery = shmem::RecoveryInProgress();

    // During recovery, we don't need to check WAL level.
    if !backup_stopped_in_recovery && !xlog_is_needed() {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("WAL level not sufficient for making an online backup")
            .errhint("\"wal_level\" must be set to \"replica\" or \"logical\" at server start.")
            .finish(loc(9219, "do_pg_backup_stop"))
            .map(|_| unreachable!());
    }

    // OK to update backup counter and session-level lock. Note that
    // CHECK_FOR_INTERRUPTS() must not occur while updating them.
    WALInsertLockAcquireExclusive()?;

    // It is expected that each do_pg_backup_start() call is matched by exactly
    // one do_pg_backup_stop() call.
    // SAFETY: live shmem region; all insertion locks held.
    unsafe {
        let ctl = &mut *xlog_ctl();
        debug_assert!(ctl.Insert.runningBackups > 0);
        ctl.Insert.runningBackups -= 1;
    }

    // Clean up session-level lock. Since CHECK_FOR_INTERRUPTS() can occur in
    // WALInsertLockRelease(), session-level lock must be cleaned up before it.
    SESSION_BACKUP_STATE.with(|c| c.set(SessionBackupState::None));

    WALInsertLockRelease()?;

    // If we are taking an online backup from the standby, we confirm that the
    // standby has not been promoted during the backup.
    if state.started_in_recovery() && !backup_stopped_in_recovery {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("the standby was promoted during online backup")
            .errhint(
                "This means that the backup being taken is corrupt \
                 and should not be used. \
                 Try taking another online backup.",
            )
            .finish(loc(9259, "do_pg_backup_stop"))
            .map(|_| unreachable!());
    }

    if backup_stopped_in_recovery {
        // During recovery, we don't write an end-of-backup record. We return the
        // current minimum recovery point as the backup end location.
        let recptr = {
            // SAFETY: live shmem region; info_lck serializes the read.
            let ctl = xlog_ctl();
            unsafe {
                let ctl = &*ctl;
                spin_lock_acquire(&ctl.info_lck);
                let r = ctl.lastFpwDisableRecPtr;
                spin_lock_release(&ctl.info_lck);
                r
            }
        };

        if state.startpoint() <= recptr {
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(
                    "WAL generated with \"full_page_writes=off\" was replayed \
                     during online backup",
                )
                .errhint(
                    "This means that the backup being taken on the standby \
                     is corrupt and should not be used. \
                     Enable \"full_page_writes\" and run CHECKPOINT on the primary, \
                     and then try an online backup again.",
                )
                .finish(loc(9299, "do_pg_backup_stop"))
                .map(|_| unreachable!());
        }

        with_control_file_lock(LW_SHARED, || {
            let cf = control_file_mut();
            state.set_stoppoint(cf.minRecoveryPoint);
            state.set_stoptli(cf.minRecoveryPointTLI);
        })?;
    } else {
        // Write the backup-end xlog record.
        //   XLogBeginInsert();
        //   XLogRegisterData(&state->startpoint, sizeof(state->startpoint));
        //   state->stoppoint = XLogInsert(RM_XLOG_ID, XLOG_BACKUP_END);
        let startpoint_bytes = state.startpoint().to_ne_bytes();
        let stoppoint =
            xloginsert::xlog_insert::call(RM_XLOG_ID, XLOG_BACKUP_END, 0, &[&startpoint_bytes])?;
        state.set_stoppoint(stoppoint);

        // Given that we're not in recovery, InsertTimeLineID is set and can't
        // change, so we can read it without a lock.
        // SAFETY: live shmem region.
        let stoptli = unsafe { (*xlog_ctl()).InsertTimeLineID };
        state.set_stoptli(stoptli);

        // Force a switch to a new xlog segment file, so that the backup is valid
        // as soon as archiver moves out the current segment file.
        RequestXLogSwitch(false)?;

        state.set_stoptime(wallclock_time());

        // Write the backup history file.
        let mut seg_no = XLByteToSeg(state.startpoint(), wal_segment_size);
        let histfilepath = BackupHistoryFilePath(
            state.stoptli(),
            seg_no,
            state.startpoint(),
            wal_segment_size,
        );

        // Build and save the contents of the backup history file.
        //   history_file = build_backup_content(state, true);
        let history_file =
            xlogbackup::build_backup_content_default(&state, true)?;

        // fp = AllocateFile(histfilepath, "w"); fprintf("%s"); fflush/FreeFile.
        fd_seams::allocate_file_write::call(&histfilepath, &history_file)?;

        // Clean out any no-longer-needed history files. As a side effect, this
        // posts a .ready file for the newly created history file.
        CleanupBackupHistory()?;

        // (avoid an unused-mut warning if the wait block below is the last user)
        let _ = &mut seg_no;
    }

    // If archiving is enabled, wait for all the required WAL files to be
    // archived before returning.
    //   XLogArchivingActive() == (XLogArchiveMode > ARCHIVE_MODE_OFF)
    //   XLogArchivingAlways() == (XLogArchiveMode == ARCHIVE_MODE_ALWAYS)
    let archive_mode = guc_tables::vars::XLogArchiveMode.read();
    let xlog_archiving_active = archive_mode > ARCHIVE_MODE_OFF;
    let xlog_archiving_always = archive_mode == ARCHIVE_MODE_ALWAYS;
    let should_wait = waitforarchive
        && ((!backup_stopped_in_recovery && xlog_archiving_active)
            || (backup_stopped_in_recovery && xlog_archiving_always));

    if should_wait {
        let mut log_seg_no = XLByteToPrevSeg(state.stoppoint(), wal_segment_size);
        let lastxlogfilename = XLogFileName(state.stoptli(), log_seg_no, wal_segment_size);

        log_seg_no = XLByteToSeg(state.startpoint(), wal_segment_size);
        let histfilename = crate::BackupHistoryFileName(
            state.stoptli(),
            log_seg_no,
            state.startpoint(),
            wal_segment_size,
        );

        let mut seconds_before_warning = 60;
        let mut waits = 0;
        let mut reported_waiting = false;

        while xlogarchive::XLogArchiveIsBusy(&lastxlogfilename)
            || xlogarchive::XLogArchiveIsBusy(&histfilename)
        {
            tcop::check_for_interrupts::call()?;

            if !reported_waiting && waits > 5 {
                ereport(NOTICE)
                    .errmsg(
                        "base backup done, waiting for required WAL segments to be archived",
                    )
                    .finish(loc(9408, "do_pg_backup_stop"))
                    .ok();
                reported_waiting = true;
            }

            let _ = latch::wait_latch_my_latch::call(
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                1000,
                WAIT_EVENT_BACKUP_WAIT_WAL_ARCHIVE,
            )?;
            latch::reset_latch_my_latch::call();

            waits += 1;
            if waits >= seconds_before_warning {
                seconds_before_warning *= 2;
                ereport(WARNING)
                    .errmsg(format!(
                        "still waiting for all required WAL segments to be archived \
                         ({waits} seconds elapsed)",
                    ))
                    .errhint(
                        "Check that your \"archive_command\" is executing properly.  \
                         You can safely cancel this backup, \
                         but the database backup will not be usable without all the WAL segments.",
                    )
                    .finish(loc(9430, "do_pg_backup_stop"))
                    .ok();
            }
        }

        ereport(NOTICE)
            .errmsg("all required WAL segments have been archived")
            .finish(loc(9436, "do_pg_backup_stop"))
            .ok();
    } else if waitforarchive {
        ereport(NOTICE)
            .errmsg(
                "WAL archiving is not enabled; you must ensure that all required WAL segments \
                 are copied through other means to complete the backup",
            )
            .finish(loc(9441, "do_pg_backup_stop"))
            .ok();
    }

    Ok(state)
}

// ===========================================================================
// do_pg_abort_backup / register_persistent_abort_backup_handler — xlog.c:9468.
// ===========================================================================

/// `do_pg_abort_backup(int code, Datum arg)` (xlog.c:9468) — abort a running
/// backup, taking the system out of backup mode. `during_backup_start` mirrors
/// the C `DatumGetBool(arg)` flag (true when called as the
/// `PG_ENSURE_ERROR_CLEANUP` callback inside `do_pg_backup_start`, false when
/// invoked as a before_shmem_exit handler).
fn do_pg_abort_backup_impl(during_backup_start: bool) -> PgResult<()> {
    // If called during backup start, there shouldn't be one already running.
    debug_assert!(
        !during_backup_start
            || get_backup_status() == SessionBackupState::None
    );

    if during_backup_start || get_backup_status() != SessionBackupState::None {
        WALInsertLockAcquireExclusive()?;
        // SAFETY: live shmem region; all insertion locks held.
        unsafe {
            let ctl = &mut *xlog_ctl();
            debug_assert!(ctl.Insert.runningBackups > 0);
            ctl.Insert.runningBackups -= 1;
        }

        SESSION_BACKUP_STATE.with(|c| c.set(SessionBackupState::None));
        WALInsertLockRelease()?;

        if !during_backup_start {
            ereport(WARNING)
                .errmsg("aborting backup due to backend exiting before pg_backup_stop was called")
                .finish(loc(9490, "do_pg_abort_backup"))
                .ok();
        }
    }
    Ok(())
}

/// The `before_shmem_exit` callback shape for `do_pg_abort_backup`
/// (xlog.c:9468): `void do_pg_abort_backup(int code, Datum arg)`.
fn do_pg_abort_backup_callback(_code: i32, arg: Datum<'static>) -> PgResult<()> {
    do_pg_abort_backup_impl(arg.as_bool())
}

/// `register_persistent_abort_backup_handler(void)` (xlog.c:9495) — register the
/// before_shmem_exit cleanup that aborts an in-progress backup if the session
/// ends without `pg_backup_stop()`, unless already registered.
pub fn register_persistent_abort_backup_handler() -> PgResult<()> {
    if ABORT_HANDLER_REGISTERED.with(core::cell::Cell::get) {
        return Ok(());
    }
    ipc::before_shmem_exit::call(do_pg_abort_backup_callback, Datum::from_bool(false))?;
    ABORT_HANDLER_REGISTERED.with(|c| c.set(true));
    Ok(())
}

// ===========================================================================
// CleanupBackupHistory — xlog.c:8769.
// ===========================================================================

/// `CleanupBackupHistory(void)` (xlog.c:8769) — remove any backup history files
/// that have already been archived (or whose archiving is not required), posting
/// `.ready` files as a side effect of `XLogArchiveCheckDone`.
fn CleanupBackupHistory() -> PgResult<()> {
    for d_name in fd_seams::read_dir_names::call(XLOGDIR)? {
        if IsBackupHistoryFileName(&d_name)
            && xlogarchive::XLogArchiveCheckDone(&d_name)?
        {
            ereport(types_error::DEBUG2)
                .errmsg(format!("removing WAL backup history file \"{d_name}\""))
                .finish(loc(8786, "CleanupBackupHistory"))
                .ok();
            let path = format!("{XLOGDIR}/{d_name}");
            fd_seams::unlink_file::call(&path);
            xlogarchive::XLogArchiveCleanup(&d_name);
        }
    }
    Ok(())
}

// ===========================================================================
// Basebackup-variant wrappers + seam installation.
// ===========================================================================

/// `do_pg_backup_start_for_basebackup(label, fastcheckpoint)` — the
/// `basebackup.c` entry: like `pg_backup_start()` but keeps the tablespace list.
fn do_pg_backup_start_for_basebackup(
    backupidstr: &str,
    fast: bool,
) -> PgResult<backup_basebackup_seams::BackupStartResult> {
    let (state, tablespaces, tablespace_map) = do_pg_backup_start(backupidstr, fast)?;
    Ok(backup_basebackup_seams::BackupStartResult {
        state,
        tablespaces,
        tablespace_map,
    })
}

/// `do_pg_backup_start(backupidstr, fast, NULL, ...)` — the SQL `pg_backup_start()`
/// path, which drops the tablespace out-list.
fn do_pg_backup_start_sql(backupidstr: &str, fast: bool) -> PgResult<(BackupState, Vec<u8>)> {
    let (state, _tablespaces, tablespace_map) = do_pg_backup_start(backupidstr, fast)?;
    Ok((state, tablespace_map))
}

/// `do_pg_abort_backup(emit_warning)` — the basebackup `PG_ENSURE_ERROR_CLEANUP`
/// seam: `emit_warning == false` is the C `during_backup_start = false` ... but
/// basebackup calls it with `BoolGetDatum(false)`, i.e. NOT during start, so a
/// running backup is taken out of backup mode and a warning IS emitted. The
/// seam's `emit_warning` parameter is the C `arg` (`during_backup_start`).
fn do_pg_abort_backup_seam(during_backup_start: bool) {
    let _ = do_pg_abort_backup_impl(during_backup_start);
}

/// Install the online-backup owner seams.
pub fn init_seams() {
    use transam_xlog_seams as xs;

    xs::do_pg_backup_start::set(do_pg_backup_start_sql);
    xs::do_pg_backup_stop::set(do_pg_backup_stop);
    xs::get_backup_status::set(get_backup_status);
    xs::register_persistent_abort_backup_handler::set(register_persistent_abort_backup_handler);

    // `XLogArchiveMode` (xlog.c GUC, `archive_mode`). The enum GUC stores
    // ARCHIVE_MODE_{OFF,ON,ALWAYS} == {0,1,2} (archive_mode_options[]); the
    // consumers (xlogarchive `XLogArchivingActive`/`XLogArchivingAlways`,
    // walreceiver, the backup-stop archive wait) read it through this seam.
    xs::xlog_archive_mode::set(|| {
        match guc_tables::vars::XLogArchiveMode.read() {
            0 => wal::xlog_consts::ArchiveMode::Off,
            1 => wal::xlog_consts::ArchiveMode::On,
            2 => wal::xlog_consts::ArchiveMode::Always,
            other => panic!("invalid archive_mode GUC value {other}"),
        }
    });

    backup_basebackup_seams::do_pg_backup_start_for_basebackup::set(
        do_pg_backup_start_for_basebackup,
    );
    backup_basebackup_seams::do_pg_abort_backup::set(do_pg_abort_backup_seam);
}
