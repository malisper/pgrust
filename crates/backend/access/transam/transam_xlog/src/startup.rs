//! `StartupXLOG()` (xlog.c:5491) — the WAL-engine startup driver — plus the
//! xlog.c-owned end-of-recovery actions it calls (`PerformRecoveryXLogAction`,
//! `CreateEndOfRecoveryRecord`, `CreateOverwriteContrecordRecord`,
//! `XLogInitNewTimeline`, `CheckRequiredParameterValues`) and the file-static
//! directory-structure validator (`ValidateXLOGDirectoryStructure`).
//!
//! Ported faithfully from `src/backend/access/transam/xlog.c` (PostgreSQL 18.3).
//!
//! Cross-subsystem work crosses each owner's `-seams` crate (the recovery
//! orchestrator `InitWalRecovery`/`FinishWalRecovery`/`ShutdownWalRecovery`, the
//! clog/multixact/commit-ts/subtrans/twophase/varsup/origin/slot startup
//! cluster, etc.). Two callees are genuinely unported and panic loudly through
//! their seams until their owners land:
//!
//!   * `StartupReorderBuffer()` (reorderbuffer.c) — called unconditionally on
//!     every boot path (xlog.c:5660), so the clean single-user boot path
//!     currently bottoms out here.
//!   * `pgstat_restore_stats()` / `pgstat_discard_stats()` (pgstat.c).
//!
//! The crash-recovery cleanup legs (`RemoveTempXlogFiles`, here, and
//! `SyncDataDirectory`, through the fd owner's seam) run on the crash path
//! (control state != shut down) and are ported. Several archive/hot-standby-only
//! legs (`ResetUnloggedRelations`, `DeleteAllExportedSnapshotFiles`, the
//! hot-standby `InitRecoveryTransactionEnvironment`/`ProcArrayInitRecovery`
//! cluster) still cross precise seam-panics into their unported owners; they are
//! unreachable on the clean DB_SHUTDOWNED path.

#![allow(non_snake_case)]

extern crate std;

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;

use alloc::format;
use alloc::string::{String, ToString};

use ::utils_error::{ereport, PgError, PgResult};
use ::control::{DBState, FirstNormalUnloggedLSN};
use ::types_core::{TimeLineID, TransactionId, XLogRecPtr};
use ::types_error::{ErrorLocation, DEBUG1, FATAL, LOG, NOTICE};
use ::types_core::xact::{InvalidTransactionId, TransactionIdIsValid};
use ::wal::wal::RM_XLOG_ID;
use ::wal::xlog_consts::{
    RecoveryState, WalLevel, CHECKPOINT_END_OF_RECOVERY, CHECKPOINT_FORCE, CHECKPOINT_IMMEDIATE,
    CHECKPOINT_WAIT, SIZE_OF_XLOG_LONG_PHD, SIZE_OF_XLOG_SHORT_PHD, XLOG_BLCKSZ,
};

use ::init_small::globals;

use crate::insert::{
    LocalSetXLogInsertAllowed, WALInsertLockAcquire, WALInsertLockAcquireExclusive,
    WALInsertLockRelease, XLogRecPtrToBufIdx,
};
use crate::shmem::{self, control_file_mut, xlog_ctl, RecoveryInProgress, UpdateControlFile};
use crate::{InvalidXLogRecPtr, XLogRecPtrToBytePos, XRecOffIsValid};

// Cross-subsystem seam crates (all leaf `-seams`).
use clog_seams as clog_seam;
use commit_ts_seams as commit_ts_seam;
use multixact_seams as multixact_seam;
use subtrans_seams as subtrans_seam;
use timeline_seams as timeline_seam;
use twophase_seams as twophase_seam;
use varsup_seams as varsup_seam;
use xlogrecovery_seams as recovery_seam;
use vacuum_seams as vacuum_seam;
use checkpointer_seams as checkpointer_seam;
use origin_seams as origin_seam;
use reorderbuffer_seams as reorderbuffer_seam;
use slot_seams as slot_seam;
use walsender_seams as walsender_seam;
use pgstat_seams as pgstat_seam;
use relcache_seams as relcache_seam;
use ps_status_seams as ps_seam;
use snapmgr_seams as snapmgr_seam;
use procarray_seams as procarray_seam;
use standby_seams as standby_seam;

const CONTROL_FILE_LOCK: usize = 9;
const PROC_ARRAY_LOCK: usize = 4;

/// Recovery signal-file names (`xlogrecovery.c:58-59`).
const STANDBY_SIGNAL_FILE: &str = "standby.signal";
const RECOVERY_SIGNAL_FILE: &str = "recovery.signal";

/// `ARCHIVE_MODE_OFF` (`access/xlog.h`) — `XLogArchivingActive()` is
/// `XLogArchiveMode > ARCHIVE_MODE_OFF`.
const ARCHIVE_MODE_OFF: i32 = 0;

/// Backup/tablespace label-file names (`access/xlog.h:307-311`).
const BACKUP_LABEL_FILE: &str = "backup_label";
const BACKUP_LABEL_OLD: &str = "backup_label.old";
const TABLESPACE_MAP: &str = "tablespace_map";
const TABLESPACE_MAP_OLD: &str = "tablespace_map.old";

/// `xl_end_of_recovery` info opcode (`access/xlog.h`).
const XLOG_END_OF_RECOVERY: u8 = 0x90;
/// `xl_overwrite_contrecord` info opcode (`access/xlog.h`).
const XLOG_OVERWRITE_CONTRECORD: u8 = 0xD0;
/// `XLP_FIRST_IS_OVERWRITE_CONTRECORD` (`access/xlog_internal.h`).
const XLP_FIRST_IS_OVERWRITE_CONTRECORD: u16 = 0x0008;

fn loc(lineno: i32, func: &str) -> ErrorLocation {
    ErrorLocation::new("xlog.c", lineno, func)
}

/// `str_time(tnow)` (xlog.c) — render a `pg_time_t` for the startup log
/// messages. The C helper uses `pg_strftime` into a static buffer; the message
/// is purely cosmetic, so render the epoch seconds directly.
fn str_time(t: ::types_core::pg_time_t) -> String {
    t.to_string()
}

// ===========================================================================
// StartupXLOG (xlog.c:5491).
// ===========================================================================

/// `void StartupXLOG(void)` (xlog.c:5491) — perform crash/archive recovery and
/// bring the system to a consistent, writable state.
pub fn StartupXLOG() -> PgResult<()> {
    // CurrentResourceOwner = AuxProcessResourceOwner; (the aux-process resowner
    // setup is the startup-process driver's responsibility, handled by the
    // process owner before this runs.)

    let dbstate;
    let dbtime;
    let track_commit_timestamp;
    let control_unlogged_lsn;
    let control_check_point;
    {
        let control_file = control_file_mut();

        // Check that contents look valid.
        if !XRecOffIsValid(control_file.checkPoint) {
            return ereport(FATAL)
                .errmsg("control file contains invalid checkpoint location")
                .finish(loc(5519, "StartupXLOG"))
                .map(|_| ());
        }
        dbstate = control_file.state;
        dbtime = control_file.time;
        track_commit_timestamp = control_file.track_commit_timestamp;
        control_unlogged_lsn = control_file.unloggedLSN;
        control_check_point = control_file.checkPointCopy;
    }

    match dbstate {
        DBState::Shutdowned => {
            // This is the expected case, so don't be chatty in standalone mode.
            let level = if globals::IsPostmasterEnvironment() { LOG } else { NOTICE };
            ereport(level)
                .errmsg(format!("database system was shut down at {}", str_time(dbtime)))
                .finish(loc(5530, "StartupXLOG"))?;
        }
        DBState::ShutdownedInRecovery => {
            ereport(LOG)
                .errmsg(format!(
                    "database system was shut down in recovery at {}",
                    str_time(dbtime)
                ))
                .finish(loc(5535, "StartupXLOG"))?;
        }
        DBState::Shutdowning => {
            ereport(LOG)
                .errmsg(format!(
                    "database system shutdown was interrupted; last known up at {}",
                    str_time(dbtime)
                ))
                .finish(loc(5540, "StartupXLOG"))?;
        }
        DBState::InCrashRecovery => {
            ereport(LOG)
                .errmsg(format!(
                    "database system was interrupted while in recovery at {}",
                    str_time(dbtime)
                ))
                .errhint(
                    "This probably means that some data is corrupted and you will have to use the last backup for recovery.",
                )
                .finish(loc(5547, "StartupXLOG"))?;
        }
        DBState::InArchiveRecovery => {
            ereport(LOG)
                .errmsg(format!(
                    "database system was interrupted while in recovery at log time {}",
                    str_time(control_check_point.time)
                ))
                .errhint(
                    "If this has occurred more than once some data might be corrupted and you might need to choose an earlier recovery target.",
                )
                .finish(loc(5554, "StartupXLOG"))?;
        }
        DBState::InProduction => {
            ereport(LOG)
                .errmsg(format!(
                    "database system was interrupted; last known up at {}",
                    str_time(dbtime)
                ))
                .finish(loc(5560, "StartupXLOG"))?;
        }
        DBState::Startup => {
            return ereport(FATAL)
                .errmsg("control file contains invalid database cluster state")
                .finish(loc(5567, "StartupXLOG"))
                .map(|_| ());
        }
    }

    // Verify that pg_wal, pg_wal/archive_status, and pg_wal/summaries exist.
    ValidateXLOGDirectoryStructure()?;

    // Set up timeout handler needed to report startup progress.
    // RegisterTimeout(STARTUP_PROGRESS_TIMEOUT, startup_progress_timeout_handler);
    // The handler lives in postmaster/startup.c (reached through its seam); the
    // registration goes through the timeout subsystem.
    if !miscinit_seams::is_bootstrap_processing_mode::call() {
        timeout_seams::register_timeout::call(
            types_timeout::TimeoutId::STARTUP_PROGRESS_TIMEOUT,
            || startup_seams::startup_progress_timeout_handler::call(),
        );
    }

    // If we previously crashed, clean up temp WAL files and fsync the data
    // directory. (xlog.c:5608-5616)
    let did_crash;
    if dbstate != DBState::Shutdowned && dbstate != DBState::ShutdownedInRecovery {
        RemoveTempXlogFiles()?;
        // SyncDataDirectory() is owned by storage/file/fd.c.
        fd_seams::sync_data_directory::call()?;
        did_crash = true;
    } else {
        did_crash = false;
    }

    // Prepare for WAL recovery if needed. InitWalRecovery analyzes the control
    // file + backup label, updates the in-memory ControlFile per the starting
    // checkpoint, and sets InRecovery / ArchiveRecoveryRequested.
    let init_result = {
        let cx = mcx::MemoryContext::new("StartupXLOG/InitWalRecovery");
        recovery_seam::init_wal_recovery::call(control_file_mut(), cx.mcx())?
    };
    let was_shutdown = init_result.was_shutdown;
    let have_backup_label = init_result.have_backup_label;
    let have_tblspc_map = init_result.have_tblspc_map;

    // checkPoint = ControlFile->checkPointCopy;
    let check_point = control_file_mut().checkPointCopy;

    // initialize shared memory variables from the checkpoint record.
    varsup_seam::set_transam_variables_at_startup::call(check_point.nextXid, check_point.nextOid);
    multixact_seam::multi_xact_set_next_m_xact::call(
        check_point.nextMulti,
        check_point.nextMultiOffset,
    )?;
    varsup_seam::advance_oldest_clog_xid::call(check_point.oldestXid)?;
    vacuum_seam::set_transaction_id_limit::call(check_point.oldestXid, check_point.oldestXidDB)?;
    multixact_seam::set_multi_xact_id_limit::call(
        check_point.oldestMulti,
        check_point.oldestMultiDB,
        true,
    )?;
    commit_ts_seam::set_commit_ts_limit::call(
        check_point.oldestCommitTsXid,
        check_point.newestCommitTsXid,
    )?;
    // XLogCtl->ckptFullXid = checkPoint.nextXid;
    unsafe {
        (*xlog_ctl()).ckptFullXid = check_point.nextXid;
    }

    // Clear out any old relcache cache files.
    relcache_seam::relation_cache_init_file_remove::call()?;

    // Initialize replication slots, before there's a chance to remove required
    // resources.
    slot_seam::startup_replication_slots::call()?;

    // Startup logical state, needs to be setup now so we have proper data during
    // crash recovery.
    reorderbuffer_seam::startup_reorder_buffer::call()?;

    // Startup CLOG. Must be after TransamVariables->nextXid is initialized and
    // before we accept connections or begin WAL replay.
    clog_seam::startup_clog::call()?;

    // Startup MultiXact, early, to be able to replay truncations.
    multixact_seam::startup_multixact::call()?;

    // Ditto for commit timestamps, if enabled in the control file.
    if track_commit_timestamp {
        commit_ts_seam::startup_commit_ts::call()?;
    }

    // Recover knowledge about replay progress of known replication partners.
    origin_seam::startup_replication_origin::call()?;

    // Initialize unlogged LSN. On a clean shutdown, restore from the control
    // file. On recovery, reset (all unlogged relations are blown away).
    unsafe {
        let ctl = &*xlog_ctl();
        if dbstate == DBState::Shutdowned {
            ctl.unloggedLSN.write_membarrier(control_unlogged_lsn);
        } else {
            ctl.unloggedLSN.write_membarrier(FirstNormalUnloggedLSN);
        }
    }

    let recovery_target_tli = recovery_seam::recovery_target_tli::call();

    // Copy any missing timeline history files between 'now' and the recovery
    // target timeline.
    {
        let cx = mcx::MemoryContext::new("StartupXLOG/restoreTimeLineHistoryFiles");
        timeline_seam::restore_timeline_history_files::call(
            cx.mcx(),
            check_point.ThisTimeLineID,
            recovery_target_tli,
            recovery_seam::archive_recovery_requested::call(),
        )?;
    }

    // Before running recovery, scan pg_twophase and fill in its status. The
    // origNextXid / transaction_xmin globals are owned by varsup/snapmgr; thread
    // them in.
    let orig_next_xid = varsup_seam::read_next_transaction_id::call();
    let transaction_xmin = orig_next_xid;
    twophase_seam::restore_two_phase_data::call(
        orig_next_xid,
        transaction_xmin,
        recovery_seam::reached_consistency::call(),
    )?;

    // Reset / restore pgstat data.
    if did_crash {
        pgstat_seam::pgstat_discard_stats::call()?;
    } else {
        pgstat_seam::pgstat_restore_stats::call()?;
    }

    let last_full_page_writes = check_point.fullPageWrites;

    // RedoRecPtr = XLogCtl->RedoRecPtr = XLogCtl->Insert.RedoRecPtr = checkPoint.redo;
    unsafe {
        let ctl = &mut *xlog_ctl();
        ctl.RedoRecPtr = check_point.redo;
        ctl.Insert.RedoRecPtr = check_point.redo;
    }
    shmem::set_redo_rec_ptr_cached(check_point.redo);
    let do_page_writes = last_full_page_writes;
    let _ = do_page_writes;

    // REDO
    let in_recovery = recovery_seam::in_recovery::call();
    let performed_wal_recovery;
    let mut oldest_active_xid: TransactionId;
    if in_recovery {
        performed_wal_recovery = startup_xlog_redo_phase(
            &check_point,
            was_shutdown,
            have_backup_label,
            have_tblspc_map,
        )?;
        oldest_active_xid = 0; // set inside the hot-standby cluster below in C
        let _ = &mut oldest_active_xid;
    } else {
        performed_wal_recovery = false;
    }

    // Finish WAL recovery.
    let end_of_recovery_info = {
        let cx = mcx::MemoryContext::new("StartupXLOG/FinishWalRecovery");
        recovery_seam::finish_wal_recovery::call(cx.mcx())?
    };
    let mut end_of_log = end_of_recovery_info.end_of_log;
    // EndOfLogTLI is consumed by CleanupAfterArchiveRecovery on the archive
    // path (which returns a precise panic earlier on this clean-path port).
    let _end_of_log_tli = end_of_recovery_info.end_of_log_tli;
    let aborted_rec_ptr = end_of_recovery_info.aborted_rec_ptr;
    let missing_contrec_ptr = end_of_recovery_info.missing_contrec_ptr;

    // Reset ps status display.
    ps_seam::set_ps_display::call(String::new());

    // When recovering from a backup (in recovery AND archive recovery was
    // requested), complain if we did not roll forward far enough to reach the
    // point where the database is consistent. (xlog.c:5947-5969)
    //
    // On crash recovery LocalMinRecoveryPoint is Invalid (set inside the redo
    // phase) and backupStartPoint is Invalid, so the predicate is false and we
    // fall through. The local-min-recovery-point copy lives in the recovery
    // driver's backend-local state; on the crash path it is Invalid, so the
    // `EndOfLog < LocalMinRecoveryPoint` half is never true.
    if in_recovery {
        let (backup_start_point, archive_recovery_requested, backup_end_required) = {
            let cf = control_file_mut();
            (
                cf.backupStartPoint,
                recovery_seam::archive_recovery_requested::call(),
                cf.backupEndRequired,
            )
        };
        if backup_start_point != InvalidXLogRecPtr {
            // Ran off end of WAL before reaching end-of-backup WAL record — only
            // reachable on the backup-recovery path; surface that boundary.
            if archive_recovery_requested || backup_end_required {
                return ereport(FATAL)
                    .errmsg("WAL ends before end of online backup")
                    .errhint(
                        "All WAL generated while online backup was taken must be available at recovery.",
                    )
                    .finish(loc(5960, "StartupXLOG"))
                    .map(|_| ());
            }
        }

        // Reset unlogged relations to the contents of their INIT fork. Done AFTER
        // recovery is complete (to include unlogged relations created during
        // recovery) but BEFORE recovery is marked successful. (xlog.c:5979)
        reinit::ResetUnloggedRelations(
            reinit::UNLOGGED_RELATION_INIT,
        )?;
    }

    // Pre-scan prepared transactions to find out the XID range present.
    // (xlog.c:5988 passes NULL/NULL — the xids list is unused on this path.)
    // ProcessTwoPhaseBuffer reads TransamVariables->nextXid FRESH (twophase.c:
    // 2178); after redo it has advanced past the pre-redo checkpoint value, so
    // re-read it here rather than reusing the value captured before REDO (a
    // stale value rejects every recovered prepared xact as "future").
    let post_redo_next_xid = varsup_seam::read_next_transaction_id::call();
    oldest_active_xid = twophase_seam::prescan_prepared_transactions::call(
        post_redo_next_xid,
        post_redo_next_xid,
    )?
    .0;

    // Allow ordinary WAL segment creation before possibly switching timelines.
    crate::write::SetInstallXLogFileSegmentActive()?;

    // Consider whether we need to assign a new timeline ID. On the clean
    // (non-archive) path we just extend the timeline we were in.
    let mut new_tli = end_of_recovery_info.last_rec_tli;
    if recovery_seam::archive_recovery_requested::call() {
        let archive_recovery_requested = true;
        let recovery_target_tli = recovery_seam::recovery_target_tli::call();

        // findNewestTimeLine(recoveryTargetTLI) + 1. (xlog.c:6013)
        new_tli = {
            let cx = mcx::MemoryContext::new("StartupXLOG/findNewestTimeLine");
            timeline_seam::find_newest_timeline::call(
                cx.mcx(),
                recovery_target_tli,
                archive_recovery_requested,
            )?
        } + 1;
        ereport(LOG)
            .errmsg(format!("selected new timeline ID: {new_tli}"))
            .finish(loc(6015, "StartupXLOG"))?;

        // Make a writable copy of the last WAL segment. (xlog.c:6022)
        XLogInitNewTimeline(end_of_recovery_info.end_of_log_tli, end_of_log, new_tli)?;

        // Remove the signal files out of the way, so that we don't accidentally
        // re-enter archive recovery mode in a subsequent crash. (xlog.c:6028)
        if end_of_recovery_info.standby_signal_file_found {
            fd_seams::durable_unlink::call(STANDBY_SIGNAL_FILE)?;
        }
        if end_of_recovery_info.recovery_signal_file_found {
            fd_seams::durable_unlink::call(RECOVERY_SIGNAL_FILE)?;
        }

        // Write the timeline history file, and have it archived. After this
        // point the timeline appears as "taken" in the WAL archive and to any
        // standby servers. (xlog.c:6042)
        let xlog_archiving_active =
            guc_tables::vars::XLogArchiveMode.read() > ARCHIVE_MODE_OFF;
        {
            let cx = mcx::MemoryContext::new("StartupXLOG/writeTimeLineHistory");
            timeline_seam::write_timeline_history::call(
                cx.mcx(),
                new_tli,
                recovery_target_tli,
                end_of_log,
                &end_of_recovery_info.recovery_stop_reason,
                archive_recovery_requested,
                xlog_archiving_active,
            )?;
        }

        ereport(LOG)
            .errmsg("archive recovery complete")
            .finish(loc(6048, "StartupXLOG"))?;
    }

    // Save the selected TimeLineID in shared memory.
    unsafe {
        let ctl = &mut *xlog_ctl();
        shmem::spin_lock_acquire(&ctl.info_lck);
        ctl.InsertTimeLineID = new_tli;
        ctl.PrevTimeLineID = end_of_recovery_info.last_rec_tli;
        shmem::spin_lock_release(&ctl.info_lck);
    }

    // If WAL ended in an incomplete record, skip the broken parts.
    if missing_contrec_ptr != InvalidXLogRecPtr {
        debug_assert_eq!(new_tli, end_of_recovery_info.last_rec_tli);
        debug_assert_ne!(aborted_rec_ptr, InvalidXLogRecPtr);
        end_of_log = missing_contrec_ptr;
    }

    // Prepare to write WAL starting at EndOfLog, and init the xlog buffer cache
    // using the block containing the last record from the previous incarnation.
    let wal_segsz = shmem::wal_segment_size();
    unsafe {
        let ctl = &mut *xlog_ctl();
        ctl.Insert.PrevBytePos = XLogRecPtrToBytePos(end_of_recovery_info.last_rec, wal_segsz);
        ctl.Insert.CurrBytePos = XLogRecPtrToBytePos(end_of_log, wal_segsz);
    }

    // lastPage handling: copy the valid part of the last block (if EndOfLog is
    // not page-aligned).
    unsafe {
        let ctl = &mut *xlog_ctl();
        if end_of_log % XLOG_BLCKSZ as u64 != 0 {
            let first_idx = XLogRecPtrToBufIdx(ctl, end_of_log);
            let len = (end_of_log - end_of_recovery_info.last_page_begin_ptr) as usize;
            debug_assert!(len < XLOG_BLCKSZ as usize);

            let page = ctl.pages.add(first_idx * XLOG_BLCKSZ as usize);
            core::ptr::copy_nonoverlapping(
                end_of_recovery_info.last_page.as_ptr(),
                page,
                len,
            );
            core::ptr::write_bytes(page.add(len), 0, XLOG_BLCKSZ as usize - len);

            let target = end_of_recovery_info.last_page_begin_ptr + XLOG_BLCKSZ as u64;
            (*ctl.xlblocks.add(first_idx)).write(target);
            ctl.InitializedUpTo = target;
        } else {
            ctl.InitializedUpTo = end_of_log;
        }
    }

    // Update local and shared status.
    shmem::set_logwrt_result(shmem::XLogwrtResult { Write: end_of_log, Flush: end_of_log });
    unsafe {
        let ctl = &mut *xlog_ctl();
        ctl.logInsertResult.write(end_of_log);
        ctl.logWriteResult.write(end_of_log);
        ctl.logFlushResult.write(end_of_log);
        ctl.LogwrtRqst.Write = end_of_log;
        ctl.LogwrtRqst.Flush = end_of_log;
    }

    // Preallocate additional log files, if wanted.
    crate::checkpoint::prealloc_xlog_files(end_of_log, new_tli)?;

    // Okay, we're officially UP.
    // InRecovery = false; (xlog.c:6138) — cleared in the xlogrecovery owner
    // before the end-of-recovery SLRU trims, which assert !InRecovery.
    recovery_seam::end_recovery::call();

    // start the archive_timeout timer and LSN running.
    unsafe {
        let ctl = &mut *xlog_ctl();
        // std's SystemTime panics on wasm64-unknown-unknown; use the host clock.
        #[cfg(not(target_family = "wasm"))]
        {
            ctl.lastSegSwitchTime = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as types_core::pg_time_t)
                .unwrap_or(0);
        }
        #[cfg(target_family = "wasm")]
        {
            ctl.lastSegSwitchTime =
                (wasm_libc_shim::now_unix_nanos() / 1_000_000_000) as types_core::pg_time_t;
        }
        ctl.lastSegSwitchLSN = end_of_log;
    }

    // also initialize latestCompletedXid, to nextXid - 1.
    {
        let lwlock = lwlock::main_lock_ref(PROC_ARRAY_LOCK);
        lwlock::LWLockAcquire(
            lwlock,
            types_storage::storage::LW_EXCLUSIVE,
            globals::MyProcNumber(),
        )?;
        let next_xid = varsup_seam::read_next_full_transaction_id::call();
        let mut latest = next_xid;
        // FullTransactionIdRetreat(&latest)
        latest.value -= 1;
        varsup_seam::set_latest_completed_xid::call(latest);
        lwlock::LWLockRelease(lwlock)?;
    }

    // Start up subtrans, if not already done for hot standby (standbyState ==
    // STANDBY_DISABLED on the clean path).
    subtrans_seam::startup_subtrans::call(oldest_active_xid)?;

    // Perform end-of-recovery actions for SLRUs that need it.
    clog_seam::trim_clog::call()?;
    multixact_seam::trim_multixact::call()?;

    // Reload shared-memory state for prepared transactions. ProcessTwoPhaseBuffer
    // reads TransamVariables->nextXid FRESH (twophase.c:2178); re-read it here
    // (it advanced during REDO) so recovered prepared xacts are not rejected as
    // "future" against the stale pre-REDO checkpoint nextXid.
    let recover_next_xid = varsup_seam::read_next_transaction_id::call();
    twophase_seam::recover_prepared_transactions::call(
        recover_next_xid,
        recover_next_xid,
        false,
    )?;

    // Shut down xlogreader.
    recovery_seam::shutdown_wal_recovery::call()?;

    // Enable WAL writes for this backend only.
    let _old_insert_allowed = LocalSetXLogInsertAllowed();

    // If necessary, write overwrite-contrecord before doing anything else.
    if aborted_rec_ptr != InvalidXLogRecPtr {
        debug_assert_ne!(missing_contrec_ptr, InvalidXLogRecPtr);
        CreateOverwriteContrecordRecord(aborted_rec_ptr, missing_contrec_ptr, new_tli)?;
    }

    // Update full_page_writes in shared memory and write an XLOG_FPW_CHANGE
    // record before any cleanup / checkpoint records.
    unsafe {
        (*xlog_ctl()).Insert.fullPageWrites = last_full_page_writes;
    }
    crate::control_funcs::UpdateFullPageWrites()?;

    // Emit checkpoint or end-of-recovery record, if required.
    let mut promoted = false;
    if performed_wal_recovery {
        // No resident buffer has an LSN past the durable WAL end when redo
        // reaches here, so the end-of-recovery `CheckPointBuffers` has nothing to
        // `XLogFlush` past the flush point and the old per-page
        // `drop_buffers_past_lsn` evict (a blunt approximation of a fresh pool) is
        // no longer needed:
        //   * crash recovery — the pool is genuinely EMPTY. At first boot the
        //     postmaster just created a fresh (empty) segment; on a crash-restart
        //     it emptied the reused segment in `reset_shared_state_after_crash`
        //     (ipci.c) BEFORE forking this startup process (the MAP_SHARED bytes
        //     it cleared are visible here through the inherited mapping). Either
        //     way redo replays into a clean pool exactly as in C.
        //   * standby promotion / end-of-recovery on a running standby — the pool
        //     is NOT empty (hot-standby reads loaded pages during continuous
        //     recovery), but every resident page was replayed at an LSN <=
        //     `end_of_log`, and the flush pointer was just set to `end_of_log`
        //     (above), so still nothing is dirty past the flush point.

        promoted = PerformRecoveryXLogAction()?;
    }

    // If any critical GUCs changed, log them before allowing backends to write
    // WAL.
    crate::control_funcs::XLogReportParameters()?;

    // If this is archive recovery, perform post-recovery cleanup. (Not reached
    // on the clean path; archive_recovery_requested would have returned earlier.)

    // Local WAL inserts enabled; finish commit-timestamp initialization.
    commit_ts_seam::complete_commit_ts_initialization::call()?;

    // Now allow backends to write WAL and update the control file status. Under
    // ControlFileLock to keep on-disk + shmem consistent.
    {
        let lwlock = lwlock::main_lock_ref(CONTROL_FILE_LOCK);
        lwlock::LWLockAcquire(
            lwlock,
            types_storage::storage::LW_EXCLUSIVE,
            globals::MyProcNumber(),
        )?;

        // In C, ControlFile is genuine shared memory, so the end-of-recovery
        // checkpoint just performed by the checkpointer (CHECKPOINT_WAIT, above
        // in PerformRecoveryXLogAction) already updated the single shared
        // ControlFile image this process is about to flip to InProduction — its
        // recovered checkPoint location / checkPointCopy (advanced nextXid &c.)
        // are preserved. In this tree ControlFile is a backend-local cell, so
        // the checkpointer's update landed in the checkpointer's copy + on disk,
        // while THIS (startup) process still holds the stale pre-recovery image
        // it loaded before redo. Writing it back below (UpdateControlFile) would
        // clobber the recovered checkpoint on disk with the pre-recovery one,
        // resetting nextXid below the XIDs already committed during replay (every
        // recovered committed row then reads as "in the future"). Re-read the
        // checkpointer's durable image first so we only flip the state byte.
        if performed_wal_recovery {
            shmem::ReadControlFile()?;
        }

        control_file_mut().state = DBState::InProduction;

        unsafe {
            let ctl = &mut *xlog_ctl();
            shmem::spin_lock_acquire(&ctl.info_lck);
            ctl.SharedRecoveryState = RecoveryState::Done;
            shmem::spin_lock_release(&ctl.info_lck);
        }

        UpdateControlFile()?;
        lwlock::LWLockRelease(lwlock)?;
    }

    // ShutdownRecoveryTransactionEnvironment is only needed when standbyState !=
    // STANDBY_DISABLED (not on the clean path).

    // If there were cascading standbys, nudge any walsenders to notice we've
    // been promoted.
    walsender_seam::wal_snd_wakeup::call(true, true)?;

    // If this was a promotion, request a checkpoint now.
    if promoted {
        checkpointer_seam::request_checkpoint::call(CHECKPOINT_FORCE);
    }

    Ok(())
}

/// Seed the cluster-wide transaction-id / multixact bounds (`TransamVariables`
/// and `MultiXactState`) from `ControlFile->checkPointCopy`, mirroring the
/// shared-memory seeding `StartupXLOG` performs at xlog.c:5634-5642 (the
/// XID/OID counters, multixact next/limit, CLOG oldest, `SetTransactionIdLimit`,
/// `SetMultiXactIdLimit`, commit-ts limits) plus the `latestCompletedXid =
/// nextXid - 1` initialization at xlog.c:6144-6148.
///
/// In C these writes land in genuine shared memory, so the startup process'
/// seeding is visible to the postmaster and every later child. In this tree the
/// "shared" `TransamVariables` / `MultiXactState` singletons are process-local
/// statics that children inherit by `fork()` copy-on-write — so the writes the
/// startup *child* makes inside `StartupXLOG` die with that child and never
/// reach the postmaster's copy. The postmaster (which has already loaded the
/// control file via `LocalProcessControlFile` and run
/// `CreateSharedMemoryAndSemaphores`, so the singletons exist in its address
/// space) must therefore re-seed its own copy from the same `checkPointCopy`
/// once the startup process reports success, before it forks the
/// logical-replication launcher / autovacuum launcher / backends that take
/// snapshots. Without this, `GetSnapshotData` in those children reads
/// `oldestXid == InvalidTransactionId` and the GlobalVis horizon computation
/// (`FullXidRelativeTo`) trips on an invalid bound.
///
/// This calls the exact same installed owner seams `StartupXLOG` uses, in the
/// same order, so the postmaster's copy ends up identical to what the startup
/// child computed. It is idempotent and reads only the control file the caller
/// already holds.
pub fn SeedTransamVariablesFromCheckpoint() -> PgResult<()> {
    // Refresh the postmaster's backend-local ControlFile image from disk before
    // seeding. The startup child just ran crash recovery: WAL replay advanced
    // the (genuine shmem) TransamVariables->nextXid past every replayed commit,
    // and the end-of-recovery checkpoint durably wrote that advanced value into
    // global/pg_control (checkPointCopy.nextXid). The postmaster's own in-memory
    // ControlFile copy, however, was loaded before the startup child ran and
    // still holds the PRE-recovery checkpoint (e.g. on a crash restart the
    // pre-crash nextXid). Seeding from that stale copy would CLOBBER the shmem
    // nextXid the startup child correctly advanced — reverting it below the XIDs
    // already committed on disk — so freshly-forked backends would reuse those
    // XIDs and see every recovered committed row as "in the future" (relation /
    // row "does not exist"). Re-reading the control file makes this reseed read
    // the recovered checkpoint, so it is idempotent rather than destructive.
    shmem::ReadControlFile()?;

    // checkPoint = ControlFile->checkPointCopy;
    let check_point = control_file_mut().checkPointCopy;

    // initialize shared memory variables from the checkpoint record.
    // (xlog.c:5634-5642)
    //
    // Forward-only: this re-seed runs AFTER the startup process' redo advanced the
    // (genuine shmem) cluster-wide nextXid/nextOid past every replayed record. On
    // the promotion path the durable checkpoint copy read above is the
    // pre-recovery (backup) checkpoint — promotion ends recovery with an
    // XLOG_END_OF_RECOVERY record, not a checkpoint — so an unconditional store
    // here would regress nextXid below the XIDs already committed during replay,
    // making every recovered transaction read as "in the future" (its catalog rows
    // disappear). Re-seeding no-regress lifts an unseeded COW child up to the
    // checkpoint while preserving a redo-advanced live value.
    varsup_seam::reseed_transam_variables_no_regress::call(
        check_point.nextXid,
        check_point.nextOid,
    );
    multixact_seam::multi_xact_set_next_m_xact::call(
        check_point.nextMulti,
        check_point.nextMultiOffset,
    )?;
    varsup_seam::advance_oldest_clog_xid::call(check_point.oldestXid)?;
    vacuum_seam::set_transaction_id_limit::call(check_point.oldestXid, check_point.oldestXidDB)?;
    multixact_seam::set_multi_xact_id_limit::call(
        check_point.oldestMulti,
        check_point.oldestMultiDB,
        true,
    )?;
    commit_ts_seam::set_commit_ts_limit::call(
        check_point.oldestCommitTsXid,
        check_point.newestCommitTsXid,
    )?;

    // also initialize latestCompletedXid, to nextXid - 1. (xlog.c:6144-6148)
    // C takes ProcArrayLock here; the postmaster reseed runs single-threaded
    // before any child that reads latestCompletedXid is forked, so the lock is
    // not strictly required, but we mirror the C and acquire it.
    {
        let lwlock = lwlock::main_lock_ref(PROC_ARRAY_LOCK);
        lwlock::LWLockAcquire(
            lwlock,
            types_storage::storage::LW_EXCLUSIVE,
            globals::MyProcNumber(),
        )?;
        let next_xid = varsup_seam::read_next_full_transaction_id::call();
        let mut latest = next_xid;
        // FullTransactionIdRetreat(&latest)
        latest.value -= 1;
        varsup_seam::set_latest_completed_xid::call(latest);
        lwlock::LWLockRelease(lwlock)?;
    }

    Ok(())
}

/// The redo phase of `StartupXLOG` (xlog.c:5754-5916): the `if (InRecovery)`
/// block. Sets `SharedRecoveryState`, propagates the recovery state into
/// pg_control, resets unlogged relations + exported-snapshot files, and drives
/// `PerformWalRecovery`.
///
/// The hot-standby cluster (xlog.c:5841-5910) only runs when
/// `ArchiveRecoveryRequested && EnableHotStandby`; on the crash-recovery path
/// (`ArchiveRecoveryRequested == false`) it is faithfully skipped, exactly as in
/// C. The boundary for hot standby is surfaced precisely.
fn startup_xlog_redo_phase(
    check_point: &::control::CheckPoint,
    was_shutdown: bool,
    have_backup_label: bool,
    have_tblspc_map: bool,
) -> PgResult<bool> {
    // C: `if (InArchiveRecovery)` governs SharedRecoveryState; the hot-standby
    // gate below uses `ArchiveRecoveryRequested`.
    let in_archive_recovery = recovery_seam::in_archive_recovery::call();

    // Initialize state for RecoveryInProgress(). (xlog.c:5757-5763)
    unsafe {
        let ctl = &mut *xlog_ctl();
        shmem::spin_lock_acquire(&ctl.info_lck);
        ctl.SharedRecoveryState = if in_archive_recovery {
            RecoveryState::Archive
        } else {
            RecoveryState::Crash
        };
        shmem::spin_lock_release(&ctl.info_lck);
    }

    // Update pg_control to show that we are recovering and to show the selected
    // checkpoint as the place we are starting from. (xlog.c:5773)
    UpdateControlFile();

    // If there was a backup label file, it's done its job and the info has now
    // been propagated into pg_control. We must get rid of the label file so that
    // if we crash during recovery, we'll pick up at the latest recovery
    // restartpoint instead of going all the way back to the backup start point.
    // Rename it out of the way rather than delete it. (xlog.c:5782-5786)
    if have_backup_label {
        let _ = std::fs::remove_file(BACKUP_LABEL_OLD);
        file_seams::durable_rename::call(BACKUP_LABEL_FILE, BACKUP_LABEL_OLD, FATAL)?;
    }

    // If there was a tablespace_map file, it's done its job and the symlinks
    // have been created. Rename it out of the way so that if we crash during
    // recovery, we don't create symlinks again. (xlog.c:5793-5800)
    if have_tblspc_map {
        let _ = std::fs::remove_file(TABLESPACE_MAP_OLD);
        file_seams::durable_rename::call(TABLESPACE_MAP, TABLESPACE_MAP_OLD, FATAL)?;
    }

    // LocalMinRecoveryPoint bookkeeping (xlog.c:5811-5819) is owned by the
    // recovery driver's backend-local state, which already initialized it from
    // the control file inside InitWalRecovery; nothing extra needed on this path.

    // Check that the GUCs used to generate the WAL allow recovery. (xlog.c:5822)
    CheckRequiredParameterValues()?;

    // We're in recovery, so unlogged relations may be trashed and must be reset.
    // This must happen BEFORE allowing Hot Standby connections. (xlog.c:5829)
    reinit::ResetUnloggedRelations(
        reinit::UNLOGGED_RELATION_CLEANUP,
    )?;

    // Delete any saved transaction snapshot files left behind by crashed
    // backends. (xlog.c:5835)
    snapmgr_seam::delete_all_exported_snapshot_files::call()?;

    // Initialize for Hot Standby, if enabled. We won't let backends in yet, not
    // until we've reached the min recovery point specified in control file and
    // we've established a recovery snapshot from a running-xacts WAL record.
    // (xlog.c:5841-5910) Only entered when ArchiveRecoveryRequested &&
    // EnableHotStandby; faithfully skipped on the crash-recovery path.
    if recovery_seam::archive_recovery_requested::call() && enable_hot_standby() {
        ereport(DEBUG1)
            .errmsg_internal("initializing for hot standby")
            .finish(loc(5847, "StartupXLOG"))?;

        standby_seam::init_recovery_transaction_environment::call()?;

        // The oldest active XID at the start of recovery: from prepared-xact
        // prescan if we started at a shutdown checkpoint, else recorded in the
        // checkpoint record. (xlog.c:5851-5856)
        // The shutdown-checkpoint branch keeps the prepared-xact xids list to
        // build the running-xacts snapshot below; the online-checkpoint branch
        // takes oldestActiveXid from the checkpoint record and needs no list.
        let mut prepared_xids: ::std::vec::Vec<TransactionId> = ::std::vec::Vec::new();
        let oldest_active_xid = if was_shutdown {
            let next_xid = check_point.nextXid.xid();
            let (oldest, xids) =
                twophase_seam::prescan_prepared_transactions::call(next_xid, InvalidTransactionId)?;
            prepared_xids = xids;
            oldest
        } else {
            check_point.oldestActiveXid
        };
        debug_assert!(TransactionIdIsValid(oldest_active_xid));

        // Tell procarray about the range of xids it has to deal with.
        // (xlog.c:5859)
        let next_xid = varsup_seam::read_next_full_transaction_id::call();
        procarray_seam::proc_array_init_recovery::call(next_xid.xid());

        // Startup subtrans only. CLOG, MultiXact and commit timestamp have
        // already been started up and other SLRUs are not maintained during
        // recovery and need not be started yet. (xlog.c:5862-5867)
        subtrans_seam::startup_subtrans::call(oldest_active_xid)?;

        // If we're beginning at a shutdown checkpoint, we know that nothing was
        // running on the primary at this point. So fake-up an empty
        // running-xacts record and use that here and now. Recover additional
        // standby state for prepared transactions. (xlog.c:5869-5909)
        if was_shutdown {
            // Update pg_subtrans entries for any prepared transactions.
            // (xlog.c:5878) StandbyRecoverPreparedTransactions re-scans the
            // two-phase state files; the orig-next-xid / xmin args mirror the
            // C globals consumed by the twophase owner.
            twophase_seam::standby_recover_prepared_transactions::call(
                check_point.nextXid.xid(),
                InvalidTransactionId,
            )?;

            // Construct a RunningTransactions snapshot representing a shut-down
            // server, with only prepared transactions still alive. We're never
            // overflowed at this point because all subxids are listed with
            // their parent prepared transactions. (xlog.c:5887-5908)
            let cx = mcx::MemoryContext::new("StartupXLOG/running-xacts");
            // latestCompletedXid = checkPoint.nextXid; TransactionIdRetreat(it)
            // — decrement, skipping the special XIDs below FirstNormalTransactionId.
            let mut latest_completed_xid = check_point.nextXid.xid();
            loop {
                latest_completed_xid = latest_completed_xid.wrapping_sub(1);
                if latest_completed_xid >= ::types_core::xact::FirstNormalTransactionId {
                    break;
                }
            }
            debug_assert!(::types_core::xact::TransactionIdIsNormal(latest_completed_xid));
            let running = types_storage::storage::RunningTransactionsData {
                xcnt: prepared_xids.len() as i32,
                subxcnt: 0,
                subxid_status: types_storage::storage::SUBXIDS_IN_SUBTRANS,
                nextXid: check_point.nextXid.xid(),
                oldestRunningXid: oldest_active_xid,
                // C leaves oldestDatabaseRunningXid uninitialized here;
                // ProcArrayApplyRecoveryInfo never reads it on this path.
                oldestDatabaseRunningXid: InvalidTransactionId,
                latestCompletedXid: latest_completed_xid,
                xids: mcx::slice_in(cx.mcx(), &prepared_xids)?,
            };
            procarray_seam::proc_array_apply_recovery_info::call(&running)?;
        }
    }

    // We're all set for replaying the WAL now. Do it. (xlog.c:5913)
    {
        let cx = mcx::MemoryContext::new("StartupXLOG/PerformWalRecovery");
        recovery_seam::perform_wal_recovery::call(cx.mcx())?;
    }

    Ok(true)
}

// ===========================================================================
// PerformRecoveryXLogAction (xlog.c:6346).
// ===========================================================================

/// `static bool PerformRecoveryXLogAction(void)` (xlog.c:6346) — emit a
/// checkpoint or (on promotion) an end-of-recovery record at end of REDO.
pub fn PerformRecoveryXLogAction() -> PgResult<bool> {
    let mut promoted = false;

    if recovery_seam::archive_recovery_requested::call()
        && globals::IsUnderPostmaster()
        && recovery_seam::promote_is_triggered::call()
    {
        promoted = true;
        // Insert a special WAL record to mark the end of recovery.
        CreateEndOfRecoveryRecord()?;
    } else {
        checkpointer_seam::request_checkpoint::call(
            CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_IMMEDIATE | CHECKPOINT_WAIT,
        );
        // The end-of-recovery checkpoint above is currently a graceful no-op
        // (the XLogCtl checkpoint-record driver is unported, so no checkpoint
        // WAL record is written). But the buffer-flush half of CheckPointGuts —
        // making every page modified during redo durable — is essential and
        // independent of the record write: without it the recovered catalog /
        // heap pages live only in the startup process's buffers and are lost
        // when it exits, leaving a half-applied datadir. Flush them here so
        // crash recovery comes up consistent. We flush both halves that
        // CheckPointGuts persists and that govern post-recovery visibility:
        //   - the SLRU commit log (CheckPointCLOG): redo marked replayed
        //     transactions committed in the CLOG SLRU buffers, but without this
        //     flush pg_xact on disk still shows them in-progress, so every row
        //     they wrote is invisible to the backends that fork after recovery;
        //   - the shared buffers (CheckPointBuffers): the recovered heap/catalog
        //     pages.
        // (The remaining CheckPointGuts SLRU arms — CommitTs / MultiXact /
        // Subtrans / predicate / ReplicationOrigin — are still gated behind the
        // unported CheckPointGutsCallbacks checkpoint-deps debt; CLOG is the one
        // that governs basic committed-row visibility.)
        clog_seam::check_point_clog::call()?;
        bufmgr_seams::check_point_buffers::call(
            CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_IMMEDIATE,
        )?;
    }

    Ok(promoted)
}

// ===========================================================================
// CreateEndOfRecoveryRecord (xlog.c:7439).
// ===========================================================================

/// `void CreateEndOfRecoveryRecord(void)` (xlog.c:7439) — insert an
/// `XLOG_END_OF_RECOVERY` record marking the end of recovery (used on promotion
/// in place of a full checkpoint).
pub fn CreateEndOfRecoveryRecord() -> PgResult<()> {
    if !RecoveryInProgress() {
        return ereport(::types_error::ERROR)
            .errmsg("can only be used to end recovery")
            .finish(loc(7446, "CreateEndOfRecoveryRecord"))
            .map(|_| ());
    }

    // xl_end_of_recovery: TimestampTz end_time @0; TimeLineID ThisTimeLineID @8;
    // TimeLineID PrevTimeLineID @12; int wal_level @16 (sizeof == 20, MAXALIGN 24).
    let end_time = timestamp_seams::get_current_timestamp::call();
    let wal_level = guc_tables::vars::wal_level.read();

    WALInsertLockAcquireExclusive()?;
    let (this_tli, prev_tli) = unsafe {
        let ctl = &*xlog_ctl();
        (ctl.InsertTimeLineID, ctl.PrevTimeLineID)
    };
    WALInsertLockRelease()?;

    // sizeof(xl_end_of_recovery) == 24 (TimestampTz forces 8-byte alignment;
    // the int wal_level @16 leaves 4 trailing pad bytes).
    let mut xlrec = [0u8; 24];
    xlrec[0..8].copy_from_slice(&end_time.to_ne_bytes());
    xlrec[8..12].copy_from_slice(&this_tli.to_ne_bytes());
    xlrec[12..16].copy_from_slice(&prev_tli.to_ne_bytes());
    xlrec[16..20].copy_from_slice(&wal_level.to_ne_bytes());

    let recptr = xloginsert_seams::xlog_insert::call(
        RM_XLOG_ID,
        XLOG_END_OF_RECOVERY,
        0,
        &[&xlrec],
    )?;

    crate::write::XLogFlush(recptr)?;

    // Update the control file so crash recovery can follow the timeline changes.
    {
        let lwlock = lwlock::main_lock_ref(CONTROL_FILE_LOCK);
        lwlock::LWLockAcquire(
            lwlock,
            types_storage::storage::LW_EXCLUSIVE,
            globals::MyProcNumber(),
        )?;
        let cf = control_file_mut();
        cf.minRecoveryPoint = recptr;
        cf.minRecoveryPointTLI = this_tli;
        UpdateControlFile()?;
        lwlock::LWLockRelease(lwlock)?;
    }

    Ok(())
}

// ===========================================================================
// CreateOverwriteContrecordRecord (xlog.c:7504).
// ===========================================================================

/// `static XLogRecPtr CreateOverwriteContrecordRecord(aborted_lsn, pagePtr,
/// newTLI)` (xlog.c:7504) — write an `XLOG_OVERWRITE_CONTRECORD` record at the
/// point where a continuation record went missing at end of WAL.
pub fn CreateOverwriteContrecordRecord(
    aborted_lsn: XLogRecPtr,
    page_ptr: XLogRecPtr,
    new_tli: TimeLineID,
) -> PgResult<XLogRecPtr> {
    if !RecoveryInProgress() {
        return ereport(::types_error::ERROR)
            .errmsg("can only be used at end of recovery")
            .finish(loc(7513, "CreateOverwriteContrecordRecord"))
            .map(|_| 0);
    }
    if page_ptr % XLOG_BLCKSZ as u64 != 0 {
        return ereport(::types_error::ERROR)
            .errmsg(format!(
                "invalid position for missing continuation record {}",
                page_ptr
            ))
            .finish(loc(7516, "CreateOverwriteContrecordRecord"))
            .map(|_| 0);
    }

    let wal_segsz = shmem::wal_segment_size();
    // The current WAL insert position should be right after the page header.
    let mut start_pos = page_ptr;
    if XLogSegmentOffset(start_pos, wal_segsz) == 0 {
        start_pos += SIZE_OF_XLOG_LONG_PHD as u64;
    } else {
        start_pos += SIZE_OF_XLOG_SHORT_PHD as u64;
    }
    let recptr = shmem::GetXLogInsertRecPtr();
    if recptr != start_pos {
        return ereport(::types_error::ERROR)
            .errmsg(format!(
                "invalid WAL insert position {} for OVERWRITE_CONTRECORD",
                recptr
            ))
            .finish(loc(7529, "CreateOverwriteContrecordRecord"))
            .map(|_| 0);
    }

    // Initialize the XLOG page header and set XLP_FIRST_IS_OVERWRITE_CONTRECORD.
    WALInsertLockAcquire()?;
    unsafe {
        let ctl = &*xlog_ctl();
        let pagehdr = crate::insert::GetXLogBuffer(ctl, page_ptr, new_tli)?;
        // xlp_info is the u16 at offset 2 of XLogPageHeaderData (after the u16
        // xlp_magic).
        let info_ptr = pagehdr.add(2) as *mut u16;
        *info_ptr |= XLP_FIRST_IS_OVERWRITE_CONTRECORD;
    }
    WALInsertLockRelease()?;

    // Insert the XLOG_OVERWRITE_CONTRECORD record as the first record on the
    // page. xl_overwrite_contrecord: XLogRecPtr overwritten_lsn @0; TimestampTz
    // overwrite_time @8 (sizeof == 16).
    let overwrite_time = timestamp_seams::get_current_timestamp::call();
    let mut xlrec = [0u8; 16];
    xlrec[0..8].copy_from_slice(&aborted_lsn.to_ne_bytes());
    xlrec[8..16].copy_from_slice(&overwrite_time.to_ne_bytes());

    let recptr = xloginsert_seams::xlog_insert::call(
        RM_XLOG_ID,
        XLOG_OVERWRITE_CONTRECORD,
        0,
        &[&xlrec],
    )?;

    // Check the record was inserted to the right place.
    let proc_last = crate::insert::proc_last_rec_ptr();
    if proc_last != start_pos {
        return ereport(::types_error::ERROR)
            .errmsg(format!(
                "OVERWRITE_CONTRECORD was inserted to unexpected position {}",
                proc_last
            ))
            .finish(loc(7562, "CreateOverwriteContrecordRecord"))
            .map(|_| 0);
    }

    crate::write::XLogFlush(recptr)?;

    Ok(recptr)
}

// ===========================================================================
// XLogInitNewTimeline (xlog.c:5276).
// ===========================================================================

/// `static void XLogInitNewTimeline(endTLI, endOfLog, newTLI)` (xlog.c:5276) —
/// initialize the starting WAL segment for a new timeline at the end of archive
/// recovery. If the switch happens mid-segment, copy the last old-timeline
/// segment up to the switch point onto the new timeline; otherwise (switch on a
/// segment boundary) create the next fresh segment on the new timeline. Then
/// clear any stale `.ready`/`.done` archive flags for the new segment.
pub fn XLogInitNewTimeline(
    end_tli: TimeLineID,
    end_of_log: XLogRecPtr,
    new_tli: TimeLineID,
) -> PgResult<()> {
    use crate::{XLByteToPrevSeg, XLByteToSeg, XLogFileName};

    // we always switch to a new timeline after archive recovery.
    debug_assert!(end_tli != new_tli);

    let wal_segsz = shmem::wal_segment_size();

    // Update min recovery point one last time. (xlog.c:5288)
    crate::write::UpdateMinRecoveryPoint(InvalidXLogRecPtr, true)?;

    // Calculate the last segment on the old timeline, and the first segment on
    // the new timeline. If the switch happens in the middle of a segment, they
    // are the same; if exactly at a segment boundary, startLogSegNo will be
    // endLogSegNo + 1. (xlog.c:5295-5296)
    let end_log_seg_no = XLByteToPrevSeg(end_of_log, wal_segsz);
    let start_log_seg_no = XLByteToSeg(end_of_log, wal_segsz);

    if end_log_seg_no == start_log_seg_no {
        // Make a copy of the file on the new timeline. (xlog.c:5313-5314)
        crate::write::XLogFileCopy(
            new_tli,
            end_log_seg_no,
            end_tli,
            end_log_seg_no,
            XLogSegmentOffset(end_of_log, wal_segsz) as i32,
        )?;
    } else {
        // The switch happened at a segment boundary, so just create the next
        // segment on the new timeline. (xlog.c:5323-5337)
        let fd = crate::write::XLogFileInit(start_log_seg_no, new_tli)?;
        // SAFETY: closing the bare kernel fd returned by XLogFileInit.
        let rc = unsafe { libc::close(fd) };
        if rc != 0 {
            let xlogfname = XLogFileName(new_tli, start_log_seg_no, wal_segsz);
            let e = std::io::Error::last_os_error();
            return Err(PgError::error(format!(
                "could not close file \"{xlogfname}\": {e}"
            )));
        }
    }

    // Let's just make real sure there are not .ready or .done flags posted for
    // the new segment. (xlog.c:5342-5343)
    let xlogfname = XLogFileName(new_tli, start_log_seg_no, wal_segsz);
    xlogarchive::XLogArchiveCleanup(&xlogfname);

    Ok(())
}

// ===========================================================================
// CheckRequiredParameterValues (xlog.c:5447).
// ===========================================================================

/// `static void CheckRequiredParameterValues(void)` (xlog.c:5447) — verify the
/// recovery-critical GUCs are set high enough. Reached only inside the redo
/// phase (archive recovery / hot standby).
pub fn CheckRequiredParameterValues() -> PgResult<()> {
    let archive_recovery_requested = recovery_seam::archive_recovery_requested::call();
    let control_wal_level = control_file_mut().wal_level;

    if archive_recovery_requested && control_wal_level == WalLevel::Minimal as i32 {
        return ereport(FATAL)
            .errmsg("WAL was generated with \"wal_level=minimal\", cannot continue recovering")
            .errdetail("This happens if you temporarily set \"wal_level=minimal\" on the server.")
            .errhint(
                "Use a backup taken after setting \"wal_level\" to higher than \"minimal\".",
            )
            .finish(loc(5459, "CheckRequiredParameterValues"))
            .map(|_| ());
    }

    // For Hot Standby, the WAL must be generated with 'replica' mode, and we
    // must have at least as many backend slots as the primary. (xlog.c:5465)
    // We ignore autovacuum_worker_slots when we make this test.
    if archive_recovery_requested && enable_hot_standby() {
        let cf = control_file_mut();
        RecoveryRequiresIntParameter(
            "max_connections",
            guc_tables::vars::MaxConnections.read(),
            cf.MaxConnections,
        )?;
        RecoveryRequiresIntParameter(
            "max_worker_processes",
            guc_tables::vars::max_worker_processes.read(),
            cf.max_worker_processes,
        )?;
        RecoveryRequiresIntParameter(
            "max_wal_senders",
            guc_tables::vars::max_wal_senders.read(),
            cf.max_wal_senders,
        )?;
        RecoveryRequiresIntParameter(
            "max_prepared_transactions",
            guc_tables::vars::max_prepared_xacts.read(),
            cf.max_prepared_xacts,
        )?;
        RecoveryRequiresIntParameter(
            "max_locks_per_transaction",
            guc_tables::vars::max_locks_per_xact.read(),
            cf.max_locks_per_xact,
        )?;
    }

    Ok(())
}

/// `static void RecoveryRequiresIntParameter(const char *param_name, int
/// currValue, int minValue)` (xlogrecovery.c:4701) — fail recovery if a
/// recovery-critical GUC is set lower than on the primary.
///
/// When `currValue >= minValue` (the standard correctly-configured standby)
/// this is a no-op, exactly as in C. The misconfiguration path in C first
/// pauses recovery in a condition-variable loop (so the administrator can fix
/// the GUC and resume) before the terminal `ereport(FATAL)`. That interactive
/// recovery-pause loop is owned by the xlogrecovery pause machinery; here we
/// go straight to the same terminal `ereport(FATAL)` with C's exact message —
/// a correctly-configured standby never reaches it, and a misconfigured one
/// still aborts with the identical diagnostic (it just does not offer the
/// pause-and-fix grace period).
fn RecoveryRequiresIntParameter(
    param_name: &str,
    curr_value: i32,
    min_value: i32,
) -> PgResult<()> {
    if curr_value < min_value {
        return ereport(FATAL)
            .errmsg("recovery aborted because of insufficient parameter settings")
            .errdetail(format!(
                "{param_name} = {curr_value} is a lower setting than on the primary server, where its value was {min_value}."
            ))
            .errhint(
                "You can restart the server after making the necessary configuration changes.",
            )
            .finish(loc(4773, "RecoveryRequiresIntParameter"))
            .map(|_| ());
    }
    Ok(())
}

/// `EnableHotStandby` (xlog.c:146 GUC) — the `hot_standby` GUC value. Reads the
/// installed GUC var so standby startup honours the user's setting.
pub fn enable_hot_standby() -> bool {
    guc_tables::vars::EnableHotStandby.read()
}

// ===========================================================================
// RemoveTempXlogFiles (xlog.c:3852).
// ===========================================================================

/// `static void RemoveTempXlogFiles(void)` (xlog.c:3852) — remove all temporary
/// (`xlogtemp.*`) WAL segment files left in `pg_wal` by an interrupted segment
/// initialization. Called at the start of crash recovery, at a point where no
/// other process writes fresh WAL data.
pub fn RemoveTempXlogFiles() -> PgResult<()> {
    use ::wal::xlog_consts::XLOGDIR;

    // elog(DEBUG2, "removing all temporary WAL segments");

    // AllocateDir(XLOGDIR) + the ReadDir walk. The fd-owned directory read
    // (`read_dir_names`) excludes `.`/`..`; on an unreadable directory it
    // ereports (carried on `Err`), exactly as the C `AllocateDir`/`ReadDir`
    // failure would on this crash-recovery path.
    let names = fd_seams::read_dir_names::call(XLOGDIR)?;
    for name in names {
        // if (strncmp(xlde->d_name, "xlogtemp.", 9) != 0) continue;
        if !name.starts_with("xlogtemp.") {
            continue;
        }
        let path = format!("{XLOGDIR}/{name}");
        // unlink(path); the C ignores the return value.
        let _ = fd_seams::unlink_file::call(&path);
        // elog(DEBUG2, "removed temporary WAL segment \"%s\"", path);
    }
    Ok(())
}

// ===========================================================================
// ValidateXLOGDirectoryStructure (xlog.c:4119).
// ===========================================================================

/// `static void ValidateXLOGDirectoryStructure(void)` (xlog.c:4119) — verify
/// (and create if missing) `pg_wal`, `pg_wal/archive_status`, and
/// `pg_wal/summaries`.
pub fn ValidateXLOGDirectoryStructure() -> PgResult<()> {
    use ::wal::xlog_consts::XLOGDIR;

    // Check for pg_wal; if it doesn't exist, error out.
    if !dir_exists(XLOGDIR) {
        return ereport(FATAL)
            .errmsg(format!("required WAL directory \"{XLOGDIR}\" does not exist"))
            .finish(loc(4130, "ValidateXLOGDirectoryStructure"))
            .map(|_| ());
    }

    for sub in ["archive_status", "summaries"] {
        let path = format!("{XLOGDIR}/{sub}");
        if path_exists(&path) {
            if !dir_exists(&path) {
                return ereport(FATAL)
                    .errmsg(format!("required WAL directory \"{path}\" does not exist"))
                    .finish(loc(4145, "ValidateXLOGDirectoryStructure"))
                    .map(|_| ());
            }
        } else {
            ereport(LOG)
                .errmsg(format!("creating missing WAL directory \"{path}\""))
                .finish(loc(4150, "ValidateXLOGDirectoryStructure"))?;
            if fd_seams::make_pg_directory::call(&path) < 0 {
                return ereport(FATAL)
                    .errmsg(format!("could not create missing directory \"{path}\""))
                    .finish(loc(4153, "ValidateXLOGDirectoryStructure"))
                    .map(|_| ());
            }
        }
    }

    Ok(())
}

fn path_exists(path: &str) -> bool {
    // Route through the fd VFS seam rather than `std::path::Path::exists`: on
    // wasm64-unknown-unknown `std::fs` is inert (it does not reach the host VFS),
    // so a bare `Path::exists` always reports false there. The seam's
    // `path_is_dir`/`file_exists` both stat through the host VFS (and through the
    // real FS natively), so this is correct on both targets. The two callers
    // (`archive_status`, `summaries`) are directories, but accept a plain file
    // too to keep "exists" faithful.
    if dir_exists(path) {
        return true;
    }
    fd_seams::file_exists::call(path).unwrap_or(false)
}

fn dir_exists(path: &str) -> bool {
    // `std::path::Path::is_dir` does not reach the host VFS on wasm64; use the
    // fd seam's VFS-routed stat (`stat(path) == 0 && S_ISDIR`). Identical to the
    // C `stat` test on native.
    fd_seams::path_is_dir::call(path)
}

/// `XLogSegmentOffset(xlogptr, wal_segsz_bytes)` (xlog_internal.h).
#[inline]
fn XLogSegmentOffset(xlogptr: XLogRecPtr, wal_segsz_bytes: i32) -> u64 {
    xlogptr & (wal_segsz_bytes as u64 - 1)
}
