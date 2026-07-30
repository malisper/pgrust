use std::cell::Cell;
use std::sync::atomic::Ordering::{Relaxed, Release, SeqCst};

use elog::{elog, ereport};
use lwlock::{LWLockAcquire, LWLockRelease, LW_EXCLUSIVE};
use types_core::TransactionId;
use types_error::{
    ErrorLocation, PgError, PgResult, DEBUG1, DEBUG2, ERRCODE_DATA_CORRUPTED, ERROR, FATAL, LOG,
    NOTICE, PANIC,
};
use types_core::XLogRecPtr;

use crate::control_file::{control_file, control_file_update};
use crate::ctl::{ControlFileLock, XLogCtl, XLogRecPtrToBufIdx};
use crate::insert::{
    LocalSetXLogInsertAllowed, RecoveryInProgress,
    WALInsertLockAcquireExclusive, WALInsertLockRelease,
};
use crate::write::{PreallocXlogFiles, XLogFlush};
use crate::*;

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

const STANDBY_SIGNAL_FILE: &str = "standby.signal";
const RECOVERY_SIGNAL_FILE: &str = "recovery.signal";
// wait_event_names.txt WaitEventIPC ordinals.
const WAIT_EVENT_ARCHIVE_CLEANUP_COMMAND: u32 = 0x0800_0001;
const WAIT_EVENT_RECOVERY_END_COMMAND: u32 = 0x0800_002E;

thread_local! {
    static LAST_FULL_PAGE_WRITES: Cell<bool> = const { Cell::new(false) };
    static CKPT_SEGS_ADDED: Cell<u64> = const { Cell::new(0) };
    static CKPT_SEGS_REMOVED: Cell<u64> = const { Cell::new(0) };
    static CKPT_SEGS_RECYCLED: Cell<u64> = const { Cell::new(0) };
    static CKPT_SLRU_WRITTEN: Cell<u64> = const { Cell::new(0) };
    // CheckpointStats.ckpt_write_t / ckpt_sync_t / ckpt_sync_end_t (xlog.c):
    // set in CheckPointGuts, consumed by the LogCheckpointEnd analog.
    static CKPT_WRITE_T: Cell<i64> = const { Cell::new(0) };
    static CKPT_SYNC_T: Cell<i64> = const { Cell::new(0) };
    static CKPT_SYNC_END_T: Cell<i64> = const { Cell::new(0) };
}

// TimestampDifferenceMilliseconds (utils/adt/timestamp.c): clamp negatives to
// 0, ceiling-divide microseconds, saturate at INT32_MAX seconds' worth.
fn timestamp_difference_milliseconds(start_time: i64, stop_time: i64) -> i64 {
    if start_time >= stop_time {
        return 0;
    }
    let diff = stop_time - start_time;
    if diff >= i32::MAX as i64 * 1000 - 999 {
        i32::MAX as i64 * 1000
    } else {
        (diff + 999) / 1000
    }
}

// LogCheckpointEnd's unconditional arm (xlog.c:6730-6738): accumulate the
// write and sync phase durations into PendingCheckpointerStats, in
// milliseconds, regardless of log_checkpoints. The seams are uninstalled in
// substrate test binaries without pgstat; skipping there loses only stats.
fn accumulate_checkpoint_timing() {
    let write_msecs = timestamp_difference_milliseconds(CKPT_WRITE_T.get(), CKPT_SYNC_T.get());
    let sync_msecs = timestamp_difference_milliseconds(CKPT_SYNC_T.get(), CKPT_SYNC_END_T.get());
    if pgstat_seams::pgstat_count_checkpointer_write_time::is_installed() {
        pgstat_seams::pgstat_count_checkpointer_write_time::call(write_msecs);
    }
    if pgstat_seams::pgstat_count_checkpointer_sync_time::is_installed() {
        pgstat_seams::pgstat_count_checkpointer_sync_time::call(sync_msecs);
    }
}

pub(crate) fn count_ckpt_slru_written() {
    CKPT_SLRU_WRITTEN.set(CKPT_SLRU_WRITTEN.get() + 1);
}
pub(crate) fn checkpoint_stats_bump_segs_added() {
    CKPT_SEGS_ADDED.set(CKPT_SEGS_ADDED.get() + 1);
}
pub(crate) fn checkpoint_stats_bump_segs_removed() {
    CKPT_SEGS_REMOVED.set(CKPT_SEGS_REMOVED.get() + 1);
}
pub(crate) fn checkpoint_stats_bump_segs_recycled() {
    CKPT_SEGS_RECYCLED.set(CKPT_SEGS_RECYCLED.get() + 1);
}
pub(crate) fn set_last_full_page_writes(v: bool) {
    LAST_FULL_PAGE_WRITES.set(v);
}

fn data_path(rel: &str) -> String {
    let dir = init_small::globals::DataDir().unwrap_or(".");
    format!("{dir}/{rel}")
}

pub(crate) fn ValidateXLOGDirectoryStructure() -> PgResult<()> {
    let pg_wal = data_path(XLOGDIR);
    let mut fi = fd::FileInfo::zeroed();
    if !(fd::pg_stat(&pg_wal, &mut fi) == 0 && fi.is_dir()) {
        return ereport(FATAL)
            .errmsg(format!("required WAL directory \"{XLOGDIR}\" does not exist"))
            .finish(loc("ValidateXLOGDirectoryStructure"));
    }
    for sub in ["archive_status", "summaries"] {
        let path = format!("{pg_wal}/{sub}");
        let mut fi = fd::FileInfo::zeroed();
        if fd::pg_stat(&path, &mut fi) == 0 {
            if !fi.is_dir() {
                return ereport(FATAL)
                    .errmsg(format!("required WAL directory \"{XLOGDIR}/{sub}\" does not exist"))
                    .finish(loc("ValidateXLOGDirectoryStructure"));
            }
        } else {
            let _ = elog(LOG, format!("creating missing WAL directory \"{XLOGDIR}/{sub}\""));
            if fd::MakePGDirectory(&path) < 0 {
                return ereport(FATAL)
                    .errmsg(format!("could not create missing directory \"{XLOGDIR}/{sub}\""))
                    .finish(loc("ValidateXLOGDirectoryStructure"));
            }
        }
    }
    Ok(())
}

fn RemoveTempXlogFiles() -> PgResult<()> {
    let dir = data_path(XLOGDIR);
    // Silent on an unopenable dir, like the pre-P1 shape.
    let d = fd::AllocateDir(&dir)?;
    if d.is_none() {
        return Ok(());
    }
    while let Some(de) = fd::ReadDirExtended(d, &dir, types_error::DEBUG1)? {
        if de.d_name.starts_with("xlogtemp.") {
            let _ = fd::pg_unlink(&format!("{dir}/{}", de.d_name));
        }
    }
    fd::FreeDir(d)?;
    Ok(())
}

fn dir_is_empty(rel: &str) -> bool {
    let dir = data_path(rel);
    let Ok(d) = fd::AllocateDir(&dir) else { return true };
    if d.is_none() {
        return true;
    }
    let first = fd::ReadDirExtended(d, &dir, types_error::DEBUG1);
    let empty = !matches!(first, Ok(Some(_)));
    let _ = fd::FreeDir(d);
    empty
}

// RS_INVAL_* mask bits (replication/slot.h) for the checkpoint slot sweep;
// the seam takes a plain u32 mask so transam_xlog needs no slot dependency.
const RS_INVAL_WAL_REMOVED: i32 = 1 << 0;
const RS_INVAL_IDLE_TIMEOUT: i32 = 1 << 3;

// Unported-unit guard: no-op only when provably no-op, loud panic otherwise.
fn require_empty_or_seam(rel: &str, installed: bool, what: &str) {
    if !installed && !dir_is_empty(rel) {
        panic!("{what} is not ported and {rel} is not empty");
    }
}

pub fn StartupXLOG() -> PgResult<()> {
    let ctl = XLogCtl();

    if !XRecOffIsValid(control_file().checkPoint) {
        return ereport(FATAL)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg("control file contains invalid checkpoint location")
            .finish(loc("StartupXLOG"));
    }

    let state = control_file().state;
    let announce = |msg: String| {
        let level = if init_small::globals::IsPostmasterEnvironment() { LOG } else { NOTICE };
        let _ = elog(level, msg);
    };
    match state {
        DB_SHUTDOWNED => announce("database system was shut down".into()),
        DB_SHUTDOWNED_IN_RECOVERY => announce("database system was shut down in recovery".into()),
        DB_SHUTDOWNING => announce("database system shutdown was interrupted; last known up".into()),
        DB_IN_CRASH_RECOVERY => announce("database system was interrupted while in recovery".into()),
        DB_IN_ARCHIVE_RECOVERY => {
            announce("database system was interrupted while in recovery at log time".into())
        }
        DB_IN_PRODUCTION => announce("database system was interrupted; last known up".into()),
        _ => {
            return ereport(FATAL)
                .errcode(ERRCODE_DATA_CORRUPTED)
                .errmsg("control file contains invalid database cluster state")
                .finish(loc("StartupXLOG"));
        }
    }

    ValidateXLOGDirectoryStructure()?;

    // C: RegisterTimeout(STARTUP_PROGRESS_TIMEOUT, ...) unless bootstrapping;
    // the installed fn (postmaster_startup) does the bootstrap check itself.
    if startup_seams::register_startup_progress_timeout::is_installed() {
        startup_seams::register_startup_progress_timeout::call();
    }

    let did_crash = if state != DB_SHUTDOWNED && state != DB_SHUTDOWNED_IN_RECOVERY {
        RemoveTempXlogFiles()?;
        fd::SyncDataDirectory()?;
        true
    } else {
        false
    };

    let init = xlogrecovery_seams::init_wal_recovery::call()?;
    let was_shutdown = init.was_shutdown;
    let check_point = control_file().checkPointCopy;

    procarray::TransamVariables().nextXid.store(check_point.nextXid.value, Relaxed);
    varsup::TransamVariables().nextOid.store(check_point.nextOid, Relaxed);
    varsup::TransamVariables().oidCount.store(0, Relaxed);
    if multixact_seams::multixact_set_next_mxact::is_installed() {
        multixact_seams::multixact_set_next_mxact::call(
            check_point.nextMulti,
            check_point.nextMultiOffset,
        );
    }
    varsup_seams::advance_oldest_clog_xid::call(check_point.oldestXid)?;
    varsup_seams::set_transaction_id_limit::call(check_point.oldestXid, check_point.oldestXidDB)?;
    if multixact_seams::set_multixact_id_limit::is_installed() {
        multixact_seams::set_multixact_id_limit::call(
            check_point.oldestMulti,
            check_point.oldestMultiDB,
            true,
        );
    }
    if commit_ts_seams::set_commit_ts_limit::is_installed() {
        commit_ts_seams::set_commit_ts_limit::call(
            check_point.oldestCommitTsXid,
            check_point.newestCommitTsXid,
        )?;
    }
    ctl.info_lck.with(|| ctl.ckptFullXid.store(check_point.nextXid.value, Relaxed));

    relcache_seams::relation_cache_init_file_remove::call();

    require_empty_or_seam(
        "pg_replslot",
        slot_seams::startup_replication_slots::is_installed(),
        "StartupReplicationSlots (replication/slot.c)",
    );
    if slot_seams::startup_replication_slots::is_installed() {
        slot_seams::startup_replication_slots::call()?;
    }
    if reorderbuffer_seams::startup_reorder_buffer::is_installed() {
        reorderbuffer_seams::startup_reorder_buffer::call()?;
    }
    if origin_seams::startup_replication_origin::is_installed() {
        origin_seams::startup_replication_origin::call()?;
    }

    clog::StartupCLOG()?;
    if multixact_seams::startup_multixact::is_installed() {
        multixact_seams::startup_multixact::call()?;
    }
    if control_file().track_commit_timestamp {
        commit_ts_seams::startup_commit_ts::call()?;
    }

    if state == DB_SHUTDOWNED {
        ctl.unloggedLSN.store(control_file().unloggedLSN, SeqCst);
    } else {
        ctl.unloggedLSN.store(control_file::FirstNormalUnloggedLSN, SeqCst);
    }

    if timeline_seams::restore_timeline_history_files::is_installed() {
        timeline_seams::restore_timeline_history_files::call(
            check_point.ThisTimeLineID,
            xlogrecovery_seams::recovery_target_tli::call(),
        )?;
    }

    require_empty_or_seam(
        "pg_twophase",
        twophase_seams::restore_two_phase_data::is_installed(),
        "restoreTwoPhaseData (twophase.c)",
    );
    if twophase_seams::restore_two_phase_data::is_installed() {
        twophase_seams::restore_two_phase_data::call()?;
    }

    if did_crash {
        if pgstat_seams::pgstat_discard_stats::is_installed() {
            pgstat_seams::pgstat_discard_stats::call()?;
        }
    } else if pgstat_seams::pgstat_restore_stats::is_installed() {
        pgstat_seams::pgstat_restore_stats::call()?;
    }

    LAST_FULL_PAGE_WRITES.set(check_point.fullPageWrites);
    crate::insert::set_local_redo_rec_ptr(check_point.redo);
    ctl.RedoRecPtr.store(check_point.redo, Relaxed);
    ctl.Insert.RedoRecPtr.store(check_point.redo, Relaxed);
    crate::insert::set_do_page_writes(check_point.fullPageWrites);

    let performed_wal_recovery = if xlogutils::in_recovery() {
        ctl.info_lck.with(|| {
            let s = if xlogrecovery_seams::in_archive_recovery::call() {
                RECOVERY_STATE_ARCHIVE
            } else {
                RECOVERY_STATE_CRASH
            };
            ctl.SharedRecoveryState.store(s, Relaxed);
        });
        UpdateControlFile()?;

        if init.have_backup_label {
            let _ = std::fs::remove_file(data_path("backup_label.old"));
            let _ = fd::durable_rename(&data_path("backup_label"), &data_path("backup_label.old"), FATAL)?;
        }
        if init.have_tblspc_map {
            let _ = std::fs::remove_file(data_path("tablespace_map.old"));
            let _ = fd::durable_rename(
                &data_path("tablespace_map"),
                &data_path("tablespace_map.old"),
                FATAL,
            )?;
        }

        if xlogrecovery_seams::in_archive_recovery::call() {
            crate::write::LOCAL_MIN_RECOVERY_POINT.set(control_file().minRecoveryPoint);
            crate::write::LOCAL_MIN_RECOVERY_POINT_TLI.set(control_file().minRecoveryPointTLI);
        } else {
            crate::write::LOCAL_MIN_RECOVERY_POINT.set(InvalidXLogRecPtr);
            crate::write::LOCAL_MIN_RECOVERY_POINT_TLI.set(0);
        }

        control_file::CheckRequiredParameterValues()?;

        // Unlogged relations may be trashed: drop their non-init forks
        // before anything can read them (reinit.c).
        fd::reinit::ResetUnloggedRelations(fd::reinit::UNLOGGED_RELATION_CLEANUP)?;
        // reinit rewrites forks at the file level, outside md: its size
        // cache must forget everything it thought it knew.
        smgr_seams::smgr_nblocks_cache_clear::call();

        snapmgr_seams::delete_all_exported_snapshot_files::call();

        if xlogrecovery_seams::archive_recovery_requested::call()
            && guc_tables::vars::EnableHotStandby.read()
        {
            let _ = elog(DEBUG1, "initializing for hot standby");

            standby_seams::init_recovery_transaction_environment::call()?;

            // C: oldestActiveXID = PrescanPreparedTransactions(&xids, &nxids)
            // when starting from a shutdown checkpoint.
            let (oldest_active_xid, prepared_xids) = if was_shutdown {
                if twophase_seams::prescan_prepared_transactions_xids::is_installed() {
                    twophase_seams::prescan_prepared_transactions_xids::call()?
                } else {
                    (
                        types_core::FullTransactionId {
                            value: procarray::TransamVariables().nextXid.load(Relaxed),
                        }
                        .xid(),
                        Vec::new(),
                    )
                }
            } else {
                (check_point.oldestActiveXid, Vec::new())
            };
            debug_assert!(oldest_active_xid != types_core::InvalidTransactionId);

            procarray_seams::proc_array_init_recovery::call(
                types_core::FullTransactionId {
                    value: procarray::TransamVariables().nextXid.load(Relaxed),
                }
                .xid(),
            );

            subtrans_seams::startup_subtrans::call(oldest_active_xid)?;

            if was_shutdown {
                // C: fake running-xacts record for a shut-down server — only
                // prepared transactions still alive; update pg_subtrans for
                // them first (StandbyRecoverPreparedTransactions).
                if twophase_seams::standby_recover_prepared_transactions::is_installed() {
                    twophase_seams::standby_recover_prepared_transactions::call()?;
                }
                let cx = mcx::MemoryContext::new("StartupXLOG/running-xacts");
                let mut latest_completed_xid = check_point.nextXid.xid();
                loop {
                    latest_completed_xid = latest_completed_xid.wrapping_sub(1);
                    if latest_completed_xid >= types_core::FirstNormalTransactionId {
                        break;
                    }
                }
                let running = types_storage::storage::RunningTransactionsData {
                    xcnt: prepared_xids.len() as i32,
                    subxcnt: 0,
                    subxid_status: types_storage::storage::SUBXIDS_IN_SUBTRANS,
                    nextXid: check_point.nextXid.xid(),
                    oldestRunningXid: oldest_active_xid,
                    oldestDatabaseRunningXid: types_core::InvalidTransactionId,
                    latestCompletedXid: latest_completed_xid,
                    xids: mcx::slice_in(cx.mcx(), &prepared_xids)?,
                };
                procarray_seams::proc_array_apply_recovery_info::call(&running)?;
            }
        }

        xlogrecovery_seams::perform_wal_recovery::call()?;
        true
    } else {
        false
    };

    let end_of_recovery_info = xlogrecovery_seams::finish_wal_recovery::call()?;
    let mut end_of_log = end_of_recovery_info.endOfLog;
    let end_of_log_tli = end_of_recovery_info.endOfLogTLI;

    if xlogutils::in_recovery()
        && (end_of_log < crate::write::LOCAL_MIN_RECOVERY_POINT.get()
            || !XLogRecPtrIsInvalid(control_file().backupStartPoint))
    {
        if xlogrecovery_seams::archive_recovery_requested::call() || control_file().backupEndRequired {
            return ereport(FATAL)
                .errmsg("WAL ends before end of online backup or consistent recovery point")
                .finish(loc("StartupXLOG"));
        }
    }

    // Reset unlogged relations to their INIT fork contents: after recovery
    // (to include ones created during it), before it is marked complete.
    if xlogutils::in_recovery() {
        fd::reinit::ResetUnloggedRelations(fd::reinit::UNLOGGED_RELATION_INIT)?;
        // File-level fork copies happened outside md: drop its size cache.
        smgr_seams::smgr_nblocks_cache_clear::call();
    }

    let oldest_active_xid: TransactionId =
        if twophase_seams::prescan_prepared_transactions::is_installed() {
            twophase_seams::prescan_prepared_transactions::call()?
        } else {
            types_core::FullTransactionId {
                value: procarray::TransamVariables().nextXid.load(Relaxed),
            }
            .xid()
        };

    SetInstallXLogFileSegmentActive()?;

    let mut new_tli = end_of_recovery_info.lastRecTLI;
    if xlogrecovery_seams::archive_recovery_requested::call() {
        let recovery_target_tli = xlogrecovery_seams::recovery_target_tli::call();
        new_tli = timeline_seams::find_newest_timeline::call(recovery_target_tli)? + 1;
        let _ = elog(LOG, format!("selected new timeline ID: {new_tli}"));

        XLogInitNewTimeline(end_of_log_tli, end_of_log, new_tli)?;

        if end_of_recovery_info.standby_signal_file_found {
            fd::durable_unlink(STANDBY_SIGNAL_FILE, FATAL)?;
        }
        if end_of_recovery_info.recovery_signal_file_found {
            fd::durable_unlink(RECOVERY_SIGNAL_FILE, FATAL)?;
        }

        timeline_seams::write_timeline_history::call(
            new_tli,
            recovery_target_tli,
            end_of_log,
            &end_of_recovery_info.recoveryStopReason,
        )?;

        let _ = elog(LOG, "archive recovery complete");
    }

    ctl.info_lck.with(|| {
        ctl.InsertTimeLineID.store(new_tli, Relaxed);
        ctl.PrevTimeLineID.store(end_of_recovery_info.lastRecTLI, Relaxed);
    });

    if !XLogRecPtrIsInvalid(end_of_recovery_info.missingContrecPtr) {
        debug_assert!(!XLogRecPtrIsInvalid(end_of_recovery_info.abortedRecPtr));
        end_of_log = end_of_recovery_info.missingContrecPtr;
    }

    let insert = &ctl.Insert;
    insert.PrevBytePos.store(XLogRecPtrToBytePos(end_of_recovery_info.lastRec), Relaxed);
    insert.CurrBytePos.store(XLogRecPtrToBytePos(end_of_log), Relaxed);

    if end_of_log % XLOG_BLCKSZ as u64 != 0 {
        let first_idx = XLogRecPtrToBufIdx(end_of_log) as usize;
        let len = (end_of_log - end_of_recovery_info.lastPageBeginPtr) as usize;
        debug_assert!(len < XLOG_BLCKSZ);
        debug_assert!(end_of_recovery_info.lastPage.len() >= len);
        let page = ctl.page_ptr(first_idx);
        // SAFETY: single-threaded startup; page buffer owned exclusively.
        unsafe {
            std::ptr::copy_nonoverlapping(end_of_recovery_info.lastPage.as_ptr(), page, len);
            std::ptr::write_bytes(page.add(len), 0, XLOG_BLCKSZ - len);
        }
        ctl.xlblocks[first_idx]
            .store(end_of_recovery_info.lastPageBeginPtr + XLOG_BLCKSZ as u64, Release);
        ctl.InitializedUpTo
            .store(end_of_recovery_info.lastPageBeginPtr + XLOG_BLCKSZ as u64, Relaxed);
    } else {
        ctl.InitializedUpTo.store(end_of_log, Relaxed);
    }

    crate::write::set_logwrt_result(end_of_log, end_of_log);
    ctl.logInsertResult.store(end_of_log, Relaxed);
    ctl.logWriteResult.store(end_of_log, Relaxed);
    ctl.logFlushResult.store(end_of_log, Relaxed);
    ctl.LogwrtRqstWrite.store(end_of_log, Relaxed);
    ctl.LogwrtRqstFlush.store(end_of_log, Relaxed);

    PreallocXlogFiles(end_of_log, new_tli)?;

    xlogutils::set_in_recovery(false);

    ctl.lastSegSwitchTime.store(crate::now_pg_time(), Relaxed);
    ctl.lastSegSwitchLSN.store(end_of_log, Relaxed);

    // latestCompletedXid = nextXid - 1 (under ProcArrayLock in C; startup is
    // still single-threaded here). FullTransactionIdRetreat: values still in
    // the first epoch below FirstNormalTransactionId stay put (no wrap).
    {
        let mut latest = procarray::TransamVariables().nextXid.load(Relaxed);
        latest -= 1;
        if latest >= types_core::FirstNormalTransactionId as u64 {
            while (latest as u32) < types_core::FirstNormalTransactionId {
                latest -= 1;
            }
        }
        procarray::TransamVariables().latestCompletedXid.store(latest, Relaxed);
    }

    // Hot standby already ran StartupSUBTRANS in the redo phase.
    if xlogutils::standby_state() == xlogutils::STANDBY_DISABLED
        && subtrans_seams::startup_subtrans::is_installed()
    {
        subtrans_seams::startup_subtrans::call(oldest_active_xid)?;
    }
    clog::TrimCLOG()?;
    if multixact_seams::trim_multixact::is_installed() {
        multixact_seams::trim_multixact::call()?;
    }

    if twophase_seams::recover_prepared_transactions::is_installed() {
        twophase_seams::recover_prepared_transactions::call()?;
    }

    xlogrecovery_seams::shutdown_wal_recovery::call()?;

    LocalSetXLogInsertAllowed();

    if !XLogRecPtrIsInvalid(end_of_recovery_info.abortedRecPtr) {
        debug_assert!(!XLogRecPtrIsInvalid(end_of_recovery_info.missingContrecPtr));
        CreateOverwriteContrecordRecord(
            end_of_recovery_info.abortedRecPtr,
            end_of_recovery_info.missingContrecPtr,
            new_tli,
        )?;
    }

    insert.fullPageWrites.store(LAST_FULL_PAGE_WRITES.get(), Relaxed);
    UpdateFullPageWrites()?;

    let promoted = if performed_wal_recovery {
        PerformRecoveryXLogAction()?
    } else {
        false
    };

    XLogReportParameters()?;

    if xlogrecovery_seams::archive_recovery_requested::call() {
        CleanupAfterArchiveRecovery(end_of_log_tli, end_of_log, new_tli)?;
    }

    if commit_ts_seams::complete_commit_ts_initialization::is_installed() {
        commit_ts_seams::complete_commit_ts_initialization::call()?;
    }

    LWLockAcquire(ControlFileLock(), LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
    control_file_update(|cf| cf.state = DB_IN_PRODUCTION);
    ctl.info_lck.with(|| ctl.SharedRecoveryState.store(RECOVERY_STATE_DONE, Relaxed));
    UpdateControlFile()?;
    LWLockRelease(ControlFileLock())?;

    // Must follow RecoverPreparedTransactions and the RECOVERY_STATE_DONE
    // flip so snapshots stop relying on KnownAssignedXids first.
    if xlogutils::standby_state() != xlogutils::STANDBY_DISABLED {
        standby_seams::shutdown_recovery_transaction_environment::call()?;
    }

    if walsender_seams::wal_snd_wakeup::is_installed() {
        walsender_seams::wal_snd_wakeup::call(true, true);
    }

    if promoted {
        checkpointer_seams::request_checkpoint::call(CHECKPOINT_FORCE)?;
    }

    Ok(())
}

// xlog.c CreateOverwriteContrecordRecord: after recovery ended at a broken
// continuation record, mark the page and write XLOG_OVERWRITE_CONTRECORD as
// the first record at page_ptr so downstream readers skip the torn record.
fn CreateOverwriteContrecordRecord(
    aborted_lsn: XLogRecPtr,
    page_ptr: XLogRecPtr,
    new_tli: TimeLineID,
) -> PgResult<XLogRecPtr> {
    if !RecoveryInProgress() {
        ereport(ERROR)
            .errmsg("can only be used at end of recovery")
            .finish(loc("CreateOverwriteContrecordRecord"))?;
    }
    if page_ptr % XLOG_BLCKSZ as u64 != 0 {
        ereport(ERROR)
            .errmsg(format!(
                "invalid position for missing continuation record {:X}/{:X}",
                page_ptr >> 32,
                page_ptr as u32
            ))
            .finish(loc("CreateOverwriteContrecordRecord"))?;
    }

    // The current WAL insert position should be right after the page header.
    let mut start_pos = page_ptr;
    if XLogSegmentOffset(start_pos, wal_segment_size()) == 0 {
        start_pos += SizeOfXLogLongPHD as u64;
    } else {
        start_pos += SizeOfXLogShortPHD as u64;
    }
    let recptr = crate::insert::GetXLogInsertRecPtr();
    if recptr != start_pos {
        ereport(ERROR)
            .errmsg(format!(
                "invalid WAL insert position {:X}/{:X} for OVERWRITE_CONTRECORD",
                recptr >> 32,
                recptr as u32
            ))
            .finish(loc("CreateOverwriteContrecordRecord"))?;
    }

    init_small::globals::StartCriticalSection();

    // Initialize the XLOG page header (by GetXLogBuffer) and set the
    // XLP_FIRST_IS_OVERWRITE_CONTRECORD flag; startup is the only WAL writer
    // here, the insertion lock is pro forma (C keeps it too).
    crate::insert::WALInsertLockAcquire();
    let pagehdr = crate::insert::GetXLogBuffer(page_ptr, new_tli);
    // SAFETY: pagehdr is the page-aligned start of an XLOG buffer page;
    // xlp_info is the u16 at offset 2 of XLogPageHeaderData.
    unsafe {
        let info = pagehdr.add(2).cast::<u16>();
        info.write(info.read() | XLP_FIRST_IS_OVERWRITE_CONTRECORD);
    }
    WALInsertLockRelease();

    // xl_overwrite_contrecord: overwritten_lsn 0..8, overwrite_time 8..16.
    let mut body = [0u8; 16];
    body[0..8].copy_from_slice(&aborted_lsn.to_ne_bytes());
    body[8..16]
        .copy_from_slice(&timestamp_seams::get_current_timestamp::call().to_ne_bytes());
    let recptr = xloginsert_seams::xlog_insert::call(RM_XLOG_ID, XLOG_OVERWRITE_CONTRECORD, &[&body])?;

    if ProcLastRecPtr() != start_pos {
        let last = ProcLastRecPtr();
        ereport(ERROR)
            .errmsg(format!(
                "OVERWRITE_CONTRECORD was inserted to unexpected position {:X}/{:X}",
                last >> 32,
                last as u32
            ))
            .finish(loc("CreateOverwriteContrecordRecord"))?;
    }

    XLogFlush(recptr)?;

    init_small::globals::EndCriticalSection();

    Ok(recptr)
}

fn PerformRecoveryXLogAction() -> PgResult<bool> {
    let promoted = xlogrecovery_seams::archive_recovery_requested::call()
        && init_small::globals::IsUnderPostmaster()
        && xlogrecovery_seams::promote_is_triggered::call();
    if promoted {
        CreateEndOfRecoveryRecord()?;
    } else {
        let flags = CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_IMMEDIATE | CHECKPOINT_WAIT;
        if checkpointer_seams::request_checkpoint::is_installed() {
            checkpointer_seams::request_checkpoint::call(flags)?;
        } else {
            // C standalone-backend semantics: RequestCheckpoint runs the
            // checkpoint in-process when there is no checkpointer.
            CreateCheckPoint(flags)?;
        }
    }
    Ok(promoted)
}

fn CreateEndOfRecoveryRecord() -> PgResult<()> {
    if !RecoveryInProgress() {
        return ereport(ERROR)
            .errmsg("can only be used to end recovery")
            .finish(loc("CreateEndOfRecoveryRecord"));
    }

    let end_time = timestamp_seams::get_current_timestamp::call();
    let wal_level_now = wal_level();

    let ctl = XLogCtl();
    WALInsertLockAcquireExclusive();
    let this_tli = ctl.InsertTimeLineID.load(Relaxed);
    let prev_tli = ctl.PrevTimeLineID.load(Relaxed);
    WALInsertLockRelease();

    init_small::globals::StartCriticalSection();

    // xl_end_of_recovery (xlog_internal.h): end_time 0..8, ThisTimeLineID
    // 8..12, PrevTimeLineID 12..16, wal_level 16..20; sizeof == 24 (8-byte
    // struct align pads the tail).
    let mut body = [0u8; 24];
    body[0..8].copy_from_slice(&end_time.to_ne_bytes());
    body[8..12].copy_from_slice(&this_tli.to_ne_bytes());
    body[12..16].copy_from_slice(&prev_tli.to_ne_bytes());
    body[16..20].copy_from_slice(&wal_level_now.to_ne_bytes());
    let recptr = xloginsert_seams::xlog_insert::call(RM_XLOG_ID, XLOG_END_OF_RECOVERY, &[&body])?;

    XLogFlush(recptr)?;

    LWLockAcquire(ControlFileLock(), LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
    control_file_update(|cf| {
        cf.minRecoveryPoint = recptr;
        cf.minRecoveryPointTLI = this_tli;
    });
    UpdateControlFile()?;
    LWLockRelease(ControlFileLock())?;

    init_small::globals::EndCriticalSection();
    Ok(())
}

fn XLogInitNewTimeline(
    end_tli: TimeLineID,
    end_of_log: XLogRecPtr,
    new_tli: TimeLineID,
) -> PgResult<()> {
    debug_assert!(end_tli != new_tli);

    crate::write::UpdateMinRecoveryPoint(InvalidXLogRecPtr, true)?;

    let wal_segsz = wal_segment_size();
    let end_log_seg_no = XLByteToPrevSeg(end_of_log, wal_segsz);
    let start_log_seg_no = XLByteToSeg(end_of_log, wal_segsz);

    if end_log_seg_no == start_log_seg_no {
        crate::write::XLogFileCopy(
            new_tli,
            end_log_seg_no,
            end_tli,
            end_log_seg_no,
            XLogSegmentOffset(end_of_log, wal_segsz) as i32,
        )?;
    } else {
        let f = crate::write::XLogFileInit(start_log_seg_no, new_tli)?;
        // SAFETY: f is the open fd returned by XLogFileInit.
        if unsafe { libc::close(f) } != 0 {
            let fname = XLogFileName(new_tli, start_log_seg_no, wal_segsz);
            let e = std::io::Error::last_os_error();
            return ereport(ERROR)
                .errmsg(format!("could not close file \"{fname}\": {e}"))
                .finish(loc("XLogInitNewTimeline"));
        }
    }

    let fname = XLogFileName(new_tli, start_log_seg_no, wal_segsz);
    crate::removal::xlog_archive_cleanup(&fname);
    Ok(())
}

fn CleanupAfterArchiveRecovery(
    end_of_log_tli: TimeLineID,
    end_of_log: XLogRecPtr,
    new_tli: TimeLineID,
) -> PgResult<()> {
    if guc_tables::vars::recoveryEndCommand.installed() {
        if let Some(cmd) = guc_tables::vars::recoveryEndCommand.read() {
            if !cmd.is_empty() {
                xlogarchive_seams::execute_recovery_command::call(
                    &cmd,
                    "recovery_end_command",
                    true,
                    WAIT_EVENT_RECOVERY_END_COMMAND,
                )?;
            }
        }
    }

    crate::removal::RemoveNonParentXlogFiles(end_of_log, new_tli)?;

    // Mid-segment switch: archive the old timeline's last segment as *.partial.
    let wal_segsz = wal_segment_size();
    if XLogSegmentOffset(end_of_log, wal_segsz) != 0 && XLogArchivingActive() {
        let end_log_seg_no = XLByteToPrevSeg(end_of_log, wal_segsz);
        let origfname = XLogFileName(end_of_log_tli, end_log_seg_no, wal_segsz);

        if !xlogarchive_seams::xlog_archive_is_ready_or_done::call(&origfname) {
            if guc_tables::vars::summarize_wal.installed()
                && guc_tables::vars::summarize_wal.read()
            {
                walsummarizer_seams::wait_for_wal_summarization::call(end_of_log)?;
            }

            let origpath = XLogFilePath(end_of_log_tli, end_log_seg_no, wal_segsz);
            let partialfname = format!("{origfname}.partial");
            let partialpath = format!("{origpath}.partial");

            crate::removal::xlog_archive_cleanup(&partialfname);

            fd::durable_rename(&origpath, &partialpath, ERROR)?;
            xlogarchive_seams::xlog_archive_notify::call(&partialfname)?;
        }
    }
    Ok(())
}

pub fn SetInstallXLogFileSegmentActive() -> PgResult<()> {
    LWLockAcquire(ControlFileLock(), LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
    XLogCtl().InstallXLogFileSegmentActive.store(true, Relaxed);
    LWLockRelease(ControlFileLock())?;
    Ok(())
}

pub fn ResetInstallXLogFileSegmentActive() {
    XLogCtl().InstallXLogFileSegmentActive.store(false, Relaxed);
}

pub fn XLogReportParameters() -> PgResult<()> {
    use init_small::globals;
    let cf = control_file();
    let wal_level_now = wal_level();
    let wal_log_hints = guc_tables::vars::wal_log_hints.read();
    let max_conns = globals::MaxConnections();
    let max_workers = globals::max_worker_processes();
    let max_senders = guc_tables::vars::max_wal_senders.read();
    let max_prepared = guc_tables::vars::max_prepared_xacts.read();
    let max_locks = guc_tables::vars::max_locks_per_xact.read();
    // Var home is commit_ts.c; installed() survives seam-free unit tests.
    let track_cts = guc_tables::vars::track_commit_timestamp.installed()
        && guc_tables::vars::track_commit_timestamp.read();

    if wal_level_now != cf.wal_level
        || wal_log_hints != cf.wal_log_hints
        || max_conns != cf.MaxConnections
        || max_workers != cf.max_worker_processes
        || max_senders != cf.max_wal_senders
        || max_prepared != cf.max_prepared_xacts
        || max_locks != cf.max_locks_per_xact
        || track_cts != cf.track_commit_timestamp
    {
        if wal_level_now != cf.wal_level || XLogIsNeeded() {
            // xl_parameter_change (pg_control.h): six ints + two bools.
            let mut body = [0u8; 28];
            body[0..4].copy_from_slice(&max_conns.to_ne_bytes());
            body[4..8].copy_from_slice(&max_workers.to_ne_bytes());
            body[8..12].copy_from_slice(&max_senders.to_ne_bytes());
            body[12..16].copy_from_slice(&max_prepared.to_ne_bytes());
            body[16..20].copy_from_slice(&max_locks.to_ne_bytes());
            body[20..24].copy_from_slice(&wal_level_now.to_ne_bytes());
            body[24] = wal_log_hints as u8;
            body[25] = track_cts as u8;
            let recptr =
                xloginsert_seams::xlog_insert::call(RM_XLOG_ID, XLOG_PARAMETER_CHANGE, &[&body])?;
            XLogFlush(recptr)?;
        }

        LWLockAcquire(ControlFileLock(), LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
        control_file_update(|cf| {
            cf.MaxConnections = max_conns;
            cf.max_worker_processes = max_workers;
            cf.max_wal_senders = max_senders;
            cf.max_prepared_xacts = max_prepared;
            cf.max_locks_per_xact = max_locks;
            cf.wal_level = wal_level_now;
            cf.wal_log_hints = wal_log_hints;
            cf.track_commit_timestamp = track_cts;
        });
        UpdateControlFile()?;
        LWLockRelease(ControlFileLock())?;
    }
    Ok(())
}

pub fn UpdateFullPageWrites() -> PgResult<()> {
    let insert = &XLogCtl().Insert;
    let full_page_writes = guc_tables::vars::fullPageWrites.read();

    if full_page_writes == insert.fullPageWrites.load(Relaxed) {
        return Ok(());
    }
    let recovery_in_progress = RecoveryInProgress();

    init_small::globals::StartCriticalSection();
    if full_page_writes {
        WALInsertLockAcquireExclusive();
        insert.fullPageWrites.store(true, Relaxed);
        WALInsertLockRelease();
    }

    if XLogStandbyInfoActive() && !recovery_in_progress {
        let body = [full_page_writes as u8];
        xloginsert_seams::xlog_insert::call(RM_XLOG_ID, XLOG_FPW_CHANGE, &[&body])?;
    }

    if !full_page_writes {
        WALInsertLockAcquireExclusive();
        insert.fullPageWrites.store(false, Relaxed);
        WALInsertLockRelease();
    }
    init_small::globals::EndCriticalSection();
    Ok(())
}

fn CheckPointGuts(check_point_redo: XLogRecPtr, flags: i32) -> PgResult<()> {
    relmapper::CheckPointRelationMap()?;
    require_empty_or_seam(
        "pg_replslot",
        slot_seams::check_point_replication_slots::is_installed(),
        "CheckPointReplicationSlots (slot.c)",
    );
    if slot_seams::check_point_replication_slots::is_installed() {
        slot_seams::check_point_replication_slots::call(flags & CHECKPOINT_IS_SHUTDOWN != 0)?;
    }
    if snapbuild_seams::check_point_snap_build::is_installed() {
        snapbuild_seams::check_point_snap_build::call()?;
    }
    if rewriteheap_seams::check_point_logical_rewrite_heap::is_installed() {
        rewriteheap_seams::check_point_logical_rewrite_heap::call()?;
    }
    if origin_seams::check_point_replication_origin::is_installed() {
        origin_seams::check_point_replication_origin::call()?;
    }

    // xlog.c CheckPointGuts: "Write out all dirty data in SLRUs and the main
    // buffer pool" — ckpt_write_t opens the write phase here.
    CKPT_WRITE_T.set(timestamp_seams::get_current_timestamp::call());
    clog::CheckPointCLOG()?;
    if commit_ts_seams::check_point_commit_ts::is_installed() {
        commit_ts_seams::check_point_commit_ts::call()?;
    }
    if subtrans_seams::check_point_subtrans::is_installed() {
        subtrans_seams::check_point_subtrans::call()?;
    }
    if multixact_seams::check_point_multixact::is_installed() {
        multixact_seams::check_point_multixact::call()?;
    }
    if predicate_seams::check_point_predicate::is_installed() {
        predicate_seams::check_point_predicate::call()?;
    }
    bufmgr_seams::check_point_buffers::call(flags)?;

    // ckpt_sync_t / ckpt_sync_end_t bracket the fsync phase (xlog.c).
    CKPT_SYNC_T.set(timestamp_seams::get_current_timestamp::call());

    // Unconditional (loud panic if uninstalled): a checkpoint that skips
    // ProcessSyncRequests fsyncs nothing yet still updates pg_control.
    sync_seams::process_sync_requests::call()?;

    CKPT_SYNC_END_T.set(timestamp_seams::get_current_timestamp::call());

    if twophase_seams::check_point_two_phase::is_installed() {
        twophase_seams::check_point_two_phase::call(check_point_redo)?;
    }
    Ok(())
}

fn wait_for_delay_chkpt(delay_type: i32) -> PgResult<()> {
    // A checkpoint that skips the delay-chkpt wait can capture a torn
    // multi-record update; uninstalled probe must be loud, never a skip.
    while procarray_seams::have_virtual_xids_delaying_chkpt::call(delay_type) {
        if sync_seams::absorb_sync_requests::is_installed() {
            sync_seams::absorb_sync_requests::call()?;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    Ok(())
}

pub const DELAY_CHKPT_START: i32 = 1 << 0;
pub const DELAY_CHKPT_COMPLETE: i32 = 1 << 1;

// LogCheckpointStart's flag words (xlog.c:6687): the recovery TAP suite
// greps these exact strings (041 matches "restartpoint starting: immediate
// wait"), so the hex-flags shorthand this replaced was a conformance break.
fn checkpoint_flag_words(flags: i32) -> String {
    let mut s = String::new();
    for (bit, word) in [
        (CHECKPOINT_IS_SHUTDOWN, " shutdown"),
        (CHECKPOINT_END_OF_RECOVERY, " end-of-recovery"),
        (CHECKPOINT_IMMEDIATE, " immediate"),
        (CHECKPOINT_FORCE, " force"),
        (CHECKPOINT_WAIT, " wait"),
        (CHECKPOINT_CAUSE_XLOG, " wal"),
        (CHECKPOINT_CAUSE_TIME, " time"),
        (CHECKPOINT_FLUSH_ALL, " flush-all"),
    ] {
        if flags & bit != 0 {
            s.push_str(word);
        }
    }
    s
}

pub fn CreateCheckPoint(flags: i32) -> PgResult<bool> {
    let ctl = XLogCtl();
    let insert = &ctl.Insert;
    let shutdown = flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY) != 0;

    if RecoveryInProgress() && flags & CHECKPOINT_END_OF_RECOVERY == 0 {
        return Err(Box::new(PgError::new(ERROR, "can't create a checkpoint during recovery")));
    }

    CKPT_SEGS_ADDED.set(0);
    CKPT_SEGS_REMOVED.set(0);
    CKPT_SEGS_RECYCLED.set(0);
    CKPT_SLRU_WRITTEN.set(0);

    sync_seams::sync_pre_checkpoint::call()?;

    init_small::globals::StartCriticalSection();

    if shutdown {
        LWLockAcquire(ControlFileLock(), LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
        control_file_update(|cf| cf.state = DB_SHUTDOWNING);
        UpdateControlFile()?;
        LWLockRelease(ControlFileLock())?;
    }

    let mut check_point = CheckPoint::ZEROED;
    check_point.time = crate::now_pg_time();

    if !shutdown && XLogStandbyInfoActive() {
        check_point.oldestActiveXid =
            procarray_seams::get_oldest_active_transaction_id::call();
    } else {
        check_point.oldestActiveXid = types_core::InvalidTransactionId;
    }

    let last_important_lsn = crate::insert::GetLastImportantRecPtr();

    if flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_FORCE) == 0
        && last_important_lsn == control_file().checkPoint
    {
        init_small::globals::EndCriticalSection();
        return Ok(false);
    }

    let old_xlog_allowed = if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
        LocalSetXLogInsertAllowed()
    } else {
        0
    };

    check_point.ThisTimeLineID = ctl.InsertTimeLineID.load(Relaxed);
    check_point.PrevTimeLineID = if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
        ctl.PrevTimeLineID.load(Relaxed)
    } else {
        check_point.ThisTimeLineID
    };

    WALInsertLockAcquireExclusive();
    check_point.fullPageWrites = insert.fullPageWrites.load(Relaxed);
    check_point.wal_level = wal_level();

    if shutdown {
        let mut cur_insert = XLogBytePosToRecPtr(insert.CurrBytePos.load(Relaxed));
        if INSERT_FREESPACE(cur_insert) == 0 {
            if XLogSegmentOffset(cur_insert, wal_segment_size()) == 0 {
                cur_insert += SizeOfXLogLongPHD as u64;
            } else {
                cur_insert += SizeOfXLogShortPHD as u64;
            }
        }
        check_point.redo = cur_insert;
        crate::insert::set_local_redo_rec_ptr(check_point.redo);
        insert.RedoRecPtr.store(check_point.redo, Relaxed);
    }
    WALInsertLockRelease();

    if !shutdown {
        let body = check_point.wal_level.to_ne_bytes();
        xloginsert_seams::xlog_insert::call(RM_XLOG_ID, XLOG_CHECKPOINT_REDO, &[&body])?;
        check_point.redo = crate::insert::local_redo_rec_ptr();
    }

    ctl.info_lck.with(|| ctl.RedoRecPtr.store(check_point.redo, Relaxed));

    if guc_tables::vars::log_checkpoints.read() {
        let _ = elog(LOG, format!("checkpoint starting:{}", checkpoint_flag_words(flags)));
    }

    {
        let tv = procarray::TransamVariables();
        let xid_gen_lock = lwlock::main_lock(varsup::XID_GEN_LOCK);
        LWLockAcquire(xid_gen_lock, lwlock::LW_SHARED, init_small::globals::MyProcNumber())?;
        check_point.nextXid = types_core::FullTransactionId { value: tv.nextXid.load(Relaxed) };
        check_point.oldestXid = tv.oldestXid.load(Relaxed);
        check_point.oldestXidDB = tv.oldestXidDB.load(Relaxed);
        LWLockRelease(xid_gen_lock)?;

        // xlog.c CreateCheckPoint reads these under CommitTsLock, LW_SHARED.
        let commit_ts_lock =
            lwlock::main_lock(types_storage::storage::COMMIT_TS_LOCK);
        LWLockAcquire(commit_ts_lock, lwlock::LW_SHARED, init_small::globals::MyProcNumber())?;
        check_point.oldestCommitTsXid = tv.oldestCommitTsXid.load(Relaxed);
        check_point.newestCommitTsXid = tv.newestCommitTsXid.load(Relaxed);
        LWLockRelease(commit_ts_lock)?;

        // An online checkpoint's nextOid covers the prefetched-but-unlogged
        // range so crash replay never hands out an OID below what the
        // pre-crash backend may already have used (xlog.c CreateCheckPoint).
        let oid_gen_lock = lwlock::main_lock(varsup::OID_GEN_LOCK);
        LWLockAcquire(oid_gen_lock, lwlock::LW_SHARED, init_small::globals::MyProcNumber())?;
        check_point.nextOid = tv.nextOid.load(Relaxed);
        if !shutdown {
            check_point.nextOid = check_point.nextOid.wrapping_add(tv.oidCount.load(Relaxed));
        }
        LWLockRelease(oid_gen_lock)?;
    }
    if multixact_seams::multixact_get_checkpt_multi::is_installed() {
        let (next_multi, next_offset, oldest_multi, oldest_db) =
            multixact_seams::multixact_get_checkpt_multi::call(shutdown);
        check_point.nextMulti = next_multi;
        check_point.nextMultiOffset = next_offset;
        check_point.oldestMulti = oldest_multi;
        check_point.oldestMultiDB = oldest_db;
    } else {
        let prev = control_file().checkPointCopy;
        check_point.nextMulti = prev.nextMulti;
        check_point.nextMultiOffset = prev.nextMultiOffset;
        check_point.oldestMulti = prev.oldestMulti;
        check_point.oldestMultiDB = prev.oldestMultiDB;
    }

    init_small::globals::EndCriticalSection();

    wait_for_delay_chkpt(DELAY_CHKPT_START)?;
    CheckPointGuts(check_point.redo, flags)?;
    wait_for_delay_chkpt(DELAY_CHKPT_COMPLETE)?;

    if !shutdown && XLogStandbyInfoActive() {
        standby_seams::log_standby_snapshot::call()?;
    }

    init_small::globals::StartCriticalSection();

    let recptr =
        xloginsert_seams::xlog_insert::call(
            RM_XLOG_ID,
            if shutdown { XLOG_CHECKPOINT_SHUTDOWN } else { XLOG_CHECKPOINT_ONLINE },
            &[&check_point.to_bytes()],
        )?;
    XLogFlush(recptr)?;

    if shutdown {
        if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
            crate::insert::set_local_xlog_insert_allowed(old_xlog_allowed);
        } else {
            crate::insert::set_local_xlog_insert_allowed(0);
        }
    }

    if shutdown && check_point.redo != ProcLastRecPtr() {
        return ereport(PANIC)
            .errmsg("concurrent write-ahead log activity while database system is shutting down")
            .finish(loc("CreateCheckPoint"))
            .map(|_| false);
    }

    // PriorRedoPtr (xlog.c): the previous checkpoint's redo, read before this
    // checkpoint overwrites checkPointCopy — feeds the recycling estimate.
    let prior_redo_ptr = control_file().checkPointCopy.redo;

    LWLockAcquire(ControlFileLock(), LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
    let unlogged = ctl.unloggedLSN.load(SeqCst);
    control_file_update(|cf| {
        if shutdown {
            cf.state = DB_SHUTDOWNED;
        }
        cf.checkPoint = PROC_LAST_REC_PTR.get();
        cf.checkPointCopy = check_point;
        cf.minRecoveryPoint = InvalidXLogRecPtr;
        cf.minRecoveryPointTLI = 0;
        cf.unloggedLSN = unlogged;
    });
    UpdateControlFile()?;
    LWLockRelease(ControlFileLock())?;

    ctl.info_lck.with(|| ctl.ckptFullXid.store(check_point.nextXid.value, Relaxed));

    init_small::globals::EndCriticalSection();

    walsummarizer_seams::wakeup_wal_summarizer::call();

    sync_seams::sync_post_checkpoint::call()?;

    if prior_redo_ptr != InvalidXLogRecPtr {
        crate::removal::UpdateCheckPointDistanceEstimate(check_point.redo - prior_redo_ptr);
    }

    injection_point::injection_point("checkpoint-before-old-wal-removal")?;

    let mut log_seg_no = XLByteToSeg(check_point.redo, wal_segment_size());
    let _ = crate::removal::KeepLogSeg(recptr, &mut log_seg_no)?;
    // xlog.c:7383: invalidate slots whose reserved WAL is about to go away
    // (max_slot_wal_keep_size) or that idled past the timeout; on any
    // invalidation, recompute the old-segment horizon from RedoRecPtr.
    // Uninstalled seam (substrate test binaries): no slot machinery exists,
    // so no slot can hold WAL — skipping is provably the C no-op.
    if slot_seams::invalidate_obsolete_replication_slots::is_installed()
        && slot_seams::invalidate_obsolete_replication_slots::call(
            (RS_INVAL_WAL_REMOVED | RS_INVAL_IDLE_TIMEOUT) as u32,
            log_seg_no,
            types_core::InvalidOid,
            types_core::InvalidTransactionId,
        )?
    {
        log_seg_no = XLByteToSeg(check_point.redo, wal_segment_size());
        let _ = crate::removal::KeepLogSeg(recptr, &mut log_seg_no)?;
    }
    log_seg_no -= 1;
    crate::removal::RemoveOldXlogFiles(
        log_seg_no,
        check_point.redo,
        recptr,
        check_point.ThisTimeLineID,
    )?;

    if !shutdown {
        PreallocXlogFiles(recptr, check_point.ThisTimeLineID)?;
    }

    // DEBT(savepoint lane): truncate_subtrans is installed by the subtrans
    // unit; until it lands, skipping only bloats pg_subtrans (no correctness
    // arm) — the one is_installed skip this crate tolerates.
    if !RecoveryInProgress() && subtrans_seams::truncate_subtrans::is_installed() {
        subtrans_seams::truncate_subtrans::call(
            procarray_seams::get_oldest_transaction_id_considered_running::call(),
        )?;
    }

    // LogCheckpointEnd (xlog.c): timing accumulation is unconditional; only
    // the log line below is gated on log_checkpoints.
    accumulate_checkpoint_timing();

    if guc_tables::vars::log_checkpoints.read() {
        let _ = elog(
            LOG,
            format!(
                "checkpoint complete: wrote {} SLRU buffers; {} WAL file(s) added, {} removed, {} recycled",
                CKPT_SLRU_WRITTEN.get(),
                CKPT_SEGS_ADDED.get(),
                CKPT_SEGS_REMOVED.get(),
                CKPT_SEGS_RECYCLED.get()
            ),
        );
    }

    Ok(true)
}

// Recovery-time checkpoint analog, anchored on the last replayed safe
// checkpoint stashed by RecoveryRestartPoint; true = new restartpoint made.
pub fn CreateRestartPoint(flags: i32) -> PgResult<bool> {
    let ctl = XLogCtl();

    let (last_ckpt_rec_ptr, last_ckpt_end_ptr, last_ckpt) = ctl.info_lck.with(|| {
        (
            ctl.lastCheckPointRecPtr.load(Relaxed),
            ctl.lastCheckPointEndPtr.load(Relaxed),
            // SAFETY: lastCheckPoint accessed only under info_lck.
            unsafe { *ctl.lastCheckPoint.get() },
        )
    });

    if !RecoveryInProgress() {
        let _ = elog(DEBUG2, "skipping restartpoint, recovery has already ended");
        return Ok(false);
    }

    // No newer safe checkpoint replayed: only push minRecoveryPoint.
    if XLogRecPtrIsInvalid(last_ckpt_rec_ptr)
        || last_ckpt.redo <= control_file().checkPointCopy.redo
    {
        let _ = elog(
            DEBUG2,
            format!(
                "skipping restartpoint, already performed at {:X}/{:X}",
                last_ckpt.redo >> 32,
                last_ckpt.redo as u32
            ),
        );
        crate::write::UpdateMinRecoveryPoint(InvalidXLogRecPtr, true)?;
        if flags & CHECKPOINT_IS_SHUTDOWN != 0 {
            LWLockAcquire(ControlFileLock(), LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
            control_file_update(|cf| cf.state = DB_SHUTDOWNED_IN_RECOVERY);
            UpdateControlFile()?;
            LWLockRelease(ControlFileLock())?;
        }
        return Ok(false);
    }

    WALInsertLockAcquireExclusive();
    crate::insert::set_local_redo_rec_ptr(last_ckpt.redo);
    ctl.Insert.RedoRecPtr.store(last_ckpt.redo, Relaxed);
    WALInsertLockRelease();
    ctl.info_lck.with(|| ctl.RedoRecPtr.store(last_ckpt.redo, Relaxed));

    CKPT_SEGS_ADDED.set(0);
    CKPT_SEGS_REMOVED.set(0);
    CKPT_SEGS_RECYCLED.set(0);
    CKPT_SLRU_WRITTEN.set(0);

    if guc_tables::vars::log_checkpoints.read() {
        let _ = elog(LOG, format!("restartpoint starting:{}", checkpoint_flag_words(flags)));
    }

    CheckPointGuts(last_ckpt.redo, flags)?;

    // xlog.c:7737: after CheckPointGuts so some restartpoint work has
    // already happened when a test parks the checkpointer here.
    injection_point::injection_point("create-restart-point")?;

    let prior_redo_ptr = control_file().checkPointCopy.redo;

    // Skip pg_control unless it still shows an older checkpoint (racing
    // end-of-recovery checkpoint).
    LWLockAcquire(ControlFileLock(), LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
    if control_file().checkPointCopy.redo < last_ckpt.redo {
        control_file_update(|cf| {
            cf.checkPoint = last_ckpt_rec_ptr;
            cf.checkPointCopy = last_ckpt;

            // A backup taken in recovery needs minRecoveryPoint past the
            // checkpoint record to pick its WAL files.
            if cf.state == DB_IN_ARCHIVE_RECOVERY {
                if cf.minRecoveryPoint < last_ckpt_end_ptr {
                    cf.minRecoveryPoint = last_ckpt_end_ptr;
                    cf.minRecoveryPointTLI = last_ckpt.ThisTimeLineID;
                    crate::write::LOCAL_MIN_RECOVERY_POINT.set(cf.minRecoveryPoint);
                    crate::write::LOCAL_MIN_RECOVERY_POINT_TLI.set(cf.minRecoveryPointTLI);
                }
                if flags & CHECKPOINT_IS_SHUTDOWN != 0 {
                    cf.state = DB_SHUTDOWNED_IN_RECOVERY;
                }
            }
        });
        UpdateControlFile()?;
    }
    LWLockRelease(ControlFileLock())?;

    let redo_rec_ptr = crate::insert::local_redo_rec_ptr();
    if prior_redo_ptr != InvalidXLogRecPtr {
        crate::removal::UpdateCheckPointDistanceEstimate(redo_rec_ptr - prior_redo_ptr);
    }

    let mut log_seg_no = XLByteToSeg(redo_rec_ptr, wal_segment_size());

    // C retreats _logSegNo by max(received, replayed).
    let receive_ptr = if walreceiverfuncs_seams::get_wal_rcv_flush_rec_ptr::is_installed() {
        walreceiverfuncs_seams::get_wal_rcv_flush_rec_ptr::call().0
    } else {
        InvalidXLogRecPtr
    };
    let (replay_ptr, mut replay_tli) = xlogrecovery_seams::get_xlog_replay_rec_ptr::call();
    let endptr = if receive_ptr < replay_ptr { replay_ptr } else { receive_ptr };
    let _ = crate::removal::KeepLogSeg(endptr, &mut log_seg_no)?;
    injection_point::injection_point("restartpoint-before-slot-invalidation")?;
    // xlog.c:7841 (CreateRestartPoint): same sweep + horizon recompute.
    if slot_seams::invalidate_obsolete_replication_slots::is_installed()
        && slot_seams::invalidate_obsolete_replication_slots::call(
            (RS_INVAL_WAL_REMOVED | RS_INVAL_IDLE_TIMEOUT) as u32,
            log_seg_no,
            types_core::InvalidOid,
            types_core::InvalidTransactionId,
        )?
    {
        log_seg_no = XLByteToSeg(redo_rec_ptr, wal_segment_size());
        let _ = crate::removal::KeepLogSeg(endptr, &mut log_seg_no)?;
    }
    log_seg_no -= 1;

    if !RecoveryInProgress() {
        replay_tli = ctl.InsertTimeLineID.load(Relaxed);
    }
    crate::removal::RemoveOldXlogFiles(log_seg_no, redo_rec_ptr, endptr, replay_tli)?;

    PreallocXlogFiles(endptr, replay_tli)?;

    // DEBT(savepoint lane): same TruncateSUBTRANS skip as CreateCheckPoint.
    if guc_tables::vars::EnableHotStandby.read()
        && subtrans_seams::truncate_subtrans::is_installed()
    {
        subtrans_seams::truncate_subtrans::call(
            procarray_seams::get_oldest_transaction_id_considered_running::call(),
        )?;
    }

    // LogCheckpointEnd(true) (xlog.c): timing accumulation is unconditional.
    accumulate_checkpoint_timing();

    if guc_tables::vars::log_checkpoints.read() {
        let _ = elog(
            LOG,
            format!(
                "restartpoint complete: wrote {} SLRU buffers; {} WAL file(s) added, {} removed, {} recycled",
                CKPT_SLRU_WRITTEN.get(),
                CKPT_SEGS_ADDED.get(),
                CKPT_SEGS_REMOVED.get(),
                CKPT_SEGS_RECYCLED.get()
            ),
        );
    }

    let level = if guc_tables::vars::log_checkpoints.read() { LOG } else { DEBUG2 };
    let mut report = ereport(level).errmsg(format!(
        "recovery restart point at {:X}/{:X}",
        last_ckpt.redo >> 32,
        last_ckpt.redo as u32
    ));
    if xlogrecovery_seams::get_latest_x_time::is_installed() {
        let xtime = xlogrecovery_seams::get_latest_x_time::call();
        if xtime != 0 {
            report = report.errdetail(format!(
                "Last completed transaction was at log time {}.",
                timestamp_seams::timestamptz_to_str::call(xtime)
            ));
        }
    }
    report.finish(loc("CreateRestartPoint"))?;

    if guc_tables::vars::archiveCleanupCommand.installed() {
        if let Some(cmd) = guc_tables::vars::archiveCleanupCommand.read() {
            if !cmd.is_empty() {
                xlogarchive_seams::execute_recovery_command::call(
                    &cmd,
                    "archive_cleanup_command",
                    false,
                    WAIT_EVENT_ARCHIVE_CLEANUP_COMMAND,
                )?;
            }
        }
    }

    Ok(true)
}

// PerformWalRecovery callback: crash recovery crossed into archive recovery.
pub fn SwitchIntoArchiveRecovery(end_rec_ptr: XLogRecPtr, replay_tli: TimeLineID) -> PgResult<()> {
    let ctl = XLogCtl();
    LWLockAcquire(ControlFileLock(), LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
    control_file_update(|cf| {
        cf.state = DB_IN_ARCHIVE_RECOVERY;
        if cf.minRecoveryPoint < end_rec_ptr {
            cf.minRecoveryPoint = end_rec_ptr;
            cf.minRecoveryPointTLI = replay_tli;
        }
    });
    let cf = control_file();
    crate::write::LOCAL_MIN_RECOVERY_POINT.set(cf.minRecoveryPoint);
    crate::write::LOCAL_MIN_RECOVERY_POINT_TLI.set(cf.minRecoveryPointTLI);
    crate::write::set_update_min_recovery_point(true);
    UpdateControlFile()?;
    ctl.info_lck
        .with(|| ctl.SharedRecoveryState.store(RECOVERY_STATE_ARCHIVE, Relaxed));
    LWLockRelease(ControlFileLock())?;
    Ok(())
}

// PerformWalRecovery callback: end of base backup, on-disk state consistent.
pub fn ReachedEndOfBackup(end_rec_ptr: XLogRecPtr, tli: TimeLineID) -> PgResult<()> {
    LWLockAcquire(ControlFileLock(), LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
    control_file_update(|cf| {
        if cf.minRecoveryPoint < end_rec_ptr {
            cf.minRecoveryPoint = end_rec_ptr;
            cf.minRecoveryPointTLI = tli;
        }
        cf.backupStartPoint = InvalidXLogRecPtr;
        cf.backupEndPoint = InvalidXLogRecPtr;
        cf.backupEndRequired = false;
    });
    UpdateControlFile()?;
    LWLockRelease(ControlFileLock())?;
    Ok(())
}

pub fn ShutdownXLOG() -> PgResult<()> {
    let level = if init_small::globals::IsPostmasterEnvironment() { LOG } else { NOTICE };
    let _ = elog(level, "shutting down");

    // Signal walsenders to move to stopping state, and wait: prevents
    // commands from writing new WAL during the shutdown checkpoint
    // (xlog.c:6682). Logical walsenders drain and exit here.
    if walsender_seams::wal_snd_init_stopping::is_installed() {
        walsender_seams::wal_snd_init_stopping::call();
        walsender_seams::wal_snd_wait_stopping::call();
    }

    if RecoveryInProgress() {
        CreateRestartPoint(CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_IMMEDIATE)?;
    } else {
        // Rotate first so every remaining record gets archived.
        if XLogArchivingActive() {
            crate::RequestXLogSwitch(false)?;
        }
        CreateCheckPoint(CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_IMMEDIATE)?;
    }
    crate::write::open_log_file_close_if_open()?;
    Ok(())
}

pub(crate) fn shutdown_xlog_seam() -> PgResult<()> {
    ShutdownXLOG()
}

pub(crate) fn XLogPutNextOid(next_oid: types_core::Oid) -> PgResult<()> {
    let body = next_oid.to_ne_bytes();
    xloginsert_seams::xlog_insert::call(RM_XLOG_ID, XLOG_NEXTOID, &[&body])?;
    Ok(())
}
