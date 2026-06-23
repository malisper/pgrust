//! The WAL replay driver (`PerformWalRecovery` / `ApplyWalRecord` /
//! `xlog_redo`-dispatch + consistency / timeline-switch / record-timestamp /
//! backup-page-consistency helpers).
//!
//! Ported 1:1 from `src/backend/access/transam/xlogrecovery.c` (lines
//! 1671-2584), driving the held recovery reader ([`crate::walrecovery`]) and the
//! real rmgr dispatch table ([`backend_access_transam_rmgr::GetRmgr`]).
//!
//! # Held-reader model
//!
//! C threads a `XLogReaderState *xlogreader` file-static through these functions
//! and reads the current decoded record through it. Here the opaque
//! [`RecordRef`] names "the held reader's current record"; the replay functions
//! resolve it against the live reader via [`crate::walrecovery::reader_state`] /
//! [`crate::walrecovery::reader_state_mut`], exactly as the C macros dereference
//! `xlogreader->record`.
//!
//! # error_context_stack divergence
//!
//! C's `ApplyWalRecord` pushes a `rm_redo_error_callback` onto
//! `error_context_stack` so a redo `ereport(ERROR)` is annotated with the WAL
//! record description. This repo retires the ambient `error_context_stack`
//! chain (see `backend-utils-error`): the "WAL redo at LSN for <desc>" context
//! attaches on error propagation instead. The desc family
//! (`rm_redo_error_callback` / `xlog_outdesc`) owns that text and is a separate
//! tracked fill; here we faithfully run the redo and let the error propagate.

extern crate alloc;

use alloc::format;

use backend_utils_error::{ereport, elog};
use types_core::{TimeLineID, TimestampTz};
use types_core::{InvalidXLogRecPtr, XLogRecPtr};
use types_error::{ErrorLocation, PgError, DEBUG1, FATAL, LOG, PANIC};
use types_wal::wal::{XLR_CHECK_CONSISTENCY, XLR_INFO_MASK};
use types_wal::wal::{RM_XACT_ID, RM_XLOG_ID};
use types_wal::xact::{
    XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED, XLOG_XACT_COMMIT, XLOG_XACT_COMMIT_PREPARED,
    XLOG_XACT_OPMASK,
};
use types_wal::xlogutils::STANDBY_INITIALIZED;
use types_wal::rmgrdesc::{xl_end_of_recovery, xl_overwrite_contrecord, xl_restore_point, CheckPoint};

use backend_access_transam_rmgr::GetRmgr;

use crate::core::{
    lsn_fmt, RecordRef, RecoveryPauseState, RecoveryTargetAction, RecoveryTargetType,
    XLogRecoveryState, XLOG_BACKUP_END, XLOG_CHECKPOINT_REDO, XLOG_CHECKPOINT_SHUTDOWN,
    XLOG_END_OF_RECOVERY, XLOG_OVERWRITE_CONTRECORD, XLOG_RESTORE_POINT,
};
use crate::walrecovery::{reader_state, reader_state_mut};

use mcx::Mcx;

// Outward owner seams.
use backend_access_transam_varsup_seams as varsup_seam;
use backend_access_transam_xlog_seams as xlog_seam;
use backend_access_transam_xlogprefetcher_seams as prefetcher_seam;
use backend_access_transam_xlogutils_seams as xlogutils_seam;
use backend_postmaster_startup_seams as startup_seam;
use backend_replication_walreceiver_seams as walreceiver_seam;
use backend_storage_buffer_bufmgr_seams as bufmgr_seam;
use backend_storage_ipc_ipc_seams as ipc_seam;
use backend_storage_ipc_pmsignal_seams as pmsignal_seam;
use backend_storage_ipc_procarray_seams as procarray_seam;
use backend_utils_adt_timestamp_seams as timestamp_seam;

use commands_variable_seam_alias as variable_seam;
// (xlogprefetcher's XLogPrefetchReconfigure seam lives in variable-seams.)
mod commands_variable_seam_alias {
    pub use backend_commands_variable_seams::*;
}

#[inline]
fn loc(lineno: i32, func: &str) -> ErrorLocation {
    ErrorLocation::new("xlogrecovery.c", lineno, func)
}

/// `BLCKSZ` (pg_config.h) — the page size used for the masked-page buffers.
const BLCKSZ: usize = types_core::primitive::BLCKSZ;

/// `timestamptz_to_str(t)` rendered into an owned `String` for a log message.
/// The C `timestamptz_to_str` never fails (it writes a static buffer); the
/// owned-copy seam can only `Err` on OOM, which we render as the C fallback
/// text rather than failing a LOG message.
fn fmt_timestamptz(mcx: Mcx<'_>, t: TimestampTz) -> alloc::string::String {
    match timestamp_seam::timestamptz_to_str::call(mcx, t) {
        Ok(s) => alloc::string::String::from(s.as_str()),
        Err(_) => alloc::string::String::from("(timestamp out of range)"),
    }
}

// ===========================================================================
// PerformWalRecovery — the main redo apply loop (xlogrecovery.c:1670-1922).
// ===========================================================================

/// `void PerformWalRecovery(void)` (xlogrecovery.c:1670) — the main redo loop:
/// read and apply WAL records until the recovery target / end of WAL is
/// reached.
///
/// `mcx` is the startup process's current memory context (C's
/// `CurrentMemoryContext`); the rmgr `rm_startup` callbacks create their
/// recovery contexts under it and `ProcessStartupProcInterrupts` pstrdups GUC
/// snapshots into it.
pub fn perform_wal_recovery<'mcx>(
    st: &mut XLogRecoveryState,
    mcx: Mcx<'mcx>,
) -> Result<(), PgError> {
    let mut reached_recovery_target = false;
    let replay_tli: TimeLineID;

    // Initialize shared variables for tracking progress of WAL replay, as if we
    // had just replayed the record before the REDO location (or the checkpoint
    // record itself, if it's a shutdown checkpoint).
    if st.redo_start_lsn < st.check_point_loc {
        crate::shmem::init_replay_progress(
            InvalidXLogRecPtr,
            st.redo_start_lsn,
            st.redo_start_tli,
        );
    } else {
        let r = reader_state();
        crate::shmem::init_replay_progress(r.ReadRecPtr, r.EndRecPtr, st.check_point_tli);
    }

    // Also ensure XLogReceiptTime has a sane value.
    crate::shmem::set_xlog_receipt_time(timestamp_seam::get_current_timestamp::call());

    // Let postmaster know we've started redo now, so that it can launch the
    // archiver if necessary.
    if backend_utils_init_small_seams::is_under_postmaster::call() {
        pmsignal_seam::send_postmaster_signal_recovery_started::call();
    }

    // Allow read-only connections immediately if we're consistent already.
    check_recovery_consistency(st)?;

    let mut record: RecordRef;

    // Find the first record that logically follows the checkpoint --- it might
    // physically precede it, though.
    if st.redo_start_lsn < st.check_point_loc {
        // back up to find the record
        replay_tli = st.redo_start_tli;
        prefetcher_seam::prefetcher_begin_read::call(st.redo_start_lsn);
        record = crate::readrecord::read_record(st, PANIC, false, replay_tli)?;

        // If a checkpoint record's redo pointer points back to an earlier LSN,
        // the record at that LSN should be an XLOG_CHECKPOINT_REDO record.
        let r = reader_state();
        if backend_access_transam_xlogreader::XLogRecGetRmid(r) != RM_XLOG_ID
            || (backend_access_transam_xlogreader::XLogRecGetInfo(r) & !XLR_INFO_MASK)
                != XLOG_CHECKPOINT_REDO
        {
            ereport(FATAL)
                .errmsg(format!(
                    "unexpected record type found at redo point {}",
                    lsn_fmt(r.ReadRecPtr)
                ))
                .finish(loc(1735, "PerformWalRecovery"))?;
        }
    } else {
        // just have to read next record after CheckPoint
        debug_assert!(reader_state().ReadRecPtr == st.check_point_loc);
        replay_tli = st.check_point_tli;
        record = crate::readrecord::read_record(st, LOG, false, replay_tli)?;
    }

    let mut replay_tli = replay_tli;

    if record != RecordRef::default() {
        let ru0 = backend_utils_misc_pg_rusage::pg_rusage_new();

        st.in_redo = true;

        // RmgrStartup() — start up all resource managers.
        backend_access_transam_rmgr::RmgrStartup(mcx)?;

        let _ = ereport(LOG)
            .errmsg(format!(
                "redo starts at {}",
                lsn_fmt(reader_state().ReadRecPtr)
            ))
            .finish(loc(1758, "PerformWalRecovery"));

        // Prepare to report progress of the redo phase.
        if !st.standby_mode {
            startup_seam::begin_startup_progress_phase::call();
        }

        // main redo apply loop
        loop {
            if !st.standby_mode {
                ereport_startup_progress_redo(reader_state().ReadRecPtr);
            }

            // Handle interrupt signals of startup process.
            startup_seam::process_startup_proc_interrupts::call(mcx)?;

            // Pause WAL replay, if requested by a hot-standby session via
            // SetRecoveryPause(). The unlocked read mirrors the C volatile peek.
            if crate::shmem::recovery_pause_state_unlocked() != RecoveryPauseState::NotPaused {
                crate::stop::recovery_pauses_here(st, false);
            }

            // Have we reached our recovery target?
            if crate::stop::recovery_stops_before(st, mcx, record)? {
                reached_recovery_target = true;
                break;
            }

            // If we've been asked to lag the primary, wait on latch until enough
            // time has passed.
            if crate::stop::recovery_apply_delay(st, record) {
                // We test for paused recovery again here. If user sets delayed
                // apply, it may be because they expect to pause recovery in case
                // of problems, so we must test again here otherwise pausing
                // during the delay-wait wouldn't work.
                if crate::shmem::recovery_pause_state_unlocked() != RecoveryPauseState::NotPaused {
                    crate::stop::recovery_pauses_here(st, false);
                }
            }

            // Apply the record.
            apply_wal_record(st, record, &mut replay_tli)?;

            // Exit loop if we reached inclusive recovery target.
            if crate::stop::recovery_stops_after(st, mcx, record)? {
                reached_recovery_target = true;
                break;
            }

            // Else, try to fetch the next WAL record.
            record = crate::readrecord::read_record(st, LOG, false, replay_tli)?;
            if record == RecordRef::default() {
                break;
            }
        }
        // end of main redo apply loop

        if reached_recovery_target {
            if !st.reached_consistency {
                ereport(FATAL)
                    .errmsg("requested recovery stop point is before consistent recovery point")
                    .finish(loc(1861, "PerformWalRecovery"))?;
            }

            // This is the last point where we can restart recovery with a new
            // recovery target, if we shutdown and begin again. After this,
            // Resource Managers may choose to do permanent corrective actions at
            // end of recovery.
            match st.recovery_target_action {
                RecoveryTargetAction::Shutdown => {
                    // exit with special return code to request shutdown of
                    // postmaster. Log messages issued from postmaster.
                    ipc_seam::proc_exit::call(3);
                }
                RecoveryTargetAction::Pause => {
                    crate::stop::set_recovery_pause(st, true);
                    crate::stop::recovery_pauses_here(st, true);
                    // drop into promote
                }
                RecoveryTargetAction::Promote => {}
            }
        }

        backend_access_transam_rmgr::RmgrCleanup();

        let _ = ereport(LOG)
            .errmsg(format!(
                "redo done at {} system usage: {}",
                lsn_fmt(reader_state().ReadRecPtr),
                backend_utils_misc_pg_rusage::pg_rusage_show(&ru0)
            ))
            .finish(loc(1893, "PerformWalRecovery"));
        let xtime = crate::shmem::get_latest_xtime();
        if xtime != 0 {
            let _ = ereport(LOG)
                .errmsg(format!(
                    "last completed transaction was at log time {}",
                    fmt_timestamptz(mcx, xtime)
                ))
                .finish(loc(1899, "PerformWalRecovery"));
        }

        st.in_redo = false;
    } else {
        // there are no WAL records following the checkpoint
        let _ = ereport(LOG)
            .errmsg("redo is not required")
            .finish(loc(1908, "PerformWalRecovery"));
    }

    // This check is intentionally after the above log messages that indicate
    // how far recovery went.
    if st.archive_recovery_requested
        && st.recovery_target != RecoveryTargetType::Unset
        && !reached_recovery_target
    {
        ereport(FATAL)
            .errcode(types_error::ERRCODE_CONFIG_FILE_ERROR)
            .errmsg("recovery ended before configured recovery target was reached")
            .finish(loc(1919, "PerformWalRecovery"))?;
    }

    Ok(())
}

/// The `ereport_startup_progress("redo in progress, ...")` macro expansion
/// (xlogrecovery.c:1772): log a progress line only when the startup-progress
/// timeout has expired.
fn ereport_startup_progress_redo(read_rec_ptr: XLogRecPtr) {
    if let Some((secs, usecs)) = startup_seam::has_startup_progress_timeout_expired::call() {
        let _ = ereport(LOG)
            .errmsg(format!(
                "redo in progress, elapsed time: {}.{:02} s, current LSN: {}",
                secs,
                usecs / 10000,
                lsn_fmt(read_rec_ptr)
            ))
            .finish(loc(1772, "PerformWalRecovery"));
    }
}

// ===========================================================================
// ApplyWalRecord (xlogrecovery.c:1927-2085).
// ===========================================================================

/// `static void ApplyWalRecord(XLogReaderState *xlogreader, XLogRecord *record,`
/// `TimeLineID *replayTLI)` (xlogrecovery.c:1927) — apply a single WAL record.
pub(crate) fn apply_wal_record(
    st: &mut XLogRecoveryState,
    _record: RecordRef,
    replay_tli: &mut TimeLineID,
) -> Result<(), PgError> {
    let mut switched_tli = false;

    // NOTE: C sets up rm_redo_error_callback on error_context_stack here; that
    // chain is retired in this repo (see the module doc). The "WAL redo at LSN"
    // context is owned by the desc family and attaches on error propagation.

    // TransamVariables->nextXid must be beyond record's xid.
    let xl_xid = backend_access_transam_xlogreader::XLogRecGetXid(reader_state());
    varsup_seam::advance_next_full_transaction_id_past_xid::call(xl_xid);

    // Before replaying this record, check if this record causes the current
    // timeline to change. The record is already considered to be part of the new
    // timeline, so we update replayTLI before replaying it. That's important so
    // that replayEndTLI, which is recorded as the minimum recovery point's TLI if
    // recovery stops after this record, is set correctly.
    let xl_rmid = backend_access_transam_xlogreader::XLogRecGetRmid(reader_state());
    let end_rec_ptr = reader_state().EndRecPtr;
    if xl_rmid == RM_XLOG_ID {
        let mut new_replay_tli = *replay_tli;
        let mut prev_replay_tli = *replay_tli;
        let info = backend_access_transam_xlogreader::XLogRecGetInfo(reader_state()) & !XLR_INFO_MASK;

        if info == XLOG_CHECKPOINT_SHUTDOWN {
            let data = backend_access_transam_xlogreader::XLogRecGetData(reader_state());
            let check_point = CheckPoint::from_bytes(data)
                .expect("XLOG_CHECKPOINT_SHUTDOWN record too short for CheckPoint");
            new_replay_tli = check_point.this_timeline_id();
            prev_replay_tli = check_point.prev_timeline_id();
        } else if info == XLOG_END_OF_RECOVERY {
            let data = backend_access_transam_xlogreader::XLogRecGetData(reader_state());
            let xlrec = xl_end_of_recovery::from_bytes(data)
                .expect("XLOG_END_OF_RECOVERY record too short for xl_end_of_recovery");
            new_replay_tli = xlrec.this_timeline_id();
            prev_replay_tli = xlrec.prev_timeline_id();
        }

        if new_replay_tli != *replay_tli {
            // Check that it's OK to switch to this TLI.
            check_time_line_switch(st, end_rec_ptr, new_replay_tli, prev_replay_tli, *replay_tli)?;

            // Following WAL records should be run with new TLI.
            *replay_tli = new_replay_tli;
            switched_tli = true;
        }
    }

    // Update shared replayEndRecPtr before replaying this record, so that
    // XLogFlush will update minRecoveryPoint correctly.
    crate::shmem::set_replay_end(end_rec_ptr, *replay_tli);

    // If we are attempting to enter Hot Standby mode, process XIDs we see.
    if xlogutils_seam::standby_state::call() >= STANDBY_INITIALIZED
        && types_core::xact::TransactionIdIsValid(xl_xid)
    {
        procarray_seam::record_known_assigned_transaction_ids::call(xl_xid)?;
    }

    // Some XLOG record types that are related to recovery are processed directly
    // here, rather than in xlog_redo().
    if xl_rmid == RM_XLOG_ID {
        xlogrecovery_redo(st, _record, *replay_tli)?;
    }

    // Now apply the WAL record itself.
    let rmgr = GetRmgr(xl_rmid)?;
    let rm_redo = rmgr
        .rm_redo
        .expect("resource manager has no redo routine");
    rm_redo(reader_state_mut())?;

    // After redo, check whether the backup pages associated with the WAL record
    // are consistent with the existing pages. This check is done only if
    // consistency check is enabled for this record.
    if (backend_access_transam_xlogreader::XLogRecGetInfo(reader_state()) & XLR_CHECK_CONSISTENCY)
        != 0
    {
        verify_backup_page_consistency(st, _record)?;
    }

    // (error_context_stack pop is a no-op here — the chain is retired.)

    // Update lastReplayedEndRecPtr after this record has been successfully
    // replayed.
    {
        let r = reader_state();
        crate::shmem::set_last_replayed(r.ReadRecPtr, r.EndRecPtr, *replay_tli);
    }

    // Wakeup walsenders (see the long comment in C): physical walsenders on a
    // new timeline + cascade, logical walsenders when cascade replication is
    // allowed.
    if xlog_seam::allow_cascade_replication::call() {
        backend_replication_walsender_seams::wal_snd_wakeup::call(switched_tli, true)?;
    }

    // If rm_redo called XLogRequestWalReceiverReply, then we wake up the receiver
    // so that it notices the updated lastReplayedEndRecPtr and sends a reply to
    // the primary.
    if st.do_request_wal_receiver_reply {
        st.do_request_wal_receiver_reply = false;
        walreceiver_seam::wal_rcv_force_reply::call();
    }

    // Allow read-only connections if we're consistent now.
    check_recovery_consistency(st)?;

    // Is this a timeline switch?
    if switched_tli {
        // Before we continue on the new timeline, clean up any (possibly bogus)
        // future WAL segments on the old timeline.
        xlog_seam::remove_non_parent_xlog_files::call(reader_state().EndRecPtr, *replay_tli)?;

        // Reset the prefetcher.
        variable_seam::xlog_prefetch_reconfigure::call();
    }

    Ok(())
}

// ===========================================================================
// xlogrecovery_redo (xlogrecovery.c:2091-2146) — the RM_XLOG_ID handler for the
// recovery-related XLOG record types.
// ===========================================================================

/// `static void xlogrecovery_redo(XLogReaderState *record, TimeLineID replayTLI)`
/// (xlogrecovery.c:2091).
pub(crate) fn xlogrecovery_redo(
    st: &mut XLogRecoveryState,
    _record: RecordRef,
    _replay_tli: TimeLineID,
) -> Result<(), PgError> {
    let info = backend_access_transam_xlogreader::XLogRecGetInfo(reader_state()) & !XLR_INFO_MASK;
    let lsn = reader_state().EndRecPtr;

    debug_assert!(backend_access_transam_xlogreader::XLogRecGetRmid(reader_state()) == RM_XLOG_ID);

    if info == XLOG_OVERWRITE_CONTRECORD {
        // Verify the payload of a XLOG_OVERWRITE_CONTRECORD record.
        let data = backend_access_transam_xlogreader::XLogRecGetData(reader_state());
        let xlrec = xl_overwrite_contrecord::from_bytes(data)
            .expect("XLOG_OVERWRITE_CONTRECORD record too short");
        let overwritten_rec_ptr = reader_state().overwrittenRecPtr;
        if xlrec.overwritten_lsn() != overwritten_rec_ptr {
            elog(
                FATAL,
                format!(
                    "mismatching overwritten LSN {} -> {}",
                    lsn_fmt(xlrec.overwritten_lsn()),
                    lsn_fmt(overwritten_rec_ptr)
                ),
            )?;
        }

        // We have safely skipped the aborted record.
        st.aborted_rec_ptr = InvalidXLogRecPtr;
        st.missing_contrec_ptr = InvalidXLogRecPtr;

        let _ = ereport(LOG)
            .errmsg(format!(
                "successfully skipped missing contrecord at {}, overwritten at {}",
                lsn_fmt(xlrec.overwritten_lsn()),
                fmt_timestamptz(
                    reader_state()
                        .decode_arena
                        .expect("recovery reader has a decode arena"),
                    xlrec.overwrite_time()
                )
            ))
            .finish(loc(2114, "xlogrecovery_redo"));

        // Verifying the record should only happen once.
        reader_state_mut().overwrittenRecPtr = InvalidXLogRecPtr;
    } else if info == XLOG_BACKUP_END {
        let data = backend_access_transam_xlogreader::XLogRecGetData(reader_state());
        // memcpy(&startpoint, XLogRecGetData(record), sizeof(startpoint)).
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&data[..8]);
        let startpoint = XLogRecPtr::from_ne_bytes(bytes);

        if st.backup_start_point == startpoint {
            // We have reached the end of base backup, the point where
            // pg_backup_stop() was done. The data on disk is now consistent
            // (assuming we have also reached minRecoveryPoint). Set
            // backupEndPoint to the current LSN, so that the next call to
            // CheckRecoveryConsistency() will notice it and do the
            // end-of-backup processing.
            let _ = elog(DEBUG1, "end of backup record reached");
            st.backup_end_point = lsn;
        } else {
            let _ = elog(
                DEBUG1,
                format!(
                    "saw end-of-backup record for backup starting at {}, waiting for {}",
                    lsn_fmt(startpoint),
                    lsn_fmt(st.backup_start_point)
                ),
            );
        }
    }

    Ok(())
}

// ===========================================================================
// CheckRecoveryConsistency (xlogrecovery.c:2195-2291).
// ===========================================================================

/// `static void CheckRecoveryConsistency(void)` (xlogrecovery.c:2195) — check
/// whether recovery has reached a consistent state and notify the postmaster.
pub(crate) fn check_recovery_consistency(st: &mut XLogRecoveryState) -> Result<(), PgError> {
    // During crash recovery, we don't reach a consistent state until we've
    // replayed all the WAL.
    if st.min_recovery_point == InvalidXLogRecPtr {
        return Ok(());
    }

    debug_assert!(st.in_archive_recovery);

    // assume that we are called in the startup process, and hence don't need a
    // lock to read lastReplayedEndRecPtr.
    let (last_replayed_end_rec_ptr, last_replayed_tli) =
        crate::shmem::last_replayed_end_rec_ptr_unlocked();

    // Have we reached the point where our base backup was completed?
    if st.backup_end_point != InvalidXLogRecPtr && st.backup_end_point <= last_replayed_end_rec_ptr {
        let save_backup_start_point = st.backup_start_point;
        let save_backup_end_point = st.backup_end_point;

        let _ = elog(DEBUG1, "end of backup reached");

        // We have reached the end of base backup, as indicated by pg_control.
        // Update the control file accordingly.
        xlog_seam::reached_end_of_backup::call(last_replayed_end_rec_ptr, last_replayed_tli)?;
        st.backup_start_point = InvalidXLogRecPtr;
        st.backup_end_point = InvalidXLogRecPtr;
        st.backup_end_required = false;

        let _ = ereport(LOG)
            .errmsg(format!(
                "completed backup recovery with redo LSN {} and end LSN {}",
                lsn_fmt(save_backup_start_point),
                lsn_fmt(save_backup_end_point)
            ))
            .finish(loc(2237, "CheckRecoveryConsistency"));
    }

    // Have we passed our safe starting point? Note that minRecoveryPoint is
    // known to be incorrectly set if recovering from a backup, until the
    // XLOG_BACKUP_END arrives to advise us of the correct minRecoveryPoint. All
    // we know prior to that is that we're not consistent yet.
    if !st.reached_consistency
        && !st.backup_end_required
        && st.min_recovery_point <= last_replayed_end_rec_ptr
    {
        // Check to see if the XLOG sequence contained any unresolved references
        // to uninitialized pages.
        xlogutils_seam::xlog_check_invalid_pages::call()?;

        // Check that pg_tblspc doesn't contain any real directories. Replay of
        // Database/CREATE_* records may have created fictitious tablespace
        // directories that should have been removed by the time consistency was
        // reached.
        check_tablespace_directory()?;

        st.reached_consistency = true;
        pmsignal_seam::send_postmaster_signal_recovery_consistent::call();
        let _ = ereport(LOG)
            .errmsg(format!(
                "consistent recovery state reached at {}",
                lsn_fmt(last_replayed_end_rec_ptr)
            ))
            .finish(loc(2268, "CheckRecoveryConsistency"));
    }

    // Have we got a valid starting snapshot that will allow queries to be run?
    // If so, we can tell postmaster that the database is consistent now,
    // enabling connections.
    if xlogutils_seam::standby_state::call() == types_wal::xlogutils::STANDBY_SNAPSHOT_READY
        && !st.local_hot_standby_active
        && st.reached_consistency
        && backend_utils_init_small_seams::is_under_postmaster::call()
    {
        crate::shmem::set_shared_hot_standby_active();
        st.local_hot_standby_active = true;
        pmsignal_seam::send_postmaster_signal_begin_hot_standby::call();
    }

    Ok(())
}

/// `static void CheckTablespaceDirectory(void)` (xlogrecovery.c:2162) — verify
/// that `./pg_tblspc` doesn't contain any real directories (only symlinks).
///
/// The directory scan + `get_dirent_type` are pure libc filesystem operations
/// (no PG TU owns them); the `allow_in_place_tablespaces` GUC governs whether a
/// stray directory is a WARNING or a PANIC.
fn check_tablespace_directory() -> Result<(), PgError> {
    use core::ffi::CStr;

    const PG_TBLSPC_DIR: &str = "pg_tblspc";

    // AllocateDir(PG_TBLSPC_DIR) — opendir. The fd-tracking AllocateDir is the
    // fd.c owner; here recovery only walks the names, so a direct readdir over
    // the libc handle mirrors the C loop (fd.c's AllocateDir/ReadDir add VFD
    // bookkeeping that has no observable effect on this scan).
    extern crate std;
    #[cfg(not(target_family = "wasm"))]
    use std::os::unix::ffi::OsStrExt;
    #[cfg(target_family = "wasm")]
    use wasm_libc_shim::osfs::OsStrBytesExt as OsStrExt;

    let entries = match std::fs::read_dir(PG_TBLSPC_DIR) {
        Ok(e) => e,
        // C's AllocateDir ereport(ERROR)s if the directory can't be opened; in
        // a valid data directory it always exists.
        Err(e) => {
            return Err(PgError::new(
                types_error::ERROR,
                format!("could not open directory \"{PG_TBLSPC_DIR}\": {e}"),
            ));
        }
    };

    let allow_in_place =
        backend_commands_tablespace_globals_seams::allow_in_place_tablespaces::call()?;

    for de in entries {
        let de = match de {
            Ok(d) => d,
            Err(_) => continue,
        };
        let name = de.file_name();
        let name_bytes = name.as_bytes();

        // Skip entries of non-oid names (strspn over digits == strlen).
        if name_bytes.is_empty() || !name_bytes.iter().all(|b| b.is_ascii_digit()) {
            continue;
        }

        // get_dirent_type(path, de, false, ERROR) != PGFILETYPE_LNK.
        let path = format!("{PG_TBLSPC_DIR}/{}", de.file_name().to_string_lossy());
        let is_lnk = std::fs::symlink_metadata(&path)
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false);
        let _ = CStr::from_bytes_with_nul(b"\0"); // keep CStr import used on all targets

        if !is_lnk {
            let level = if allow_in_place {
                types_error::WARNING
            } else {
                PANIC
            };
            ereport(level)
                .errcode(types_error::ERRCODE_DATA_CORRUPTED)
                .errmsg(format!(
                    "unexpected directory entry \"{}\" found in {PG_TBLSPC_DIR}",
                    de.file_name().to_string_lossy()
                ))
                .errdetail(format!(
                    "All directory entries in {PG_TBLSPC_DIR}/ should be symbolic links."
                ))
                .errhint(
                    "Remove those directories, or set \"allow_in_place_tablespaces\" to ON \
                     transiently to let recovery complete.",
                )
                .finish(loc(2180, "CheckTablespaceDirectory"))?;
        }
    }

    Ok(())
}

// ===========================================================================
// checkTimeLineSwitch (xlogrecovery.c:2398-2436).
// ===========================================================================

/// `static void checkTimeLineSwitch(XLogRecPtr lsn, TimeLineID newTLI,`
/// `TimeLineID prevTLI, TimeLineID replayTLI)` (xlogrecovery.c:2398).
pub(crate) fn check_time_line_switch(
    st: &XLogRecoveryState,
    lsn: XLogRecPtr,
    new_tli: TimeLineID,
    prev_tli: TimeLineID,
    replay_tli: TimeLineID,
) -> Result<(), PgError> {
    // Check that the record agrees on what the current (old) timeline is.
    if prev_tli != replay_tli {
        ereport(PANIC)
            .errmsg(format!(
                "unexpected previous timeline ID {prev_tli} (current timeline ID {replay_tli}) \
                 in checkpoint record"
            ))
            .finish(loc(2404, "checkTimeLineSwitch"))?;
    }

    // The new timeline better be in the list of timelines we expect to see,
    // according to the timeline history. It should also not decrease.
    if new_tli < replay_tli
        || !backend_access_transam_timeline_seams::tli_in_history::call(
            new_tli,
            &st.expected_tles,
        )
    {
        ereport(PANIC)
            .errmsg(format!(
                "unexpected timeline ID {new_tli} (after {replay_tli}) in checkpoint record"
            ))
            .finish(loc(2413, "checkTimeLineSwitch"))?;
    }

    // If we have not yet reached min recovery point, and we're about to switch
    // to a timeline greater than the timeline of the min recovery point:
    // trouble. After switching to the new timeline, we could not possibly visit
    // the min recovery point on the correct timeline anymore.
    if st.min_recovery_point != InvalidXLogRecPtr
        && lsn < st.min_recovery_point
        && new_tli > st.min_recovery_point_tli
    {
        ereport(PANIC)
            .errmsg(format!(
                "unexpected timeline ID {new_tli} in checkpoint record, before reaching minimum \
                 recovery point {} on timeline {}",
                lsn_fmt(st.min_recovery_point),
                st.min_recovery_point_tli
            ))
            .finish(loc(2429, "checkTimeLineSwitch"))?;
    }

    // Looks good.
    Ok(())
}

// ===========================================================================
// getRecordTimestamp (xlogrecovery.c:2447-2472).
// ===========================================================================

/// `static bool getRecordTimestamp(XLogReaderState *record, TimestampTz *recordXtime)`
/// (xlogrecovery.c:2447) — extract the commit/abort/restore-point timestamp from
/// a record, if it carries one.
pub(crate) fn get_record_timestamp(_record: RecordRef, record_xtime: &mut TimestampTz) -> bool {
    let r = reader_state();
    let info = backend_access_transam_xlogreader::XLogRecGetInfo(r) & !XLR_INFO_MASK;
    let xact_info = info & XLOG_XACT_OPMASK;
    let rmid = backend_access_transam_xlogreader::XLogRecGetRmid(r);

    if rmid == RM_XLOG_ID && info == XLOG_RESTORE_POINT {
        let data = backend_access_transam_xlogreader::XLogRecGetData(r);
        // ((xl_restore_point *) XLogRecGetData(record))->rp_time.
        if let Some(rp) = xl_restore_point::from_bytes(data) {
            *record_xtime = rp.rp_time();
            return true;
        }
        // A truncated restore-point record can't carry a usable timestamp.
        return false;
    }
    if rmid == RM_XACT_ID
        && (xact_info == XLOG_XACT_COMMIT || xact_info == XLOG_XACT_COMMIT_PREPARED)
    {
        // ((xl_xact_commit *) XLogRecGetData(record))->xact_time — xact_time is
        // the first field of xl_xact_commit (a TimestampTz at offset 0).
        let data = backend_access_transam_xlogreader::XLogRecGetData(r);
        *record_xtime = read_xact_time(data);
        return true;
    }
    if rmid == RM_XACT_ID && (xact_info == XLOG_XACT_ABORT || xact_info == XLOG_XACT_ABORT_PREPARED)
    {
        // ((xl_xact_abort *) XLogRecGetData(record))->xact_time — likewise the
        // first field of xl_xact_abort.
        let data = backend_access_transam_xlogreader::XLogRecGetData(r);
        *record_xtime = read_xact_time(data);
        return true;
    }
    false
}

/// Read the leading `TimestampTz xact_time` field shared by `xl_xact_commit`
/// and `xl_xact_abort` (offset 0, an 8-byte `int64`).
#[inline]
fn read_xact_time(data: &[u8]) -> TimestampTz {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&data[..8]);
    TimestampTz::from_ne_bytes(bytes)
}

// ===========================================================================
// verifyBackupPageConsistency (xlogrecovery.c:2482-2584).
// ===========================================================================

/// `static void verifyBackupPageConsistency(XLogReaderState *record)`
/// (xlogrecovery.c:2482) — the `wal_consistency_checking` masked-page
/// comparison.
pub(crate) fn verify_backup_page_consistency(
    _st: &XLogRecoveryState,
    _record: RecordRef,
) -> Result<(), PgError> {
    use types_storage::buf::BUFFER_LOCK_EXCLUSIVE;
    use types_storage::{BufferIsValid, InvalidBuffer, ReadBufferMode};

    let rmid = backend_access_transam_xlogreader::XLogRecGetRmid(reader_state());
    let rmgr = GetRmgr(rmid)?;

    // Records with no backup blocks have no need for consistency checks.
    // XLogRecHasAnyBlockRefs == (max_block_id >= 0).
    let max_block_id = read_max_block_id();
    if max_block_id < 0 {
        return Ok(());
    }

    debug_assert!(
        (backend_access_transam_xlogreader::XLogRecGetInfo(reader_state()) & XLR_CHECK_CONSISTENCY)
            != 0
    );

    let end_rec_ptr = reader_state().EndRecPtr;

    // Scratch pages for the masked comparison (C's primary_image_masked /
    // replay_image_masked file-statics, palloc'd BLCKSZ each).
    let mut replay_image_masked = alloc::vec![0u8; BLCKSZ];
    let mut primary_image_masked = alloc::vec![0u8; BLCKSZ];

    for block_id in 0..=max_block_id as u8 {
        let tag = match backend_access_transam_xlogreader::xlog_rec_get_block_tag_extended(
            reader_state(),
            block_id,
        ) {
            Some(t) => t,
            // WAL record doesn't contain a block reference with the given id.
            None => continue,
        };

        debug_assert!(backend_access_transam_xlogreader::xlog_rec_has_block_image(
            reader_state(),
            block_id
        ));

        if backend_access_transam_xlogreader::xlog_rec_block_image_apply(reader_state(), block_id) {
            // WAL record has already applied the page, so bypass the consistency
            // check as that would result in comparing the full page stored in the
            // record with itself.
            continue;
        }

        // Read the contents from the current buffer and store it in a temporary
        // page.
        let buf = bufmgr_seam::xlog_read_buffer_extended::call(
            tag.rlocator,
            tag.forknum,
            tag.blkno,
            ReadBufferMode::NormalNoLog,
            InvalidBuffer,
        )?;
        if !BufferIsValid(buf) {
            continue;
        }

        bufmgr_seam::lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;

        // Take a copy of the local page where WAL has been applied to have a
        // comparison base before masking it.
        let page = bufmgr_seam::buffer_get_page::call(
            reader_state().decode_arena.expect("recovery reader has a decode arena"),
            buf,
        )?;
        replay_image_masked[..BLCKSZ].copy_from_slice(&page[..BLCKSZ]);

        // No need for this page anymore now that a copy is in.
        bufmgr_seam::unlock_release_buffer::call(buf);

        // If the block LSN is already ahead of this WAL record, we can't expect
        // contents to match. This can happen if recovery is restarted.
        if page_get_lsn(&replay_image_masked) > end_rec_ptr {
            continue;
        }

        // Read the contents from the backup copy, stored in WAL record and store
        // it in a temporary page (a plain scratch buffer, masked in place).
        if !backend_access_transam_xlogreader::restore_block_image_bytes(
            reader_state(),
            block_id,
            &mut primary_image_masked,
        )? {
            let msg = backend_access_transam_xlogreader::reader_errormsg_buf(reader_state());
            return Err(PgError::new(types_error::ERROR, msg)
                .with_sqlstate(types_error::ERRCODE_INTERNAL_ERROR));
        }

        // If masking function is defined, mask both the primary and replay
        // images.
        if let Some(rm_mask) = rmgr.rm_mask {
            rm_mask(&mut replay_image_masked, tag.blkno)?;
            rm_mask(&mut primary_image_masked, tag.blkno)?;
        }

        // Time to compare the primary and replay images.
        if replay_image_masked[..BLCKSZ] != primary_image_masked[..BLCKSZ] {
            elog(
                FATAL,
                format!(
                    "inconsistent page found, rel {}/{}/{}, forknum {}, blkno {}",
                    tag.rlocator.spcOid,
                    tag.rlocator.dbOid,
                    tag.rlocator.relNumber,
                    tag.forknum as u32,
                    tag.blkno
                ),
            )?;
        }
    }

    Ok(())
}

/// `XLogRecMaxBlockId(record)` over the held reader (`record->max_block_id`).
fn read_max_block_id() -> i32 {
    backend_access_transam_xlogreader::reader_max_block_id(reader_state())
}

/// `PageGetLSN(page)` (bufpage.h) — the page LSN lives at the start of the page
/// header (`pd_lsn`, an 8-byte `XLogRecPtr`). Read off the scratch byte copy.
#[inline]
fn page_get_lsn(page: &[u8]) -> XLogRecPtr {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&page[..8]);
    XLogRecPtr::from_ne_bytes(bytes)
}
