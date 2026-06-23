//! Recovery-target stop-point logic + the pause-state accessors
//! (`recoveryStopsBefore` / `recoveryStopsAfter`, `getRecoveryStopReason`,
//! `recoveryPausesHere`, `recoveryApplyDelay`, `GetRecoveryPauseState` /
//! `SetRecoveryPause` / `ConfirmRecoveryPaused`).
//!
//! **Scaffold module.** Faithful signatures, honest `panic!` bodies the
//! family-fill lanes replace against [`crate::core::XLogRecoveryState`].
//!
//! Note: the *shared* `GetRecoveryPauseState` / `SetRecoveryPause` that read the
//! `XLogRecoveryCtl` shmem region under `info_lck` are the already-landed
//! [`crate::shmem`] accessors; the `&mut XLogRecoveryState` forms here are the
//! startup process's view that the C file threads through replay.
//!
//! Ported from `src/backend/access/transam/xlogrecovery.c`.

use alloc::format;
use alloc::string::String;

use mcx::Mcx;
use types_core::primitive::{TimestampTz, TransactionId};
use types_core::{InvalidTransactionId, InvalidXLogRecPtr};
use types_error::{PgError, LOG};
use wal::wal::{RM_XACT_ID, RM_XLOG_ID, XLR_INFO_MASK};
use wal::xact::{
    XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED, XLOG_XACT_COMMIT, XLOG_XACT_COMMIT_PREPARED,
    XLOG_XACT_OPMASK,
};
use wal::rmgrdesc::xl_restore_point;

use utils_error::ereport;

use crate::core::{lsn_fmt, RecordRef, RecoveryPauseState, RecoveryTargetType, XLogRecoveryState};
use crate::replay::get_record_timestamp;
use crate::walrecovery::reader_state;

use timestamp_seams as timestamp_seam;

#[inline]
fn loc(lineno: i32, func: &str) -> types_error::ErrorLocation {
    types_error::ErrorLocation::new("xlogrecovery.c", lineno, func)
}

/// `timestamptz_to_str(t)` rendered to an owned `String` for a log message; the
/// C never fails (static buffer), so OOM of the owned copy renders the fallback
/// text rather than failing a LOG message.
fn fmt_timestamptz(mcx: Mcx<'_>, t: TimestampTz) -> String {
    match timestamp_seam::timestamptz_to_str::call(mcx, t) {
        Ok(s) => String::from(s.as_str()),
        Err(_) => String::from("(timestamp out of range)"),
    }
}

/// `static bool recoveryStopsBefore(XLogReaderState *record)`
/// (xlogrecovery.c:2594).
pub(crate) fn recovery_stops_before(
    st: &mut XLogRecoveryState,
    mcx: Mcx<'_>,
    _record: RecordRef,
) -> Result<bool, PgError> {
    let mut stops_here = false;
    let is_commit;
    let mut record_xtime: TimestampTz = 0;
    let record_xid: TransactionId;

    let r = reader_state();

    // Ignore recovery target settings when not in archive recovery (meaning we
    // are in crash recovery).
    if !st.archive_recovery_requested {
        return Ok(false);
    }

    // Check if we should stop as soon as reaching consistency.
    if st.recovery_target == RecoveryTargetType::Immediate && st.reached_consistency {
        ereport(LOG)
            .errmsg("recovery stopping after reaching consistency")
            .finish(loc(2613, "recoveryStopsBefore"))?;

        st.recovery_stop_after = false;
        st.recovery_stop_xid = InvalidTransactionId;
        st.recovery_stop_lsn = InvalidXLogRecPtr;
        st.recovery_stop_time = 0;
        st.recovery_stop_name.clear();
        return Ok(true);
    }

    // Check if target LSN has been reached.
    if st.recovery_target == RecoveryTargetType::Lsn
        && !st.recovery_target_inclusive
        && r.ReadRecPtr >= st.recovery_target_lsn
    {
        st.recovery_stop_after = false;
        st.recovery_stop_xid = InvalidTransactionId;
        st.recovery_stop_lsn = r.ReadRecPtr;
        st.recovery_stop_time = 0;
        st.recovery_stop_name.clear();
        ereport(LOG)
            .errmsg(format!(
                "recovery stopping before WAL location (LSN) \"{}\"",
                lsn_fmt(st.recovery_stop_lsn)
            ))
            .finish(loc(2634, "recoveryStopsBefore"))?;
        return Ok(true);
    }

    // Otherwise we only consider stopping before COMMIT or ABORT records.
    if xlogreader::XLogRecGetRmid(r) != RM_XACT_ID {
        return Ok(false);
    }

    let info = xlogreader::XLogRecGetInfo(r);
    let xact_info = info & XLOG_XACT_OPMASK;

    if xact_info == XLOG_XACT_COMMIT {
        is_commit = true;
        record_xid = xlogreader::XLogRecGetXid(r);
    } else if xact_info == XLOG_XACT_COMMIT_PREPARED {
        let data = xlogreader::XLogRecGetData(r);
        let parsed = xactdesc::parse_commit_record(info, data)?;
        is_commit = true;
        record_xid = parsed.twophase_xid;
    } else if xact_info == XLOG_XACT_ABORT {
        is_commit = false;
        record_xid = xlogreader::XLogRecGetXid(r);
    } else if xact_info == XLOG_XACT_ABORT_PREPARED {
        let data = xlogreader::XLogRecGetData(r);
        let parsed = xactdesc::parse_abort_record(info, data)?;
        is_commit = false;
        record_xid = parsed.twophase_xid;
    } else {
        return Ok(false);
    }

    if st.recovery_target == RecoveryTargetType::Xid && !st.recovery_target_inclusive {
        // There can be only one transaction end record with this exact
        // transactionid; test for equality only.
        stops_here = record_xid == st.recovery_target_xid;
    }

    // Note: we must fetch recordXtime regardless of recoveryTarget setting.
    if get_record_timestamp(_record, &mut record_xtime)
        && st.recovery_target == RecoveryTargetType::Time
    {
        // There can be many transactions that share the same commit time, so we
        // stop after the last one if inclusive, or at the first one if exclusive.
        if st.recovery_target_inclusive {
            stops_here = record_xtime > st.recovery_target_time;
        } else {
            stops_here = record_xtime >= st.recovery_target_time;
        }
    }

    if stops_here {
        st.recovery_stop_after = false;
        st.recovery_stop_xid = record_xid;
        st.recovery_stop_time = record_xtime;
        st.recovery_stop_lsn = InvalidXLogRecPtr;
        st.recovery_stop_name.clear();

        if is_commit {
            ereport(LOG)
                .errmsg(format!(
                    "recovery stopping before commit of transaction {}, time {}",
                    st.recovery_stop_xid,
                    fmt_timestamptz(mcx, st.recovery_stop_time)
                ))
                .finish(loc(2724, "recoveryStopsBefore"))?;
        } else {
            ereport(LOG)
                .errmsg(format!(
                    "recovery stopping before abort of transaction {}, time {}",
                    st.recovery_stop_xid,
                    fmt_timestamptz(mcx, st.recovery_stop_time)
                ))
                .finish(loc(2731, "recoveryStopsBefore"))?;
        }
    }

    Ok(stops_here)
}

/// `static bool recoveryStopsAfter(XLogReaderState *record)`
/// (xlogrecovery.c:2747). Same as `recoveryStopsBefore`, but called after
/// applying the record. We also track the timestamp of the latest applied
/// COMMIT/ABORT record in `XLogRecoveryCtl->recoveryLastXTime`.
pub(crate) fn recovery_stops_after(
    st: &mut XLogRecoveryState,
    mcx: Mcx<'_>,
    _record: RecordRef,
) -> Result<bool, PgError> {
    let mut record_xtime: TimestampTz = 0;

    let r = reader_state();

    // Ignore recovery target settings when not in archive recovery.
    if !st.archive_recovery_requested {
        return Ok(false);
    }

    let info = xlogreader::XLogRecGetInfo(r) & !XLR_INFO_MASK;
    let rmid = xlogreader::XLogRecGetRmid(r);

    // There can be many restore points that share the same name; we stop at the
    // first one.
    if st.recovery_target == RecoveryTargetType::Name
        && rmid == RM_XLOG_ID
        && info == crate::core::XLOG_RESTORE_POINT
    {
        let data = xlogreader::XLogRecGetData(r);
        if let Some(rp) = xl_restore_point::from_bytes(data) {
            // strcmp(recordRestorePointData->rp_name, recoveryTargetName) == 0
            if rp.rp_name() == st.recovery_target_name.as_bytes() {
                st.recovery_stop_after = true;
                st.recovery_stop_xid = InvalidTransactionId;
                st.recovery_stop_lsn = InvalidXLogRecPtr;
                let mut t: TimestampTz = 0;
                let _ = get_record_timestamp(_record, &mut t);
                st.recovery_stop_time = t;
                // strlcpy(recoveryStopName, rp_name, MAXFNAMELEN).
                st.recovery_stop_name =
                    String::from(core::str::from_utf8(rp.rp_name()).unwrap_or(""));

                ereport(LOG)
                    .errmsg(format!(
                        "recovery stopping at restore point \"{}\", time {}",
                        st.recovery_stop_name,
                        fmt_timestamptz(mcx, st.recovery_stop_time)
                    ))
                    .finish(loc(2784, "recoveryStopsAfter"))?;
                return Ok(true);
            }
        }
    }

    // Check if the target LSN has been reached.
    if st.recovery_target == RecoveryTargetType::Lsn
        && st.recovery_target_inclusive
        && r.ReadRecPtr >= st.recovery_target_lsn
    {
        st.recovery_stop_after = true;
        st.recovery_stop_xid = InvalidTransactionId;
        st.recovery_stop_lsn = r.ReadRecPtr;
        st.recovery_stop_time = 0;
        st.recovery_stop_name.clear();
        ereport(LOG)
            .errmsg(format!(
                "recovery stopping after WAL location (LSN) \"{}\"",
                lsn_fmt(st.recovery_stop_lsn)
            ))
            .finish(loc(2802, "recoveryStopsAfter"))?;
        return Ok(true);
    }

    if rmid != RM_XACT_ID {
        return Ok(false);
    }

    let xact_info = info & XLOG_XACT_OPMASK;

    if xact_info == XLOG_XACT_COMMIT
        || xact_info == XLOG_XACT_COMMIT_PREPARED
        || xact_info == XLOG_XACT_ABORT
        || xact_info == XLOG_XACT_ABORT_PREPARED
    {
        // Update the last applied transaction timestamp.
        if get_record_timestamp(_record, &mut record_xtime) {
            crate::shmem::set_latest_xtime(record_xtime);
        }

        // Extract the XID of the committed/aborted transaction.
        let full_info = xlogreader::XLogRecGetInfo(r);
        let record_xid = if xact_info == XLOG_XACT_COMMIT_PREPARED {
            let data = xlogreader::XLogRecGetData(r);
            let parsed = xactdesc::parse_commit_record(full_info, data)?;
            parsed.twophase_xid
        } else if xact_info == XLOG_XACT_ABORT_PREPARED {
            let data = xlogreader::XLogRecGetData(r);
            let parsed = xactdesc::parse_abort_record(full_info, data)?;
            parsed.twophase_xid
        } else {
            xlogreader::XLogRecGetXid(r)
        };

        // There can be only one transaction end record with this exact
        // transactionid; test for equality only.
        if st.recovery_target == RecoveryTargetType::Xid
            && st.recovery_target_inclusive
            && record_xid == st.recovery_target_xid
        {
            st.recovery_stop_after = true;
            st.recovery_stop_xid = record_xid;
            st.recovery_stop_time = record_xtime;
            st.recovery_stop_lsn = InvalidXLogRecPtr;
            st.recovery_stop_name.clear();

            if xact_info == XLOG_XACT_COMMIT || xact_info == XLOG_XACT_COMMIT_PREPARED {
                ereport(LOG)
                    .errmsg(format!(
                        "recovery stopping after commit of transaction {}, time {}",
                        st.recovery_stop_xid,
                        fmt_timestamptz(mcx, st.recovery_stop_time)
                    ))
                    .finish(loc(2869, "recoveryStopsAfter"))?;
            } else if xact_info == XLOG_XACT_ABORT || xact_info == XLOG_XACT_ABORT_PREPARED {
                ereport(LOG)
                    .errmsg(format!(
                        "recovery stopping after abort of transaction {}, time {}",
                        st.recovery_stop_xid,
                        fmt_timestamptz(mcx, st.recovery_stop_time)
                    ))
                    .finish(loc(2877, "recoveryStopsAfter"))?;
            }
            return Ok(true);
        }
    }

    // Check if we should stop as soon as reaching consistency.
    if st.recovery_target == RecoveryTargetType::Immediate && st.reached_consistency {
        ereport(LOG)
            .errmsg("recovery stopping after reaching consistency")
            .finish(loc(2889, "recoveryStopsAfter"))?;

        st.recovery_stop_after = true;
        st.recovery_stop_xid = InvalidTransactionId;
        st.recovery_stop_time = 0;
        st.recovery_stop_lsn = InvalidXLogRecPtr;
        st.recovery_stop_name.clear();
        return Ok(true);
    }

    Ok(false)
}

/// `static char *getRecoveryStopReason(void)` (xlogrecovery.c:2907) — create a
/// comment for the history file to explain why and where the timeline changed.
pub(crate) fn get_recovery_stop_reason(st: &XLogRecoveryState, mcx: Mcx<'_>) -> String {
    let after = if st.recovery_stop_after { "after" } else { "before" };
    match st.recovery_target {
        RecoveryTargetType::Xid => {
            format!("{} transaction {}", after, st.recovery_stop_xid)
        }
        RecoveryTargetType::Time => {
            format!("{} {}\n", after, fmt_timestamptz(mcx, st.recovery_stop_time))
        }
        RecoveryTargetType::Lsn => {
            format!("{} LSN {}\n", after, lsn_fmt(st.recovery_stop_lsn))
        }
        RecoveryTargetType::Name => {
            format!("at restore point \"{}\"", st.recovery_stop_name)
        }
        RecoveryTargetType::Immediate => String::from("reached consistency"),
        RecoveryTargetType::Unset => String::from("no recovery target specified"),
    }
}

/// `static void recoveryPausesHere(bool endOfRecovery)` (xlogrecovery.c) — block
/// here while the recovery pause state is set.
pub(crate) fn recovery_pauses_here(_st: &mut XLogRecoveryState, _end_of_recovery: bool) {
    panic!(
        "blocked: xlogrecovery::stop::recovery_pauses_here — pause loop needs \
         ProcessStartupProcInterrupts + recovery-pause CV timed sleep + CheckForStandbyTrigger \
         (unported startup-proc/promote owners); pending stop-family fill"
    )
}

/// `static bool recoveryApplyDelay(XLogReaderState *record)`
/// (xlogrecovery.c:3004) — honor `recovery_min_apply_delay` for a commit record.
pub(crate) fn recovery_apply_delay(st: &mut XLogRecoveryState, record: RecordRef) -> bool {
    // Nothing to do if no delay configured. (The GUC's boot value is 0, so this
    // is the universal crash-recovery / non-standby case.)
    if crate::gucvars::recovery_min_apply_delay() <= 0 {
        return false;
    }

    // No delay is applied on a database not yet consistent.
    if !st.reached_consistency {
        return false;
    }

    // Nothing to do if crash recovery is requested.
    if !st.archive_recovery_requested {
        return false;
    }

    // Is it a COMMIT record? We deliberately do not delay aborts (no MVCC
    // effect). Read the held reader's current record, mirroring the C
    // `XLogRecGetRmid(record)` / `XLogRecGetInfo(record)` over `xlogreader`.
    let r = reader_state();
    if xlogreader::XLogRecGetRmid(r) != RM_XACT_ID {
        return false;
    }
    let xact_info = xlogreader::XLogRecGetInfo(r) & XLOG_XACT_OPMASK;
    if xact_info != XLOG_XACT_COMMIT && xact_info != XLOG_XACT_COMMIT_PREPARED {
        return false;
    }

    let mut xtime: TimestampTz = 0;
    if !get_record_timestamp(record, &mut xtime) {
        return false;
    }

    // delayUntil = TimestampTzPlusMilliseconds(xtime, recovery_min_apply_delay).
    let delay_until =
        xtime + (crate::gucvars::recovery_min_apply_delay() as TimestampTz) * 1000;

    // Exit without arming the latch if it's already past time to apply this
    // record.
    let now = timestamp_seam::get_current_timestamp::call();
    let msecs = timestamp_seam::timestamp_difference_milliseconds::call(now, delay_until);
    if msecs <= 0 {
        return false;
    }

    // The remaining timed wait loop (ResetLatch / ProcessStartupProcInterrupts /
    // CheckForStandbyTrigger / WaitLatch on XLogRecoveryCtl->recoveryWakeupLatch)
    // is reached only when a positive `recovery_min_apply_delay` is configured on
    // a consistent archive-recovery standby — never on the crash-recovery path.
    // Surface that (unported startup-proc latch-wait) boundary precisely.
    panic!(
        "blocked: xlogrecovery::stop::recovery_apply_delay wait loop (xlogrecovery.c:3049) — \
         the recovery_min_apply_delay timed WaitLatch on recoveryWakeupLatch + \
         ProcessStartupProcInterrupts + CheckForStandbyTrigger require the unported \
         startup-proc latch-wait machinery; reached only with a positive \
         recovery_min_apply_delay on a consistent standby; pending stop-family fill"
    )
}

/// `RecoveryPauseState GetRecoveryPauseState(void)` (xlogrecovery.c:3091) — the
/// startup process's view of the pause state. Reads `XLogRecoveryCtl` under
/// `info_lck` (the shared-region accessor).
pub fn get_recovery_pause_state(_st: &XLogRecoveryState) -> RecoveryPauseState {
    crate::shmem::get_recovery_pause_state()
}

/// `void SetRecoveryPause(bool recoveryPause)` (xlogrecovery.c:3111)
pub fn set_recovery_pause(_st: &mut XLogRecoveryState, recovery_pause: bool) {
    crate::shmem::set_recovery_pause(recovery_pause);
}

/// `static void ConfirmRecoveryPaused(void)` (xlogrecovery.c:3131) — transition
/// `PauseRequested` -> `Paused` once the redo loop notices the request.
pub(crate) fn confirm_recovery_paused(_st: &mut XLogRecoveryState) {
    crate::shmem::confirm_recovery_paused();
}
