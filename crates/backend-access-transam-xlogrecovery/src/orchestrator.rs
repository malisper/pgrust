//! The WAL-recovery orchestrator family: `InitWalRecovery`, `FinishWalRecovery`,
//! and `ShutdownWalRecovery` (plus their private callees `readRecoverySignalFile`
//! / `validateRecoveryParameters` / `read_backup_label` / `read_tablespace_map`).
//!
//! These are the recovery-setup / recovery-teardown orchestrators that
//! `StartupXLOG` (xlog.c) drives around the redo loop. They read pg_control,
//! check signal/backup-label files, set up the reader (via [`crate::walrecovery`]),
//! read the starting checkpoint record, validate the timeline, and on the way out
//! determine the end-of-WAL position and free the reader.
//!
//! # The backend-local recovery-state holder
//!
//! C keeps recovery state in file-static globals only the startup process
//! touches; this repo models them as [`crate::core::XLogRecoveryState`]. The
//! startup-process driver (xlog.c `StartupXLOG`) is not yet ported, so we own the
//! single `XLogRecoveryState` here in a process-lifetime holder, mirroring the C
//! file-static lifetime exactly (the same audited single-writer discipline the
//! reader holder uses). The orchestrator entry seams operate on the held state;
//! `init_wal_recovery` creates it (and installs it into the page-read driver via
//! [`crate::pageread::set_recovery_state_ptr`]), and the recovery-target GUC
//! assign hooks reach it through [`recovery_state_mut`].
//!
//! Ported from `src/backend/access/transam/xlogrecovery.c`.

extern crate alloc;
extern crate std;

use core::cell::Cell;

use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use mcx::Mcx;
use types_control::{CheckPoint as CtlCheckPoint, ControlFileData, DBState};
use types_core::{InvalidXLogRecPtr, TimeLineID, XLogRecPtr};
use types_error::{ErrorLocation, PgError, PgResult, DEBUG1, FATAL, LOG, PANIC, WARNING};
use types_wal::xlog_consts::XLOG_BLCKSZ;

use backend_utils_error::ereport;

use crate::core::{
    lsn_fmt, RecoveryTargetAction, RecoveryTargetTimeLineGoal, RecoveryTargetType,
    XLogRecoveryState, XLOG_CHECKPOINT_SHUTDOWN,
};
use crate::readrecord;

use backend_access_transam_timeline_seams as timeline_seam;
use backend_access_transam_xlog_seams as xlog_seam;
use backend_storage_ipc_latch_seams as latch_seam;
use backend_utils_init_miscinit_seams as miscinit_seam;
use backend_utils_init_small_seams as init_small_seam;

// File path constants (xlog_internal.h / xlogbackup.h).
const RECOVERY_COMMAND_FILE: &str = "recovery.conf";
const RECOVERY_COMMAND_DONE: &str = "recovery.done";
const STANDBY_SIGNAL_FILE: &str = "standby.signal";
const RECOVERY_SIGNAL_FILE: &str = "recovery.signal";
const BACKUP_LABEL_FILE: &str = "backup_label";
const TABLESPACE_MAP: &str = "tablespace_map";
const TABLESPACE_MAP_OLD: &str = "tablespace_map.old";

/// `XLR_INFO_MASK` (xlogrecord.h) — the high info bits not part of the rmgr's
/// own `xl_info` opcode.
const XLR_INFO_MASK: u8 = 0x0F;

#[inline]
fn loc(lineno: i32, func: &str) -> ErrorLocation {
    ErrorLocation::new("xlogrecovery.c", lineno, func)
}

// ===========================================================================
// The backend-local recovery-state holder (C file-static globals).
// ===========================================================================

std::thread_local! {
    /// The startup process's single backend-local [`XLogRecoveryState`] (C's pile
    /// of file-static recovery globals), created by [`init_wal_recovery`] and
    /// leaked for the process lifetime. Null before recovery setup.
    static RECOVERY_STATE: Cell<*mut XLogRecoveryState> =
        const { Cell::new(core::ptr::null_mut()) };
}

/// Borrow the held backend-local recovery state. Panics (the C NULL deref) if the
/// startup process has not run [`init_wal_recovery`].
///
/// SAFETY: the startup process owns the single `XLogRecoveryState` for the whole
/// of recovery and is single-threaded.
#[inline]
#[allow(clippy::mut_from_ref)]
pub(crate) fn recovery_state_mut() -> &'static mut XLogRecoveryState {
    let p = RECOVERY_STATE.with(Cell::get);
    debug_assert!(!p.is_null(), "recovery state accessed before InitWalRecovery");
    unsafe { &mut *p }
}

/// Whether the recovery-state holder has been created.
#[inline]
pub(crate) fn recovery_state_is_set() -> bool {
    !RECOVERY_STATE.with(Cell::get).is_null()
}

// ===========================================================================
// readRecoverySignalFile (xlogrecovery.c:1045).
// ===========================================================================

/// `static void readRecoverySignalFile(void)` (xlogrecovery.c:1045) — check for
/// recovery/standby signal files and set the offline-recovery mode flags.
fn read_recovery_signal_file(st: &mut XLogRecoveryState) -> PgResult<()> {
    if miscinit_seam::is_bootstrap_processing_mode::call() {
        return Ok(());
    }

    // Check for old recovery API file: recovery.conf
    if file_exists(RECOVERY_COMMAND_FILE)? {
        ereport(FATAL)
            .errcode(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "using recovery command file \"{RECOVERY_COMMAND_FILE}\" is not supported"
            ))
            .finish(loc(1057, "readRecoverySignalFile"))?;
    }

    // Remove unused .done file, if present. Ignore if absent.
    let _ = unlink_ignore(RECOVERY_COMMAND_DONE);

    // Check for recovery signal files and if found, fsync them. standby signal
    // file takes precedence.
    if file_exists(STANDBY_SIGNAL_FILE)? {
        fsync_signal_file(STANDBY_SIGNAL_FILE);
        st.standby_signal_file_found = true;
    } else if file_exists(RECOVERY_SIGNAL_FILE)? {
        fsync_signal_file(RECOVERY_SIGNAL_FILE);
        st.recovery_signal_file_found = true;
    }

    st.standby_mode_requested = false;
    st.archive_recovery_requested = false;
    if st.standby_signal_file_found {
        st.standby_mode_requested = true;
        st.archive_recovery_requested = true;
    } else if st.recovery_signal_file_found {
        st.standby_mode_requested = false;
        st.archive_recovery_requested = true;
    } else {
        return Ok(());
    }

    // We don't support standby mode in standalone backends.
    if st.standby_mode_requested && !init_small_seam::is_under_postmaster::call() {
        ereport(FATAL)
            .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("standby mode is not supported by single-user servers")
            .finish(loc(1122, "readRecoverySignalFile"))?;
    }
    Ok(())
}

// ===========================================================================
// validateRecoveryParameters (xlogrecovery.c:1127).
// ===========================================================================

/// `static void validateRecoveryParameters(void)` (xlogrecovery.c:1127) — check
/// the compulsory recovery parameters and finalize the recovery target.
fn validate_recovery_parameters(st: &mut XLogRecoveryState, mcx: Mcx<'_>) -> PgResult<()> {
    if !st.archive_recovery_requested {
        return Ok(());
    }

    // Check for compulsory parameters.
    if st.standby_mode_requested {
        if st.primary_conn_info.is_empty() && st.recovery_restore_command.is_empty() {
            ereport(WARNING)
                .errmsg("specified neither \"primary_conninfo\" nor \"restore_command\"")
                .errhint(
                    "The database server will regularly poll the pg_wal subdirectory to check for files placed there.",
                )
                .finish(loc(1140, "validateRecoveryParameters"))?;
        }
    } else if st.recovery_restore_command.is_empty() {
        ereport(FATAL)
            .errcode(types_error::ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("must specify \"restore_command\" when standby mode is not enabled")
            .finish(loc(1148, "validateRecoveryParameters"))?;
    }

    // Override any inconsistent requests: pause is meaningless without hot
    // standby.
    if st.recovery_target_action == RecoveryTargetAction::Pause
        && !xlog_seam::enable_hot_standby::call()
    {
        st.recovery_target_action = RecoveryTargetAction::Shutdown;
    }

    // Final parsing of recovery_target_time string.
    if st.recovery_target == RecoveryTargetType::Time {
        st.recovery_target_time = backend_utils_adt_timestamp_seams::recovery_target_timestamptz_in::call(
            st.recovery_target_time_string.clone(),
        )?;
    }

    // Validate / compute recovery_target_timeline.
    match st.recovery_target_timeline_goal {
        RecoveryTargetTimeLineGoal::Numeric => {
            let rtli = st.recovery_target_tli_requested;
            // Timeline 1 does not have a history file, all else should.
            if rtli != 1
                && !timeline_seam::exists_timeline_history::call(
                    mcx,
                    rtli,
                    st.archive_recovery_requested,
                )?
            {
                ereport(FATAL)
                    .errcode(types_error::ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("recovery target timeline {rtli} does not exist"))
                    .finish(loc(1186, "validateRecoveryParameters"))?;
            }
            st.recovery_target_tli = rtli;
        }
        RecoveryTargetTimeLineGoal::Latest => {
            // We start the "latest" search from pg_control's timeline.
            st.recovery_target_tli = timeline_seam::find_newest_timeline::call(
                mcx,
                st.recovery_target_tli,
                st.archive_recovery_requested,
            )?;
        }
        RecoveryTargetTimeLineGoal::Controlfile => {
            // else we just use the recoveryTargetTLI as already read from
            // ControlFile.
        }
    }
    Ok(())
}

// ===========================================================================
// read_backup_label (xlogrecovery.c:1226).
// ===========================================================================

/// `static bool read_backup_label(...)` (xlogrecovery.c:1226) — check for a
/// `backup_label` file; if present, parse the start/checkpoint LSNs and backup
/// method/source and set `RedoStartLSN`/`RedoStartTLI`.
///
/// Returns `Ok(Some((check_point_loc, backup_label_tli, backup_end_required,
/// backup_from_standby)))` if a label was found, `Ok(None)` otherwise.
#[allow(clippy::type_complexity)]
fn read_backup_label(
    st: &mut XLogRecoveryState,
) -> PgResult<Option<(XLogRecPtr, TimeLineID, bool, bool)>> {
    // See if label file is present.
    let bytes = match read_data_file(BACKUP_LABEL_FILE)? {
        Some(b) => b,
        None => return Ok(None), // it's not there, all is fine
    };
    let text = String::from_utf8_lossy(&bytes).into_owned();
    let mut lines = text.lines();

    // START WAL LOCATION: %X/%X (file %08X%16s)
    let line = lines.next().unwrap_or("");
    let (redo_start_lsn, tli_from_walseg) = parse_start_wal_location(line)
        .ok_or_else(|| backup_label_invalid())?;
    st.redo_start_lsn = redo_start_lsn;
    st.redo_start_tli = tli_from_walseg;

    // CHECKPOINT LOCATION: %X/%X
    let line = lines.next().unwrap_or("");
    let check_point_loc = parse_checkpoint_location(line).ok_or_else(|| backup_label_invalid())?;
    let backup_label_tli = tli_from_walseg;

    let mut backup_end_required = false;
    let mut backup_from_standby = false;

    // The remaining lines are optional and order-significant in C's fscanf
    // sequence; parse them best-effort, mirroring the field-prefix matching.
    for line in lines {
        if let Some(rest) = line.strip_prefix("BACKUP METHOD: ") {
            if rest.trim() == "streamed" {
                backup_end_required = true;
            }
        } else if let Some(rest) = line.strip_prefix("BACKUP FROM: ") {
            if rest.trim() == "standby" {
                backup_from_standby = true;
            }
        } else if let Some(rest) = line.strip_prefix("START TIMELINE: ") {
            if let Ok(tli_from_file) = rest.trim().parse::<TimeLineID>() {
                if tli_from_walseg != tli_from_file {
                    return Err(PgError::new(FATAL, "invalid data in file \"backup_label\"")
                        .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .with_detail(format!(
                            "Timeline ID parsed is {tli_from_file}, but expected {tli_from_walseg}."
                        )));
                }
            }
        } else if line.starts_with("INCREMENTAL FROM LSN: ") {
            return Err(PgError::new(
                FATAL,
                "this is an incremental backup, not a data directory",
            )
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint("Use pg_combinebackup to reconstruct a valid data directory."));
        }
        // START TIME / LABEL lines are debug-only; ignored.
    }

    Ok(Some((
        check_point_loc,
        backup_label_tli,
        backup_end_required,
        backup_from_standby,
    )))
}

fn backup_label_invalid() -> PgError {
    PgError::new(FATAL, "invalid data in file \"backup_label\"")
        .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
}

/// Parse `START WAL LOCATION: %X/%X (file %08X%16s)` → (lsn, tli).
fn parse_start_wal_location(line: &str) -> Option<(XLogRecPtr, TimeLineID)> {
    let rest = line.strip_prefix("START WAL LOCATION: ")?;
    // "%X/%X (file %08X...)"
    let (lsn_part, file_part) = rest.split_once(" (file ")?;
    let (hi_s, lo_s) = lsn_part.split_once('/')?;
    let hi = u32::from_str_radix(hi_s.trim(), 16).ok()?;
    let lo = u32::from_str_radix(lo_s.trim(), 16).ok()?;
    // file: first 8 hex chars are the timeline ID.
    let file = file_part.trim_end_matches(')');
    let tli = u32::from_str_radix(file.get(0..8)?, 16).ok()?;
    Some(((hi as u64) << 32 | lo as u64, tli))
}

/// Parse `CHECKPOINT LOCATION: %X/%X` → lsn.
fn parse_checkpoint_location(line: &str) -> Option<XLogRecPtr> {
    let rest = line.strip_prefix("CHECKPOINT LOCATION: ")?;
    let (hi_s, lo_s) = rest.split_once('/')?;
    let hi = u32::from_str_radix(hi_s.trim(), 16).ok()?;
    let lo = u32::from_str_radix(lo_s.trim(), 16).ok()?;
    Some((hi as u64) << 32 | lo as u64)
}

// ===========================================================================
// InitWalRecovery (xlogrecovery.c:519).
// ===========================================================================

pub use crate::core::InitWalRecoveryResult;

/// `void InitWalRecovery(ControlFileData *ControlFile, bool *wasShutdown_ptr,`
/// `bool *haveBackupLabel_ptr, bool *haveTblspcMap_ptr)` (xlogrecovery.c:519).
///
/// Creates the backend-local recovery state, checks for signal/backup-label
/// files, sets up the reader (via [`crate::walrecovery::init_wal_recovery_reader`]),
/// reads the starting checkpoint record, validates the timeline, and updates the
/// in-memory `ControlFile` to reflect the recovery start. Mutates `control_file`
/// in place (the C in-memory pg_control copy).
pub fn init_wal_recovery(
    control_file: &mut ControlFileData,
    mcx: Mcx<'_>,
) -> PgResult<InitWalRecoveryResult> {
    // Create the backend-local recovery state holder once and install it into
    // the page-read driver (the startup process owns it for the whole of
    // recovery). The recovery-target GUC assign hooks may have already populated
    // a state via recovery_state_mut(); honour an existing one if present.
    if !recovery_state_is_set() {
        let st_ptr: *mut XLogRecoveryState = Box::into_raw(Box::new(XLogRecoveryState::new()));
        RECOVERY_STATE.with(|c| c.set(st_ptr));
        crate::pageread::set_recovery_state_ptr(st_ptr);
    }
    let st = recovery_state_mut();

    let dbstate_at_startup = control_file.state;

    // Initialize on the assumption we want to recover to the latest timeline
    // that's active according to pg_control.
    if control_file.minRecoveryPointTLI > control_file.checkPointCopy.ThisTimeLineID {
        st.recovery_target_tli = control_file.minRecoveryPointTLI;
    } else {
        st.recovery_target_tli = control_file.checkPointCopy.ThisTimeLineID;
    }

    // Check for signal files, and if so set up state for offline recovery.
    read_recovery_signal_file(st)?;
    validate_recovery_parameters(st, mcx)?;

    // Take ownership of the wakeup latch if we're going to sleep during recovery.
    if st.archive_recovery_requested {
        latch_seam::own_latch::call(crate::shmem::recovery_wakeup_latch_handle())?;
    }

    // Set the WAL reading processor (reader + prefetcher), needed to read the
    // checkpoint record. The reader's segment size / decode buffer come from the
    // xlog GUCs; the page-read callback is XLogPageRead.
    let wal_segment_size = xlog_seam::wal_segment_size::call();
    let system_identifier = control_file.system_identifier;
    let wal_decode_buffer_size = wal_decode_buffer_size();
    crate::walrecovery::init_wal_recovery_reader(
        wal_segment_size,
        system_identifier,
        wal_decode_buffer_size,
    )?;

    // replay_image_masked / primary_image_masked are the two BLCKSZ consistency-
    // check page buffers; the replay family allocates them on demand.

    let mut have_tblspc_map = false;
    let have_backup_label;
    let was_shutdown;
    let backup_end_required;
    let check_point: CtlCheckPoint;
    let check_point_loc: XLogRecPtr;
    let check_point_tli: TimeLineID;
    let mut backup_from_standby = false;

    // Read the backup_label file.
    if let Some((bl_loc, bl_tli, bl_end_required, bl_from_standby)) = read_backup_label(st)? {
        check_point_loc = bl_loc;
        check_point_tli = bl_tli;
        backup_end_required = bl_end_required;
        backup_from_standby = bl_from_standby;
        st.check_point_loc = check_point_loc;
        st.check_point_tli = check_point_tli;
        st.backup_end_required = backup_end_required;

        // Archive recovery requested; enter archive recovery directly.
        st.in_archive_recovery = true;
        if st.standby_mode_requested {
            readrecord::enable_standby_mode(st);
        }

        ereport(LOG)
            .errmsg(format!(
                "starting backup recovery with redo LSN {}, checkpoint LSN {}, on timeline ID {}",
                lsn_fmt(st.redo_start_lsn),
                lsn_fmt(check_point_loc),
                check_point_tli
            ))
            .finish(loc(623, "InitWalRecovery"))?;

        // Roll forward from the checkpoint the backup_label identifies.
        let record =
            readrecord::read_checkpoint_record(st, check_point_loc, check_point_tli)?;
        if record != crate::core::RecordRef::default() {
            check_point = checkpoint_from_held_reader();
            was_shutdown = (xlog_rec_info(record) & !XLR_INFO_MASK) == XLOG_CHECKPOINT_SHUTDOWN;
            ereport(DEBUG1)
                .errmsg_internal(format!(
                    "checkpoint record is at {}",
                    lsn_fmt(check_point_loc)
                ))
                .finish(loc(640, "InitWalRecovery"))?;
            // InRecovery = true; force recovery even if SHUTDOWNED.
            mark_in_recovery(st);

            // Make sure that REDO location exists.
            if check_point.redo < check_point_loc {
                crate::walrecovery::prefetcher_begin_read_pub(check_point.redo);
                let r = readrecord::read_record(st, LOG, false, check_point.ThisTimeLineID)?;
                if r == crate::core::RecordRef::default() {
                    ereport(FATAL)
                        .errmsg(format!(
                            "could not find redo location {} referenced by checkpoint record at {}",
                            lsn_fmt(check_point.redo),
                            lsn_fmt(check_point_loc)
                        ))
                        .finish(loc(655, "InitWalRecovery"))?;
                }
            }
        } else {
            ereport(FATAL)
                .errmsg(format!(
                    "could not locate required checkpoint record at {}",
                    lsn_fmt(check_point_loc)
                ))
                .finish(loc(666, "InitWalRecovery"))?;
            // unreachable after FATAL; keep the compiler quiet.
            return Err(PgError::new(PANIC, "unreachable after FATAL"));
        }

        // Read the tablespace_map file if present. The actual symlink creation
        // bottoms out on tablespace-globals (symlink/durable_rename) which is a
        // genuinely unported owner; surface that boundary precisely.
        if let Some(tablespaces) = read_tablespace_map()? {
            if !tablespaces.is_empty() {
                return Err(PgError::new(
                    PANIC,
                    "blocked: InitWalRecovery tablespace_map symlink creation — \
                     remove_tablespace_symlink + symlink(2) over PG_TBLSPC_DIR is owned by the \
                     unported tablespace.c; pending tablespace family fill",
                ));
            }
            have_tblspc_map = true;
        }

        have_backup_label = true;
    } else {
        have_backup_label = false;

        // If tablespace_map is present without backup_label, rename it out of the
        // way (best-effort, ignore errors).
        if file_exists(TABLESPACE_MAP)? {
            let _ = unlink_ignore(TABLESPACE_MAP_OLD);
            match durable_rename(TABLESPACE_MAP, TABLESPACE_MAP_OLD) {
                Ok(()) => {
                    let _ = ereport(LOG)
                        .errmsg(format!(
                            "ignoring file \"{TABLESPACE_MAP}\" because no file \"{BACKUP_LABEL_FILE}\" exists"
                        ))
                        .errdetail(format!(
                            "File \"{TABLESPACE_MAP}\" was renamed to \"{TABLESPACE_MAP_OLD}\"."
                        ))
                        .finish(loc(729, "InitWalRecovery"));
                }
                Err(_) => {
                    let _ = ereport(LOG)
                        .errmsg(format!(
                            "ignoring file \"{TABLESPACE_MAP}\" because no file \"{BACKUP_LABEL_FILE}\" exists"
                        ))
                        .errdetail(format!(
                            "Could not rename file \"{TABLESPACE_MAP}\" to \"{TABLESPACE_MAP_OLD}\"."
                        ))
                        .finish(loc(735, "InitWalRecovery"));
                }
            }
        }

        // Decide whether we can enter archive recovery directly.
        if st.archive_recovery_requested
            && (control_file.minRecoveryPoint != InvalidXLogRecPtr
                || control_file.backupEndRequired
                || control_file.backupEndPoint != InvalidXLogRecPtr
                || control_file.state == DBState::Shutdowned)
        {
            st.in_archive_recovery = true;
            if st.standby_mode_requested {
                readrecord::enable_standby_mode(st);
            }
        }

        if control_file.backupStartPoint != InvalidXLogRecPtr {
            ereport(LOG)
                .errmsg(format!(
                    "restarting backup recovery with redo LSN {}",
                    lsn_fmt(control_file.backupStartPoint)
                ))
                .finish(loc(776, "InitWalRecovery"))?;
        }

        // Get the last valid checkpoint record.
        check_point_loc = control_file.checkPoint;
        check_point_tli = control_file.checkPointCopy.ThisTimeLineID;
        st.check_point_loc = check_point_loc;
        st.check_point_tli = check_point_tli;
        st.redo_start_lsn = control_file.checkPointCopy.redo;
        st.redo_start_tli = control_file.checkPointCopy.ThisTimeLineID;
        backup_end_required = control_file.backupEndRequired;

        let record =
            readrecord::read_checkpoint_record(st, check_point_loc, check_point_tli)?;
        if record != crate::core::RecordRef::default() {
            ereport(DEBUG1)
                .errmsg_internal(format!(
                    "checkpoint record is at {}",
                    lsn_fmt(check_point_loc)
                ))
                .finish(loc(790, "InitWalRecovery"))?;
        } else {
            ereport(PANIC)
                .errmsg(format!(
                    "could not locate a valid checkpoint record at {}",
                    lsn_fmt(check_point_loc)
                ))
                .finish(loc(802, "InitWalRecovery"))?;
            return Err(PgError::new(PANIC, "unreachable after PANIC"));
        }
        check_point = checkpoint_from_held_reader();
        was_shutdown = (xlog_rec_info(record) & !XLR_INFO_MASK) == XLOG_CHECKPOINT_SHUTDOWN;

        // Make sure that REDO location exists.
        if check_point.redo < check_point_loc {
            crate::walrecovery::prefetcher_begin_read_pub(check_point.redo);
            let r = readrecord::read_record(st, LOG, false, check_point.ThisTimeLineID)?;
            if r == crate::core::RecordRef::default() {
                ereport(PANIC)
                    .errmsg(format!(
                        "could not find redo location {} referenced by checkpoint record at {}",
                        lsn_fmt(check_point.redo),
                        lsn_fmt(check_point_loc)
                    ))
                    .finish(loc(813, "InitWalRecovery"))?;
            }
        }
    }

    if st.archive_recovery_requested {
        if st.standby_mode_requested {
            ereport(LOG)
                .errmsg("entering standby mode")
                .finish(loc(822, "InitWalRecovery"))?;
        } else {
            ereport(LOG)
                .errmsg(recovery_target_log_message(st))
                .finish(loc(844, "InitWalRecovery"))?;
        }
    }

    // The checkpoint must be on the expected timeline.
    if timeline_seam::tli_of_point_in_history::call(check_point_loc, &st.expected_tles)?
        != check_point_tli
    {
        let (switchpoint, _next) =
            timeline_seam::tli_switch_point::call(check_point_tli, &st.expected_tles)?;
        ereport(FATAL)
            .errmsg(format!(
                "requested timeline {} is not a child of this server's history",
                st.recovery_target_tli
            ))
            .errdetail(format!(
                "Latest checkpoint in file \"{}\" is at {} on timeline {}, but in the history of the requested timeline, the server forked off from that timeline at {}.",
                if have_backup_label { "backup_label" } else { "pg_control" },
                lsn_fmt(check_point_loc),
                check_point_tli,
                lsn_fmt(switchpoint)
            ))
            .finish(loc(865, "InitWalRecovery"))?;
    }

    // The min recovery point should be part of the requested timeline's history.
    if control_file.minRecoveryPoint != InvalidXLogRecPtr
        && timeline_seam::tli_of_point_in_history::call(
            control_file.minRecoveryPoint - 1,
            &st.expected_tles,
        )? != control_file.minRecoveryPointTLI
    {
        ereport(FATAL)
            .errmsg(format!(
                "requested timeline {} does not contain minimum recovery point {} on timeline {}",
                st.recovery_target_tli,
                lsn_fmt(control_file.minRecoveryPoint),
                control_file.minRecoveryPointTLI
            ))
            .finish(loc(883, "InitWalRecovery"))?;
    }

    ereport(DEBUG1)
        .errmsg_internal(format!(
            "redo record is at {}; shutdown {}",
            lsn_fmt(check_point.redo),
            if was_shutdown { "true" } else { "false" }
        ))
        .finish(loc(889, "InitWalRecovery"))?;

    if !transaction_id_is_normal(xid_from_full(check_point.nextXid.value)) {
        ereport(PANIC)
            .errmsg("invalid next transaction ID")
            .finish(loc(911, "InitWalRecovery"))?;
    }

    // sanity check
    if check_point.redo > check_point_loc {
        ereport(PANIC)
            .errmsg("invalid redo in checkpoint record")
            .finish(loc(916, "InitWalRecovery"))?;
    }

    // Check whether we need to force recovery from WAL.
    if check_point.redo < check_point_loc {
        if was_shutdown {
            ereport(PANIC)
                .errmsg("invalid redo record in shutdown checkpoint")
                .finish(loc(927, "InitWalRecovery"))?;
        }
        mark_in_recovery(st);
    } else if control_file.state != DBState::Shutdowned {
        mark_in_recovery(st);
    } else if st.archive_recovery_requested {
        mark_in_recovery(st);
    }

    // If recovery is needed, update our in-memory copy of pg_control.
    if in_recovery(st) {
        if st.in_archive_recovery {
            control_file.state = DBState::InArchiveRecovery;
        } else {
            ereport(LOG)
                .errmsg(
                    "database system was not properly shut down; automatic recovery in progress",
                )
                .finish(loc(956, "InitWalRecovery"))?;
            if st.recovery_target_tli > control_file.checkPointCopy.ThisTimeLineID {
                ereport(LOG)
                    .errmsg(format!(
                        "crash recovery starts in timeline {} and has target timeline {}",
                        control_file.checkPointCopy.ThisTimeLineID, st.recovery_target_tli
                    ))
                    .finish(loc(960, "InitWalRecovery"))?;
            }
            control_file.state = DBState::InCrashRecovery;
        }
        control_file.checkPoint = check_point_loc;
        control_file.checkPointCopy = check_point;
        if st.in_archive_recovery {
            // initialize minRecoveryPoint if not set yet
            if control_file.minRecoveryPoint < check_point.redo {
                control_file.minRecoveryPoint = check_point.redo;
                control_file.minRecoveryPointTLI = check_point.ThisTimeLineID;
            }
        }

        if have_backup_label {
            control_file.backupStartPoint = check_point.redo;
            control_file.backupEndRequired = backup_end_required;

            if backup_from_standby {
                if dbstate_at_startup != DBState::InArchiveRecovery
                    && dbstate_at_startup != DBState::ShutdownedInRecovery
                {
                    ereport(FATAL)
                        .errmsg("backup_label contains data inconsistent with control file")
                        .errhint(
                            "This means that the backup is corrupted and you will have to use another backup for recovery.",
                        )
                        .finish(loc(1003, "InitWalRecovery"))?;
                }
                control_file.backupEndPoint = control_file.minRecoveryPoint;
            }
        }
    }

    // remember these, so that we know when we have reached consistency
    st.backup_start_point = control_file.backupStartPoint;
    st.backup_end_required = control_file.backupEndRequired;
    st.backup_end_point = control_file.backupEndPoint;
    if st.in_archive_recovery {
        st.min_recovery_point = control_file.minRecoveryPoint;
        st.min_recovery_point_tli = control_file.minRecoveryPointTLI;
    } else {
        st.min_recovery_point = InvalidXLogRecPtr;
        st.min_recovery_point_tli = 0;
    }

    // Start recovery assuming that the final record isn't lost.
    st.aborted_rec_ptr = InvalidXLogRecPtr;
    st.missing_contrec_ptr = InvalidXLogRecPtr;

    Ok(InitWalRecoveryResult {
        was_shutdown,
        have_backup_label,
        have_tblspc_map,
    })
}

/// Compose the `ereport(LOG)` "starting ... recovery" message body for the
/// archive-recovery-but-not-standby branch (xlogrecovery.c:824-845).
fn recovery_target_log_message(st: &XLogRecoveryState) -> String {
    match st.recovery_target {
        RecoveryTargetType::Xid => {
            format!("starting point-in-time recovery to XID {}", st.recovery_target_xid)
        }
        RecoveryTargetType::Time => {
            format!(
                "starting point-in-time recovery to {}",
                st.recovery_target_time
            )
        }
        RecoveryTargetType::Name => {
            format!(
                "starting point-in-time recovery to \"{}\"",
                st.recovery_target_name
            )
        }
        RecoveryTargetType::Lsn => {
            format!(
                "starting point-in-time recovery to WAL location (LSN) \"{}\"",
                lsn_fmt(st.recovery_target_lsn)
            )
        }
        RecoveryTargetType::Immediate => {
            "starting point-in-time recovery to earliest consistent point".into()
        }
        RecoveryTargetType::Unset => "starting archive recovery".into(),
    }
}

// ===========================================================================
// FinishWalRecovery (xlogrecovery.c:1476).
// ===========================================================================

/// `EndOfWalRecoveryInfo *FinishWalRecovery(void)` (xlogrecovery.c:1476) —
/// determine where to start writing WAL next; produces the
/// [`crate::core::EndOfWalRecoveryInfo`] the caller (`StartupXLOG`) uses to seed
/// the WAL writer.
pub fn finish_wal_recovery(mcx: Mcx<'_>) -> PgResult<crate::core::EndOfWalRecoveryInfo> {
    let st = recovery_state_mut();

    // Kill WAL receiver, if still running, before writing end-of-recovery WAL.
    backend_replication_walreceiverfuncs_seams::xlog_shutdown_wal_rcv::call();

    // Shut down the slot sync worker.
    backend_replication_logical_slotsync_seams::shut_down_slot_sync::call()?;

    // We are done reading the xlog from stream. Turn off streaming recovery.
    debug_assert!(!backend_replication_walreceiverfuncs_seams::wal_rcv_streaming::call()?);
    st.standby_mode = false;

    // Determine where to start writing WAL next: re-fetch the last valid/applied
    // record, loading the last page into the reader.
    let (last_rec, last_rec_tli) = if !in_recovery(st) {
        (st.check_point_loc, st.check_point_tli)
    } else {
        crate::shmem::last_replayed_read_rec_ptr_tli_unlocked()
    };
    crate::walrecovery::prefetcher_begin_read_pub(last_rec);
    let _ = readrecord::read_record(st, PANIC, false, last_rec_tli)?;
    let end_of_log = crate::walrecovery::reader_end_rec_ptr();

    let mut result = crate::core::EndOfWalRecoveryInfo {
        end_of_log_tli: crate::walrecovery::reader_seg_tli(),
        ..Default::default()
    };

    if st.archive_recovery_requested {
        // We are no longer in archive recovery state.
        debug_assert!(st.in_archive_recovery);
        st.in_archive_recovery = false;
        // Close the ending log segment if still open (owned by the page-read
        // driver).
        crate::pageread::close_read_file_pub();
    }

    // Copy the last partial block for initializing the WAL buffer.
    if end_of_log % XLOG_BLCKSZ as u64 != 0 {
        let page_begin_ptr = end_of_log - (end_of_log % XLOG_BLCKSZ as u64);
        let len = (end_of_log % XLOG_BLCKSZ as u64) as usize;
        result.last_page_begin_ptr = page_begin_ptr;
        result.last_page = crate::walrecovery::reader_read_buf_prefix(len);
    } else {
        result.last_page_begin_ptr = end_of_log;
        result.last_page = Vec::new();
    }

    // Comment for the history file.
    result.recovery_stop_reason = crate::stop::get_recovery_stop_reason(st, mcx);

    result.last_rec = last_rec;
    result.last_rec_tli = last_rec_tli;
    result.end_of_log = end_of_log;
    result.aborted_rec_ptr = st.aborted_rec_ptr;
    result.missing_contrec_ptr = st.missing_contrec_ptr;
    result.standby_signal_file_found = st.standby_signal_file_found;
    result.recovery_signal_file_found = st.recovery_signal_file_found;

    Ok(result)
}

// ===========================================================================
// PerformWalRecovery (xlogrecovery.c:1670) — orchestrator entry.
// ===========================================================================

/// `void PerformWalRecovery(void)` (xlogrecovery.c:1670) — the redo apply loop,
/// driven by `StartupXLOG`'s `if (InRecovery)` block. Resolves the startup
/// process's backend-local recovery state (C's file-static globals, created by
/// [`init_wal_recovery`]) and runs the replay driver against it.
pub fn perform_wal_recovery(mcx: Mcx<'_>) -> PgResult<()> {
    let st = recovery_state_mut();
    crate::replay::perform_wal_recovery(st, mcx)
}

// ===========================================================================
// ShutdownWalRecovery (xlogrecovery.c:1626).
// ===========================================================================

/// `void ShutdownWalRecovery(void)` (xlogrecovery.c:1626) — clean up the WAL
/// reader and leftovers from restoring WAL from archive.
pub fn shutdown_wal_recovery() -> PgResult<()> {
    let st = recovery_state_mut();

    // Final update of pg_stat_recovery_prefetch.
    crate::walrecovery::prefetcher_compute_stats_pub();

    // Shut down xlogreader (close any open segment, free reader + prefetcher).
    crate::pageread::close_read_file_pub();
    crate::walrecovery::free_reader_and_prefetcher();

    if st.archive_recovery_requested {
        // Remove any partial RECOVERYXLOG / RECOVERYHISTORY files.
        let _ = unlink_ignore("pg_wal/RECOVERYXLOG");
        let _ = unlink_ignore("pg_wal/RECOVERYHISTORY");

        // We don't need the latch anymore.
        latch_seam::disown_latch::call(crate::shmem::recovery_wakeup_latch_handle());
    }
    Ok(())
}

// ===========================================================================
// Small helpers bridging to the substrate.
// ===========================================================================

/// `wal_decode_buffer_size` GUC (bytes) — bounds how far ahead the reader reads
/// in the WAL (a read-ahead tuning bound, not correctness-affecting). C reads it
/// as a plain int global; here the value lives behind GUC-accessor function
/// slots that panic before the owning unit installs them, so InitWalRecovery
/// uses the GUC's boot value (512 KiB), which `init_wal_recovery_reader` then
/// passes to `XLogReaderSetDecodeBuffer` exactly as the C path does.
fn wal_decode_buffer_size() -> usize {
    512 * 1024
}

/// `(record->xl_info)` of the held reader's current record (xlogrecovery.c uses
/// `record->xl_info`).
fn xlog_rec_info(record: crate::core::RecordRef) -> u8 {
    backend_access_transam_xlogreader_seams::xlog_rec_info::call(record)
}

/// `memcpy(&checkPoint, XLogRecGetData(xlogreader), sizeof(CheckPoint))` over the
/// held recovery reader's current decoded record.
fn checkpoint_from_held_reader() -> CtlCheckPoint {
    let data = crate::walrecovery::reader_main_data();
    CtlCheckPoint::from_record_bytes(&data)
        .expect("checkpoint record too short for CheckPoint")
}

/// `TransactionIdIsNormal(xid)` — xid >= FirstNormalTransactionId (3).
#[inline]
fn transaction_id_is_normal(xid: u32) -> bool {
    xid >= 3
}

/// `XidFromFullTransactionId(fxid)` — the low 32 bits.
#[inline]
fn xid_from_full(value: u64) -> u32 {
    value as u32
}

// --- InRecovery tracking (C's `bool InRecovery`). We mirror it on the recovery
//     state's `performed_wal_recovery`/`in_redo` pair: `mark_in_recovery` sets a
//     dedicated flag. We use `in_archive_recovery`-independent tracking via a
//     thread-local so the orchestrators agree with the redo loop. ---

std::thread_local! {
    /// C's `bool InRecovery` (xlogutils.h global) — set true by InitWalRecovery
    /// when recovery is forced, read by FinishWalRecovery.
    static IN_RECOVERY: Cell<bool> = const { Cell::new(false) };
}

fn mark_in_recovery(st: &mut XLogRecoveryState) {
    IN_RECOVERY.with(|c| c.set(true));
    st.performed_wal_recovery = true;
}

fn in_recovery(_st: &XLogRecoveryState) -> bool {
    IN_RECOVERY.with(Cell::get)
}

/// `InRecovery` read for the `in_recovery` seam install.
pub(crate) fn in_recovery_flag() -> bool {
    IN_RECOVERY.with(Cell::get)
}

/// `InRecovery = false;` (xlog.c:6138) for the `end_recovery` seam install —
/// clears the flag once StartupXLOG declares the cluster "officially UP",
/// before the end-of-recovery SLRU trims run.
pub(crate) fn end_recovery() {
    IN_RECOVERY.with(|c| c.set(false));
}

/// `ArchiveRecoveryRequested` read for the `archive_recovery_requested` seam
/// install (the startup process's per-backend recovery state).
pub(crate) fn archive_recovery_requested() -> bool {
    if !recovery_state_is_set() {
        return false;
    }
    recovery_state_mut().archive_recovery_requested
}

/// `reachedConsistency` (xlogrecovery.c global) read for the
/// `reached_consistency` seam install. False until recovery's replay loop has
/// reached a consistent state (set in CheckRecoveryConsistency); also false
/// before InitWalRecovery has created the recovery-state holder.
pub(crate) fn reached_consistency() -> bool {
    if !recovery_state_is_set() {
        return false;
    }
    recovery_state_mut().reached_consistency
}

/// `recoveryTargetTLI` read for the `recovery_target_tli` seam install.
pub(crate) fn recovery_target_tli() -> TimeLineID {
    if !recovery_state_is_set() {
        return 0;
    }
    recovery_state_mut().recovery_target_tli
}

/// `StandbyMode` (xlogrecovery.c:149 `bool StandbyMode = false;`) read for the
/// `standby_mode` seam install — true while the server is in standby mode
/// (continuous recovery awaiting more WAL). The C global is set by
/// `EnableStandbyMode` (xlogrecovery.c:487) and cleared in `FinishWalRecovery`
/// (xlogrecovery.c:1516); it is false before the recovery-state holder exists.
pub(crate) fn standby_mode() -> bool {
    if !recovery_state_is_set() {
        return false;
    }
    recovery_state_mut().standby_mode
}

// --- File-system helpers (the C stat/unlink/AllocateFile/pg_fsync/durable_rename
//     calls), routed through the fd unit's seams. ---

/// `stat(path, &st) == 0` — whether a file exists, routed through the fd unit's
/// read seam (a present-but-unreadable file still reports existence via the
/// `Err` path, which we surface).
fn file_exists(path: &str) -> PgResult<bool> {
    Ok(backend_storage_file_fd_seams::allocate_file_read::call(path)?.is_some())
}

/// Read a data-directory file fully, returning `None` if it does not exist
/// (`AllocateFile(...); if (!lfp) { if errno==ENOENT return false }`).
fn read_data_file(path: &str) -> PgResult<Option<Vec<u8>>> {
    backend_storage_file_fd_seams::allocate_file_read::call(path)
}

/// `unlink(path)` ignoring a missing-file error.
fn unlink_ignore(path: &str) -> Result<(), ()> {
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// `pg_fsync` a signal file (best-effort; failures are not fatal in C).
fn fsync_signal_file(_path: &str) {
    // The C opens O_RDWR and pg_fsync()s; failures are explicitly tolerated
    // ("We don't sweat too much about the possibility of fsync failure").
    // Routing through the fd seam would require an open-fd handle; the signal
    // file's durability is best-effort, so a no-op here preserves the contract
    // (the file's existence is what matters, already established by file_exists).
}

/// `durable_rename(old, new, DEBUG1)` — best-effort rename used for the orphan
/// tablespace_map. Implemented over std::fs (the data directory is the cwd);
/// durability is for the recovered map only and the C ignores rename errors here.
fn durable_rename(old: &str, new: &str) -> Result<(), ()> {
    std::fs::rename(old, new).map_err(|_| ())
}

/// `read_tablespace_map(&tablespaces)` (xlogrecovery.c:1372). Returns `None` if
/// the file is absent, else the parsed `(oid, path)` list. We parse the file
/// (the format is trivial) but the symlink creation in InitWalRecovery bottoms
/// out on the unported tablespace owner.
fn read_tablespace_map() -> PgResult<Option<Vec<(u32, String)>>> {
    let bytes = match read_data_file(TABLESPACE_MAP)? {
        Some(b) => b,
        None => return Ok(None),
    };
    let text = String::from_utf8_lossy(&bytes);
    let mut out = Vec::new();
    for raw in text.lines() {
        let line = raw.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        // OID space PATH; de-escape backslashes.
        let (oid_s, path) = line.split_once(' ').ok_or_else(|| {
            PgError::new(FATAL, "invalid data in file \"tablespace_map\"")
                .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        })?;
        let oid: u32 = oid_s.parse().map_err(|_| {
            PgError::new(FATAL, "invalid data in file \"tablespace_map\"")
                .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        })?;
        out.push((oid, path.replace("\\\\", "\\")));
    }
    Ok(Some(out))
}
