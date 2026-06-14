//! Shared core for the `xlogrecovery.c` port: the recovery enums, the
//! recovery-target / mode / cursor / consistency state, the seam carrier types,
//! and the backend-local [`XLogRecoveryState`] that holds C's pile of
//! file-static globals.
//!
//! # Two state models
//!
//! C keeps recovery state in two places: ~50 file-static / extern globals that
//! only the startup process touches, **and** the shared-memory
//! `XLogRecoveryCtlData` (guarded by `info_lck` so hot-standby backends can read
//! the replay LSNs / pause state). This repo splits them faithfully:
//!
//! * the shared region is [`crate::shmem::XLogRecoveryShared`] (the F0
//!   recovery-shmem keystone — a real `#[repr(C)]` shmem struct);
//! * the backend-local file-statics are [`XLogRecoveryState`] here, threaded as
//!   `&mut XLogRecoveryState` through every recovery entry point (the startup
//!   process is the sole writer during replay).
//!
//! Ported 1:1 from `src/backend/access/transam/xlogrecovery.c` (field set,
//! initial values, the recovery enums from `access/xlogrecovery.h`).

use alloc::string::String;
use alloc::vec::Vec;

use types_core::{TimeLineID, TimestampTz, TransactionId, XLogRecPtr};
use types_core::{InvalidTransactionId, InvalidXLogRecPtr};
use types_wal::xlog_consts::MAXFNAMELEN;

/// `RecoveryPauseState` (access/xlogrecovery.h) — re-exported from the canonical
/// home in `types_wal` so the backend-local entry points and the shared region
/// name the same type.
pub use types_wal::wal::RecoveryPauseState;

// ===========================================================================
// Seam carrier types (the recovery-driver seam vocabulary). These travel
// across the prefetcher / page-read seams the family-fills install into the
// not-yet-ported page-read owner; they are defined in `types-wal` so the owner
// seam crates can name them without a dependency cycle, and re-exported here so
// the recovery crate's API and the seam decls share one vocabulary.
// ===========================================================================

pub use types_wal::xlogrecovery_carriers::{
    DecodedBlockTag, ReadRecordResult, RecordRef, XLogPageReadResult, XLogSource,
};

/// Human-readable names for [`XLogSource`], for debugging output.
/// (`xlogSourceNames[]`, xlogrecovery.c:220)
pub const XLOG_SOURCE_NAMES: [&str; 4] = ["any", "archive", "pg_wal", "stream"];

/// `xlogSourceNames[source]`.
#[inline]
pub fn xlog_source_name(source: XLogSource) -> &'static str {
    XLOG_SOURCE_NAMES[source as usize]
}

// ===========================================================================
// Recovery enums (access/xlogrecovery.h).
// ===========================================================================

/// Recovery target type. Only set during a Point-in-Time recovery, not when in
/// standby mode. (`RecoveryTargetType`, xlogrecovery.h:23)
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum RecoveryTargetType {
    #[default]
    Unset,
    Xid,
    Time,
    Name,
    Lsn,
    Immediate,
}

/// Recovery target TimeLine goal. (`RecoveryTargetTimeLineGoal`,
/// xlogrecovery.h:36)
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum RecoveryTargetTimeLineGoal {
    Controlfile,
    #[default]
    Latest,
    Numeric,
}

/// `RECOVERY_TARGET_ACTION_*` (xlog_internal.h): what to do once the recovery
/// target is reached.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum RecoveryTargetAction {
    #[default]
    Pause = 0,
    Promote = 1,
    Shutdown = 2,
}

/// What [`crate::shmem`]'s `finish_wal_recovery` will return: where recovery
/// ended, and why. (`EndOfWalRecoveryInfo`, xlogrecovery.h:91) — owned form.
#[derive(Clone, Debug, Default)]
pub struct EndOfWalRecoveryInfo {
    /// start of last valid or applied record
    pub last_rec: XLogRecPtr,
    pub last_rec_tli: TimeLineID,
    /// end of last valid or applied record
    pub end_of_log: XLogRecPtr,
    pub end_of_log_tli: TimeLineID,
    /// LSN of the page that contains `end_of_log`
    pub last_page_begin_ptr: XLogRecPtr,
    /// copy of the last page, up to `end_of_log` (empty if page-aligned)
    pub last_page: Vec<u8>,
    /// start pointer of a broken record at end of WAL when recovery completes
    pub aborted_rec_ptr: XLogRecPtr,
    /// location of the first contrecord that went missing
    pub missing_contrec_ptr: XLogRecPtr,
    /// short human-readable string describing why recovery ended
    pub recovery_stop_reason: String,
    /// standby.signal file was found
    pub standby_signal_file_found: bool,
    /// recovery.signal file was found
    pub recovery_signal_file_found: bool,
}

// ===========================================================================
// pg_control info opcodes + signal constants used by the recovery families.
// ===========================================================================

/// Unsupported old recovery command file name, relative to `$PGDATA`.
pub const RECOVERY_COMMAND_FILE: &str = "recovery.conf";

/// `XLOG_RESTORE_POINT` info opcode (catalog/pg_control.h, value 0x70).
pub const XLOG_RESTORE_POINT: u8 = 0x70;
/// `XLOG_CHECKPOINT_SHUTDOWN` (catalog/pg_control.h:68).
pub const XLOG_CHECKPOINT_SHUTDOWN: u8 = 0x00;
/// `XLOG_CHECKPOINT_ONLINE` (catalog/pg_control.h).
pub const XLOG_CHECKPOINT_ONLINE: u8 = 0x10;
/// `XLOG_BACKUP_END` (catalog/pg_control.h:73).
pub const XLOG_BACKUP_END: u8 = 0x50;
/// `XLOG_END_OF_RECOVERY` (catalog/pg_control.h:77).
pub const XLOG_END_OF_RECOVERY: u8 = 0x90;
/// `XLOG_OVERWRITE_CONTRECORD` (catalog/pg_control.h:81).
pub const XLOG_OVERWRITE_CONTRECORD: u8 = 0xD0;
/// `XLOG_CHECKPOINT_REDO` (catalog/pg_control.h:82).
pub const XLOG_CHECKPOINT_REDO: u8 = 0xE0;

/// `PMSIGNAL_RECOVERY_STARTED` (storage/pmsignal.h).
pub const PMSIGNAL_RECOVERY_STARTED: i32 = 0;
/// `PMSIGNAL_BEGIN_HOT_STANDBY` (storage/pmsignal.h).
pub const PMSIGNAL_BEGIN_HOT_STANDBY: i32 = 2;
/// `PMSIGNAL_RECOVERY_CONSISTENT` — sent when consistency is first reached.
pub const PMSIGNAL_RECOVERY_CONSISTENT: i32 = 1;

/// `HotStandbyState` (access/xlogutils.h): STANDBY_DISABLED .. SNAPSHOT_READY.
pub const STANDBY_DISABLED: i32 = 0;
pub const STANDBY_INITIALIZED: i32 = 1;
pub const STANDBY_SNAPSHOT_PENDING: i32 = 2;
pub const STANDBY_SNAPSHOT_READY: i32 = 3;

// ===========================================================================
// Backend-local recovery state (C file-static globals; xlogrecovery.c:84-390).
// The XLogRecoveryCtlData *shared* fields live in `crate::shmem`; the few
// fields C duplicates locally (the SharedHotStandbyActive read-back etc.) are
// kept here for the startup process's own use, 1:1 with the C file-statics.
// ===========================================================================

/// The Rust home for xlogrecovery.c's file-static / extern globals. The startup
/// process creates this once (via [`XLogRecoveryState::new`]) and threads
/// `&mut XLogRecoveryState` through every recovery entry point.
#[derive(Debug)]
pub struct XLogRecoveryState {
    // -- options formerly taken from recovery.conf (xlogrecovery.c:84-95) --
    /// `recoveryRestoreCommand`.
    pub recovery_restore_command: String,
    /// `recoveryEndCommand`.
    pub recovery_end_command: String,
    /// `archiveCleanupCommand`.
    pub archive_cleanup_command: String,
    /// `recoveryTarget`.
    pub recovery_target: RecoveryTargetType,
    /// `recoveryTargetInclusive`.
    pub recovery_target_inclusive: bool,
    /// `recoveryTargetAction`.
    pub recovery_target_action: RecoveryTargetAction,
    /// `recoveryTargetXid`.
    pub recovery_target_xid: TransactionId,
    /// `recovery_target_time_string`.
    pub recovery_target_time_string: String,
    /// `recoveryTargetTime`.
    pub recovery_target_time: TimestampTz,
    /// `recoveryTargetName`.
    pub recovery_target_name: String,
    /// `recoveryTargetLSN`.
    pub recovery_target_lsn: XLogRecPtr,
    /// `recovery_min_apply_delay` (milliseconds).
    pub recovery_min_apply_delay: i32,

    // -- streaming options (xlogrecovery.c:98-100) --
    /// `PrimaryConnInfo`.
    pub primary_conn_info: String,
    /// `PrimarySlotName`.
    pub primary_slot_name: String,
    /// `wal_receiver_create_temp_slot`.
    pub wal_receiver_create_temp_slot: bool,

    // -- timeline target (xlogrecovery.c:122-126) --
    /// `recoveryTargetTimeLineGoal`.
    pub recovery_target_timeline_goal: RecoveryTargetTimeLineGoal,
    /// `recoveryTargetTLIRequested`.
    pub recovery_target_tli_requested: TimeLineID,
    /// `recoveryTargetTLI`.
    pub recovery_target_tli: TimeLineID,
    /// `curFileTLI`.
    pub cur_file_tli: TimeLineID,

    // -- mode flags (xlogrecovery.c:139-153) --
    /// `ArchiveRecoveryRequested`.
    pub archive_recovery_requested: bool,
    /// `InArchiveRecovery`.
    pub in_archive_recovery: bool,
    /// `StandbyModeRequested`.
    pub standby_mode_requested: bool,
    /// `StandbyMode`.
    pub standby_mode: bool,
    /// standby.signal file was present at startup.
    pub standby_signal_file_found: bool,
    /// recovery.signal file was present at startup.
    pub recovery_signal_file_found: bool,

    // -- checkpoint start state (xlogrecovery.c:169-172) --
    /// `CheckPointLoc`.
    pub check_point_loc: XLogRecPtr,
    /// `CheckPointTLI`.
    pub check_point_tli: TimeLineID,
    /// `RedoStartLSN`.
    pub redo_start_lsn: XLogRecPtr,
    /// `RedoStartTLI`.
    pub redo_start_tli: TimeLineID,

    // -- local cached shared flags (xlogrecovery.c:178-184) --
    /// `LocalHotStandbyActive` (false means "not known").
    pub local_hot_standby_active: bool,
    /// `LocalPromoteIsTriggered` (false means "not known").
    pub local_promote_is_triggered: bool,

    // -- reader objects + flags (xlogrecovery.c:187-205) --
    /// `doRequestWalReceiverReply`.
    pub do_request_wal_receiver_reply: bool,
    /// flag to tell the page-read driver that we have started replaying.
    pub in_redo: bool,

    // -- WAL read cursor (xlogrecovery.c:232-250) --
    /// where we got the currently open file from.
    pub read_source: XLogSource,
    /// which source we're currently reading from.
    pub current_source: XLogSource,
    /// our last attempt to read from `currentSource` failed.
    pub last_source_failed: bool,
    /// a config change requires a walreceiver restart.
    pub pending_walrcv_restart: bool,

    // -- receipt tracking (xlogrecovery.c:260-265) --
    /// when we last obtained some WAL data to process.
    pub xlog_receipt_time: TimestampTz,
    /// where we last successfully read some WAL.
    pub xlog_receipt_source: XLogSource,
    /// local copy of `WalRcv->flushedUpto`.
    pub flushed_upto: XLogRecPtr,
    /// TLI of the data in `flushedUpto`.
    pub receive_tli: TimeLineID,

    // -- consistency (xlogrecovery.c:280-305) --
    /// copy of `minRecoveryPoint` from the control file.
    pub min_recovery_point: XLogRecPtr,
    /// TLI of `minRecoveryPoint`.
    pub min_recovery_point_tli: TimeLineID,
    /// backup start point.
    pub backup_start_point: XLogRecPtr,
    /// backup end point.
    pub backup_end_point: XLogRecPtr,
    /// must reach the backup end point.
    pub backup_end_required: bool,
    /// Have we reached a consistent database state?
    pub reached_consistency: bool,

    // -- end-of-WAL + stop point (xlogrecovery.c:379-390) --
    /// start pointer of a broken record at end of WAL.
    pub aborted_rec_ptr: XLogRecPtr,
    /// location of the first contrecord that went missing.
    pub missing_contrec_ptr: XLogRecPtr,
    /// recovery stop xid.
    pub recovery_stop_xid: TransactionId,
    /// recovery stop time.
    pub recovery_stop_time: TimestampTz,
    /// recovery stop LSN.
    pub recovery_stop_lsn: XLogRecPtr,
    /// recovery stop name (a NUL-trimmed name of at most `MAXFNAMELEN-1` chars).
    pub recovery_stop_name: String,
    /// stop-after vs stop-before.
    pub recovery_stop_after: bool,

    // -- read-back copies of the XLogRecoveryCtlData shared fields the startup
    //    process caches locally (xlogrecovery.c:311). The authoritative shared
    //    words live in `crate::shmem::XLogRecoveryShared`. --
    /// `SharedHotStandbyActive` (startup-local cache).
    pub shared_hot_standby_active: bool,
    /// `SharedPromoteIsTriggered` (startup-local cache).
    pub shared_promote_is_triggered: bool,
    /// Last record successfully replayed: start position.
    pub last_replayed_read_rec_ptr: XLogRecPtr,
    /// Last record successfully replayed: end+1 position.
    pub last_replayed_end_rec_ptr: XLogRecPtr,
    /// Last record successfully replayed: timeline.
    pub last_replayed_tli: TimeLineID,
    /// While replaying a record: end+1 of that record.
    pub replay_end_rec_ptr: XLogRecPtr,
    pub replay_end_tli: TimeLineID,
    /// timestamp of last COMMIT/ABORT record replayed (or being replayed).
    pub recovery_last_xtime: TimestampTz,
    /// timestamp of when we started replaying the current chunk of WAL data.
    pub current_chunk_start_time: TimestampTz,
    /// Recovery pause state.
    pub recovery_pause_state: RecoveryPauseState,

    // -- current decoded record handle (the externally-owned reader/decoder) --
    /// The record currently being read / applied (handle to the reader). 0 ==
    /// no record (C `NULL`).
    pub current_record: RecordRef,

    /// `performedWalRecovery` — a `StartupXLOG` local mirrored onto the recovery
    /// state so the end-of-recovery WAL action can read it across the
    /// `startup_xlog` / `startup_xlog_after_init` split. True iff the redo loop
    /// actually ran (i.e. `InRecovery` was set on entry to the `if (InRecovery)`
    /// block).
    pub performed_wal_recovery: bool,
}

impl Default for XLogRecoveryState {
    fn default() -> Self {
        Self::new()
    }
}

impl XLogRecoveryState {
    /// Create the recovery state with the same initial values C gives its
    /// globals.
    pub fn new() -> Self {
        Self {
            recovery_restore_command: String::new(),
            recovery_end_command: String::new(),
            archive_cleanup_command: String::new(),
            recovery_target: RecoveryTargetType::Unset,
            // C: `bool recoveryTargetInclusive = true;`
            recovery_target_inclusive: true,
            recovery_target_action: RecoveryTargetAction::Pause,
            recovery_target_xid: 0,
            recovery_target_time_string: String::new(),
            recovery_target_time: 0,
            recovery_target_name: String::new(),
            recovery_target_lsn: InvalidXLogRecPtr,
            recovery_min_apply_delay: 0,
            primary_conn_info: String::new(),
            primary_slot_name: String::new(),
            wal_receiver_create_temp_slot: false,
            // C: `recoveryTargetTimeLineGoal = RECOVERY_TARGET_TIMELINE_LATEST;`
            recovery_target_timeline_goal: RecoveryTargetTimeLineGoal::Latest,
            recovery_target_tli_requested: 0,
            recovery_target_tli: 0,
            cur_file_tli: 0,
            archive_recovery_requested: false,
            in_archive_recovery: false,
            standby_mode_requested: false,
            standby_mode: false,
            standby_signal_file_found: false,
            recovery_signal_file_found: false,
            check_point_loc: InvalidXLogRecPtr,
            check_point_tli: 0,
            redo_start_lsn: InvalidXLogRecPtr,
            redo_start_tli: 0,
            local_hot_standby_active: false,
            local_promote_is_triggered: false,
            do_request_wal_receiver_reply: false,
            in_redo: false,
            read_source: XLogSource::Any,
            current_source: XLogSource::Any,
            last_source_failed: false,
            pending_walrcv_restart: false,
            xlog_receipt_time: 0,
            xlog_receipt_source: XLogSource::Any,
            flushed_upto: 0,
            receive_tli: 0,
            min_recovery_point: 0,
            min_recovery_point_tli: 0,
            backup_start_point: 0,
            backup_end_point: 0,
            backup_end_required: false,
            reached_consistency: false,
            aborted_rec_ptr: 0,
            missing_contrec_ptr: 0,
            recovery_stop_xid: InvalidTransactionId,
            recovery_stop_time: 0,
            recovery_stop_lsn: InvalidXLogRecPtr,
            recovery_stop_name: String::new(),
            recovery_stop_after: false,
            shared_hot_standby_active: false,
            shared_promote_is_triggered: false,
            last_replayed_read_rec_ptr: InvalidXLogRecPtr,
            last_replayed_end_rec_ptr: InvalidXLogRecPtr,
            last_replayed_tli: 0,
            replay_end_rec_ptr: InvalidXLogRecPtr,
            replay_end_tli: 0,
            recovery_last_xtime: 0,
            current_chunk_start_time: 0,
            recovery_pause_state: RecoveryPauseState::NotPaused,
            current_record: RecordRef::default(),
            performed_wal_recovery: false,
        }
    }
}

/// Trim a recovery-stop name to at most `MAXFNAMELEN - 1` bytes
/// (`strlcpy(recoveryStopName, ..., MAXFNAMELEN)`).
#[allow(dead_code)]
pub(crate) fn truncate_stop_name(mut name: String) -> String {
    if name.len() >= MAXFNAMELEN {
        name.truncate(MAXFNAMELEN - 1);
    }
    name
}

/// `LSN_FORMAT_ARGS(lsn)` rendered as the canonical `%X/%X`.
#[inline]
#[allow(dead_code)]
pub(crate) fn lsn_fmt(lsn: XLogRecPtr) -> String {
    alloc::format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

/// `XLogRecPtrIsInvalid(ptr)` -> `ptr == 0`.
#[inline]
#[allow(dead_code)]
pub(crate) fn xlog_rec_ptr_is_invalid(ptr: XLogRecPtr) -> bool {
    ptr == InvalidXLogRecPtr
}
