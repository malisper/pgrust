//! `backend-access-transam-xlog` — an idiomatic Rust port of the WAL engine
//! `src/backend/access/transam/xlog.c` (PostgreSQL 18.3).
//!
//! ## What is grounded 1:1 in this crate
//!
//! The pure *arithmetic* and *codec* core of xlog.c is ported faithfully,
//! function for function:
//!
//!   * byte-pos <-> LSN arithmetic ([`XLogBytePosToRecPtr`],
//!     [`XLogBytePosToEndRecPtr`], [`XLogRecPtrToBytePos`], the
//!     [`UsableBytesInPage`]/[`UsableBytesInSegment`] helpers).
//!   * segment/file-name arithmetic + codec (`xlog_internal.h` macros and the
//!     WAL file-name / path / sidecar codec).
//!   * checkpoint-distance arithmetic ([`CalculateCheckpointSegments`]).
//!   * the `WalConfig` predicate macros (the xlog.h inline predicates).
//!   * validity predicates ([`XLogRecPtrIsInvalid`], [`IsValidWalSegSize`],
//!     [`check_wal_segment_size`]).
//!   * the WAL-retention horizon arithmetic ([`retention`]).
//!   * the `CheckPoint` <-> on-disk byte image codec ([`checkpoint`]).
//!   * the checkpoint state machine ([`checkpoint::CreateCheckPoint`],
//!     [`checkpoint::CheckPointGuts`], [`checkpoint::CreateRestartPoint`]) and
//!     the XLOG-rmgr redo dispatch ([`redo::xlog_redo`]).
//!
//! ## The deferred hard core: the XLogCtl shmem WAL-write / fsync DRIVER
//!
//! xlog.c's insertion-lock shmem driver (`XLogWrite`, `AdvanceXLInsertBuffer`,
//! `StartupXLOG`, the `XLogCtl` shmem readers, the control-file disk I/O) is the
//! known hard core. It requires the not-yet-ported shared-memory / fd / spinlock
//! substrate (`ShmemInitStruct`, `fd.c`, spinlocks). Per the project rule (a
//! callee crate that is not ported yet is the only acceptable missing piece),
//! those driver legs and every genuinely cross-subsystem read/call cross the
//! seams in [`seam`]; they panic loudly until the owning subsystem lands. This
//! crate's OWN logic — all of the arithmetic, codecs, the checkpoint/redo
//! control flow — is present and complete. See `DESIGN_DEBT.md` (`xlog-driver`).

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::format;
use alloc::string::String;

use backend_utils_error::{PgError, PgResult};

use types_core::{
    FullTransactionId, MultiXactId, MultiXactOffset, Oid, TimeLineID,
    TransactionId, XLogRecPtr, XLogSegNo,
};
use types_tuple::Datum;
use types_wal::xlog_consts::{
    ArchiveMode, WALAvailability, WalCompression, WalLevel, WalSyncMethod,
    DEFAULT_XLOG_SEG_SIZE, SIZE_OF_XLOG_LONG_PHD, SIZE_OF_XLOG_SHORT_PHD, WAL_SEG_MAX_SIZE,
    WAL_SEG_MIN_SIZE, XLOGDIR, XLOG_BLCKSZ, XLOG_FNAME_LEN,
};

pub mod checkpoint;
pub use checkpoint::{
    checkpoint_to_bytes, CheckPointGuts as DoCheckPointGuts, CheckpointState, CheckpointStats,
    CreateCheckPoint as DoCreateCheckPoint, CreateRestartPoint as DoCreateRestartPoint,
    SIZE_OF_CHECK_POINT,
};

pub mod redo;
pub use redo::xlog_redo;

pub mod retention;

pub mod shmem;
pub use shmem::{
    DataChecksumsEnabled, GetDefaultCharSignedness, GetFlushRecPtr, GetInsertRecPtr,
    GetMockAuthenticationNonce, GetRedoRecPtr, GetSystemIdentifier, GetWALInsertionTimeLineIfSet,
    GetXLogInsertRecPtr, ReadControlFile, RecoveryInProgress, UpdateControlFile, WriteControlFile,
    XLOGShmemInit, XLOGShmemSize,
};

pub mod insert;
pub use insert::{XLogInsertAllowed, XLogInsertRecord};

pub mod write;
pub use write::{
    issue_xlog_fsync, IsInstallXLogFileSegmentActive, ResetInstallXLogFileSegmentActive,
    SetInstallXLogFileSegmentActive, XLogBackgroundFlush, XLogFileInit, XLogFileOpen, XLogFlush,
    XLogShutdownWalRcv,
};

pub mod control_funcs;
pub use control_funcs::{
    AllowCascadeReplication, ReachedEndOfBackup, RequestXLogSwitch, UpdateFullPageWrites,
    XLogPutNextOid, XLogReportParameters, XLogRestorePoint,
};

pub mod startup;
pub use startup::{
    CheckRequiredParameterValues, CreateEndOfRecoveryRecord, CreateOverwriteContrecordRecord,
    PerformRecoveryXLogAction, StartupXLOG, ValidateXLOGDirectoryStructure, XLogInitNewTimeline,
};

pub mod guc_state;
pub mod guc_vars;

pub mod driver;
pub use driver::{
    CheckXLogRemoved, GetFakeLSNForUnloggedRel, GetFullPageWriteInfo, GetLastImportantRecPtr,
    GetLastSegSwitchData, GetOldestRestartPoint, GetRecoveryState, GetWALInsertionTimeLine,
    GetXLogWriteRecPtr, SetWalWriterSleeping, XLogGetLastRemovedSegno,
    XLogGetOldestSegno, XLogGetReplicationSlotMinimumLSN, XLogNeedsFlush, XLogSetAsyncXactLSN,
    XLogSetReplicationSlotMinimumLSN,
};

/// `.partial` / `.history` / `.backup` sidecar suffixes (`xlog_internal.h`).
pub const XLOG_FILE_SUFFIX_PARTIAL: &str = ".partial";
pub const TL_HISTORY_SUFFIX: &str = ".history";
pub const BACKUP_HISTORY_SUFFIX: &str = ".backup";

/// `InvalidXLogRecPtr` (`access/xlogdefs.h`).
pub const InvalidXLogRecPtr: XLogRecPtr = 0;

// ===========================================================================
// WalConfig — the WAL-related GUC values + the xlog.h predicate macros.
// ===========================================================================

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalConfig {
    pub wal_segment_size: i32,
    pub min_wal_size_mb: i32,
    pub max_wal_size_mb: i32,
    pub wal_keep_size_mb: i32,
    pub max_slot_wal_keep_size_mb: i32,
    pub XLOGbuffers: i32,
    pub XLogArchiveTimeout: i32,
    pub wal_retrieve_retry_interval: i32,
    pub EnableHotStandby: bool,
    pub fullPageWrites: bool,
    pub wal_log_hints: bool,
    pub wal_compression: WalCompression,
    pub wal_init_zero: bool,
    pub wal_recycle: bool,
    pub log_checkpoints: bool,
    pub CommitDelay: i32,
    pub CommitSiblings: i32,
    pub track_wal_io_timing: bool,
    pub wal_decode_buffer_size: i32,
    pub XLogArchiveMode: ArchiveMode,
    pub wal_sync_method: WalSyncMethod,
    pub wal_level: WalLevel,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            wal_segment_size: DEFAULT_XLOG_SEG_SIZE,
            min_wal_size_mb: 80,
            max_wal_size_mb: 1024,
            wal_keep_size_mb: 0,
            max_slot_wal_keep_size_mb: -1,
            XLOGbuffers: -1,
            XLogArchiveTimeout: 0,
            wal_retrieve_retry_interval: 5000,
            EnableHotStandby: false,
            fullPageWrites: true,
            wal_log_hints: false,
            wal_compression: WalCompression::None,
            wal_init_zero: true,
            wal_recycle: true,
            log_checkpoints: true,
            CommitDelay: 0,
            CommitSiblings: 5,
            track_wal_io_timing: false,
            wal_decode_buffer_size: 512,
            XLogArchiveMode: ArchiveMode::Off,
            wal_sync_method: WalSyncMethod::OpenDsync,
            wal_level: WalLevel::Replica,
        }
    }
}

impl WalConfig {
    /// `XLogArchivingActive()` (xlog.h): `XLogArchiveMode > ARCHIVE_MODE_OFF`.
    pub fn XLogArchivingActive(&self) -> bool {
        self.XLogArchiveMode > ArchiveMode::Off
    }

    /// `XLogArchivingAlways()` (xlog.h): `XLogArchiveMode == ARCHIVE_MODE_ALWAYS`.
    pub fn XLogArchivingAlways(&self) -> bool {
        self.XLogArchiveMode == ArchiveMode::Always
    }

    /// `XLogIsNeeded()` (xlog.h): `wal_level >= WAL_LEVEL_REPLICA`.
    pub fn XLogIsNeeded(&self) -> bool {
        self.wal_level >= WalLevel::Replica
    }

    /// `XLogHintBitIsNeeded()` (xlog.h): `DataChecksumsEnabled() || wal_log_hints`.
    pub fn XLogHintBitIsNeeded(&self, data_checksums_enabled: bool) -> bool {
        data_checksums_enabled || self.wal_log_hints
    }

    /// `XLogStandbyInfoActive()` (xlog.h): `wal_level >= WAL_LEVEL_REPLICA`.
    pub fn XLogStandbyInfoActive(&self) -> bool {
        self.wal_level >= WalLevel::Replica
    }

    /// `XLogLogicalInfoActive()` (xlog.h): `wal_level >= WAL_LEVEL_LOGICAL`.
    pub fn XLogLogicalInfoActive(&self) -> bool {
        self.wal_level >= WalLevel::Logical
    }
}

// ===========================================================================
// Pure validity predicates.
// ===========================================================================

/// `XLogRecPtrIsInvalid(record)` == `record == InvalidXLogRecPtr` (== 0).
pub fn XLogRecPtrIsInvalid(record: XLogRecPtr) -> bool {
    record == 0
}

/// `IsValidWalSegSize(size)` (xlog_internal.h).
pub fn IsValidWalSegSize(size: i32) -> bool {
    size > 0 && (size & (size - 1)) == 0 && (WAL_SEG_MIN_SIZE..=WAL_SEG_MAX_SIZE).contains(&size)
}

/// `check_wal_segment_size(*newval, ...)` (xlog.c) — the GUC check hook.
pub fn check_wal_segment_size(newval: i32) -> Result<(), &'static str> {
    if !IsValidWalSegSize(newval) {
        return Err("The WAL segment size must be a power of two between 1 MB and 1 GB.");
    }
    Ok(())
}

// ===========================================================================
// Segment / byte arithmetic — the xlog_internal.h macros, ported 1:1.
// ===========================================================================

/// `XLogSegmentsPerXLogId(wal_segsz_bytes)` == `0x100000000 / wal_segsz_bytes`.
pub fn XLogSegmentsPerXLogId(wal_segsz_bytes: i32) -> u64 {
    0x1_0000_0000_u64 / wal_segsz_bytes as u64
}

/// `XLogSegNoOffsetToRecPtr(segno, offset, wal_segsz_bytes, dest)`.
pub fn XLogSegNoOffsetToRecPtr(segno: XLogSegNo, offset: u32, wal_segsz_bytes: i32) -> XLogRecPtr {
    segno
        .wrapping_mul(wal_segsz_bytes as u64)
        .wrapping_add(offset as u64)
}

/// `XLogSegmentOffset(xlogptr, wal_segsz_bytes)`.
pub fn XLogSegmentOffset(xlogptr: XLogRecPtr, wal_segsz_bytes: i32) -> u32 {
    (xlogptr & (wal_segsz_bytes as u64 - 1)) as u32
}

/// `XLByteToSeg(xlrp, logSegNo, wal_segsz_bytes)`.
pub fn XLByteToSeg(xlrp: XLogRecPtr, wal_segsz_bytes: i32) -> XLogSegNo {
    xlrp / wal_segsz_bytes as u64
}

/// `XLByteToPrevSeg(xlrp, logSegNo, wal_segsz_bytes)`.
pub fn XLByteToPrevSeg(xlrp: XLogRecPtr, wal_segsz_bytes: i32) -> XLogSegNo {
    xlrp.wrapping_sub(1) / wal_segsz_bytes as u64
}

/// `XLogMBVarToSegs(mbvar, wal_segsz_bytes)`.
pub fn XLogMBVarToSegs(mbvar: i32, wal_segsz_bytes: i32) -> i32 {
    mbvar / (wal_segsz_bytes / (1024 * 1024))
}

/// `ConvertToXSegs(x, segsize)` == `XLogMBVarToSegs(x, segsize)` (xlog.c:628).
pub fn ConvertToXSegs(x: i32, wal_segsz_bytes: i32) -> i32 {
    XLogMBVarToSegs(x, wal_segsz_bytes)
}

/// `XLByteInSeg(xlrp, logSegNo, wal_segsz_bytes)`.
pub fn XLByteInSeg(xlrp: XLogRecPtr, log_seg_no: XLogSegNo, wal_segsz_bytes: i32) -> bool {
    XLByteToSeg(xlrp, wal_segsz_bytes) == log_seg_no
}

/// `XLByteInPrevSeg(xlrp, logSegNo, wal_segsz_bytes)`.
pub fn XLByteInPrevSeg(xlrp: XLogRecPtr, log_seg_no: XLogSegNo, wal_segsz_bytes: i32) -> bool {
    XLByteToPrevSeg(xlrp, wal_segsz_bytes) == log_seg_no
}

/// `XRecOffIsValid(xlrp)` == `xlrp % XLOG_BLCKSZ >= SizeOfXLogShortPHD`.
pub fn XRecOffIsValid(xlrp: XLogRecPtr) -> bool {
    (xlrp % XLOG_BLCKSZ as u64) as usize >= SIZE_OF_XLOG_SHORT_PHD
}

// ===========================================================================
// "Usable byte position" helpers + the byte-pos <-> LSN conversions.
// ===========================================================================

/// `UsableBytesInPage` (xlog.c:622) == `XLOG_BLCKSZ - SizeOfXLogShortPHD`.
pub const fn UsableBytesInPage() -> u64 {
    (XLOG_BLCKSZ - SIZE_OF_XLOG_SHORT_PHD) as u64
}

/// `UsableBytesInSegment` (computed in `XLOGShmemInit`, xlog.c).
pub fn UsableBytesInSegment(wal_segsz_bytes: i32) -> u64 {
    (wal_segsz_bytes as u64 / XLOG_BLCKSZ as u64) * UsableBytesInPage()
        - (SIZE_OF_XLOG_LONG_PHD - SIZE_OF_XLOG_SHORT_PHD) as u64
}

/// `XLogBytePosToRecPtr(bytepos)` (xlog.c).
pub fn XLogBytePosToRecPtr(bytepos: u64, wal_segsz_bytes: i32) -> XLogRecPtr {
    let usable_in_seg = UsableBytesInSegment(wal_segsz_bytes);
    let usable_in_page = UsableBytesInPage();

    let fullsegs = bytepos / usable_in_seg;
    let mut bytesleft = bytepos % usable_in_seg;

    let seg_offset: u64;
    if bytesleft < (XLOG_BLCKSZ - SIZE_OF_XLOG_LONG_PHD) as u64 {
        seg_offset = bytesleft + SIZE_OF_XLOG_LONG_PHD as u64;
    } else {
        let mut off = XLOG_BLCKSZ as u64;
        bytesleft -= (XLOG_BLCKSZ - SIZE_OF_XLOG_LONG_PHD) as u64;

        let fullpages = bytesleft / usable_in_page;
        bytesleft %= usable_in_page;

        off += fullpages * XLOG_BLCKSZ as u64 + bytesleft + SIZE_OF_XLOG_SHORT_PHD as u64;
        seg_offset = off;
    }

    XLogSegNoOffsetToRecPtr(fullsegs, seg_offset as u32, wal_segsz_bytes)
}

/// `XLogBytePosToEndRecPtr(bytepos)` (xlog.c).
pub fn XLogBytePosToEndRecPtr(bytepos: u64, wal_segsz_bytes: i32) -> XLogRecPtr {
    let usable_in_seg = UsableBytesInSegment(wal_segsz_bytes);
    let usable_in_page = UsableBytesInPage();

    let fullsegs = bytepos / usable_in_seg;
    let mut bytesleft = bytepos % usable_in_seg;

    let seg_offset: u64;
    if bytesleft < (XLOG_BLCKSZ - SIZE_OF_XLOG_LONG_PHD) as u64 {
        if bytesleft == 0 {
            seg_offset = 0;
        } else {
            seg_offset = bytesleft + SIZE_OF_XLOG_LONG_PHD as u64;
        }
    } else {
        let mut off = XLOG_BLCKSZ as u64;
        bytesleft -= (XLOG_BLCKSZ - SIZE_OF_XLOG_LONG_PHD) as u64;

        let fullpages = bytesleft / usable_in_page;
        bytesleft %= usable_in_page;

        if bytesleft == 0 {
            off += fullpages * XLOG_BLCKSZ as u64 + bytesleft;
        } else {
            off += fullpages * XLOG_BLCKSZ as u64 + bytesleft + SIZE_OF_XLOG_SHORT_PHD as u64;
        }
        seg_offset = off;
    }

    XLogSegNoOffsetToRecPtr(fullsegs, seg_offset as u32, wal_segsz_bytes)
}

/// `XLogRecPtrToBytePos(ptr)` (xlog.c) — inverse of [`XLogBytePosToRecPtr`].
pub fn XLogRecPtrToBytePos(ptr: XLogRecPtr, wal_segsz_bytes: i32) -> u64 {
    let usable_in_seg = UsableBytesInSegment(wal_segsz_bytes);
    let usable_in_page = UsableBytesInPage();

    let fullsegs = XLByteToSeg(ptr, wal_segsz_bytes);

    let fullpages = (XLogSegmentOffset(ptr, wal_segsz_bytes) as u64) / XLOG_BLCKSZ as u64;
    let offset = ptr % XLOG_BLCKSZ as u64;

    let mut result: u64;
    if fullpages == 0 {
        result = fullsegs * usable_in_seg;
        if offset > 0 {
            result += offset - SIZE_OF_XLOG_LONG_PHD as u64;
        }
    } else {
        result = fullsegs * usable_in_seg
            + (XLOG_BLCKSZ - SIZE_OF_XLOG_LONG_PHD) as u64
            + (fullpages - 1) * usable_in_page;
        if offset > 0 {
            result += offset - SIZE_OF_XLOG_SHORT_PHD as u64;
        }
    }

    result
}

// ===========================================================================
// WAL file-name / path / sidecar-name codec (xlog_internal.h + xlog.c).
// ===========================================================================

fn is_upper_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'A'..=b'F').contains(&byte)
}

fn parse_hex_u32(s: &str) -> PgResult<u32> {
    u32::from_str_radix(s, 16).map_err(|_| PgError::error("invalid WAL file name"))
}

/// `XLogFileName(fname, tli, logSegNo, wal_segsz_bytes)` (xlog_internal.h).
pub fn XLogFileName(tli: TimeLineID, log_seg_no: XLogSegNo, wal_segsz_bytes: i32) -> String {
    let segments = XLogSegmentsPerXLogId(wal_segsz_bytes);
    format!(
        "{tli:08X}{:08X}{:08X}",
        log_seg_no / segments,
        log_seg_no % segments
    )
}

/// `XLogFileNameById(fname, tli, log, seg)` (xlog_internal.h).
pub fn XLogFileNameById(tli: TimeLineID, log: u32, seg: u32) -> String {
    format!("{tli:08X}{log:08X}{seg:08X}")
}

/// `IsXLogFileName(fname)` (xlog_internal.h): 24 upper-hex chars.
pub fn IsXLogFileName(fname: &str) -> bool {
    fname.len() == XLOG_FNAME_LEN && fname.bytes().all(is_upper_hex)
}

/// `IsPartialXLogFileName(fname)` (xlog_internal.h).
pub fn IsPartialXLogFileName(fname: &str) -> bool {
    fname.len() == XLOG_FNAME_LEN + XLOG_FILE_SUFFIX_PARTIAL.len()
        && fname[..XLOG_FNAME_LEN].bytes().all(is_upper_hex)
        && &fname[XLOG_FNAME_LEN..] == XLOG_FILE_SUFFIX_PARTIAL
}

/// `XLogFromFileName(fname, tli, logSegNo, wal_segsz_bytes)` (xlog_internal.h).
pub fn XLogFromFileName(fname: &str, wal_segsz_bytes: i32) -> PgResult<(TimeLineID, XLogSegNo)> {
    if !IsXLogFileName(fname) {
        return Err(PgError::error("invalid WAL file name"));
    }
    let tli = parse_hex_u32(&fname[0..8])?;
    let log = parse_hex_u32(&fname[8..16])?;
    let seg = parse_hex_u32(&fname[16..24])?;
    Ok((
        tli,
        log as u64 * XLogSegmentsPerXLogId(wal_segsz_bytes) + seg as u64,
    ))
}

/// `XLogFilePath(path, tli, logSegNo, wal_segsz_bytes)` (xlog_internal.h).
pub fn XLogFilePath(tli: TimeLineID, log_seg_no: XLogSegNo, wal_segsz_bytes: i32) -> String {
    format!("{XLOGDIR}/{}", XLogFileName(tli, log_seg_no, wal_segsz_bytes))
}

/// `TLHistoryFileName(fname, tli)` (xlog_internal.h).
pub fn TLHistoryFileName(tli: TimeLineID) -> String {
    format!("{tli:08X}{TL_HISTORY_SUFFIX}")
}

/// `IsTLHistoryFileName(fname)` (xlog_internal.h).
pub fn IsTLHistoryFileName(fname: &str) -> bool {
    fname.len() == 8 + TL_HISTORY_SUFFIX.len()
        && fname[..8].bytes().all(is_upper_hex)
        && &fname[8..] == TL_HISTORY_SUFFIX
}

/// `TLHistoryFilePath(path, tli)` (xlog_internal.h).
pub fn TLHistoryFilePath(tli: TimeLineID) -> String {
    format!("{XLOGDIR}/{}", TLHistoryFileName(tli))
}

/// `StatusFilePath(path, xlog, suffix)` (xlog_internal.h).
pub fn StatusFilePath(xlog: &str, suffix: &str) -> String {
    format!("{XLOGDIR}/archive_status/{xlog}{suffix}")
}

/// `BackupHistoryFileName(...)` (xlog_internal.h).
pub fn BackupHistoryFileName(
    tli: TimeLineID,
    log_seg_no: XLogSegNo,
    startpoint: XLogRecPtr,
    wal_segsz_bytes: i32,
) -> String {
    let segments = XLogSegmentsPerXLogId(wal_segsz_bytes);
    format!(
        "{tli:08X}{:08X}{:08X}.{:08X}{BACKUP_HISTORY_SUFFIX}",
        log_seg_no / segments,
        log_seg_no % segments,
        XLogSegmentOffset(startpoint, wal_segsz_bytes)
    )
}

/// `IsBackupHistoryFileName(fname)` (xlog_internal.h).
pub fn IsBackupHistoryFileName(fname: &str) -> bool {
    fname.len() > XLOG_FNAME_LEN
        && fname[..XLOG_FNAME_LEN].bytes().all(is_upper_hex)
        && fname.ends_with(BACKUP_HISTORY_SUFFIX)
}

/// `BackupHistoryFilePath(...)` (xlog_internal.h).
pub fn BackupHistoryFilePath(
    tli: TimeLineID,
    log_seg_no: XLogSegNo,
    startpoint: XLogRecPtr,
    wal_segsz_bytes: i32,
) -> String {
    format!(
        "{XLOGDIR}/{}",
        BackupHistoryFileName(tli, log_seg_no, startpoint, wal_segsz_bytes)
    )
}

// ===========================================================================
// Checkpoint-distance arithmetic (xlog.c:CalculateCheckpointSegments).
// ===========================================================================

/// `CalculateCheckpointSegments()` (xlog.c).
pub fn CalculateCheckpointSegments(
    max_wal_size_mb: i32,
    wal_segsz_bytes: i32,
    checkpoint_completion_target: f64,
) -> i32 {
    let target = ConvertToXSegs(max_wal_size_mb, wal_segsz_bytes) as f64
        / (1.0 + checkpoint_completion_target);

    let mut checkpoint_segments = target as i32;
    if checkpoint_segments < 1 {
        checkpoint_segments = 1;
    }
    checkpoint_segments
}

/// `assign_max_wal_size(newval, extra)` (xlog.c:2224) — the GUC assign hook:
/// set `max_wal_size_mb` then recompute `CheckPointSegments`. The owning GUC
/// state holds `max_wal_size_mb`/`CheckPointCompletionTarget`/`CheckPointSegments`;
/// this returns the recomputed segment count to publish.
pub fn assign_max_wal_size(newval: i32, wal_segsz_bytes: i32, checkpoint_completion_target: f64) -> i32 {
    CalculateCheckpointSegments(newval, wal_segsz_bytes, checkpoint_completion_target)
}

/// `assign_checkpoint_completion_target(newval, extra)` (xlog.c:2231) — set
/// `CheckPointCompletionTarget` then recompute `CheckPointSegments`.
pub fn assign_checkpoint_completion_target(newval: f64, max_wal_size_mb: i32, wal_segsz_bytes: i32) -> i32 {
    CalculateCheckpointSegments(max_wal_size_mb, wal_segsz_bytes, newval)
}

/// `XLogCheckpointNeeded(new_segno)` (xlog.c:2304) — whether enough xlog space
/// has been consumed since the last checkpoint REDO that a new checkpoint is
/// needed. `redo_rec_ptr`/`checkpoint_segments` are the (driver-owned)
/// `RedoRecPtr` and `CheckPointSegments`; the arithmetic is grounded here.
pub fn XLogCheckpointNeeded(
    new_segno: XLogSegNo,
    redo_rec_ptr: XLogRecPtr,
    checkpoint_segments: i32,
    wal_segsz_bytes: i32,
) -> bool {
    let old_segno = XLByteToSeg(redo_rec_ptr, wal_segsz_bytes);
    new_segno >= old_segno.wrapping_add((checkpoint_segments - 1) as u64)
}

// ===========================================================================
// WAL-buffer count auto-tune + the wal_buffers GUC check hook (xlog.c).
// ===========================================================================

/// `XLOGChooseNumBuffers()` (xlog.c:4681) — auto-tuned WAL buffer count:
/// `NBuffers / 32`, clamped to `[8, wal_segment_size / XLOG_BLCKSZ]`.
pub fn XLOGChooseNumBuffers(NBuffers: i32, wal_segsz_bytes: i32) -> i32 {
    let mut xbuffers = NBuffers / 32;
    if xbuffers > wal_segsz_bytes / XLOG_BLCKSZ as i32 {
        xbuffers = wal_segsz_bytes / XLOG_BLCKSZ as i32;
    }
    if xbuffers < 8 {
        xbuffers = 8;
    }
    xbuffers
}

/// `check_wal_buffers(*newval, ...)` (xlog.c:4697) — the GUC check hook. `-1`
/// requests auto-tune (left as `-1` until `XLOGShmemSize` if `XLOGbuffers` is
/// still `-1`, else substituted with [`XLOGChooseNumBuffers`]); manual values
/// below 4 blocks are silently clamped to 4. Returns the (possibly rewritten)
/// value; the hook never rejects.
pub fn check_wal_buffers(newval: i32, XLOGbuffers: i32, NBuffers: i32, wal_segsz_bytes: i32) -> i32 {
    let mut v = newval;
    if v == -1 {
        if XLOGbuffers == -1 {
            return v;
        }
        v = XLOGChooseNumBuffers(NBuffers, wal_segsz_bytes);
    }
    if v < 4 {
        v = 4;
    }
    v
}

// ===========================================================================
// get_sync_bit — the WalSyncMethod -> open(2) sync-flag mapping (xlog.c:8678).
// ===========================================================================

/// `get_sync_bit(method)` (xlog.c:8678) — the open(2) flag bits for a
/// `wal_sync_method`. The platform `O_SYNC`/`O_DSYNC` values and the
/// already-computed `o_direct_flag` (which depends on `io_direct_flags` /
/// `AmWalReceiverProcess`, a driver concern) are supplied by the caller; the
/// branch logic — the `!enableFsync` short-circuit, the fsync/fdatasync/
/// writethrough no-extra-bit arms, the open/open_dsync arms, and the
/// unrecognized-method `elog(ERROR)` default — is grounded here.
pub fn get_sync_bit(
    method: WalSyncMethod,
    o_direct_flag: i32,
    enable_fsync: bool,
    o_sync: i32,
    o_dsync: i32,
) -> PgResult<i32> {
    if !enable_fsync {
        return Ok(o_direct_flag);
    }
    match method {
        WalSyncMethod::Fsync | WalSyncMethod::FsyncWritethrough | WalSyncMethod::Fdatasync => {
            Ok(o_direct_flag)
        }
        WalSyncMethod::Open => Ok(o_sync | o_direct_flag),
        WalSyncMethod::OpenDsync => Ok(o_dsync | o_direct_flag),
    }
}

// ===========================================================================
// The XLogCtl shmem-write / WAL-write / fsync DRIVER + cross-subsystem getters.
//
// These are xlog.c's OWN driver functions whose bodies operate the `XLogCtl`
// shared-memory region, the open WAL segment files, and the control file. They
// require the not-yet-ported shared-memory / fd / spinlock substrate
// (`ShmemInitStruct`, fd.c, spinlocks). Per the project rule, a code path may
// panic because a callee's crate isn't ported yet — so each entry point panics
// loudly with its name and the `xlog-driver` debt tag, never a silent stub.
// The arithmetic/codec each would consult is grounded above. See DESIGN_DEBT.md.
// ===========================================================================

macro_rules! xlog_driver_deferred {
    ($( $(#[$attr:meta])* pub fn $name:ident ( $($arg:ident : $argty:ty),* $(,)? ) $(-> $ret:ty)? ; )+) => {
        $(
            $(#[$attr])*
            pub fn $name ( $($arg : $argty),* ) $(-> $ret)? {
                $( let _ = &$arg; )*
                panic!(concat!(
                    "xlog driver not ported (xlog-driver debt): ",
                    stringify!($name),
                    " requires the XLogCtl shmem / fd / spinlock substrate"
                ))
            }
        )+
    };
}

// `XLogFlush`, `XLogBackgroundFlush`, `XLogFileInit`, `XLogFileOpen` are now
// REAL: ported in [`crate::write`] and re-exported above. They are no longer in
// the deferred-driver list.
// Newly REAL in [`crate::driver`] (re-exported above), removed from the
// deferred list: `XLogNeedsFlush`, `CheckXLogRemoved`, `SetWalWriterSleeping`,
// `GetXLogWriteRecPtr`, `GetLastImportantRecPtr`, `GetWALInsertionTimeLine`,
// `GetFullPageWriteInfo`, `GetRecoveryState`, `GetFakeLSNForUnloggedRel`,
// `XLogGetLastRemovedSegno`, `XLogGetOldestSegno`, `GetLastSegSwitchData`,
// `GetOldestRestartPoint`, `XLogGetReplicationSlotMinimumLSN`,
// `XLogSetReplicationSlotMinimumLSN`, `XLogSetAsyncXactLSN`.
xlog_driver_deferred! {
    /// `ShutdownXLOG(code, arg)` — the WAL-engine shutdown driver.
    pub fn ShutdownXLOG(code: i32, arg: Datum<'static>);
    // `XLogPutNextOid`, `RequestXLogSwitch`, `XLogRestorePoint`,
    // `UpdateFullPageWrites` are now REAL in [`crate::control_funcs`]
    // (re-exported above), built on the ported WAL-insert path + the XLogCtl /
    // ControlFile shmem region; they are no longer in the deferred-driver list.

    /// `GetActiveWalLevelOnStandby()` — the wal_level a standby replays with.
    pub fn GetActiveWalLevelOnStandby() -> WalLevel;

    // --- BootStrapXLOG + its cross-subsystem in-memory updates ---
    /// `BootStrapXLOG(data_checksum_version)` — create the initial WAL +
    /// control file during `initdb`.
    pub fn BootStrapXLOG(data_checksum_version: u32);
    /// `TransamVariables->nextXid/nextOid; oidCount = 0` (xlog.c:5158-5160).
    pub fn SetTransamVariablesAtBootstrap(next_xid: FullTransactionId, next_oid: Oid);
    /// `MultiXactSetNextMXact(nextMulti, nextMultiOffset)` (multixact.c).
    pub fn MultiXactSetNextMXact(next_multi: MultiXactId, next_multi_offset: MultiXactOffset);
    /// `AdvanceOldestClogXid(oldestXid)` (clog.c).
    pub fn AdvanceOldestClogXid(oldest_xid: TransactionId);
    /// `SetTransactionIdLimit(oldestXid, oldestXidDB)` (varsup.c).
    pub fn SetTransactionIdLimit(oldest_xid: TransactionId, oldest_xid_db: Oid);
    /// `SetMultiXactIdLimit(oldestMulti, oldestMultiDB, is_startup)` (multixact.c).
    pub fn SetMultiXactIdLimit(oldest_multi: MultiXactId, oldest_multi_db: Oid, is_startup: bool);
    /// `SetCommitTsLimit(oldestXact, newestXact)` (commit_ts.c).
    pub fn SetCommitTsLimit(oldest_xact: TransactionId, newest_xact: TransactionId);
}

/// `GetWALAvailability(targetLSN)` — classify a WAL position's retention state.
/// The pure classification is [`retention::GetWALAvailability`]; this
/// process-singleton entry reads `XLogCtl`/GUC posture, which is the deferred
/// driver (DESIGN_DEBT.md `xlog-driver`).
pub fn GetWALAvailability(target_lsn: XLogRecPtr) -> WALAvailability {
    let _ = target_lsn;
    panic!("xlog driver not ported (xlog-driver debt): GetWALAvailability entry requires XLogCtl/GUC posture; use retention::GetWALAvailability with the values supplied")
}

/// `SetConfigOption(...)` as called from `ReadControlFile` (guc.c). Deferred to
/// the GUC subsystem (DESIGN_DEBT.md `xlog-driver`).
pub fn SetConfigOptionInternal(name: &str, value: &str) {
    let _ = (name, value);
    panic!("xlog driver not ported (xlog-driver debt): SetConfigOptionInternal requires the GUC subsystem")
}

/// `CreateCheckPoint(flags)` — the process-singleton checkpoint entry point.
/// The faithful body is [`checkpoint::CreateCheckPoint`], which operates on an
/// owned [`checkpoint::CheckpointState`]; the process-singleton state holder and
/// its cross-subsystem providers are the deferred driver (DESIGN_DEBT.md).
pub fn CreateCheckPoint(flags: i32) -> bool {
    let _ = flags;
    panic!("xlog driver not ported (xlog-driver debt): CreateCheckPoint process-singleton requires the XLogCtl shmem substrate; use checkpoint::CreateCheckPoint with an owned CheckpointState")
}

/// `CreateRestartPoint(flags)` — the process-singleton restartpoint entry point.
pub fn CreateRestartPoint(flags: i32) -> bool {
    let _ = flags;
    panic!("xlog driver not ported (xlog-driver debt): CreateRestartPoint process-singleton requires the XLogCtl shmem substrate; use checkpoint::CreateRestartPoint with an owned CheckpointState")
}

/// Install this crate's inward seams. `xlog_redo` is the `rm_redo` callback the
/// rmgr table (backend-access-transam-rmgr) installs across the access cycle;
/// it must be `set()` here or the recovery dispatch panics on the first XLOG
/// record. The remaining declarations in `backend-access-transam-xlog-seams`
/// front the still-deferred XLogCtl shmem driver and are installed as their
/// legs land.
pub fn init_seams() {
    use backend_access_transam_xlog_seams as s;
    s::xlog_redo::set(redo::xlog_redo);

    // `int wal_level` (xlog.c GUC) — the effective `wal_level` value. C reads the
    // global `int wal_level` directly; here it lives in the `wal_level` enum GUC
    // slot (boot_val WAL_LEVEL_REPLICA), whose stored int is the WalLevel ordinal
    // (minimal=0/replica=1/logical=2, catalog/pg_control.h order).
    s::wal_level::set(|| {
        let lvl = backend_utils_misc_guc_tables::vars::wal_level.read();
        match lvl {
            x if x == WalLevel::Minimal as i32 => WalLevel::Minimal,
            x if x == WalLevel::Replica as i32 => WalLevel::Replica,
            x if x == WalLevel::Logical as i32 => WalLevel::Logical,
            other => panic!("invalid wal_level GUC value {other}"),
        }
    });

    // XLogLogicalInfoActive() (xlog.h:126): `wal_level >= WAL_LEVEL_LOGICAL`.
    // Read by the commit-record path (RecordTransactionCommit) and procarray
    // horizon computation to decide whether logical-decoding info is logged.
    s::xlog_logical_info_active::set(|| {
        backend_utils_misc_guc_tables::vars::wal_level.read() >= WalLevel::Logical as i32
    });

    // XLogStandbyInfoActive() (xlog.h): `wal_level >= WAL_LEVEL_REPLICA`. Read
    // by the commit path to decide whether standby/hot-standby info is logged.
    s::xlog_standby_info_active::set(|| {
        backend_utils_misc_guc_tables::vars::wal_level.read() >= WalLevel::Replica as i32
    });

    // XLogArchivingActive() (xlog.h): `XLogArchiveMode > ARCHIVE_MODE_OFF`. Read
    // by the postmaster's LaunchMissingBackgroundProcesses to decide whether to
    // start the archiver. `archive_mode` enum GUC (ARCHIVE_MODE_OFF = 0).
    backend_postmaster_postmaster_seams::xlog_archiving_active::set(|| {
        backend_utils_misc_guc_tables::vars::XLogArchiveMode.read()
            > ArchiveMode::Off as i32
    });

    // XLogArchivingAlways() (xlog.h): `XLogArchiveMode == ARCHIVE_MODE_ALWAYS`.
    // Read by the same postmaster path (archiver may run on a standby when
    // archive_mode = always). ARCHIVE_MODE_ALWAYS = 2.
    backend_postmaster_postmaster_seams::xlog_archiving_always::set(|| {
        backend_utils_misc_guc_tables::vars::XLogArchiveMode.read()
            == ArchiveMode::Always as i32
    });

    // xlog.c-owned GUC variable accessors + assign hooks (max_wal_size /
    // min_wal_size / checkpoint_completion_target). The GUC machinery fires the
    // assign hooks during InitializeGUCOptions to seed CheckPointSegments.
    guc_state::install();

    // The remaining xlog.c-owned WAL-settings GUC variable accessors
    // (full_page_writes / wal_log_hints / wal_init_zero / wal_recycle /
    // log_checkpoints / track_wal_io_timing / archive_timeout /
    // wal_decode_buffer_size / wal_keep_size / max_slot_wal_keep_size /
    // commit_delay / commit_siblings / wal_retrieve_retry_interval /
    // wal_buffers / wal_segment_size / archive_command /
    // wal_consistency_checking / archive_mode / wal_compression / wal_level /
    // wal_sync_method). Read by C straight from each GUC slot.
    guc_vars::install();

    // LocalProcessControlFile(reset) (xlog.c:4908) — the single-user boot driver
    // (backend-tcop-postgres) reads pg_control through this seam before shmem
    // exists. Real body in [`crate::shmem`].
    backend_tcop_postgres_seams::local_process_control_file::set(shmem::LocalProcessControlFile);

    // Same LocalProcessControlFile(reset), exposed to the postmaster crash-restart
    // reaper (PostmasterStateMachine -> local_process_control_file(true),
    // statemachine.rs:299). The postmaster-side seam returns unit (the postmaster
    // FATAL-aborts on a control-file read failure, like C), so wrap the PgResult.
    backend_postmaster_postmaster_seams::local_process_control_file::set(|reset| {
        shmem::LocalProcessControlFile(reset).expect("LocalProcessControlFile failed");
    });

    // InitializeWalConsistencyChecking() (xlog.c:4846) — reapply the
    // wal_consistency_checking GUC after shared_preload_libraries are loaded
    // (so custom resource managers are known). Called from
    // PostgresSingleUserMain. Real body in [`crate::guc_vars`].
    backend_tcop_postgres_seams::initialize_wal_consistency_checking::set(|| {
        guc_vars::InitializeWalConsistencyChecking();
        Ok(())
    });

    // XLogCtl shmem position readers + control-file-backed predicates (xlog.c).
    s::get_redo_rec_ptr::set(shmem::GetRedoRecPtr);
    s::get_xlog_insert_rec_ptr::set(shmem::GetXLogInsertRecPtr);
    s::get_insert_rec_ptr::set(shmem::GetInsertRecPtr);
    s::get_flush_rec_ptr::set(shmem::GetFlushRecPtr);
    s::get_wal_insertion_timeline_if_set::set(shmem::GetWALInsertionTimeLineIfSet);
    s::get_system_identifier::set(shmem::GetSystemIdentifier);
    s::data_checksums_enabled::set(shmem::DataChecksumsEnabled);

    // `InRecovery` (declared in xlog.h, but the storage is the file-static global
    // in xlogrecovery.c — xlogrecovery.c:642 et al set it). The
    // `backend-access-transam-xlog-seams::in_recovery` forwarding declaration
    // delegates to the xlogrecovery owner's per-backend recovery-state read,
    // which xlogrecovery.c installs via its own `in_recovery` seam.
    s::in_recovery::set(backend_access_transam_xlogrecovery_seams::in_recovery::call);

    // The ipci shmem accumulator slots (XLOGShmemSize/XLOGShmemInit).
    s::xlog_shmem_size::set(shmem::xlog_shmem_size_seam);
    s::xlog_shmem_init::set(shmem::xlog_shmem_init_seam);

    // The WAL-insertion entry (xloginsert.c's XLogInsert calls this) + the
    // recovery predicate + the per-backend record-position globals it updates.
    s::xlog_insert_record::set(insert::XLogInsertRecord);
    s::recovery_in_progress::set(shmem::RecoveryInProgress);

    // The decision inputs xloginsert.c's XLogInsert / XLogBeginInsert read
    // before they hold an insertion lock (XLogInsertAllowed / full-page-write
    // info), plus the WAL-compression / consistency-checking GUC reads its
    // XLogRecordAssemble consults.
    s::xlog_insert_allowed::set(insert::XLogInsertAllowed);
    s::get_full_page_write_info::set(driver::GetFullPageWriteInfo);
    s::wal_compression::set(write::wal_compression);
    s::wal_consistency_checking::set(guc_vars::wal_consistency_checking);
    s::proc_last_rec_ptr::set(insert::proc_last_rec_ptr);
    s::xact_last_rec_end::set(insert::xact_last_rec_end);
    s::set_xact_last_rec_end::set(insert::set_xact_last_rec_end);
    s::set_xact_last_commit_end::set(insert::set_xact_last_commit_end);

    // WAL-write / fsync driver (xlog.c XLogWrite/XLogFlush/issue_xlog_fsync/
    // XLogFileInit) + the xlog.c-owned durability GUC reads consumed by fd.c.
    s::xlog_flush::set(write::XLogFlush);
    s::xlog_file_init::set(write::XLogFileInit);
    s::issue_xlog_fsync::set(write::issue_xlog_fsync);
    s::enable_fsync::set(write::enable_fsync);
    s::wal_sync_method::set(write::wal_sync_method);
    s::wal_segment_size::set(shmem::wal_segment_size);

    // xlog.c GUC-value reads consumed by the checkpointer aux-process main loop
    // (CheckArchiveTimeout / IsCheckpointOnSchedule). `XLogArchiveTimeout` is the
    // `archive_timeout` GUC; `CheckPointSegments` is the GUC-derived segment
    // budget (read as f64 for the float division in IsCheckpointOnSchedule).
    s::xlog_archive_timeout::set(write::xlog_archive_timeout);
    s::check_point_segments::set(write::check_point_segments_f64);
    // `XLogFilePath(tli, segno, wal_segsz_bytes)` — the seam reads the segment
    // size from the xlog global, so it drops the explicit byte-size argument.
    s::xlog_file_path::set(|tli, seg| XLogFilePath(tli, seg, shmem::wal_segment_size()));

    // `Is/Set/ResetInstallXLogFileSegmentActive` + `XLogShutdownWalRcv` live in
    // xlog.c and touch the xlog-owned `XLogCtl->InstallXLogFileSegmentActive`
    // flag under ControlFileLock. The recovery page-read driver reaches them
    // through the walreceiverfuncs-seams crate (where the seams were declared),
    // so install those entries here, in the real owner. The C return is void;
    // the lwlock/condvar `ereport(ERROR)` unwinds (here: PgError -> panic at the
    // void seam boundary, matching the longjmp).
    s::is_install_xlog_file_segment_active::set(write::IsInstallXLogFileSegmentActive);

    // XLogCtl shmem READ accessors + small in-memory setters (xlog.c), now
    // REAL in [`crate::driver`]. These front the checkpointer / bgwriter /
    // walwriter / xlogfuncs consumers.
    s::xlog_needs_flush::set(driver::XLogNeedsFlush);
    s::xlog_set_async_xact_lsn::set(driver::XLogSetAsyncXactLSN);
    s::xlog_set_replication_slot_minimum_lsn::set(driver::XLogSetReplicationSlotMinimumLSN);
    s::xlog_get_replication_slot_minimum_lsn::set(driver::XLogGetReplicationSlotMinimumLSN);
    s::xlog_get_last_removed_segno::set(driver::XLogGetLastRemovedSegno);
    s::xlog_get_oldest_segno::set(driver::XLogGetOldestSegno);
    s::get_xlog_write_rec_ptr::set(driver::GetXLogWriteRecPtr);
    s::get_fake_lsn_for_unlogged_rel::set(driver::GetFakeLSNForUnloggedRel);
    s::get_last_seg_switch_data::set(driver::GetLastSegSwitchData);
    s::get_last_important_rec_ptr::set(driver::GetLastImportantRecPtr);

    // The small WAL-record-emitting + control-file housekeeping functions, now
    // REAL in [`crate::control_funcs`] (built on the ported WAL-insert path +
    // the XLogCtl / ControlFile shmem region). These front varsup (XLogPutNextOid),
    // the checkpointer (RequestXLogSwitch / UpdateFullPageWrites), and the
    // recovery driver (ReachedEndOfBackup / AllowCascadeReplication).
    // `StartupXLOG()` — the WAL-engine startup driver (xlog.c). The startup
    // process reaches it through the xlog-seams `startup_xlog` slot.
    s::startup_xlog::set(startup::StartupXLOG);
    // COW-model re-seed of the cluster XID/multixact bounds from the control
    // file, called by the postmaster after the startup process succeeds so the
    // postmaster's fork-inherited copy of `TransamVariables`/`MultiXactState` is
    // valid before launcher/backend children take snapshots.
    s::seed_transam_variables_from_checkpoint::set(startup::SeedTransamVariablesFromCheckpoint);

    s::xlog_put_next_oid::set(control_funcs::XLogPutNextOid);
    s::request_xlog_switch::set(control_funcs::RequestXLogSwitch);
    s::update_full_page_writes::set(control_funcs::UpdateFullPageWrites);
    s::reached_end_of_backup::set(control_funcs::ReachedEndOfBackup);
    s::allow_cascade_replication::set(control_funcs::AllowCascadeReplication);
    {
        use backend_replication_walreceiverfuncs_seams as wf;
        wf::set_install_xlog_file_segment_active::set(|| {
            write::SetInstallXLogFileSegmentActive()
                .expect("SetInstallXLogFileSegmentActive failed")
        });
        wf::reset_install_xlog_file_segment_active::set(|| {
            write::ResetInstallXLogFileSegmentActive()
                .expect("ResetInstallXLogFileSegmentActive failed")
        });
        wf::xlog_shutdown_wal_rcv::set(|| {
            write::XLogShutdownWalRcv().expect("XLogShutdownWalRcv failed")
        });
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "insert_tests.rs"]
mod insert_tests;
