//! xlog.c (PostgreSQL 18.3): control file, XLogCtl, the WAL insert/write/
//! flush engine, and StartupXLOG. Recovery record-reading is xlogrecovery's;
//! record assembly is xloginsert's.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use std::cell::Cell;

use types_core::{pg_time_t, TimeLineID, XLogRecPtr, XLogSegNo};
use types_error::PgResult;

mod backup;
pub use backup::{
    do_pg_abort_backup, do_pg_backup_start, do_pg_backup_stop, get_backup_status,
    register_persistent_abort_backup_handler, SessionBackupState, TablespaceInfo,
};
pub mod control_file;
pub mod ctl;
pub mod flushpipe;
pub mod guc_vars;
pub mod insert;
pub mod redo;
pub(crate) mod removal;
pub use removal::{CheckXLogRemoved, RemoveNonParentXlogFiles, XLogGetOldestSegno};
pub mod startup;
pub mod write;

#[cfg(test)]
mod tests;

pub use control_file::{
    CheckPoint, ControlFileData, DataChecksumsEnabled, GetActiveWalLevelOnStandby,
    GetDefaultCharSignedness,
    control_file_mark_read_for_tests, GetMockAuthenticationNonce, GetSystemIdentifier,
    LocalProcessControlFile, ReadControlFile,
    UpdateControlFile,
};
pub use ctl::{GetWALInsertionTimeLineIfSet, XLOGShmemInit, XLOGShmemResetAfterCrash, XLOGShmemSize};
pub use insert::{
    GetFullPageWriteInfo, GetInsertRecPtr, GetLastImportantRecPtr, GetRedoRecPtr, GetXLogInsertRecPtr,
    RecoveryInProgress, XLogInsertAllowed, XLogInsertRecord,
};
pub use startup::{
    CreateCheckPoint, CreateRestartPoint, ReachedEndOfBackup, ResetInstallXLogFileSegmentActive,
    SetInstallXLogFileSegmentActive, ShutdownXLOG, StartupXLOG, SwitchIntoArchiveRecovery,
    UpdateFullPageWrites,
};
pub use write::{
    stamp_wal_sync_method, wal_flush_pacing_decide, GetFlushRecPtr, GetLastSegSwitchData,
    GetXLogWriteRecPtr, SetWalWriterSleeping, WalFlushPacing, XLogBackgroundFlush, XLogFileInit,
    XLogFileOpen, XLogFlush, XLogNeedsFlush, XLogSetAsyncXactLSN,
};

pub const InvalidXLogRecPtr: XLogRecPtr = 0;
pub const XLOG_BLCKSZ: usize = xlogreader_seams::XLOG_BLCKSZ;
pub const SizeOfXLogRecord: usize = 24;
pub const SizeOfXLogShortPHD: usize = 24;
pub const SizeOfXLogLongPHD: usize = 40;
pub const XLOG_PAGE_MAGIC: u16 = 0xD118;
pub const XLOGDIR: &str = "pg_wal";

pub const XLP_FIRST_IS_CONTRECORD: u16 = 0x0001;
pub const XLP_LONG_HEADER: u16 = 0x0002;
pub const XLP_BKP_REMOVABLE: u16 = 0x0004;
pub const XLP_FIRST_IS_OVERWRITE_CONTRECORD: u16 = 0x0008;

pub const XLR_INFO_MASK: u8 = 0x0F;
pub const XLOG_INCLUDE_ORIGIN: u8 = 0x01;
pub const XLOG_MARK_UNIMPORTANT: u8 = 0x02;

pub const RM_XLOG_ID: u8 = 0;

pub const XLOG_CHECKPOINT_SHUTDOWN: u8 = 0x00;
pub const XLOG_CHECKPOINT_ONLINE: u8 = 0x10;
pub const XLOG_NOOP: u8 = 0x20;
pub const XLOG_NEXTOID: u8 = 0x30;
pub const XLOG_SWITCH: u8 = 0x40;
pub const XLOG_BACKUP_END: u8 = 0x50;
pub const XLOG_PARAMETER_CHANGE: u8 = 0x60;
pub const XLOG_RESTORE_POINT: u8 = 0x70;
pub const XLOG_FPW_CHANGE: u8 = 0x80;
pub const XLOG_END_OF_RECOVERY: u8 = 0x90;
pub const XLOG_FPI_FOR_HINT: u8 = 0xA0;
pub const XLOG_FPI: u8 = 0xB0;
pub const XLOG_OVERWRITE_CONTRECORD: u8 = 0xD0;
pub const XLOG_CHECKPOINT_REDO: u8 = 0xE0;

pub const CHECKPOINT_IS_SHUTDOWN: i32 = 0x0001;
pub const CHECKPOINT_END_OF_RECOVERY: i32 = 0x0002;
pub const CHECKPOINT_IMMEDIATE: i32 = 0x0004;
pub const CHECKPOINT_FORCE: i32 = 0x0008;
pub const CHECKPOINT_FLUSH_ALL: i32 = 0x0010;
pub const CHECKPOINT_WAIT: i32 = 0x0020;
pub const CHECKPOINT_REQUESTED: i32 = 0x0040;
pub const CHECKPOINT_CAUSE_XLOG: i32 = 0x0080;
pub const CHECKPOINT_CAUSE_TIME: i32 = 0x0100;

pub type RecoveryState = i32;
pub const RECOVERY_STATE_CRASH: RecoveryState = 0;
pub const RECOVERY_STATE_ARCHIVE: RecoveryState = 1;
pub const RECOVERY_STATE_DONE: RecoveryState = 2;

pub type DBState = i32;
pub const DB_STARTUP: DBState = 0;
pub const DB_SHUTDOWNED: DBState = 1;
pub const DB_SHUTDOWNED_IN_RECOVERY: DBState = 2;
pub const DB_SHUTDOWNING: DBState = 3;
pub const DB_IN_CRASH_RECOVERY: DBState = 4;
pub const DB_IN_ARCHIVE_RECOVERY: DBState = 5;
pub const DB_IN_PRODUCTION: DBState = 6;

pub const WAL_LEVEL_MINIMAL: i32 = 0;
pub const WAL_LEVEL_REPLICA: i32 = 1;
pub const WAL_LEVEL_LOGICAL: i32 = 2;

pub const WAL_SYNC_METHOD_FSYNC: i32 = 0;
pub const WAL_SYNC_METHOD_FDATASYNC: i32 = 1;
pub const WAL_SYNC_METHOD_OPEN: i32 = 2;
pub const WAL_SYNC_METHOD_FSYNC_WRITETHROUGH: i32 = 3;
pub const WAL_SYNC_METHOD_OPEN_DSYNC: i32 = 4;

// xlogdefs.h DEFAULT_WAL_SYNC_METHOD: port/linux.h pins fdatasync; elsewhere
// O_DSYNC != O_SYNC selects open_datasync (macOS).
#[cfg(target_os = "linux")]
pub const DEFAULT_WAL_SYNC_METHOD: i32 = WAL_SYNC_METHOD_FDATASYNC;
#[cfg(not(target_os = "linux"))]
pub const DEFAULT_WAL_SYNC_METHOD: i32 = WAL_SYNC_METHOD_OPEN_DSYNC;

pub const NUM_XLOGINSERT_LOCKS: usize = 8;

pub const fn MAXALIGN(n: usize) -> usize {
    (n + 7) & !7
}
pub const fn MAXALIGN64(n: u64) -> u64 {
    (n + 7) & !7
}

pub fn wal_level() -> i32 {
    guc_tables::vars::wal_level.read()
}
pub fn XLogIsNeeded() -> bool {
    wal_level() >= WAL_LEVEL_REPLICA
}
pub fn XLogStandbyInfoActive() -> bool {
    wal_level() >= WAL_LEVEL_REPLICA
}
pub fn XLogLogicalInfoActive() -> bool {
    wal_level() >= WAL_LEVEL_LOGICAL
}
pub fn XLogArchivingActive() -> bool {
    guc_tables::vars::XLogArchiveMode.read() > 0
}
pub fn XLogArchivingAlways() -> bool {
    guc_tables::vars::XLogArchiveMode.read() == 2
}

// xlog_internal.h archive-status naming.
pub const MAXFNAMELEN: usize = 64;
pub const MIN_XFN_CHARS: usize = 16;
pub const MAX_XFN_CHARS: usize = 40;
pub const VALID_XFN_CHARS: &str = "0123456789ABCDEF.history.backup.partial";

pub fn StatusFilePath(xlog: &str, suffix: &str) -> String {
    format!("{XLOGDIR}/archive_status/{xlog}{suffix}")
}

pub fn IsTLHistoryFileName(fname: &str) -> bool {
    let b = fname.as_bytes();
    b.len() == 8 + ".history".len()
        && b[..8].iter().all(|c| c.is_ascii_digit() || (b'A'..=b'F').contains(c))
        && fname.ends_with(".history")
}

/// GetRecoveryState (xlog.c).
pub fn GetRecoveryState() -> RecoveryState {
    let ctl = ctl::XLogCtl();
    ctl.info_lck.with(|| ctl.SharedRecoveryState.load(Relaxed))
}

/// GetOldestRestartPoint (xlog.c): last-restartpoint redo location + TLI.
pub fn GetOldestRestartPoint() -> PgResult<(XLogRecPtr, TimeLineID)> {
    lwlock::LWLockAcquire(
        ctl::ControlFileLock(),
        lwlock::LW_SHARED,
        init_small::globals::MyProcNumber(),
    )?;
    let cf = control_file::control_file();
    let r = (cf.checkPointCopy.redo, cf.checkPointCopy.ThisTimeLineID);
    lwlock::LWLockRelease(ctl::ControlFileLock())?;
    Ok(r)
}

/// RequestXLogSwitch (xlog.c): XLOG SWITCH record, no data.
pub fn RequestXLogSwitch(mark_unimportant: bool) -> PgResult<XLogRecPtr> {
    let flags = if mark_unimportant { XLOG_MARK_UNIMPORTANT } else { 0 };
    xloginsert_seams::xlog_insert_with_flags::call(RM_XLOG_ID, XLOG_SWITCH, flags, &[])
}

// wal_segment_size + UsableBytesInSegment are fixed by ReadControlFile before
// any WAL access; cached here so per-record arithmetic is a plain load.
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering::Relaxed};
static WAL_SEGMENT_SIZE: AtomicI32 = AtomicI32::new(16 * 1024 * 1024);
static USABLE_BYTES_IN_SEGMENT: AtomicU64 = AtomicU64::new(
    (16 * 1024 * 1024 / XLOG_BLCKSZ as u64) * UsableBytesInPage
        - (SizeOfXLogLongPHD - SizeOfXLogShortPHD) as u64,
);

pub const UsableBytesInPage: u64 = (XLOG_BLCKSZ - SizeOfXLogShortPHD) as u64;

pub fn wal_segment_size() -> i32 {
    WAL_SEGMENT_SIZE.load(Relaxed)
}

pub(crate) fn set_wal_segment_size(size: i32) {
    WAL_SEGMENT_SIZE.store(size, Relaxed);
    let usable = (size as u64 / XLOG_BLCKSZ as u64) * UsableBytesInPage
        - (SizeOfXLogLongPHD - SizeOfXLogShortPHD) as u64;
    USABLE_BYTES_IN_SEGMENT.store(usable, Relaxed);
}

pub fn UsableBytesInSegment() -> u64 {
    USABLE_BYTES_IN_SEGMENT.load(Relaxed)
}

pub fn IsValidWalSegSize(size: i32) -> bool {
    size > 0 && (size & (size - 1)) == 0 && (1024 * 1024..=1024 * 1024 * 1024).contains(&size)
}

pub fn XLogSegmentsPerXLogId(wal_segsz: i32) -> u64 {
    0x1_0000_0000_u64 / wal_segsz as u64
}
pub fn XLogSegNoOffsetToRecPtr(segno: XLogSegNo, offset: u32, wal_segsz: i32) -> XLogRecPtr {
    segno * wal_segsz as u64 + offset as u64
}
pub fn XLogSegmentOffset(ptr: XLogRecPtr, wal_segsz: i32) -> u32 {
    (ptr & (wal_segsz as u64 - 1)) as u32
}
pub fn XLByteToSeg(ptr: XLogRecPtr, wal_segsz: i32) -> XLogSegNo {
    ptr / wal_segsz as u64
}
pub fn XLByteToPrevSeg(ptr: XLogRecPtr, wal_segsz: i32) -> XLogSegNo {
    (ptr - 1) / wal_segsz as u64
}
pub fn XLByteInPrevSeg(ptr: XLogRecPtr, segno: XLogSegNo, wal_segsz: i32) -> bool {
    XLByteToPrevSeg(ptr, wal_segsz) == segno
}
pub fn XLogMBVarToSegs(mbvar: i32, wal_segsz: i32) -> i32 {
    mbvar / (wal_segsz / (1024 * 1024))
}
pub fn XRecOffIsValid(ptr: XLogRecPtr) -> bool {
    ptr % XLOG_BLCKSZ as u64 >= SizeOfXLogShortPHD as u64
}
pub fn XLogRecPtrIsInvalid(ptr: XLogRecPtr) -> bool {
    ptr == InvalidXLogRecPtr
}

pub fn XLogFileName(tli: TimeLineID, segno: XLogSegNo, wal_segsz: i32) -> String {
    let per_id = XLogSegmentsPerXLogId(wal_segsz);
    format!("{tli:08X}{:08X}{:08X}", segno / per_id, segno % per_id)
}
pub fn XLogFilePath(tli: TimeLineID, segno: XLogSegNo, wal_segsz: i32) -> String {
    format!("{XLOGDIR}/{}", XLogFileName(tli, segno, wal_segsz))
}

pub const fn INSERT_FREESPACE(endptr: XLogRecPtr) -> usize {
    if endptr % XLOG_BLCKSZ as u64 == 0 {
        0
    } else {
        XLOG_BLCKSZ - (endptr % XLOG_BLCKSZ as u64) as usize
    }
}

pub fn XLogBytePosToRecPtr(bytepos: u64) -> XLogRecPtr {
    let usable_seg = UsableBytesInSegment();
    let fullsegs = bytepos / usable_seg;
    let mut bytesleft = bytepos % usable_seg;
    let seg_offset;
    if bytesleft < (XLOG_BLCKSZ - SizeOfXLogLongPHD) as u64 {
        seg_offset = bytesleft + SizeOfXLogLongPHD as u64;
    } else {
        bytesleft -= (XLOG_BLCKSZ - SizeOfXLogLongPHD) as u64;
        let fullpages = bytesleft / UsableBytesInPage;
        bytesleft %= UsableBytesInPage;
        seg_offset = XLOG_BLCKSZ as u64
            + fullpages * XLOG_BLCKSZ as u64
            + bytesleft
            + SizeOfXLogShortPHD as u64;
    }
    XLogSegNoOffsetToRecPtr(fullsegs, seg_offset as u32, wal_segment_size())
}

pub fn XLogBytePosToEndRecPtr(bytepos: u64) -> XLogRecPtr {
    let usable_seg = UsableBytesInSegment();
    let fullsegs = bytepos / usable_seg;
    let mut bytesleft = bytepos % usable_seg;
    let seg_offset;
    if bytesleft < (XLOG_BLCKSZ - SizeOfXLogLongPHD) as u64 {
        seg_offset = if bytesleft == 0 { 0 } else { bytesleft + SizeOfXLogLongPHD as u64 };
    } else {
        bytesleft -= (XLOG_BLCKSZ - SizeOfXLogLongPHD) as u64;
        let fullpages = bytesleft / UsableBytesInPage;
        bytesleft %= UsableBytesInPage;
        let mut off = XLOG_BLCKSZ as u64 + fullpages * XLOG_BLCKSZ as u64 + bytesleft;
        if bytesleft != 0 {
            off += SizeOfXLogShortPHD as u64;
        }
        seg_offset = off;
    }
    XLogSegNoOffsetToRecPtr(fullsegs, seg_offset as u32, wal_segment_size())
}

pub fn XLogRecPtrToBytePos(ptr: XLogRecPtr) -> u64 {
    let wal_segsz = wal_segment_size();
    let usable_seg = UsableBytesInSegment();
    let fullsegs = XLByteToSeg(ptr, wal_segsz);
    let fullpages = XLogSegmentOffset(ptr, wal_segsz) as u64 / XLOG_BLCKSZ as u64;
    let offset = ptr % XLOG_BLCKSZ as u64;
    if fullpages == 0 {
        let mut result = fullsegs * usable_seg;
        if offset > 0 {
            debug_assert!(offset >= SizeOfXLogLongPHD as u64);
            result += offset - SizeOfXLogLongPHD as u64;
        }
        result
    } else {
        let mut result = fullsegs * usable_seg
            + (XLOG_BLCKSZ - SizeOfXLogLongPHD) as u64
            + (fullpages - 1) * UsableBytesInPage;
        if offset > 0 {
            debug_assert!(offset >= SizeOfXLogShortPHD as u64);
            result += offset - SizeOfXLogShortPHD as u64;
        }
        result
    }
}

static CHECK_POINT_SEGMENTS: AtomicI32 = AtomicI32::new(0);
pub fn CheckPointSegments() -> i32 {
    CHECK_POINT_SEGMENTS.load(Relaxed)
}
pub fn CalculateCheckpointSegments() {
    let target = XLogMBVarToSegs(guc_tables::vars::max_wal_size_mb.read(), wal_segment_size())
        as f64
        / (1.0 + guc_tables::vars::CheckPointCompletionTarget.read());
    CHECK_POINT_SEGMENTS.store((target as i32).max(1), Relaxed);
}

pub fn XLogCheckpointNeeded(new_segno: XLogSegNo) -> bool {
    let old_segno = XLByteToSeg(insert::local_redo_rec_ptr(), wal_segment_size());
    new_segno >= old_segno + (CheckPointSegments() - 1) as u64
}

fn assign_max_wal_size(_newval: i32, _extra: Option<&guc_tables::GucHookExtra>) {
    CalculateCheckpointSegments();
}
fn assign_checkpoint_completion_target(
    _newval: f64,
    _extra: Option<&guc_tables::GucHookExtra>,
) {
    CalculateCheckpointSegments();
}
fn check_wal_segment_size_hook(
    newval: &mut i32,
    _extra: &mut Option<guc_tables::GucHookExtra>,
    _source: types_guc::GucSource,
) -> PgResult<bool> {
    Ok(IsValidWalSegSize(*newval))
}
fn XLOGChooseNumBuffers() -> i32 {
    let mut xbuffers = init_small::globals::NBuffers() / 32;
    xbuffers = xbuffers.min(wal_segment_size() / XLOG_BLCKSZ as i32);
    xbuffers.max(8)
}
fn check_wal_buffers_hook(
    newval: &mut i32,
    _extra: &mut Option<guc_tables::GucHookExtra>,
    _source: types_guc::GucSource,
) -> PgResult<bool> {
    if *newval == -1 {
        if guc_tables::vars::XLOGbuffers.read() == -1 {
            return Ok(true);
        }
        *newval = XLOGChooseNumBuffers();
    }
    if *newval < 4 {
        *newval = 4;
    }
    Ok(true)
}

// wal_consistency_checking[] (xlog.c): the per-rmgr flag array that the
// write side (XLogRecordAssemble) probes to force a consistency-check FPI.
// C keeps it as a process-global `bool *`; pgrust keeps it per-thread (each
// backend/startup thread owns its GUC state, as every C process does), and
// gates the hot-path read behind WCC_ANY so a record assembled with the GUC
// off pays a single bool load.
const RM_N_IDS: usize = 256;

// WCC_FLAGS / WCC_ANY live in the crate's existing WAL-insert thread_local!
// block below (with PROC_LAST_REC_PTR et al.) rather than their own block, so
// the TLS census counts one site for the two cells.

fn set_wal_consistency_checking(flags: [bool; RM_N_IDS]) {
    WCC_ANY.set(flags.iter().any(|&b| b));
    WCC_FLAGS.set(flags);
}

/// `wal_consistency_checking[rmid]` (xlog.c): true when consistency-check
/// full-page images must be forced for records of this resource manager.
#[inline]
pub fn wal_consistency_checking(rmid: u8) -> bool {
    if !WCC_ANY.get() {
        return false;
    }
    // SAFETY: thread-local Cell; reading one element without copying the whole
    // array. No aliasing: assignment goes through Cell::set (a full move).
    WCC_FLAGS.with(|c| unsafe { (*c.as_ptr())[rmid as usize] })
}

// check_wal_consistency_checking (xlog.c): parse the comma-separated rmgr-name
// list (or "all"), matching case-insensitively against the maskable resource
// managers (those with an rm_mask). The validated per-rmgr flag array is
// stashed as the GUC "extra" so assign can apply it atomically (C's guc_malloc
// extra). pgrust has no custom resource managers (no extension ABI, carve
// ruling), so C's process_shared_preload_libraries deferral collapses to a
// plain "Unrecognized key word" rejection here.
//
// Divergence from C: SplitIdentifierString's double-quoted-identifier syntax
// is not reproduced (rmgr names never need quoting); the unquoted comma list
// with surrounding whitespace, and its "List syntax is invalid." rejection of
// empty/dangling elements, are matched exactly.
fn parse_wal_consistency_checking(spec: &str) -> Result<[bool; RM_N_IDS], String> {
    let mut flags = [false; RM_N_IDS];
    let maskable: Vec<(&'static str, u8)> =
        if transam_xlog_seams::wal_consistency_maskable_rmgrs::is_installed() {
            transam_xlog_seams::wal_consistency_maskable_rmgrs::call()
        } else {
            Vec::new()
        };

    let tokens = match split_identifier_list(spec) {
        Some(t) => t,
        None => return Err("List syntax is invalid.".to_string()),
    };

    for tok in tokens {
        if tok.eq_ignore_ascii_case("all") {
            for &(_, rmid) in &maskable {
                flags[rmid as usize] = true;
            }
        } else if let Some(&(_, rmid)) =
            maskable.iter().find(|(name, _)| tok.eq_ignore_ascii_case(name))
        {
            flags[rmid as usize] = true;
        } else {
            return Err(format!("Unrecognized key word: \"{tok}\"."));
        }
    }
    Ok(flags)
}

// SplitIdentifierString(rawstring, ',', ...) restricted to unquoted ASCII
// tokens (varlena.c). Returns None on the syntax errors C's SplitIdentifierString
// rejects with `false`: an empty element (leading/trailing/doubled separator)
// or whitespace embedded inside a token. An all-whitespace or empty input is a
// valid empty list.
fn split_identifier_list(raw: &str) -> Option<Vec<&str>> {
    let s = raw.as_bytes();
    let is_space = |b: u8| b == b' ' || b == b'\t' || b == b'\n' || b == b'\r' || b == 0x0b || b == 0x0c;
    let mut names = Vec::new();
    let mut p = 0usize;
    while p < s.len() && is_space(s[p]) {
        p += 1;
    }
    if p == s.len() {
        return Some(names);
    }
    loop {
        let start = p;
        while p < s.len() && s[p] != b',' && !is_space(s[p]) {
            p += 1;
        }
        if p == start {
            // empty element (e.g. "a,," or leading ",")
            return None;
        }
        let tok = &raw[start..p];
        while p < s.len() && is_space(s[p]) {
            p += 1;
        }
        if p == s.len() {
            names.push(tok);
            return Some(names);
        }
        if s[p] != b',' {
            // whitespace inside a token ("a b")
            return None;
        }
        names.push(tok);
        p += 1;
        while p < s.len() && is_space(s[p]) {
            p += 1;
        }
    }
}

fn check_wal_consistency_checking_hook(
    newval: &mut Option<String>,
    extra: &mut Option<guc_tables::GucHookExtra>,
    _source: types_guc::GucSource,
) -> PgResult<bool> {
    match parse_wal_consistency_checking(newval.as_deref().unwrap_or("")) {
        Ok(flags) => {
            *extra = Some(Box::new(flags));
            Ok(true)
        }
        Err(detail) => {
            if guc_seams::guc_check_errdetail::is_installed() {
                guc_seams::guc_check_errdetail::call(detail);
            }
            Ok(false)
        }
    }
}

fn assign_wal_consistency_checking_hook(
    _newval: Option<&str>,
    extra: Option<&guc_tables::GucHookExtra>,
) {
    // C: wal_consistency_checking = extra. A missing/mistyped extra can only
    // happen on the boot assignment before a check ran; treat as all-false.
    let flags = extra
        .and_then(|e| e.downcast_ref::<[bool; RM_N_IDS]>())
        .copied()
        .unwrap_or([false; RM_N_IDS]);
    set_wal_consistency_checking(flags);
}

pub fn InitializeWalConsistencyChecking() -> PgResult<()> {
    // C re-runs the check for custom resource managers deferred during
    // shared_preload_libraries loading. pgrust has no extension ABI, so no
    // token is ever deferred and there is nothing to re-run here.
    Ok(())
}

thread_local! {
    pub(crate) static PROC_LAST_REC_PTR: Cell<XLogRecPtr> = const { Cell::new(0) };
    pub(crate) static XACT_LAST_REC_END: Cell<XLogRecPtr> = const { Cell::new(0) };
    pub(crate) static XACT_LAST_COMMIT_END: Cell<XLogRecPtr> = const { Cell::new(0) };
    // wal_consistency_checking[] (xlog.c): per-rmgr consistency-check flags,
    // derived from the GUC by assign_wal_consistency_checking_hook. WCC_ANY
    // gates the hot-path read (see wal_consistency_checking()).
    static WCC_FLAGS: Cell<[bool; RM_N_IDS]> = const { Cell::new([false; RM_N_IDS]) };
    static WCC_ANY: Cell<bool> = const { Cell::new(false) };
    // pgWalUsage (instrument.h); UnsafeCell so the per-record adds are bare
    // field increments (single-entry leaf accesses only).
    pub(crate) static WAL_USAGE: core::cell::UnsafeCell<types_core::instrument::WalUsage> = const {
        core::cell::UnsafeCell::new(types_core::instrument::WalUsage {
            wal_records: 0,
            wal_fpi: 0,
            wal_bytes: 0,
            wal_buffers_full: 0,
        })
    };
}

#[inline(always)]
pub(crate) fn wal_usage_update<R>(f: impl FnOnce(&mut types_core::instrument::WalUsage) -> R) -> R {
    // SAFETY: thread-local; callers' closures are leaves (no re-entry, no
    // escaping reference).
    WAL_USAGE.with(|s| f(unsafe { &mut *s.get() }))
}

pub fn WalUsageFpi() -> i64 {
    wal_usage_update(|wu| wu.wal_fpi)
}

pub fn pgWalUsage() -> types_core::instrument::WalUsage {
    wal_usage_update(|wu| *wu)
}

pub fn ProcLastRecPtr() -> XLogRecPtr {
    PROC_LAST_REC_PTR.get()
}
pub fn XactLastRecEnd() -> XLogRecPtr {
    XACT_LAST_REC_END.get()
}

pub(crate) fn now_pg_time() -> pg_time_t {
    // DST P2 (contract §1.2): libc::time -> pg_clock::wall_secs().
    pg_clock::wall_secs() as pg_time_t
}

pub fn init_seams() {
    use transam_xlog_seams as s;

    s::xlog_redo::set(redo::xlog_redo);
    s::data_checksums_enabled::set(DataChecksumsEnabled);
    s::xlog_flush::set(write::XLogFlush);
    // GL-FLUSHPIPE-1: the sync-commit durability wait (xact.c:1502's call
    // site, and only it) routes through the pipelined variant; disarmed it
    // falls straight into XLogFlush (one memoized bool read).
    s::xlog_flush_commit::set(write::XLogFlushPipelined);
    s::xlog_needs_flush::set(write::XLogNeedsFlush);
    s::count_ckpt_slru_written::set(startup::count_ckpt_slru_written);
    s::xlog_logical_info_active::set(XLogLogicalInfoActive);
    s::xlog_standby_info_active::set(XLogStandbyInfoActive);
    s::recovery_in_progress::set(insert::RecoveryInProgress);
    s::wal_usage_fpi::set(WalUsageFpi);
    s::wal_usage::set(pgWalUsage);
    s::get_flush_rec_ptr::set(write::get_flush_rec_ptr_seam);
    s::wal_segment_size::set(wal_segment_size);
    s::xact_last_rec_end::set(XactLastRecEnd);
    s::set_xact_last_rec_end::set(|lsn| XACT_LAST_REC_END.set(lsn));
    s::set_xact_last_commit_end::set(|lsn| XACT_LAST_COMMIT_END.set(lsn));
    s::xact_last_commit_end::set(|| XACT_LAST_COMMIT_END.get());
    s::xlog_set_async_xact_lsn::set(write::XLogSetAsyncXactLSN);
    s::startup_xlog::set(startup::StartupXLOG);
    s::shutdown_xlog::set(startup::shutdown_xlog_seam);
    s::get_redo_rec_ptr::set(insert::GetRedoRecPtr);
    s::xlog_insert_record::set(insert::xlog_insert_record_seam);
    s::xlog_insert_allowed::set(insert::XLogInsertAllowed);
    s::get_full_page_write_info::set(insert::GetFullPageWriteInfo);
    s::xlog_put_next_oid::set(startup::XLogPutNextOid);
    s::initialize_wal_consistency_checking::set(InitializeWalConsistencyChecking);

    guc_tables::hooks::assign_max_wal_size.install(assign_max_wal_size);
    guc_tables::hooks::assign_checkpoint_completion_target
        .install(assign_checkpoint_completion_target);
    guc_tables::hooks::check_wal_segment_size.install(check_wal_segment_size_hook);
    guc_tables::hooks::check_wal_buffers.install(check_wal_buffers_hook);
    guc_tables::hooks::assign_wal_sync_method.install(write::assign_wal_sync_method);
    guc_tables::hooks::check_wal_consistency_checking
        .install(check_wal_consistency_checking_hook);
    guc_tables::hooks::assign_wal_consistency_checking
        .install(assign_wal_consistency_checking_hook);
    guc_vars::install_wal_consistency_checking_string();
    guc_vars::install_xlog_archive_command();
    guc_tables::option_sets::wal_sync_method_options.install(guc_vars::WAL_SYNC_METHOD_OPTIONS);
    guc_tables::option_sets::archive_mode_options.install(guc_vars::ARCHIVE_MODE_OPTIONS);
    guc_vars::install();
    guc_vars::install_wal_segment_size();
    guc_vars::install_checkpoint_completion_target();
}
