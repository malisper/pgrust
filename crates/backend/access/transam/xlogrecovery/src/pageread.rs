//! The WAL page-read driver: `XLogPageRead` (the reader's `page_read`
//! callback), `WaitForWALToBecomeAvailable` (the standby source state machine),
//! `XLogFileRead` / `XLogFileReadAnyTLI` (segment-file open from
//! archive/pg_wal/stream), `rescanLatestTimeLine`, and the page-read file-static
//! globals (`readFile`/`readSegNo`/`readOff`/`readLen`/`readSource`/
//! `lastSourceFailed`/`flushedUpto`/`receiveTLI`/`curFileTLI`/`InRedo` and the
//! `XLogPageReadPrivate` per-read scratch).
//!
//! Ported 1:1 from `src/backend/access/transam/xlogrecovery.c`
//! (lines 3320-4052, 4148-4233, 4235-4410).
//!
//! `XLogPageRead` is installed as the recovery reader's `routine.page_read`
//! (`XLogPageReadCB` = `fn(&mut XLogReaderState, ...) -> PgResult<i32>`), so it
//! cannot receive `&mut XLogRecoveryState` as a parameter. In C it is a `static`
//! function in the same translation unit that reads file-statics directly; here
//! the page-read read-cursor statics live as thread-locals (single startup
//! process, matching C), and the recovery mode/option globals are reached
//! through a raw `*mut XLogRecoveryState` thread-local that the startup process
//! installs for the duration of recovery — the same single-writer raw-pointer
//! idiom `shmem.rs::ctl_ptr` uses for the recovery shmem block.
//!
//! Streaming-source legs (walreceiver start/stop/flush, KnownAssignedXids idle
//! maintenance, RequestCheckpoint, latch wait) reach not-yet-ported owners
//! through their `-seams` crates and mirror-pg-and-panic until those owners
//! land — the sanctioned seam-and-panic boundary.

extern crate std;

use core::any::Any;
use core::cell::Cell;

use alloc::format;
use alloc::string::String;

use ::types_core::{TimeLineID, TimestampTz, XLogRecPtr, XLogSegNo};
use ::types_core::InvalidXLogRecPtr;
use ::types_error::{ErrorLevel, ErrorLocation, PgError, DEBUG1, DEBUG2, LOG, PANIC};
use ::wal::rmgr::{XLogReaderState, XLREAD_FAIL, XLREAD_WOULDBLOCK};
use ::wal::xlog_consts::XLOG_BLCKSZ;
use ::wal::xlogrecovery_carriers::{XLogPageReadResult, XLogSource};

use ::utils_error::elog;
use ::utils_error::ereport;

use crate::core::{lsn_fmt, RecoveryTargetTimeLineGoal, XLogRecoveryState};

use timeline_seams as timeline_seam;
use transam_xlog_seams as xlog_seam;
use xlogarchive_seams as xlogarchive_seam;
use xlogprefetcher_seams as prefetcher_seam;
use checkpointer_seams as checkpointer_seam;
use startup_seams as startup_seam;
use walreceiver_seams as walreceiver_seam;
use walreceiverfuncs_seams as walrcv_seam;
use fd_seams as fd_seam;
use procarray_seams as procarray_seam;
use pgstat_io_seams as pgstat_io_seam;
use timestamp_seams as timestamp_seam;
use init_small_seams as init_small_seam;
use more_seams as ps_seam;

#[inline]
fn loc(lineno: i32, func: &str) -> ErrorLocation {
    ErrorLocation::new("xlogrecovery.c", lineno, func)
}

/// `CHECKPOINT_CAUSE_XLOG` (xlog.h) — checkpoint requested because we've
/// consumed too much WAL since the last one.
const CHECKPOINT_CAUSE_XLOG: i32 = 1 << 8;

/// `WL_LATCH_SET` / `WL_TIMEOUT` / `WL_EXIT_ON_PM_DEATH` (latch.h).
const WL_LATCH_SET: u32 = 1 << 0;
const WL_TIMEOUT: u32 = 1 << 3;
const WL_EXIT_ON_PM_DEATH: u32 = 1 << 5;

/// `WAIT_EVENT_RECOVERY_RETRIEVE_RETRY_INTERVAL` / `WAIT_EVENT_RECOVERY_WAL_STREAM`
/// (wait_event_types.h). The exact codes are owned by the wait-event registry;
/// recovery only passes them through to `WaitLatch`.
const WAIT_EVENT_RECOVERY_RETRIEVE_RETRY_INTERVAL: u32 = 0;
const WAIT_EVENT_RECOVERY_WAL_STREAM: u32 = 0;

// ===========================================================================
// Page-read file-static globals (xlogrecovery.c:126, 232-265). These are C
// process-local `static`s touched by the page-read driver. The startup process
// is the single-threaded reader of WAL during recovery, exactly as C's
// file-statics assume, so they are thread-locals.
// ===========================================================================

std::thread_local! {
    /// `static int readFile = -1;` (xlogrecovery.c:232).
    static READ_FILE: Cell<i32> = const { Cell::new(-1) };
    /// `static XLogSegNo readSegNo = 0;` (xlogrecovery.c:233).
    static READ_SEG_NO: Cell<XLogSegNo> = const { Cell::new(0) };
    /// `static uint32 readOff = 0;` (xlogrecovery.c:234).
    static READ_OFF: Cell<u32> = const { Cell::new(0) };
    /// `static uint32 readLen = 0;` (xlogrecovery.c:235).
    static READ_LEN: Cell<u32> = const { Cell::new(0) };
    /// `static XLogSource readSource = XLOG_FROM_ANY;` (xlogrecovery.c:236).
    static READ_SOURCE: Cell<XLogSource> = const { Cell::new(XLogSource::Any) };
    /// `static bool lastSourceFailed = false;` (xlogrecovery.c:249).
    static LAST_SOURCE_FAILED: Cell<bool> = const { Cell::new(false) };
    /// `static XLogRecPtr flushedUpto = 0;` (xlogrecovery.c:264).
    static FLUSHED_UPTO: Cell<XLogRecPtr> = const { Cell::new(0) };
    /// `static TimeLineID receiveTLI = 0;` (xlogrecovery.c:265).
    static RECEIVE_TLI: Cell<TimeLineID> = const { Cell::new(0) };
    /// `static TimeLineID curFileTLI;` (xlogrecovery.c:126).
    static CUR_FILE_TLI: Cell<TimeLineID> = const { Cell::new(0) };
    /// `bool InRecovery`/`InRedo` accounting for `RestoreArchivedFile`'s
    /// `cleanupEnabled` arg (xlogrecovery.c uses the file-static `InRedo`).
    static IN_REDO: Cell<bool> = const { Cell::new(false) };
    /// `WaitForWALToBecomeAvailable`'s `static TimestampTz last_fail_time = 0`
    /// (xlogrecovery.c:3580).
    static LAST_FAIL_TIME: Cell<TimestampTz> = const { Cell::new(0) };
    /// The recovery state, installed for the duration of recovery by the startup
    /// process. Null outside recovery; the page-read driver dereferences it as
    /// C reads its file-static mode/option globals.
    static RECOVERY_STATE: Cell<*mut XLogRecoveryState> =
        const { Cell::new(core::ptr::null_mut()) };
}

// --- read-cursor accessors used by readrecord.rs to reconcile st mirrors ---

#[inline]
pub(crate) fn read_source() -> XLogSource {
    READ_SOURCE.with(Cell::get)
}
#[inline]
pub(crate) fn set_read_source(s: XLogSource) {
    READ_SOURCE.with(|c| c.set(s));
}
#[inline]
pub(crate) fn last_source_failed() -> bool {
    LAST_SOURCE_FAILED.with(Cell::get)
}
#[inline]
pub(crate) fn set_last_source_failed(v: bool) {
    LAST_SOURCE_FAILED.with(|c| c.set(v));
}
#[inline]
pub(crate) fn flushed_upto() -> XLogRecPtr {
    FLUSHED_UPTO.with(Cell::get)
}
#[inline]
pub(crate) fn receive_tli() -> TimeLineID {
    RECEIVE_TLI.with(Cell::get)
}
#[inline]
pub(crate) fn set_in_redo(v: bool) {
    IN_REDO.with(|c| c.set(v));
}

/// Install (or clear, with a null pointer) the recovery-state pointer the
/// page-read driver reads its mode/option globals through. The startup process
/// brackets recovery with this; matches C's file-static lifetime.
///
/// # Safety
/// `st` must outlive the recovery replay loop (the startup process owns the
/// single `XLogRecoveryState` for the whole of recovery and is single-threaded).
pub(crate) fn set_recovery_state_ptr(st: *mut XLogRecoveryState) {
    RECOVERY_STATE.with(|c| c.set(st));
}

/// Borrow the installed recovery state. Panics (the C NULL deref) if the driver
/// runs before the startup process installed it.
#[inline]
#[allow(clippy::mut_from_ref)]
fn recovery_state() -> &'static mut XLogRecoveryState {
    let p = RECOVERY_STATE.with(Cell::get);
    debug_assert!(!p.is_null(), "page-read driver ran before recovery state install");
    // SAFETY: single-threaded startup process; `p` points at the live
    // `XLogRecoveryState` the startup process owns for the whole of recovery.
    unsafe { &mut *p }
}

/// `XLogPageReadPrivate` (xlogrecovery.c:196) — per-read scratch the recovery
/// driver stores in `reader->private_data` before driving a record read.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct XLogPageReadPrivate {
    /// `int emode`.
    pub emode: ErrorLevel,
    /// `bool fetching_ckpt`.
    pub fetching_ckpt: bool,
    /// `bool randAccess`.
    pub rand_access: bool,
    /// `TimeLineID replayTLI`.
    pub replay_tli: TimeLineID,
}

/// Read `reader->private_data` as the `XLogPageReadPrivate` the recovery driver
/// stored there (xlogrecovery.c:3323).
fn page_read_private(reader: &XLogReaderState<'_>) -> XLogPageReadPrivate {
    let any: &dyn Any = reader
        .private_data
        .as_ref()
        .expect("XLogPageReadPrivate must be installed on the recovery reader")
        .as_ref();
    *any
        .downcast_ref::<XLogPageReadPrivate>()
        .expect("reader->private_data is XLogPageReadPrivate")
}

// ===========================================================================
// Segment arithmetic (xlog_internal.h). Pure; mirrors the inlined helpers in
// readrecord.rs.
// ===========================================================================

#[inline]
fn byte_to_seg(ptr: XLogRecPtr, wal_segment_size: i32) -> XLogSegNo {
    ptr / wal_segment_size as u64
}

#[inline]
fn byte_in_seg(ptr: XLogRecPtr, segno: XLogSegNo, wal_segment_size: i32) -> bool {
    ptr / wal_segment_size as u64 == segno
}

#[inline]
fn xlog_segment_offset(ptr: XLogRecPtr, wal_segment_size: i32) -> u32 {
    (ptr % wal_segment_size as u64) as u32
}

#[inline]
fn xlog_file_name(tli: TimeLineID, log_seg_no: XLogSegNo, wal_segment_size: i32) -> String {
    let per: u64 = 0x1_0000_0000_u64 / wal_segment_size as u64;
    format!(
        "{:08X}{:08X}{:08X}",
        tli,
        (log_seg_no / per) as u32,
        (log_seg_no % per) as u32
    )
}

// ===========================================================================
// XLogPageRead (xlogrecovery.c:3320).
// ===========================================================================

/// `static int XLogPageRead(XLogReaderState *xlogreader, XLogRecPtr
/// targetPagePtr, int reqLen, XLogRecPtr targetRecPtr, char *readBuf)`
/// (xlogrecovery.c:3320). The reader's `page_read` callback. Reads the
/// requested page into `reader.readBuf`, returning the valid byte count or an
/// `XLREAD_*` sentinel on failure.
pub(crate) fn x_log_page_read(
    reader: &mut XLogReaderState<'_>,
    target_page_ptr: XLogRecPtr,
    req_len: i32,
    target_rec_ptr: XLogRecPtr,
) -> Result<i32, PgError> {
    let private = page_read_private(reader);
    let emode = private.emode;

    let wal_segment_size = xlog_seam::wal_segment_size::call();
    let target_page_off = xlog_segment_offset(target_page_ptr, wal_segment_size);

    // See if we need to switch to a new segment because the requested record
    // is not in the currently open one.
    if READ_FILE.with(Cell::get) >= 0
        && !byte_in_seg(target_page_ptr, READ_SEG_NO.with(Cell::get), wal_segment_size)
    {
        // Request a restartpoint if we've replayed too much xlog since the last
        // one.
        if recovery_state().archive_recovery_requested
            && init_small_seam::is_under_postmaster::call()
        {
            let read_seg_no = READ_SEG_NO.with(Cell::get);
            if xlog_seam::xlog_checkpoint_needed::call(read_seg_no) {
                let _ = xlog_seam::get_redo_rec_ptr::call();
                if xlog_seam::xlog_checkpoint_needed::call(read_seg_no) {
                    checkpointer_seam::request_checkpoint::call(CHECKPOINT_CAUSE_XLOG);
                }
            }
        }

        close_read_file();
        READ_FILE.with(|c| c.set(-1));
        READ_SOURCE.with(|c| c.set(XLogSource::Any));
    }

    READ_SEG_NO.with(|c| c.set(byte_to_seg(target_page_ptr, wal_segment_size)));

    // retry:
    loop {
        // See if we need to retrieve more data.
        if READ_FILE.with(Cell::get) < 0
            || (READ_SOURCE.with(Cell::get) == XLogSource::Stream
                && FLUSHED_UPTO.with(Cell::get) < target_page_ptr + req_len as u64)
        {
            if READ_FILE.with(Cell::get) >= 0
                && reader.nonblocking
                && READ_SOURCE.with(Cell::get) == XLogSource::Stream
                && FLUSHED_UPTO.with(Cell::get) < target_page_ptr + req_len as u64
            {
                return Ok(XLREAD_WOULDBLOCK);
            }

            match wait_for_wal_to_become_available(
                target_page_ptr + req_len as u64,
                private.rand_access,
                private.fetching_ckpt,
                target_rec_ptr,
                private.replay_tli,
                reader.EndRecPtr,
                reader.nonblocking,
            )? {
                XLogPageReadResult::WouldBlock => return Ok(XLREAD_WOULDBLOCK),
                XLogPageReadResult::Fail => {
                    close_read_file();
                    READ_FILE.with(|c| c.set(-1));
                    READ_LEN.with(|c| c.set(0));
                    READ_SOURCE.with(|c| c.set(XLogSource::Any));
                    return Ok(XLREAD_FAIL);
                }
                XLogPageReadResult::Success => {}
            }
        }

        // At this point, we have the right segment open and if we're streaming
        // we know the requested record is in it.
        debug_assert!(READ_FILE.with(Cell::get) != -1);

        // If the current segment is being streamed from the primary, calculate
        // how much of the current page we have received already.
        if READ_SOURCE.with(Cell::get) == XLogSource::Stream {
            let flushed = FLUSHED_UPTO.with(Cell::get);
            if (target_page_ptr / XLOG_BLCKSZ as u64) != (flushed / XLOG_BLCKSZ as u64) {
                READ_LEN.with(|c| c.set(XLOG_BLCKSZ as u32));
            } else {
                READ_LEN.with(|c| {
                    c.set(xlog_segment_offset(flushed, wal_segment_size) - target_page_off)
                });
            }
        } else {
            READ_LEN.with(|c| c.set(XLOG_BLCKSZ as u32));
        }

        // Read the requested page.
        READ_OFF.with(|c| c.set(target_page_off));

        let io_start = pgstat_io_seam::pgstat_prepare_io_time::call();

        let fd = READ_FILE.with(Cell::get);
        let read_off = READ_OFF.with(Cell::get);
        // Ensure the reader's page buffer exists and is XLOG_BLCKSZ bytes.
        ensure_read_buf(reader);
        let buf = reader
            .readBuf
            .as_mut()
            .expect("readBuf allocated above");
        let r = fd_seam::pg_pread::call(fd, &mut buf[..XLOG_BLCKSZ], read_off as i64);

        if r != XLOG_BLCKSZ as isize {
            let save_errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);

            pgstat_io_seam::pgstat_count_io_op_time_wal_read::call(io_start, if r > 0 { r as u32 } else { 0 });

            let cur_file_tli = CUR_FILE_TLI.with(Cell::get);
            let read_seg_no = READ_SEG_NO.with(Cell::get);
            let fname = xlog_file_name(cur_file_tli, read_seg_no, wal_segment_size);
            let level = crate::readrecord::emode_for_corrupt_record(
                recovery_state(),
                emode,
                target_page_ptr + req_len as u64,
            );
            if r < 0 {
                let _ = ereport(level)
                    .errmsg(format!(
                        "could not read from WAL segment {}, LSN {}, offset {}: {}",
                        fname,
                        lsn_fmt(target_page_ptr),
                        read_off,
                        std::io::Error::from_raw_os_error(save_errno)
                    ))
                    .finish(loc(3443, "XLogPageRead"))?;
            } else {
                let _ = ereport(level)
                    .errmsg(format!(
                        "could not read from WAL segment {}, LSN {}, offset {}: read {} of {}",
                        fname,
                        lsn_fmt(target_page_ptr),
                        read_off,
                        r,
                        XLOG_BLCKSZ
                    ))
                    .finish(loc(3450, "XLogPageRead"))?;
            }
            // goto next_record_is_invalid;
            match next_record_is_invalid(reader) {
                NextRecord::Retry => continue,
                NextRecord::Return(code) => return Ok(code),
            }
        }

        pgstat_io_seam::pgstat_count_io_op_time_wal_read::call(io_start, r as u32);

        reader.seg.ws_tli = CUR_FILE_TLI.with(Cell::get);

        // Check the page header immediately so we can retry immediately if it's
        // not valid (the recycled-segment contrecord corner case). Only in
        // standby mode; otherwise ReadPageInternal validates.
        if recovery_state().standby_mode
            && (target_page_ptr % wal_segment_size as u64) == 0
        {
            let valid = {
                let page = reader.readBuf.as_ref().expect("readBuf set").clone();
                xlogreader::XLogReaderValidatePageHeader(
                    reader,
                    target_page_ptr,
                    &page[..XLOG_BLCKSZ],
                )
            };
            if !valid {
                if let Some(buf) = reader.errormsg_buf.as_ref() {
                    if !buf.as_str().is_empty() {
                        let msg = String::from(buf.as_str());
                        let level = crate::readrecord::emode_for_corrupt_record(
                            recovery_state(),
                            emode,
                            reader.EndRecPtr,
                        );
                        let _ = ereport(level)
                            .errmsg_internal(msg)
                            .finish(loc(3508, "XLogPageRead"))?;
                    }
                }
                xlogreader::XLogReaderResetError(reader);
                match next_record_is_invalid(reader) {
                    NextRecord::Retry => continue,
                    NextRecord::Return(code) => return Ok(code),
                }
            }
        }

        return Ok(READ_LEN.with(Cell::get) as i32);
    }
}

enum NextRecord {
    Retry,
    Return(i32),
}

/// `next_record_is_invalid:` (xlogrecovery.c:3517) — the page-read cleanup +
/// standby-retry / give-up tail.
fn next_record_is_invalid(reader: &mut XLogReaderState<'_>) -> NextRecord {
    // If we're reading ahead, give up fast.
    if reader.nonblocking {
        return NextRecord::Return(XLREAD_WOULDBLOCK);
    }

    LAST_SOURCE_FAILED.with(|c| c.set(true));

    close_read_file();
    READ_FILE.with(|c| c.set(-1));
    READ_LEN.with(|c| c.set(0));
    READ_SOURCE.with(|c| c.set(XLogSource::Any));

    if recovery_state().standby_mode {
        NextRecord::Retry
    } else {
        NextRecord::Return(XLREAD_FAIL)
    }
}

/// `close(readFile)` on the open WAL segment fd, if any.
fn close_read_file() {
    let fd = READ_FILE.with(Cell::get);
    if fd >= 0 {
        fd_seam::close_fd::call(fd);
    }
}

/// `if (readFile >= 0) { close(readFile); readFile = -1; }` — the orchestrators'
/// (FinishWalRecovery / ShutdownWalRecovery) ending-segment close.
pub(crate) fn close_read_file_pub() {
    close_read_file();
    READ_FILE.with(|c| c.set(-1));
}

/// Ensure `reader.readBuf` is allocated to at least `XLOG_BLCKSZ` bytes. The
/// reader pallocs this in C (`XLogReaderAllocate`); here the recovery reader's
/// arena owns it.
fn ensure_read_buf(reader: &mut XLogReaderState<'_>) {
    let need = reader
        .readBuf
        .as_ref()
        .map(|b| b.len() < XLOG_BLCKSZ)
        .unwrap_or(true);
    if need {
        let arena = reader
            .decode_arena
            .as_ref()
            .copied()
            .expect("recovery reader has a decode arena");
        let mut v = mcx::vec_with_capacity_in(arena, XLOG_BLCKSZ)
            .expect("XLOG_BLCKSZ readBuf fits");
        v.resize(XLOG_BLCKSZ, 0u8);
        reader.readBuf = Some(v);
    }
}

// ===========================================================================
// WaitForWALToBecomeAvailable (xlogrecovery.c:3574).
// ===========================================================================

/// `static XLogPageReadResult WaitForWALToBecomeAvailable(...)`
/// (xlogrecovery.c:3574) — the standby-mode source state machine.
fn wait_for_wal_to_become_available(
    rec_ptr: XLogRecPtr,
    rand_access: bool,
    fetching_ckpt: bool,
    tli_rec_ptr: XLogRecPtr,
    replay_tli: TimeLineID,
    replay_lsn: XLogRecPtr,
    nonblocking: bool,
) -> Result<XLogPageReadResult, PgError> {
    let mut streaming_reply_sent = false;

    let st = recovery_state();

    // Establish the entry currentSource per the state machine preamble.
    if !st.in_archive_recovery {
        crate::shmem::set_current_source(XLogSource::PgWal);
    } else if crate::shmem::current_source() == XLogSource::Any
        || (!st.standby_mode && crate::shmem::current_source() == XLogSource::Stream)
    {
        LAST_SOURCE_FAILED.with(|c| c.set(false));
        crate::shmem::set_current_source(XLogSource::Archive);
    }

    loop {
        let st = recovery_state();
        let old_source = crate::shmem::current_source();
        let mut start_wal_receiver = false;

        // First, advance the state machine if the current source failed.
        if LAST_SOURCE_FAILED.with(Cell::get) {
            if nonblocking {
                return Ok(XLogPageReadResult::WouldBlock);
            }

            match crate::shmem::current_source() {
                XLogSource::Archive | XLogSource::PgWal => {
                    // Check to see if promotion is requested.
                    if st.standby_mode && crate::promote::check_for_standby_trigger(st) {
                        walrcv_seam::xlog_shutdown_wal_rcv::call();
                        return Ok(XLogPageReadResult::Fail);
                    }
                    if !st.standby_mode {
                        return Ok(XLogPageReadResult::Fail);
                    }
                    crate::shmem::set_current_source(XLogSource::Stream);
                    start_wal_receiver = true;
                }
                XLogSource::Stream => {
                    debug_assert!(st.standby_mode);

                    if walrcv_seam::wal_rcv_streaming::call()? {
                        walrcv_seam::xlog_shutdown_wal_rcv::call();
                    } else {
                        walrcv_seam::reset_install_xlog_file_segment_active::call();
                    }

                    if st.recovery_target_timeline_goal == RecoveryTargetTimeLineGoal::Latest
                        && rescan_latest_timeline(st, replay_tli, replay_lsn)?
                    {
                        crate::shmem::set_current_source(XLogSource::Archive);
                    } else {
                        // Sleep wal_retrieve_retry_interval to avoid busy-waiting.
                        let retry = walreceiver_seam::wal_retrieve_retry_interval::call();
                        let mut now = timestamp_seam::get_current_timestamp::call();
                        let last_fail = LAST_FAIL_TIME.with(Cell::get);
                        if !timestamp_seam::timestamp_difference_exceeds::call(last_fail, now, retry) {
                            let wait_time = retry as i64
                                - timestamp_seam::timestamp_difference_milliseconds::call(
                                    last_fail, now,
                                );
                            let _ = elog(
                                LOG,
                                &format!(
                                    "waiting for WAL to become available at {}",
                                    lsn_fmt(rec_ptr)
                                ),
                            );
                            procarray_seam::known_assigned_transaction_ids_idle_maintenance::call();
                            recovery_wait_latch(
                                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                                wait_time,
                                WAIT_EVENT_RECOVERY_RETRIEVE_RETRY_INTERVAL,
                            )?;
                            now = timestamp_seam::get_current_timestamp::call();
                            startup_proc_interrupts()?;
                        }
                        LAST_FAIL_TIME.with(|c| c.set(now));
                        crate::shmem::set_current_source(XLogSource::Archive);
                    }
                }
                XLogSource::Any => {
                    return Err(PgError::error(&format!(
                        "unexpected WAL source {:?}",
                        crate::shmem::current_source()
                    )));
                }
            }
        } else if crate::shmem::current_source() == XLogSource::PgWal {
            // We prefer the archive over pg_wal, so try the archive next.
            if st.in_archive_recovery {
                crate::shmem::set_current_source(XLogSource::Archive);
            }
        }

        if crate::shmem::current_source() != old_source {
            let _ = elog(
                DEBUG2,
                &format!(
                    "switched WAL source from {:?} to {:?} after {}",
                    old_source,
                    crate::shmem::current_source(),
                    if LAST_SOURCE_FAILED.with(Cell::get) {
                        "failure"
                    } else {
                        "success"
                    }
                ),
            );
        }

        // We've handled possible failure. Try to read from the chosen source.
        LAST_SOURCE_FAILED.with(|c| c.set(false));

        match crate::shmem::current_source() {
            XLogSource::Archive | XLogSource::PgWal => {
                // Close any old file we might have open.
                if READ_FILE.with(Cell::get) >= 0 {
                    close_read_file();
                    READ_FILE.with(|c| c.set(-1));
                }
                if rand_access {
                    CUR_FILE_TLI.with(|c| c.set(0));
                }

                let read_seg_no = READ_SEG_NO.with(Cell::get);
                let source = if crate::shmem::current_source() == XLogSource::Archive {
                    XLogSource::Any
                } else {
                    crate::shmem::current_source()
                };
                let fd = x_log_file_read_any_tli(st, read_seg_no, source)?;
                READ_FILE.with(|c| c.set(fd));
                if fd >= 0 {
                    return Ok(XLogPageReadResult::Success);
                }
                LAST_SOURCE_FAILED.with(|c| c.set(true));
            }

            XLogSource::Stream => {
                debug_assert!(st.standby_mode);

                // C reads the live `PrimaryConnInfo` / `PrimarySlotName` GUC
                // globals when (re)requesting streaming (see the
                // `RequestXLogStreaming(tli, ptr, PrimaryConnInfo,
                // PrimarySlotName, ...)` call in xlogrecovery.c). `StartupRereadConfig`
                // updates those GUCs on SIGHUP and flags a walreceiver restart, so
                // the restarted receiver must pick up the new values. These `st`
                // fields are only a per-recovery snapshot taken at init; refresh
                // them from the live GUCs here so a `primary_slot_name` /
                // `primary_conninfo` change applied via reload (no restart) takes
                // effect — otherwise a cascaded standby reconnects without its
                // configured slot and hot_standby_feedback never reaches the slot.
                st.primary_conn_info = crate::gucvars::primary_conn_info().unwrap_or_default();
                st.primary_slot_name = crate::gucvars::primary_slot_name().unwrap_or_default();
                st.wal_receiver_create_temp_slot =
                    crate::gucvars::wal_receiver_create_temp_slot();

                // Shutdown walreceiver if a restart was requested.
                if crate::shmem::pending_wal_rcv_restart() && !start_wal_receiver {
                    walrcv_seam::xlog_shutdown_wal_rcv::call();
                    if st.recovery_target_timeline_goal == RecoveryTargetTimeLineGoal::Latest {
                        rescan_latest_timeline(st, replay_tli, replay_lsn)?;
                    }
                    start_wal_receiver = true;
                }
                crate::shmem::set_pending_wal_rcv_restart(false);

                // Launch walreceiver if needed.
                if start_wal_receiver
                    && !st.primary_conn_info.is_empty()
                {
                    let (ptr, tli) = if fetching_ckpt {
                        (st.redo_start_lsn, st.redo_start_tli)
                    } else {
                        let tli = timeline_seam::tli_of_point_in_history::call(
                            tli_rec_ptr,
                            &st.expected_tles,
                        )?;
                        let cur = CUR_FILE_TLI.with(Cell::get);
                        if cur > 0 && tli < cur {
                            return Err(PgError::error(&format!(
                                "according to history file, WAL location {} belongs to timeline {}, but previous recovered WAL file came from timeline {}",
                                lsn_fmt(tli_rec_ptr), tli, cur
                            )));
                        }
                        (rec_ptr, tli)
                    };
                    CUR_FILE_TLI.with(|c| c.set(tli));
                    walrcv_seam::set_install_xlog_file_segment_active::call();
                    walrcv_seam::request_xlog_streaming::call(
                        tli,
                        ptr,
                        &st.primary_conn_info,
                        &st.primary_slot_name,
                        st.wal_receiver_create_temp_slot,
                    )?;
                    FLUSHED_UPTO.with(|c| c.set(0));
                }

                // Check if WAL receiver is active or wait to start up.
                if !walrcv_seam::wal_rcv_streaming::call()? {
                    LAST_SOURCE_FAILED.with(|c| c.set(true));
                } else {
                    // Walreceiver is active, so see if new data has arrived.
                    let havedata = if rec_ptr < FLUSHED_UPTO.with(Cell::get) {
                        true
                    } else {
                        let (flushed, latest_chunk_start, recv_tli) =
                            walrcv_seam::get_wal_rcv_flush_rec_ptr_full::call();
                        FLUSHED_UPTO.with(|c| c.set(flushed));
                        RECEIVE_TLI.with(|c| c.set(recv_tli));
                        if rec_ptr < flushed && recv_tli == CUR_FILE_TLI.with(Cell::get) {
                            if latest_chunk_start <= rec_ptr {
                                let now = timestamp_seam::get_current_timestamp::call();
                                crate::shmem::set_xlog_receipt_time(now);
                                crate::shmem::set_current_chunk_start_time(now);
                            }
                            true
                        } else {
                            false
                        }
                    };

                    if havedata {
                        if READ_FILE.with(Cell::get) < 0 {
                            if st.expected_tles.is_empty() {
                                let ctx = mcx::MemoryContext::new("recovery timeline history");
                                let tles = timeline_seam::read_timeline_history::call(
                                    ctx.mcx(),
                                    st.recovery_target_tli,
                                )?;
                                st.expected_tles = tles.iter().copied().collect();
                            }
                            let read_seg_no = READ_SEG_NO.with(Cell::get);
                            let recv_tli = RECEIVE_TLI.with(Cell::get);
                            let fd = x_log_file_read(
                                st, read_seg_no, recv_tli, XLogSource::Stream, false,
                            )?;
                            READ_FILE.with(|c| c.set(fd));
                            debug_assert!(fd >= 0);
                        } else {
                            READ_SOURCE.with(|c| c.set(XLogSource::Stream));
                            crate::shmem::set_xlog_receipt_source(XLogSource::Stream);
                            return Ok(XLogPageReadResult::Success);
                        }
                    } else if nonblocking {
                        return Ok(XLogPageReadResult::WouldBlock);
                    } else if crate::promote::check_for_standby_trigger(st) {
                        // After being triggered, replay what was streamed.
                        LAST_SOURCE_FAILED.with(|c| c.set(true));
                    } else {
                        // Tell the upstream our replay location now.
                        if !streaming_reply_sent {
                            walrcv_seam::set_force_reply::call();
                            streaming_reply_sent = true;
                        }
                        procarray_seam::known_assigned_transaction_ids_idle_maintenance::call();
                        // Update pg_stat_recovery_prefetch before sleeping.
                        prefetcher_seam::prefetcher_compute_stats::call();
                        recovery_wait_latch(
                            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
                            -1,
                            WAIT_EVENT_RECOVERY_WAL_STREAM,
                        )?;
                    }
                }
            }

            XLogSource::Any => {
                return Err(PgError::error(&format!(
                    "unexpected WAL source {:?}",
                    crate::shmem::current_source()
                )));
            }
        }

        // Check for recovery pause here so a requested pause takes effect quickly.
        if crate::shmem::get_recovery_pause_state()
            != ::wal::wal::RecoveryPauseState::NotPaused
        {
            let ctx = mcx::MemoryContext::new("recovery pause");
            crate::stop::recovery_pauses_here(recovery_state(), ctx.mcx(), false)?;
        }

        // Handle interrupts of the startup process.
        startup_proc_interrupts()?;
    }
}

/// `WaitLatch(&XLogRecoveryCtl->recoveryWakeupLatch, ...)` then
/// `ResetLatch(...)`. The recovery wakeup latch is the recovery-shmem shared
/// latch; the latch unit's handle wiring for it is a separate keystone, so
/// driving the wait reaches the latch seam through the recovery wakeup-latch
/// handle.
fn recovery_wait_latch(wake_events: u32, timeout: i64, wait_event: u32) -> Result<(), PgError> {
    let handle = crate::shmem::recovery_wakeup_latch_handle();
    let _ = latch::WaitLatch(Some(handle), wake_events, timeout, wait_event)?;
    latch::ResetLatch(handle);
    Ok(())
}

/// `ProcessStartupProcInterrupts()` (startup.c) through its seam.
fn startup_proc_interrupts() -> Result<(), PgError> {
    let ctx = mcx::MemoryContext::new("startup proc interrupts");
    startup_seam::process_startup_proc_interrupts::call(ctx.mcx())
}

// ===========================================================================
// rescanLatestTimeLine (xlogrecovery.c:4148).
// ===========================================================================

/// `static bool rescanLatestTimeLine(TimeLineID replayTLI, XLogRecPtr
/// replayLSN)` (xlogrecovery.c:4148) — re-scan for a newer timeline and switch
/// the recovery target to it if valid.
fn rescan_latest_timeline(
    st: &mut XLogRecoveryState,
    replay_tli: TimeLineID,
    replay_lsn: XLogRecPtr,
) -> Result<bool, PgError> {
    let old_target = st.recovery_target_tli;
    let ctx = mcx::MemoryContext::new("recovery rescan timeline");
    let mcx = ctx.mcx();

    let newtarget = timeline_seam::find_newest_timeline::call(
        mcx,
        st.recovery_target_tli,
        st.archive_recovery_requested,
    )?;
    if newtarget == st.recovery_target_tli {
        return Ok(false);
    }

    let new_expected_tles =
        timeline_seam::read_timeline_history::call(mcx, newtarget)?;

    // If the current timeline is not part of the history of the new timeline,
    // we cannot proceed to it.
    let current_tle = new_expected_tles
        .iter()
        .copied()
        .find(|tle| tle.tli == st.recovery_target_tli);
    let current_tle = match current_tle {
        Some(tle) => tle,
        None => {
            let _ = ereport(LOG)
                .errmsg(format!(
                    "new timeline {} is not a child of database system timeline {}",
                    newtarget, replay_tli
                ))
                .finish(loc(4189, "rescanLatestTimeLine"));
            return Ok(false);
        }
    };

    if current_tle.end < replay_lsn {
        let _ = ereport(LOG)
            .errmsg(format!(
                "new timeline {} forked off current database system timeline {} before current recovery point {}",
                newtarget, replay_tli, lsn_fmt(replay_lsn)
            ))
            .finish(loc(4200, "rescanLatestTimeLine"));
        return Ok(false);
    }

    // The new timeline history seems valid. Switch target.
    st.recovery_target_tli = newtarget;
    st.expected_tles = new_expected_tles.iter().copied().collect();

    // Try to ensure we have all the history files between the old and new target.
    timeline_seam::restore_timeline_history_files::call(
        mcx,
        old_target + 1,
        newtarget,
        st.archive_recovery_requested,
    )?;

    let _ = ereport(LOG)
        .errmsg(format!("new target timeline is {}", st.recovery_target_tli))
        .finish(loc(4221, "rescanLatestTimeLine"));

    Ok(true)
}

// ===========================================================================
// XLogFileRead / XLogFileReadAnyTLI (xlogrecovery.c:4235, 4316).
// ===========================================================================

/// `static int XLogFileRead(XLogSegNo segno, TimeLineID tli, XLogSource source,
/// bool notfoundOk)` (xlogrecovery.c:4235) — open a single WAL segment from the
/// requested source. Returns the open fd, or `-1` for a `notfoundOk` miss.
fn x_log_file_read(
    _st: &mut XLogRecoveryState,
    segno: XLogSegNo,
    tli: TimeLineID,
    source: XLogSource,
    notfound_ok: bool,
) -> Result<i32, PgError> {
    let wal_segment_size = xlog_seam::wal_segment_size::call();
    let xlogfname = xlog_file_name(tli, segno, wal_segment_size);

    let mut path: String;
    match source {
        XLogSource::Archive => {
            ps_seam::set_ps_display::call(&format!("waiting for {}", xlogfname));
            let ctx = mcx::MemoryContext::new("recovery restore xlog");
            let restored = xlogarchive_seam::restore_archived_file::call(
                ctx.mcx(),
                &xlogfname,
                "RECOVERYXLOG",
                wal_segment_size as i64,
                IN_REDO.with(Cell::get),
            )?
            .map(|p| String::from(p.as_str()));
            match restored {
                Some(p) => path = p,
                None => return Ok(-1),
            }
        }
        XLogSource::PgWal | XLogSource::Stream => {
            path = xlog_seam::xlog_file_path::call(tli, segno);
        }
        XLogSource::Any => {
            return Err(PgError::error(&format!(
                "invalid XLogFileRead source {:?}",
                source
            )));
        }
    }

    // If fetched from archive, replace the existing pg_wal segment with it.
    if source == XLogSource::Archive {
        debug_assert!(!xlog_seam::is_install_xlog_file_segment_active::call());
        xlogarchive_seam::keep_file_restored_from_archive::call(&path, &xlogfname)?;
        path = xlog_seam::xlog_file_path::call(tli, segno);
    }

    match fd_seam::basic_open_file::call(&path) {
        Ok(fd) => {
            CUR_FILE_TLI.with(|c| c.set(tli));
            ps_seam::set_ps_display::call(&format!("recovering {}", xlogfname));
            READ_SOURCE.with(|c| c.set(source));
            crate::shmem::set_xlog_receipt_source(source);
            if source != XLogSource::Stream {
                crate::shmem::set_xlog_receipt_time(
                    timestamp_seam::get_current_timestamp::call(),
                );
            }
            Ok(fd)
        }
        Err(errno) => {
            if errno != libc_enoent() || !notfound_ok {
                ereport(PANIC)
                    .errmsg(format!(
                        "could not open file \"{}\": {}",
                        path,
                        std::io::Error::from_raw_os_error(errno)
                    ))
                    .finish(loc(4307, "XLogFileRead"))?;
            }
            Ok(-1)
        }
    }
}

/// `static int XLogFileReadAnyTLI(XLogSegNo segno, XLogSource source)`
/// (xlogrecovery.c:4316) — search for the segment with any TLI in expectedTLEs.
fn x_log_file_read_any_tli(
    st: &mut XLogRecoveryState,
    segno: XLogSegNo,
    source: XLogSource,
) -> Result<i32, PgError> {
    let wal_segment_size = xlog_seam::wal_segment_size::call();

    // The list of TLIs to scan: expectedTLEs, or read it now (without saving).
    let tles: alloc::vec::Vec<::wal::wal::TimeLineHistoryEntry> = if !st.expected_tles.is_empty() {
        st.expected_tles.clone()
    } else {
        let ctx = mcx::MemoryContext::new("recovery anytli history");
        let read = timeline_seam::read_timeline_history::call(ctx.mcx(), st.recovery_target_tli)?;
        let owned: alloc::vec::Vec<_> = read.iter().copied().collect();
        owned
    };
    let read_from_expected = !st.expected_tles.is_empty();

    let cur_file_tli = CUR_FILE_TLI.with(Cell::get);
    for hent in &tles {
        let tli = hent.tli;
        if tli < cur_file_tli {
            break; // don't bother with too-old TLIs
        }

        // Skip a TLI whose segment doesn't belong to it.
        if hent.begin != InvalidXLogRecPtr {
            let beginseg = byte_to_seg(hent.begin, wal_segment_size);
            if segno < beginseg {
                continue;
            }
        }

        if source == XLogSource::Any || source == XLogSource::Archive {
            let fd = x_log_file_read(st, segno, tli, XLogSource::Archive, true)?;
            if fd != -1 {
                let _ = elog(DEBUG1, "got WAL segment from archive");
                if !read_from_expected {
                    st.expected_tles = tles.clone();
                }
                return Ok(fd);
            }
        }

        if source == XLogSource::Any || source == XLogSource::PgWal {
            let fd = x_log_file_read(st, segno, tli, XLogSource::PgWal, true)?;
            if fd != -1 {
                if !read_from_expected {
                    st.expected_tles = tles.clone();
                }
                return Ok(fd);
            }
        }
    }

    // Couldn't find it. Complain about the front timeline.
    let path = xlog_seam::xlog_file_path::call(st.recovery_target_tli, segno);
    let _ = ereport(DEBUG2)
        .errmsg(format!(
            "could not open file \"{}\": {}",
            path,
            std::io::Error::from_raw_os_error(libc_enoent())
        ))
        .finish(loc(4408, "XLogFileReadAnyTLI"));
    Ok(-1)
}

/// `ENOENT` — "No such file or directory".
#[inline]
fn libc_enoent() -> i32 {
    2
}
