//! The WAL read-record retry loop (`ReadRecord` / `XLogPageRead` /
//! `ReadCheckpointRecord` + `EnableStandbyMode` / `emode_for_corrupt_record`).
//!
//! The corrupt-record retry / source-switching STATE MACHINE
//! (crash→archive-recovery transition, the standby retry loop, the page-TLI
//! history check, the corrupt-record log-level downgrade) is ported 1:1 against
//! [`crate::core::XLogRecoveryState`].
//!
//! The actual page-read driver (`XLogPageRead` / `WaitForWALToBecomeAvailable` /
//! `XLogFileRead` / `XLogFileReadAnyTLI`) is the KNOWN HARD-CORE WAL file I/O.
//! It lives in the not-yet-ported page-read owner (`xlogprefetcher.c` owns the
//! `XLogPrefetcher` + its `XLogReaderState`, whose `page_read` callback *is*
//! `XLogPageRead`). The recovery driver reaches it solely through the
//! prefetcher seams ([`xlogprefetcher_seams::prefetcher_begin_read`] /
//! [`xlogprefetcher_seams::prefetcher_read_record`]), which return the decoded
//! record plus the reader-state fields this loop inspects. Those seams are
//! declared but uninstalled (the owner is unported), so a call panics loudly —
//! the sanctioned seam-and-panic boundary, exactly as the re-scaffold notes.
//!
//! Ported from `src/backend/access/transam/xlogrecovery.c` (lines 485-518,
//! 3153-3305, 4074-4147).

use alloc::format;

use ::utils_error::{ereport, elog};
use ::types_core::{TimeLineID, XLogRecPtr};
use ::types_core::InvalidXLogRecPtr;
use ::types_error::{ErrorLevel, ErrorLocation, PgError, DEBUG1, LOG};
use ::wal::wal::{RM_XLOG_ID, XLR_INFO_MASK};
use ::wal::xlog_consts::XLOG_BLCKSZ;

use crate::core::{
    lsn_fmt, RecordRef, XLogRecoveryState, XLogSource, XLOG_CHECKPOINT_ONLINE,
    XLOG_CHECKPOINT_SHUTDOWN,
};

use timeline_seams as timeline_seam;
use transam_xlog_seams as xlog_seam;
use xlogprefetcher_seams as prefetcher_seam;
use xlogreader_seams as reader_seam;

/// Tracks `emode_for_corrupt_record`'s `static XLogRecPtr lastComplaint`. The
/// startup process is the single caller (single-threaded WAL replay), matching
/// C's file-static exactly.
static mut LAST_COMPLAINT: XLogRecPtr = 0;

#[inline]
fn loc(lineno: i32, func: &str) -> ErrorLocation {
    ErrorLocation::new("xlogrecovery.c", lineno, func)
}

/// `SizeOfXLogRecord` (xlog_internal.h) = 24 bytes.
const SIZE_OF_XLOG_RECORD: u32 = 24;
/// `SizeOfXLogRecordDataHeaderShort` = 2 bytes.
const SIZE_OF_XLOG_RECORD_DATA_HEADER_SHORT: u32 = 2;
/// `sizeof(CheckPoint)` (catalog/pg_control.h) = 88 bytes on LP64.
const SIZE_OF_CHECK_POINT: u32 = 88;

/// `XRecOffIsValid(recptr)` (xlog_internal.h): the record offset within a page
/// must be `>= SizeOfXLogShortPHD` (= 24); offset 0 is the page header.
#[inline]
fn x_rec_off_is_valid(recptr: XLogRecPtr) -> bool {
    // recptr % XLOG_BLCKSZ >= SizeOfXLogShortPHD
    const SIZE_OF_XLOG_SHORT_PHD: u64 = 24;
    (recptr % XLOG_BLCKSZ as u64) >= SIZE_OF_XLOG_SHORT_PHD
}

/// `XLByteToSeg(xlrp, logSegNo, wal_segsz_bytes)` (xlog_internal.h) — the WAL
/// segment number containing `ptr`. Pure arithmetic, inlined per the
/// re-scaffold note.
#[inline]
fn byte_to_seg(ptr: XLogRecPtr, wal_segment_size: i32) -> u64 {
    ptr / wal_segment_size as u64
}

/// `XLogSegmentOffset(xlogptr, wal_segsz_bytes)` (xlog_internal.h) — the byte
/// offset within the segment.
#[inline]
fn xlog_segment_offset(ptr: XLogRecPtr, wal_segment_size: i32) -> u32 {
    (ptr % wal_segment_size as u64) as u32
}

/// `XLogFileName(fname, tli, logSegNo, wal_segsz_bytes)` (xlog_internal.h) — the
/// bare WAL segment filename `"%08X%08X%08X"`. Pure arithmetic, inlined.
#[inline]
fn xlog_file_name(tli: TimeLineID, log_seg_no: u64, wal_segment_size: i32) -> alloc::string::String {
    // XLogSegmentsPerXLogId(wal_segsz_bytes) = UINT64CONST(0x100000000) / sz.
    let per: u64 = 0x1_0000_0000_u64 / wal_segment_size as u64;
    format!(
        "{:08X}{:08X}{:08X}",
        tli,
        (log_seg_no / per) as u32,
        (log_seg_no % per) as u32
    )
}

/// `static void EnableStandbyMode(void)` (xlogrecovery.c:485) — enter standby
/// mode and disable startup-progress reporting.
pub(crate) fn enable_standby_mode(st: &mut XLogRecoveryState) {
    st.standby_mode = true;

    // To avoid server log bloat, we don't report recovery progress in a standby
    // as it will always be in recovery unless promoted. We disable startup
    // progress timeout in standby mode to avoid calling
    // startup_progress_timeout_handler() unnecessarily.
    startup_seams::disable_startup_progress_timeout::call();
}

/// `static int emode_for_corrupt_record(int emode, XLogRecPtr RecPtr)`
/// (xlogrecovery.c:4074) — lower the error level for the first corrupt-record
/// report from a given source so we don't spam the log while waiting for WAL.
pub(crate) fn emode_for_corrupt_record(
    st: &XLogRecoveryState,
    emode: ErrorLevel,
    rec_ptr: XLogRecPtr,
) -> ErrorLevel {
    let mut emode = emode;
    if st.read_source == XLogSource::PgWal && emode == LOG {
        // SAFETY: the startup process is the single-threaded caller of recovery,
        // exactly as C's file-static `lastComplaint` assumes.
        unsafe {
            if rec_ptr == LAST_COMPLAINT {
                emode = DEBUG1;
            } else {
                LAST_COMPLAINT = rec_ptr;
            }
        }
    }
    emode
}

/// `static XLogRecord *ReadRecord(XLogPrefetcher *xlogprefetcher, int emode,`
/// `bool fetching_ckpt, TimeLineID replayTLI)` (xlogrecovery.c:3153) — read the
/// next WAL record, retrying across WAL sources as needed.
///
/// Returns `RecordRef::default()` (C `NULL`) at end-of-WAL / give-up; on the
/// `ereport(emode_for_corrupt_record >= ERROR)` paths returns `Err`.
pub(crate) fn read_record(
    st: &mut XLogRecoveryState,
    emode: ErrorLevel,
    fetching_ckpt: bool,
    replay_tli: TimeLineID,
) -> Result<RecordRef, PgError> {
    // Pass through parameters to XLogPageRead via the held reader's
    // private_data (XLogPageReadPrivate), exactly as C's ReadRecord sets
    // private->{fetching_ckpt,emode,randAccess,replayTLI} before the loop
    // (xlogrecovery.c:3160-3163). The page-read driver reads them back from
    // reader.private_data on each XLogPageRead call.
    crate::walrecovery::set_page_read_private(emode, fetching_ckpt, replay_tli);

    // This is the first attempt to read this page.
    //
    // `lastSourceFailed` is a single file-static in C (xlogrecovery.c:249),
    // shared between ReadRecord and WaitForWALToBecomeAvailable. The page-read
    // driver reads it through the `pageread` accessor, so route ReadRecord's
    // writes there too — keeping the struct field write-only here would leave
    // WaitForWALToBecomeAvailable's copy stale, so it would never advance its
    // source state machine / wait on the latch after an incomplete record,
    // busy-looping (and never honoring a shutdown request).
    st.last_source_failed = false;
    crate::pageread::set_last_source_failed(false);

    loop {
        // record = XLogPrefetcherReadRecord(xlogprefetcher, &errormsg);
        let res = prefetcher_seam::prefetcher_read_record::call();
        st.read_source = res.read_source;

        let mut record = res.record;
        st.current_record = record;

        if record == RecordRef::default() {
            // When we find that WAL ends in an incomplete record, keep track of
            // that record. After recovery is done, we'll write a record to
            // indicate to downstream WAL readers that that portion is to be
            // ignored.
            //
            // However, when ArchiveRecoveryRequested = true, we're going to
            // switch to a new timeline at the end of recovery. We will only copy
            // WAL over to the new timeline up to the end of the last complete
            // record, so if we did this, we would later create an overwrite
            // contrecord in the wrong place, breaking everything.
            if !st.archive_recovery_requested && res.aborted_rec_ptr != InvalidXLogRecPtr {
                st.aborted_rec_ptr = res.aborted_rec_ptr;
                st.missing_contrec_ptr = res.missing_contrec_ptr;
            }

            // if (readFile >= 0) { close(readFile); readFile = -1; }
            // (xlogrecovery.c:3195-3199). This MUST run on every null record,
            // not just on a page-read driver failure: when the reader hits an
            // incomplete record at the end of valid WAL (e.g. a partial record
            // left after the upstream walreceiver disconnects), the page itself
            // is still present in the open segment, so XLogPageRead would keep
            // returning it (readFile >= 0, source not Stream) and ReadRecord
            // would re-report "invalid record length" forever without ever
            // re-entering WaitForWALToBecomeAvailable — a tight busy loop that
            // also never honors a shutdown request. Closing the fd here forces
            // the next XLogPageRead to call WaitForWALToBecomeAvailable, which
            // consults lastSourceFailed, advances the source state machine,
            // waits on the latch, and processes startup-proc interrupts.
            crate::pageread::close_read_file_pub();

            // We only end up here without a message when XLogPageRead() failed -
            // in that case we already logged something. In StandbyMode that only
            // happens if we have been triggered, so we shouldn't loop anymore in
            // that case.
            if let Some(msg) = res.errormsg {
                let level = emode_for_corrupt_record(st, emode, res.end_rec_ptr);
                // ereport(>= ERROR) returns Err and unwinds; LOG/DEBUG returns Ok.
                ereport(level)
                    .errmsg_internal(msg) // already translated
                    .finish(loc(3210, "ReadRecord"))?;
            }
        } else if !timeline_seam::tli_in_history::call(res.latest_page_tli, &st.expected_tles) {
            // Check page TLI is one of the expected values.
            let wal_segment_size = xlog_seam::wal_segment_size::call();
            let segno = byte_to_seg(res.latest_page_ptr, wal_segment_size);
            let offset = xlog_segment_offset(res.latest_page_ptr, wal_segment_size);
            let fname = xlog_file_name(res.seg_tli, segno, wal_segment_size);
            let level = emode_for_corrupt_record(st, emode, res.end_rec_ptr);
            ereport(level)
                .errmsg(format!(
                    "unexpected timeline ID {} in WAL segment {}, LSN {}, offset {}",
                    res.latest_page_tli,
                    fname,
                    lsn_fmt(res.latest_page_ptr),
                    offset
                ))
                .finish(loc(3230, "ReadRecord"))?;
            record = RecordRef::default();
            st.current_record = record;
        }

        if record != RecordRef::default() {
            // Great, got a record.
            return Ok(record);
        }

        // No valid record available from this source.  Mirror the single C
        // file-static so WaitForWALToBecomeAvailable advances its state machine
        // (and blocks/waits, checking interrupts) on the next page-read call.
        st.last_source_failed = true;
        crate::pageread::set_last_source_failed(true);

        // If archive recovery was requested, but we were still doing crash
        // recovery, switch to archive recovery and retry using the offline
        // archive. We have now replayed all the valid WAL in pg_wal, so we are
        // presumably now consistent.
        //
        // We require that there's at least some valid WAL present in pg_wal,
        // however (!fetching_ckpt). We could recover using the WAL from the
        // archive, even if pg_wal is completely empty, but we'd have no idea how
        // far we'd have to replay to reach consistency. So err on the safe side
        // and give up.
        if !st.in_archive_recovery && st.archive_recovery_requested && !fetching_ckpt {
            let _ = elog(
                DEBUG1,
                "reached end of WAL in pg_wal, entering archive recovery",
            );
            st.in_archive_recovery = true;
            if st.standby_mode_requested {
                enable_standby_mode(st);
            }

            xlog_seam::switch_into_archive_recovery::call(res.end_rec_ptr, replay_tli)?;
            st.min_recovery_point = res.end_rec_ptr;
            st.min_recovery_point_tli = replay_tli;

            crate::replay::check_recovery_consistency(st)?;

            // Before we retry, reset lastSourceFailed and currentSource so that
            // we will check the archive next.  Both are single C file-statics
            // consulted by WaitForWALToBecomeAvailable; route through the
            // canonical accessors (the struct fields alone are not what the
            // page-read driver reads).
            st.last_source_failed = false;
            crate::pageread::set_last_source_failed(false);
            st.current_source = XLogSource::Any;
            crate::shmem::set_current_source(XLogSource::Any);

            continue;
        }

        // In standby mode, loop back to retry. Otherwise, give up.
        if st.standby_mode && !crate::promote::check_for_standby_trigger(st) {
            continue;
        } else {
            return Ok(RecordRef::default());
        }
    }
}

/// `static XLogRecord *ReadCheckpointRecord(XLogPrefetcher *xlogprefetcher,`
/// `XLogRecPtr RecPtr, TimeLineID replayTLI)` (xlogrecovery.c:4093) — read the
/// checkpoint record at a given LSN, with structured error reporting.
pub(crate) fn read_checkpoint_record(
    st: &mut XLogRecoveryState,
    rec_ptr: XLogRecPtr,
    replay_tli: TimeLineID,
) -> Result<RecordRef, PgError> {
    // Assert(xlogreader != NULL);

    if !x_rec_off_is_valid(rec_ptr) {
        let _ = ereport(LOG)
            .errmsg("invalid checkpoint location")
            .finish(loc(4106, "ReadCheckpointRecord"));
        return Ok(RecordRef::default());
    }

    prefetcher_seam::prefetcher_begin_read::call(rec_ptr);
    let record = read_record(st, LOG, true, replay_tli)?;

    if record == RecordRef::default() {
        let _ = ereport(LOG)
            .errmsg("invalid checkpoint record")
            .finish(loc(4117, "ReadCheckpointRecord"));
        return Ok(RecordRef::default());
    }
    if reader_seam::xlog_rec_rmid::call(record) != RM_XLOG_ID {
        let _ = ereport(LOG)
            .errmsg("invalid resource manager ID in checkpoint record")
            .finish(loc(4124, "ReadCheckpointRecord"));
        return Ok(RecordRef::default());
    }
    let info = reader_seam::xlog_rec_info::call(record) & !XLR_INFO_MASK;
    if info != XLOG_CHECKPOINT_SHUTDOWN && info != XLOG_CHECKPOINT_ONLINE {
        let _ = ereport(LOG)
            .errmsg("invalid xl_info in checkpoint record")
            .finish(loc(4133, "ReadCheckpointRecord"));
        return Ok(RecordRef::default());
    }
    if reader_seam::xlog_rec_total_len::call(record)
        != SIZE_OF_XLOG_RECORD + SIZE_OF_XLOG_RECORD_DATA_HEADER_SHORT + SIZE_OF_CHECK_POINT
    {
        let _ = ereport(LOG)
            .errmsg("invalid length of checkpoint record")
            .finish(loc(4140, "ReadCheckpointRecord"));
        return Ok(RecordRef::default());
    }
    Ok(record)
}
