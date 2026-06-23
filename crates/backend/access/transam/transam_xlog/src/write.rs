//! The WAL-write / fsync driver of `access/transam/xlog.c` (PostgreSQL 18.3):
//! the path that dumps the shared WAL-buffer ring pages into the on-disk WAL
//! segment files and fsyncs them.
//!
//! This is the "hard core" that the byte-pos/codec/insert layers feed: once a
//! record has been copied into the WAL-buffer ring by [`crate::insert`], it
//! must eventually be written through to the segment file ([`XLogWrite`]) and
//! made durable ([`XLogFlush`] / [`issue_xlog_fsync`]). Segment files are
//! created/extended by [`XLogFileInit`] (via `XLogFileInitInternal` +
//! `InstallXLogFileSegment`).
//!
//! Concurrency mirrors C exactly: [`WaitXLogInsertionsToFinish`] waits on the
//! genuine `WALInsertLock` array (`LWLockWaitForVar` over the `insertingAt`
//! atomics), `WALWriteLock`/`ControlFileLock` are the real builtin LWLocks, and
//! the (Write, Flush) results are published through the `logWriteResult`/
//! `logFlushResult` shmem atomics with the C memory-barrier ordering.
//!
//! Cross-subsystem callees that are genuinely unported cross their owners'
//! seams: the checkpointer (`RequestCheckpoint`), the walsenders
//! (`WalSndWakeup`), and the WAL archiver (`XLogArchiveNotifySeg`, ported).
//! Everything else here is grounded 1:1 in xlog.c.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate std;

use core::cell::Cell;

use utils_error::{PgError, PgResult};
use types_core::{pg_time_t, TimeLineID, XLogRecPtr, XLogSegNo};
use types_storage::storage::{pg_atomic_uint64, LW_EXCLUSIVE, LW_SHARED};
use wal::xlog_consts::{WalSyncMethod, CHECKPOINT_CAUSE_XLOG, XLOG_BLCKSZ};

use fd_seams as fd;
use pgstat_io_seams as pgstat_io;
use lwlock as lwlock;
use init_small::globals;

use xlogarchive as xlogarchive;
use checkpointer_seams as checkpointer;
use walsender_seams as walsender;
use walreceiverfuncs_seams as walrcv;
use timestamp_seams as timestamp;
use guc_tables::vars;

use crate::insert::XLogRecPtrToBufIdx;
use crate::shmem::{
    self, logwrt_result, refresh_xlog_write_result, set_logwrt_result, wal_segment_size, xlog_ctl,
    XLogCtlData, XLogwrtRqst,
};
use crate::{
    GetRedoRecPtr, XLByteInPrevSeg, XLByteToPrevSeg, XLByteToSeg, XLogBytePosToEndRecPtr,
    XLogFileName, XLogFilePath, XLogSegmentOffset,
};

// ===========================================================================
// open(2) / file constants (fcntl.h / xlog_internal.h) mirrored for the
// segment-file open paths.
// ===========================================================================

/// `XLOGDIR` (xlog_internal.h).
const XLOGDIR: &str = "pg_wal";
/// `PG_BINARY` (c.h) — 0 on non-Windows.
const PG_BINARY: i32 = 0;

const O_RDONLY: i32 = libc::O_RDONLY;
const O_RDWR: i32 = libc::O_RDWR;
const O_CREAT: i32 = libc::O_CREAT;
const O_EXCL: i32 = libc::O_EXCL;
const O_CLOEXEC: i32 = libc::O_CLOEXEC;

const ENOENT: i32 = libc::ENOENT;

/// `CHECKPOINT_SEGMENTS` arithmetic uses the GUC-derived `CheckPointSegments`
/// (recomputed by `ReadControlFile`/GUC assign). The crate exposes its
/// arithmetic via `recompute_segment_derived`; for the driver we keep a
/// backend-local cache of the latest value (file-scope `int CheckPointSegments`
/// in xlog.c).
std::thread_local! {
    /// `static int CheckPointSegments` (xlog.c).
    static CHECK_POINT_SEGMENTS: Cell<i32> = const { Cell::new(3) };

    /// `static int openLogFile = -1` (xlog.c) — the currently-open WAL segment
    /// fd for writing (a bare `BasicOpenFile` kernel fd).
    static OPEN_LOG_FILE: Cell<i32> = const { Cell::new(-1) };
    /// `static XLogSegNo openLogSegNo = 0` (xlog.c).
    static OPEN_LOG_SEG_NO: Cell<XLogSegNo> = const { Cell::new(0) };
    /// `static TimeLineID openLogTLI = 0` (xlog.c).
    static OPEN_LOG_TLI: Cell<TimeLineID> = const { Cell::new(0) };
}

/// Publish the resolved `CheckPointSegments` (the GUC/control-file derived
/// checkpoint distance). Called by the startup/GUC path; owned by xlog.c.
pub fn set_check_point_segments(n: i32) {
    CHECK_POINT_SEGMENTS.with(|c| c.set(n));
}

#[inline]
fn check_point_segments() -> i32 {
    CHECK_POINT_SEGMENTS.with(Cell::get)
}

/// `CheckPointSegments` (xlog.c:181, `int`) read as a `double` for the
/// `check_point_segments` seam — the checkpointer's `IsCheckpointOnSchedule`
/// divides by it in floating-point (checkpointer.c:888 `(double)
/// CheckPointSegments`). Faithful: read the int cache, widen to f64.
pub fn check_point_segments_f64() -> f64 {
    check_point_segments() as f64
}

/// `int XLogArchiveTimeout` (xlog.c GUC `archive_timeout`, seconds) — a plain
/// global read for the `xlog_archive_timeout` seam consumed by the
/// checkpointer's `CheckArchiveTimeout` / main-loop timeout computation.
#[inline]
pub fn xlog_archive_timeout() -> i32 {
    vars::XLogArchiveTimeout.read()
}

// ===========================================================================
// GUC reads (xlog.c file-scope GUC globals, stored in the guc-tables vars).
// ===========================================================================

/// `bool enableFsync` (the `fsync` GUC).
#[inline]
pub fn enable_fsync() -> bool {
    vars::enableFsync.read()
}

/// `int wal_compression` (xlog.c) — the `wal_compression` GUC enum ordinal
/// (`WAL_COMPRESSION_*`); read directly from the GUC slot. Installed into the
/// `wal_compression` seam for `xloginsert`'s `XLogRecordAssemble`.
#[inline]
pub fn wal_compression() -> i32 {
    vars::wal_compression.read()
}

// `wal_consistency_checking[rmid]` (xlog.c) is now REAL in [`crate::guc_vars`]
// (the per-rmgr bool array built by `assign_wal_consistency_checking`), and is
// installed into the `wal_consistency_checking` seam from there.

/// `int wal_sync_method` (the `wal_sync_method` GUC).
#[inline]
pub fn wal_sync_method() -> WalSyncMethod {
    match vars::wal_sync_method.read() {
        0 => WalSyncMethod::Fsync,
        1 => WalSyncMethod::Fdatasync,
        2 => WalSyncMethod::Open,
        3 => WalSyncMethod::FsyncWritethrough,
        4 => WalSyncMethod::OpenDsync,
        _ => WalSyncMethod::OpenDsync,
    }
}

#[inline]
fn wal_init_zero() -> bool {
    vars::wal_init_zero.read()
}
#[inline]
fn commit_delay() -> i32 {
    vars::CommitDelay.read()
}
#[inline]
fn commit_siblings() -> i32 {
    vars::CommitSiblings.read()
}
#[inline]
fn wal_writer_delay() -> i32 {
    vars::WalWriterDelay.read()
}
#[inline]
fn wal_writer_flush_after() -> i32 {
    vars::WalWriterFlushAfter.read()
}

/// `XLogArchivingActive()` (xlog.h): `XLogArchiveMode > ARCHIVE_MODE_OFF`. The
/// `archive_mode` GUC (`ARCHIVE_MODE_OFF` = 0).
#[inline]
fn xlog_archiving_active() -> bool {
    vars::XLogArchiveMode.read() > 0
}

// ===========================================================================
// Misc helpers.
// ===========================================================================

/// `NextBufIdx(idx)` (xlog.c:611).
#[inline]
unsafe fn NextBufIdx(ctl: &XLogCtlData, idx: i32) -> i32 {
    if idx == ctl.XLogCacheBlck {
        0
    } else {
        idx + 1
    }
}

/// `time(NULL)` as a `pg_time_t`.
#[inline]
fn time_now() -> pg_time_t {
    // SAFETY: time(2) with a null arg returns the current time.
    (unsafe { libc::time(core::ptr::null_mut()) }) as pg_time_t
}

/// `WALWriteLock` — offset 8 in the `MainLWLockArray` (`lwlocklist.h`).
const WAL_WRITE_LOCK: usize = 8;
/// `ControlFileLock` — offset 9 in the `MainLWLockArray` (`lwlocklist.h`).
const CONTROL_FILE_LOCK: usize = 9;

// ===========================================================================
// WaitXLogInsertionsToFinish (xlog.c:1531).
// ===========================================================================

/// `WaitXLogInsertionsToFinish(upto)` (xlog.c:1531) — wait until all WAL
/// insertions older than `upto` have finished, returning the point up to which
/// all insertions are known finished.
///
/// # Safety
/// `ctl` must reference the live `XLogCtl` shmem region.
pub(crate) unsafe fn WaitXLogInsertionsToFinish(
    ctl: &XLogCtlData,
    mut upto: XLogRecPtr,
) -> PgResult<XLogRecPtr> {
    let insert = &ctl.Insert;

    // Check if there's any work to do (membarrier read for freshness).
    let inserted = ctl.logInsertResult.read_membarrier();
    if upto <= inserted {
        return Ok(inserted);
    }

    // Read the current insert position.
    shmem::spin_lock_acquire(&insert.insertpos_lck);
    let bytepos = insert.CurrBytePos;
    shmem::spin_lock_release(&insert.insertpos_lck);
    let reserved_upto = XLogBytePosToEndRecPtr(bytepos, wal_segment_size());

    if upto > reserved_upto {
        // Request to flush past end of generated WAL; clamp to reserved.
        upto = reserved_upto;
    }

    let mut finished_upto = reserved_upto;
    for i in 0..crate::shmem::NUM_XLOGINSERT_LOCKS {
        let mut insertingat: XLogRecPtr = crate::InvalidXLogRecPtr;
        let lock = insert_lock(ctl, i);
        let atomic = inserting_at(ctl, i);

        loop {
            // LWLockWaitForVar: returns true if the lock was free (no insert in
            // progress); otherwise waits and reports the inserter's position
            // through the out-param `newval`.
            let mut newval: u64 = insertingat;
            let free =
                lwlock::LWLockWaitForVar(lock, atomic, insertingat, &mut newval, globals::MyProcNumber())?;
            if free {
                insertingat = crate::InvalidXLogRecPtr;
                break;
            }
            insertingat = newval;
            if insertingat >= upto {
                break;
            }
        }

        if insertingat != crate::InvalidXLogRecPtr && insertingat < finished_upto {
            finished_upto = insertingat;
        }
    }

    // Advance the shared limit monotonically and return the freshest value.
    finished_upto = ctl.logInsertResult.monotonic_advance(finished_upto);
    Ok(finished_upto)
}

/// `&WALInsertLocks[i].l.lock`.
#[inline]
unsafe fn insert_lock(ctl: &XLogCtlData, i: usize) -> &'static types_storage::storage::LWLock {
    let locks = ctl.Insert.WALInsertLocks;
    &(*locks.add(i)).l.lock
}

/// `&WALInsertLocks[i].l.insertingAt`.
#[inline]
unsafe fn inserting_at(ctl: &XLogCtlData, i: usize) -> &'static pg_atomic_uint64 {
    let locks = ctl.Insert.WALInsertLocks;
    &(*locks.add(i)).l.insertingAt
}

// ===========================================================================
// XLogCheckpointNeeded (xlog.c:2303).
// ===========================================================================

/// `XLogCheckpointNeeded(new_segno)` (xlog.c:2303) — would a checkpoint be due
/// (enough WAL since the redo point) by the time `new_segno` is reached?
fn XLogCheckpointNeeded(new_segno: XLogSegNo) -> bool {
    let seg = wal_segment_size();
    let old_segno = XLByteToSeg(GetRedoRecPtrCached(), seg);
    new_segno >= old_segno + (check_point_segments() as u64 - 1)
}

/// `RedoRecPtr` backend-local cache read (the driver consults the possibly-stale
/// local copy first, like C's `XLogCheckpointNeeded`).
#[inline]
fn GetRedoRecPtrCached() -> XLogRecPtr {
    shmem::redo_rec_ptr_cached()
}

// ===========================================================================
// XLogWrite (xlog.c:2328).
// ===========================================================================

/// `XLogWrite(WriteRqst, tli, flexible)` (xlog.c:2328) — write (and possibly
/// fsync) the WAL at least as far as `WriteRqst` indicates.
///
/// Must be called with `WALWriteLock` held; `WaitXLogInsertionsToFinish` must
/// have been called for `WriteRqst.Write` beforehand.
///
/// # Safety
/// `ctl` must reference the live `XLogCtl` shmem region.
pub(crate) unsafe fn XLogWrite(
    ctl: &XLogCtlData,
    write_rqst: XLogwrtRqst,
    tli: TimeLineID,
    flexible: bool,
) -> PgResult<()> {
    let seg = wal_segment_size();
    let blcksz = XLOG_BLCKSZ as u64;

    // Refresh local LogwrtResult.
    refresh_xlog_write_result(ctl);
    let mut result = logwrt_result();

    let mut npages: usize = 0;
    let mut startidx: i32 = 0;
    let mut startoffset: u32 = 0;

    let mut curridx = XLogRecPtrToBufIdx(ctl, result.Write) as i32;

    while result.Write < write_rqst.Write {
        let end_ptr = (*ctl.xlblocks.add(curridx as usize)).read();
        if result.Write >= end_ptr {
            return Err(PgError::error(std::format!(
                "xlog write request {:X}/{:X} is past end of log {:X}/{:X}",
                (result.Write >> 32) as u32,
                result.Write as u32,
                (end_ptr >> 32) as u32,
                end_ptr as u32,
            )));
        }

        // Advance to end of current buffer page.
        result.Write = end_ptr;
        let ispartialpage = write_rqst.Write < result.Write;

        if !XLByteInPrevSeg(result.Write, OPEN_LOG_SEG_NO.with(Cell::get), seg) {
            // Switch to new logfile segment. No pending pages can exist (we dump
            // at segment end).
            debug_assert_eq!(npages, 0);
            if OPEN_LOG_FILE.with(Cell::get) >= 0 {
                XLogFileClose()?;
            }
            OPEN_LOG_SEG_NO.with(|c| c.set(XLByteToPrevSeg(result.Write, seg)));
            OPEN_LOG_TLI.with(|c| c.set(tli));

            // create/use new log file. (ReserveExternalFD is fd.c-internal
            // budgeting accounting against max_safe_fds — a behaviour-preserving
            // omission for the write mechanics.)
            let fd = XLogFileInit(OPEN_LOG_SEG_NO.with(Cell::get), tli)?;
            OPEN_LOG_FILE.with(|c| c.set(fd));
        }

        // Make sure the logfile is open.
        if OPEN_LOG_FILE.with(Cell::get) < 0 {
            OPEN_LOG_SEG_NO.with(|c| c.set(XLByteToPrevSeg(result.Write, seg)));
            OPEN_LOG_TLI.with(|c| c.set(tli));
            let fd = XLogFileOpen(OPEN_LOG_SEG_NO.with(Cell::get), tli)?;
            OPEN_LOG_FILE.with(|c| c.set(fd));
        }

        // Add the page to the pending-dump set.
        if npages == 0 {
            startidx = curridx;
            startoffset = XLogSegmentOffset(result.Write - blcksz, seg);
        }
        npages += 1;

        let last_iteration = write_rqst.Write <= result.Write;
        let finishing_seg =
            !ispartialpage && (startoffset as u64 + npages as u64 * blcksz) >= seg as u64;

        if last_iteration || curridx == ctl.XLogCacheBlck || finishing_seg {
            // OK to write the page(s).
            let from = ctl.pages.add(startidx as usize * XLOG_BLCKSZ);
            let nbytes = npages * XLOG_BLCKSZ;
            // SAFETY: the page range [startidx, startidx+npages) is contiguous in
            // the ring (we never span the ring wrap, since curridx==XLogCacheBlck
            // forces a dump).
            let buf = core::slice::from_raw_parts(from, nbytes);

            let mut nleft = nbytes;
            let mut off = startoffset as i64;
            let mut bufpos = 0usize;
            while nleft > 0 {
                // Measure I/O timing to write WAL data, for pg_stat_io
                // (xlog.c:2455).
                let io_start = pgstat_io::pgstat_prepare_io_time::call();

                let written = fd::pg_pwrite::call(
                    OPEN_LOG_FILE.with(Cell::get),
                    &buf[bufpos..bufpos + nleft],
                    off,
                );

                // pgstat_count_io_op_time(IOOBJECT_WAL, IOCONTEXT_NORMAL,
                // IOOP_WRITE, start, 1, written) (xlog.c:2461). The seam shape
                // is pre-bound to WAL/NORMAL/WRITE; pass the bytes written
                // (clamp a negative short-write/EINTR result to 0).
                pgstat_io::pgstat_count_io_op_time::call(
                    io_start,
                    written.max(0) as u32,
                );

                if written <= 0 {
                    let errno = (-written) as i32;
                    if errno == libc::EINTR as i32 {
                        continue;
                    }
                    let xlogfname =
                        XLogFileName(tli, OPEN_LOG_SEG_NO.with(Cell::get), seg);
                    return Err(PgError::error(std::format!(
                        "could not write to log file \"{}\" at offset {}, length {}",
                        xlogfname, startoffset, nleft
                    )));
                }
                let w = written as usize;
                nleft -= w;
                bufpos += w;
                off += written as i64;
            }
            startoffset = off as u32;

            npages = 0;

            if finishing_seg {
                issue_xlog_fsync(OPEN_LOG_FILE.with(Cell::get), OPEN_LOG_SEG_NO.with(Cell::get), tli)?;

                // Signal walsenders.
                let _ = walsender::wal_snd_wakeup::call(true, false);

                result.Flush = result.Write; // end of page

                if xlog_archiving_active() {
                    xlogarchive::XLogArchiveNotifySeg(OPEN_LOG_SEG_NO.with(Cell::get), tli)?;
                }

                let ctl_mut = ctl as *const XLogCtlData as *mut XLogCtlData;
                (*ctl_mut).lastSegSwitchTime = time_now();
                (*ctl_mut).lastSegSwitchLSN = result.Flush;

                // Request a checkpoint if too much WAL has accrued.
                if globals::IsUnderPostmaster()
                    && XLogCheckpointNeeded(OPEN_LOG_SEG_NO.with(Cell::get))
                {
                    let _ = GetRedoRecPtr();
                    if XLogCheckpointNeeded(OPEN_LOG_SEG_NO.with(Cell::get)) {
                        checkpointer::request_checkpoint::call(CHECKPOINT_CAUSE_XLOG);
                    }
                }
            }
        }

        if ispartialpage {
            result.Write = write_rqst.Write;
            break;
        }
        curridx = NextBufIdx(ctl, curridx);

        if flexible && npages == 0 {
            break;
        }
    }

    debug_assert_eq!(npages, 0);

    // If asked to flush, do so.
    if result.Flush < write_rqst.Flush && result.Flush < result.Write {
        let sm = wal_sync_method();
        if sm != WalSyncMethod::Open && sm != WalSyncMethod::OpenDsync {
            if OPEN_LOG_FILE.with(Cell::get) >= 0
                && !XLByteInPrevSeg(result.Write, OPEN_LOG_SEG_NO.with(Cell::get), seg)
            {
                XLogFileClose()?;
            }
            if OPEN_LOG_FILE.with(Cell::get) < 0 {
                OPEN_LOG_SEG_NO.with(|c| c.set(XLByteToPrevSeg(result.Write, seg)));
                OPEN_LOG_TLI.with(|c| c.set(tli));
                let fd = XLogFileOpen(OPEN_LOG_SEG_NO.with(Cell::get), tli)?;
                OPEN_LOG_FILE.with(|c| c.set(fd));
            }
            issue_xlog_fsync(OPEN_LOG_FILE.with(Cell::get), OPEN_LOG_SEG_NO.with(Cell::get), tli)?;
        }

        let _ = walsender::wal_snd_wakeup::call(true, false);
        result.Flush = result.Write;
    }

    // Update shared-memory status: keep the request values from falling behind
    // the result values.
    let ctl_mut = ctl as *const XLogCtlData as *mut XLogCtlData;
    shmem::spin_lock_acquire(&ctl.info_lck);
    if (*ctl_mut).LogwrtRqst.Write < result.Write {
        (*ctl_mut).LogwrtRqst.Write = result.Write;
    }
    if (*ctl_mut).LogwrtRqst.Flush < result.Flush {
        (*ctl_mut).LogwrtRqst.Flush = result.Flush;
    }
    shmem::spin_lock_release(&ctl.info_lck);

    // Write 'Write' first, barrier, then 'Flush'.
    ctl.logWriteResult.write(result.Write);
    core::sync::atomic::fence(core::sync::atomic::Ordering::Release);
    ctl.logFlushResult.write(result.Flush);

    set_logwrt_result(result);
    Ok(())
}

// ===========================================================================
// XLogFlush (xlog.c:2804).
// ===========================================================================

/// `XLogFlush(record)` (xlog.c:2804) — ensure WAL is flushed up to `record`.
pub fn XLogFlush(record: XLogRecPtr) -> PgResult<()> {
    let ctl_ptr = xlog_ctl();
    if ctl_ptr.is_null() {
        return Err(PgError::error("XLogFlush: XLogCtl shmem not initialized"));
    }
    // SAFETY: live shmem region.
    let ctl = unsafe { &*ctl_ptr };
    let insert_tli = ctl.InsertTimeLineID;

    // During REDO we update minRecoveryPoint instead of flushing. That path is
    // owned by the recovery side (xlogrecovery / UpdateMinRecoveryPoint), still
    // unported — but XLogInsertAllowed() is false only in recovery, and F2 is
    // the not-in-recovery durable path. Mirror C: if insertion isn't allowed,
    // defer to the recovery owner.
    if !crate::insert::XLogInsertAllowed() {
        // UpdateMinRecoveryPoint(record, false) — recovery-side, unported.
        panic!(
            "xlog recovery driver not ported: XLogFlush during recovery must call \
             UpdateMinRecoveryPoint (owned by the xlogrecovery side, task #13)"
        );
    }

    // Quick exit if already known flushed.
    if record <= logwrt_result().Flush {
        return Ok(());
    }

    globals::StartCriticalSection();

    let mut write_rqst_ptr = record;

    loop {
        // Done already?
        unsafe { refresh_xlog_write_result(ctl) };
        if record <= logwrt_result().Flush {
            break;
        }

        // Wait for in-flight insertions to the pages we're about to write.
        shmem::spin_lock_acquire(&ctl.info_lck);
        if write_rqst_ptr < ctl.LogwrtRqst.Write {
            write_rqst_ptr = ctl.LogwrtRqst.Write;
        }
        shmem::spin_lock_release(&ctl.info_lck);
        let mut insertpos = unsafe { WaitXLogInsertionsToFinish(ctl, write_rqst_ptr)? };

        // Try to get the write lock, or wait until someone else flushed for us.
        let wal_write_lock = lwlock::main_lock_ref(WAL_WRITE_LOCK);
        if !lwlock::LWLockAcquireOrWait(wal_write_lock, LW_EXCLUSIVE, globals::MyProcNumber())? {
            continue;
        }

        // Got the lock; recheck.
        unsafe { refresh_xlog_write_result(ctl) };
        if record <= logwrt_result().Flush {
            lwlock::LWLockRelease(wal_write_lock)?;
            break;
        }

        // Optional group-commit sleep.
        if commit_delay() > 0
            && enable_fsync()
            && minimum_active_backends(commit_siblings())
        {
            // SAFETY: pg_usleep is a plain sleep.
            unsafe { libc::usleep(commit_delay() as libc::useconds_t) };
            insertpos = unsafe { WaitXLogInsertionsToFinish(ctl, insertpos)? };
        }

        let write_rqst = XLogwrtRqst {
            Write: insertpos,
            Flush: insertpos,
        };
        unsafe { XLogWrite(ctl, write_rqst, insert_tli, false)? };

        lwlock::LWLockRelease(wal_write_lock)?;
        break;
    }

    globals::EndCriticalSection();

    // Wake up walsenders.
    let _ = walsender::wal_snd_wakeup::call(true, !shmem::RecoveryInProgress());

    if logwrt_result().Flush < record {
        return Err(PgError::error(std::format!(
            "xlog flush request {:X}/{:X} is not satisfied --- flushed only to {:X}/{:X}",
            (record >> 32) as u32,
            record as u32,
            (logwrt_result().Flush >> 32) as u32,
            logwrt_result().Flush as u32,
        )));
    }
    Ok(())
}

/// `MinimumActiveBackends(min)` (procarray.c) — at least `min` other backends
/// have active transactions. Owned by procarray (NEEDS_DECOMP, task #121, the
/// group-commit accounting is not yet ported). Conservatively report `false`
/// (no extra group-commit delay) when unavailable — behaviour-preserving for
/// the flush mechanics (the sleep is a throughput optimization only).
#[inline]
fn minimum_active_backends(_min: i32) -> bool {
    false
}

// ===========================================================================
// XLogBackgroundFlush (xlog.c:2992).
// ===========================================================================

/// `XLogBackgroundFlush(void)` (xlog.c:2992) — the periodic walwriter flush.
/// Returns true if there was any work to do.
pub fn XLogBackgroundFlush() -> PgResult<bool> {
    let ctl_ptr = xlog_ctl();
    if ctl_ptr.is_null() {
        return Err(PgError::error(
            "XLogBackgroundFlush: XLogCtl shmem not initialized",
        ));
    }
    // SAFETY: live shmem region.
    let ctl = unsafe { &*ctl_ptr };

    let mut flexible = true;

    // No flushing during recovery.
    if shmem::RecoveryInProgress() {
        return Ok(false);
    }

    let insert_tli = ctl.InsertTimeLineID;

    // Read updated LogwrtRqst.
    shmem::spin_lock_acquire(&ctl.info_lck);
    let mut write_rqst = ctl.LogwrtRqst;
    shmem::spin_lock_release(&ctl.info_lck);

    // Back off to last completed page boundary.
    write_rqst.Write -= write_rqst.Write % XLOG_BLCKSZ as u64;

    // If already flushed that far, consider async commit records.
    unsafe { refresh_xlog_write_result(ctl) };
    if write_rqst.Write <= logwrt_result().Flush {
        shmem::spin_lock_acquire(&ctl.info_lck);
        write_rqst.Write = ctl.asyncXactLSN;
        shmem::spin_lock_release(&ctl.info_lck);
        flexible = false; // ensure it all gets written
    }

    // If already known flushed, we're done (maybe close a stale fd).
    if write_rqst.Write <= logwrt_result().Flush {
        if OPEN_LOG_FILE.with(Cell::get) >= 0
            && !XLByteInPrevSeg(
                logwrt_result().Write,
                OPEN_LOG_SEG_NO.with(Cell::get),
                wal_segment_size(),
            )
        {
            XLogFileClose()?;
        }
        return Ok(false);
    }

    // Determine how far to flush.
    let now = timestamp::get_current_timestamp::call();
    let flushblocks =
        write_rqst.Write / XLOG_BLCKSZ as u64 - logwrt_result().Flush / XLOG_BLCKSZ as u64;

    let lastflush = LAST_FLUSH.with(Cell::get);
    if wal_writer_flush_after() == 0 || lastflush == 0 {
        write_rqst.Flush = write_rqst.Write;
        LAST_FLUSH.with(|c| c.set(now));
    } else if timestamp::timestamp_difference_exceeds::call(lastflush, now, wal_writer_delay()) {
        write_rqst.Flush = write_rqst.Write;
        LAST_FLUSH.with(|c| c.set(now));
    } else if flushblocks >= wal_writer_flush_after() as u64 {
        write_rqst.Flush = write_rqst.Write;
        LAST_FLUSH.with(|c| c.set(now));
    } else {
        write_rqst.Flush = 0;
    }

    globals::StartCriticalSection();

    unsafe { WaitXLogInsertionsToFinish(ctl, write_rqst.Write)? };
    let wal_write_lock = lwlock::main_lock_ref(WAL_WRITE_LOCK);
    lwlock::LWLockAcquire(wal_write_lock, LW_EXCLUSIVE, globals::MyProcNumber())?;
    unsafe { refresh_xlog_write_result(ctl) };
    if write_rqst.Write > logwrt_result().Write || write_rqst.Flush > logwrt_result().Flush {
        unsafe { XLogWrite(ctl, write_rqst, insert_tli, flexible)? };
    }
    lwlock::LWLockRelease(wal_write_lock)?;

    globals::EndCriticalSection();

    let _ = walsender::wal_snd_wakeup::call(true, !shmem::RecoveryInProgress());

    // Pre-initialize no-longer-needed WAL buffers for future use.
    // SAFETY: `ctl` is the live shmem region.
    unsafe { crate::insert::advance_xl_insert_buffer_opportunistic(ctl, insert_tli)? };

    Ok(true)
}

std::thread_local! {
    /// `static TimestampTz lastflush` in `XLogBackgroundFlush`.
    static LAST_FLUSH: Cell<i64> = const { Cell::new(0) };
}

// ===========================================================================
// XLogFileInit / XLogFileInitInternal / InstallXLogFileSegment (xlog.c:3211 /
// 3400 / 3582).
// ===========================================================================

/// `XLogFileInit(logsegno, logtli)` (xlog.c:3400) — create or open the WAL
/// segment, returning the fd.
pub fn XLogFileInit(logsegno: XLogSegNo, logtli: TimeLineID) -> PgResult<i32> {
    debug_assert!(logtli != 0);

    let (fd, path, _added) = XLogFileInitInternal(logsegno, logtli)?;
    if fd >= 0 {
        return Ok(fd);
    }

    // Open the original target segment (might not be the file we just made).
    let flags = O_RDWR | PG_BINARY | O_CLOEXEC | get_sync_bit(wal_sync_method());
    match fd::basic_open_file_flags::call(&path, flags) {
        Ok(fd) => Ok(fd),
        Err(e) => Err(PgError::error(std::format!(
            "could not open file \"{}\": {}",
            path,
            std::io::Error::from_raw_os_error(e)
        ))),
    }
}

/// `XLogFileInitInternal(logsegno, logtli, *added, path)` (xlog.c:3211) —
/// returns `(fd, path, added)`. `fd >= 0` means an existing segment was opened;
/// `-1` means the segment was (pre-)created and the caller should open `path`.
/// `added` mirrors the C `*added` out-param: true when this call raised the
/// extant-segment count (consumed by `PreallocXlogFiles` for
/// `CheckpointStats.ckpt_segs_added`).
pub(crate) fn XLogFileInitInternal(
    logsegno: XLogSegNo,
    logtli: TimeLineID,
) -> PgResult<(i32, std::string::String, bool)> {
    debug_assert!(logtli != 0);
    let seg = wal_segment_size();
    let path = XLogFilePath(logtli, logsegno, seg);

    // Try the existent file.
    let open_existing =
        O_RDWR | PG_BINARY | O_CLOEXEC | get_sync_bit(wal_sync_method());
    match fd::basic_open_file_flags::call(&path, open_existing) {
        // *added = false (set above implicitly): an existent file was reused.
        Ok(fd) => return Ok((fd, path, false)),
        Err(e) => {
            if e != ENOENT {
                return Err(PgError::error(std::format!(
                    "could not open file \"{}\": {}",
                    path,
                    std::io::Error::from_raw_os_error(e)
                )));
            }
        }
    }

    // Initialize an empty (all-zeroes) segment in a temp file.
    let tmppath = std::format!("{XLOGDIR}/xlogtemp.{}", std::process::id());
    fd::unlink_file::call(&tmppath);

    let open_flags = O_RDWR | O_CREAT | O_EXCL | PG_BINARY;
    let fd = match fd::basic_open_file_flags::call(&tmppath, open_flags) {
        Ok(fd) if fd >= 0 => fd,
        Ok(_) | Err(_) => {
            let e = fd::last_errno::call();
            return Err(PgError::error(std::format!(
                "could not create file \"{}\": {}",
                tmppath,
                std::io::Error::from_raw_os_error(e)
            )));
        }
    };

    // Fill the file.
    let mut save_errno = 0i32;
    if wal_init_zero() {
        let rc = fd::pg_pwrite_zeros::call(fd, seg as usize, 0);
        if rc < 0 {
            save_errno = (-rc) as i32;
        }
    } else {
        // Seek-and-write-one-byte at end.
        let one = [0u8; 1];
        if fd::pg_pwrite::call(fd, &one, (seg - 1) as i64) != 1 {
            let e = fd::last_errno::call();
            save_errno = if e != 0 { e } else { libc::ENOSPC };
        }
    }

    if save_errno != 0 {
        fd::unlink_file::call(&tmppath);
        close_bare_fd(fd);
        return Err(PgError::error(std::format!(
            "could not write to file \"{}\": {}",
            tmppath,
            std::io::Error::from_raw_os_error(save_errno)
        )));
    }

    // fsync the temp file.
    if fd::pg_fsync::call(fd) != 0 {
        let e = fd::last_errno::call();
        close_bare_fd(fd);
        return Err(PgError::error(std::format!(
            "could not fsync file \"{}\": {}",
            tmppath,
            std::io::Error::from_raw_os_error(e)
        )));
    }

    close_bare_fd(fd);

    // Move the segment into place. Cope with someone else having created it.
    let mut installed_segno = logsegno;
    let max_segno = logsegno + check_point_segments() as u64;
    let added = InstallXLogFileSegment(&mut installed_segno, &tmppath, true, max_segno, logtli)?;
    if !added {
        fd::unlink_file::call(&tmppath);
    }

    Ok((-1, path, added))
}

/// `PreallocXlogFiles(endptr, tli)` (xlog.c:3710) — preallocate log files beyond
/// the specified log endpoint. New segments are added to the [`CheckpointStats`]
/// `ckpt_segs_added` counter. Caller-supplied `stats` mirrors the C file-static
/// `CheckpointStats` global. `Err` is the `XLogFileInitInternal` ereport(ERROR)
/// (a full filesystem etc.) — see the C comment block at xlog.c:3690.
pub(crate) fn PreallocXlogFiles(
    endptr: XLogRecPtr,
    tli: TimeLineID,
    stats: &mut crate::checkpoint::CheckpointStats,
) -> PgResult<()> {
    // if (!XLogCtl->InstallXLogFileSegmentActive) return; — unlocked check.
    let ctl = xlog_ctl();
    let active = if ctl.is_null() {
        false
    } else {
        // SAFETY: live shmem region; the C does this read unlocked too.
        unsafe { (*ctl).InstallXLogFileSegmentActive }
    };
    if !active {
        return Ok(());
    }

    let seg = wal_segment_size();
    let mut log_seg_no = XLByteToPrevSeg(endptr, seg);
    let offset = XLogSegmentOffset(endptr.wrapping_sub(1), seg);
    if offset >= (0.75 * seg as f64) as u32 {
        log_seg_no += 1;
        let (lf, _path, added) = XLogFileInitInternal(log_seg_no, tli)?;
        if lf >= 0 {
            close_bare_fd(lf);
        }
        if added {
            stats.ckpt_segs_added += 1;
        }
    }
    Ok(())
}

/// Close a bare kernel fd (the `close(fd)` for a `BasicOpenFile` result that is
/// NOT tracked by the transient-descriptor table).
#[inline]
fn close_bare_fd(fd: i32) {
    // SAFETY: closing a live bare kernel fd.
    unsafe {
        libc::close(fd);
    }
}

/// `InstallXLogFileSegment(*segno, tmppath, find_free, max_segno, tli)`
/// (xlog.c:3582) — rename the temp file into its final segment name, finding a
/// free slot if `find_free`. Returns true if it raised the extant-segment
/// count.
fn InstallXLogFileSegment(
    segno: &mut XLogSegNo,
    tmppath: &str,
    find_free: bool,
    max_segno: XLogSegNo,
    tli: TimeLineID,
) -> PgResult<bool> {
    debug_assert!(tli != 0);
    let seg = wal_segment_size();
    let mut path = XLogFilePath(tli, *segno, seg);

    let control_file_lock = lwlock::main_lock_ref(CONTROL_FILE_LOCK);
    lwlock::LWLockAcquire(control_file_lock, LW_EXCLUSIVE, globals::MyProcNumber())?;

    // SAFETY: live shmem region; ControlFileLock held.
    let ctl = xlog_ctl();
    let active = if ctl.is_null() {
        false
    } else {
        unsafe { (*ctl).InstallXLogFileSegmentActive }
    };
    if !active {
        lwlock::LWLockRelease(control_file_lock)?;
        return Ok(false);
    }

    if !find_free {
        // Force installation: remove any pre-existing segment file.
        fd::unlink_file::call(&path);
    } else {
        // Find a free slot.
        while fd::file_exists::call(&path)? {
            if *segno >= max_segno {
                lwlock::LWLockRelease(control_file_lock)?;
                return Ok(false);
            }
            *segno += 1;
            path = XLogFilePath(tli, *segno, seg);
        }
    }

    // durable_rename(tmppath, path, LOG).
    if fd::rename_file::call(tmppath, &path) != 0 {
        lwlock::LWLockRelease(control_file_lock)?;
        return Ok(false);
    }
    // Durability of the rename: fsync the containing directory.
    fd::fsync_fname::call(XLOGDIR, true)?;

    lwlock::LWLockRelease(control_file_lock)?;
    Ok(true)
}

/// `SetInstallXLogFileSegmentActive(void)` (xlog.c:9554) — enable WAL file
/// recycling and preallocation by setting the `XLogCtl->InstallXLogFileSegmentActive`
/// flag under `ControlFileLock` in exclusive mode.
pub fn SetInstallXLogFileSegmentActive() -> PgResult<()> {
    let control_file_lock = lwlock::main_lock_ref(CONTROL_FILE_LOCK);
    lwlock::LWLockAcquire(control_file_lock, LW_EXCLUSIVE, globals::MyProcNumber())?;
    // SAFETY: live shmem region; ControlFileLock held exclusively.
    let ctl = xlog_ctl();
    if !ctl.is_null() {
        unsafe { (*ctl).InstallXLogFileSegmentActive = true };
    }
    lwlock::LWLockRelease(control_file_lock)?;
    Ok(())
}

/// `ResetInstallXLogFileSegmentActive(void)` (xlog.c:9563) — disable WAL file
/// recycling and preallocation by clearing the `XLogCtl->InstallXLogFileSegmentActive`
/// flag under `ControlFileLock` in exclusive mode.
pub fn ResetInstallXLogFileSegmentActive() -> PgResult<()> {
    let control_file_lock = lwlock::main_lock_ref(CONTROL_FILE_LOCK);
    lwlock::LWLockAcquire(control_file_lock, LW_EXCLUSIVE, globals::MyProcNumber())?;
    // SAFETY: live shmem region; ControlFileLock held exclusively.
    let ctl = xlog_ctl();
    if !ctl.is_null() {
        unsafe { (*ctl).InstallXLogFileSegmentActive = false };
    }
    lwlock::LWLockRelease(control_file_lock)?;
    Ok(())
}

/// `IsInstallXLogFileSegmentActive(void)` (xlog.c:9571) — read the
/// `XLogCtl->InstallXLogFileSegmentActive` flag under `ControlFileLock` in
/// shared mode.
pub fn IsInstallXLogFileSegmentActive() -> bool {
    let control_file_lock = lwlock::main_lock_ref(CONTROL_FILE_LOCK);
    // The read seam (consumed by the recovery page-read driver) cannot return a
    // PgResult; the lock acquire is infallible in practice here. Acquire/read/
    // release mirroring the C exactly.
    lwlock::LWLockAcquire(control_file_lock, LW_SHARED, globals::MyProcNumber())
        .expect("ControlFileLock acquire failed in IsInstallXLogFileSegmentActive");
    // SAFETY: live shmem region; ControlFileLock held in shared mode.
    let ctl = xlog_ctl();
    let result = if ctl.is_null() {
        false
    } else {
        unsafe { (*ctl).InstallXLogFileSegmentActive }
    };
    lwlock::LWLockRelease(control_file_lock)
        .expect("ControlFileLock release failed in IsInstallXLogFileSegmentActive");
    result
}

/// `XLogShutdownWalRcv(void)` (xlog.c:9546) — a thin wrapper around
/// `ShutdownWalRcv()` (walreceiverfuncs.c) followed by
/// `ResetInstallXLogFileSegmentActive()`.
///
/// `ShutdownWalRcv` lives in the walreceiverfuncs owner; it is reached through
/// the `shutdown_wal_rcv` seam. The `ResetInstallXLogFileSegmentActive` call
/// touches the xlog-owned `XLogCtl` flag directly.
pub fn XLogShutdownWalRcv() -> PgResult<()> {
    walrcv::shutdown_wal_rcv::call();
    ResetInstallXLogFileSegmentActive()?;
    Ok(())
}

/// `XLogFileOpen(segno, tli)` (xlog.c:3637) — open a pre-existing segment for
/// writing.
pub fn XLogFileOpen(segno: XLogSegNo, tli: TimeLineID) -> PgResult<i32> {
    let path = XLogFilePath(tli, segno, wal_segment_size());
    let flags = O_RDWR | PG_BINARY | O_CLOEXEC | get_sync_bit(wal_sync_method());
    match fd::basic_open_file_flags::call(&path, flags) {
        Ok(fd) => Ok(fd),
        Err(e) => Err(PgError::error(std::format!(
            "could not open file \"{}\": {}",
            path,
            std::io::Error::from_raw_os_error(e)
        ))),
    }
}

/// `XLogFileClose(void)` (xlog.c:3659) — close the current write segment.
fn XLogFileClose() -> PgResult<()> {
    let fd = OPEN_LOG_FILE.with(Cell::get);
    debug_assert!(fd >= 0);
    // The posix_fadvise(DONTNEED) is a cache hint we omit (behaviour-preserving).
    // SAFETY: closing a live bare kernel fd.
    let rc = unsafe { libc::close(fd) };
    if rc != 0 {
        let e = fd::last_errno::call();
        let xlogfname = XLogFileName(
            OPEN_LOG_TLI.with(Cell::get),
            OPEN_LOG_SEG_NO.with(Cell::get),
            wal_segment_size(),
        );
        return Err(PgError::error(std::format!(
            "could not close file \"{}\": {}",
            xlogfname,
            std::io::Error::from_raw_os_error(e)
        )));
    }
    OPEN_LOG_FILE.with(|c| c.set(-1));
    // ReleaseExternalFD (fd.c-internal max_safe_fds accounting) omitted — paired
    // with the omitted ReserveExternalFD; behaviour-preserving for the write
    // mechanics.
    Ok(())
}

// ===========================================================================
// get_sync_bit / issue_xlog_fsync (xlog.c:8677 / 8768).
// ===========================================================================

/// `get_sync_bit(method)` (xlog.c:8677) — the `open(2)` sync flag for the WAL
/// sync method. (The `io_direct_flags` / O_DIRECT WAL path is not modeled; it is
/// a behaviour-preserving omission of an optimization GUC.)
fn get_sync_bit(method: WalSyncMethod) -> i32 {
    if !enable_fsync() {
        return 0;
    }
    match method {
        WalSyncMethod::Fsync
        | WalSyncMethod::FsyncWritethrough
        | WalSyncMethod::Fdatasync => 0,
        WalSyncMethod::Open => libc::O_SYNC,
        WalSyncMethod::OpenDsync => libc::O_DSYNC,
    }
}

/// `issue_xlog_fsync(fd, segno, tli)` (xlog.c:8768) — issue the appropriate
/// fsync (if any) for a WAL output file.
pub fn issue_xlog_fsync(fd: i32, segno: XLogSegNo, tli: TimeLineID) -> PgResult<()> {
    debug_assert!(tli != 0);

    let method = wal_sync_method();
    // Quick exit if fsync is disabled or write() already synced.
    if !enable_fsync() || method == WalSyncMethod::Open || method == WalSyncMethod::OpenDsync {
        return Ok(());
    }

    // The repo's `pg_fsync` seam honors wal_sync_method internally (fsync vs
    // fdatasync vs writethrough) the same way fd.c's pg_fsync dispatches; a
    // nonzero return is the C `ereport(PANIC)` surface.
    let rc = fd::pg_fsync::call(fd);
    if rc != 0 {
        let e = if rc < 0 { -rc } else { fd::last_errno::call() };
        let xlogfname = XLogFileName(tli, segno, wal_segment_size());
        return Err(PgError::error(std::format!(
            "could not fsync file \"{}\": {}",
            xlogfname,
            std::io::Error::from_raw_os_error(e)
        )));
    }
    Ok(())
}

/// `WALRead(state, buf, startptr, count, tli, &errinfo)` (xlogreader.c:1514),
/// reshaped for the `wal_read` seam: read `count` bytes of WAL beginning at
/// `startptr` on timeline `tli` and return them owned. The C drives the read
/// through the reader's cached `seg`/`segcxt` (open one segment, pread, advance,
/// re-open at each segment boundary) using the reader routine's `segment_open`
/// callback (`wal_segment_open`, which opens `pg_wal/<segfile>`). The seam has
/// no reader to cache an fd in, so this opens each spanned segment with a plain
/// `BasicOpenFile(O_RDONLY)` and closes it before advancing — the same bytes are
/// read, only without the cross-call fd cache (a single 2PC prepare record never
/// spans more than the one or two segments it covers here).
pub fn wal_read(
    startptr: XLogRecPtr,
    count: i32,
    tli: TimeLineID,
) -> transam_xlog_seams::WalReadOutcome {
    use transam_xlog_seams::{WalReadErrorInfo, WalReadOutcome};

    let seg = wal_segment_size();
    let mut out = std::vec![0u8; count.max(0) as usize];
    let mut recptr = startptr;
    let mut nbytes = count as i64;
    let mut p: usize = 0;

    while nbytes > 0 {
        let startoff = XLogSegmentOffset(recptr, seg);
        let segno = XLByteToSeg(recptr, seg);

        // open(pg_wal/<segfile>, O_RDONLY) — the C wal_segment_open callback.
        let path = XLogFilePath(tli, segno, seg);
        let fd = match fd::basic_open_file_flags::call(&path, O_RDONLY | PG_BINARY | O_CLOEXEC) {
            Ok(fd) if fd >= 0 => fd,
            Ok(_) | Err(_) => {
                let e = fd::last_errno::call();
                return WalReadOutcome::Error(WalReadErrorInfo {
                    wre_errno: e,
                    wre_off: startoff as i32,
                    wre_req: 0,
                    wre_read: -1,
                    wre_seg_segno: segno,
                    wre_seg_tli: tli,
                });
            }
        };

        // How many bytes are within this segment?
        let segbytes: i64 = if nbytes > (seg as i64 - startoff as i64) {
            seg as i64 - startoff as i64
        } else {
            nbytes
        };

        let readbytes = fd::pg_pread::call(fd, &mut out[p..p + segbytes as usize], startoff as i64);
        if readbytes <= 0 {
            let e = if readbytes < 0 { fd::last_errno::call() } else { 0 };
            close_bare_fd(fd);
            return WalReadOutcome::Error(WalReadErrorInfo {
                wre_errno: e,
                wre_off: startoff as i32,
                wre_req: segbytes as i32,
                wre_read: readbytes as i32,
                wre_seg_segno: segno,
                wre_seg_tli: tli,
            });
        }
        close_bare_fd(fd);

        recptr += readbytes as u64;
        nbytes -= readbytes as i64;
        p += readbytes as usize;
    }

    WalReadOutcome::Ok(out)
}
