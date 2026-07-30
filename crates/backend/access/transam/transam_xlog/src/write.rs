use std::cell::Cell;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use elog::ereport;
use lwlock::{LWLockAcquire, LWLockAcquireOrWait, LWLockConditionalAcquire, LWLockRelease, LW_EXCLUSIVE, LW_SHARED};
use types_core::{TimeLineID, XLogRecPtr, XLogSegNo};
use types_error::{ErrorLocation, PgError, PgResult, ERROR, LOG, PANIC};

use crate::ctl::{ControlFileLock, NextBufIdx, WALWriteLock, XLogCtl, XLogRecPtrToBufIdx};
use crate::insert::{WaitXLogInsertionsToFinish, XLogInsertAllowed};
use crate::*;

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

thread_local! {
    // (Write, Flush) — private copy of the shared log write/flush results.
    pub(crate) static LOGWRT_RESULT: Cell<(XLogRecPtr, XLogRecPtr)> = const { Cell::new((0, 0)) };
    static OPEN_LOG_FILE: Cell<i32> = const { Cell::new(-1) };
    static OPEN_LOG_SEG_NO: Cell<XLogSegNo> = const { Cell::new(0) };
    static OPEN_LOG_TLI: Cell<TimeLineID> = const { Cell::new(0) };
    pub(crate) static LOCAL_MIN_RECOVERY_POINT: Cell<XLogRecPtr> = const { Cell::new(0) };
    pub(crate) static LOCAL_MIN_RECOVERY_POINT_TLI: Cell<TimeLineID> = const { Cell::new(0) };
    static UPDATE_MIN_RECOVERY_POINT: Cell<bool> = const { Cell::new(true) };
    // wake_wal_senders (xlog.c): set after an fsync in XLogWrite, consumed at the
    // XLogWrite / XLogBackgroundFlush tails. Per-backend like C's global (set and
    // read within one flushing backend).
    static WAKE_WAL_SENDERS: Cell<bool> = const { Cell::new(false) };
}

// WalSndWakeupRequest() (walsender.h).
fn WalSndWakeupRequest() {
    WAKE_WAL_SENDERS.set(true);
}

// WalSndWakeupProcessRequests(physical, logical) (walsender.h). The
// WAKE_WAL_SENDERS early-out keeps read-only backends (no WAL fsync) off the
// wakeup machinery entirely; the seam is skipped when walsender is unlinked.
fn WalSndWakeupProcessRequests(physical: bool, logical: bool) {
    if !WAKE_WAL_SENDERS.get() {
        return;
    }
    WAKE_WAL_SENDERS.set(false);
    // is_installed() first: standalone transam_xlog tests do not link walsender
    // (nor install GUCs), so short-circuit before reading max_wal_senders.
    if walsender_seams::wal_snd_wakeup::is_installed()
        && guc_tables::vars::max_wal_senders.read() > 0
    {
        walsender_seams::wal_snd_wakeup::call(physical, logical);
    }
}

// Flush is read before Write with an intervening barrier so Flush always
// trails the Write value seen (C's RefreshXLogWriteResult).
pub(crate) fn RefreshXLogWriteResult() {
    let ctl = XLogCtl();
    let flush = ctl.logFlushResult.load(Acquire);
    let write = ctl.logWriteResult.load(Acquire);
    LOGWRT_RESULT.set((write, flush));
}

pub(crate) fn set_logwrt_result(write: XLogRecPtr, flush: XLogRecPtr) {
    LOGWRT_RESULT.set((write, flush));
}

// assign_wal_sync_method (xlog.c): a changed sync method invalidates the
// open-flag/fsync posture of the open segment; close it (fsync first).
pub(crate) fn assign_wal_sync_method(new_val: i32, _extra: Option<&guc_tables::GucHookExtra>) {
    if wal_sync_method() != new_val && OPEN_LOG_FILE.get() >= 0 {
        if fd::pg_fsync(OPEN_LOG_FILE.get()) != 0 {
            panic!(
                "could not fsync file \"{}\"",
                XLogFileName(OPEN_LOG_TLI.get(), OPEN_LOG_SEG_NO.get(), wal_segment_size())
            );
        }
        let _ = XLogFileClose();
    }
}

fn wal_sync_method() -> i32 {
    guc_tables::vars::wal_sync_method.read()
}

/// M4 walwriter job overlay: stamp wal_sync_method into the EXECUTING
/// worker's cell with the assign-hook semantics — a changed method
/// invalidates the open segment's sync posture, so the open WAL file (per
/// worker, like C's per-backend openLogFile) is fsync'd + closed first
/// (assign_wal_sync_method). Raw var writes must not bypass that.
pub fn stamp_wal_sync_method(new_val: i32) {
    assign_wal_sync_method(new_val, None);
    guc_tables::vars::wal_sync_method.write(new_val);
}

fn wal_io_start() -> i64 {
    pgstat::io::pgstat_prepare_io_time(guc_tables::vars::track_wal_io_timing.read())
}

fn count_wal_io(io_context: pgstat::io::IOContext, io_op: pgstat::io::IOOp, start_ns: i64, bytes: u64) {
    pgstat::io::pgstat_count_io_op_time(pgstat::io::IOObject::Wal, io_context, io_op, start_ns, 1, bytes);
}

fn get_sync_bit(method: i32) -> i32 {
    if !init_small::globals::enableFsync() {
        return 0;
    }
    match method {
        WAL_SYNC_METHOD_FSYNC | WAL_SYNC_METHOD_FSYNC_WRITETHROUGH | WAL_SYNC_METHOD_FDATASYNC => 0,
        WAL_SYNC_METHOD_OPEN => libc::O_SYNC,
        WAL_SYNC_METHOD_OPEN_DSYNC => libc::O_DSYNC,
        _ => panic!("unrecognized \"wal_sync_method\": {method}"),
    }
}

pub fn issue_xlog_fsync(fd: i32, segno: XLogSegNo, tli: TimeLineID) -> PgResult<()> {
    let method = wal_sync_method();
    if !init_small::globals::enableFsync()
        || method == WAL_SYNC_METHOD_OPEN
        || method == WAL_SYNC_METHOD_OPEN_DSYNC
    {
        return Ok(());
    }
    let start_ns = wal_io_start();
    let rc = match method {
        WAL_SYNC_METHOD_FSYNC => fd::pg_fsync_no_writethrough(fd),
        WAL_SYNC_METHOD_FSYNC_WRITETHROUGH => fd::pg_fsync_writethrough(fd),
        WAL_SYNC_METHOD_FDATASYNC => fd::pg_fdatasync(fd),
        _ => panic!("unrecognized \"wal_sync_method\": {method}"),
    };
    if rc != 0 {
        let fname = XLogFileName(tli, segno, wal_segment_size());
        return ereport(PANIC)
            .errmsg(format!("could not fsync file \"{fname}\""))
            .finish(loc("issue_xlog_fsync"));
    }
    count_wal_io(pgstat::io::IOContext::IOCONTEXT_NORMAL, pgstat::io::IOOp::Fsync, start_ns, 0);
    Ok(())
}

pub(crate) fn InstallXLogFileSegment(
    segno: &mut XLogSegNo,
    tmppath: &str,
    find_free: bool,
    max_segno: XLogSegNo,
    tli: TimeLineID,
) -> PgResult<bool> {
    debug_assert!(tli != 0);
    let mut path = XLogFilePath(tli, *segno, wal_segment_size());

    LWLockAcquire(ControlFileLock(), LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
    if !XLogCtl().InstallXLogFileSegmentActive.load(Relaxed) {
        LWLockRelease(ControlFileLock())?;
        return Ok(false);
    }

    if !find_free {
        fd::durable_unlink(&path, types_error::DEBUG1).ok();
    } else {
        // stat(2) existence probe, as in C's InstallXLogFileSegment.
        let mut fi = fd::FileInfo::zeroed();
        while fd::pg_stat(&path, &mut fi) == 0 {
            if *segno >= max_segno {
                LWLockRelease(ControlFileLock())?;
                return Ok(false);
            }
            *segno += 1;
            path = XLogFilePath(tli, *segno, wal_segment_size());
        }
    }

    if fd::durable_rename(tmppath, &path, LOG).map(|rc| rc != 0).unwrap_or(true) {
        LWLockRelease(ControlFileLock())?;
        return Ok(false);
    }
    LWLockRelease(ControlFileLock())?;
    Ok(true)
}

fn XLogFileInitInternal(
    logsegno: XLogSegNo,
    logtli: TimeLineID,
    added: &mut bool,
    path: &mut String,
) -> PgResult<i32> {
    debug_assert!(logtli != 0);
    *path = XLogFilePath(logtli, logsegno, wal_segment_size());
    *added = false;

    let open_flags = libc::O_RDWR | libc::O_CLOEXEC | get_sync_bit(wal_sync_method());
    match fd::BasicOpenFile(path, open_flags) {
        Ok(f) if f >= 0 => return Ok(f),
        res => {
            let _ = res?;
            // As in C: only ENOENT from the failed open falls through to
            // segment creation; anything else (EACCES, a directory in the
            // way, ...) reports the open failure.
            let en = fd::get_errno();
            if en != libc::ENOENT {
                return ereport(ERROR)
                    .with_saved_errno(en)
                    .errcode_for_file_access()
                    .errmsg(format!("could not open file \"{path}\": %m"))
                    .finish(loc("XLogFileInitInternal"))
                    .map(|_| -1);
            }
        }
    }

    // Merge composition (dst/p4-simnet): p5's wasm-safe process_id seam
    // (std::process::id aborts on wasm32-wasip1) + substrate's vfs-routed
    // unlink.
    let tmppath = format!("{XLOGDIR}/xlogtemp.{}", init_small::globals::process_id());
    let _ = fd::pg_unlink(&tmppath);

    let f = fd::BasicOpenFile(&tmppath, libc::O_RDWR | libc::O_CREAT | libc::O_EXCL)?;

    let wal_segsz = wal_segment_size();
    let mut save_err: Option<std::io::Error> = None;
    let io_start = wal_io_start();
    let init_zero = guc_tables::vars::wal_init_zero.read();
    if init_zero {
        let rc = fd::io::pg_pwrite_zeros(f, wal_segsz as usize, 0);
        if rc < 0 {
            save_err = Some(std::io::Error::last_os_error());
        }
    } else {
        // One byte at segment end.
        let rc = fd::pg_pwrite(f, b"\0", (wal_segsz - 1) as i64);
        if rc != 1 {
            save_err = Some(std::io::Error::last_os_error());
        }
    }
    count_wal_io(
        pgstat::io::IOContext::IOCONTEXT_INIT,
        pgstat::io::IOOp::Write,
        io_start,
        if init_zero { wal_segsz as u64 } else { 1 },
    );
    if let Some(e) = save_err {
        let _ = fd::pg_unlink(&tmppath);
        // fd owned here.
        fd::pg_close(f);
        return ereport(ERROR)
            .errmsg(format!("could not write to file \"{tmppath}\": {e}"))
            .finish(loc("XLogFileInitInternal"))
            .map(|_| -1);
    }

    let io_start = wal_io_start();
    if fd::pg_fsync(f) != 0 {
        // fd owned here.
        fd::pg_close(f);
        return ereport(ERROR)
            .errmsg(format!("could not fsync file \"{tmppath}\""))
            .finish(loc("XLogFileInitInternal"))
            .map(|_| -1);
    }
    count_wal_io(pgstat::io::IOContext::IOCONTEXT_INIT, pgstat::io::IOOp::Fsync, io_start, 0);
    // fd owned here.
    if fd::pg_close(f) != 0 {
        return ereport(ERROR)
            .errmsg(format!("could not close file \"{tmppath}\""))
            .finish(loc("XLogFileInitInternal"))
            .map(|_| -1);
    }

    let mut installed_segno = logsegno;
    let max_segno = logsegno + CheckPointSegments() as u64;
    if InstallXLogFileSegment(&mut installed_segno, &tmppath, true, max_segno, logtli)? {
        *added = true;
    } else {
        let _ = fd::pg_unlink(&tmppath);
    }
    Ok(-1)
}

pub fn XLogFileInit(logsegno: XLogSegNo, logtli: TimeLineID) -> PgResult<i32> {
    let mut ignore_added = false;
    let mut path = String::new();
    let f = XLogFileInitInternal(logsegno, logtli, &mut ignore_added, &mut path)?;
    if f >= 0 {
        return Ok(f);
    }
    let f = fd::BasicOpenFile(
        &path,
        libc::O_RDWR | libc::O_CLOEXEC | get_sync_bit(wal_sync_method()),
    )?;
    if f < 0 {
        return ereport(ERROR)
            .errmsg(format!("could not open file \"{path}\""))
            .finish(loc("XLogFileInit"))
            .map(|_| -1);
    }
    Ok(f)
}

pub(crate) fn set_update_min_recovery_point(v: bool) {
    UPDATE_MIN_RECOVERY_POINT.set(v);
}

pub(crate) fn XLogFileCopy(
    dest_tli: TimeLineID,
    destsegno: XLogSegNo,
    src_tli: TimeLineID,
    srcsegno: XLogSegNo,
    upto: i32,
) -> PgResult<()> {
    let wal_segsz = wal_segment_size();
    let path = XLogFilePath(src_tli, srcsegno, wal_segsz);
    let srcfd = fd::OpenTransientFile(&path, libc::O_RDONLY)?;
    if srcfd < 0 {
        return ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{path}\""))
            .finish(loc("XLogFileCopy"));
    }

    let tmppath = format!("{XLOGDIR}/xlogtemp.{}", std::process::id());
    let _ = std::fs::remove_file(&tmppath);

    // No get_sync_bit(): fsync only once at end of fill.
    let f = fd::OpenTransientFile(&tmppath, libc::O_RDWR | libc::O_CREAT | libc::O_EXCL)?;
    if f < 0 {
        fd::CloseTransientFile(srcfd);
        return ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not create file \"{tmppath}\""))
            .finish(loc("XLogFileCopy"));
    }

    let mut buffer = vec![0u8; XLOG_BLCKSZ];
    let mut nbytes: i32 = 0;
    while nbytes < wal_segsz {
        let mut nread = upto - nbytes;
        if nread < XLOG_BLCKSZ as i32 {
            buffer.fill(0);
        }
        if nread > 0 {
            if nread > XLOG_BLCKSZ as i32 {
                nread = XLOG_BLCKSZ as i32;
            }
            // SAFETY: srcfd open; buffer holds >= nread bytes.
            let r = unsafe { libc::read(srcfd, buffer.as_mut_ptr().cast(), nread as usize) };
            if r != nread as isize {
                let builder = ereport(ERROR);
                let builder = if r < 0 {
                    builder
                        .errcode_for_file_access()
                        .errmsg(format!("could not read file \"{path}\""))
                } else {
                    builder.errmsg(format!(
                        "could not read file \"{path}\": read {r} of {nread}"
                    ))
                };
                return builder.finish(loc("XLogFileCopy"));
            }
        }
        // SAFETY: f open; buffer is XLOG_BLCKSZ bytes.
        let w = unsafe { libc::write(f, buffer.as_ptr().cast(), XLOG_BLCKSZ) };
        if w != XLOG_BLCKSZ as isize {
            let e = std::io::Error::last_os_error();
            let _ = std::fs::remove_file(&tmppath);
            return ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!("could not write to file \"{tmppath}\": {e}"))
                .finish(loc("XLogFileCopy"));
        }
        nbytes += XLOG_BLCKSZ as i32;
    }

    if fd::pg_fsync(f) != 0 {
        return ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not fsync file \"{tmppath}\""))
            .finish(loc("XLogFileCopy"));
    }
    if fd::CloseTransientFile(f) != 0 {
        return ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{tmppath}\""))
            .finish(loc("XLogFileCopy"));
    }
    if fd::CloseTransientFile(srcfd) != 0 {
        return ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{path}\""))
            .finish(loc("XLogFileCopy"));
    }

    let mut installed_segno = destsegno;
    if !InstallXLogFileSegment(&mut installed_segno, &tmppath, false, 0, dest_tli)? {
        return Err(Box::new(PgError::new(
            ERROR,
            "InstallXLogFileSegment should not have failed",
        )));
    }
    Ok(())
}

pub fn XLogFileOpen(segno: XLogSegNo, tli: TimeLineID) -> PgResult<i32> {
    let path = XLogFilePath(tli, segno, wal_segment_size());
    match fd::BasicOpenFile(&path, libc::O_RDWR | libc::O_CLOEXEC | get_sync_bit(wal_sync_method()))
    {
        Ok(f) if f >= 0 => Ok(f),
        _ => ereport(PANIC)
            .errmsg(format!("could not open file \"{path}\""))
            .finish(loc("XLogFileOpen"))
            .map(|_| -1),
    }
}

fn XLogFileClose() -> PgResult<()> {
    let f = OPEN_LOG_FILE.get();
    debug_assert!(f >= 0);
    // fd tracked by OPEN_LOG_FILE.
    if fd::pg_close(f) != 0 {
        let fname = XLogFileName(OPEN_LOG_TLI.get(), OPEN_LOG_SEG_NO.get(), wal_segment_size());
        return ereport(PANIC)
            .errmsg(format!("could not close file \"{fname}\""))
            .finish(loc("XLogFileClose"));
    }
    OPEN_LOG_FILE.set(-1);
    fd::ReleaseExternalFD();
    Ok(())
}

pub(crate) fn PreallocXlogFiles(endptr: XLogRecPtr, tli: TimeLineID) -> PgResult<()> {
    if !XLogCtl().InstallXLogFileSegmentActive.load(Relaxed) {
        return Ok(());
    }
    let wal_segsz = wal_segment_size();
    let mut segno = XLByteToPrevSeg(endptr, wal_segsz);
    let offset = XLogSegmentOffset(endptr - 1, wal_segsz);
    if offset as f64 >= 0.75 * wal_segsz as f64 {
        segno += 1;
        let mut added = false;
        let mut path = String::new();
        let lf = XLogFileInitInternal(segno, tli, &mut added, &mut path)?;
        if lf >= 0 {
            // fd returned open by XLogFileInitInternal.
            fd::pg_close(lf);
        }
        if added {
            crate::startup::checkpoint_stats_bump_segs_added();
        }
    }
    Ok(())
}

// (Write, Flush) request pair.
pub(crate) fn XLogWrite(write_rqst: (XLogRecPtr, XLogRecPtr), tli: TimeLineID, flexible: bool) -> PgResult<()> {
    let ctl = XLogCtl();
    debug_assert!(init_small::globals::CritSectionCount() > 0);

    RefreshXLogWriteResult();

    let mut npages = 0usize;
    let mut startidx = 0i32;
    let mut startoffset: u32 = 0;
    let mut curridx = XLogRecPtrToBufIdx(LOGWRT_RESULT.get().0);

    let (rqst_write, rqst_flush) = write_rqst;
    let mut ispartialpage;

    while LOGWRT_RESULT.get().0 < rqst_write {
        let (mut lw_write, lw_flush) = LOGWRT_RESULT.get();
        let end_ptr = ctl.xlblocks[curridx as usize].load(Acquire);
        if lw_write >= end_ptr {
            return ereport(PANIC)
                .errmsg(format!(
                    "xlog write request {:X}/{:X} is past end of log {:X}/{:X}",
                    lw_write >> 32, lw_write & 0xFFFF_FFFF, end_ptr >> 32, end_ptr & 0xFFFF_FFFF
                ))
                .finish(loc("XLogWrite"));
        }
        lw_write = end_ptr;
        LOGWRT_RESULT.set((lw_write, lw_flush));
        ispartialpage = rqst_write < lw_write;

        let wal_segsz = wal_segment_size();
        if !XLByteInPrevSeg(lw_write, OPEN_LOG_SEG_NO.get(), wal_segsz) {
            debug_assert_eq!(npages, 0);
            if OPEN_LOG_FILE.get() >= 0 {
                XLogFileClose()?;
            }
            OPEN_LOG_SEG_NO.set(XLByteToPrevSeg(lw_write, wal_segsz));
            OPEN_LOG_TLI.set(tli);
            OPEN_LOG_FILE.set(XLogFileInit(OPEN_LOG_SEG_NO.get(), tli)?);
            fd::ReserveExternalFD()?;
        }
        if OPEN_LOG_FILE.get() < 0 {
            OPEN_LOG_SEG_NO.set(XLByteToPrevSeg(lw_write, wal_segsz));
            OPEN_LOG_TLI.set(tli);
            OPEN_LOG_FILE.set(XLogFileOpen(OPEN_LOG_SEG_NO.get(), tli)?);
            fd::ReserveExternalFD()?;
        }

        if npages == 0 {
            startidx = curridx;
            startoffset = XLogSegmentOffset(lw_write - XLOG_BLCKSZ as u64, wal_segsz);
        }
        npages += 1;

        let last_iteration = rqst_write <= lw_write;
        let finishing_seg =
            !ispartialpage && startoffset as usize + npages * XLOG_BLCKSZ >= wal_segsz as usize;

        if last_iteration || curridx == ctl.XLogCacheBlck || finishing_seg {
            let mut from = ctl.page_ptr(startidx as usize);
            let mut nleft = npages * XLOG_BLCKSZ;
            loop {
                let io_start = wal_io_start();
                // SAFETY: [from, from+nleft) lies inside the WAL buffer
                // array; the insert protocol guarantees these pages are
                // fully written (WaitXLogInsertionsToFinish ran).
                // THE WAL write hot path — fd::pg_pwrite is an #[inline]
                // shim chain down to the same libc::pwrite (zero-cost gate:
                // /asm-diff FileWriteV-class letters).
                let written = fd::pg_pwrite(
                    OPEN_LOG_FILE.get(),
                    unsafe { std::slice::from_raw_parts(from.cast::<u8>(), nleft) },
                    startoffset as i64,
                );
                count_wal_io(
                    pgstat::io::IOContext::IOCONTEXT_NORMAL,
                    pgstat::io::IOOp::Write,
                    io_start,
                    written.max(0) as u64,
                );
                if written <= 0 {
                    let e = std::io::Error::last_os_error();
                    if e.kind() == std::io::ErrorKind::Interrupted {
                        continue;
                    }
                    let fname = XLogFileName(tli, OPEN_LOG_SEG_NO.get(), wal_segsz);
                    return ereport(PANIC)
                        .errmsg(format!(
                            "could not write to log file \"{fname}\" at offset {startoffset}, length {nleft}: {e}"
                        ))
                        .finish(loc("XLogWrite"));
                }
                nleft -= written as usize;
                // SAFETY: written <= nleft.
                from = unsafe { from.add(written as usize) };
                startoffset += written as u32;
                if nleft == 0 {
                    break;
                }
            }
            npages = 0;

            if finishing_seg {
                issue_xlog_fsync(OPEN_LOG_FILE.get(), OPEN_LOG_SEG_NO.get(), tli)?;
                WalSndWakeupRequest();
                LOGWRT_RESULT.set((lw_write, lw_write));

                if XLogArchivingActive() {
                    xlogarchive_seams::xlog_archive_notify_seg::call(
                        OPEN_LOG_SEG_NO.get(),
                        tli,
                    )?;
                }

                ctl.lastSegSwitchTime.store(crate::now_pg_time(), Relaxed);
                ctl.lastSegSwitchLSN.store(LOGWRT_RESULT.get().1, Relaxed);

                if init_small::globals::IsUnderPostmaster()
                    && XLogCheckpointNeeded(OPEN_LOG_SEG_NO.get())
                {
                    crate::insert::GetRedoRecPtr();
                    if XLogCheckpointNeeded(OPEN_LOG_SEG_NO.get())
                        && checkpointer_seams::request_checkpoint::is_installed()
                    {
                        checkpointer_seams::request_checkpoint::call(CHECKPOINT_CAUSE_XLOG)?;
                    }
                }
            }
        }

        if ispartialpage {
            let (_, f) = LOGWRT_RESULT.get();
            LOGWRT_RESULT.set((rqst_write, f));
            break;
        }
        curridx = NextBufIdx(curridx);
        if flexible && npages == 0 {
            break;
        }
    }

    debug_assert_eq!(npages, 0);

    let (lw_write, lw_flush) = LOGWRT_RESULT.get();
    if lw_flush < rqst_flush && lw_flush < lw_write {
        let method = wal_sync_method();
        if method != WAL_SYNC_METHOD_OPEN && method != WAL_SYNC_METHOD_OPEN_DSYNC {
            let wal_segsz = wal_segment_size();
            if OPEN_LOG_FILE.get() >= 0
                && !XLByteInPrevSeg(lw_write, OPEN_LOG_SEG_NO.get(), wal_segsz)
            {
                XLogFileClose()?;
            }
            if OPEN_LOG_FILE.get() < 0 {
                OPEN_LOG_SEG_NO.set(XLByteToPrevSeg(lw_write, wal_segsz));
                OPEN_LOG_TLI.set(tli);
                OPEN_LOG_FILE.set(XLogFileOpen(OPEN_LOG_SEG_NO.get(), tli)?);
                fd::ReserveExternalFD()?;
            }
            issue_xlog_fsync(OPEN_LOG_FILE.get(), OPEN_LOG_SEG_NO.get(), tli)?;
        }
        // C signals the walsender wakeup OUTSIDE the sync-method guard
        // (xlog.c:2553): with wal_sync_method=open/open_dsync the flush
        // advances without an explicit fsync and walsenders must still be
        // woken, or sync-rep/catchup waits stall until the next write.
        WalSndWakeupRequest();
        LOGWRT_RESULT.set((lw_write, lw_write));
    }

    let (lw_write, lw_flush) = LOGWRT_RESULT.get();
    ctl.info_lck.with(|| {
        if ctl.LogwrtRqstWrite.load(Relaxed) < lw_write {
            ctl.LogwrtRqstWrite.store(lw_write, Relaxed);
        }
        if ctl.LogwrtRqstFlush.load(Relaxed) < lw_flush {
            ctl.LogwrtRqstFlush.store(lw_flush, Relaxed);
        }
    });

    // Write published before Flush (readers see Flush trailing Write).
    ctl.logWriteResult.store(lw_write, Release);
    ctl.logFlushResult.store(lw_flush, Release);

    // Wake up walsenders once the new flush position is published so a woken
    // sender reads the advanced LSN (C wakes after END_CRIT_SECTION).
    WalSndWakeupProcessRequests(true, !crate::insert::RecoveryInProgress());
    Ok(())
}

pub(crate) fn UpdateMinRecoveryPoint(lsn: XLogRecPtr, force: bool) -> PgResult<()> {
    if !UPDATE_MIN_RECOVERY_POINT.get() || (!force && lsn <= LOCAL_MIN_RECOVERY_POINT.get()) {
        return Ok(());
    }
    if XLogRecPtrIsInvalid(LOCAL_MIN_RECOVERY_POINT.get()) && xlogutils::in_recovery() {
        UPDATE_MIN_RECOVERY_POINT.set(false);
        return Ok(());
    }

    LWLockAcquire(ControlFileLock(), LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
    let cf = control_file::control_file();
    LOCAL_MIN_RECOVERY_POINT.set(cf.minRecoveryPoint);
    LOCAL_MIN_RECOVERY_POINT_TLI.set(cf.minRecoveryPointTLI);

    if XLogRecPtrIsInvalid(LOCAL_MIN_RECOVERY_POINT.get()) {
        UPDATE_MIN_RECOVERY_POINT.set(false);
    } else if force || LOCAL_MIN_RECOVERY_POINT.get() < lsn {
        let (new_mrp, new_mrp_tli) = xlogrecovery_seams::get_current_replay_rec_ptr::call();
        if !force && new_mrp < lsn {
            let _ = elog::elog(
                types_error::WARNING,
                format!(
                    "xlog min recovery request {:X}/{:X} is past current point {:X}/{:X}",
                    lsn >> 32, lsn & 0xFFFF_FFFF, new_mrp >> 32, new_mrp & 0xFFFF_FFFF
                ),
            );
        }
        if cf.minRecoveryPoint < new_mrp {
            control_file::control_file_update(|cf| {
                cf.minRecoveryPoint = new_mrp;
                cf.minRecoveryPointTLI = new_mrp_tli;
            });
            UpdateControlFile()?;
            LOCAL_MIN_RECOVERY_POINT.set(new_mrp);
            LOCAL_MIN_RECOVERY_POINT_TLI.set(new_mrp_tli);
        }
    }
    LWLockRelease(ControlFileLock())?;
    Ok(())
}

pub fn XLogFlush(record: XLogRecPtr) -> PgResult<()> {
    let ctl = XLogCtl();

    if !XLogInsertAllowed() {
        return UpdateMinRecoveryPoint(record, false);
    }
    if record <= LOGWRT_RESULT.get().1 {
        return Ok(());
    }

    let insert_tli = ctl.InsertTimeLineID.load(Relaxed);
    init_small::globals::StartCriticalSection();

    // GL-ERRFIX-1 E-2: every fallible step below sits INSIDE the critical
    // section opened above. Propagating with `?` from here skipped
    // EndCriticalSection and LEAKED CritSectionCount onto the calling thread
    // — and the caller can be a pool worker, whose Fatal arm never resets it,
    // so an innocent later statement on that thread would begin inside a
    // phantom critical section. The loop moves into a closure so the section
    // is always balanced; the error is carried out and re-raised after
    // EndCriticalSection. Control flow is otherwise line-for-line identical.
    let mut write_rqst_ptr = record;
    let loop_result: PgResult<()> = (|| {
        loop {
            RefreshXLogWriteResult();
            if record <= LOGWRT_RESULT.get().1 {
                break;
            }

            ctl.info_lck.with(|| {
                let w = ctl.LogwrtRqstWrite.load(Relaxed);
                if write_rqst_ptr < w {
                    write_rqst_ptr = w;
                }
            });
            let mut insertpos = WaitXLogInsertionsToFinish(write_rqst_ptr);

            if !LWLockAcquireOrWait(
                WALWriteLock(),
                LW_EXCLUSIVE,
                init_small::globals::MyProcNumber(),
            )? {
                continue;
            }
            RefreshXLogWriteResult();
            if record <= LOGWRT_RESULT.get().1 {
                LWLockRelease(WALWriteLock())?;
                break;
            }

            let commit_delay = guc_tables::vars::CommitDelay.read();
            if commit_delay > 0
                && init_small::globals::enableFsync()
                && procarray_seams::minimum_active_backends::is_installed()
                && procarray_seams::minimum_active_backends::call(
                    guc_tables::vars::CommitSiblings.read(),
                )
            {
                std::thread::sleep(std::time::Duration::from_micros(commit_delay as u64));
                insertpos = WaitXLogInsertionsToFinish(insertpos);
            }

            XLogWrite((insertpos, insertpos), insert_tli, false)?;
            LWLockRelease(WALWriteLock())?;
            break;
        }
        Ok(())
    })();

    init_small::globals::EndCriticalSection();
    loop_result?;

    if LOGWRT_RESULT.get().1 < record {
        return Err(Box::new(PgError::new(
            ERROR,
            format!(
                "xlog flush request {:X}/{:X} is not satisfied --- flushed only to {:X}/{:X}",
                record >> 32,
                record & 0xFFFF_FFFF,
                LOGWRT_RESULT.get().1 >> 32,
                LOGWRT_RESULT.get().1 & 0xFFFF_FFFF
            ),
        )));
    }
    Ok(())
}

pub fn XLogNeedsFlush(record: XLogRecPtr) -> bool {
    if crate::insert::RecoveryInProgress() {
        if XLogRecPtrIsInvalid(LOCAL_MIN_RECOVERY_POINT.get()) && xlogutils::in_recovery() {
            UPDATE_MIN_RECOVERY_POINT.set(false);
        }
        if record <= LOCAL_MIN_RECOVERY_POINT.get() || !UPDATE_MIN_RECOVERY_POINT.get() {
            return false;
        }
        let acquired = LWLockConditionalAcquire(ControlFileLock(), LW_SHARED).unwrap_or(false);
        if !acquired {
            return true;
        }
        let cf = control_file::control_file();
        LOCAL_MIN_RECOVERY_POINT.set(cf.minRecoveryPoint);
        LOCAL_MIN_RECOVERY_POINT_TLI.set(cf.minRecoveryPointTLI);
        let _ = LWLockRelease(ControlFileLock());
        if XLogRecPtrIsInvalid(LOCAL_MIN_RECOVERY_POINT.get()) {
            UPDATE_MIN_RECOVERY_POINT.set(false);
        }
        return !(record <= LOCAL_MIN_RECOVERY_POINT.get() || !UPDATE_MIN_RECOVERY_POINT.get());
    }

    if record <= LOGWRT_RESULT.get().1 {
        return false;
    }
    RefreshXLogWriteResult();
    record > LOGWRT_RESULT.get().1
}

pub fn XLogSetAsyncXactLSN(async_xact_lsn: XLogRecPtr) {
    let ctl = XLogCtl();
    let (sleeping, prev) = ctl.info_lck.with(|| {
        let sleeping = ctl.WalWriterSleeping.load(Relaxed);
        let prev = ctl.asyncXactLSN.load(Relaxed);
        if prev < async_xact_lsn {
            ctl.asyncXactLSN.store(async_xact_lsn, Relaxed);
        }
        (sleeping, prev)
    });

    if async_xact_lsn <= prev {
        return;
    }

    let wakeup = if sleeping {
        true
    } else {
        RefreshXLogWriteResult();
        let flushblocks =
            async_xact_lsn as i64 / XLOG_BLCKSZ as i64 - LOGWRT_RESULT.get().1 as i64 / XLOG_BLCKSZ as i64;
        let flush_after = guc_tables::vars::WalWriterFlushAfter.read();
        flush_after == 0 || flushblocks >= flush_after as i64
    };

    if wakeup {
        let walwriter = lmgr_proc::ProcGlobal().walwriterProc.load(Relaxed);
        if walwriter != types_core::INVALID_PROC_NUMBER {
            latch::SetLatch(types_storage::latch::LatchHandle::proc(walwriter));
        }
    }
}

/// C XLogBackgroundFlush's function-static `lastflush`, extracted to
/// CALLER-owned state (M4 walwriter migration): job-mode cycles hop pool
/// workers, and the previous thread-local would reset the flush pacing to
/// "flush immediately" on every hop, silently defeating
/// wal_writer_flush_after batching. The thread driver owns one on its
/// frame — behavior identical to the C static (walwriter is the only
/// caller).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalFlushPacing {
    last_flush: i64,
}

impl WalFlushPacing {
    pub const fn new() -> WalFlushPacing {
        WalFlushPacing { last_flush: 0 }
    }
}

/// The flush-or-defer decision (xlog.c XLogBackgroundFlush's pacing
/// block), pure for the deterministic unit tests: whether to flush up to
/// the write request now; advances the pacing clock when it does. C
/// ordering preserved exactly: flush_after==0 / first call → flush;
/// wal_writer_delay elapsed → flush; wal_writer_flush_after blocks
/// accumulated → flush; else defer.
pub fn wal_flush_pacing_decide(
    pacing: &mut WalFlushPacing,
    now: i64,
    flushblocks: i64,
    flush_after: i32,
    delay_us: i64,
) -> bool {
    if flush_after == 0
        || pacing.last_flush == 0
        || now - pacing.last_flush >= delay_us
        || flushblocks >= flush_after as i64
    {
        pacing.last_flush = now;
        true
    } else {
        false
    }
}

/// XLogBackgroundFlush (xlog.c).
pub fn XLogBackgroundFlush(pacing: &mut WalFlushPacing) -> PgResult<bool> {
    if crate::insert::RecoveryInProgress() {
        return Ok(false);
    }

    let ctl = XLogCtl();
    let insert_tli = ctl.InsertTimeLineID.load(Relaxed);

    let (mut write_rqst_write, mut write_rqst_flush) = ctl.info_lck.with(|| {
        (ctl.LogwrtRqstWrite.load(Relaxed), ctl.LogwrtRqstFlush.load(Relaxed))
    });
    let _ = write_rqst_flush;
    let mut flexible = true;

    write_rqst_write -= write_rqst_write % XLOG_BLCKSZ as u64;

    RefreshXLogWriteResult();
    if write_rqst_write <= LOGWRT_RESULT.get().1 {
        write_rqst_write = ctl.info_lck.with(|| ctl.asyncXactLSN.load(Relaxed));
        flexible = false;
    }

    if write_rqst_write <= LOGWRT_RESULT.get().1 {
        if OPEN_LOG_FILE.get() >= 0
            && !XLByteInPrevSeg(LOGWRT_RESULT.get().0, OPEN_LOG_SEG_NO.get(), wal_segment_size())
        {
            XLogFileClose()?;
        }
        return Ok(false);
    }

    let now = timestamp_seams::get_current_timestamp::call();
    let flushblocks =
        write_rqst_write as i64 / XLOG_BLCKSZ as i64 - LOGWRT_RESULT.get().1 as i64 / XLOG_BLCKSZ as i64;
    let flush_after = guc_tables::vars::WalWriterFlushAfter.read();
    let delay_us = guc_tables::vars::WalWriterDelay.read() as i64 * 1000;

    if wal_flush_pacing_decide(pacing, now, flushblocks, flush_after, delay_us) {
        write_rqst_flush = write_rqst_write;
    } else {
        write_rqst_flush = 0;
    }

    init_small::globals::StartCriticalSection();

    WaitXLogInsertionsToFinish(write_rqst_write);
    LWLockAcquire(WALWriteLock(), LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
    RefreshXLogWriteResult();
    if write_rqst_write > LOGWRT_RESULT.get().0 || write_rqst_flush > LOGWRT_RESULT.get().1 {
        XLogWrite((write_rqst_write, write_rqst_flush), insert_tli, flexible)?;
    }
    LWLockRelease(WALWriteLock())?;

    init_small::globals::EndCriticalSection();

    // Wake up walsenders now that we've released heavily contended locks.
    WalSndWakeupProcessRequests(true, !crate::insert::RecoveryInProgress());

    crate::insert::AdvanceXLInsertBuffer(InvalidXLogRecPtr, insert_tli, true);

    Ok(true)
}

pub fn SetWalWriterSleeping(sleeping: bool) {
    let ctl = XLogCtl();
    ctl.info_lck.with(|| ctl.WalWriterSleeping.store(sleeping, Relaxed));
}

pub fn GetLastSegSwitchData() -> (i64, XLogRecPtr) {
    let ctl = XLogCtl();
    LWLockAcquire(WALWriteLock(), LW_SHARED, init_small::globals::MyProcNumber())
        .expect("GetLastSegSwitchData");
    let result = (ctl.lastSegSwitchTime.load(Relaxed), ctl.lastSegSwitchLSN.load(Relaxed));
    LWLockRelease(WALWriteLock()).expect("GetLastSegSwitchData");
    result
}

pub fn GetXLogWriteRecPtr() -> XLogRecPtr {
    RefreshXLogWriteResult();
    LOGWRT_RESULT.get().0
}

pub(crate) fn get_flush_rec_ptr_seam() -> (XLogRecPtr, TimeLineID) {
    let ctl = XLogCtl();
    debug_assert_eq!(ctl.SharedRecoveryState.load(Relaxed), RECOVERY_STATE_DONE);
    RefreshXLogWriteResult();
    (LOGWRT_RESULT.get().1, ctl.InsertTimeLineID.load(Relaxed))
}

pub fn GetFlushRecPtr(insert_tli: Option<&mut TimeLineID>) -> XLogRecPtr {
    let (flush, tli) = get_flush_rec_ptr_seam();
    if let Some(t) = insert_tli {
        *t = tli;
    }
    flush
}

pub(crate) fn open_log_file_close_if_open() -> PgResult<()> {
    if OPEN_LOG_FILE.get() >= 0 {
        XLogFileClose()?;
    }
    Ok(())
}

