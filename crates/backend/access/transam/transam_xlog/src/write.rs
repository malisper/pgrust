use std::cell::{Cell, RefCell};
use std::os::fd::AsRawFd;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use vfs::VfsFd;

use elog::ereport;
use lwlock::{LWLockAcquire, LWLockAcquireOrWait, LWLockConditionalAcquire, LWLockRelease, LW_EXCLUSIVE, LW_SHARED};
use types_core::{TimeLineID, XLogRecPtr, XLogSegNo};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_DATA_CORRUPTED, ERRCODE_INVALID_PARAMETER_VALUE, ERROR,
    LOG, PANIC,
};

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
    // C's per-backend `openLogFile`. The C process model never closes this
    // fd before backend exit — process death reclaims it (xlog.c has no
    // at-exit close). Backends here are THREADS, so the holder itself is the
    // close guard: VfsFd's Drop runs on every thread-death path (clean
    // proc_exit unwind, FATAL, quickdie's exit_thread_raw, TLS teardown) and
    // releases through the vfs that minted the fd (finding F1b). Only this
    // thread's own descriptor is involved — walwriter/checkpointer keep
    // theirs until THEIR thread ends, matching C's per-process scope.
    // (Without the guard, every DML backend leaked its WAL segment fd:
    // Thermite server-soak finding, ~1 host fd per connection.)
    static OPEN_LOG_FILE: RefCell<Option<VfsFd>> = const { RefCell::new(None) };
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

/// Raw-fd view of C's `openLogFile` (-1 = no open WAL segment). Ownership
/// stays with the OPEN_LOG_FILE guard; callers must not close this fd
/// directly — deliberate closes go through [`XLogFileClose`].
fn open_log_file() -> i32 {
    OPEN_LOG_FILE.with(|c| c.borrow().as_ref().map_or(-1, |g| g.as_raw_fd()))
}

/// C's `openLogFile = fd` after XLogFileInit/XLogFileOpen.
fn set_open_log_file(raw: i32) {
    debug_assert!(raw >= 0);
    // SAFETY: `raw` was just minted by BasicOpenFile (vfs::open) on this
    // thread and is exclusively owned by the guard from here on; the guard
    // is TLS-bound, honoring VfsFd's thread-affinity contract.
    let guard = unsafe { VfsFd::from_raw(raw) };
    let prev = OPEN_LOG_FILE.with(|c| c.borrow_mut().replace(guard));
    // The write paths always XLogFileClose() before opening the next
    // segment; if that invariant ever broke, dropping `prev` still closes
    // the orphaned descriptor rather than leaking it.
    debug_assert!(prev.is_none(), "WAL segment fd replaced while still open");
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

// wait_event.h, PG_WAIT_IO class: the ids are the positions in the
// waitevent crate's IO name table (pgstat_get_wait_event(PG_WAIT_IO | 80)
// == "WalWrite").
const PG_WAIT_IO: u32 = 0x0A00_0000;
const WAIT_EVENT_WAL_COPY_READ: u32 = PG_WAIT_IO | 70;
const WAIT_EVENT_WAL_COPY_SYNC: u32 = PG_WAIT_IO | 71;
const WAIT_EVENT_WAL_COPY_WRITE: u32 = PG_WAIT_IO | 72;
const WAIT_EVENT_WAL_INIT_SYNC: u32 = PG_WAIT_IO | 73;
const WAIT_EVENT_WAL_INIT_WRITE: u32 = PG_WAIT_IO | 74;
const WAIT_EVENT_WAL_SYNC: u32 = PG_WAIT_IO | 78;
const WAIT_EVENT_WAL_SYNC_METHOD_ASSIGN: u32 = PG_WAIT_IO | 79;
const WAIT_EVENT_WAL_WRITE: u32 = PG_WAIT_IO | 80;

// pgstat_report_wait_start/end (wait_event.h): the WAL file I/O of xlog.c is
// bracketed so pg_stat_activity.wait_event shows WALWrite/WALSync/... while
// the syscall runs. The seam is uninstalled in unit tests (no backend
// status storage), where the report is a no-op.
#[inline]
fn report_wait_start(wait_event_info: u32) {
    if waitevent_seams::pgstat_report_wait_start::is_installed() {
        waitevent_seams::pgstat_report_wait_start::call(wait_event_info);
    }
}

#[inline]
fn report_wait_end() {
    if waitevent_seams::pgstat_report_wait_end::is_installed() {
        waitevent_seams::pgstat_report_wait_end::call();
    }
}

// assign_wal_sync_method (xlog.c:8712-8738): a changed sync method
// invalidates the open-flag/fsync posture of the open segment; fsync it
// under WAIT_EVENT_WAL_SYNC_METHOD_ASSIGN and close it if the open flags
// change. An fsync failure is ereport(PANIC, errcode_for_file_access(),
// "could not fsync file \"%s\": %m") — the structured PANIC (log line +
// crash choreography), not a raw thread panic.
pub(crate) fn assign_wal_sync_method(new_val: i32, _extra: Option<&guc_tables::GucHookExtra>) {
    if wal_sync_method() != new_val && open_log_file() >= 0 {
        report_wait_start(WAIT_EVENT_WAL_SYNC_METHOD_ASSIGN);
        if fd::pg_fsync(open_log_file()) != 0 {
            let en = fd::get_errno();
            let xlogfname =
                XLogFileName(OPEN_LOG_TLI.get(), OPEN_LOG_SEG_NO.get(), wal_segment_size());
            let _ = ereport(PANIC)
                .with_saved_errno(en)
                .errcode_for_file_access()
                .errmsg(format!("could not fsync file \"{xlogfname}\": %m"))
                .finish(loc("assign_wal_sync_method"));
            unreachable!("ereport(PANIC) returned");
        }
        report_wait_end();
        if get_sync_bit(wal_sync_method()) != get_sync_bit(new_val) {
            let _ = XLogFileClose();
        }
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

// get_sync_bit (xlog.c:8654-8700): the extra open(2) flags for a WAL
// segment from wal_sync_method, fsync and debug_io_direct.
pub(crate) fn get_sync_bit(method: i32) -> i32 {
    // Use O_DIRECT if requested, except in the walreceiver process: the
    // startup process reads its WAL right after it is written, and the
    // walreceiver performs unaligned writes, which don't work with O_DIRECT.
    let o_direct_flag = if fd::io_direct_flags() & types_storage::IO_DIRECT_WAL != 0
        && miscinit::GetMyBackendType() != types_core::BackendType::WalReceiver
    {
        vfs::PG_O_DIRECT
    } else {
        0
    };

    // If fsync is disabled, never open in sync mode.
    if !init_small::globals::enableFsync() {
        return o_direct_flag;
    }
    match method {
        WAL_SYNC_METHOD_FSYNC | WAL_SYNC_METHOD_FSYNC_WRITETHROUGH | WAL_SYNC_METHOD_FDATASYNC => {
            o_direct_flag
        }
        WAL_SYNC_METHOD_OPEN => libc::O_SYNC | o_direct_flag,
        WAL_SYNC_METHOD_OPEN_DSYNC => libc::O_DSYNC | o_direct_flag,
        _ => panic!("unrecognized \"wal_sync_method\": {method}"),
    }
}

// issue_xlog_fsync (xlog.c:8771-8785): the PANIC text for a failed sync,
// keyed by the wal_sync_method primitive that failed ("%s" = segment name,
// "%m" = strerror, both expanded by the caller's ereport).
pub(crate) fn fsync_failure_message(method: i32) -> &'static str {
    match method {
        WAL_SYNC_METHOD_FSYNC_WRITETHROUGH => "could not fsync write-through file \"%s\": %m",
        WAL_SYNC_METHOD_FDATASYNC => "could not fdatasync file \"%s\": %m",
        // WAL_SYNC_METHOD_FSYNC; any other method PANICs before a sync runs.
        _ => "could not fsync file \"%s\": %m",
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
    report_wait_start(WAIT_EVENT_WAL_SYNC);
    let rc = match method {
        WAL_SYNC_METHOD_FSYNC => fd::pg_fsync_no_writethrough(fd),
        WAL_SYNC_METHOD_FSYNC_WRITETHROUGH => fd::pg_fsync_writethrough(fd),
        WAL_SYNC_METHOD_FDATASYNC => fd::pg_fdatasync(fd),
        _ => {
            // xlog.c:8799-8803: the structured PANIC (ERRCODE_INVALID_PARAMETER_VALUE),
            // not a raw thread panic.
            return ereport(PANIC)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg_internal(format!("unrecognized \"wal_sync_method\": {method}"))
                .finish(loc("issue_xlog_fsync"));
        }
    };
    if rc != 0 {
        // xlog.c:8788-8797: errcode_for_file_access() + "%m".
        let en = fd::get_errno();
        let fname = XLogFileName(tli, segno, wal_segment_size());
        return ereport(PANIC)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(fsync_failure_message(method).replace("%s", &fname))
            .finish(loc("issue_xlog_fsync"));
    }
    report_wait_end();
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
    // xlog.c:3258-3291: save_errno; "if write didn't set errno, assume no
    // disk space".
    let mut save_errno: i32 = 0;
    let io_start = wal_io_start();
    let init_zero = guc_tables::vars::wal_init_zero.read();
    report_wait_start(WAIT_EVENT_WAL_INIT_WRITE);
    if init_zero {
        let rc = fd::io::pg_pwrite_zeros(f, wal_segsz as usize, 0);
        if rc < 0 {
            let en = fd::get_errno();
            save_errno = if en != 0 { en } else { libc::ENOSPC };
        }
    } else {
        // One byte at segment end.
        let rc = fd::pg_pwrite(f, b"\0", (wal_segsz - 1) as i64);
        if rc != 1 {
            let en = fd::get_errno();
            save_errno = if en != 0 { en } else { libc::ENOSPC };
        }
    }
    report_wait_end();
    if save_errno != 0 {
        // If we fail to make the file, delete it to release disk space.
        let _ = fd::pg_unlink(&tmppath);
        // fd owned here.
        fd::pg_close(f);
        return ereport(ERROR)
            .with_saved_errno(save_errno)
            .errcode_for_file_access()
            .errmsg(format!("could not write to file \"{tmppath}\": %m"))
            .finish(loc("XLogFileInitInternal"))
            .map(|_| -1);
    }

    // upstream 13f940b4b56f (18.6): Fix pgstat_count_io_op_time() calls passing incorrect information
    // A full segment worth of data is written when using wal_init_zero. One
    // byte is written when not using it.
    count_wal_io(
        pgstat::io::IOContext::IOCONTEXT_INIT,
        pgstat::io::IOOp::Write,
        io_start,
        if init_zero { wal_segsz as u64 } else { 1 },
    );

    let io_start = wal_io_start();
    report_wait_start(WAIT_EVENT_WAL_INIT_SYNC);
    if fd::pg_fsync(f) != 0 {
        // xlog.c:3306-3315: save_errno; close(fd); errno = save_errno;
        // ereport(ERROR, errcode_for_file_access(), "...: %m").
        let en = fd::get_errno();
        // fd owned here.
        fd::pg_close(f);
        return ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not fsync file \"{tmppath}\": %m"))
            .finish(loc("XLogFileInitInternal"))
            .map(|_| -1);
    }
    report_wait_end();
    count_wal_io(pgstat::io::IOContext::IOCONTEXT_INIT, pgstat::io::IOOp::Fsync, io_start, 0);
    // fd owned here.
    if fd::pg_close(f) != 0 {
        let en = fd::get_errno();
        return ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{tmppath}\": %m"))
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
        let en = fd::get_errno();
        return ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{path}\": %m"))
            .finish(loc("XLogFileCopy"));
    }

    let tmppath = format!("{XLOGDIR}/xlogtemp.{}", std::process::id());
    let _ = std::fs::remove_file(&tmppath);

    // No get_sync_bit(): fsync only once at end of fill.
    let f = fd::OpenTransientFile(&tmppath, libc::O_RDWR | libc::O_CREAT | libc::O_EXCL)?;
    if f < 0 {
        let en = fd::get_errno();
        fd::CloseTransientFile(srcfd);
        return ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not create file \"{tmppath}\": %m"))
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
            report_wait_start(WAIT_EVENT_WAL_COPY_READ);
            // SAFETY: srcfd open; buffer holds >= nread bytes.
            let r = unsafe { libc::read(srcfd, buffer.as_mut_ptr().cast(), nread as usize) };
            if r != nread as isize {
                // xlog.c:3474-3484: a failed read is errcode_for_file_access()
                // + "%m"; a short read is ERRCODE_DATA_CORRUPTED.
                if r < 0 {
                    let en = fd::get_errno();
                    return ereport(ERROR)
                        .with_saved_errno(en)
                        .errcode_for_file_access()
                        .errmsg(format!("could not read file \"{path}\": %m"))
                        .finish(loc("XLogFileCopy"));
                }
                return ereport(ERROR)
                    .errcode(ERRCODE_DATA_CORRUPTED)
                    .errmsg(format!("could not read file \"{path}\": read {r} of {nread}"))
                    .finish(loc("XLogFileCopy"));
            }
            report_wait_end();
        }
        fd::set_errno(0);
        report_wait_start(WAIT_EVENT_WAL_COPY_WRITE);
        // SAFETY: f open; buffer is XLOG_BLCKSZ bytes.
        let w = unsafe { libc::write(f, buffer.as_ptr().cast(), XLOG_BLCKSZ) };
        if w != XLOG_BLCKSZ as isize {
            // xlog.c:3490-3502: unlink the partial file; "if write didn't
            // set errno, assume problem is no disk space".
            let en = fd::get_errno();
            let _ = std::fs::remove_file(&tmppath);
            return ereport(ERROR)
                .with_saved_errno(if en != 0 { en } else { libc::ENOSPC })
                .errcode_for_file_access()
                .errmsg(format!("could not write to file \"{tmppath}\": %m"))
                .finish(loc("XLogFileCopy"));
        }
        report_wait_end();
        nbytes += XLOG_BLCKSZ as i32;
    }

    report_wait_start(WAIT_EVENT_WAL_COPY_SYNC);
    if fd::pg_fsync(f) != 0 {
        // C xlog.c:3510: data_sync_elevel(ERROR) — PANIC at default
        // data_sync_retry=off (post-fsyncgate: never retry a failed fsync).
        let en = fd::get_errno();
        return ereport(fd::data_sync_elevel(ERROR))
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not fsync file \"{tmppath}\": %m"))
            .finish(loc("XLogFileCopy"));
    }
    report_wait_end();
    if fd::CloseTransientFile(f) != 0 {
        let en = fd::get_errno();
        return ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{tmppath}\": %m"))
            .finish(loc("XLogFileCopy"));
    }
    if fd::CloseTransientFile(srcfd) != 0 {
        let en = fd::get_errno();
        return ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{path}\": %m"))
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
    // Deliberate close: disarm the guard (into_raw) and release through
    // pg_close, exactly the VfsFd contract.
    let guard = OPEN_LOG_FILE.with(|c| c.borrow_mut().take());
    debug_assert!(guard.is_some());
    let f = guard.map_or(-1, VfsFd::into_raw);
    if fd::pg_close(f) != 0 {
        let fname = XLogFileName(OPEN_LOG_TLI.get(), OPEN_LOG_SEG_NO.get(), wal_segment_size());
        return ereport(PANIC)
            .errmsg(format!("could not close file \"{fname}\""))
            .finish(loc("XLogFileClose"));
    }
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
            if open_log_file() >= 0 {
                XLogFileClose()?;
            }
            OPEN_LOG_SEG_NO.set(XLByteToPrevSeg(lw_write, wal_segsz));
            OPEN_LOG_TLI.set(tli);
            set_open_log_file(XLogFileInit(OPEN_LOG_SEG_NO.get(), tli)?);
            fd::ReserveExternalFD()?;
        }
        if open_log_file() < 0 {
            OPEN_LOG_SEG_NO.set(XLByteToPrevSeg(lw_write, wal_segsz));
            OPEN_LOG_TLI.set(tli);
            set_open_log_file(XLogFileOpen(OPEN_LOG_SEG_NO.get(), tli)?);
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
                report_wait_start(WAIT_EVENT_WAL_WRITE);
                let written = fd::pg_pwrite(
                    open_log_file(),
                    unsafe { std::slice::from_raw_parts(from.cast::<u8>(), nleft) },
                    startoffset as i64,
                );
                report_wait_end();
                if written <= 0 {
                    // xlog.c:2440-2452: EINTR retries; otherwise
                    // ereport(PANIC, errcode_for_file_access(), "...: %m")
                    // (ENOSPC -> SQLSTATE 53100, strerror text).
                    let en = fd::get_errno();
                    if en == libc::EINTR {
                        continue;
                    }
                    let fname = XLogFileName(tli, OPEN_LOG_SEG_NO.get(), wal_segsz);
                    return ereport(PANIC)
                        .with_saved_errno(en)
                        .errcode_for_file_access()
                        .errmsg(format!(
                            "could not write to log file \"{fname}\" at offset {startoffset}, length {nleft}: %m"
                        ))
                        .finish(loc("XLogWrite"));
                }

                // upstream 13f940b4b56f (18.6): Fix pgstat_count_io_op_time() calls passing incorrect information
                count_wal_io(
                    pgstat::io::IOContext::IOCONTEXT_NORMAL,
                    pgstat::io::IOOp::Write,
                    io_start,
                    written as u64,
                );
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
                issue_xlog_fsync(open_log_file(), OPEN_LOG_SEG_NO.get(), tli)?;
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
            if open_log_file() >= 0
                && !XLByteInPrevSeg(lw_write, OPEN_LOG_SEG_NO.get(), wal_segsz)
            {
                XLogFileClose()?;
            }
            if open_log_file() < 0 {
                OPEN_LOG_SEG_NO.set(XLByteToPrevSeg(lw_write, wal_segsz));
                OPEN_LOG_TLI.set(tli);
                set_open_log_file(XLogFileOpen(OPEN_LOG_SEG_NO.get(), tli)?);
                fd::ReserveExternalFD()?;
            }
            issue_xlog_fsync(open_log_file(), OPEN_LOG_SEG_NO.get(), tli)?;
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

    // C's XLogWrite only REQUESTS the walsender wakeup (xlog.c:2482/2554);
    // XLogFlush and XLogBackgroundFlush process it after LWLockRelease
    // (WALWriteLock) + END_CRIT_SECTION (2913/3088). AdvanceXLInsertBuffer's
    // write leaves the request for the caller's next processing site.
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

/// The commit_delay gate (xlog.c:2881-2884), shared verbatim by
/// [`XLogFlush`] and [`XLogFlushPipelined`]. Returns whether it slept (the
/// caller then re-waits insertions, as C does).
///
/// The sleep rides `waiter::sleep` (GL-FLUSHSIM-1 negotiated ask §5.3.2):
/// DST-schedulable (virtual clock), and native-equivalent — a pending
/// unpark can end it early, exactly as C's pg_usleep returns early on
/// EINTR (the delay is best-effort batching either way).
fn commit_delay_gate_sleep() -> bool {
    let commit_delay = guc_tables::vars::CommitDelay.read();
    if commit_delay > 0
        && init_small::globals::enableFsync()
        && procarray_seams::minimum_active_backends::is_installed()
        && procarray_seams::minimum_active_backends::call(
            guc_tables::vars::CommitSiblings.read(),
        )
    {
        waiter::sleep(std::time::Duration::from_micros(commit_delay as u64));
        return true;
    }
    false
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

            if commit_delay_gate_sleep() {
                insertpos = WaitXLogInsertionsToFinish(insertpos);
            }

            XLogWrite((insertpos, insertpos), insert_tli, false)?;
            LWLockRelease(WALWriteLock())?;
            // GL-FLUSHPIPE-1: this leader's flush may cover pipelined commit
            // waiters; complete them (after the lock release — wakes must not
            // lengthen the flush critical path). Knob-OFF cost: one memoized
            // bool read.
            crate::flushpipe::complete_up_to(LOGWRT_RESULT.get().1);
            break;
        }
        Ok(())
    })();

    init_small::globals::EndCriticalSection();
    loop_result?;

    // xlog.c:2912-2913: wake up walsenders now that we've released heavily
    // contended locks.
    WalSndWakeupProcessRequests(true, !crate::insert::RecoveryInProgress());

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

/// GL-FLUSHPIPE-1 — [`XLogFlush`] for the sync-commit call site (xact.c:1502's
/// `XLogFlush(XactLastRecEnd)`, and ONLY that site; every other flush caller
/// keeps the incumbent path). Installed as the `xlog_flush_commit` seam.
///
/// Disarmed (knob off / no walwriter / no latch) this IS [`XLogFlush`] (one
/// memoized bool read of overhead). Armed, the ONE behavioral change is the
/// contended arm: where [`XLogFlush`] joins the WALWriteLock convoy
/// (`LWLockAcquireOrWait` — the leader-follower herd), this registers the
/// commit LSN on the pending-flush queue and parks for a DIRECTED completion
/// ([`crate::flushpipe`]; walwriter is the guaranteed-progress flusher). The
/// uncontended arm (conditional acquire won) is the incumbent leader half,
/// line-for-line — including the commit_delay gate, which composes here
/// exactly as in [`XLogFlush`]. Commit ordering, criticality, and the
/// non-cancellable wait posture are all unchanged (letter §2: "v1 splits
/// nothing").
pub fn XLogFlushPipelined(record: XLogRecPtr) -> PgResult<()> {
    if !crate::flushpipe::pipeline_available() {
        return XLogFlush(record);
    }

    let ctl = XLogCtl();

    // Parity prelude (XLogFlush).
    if !XLogInsertAllowed() {
        return UpdateMinRecoveryPoint(record, false);
    }
    if record <= LOGWRT_RESULT.get().1 {
        return Ok(());
    }

    let insert_tli = ctl.InsertTimeLineID.load(Relaxed);
    init_small::globals::StartCriticalSection();

    // GL-ERRFIX-1 (E-2, the "escape being uncounted" half): balance the critical
    // section on the `?`-escape paths exactly as [`XLogFlush`] above — the loop
    // runs in a closure so EndCriticalSection is unconditional before any error
    // from a lock op or XLogWrite propagates out (C's END_CRIT_SECTION is
    // unconditional and an in-crit ereport promotes to PANIC).
    let loop_result: PgResult<()> = (|| {
        let mut write_rqst_ptr = record;
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
            // As in XLogFlush: wait for in-flight insertions BEFORE taking the
            // write lock, never while holding it.
            let mut insertpos = WaitXLogInsertionsToFinish(write_rqst_ptr);

            if !LWLockConditionalAcquire(WALWriteLock(), LW_EXCLUSIVE)? {
                // A flush is in flight — the pipelining case. Register + park;
                // the in-flight leader's walk (or walwriter) completes us.
                // Completed => loop re-reads results and breaks covered.
                // Retry (liveness re-arm, flusher-death windows) => same loop:
                // we contend for the lock ourselves next lap, C's
                // uncovered-follower-becomes-leader shape.
                let _ = crate::flushpipe::wait_for_flush(record);
                continue;
            }

            // LEADER arm — XLogFlush's got-the-lock half, verbatim.
            RefreshXLogWriteResult();
            if record <= LOGWRT_RESULT.get().1 {
                LWLockRelease(WALWriteLock())?;
                break;
            }

            if commit_delay_gate_sleep() {
                insertpos = WaitXLogInsertionsToFinish(insertpos);
            }

            XLogWrite((insertpos, insertpos), insert_tli, false)?;
            LWLockRelease(WALWriteLock())?;
            crate::flushpipe::count_leader_flush();
            crate::flushpipe::complete_up_to(LOGWRT_RESULT.get().1);
            break;
        }
        Ok(())
    })();

    init_small::globals::EndCriticalSection();
    loop_result?;

    // xlog.c:2912-2913: wake up walsenders now that we've released heavily
    // contended locks.
    WalSndWakeupProcessRequests(true, !crate::insert::RecoveryInProgress());

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

    // GL-FLUSHPIPE-1 (armed only; knob-OFF cost one memoized bool read):
    // pipelined commit waiters are SYNC durability requests — raise the
    // write request to the pending max (which can exceed both LogwrtRqst
    // and asyncXactLSN: a mid-page commit record bumps neither) and treat
    // the cycle as flush-forced below, bypassing the async batching
    // deliberately built into the pacing.
    let pending_force = crate::flushpipe::pending_max()
        .filter(|p| *p > LOGWRT_RESULT.get().1);
    if let Some(p) = pending_force {
        if write_rqst_write < p {
            write_rqst_write = p;
        }
        flexible = false;
    }

    if write_rqst_write <= LOGWRT_RESULT.get().1 {
        // GL-FLUSHPIPE-1 safety drain: nothing to flush, but a registrant
        // covered by an earlier flush may still be parked (e.g. its
        // completer raced its link — the fence pair makes one side win,
        // and this cycle is the guaranteed winner's backstop).
        crate::flushpipe::complete_up_to(LOGWRT_RESULT.get().1);
        if open_log_file() >= 0
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

    // Pending commit waiters force the flush leg: flush_after=0 takes the
    // pacing's always-flush arm (and stamps its clock), C ordering intact.
    let flush_after = if pending_force.is_some() { 0 } else { flush_after };

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

    // GL-FLUSHPIPE-1: complete pipelined commit waiters this cycle's flush
    // covered (after the lock release; no-op when the knob is off).
    crate::flushpipe::complete_up_to(LOGWRT_RESULT.get().1);

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
    if open_log_file() >= 0 {
        XLogFileClose()?;
    }
    Ok(())
}

