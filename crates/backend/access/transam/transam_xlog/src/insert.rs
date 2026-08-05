use std::cell::Cell;
use std::sync::atomic::Ordering::Relaxed;

use crc32c::{fin_crc32c, pg_comp_crc32c};
use elog::{elog, ereport};
use lwlock::{
    LWLockAcquire, LWLockRelease, LWLockReleaseClearVar, LWLockUpdateVar, LWLockWaitForVar,
    LW_EXCLUSIVE,
};
use types_core::{TimeLineID, XLogRecPtr};
use types_error::{ErrorLocation, PgResult, ERRCODE_DATA_CORRUPTED, LOG, PANIC};

use crate::ctl::{XLogCtl, XLogRecPtrToBufIdx};
use crate::write::{RefreshXLogWriteResult, LOGWRT_RESULT};
use crate::*;

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

thread_local! {
    static LOCAL_RECOVERY_IN_PROGRESS: Cell<bool> = const { Cell::new(true) };
    static LOCAL_XLOG_INSERT_ALLOWED: Cell<i8> = const { Cell::new(-1) };
    static REDO_REC_PTR: Cell<XLogRecPtr> = const { Cell::new(0) };
    static DO_PAGE_WRITES: Cell<bool> = const { Cell::new(false) };
    static MY_LOCK_NO: Cell<usize> = const { Cell::new(0) };
    static HOLDING_ALL_LOCKS: Cell<bool> = const { Cell::new(false) };
    static LOCK_TO_TRY: Cell<isize> = const { Cell::new(-1) };
    static CACHED_PAGE: Cell<u64> = const { Cell::new(0) };
    static CACHED_POS: Cell<*mut u8> = const { Cell::new(std::ptr::null_mut()) };
}

pub fn RecoveryInProgress() -> bool {
    if !LOCAL_RECOVERY_IN_PROGRESS.get() {
        false
    } else {
        let v = XLogCtl().SharedRecoveryState.load(Relaxed) != RECOVERY_STATE_DONE;
        LOCAL_RECOVERY_IN_PROGRESS.set(v);
        v
    }
}

pub fn XLogInsertAllowed() -> bool {
    let v = LOCAL_XLOG_INSERT_ALLOWED.get();
    if v >= 0 {
        return v != 0;
    }
    if RecoveryInProgress() {
        return false;
    }
    LOCAL_XLOG_INSERT_ALLOWED.set(1);
    true
}

pub(crate) fn LocalSetXLogInsertAllowed() -> i8 {
    let old = LOCAL_XLOG_INSERT_ALLOWED.get();
    LOCAL_XLOG_INSERT_ALLOWED.set(1);
    old
}

pub(crate) fn set_local_xlog_insert_allowed(v: i8) {
    LOCAL_XLOG_INSERT_ALLOWED.set(v);
}

pub fn GetRedoRecPtr() -> XLogRecPtr {
    let ctl = XLogCtl();
    let ptr = ctl.info_lck.with(|| ctl.RedoRecPtr.load(Relaxed));
    if REDO_REC_PTR.get() < ptr {
        REDO_REC_PTR.set(ptr);
    }
    REDO_REC_PTR.get()
}

pub(crate) fn local_redo_rec_ptr() -> XLogRecPtr {
    REDO_REC_PTR.get()
}

pub(crate) fn set_local_redo_rec_ptr(ptr: XLogRecPtr) {
    REDO_REC_PTR.set(ptr);
}

pub(crate) fn set_do_page_writes(v: bool) {
    DO_PAGE_WRITES.set(v);
}

pub fn GetFullPageWriteInfo() -> (XLogRecPtr, bool) {
    (REDO_REC_PTR.get(), DO_PAGE_WRITES.get())
}

fn my_proc_number() -> types_core::ProcNumber {
    init_small::globals::MyProcNumber()
}

pub(crate) fn WALInsertLockAcquire() {
    let mut try_no = LOCK_TO_TRY.get();
    if try_no == -1 {
        try_no = (my_proc_number().max(0) as usize % NUM_XLOGINSERT_LOCKS) as isize;
        LOCK_TO_TRY.set(try_no);
    }
    MY_LOCK_NO.set(try_no as usize);
    let immed = LWLockAcquire(
        &XLogCtl().Insert.WALInsertLocks[try_no as usize].lock,
        LW_EXCLUSIVE,
        my_proc_number(),
    )
    .expect("WALInsertLockAcquire");
    if !immed {
        LOCK_TO_TRY.set((try_no + 1) % NUM_XLOGINSERT_LOCKS as isize);
    }
}

pub(crate) fn WALInsertLockAcquireExclusive() {
    let locks = &XLogCtl().Insert.WALInsertLocks;
    for i in 0..NUM_XLOGINSERT_LOCKS - 1 {
        LWLockAcquire(&locks[i].lock, LW_EXCLUSIVE, my_proc_number())
            .expect("WALInsertLockAcquireExclusive");
        LWLockUpdateVar(&locks[i].lock, &locks[i].insertingAt, u64::MAX);
    }
    LWLockAcquire(
        &locks[NUM_XLOGINSERT_LOCKS - 1].lock,
        LW_EXCLUSIVE,
        my_proc_number(),
    )
    .expect("WALInsertLockAcquireExclusive");
    HOLDING_ALL_LOCKS.set(true);
}

pub(crate) fn WALInsertLockRelease() {
    let locks = &XLogCtl().Insert.WALInsertLocks;
    if HOLDING_ALL_LOCKS.get() {
        for l in locks.iter() {
            LWLockReleaseClearVar(&l.lock, &l.insertingAt, 0).expect("WALInsertLockRelease");
        }
        HOLDING_ALL_LOCKS.set(false);
    } else {
        let l = &locks[MY_LOCK_NO.get()];
        LWLockReleaseClearVar(&l.lock, &l.insertingAt, 0).expect("WALInsertLockRelease");
    }
}

fn WALInsertLockUpdateInsertingAt(inserting_at: XLogRecPtr) {
    let locks = &XLogCtl().Insert.WALInsertLocks;
    let idx = if HOLDING_ALL_LOCKS.get() { NUM_XLOGINSERT_LOCKS - 1 } else { MY_LOCK_NO.get() };
    LWLockUpdateVar(&locks[idx].lock, &locks[idx].insertingAt, inserting_at);
}

// XLogRecord header byte offsets (xlogrecord.h, 24 bytes).
const XL_TOT_LEN: usize = 0;
const XL_PREV: usize = 8;
const XL_INFO: usize = 16;
const XL_RMID: usize = 17;
const XL_CRC: usize = 20;

// M4 NOTE: this spinlocked CurrBytePos/PrevBytePos reservation is C's shape
// (insertpos_lck). The M4 group-commit lever replaces it with a lock-free
// 128-bit CAS reservation (curr|prev packed) — keep the byte-position
// representation, it is what makes the reservation O(1).
#[inline(always)]
fn ReserveXLogInsertLocation(size: usize) -> (XLogRecPtr, XLogRecPtr, XLogRecPtr) {
    let insert = &XLogCtl().Insert;
    let size = MAXALIGN(size) as u64;
    debug_assert!(size > SizeOfXLogRecord as u64);

    let (startbytepos, endbytepos, prevbytepos);
    insert.insertpos_lck.acquire();
    startbytepos = insert.CurrBytePos.load(Relaxed);
    endbytepos = startbytepos + size;
    prevbytepos = insert.PrevBytePos.load(Relaxed);
    insert.CurrBytePos.store(endbytepos, Relaxed);
    insert.PrevBytePos.store(startbytepos, Relaxed);
    insert.insertpos_lck.release();

    let start_pos = XLogBytePosToRecPtr(startbytepos);
    let end_pos = XLogBytePosToEndRecPtr(endbytepos);
    let prev_ptr = XLogBytePosToRecPtr(prevbytepos);
    debug_assert_eq!(XLogRecPtrToBytePos(start_pos), startbytepos);
    debug_assert_eq!(XLogRecPtrToBytePos(end_pos), endbytepos);
    debug_assert_eq!(XLogRecPtrToBytePos(prev_ptr), prevbytepos);
    (start_pos, end_pos, prev_ptr)
}

fn ReserveXLogSwitch() -> (bool, XLogRecPtr, XLogRecPtr, XLogRecPtr) {
    let insert = &XLogCtl().Insert;
    let size = MAXALIGN(SizeOfXLogRecord) as u64;
    let wal_segsz = wal_segment_size();

    insert.insertpos_lck.acquire();
    let startbytepos = insert.CurrBytePos.load(Relaxed);
    let ptr = XLogBytePosToEndRecPtr(startbytepos);
    if XLogSegmentOffset(ptr, wal_segsz) == 0 {
        insert.insertpos_lck.release();
        return (false, ptr, ptr, 0);
    }
    let mut endbytepos = startbytepos + size;
    let prevbytepos = insert.PrevBytePos.load(Relaxed);
    let start_pos = XLogBytePosToRecPtr(startbytepos);
    let mut end_pos = XLogBytePosToEndRecPtr(endbytepos);
    let segleft = wal_segsz as u64 - XLogSegmentOffset(end_pos, wal_segsz) as u64;
    if segleft != wal_segsz as u64 {
        end_pos += segleft;
        endbytepos = XLogRecPtrToBytePos(end_pos);
    }
    insert.CurrBytePos.store(endbytepos, Relaxed);
    insert.PrevBytePos.store(startbytepos, Relaxed);
    insert.insertpos_lck.release();

    let prev_ptr = XLogBytePosToRecPtr(prevbytepos);
    debug_assert_eq!(XLogSegmentOffset(end_pos, wal_segsz), 0);
    (true, start_pos, end_pos, prev_ptr)
}

pub(crate) fn WaitXLogInsertionsToFinish(upto: XLogRecPtr) -> XLogRecPtr {
    let ctl = XLogCtl();
    let insert = &ctl.Insert;

    let inserted = ctl.logInsertResult.load(std::sync::atomic::Ordering::SeqCst);
    if upto <= inserted {
        return inserted;
    }

    let bytepos = insert.insertpos_lck.with(|| insert.CurrBytePos.load(Relaxed));
    let reserved_upto = XLogBytePosToEndRecPtr(bytepos);

    let mut upto = upto;
    if upto > reserved_upto {
        let _ = elog(
            LOG,
            format!(
                "request to flush past end of generated WAL; request {:X}/{:X}, current position {:X}/{:X}",
                upto >> 32, upto & 0xFFFF_FFFF, reserved_upto >> 32, reserved_upto & 0xFFFF_FFFF
            ),
        );
        upto = reserved_upto;
    }

    let mut finished_upto = reserved_upto;
    for l in insert.WALInsertLocks.iter() {
        let mut insertingat: XLogRecPtr = InvalidXLogRecPtr;
        loop {
            let free = LWLockWaitForVar(
                &l.lock,
                &l.insertingAt,
                insertingat,
                &mut insertingat,
                my_proc_number(),
            )
            .expect("WaitXLogInsertionsToFinish");
            if free {
                insertingat = InvalidXLogRecPtr;
                break;
            }
            if insertingat >= upto {
                break;
            }
        }
        if insertingat != InvalidXLogRecPtr && insertingat < finished_upto {
            finished_upto = insertingat;
        }
    }

    // pg_atomic_monotonic_advance_u64(logInsertResult, finishedUpto).
    let mut cur = ctl.logInsertResult.load(Relaxed);
    loop {
        if cur >= finished_upto {
            finished_upto = cur;
            break;
        }
        match ctl.logInsertResult.compare_exchange(
            cur,
            finished_upto,
            std::sync::atomic::Ordering::SeqCst,
            Relaxed,
        ) {
            Ok(_) => break,
            Err(v) => cur = v,
        }
    }
    finished_upto
}

pub(crate) fn GetXLogBuffer(ptr: XLogRecPtr, tli: TimeLineID) -> *mut u8 {
    let ctl = XLogCtl();

    if ptr / XLOG_BLCKSZ as u64 == CACHED_PAGE.get() && !CACHED_POS.get().is_null() {
        // SAFETY: cache validated below when set; offset < XLOG_BLCKSZ.
        return unsafe { CACHED_POS.get().add((ptr % XLOG_BLCKSZ as u64) as usize) };
    }

    let idx = XLogRecPtrToBufIdx(ptr) as usize;
    let expected_endptr = ptr + (XLOG_BLCKSZ as u64 - ptr % XLOG_BLCKSZ as u64);
    let endptr = ctl.xlblocks[idx].load(std::sync::atomic::Ordering::Acquire);
    if expected_endptr != endptr {
        let initialized_upto = if ptr % XLOG_BLCKSZ as u64 == SizeOfXLogShortPHD as u64
            && XLogSegmentOffset(ptr, wal_segment_size()) as usize > XLOG_BLCKSZ
        {
            ptr - SizeOfXLogShortPHD as u64
        } else if ptr % XLOG_BLCKSZ as u64 == SizeOfXLogLongPHD as u64
            && (XLogSegmentOffset(ptr, wal_segment_size()) as usize) < XLOG_BLCKSZ
        {
            ptr - SizeOfXLogLongPHD as u64
        } else {
            ptr
        };
        WALInsertLockUpdateInsertingAt(initialized_upto);
        AdvanceXLInsertBuffer(ptr, tli, false);
        let endptr = ctl.xlblocks[idx].load(std::sync::atomic::Ordering::Acquire);
        if expected_endptr != endptr {
            panic!(
                "could not find WAL buffer for {:X}/{:X}",
                ptr >> 32,
                ptr & 0xFFFF_FFFF
            );
        }
    }

    let page = ctl.page_ptr(idx);
    CACHED_PAGE.set(ptr / XLOG_BLCKSZ as u64);
    CACHED_POS.set(page);
    // SAFETY: page is a valid XLOG_BLCKSZ buffer; offset < XLOG_BLCKSZ.
    unsafe { page.add((ptr % XLOG_BLCKSZ as u64) as usize) }
}

pub(crate) fn AdvanceXLInsertBuffer(upto: XLogRecPtr, tli: TimeLineID, opportunistic: bool) {
    let ctl = XLogCtl();
    let insert = &ctl.Insert;

    LWLockAcquire(ctl::WALBufMappingLock(), LW_EXCLUSIVE, my_proc_number())
        .expect("AdvanceXLInsertBuffer");

    while upto >= ctl.InitializedUpTo.load(Relaxed) || opportunistic {
        let nextidx = XLogRecPtrToBufIdx(ctl.InitializedUpTo.load(Relaxed)) as usize;

        let old_page_rqst_ptr = ctl.xlblocks[nextidx].load(std::sync::atomic::Ordering::Acquire);
        if LOGWRT_RESULT.get().0 < old_page_rqst_ptr {
            if opportunistic {
                break;
            }
            ctl.info_lck.with(|| {
                if ctl.LogwrtRqstWrite.load(Relaxed) < old_page_rqst_ptr {
                    ctl.LogwrtRqstWrite.store(old_page_rqst_ptr, Relaxed);
                }
            });
            RefreshXLogWriteResult();
            if LOGWRT_RESULT.get().0 < old_page_rqst_ptr {
                LWLockRelease(ctl::WALBufMappingLock()).expect("AdvanceXLInsertBuffer");
                WaitXLogInsertionsToFinish(old_page_rqst_ptr);
                LWLockAcquire(ctl::WALWriteLock(), LW_EXCLUSIVE, my_proc_number())
                    .expect("AdvanceXLInsertBuffer");
                RefreshXLogWriteResult();
                if LOGWRT_RESULT.get().0 >= old_page_rqst_ptr {
                    LWLockRelease(ctl::WALWriteLock()).expect("AdvanceXLInsertBuffer");
                } else {
                    crate::write::XLogWrite((old_page_rqst_ptr, 0), tli, false)
                        .expect("XLogWrite from AdvanceXLInsertBuffer");
                    LWLockRelease(ctl::WALWriteLock()).expect("AdvanceXLInsertBuffer");
                    crate::wal_usage_update(|wu| wu.wal_buffers_full += 1);
                    init_small::globals::SetPgStatReportFixed(true);
                }
                LWLockAcquire(ctl::WALBufMappingLock(), LW_EXCLUSIVE, my_proc_number())
                    .expect("AdvanceXLInsertBuffer");
                continue;
            }
        }

        let new_page_begin_ptr = ctl.InitializedUpTo.load(Relaxed);
        let new_page_end_ptr = new_page_begin_ptr + XLOG_BLCKSZ as u64;
        debug_assert_eq!(XLogRecPtrToBufIdx(new_page_begin_ptr) as usize, nextidx);
        let new_page = ctl.page_ptr(nextidx);

        ctl.xlblocks[nextidx].store(InvalidXLogRecPtr, std::sync::atomic::Ordering::Release);

        // SAFETY: this buffer slot is unmapped (xlblocks invalidated above,
        // old contents written out); we are the only initializer under
        // WALBufMappingLock.
        unsafe {
            std::ptr::write_bytes(new_page, 0, XLOG_BLCKSZ);
            write_u16(new_page, 0, XLOG_PAGE_MAGIC);
            write_u32(new_page, 4, tli);
            write_u64(new_page, 8, new_page_begin_ptr);
            let mut info: u16 = 0;
            if insert.runningBackups.load(Relaxed) == 0 {
                info |= XLP_BKP_REMOVABLE;
            }
            if XLogSegmentOffset(new_page_begin_ptr, wal_segment_size()) == 0 {
                write_u64(new_page, 24, control_file::control_file().system_identifier);
                write_u32(new_page, 32, wal_segment_size() as u32);
                write_u32(new_page, 36, XLOG_BLCKSZ as u32);
                info |= XLP_LONG_HEADER;
            }
            write_u16(new_page, 2, info);
        }

        ctl.xlblocks[nextidx].store(new_page_end_ptr, std::sync::atomic::Ordering::Release);
        ctl.InitializedUpTo.store(new_page_end_ptr, Relaxed);
    }
    LWLockRelease(ctl::WALBufMappingLock()).expect("AdvanceXLInsertBuffer");
}

// XLogPageHeaderData field writes (xlog_internal.h): xlp_magic u16@0,
// xlp_info u16@2, xlp_tli u32@4, xlp_pageaddr u64@8, xlp_rem_len u32@16;
// long header adds xlp_sysid u64@24, xlp_seg_size u32@32, xlp_xlog_blcksz u32@36.
pub(crate) unsafe fn write_u16(p: *mut u8, off: usize, v: u16) {
    std::ptr::copy_nonoverlapping(v.to_ne_bytes().as_ptr(), p.add(off), 2);
}
pub(crate) unsafe fn write_u32(p: *mut u8, off: usize, v: u32) {
    std::ptr::copy_nonoverlapping(v.to_ne_bytes().as_ptr(), p.add(off), 4);
}
pub(crate) unsafe fn write_u64(p: *mut u8, off: usize, v: u64) {
    std::ptr::copy_nonoverlapping(v.to_ne_bytes().as_ptr(), p.add(off), 8);
}
pub(crate) unsafe fn read_u16(p: *const u8, off: usize) -> u16 {
    let mut b = [0u8; 2];
    std::ptr::copy_nonoverlapping(p.add(off), b.as_mut_ptr(), 2);
    u16::from_ne_bytes(b)
}

fn CopyXLogRecordToWAL(
    write_len: usize,
    is_log_switch: bool,
    rechdr: &[u8; 24],
    rdatas: &[&[u8]],
    start_pos: XLogRecPtr,
    end_pos: XLogRecPtr,
    tli: TimeLineID,
) -> PgResult<()> {
    let mut curr_pos = start_pos;
    let mut currpos = GetXLogBuffer(curr_pos, tli);
    let mut freespace = INSERT_FREESPACE(curr_pos);
    debug_assert!(freespace >= 4);

    let mut written = 0usize;
    let header: &[u8] = rechdr;
    for rdata in std::iter::once(&header).chain(rdatas.iter()) {
        let mut data = rdata.as_ptr();
        let mut len = rdata.len();
        while len > freespace {
            debug_assert!(curr_pos % XLOG_BLCKSZ as u64 >= SizeOfXLogShortPHD as u64 || freespace == 0);
            // SAFETY: freespace bytes remain on this buffer page at currpos.
            unsafe { std::ptr::copy_nonoverlapping(data, currpos, freespace) };
            data = unsafe { data.add(freespace) };
            len -= freespace;
            written += freespace;
            curr_pos += freespace as u64;

            currpos = GetXLogBuffer(curr_pos, tli);
            // SAFETY: currpos is the page head; set xlp_rem_len + contrecord
            // flag (we own the page per the insert protocol).
            unsafe {
                write_u32(currpos, 16, (write_len - written) as u32);
                let info = read_u16(currpos, 2) | XLP_FIRST_IS_CONTRECORD;
                write_u16(currpos, 2, info);
            }
            let hdr = if XLogSegmentOffset(curr_pos, wal_segment_size()) == 0 {
                SizeOfXLogLongPHD
            } else {
                SizeOfXLogShortPHD
            };
            curr_pos += hdr as u64;
            currpos = unsafe { currpos.add(hdr) };
            freespace = INSERT_FREESPACE(curr_pos);
        }
        debug_assert!(curr_pos % XLOG_BLCKSZ as u64 >= SizeOfXLogShortPHD as u64 || len == 0);
        // SAFETY: len <= freespace on this page.
        unsafe { std::ptr::copy_nonoverlapping(data, currpos, len) };
        currpos = unsafe { currpos.add(len) };
        curr_pos += len as u64;
        freespace -= len;
        written += len;
    }
    debug_assert_eq!(written, write_len);

    if is_log_switch && XLogSegmentOffset(curr_pos, wal_segment_size()) != 0 {
        debug_assert_eq!(write_len, SizeOfXLogRecord);
        debug_assert_eq!(XLogSegmentOffset(end_pos, wal_segment_size()), 0);
        curr_pos += freespace as u64;
        while curr_pos < end_pos {
            currpos = GetXLogBuffer(curr_pos, tli);
            // SAFETY: page owned by us; zero the short header for
            // compressibility (C's MemSet of SizeOfXLogShortPHD).
            unsafe { std::ptr::write_bytes(currpos, 0, SizeOfXLogShortPHD) };
            curr_pos += XLOG_BLCKSZ as u64;
        }
    } else {
        curr_pos = MAXALIGN64(curr_pos);
    }

    if curr_pos != end_pos {
        return ereport(PANIC)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg("space reserved for WAL record does not match what was written")
            .finish(loc("CopyXLogRecordToWAL"));
    }
    Ok(())
}

pub fn XLogInsertRecord(
    rechdr: &mut [u8; 24],
    rdatas: &[&[u8]],
    fpw_lsn: XLogRecPtr,
    flags: u8,
    num_fpi: i32,
    topxid_included: bool,
) -> PgResult<XLogRecPtr> {
    let ctl = XLogCtl();
    let insert = &ctl.Insert;

    let xl_tot_len = u32::from_ne_bytes(rechdr[XL_TOT_LEN..XL_TOT_LEN + 4].try_into().unwrap()) as usize;
    let info = rechdr[XL_INFO] & !XLR_INFO_MASK;
    let rmid = rechdr[XL_RMID];

    #[derive(PartialEq, Clone, Copy)]
    enum Class {
        Normal,
        SpecialSwitch,
        SpecialCheckpoint,
    }
    let class = if rmid == RM_XLOG_ID {
        if info == XLOG_SWITCH {
            Class::SpecialSwitch
        } else if info == XLOG_CHECKPOINT_REDO {
            Class::SpecialCheckpoint
        } else {
            Class::Normal
        }
    } else {
        Class::Normal
    };

    if !XLogInsertAllowed() {
        return Err(elog_helpers::error_result("cannot make new WAL entries during recovery"));
    }

    let insert_tli = ctl.InsertTimeLineID.load(Relaxed);
    let prev_do_page_writes = DO_PAGE_WRITES.get();

    init_small::globals::StartCriticalSection();

    let (inserted, start_pos, end_pos);
    match class {
        Class::Normal => {
            WALInsertLockAcquire();
            let shared_redo = insert.RedoRecPtr.load(Relaxed);
            if REDO_REC_PTR.get() != shared_redo {
                debug_assert!(REDO_REC_PTR.get() < shared_redo);
                REDO_REC_PTR.set(shared_redo);
            }
            let do_page_writes =
                insert.fullPageWrites.load(Relaxed) || insert.runningBackups.load(Relaxed) > 0;
            DO_PAGE_WRITES.set(do_page_writes);

            if do_page_writes
                && (!prev_do_page_writes
                    || (fpw_lsn != InvalidXLogRecPtr && fpw_lsn <= REDO_REC_PTR.get()))
            {
                WALInsertLockRelease();
                init_small::globals::EndCriticalSection();
                return Ok(InvalidXLogRecPtr);
            }

            let (s, e, prev) = ReserveXLogInsertLocation(xl_tot_len);
            rechdr[XL_PREV..XL_PREV + 8].copy_from_slice(&prev.to_ne_bytes());
            (inserted, start_pos, end_pos) = (true, s, e);
        }
        Class::SpecialSwitch => {
            debug_assert_eq!(fpw_lsn, InvalidXLogRecPtr);
            WALInsertLockAcquireExclusive();
            let (ok, s, e, prev) = ReserveXLogSwitch();
            if ok {
                rechdr[XL_PREV..XL_PREV + 8].copy_from_slice(&prev.to_ne_bytes());
            }
            (inserted, start_pos, end_pos) = (ok, s, e);
        }
        Class::SpecialCheckpoint => {
            debug_assert_eq!(fpw_lsn, InvalidXLogRecPtr);
            WALInsertLockAcquireExclusive();
            let (s, e, prev) = ReserveXLogInsertLocation(xl_tot_len);
            rechdr[XL_PREV..XL_PREV + 8].copy_from_slice(&prev.to_ne_bytes());
            REDO_REC_PTR.set(s);
            insert.RedoRecPtr.store(s, Relaxed);
            (inserted, start_pos, end_pos) = (true, s, e);
        }
    }

    let mut end_pos = end_pos;
    if inserted {
        let body_crc = u32::from_ne_bytes(rechdr[XL_CRC..XL_CRC + 4].try_into().unwrap());
        let crc = fin_crc32c(pg_comp_crc32c(body_crc, &rechdr[..XL_CRC]));
        rechdr[XL_CRC..XL_CRC + 4].copy_from_slice(&crc.to_ne_bytes());

        CopyXLogRecordToWAL(
            xl_tot_len,
            class == Class::SpecialSwitch,
            rechdr,
            rdatas,
            start_pos,
            end_pos,
            insert_tli,
        )?;

        if flags & XLOG_MARK_UNIMPORTANT == 0 {
            let lockno = if HOLDING_ALL_LOCKS.get() { 0 } else { MY_LOCK_NO.get() };
            insert.WALInsertLocks[lockno].lastImportantAt.store(start_pos, Relaxed);
        }
    }

    WALInsertLockRelease();
    init_small::globals::EndCriticalSection();

    xact_seams::mark_current_transaction_id_logged_if_any::call();
    if topxid_included {
        xact_seams::mark_subxact_top_xid_logged::call();
    }

    if start_pos / XLOG_BLCKSZ as u64 != end_pos / XLOG_BLCKSZ as u64 {
        ctl.info_lck.with(|| {
            if ctl.LogwrtRqstWrite.load(Relaxed) < end_pos {
                ctl.LogwrtRqstWrite.store(end_pos, Relaxed);
            }
        });
        RefreshXLogWriteResult();
    }

    if class == Class::SpecialSwitch {
        crate::write::XLogFlush(end_pos)?;
        if inserted {
            end_pos = start_pos + SizeOfXLogRecord as u64;
            if start_pos / XLOG_BLCKSZ as u64 != end_pos / XLOG_BLCKSZ as u64 {
                let offset = XLogSegmentOffset(end_pos, wal_segment_size()) as u64;
                if offset == end_pos % XLOG_BLCKSZ as u64 {
                    end_pos += SizeOfXLogLongPHD as u64;
                } else {
                    end_pos += SizeOfXLogShortPHD as u64;
                }
            }
        }
    }

    PROC_LAST_REC_PTR.set(start_pos);
    XACT_LAST_REC_END.set(end_pos);
    if inserted {
        crate::wal_usage_update(|wu| {
            wu.wal_bytes = wu.wal_bytes.wrapping_add(xl_tot_len as u64);
            wu.wal_records += 1;
            wu.wal_fpi += num_fpi as i64;
        });
        init_small::globals::SetPgStatReportFixed(true);
    }

    Ok(end_pos)
}

pub(crate) fn xlog_insert_record_seam(
    rechdr: &mut [u8; 24],
    rdatas: &[&[u8]],
    fpw_lsn: XLogRecPtr,
    flags: u8,
    num_fpi: i32,
    topxid_included: bool,
) -> PgResult<XLogRecPtr> {
    XLogInsertRecord(rechdr, rdatas, fpw_lsn, flags, num_fpi, topxid_included)
}

pub fn GetLastImportantRecPtr() -> XLogRecPtr {
    let locks = &XLogCtl().Insert.WALInsertLocks;
    let mut res = InvalidXLogRecPtr;
    for l in locks.iter() {
        LWLockAcquire(&l.lock, LW_EXCLUSIVE, my_proc_number()).expect("GetLastImportantRecPtr");
        let last = l.lastImportantAt.load(Relaxed);
        LWLockRelease(&l.lock).expect("GetLastImportantRecPtr");
        res = res.max(last);
    }
    res
}

pub fn GetInsertRecPtr() -> XLogRecPtr {
    let ctl = XLogCtl();
    ctl.info_lck.with(|| ctl.LogwrtRqstWrite.load(Relaxed))
}

pub fn GetXLogInsertRecPtr() -> XLogRecPtr {
    let insert = &XLogCtl().Insert;
    let current_bytepos = insert.insertpos_lck.with(|| insert.CurrBytePos.load(Relaxed));
    XLogBytePosToRecPtr(current_bytepos)
}

const _: () = {
    assert!(XL_TOT_LEN == 0 && XL_PREV == 8 && XL_INFO == 16 && XL_RMID == 17 && XL_CRC == 20);
};

use crate::ctl;

mod elog_helpers {
    use types_error::{PgError, ERROR};

    pub fn error_result(msg: &str) -> Box<PgError> {
        Box::new(PgError::new(ERROR, msg.to_string()))
    }
}
