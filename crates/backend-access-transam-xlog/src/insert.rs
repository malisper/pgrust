//! The WAL-insertion path of `access/transam/xlog.c` (PostgreSQL 18.3): the
//! two-step insert protocol that copies an assembled WAL record into the shared
//! `XLogCtl` WAL-buffer ring.
//!
//! Step 1 reserves space (`ReserveXLogInsertLocation` / `ReserveXLogSwitch`,
//! bumping `Insert.CurrBytePos` under `insertpos_lck`). Step 2 copies the record
//! into the right WAL pages (`CopyXLogRecordToWAL` walking the page ring via
//! `GetXLogBuffer` / `AdvanceXLInsertBuffer`). Concurrency is mediated by the
//! genuine `WALInsertLock` array (the LWLock + `insertingAt` atomic +
//! `lastImportantAt`) allocated in [`crate::shmem`], synchronized through
//! `backend-storage-lmgr-lwlock` (`LWLockAcquire` / `LWLockWaitForVar` /
//! `LWLockUpdateVar` / `LWLockReleaseClearVar`).
//!
//! The page-eviction leg of `AdvanceXLInsertBuffer` (the call into `XLogWrite`
//! when a buffer that still needs flushing must be reused) is the WAL-WRITE
//! driver — still deferred (`xlog-driver` debt, task #156/F2) — so it panics
//! loudly; likewise the `XLogFlush` of an `XLOG_SWITCH` record. Every other leg
//! of the insertion path is grounded 1:1.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate std;

use core::cell::Cell;

use backend_utils_error::{PgError, PgResult};
use types_core::{TimeLineID, XLogRecPtr};
use types_storage::storage::{LWLock, LW_EXCLUSIVE};
use types_wal::xlog_consts::{
    SIZE_OF_XLOG_LONG_PHD as SizeOfXLogLongPHD, SIZE_OF_XLOG_SHORT_PHD as SizeOfXLogShortPHD,
    XLOG_BLCKSZ,
};
use types_wal::wal::{RM_XLOG_ID, XLR_INFO_MASK, XLOG_MARK_UNIMPORTANT};

use backend_storage_lmgr_lwlock as lwlock;
use backend_utils_init_small::globals;

use crate::shmem::{
    self, redo_rec_ptr_cached, refresh_xlog_write_result, set_redo_rec_ptr_cached, wal_insert_locks,
    wal_segment_size, xlog_ctl, XLogCtlData, NUM_XLOGINSERT_LOCKS,
};
use crate::{XLogBytePosToEndRecPtr, XLogBytePosToRecPtr, XLogRecPtrToBytePos, XLogSegmentOffset};

// ===========================================================================
// Compile-time mirrors of the xlog.c record/page constants used here.
// ===========================================================================

/// `XLOG_PAGE_MAGIC` (access/xlog_internal.h).
const XLOG_PAGE_MAGIC: u16 = 0xD118;

/// Page-header flag bits (access/xlog_internal.h).
const XLP_FIRST_IS_CONTRECORD: u16 = 0x0001;
const XLP_LONG_HEADER: u16 = 0x0002;
const XLP_BKP_REMOVABLE: u16 = 0x0004;

/// `XLOG_SWITCH` / `XLOG_CHECKPOINT_REDO` (catalog/pg_control.h) — XLOG-rmgr
/// info bytes that need special insertion handling.
const XLOG_SWITCH: u8 = 0x40;
const XLOG_CHECKPOINT_REDO: u8 = 0xE0;

/// `SizeOfXLogRecord` (access/xlogrecord.h) — `offsetof(XLogRecord, xl_crc) +
/// sizeof(pg_crc32c)` = 24 on LP64.
pub(crate) const SizeOfXLogRecord: usize = 24;

/// `offsetof(XLogRecord, xl_prev)` and `offsetof(XLogRecord, xl_crc)` in the
/// LP64 `XLogRecord` layout: `xl_tot_len@0(u32) xl_xid@4(u32) xl_prev@8(u64)
/// xl_info@16(u8) xl_rmid@17(u8) pad@18..20 xl_crc@20(u32)`.
const OFFSETOF_XL_TOT_LEN: usize = 0;
const OFFSETOF_XL_PREV: usize = 8;
const OFFSETOF_XL_INFO: usize = 16;
const OFFSETOF_XL_RMID: usize = 17;
const OFFSETOF_XL_CRC: usize = 20;

/// `MAXIMUM_ALIGNOF` (pg_config.h on LP64).
const MAXIMUM_ALIGNOF: usize = 8;

/// `MAXALIGN(LEN)` (c.h).
#[inline]
fn MAXALIGN(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `MAXALIGN64(LEN)` (c.h) — same arithmetic over a 64-bit position.
#[inline]
fn MAXALIGN64(len: u64) -> u64 {
    (len + (MAXIMUM_ALIGNOF as u64 - 1)) & !(MAXIMUM_ALIGNOF as u64 - 1)
}

/// `INSERT_FREESPACE(endptr)` (xlog.c:605) — bytes left on the page after
/// `endptr`.
#[inline]
fn INSERT_FREESPACE(endptr: XLogRecPtr) -> usize {
    let blcksz = XLOG_BLCKSZ as u64;
    if endptr % blcksz == 0 {
        0
    } else {
        (blcksz - endptr % blcksz) as usize
    }
}

/// `XLogRecPtrToBufIdx(recptr)` (xlog.c:616) — the WAL buffer index a page
/// maps to. `XLogCacheBlck + 1` is the buffer count.
///
/// # Safety
/// `ctl` must reference the live `XLogCtl` shmem region.
#[inline]
pub(crate) unsafe fn XLogRecPtrToBufIdx(ctl: &XLogCtlData, recptr: XLogRecPtr) -> usize {
    let nbuffers = (ctl.XLogCacheBlck + 1) as u64;
    ((recptr / XLOG_BLCKSZ as u64) % nbuffers) as usize
}

// ===========================================================================
// xlog.c file-scope backend-local globals used by the insert path.
// ===========================================================================

std::thread_local! {
    /// `static bool doPageWrites` (xlog.c) — whether records should include
    /// full-page images (recomputed under the insertion lock).
    static DO_PAGE_WRITES: Cell<bool> = const { Cell::new(false) };

    /// `static bool holdingAllLocks` (xlog.c) — set while a backend holds all
    /// WAL insertion locks (xlog-switch / checkpoint-redo special records).
    static HOLDING_ALL_LOCKS: Cell<bool> = const { Cell::new(false) };

    /// `static int MyLockNo` (xlog.c) — the single insertion lock this backend
    /// currently holds (normal records).
    static MY_LOCK_NO: Cell<usize> = const { Cell::new(0) };

    /// `static int lockToTry = -1` (function-local static in
    /// `WALInsertLockAcquire`).
    static LOCK_TO_TRY: Cell<i32> = const { Cell::new(-1) };

    /// `XLogRecPtr ProcLastRecPtr` (xlog.c) — start of the most recently
    /// inserted record.
    static PROC_LAST_REC_PTR: Cell<XLogRecPtr> = const { Cell::new(0) };

    /// `XLogRecPtr XactLastRecEnd` (xlog.c) — end of the most recently
    /// inserted record of the current transaction.
    static XACT_LAST_REC_END: Cell<XLogRecPtr> = const { Cell::new(0) };

    /// `XLogRecPtr XactLastCommitEnd` (xlog.c) — end of the last commit record.
    static XACT_LAST_COMMIT_END: Cell<XLogRecPtr> = const { Cell::new(0) };

    /// `GetXLogBuffer`'s function-local cache: `static uint64 cachedPage` /
    /// `static char *cachedPos`. We cache the page number and the in-ring byte
    /// offset of that page's start (a raw `*mut u8`).
    static CACHED_PAGE: Cell<u64> = const { Cell::new(0) };
    static CACHED_POS: Cell<*mut u8> = const { Cell::new(core::ptr::null_mut()) };
}

// --- seam-installable accessors for the xlog.c backend-local globals ---

/// `ProcLastRecPtr` reader (seam `proc_last_rec_ptr`).
pub fn proc_last_rec_ptr() -> XLogRecPtr {
    PROC_LAST_REC_PTR.with(Cell::get)
}

/// `XactLastRecEnd` reader (seam `xact_last_rec_end`).
pub fn xact_last_rec_end() -> XLogRecPtr {
    XACT_LAST_REC_END.with(Cell::get)
}

/// `XactLastRecEnd` writer (seam `set_xact_last_rec_end`).
pub fn set_xact_last_rec_end(lsn: XLogRecPtr) {
    XACT_LAST_REC_END.with(|c| c.set(lsn));
}

/// `XactLastCommitEnd` writer (seam `set_xact_last_commit_end`).
pub fn set_xact_last_commit_end(lsn: XLogRecPtr) {
    XACT_LAST_COMMIT_END.with(|c| c.set(lsn));
}

// ===========================================================================
// Little-endian-of-native byte-poke helpers over the WAL page bytes / record
// header. The WAL on-disk image is native-endian; we go through raw pointers
// because the pages live in the shmem ring.
// ===========================================================================

#[inline]
unsafe fn poke_u16(p: *mut u8, off: usize, v: u16) {
    core::ptr::copy_nonoverlapping(v.to_ne_bytes().as_ptr(), p.add(off), 2);
}
#[inline]
unsafe fn poke_u32(p: *mut u8, off: usize, v: u32) {
    core::ptr::copy_nonoverlapping(v.to_ne_bytes().as_ptr(), p.add(off), 4);
}
#[inline]
unsafe fn poke_u64(p: *mut u8, off: usize, v: u64) {
    core::ptr::copy_nonoverlapping(v.to_ne_bytes().as_ptr(), p.add(off), 8);
}
#[inline]
unsafe fn read_u16(p: *const u8, off: usize) -> u16 {
    let mut b = [0u8; 2];
    core::ptr::copy_nonoverlapping(p.add(off), b.as_mut_ptr(), 2);
    u16::from_ne_bytes(b)
}

#[inline]
fn rdata_u32(rdata: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(rdata[off..off + 4].try_into().unwrap())
}

// ===========================================================================
// WALInsertLockAcquire / Release / AcquireExclusive / UpdateInsertingAt
// (xlog.c:1398 / 1472 / 1443 / 1498).
// ===========================================================================

/// `&WALInsertLocks[i].l.lock`.
///
/// # Safety
/// The lock array must have been allocated by `XLOGShmemInit`.
unsafe fn insert_lock(i: usize) -> &'static LWLock {
    let locks = wal_insert_locks();
    &(*locks.add(i)).l.lock
}

/// `&WALInsertLocks[i].l.insertingAt` (the per-lock atomic).
///
/// # Safety
/// The lock array must have been allocated by `XLOGShmemInit`.
unsafe fn inserting_at(i: usize) -> &'static types_storage::storage::pg_atomic_uint64 {
    let locks = wal_insert_locks();
    &(*locks.add(i)).l.insertingAt
}

/// `WALInsertLockAcquire(void)` (xlog.c:1398).
fn WALInsertLockAcquire() -> PgResult<()> {
    if LOCK_TO_TRY.with(Cell::get) == -1 {
        let n = globals::MyProcNumber().rem_euclid(NUM_XLOGINSERT_LOCKS as i32);
        LOCK_TO_TRY.with(|c| c.set(n));
    }
    let lock_to_try = LOCK_TO_TRY.with(Cell::get);
    MY_LOCK_NO.with(|c| c.set(lock_to_try as usize));

    // SAFETY: the lock array is live once XLOGShmemInit has run.
    let immed = unsafe {
        lwlock::LWLockAcquire(insert_lock(lock_to_try as usize), LW_EXCLUSIVE, globals::MyProcNumber())?
    };
    if !immed {
        let next = (lock_to_try + 1).rem_euclid(NUM_XLOGINSERT_LOCKS as i32);
        LOCK_TO_TRY.with(|c| c.set(next));
    }
    Ok(())
}

/// `WALInsertLockAcquireExclusive(void)` (xlog.c:1443).
fn WALInsertLockAcquireExclusive() -> PgResult<()> {
    // SAFETY: live lock array.
    unsafe {
        for i in 0..NUM_XLOGINSERT_LOCKS - 1 {
            lwlock::LWLockAcquire(insert_lock(i), LW_EXCLUSIVE, globals::MyProcNumber())?;
            lwlock::LWLockUpdateVar(insert_lock(i), inserting_at(i), u64::MAX);
        }
        // The last lock's value is reset to 0 at release.
        lwlock::LWLockAcquire(
            insert_lock(NUM_XLOGINSERT_LOCKS - 1),
            LW_EXCLUSIVE,
            globals::MyProcNumber(),
        )?;
    }
    HOLDING_ALL_LOCKS.with(|c| c.set(true));
    Ok(())
}

/// `WALInsertLockRelease(void)` (xlog.c:1472).
fn WALInsertLockRelease() -> PgResult<()> {
    if HOLDING_ALL_LOCKS.with(Cell::get) {
        // SAFETY: live lock array.
        unsafe {
            for i in 0..NUM_XLOGINSERT_LOCKS {
                lwlock::LWLockReleaseClearVar(insert_lock(i), inserting_at(i), 0)?;
            }
        }
        HOLDING_ALL_LOCKS.with(|c| c.set(false));
    } else {
        let n = MY_LOCK_NO.with(Cell::get);
        // SAFETY: live lock array.
        unsafe {
            lwlock::LWLockReleaseClearVar(insert_lock(n), inserting_at(n), 0)?;
        }
    }
    Ok(())
}

/// `WALInsertLockUpdateInsertingAt(insertingAt)` (xlog.c:1498).
fn WALInsertLockUpdateInsertingAt(insertingat: XLogRecPtr) {
    // SAFETY: live lock array.
    unsafe {
        if HOLDING_ALL_LOCKS.with(Cell::get) {
            let last = NUM_XLOGINSERT_LOCKS - 1;
            lwlock::LWLockUpdateVar(insert_lock(last), inserting_at(last), insertingat);
        } else {
            let n = MY_LOCK_NO.with(Cell::get);
            lwlock::LWLockUpdateVar(insert_lock(n), inserting_at(n), insertingat);
        }
    }
}

// ===========================================================================
// ReserveXLogInsertLocation / ReserveXLogSwitch (xlog.c:1135 / 1191).
// ===========================================================================

/// `ReserveXLogInsertLocation(size, *StartPos, *EndPos, *PrevPtr)`
/// (xlog.c:1135). Returns `(StartPos, EndPos, PrevPtr)`.
///
/// # Safety
/// `ctl` must reference the live `XLogCtl` shmem region.
unsafe fn ReserveXLogInsertLocation(
    ctl: &XLogCtlData,
    size: usize,
) -> (XLogRecPtr, XLogRecPtr, XLogRecPtr) {
    let size = MAXALIGN(size);
    debug_assert!(size > SizeOfXLogRecord);

    let insert = &ctl.Insert;
    let insert_mut = insert as *const _ as *mut crate::shmem::XLogCtlInsert;

    shmem::spin_lock_acquire(&insert.insertpos_lck);
    let startbytepos = (*insert_mut).CurrBytePos;
    let endbytepos = startbytepos + size as u64;
    let prevbytepos = (*insert_mut).PrevBytePos;
    (*insert_mut).CurrBytePos = endbytepos;
    (*insert_mut).PrevBytePos = startbytepos;
    shmem::spin_lock_release(&insert.insertpos_lck);

    let seg = wal_segment_size();
    let start_pos = XLogBytePosToRecPtr(startbytepos, seg);
    let end_pos = XLogBytePosToEndRecPtr(endbytepos, seg);
    let prev_ptr = XLogBytePosToRecPtr(prevbytepos, seg);

    debug_assert_eq!(XLogRecPtrToBytePos(start_pos, seg), startbytepos);
    debug_assert_eq!(XLogRecPtrToBytePos(end_pos, seg), endbytepos);
    debug_assert_eq!(XLogRecPtrToBytePos(prev_ptr, seg), prevbytepos);

    (start_pos, end_pos, prev_ptr)
}

/// `ReserveXLogSwitch(*StartPos, *EndPos, *PrevPtr)` (xlog.c:1191). Returns
/// `(inserted, StartPos, EndPos, PrevPtr)`.
///
/// # Safety
/// `ctl` must reference the live `XLogCtl` shmem region, and the caller must
/// hold all the WAL insertion locks.
unsafe fn ReserveXLogSwitch(ctl: &XLogCtlData) -> (bool, XLogRecPtr, XLogRecPtr, XLogRecPtr) {
    let insert = &ctl.Insert;
    let insert_mut = insert as *const _ as *mut crate::shmem::XLogCtlInsert;
    let seg = wal_segment_size();
    let size = MAXALIGN(SizeOfXLogRecord) as u64;

    shmem::spin_lock_acquire(&insert.insertpos_lck);
    let startbytepos = (*insert_mut).CurrBytePos;

    let ptr = XLogBytePosToEndRecPtr(startbytepos, seg);
    if XLogSegmentOffset(ptr, seg) == 0 {
        shmem::spin_lock_release(&insert.insertpos_lck);
        return (false, ptr, ptr, 0);
    }

    let mut endbytepos = startbytepos + size;
    let prevbytepos = (*insert_mut).PrevBytePos;

    let start_pos = XLogBytePosToRecPtr(startbytepos, seg);
    let mut end_pos = XLogBytePosToEndRecPtr(endbytepos, seg);

    let segleft = seg as u32 - XLogSegmentOffset(end_pos, seg);
    if segleft != seg as u32 {
        // Consume the rest of the segment.
        end_pos += segleft as u64;
        endbytepos = XLogRecPtrToBytePos(end_pos, seg);
    }
    (*insert_mut).CurrBytePos = endbytepos;
    (*insert_mut).PrevBytePos = startbytepos;

    shmem::spin_lock_release(&insert.insertpos_lck);

    let prev_ptr = XLogBytePosToRecPtr(prevbytepos, seg);

    debug_assert_eq!(XLogSegmentOffset(end_pos, seg), 0);
    debug_assert_eq!(XLogRecPtrToBytePos(end_pos, seg), endbytepos);
    debug_assert_eq!(XLogRecPtrToBytePos(start_pos, seg), startbytepos);
    debug_assert_eq!(XLogRecPtrToBytePos(prev_ptr, seg), prevbytepos);

    (true, start_pos, end_pos, prev_ptr)
}

// ===========================================================================
// GetXLogBuffer / AdvanceXLInsertBuffer (xlog.c:1659 / 2012).
// ===========================================================================

/// `GetXLogBuffer(ptr, tli)` (xlog.c:1659) — pointer into the WAL-buffer ring
/// for the page containing `ptr`, initialising the page if needed.
///
/// # Safety
/// `ctl` must reference the live `XLogCtl` shmem region; the caller must hold a
/// WAL insertion lock (see the C function comment).
unsafe fn GetXLogBuffer(ctl: &XLogCtlData, ptr: XLogRecPtr, tli: TimeLineID) -> PgResult<*mut u8> {
    let blcksz = XLOG_BLCKSZ as u64;

    // Fast path: same page as last time.
    if ptr / blcksz == CACHED_PAGE.with(Cell::get) {
        let cached = CACHED_POS.with(Cell::get);
        return Ok(cached.add((ptr % blcksz) as usize));
    }

    let idx = XLogRecPtrToBufIdx(ctl, ptr);

    let mut expected_end_ptr = ptr;
    expected_end_ptr += blcksz - ptr % blcksz;

    let mut endptr = (*ctl.xlblocks.add(idx)).read();
    if expected_end_ptr != endptr {
        let seg = wal_segment_size();
        let initialized_upto = if ptr % blcksz == SizeOfXLogShortPHD as u64
            && XLogSegmentOffset(ptr, seg) as u64 > blcksz
        {
            ptr - SizeOfXLogShortPHD as u64
        } else if ptr % blcksz == SizeOfXLogLongPHD as u64
            && (XLogSegmentOffset(ptr, seg) as u64) < blcksz
        {
            ptr - SizeOfXLogLongPHD as u64
        } else {
            ptr
        };

        WALInsertLockUpdateInsertingAt(initialized_upto);

        AdvanceXLInsertBuffer(ctl, ptr, tli, false)?;
        endptr = (*ctl.xlblocks.add(idx)).read();

        if expected_end_ptr != endptr {
            return Err(PgError::error(std::format!(
                "could not find WAL buffer for {:X}/{:X}",
                (ptr >> 32) as u32,
                ptr as u32
            )));
        }
    } else {
        core::sync::atomic::fence(core::sync::atomic::Ordering::SeqCst);
    }

    let cached_page = ptr / blcksz;
    let cached_pos = ctl.pages.add(idx * XLOG_BLCKSZ);
    CACHED_PAGE.with(|c| c.set(cached_page));
    CACHED_POS.with(|c| c.set(cached_pos));

    Ok(cached_pos.add((ptr % blcksz) as usize))
}

/// `AdvanceXLInsertBuffer(upto, tli, opportunistic)` (xlog.c:2012) —
/// initialise WAL-buffer ring pages up to `upto`.
///
/// # Safety
/// `ctl` must reference the live `XLogCtl` shmem region.
unsafe fn AdvanceXLInsertBuffer(
    ctl: &XLogCtlData,
    upto: XLogRecPtr,
    tli: TimeLineID,
    opportunistic: bool,
) -> PgResult<()> {
    let blcksz = XLOG_BLCKSZ as u64;
    let seg = wal_segment_size();
    let insert = &ctl.Insert;
    // `XLogCtl` is shmem; the few fields we mutate (InitializedUpTo,
    // LogwrtRqst) are protected by the locks below as in C.
    let ctl_mut = ctl as *const XLogCtlData as *mut XLogCtlData;

    // LWLockAcquire(WALBufMappingLock, LW_EXCLUSIVE).
    let wal_buf_mapping = wal_buf_mapping_lock_offset();
    let mut guard =
        lwlock::LWLockAcquireMain(wal_buf_mapping, LW_EXCLUSIVE, globals::MyProcNumber())?;

    while upto >= (*ctl_mut).InitializedUpTo || opportunistic {
        let nextidx = XLogRecPtrToBufIdx(ctl, (*ctl_mut).InitializedUpTo);

        let old_page_rqst_ptr = (*ctl.xlblocks.add(nextidx)).read();
        if shmem::logwrt_result().Write < old_page_rqst_ptr {
            if opportunistic {
                break;
            }

            // Advance shared write request.
            shmem::spin_lock_acquire(&ctl.info_lck);
            if (*ctl_mut).LogwrtRqst.Write < old_page_rqst_ptr {
                (*ctl_mut).LogwrtRqst.Write = old_page_rqst_ptr;
            }
            shmem::spin_lock_release(&ctl.info_lck);

            refresh_xlog_write_result(ctl);
            if shmem::logwrt_result().Write < old_page_rqst_ptr {
                // Must write/evict the old page. Release WALBufMappingLock
                // first so all insertions up to this position can finish,
                // avoiding deadlock; then wait, take WALWriteLock, and write.
                guard.release()?;

                crate::write::WaitXLogInsertionsToFinish(ctl, old_page_rqst_ptr)?;

                let wal_write_lock = lwlock::main_lock_ref(wal_write_lock_offset());
                lwlock::LWLockAcquire(wal_write_lock, LW_EXCLUSIVE, globals::MyProcNumber())?;

                refresh_xlog_write_result(ctl);
                if shmem::logwrt_result().Write >= old_page_rqst_ptr {
                    // Someone else wrote it already.
                    lwlock::LWLockRelease(wal_write_lock)?;
                } else {
                    // Write it ourselves.
                    let write_rqst = crate::shmem::XLogwrtRqst {
                        Write: old_page_rqst_ptr,
                        Flush: 0,
                    };
                    crate::write::XLogWrite(ctl, write_rqst, tli, false)?;
                    lwlock::LWLockRelease(wal_write_lock)?;
                    // pgWalUsage.wal_buffers_full++ / pgstat_report_fixed:
                    // pgstat instrumentation (unported), behaviour-preserving.
                }

                // Re-acquire WALBufMappingLock and retry.
                guard = lwlock::LWLockAcquireMain(
                    wal_buf_mapping,
                    LW_EXCLUSIVE,
                    globals::MyProcNumber(),
                )?;
                continue;
            }
        }

        // Set up the next output page.
        let new_page_begin_ptr = (*ctl_mut).InitializedUpTo;
        let new_page_end_ptr = new_page_begin_ptr + blcksz;
        debug_assert_eq!(XLogRecPtrToBufIdx(ctl, new_page_begin_ptr), nextidx);

        let new_page = ctl.pages.add(nextidx * XLOG_BLCKSZ);

        // Mark the xlblock invalid + write barrier before initializing.
        (*ctl.xlblocks.add(nextidx)).write(crate::InvalidXLogRecPtr);
        core::sync::atomic::fence(core::sync::atomic::Ordering::SeqCst);

        // Re-zero the buffer.
        core::ptr::write_bytes(new_page, 0, XLOG_BLCKSZ);

        // Fill the new page's header.
        poke_u16(new_page, 0, XLOG_PAGE_MAGIC); // xlp_magic
        let mut xlp_info: u16 = 0;
        poke_u32(new_page, 4, tli); // xlp_tli
        poke_u64(new_page, 8, new_page_begin_ptr); // xlp_pageaddr
        // xlp_rem_len already 0 from memset.

        if insert.runningBackups == 0 {
            xlp_info |= XLP_BKP_REMOVABLE;
        }

        // First page of a segment: long header.
        if XLogSegmentOffset(new_page_begin_ptr, seg) == 0 {
            // XLogLongPageHeaderData: std @0..20, xlp_sysid @24(u64),
            // xlp_seg_size @32(u32), xlp_xlog_blcksz @36(u32).
            poke_u64(new_page, 24, shmem::GetSystemIdentifier());
            poke_u32(new_page, 32, seg as u32);
            poke_u32(new_page, 36, XLOG_BLCKSZ as u32);
            xlp_info |= XLP_LONG_HEADER;
        }
        poke_u16(new_page, 2, xlp_info); // xlp_info

        // Make initialization visible before the xlblocks update.
        core::sync::atomic::fence(core::sync::atomic::Ordering::SeqCst);

        (*ctl.xlblocks.add(nextidx)).write(new_page_end_ptr);
        (*ctl_mut).InitializedUpTo = new_page_end_ptr;
    }

    guard.release()?;
    Ok(())
}

/// `AdvanceXLInsertBuffer(InvalidXLogRecPtr, tli, true)` — the opportunistic
/// WAL-buffer pre-initialization the walwriter does after a background flush
/// (xlog.c:3117). Exposed for [`crate::write::XLogBackgroundFlush`].
///
/// # Safety
/// `ctl` must reference the live `XLogCtl` shmem region.
pub(crate) unsafe fn advance_xl_insert_buffer_opportunistic(
    ctl: &XLogCtlData,
    tli: TimeLineID,
) -> PgResult<()> {
    AdvanceXLInsertBuffer(ctl, crate::InvalidXLogRecPtr, tli, true)
}

/// `WALBufMappingLock` offset into the main LWLock array. The individual
/// builtin locks occupy the first `NUM_INDIVIDUAL_LWLOCKS` slots in
/// `lwlocklist.h` order; `WALBufMapping` is entry 7.
#[inline]
fn wal_buf_mapping_lock_offset() -> usize {
    7
}

/// `WALWriteLock` — entry 8 in the `MainLWLockArray` (`lwlocklist.h`).
#[inline]
fn wal_write_lock_offset() -> usize {
    8
}

// ===========================================================================
// CopyXLogRecordToWAL (xlog.c:1252).
// ===========================================================================

/// `CopyXLogRecordToWAL(write_len, isLogSwitch, rdata, StartPos, EndPos, tli)`
/// (xlog.c:1252). `rdata` is the assembled record chain as byte fragments
/// (fragment 0 is the already-finalised header).
///
/// # Safety
/// `ctl` must reference the live `XLogCtl` shmem region; the caller holds the
/// insertion lock.
unsafe fn CopyXLogRecordToWAL(
    ctl: &XLogCtlData,
    write_len: usize,
    is_log_switch: bool,
    rdata: &[&[u8]],
    start_pos: XLogRecPtr,
    end_pos: XLogRecPtr,
    tli: TimeLineID,
) -> PgResult<()> {
    let blcksz = XLOG_BLCKSZ as u64;
    let seg = wal_segment_size();

    let mut curr_pos = start_pos;
    let mut currpos = GetXLogBuffer(ctl, curr_pos, tli)?;
    let mut freespace = INSERT_FREESPACE(curr_pos);

    debug_assert!(freespace >= core::mem::size_of::<u32>());

    let mut written: usize = 0;
    for frag in rdata {
        let mut rdata_data: *const u8 = frag.as_ptr();
        let mut rdata_len: usize = frag.len();

        while rdata_len > freespace {
            // Write what fits, continue on the next page.
            debug_assert!(curr_pos % blcksz >= SizeOfXLogShortPHD as u64 || freespace == 0);
            core::ptr::copy_nonoverlapping(rdata_data, currpos, freespace);
            rdata_data = rdata_data.add(freespace);
            rdata_len -= freespace;
            written += freespace;
            curr_pos += freespace as u64;

            // Next page: set xlp_rem_len + XLP_FIRST_IS_CONTRECORD.
            currpos = GetXLogBuffer(ctl, curr_pos, tli)?;
            poke_u32(currpos, 16, (write_len - written) as u32); // xlp_rem_len
            let info = read_u16(currpos, 2) | XLP_FIRST_IS_CONTRECORD;
            poke_u16(currpos, 2, info);

            // Skip over the page header.
            if XLogSegmentOffset(curr_pos, seg) == 0 {
                curr_pos += SizeOfXLogLongPHD as u64;
                currpos = currpos.add(SizeOfXLogLongPHD);
            } else {
                curr_pos += SizeOfXLogShortPHD as u64;
                currpos = currpos.add(SizeOfXLogShortPHD);
            }
            freespace = INSERT_FREESPACE(curr_pos);
        }

        debug_assert!(curr_pos % blcksz >= SizeOfXLogShortPHD as u64 || rdata_len == 0);
        core::ptr::copy_nonoverlapping(rdata_data, currpos, rdata_len);
        currpos = currpos.add(rdata_len);
        curr_pos += rdata_len as u64;
        freespace -= rdata_len;
        written += rdata_len;
    }
    debug_assert_eq!(written, write_len);

    if is_log_switch && XLogSegmentOffset(curr_pos, seg) != 0 {
        debug_assert_eq!(write_len, SizeOfXLogRecord);
        debug_assert_eq!(XLogSegmentOffset(end_pos, seg), 0);

        // Use up all the remaining space on the current page.
        curr_pos += freespace as u64;

        // Flush each remaining page in the segment one at a time.
        while curr_pos < end_pos {
            let p = GetXLogBuffer(ctl, curr_pos, tli)?;
            core::ptr::write_bytes(p, 0, SizeOfXLogShortPHD);
            curr_pos += blcksz;
        }
    } else {
        curr_pos = MAXALIGN64(curr_pos);
    }

    if curr_pos != end_pos {
        return Err(PgError::error(
            "space reserved for WAL record does not match what was written",
        ));
    }
    Ok(())
}

// ===========================================================================
// XLogInsertRecord (xlog.c:772) — the insertion entry the xloginsert.c
// `XLogInsert` calls (the `xlog_insert_record` seam).
// ===========================================================================

/// WAL-insertion class (xlog.c `WalInsertClass`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum WalInsertClass {
    Normal,
    SpecialSwitch,
    SpecialCheckpoint,
}

/// `XLogInsertRecord(rdata, fpw_lsn, flags, num_fpi, topxid_included)`
/// (xlog.c:772). `rdata` carries the assembled record fragments in order;
/// `rdata[0]` is the fixed `XLogRecord` header (with the caller's partial CRC
/// in `xl_crc`). Returns the record end LSN, or `InvalidXLogRecPtr` (`0`) when
/// the caller must recompute and retry.
pub fn XLogInsertRecord(
    rdata: &[&[u8]],
    fpw_lsn: XLogRecPtr,
    flags: u8,
    num_fpi: i32,
    topxid_included: bool,
) -> PgResult<XLogRecPtr> {
    let _ = num_fpi;
    let ctl_ptr = xlog_ctl();
    if ctl_ptr.is_null() {
        return Err(PgError::error(
            "XLogInsertRecord: XLogCtl shmem not initialized",
        ));
    }
    // SAFETY: ctl_ptr is the live XLogCtl shmem region.
    let ctl = unsafe { &*ctl_ptr };

    // The first chunk holds the whole record header.
    let header = rdata[0];
    debug_assert!(header.len() >= SizeOfXLogRecord);

    let xl_tot_len = rdata_u32(header, OFFSETOF_XL_TOT_LEN) as usize;
    let xl_rmid = header[OFFSETOF_XL_RMID];
    let info = header[OFFSETOF_XL_INFO] & !XLR_INFO_MASK;

    // Does this record type require special handling?
    let mut class = WalInsertClass::Normal;
    if xl_rmid == RM_XLOG_ID {
        if info == XLOG_SWITCH {
            class = WalInsertClass::SpecialSwitch;
        } else if info == XLOG_CHECKPOINT_REDO {
            class = WalInsertClass::SpecialCheckpoint;
        }
    }

    // we assume that all of the record header is in the first chunk.
    debug_assert!(rdata[0].len() >= SizeOfXLogRecord);

    if !XLogInsertAllowed() {
        return Err(PgError::error(
            "cannot make new WAL entries during recovery",
        ));
    }

    // Not in recovery -> InsertTimeLineID is fixed; read without a lock.
    let insert_tli = ctl.InsertTimeLineID;

    let prev_do_page_writes = DO_PAGE_WRITES.with(Cell::get);

    // We own a heap copy of the header so we can fill in xl_prev / xl_crc.
    let mut hdr: std::vec::Vec<u8> = header.to_vec();

    let start_pos;
    let end_pos;
    let inserted;

    // START_CRIT_SECTION() — interrupts already held off by the caller's crit
    // section in C; the seam path keeps the critical-section semantics implicit.

    match class {
        WalInsertClass::Normal => {
            WALInsertLockAcquire()?;

            // Re-check RedoRecPtr.
            let insert_redo = ctl.Insert.RedoRecPtr;
            if redo_rec_ptr_cached() != insert_redo {
                debug_assert!(redo_rec_ptr_cached() < insert_redo);
                set_redo_rec_ptr_cached(insert_redo);
            }
            let dpw = ctl.Insert.fullPageWrites || ctl.Insert.runningBackups > 0;
            DO_PAGE_WRITES.with(|c| c.set(dpw));

            if dpw
                && (!prev_do_page_writes
                    || (fpw_lsn != crate::InvalidXLogRecPtr && fpw_lsn <= redo_rec_ptr_cached()))
            {
                // Caller must back up a buffer it didn't; start over.
                WALInsertLockRelease()?;
                return Ok(crate::InvalidXLogRecPtr);
            }

            // SAFETY: ctl is the live shmem region.
            let (s, e, prev) = unsafe { ReserveXLogInsertLocation(ctl, xl_tot_len) };
            poke_prev(&mut hdr, prev);
            start_pos = s;
            end_pos = e;
            inserted = true;
        }
        WalInsertClass::SpecialSwitch => {
            debug_assert_eq!(fpw_lsn, crate::InvalidXLogRecPtr);
            WALInsertLockAcquireExclusive()?;
            // SAFETY: ctl is the live shmem region; all locks held.
            let (ins, s, e, prev) = unsafe { ReserveXLogSwitch(ctl) };
            poke_prev(&mut hdr, prev);
            start_pos = s;
            end_pos = e;
            inserted = ins;
        }
        WalInsertClass::SpecialCheckpoint => {
            debug_assert_eq!(fpw_lsn, crate::InvalidXLogRecPtr);
            WALInsertLockAcquireExclusive()?;
            // SAFETY: ctl is the live shmem region; all locks held.
            let (s, e, prev) = unsafe { ReserveXLogInsertLocation(ctl, xl_tot_len) };
            poke_prev(&mut hdr, prev);
            start_pos = s;
            end_pos = e;
            // RedoRecPtr = Insert->RedoRecPtr = StartPos.
            let ctl_mut = ctl_ptr;
            // SAFETY: holding all locks serializes RedoRecPtr update.
            unsafe {
                (*ctl_mut).Insert.RedoRecPtr = s;
            }
            set_redo_rec_ptr_cached(s);
            inserted = true;
        }
    }

    if inserted {
        // Calculate CRC of the record header now that xl_prev is filled in.
        // rdata_crc starts as rechdr->xl_crc (the caller's running CRC over the
        // record data), then COMP over offsetof(XLogRecord, xl_crc) header
        // bytes, then FIN.
        let seed = rdata_u32(&hdr, OFFSETOF_XL_CRC);
        let crc = finish_record_crc(seed, &hdr[..OFFSETOF_XL_CRC]);
        poke_crc(&mut hdr, crc);

        // Build the rdata view with the finalised header replacing fragment 0.
        let mut frags: std::vec::Vec<&[u8]> = std::vec::Vec::with_capacity(rdata.len());
        frags.push(&hdr[..]);
        for f in &rdata[1..] {
            frags.push(f);
        }

        // SAFETY: ctl is the live shmem region; insertion lock held.
        unsafe {
            CopyXLogRecordToWAL(
                ctl,
                xl_tot_len,
                class == WalInsertClass::SpecialSwitch,
                &frags,
                start_pos,
                end_pos,
                insert_tli,
            )?;
        }

        // Update lastImportantAt unless flagged unimportant.
        if (flags & XLOG_MARK_UNIMPORTANT) == 0 {
            let lockno = if HOLDING_ALL_LOCKS.with(Cell::get) {
                0
            } else {
                MY_LOCK_NO.with(Cell::get)
            };
            // SAFETY: live lock array.
            unsafe {
                let locks = wal_insert_locks();
                (*locks.add(lockno)).l.lastImportantAt = start_pos;
            }
        }
    }

    WALInsertLockRelease()?;
    // END_CRIT_SECTION().

    // MarkCurrentTransactionIdLoggedIfAny() / MarkSubxactTopXidLogged():
    // transaction-bookkeeping owned by xact.c (still unported); behaviour-
    // preserving for the insert mechanics, so skipped here. (topxid_included is
    // only consulted by that MarkSubxactTopXidLogged path.)
    let _ = topxid_included;

    // Update shared LogwrtRqst.Write if we crossed a page boundary.
    if start_pos / XLOG_BLCKSZ as u64 != end_pos / XLOG_BLCKSZ as u64 {
        // SAFETY: ctl is the live shmem region; info_lck serializes.
        unsafe {
            shmem::spin_lock_acquire(&ctl.info_lck);
            if (*ctl_ptr).LogwrtRqst.Write < end_pos {
                (*ctl_ptr).LogwrtRqst.Write = end_pos;
            }
            shmem::spin_lock_release(&ctl.info_lck);
            refresh_xlog_write_result(ctl);
        }
    }

    // XLOG_SWITCH: flush the record + the padding, then return the end of just
    // the xlog-switch record. (C: `XLogFlush(EndPos)`; the C also rewinds the
    // return value to the actual record end via `RegisterSegmentBoundary`/the
    // page-skip arithmetic — here `end_pos` already names the record end.)
    if class == WalInsertClass::SpecialSwitch {
        crate::write::XLogFlush(end_pos)?;
    }

    // Update our global variables.
    PROC_LAST_REC_PTR.with(|c| c.set(start_pos));
    XACT_LAST_REC_END.with(|c| c.set(end_pos));

    // pgWalUsage / pgstat_report_fixed instrumentation is owned by pgstat
    // (still unported) — behaviour-preserving bookkeeping, skipped.

    Ok(end_pos)
}

/// Fill `xl_prev` in the owned header copy.
#[inline]
fn poke_prev(hdr: &mut [u8], prev: XLogRecPtr) {
    hdr[OFFSETOF_XL_PREV..OFFSETOF_XL_PREV + 8].copy_from_slice(&prev.to_ne_bytes());
}

/// Fill `xl_crc` in the owned header copy.
#[inline]
fn poke_crc(hdr: &mut [u8], crc: u32) {
    hdr[OFFSETOF_XL_CRC..OFFSETOF_XL_CRC + 4].copy_from_slice(&crc.to_ne_bytes());
}

/// `COMP_CRC32C(rdata_crc, rechdr, offsetof(XLogRecord, xl_crc));
/// FIN_CRC32C(rdata_crc);` — continue the caller's running CRC over the header
/// prefix and finalise. `seed` is the partial (non-finalised) CRC the caller
/// accumulated over the record data.
#[inline]
fn finish_record_crc(seed: u32, header_prefix: &[u8]) -> u32 {
    let crc = port_crc32c::pg_comp_crc32c_sb8(seed, header_prefix);
    crc ^ 0xFFFF_FFFF
}

// ===========================================================================
// XLogInsertAllowed (xlog.c:6451).
// ===========================================================================

std::thread_local! {
    /// `static int LocalXLogInsertAllowed = -1` (xlog.c:261).
    static LOCAL_XLOG_INSERT_ALLOWED: Cell<i32> = const { Cell::new(-1) };
}

/// `XLogInsertAllowed(void)` (xlog.c:6451).
pub fn XLogInsertAllowed() -> bool {
    let v = LOCAL_XLOG_INSERT_ALLOWED.with(Cell::get);
    if v >= 0 {
        return v != 0;
    }
    if shmem::RecoveryInProgress() {
        return false;
    }
    LOCAL_XLOG_INSERT_ALLOWED.with(|c| c.set(1));
    true
}
