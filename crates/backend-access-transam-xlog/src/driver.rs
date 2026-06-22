//! The `XLogCtl` shmem READ accessors + the small in-memory setters of
//! `access/transam/xlog.c` (PostgreSQL 18.3) — the F2 "driver" leg that reads
//! and pokes the live `XLogCtl` shared-memory region under the genuine
//! spinlocks / builtin LWLocks. These are the small process-singleton entry
//! points the checkpointer / bgwriter / walwriter / startup process consult
//! once the WAL engine is running.
//!
//! The heavy write/fsync loop is in [`crate::write`]; the per-record insert
//! path is in [`crate::insert`]; this module collects the remaining small
//! shmem getters/setters whose bodies are pure reads/writes of `XLogCtl`
//! fields under `info_lck` / `WALWriteLock` / `ControlFileLock`, plus
//! [`XLogNeedsFlush`]'s not-in-recovery durable predicate.
//!
//! Each genuinely cross-subsystem callee (the walwriter latch wakeup, the
//! recovery-side `minRecoveryPoint` machinery owned by xlogrecovery, task #13)
//! crosses its owner's seam or seam-and-panics until it lands; everything else
//! is grounded 1:1 in xlog.c.

#![allow(non_snake_case)]

extern crate std;

use backend_utils_error::PgResult;
use types_core::{pg_time_t, TimeLineID, XLogRecPtr, XLogSegNo};
use types_storage::storage::{LW_EXCLUSIVE, LW_SHARED};
use types_wal::xlog_consts::{RecoveryState, XLOG_BLCKSZ};

use backend_storage_file_fd_seams as fd;
use backend_storage_lmgr_lwlock as lwlock;
use backend_utils_init_small::globals;
use backend_utils_misc_guc_tables::vars;

use crate::shmem::{
    self, logwrt_result, refresh_xlog_write_result, wal_segment_size, xlog_ctl, XLogCtlData,
};
use crate::{
    InvalidXLogRecPtr, IsXLogFileName, XLogFileName, XLogFromFileName, XLogRecPtrIsInvalid,
};

/// `WALWriteLock` — offset 8 in the `MainLWLockArray` (`lwlocklist.h`).
const WAL_WRITE_LOCK: usize = 8;
/// `ControlFileLock` — offset 9 in the `MainLWLockArray` (`lwlocklist.h`).
const CONTROL_FILE_LOCK: usize = 9;

/// `XLOGDIR` (xlog_internal.h).
const XLOGDIR: &str = "pg_wal";

/// Acquire the live `XLogCtl` shmem region, panicking if not yet initialized.
/// (The C globals are always set by `XLOGShmemInit` before any of these
/// process-singleton entries can run.)
#[inline]
fn ctl<'a>() -> &'a XLogCtlData {
    let p = xlog_ctl();
    assert!(!p.is_null(), "XLogCtl shmem not initialized");
    // SAFETY: live shmem region, set by XLOGShmemInit.
    unsafe { &*p }
}

// ===========================================================================
// XLogSetAsyncXactLSN (xlog.c:2629).
// ===========================================================================

/// `XLogSetAsyncXactLSN(asyncXactLSN)` (xlog.c:2629) — record the LSN of an
/// async commit, and (if appropriate) wake the WAL writer so it reaches disk
/// in time.
pub fn XLogSetAsyncXactLSN(async_xact_lsn: XLogRecPtr) {
    let ctl = ctl();
    let write_rqst_ptr = async_xact_lsn;
    let mut wakeup = false;

    shmem::spin_lock_acquire(&ctl.info_lck);
    let sleeping = ctl.WalWriterSleeping;
    let prev_async_xact_lsn = ctl.asyncXactLSN;
    if ctl.asyncXactLSN < async_xact_lsn {
        // SAFETY: live shmem region, info_lck held.
        let ctl_mut = ctl as *const XLogCtlData as *mut XLogCtlData;
        unsafe { (*ctl_mut).asyncXactLSN = async_xact_lsn };
    }
    shmem::spin_lock_release(&ctl.info_lck);

    // If somebody else already pushed a more aggressive LSN, they did our work.
    if async_xact_lsn <= prev_async_xact_lsn {
        return;
    }

    // If the walwriter is sleeping, kick it; otherwise decide whether it has
    // enough WAL to flush (same arithmetic as XLogBackgroundFlush).
    if sleeping {
        wakeup = true;
    } else {
        // SAFETY: live shmem region.
        unsafe { refresh_xlog_write_result(ctl) };
        // C: `int flushblocks = WriteRqstPtr / XLOG_BLCKSZ - LogwrtResult.Flush
        // / XLOG_BLCKSZ;` — the two block counts are `XLogRecPtr` (uint64) but
        // their difference is assigned to a *signed* `int`. When the flush
        // position is already past the requested async LSN the uint64 subtraction
        // wraps and narrows to a negative `int`, which then correctly fails the
        // `>= WalWriterFlushAfter` test (no wakeup). Mirror that with wrapping
        // u64 math narrowed to i32 rather than a checked subtraction (which would
        // panic on underflow in debug builds).
        let flushblocks = (write_rqst_ptr / XLOG_BLCKSZ as u64)
            .wrapping_sub(logwrt_result().Flush / XLOG_BLCKSZ as u64)
            as i32;
        let wal_writer_flush_after = vars::WalWriterFlushAfter.read();
        if wal_writer_flush_after == 0 || flushblocks >= wal_writer_flush_after {
            wakeup = true;
        }
    }

    if wakeup {
        // SetLatch(&GetPGProcByNumber(ProcGlobal->walwriterProc)->procLatch).
        // The proc-array / latch wakeup is owned by proc.c / latch.c; reaching
        // the walwriter's latch is a throughput optimization (the walwriter
        // also wakes on its own timer). Deferred to those owners — the wakeup
        // is not durability-critical.
        wake_walwriter();
    }
}

/// `SetLatch(&walwriter->procLatch)` — wake the WAL writer. The proc-array
/// lookup (`ProcGlobal->walwriterProc`) + `SetLatch` are owned by proc.c /
/// latch.c (not yet ported here). This wakeup is a latency optimization only
/// (the walwriter also fires on its own `WalWriterDelay` timer), so until those
/// owners land we behaviour-preservingly skip the early kick.
#[inline]
fn wake_walwriter() {
    // proc.c/latch.c owner not yet reachable from this crate; no-op (the
    // walwriter wakes on its timer regardless). Behaviour-preserving.
}

// ===========================================================================
// XLogSetReplicationSlotMinimumLSN / XLogGetReplicationSlotMinimumLSN
// (xlog.c:2689 / 2702).
// ===========================================================================

/// `XLogSetReplicationSlotMinimumLSN(lsn)` (xlog.c:2689) — record the LSN up to
/// which WAL may be removed (not required by any replication slot).
pub fn XLogSetReplicationSlotMinimumLSN(lsn: XLogRecPtr) {
    let ctl = ctl();
    shmem::spin_lock_acquire(&ctl.info_lck);
    // SAFETY: live shmem region, info_lck held.
    let ctl_mut = ctl as *const XLogCtlData as *mut XLogCtlData;
    unsafe { (*ctl_mut).replicationSlotMinLSN = lsn };
    shmem::spin_lock_release(&ctl.info_lck);
}

/// `XLogGetReplicationSlotMinimumLSN()` (xlog.c:2702) — the oldest LSN we must
/// retain to satisfy some replication slot.
pub fn XLogGetReplicationSlotMinimumLSN() -> XLogRecPtr {
    let ctl = ctl();
    shmem::spin_lock_acquire(&ctl.info_lck);
    let retval = ctl.replicationSlotMinLSN;
    shmem::spin_lock_release(&ctl.info_lck);
    retval
}

// ===========================================================================
// XLogNeedsFlush (xlog.c:3128).
// ===========================================================================

/// `XLogNeedsFlush(record)` (xlog.c:3128) — whether `record` still needs
/// flushing.
///
/// During recovery the predicate is "would `minRecoveryPoint` need updating",
/// which depends on the recovery-side `LocalMinRecoveryPoint` /
/// `updateMinRecoveryPoint` machinery owned by xlogrecovery (task #13). That
/// leg seam-and-panics; the not-in-recovery durable path (the F2 concern) is
/// grounded here.
pub fn XLogNeedsFlush(record: XLogRecPtr) -> PgResult<bool> {
    if shmem::RecoveryInProgress() {
        // minRecoveryPoint update predicate — recovery-side, unported.
        let _ = XLogRecPtrIsInvalid(record);
        panic!(
            "xlog recovery driver not ported: XLogNeedsFlush during recovery consults \
             LocalMinRecoveryPoint / updateMinRecoveryPoint (owned by xlogrecovery, task #13)"
        );
    }

    // Quick exit if already known flushed.
    if record <= logwrt_result().Flush {
        return Ok(false);
    }

    // Read LogwrtResult and update local state, then check again.
    // SAFETY: live shmem region.
    unsafe { refresh_xlog_write_result(ctl()) };
    if record <= logwrt_result().Flush {
        return Ok(false);
    }

    Ok(true)
}

// ===========================================================================
// CheckXLogRemoved / XLogGetLastRemovedSegno / XLogGetOldestSegno
// (xlog.c:3747 / 3777 / 3793).
// ===========================================================================

/// `CheckXLogRemoved(segno, tli)` (xlog.c:3747) — error if a WAL segment that
/// is still needed has already been removed.
pub fn CheckXLogRemoved(segno: XLogSegNo, tli: TimeLineID) -> PgResult<()> {
    let ctl = ctl();
    shmem::spin_lock_acquire(&ctl.info_lck);
    let last_removed_seg_no = ctl.lastRemovedSegNo;
    shmem::spin_lock_release(&ctl.info_lck);

    if segno <= last_removed_seg_no {
        let filename = XLogFileName(tli, segno, wal_segment_size());
        return Err(backend_utils_error::PgError::error(std::format!(
            "requested WAL segment {filename} has already been removed"
        )));
    }
    Ok(())
}

/// `XLogGetLastRemovedSegno()` (xlog.c:3777) — the last WAL segment removed, or
/// `0` if none since startup.
pub fn XLogGetLastRemovedSegno() -> XLogSegNo {
    let ctl = ctl();
    shmem::spin_lock_acquire(&ctl.info_lck);
    let last_removed_seg_no = ctl.lastRemovedSegNo;
    shmem::spin_lock_release(&ctl.info_lck);
    last_removed_seg_no
}

/// `XLogGetOldestSegno(tli)` (xlog.c:3793) — the oldest WAL segment on `tli`
/// still present in `XLOGDIR`, or `0` if none.
pub fn XLogGetOldestSegno(tli: TimeLineID) -> XLogSegNo {
    let mut oldest_segno: XLogSegNo = 0;
    let seg = wal_segment_size();

    let names = fd::read_dir_names_logged::call(XLOGDIR);
    for name in names {
        // Ignore files that are not XLOG segments.
        if !IsXLogFileName(&name) {
            continue;
        }
        // Parse the filename for its TLI and segno.
        let (file_tli, file_segno) = match XLogFromFileName(&name, seg) {
            Ok(v) => v,
            Err(_) => continue,
        };
        // Ignore anything that's not from the TLI of interest.
        if tli != file_tli {
            continue;
        }
        if oldest_segno == 0 || file_segno < oldest_segno {
            oldest_segno = file_segno;
        }
    }
    oldest_segno
}

// ===========================================================================
// GetFakeLSNForUnloggedRel (xlog.c:4663).
// ===========================================================================

/// `GetFakeLSNForUnloggedRel()` (xlog.c:4663) — a monotonically-increasing
/// fake LSN for unlogged relations.
pub fn GetFakeLSNForUnloggedRel() -> XLogRecPtr {
    ctl().unloggedLSN.fetch_add(1)
}

// ===========================================================================
// GetRecoveryState (xlog.c:6431).
// ===========================================================================

/// `GetRecoveryState()` (xlog.c:6431) — the crash / archive / done recovery
/// state.
pub fn GetRecoveryState() -> RecoveryState {
    let ctl = ctl();
    shmem::spin_lock_acquire(&ctl.info_lck);
    let retval = ctl.SharedRecoveryState;
    shmem::spin_lock_release(&ctl.info_lck);
    retval
}

// ===========================================================================
// GetFullPageWriteInfo (xlog.c:6528).
// ===========================================================================

/// `GetFullPageWriteInfo(*RedoRecPtr_p, *doPageWrites_p)` (xlog.c:6528) — the
/// backend-local `RedoRecPtr` + `doPageWrites` cached at the last
/// `RefreshFullPageWrites`/insert. Returns `(RedoRecPtr, doPageWrites)`.
pub fn GetFullPageWriteInfo() -> (XLogRecPtr, bool) {
    (shmem::redo_rec_ptr_cached(), crate::insert::do_page_writes())
}

// ===========================================================================
// GetWALInsertionTimeLine (xlog.c:6581).
// ===========================================================================

/// `GetWALInsertionTimeLine()` (xlog.c:6581) — the WAL insertion timeline of a
/// system known not to be in recovery (no lock required, the value is fixed).
pub fn GetWALInsertionTimeLine() -> TimeLineID {
    let ctl = ctl();
    debug_assert!(ctl.SharedRecoveryState == RecoveryState::Done);
    ctl.InsertTimeLineID
}

// ===========================================================================
// GetLastImportantRecPtr (xlog.c:6617).
// ===========================================================================

/// `GetLastImportantRecPtr()` (xlog.c:6617) — the LSN of the last *important*
/// WAL record inserted, the max of `WALInsertLocks[i].lastImportantAt`.
pub fn GetLastImportantRecPtr() -> XLogRecPtr {
    let ctl = ctl();
    let mut res: XLogRecPtr = InvalidXLogRecPtr;
    for i in 0..crate::shmem::NUM_XLOGINSERT_LOCKS {
        // WAL insert locks only support exclusive mode; we must use it to avoid
        // torn reads of the LSN on weakly-ordered platforms.
        // SAFETY: `ctl` is the live shmem region; the lock array is co-allocated.
        let lock = unsafe { &(*ctl.Insert.WALInsertLocks.add(i)).l.lock };
        lwlock::LWLockAcquire(lock, LW_EXCLUSIVE, globals::MyProcNumber())
            .expect("WAL insert lock acquire failed in GetLastImportantRecPtr");
        // SAFETY: lock held; live shmem region.
        let last_important = unsafe { (*ctl.Insert.WALInsertLocks.add(i)).l.lastImportantAt };
        lwlock::LWLockRelease(lock)
            .expect("WAL insert lock release failed in GetLastImportantRecPtr");
        if res < last_important {
            res = last_important;
        }
    }
    res
}

// ===========================================================================
// GetLastSegSwitchData (xlog.c:6646).
// ===========================================================================

/// `GetLastSegSwitchData(*lastSwitchLSN)` (xlog.c:6646) — the time + LSN of the
/// last WAL segment switch. Returns `(lastSegSwitchTime, lastSegSwitchLSN)`.
pub fn GetLastSegSwitchData() -> (pg_time_t, XLogRecPtr) {
    let ctl = ctl();
    let wal_write_lock = lwlock::main_lock_ref(WAL_WRITE_LOCK);
    // Need WALWriteLock, but a shared lock is sufficient.
    lwlock::LWLockAcquire(wal_write_lock, LW_SHARED, globals::MyProcNumber())
        .expect("WALWriteLock acquire failed in GetLastSegSwitchData");
    let result = ctl.lastSegSwitchTime;
    let last_switch_lsn = ctl.lastSegSwitchLSN;
    lwlock::LWLockRelease(wal_write_lock)
        .expect("WALWriteLock release failed in GetLastSegSwitchData");
    (result, last_switch_lsn)
}

// ===========================================================================
// GetXLogWriteRecPtr (xlog.c:9524).
// ===========================================================================

/// `GetXLogWriteRecPtr()` (xlog.c:9524) — the last *written* (not necessarily
/// flushed) WAL position.
pub fn GetXLogWriteRecPtr() -> XLogRecPtr {
    // SAFETY: live shmem region.
    unsafe { refresh_xlog_write_result(ctl()) };
    logwrt_result().Write
}

// ===========================================================================
// GetOldestRestartPoint (xlog.c:9533).
// ===========================================================================

/// `GetOldestRestartPoint(*oldrecptr, *oldtli)` (xlog.c:9533) — the redo
/// pointer + TLI of the last checkpoint/restartpoint, read from the control
/// file under `ControlFileLock`. Returns `(oldrecptr, oldtli)`.
pub fn GetOldestRestartPoint() -> (XLogRecPtr, TimeLineID) {
    let control_file_lock = lwlock::main_lock_ref(CONTROL_FILE_LOCK);
    lwlock::LWLockAcquire(control_file_lock, LW_SHARED, globals::MyProcNumber())
        .expect("ControlFileLock acquire failed in GetOldestRestartPoint");
    let (oldrecptr, oldtli) = shmem::control_file_checkpoint_redo();
    lwlock::LWLockRelease(control_file_lock)
        .expect("ControlFileLock release failed in GetOldestRestartPoint");
    (oldrecptr, oldtli)
}

// ===========================================================================
// SetWalWriterSleeping (xlog.c:9586).
// ===========================================================================

/// `SetWalWriterSleeping(sleeping)` (xlog.c:9586) — publish the walwriter idle
/// state into shared memory.
pub fn SetWalWriterSleeping(sleeping: bool) {
    let ctl = ctl();
    shmem::spin_lock_acquire(&ctl.info_lck);
    // SAFETY: live shmem region, info_lck held.
    let ctl_mut = ctl as *const XLogCtlData as *mut XLogCtlData;
    unsafe { (*ctl_mut).WalWriterSleeping = sleeping };
    shmem::spin_lock_release(&ctl.info_lck);
}
