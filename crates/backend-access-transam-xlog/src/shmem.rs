//! The `XLogCtl` shared-memory region of `access/transam/xlog.c` (PostgreSQL
//! 18.3): the `XLogCtlData`/`XLogCtlInsert`/`WALInsertLock` shmem structs, the
//! WAL-insertion-lock array, `XLOGShmemSize`/`XLOGShmemInit`, the control-file
//! disk I/O (`ReadControlFile`/`WriteControlFile`/`UpdateControlFile`), and the
//! shmem position readers (`GetRedoRecPtr`/`GetXLogInsertRecPtr`/
//! `GetFlushRecPtr`/`GetWALInsertionTimeLineIfSet`/`GetSystemIdentifier`/
//! `DataChecksumsEnabled`/…).
//!
//! The C file-scope globals `XLogCtl` (a `XLogCtlData *` into shared memory),
//! `ControlFile` (a `ControlFileData *`), the private `WALInsertLocks` copy, and
//! the backend-local `RedoRecPtr`/`LogwrtResult`/`wal_segment_size` caches are
//! reproduced as backend-thread-local cells holding the *real* shared-memory
//! pointers reserved through `ShmemInitStruct`. The structs are laid out
//! `#[repr(C)]` exactly as in C, and the trailing `xlblocks`/`pages`/
//! `WALInsertLocks` arrays are sub-allocated from the same `ShmemInitStruct`
//! region with the same alignment arithmetic, so the lock array and atomics are
//! the genuine shared words other backends synchronize on (LWLockWaitForVar /
//! pg_atomic over `insertingAt`, the `info_lck`/`insertpos_lck` spinlocks).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate std;

use alloc::vec::Vec;
use core::cell::Cell;
use core::mem::{align_of, size_of};

use backend_utils_error::{PgError, PgResult};

use types_control::{
    ControlFileData, CheckPoint, DBState, FLOATFORMAT_VALUE, MOCK_AUTH_NONCE_LEN,
    PG_CONTROL_FILE_SIZE, PG_CONTROL_VERSION,
};
use types_core::{pg_crc32c, pg_time_t, FullTransactionId, TimeLineID, XLogRecPtr, XLogSegNo};
use types_storage::storage::{pg_atomic_uint64, LWLock, Spinlock, LWTRANCHE_WAL_INSERT};
use types_wal::xlog_consts::{
    RecoveryState, SIZE_OF_XLOG_LONG_PHD, SIZE_OF_XLOG_SHORT_PHD, XLOG_BLCKSZ,
};

use backend_storage_file_fd_seams as fd;
use backend_storage_ipc_shmem_seams as shmem;
use backend_storage_lmgr_lwlock_seams as lwlock;

use crate::{
    CalculateCheckpointSegments, ConvertToXSegs, InvalidXLogRecPtr, IsValidWalSegSize,
    UsableBytesInPage, XLogBytePosToRecPtr,
};

// ===========================================================================
// Compile-time configuration mirrors of the C macros consulted here.
// ===========================================================================

/// `NUM_XLOGINSERT_LOCKS` (xlog.c:175).
pub const NUM_XLOGINSERT_LOCKS: usize = 8;

/// `PG_CACHE_LINE_SIZE` (`pg_config_manual.h`); the `WALInsertLockPadded`
/// stride and the `XLogCtlInsert.pad`.
pub const PG_CACHE_LINE_SIZE: usize = 128;

/// `PG_IO_ALIGN_SIZE` (`c.h`) — the I/O buffer alignment.
pub const PG_IO_ALIGN_SIZE: usize = 4096;

/// `XLOG_CONTROL_FILE` (`access/xlog_internal.h`) — relative to `DataDir`
/// (the process has chdir'd into the data directory).
pub const XLOG_CONTROL_FILE: &str = "global/pg_control";

/// `CATALOG_VERSION_NO` (`catalog/catversion.h`, PostgreSQL 18.3).
pub const CATALOG_VERSION_NO: u32 = 202506291;

// Compile-time-fixed compatibility constants written to / checked against the
// control file (the C `#define`s from pg_config*.h, mirrored for the codec).
const BLCKSZ: u32 = 8192;
const RELSEG_SIZE: u32 = 131072;
const NAMEDATALEN: u32 = 64;
const INDEX_MAX_KEYS: u32 = 32;
const TOAST_MAX_CHUNK_SIZE: u32 = 1996;
const LOBLKSIZE: u32 = 2048;
const MAXIMUM_ALIGNOF: u32 = 8;
const FLOAT8PASSBYVAL: bool = true;

// ===========================================================================
// XLogwrtRqst / XLogwrtResult — the (Write, Flush) request/result pairs.
// ===========================================================================

/// `XLogwrtRqst` (xlog.c) — a (Write, Flush) request pair.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct XLogwrtRqst {
    pub Write: XLogRecPtr,
    pub Flush: XLogRecPtr,
}

/// `XLogwrtResult` (xlog.c) — a (Write, Flush) result pair.
#[derive(Clone, Copy, Debug, Default)]
pub struct XLogwrtResult {
    pub Write: XLogRecPtr,
    pub Flush: XLogRecPtr,
}

// ===========================================================================
// WALInsertLock / WALInsertLockPadded — the shmem WAL-insertion lock array.
// ===========================================================================

/// `WALInsertLock` (xlog.c:392) — one WAL-insertion lock: the `LWLock`, the
/// `insertingAt` atomic (the position an inserter is currently filling, used by
/// `LWLockWaitForVar`), and the `lastImportantAt` LSN.
#[repr(C)]
pub struct WALInsertLock {
    pub lock: LWLock,
    pub insertingAt: pg_atomic_uint64,
    pub lastImportantAt: XLogRecPtr,
}

/// `WALInsertLockPadded` (xlog.c:406) — a `WALInsertLock` padded out to a full
/// cache line so each lock sits on its own line. `#[repr(C, align(128))]`
/// reproduces both the size and the placement guarantee of the C union.
#[repr(C, align(128))]
pub struct WALInsertLockPadded {
    pub l: WALInsertLock,
}

const _: () = assert!(size_of::<WALInsertLockPadded>() == PG_CACHE_LINE_SIZE);

// ===========================================================================
// XLogCtlInsert — shared state for WAL insertion (xlog.c:421).
// ===========================================================================

/// `XLogCtlInsert` (xlog.c:421) — shared state for WAL insertion.
#[repr(C)]
pub struct XLogCtlInsert {
    /// `insertpos_lck` — protects `CurrBytePos`/`PrevBytePos`.
    pub insertpos_lck: Spinlock,

    pub CurrBytePos: u64,
    pub PrevBytePos: u64,

    /// Keep the heavily-contended spinlock and byte positions on their own
    /// cache line, away from the rarely-updated `RedoRecPtr`/`fullPageWrites`.
    pub pad: [u8; PG_CACHE_LINE_SIZE],

    pub RedoRecPtr: XLogRecPtr,
    pub fullPageWrites: bool,

    pub runningBackups: i32,
    pub lastBackupStart: XLogRecPtr,

    /// `WALInsertLockPadded *WALInsertLocks` — points into the same
    /// `ShmemInitStruct` region as the enclosing `XLogCtlData`.
    pub WALInsertLocks: *mut WALInsertLockPadded,
}

// ===========================================================================
// XLogCtlData — total shared-memory state for XLOG (xlog.c:475).
// ===========================================================================

/// `XLogCtlData` (xlog.c:475) — the total shared-memory state for XLOG. Laid
/// out `#[repr(C)]` field-for-field with the C struct; the trailing
/// `pages`/`xlblocks` are raw pointers into the same `ShmemInitStruct` region.
#[repr(C)]
pub struct XLogCtlData {
    pub Insert: XLogCtlInsert,

    // Protected by info_lck:
    pub LogwrtRqst: XLogwrtRqst,
    pub RedoRecPtr: XLogRecPtr,
    pub ckptFullXid: FullTransactionId,
    pub asyncXactLSN: XLogRecPtr,
    pub replicationSlotMinLSN: XLogRecPtr,

    pub lastRemovedSegNo: XLogSegNo,

    /// Fake LSN counter, for unlogged relations.
    pub unloggedLSN: pg_atomic_uint64,

    pub lastSegSwitchTime: pg_time_t,
    pub lastSegSwitchLSN: XLogRecPtr,

    // Accessed using atomics — info_lck not needed:
    pub logInsertResult: pg_atomic_uint64,
    pub logWriteResult: pg_atomic_uint64,
    pub logFlushResult: pg_atomic_uint64,

    pub InitializedUpTo: XLogRecPtr,

    pub pages: *mut u8,
    pub xlblocks: *mut pg_atomic_uint64,
    pub XLogCacheBlck: i32,

    pub InsertTimeLineID: TimeLineID,
    pub PrevTimeLineID: TimeLineID,

    pub SharedRecoveryState: RecoveryState,

    pub InstallXLogFileSegmentActive: bool,

    pub WalWriterSleeping: bool,

    pub lastCheckPointRecPtr: XLogRecPtr,
    pub lastCheckPointEndPtr: XLogRecPtr,
    pub lastCheckPoint: CheckPoint,

    pub lastFpwDisableRecPtr: XLogRecPtr,

    pub info_lck: Spinlock,
}

// ===========================================================================
// C file-scope globals (xlog.c:591-594, plus the backend-local LSN caches).
//
// These hold the genuine shared-memory pointers reserved by ShmemInitStruct,
// so reads/writes go through the real shared words. They are per-backend
// process locals in C (each backend attaches to the same segment), modeled
// here as thread-local cells.
// ===========================================================================

std::thread_local! {
    /// `static XLogCtlData *XLogCtl` (xlog.c:591).
    static XLOG_CTL: Cell<*mut XLogCtlData> = const { Cell::new(core::ptr::null_mut()) };
    /// `static ControlFileData *ControlFile` (xlog.c:597).
    static CONTROL_FILE: Cell<*mut ControlFileData> = const { Cell::new(core::ptr::null_mut()) };
    /// `static WALInsertLockPadded *WALInsertLocks` (xlog.c:594).
    static WAL_INSERT_LOCKS: Cell<*mut WALInsertLockPadded> =
        const { Cell::new(core::ptr::null_mut()) };

    /// `static XLogRecPtr RedoRecPtr` (xlog.c) — backend-local cached redo ptr.
    static REDO_REC_PTR: Cell<XLogRecPtr> = const { Cell::new(0) };
    /// `static XLogwrtResult LogwrtResult` (xlog.c) — backend-local cache.
    static LOGWRT_RESULT: Cell<XLogwrtResult> =
        const { Cell::new(XLogwrtResult { Write: 0, Flush: 0 }) };

    /// `int wal_segment_size` (xlog.c GUC global). Read from the control file by
    /// `ReadControlFile`; defaults to the build default until then.
    static WAL_SEGMENT_SIZE: Cell<i32> =
        const { Cell::new(types_wal::xlog_consts::DEFAULT_XLOG_SEG_SIZE) };

    /// `int XLOGbuffers` (xlog.c GUC global) — the resolved WAL buffer count.
    /// The auto-tune (`-1`) is resolved by `check_wal_buffers` before
    /// `XLOGShmemSize`/`XLOGShmemInit` run; until set we use the GUC minimum
    /// (8 blocks, `XLOGChooseNumBuffers`'s floor).
    static XLOG_BUFFERS: Cell<i32> = const { Cell::new(8) };
}

/// Set the resolved `XLOGbuffers` GUC value (called by the GUC `check_wal_buffers`
/// path before shmem sizing/init). Owned by xlog.c.
pub fn set_xlog_buffers(n: i32) {
    XLOG_BUFFERS.with(|c| c.set(n));
}

/// Read the resolved `XLOGbuffers` GUC value.
#[inline]
pub fn xlog_buffers() -> i32 {
    XLOG_BUFFERS.with(Cell::get)
}

#[inline]
pub(crate) fn xlog_ctl() -> *mut XLogCtlData {
    XLOG_CTL.with(Cell::get)
}

/// The genuine `WALInsertLocks` shmem array pointer (backend-local copy).
#[inline]
pub(crate) fn wal_insert_locks() -> *mut WALInsertLockPadded {
    WAL_INSERT_LOCKS.with(Cell::get)
}

/// `static XLogRecPtr RedoRecPtr` (xlog.c) — the backend-local cached redo
/// pointer cell, exposed for the insert path.
pub(crate) fn redo_rec_ptr_cached() -> XLogRecPtr {
    REDO_REC_PTR.with(Cell::get)
}
pub(crate) fn set_redo_rec_ptr_cached(v: XLogRecPtr) {
    REDO_REC_PTR.with(|c| c.set(v));
}

/// `static XLogwrtResult LogwrtResult` (xlog.c) — backend-local cache.
pub(crate) fn logwrt_result() -> XLogwrtResult {
    LOGWRT_RESULT.with(Cell::get)
}

/// Overwrite the backend-local `LogwrtResult` cache (the WAL-write driver
/// advances `Write`/`Flush` locally as it dumps/fsyncs pages, before publishing
/// them back into the shared atomics).
pub(crate) fn set_logwrt_result(v: XLogwrtResult) {
    LOGWRT_RESULT.with(|c| c.set(v));
}

/// `RefreshXLogWriteResult(LogwrtResult)` (xlog.c macro) — pull the atomic
/// write/flush results from shared memory into the backend-local cache.
///
/// # Safety
/// `ctl` must reference the live `XLogCtl` shmem region.
pub(crate) unsafe fn refresh_xlog_write_result(ctl: &XLogCtlData) {
    let write = ctl.logWriteResult.read();
    let flush = ctl.logFlushResult.read();
    LOGWRT_RESULT.with(|c| c.set(XLogwrtResult { Write: write, Flush: flush }));
}

#[inline]
fn control_file_ptr() -> *mut ControlFileData {
    CONTROL_FILE.with(Cell::get)
}

/// Read `wal_segment_size` (the GUC global owned by xlog.c).
#[inline]
pub fn wal_segment_size() -> i32 {
    WAL_SEGMENT_SIZE.with(Cell::get)
}

// ===========================================================================
// SpinLockAcquire / SpinLockRelease / SpinLockInit (storage/s_lock.h macros).
// ===========================================================================

#[inline]
pub(crate) fn spin_lock_acquire(lock: &Spinlock) {
    backend_storage_lmgr_s_lock::s_lock_macro(lock, Some(file!()), line!() as i32, None);
}

#[inline]
pub(crate) fn spin_lock_release(lock: &Spinlock) {
    backend_storage_lmgr_s_lock::s_unlock(lock);
}

#[inline]
fn spin_lock_init(lock: &Spinlock) {
    backend_storage_lmgr_s_lock::s_init_lock(lock);
}

// ===========================================================================
// XLOGShmemSize / XLOGShmemInit (xlog.c:4929 / 4980).
// ===========================================================================

/// `XLOGShmemSize()` (xlog.c:4929) — size of the `XLogCtl` shmem region.
///
/// The C `XLOGbuffers == -1` auto-tune is a GUC concern handled before this is
/// reached (`check_wal_buffers`); here we take the resolved buffer count.
pub fn XLOGShmemSize(XLOGbuffers: i32) -> PgResult<types_core::Size> {
    debug_assert!(XLOGbuffers > 0);

    // XLogCtl
    let mut size: types_core::Size = size_of::<XLogCtlData>();

    // WAL insertion locks, plus alignment.
    size = shmem::add_size::call(
        size,
        shmem::mul_size::call(size_of::<WALInsertLockPadded>(), NUM_XLOGINSERT_LOCKS + 1)?,
    )?;
    // xlblocks array.
    size = shmem::add_size::call(
        size,
        shmem::mul_size::call(size_of::<pg_atomic_uint64>(), XLOGbuffers as usize)?,
    )?;
    // extra alignment padding for XLOG I/O buffers.
    size = shmem::add_size::call(size, core::cmp::max(XLOG_BLCKSZ, PG_IO_ALIGN_SIZE))?;
    // and the buffers themselves.
    size = shmem::add_size::call(
        size,
        shmem::mul_size::call(XLOG_BLCKSZ, XLOGbuffers as usize)?,
    )?;

    // Note: ControlFileData is not counted; it comes out of the slop factor.
    Ok(size)
}

/// `XLOGShmemInit()` (xlog.c:4980) — allocate-or-attach the `XLogCtl` and
/// `Control File` shared-memory structures and perform the basic
/// initialization that `StartupXLOG` later completes.
///
/// `XLOGbuffers` is the resolved WAL buffer count (the GUC after auto-tune).
pub fn XLOGShmemInit(XLOGbuffers: i32) -> PgResult<()> {
    let shmem_size = XLOGShmemSize(XLOGbuffers)?;

    // XLogCtl = ShmemInitStruct("XLOG Ctl", XLOGShmemSize(), &foundXLog);
    let (xlog_raw, found_xlog) = shmem::shmem_init_struct::call("XLOG Ctl", shmem_size)?;
    let xlog_ctl_ptr = xlog_raw as *mut XLogCtlData;

    // localControlFile = ControlFile;
    let local_control_file = control_file_ptr();
    // ControlFile = ShmemInitStruct("Control File", sizeof(ControlFileData), &foundCFile);
    let (cfile_raw, found_cfile) =
        shmem::shmem_init_struct::call("Control File", size_of::<ControlFileData>())?;
    let control_file_new = cfile_raw as *mut ControlFileData;

    XLOG_CTL.with(|c| c.set(xlog_ctl_ptr));
    CONTROL_FILE.with(|c| c.set(control_file_new));

    if found_cfile || found_xlog {
        // Both should be present or neither.
        debug_assert!(found_cfile && found_xlog);

        // Initialize the local copy of WALInsertLocks.
        // SAFETY: `xlog_ctl_ptr` is a live `ShmemInitStruct` region of at least
        // `sizeof(XLogCtlData)`; the segment outlives the process.
        let locks = unsafe { (*xlog_ctl_ptr).Insert.WALInsertLocks };
        WAL_INSERT_LOCKS.with(|c| c.set(locks));

        // The local control-file image, if any, is dropped (pfree). Our local
        // image was a Box; releasing the pointer cell already abandons it.
        let _ = local_control_file;
        return Ok(());
    }

    // SAFETY: fresh region of `XLOGShmemSize()` bytes; single-process init.
    unsafe {
        // memset(XLogCtl, 0, sizeof(XLogCtlData));
        core::ptr::write_bytes(xlog_raw, 0, size_of::<XLogCtlData>());

        // Move the already-read local control file into shared memory.
        if !local_control_file.is_null() {
            core::ptr::copy_nonoverlapping(local_control_file, control_file_new, 1);
            // pfree(localControlFile): reclaim the Box we leaked at read time.
            drop(std::boxed::Box::from_raw(local_control_file));
        } else {
            core::ptr::write(control_file_new, ControlFileData::default());
        }

        let ctl = &mut *xlog_ctl_ptr;

        // allocptr = ((char *) XLogCtl) + sizeof(XLogCtlData);
        let mut allocptr = xlog_raw.add(size_of::<XLogCtlData>());

        // XLogCtl->xlblocks = (pg_atomic_uint64 *) allocptr;
        let xlblocks = allocptr as *mut pg_atomic_uint64;
        ctl.xlblocks = xlblocks;
        allocptr = allocptr.add(size_of::<pg_atomic_uint64>() * XLOGbuffers as usize);

        for i in 0..XLOGbuffers as usize {
            core::ptr::write(xlblocks.add(i), pg_atomic_uint64::new(InvalidXLogRecPtr));
        }

        // WAL insertion locks: align to the full padded size.
        let stride = size_of::<WALInsertLockPadded>();
        allocptr = allocptr.add(stride - (allocptr as usize) % stride);
        let locks = allocptr as *mut WALInsertLockPadded;
        ctl.Insert.WALInsertLocks = locks;
        WAL_INSERT_LOCKS.with(|c| c.set(locks));
        allocptr = allocptr.add(stride * NUM_XLOGINSERT_LOCKS);

        for i in 0..NUM_XLOGINSERT_LOCKS {
            let slot = &mut *locks.add(i);
            // LWLockInitialize(&WALInsertLocks[i].l.lock, LWTRANCHE_WAL_INSERT);
            lwlock::lwlock_initialize::call(&mut slot.l.lock, LWTRANCHE_WAL_INSERT);
            core::ptr::write(&mut slot.l.insertingAt, pg_atomic_uint64::new(InvalidXLogRecPtr));
            slot.l.lastImportantAt = InvalidXLogRecPtr;
        }

        // Align the page buffers to a full xlog block boundary.
        let aligned = type_align(XLOG_BLCKSZ, allocptr as usize);
        allocptr = aligned as *mut u8;
        ctl.pages = allocptr;
        core::ptr::write_bytes(ctl.pages, 0, XLOG_BLCKSZ * XLOGbuffers as usize);

        // Basic initialization of XLogCtl shared data.
        ctl.XLogCacheBlck = XLOGbuffers - 1;
        ctl.SharedRecoveryState = RecoveryState::Crash;
        ctl.InstallXLogFileSegmentActive = false;
        ctl.WalWriterSleeping = false;

        spin_lock_init(&ctl.Insert.insertpos_lck);
        spin_lock_init(&ctl.info_lck);
        core::ptr::write(&mut ctl.logInsertResult, pg_atomic_uint64::new(InvalidXLogRecPtr));
        core::ptr::write(&mut ctl.logWriteResult, pg_atomic_uint64::new(InvalidXLogRecPtr));
        core::ptr::write(&mut ctl.logFlushResult, pg_atomic_uint64::new(InvalidXLogRecPtr));
        core::ptr::write(&mut ctl.unloggedLSN, pg_atomic_uint64::new(InvalidXLogRecPtr));
    }

    Ok(())
}

/// `TYPEALIGN(ALIGNVAL, LEN)` (`c.h`).
#[inline]
fn type_align(alignval: usize, len: usize) -> usize {
    (len + (alignval - 1)) & !(alignval - 1)
}

// ===========================================================================
// Position readers — the genuine shmem reads (xlog.c:6499 / 6561 / 6598 /
// 9509) under info_lck / insertpos_lck.
// ===========================================================================

/// `GetRedoRecPtr()` (xlog.c:6499) — the current redo pointer from shared
/// memory, also refreshing the backend-local `RedoRecPtr`.
pub fn GetRedoRecPtr() -> XLogRecPtr {
    let ctl = xlog_ctl();
    // SAFETY: `ctl` is the live shmem region; `info_lck` serializes the read.
    let ptr = unsafe {
        let ctl = &*ctl;
        spin_lock_acquire(&ctl.info_lck);
        let p = ctl.RedoRecPtr;
        spin_lock_release(&ctl.info_lck);
        p
    };

    REDO_REC_PTR.with(|c| {
        if c.get() < ptr {
            c.set(ptr);
        }
        c.get()
    })
}

/// `GetInsertRecPtr()` (xlog.c:6544) — approximate WAL insert position.
pub fn GetInsertRecPtr() -> XLogRecPtr {
    let ctl = xlog_ctl();
    // SAFETY: live shmem region; `info_lck` serializes the read.
    unsafe {
        let ctl = &*ctl;
        spin_lock_acquire(&ctl.info_lck);
        let recptr = ctl.LogwrtRqst.Write;
        spin_lock_release(&ctl.info_lck);
        recptr
    }
}

/// `GetFlushRecPtr(*insertTLI)` (xlog.c:6561) — the last flushed WAL position
/// plus the insert timeline. Must only be called when not in recovery.
pub fn GetFlushRecPtr() -> (XLogRecPtr, TimeLineID) {
    let ctl = xlog_ctl();
    // SAFETY: live shmem region; not-in-recovery means the TLI is fixed.
    let (flush, tli) = unsafe {
        let ctl = &*ctl;
        debug_assert!(ctl.SharedRecoveryState == RecoveryState::Done);
        // RefreshXLogWriteResult(LogwrtResult): pull the atomic write/flush
        // results into the backend-local cache.
        let write = ctl.logWriteResult.read();
        let flush = ctl.logFlushResult.read();
        LOGWRT_RESULT.with(|c| c.set(XLogwrtResult { Write: write, Flush: flush }));
        (flush, ctl.InsertTimeLineID)
    };
    (flush, tli)
}

/// `GetWALInsertionTimeLineIfSet()` (xlog.c:6598) — the insert timeline once
/// set in shared memory, else `0`.
pub fn GetWALInsertionTimeLineIfSet() -> TimeLineID {
    let ctl = xlog_ctl();
    // SAFETY: live shmem region; `info_lck` serializes the read.
    unsafe {
        let ctl = &*ctl;
        spin_lock_acquire(&ctl.info_lck);
        let tli = ctl.InsertTimeLineID;
        spin_lock_release(&ctl.info_lck);
        tli
    }
}

/// `GetXLogInsertRecPtr()` (xlog.c:9509) — the exact current WAL insert
/// position, derived from `Insert.CurrBytePos`.
pub fn GetXLogInsertRecPtr() -> XLogRecPtr {
    let ctl = xlog_ctl();
    // SAFETY: live shmem region; `insertpos_lck` serializes the read.
    let current_bytepos = unsafe {
        let insert = &(*ctl).Insert;
        spin_lock_acquire(&insert.insertpos_lck);
        let p = insert.CurrBytePos;
        spin_lock_release(&insert.insertpos_lck);
        p
    };
    XLogBytePosToRecPtr(current_bytepos, wal_segment_size())
}

// ===========================================================================
// Control-file readers (xlog.c:4615 / 4635 / 4649).
// ===========================================================================

/// `RecoveryInProgress()` (xlog.c:6411) — whether WAL recovery is still in
/// progress. Reads the shared `SharedRecoveryState`; `RECOVERY_STATE_DONE`
/// means recovery has ended and WAL insertion is permitted. (The backend-local
/// `LocalRecoveryInProgress` fast-path cache is a behaviour-preserving omission;
/// the read is cheap and the insert path only consults it when not in recovery.)
pub fn RecoveryInProgress() -> bool {
    let ctl = xlog_ctl();
    if ctl.is_null() {
        // Before XLOGShmemInit (bootstrap) C treats the system as not in
        // recovery (InRecovery drives that path separately).
        return false;
    }
    // SAFETY: live shmem region.
    unsafe { (*ctl).SharedRecoveryState != RecoveryState::Done }
}

/// `GetSystemIdentifier()` (xlog.c:4615) — the cluster's 64-bit system id.
pub fn GetSystemIdentifier() -> u64 {
    let cf = control_file_ptr();
    debug_assert!(!cf.is_null());
    // SAFETY: `cf` is the live `Control File` shmem image.
    unsafe { (*cf).system_identifier }
}

/// `GetMockAuthenticationNonce()` (xlog.c:4625) — the control-file nonce.
pub fn GetMockAuthenticationNonce() -> Option<Vec<u8>> {
    let cf = control_file_ptr();
    if cf.is_null() {
        return None;
    }
    // SAFETY: `cf` is the live `Control File` shmem image.
    Some(unsafe { (*cf).mock_authentication_nonce.to_vec() })
}

/// `DataChecksumsEnabled()` (xlog.c:4635) — whether data-page checksums are on,
/// i.e. `ControlFile->data_checksum_version > 0`.
pub fn DataChecksumsEnabled() -> bool {
    let cf = control_file_ptr();
    debug_assert!(!cf.is_null());
    // SAFETY: `cf` is the live `Control File` shmem image.
    unsafe { (*cf).data_checksum_version > 0 }
}

/// `ControlFile->checkPointCopy.{redo,ThisTimeLineID}` — the redo pointer + TLI
/// of the last checkpoint/restartpoint recorded in the control file. The caller
/// (`GetOldestRestartPoint`) holds `ControlFileLock`; this is the bare read.
pub(crate) fn control_file_checkpoint_redo() -> (XLogRecPtr, TimeLineID) {
    let cf = control_file_ptr();
    debug_assert!(!cf.is_null());
    // SAFETY: `cf` is the live `Control File` shmem image; lock held by caller.
    unsafe { ((*cf).checkPointCopy.redo, (*cf).checkPointCopy.ThisTimeLineID) }
}

/// `GetDefaultCharSignedness()` (xlog.c:4649).
pub fn GetDefaultCharSignedness() -> bool {
    let cf = control_file_ptr();
    debug_assert!(!cf.is_null());
    // SAFETY: `cf` is the live `Control File` shmem image.
    unsafe { (*cf).default_char_signedness }
}

// ===========================================================================
// ControlFileData <-> on-disk byte image codec.
//
// Field order, types and alignment padding mirror the C struct so the CRC is
// computed over the identical byte sequence the C backend produces.
// ===========================================================================

/// `sizeof(ControlFileData)` on LP64 (catalog/pg_control.h), up to and
/// including the trailing `crc` field.
pub const SIZE_OF_CONTROL_FILE_DATA: usize = 296;

fn put_u32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_ne_bytes());
}
fn put_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_ne_bytes());
}
fn put_i32(b: &mut Vec<u8>, v: i32) {
    b.extend_from_slice(&v.to_ne_bytes());
}
fn put_i64(b: &mut Vec<u8>, v: i64) {
    b.extend_from_slice(&v.to_ne_bytes());
}
fn put_f64(b: &mut Vec<u8>, v: f64) {
    b.extend_from_slice(&v.to_ne_bytes());
}
fn put_bool(b: &mut Vec<u8>, v: bool) {
    b.push(v as u8);
}
fn pad(b: &mut Vec<u8>, n: usize) {
    for _ in 0..n {
        b.push(0);
    }
}

fn checkpoint_image(b: &mut Vec<u8>, cp: &CheckPoint) {
    put_u64(b, cp.redo); // @0
    put_u32(b, cp.ThisTimeLineID); // @8
    put_u32(b, cp.PrevTimeLineID); // @12
    put_bool(b, cp.fullPageWrites); // @16
    pad(b, 3);
    put_i32(b, cp.wal_level); // @20
    put_u64(b, cp.nextXid.value); // @24
    put_u32(b, cp.nextOid); // @32
    put_u32(b, cp.nextMulti); // @36
    put_u32(b, cp.nextMultiOffset); // @40
    put_u32(b, cp.oldestXid); // @44
    put_u32(b, cp.oldestXidDB); // @48
    put_u32(b, cp.oldestMulti); // @52
    put_u32(b, cp.oldestMultiDB); // @56
    pad(b, 4); // @60: 8-align pg_time_t
    put_i64(b, cp.time); // @64
    put_u32(b, cp.oldestCommitTsXid); // @72
    put_u32(b, cp.newestCommitTsXid); // @76
    put_u32(b, cp.oldestActiveXid); // @80
    pad(b, 4); // -> 88
}

/// Serialize a [`ControlFileData`] into its C-ABI byte image, *excluding* the
/// `crc` field (the CRC is computed over `offsetof(ControlFileData, crc)`).
fn control_file_image_no_crc(cf: &ControlFileData) -> Vec<u8> {
    let mut b = Vec::with_capacity(SIZE_OF_CONTROL_FILE_DATA);
    put_u64(&mut b, cf.system_identifier); // @0
    put_u32(&mut b, cf.pg_control_version); // @8
    put_u32(&mut b, cf.catalog_version_no); // @12
    put_u32(&mut b, cf.state as u32); // @16 (DBState enum: int)
    pad(&mut b, 4); // @20: 8-align pg_time_t
    put_i64(&mut b, cf.time); // @24
    put_u64(&mut b, cf.checkPoint); // @32
    checkpoint_image(&mut b, &cf.checkPointCopy); // @40 .. +88 = @128
    put_u64(&mut b, cf.unloggedLSN); // @128
    put_u64(&mut b, cf.minRecoveryPoint); // @136
    put_u32(&mut b, cf.minRecoveryPointTLI); // @144
    pad(&mut b, 4); // @148: 8-align
    put_u64(&mut b, cf.backupStartPoint); // @152
    put_u64(&mut b, cf.backupEndPoint); // @160
    put_bool(&mut b, cf.backupEndRequired); // @168
    pad(&mut b, 3); // @169
    put_i32(&mut b, cf.wal_level); // @172
    put_bool(&mut b, cf.wal_log_hints); // @176
    pad(&mut b, 3); // @177
    put_i32(&mut b, cf.MaxConnections); // @180
    put_i32(&mut b, cf.max_worker_processes); // @184
    put_i32(&mut b, cf.max_wal_senders); // @188
    put_i32(&mut b, cf.max_prepared_xacts); // @192
    put_i32(&mut b, cf.max_locks_per_xact); // @196
    put_bool(&mut b, cf.track_commit_timestamp); // @200
    pad(&mut b, 3); // @201
    put_u32(&mut b, cf.maxAlign); // @204
    put_f64(&mut b, cf.floatFormat); // @208
    put_u32(&mut b, cf.blcksz); // @216
    put_u32(&mut b, cf.relseg_size); // @220
    put_u32(&mut b, cf.xlog_blcksz); // @224
    put_u32(&mut b, cf.xlog_seg_size); // @228
    put_u32(&mut b, cf.nameDataLen); // @232
    put_u32(&mut b, cf.indexMaxKeys); // @236
    put_u32(&mut b, cf.toast_max_chunk_size); // @240
    put_u32(&mut b, cf.loblksize); // @244
    put_bool(&mut b, cf.float8ByVal); // @248
    pad(&mut b, 3); // @249
    put_u32(&mut b, cf.data_checksum_version); // @252
    put_bool(&mut b, cf.default_char_signedness); // @256
    b.extend_from_slice(&cf.mock_authentication_nonce); // @257 .. @289
    pad(&mut b, 3); // @289: 4-align pg_crc32c -> @292
    debug_assert_eq!(b.len(), offset_of_crc());
    b
}

/// `offsetof(ControlFileData, crc)`.
const fn offset_of_crc() -> usize {
    292
}

const fn INIT_CRC32C() -> u32 {
    0xFFFF_FFFF
}
fn COMP_CRC32C(crc: u32, data: &[u8]) -> u32 {
    port_crc32c::pg_comp_crc32c_sb8(crc, data)
}
const fn FIN_CRC32C(crc: u32) -> u32 {
    crc ^ 0xFFFF_FFFF
}
const fn EQ_CRC32C(c1: u32, c2: u32) -> bool {
    c1 == c2
}

/// Compute the control-file CRC over `offsetof(ControlFileData, crc)` bytes.
fn control_file_crc(image_no_crc: &[u8]) -> pg_crc32c {
    let mut crc = INIT_CRC32C();
    crc = COMP_CRC32C(crc, image_no_crc);
    FIN_CRC32C(crc)
}

/// Serialize a [`ControlFileData`] into its full byte image including the
/// trailing CRC (296 bytes).
fn control_file_to_bytes(cf: &ControlFileData) -> Vec<u8> {
    let mut b = control_file_image_no_crc(cf);
    let crc = control_file_crc(&b);
    put_u32(&mut b, crc); // @292 .. @296
    debug_assert_eq!(b.len(), SIZE_OF_CONTROL_FILE_DATA);
    b
}

fn get_u32(b: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(b[off..off + 4].try_into().unwrap())
}
fn get_i32(b: &[u8], off: usize) -> i32 {
    i32::from_ne_bytes(b[off..off + 4].try_into().unwrap())
}
fn get_u64(b: &[u8], off: usize) -> u64 {
    u64::from_ne_bytes(b[off..off + 8].try_into().unwrap())
}
fn get_i64(b: &[u8], off: usize) -> i64 {
    i64::from_ne_bytes(b[off..off + 8].try_into().unwrap())
}
fn get_f64(b: &[u8], off: usize) -> f64 {
    f64::from_ne_bytes(b[off..off + 8].try_into().unwrap())
}
fn get_bool(b: &[u8], off: usize) -> bool {
    b[off] != 0
}

fn checkpoint_from_bytes(b: &[u8], base: usize) -> CheckPoint {
    CheckPoint {
        redo: get_u64(b, base),
        ThisTimeLineID: get_u32(b, base + 8),
        PrevTimeLineID: get_u32(b, base + 12),
        fullPageWrites: get_bool(b, base + 16),
        wal_level: get_i32(b, base + 20),
        nextXid: FullTransactionId {
            value: get_u64(b, base + 24),
        },
        nextOid: get_u32(b, base + 32),
        nextMulti: get_u32(b, base + 36),
        nextMultiOffset: get_u32(b, base + 40),
        oldestXid: get_u32(b, base + 44),
        oldestXidDB: get_u32(b, base + 48),
        oldestMulti: get_u32(b, base + 52),
        oldestMultiDB: get_u32(b, base + 56),
        time: get_i64(b, base + 64),
        oldestCommitTsXid: get_u32(b, base + 72),
        newestCommitTsXid: get_u32(b, base + 76),
        oldestActiveXid: get_u32(b, base + 80),
    }
}

fn db_state_from_u32(v: u32) -> DBState {
    match v {
        0 => DBState::Startup,
        1 => DBState::Shutdowned,
        2 => DBState::ShutdownedInRecovery,
        3 => DBState::Shutdowning,
        4 => DBState::InCrashRecovery,
        5 => DBState::InArchiveRecovery,
        6 => DBState::InProduction,
        _ => DBState::Startup,
    }
}

/// Deserialize a [`ControlFileData`] from its on-disk byte image.
fn control_file_from_bytes(b: &[u8]) -> ControlFileData {
    let mut nonce = [0u8; MOCK_AUTH_NONCE_LEN];
    nonce.copy_from_slice(&b[257..257 + MOCK_AUTH_NONCE_LEN]);
    ControlFileData {
        system_identifier: get_u64(b, 0),
        pg_control_version: get_u32(b, 8),
        catalog_version_no: get_u32(b, 12),
        state: db_state_from_u32(get_u32(b, 16)),
        time: get_i64(b, 24),
        checkPoint: get_u64(b, 32),
        checkPointCopy: checkpoint_from_bytes(b, 40),
        unloggedLSN: get_u64(b, 128),
        minRecoveryPoint: get_u64(b, 136),
        minRecoveryPointTLI: get_u32(b, 144),
        backupStartPoint: get_u64(b, 152),
        backupEndPoint: get_u64(b, 160),
        backupEndRequired: get_bool(b, 168),
        wal_level: get_i32(b, 172),
        wal_log_hints: get_bool(b, 176),
        MaxConnections: get_i32(b, 180),
        max_worker_processes: get_i32(b, 184),
        max_wal_senders: get_i32(b, 188),
        max_prepared_xacts: get_i32(b, 192),
        max_locks_per_xact: get_i32(b, 196),
        track_commit_timestamp: get_bool(b, 200),
        maxAlign: get_u32(b, 204),
        floatFormat: get_f64(b, 208),
        blcksz: get_u32(b, 216),
        relseg_size: get_u32(b, 220),
        xlog_blcksz: get_u32(b, 224),
        xlog_seg_size: get_u32(b, 228),
        nameDataLen: get_u32(b, 232),
        indexMaxKeys: get_u32(b, 236),
        toast_max_chunk_size: get_u32(b, 240),
        loblksize: get_u32(b, 244),
        float8ByVal: get_bool(b, 248),
        data_checksum_version: get_u32(b, 252),
        default_char_signedness: get_bool(b, 256),
        mock_authentication_nonce: nonce,
        crc: get_u32(b, 292),
    }
}

// ===========================================================================
// WriteControlFile / ReadControlFile / UpdateControlFile (xlog.c:4259 / 4368 /
// 4606).
//
// `ControlFile` is a backend-local image until XLOGShmemInit moves it into
// shared memory. Before shmem init (bootstrap), the image is a heap Box whose
// raw pointer is stored in the CONTROL_FILE cell; XLOGShmemInit reclaims it.
// ===========================================================================

/// Ensure a writable `ControlFile` image exists (a heap-Box stand-in for C's
/// `palloc`ed local image), returning a `&mut`.
pub(crate) fn control_file_mut<'a>() -> &'a mut ControlFileData {
    let cur = control_file_ptr();
    if cur.is_null() {
        let boxed = std::boxed::Box::new(ControlFileData::default());
        let raw = std::boxed::Box::into_raw(boxed);
        CONTROL_FILE.with(|c| c.set(raw));
        // SAFETY: just-created live Box.
        unsafe { &mut *raw }
    } else {
        // SAFETY: live image (Box or shmem).
        unsafe { &mut *cur }
    }
}

/// `WriteControlFile()` (xlog.c:4259) — fill the compatibility/version fields
/// of the in-memory `ControlFile`, compute its CRC, and write the
/// `PG_CONTROL_FILE_SIZE`-padded image to `global/pg_control`.
pub fn WriteControlFile() -> PgResult<()> {
    {
        let cf = control_file_mut();
        cf.pg_control_version = PG_CONTROL_VERSION;
        cf.catalog_version_no = CATALOG_VERSION_NO;
        cf.maxAlign = MAXIMUM_ALIGNOF;
        cf.floatFormat = FLOATFORMAT_VALUE;
        cf.blcksz = BLCKSZ;
        cf.relseg_size = RELSEG_SIZE;
        cf.xlog_blcksz = XLOG_BLCKSZ as u32;
        cf.xlog_seg_size = wal_segment_size() as u32;
        cf.nameDataLen = NAMEDATALEN;
        cf.indexMaxKeys = INDEX_MAX_KEYS;
        cf.toast_max_chunk_size = TOAST_MAX_CHUNK_SIZE;
        cf.loblksize = LOBLKSIZE;
        cf.float8ByVal = FLOAT8PASSBYVAL;
        cf.default_char_signedness = true;
    }

    // Serialize with CRC, then zero-pad to PG_CONTROL_FILE_SIZE.
    // SAFETY: image is live.
    let cf = unsafe { &*control_file_ptr() };
    let mut buffer = control_file_to_bytes(cf);
    buffer.resize(PG_CONTROL_FILE_SIZE, 0);

    fd::allocate_file_write::call(XLOG_CONTROL_FILE, &buffer)
}

/// `LocalProcessControlFile(reset)` (xlog.c:4908) — allocate the backend-local
/// `ControlFile` image and read `global/pg_control` into it. Called before shmem
/// exists (shmem sizing can depend on the control-file contents); `XLOGShmemInit`
/// later copies it into shared memory.
///
/// C asserts `reset || ControlFile == NULL`, then unconditionally `palloc`s a
/// fresh `ControlFileData` (in `reset` the old pointer is a dangling reference
/// into freed shmem) and calls `ReadControlFile()`. We mirror that by dropping
/// any prior backend-local image and re-allocating.
pub fn LocalProcessControlFile(reset: bool) -> PgResult<()> {
    debug_assert!(reset || control_file_ptr().is_null());
    // palloc a fresh image: drop any prior local Box and null the cell so
    // control_file_mut() (called inside ReadControlFile) allocates anew.
    let prior = control_file_ptr();
    if !prior.is_null() && !reset {
        // Non-reset path: C still palloc's a fresh struct, leaking the old one
        // into the (short-lived) startup context. Reclaim ours instead of leaking.
        // SAFETY: prior is a live Box created by control_file_mut.
        drop(unsafe { std::boxed::Box::from_raw(prior) });
    }
    CONTROL_FILE.with(|c| c.set(core::ptr::null_mut()));
    ReadControlFile()
}

/// `ReadControlFile()` (xlog.c:4368) — read `global/pg_control`, verify the
/// version + CRC + compatibility fields, and publish `wal_segment_size` and the
/// derived checkpoint-segments.
pub fn ReadControlFile() -> PgResult<()> {
    let bytes = match fd::allocate_file_read::call(XLOG_CONTROL_FILE)? {
        Some(b) => b,
        None => {
            return Err(PgError::error(std::format!(
                "could not open file \"{XLOG_CONTROL_FILE}\""
            )))
        }
    };
    if bytes.len() < SIZE_OF_CONTROL_FILE_DATA {
        return Err(PgError::error(std::format!(
            "could not read file \"{XLOG_CONTROL_FILE}\": read {} of {}",
            bytes.len(),
            SIZE_OF_CONTROL_FILE_DATA
        )));
    }

    let cf = control_file_from_bytes(&bytes);

    // Version check (precedes CRC, per the C ordering).
    if cf.pg_control_version != PG_CONTROL_VERSION {
        return Err(PgError::error(std::format!(
            "database files are incompatible with server: control file PG_CONTROL_VERSION {} but server {}",
            cf.pg_control_version, PG_CONTROL_VERSION
        )));
    }

    // CRC over offsetof(ControlFileData, crc).
    let crc = control_file_crc(&bytes[..offset_of_crc()]);
    if !EQ_CRC32C(crc, cf.crc) {
        return Err(PgError::error("incorrect checksum in control file"));
    }

    // Compatibility checks.
    macro_rules! check_eq {
        ($field:expr, $expected:expr, $name:literal) => {
            if $field != $expected {
                return Err(PgError::error(std::format!(
                    "database files are incompatible with server: {} {} but server {}",
                    $name, $field, $expected
                )));
            }
        };
    }
    check_eq!(cf.catalog_version_no, CATALOG_VERSION_NO, "CATALOG_VERSION_NO");
    check_eq!(cf.maxAlign, MAXIMUM_ALIGNOF, "MAXALIGN");
    if cf.floatFormat != FLOATFORMAT_VALUE {
        return Err(PgError::error(
            "database files are incompatible with server: float format mismatch",
        ));
    }
    check_eq!(cf.blcksz, BLCKSZ, "BLCKSZ");
    check_eq!(cf.relseg_size, RELSEG_SIZE, "RELSEG_SIZE");
    check_eq!(cf.xlog_blcksz, XLOG_BLCKSZ as u32, "XLOG_BLCKSZ");
    check_eq!(cf.nameDataLen, NAMEDATALEN, "NAMEDATALEN");
    check_eq!(cf.indexMaxKeys, INDEX_MAX_KEYS, "INDEX_MAX_KEYS");
    check_eq!(cf.toast_max_chunk_size, TOAST_MAX_CHUNK_SIZE, "TOAST_MAX_CHUNK_SIZE");
    check_eq!(cf.loblksize, LOBLKSIZE, "LOBLKSIZE");
    if cf.float8ByVal != FLOAT8PASSBYVAL {
        return Err(PgError::error(
            "database files are incompatible with server: USE_FLOAT8_BYVAL mismatch",
        ));
    }

    let seg = cf.xlog_seg_size as i32;
    if !IsValidWalSegSize(seg) {
        return Err(PgError::error(std::format!(
            "invalid WAL segment size in control file ({seg} bytes)"
        )));
    }
    WAL_SEGMENT_SIZE.with(|c| c.set(seg));

    // Publish the loaded image into the (backend-local) ControlFile cell.
    *control_file_mut() = cf;

    Ok(())
}

/// `UpdateControlFile()` (xlog.c:4606) — re-serialize the in-memory
/// `ControlFile` (recomputing the CRC) and write it durably to
/// `global/pg_control`.
pub fn UpdateControlFile() -> PgResult<()> {
    let cf = control_file_ptr();
    if cf.is_null() {
        return Err(PgError::error("UpdateControlFile: ControlFile not initialized"));
    }
    // SAFETY: live image.
    let cf = unsafe { &*cf };
    let mut buffer = control_file_to_bytes(cf);
    buffer.resize(PG_CONTROL_FILE_SIZE, 0);
    fd::allocate_file_write::call(XLOG_CONTROL_FILE, &buffer)
}

/// Recompute `UsableBytesInSegment` + `CalculateCheckpointSegments` after a
/// `wal_segment_size` change (the tail of `ReadControlFile`). Exposed for the
/// startup driver; pure arithmetic over the published `wal_segment_size`.
pub fn recompute_segment_derived(
    min_wal_size_mb: i32,
    max_wal_size_mb: i32,
    checkpoint_completion_target: f64,
) -> PgResult<i32> {
    let seg = wal_segment_size();
    if ConvertToXSegs(min_wal_size_mb, seg) < 2 {
        return Err(PgError::error(
            "\"min_wal_size\" must be at least twice \"wal_segment_size\"",
        ));
    }
    if ConvertToXSegs(max_wal_size_mb, seg) < 2 {
        return Err(PgError::error(
            "\"max_wal_size\" must be at least twice \"wal_segment_size\"",
        ));
    }
    let _ = UsableBytesInPage();
    Ok(CalculateCheckpointSegments(
        max_wal_size_mb,
        seg,
        checkpoint_completion_target,
    ))
}

// ===========================================================================
// No-arg seam wrappers (the `xlog_shmem_size` / `xlog_shmem_init` seam
// contract takes no args; the C reads the resolved XLOGbuffers GUC global).
// ===========================================================================

/// `XLOGShmemSize()` seam wrapper — reads the resolved `XLOGbuffers` GUC global.
pub fn xlog_shmem_size_seam() -> PgResult<types_core::Size> {
    XLOGShmemSize(xlog_buffers())
}

/// `XLOGShmemInit()` seam wrapper — reads the resolved `XLOGbuffers` GUC global.
pub fn xlog_shmem_init_seam() -> PgResult<()> {
    XLOGShmemInit(xlog_buffers())
}

// Compile-time sanity: the codec offsets line up with the alignment of the
// fields they precede.
const _: () = {
    assert!(align_of::<XLogCtlData>() <= PG_CACHE_LINE_SIZE);
    assert!(SIZE_OF_XLOG_LONG_PHD > SIZE_OF_XLOG_SHORT_PHD);
};

#[cfg(test)]
#[path = "shmem_tests.rs"]
mod shmem_tests;
