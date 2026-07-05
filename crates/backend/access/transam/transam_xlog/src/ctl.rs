use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::OnceLock;

use lwlock::{LWLock, LWLockInitialize};
use types_core::{TimeLineID, XLogRecPtr};

use crate::{CheckPoint, RecoveryState, InvalidXLogRecPtr, NUM_XLOGINSERT_LOCKS, RECOVERY_STATE_CRASH, XLOG_BLCKSZ};

// s_lock.h shape: single TAS on the uncontended path; contended acquires go
// through C's perform_spin_delay backoff (unbounded busy-spin collapsed the
// multi-client write gate when clients > vCPU: swp1_acq dominated cycles,
// job pgrust-m4mc-gate-1783128775-95788).
pub struct SpinLock(AtomicBool);

impl SpinLock {
    pub const fn new() -> Self {
        SpinLock(AtomicBool::new(false))
    }
    #[inline]
    pub fn acquire(&self) {
        if self.0.swap(true, Ordering::Acquire) {
            self.acquire_contended();
        }
    }
    #[cold]
    #[inline(never)]
    fn acquire_contended(&self) {
        let mut delay = s_lock_seams::SpinDelayStatus::new(file!(), line!() as i32, "XLogCtl");
        loop {
            if !self.0.load(Ordering::Relaxed) && !self.0.swap(true, Ordering::Acquire) {
                break;
            }
            s_lock_seams::perform_spin_delay::call(&mut delay);
        }
        s_lock_seams::finish_spin_delay::call(&delay);
    }
    #[inline]
    pub fn release(&self) {
        self.0.store(false, Ordering::Release);
    }
    #[inline]
    pub fn with<R>(&self, f: impl FnOnce() -> R) -> R {
        self.acquire();
        let r = f();
        self.release();
        r
    }
}

const LWTRANCHE_WAL_INSERT: i32 = lwlock::LWTRANCHE_XACT_BUFFER + 7;

#[repr(C, align(128))]
pub struct WALInsertLockPadded {
    pub lock: LWLock,
    pub insertingAt: AtomicU64,
    pub lastImportantAt: AtomicU64,
}

pub struct XLogCtlInsert {
    pub insertpos_lck: SpinLock,
    // Protected by insertpos_lck (plain values behind the spinlock).
    pub CurrBytePos: AtomicU64,
    pub PrevBytePos: AtomicU64,
    // Read under any insertion lock; written holding all of them.
    pub RedoRecPtr: AtomicU64,
    pub fullPageWrites: AtomicBool,
    pub runningBackups: AtomicI32,
    pub lastBackupStart: AtomicU64,
    pub WALInsertLocks: [WALInsertLockPadded; NUM_XLOGINSERT_LOCKS],
}

pub struct XLogCtlData {
    pub Insert: XLogCtlInsert,

    pub info_lck: SpinLock,
    // Protected by info_lck:
    pub LogwrtRqstWrite: AtomicU64,
    pub LogwrtRqstFlush: AtomicU64,
    pub RedoRecPtr: AtomicU64,
    pub ckptFullXid: AtomicU64,
    pub asyncXactLSN: AtomicU64,
    pub replicationSlotMinLSN: AtomicU64,
    pub lastCheckPointRecPtr: AtomicU64,
    pub lastCheckPointEndPtr: AtomicU64,
    pub lastCheckPoint: UnsafeCell<CheckPoint>,
    pub lastFpwDisableRecPtr: AtomicU64,
    pub InsertTimeLineID: AtomicU32,
    pub PrevTimeLineID: AtomicU32,
    pub SharedRecoveryState: AtomicI32,
    pub WalWriterSleeping: AtomicBool,

    pub lastRemovedSegNo: AtomicU64,
    pub unloggedLSN: AtomicU64,

    // Protected by WALWriteLock:
    pub lastSegSwitchTime: AtomicI64,
    pub lastSegSwitchLSN: AtomicU64,

    pub logInsertResult: AtomicU64,
    pub logWriteResult: AtomicU64,
    pub logFlushResult: AtomicU64,

    // Protected by WALBufMappingLock:
    pub InitializedUpTo: AtomicU64,

    pub xlblocks: Box<[AtomicU64]>,
    pages: *mut u8,
    pub XLogCacheBlck: i32,

    // Protected by ControlFileLock:
    pub InstallXLogFileSegmentActive: AtomicBool,
}

// SAFETY: every field is atomic or protocol-guarded exactly as in C's shmem
// image: `lastCheckPoint` only under info_lck, `pages` bytes only by the
// owner of the corresponding reserved WAL range / WALBufMappingLock (see
// GetXLogBuffer / AdvanceXLInsertBuffer).
unsafe impl Sync for XLogCtlData {}
unsafe impl Send for XLogCtlData {}

impl XLogCtlData {
    #[inline]
    pub fn page_ptr(&self, idx: usize) -> *mut u8 {
        debug_assert!(idx <= self.XLogCacheBlck as usize);
        // SAFETY: idx bounded by XLogCacheBlck; buffer is XLOG_BLCKSZ*(blck+1).
        unsafe { self.pages.add(idx * XLOG_BLCKSZ) }
    }
}

static XLOG_CTL: OnceLock<&'static XLogCtlData> = OnceLock::new();

pub fn XLogCtl() -> &'static XLogCtlData {
    XLOG_CTL.get().unwrap_or_else(|| panic!("XLOGShmemInit has not run"))
}

pub fn xlog_ctl_initialized() -> bool {
    XLOG_CTL.get().is_some()
}

pub const WAL_BUF_MAPPING_LOCK: usize = 7;
pub const WAL_WRITE_LOCK: usize = 8;
pub const CONTROL_FILE_LOCK: usize = 9;

pub fn WALBufMappingLock() -> &'static LWLock {
    lwlock::main_lock(WAL_BUF_MAPPING_LOCK)
}
pub fn WALWriteLock() -> &'static LWLock {
    lwlock::main_lock(WAL_WRITE_LOCK)
}
pub fn ControlFileLock() -> &'static LWLock {
    lwlock::main_lock(CONTROL_FILE_LOCK)
}

pub fn XLOGShmemSize() -> usize {
    let xlog_buffers = crate::ctl::xlog_buffers();
    std::mem::size_of::<XLogCtlData>()
        + std::mem::size_of::<WALInsertLockPadded>() * (NUM_XLOGINSERT_LOCKS + 1)
        + std::mem::size_of::<AtomicU64>() * xlog_buffers as usize
        + XLOG_BLCKSZ
        + XLOG_BLCKSZ * xlog_buffers as usize
}

pub(crate) fn xlog_buffers() -> i32 {
    let mut n = guc_tables::vars::XLOGbuffers.read();
    if n == -1 {
        n = crate::XLOGChooseNumBuffers();
        guc_tables::vars::XLOGbuffers.write(n);
    }
    debug_assert!(n > 0);
    n
}

pub fn XLOGShmemInit() {
    if XLOG_CTL.get().is_some() {
        return;
    }
    let xlog_buffers = xlog_buffers();

    let mut xlblocks = Vec::with_capacity(xlog_buffers as usize);
    for _ in 0..xlog_buffers {
        xlblocks.push(AtomicU64::new(InvalidXLogRecPtr));
    }

    let layout =
        std::alloc::Layout::from_size_align(XLOG_BLCKSZ * xlog_buffers as usize, XLOG_BLCKSZ)
            .expect("xlog buffer layout");
    // SAFETY: non-zero size; zeroed + leaked for the cluster lifetime, as C's
    // shmem segment is.
    let pages = unsafe { std::alloc::alloc_zeroed(layout) };
    assert!(!pages.is_null(), "out of memory allocating WAL buffers");

    let make_insert_lock = || {
        let mut lock = LWLock {
            tranche: 0,
            state: AtomicU32::new(0),
            waiters: UnsafeCell::new(Default::default()),
        };
        LWLockInitialize(&mut lock, LWTRANCHE_WAL_INSERT);
        WALInsertLockPadded {
            lock,
            insertingAt: AtomicU64::new(InvalidXLogRecPtr),
            lastImportantAt: AtomicU64::new(InvalidXLogRecPtr),
        }
    };

    let ctl: &'static XLogCtlData = Box::leak(Box::new(XLogCtlData {
        Insert: XLogCtlInsert {
            insertpos_lck: SpinLock::new(),
            CurrBytePos: AtomicU64::new(0),
            PrevBytePos: AtomicU64::new(0),
            RedoRecPtr: AtomicU64::new(InvalidXLogRecPtr),
            fullPageWrites: AtomicBool::new(false),
            runningBackups: AtomicI32::new(0),
            lastBackupStart: AtomicU64::new(InvalidXLogRecPtr),
            WALInsertLocks: std::array::from_fn(|_| make_insert_lock()),
        },
        info_lck: SpinLock::new(),
        LogwrtRqstWrite: AtomicU64::new(0),
        LogwrtRqstFlush: AtomicU64::new(0),
        RedoRecPtr: AtomicU64::new(InvalidXLogRecPtr),
        ckptFullXid: AtomicU64::new(0),
        asyncXactLSN: AtomicU64::new(InvalidXLogRecPtr),
        replicationSlotMinLSN: AtomicU64::new(InvalidXLogRecPtr),
        lastCheckPointRecPtr: AtomicU64::new(InvalidXLogRecPtr),
        lastCheckPointEndPtr: AtomicU64::new(InvalidXLogRecPtr),
        lastCheckPoint: UnsafeCell::new(CheckPoint::ZEROED),
        lastFpwDisableRecPtr: AtomicU64::new(InvalidXLogRecPtr),
        InsertTimeLineID: AtomicU32::new(0),
        PrevTimeLineID: AtomicU32::new(0),
        SharedRecoveryState: AtomicI32::new(RECOVERY_STATE_CRASH),
        WalWriterSleeping: AtomicBool::new(false),
        lastRemovedSegNo: AtomicU64::new(0),
        unloggedLSN: AtomicU64::new(InvalidXLogRecPtr),
        lastSegSwitchTime: AtomicI64::new(0),
        lastSegSwitchLSN: AtomicU64::new(InvalidXLogRecPtr),
        logInsertResult: AtomicU64::new(InvalidXLogRecPtr),
        logWriteResult: AtomicU64::new(InvalidXLogRecPtr),
        logFlushResult: AtomicU64::new(InvalidXLogRecPtr),
        InitializedUpTo: AtomicU64::new(InvalidXLogRecPtr),
        xlblocks: xlblocks.into_boxed_slice(),
        pages,
        XLogCacheBlck: xlog_buffers - 1,
        InstallXLogFileSegmentActive: AtomicBool::new(false),
    }));

    XLOG_CTL.set(ctl).unwrap_or_else(|_| panic!("XLOGShmemInit raced"));
}

fn reset_lwlock_in_place(lock: &LWLock) {
    lock.state.store(lwlock::LW_FLAG_RELEASE_OK, Ordering::Relaxed);
    // SAFETY: crash choreography drained every child before reset; the
    // postmaster thread has exclusive access, so no holder or waiter exists.
    unsafe {
        *lock.waiters.get() = lmgr_proc_seams::proclist_head {
            head: types_core::INVALID_PROC_NUMBER,
            tail: types_core::INVALID_PROC_NUMBER,
        };
    }
}

/// Crash-cycle reset in place to the post-XLOGShmemInit boot image
/// (notes/crash-restart-design.md); StartupXLOG re-seeds from pg_control/WAL.
pub fn XLOGShmemResetAfterCrash() {
    let ctl = XLogCtl();
    assert_eq!(ctl.XLogCacheBlck, xlog_buffers() - 1);

    let ins = &ctl.Insert;
    ins.insertpos_lck.0.store(false, Ordering::Relaxed);
    ins.CurrBytePos.store(0, Ordering::Relaxed);
    ins.PrevBytePos.store(0, Ordering::Relaxed);
    ins.RedoRecPtr.store(InvalidXLogRecPtr, Ordering::Relaxed);
    ins.fullPageWrites.store(false, Ordering::Relaxed);
    ins.runningBackups.store(0, Ordering::Relaxed);
    ins.lastBackupStart.store(InvalidXLogRecPtr, Ordering::Relaxed);
    for l in &ins.WALInsertLocks {
        reset_lwlock_in_place(&l.lock);
        l.insertingAt.store(InvalidXLogRecPtr, Ordering::Relaxed);
        l.lastImportantAt.store(InvalidXLogRecPtr, Ordering::Relaxed);
    }

    ctl.info_lck.0.store(false, Ordering::Relaxed);
    ctl.LogwrtRqstWrite.store(0, Ordering::Relaxed);
    ctl.LogwrtRqstFlush.store(0, Ordering::Relaxed);
    ctl.RedoRecPtr.store(InvalidXLogRecPtr, Ordering::Relaxed);
    ctl.ckptFullXid.store(0, Ordering::Relaxed);
    ctl.asyncXactLSN.store(InvalidXLogRecPtr, Ordering::Relaxed);
    ctl.replicationSlotMinLSN.store(InvalidXLogRecPtr, Ordering::Relaxed);
    ctl.lastCheckPointRecPtr.store(InvalidXLogRecPtr, Ordering::Relaxed);
    ctl.lastCheckPointEndPtr.store(InvalidXLogRecPtr, Ordering::Relaxed);
    // SAFETY: exclusive access as above (C protocol: info_lck).
    unsafe { *ctl.lastCheckPoint.get() = CheckPoint::ZEROED };
    ctl.lastFpwDisableRecPtr.store(InvalidXLogRecPtr, Ordering::Relaxed);
    ctl.InsertTimeLineID.store(0, Ordering::Relaxed);
    ctl.PrevTimeLineID.store(0, Ordering::Relaxed);
    ctl.SharedRecoveryState.store(RECOVERY_STATE_CRASH, Ordering::Relaxed);
    ctl.WalWriterSleeping.store(false, Ordering::Relaxed);
    ctl.lastRemovedSegNo.store(0, Ordering::Relaxed);
    ctl.unloggedLSN.store(InvalidXLogRecPtr, Ordering::Relaxed);
    ctl.lastSegSwitchTime.store(0, Ordering::Relaxed);
    ctl.lastSegSwitchLSN.store(InvalidXLogRecPtr, Ordering::Relaxed);
    ctl.logInsertResult.store(InvalidXLogRecPtr, Ordering::Relaxed);
    ctl.logWriteResult.store(InvalidXLogRecPtr, Ordering::Relaxed);
    ctl.logFlushResult.store(InvalidXLogRecPtr, Ordering::Relaxed);
    ctl.InitializedUpTo.store(InvalidXLogRecPtr, Ordering::Relaxed);
    for b in ctl.xlblocks.iter() {
        b.store(InvalidXLogRecPtr, Ordering::Relaxed);
    }
    // SAFETY: exclusive access as above; the allocation spans
    // XLOG_BLCKSZ * (XLogCacheBlck + 1) bytes.
    unsafe {
        std::ptr::write_bytes(ctl.pages, 0, XLOG_BLCKSZ * (ctl.XLogCacheBlck as usize + 1));
    }
    ctl.InstallXLogFileSegmentActive.store(false, Ordering::Relaxed);
}

pub fn NextBufIdx(idx: i32) -> i32 {
    if idx == XLogCtl().XLogCacheBlck {
        0
    } else {
        idx + 1
    }
}

pub fn XLogRecPtrToBufIdx(recptr: XLogRecPtr) -> i32 {
    ((recptr / XLOG_BLCKSZ as u64) % (XLogCtl().XLogCacheBlck as u64 + 1)) as i32
}

pub fn GetRecoveryState() -> RecoveryState {
    let ctl = XLogCtl();
    ctl.info_lck.with(|| ctl.SharedRecoveryState.load(Ordering::Relaxed))
}

pub fn GetWALInsertionTimeLine() -> TimeLineID {
    debug_assert_eq!(
        XLogCtl().SharedRecoveryState.load(Ordering::Relaxed),
        crate::RECOVERY_STATE_DONE
    );
    XLogCtl().InsertTimeLineID.load(Ordering::Relaxed)
}

pub fn GetWALInsertionTimeLineIfSet() -> TimeLineID {
    let ctl = XLogCtl();
    ctl.info_lck.with(|| ctl.InsertTimeLineID.load(Ordering::Relaxed))
}

pub fn GetFakeLSNForUnloggedRel() -> XLogRecPtr {
    XLogCtl().unloggedLSN.fetch_add(1, Ordering::SeqCst)
}
