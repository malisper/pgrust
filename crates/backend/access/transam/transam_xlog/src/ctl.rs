use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::OnceLock;

use lwlock::{LWLock, LWLockInitialize};
use types_core::{TimeLineID, XLogRecPtr};

use crate::{CheckPoint, RecoveryState, InvalidXLogRecPtr, NUM_XLOGINSERT_LOCKS, RECOVERY_STATE_CRASH, XLOG_BLCKSZ};

// s_lock.h shape: TAS spin with exponential backoff elided (uncontended in
// the M1 single-backend profile; contention shape is an M4 lever).
pub struct SpinLock(AtomicBool);

impl SpinLock {
    pub const fn new() -> Self {
        SpinLock(AtomicBool::new(false))
    }
    #[inline]
    pub fn acquire(&self) {
        while self.0.swap(true, Ordering::Acquire) {
            std::hint::spin_loop();
        }
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

pub fn GetFakeLSNForUnloggedRel() -> XLogRecPtr {
    XLogCtl().unloggedLSN.fetch_add(1, Ordering::SeqCst)
}
