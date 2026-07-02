use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use init_small::globals;
use lwlock::{LWLock, LWLockPadded};
use types_core::{Buffer, ForkNumber, InvalidBlockNumber, InvalidOid, BLCKSZ, INVALID_PROC_NUMBER};
use types_error::{PgResult, ERRCODE_OUT_OF_MEMORY, ERROR};
use types_storage::buf::{buftag, PgAioWaitRef, BM_LOCKED, FREENEXT_END_OF_LIST};
use types_storage::LWTRANCHE_BUFFER_CONTENT;

pub const BUFFERDESC_PAD_TO_SIZE: usize = 64;
pub const PG_IO_ALIGN_SIZE: usize = 4096;

#[repr(C)]
pub struct BufferDesc {
    tag: UnsafeCell<buftag>,
    pub buf_id: i32,
    pub state: AtomicU32,
    wait_backend_pgprocno: UnsafeCell<i32>,
    free_next: UnsafeCell<i32>,
    io_wref: UnsafeCell<PgAioWaitRef>,
    pub content_lock: LWLock,
}

// SAFETY: C's buf_internals.h concurrency contract — `tag`, `wait_backend_pgprocno`,
// `io_wref` are written only while BM_LOCKED is held in `state` (and `tag` is
// stable while any pin is held); `free_next` is written only under the strategy
// spinlock; `state` is atomic; `content_lock` is Sync.
unsafe impl Sync for BufferDesc {}
unsafe impl Send for BufferDesc {}

impl BufferDesc {
    fn initial(buf_id: i32, free_next: i32) -> BufferDesc {
        BufferDesc {
            tag: UnsafeCell::new(cleared_buftag()),
            buf_id,
            state: AtomicU32::new(0),
            wait_backend_pgprocno: UnsafeCell::new(INVALID_PROC_NUMBER),
            free_next: UnsafeCell::new(free_next),
            io_wref: UnsafeCell::new(PgAioWaitRef::default()),
            content_lock: LWLockPadded::new_unlocked(LWTRANCHE_BUFFER_CONTENT).lock,
        }
    }

    /// Caller holds a pin or the header lock (tag writers hold the header lock
    /// with refcount 0, so pinned readers race nothing).
    #[inline]
    pub fn tag(&self) -> buftag {
        // SAFETY: caller contract above.
        unsafe { *self.tag.get() }
    }

    /// # Safety
    /// Header lock held, no other pins (BufferAlloc/Invalidate sites only).
    #[inline]
    pub(crate) unsafe fn set_tag(&self, tag: buftag) {
        *self.tag.get() = tag;
    }

    #[inline]
    pub(crate) fn wait_backend_pgprocno(&self) -> i32 {
        // SAFETY: read under header lock at all call sites.
        unsafe { *self.wait_backend_pgprocno.get() }
    }

    /// # Safety
    /// Header lock held.
    #[inline]
    pub(crate) unsafe fn set_wait_backend_pgprocno(&self, procno: i32) {
        *self.wait_backend_pgprocno.get() = procno;
    }

    #[inline]
    pub(crate) fn free_next(&self) -> i32 {
        // SAFETY: read under the strategy spinlock at all call sites.
        unsafe { *self.free_next.get() }
    }

    /// # Safety
    /// Strategy spinlock held.
    #[inline]
    pub(crate) unsafe fn set_free_next(&self, next: i32) {
        *self.free_next.get() = next;
    }
}

#[repr(C, align(64))]
struct BufferDescPadded {
    desc: BufferDesc,
}

const _: () = assert!(core::mem::size_of::<BufferDesc>() <= BUFFERDESC_PAD_TO_SIZE);
const _: () = assert!(core::mem::size_of::<BufferDescPadded>() == BUFFERDESC_PAD_TO_SIZE);

pub fn cleared_buftag() -> buftag {
    buftag {
        spcOid: InvalidOid,
        dbOid: InvalidOid,
        relNumber: 0,
        forkNum: ForkNumber::InvalidForkNumber,
        blockNum: InvalidBlockNumber,
    }
}

struct BufferPool {
    descs: *const BufferDescPadded,
    blocks: *mut u8,
    nbuffers: i32,
}

// SAFETY: `descs`/`blocks` are leaked process-lifetime allocations (C's shmem
// carve); all mutation goes through BufferDesc's contract or pinned pages.
unsafe impl Sync for BufferPool {}
unsafe impl Send for BufferPool {}

static POOL: OnceLock<BufferPool> = OnceLock::new();

#[inline]
fn pool() -> &'static BufferPool {
    POOL.get()
        .expect("bufmgr: BufferManagerShmemInit (buf_init.c) not called")
}

pub fn buffer_pool_initialized() -> bool {
    POOL.get().is_some()
}

#[inline]
pub fn NBuffersInited() -> i32 {
    pool().nbuffers
}

#[inline]
pub fn GetBufferDescriptor(id: i32) -> &'static BufferDesc {
    let p = pool();
    assert!((0..p.nbuffers).contains(&id), "bad buffer id: {id}");
    // SAFETY: in-bounds (asserted); descriptors live for the process lifetime.
    unsafe { &(*p.descs.add(id as usize)).desc }
}

#[inline]
pub fn BufferDescriptorGetBuffer(desc: &BufferDesc) -> Buffer {
    desc.buf_id + 1
}

#[inline]
pub fn BufferGetBlockPtr(buffer: Buffer) -> *mut u8 {
    let p = pool();
    assert!(buffer > 0 && buffer <= p.nbuffers, "bad buffer ID: {buffer}");
    // SAFETY: in-bounds (asserted).
    unsafe { p.blocks.add((buffer as usize - 1) * BLCKSZ) }
}

/// BufferManagerShmemInit (buf_init.c) minus localbuf/checkpoint carve-outs:
/// descriptor array (64B padded per BUFFERDESC_PAD_TO_SIZE), IO-aligned block
/// array, freelist chain, then StrategyInitialize + InitBufTable.
pub fn BufferManagerShmemInit() -> PgResult<()> {
    let n = globals::NBuffers();
    assert!(n > 0, "NBuffers not set");
    let nu = n as usize;

    let desc_layout =
        core::alloc::Layout::array::<BufferDescPadded>(nu).expect("buffer descriptor layout");
    // SAFETY: non-zero layout; initialized element-by-element below before publish.
    let descs = unsafe { std::alloc::alloc(desc_layout) } as *mut BufferDescPadded;
    let blk_layout = core::alloc::Layout::from_size_align(nu * BLCKSZ, PG_IO_ALIGN_SIZE)
        .expect("buffer block layout");
    // SAFETY: non-zero layout; zeroed like a fresh shmem segment.
    let blocks = unsafe { std::alloc::alloc_zeroed(blk_layout) };
    if descs.is_null() || blocks.is_null() {
        return Err(Box::new(
            types_error::PgError::new(ERROR, "out of memory".into())
                .with_sqlstate(ERRCODE_OUT_OF_MEMORY),
        ));
    }
    for i in 0..nu {
        let free_next = if i + 1 < nu {
            (i + 1) as i32
        } else {
            FREENEXT_END_OF_LIST
        };
        // SAFETY: in-bounds write into the fresh allocation.
        unsafe {
            core::ptr::write(
                descs.add(i),
                BufferDescPadded {
                    desc: BufferDesc::initial(i as i32, free_next),
                },
            );
        }
    }
    POOL.set(BufferPool {
        descs,
        blocks,
        nbuffers: n,
    })
    .unwrap_or_else(|_| panic!("bufmgr: buffer pool initialized twice"));

    crate::freelist::StrategyInitialize(n)?;
    Ok(())
}

pub fn LockBufHdr(desc: &BufferDesc) -> u32 {
    let mut spins = 0u32;
    loop {
        let old = desc.state.fetch_or(BM_LOCKED, Ordering::Acquire);
        if old & BM_LOCKED == 0 {
            return old | BM_LOCKED;
        }
        spin_delay(&mut spins);
    }
}

#[inline]
pub fn UnlockBufHdr(desc: &BufferDesc, buf_state: u32) {
    desc.state.store(buf_state & !BM_LOCKED, Ordering::Release);
}

pub(crate) fn WaitBufHdrUnlocked(desc: &BufferDesc) -> u32 {
    let mut spins = 0u32;
    loop {
        let state = desc.state.load(Ordering::Acquire);
        if state & BM_LOCKED == 0 {
            return state;
        }
        spin_delay(&mut spins);
    }
}

// C escalates to a random usleep via s_lock.c; bounded ISB spin + yield keeps
// the uncontended path identical and the contended path OS-fair.
#[inline]
fn spin_delay(spins: &mut u32) {
    *spins += 1;
    if *spins < 1024 {
        core::hint::spin_loop();
    } else {
        std::thread::yield_now();
    }
}
