use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicU32, Ordering};

use condition_variable::ConditionVariable;
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
    pub(crate) fn initial(buf_id: i32, free_next: i32) -> BufferDesc {
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

    /// Caller holds a pin or the header lock (writers require refcount 0).
    #[inline]
    pub fn tag(&self) -> buftag {
        // SAFETY: caller contract above.
        unsafe { *self.tag.get() }
    }

    /// Racy, unlocked snapshot of the tag for whole-pool sweep prechecks
    /// (DropRelationBuffers etc.), matching bufmgr.c's unlocked `&bufHdr->tag`
    /// precheck before `LockBufHdr`. The authoritative test is always re-done
    /// under the header lock; a torn or stale value here only costs (or skips)
    /// one recheck and never decides anything.
    ///
    /// # Safety / concurrency
    /// `read_volatile` of a `Copy` POD reads the bytes without ever forming a
    /// `&buftag` reference over the `UnsafeCell`, so it does not conflict with
    /// `set_tag`'s write in the C11/Rust reference model the way the plain
    /// `tag()` accessor (which materializes `&buftag`) would. The read may be
    /// torn or stale; that is the TOLERATED racy precheck, exactly as in C.
    #[inline]
    pub fn tag_racy_snapshot(&self) -> buftag {
        // SAFETY: `tag` points at a live, aligned `buftag` for the process
        // lifetime; volatile POD read, tearing/staleness tolerated per above.
        unsafe { core::ptr::read_volatile(self.tag.get()) }
    }

    /// # Safety: header lock held, no other pins.
    #[inline]
    pub(crate) unsafe fn set_tag(&self, tag: buftag) {
        *self.tag.get() = tag;
    }

    #[inline]
    pub(crate) fn wait_backend_pgprocno(&self) -> i32 {
        // SAFETY: read under header lock at all call sites.
        unsafe { *self.wait_backend_pgprocno.get() }
    }

    /// # Safety: header lock held.
    #[inline]
    pub(crate) unsafe fn set_wait_backend_pgprocno(&self, procno: i32) {
        *self.wait_backend_pgprocno.get() = procno;
    }

    #[inline]
    pub(crate) fn free_next(&self) -> i32 {
        // SAFETY: read under the strategy spinlock at all call sites.
        unsafe { *self.free_next.get() }
    }

    /// # Safety: strategy spinlock held.
    #[inline]
    pub(crate) unsafe fn set_free_next(&self, next: i32) {
        *self.free_next.get() = next;
    }

    /// Read under the header lock; armed == a uring read is (or was) in flight.
    // io_uring read lane helper; wiring lands with the uring completion arms.
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn io_wref_armed(&self) -> bool {
        // SAFETY: header lock held per contract above.
        let w = unsafe { &*self.io_wref.get() };
        w.aio_index != 0 || w.generation_upper != 0 || w.generation_lower != 0
    }

    /// Read under the header lock.
    #[inline]
    pub(crate) fn io_wref(&self) -> PgAioWaitRef {
        // SAFETY: header lock held per contract above.
        unsafe { *self.io_wref.get() }
    }

    /// # Safety: header lock held.
    #[inline]
    pub(crate) unsafe fn set_io_wref(&self, w: PgAioWaitRef) {
        *self.io_wref.get() = w;
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

// C's BufferDescriptors/BufferBlocks/NBuffers globals: published once before
// any backend touches the pool (thread spawn synchronizes), then read with
// plain loads — an OnceLock re-check would put an acquire load on every hit.
struct BufferPool {
    descs: core::sync::atomic::AtomicPtr<BufferDescPadded>,
    blocks: core::sync::atomic::AtomicPtr<u8>,
    io_cvs: core::sync::atomic::AtomicPtr<ConditionVariable>,
    nbuffers: core::sync::atomic::AtomicI32,
}

static POOL: BufferPool = BufferPool {
    descs: core::sync::atomic::AtomicPtr::new(core::ptr::null_mut()),
    blocks: core::sync::atomic::AtomicPtr::new(core::ptr::null_mut()),
    io_cvs: core::sync::atomic::AtomicPtr::new(core::ptr::null_mut()),
    nbuffers: core::sync::atomic::AtomicI32::new(0),
};

#[cold]
#[inline(never)]
fn pool_uninit() -> ! {
    panic!("bufmgr: BufferManagerShmemInit (buf_init.c) not called")
}

#[inline]
fn pool_descs() -> *const BufferDescPadded {
    let p = POOL.descs.load(Ordering::Relaxed);
    if p.is_null() {
        pool_uninit();
    }
    p
}

// Pool-state probe kept for boot/shutdown diagnostics; no consumer wired yet.
#[allow(dead_code)]
pub fn buffer_pool_initialized() -> bool {
    !POOL.descs.load(Ordering::Relaxed).is_null()
}

#[inline]
pub fn NBuffersInited() -> i32 {
    POOL.nbuffers.load(Ordering::Relaxed)
}

#[inline]
pub fn GetBufferDescriptor(id: i32) -> &'static BufferDesc {
    let descs = pool_descs();
    debug_assert!((0..NBuffersInited()).contains(&id), "bad buffer id: {id}");
    // SAFETY: in-bounds (caller-checked per C, debug-asserted); descriptors
    // live for the process lifetime.
    unsafe { &(*descs.add(id as usize)).desc }
}

#[inline]
pub fn BufferDescriptorGetBuffer(desc: &BufferDesc) -> Buffer {
    desc.buf_id + 1
}

/// C's BufferIOCVArray (buf_init.c): one CV per buffer, off the padded desc.
#[inline]
pub(crate) fn BufferDescriptorGetIOCV(desc: &BufferDesc) -> &'static ConditionVariable {
    let cvs = POOL.io_cvs.load(Ordering::Relaxed);
    debug_assert!(!cvs.is_null());
    // SAFETY: published before `descs` (the Release-ordered flag); in-bounds.
    unsafe { &*cvs.add(desc.buf_id as usize) }
}

#[inline]
pub fn BufferGetBlockPtr(buffer: Buffer) -> *mut u8 {
    let blocks = POOL.blocks.load(Ordering::Relaxed);
    debug_assert!(
        !blocks.is_null() && buffer > 0 && buffer <= NBuffersInited(),
        "bad buffer ID: {buffer}"
    );
    // SAFETY: in-bounds (caller-checked per C, debug-asserted).
    unsafe { blocks.add((buffer as usize - 1) * BLCKSZ) }
}

// C sizes for the BufferManagerShmemSize terms (LP64; the pgrust structs
// are not byte-identical to C's, so the C shmem estimate is spelled with C's
// numbers): sizeof(BufferDescPadded) = BUFFERDESC_PAD_TO_SIZE (64,
// buf_internals.h:293), PG_CACHE_LINE_SIZE (128, pg_config_manual.h:212),
// sizeof(ConditionVariableMinimallyPadded) = CV_MINIMAL_SIZE (16,
// condition_variable.h:38), sizeof(CkptSortItem) = 5 * 4 (buf_internals.h).
const C_PG_CACHE_LINE_SIZE: usize = 128;
const C_SIZEOF_CONDITION_VARIABLE_MINIMALLY_PADDED: usize = 16;
const C_SIZEOF_CKPT_SORT_ITEM: usize = 20;

/// BufferManagerShmemSize (buf_init.c:145): the shared-memory estimate for
/// the buffer pool — descriptors, blocks, freelist.c's structures, the I/O
/// condition variables and bufmgr.c's checkpoint sort array.
pub fn BufferManagerShmemSize() -> PgResult<usize> {
    let nbuffers = globals::NBuffers() as usize;
    let mut size: usize = 0;
    // size of buffer descriptors, plus alignment padding
    size = mcx::add_size(size, mcx::mul_size(nbuffers, BUFFERDESC_PAD_TO_SIZE)?)?;
    size = mcx::add_size(size, C_PG_CACHE_LINE_SIZE)?;
    // size of data pages, plus alignment padding
    size = mcx::add_size(size, PG_IO_ALIGN_SIZE)?;
    size = mcx::add_size(size, mcx::mul_size(nbuffers, BLCKSZ)?)?;
    // size of stuff controlled by freelist.c
    size = mcx::add_size(size, crate::freelist::StrategyShmemSize()?)?;
    // size of I/O condition variables, plus alignment padding
    size = mcx::add_size(
        size,
        mcx::mul_size(nbuffers, C_SIZEOF_CONDITION_VARIABLE_MINIMALLY_PADDED)?,
    )?;
    size = mcx::add_size(size, C_PG_CACHE_LINE_SIZE)?;
    // size of checkpoint sort array in bufmgr.c
    size = mcx::add_size(size, mcx::mul_size(nbuffers, C_SIZEOF_CKPT_SORT_ITEM)?)?;
    Ok(size)
}

/// BufferManagerShmemInit (buf_init.c) minus localbuf/checkpoint carve-outs.
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
    let cv_layout = core::alloc::Layout::array::<ConditionVariable>(nu).expect("buffer IO CV layout");
    // SAFETY: non-zero layout; initialized element-by-element below before publish.
    let io_cvs = unsafe { std::alloc::alloc(cv_layout) } as *mut ConditionVariable;
    if descs.is_null() || blocks.is_null() || io_cvs.is_null() {
        return Err(Box::new(
            types_error::PgError::new(ERROR, "out of memory")
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
            core::ptr::write(io_cvs.add(i), ConditionVariable::new());
        }
    }
    POOL.blocks.store(blocks, Ordering::Relaxed);
    POOL.io_cvs.store(io_cvs, Ordering::Relaxed);
    POOL.nbuffers.store(n, Ordering::Relaxed);
    // C's extern BufferBlocks: lets BufferGetPage stay a header inline in
    // pin-holding consumers (bufmgr_seams::BufferPin::page()).
    bufmgr_seams::publish_buffer_blocks(blocks);
    // `descs` is the publish flag: stored last, Release orders the fields
    // (and the initialized array) before it.
    assert!(
        POOL.descs
            .compare_exchange(
                core::ptr::null_mut(),
                descs,
                Ordering::Release,
                Ordering::Relaxed
            )
            .is_ok(),
        "bufmgr: buffer pool initialized twice"
    );

    crate::freelist::StrategyInitialize(n)?;
    Ok(())
}

/// Crash-cycle reset in place to the BufferManagerShmemInit boot image — never
/// re-run init (leaks NBuffers×8K). Page bytes stay: cleared tags make them
/// unreachable (notes/crash-restart-design.md).
pub fn BufferManagerShmemResetAfterCrash() {
    // In-flight uring DMA targets pool pages; wait it out before recycling.
    if aio_seams::uring_drain_all_raw::is_installed() {
        aio_seams::uring_drain_all_raw::call();
    }
    let n = NBuffersInited();
    assert!(
        n > 0 && n == globals::NBuffers(),
        "bufmgr: NBuffers changed across the crash cycle"
    );
    for i in 0..n {
        let desc = GetBufferDescriptor(i);
        let free_next = if i + 1 < n {
            i + 1
        } else {
            FREENEXT_END_OF_LIST
        };
        // SAFETY: every child is dead (crash choreography); the postmaster
        // thread has exclusive access — no pin, lock holder, or waiter exists.
        unsafe {
            desc.set_tag(cleared_buftag());
            desc.set_wait_backend_pgprocno(INVALID_PROC_NUMBER);
            desc.set_free_next(free_next);
            *desc.io_wref.get() = PgAioWaitRef::default();
            *desc.content_lock.waiters.get() = lmgr_proc_seams::proclist_head {
                head: INVALID_PROC_NUMBER,
                tail: INVALID_PROC_NUMBER,
            };
        }
        desc.state.store(0, Ordering::Relaxed);
        desc.content_lock
            .state
            .store(lwlock::LW_FLAG_RELEASE_OK, Ordering::Relaxed);
        // SAFETY: exclusive access as above; drops dead procs off the IO CV.
        unsafe {
            core::ptr::write(
                POOL.io_cvs.load(Ordering::Relaxed).add(i as usize),
                ConditionVariable::new(),
            );
        }
    }
    crate::buf_table::BufTableResetAfterCrash();
    crate::freelist::StrategyResetAfterCrash(n);
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
//
// DELIBERATELY NOT a Waiter site (M0 lane C decision): the buffer-header
// lock is held for nanoseconds-to-microseconds and has no wake edge to
// route — parking machinery (slot mutex, handle publication) would cost
// more than the wait it replaces. This is the wrong tool boundary for the
// structured wait primitive; the migrated raw waits (s_lock backoff,
// pg_barrier poll, pg_sema block, latch waits) all had ms-scale sleeps or
// real wake edges.
#[inline]
fn spin_delay(spins: &mut u32) {
    *spins += 1;
    if *spins < 1024 {
        core::hint::spin_loop();
    } else {
        std::thread::yield_now();
    }
}
