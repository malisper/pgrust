use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicI32, AtomicU32, Ordering};
use std::sync::OnceLock;

use init_small::globals;
use types_core::{Buffer, InvalidBuffer, BLCKSZ};
use types_error::{ErrorLocation, PgResult, ERROR};
use types_storage::buf::{
    BufferAccessStrategy, BufferAccessStrategyData, BufferAccessStrategyType, IOContext, Victim,
    BM_LOCKED, BUF_REFCOUNT_MASK, BUF_USAGECOUNT_MASK, BUF_USAGECOUNT_ONE, FREENEXT_NOT_IN_LIST,
};
use types_storage::storage::CacheAligned;
use types_storage::NUM_AUXILIARY_PROCS;

use crate::buf_hdr::{
    BufferDescriptorGetBuffer, GetBufferDescriptor, LockBufHdr, NBuffersInited, UnlockBufHdr,
};
use crate::buf_table::InitBufTable;

pub(crate) struct SpinLock(AtomicU32);

impl SpinLock {
    const fn new() -> SpinLock {
        SpinLock(AtomicU32::new(0))
    }

    #[inline]
    fn acquire(&self) {
        let mut spins = 0u32;
        while self
            .0
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            spins += 1;
            if spins < 1024 {
                core::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
    }

    #[inline]
    fn release(&self) {
        self.0.store(0, Ordering::Release);
    }

    #[inline]
    fn with<R>(&self, f: impl FnOnce() -> R) -> R {
        self.acquire();
        let r = f();
        self.release();
        r
    }
}

struct ClockSweep {
    next_victim_buffer: AtomicU32,
    complete_passes: UnsafeCell<u32>,
}

// false sharing: every backend fetch-adds the clock hand under buffer
// pressure; C CACHELINEALIGNs StrategyControl in shmem. Keep the hand (and
// the complete_passes it advances with) off the spinlock's line.
#[repr(C)]
struct BufferStrategyControl {
    clock: CacheAligned<ClockSweep>,
    buffer_strategy_lock: SpinLock,
    first_free_buffer: AtomicI32,
    last_free_buffer: UnsafeCell<i32>,
    num_buffer_allocs: AtomicU32,
    bgwprocno: AtomicI32,
}

// SAFETY: `last_free_buffer`/`complete_passes` are touched only under
// `buffer_strategy_lock` (C freelist.c contract); the rest are atomics.
unsafe impl Sync for BufferStrategyControl {}
unsafe impl Send for BufferStrategyControl {}

static STRATEGY: OnceLock<BufferStrategyControl> = OnceLock::new();

#[inline]
fn ctl() -> &'static BufferStrategyControl {
    STRATEGY
        .get()
        .expect("bufmgr: StrategyInitialize (freelist.c) not called")
}

pub fn StrategyInitialize(nbuffers: i32) -> PgResult<()> {
    InitBufTable(nbuffers + lwlock::NUM_BUFFER_PARTITIONS)?;
    STRATEGY
        .set(BufferStrategyControl {
            clock: CacheAligned(ClockSweep {
                next_victim_buffer: AtomicU32::new(0),
                complete_passes: UnsafeCell::new(0),
            }),
            buffer_strategy_lock: SpinLock::new(),
            first_free_buffer: AtomicI32::new(0),
            last_free_buffer: UnsafeCell::new(nbuffers - 1),
            num_buffer_allocs: AtomicU32::new(0),
            bgwprocno: AtomicI32::new(-1),
        })
        .unwrap_or_else(|_| panic!("bufmgr: strategy control initialized twice"));
    Ok(())
}

/// Crash-cycle reset to the StrategyInitialize boot image; the caller's
/// descriptor walk relinked every free_next.
pub(crate) fn StrategyResetAfterCrash(nbuffers: i32) {
    let c = ctl();
    c.buffer_strategy_lock.release();
    c.clock.next_victim_buffer.store(0, Ordering::Relaxed);
    c.first_free_buffer.store(0, Ordering::Relaxed);
    // SAFETY: exclusive postmaster access (crash choreography).
    unsafe {
        *c.last_free_buffer.get() = nbuffers - 1;
        *c.clock.complete_passes.get() = 0;
    }
    c.num_buffer_allocs.store(0, Ordering::Relaxed);
    c.bgwprocno.store(-1, Ordering::Relaxed);
}

/// Wraparound folds the hand and bumps completePasses under the spinlock so
/// StrategySyncStart reads a consistent pair.
fn ClockSweepTick() -> i32 {
    let c = ctl();
    let nbuffers = NBuffersInited() as u32;
    let mut victim = c.clock.next_victim_buffer.fetch_add(1, Ordering::Relaxed);
    if victim >= nbuffers {
        let original_victim = victim;
        victim %= nbuffers;
        if victim == 0 {
            let mut expected = original_victim + 1;
            c.buffer_strategy_lock.with(|| loop {
                let wrapped = expected % nbuffers;
                match c.clock.next_victim_buffer.compare_exchange(
                    expected,
                    wrapped,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: strategy spinlock held.
                        unsafe { *c.clock.complete_passes.get() += 1 };
                        break;
                    }
                    Err(v) => {
                        if v < nbuffers {
                            break;
                        }
                        expected = v;
                    }
                }
            });
        }
    }
    victim as i32
}

/// Returns the victim with its header spinlock HELD (`Victim` carries that
/// contract) plus `from_ring`.
pub fn StrategyGetBuffer(strategy: &BufferAccessStrategy) -> PgResult<(Victim, bool)> {
    let c = ctl();

    if let Some(s) = strategy {
        if let Some(victim) = GetBufferFromRing(&mut s.borrow_mut()) {
            return Ok((victim, true));
        }
    }

    let bgwprocno = c.bgwprocno.load(Ordering::Relaxed);
    if bgwprocno != -1 {
        c.bgwprocno.store(-1, Ordering::Relaxed);
        latch::SetLatch(types_storage::latch::LatchHandle::proc(bgwprocno));
    }

    c.num_buffer_allocs.fetch_add(1, Ordering::Relaxed);

    if c.first_free_buffer.load(Ordering::Relaxed) >= 0 {
        loop {
            let buf_id = c.buffer_strategy_lock.with(|| {
                let head = c.first_free_buffer.load(Ordering::Relaxed);
                if head < 0 {
                    return -1;
                }
                let buf = GetBufferDescriptor(head);
                debug_assert!(buf.free_next() != FREENEXT_NOT_IN_LIST);
                c.first_free_buffer.store(buf.free_next(), Ordering::Relaxed);
                // SAFETY: strategy spinlock held.
                unsafe { buf.set_free_next(FREENEXT_NOT_IN_LIST) };
                head
            });
            if buf_id < 0 {
                break;
            }
            let buf = GetBufferDescriptor(buf_id);
            let local_buf_state = LockBufHdr(buf);
            if local_buf_state & BUF_REFCOUNT_MASK == 0
                && local_buf_state & BUF_USAGECOUNT_MASK == 0
            {
                if let Some(s) = strategy {
                    AddBufferToRing(&mut s.borrow_mut(), BufferDescriptorGetBuffer(buf));
                }
                return Ok((victim(buf_id, local_buf_state), false));
            }
            UnlockBufHdr(buf, local_buf_state);
        }
    }

    let mut trycounter = NBuffersInited();
    loop {
        let buf_id = ClockSweepTick();
        let buf = GetBufferDescriptor(buf_id);
        let local_buf_state = LockBufHdr(buf);
        if local_buf_state & BUF_REFCOUNT_MASK == 0 {
            if local_buf_state & BUF_USAGECOUNT_MASK != 0 {
                UnlockBufHdr(buf, local_buf_state - BUF_USAGECOUNT_ONE);
                trycounter = NBuffersInited();
            } else {
                if let Some(s) = strategy {
                    AddBufferToRing(&mut s.borrow_mut(), BufferDescriptorGetBuffer(buf));
                }
                return Ok((victim(buf_id, local_buf_state), false));
            }
        } else {
            trycounter -= 1;
            if trycounter == 0 {
                UnlockBufHdr(buf, local_buf_state);
                return Err(Box::new(
                    types_error::PgError::new(ERROR, "no unpinned buffers available")
                        .with_error_location(ErrorLocation::new(
                            "freelist.c",
                            0,
                            "StrategyGetBuffer",
                        )),
                ));
            }
            UnlockBufHdr(buf, local_buf_state);
        }
    }
}

#[inline]
fn victim(buf_id: i32, buf_state: u32) -> Victim {
    debug_assert!(buf_state & BM_LOCKED != 0);
    Victim { buf_id, buf_state }
}

pub fn StrategyFreeBuffer(buf_id: i32) {
    let c = ctl();
    let buf = GetBufferDescriptor(buf_id);
    c.buffer_strategy_lock.with(|| {
        if buf.free_next() == FREENEXT_NOT_IN_LIST {
            let head = c.first_free_buffer.load(Ordering::Relaxed);
            // SAFETY: strategy spinlock held.
            unsafe {
                buf.set_free_next(head);
                if head < 0 {
                    *c.last_free_buffer.get() = buf_id;
                }
            }
            c.first_free_buffer.store(buf_id, Ordering::Relaxed);
        }
    });
}

/// Bgwriter's consistent read of the clock pair.
pub fn StrategySyncStart() -> (i32, u32, u32) {
    let c = ctl();
    let nbuffers = NBuffersInited() as u32;
    c.buffer_strategy_lock.with(|| {
        let hand = c.clock.next_victim_buffer.load(Ordering::Relaxed);
        let result = (hand % nbuffers) as i32;
        // SAFETY: strategy spinlock held.
        let complete_passes = unsafe { *c.clock.complete_passes.get() } + hand / nbuffers;
        let num_buf_alloc = c.num_buffer_allocs.swap(0, Ordering::Relaxed);
        (result, complete_passes, num_buf_alloc)
    })
}

pub fn StrategyNotifyBgWriter(bgwprocno: i32) {
    let c = ctl();
    c.buffer_strategy_lock
        .with(|| c.bgwprocno.store(bgwprocno, Ordering::Relaxed));
}

pub fn have_free_buffer() -> bool {
    ctl().first_free_buffer.load(Ordering::Relaxed) >= 0
}

pub fn GetAccessStrategy(btype: BufferAccessStrategyType) -> BufferAccessStrategy {
    let ring_size_kb: i32 = match btype {
        BufferAccessStrategyType::BasNormal => return None,
        BufferAccessStrategyType::BasBulkread => {
            let mut kb = 256;
            let ring_max_kb = (GetPinLimit() * (BLCKSZ as i32 / 1024)).max(kb);
            kb += (BLCKSZ as i32 / 1024)
                * crate::gucs::io_combine_limit()
                * crate::gucs::effective_io_concurrency();
            kb.min(ring_max_kb)
        }
        BufferAccessStrategyType::BasBulkwrite => 16 * 1024,
        BufferAccessStrategyType::BasVacuum => 2 * 1024,
    };
    GetAccessStrategyWithSize(btype, ring_size_kb)
}

pub fn GetAccessStrategyWithSize(
    btype: BufferAccessStrategyType,
    ring_size_kb: i32,
) -> BufferAccessStrategy {
    debug_assert!(ring_size_kb >= 0);
    let mut ring_buffers = ring_size_kb / (BLCKSZ as i32 / 1024);
    if ring_buffers == 0 {
        return None;
    }
    ring_buffers = ring_buffers.min(NBuffersInited() / 8);
    Some(std::rc::Rc::new(core::cell::RefCell::new(
        BufferAccessStrategyData {
            btype,
            nbuffers: ring_buffers,
            current: 0,
            buffers: vec![InvalidBuffer; ring_buffers as usize],
        },
    )))
}

pub fn FreeAccessStrategy(strategy: BufferAccessStrategy) {
    drop(strategy);
}

pub fn GetAccessStrategyBufferCount(strategy: &BufferAccessStrategy) -> i32 {
    match strategy {
        None => 0,
        Some(s) => s.borrow().nbuffers,
    }
}

/// Header lock held on success.
fn GetBufferFromRing(s: &mut BufferAccessStrategyData) -> Option<Victim> {
    s.current += 1;
    if s.current >= s.nbuffers {
        s.current = 0;
    }
    let bufnum = s.buffers[s.current as usize];
    if bufnum == InvalidBuffer {
        return None;
    }
    let buf = GetBufferDescriptor(bufnum - 1);
    let local_buf_state = LockBufHdr(buf);
    if local_buf_state & BUF_REFCOUNT_MASK == 0
        && (local_buf_state & BUF_USAGECOUNT_MASK) <= BUF_USAGECOUNT_ONE
    {
        return Some(victim(bufnum - 1, local_buf_state));
    }
    UnlockBufHdr(buf, local_buf_state);
    None
}

fn AddBufferToRing(s: &mut BufferAccessStrategyData, buf: Buffer) {
    s.buffers[s.current as usize] = buf;
}

pub fn StrategyRejectBuffer(
    strategy: &BufferAccessStrategy,
    buf_id: i32,
    from_ring: bool,
) -> bool {
    let Some(s) = strategy else { return false };
    let mut s = s.borrow_mut();
    if s.btype != BufferAccessStrategyType::BasBulkread {
        return false;
    }
    if !from_ring || s.buffers[s.current as usize] != buf_id + 1 {
        return false;
    }
    let cur = s.current as usize;
    s.buffers[cur] = InvalidBuffer;
    true
}

pub fn IOContextForStrategy(strategy: &BufferAccessStrategy) -> IOContext {
    match strategy {
        None => IOContext::IOCONTEXT_NORMAL,
        Some(s) => match s.borrow().btype {
            BufferAccessStrategyType::BasNormal => unreachable!("strategy ring with BAS_NORMAL"),
            BufferAccessStrategyType::BasBulkread => IOContext::IOCONTEXT_BULKREAD,
            BufferAccessStrategyType::BasBulkwrite => IOContext::IOCONTEXT_BULKWRITE,
            BufferAccessStrategyType::BasVacuum => IOContext::IOCONTEXT_VACUUM,
        },
    }
}

/// MaxProportionalPins (bufmgr.c InitBufferManagerAccess).
pub fn GetPinLimit() -> i32 {
    let max_backends = globals::MaxBackends().max(1);
    NBuffersInited() / (max_backends + NUM_AUXILIARY_PROCS)
}
