//! `freelist.c` — the buffer-pool replacement strategy: the clock sweep and the
//! free list.
//!
//! The `BufferStrategyControl` is modeled field-for-field with the real spinlock
//! + atomic semantics:
//!
//!  * `buffer_strategy_lock` is a real [`Spinlock`] (`slock_t`), acquired with
//!    `s_lock` and released with `s_unlock` from `backend-storage-lmgr-s-lock`,
//!  * `nextVictimBuffer` and `numBufferAllocs` are `pg_atomic_uint32` atomics
//!    (modeled with [`AtomicU32`], lock-free as in C),
//!  * `firstFreeBuffer` / `lastFreeBuffer` / `completePasses` / `bgwprocno` are
//!    the spinlock-protected integer fields.
//!
//! The control block is placed via the `ShmemInitStruct` seam (which returns
//! the `found` flag so `StrategyInitialize` runs its "initialize once" path).
//! The per-buffer header (`LockBufHdr` / `UnlockBufHdr` / `freeNext`) lives in
//! the bufmgr-owned shmem descriptor array and is reached through the
//! `lock_buf_hdr` / `unlock_buf_hdr` / `buf_free_next` bufmgr seams. The clock
//! sweep + free-list-first algorithm is unchanged.

use std::cell::Cell;
use std::sync::atomic::{AtomicU32, Ordering};

use backend_storage_lmgr_s_lock::{s_lock_macro, s_unlock, Spinlock};
use types_core::Size;
use types_error::{PgError, PgResult};
use types_storage::buf::{
    BufferAccessStrategyData, Victim, BUF_USAGECOUNT_ONE, FREENEXT_NOT_IN_LIST,
};
use types_storage::NUM_BUFFER_PARTITIONS;

use crate::strategy::BufferAccessStrategyRing;
use crate::{buf_state_get_refcount, buf_state_get_usagecount};

use backend_storage_buffer_bufmgr_seams as bufmgr_seam;

/// `BufferStrategyControl` (freelist.c) — the shared freelist control
/// information, modeled field-for-field with the real spinlock + atomic
/// semantics. The shared-memory allocation is signaled through the
/// `ShmemInitStruct` seam.
pub struct BufferStrategyControl {
    /// `slock_t buffer_strategy_lock` — the spinlock word. Protects
    /// `first_free_buffer`, `last_free_buffer`, `complete_passes`, `bgwprocno`,
    /// and the per-buffer `freeNext` links.
    buffer_strategy_lock: Spinlock,
    /// `pg_atomic_uint32 nextVictimBuffer` — clock-sweep hand (only ever
    /// increased; used modulo NBuffers).
    next_victim_buffer: AtomicU32,
    /// `int firstFreeBuffer` — head of the list of unused buffers.
    first_free_buffer: Cell<i32>,
    /// `int lastFreeBuffer` — tail of the list of unused buffers (undefined when
    /// `first_free_buffer < 0`).
    last_free_buffer: Cell<i32>,
    /// `uint32 completePasses` — complete cycles of the clock sweep.
    complete_passes: Cell<u32>,
    /// `pg_atomic_uint32 numBufferAllocs` — buffers allocated since last reset.
    num_buffer_allocs: AtomicU32,
    /// `int bgwprocno` — bgworker proc to notify upon activity, or -1.
    bgwprocno: Cell<i32>,
    /// `NBuffers` — the size of the shared buffer pool (not part of the C
    /// struct; it is the `NBuffers` global the routines consult).
    nbuffers: u32,
}

impl BufferStrategyControl {
    /// `StrategyInitialize` — get-or-create the shared strategy control block.
    /// Honors the `found` flag like C: on first creation (postmaster) initialize
    /// the spinlock, the free-list head/tail, and clear the statistics; on
    /// attach reuse the existing contents.
    ///
    /// The per-buffer `freeNext` links are set up by the caller (through the
    /// header seam) during `BufferManagerShmemInit`, matching C where
    /// `StrategyInitialize` "grabs the whole linked list of free buffers ...
    /// previously set up by BufferManagerShmemInit".
    pub fn StrategyInitialize(nbuffers: u32) -> PgResult<Self> {
        let size = core::mem::size_of::<Self>();
        let (_addr, found) =
            backend_storage_ipc_shmem_seams::shmem_init_struct::call("Buffer Strategy Status", size)?;
        let n = nbuffers as i32;
        // The control block is allocated zeroed; field initialization happens
        // only on first creation, mirroring C's `if (!found) { ... }`. In this
        // single-owner substrate the postmaster is the sole creator (`found` is
        // always false here); the owned Rust handle cannot alias a peer
        // backend's live segment.
        let control = Self {
            buffer_strategy_lock: Spinlock::new(),
            next_victim_buffer: AtomicU32::new(0),
            first_free_buffer: Cell::new(0),
            last_free_buffer: Cell::new(0),
            complete_passes: Cell::new(0),
            num_buffer_allocs: AtomicU32::new(0),
            bgwprocno: Cell::new(0),
            nbuffers,
        };
        if !found {
            // Only done once, usually in postmaster.
            // SpinLockInit(&buffer_strategy_lock): Spinlock::new() above is the
            // S_INIT_LOCK; nothing more to do.
            // Grab the whole linked list of free buffers for our strategy. We
            // assume it was previously set up by BufferManagerShmemInit().
            // (firstFreeBuffer = 0; lastFreeBuffer = NBuffers - 1: when
            // NBuffers == 0 this leaves lastFreeBuffer = -1, the C value.)
            control.first_free_buffer.set(0);
            control.last_free_buffer.set(n - 1);
            // Initialize the clock sweep pointer: pg_atomic_init_u32(.., 0).
            control.next_victim_buffer.store(0, Ordering::Relaxed);
            // Clear statistics.
            control.complete_passes.set(0);
            control.num_buffer_allocs.store(0, Ordering::Relaxed);
            // No pending notification.
            control.bgwprocno.set(-1);
        }
        Ok(control)
    }

    pub fn nbuffers(&self) -> u32 {
        self.nbuffers
    }

    /// `SpinLockAcquire(&buffer_strategy_lock)` returning an RAII guard.
    fn acquire_lock(&self) -> SpinGuard<'_> {
        s_lock_macro(
            &self.buffer_strategy_lock,
            Some(file!()),
            line!() as i32,
            Some("StrategyControl::acquire_lock"),
        );
        SpinGuard { control: self }
    }

    /// `have_free_buffer` — a lockless check (`firstFreeBuffer >= 0`).
    pub fn have_free_buffer(&self) -> bool {
        self.first_free_buffer.get() >= 0
    }

    /// `StrategyFreeBuffer` — put a buffer back on the free-list head. Matches
    /// the C guard that refuses to corrupt the list if the buffer is already in
    /// it (`freeNext != FREENEXT_NOT_IN_LIST`). The `freeNext` links live in the
    /// descriptor array (reached through the header seam), protected by the
    /// spinlock.
    pub fn free_buffer(&self, buf_id: i32) -> PgResult<()> {
        let _guard = self.acquire_lock();
        if bufmgr_seam::buf_free_next::call(buf_id) == FREENEXT_NOT_IN_LIST {
            let head = self.first_free_buffer.get();
            bufmgr_seam::set_buf_free_next::call(buf_id, head);
            if head < 0 {
                self.last_free_buffer.set(buf_id);
            }
            self.first_free_buffer.set(buf_id);
        }
        Ok(())
    }

    /// `StrategyNotifyBgWriter` — set (or clear, with -1) the bgwriter proc
    /// number the next `StrategyGetBuffer` will wake. Acquires the spinlock just
    /// to make the store appear atomic to `StrategyGetBuffer`, as in C.
    pub fn notify_bgwriter(&self, bgwprocno: i32) -> PgResult<()> {
        let _guard = self.acquire_lock();
        self.bgwprocno.set(bgwprocno);
        Ok(())
    }

    /// `StrategySyncStart` — return `nextVictimBuffer % NBuffers`, the
    /// complete-passes count adjusted by the in-flight wraparound
    /// (`nextVictimBuffer / NBuffers`), and the reset alloc count. Serialized by
    /// the spinlock so the pair is consistent.
    pub fn sync_start(&self) -> PgResult<(i32, u32, u32)> {
        let _guard = self.acquire_lock();
        // pg_atomic_read_u32 — No barrier semantics (atomics.h), so Relaxed.
        let next = self.next_victim_buffer.load(Ordering::Relaxed);
        if self.nbuffers == 0 {
            return Ok((
                0,
                self.complete_passes.get(),
                // pg_atomic_exchange_u32 — Full barrier semantics.
                self.num_buffer_allocs.swap(0, Ordering::SeqCst),
            ));
        }
        let result = (next % self.nbuffers) as i32;
        let complete_passes = self.complete_passes.get() + next / self.nbuffers;
        // pg_atomic_exchange_u32 — Full barrier semantics.
        let num_buf_alloc = self.num_buffer_allocs.swap(0, Ordering::SeqCst);
        Ok((result, complete_passes, num_buf_alloc))
    }

    /// Snapshot of `completePasses` (without the in-flight adjustment).
    pub fn complete_passes(&self) -> u32 {
        self.complete_passes.get()
    }

    /// `numBufferAllocs` accumulator read+reset.
    pub fn take_num_buffer_allocs(&self) -> u32 {
        // pg_atomic_exchange_u32 — Full barrier semantics.
        self.num_buffer_allocs.swap(0, Ordering::SeqCst)
    }
}

/// RAII guard for `buffer_strategy_lock` (`SpinLockRelease` on drop).
struct SpinGuard<'a> {
    control: &'a BufferStrategyControl,
}

impl Drop for SpinGuard<'_> {
    fn drop(&mut self) {
        s_unlock(&self.control.buffer_strategy_lock);
    }
}

/// `StrategyShmemSize` — footprint of the buffer lookup hash plus the strategy
/// control block, matching `BufTableShmemSize(NBuffers + NUM_BUFFER_PARTITIONS)
/// + MAXALIGN(sizeof(BufferStrategyControl))`.
pub fn StrategyShmemSize(nbuffers: i32) -> Size {
    let hash = crate::buf_table::BufTableShmemSize(nbuffers.saturating_add(NUM_BUFFER_PARTITIONS));
    let control = core::mem::size_of::<BufferStrategyControl>().next_multiple_of(8);
    hash + control
}

/// The clock-sweep hand. Wraps a [`BufferStrategyControl`], implementing
/// `ClockSweepTick` and the free-list-first victim selection of
/// `StrategyGetBuffer`.
pub struct ClockSweep<'a> {
    control: &'a BufferStrategyControl,
}

impl<'a> ClockSweep<'a> {
    pub fn new(control: &'a BufferStrategyControl) -> Self {
        Self { control }
    }

    /// `ClockSweepTick` — advance the clock hand by one and return the victim
    /// buffer index, wrapping modulo NBuffers and bumping `completePasses` on
    /// each full revolution. Faithful to the lock-free fetch-add + CAS dance in
    /// freelist.c; the wrap path takes `buffer_strategy_lock` while incrementing
    /// `completePasses`.
    pub fn tick(&self) -> PgResult<u32> {
        let nbuffers = self.control.nbuffers;
        if nbuffers == 0 {
            return Ok(0);
        }
        // pg_atomic_fetch_add_u32 — Full barrier semantics (atomics.h).
        let victim = self.control.next_victim_buffer.fetch_add(1, Ordering::SeqCst);
        if victim >= nbuffers {
            let original_victim = victim;
            let victim = victim % nbuffers;
            if victim == 0 {
                let mut expected = original_victim.wrapping_add(1);
                loop {
                    let _guard = self.control.acquire_lock();
                    let wrapped = expected % nbuffers;
                    // pg_atomic_compare_exchange_u32 — Full barrier semantics
                    // (both success and failure orderings).
                    match self.control.next_victim_buffer.compare_exchange(
                        expected,
                        wrapped,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => {
                            self.control
                                .complete_passes
                                .set(self.control.complete_passes.get().wrapping_add(1));
                            break;
                        }
                        Err(actual) => expected = actual,
                    }
                }
            }
            return Ok(victim);
        }
        Ok(victim)
    }

    /// `StrategyGetBuffer` — select the next victim buffer for `BufferAlloc`,
    /// returning a [`Victim`] whose buffer header spinlock is STILL HELD, like
    /// the C contract: the caller must `PinBuffer_Locked` it before releasing
    /// the header lock so no other backend can pin the victim in between.
    ///
    /// `strategy` is `Some(ring)` for a non-default `BufferAccessStrategy`,
    /// `None` for the default (no-ring) strategy. `*from_ring` (returned through
    /// the `&mut bool`) is true iff the victim came from the strategy ring.
    pub fn get_buffer(
        &self,
        mut strategy: Option<&mut BufferAccessStrategyData>,
        from_ring: &mut bool,
    ) -> PgResult<Victim> {
        let control = self.control;
        *from_ring = false;

        // If given a strategy object, see whether it can select a buffer. We
        // assume strategy objects don't need buffer_strategy_lock.
        if let Some(ref mut strat) = strategy {
            if let Some(victim) = strat.get_buffer_from_ring()? {
                *from_ring = true;
                return Ok(victim);
            }
        }

        // Waken the bgwriter if asked (read-once + reset, then SetLatch).
        let bgwprocno = control.bgwprocno.get();
        if bgwprocno != -1 {
            control.bgwprocno.set(-1);
            backend_storage_ipc_latch_seams::set_latch_for_procno::call(bgwprocno);
        }

        // Count buffer allocation requests for the bgwriter's rate estimate.
        // (Ring recycles returned above, before this point.)
        // pg_atomic_fetch_add_u32 — Full barrier semantics (atomics.h).
        control.num_buffer_allocs.fetch_add(1, Ordering::SeqCst);

        // First: try the free list (lockless check, then under the spinlock pop).
        if control.first_free_buffer.get() >= 0 {
            loop {
                let buf = {
                    let _guard = control.acquire_lock();
                    if control.first_free_buffer.get() < 0 {
                        break;
                    }
                    let buf = control.first_free_buffer.get();
                    debug_assert_ne!(bufmgr_seam::buf_free_next::call(buf), FREENEXT_NOT_IN_LIST);
                    // Unconditionally remove from freelist.
                    control
                        .first_free_buffer
                        .set(bufmgr_seam::buf_free_next::call(buf));
                    bufmgr_seam::set_buf_free_next::call(buf, FREENEXT_NOT_IN_LIST);
                    buf
                };
                // Released the spinlock; now LockBufHdr the candidate and keep
                // the lock held iff it is usable (return-with-lock-held path).
                let buf_state = bufmgr_seam::lock_buf_hdr::call(buf);
                if buf_state_get_refcount(buf_state) == 0 && buf_state_get_usagecount(buf_state) == 0
                {
                    if let Some(ref mut strat) = strategy {
                        strat.add_buffer_to_ring(buf);
                    }
                    return Ok(Victim { buf_id: buf, buf_state });
                }
                // Pinned or hot: UnlockBufHdr, discard and retry.
                bufmgr_seam::unlock_buf_hdr::call(buf, buf_state);
            }
        }

        // Nothing usable on the freelist: run the clock sweep.
        let mut trycounter = control.nbuffers;
        loop {
            let buf = self.tick()? as i32;
            let mut buf_state = bufmgr_seam::lock_buf_hdr::call(buf);
            if buf_state_get_refcount(buf_state) == 0 {
                if buf_state_get_usagecount(buf_state) != 0 {
                    buf_state -= BUF_USAGECOUNT_ONE;
                    trycounter = control.nbuffers;
                } else {
                    // Found a usable buffer: return with the header lock held.
                    if let Some(ref mut strat) = strategy {
                        strat.add_buffer_to_ring(buf);
                    }
                    return Ok(Victim { buf_id: buf, buf_state });
                }
            } else {
                trycounter -= 1;
                if trycounter == 0 {
                    // Scanned all buffers without a state change: all pinned.
                    bufmgr_seam::unlock_buf_hdr::call(buf, buf_state);
                    return Err(PgError::error("no unpinned buffers available"));
                }
            }
            bufmgr_seam::unlock_buf_hdr::call(buf, buf_state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{install_test_seams, TestHeaders};
    use backend_storage_buffer_bufmgr_seams as bufmgr_seam;

    fn get_default(sweep: &ClockSweep) -> PgResult<Victim> {
        let mut from_ring = false;
        sweep.get_buffer(None, &mut from_ring)
    }

    #[test]
    fn clock_hand_wraps_and_counts_passes() {
        let _g = install_test_seams();
        TestHeaders::reset(4);
        let ctl = BufferStrategyControl::StrategyInitialize(4).unwrap();
        let sweep = ClockSweep::new(&ctl);
        for expected in 0..4 {
            assert_eq!(sweep.tick().unwrap(), expected);
        }
        assert_eq!(ctl.complete_passes(), 0);
        assert_eq!(sweep.tick().unwrap(), 0);
        assert_eq!(ctl.complete_passes(), 1);
    }

    #[test]
    fn get_buffer_pops_free_list_first() {
        let _g = install_test_seams();
        TestHeaders::reset(3);
        let ctl = BufferStrategyControl::StrategyInitialize(3).unwrap();
        let sweep = ClockSweep::new(&ctl);
        assert!(ctl.have_free_buffer());
        let victim = get_default(&sweep).unwrap();
        assert_eq!(victim.buf_id, 0);
        let (id, st) = victim.into_parts();
        bufmgr_seam::unlock_buf_hdr::call(id, st);
    }

    #[test]
    fn clock_sweep_decrements_usagecount_until_zero() {
        let _g = install_test_seams();
        TestHeaders::reset(1);
        let ctl = BufferStrategyControl::StrategyInitialize(1).unwrap();
        let sweep = ClockSweep::new(&ctl);
        let v = get_default(&sweep).unwrap();
        assert_eq!(v.buf_id, 0);
        let (id, st) = v.into_parts();
        bufmgr_seam::unlock_buf_hdr::call(id, st);
        TestHeaders::set(0, 0, 2);
        let v = get_default(&sweep).unwrap();
        assert_eq!(v.buf_id, 0);
        let (id, st) = v.into_parts();
        bufmgr_seam::unlock_buf_hdr::call(id, st);
        assert_eq!(TestHeaders::usagecount(0), 0);
    }

    #[test]
    fn all_pinned_pool_errors() {
        let _g = install_test_seams();
        TestHeaders::reset(2);
        let ctl = BufferStrategyControl::StrategyInitialize(2).unwrap();
        let sweep = ClockSweep::new(&ctl);
        let v0 = get_default(&sweep).unwrap();
        bufmgr_seam::unlock_buf_hdr::call(v0.buf_id, v0.buf_state);
        let v1 = get_default(&sweep).unwrap();
        bufmgr_seam::unlock_buf_hdr::call(v1.buf_id, v1.buf_state);
        TestHeaders::set(0, 1, 1);
        TestHeaders::set(1, 1, 1);
        assert!(get_default(&sweep).is_err());
    }

    #[test]
    fn freed_buffer_returns_to_list() {
        let _g = install_test_seams();
        TestHeaders::reset(2);
        let ctl = BufferStrategyControl::StrategyInitialize(2).unwrap();
        let sweep = ClockSweep::new(&ctl);
        let v0 = get_default(&sweep).unwrap();
        bufmgr_seam::unlock_buf_hdr::call(v0.buf_id, v0.buf_state);
        let v1 = get_default(&sweep).unwrap();
        bufmgr_seam::unlock_buf_hdr::call(v1.buf_id, v1.buf_state);
        assert!(!ctl.have_free_buffer());
        ctl.free_buffer(1).unwrap();
        assert!(ctl.have_free_buffer());
        // Double-free is a no-op (freeNext guard).
        ctl.free_buffer(1).unwrap();
        assert!(ctl.have_free_buffer());
    }

    #[test]
    fn bgwriter_latch_fires_once_then_clears() {
        let _g = install_test_seams();
        TestHeaders::reset(2);
        let ctl = BufferStrategyControl::StrategyInitialize(2).unwrap();
        let sweep = ClockSweep::new(&ctl);
        TestHeaders::clear_latches();
        ctl.notify_bgwriter(7).unwrap();
        let v0 = get_default(&sweep).unwrap();
        bufmgr_seam::unlock_buf_hdr::call(v0.buf_id, v0.buf_state);
        let v1 = get_default(&sweep).unwrap();
        bufmgr_seam::unlock_buf_hdr::call(v1.buf_id, v1.buf_state);
        assert_eq!(TestHeaders::latches(), alloc::vec![7]);
    }

    #[test]
    fn sync_start_reports_passes_and_resets_allocs() {
        let _g = install_test_seams();
        TestHeaders::reset(4);
        let ctl = BufferStrategyControl::StrategyInitialize(4).unwrap();
        let sweep = ClockSweep::new(&ctl);
        let v0 = get_default(&sweep).unwrap();
        bufmgr_seam::unlock_buf_hdr::call(v0.buf_id, v0.buf_state);
        let v1 = get_default(&sweep).unwrap();
        bufmgr_seam::unlock_buf_hdr::call(v1.buf_id, v1.buf_state);
        let (start, passes, allocs) = ctl.sync_start().unwrap();
        assert_eq!(start, 0);
        assert_eq!(passes, 0);
        assert_eq!(allocs, 2);
        let (_, _, allocs2) = ctl.sync_start().unwrap();
        assert_eq!(allocs2, 0);
    }
}
