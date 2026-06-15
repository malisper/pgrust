//! `freelist.c` — backend-private buffer-ring management: the
//! `BufferAccessStrategy` object family.
//!
//! A `BufferAccessStrategy` is the small ring of shared buffers that a bulk
//! operation (a sequential scan, COPY, VACUUM, ...) recycles instead of
//! evicting arbitrary buffers and trashing the whole cache. This is PRIVATE,
//! BACKEND-LOCAL state — NOT shared memory: the ring is an owned `Vec<Buffer>`
//! touched only by the owning backend, exactly as `freelist.c` documents. The
//! ring merely *names* shared buffers (by `Buffer` number); the buffers
//! themselves live in the shared pool and are still selected/pinned/evicted
//! through the shared clock sweep ([`crate::ClockSweep`]).
//!
//! When the ring reuses a buffer, the per-buffer header (`LockBufHdr` /
//! `UnlockBufHdr`) lives in the bufmgr-owned shmem descriptor array and is
//! reached through the same `lock_buf_hdr` / `unlock_buf_hdr` seams the shared
//! clock sweep uses. The cluster knobs (`GetPinLimit`, `io_combine_limit`,
//! `effective_io_concurrency`) consulted when sizing a `BAS_BULKREAD` ring are
//! read through the bufmgr seams.

use types_error::{PgError, PgResult};
use types_storage::buf::{
    BufferAccessStrategyData, BufferAccessStrategyType, IOContext, Victim,
};
use types_core::{Buffer, BLCKSZ};
use types_storage::InvalidBuffer;

use crate::{buf_state_get_refcount, buf_state_get_usagecount};

use backend_storage_buffer_bufmgr_seams as bufmgr_seam;

/// `BLCKSZ / 1024` — kilobytes per buffer, used to convert a ring size in KB to
/// a ring size in buffers (freelist.c).
const BLCKSZ_KB: i32 = (BLCKSZ / 1024) as i32;

/// `GetAccessStrategy(btype)` — create a `BufferAccessStrategyData` ring for
/// the given access type, choosing the ring size per buffer/README. Returns
/// `Ok(None)` for `BAS_NORMAL` (the "default", no-ring strategy) and for any
/// type whose computed ring size rounds down to zero buffers.
///
/// The cluster knobs (`GetPinLimit`, `io_combine_limit`,
/// `effective_io_concurrency`) are read through their seams. The ring struct
/// lives in `types-storage`, so this is a free constructor rather than an
/// associated `new`.
pub fn get_access_strategy_ring(
    btype: BufferAccessStrategyType,
    nbuffers_total: i32,
) -> PgResult<Option<BufferAccessStrategyData>> {
        let ring_size_kb = match btype {
            // If someone asks for NORMAL, just give 'em a "default" object (None).
            BufferAccessStrategyType::BasNormal => return Ok(None),
            BufferAccessStrategyType::BasBulkread => {
                // The ring always needs to be large enough to allow some
                // separation in time between providing a buffer to the user and
                // that buffer being reused. Start minimal and grow if
                // appropriate.
                let mut ring_size_kb: i32 = 256;

                // No point in a larger ring if we won't be allowed to pin
                // sufficiently many buffers. But never below the minimal size.
                let mut ring_max_kb = bufmgr_seam::get_pin_limit::call().saturating_mul(BLCKSZ_KB);
                ring_max_kb = ring_size_kb.max(ring_max_kb);

                // Additionally have space for the configured degree of IO
                // concurrency. Each IO can be up to io_combine_limit blocks
                // large, and we want to start up to effective_io_concurrency IOs.
                ring_size_kb = ring_size_kb.saturating_add(
                    BLCKSZ_KB
                        .saturating_mul(bufmgr_seam::io_combine_limit::call())
                        .saturating_mul(bufmgr_seam::effective_io_concurrency::call()),
                );

                if ring_size_kb > ring_max_kb {
                    ring_size_kb = ring_max_kb;
                }
                ring_size_kb
            }
            BufferAccessStrategyType::BasBulkwrite => 16 * 1024,
            BufferAccessStrategyType::BasVacuum => 2048,
        };

        get_access_strategy_with_size_ring(btype, ring_size_kb, nbuffers_total)
}

/// `GetAccessStrategyWithSize(btype, ring_size_kb)` — create a
/// `BufferAccessStrategyData` whose ring holds `ring_size_kb / (BLCKSZ/1024)`
/// buffers, capped at 1/8 of the shared buffer pool. Returns `Ok(None)` when
/// `ring_size_kb` rounds down to zero buffers (0 means "unlimited"; no ring
/// needed). `ring_size_kb` must not be negative.
pub fn get_access_strategy_with_size_ring(
    btype: BufferAccessStrategyType,
    ring_size_kb: i32,
    nbuffers_total: i32,
) -> PgResult<Option<BufferAccessStrategyData>> {
        // Assert(ring_size_kb >= 0).
        if ring_size_kb < 0 {
            return Err(PgError::error(
                "GetAccessStrategyWithSize: ring_size_kb must not be negative",
            ));
        }

        // Figure out how many buffers ring_size_kb is.
        let mut ring_buffers = ring_size_kb / BLCKSZ_KB;

        // 0 means unlimited, so no BufferAccessStrategy required.
        if ring_buffers == 0 {
            return Ok(None);
        }

        // Cap to 1/8th of shared_buffers.
        ring_buffers = (nbuffers_total / 8).min(ring_buffers);

        // NBuffers should never be less than 16, so this shouldn't happen.
        debug_assert!(ring_buffers > 0);

        // Allocate the object and initialize all ring slots to InvalidBuffer
        // (the C `palloc0` of offsetof(..., buffers) + nbuffers *
        // sizeof(Buffer)). Reserve fallibly; surface OOM as a PgError.
        let mut buffers: alloc::vec::Vec<Buffer> = alloc::vec::Vec::new();
        buffers
            .try_reserve(ring_buffers as usize)
            .map_err(|_| PgError::error("out of memory (buffer access strategy ring)"))?;
        buffers.resize(ring_buffers as usize, InvalidBuffer);

        Ok(Some(BufferAccessStrategyData {
            btype,
            nbuffers: ring_buffers,
            current: 0,
            buffers,
        }))
}

/// The `freelist.c` ring algorithms over a [`BufferAccessStrategyData`]. The
/// struct itself lives in `types-storage` (the shared vocabulary crate the
/// pointer-threading consumers reach), so the per-instance policy — which
/// touches the bufmgr header/GUC seams this crate consumes — is an extension
/// trait implemented here. `current` starts at 0 from the `palloc0`, but the
/// first `get_buffer_from_ring` pre-increments it (wrapping from `nbuffers -
/// 1`), so the first slot examined is index 1 ... then 0, exactly as C does.
pub trait BufferAccessStrategyRing {
    fn GetAccessStrategyBufferCount(&self) -> i32;
    fn GetAccessStrategyPinLimit(&self) -> i32;
    fn IOContextForStrategy(&self) -> PgResult<IOContext>;
    fn get_buffer_from_ring(&mut self) -> PgResult<Option<Victim>>;
    fn add_buffer_to_ring(&mut self, buf_id: i32);
    fn reject_buffer(&mut self, buf_id: i32, from_ring: bool) -> bool;
    fn btype(&self) -> BufferAccessStrategyType;
    fn current(&self) -> i32;
}

impl BufferAccessStrategyRing for BufferAccessStrategyData {
    /// `GetAccessStrategyBufferCount(strategy)` — the number of buffers in the
    /// ring. The free-function form returns 0 on a NULL strategy; that NULL case
    /// is the caller's `Option::map_or(0, ...)`.
    fn GetAccessStrategyBufferCount(&self) -> i32 {
        self.nbuffers
    }

    /// `GetAccessStrategyPinLimit(strategy)` — cap on how many ring buffers a
    /// caller should pin at once while looking ahead. `BAS_BULKREAD` may pin the
    /// whole ring (it uses `StrategyRejectBuffer`); the others pin at most half.
    /// The NULL-strategy case (return `NBuffers`) is the caller's, for a `None`.
    fn GetAccessStrategyPinLimit(&self) -> i32 {
        match self.btype {
            BufferAccessStrategyType::BasBulkread => self.nbuffers,
            _ => self.nbuffers / 2,
        }
    }

    /// `IOContextForStrategy(strategy)` — the I/O-stats context for this ring's
    /// reads/writes. The NULL-strategy case (`IOCONTEXT_NORMAL`) is the
    /// caller's. `BAS_NORMAL` is unreachable here because `GetAccessStrategy`
    /// never builds a ring for it (`pg_unreachable()`).
    fn IOContextForStrategy(&self) -> PgResult<IOContext> {
        match self.btype {
            BufferAccessStrategyType::BasNormal => Err(PgError::error(
                "unrecognized BufferAccessStrategyType: BAS_NORMAL",
            )),
            BufferAccessStrategyType::BasBulkread => Ok(IOContext::IOCONTEXT_BULKREAD),
            BufferAccessStrategyType::BasBulkwrite => Ok(IOContext::IOCONTEXT_BULKWRITE),
            BufferAccessStrategyType::BasVacuum => Ok(IOContext::IOCONTEXT_VACUUM),
        }
    }

    /// `GetBufferFromRing(strategy, buf_state)` — try to reuse the next buffer
    /// in the ring, advancing the ring cursor first. Returns:
    ///
    ///  * `Ok(Some(victim))` — the ring slot held a buffer that is unpinned and
    ///    has usagecount <= 1, so it is reusable; the [`Victim`] carries the
    ///    header spinlock STILL HELD ("the bufhdr spin lock is held on the
    ///    returned buffer"). The caller must `PinBuffer_Locked` it.
    ///  * `Ok(None)` — the slot was empty, or the buffer in it is pinned / too
    ///    hot to reuse; the caller should fall through to the clock sweep and
    ///    then record the fresh victim back here with `add_buffer_to_ring`.
    fn get_buffer_from_ring(&mut self) -> PgResult<Option<Victim>> {
        // Advance to next ring slot.
        self.current += 1;
        if self.current >= self.nbuffers {
            self.current = 0;
        }

        // If the slot hasn't been filled yet, tell the caller to allocate a new
        // buffer with the normal allocation strategy.
        let bufnum = self.buffers[self.current as usize];
        if bufnum == InvalidBuffer {
            return Ok(None);
        }

        // If the buffer is pinned we cannot use it. If usage_count is 0 or 1 then
        // the buffer is fair game (we expect 1, since our own previous usage of
        // the ring element would have left it there, but clock sweep may have
        // decremented it). A higher usage_count means someone else touched it.
        //
        // GetBufferDescriptor(bufnum - 1): Buffer numbers are 1-based.
        let buf_id = bufnum - 1;
        let buf_state = bufmgr_seam::lock_buf_hdr::call(buf_id);
        if buf_state_get_refcount(buf_state) == 0 && buf_state_get_usagecount(buf_state) <= 1 {
            return Ok(Some(Victim { buf_id, buf_state }));
        }
        bufmgr_seam::unlock_buf_hdr::call(buf_id, buf_state);

        // Tell caller to allocate a new buffer with the normal allocation
        // strategy.
        Ok(None)
    }

    /// `AddBufferToRing(strategy, buf)` — record `buf_id` as the buffer now
    /// occupying the current ring slot. Called by `StrategyGetBuffer` after the
    /// normal path picks a fresh victim for a ring-based strategy. Per the C
    /// contract this is called WITH the buffer's header spinlock held and must
    /// be cheap — a single store. `buf_id` is the 0-based index
    /// (`BufferDescriptorGetBuffer(buf)` stores the 1-based `Buffer`).
    fn add_buffer_to_ring(&mut self, buf_id: i32) {
        self.buffers[self.current as usize] = buf_id + 1;
    }

    /// `StrategyRejectBuffer(strategy, buf, from_ring)` — decide whether to
    /// reject a dirty buffer the clock sweep selected (when writing it out would
    /// require flushing WAL too). Returns `true` if the buffer manager should
    /// ask for a new victim, `false` if this buffer should be written and
    /// re-used.
    ///
    /// Faithful: only `BAS_BULKREAD` rejects; only a buffer that came
    /// `from_ring` AND is still the one in the current slot is rejected; on
    /// rejection the slot is cleared to `InvalidBuffer` so the ring can't loop
    /// forever on all-dirty members. `buf_id` is the 0-based index.
    fn reject_buffer(&mut self, buf_id: i32, from_ring: bool) -> bool {
        // We only do this in bulkread mode.
        if self.btype != BufferAccessStrategyType::BasBulkread {
            return false;
        }

        // Don't muck with behavior of normal buffer-replacement strategy.
        if !from_ring || self.buffers[self.current as usize] != buf_id + 1 {
            return false;
        }

        // Remove the dirty buffer from the ring.
        self.buffers[self.current as usize] = InvalidBuffer;

        true
    }

    /// The strategy's access type (`BufferAccessStrategyData.btype`).
    fn btype(&self) -> BufferAccessStrategyType {
        self.btype
    }

    /// `strategy->current` — the index of the current ring slot.
    fn current(&self) -> i32 {
        self.current
    }
}

/// `FreeAccessStrategy(strategy)` — release a `BufferAccessStrategy` object. In
/// C this is a guarded `pfree`; in Rust the `Rc<RefCell<..>>` ring storage is
/// freed when the last reference is dropped, so this consumes the handle (the
/// "don't crash on a default (NULL) strategy" guard is the `Option` being
/// `None`).
pub fn FreeAccessStrategy(strategy: types_storage::buf::BufferAccessStrategy) {
    drop(strategy);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{install_test_seams, TestHeaders};
    use backend_storage_buffer_bufmgr_seams as bufmgr_seam;

    // NBuffers used by the ring-cap tests (1/8th == big enough not to clamp the
    // small rings below).
    const NBUFFERS: i32 = 1024;

    fn release(v: Victim) {
        bufmgr_seam::unlock_buf_hdr::call(v.buf_id, v.buf_state);
    }

    #[test]
    fn normal_strategy_is_none() {
        let _g = install_test_seams();
        let s =
            get_access_strategy_ring(BufferAccessStrategyType::BasNormal, NBUFFERS)
                .unwrap();
        assert!(s.is_none());
    }

    #[test]
    fn bulkwrite_ring_is_16mb_worth_of_buffers() {
        let _g = install_test_seams();
        let s = get_access_strategy_ring(
            BufferAccessStrategyType::BasBulkwrite,
            NBUFFERS,
        )
        .unwrap()
        .unwrap();
        // ring_buffers = 16*1024 / 8 = 2048, capped to NBUFFERS/8 = 128.
        assert_eq!(s.GetAccessStrategyBufferCount(), NBUFFERS / 8);
        assert_eq!(s.btype(), BufferAccessStrategyType::BasBulkwrite);
    }

    #[test]
    fn vacuum_ring_is_2mb_worth_of_buffers() {
        let _g = install_test_seams();
        let s = get_access_strategy_ring(BufferAccessStrategyType::BasVacuum, 8192)
            .unwrap()
            .unwrap();
        assert_eq!(s.GetAccessStrategyBufferCount(), 2048 / BLCKSZ_KB);
    }

    #[test]
    fn bulkread_ring_grows_with_io_concurrency() {
        let _g = install_test_seams();
        TestHeaders::set_pin_limit(i32::MAX);
        TestHeaders::set_io_combine_limit(16);
        TestHeaders::set_effective_io_concurrency(10);
        let s = get_access_strategy_ring(
            BufferAccessStrategyType::BasBulkread,
            4096,
        )
        .unwrap()
        .unwrap();
        // 256 + 8*16*10 = 1536 KB => 192 buffers.
        assert_eq!(s.GetAccessStrategyBufferCount(), 1536 / BLCKSZ_KB);
    }

    #[test]
    fn bulkread_ring_capped_by_pin_limit() {
        let _g = install_test_seams();
        TestHeaders::set_pin_limit(1); // ring_max_kb = 1*8 = 8, clamped up to 256
        TestHeaders::set_io_combine_limit(16);
        TestHeaders::set_effective_io_concurrency(10);
        let s = get_access_strategy_ring(
            BufferAccessStrategyType::BasBulkread,
            4096,
        )
        .unwrap()
        .unwrap();
        // grows to 1536 then re-clamped to ring_max_kb == max(256, 8) == 256.
        assert_eq!(s.GetAccessStrategyBufferCount(), 256 / BLCKSZ_KB);
    }

    #[test]
    fn zero_size_yields_no_strategy() {
        let _g = install_test_seams();
        let s = get_access_strategy_with_size_ring(
            BufferAccessStrategyType::BasVacuum,
            0,
            NBUFFERS,
        )
        .unwrap();
        assert!(s.is_none());
        let s = get_access_strategy_with_size_ring(
            BufferAccessStrategyType::BasVacuum,
            BLCKSZ_KB - 1,
            NBUFFERS,
        )
        .unwrap();
        assert!(s.is_none());
    }

    #[test]
    fn negative_size_errors() {
        let _g = install_test_seams();
        let r = get_access_strategy_with_size_ring(
            BufferAccessStrategyType::BasVacuum,
            -1,
            NBUFFERS,
        );
        assert!(r.is_err());
    }

    #[test]
    fn ring_capped_to_one_eighth_of_pool() {
        let _g = install_test_seams();
        let s = get_access_strategy_with_size_ring(
            BufferAccessStrategyType::BasBulkwrite,
            16 * 1024,
            80, // NBuffers/8 == 10
        )
        .unwrap()
        .unwrap();
        assert_eq!(s.GetAccessStrategyBufferCount(), 10);
    }

    #[test]
    fn pin_limit_bulkread_is_whole_ring_others_half() {
        let _g = install_test_seams();
        let read = get_access_strategy_with_size_ring(
            BufferAccessStrategyType::BasBulkread,
            256,
            NBUFFERS,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            read.GetAccessStrategyPinLimit(),
            read.GetAccessStrategyBufferCount()
        );
        let vac = get_access_strategy_with_size_ring(
            BufferAccessStrategyType::BasVacuum,
            256,
            NBUFFERS,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            vac.GetAccessStrategyPinLimit(),
            vac.GetAccessStrategyBufferCount() / 2
        );
    }

    #[test]
    fn io_context_maps_each_type() {
        let _g = install_test_seams();
        let read = get_access_strategy_with_size_ring(
            BufferAccessStrategyType::BasBulkread,
            256,
            NBUFFERS,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            read.IOContextForStrategy().unwrap(),
            IOContext::IOCONTEXT_BULKREAD
        );
        let write = get_access_strategy_with_size_ring(
            BufferAccessStrategyType::BasBulkwrite,
            16 * 1024,
            NBUFFERS,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            write.IOContextForStrategy().unwrap(),
            IOContext::IOCONTEXT_BULKWRITE
        );
        let vac = get_access_strategy_with_size_ring(
            BufferAccessStrategyType::BasVacuum,
            256,
            NBUFFERS,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            vac.IOContextForStrategy().unwrap(),
            IOContext::IOCONTEXT_VACUUM
        );
    }

    #[test]
    fn empty_slot_returns_none_and_advances() {
        let _g = install_test_seams();
        TestHeaders::reset(8);
        let mut s = get_access_strategy_with_size_ring(
            BufferAccessStrategyType::BasBulkread,
            256,
            NBUFFERS,
        )
        .unwrap()
        .unwrap();
        // First call: current pre-increments to 1, slot empty => None.
        assert!(s.get_buffer_from_ring().unwrap().is_none());
        assert_eq!(s.current(), 1);
        assert_eq!(TestHeaders::locked_count(), 0);
    }

    #[test]
    fn add_then_get_reuses_buffer_with_lock_held() {
        let _g = install_test_seams();
        TestHeaders::reset(8);
        let mut s = get_access_strategy_with_size_ring(
            BufferAccessStrategyType::BasBulkread,
            256,
            NBUFFERS,
        )
        .unwrap()
        .unwrap();
        assert!(s.get_buffer_from_ring().unwrap().is_none());
        assert_eq!(s.current(), 1);
        s.add_buffer_to_ring(5);
        TestHeaders::set(5, 0, 1);
        for _ in 0..s.GetAccessStrategyBufferCount() {
            if let Some(v) = s.get_buffer_from_ring().unwrap() {
                assert_eq!(v.buf_id, 5);
                assert_eq!(TestHeaders::locked_count(), 1);
                release(v);
                assert_eq!(TestHeaders::locked_count(), 0);
                return;
            }
        }
        panic!("ring never returned the recorded buffer");
    }

    #[test]
    fn pinned_ring_buffer_is_skipped() {
        let _g = install_test_seams();
        TestHeaders::reset(8);
        let mut s = get_access_strategy_with_size_ring(
            BufferAccessStrategyType::BasBulkread,
            256,
            NBUFFERS,
        )
        .unwrap()
        .unwrap();
        assert!(s.get_buffer_from_ring().unwrap().is_none());
        s.add_buffer_to_ring(5);
        TestHeaders::set(5, 1, 1); // pinned
        let mut saw_slot1 = false;
        for _ in 0..s.GetAccessStrategyBufferCount() {
            let r = s.get_buffer_from_ring().unwrap();
            if s.current() == 1 {
                saw_slot1 = true;
                assert!(r.is_none());
            }
        }
        assert!(saw_slot1);
        assert_eq!(TestHeaders::locked_count(), 0);
    }

    #[test]
    fn hot_ring_buffer_is_skipped() {
        let _g = install_test_seams();
        TestHeaders::reset(8);
        let mut s = get_access_strategy_with_size_ring(
            BufferAccessStrategyType::BasBulkread,
            256,
            NBUFFERS,
        )
        .unwrap()
        .unwrap();
        assert!(s.get_buffer_from_ring().unwrap().is_none());
        s.add_buffer_to_ring(5);
        TestHeaders::set(5, 0, 2); // unpinned but usagecount 2
        for _ in 0..s.GetAccessStrategyBufferCount() {
            let r = s.get_buffer_from_ring().unwrap();
            if s.current() == 1 {
                assert!(r.is_none());
            }
        }
        assert_eq!(TestHeaders::locked_count(), 0);
    }

    #[test]
    fn reject_buffer_only_bulkread_from_ring_current() {
        let _g = install_test_seams();
        TestHeaders::reset(8);
        let mut read = get_access_strategy_with_size_ring(
            BufferAccessStrategyType::BasBulkread,
            256,
            NBUFFERS,
        )
        .unwrap()
        .unwrap();
        read.add_buffer_to_ring(5);
        assert!(!read.reject_buffer(5, false));
        assert!(!read.reject_buffer(9, true));
        assert!(read.reject_buffer(5, true));
        assert!(!read.reject_buffer(5, true));
    }

    #[test]
    fn reject_buffer_noop_for_non_bulkread() {
        let _g = install_test_seams();
        TestHeaders::reset(8);
        let mut vac = get_access_strategy_with_size_ring(
            BufferAccessStrategyType::BasVacuum,
            256,
            NBUFFERS,
        )
        .unwrap()
        .unwrap();
        vac.add_buffer_to_ring(5);
        assert!(!vac.reject_buffer(5, true));
    }

    #[test]
    fn free_access_strategy_consumes() {
        let _g = install_test_seams();
        let s = get_access_strategy_with_size_ring(
            BufferAccessStrategyType::BasVacuum,
            256,
            NBUFFERS,
        )
        .unwrap()
        .unwrap();
        // Wrap the ring in the by-pointer handle (`Rc<RefCell<_>>`), as
        // `GetAccessStrategy` does, then free it.
        let handle: types_storage::buf::BufferAccessStrategy =
            Some(alloc::rc::Rc::new(core::cell::RefCell::new(s)));
        FreeAccessStrategy(handle);
    }
}
