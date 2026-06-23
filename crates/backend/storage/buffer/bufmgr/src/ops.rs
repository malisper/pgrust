//! Pin / unpin / release / refcount primitives (bufmgr.c) — the backend-local
//! pin lifecycle on an already-resident shared buffer.
//!
//! F1b (this stage): `PinBuffer` / `PinBuffer_Locked` / `UnpinBuffer{,NoOwner}`
//! / `WakePinCountWaiter` / `ReleaseBuffer` / `UnlockReleaseBuffer` /
//! `IncrBufferRefCount` / `BufferIsPermanent`. These operate purely on the
//! shared `state` word (the lock-free pin CAS) and this backend's private pin
//! map — no I/O, no allocation, no victim selection (those are F2/F3/F5). The
//! resource-owner bookkeeping crosses the `remember_buffer` / `forget_buffer` /
//! `resowner_enlarge` seams (bufmgr-defined `ResourceOwnerDesc` callbacks,
//! installed by resowner when it ports — panic-until-owner).

use ::types_core::primitive::Buffer;
use ::types_error::{PgError, PgResult};
use ::types_storage::buf::{
    BM_PERMANENT, BM_PIN_COUNT_WAITER, BM_VALID, BUF_REFCOUNT_ONE, BUF_REFCOUNT_MASK,
    BUF_USAGECOUNT_MASK, BUF_USAGECOUNT_ONE, BM_MAX_USAGE_COUNT, BM_LOCKED,
};
use crate::mgr::BufferManager;

use bufmgr_seams as sb;

/// `InvalidBuffer` (buf.h).
const INVALID_BUFFER: Buffer = 0;

/// `BUF_STATE_GET_REFCOUNT(buf_state)` (buf_internals.h).
#[inline]
fn buf_state_get_refcount(buf_state: u32) -> u32 {
    buf_state & BUF_REFCOUNT_MASK
}

/// `BUF_STATE_GET_USAGECOUNT(buf_state)` (buf_internals.h).
#[inline]
fn buf_state_get_usagecount(buf_state: u32) -> u32 {
    (buf_state & BUF_USAGECOUNT_MASK) / BUF_USAGECOUNT_ONE
}

/// `BufferDescriptorGetBuffer(buf)` — the 1-based [`Buffer`] handle for a
/// 0-based `buf_id`.
#[inline]
fn buf_id_to_buffer(buf_id: i32) -> Buffer {
    buf_id + 1
}

impl BufferManager {
    /// `BufferIsValid(buffer)` for a shared buffer (1..=NBuffers). Local/temp
    /// buffers are a separate, backend-local layer not handled here.
    #[inline]
    fn shared_buffer_is_valid(&self, buffer: Buffer) -> bool {
        buffer != INVALID_BUFFER && buffer > 0 && (buffer as i64) <= self.nbuffers() as i64
    }

    /// `buffer - 1`, with the `BufferIsValid` `elog(ERROR, "bad buffer ID")`
    /// surface (bufmgr.c's repeated guard).
    #[inline]
    fn buffer_to_buf_id(&self, buffer: Buffer) -> PgResult<usize> {
        if !self.shared_buffer_is_valid(buffer) {
            return Err(PgError::error(format!("bad buffer ID: {buffer}")));
        }
        Ok((buffer - 1) as usize)
    }

    // -- pin / unpin (bufmgr.c) --------------------------------------------

    /// `PinBuffer(buf, strategy)` (bufmgr.c:3067) — pin a shared buffer. A
    /// backend-local pin that promotes the shared refcount when this backend's
    /// private count crosses 0->1, with the usagecount bump. `has_strategy` is
    /// `strategy != NULL`. Returns true iff the buffer is `BM_VALID`. Faithful
    /// to the CAS loop with the `WaitBufHdrUnlocked` re-read on `BM_LOCKED`.
    #[allow(dead_code)]
    pub(crate) fn pin_buffer(&self, buf_id: usize, has_strategy: bool) -> bool {
        let b = buf_id_to_buffer(buf_id as i32);
        let refc = self.private_refcount();
        refc.ReservePrivateRefCountEntry();

        let result;
        if refc.GetPrivateRefCountEntry(b, true).is_none() {
            refc.NewPrivateRefCountEntry(b);
            let mut old_buf_state = self.read_state(buf_id);
            loop {
                if old_buf_state & BM_LOCKED != 0 {
                    old_buf_state = self.wait_buf_hdr_unlocked(buf_id);
                }
                let mut buf_state = old_buf_state;
                buf_state += BUF_REFCOUNT_ONE;
                if !has_strategy {
                    if buf_state_get_usagecount(buf_state) < BM_MAX_USAGE_COUNT {
                        buf_state += BUF_USAGECOUNT_ONE;
                    }
                } else if buf_state_get_usagecount(buf_state) == 0 {
                    buf_state += BUF_USAGECOUNT_ONE;
                }
                match self.state_compare_exchange(buf_id, old_buf_state, buf_state) {
                    Ok(_) => {
                        result = buf_state & BM_VALID != 0;
                        break;
                    }
                    Err(actual) => old_buf_state = actual,
                }
            }
        } else {
            // If we previously pinned the buffer, it is likely to be valid, but
            // it may not be if StartReadBuffers() was called and
            // WaitReadBuffers() hasn't been called yet.  We'll check by loading
            // the flags without locking.  This is racy, but it's OK to return
            // false spuriously: when WaitReadBuffers() calls StartBufferIO(),
            // it'll see that it's now valid.
            result = self.read_state(buf_id) & BM_VALID != 0;
        }

        refc.incr(buf_id as i32);
        // ResourceOwnerRememberBuffer(CurrentResourceOwner, b) (bufmgr.c:3151).
        sb::remember_buffer::call(b);
        result
    }

    /// `PinBuffer_Locked(buf)` (bufmgr.c:3178) — as [`Self::pin_buffer`], but the
    /// caller already holds the header spinlock (released here). No usagecount
    /// change, no `BM_VALID` test, and no preexisting-pin search. `buf_state` is
    /// the state word returned by the header lock.
    #[allow(dead_code)]
    pub(crate) fn pin_buffer_locked(&self, buf_id: usize, buf_state: u32) {
        debug_assert!(buf_state & BM_LOCKED != 0);
        // Since we hold the buffer spinlock, we can update the buffer state and
        // release the lock in one operation.
        let new_state = buf_state + BUF_REFCOUNT_ONE;
        self.unlock_buf_hdr(buf_id, new_state);

        let b = buf_id_to_buffer(buf_id as i32);
        let refc = self.private_refcount();
        // C bufmgr.c:3204 Assert(GetPrivateRefCountEntry(b, false) == NULL): the
        // caller contract is "no preexisting pin by this backend". A violation
        // here means an UPSTREAM PIN LEAK (some scan path failed to unpin) —
        // name the page so the leaker is identifiable.
        if refc.GetPrivateRefCountEntry(b, false).is_some() {
            let tag = self.desc_tag(buf_id);
            panic!(
                "PinBuffer_Locked: buffer {b} already pinned by this backend \
                 (UPSTREAM PIN LEAK) — tag rel={}/{}/{} fork={:?} block={}",
                tag.spcOid, tag.dbOid, tag.relNumber, tag.forkNum, tag.blockNum
            );
        }
        refc.NewPrivateRefCountEntry(b);
        refc.incr(buf_id as i32);
        // ResourceOwnerRememberBuffer(CurrentResourceOwner, b) (bufmgr.c:3211).
        sb::remember_buffer::call(b);
    }

    /// `WakePinCountWaiter(buf)` (bufmgr.c:3224) — wake the backend parked as the
    /// `BM_PIN_COUNT_WAITER` once we have released the last non-waiter pin.
    #[allow(dead_code)]
    pub(crate) fn wake_pin_count_waiter(&self, buf_id: usize) {
        // Acquire the buffer header lock, re-check that there's a waiter. Another
        // backend could have unpinned this buffer, and already woken up the
        // waiter.
        let mut buf_state = self.lock_buf_hdr(buf_id);
        if buf_state & BM_PIN_COUNT_WAITER != 0 && buf_state_get_refcount(buf_state) == 1 {
            // we just released the last pin other than the waiter's
            let wait_backend_pgprocno = self.wait_backend_pgprocno(buf_id);
            buf_state &= !BM_PIN_COUNT_WAITER;
            self.unlock_buf_hdr(buf_id, buf_state);
            // ProcSendSignal(wait_backend_pgprocno) — SetLatch on its procLatch.
            lmgr_proc_seams::set_proc_latch::call(wait_backend_pgprocno);
        } else {
            self.unlock_buf_hdr(buf_id, buf_state);
        }
    }

    /// `UnpinBufferNoOwner(buf)` (bufmgr.c:3268) — drop a backend-local pin
    /// WITHOUT touching the resource owner. When this backend's private count
    /// reaches 0, decrement the shared refcount (CAS loop, with the `BM_LOCKED`
    /// wait) and wake a pin-count waiter if any.
    #[allow(dead_code)]
    pub(crate) fn unpin_buffer_no_owner(&self, buf_id: usize) {
        let refc = self.private_refcount();
        debug_assert!(refc.get(buf_id as i32) > 0);
        let new = refc.decr(buf_id as i32);
        if new == 0 {
            // Decrement the shared reference count.
            //
            // Since buffer spinlock holder can update status using just write,
            // it's not safe to use atomic decrement here; thus use a CAS loop.
            let mut old_buf_state = self.read_state(buf_id);
            let buf_state = loop {
                if old_buf_state & BM_LOCKED != 0 {
                    old_buf_state = self.wait_buf_hdr_unlocked(buf_id);
                }
                let buf_state = old_buf_state - BUF_REFCOUNT_ONE;
                match self.state_compare_exchange(buf_id, old_buf_state, buf_state) {
                    Ok(_) => break buf_state,
                    Err(actual) => old_buf_state = actual,
                }
            };

            // Support LockBufferForCleanup()
            if buf_state & BM_PIN_COUNT_WAITER != 0 {
                self.wake_pin_count_waiter(buf_id);
            }

            let entry = crate::refcount::PrivateRefCountEntry {
                buffer: buf_id_to_buffer(buf_id as i32),
                refcount: 0,
            };
            refc.ForgetPrivateRefCountEntry(entry);
        }
    }

    /// `UnpinBuffer(buf)` (bufmgr.c:3258) — make a buffer available for
    /// replacement; this ALWAYS adjusts `CurrentResourceOwner`. Faithful to C:
    /// `ResourceOwnerForgetBuffer(CurrentResourceOwner, b)` then
    /// `UnpinBufferNoOwner(buf)`.
    #[allow(dead_code)]
    pub(crate) fn unpin_buffer(&self, buf_id: usize) {
        let b = buf_id_to_buffer(buf_id as i32);
        sb::forget_buffer::call(b);
        self.unpin_buffer_no_owner(buf_id);
    }

    // -- public C-named entries (bufmgr.c) ---------------------------------

    /// `ReleaseBuffer(buffer)` (bufmgr.c:5366) — decrement a buffer's pin count.
    pub fn ReleaseBuffer(&self, buffer: Buffer) -> PgResult<()> {
        // if (!BufferIsValid(buffer)) elog(ERROR, "bad buffer ID"). A local
        // (temp) buffer carries a negative handle and IS valid.
        if !self.shared_buffer_is_valid(buffer)
            && !crate::buf_lock::buffer_is_local(buffer)
        {
            return Err(PgError::error(format!("bad buffer ID: {buffer}")));
        }
        // if (BufferIsLocal(buffer)) UnpinLocalBuffer(buffer); (bufmgr.c:5373).
        if crate::buf_lock::buffer_is_local(buffer) {
            return buffer_support_seams::unpin_local_buffer::call(buffer);
        }
        // UnpinBuffer(GetBufferDescriptor(buffer - 1)) (bufmgr.c:5374).
        let buf_id = (buffer - 1) as usize;
        self.unpin_buffer(buf_id);
        Ok(())
    }

    /// `ResOwnerReleaseBufferPin(Datum res)` (bufmgr.c:6555) — release a leaked
    /// buffer pin found by the resource owner during release. Like
    /// `ReleaseBuffer`, but does NOT call `ResourceOwnerForgetBuffer` (the owner
    /// is already mid-release):
    ///
    /// ```c
    /// if (!BufferIsValid(buffer)) elog(ERROR, "bad buffer ID: %d", buffer);
    /// if (BufferIsLocal(buffer)) UnpinLocalBufferNoOwner(buffer);
    /// else UnpinBufferNoOwner(GetBufferDescriptor(buffer - 1));
    /// ```
    ///
    /// Local buffers (negative handle) are the separate backend-local layer not
    /// modeled in this shared core; the reachable resource-owner release path
    /// only ever carries shared pins.
    pub fn ResOwnerReleaseBufferPin(&self, buffer: Buffer) -> PgResult<()> {
        // if (!BufferIsValid(buffer)) elog(ERROR, "bad buffer ID"). Local
        // (temp) buffers carry a negative handle and are valid.
        if !self.shared_buffer_is_valid(buffer)
            && !crate::buf_lock::buffer_is_local(buffer)
        {
            return Err(PgError::error(format!("bad buffer ID: {buffer}")));
        }
        // if (BufferIsLocal(buffer)) UnpinLocalBufferNoOwner(buffer); (bufmgr.c:6564).
        if crate::buf_lock::buffer_is_local(buffer) {
            return buffer_support_seams::unpin_local_buffer_no_owner::call(buffer);
        }
        // UnpinBufferNoOwner(GetBufferDescriptor(buffer - 1)) (bufmgr.c:6566).
        let buf_id = (buffer - 1) as usize;
        self.unpin_buffer_no_owner(buf_id);
        Ok(())
    }

    /// `UnlockReleaseBuffer(buffer)` (bufmgr.c:5383) — release the content lock
    /// then the pin: `LockBuffer(buffer, BUFFER_LOCK_UNLOCK)` + `ReleaseBuffer`.
    pub fn UnlockReleaseBuffer(&self, buffer: Buffer) -> PgResult<()> {
        // LockBuffer(buffer, BUFFER_LOCK_UNLOCK). For a local (temp) buffer
        // there is no content lock (LockBuffer's BufferIsLocal arm returns),
        // so only the pin is released.
        if !crate::buf_lock::buffer_is_local(buffer) {
            let buf_id = self.buffer_to_buf_id(buffer)?;
            // LockBuffer(buffer, BUFFER_LOCK_UNLOCK) == LWLockRelease(content_lock)
            // (the unconditional unlock leg of LockBuffer; direct lwlock dep).
            lwlock::LWLockRelease(self.content_lock(buf_id))?;
        }
        self.ReleaseBuffer(buffer)
    }

    /// `IncrBufferRefCount(buffer)` (bufmgr.c:5398) — bump a pinned buffer's
    /// backend-local pin count by one (no shared-state change since the buffer
    /// is already pinned by this backend).
    pub fn IncrBufferRefCount(&self, buffer: Buffer) -> PgResult<()> {
        // Assert(BufferIsPinned(buffer)).
        // ResourceOwnerEnlarge(CurrentResourceOwner) (bufmgr.c:5402).
        sb::resowner_enlarge::call()?;
        // if (BufferIsLocal(buffer)) LocalRefCount[-buffer - 1]++; (bufmgr.c:5404).
        if crate::buf_lock::buffer_is_local(buffer) {
            buffer_support_seams::incr_local_buffer_ref_count::call(buffer)?;
        } else {
            let buf_id = self.buffer_to_buf_id(buffer)?;
            let refc = self.private_refcount();
            debug_assert!(refc.get(buf_id as i32) > 0, "IncrBufferRefCount: buffer is not pinned");
            // ref = GetPrivateRefCountEntry(buffer, true); ref->refcount++.
            refc.incr(buf_id as i32);
        }
        // ResourceOwnerRememberBuffer(CurrentResourceOwner, buffer) (bufmgr.c:5415).
        sb::remember_buffer::call(buffer);
        Ok(())
    }

    /// `BufferIsPermanent(buffer)` (bufmgr.c:5586) — whether the buffer survives
    /// a crash (its relation is WAL-logged). The caller must hold a pin.
    pub fn BufferIsPermanent(&self, buffer: Buffer) -> PgResult<bool> {
        // Local buffers are used only for temp relations -> always false
        // (bufmgr.c:5590).
        if crate::buf_lock::buffer_is_local(buffer) {
            return Ok(false);
        }
        let buf_id = self.buffer_to_buf_id(buffer)?;
        // Assert(BufferIsPinned(buffer)).
        debug_assert!(
            self.private_refcount().get(buf_id as i32) > 0,
            "BufferIsPermanent: buffer is not pinned"
        );
        // Synchronization here is not strictly necessary; BM_PERMANENT cannot
        // change while a buffer is pinned, so no atomicity is needed (the C reads
        // pg_atomic_read_u32 without the header lock).
        Ok(self.read_state(buf_id) & BM_PERMANENT != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Install no-op resource-owner stubs ONCE (resowner is unported; the
    /// production seams panic-until-owner; `set` itself panics on a second
    /// install). Mirrors the src-idiomatic test wiring. The pin-state assertions
    /// read each test's own `BufferManager` instance, so they don't race across
    /// the parallel test harness.
    fn install_resowner_stubs() {
        crate::mgr::test_seams::install();
    }

    /// `BufferManagerShmemInit` with a small pool; the descriptor states start
    /// zeroed (refcount 0, usagecount 0).
    fn mk() -> BufferManager {
        BufferManager::new(4)
    }

    #[test]
    fn pin_promotes_shared_refcount_and_bumps_usagecount() {
        install_resowner_stubs();
        let bm = mk();
        // 0-based buf_id 0 == Buffer 1.
        let valid = bm.pin_buffer(0, false);
        // not BM_VALID yet (state still 0), but pin succeeds.
        assert!(!valid);
        // private + shared refcount each 1.
        assert_eq!(bm.private_refcount().get(0), 1);
        assert_eq!(buf_state_get_refcount(bm.read_state(0)), 1);
        // usagecount bumped (no strategy).
        assert_eq!(buf_state_get_usagecount(bm.read_state(0)), 1);

        // A second pin by this backend: shared refcount unchanged, private++.
        let _ = bm.pin_buffer(0, false);
        assert_eq!(bm.private_refcount().get(0), 2);
        assert_eq!(buf_state_get_refcount(bm.read_state(0)), 1);
    }

    #[test]
    fn pin_with_strategy_caps_usagecount_at_one() {
        install_resowner_stubs();
        let bm = mk();
        let _ = bm.pin_buffer(1, true);
        assert_eq!(buf_state_get_usagecount(bm.read_state(1)), 1);
        // unpin so usagecount stays; re-pin with strategy keeps it at 1.
        bm.unpin_buffer(1);
        let _ = bm.pin_buffer(1, true);
        assert_eq!(buf_state_get_usagecount(bm.read_state(1)), 1);
    }

    #[test]
    fn release_drops_shared_refcount_to_zero() {
        install_resowner_stubs();
        let bm = mk();
        let _ = bm.pin_buffer(2, false);
        assert_eq!(buf_state_get_refcount(bm.read_state(2)), 1);
        // Buffer 3 == buf_id 2.
        bm.ReleaseBuffer(3).unwrap();
        assert_eq!(bm.private_refcount().get(2), 0);
        assert_eq!(buf_state_get_refcount(bm.read_state(2)), 0);
    }

    #[test]
    fn incr_buffer_ref_count_bumps_only_private() {
        install_resowner_stubs();
        let bm = mk();
        let _ = bm.pin_buffer(0, false);
        bm.IncrBufferRefCount(1).unwrap();
        assert_eq!(bm.private_refcount().get(0), 2);
        // shared refcount unchanged (still 1: already pinned).
        assert_eq!(buf_state_get_refcount(bm.read_state(0)), 1);
    }

    #[test]
    fn release_buffer_rejects_invalid() {
        install_resowner_stubs();
        let bm = mk();
        assert!(bm.ReleaseBuffer(0).is_err());
        assert!(bm.ReleaseBuffer(99).is_err());
    }

    #[test]
    fn pin_buffer_locked_takes_one_pin() {
        install_resowner_stubs();
        let bm = mk();
        // Acquire the header lock to get the state word, then PinBuffer_Locked.
        let state = bm.lock_buf_hdr(0);
        bm.pin_buffer_locked(0, state);
        assert_eq!(bm.private_refcount().get(0), 1);
        assert_eq!(buf_state_get_refcount(bm.read_state(0)), 1);
    }
}
