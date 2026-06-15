//! `BufferAlloc` / `GetVictimBuffer` / `InvalidateBuffer` /
//! `InvalidateVictimBuffer` and the buffer-I/O lifecycle
//! (`StartBufferIO` / `TerminateBufferIO` / `WaitIO`) plus victim selection
//! (`StrategyGetBuffer` / `StrategyFreeBuffer`) — bufmgr.c.
//!
//! F2a (this stage): the allocate-or-evict half of the buffer pool. Victim
//! selection runs through the buffer-support clock sweep
//! ([`backend_storage_buffer_support::ClockSweep`]) over THIS manager's
//! descriptor array (reached by the support code through the F1-installed
//! header/freelist seams). The dirty-victim flush rides the `flush_one_buffer`
//! seam (installed by the flush owner in F5 — panic-until-owner). The mapping
//! (`BufferMappingLock`) partition locks are taken directly via the lwlock dep
//! ([`crate::mgr::BufferManager::map_acquire`]); the per-buffer content lock for
//! the victim flush is a direct content-lock acquire.
//!
//! The read path (`ReadBuffer*` / `StartReadBuffers` / `WaitReadBuffers`) and
//! the AIO staging callbacks are F3/F4; the actual `FlushBuffer`/`BufferSync`
//! write engine is F5. `start_buffer_io` / `terminate_buffer_io` / `wait_io` are
//! ported here (the IO-progress machinery) for those later stages to consume.
//!
//! Every entry here is consumed by the read/extend/flush layers (F2b/F3/F5) or
//! by the drop-relation path, none of which has landed yet — so the whole module
//! is `dead_code` until those wire it. (`buffer_alloc` is the `BufferAlloc`
//! lookup-or-allocate core the read path drives; `invalidate_buffer` the
//! drop-relation path; the IO lifecycle the read/flush owners.)
#![allow(dead_code)]

use backend_storage_buffer_support::ClockSweep;
use types_core::primitive::{BlockNumber, Buffer, ForkNumber};
use types_error::{PgError, PgResult};
use types_storage::buf::{
    buftag, PgAioWaitRef, BM_CHECKPOINT_NEEDED, BM_DIRTY, BM_IO_ERROR, BM_IO_IN_PROGRESS,
    BM_JUST_DIRTIED, BM_PERMANENT, BM_PIN_COUNT_WAITER, BM_TAG_VALID, BM_VALID, BUF_FLAG_MASK,
    BUF_REFCOUNT_ONE, BUF_USAGECOUNT_MASK, BUF_USAGECOUNT_ONE,
};
use types_storage::storage::LWLockMode;
use types_storage::RelFileLocatorBackend;
use types_tuple::access::RELPERSISTENCE_PERMANENT;

use crate::mgr::BufferManager;

use backend_storage_buffer_bufmgr_seams as sb;
use backend_storage_buffer_support::{buf_table_hash_code, buf_table_hash_partition};
use backend_storage_lmgr_lwlock as lwlock;

/// `BUF_STATE_GET_REFCOUNT(buf_state)` (buf_internals.h).
#[inline]
fn buf_state_get_refcount(buf_state: u32) -> u32 {
    buf_state & types_storage::buf::BUF_REFCOUNT_MASK
}

/// `BUF_STATE_GET_USAGECOUNT(buf_state)` (buf_internals.h).
#[inline]
fn buf_state_get_usagecount(buf_state: u32) -> u32 {
    (buf_state & BUF_USAGECOUNT_MASK) / BUF_USAGECOUNT_ONE
}

/// `BufferDescriptorGetBuffer(buf)` — the 1-based [`Buffer`] for a 0-based
/// `buf_id`.
#[inline]
fn buf_id_to_buffer(buf_id: i32) -> Buffer {
    buf_id + 1
}

/// `INIT_FORKNUM` (common/relpath.h) — the init fork is always WAL-logged.
const INIT_FORKNUM: ForkNumber = ForkNumber::INIT_FORKNUM;

/// `WAIT_EVENT_BUFFER_IO` (PG_WAIT_IO class) — the wait-event id parked on
/// while sleeping for a buffer's in-progress I/O to complete (`WaitIO`).
const WAIT_EVENT_BUFFER_IO: u32 = 0x0a00_0000;

/// `InitBufferTag(&tag, rlocator, forkNum, blockNum)` (buf_internals.h) — build
/// the buffer tag from the *unbacked* `RelFileLocator` (the `backend` part of a
/// `RelFileLocatorBackend` is NOT part of the tag; temp buffers go to the local
/// pool, out of this shared core).
fn make_tag(rlocator: RelFileLocatorBackend, forknum: ForkNumber, blocknum: BlockNumber) -> buftag {
    buftag {
        spcOid: rlocator.locator.spcOid,
        dbOid: rlocator.locator.dbOid,
        relNumber: rlocator.locator.relNumber,
        forkNum: forknum,
        blockNum: blocknum,
    }
}

/// `BufTableHashPartition(BufTableHashCode(&tag))` — the partition index of a
/// tag, used to select its `BufferMappingLock`.
fn tag_partition(tag: &buftag) -> u32 {
    buf_table_hash_partition(buf_table_hash_code(tag))
}

impl BufferManager {
    // -- victim selection (freelist.c StrategyGetBuffer / StrategyFreeBuffer) --

    /// `StrategyGetBuffer(strategy, *buf_state, *from_ring)` (freelist.c:196) —
    /// select the next victim buffer for [`Self::get_victim_buffer`], returning
    /// `(buf_id, buf_state)` with the buffer header spinlock STILL HELD (the C
    /// contract: the caller `PinBuffer_Locked`s before releasing it). Runs the
    /// shared clock sweep over THIS manager's descriptor array (the support code
    /// reaches the state word + freeNext via the F1-installed header/freelist
    /// seams).
    ///
    /// `has_strategy` collapses the `BufferAccessStrategy` ring to a bool, like
    /// src-idiomatic: a real ring would be threaded as `Some(&mut strategy)` into
    /// `ClockSweep::get_buffer` (which already returns `from_ring`); here we call
    /// it with `None` (the default no-ring strategy) and document the collapse,
    /// same posture as the F1 freelist note. The dirty-victim WAL-veto
    /// (`StrategyRejectBuffer`) is unreachable while the ring is collapsed.
    fn strategy_get_buffer(&self, _has_strategy: bool) -> PgResult<(usize, u32)> {
        let sweep = ClockSweep::new(self.strategy_control());
        let mut from_ring = false;
        let victim = sweep.get_buffer(None, &mut from_ring)?;
        Ok((victim.buf_id as usize, victim.buf_state))
    }

    /// `StrategyFreeBuffer(buf)` (freelist.c:363) — put a buffer back on the
    /// freelist head (only if not already linked). The support control block
    /// owns the head; the per-descriptor `freeNext` it touches is reached via the
    /// F1-installed `buf_free_next` / `set_buf_free_next` seams.
    pub(crate) fn strategy_free_buffer(&self, buf_id: usize) -> PgResult<()> {
        self.strategy_control().free_buffer(buf_id as i32)
    }

    // -- GetVictimBuffer (bufmgr.c:2345) -----------------------------------

    /// `GetVictimBuffer(strategy, io_context)` (bufmgr.c:2345) — select a clean,
    /// unpinned victim buffer, flushing it first if dirty, then pin it. On return
    /// the buffer is pinned (refcount 1), not in the lookup hash, and tag-cleared.
    pub(crate) fn get_victim_buffer(&self, has_strategy: bool) -> PgResult<usize> {
        let (buf_id, buf_state) = self.strategy_get_buffer(has_strategy)?;
        debug_assert_eq!(buf_state_get_refcount(buf_state), 0);

        // Pin it while we still hold the header lock.
        self.pin_buffer_locked(buf_id, buf_state);
        let buf_state = self.read_state(buf_id);

        // DEFERRED (bufmgr.c:2425-2441 — StrategyRejectBuffer/WAL veto): a ring
        // strategy may refuse a dirty victim whose LSN still needs a WAL flush,
        // looping to pick a fresh buffer outside the ring. UNREACHABLE here: the
        // ring is collapsed to `has_strategy: bool` (no `from_ring` tracking), so
        // `StrategyRejectBuffer` would always return false and the unconditional
        // flush already matches C for every supported path. The veto becomes
        // meaningful only once a real ring is threaded through victim selection.
        //
        // If the buffer was dirty, write it out (FlushBuffer under a SHARE content
        // lock; the physical write engine is the F5 flush owner via the
        // `flush_one_buffer` seam — panic-until-owner).
        if buf_state & BM_DIRTY != 0 {
            let lock = self.content_lock(buf_id);
            lwlock::LWLockAcquire(
                lock,
                LWLockMode::LW_SHARED,
                backend_storage_lmgr_proc_seams::my_proc_number::call(),
            )?;
            let flush = sb::flush_one_buffer::call(buf_id_to_buffer(buf_id as i32));
            lwlock::LWLockRelease(lock)?;
            flush?;
        }

        // Now it is safe to release the victim's old mapping + invalidate it.
        if buf_state & BM_TAG_VALID != 0 {
            let old_tag = self.desc_tag(buf_id);
            let part = tag_partition(&old_tag);
            let guard = self.map_acquire(part, LWLockMode::LW_EXCLUSIVE)?;
            let still = self.invalidate_victim_buffer(buf_id);
            guard.release()?;
            let still = still?;
            if !still {
                // Someone re-dirtied/re-pinned it; retry from scratch.
                // UnpinBuffer(buf_hdr) (bufmgr.c:2481).
                self.unpin_buffer(buf_id);
                return self.get_victim_buffer(has_strategy);
            }
        }
        Ok(buf_id)
    }

    /// `InvalidateVictimBuffer(buf_hdr)` (bufmgr.c:2277) — clear a pinned victim
    /// buffer's tag/flags so it can be reused. Returns false if it became dirty
    /// or someone else pinned it (refcount > 1) meanwhile. Caller holds the
    /// partition `BufferMappingLock`.
    pub(crate) fn invalidate_victim_buffer(&self, buf_id: usize) -> PgResult<bool> {
        let mut buf_state = self.lock_buf_hdr(buf_id);
        debug_assert!(buf_state_get_refcount(buf_state) > 0);
        if buf_state & BM_DIRTY != 0 || buf_state_get_refcount(buf_state) != 1 {
            self.unlock_buf_hdr(buf_id, buf_state);
            return Ok(false);
        }
        let old_tag = self.desc_tag(buf_id);
        // Clear the tag + the tag-valid/valid/usage bits, keep the lock + pin.
        buf_state &= !(BM_VALID
            | BM_TAG_VALID
            | BM_DIRTY
            | BM_JUST_DIRTIED
            | BM_CHECKPOINT_NEEDED
            | BM_IO_ERROR
            | BM_PERMANENT
            | BUF_USAGECOUNT_MASK);
        self.set_desc_tag(buf_id, buftag::default());
        self.unlock_buf_hdr(buf_id, buf_state);
        // Remove the old mapping entry (caller holds the partition lock).
        let code = buf_table_hash_code(&old_tag);
        self.buf_table().delete(&old_tag, code)?;
        Ok(true)
    }

    /// `InvalidateBuffer(buf)` (bufmgr.c:2178) — mark a shared buffer invalid and
    /// return it to the freelist. The header spinlock must be HELD at entry (the
    /// observed `buf_state`); it is dropped before returning. Used by the
    /// drop-relation path.
    pub(crate) fn invalidate_buffer(&self, buf_id: usize, buf_state: u32) -> PgResult<()> {
        // Save the original buffer tag before dropping the spinlock (bufmgr.c:2187).
        let old_tag = self.desc_tag(buf_id);
        let part = tag_partition(&old_tag);
        // Release the header lock; we re-lock under the partition lock below.
        // (Assert(buf_state & BM_LOCKED); UnlockBufHdr — bufmgr.c:2190-2191.)
        self.unlock_buf_hdr(buf_id, buf_state);

        // retry: (bufmgr.c:2201) — loop until the buffer is evictable.
        loop {
            // Acquire exclusive mapping lock in preparation for changing the
            // buffer's association (bufmgr.c:2207).
            let guard = self.map_acquire(part, LWLockMode::LW_EXCLUSIVE)?;
            // Re-lock the buffer header (bufmgr.c:2210).
            let buf_state = self.lock_buf_hdr(buf_id);

            // If it's changed while we were waiting for lock, do nothing
            // (bufmgr.c:2213-2218).
            if self.desc_tag(buf_id) != old_tag {
                self.unlock_buf_hdr(buf_id, buf_state);
                guard.release()?;
                return Ok(());
            }

            // We assume the reason for it to be pinned is that either we were
            // asynchronously reading the page in before erroring out or someone
            // else is flushing the page out. Wait for the IO to finish, then
            // retry (bufmgr.c:2230-2239).
            if buf_state_get_refcount(buf_state) != 0 {
                self.unlock_buf_hdr(buf_id, buf_state);
                guard.release()?;
                // Safety check: should definitely not be our *own* pin
                // (bufmgr.c:2234-2236).
                if self.private_refcount().get(buf_id as i32) > 0 {
                    return Err(PgError::error("buffer is pinned in InvalidateBuffer"));
                }
                self.wait_io(buf_id)?;
                continue;
            }

            // Clear out the buffer's tag and flags. We must do this to ensure
            // that linear scans of the buffer array don't think the buffer is
            // valid (bufmgr.c:2241-2248).
            let old_flags = buf_state & BUF_FLAG_MASK;
            self.set_desc_tag(buf_id, buftag::default());
            let new_state = buf_state & !(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
            self.unlock_buf_hdr(buf_id, new_state);

            // Remove the buffer from the lookup hashtable, if it was in there
            // (bufmgr.c:2250-2254).
            if old_flags & BM_TAG_VALID != 0 {
                let code = buf_table_hash_code(&old_tag);
                self.buf_table().delete(&old_tag, code)?;
            }

            // Done with mapping lock (bufmgr.c:2259).
            guard.release()?;

            // Insert the buffer at the head of the list of free buffers
            // (bufmgr.c:2264).
            self.strategy_free_buffer(buf_id)?;
            return Ok(());
        }
    }

    // -- BufferAlloc (bufmgr.c:2000) ---------------------------------------

    /// `BufferAlloc(smgr, relpersistence, forkNum, blockNum, strategy, *foundPtr,
    /// io_context)` (bufmgr.c:2000) — the buffer-pool lookup-or-allocate core.
    /// Returns `(buf_id, found)` where `found` is the `*foundPtr` out-parameter
    /// (the block was already resident).
    pub(crate) fn buffer_alloc(
        &self,
        rlocator: RelFileLocatorBackend,
        relpersistence: u8,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        has_strategy: bool,
    ) -> PgResult<(usize, bool)> {
        // Make sure we will have room to remember the buffer pin (bufmgr.c:2014).
        sb::resowner_enlarge::call()?;
        self.private_refcount().ReservePrivateRefCountEntry();

        let new_tag = make_tag(rlocator, forknum, blocknum);
        let new_code = buf_table_hash_code(&new_tag);
        let new_part = buf_table_hash_partition(new_code);

        // See if the block is in the pool already (shared partition lock).
        let guard = self.map_acquire(new_part, LWLockMode::LW_SHARED)?;
        let existing = self.buf_table().lookup(&new_tag, new_code);
        if existing >= 0 {
            let buf_id = existing as usize;
            let valid = self.pin_buffer(buf_id, has_strategy);
            guard.release()?;
            return Ok((buf_id, valid));
        }
        guard.release()?;

        // Acquire a victim buffer.
        let victim = self.get_victim_buffer(has_strategy)?;

        // Try to insert under the new tag.
        let guard = self.map_acquire(new_part, LWLockMode::LW_EXCLUSIVE)?;
        let existing = self.buf_table().insert(new_tag, new_code, victim as i32)?;
        if existing >= 0 {
            // Collision: someone beat us to it. Give up the victim.
            // UnpinBuffer(victim_buf_hdr) (bufmgr.c:2767).
            self.unpin_buffer(victim);
            self.strategy_free_buffer(victim)?;
            let buf_id = existing as usize;
            let valid = self.pin_buffer(buf_id, has_strategy);
            guard.release()?;
            return Ok((buf_id, valid));
        }

        // Lock the header to change its tag.
        let mut victim_buf_state = self.lock_buf_hdr(victim);
        debug_assert_eq!(buf_state_get_refcount(victim_buf_state), 1);
        debug_assert_eq!(
            victim_buf_state & (BM_TAG_VALID | BM_VALID | BM_DIRTY | BM_IO_IN_PROGRESS),
            0
        );
        self.set_desc_tag(victim, new_tag);
        victim_buf_state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;
        if relpersistence == RELPERSISTENCE_PERMANENT || forknum == INIT_FORKNUM {
            victim_buf_state |= BM_PERMANENT;
        }
        self.unlock_buf_hdr(victim, victim_buf_state);
        guard.release()?;
        Ok((victim, false))
    }

    // -- buffer I/O lifecycle (bufmgr.c StartBufferIO/TerminateBufferIO/WaitIO) --

    /// `StartBufferIO(buf, forInput, nowait)` (bufmgr.c:6038) — begin I/O on a
    /// buffer, setting `BM_IO_IN_PROGRESS`. Returns true if we now own the I/O
    /// (must terminate it), false if it is already valid (someone else did it).
    ///
    /// `io_wref` carries the AIO wait reference to stamp onto this shared
    /// buffer's descriptor for the readv staging path (`buffer_stage_common`,
    /// bufmgr.c:6854). For a synchronous read (the F3 read-or-zero path) it is
    /// `None` and the field is left at its default invalid reference.
    pub(crate) fn start_buffer_io(
        &self,
        buf_id: usize,
        _nowait: bool,
        io_wref: Option<PgAioWaitRef>,
    ) -> PgResult<bool> {
        // Make sure we will have room to remember the buffer I/O (bufmgr.c:6042).
        sb::resowner_enlarge::call()?;
        loop {
            let buf_state = self.lock_buf_hdr(buf_id);
            if buf_state & BM_IO_IN_PROGRESS == 0 {
                if buf_state & BM_VALID != 0 {
                    // Someone else already read it in.
                    self.unlock_buf_hdr(buf_id, buf_state);
                    return Ok(false);
                }
                // Stamp the AIO wait reference while we hold the header lock so
                // the AIO subsystem can find the in-flight IO (bufmgr.c:6854).
                if let Some(io_ref) = io_wref {
                    self.set_io_wref(buf_id, io_ref);
                }
                self.unlock_buf_hdr(buf_id, buf_state | BM_IO_IN_PROGRESS);
                // ResourceOwnerRememberBufferIO(CurrentResourceOwner, ...) (bufmgr.c:6068).
                sb::remember_buffer_io::call(buf_id_to_buffer(buf_id as i32));
                return Ok(true);
            }
            // I/O already in progress: wait for it.
            self.unlock_buf_hdr(buf_id, buf_state);
            self.wait_io(buf_id)?;
        }
    }

    /// `TerminateBufferIO(buf, clear_dirty, set_flag_bits, forget_owner,
    /// release_aio)` (bufmgr.c:6095) — finish I/O: clear `BM_IO_IN_PROGRESS` /
    /// `BM_IO_ERROR`, apply `set_flag_bits`, optionally clear
    /// `BM_DIRTY | BM_CHECKPOINT_NEEDED` (only when not re-dirtied), and on
    /// `release_aio` drop the AIO subsystem's shared pin. After dropping the
    /// header lock: if `forget_owner`, release the buffer I/O from the current
    /// resource owner; broadcast the I/O condvar; and, only when `release_aio`
    /// and a `BM_PIN_COUNT_WAITER` is set, wake that waiter (bufmgr.c:6136-6137).
    pub(crate) fn terminate_buffer_io(
        &self,
        buf_id: usize,
        clear_dirty: bool,
        set_flag_bits: u32,
        forget_owner: bool,
        release_aio: bool,
    ) -> PgResult<()> {
        let mut buf_state = self.lock_buf_hdr(buf_id);
        debug_assert!(buf_state & BM_IO_IN_PROGRESS != 0);
        buf_state &= !BM_IO_IN_PROGRESS;

        // Clear earlier errors, if this IO failed, it'll be marked again.
        buf_state &= !BM_IO_ERROR;

        if clear_dirty && buf_state & BM_JUST_DIRTIED == 0 {
            buf_state &= !(BM_DIRTY | BM_CHECKPOINT_NEEDED);
        }

        if release_aio {
            // Release ownership by the AIO subsystem (bufmgr.c:6114-6118).
            debug_assert!(buf_state_get_refcount(buf_state) > 0);
            buf_state -= BUF_REFCOUNT_ONE;
            // pgaio_wref_clear(&buf->io_wref) (bufmgr.c:6116) — clear the
            // in-flight wait reference now that the AIO subsystem's ownership is
            // released. `io_wref` is a header-spinlock-protected field like `tag`.
            self.set_io_wref(buf_id, PgAioWaitRef::default());
        }

        buf_state |= set_flag_bits;
        self.unlock_buf_hdr(buf_id, buf_state);

        if forget_owner {
            // ResourceOwnerForgetBufferIO(CurrentResourceOwner, ...) (bufmgr.c:6122).
            sb::forget_buffer_io::call(buf_id_to_buffer(buf_id as i32));
        }

        // ConditionVariableBroadcast(BufferDescriptorGetIOCV(buf)) (bufmgr.c:6125).
        backend_storage_lmgr_condition_variable::ConditionVariableBroadcast(self.io_cv(buf_id));

        // We may have just released the last pin other than the waiter's
        // (bufmgr.c:6136-6137).
        if release_aio && buf_state & BM_PIN_COUNT_WAITER != 0 {
            self.wake_pin_count_waiter(buf_id);
        }
        Ok(())
    }

    /// `WaitIO(buf)` (bufmgr.c:5959) — wait for the in-progress I/O on a buffer
    /// to complete, riding the per-buffer I/O condition variable
    /// (`BufferDescriptorGetIOCV`).
    pub(crate) fn wait_io(&self, buf_id: usize) -> PgResult<()> {
        let cv = self.io_cv(buf_id);
        backend_storage_lmgr_condition_variable::ConditionVariablePrepareToSleep(cv);
        loop {
            // For now, just re-read the buffer state under the header lock; in a
            // real server this is a snapshot the broadcast wakes us to recheck.
            let buf_state = self.lock_buf_hdr(buf_id);
            self.unlock_buf_hdr(buf_id, buf_state);
            if buf_state & BM_IO_IN_PROGRESS == 0 {
                break;
            }
            backend_storage_lmgr_condition_variable::ConditionVariableSleep(cv, WAIT_EVENT_BUFFER_IO)?;
        }
        backend_storage_lmgr_condition_variable::ConditionVariableCancelSleep();
        Ok(())
    }
}
