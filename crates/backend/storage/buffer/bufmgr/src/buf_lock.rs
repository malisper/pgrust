//! Content-lock acquisition + cleanup (super-exclusive) locks + the hint-bit
//! dirty marking — the `bufmgr.c` content-lock surface (F1c).
//!
//! Ported function-by-function from `storage/buffer/bufmgr.c` (PG 18.3); branch
//! order, integer casts, locking order and atomic barriers preserved.
//!
//! Implemented here (line numbers are the C source):
//!  * `BufferIsExclusiveLocked` (2886)
//!  * `UnlockBuffers` (5572)
//!  * `LockBuffer` (5600)
//!  * `ConditionalLockBuffer` (5626)
//!  * `CheckBufferIsPinnedOnce` (5647)
//!  * `LockBufferForCleanup` (5680)
//!  * `HoldingBufferPinThatDelaysRecovery` (5822)
//!  * `ConditionalLockBufferForCleanup` (5848)
//!  * `IsBufferCleanupOK` (5906)
//!  * `MarkBufferDirtyHint` (5430)
//!
//! The per-buffer content lock is a real `LWLock` acquired DIRECTLY
//! (`LWLockAcquire(&content_locks[buf_id], mode)`) — there is no central content
//! seam. The buffer header `state` atom, the header spinlock, and the
//! spinlock-protected `wait_backend_pgprocno` are reached through [`mgr`]'s
//! in-crate primitives.
//!
//! Local (negative) buffers are owned by the separate `localbuf.c` subsystem
//! (`backend-storage-buffer-support`); the `BufferIsLocal` arms cross to it
//! through `backend-storage-buffer-support-seams`.
//!
//! Two deep legs stay panic-until-owner (sanctioned, per the F1c scope): the
//! `MarkBufferDirtyHint` WAL full-page-image leg's `XLogSaveBufferForHint`
//! (the owner `xloginsert` installs it; consumed across
//! `backend-access-transam-xloginsert-seams`) is wired live, but the
//! `LockBufferForCleanup` `InHotStandby` recovery-conflict wait is bundled into
//! a single bufmgr-defined outward seam installed by the recovery/standby owner
//! when it ports.

use types_core::primitive::{Buffer, XLogRecPtr};
use types_storage::buf::{
    BM_DIRTY, BM_JUST_DIRTIED, BM_PERMANENT, BM_PIN_COUNT_WAITER, BUFFER_LOCK_EXCLUSIVE,
    BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK,
};
use types_error::{PgError, PgResult};
use types_storage::storage::{LWLockMode, RelFileLocator};

use bufmgr_seams as sb;
use lwlock as lwlock;

use crate::mgr::BufferManager;

/// `InvalidXLogRecPtr` (xlogdefs.h).
const INVALID_XLOG_REC_PTR: XLogRecPtr = 0;

/// `BUF_STATE_GET_REFCOUNT(buf_state)` (buf_internals.h).
#[inline]
fn buf_state_get_refcount(buf_state: u32) -> u32 {
    buf_state & types_storage::buf::BUF_REFCOUNT_MASK
}

/// `BufferIsLocal(buffer)` (buf.h) — temp/local buffers carry a NEGATIVE handle.
#[inline]
pub(crate) fn buffer_is_local(buffer: Buffer) -> bool {
    buffer < 0
}

impl BufferManager {
    /// `BufferIsExclusiveLocked(buffer)` (bufmgr.c:2886) — does THIS backend hold
    /// the buffer's content lock in exclusive mode? The buffer must be pinned.
    pub fn BufferIsExclusiveLocked(&self, buffer: Buffer) -> PgResult<bool> {
        // Assert(BufferIsPinned(buffer));
        if buffer_is_local(buffer) {
            // Content locks are not maintained for local buffers.
            return Ok(true);
        }
        let buf_id = self.buffer_to_buf_id_pub(buffer)?;
        Ok(lwlock::LWLockHeldByMeInMode(
            self.content_lock(buf_id),
            LWLockMode::LW_EXCLUSIVE,
        ))
    }

    /// `LockBuffer(buffer, mode)` (bufmgr.c:5600) — acquire or release the
    /// buffer's content lock. `mode` is one of `BUFFER_LOCK_UNLOCK` /
    /// `BUFFER_LOCK_SHARE` / `BUFFER_LOCK_EXCLUSIVE`.
    pub fn LockBuffer(&self, buffer: Buffer, mode: i32) -> PgResult<()> {
        // Assert(BufferIsPinned(buffer));
        if buffer_is_local(buffer) {
            // local buffers need no lock.
            return Ok(());
        }
        let buf_id = self.buffer_to_buf_id_pub(buffer)?;
        let lock = self.content_lock(buf_id);
        if mode == BUFFER_LOCK_UNLOCK {
            lwlock::LWLockRelease(lock)?;
        } else if mode == BUFFER_LOCK_SHARE {
            lwlock::LWLockAcquire(
                lock,
                LWLockMode::LW_SHARED,
                lmgr_proc_seams::my_proc_number::call(),
            )?;
        } else if mode == BUFFER_LOCK_EXCLUSIVE {
            lwlock::LWLockAcquire(
                lock,
                LWLockMode::LW_EXCLUSIVE,
                lmgr_proc_seams::my_proc_number::call(),
            )?;
        } else {
            return Err(PgError::error(format!(
                "unrecognized buffer lock mode: {mode}"
            )));
        }
        Ok(())
    }

    /// `ConditionalLockBuffer(buffer)` (bufmgr.c:5626) — try to take the
    /// EXCLUSIVE content lock without waiting; returns whether it was acquired.
    pub fn ConditionalLockBuffer(&self, buffer: Buffer) -> PgResult<bool> {
        // Assert(BufferIsPinned(buffer));
        if buffer_is_local(buffer) {
            // act as though we got it.
            return Ok(true);
        }
        let buf_id = self.buffer_to_buf_id_pub(buffer)?;
        lwlock::LWLockConditionalAcquire(self.content_lock(buf_id), LWLockMode::LW_EXCLUSIVE)
    }

    /// `UnlockBuffers()` (bufmgr.c:5572) — release the in-progress PIN_COUNT
    /// request on the error/cleanup path (LWLockReleaseAll already dropped the
    /// content locks themselves).
    pub fn UnlockBuffers(&self) {
        let buf_id = self.pin_count_wait_buf().get();
        if buf_id >= 0 {
            let buf_id = buf_id as usize;
            let mut buf_state = self.lock_buf_hdr(buf_id);
            // Don't complain if the flag is not set; it could have been reset but
            // we got a cancel/die interrupt before getting the signal.
            if buf_state & BM_PIN_COUNT_WAITER != 0
                && self.wait_backend_pgprocno(buf_id) == lmgr_proc_seams::my_proc_number::call()
            {
                buf_state &= !BM_PIN_COUNT_WAITER;
            }
            self.unlock_buf_hdr(buf_id, buf_state);
            self.pin_count_wait_buf().set(-1);
        }
    }

    /// `CheckBufferIsPinnedOnce(buffer)` (bufmgr.c:5647) — verify THIS backend
    /// pins `buffer` exactly once (the sole-pinner precondition for a cleanup
    /// lock).
    pub fn CheckBufferIsPinnedOnce(&self, buffer: Buffer) -> PgResult<()> {
        if buffer_is_local(buffer) {
            let count = sb::local_ref_count::call(buffer)?;
            if count != 1 {
                return Err(PgError::error(format!("incorrect local pin count: {count}")));
            }
        } else {
            let buf_id = self.buffer_to_buf_id_pub(buffer)?;
            let count = self.private_refcount().get(buf_id as i32);
            if count != 1 {
                return Err(PgError::error(format!("incorrect local pin count: {count}")));
            }
        }
        Ok(())
    }

    /// `LockBufferForCleanup(buffer)` (bufmgr.c:5680) — acquire a cleanup
    /// (super-exclusive) lock: an exclusive content lock plus the observation
    /// that no other backend pins the buffer. Loops, registering
    /// `BM_PIN_COUNT_WAITER` and parking on the wait signal, until the shared
    /// refcount is 1.
    pub fn LockBufferForCleanup(&self, buffer: Buffer) -> PgResult<()> {
        // Assert(BufferIsPinned(buffer));
        // Assert(PinCountWaitBuf == NULL);
        debug_assert_eq!(
            self.pin_count_wait_buf().get(),
            -1,
            "LockBufferForCleanup: PinCountWaitBuf == NULL"
        );

        self.CheckBufferIsPinnedOnce(buffer)?;

        // Nobody else to wait for.
        if buffer_is_local(buffer) {
            return Ok(());
        }

        let buf_id = self.buffer_to_buf_id_pub(buffer)?;

        // Recovery-conflict bookkeeping, faithful to the C locals.
        let mut wait_start: i64 = 0;
        let mut waiting = false;
        let mut logged_recovery_conflict = false;

        loop {
            // Try to acquire lock.
            self.LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE)?;
            let mut buf_state = self.lock_buf_hdr(buf_id);

            debug_assert!(buf_state_get_refcount(buf_state) > 0);
            if buf_state_get_refcount(buf_state) == 1 {
                // Successfully acquired exclusive lock with pincount 1.
                self.unlock_buf_hdr(buf_id, buf_state);

                // Emit the log message if recovery conflict on buffer pin was
                // resolved but the startup process waited longer than
                // deadlock_timeout for it. Part of the deep standby leg (the
                // single bundling seam): only reachable after the standby leg
                // below set `logged_recovery_conflict`.
                if logged_recovery_conflict {
                    sb::lock_buffer_for_cleanup_recovery_wait::call(
                        buffer,
                        wait_start,
                        waiting,
                        logged_recovery_conflict,
                        true, /* resolved: emit the resolved-after-deadlock log */
                    )?;
                }

                if waiting {
                    // reset ps display to remove the suffix if we added one.
                    ps_status_seams::set_ps_display_remove_suffix::call();
                    waiting = false;
                }
                let _ = waiting;
                return Ok(());
            }
            // Failed, so mark myself as waiting for pincount 1.
            if buf_state & BM_PIN_COUNT_WAITER != 0 {
                self.unlock_buf_hdr(buf_id, buf_state);
                self.LockBuffer(buffer, BUFFER_LOCK_UNLOCK)?;
                return Err(PgError::error(
                    "multiple backends attempting to wait for pincount 1",
                ));
            }
            self.set_wait_backend_pgprocno(
                buf_id,
                lmgr_proc_seams::my_proc_number::call(),
            );
            self.pin_count_wait_buf().set(buf_id as i32);
            buf_state |= BM_PIN_COUNT_WAITER;
            self.unlock_buf_hdr(buf_id, buf_state);
            self.LockBuffer(buffer, BUFFER_LOCK_UNLOCK)?;

            // Wait to be signaled by UnpinBuffer().
            if sb::in_hot_standby::call() {
                // The whole InHotStandby recovery-conflict wait (ps-display
                // suffix, deadlock-timeout logging, publish-bufid, alarm + park,
                // reset bufid) is a deep standby/startup leg. It is bundled into
                // ONE bufmgr-defined outward seam, installed by the recovery
                // owner when it ports (panic-until-owner — sanctioned). It
                // returns the updated (wait_start, waiting, logged) locals.
                let (ws, w, lrc) = sb::lock_buffer_for_cleanup_recovery_wait_park::call(
                    buffer,
                    wait_start,
                    waiting,
                    logged_recovery_conflict,
                )?;
                wait_start = ws;
                waiting = w;
                logged_recovery_conflict = lrc;
            } else {
                lmgr_proc_seams::proc_wait_for_signal::call(WAIT_EVENT_BUFFER_PIN)?;
            }

            // Remove flag marking us as waiter. Normally already cleared, but
            // ProcWaitForSignal() can return for other signals too. Only reset
            // the flag if we are still the registered waiter.
            buf_state = self.lock_buf_hdr(buf_id);
            if (buf_state & BM_PIN_COUNT_WAITER) != 0
                && self.wait_backend_pgprocno(buf_id)
                    == lmgr_proc_seams::my_proc_number::call()
            {
                buf_state &= !BM_PIN_COUNT_WAITER;
            }
            self.unlock_buf_hdr(buf_id, buf_state);

            self.pin_count_wait_buf().set(-1);
            // Loop back and try again.
        }
    }

    /// `HoldingBufferPinThatDelaysRecovery()` (bufmgr.c:5822) — true if THIS
    /// backend pins the buffer the startup process published as the one it
    /// waits on (a recovery-conflict cancellation predicate).
    pub fn HoldingBufferPinThatDelaysRecovery(&self) -> bool {
        let bufid = sb::startup_buffer_pin_wait_buf_id::call();

        // If the bufid is not set (woken slowly / spurious interrupt), do
        // nothing.
        if bufid < 0 {
            return false;
        }

        // GetPrivateRefCount(bufid + 1) > 0
        self.private_refcount().get(bufid) > 0
    }

    /// `ConditionalLockBufferForCleanup(buffer)` (bufmgr.c:5848) — like
    /// [`Self::LockBufferForCleanup`] but never blocks: take the exclusive
    /// content lock only if it is immediately grantable AND the shared refcount
    /// is 1.
    pub fn ConditionalLockBufferForCleanup(&self, buffer: Buffer) -> PgResult<bool> {
        // Assert(BufferIsValid(buffer));
        if buffer_is_local(buffer) {
            let refcount = sb::local_ref_count::call(buffer)?;
            // There should be exactly one pin.
            debug_assert!(refcount > 0);
            if refcount != 1 {
                return Ok(false);
            }
            // Nobody else to wait for.
            return Ok(true);
        }

        // There should be exactly one local pin.
        let buf_id = self.buffer_to_buf_id_pub(buffer)?;
        let refcount = self.private_refcount().get(buf_id as i32);
        debug_assert!(refcount != 0);
        if refcount != 1 {
            return Ok(false);
        }

        // Try to acquire lock.
        if !self.ConditionalLockBuffer(buffer)? {
            return Ok(false);
        }

        let buf_state = self.lock_buf_hdr(buf_id);
        let refcount = buf_state_get_refcount(buf_state);

        debug_assert!(refcount > 0);
        if refcount == 1 {
            // Successfully acquired exclusive lock with pincount 1.
            self.unlock_buf_hdr(buf_id, buf_state);
            return Ok(true);
        }

        // Failed, so release the lock.
        self.unlock_buf_hdr(buf_id, buf_state);
        self.LockBuffer(buffer, BUFFER_LOCK_UNLOCK)?;
        Ok(false)
    }

    /// `IsBufferCleanupOK(buffer)` (bufmgr.c:5906) — we already hold the
    /// exclusive content lock; report whether it happens to be a cleanup lock
    /// (shared refcount 1).
    pub fn IsBufferCleanupOK(&self, buffer: Buffer) -> PgResult<bool> {
        // Assert(BufferIsValid(buffer));
        if buffer_is_local(buffer) {
            // There should be exactly one pin.
            if sb::local_ref_count::call(buffer)? != 1 {
                return Ok(false);
            }
            // Nobody else to wait for.
            return Ok(true);
        }

        // There should be exactly one local pin.
        let buf_id = self.buffer_to_buf_id_pub(buffer)?;
        if self.private_refcount().get(buf_id as i32) != 1 {
            return Ok(false);
        }

        // caller must hold exclusive lock on buffer.
        debug_assert!(
            lwlock::LWLockHeldByMeInMode(self.content_lock(buf_id), LWLockMode::LW_EXCLUSIVE),
            "IsBufferCleanupOK: caller must hold the exclusive content lock"
        );

        let buf_state = self.lock_buf_hdr(buf_id);

        debug_assert!(buf_state_get_refcount(buf_state) > 0);
        if buf_state_get_refcount(buf_state) == 1 {
            // pincount is OK.
            self.unlock_buf_hdr(buf_id, buf_state);
            return Ok(true);
        }

        self.unlock_buf_hdr(buf_id, buf_state);
        Ok(false)
    }

    /// `MarkBufferDirtyHint(buffer, buffer_std)` (bufmgr.c:5430) — mark a buffer
    /// dirty for a hint-bit-only change. The caller holds only a SHARE (or
    /// exclusive) content lock, writes no WAL itself, and dirtying is
    /// best-effort. When checksums (`XLogHintBitIsNeeded`) require it, an
    /// `XLOG_FPI_FOR_HINT` full-page image is logged first, bracketed by the
    /// checkpoint-delay flag.
    pub fn MarkBufferDirtyHint(&self, buffer: Buffer, buffer_std: bool) -> PgResult<()> {
        if !self.buffer_is_valid(buffer) && !buffer_is_local(buffer) {
            return Err(PgError::error(format!("bad buffer ID: {buffer}")));
        }

        if buffer_is_local(buffer) {
            // MarkLocalBufferDirty(buffer); return;
            return sb::mark_local_buffer_dirty::call(buffer);
        }

        let buf_id = self.buffer_to_buf_id_pub(buffer)?;

        debug_assert!(
            self.private_refcount().get(buf_id as i32) > 0,
            "MarkBufferDirtyHint: GetPrivateRefCount(buffer) > 0"
        );
        // here, either share or exclusive lock is OK:
        // Assert(LWLockHeldByMe(BufferDescriptorGetContentLock(bufHdr)));
        debug_assert!(
            lwlock::LWLockHeldByMe(self.content_lock(buf_id)),
            "MarkBufferDirtyHint: caller must hold a content lock"
        );

        // Fast path: if the status bits already look set, don't take the
        // spinlock. Unlocked read — a missed just-cleared flag is harmless
        // here (this routine is only used where a lost dirty is acceptable).
        if (self.read_state(buf_id) & (BM_DIRTY | BM_JUST_DIRTIED))
            != (BM_DIRTY | BM_JUST_DIRTIED)
        {
            let mut lsn: XLogRecPtr = INVALID_XLOG_REC_PTR;
            let mut dirtied = false;
            let mut delay_chkpt_flags = false;

            // WAL-log a full page image to protect the hint update from torn
            // writes, iff this is the first change since the last checkpoint.
            if transam_xlog_seams::xlog_hint_bit_is_needed::call()
                && (self.read_state(buf_id) & BM_PERMANENT) != 0
            {
                // If we must not write WAL (in recovery, or a
                // relfilelocator-specific skip), don't dirty the page: set the
                // hint but let it be lost on eviction/shutdown.
                // BufTagGetRelFileLocator(&bufHdr->tag) (buf_internals.h).
                let tag = self.desc_tag(buf_id);
                let rlocator = RelFileLocator {
                    spcOid: tag.spcOid,
                    dbOid: tag.dbOid,
                    relNumber: tag.relNumber,
                };
                if transam_xlog_seams::recovery_in_progress::call()
                    || catalog_storage_seams::rel_file_locator_skipping_wal::call(rlocator)
                {
                    return Ok(());
                }

                // We must issue the WAL record before marking the buffer dirty,
                // serialised against checkpoints via DELAY_CHKPT_START.
                // Assert((MyProc->delayChkptFlags & DELAY_CHKPT_START) == 0);
                lmgr_proc_seams::my_proc_set_delay_chkpt_start::call(true);
                delay_chkpt_flags = true;
                lsn = xloginsert_seams::xlog_save_buffer_for_hint::call(
                    buffer, buffer_std,
                )?;
            }

            let mut buf_state = self.lock_buf_hdr(buf_id);

            debug_assert!(buf_state_get_refcount(buf_state) > 0);

            if buf_state & BM_DIRTY == 0 {
                dirtied = true; // "will be dirtied by this action"

                // Set the page LSN if we wrote a backup block. Setting it while
                // holding the header lock makes share-lock readers obtain the
                // header lock before PageGetLSN (BufferGetLSNAtomic).
                if lsn != INVALID_XLOG_REC_PTR {
                    self.with_block_mut(buf_id, |block| {
                        let mut page = page::PageMut::new(block)
                            .expect("buffer block is BLCKSZ");
                        page::PageSetLSN(&mut page, lsn);
                    });
                }
            }

            buf_state |= BM_DIRTY | BM_JUST_DIRTIED;
            self.unlock_buf_hdr(buf_id, buf_state);

            if delay_chkpt_flags {
                lmgr_proc_seams::my_proc_set_delay_chkpt_start::call(false);
            }

            if dirtied {
                // pgBufferUsage.shared_blks_dirtied++; VacuumCostBalance bump
                // (VacuumCostActive) — pure accounting via the pgstat seam.
                sb::count_buffer_dirtied::call();
            }
        }
        Ok(())
    }
}

/// `WAIT_EVENT_BUFFER_PIN` (PG_WAIT_BUFFERPIN class) — the wait-event id parked
/// on in the non-standby `LockBufferForCleanup` path.
const WAIT_EVENT_BUFFER_PIN: u32 = 0x0400_0000;

#[cfg(test)]
mod tests {
    use super::*;

    fn install_stubs() {
        crate::mgr::test_seams::install();
    }

    fn mk() -> BufferManager {
        BufferManager::new(8)
    }

    /// `CheckBufferIsPinnedOnce` succeeds at exactly one pin and errors at 0 or 2.
    #[test]
    fn check_pinned_once_requires_exactly_one() {
        install_stubs();
        let bm = mk();
        let _ = bm.pin_buffer_for_test(0, false);
        // One pin -> OK (Buffer 1 == buf_id 0).
        assert!(bm.CheckBufferIsPinnedOnce(1).is_ok());
        // Two pins -> error.
        let _ = bm.pin_buffer_for_test(0, false);
        let err = bm.CheckBufferIsPinnedOnce(1).unwrap_err();
        assert!(format!("{err:?}").contains("incorrect local pin count"));
    }

    /// A bad (zero) buffer id is rejected by `MarkBufferDirtyHint`.
    #[test]
    fn mark_dirty_hint_rejects_invalid_buffer() {
        install_stubs();
        let bm = mk();
        let err = bm.MarkBufferDirtyHint(0, true).unwrap_err();
        assert!(format!("{err:?}").contains("bad buffer ID"));
    }

    /// `LockBuffer` rejects an unrecognized mode.
    #[test]
    fn lock_buffer_rejects_bad_mode() {
        install_stubs();
        let bm = mk();
        let _ = bm.pin_buffer_for_test(0, false);
        let err = bm.LockBuffer(1, 99).unwrap_err();
        assert!(format!("{err:?}").contains("unrecognized buffer lock mode"));
    }

    /// Share-lock acquire then unlock round-trips on the direct content lock.
    #[test]
    fn lock_buffer_share_then_unlock() {
        install_stubs();
        let bm = mk();
        let _ = bm.pin_buffer_for_test(2, false);
        bm.LockBuffer(3, BUFFER_LOCK_SHARE).unwrap();
        bm.LockBuffer(3, BUFFER_LOCK_UNLOCK).unwrap();
    }

    /// `ConditionalLockBuffer` acquires the exclusive content lock when free.
    #[test]
    fn conditional_lock_buffer_acquires_when_free() {
        install_stubs();
        let bm = mk();
        let _ = bm.pin_buffer_for_test(1, false);
        assert!(bm.ConditionalLockBuffer(2).unwrap());
        // Exclusive-locked now.
        assert!(bm.BufferIsExclusiveLocked(2).unwrap());
        bm.LockBuffer(2, BUFFER_LOCK_UNLOCK).unwrap();
    }

    /// `IsBufferCleanupOK` is true for a sole pin under an exclusive lock and
    /// false when pinned twice.
    #[test]
    fn is_buffer_cleanup_ok_sole_pin() {
        install_stubs();
        let bm = mk();
        let _ = bm.pin_buffer_for_test(0, false);
        bm.LockBuffer(1, BUFFER_LOCK_EXCLUSIVE).unwrap();
        assert!(bm.IsBufferCleanupOK(1).unwrap());
        // Second pin -> not cleanup-OK.
        let _ = bm.pin_buffer_for_test(0, false);
        assert!(!bm.IsBufferCleanupOK(1).unwrap());
        bm.LockBuffer(1, BUFFER_LOCK_UNLOCK).unwrap();
    }

    /// `ConditionalLockBufferForCleanup` returns true for a sole pin and false
    /// when this backend pins twice (refcount-mismatch path, no lock taken).
    #[test]
    fn conditional_cleanup_sole_pin() {
        install_stubs();
        let bm = mk();
        let _ = bm.pin_buffer_for_test(4, false);
        assert!(bm.ConditionalLockBufferForCleanup(5).unwrap());
        bm.LockBuffer(5, BUFFER_LOCK_UNLOCK).unwrap();
        // Two private pins -> immediate false (no content lock attempted).
        let _ = bm.pin_buffer_for_test(4, false);
        assert!(!bm.ConditionalLockBufferForCleanup(5).unwrap());
    }
}
