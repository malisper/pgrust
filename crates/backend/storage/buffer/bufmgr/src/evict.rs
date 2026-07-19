//! EvictUnpinnedBuffer / EvictAllUnpinnedBuffers / EvictRelUnpinnedBuffers
//! (bufmgr.c, PG17+): testing/development eviction used by contrib
//! pg_buffercache. Inherently racy by design — the buffer may be re-filled
//! (even by the same block) the moment we return; C documents the same.

use core::sync::atomic::Ordering;

use lwlock::{LWLockAcquire, LWLockRelease, LW_SHARED};
use types_core::Buffer;
use types_error::PgResult;
use types_storage::buf::{IOContext, BM_DIRTY, BM_VALID};
use types_storage::RelFileLocator;

use types_storage::buf::buftag;

use crate::buf_hdr::{BufferDesc, GetBufferDescriptor, LockBufHdr, NBuffersInited, UnlockBufHdr};
use crate::pin::{buffer_refcount, resowner_enlarge_for_pin, PinBuffer_Locked, UnpinBuffer};
use crate::privref::ReservePrivateRefCountEntry;
use crate::read::InvalidateVictimBuffer;

/// Per-buffer eviction outcome counters (C's out-params trio).
#[derive(Clone, Copy, Default)]
pub struct EvictCounts {
    pub evicted: i32,
    pub flushed: i32,
    pub skipped: i32,
}

// BufTagMatchesRelFileLocator (buf_internals.h).
#[inline]
fn tag_matches_locator(tag: &buftag, rl: &RelFileLocator) -> bool {
    tag.spcOid == rl.spcOid && tag.dbOid == rl.dbOid && tag.relNumber == rl.relNumber
}

/// `EvictUnpinnedBufferInternal` (bufmgr.c): caller holds the buffer header
/// lock (the `buf_state` word from `LockBufHdr`). Returns
/// (evicted, buffer_flushed). The header lock is always released.
fn evict_unpinned_buffer_internal(
    desc: &BufferDesc,
    buf_state: u32,
) -> PgResult<(bool, bool)> {
    let mut buffer_flushed = false;

    if buf_state & BM_VALID == 0 {
        UnlockBufHdr(desc, buf_state);
        return Ok((false, buffer_flushed));
    }

    // Check that it's not pinned already.
    if buffer_refcount(buf_state) > 0 {
        UnlockBufHdr(desc, buf_state);
        return Ok((false, buffer_flushed));
    }

    PinBuffer_Locked(desc); // releases the header lock

    let result = (|| -> PgResult<bool> {
        // If it was dirty, try to clean it once.
        if buf_state & BM_DIRTY != 0 {
            LWLockAcquire(
                &desc.content_lock,
                LW_SHARED,
                init_small::globals::MyProcNumber(),
            )?;
            let flush_result = crate::write::FlushBuffer(desc, IOContext::IOCONTEXT_NORMAL);
            buffer_flushed = true;
            LWLockRelease(&desc.content_lock)?;
            flush_result?;
        }

        // False if it became dirty again or someone else pinned it.
        InvalidateVictimBuffer(desc)
    })();

    UnpinBuffer(desc);

    Ok((result?, buffer_flushed))
}

/// `EvictUnpinnedBuffer` (bufmgr.c). Returns (evicted, buffer_flushed).
/// The caller validates the 1-based buffer id range.
pub fn EvictUnpinnedBuffer(buf: Buffer) -> PgResult<(bool, bool)> {
    debug_assert!(buf >= 1 && buf <= NBuffersInited());

    // Make sure we can pin the buffer.
    resowner_enlarge_for_pin()?;
    ReservePrivateRefCountEntry();

    let desc = GetBufferDescriptor(buf - 1);
    let buf_state = LockBufHdr(desc);
    evict_unpinned_buffer_internal(desc, buf_state)
}

/// `EvictAllUnpinnedBuffers` (bufmgr.c).
pub fn EvictAllUnpinnedBuffers() -> PgResult<EvictCounts> {
    let mut counts = EvictCounts::default();

    for id in 0..NBuffersInited() {
        let desc = GetBufferDescriptor(id);

        postgres_seams::check_for_interrupts::call()?;

        let buf_state = desc.state.load(Ordering::Relaxed);
        if buf_state & BM_VALID == 0 {
            continue;
        }

        resowner_enlarge_for_pin()?;
        ReservePrivateRefCountEntry();

        let buf_state = LockBufHdr(desc);
        let (evicted, flushed) = evict_unpinned_buffer_internal(desc, buf_state)?;
        if evicted {
            counts.evicted += 1;
        } else {
            counts.skipped += 1;
        }
        if flushed {
            counts.flushed += 1;
        }
    }

    Ok(counts)
}

/// `EvictRelUnpinnedBuffers` (bufmgr.c). The caller holds at least
/// AccessShareLock on the (shared-buffers) relation and passes its locator.
pub fn EvictRelUnpinnedBuffers(rlocator: &RelFileLocator) -> PgResult<EvictCounts> {
    let mut counts = EvictCounts::default();

    for id in 0..NBuffersInited() {
        let desc = GetBufferDescriptor(id);

        postgres_seams::check_for_interrupts::call()?;

        // C also prechecks &desc->tag without the header lock; that racy
        // UnsafeCell read violates BufferDesc::tag()'s pin-or-header-lock
        // contract here, so the state word is the only unlocked filter and
        // the tag test happens under the lock below.
        let buf_state = desc.state.load(Ordering::Relaxed);
        if buf_state & BM_VALID == 0 {
            continue;
        }

        // Make sure we can pin the buffer.
        resowner_enlarge_for_pin()?;
        ReservePrivateRefCountEntry();

        let buf_state = LockBufHdr(desc);

        // Recheck state, and test the tag, under the lock.
        if buf_state & BM_VALID == 0 || !tag_matches_locator(&desc.tag(), rlocator) {
            UnlockBufHdr(desc, buf_state);
            continue;
        }

        let (evicted, flushed) = evict_unpinned_buffer_internal(desc, buf_state)?;
        if evicted {
            counts.evicted += 1;
        } else {
            counts.skipped += 1;
        }
        if flushed {
            counts.flushed += 1;
        }
    }

    Ok(counts)
}
