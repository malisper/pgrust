use core::sync::atomic::Ordering;

use init_small::globals;
use lwlock::{
    LWLockAcquire, LWLockConditionalAcquire, LWLockHeldByMeInMode, LWLockRelease, LW_EXCLUSIVE,
    LW_SHARED,
};
use types_core::{BlockNumber, Buffer, BufferIsValid, XLogRecPtr, BLCKSZ};
use types_error::{ErrorLocation, PgResult, ERROR};
use types_storage::buf::{
    buftag, BM_DIRTY, BM_JUST_DIRTIED, BM_LOCKED, BM_PIN_COUNT_WAITER,
};
use types_storage::bufpage::PageRef;

use crate::buf_hdr::{
    BufferDesc, BufferGetBlockPtr, GetBufferDescriptor, LockBufHdr, UnlockBufHdr,
    WaitBufHdrUnlocked,
};
use crate::counters;
use crate::pin::{
    bad_buffer_id, buffer_refcount, pin_count_wait_buf, set_pin_count_wait_buf, BufferIsPinned,
    CheckBufferIsPinnedOnce, ReleaseBuffer,
};

const PG_WAIT_BUFFERPIN: u32 = 0x0400_0000;

pub const BUFFER_LOCK_UNLOCK: i32 = 0;
pub const BUFFER_LOCK_SHARE: i32 = 1;
pub const BUFFER_LOCK_EXCLUSIVE: i32 = 2;

#[inline]
fn shared_desc(buffer: Buffer) -> &'static BufferDesc {
    debug_assert!(buffer > 0);
    GetBufferDescriptor(buffer - 1)
}

pub fn LockBuffer(buffer: Buffer, mode: i32) -> PgResult<()> {
    debug_assert!(BufferIsPinned(buffer));
    if buffer < 0 {
        return Ok(());
    }
    let desc = shared_desc(buffer);
    match mode {
        BUFFER_LOCK_UNLOCK => LWLockRelease(&desc.content_lock),
        BUFFER_LOCK_SHARE => {
            LWLockAcquire(&desc.content_lock, LW_SHARED, globals::MyProcNumber()).map(|_| ())
        }
        BUFFER_LOCK_EXCLUSIVE => {
            LWLockAcquire(&desc.content_lock, LW_EXCLUSIVE, globals::MyProcNumber()).map(|_| ())
        }
        _ => Err(Box::new(
            types_error::PgError::new(ERROR, format!("unrecognized buffer lock mode: {mode}"))
                .with_error_location(ErrorLocation::new(file!(), line!() as i32, "LockBuffer")),
        )),
    }
}

pub fn ConditionalLockBuffer(buffer: Buffer) -> PgResult<bool> {
    debug_assert!(BufferIsPinned(buffer));
    if buffer < 0 {
        return Ok(true);
    }
    LWLockConditionalAcquire(&shared_desc(buffer).content_lock, LW_EXCLUSIVE)
}

/// LockBufferForCleanup (bufmgr.c): loops until it holds the exclusive
/// content lock with pincount 1; waits on ProcWaitForSignal, woken by
/// UnpinBuffer's WakePinCountWaiter.
pub fn LockBufferForCleanup(buffer: Buffer) -> PgResult<()> {
    debug_assert!(BufferIsPinned(buffer));
    debug_assert!(pin_count_wait_buf() == -1);
    if buffer >= 0 && crate::privref::GetPrivateRefCount(buffer) != 1 {
        // Uncollected uring prefetch reads hold thread-owned pins (AtEOXact
        // precedent); wait them out before the pinned-once check.
        crate::uring::drain_own();
    }
    CheckBufferIsPinnedOnce(buffer)?;
    if buffer < 0 {
        return Ok(());
    }
    let desc = shared_desc(buffer);
    loop {
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE)?;
        let mut buf_state = LockBufHdr(desc);
        debug_assert!(buffer_refcount(buf_state) > 0);
        if buffer_refcount(buf_state) == 1 {
            UnlockBufHdr(desc, buf_state);
            return Ok(());
        }
        if buf_state & BM_PIN_COUNT_WAITER != 0 {
            UnlockBufHdr(desc, buf_state);
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK)?;
            return Err(Box::new(
                types_error::PgError::new(
                    ERROR,
                    "multiple backends attempting to wait for pincount 1",
                )
                .with_error_location(ErrorLocation::new(file!(), line!() as i32, "LockBufferForCleanup")),
            ));
        }
        // SAFETY: header lock held.
        unsafe { desc.set_wait_backend_pgprocno(globals::MyProcNumber()) };
        set_pin_count_wait_buf(desc.buf_id);
        buf_state |= BM_PIN_COUNT_WAITER;
        UnlockBufHdr(desc, buf_state);
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK)?;
        if xlogutils_seams::in_hot_standby::is_installed()
            && xlogutils_seams::in_hot_standby::call()
        {
            // Startup-process arm: on error PIN_COUNT_WAIT_BUF stays set so
            // the abort path's UnlockBuffers clears BM_PIN_COUNT_WAITER.
            lmgr_proc::SetStartupBufferPinWaitBufId(buffer - 1);
            let waited = standby_seams::resolve_recovery_conflict_with_buffer_pin::call();
            lmgr_proc::SetStartupBufferPinWaitBufId(-1);
            waited?;
        } else {
            lmgr_proc::ProcWaitForSignal(PG_WAIT_BUFFERPIN);
        }
        // ProcWaitForSignal can return on unrelated latch sets; clear the
        // waiter flag only if it is still ours, then retry.
        let mut buf_state = LockBufHdr(desc);
        if buf_state & BM_PIN_COUNT_WAITER != 0
            && desc.wait_backend_pgprocno() == globals::MyProcNumber()
        {
            buf_state &= !BM_PIN_COUNT_WAITER;
        }
        UnlockBufHdr(desc, buf_state);
        set_pin_count_wait_buf(-1);
    }
}

pub fn ConditionalLockBufferForCleanup(buffer: Buffer) -> PgResult<bool> {
    debug_assert!(BufferIsValid(buffer));
    debug_assert!(pin_count_wait_buf() == -1);
    if buffer < 0 {
        let refcount = crate::localbuf::local_ref_count(buffer);
        debug_assert!(refcount > 0);
        return Ok(refcount == 1);
    }
    if crate::privref::GetPrivateRefCount(buffer) != 1 {
        crate::uring::drain_own();
        if crate::privref::GetPrivateRefCount(buffer) != 1 {
            return Ok(false);
        }
    }
    if !ConditionalLockBuffer(buffer)? {
        return Ok(false);
    }
    let desc = shared_desc(buffer);
    let buf_state = LockBufHdr(desc);
    debug_assert!(buffer_refcount(buf_state) > 0);
    if buffer_refcount(buf_state) == 1 {
        UnlockBufHdr(desc, buf_state);
        return Ok(true);
    }
    UnlockBufHdr(desc, buf_state);
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK)?;
    Ok(false)
}

/// IsBufferCleanupOK (bufmgr.c): caller holds pin + exclusive content lock.
pub fn IsBufferCleanupOK(buffer: Buffer) -> bool {
    debug_assert!(BufferIsValid(buffer));
    if buffer < 0 {
        panic!("unported callee reached from bufmgr.c IsBufferCleanupOK: LocalRefCount (localbuf.c)");
    }
    debug_assert!(BufferIsPinned(buffer));
    let desc = shared_desc(buffer);
    debug_assert!(LWLockHeldByMeInMode(&desc.content_lock, LW_EXCLUSIVE));
    let buf_state = LockBufHdr(desc);
    debug_assert!(buffer_refcount(buf_state) > 0);
    let ok = buffer_refcount(buf_state) == 1 && crate::privref::GetPrivateRefCount(buffer) == 1;
    UnlockBufHdr(desc, buf_state);
    ok
}

pub fn UnlockReleaseBuffer(buffer: Buffer) -> PgResult<()> {
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK)?;
    ReleaseBuffer(buffer)
}

pub fn MarkBufferDirty(buffer: Buffer) -> PgResult<()> {
    if !BufferIsValid(buffer) {
        return Err(bad_buffer_id(buffer, "MarkBufferDirty"));
    }
    if buffer < 0 {
        crate::localbuf::MarkLocalBufferDirty(buffer);
        return Ok(());
    }
    let desc = shared_desc(buffer);
    debug_assert!(BufferIsPinned(buffer));
    debug_assert!(LWLockHeldByMeInMode(&desc.content_lock, LW_EXCLUSIVE));

    let mut old = desc.state.load(Ordering::Acquire);
    let old_buf_state;
    loop {
        if old & BM_LOCKED != 0 {
            old = WaitBufHdrUnlocked(desc);
        }
        debug_assert!(buffer_refcount(old) > 0);
        let new = old | BM_DIRTY | BM_JUST_DIRTIED;
        match desc
            .state
            .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                old_buf_state = old;
                break;
            }
            Err(v) => old = v,
        }
    }
    if old_buf_state & BM_DIRTY == 0 {
        counters::dirtied();
        if globals::VacuumCostActive() {
            globals::SetVacuumCostBalance(
                globals::VacuumCostBalance() + globals::VacuumCostPageDirty(),
            );
        }
    }
    Ok(())
}

pub fn BufferGetBlockNumber(buffer: Buffer) -> BlockNumber {
    debug_assert!(BufferIsPinned(buffer));
    if buffer < 0 {
        return crate::localbuf::local_desc(buffer).tag().blockNum;
    }
    shared_desc(buffer).tag().blockNum
}

pub fn BufferGetTag(buffer: Buffer) -> buftag {
    debug_assert!(BufferIsPinned(buffer));
    if buffer < 0 {
        return crate::localbuf::local_desc(buffer).tag();
    }
    shared_desc(buffer).tag()
}

/// BufferGetPage: raw page pointer, valid while the caller's pin is held.
pub fn BufferGetPagePtr(buffer: Buffer) -> core::ptr::NonNull<u8> {
    debug_assert!(BufferIsPinned(buffer));
    if buffer < 0 {
        let p = crate::localbuf::local_block_ptr(buffer);
        debug_assert!(!p.is_null());
        // SAFETY: pinned local buffers have their block storage allocated.
        return unsafe { core::ptr::NonNull::new_unchecked(p) };
    }
    // SAFETY: pool pointer is non-null for a valid buffer id.
    unsafe { core::ptr::NonNull::new_unchecked(BufferGetBlockPtr(buffer)) }
}

/// Pin-scoped read view (types_storage PageRef is the vocabulary heapam
/// consumes).
pub fn buffer_page_ref<'p>(buffer: Buffer) -> PageRef<'p> {
    // SAFETY: caller holds a pin for 'p (BufferIsPinned asserted inside
    // BufferGetPagePtr); page images are BLCKSZ and MAXALIGNed.
    unsafe { PageRef::from_raw(BufferGetPagePtr(buffer)) }
}

pub fn buffer_page_is_new(buffer: Buffer) -> bool {
    buffer_page_ref(buffer).is_new()
}

// pd_lsn is two u32 halves (PageXLogRecPtr); written only under the content
// lock, read under pin — raw-ptr access, never a &mut over the page.
pub fn buffer_page_get_lsn(buffer: Buffer) -> XLogRecPtr {
    let p = BufferGetPagePtr(buffer).as_ptr();
    // SAFETY: pinned page image; 4-aligned reads at offsets 0 and 4.
    unsafe {
        let xlogid = p.cast::<u32>().read();
        let xrecoff = p.add(4).cast::<u32>().read();
        ((xlogid as u64) << 32) | xrecoff as u64
    }
}

pub fn buffer_page_set_lsn(buffer: Buffer, lsn: XLogRecPtr) {
    let p = BufferGetPagePtr(buffer).as_ptr();
    // SAFETY: caller holds the exclusive content lock (C PageSetLSN contract).
    unsafe {
        p.cast::<u32>().write((lsn >> 32) as u32);
        p.add(4).cast::<u32>().write(lsn as u32);
    }
}

/// RestoreBlockImage's memcpy onto BufferGetPage (seam surface for xlogutils).
pub fn overwrite_buffer_page(buffer: Buffer, page: &[u8]) {
    assert!(page.len() <= BLCKSZ);
    let p = BufferGetPagePtr(buffer).as_ptr();
    // SAFETY: pinned + caller holds the exclusive content lock during redo.
    unsafe { core::ptr::copy_nonoverlapping(page.as_ptr(), p, page.len()) };
}

