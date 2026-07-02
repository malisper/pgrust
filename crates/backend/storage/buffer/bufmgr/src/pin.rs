use core::sync::atomic::Ordering;

use init_small::globals;
use types_core::{Buffer, BufferIsValid};
use types_error::{ErrorLocation, PgResult, ERROR};
use types_storage::buf::{
    BufferAccessStrategy, BM_LOCKED, BM_MAX_USAGE_COUNT, BM_PIN_COUNT_WAITER, BM_VALID,
    BUF_REFCOUNT_MASK, BUF_REFCOUNT_ONE, BUF_USAGECOUNT_MASK, BUF_USAGECOUNT_ONE,
};

use crate::buf_hdr::{
    BufferDesc, BufferDescriptorGetBuffer, GetBufferDescriptor, LockBufHdr, UnlockBufHdr,
    WaitBufHdrUnlocked,
};
use crate::privref::{self, GetPrivateRefCount, ReservePrivateRefCountEntry};

#[inline]
pub(crate) fn buffer_usagecount(state: u32) -> u32 {
    (state & BUF_USAGECOUNT_MASK) >> 18
}

#[inline]
pub(crate) fn buffer_refcount(state: u32) -> u32 {
    state & BUF_REFCOUNT_MASK
}

/// PinBuffer (bufmgr.c): the PG9.6 single-atomic pin — one CAS on the header
/// word, usage bump fused in; returns whether the buffer is valid. Caller has
/// run ReservePrivateRefCountEntry (+ resowner enlarge, pre-resowner a no-op).
//
// M2 swizzling decision site: under swizzling + optimistic latching a warm-hit
// pin becomes a version-validated read with zero atomics; this CAS (and the
// UnpinBuffer decrement) is what that replaces (docs/beat-postgres.md §7).
pub(crate) fn PinBuffer(desc: &BufferDesc, strategy: &BufferAccessStrategy) -> bool {
    let b = BufferDescriptorGetBuffer(desc);
    let already = privref::track_pin(b);
    if already > 0 {
        return desc.state.load(Ordering::Acquire) & BM_VALID != 0;
    }

    let result;
    let mut old = desc.state.load(Ordering::Acquire);
    loop {
        if old & BM_LOCKED != 0 {
            old = WaitBufHdrUnlocked(desc);
        }
        let mut new = old + BUF_REFCOUNT_ONE;
        match strategy {
            None => {
                if buffer_usagecount(old) < BM_MAX_USAGE_COUNT {
                    new += BUF_USAGECOUNT_ONE;
                }
            }
            Some(_) => {
                if buffer_usagecount(old) == 0 {
                    new += BUF_USAGECOUNT_ONE;
                }
            }
        }
        match desc
            .state
            .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                result = new & BM_VALID != 0;
                break;
            }
            Err(v) => old = v,
        }
    }
    result
}

/// PinBuffer_Locked (bufmgr.c): pin while holding the header lock; the
/// refcount bump and the unlock are one release store. Caller guarantees no
/// preexisting local pin and has reserved a private refcount entry.
pub(crate) fn PinBuffer_Locked(desc: &BufferDesc) {
    let b = BufferDescriptorGetBuffer(desc);
    debug_assert!(GetPrivateRefCount(b) == 0);
    let old_state = desc.state.load(Ordering::Relaxed);
    debug_assert!(old_state & BM_LOCKED != 0);
    UnlockBufHdr(desc, old_state + BUF_REFCOUNT_ONE);
    let prev = privref::track_pin(b);
    debug_assert!(prev == 0);
}

/// UnpinBuffer / UnpinBufferNoOwner (bufmgr.c).
pub(crate) fn UnpinBuffer(desc: &BufferDesc) {
    let b = BufferDescriptorGetBuffer(desc);
    if !privref::track_unpin(b) {
        return;
    }
    let mut old = desc.state.load(Ordering::Acquire);
    let buf_state;
    loop {
        if old & BM_LOCKED != 0 {
            old = WaitBufHdrUnlocked(desc);
        }
        debug_assert!(buffer_refcount(old) > 0);
        let new = old - BUF_REFCOUNT_ONE;
        // C note kept: no atomic sub — the header-lock holder writes state
        // with a plain store, so lock-free updates must CAS on unlocked values.
        match desc
            .state
            .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                buf_state = new;
                break;
            }
            Err(v) => old = v,
        }
    }
    if buf_state & BM_PIN_COUNT_WAITER != 0 {
        WakePinCountWaiter(desc);
    }
}

/// WakePinCountWaiter (bufmgr.c).
fn WakePinCountWaiter(desc: &BufferDesc) {
    let mut buf_state = LockBufHdr(desc);
    if buf_state & BM_PIN_COUNT_WAITER != 0 && buffer_refcount(buf_state) == 1 {
        let _wait_procno = desc.wait_backend_pgprocno();
        buf_state &= !BM_PIN_COUNT_WAITER;
        UnlockBufHdr(desc, buf_state);
        panic!(
            "unported callee reached from bufmgr.c WakePinCountWaiter: ProcSendSignal (storage/lmgr/proc.c)"
        );
    }
    UnlockBufHdr(desc, buf_state);
}

/// ReleaseBuffer (bufmgr.c).
pub fn ReleaseBuffer(buffer: Buffer) -> PgResult<()> {
    if !BufferIsValid(buffer) {
        return Err(bad_buffer_id(buffer, "ReleaseBuffer"));
    }
    if buffer < 0 {
        panic!("unported callee reached from bufmgr.c ReleaseBuffer: UnpinLocalBuffer (localbuf.c)");
    }
    UnpinBuffer(GetBufferDescriptor(buffer - 1));
    Ok(())
}

/// IncrBufferRefCount (bufmgr.c).
pub fn IncrBufferRefCount(buffer: Buffer) {
    assert!(BufferIsPinned(buffer), "buffer {buffer} is not pinned");
    if buffer < 0 {
        panic!("unported callee reached from bufmgr.c IncrBufferRefCount: LocalRefCount (localbuf.c)");
    }
    privref::track_incr(buffer);
}

/// BufferIsPinned (bufmgr.c macro; shared-buffer arm).
pub fn BufferIsPinned(buffer: Buffer) -> bool {
    if !BufferIsValid(buffer) {
        return false;
    }
    if buffer < 0 {
        panic!("unported callee reached from bufmgr.c BufferIsPinned: LocalRefCount (localbuf.c)");
    }
    GetPrivateRefCount(buffer) > 0
}

/// CheckBufferIsPinnedOnce (bufmgr.c).
pub fn CheckBufferIsPinnedOnce(buffer: Buffer) -> PgResult<()> {
    if buffer < 0 {
        panic!(
            "unported callee reached from bufmgr.c CheckBufferIsPinnedOnce: LocalRefCount (localbuf.c)"
        );
    }
    let count = GetPrivateRefCount(buffer);
    if count != 1 {
        return Err(Box::new(
            types_error::PgError::new(ERROR, format!("incorrect local pin count: {count}"))
                .with_error_location(ErrorLocation::new("bufmgr.c", 0, "CheckBufferIsPinnedOnce")),
        ));
    }
    Ok(())
}

thread_local! {
    static PIN_COUNT_WAIT_BUF: core::cell::Cell<i32> = const { core::cell::Cell::new(-1) };
}

pub(crate) fn set_pin_count_wait_buf(buf_id: i32) {
    PIN_COUNT_WAIT_BUF.with(|c| c.set(buf_id));
}

pub(crate) fn pin_count_wait_buf() -> i32 {
    PIN_COUNT_WAIT_BUF.with(|c| c.get())
}

/// UnlockBuffers (bufmgr.c): error-path cleanup of a pending pin-count wait.
pub fn UnlockBuffers() {
    let buf_id = pin_count_wait_buf();
    if buf_id >= 0 {
        let desc = GetBufferDescriptor(buf_id);
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

/// AtEOXact_Buffers (bufmgr.c): leak check; pre-resowner the private refcount
/// ledger is authoritative, so leaked pins surface here as warnings.
pub fn AtEOXact_Buffers(_is_commit: bool) {
    if cfg!(debug_assertions) {
        CheckForBufferLeaks();
    }
    debug_assert!(privref::overflow_count() == 0);
}

fn CheckForBufferLeaks() {
    let mut refcount_errors = 0;
    privref::for_each_held(|buffer, refcount| {
        let _ = elog::elog(
            types_error::WARNING,
            format!("buffer refcount leak: [{buffer}] (refcount={refcount})"),
        );
        refcount_errors += 1;
    });
    debug_assert!(refcount_errors == 0, "buffer refcount leaks detected");
}

#[cold]
pub(crate) fn bad_buffer_id(buffer: Buffer, funcname: &'static str) -> Box<types_error::PgError> {
    Box::new(
        types_error::PgError::new(ERROR, format!("bad buffer ID: {buffer}"))
            .with_error_location(ErrorLocation::new("bufmgr.c", 0, funcname)),
    )
}

pub(crate) use ReservePrivateRefCountEntry as reserve_entry;
