use core::sync::atomic::Ordering;

use datum::Datum;
use init_small::globals;
use types_core::{Buffer, BufferIsValid};
use types_error::{ErrorLocation, PgResult, ERROR};
use types_resowner::{ResourceOwnerDesc, RELEASE_PRIO_BUFFER_PINS, RESOURCE_RELEASE_BEFORE_LOCKS};
use types_storage::buf::{
    BufferAccessStrategy, BM_LOCKED, BM_MAX_USAGE_COUNT, BM_PIN_COUNT_WAITER, BM_VALID,
    BUF_REFCOUNT_MASK, BUF_REFCOUNT_ONE, BUF_USAGECOUNT_MASK, BUF_USAGECOUNT_ONE,
};

use crate::buf_hdr::{
    BufferDesc, BufferDescriptorGetBuffer, GetBufferDescriptor, LockBufHdr, UnlockBufHdr,
    WaitBufHdrUnlocked,
};
use crate::privref::{self, GetPrivateRefCount};

// buffer_pin_resowner_desc (bufmgr.c): abort's ResourceOwnerRelease
// (BEFORE_LOCKS) drops pins the error path never reached an unpin for.
static BUFFER_PIN_DESC: ResourceOwnerDesc = ResourceOwnerDesc {
    name: "buffer pin",
    release_phase: RESOURCE_RELEASE_BEFORE_LOCKS,
    release_priority: RELEASE_PRIO_BUFFER_PINS,
    ReleaseResource: ResOwnerReleaseBufferPin,
    DebugPrint: Some(ResOwnerPrintBufferPin),
};

fn ResOwnerReleaseBufferPin(res: Datum) {
    let buffer = res.as_i32();
    assert!(BufferIsValid(buffer), "bad buffer ID: {buffer}");
    if buffer < 0 {
        crate::localbuf::UnpinLocalBufferNoOwner(buffer);
        return;
    }
    UnpinBufferNoOwner(GetBufferDescriptor(buffer - 1));
}

fn ResOwnerPrintBufferPin<'a>(mcx: mcx::Mcx<'a>, res: Datum) -> PgResult<mcx::PgString<'a>> {
    let buffer = res.as_i32();
    mcx::PgString::from_str_in(
        &format!("buffer {buffer} (refcount={})", GetPrivateRefCount(buffer)),
        mcx,
    )
}

#[inline]
pub(crate) fn RememberBufferPin(b: Buffer) {
    resowner::ResourceOwnerRemember(
        resowner::CurrentResourceOwner(),
        Datum::from_i32(b),
        &BUFFER_PIN_DESC,
    )
    .expect("ResourceOwnerRememberBuffer");
}

#[cfg(test)]
pub(crate) fn buffer_pin_desc() -> &'static ResourceOwnerDesc {
    &BUFFER_PIN_DESC
}

#[inline]
pub(crate) fn resowner_enlarge_for_pin() -> PgResult<()> {
    resowner::ResourceOwnerEnlarge(resowner::CurrentResourceOwner())
}

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
/// run ReservePrivateRefCountEntry and resowner_enlarge_for_pin.
//
// M2 swizzling decision site: under swizzling + optimistic latching a warm-hit
// pin becomes a version-validated read with zero atomics; this CAS (and the
// UnpinBuffer decrement) is what that replaces (docs/beat-postgres.md §7).
#[inline]
pub(crate) fn PinBuffer(desc: &BufferDesc, strategy: &BufferAccessStrategy) -> bool {
    let b = BufferDescriptorGetBuffer(desc);
    let already = privref::track_pin(b);
    if already > 0 {
        RememberBufferPin(b);
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
    RememberBufferPin(b);
    result
}

/// PinBuffer_Locked (bufmgr.c): pin while holding the header lock; the
/// refcount bump and the unlock are one release store. Caller has reserved a
/// private refcount entry.
///
/// C asserts there is no preexisting local pin, and so do we — but the
/// assertion is not what keeps the accounting straight, in C or here. The
/// shared bump below is unconditional (it is fused into the header unlock, so
/// unlike `PinBuffer` this function cannot branch on the private entry), so the
/// private entry it takes must be a fresh one that can be dropped on its own:
/// `privref::new_pin_entry`, C's `NewPrivateRefCountEntry` + `refcount++`,
/// which deliberately does not look for an existing entry. That is what makes
/// two shared bumps produce two shared drops even in a build where the
/// assertion does not exist. See `new_pin_entry`'s note.
#[inline]
pub(crate) fn PinBuffer_Locked(desc: &BufferDesc) {
    let b = BufferDescriptorGetBuffer(desc);
    debug_assert!(GetPrivateRefCount(b) == 0);
    let old_state = desc.state.load(Ordering::Relaxed);
    debug_assert!(old_state & BM_LOCKED != 0);
    UnlockBufHdr(desc, old_state + BUF_REFCOUNT_ONE);
    privref::new_pin_entry(b);
    RememberBufferPin(b);
}

// ResourceOwnerForgetBuffer failure (entry missing, or owner already
// releasing) must not panic: unpins run from drop guards during unwind, and a
// panic there aborts the process. WARN and continue so the shared refcount is
// still released.
#[inline]
pub(crate) fn ForgetBufferPin(b: Buffer) {
    if let Err(e) = resowner::ResourceOwnerForget(
        resowner::CurrentResourceOwner(),
        Datum::from_i32(b),
        &BUFFER_PIN_DESC,
    ) {
        let _ = elog::elog(
            types_error::WARNING,
            format!("ResourceOwnerForgetBuffer: buffer {b}: {e}"),
        );
    }
}

#[inline]
pub(crate) fn UnpinBuffer(desc: &BufferDesc) {
    ForgetBufferPin(BufferDescriptorGetBuffer(desc));
    UnpinBufferNoOwner(desc);
}

#[inline]
pub(crate) fn UnpinBufferNoOwner(desc: &BufferDesc) {
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

// WakePinCountWaiter (bufmgr.c): re-check under the header lock — another
// backend may have unpinned and woken the waiter already. Runs on every
// unpin, including abort-path resowner release, so it must never panic.
pub(crate) fn WakePinCountWaiter(desc: &BufferDesc) {
    let mut buf_state = LockBufHdr(desc);
    if buf_state & BM_PIN_COUNT_WAITER != 0 && buffer_refcount(buf_state) == 1 {
        let wait_procno = desc.wait_backend_pgprocno();
        buf_state &= !BM_PIN_COUNT_WAITER;
        UnlockBufHdr(desc, buf_state);
        if let Err(e) = lmgr_proc::ProcSendSignal(wait_procno) {
            let _ = elog::elog(
                types_error::WARNING,
                format!("could not wake pin count waiter {wait_procno}: {e}"),
            );
        }
    } else {
        UnlockBufHdr(desc, buf_state);
    }
}

pub fn ReleaseBuffer(buffer: Buffer) -> PgResult<()> {
    if !BufferIsValid(buffer) {
        return Err(bad_buffer_id(buffer, "ReleaseBuffer"));
    }
    if buffer < 0 {
        crate::localbuf::UnpinLocalBuffer(buffer);
        return Ok(());
    }
    UnpinBuffer(GetBufferDescriptor(buffer - 1));
    Ok(())
}

pub fn IncrBufferRefCount(buffer: Buffer) {
    assert!(BufferIsPinned(buffer), "buffer {buffer} is not pinned");
    resowner_enlarge_for_pin().expect("ResourceOwnerEnlarge");
    if buffer < 0 {
        crate::localbuf::incr_local_ref_count(buffer);
        RememberBufferPin(buffer);
        return;
    }
    privref::track_incr(buffer);
    RememberBufferPin(buffer);
}

pub fn BufferIsPinned(buffer: Buffer) -> bool {
    if !BufferIsValid(buffer) {
        return false;
    }
    if buffer < 0 {
        return crate::localbuf::local_ref_count(buffer) > 0;
    }
    GetPrivateRefCount(buffer) > 0
}

pub fn CheckBufferIsPinnedOnce(buffer: Buffer) -> PgResult<()> {
    let count = if buffer < 0 {
        crate::localbuf::local_ref_count(buffer)
    } else {
        GetPrivateRefCount(buffer)
    };
    if count != 1 {
        return Err(Box::new(
            types_error::PgError::new(ERROR, format!("incorrect local pin count: {count}"))
                .with_error_location(ErrorLocation::new(file!(), line!() as i32, "CheckBufferIsPinnedOnce")),
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

/// AtEOXact_Buffers (bufmgr.c): leak check only — pins remembered on the
/// resource owner were already dropped by ResourceOwnerRelease(BEFORE_LOCKS).
pub fn AtEOXact_Buffers(is_commit: bool) {
    // In-flight/uncollected uring prefetch reads hold thread-owned pins; wait
    // them out before the leak check (order: pinned pages can carry live DMA).
    crate::uring::drain_own();
    if cfg!(debug_assertions) {
        CheckForBufferLeaks();
    }
    debug_assert!(privref::overflow_count() == 0);
    crate::localbuf::AtEOXact_LocalBuffers(is_commit);
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, funcname)),
    )
}
