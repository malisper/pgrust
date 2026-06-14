#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `backend-storage-ipc-shm-mq` — single-reader, single-writer shared-memory
//! message queue (`src/backend/storage/ipc/shm_mq.c`).
//!
//! Both the sender and the receiver must have a `PGPROC`; their respective
//! process latches are used for synchronization. Only the sender may send,
//! and only the receiver may receive.
//!
//! The `shm_mq` header and its trailing `mq_ring` buffer live inside a
//! shared-memory (DSM or main-shmem) segment: the in-segment layout is the
//! `#[repr(C)]` [`InSegmentShmMq`] placed at a caller-chosen address, and
//! [`ShmMq`] is the owned wrapper replacing the C `*mut shm_mq`. The shared
//! parts use the real shared-memory primitives — the in-segment
//! [`Spinlock`] for `mq_mutex`, 8-byte atomics for the byte counters, an
//! `AtomicBool` for `mq_detached` — never `std::sync`. The sender/receiver
//! identities (`PGPROC *` in C) are stored as [`ProcNumber`]s with
//! [`INVALID_PROC_NUMBER`] for NULL; waking the peer resolves the PGPROC's
//! latch by number through the proc/latch seams.
//!
//! The backend-private `shm_mq_handle` is NOT shmem; it is [`ShmMqHandle`],
//! owned by this backend. Its reassembly buffer (`mqh_buffer`,
//! `MemoryContextAlloc`'d in `mqh_context` in C) is a `PgVec` in the
//! caller-provided `Mcx` captured at attach time. C reads of the ambient
//! `MyLatch` become the `my_latch` handle threaded through attach into the
//! handle state.

use core::mem::{offset_of, size_of};
use core::ptr::{self, NonNull};
use core::sync::atomic::{compiler_fence, fence, AtomicBool, AtomicI32, AtomicU64, Ordering};

use backend_postmaster_bgworker_seams::get_background_worker_pid;
use backend_storage_ipc_dsm_core::dsm::{cancel_on_dsm_detach, on_dsm_detach, DsmSegmentId};
use backend_storage_ipc_latch_seams::{reset_latch, set_latch, wait_latch};
use backend_storage_lmgr_proc_seams::proc_latch;
use backend_storage_lmgr_s_lock::{s_lock_macro, s_unlock, Spinlock};
use backend_tcop_postgres_seams::check_for_interrupts;
use backend_utils_error::ereport;
use mcx::{Mcx, PgBox, PgVec, MAX_ALLOC_SIZE};
use types_bgworker::{BackgroundWorkerHandle, BgwHandleStatus};
use types_core::{ProcNumber, Size, INVALID_PROC_NUMBER};
use types_datum::Datum;
use types_error::{
    ErrorLocation, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR,
};
use types_pgstat::wait_event::{
    WAIT_EVENT_MESSAGE_QUEUE_INTERNAL, WAIT_EVENT_MESSAGE_QUEUE_RECEIVE,
    WAIT_EVENT_MESSAGE_QUEUE_SEND,
};
use types_storage::latch::LatchHandle;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET};

fn loc(funcname: &str) -> ErrorLocation {
    ErrorLocation::new("shm_mq.c", 0, funcname)
}

/// `MyProcNumber` (globals.c) — this backend's pgprocno, the identity the C
/// compares against the queue's stored `PGPROC *`s.
fn my_proc_number() -> ProcNumber {
    backend_utils_init_small_seams::my_proc_number::call()
}

/// `MAXIMUM_ALIGNOF` on the 64-bit target.
pub const MAXIMUM_ALIGNOF: Size = 8;

/// `MaxAllocSize` (utils/memutils.h) — the largest single palloc.
pub const MaxAllocSize: Size = MAX_ALLOC_SIZE;

/// `MQH_INITIAL_BUFSIZE`.
pub const MQH_INITIAL_BUFSIZE: Size = 8192;

/// `MAXALIGN(LEN)`.
#[inline]
const fn MAXALIGN(len: Size) -> Size {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `MAXALIGN_DOWN(LEN)`.
#[inline]
const fn MAXALIGN_DOWN(len: Size) -> Size {
    len & !(MAXIMUM_ALIGNOF - 1)
}

/// `Min(a, b)`.
#[inline]
fn min_size(a: Size, b: Size) -> Size {
    if a < b {
        a
    } else {
        b
    }
}

/// `pg_nextpower2_size_t` (`port/pg_bitutils.h`) — smallest power of two
/// >= `num` (caller guarantees `num >= 1`).
#[inline]
fn pg_nextpower2_size_t(num: Size) -> Size {
    debug_assert!(num >= 1);
    if num <= 1 {
        return 1;
    }
    let bits = Size::BITS - (num - 1).leading_zeros();
    1usize << bits
}

// ---------------------------------------------------------------------------
// In-segment layout (repr(C); lives inside the shared segment).
// ---------------------------------------------------------------------------

/// `struct shm_mq` (shm_mq.c) as it lives at the head of the segment, up to
/// (but not including) the flexible `mq_ring[]` byte buffer.
///
/// Synchronization (from the C file comment): `mq_receiver`/`mq_bytes_read`
/// change only via the receiver, `mq_sender`/`mq_bytes_written` only via the
/// sender. The identities are protected by `mq_mutex` but cannot change once
/// set, so they may be read without the lock once known set. The byte
/// counters are 8-byte atomics ordered against `mq_ring` access by explicit
/// barriers. `mq_detached` only ever goes false→true and needs no lock.
/// `mq_ring_size`/`mq_ring_offset` never change after initialization.
#[repr(C)]
struct InSegmentShmMq {
    /// `slock_t mq_mutex`.
    mq_mutex: Spinlock,
    /// `PGPROC *mq_receiver` — the receiver's `ProcNumber`, or
    /// `INVALID_PROC_NUMBER` for NULL (set once, under the lock).
    mq_receiver: AtomicI32,
    /// `PGPROC *mq_sender` — likewise for the sender.
    mq_sender: AtomicI32,
    /// `pg_atomic_uint64 mq_bytes_read`.
    mq_bytes_read: AtomicU64,
    /// `pg_atomic_uint64 mq_bytes_written`.
    mq_bytes_written: AtomicU64,
    /// `Size mq_ring_size`.
    mq_ring_size: Size,
    /// `bool mq_detached`.
    mq_detached: AtomicBool,
    /// `uint8 mq_ring_offset` — padding from `mq_ring` to the MAXALIGN'd ring.
    mq_ring_offset: u8,
    /// `char mq_ring[FLEXIBLE_ARRAY_MEMBER]`.
    mq_ring: [u8; 0],
}

/// `offsetof(shm_mq, mq_ring)`.
#[inline]
const fn mq_ring_member_offset() -> Size {
    offset_of!(InSegmentShmMq, mq_ring)
}

/// `shm_mq_minimum_size` — enough for the header and at least one chunk.
pub const fn shm_mq_minimum_size() -> Size {
    MAXALIGN(mq_ring_member_offset()) + MAXIMUM_ALIGNOF
}

// ---------------------------------------------------------------------------
// The queue handle (replaces the C `shm_mq *`).
// ---------------------------------------------------------------------------

/// A handle to a `shm_mq` living at the head of a shared-memory segment.
/// Borrows the segment (does not own it); all access goes through the
/// in-segment [`InSegmentShmMq`] header and the trailing ring.
#[derive(Clone, Copy, Debug)]
pub struct ShmMq {
    base: NonNull<u8>,
}

// The handle is a borrow of a shared segment whose cross-process
// synchronization is the in-segment spinlock + atomics' responsibility.
unsafe impl Send for ShmMq {}

impl ShmMq {
    /// Wrap a queue header already living at `base`.
    ///
    /// # Safety
    ///
    /// `base` must point at a live `InSegmentShmMq` for the segment's
    /// lifetime.
    #[inline]
    pub unsafe fn from_base(base: NonNull<u8>) -> Self {
        Self { base }
    }

    /// The segment base address (the queue's identity, as
    /// `PointerGetDatum(mq)`).
    #[inline]
    pub fn base(&self) -> NonNull<u8> {
        self.base
    }

    #[inline]
    fn header_ptr(&self) -> *mut InSegmentShmMq {
        self.base.as_ptr().cast::<InSegmentShmMq>()
    }

    /// Shared view of the in-segment header.
    ///
    /// # Safety
    ///
    /// The segment must remain mapped and contain a valid `InSegmentShmMq`.
    #[inline]
    unsafe fn header(&self) -> &InSegmentShmMq {
        &*self.header_ptr()
    }

    #[inline]
    fn lock(&self) -> &Spinlock {
        // SAFETY: mq_mutex is part of the live in-segment header.
        unsafe { &self.header().mq_mutex }
    }

    /// `pg_atomic_read_u64(&mq->mq_bytes_read)`.
    #[inline]
    fn bytes_read(&self) -> u64 {
        // SAFETY: part of the live in-segment header.
        unsafe { self.header().mq_bytes_read.load(Ordering::Relaxed) }
    }

    /// `pg_atomic_read_u64(&mq->mq_bytes_written)`.
    #[inline]
    fn bytes_written(&self) -> u64 {
        // SAFETY: part of the live in-segment header.
        unsafe { self.header().mq_bytes_written.load(Ordering::Relaxed) }
    }

    /// `mq->mq_ring_size`.
    #[inline]
    fn ring_size(&self) -> Size {
        // SAFETY: immutable after create; part of the live header.
        unsafe { self.header().mq_ring_size }
    }

    /// `mq->mq_detached`.
    #[inline]
    fn detached(&self) -> bool {
        // SAFETY: part of the live header.
        unsafe { self.header().mq_detached.load(Ordering::Relaxed) }
    }

    /// `mq->mq_detached = true`.
    #[inline]
    fn set_detached(&self) {
        // SAFETY: part of the live header.
        unsafe { self.header().mq_detached.store(true, Ordering::Relaxed) };
    }

    /// `mq->mq_receiver`.
    #[inline]
    fn receiver(&self) -> ProcNumberOrNull {
        // SAFETY: part of the live header.
        ProcNumberOrNull(unsafe { self.header().mq_receiver.load(Ordering::Relaxed) })
    }

    /// `mq->mq_sender`.
    #[inline]
    fn sender(&self) -> ProcNumberOrNull {
        // SAFETY: part of the live header.
        ProcNumberOrNull(unsafe { self.header().mq_sender.load(Ordering::Relaxed) })
    }

    /// Raw pointer to `mq->mq_ring[mq->mq_ring_offset + offset]`.
    ///
    /// # Safety
    ///
    /// The segment must hold a ring of at least `mq_ring_offset + offset + 1`
    /// bytes following the header.
    #[inline]
    unsafe fn ring_ptr(&self, offset: Size) -> *mut u8 {
        let ring_offset = self.header().mq_ring_offset as Size;
        self.base
            .as_ptr()
            .add(mq_ring_member_offset() + ring_offset + offset)
    }

    /// `SpinLockAcquire(&mq->mq_mutex)`, returning an RAII release guard.
    fn spin_lock(&self, func: &'static str) -> MqSpinGuard<'_> {
        s_lock_macro(self.lock(), Some(file!()), line!() as i32, Some(func));
        MqSpinGuard { lock: self.lock() }
    }
}

/// A `ProcNumber` slot that may be unset (`INVALID_PROC_NUMBER` stands in for
/// the C NULL `PGPROC *`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProcNumberOrNull(ProcNumber);

impl ProcNumberOrNull {
    #[inline]
    fn is_null(self) -> bool {
        self.0 == INVALID_PROC_NUMBER
    }

    #[inline]
    fn get(self) -> Option<ProcNumber> {
        if self.is_null() {
            None
        } else {
            Some(self.0)
        }
    }
}

/// RAII guard for the in-segment `mq_mutex`: `SpinLockRelease` on drop.
struct MqSpinGuard<'a> {
    lock: &'a Spinlock,
}

impl Drop for MqSpinGuard<'_> {
    fn drop(&mut self) {
        s_unlock(self.lock);
    }
}

/// `SetLatch(&GetPGProcByNumber(procno)->procLatch)`.
fn set_proc_latch(procno: ProcNumber) {
    set_latch::call(proc_latch::call(procno));
}

// ---------------------------------------------------------------------------
// The backend-private handle (`shm_mq_handle`). NOT shmem.
// ---------------------------------------------------------------------------

/// Backend-private handle for access to a queue (`shm_mq_handle`).
///
/// `mqh_buffer`/`mqh_buflen` collapse into the optional `PgVec` (its length
/// is the C `mqh_buflen`). `my_latch` is the C ambient `MyLatch` threaded as
/// explicit handle state captured at attach time.
pub struct ShmMqHandle<'mcx> {
    mqh_queue: ShmMq,
    mqh_segment: Option<DsmSegmentId>,
    mqh_handle: Option<BackgroundWorkerHandle>,
    /// `mqh_buffer` + `mqh_buflen` — local reassembly buffer (NULL until
    /// first needed).
    mqh_buffer: Option<PgVec<'mcx, u8>>,
    mqh_consume_pending: Size,
    mqh_send_pending: Size,
    mqh_partial_bytes: Size,
    mqh_expected_bytes: Size,
    mqh_length_word_complete: bool,
    mqh_counterparty_attached: bool,
    /// `mqh_context` — the context buffer growth allocates in.
    mqh_context: Mcx<'mcx>,
    /// This backend's process latch (C `MyLatch`).
    my_latch: LatchHandle,
}

// ---------------------------------------------------------------------------
// Results and iovec (`shm_mq.h`).
// ---------------------------------------------------------------------------

/// Possible results of a send or receive operation (`shm_mq_result`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum shm_mq_result {
    /// Sent or received a message.
    SHM_MQ_SUCCESS,
    /// Not completed; retry later.
    SHM_MQ_WOULD_BLOCK,
    /// Other process has detached queue.
    SHM_MQ_DETACHED,
}

pub use shm_mq_result::*;

/// Descriptor for one source location of a gathered write (`shm_mq_iovec`).
#[derive(Clone, Copy, Debug)]
pub struct shm_mq_iovec<'a> {
    pub data: &'a [u8],
}

impl<'a> shm_mq_iovec<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    /// `iov.len`.
    #[inline]
    fn len(&self) -> Size {
        self.data.len()
    }
}

/* ============================ public API ============================ */

/// Initialize a new shared message queue (`shm_mq_create`).
///
/// # Safety
///
/// `address` must point to at least `size` bytes of writable, MAXALIGN'd
/// shared memory that outlives the returned handle, with
/// `size > MAXALIGN(offsetof(shm_mq, mq_ring))`.
pub unsafe fn shm_mq_create(address: NonNull<u8>, size: Size) -> ShmMq {
    let mq = ShmMq { base: address };
    let data_offset = MAXALIGN(mq_ring_member_offset());

    // If the size isn't MAXALIGN'd, just discard the odd bytes.
    let size = MAXALIGN_DOWN(size);

    // Queue size must be large enough to hold some data.
    debug_assert!(size > data_offset);

    // Initialize queue header (SpinLockInit + field init).
    ptr::write(
        mq.header_ptr(),
        InSegmentShmMq {
            mq_mutex: Spinlock::new(),
            mq_receiver: AtomicI32::new(INVALID_PROC_NUMBER),
            mq_sender: AtomicI32::new(INVALID_PROC_NUMBER),
            mq_bytes_read: AtomicU64::new(0),
            mq_bytes_written: AtomicU64::new(0),
            mq_ring_size: size - data_offset,
            mq_detached: AtomicBool::new(false),
            mq_ring_offset: (data_offset - mq_ring_member_offset()) as u8,
            mq_ring: [],
        },
    );

    mq
}

/// Set the identity of the receiving process (`shm_mq_set_receiver`).
pub fn shm_mq_set_receiver(mq: ShmMq, proc: ProcNumber) {
    let guard = mq.spin_lock("shm_mq_set_receiver");
    debug_assert!(mq.receiver().is_null());
    // SAFETY: header is live; we hold the spinlock.
    unsafe { mq.header().mq_receiver.store(proc, Ordering::Relaxed) };
    let sender = mq.sender();
    drop(guard);

    if let Some(sender) = sender.get() {
        set_proc_latch(sender);
    }
}

/// Set the identity of the sending process (`shm_mq_set_sender`).
pub fn shm_mq_set_sender(mq: ShmMq, proc: ProcNumber) {
    let guard = mq.spin_lock("shm_mq_set_sender");
    debug_assert!(mq.sender().is_null());
    // SAFETY: header is live; we hold the spinlock.
    unsafe { mq.header().mq_sender.store(proc, Ordering::Relaxed) };
    let receiver = mq.receiver();
    drop(guard);

    if let Some(receiver) = receiver.get() {
        set_proc_latch(receiver);
    }
}

/// Get the configured receiver (`shm_mq_get_receiver`).
pub fn shm_mq_get_receiver(mq: ShmMq) -> Option<ProcNumber> {
    let guard = mq.spin_lock("shm_mq_get_receiver");
    let receiver = mq.receiver();
    drop(guard);
    receiver.get()
}

/// Get the configured sender (`shm_mq_get_sender`).
pub fn shm_mq_get_sender(mq: ShmMq) -> Option<ProcNumber> {
    let guard = mq.spin_lock("shm_mq_get_sender");
    let sender = mq.sender();
    drop(guard);
    sender.get()
}

/// Attach to a shared message queue so we can send or receive messages
/// (`shm_mq_attach`).
///
/// `mcx` is the C `CurrentMemoryContext` at the call (`mqh_context`): the
/// context that should last at least as long as the queue is used; buffer
/// growth allocates there. If `seg` is provided, the queue is automatically
/// detached when that DSM segment is detached; the accompanying
/// `Mcx<'static>` is the `TopMemoryContext` handle the dsm unit allocates
/// its callback record in (its OOM is the `Err`). If `handle` is provided,
/// the queue can be read or written even before the other process has
/// attached. `my_latch` is this backend's process latch (C `MyLatch`),
/// used by the blocking paths.
pub fn shm_mq_attach<'mcx>(
    mq: ShmMq,
    mcx: Mcx<'mcx>,
    seg: Option<(DsmSegmentId, Mcx<'static>)>,
    handle: Option<BackgroundWorkerHandle>,
    my_latch: LatchHandle,
) -> PgResult<PgBox<'mcx, ShmMqHandle<'mcx>>> {
    debug_assert!({
        let me = my_proc_number();
        mq.receiver().get() == Some(me) || mq.sender().get() == Some(me)
    });

    // C `palloc`s the `shm_mq_handle`; the owned box is the equivalent here.
    // The DSM auto-detach callback (registered below) only ever *flags* the
    // queue detached through `mq`'s base address — it must not touch this
    // backend-local box. The box's `Drop` owns the actual free
    // (`shm_mq_detach` consumes it; see the struct comment / OPTION (i)).
    let mqh = mcx::alloc_in(
        mcx,
        ShmMqHandle {
            mqh_queue: mq,
            mqh_segment: seg.map(|(s, _)| s),
            mqh_handle: handle,
            mqh_buffer: None,
            mqh_consume_pending: 0,
            mqh_send_pending: 0,
            mqh_partial_bytes: 0,
            mqh_expected_bytes: 0,
            mqh_length_word_complete: false,
            mqh_counterparty_attached: false,
            mqh_context: mcx,
            my_latch,
        },
    )?;

    if let Some((seg_id, top_mcx)) = seg {
        on_dsm_detach(
            seg_id,
            shm_mq_detach_callback,
            Datum::from_usize(mq.base.as_ptr() as usize),
            top_mcx,
        )?;
    }

    Ok(mqh)
}

/// Associate a `BackgroundWorkerHandle` with a handle just as if it had been
/// passed to `shm_mq_attach` (`shm_mq_set_handle`).
pub fn shm_mq_set_handle(mqh: &mut ShmMqHandle<'_>, handle: BackgroundWorkerHandle) {
    debug_assert!(mqh.mqh_handle.is_none());
    mqh.mqh_handle = Some(handle);
}

/// Get the shm_mq from a handle (`shm_mq_get_queue`).
pub fn shm_mq_get_queue(mqh: &ShmMqHandle<'_>) -> ShmMq {
    mqh.mqh_queue
}

/// Write a message into a shared message queue (`shm_mq_send`).
///
/// # Safety
///
/// `mqh` must wrap a live, initialized, attached `shm_mq`.
pub unsafe fn shm_mq_send(
    mqh: &mut ShmMqHandle<'_>,
    data: &[u8],
    nowait: bool,
    force_flush: bool,
) -> PgResult<shm_mq_result> {
    let iov = [shm_mq_iovec::new(data)];
    shm_mq_sendv(mqh, &iov, nowait, force_flush)
}

/// Write a message gathered from multiple addresses (`shm_mq_sendv`).
///
/// When `nowait` is false, we wait on our process latch when the ring buffer
/// fills up. When `nowait` is true and the buffer becomes full we return
/// `SHM_MQ_WOULD_BLOCK`; the caller should retry with the same arguments
/// each time the process latch is set (once begun, the sending of a message
/// cannot be aborted except by detaching). When `force_flush` is true, we
/// immediately update `mq_bytes_written` and notify the receiver; otherwise
/// not until we have written more than 1/4 of the ring.
///
/// # Safety
///
/// `mqh` must wrap a live, initialized, attached `shm_mq`.
pub unsafe fn shm_mq_sendv(
    mqh: &mut ShmMqHandle<'_>,
    iov: &[shm_mq_iovec<'_>],
    nowait: bool,
    force_flush: bool,
) -> PgResult<shm_mq_result> {
    let mq = mqh.mqh_queue;
    let iovcnt = iov.len();
    let mut which_iov = 0usize;

    debug_assert!(mq.sender().get() == Some(my_proc_number()));

    // Compute total size of write.
    let mut nbytes: Size = 0;
    for v in iov {
        nbytes += v.len();
    }

    // Prevent writing messages overwhelming the receiver.
    if nbytes > MaxAllocSize {
        ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!(
                "cannot send a message of size {nbytes} via shared memory queue"
            ))
            .finish(loc("shm_mq_sendv"))?;
        return Ok(SHM_MQ_DETACHED); // unreachable
    }

    // Try to write, or finish writing, the length word into the buffer.
    while !mqh.mqh_length_word_complete {
        debug_assert!(mqh.mqh_partial_bytes < size_of::<Size>());
        let nbytes_bytes = nbytes.to_ne_bytes();
        let (res, bytes_written) = shm_mq_send_bytes(
            mqh,
            size_of::<Size>() - mqh.mqh_partial_bytes,
            &nbytes_bytes[mqh.mqh_partial_bytes..],
            nowait,
        )?;

        if res == SHM_MQ_DETACHED {
            // Reset state in case caller tries to send another message.
            mqh.mqh_partial_bytes = 0;
            mqh.mqh_length_word_complete = false;
            return Ok(res);
        }
        mqh.mqh_partial_bytes += bytes_written;

        if mqh.mqh_partial_bytes >= size_of::<Size>() {
            debug_assert!(mqh.mqh_partial_bytes == size_of::<Size>());
            mqh.mqh_partial_bytes = 0;
            mqh.mqh_length_word_complete = true;
        }

        if res != SHM_MQ_SUCCESS {
            return Ok(res);
        }

        // Length word can't be split unless bigger than required alignment.
        debug_assert!(mqh.mqh_length_word_complete || size_of::<Size>() > MAXIMUM_ALIGNOF);
    }

    // Write the actual data bytes into the buffer.
    //
    // The C loop is a do/while whose `continue`s jump to the
    // `mqh_partial_bytes < nbytes` controlling expression, so every branch
    // (the iov-advance, tmpbuf, and chunk paths) is followed by that check.
    debug_assert!(mqh.mqh_partial_bytes <= nbytes);
    let mut offset = mqh.mqh_partial_bytes;
    loop {
        // Figure out which bytes need to be sent next.
        if offset >= iov[which_iov].len() {
            offset -= iov[which_iov].len();
            which_iov += 1;
            if which_iov >= iovcnt {
                break;
            }
        } else if which_iov + 1 < iovcnt && offset + MAXIMUM_ALIGNOF > iov[which_iov].len() {
            // We want to avoid copying the data if at all possible, but every
            // chunk of bytes we write into the queue has to be MAXALIGN'd,
            // except the last. Thus, if a chunk other than the last one ends
            // on a non-MAXALIGN'd boundary, combine the tail end of its data
            // with data from one or more following chunks until we either
            // reach the last chunk or accumulate a MAXALIGN'd number of
            // bytes.
            let mut tmpbuf = [0u8; MAXIMUM_ALIGNOF];
            let mut j = 0usize;

            loop {
                if offset < iov[which_iov].len() {
                    tmpbuf[j] = iov[which_iov].data[offset];
                    j += 1;
                    offset += 1;
                    if j == MAXIMUM_ALIGNOF {
                        break;
                    }
                } else {
                    offset -= iov[which_iov].len();
                    which_iov += 1;
                    if which_iov >= iovcnt {
                        break;
                    }
                }
            }

            let (res, bytes_written) = shm_mq_send_bytes(mqh, j, &tmpbuf[..j], nowait)?;

            if res == SHM_MQ_DETACHED {
                // Reset state in case caller tries to send another message.
                mqh.mqh_partial_bytes = 0;
                mqh.mqh_length_word_complete = false;
                return Ok(res);
            }

            mqh.mqh_partial_bytes += bytes_written;
            if res != SHM_MQ_SUCCESS {
                return Ok(res);
            }
        } else {
            // If this is the last chunk, we can write all the data, even if
            // it isn't a multiple of MAXIMUM_ALIGNOF. Otherwise,
            // MAXALIGN_DOWN the write size.
            let mut chunksize = iov[which_iov].len() - offset;
            if which_iov + 1 < iovcnt {
                chunksize = MAXALIGN_DOWN(chunksize);
            }
            let (res, bytes_written) =
                shm_mq_send_bytes(mqh, chunksize, &iov[which_iov].data[offset..], nowait)?;

            if res == SHM_MQ_DETACHED {
                // Reset state in case caller tries to send another message.
                mqh.mqh_length_word_complete = false;
                mqh.mqh_partial_bytes = 0;
                return Ok(res);
            }

            mqh.mqh_partial_bytes += bytes_written;
            offset += bytes_written;
            if res != SHM_MQ_SUCCESS {
                return Ok(res);
            }
        }

        // The do/while controlling expression.
        if mqh.mqh_partial_bytes >= nbytes {
            break;
        }
    }

    // Reset for next message.
    mqh.mqh_partial_bytes = 0;
    mqh.mqh_length_word_complete = false;

    // If queue has been detached, let caller know.
    if mq.detached() {
        return Ok(SHM_MQ_DETACHED);
    }

    // If the counterparty is known to have attached, we can read mq_receiver
    // without acquiring the spinlock. Otherwise, more caution is needed.
    let receiver = if mqh.mqh_counterparty_attached {
        mq.receiver()
    } else {
        let guard = mq.spin_lock("shm_mq_sendv");
        let receiver = mq.receiver();
        drop(guard);
        if !receiver.is_null() {
            mqh.mqh_counterparty_attached = true;
        }
        receiver
    };

    // If the caller has requested force flush or we have written more than
    // 1/4 of the ring size, mark it as written in shared memory and notify
    // the receiver.
    if force_flush || mqh.mqh_send_pending > (mq.ring_size() >> 2) {
        shm_mq_inc_bytes_written(mq, mqh.mqh_send_pending);
        if let Some(receiver) = receiver.get() {
            set_proc_latch(receiver);
        }
        mqh.mqh_send_pending = 0;
    }

    Ok(SHM_MQ_SUCCESS)
}

/// Receive a message from a shared message queue (`shm_mq_receive`).
///
/// On success returns `(SHM_MQ_SUCCESS, payload)`. If the entire message
/// exists in the queue as a single contiguous chunk, `payload` points
/// directly into shared memory; otherwise it points into the handle's
/// reassembly buffer. Either way it remains valid until the next receive
/// operation on the queue (the borrow of `mqh` enforces no shorter).
///
/// When `nowait` is false we wait on our process latch when the ring buffer
/// is empty; each call then returns a complete message (unless the sender
/// detaches). When `nowait` is true we return `SHM_MQ_WOULD_BLOCK` instead
/// of waiting; the caller should call again after the process latch is set.
///
/// # Safety
///
/// `mqh` must wrap a live, initialized, attached `shm_mq`.
pub unsafe fn shm_mq_receive<'a>(
    mqh: &'a mut ShmMqHandle<'_>,
    nowait: bool,
) -> PgResult<(shm_mq_result, &'a [u8])> {
    let mq = mqh.mqh_queue;
    let mut rb: Size = 0;
    let mut nbytes: Size;
    let mut rawdata: *mut u8 = ptr::null_mut();

    debug_assert!(mq.receiver().get() == Some(my_proc_number()));

    // We can't receive data until the sender has attached.
    if !mqh.mqh_counterparty_attached {
        if nowait {
            // We shouldn't return at this point at all unless the sender
            // hasn't attached yet. However, the correct return value depends
            // on whether the sender is still attached. If we first test
            // whether the sender has ever attached and then test whether the
            // sender has detached, there's a race condition: a sender that
            // attaches and detaches very quickly might fool us into thinking
            // the sender never attached at all. So, test whether our
            // counterparty is definitively gone first, and only afterwards
            // check whether the sender ever attached in the first place.
            let counterparty_gone = shm_mq_counterparty_gone(mq, mqh.mqh_handle);
            if shm_mq_get_sender(mq).is_none() {
                if counterparty_gone {
                    return Ok((SHM_MQ_DETACHED, &[]));
                } else {
                    return Ok((SHM_MQ_WOULD_BLOCK, &[]));
                }
            }
        } else if !shm_mq_wait_internal(mq, WaitTarget::Sender, mqh.mqh_handle, mqh.my_latch)?
            && shm_mq_get_sender(mq).is_none()
        {
            mq.set_detached();
            return Ok((SHM_MQ_DETACHED, &[]));
        }
        mqh.mqh_counterparty_attached = true;
    }

    // If we've consumed an amount of data greater than 1/4th of the ring
    // size, mark it consumed in shared memory. We try to avoid doing this
    // unnecessarily when only a small amount of data has been consumed,
    // because SetLatch() is fairly expensive.
    if mqh.mqh_consume_pending > mq.ring_size() / 4 {
        shm_mq_inc_bytes_read(mq, mqh.mqh_consume_pending);
        mqh.mqh_consume_pending = 0;
    }

    // Try to read, or finish reading, the length word from the buffer.
    while !mqh.mqh_length_word_complete {
        debug_assert!(mqh.mqh_partial_bytes < size_of::<Size>());
        let (res, this_rb, this_raw) =
            shm_mq_receive_bytes(mqh, size_of::<Size>() - mqh.mqh_partial_bytes, nowait)?;
        rb = this_rb;
        rawdata = this_raw;
        if res != SHM_MQ_SUCCESS {
            return Ok((res, &[]));
        }

        // Hopefully, we'll receive the entire message length word at once.
        // But if sizeof(Size) > MAXIMUM_ALIGNOF, it might be split over
        // multiple reads.
        if mqh.mqh_partial_bytes == 0 && rb >= size_of::<Size>() {
            nbytes = read_size_ne(rawdata);

            // If we've already got the whole message, we're done.
            let needed = MAXALIGN(size_of::<Size>()) + MAXALIGN(nbytes);
            if rb >= needed {
                mqh.mqh_consume_pending += needed;
                let payload = slice_from_raw(rawdata.add(MAXALIGN(size_of::<Size>())), nbytes);
                return Ok((SHM_MQ_SUCCESS, payload));
            }

            // We don't have the whole message, but we at least have the whole
            // length word.
            mqh.mqh_expected_bytes = nbytes;
            mqh.mqh_length_word_complete = true;
            mqh.mqh_consume_pending += MAXALIGN(size_of::<Size>());
            rb -= MAXALIGN(size_of::<Size>());
        } else {
            // Can't be split unless bigger than required alignment.
            debug_assert!(size_of::<Size>() > MAXIMUM_ALIGNOF);

            // Message word is split; need buffer to reassemble.
            if mqh.mqh_buffer.is_none() {
                mqh.mqh_buffer = Some(alloc_buffer(mqh.mqh_context, MQH_INITIAL_BUFSIZE)?);
            }
            let buffer = mqh.mqh_buffer.as_mut().expect("just ensured present");
            debug_assert!(buffer.len() >= size_of::<Size>());

            // Copy partial length word; remember to consume it.
            let lengthbytes = if mqh.mqh_partial_bytes + rb > size_of::<Size>() {
                size_of::<Size>() - mqh.mqh_partial_bytes
            } else {
                rb
            };
            ptr::copy_nonoverlapping(
                rawdata,
                buffer.as_mut_ptr().add(mqh.mqh_partial_bytes),
                lengthbytes,
            );
            mqh.mqh_partial_bytes += lengthbytes;
            mqh.mqh_consume_pending += MAXALIGN(lengthbytes);
            rb -= lengthbytes;

            // If we now have the whole word, we're ready to read payload.
            if mqh.mqh_partial_bytes >= size_of::<Size>() {
                debug_assert!(mqh.mqh_partial_bytes == size_of::<Size>());
                mqh.mqh_expected_bytes = read_size_ne(buffer.as_ptr());
                mqh.mqh_length_word_complete = true;
                mqh.mqh_partial_bytes = 0;
            }
        }
    }
    nbytes = mqh.mqh_expected_bytes;

    // Should be disallowed on the sending side already, but better check and
    // error out on the receiver side as well rather than trying to read a
    // prohibitively large message.
    if nbytes > MaxAllocSize {
        ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!(
                "invalid message size {nbytes} in shared memory queue"
            ))
            .finish(loc("shm_mq_receive"))?;
        return Ok((SHM_MQ_DETACHED, &[])); // unreachable
    }

    if mqh.mqh_partial_bytes == 0 {
        // Try to obtain the whole message in a single chunk. If this works,
        // we need not copy the data and can return a pointer directly into
        // shared memory.
        let (res, this_rb, this_raw) = shm_mq_receive_bytes(mqh, nbytes, nowait)?;
        rb = this_rb;
        rawdata = this_raw;
        if res != SHM_MQ_SUCCESS {
            return Ok((res, &[]));
        }
        if rb >= nbytes {
            mqh.mqh_length_word_complete = false;
            mqh.mqh_consume_pending += MAXALIGN(nbytes);
            let payload = slice_from_raw(rawdata, nbytes);
            return Ok((SHM_MQ_SUCCESS, payload));
        }

        // The message has wrapped the buffer. We'll need to copy it in order
        // to return it to the client in one chunk. First, make sure we have a
        // large enough buffer available.
        if mqh.mqh_buffer.as_ref().map_or(0, |b| b.len()) < nbytes {
            // Increase size to the next power of 2 that's >= nbytes, but
            // limit to MaxAllocSize.
            let newbuflen = min_size(pg_nextpower2_size_t(nbytes), MaxAllocSize);
            // pfree the old buffer before allocating the replacement.
            mqh.mqh_buffer = None;
            mqh.mqh_buffer = Some(alloc_buffer(mqh.mqh_context, newbuflen)?);
        }
    }

    // Loop until we've copied the entire message.
    loop {
        // Copy as much as we can.
        debug_assert!(mqh.mqh_partial_bytes + rb <= nbytes);
        if rb > 0 {
            let buffer = mqh
                .mqh_buffer
                .as_mut()
                .expect("reassembly buffer allocated before copy loop");
            ptr::copy_nonoverlapping(
                rawdata,
                buffer.as_mut_ptr().add(mqh.mqh_partial_bytes),
                rb,
            );
            mqh.mqh_partial_bytes += rb;
        }

        // Update count of bytes that can be consumed, accounting for
        // alignment padding. This never actually inserts padding except at
        // the end of a message, because the buffer size is a multiple of
        // MAXIMUM_ALIGNOF, and each read and write is as well.
        debug_assert!(mqh.mqh_partial_bytes == nbytes || rb == MAXALIGN(rb));
        mqh.mqh_consume_pending += MAXALIGN(rb);

        // If we got all the data, exit the loop.
        if mqh.mqh_partial_bytes >= nbytes {
            break;
        }

        // Wait for some more data.
        let still_needed = nbytes - mqh.mqh_partial_bytes;
        let (res, this_rb, this_raw) = shm_mq_receive_bytes(mqh, still_needed, nowait)?;
        rb = this_rb;
        rawdata = this_raw;
        if res != SHM_MQ_SUCCESS {
            return Ok((res, &[]));
        }
        if rb > still_needed {
            rb = still_needed;
        }
    }

    // Return the complete message, and reset for next message.
    mqh.mqh_length_word_complete = false;
    mqh.mqh_partial_bytes = 0;
    let payload = slice_from_raw(
        mqh.mqh_buffer
            .as_ref()
            .expect("reassembly buffer holds the message")
            .as_ptr(),
        nbytes,
    );
    Ok((SHM_MQ_SUCCESS, payload))
}

/// Wait for the other process that's supposed to use this queue to attach to
/// it (`shm_mq_wait_for_attach`).
///
/// Returns `SHM_MQ_DETACHED` if the worker has already detached or dies;
/// `SHM_MQ_SUCCESS` if we detect that it has attached. We can only detect
/// that the worker died before attaching if a background worker handle was
/// passed to `shm_mq_attach`.
///
/// # Safety
///
/// `mqh` must wrap a live, initialized, attached `shm_mq`.
pub unsafe fn shm_mq_wait_for_attach(mqh: &mut ShmMqHandle<'_>) -> PgResult<shm_mq_result> {
    let mq = mqh.mqh_queue;

    let victim = if shm_mq_get_receiver(mq) == Some(my_proc_number()) {
        WaitTarget::Sender
    } else {
        debug_assert!(shm_mq_get_sender(mq) == Some(my_proc_number()));
        WaitTarget::Receiver
    };

    if shm_mq_wait_internal(mq, victim, mqh.mqh_handle, mqh.my_latch)? {
        Ok(SHM_MQ_SUCCESS)
    } else {
        Ok(SHM_MQ_DETACHED)
    }
}

/// Detach from a shared message queue, destroying the handle
/// (`shm_mq_detach`). Consumes the owned handle box: the C pfrees of
/// `mqh_buffer` and the handle itself are the box's drop at end of scope
/// (OPTION (i): Rust's `Drop` owns the free; the DSM callback only flags).
pub fn shm_mq_detach(mut mqh: PgBox<'_, ShmMqHandle<'_>>) {
    // Before detaching, notify the receiver about any already-written data.
    if mqh.mqh_send_pending > 0 {
        shm_mq_inc_bytes_written(mqh.mqh_queue, mqh.mqh_send_pending);
        mqh.mqh_send_pending = 0;
    }

    // Notify counterparty that we're outta here.
    shm_mq_detach_internal(mqh.mqh_queue);

    // Cancel on_dsm_detach callback, if any.
    if let Some(seg) = mqh.mqh_segment {
        cancel_on_dsm_detach(
            seg,
            shm_mq_detach_callback,
            Datum::from_usize(mqh.mqh_queue.base.as_ptr() as usize),
        );
    }
}

/* ============================ internal functions ============================ */

/// Notify counterparty that we're detaching from the shared message queue
/// (`shm_mq_detach_internal`).
///
/// Makes sure the process we're communicating with doesn't block forever
/// waiting for us to fill or drain the queue once we've lost interest. When
/// the sender detaches, the receiver can read any messages remaining in the
/// queue; further reads return `SHM_MQ_DETACHED`. If the receiver detaches,
/// further attempts to send likewise return `SHM_MQ_DETACHED`. Separated
/// from `shm_mq_detach` because the on_dsm_detach callback does only this
/// much (it must not touch the backend-local handle).
fn shm_mq_detach_internal(mq: ShmMq) {
    let my_proc = my_proc_number();

    let guard = mq.spin_lock("shm_mq_detach_internal");
    let victim = if mq.sender().get() == Some(my_proc) {
        mq.receiver()
    } else {
        debug_assert!(mq.receiver().get() == Some(my_proc));
        mq.sender()
    };
    mq.set_detached();
    drop(guard);

    if let Some(victim) = victim.get() {
        set_proc_latch(victim);
    }
}

/// Write bytes into a shared message queue (`shm_mq_send_bytes`). Returns the
/// result and the number of bytes actually written (the C `*bytes_written`).
///
/// # Safety
///
/// `mqh` must wrap a live, initialized, attached `shm_mq`.
unsafe fn shm_mq_send_bytes(
    mqh: &mut ShmMqHandle<'_>,
    nbytes: Size,
    data: &[u8],
    nowait: bool,
) -> PgResult<(shm_mq_result, Size)> {
    let mq = mqh.mqh_queue;
    let mut sent: Size = 0;
    let ringsize = mq.ring_size();

    while sent < nbytes {
        // Compute number of ring buffer bytes used and available.
        let rb = mq.bytes_read();
        let wb = mq.bytes_written() + mqh.mqh_send_pending as u64;
        debug_assert!(wb >= rb);
        let used = wb - rb;
        debug_assert!(used as Size <= ringsize);
        let available = min_size(ringsize - used as Size, nbytes - sent);

        // Bail out if the queue has been detached. We would be in trouble if
        // the compiler cached mq_detached in a register across iterations, so
        // insert a compiler barrier.
        compiler_fence(Ordering::SeqCst);
        if mq.detached() {
            return Ok((SHM_MQ_DETACHED, sent));
        }

        if available == 0 && !mqh.mqh_counterparty_attached {
            // The queue is full, so if the receiver isn't yet known to be
            // attached, we must wait for that to happen.
            if nowait {
                if shm_mq_counterparty_gone(mq, mqh.mqh_handle) {
                    return Ok((SHM_MQ_DETACHED, sent));
                }
                if shm_mq_get_receiver(mq).is_none() {
                    return Ok((SHM_MQ_WOULD_BLOCK, sent));
                }
            } else if !shm_mq_wait_internal(mq, WaitTarget::Receiver, mqh.mqh_handle, mqh.my_latch)?
            {
                mq.set_detached();
                return Ok((SHM_MQ_DETACHED, sent));
            }
            mqh.mqh_counterparty_attached = true;

            // The receiver may have read some data after attaching, so we
            // must not wait without rechecking the queue state.
        } else if available == 0 {
            // Update the pending send bytes in the shared memory.
            shm_mq_inc_bytes_written(mq, mqh.mqh_send_pending);

            // Since mqh_counterparty_attached is known true at this point,
            // mq_receiver has been set, and it can't change once set; read it
            // without acquiring the spinlock.
            debug_assert!(mqh.mqh_counterparty_attached);
            let receiver = mq
                .receiver()
                .get()
                .expect("mq_receiver set once counterparty attached (shm_mq.c SetLatch site)");
            set_proc_latch(receiver);

            // We have just updated the mqh_send_pending bytes in the shared
            // memory so reset it.
            mqh.mqh_send_pending = 0;

            // Skip manipulation of our latch if nowait = true.
            if nowait {
                return Ok((SHM_MQ_WOULD_BLOCK, sent));
            }

            // Wait for our latch to be set. It might already be set for some
            // unrelated reason, but that'll just result in one extra trip
            // through the loop. It's worth it to avoid resetting the latch at
            // top of loop, because setting an already-set latch is much
            // cheaper than setting one that has been reset.
            wait_latch::call(
                mqh.my_latch,
                WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
                0,
                WAIT_EVENT_MESSAGE_QUEUE_SEND,
            )?;

            // Reset the latch so we don't spin.
            reset_latch::call(mqh.my_latch);

            // An interrupt may have occurred while we were waiting.
            check_for_interrupts::call()?;
        } else {
            let offset = (wb % ringsize as u64) as Size;
            let sendnow = min_size(available, ringsize - offset);

            // Write as much data as we can via a single copy. Make sure
            // these writes happen after the read of mq_bytes_read, above.
            // This barrier pairs with the one in shm_mq_inc_bytes_read.
            // (Since we're separating the read of mq_bytes_read from a
            // subsequent write to mq_ring, we need a full barrier here.)
            fence(Ordering::SeqCst);
            ptr::copy_nonoverlapping(data.as_ptr().add(sent), mq.ring_ptr(offset), sendnow);
            sent += sendnow;

            // Update count of bytes written, with alignment padding. This
            // never actually inserts padding except at the end of a run of
            // bytes, because the buffer size is a multiple of
            // MAXIMUM_ALIGNOF, and each read is as well.
            debug_assert!(sent == nbytes || sendnow == MAXALIGN(sendnow));

            // For efficiency, we don't update the bytes written in shared
            // memory and don't set the reader's latch here; see the comments
            // atop the shm_mq_handle structure.
            mqh.mqh_send_pending += MAXALIGN(sendnow);
        }
    }

    Ok((SHM_MQ_SUCCESS, sent))
}

/// Wait until at least `bytes_needed` bytes are available to be read, or
/// until the buffer wraps around (`shm_mq_receive_bytes`). Returns the
/// result, the number of contiguous readable bytes (the C `*nbytesp`), and a
/// pointer to them (`*datap`).
///
/// # Safety
///
/// `mqh` must wrap a live, initialized, attached `shm_mq`.
unsafe fn shm_mq_receive_bytes(
    mqh: &mut ShmMqHandle<'_>,
    bytes_needed: Size,
    nowait: bool,
) -> PgResult<(shm_mq_result, Size, *mut u8)> {
    let mq = mqh.mqh_queue;
    let ringsize = mq.ring_size();

    loop {
        // Get bytes written, so we can compute what's available to read.
        let written = mq.bytes_written();

        // Get bytes read. Include the bytes we could consume but have not
        // yet consumed.
        let read = mq.bytes_read() + mqh.mqh_consume_pending as u64;
        let used = written - read;
        debug_assert!(used as Size <= ringsize);
        let offset = (read % ringsize as u64) as Size;

        // If we have enough data or buffer has wrapped, we're done.
        if used as Size >= bytes_needed || offset + used as Size >= ringsize {
            let nbytesp = min_size(used as Size, ringsize - offset);
            let datap = mq.ring_ptr(offset);

            // Separate the read of mq_bytes_written, above, from the caller's
            // attempt to read the data itself. Pairs with the barrier in
            // shm_mq_inc_bytes_written.
            fence(Ordering::Acquire);
            return Ok((SHM_MQ_SUCCESS, nbytesp, datap));
        }

        // Fall out before waiting if the queue has been detached. We don't
        // check this until *after* considering whether the data already
        // available is enough, since the receiver can finish receiving a
        // message stored in the buffer even after the sender has detached.
        if mq.detached() {
            // If the writer advanced mq_bytes_written and then set
            // mq_detached, we might not have read the final value above.
            // Insert a read barrier and then check again.
            fence(Ordering::Acquire);
            if written != mq.bytes_written() {
                continue;
            }
            return Ok((SHM_MQ_DETACHED, 0, ptr::null_mut()));
        }

        // We didn't get enough data to satisfy the request, so mark any data
        // previously-consumed as read to make more buffer space.
        if mqh.mqh_consume_pending > 0 {
            shm_mq_inc_bytes_read(mq, mqh.mqh_consume_pending);
            mqh.mqh_consume_pending = 0;
        }

        // Skip manipulation of our latch if nowait = true.
        if nowait {
            return Ok((SHM_MQ_WOULD_BLOCK, 0, ptr::null_mut()));
        }

        // Wait for our latch to be set; see shm_mq_send_bytes on why we
        // don't reset it at top of loop.
        wait_latch::call(
            mqh.my_latch,
            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
            0,
            WAIT_EVENT_MESSAGE_QUEUE_RECEIVE,
        )?;

        // Reset the latch so we don't spin.
        reset_latch::call(mqh.my_latch);

        // An interrupt may have occurred while we were waiting.
        check_for_interrupts::call()?;
    }
}

/// Test whether a counterparty who may not even be alive yet is definitely
/// gone (`shm_mq_counterparty_gone`).
fn shm_mq_counterparty_gone(mq: ShmMq, handle: Option<BackgroundWorkerHandle>) -> bool {
    // If the queue has been detached, counterparty is definitely gone.
    if mq.detached() {
        return true;
    }

    // If there's a handle, check worker status.
    if let Some(handle) = handle {
        // Check for unexpected worker death.
        let (status, _pid) = get_background_worker_pid::call(handle);
        if status != BgwHandleStatus::Started && status != BgwHandleStatus::NotYetStarted {
            // Mark it detached, just to make it official.
            mq.set_detached();
            return true;
        }
    }

    // Counterparty is not definitively gone.
    false
}

/// Which sender/receiver identity `shm_mq_wait_internal` watches (the C
/// `PGPROC **ptr` argument: `&mq->mq_sender` or `&mq->mq_receiver`).
#[derive(Clone, Copy)]
enum WaitTarget {
    Sender,
    Receiver,
}

/// Wait for the counterpart to attach (`shm_mq_wait_internal`). We exit when
/// the other process attaches as expected, or, if `handle` is provided, when
/// the referenced background process or the postmaster dies. If `handle` is
/// `None` and the process never attaches we'd wait here forever — but we do
/// check for interrupts. Returns true when the counterpart attached, false
/// on detach/death.
fn shm_mq_wait_internal(
    mq: ShmMq,
    target: WaitTarget,
    handle: Option<BackgroundWorkerHandle>,
    my_latch: LatchHandle,
) -> PgResult<bool> {
    let mut result;

    loop {
        // Acquire the lock just long enough to check the pointer.
        let guard = mq.spin_lock("shm_mq_wait_internal");
        let ptr_val = match target {
            WaitTarget::Sender => mq.sender(),
            WaitTarget::Receiver => mq.receiver(),
        };
        result = !ptr_val.is_null();
        drop(guard);

        // Fail if detached; else succeed if initialized.
        if mq.detached() {
            result = false;
            break;
        }
        if result {
            break;
        }

        if let Some(handle) = handle {
            // Check for unexpected worker death.
            let (status, _pid) = get_background_worker_pid::call(handle);
            if status != BgwHandleStatus::Started && status != BgwHandleStatus::NotYetStarted {
                result = false;
                break;
            }
        }

        // Wait to be signaled.
        wait_latch::call(
            my_latch,
            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
            0,
            WAIT_EVENT_MESSAGE_QUEUE_INTERNAL,
        )?;

        // Reset the latch so we don't spin.
        reset_latch::call(my_latch);

        // An interrupt may have occurred while we were waiting.
        check_for_interrupts::call()?;
    }

    Ok(result)
}

/// Increment the number of bytes read (`shm_mq_inc_bytes_read`).
fn shm_mq_inc_bytes_read(mq: ShmMq, n: Size) {
    // Separate prior reads of mq_ring from the increment of mq_bytes_read
    // which follows. This pairs with the full barrier in shm_mq_send_bytes.
    // We only need a read barrier here because the increment of
    // mq_bytes_read is actually a read followed by a dependent write.
    fence(Ordering::Acquire);

    // There's no need for fetch_add: nobody else can be changing this value.
    let cur = mq.bytes_read();
    // SAFETY: header is live; only this (receiver) backend writes
    // mq_bytes_read.
    unsafe {
        mq.header()
            .mq_bytes_read
            .store(cur + n as u64, Ordering::Relaxed)
    };

    // We shouldn't have any bytes to read without a sender, so we can read
    // mq_sender here without a lock. Once it's initialized, it can't change.
    let sender = mq
        .sender()
        .get()
        .expect("bytes to read imply mq_sender is set (shm_mq.c Assert)");
    set_proc_latch(sender);
}

/// Increment the number of bytes written (`shm_mq_inc_bytes_written`).
fn shm_mq_inc_bytes_written(mq: ShmMq, n: Size) {
    // Separate prior reads of mq_ring from the write of mq_bytes_written
    // we're about to do. Pairs with the read barrier in
    // shm_mq_receive_bytes.
    fence(Ordering::Release);

    // There's no need for fetch_add: nobody else can be changing this value.
    let cur = mq.bytes_written();
    // SAFETY: header is live; only this (sender) backend writes
    // mq_bytes_written.
    unsafe {
        mq.header()
            .mq_bytes_written
            .store(cur + n as u64, Ordering::Relaxed)
    };
}

/// Shim for on_dsm_detach callback (`shm_mq_detach_callback`). `arg` is the
/// queue base address minted in `shm_mq_attach`.
fn shm_mq_detach_callback(_seg: DsmSegmentId, arg: Datum) -> PgResult<()> {
    // SAFETY: arg is the queue base address registered by shm_mq_attach; the
    // segment is still mapped at detach time (callbacks run before unmap).
    let mq = unsafe {
        ShmMq::from_base(
            NonNull::new(arg.as_usize() as *mut u8).expect("queue base address is non-null"),
        )
    };
    shm_mq_detach_internal(mq);
    Ok(())
}

/* ============================ small helpers ============================ */

/// Read a native-endian `Size` from `ptr` (`*(Size *) ptr`).
///
/// # Safety
///
/// `ptr` must point to at least `size_of::<Size>()` readable bytes.
unsafe fn read_size_ne(ptr: *const u8) -> Size {
    let mut bytes = [0u8; size_of::<Size>()];
    ptr::copy_nonoverlapping(ptr, bytes.as_mut_ptr(), size_of::<Size>());
    Size::from_ne_bytes(bytes)
}

/// Build a byte slice from a raw pointer + length, for payloads pointing
/// either into shmem or into the handle's reassembly buffer.
///
/// # Safety
///
/// `ptr` must point to at least `len` valid bytes that outlive `'a`.
unsafe fn slice_from_raw<'a>(ptr: *const u8, len: Size) -> &'a [u8] {
    if len == 0 {
        return &[];
    }
    core::slice::from_raw_parts(ptr, len)
}

/// `MemoryContextAlloc(mqh->mqh_context, len)` for the reassembly buffer:
/// fallible reservation whose failure is the context's OOM error.
fn alloc_buffer(mcx: Mcx<'_>, len: Size) -> PgResult<PgVec<'_, u8>> {
    let mut buf = mcx::vec_with_capacity_in(mcx, len)?;
    buf.resize(len, 0);
    Ok(buf)
}

// ===========================================================================
// Seam layer (OPTION (i)): the backend-private `shm_mq_handle` is the owned
// `PgBox<ShmMqHandle>` parked in this process-global registry; across the seam
// it is named by a small id ([`types_execparallel::ShmMqAttachHandle`]). The
// in-segment `shm_mq` is named by its real base address
// ([`types_execparallel::ShmMqHandle`], value == base as usize). This is the
// real-DSM substrate the parallel executor and parallel orchestration consume.
// ===========================================================================
mod seam_layer {
    use core::cell::RefCell;
    use core::ptr::NonNull;

    use backend_storage_ipc_dsm_core::dsm::DsmSegmentId;
    use backend_utils_mmgr_mcxt_seams::top_memory_context;
    use mcx::PgBox;
    use types_error::PgResult;
    use types_execparallel::{
        BackgroundWorkerHandle as ExecBgwHandle, DsmSegmentHandle as ExecDsmSeg, SerializeCursor,
        ShmMqAttachHandle, ShmMqHandle as ExecShmMq, Size,
    };
    use types_parallel::ShmMqResult;

    use crate::{
        shm_mq_attach as real_attach, shm_mq_create as real_create, shm_mq_detach as real_detach,
        shm_mq_get_queue as real_get_queue, shm_mq_get_sender as real_get_sender,
        shm_mq_receive as real_receive, shm_mq_send as real_send,
        shm_mq_set_handle as real_set_handle, shm_mq_set_receiver, shm_mq_set_sender, ShmMq,
        ShmMqHandle, SHM_MQ_DETACHED, SHM_MQ_SUCCESS, SHM_MQ_WOULD_BLOCK,
    };

    /// `MyProcNumber` — the identity passed to `shm_mq_set_receiver/sender(mq,
    /// MyProc)`.
    fn my_proc() -> types_core::ProcNumber {
        backend_utils_init_small_seams::my_proc_number::call()
    }

    /// One live attached handle: the owned box (OPTION (i): its `Drop`/`detach`
    /// owns the free). `'static` because it is parked here for the queue's life
    /// and allocated in `TopMemoryContext`.
    struct Registry {
        /// `Some` for a live slot; `None` for a freed one (reusable).
        slots: alloc::vec::Vec<Option<PgBox<'static, ShmMqHandle<'static>>>>,
    }

    impl Registry {
        const fn new() -> Self {
            Self {
                slots: alloc::vec::Vec::new(),
            }
        }

        /// Park an owned handle, returning its 1-based id (0 is the NULL
        /// sentinel of `ShmMqAttachHandle`).
        fn insert(&mut self, h: PgBox<'static, ShmMqHandle<'static>>) -> ShmMqAttachHandle {
            if let Some(i) = self.slots.iter().position(Option::is_none) {
                self.slots[i] = Some(h);
                ShmMqAttachHandle(i + 1)
            } else {
                self.slots.push(Some(h));
                ShmMqAttachHandle(self.slots.len())
            }
        }

        fn idx(h: ShmMqAttachHandle) -> usize {
            debug_assert!(h.0 >= 1, "ShmMqAttachHandle 0 is the NULL sentinel");
            h.0 - 1
        }

        fn get_mut(&mut self, h: ShmMqAttachHandle) -> &mut PgBox<'static, ShmMqHandle<'static>> {
            self.slots[Self::idx(h)]
                .as_mut()
                .expect("live shm_mq_handle id")
        }

        fn get(&self, h: ShmMqAttachHandle) -> &PgBox<'static, ShmMqHandle<'static>> {
            self.slots[Self::idx(h)]
                .as_ref()
                .expect("live shm_mq_handle id")
        }

        /// Take the owned box back out (freeing the slot); the caller consumes
        /// it via `shm_mq_detach`.
        fn take(&mut self, h: ShmMqAttachHandle) -> PgBox<'static, ShmMqHandle<'static>> {
            self.slots[Self::idx(h)]
                .take()
                .expect("live shm_mq_handle id")
        }
    }

    thread_local! {
        static REGISTRY: RefCell<Registry> = const { RefCell::new(Registry::new()) };
    }

    fn with_registry<R>(f: impl FnOnce(&mut Registry) -> R) -> R {
        REGISTRY.with(|r| f(&mut r.borrow_mut()))
    }

    /// `ShmMqHandle` token (real base address as usize) -> live `ShmMq`.
    fn mq_from_token(mq: ExecShmMq) -> ShmMq {
        // SAFETY: the token value is the real in-segment base minted by
        // `shm_mq_create_at` (a real `shm_toc` chunk address); it addresses a
        // live `InSegmentShmMq`.
        unsafe { ShmMq::from_base(NonNull::new(mq.0 as *mut u8).expect("shm_mq base non-null")) }
    }

    fn token_from_mq(mq: ShmMq) -> ExecShmMq {
        ExecShmMq(mq.base().as_ptr() as usize)
    }

    /// The leader/worker `DsmSegmentHandle` carries the real `DsmSegmentId`
    /// (opacity-inherited: handle value == id), `0` being the NULL sentinel.
    fn seg_id_of(seg: ExecDsmSeg) -> DsmSegmentId {
        DsmSegmentId::from_u64(seg.0 as u64)
    }

    /// `shm_mq_create_at` (`shm_mq_create(chunk + i*size, size)`).
    fn shm_mq_create_at(chunk: SerializeCursor, index: i32, size: Size) -> ExecShmMq {
        let addr = chunk.0 + (index as Size) * size;
        let base = NonNull::new(addr as *mut u8).expect("shm_mq chunk address non-null");
        // SAFETY: `addr` is a real, MAXALIGN'd, `size`-byte in-segment chunk the
        // caller carved out of the queue DSM region.
        let mq = unsafe { real_create(base, size) };
        token_from_mq(mq)
    }

    /// `(shm_mq *) (chunk + i*size)` — name an already-created in-segment queue
    /// without re-initializing it (the worker side's plain cast). The token is
    /// just the real base address; no header write.
    fn shm_mq_at(chunk: SerializeCursor, index: i32, size: Size) -> ExecShmMq {
        let addr = chunk.0 + (index as Size) * size;
        debug_assert!(addr != 0, "shm_mq chunk address non-null");
        ExecShmMq(addr)
    }

    fn shm_mq_set_receiver_to_myproc(mq: ExecShmMq) {
        shm_mq_set_receiver(mq_from_token(mq), my_proc());
    }

    fn shm_mq_set_sender_to_myproc(mq: ExecShmMq) {
        shm_mq_set_sender(mq_from_token(mq), my_proc());
    }

    fn shm_mq_get_sender(mq: ExecShmMq) -> Option<types_core::ProcNumber> {
        real_get_sender(mq_from_token(mq))
    }

    fn shm_mq_attach(mq: ExecShmMq, seg: Option<ExecDsmSeg>) -> PgResult<ShmMqAttachHandle> {
        // C `shm_mq_attach` allocates the handle in `CurrentMemoryContext`; the
        // error/tuple queues live as long as the parallel context, so we use
        // `TopMemoryContext` (also the `'static` context the dsm unit records
        // its `on_dsm_detach` callback in).
        let top = top_memory_context::call();
        // `0` is the NULL sentinel for the execParallel `DsmSegmentHandle`.
        let seg = seg.filter(|s| s.0 != 0).map(|s| (seg_id_of(s), top));
        let my_latch = backend_storage_ipc_latch_seams::my_latch::call();
        let mqh = real_attach(mq_from_token(mq), top, seg, None, my_latch)?;
        Ok(with_registry(|r| r.insert(mqh)))
    }

    fn shm_mq_set_handle(mqh: ShmMqAttachHandle, handle: ExecBgwHandle) {
        let real =
            backend_postmaster_bgworker_seams::background_worker_handle_from_token::call(handle);
        with_registry(|r| real_set_handle(r.get_mut(mqh), real));
    }

    fn shm_mq_get_queue(mqh: ShmMqAttachHandle) -> ExecShmMq {
        with_registry(|r| token_from_mq(real_get_queue(r.get(mqh))))
    }

    fn shm_mq_receive(
        mqh: ShmMqAttachHandle,
    ) -> PgResult<(Option<ShmMqResult>, alloc::vec::Vec<u8>)> {
        // C: `shm_mq_receive(error_mqh, &nbytes, &data, true)` — non-blocking.
        // The payload borrows the queue/reassembly buffer; copy it out (within
        // the registry borrow) for the caller, which re-parses it in its own
        // context. Empty on any non-success result.
        with_registry(|r| {
            // SAFETY: the registry holds a live, attached handle for this id.
            let (res, data) = unsafe { real_receive(r.get_mut(mqh), true) }?;
            let result = match res {
                SHM_MQ_SUCCESS => Some(ShmMqResult::Success),
                SHM_MQ_WOULD_BLOCK => Some(ShmMqResult::WouldBlock),
                SHM_MQ_DETACHED => Some(ShmMqResult::Detached),
            };
            let owned = if result == Some(ShmMqResult::Success) {
                data.to_vec()
            } else {
                alloc::vec::Vec::new()
            };
            Ok((result, owned))
        })
    }

    fn shm_mq_detach(mqh: ShmMqAttachHandle) {
        // Take the owned box out of the registry and consume it: `shm_mq_detach`
        // notifies the counterparty, cancels the on_dsm_detach callback, and the
        // box's `Drop` frees the handle + its reassembly buffer (OPTION (i)).
        let owned = with_registry(|r| r.take(mqh));
        real_detach(owned);
    }

    fn shm_mq_send(
        mqh: ShmMqAttachHandle,
        data: alloc::vec::Vec<u8>,
        nowait: bool,
        force_flush: bool,
    ) -> PgResult<ShmMqResult> {
        with_registry(|r| {
            // SAFETY: the registry holds a live, attached handle for this id.
            let res = unsafe { real_send(r.get_mut(mqh), &data, nowait, force_flush) }?;
            Ok(match res {
                SHM_MQ_SUCCESS => ShmMqResult::Success,
                SHM_MQ_WOULD_BLOCK => ShmMqResult::WouldBlock,
                SHM_MQ_DETACHED => ShmMqResult::Detached,
            })
        })
    }

    fn shm_mq_receive_nowait(
        mqh: ShmMqAttachHandle,
        nowait: bool,
    ) -> PgResult<(Option<ShmMqResult>, alloc::vec::Vec<u8>)> {
        with_registry(|r| {
            // SAFETY: the registry holds a live, attached handle for this id.
            let (res, data) = unsafe { real_receive(r.get_mut(mqh), nowait) }?;
            let result = match res {
                SHM_MQ_SUCCESS => Some(ShmMqResult::Success),
                SHM_MQ_WOULD_BLOCK => Some(ShmMqResult::WouldBlock),
                SHM_MQ_DETACHED => Some(ShmMqResult::Detached),
            };
            let owned = if result == Some(ShmMqResult::Success) {
                data.to_vec()
            } else {
                alloc::vec::Vec::new()
            };
            Ok((result, owned))
        })
    }

    pub fn install() {
        use backend_storage_ipc_shm_mq_seams as seams;
        seams::shm_mq_create_at::set(shm_mq_create_at);
        seams::shm_mq_at::set(shm_mq_at);
        seams::shm_mq_set_receiver_to_myproc::set(shm_mq_set_receiver_to_myproc);
        seams::shm_mq_set_sender_to_myproc::set(shm_mq_set_sender_to_myproc);
        seams::shm_mq_get_sender::set(shm_mq_get_sender);
        seams::shm_mq_attach::set(shm_mq_attach);
        seams::shm_mq_set_handle::set(shm_mq_set_handle);
        seams::shm_mq_get_queue::set(shm_mq_get_queue);
        seams::shm_mq_receive::set(shm_mq_receive);
        seams::shm_mq_detach::set(shm_mq_detach);
        seams::shm_mq_send::set(shm_mq_send);
        seams::shm_mq_receive_nowait::set(shm_mq_receive_nowait);
    }
}

extern crate alloc;

/// Install this crate's implementations into the `shm-mq` seam crate: the
/// OPTION (i) `PgBox<ShmMqHandle>`-registry-backed create/attach/detach/… seams
/// the parallel executor and parallel orchestration consume.
pub fn init_seams() {
    seam_layer::install();
}

#[cfg(test)]
mod tests;
