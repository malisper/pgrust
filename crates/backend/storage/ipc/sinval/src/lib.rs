#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! `backend-storage-ipc-sinval` — shared cache-invalidation communication.
//!
//! Port of `src/backend/storage/ipc/sinval.c` (the thin interface layer) and
//! `src/backend/storage/ipc/sinvaladt.c` (the shared SI message queue
//! `SISeg`/`ProcState` plus the per-backend local-transaction-id counter).
//!
//! ## Per-backend vs. shared state
//!
//! `SharedInvalidMessageCounter`, `catchupInterruptPending`, the
//! `ReceiveSharedInvalidMessages` static receive buffer/counters, and
//! `nextLocalTransactionId` are plain *backend* globals in the C — process-local,
//! not in shared memory. A backend is single-threaded, so they are modelled as
//! `thread_local!` cells. The `SISeg`/`ProcState` state, by contrast, lives in
//! genuine byte-addressed shared memory reserved with `ShmemInitStruct`, with a
//! real in-segment `slock_t` spinlock (`msgnumLock`) and the two named
//! `SInval{Read,Write}Lock` LWLocks.
//!
//! ## `ReceiveSharedInvalidMessages` recursion
//!
//! The C deliberately uses a *static* buffer + `nextmsg`/`nummsgs` counters so a
//! call that recurses (because `invalFunction`/`resetFunction` themselves trigger
//! another `ReceiveSharedInvalidMessages`) can consume messages the outer call
//! already pulled out of the queue. We preserve that with `thread_local!` cells.

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use core::cell::Cell;
use core::ffi::c_int;
use core::mem::{offset_of, size_of, MaybeUninit};
use core::ptr::NonNull;
use std::cell::RefCell;

use ::s_lock::{s_lock, s_unlock};
use ::utils_error::elog;

use transam_xact_seams as xact;
use latch_seams as latch;
use procsignal_seams as procsignal;
use dsm_core_seams as ipc;
use ipc_shmem_seams as shmem;
use lwlock_seams as lwlock;
use inval_seams as inval;
use init_small_seams as init_small;

use ::types_tuple::Datum;
use ::types_error::{PgError, PgResult, DEBUG4, PANIC};
use ::types_storage::sinval::{SharedInvalCatcacheMsg, SharedInvalidationMessage};
use ::types_storage::storage::Spinlock;
use ::types_core::ProcNumber;
use ::types_storage::{
    LWLockMode, ProcSignalReason, NUM_AUXILIARY_PROCS, SINVAL_READ_LOCK, SINVAL_WRITE_LOCK,
};

/// A benign placeholder for unwritten receive-buffer slots. The C
/// `ReceiveSharedInvalidMessages` static buffer is uninitialized and only the
/// first `getResult` slots are ever read; this fills the scratch array with a
/// value that is never observed (overwritten by `SIGetDataEntries` or left past
/// the returned count).
const RECEIVE_BUFFER_PLACEHOLDER: SharedInvalidationMessage =
    SharedInvalidationMessage::Catcache(SharedInvalCatcacheMsg {
        id: 0,
        dbId: 0,
        hashValue: 0,
    });

type LocalTransactionId = ::types_core::LocalTransactionId;
const InvalidLocalTransactionId: LocalTransactionId = ::types_core::xact::InvalidLocalTransactionId;

// ===========================================================================
// sinvaladt.c — configurable parameters (sinvaladt.c:129-134)
// ===========================================================================

/// `MAXNUMMESSAGES` — max number of shared-inval messages we can buffer. Must be
/// a power of 2 for speed.
pub const MAXNUMMESSAGES: usize = 4096;
/// `MSGNUMWRAPAROUND` — how often to reduce `MsgNum` variables to avoid overflow.
/// Must be a multiple of `MAXNUMMESSAGES`. Should be large.
pub const MSGNUMWRAPAROUND: c_int = (MAXNUMMESSAGES as c_int) * 262_144;
/// `CLEANUP_MIN` — minimum number of messages in the buffer before we bother to
/// call `SICleanupQueue`.
pub const CLEANUP_MIN: c_int = (MAXNUMMESSAGES / 2) as c_int;
/// `CLEANUP_QUANTUM` — how often (in messages) to call `SICleanupQueue` once we
/// exceed `CLEANUP_MIN`. Should be a power of 2 for speed.
pub const CLEANUP_QUANTUM: c_int = (MAXNUMMESSAGES / 16) as c_int;
/// `SIG_THRESHOLD` — minimum number of messages a backend must have fallen behind
/// before we send it `PROCSIG_CATCHUP_INTERRUPT`.
pub const SIG_THRESHOLD: c_int = (MAXNUMMESSAGES / 2) as c_int;
/// `WRITE_QUANTUM` — max messages to push into the buffer per iteration of
/// `SIInsertDataEntries`.
pub const WRITE_QUANTUM: usize = 64;

/// `#define MAXINVALMSGS 32` — the static receive buffer size (sinval.c:72).
const MAXINVALMSGS: usize = 32;

// ===========================================================================
// Per-backend globals (sinval.c / sinvaladt.c file scope)
// ===========================================================================

thread_local! {
    /// `uint64 SharedInvalidMessageCounter` — counter of messages processed;
    /// overflow is unimportant (so it wraps, like the C `++`).
    static SHARED_INVALID_MESSAGE_COUNTER: Cell<u64> = const { Cell::new(0) };

    /// `volatile sig_atomic_t catchupInterruptPending` — set by the catchup
    /// signal handler, cleared once the backend has caught up.
    static CATCHUP_INTERRUPT_PENDING: Cell<bool> = const { Cell::new(false) };

    /// `static LocalTransactionId nextLocalTransactionId` (sinvaladt.c:209) — a
    /// process-local counter (a backend global), not shared state.
    static NEXT_LOCAL_TRANSACTION_ID: Cell<LocalTransactionId> =
        const { Cell::new(InvalidLocalTransactionId) };

    /// The C `static SharedInvalidationMessage messages[MAXINVALMSGS]`
    /// (sinval.c:73).
    static MESSAGES: RefCell<[Option<SharedInvalidationMessage>; MAXINVALMSGS]> =
        const { RefCell::new([None; MAXINVALMSGS]) };
    /// The C `static volatile int nextmsg` (sinval.c:79).
    static NEXTMSG: Cell<i32> = const { Cell::new(0) };
    /// The C `static volatile int nummsgs` (sinval.c:80).
    static NUMMSGS: Cell<i32> = const { Cell::new(0) };

    /// `static SISeg *shmInvalBuffer` (sinvaladt.c:206) — the per-process handle
    /// to the shared segment. `None` until `SharedInvalShmemInit`.
    static SHM_INVAL_BUFFER: Cell<Option<SISeg>> = const { Cell::new(None) };
}

/// `SharedInvalidMessageCounter` (sinval.c:24) — running count of shared
/// invalidation messages this backend has processed. Pure global read.
#[inline]
pub fn SharedInvalidMessageCounter() -> u64 {
    SHARED_INVALID_MESSAGE_COUNTER.with(Cell::get)
}

/// Read the `catchupInterruptPending` flag (sinval.c:39).
#[inline]
pub fn catchupInterruptPending() -> bool {
    CATCHUP_INTERRUPT_PENDING.with(Cell::get)
}

#[inline]
fn bump_counter() {
    SHARED_INVALID_MESSAGE_COUNTER.with(|c| c.set(c.get().wrapping_add(1)));
}

// ===========================================================================
// sinval.c — SendSharedInvalidMessages
// ===========================================================================

/// `SendSharedInvalidMessages` (sinval.c:46) — add shared-cache-invalidation
/// message(s) to the global SI message queue.
pub fn SendSharedInvalidMessages(msgs: &[SharedInvalidationMessage]) -> PgResult<()> {
    SIInsertDataEntries(msgs)
}

// ===========================================================================
// sinval.c — ReceiveSharedInvalidMessages
// ===========================================================================

/// Read `messages[i]` from the static receive buffer.
#[inline]
fn message_at(i: i32) -> SharedInvalidationMessage {
    MESSAGES.with(|m| {
        m.borrow()[i as usize]
            .expect("ReceiveSharedInvalidMessages: message slot read before fill")
    })
}

/// `ReceiveSharedInvalidMessages` (sinval.c:68) — process shared-cache-
/// invalidation messages waiting for this backend.
///
/// We guarantee to process all messages queued before the routine was entered.
/// It can be invoked recursively from inside `inval_function`/`reset_function`;
/// the static (here `thread_local!`) buffer + counters let a recursive call
/// consume messages already sucked out of the queue.
pub fn ReceiveSharedInvalidMessages(
    inval_function: &mut dyn FnMut(&SharedInvalidationMessage),
    reset_function: &mut dyn FnMut(),
) -> PgResult<()> {
    // Deal with any messages still pending from an outer recursion.
    while NEXTMSG.with(Cell::get) < NUMMSGS.with(Cell::get) {
        let cur = NEXTMSG.with(Cell::get);
        NEXTMSG.with(|c| c.set(cur + 1));
        let msg = message_at(cur);

        bump_counter();
        inval_function(&msg);
    }

    loop {
        NEXTMSG.with(|c| c.set(0));
        NUMMSGS.with(|c| c.set(0));

        // Try to get some more messages.
        let mut buf = [RECEIVE_BUFFER_PLACEHOLDER; MAXINVALMSGS];
        let get_result = SIGetDataEntries(&mut buf)?;

        if get_result < 0 {
            // got a reset message
            elog(DEBUG4, "cache state reset")?;
            bump_counter();
            reset_function();
            break; // nothing more to do
        }

        // Copy the fetched messages into the static buffer.
        MESSAGES.with(|m| {
            let mut slots = m.borrow_mut();
            for (slot, msg) in slots.iter_mut().zip(buf.iter().take(get_result as usize)) {
                *slot = Some(*msg);
            }
        });

        // Process them, being wary that a recursive call might eat some.
        NEXTMSG.with(|c| c.set(0));
        NUMMSGS.with(|c| c.set(get_result));

        while NEXTMSG.with(Cell::get) < NUMMSGS.with(Cell::get) {
            let cur = NEXTMSG.with(Cell::get);
            NEXTMSG.with(|c| c.set(cur + 1));
            let msg = message_at(cur);

            bump_counter();
            inval_function(&msg);
        }

        // We only need to loop if the last SIGetDataEntries call (which might
        // have been within a recursive call) returned a full buffer.
        if NUMMSGS.with(Cell::get) != MAXINVALMSGS as i32 {
            break;
        }
    }

    // We are now caught up. If we received a catchup signal, reset that flag and
    // call SICleanupQueue() to pass the catchup signal on to the next slowest
    // backend ("daisy chaining").
    if CATCHUP_INTERRUPT_PENDING.with(Cell::get) {
        CATCHUP_INTERRUPT_PENDING.with(|c| c.set(false));
        elog(DEBUG4, "sinval catchup complete, cleaning queue")?;
        SICleanupQueue(false, 0)?;
    }
    Ok(())
}

// ===========================================================================
// sinval.c — HandleCatchupInterrupt
// ===========================================================================

/// `HandleCatchupInterrupt` (sinval.c:153) — called when
/// `PROCSIG_CATCHUP_INTERRUPT` is received. Called by a SIGNAL HANDLER in the C;
/// it just sets the pending flag and the process's latch. Infallible.
pub fn HandleCatchupInterrupt() {
    CATCHUP_INTERRUPT_PENDING.with(|c| c.set(true));

    // make sure the event is processed in due course: SetLatch(MyLatch).
    latch::set_latch_my_latch::call();
}

// ===========================================================================
// sinval.c — ProcessCatchupInterrupt
// ===========================================================================

/// `ProcessCatchupInterrupt` (sinval.c:173) — the portion of catchup-interrupt
/// handling that runs outside the signal handler, so it can actually process
/// pending invalidations.
pub fn ProcessCatchupInterrupt() -> PgResult<()> {
    while CATCHUP_INTERRUPT_PENDING.with(Cell::get) {
        // Cause ReceiveSharedInvalidMessages() to run, which does the work and
        // resets catchupInterruptPending. If inside a transaction we can call
        // AcceptInvalidationMessages() directly; otherwise start and immediately
        // end a transaction (the accept happens down inside transaction start).
        if xact::is_transaction_or_transaction_block::call() {
            elog(DEBUG4, "ProcessCatchupEvent inside transaction")?;
            inval::accept_invalidation_messages::call()?;
        } else {
            elog(DEBUG4, "ProcessCatchupEvent outside transaction")?;
            xact::start_transaction_command::call()?;
            xact::commit_transaction_command::call()?;
        }
    }
    Ok(())
}

// ===========================================================================
// sinvaladt.c — in-segment layout (raw, repr(C); lives inside shared memory)
// ===========================================================================

/// `ProcState` (sinvaladt.c:137) — per-backend state in the shared invalidation
/// structure. Lives in the trailing `procState[]` array of the segment.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcState {
    /// `pid_t procPid` — PID of backend, for signaling. Zero in an inactive
    /// entry.
    pub procPid: libc::pid_t,
    /// `int nextMsgNum` — next message number to read. Meaningless if
    /// `procPid == 0` or `resetState` is true.
    pub nextMsgNum: c_int,
    /// `bool resetState` — backend needs to reset its state.
    pub resetState: bool,
    /// `bool signaled` — backend has been sent catchup signal.
    pub signaled: bool,
    /// `bool hasMessages` — backend has unread messages.
    pub hasMessages: bool,
    /// `bool sendOnly` — backend only sends, never receives.
    pub sendOnly: bool,
    /// `LocalTransactionId nextLXID` — next `LocalTransactionId` to use for this
    /// idle backend slot.
    pub nextLXID: LocalTransactionId,
}

impl Default for ProcState {
    fn default() -> Self {
        Self {
            procPid: 0,
            nextMsgNum: 0,
            resetState: false,
            signaled: false,
            hasMessages: false,
            sendOnly: false,
            nextLXID: InvalidLocalTransactionId,
        }
    }
}

/// The fixed header of the `SISeg` shared cache invalidation memory segment
/// (sinvaladt.c:165). The trailing `procState[NumProcStateSlots]` and
/// `pgprocnos[NumProcStateSlots]` arrays follow this header in the segment and
/// are reached by `offsetof + index` arithmetic (see [`SISeg`]).
///
/// In C `pgprocnos` is a `int *` pointer that `SharedInvalShmemInit` points just
/// past the `procState` array; here that pointer is implicit — the `pgprocnos`
/// array is always immediately after `procState`, so [`SISeg`] computes its
/// address rather than storing a raw pointer in the header.
#[repr(C)]
struct InSegmentSISegHeader {
    /// `int minMsgNum` — oldest message still needed.
    minMsgNum: c_int,
    /// `int maxMsgNum` — next message number to be assigned.
    maxMsgNum: c_int,
    /// `int nextThreshold` — # of messages to call `SICleanupQueue`.
    nextThreshold: c_int,
    /// `slock_t msgnumLock` — spinlock protecting `maxMsgNum`. A real in-segment
    /// spinlock word.
    msgnumLock: Spinlock,
    /// `SharedInvalidationMessage buffer[MAXNUMMESSAGES]` — circular buffer.
    /// Initially all unused (never read before being written), modelled with
    /// `MaybeUninit` slots.
    buffer: [MaybeUninit<SharedInvalidationMessage>; MAXNUMMESSAGES],
    /// `int numProcs` — number of `procState` slots currently in use.
    numProcs: c_int,
    // `int *pgprocnos` — implicit (immediately follows `procState[]`).
    // `ProcState procState[FLEXIBLE_ARRAY_MEMBER]` — follows this header.
}

/// A per-process view onto the one shared `SISeg` segment. Mirrors C's
/// process-local `shmInvalBuffer`: just a handle (segment pointer + slot count),
/// never a copy of the data. Cross-process serialization is the caller's
/// responsibility (via the `SInval*Lock` LWLocks and the `msgnumLock` spinlock).
#[derive(Clone, Copy)]
struct SISeg {
    header: NonNull<InSegmentSISegHeader>,
    /// `NumProcStateSlots` — the length of the `procState`/`pgprocnos` arrays.
    slots: usize,
}

impl SISeg {
    fn header(&self) -> &InSegmentSISegHeader {
        // SAFETY: `header` was reserved by `ShmemInitStruct` and remains live for
        // the segment's lifetime; the fixed header occupies the leading bytes.
        unsafe { self.header.as_ref() }
    }

    fn header_mut(&mut self) -> &mut InSegmentSISegHeader {
        // SAFETY: as above; `&mut self` gives exclusive access to the handle.
        unsafe { self.header.as_mut() }
    }

    /// `&segP->buffer[i]` read of a circular-buffer slot known to be written.
    ///
    /// # Safety
    ///
    /// `index` must address a slot in `[minMsgNum, maxMsgNum)` (mod buffer), i.e.
    /// one previously written by `SIInsertDataEntries`.
    unsafe fn buffer_read(&self, index: usize) -> SharedInvalidationMessage {
        // SAFETY: the caller guarantees the slot was written (assume_init), and
        // `SharedInvalidationMessage` is `Copy`.
        unsafe { self.header().buffer[index].assume_init() }
    }

    fn buffer_write(&mut self, index: usize, message: SharedInvalidationMessage) {
        self.header_mut().buffer[index] = MaybeUninit::new(message);
    }

    /// `&segP->procState[i]` — the per-backend slot array following the header.
    fn proc_state_ptr(&self) -> *mut ProcState {
        // SAFETY: `procState` begins at `proc_state_offset()` and has `slots`
        // entries reserved by `SharedInvalShmemSize`.
        unsafe {
            self.header
                .as_ptr()
                .cast::<u8>()
                .add(proc_state_offset())
                .cast::<ProcState>()
        }
    }

    fn proc_state(&self, index: usize) -> &ProcState {
        debug_assert!(index < self.slots);
        // SAFETY: index < slots, so the slot is within the reserved array.
        unsafe { &*self.proc_state_ptr().add(index) }
    }

    fn proc_state_mut(&mut self, index: usize) -> &mut ProcState {
        debug_assert!(index < self.slots);
        // SAFETY: index < slots; `&mut self` gives exclusive access.
        unsafe { &mut *self.proc_state_ptr().add(index) }
    }

    /// `segP->pgprocnos` — the dense array of in-use `ProcState` indexes,
    /// immediately following the `procState[]` array in the segment.
    fn pgprocnos_ptr(&self) -> *mut c_int {
        // SAFETY: the `pgprocnos` array begins right after the `slots`-entry
        // `procState` array; both were reserved by `SharedInvalShmemSize`.
        unsafe { self.proc_state_ptr().add(self.slots).cast::<c_int>() }
    }

    /// The `numProcs` in-use entries of `pgprocnos`.
    fn pgprocnos(&self) -> &[ProcNumber] {
        let count = self.header().numProcs.max(0) as usize;
        // SAFETY: `numProcs <= slots` is maintained by the queue; the array has
        // `slots` reserved entries.
        unsafe { core::slice::from_raw_parts(self.pgprocnos_ptr(), count) }
    }

    fn pgprocnos_set(&mut self, index: usize, value: ProcNumber) {
        debug_assert!(index < self.slots);
        // SAFETY: index < slots, within the reserved `pgprocnos` array.
        unsafe { self.pgprocnos_ptr().add(index).write(value) };
    }
}

/// Read the per-process `shmInvalBuffer` handle as a fresh [`SISeg`] view.
fn current_seg_view() -> PgResult<SISeg> {
    SHM_INVAL_BUFFER
        .with(Cell::get)
        .ok_or_else(|| PgError::error("shared invalidation memory has not been initialized"))
}

fn current_proc_number() -> PgResult<ProcNumber> {
    // C: if (MyProcNumber < 0) elog(ERROR, "MyProcNumber not set");
    let proc_number = init_small::my_proc_number::call();
    if proc_number < 0 {
        return Err(PgError::error("MyProcNumber not set"));
    }
    Ok(proc_number)
}

// ===========================================================================
// LWLock + spinlock guards (the SInval*Lock named locks / in-segment msgnumLock)
// ===========================================================================

/// RAII guard for one of the named SI-queue LWLocks, acquired through the
/// lwlock main-array seam by offset.
///
/// Mirrors `LWLockAcquire(lock, mode)` / `LWLockRelease(lock)`. `Drop` is the
/// silent abort backstop (C's `LWLockReleaseAll`); [`Self::release`] is the
/// explicit release at the point where C calls `LWLockRelease`, sequencing the
/// release-before-signal / re-acquire dance in `SICleanupQueue` to match C.
struct SinvalLwLockGuard {
    lock_offset: usize,
    released: bool,
}

impl SinvalLwLockGuard {
    fn acquire(lock_offset: usize, exclusive: bool) -> PgResult<Self> {
        let mode = if exclusive {
            LWLockMode::LW_EXCLUSIVE
        } else {
            LWLockMode::LW_SHARED
        };
        // The lwlock owner returns a MainLWLockGuard whose Drop would release the
        // lock; this wrapper performs the release explicitly at the C-faithful
        // point, so forget the owner's guard to keep the release single.
        let guard = lwlock::lwlock_acquire_main::call(lock_offset, mode)?;
        core::mem::forget(guard);
        Ok(Self {
            lock_offset,
            released: false,
        })
    }

    /// Explicit `LWLockRelease`, mirroring the C source's release order.
    fn release(mut self) -> PgResult<()> {
        self.release_in_place()
    }

    fn release_in_place(&mut self) -> PgResult<()> {
        if !self.released {
            self.released = true;
            lwlock::lwlock_release_main::call(self.lock_offset)?;
        }
        Ok(())
    }
}

impl Drop for SinvalLwLockGuard {
    fn drop(&mut self) {
        // On the error/panic path, ensure the LWLock is not leaked. On the
        // success path the caller has already released, so this is a no-op.
        let _ = self.release_in_place();
    }
}

/// Acquire `segP->msgnumLock` (an in-shmem `slock_t`), run `f`, release it.
///
/// `SpinLockAcquire(&segP->msgnumLock)` / `SpinLockRelease(&segP->msgnumLock)`
/// (sinvaladt.c:422-424 / 511-513). The spinlock word lives inside the shared
/// `SISeg`, viewed through the repo's ported `s_lock` primitive.
fn with_msgnum_lock<R>(seg: &mut SISeg, f: impl FnOnce(&mut SISeg) -> R) -> R {
    // SAFETY: the spinlock word lives at a fixed offset inside the live segment
    // and remains valid for the call. We take a shared `&Spinlock` (its ops are
    // through an interior-mutable atomic), so this does not alias the `&mut SISeg`
    // borrow used inside `f` (which touches other header fields).
    let lock: &Spinlock = unsafe { &*core::ptr::addr_of!(seg.header().msgnumLock) };
    s_lock(lock, Some(file!()), line!() as i32, Some("SIInsertDataEntries"));
    let result = f(seg);
    s_unlock(lock);
    result
}

// ===========================================================================
// sinvaladt.c — sizing (SharedInvalShmemSize) + the procState offset
// ===========================================================================

fn proc_state_offset() -> usize {
    offset_of!(InSegmentSISegHeader, numProcs) + size_of::<c_int>()
}

/// `#define NumProcStateSlots (MaxBackends + NUM_AUXILIARY_PROCS)`
/// (sinvaladt.c:204).
fn num_proc_state_slots() -> PgResult<usize> {
    let slots = init_small::max_backends::call() + NUM_AUXILIARY_PROCS;
    if slots <= 0 {
        return Err(PgError::error("NumProcStateSlots is not positive"));
    }
    Ok(slots as usize)
}

/// `SharedInvalShmemSize` (sinvaladt.c:217) — return shared-memory space needed.
pub fn SharedInvalShmemSize() -> PgResult<::types_core::Size> {
    sinval_shmem_size_for_slots(num_proc_state_slots()?)
}

fn sinval_shmem_size_for_slots(slots: usize) -> PgResult<::types_core::Size> {
    // size = offsetof(SISeg, procState);
    let size = proc_state_offset();
    // size = add_size(size, mul_size(sizeof(ProcState), NumProcStateSlots));
    let size = shmem::add_size::call(size, shmem::mul_size::call(size_of::<ProcState>(), slots)?)?;
    // size = add_size(size, mul_size(sizeof(int), NumProcStateSlots));
    shmem::add_size::call(size, shmem::mul_size::call(size_of::<c_int>(), slots)?)
}

// ===========================================================================
// sinvaladt.c — SharedInvalShmemInit (sinvaladt.c:233)
// ===========================================================================

/// `SharedInvalShmemInit` (sinvaladt.c:233) — create and initialize the SI
/// message buffer.
pub fn SharedInvalShmemInit() -> PgResult<()> {
    let slots = num_proc_state_slots()?;
    // shmInvalBuffer = ShmemInitStruct("shmInvalBuffer", SharedInvalShmemSize(), &found);
    let (ptr, found) = shmem::shmem_init_struct::call("shmInvalBuffer", SharedInvalShmemSize()?)?;
    let location = NonNull::new(ptr.cast::<InSegmentSISegHeader>())
        .ok_or_else(|| PgError::error("ShmemInitStruct produced a null pointer"))?;
    if !found {
        // SAFETY: `location` is a freshly reserved region of `SharedInvalShmemSize`
        // bytes that we initialize before any reader can observe it.
        unsafe { initialize_sinval_memory(location, slots) };
    }

    SHM_INVAL_BUFFER.with(|c| {
        c.set(Some(SISeg {
            header: location,
            slots,
        }))
    });
    Ok(())
}

/// Initialize the just-reserved `SISeg` (the `if (found) return;` else-branch of
/// `SharedInvalShmemInit`).
///
/// # Safety
///
/// `header` must point at a freshly reserved, exclusively owned region of at
/// least `sinval_shmem_size_for_slots(slots)` bytes.
unsafe fn initialize_sinval_memory(header: NonNull<InSegmentSISegHeader>, slots: usize) {
    // SAFETY: caller guarantees `header` is a fresh region of the right size.
    unsafe {
        let h = &mut *header.as_ptr();
        // minMsgNum = 0; maxMsgNum = 0; nextThreshold = CLEANUP_MIN;
        h.minMsgNum = 0;
        h.maxMsgNum = 0;
        h.nextThreshold = CLEANUP_MIN;
        // SpinLockInit(&shmInvalBuffer->msgnumLock);
        h.msgnumLock = Spinlock::new();
        // The buffer[] array is initially all unused, so we need not fill it.
        // shmInvalBuffer->numProcs = 0;
        h.numProcs = 0;

        // Mark all backends inactive, and initialize nextLXID.
        let proc_state = header
            .as_ptr()
            .cast::<u8>()
            .add(proc_state_offset())
            .cast::<ProcState>();
        for index in 0..slots {
            proc_state.add(index).write(ProcState::default());
        }
        // The C sets shmInvalBuffer->pgprocnos = &procState[slots]; here that
        // address is implicit (computed by SISeg::pgprocnos_ptr). The entries are
        // read only up to `numProcs`, which is 0 here, so no init is needed.
    }
}

// ===========================================================================
// sinvaladt.c — SharedInvalBackendInit (sinvaladt.c:271)
// ===========================================================================

/// `SharedInvalBackendInit` (sinvaladt.c:271) — initialize a new backend to
/// operate on the sinval buffer.
pub fn SharedInvalBackendInit(sendOnly: bool) -> PgResult<()> {
    let proc_number = current_proc_number()?;
    let mut seg = current_seg_view()?;
    // C: if (MyProcNumber >= NumProcStateSlots) elog(PANIC, ...);
    if proc_number as usize >= seg.slots {
        return Err(PgError::new(
            PANIC,
            format!(
                "unexpected MyProcNumber {proc_number} in SharedInvalBackendInit (max {})",
                seg.slots
            ),
        ));
    }

    // C: LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE); (sinvaladt.c:290)
    // This can run in parallel with read operations, but not with write
    // operations, since SIInsertDataEntries relies on the pgprocnos array.
    let lock = SinvalLwLockGuard::acquire(SINVAL_WRITE_LOCK, true)?;

    let old_pid = seg.proc_state(proc_number as usize).procPid;
    if old_pid != 0 {
        // C releases SInvalWriteLock before the elog(ERROR). (sinvaladt.c:295)
        lock.release()?;
        return Err(PgError::error(format!(
            "sinval slot for backend {proc_number} is already in use by process {old_pid}"
        )));
    }

    // shmInvalBuffer->pgprocnos[shmInvalBuffer->numProcs++] = MyProcNumber;
    let num_procs = seg.header().numProcs as usize;
    seg.pgprocnos_set(num_procs, proc_number);
    seg.header_mut().numProcs += 1;

    // Fetch next local transaction ID into local memory.
    let max_msg_num = seg.header().maxMsgNum;
    let next_lxid;
    {
        let slot = seg.proc_state_mut(proc_number as usize);
        next_lxid = slot.nextLXID;
        // mark myself active, with all extant messages already read.
        slot.procPid = init_small::my_proc_pid::call();
        slot.nextMsgNum = max_msg_num;
        slot.resetState = false;
        slot.signaled = false;
        slot.hasMessages = false;
        slot.sendOnly = sendOnly;
    }

    // C: LWLockRelease(SInvalWriteLock); (sinvaladt.c:313)
    lock.release()?;

    NEXT_LOCAL_TRANSACTION_ID.with(|c| c.set(next_lxid));

    // C: on_shmem_exit(CleanupInvalidationState, PointerGetDatum(segP));
    // (sinvaladt.c:316). The segment is reached through the process-global
    // handle, so the arg word is unused.
    ipc::on_shmem_exit::call(cleanup_invalidation_state_callback, Datum::null())?;
    Ok(())
}

// ===========================================================================
// sinvaladt.c — CleanupInvalidationState (sinvaladt.c:327)
// ===========================================================================

/// `on_shmem_exit` trampoline matching `void (*)(int code, Datum arg)`.
fn cleanup_invalidation_state_callback(_code: c_int, _arg: Datum<'static>) -> PgResult<()> {
    CleanupInvalidationState()
}

/// `CleanupInvalidationState` (sinvaladt.c:327) — mark the current backend as no
/// longer active. Called via `on_shmem_exit()` during backend shutdown.
pub fn CleanupInvalidationState() -> PgResult<()> {
    let proc_number = current_proc_number()?;
    // nextLocalTransactionId is process-local; read it before taking the lock.
    let next_lxid = NEXT_LOCAL_TRANSACTION_ID.with(Cell::get);
    let mut seg = current_seg_view()?;
    if proc_number as usize >= seg.slots {
        return Err(PgError::new(PANIC, "could not find sinval backend slot"));
    }

    // C: LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE); (sinvaladt.c:336)
    let lock = SinvalLwLockGuard::acquire(SINVAL_WRITE_LOCK, true)?;

    {
        let slot = seg.proc_state_mut(proc_number as usize);
        // Update next local transaction ID for next holder of this proc number.
        slot.nextLXID = next_lxid;
        // Mark myself inactive.
        slot.procPid = 0;
        slot.nextMsgNum = 0;
        slot.resetState = false;
        slot.signaled = false;
    }

    // for (i = segP->numProcs - 1; i >= 0; i--) ...
    let found = seg
        .pgprocnos()
        .iter()
        .position(|entry| *entry == proc_number);
    let Some(index) = found else {
        // C holds SInvalWriteLock through the elog(PANIC) (sinvaladt.c:358); the
        // panic terminates the process so the lock state is irrelevant, but we
        // release on the error path for tidiness.
        lock.release()?;
        return Err(PgError::new(PANIC, "could not find entry in sinval array"));
    };
    let last_index = seg.header().numProcs as usize - 1;
    if index != last_index {
        let last = seg.pgprocnos()[last_index];
        seg.pgprocnos_set(index, last);
    }
    seg.header_mut().numProcs -= 1;

    // C: LWLockRelease(SInvalWriteLock); (sinvaladt.c:362)
    lock.release()?;
    Ok(())
}

// ===========================================================================
// sinvaladt.c — SIInsertDataEntries (sinvaladt.c:369)
// ===========================================================================

/// `SIInsertDataEntries` (sinvaladt.c:369) — add new invalidation message(s) to
/// the buffer.
///
/// `n` can be arbitrarily large; the work is divided into groups of no more than
/// [`WRITE_QUANTUM`] messages so the lock is not held too long, and so
/// `SICleanupQueue` is considered once per iteration.
pub fn SIInsertDataEntries(data: &[SharedInvalidationMessage]) -> PgResult<()> {
    let mut offset = 0;
    while offset < data.len() {
        let nthistime = (data.len() - offset).min(WRITE_QUANTUM);
        insert_data_entries_chunk(&data[offset..offset + nthistime])?;
        offset += nthistime;
    }
    Ok(())
}

fn insert_data_entries_chunk(data: &[SharedInvalidationMessage]) -> PgResult<()> {
    let mut seg = current_seg_view()?;

    // C: LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE); (sinvaladt.c:392)
    let mut lock = Some(SinvalLwLockGuard::acquire(SINVAL_WRITE_LOCK, true)?);

    // C: If the buffer is full, we *must* acquire some space; otherwise clean the
    // queue only when it's exceeded the next fullness threshold. We loop and
    // recheck after any SICleanupQueue (called with callerHasWriteLock = true, so
    // it returns with SInvalWriteLock held). (sinvaladt.c:401-409)
    loop {
        let header = seg.header();
        let num_msgs = header.maxMsgNum - header.minMsgNum;
        if num_msgs + data.len() as c_int > MAXNUMMESSAGES as c_int
            || num_msgs >= header.nextThreshold
        {
            lock = cleanup_queue_locked(&mut seg, lock.take(), data.len() as c_int)?;
        } else {
            break;
        }
    }
    let mut lock = lock.ok_or_else(|| {
        PgError::error("insert_data_entries_chunk: SInvalWriteLock not held after the cleanup loop")
    })?;

    // C: insert new message(s) into proper slot of circular buffer.
    // (sinvaladt.c:414-419)
    let mut max = seg.header().maxMsgNum;
    for message in data {
        seg.buffer_write(max as usize % MAXNUMMESSAGES, *message);
        max += 1;
    }

    // C: update current value of maxMsgNum using the msgnumLock spinlock.
    // (sinvaladt.c:422-424)
    with_msgnum_lock(&mut seg, |seg| {
        seg.header_mut().maxMsgNum = max;
    });

    // C: now give everyone a swift kick to read the new messages.
    // (sinvaladt.c:433-438)
    let num_procs = seg.header().numProcs.max(0) as usize;
    for i in 0..num_procs {
        let proc_number = seg.pgprocnos()[i];
        seg.proc_state_mut(proc_number as usize).hasMessages = true;
    }

    // C: LWLockRelease(SInvalWriteLock); (sinvaladt.c:440)
    lock.release_in_place()?;
    Ok(())
}

// ===========================================================================
// sinvaladt.c — SIGetDataEntries (sinvaladt.c:472)
// ===========================================================================

/// `SIGetDataEntries` (sinvaladt.c:472) — get next SI message(s) for the current
/// backend, if there are any.
///
/// Returns `0` (no SI message available), `n > 0` (the next `n` SI messages were
/// extracted into `data[]`), or `-1` (an SI reset message was extracted).
pub fn SIGetDataEntries(data: &mut [SharedInvalidationMessage]) -> PgResult<c_int> {
    let proc_number = current_proc_number()?;
    let mut seg = current_seg_view()?;
    if proc_number as usize >= seg.slots {
        return Err(PgError::error("invalid sinval backend slot"));
    }

    // C: before starting to take locks, do a quick, unlocked test to see whether
    // there can possibly be anything to read. (sinvaladt.c:494-495)
    if !seg.proc_state(proc_number as usize).hasMessages {
        return Ok(0);
    }

    // C: LWLockAcquire(SInvalReadLock, LW_SHARED); (sinvaladt.c:497)
    let lock = SinvalLwLockGuard::acquire(SINVAL_READ_LOCK, false)?;

    // C: we must reset hasMessages before determining how many messages we're
    // going to read. (sinvaladt.c:508)
    seg.proc_state_mut(proc_number as usize).hasMessages = false;

    // C: fetch current value of maxMsgNum using the msgnumLock spinlock.
    // (sinvaladt.c:511-513)
    let max = with_msgnum_lock(&mut seg, |seg| seg.header().maxMsgNum);

    if seg.proc_state(proc_number as usize).resetState {
        // C: Force reset. We can say we have dealt with any messages added since
        // the reset, and clear the signaled flag too. (sinvaladt.c:515-526)
        let slot = seg.proc_state_mut(proc_number as usize);
        slot.nextMsgNum = max;
        slot.resetState = false;
        slot.signaled = false;
        // C: LWLockRelease(SInvalReadLock); return -1; (sinvaladt.c:525-526)
        lock.release()?;
        return Ok(-1);
    }

    // C: Retrieve messages and advance backend's counter, until data array is
    // full or there are no more messages. (sinvaladt.c:537-542)
    let mut n = 0;
    while n < data.len() && seg.proc_state(proc_number as usize).nextMsgNum < max {
        let next = seg.proc_state(proc_number as usize).nextMsgNum;
        // SAFETY: next < max and next >= minMsgNum, so the slot was written.
        data[n] = unsafe { seg.buffer_read(next as usize % MAXNUMMESSAGES) };
        seg.proc_state_mut(proc_number as usize).nextMsgNum += 1;
        n += 1;
    }

    // C: If we have caught up completely, reset our "signaled" flag so we'll get
    // another signal if we fall behind again. Otherwise reset hasMessages so we
    // see the remaining messages next time. (sinvaladt.c:551-554)
    {
        let slot = seg.proc_state_mut(proc_number as usize);
        if slot.nextMsgNum >= max {
            slot.signaled = false;
        } else {
            slot.hasMessages = true;
        }
    }

    // C: LWLockRelease(SInvalReadLock); (sinvaladt.c:556)
    lock.release()?;
    Ok(n as c_int)
}

// ===========================================================================
// sinvaladt.c — SICleanupQueue (sinvaladt.c:576)
// ===========================================================================

/// `SICleanupQueue` (sinvaladt.c:576) — remove messages that have been consumed
/// by all active backends.
///
/// `caller_has_write_lock` is true if the caller is holding `SInvalWriteLock`.
/// `minFree` is the minimum number of message slots to make free.
pub fn SICleanupQueue(caller_has_write_lock: bool, minFree: c_int) -> PgResult<()> {
    let mut seg = current_seg_view()?;
    if caller_has_write_lock {
        // The caller already holds SInvalWriteLock; synthesize the held guard so
        // the release-before-signal / re-acquire dance matches C, then release
        // the synthesized one (the caller keeps holding the lock conceptually).
        let held = SinvalLwLockGuard::acquire(SINVAL_WRITE_LOCK, true)?;
        let returned = cleanup_queue_locked(&mut seg, Some(held), minFree)?;
        if let Some(guard) = returned {
            guard.release()?;
        }
        Ok(())
    } else {
        let returned = cleanup_queue_locked(&mut seg, None, minFree)?;
        debug_assert!(returned.is_none());
        Ok(())
    }
}

/// Body of `SICleanupQueue(callerHasWriteLock, minFree)` (sinvaladt.c:576-684).
///
/// `caller_write_lock` is `Some(guard)` when the caller (e.g.
/// `SIInsertDataEntries`) already holds `SInvalWriteLock`; the guard is owned by
/// this routine so it can release it before `SendProcSignal` and re-acquire it
/// afterward, exactly as C does. The (possibly re-acquired) write-lock guard is
/// handed back to the caller. This routine additionally takes `SInvalReadLock`
/// exclusive, recomputes `minMsgNum`, and resets/identifies lagging backends.
fn cleanup_queue_locked(
    seg: &mut SISeg,
    caller_write_lock: Option<SinvalLwLockGuard>,
    minFree: c_int,
) -> PgResult<Option<SinvalLwLockGuard>> {
    let caller_has_write_lock = caller_write_lock.is_some();
    // C: Lock out all writers and readers. (sinvaladt.c:588-590)
    let mut write_lock = match caller_write_lock {
        Some(guard) => guard,
        None => SinvalLwLockGuard::acquire(SINVAL_WRITE_LOCK, true)?,
    };
    let read_lock = SinvalLwLockGuard::acquire(SINVAL_READ_LOCK, true)?;

    // C: Recompute minMsgNum = minimum of all backends' nextMsgNum, identify the
    // furthest-back backend that needs signaling, reset any too-far-back ones.
    // (sinvaladt.c:599-634)
    let mut min = seg.header().maxMsgNum;
    let mut minsig = min - SIG_THRESHOLD;
    let lowbound = min - MAXNUMMESSAGES as c_int + minFree;
    let mut need_signal: Option<ProcNumber> = None;

    let num_procs = seg.header().numProcs.max(0) as usize;
    for i in 0..num_procs {
        let proc_number = seg.pgprocnos()[i];
        let slot = seg.proc_state(proc_number as usize);
        let n = slot.nextMsgNum;
        // Ignore if already in reset state. Assert(stateP->procPid != 0).
        debug_assert!(slot.procPid != 0);
        if slot.resetState || slot.sendOnly {
            continue;
        }
        // If we must free space and this backend is preventing it, force reset.
        if n < lowbound {
            seg.proc_state_mut(proc_number as usize).resetState = true;
            // no point in signaling him ...
            continue;
        }
        // Track the global minimum nextMsgNum.
        if n < min {
            min = n;
        }
        // Also see who's furthest back of the unsignaled backends.
        if n < minsig && !slot.signaled {
            minsig = n;
            need_signal = Some(proc_number);
        }
    }
    seg.header_mut().minMsgNum = min;

    // C: When minMsgNum gets really large, decrement all message counters to
    // forestall overflow. (sinvaladt.c:642-648)
    if min >= MSGNUMWRAPAROUND {
        seg.header_mut().minMsgNum -= MSGNUMWRAPAROUND;
        seg.header_mut().maxMsgNum -= MSGNUMWRAPAROUND;
        for i in 0..num_procs {
            let proc_number = seg.pgprocnos()[i];
            seg.proc_state_mut(proc_number as usize).nextMsgNum -= MSGNUMWRAPAROUND;
        }
    }

    // C: Determine how many messages are still queued, set the next threshold.
    // (sinvaladt.c:654-658)
    let num_msgs = seg.header().maxMsgNum - seg.header().minMsgNum;
    seg.header_mut().nextThreshold = if num_msgs < CLEANUP_MIN {
        CLEANUP_MIN
    } else {
        (num_msgs / CLEANUP_QUANTUM + 1) * CLEANUP_QUANTUM
    };

    // C: Lastly, signal anyone who needs a catchup interrupt. SendProcSignal
    // might not be fast, so we don't hold locks while executing it.
    // (sinvaladt.c:665-683)
    if let Some(proc_number) = need_signal {
        let his_pid;
        {
            let slot = seg.proc_state_mut(proc_number as usize);
            slot.signaled = true;
            his_pid = slot.procPid;
        }
        // C: ProcNumber his_procNumber = (needSig - &segP->procState[0]); — the
        // ProcState index, which is the pgprocno we already have.
        // C: LWLockRelease(SInvalReadLock); LWLockRelease(SInvalWriteLock);
        read_lock.release()?;
        write_lock.release_in_place()?;
        elog(DEBUG4, format!("sending sinval catchup signal to PID {his_pid}"))?;
        procsignal::send_proc_signal::call(
            his_pid,
            ProcSignalReason::PROCSIG_CATCHUP_INTERRUPT,
            proc_number,
        );
        if caller_has_write_lock {
            // C: re-acquire SInvalWriteLock for the caller. (sinvaladt.c:675-676)
            return Ok(Some(SinvalLwLockGuard::acquire(SINVAL_WRITE_LOCK, true)?));
        }
        Ok(None)
    } else {
        // C: LWLockRelease(SInvalReadLock); release write lock iff we took it.
        // (sinvaladt.c:680-682)
        read_lock.release()?;
        if caller_has_write_lock {
            Ok(Some(write_lock))
        } else {
            write_lock.release()?;
            Ok(None)
        }
    }
}

// ===========================================================================
// sinvaladt.c — GetNextLocalTransactionId (sinvaladt.c:700)
// ===========================================================================

/// `GetNextLocalTransactionId` (sinvaladt.c:700) — allocate a new
/// `LocalTransactionId` from the process-local counter, looping to avoid
/// returning `InvalidLocalTransactionId` at wraparound. Infallible (backend
/// global).
pub fn GetNextLocalTransactionId() -> LocalTransactionId {
    loop {
        // result = nextLocalTransactionId++;
        let result = NEXT_LOCAL_TRANSACTION_ID.with(Cell::get);
        NEXT_LOCAL_TRANSACTION_ID.with(|c| c.set(result.wrapping_add(1)));
        // while (!LocalTransactionIdIsValid(result));
        if result != InvalidLocalTransactionId {
            return result;
        }
    }
}

// ===========================================================================
// Seam wiring
// ===========================================================================

/// Install this crate's implementations into its seam crate.
pub fn init_seams() {
    sinval_seams::send_shared_invalid_messages::set(SendSharedInvalidMessages);
    sinval_seams::receive_shared_invalid_messages::set(
        ReceiveSharedInvalidMessages,
    );
    sinval_seams::handle_catchup_interrupt::set(HandleCatchupInterrupt);
    sinval_seams::shared_inval_backend_init::set(SharedInvalBackendInit);
    sinval_seams::get_next_local_transaction_id::set(GetNextLocalTransactionId);
    sinval_seams::shared_invalid_message_counter::set(
        SharedInvalidMessageCounter,
    );
    sinval_seams::shared_inval_shmem_size::set(SharedInvalShmemSize);
    sinval_seams::shared_inval_shmem_init::set(SharedInvalShmemInit);
}

#[cfg(test)]
mod tests;
