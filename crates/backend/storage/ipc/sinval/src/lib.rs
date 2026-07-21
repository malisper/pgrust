// Barrier mapping vs C (notes/sinval-atomics.md): the ring is cross-thread
// state, so every shared field is an atomic or an UnsafeCell slot. C's plain
// accesses under SInval{Read,Write}Lock map to Relaxed (the LWLock provides
// the ordering, as in C); C's tolerated unlocked hasMessages read
// (sinvaladt.c:483-495) is an Acquire load paired with the sender's Release
// store; maxMsgNum keeps C's msgnumLock spinlock as the publication barrier
// for buffer slots (sinvaladt.c:95-101).
#![allow(non_snake_case)]

use std::cell::{Cell, RefCell, UnsafeCell};
use std::mem::size_of;
use std::ptr::NonNull;
use std::sync::atomic::{
    AtomicBool, AtomicI32, AtomicPtr, AtomicU32, AtomicUsize,
    Ordering::{Acquire, Relaxed, Release},
};

use elog::elog;
use init_small::globals::{MaxBackends, MyLatch, MyProcNumber, MyProcPid};
use lwlock::{main_lock, LWLockAcquire, LWLockRelease, LW_EXCLUSIVE, LW_SHARED};
use types_core::xact::InvalidLocalTransactionId;
use types_core::{LocalTransactionId, ProcNumber};
use types_error::{PgError, PgResult, DEBUG4, PANIC};
use types_storage::sinval::{SharedInvalCatcacheMsg, SHARED_INVALIDATION_MESSAGE_SIZE};
use types_storage::storage::Spinlock;
use types_storage::{
    ProcSignalReason, SharedInvalidationMessage, NUM_AUXILIARY_PROCS, SINVAL_READ_LOCK,
    SINVAL_WRITE_LOCK,
};

pub const MAXNUMMESSAGES: i32 = 4096;
const MSGNUMWRAPAROUND: i32 = MAXNUMMESSAGES * 262144;
const CLEANUP_MIN: i32 = MAXNUMMESSAGES / 2;
const CLEANUP_QUANTUM: i32 = MAXNUMMESSAGES / 16;
const SIG_THRESHOLD: i32 = MAXNUMMESSAGES / 2;
const WRITE_QUANTUM: usize = 64;
const MAXINVALMSGS: usize = 32;

type WireMsg = [u8; SHARED_INVALIDATION_MESSAGE_SIZE];

pub struct ProcState {
    procPid: AtomicI32,
    nextMsgNum: AtomicI32,
    resetState: AtomicBool,
    signaled: AtomicBool,
    hasMessages: AtomicBool,
    sendOnly: AtomicBool,
    nextLXID: AtomicU32,
}

const _: () = assert!(size_of::<ProcState>() == 16);

#[repr(C)]
struct SISegHdr {
    minMsgNum: AtomicI32,
    maxMsgNum: AtomicI32,
    nextThreshold: AtomicI32,
    msgnumLock: Spinlock,
    buffer: [UnsafeCell<WireMsg>; MAXNUMMESSAGES as usize],
    numProcs: AtomicI32,
    // ProcState[NumProcStateSlots] then pgprocnos i32[NumProcStateSlots]
    // follow the header in the segment (C's FLEXIBLE_ARRAY_MEMBER + pointer).
}

#[derive(Clone, Copy)]
struct SISeg {
    base: NonNull<SISegHdr>,
    slots: usize,
}

impl SISeg {
    fn hdr(&self) -> &SISegHdr {
        // SAFETY: `base` addresses a live segment of SharedInvalShmemSize
        // bytes, initialized before publication, never freed.
        unsafe { self.base.as_ref() }
    }

    fn proc_states(&self) -> &[ProcState] {
        // SAFETY: `slots` ProcState entries follow the header, initialized by
        // init_segment; all fields are atomics, safe under shared refs.
        unsafe {
            std::slice::from_raw_parts(self.base.as_ptr().add(1).cast::<ProcState>(), self.slots)
        }
    }

    fn pgprocnos(&self) -> &[AtomicI32] {
        // SAFETY: `slots` i32 entries follow the ProcState array within the
        // reserved segment.
        unsafe {
            std::slice::from_raw_parts(
                self.base
                    .as_ptr()
                    .add(1)
                    .cast::<ProcState>()
                    .add(self.slots)
                    .cast::<AtomicI32>(),
                self.slots,
            )
        }
    }

    fn buffer_write(&self, msgnum: i32, msg: &SharedInvalidationMessage) {
        let slot = &self.hdr().buffer[(msgnum % MAXNUMMESSAGES) as usize];
        // SAFETY: only SIInsertDataEntries writes slots, under exclusive
        // SInvalWriteLock, and only at msgnum >= maxMsgNum, which no reader
        // can observe until the msgnumLock-bracketed maxMsgNum store.
        unsafe { *slot.get() = msg.to_wire_bytes() };
    }

    fn buffer_read(&self, msgnum: i32) -> SharedInvalidationMessage {
        let slot = &self.hdr().buffer[(msgnum % MAXNUMMESSAGES) as usize];
        // SAFETY: msgnum < the maxMsgNum fetched under msgnumLock, whose
        // acquire/release pair orders the slot write before this read; slots
        // below maxMsgNum are never rewritten until recycled past minMsgNum.
        let raw = unsafe { *slot.get() };
        SharedInvalidationMessage::from_wire_bytes(raw)
            .unwrap_or_else(|| panic!("unrecognized SI message ID {}", raw[0] as i8))
    }
}

const RECEIVE_PLACEHOLDER: SharedInvalidationMessage =
    SharedInvalidationMessage::Catcache(SharedInvalCatcacheMsg {
        id: 0,
        dbId: 0,
        hashValue: 0,
    });

struct Local {
    counter: Cell<u64>,
    catchup_pending: Cell<bool>,
    messages: RefCell<[SharedInvalidationMessage; MAXINVALMSGS]>,
    nextmsg: Cell<i32>,
    nummsgs: Cell<i32>,
    next_lxid: Cell<LocalTransactionId>,
    seg: Cell<Option<SISeg>>,
    // Our slot in proc_states, cached at SharedInvalBackendInit — C caches the
    // whole stateP pointer; the per-statement probe must not re-derive it
    // through the MyProcNumber TLS read. < 0 until initialized.
    my_procno: Cell<i32>,
}

thread_local! {
    static LOCAL: Local = const {
        Local {
            counter: Cell::new(0),
            catchup_pending: Cell::new(false),
            messages: RefCell::new([RECEIVE_PLACEHOLDER; MAXINVALMSGS]),
            nextmsg: Cell::new(0),
            nummsgs: Cell::new(0),
            next_lxid: Cell::new(InvalidLocalTransactionId),
            seg: Cell::new(None),
            my_procno: Cell::new(-1),
        }
    };
}

#[inline]
pub fn SharedInvalidMessageCounter() -> u64 {
    LOCAL.with(|st| st.counter.get())
}

#[inline]
pub fn catchupInterruptPending() -> bool {
    LOCAL.with(|st| st.catchup_pending.get())
}

fn current_seg() -> SISeg {
    if let Some(seg) = LOCAL.with(|st| st.seg.get()) {
        return seg;
    }
    // C maps shmInvalBuffer at shmem attach, so threads without a
    // SharedInvalBackendInit (startup redo) can still insert; bind lazily.
    attach_seg();
    LOCAL
        .with(|st| st.seg.get())
        .expect("shared invalidation memory is not attached (SharedInvalShmemInit)")
}

// C attaches shmInvalBuffer per-process at shmem attach; in the thread model
// SharedInvalShmemInit runs once (postmaster thread) and publishes here, and
// each backend thread binds its TLS copy at SharedInvalBackendInit.
static SEG_BASE: AtomicPtr<SISegHdr> = AtomicPtr::new(std::ptr::null_mut());
static SEG_SLOTS: AtomicUsize = AtomicUsize::new(0);

fn attach_seg() {
    LOCAL.with(|st| {
        if st.seg.get().is_none() {
            if let Some(base) = NonNull::new(SEG_BASE.load(Acquire)) {
                st.seg.set(Some(SISeg {
                    base,
                    slots: SEG_SLOTS.load(Relaxed),
                }));
            }
        }
    });
}

fn spin_acquire(lock: &Spinlock, func: &'static str) {
    if lock.tas() != 0 {
        let mut delay = s_lock_seams::SpinDelayStatus::new(file!(), line!() as i32, func);
        while lock.tas_spin() != 0 {
            s_lock_seams::perform_spin_delay::call(&mut delay);
        }
        s_lock_seams::finish_spin_delay::call(&delay);
    }
}

fn num_proc_state_slots() -> usize {
    (MaxBackends() + NUM_AUXILIARY_PROCS) as usize
}

pub fn SharedInvalShmemSize() -> PgResult<usize> {
    let slots = num_proc_state_slots();
    let size = size_of::<SISegHdr>();
    let size = shmem_seams::add_size::call(
        size,
        shmem_seams::mul_size::call(size_of::<ProcState>(), slots)?,
    )?;
    shmem_seams::add_size::call(size, shmem_seams::mul_size::call(size_of::<i32>(), slots)?)
}

pub fn SharedInvalShmemInit() -> PgResult<()> {
    let slots = num_proc_state_slots();
    let (ptr, found) =
        shmem_seams::shmem_init_struct::call("shmInvalBuffer", SharedInvalShmemSize()?)?;
    let base = NonNull::new(ptr.cast::<SISegHdr>())
        .ok_or_else(|| Box::new(PgError::error("ShmemInitStruct returned NULL")))?;
    let seg = SISeg { base, slots };
    if !found {
        init_segment(seg);
    }
    SEG_SLOTS.store(slots, Relaxed);
    SEG_BASE.store(seg.base.as_ptr(), Release);
    LOCAL.with(|st| st.seg.set(Some(seg)));
    Ok(())
}

/// Crash-cycle reset in place to the post-SharedInvalShmemInit image
/// (notes/crash-restart-design.md); postmaster thread only, all children dead.
pub fn SharedInvalShmemResetAfterCrash() {
    let base = NonNull::new(SEG_BASE.load(Acquire))
        .expect("SharedInvalShmemResetAfterCrash before SharedInvalShmemInit");
    init_segment(SISeg {
        base,
        slots: SEG_SLOTS.load(Relaxed),
    });
}

fn init_segment(seg: SISeg) {
    let h = seg.hdr();
    h.minMsgNum.store(0, Relaxed);
    h.maxMsgNum.store(0, Relaxed);
    h.nextThreshold.store(CLEANUP_MIN, Relaxed);
    h.msgnumLock.unlock();
    h.numProcs.store(0, Relaxed);
    for state in seg.proc_states() {
        state.procPid.store(0, Relaxed);
        state.nextMsgNum.store(0, Relaxed);
        state.resetState.store(false, Relaxed);
        state.signaled.store(false, Relaxed);
        state.hasMessages.store(false, Relaxed);
        state.sendOnly.store(false, Relaxed);
        state.nextLXID.store(InvalidLocalTransactionId, Relaxed);
    }
}

pub fn SharedInvalBackendInit(sendOnly: bool) -> PgResult<()> {
    let my = MyProcNumber();
    if my < 0 {
        return Err(Box::new(PgError::error("MyProcNumber not set")));
    }
    attach_seg();
    let seg = current_seg();
    if my as usize >= seg.slots {
        return Err(Box::new(PgError::new(
            PANIC,
            format!(
                "unexpected MyProcNumber {my} in SharedInvalBackendInit (max {})",
                seg.slots
            ),
        )));
    }
    let state = &seg.proc_states()[my as usize];

    let write_lock = main_lock(SINVAL_WRITE_LOCK);
    LWLockAcquire(write_lock, LW_EXCLUSIVE, my)?;

    let old_pid = state.procPid.load(Relaxed);
    if old_pid != 0 {
        LWLockRelease(write_lock)?;
        return Err(Box::new(PgError::error(format!(
            "sinval slot for backend {my} is already in use by process {old_pid}"
        ))));
    }

    let h = seg.hdr();
    let num_procs = h.numProcs.load(Relaxed);
    seg.pgprocnos()[num_procs as usize].store(my, Relaxed);
    h.numProcs.store(num_procs + 1, Relaxed);

    let next_lxid = state.nextLXID.load(Relaxed);
    state.procPid.store(MyProcPid(), Relaxed);
    state.nextMsgNum.store(h.maxMsgNum.load(Relaxed), Relaxed);
    state.resetState.store(false, Relaxed);
    state.signaled.store(false, Relaxed);
    state.hasMessages.store(false, Relaxed);
    state.sendOnly.store(sendOnly, Relaxed);

    LWLockRelease(write_lock)?;

    LOCAL.with(|st| {
        st.next_lxid.set(next_lxid);
        st.my_procno.set(my);
    });

    ipc_seams::on_shmem_exit::call(cleanup_invalidation_state_callback, 0);
    Ok(())
}

fn cleanup_invalidation_state_callback(_code: i32, _arg: usize) {
    // Retention park (wretain): the slot stays registered so DDL between
    // tasks accumulates against our nextMsgNum (drained at the next claim by
    // AcceptInvalidationMessages; SICleanupQueue's resetState arm covers a
    // long park). ReattachRetainedBackend re-arms this callback.
    if init_small::wretain::parking() {
        init_small::wretain::note_sinval_retained();
        return;
    }
    CleanupInvalidationState().expect("CleanupInvalidationState failed");
}

/// Retention claim (wretain): the retained slot is live; refresh its pid
/// (per-task synthetic pids — catchup signals must target the live one),
/// re-register the exit callback the park teardown consumed, and
/// sanity-check ownership.
pub fn ReattachRetainedBackend() -> PgResult<()> {
    let seg = current_seg();
    let my = MyProcNumber();
    let cached = LOCAL.with(|st| st.my_procno.get());
    if cached != my || my < 0 {
        return Err(Box::new(PgError::new(
            PANIC,
            format!("retained sinval slot procno mismatch: cached {cached}, MyProcNumber {my}"),
        )));
    }
    let state = &seg.proc_states()[my as usize];
    if state.procPid.load(Relaxed) == 0 {
        return Err(Box::new(PgError::new(
            PANIC,
            format!("retained sinval slot {my} is not registered"),
        )));
    }
    // Plain store, no lock: concurrent SICleanupQueue readers only use this
    // for the catchup SendProcSignal target; either pid value is benign.
    state.procPid.store(MyProcPid(), Relaxed);
    ipc_seams::on_shmem_exit::call(cleanup_invalidation_state_callback, 0);
    Ok(())
}

/// Debug invariant (InitPostgres's warm arm): the retained slot was
/// reattached for THIS task — cached procno matches MyProcNumber and the
/// slot's procPid was refreshed to the task's pid (ReattachRetainedBackend
/// ran after InitProcessGlobals assigned it).
pub fn RetainedSlotIsCurrent() -> bool {
    let my = MyProcNumber();
    if my < 0 || LOCAL.with(|st| st.my_procno.get()) != my {
        return false;
    }
    let seg = current_seg();
    (my as usize) < seg.slots
        && seg.proc_states()[my as usize].procPid.load(Relaxed) == MyProcPid()
}

pub fn CleanupInvalidationState() -> PgResult<()> {
    let seg = current_seg();
    let my = MyProcNumber();
    let next_lxid = LOCAL.with(|st| st.next_lxid.get());

    let write_lock = main_lock(SINVAL_WRITE_LOCK);
    LWLockAcquire(write_lock, LW_EXCLUSIVE, my)?;

    let state = &seg.proc_states()[my as usize];
    state.nextLXID.store(next_lxid, Relaxed);
    state.procPid.store(0, Relaxed);
    state.nextMsgNum.store(0, Relaxed);
    state.resetState.store(false, Relaxed);
    state.signaled.store(false, Relaxed);

    let h = seg.hdr();
    let num_procs = h.numProcs.load(Relaxed) as usize;
    let pgprocnos = seg.pgprocnos();
    let Some(index) = (0..num_procs).rfind(|&i| pgprocnos[i].load(Relaxed) == my) else {
        // Release before erroring: this cleanup runs on exit/park teardown
        // paths whose callers may not unwind through ProcKill's
        // LWLockReleaseAll (a retained-park release drains no exit
        // callbacks) — an early return holding the sinval write lock
        // silently wedges every later backend exit and sinval catchup.
        LWLockRelease(write_lock)?;
        return Err(Box::new(PgError::new(
            PANIC,
            "could not find entry in sinval array",
        )));
    };
    if index != num_procs - 1 {
        pgprocnos[index].store(pgprocnos[num_procs - 1].load(Relaxed), Relaxed);
    }
    h.numProcs.store(num_procs as i32 - 1, Relaxed);

    LWLockRelease(write_lock)?;
    Ok(())
}

pub fn SendSharedInvalidMessages(msgs: &[SharedInvalidationMessage]) -> PgResult<()> {
    SIInsertDataEntries(msgs)
}

pub fn SIInsertDataEntries(data: &[SharedInvalidationMessage]) -> PgResult<()> {
    let seg = current_seg();
    let write_lock = main_lock(SINVAL_WRITE_LOCK);
    let mut rest = data;
    while !rest.is_empty() {
        let (chunk, tail) = rest.split_at(rest.len().min(WRITE_QUANTUM));
        rest = tail;

        LWLockAcquire(write_lock, LW_EXCLUSIVE, MyProcNumber())?;

        let h = seg.hdr();
        loop {
            let num_msgs = h.maxMsgNum.load(Relaxed) - h.minMsgNum.load(Relaxed);
            if num_msgs + chunk.len() as i32 > MAXNUMMESSAGES
                || num_msgs >= h.nextThreshold.load(Relaxed)
            {
                SICleanupQueue(true, chunk.len() as i32)?;
            } else {
                break;
            }
        }

        let mut max = h.maxMsgNum.load(Relaxed);
        for msg in chunk {
            seg.buffer_write(max, msg);
            max += 1;
        }

        spin_acquire(&h.msgnumLock, "SIInsertDataEntries");
        h.maxMsgNum.store(max, Relaxed);
        h.msgnumLock.unlock();

        let num_procs = h.numProcs.load(Relaxed) as usize;
        for i in 0..num_procs {
            let procno = seg.pgprocnos()[i].load(Relaxed);
            seg.proc_states()[procno as usize]
                .hasMessages
                .store(true, Release);
        }

        LWLockRelease(write_lock)?;
    }
    Ok(())
}

fn SIGetDataEntries(seg: SISeg, data: &mut [SharedInvalidationMessage]) -> PgResult<i32> {
    let my = MyProcNumber();
    let state = &seg.proc_states()[my as usize];

    if !state.hasMessages.load(Acquire) {
        return Ok(0);
    }

    let read_lock = main_lock(SINVAL_READ_LOCK);
    LWLockAcquire(read_lock, LW_SHARED, my)?;

    // Reset hasMessages before fetching maxMsgNum, so messages arriving after
    // the fetch re-set the flag (sinvaladt.c:499-508).
    state.hasMessages.store(false, Relaxed);

    let h = seg.hdr();
    spin_acquire(&h.msgnumLock, "SIGetDataEntries");
    let max = h.maxMsgNum.load(Relaxed);
    h.msgnumLock.unlock();

    if state.resetState.load(Relaxed) {
        state.nextMsgNum.store(max, Relaxed);
        state.resetState.store(false, Relaxed);
        state.signaled.store(false, Relaxed);
        LWLockRelease(read_lock)?;
        return Ok(-1);
    }

    let mut n = 0usize;
    // Single nextMsgNum store: intermediate values are unobservable (any
    // other reader of this slot holds SInvalReadLock exclusive).
    let mut next = state.nextMsgNum.load(Relaxed);
    while n < data.len() && next < max {
        data[n] = seg.buffer_read(next);
        next += 1;
        n += 1;
    }
    state.nextMsgNum.store(next, Relaxed);

    if next >= max {
        state.signaled.store(false, Relaxed);
    } else {
        state.hasMessages.store(true, Relaxed);
    }

    LWLockRelease(read_lock)?;
    Ok(n as i32)
}

pub fn ReceiveSharedInvalidMessages(
    inval_function: &mut dyn FnMut(&SharedInvalidationMessage) -> PgResult<()>,
    reset_function: &mut dyn FnMut() -> PgResult<()>,
) -> PgResult<()> {
    LOCAL.with(|st| {
        // Per-statement empty-queue probe, C's order (sinvaladt.c:473): ONE
        // Acquire load of hasMessages (through the procno cached at backend
        // init, C's stateP) before the receive buffer's RefCell or the
        // message-cursor Cells are touched. No pending outer-recursion
        // messages (nextmsg == nummsgs holds after every drain), no shared
        // messages, no catchup: done. Kept out of receive_impl so the empty
        // statement never pays its buffer-sized frame; the slow path
        // re-checks everything under SInvalReadLock.
        if st.nextmsg.get() >= st.nummsgs.get() && !st.catchup_pending.get() {
            let procno = st.my_procno.get();
            if procno >= 0 {
                let seg = st.seg.get().expect("shared invalidation memory is not attached");
                if !seg.proc_states()[procno as usize].hasMessages.load(Acquire) {
                    return Ok(());
                }
            }
        }
        receive_impl(st, inval_function, reset_function)
    })
}

#[inline(never)]
fn receive_impl(
    st: &Local,
    inval_function: &mut dyn FnMut(&SharedInvalidationMessage) -> PgResult<()>,
    reset_function: &mut dyn FnMut() -> PgResult<()>,
) -> PgResult<()> {
    // Messages still pending from an outer recursion. Each message is copied
    // out before the callback runs (C: msg = messages[nextmsg++]), so no
    // borrow is live if inval_function recurses back here.
    while st.nextmsg.get() < st.nummsgs.get() {
        let cur = st.nextmsg.get();
        st.nextmsg.set(cur + 1);
        let msg = st.messages.borrow()[cur as usize];
        st.counter.set(st.counter.get().wrapping_add(1));
        inval_function(&msg)?;
    }

    loop {
        st.nextmsg.set(0);
        st.nummsgs.set(0);

        let get_result = {
            let seg = st.seg.get().expect("shared invalidation memory is not attached");
            let mut buf = st.messages.borrow_mut();
            SIGetDataEntries(seg, &mut buf[..])?
        };

        if get_result < 0 {
            elog(DEBUG4, "cache state reset")?;
            st.counter.set(st.counter.get().wrapping_add(1));
            reset_function()?;
            break;
        }

        st.nextmsg.set(0);
        st.nummsgs.set(get_result);

        while st.nextmsg.get() < st.nummsgs.get() {
            let cur = st.nextmsg.get();
            st.nextmsg.set(cur + 1);
            let msg = st.messages.borrow()[cur as usize];
            st.counter.set(st.counter.get().wrapping_add(1));
            inval_function(&msg)?;
        }

        if st.nummsgs.get() != MAXINVALMSGS as i32 {
            break;
        }
    }

    if st.catchup_pending.get() {
        st.catchup_pending.set(false);
        elog(DEBUG4, "sinval catchup complete, cleaning queue")?;
        SICleanupQueue(false, 0)?;
    }
    Ok(())
}

pub fn SICleanupQueue(caller_has_write_lock: bool, min_free: i32) -> PgResult<()> {
    let seg = current_seg();
    let my = MyProcNumber();
    let write_lock = main_lock(SINVAL_WRITE_LOCK);
    let read_lock = main_lock(SINVAL_READ_LOCK);

    if !caller_has_write_lock {
        LWLockAcquire(write_lock, LW_EXCLUSIVE, my)?;
    }
    LWLockAcquire(read_lock, LW_EXCLUSIVE, my)?;

    let h = seg.hdr();
    let mut min = h.maxMsgNum.load(Relaxed);
    let mut minsig = min - SIG_THRESHOLD;
    let lowbound = min - MAXNUMMESSAGES + min_free;
    let mut need_sig: Option<usize> = None;

    let num_procs = h.numProcs.load(Relaxed) as usize;
    for i in 0..num_procs {
        let procno = seg.pgprocnos()[i].load(Relaxed) as usize;
        let state = &seg.proc_states()[procno];
        let n = state.nextMsgNum.load(Relaxed);

        debug_assert!(state.procPid.load(Relaxed) != 0);
        if state.resetState.load(Relaxed) || state.sendOnly.load(Relaxed) {
            continue;
        }
        if n < lowbound {
            state.resetState.store(true, Relaxed);
            continue;
        }
        if n < min {
            min = n;
        }
        if n < minsig && !state.signaled.load(Relaxed) {
            minsig = n;
            need_sig = Some(procno);
        }
    }
    h.minMsgNum.store(min, Relaxed);

    if min >= MSGNUMWRAPAROUND {
        h.minMsgNum.fetch_sub(MSGNUMWRAPAROUND, Relaxed);
        h.maxMsgNum.fetch_sub(MSGNUMWRAPAROUND, Relaxed);
        for i in 0..num_procs {
            let procno = seg.pgprocnos()[i].load(Relaxed) as usize;
            seg.proc_states()[procno]
                .nextMsgNum
                .fetch_sub(MSGNUMWRAPAROUND, Relaxed);
        }
    }

    let num_msgs = h.maxMsgNum.load(Relaxed) - h.minMsgNum.load(Relaxed);
    h.nextThreshold.store(
        if num_msgs < CLEANUP_MIN {
            CLEANUP_MIN
        } else {
            (num_msgs / CLEANUP_QUANTUM + 1) * CLEANUP_QUANTUM
        },
        Relaxed,
    );

    if let Some(procno) = need_sig {
        let state = &seg.proc_states()[procno];
        let his_pid = state.procPid.load(Relaxed);
        state.signaled.store(true, Relaxed);
        LWLockRelease(read_lock)?;
        LWLockRelease(write_lock)?;
        elog(
            DEBUG4,
            format!("sending sinval catchup signal to PID {his_pid}"),
        )?;
        procsignal::SendProcSignal(
            his_pid,
            ProcSignalReason::PROCSIG_CATCHUP_INTERRUPT,
            procno as ProcNumber,
        );
        if caller_has_write_lock {
            LWLockAcquire(write_lock, LW_EXCLUSIVE, my)?;
        }
    } else {
        LWLockRelease(read_lock)?;
        if !caller_has_write_lock {
            LWLockRelease(write_lock)?;
        }
    }
    Ok(())
}

// Signal-handler-reachable in C: allocation- and lock-free.
pub fn HandleCatchupInterrupt() {
    LOCAL.with(|st| st.catchup_pending.set(true));
    latch::SetLatch(MyLatch().expect("HandleCatchupInterrupt: MyLatch is not set"));
}

pub fn ProcessCatchupInterrupt() -> PgResult<()> {
    while LOCAL.with(|st| st.catchup_pending.get()) {
        if xact_seams::is_transaction_or_transaction_block::call() {
            elog(DEBUG4, "ProcessCatchupEvent inside transaction")?;
            inval_seams::accept_invalidation_messages::call()?;
        } else {
            elog(DEBUG4, "ProcessCatchupEvent outside transaction")?;
            xact_seams::start_transaction_command::call()?;
            xact_seams::commit_transaction_command::call()?;
        }
    }
    Ok(())
}

pub fn GetNextLocalTransactionId() -> LocalTransactionId {
    LOCAL.with(|st| loop {
        let result = st.next_lxid.get();
        st.next_lxid.set(result.wrapping_add(1));
        if result != InvalidLocalTransactionId {
            return result;
        }
    })
}

pub fn init_seams() {
    sinval_seams::send_shared_invalid_messages::set(SendSharedInvalidMessages);
    sinval_seams::receive_shared_invalid_messages::set(ReceiveSharedInvalidMessages);
    sinval_seams::handle_catchup_interrupt::set(HandleCatchupInterrupt);
}

#[cfg(test)]
mod tests;
