#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::Cell;
use std::sync::atomic::{
    fence, AtomicUsize,
    Ordering::{Acquire, Relaxed, Release, SeqCst},
};
use std::sync::Mutex;

use init_small::globals::{IsUnderPostmaster, MyLatch, MyProcPid};
use types_core::{pgsocket, PGINVALID_SOCKET};
use types_error::{PgError, PgResult, PANIC};
use types_storage::latch::{Latch, LatchHandle, LatchKind};
use types_storage::waiteventset::{
    WaitEventSetHandle, WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_SOCKET_MASK,
    WL_TIMEOUT,
};
use waiteventset_seams as wes;

const LatchWaitSetLatchPos: i32 = 0;
const LatchWaitSetPostmasterDeathPos: i32 = 1;

// C callers declare `Latch` storage themselves (miscinit.c's LocalLatchData,
// aux-process statics); here that storage is this fixed slab, handed out by
// allocate_local_latch. Const-init statics keep handle resolution a plain
// index — SetLatch stays lock- and allocation-free (signal-handler-reachable).
const LOCAL_LATCH_CAP: usize = 4096;
static LOCAL_LATCHES: [Latch; LOCAL_LATCH_CAP] = [const { Latch::new(false, 0) }; LOCAL_LATCH_CAP];
static LOCAL_LATCH_NEXT: AtomicUsize = AtomicUsize::new(0);
// C's LocalLatchData is per-process storage reclaimed by process death; the
// thread model reclaims explicitly: backend teardown pushes its slot here and
// later backends pop before bumping LOCAL_LATCH_NEXT. Startup/teardown paths
// only — set_latch never touches this lock.
static LOCAL_LATCH_FREE: Mutex<Vec<usize>> = Mutex::new(Vec::new());

pub fn allocate_local_latch() -> LatchHandle {
    let recycled = LOCAL_LATCH_FREE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .pop();
    if let Some(id) = recycled {
        return LatchHandle::new(id);
    }
    let id = LOCAL_LATCH_NEXT.fetch_add(1, Relaxed);
    assert!(id < LOCAL_LATCH_CAP, "local latch slab exhausted");
    LatchHandle::new(id + 1)
}

pub fn free_local_latch(latch: LatchHandle) {
    let LatchKind::Local(id) = latch.kind() else {
        panic!("free_local_latch: not a local latch: {latch:?}")
    };
    debug_assert!(id >= 1 && id <= LOCAL_LATCH_NEXT.load(Relaxed));
    let mut free = LOCAL_LATCH_FREE.lock().unwrap_or_else(|e| e.into_inner());
    debug_assert!(
        !free.contains(&id),
        "free_local_latch: double free of slot {id}"
    );
    free.push(id);
}

// Slots ever bump-allocated: recycling bounds this by peak concurrent
// backends, not connections served over the postmaster lifetime.
pub fn local_latch_high_water() -> usize {
    LOCAL_LATCH_NEXT.load(Relaxed)
}

pub fn latch_ref(latch: LatchHandle) -> &'static Latch {
    match latch.kind() {
        LatchKind::Local(id) => {
            debug_assert!(id >= 1 && id <= LOCAL_LATCH_NEXT.load(Relaxed));
            &LOCAL_LATCHES[id - 1]
        }
        LatchKind::Proc(procno) => lmgr_proc_seams::proc_latch::call(procno),
        LatchKind::RecoveryWakeup => {
            panic!("latch_ref: recoveryWakeupLatch owner (xlogrecovery) is not ported")
        }
    }
}

thread_local! {
    // static WaitEventSet *LatchWaitSet — never freed, like the C static.
    static LATCH_WAIT_SET: Cell<Option<WaitEventSetHandle>> = const { Cell::new(None) };
}

pub fn InitializeLatchWaitSet() -> PgResult<()> {
    debug_assert!(LATCH_WAIT_SET.get().is_none());

    let set = wes::create_wait_event_set::call(2)?;
    let latch_pos =
        wes::add_wait_event_to_set::call(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch(), None)?;
    debug_assert_eq!(latch_pos, LatchWaitSetLatchPos);

    if IsUnderPostmaster() {
        let pos =
            wes::add_wait_event_to_set::call(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, None, None)?;
        debug_assert_eq!(pos, LatchWaitSetPostmasterDeathPos);
    }

    LATCH_WAIT_SET.set(Some(set));
    Ok(())
}

// Init/Own/Disown are plain accesses in C, ordered only by the caller's
// interlock (fork ordering, explicit locks); Release stores / Acquire loads
// keep that publication sound across threads (notes/latch-atomics.md).
pub fn InitLatch(latch: LatchHandle) {
    let l = latch_ref(latch);
    l.is_set.store(0, Relaxed);
    l.maybe_sleeping.store(0, Relaxed);
    l.owner_pid.store(MyProcPid(), Relaxed);
    l.is_shared.store(false, Release);
}

pub fn InitSharedLatch(latch: LatchHandle) {
    let l = latch_ref(latch);
    l.is_set.store(0, Relaxed);
    l.maybe_sleeping.store(0, Relaxed);
    l.owner_pid.store(0, Relaxed);
    l.is_shared.store(true, Release);
}

pub fn OwnLatch(latch: LatchHandle) -> PgResult<()> {
    let l = latch_ref(latch);
    debug_assert!(l.is_shared.load(Acquire));

    let owner_pid = l.owner_pid.load(Acquire);
    if owner_pid != 0 {
        return Err(latch_already_owned(owner_pid));
    }

    l.owner_pid.store(MyProcPid(), Release);
    Ok(())
}

#[cold]
#[inline(never)]
fn latch_already_owned(owner_pid: i32) -> Box<PgError> {
    Box::new(PgError::new(
        PANIC,
        format!("latch already owned by PID {owner_pid}"),
    ))
}

pub fn DisownLatch(latch: LatchHandle) {
    let l = latch_ref(latch);
    debug_assert!(l.is_shared.load(Acquire));
    debug_assert_eq!(l.owner_pid.load(Acquire), MyProcPid());

    l.owner_pid.store(0, Release);
}

pub fn WaitLatch(
    latch: Option<LatchHandle>,
    wakeEvents: u32,
    timeout: i64,
    wait_event_info: u32,
) -> PgResult<u32> {
    debug_assert!(
        !IsUnderPostmaster() || wakeEvents & (WL_EXIT_ON_PM_DEATH | WL_POSTMASTER_DEATH) != 0
    );

    let latch = if wakeEvents & WL_LATCH_SET != 0 {
        latch
    } else {
        None
    };
    let set = LATCH_WAIT_SET
        .get()
        .expect("LatchWaitSet is not initialized");
    wes::modify_wait_event::call(set, LatchWaitSetLatchPos, WL_LATCH_SET, latch)?;

    if IsUnderPostmaster() {
        wes::modify_wait_event::call(
            set,
            LatchWaitSetPostmasterDeathPos,
            wakeEvents & (WL_EXIT_ON_PM_DEATH | WL_POSTMASTER_DEATH),
            None,
        )?;
    }

    let timeout = if wakeEvents & WL_TIMEOUT != 0 {
        timeout
    } else {
        -1
    };
    let res = match wes::wait_event_set_wait_one::call(set, timeout, wait_event_info)? {
        None => Ok(WL_TIMEOUT),
        Some(event) => Ok(event.events),
    };
    drain_timeout_interrupt();
    drain_thread_signals()?;
    res
}

// The thread model's SIGALRM delivery point: C's handler interrupts the sleep
// itself; here the timeout timer thread posts + SetLatches, and the woken
// backend fires its timeout handlers before returning (notes/timeout-threads.md).
fn drain_timeout_interrupt() {
    if timeout_seams::process_timeout_interrupt::is_installed() {
        timeout_seams::process_timeout_interrupt::call();
    }
}

// The thread model's kill(2) delivery point: senders pend a signo on this
// thread's ProcSignal slot + SetLatch; the woken waiter runs its registered
// dispositions here, as C's unblocked handler would during the sleep.
fn drain_thread_signals() -> PgResult<()> {
    if procsignal_seams::drain_thread_signals::is_installed() {
        procsignal_seams::drain_thread_signals::call()?;
    }
    Ok(())
}

pub fn WaitLatchOrSocket(
    latch: Option<LatchHandle>,
    wakeEvents: u32,
    sock: pgsocket,
    timeout: i64,
    wait_event_info: u32,
) -> PgResult<u32> {
    let set = wes::create_wait_event_set_current_owner::call(3)?;
    // C frees via CurrentResourceOwner on the ereport path and explicitly on
    // success; freeing on both paths here is the same resource outcome.
    let result = wait_latch_or_socket(set, latch, wakeEvents, sock, timeout, wait_event_info);
    wes::free_wait_event_set::call(set);
    result
}

fn wait_latch_or_socket(
    set: WaitEventSetHandle,
    latch: Option<LatchHandle>,
    wakeEvents: u32,
    sock: pgsocket,
    mut timeout: i64,
    wait_event_info: u32,
) -> PgResult<u32> {
    if wakeEvents & WL_TIMEOUT != 0 {
        debug_assert!(timeout >= 0);
    } else {
        timeout = -1;
    }

    if wakeEvents & WL_LATCH_SET != 0 {
        wes::add_wait_event_to_set::call(set, WL_LATCH_SET, PGINVALID_SOCKET, latch, None)?;
    }

    debug_assert!(
        !IsUnderPostmaster() || wakeEvents & (WL_EXIT_ON_PM_DEATH | WL_POSTMASTER_DEATH) != 0
    );

    if wakeEvents & WL_POSTMASTER_DEATH != 0 && IsUnderPostmaster() {
        wes::add_wait_event_to_set::call(set, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, None, None)?;
    }

    if wakeEvents & WL_EXIT_ON_PM_DEATH != 0 && IsUnderPostmaster() {
        wes::add_wait_event_to_set::call(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, None, None)?;
    }

    if wakeEvents & WL_SOCKET_MASK != 0 {
        wes::add_wait_event_to_set::call(set, wakeEvents & WL_SOCKET_MASK, sock, None, None)?;
    }

    let mut ret = 0;
    match wes::wait_event_set_wait_one::call(set, timeout, wait_event_info)? {
        None => ret |= WL_TIMEOUT,
        Some(event) => {
            ret |= event.events & (WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_SOCKET_MASK);
        }
    }
    drain_timeout_interrupt();
    drain_thread_signals()?;
    Ok(ret)
}

pub fn SetLatch(latch: LatchHandle) {
    set_latch(latch_ref(latch));
}

// Signal-handler-reachable: no allocation, no locks, no errors.
pub fn set_latch(latch: &Latch) {
    // pg_memory_barrier(): flag stores by this backend must be globally
    // visible before is_set is checked/set. Full fence, matching C — the
    // store->load edge is beyond Release/Acquire (notes/latch-atomics.md).
    fence(SeqCst);

    if latch.is_set.load(Relaxed) != 0 {
        return;
    }
    latch.is_set.store(1, Relaxed);

    fence(SeqCst);
    if latch.maybe_sleeping.load(Relaxed) == 0 {
        return;
    }

    // owner_pid read exactly once: a concurrent own/disown may lose this
    // wake, which C tolerates — waiters recheck at the bottom of their loops.
    let owner_pid = latch.owner_pid.load(Relaxed);
    if owner_pid == 0 {
    } else if owner_pid == MyProcPid() {
        wes::wakeup_my_proc::call();
    } else {
        wes::wakeup_other_proc::call(owner_pid);
    }
}

pub fn ResetLatch(latch: LatchHandle) {
    let l = latch_ref(latch);
    debug_assert_eq!(l.owner_pid.load(Acquire), MyProcPid());
    debug_assert_eq!(l.maybe_sleeping.load(Acquire), 0);

    l.is_set.store(0, Relaxed);

    // pg_memory_barrier(): the is_set clear must reach memory before we read
    // any flag variables, or a concurrent SetLatch could skip the wake.
    fence(SeqCst);
}

fn set_latch_my_latch() {
    SetLatch(MyLatch().expect("SetLatch(MyLatch): MyLatch is not set"));
}

pub fn init_seams() {
    latch_seams::set_latch_my_latch::set(set_latch_my_latch);
    latch_seams::set_latch::set(set_latch);
    latch_seams::own_latch::set(own_latch_ref);
    latch_seams::disown_latch::set(disown_latch_ref);
    latch_seams::reset_latch_my_latch::set(|| {
        ResetLatch(MyLatch().expect("ResetLatch(MyLatch): MyLatch is not set"));
    });
    latch_seams::wait_latch_my_latch::set(|wake_events, timeout, wait_event_info| {
        WaitLatch(MyLatch(), wake_events, timeout, wait_event_info)
            .unwrap_or_else(|e| panic!("WaitLatch(MyLatch): {e:?}"))
    });
}

// OwnLatch/DisownLatch over the caller's &Latch (lmgr_proc holds PGPROC refs,
// not handles).
fn own_latch_ref(l: &Latch) {
    debug_assert!(l.is_shared.load(Acquire));
    let owner_pid = l.owner_pid.load(Acquire);
    if owner_pid != 0 {
        panic!("latch already owned by PID {owner_pid}");
    }
    l.owner_pid.store(MyProcPid(), Release);
}

fn disown_latch_ref(l: &Latch) {
    debug_assert!(l.is_shared.load(Acquire));
    debug_assert_eq!(l.owner_pid.load(Acquire), MyProcPid());
    l.owner_pid.store(0, Release);
}

#[cfg(test)]
mod tests;
