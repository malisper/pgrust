#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::Cell;
use std::sync::atomic::{
    AtomicPtr, AtomicUsize,
    Ordering::{Acquire, Relaxed, Release},
};
// DST P2 (contract §1.3): CV timed-sleep deadline math rides pg_clock (the
// one monotonic authority); the wait itself stays on the waiter park
// primitive via WaitLatch.
use pg_clock::MonoStamp;

use init_small::globals::{MyLatch, MyProcNumber};
use latch::{set_latch, ResetLatch, WaitLatch};
use lmgr_proc::GetPGProcByNumber;
use types_core::{ProcNumber, INVALID_PROC_NUMBER};
use types_error::PgResult;
use types_storage::storage::{proclist_head, proclist_node, Spinlock, SyncCell};
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

pub struct ConditionVariable {
    mutex: Spinlock,
    // [CV] wakeup list, linked through PGPROC.cvWaitLink.
    wakeup: SyncCell<proclist_head>,
}

// SAFETY: mutex serializes wakeup per the [CV] domain.
unsafe impl Sync for ConditionVariable {}

impl ConditionVariable {
    pub const fn new() -> Self {
        Self {
            mutex: Spinlock::new(),
            wakeup: SyncCell::new(proclist_head {
                head: INVALID_PROC_NUMBER,
                tail: INVALID_PROC_NUMBER,
            }),
        }
    }
}

impl Default for ConditionVariable {
    fn default() -> Self {
        Self::new()
    }
}

thread_local! {
    static CV_SLEEP_TARGET: Cell<Option<&'static ConditionVariable>> = const { Cell::new(None) };
}

fn spin_acquire(lock: &Spinlock) {
    if lock.tas() != 0 {
        let mut delay =
            s_lock_seams::SpinDelayStatus::new(file!(), line!() as i32, "ConditionVariable");
        while lock.tas_spin() != 0 {
            s_lock_seams::perform_spin_delay::call(&mut delay);
        }
        s_lock_seams::finish_spin_delay::call(&delay);
    }
}

fn cv_wait_link(procno: ProcNumber) -> proclist_node {
    // SAFETY: [CV] serialized by the condition variable's spinlock
    unsafe { GetPGProcByNumber(procno).cvWaitLink.get() }
}

fn set_cv_wait_link(procno: ProcNumber, node: proclist_node) {
    // SAFETY: [CV] serialized by the condition variable's spinlock
    unsafe { GetPGProcByNumber(procno).cvWaitLink.set(node); }
}

fn proclist_push_tail(list: &mut proclist_head, procno: ProcNumber) {
    let mut node = cv_wait_link(procno);
    debug_assert!(node.next == 0 && node.prev == 0);

    if list.tail == INVALID_PROC_NUMBER {
        debug_assert!(list.head == INVALID_PROC_NUMBER);
        node.next = INVALID_PROC_NUMBER;
        node.prev = INVALID_PROC_NUMBER;
        list.head = procno;
    } else {
        node.prev = list.tail;
        let mut tail_node = cv_wait_link(node.prev);
        tail_node.next = procno;
        set_cv_wait_link(node.prev, tail_node);
        node.next = INVALID_PROC_NUMBER;
    }
    list.tail = procno;
    set_cv_wait_link(procno, node);
}

fn proclist_delete(list: &mut proclist_head, procno: ProcNumber) {
    let node = cv_wait_link(procno);
    debug_assert!(node.next != 0 || node.prev != 0);

    if node.prev == INVALID_PROC_NUMBER {
        debug_assert!(list.head == procno);
        list.head = node.next;
    } else {
        let mut prev_node = cv_wait_link(node.prev);
        prev_node.next = node.next;
        set_cv_wait_link(node.prev, prev_node);
    }
    if node.next == INVALID_PROC_NUMBER {
        debug_assert!(list.tail == procno);
        list.tail = node.prev;
    } else {
        let mut next_node = cv_wait_link(node.next);
        next_node.prev = node.prev;
        set_cv_wait_link(node.next, next_node);
    }
    set_cv_wait_link(procno, proclist_node { next: 0, prev: 0 });
}

fn proclist_contains(list: &proclist_head, procno: ProcNumber) -> bool {
    let node = cv_wait_link(procno);
    if node.prev == 0 && node.next == 0 {
        return false;
    }
    debug_assert!(node.prev != INVALID_PROC_NUMBER || list.head == procno);
    debug_assert!(node.next != INVALID_PROC_NUMBER || list.tail == procno);
    true
}

fn proclist_pop_head(list: &mut proclist_head) -> ProcNumber {
    debug_assert!(list.head != INVALID_PROC_NUMBER);
    let procno = list.head;
    proclist_delete(list, procno);
    procno
}

fn wakeup_mut(cv: &ConditionVariable) -> &mut proclist_head {
    // SAFETY: caller holds cv.mutex; the borrow ends before unlock.
    unsafe { &mut *cv.wakeup.ptr() }
}

/// PGRUST_MQ_RECHECK_MS (same knob as shm_mq::stall::recheck_ms; read here
/// directly to keep this crate below shm_mq in the layering). Default
/// 1000 ms; <= 0 restores the plain infinite sleep.
fn recheck_ms() -> i64 {
    static RECHECK: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *RECHECK.get_or_init(|| {
        std::env::var("PGRUST_MQ_RECHECK_MS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(1_000)
    })
}

pub fn ConditionVariablePrepareToSleep(cv: &'static ConditionVariable) {
    let pgprocno = MyProcNumber();
    debug_assert!(pgprocno != INVALID_PROC_NUMBER);

    if CV_SLEEP_TARGET.with(|t| t.get()).is_some() {
        ConditionVariableCancelSleep();
    }

    CV_SLEEP_TARGET.with(|t| t.set(Some(cv)));

    spin_acquire(&cv.mutex);
    proclist_push_tail(wakeup_mut(cv), pgprocno);
    cv.mutex.unlock();
}

pub fn ConditionVariableSleep(
    cv: &'static ConditionVariable,
    wait_event_info: u32,
) -> PgResult<()> {
    ConditionVariableTimedSleep(cv, -1, wait_event_info).map(|_| ())
}

pub fn ConditionVariableTimedSleep(
    cv: &'static ConditionVariable,
    timeout: i64,
    wait_event_info: u32,
) -> PgResult<bool> {
    let target_is_cv = CV_SLEEP_TARGET.with(|t| t.get()).is_some_and(|t| core::ptr::eq(t, cv));
    if !target_is_cv {
        ConditionVariablePrepareToSleep(cv);
        return Ok(false);
    }

    let start_time = MonoStamp::now();
    let mut cur_timeout: i64 = -1;
    let wait_events = if timeout >= 0 {
        debug_assert!(timeout <= i32::MAX as i64);
        cur_timeout = timeout;
        WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH
    } else if recheck_ms() > 0 {
        // Recheck cadence (shm_mq stall.rs rationale): a CV signal removes
        // this proc from the wakeup list and SetLatches it, but a lost
        // cross-thread wake byte leaves the sleeper in epoll forever (the
        // production BufferIo hang shape under wake loss). The loop below is
        // a legal recheck: a timeout wake re-tests list membership, so a
        // dropped signal costs at most one recheck period. The caller-facing
        // contract is unchanged (we return only when signaled/cancelled or
        // the caller's own timeout expires).
        cur_timeout = recheck_ms();
        WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH
    } else {
        WL_LATCH_SET | WL_EXIT_ON_PM_DEATH
    };

    let my_latch = MyLatch().expect("MyLatch is not set");
    loop {
        let mut done = false;

        WaitLatch(Some(my_latch), wait_events, cur_timeout, wait_event_info)?;
        ResetLatch(my_latch);

        let my_procno = MyProcNumber();
        spin_acquire(&cv.mutex);
        {
            let wakeup = wakeup_mut(cv);
            if !proclist_contains(wakeup, my_procno) {
                done = true;
                proclist_push_tail(wakeup, my_procno);
            }
        }
        cv.mutex.unlock();

        postgres_seams::check_for_interrupts::call()?;
        if !CV_SLEEP_TARGET.with(|t| t.get()).is_some_and(|t| core::ptr::eq(t, cv)) {
            done = true;
        }

        if done {
            return Ok(false);
        }

        if timeout >= 0 {
            cur_timeout = timeout - start_time.elapsed_ms();
            if cur_timeout <= 0 {
                return Ok(true);
            }
        }
    }
}

pub fn ConditionVariableCancelSleep() -> bool {
    let Some(cv) = CV_SLEEP_TARGET.with(|t| t.get()) else {
        return false;
    };
    let mut signaled = false;

    spin_acquire(&cv.mutex);
    {
        let wakeup = wakeup_mut(cv);
        let my_procno = MyProcNumber();
        if proclist_contains(wakeup, my_procno) {
            proclist_delete(wakeup, my_procno);
        } else {
            signaled = true;
        }
    }
    cv.mutex.unlock();

    CV_SLEEP_TARGET.with(|t| t.set(None));
    signaled
}

pub fn ConditionVariableSignal(cv: &ConditionVariable) {
    let mut proc = INVALID_PROC_NUMBER;

    spin_acquire(&cv.mutex);
    {
        let wakeup = wakeup_mut(cv);
        if wakeup.head != INVALID_PROC_NUMBER {
            proc = proclist_pop_head(wakeup);
        }
    }
    cv.mutex.unlock();

    if proc != INVALID_PROC_NUMBER {
        set_latch(&GetPGProcByNumber(proc).procLatch);
    }
}

pub fn ConditionVariableBroadcast(cv: &'static ConditionVariable) {
    let pgprocno = MyProcNumber();
    debug_assert!(pgprocno != INVALID_PROC_NUMBER);
    let mut proc = INVALID_PROC_NUMBER;
    let mut have_sentinel = false;

    // Our cvWaitLink doubles as the sentinel; it must be free first.
    if CV_SLEEP_TARGET.with(|t| t.get()).is_some() {
        ConditionVariableCancelSleep();
    }

    spin_acquire(&cv.mutex);
    {
        let wakeup = wakeup_mut(cv);
        debug_assert!(!proclist_contains(wakeup, pgprocno));
        if wakeup.head != INVALID_PROC_NUMBER {
            proc = proclist_pop_head(wakeup);
            if wakeup.head != INVALID_PROC_NUMBER {
                proclist_push_tail(wakeup, pgprocno);
                have_sentinel = true;
            }
        }
    }
    cv.mutex.unlock();

    if proc != INVALID_PROC_NUMBER {
        set_latch(&GetPGProcByNumber(proc).procLatch);
    }

    while have_sentinel {
        proc = INVALID_PROC_NUMBER;
        spin_acquire(&cv.mutex);
        {
            let wakeup = wakeup_mut(cv);
            if wakeup.head != INVALID_PROC_NUMBER {
                proc = proclist_pop_head(wakeup);
            }
            have_sentinel = proclist_contains(wakeup, pgprocno);
        }
        cv.mutex.unlock();

        if proc != INVALID_PROC_NUMBER && proc != pgprocno {
            set_latch(&GetPGProcByNumber(proc).procLatch);
        }
    }
}

// procsignal's pss_barrierCV per-slot storage (slot index == ProcNumber):
// C embeds one ConditionVariable in every ProcSignalSlot and
// ProcSignalShmemInit runs ConditionVariableInit over NumProcSignalSlots =
// MaxBackends + NUM_AUXILIARY_PROCS of them (procsignal.c:75-160). The
// storage is sized from that count at shmem init (a leaked boot allocation,
// procsignal's own psh_slot shape) — never a const cap: max_connections may
// reach MAX_BACKENDS.
// Published once: len stored first, ptr Release-stored last, under the init
// spinlock; readers Acquire the ptr (no raw std sync type — the ratchet
// ledger's once/rawsync budgets for this file stay where they are).
static BARRIER_CVS: AtomicPtr<ConditionVariable> = AtomicPtr::new(core::ptr::null_mut());
static BARRIER_CV_LEN: AtomicUsize = AtomicUsize::new(0);
static BARRIER_CV_INIT: Spinlock = Spinlock::new();

/// ProcSignalShmemInit's per-slot `ConditionVariableInit(&slot->pss_barrierCV)`
/// (procsignal.c:156): allocate one CV per ProcSignal slot. A repeat call
/// keeps the first slab (psh_slot's init-once shape).
pub fn ProcSignalBarrierCvsInit(num_slots: i32) {
    assert!(num_slots > 0, "ProcSignalBarrierCvsInit: NumProcSignalSlots not initialized");
    spin_acquire(&BARRIER_CV_INIT);
    if BARRIER_CVS.load(Acquire).is_null() {
        let cvs: &'static mut [ConditionVariable] = (0..num_slots)
            .map(|_| ConditionVariable::new())
            .collect::<Vec<_>>()
            .leak();
        BARRIER_CV_LEN.store(cvs.len(), Relaxed);
        BARRIER_CVS.store(cvs.as_mut_ptr(), Release);
    }
    BARRIER_CV_INIT.unlock();
}

fn barrier_cvs() -> &'static [ConditionVariable] {
    let ptr = BARRIER_CVS.load(Acquire);
    if ptr.is_null() {
        panic!("ProcSignal barrier CVs not initialized (ProcSignalShmemInit not called)");
    }
    let len = BARRIER_CV_LEN.load(Relaxed);
    // SAFETY: a leaked `len`-element slab, never freed or moved; the Acquire
    // load pairs with the Release store that follows the len store and the
    // writes that initialized every element.
    unsafe { core::slice::from_raw_parts(ptr, len) }
}

static CHECKPOINTER_CVS: [ConditionVariable; 2] = [const { ConditionVariable::new() }; 2];

fn barrier_cv(slot: i32) -> &'static ConditionVariable {
    let cvs = barrier_cvs();
    assert!(
        (0..cvs.len() as i32).contains(&slot),
        "ProcSignal barrier CV slot {slot} out of range (NumProcSignalSlots = {})",
        cvs.len()
    );
    &cvs[slot as usize]
}

fn checkpointer_cv(cv: condition_variable_seams::CheckpointerCv) -> &'static ConditionVariable {
    match cv {
        condition_variable_seams::CheckpointerCv::Start => &CHECKPOINTER_CVS[0],
        condition_variable_seams::CheckpointerCv::Done => &CHECKPOINTER_CVS[1],
    }
}

// Crash-cycle re-init (ConditionVariableInit image): a backend killed while
// parked leaves its procno in wakeup, and ProcGlobalReset zeroes the
// cvWaitLink side; both must clear together. Postmaster only, children dead.
pub fn cv_reset_after_crash(cv: &ConditionVariable) {
    cv.mutex.unlock();
    // SAFETY: [CV] postmaster-only crash re-init; children dead
    unsafe {
        cv.wakeup.set(proclist_head {
            head: INVALID_PROC_NUMBER,
            tail: INVALID_PROC_NUMBER,
        });
    }
}

pub fn ProcSignalBarrierCvsResetAfterCrash() {
    for cv in barrier_cvs() {
        cv_reset_after_crash(cv);
    }
}

pub fn CheckpointerCvsResetAfterCrash() {
    for cv in &CHECKPOINTER_CVS {
        cv_reset_after_crash(cv);
    }
}

pub fn init_seams() {
    condition_variable_seams::condition_variable_cancel_sleep::set(ConditionVariableCancelSleep);
    condition_variable_seams::proc_signal_barrier_cv_timed_sleep::set(|slot, timeout, info| {
        ConditionVariableTimedSleep(barrier_cv(slot), timeout, info)
    });
    condition_variable_seams::proc_signal_barrier_cv_broadcast::set(|slot| {
        ConditionVariableBroadcast(barrier_cv(slot))
    });
    condition_variable_seams::proc_signal_barrier_cvs_init::set(ProcSignalBarrierCvsInit);
    condition_variable_seams::checkpointer_cv_broadcast::set(|cv| {
        ConditionVariableBroadcast(checkpointer_cv(cv))
    });
    condition_variable_seams::checkpointer_cv_prepare_to_sleep::set(|cv| {
        ConditionVariablePrepareToSleep(checkpointer_cv(cv))
    });
    condition_variable_seams::checkpointer_cv_sleep::set(|cv, info| {
        ConditionVariableSleep(checkpointer_cv(cv), info)
    });
    condition_variable_seams::proc_signal_barrier_cvs_reset_after_crash::set(
        ProcSignalBarrierCvsResetAfterCrash,
    );
    condition_variable_seams::checkpointer_cvs_reset_after_crash::set(
        CheckpointerCvsResetAfterCrash,
    );
}

#[cfg(test)]
mod tests;
