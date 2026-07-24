// barrier.c (storage/ipc): dynamic-party phase barrier. Thread-native: the
// spinlock+ConditionVariable pair is a std Mutex over the state plus Waiter
// parks (M0 lane C — the raw 10ms Condvar poll moved onto the structured
// wait primitive). Phase-advancers unpark every registered waiter promptly;
// the 10ms timed park is retained as the InterruptPending poll (C's
// ConditionVariableSleep checks interrupts per wakeup), so a dead peer
// surfaces as an error instead of a hang, and cancels keep their latency.
#![allow(non_snake_case)]

use ::types_error::PgResult;

// PERMIT-S2 absorb (permit-s1 compose §1.3 recipe): the loom-breadth
// branch's private cfg(loom) shim became an unconditional pgsync import —
// pgsync is THE single world-cfg point (native arm = the identical std
// re-export, zero cost; `--cfg loom` = loom's checked Mutex), so the loom
// models in tests/loom.rs drive the real pgsync types.
use pgsync::{Mutex, MutexGuard};

// Park route: how a non-releasing arrival blocks and how a phase advance
// wakes it. Production rides the global waiter slab (10ms timed park = the
// InterruptPending poll cadence C's ConditionVariableSleep had); the loom
// build routes through model-owned waiter Slots (the waiter crate's slot
// core IS loom-modeled), with untimed parks — a lost phase-advance wake is
// then a deadlock loom's detector reports instead of something the 10ms
// cadence would paper over. The interrupt drain is production-only (seams
// are not installed in models; a model never has InterruptPending).
#[cfg(not(loom))]
mod route {
    use ::types_error::PgResult;

    #[inline]
    pub(crate) fn current_word() -> u64 {
        waiter::current_handle().as_u64()
    }

    #[inline]
    pub(crate) fn park_wait() -> PgResult<()> {
        // 10ms timed park = the InterruptPending poll cadence the old
        // Condvar wait had; a phase advance unparks promptly.
        let _ = waiter::park_timeout(core::time::Duration::from_millis(10));
        if init_small::globals::InterruptPending() {
            postgres_seams::check_for_interrupts::call()?;
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn unpark_word(word: u64) {
        waiter::unpark_word(word);
    }
}

#[cfg(loom)]
mod route {
    use std::cell::Cell;
    use std::sync::Arc;

    use ::types_error::PgResult;
    use waiter::clock::WaiterClock;
    use waiter::{Slot, SlotInner};

    /// Untimed model clock (the waiter loom models' LoomClock shape): parks
    /// block on the slot condvar until a real unpark — no cadence, no time.
    struct ModelClock;

    impl WaiterClock for ModelClock {
        fn now_ms(&self) -> i64 {
            0
        }
        fn wait<'a>(
            &self,
            slot: &'a Slot,
            guard: pgsync::MutexGuard<'a, SlotInner>,
            _timeout_ms: Option<i64>,
        ) -> (pgsync::MutexGuard<'a, SlotInner>, bool) {
            (slot.wait_for_model(guard), false)
        }
    }

    static CLOCK: ModelClock = ModelClock;

    // Per-execution slot registry (loom::lazy_static resets between model
    // iterations, loom::thread_local between model threads). Word layout
    // mirrors WakerHandle: (index+1) << 32 | token, never zero.
    loom::lazy_static! {
        static ref SLOTS: loom::sync::Mutex<Vec<Arc<Slot>>> =
            loom::sync::Mutex::new(Vec::new());
    }
    loom::thread_local! {
        static MY_WORD: Cell<u64> = Cell::new(0);
    }

    pub(crate) fn current_word() -> u64 {
        MY_WORD.with(|w| {
            if w.get() == 0 {
                let slot = Arc::new(Slot::new_for_model());
                let token = slot.issue_token();
                let mut v = SLOTS.lock().unwrap();
                v.push(slot);
                w.set(((v.len() as u64) << 32) | token as u64);
            }
            w.get()
        })
    }

    pub(crate) fn park_wait() -> PgResult<()> {
        let word = current_word();
        let slot = {
            let v = SLOTS.lock().unwrap();
            Arc::clone(&v[((word >> 32) - 1) as usize])
        };
        // Notified is the only outcome (untimed, no cadence); the caller's
        // loop re-tests the phase either way.
        let _ = slot.park_core(None, None, &CLOCK);
        Ok(())
    }

    pub(crate) fn unpark_word(word: u64) {
        if word == 0 {
            return;
        }
        let slot = {
            let v = SLOTS.lock().unwrap();
            v.get(((word >> 32) - 1) as usize).map(Arc::clone)
        };
        if let Some(s) = slot {
            let _ = s.unpark_token(word as u32);
        }
    }
}

pub fn init_seams() {}

struct BarrierInner {
    phase: i32,
    participants: i32,
    arrived: i32,
    elected: i32,
    static_party: bool,
    /// Packed waiter handles of parked arrive_and_wait callers.
    waiters: Vec<u64>,
}

pub struct Barrier {
    inner: Mutex<BarrierInner>,
}

impl Barrier {
    /// `BarrierInit`.
    pub fn new(participants: i32) -> Barrier {
        Barrier {
            inner: Mutex::new(BarrierInner {
                phase: 0,
                participants,
                arrived: 0,
                elected: 0,
                static_party: participants > 0,
                waiters: Vec::new(),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, BarrierInner> {
        // Poison-tolerant (loom's Mutex never poisons in models but keeps
        // the same Result API, so this line is cfg-free).
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `BarrierArriveAndWait`: true in the one elected participant.
    pub fn arrive_and_wait(&self) -> PgResult<bool> {
        let (start_phase, next_phase);
        {
            let mut b = self.lock();
            start_phase = b.phase;
            next_phase = start_phase + 1;
            b.arrived += 1;
            if b.arrived == b.participants {
                b.arrived = 0;
                b.phase = next_phase;
                b.elected = next_phase;
                let woken = std::mem::take(&mut b.waiters);
                drop(b);
                Self::unpark_all(woken);
                return Ok(true);
            }
        }
        let mut elected = false;
        let handle = route::current_word();
        let mut b = self.lock();
        loop {
            debug_assert!(b.phase == start_phase || b.phase == next_phase);
            if b.phase == next_phase {
                if b.elected != next_phase {
                    // The releasing arrival is normally elected; if the phase
                    // advanced because someone detached, elect a woken waiter.
                    b.elected = next_phase;
                    elected = true;
                }
                break;
            }
            if !b.waiters.contains(&handle) {
                b.waiters.push(handle);
            }
            drop(b);
            route::park_wait()?;
            b = self.lock();
        }
        if let Some(pos) = b.waiters.iter().position(|w| *w == handle) {
            b.waiters.swap_remove(pos);
        }
        Ok(elected)
    }

    fn unpark_all(handles: Vec<u64>) {
        for h in handles {
            route::unpark_word(h);
        }
    }

    /// `BarrierArriveAndDetach`: true if the caller was the last to detach.
    pub fn arrive_and_detach(&self) -> bool {
        self.detach_impl(true)
    }

    /// `BarrierArriveAndDetachExceptLast`: true if the caller was the last to
    /// arrive and is therefore still attached.
    pub fn arrive_and_detach_except_last(&self) -> bool {
        let mut b = self.lock();
        if b.participants > 1 {
            b.participants -= 1;
            return false;
        }
        debug_assert!(b.participants == 1);
        b.phase += 1;
        true
    }

    /// `BarrierAttach`: returns the current phase.
    pub fn attach(&self) -> i32 {
        let mut b = self.lock();
        debug_assert!(!b.static_party);
        b.participants += 1;
        b.phase
    }

    /// `BarrierDetach`: true if this participant was the last to detach.
    pub fn detach(&self) -> bool {
        self.detach_impl(false)
    }

    /// `BarrierPhase`. The caller must be attached (the phase cannot advance
    /// without it, so an unlocked read is C's contract; we take the lock —
    /// uncontended — rather than replicate the fence argument).
    pub fn phase(&self) -> i32 {
        self.lock().phase
    }

    /// `BarrierParticipants` (debugging only in C).
    pub fn participants(&self) -> i32 {
        self.lock().participants
    }

    /// Re-run `BarrierInit` in place (C reinitializes barriers embedded in
    /// reused shared memory, e.g. `ExecHashJoinReInitializeDSM`).
    ///
    /// **Clearing leftover state is the job, not a precondition.** C's
    /// `BarrierInit` asserts nothing here and unconditionally zeroes
    /// `participants`/`arrived`/`phase`/`elected` plus the condition variable —
    /// and C's own rescan path reaches it with a non-zero `participants`
    /// whenever `ExecHashJoinReInitializeDSM` takes the
    /// `hj_HashTable == NULL` branch and so skips the detach in
    /// `ExecHashTableDetach`. A pair of asserts demanding a zero count here was
    /// therefore a constraint this port invented, not one C imposes; measured
    /// on a shipped profile it fired on stale bookkeeping only — a leftover
    /// count at the terminal build phase with **no** parked waiter
    /// (GL-ASSERTMASK-1 §4). They are gone rather than re-graded.
    pub fn reset(&self) {
        let mut b = self.lock();
        b.phase = 0;
        b.participants = 0;
        b.arrived = 0;
        b.elected = 0;
        b.static_party = false;
        b.waiters.clear();
    }

    fn detach_impl(&self, arrive: bool) -> bool {
        let mut b = self.lock();
        debug_assert!(!b.static_party);
        debug_assert!(b.participants > 0);
        b.participants -= 1;
        let release = (arrive || b.participants > 0) && b.arrived == b.participants;
        let mut woken = Vec::new();
        if release {
            b.arrived = 0;
            b.phase += 1;
            woken = std::mem::take(&mut b.waiters);
        }
        let last = b.participants == 0;
        drop(b);
        Self::unpark_all(woken);
        last
    }
}

// Unit tests drive real std threads over the global waiter slab — production
// surface only (the loom models live in tests/loom.rs).
#[cfg(all(test, not(loom)))]
mod tests {
    use super::Barrier;

    #[test]
    fn static_party_phases() {
        let b = std::sync::Arc::new(Barrier::new(3));
        let mut handles = Vec::new();
        for _ in 0..3 {
            let b = std::sync::Arc::clone(&b);
            handles.push(std::thread::spawn(move || {
                let mut elected = 0;
                for _ in 0..5 {
                    if b.arrive_and_wait().unwrap() {
                        elected += 1;
                    }
                }
                elected
            }));
        }
        let total: i32 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        // Exactly one election per phase.
        assert_eq!(total, 5);
        assert_eq!(b.lock().phase, 5);
    }

    #[test]
    fn dynamic_attach_detach() {
        let b = Barrier::new(0);
        assert_eq!(b.attach(), 0);
        assert_eq!(b.attach(), 0);
        // Two attached; one arrives-and-detaches: the other hasn't arrived,
        // so the phase holds; the final detach leaves the phase alone too.
        assert!(!b.arrive_and_detach());
        assert_eq!(b.phase(), 0);
        assert!(b.detach());
        assert_eq!(b.lock().phase, 0);
        // A sole participant arriving-and-detaching advances the phase.
        b.attach();
        assert!(b.arrive_and_detach());
        assert_eq!(b.lock().phase, 1);
    }

    #[test]
    fn detach_except_last() {
        let b = Barrier::new(0);
        b.attach();
        b.attach();
        assert!(!b.arrive_and_detach_except_last());
        assert!(b.arrive_and_detach_except_last());
        assert_eq!(b.phase(), 1);
        assert_eq!(b.participants(), 1);
    }
}
