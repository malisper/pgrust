// barrier.c (storage/ipc): dynamic-party phase barrier. Thread-native: the
// spinlock+ConditionVariable pair is a std Mutex over the state plus Waiter
// parks (M0 lane C — the raw 10ms Condvar poll moved onto the structured
// wait primitive). Phase-advancers unpark every registered waiter promptly;
// the 10ms timed park is retained as the InterruptPending poll (C's
// ConditionVariableSleep checks interrupts per wakeup), so a dead peer
// surfaces as an error instead of a hang, and cancels keep their latency.
#![allow(non_snake_case)]

use std::sync::Mutex;

use ::types_error::PgResult;

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

    fn lock(&self) -> std::sync::MutexGuard<'_, BarrierInner> {
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
        let handle = waiter::current_handle().as_u64();
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
            // 10ms timed park = the InterruptPending poll cadence the old
            // Condvar wait had; a phase advance unparks promptly.
            let _ = waiter::park_timeout(core::time::Duration::from_millis(10));
            if init_small::globals::InterruptPending() {
                postgres_seams::check_for_interrupts::call()?;
            }
            b = self.lock();
        }
        if let Some(pos) = b.waiters.iter().position(|w| *w == handle) {
            b.waiters.swap_remove(pos);
        }
        Ok(elected)
    }

    fn unpark_all(handles: Vec<u64>) {
        for h in handles {
            waiter::unpark_word(h);
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
    /// reused shared memory, e.g. ExecHashJoinReInitializeDSM). The caller
    /// must guarantee no participant is attached.
    pub fn reset(&self) {
        let mut b = self.lock();
        debug_assert!(b.participants == 0 && b.arrived == 0);
        debug_assert!(b.waiters.is_empty());
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

#[cfg(test)]
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
