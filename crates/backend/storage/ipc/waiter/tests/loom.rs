//! Loom models for the Waiter protocol (M0 gate; parallelism-redesign §4:
//! "Loom models FIRST ... they are new work, not a harvest").
//!
//! Run: RUSTFLAGS="--cfg loom" cargo test -p waiter --test loom --release
//!
//! Models:
//!   1. park/unpark race incl. wake-before-park — an unpark issued at ANY
//!      point relative to the park is never lost (no deadlock in any
//!      interleaving; loom's deadlock detector is the oracle).
//!   2. handle reuse — a stale-token unpark is a strict no-op: it neither
//!      wakes the new incarnation nor corrupts a fresh unpark.
//!   3. poison — retire (owner death) racing an unpark never delivers into a
//!      freed slot and never panics.
//!   4. latch-over-Waiter Dekker equivalence — a mirror of latch.rs's
//!      set_latch / WaitLatch pair (is_set + maybe_sleeping + SeqCst fences +
//!      waker word) over the slot core: after set_latch returns, a waiter
//!      that entered the wait loop always observes is_set (no lost wake),
//!      with the recheck cadence DISABLED — proving the primitive alone,
//!      not the backstop.
#![cfg(loom)]

use loom::sync::atomic::{fence, AtomicI32, AtomicU64, Ordering};
use loom::sync::Arc;
use loom::thread;

use waiter::clock::WaiterClock;
use waiter::{ParkResult, Slot, SlotInner, Unparked};

/// Loom clock: no time (models use untimed parks; cadence disabled — the
/// models prove the protocol without the debug backstop).
struct LoomClock;

impl WaiterClock for LoomClock {
    fn now_ms(&self) -> i64 {
        0
    }
    fn wait<'a>(
        &self,
        slot: &'a Slot,
        guard: loom::sync::MutexGuard<'a, SlotInner>,
        _timeout_ms: Option<i64>,
    ) -> (loom::sync::MutexGuard<'a, SlotInner>, bool) {
        (slot.wait_for_model(guard), false)
    }
}

static CLOCK: LoomClock = LoomClock;

// Slots live inside one loom execution (loom objects must not leak across
// model iterations), shared across model threads via loom Arc.
fn fresh_slot() -> Arc<Slot> {
    Arc::new(Slot::new_for_model())
}

fn park_until_notified(slot: &Slot) {
    loop {
        match slot.park_core(None, None, &CLOCK) {
            ParkResult::Notified => return,
            ParkResult::Recheck => continue,
            ParkResult::TimedOut => unreachable!("untimed park cannot time out"),
        }
    }
}

#[test]
fn unpark_never_lost_incl_wake_before_park() {
    loom::model(|| {
        let slot = fresh_slot();
        let token = slot.issue_token();

        let waker = {
            let slot = Arc::clone(&slot);
            thread::spawn(move || {
                // Any interleaving: before, during, or after the park entry.
                assert_ne!(slot.unpark_token(token), Unparked::Stale);
            })
        };

        // If the unpark already landed, park consumes the latched notify;
        // if not, the unpark must wake us. Loom's deadlock detection fails
        // the model if any interleaving leaves this blocked.
        park_until_notified(&slot);

        waker.join().unwrap();
    });
}

#[test]
fn stale_handle_is_noop_and_fresh_unpark_still_delivers() {
    loom::model(|| {
        let slot = fresh_slot();
        let old_token = slot.issue_token();
        // Reuse boundary: the owner reissues; old handles go stale.
        let new_token = slot.reissue_token();
        assert_ne!(old_token, new_token);

        let stale = {
            let slot = Arc::clone(&slot);
            thread::spawn(move || {
                // Must be a strict no-op in every interleaving.
                assert_eq!(slot.unpark_token(old_token), Unparked::Stale);
            })
        };
        let fresh = {
            let slot = Arc::clone(&slot);
            thread::spawn(move || {
                assert_ne!(slot.unpark_token(new_token), Unparked::Stale);
            })
        };

        // The fresh unpark must still deliver despite the racing stale one.
        park_until_notified(&slot);

        stale.join().unwrap();
        fresh.join().unwrap();
    });
}

#[test]
fn poison_on_owner_death_races_unpark_safely() {
    loom::model(|| {
        let slot = fresh_slot();
        let token = slot.issue_token();

        let waker = {
            let slot = Arc::clone(&slot);
            thread::spawn(move || {
                // Racing an owner death: either it lands first (Pending into
                // an incarnation that is then retired — dropped with the
                // token bump) or it observes the poison (Stale). Never a
                // panic, never a delivery into Free.
                let r = slot.unpark_token(token);
                assert!(matches!(r, Unparked::Pending | Unparked::Stale));
            })
        };

        // Owner dies without parking.
        slot.retire_token();

        waker.join().unwrap();

        // The retired incarnation's handle is now always stale.
        assert_eq!(slot.unpark_token(token), Unparked::Stale);
    });
}

// ---------------------------------------------------------------------------
// Latch-over-Waiter Dekker equivalence.
//
// Mirror of the 4-atomic latch protocol as reimplemented over the Waiter
// (latch/src/lib.rs set_latch + WaitLatch): the model must show that for a
// concurrent set_latch / wait_latch pair, the waiter always terminates
// having observed is_set — the store->load SeqCst fence discipline plus the
// waker-word publication means a sleeping owner is always unparked.
// ---------------------------------------------------------------------------

struct ModelLatch {
    is_set: AtomicI32,
    maybe_sleeping: AtomicI32,
    waker: AtomicU64,
}

impl ModelLatch {
    fn new() -> Self {
        ModelLatch {
            is_set: AtomicI32::new(0),
            maybe_sleeping: AtomicI32::new(0),
            waker: AtomicU64::new(0),
        }
    }

    /// latch.rs set_latch over the Waiter: fence; is_set store; fence;
    /// maybe_sleeping check; waker-word unpark.
    fn set(&self, slot: &Slot) {
        fence(Ordering::SeqCst);
        if self.is_set.load(Ordering::Relaxed) != 0 {
            return;
        }
        // Dekker flag edges as RMWs (swap/fetch_add). The REAL code uses
        // C's pg_memory_barrier discipline: Relaxed/SC flag ops between
        // SeqCst fences, whose single total order forbids the miss-miss
        // case (both sides reading the pre-store value). Loom 0.7 does not
        // model that total order — its store-buffer approximation admits
        // the weak outcome even for plain SeqCst stores (see
        // loom_litmus.rs, which FAILS the SC store-buffer litmus). RMW
        // operations are never buffered by loom, so expressing the two flag
        // stores and the two recheck reads as RMWs carries exactly the
        // recency the fences guarantee on the real memory model, at
        // equal-or-stronger ordering. What the model then verifies for real
        // is everything AROUND those edges: the waker-word publication
        // ordering (loom DID find the real lost-wake when the waker was
        // read without an acquire edge — fixed by pairing the acquire read
        // with the release maybe_sleeping store), the slot state machine,
        // and the park/unpark protocol composition.
        self.is_set.swap(1, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        if self.maybe_sleeping.fetch_add(0, Ordering::SeqCst) == 0 {
            return;
        }
        let word = self.waker.load(Ordering::Acquire);
        if word != 0 {
            // In production this is waiter::unpark_word (handle-validated);
            // the model drives the slot core with the published token.
            let token = word as u32;
            slot.unpark_token(token);
        }
    }

    /// latch.rs WaitLatch (no timeout, cadence off): publish waker, arm
    /// maybe_sleeping, recheck is_set, park; repeat until is_set.
    fn wait(&self, slot: &Slot, token: u32) {
        loop {
            if self.is_set.load(Ordering::SeqCst) != 0 {
                return;
            }
            self.waker.store(token as u64 | (1 << 32), Ordering::Release);
            // RMW store: see set() — the release ordering also publishes
            // the waker word to the setter's acquire-strength RMW read.
            self.maybe_sleeping.swap(1, Ordering::SeqCst);
            // Dekker recheck: the setter's is_set store is fenced before its
            // maybe_sleeping load, so either we see is_set here or it sees
            // maybe_sleeping = 1 and unparks us. RMW read for the same
            // loom-expressibility reason as the setter side (see set()).
            if self.is_set.fetch_add(0, Ordering::SeqCst) != 0 {
                self.maybe_sleeping.store(0, Ordering::SeqCst);
                return;
            }
            let r = slot.park_core(None, None, &CLOCK);
            self.maybe_sleeping.store(0, Ordering::SeqCst);
            debug_assert!(matches!(r, ParkResult::Notified | ParkResult::Recheck));
        }
    }
}

#[test]
fn latch_dekker_over_waiter_no_lost_wake() {
    loom::model(|| {
        let slot = fresh_slot();
        let token = slot.issue_token();
        let latch = Arc::new(ModelLatch::new());

        let setter = {
            let latch = Arc::clone(&latch);
            let slot = Arc::clone(&slot);
            thread::spawn(move || {
                latch.set(&slot);
            })
        };

        // Must terminate in every interleaving (loom deadlock-detects the
        // lost-wake case) and terminate only with is_set observed.
        latch.wait(&slot, token);
        assert_eq!(latch.is_set.load(Ordering::SeqCst), 1);

        setter.join().unwrap();
    });
}

#[test]
fn latch_dekker_set_before_wait_short_circuits() {
    loom::model(|| {
        let slot = fresh_slot();
        let token = slot.issue_token();
        let latch = ModelLatch::new();
        latch.set(&slot);
        // No sleeper was armed: wait must return without parking forever.
        latch.wait(&slot, token);
        assert_eq!(latch.is_set.load(Ordering::SeqCst), 1);
    });
}

// ---------------------------------------------------------------------------
// IoToken (§2.9 addendum): multi-registrant unpark, register-after-complete
// race, completer-is-registrant. Handles in the model are (slot, token)
// pairs; completion drives the slot core exactly as production complete()
// drives the global table.
// ---------------------------------------------------------------------------

use waiter::io::{IoRegister, IoTokenCore};

type ModelHandle = (Arc<Slot>, u32);

fn complete_all(token: &IoTokenCore<ModelHandle>) -> usize {
    token.complete_with(|(slot, tok)| {
        slot.unpark_token(tok);
    })
}

#[test]
fn io_token_register_complete_race_never_hangs() {
    loom::model(|| {
        let slot = fresh_slot();
        let tok = slot.issue_token();
        let io = Arc::new(IoTokenCore::<ModelHandle>::new(1, 1));

        let completer = {
            let io = Arc::clone(&io);
            thread::spawn(move || {
                // ANY thread completes; racing the registration below.
                complete_all(&io);
            })
        };

        // Register-after-complete race: either we registered in time (the
        // completer unparks us) or the completed-fast-path tells us not to
        // park. Neither interleaving may hang (loom deadlock oracle).
        match io.register((Arc::clone(&slot), tok)) {
            IoRegister::AlreadyCompleted => {}
            IoRegister::Registered => {
                while !io.is_completed() {
                    match slot.park_core(None, None, &CLOCK) {
                        ParkResult::Notified | ParkResult::Recheck => {}
                        ParkResult::TimedOut => unreachable!(),
                    }
                }
            }
        }

        completer.join().unwrap();
        assert!(io.is_completed());
    });
}

#[test]
fn io_token_multi_registrant_all_unparked() {
    loom::model(|| {
        let io = Arc::new(IoTokenCore::<ModelHandle>::new(2, 9));

        let registrant = {
            let io = Arc::clone(&io);
            thread::spawn(move || {
                let slot = fresh_slot();
                let tok = slot.issue_token();
                match io.register((Arc::clone(&slot), tok)) {
                    IoRegister::AlreadyCompleted => {}
                    IoRegister::Registered => {
                        while !io.is_completed() {
                            match slot.park_core(None, None, &CLOCK) {
                                ParkResult::Notified | ParkResult::Recheck => {}
                                ParkResult::TimedOut => unreachable!(),
                            }
                        }
                    }
                }
            })
        };

        // Completer-is-registrant: this thread registers itself, then
        // completes; its own handle gets a latched notify, the other
        // registrant (if it won the race) gets a real unpark.
        let slot = fresh_slot();
        let tok = slot.issue_token();
        assert_eq!(
            io.register((Arc::clone(&slot), tok)),
            IoRegister::Registered
        );
        let delivered = complete_all(&io);
        assert!(delivered >= 1, "own registration must be delivered");
        // Idempotence: nothing left for a second completer.
        assert_eq!(complete_all(&io), 0);

        registrant.join().unwrap();
    });
}

// ---------------------------------------------------------------------------
// M1 §2.9 wait protocol (IoTokenCore::wait_with): the reap/park/complete
// races behind bufmgr WaitIO's uring arm. The "ring" is modeled as one
// atomic state bit (CQE consumed / buffer state settled) — completers set
// state THEN complete the token, exactly reap_locked's order; the parker
// runs the production protocol with model park primitives.
// ---------------------------------------------------------------------------

use waiter::io::IoWaitOutcome;

/// Model park: a real block on the slot core (the LoomClock never times
/// out, so this only returns on a delivered notify).
fn model_park(slot: &Arc<Slot>) -> ParkResult {
    slot.park_core(None, None, &CLOCK)
}

/// Model park with the cadence already elapsed: a latched notify still wins
/// (Notified), otherwise the park returns Recheck immediately — models the
/// recheck backstop firing.
fn model_park_cadence_elapsed(slot: &Arc<Slot>) -> ParkResult {
    slot.park_core(None, Some(0), &CLOCK)
}

#[test]
fn uring_wait_reap_park_complete_race_never_hangs() {
    loom::model(|| {
        let state_done = Arc::new(AtomicI32::new(0));
        let io = Arc::new(IoTokenCore::<ModelHandle>::new(1, 1));

        // The reaping thread (owner boundary reap or foreign blocking
        // reap): buffer/ring state settles BEFORE the token completes.
        let reaper = {
            let state_done = Arc::clone(&state_done);
            let io = Arc::clone(&io);
            thread::spawn(move || {
                state_done.store(1, Ordering::Release);
                complete_all(&io);
            })
        };

        let slot = fresh_slot();
        let tok = slot.issue_token();
        let outcome = io.wait_with(
            (Arc::clone(&slot), tok),
            || model_park(&slot),
            || state_done.load(Ordering::Acquire) == 1,
            || unreachable!("untimed model park never rechecks"),
        );
        // Ordering contract: however the wait exits, the settled state must
        // be visible (reap_locked runs completions before complete()).
        match outcome {
            IoWaitOutcome::AlreadyCompleted | IoWaitOutcome::Completed => {
                assert_eq!(state_done.load(Ordering::Acquire), 1);
            }
            other => unreachable!("model cannot reach {other:?}"),
        }
        reaper.join().unwrap();
    });
}

#[test]
fn uring_wait_dropped_completion_recheck_backstop() {
    loom::model(|| {
        let state_done = Arc::new(AtomicI32::new(0));
        let io = Arc::new(IoTokenCore::<ModelHandle>::new(1, 2));

        // FAULT: the reaper consumes the CQE (state settles) but the token
        // completion is dropped — the PGRUST_TEST_URING_DROP_TOKEN_COMPLETE
        // shape. No unpark will ever arrive.
        let reaper = {
            let state_done = Arc::clone(&state_done);
            thread::spawn(move || {
                state_done.store(1, Ordering::Release);
            })
        };

        let slot = fresh_slot();
        let tok = slot.issue_token();
        let reap_state = Arc::clone(&state_done);
        let outcome = io.wait_with(
            (Arc::clone(&slot), tok),
            // Cadence-elapsed park: Recheck fires (no completer wake exists).
            || model_park_cadence_elapsed(&slot),
            || state_done.load(Ordering::Acquire) == 1,
            // Degraded targeted reap: the waiter consumes the CQE itself
            // (idempotent with the racing reaper in the real ring — the
            // ring mutex + done bit serialize; here the store is idempotent
            // by construction).
            || reap_state.store(1, Ordering::Release),
        );
        match outcome {
            // Backstop observed the settled state after the lost wake…
            IoWaitOutcome::StateSettled
            // …or the cadence beat the reaper and the waiter reaped itself.
            | IoWaitOutcome::Reaped => {}
            other => unreachable!("dropped completion cannot deliver {other:?}"),
        }
        assert_eq!(state_done.load(Ordering::Acquire), 1, "IO must be home");
        reaper.join().unwrap();
    });
}

#[test]
fn uring_wait_backstop_races_live_completer() {
    loom::model(|| {
        let state_done = Arc::new(AtomicI32::new(0));
        let io = Arc::new(IoTokenCore::<ModelHandle>::new(1, 3));

        // Healthy completer racing a waiter whose cadence fires anyway
        // (spurious recheck): every interleaving must terminate with the
        // state home and never a false StateSettled.
        let reaper = {
            let state_done = Arc::clone(&state_done);
            let io = Arc::clone(&io);
            thread::spawn(move || {
                state_done.store(1, Ordering::Release);
                complete_all(&io);
            })
        };

        let slot = fresh_slot();
        let tok = slot.issue_token();
        let reap_state = Arc::clone(&state_done);
        let outcome = io.wait_with(
            (Arc::clone(&slot), tok),
            || model_park_cadence_elapsed(&slot),
            || state_done.load(Ordering::Acquire) == 1,
            || reap_state.store(1, Ordering::Release),
        );
        match outcome {
            IoWaitOutcome::AlreadyCompleted
            | IoWaitOutcome::Completed
            | IoWaitOutcome::StateSettled => {
                assert_eq!(state_done.load(Ordering::Acquire), 1);
            }
            IoWaitOutcome::Reaped => {} // waiter drove it home itself
        }
        reaper.join().unwrap();
    });
}
