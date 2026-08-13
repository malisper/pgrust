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
//!   5. pg_sema-over-Waiter — a mirror of pg_sema's count + waiter-word
//!      Dekker (crates/backend/port/pg_sema): a concurrent lock/unlock pair
//!      never loses a wake, and the retire-on-return discipline leaves NO
//!      handle residue for the next ownership epoch (proc-slot/pool-thread
//!      reuse; the 2026-07 dev-profile "concurrent waiters" wedge shape).
//!   6. flushpipe-over-Waiter (GL-FLUSHPIPE-1) — a mirror of the
//!      pending-flush queue's register/complete Dekker
//!      (transam_xlog::flushpipe): a commit registrant either self-serves
//!      on the published flush result or is completed by a flusher's walk,
//!      in every interleaving, with the timeout backstop disabled; plus
//!      partial coverage (an uncovering flush neither completes early nor
//!      strands) and one-walk-completes-multiple (two registrants, hint
//!      coherence).
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
// SUPERSEDED-BUT-KEPT (LATCH-LOOM lane): the latch crate is now
// loom-buildable and latch/tests/loom.rs drives the REAL
// WaitLatch/SetLatch/ResetLatch code directly — those direct models are the
// authoritative latch lost-wakeup oracle. This mirror stays as belt and
// suspenders for the SLOT-CORE composition it also exercises, FROZEN in the
// coarser all-SeqCst-RMW dialect it was written in. Caveat discovered by
// the direct models' red battery (notes/dst-latch-loom.md): that coarse
// dialect over-synchronizes through phantom RMW writes (a SeqCst RMW on a
// WRITE edge acquire-chains the other side's recheck clock), so THIS mirror
// cannot catch wake-route ORDERING bugs (e.g. publish-after-arm); the
// direct models' refined dialect (read edges SC-RMW, write edges Release
// swap, fences elided) can, and does.
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
        // Early-return recheck as an RMW (LOOM-BREADTH inc-1; production is
        // fence + Relaxed load): once reset() joins the protocol this edge
        // is load-bearing — a stale 1 read here after a concurrent clear
        // skips the wake, the exact pairing C's ResetLatch fence comment
        // names ("or a concurrent SetLatch could skip the wake"). The RMW
        // carries the fences' recency in loom's dialect (see the big
        // comment below + loom_litmus.rs); the red battery demotes it back
        // to Relaxed and latch_reset_recheck_no_lost_wake must fail.
        if self.is_set.fetch_add(0, Ordering::SeqCst) != 0 {
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

    /// latch.rs ResetLatch: clear is_set, then pg_memory_barrier() — "the
    /// is_set clear must reach memory before we read any flag variables, or
    /// a concurrent SetLatch could skip the wake". Store + fence rendered as
    /// a SeqCst swap, the module's RMW translation.
    fn reset(&self) {
        self.is_set.swap(0, Ordering::SeqCst);
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

/// The RESET race surface (LOOM-BREADTH inc-1): C's canonical latch loop
/// `for(;;){ if (work) break; WaitLatch(); ResetLatch(); }` with a STRAY
/// prior set (any signal-shaped SetLatch — rendered as a deterministic
/// pre-set so the model stays 2 threads) racing the real work setter. The
/// owner wakes on the stray set and resets: the work setter's set_latch
/// racing that reset must still deliver — either its early-return check
/// sees the cleared is_set (full Dekker set follows) or, having seen 1,
/// the SC order puts its work-flag store before the owner's post-reset
/// flag re-read. The flag edges carry ResetLatch's fence strength (RMW
/// translation, see set() / reset()). A lost wake parks the owner forever
/// — loom's deadlock detector fails the model.
#[test]
fn latch_reset_recheck_no_lost_wake() {
    loom::model(|| {
        let slot = fresh_slot();
        let token = slot.issue_token();
        let latch = Arc::new(ModelLatch::new());
        let work = Arc::new(AtomicI32::new(0));

        // Stray set, deterministic prefix: is_set = 1, no work posted.
        latch.set(&slot);

        // Work setter: post the flag, then set — the C discipline.
        let worker = {
            let latch = Arc::clone(&latch);
            let slot = Arc::clone(&slot);
            let work = Arc::clone(&work);
            thread::spawn(move || {
                // Plain SC store, NOT an RMW: the work flag is a plain
                // fence-ordered write in production, and an RMW here would
                // chain-acquire the owner's reset through the flag cell,
                // causally protecting set()'s early-return check and making
                // the model vacuous for the reset race (its red battery
                // then passes). The is_set/maybe_sleeping edges stay RMWs
                // per the module dialect; the flag edges stay plain so the
                // stale-read window the fences must close remains open for
                // loom to drive through.
                work.store(1, Ordering::SeqCst);
                latch.set(&slot);
            })
        };

        // Owner: the canonical wait loop. The first wait short-circuits on
        // the stray set; the reset then races the worker's set. Terminates
        // in EVERY interleaving (a reset eating the work wake deadlocks).
        loop {
            // Plain SC load (see the worker's comment): no RMW causal
            // bridge through the flag cell.
            if work.load(Ordering::SeqCst) != 0 {
                break;
            }
            latch.wait(&slot, token);
            latch.reset();
        }

        worker.join().unwrap();
    });
}

// ---------------------------------------------------------------------------
// pg_sema-over-Waiter.
//
// Mirror of crates/backend/port/pg_sema's PGSemaphoreLock/Unlock protocol
// (count + published waiter word) over the slot core: the count post and the
// handle publication form the same Dekker pair as the latch — an unlock
// either sees the published handle (unpark delivers) or the locker's recheck
// sees the posted count. The models additionally pin the RETIRE-ON-RETURN
// discipline: a returned lock leaves waiter == 0, so an ownership-epoch
// change (proc-slot reuse, wretain token reissue — the 2026-07 dev-profile
// "concurrent waiters on one PGPROC semaphore" wedge) finds no stale
// residue, and the in-lock single-waiter assert stays exact.
// ---------------------------------------------------------------------------

struct ModelSema {
    count: AtomicI32,
    /// Packed handle word of the blocked owner (0 = none). The model packs
    /// (1 << 32) | token, mirroring WakerHandle's never-zero layout.
    waiter: AtomicU64,
}

fn pack(token: u32) -> u64 {
    (1u64 << 32) | token as u64
}

impl ModelSema {
    fn new() -> Self {
        ModelSema {
            count: AtomicI32::new(0),
            waiter: AtomicU64::new(0),
        }
    }

    /// pg_sema PGSemaphoreLock. Flag edges are RMWs where the production
    /// code uses fence-disciplined plain stores/loads — the same loom-0.7
    /// expressibility translation as ModelLatch (see set()'s comment): the
    /// count recheck read, the waiter read in unlock(), and the retire
    /// store are RMWs so loom carries the recency the SeqCst fences
    /// guarantee on the real memory model, at equal-or-stronger ordering.
    fn lock(&self, slot: &Slot, token: u32) {
        let handle = pack(token);
        let mut published = false;
        loop {
            let mut c = self.count.load(Ordering::Acquire);
            while c > 0 {
                match self
                    .count
                    .compare_exchange_weak(c, c - 1, Ordering::SeqCst, Ordering::Acquire)
                {
                    Ok(_) => {
                        if published {
                            // Retire-on-return (production: store(0, SeqCst)).
                            self.waiter.swap(0, Ordering::SeqCst);
                        }
                        return;
                    }
                    Err(actual) => c = actual,
                }
            }
            let prev = self.waiter.swap(handle, Ordering::SeqCst);
            // The production debug_assert: with retire-on-return, prev is 0
            // or this call's own handle in EVERY interleaving and EVERY
            // ownership epoch — anything else is a concurrent waiter (or,
            // pre-fix, the cross-epoch residue this model regression-pins).
            assert!(
                prev == 0 || prev == handle,
                "pg_sema model: concurrent waiters / stale cross-epoch residue"
            );
            published = true;
            fence(Ordering::SeqCst);
            if self.count.fetch_add(0, Ordering::SeqCst) > 0 {
                continue;
            }
            let r = slot.park_core(None, None, &CLOCK);
            assert!(matches!(r, ParkResult::Notified | ParkResult::Recheck));
        }
    }

    /// pg_sema PGSemaphoreUnlock: post the count, then wake the published
    /// handle (production: fence + load(Acquire) + handle-validated unpark).
    fn unlock(&self, slot: &Slot) {
        self.count.fetch_add(1, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        let word = self.waiter.fetch_add(0, Ordering::SeqCst);
        if word != 0 {
            // Handle-validated in production (a stale token is a no-op).
            slot.unpark_token(word as u32);
        }
    }
}

#[test]
fn pg_sema_lock_unlock_no_lost_wake() {
    loom::model(|| {
        let slot = fresh_slot();
        let token = slot.issue_token();
        let sema = Arc::new(ModelSema::new());

        let unlocker = {
            let sema = Arc::clone(&sema);
            let slot = Arc::clone(&slot);
            thread::spawn(move || {
                sema.unlock(&slot);
            })
        };

        // Any interleaving (post-before-lock, post-during-publication,
        // post-after-park) must complete: loom's deadlock detector is the
        // lost-wake oracle.
        sema.lock(&slot, token);
        assert_eq!(sema.count.load(Ordering::SeqCst), 0);

        unlocker.join().unwrap();
    });
}

#[test]
fn pg_sema_retire_on_return_no_epoch_residue() {
    loom::model(|| {
        let slot = fresh_slot();
        let token1 = slot.issue_token();
        let sema = Arc::new(ModelSema::new());

        // Epoch 1: a lock/unlock pair in every interleaving (parked or
        // fast-path).
        let unlocker = {
            let sema = Arc::clone(&sema);
            let slot = Arc::clone(&slot);
            thread::spawn(move || {
                sema.unlock(&slot);
            })
        };
        sema.lock(&slot, token1);
        unlocker.join().unwrap();

        // Retire-on-return: NO residue may survive the returned lock —
        // this is exactly what the pre-fix code violated (the handle stayed
        // published after the wait).
        assert_eq!(
            sema.waiter.load(Ordering::SeqCst),
            0,
            "returned lock left its handle published"
        );

        // Ownership-epoch boundary: pool-thread reuse reissues the token
        // (outstanding handles go stale); proc-slot reuse hands the sema to
        // a thread with a different handle. Pre-fix, epoch 2's lock read
        // epoch 1's handle here and tripped the single-waiter assert.
        let token2 = slot.reissue_token();
        assert_ne!(token1, token2);

        let unlocker = {
            let sema = Arc::clone(&sema);
            let slot = Arc::clone(&slot);
            thread::spawn(move || {
                sema.unlock(&slot);
            })
        };
        sema.lock(&slot, token2);
        unlocker.join().unwrap();

        assert_eq!(sema.waiter.load(Ordering::SeqCst), 0);
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

// ---------------------------------------------------------------------------
// flushpipe-over-Waiter (GL-FLUSHPIPE-1).
//
// Mirror of crates/backend/access/transam/transam_xlog/src/flushpipe.rs's
// register/complete protocol: the pending-flush queue (link under lock +
// min_pending hint) and the flush publication (logFlushResult store + walk)
// form a Dekker pair over two SeqCst fences — a flusher either sees the
// registrant's link (its walk completes the node) or the registrant's
// post-link recheck sees the published flush result (it self-serves under
// the lock). The models run with the production park loop's TIMEOUT BACKSTOP
// DISABLED (untimed parks): they prove the fence pair alone, loom's deadlock
// detector being the lost-wake oracle.
//
// Expressibility translations, as in ModelSema above (loom carries recency
// for RMWs, not for SC stores — the GL-STMTTASK-3 semaphore lesson): the
// production logFlushResult publication (Release store serialized by
// WALWriteLock) is fetch_max(SeqCst) — equal-or-stronger and monotonic like
// the lock-serialized original; the min_pending hint write (fence-guarded
// plain store under the spinlock) is swap(SeqCst); the completed
// publication (Release store whose production recency rides the
// latch-mutex release/acquire chain into the woken waiter) is swap(SeqCst);
// the fence-guarded loads are fetch_add(0, SeqCst). The intrusive-list
// mechanics themselves are pinned by flushpipe's own unit tests; THESE
// models own the interleavings.
// KEEP IN SYNC with flushpipe.rs (module doc cross-references this file).
// ---------------------------------------------------------------------------

/// The queue mirror: linked nodes are indices present in `list` (sorted by
/// LSN), min_pending mirrors the lock-free skip hint.
struct ModelPipe {
    list: loom::sync::Mutex<Vec<usize>>,
    min_pending: AtomicU64,
    flush_result: AtomicU64,
}

struct ModelPipeNode {
    lsn: u64,
    completed: AtomicU64, // 0/1 (AtomicBool shape; u64 for the RMW reads)
    slot: Arc<Slot>,
    token: u32,
}

impl ModelPipe {
    fn new() -> Self {
        ModelPipe {
            list: loom::sync::Mutex::new(Vec::new()),
            min_pending: AtomicU64::new(u64::MAX),
            flush_result: AtomicU64::new(0),
        }
    }

    fn refresh_hint(&self, list: &[usize], nodes: &[ModelPipeNode]) {
        let min = list.iter().map(|&i| nodes[i].lsn).min().unwrap_or(u64::MAX);
        // RMW where production is a fence-disciplined plain store (header
        // translation note).
        self.min_pending.swap(min, Ordering::SeqCst);
    }

    /// flushpipe::wait_for_flush (backstop disabled).
    fn register_and_wait(&self, nodes: &[ModelPipeNode], me: usize) {
        {
            let mut list = self.list.lock().unwrap();
            let pos = list
                .iter()
                .position(|&i| nodes[i].lsn > nodes[me].lsn)
                .unwrap_or(list.len());
            list.insert(pos, me);
            self.refresh_hint(&list, nodes);
        }
        // Registrant fence: pairs with the flusher's post-publish fence.
        fence(Ordering::SeqCst);
        if self.flush_result.fetch_add(0, Ordering::SeqCst) >= nodes[me].lsn {
            let self_unlinked = {
                let mut list = self.list.lock().unwrap();
                match list.iter().position(|&i| i == me) {
                    Some(pos) => {
                        list.remove(pos);
                        self.refresh_hint(&list, nodes);
                        true
                    }
                    None => false, // a completer collected us; wake imminent
                }
            };
            if self_unlinked {
                return;
            }
        }
        // (Production also kicks the walwriter latch here; the model's
        // flusher threads are the guaranteed progress.)
        while nodes[me].completed.fetch_add(0, Ordering::SeqCst) == 0 {
            let r = nodes[me].slot.park_core(None, None, &CLOCK);
            assert!(matches!(r, ParkResult::Notified | ParkResult::Recheck));
        }
    }

    /// XLogWrite-tail publication + flushpipe::complete_up_to.
    fn publish_and_complete(&self, nodes: &[ModelPipeNode], flushed: u64) {
        self.flush_result.fetch_max(flushed, Ordering::SeqCst);
        // Flusher fence: pairs with the registrant's post-link fence.
        fence(Ordering::SeqCst);
        if self.min_pending.fetch_add(0, Ordering::SeqCst) > flushed {
            return;
        }
        let covered: Vec<usize> = {
            let mut list = self.list.lock().unwrap();
            let covered: Vec<usize> =
                list.iter().copied().filter(|&i| nodes[i].lsn <= flushed).collect();
            list.retain(|&i| nodes[i].lsn > flushed);
            self.refresh_hint(&list, nodes);
            covered
        };
        for i in covered {
            // Production order: completed publication THEN SetLatch (swap
            // per the header translation note).
            nodes[i].completed.swap(1, Ordering::SeqCst);
            nodes[i].slot.unpark_token(nodes[i].token);
        }
    }
}

fn model_pipe_node(lsn: u64) -> ModelPipeNode {
    let slot = fresh_slot();
    let token = slot.issue_token();
    ModelPipeNode { lsn, completed: AtomicU64::new(0), slot, token }
}

#[test]
fn flushpipe_register_vs_flush_no_lost_wake() {
    loom::model(|| {
        let pipe = Arc::new(ModelPipe::new());
        let nodes = Arc::new(vec![model_pipe_node(100)]);

        // One flusher whose flush covers the registrant, at EVERY point
        // relative to the registration (publish-before-link => registrant
        // self-serves; link-before-publish => the walk completes it; the
        // straddles => the fence pair forces one side to win). Deadlock =
        // lost wake.
        let flusher = {
            let pipe = Arc::clone(&pipe);
            let nodes = Arc::clone(&nodes);
            thread::spawn(move || {
                pipe.publish_and_complete(&nodes, 150);
            })
        };

        pipe.register_and_wait(&nodes, 0);
        // Exit licenses (module-doc invariants): durable at return, never
        // linked at return.
        assert!(pipe.flush_result.load(Ordering::SeqCst) >= nodes[0].lsn);
        assert!(!pipe.list.lock().unwrap().contains(&0));

        flusher.join().unwrap();
    });
}

#[test]
fn flushpipe_partial_coverage_never_completes_early() {
    // The list-mutex + slot ops make these two models branch-heavy;
    // unbounded exploration is minutes-scale, out of family with the other
    // waiter models. Bounded per the runtime-model precedent
    // (preemption_bound = Some(3); loom guidance: real bugs surface within
    // 2-3 preemptions — the register/complete deadlock this file's history
    // caught reproduced at bound 3 in seconds).
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    b.check(|| {
        let pipe = Arc::new(ModelPipe::new());
        let nodes = Arc::new(vec![model_pipe_node(200)]);

        // The first flush (150) does NOT cover the registrant (its walk
        // must neither complete nor strand it — min_pending 200 > 150 is
        // the correct skip); the second (250) does. The registrant must
        // return exactly once, only after 250 is published.
        let flusher = {
            let pipe = Arc::clone(&pipe);
            let nodes = Arc::clone(&nodes);
            thread::spawn(move || {
                pipe.publish_and_complete(&nodes, 150);
                pipe.publish_and_complete(&nodes, 250);
            })
        };

        pipe.register_and_wait(&nodes, 0);
        assert!(
            pipe.flush_result.load(Ordering::SeqCst) >= 200,
            "returned before its covering flush was published"
        );
        assert!(!pipe.list.lock().unwrap().contains(&0));

        flusher.join().unwrap();
        assert_eq!(pipe.min_pending.load(Ordering::SeqCst), u64::MAX);
    });
}

#[test]
fn flushpipe_one_walk_completes_multiple_in_lsn_order() {
    // Bounded like flushpipe_partial_coverage_never_completes_early (the
    // 4-thread space is the largest of the three models).
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    b.check(|| {
        let pipe = Arc::new(ModelPipe::new());
        let nodes = Arc::new(vec![model_pipe_node(100), model_pipe_node(200)]);

        // Two concurrent registrants, one covering flush: every
        // interleaving of the two links, the two rechecks, and the single
        // walk must complete both (walk-collects or self-serve each).
        let reg_b = {
            let pipe = Arc::clone(&pipe);
            let nodes = Arc::clone(&nodes);
            thread::spawn(move || {
                pipe.register_and_wait(&nodes, 1);
                assert!(pipe.flush_result.load(Ordering::SeqCst) >= 200);
            })
        };
        let flusher = {
            let pipe = Arc::clone(&pipe);
            let nodes = Arc::clone(&nodes);
            thread::spawn(move || {
                pipe.publish_and_complete(&nodes, 250);
            })
        };

        pipe.register_and_wait(&nodes, 0);
        assert!(pipe.flush_result.load(Ordering::SeqCst) >= 100);

        reg_b.join().unwrap();
        flusher.join().unwrap();
        assert!(pipe.list.lock().unwrap().is_empty());
        assert_eq!(pipe.min_pending.load(Ordering::SeqCst), u64::MAX);
    });
}


// ---------------------------------------------------------------------------
// 7. LWLock wakeup flag handoff (GL-TESTFIX-1 F-R1-5).
//
// Mirror of lwlock's LWLockWakeup drain vs a woken waiter's re-enqueue: the
// waker proclist_delete's the waiter's NON-ATOMIC wait-link node, then
// publishes lwWaiting = LW_WS_NOT_WAITING; the waiter re-queues (push_tail
// writes the same node) as soon as it OBSERVES the flag. The subtlety this
// model pins: the waiter can reach that observation WITHOUT a semaphore
// edge — extraWaits reposts leave stale counts on its semaphore, so the
// sem-lock it consumes may pair with an OLD post, not this waker's. The
// flag itself must therefore carry the ordering: seam store Release, seam
// load Acquire (lmgr_proc::init_seams). C is sound with a plain store
// (pg_write_barrier + the sem syscall barrier + control dependency); the
// Rust model is not, for non-atomic link cells.
//
// Sensitivity (verified during F-R1-5 development, not encoded): modeling
// the PRE-fix shape — fence(Release) + Relaxed flag store on the waker,
// Relaxed flag load on the waiter — reds this model with loom's
// "Causality violation: Concurrent write accesses to `UnsafeCell`" in the
// first interleavings explored.
// ---------------------------------------------------------------------------

#[test]
fn lwlock_wakeup_flag_handoff_orders_link_writes() {
    loom::model(|| {
        // The waiter's proclist node (prev/next packed): non-atomic, exactly
        // SyncCell<proclist_node>.
        let link = Arc::new(loom::cell::UnsafeCell::new(0u64));
        // lwWaiting: LW_WS_PENDING_WAKEUP = 2 -> LW_WS_NOT_WAITING = 0.
        let flag = Arc::new(AtomicI32::new(2));

        let waker = {
            let link = Arc::clone(&link);
            let flag = Arc::clone(&flag);
            thread::spawn(move || {
                // LWLockWakeup drain: proclist_delete(&mut wakeup, cur) —
                // off-list marks written into the waiter's node...
                link.with_mut(|p| unsafe { *p = u64::MAX });
                // ...then the flag publication. Release = the seam store
                // (set_proc_lw_waiting); the sem unlock that follows in
                // production is deliberately NOT modeled — the waiter's
                // stale-count path does not synchronize through it.
                flag.store(0, Ordering::Release);
            })
        };

        // Waiter: consumed a STALE semaphore count (no edge with the waker),
        // now checks lwWaiting — the seam load (Acquire) — and re-queues.
        while flag.load(Ordering::Acquire) != 0 {
            loom::thread::yield_now();
        }
        // LWLockQueueSelf -> proclist_push_tail: writes its own node.
        link.with_mut(|p| unsafe { *p = 0x1 });

        waker.join().unwrap();
    });
}

// ---------------------------------------------------------------------------
// deferred-ticket-over-Waiter (inc-5(d) d1; GL-FLUSHPIPE-1 §8.6 G-d1-2 —
// the 4 mandatory models).
//
// Mirror of flushpipe.rs's DEFERRED registration (heap ticket + registry):
// REGISTRY REMOVAL IS THE CLAIM (a loom Mutex<Vec> mirrors the SpinLock'd
// Vec; exactly one remover per ticket) and ONLY the remover performs the
// PENDING->COMPLETED transition; the single tail host owns
// COMPLETED->CONSUMED. Both transitions are swap()s with asserts on the
// swapped-out value — the RMW dialect law (G-d1-2; loom carries recency
// for RMWs, not SC stores — 3 on-record incidents). Same Dekker pair as
// ModelPipe (post-link fence vs post-publish fence). The registrant and
// tail host share a thread here (the worker->session handoff edge is the
// RG completion edge, proven by its own models); the BACKSTOP laps are
// modeled as explicit deterministic branches (loom cannot time out — the
// production timeout only chooses WHEN the branch runs, not WHAT it does).
// KEEP IN SYNC with flushpipe.rs (module doc cross-references this file).
// ---------------------------------------------------------------------------

const TK_PENDING: u64 = 0;
const TK_COMPLETED: u64 = 1;
const TK_CONSUMED: u64 = 2;

struct ModelTicket {
    lsn: u64,
    state: AtomicU64,
    slot: Arc<Slot>,
    token: u32,
}

struct ModelTicketPipe {
    registry: loom::sync::Mutex<Vec<usize>>,
    min_pending: AtomicU64,
    flush_result: AtomicU64,
}

fn model_ticket(lsn: u64) -> ModelTicket {
    let slot = fresh_slot();
    let token = slot.issue_token();
    ModelTicket { lsn, state: AtomicU64::new(TK_PENDING), slot, token }
}

impl ModelTicketPipe {
    fn new() -> Self {
        ModelTicketPipe {
            registry: loom::sync::Mutex::new(Vec::new()),
            min_pending: AtomicU64::new(u64::MAX),
            flush_result: AtomicU64::new(0),
        }
    }

    fn refresh_hint(&self, reg: &[usize], tickets: &[ModelTicket]) {
        let min = reg.iter().map(|&i| tickets[i].lsn).min().unwrap_or(u64::MAX);
        self.min_pending.swap(min, Ordering::SeqCst);
    }

    /// flushpipe::DeferredRegistry::claim — removal IS the claim.
    fn claim(&self, tickets: &[ModelTicket], me: usize) -> bool {
        let mut reg = self.registry.lock().unwrap();
        match reg.iter().position(|&i| i == me) {
            Some(pos) => {
                reg.remove(pos);
                self.refresh_hint(&reg, tickets);
                true
            }
            None => false,
        }
    }

    /// flushpipe::register_deferred (registrant RETURNS; no park).
    fn register(&self, tickets: &[ModelTicket], me: usize) {
        {
            let mut reg = self.registry.lock().unwrap();
            reg.push(me);
            self.refresh_hint(&reg, tickets);
        }
        fence(Ordering::SeqCst);
        if self.flush_result.fetch_add(0, Ordering::SeqCst) >= tickets[me].lsn
            && self.claim(tickets, me)
        {
            // Born-covered self-claim: claimant owns PENDING->COMPLETED.
            assert_eq!(tickets[me].state.swap(TK_COMPLETED, Ordering::SeqCst), TK_PENDING);
        }
    }

    /// flushpipe::complete_deferred_up_to (+ the publication).
    fn publish_and_complete(&self, tickets: &[ModelTicket], flushed: u64) {
        self.flush_result.fetch_max(flushed, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        if self.min_pending.fetch_add(0, Ordering::SeqCst) > flushed {
            return;
        }
        let claimed: Vec<usize> = {
            let mut reg = self.registry.lock().unwrap();
            let claimed: Vec<usize> =
                reg.iter().copied().filter(|&i| tickets[i].lsn <= flushed).collect();
            reg.retain(|&i| tickets[i].lsn > flushed);
            self.refresh_hint(&reg, tickets);
            claimed
        };
        for i in claimed {
            assert_eq!(tickets[i].state.swap(TK_COMPLETED, Ordering::SeqCst), TK_PENDING);
            tickets[i].slot.unpark_token(tickets[i].token);
        }
    }

    /// flushpipe::ticket_wait, PARK path (backstop disabled — models 1/2).
    fn wait_consume(&self, tickets: &[ModelTicket], me: usize) {
        loop {
            if tickets[me].state.fetch_add(0, Ordering::SeqCst) == TK_COMPLETED {
                assert_eq!(
                    tickets[me].state.swap(TK_CONSUMED, Ordering::SeqCst),
                    TK_COMPLETED,
                    "single consumer"
                );
                return;
            }
            let r = tickets[me].slot.park_core(None, None, &CLOCK);
            assert!(matches!(r, ParkResult::Notified | ParkResult::Recheck));
        }
    }

    /// flushpipe::ticket_wait, one BACKSTOP lap taken deterministically
    /// (models 3/4): covered => self-claim+complete; uncovered => claim =
    /// Retry (true) / claimed-by-completer => fall through to consume.
    /// Returns true on Retry (ticket dead, caller re-flushes inline).
    fn backstop_then_consume(&self, tickets: &[ModelTicket], me: usize) -> bool {
        if tickets[me].state.fetch_add(0, Ordering::SeqCst) != TK_COMPLETED {
            if self.flush_result.fetch_add(0, Ordering::SeqCst) >= tickets[me].lsn {
                if self.claim(tickets, me) {
                    assert_eq!(
                        tickets[me].state.swap(TK_COMPLETED, Ordering::SeqCst),
                        TK_PENDING
                    );
                }
            } else if self.claim(tickets, me) {
                // Uncovered re-arm: ticket unlinked and dead (G-d1-1).
                assert_eq!(tickets[me].state.fetch_add(0, Ordering::SeqCst), TK_PENDING);
                return true;
            }
        }
        self.wait_consume(tickets, me);
        false
    }
}

/// Model 1 — register vs flush publication: the Dekker pair; a covering
/// flush at ANY point relative to registration never strands the ticket
/// (deadlock = lost wake).
#[test]
fn ticket_register_vs_flush_no_lost_wake() {
    loom::model(|| {
        let pipe = Arc::new(ModelTicketPipe::new());
        let tickets = Arc::new(vec![model_ticket(100)]);

        let flusher = {
            let pipe = Arc::clone(&pipe);
            let tickets = Arc::clone(&tickets);
            thread::spawn(move || {
                pipe.publish_and_complete(&tickets, 150);
            })
        };

        pipe.register(&tickets, 0);
        pipe.wait_consume(&tickets, 0);
        assert_eq!(tickets[0].state.load(Ordering::SeqCst), TK_CONSUMED);
        assert!(!pipe.registry.lock().unwrap().contains(&0));

        flusher.join().unwrap();
    });
}

/// Model 2 — consume-exactly-once vs completer mark: the completer's
/// COMPLETED swap and wake race the consumer's swap; the swapped-out
/// asserts are the exactly-once teeth.
#[test]
fn ticket_consume_exactly_once_vs_completer_mark() {
    loom::model(|| {
        let pipe = Arc::new(ModelTicketPipe::new());
        let tickets = Arc::new(vec![model_ticket(100)]);
        // Pre-registered (registration's own races are model 1).
        {
            let mut reg = pipe.registry.lock().unwrap();
            reg.push(0);
            pipe.refresh_hint(&reg, &tickets);
        }

        let flusher = {
            let pipe = Arc::clone(&pipe);
            let tickets = Arc::clone(&tickets);
            thread::spawn(move || {
                pipe.publish_and_complete(&tickets, 100);
            })
        };

        pipe.wait_consume(&tickets, 0);
        assert_eq!(tickets[0].state.load(Ordering::SeqCst), TK_CONSUMED);

        flusher.join().unwrap();
        assert_eq!(pipe.min_pending.load(Ordering::SeqCst), u64::MAX);
    });
}

/// Model 3 — retry re-arm (crash/flusher-death class) vs a late covering
/// flush: exactly one claimant wins; Retry leaves the ticket PENDING and
/// unlinked (the caller re-flushes inline — nothing can complete it
/// afterwards); a losing waiter falls through and consumes.
#[test]
fn ticket_retry_rearm_vs_completer_claim() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    b.check(|| {
        let pipe = Arc::new(ModelTicketPipe::new());
        let tickets = Arc::new(vec![model_ticket(200)]);
        {
            let mut reg = pipe.registry.lock().unwrap();
            reg.push(0);
            pipe.refresh_hint(&reg, &tickets);
        }

        // The flusher publishes a COVERING flush concurrently with the
        // waiter's backstop lap that (racing it) may still read the
        // pre-publication flush_result and take the uncovered-Retry arm.
        let flusher = {
            let pipe = Arc::clone(&pipe);
            let tickets = Arc::clone(&tickets);
            thread::spawn(move || {
                pipe.publish_and_complete(&tickets, 250);
            })
        };

        let retried = pipe.backstop_then_consume(&tickets, 0);
        if retried {
            // The waiter claimed first: the ticket is dead-PENDING and
            // unlinked; the completer's walk must NOT have completed it.
            assert_eq!(tickets[0].state.load(Ordering::SeqCst), TK_PENDING);
        } else {
            assert_eq!(tickets[0].state.load(Ordering::SeqCst), TK_CONSUMED);
        }
        assert!(!pipe.registry.lock().unwrap().contains(&0));

        flusher.join().unwrap();
    });
}

/// Model 4 (the review's added model) — completer walk vs the waiter's
/// covered-backstop self-claim: both are legal claimants of a covered
/// ticket; registry removal arbitrates, exactly one performs
/// PENDING->COMPLETED, the waiter always exits CONSUMED.
#[test]
fn ticket_completer_vs_backstop_selfserve_claim_race() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    b.check(|| {
        let pipe = Arc::new(ModelTicketPipe::new());
        let tickets = Arc::new(vec![model_ticket(100)]);
        {
            let mut reg = pipe.registry.lock().unwrap();
            reg.push(0);
            pipe.refresh_hint(&reg, &tickets);
        }
        // Flush ALREADY covering: the waiter's backstop lap self-claims
        // while the completer's walk claims concurrently.
        pipe.flush_result.fetch_max(150, Ordering::SeqCst);

        let flusher = {
            let pipe = Arc::clone(&pipe);
            let tickets = Arc::clone(&tickets);
            thread::spawn(move || {
                pipe.publish_and_complete(&tickets, 150);
            })
        };

        let retried = pipe.backstop_then_consume(&tickets, 0);
        assert!(!retried, "covered ticket must never Retry");
        assert_eq!(tickets[0].state.load(Ordering::SeqCst), TK_CONSUMED);
        assert!(!pipe.registry.lock().unwrap().contains(&0));

        flusher.join().unwrap();
        assert_eq!(pipe.min_pending.load(Ordering::SeqCst), u64::MAX);
    });
}
