//! DIRECT loom models of the latch lost-wakeup contract (LATCH-LOOM lane).
//!
//! Run: RUSTFLAGS="--cfg loom" cargo test -p latch --test loom --release
//!
//! These models drive the REAL production code — `latch::WaitLatch`,
//! `latch::SetLatch`, `latch::ResetLatch`, the real local-latch slab, and
//! the real waiter slot core underneath the park — not a mirror. They retire
//! the standing "latch is not loom-buildable → mirror models only"
//! disclosure (the latch-over-Waiter mirror in waiter/tests/loom.rs is now
//! superseded-but-kept; see notes/dst-latch-loom.md).
//!
//! What "lost wakeup" means: the waiter checks its flag (is_set), finds it
//! clear, and goes to sleep; the setter sets the flag and looks for a
//! sleeper to wake. If the two interleave so that the setter misses the
//! sleeper AND the sleeper misses the flag, the waiter sleeps forever with
//! its flag set — the lost wakeup. C latch.c prevents it with a Dekker
//! store->load discipline (pg_memory_barrier() pairs); the port renders
//! those barriers as fence(SeqCst) + the Latch protocol-edge methods.
//!
//! Model dialect (disclosed): loom 0.7 does not model the single total
//! order of SeqCst operations/fences (waiter/tests/loom_litmus.rs proves it
//! admits the SC store-buffer weak outcome; loom rt/thread.rs has SC-op
//! ordering literally disabled), so the fence-disciplined plain flag
//! accesses compile, UNDER LOOM ONLY, to the RMW dialect inside the Latch
//! methods, and the pg_memory_barrier() calls compile out (their strength
//! rides the RMW edges) — types_storage latch.rs, "THE DIALECT LAW" block,
//! including why write edges are Release swaps and why a live loom fence
//! would poison the model. The control flow, protocol structure, waker-word
//! publication, park/unpark composition, and the waiter slot state machine
//! are the production code verbatim — loom explores and verifies all of
//! THAT for real; each dialect edge stands in for exactly one C barrier at
//! equal-or-stronger ordering.
//!
//! Red battery (transient source weakenings, run + reverted; verdicts +
//! the R2 investigation in notes/dst-latch-loom.md):
//!   R1 strip the wake: delete set_latch's `waiter::unpark_word` call
//!      → CAUGHT, 3 models deadlock in 0.00s. The naked-wake control red.
//!   R2 wake-route order: publish the waker AFTER arming maybe_sleeping in
//!      wait_latch_on_waiter (the Dekker order the source comment pins)
//!      → CAUGHT, 3 models deadlock (the setter reads a stale waker word
//!      and skips the unpark). This red drove the dialect refinement: it
//!      passed silently under both the all-SeqCst-RMW translation and live
//!      loom fences — see the dialect-fidelity guards at the bottom.
//!   R3 omit SetLatch's FIRST pg_memory_barrier(): demote
//!      set_path_probe_is_set's loom arm to the native Relaxed load
//!      → CAUGHT: reset_rewait_cycle deadlocks (stale is_set=1 read after
//!      a concurrent ResetLatch skips the wake — the exact race C's
//!      ResetLatch comment names).
//!   R4 omit SetLatch's SECOND pg_memory_barrier(): demote
//!      set_path_sleeping_armed's loom arm to the native Acquire load
//!      → CAUGHT, 3 models deadlock in 0.26s (armed sleeper missed).
//!
//! Seam story (mission 3): the three wait-path seam registries
//! (waiteventset_seams, procsignal_seams, timeout_seams — plus
//! waitevent_seams' pgstat pair) compile under loom as-is and stay EMPTY in
//! models: their `is_installed()` guards read never-written statics, so the
//! drain hooks are inert, and nothing may install a seam inside
//! `loom::model` (seam slots are process-global — cross-iteration state, and
//! `set()` panics on the second iteration by design). "A signal fired" IS
//! `SetLatch` from another thread: the procsignal sender pends a signo and
//! calls SetLatch, the timeout timer thread posts and calls SetLatch — the
//! [`signal_world`] driver below is that exact call, which is why the models
//! need no installed seams to exercise the wake path.
#![cfg(loom)]

use loom::sync::atomic::{AtomicI32, Ordering};
use loom::sync::Arc;
use loom::thread;

use types_storage::latch::LatchHandle;
use types_storage::waiteventset::WL_LATCH_SET;

/// The loom-world analog of a signal/event delivery: exactly the call the
/// procsignal sender / timeout timer thread / waiteventset waker makes after
/// pending its payload. See the module doc's seam story.
mod signal_world {
    use super::*;

    pub fn deliver_signal_wake(h: LatchHandle) {
        latch::SetLatch(h);
    }
}

/// The canonical consumer wait: WL_LATCH_SET only (no timeout — the models
/// prove the wake protocol; the loom clock has no time). Returns having
/// OBSERVED the latch set — the contract under test.
fn wait_for_set(h: LatchHandle) {
    let res = latch::WaitLatch(Some(h), WL_LATCH_SET, 0, 0).expect("WaitLatch");
    assert_eq!(res, WL_LATCH_SET);
}

fn fresh_latch() -> LatchHandle {
    let h = latch::allocate_local_latch();
    latch::InitLatch(h);
    h
}

/// THE lost-wakeup contract, directly on the production code: waiter does
/// check-flag→park (WaitLatch), setter does set-flag→wake (SetLatch) from
/// another model thread, at every interleaving loom explores. No execution
/// may leave a parked waiter with its flag set — loom's deadlock detector is
/// the oracle, and the terminal assert pins that the wait observed the flag.
#[test]
fn latch_direct_set_vs_wait_never_lost() {
    loom::model(|| {
        let h = fresh_latch();

        let setter = thread::spawn(move || {
            signal_world::deliver_signal_wake(h);
        });

        // Before, during (mid-Dekker), or after the park entry: the set
        // must always be observed.
        wait_for_set(h);
        assert!(latch::latch_ref(h).is_set());

        setter.join().unwrap();
        latch::free_local_latch(h);
    });
}

/// Deterministic short-circuit arm: a set that lands before the wait entry
/// must be consumed without parking (the latched-notify path through the
/// real slab + waiter slot).
#[test]
fn latch_direct_set_before_wait_short_circuits() {
    loom::model(|| {
        let h = fresh_latch();
        latch::SetLatch(h);
        wait_for_set(h);
        assert!(latch::latch_ref(h).is_set());
        latch::free_local_latch(h);
    });
}

/// The reset/re-wait cycle — C's canonical latch loop
/// `for (;;) { if (work) break; WaitLatch(); ResetLatch(); }` with a STRAY
/// prior set (any signal-shaped SetLatch, rendered as a deterministic
/// prefix so the model stays 2 threads) racing the real work setter. The
/// owner wakes on the stray set and resets; the worker's set_latch racing
/// that reset must still deliver — either its early-return probe sees the
/// cleared is_set (and runs the full Dekker set), or, having seen 1, the
/// probe's RMW edge orders the owner's post-reset work re-read after the
/// worker's work store (the recency C's barriers guarantee). A lost wake
/// parks the owner forever.
#[test]
fn latch_direct_reset_rewait_cycle_no_lost_wake() {
    loom::model(|| {
        let h = fresh_latch();
        let work = Arc::new(AtomicI32::new(0));

        // Stray set, deterministic prefix: is_set = 1, no work posted.
        latch::SetLatch(h);

        let worker = {
            let work = Arc::clone(&work);
            thread::spawn(move || {
                // Plain SC store, NOT an RMW: the work flag is a plain
                // fence-ordered write in production, and an RMW here would
                // chain-acquire through the flag cell and causally protect
                // the probe edge, making the model vacuous for the reset
                // race (its red R3 would then pass). Same reasoning as the
                // superseded mirror (waiter/tests/loom.rs).
                work.store(1, Ordering::SeqCst);
                latch::SetLatch(h);
            })
        };

        // The canonical owner loop. Terminates in EVERY interleaving (a
        // reset eating the work wake deadlocks the model).
        loop {
            if work.load(Ordering::SeqCst) != 0 {
                break;
            }
            wait_for_set(h);
            latch::ResetLatch(h);
        }

        worker.join().unwrap();
        latch::free_local_latch(h);
    });
}

/// Two setters race one waiter: SetLatch is idempotent-concurrent (C: "This
/// is cheap if the latch is already set") — one signal-shaped delivery and
/// one plain set, in every interleaving, must produce exactly a woken waiter
/// observing is_set, with the second setter's early-return probe never
/// eating the wake.
#[test]
fn latch_direct_two_setters_race() {
    loom::model(|| {
        let h = fresh_latch();

        let s1 = thread::spawn(move || {
            latch::SetLatch(h);
        });
        let s2 = thread::spawn(move || {
            signal_world::deliver_signal_wake(h);
        });

        wait_for_set(h);
        assert!(latch::latch_ref(h).is_set());

        s1.join().unwrap();
        s2.join().unwrap();
        latch::free_local_latch(h);
    });
}

// ---------------------------------------------------------------------------
// Dialect-fidelity guards (the LATCH-LOOM investigation's load-bearing
// probes, kept as tripwires — each assert names the loom behavior the
// dialect law depends on; if loom's semantics shift, these fail FIRST and
// point at the law row to revisit).
// ---------------------------------------------------------------------------

/// Guards dialect-law condition "write edges acquire NOTHING": loom must
/// keep relaxed RMW reads causality-free. If this ever fails, loom has
/// started joining clocks on relaxed RMW reads — the write-edge swaps would
/// then over-synchronize and the R2 red class goes toothless.
#[test]
fn dialect_guard_relaxed_rmw_reads_do_not_join_causality() {
    let weak = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let w2 = std::sync::Arc::clone(&weak);
    loom::model(move || {
        let x = Arc::new(AtomicI32::new(0));
        let g = Arc::new(AtomicI32::new(0));
        let (x1, g1) = (Arc::clone(&x), Arc::clone(&g));
        let weak_t = std::sync::Arc::clone(&w2);
        let t = thread::spawn(move || {
            // reader side: relaxed RMW that may read t1's phantom write
            let prev = g1.swap(9, Ordering::Relaxed);
            if prev == 5 {
                let r = x1.load(Ordering::Relaxed);
                if r == 0 {
                    weak_t.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }
        });
        x.store(1, Ordering::Relaxed);
        g.fetch_add(5, Ordering::SeqCst); // release-write 5 with x=1 in clock
        t.join().unwrap();
    });
    assert!(
        weak.load(std::sync::atomic::Ordering::SeqCst),
        "loom JOINS causality on relaxed RMW reads (conservatism confirmed)"
    );
}


/// Guards model sensitivity for the R2 bug class: the publish-after-arm
/// lost wake (parked waiter + armed setter reading a stale waker word) must
/// stay EXPRESSIBLE in the dialect's op vocabulary. If this ever fails, the
/// direct models can no longer catch wake-route ordering bugs — the
/// R2-hiding investigation (worklog) must be re-run.
#[test]
fn dialect_guard_publish_after_arm_lost_wake_expressible() {
    let weak = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let w2 = std::sync::Arc::clone(&weak);
    loom::model(move || {
        let is_set = Arc::new(AtomicI32::new(0));
        let ms = Arc::new(AtomicI32::new(0));
        let wk = Arc::new(loom::sync::atomic::AtomicU64::new(0));
        let lost = Arc::new(AtomicI32::new(0));
        let (i1, m1, k1, l1) = (
            Arc::clone(&is_set),
            Arc::clone(&ms),
            Arc::clone(&wk),
            Arc::clone(&lost),
        );
        let setter = thread::spawn(move || {
            if i1.fetch_add(0, Ordering::SeqCst) != 0 {
                return;
            }
            i1.swap(1, Ordering::Release); // S2 mark_set (final dialect)
            if m1.fetch_add(0, Ordering::SeqCst) == 0 {
                return;
            }
            let r = k1.load(Ordering::Acquire); // S4
            if r == 0 {
                l1.store(1, Ordering::SeqCst); // wake LOST (no route)
            }
        });
        // waiter, R2 order
        if is_set.fetch_add(0, Ordering::SeqCst) == 0 {
            ms.swap(1, Ordering::Release); // W2 arm FIRST (the bug)
            wk.store(7, Ordering::Release); // W1 publish late
            if is_set.fetch_add(0, Ordering::SeqCst) == 0 {
                // W3' recheck saw 0: the waiter PARKS. Lost wake iff the
                // setter took the armed path but read waker 0.
                setter.join().unwrap();
                if lost.load(Ordering::SeqCst) == 1 {
                    w2.store(true, std::sync::atomic::Ordering::SeqCst);
                }
                return;
            }
        }
        setter.join().unwrap();
    });
    assert!(
        weak.load(std::sync::atomic::Ordering::SeqCst),
        "R2+parked: the lost-wake interleaving is NOT reachable in loom"
    );
}
