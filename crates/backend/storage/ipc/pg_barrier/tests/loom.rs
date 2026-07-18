//! Loom models for the pg_barrier phase protocol (LOOM-BREADTH inc-1: the
//! P3 scheduler was cancelled, so loom widens from the runtime crate to
//! every concurrency primitive; census arm (d) — pg_barrier is live
//! production protocol under nodehash/parallel.rs, D4).
//!
//! Run: RUSTFLAGS="--cfg loom" cargo test -p pg_barrier --test loom --release
//!
//! Under `--cfg loom` the barrier's state mutex is loom's and the park route
//! rides model-owned waiter Slots with UNTIMED parks (src/lib.rs `route`):
//! the 10ms InterruptPending cadence is gone, so a lost phase-advance wake
//! is a deadlock loom's detector reports — the models prove the unpark
//! protocol itself, not the poll backstop.
//!
//! Invariants modeled (the barrier's REAL contract, not its field layout):
//!   1. static party: every arrive_and_wait returns once all participants
//!      arrive (no lost release), and EXACTLY ONE participant per phase is
//!      elected (`true` return).
//!   2. dynamic party: a detach that completes the party releases the parked
//!      arriver in every interleaving, and the woken waiter inherits the
//!      election when the phase advanced without a releasing arrival.
//!   3. arrive_and_detach: the arriving detach releases the parked peer
//!      (exactly-one-election again), and never reads as last-detach while
//!      a peer is still attached.
//!
//! Sizing: 2 loom threads each (main counts), single phase — well inside
//! the loom-fast exhaustive budget; no preemption bound needed.
#![cfg(loom)]

use std::sync::Arc;

use pg_barrier::Barrier;

/// Invariant 1: 2-party static barrier — both arrivals return (loom's
/// deadlock detector oracles the parked side being released in EVERY
/// interleaving, including park-after-release) and exactly one is elected.
#[test]
fn barrier_static_party_release_single_election() {
    loom::model(|| {
        let b = Arc::new(Barrier::new(2));
        let peer = {
            let b = Arc::clone(&b);
            loom::thread::spawn(move || b.arrive_and_wait().unwrap())
        };
        let elected_here = b.arrive_and_wait().unwrap();
        let elected_peer = peer.join().unwrap();
        assert!(
            elected_here ^ elected_peer,
            "exactly one participant elected per phase (got {elected_here}/{elected_peer})"
        );
        assert_eq!(b.phase(), 1);
    });
}

/// Invariant 2: dynamic party — the last non-arrived participant detaching
/// advances the phase and must wake the parked arriver (detector-oracled);
/// the woken waiter inherits the election (the `elected != next_phase`
/// re-election arm), whichever side moves first.
#[test]
fn barrier_detach_releases_parked_arriver() {
    loom::model(|| {
        let b = Arc::new(Barrier::new(0));
        // Attach both parties on the main thread: a deterministic prefix —
        // the modeled race is arrive-vs-detach, not attach ordering.
        b.attach();
        b.attach();
        let arriver = {
            let b = Arc::clone(&b);
            loom::thread::spawn(move || {
                let elected = b.arrive_and_wait().unwrap();
                assert!(
                    elected,
                    "phase advanced without a releasing arrival: the woken \
                     waiter must inherit the election"
                );
            })
        };
        // Leaves without arriving: participants 2 -> 1; if the peer already
        // arrived this completes the party (phase advance + wake), otherwise
        // the peer's own arrival releases itself.
        b.detach();
        arriver.join().unwrap();
        assert_eq!(b.phase(), 1);
    });
}

/// Invariant 3: an ARRIVING detach releases the parked peer, is never
/// last-detach with a peer attached, and election lands on exactly one side
/// (the peer when it was already parked; the peer-as-releasing-arrival when
/// the detach went first).
#[test]
fn barrier_arrive_and_detach_releases_peer() {
    loom::model(|| {
        let b = Arc::new(Barrier::new(0));
        b.attach();
        b.attach();
        let peer = {
            let b = Arc::clone(&b);
            loom::thread::spawn(move || b.arrive_and_wait().unwrap())
        };
        let last = b.arrive_and_detach();
        assert!(!last, "peer still attached: arrive_and_detach is not last");
        let peer_elected = peer.join().unwrap();
        assert!(
            peer_elected,
            "the sole remaining participant owns the phase election"
        );
        assert_eq!(b.phase(), 1);
    });
}
