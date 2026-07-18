//! Loom models for the fdnb shared-layer duplex protocol (LOOM-BREADTH
//! inc-2 item 1: the sim-net / fd-provider reader/writer duplex, the
//! poll/write interplay that N1 fixed — see `fdnb/src/lib.rs` and
//! `notes/p4-simnet-lane.md` §4).
//!
//! Run: RUSTFLAGS="--cfg loom" cargo test -p fdnb --test loom --release
//!
//! ## What this proves (and the mirror-dialect it proves it IN)
//!
//! N1 (`write_noblock`, src/lib.rs): POLLOUT on a pipe does NOT license a
//! blocking write of an arbitrary buffer — the kernel guarantees room for
//! only a bounded amount, and an uncapped blocking write TRANSFERS WHAT FITS
//! AND THEN BLOCKS. In a *bidirectional* protocol where each peer flushes
//! before it drains (pqcomm's `internal_flush_buffer` then `pq_recvbuf`),
//! two such blocking flushes deadlock: each fills the other's buffer to
//! capacity and parks, and neither ever reaches its drain. The fix caps each
//! noblock write at `NB_WRITE_CAP` so a full/near-full buffer returns
//! promptly (short/WouldBlock) and the peer's event loop goes on to DRAIN —
//! breaking the cycle.
//!
//! Loom cannot intercept the real `libc::poll`/`libc::write` syscalls the fd
//! provider executes, so these models drive a MIRROR duplex (`Pipe` = a
//! bounded byte count under a loom `Mutex`, `Ready` = the mirror of
//! `pgsync::ParkLot`, the runtime idle-worker eventcount). The mirror is
//! FAITHFUL to the two behaviours that matter: (a) a capped noblock write
//! never parks; (b) an uncapped blocking flush parks until the whole
//! remainder fits. The determinism-lint header already rules the real
//! syscall sites behind the §2.4 seam; this is the exhaustive guard for the
//! *protocol shape* the seam implements. Mirror-dialect honesty:
//! `notes/loom-breadth-inc2.md` §1.
//!
//! Models:
//!   1. `duplex_capped_flush_no_deadlock` — two peers, each sends L>cap bytes
//!      then receives L, over two cap-C pipes, using CAPPED non-blocking
//!      writes + interleaved drains parked on the shared readiness lot. In
//!      EVERY interleaving both peers complete (loom's deadlock detector is
//!      the oracle; the lost-wakeup surface is the readiness lot).
//!   2. `duplex_readiness_wake_never_lost` — the isolated lost-wakeup pin:
//!      a peer that captured the lot epoch, found no progress, and is about
//!      to park is still woken by the other side's state change (the N2/N1
//!      "empty+writer-live ⇒ park, not spin" readiness rule).
#![cfg(loom)]

use std::sync::Arc;

use loom::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use loom::sync::{Condvar, Mutex};
use loom::thread;

/// Mirror of `pgsync::ParkLot` (the runtime idle-worker eventcount): a peer
/// with nothing to do captures `epoch()`, re-checks its readiness, then
/// `park(seen)`; any buffer state change bumps the epoch (SeqCst) before
/// notifying under the mutex, so a wake issued at any point relative to the
/// park is never lost.
struct Ready {
    epoch: AtomicUsize,
    m: Mutex<()>,
    cv: Condvar,
}

impl Ready {
    fn new() -> Self {
        Ready { epoch: AtomicUsize::new(0), m: Mutex::new(()), cv: Condvar::new() }
    }
    fn epoch(&self) -> usize {
        self.epoch.load(SeqCst)
    }
    fn park(&self, seen: usize) {
        let mut g = self.m.lock().unwrap();
        while self.epoch.load(SeqCst) == seen {
            g = self.cv.wait(g).unwrap();
        }
    }
    fn wake(&self) {
        // Bump BEFORE the mutex (the ParkLot lost-wakeup argument).
        self.epoch.fetch_add(1, SeqCst);
        let g = self.m.lock().unwrap();
        drop(g);
        self.cv.notify_all();
    }
}

/// A bounded byte channel (mirror of one direction of the sim-net duplex /
/// an fd pipe). `used` is the byte count; `cap` the buffer capacity.
struct Pipe {
    used: Mutex<usize>,
    /// Blocking-flush condvar — used ONLY by the buggy uncapped arm to wait
    /// for room. The capped arm never touches it.
    room_cv: Condvar,
    cap: usize,
}

impl Pipe {
    fn new(cap: usize) -> Self {
        Pipe { used: Mutex::new(0), room_cv: Condvar::new(), cap }
    }

    /// N1 FIX shape: a non-blocking, per-call-capped write. Returns the
    /// bytes accepted (0 = WouldBlock, buffer full). NEVER parks — the whole
    /// point of the cap.
    fn write_noblock_capped(&self, want: usize, cap_step: usize, ready: &Ready) -> usize {
        let mut used = self.used.lock().unwrap();
        let free = self.cap - *used;
        if free == 0 {
            return 0;
        }
        let n = want.min(free).min(cap_step);
        *used += n;
        drop(used);
        ready.wake(); // data available for the peer's drain
        n
    }

    /// N1 BUG shape: an uncapped BLOCKING write — waits until the WHOLE
    /// `want` fits, then transfers it. This is what "poll said writable, so
    /// issue the full blocking write" does on a pipe.
    ///
    /// RED-BATTERY ARM: `duplex_capped_flush_no_deadlock` swaps
    /// `peer_capped` → `peer_blocking` to reproduce the N1 deadlock; loom's
    /// detector must catch it. Kept in-tree (dead in the green build) so the
    /// red is a one-line swap, not a re-author (`notes/loom-breadth-inc2.md`
    /// §1 red battery).
    #[allow(dead_code)]
    fn write_blocking_uncapped(&self, want: usize) {
        let mut used = self.used.lock().unwrap();
        while self.cap - *used < want {
            used = self.room_cv.wait(used).unwrap();
        }
        *used += want;
    }

    /// Drain up to `upto` bytes; returns bytes removed (0 = empty). Frees
    /// room, so it wakes both the readiness lot (capped arm) and the
    /// room_cv (blocking arm).
    fn drain(&self, upto: usize, ready: &Ready) -> usize {
        let mut used = self.used.lock().unwrap();
        let g = (*used).min(upto);
        *used -= g;
        drop(used);
        if g > 0 {
            self.room_cv.notify_all();
            ready.wake();
        }
        g
    }
}

/// The CORRECT (N1-fixed) peer event loop: capped non-blocking writes
/// interleaved with drains; park on the shared readiness lot only when a
/// full round moved no byte in either direction. Finite: total volume is
/// bounded and every no-progress park is woken by the peer's next state
/// change.
fn peer_capped(out: &Pipe, inn: &Pipe, l: usize, cap_step: usize, ready: &Ready) {
    let mut sent = 0;
    let mut recvd = 0;
    while sent < l || recvd < l {
        let seen = ready.epoch();
        let mut progressed = false;
        if recvd < l {
            let g = inn.drain(l - recvd, ready);
            if g > 0 {
                recvd += g;
                progressed = true;
            }
        }
        if sent < l {
            let w = out.write_noblock_capped(l - sent, cap_step, ready);
            if w > 0 {
                sent += w;
                progressed = true;
            }
        }
        if sent >= l && recvd >= l {
            break;
        }
        if !progressed {
            // Nothing to do: the buffers are full-out / empty-in. Park until
            // the peer drains our `out` or fills our `inn` (readiness rule:
            // empty+writer-live ⇒ park, never spin).
            ready.park(seen);
        }
    }
}

/// The BUGGY (uncapped) peer: flush the WHOLE message with a blocking write,
/// THEN drain — the "flush before read" order every wire protocol uses.
/// With two peers and cap < L this is the N1 deadlock. RED-BATTERY ARM.
#[allow(dead_code)]
fn peer_blocking(out: &Pipe, inn: &Pipe, l: usize, ready: &Ready) {
    out.write_blocking_uncapped(l);
    let mut recvd = 0;
    while recvd < l {
        let seen = ready.epoch();
        let g = inn.drain(l - recvd, ready);
        if g > 0 {
            recvd += g;
        } else {
            ready.park(seen);
        }
    }
}

/// Model 1: two peers, two cap-1 pipes, L=2 (> cap), capped step 1. The
/// capped non-blocking flush + interleaved drain completes in every
/// interleaving — no deadlock.
///
/// PREEMPTION-BOUNDED at 3 (the runtime whole-runtime models' landed
/// pattern): the two-peer event loop over the shared readiness lot is a
/// branch-heavy space that is intractable fully unbounded, and loom's own
/// guidance is that real bugs — including the N1 deadlock this proves —
/// surface within 2-3 preemptions (the RED both-peers-blocking deadlock
/// appears at 0-1). The fast tier further env-caps permutations/duration
/// like every Builder model; the exhaustive tier runs this bound
/// unbounded-permutations.
#[test]
fn duplex_capped_flush_no_deadlock() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    // The event loop's per-round branch count (mutex + cv on every pipe op)
    // overruns the default 1k budget on a single execution.
    b.max_branches = 200_000;
    b.check(|| {
        const L: usize = 2;
        const CAP: usize = 1;
        const STEP: usize = 1;
        let a2b = Arc::new(Pipe::new(CAP));
        let b2a = Arc::new(Pipe::new(CAP));
        let ready = Arc::new(Ready::new());

        let peer_b = {
            let (out, inn, ready) = (Arc::clone(&b2a), Arc::clone(&a2b), Arc::clone(&ready));
            thread::spawn(move || peer_capped(&out, &inn, L, STEP, &ready))
        };
        // Peer A on this thread: out = a2b, in = b2a.
        peer_capped(&a2b, &b2a, L, STEP, &ready);
        peer_b.join().unwrap();

        assert_eq!(*a2b.used.lock().unwrap(), 0, "a2b fully drained");
        assert_eq!(*b2a.used.lock().unwrap(), 0, "b2a fully drained");
    });
}

/// Model 2: the isolated readiness lost-wakeup pin. A consumer captures the
/// lot epoch, observes the pipe empty (would park), racing a producer that
/// writes one byte and wakes. The consumer must always end up draining the
/// byte — a lost wake parks it forever (deadlock oracle).
#[test]
fn duplex_readiness_wake_never_lost() {
    loom::model(|| {
        let pipe = Arc::new(Pipe::new(1));
        let ready = Arc::new(Ready::new());

        let producer = {
            let (pipe, ready) = (Arc::clone(&pipe), Arc::clone(&ready));
            thread::spawn(move || {
                pipe.write_noblock_capped(1, 1, &ready);
            })
        };

        let mut got = 0;
        while got < 1 {
            let seen = ready.epoch();
            let g = pipe.drain(1, &ready);
            if g > 0 {
                got += g;
            } else {
                ready.park(seen);
            }
        }
        producer.join().unwrap();
        assert_eq!(got, 1);
    });
}
