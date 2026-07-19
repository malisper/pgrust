//! `waiter_mailbox` — the shared channel-conversion utility proposed by the
//! DST P3 scheduler design (§3, the row-5/7/8/10 conversions): ONE audited
//! bounded/unbounded MPMC queue so the five raw-mpsc channel sites convert
//! onto a single modeled protocol instead of five bespoke ones.
//!
//! Implementation delta from the design text, disclosed: the design sketches
//! not-empty/not-full WAKER-WORD lists (the rg.rs discipline); this
//! implementation rides two [`ParkLot`] eventcounts instead — the identical
//! lost-wake-free protocol class (capture epoch under the state lock →
//! release → park; every unblocking state change bumps-then-notifies), with
//! two structural advantages the permit-s4 lane ratified:
//!   * no registration list to keep clean across error unwinds (the row-2
//!     "popped word whose owner unwound" trap cannot exist — there is no
//!     per-owner word to pop), and
//!   * pure pgsync: the mailbox compiles in ALL THREE worlds (native / loom /
//!     sim), so the loom models drive THIS production type directly instead
//!     of a mirror, and sim hooks see every park through the wrapped Condvar.
//! Wakes are wake-all; the herd is participant-bounded (encoder pools are
//! DOP-sized) and messages are batch-grain at every converted site.
//!
//! THE MAILBOX LAW (design §3, stated for the row-5 recv-under-mutex kill):
//! **never park holding the mailbox mutex.** Both `send` and `recv` capture
//! the relevant lot epoch UNDER the state lock, then RELEASE it, then park.
//! A receiver that blocks while holding the queue lock wedges every other
//! participant on a raw `Mutex::lock` — the token-holder shape invisible to
//! the sim layers until the lock shims land; the loom red-battery for this
//! module proves the deadlock is caught if the law is broken.
//!
//! Disconnect semantics mirror `std::sync::mpsc` exactly (the conversions
//! must be drop-in for abort topologies that lean on channel disconnect):
//!   * last `MailboxSender` drop closes the send side; receivers DRAIN the
//!     queue, then observe `None` (drain-then-exit, design row 10);
//!   * last `MailboxReceiver` drop closes the receive side; a blocked or
//!     subsequent `send` returns `Err(value)` (the "encoder pool exited
//!     early" leg of row 5, C's detached-mq send failure of row 7).
//! Sender/receiver drops during panic unwind run these transitions
//! automatically — the RAII property the raw-mpsc abort topologies relied
//! on is preserved by construction.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::{lock, Mutex, ParkLot};

struct MailboxState<T> {
    queue: VecDeque<T>,
    /// Live `MailboxSender` clones; 0 = send side closed.
    senders: usize,
    /// Live `MailboxReceiver` clones; 0 = receive side closed.
    receivers: usize,
}

struct MailboxCore<T> {
    state: Mutex<MailboxState<T>>,
    /// None = unbounded (sends never park).
    cap: Option<usize>,
    /// Receivers park here (empty queue); bumped by push and by send-side
    /// close.
    not_empty: ParkLot,
    /// Senders park here (full bounded queue); bumped by pop and by
    /// receive-side close.
    not_full: ParkLot,
}

/// Outcome of [`MailboxReceiver::try_recv`].
pub enum TryRecv<T> {
    Msg(T),
    Empty,
    /// Send side closed AND the queue is drained.
    Disconnected,
}

/// Error of [`MailboxSender::try_send`] (`mpsc::TrySendError` shape: the
/// caller owns the value back either way).
pub enum TrySend<T> {
    /// Bounded queue at capacity; nothing was enqueued.
    Full(T),
    /// Every receiver is gone; the message would never be consumed.
    Disconnected(T),
}

/// Sending half of a mailbox; clone-counted (last drop closes the side).
pub struct MailboxSender<T> {
    core: Arc<MailboxCore<T>>,
}

/// Receiving half of a mailbox; clone-counted MPMC (share by cloning — never
/// by wrapping one receiver in a Mutex; see the mailbox law above).
pub struct MailboxReceiver<T> {
    core: Arc<MailboxCore<T>>,
}

/// Build a mailbox. `cap = Some(n)` bounds the queue at n messages (senders
/// park while full — `sync_channel(n)` semantics); `None` is unbounded
/// (sends never park — `channel()` semantics).
pub fn mailbox<T>(cap: Option<usize>) -> (MailboxSender<T>, MailboxReceiver<T>) {
    let core = Arc::new(MailboxCore {
        state: Mutex::new(MailboxState {
            queue: VecDeque::new(),
            senders: 1,
            receivers: 1,
        }),
        cap,
        not_empty: ParkLot::new(),
        not_full: ParkLot::new(),
    });
    (
        MailboxSender { core: Arc::clone(&core) },
        MailboxReceiver { core },
    )
}

impl<T> MailboxSender<T> {
    /// Blocking send. Parks (lock RELEASED) while a bounded queue is full.
    /// `Err(value)` ⇔ every receiver is gone — the message would never be
    /// consumed (mpsc `SendError` semantics; the caller owns the value
    /// back).
    pub fn send(&self, value: T) -> Result<(), T> {
        let core = &*self.core;
        loop {
            let seen = {
                let mut g = lock(&core.state);
                if g.receivers == 0 {
                    return Err(value);
                }
                if core.cap.map_or(true, |c| g.queue.len() < c) {
                    g.queue.push_back(value);
                    drop(g);
                    core.not_empty.wake_all();
                    return Ok(());
                }
                // Full: capture the epoch UNDER the state lock (a pop that
                // frees space bumps strictly after its own state change, so
                // either we observe the bump here or the park sees it).
                core.not_full.epoch()
            };
            // The mailbox law: the state lock is released; park on the lot.
            core.not_full.park(seen);
        }
    }

    /// Non-blocking send (the row-8 feeder shape: one feeder round-robins
    /// several runs' channels and must not park on one full channel while
    /// the others starve). Never parks; a `Full` return leaves the queue
    /// untouched and hands the value back.
    pub fn try_send(&self, value: T) -> Result<(), TrySend<T>> {
        let core = &*self.core;
        let mut g = lock(&core.state);
        if g.receivers == 0 {
            return Err(TrySend::Disconnected(value));
        }
        if core.cap.map_or(true, |c| g.queue.len() < c) {
            g.queue.push_back(value);
            drop(g);
            core.not_empty.wake_all();
            return Ok(());
        }
        Err(TrySend::Full(value))
    }
}

impl<T> MailboxReceiver<T> {
    /// Blocking receive. Parks (lock RELEASED) while empty and senders
    /// remain. `None` ⇔ send side closed AND the queue is drained
    /// (drain-then-exit: pending messages are always delivered first).
    pub fn recv(&self) -> Option<T> {
        let core = &*self.core;
        loop {
            let seen = {
                let mut g = lock(&core.state);
                if let Some(v) = g.queue.pop_front() {
                    drop(g);
                    core.not_full.wake_all();
                    return Some(v);
                }
                if g.senders == 0 {
                    return None;
                }
                core.not_empty.epoch()
            };
            core.not_empty.park(seen);
        }
    }

    /// Non-blocking receive (row-7 shape: latch-driven leader drains between
    /// its own waits).
    pub fn try_recv(&self) -> TryRecv<T> {
        let core = &*self.core;
        let mut g = lock(&core.state);
        if let Some(v) = g.queue.pop_front() {
            drop(g);
            core.not_full.wake_all();
            return TryRecv::Msg(v);
        }
        if g.senders == 0 {
            return TryRecv::Disconnected;
        }
        TryRecv::Empty
    }
}

impl<T> Clone for MailboxSender<T> {
    fn clone(&self) -> Self {
        lock(&self.core.state).senders += 1;
        MailboxSender { core: Arc::clone(&self.core) }
    }
}

impl<T> Clone for MailboxReceiver<T> {
    fn clone(&self) -> Self {
        lock(&self.core.state).receivers += 1;
        MailboxReceiver { core: Arc::clone(&self.core) }
    }
}

impl<T> Drop for MailboxSender<T> {
    fn drop(&mut self) {
        let last = {
            let mut g = lock(&self.core.state);
            g.senders -= 1;
            g.senders == 0
        };
        if last {
            // Send side closed: wake every parked receiver so it can
            // observe the drain-then-None condition.
            self.core.not_empty.wake_all();
        }
    }
}

impl<T> Drop for MailboxReceiver<T> {
    fn drop(&mut self) {
        let last = {
            let mut g = lock(&self.core.state);
            g.receivers -= 1;
            g.receivers == 0
        };
        if last {
            // Receive side closed: wake every parked sender so its send can
            // return Err(value).
            self.core.not_full.wake_all();
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    // The four thread-spawning tests are native/loom-only: under
    // `--cfg pgrust_sim` the pgsync unit binary's scheduler tests install
    // the GLOBAL sim hooks, and a hooked Condvar wait on a thread that
    // never entered a scheduler door (these tests spawn raw std threads)
    // spins/wedges by design (unregistered-thread fall-through is per-op,
    // not per-wait-loop). Sim-world mailbox coverage = the single-threaded
    // tests below + the converted call sites' sim corpora (P5 demo). The
    // cross-test hook-leak interaction is ledgered in
    // notes/dst-permit-s4.md for the WS-CORE owner.

    #[test]
    fn send_recv_fifo() {
        let (tx, rx) = mailbox::<u32>(Some(4));
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        assert_eq!(rx.recv(), Some(1));
        assert_eq!(rx.recv(), Some(2));
    }

    #[test]
    fn drain_then_none_on_sender_drop() {
        let (tx, rx) = mailbox::<u32>(None);
        tx.send(7).unwrap();
        drop(tx);
        assert_eq!(rx.recv(), Some(7)); // pending message delivered first
        assert_eq!(rx.recv(), None); // then the close is observed
        assert_eq!(rx.recv(), None); // idempotent
    }

    #[test]
    fn send_fails_when_receivers_gone() {
        let (tx, rx) = mailbox::<u32>(Some(1));
        drop(rx);
        assert_eq!(tx.send(9), Err(9));
    }

    #[test]
    #[cfg(not(pgrust_sim))]
    fn bounded_send_parks_until_recv() {
        let (tx, rx) = mailbox::<u32>(Some(1));
        tx.send(1).unwrap();
        let t = std::thread::spawn(move || {
            tx.send(2).unwrap(); // parks: queue is at cap
            tx.send(3).unwrap();
        });
        assert_eq!(rx.recv(), Some(1));
        assert_eq!(rx.recv(), Some(2));
        assert_eq!(rx.recv(), Some(3));
        t.join().unwrap();
        assert_eq!(rx.recv(), None);
    }

    #[test]
    #[cfg(not(pgrust_sim))]
    fn recv_parks_until_send() {
        let (tx, rx) = mailbox::<u32>(Some(1));
        let t = std::thread::spawn(move || rx.recv());
        std::thread::sleep(std::time::Duration::from_millis(20));
        tx.send(42).unwrap();
        assert_eq!(t.join().unwrap(), Some(42));
    }

    #[test]
    #[cfg(not(pgrust_sim))]
    fn parked_sender_unblocks_on_receiver_drop() {
        let (tx, rx) = mailbox::<u32>(Some(1));
        tx.send(1).unwrap();
        let t = std::thread::spawn(move || tx.send(2)); // parks on full
        std::thread::sleep(std::time::Duration::from_millis(20));
        drop(rx); // last receiver: parked send must return Err(2)
        assert_eq!(t.join().unwrap(), Err(2));
    }

    #[test]
    fn mpmc_clone_counts() {
        let (tx, rx) = mailbox::<u32>(None);
        let tx2 = tx.clone();
        let rx2 = rx.clone();
        drop(tx);
        // tx2 still live: receivers must not see disconnect.
        tx2.send(5).unwrap();
        assert_eq!(rx2.recv(), Some(5));
        drop(tx2);
        assert_eq!(rx.recv(), None);
        assert_eq!(rx2.recv(), None);
    }

    #[test]
    fn try_send_full_and_disconnected() {
        let (tx, rx) = mailbox::<u32>(Some(1));
        assert!(tx.try_send(1).is_ok());
        assert!(matches!(tx.try_send(2), Err(TrySend::Full(2))));
        assert_eq!(rx.recv(), Some(1));
        assert!(tx.try_send(3).is_ok());
        drop(rx);
        assert!(matches!(tx.try_send(4), Err(TrySend::Disconnected(4))));
    }

    #[test]
    fn try_recv_distinguishes_empty_and_disconnected() {
        let (tx, rx) = mailbox::<u32>(None);
        assert!(matches!(rx.try_recv(), TryRecv::Empty));
        tx.send(1).unwrap();
        assert!(matches!(rx.try_recv(), TryRecv::Msg(1)));
        drop(tx);
        assert!(matches!(rx.try_recv(), TryRecv::Disconnected));
    }

    #[test]
    #[cfg(not(pgrust_sim))]
    fn pipeline_two_consumers() {
        let (tx, rx) = mailbox::<u64>(Some(2));
        let rx2 = rx.clone();
        let c1 = std::thread::spawn(move || {
            let mut got = Vec::new();
            while let Some(v) = rx.recv() {
                got.push(v);
            }
            got
        });
        let c2 = std::thread::spawn(move || {
            let mut got = Vec::new();
            while let Some(v) = rx2.recv() {
                got.push(v);
            }
            got
        });
        for i in 0..100u64 {
            tx.send(i).unwrap();
        }
        drop(tx);
        let mut all = c1.join().unwrap();
        all.extend(c2.join().unwrap());
        all.sort_unstable();
        assert_eq!(all, (0..100).collect::<Vec<_>>());
    }
}
