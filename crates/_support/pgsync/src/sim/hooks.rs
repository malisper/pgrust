//! The WS-CORE seam (permit-s1 contract §2.1): the scheduler hook API the
//! sim-world wrappers call at every interception point. PINNED in WS-SYNC's
//! first pushed commit so WS-CORE develops against a fixed surface; WS-CORE
//! owns this file from then on (refinements are CORE's to make, with a
//! coordination row in both worklogs).
//!
//! Contract vocabulary (one call class per interception point): `block_on`,
//! `touch`, `timed_park`, `spawn`, `exit`, `pick_waiter`. WS-SYNC additions
//! required to make the wrapper protocol complete (deviations recorded in
//! `notes/permit-s1-sync.md` §deviations):
//! - [`SchedulerHooks::wake`] — the second half of every notify/unlock
//!   interception: names the `block_on`-parked thread that a seeded pick
//!   chose, so the scheduler can mark it runnable.
//! - [`SchedulerHooks::current_vpid`] — wrappers enqueue the CALLER in their
//!   wait lists; the scheduler owns thread→vpid identity (registered at the
//!   spawn door), so the wrappers ask rather than duplicate a registry.
//! - [`SchedulerHooks::now_ns`] — SimClock read for wrapper deadline math in
//!   `wait_timeout`-shaped ops (time advances only when nothing is runnable;
//!   P3 §2.4).
//!
//! THE WRAPPER PROTOCOL the scheduler must satisfy (and may rely on):
//! - Every wrapper op runs on a REGISTERED thread (registration at the
//!   launch_backend spawn door / `pgsync::thread` sim spawn prologue).
//! - `block_on(site, kind)` is the park: the caller yields the permit and
//!   the call returns only when the scheduler next grants this thread the
//!   permit (i.e. after some `wake(vpid)` named it, or — for `timed_park` —
//!   after the virtual deadline). Wrappers call it in a PREDICATE LOOP and
//!   re-check their condition on return, so a spurious grant is safe.
//! - Permit transfer happens ONLY at `block_on` / `timed_park` (parks) and,
//!   optionally + seeded, at `touch` (preemption). The query hooks
//!   (`pick_waiter`, `current_vpid`, `now_ns`) and `wake` never yield the
//!   permit.
//! - `wake(vpid)` MAY BE DROPPED when the target is not parked (e.g. it was
//!   preempted at a `touch` and is already runnable). Wrappers therefore use
//!   CHECK-BEFORE-PARK: a waker falsifies the wait predicate (removes the
//!   wakee from the wait list) BEFORE calling `wake`, and a waiter re-checks
//!   its predicate immediately before every park, with no hook call in the
//!   check→park gap (the permit is held across it, so no waker can run
//!   there). NOTE this corrects the originally pinned claim that
//!   enqueue-then-park alone cannot lose a wake — false when a hook sits in
//!   the gap (Condvar's unlock does; adversarial review BLOCKING-1,
//!   2026-07-18). Doc-only correction by WS-SYNC; the trait surface is
//!   unchanged and the wording matches the WS-CORE scheduler's shipped
//!   behavior (wake on a runnable slot is a no-op).
//! - `timed_park` returns `true` when the virtual deadline expired (the
//!   caller decides timeout vs notified by consulting its wait list).
//! - `pick_waiter(site, n)` returns an index in `0..n` drawn from
//!   SimEntropy — every OS-arbitrary choice (which wakee, which next holder,
//!   which sender proceeds) routes through it.
//! - `touch(site, kind)` marks an optional-preemption point (unlock, notify,
//!   send) — the seeded-preemption diversity dial (`PGRUST_SIM_PREEMPT_P`).
//! - Every hook carries the caller's `#[track_caller]` Location: the
//!   watchdog dump and the SCHEDOP log NAME blocked sites symbolically.
//!
//! Install discipline: `install()` once, at corpus boot, BEFORE any thread
//! the scheduler must govern is spawned. Mixed-mode (installing after
//! wrapper ops began parking on the std fallback path) is outside the
//! contract. With nothing installed the wrappers are plain std behavior.

use core::panic::Location;
use core::time::Duration;
// Raw std deliberately: this is pgsync's own control plane (the hooks slot
// must not recurse into the wrappers it serves). Scanner-visible; ledgered
// under the PERMIT-S1-SYNC allowlist band.
use std::sync::OnceLock as StdOnceLock;

/// Stable thread identity: `reserve_child_pid` vpids for backend threads
/// (P3 §1.4 inheritance); `pgsync::thread` sim spawns outside the door use
/// the synthetic range (`0x8000_0000..`).
pub type Vpid = u32;

/// Interception-point vocabulary for `block_on` / `touch` / SCHEDOP logging.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum OpClass {
    MutexLock,
    MutexUnlock,
    RwRead,
    RwWrite,
    RwUnlock,
    CondWait,
    CondNotify,
    ChanSend,
    ChanRecv,
    SemAcquire,
    BarrierWait,
    OnceInit,
    Join,
    Park,
    Sleep,
    Spawn,
    Exit,
}

impl OpClass {
    /// grep-stable lowercase token for SCHEDOP lines.
    pub fn as_str(self) -> &'static str {
        match self {
            OpClass::MutexLock => "mutex-lock",
            OpClass::MutexUnlock => "mutex-unlock",
            OpClass::RwRead => "rw-read",
            OpClass::RwWrite => "rw-write",
            OpClass::RwUnlock => "rw-unlock",
            OpClass::CondWait => "cond-wait",
            OpClass::CondNotify => "cond-notify",
            OpClass::ChanSend => "chan-send",
            OpClass::ChanRecv => "chan-recv",
            OpClass::SemAcquire => "sem-acquire",
            OpClass::BarrierWait => "barrier-wait",
            OpClass::OnceInit => "once-init",
            OpClass::Join => "join",
            OpClass::Park => "park",
            OpClass::Sleep => "sleep",
            OpClass::Spawn => "spawn",
            OpClass::Exit => "exit",
        }
    }
}

/// The permit scheduler, as the wrappers see it. Implemented by WS-CORE in
/// `pgsync::sim`; [`PanicHooks`] is the development stub.
pub trait SchedulerHooks: Sync {
    /// Would-block park: yield the permit; return when this thread is next
    /// granted it. Called in a predicate loop (spurious grants are safe).
    fn block_on(&self, site: &'static Location<'static>, kind: OpClass);

    /// Non-blocking touch (unlock/notify/send): optional seeded preemption.
    fn touch(&self, site: &'static Location<'static>, kind: OpClass);

    /// Timed park on SimClock virtual time (sleep and every `*_timeout`).
    /// Returns `true` iff the virtual deadline expired (vs woken earlier).
    /// Virtual time advances only when nothing is runnable (P3 §2.4).
    fn timed_park(&self, site: &'static Location<'static>, dur: Duration) -> bool;

    /// Thread registration (the spawn door + `pgsync::thread` sim spawns).
    fn spawn(&self, vpid: Vpid, site: &'static Location<'static>);

    /// Thread deregistration; post-dates all shared TLS teardown (P3 §1.6
    /// rule 3: joiners wake on this). `#[track_caller]` (F5 ledger row) so
    /// the Exit / join-Wake SCHEDOP lines carry the wrapper's exit site.
    #[track_caller]
    fn exit(&self, vpid: Vpid);

    /// Seeded choice over `n` waiters (SimEntropy): returns an index in
    /// `0..n`. Callers guarantee `n >= 1`.
    fn pick_waiter(&self, site: &'static Location<'static>, n: usize) -> usize;

    /// Mark a `block_on`-parked thread runnable (the wakee a seeded pick
    /// chose; the notify/unlock second half).
    fn wake(&self, vpid: Vpid, site: &'static Location<'static>);

    /// The CALLER's vpid (scheduler-owned thread→vpid registry).
    fn current_vpid(&self) -> Vpid;

    /// SimClock read (ns) for wrapper deadline math.
    fn now_ns(&self) -> u64;
}

static HOOKS: StdOnceLock<&'static dyn SchedulerHooks> = StdOnceLock::new();

/// Install the scheduler (once per process, at corpus boot, before governed
/// threads spawn).
pub fn install(h: &'static dyn SchedulerHooks) {
    HOOKS
        .set(h)
        .unwrap_or_else(|_| panic!("pgsync::sim::hooks: scheduler already installed"));
}

/// The installed scheduler, if any. `None` = wrappers behave as plain std
/// (today's sim binaries: sim-net-e2e / dst-smoke are unchanged).
#[inline]
pub fn installed() -> Option<&'static dyn SchedulerHooks> {
    HOOKS.get().copied()
}

/// Panic-on-call stub (contract §2.1): pins the surface so WS-CORE can
/// develop unit batteries against a fixed trait before the scheduler lands.
/// Never installed by pgsync itself.
pub struct PanicHooks;

impl SchedulerHooks for PanicHooks {
    fn block_on(&self, site: &'static Location<'static>, kind: OpClass) {
        panic!("pgsync sim hooks stub: block_on({}) at {site}", kind.as_str());
    }
    fn touch(&self, site: &'static Location<'static>, kind: OpClass) {
        panic!("pgsync sim hooks stub: touch({}) at {site}", kind.as_str());
    }
    fn timed_park(&self, site: &'static Location<'static>, dur: Duration) -> bool {
        panic!("pgsync sim hooks stub: timed_park({dur:?}) at {site}");
    }
    fn spawn(&self, vpid: Vpid, site: &'static Location<'static>) {
        panic!("pgsync sim hooks stub: spawn(vpid={vpid}) at {site}");
    }
    fn exit(&self, vpid: Vpid) {
        panic!("pgsync sim hooks stub: exit(vpid={vpid})");
    }
    fn pick_waiter(&self, site: &'static Location<'static>, n: usize) -> usize {
        panic!("pgsync sim hooks stub: pick_waiter(n={n}) at {site}");
    }
    fn wake(&self, vpid: Vpid, site: &'static Location<'static>) {
        panic!("pgsync sim hooks stub: wake(vpid={vpid}) at {site}");
    }
    fn current_vpid(&self) -> Vpid {
        panic!("pgsync sim hooks stub: current_vpid()");
    }
    fn now_ns(&self) -> u64 {
        panic!("pgsync sim hooks stub: now_ns()");
    }
}
