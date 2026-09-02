//! GL-FLUSHPIPE-1 — the pending-flush queue (flush pipelining v1).
//!
//! PORTED from the pre-v0.2 main (archive/main-t56-20260729): the v1
//! stack-node queue (archive 7a9a683159 + liveness 796e776348 + the
//! GL-FLUSHSIM-1 findings a2433546a4) and the inc-5(d) d1 DeferredTail
//! ticket half (archive e3bcfe4b1b). The stmt-task d2 commit-PREFIX
//! bracket (worker-inserted commit records + the committail mode) was NOT
//! ported — it is stmt-as-task substrate; the deferred-ticket API below is
//! the flushpipe half of that co-design, substrate-first per §8.2 (no
//! in-tree production caller until a tail-host consumer lands).
//!
//! # What this is
//!
//! The commit-durability wait, re-hosted: when a transaction commits with
//! synchronous_commit=on and a WAL flush is already IN FLIGHT, the
//! committing backend REGISTERS {commit LSN, its proc latch} on this queue
//! and parks, instead of joining the WALWriteLock convoy
//! (LWLockAcquireOrWait leader-follower). Whoever publishes a new flush
//! result (a commit-path leader, walwriter's background flush, any
//! XLogFlush caller) walks the queue afterwards and DIRECTLY wakes exactly
//! the registrants whose LSN the flush covered — LSN-ordered completion,
//! no herd wake, no lock re-formation scramble. Walwriter is the
//! guaranteed-progress flusher: every registration sets its latch (the
//! async-commit wake vector), and its cycle flushes up to the pending max
//! bypassing the wal_writer_flush_after/delay pacing.
//!
//! COMMIT ORDERING IS UNTOUCHED. This changes only HOW the committing
//! thread waits inside its flush step (RecordTransactionCommit's
//! XLogFlush(XactLastRecEnd), xact.c:1502); the caller still returns only
//! when the record is durable, and clog mark / ProcArrayEndTransaction /
//! lock release / the ack all stay after durability, byte-C. See
//! GL-FLUSHPIPE-1 letter §2 (the split decision: "v1 splits nothing",
//! coordinator-signed).
//!
//! # Knob
//!
//! `PGRUST_FLUSH_PIPELINE` — DEFAULT ON; `0`/`off` is the kill switch (t35
//! exact-spelling law, inverted — Michael ruled the archive's HOLD-CONFIRM
//! flip candidate 18e95a10b6 CONFIRMED after PR #1017 landed the pre-flip
//! posture). Disarmed the pipeline is structurally inert: the only
//! knob-OFF cost is one memoized bool read at the commit flush call site
//! ([`crate::write::XLogFlushPipelined`] falls straight into the incumbent
//! [`crate::write::XLogFlush`]) — the completion-walk hooks short-circuit
//! on the same memo. `PGRUST_FLUSH_PIPELINE_TRACE=1` adds per-event LOG
//! lines (e2e witnesses only; never default).
//!
//! # Protocol (the loom-modeled part)
//!
//! Registrant:                          Flusher:
//!   node.init (plain)                    ctl.logFlushResult.store(Release)
//!   lock { link; minmax update }         fence(SeqCst)
//!   fence(SeqCst)                        if min_pending > flushed: skip
//!   read ctl.logFlushResult              lock { unlink covered; minmax }
//!   covered? try_unlink : park           per node: completed.store(Release)
//!                                                  SetLatch(node's proc)
//!
//! The two SeqCst fences are the Dekker pair: in the fence total order one
//! side sees the other — either the registrant's post-link read sees the
//! new flush result (it self-serves), or the flusher's post-publish
//! min_pending read sees the link (it completes the node). Lost wakes are
//! therefore impossible; the park loop's timeout recheck stands behind it
//! as the debug backstop, as everywhere (a nonzero backstop-selfserve
//! counter in steady state is the missed-wake tripwire). Loom mirror:
//! crates/backend/storage/ipc/waiter/tests/loom.rs `flushpipe_*` (protocol
//! mirror, the pg_sema-model precedent) — keep both sides in sync.
//!
//! LIVENESS is self-sufficient, not flusher-dependent: an uncovered
//! backstop lap self-unlinks and returns [`FlushWaitOutcome::Retry`] — the
//! caller retakes its loop and contends for WALWriteLock itself, i.e. C's
//! uncovered-woken-follower-becomes-leader shape at backstop cadence. The
//! walwriter kick is the fast completion path, never the liveness
//! argument (walwriter exits before the shutdown checkpoint; a committer
//! parked across that window must be able to flush its own record).
//!
//! # Node lifetime (why the raw pointers are sound)
//!
//! A node lives on its registrant's STACK. Invariants:
//! - All link/unlink/traversal happens under [`Pipe::lck`].
//! - The registrant NEVER returns while its node is linked: it exits only
//!   after (a) it unlinked the node itself under the lock, or (b) it
//!   observed `completed == true` — and a completer sets `completed` only
//!   AFTER unlinking the node and never touches the node again afterwards
//!   (`procno` is copied out first).
//! - Completers collect (ptr, procno) under the lock; between unlink and
//!   the `completed` store the registrant is parked or looping (it cannot
//!   free the node before observing `completed`).
//!
//! # Bounded resources (contention-evidence laws)
//!
//! No new bounded resource: one node per committing backend (stack-owned,
//! zero allocation), at most one in flight per proc; the spinlock is never
//! held across a park, a wake, or any syscall (wakes are delivered after
//! release from a collected list). The queue introduces no new wait-for
//! edge: registrants wait on flush progress only, and flush progress never
//! waits on a registrant. New wait event `FlushPipeline` (IPC class) makes
//! the wait observable in pg_stat_activity (the contention-law tripwire).

use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst};
use std::sync::atomic::{fence, AtomicBool, AtomicU64, AtomicU8};
use std::sync::Arc;
use std::sync::OnceLock;

use types_core::{ProcNumber, XLogRecPtr, INVALID_PROC_NUMBER};
use types_storage::latch::LatchHandle;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

use crate::ctl::{SpinLock, XLogCtl};

// wait_event_names.txt IPC section: appended after the C 18.6 rows (index
// 58, pgrust-specific; the IPC names table gained the row in waitevent).
// upstream 33101632235a (18.6): index 57 is C's WalReceiverUpstreamCatchup.
const PG_WAIT_IPC: u32 = 0x0800_0000;
pub const WAIT_EVENT_FLUSH_PIPELINE: u32 = PG_WAIT_IPC + 58;

/// Park-loop timeout backstop (ms): defense-in-depth recheck cadence, NOT
/// a correctness input (the fence pair above is). Sized well under the
/// deadlock-watch class but long enough to never fire in a healthy run.
const BACKSTOP_MS: i64 = 100;

/// `PGRUST_FLUSH_PIPELINE` — DEFAULT ON (Michael's ruling on the archive
/// flip candidate 18e95a10b6, 2026-08-13; PR #1017 landed the pre-flip
/// posture). The kill switch disarms with exactly `0`/`off` (the t35
/// exact-spelling law, inverted); `1`/`on` remain accepted explicit-ON
/// spellings.
pub fn flush_pipeline_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_FLUSH_PIPELINE").ok().as_deref().map(str::trim),
            Some("0") | Some("off")
        )
    })
}

pub(crate) fn trace_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        matches!(
            std::env::var("PGRUST_FLUSH_PIPELINE_TRACE").ok().as_deref().map(str::trim),
            Some("1") | Some("on")
        )
    })
}

fn trace(msg: impl FnOnce() -> String) {
    if trace_enabled() {
        let _ = elog::elog(types_error::LOG, format!("flushpipe: {}", msg()));
    }
}

// ---------------------------------------------------------------------------
// Counters (witnesses for e2e + unit tests; Relaxed — diagnostics only).
// ---------------------------------------------------------------------------

pub struct FlushPipeStats {
    /// Registrations that linked a node (the pipelined waits).
    pub registered: AtomicU64,
    /// Registrations already covered at the post-link recheck (self-unlink).
    pub self_served: AtomicU64,
    /// Nodes completed by a flusher's queue walk (directed wakes).
    pub completed: AtomicU64,
    /// Pipelined-leader flushes (conditional WALWriteLock acquire won).
    pub leader_flushes: AtomicU64,
    /// Park-loop backstop rechecks that found the node covered (steady-state
    /// nonzero = missed-wake tripwire; see the module doc).
    pub backstop_self_served: AtomicU64,
    /// Liveness re-arms: TIMEOUT laps that found the flush still short of
    /// the node's LSN and handed the waiter back to the leader path
    /// (expected only in flusher-death windows, e.g. shutdown).
    pub retry_rearm: AtomicU64,
    /// Completions observed only AFTER a timeout lap (the directed wake
    /// was lost/misdirected/late; the completion itself still delivered).
    /// GL-FLUSHSIM-1 §5.2: closes the stats blind spot for lost WAKES —
    /// nonzero in steady state is the soft missed-wake tripwire (the node
    /// still exits via `completed`, so the L4 conservation identity
    /// registered == completed + self_served + backstop_self_served +
    /// retry_rearm is UNTOUCHED by this overlap counter).
    pub completed_after_timeout: AtomicU64,
    /// inc-5(d) d1 — deferred-ticket registrations (the DeferredTail half;
    /// L4-style identity: ticket_registered == ticket_completed +
    /// ticket_self_served + ticket_retry_rearm).
    pub ticket_registered: AtomicU64,
    /// Deferred tickets claimed by a completer walk (directed wakes).
    pub ticket_completed: AtomicU64,
    /// Deferred tickets claimed covered by the registrant/waiter itself
    /// (born-covered at registration, or the waiter's backstop lap).
    pub ticket_self_served: AtomicU64,
    /// Deferred-ticket liveness re-arms (G-d1-1: uncovered at backstop ⇒
    /// the WAITER retakes the leader path; flusher-death windows only).
    pub ticket_retry_rearm: AtomicU64,
}

pub static STATS: FlushPipeStats = FlushPipeStats {
    registered: AtomicU64::new(0),
    self_served: AtomicU64::new(0),
    completed: AtomicU64::new(0),
    leader_flushes: AtomicU64::new(0),
    backstop_self_served: AtomicU64::new(0),
    retry_rearm: AtomicU64::new(0),
    completed_after_timeout: AtomicU64::new(0),
    ticket_registered: AtomicU64::new(0),
    ticket_completed: AtomicU64::new(0),
    ticket_self_served: AtomicU64::new(0),
    ticket_retry_rearm: AtomicU64::new(0),
};

pub(crate) fn count_leader_flush() {
    STATS.leader_flushes.fetch_add(1, Relaxed);
    trace(|| "leader flush".into());
}

/// GL-FLUSHSIM-1 negotiated seam (letter §5.3 ask 1) — SIM BUILDS ONLY,
/// zero production cost: the flushsim mutation battery arms this one-shot
/// flag to make the NEXT registration skip its queue link (the true
/// dropped-REGISTRATION mutation; the wake-drop class is harness-side).
/// The dropped registrant parks forever awaiting a completion that cannot
/// come — the harness's liveness budget (L3) and counter-conservation (L4)
/// oracles are what must catch it.
#[cfg(pgrust_sim)]
pub mod sim {
    use std::sync::atomic::{AtomicBool, Ordering};

    pub static SKIP_NEXT_LINK: AtomicBool = AtomicBool::new(false);

    pub(crate) fn take_skip_link() -> bool {
        SKIP_NEXT_LINK.swap(false, Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// The queue.
// ---------------------------------------------------------------------------

/// One registrant's stack-resident wait node.
struct FlushWaitNode {
    lsn: XLogRecPtr,
    procno: ProcNumber,
    /// Set by a completer strictly AFTER unlinking (Release); the
    /// registrant's exit license on the completion path (Acquire).
    completed: AtomicBool,
    /// Intrusive links + membership. Guarded by [`Pipe::lck`].
    prev: UnsafeCell<*mut FlushWaitNode>,
    next: UnsafeCell<*mut FlushWaitNode>,
    linked: UnsafeCell<bool>,
}

/// List ends. Guarded by [`Pipe::lck`].
struct PipeList {
    /// Lowest LSN.
    head: *mut FlushWaitNode,
    /// Highest LSN.
    tail: *mut FlushWaitNode,
}

struct Pipe {
    lck: SpinLock,
    list: UnsafeCell<PipeList>,
    /// Lowest pending LSN, `u64::MAX` when empty. Written under `lck`;
    /// read lock-free by [`complete_up_to`]'s skip (fence-paired, module
    /// doc). Conservatively stale reads are safe: a miss on a brand-new
    /// entry is exactly the race the fence pair closes.
    min_pending: AtomicU64,
    /// Highest pending LSN, 0 when empty. Written under `lck`; read
    /// lock-free by the walwriter override ([`pending_max`]).
    max_pending: AtomicU64,
}

// SAFETY: `list` (and node link fields) are only touched under `lck`; the
// node-lifetime invariants are in the module doc.
unsafe impl Sync for Pipe {}

static PIPE: Pipe = Pipe {
    lck: SpinLock::new(),
    list: UnsafeCell::new(PipeList { head: ptr::null_mut(), tail: ptr::null_mut() }),
    min_pending: AtomicU64::new(u64::MAX),
    max_pending: AtomicU64::new(0),
};

impl Pipe {
    /// Recompute the lock-free hints from the list ends. Call under `lck`
    /// after every mutation.
    ///
    /// SAFETY: caller holds `lck`.
    unsafe fn refresh_hints(&self) {
        let l = &*self.list.get();
        if l.head.is_null() {
            self.min_pending.store(u64::MAX, Relaxed);
            self.max_pending.store(0, Relaxed);
        } else {
            self.min_pending.store((*l.head).lsn, Relaxed);
            self.max_pending.store((*l.tail).lsn, Relaxed);
        }
    }

    /// LSN-ordered insert (from the tail: commit LSNs mostly arrive in
    /// order, so this is O(1) in the common case).
    ///
    /// SAFETY: caller holds `lck`; `node` is unlinked and outlives its
    /// list membership (module-doc invariants).
    unsafe fn link(&self, node: *mut FlushWaitNode) {
        let l = &mut *self.list.get();
        debug_assert!(!*(*node).linked.get());
        let mut at = l.tail;
        while !at.is_null() && (*at).lsn > (*node).lsn {
            at = *(*at).prev.get();
        }
        // Insert after `at` (null = new head).
        if at.is_null() {
            *(*node).prev.get() = ptr::null_mut();
            *(*node).next.get() = l.head;
            if l.head.is_null() {
                l.tail = node;
            } else {
                *(*l.head).prev.get() = node;
            }
            l.head = node;
        } else {
            let after = *(*at).next.get();
            *(*node).prev.get() = at;
            *(*node).next.get() = after;
            *(*at).next.get() = node;
            if after.is_null() {
                l.tail = node;
            } else {
                *(*after).prev.get() = node;
            }
        }
        *(*node).linked.get() = true;
        self.refresh_hints();
    }

    /// SAFETY: caller holds `lck`; `node` is linked.
    unsafe fn unlink(&self, node: *mut FlushWaitNode) {
        let l = &mut *self.list.get();
        debug_assert!(*(*node).linked.get());
        let prev = *(*node).prev.get();
        let next = *(*node).next.get();
        if prev.is_null() {
            l.head = next;
        } else {
            *(*prev).next.get() = next;
        }
        if next.is_null() {
            l.tail = prev;
        } else {
            *(*next).prev.get() = prev;
        }
        *(*node).linked.get() = false;
        self.refresh_hints();
    }
}

/// A completed-collection entry: everything a completer may touch after
/// releasing the lock. `node` is dereferenced exactly once (the
/// `completed` store) and never after it.
struct Covered {
    node: *mut FlushWaitNode,
    procno: ProcNumber,
    lsn: XLogRecPtr,
}

/// Directed completion: wake every registrant whose LSN `flushed` covers.
/// Call AFTER publishing a new `logFlushResult` (and never under
/// WALWriteLock — wakes must not lengthen the flush critical path).
/// Knob-OFF cost at the hook sites: one memoized bool read.
pub(crate) fn complete_up_to(flushed: XLogRecPtr) {
    if !flush_pipeline_enabled() {
        return;
    }
    // Dekker fence (module doc): pairs with the registrant's post-link
    // fence — BOTH registration flavors (stack node and deferred ticket)
    // pair against this one emission. The per-half hint skips below are
    // the common-case zero-cost exits.
    fence(SeqCst);
    complete_deferred_up_to(flushed);
    if PIPE.min_pending.load(Relaxed) > flushed {
        return;
    }
    let mut covered: Vec<Covered> = Vec::new();
    PIPE.lck.with(|| {
        // SAFETY: under lck.
        unsafe {
            let l = &*PIPE.list.get();
            let mut at = l.head;
            while !at.is_null() && (*at).lsn <= flushed {
                let next = *(*at).next.get();
                PIPE.unlink(at);
                covered.push(Covered { node: at, procno: (*at).procno, lsn: (*at).lsn });
                at = next;
            }
        }
    });
    if covered.is_empty() {
        return;
    }
    STATS.completed.fetch_add(covered.len() as u64, Relaxed);
    trace(|| {
        format!(
            "completed {} waiter(s) up to {:X}/{:X}",
            covered.len(),
            flushed >> 32,
            flushed & 0xFFFF_FFFF
        )
    });
    for c in covered {
        let _ = c.lsn;
        // SAFETY: the node is unlinked and its registrant is still waiting
        // on `completed` (module-doc lifetime invariants). This store is
        // the LAST touch of the node; `procno` was copied under the lock.
        unsafe {
            (*c.node).completed.store(true, Release);
        }
        latch::SetLatch(LatchHandle::proc(c.procno));
    }
}

/// The walwriter override input: highest pending LSN, if any registrant
/// is waiting — across BOTH registration flavors. Lock-free read (hint
/// semantics — the walwriter cycle that consumes it also walks the queue
/// afterwards, so staleness only costs one extra cycle).
pub(crate) fn pending_max() -> Option<XLogRecPtr> {
    if !flush_pipeline_enabled() {
        return None;
    }
    let stack = PIPE.max_pending.load(Acquire);
    let deferred = DEFERRED.max_pending.load(Acquire);
    match stack.max(deferred) {
        0 => None,
        lsn => Some(lsn),
    }
}

/// Whether the pipelined commit wait may be used right now: the knob is
/// armed AND the guaranteed-progress flusher (walwriter) is alive AND this
/// thread owns a latch to park on. Callers fall back to the incumbent
/// [`crate::write::XLogFlush`] otherwise. (pub: deferred-path admission
/// consults it cross-crate — G-d1-1 iii, walwriter dead ⇒ REFUSE the
/// deferred path, the caller hosts the whole commit as today.)
pub fn pipeline_available() -> bool {
    if !flush_pipeline_enabled() {
        return false;
    }
    if lmgr_proc::ProcGlobal().walwriterProc.load(Relaxed) == INVALID_PROC_NUMBER {
        return false;
    }
    init_small::globals::MyLatch().is_some()
}

fn wake_walwriter() {
    let walwriter = lmgr_proc::ProcGlobal().walwriterProc.load(Relaxed);
    if walwriter != INVALID_PROC_NUMBER {
        latch::SetLatch(LatchHandle::proc(walwriter));
    }
}

/// How a pipelined wait ended. (pub since d1: `ticket_wait` is
/// crate-external API.)
pub enum FlushWaitOutcome {
    /// The wait is over and the caller's LSN is covered (directed
    /// completion or covered self-serve). The caller's loop re-checks and
    /// breaks.
    Completed,
    /// LIVENESS re-arm: the backstop found the flush still short of our
    /// LSN — the flusher we were depending on may no longer exist
    /// (walwriter exit at shutdown, crash-reinit windows). We have
    /// self-unlinked; the caller must retake its loop and contend for
    /// WALWriteLock ITSELF (C's follower self-sufficiency: an uncovered
    /// woken follower becomes the next leader). Fires only at backstop
    /// cadence — never in a healthy pipeline.
    Retry,
}

/// Register on the pending queue and wait until `logFlushResult >= lsn`
/// (or a liveness re-arm; see [`FlushWaitOutcome`]).
///
/// Caller contract: inside the commit critical section, called ONLY from
/// [`crate::write::XLogFlushPipelined`]'s contended arm after
/// [`pipeline_available`] returned true; a flush is normally in flight
/// (the conditional WALWriteLock acquire just failed). Non-cancellable by
/// design — the same interrupt posture as the WALWriteLock convoy wait it
/// replaces (a commit whose record is inserted cannot be abandoned).
pub(crate) fn wait_for_flush(lsn: XLogRecPtr) -> FlushWaitOutcome {
    let ctl = XLogCtl();
    let node = FlushWaitNode {
        lsn,
        procno: init_small::globals::MyProcNumber(),
        completed: AtomicBool::new(false),
        prev: UnsafeCell::new(ptr::null_mut()),
        next: UnsafeCell::new(ptr::null_mut()),
        linked: UnsafeCell::new(false),
    };
    let node_ptr = &node as *const FlushWaitNode as *mut FlushWaitNode;

    // GL-FLUSHSIM-1 dropped-registration mutation seam (sim builds only;
    // compiles to `false` in production).
    #[cfg(pgrust_sim)]
    let link_dropped = sim::take_skip_link();
    #[cfg(not(pgrust_sim))]
    let link_dropped = false;

    if !link_dropped {
        // SAFETY: node is unlinked; lifetime invariants per module doc.
        PIPE.lck.with(|| unsafe { PIPE.link(node_ptr) });
    }
    STATS.registered.fetch_add(1, Relaxed);

    let try_self_unlink = || {
        PIPE.lck.with(|| {
            // SAFETY: under lck; unlink only if still linked. If a
            // completer already collected us this returns false and
            // `completed` is imminent.
            unsafe {
                if *node.linked.get() {
                    PIPE.unlink(node_ptr);
                    true
                } else {
                    false
                }
            }
        })
    };

    // Dekker fence (module doc): pairs with complete_up_to's post-publish
    // fence. If the covering flush published before our link became
    // visible to its walk, this read sees it and we self-serve.
    fence(SeqCst);
    if ctl.logFlushResult.load(Acquire) >= lsn {
        if try_self_unlink() {
            STATS.self_served.fetch_add(1, Relaxed);
            trace(|| format!("self-served at {:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF));
            return FlushWaitOutcome::Completed;
        }
        // A completer already collected us: `completed` is imminent; fall
        // into the park loop to consume its wake.
    } else {
        // Fast completion path: the flusher daemon covers us even when the
        // in-flight leader's batch ends below our LSN and no later commit
        // arrives (liveness itself does NOT depend on this — the backstop
        // re-arm below is self-sufficient).
        wake_walwriter();
    }
    trace(|| format!("registered at {:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF));

    let my_latch = init_small::globals::MyLatch();
    loop {
        if node.completed.load(Acquire) {
            return FlushWaitOutcome::Completed;
        }
        // Latch protocol: wait, reset, then re-check the predicate. The
        // completer stores `completed` BEFORE its SetLatch, so a set seen
        // here implies the recheck observes it; a set that lands after our
        // ResetLatch re-arms the latch for the next lap. Errors are
        // epoll-class (fatal elsewhere); treating one as a timeout keeps
        // the loop sound (the backstop lap below then adjudicates).
        let woke = latch::WaitLatch(
            my_latch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            BACKSTOP_MS,
            WAIT_EVENT_FLUSH_PIPELINE,
        );
        if let Some(l) = my_latch {
            latch::ResetLatch(l);
        }
        // GL-FLUSHSIM-1 FINDING-1: the backstop/re-arm block is gated on a
        // GENUINE timeout. A latch-set wake that finds us neither
        // completed nor covered (spurious sets: the SwitchToSharedLatch
        // startup set; a completer's SetLatch left unconsumed by a
        // loop-head completion on the PREVIOUS registration) must simply
        // re-park — escalating it re-armed registrants onto the leader
        // path at ZERO cadence under a healthy flusher, degrading directed
        // completion back to convoy leadership and poisoning the
        // retry_rearm==0 witness (sim-witnessed: retry_rearm=2 in the
        // first 4 virtual ms of a healthy trajectory).
        let timed_out = match woke {
            Ok(ev) => ev & WL_TIMEOUT != 0,
            Err(_) => true,
        };
        if node.completed.load(Acquire) {
            if timed_out {
                // The completion arrived but its directed wake never did
                // (lost/misdirected/late) — the soft missed-wake tripwire
                // (GL-FLUSHSIM-1 §5.2).
                STATS.completed_after_timeout.fetch_add(1, Relaxed);
                trace(|| "completed observed on a TIMEOUT lap (lost wake?)".into());
            }
            return FlushWaitOutcome::Completed;
        }
        if !timed_out {
            continue; // spurious latch wake: re-park, no escalation
        }
        // Timeout backstop lap. Covered-but-unwoken = the missed-wake
        // tripwire (diagnostics; the fence pair makes it unreachable).
        // NOT covered = the LIVENESS re-arm: unlike C's woken follower we
        // have no lock queue re-entry, and the flusher we were counting on
        // may be gone (walwriter exit at shutdown; crash-reinit windows) —
        // self-unlink and hand control back to the caller's loop, which
        // contends for WALWriteLock itself (uncovered-follower-becomes-
        // leader, C's own self-sufficiency, at backstop cadence).
        if ctl.logFlushResult.load(SeqCst) >= lsn {
            if try_self_unlink() {
                STATS.backstop_self_served.fetch_add(1, Relaxed);
                trace(|| "BACKSTOP self-serve (missed-wake tripwire)".into());
                return FlushWaitOutcome::Completed;
            }
            // Collected: completed is imminent; next lap consumes it.
        } else if try_self_unlink() {
            STATS.retry_rearm.fetch_add(1, Relaxed);
            trace(|| "backstop re-arm: retaking the leader path".into());
            return FlushWaitOutcome::Retry;
        }
        // else: a completer collected us between the reads; next lap
        // consumes its wake.
    }
}

// ---------------------------------------------------------------------------
// inc-5(d) d1 — DEFERRED registration (the DeferredTail queue half;
// flushpipe inc-1.5 §5, GL-FLUSHPIPE-1 §8.2 APPROVED-WITH-GATES; co-owned
// with GL-STMTTASK-2 §14). A registrant that must NOT park (in the archive:
// the d2 worker commit prefix and R3 commit cycles — neither ported)
// registers a HEAP ticket and returns; the flush walk directed-wakes the
// ticket's TARGET (the tail host's latch); the tail host consumes the
// ticket exactly once and runs the tail. Substrate-first per §8.2: the API
// is pub with no in-tree production caller until a tail-host consumer
// lands.
//
// OWNERSHIP INVARIANTS (G-d1-3 — the stack-node table does NOT apply here;
// this is the heap ticket's own):
//   T1. A ticket is Arc-shared three ways at most: the registry entry, the
//       registrant's handle (becomes the tail host's), and a claimant's
//       transient walk copy. Memory frees when the last Arc drops — no
//       consume-side free races (consume marks, Arc frees).
//   T2. REGISTRY REMOVAL IS THE CLAIM: exactly one party removes a given
//       ticket from the registry (the SpinLock arbitrates), and ONLY the
//       remover performs the PENDING->COMPLETED transition. The waiter's
//       COMPLETED->CONSUMED transition has exactly one candidate thread
//       (the single tail host) — both transitions are RMW swaps with
//       loud teeth on the swapped-out value (G-d1-2: never a bare store).
//   T3. The completer's last touch is the COMPLETED swap + SetLatch —
//       through its own Arc, so ticket lifetime never depends on the
//       waiter's timing (the stack-node "last touch" argument is not
//       needed and not used).
//   T4. The Dekker pair is the SAME as the stack path's: registration
//       links under the registry lock, fences, then rechecks
//       logFlushResult (self-claim if covered); complete_up_to emits one
//       post-publish fence for both halves.
// LIVENESS (G-d1-1): registration requires pipeline_available() (walwriter
// alive; caller refuses the deferred path otherwise) and kicks the
// walwriter; the WAITER runs the same backstop cadence as wait_for_flush —
// covered-at-timeout self-claims (missed-wake tripwire), uncovered-at-
// timeout unlinks and returns Retry (uncovered-follower-becomes-leader:
// the caller contends for WALWriteLock itself and re-registers or
// completes inline).
// Loom mirrors (KEEP IN SYNC): waiter/tests/loom.rs "deferred-ticket"
// section — 4 models (register-vs-flush; consume-exactly-once vs
// completer-mark; retry re-arm vs completer claim; completer-walk vs
// waiter-backstop self-claim race).
// ---------------------------------------------------------------------------

/// Ticket states (RMW-only transitions; see T2).
const TICKET_PENDING: u8 = 0;
const TICKET_COMPLETED: u8 = 1;
const TICKET_CONSUMED: u8 = 2;

struct TicketInner {
    lsn: XLogRecPtr,
    /// The TAIL HOST's proc latch (registered by the deferring party on
    /// its behalf).
    wake_procno: ProcNumber,
    state: AtomicU8,
}

/// The deferred-registration handle: returned to the registrant, carried
/// to the tail host on the engagement's return path (transport-agnostic).
pub struct DeferredFlushTicket {
    inner: Arc<TicketInner>,
}

impl DeferredFlushTicket {
    /// The registered commit LSN (the reified {commit_lsn, ticket} pair's
    /// other half rides beside it on the return path).
    pub fn lsn(&self) -> XLogRecPtr {
        self.inner.lsn
    }
}

/// The deferred registry: SpinLock'd Vec (unordered; ticket volume =
/// in-flight deferred commits) + the same lock-free hint pair as `Pipe`.
struct DeferredRegistry {
    lck: SpinLock,
    tickets: UnsafeCell<Vec<Arc<TicketInner>>>,
    min_pending: AtomicU64,
    max_pending: AtomicU64,
}

// SAFETY: `tickets` is only touched under `lck` (T2).
unsafe impl Sync for DeferredRegistry {}

static DEFERRED: DeferredRegistry = DeferredRegistry {
    lck: SpinLock::new(),
    tickets: UnsafeCell::new(Vec::new()),
    min_pending: AtomicU64::new(u64::MAX),
    max_pending: AtomicU64::new(0),
};

impl DeferredRegistry {
    /// SAFETY: caller holds `lck`.
    unsafe fn refresh_hints(&self) {
        let t = &*self.tickets.get();
        let min = t.iter().map(|x| x.lsn).min().unwrap_or(u64::MAX);
        let max = t.iter().map(|x| x.lsn).max().unwrap_or(0);
        self.min_pending.store(min, Relaxed);
        self.max_pending.store(max, Relaxed);
    }

    /// Remove-if-present (THE CLAIM, T2). True = the caller now owns the
    /// PENDING->COMPLETED transition.
    fn claim(&self, ticket: &Arc<TicketInner>) -> bool {
        self.lck.with(|| unsafe {
            let t = &mut *self.tickets.get();
            match t.iter().position(|x| Arc::ptr_eq(x, ticket)) {
                Some(i) => {
                    t.swap_remove(i);
                    self.refresh_hints();
                    true
                }
                None => false,
            }
        })
    }
}

/// Register a deferred flush wait: heap ticket, registrant RETURNS.
/// `None` = the deferred path is refused (knob off, walwriter dead, no
/// latch) — the caller keeps the incumbent synchronous commit path
/// (fail-toward-incumbent; G-d1-1 admission).
pub fn register_deferred(
    lsn: XLogRecPtr,
    wake_procno: ProcNumber,
) -> Option<DeferredFlushTicket> {
    if !pipeline_available() {
        return None;
    }
    let ctl = XLogCtl();
    let ticket =
        Arc::new(TicketInner { lsn, wake_procno, state: AtomicU8::new(TICKET_PENDING) });
    DEFERRED.lck.with(|| unsafe {
        let t = &mut *DEFERRED.tickets.get();
        t.push(Arc::clone(&ticket));
        DEFERRED.refresh_hints();
    });
    STATS.ticket_registered.fetch_add(1, Relaxed);
    // Dekker fence (T4): pairs with complete_up_to's post-publish fence.
    fence(SeqCst);
    if ctl.logFlushResult.load(Acquire) >= lsn {
        if DEFERRED.claim(&ticket) {
            // Born-covered: claim + complete ourselves; the tail host
            // consumes on first look, no wake needed.
            let prev = ticket.state.swap(TICKET_COMPLETED, AcqRel);
            debug_assert_eq!(prev, TICKET_PENDING, "claimed ticket must be PENDING (T2)");
            STATS.ticket_self_served.fetch_add(1, Relaxed);
            trace(|| format!("ticket self-served at {:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF));
        }
        // else: a completer claimed it between the fence and here — its
        // COMPLETED store + wake are imminent.
    } else {
        wake_walwriter();
    }
    trace(|| format!("ticket registered at {:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF));
    Some(DeferredFlushTicket { inner: ticket })
}

/// The deferred half of the flush walk. Caller: `complete_up_to` (fence
/// already emitted there).
fn complete_deferred_up_to(flushed: XLogRecPtr) {
    if DEFERRED.min_pending.load(Relaxed) > flushed {
        return;
    }
    let claimed: Vec<Arc<TicketInner>> = DEFERRED.lck.with(|| unsafe {
        let t = &mut *DEFERRED.tickets.get();
        let mut out = Vec::new();
        let mut i = 0;
        while i < t.len() {
            if t[i].lsn <= flushed {
                out.push(t.swap_remove(i));
            } else {
                i += 1;
            }
        }
        DEFERRED.refresh_hints();
        out
    });
    if claimed.is_empty() {
        return;
    }
    STATS.ticket_completed.fetch_add(claimed.len() as u64, Relaxed);
    trace(|| format!("completed {} ticket(s)", claimed.len()));
    for t in claimed {
        // T2/T3: we removed it, we own the transition; publication then
        // directed wake, through our own Arc.
        let prev = t.state.swap(TICKET_COMPLETED, AcqRel);
        debug_assert_eq!(prev, TICKET_PENDING, "claimed ticket must be PENDING (T2)");
        latch::SetLatch(LatchHandle::proc(t.wake_procno));
    }
}

/// The TAIL HOST's wait: park on MY latch until the ticket completes,
/// then CONSUME it (exactly-once; the caller runs the post-flush tail).
/// [`FlushWaitOutcome::Retry`] = the G-d1-1 liveness re-arm — the ticket
/// is unlinked and dead; the caller must contend for WALWriteLock itself
/// (flush to `lsn()` inline) and proceed WITHOUT re-registering.
/// Non-cancellable by design (the same posture as `wait_for_flush`: a
/// commit whose record is inserted cannot be abandoned — the (d)
/// point-of-no-return law).
pub fn ticket_wait(ticket: &DeferredFlushTicket) -> FlushWaitOutcome {
    let ctl = XLogCtl();
    let inner = &ticket.inner;
    let my_latch = init_small::globals::MyLatch();
    loop {
        match inner.state.load(Acquire) {
            TICKET_COMPLETED => {
                let prev = inner.state.swap(TICKET_CONSUMED, AcqRel);
                debug_assert_eq!(prev, TICKET_COMPLETED, "single consumer (T2)");
                return FlushWaitOutcome::Completed;
            }
            TICKET_CONSUMED => unreachable!("deferred ticket consumed twice"),
            _ => {}
        }
        let woke = latch::WaitLatch(
            my_latch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            BACKSTOP_MS,
            WAIT_EVENT_FLUSH_PIPELINE,
        );
        if let Some(l) = my_latch {
            latch::ResetLatch(l);
        }
        let timed_out = match woke {
            Ok(ev) => ev & WL_TIMEOUT != 0,
            Err(_) => true,
        };
        if !timed_out {
            continue; // spurious latch wake (or completion: loop head consumes)
        }
        if inner.state.load(Acquire) == TICKET_COMPLETED {
            continue; // completed during the lap; loop head consumes
        }
        // Timeout backstop lap (mirrors wait_for_flush's; G-d1-1).
        if ctl.logFlushResult.load(SeqCst) >= inner.lsn {
            if DEFERRED.claim(inner) {
                let prev = inner.state.swap(TICKET_COMPLETED, AcqRel);
                debug_assert_eq!(prev, TICKET_PENDING, "claimed ticket must be PENDING (T2)");
                STATS.ticket_self_served.fetch_add(1, Relaxed);
                trace(|| "ticket BACKSTOP self-serve (missed-wake tripwire)".into());
            }
            // Claimed by a completer or by us: loop head consumes.
            continue;
        }
        if DEFERRED.claim(inner) {
            STATS.ticket_retry_rearm.fetch_add(1, Relaxed);
            trace(|| "ticket backstop re-arm: retaking the leader path".into());
            return FlushWaitOutcome::Retry;
        }
        // A completer claimed between the reads; loop head consumes.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The queue core under its lock, exercised directly (the full
    // wait_for_flush path needs XLogCtl + latches and is covered by the
    // write.rs integration tests + the e2e; the loom mirror in
    // waiter/tests/loom.rs owns the fence-protocol interleavings).

    fn mknode(lsn: u64, procno: ProcNumber) -> Box<FlushWaitNode> {
        Box::new(FlushWaitNode {
            lsn,
            procno,
            completed: AtomicBool::new(false),
            prev: UnsafeCell::new(ptr::null_mut()),
            next: UnsafeCell::new(ptr::null_mut()),
            linked: UnsafeCell::new(false),
        })
    }

    fn collect_covered(flushed: u64) -> Vec<u64> {
        let mut out = Vec::new();
        PIPE.lck.with(|| unsafe {
            let l = &*PIPE.list.get();
            let mut at = l.head;
            while !at.is_null() && (*at).lsn <= flushed {
                let next = *(*at).next.get();
                PIPE.unlink(at);
                out.push((*at).lsn);
                at = next;
            }
        });
        out
    }

    fn drain_all() {
        collect_covered(u64::MAX);
        assert_eq!(PIPE.min_pending.load(Relaxed), u64::MAX);
        assert_eq!(PIPE.max_pending.load(Relaxed), 0);
    }

    #[test]
    fn ordered_link_and_covered_walk() {
        // Serialize against other tests in this mod via a local mutex.
        static GATE: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _g = GATE.lock().unwrap();

        drain_all();
        let (a, b, c, d) = (mknode(300, 3), mknode(100, 1), mknode(200, 2), mknode(150, 4));
        let (pa, pb, pc, pd) = (
            &*a as *const _ as *mut FlushWaitNode,
            &*b as *const _ as *mut FlushWaitNode,
            &*c as *const _ as *mut FlushWaitNode,
            &*d as *const _ as *mut FlushWaitNode,
        );
        PIPE.lck.with(|| unsafe {
            PIPE.link(pa); // 300
            PIPE.link(pb); // 100 -> head
            PIPE.link(pc); // 200 -> middle
            PIPE.link(pd); // 150 -> between 100 and 200
        });
        assert_eq!(PIPE.min_pending.load(Relaxed), 100);
        assert_eq!(PIPE.max_pending.load(Relaxed), 300);

        // Nothing covered below the head.
        assert!(collect_covered(50).is_empty());
        // LSN-ordered prefix pops.
        assert_eq!(collect_covered(200), vec![100, 150, 200]);
        assert_eq!(PIPE.min_pending.load(Relaxed), 300);
        assert_eq!(PIPE.max_pending.load(Relaxed), 300);
        assert_eq!(collect_covered(1000), vec![300]);
        assert_eq!(PIPE.min_pending.load(Relaxed), u64::MAX);
        assert_eq!(PIPE.max_pending.load(Relaxed), 0);

        // Self-unlink of a middle node keeps ends + hints coherent.
        PIPE.lck.with(|| unsafe {
            PIPE.link(pb);
            PIPE.link(pd);
            PIPE.link(pc);
        });
        PIPE.lck.with(|| unsafe {
            assert!(*(*pd).linked.get());
            PIPE.unlink(pd);
        });
        assert_eq!(PIPE.min_pending.load(Relaxed), 100);
        assert_eq!(PIPE.max_pending.load(Relaxed), 200);
        assert_eq!(collect_covered(u64::MAX), vec![100, 200]);
    }

    // ---- inc-5(d) d1 ticket pins (registry mechanics + state machine;
    // the interleavings are the 4 loom mirrors in waiter/tests/loom.rs).

    fn mkticket(lsn: u64) -> Arc<TicketInner> {
        Arc::new(TicketInner {
            lsn,
            wake_procno: INVALID_PROC_NUMBER,
            state: AtomicU8::new(TICKET_PENDING),
        })
    }

    fn deferred_len() -> usize {
        DEFERRED.lck.with(|| unsafe { (*DEFERRED.tickets.get()).len() })
    }

    #[test]
    fn ticket_claim_is_exactly_once_and_hints_track() {
        static GATE: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _g = GATE.lock().unwrap();
        assert_eq!(deferred_len(), 0);

        let (a, b) = (mkticket(100), mkticket(300));
        DEFERRED.lck.with(|| unsafe {
            let t = &mut *DEFERRED.tickets.get();
            t.push(Arc::clone(&a));
            t.push(Arc::clone(&b));
            DEFERRED.refresh_hints();
        });
        assert_eq!(DEFERRED.min_pending.load(Relaxed), 100);
        assert_eq!(DEFERRED.max_pending.load(Relaxed), 300);

        // T2: removal is the claim; the second claimant loses.
        assert!(DEFERRED.claim(&a));
        assert!(!DEFERRED.claim(&a));
        assert_eq!(DEFERRED.min_pending.load(Relaxed), 300);

        // The claimant owns PENDING->COMPLETED; the single consumer owns
        // COMPLETED->CONSUMED — swapped-out values are the teeth.
        assert_eq!(a.state.swap(TICKET_COMPLETED, AcqRel), TICKET_PENDING);
        assert_eq!(a.state.swap(TICKET_CONSUMED, AcqRel), TICKET_COMPLETED);

        assert!(DEFERRED.claim(&b));
        assert_eq!(DEFERRED.min_pending.load(Relaxed), u64::MAX);
        assert_eq!(DEFERRED.max_pending.load(Relaxed), 0);
        assert_eq!(b.state.load(Acquire), TICKET_PENDING);
    }

    #[test]
    fn ticket_register_refuses_without_pipeline() {
        // G-d1-1(iii) fail-toward-incumbent at admission. FLIP-era posture
        // (archive t49 compose 4d4836d90f): unset now means ARMED (the
        // ruled inversion), and the armed path consults ProcGlobal —
        // uninitializable in a bare unit process — so the refusal
        // assertion runs only when the control arm is SPELLED (=0|off),
        // per the flip's own off-spelling law. (The pre-flip test also
        // only exercised the knob gate: unset returned at gate 1, never
        // reaching ProcGlobal — coverage is unchanged, just re-spelled.)
        // The refusal stays covered in-server by the knob-off e2e legs.
        if matches!(
            std::env::var("PGRUST_FLUSH_PIPELINE").ok().as_deref().map(str::trim),
            Some("0") | Some("off")
        ) {
            assert!(register_deferred(42, INVALID_PROC_NUMBER).is_none());
        }
    }

    #[test]
    fn knob_default_on() {
        // Michael-ruled flip: env unset => armed (archive 18e95a10b6's §7
        // inversion, CONFIRMED). No env manipulation (memoized reads race
        // across tests), and no PIPE probes: with the knob armed they'd
        // contend on the shared static pipe's spinlock with the walk tests
        // (the pre-flip test used them as knob-off no-op pins; that
        // posture is gone by design).
        if std::env::var("PGRUST_FLUSH_PIPELINE").is_err() {
            assert!(flush_pipeline_enabled());
        }
    }
}
