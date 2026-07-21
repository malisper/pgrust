//! M2 POOL BINDING — standing runtime executors (parallelism redesign
//! §2.3; notes/m2-pool-binding.md).
//!
//! The M1 runtime-scan arm launches wpool helpers PER ENGAGEMENT and pays
//! full worker launch + a double bind (parallel_worker_body's init for a
//! trivial entry task, then the query-task binder's re-bind at
//! POST_TASK_PARK) — the ~8ms fixed cost M1-a attributed. This module makes
//! the helpers STANDING: a process-lifetime gang of executor threads with
//! bgworker-shaped identity (PGPROC from a boot-reserved segment — see
//! `postinit::InitializeMaxBackends` — so the gang never consumes the
//! bgworker registry, parallel-class, or postmaster-child-slot budgets the
//! legacy arms measure against), DB-PINNED on first use (tech-debt TD-1:
//! bind/unbind is cheap WITHIN a database; cross-db engagements refuse
//! fail-closed and the leader falls back to the launched path).
//!
//! Per engagement a standing worker pays exactly: one condvar wake, one
//! lock-group join, ONE query-task-binder bind (GUC transfer = the query
//! pin's single Arc apply, which also adopts the leader's CURRENT base —
//! so a boot-captured gang GUC base can never leak stale reload state into
//! a query), the driver's executor build + pinned drive, one unbind, one
//! lock-group leave. No thread launch, no entry task, no double bind, no
//! worker-exit join.
//!
//! Parking discipline (wpool precedent, launch_backend wpool docs): workers
//! park BETWEEN engagements on a plain process-local Condvar — never on
//! shared-memory latches — so crash reinit can invalidate them with an
//! epoch bump without woken threads touching reset shared memory
//! (flush_for_crash discipline). DROP DATABASE rides the same
//! `parallel_pool_retire_db` seam wpool uses: gang workers pinned to the
//! dropped database exit (ProcKill returns the PGPROC, RemoveProcFromArray
//! clears the procarray entry CountOtherDBBackends polls).
//!
//! Thread identity is the launch_backend spawn glue's (rtgang): postmaster
//! prelude + InitPostmasterChild + InitProcess(BgWorker, boot-reserved
//! segment) + BaseInit + the synthetic bgworker entry, then
//! `gang_worker_loop` here; the glue also owns the run_child_task-shaped
//! ProcExitThread catch + deferred-callback drain (ProcKill) at exit.
//!
//! Kill-switch layering: the gang exists only under PGRUST_RUNTIME=1 (the
//! reserved PGPROC segment too), engages only for published engagements
//! (the M1 arm's own arming gates those), and PGRUST_RUNTIME_POOLBIND=0
//! disables this module entirely (leader falls back to the launched path).
//!
//! M2 inc-1: ALL runtime SQL arms use this channel — the per-arm driver
//! rides each engagement's ParallelShared (`StandingDriver`,
//! `set_standing_driver`) instead of a process-global slot; the sink arms
//! carry their own layered kill (PGRUST_RUNTIME_POOLBIND_SINKS=0, gated at
//! their call sites in execmain's standing_channel).

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;

use pgsync::{Condvar, Mutex, OnceLock};

use types_core::{InvalidOid, Oid};
use types_error::WARNING;

use super::ParallelShared;

/// One published engagement: `tickets` participation slots over one
/// ParallelShared. Workers claim tickets, run the registered driver bound,
/// and detach; the leader closes the board entry and waits for
/// detached == claimed before its executor arena may unwind (the SendConst
/// contract's join, replacing DestroyParallelContext's worker-exit wait).
pub struct StandingEngagement {
    shared: Arc<ParallelShared>,
    tickets: usize,
    claimed: AtomicUsize,
    detached: AtomicUsize,
    /// Pre-driver refusals (db mismatch, leader already gone, connect
    /// failure): the worker never reached the arm's payload accounting, so
    /// the board carries them for the leader's nobody-participates check.
    refused: AtomicUsize,
    closed: AtomicBool,
}

impl StandingEngagement {
    pub fn claimed(&self) -> usize {
        self.claimed.load(SeqCst)
    }
    pub fn detached(&self) -> usize {
        self.detached.load(SeqCst)
    }
    pub fn refused(&self) -> usize {
        self.refused.load(SeqCst)
    }
    pub fn tickets(&self) -> usize {
        self.tickets
    }

    fn try_claim(&self) -> Option<usize> {
        if self.closed.load(SeqCst) {
            return None;
        }
        // Over-claims (fetch_add races, bounded by gang size) are returned.
        let t = self.claimed.fetch_add(1, SeqCst);
        if t < self.tickets && !self.closed.load(SeqCst) {
            Some(t)
        } else {
            self.claimed.fetch_sub(1, SeqCst);
            // The leader's Condvar join (close_and_await) may have observed
            // the transient inflated `claimed` at its under-lock check and
            // parked on it; unlike the poll it replaced, a cv join is not
            // lost-wake-tolerant, so the settling decrement carries the same
            // lock-mediated wake as a detach. (Never runs under the gang
            // lock: try_claim is called after the wake scope releases it.)
            let (lock, cv) = gang();
            drop(pgsync::lock(lock));
            cv.notify_all();
            None
        }
    }
}

/// Everything a worker does after claiming a ticket happens under this
/// guard: detach is UNCONDITIONAL (error, panic, even a FATAL's
/// ProcExitThread unwind through the gang frame), so the leader's
/// detached==claimed join can never wedge on a dying worker. (abort() is
/// the only escape, and it takes the whole process.)
struct DetachGuard<'a> {
    entry: &'a StandingEngagement,
}

impl Drop for DetachGuard<'_> {
    fn drop(&mut self) {
        // The modeled detach wake (loom spec: runtime/tests/loom.rs
        // `standing_gang_detach_count_join_no_lost_wake`): bump `detached`
        // FIRST, then a lock-mediated notify — either the leader sees the
        // new count at its under-lock check in close_and_await, or it is
        // already parked when the notify fires. The leader latch set stays:
        // the lanev2 leader poll loops (runtime_scan et al.) park on the
        // leader latch between their own completion re-polls.
        self.entry.detached.fetch_add(1, SeqCst);
        latch::SetLatch(types_storage::latch::LatchHandle::proc(
            self.entry.shared.parallel_leader_proc_number,
        ));
        let (lock, cv) = gang();
        drop(pgsync::lock(lock));
        cv.notify_all();
    }
}

#[derive(Clone, Copy, PartialEq)]
enum SlotState {
    /// Never spawned, or exited (retired/died); try_engage may respawn.
    Vacant,
    /// Thread launched; identity/db init happens on the thread.
    Live,
}

struct GangState {
    slots: Vec<SlotState>,
    current: Option<Arc<StandingEngagement>>,
    /// Crash-reinit fence: bumped (with a wake) before shared memory is
    /// reset; woken workers whose captured epoch mismatches exit RAW —
    /// no shared-memory touch (wpool flush_for_crash discipline).
    epoch: u64,
    /// DROP DATABASE rider: databases whose pinned workers must exit.
    /// A SET, never auto-cleared by workers — a one-shot flag could be
    /// consumed by the first matching worker while a second parked one
    /// misses it (wedging CountOtherDBBackends). Bounded by DROPs per
    /// process lifetime; try_engage prunes an entry when a leader engages
    /// from that database again (the oid exists again).
    retired_dbs: Vec<Oid>,
    retire_all: bool,
}

static GANG: OnceLock<(Mutex<GangState>, Condvar)> = OnceLock::new();
static SPAWNER: OnceLock<fn(usize) -> bool> = OnceLock::new();
static GANG_SIZE: OnceLock<usize> = OnceLock::new();

fn gang() -> &'static (Mutex<GangState>, Condvar) {
    GANG.get_or_init(|| {
        (
            Mutex::new(GangState {
                slots: Vec::new(),
                current: None,
                epoch: 0,
                retired_dbs: Vec::new(),
                retire_all: false,
            }),
            Condvar::new(),
        )
    })
}

/// PGRUST_RUNTIME_POOLBIND=0 kills this module (launched-path fallback).
pub fn pool_binding_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_POOLBIND").map_or(true, |v| v.trim() != "0")
    })
}

/// Boot wiring (launch_backend rtgang): the thread spawner and the gang
/// size (= the boot-reserved PGPROC count). Once; later calls ignored.
pub fn install_spawner(size: usize, f: fn(usize) -> bool) {
    let _ = SPAWNER.set(f);
    let _ = GANG_SIZE.set(size);
}

/// One arm's engagement driver, carried per engagement on its
/// ParallelShared (`super::set_standing_driver`) — the per-arm dispatch
/// that replaced the single process-global driver slot when the sink arms
/// joined the standing channel (M2 inc-1). `drive` runs ON the standing
/// worker, fully impersonated (worker number + lock group), and owns the
/// binder wrap + executor build + pinned drive + payload error routing.
#[derive(Clone, Copy)]
pub struct StandingDriver {
    pub drive: fn(&ParallelShared),
    /// True when the driver binds through the DEFERRED binder
    /// (DeferredQueryTaskBinding — the scan arm's ceremony-v2 lazy
    /// first-touch path): serve_ticket may then defer the procarray/
    /// ProcSignal visibility bracket to the first-touch bind. False = the
    /// driver binds EAGERLY (with_query_task_binding — the sink arms):
    /// visibility is re-established up front and any parked sticky
    /// retention is evicted first (the eager validate()'s envelope gate
    /// refuses over a live retained session bind).
    pub deferred_bind: bool,
}

pub fn gang_size() -> usize {
    GANG_SIZE.get().copied().unwrap_or(0)
}

/// Leader side: publish an engagement for `dop` standing participants.
/// None = the standing path is unavailable (kill switch, no boot wiring,
/// board busy, nothing spawnable) — the caller falls back to the launched
/// path. The returned entry is LIVE: workers may already be claiming.
///
/// The caller must already be a lock-group leader (BecomeLockGroupLeader)
/// and must call `close_and_await` on the returned entry before its
/// executor arena unwinds, on every path.
pub fn try_engage(shared: &Arc<ParallelShared>, dop: usize) -> Option<Arc<StandingEngagement>> {
    if !pool_binding_enabled() || dop == 0 {
        return None;
    }
    let spawner = SPAWNER.get()?;
    // Per-arm dispatch (M2 inc-1): the engagement must carry its driver.
    shared.standing_driver()?;
    let size = gang_size();
    if size == 0 {
        return None;
    }
    // Workers join the leader's lock group the moment they claim a ticket:
    // the leader must already be a group leader (idempotent; the launched
    // path's LaunchParallelWorkers does the same).
    if lmgr_proc::BecomeLockGroupLeader().is_err() {
        return None;
    }
    let (lock, cv) = gang();
    let mut g = lock.lock().unwrap_or_else(|p| p.into_inner());
    if g.current.is_some() {
        // One engagement at a time (single-query M2 scope): a busy board
        // falls back to the launched path, never queues.
        return None;
    }
    if g.slots.is_empty() {
        g.slots = vec![SlotState::Vacant; size];
    }
    // Crash-fence recovery: a leader engaging proves reinit completed
    // (backends only run against live shared memory). Pre-crash threads
    // stay fenced by the epoch bump; without this clear, every respawned
    // worker would raw-exit on the stale retire_all and leak its freshly
    // claimed PGPROC against LIVE shmem, draining the bgworker freelist.
    g.retire_all = false;
    // The engaging leader IS connected to this database — it exists again;
    // any retire entry for its oid is stale (recreated oid). Prune so
    // freshly-pinned workers don't spuriously exit at their next wake.
    g.retired_dbs.retain(|d| *d != shared.database_id);
    // Respawn vacant slots (first engagement, post-retire, post-death).
    for (i, s) in g.slots.iter_mut().enumerate() {
        if *s == SlotState::Vacant && spawner(i) {
            *s = SlotState::Live;
        }
    }
    if !g.slots.iter().any(|s| *s == SlotState::Live) {
        return None;
    }
    let entry = Arc::new(StandingEngagement {
        shared: Arc::clone(shared),
        tickets: dop,
        claimed: AtomicUsize::new(0),
        detached: AtomicUsize::new(0),
        refused: AtomicUsize::new(0),
        closed: AtomicBool::new(false),
    });
    g.current = Some(Arc::clone(&entry));
    cv.notify_all();
    Some(entry)
}

/// Leader side: close the board entry (no new claims) and wait until every
/// claimed participant detached. Interrupt-opaque by design: detach is
/// Drop-guaranteed on the workers, so this wait is bounded by one drive
/// teardown; the caller handles query-level errors/cancel BEFORE calling
/// (abort the RG first — drives observe it at the next morsel boundary).
pub fn close_and_await(entry: &Arc<StandingEngagement>) {
    entry.closed.store(true, SeqCst);
    let (lock, cv) = gang();
    let mut g = pgsync::lock(lock);
    if let Some(cur) = &g.current {
        if Arc::ptr_eq(cur, entry) {
            g.current = None;
        }
    }
    // Wake ticketless workers parked on the board state.
    cv.notify_all();
    // Post-close claim race: try_claim rechecks `closed` after its
    // fetch_add and returns over-claims, so `claimed` is stable once
    // closed is visible and every claimer either detaches or never held
    // a ticket; an over-claim's settling decrement carries its own wake
    // (see try_claim).
    //
    // The detach-count Condvar join (permit step-3 conversion, census §3
    // row 1): replaces the wait_parallel_finish_quantum latch-poll with the
    // lost-wake-FREE shape the loom model
    // `standing_gang_detach_count_join_no_lost_wake` pins — the condition is
    // re-checked under the gang lock, and every counter movement notifies
    // through the same lock, so the wait can never park past the last
    // detach. Interrupt-opacity is unchanged (the poll discarded cancel
    // dispositions here too; the wait stays bounded by one drive teardown
    // because detach is Drop-guaranteed on the workers).
    while entry.detached.load(SeqCst) < entry.claimed.load(SeqCst) {
        g = cv.wait(g).unwrap_or_else(|p| p.into_inner());
    }
    drop(g);
}

/// DROP DATABASE rider (parallel_pool_retire_db seam, alongside wpool's):
/// standing workers pinned to the dropped database exit — releasing their
/// PGPROCs and procarray entries for CountOtherDBBackends.
pub fn retire_db(dboid: Oid) {
    if GANG.get().is_none() {
        return;
    }
    let (lock, cv) = gang();
    let mut g = lock.lock().unwrap_or_else(|p| p.into_inner());
    if !g.retired_dbs.contains(&dboid) {
        g.retired_dbs.push(dboid);
    }
    cv.notify_all();
}

/// Crash reinit (wpool flush_for_crash discipline): shared memory is about
/// to be reset wholesale — bump the epoch so every woken worker exits RAW,
/// touching nothing shared.
pub fn flush_for_crash() {
    if GANG.get().is_none() {
        return;
    }
    let (lock, cv) = gang();
    let mut g = lock.lock().unwrap_or_else(|p| p.into_inner());
    g.epoch += 1;
    g.retire_all = true;
    g.slots.iter_mut().for_each(|s| *s = SlotState::Vacant);
    g.current = None;
    cv.notify_all();
}

/// How a `gang_worker_loop` thread wants to exit; the spawn glue acts.
pub enum GangExit {
    /// Ordinary exit: run `ipc::proc_exit` so the deferred callbacks
    /// (ProcKill, RemoveProcFromArray, sinval cleanup) release identity
    /// against LIVE shared memory.
    Clean,
    /// Crash fence: shared memory may be mid-reset — exit the thread with
    /// NO shared-memory interaction (no callbacks).
    Raw,
}

/// True when an unwind payload is EXIT-COMMITTED (FATAL's ProcExitThread /
/// PanicExitThread): drivers and containment layers must rethrow these —
/// the thread is dying and its proc_exit callback chain owns cleanup.
pub fn is_exit_unwind(payload: &(dyn std::any::Any + Send)) -> bool {
    payload.is::<ipc::ProcExitThread>() || payload.is::<types_error::PanicExitThread>()
}

// ---------------------------------------------------------------------------
// CEREMONY-V2 deferred visibility (lazy bind): a standing worker claiming a
// ticket no longer re-enters the procarray / retakes its ProcSignal slot up
// front — a participant that never claims work stays park-invisible
// throughout. serve_ticket ARMS the deferral before the driver; the deferred
// binder's first-touch bind CONSUMES it (procarray re-add BEFORE any
// snapshot state exists — xmin only counts while visible — plus the
// no-callback ProcSignal re-init); serve_ticket's tail removes both iff the
// deferral ENGAGED. The first-connect path stays eager (InitPostgres adds
// visibility as a side effect of connecting).
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum DeferredVis {
    Off,
    Armed,
    Engaged,
}

thread_local! {
    static DEFERRED_VIS: std::cell::Cell<DeferredVis> =
        const { std::cell::Cell::new(DeferredVis::Off) };
}

fn arm_deferred_visibility() {
    DEFERRED_VIS.with(|c| c.set(DeferredVis::Armed));
}

/// Tail read: did the first-touch consume the arming? Resets to Off.
fn take_deferred_visibility_engaged() -> bool {
    DEFERRED_VIS.with(|c| {
        let engaged = c.get() == DeferredVis::Engaged;
        c.set(DeferredVis::Off);
        engaged
    })
}

/// Called by the deferred binder at first touch (query_task_guard::
/// DeferredQueryTaskBinding::bind_now). No-op unless a standing serve armed
/// the deferral on this thread.
pub(crate) fn engage_deferred_visibility() -> types_error::PgResult<()> {
    if DEFERRED_VIS.with(|c| c.get()) != DeferredVis::Armed {
        return Ok(());
    }
    super::gtrace("w.vis.begin");
    procarray_seams::proc_array_add::call(init_small::globals::MyProcNumber())?;
    if let Err(e) = procsignal::ProcSignalReinitStanding(&[]) {
        let _ = elog::elog(
            WARNING,
            format!("standing executor ProcSignal re-init failed: {}", e.message()),
        );
    }
    super::gtrace("w.vis.end");
    DEFERRED_VIS.with(|c| c.set(DeferredVis::Engaged));
    Ok(())
}

/// Glue: mark a slot respawnable (worker exit / init failure).
pub fn note_worker_exit(ordinal: usize) {
    if GANG.get().is_none() {
        return;
    }
    let (lock, _) = gang();
    let mut g = lock.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(s) = g.slots.get_mut(ordinal) {
        *s = SlotState::Vacant;
    }
}

/// Worker loop, called by the launch_backend spawn glue on a thread that
/// already owns full bgworker-shaped identity (see module doc). Parks on
/// the gang condvar between engagements; returns only to exit.
pub fn gang_worker_loop(_ordinal: usize) -> GangExit {
    let my_epoch = {
        let (lock, _) = gang();
        lock.lock().unwrap_or_else(|p| p.into_inner()).epoch
    };

    loop {
        enum Wake {
            Engage(Arc<StandingEngagement>),
            Blocked(Arc<StandingEngagement>),
            RetireRaw,
            Retire,
        }
        let wake = {
            let (lock, cv) = gang();
            let mut g = lock.lock().unwrap_or_else(|p| p.into_inner());
            loop {
                if g.epoch != my_epoch || g.retire_all {
                    break Wake::RetireRaw;
                }
                {
                    let mine = init_small::globals::MyDatabaseId();
                    if mine != InvalidOid && g.retired_dbs.contains(&mine) {
                        break Wake::Retire;
                    }
                }
                match g.current.as_ref() {
                    Some(entry) => {
                        let db = entry.shared.database_id;
                        let mine = init_small::globals::MyDatabaseId();
                        // DB-pinning (TD-1): unconnected workers adopt the
                        // engagement's database; connected ones only serve
                        // their own. Mismatch = parked non-participation.
                        if mine == InvalidOid || mine == db {
                            break Wake::Engage(Arc::clone(entry));
                        }
                        break Wake::Blocked(Arc::clone(entry));
                    }
                    None => {}
                }
                g = cv.wait(g).unwrap_or_else(|p| p.into_inner());
            }
        };
        match wake {
            Wake::RetireRaw => {
                // Sticky retention (ceremony-v2) is heap-only and its guard
                // is disarmed while parked: a plain drop, no shared-memory
                // interaction — safe under the crash fence.
                super::query_task_guard::sticky_clear();
                return GangExit::Raw;
            }
            Wake::Retire => {
                super::query_task_guard::sticky_clear();
                // Parked workers are procarray-ABSENT (serve_ticket's
                // bracket); the exit-callback chain (RemoveProcFromArray)
                // expects membership — rejoin before proc_exit.
                if init_small::globals::MyDatabaseId() != InvalidOid {
                    let _ = procarray_seams::proc_array_add::call(
                        init_small::globals::MyProcNumber(),
                    );
                }
                return GangExit::Clean;
            }
            Wake::Engage(entry) => match entry.try_claim() {
                Some(ticket) => serve_ticket(&entry, ticket),
                None => {
                    // Claim-race loser: if still unconnected, WARM UP
                    // against the engagement's database anyway — otherwise
                    // a lower-DOP engagement stream keeps hitting cold
                    // InitPostgres on whichever unconnected worker wins a
                    // later claim race (the connect cost lands INSIDE a
                    // measured query). Cold connects then all happen on
                    // the gang's first engagement, any DOP.
                    warm_connect(&entry);
                    park_until_board_changes(&entry);
                }
            },
            Wake::Blocked(entry) => park_until_board_changes(&entry),
        }
    }
}

/// Ticketless warm-up: connect an unconnected worker to the engagement's
/// database (procarray bracket included — connected-but-parked workers
/// stay invisible to CountOtherDBBackends). A FAILED connect is
/// thread-fatal (see `connect_failed_die`): InitPostgres may have claimed
/// shared identity (the sinval slot is claimed early) before failing, and
/// only this thread's exit-callback drain can release it — surviving
/// half-connected poisons the slot for the server's life. Exit-committed
/// unwinds keep unwinding to the glue.
fn warm_connect(entry: &Arc<StandingEngagement>) {
    if init_small::globals::MyDatabaseId() != InvalidOid
        || entry.shared.database_id == InvalidOid
    {
        return;
    }
    super::gtrace("g.warmconn.begin");
    let connected = catch_unwind(AssertUnwindSafe(|| {
        bgworker::BackgroundWorkerInitializeConnectionByOid(
            entry.shared.database_id,
            entry.shared.authenticated_user_id,
            bgworker::BGWORKER_BYPASS_ALLOWCONN | bgworker::BGWORKER_BYPASS_ROLELOGINCHECK,
        )
        .and_then(|()| {
            mbutils::SetClientEncoding(mbutils::GetDatabaseEncoding()).map(|_| ())
        })
    }));
    super::gtrace("g.warmconn.end");
    match connected {
        Ok(Ok(())) => {
            // Park-invisibility: InitPostgres added us to the procarray
            // and took a ProcSignal slot; release both until the next
            // claimed ticket re-adds them.
            if let Err(e) = procarray_seams::proc_array_remove::call(
                init_small::globals::MyProcNumber(),
                types_core::InvalidTransactionId,
            ) {
                let _ = elog::elog(
                    WARNING,
                    format!(
                        "standing executor warm-up procarray remove failed: {}",
                        e.message()
                    ),
                );
            }
            procsignal::ProcSignalRelease();
        }
        Ok(Err(e)) => {
            let _ = elog::elog(
                WARNING,
                format!("standing executor warm-up connect failed: {}", e.message()),
            );
            connect_failed_die();
        }
        Err(payload) => {
            if is_exit_unwind(&*payload) {
                resume_unwind(payload);
            }
            let _ = elog::elog(
                WARNING,
                "standing executor warm-up connect panicked".to_string(),
            );
            connect_failed_die();
        }
    }
}

/// A gang worker's InitPostgres failed (Err or a caught generic panic): the
/// thread may hold PARTIALLY-CLAIMED shared identity — SharedInvalBackendInit
/// claims the sinval slot early in the connect, and any later failure
/// (ProcSignalInit, the startup transaction, CheckMyDatabase, an assert)
/// returns with the slot claimed under this thread's pid. In C a bgworker
/// connect failure is FATAL and proc_exit's callback chain releases whatever
/// was claimed; the pre-fix "stay cold and retry" arm instead self-collided
/// forever ("sinval slot for backend N is already in use by process <own
/// pid>" on every retry — INBOX-standing-gang-dev-wedge item 3, poisoning
/// every later standing warm-up). Die the way C does: the glue's deferred
/// drain releases identity against live shared memory and the slot respawns
/// on the next engagement.
fn connect_failed_die() -> ! {
    ipc::proc_exit(1, init_small::globals::MyProcPid())
}

/// The board still shows an engagement we cannot serve (no ticket / wrong
/// db) — wait until it changes so the wake loop does not spin.
fn park_until_board_changes(entry: &Arc<StandingEngagement>) {
    let mine = init_small::globals::MyDatabaseId();
    let (lock, cv) = gang();
    let mut g = lock.lock().unwrap_or_else(|p| p.into_inner());
    while g
        .current
        .as_ref()
        .is_some_and(|cur| Arc::ptr_eq(cur, entry))
        && !g.retire_all
        && !(mine != InvalidOid && g.retired_dbs.contains(&mine))
    {
        g = cv.wait(g).unwrap_or_else(|p| p.into_inner());
    }
}

/// One claimed ticket: connect if first use (db-pin), impersonate a
/// parallel worker, join the leader's lock group, run the driver (which
/// owns the single binder bind + executor build + pinned drive), then
/// restore the standing state. Detach is Drop-guaranteed.
///
/// PROCARRAY VISIBILITY brackets the bound span (the wpool park
/// precedent: a PARKED worker must be invisible to CountOtherDBBackends
/// — an idle procarray entry pinned to a database blocks DROP DATABASE,
/// whose retire rider fires only in late dropdb cleanup, after the
/// count). First use: InitPostgres's InitProcessPhase2 adds; later
/// engagements re-add here. The NORMAL tail removes; unwind paths
/// deliberately leave the entry for the thread-exit callback
/// (RemoveProcFromArray) — removing under unwind would double-remove at
/// the thread top and abort the callback drain that releases the PGPROC.
fn serve_ticket(entry: &Arc<StandingEngagement>, ticket: usize) {
    let shared = &entry.shared;
    let detach = DetachGuard { entry };
    let mut in_procarray = false;

    // Per-arm dispatch (M2 inc-1): the driver rides the engagement's
    // ParallelShared. Unreachable-missing (try_engage gates on it) —
    // refuse fail-closed rather than expect().
    let Some(driver) = shared.standing_driver() else {
        entry.refused.fetch_add(1, SeqCst);
        return;
    };

    // First engagement on this worker: adopt the engagement's database
    // (exactly parallel_worker_body's connect flags).
    if init_small::globals::MyDatabaseId() == InvalidOid {
        if shared.database_id == InvalidOid {
            entry.refused.fetch_add(1, SeqCst);
            return;
        }
        super::gtrace("g.conn.begin");
        let connected = catch_unwind(AssertUnwindSafe(|| {
            bgworker::BackgroundWorkerInitializeConnectionByOid(
                shared.database_id,
                shared.authenticated_user_id,
                bgworker::BGWORKER_BYPASS_ALLOWCONN | bgworker::BGWORKER_BYPASS_ROLELOGINCHECK,
            )
            .and_then(|()| {
                mbutils::SetClientEncoding(mbutils::GetDatabaseEncoding()).map(|_| ())
            })
        }));
        super::gtrace("g.conn.end");
        match connected {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                let _ = elog::elog(
                    WARNING,
                    format!("standing executor connect failed: {}", e.message()),
                );
                // Refuse the ticket for the leader's accounting, then die:
                // the failed InitPostgres may hold a partially-claimed
                // identity (sinval slot) only an exit drain releases (see
                // connect_failed_die). The DetachGuard drops on the unwind.
                entry.refused.fetch_add(1, SeqCst);
                drop(detach);
                connect_failed_die();
            }
            Err(payload) => {
                // FATAL-shaped connect failure: refuse the ticket, detach
                // (guard), and keep the exit unwinding to the glue. A
                // generic (non-exit) panic converts to the same thread-fatal
                // exit as the Err arm — half-connected survival is the
                // sinval-slot self-poison.
                entry.refused.fetch_add(1, SeqCst);
                drop(detach);
                if is_exit_unwind(&*payload) {
                    resume_unwind(payload);
                }
                let _ = elog::elog(
                    WARNING,
                    "standing executor connect panicked".to_string(),
                );
                connect_failed_die();
            }
        }
        in_procarray = true; // InitPostgres's InitProcessPhase2 added us.
    } else {
        // EAGER-binding drivers (the sink arms) cannot bind over a parked
        // sticky retention (the eager validate()'s envelope gate refuses a
        // live retained session bind) — evict it with the full session
        // restore FIRST (the deferred binder evicts its own mismatches in
        // DeferredQueryTaskBinding::new). A failed eviction refuses
        // fail-closed, pre-claim, pre-visibility (nothing to remove at the
        // tail).
        if !driver.deferred_bind {
            if let Err(e) = super::query_task_guard::sticky_evict_parked() {
                let _ = elog::elog(
                    WARNING,
                    format!(
                        "standing executor sticky eviction failed: {}",
                        e.message()
                    ),
                );
                entry.refused.fetch_add(1, SeqCst);
                return;
            }
        }
        if driver.deferred_bind && super::query_task_guard::lazy_bind_enabled() {
            // CEREMONY-V2 lazy bind (deferred-binder drivers only — the
            // sink arms' eager binder never calls the consuming bind_now,
            // so arming it for them would bind with the worker
            // procarray-INVISIBLE): visibility (procarray + ProcSignal) is
            // deferred to the FIRST MORSEL CLAIM — the deferred binder
            // consumes this arming before its snapshot restore (xmin
            // ordering held). A participant that never claims work stays
            // park-invisible and pays neither the procarray lock nor the
            // ProcSignal slot churn.
            arm_deferred_visibility();
        } else {
            // Re-engagement on a connected worker: rejoin the procarray
            // BEFORE any snapshot state exists (the binder's snapshot
            // restore publishes xmin, which only counts while visible),
            // and retake a ProcSignal slot (released at every park — see
            // the tail).
            if let Err(e) = procarray_seams::proc_array_add::call(
                init_small::globals::MyProcNumber(),
            ) {
                let _ = elog::elog(
                    WARNING,
                    format!(
                        "standing executor procarray re-add failed: {}",
                        e.message()
                    ),
                );
                entry.refused.fetch_add(1, SeqCst);
                return;
            }
            in_procarray = true;
            // No-callback variant: the connect-time init registered the
            // exit callback once; re-registering per engagement would grow
            // the exit-callback stack unboundedly.
            if let Err(e) = procsignal::ProcSignalReinitStanding(&[]) {
                let _ = elog::elog(
                    WARNING,
                    format!(
                        "standing executor ProcSignal re-init failed: {}",
                        e.message()
                    ),
                );
            }
        }
    }
    debug_assert_eq!(init_small::globals::MyDatabaseId(), shared.database_id);

    // Parallel-worker impersonation for the binder's validate() and the
    // executor's IsParallelWorker gates; cleared on every exit path.
    super::PARALLEL_WORKER_NUMBER.with(|c| c.set(ticket as i32));
    let joined = lmgr_proc::BecomeLockGroupMember(
        shared.parallel_leader_proc_number,
        shared.parallel_leader_pid,
    );
    match joined {
        Ok(true) => {
            // The driver catches its own panics into the payload (the M1
            // hook discipline); this outer catch is containment of last
            // resort so lock-group leave + unimpersonation always run —
            // EXCEPT for exit-committed unwinds (FATAL's ProcExitThread /
            // PanicExitThread): a backend must actually die after FATAL,
            // and its proc_exit callback chain (ProcKill) performs the
            // lock-group leave; swallowing it would leave a terminated
            // worker serving future engagements. DetachGuard already
            // covers the leader's join on this path.
            let r = catch_unwind(AssertUnwindSafe(|| (driver.drive)(shared)));
            if let Err(payload) = r {
                if payload.is::<ipc::ProcExitThread>()
                    || payload.is::<types_error::PanicExitThread>()
                {
                    resume_unwind(payload);
                }
            }
            lmgr_proc::LeaveLockGroup();
        }
        Ok(false) => {
            // Leader already gone (cancel raced the publish): refuse.
            entry.refused.fetch_add(1, SeqCst);
        }
        Err(e) => {
            let _ = elog::elog(
                WARNING,
                format!("standing executor lock-group join failed: {}", e.message()),
            );
            entry.refused.fetch_add(1, SeqCst);
        }
    }
    super::PARALLEL_WORKER_NUMBER.with(|c| c.set(-1));
    // Park invisibility (see the fn doc): leave the procarray (parked
    // workers must be invisible to CountOtherDBBackends) AND release the
    // ProcSignal slot (a live slot whose owner never drains signals
    // wedges WaitForProcSignalBarrier — dropdb's SMGRRELEASE barrier).
    // Unwind paths above skip both deliberately — the thread-exit
    // callbacks handle them then. A deferred-visibility ticket only
    // removes what its first-touch actually added.
    let in_procarray = in_procarray || take_deferred_visibility_engaged();
    if in_procarray {
        if let Err(e) = procarray_seams::proc_array_remove::call(
            init_small::globals::MyProcNumber(),
            types_core::InvalidTransactionId,
        ) {
            let _ = elog::elog(
                WARNING,
                format!("standing executor procarray remove failed: {}", e.message()),
            );
        }
        procsignal::ProcSignalRelease();
    }
}
