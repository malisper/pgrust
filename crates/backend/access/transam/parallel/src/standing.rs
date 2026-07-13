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

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

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
        self.entry.detached.fetch_add(1, SeqCst);
        latch::SetLatch(types_storage::latch::LatchHandle::proc(
            self.entry.shared.parallel_leader_proc_number,
        ));
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
static DRIVER: OnceLock<fn(&ParallelShared)> = OnceLock::new();
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

/// The engagement driver (execmain's runtime arm): runs ON the standing
/// worker, fully impersonated (worker number + lock group), and owns the
/// binder wrap + executor build + pinned drive + payload error routing.
pub fn register_standing_driver(f: fn(&ParallelShared)) {
    let _ = DRIVER.set(f);
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
    DRIVER.get()?;
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
    {
        let (lock, cv) = gang();
        let mut g = lock.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(cur) = &g.current {
            if Arc::ptr_eq(cur, entry) {
                g.current = None;
            }
        }
        // Wake ticketless workers parked on the board state.
        cv.notify_all();
    }
    // Post-close claim race: try_claim rechecks `closed` after its
    // fetch_add and returns over-claims, so `claimed` is stable once
    // closed is visible and every claimer either detaches or never held
    // a ticket.
    while entry.detached.load(SeqCst) < entry.claimed.load(SeqCst) {
        super::wait_parallel_finish_quantum();
    }
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
            Wake::RetireRaw => return GangExit::Raw,
            Wake::Retire => return GangExit::Clean,
            Wake::Engage(entry) => match entry.try_claim() {
                Some(ticket) => serve_ticket(&entry, ticket),
                None => park_until_board_changes(&entry),
            },
            Wake::Blocked(entry) => park_until_board_changes(&entry),
        }
    }
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
fn serve_ticket(entry: &Arc<StandingEngagement>, ticket: usize) {
    let shared = &entry.shared;
    let detach = DetachGuard { entry };

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
                entry.refused.fetch_add(1, SeqCst);
                return;
            }
            Err(payload) => {
                // FATAL-shaped connect failure: refuse the ticket, detach
                // (guard), and keep the exit unwinding to the glue.
                entry.refused.fetch_add(1, SeqCst);
                drop(detach);
                resume_unwind(payload);
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
            let driver = DRIVER.get().expect("standing driver registered (try_engage gate)");
            // The driver catches its own panics into the payload (the M1
            // hook discipline); this outer catch is containment of last
            // resort so lock-group leave + unimpersonation always run.
            let _ = catch_unwind(AssertUnwindSafe(|| driver(shared)));
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
}
