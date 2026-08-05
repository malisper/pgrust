//! The janitor's shared registry: pin table + mint/seal queues + warm-pool
//! state + wakeup handle.
//!
//! "Shmem" in the thread-per-backend port is a process-global static (the
//! autovacuum shmem.rs / launcher CTX precedent): one `pgsync::Mutex` IS the
//! LWLock, visible to every backend thread and the janitor by construction.
//! Everything here is restart-lossy BY DESIGN (spec D1 items 3 and 5):
//! pins protect state within one postmaster lifetime; for a database that
//! must survive restarts, `ALTER DATABASE ... RENAME` out of the prefix.

use types_core::{InvalidOid, Oid, ProcNumber};
use types_error::{PgError, PgResult, ERRCODE_CONFIGURATION_LIMIT_EXCEEDED};

/// Fixed pin-table capacity. Ephemeral databases are per-test-worker scoped;
/// a suite pinning more than this many at once is holding the janitor wrong
/// (the error says so). Small on purpose: the table is scanned linearly
/// under the lock on every reap tick.
///
/// Capacity-cliff audit (2026-08-04, the STORM_N=200 Ensure-table finding):
/// a fixed bound stays CORRECT here, unlike the Ensure table, because pins
/// are USER-driven — one explicit `pgrust_pin_database()` call per database
/// a suite wants kept — and do not scale with connection concurrency. The
/// overflow error is clean, names the limit, and the actionable remedy
/// (unpin something) is entirely in the caller's hands.
pub const MAX_PINS: usize = 64;

/// Headroom on top of `max_connections` in `ensure_capacity`: covers the
/// entries that hold no connection slot — resolved (Done/Failed) entries
/// lingering through ENSURE_LINGER_NS after their last waiter left, and
/// pending stragglers whose waiter timed out or died between service ticks.
/// Mints serialize in the janitor (two synchronous checkpoints each) and
/// resolved entries retire ENSURE_LINGER_NS after completion, so the
/// waiter-less population is a short tail, not a scale factor; 64 (the old
/// fixed capacity) is generous for it.
pub const ENSURE_CAPACITY_SLACK: usize = 64;

/// Boot default of `max_connections` (guc tables), the `ensure_capacity`
/// fallback for unit tests that run without the GUC accessors installed.
const MAX_CONNECTIONS_BOOT_VAL: usize = 100;

/// Ensure-table capacity: `max_connections + ENSURE_CAPACITY_SLACK`.
///
/// Sizing rationale (the fleet STORM_N=200 capacity cliff, 2026-08-04):
/// every PENDING entry with waiters was posted by a connecting client
/// backend that is parked on it, and concurrent client backends are
/// bounded by max_connections — so the table sized from max_connections
/// (plus the waiter-less slack above) can never legitimately fill. The
/// old fixed 64 was a capacity cliff, not a resource bound: a 200-token
/// cold-start storm FATALed every waiter past the 64th. max_connections
/// is PGC_POSTMASTER — fixed for the postmaster's lifetime and loaded at
/// config time, long before the first backend can post (mint posts
/// require a registered janitor, which registers after config load) — so
/// this reads the live backing cell instead of snapshotting; the analog
/// of shmem tables sizing themselves from MaxConnections at allocation.
/// Overflow (`PostEnsure::TableFull`) is therefore an invariant
/// violation — a janitor defect such as leaked entries — and mint.rs
/// words its FATAL accordingly.
pub fn ensure_capacity() -> usize {
    let max_conn = if guc_tables::vars::MaxConnections.installed() {
        guc_tables::vars::MaxConnections.read().max(1) as usize
    } else {
        MAX_CONNECTIONS_BOOT_VAL
    };
    max_conn + ENSURE_CAPACITY_SLACK
}

/// How long a resolved (Done/Failed) Ensure entry with no remaining waiters
/// lingers before `gc_ensures` retires it. The linger is the fresh-mint
/// shield's tail: a Done entry keeps its database name exempt from
/// sweep/reap (`ensure_shields`) until the waiters' connect attempts have
/// comfortably either bound to the database (CountDBBackends > 0 resets any
/// streak) or given up — without it, a zero/short per-template grace could
/// reap a minted database in the mint-to-first-connect window.
pub const ENSURE_LINGER_NS: u64 = 5_000_000_000;

/// One mint request, keyed by database name (idempotent: concurrent
/// connects to the same name join one entry — one CREATE for N waiters).
struct EnsureEntry {
    /// Monotonic generation, the waiter's handle: entries are addressed by
    /// gen (never by index or name) so a retired-and-reposted name can
    /// never alias a stale waiter to the wrong attempt (ABA guard).
    gen: u64,
    name: String,
    template: String,
    /// The connecting role at post time: name for createdb's owner option,
    /// oid for per-role cap accounting of in-flight mints.
    owner_name: String,
    owner_oid: Oid,
    /// Parked backends to SetLatch on completion.
    waiters: Vec<ProcNumber>,
    outcome: EnsureOutcome,
    /// mono_ns stamp when `outcome` left Pending (0 while Pending).
    completed_ns: u64,
}

#[derive(Clone)]
enum EnsureOutcome {
    Pending,
    Done,
    /// The janitor's saved error, cloned to EVERY waiter (PgError is Clone;
    /// spec D2: a CREATE failure must surface to all waiters, never hang
    /// them).
    Failed(Box<PgError>),
}

/// Waiter-visible snapshot of an entry's state.
pub enum EnsureStatus {
    Pending,
    Done,
    Failed(Box<PgError>),
    /// The entry no longer exists (retired). A registered waiter only sees
    /// this after a bug or a GC race — callers must treat it as a failure,
    /// never park on it.
    Gone,
}

/// Result of `post_ensure` (plain data: the FATAL construction lives in
/// mint.rs, which owns error wording; the registry stays elog-free).
pub enum PostEnsure {
    /// Ensure created (this backend is the first waiter).
    Posted(u64),
    /// Joined an existing pending entry for the same name.
    Joined(u64),
    /// No janitor is registered: nothing will ever service the queue.
    JanitorAbsent,
    /// live + in-flight minted databases for this role reached the cap.
    PerRoleCap { counted: usize, max: i32 },
    /// The Ensure table is full — an invariant violation, not a load
    /// condition, since `ensure_capacity()` sizes the table from
    /// max_connections (see its rationale). Carried with the capacity so
    /// the FATAL can name it without re-deriving.
    TableFull { cap: usize },
}

/// Janitor-side view of a pending entry.
#[derive(Clone)]
pub struct PendingEnsure {
    pub gen: u64,
    pub name: String,
    pub template: String,
    pub owner_name: String,
    /// True only for warm-pool replenish specs (gen 0, no registry entry):
    /// the shared mint bodies then create the database with
    /// ALLOW_CONNECTIONS false — a listed spare must not be enterable
    /// (connect-write-disconnect would poison its content invisibly; the
    /// handout flips connectability on inside its own transaction). Client
    /// Ensures always mint connectable (stock createdb default).
    pub spare: bool,
}

/// Fixed spare-table capacity (D3 warm pool): the GLOBAL ceiling the
/// per-template pools share — when pooled-templates x pool_size exceeds it,
/// the replenisher round-robins the cap across templates (pool.rs). Also
/// the pgrust.ephemeral_db_pool_size GUC's own max. Same capacity-cliff
/// audit verdict as MAX_PINS: the pool is OPERATOR-sized (one GUC), never
/// concurrency-scaled, and overflow is a silent skip-add (the replenisher
/// simply stops early), not an error a connect path can hit.
pub const MAX_SPARES: usize = 4096;

/// The effective global spare ceiling: MAX_SPARES, or the
/// PGRUST_EPHEMERAL_DB_MAX_SPARES environment override CLAMPED to it —
/// a rig-only knob (janitor-mint-races' round-robin phase needs the
/// cap-binding regime without minting thousands of spares). Read once per
/// postmaster lifetime; never documented as user surface.
pub(crate) fn max_spares_cap() -> usize {
    fn read_once() -> usize {
        match std::env::var("PGRUST_EPHEMERAL_DB_MAX_SPARES") {
            Ok(v) => v
                .trim()
                .parse::<usize>()
                .ok()
                .filter(|&n| n >= 1)
                .map(|n| n.min(MAX_SPARES))
                .unwrap_or(MAX_SPARES),
            Err(_) => MAX_SPARES,
        }
    }
    pgsync::process_global! {
        static MAX_SPARES_CAP: pgsync::Mutex<Option<usize>> = pgsync::Mutex::new(None);
    }
    let mut g = MAX_SPARES_CAP.lock().unwrap_or_else(|e| e.into_inner());
    *g.get_or_insert_with(read_once)
}

/// Fixed capacity of the pooled-template set (usage-keyed warm pools): the
/// distinct templates minted-from since boot that the replenisher maintains
/// spares for. Overflow is a silent skip (fail-safe QoS: an unpooled
/// template's mints are merely cold) — the MAX_TEMPLATE_FLUSH_MARKS shape.
pub const MAX_POOLED_TEMPLATES: usize = 64;

/// One pre-minted spare clone of a POOLED template (D3 warm pool,
/// usage-keyed redesign), restart-lossy like everything here: post-restart
/// leftovers are unregistered survivors the startup sweep drops, and the
/// pool cold-starts empty and replenishes as templates are minted from.
#[derive(Clone)]
pub struct SpareEntry {
    /// The spare's current datname (`<prefix>spare_<seq>`).
    pub name: String,
    /// Its pg_database oid (preserved across the handout RENAME).
    pub oid: Oid,
    /// Template identity AT MINT TIME: name + oid. A template rebuild
    /// (same name, new oid) or drop makes the spare STALE —
    /// `drain_stale_spares` removes it and the replenisher drops the
    /// database.
    pub template_name: String,
    pub template_oid: Oid,
    /// The template's datallowconn AT MINT TIME. A datallowconn EDGE
    /// (sealed template unsealed-for-writes then re-sealed, or a writable
    /// template1-shape template sealed) observed by the replenish probe or
    /// the handout re-check makes the spare STALE too: its copied content
    /// predates a window in which ordinary connections could write the
    /// template. Both-connectable spares are kept — an always-connectable
    /// template serves spares whose content is as old as their
    /// mint, the documented pool staleness residual (addendum item 6); an
    /// unseal-reseal cycle wholly between janitor observations remains
    /// invisible, mirroring the flush-mark discipline.
    pub template_connectable: bool,
}

/// Fixed capacity of the sealed-template flush-mark table (batch
/// pre-checkpoint skip). Overflow is fail-safe: an unmarkable template
/// simply keeps paying the pre-checkpoint.
pub const MAX_TEMPLATE_FLUSH_MARKS: usize = 64;

/// Fixed seal-request capacity (the MAX_PINS shape: bounded, linear scan
/// under the lock, reject-loud on overflow). Seals are USER-driven — one
/// `pgrust_seal_template()` call per template a suite builds — never
/// concurrency-scaled, and each seal is a once-per-schema-change event;
/// eight concurrent ones is already holding the janitor wrong.
pub const MAX_SEALS: usize = 8;

/// One `pgrust_seal_template()` request (seal.rs owns the choreography;
/// this is the storage). Keyed by resolved catalog datname; concurrent
/// same-name callers JOIN one entry (the Ensure idempotency shape).
struct SealEntry {
    /// Monotonic generation, the waiter's AND the vacuum worker's handle
    /// (the EnsureEntry ABA-guard rationale: never address by index/name).
    gen: u64,
    name: String,
    /// The target's pg_database oid, recorded by `begin_seal_vacuum` when
    /// the janitor validated the entry (InvalidOid while Pending): the
    /// vacuum worker fetches it by gen (`seal_vacuum_target`).
    oid: Oid,
    /// Parked backends to SetLatch on completion.
    waiters: Vec<ProcNumber>,
    state: SealState,
}

enum SealState {
    /// Posted, not yet picked up by the janitor's seal pass.
    Pending,
    /// The one-shot vacuum worker was launched; `deadline_ns` is the leak
    /// bound (a worker the postmaster never started never reports back).
    Vacuuming { deadline_ns: u64 },
    /// The worker reported: Ok = VACUUM (FREEZE, ANALYZE) committed, the
    /// janitor flips the flags next pass; Err = the janitor fails the
    /// entry next pass (report + completion stay janitor-side — the
    /// containment/log discipline lives in one thread).
    VacuumDone(Result<(), Box<PgError>>),
    Done,
    Failed(Box<PgError>),
}

impl SealState {
    fn terminal(&self) -> bool {
        matches!(self, SealState::Done | SealState::Failed(_))
    }
}

/// Result of `post_seal` (plain data, the PostEnsure convention: error
/// wording lives in seal.rs).
pub enum PostSeal {
    Posted(u64),
    /// Joined an existing in-flight entry for the same datname.
    Joined(u64),
    JanitorAbsent,
    TableFull,
}

/// Waiter-visible snapshot of a seal entry's state.
pub enum SealStatus {
    /// Pending / Vacuuming / VacuumDone — still being driven.
    InProgress,
    Done,
    Failed(Box<PgError>),
    /// Retired (a bug or a GC race, the EnsureStatus::Gone contract):
    /// callers fail closed, never park on it.
    Gone,
}

/// One unit of janitor-side seal work this pass (seal.rs drives these).
pub(crate) enum SealWork {
    /// Pending entry: validate the target and launch the vacuum worker.
    Validate { gen: u64, name: String },
    /// Worker reported success: flip IS_TEMPLATE/ALLOW_CONNECTIONS.
    Flip { gen: u64, name: String, oid: Oid },
    /// Worker reported failure: fail the entry with its saved error.
    VacuumFailed { gen: u64, name: String, err: Box<PgError> },
    /// Vacuuming past its deadline with no report: fail the entry.
    TimedOut { gen: u64, name: String },
}

struct RegistryState {
    /// One-shot deferred-startup-sweep request (set at janitor start; the
    /// first tick runs the sweep and a contained failure re-arms it).
    sweep_pending: bool,
    /// The janitor's PGPROC number while it is running (launcher_pid
    /// precedent): lets backend-side posts wake the loop instead of
    /// waiting out the tick.
    janitor_proc: Option<ProcNumber>,
    /// Pinned database names (unqualified, byte-compared against datname).
    /// Always the RESOLVED catalog datname (<= NAMEDATALEN-1 bytes), never a
    /// caller's raw argument: builtins.rs resolves before pinning, because a
    /// longer-than-datname argument would find the database through the
    /// truncating scan key yet never match the reap loop's comparison.
    pins: Vec<String>,
    /// D2 mint requests (bounded by `ensure_capacity()`).
    ensures: Vec<EnsureEntry>,
    next_ensure_gen: u64,
    /// D3 warm-pool spares (bounded by max_spares_cap()). Mutated ONLY from
    /// the janitor loop (replenish/handout), read by the reap/sweep shields.
    spares: Vec<SpareEntry>,
    /// Usage-keyed pooled-template set (bounded by MAX_POOLED_TEMPLATES):
    /// the template NAMES minted-from since boot, in first-mint order. The
    /// replenisher maintains up to pool_size spares PER listed template
    /// (round-robining the global spare cap when it binds) and de-lists
    /// templates whose catalog row is gone. Restart-lossy by design: the
    /// first mint after a restart re-registers.
    pooled_templates: Vec<String>,
    /// Round-robin cursor over `pooled_templates` for cap-bound replenish
    /// fairness (advanced once per replenish pass).
    pool_rr: usize,
    /// Monotonic spare-name sequence: a name that ever failed a handout
    /// (occupied, squatted) is burned and never reused.
    next_spare_seq: u64,
    /// Sealed-template flush marks (batch pre-checkpoint skip):
    /// (template oid, datfrozenxid, datminmxid) at mark time. Restart-lossy
    /// BY DESIGN — the first batch touching a template after janitor start
    /// pays the FLUSH_ALL pre-checkpoint once and marks it (self-healing,
    /// no marker file). The xid/mxid halves make a COMPLETED
    /// anti-wraparound autovacuum (the one writer ALLOW_CONNECTIONS false
    /// does not stop; it advances datfrozenxid/datminmxid at its end)
    /// self-invalidate the mark. Marks whose template was DROPPED are
    /// pruned by the reap pass (`retain_template_flush_marks`): the
    /// observed-unseal clear sites key on a live tuple's oid, so a dropped
    /// template — one per rebuild under the new-name recipe — would
    /// otherwise leak its slot until the table fills and marking silently
    /// stops (every batch then re-pays the pre-checkpoint, with no
    /// witness).
    template_flush_marks: Vec<(Oid, u32, u32)>,
    /// Cached swept-relation counts per template oid (mint-strategy pick,
    /// F2): the wal_log price observed by `dbcommands::count_swept_relations`
    /// the first time a strategy pick needs it. Cleared at every
    /// observed-unseal/connectable site TOGETHER with the flush mark
    /// (`clear_template_flushed`) — once ordinary connections can reach the
    /// template its relation population can change — and pruned with the
    /// marks when the template is dropped. Restart-lossy like everything
    /// here (first pick after janitor start re-counts). Staleness is a
    /// strategy-quality concern only, never correctness: either strategy
    /// mints a correct clone.
    template_relcounts: Vec<(Oid, usize)>,
    /// One-shot latch for the replenisher's prefix-too-long-for-spare-names
    /// refusal line: the prefix is PGC_POSTMASTER and the spare seq is
    /// monotonic, so the condition is permanent once true — without the
    /// latch the refusal would log on EVERY deficit tick (~2 lines/s for
    /// the life of the server).
    pool_name_overflow_logged: bool,
    /// Post-mint prewarm queue (prewarm.rs): (datname, oid) of databases the
    /// janitor minted and has not yet touched. Bounded by MAX_TOUCHES;
    /// enqueue drops on overflow (prewarm is background QoS — an untouched
    /// database is merely a cold one). Restart-lossy like everything here.
    touch_queue: Vec<(String, Oid)>,
    /// Dispatched touches whose worker has not reported back: (oid,
    /// deadline mono_ns). The deadline is the leak bound — a worker that
    /// never STARTED (postmaster refused the spawn) never runs its clear
    /// guard, so `begin_touches` expires stale entries instead of counting
    /// them against the in-flight cap forever.
    touch_inflight: Vec<(Oid, u64)>,
    /// Shared-catalog lifecycle ops (pg_database/pg_shdepend/
    /// pg_db_role_setting row churn: mints, handout renames, drops) since
    /// the last maintenance VACUUM (maint.rs). Monotonic between resets.
    catalog_churn: u64,
    /// mono_ns of the most recent Ensure post (mint_on_connect traffic).
    /// The replenish-deferral signal (pool.rs should_defer_refill): while
    /// mint traffic is landing and the pool is at/above half target, the
    /// refill yields the loop to handout dispatch instead of shadowing
    /// arrivals behind its copy+checkpoint wall. 0 = never.
    last_ensure_post_ns: u64,
    /// `pgrust_seal_template()` requests (bounded by MAX_SEALS; seal.rs
    /// owns the choreography).
    seals: Vec<SealEntry>,
    next_seal_gen: u64,
}

pgsync::process_global! {
    static REGISTRY: pgsync::Mutex<RegistryState> = pgsync::Mutex::new(RegistryState {
        sweep_pending: false,
        janitor_proc: None,
        pins: Vec::new(),
        ensures: Vec::new(),
        next_ensure_gen: 1,
        spares: Vec::new(),
        pooled_templates: Vec::new(),
        pool_rr: 0,
        next_spare_seq: 1,
        template_flush_marks: Vec::new(),
        template_relcounts: Vec::new(),
        pool_name_overflow_logged: false,
        touch_queue: Vec::new(),
        touch_inflight: Vec::new(),
        catalog_churn: 0,
        last_ensure_post_ns: 0,
        seals: Vec::new(),
        next_seal_gen: 1,
    });
}

fn with_registry<R>(f: impl FnOnce(&mut RegistryState) -> R) -> R {
    let mut guard = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    f(&mut guard)
}

/// Pin `name`: the janitor will not reap it while the pin lives (one
/// postmaster lifetime at most). Returns true if newly pinned, false if it
/// was already pinned (idempotent). Errors only when the fixed table is
/// full.
///
/// Timing contract (main_loop::drop_one re-checks pins immediately before
/// each drop): a pin call that returns before a reap cycle selects victims
/// — the pin-soak shape — always protects. A pin racing an in-flight cycle
/// is honored up to the final pre-drop re-check; a pin that lands after the
/// janitor has already begun dropping that database cannot save it. Callers
/// must pin BEFORE abandoning a database they want kept.
pub fn pin(name: &str) -> PgResult<bool> {
    with_registry(|r| {
        if r.pins.iter().any(|p| p == name) {
            return Ok(false);
        }
        if r.pins.len() >= MAX_PINS {
            return Err(Box::new(
                PgError::error(format!(
                    "cannot pin database \"{name}\": the pin table is full ({MAX_PINS} entries)"
                ))
                .with_sqlstate(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
            ));
        }
        r.pins.push(name.to_string());
        Ok(true)
    })
}

/// Unpin `name`. Returns true if a pin was removed, false if none existed.
pub fn unpin(name: &str) -> bool {
    with_registry(|r| {
        let before = r.pins.len();
        r.pins.retain(|p| p != name);
        r.pins.len() != before
    })
}

pub fn is_pinned(name: &str) -> bool {
    with_registry(|r| r.pins.iter().any(|p| p == name))
}

/// Snapshot of the pinned names (logging/tests).
pub fn pinned_names() -> Vec<String> {
    with_registry(|r| r.pins.clone())
}

/// Is a janitor currently registered? Waiter-side belt-and-suspenders
/// (mint::wait_for_mint): the exit drain fails pending entries, but a
/// wedge between death and drain must not cost waiters the full deadline.
pub fn janitor_present() -> bool {
    with_registry(|r| r.janitor_proc.is_some())
}

/// Request the deferred startup sweep (janitor start; re-armed on a
/// contained sweep failure).
pub fn request_sweep() {
    with_registry(|r| r.sweep_pending = true);
}

/// One-shot take of the sweep request (janitor loop only).
pub(crate) fn take_sweep_request() -> bool {
    with_registry(|r| std::mem::take(&mut r.sweep_pending))
}

pub(crate) fn set_janitor_proc(proc: Option<ProcNumber>) {
    with_registry(|r| r.janitor_proc = proc);
}

/// Set the janitor's latch so the next tick runs now (no-op when the
/// janitor is not running).
pub fn wake_janitor() {
    if let Some(procno) = with_registry(|r| r.janitor_proc) {
        latch::SetLatch(types_storage::latch::LatchHandle::proc(procno));
    }
}

// ---------------------------------------------------------------------------
// D2 Ensure table (mint-on-connect).
// ---------------------------------------------------------------------------

/// Post (or join) a mint Ensure for `name`. One atomic sequence under the
/// registry lock: janitor-present check, same-name join,
/// per-role cap, capacity, insert. `live_owned` is the caller's catalog
/// scan of live prefix-matching non-template database NAMES owned by
/// `owner_oid` (computed OUTSIDE the lock — no catalog I/O in here); the
/// cap adds this role's other in-flight entries — PENDING ones, plus
/// resolved-Done lingering ones whose names the caller's scan did NOT see
/// (a same-role mint that committed between the caller's scan and this
/// post is otherwise in neither term and the cap could overshoot). The
/// residual: a post delayed past ENSURE_LINGER_NS after such a commit can
/// still miss both terms — bounded and documented (M3 addendum item 8).
/// Joining an existing entry is exempt from the cap: it creates no new
/// database. The join is keyed by NAME ALONE, not owner: a different
/// allowlisted role joining a same-name Pending entry attaches to a
/// database owned by the FIRST poster (cross-role collision semantics,
/// recorded in the M3 addendum — token collisions across roles are a
/// harness misuse, and creation-owner-wins is the only serializable
/// answer).
#[allow(clippy::too_many_arguments)]
pub fn post_ensure(
    name: &str,
    template: &str,
    owner_name: &str,
    owner_oid: Oid,
    waiter: ProcNumber,
    live_owned: &[String],
    max_per_role: i32,
) -> PostEnsure {
    with_registry(|r| {
        if r.janitor_proc.is_none() {
            return PostEnsure::JanitorAbsent;
        }
        // Traffic stamp for the replenish-deferral signal (pool.rs), taken
        // for every admitted post shape (Joined and Posted alike): both
        // mean a waiter is parked on dispatch latency right now.
        r.last_ensure_post_ns = pg_clock::mono_ns();
        if let Some(e) = r
            .ensures
            .iter_mut()
            .find(|e| e.name == name && matches!(e.outcome, EnsureOutcome::Pending))
        {
            if !e.waiters.contains(&waiter) {
                e.waiters.push(waiter);
            }
            return PostEnsure::Joined(e.gen);
        }
        if max_per_role > 0 {
            let in_flight = r
                .ensures
                .iter()
                .filter(|e| {
                    e.owner_oid == owner_oid
                        && match &e.outcome {
                            EnsureOutcome::Pending => true,
                            // Done + name absent from the caller's scan =
                            // committed inside the scan-to-post window:
                            // counted here or the cap overshoots. Done +
                            // name present is already in live_owned.
                            EnsureOutcome::Done => !live_owned.iter().any(|n| n == &e.name),
                            EnsureOutcome::Failed(_) => false,
                        }
                })
                .count();
            let counted = live_owned.len() + in_flight;
            if counted >= max_per_role as usize {
                return PostEnsure::PerRoleCap {
                    counted,
                    max: max_per_role,
                };
            }
        }
        let cap = ensure_capacity();
        if r.ensures.len() >= cap {
            return PostEnsure::TableFull { cap };
        }
        let gen = r.next_ensure_gen;
        r.next_ensure_gen += 1;
        r.ensures.push(EnsureEntry {
            gen,
            name: name.to_string(),
            template: template.to_string(),
            owner_name: owner_name.to_string(),
            owner_oid,
            waiters: vec![waiter],
            outcome: EnsureOutcome::Pending,
            completed_ns: 0,
        });
        PostEnsure::Posted(gen)
    })
}

/// mono_ns of the most recent admitted Ensure post (0 = never): the
/// replenish-deferral traffic signal (pool.rs should_defer_refill).
pub(crate) fn last_ensure_post_ns() -> u64 {
    with_registry(|r| r.last_ensure_post_ns)
}

/// Waiter-side poll of an entry's state (Failed hands back a clone of the
/// janitor's saved error).
pub fn ensure_status(gen: u64) -> EnsureStatus {
    with_registry(|r| match r.ensures.iter().find(|e| e.gen == gen) {
        None => EnsureStatus::Gone,
        Some(e) => match &e.outcome {
            EnsureOutcome::Pending => EnsureStatus::Pending,
            EnsureOutcome::Done => EnsureStatus::Done,
            EnsureOutcome::Failed(err) => EnsureStatus::Failed(err.clone()),
        },
    })
}

/// Deregister a waiter (every waiter exit path — success, timeout, CFI
/// abort — runs this, via mint.rs's drop guard). Entry retirement itself is
/// the janitor's job (`gc_ensures`), so a Done entry keeps shielding its
/// name through the linger window even after the last waiter left.
pub fn remove_ensure_waiter(gen: u64, waiter: ProcNumber) {
    with_registry(|r| {
        if let Some(e) = r.ensures.iter_mut().find(|e| e.gen == gen) {
            e.waiters.retain(|&w| w != waiter);
        }
    });
}

/// Snapshot of the pending entries, oldest first (janitor service pass).
pub fn pending_ensures() -> Vec<PendingEnsure> {
    with_registry(|r| {
        r.ensures
            .iter()
            .filter(|e| matches!(e.outcome, EnsureOutcome::Pending))
            .map(|e| PendingEnsure {
                gen: e.gen,
                name: e.name.clone(),
                template: e.template.clone(),
                owner_name: e.owner_name.clone(),
                spare: false,
            })
            .collect()
    })
}

/// Resolve a pending entry (janitor side) and return the waiters to wake —
/// the caller SetLatches them OUTSIDE the lock. `now_ns` stamps the linger
/// clock.
pub fn complete_ensure(gen: u64, result: Result<(), Box<PgError>>, now_ns: u64) -> Vec<ProcNumber> {
    with_registry(|r| {
        let Some(e) = r.ensures.iter_mut().find(|e| e.gen == gen) else {
            return Vec::new();
        };
        if !matches!(e.outcome, EnsureOutcome::Pending) {
            return Vec::new();
        }
        e.outcome = match result {
            Ok(()) => EnsureOutcome::Done,
            Err(err) => EnsureOutcome::Failed(err),
        };
        e.completed_ns = now_ns;
        e.waiters.clone()
    })
}

/// Fail EVERY pending entry (janitor-exit path: a queue nothing will ever
/// service again must reject loudly, never hang its waiters) and return
/// all their waiters for waking.
pub fn fail_pending_ensures(err: &PgError, now_ns: u64) -> Vec<ProcNumber> {
    with_registry(|r| fail_pending_locked(r, err, now_ns))
}

fn fail_pending_locked(r: &mut RegistryState, err: &PgError, now_ns: u64) -> Vec<ProcNumber> {
    let mut waiters = Vec::new();
    for e in r.ensures.iter_mut() {
        if matches!(e.outcome, EnsureOutcome::Pending) {
            e.outcome = EnsureOutcome::Failed(Box::new(err.clone()));
            e.completed_ns = now_ns;
            waiters.extend(e.waiters.iter().copied());
        }
    }
    waiters
}

/// The fresh-mint shield consulted by sweep and reap alongside pins: an
/// Ensure entry for `name` — pending OR resolved-but-lingering — exempts
/// the database (see ENSURE_LINGER_NS for why resolved entries count).
pub fn ensure_shields(name: &str) -> bool {
    with_registry(|r| r.ensures.iter().any(|e| e.name == name))
}

/// Retire resolved entries whose waiters are gone and whose linger expired
/// (janitor tick tail).
pub fn gc_ensures(now_ns: u64) {
    with_registry(|r| {
        r.ensures.retain(|e| {
            matches!(e.outcome, EnsureOutcome::Pending)
                || !e.waiters.is_empty()
                || now_ns.saturating_sub(e.completed_ns) < ENSURE_LINGER_NS
        });
    });
}

// ---------------------------------------------------------------------------
// pgrust_seal_template requests (seal.rs owns the choreography; the state
// machine lives here so waiters, the janitor loop, and the one-shot vacuum
// worker all mutate it under the one registry lock).
// ---------------------------------------------------------------------------

/// Post (or join) a seal request for the resolved datname `name`. One
/// atomic sequence under the registry lock (the post_ensure shape):
/// janitor-present check, same-name join, capacity, insert.
/// The join is keyed by name over NON-TERMINAL entries only — a terminal
/// (Done/Failed) entry lingering for its waiters must not absorb a fresh
/// request, which legitimately re-seals after a manual unseal.
pub fn post_seal(name: &str, waiter: ProcNumber) -> PostSeal {
    with_registry(|r| {
        if r.janitor_proc.is_none() {
            return PostSeal::JanitorAbsent;
        }
        if let Some(e) = r
            .seals
            .iter_mut()
            .find(|e| e.name == name && !e.state.terminal())
        {
            if !e.waiters.contains(&waiter) {
                e.waiters.push(waiter);
            }
            return PostSeal::Joined(e.gen);
        }
        if r.seals.len() >= MAX_SEALS {
            return PostSeal::TableFull;
        }
        let gen = r.next_seal_gen;
        r.next_seal_gen += 1;
        r.seals.push(SealEntry {
            gen,
            name: name.to_string(),
            oid: InvalidOid,
            waiters: vec![waiter],
            state: SealState::Pending,
        });
        PostSeal::Posted(gen)
    })
}

/// Waiter-side poll (Failed hands back a clone of the saved error).
pub fn seal_status(gen: u64) -> SealStatus {
    with_registry(|r| match r.seals.iter().find(|e| e.gen == gen) {
        None => SealStatus::Gone,
        Some(e) => match &e.state {
            SealState::Done => SealStatus::Done,
            SealState::Failed(err) => SealStatus::Failed(err.clone()),
            _ => SealStatus::InProgress,
        },
    })
}

/// Deregister a waiter (every waiter exit path, via seal.rs's drop guard —
/// the remove_ensure_waiter contract).
pub fn remove_seal_waiter(gen: u64, waiter: ProcNumber) {
    with_registry(|r| {
        if let Some(e) = r.seals.iter_mut().find(|e| e.gen == gen) {
            e.waiters.retain(|&w| w != waiter);
        }
    });
}

/// The janitor seal pass's work snapshot: everything that needs driving
/// this tick, oldest first. `now_ns` classifies Vacuuming deadlines.
pub(crate) fn seal_work(now_ns: u64) -> Vec<SealWork> {
    with_registry(|r| {
        r.seals
            .iter()
            .filter_map(|e| match &e.state {
                SealState::Pending => Some(SealWork::Validate {
                    gen: e.gen,
                    name: e.name.clone(),
                }),
                SealState::Vacuuming { deadline_ns } if now_ns >= *deadline_ns => {
                    Some(SealWork::TimedOut {
                        gen: e.gen,
                        name: e.name.clone(),
                    })
                }
                SealState::Vacuuming { .. } => None,
                SealState::VacuumDone(Ok(())) => Some(SealWork::Flip {
                    gen: e.gen,
                    name: e.name.clone(),
                    oid: e.oid,
                }),
                SealState::VacuumDone(Err(err)) => Some(SealWork::VacuumFailed {
                    gen: e.gen,
                    name: e.name.clone(),
                    err: err.clone(),
                }),
                SealState::Done | SealState::Failed(_) => None,
            })
            .collect()
    })
}

/// Pending -> Vacuuming (janitor, after validation launched the worker):
/// records the validated oid and the worker's report deadline. False = the
/// entry is gone or no longer Pending (nothing was mutated).
pub(crate) fn begin_seal_vacuum(gen: u64, oid: Oid, deadline_ns: u64) -> bool {
    with_registry(|r| {
        let Some(e) = r.seals.iter_mut().find(|e| e.gen == gen) else {
            return false;
        };
        if !matches!(e.state, SealState::Pending) {
            return false;
        }
        e.oid = oid;
        e.state = SealState::Vacuuming { deadline_ns };
        true
    })
}

/// The vacuum worker's target lookup (bgw_main_arg carries only the gen).
/// None = the entry was retired or is not awaiting a vacuum.
pub fn seal_vacuum_target(gen: u64) -> Option<Oid> {
    with_registry(|r| {
        r.seals
            .iter()
            .find(|e| e.gen == gen && matches!(e.state, SealState::Vacuuming { .. }))
            .map(|e| e.oid)
    })
}

/// The vacuum worker's report (runs on ITS thread, the finish_touch
/// exception to the loop-only discipline): Vacuuming -> VacuumDone. The
/// janitor drives the rest next pass (the caller wakes it). A report
/// against a retired/re-staged gen is a no-op — a worker outliving its
/// entry's timeout must not resurrect it.
pub fn finish_seal_vacuum(gen: u64, result: Result<(), Box<PgError>>) {
    with_registry(|r| {
        if let Some(e) = r.seals.iter_mut().find(|e| e.gen == gen) {
            if matches!(e.state, SealState::Vacuuming { .. }) {
                e.state = SealState::VacuumDone(result);
            }
        }
    });
}

/// Vacuuming -> Pending (worker registration refused/failed): the next
/// tick's seal pass revalidates and retries (the prewarm requeue_touch
/// convention).
pub(crate) fn requeue_seal(gen: u64) {
    with_registry(|r| {
        if let Some(e) = r.seals.iter_mut().find(|e| e.gen == gen) {
            if matches!(e.state, SealState::Vacuuming { .. }) {
                e.state = SealState::Pending;
            }
        }
    });
}

/// Resolve a seal entry (janitor side) and return the waiters to wake
/// OUTSIDE the lock (the complete_ensure convention). No-op on terminal
/// entries.
pub(crate) fn complete_seal(gen: u64, result: Result<(), Box<PgError>>) -> Vec<ProcNumber> {
    with_registry(|r| {
        let Some(e) = r.seals.iter_mut().find(|e| e.gen == gen) else {
            return Vec::new();
        };
        if e.state.terminal() {
            return Vec::new();
        }
        e.state = match result {
            Ok(()) => SealState::Done,
            Err(err) => SealState::Failed(err),
        };
        e.waiters.clone()
    })
}

/// The seal shield, consulted by sweep and reap NEXT TO pins/ensures/
/// spares: a database with a seal IN FLIGHT (non-terminal entry) is exempt
/// from reaping — the target of a seal is by definition not yet a template,
/// so an already-grace-idle in-prefix target would otherwise be reaped out
/// from under its own seal. Terminal entries do not shield: Done means the
/// database IS a template now (the template exemption takes over), Failed
/// means ordinary lifecycle resumes.
pub fn seal_shields(name: &str) -> bool {
    with_registry(|r| {
        r.seals
            .iter()
            .any(|e| e.name == name && !e.state.terminal())
    })
}

/// Retire terminal entries whose waiters are gone (janitor tick tail, next
/// to gc_ensures). No linger needed: unlike mints there is no
/// mint-to-first-connect window to shield — the flip committed before Done.
pub(crate) fn gc_seals() {
    with_registry(|r| {
        r.seals
            .retain(|e| !e.state.terminal() || !e.waiters.is_empty());
    });
}

// ---------------------------------------------------------------------------
// D3 warm-pool spare set.
// ---------------------------------------------------------------------------

/// The warm-pool shield, consulted by sweep and reap NEXT TO pins and
/// ensure_shields (all three call sites — the enumeration predicates AND
/// the pre-drop re-check — or spares are lost silently): a listed spare is
/// exempt from reaping while listed. Unlisted leftovers (post-restart, or
/// dropped-from-pool poisoned spares) are ordinary ephemeral candidates.
pub fn spare_shields(name: &str) -> bool {
    with_registry(|r| r.spares.iter().any(|s| s.name == name))
}

/// Cheap pool-armed probe: the handout pass bails on this before touching
/// any transaction machinery, so the pool-off service path costs one
/// registry lock.
pub(crate) fn any_spares() -> bool {
    with_registry(|r| !r.spares.is_empty())
}

/// Register a freshly minted spare. False = table full or duplicate name
/// (both are replenisher bookkeeping bugs upstream, tolerated fail-safe:
/// an unregistered spare is unshielded and reaps like any ephemeral).
pub(crate) fn add_spare(e: SpareEntry) -> bool {
    with_registry(|r| {
        if r.spares.len() >= max_spares_cap() || r.spares.iter().any(|s| s.name == e.name) {
            return false;
        }
        r.spares.push(e);
        true
    })
}

// ---------------------------------------------------------------------------
// Usage-keyed pooled-template set (item A redesign): the replenisher
// maintains spares for every template minted-from since boot.
// ---------------------------------------------------------------------------

/// Register `template` as pooled (called on every successful COLD mint;
/// the first call per template is the registration — that mint was served
/// cold by design, replenish maintains warmth thereafter). Idempotent;
/// false = already listed or the set is full (silent fail-safe QoS).
pub(crate) fn note_pooled_template(template: &str) -> bool {
    with_registry(|r| {
        if r.pooled_templates.iter().any(|t| t == template)
            || r.pooled_templates.len() >= MAX_POOLED_TEMPLATES
        {
            return false;
        }
        r.pooled_templates.push(template.to_string());
        true
    })
}

/// Snapshot of the pooled-template names, first-mint order (replenish pass).
pub(crate) fn pooled_templates() -> Vec<String> {
    with_registry(|r| r.pooled_templates.clone())
}

/// De-list a pooled template (its catalog row is gone: the replenish probe
/// missed it). Its remaining spares are drained by the same pass.
pub(crate) fn remove_pooled_template(template: &str) -> bool {
    with_registry(|r| {
        let before = r.pooled_templates.len();
        r.pooled_templates.retain(|t| t != template);
        r.pooled_templates.len() != before
    })
}

/// Advance and return the round-robin cursor over `n` pooled templates
/// (cap-bound replenish fairness, one step per pass). 0 when n == 0.
pub(crate) fn advance_pool_rr(n: usize) -> usize {
    with_registry(|r| {
        if n == 0 {
            r.pool_rr = 0;
            return 0;
        }
        let cur = r.pool_rr % n;
        r.pool_rr = (cur + 1) % n;
        cur
    })
}

/// Total listed spares across all templates (global-cap accounting).
pub(crate) fn total_spares() -> usize {
    with_registry(|r| r.spares.len())
}

/// First spare minted from `template_name` (handout candidate). A clone,
/// not a removal: the entry keeps shielding the spare's name until the
/// handout RENAME commits (`remove_spare` then retires it) or fails
/// (poisoned spares are removed and left to the ordinary reap path).
pub(crate) fn peek_spare(template_name: &str) -> Option<SpareEntry> {
    with_registry(|r| {
        r.spares
            .iter()
            .find(|s| s.template_name == template_name)
            .cloned()
    })
}

pub(crate) fn remove_spare(name: &str) -> bool {
    with_registry(|r| {
        let before = r.spares.len();
        r.spares.retain(|s| s.name != name);
        r.spares.len() != before
    })
}

/// Live spares matching the CURRENT default-template identity (replenish
/// deficit accounting; stale spares are drained, never counted).
pub(crate) fn spare_count(template_name: &str, template_oid: Oid) -> usize {
    with_registry(|r| {
        r.spares
            .iter()
            .filter(|s| s.template_name == template_name && s.template_oid == template_oid)
            .count()
    })
}

/// Remove and return every spare NOT matching one of `identities`
/// ((template name, template oid, template datallowconn AS OBSERVED NOW) —
/// one entry per pooled template with a VALID pool this pass; an empty
/// slice drains ALL spares). A spare whose template was dropped, rebuilt
/// (new oid), unsealed, or crossed a connectable EDGE (either direction —
/// see `SpareEntry::template_connectable`) is stale; templates absent from
/// the slice drain wholesale, which is exactly the per-template drain the
/// item-A ruling asks for (resealing/dropping/unsealing template T drains
/// T's spares only — every other template's identity is still listed and
/// still matches). The caller drops the returned databases via the batch
/// drop path.
pub(crate) fn drain_stale_spares(identities: &[(String, Oid, bool)]) -> Vec<SpareEntry> {
    with_registry(|r| {
        let (keep, stale): (Vec<SpareEntry>, Vec<SpareEntry>) =
            r.spares.drain(..).partition(|s| {
                identities.iter().any(|(name, oid, allowconn)| {
                    s.template_name == *name
                        && s.template_oid == *oid
                        && s.template_connectable == *allowconn
                })
            });
        r.spares = keep;
        stale
    })
}

/// Remove and return identity-matching spares beyond `keep` (newest first
/// leave; the oldest `keep` stay): the pool_size-shrink drain. The caller
/// drops the returned databases via the batch drop path.
pub(crate) fn take_excess_spares(
    template_name: &str,
    template_oid: Oid,
    keep: usize,
) -> Vec<SpareEntry> {
    with_registry(|r| {
        let mut seen = 0usize;
        let (kept, excess): (Vec<SpareEntry>, Vec<SpareEntry>) =
            r.spares.drain(..).partition(|s| {
                if s.template_name == template_name && s.template_oid == template_oid {
                    seen += 1;
                    seen <= keep
                } else {
                    true
                }
            });
        r.spares = kept;
        excess
    })
}

/// Next spare-name sequence number (monotonic per postmaster lifetime).
pub(crate) fn next_spare_seq() -> u64 {
    with_registry(|r| {
        let s = r.next_spare_seq;
        r.next_spare_seq += 1;
        s
    })
}

/// One-shot token for the replenisher's spare-name-overflow refusal line:
/// true exactly once per postmaster lifetime (the condition — prefix too
/// long for `<prefix>spare_<seq>` — is permanent: the prefix is
/// PGC_POSTMASTER and the seq is monotonic).
pub(crate) fn pool_name_overflow_log_once() -> bool {
    with_registry(|r| !std::mem::replace(&mut r.pool_name_overflow_logged, true))
}

// ---------------------------------------------------------------------------
// Post-mint prewarm bookkeeping (prewarm.rs owns the policy; this is the
// storage). All mutation runs under the one registry lock; `finish_touch`
// is the exception to the loop-only discipline — it is called from the
// prewarm WORKER's thread (its exit guard), which is exactly why the
// in-flight table exists here and not in janitor-loop-local state.
// ---------------------------------------------------------------------------

/// Fixed touch-queue capacity: MAX_SPARES (a full pool replenish) plus a
/// mint batch. Overflow drops the enqueue — prewarm is background QoS,
/// never a correctness edge.
pub(crate) const MAX_TOUCHES: usize = MAX_SPARES + crate::mint::MINT_BATCH_MAX;

/// Enqueue a freshly minted database for a prewarm touch. Deduped by oid
/// against both the queue and the in-flight set (an idempotent re-mint of
/// the same name can otherwise enqueue twice across ticks). Returns false
/// when dropped (full or duplicate).
pub(crate) fn enqueue_touch(name: &str, oid: Oid) -> bool {
    with_registry(|r| {
        if r.touch_queue.len() >= MAX_TOUCHES
            || r.touch_queue.iter().any(|&(_, o)| o == oid)
            || r.touch_inflight.iter().any(|&(o, _)| o == oid)
        {
            return false;
        }
        r.touch_queue.push((name.to_string(), oid));
        true
    })
}

/// Dispatch step (prewarm.rs, once per janitor tick): expire deadline-passed
/// in-flight entries, then pop up to `max_inflight - inflight` targets off
/// the queue front (FIFO) and record them in-flight until `deadline_ns`.
pub(crate) fn begin_touches(
    now_ns: u64,
    deadline_ns: u64,
    max_inflight: usize,
) -> Vec<(String, Oid)> {
    with_registry(|r| {
        r.touch_inflight.retain(|&(_, d)| d > now_ns);
        let room = max_inflight.saturating_sub(r.touch_inflight.len());
        let take = room.min(r.touch_queue.len());
        let out: Vec<(String, Oid)> = r.touch_queue.drain(..take).collect();
        for &(_, oid) in &out {
            r.touch_inflight.push((oid, deadline_ns));
        }
        out
    })
}

/// The prewarm worker's exit guard (runs on ITS thread, success or failure):
/// the touch is no longer in flight.
pub fn finish_touch(oid: Oid) {
    with_registry(|r| r.touch_inflight.retain(|&(o, _)| o != oid));
}

/// Registration failed (no free bgworker slot): put the target back at the
/// queue FRONT (it is the oldest) and release its in-flight slot; the next
/// tick retries.
pub(crate) fn requeue_touch(name: String, oid: Oid) {
    with_registry(|r| {
        r.touch_inflight.retain(|&(o, _)| o != oid);
        if r.touch_queue.len() < MAX_TOUCHES && !r.touch_queue.iter().any(|&(_, o)| o == oid) {
            r.touch_queue.insert(0, (name, oid));
        }
    })
}

// ---------------------------------------------------------------------------
// Shared-catalog churn accounting (maint.rs owns the cadence policy).
// ---------------------------------------------------------------------------

/// Record `n` shared-catalog lifecycle ops (mint commits, handout renames,
/// drops). Saturating: the counter is a cadence trigger, not a ledger.
pub(crate) fn note_catalog_churn(n: u64) {
    with_registry(|r| r.catalog_churn = r.catalog_churn.saturating_add(n))
}

pub(crate) fn catalog_churn() -> u64 {
    with_registry(|r| r.catalog_churn)
}

/// Reset after a SUCCESSFUL maintenance run only: a failed run keeps its
/// churn so the retry (interval-spaced by maint.rs) stays armed.
pub(crate) fn reset_catalog_churn() {
    with_registry(|r| r.catalog_churn = 0)
}

// ---------------------------------------------------------------------------
// Sealed-template flush marks (batch pre-checkpoint skip; the safety
// rationale lives on mint.rs's batch body, next to the skip itself).
// ---------------------------------------------------------------------------

/// Is `oid` marked sealed-and-flushed with EXACTLY this
/// datfrozenxid/datminmxid? A mismatch (a completed anti-wraparound
/// autovacuum advanced either) reads as unmarked, so the next batch pays
/// the pre-checkpoint and re-marks.
pub(crate) fn template_flushed_matches(oid: Oid, frozenxid: u32, minmxid: u32) -> bool {
    with_registry(|r| {
        r.template_flush_marks
            .iter()
            .any(|&(o, f, m)| o == oid && f == frozenxid && m == minmxid)
    })
}

/// Upsert the flush mark for `oid`. Skip-on-full is fail-safe (the batch
/// keeps checkpointing).
pub(crate) fn mark_template_flushed(oid: Oid, frozenxid: u32, minmxid: u32) {
    with_registry(|r| {
        if let Some(slot) = r.template_flush_marks.iter_mut().find(|(o, _, _)| *o == oid) {
            slot.1 = frozenxid;
            slot.2 = minmxid;
            return;
        }
        if r.template_flush_marks.len() < MAX_TEMPLATE_FLUSH_MARKS {
            r.template_flush_marks.push((oid, frozenxid, minmxid));
        }
    })
}

/// Drop `oid`'s flush mark AND its cached swept-relation count. Called
/// whenever a janitor probe OBSERVES the template unsealed or connectable
/// (datistemplate = false or datallowconn = true): once ordinary
/// connections can reach it, "no dirty buffers can exist" no longer holds
/// (the mark must not survive a later re-seal) and its relation population
/// can change (the strategy pick must re-count on the next observation).
/// (An unseal-write-reseal cycle entirely between janitor observations is
/// invisible — the documented residual; the recipe never unseals a
/// template, rebuilds get a NEW name.) All observation sites clear:
/// preflight, the batch re-check, the SERIAL mint's template check
/// (mint_one — the single-entry tick bypasses preflight entirely), and
/// the warm-pool probe + handout re-check.
pub(crate) fn clear_template_flushed(oid: Oid) {
    with_registry(|r| {
        r.template_flush_marks.retain(|&(o, _, _)| o != oid);
        r.template_relcounts.retain(|&(o, _)| o != oid);
    })
}

/// Cached swept-relation count for `oid` (mint-strategy pick), if observed.
pub(crate) fn template_relcount(oid: Oid) -> Option<usize> {
    with_registry(|r| {
        r.template_relcounts
            .iter()
            .find(|&&(o, _)| o == oid)
            .map(|&(_, n)| n)
    })
}

/// Upsert the swept-relation count for `oid`. Skip-on-full is fail-safe
/// (an uncached template re-counts at each pick — costs a pg_class read,
/// never correctness); bounded by the flush-mark table's cap, the same
/// one-slot-per-live-template population.
pub(crate) fn set_template_relcount(oid: Oid, n: usize) {
    with_registry(|r| {
        if let Some(slot) = r.template_relcounts.iter_mut().find(|(o, _)| *o == oid) {
            slot.1 = n;
            return;
        }
        if r.template_relcounts.len() < MAX_TEMPLATE_FLUSH_MARKS {
            r.template_relcounts.push((oid, n));
        }
    })
}

/// Prune flush marks whose template no longer exists (reap-pass tail, fed
/// the full pg_database oid set from the tick's one catalog scan). Without
/// this, a dropped template's mark — one per rebuild under the
/// rebuilds-get-a-NEW-name recipe — leaks its slot forever: at
/// MAX_TEMPLATE_FLUSH_MARKS dead entries, `mark_template_flushed`
/// silently stops marking and every batch re-pays the FLUSH_ALL
/// pre-checkpoint with no witness.
pub(crate) fn retain_template_flush_marks(live_oids: &[Oid]) {
    with_registry(|r| {
        r.template_flush_marks
            .retain(|&(o, _, _)| live_oids.contains(&o));
        // The relcount cache leaks a slot per dropped template exactly the
        // same way; prune it on the same feed.
        r.template_relcounts.retain(|&(o, _)| live_oids.contains(&o));
    })
}

/// Serializes every test that touches the process-global pin table, across
/// ALL of this crate's test modules (registry_semantics' capacity phase
/// transiently FILLS the table; any concurrent pin/unpin — e.g.
/// main_loop's pre-drop re-check test — would perturb its accounting, and
/// vice versa). Same discipline as registry_semantics' one-test-function
/// rule, extended crate-wide.
#[cfg(test)]
pub(crate) fn test_pin_table_lock() -> pgsync::MutexGuard<'static, ()> {
    pgsync::process_global! {
        static TEST_PIN_TABLE: pgsync::Mutex<()> = pgsync::Mutex::new(());
    }
    TEST_PIN_TABLE.lock().unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ONE test function on purpose: the registry is process-global state and
    // the test harness runs tests concurrently; splitting these assertions
    // across #[test] fns would race through the shared pin table.
    #[test]
    fn registry_semantics() {
        let _table = test_pin_table_lock();
        // pin is idempotent and reports first-pin.
        assert!(pin("tv_reg_a").unwrap());
        assert!(!pin("tv_reg_a").unwrap());
        assert!(is_pinned("tv_reg_a"));
        assert!(!is_pinned("tv_reg_b"));

        // unpin reports whether a pin existed.
        assert!(unpin("tv_reg_a"));
        assert!(!unpin("tv_reg_a"));
        assert!(!is_pinned("tv_reg_a"));

        // The table is bounded: filling it errors on the next distinct name
        // and the error names the limit.
        let base = pinned_names().len();
        let mut mine = Vec::new();
        for i in base..MAX_PINS {
            let name = format!("tv_reg_fill_{i}");
            assert!(pin(&name).unwrap());
            mine.push(name);
        }
        let overflow = pin("tv_reg_overflow").unwrap_err();
        assert!(overflow.message().contains("pin table is full"));
        // Re-pinning an existing name still succeeds while full (idempotent
        // path is checked before capacity).
        assert!(!pin(&mine[0]).unwrap());
        for name in &mine {
            assert!(unpin(name));
        }

        // One-shot sweep request.
        request_sweep();
        assert!(take_sweep_request());
        assert!(!take_sweep_request());

        // wake_janitor with no janitor running is a no-op.
        set_janitor_proc(None);
        wake_janitor();
    }

    // ONE test function for the whole Ensure/grace surface, same rationale
    // as registry_semantics, under the same crate-wide lock (shared
    // process-global state). No path in here may SetLatch: unit tests have
    // no proc table.
    #[test]
    fn ensure_semantics() {
        let _table = test_pin_table_lock();
        let now = 100_000_000_000u64;

        let post = |name: &str, waiter: ProcNumber, live: &[String], max: i32| {
            post_ensure(name, "tpl_x", "minter", 90301, waiter, live, max)
        };
        let live1 = vec!["tv_e_live1".to_string()];

        // Absent janitor: rejected before anything is queued.
        set_janitor_proc(None);
        assert!(matches!(post("tv_e_a", 1, &[], 0), PostEnsure::JanitorAbsent));

        set_janitor_proc(Some(7));

        // Post, then same-name joins coalesce onto one entry (idempotent
        // Ensure; duplicate waiter procnos dedupe).
        let PostEnsure::Posted(gen_a) = post("tv_e_a", 1, &[], 0) else {
            panic!("expected Posted");
        };
        assert!(matches!(post("tv_e_a", 2, &[], 0), PostEnsure::Joined(g) if g == gen_a));
        assert!(matches!(post("tv_e_a", 2, &[], 0), PostEnsure::Joined(g) if g == gen_a));
        assert!(matches!(ensure_status(gen_a), EnsureStatus::Pending));
        assert_eq!(pending_ensures().len(), 1);

        // Per-role cap counts live catalog rows + this role's OTHER pending
        // entries; joining is cap-exempt (checked above: live 0, max 0).
        // live 1 + in-flight 1 (tv_e_a) = 2 >= max 2: refused.
        assert!(matches!(
            post("tv_e_b", 3, &live1, 2),
            PostEnsure::PerRoleCap { counted: 2, max: 2 }
        ));
        // max 3 admits it.
        let PostEnsure::Posted(gen_b) = post("tv_e_b", 3, &live1, 3) else {
            panic!("expected Posted");
        };

        // Shield: pending entries exempt their name from sweep/reap.
        assert!(ensure_shields("tv_e_a"));
        assert!(ensure_shields("tv_e_b"));
        assert!(!ensure_shields("tv_e_zzz"));

        // Completion returns the waiters (the janitor wakes them outside
        // the lock) exactly once; the entry then reports Done and keeps
        // shielding through the linger window.
        let w = complete_ensure(gen_a, Ok(()), now);
        assert_eq!(w, vec![1, 2]);
        assert!(
            complete_ensure(gen_a, Ok(()), now).is_empty(),
            "already resolved"
        );
        assert!(matches!(ensure_status(gen_a), EnsureStatus::Done));
        assert!(ensure_shields("tv_e_a"));

        // The scan-to-post window (cap TOCTOU): a same-role mint that
        // resolved Done AFTER the caller's catalog scan counts toward the
        // cap unless the caller's scan saw its row. tv_e_a is Done and
        // ABSENT from this caller's scan: Done(1) + pending tv_e_b(1) = 2
        // >= max 2 — refused (without the Done term the cap overshoots).
        assert!(matches!(
            post("tv_e_cap", 4, &[], 2),
            PostEnsure::PerRoleCap { counted: 2, max: 2 }
        ));
        // With tv_e_a IN the scan it is counted once, via live_owned:
        // live 1 + pending tv_e_b = 2 < 3 — admitted.
        let scanned = vec!["tv_e_a".to_string()];
        let PostEnsure::Posted(gen_c) = post("tv_e_cap", 4, &scanned, 3) else {
            panic!("Done entry seen by the scan must not double-count");
        };
        // Failed entries never count toward the cap (no database exists):
        // Done tv_e_a(1) + pending tv_e_b(1) + Failed tv_e_cap(0) = 2 < 3.
        complete_ensure(gen_c, Err(Box::new(PgError::error("x".to_string()))), now);
        let PostEnsure::Posted(gen_d) = post("tv_e_cap2", 5, &[], 3) else {
            panic!("Failed entries must not count toward the cap");
        };
        complete_ensure(gen_d, Err(Box::new(PgError::error("x".to_string()))), now);
        remove_ensure_waiter(gen_c, 4);
        remove_ensure_waiter(gen_d, 5);

        // Waiters deregister; the resolved entry still lingers, then GC
        // retires it after ENSURE_LINGER_NS.
        remove_ensure_waiter(gen_a, 1);
        remove_ensure_waiter(gen_a, 2);
        gc_ensures(now + 1);
        assert!(
            ensure_shields("tv_e_a"),
            "must linger for the fresh-mint shield"
        );
        gc_ensures(now + ENSURE_LINGER_NS + 1);
        assert!(!ensure_shields("tv_e_a"));
        assert!(matches!(ensure_status(gen_a), EnsureStatus::Gone));

        // fail_pending_ensures (paused-drain / janitor-exit): every pending
        // entry fails with a CLONE of the same error; waiters are handed
        // back for waking.
        let cause = PgError::error("janitor exited".to_string());
        let w = fail_pending_ensures(&cause, now);
        assert_eq!(w, vec![3]);
        match ensure_status(gen_b) {
            EnsureStatus::Failed(e) => assert_eq!(e.message(), "janitor exited"),
            _ => panic!("expected Failed"),
        }
        remove_ensure_waiter(gen_b, 3);
        gc_ensures(now + ENSURE_LINGER_NS + 1);
        assert!(matches!(ensure_status(gen_b), EnsureStatus::Gone));

        // Capacity: fill the table with pending entries; the next distinct
        // name is refused loudly. The capacity is max_connections-derived
        // (ensure_capacity; boot-default fallback in unit tests) and the
        // refusal reports it — the invariant-violation path stays covered
        // even though live servers can no longer reach it.
        let cap = ensure_capacity();
        assert_eq!(
            cap,
            100 + ENSURE_CAPACITY_SLACK,
            "unit tests run on the boot-default max_connections fallback"
        );
        let mut gens = Vec::new();
        for i in 0..cap {
            match post(&format!("tv_e_fill_{i}"), 9, &[], 0) {
                PostEnsure::Posted(g) => gens.push(g),
                _ => panic!("fill {i} refused"),
            }
        }
        assert!(matches!(
            post("tv_e_overflow", 9, &[], 0),
            PostEnsure::TableFull { cap: c } if c == cap
        ));
        // A join still works while full (idempotent path precedes capacity).
        assert!(matches!(
            post("tv_e_fill_0", 10, &[], 0),
            PostEnsure::Joined(_)
        ));
        let cause = PgError::error("drain".to_string());
        for w in fail_pending_ensures(&cause, now) {
            for &g in &gens {
                remove_ensure_waiter(g, w);
            }
        }
        gc_ensures(u64::MAX);
        assert_eq!(pending_ensures().len(), 0);

        set_janitor_proc(None);
    }

    // ONE test function for the whole seal-request state machine, same
    // process-global-state rationale as its siblings, under the same
    // crate-wide lock. No path in here may SetLatch (no proc table).
    #[test]
    fn seal_request_semantics() {
        let _table = test_pin_table_lock();

        // Absent janitor: rejected before anything is queued (the
        // post_ensure taxonomy).
        set_janitor_proc(None);
        assert!(matches!(post_seal("tv_s_a", 1), PostSeal::JanitorAbsent));
        set_janitor_proc(Some(7));

        // Post, then same-name joins coalesce (idempotent; duplicate
        // waiters dedupe).
        let PostSeal::Posted(gen_a) = post_seal("tv_s_a", 1) else {
            panic!("expected Posted");
        };
        assert!(matches!(post_seal("tv_s_a", 2), PostSeal::Joined(g) if g == gen_a));
        assert!(matches!(post_seal("tv_s_a", 2), PostSeal::Joined(g) if g == gen_a));
        assert!(matches!(seal_status(gen_a), SealStatus::InProgress));

        // The shield covers every non-terminal state (deleting the
        // seal_shielded clause from reap_candidate fails main_loop/reap
        // tests; deleting the non-terminal predicate here fails this).
        assert!(seal_shields("tv_s_a"));
        assert!(!seal_shields("tv_s_zzz"));

        // Pending work is Validate; begin_seal_vacuum stages the oid and
        // moves to Vacuuming (work then hides it until the deadline).
        let now = 100_000_000_000u64;
        let work = seal_work(now);
        assert!(matches!(
            work.as_slice(),
            [SealWork::Validate { gen, name }] if *gen == gen_a && name == "tv_s_a"
        ));
        assert!(begin_seal_vacuum(gen_a, 90601, now + 1_000));
        assert!(!begin_seal_vacuum(gen_a, 90601, now + 1_000), "not Pending anymore");
        assert!(seal_work(now).is_empty(), "in-deadline Vacuuming is not work");
        assert_eq!(seal_vacuum_target(gen_a), Some(90601));
        assert_eq!(seal_vacuum_target(999_999), None);

        // Worker success: VacuumDone(Ok) surfaces as Flip work with the
        // staged oid; completion returns the waiters exactly once and the
        // shield drops on the terminal state.
        finish_seal_vacuum(gen_a, Ok(()));
        let work = seal_work(now);
        assert!(matches!(
            work.as_slice(),
            [SealWork::Flip { gen, oid, .. }] if *gen == gen_a && *oid == 90601
        ));
        let w = complete_seal(gen_a, Ok(()));
        assert_eq!(w, vec![1, 2]);
        assert!(complete_seal(gen_a, Ok(())).is_empty(), "already terminal");
        assert!(matches!(seal_status(gen_a), SealStatus::Done));
        assert!(!seal_shields("tv_s_a"), "Done does not shield");

        // A terminal entry lingers for its waiters but never absorbs a
        // fresh same-name post (re-seal after manual unseal is legal).
        let PostSeal::Posted(gen_a2) = post_seal("tv_s_a", 3) else {
            panic!("terminal entry must not absorb a fresh post");
        };
        assert_ne!(gen_a2, gen_a);

        // Worker failure: the saved error fans out through VacuumFailed
        // work and the Failed terminal state.
        assert!(begin_seal_vacuum(gen_a2, 90602, now + 1_000));
        finish_seal_vacuum(
            gen_a2,
            Err(Box::new(PgError::error("vacuum blew up".to_string()))),
        );
        let work = seal_work(now);
        assert!(matches!(
            work.as_slice(),
            [SealWork::VacuumFailed { gen, err, .. }]
                if *gen == gen_a2 && err.message() == "vacuum blew up"
        ));
        let w = complete_seal(gen_a2, Err(Box::new(PgError::error("vacuum blew up".to_string()))));
        assert_eq!(w, vec![3]);
        match seal_status(gen_a2) {
            SealStatus::Failed(e) => assert_eq!(e.message(), "vacuum blew up"),
            _ => panic!("expected Failed"),
        }

        // Deadline expiry: Vacuuming past its deadline is TimedOut work; a
        // late worker report against the timed-out-and-completed entry is a
        // no-op (never resurrects).
        let PostSeal::Posted(gen_b) = post_seal("tv_s_b", 4) else {
            panic!("expected Posted");
        };
        assert!(begin_seal_vacuum(gen_b, 90603, now + 1_000));
        let work = seal_work(now + 1_000);
        assert!(matches!(
            work.as_slice(),
            [SealWork::TimedOut { gen, .. }] if *gen == gen_b
        ));
        let w = complete_seal(gen_b, Err(Box::new(PgError::error("timed out".to_string()))));
        assert_eq!(w, vec![4]);
        finish_seal_vacuum(gen_b, Ok(()));
        assert!(matches!(seal_status(gen_b), SealStatus::Failed(_)));

        // GC retires terminal entries once their waiters leave; live gens
        // report Gone afterwards (fail-closed for any stale waiter).
        gc_seals();
        assert!(matches!(seal_status(gen_a), SealStatus::Done), "waiters still registered");
        remove_seal_waiter(gen_a, 1);
        remove_seal_waiter(gen_a, 2);
        remove_seal_waiter(gen_a2, 3);
        remove_seal_waiter(gen_b, 4);
        gc_seals();
        assert!(matches!(seal_status(gen_a), SealStatus::Gone));
        assert!(matches!(seal_status(gen_b), SealStatus::Gone));

        // Capacity: the table is bounded; overflow refuses; joins still
        // work while full (idempotent path precedes capacity).
        let mut gens = Vec::new();
        for i in 0..MAX_SEALS {
            match post_seal(&format!("tv_s_fill_{i}"), 9) {
                PostSeal::Posted(g) => gens.push(g),
                _ => panic!("fill {i} refused"),
            }
        }
        assert!(matches!(post_seal("tv_s_overflow", 9), PostSeal::TableFull));
        assert!(matches!(post_seal("tv_s_fill_0", 10), PostSeal::Joined(_)));
        for &g in &gens {
            complete_seal(g, Ok(()));
            remove_seal_waiter(g, 9);
        }
        remove_seal_waiter(gens[0], 10);
        gc_seals();

        set_janitor_proc(None);
    }

    // ONE test function for the whole D3 warm-pool surface (spare set +
    // sealed-template flush marks), same process-global-state rationale as
    // its siblings, under the same crate-wide lock.
    #[test]
    fn spare_and_flush_mark_semantics() {
        let _table = test_pin_table_lock();

        let sp = |name: &str, oid: Oid, tpl: &str, tpl_oid: Oid| SpareEntry {
            name: name.to_string(),
            oid,
            template_name: tpl.to_string(),
            template_oid: tpl_oid,
            template_connectable: false,
        };

        // Empty pool: no shields, no peeks, cheap any_spares probe.
        assert!(!any_spares());
        assert!(!spare_shields("tv_spare_1"));
        assert!(peek_spare("tpl_a").is_none());

        // Registration shields the name; duplicates are refused.
        assert!(add_spare(sp("tv_spare_1", 90401, "tpl_a", 90400)));
        assert!(!add_spare(sp("tv_spare_1", 90499, "tpl_a", 90400)));
        assert!(any_spares());
        assert!(spare_shields("tv_spare_1"));
        assert!(!spare_shields("tv_spare_2"));

        // peek matches by template NAME and clones (the entry keeps
        // shielding until remove_spare).
        assert!(add_spare(sp("tv_spare_2", 90402, "tpl_b", 90410)));
        let got = peek_spare("tpl_a").expect("tpl_a spare");
        assert_eq!((got.name.as_str(), got.oid), ("tv_spare_1", 90401));
        assert_eq!(got.template_oid, 90400);
        assert!(spare_shields("tv_spare_1"), "peek must not remove");
        assert!(peek_spare("tpl_zzz").is_none());

        // Identity-filtered count: same name + same oid only.
        assert!(add_spare(sp("tv_spare_3", 90403, "tpl_a", 90400)));
        assert_eq!(spare_count("tpl_a", 90400), 2);
        assert_eq!(spare_count("tpl_a", 90499), 0, "rebuilt-template oid mismatch");
        assert_eq!(spare_count("tpl_b", 90410), 1);

        // Per-template stale drain: with only tpl_b's identity valid, the
        // tpl_a spares drain and the tpl_b spare is kept — dropping/
        // unsealing/resealing ONE template drains ITS spares only (the
        // item-A per-template drain contract).
        let stale = drain_stale_spares(&[("tpl_b".to_string(), 90410, false)]);
        let mut names: Vec<&str> = stale.iter().map(|s| s.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, ["tv_spare_1", "tv_spare_3"]);
        assert!(spare_shields("tv_spare_2"));
        assert!(!spare_shields("tv_spare_1"), "drained spares stop shielding");
        // Same name, NEW template oid (template rebuilt under its name):
        // stale too.
        let stale = drain_stale_spares(&[("tpl_b".to_string(), 90411, false)]);
        assert_eq!(stale.len(), 1);
        assert!(!any_spares());

        // Multi-template keep: both identities listed, both spares stay.
        assert!(add_spare(sp("tv_spare_m1", 90451, "tpl_m1", 90450)));
        assert!(add_spare(sp("tv_spare_m2", 90453, "tpl_m2", 90452)));
        let stale = drain_stale_spares(&[
            ("tpl_m1".to_string(), 90450, false),
            ("tpl_m2".to_string(), 90452, false),
        ]);
        assert!(stale.is_empty(), "matching spares of BOTH templates survive");
        assert_eq!(drain_stale_spares(&[]).len(), 2, "empty identity set drains all");

        // A datallowconn EDGE drains (either direction: a spare minted from
        // a sealed template with the template now observed connectable, and
        // vice versa); a STABLE datallowconn keeps the spare (the
        // always-connectable template1-shape pool, documented staleness).
        assert!(add_spare(SpareEntry {
            name: "tv_spare_c1".to_string(),
            oid: 90441,
            template_name: "tpl_c".to_string(),
            template_oid: 90440,
            template_connectable: false,
        }));
        assert!(add_spare(SpareEntry {
            name: "tv_spare_c2".to_string(),
            oid: 90442,
            template_name: "tpl_c".to_string(),
            template_oid: 90440,
            template_connectable: true,
        }));
        let stale = drain_stale_spares(&[("tpl_c".to_string(), 90440, true)]);
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].name, "tv_spare_c1", "sealed-minted spare drains on the edge");
        assert!(spare_shields("tv_spare_c2"), "connectable-stable spare stays");
        assert_eq!(drain_stale_spares(&[]).len(), 1);

        // Empty identity set = no valid pool: drains everything.
        assert!(add_spare(sp("tv_spare_4", 90404, "tpl_a", 90400)));
        assert_eq!(drain_stale_spares(&[]).len(), 1);
        assert!(!any_spares());

        // Excess drain (pool_size shrink): keeps the OLDEST `keep`
        // identity-matching spares, returns the rest, never touches other
        // identities.
        assert!(add_spare(sp("tv_spare_e1", 90421, "tpl_e", 90420)));
        assert!(add_spare(sp("tv_spare_e2", 90422, "tpl_e", 90420)));
        assert!(add_spare(sp("tv_spare_e3", 90423, "tpl_e", 90420)));
        assert!(add_spare(sp("tv_spare_o1", 90431, "tpl_o", 90430)));
        let excess = take_excess_spares("tpl_e", 90420, 1);
        let mut names: Vec<&str> = excess.iter().map(|s| s.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, ["tv_spare_e2", "tv_spare_e3"]);
        assert!(spare_shields("tv_spare_e1"), "oldest survivor stays");
        assert!(spare_shields("tv_spare_o1"), "other identities untouched");
        assert!(take_excess_spares("tpl_e", 90420, 1).is_empty());
        assert_eq!(drain_stale_spares(&[]).len(), 2);

        // remove_spare reports presence; the seq is monotonic (burned names
        // never reused).
        assert!(add_spare(sp("tv_spare_5", 90405, "tpl_a", 90400)));
        assert!(remove_spare("tv_spare_5"));
        assert!(!remove_spare("tv_spare_5"));
        let s1 = next_spare_seq();
        let s2 = next_spare_seq();
        assert!(s2 > s1);

        // Capacity: the table is bounded and overflow is a silent skip-add
        // (fail-safe: an unregistered spare just reaps). Unit tests run
        // without the env override, so the cap is MAX_SPARES.
        assert_eq!(max_spares_cap(), MAX_SPARES);
        for i in 0..MAX_SPARES {
            assert!(add_spare(sp(&format!("tv_spare_f{i}"), 91000 + i as Oid, "tpl_f", 90900)));
        }
        assert!(!add_spare(sp("tv_spare_overflow", 91999, "tpl_f", 90900)));
        assert_eq!(total_spares(), MAX_SPARES);
        assert_eq!(drain_stale_spares(&[]).len(), MAX_SPARES);

        // Pooled-template set: idempotent registration in first-mint order,
        // bounded, delistable; the rr cursor rotates over the live count.
        assert!(note_pooled_template("tpl_p1"));
        assert!(!note_pooled_template("tpl_p1"), "idempotent");
        assert!(note_pooled_template("tpl_p2"));
        assert_eq!(pooled_templates(), ["tpl_p1", "tpl_p2"]);
        assert_eq!(advance_pool_rr(2), 0);
        assert_eq!(advance_pool_rr(2), 1);
        assert_eq!(advance_pool_rr(2), 0, "rr wraps");
        assert!(remove_pooled_template("tpl_p1"));
        assert!(!remove_pooled_template("tpl_p1"));
        assert_eq!(pooled_templates(), ["tpl_p2"]);
        assert_eq!(advance_pool_rr(1), 0);
        assert!(remove_pooled_template("tpl_p2"));
        for i in 0..MAX_POOLED_TEMPLATES {
            assert!(note_pooled_template(&format!("tpl_pf{i}")));
        }
        assert!(
            !note_pooled_template("tpl_p_overflow"),
            "set overflow is a silent skip (QoS, never an error)"
        );
        for i in 0..MAX_POOLED_TEMPLATES {
            assert!(remove_pooled_template(&format!("tpl_pf{i}")));
        }
        assert_eq!(advance_pool_rr(0), 0);

        // Flush marks: unmarked -> no match; mark -> exact-identity match;
        // an advanced datfrozenxid OR datminmxid (completed wraparound
        // autovacuum) reads unmarked; re-mark updates in place; clear
        // (observed-unsealed invalidation) removes.
        assert!(!template_flushed_matches(90400, 700, 1));
        mark_template_flushed(90400, 700, 1);
        assert!(template_flushed_matches(90400, 700, 1));
        assert!(!template_flushed_matches(90400, 800, 1));
        assert!(!template_flushed_matches(90400, 700, 2));
        assert!(!template_flushed_matches(90401, 700, 1));
        mark_template_flushed(90400, 800, 2);
        assert!(template_flushed_matches(90400, 800, 2));
        assert!(!template_flushed_matches(90400, 700, 1));
        clear_template_flushed(90400);
        assert!(!template_flushed_matches(90400, 800, 2));
        // Mark-table overflow is fail-safe: the 65th template just never
        // marks (keeps checkpointing), existing marks intact.
        for i in 0..MAX_TEMPLATE_FLUSH_MARKS {
            mark_template_flushed(92000 + i as Oid, 1, 1);
        }
        mark_template_flushed(93000, 1, 1);
        assert!(!template_flushed_matches(93000, 1, 1));
        assert!(template_flushed_matches(92000, 1, 1));
        // Dead-oid pruning (the reap-pass tail): marks whose template is
        // absent from the live oid set are dropped, live ones survive —
        // deleting the retain call would leak one slot per template
        // rebuild until marking silently stops at the table bound.
        retain_template_flush_marks(&[92000]);
        assert!(template_flushed_matches(92000, 1, 1));
        assert!(!template_flushed_matches(92001, 1, 1), "dead-oid mark pruned");
        retain_template_flush_marks(&[]);
        assert!(!template_flushed_matches(92000, 1, 1));

        // Relcount cache (mint-strategy pick): miss -> None; set -> hit;
        // upsert replaces; the observed-unseal clear drops BOTH the flush
        // mark and the count; dead-oid pruning covers it on the same feed.
        assert_eq!(template_relcount(90500), None);
        set_template_relcount(90500, 231);
        assert_eq!(template_relcount(90500), Some(231));
        set_template_relcount(90500, 260);
        assert_eq!(template_relcount(90500), Some(260));
        mark_template_flushed(90500, 700, 1);
        clear_template_flushed(90500);
        assert_eq!(template_relcount(90500), None, "unseal observation clears the count");
        assert!(!template_flushed_matches(90500, 700, 1));
        set_template_relcount(90501, 5);
        set_template_relcount(90502, 6);
        retain_template_flush_marks(&[90502]);
        assert_eq!(template_relcount(90501), None, "dead-oid relcount pruned");
        assert_eq!(template_relcount(90502), Some(6));
        retain_template_flush_marks(&[]);
        assert_eq!(template_relcount(90502), None);

        // The spare-name-overflow log latch fires exactly once per
        // lifetime (the misconfiguration is permanent: PGC_POSTMASTER
        // prefix, monotonic seq).
        assert!(pool_name_overflow_log_once());
        assert!(!pool_name_overflow_log_once());
    }
}
