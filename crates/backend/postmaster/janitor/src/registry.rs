//! The janitor's shared registry: pin table + pause state + wakeup handle.
//!
//! "Shmem" in the thread-per-backend port is a process-global static (the
//! autovacuum shmem.rs / launcher CTX precedent): one `pgsync::Mutex` IS the
//! LWLock, visible to every backend thread and the janitor by construction.
//! Everything here is restart-lossy BY DESIGN (spec D1 items 3 and 5):
//! pins and the pause flag protect state within one postmaster lifetime;
//! durability across restarts is the marker file's job (marker.rs) and,
//! for a database that must survive restarts, `ALTER DATABASE ... RENAME`
//! out of the prefix.

use types_core::{Oid, ProcNumber};
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
/// Sizing rationale (the CI cluster STORM_N=200 capacity cliff, 2026-08-04):
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

/// Fixed per-template grace-override capacity (the MAX_PINS shape: bounded,
/// linear scan under the lock, reject-loud on overflow). Same capacity-cliff
/// audit verdict as MAX_PINS (2026-08-04): overrides are USER-driven — one
/// `pgrust_set_template_grace()` call per template — never
/// concurrency-scaled, and the overflow error is clean and actionable
/// (clear an override).
pub const MAX_TEMPLATE_GRACES: usize = 64;

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
    /// Adoption guard is up: Ensures are rejected immediately (spec item 4).
    JanitorPaused,
    /// live + in-flight minted databases for this role reached the cap.
    PerRoleCap { counted: usize, max: i32 },
    /// The Ensure table is full — an invariant violation, not a load
    /// condition, since `ensure_capacity()` sizes the table from
    /// max_connections (see its rationale). Carried with the capacity so
    /// the FATAL can name it without re-deriving.
    TableFull { cap: usize },
}

/// Janitor-side view of a pending entry.
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

/// Fixed spare-table capacity (D3 warm pool): the hard ceiling
/// pgrust.ephemeral_db_pool_size is clamped under (the GUC's own max is
/// this value). Same capacity-cliff audit verdict as MAX_PINS: the pool is
/// OPERATOR-sized (one GUC), never concurrency-scaled, and overflow is a
/// silent skip-add (the replenisher simply stops early), not an error a
/// connect path can hit.
pub const MAX_SPARES: usize = 64;

/// One pre-minted spare clone of the default template (D3 warm pool),
/// restart-lossy like everything here: post-restart leftovers are
/// unregistered survivors the startup sweep drops, and the pool cold-starts
/// empty and replenishes.
#[derive(Clone)]
pub struct SpareEntry {
    /// The spare's current datname (`<prefix>spare_<seq>`).
    pub name: String,
    /// Its pg_database oid (preserved across the handout RENAME).
    pub oid: Oid,
    /// Template identity AT MINT TIME: name + oid. A default-template
    /// repoint (name changes) or rebuild (same name, new oid) makes the
    /// spare STALE — `drain_stale_spares` removes it and the replenisher
    /// drops the database.
    pub template_name: String,
    pub template_oid: Oid,
    /// The template's datallowconn AT MINT TIME. A datallowconn EDGE
    /// (sealed template unsealed-for-writes then re-sealed, or a writable
    /// template1-shape template sealed) observed by the replenish probe or
    /// the handout re-check makes the spare STALE too: its copied content
    /// predates a window in which ordinary connections could write the
    /// template. Both-connectable spares are kept — an always-connectable
    /// default template serves spares whose content is as old as their
    /// mint, the documented pool staleness residual (addendum item 6); an
    /// unseal-reseal cycle wholly between janitor observations remains
    /// invisible, mirroring the flush-mark discipline.
    pub template_connectable: bool,
}

/// Fixed capacity of the sealed-template flush-mark table (batch
/// pre-checkpoint skip). Overflow is fail-safe: an unmarkable template
/// simply keeps paying the pre-checkpoint.
pub const MAX_TEMPLATE_FLUSH_MARKS: usize = 64;

struct RegistryState {
    /// Adoption-guard pause (spec item 4): while true the janitor performs
    /// no sweep and no reaping. D2's mint path must reject Ensures
    /// immediately while paused — `is_paused()` is that contract stub.
    paused: bool,
    /// One-shot deferred-startup-sweep request, set by unpause.
    sweep_pending: bool,
    /// The janitor's PGPROC number while it is running (launcher_pid
    /// precedent): lets `pgrust_janitor_unpause()` wake the loop instead of
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
    /// D2 per-template grace overrides, seconds, keyed by template name
    /// (restart-lossy like pins, spec D2).
    template_graces: Vec<(String, i32)>,
    /// D3 warm-pool spares (bounded by MAX_SPARES). Mutated ONLY from the
    /// janitor loop (replenish/handout), read by the reap/sweep shields.
    spares: Vec<SpareEntry>,
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
}

pgsync::process_global! {
    static REGISTRY: pgsync::Mutex<RegistryState> = pgsync::Mutex::new(RegistryState {
        paused: false,
        sweep_pending: false,
        janitor_proc: None,
        pins: Vec::new(),
        ensures: Vec::new(),
        next_ensure_gen: 1,
        template_graces: Vec::new(),
        spares: Vec::new(),
        next_spare_seq: 1,
        template_flush_marks: Vec::new(),
        template_relcounts: Vec::new(),
        pool_name_overflow_logged: false,
        touch_queue: Vec::new(),
        touch_inflight: Vec::new(),
        catalog_churn: 0,
    });
}

fn with_registry<R>(f: impl FnOnce(&mut RegistryState) -> R) -> R {
    let mut guard = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    f(&mut guard)
}

pgsync::process_global! {
    static UNPAUSE_LOCK: pgsync::Mutex<()> = pgsync::Mutex::new(());
}

/// Serialize `pgrust_janitor_unpause()` end-to-end: paused-state check +
/// durable marker write + state flip run under one lock. Two concurrent
/// unpause calls otherwise race on the marker's single fixed temp path
/// (marker.rs): an interleaving could durably rename a not-yet-written temp
/// file into place (an empty marker — fail-safe, the guard re-pauses on
/// restart, but a defect). The registry mutex cannot cover this — the
/// registry accessors re-lock it internally and marker I/O (fsyncs) must
/// not run under it — hence a dedicated lock.
pub fn with_unpause_lock<R>(f: impl FnOnce() -> R) -> R {
    let _guard = UNPAUSE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    f()
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

/// The paused-state contract point: D1's guard sets it, D2's mint path must
/// consult it (Ensures against a paused janitor get an immediate clean
/// FATAL, never a hang).
pub fn is_paused() -> bool {
    with_registry(|r| r.paused)
}

/// Is a janitor currently registered? Waiter-side belt-and-suspenders
/// (mint::wait_for_mint): the exit drain fails pending entries, but a
/// wedge between death and drain must not cost waiters the full deadline.
pub fn janitor_present() -> bool {
    with_registry(|r| r.janitor_proc.is_some())
}

pub(crate) fn set_paused(paused: bool) {
    with_registry(|r| r.paused = paused);
}

/// Request the deferred startup sweep (unpause path).
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
/// registry lock: janitor-present check, paused check, same-name join,
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
        if r.paused {
            return PostEnsure::JanitorPaused;
        }
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

/// Paused-drain variant: fail the pending entries ONLY IF the registry is
/// still paused, re-checked under the SAME lock post_ensure admits entries
/// under. Without the re-check, a drain launched by a tick that observed
/// paused==true races `pgrust_janitor_unpause()`: the flag flips, a fresh
/// Ensure is legitimately admitted at post_ensure's paused check, and the
/// already-in-flight drain would then fail it with a wrong-cause "paused"
/// FATAL right after a successful unpause. Skipping is hang-free: once
/// unpaused, the next tick's service pass handles whatever is pending.
pub fn fail_pending_ensures_if_paused(err: &PgError, now_ns: u64) -> Vec<ProcNumber> {
    with_registry(|r| {
        if !r.paused {
            return Vec::new();
        }
        fail_pending_locked(r, err, now_ns)
    })
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
// D2 per-template grace overrides.
// ---------------------------------------------------------------------------

/// Set (secs >= 0) or clear (secs < 0) the reap-grace override for clones
/// of `template` (names matching `<prefix><template>__<token>`; keyed on
/// the RESOLVED catalog datname — builtins.rs resolves, same rationale as
/// pins). Returns true when an override is now in place, false when the
/// call cleared (or found nothing to clear). Errors only on table overflow.
pub fn set_template_grace(template: &str, secs: i32) -> PgResult<bool> {
    with_registry(|r| {
        if secs < 0 {
            r.template_graces.retain(|(t, _)| t != template);
            return Ok(false);
        }
        if let Some(slot) = r.template_graces.iter_mut().find(|(t, _)| t == template) {
            slot.1 = secs;
            return Ok(true);
        }
        if r.template_graces.len() >= MAX_TEMPLATE_GRACES {
            return Err(Box::new(
                PgError::error(format!(
                    "cannot set grace for template \"{template}\": the override table is full \
                     ({MAX_TEMPLATE_GRACES} entries)"
                ))
                .with_sqlstate(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
            ));
        }
        r.template_graces.push((template.to_string(), secs));
        Ok(true)
    })
}

/// The override for `template`, if any (reap pass, per row).
pub fn template_grace_override(template: &str) -> Option<i32> {
    with_registry(|r| {
        r.template_graces
            .iter()
            .find(|(t, _)| t == template)
            .map(|&(_, s)| s)
    })
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
        if r.spares.len() >= MAX_SPARES || r.spares.iter().any(|s| s.name == e.name) {
            return false;
        }
        r.spares.push(e);
        true
    })
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

/// Remove and return every spare NOT matching `identity` ((template name,
/// template oid, template datallowconn AS OBSERVED NOW)); `None` = no
/// valid pool (feature off, template unset/missing/unsealed) drains ALL
/// spares. The datallowconn term drains spares across a connectable EDGE
/// (either direction — see `SpareEntry::template_connectable`); a stable
/// datallowconn keeps them. The caller drops the returned databases via
/// the batch drop path.
pub(crate) fn drain_stale_spares(identity: Option<(&str, Oid, bool)>) -> Vec<SpareEntry> {
    with_registry(|r| {
        let (keep, stale): (Vec<SpareEntry>, Vec<SpareEntry>) =
            r.spares.drain(..).partition(|s| match identity {
                Some((name, oid, allowconn)) => {
                    s.template_name == name
                        && s.template_oid == oid
                        && s.template_connectable == allowconn
                }
                None => false,
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

        // Pause + one-shot sweep request.
        assert!(!is_paused());
        set_paused(true);
        assert!(is_paused());
        set_paused(false);
        assert!(!is_paused());
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
    fn ensure_and_grace_semantics() {
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

        // Paused janitor: rejected (spec item 4 / D2).
        set_paused(true);
        assert!(matches!(post("tv_e_a", 1, &[], 0), PostEnsure::JanitorPaused));
        set_paused(false);

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

        // The paused-drain variant fails pending entries ONLY while
        // actually paused, re-checked under the lock: an unpause racing an
        // in-flight drain must not fail a legitimately-admitted fresh
        // Ensure with a wrong-cause "paused" FATAL.
        let PostEnsure::Posted(gen_p) = post("tv_e_pd", 6, &[], 0) else {
            panic!("expected Posted");
        };
        let cause = PgError::error("paused".to_string());
        assert!(
            fail_pending_ensures_if_paused(&cause, now).is_empty(),
            "unpaused: the conditional drain must be a no-op"
        );
        assert!(matches!(ensure_status(gen_p), EnsureStatus::Pending));
        set_paused(true);
        assert_eq!(fail_pending_ensures_if_paused(&cause, now), vec![6]);
        set_paused(false);
        assert!(matches!(ensure_status(gen_p), EnsureStatus::Failed(_)));
        remove_ensure_waiter(gen_p, 6);
        gc_ensures(now + ENSURE_LINGER_NS + 1);
        assert!(matches!(ensure_status(gen_p), EnsureStatus::Gone));

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

        // Template grace overrides: set, replace, lookup, clear, overflow.
        assert_eq!(template_grace_override("tpl_g"), None);
        assert!(set_template_grace("tpl_g", 30).unwrap());
        assert_eq!(template_grace_override("tpl_g"), Some(30));
        assert!(set_template_grace("tpl_g", 0).unwrap());
        assert_eq!(template_grace_override("tpl_g"), Some(0));
        assert!(!set_template_grace("tpl_g", -1).unwrap());
        assert_eq!(template_grace_override("tpl_g"), None);
        for i in 0..MAX_TEMPLATE_GRACES {
            set_template_grace(&format!("tpl_fill_{i}"), 1).unwrap();
        }
        let overflow = set_template_grace("tpl_overflow", 1).unwrap_err();
        assert!(overflow.message().contains("override table is full"));
        for i in 0..MAX_TEMPLATE_GRACES {
            set_template_grace(&format!("tpl_fill_{i}"), -1).unwrap();
        }

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

        // Stale drain: a repointed default template (identity = tpl_b)
        // drains the tpl_a spares and keeps the match.
        let stale = drain_stale_spares(Some(("tpl_b", 90410, false)));
        let mut names: Vec<&str> = stale.iter().map(|s| s.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, ["tv_spare_1", "tv_spare_3"]);
        assert!(spare_shields("tv_spare_2"));
        assert!(!spare_shields("tv_spare_1"), "drained spares stop shielding");
        // Same name, NEW template oid (template rebuilt under its name):
        // stale too.
        let stale = drain_stale_spares(Some(("tpl_b", 90411, false)));
        assert_eq!(stale.len(), 1);
        assert!(!any_spares());

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
        let stale = drain_stale_spares(Some(("tpl_c", 90440, true)));
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].name, "tv_spare_c1", "sealed-minted spare drains on the edge");
        assert!(spare_shields("tv_spare_c2"), "connectable-stable spare stays");
        assert_eq!(drain_stale_spares(None).len(), 1);

        // None = no valid pool: drains everything.
        assert!(add_spare(sp("tv_spare_4", 90404, "tpl_a", 90400)));
        assert_eq!(drain_stale_spares(None).len(), 1);
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
        assert_eq!(drain_stale_spares(None).len(), 2);

        // remove_spare reports presence; the seq is monotonic (burned names
        // never reused).
        assert!(add_spare(sp("tv_spare_5", 90405, "tpl_a", 90400)));
        assert!(remove_spare("tv_spare_5"));
        assert!(!remove_spare("tv_spare_5"));
        let s1 = next_spare_seq();
        let s2 = next_spare_seq();
        assert!(s2 > s1);

        // Capacity: the table is bounded and overflow is a silent skip-add
        // (fail-safe: an unregistered spare just reaps).
        for i in 0..MAX_SPARES {
            assert!(add_spare(sp(&format!("tv_spare_f{i}"), 91000 + i as Oid, "tpl_f", 90900)));
        }
        assert!(!add_spare(sp("tv_spare_overflow", 91999, "tpl_f", 90900)));
        assert_eq!(drain_stale_spares(None).len(), MAX_SPARES);

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
