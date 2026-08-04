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
}

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
}
