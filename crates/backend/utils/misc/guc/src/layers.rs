// GUC layered immutable snapshots — docs/design/parallelism-redesign-2026-07.md
// §2.4 (standalone M4 item, pulled forward).
//
// Layers, bottom to top:
//
//   BASE      one process-wide Arc<GucBaseSnapshot>: the postmaster's config
//             state (boot defaults + environment + argv + config file),
//             captured typed (validated values + check-hook extras, no string
//             round-trip). A SIGHUP reload publishes a NEW base atomically;
//             any thread holding the old Arc keeps a stable, torn-read-free
//             view. This retires the reload dead-diff hazard class
//             (notes/recovery-standby-tail-state.md): a thread child that
//             diffs pre/post-reload GUCs compares ITS OWN Arcs, never a
//             process-global backing the postmaster rewrites underneath it.
//
//   SESSION   the per-thread GucRegistry (guc::store) is, unchanged, the
//             session's delta layer: SET/RESET/SET LOCAL live there with full
//             C source/priority semantics. SESSION_BASE additionally records
//             which base this thread last adopted — its "started-with" values
//             (the walreceiver reload-diff fix pattern, generalized). A
//             session adopts a newer base only through its own
//             between-statements ProcessConfigFile pass (C parity: reloads
//             apply between statements, never mid-query).
//
//   QUERY PIN Arc<GucQuerySnapshot>: the adopted base Arc plus the session's
//             composed nondefault capture, taken at most once per statement
//             window (cache keyed on the store mutation counter) and shared
//             with parallel workers — and, later, runtime tasks — by Arc
//             clone: no locks, no re-capture per launch, no torn reads.
//
// The guc/guc_tables API surface is unchanged; this module only adds the
// snapshot plumbing. Hot GUC reads stay direct TLS/backing loads.

use std::cell::RefCell;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use types_error::PgResult;

use crate::registry::CapturedGuc;
use crate::store;

/// An immutable capture of one thread's nondefault GUC state (boot defaults
/// are implicit: both sides of any share were initialized from the same
/// static tables). `vars` carries leader-validated values and check-hook
/// extras by Arc, so applying a snapshot never reruns check hooks (§3.4
/// P-guc contract: check hooks are pure validators; assign hooks rerun).
pub struct GucBaseSnapshot {
    epoch: u64,
    vars: Vec<CapturedGuc>,
}

impl GucBaseSnapshot {
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn vars(&self) -> &[CapturedGuc] {
        &self.vars
    }

    pub fn contains(&self, name: &str) -> bool {
        self.vars.iter().any(|v| v.name() == name)
    }

    /// Lookup for reload-diff consumers (started-with vs new-base compares).
    /// Linear: the nondefault set is small and this is never on a hot path.
    pub fn get(&self, name: &str) -> Option<&CapturedGuc> {
        self.vars.iter().find(|v| v.name() == name)
    }
}

/// Epoch 0, empty: "boot defaults only". Served until a postmaster publishes;
/// keeps every consumer total (single-user mode, unit rigs).
fn empty_base() -> Arc<GucBaseSnapshot> {
    static EMPTY: OnceLock<Arc<GucBaseSnapshot>> = OnceLock::new();
    EMPTY.get_or_init(|| Arc::new(GucBaseSnapshot { epoch: 0, vars: Vec::new() })).clone()
}

static GUC_BASE: RwLock<Option<Arc<GucBaseSnapshot>>> = RwLock::new(None);

// Single-publisher discipline: the postmaster thread owns base publication.
// `mutations` is the publisher's store mutation counter at last publish —
// the republish-needed test is one counter compare.
struct PublisherState {
    thread: std::thread::ThreadId,
    mutations: u64,
}

static PUBLISHER: Mutex<Option<PublisherState>> = Mutex::new(None);

/// The current process-wide base. Cheap (one RwLock read + Arc clone) but not
/// hot-path cheap: consumers hold the Arc for their window (session adoption,
/// child launch), they do not re-read per query.
pub fn current_base() -> Arc<GucBaseSnapshot> {
    GUC_BASE
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .unwrap_or_else(empty_base)
}

/// Publisher side (the postmaster thread in production — every call site is
/// on it): republish the base from this thread's store iff the store changed
/// since the last publish, and return the current base. A call from a
/// different thread re-establishes that thread as the publisher and always
/// republishes (its mutation counter is unrelated); readers are safe either
/// way — a published base is immutable, swaps are atomic. This tolerance
/// exists for multi-test binaries where each #[test] thread stands up its
/// own "postmaster".
pub fn ensure_base_current() -> Arc<GucBaseSnapshot> {
    let me = std::thread::current().id();
    let mutations = store::store_mutation_count();
    let mut publisher = PUBLISHER.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(p) = publisher.as_ref() {
        if p.thread == me && p.mutations == mutations {
            drop(publisher);
            return current_base();
        }
    }
    let vars = store::capture_session_gucs();
    let epoch = current_base().epoch + 1;
    let base = Arc::new(GucBaseSnapshot { epoch, vars });
    *GUC_BASE.write().unwrap_or_else(|e| e.into_inner()) = Some(base.clone());
    *publisher = Some(PublisherState { thread: me, mutations });
    base
}

thread_local! {
    // The base this thread last adopted (its started-with values). Sticky:
    // set at child bring-up / worker bind, advanced only by this thread's own
    // ProcessConfigFile pass — a postmaster reload never moves it mid-query.
    static SESSION_BASE: RefCell<Option<Arc<GucBaseSnapshot>>> = const { RefCell::new(None) };
    // Statement-window cache for the query pin, keyed on the store mutation
    // counter: zero captures unless the session's GUC state changed.
    static QUERY_PIN: RefCell<Option<CachedPin>> = const { RefCell::new(None) };
}

struct CachedPin {
    mutations: u64,
    pin: Arc<GucQuerySnapshot>,
}

/// This thread's adopted base. First read adopts the then-current base and
/// stays on it (so pre/post diffs against it are stable) until
/// `adopt_current_base` — called from this thread's own config-reload pass —
/// advances it.
pub fn session_base() -> Arc<GucBaseSnapshot> {
    SESSION_BASE.with(|slot| {
        let mut slot = slot.borrow_mut();
        match slot.as_ref() {
            Some(base) => base.clone(),
            None => {
                let base = current_base();
                *slot = Some(base.clone());
                base
            }
        }
    })
}

pub fn set_session_base(base: Arc<GucBaseSnapshot>) {
    SESSION_BASE.with(|slot| *slot.borrow_mut() = Some(base));
    // A held pin stays valid on its old base by design, but the CACHE must
    // not serve a pre-adoption pin for the next statement (the reload pass
    // usually also bumps the store mutation counter; adoption alone must be
    // sufficient on its own).
    QUERY_PIN.with(|slot| *slot.borrow_mut() = None);
}

/// Adopt the current process-wide base as this thread's session base. Called
/// at the tail of this thread's own ProcessConfigFile(PGC_SIGHUP) pass — the
/// C-parity point where a backend consumes a reload (between statements).
pub fn adopt_current_base() -> Arc<GucBaseSnapshot> {
    let base = current_base();
    set_session_base(base.clone());
    base
}

/// The composed per-query view: the session's adopted base plus its full
/// nondefault capture (a superset of the session-vs-base delta; applying it
/// over the same base reproduces the session state exactly, which is what
/// worker binds need).
pub struct GucQuerySnapshot {
    base: Arc<GucBaseSnapshot>,
    session: Vec<CapturedGuc>,
}

impl GucQuerySnapshot {
    pub fn base(&self) -> &Arc<GucBaseSnapshot> {
        &self.base
    }

    pub fn session_vars(&self) -> &[CapturedGuc] {
        &self.session
    }
}

/// The session's query pin: captured on first use per statement window,
/// reused (one Arc clone) until any store mutation — SET/RESET, transaction
/// end with stacked GUCs, this thread's reload pass — invalidates it.
/// Reloads elsewhere in the process never invalidate a held pin: a running
/// query keeps the view it started with.
pub fn current_query_pin() -> Arc<GucQuerySnapshot> {
    let mutations = store::store_mutation_count();
    QUERY_PIN.with(|slot| {
        let mut slot = slot.borrow_mut();
        if let Some(cached) = slot.as_ref() {
            if cached.mutations == mutations {
                return cached.pin.clone();
            }
        }
        let base = session_base();
        let session = store::capture_session_gucs();
        // Content-equal re-mint DEDUP: SET/RESET churn that lands back on
        // identical session state (a no-op SET, or the benchmark protocol's
        // per-try RESET ALL + identical forcing prefix) bumps the mutation
        // counter but re-mints a content-identical snapshot. Reusing the
        // cached Arc preserves pin IDENTITY — the standing-gang sticky
        // binding key (parallel::query_task_guard keys on Arc::ptr_eq), so
        // gang workers sticky-resume (~10us) instead of full-rebinding
        // (~300us + ~260us eviction) per engagement. Equality is
        // conservative (CapturedGuc::content_eq: exact values, bitwise
        // floats, extras by Arc identity); the cache is already dropped on
        // base adoption (set_session_base), so a dedup hit can never serve
        // a pre-adoption pin. PGRUST_GUC_PIN_DEDUP=0 kills the dedup (the
        // counter-keyed cache above is unchanged).
        if pin_dedup_enabled() {
            if let Some(cached) = slot.as_mut() {
                if Arc::ptr_eq(&cached.pin.base, &base)
                    && cached.pin.session.len() == session.len()
                    && cached
                        .pin
                        .session
                        .iter()
                        .zip(session.iter())
                        .all(|(a, b)| a.content_eq(b))
                {
                    cached.mutations = mutations;
                    return cached.pin.clone();
                }
            }
        }
        let pin = Arc::new(GucQuerySnapshot { base, session });
        *slot = Some(CachedPin { mutations, pin: pin.clone() });
        pin
    })
}

/// Kill switch for the content-equal pin re-mint dedup (default ON;
/// PGRUST_GUC_PIN_DEDUP=0 disables). Read once per process.
fn pin_dedup_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_GUC_PIN_DEDUP").map_or(true, |v| v.trim() != "0"))
}

/// Worker side: apply a leader's pin onto this thread's store (assign hooks
/// fire, check hooks don't — the leader validated) and adopt the pin's base,
/// so the worker's started-with view is byte-for-byte the leader's.
pub fn bind_query_pin(pin: &Arc<GucQuerySnapshot>) -> PgResult<store::SessionGucBinding> {
    let binding = store::bind_session_gucs(pin.session_vars())?;
    set_session_base(pin.base.clone());
    Ok(binding)
}

/// PGRUST_NO_GUC_BASE reverts child launches to the per-launch string
/// capture/re-parse path (check hooks rerun per child) for A/B.
pub fn base_share_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("PGRUST_NO_GUC_BASE").is_none())
}

/// Child bring-up from a shared base (the launch_backend path): applies every
/// base var onto this thread's freshly initialized store — same guard
/// sequence and end-state as the string restore (set_config_option with
/// GUC_ACTION_SET, is_reload=true), minus only the parse + check-hook rerun
/// (the postmaster validated; extras cross by Arc, assign hooks rerun here) —
/// then adopts the base as this thread's started-with view.
pub fn bind_base(base: &Arc<GucBaseSnapshot>) -> PgResult<()> {
    let trace = std::env::var_os("PGRUST_GATHER_TRACE").is_some();
    let t0 = trace.then(std::time::Instant::now);
    for cap in base.vars() {
        let mut deferred: Vec<crate::registry::DeferredAssignHook> = Vec::new();
        // exact=false: the child-launch base bind keeps the legacy restore
        // semantics guc-snapshots was ratified under (source-priority guards
        // active). exact=true is reserved for M0's session-envelope exact
        // replacement (apply_captured_session_gucs_impl), which added the param.
        store::with_store_mut(|reg| {
            crate::registry::bind_captured_guc(reg, cap, &mut deferred, false)
        })
            .expect("bind_base: InitializeGUCOptions must run on this thread first")?;
        for hook in deferred {
            hook();
        }
    }
    set_session_base(base.clone());
    if let Some(t0) = t0 {
        eprintln!(
            "GTRACE guc.bind_base n={} epoch={} total_us={}",
            base.vars().len(),
            base.epoch(),
            t0.elapsed().as_micros()
        );
    }
    Ok(())
}

/// Test-only: drop process-wide publisher state so each test can stand up its
/// own "postmaster" thread. Production code must never call this.
#[doc(hidden)]
pub fn reset_layers_for_tests() {
    *PUBLISHER.lock().unwrap_or_else(|e| e.into_inner()) = None;
    *GUC_BASE.write().unwrap_or_else(|e| e.into_inner()) = None;
    SESSION_BASE.with(|slot| *slot.borrow_mut() = None);
    QUERY_PIN.with(|slot| *slot.borrow_mut() = None);
}
