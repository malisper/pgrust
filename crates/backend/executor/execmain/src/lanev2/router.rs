//! M5 unified admission router (docs/design/m5-planner.md §2) — M5-0 seed.
//!
//! M5-0 ships ONLY the engine switch's executor-side probe: under
//! `pgrust.parallel_engine = runtime` with no process runtime pool the engine
//! degrades to legacy with a LOUD-ONCE server-log line (§2.2). Runtime mode
//! engages nothing at this increment — the M5-1 router (one executor-startup
//! walk dispatching to the six arm engagements, consolidated refusal
//! taxonomy per §2.4, per-class counters + trace surface) lands here next.
//!
//! Layering contract (§2.2, restated where the code lives):
//!   1. `pgrust.parallel_engine` — the product selector (default legacy =
//!      zero behavior change, byte-for-byte);
//!   2. `PGRUST_RUNTIME=1` — the process master switch (pool exists);
//!   3. per-arm pool GUCs + `PGRUST_RUNTIME_*` kills — the DEVELOPER/bench
//!      override layer BENEATH the switch; arm recipes work verbatim in
//!      both engine settings and never consult the switch;
//!   4. each arm's fail-closed shape/binder admission walk.
//! `pgrust.runtime_dop` is consulted ONLY under engine=runtime (by the M5-1
//! router); the per-arm bench getters never read it.

/// Is the runtime engine SELECTED AND SERVICEABLE for this session?
/// False in every legacy-default session at the cost of one TLS read + cmp
/// (the off-path discipline: default returns at the first check).
///
/// Degrade rule (§2.2): engine=runtime with no pool (PGRUST_RUNTIME unset,
/// or the global pool not started) behaves exactly as engine=legacy, with a
/// loud-once LOG line so the misconfiguration is visible in the server log
/// without perturbing client output (regress stays byte-identical: LOG is
/// server-log-only at default client_min_messages).
#[inline]
pub(crate) fn engine_runtime_active() -> bool {
    if !::guc_tables::runtime_pool::parallel_engine_is_runtime() {
        return false;
    }
    engine_runtime_pool_ready()
}

/// The pool-availability half of [`engine_runtime_active`], split out so the
/// loud-once emission is testable and off the inline path.
#[cold]
fn engine_runtime_pool_ready() -> bool {
    if runtime::runtime_enabled() && runtime::global().is_some() {
        return true;
    }
    static WARNED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    WARNED.get_or_init(|| {
        // Loud-once to stderr — the postmaster/server log, the same channel
        // the runtime lifecycle traces use (elog is deliberately not a lib
        // dependency of execmain). Server-log-only keeps client output — and
        // therefore regress under engine=runtime — byte-identical.
        eprintln!(
            "[m5-router] pgrust.parallel_engine=runtime but the runtime pool is \
             unavailable (PGRUST_RUNTIME is not 1); degrading to the legacy engine"
        );
    });
    false
}
