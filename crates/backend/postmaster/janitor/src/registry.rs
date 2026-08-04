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

use types_core::ProcNumber;
use types_error::{PgError, PgResult, ERRCODE_CONFIGURATION_LIMIT_EXCEEDED};

/// Fixed pin-table capacity. Ephemeral databases are per-test-worker scoped;
/// a suite pinning more than this many at once is holding the janitor wrong
/// (the error says so). Small on purpose: the table is scanned linearly
/// under the lock on every reap tick.
pub const MAX_PINS: usize = 64;

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
}

pgsync::process_global! {
    static REGISTRY: pgsync::Mutex<RegistryState> = pgsync::Mutex::new(RegistryState {
        paused: false,
        sweep_pending: false,
        janitor_proc: None,
        pins: Vec::new(),
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
}
