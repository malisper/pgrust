//! D1 `max_active_queries` statement-admission gate
//! (docs/design/connection-scaling.md §D1). pgrust-only.
//!
//! A counting semaphore acquired at the top of statement execution
//! (exec_simple_query and the extended-protocol Execute path), BEFORE any
//! snapshot is taken or lock acquired — a queued statement must not hold a
//! vacuum horizon or sit in the deadlock graph. Released when the statement
//! scope ends, on every path: the guard is a Drop type, and both Err
//! unwinds and panics run drops (main_loop's recovery arms catch them
//! above this frame), so an error mid-query releases the slot.
//!
//! Fairness: waiters park on the ported ConditionVariable, whose wakeup
//! list is a FIFO proclist — `ConditionVariableSignal` wakes the head
//! waiter, one per release (no herd). Admission itself is a CAS against
//! the active count, and a newcomer defers to queued waiters (the
//! WAITERS-nonzero check in the fast path), so service order is FIFO up to
//! the microsecond-scale window between a waiter's wakeup and its CAS —
//! approximate FIFO, with the CV's bounded recheck loop (~1s) as the
//! lost-wakeup/GUC-raise backstop. PGC_SIGHUP: raising the limit takes
//! effect for new admissions immediately and for parked waiters within one
//! recheck.
//!
//! Reentrancy: a thread-local depth counter — only the 0→1 transition can
//! acquire, so SPI/nested execution under an admitted statement never
//! double-acquires (nested paths do not pass through these entry points
//! today; the counter makes that structural).
//!
//! Exemptions, evaluated at acquire time and remembered in the guard so
//! release always balances: gate off (0), superusers (the is_superuser
//! session state — the same signal `superuser_reserved_connections`
//! reserves for, kept current by SET ROLE), walsenders/replication
//! (exec_simple_query serves walsender 'Q' messages), single-user mode,
//! and any thread without a PGPROC (the CV parks via the proc latch).
//! Autovacuum and background workers never pass through tcop statement
//! dispatch, so they are structurally exempt.
//!
//! Observability: waiters show in pg_stat_activity as wait_event_type
//! "Extension", wait_event "MaxActiveQueries" (custom wait event, lazily
//! registered); counters are readable via the `pgrust: admission stats`
//! simple-query debug command (simple_query.rs), which also reports the D6
//! connection-queue counters.

use std::cell::Cell;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicI32, AtomicU64};
use std::sync::OnceLock;
use std::time::Instant;

use condition_variable::{
    ConditionVariable, ConditionVariableCancelSleep, ConditionVariablePrepareToSleep,
    ConditionVariableSignal, ConditionVariableSleep,
};
use types_error::PgResult;

static GATE_CV: ConditionVariable = ConditionVariable::new();

static ACTIVE: AtomicI32 = AtomicI32::new(0);
static WAITERS: AtomicI32 = AtomicI32::new(0);
static TOTAL_WAITS: AtomicU64 = AtomicU64::new(0);
static TOTAL_WAIT_US: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static DEPTH: Cell<u32> = const { Cell::new(0) };
}

fn wait_event() -> u32 {
    static WE: OnceLock<u32> = OnceLock::new();
    *WE.get_or_init(|| {
        waitevent::custom::WaitEventExtensionNew("MaxActiveQueries")
            .unwrap_or(waitevent::PG_WAIT_EXTENSION)
    })
}

/// (active, waiters, total_waits, total_wait_us)
pub fn stats() -> (i32, i32, u64, u64) {
    (
        ACTIVE.load(Relaxed),
        WAITERS.load(Relaxed),
        TOTAL_WAITS.load(Relaxed),
        TOTAL_WAIT_US.load(Relaxed),
    )
}

/// Statement-scope admission guard. Hold it for the life of the statement;
/// drop releases the slot (or only the reentrancy depth if this level was
/// nested/exempt) on every exit path, including error unwinds and panics.
pub struct StatementAdmission {
    acquired: bool,
}

impl Drop for StatementAdmission {
    fn drop(&mut self) {
        DEPTH.with(|d| d.set(d.get() - 1));
        if self.acquired {
            ACTIVE.fetch_sub(1, Relaxed);
            if WAITERS.load(Relaxed) > 0 {
                ConditionVariableSignal(&GATE_CV);
            }
        }
    }
}

fn try_admit(limit: i32, defer_to_waiters: bool) -> bool {
    loop {
        if defer_to_waiters && WAITERS.load(Relaxed) > 0 {
            return false; // no barging past the FIFO
        }
        let cur = ACTIVE.load(Relaxed);
        if cur >= limit {
            return false;
        }
        if ACTIVE
            .compare_exchange_weak(cur, cur + 1, Relaxed, Relaxed)
            .is_ok()
        {
            return true;
        }
    }
}

pub fn acquire_for_statement() -> PgResult<StatementAdmission> {
    let depth = DEPTH.with(|d| {
        let v = d.get();
        d.set(v + 1);
        v
    });
    // From here every early return / `?` must be balanced by the guard.
    let mut guard = StatementAdmission { acquired: false };

    if depth > 0 {
        return Ok(guard); // nested execution: the top level holds the slot
    }

    let limit = guc_tables::backing::max_active_queries();
    if limit <= 0 {
        return Ok(guard); // gate off
    }

    // Exemptions (see module comment).
    if guc_tables::backing::current_role_is_superuser()
        || walsender_seams::am_walsender()
        || !init_small::globals::IsUnderPostmaster()
        || lmgr_proc::MyProc().is_none()
    {
        return Ok(guard);
    }

    if try_admit(limit, true) {
        guard.acquired = true;
        return Ok(guard);
    }

    // Slow path: FIFO-park on the CV until a release signals us (head
    // first) or the recheck tick re-runs the CAS.
    WAITERS.fetch_add(1, Relaxed);
    TOTAL_WAITS.fetch_add(1, Relaxed);
    let started = Instant::now();
    ConditionVariablePrepareToSleep(&GATE_CV);
    let outcome = loop {
        // Re-read the limit each pass: PGC_SIGHUP raises (or disables)
        // reach parked waiters within one CV recheck tick.
        let limit = guc_tables::backing::max_active_queries();
        if limit <= 0 {
            break Ok(false); // gate disabled while we waited: pass, uncounted
        }
        // We ARE the queue here — admit on capacity alone.
        if try_admit(limit, false) {
            break Ok(true);
        }
        // Sleeps report the MaxActiveQueries wait event and check for
        // interrupts, so cancel/terminate unwind out of the wait.
        if let Err(e) = ConditionVariableSleep(&GATE_CV, wait_event()) {
            break Err(e);
        }
    };
    let signaled_while_leaving = ConditionVariableCancelSleep();
    WAITERS.fetch_sub(1, Relaxed);
    TOTAL_WAIT_US.fetch_add(started.elapsed().as_micros() as u64, Relaxed);
    // A wake token consumed on our way out belongs to the next waiter.
    if signaled_while_leaving && WAITERS.load(Relaxed) > 0 {
        ConditionVariableSignal(&GATE_CV);
    }
    guard.acquired = outcome?; // cancel/die while queued: guard drop rebalances depth only
    Ok(guard)
}
