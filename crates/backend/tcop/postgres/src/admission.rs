//! D1 `max_active_queries` TRANSACTION-admission gate
//! (docs/design/connection-scaling.md §D1). pgrust-only.
//!
//! A counting semaphore consulted at the top of statement execution
//! (exec_simple_query and the extended-protocol Execute path), BEFORE any
//! snapshot is taken or lock acquired. The grain is the TRANSACTION, not
//! the statement: a session acquires its slot at the first statement of a
//! transaction and holds it until the transaction ends (commit, rollback,
//! pipeline Sync, error recovery of an implicit transaction, or backend
//! exit). Statements inside an already-admitted transaction NEVER consult
//! the gate and NEVER park.
//!
//! Why transaction grain (SOAK3 fix; validate/connscale
//! notes/connscale-validation-vs-pgbouncer.md §6a): the original
//! statement-grain gate released the slot between the statements of an
//! open transaction. A multi-statement transaction holds row locks across
//! that window, so under RW oversubscription its NEXT statement parked in
//! the admission FIFO behind the very statements blocked on its locks —
//! lock holders waiting behind lock waiters, invisible to the deadlock
//! detector (the gate CV is not a lock), permanent. At transaction grain a
//! parked waiter is always at a transaction boundary and by construction
//! holds no locks, which removes the deadlock class entirely and matches
//! pgbouncer transaction-pooling semantics (a transaction occupies its
//! pool slot from first statement to transaction end). Consequences,
//! accepted and intentional: an idle-in-transaction session keeps its slot
//! (it holds locks; idle_in_transaction_session_timeout is the standing
//! mitigation, exactly as with pgbouncer's pinned server connection), and
//! an explicit transaction in the aborted state keeps its slot until the
//! client sends ROLLBACK.
//!
//! Safety valve: a live transaction that reaches the gate WITHOUT a slot —
//! the gate was armed by SIGHUP mid-transaction, or an exemption lapsed
//! mid-transaction (e.g. superuser SET ROLE to a non-superuser) — may
//! already hold locks, so it runs uncounted rather than parking; the gate
//! reconverges at that session's next transaction. Extended-protocol
//! pipelining is part of the same rule: the first Execute of a transaction
//! gates (blockstate still TBLOCK_STARTED, no pipelined statement has
//! completed), later statements before Sync ride the XACT_FLAGS_PIPELINING
//! arm of [`txn_still_open`] and the slot releases at the Sync that ends
//! the pipeline's transaction.
//!
//! Release points (the slot outlives any single statement frame, so RAII
//! alone no longer suffices): (1) the statement guard's drop, when the
//! transaction is over by then — the single-statement/autocommit path;
//! (2) the Sync arm of the main loop, after finish_xact_command ends a
//! pipeline's implicit transaction; (3) error recovery, after
//! AbortCurrentTransaction ends a single-statement or implicit
//! transaction (an explicit block survives in TBLOCK_ABORT and keeps its
//! slot until ROLLBACK); (4) an on_proc_exit callback, armed when a
//! session first takes a slot, which covers FATAL exits —
//! pg_terminate_backend, client EOF mid-transaction — so terminated
//! sessions always return their slot (the §6a-verified cleanup property).
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
//! recheck; lowering (or disabling) it never orphans a held slot — release
//! is driven by SLOT_HELD, not by the current limit.
//!
//! Reentrancy: a thread-local depth counter — only the 0→1 transition can
//! acquire, so SPI/nested execution under an admitted statement never
//! double-acquires (nested paths do not pass through these entry points
//! today; the counter makes that structural).
//!
//! Exemptions, evaluated at acquire time: gate off (0), superusers (the
//! is_superuser session state — the same signal
//! `superuser_reserved_connections` reserves for, kept current by SET
//! ROLE), walsenders/replication (exec_simple_query serves walsender 'Q'
//! messages), single-user mode, and any thread without a PGPROC (the CV
//! parks via the proc latch). Autovacuum and background workers never pass
//! through tcop statement dispatch, so they are structurally exempt.
//! Release stays balanced under all of them because only SLOT_HELD
//! sessions ever decrement ACTIVE.
//!
//! Observability: waiters show in pg_stat_activity as wait_event_type
//! "Extension", wait_event "MaxActiveQueries" (custom wait event, lazily
//! registered); counters are readable via the `pgrust: admission stats`
//! simple-query debug command (simple_query.rs), which also reports the D6
//! connection-queue counters. `active` counts admitted transactions.

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
    /// This session's open transaction holds an admission slot. Set on
    /// admission, cleared at the release points in the module comment.
    static SLOT_HELD: Cell<bool> = const { Cell::new(false) };
    /// The proc-exit release callback is registered for this backend
    /// (registered once, on the first slot this session ever takes).
    static EXIT_HOOK_ARMED: Cell<bool> = const { Cell::new(false) };
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

/// The session's current transaction (or extended-protocol pipeline) is
/// still open at a statement boundary: an explicit or implicit transaction
/// block is in progress (BEGIN..COMMIT, multi-statement simple message,
/// aborted block awaiting ROLLBACK), or a pipelined statement completed in
/// a transaction command that has not finished (XACT_FLAGS_PIPELINING is
/// set by exec_execute_message before the implicit block is materialized
/// by the next start_xact_command, so blockstate alone is not enough).
fn txn_still_open() -> bool {
    // An extended-protocol Execute arrives with the transaction command
    // already started by Parse/Bind (relation locks and a registered
    // snapshot held): parking a lock holder behind the gate is the deadlock
    // the deadlock detector cannot see, so any started command bypasses.
    xact::IsTransactionBlock() || crate::xact_started()
}

fn release_slot() {
    SLOT_HELD.with(|s| s.set(false));
    ACTIVE.fetch_sub(1, Relaxed);
    if WAITERS.load(Relaxed) > 0 {
        ConditionVariableSignal(&GATE_CV);
    }
}

/// Transaction-end release probe: returns the slot iff this session holds
/// one and its transaction is over. Cheap no-op otherwise; callable from
/// any of the release points (guard drop, Sync arm, error recovery).
pub(crate) fn release_if_txn_over() {
    if !SLOT_HELD.with(|s| s.get()) {
        return;
    }
    if txn_still_open() {
        return;
    }
    release_slot();
}

/// on_proc_exit: a dying backend (clean Terminate, FATAL, terminated by
/// admin) returns its slot regardless of transaction state — locks are
/// torn down by the same exit walk, so nothing can wait on us afterwards.
fn release_on_proc_exit(_code: i32, _arg: usize) {
    if SLOT_HELD.with(|s| s.get()) {
        release_slot();
    }
}

fn hold_slot() {
    SLOT_HELD.with(|s| s.set(true));
    EXIT_HOOK_ARMED.with(|h| {
        if !h.get() {
            h.set(true);
            ipc::on_proc_exit(release_on_proc_exit, 0);
        }
    });
}

/// Statement-scope admission guard. Its drop is release point (1): when the
/// statement scope ends — on every path: Ok, Err unwind, panic (main_loop's
/// recovery arms catch them above this frame) — the slot is returned iff
/// the session's transaction is over. Slot ownership itself lives in the
/// session (SLOT_HELD), not in the guard, because a transaction outlives
/// its statements.
pub struct StatementAdmission {
    _priv: (),
}

impl Drop for StatementAdmission {
    fn drop(&mut self) {
        let depth = DEPTH.with(|d| {
            let v = d.get() - 1;
            d.set(v);
            v
        });
        if depth == 0 {
            release_if_txn_over();
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
    let guard = StatementAdmission { _priv: () };

    if depth > 0 {
        return Ok(guard); // nested execution: the top level owns admission
    }

    let limit = guc_tables::backing::max_active_queries();
    if limit <= 0 {
        // Gate off. A slot held from before a SIGHUP disable still drains
        // through the guard drop's release_if_txn_over.
        return Ok(guard);
    }

    // Transaction grain: this session's open transaction was already
    // admitted; its statements run without consulting the gate.
    if SLOT_HELD.with(|s| s.get()) {
        return Ok(guard);
    }

    // Exemptions (see module comment).
    if guc_tables::backing::current_role_is_superuser()
        || walsender_seams::am_walsender()
        || !init_small::globals::IsUnderPostmaster()
        || lmgr_proc::MyProc().is_none()
    {
        return Ok(guard);
    }

    // Safety valve (see module comment): a live transaction with no slot
    // may hold locks — run it uncounted, NEVER park it.
    if txn_still_open() {
        return Ok(guard);
    }

    if try_admit(limit, true) {
        hold_slot();
        return Ok(guard);
    }

    // Slow path: FIFO-park on the CV until a release signals us (head
    // first) or the recheck tick re-runs the CAS. We are at a transaction
    // boundary with no slot: by construction we hold no locks and no
    // snapshot while parked.
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
    // Cancel/die while queued: `?` propagates and the guard drop rebalances
    // depth only (SLOT_HELD is still false).
    if outcome? {
        hold_slot();
    }
    Ok(guard)
}
