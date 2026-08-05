//! Pooled-parallel-worker retention state (thread-native parallel, phase 4
//! retention increment). A retention-mode pool standby keeps its PGPROC +
//! sinval slot + warm TLS caches across tasks; this module is the TLS state
//! machine every participating init/teardown site consults. It lives at the
//! bottom of the dep graph (init_small) because the consumers span lmgr_proc,
//! sinval, fd, pgstat, postinit, bgworker, and launch_backend.
//!
//! Lifecycle of a retention-mode standby thread:
//!   spawn -> [claim: begin_task] first task, fresh init
//!         -> [park decision: request_park before proc_exit(0)]
//!         -> teardown callbacks run their park arms (ProcKill partial,
//!            CleanupInvalidationState skipped)
//!         -> [confirm_parked] thread re-enters the pool, identity retained
//!         -> [claim: begin_task] warm task: reattach arms instead of init,
//!            InitPostgres skips Phase2/3, sinval drain replaces
//!            InvalidateSystemCaches
//!         -> ... -> [retire] full identity teardown on pool shrink/flush.
//!
//! Kill switch: PGRUST_NO_RELCACHE_RETAIN=1 reverts to one-task rotation.

use std::cell::Cell;

use types_core::{InvalidOid, Oid};

pub fn retention_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("PGRUST_NO_RELCACHE_RETAIN").is_none())
}

thread_local! {
    // This thread is a retention-mode pool standby (set for the task's whole
    // extent; cleared only when the thread rotates/retires).
    static CANDIDATE: Cell<bool> = const { Cell::new(false) };
    // PGPROC + sinval slot survived a previous task on this thread.
    static IDENTITY_HELD: Cell<bool> = const { Cell::new(false) };
    // Latched by the worker just before proc_exit(0): exit-callback park arms
    // are armed for this teardown.
    static PARKING: Cell<bool> = const { Cell::new(false) };
    // The current task runs the warm (reattach) init arm.
    static WARM_CLAIM: Cell<bool> = const { Cell::new(false) };
    // Database the retained caches were built against.
    static RETAINED_DB: Cell<Oid> = const { Cell::new(InvalidOid) };
    // procsignal shared barrier generation observed at park; a claim-time
    // advance means barrier work (smgr release) was missed while parked.
    static PARKED_BARRIER_GEN: Cell<u64> = const { Cell::new(0) };
    // Recorded by the teardown park arms themselves: a park only counts when
    // BOTH resources actually survived (a callback failure mid-teardown can
    // re-enter proc_exit and run later callbacks without the park arm).
    static RETAINED_PROC: Cell<bool> = const { Cell::new(false) };
    static RETAINED_SINVAL: Cell<bool> = const { Cell::new(false) };
    // This thread's caches were (re)built while bound to a leader transaction
    // holding UNBROADCAST invalidation messages (leader_pending_invals =
    // uncommitted DDL): entries loaded during that task reflect uncommitted
    // catalog state. If that transaction later ABORTS, no sinval traffic ever
    // corrects them — an aborted transaction broadcasts nothing, because in
    // C every backend that could have cached its uncommitted state is dead by
    // then (parallel workers are per-query processes; parallel.c runs
    // InvalidateSystemCaches() on every fresh start). A retained thread
    // outlives the query, so the poison survives the abort: shipped instance,
    // the tableam.c:172 locator assert (table_beginscan_parallel) tripping on
    // a worker whose relcache still held a rolled-back TRUNCATE's
    // relfilelocator. Survives begin_task/confirm_parked; cleared only when a
    // claim-side blanket InvalidateSystemCaches actually runs (or the
    // identity retires).
    static CACHES_TAINTED: Cell<bool> = const { Cell::new(false) };
}

pub fn begin_task(candidate: bool) {
    CANDIDATE.set(candidate);
    WARM_CLAIM.set(candidate && IDENTITY_HELD.get());
    PARKING.set(false);
    RETAINED_PROC.set(false);
    RETAINED_SINVAL.set(false);
}

pub fn candidate() -> bool {
    CANDIDATE.get()
}

pub fn refuse_park() {
    CANDIDATE.set(false);
    WARM_CLAIM.set(false);
    PARKING.set(false);
}

#[inline]
pub fn warm_claim() -> bool {
    WARM_CLAIM.get()
}

pub fn identity_held() -> bool {
    IDENTITY_HELD.get()
}

/// Worker thread, just before proc_exit(0) on a clean task: arm the
/// exit-callback park arms for the teardown about to run.
pub fn request_park(barrier_gen: u64) {
    debug_assert!(CANDIDATE.get());
    PARKING.set(true);
    PARKED_BARRIER_GEN.set(barrier_gen);
}

/// Consulted by ProcKill / CleanupInvalidationState during teardown.
#[inline]
pub fn parking() -> bool {
    PARKING.get()
}

/// ProcKill's park arm ran: the PGPROC survived this teardown.
pub fn note_proc_retained() {
    RETAINED_PROC.set(true);
}

/// CleanupInvalidationState's park arm ran: the sinval slot survived.
pub fn note_sinval_retained() {
    RETAINED_SINVAL.set(true);
}

pub fn proc_retained() -> bool {
    RETAINED_PROC.get()
}

pub fn sinval_retained() -> bool {
    RETAINED_SINVAL.get()
}

/// Standby loop, after run_child_task returns: true when the park went
/// through whole (both resources survived) and the identity is retained for
/// the next claim. On false the caller must run the release path, which
/// consults MyProc (still set = the identity survived the teardown, e.g. a
/// task that died before reattach, or a partial park) and then
/// clear_identity().
pub fn confirm_parked() -> bool {
    let parked = PARKING.get() && RETAINED_PROC.get() && RETAINED_SINVAL.get();
    PARKING.set(false);
    if parked {
        IDENTITY_HELD.set(true);
        RETAINED_PROC.set(false);
        RETAINED_SINVAL.set(false);
    }
    WARM_CLAIM.set(false);
    parked
}

/// The current task bound a transaction with unbroadcast invalidation
/// messages (leader_pending_invals): cache entries built from here on may
/// hold uncommitted catalog state that an abort will never invalidate.
pub fn note_caches_tainted() {
    CACHES_TAINTED.set(true);
}

pub fn caches_tainted() -> bool {
    CACHES_TAINTED.get()
}

/// A claim-side blanket InvalidateSystemCaches ran: the poison (if any) is
/// flushed and the warm drain is trustworthy again.
pub fn clear_caches_taint() {
    CACHES_TAINTED.set(false);
}

pub fn set_retained_db(dboid: Oid) {
    RETAINED_DB.set(dboid);
}

pub fn retained_db() -> Oid {
    RETAINED_DB.get()
}

pub fn parked_barrier_gen() -> u64 {
    PARKED_BARRIER_GEN.get()
}

/// Retirement (pool shrink/flush): the caller runs the retained-identity
/// teardown; this drops the retention marks.
pub fn clear_identity() {
    IDENTITY_HELD.set(false);
    RETAINED_DB.set(InvalidOid);
    CANDIDATE.set(false);
    WARM_CLAIM.set(false);
    PARKING.set(false);
    CACHES_TAINTED.set(false);
}
