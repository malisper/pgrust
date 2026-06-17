//! `replication/logical/worker.c` — the logical-replication apply-worker.
//!
//! NEEDS_DECOMP / F0. worker.c is the ~5.2k-LOC logical-replication apply-worker
//! main loop (`ApplyWorkerMain` / `LogicalRepApplyLoop` / `apply_dispatch` /
//! `apply_handle_*` / streaming-transaction spool machinery / parallel-apply
//! leader path). That whole engine is gated on the #351 logical-decoding
//! keystone and is **deliberately not ported here** — it remains unported under
//! #351 and continues to live behind the loud `*_seams` panics in
//! `backend-replication-logical-worker-seams`.
//!
//! This F0 slice ports ONLY the coherent commit-wakeup family, which is what the
//! transaction-commit path (`CommitTransaction` -> `AtEOXact_LogicalRepWorkers`)
//! reaches at the very tail of commit:
//!
//! * [`AtEOXact_LogicalRepWorkers`] (worker.c:5152) — installed as the
//!   `at_eoxact_logical_rep_workers` seam consumed by xact commit/abort.
//! * [`LogicalRepWorkersWakeupAtCommit`] (worker.c:5135) — the appender that
//!   schedules a wakeup at commit; installed as its inward seam.
//! * `on_commit_wakeup_workers_subids` (worker.c:295) — the worker.c-private
//!   `List *` of subscription OIDs whose workers to wake at commit.
//!
//! The find/wakeup primitives (`logicalrep_workers_find` /
//! `logicalrep_worker_wakeup_ptr`) and `LogicalRepWorkerLock` are owned by
//! launcher.c, ported as `backend-replication-logical-launcher`; this crate
//! calls the launcher's real API.

#![allow(non_snake_case)]

use types_error::PgResult;
use types_storage::LWLockMode;

use backend_storage_lmgr_lwlock_seams as lwlock;

/// `LogicalRepWorkerLock` — individual built-in LWLock #43 (lwlocklist.h).
/// Same offset the launcher uses for this lock.
const LOGICAL_REP_WORKER_LOCK: usize = 43;

// ===========================================================================
// worker.c module-static state.
// ===========================================================================
//
// `static List *on_commit_wakeup_workers_subids = NIL;` (worker.c:295).
//
// In C this is a backend-private `List *` allocated in `TopTransactionContext`
// and reclaimed automatically when the transaction's memory is freed at xact
// cleanup. It is per-backend, so it is modeled as a thread-local `Vec<Oid>`
// (the established backend-global pattern). `NIL` is the empty vector;
// resetting to `NIL` is `clear()`. The dedup of `list_append_unique_oid` is
// the `contains`-then-`push` below.

thread_local! {
    /// `static List *on_commit_wakeup_workers_subids = NIL;` (worker.c:295).
    static ON_COMMIT_WAKEUP_WORKERS_SUBIDS: core::cell::RefCell<Vec<types_core::Oid>> =
        const { core::cell::RefCell::new(Vec::new()) };
}

// ===========================================================================
// LWLock helpers (LogicalRepWorkerLock).
// ===========================================================================

#[inline]
fn worker_lock_acquire(mode: LWLockMode) -> PgResult<()> {
    lwlock::lwlock_acquire_main::call(LOGICAL_REP_WORKER_LOCK, mode).map(|_| ())
}

#[inline]
fn worker_lock_release() -> PgResult<()> {
    lwlock::lwlock_release_main::call(LOGICAL_REP_WORKER_LOCK)
}

// ===========================================================================
// Commit-wakeup family (worker.c:5135 / 5152).
// ===========================================================================

/// `LogicalRepWorkersWakeupAtCommit(Oid subid)` (worker.c:5135).
///
/// Request wakeup of the workers for the given subscription OID at commit of
/// the current transaction. In C this switches to `TopTransactionContext` and
/// appends `subid` to `on_commit_wakeup_workers_subids` via
/// `list_append_unique_oid` (append-if-not-already-present). The thread-local
/// `Vec<Oid>` carries the transaction-scoped lifetime; the dedup is the
/// `contains` check.
pub fn LogicalRepWorkersWakeupAtCommit(subid: types_core::Oid) -> PgResult<()> {
    ON_COMMIT_WAKEUP_WORKERS_SUBIDS.with(|cell| {
        let mut list = cell.borrow_mut();
        if !list.contains(&subid) {
            list.push(subid);
        }
    });
    Ok(())
}

/// `AtEOXact_LogicalRepWorkers(bool isCommit)` (worker.c:5152).
///
/// Wake up the workers of any subscriptions that were changed in this xact.
///
/// On commit, if the subid list is non-empty, acquire `LogicalRepWorkerLock`
/// in shared mode, and for each scheduled subid find its workers
/// (`logicalrep_workers_find(subid, true, false)`) and wake each one
/// (`logicalrep_worker_wakeup_ptr(worker)`), then release the lock.
///
/// The list storage is reclaimed automatically in xact cleanup in C; here it
/// is explicitly cleared (the equivalent of resetting the static to `NIL`).
pub fn AtEOXact_LogicalRepWorkers(is_commit: bool) -> PgResult<()> {
    // Snapshot the scheduled subids and clear the list. We take the snapshot
    // first so the launcher calls below do not re-enter the RefCell borrow.
    let subids: Vec<types_core::Oid> =
        ON_COMMIT_WAKEUP_WORKERS_SUBIDS.with(|cell| core::mem::take(&mut *cell.borrow_mut()));

    if is_commit && !subids.is_empty() {
        // LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
        worker_lock_acquire(LWLockMode::LW_SHARED)?;

        // foreach(lc, on_commit_wakeup_workers_subids)
        let result = (|| -> PgResult<()> {
            for subid in &subids {
                // workers = logicalrep_workers_find(subid, true, false);
                let workers = backend_replication_logical_launcher::logicalrep_workers_find(
                    *subid, true, false,
                )?;
                // foreach(lc2, workers) logicalrep_worker_wakeup_ptr(worker);
                for slot in workers {
                    backend_replication_logical_launcher_seams::logicalrep_worker_wakeup_ptr::call(
                        slot,
                    )?;
                }
            }
            Ok(())
        })();

        // LWLockRelease(LogicalRepWorkerLock); — released regardless of result.
        let rel = worker_lock_release();
        result?;
        rel?;
    }

    // The List storage is reclaimed automatically in xact cleanup; the
    // explicit `take` above already reset it to NIL.
    Ok(())
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install the worker.c-owned seams ported in this F0 slice. The rest of
/// worker.c (the apply main loop) stays unported under #351 and keeps panicking
/// loudly through its `*_seams` stubs.
pub fn init_seams() {
    use backend_replication_logical_worker_seams as s;

    // `AtEOXact_LogicalRepWorkers(isCommit)` — the seam is the infallible
    // `(bool)` shape (the C return is void). The launcher find/wakeup calls
    // it makes are infallible on the boot/single-user path (the subid list is
    // always empty), so unwrap the always-`Ok` result here.
    s::at_eoxact_logical_rep_workers::set(|is_commit| {
        AtEOXact_LogicalRepWorkers(is_commit)
            .expect("AtEOXact_LogicalRepWorkers wakeup is infallible on this path")
    });

    s::LogicalRepWorkersWakeupAtCommit::set(LogicalRepWorkersWakeupAtCommit);
}
