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

extern crate alloc;
use alloc::string::String;

use ::types_core::{Oid, XLogRecPtr};
use ::types_error::PgResult;
use ::replication_launcher::LogicalRepWorkerType;
use ::types_storage::LWLockMode;

use ::launcher::with_my_logical_rep_worker;
use lwlock_seams as lwlock;

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
    static ON_COMMIT_WAKEUP_WORKERS_SUBIDS: core::cell::RefCell<Vec<::types_core::Oid>> =
        const { core::cell::RefCell::new(Vec::new()) };
}

// ===========================================================================
// worker.c backend-global apply-worker identity state.
// ===========================================================================
//
// These mirror the worker.c globals that the apply main loop sets when it
// attaches and (re-)reads its subscription. The apply engine itself stays
// unported under #351, but its identity-state globals are owned here and read
// across the worker/launcher/parallel-apply cycle through the worker seams.
//
//   `Subscription *MySubscription = NULL;`            (worker.c:292)
//   `bool InitializingApplyWorker = false;`           (worker.c:312)
//   `WalReceiverConn *LogRepWorkerWalRcvConn = NULL;` (worker.c:290)
//
// Each is a per-backend global, so it is modeled with the established backend-
// global thread-local pattern. `MySubscription` is carried as the value-typed
// subset the cross-cycle seam consumers read (`oid`, `name`, `skiplsn`); the
// rest of the C `Subscription` fields are read only inside the unported apply
// engine and are populated alongside it when it lands. `None` mirrors the C
// `NULL` (a read of `MySubscription->field` against `NULL` is a bug in C, so
// the infallible accessors panic, mirroring that NULL-deref).

/// Value-typed subset of `MySubscription` (`catalog/pg_subscription.h`
/// `Subscription`) reached across the worker/launcher/parallel-apply seam
/// cycle. The full struct is read only inside the unported apply engine.
#[derive(Clone, Debug, Default)]
pub struct MySubscriptionState {
    /// `MySubscription->oid`.
    pub oid: Oid,
    /// `MySubscription->name`.
    pub name: String,
    /// `MySubscription->skiplsn`.
    pub skiplsn: XLogRecPtr,
}

thread_local! {
    /// `Subscription *MySubscription = NULL;` (worker.c:292).
    static MY_SUBSCRIPTION: core::cell::RefCell<Option<MySubscriptionState>> =
        const { core::cell::RefCell::new(None) };

    /// `bool InitializingApplyWorker = false;` (worker.c:312).
    static INITIALIZING_APPLY_WORKER: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };

    /// `WalReceiverConn *LogRepWorkerWalRcvConn = NULL;` (worker.c:290) —
    /// modeled as the "is a connection held?" predicate the cross-cycle path
    /// reads (`LogRepWorkerWalRcvConn != NULL`). The connection object itself
    /// is owned by the unported apply engine / walreceiver dispatch.
    static HAVE_WALRCV_CONN: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
}

/// Set/clear `MySubscription` (worker.c). Called by the apply engine's
/// `InitializeLogRepWorker` / `maybe_reread_subscription` when it lands; `None`
/// resets it to the C `NULL`.
pub fn set_my_subscription(sub: Option<MySubscriptionState>) {
    MY_SUBSCRIPTION.with(|c| *c.borrow_mut() = sub);
}

/// Set `InitializingApplyWorker` (worker.c).
pub fn set_initializing_apply_worker(v: bool) {
    INITIALIZING_APPLY_WORKER.with(|c| c.set(v));
}

/// Set whether `LogRepWorkerWalRcvConn` is currently non-NULL (worker.c).
pub fn set_have_walrcv_conn(v: bool) {
    HAVE_WALRCV_CONN.with(|c| c.set(v));
}

/// Read a field of `*MySubscription`. Panics if `MySubscription == NULL`,
/// mirroring the C NULL-deref (only reachable inside an attached apply worker).
fn with_my_subscription<R>(f: impl FnOnce(&MySubscriptionState) -> R) -> R {
    MY_SUBSCRIPTION.with(|c| {
        let b = c.borrow();
        let sub = b
            .as_ref()
            .expect("MySubscription is NULL (read outside an apply worker)");
        f(sub)
    })
}

// ===========================================================================
// LWLock helpers (LogicalRepWorkerLock).
// ===========================================================================

// `LogicalRepWorkerLock` is acquired in `AtEOXact_LogicalRepWorkers` directly via
// the `lwlock_acquire_main` RAII guard (held for the loop, released at C's
// `LWLockRelease` call site). Wrapping acquire in a guard-discarding helper would
// release the lock immediately — the bug fixed here.

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
pub fn LogicalRepWorkersWakeupAtCommit(subid: ::types_core::Oid) -> PgResult<()> {
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
    let subids: Vec<::types_core::Oid> =
        ON_COMMIT_WAKEUP_WORKERS_SUBIDS.with(|cell| core::mem::take(&mut *cell.borrow_mut()));

    if is_commit && !subids.is_empty() {
        // LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
        // Hold the RAII guard for the whole loop: the seam returns a
        // `MainLWLockGuard` whose `Drop` releases the lock (and is the abort
        // backstop). Discarding it here would release the lock *immediately*, so
        // the explicit release below would then hit an unheld lock — the
        // subscription.sql commit-path `cannot abort transaction` PANIC.
        let guard = lwlock::lwlock_acquire_main::call(LOGICAL_REP_WORKER_LOCK, LWLockMode::LW_SHARED)?;

        // foreach(lc, on_commit_wakeup_workers_subids)
        let result = (|| -> PgResult<()> {
            for subid in &subids {
                // workers = logicalrep_workers_find(subid, true, false);
                let workers = ::launcher::logicalrep_workers_find(
                    *subid, true, false,
                )?;
                // foreach(lc2, workers) logicalrep_worker_wakeup_ptr(worker);
                for slot in workers {
                    launcher_seams::logicalrep_worker_wakeup_ptr::call(
                        slot,
                    )?;
                }
            }
            Ok(())
        })();

        // LWLockRelease(LogicalRepWorkerLock); — released regardless of result
        // (explicit release at C's call site; surfaces any release error).
        let rel = guard.release();
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
    use worker_seams as s;

    // `AtEOXact_LogicalRepWorkers(isCommit)` — the seam is the infallible
    // `(bool)` shape (the C return is void). The launcher find/wakeup calls
    // it makes are infallible on the boot/single-user path (the subid list is
    // always empty), so unwrap the always-`Ok` result here.
    s::at_eoxact_logical_rep_workers::set(|is_commit| {
        AtEOXact_LogicalRepWorkers(is_commit)
            .expect("AtEOXact_LogicalRepWorkers wakeup is infallible on this path")
    });

    s::LogicalRepWorkersWakeupAtCommit::set(LogicalRepWorkersWakeupAtCommit);

    // -------------------------------------------------------------------
    // Apply-worker / subscription identity + state accessors.
    //
    // The `MyLogicalRepWorker->field` reads resolve against the launcher-owned
    // shared worker slot (`with_my_logical_rep_worker`); the `MySubscription`
    // and worker.c-global reads resolve against this crate's backend-global
    // state. The infallible seams panic on a NULL-deref (read outside an
    // attached apply worker), mirroring the C inline accessors.
    // -------------------------------------------------------------------

    // `am_leader_apply_worker()` (worker_internal.h:341):
    //   Assert(MyLogicalRepWorker->in_use);
    //   return (MyLogicalRepWorker->type == WORKERTYPE_APPLY);
    s::am_leader_apply_worker::set(|| {
        with_my_logical_rep_worker(|w| {
            debug_assert!(w.in_use);
            w.wtype == LogicalRepWorkerType::Apply
        })
    });

    // `am_parallel_apply_worker()` (worker_internal.h:348):
    //   Assert(MyLogicalRepWorker->in_use);
    //   return isParallelApplyWorker(MyLogicalRepWorker);
    s::am_parallel_apply_worker::set(|| {
        with_my_logical_rep_worker(|w| {
            debug_assert!(w.in_use);
            w.is_parallel_apply_worker()
        })
        .expect("am_parallel_apply_worker read outside an apply worker")
    });

    // `MyLogicalRepWorker->parallel_apply`.
    s::my_worker_parallel_apply::set(|| {
        with_my_logical_rep_worker(|w| w.parallel_apply)
            .expect("MyLogicalRepWorker->parallel_apply read outside an apply worker")
    });

    // `MyLogicalRepWorker->dbid`.
    s::my_worker_dbid::set(|| {
        with_my_logical_rep_worker(|w| w.dbid)
            .expect("MyLogicalRepWorker->dbid read outside an apply worker")
    });

    // `MyLogicalRepWorker->userid`.
    s::my_worker_userid::set(|| {
        with_my_logical_rep_worker(|w| w.userid)
            .expect("MyLogicalRepWorker->userid read outside an apply worker")
    });

    // `MyLogicalRepWorker->subid`.
    s::my_worker_subid::set(|| {
        with_my_logical_rep_worker(|w| w.subid)
            .expect("MyLogicalRepWorker->subid read outside an apply worker")
    });

    // `MyLogicalRepWorker->leader_pid`.
    s::my_worker_leader_pid::set(|| {
        with_my_logical_rep_worker(|w| w.leader_pid)
            .expect("MyLogicalRepWorker->leader_pid read outside an apply worker")
    });

    // `MyLogicalRepWorker->generation`.
    s::my_worker_generation::set(|| {
        with_my_logical_rep_worker(|w| w.generation)
            .expect("MyLogicalRepWorker->generation read outside an apply worker")
    });

    // `MySubscription->skiplsn`.
    s::my_subscription_skiplsn::set(|| with_my_subscription(|s| s.skiplsn));

    // `MySubscription->oid`.
    s::my_subscription_oid::set(|| with_my_subscription(|s| s.oid));

    // `MySubscription->name`.
    s::my_subscription_name::set(|| with_my_subscription(|s| s.name.clone()));

    // `InitializingApplyWorker` (worker.c global).
    s::initializing_apply_worker::set(|| INITIALIZING_APPLY_WORKER.with(|c| c.get()));

    // `LogRepWorkerWalRcvConn != NULL` (worker.c global).
    s::have_walrcv_conn::set(|| HAVE_WALRCV_CONN.with(|c| c.get()));
}
