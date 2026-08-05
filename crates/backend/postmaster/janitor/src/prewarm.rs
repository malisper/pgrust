//! Post-mint prewarm (docs/design/test-views.md prewarm addendum, F1):
//! after any successful janitor mint — warm-pool spare replenish AND cold
//! batch/serial mints — a one-shot internal worker connects to the new
//! database once and exits, so the fresh clone's relcache init file
//! (`pgrust_internal.init`, relcache::initfile) gets written and its
//! catalog pages land in shared buffers BEFORE the first client session
//! pays the fresh-database catalog bootstrap (~4.5-9ms server-side vs ~1ms
//! warm, the mint-profile finding).
//!
//! MECHANISM (recorded decision, option (i) of the charter): a DYNAMIC
//! background worker per touch, `bgworker::RegisterDynamicBackgroundWorker`
//! with `BGW_NEVER_RESTART` and a database connection by OID. Chosen over
//! (iii) a janitor-side smgr page read because only a real DB-connected
//! session runs `RelationCacheInitializePhase3`, which is what BUILDS and
//! WRITES the init file — warming pages alone leaves the dominant relcache
//! bootstrap cost in place. The janitor itself cannot do the touch: a
//! session is bound to its home database for life (InitPostgres), so a
//! separate worker is forced. `BGWORKER_BYPASS_ALLOWCONN` is load-bearing:
//! warm-pool spares are minted ALLOW_CONNECTIONS false (content-poisoning
//! defense) and ordinary sessions cannot enter them.
//!
//! STRICTLY OFF THE WAITER CRITICAL PATH: mint completion wakes the
//! waiters first; the mint/replenish sites merely ENQUEUE (one registry
//! push), and dispatch runs as its own later tick step. A cold-mint
//! waiter's own first session racing the touch is benign — whoever runs
//! first builds the init file; concurrent writers are safe because the
//! write path is temp-file + rename with a per-writer temp name
//! (initfile.rs; the temp name carries MyProcNumber, the thread-model
//! analog of C's per-PID uniqueness) and the rename is serialized under
//! RelCacheInitLock. To avoid burning workers on that redundancy, dispatch
//! SKIPS a target whose datadir already has the init file (one pg_stat) —
//! which also makes the race-suite prewarm witness exact: a "prewarmed"
//! log line implies the file was absent at dispatch and present after.
//!
//! Touch racing a handout/drop of the same database is absorbed by the
//! stock occupancy machinery: RenameDatabase/dropdb run
//! CountOtherDBBackends' bounded retry loop (~5s at 100ms), and a touch
//! session lives for milliseconds. A touch whose database was dropped
//! first fails its connect and exits 1 — contained by construction (the
//! worker is NEVER_RESTART and owns no janitor state; its exit guard
//! clears the in-flight slot on every path).

use elog::elog as log_report;
use types_core::{InvalidOid, Oid};
use types_error::{PgResult, LOG};

use crate::registry;

/// In-flight touch ceiling: bounds concurrent worker-slot consumption
/// (max_worker_processes defaults to 8; parallel query needs the rest) and
/// the transient backend count the reap pass observes. Deficits drain over
/// subsequent ~500ms ticks; the worker's exit guard wakes the janitor so a
/// draining queue does not wait out full ticks.
pub(crate) const TOUCH_INFLIGHT_MAX: usize = 4;

/// In-flight leak bound: a worker that never started (postmaster refused
/// the spawn after registration succeeded) never runs its exit guard; its
/// slot expires after this and the cap recovers. Generous vs the ~5-10ms
/// expected touch.
const TOUCH_DEADLINE_NS: u64 = 15_000_000_000;

/// One dispatch pass, its own janitor tick step (after the warm-pool
/// replenish — so spares minted this tick dispatch this tick — and before
/// the reap pass). Feature-gated on `pgrust.ephemeral_db_prewarm`; a
/// paused janitor never reaches this step (the tick's paused branch).
pub(crate) fn dispatch_pass() -> PgResult<()> {
    if !crate::ephemeral_db_prewarm() {
        return Ok(());
    }
    let now = pg_clock::mono_ns();
    let targets = registry::begin_touches(now, now + TOUCH_DEADLINE_NS, TOUCH_INFLIGHT_MAX);
    let mut targets = targets.into_iter();
    while let Some((name, oid)) = targets.next() {
        // Already-warm skip (rationale above): the waiter's own first
        // session, or a previous touch of a re-enqueued oid, already built
        // the file. Default-tablespace path only — a non-default-tablespace
        // clone misses this stat and simply gets its (harmless) touch.
        if init_file_present(oid) {
            registry::finish_touch(oid);
            continue;
        }
        let bgw = bgworker::BackgroundWorker {
            bgw_name: format!("pgrust ephemeral-db prewarm: {name}"),
            bgw_type: "pgrust ephemeral-db prewarm".to_string(),
            bgw_flags: bgworker::BGWORKER_SHMEM_ACCESS
                | bgworker::BGWORKER_BACKEND_DATABASE_CONNECTION,
            bgw_start_time: bgworker::BgWorkerStartTime::RecoveryFinished,
            bgw_restart_time: bgworker::BGW_NEVER_RESTART,
            bgw_main: prewarm_touch_main,
            bgw_main_arg: oid as u64,
            bgw_extra: [0; bgworker::BGW_EXTRALEN],
            bgw_notify_pid: 0,
        };
        match bgworker::RegisterDynamicBackgroundWorker(bgw)? {
            Some(_) => {}
            None => {
                // No free worker slot (transient: parallel query burst).
                // Silent by design — the postmaster already logs sustained
                // slot pressure — put the target back and retry next tick.
                // The rest of this pass's drained targets go back too:
                // begin_touches marked them in-flight, and leaving them
                // undispatched would strand them until the deadline expiry.
                registry::requeue_touch(name, oid);
                for (n, o) in targets {
                    registry::requeue_touch(n, o);
                }
                break;
            }
        }
    }
    Ok(())
}

/// Does `base/<oid>/pgrust_internal.init` exist? vfs-routed stat (fd), the
/// initfile.rs convention: datadir-domain paths must ride the vfs.
fn init_file_present(oid: Oid) -> bool {
    let path = format!("base/{oid}/{}", relcache::initfile::RELCACHE_INIT_FILENAME);
    let mut st = fd::FileInfo::zeroed();
    fd::pg_stat(&path, &mut st) == 0
}

/// The touch worker body (bgw_main; arg = the target database oid): connect
/// by oid — InitPostgres runs the relcache bootstrap, which writes the init
/// file and warms the catalog pages, i.e. the connect IS the work — log the
/// witness line, exit 0. Errors (database dropped first, shutdown) exit 1
/// through the bgworker harness's ordinary report path.
pub(crate) fn prewarm_touch_main(main_arg: u64) -> PgResult<()> {
    let oid = main_arg as Oid;
    // The in-flight slot must clear on EVERY exit path — including FATAL
    // unwinds out of the connect — or the cap leaks until the deadline.
    // Waking the janitor lets a draining queue proceed without waiting out
    // the ~500ms tick.
    struct FinishTouch(Oid);
    impl Drop for FinishTouch {
        fn drop(&mut self) {
            registry::finish_touch(self.0);
            registry::wake_janitor();
        }
    }
    let _guard = FinishTouch(oid);

    bgworker::BackgroundWorkerUnblockSignals();
    let t0 = pg_clock::mono_ns();
    // BYPASS_ALLOWCONN: warm-pool spares are ALLOW_CONNECTIONS false while
    // listed. By-OID connect keeps a concurrent handout RENAME benign (the
    // oid survives the rename; worst case this touch warms the database
    // under its new client-facing name).
    bgworker::BackgroundWorkerInitializeConnectionByOid(
        oid,
        InvalidOid,
        bgworker::BGWORKER_BYPASS_ALLOWCONN,
    )?;
    let ms = pg_clock::mono_ns().saturating_sub(t0) as f64 / 1e6;
    // The prewarm witness line (race-suite prewarm phase greps it): written
    // AFTER the connect completed, so the init file exists when it appears.
    let _ = log_report(
        LOG,
        format!(
            "pgrust ephemeral-db janitor: prewarmed ephemeral database oid {oid} \
             (relcache init built, connect in {ms:.2} ms)"
        ),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The queue/in-flight bookkeeping the dispatch pass rides on
    /// (RELEASE-effective plain asserts). ONE test function under the
    /// crate-wide registry test lock (the registry.rs convention: the
    /// touch tables are process-global).
    #[test]
    fn touch_queue_semantics() {
        let _table = registry::test_pin_table_lock();

        // FIFO up to the in-flight cap; dedupe by oid; drained entries are
        // in flight until finished or expired.
        assert!(registry::enqueue_touch("tv_t1", 91001));
        assert!(!registry::enqueue_touch("tv_t1_dup", 91001), "dedupe by oid");
        assert!(registry::enqueue_touch("tv_t2", 91002));
        assert!(registry::enqueue_touch("tv_t3", 91003));

        let got = registry::begin_touches(1_000, 2_000, 2);
        assert_eq!(
            got.iter().map(|&(_, o)| o).collect::<Vec<_>>(),
            vec![91001, 91002],
            "FIFO, capped at max_inflight"
        );
        // In-flight entries block re-enqueue of the same oid.
        assert!(!registry::enqueue_touch("tv_t1_again", 91001));
        // Cap holds while both are in flight.
        assert!(registry::begin_touches(1_100, 2_000, 2).is_empty());

        // finish_touch releases a slot; the next take drains the queue.
        registry::finish_touch(91001);
        let got = registry::begin_touches(1_200, 2_200, 2);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].1, 91003);

        // requeue_touch puts a failed registration back at the FRONT and
        // frees its slot.
        registry::requeue_touch("tv_t3".to_string(), 91003);
        assert!(registry::enqueue_touch("tv_t4", 91004));
        let got = registry::begin_touches(1_300, 2_300, 4);
        assert_eq!(
            got.iter().map(|&(_, o)| o).collect::<Vec<_>>(),
            vec![91003, 91004],
            "requeued target leads the queue; 91002 is still in flight"
        );

        // Deadline expiry frees leaked slots: 91002's deadline (2_000)
        // passes and the cap recovers.
        registry::finish_touch(91003);
        registry::finish_touch(91004);
        assert!(registry::enqueue_touch("tv_t5", 91005));
        let got = registry::begin_touches(3_000, 4_000, 1);
        assert_eq!(got.len(), 1, "expired in-flight entry no longer counts");
        assert_eq!(got[0].1, 91005);
        registry::finish_touch(91005);
        // 91002 expired out of the in-flight table; nothing left queued.
        assert!(registry::begin_touches(5_000, 6_000, 4).is_empty());

        // Churn accounting: monotonic note, reset-on-success only.
        registry::reset_catalog_churn();
        registry::note_catalog_churn(3);
        registry::note_catalog_churn(2);
        assert_eq!(registry::catalog_churn(), 5);
        registry::reset_catalog_churn();
        assert_eq!(registry::catalog_churn(), 0);
    }
}
