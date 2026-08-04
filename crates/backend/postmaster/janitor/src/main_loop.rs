//! The janitor's background-worker body: connect, adoption guard, startup
//! sweep, then the ~500ms reap loop. ALL lifecycle mutations serialize
//! through this loop — a design invariant D2's mint path relies on.

use elog::{elog as log_report, ereport};
use init_small::globals as g;
use procsignal::ThreadSignalHandler::Simple;
use types_core::Oid;
use types_error::{PgResult, ERRCODE_ADMIN_SHUTDOWN, FATAL, LOG, WARNING};
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

use crate::dbscan::{scan_prefix_rows, DbRow};
use crate::marker::{self, Guard};
use crate::reap::{reap_candidate, StreakTracker};
use crate::{grammar, mint, registry};

/// Poll cadence (spec: "~500ms").
const TICK_MS: i64 = 500;

/// pg_stat_activity wait tag for the idle tick: the Extension class, exactly
/// what a C extension bgworker (worker_spi) reports. The class has no name
/// table (runtime-tagged in C), so no wait_event_names.txt index to mis-pin
/// (GL-SYNCWEDGE-1 / scripts/lint-waitevent-tags.sh applies only to indexed
/// classes).
const PG_WAIT_EXTENSION: u32 = 0x0700_0000;

/// The bgw_main entry (registered in lib.rs). The wrapper exists to make the
/// containment contract's exit loud: any unrecoverable error is prefixed
/// with an explicit disabled-until-restart log before the FATAL-clean
/// status-1 exit (BGW_NEVER_RESTART then keeps the registration from
/// respawning; the server stays up).
pub(crate) fn janitor_bgw_main(main_arg: u64) -> PgResult<()> {
    let r = janitor_main(main_arg);
    if let Err(e) = &r {
        // SIGTERM shutdown arrives as bgworker_die's FATAL 57P01 — normal
        // server shutdown, not a janitor failure.
        if e.sqlstate() != ERRCODE_ADMIN_SHUTDOWN {
            let _ = log_report(
                WARNING,
                format!(
                    "pgrust ephemeral-db janitor: unrecoverable error; the janitor is DISABLED \
                     until the server restarts (no ephemeral database will be swept or reaped): {}",
                    e.message()
                ),
            );
        }
    }
    r
}

fn janitor_main(_main_arg: u64) -> PgResult<()> {
    // bgworker's default handlers leave SIGHUP ignored: install the config-
    // reload flag handler ourselves (launcher precedent) or the PGC_SIGHUP
    // grace GUC would silently never re-read.
    procsignal::pqsignal_thread(
        procsignal::signums::SIGHUP,
        Simple(interrupt::SignalHandlerForConfigReload),
    );
    // SIGTERM keeps bgworker's default (bgworker_die: FATAL, clean exit 1).
    bgworker::BackgroundWorkerUnblockSignals();

    // Full DB-connected session (dropdb needs one); superuser context
    // (username=None → InitializeSessionUserIdStandalone), so the janitor's
    // drops never fail ownership checks — the namespace contract.
    bgworker::BackgroundWorkerInitializeConnection(Some(crate::JANITOR_HOME_DB), None, 0)?;

    registry::set_janitor_proc(lmgr_proc::MyProc());
    struct ClearProc;
    impl Drop for ClearProc {
        fn drop(&mut self) {
            registry::set_janitor_proc(None);
            // Exit drain (D2): BGW_NEVER_RESTART means nothing will ever
            // service the Ensure queue again — fail every pending entry and
            // wake its waiters (parking on a latch with only a deadline as
            // the escape is a hang in spec terms). Runs on FATAL unwinds
            // too, which is the point.
            mint::fail_pending_and_wake(
                &types_error::PgError::error(
                    "the pgrust ephemeral-db janitor exited; it is disabled until the server \
                     restarts"
                        .to_string(),
                )
                .with_sqlstate(types_error::ERRCODE_CANNOT_CONNECT_NOW),
            );
        }
    }
    let _clear_proc = ClearProc;

    let prefix = crate::ephemeral_db_prefix();
    // Registration is gated on a non-empty prefix and the GUC is
    // PGC_POSTMASTER: empty here would mean the gate broke.
    debug_assert!(!prefix.is_empty());

    let _ = log_report(
        LOG,
        format!(
            "pgrust ephemeral-db janitor started: prefix \"{prefix}\", grace {}s, tick {TICK_MS}ms",
            crate::ephemeral_db_grace_secs()
        ),
    );

    // First iteration (spec item 3: the earliest a DB-connected worker can
    // run — between server start and this point a client may reconnect to a
    // leaked database; it then falls back to normal grace-based reaping):
    // adoption guard, then either the paused state or the startup sweep
    // (scheduled via the deferred-sweep request, run by the first tick).
    startup_guard_and_sweep(&prefix)?;

    let mut streaks = StreakTracker::new();

    loop {
        let rc = latch::WaitLatch(
            g::MyLatch(),
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            TICK_MS,
            PG_WAIT_EXTENSION,
        )?;
        if rc & WL_LATCH_SET != 0 {
            if let Some(l) = g::MyLatch() {
                latch::ResetLatch(l);
            }
            postgres_seams::check_for_interrupts::call()?;
        }

        // PGC_SIGHUP reload (grace): GUC stores are thread-local, so the
        // loop must run the reload idiom itself (checkpointer/launcher
        // precedent). A broken config file must not kill a NEVER_RESTART
        // worker: contain and keep the old values.
        if interrupt::ConfigReloadPending() {
            interrupt::SetConfigReloadPending(false);
            if let Err(e) = guc_file::ProcessConfigFile(types_guc::GucContext::PGC_SIGHUP) {
                contain(e, "reloading configuration")?;
            }
        }

        // Paused (adoption guard): no sweep, no reaping, and D2 mint
        // Ensures are rejected against registry::is_paused(). Keep ticking
        // so SIGHUP/shutdown stay responsive. The drain matters: Ensures
        // posted between set_janitor_proc (above) and the guard's pause
        // decision would otherwise wait out their full timeout — the
        // backend post path refuses NEW ones, this rejects the window's
        // stragglers (both halves are required; neither alone closes the
        // race).
        if registry::is_paused() {
            mint::reject_pending_paused();
            continue;
        }

        // Deferred startup sweep, requested by pgrust_janitor_unpause()
        // after it durably wrote the marker, or by the adoption guard on a
        // normal (marker-acknowledged) start. A contained failure re-arms
        // the request: "unpause runs the deferred sweep" must not silently
        // downgrade to grace-based reaping because one pass failed.
        if registry::take_sweep_request() {
            let _ = log_report(
                LOG,
                format!("pgrust ephemeral-db janitor: running deferred startup sweep for prefix \"{prefix}\""),
            );
            if let Err(e) = sweep_pass(&prefix) {
                registry::request_sweep();
                contain(e, "deferred startup sweep")?;
            }
        }

        // D2 mint servicing, AFTER the deferred sweep (a fresh mint must
        // not race the sweep it may have been queued behind) and BEFORE the
        // reap pass (mint-before-reap: the ordering decision and its safety
        // argument live on mint::service_pass).
        if let Err(e) = mint::service_pass() {
            contain(e, "mint service pass")?;
        }

        if let Err(e) = reap_pass(&prefix, &mut streaks) {
            contain(e, "reap pass")?;
        }

        // Retire resolved Ensure entries whose waiters left and whose
        // fresh-mint shield linger expired.
        registry::gc_ensures(pg_clock::mono_ns());
    }
}

/// The containment contract's catch: report the error, abort any open
/// transaction, and keep the loop alive. FATAL-class errors (shutdown,
/// InitPostgres-grade failures) are unrecoverable and propagate — in C a
/// FATAL would never reach PG_CATCH at all (the autovacuum-worker
/// containment shape, worker.rs). pub(crate): mint::service_pass contains
/// per-Ensure createdb failures through the same choreography.
pub(crate) fn contain(e: Box<types_error::PgError>, what: &str) -> PgResult<()> {
    if e.level() >= FATAL {
        return Err(e);
    }
    g::HoldInterrupts();
    elog::emit_error_report_for(&e);
    let _ = log_report(
        LOG,
        format!("pgrust ephemeral-db janitor: {what} failed (see above); continuing"),
    );
    xact::AbortOutOfAnyTransaction()?;
    elog::FlushErrorState();
    g::ResumeInterrupts();
    Ok(())
}

/// Seqscan pg_database inside a private transaction (the autovacuum
/// get_database_list template); the scan body itself is shared with the
/// backend-side mint cap count (dbscan.rs, which documents the row shape
/// and the non-UTF-8 skip).
fn list_prefix_databases(prefix: &str) -> PgResult<Vec<DbRow>> {
    xact::StartTransactionCommand()?;
    let rows = scan_prefix_rows(prefix)?;
    xact::CommitTransactionCommand()?;
    Ok(rows)
}

/// The adoption guard (spec item 4), run once before the first tick.
fn startup_guard_and_sweep(prefix: &str) -> PgResult<()> {
    let rows = list_prefix_databases(prefix)?;
    match marker::decode(marker::read()?.as_deref(), prefix) {
        Guard::Acknowledged => {
            // Normal (re)start: leftover ephemerals are disposable across
            // restarts — sweep them (spec item 3). The sweep is scheduled
            // through the deferred-sweep mechanism and runs on the first
            // tick: its failures are then contained and RETRIED by the loop
            // instead of disabling the janitor (containment symmetry — the
            // guard decision itself, marker I/O and this enumeration, still
            // propagates: the guard must never be silently defeated). Pins
            // do not survive restart; any pin present by the time the sweep
            // runs was taken in the reconnect window and is honored
            // (sweep_pass re-reads both the catalog and the pin table).
            registry::request_sweep();
        }
        Guard::Unacknowledged { recorded } => {
            let survivors: Vec<&DbRow> = rows.iter().filter(|d| !d.istemplate).collect();
            if survivors.is_empty() {
                // Nothing the sweep would touch: adopt the prefix now so
                // the next restart is a normal one.
                marker::write(prefix)?;
                let _ = log_report(
                    LOG,
                    format!(
                        "pgrust ephemeral-db janitor: prefix \"{prefix}\" adopted (no existing \
                         non-template databases match); marker \"{}\" written",
                        marker::MARKER_FILE
                    ),
                );
            } else {
                registry::set_paused(true);
                let names: Vec<&str> = survivors.iter().map(|d| d.name.as_str()).collect();
                let recorded_desc = match recorded {
                    Some(p) => format!("records prefix \"{p}\""),
                    None => "is absent".to_string(),
                };
                // Loud by design: this is the `prefix = 'prod'` foot-gun
                // guard. ereport(WARNING) so it stands out in the log.
                let _ = ereport(WARNING)
                    .errmsg(format!(
                        "pgrust ephemeral-db janitor: PAUSED — {} database(s) match prefix \
                         \"{prefix}\" but the adoption-guard marker {recorded_desc}: {}",
                        names.len(),
                        names.join(", ")
                    ))
                    .errdetail(
                        "No startup sweep and no reaping will run while paused; \
                         mint requests are rejected."
                            .to_string(),
                    )
                    .errhint(
                        "Run SELECT pgrust_janitor_unpause(); (superuser) to acknowledge the \
                         prefix, write the marker, and run the deferred startup sweep — or \
                         restart with a different pgrust.ephemeral_db_prefix."
                            .to_string(),
                    )
                    .finish(loc("startup_guard_and_sweep"));
            }
        }
    }
    Ok(())
}

/// Startup sweep (spec item 3): the reap predicate minus the grace clause.
fn sweep_pass(prefix: &str) -> PgResult<()> {
    let rows = list_prefix_databases(prefix)?;
    sweep_rows(prefix, &rows)
}

fn sweep_rows(prefix: &str, rows: &[DbRow]) -> PgResult<()> {
    let own = g::MyDatabaseId();
    let victims: Vec<(Oid, &str)> = rows
        .iter()
        .filter(|d| {
            reap_candidate(
                &d.name,
                prefix,
                d.istemplate,
                registry::is_pinned(&d.name),
                d.oid == own,
                registry::ensure_shields(&d.name),
            )
        })
        .map(|d| (d.oid, d.name.as_str()))
        .collect();
    if victims.is_empty() {
        return Ok(());
    }
    let names: Vec<&str> = victims.iter().map(|&(_, n)| n).collect();
    let _ = log_report(
        LOG,
        format!(
            "pgrust ephemeral-db janitor: startup sweep dropping {} database(s): {}",
            victims.len(),
            names.join(", ")
        ),
    );
    drop_batch(&victims, None)?;
    Ok(())
}

/// One reap tick (spec item 2): observe zero-backend streaks over the
/// candidates and batch-drop the ones idle for at least the grace period.
fn reap_pass(prefix: &str, streaks: &mut StreakTracker) -> PgResult<()> {
    let rows = list_prefix_databases(prefix)?;
    let own = g::MyDatabaseId();
    let default_grace_secs = crate::ephemeral_db_grace_secs().max(0) as u64;
    // The one monotonic authority (determinism choke; never std::time).
    let now_ns = pg_clock::mono_ns();

    let mut seen: Vec<Oid> = Vec::new();
    let mut victims: Vec<(Oid, &str)> = Vec::new();
    for d in &rows {
        if !reap_candidate(
            &d.name,
            prefix,
            d.istemplate,
            registry::is_pinned(&d.name),
            d.oid == own,
            registry::ensure_shields(&d.name),
        ) {
            continue;
        }
        seen.push(d.oid);
        // Per-template grace (spec D2: a clone of template A reaps on A's
        // override, not the default). Attribution is by NAME GRAMMAR alone
        // (grammar::template_of): bare tokens carry no template segment and
        // reap on the default grace.
        let grace_secs = grammar::template_of(prefix, &d.name)
            .and_then(|t| registry::template_grace_override(t))
            .map(|s| s.max(0) as u64)
            .unwrap_or(default_grace_secs);
        let grace_ns = grace_secs * 1_000_000_000;
        // Procarray ground truth — never refcounts (spec item 2). NOTE:
        // CountDBBackends does not count prepared xacts, so a database
        // holding only a prepared transaction accrues a streak; its drop
        // then fails on dropdb's own occupancy check (which DOES count
        // them) and the failed-drop streak reset in drop_batch bounds the
        // retry to once per grace period.
        let backends = procarray::CountDBBackends(d.oid)?;
        if let Some(idle_ns) = streaks.observe(d.oid, backends, now_ns) {
            if idle_ns >= grace_ns {
                victims.push((d.oid, &d.name));
            }
        }
    }
    // Databases that left the candidate set restart their streak from
    // scratch if they ever come back.
    streaks.retain_seen(&seen);

    if victims.is_empty() {
        return Ok(());
    }
    let names: Vec<&str> = victims.iter().map(|&(_, n)| n).collect();
    let _ = log_report(
        LOG,
        format!(
            "pgrust ephemeral-db janitor: reaping {} idle database(s) (default grace {}s): {}",
            victims.len(),
            default_grace_secs,
            names.join(", ")
        ),
    );
    drop_batch(&victims, Some(streaks))?;
    Ok(())
}

/// Batch-drop: each drop in its own transaction through the sanctioned
/// skip-checkpoint dropdb wrapper, then ONE immediate checkpoint for the
/// cycle if anything was dropped (spec item 2; the deferral safety analysis
/// lives on `dbcommands::dropdb_skip_checkpoint`). Per-database errors are
/// contained AND reset the database's streak (`streaks`, when the caller
/// tracks one): "being accessed by other users" is an EXPECTED race (a
/// client reconnected between the backend count and the drop — dropdb's own
/// occupancy check is the safety mechanism, force=false on purpose), and
/// the database falls back to re-earning a FULL grace period. Without the
/// reset, a drop that dropdb refuses deterministically — a prepared
/// transaction (invisible to CountDBBackends, counted by
/// CountOtherDBBackends) or a logical-replication subscription — would be
/// re-attempted every tick forever: each attempt holds the database's
/// AccessExclusiveLock through CountOtherDBBackends' ~5s retry loop
/// (stretching every cycle and starving the very connection that could
/// resolve the block) and emits a contained error report per tick. With the
/// reset, retries cost at most one attempt per grace period.
fn drop_batch(victims: &[(Oid, &str)], mut streaks: Option<&mut StreakTracker>) -> PgResult<usize> {
    let mut dropped = 0usize;
    for &(oid, name) in victims {
        match drop_one(oid, name) {
            Ok(true) => dropped += 1,
            // Skipped: vanished, re-minted under a new oid, or pinned
            // mid-cycle. Nothing was dropped; no checkpoint owed for it.
            Ok(false) => {}
            Err(e) => {
                if let Some(s) = streaks.as_deref_mut() {
                    s.reset(oid);
                }
                contain(e, &format!("dropping database \"{name}\""))?;
            }
        }
    }
    if dropped > 0 {
        checkpointer::RequestCheckpoint(
            transam_xlog::CHECKPOINT_IMMEDIATE
                | transam_xlog::CHECKPOINT_FORCE
                | transam_xlog::CHECKPOINT_WAIT,
        )?;
    }
    Ok(dropped)
}

/// Drop one enumerated victim. Returns true if the database was dropped,
/// false if the drop was skipped.
fn drop_one(oid: Oid, name: &str) -> PgResult<bool> {
    drop_one_gated(oid, name, dropdb_this_victim)
}

/// drop_one's gate-then-drop choreography, generic over the drop action so
/// the pre-drop pin re-check is unit-testable (the reap.rs convention:
/// decision logic separated from catalog/xact I/O — the pinsoak gate's E2E
/// NON-COVERAGE note owes exactly this check to a unit test). Production
/// injects `dropdb_this_victim`, which needs a booted catalog; tests inject
/// a probe.
fn drop_one_gated(
    oid: Oid,
    name: &str,
    drop_action: impl FnOnce(Oid, &str) -> PgResult<bool>,
) -> PgResult<bool> {
    // Pin re-check, immediately before the drop: a pgrust_pin_database()
    // call that returned true after this cycle's candidate scan must still
    // protect the database — the window between enumeration and this point
    // spans the whole batch (potentially seconds). The residual race — a
    // pin landing after this check, while the drop below is in flight — is
    // the documented contract boundary on registry::pin(): pin BEFORE
    // abandoning.
    if registry::is_pinned(name) {
        let _ = log_report(
            LOG,
            format!(
                "pgrust ephemeral-db janitor: skipping drop of \"{name}\": pinned during this cycle"
            ),
        );
        return Ok(false);
    }
    drop_action(oid, name)
}

/// The real drop action: the sanctioned skip-checkpoint dropdb wrapper in
/// its own transaction.
fn dropdb_this_victim(oid: Oid, name: &str) -> PgResult<bool> {
    let cx = mcx::MemoryContext::new("pgrust janitor dropdb");
    xact::StartTransactionCommand()?;
    // missing_ok=true: the database may have vanished since enumeration
    // (manual drop, rename). expected_oid: a same-named database re-minted
    // since enumeration earned NONE of this cycle's eligibility — dropdb
    // re-resolves the name under AccessExclusiveLock, compares oids, and
    // skips on mismatch. force=false: never terminate a backend that won
    // the reconnect race.
    let dropped = dbcommands::dropdb_skip_checkpoint(cx.mcx(), name, true, false, Some(oid))?;
    xact::CommitTransactionCommand()?;
    Ok(dropped)
}

fn loc(func: &'static str) -> types_error::ErrorLocation {
    types_error::ErrorLocation::new(
        "crates/backend/postmaster/janitor/src/main_loop.rs",
        0,
        func,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pre-drop pin re-check (the pinsoak gate's owed unit test:
    /// a pin landing inside the enumeration->drop window is not provocable
    /// from bash timing). RELEASE-effective — plain asserts, no
    /// debug_assert. Deleting the is_pinned re-check in drop_one_gated
    /// fails this test: the injected drop action would run on a pinned
    /// database.
    ///
    /// ONE test function on purpose (the registry.rs convention), under the
    /// crate-wide pin-table test lock: the pin table is process-global and
    /// registry_semantics transiently fills it to capacity.
    #[test]
    fn pre_drop_pin_recheck_gates_the_drop() {
        use core::cell::Cell;

        let _table = registry::test_pin_table_lock();

        // A pin that landed mid-cycle (after enumeration chose this victim,
        // before its drop) must stop the drop action from running AT ALL,
        // and the skip must report Ok(false) — "nothing dropped, no
        // checkpoint owed" (drop_batch's contract).
        let pinned = "tv_dropgate_pinned";
        assert!(registry::pin(pinned).unwrap());
        let ran = Cell::new(false);
        let r = drop_one_gated(90201, pinned, |_, _| {
            ran.set(true);
            Ok(true)
        });
        assert!(
            matches!(r, Ok(false)),
            "pinned victim must be skipped as Ok(false), got {r:?}"
        );
        assert!(
            !ran.get(),
            "drop action ran on a pinned database: the pre-drop pin re-check is gone"
        );
        assert!(registry::unpin(pinned));

        // Unpinned: the gate passes straight through to the action with the
        // enumerated (oid, name) intact, and returns its verdict unchanged.
        let free = "tv_dropgate_free";
        let ran = Cell::new(false);
        let r = drop_one_gated(90202, free, |oid, name| {
            ran.set(true);
            assert_eq!(oid, 90202, "gate must forward the enumerated oid");
            assert_eq!(name, free, "gate must forward the victim name");
            Ok(true)
        });
        assert!(
            matches!(r, Ok(true)),
            "unpinned drop verdict passes through, got {r:?}"
        );
        assert!(ran.get(), "unpinned victim must reach the drop action");
    }
}
