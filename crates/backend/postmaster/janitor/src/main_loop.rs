//! The janitor's background-worker body: connect, startup sweep, then the
//! ~500ms reap loop. ALL lifecycle mutations serialize through this loop —
//! a design invariant D2's mint path relies on.
//!
//! THE PREFIX IS THE CONTRACT (ruling 2026-08-05): arming the janitor with
//! `pgrust.ephemeral_db_prefix` hands it the whole matching namespace. The
//! startup sweep unconditionally drops EVERY prefix-matching non-template
//! database — pre-existing or leftover alike (templates are exempt via
//! IS_TEMPLATE, as always) — with an informative log line naming what was
//! dropped. There is no adoption guard, no PAUSED state, no marker file,
//! and no provenance tracking: a database named under the prefix is
//! ephemeral by definition.

use elog::elog as log_report;
use init_small::globals as g;
use procsignal::ThreadSignalHandler::Simple;
use types_core::Oid;
use types_error::{PgResult, ERRCODE_ADMIN_SHUTDOWN, FATAL, LOG, WARNING};
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

use crate::dbscan::{scan_prefix_rows, DbRow};
use crate::reap::{reap_candidate, StreakTracker};
use crate::{maint, mint, pool, prewarm, registry, seal};

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

    // Startup sweep (spec item 3), scheduled via the deferred-sweep request
    // and run by the first tick so its failures are contained and RETRIED
    // by the loop instead of disabling the janitor. Unconditional — THE
    // PREFIX IS THE CONTRACT (module doc): every prefix-matching
    // non-template database is dropped, pre-existing or leftover alike.
    // (Between server start and the first tick a client may reconnect to a
    // leftover; it then blocks the drop via dropdb's own occupancy check
    // and falls back to normal grace-based reaping. Pins do not survive
    // restart; any pin present by sweep time was taken in that reconnect
    // window and is honored — sweep_pass re-reads the pin table.)
    registry::request_sweep();

    let mut streaks = StreakTracker::new();
    let mut maint_state = maint::MaintState::new();

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

        // Deferred startup sweep (requested at janitor start above). A
        // contained failure re-arms the request: the startup sweep must not
        // silently downgrade to grace-based reaping because one pass failed.
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

        // pgrust_seal_template servicing (seal.rs state machine), AFTER
        // mint servicing (waiter latency outranks the rare seal) and
        // BEFORE the warm-pool replenish (a template sealed this tick is
        // then visible to the same tick's replenish probe). Per-entry
        // failures are contained inside the pass; only FATALs reach here.
        if let Err(e) = seal::seal_pass() {
            contain(e, "template seal pass")?;
        }

        // D3 warm-pool replenish, BETWEEN mint servicing and the reap pass
        // (recorded ordering decision): after the deferred sweep (fresh
        // spares must not be minted just to be swept), after mint servicing
        // (waiters outrank refill, and handed-out slots are visible to the
        // deficit count), before reap enumeration (new spares are
        // registered/shielded before the pass that would otherwise see them
        // as zero-connection candidates).
        if let Err(e) = pool::replenish_pass(&prefix) {
            contain(e, "warm-pool replenish pass")?;
        }

        // Post-mint prewarm dispatch (prewarm.rs), AFTER replenish (spares
        // minted this tick dispatch this same tick) and BEFORE the reap
        // pass (order-indifferent for safety — every touch target is
        // Ensure-shielded or spare-shielded — but a touch backend appearing
        // before the reap enumeration keeps the streak observation honest
        // within the tick). Strictly off any waiter's critical path: the
        // mint sites only enqueue; workers launch here.
        if let Err(e) = prewarm::dispatch_pass() {
            contain(e, "prewarm dispatch pass")?;
        }

        if let Err(e) = reap_pass(&prefix, &mut streaks) {
            contain(e, "reap pass")?;
        }

        // Shared-catalog maintenance (maint.rs), AFTER the reap pass so
        // this tick's drops count toward the churn trigger.
        if let Err(e) = maint::maintenance_pass(&mut maint_state) {
            contain(e, "shared-catalog maintenance")?;
        }

        // Late warm-handout re-check, LAST work of the turn (the
        // tick-quantized-dispatch fix, part 3): an Ensure posted while
        // replenish/prewarm/reap/maint ran above would otherwise wait out
        // the NEXT full turn even against a stocked pool — its wake landed
        // on an already-running loop. Warm entries are served here
        // (~0.03ms renames); cold arrivals stay Pending and cost one
        // latch-immediate turn as before (mint::late_handout_pass's
        // rationale).
        if let Err(e) = mint::late_handout_pass() {
            contain(e, "late warm-handout pass")?;
        }

        // Retire resolved Ensure entries whose waiters left and whose
        // fresh-mint shield linger expired; retire terminal seal entries
        // whose waiters left (no linger — the flip committed before Done).
        registry::gc_ensures(pg_clock::mono_ns());
        registry::gc_seals();
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

/// `list_prefix_databases` plus the UNFILTERED live-oid set from the same
/// single scan (reap pass only): the oid set feeds the dead-template
/// flush-mark pruning — templates live outside the prefix, so the filtered
/// rows cannot drive it.
fn list_prefix_databases_all_oids(prefix: &str) -> PgResult<(Vec<DbRow>, Vec<Oid>)> {
    xact::StartTransactionCommand()?;
    let mut all_oids: Vec<Oid> = Vec::new();
    let rows = crate::dbscan::scan_prefix_rows_collect(prefix, Some(&mut all_oids))?;
    xact::CommitTransactionCommand()?;
    Ok((rows, all_oids))
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
                registry::spare_shields(&d.name),
                registry::seal_shields(&d.name),
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
    let (rows, all_oids) = list_prefix_databases_all_oids(prefix)?;
    // Flush-mark hygiene, piggybacked on the tick's one catalog scan: drop
    // marks whose template no longer exists (a DROP DATABASE clears no
    // mark — the observed-unseal sites key on a live tuple — and the
    // rebuilds-get-a-NEW-name recipe would otherwise leak one dead slot
    // per rebuild until marking silently stops at the table bound and
    // every batch re-pays the pre-checkpoint).
    registry::retain_template_flush_marks(&all_oids);
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
            registry::spare_shields(&d.name),
            registry::seal_shields(&d.name),
        ) {
            continue;
        }
        seen.push(d.oid);
        // One GLOBAL grace (pgrust.ephemeral_db_grace): the per-template
        // override surface was deleted with pgrust_set_template_grace
        // (ruling 2026-08-05 — pin a database, or raise the global grace).
        let grace_ns = default_grace_secs * 1_000_000_000;
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
pub(crate) fn drop_batch(
    victims: &[(Oid, &str)],
    mut streaks: Option<&mut StreakTracker>,
) -> PgResult<usize> {
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
        // Shared-catalog churn (maint.rs): one pg_database delete (plus
        // shdepend cleanup) per drop.
        registry::note_catalog_churn(dropped as u64);
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
    // Warm-pool spare re-check, same last-instant discipline as pins: spare
    // registration is janitor-loop-internal today (replenish and reap run
    // sequentially in one thread), so this is defensive rather than
    // race-closing — but the shield contract says "exempt while listed" and
    // the enumeration-to-drop window spans the whole batch; a future
    // registration path outside the loop must not silently lose spares.
    if registry::spare_shields(name) {
        let _ = log_report(
            LOG,
            format!(
                "pgrust ephemeral-db janitor: skipping drop of \"{name}\": listed as a warm-pool \
                 spare"
            ),
        );
        return Ok(false);
    }
    // Seal re-check, same last-instant discipline: a pgrust_seal_template()
    // posted from a backend after this cycle's candidate scan (its entry
    // shields the instant post_seal returns) must still protect the target
    // — the seal pass that will drive it runs in this same loop, but the
    // POST is backend-side and can land anywhere inside the
    // enumeration-to-drop window.
    if registry::seal_shields(name) {
        let _ = log_report(
            LOG,
            format!(
                "pgrust ephemeral-db janitor: skipping drop of \"{name}\": a seal request is in \
                 flight for it"
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

        // The warm-pool spare re-check gates identically (deleting the
        // spare_shields re-check in drop_one_gated fails this).
        let spare = "tv_dropgate_spare";
        assert!(registry::add_spare(registry::SpareEntry {
            name: spare.to_string(),
            oid: 90203,
            template_name: "tpl_gate".to_string(),
            template_oid: 90200,
            template_connectable: false,
        }));
        let ran = Cell::new(false);
        let r = drop_one_gated(90203, spare, |_, _| {
            ran.set(true);
            Ok(true)
        });
        assert!(
            matches!(r, Ok(false)),
            "listed spare must be skipped as Ok(false), got {r:?}"
        );
        assert!(
            !ran.get(),
            "drop action ran on a listed spare: the pre-drop spare re-check is gone"
        );
        assert!(registry::remove_spare(spare));

        // The seal re-check gates identically (deleting the seal_shields
        // re-check in drop_one_gated fails this): a non-terminal seal entry
        // shields; completing it releases the gate.
        let sealing = "tv_dropgate_sealing";
        registry::set_janitor_proc(Some(11));
        let registry::PostSeal::Posted(gen) = registry::post_seal(sealing, 12) else {
            panic!("expected Posted");
        };
        let ran = Cell::new(false);
        let r = drop_one_gated(90204, sealing, |_, _| {
            ran.set(true);
            Ok(true)
        });
        assert!(
            matches!(r, Ok(false)),
            "seal-in-flight victim must be skipped as Ok(false), got {r:?}"
        );
        assert!(
            !ran.get(),
            "drop action ran under an in-flight seal: the pre-drop seal re-check is gone"
        );
        registry::complete_seal(gen, Ok(()));
        let ran = Cell::new(false);
        let r = drop_one_gated(90204, sealing, |_, _| {
            ran.set(true);
            Ok(true)
        });
        assert!(matches!(r, Ok(true)), "terminal seal stops shielding");
        assert!(ran.get());
        registry::remove_seal_waiter(gen, 12);
        registry::gc_seals();
        registry::set_janitor_proc(None);
    }
}
