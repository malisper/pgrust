//! Shared-catalog maintenance (docs/design/test-views.md prewarm addendum,
//! F2): the janitor's own mint/drop/rename traffic churns the SHARED
//! catalogs — pg_database (one insert per mint, one delete per drop, one
//! update per handout rename/chown), pg_shdepend (owner dependencies), and
//! pg_db_role_setting (the M4 setting copies) — and the recommended rig
//! runs `autovacuum = off`, so dead tuples accumulate unboundedly: the
//! mint-profile finding shows the fresh-database first-use cost growing
//! 4.5 -> 9.3ms over ~200 mints from exactly this bloat. A low-frequency
//! janitor tick step VACUUMs the three catalogs from the janitor's home
//! session via the internal `commands_vacuum::vacuum` entry (the
//! autovacuum worker's own call shape — a NodeList of oid-only
//! VacuumRelation nodes, `is_top_level = true` between plain transaction
//! commands).
//!
//! CADENCE (recorded decision — constants, not GUCs): the natural trigger
//! unit is CATALOG CHURN, not wall time — an idle janitor must never
//! vacuum, and a busy one must vacuum in proportion to the garbage it
//! makes. `MAINT_CHURN_OPS = 32` lifecycle ops keeps pg_database's dead
//! fraction bounded well below the measured 2x first-use degradation knee
//! (~200 ops), at under-once-a-minute frequency for a typical CI mint
//! rate; `MAINT_MIN_INTERVAL_NS = 60s` spaces RUNS so a mint storm cannot
//! turn the tick into a vacuum loop (the first run after start is exempt —
//! a threshold crossing fires immediately, which is also what makes the
//! race-suite witness deterministic). An operator knob would be policy
//! without a policy-maker: the janitor is the only writer that matters
//! here, and it can see its own churn.
//!
//! Containment: the pass returns into the tick's `contain()` like every
//! other step (errors never kill the loop); `last_run_ns` is stamped
//! BEFORE the vacuum so a persistently failing vacuum retries once per
//! interval, not once per tick; the churn counter resets only on SUCCESS
//! so a contained failure stays armed. Paused janitors skip the step for
//! free (the tick's paused branch `continue`s before it).

use elog::elog as log_report;
use mcx::Mcx;
use tableam_vocab::{
    VacOptValue, VacuumParams, VACOPT_PROCESS_MAIN, VACOPT_PROCESS_TOAST,
    VACOPT_SKIP_DATABASE_STATS, VACOPT_SKIP_LOCKED, VACOPT_VACUUM,
};
use types_core::{InvalidOid, Oid};
use types_error::{PgResult, LOG};
use types_nodes::parsenodes::VacuumRelation;
use types_nodes::{Node, NodeList};

use crate::registry;

/// Shared-catalog lifecycle ops between maintenance runs (rationale above).
pub(crate) const MAINT_CHURN_OPS: u64 = 32;

/// Minimum spacing between maintenance RUNS (first run exempt).
pub(crate) const MAINT_MIN_INTERVAL_NS: u64 = 60 * 1_000_000_000;

/// The shared catalogs the janitor churns. pg_shdepend and
/// pg_db_role_setting have TOAST relations (VACOPT_PROCESS_TOAST covers
/// them in the same pass, C's VACUUM default).
const SHARED_CATALOGS: [Oid; 3] = [
    types_core::DATABASE_RELATION_ID, // pg_database (1262)
    1214,                             // pg_shdepend (SharedDependRelationId)
    2964,                             // pg_db_role_setting (DbRoleSettingRelationId)
];

/// Janitor-loop-local cadence state (restart-lossy with the loop itself).
pub(crate) struct MaintState {
    last_run_ns: u64,
}

impl MaintState {
    pub(crate) fn new() -> Self {
        MaintState { last_run_ns: 0 }
    }
}

/// The pure cadence decision (unit-tested): due iff the churn threshold is
/// met AND the spacing since the last RUN (0 = never ran) has elapsed.
fn maintenance_due(churn: u64, now_ns: u64, last_run_ns: u64) -> bool {
    churn >= MAINT_CHURN_OPS
        && (last_run_ns == 0 || now_ns.saturating_sub(last_run_ns) >= MAINT_MIN_INTERVAL_NS)
}

/// One maintenance tick step (main_loop, after the reap pass so this tick's
/// drops count toward the trigger). Errors propagate to the tick's
/// contain().
pub(crate) fn maintenance_pass(state: &mut MaintState) -> PgResult<()> {
    let churn = registry::catalog_churn();
    let now = pg_clock::mono_ns();
    if !maintenance_due(churn, now, state.last_run_ns) {
        return Ok(());
    }
    state.last_run_ns = now;
    vacuum_shared_catalogs()?;
    registry::reset_catalog_churn();
    let ms = pg_clock::mono_ns().saturating_sub(now) as f64 / 1e6;
    // The maintenance witness line (race-suite maintenance phase greps it),
    // written AFTER the vacuum committed — the line is truthful when read.
    let _ = log_report(
        LOG,
        format!(
            "pgrust ephemeral-db janitor: vacuumed shared catalogs pg_database, pg_shdepend, \
             pg_db_role_setting after {churn} lifecycle op(s) ({ms:.1} ms)"
        ),
    );
    Ok(())
}

/// VACUUM (SKIP_LOCKED, PROCESS_TOAST) of the three shared catalogs through
/// the internal entry, in the janitor's home session. The autovacuum-worker
/// call shape: `vacuum()` is called between plain transaction commands
/// (never a transaction BLOCK, so its PreventInTransactionBlock passes),
/// commits the entry transaction itself, runs each relation in its own
/// transaction, and leaves a fresh transaction open for this caller's
/// commit. On Err the open transaction is left for the tick's contain() to
/// abort (the service_serial convention). SKIP_LOCKED: the janitor must
/// never queue behind a DDL lock; a skipped catalog is retried next
/// interval. SKIP_DATABASE_STATS: vac_update_datfrozenxid churns the very
/// pg_database rows this pass is cleaning (and autovacuum's per-table calls
/// skip it the same way).
fn vacuum_shared_catalogs() -> PgResult<()> {
    let cx = mcx::MemoryContext::new("pgrust janitor shared-catalog maintenance");
    xact::StartTransactionCommand()?;
    let mcx = cx.mcx();
    let rels = shared_catalog_rel_list(mcx)?;
    let params = VacuumParams {
        options: VACOPT_VACUUM
            | VACOPT_PROCESS_MAIN
            | VACOPT_PROCESS_TOAST
            | VACOPT_SKIP_LOCKED
            | VACOPT_SKIP_DATABASE_STATS,
        // -1s resolve to the GUC defaults inside vacuum_get_cutoffs, the
        // interactive-VACUUM shape.
        freeze_min_age: -1,
        freeze_table_age: -1,
        multixact_freeze_min_age: -1,
        multixact_freeze_table_age: -1,
        is_wraparound: false,
        log_min_duration: -1,
        index_cleanup: VacOptValue::Unspecified,
        truncate: VacOptValue::Unspecified,
        toast_parent: InvalidOid,
        max_eager_freeze_failure_rate: guc_tables::vars::vacuum_max_eager_freeze_failure_rate
            .read(),
        nworkers: -1,
    };
    commands_vacuum::vacuum(mcx, &rels, &params, None, true)?;
    xact::CommitTransactionCommand()?;
    Ok(())
}

/// Oid-only VacuumRelation nodes (the autovacuum_do_vac_analyze
/// construction) for the three shared catalogs.
fn shared_catalog_rel_list<'mcx>(mcx: Mcx<'mcx>) -> PgResult<NodeList<'mcx>> {
    let mut it = SHARED_CATALOGS.iter();
    let first = vacuum_rel_node(mcx, *it.next().expect("non-empty"))?;
    let mut list = NodeList::make1(mcx, first)?;
    for &oid in it {
        list.lappend(mcx, vacuum_rel_node(mcx, oid)?)?;
    }
    Ok(list)
}

fn vacuum_rel_node<'mcx>(mcx: Mcx<'mcx>, oid: Oid) -> PgResult<Node<'mcx>> {
    let mut n = Node::build::<VacuumRelation>(mcx)?;
    n.relation = None;
    n.oid = oid;
    Ok(n.seal())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pure cadence decision (RELEASE-effective plain asserts): churn
    /// threshold inclusive, first run exempt from spacing, spacing gates
    /// re-runs, and an idle janitor (zero churn) is never due.
    #[test]
    fn maintenance_cadence_semantics() {
        // Idle: never due, regardless of time.
        assert!(!maintenance_due(0, 0, 0));
        assert!(!maintenance_due(0, u64::MAX, 0));
        // Below threshold: not due.
        assert!(!maintenance_due(MAINT_CHURN_OPS - 1, 1_000, 0));
        // At threshold, never ran: due immediately (the witness
        // determinism clause).
        assert!(maintenance_due(MAINT_CHURN_OPS, 0, 0));
        // Re-run inside the spacing window: not due even at high churn.
        let last = 10_000;
        assert!(!maintenance_due(
            u64::MAX,
            last + MAINT_MIN_INTERVAL_NS - 1,
            last
        ));
        // Spacing elapsed (inclusive boundary): due again.
        assert!(maintenance_due(
            MAINT_CHURN_OPS,
            last + MAINT_MIN_INTERVAL_NS,
            last
        ));
    }
}
