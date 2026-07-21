//! PARTWISE-MORSELS plan-time probe (night/partitionwise-morsels,
//! increment 1): Gather suppression for the PLAIN (ungrouped) count/sum
//! fold over ONE partitioned table — the first keyed sub-shape of the
//! legacy partitioned-append family, now a REAL matrix class
//! (`CoverClass::PartwisePlainFold`, m5-coverage.tsv row
//! `partwise-plain-fold`, covered/runtime). DEFAULT ON since the
//! GL-PARTWISE-1 flip (2026-07-21); `PGRUST_LANE_V2_PARTWISE=0|off` is the
//! kill switch (t35 exact-spelling flipped-kill; restores the keep-Gather
//! posture byte-for-byte).
//!
//! LIVES IN ITS OWN MODULE by the night-run coordination note: another lane
//! is widening probe classes in `m5_suppress.rs`, so this lane's additions
//! are one 10-line hook there (the partitioned-parent branch of
//! `classify_covered`) plus this self-contained file.
//!
//! What suppressing buys: the planner's parallel shape for this query is
//! `FinalizeAgg → Gather → PartialAgg → (Parallel) Append → scans` (the
//! regress corpus's dominant Parallel Append form — 12 of the 26 plan
//! sites, see the census in scratchpad/night/partitionwise-morsels-design
//! .md). Suppressed, the serial plan `Agg → Append → SeqScan×N` reaches the
//! executor and the knob's OTHER half — the runtime partition-as-morsel arm
//! (`execmain/src/lanev2/runtime_partwise.rs`, SAME env spelling: knob
//! coherence, a keyed shape whose arm is disarmed would land on serial) —
//! engages it at pool DOP.
//!
//! Probe ⊂ walk (risk P1, the strictly-narrower law): every gate below has
//! a matching-or-wider executor-admission refusal, and the shapes the probe
//! CANNOT prove at plan time (child fold-prefix armability, fusibility) are
//! covered by the planner-choice guards — unindexed heap/pgrcolumnar leaf
//! partitions with statistics, no quals, bare-Var count/sum tlist — that
//! make the suppressed serial plan shape (all-SeqScan children, no
//! projections, Fold-classified agg) certain. False negatives cost "legacy
//! instead of runtime" (safe); the guards above are what keep the false-
//! positive (suppress-then-refuse ⇒ serial-instead-of-legacy) channel shut.
//!
//! NAMED not-keyed sub-shapes (uncovered by construction; the census maps
//! each to corpus sites): quals (plan-time pruning interaction), grouped
//! partitionwise agg (partition_aggregate.out's main body — increment ≥2 on
//! the grouped sink), runtime/PARAM pruning (partition_prune.out),
//! UNION-ALL Appends (setops prefilter), inheritance parents (relkind 'r'),
//! sub-partitioned/foreign/indexed children, min/max/avg tlists (v1
//! whitelist is count/sum — the increment's byte-parity envelope), poly
//! numeric states.

use types_error::PgResult;
use types_nodes::parsenodes::{Query, RTEKind};
use crate::m5_suppress::{is_whitelisted_agg, size_floors_enabled, trace_armed};
use crate::run::PlannerRun;
use types_pathnodes::AMFLAG_PGRCOLUMNAR;

/// Knob — DEFAULT ON since the GL-PARTWISE-1 flip;
/// `PGRUST_LANE_V2_PARTWISE=0|off` kills (t35 exact-spelling flipped-kill).
/// The executor arm reads the IDENTICAL spelling
/// (`lanev2/runtime_partwise.rs`) — the GROUPSINK knob-coherence law.
/// FLIP PROVENANCE: GL-PARTWISE-1 (scratchpad/night/fleet-ab-parallelism.md,
/// 2026-07-21 @ a88c58496e70): witnessed 3-way ladder wins/ties every
/// DOP{4,8,16} x {heap,cb,skew} cell (rt/legacy 0.998→0.333), partitioned
/// byte-identity spot 3-way + ON==OFF PASS, headline-bank flatness 0.9786
/// hot (probe provably silent on unpartitioned banks).
pub(crate) fn partwise_probe_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_LANE_V2_PARTWISE").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// Increment-1 aggregate whitelist: count(*)/count(any)/sum(int2/4/8) — a
/// deliberate SUBSET of m5_suppress's PLAIN_FOLD_AGGS (pg_proc OIDs of
/// record, same vendored REL 18.3 table). min/max/avg widen in a later
/// increment once the byte-parity battery covers their partials across
/// partitions.
const F_COUNT_STAR: u32 = 2803;
const F_COUNT_ANY: u32 = 2147;
const F_SUM_INT8: u32 = 2107;
const F_SUM_INT4: u32 = 2108;
const F_SUM_INT2: u32 = 2109;
const PARTWISE_FOLD_AGGS: &[u32] =
    &[F_COUNT_STAR, F_COUNT_ANY, F_SUM_INT8, F_SUM_INT4, F_SUM_INT2];

/// Classify a single-RangeTblRef query whose RTE is a partitioned parent
/// (`relkind 'p'`, `inh`) — reached from `classify_covered`'s hook, so the
/// structural prefilter (SELECT, no sublinks/CTEs/setops/HAVING/windows/
/// grouping sets/DISTINCT ON/row marks) already passed. `Ok(true)` ⇒
/// suppress Gather for this query.
pub(crate) fn classify_partitionwise(
    run: &mut PlannerRun<'_>,
    parse: &Query<'_>,
    rti: usize,
) -> PgResult<bool> {
    // --- Plain (ungrouped) agg, no composition: Agg must be the plan root
    // (a Sort/Limit above it is a walk refusal — the suppress-then-refuse
    // direction), one output row.
    if !parse.hasAggs
        || !parse.groupClause.is_nil()
        || !parse.distinctClause.is_nil()
        || !parse.sortClause.is_nil()
        || parse.limitCount.is_some()
        || parse.limitOffset.is_some()
    {
        return Ok(false);
    }
    // No quals in v1: keeps the pruning interaction out of the envelope
    // (plan-time pruning would be fine — pruned children never reach the
    // Append — but the executor arm's census/prewhere economics per child
    // are unmeasured; a qual also risks a projected child scan shape).
    let Some(top) = parse.jointree else { return Ok(false) };
    if top.quals.is_some() {
        return Ok(false);
    }
    // Every tlist entry a whitelisted plain aggregate (bare int-family Var
    // args on the parent rel or count(*)).
    if parse.targetList.is_nil() {
        return Ok(false);
    }
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
        if !is_whitelisted_agg(tle.expr, rti, PARTWISE_FOLD_AGGS) {
            return Ok(false);
        }
    }

    // --- Children: every leaf partition a plain unindexed relation with a
    // sized RelOptInfo, uniform AM (all heap or all pgrcolumnar). Refusals:
    // sub-partitioned children (relkind 'p' — their Append nests), foreign
    // partitions, indexed children (index paths could cost a child plan
    // shape the executor arm refuses), missing/unsized child rels.
    let mut nchildren = 0usize;
    let mut uniform_cb: Option<bool> = None;
    let mut rows = 0.0f64;
    let mut pages = 0.0f64;
    for ai in 0..run.root.append_rel_list.len() {
        let (parent, child) = {
            let a = &run.root.append_rel_list[ai];
            (a.parent_relid as usize, a.child_relid as usize)
        };
        if parent != rti {
            return Ok(false); // multi-level expansion — not the flat v1 shape
        }
        let Some(crte) = parse.rtable.nth(child - 1).as_range_tbl_entry() else {
            return Ok(false);
        };
        if crte.rtekind != RTEKind::RTE_RELATION
            || crte.relkind != types_rel::RELKIND_RELATION
            || crte.tablesample.is_some()
        {
            return Ok(false);
        }
        let Some(crel_id) = run.root.simple_rel_array.get(child).copied().flatten() else {
            return Ok(false);
        };
        let crel = run.root.rel(crel_id);
        if !crel.indexlist.is_empty() {
            return Ok(false);
        }
        let is_cb = crel.amflags & AMFLAG_PGRCOLUMNAR != 0;
        match uniform_cb {
            None => uniform_cb = Some(is_cb),
            Some(prev) if prev != is_cb => return Ok(false),
            _ => {}
        }
        rows += crel.rows.max(0.0);
        pages += f64::from(crel.pages);
        nchildren += 1;
    }
    // <2 children: a one-child Append is elided by setrefs (the single-rel
    // classes own the residue); 0 children = nothing to parallelize.
    if nchildren < 2 {
        return Ok(false);
    }
    let is_cb = uniform_cb.expect("children walked");

    // --- PROVISIONAL per-AM engagement floors, KEPT at the GL-PARTWISE-1
    // flip: the single-rel fold classes' floors reused per AM
    // (CbPlainAggFold / HeapCmpFoldPrefix, notes/m5-5-floors.md), applied
    // to the SUM of the children — the engagement claims the concatenated
    // space, so the sum is the economically meaningful size. The letter's
    // ladder ran floors-off at 10M (every cell wins/ties), which validates
    // the covered region but does NOT place the crossover; the reuse stays
    // the guard. Per-AM shape is why this class is rectangle-retained in
    // class_guard/cover_class_curve (NO_GUARD there; the floor lives HERE).
    // NAMED FOLLOW-UP (non-blocking): floor-calibration mini-ladder
    // ({100k..2.5M} x dop{4,8}, floors-off B-cells) to re-place per-AM
    // floors from partitioned measurements.
    if size_floors_enabled() {
        let dop = guc_tables::runtime_pool::runtime_dop();
        let ok = if is_cb {
            dop >= 12 || rows <= 1_500_000.0
        } else {
            rows >= 1_000_000.0 && dop >= 12
        };
        if !ok {
            if trace_armed() {
                eprintln!(
                    "m5-suppress-floor: partwise children={nchildren} rows={rows:.0} \
                     pages={pages:.0} dop={dop} => gather stands"
                );
            }
            return Ok(false);
        }
    }
    // Matrix consult (the drift-guarded routing surface): the admission
    // above keys the class; whether the class routes runtime is the
    // BOOTSTRAP_MATRIX/tsv pair's verdict, same as every covered sibling.
    let covered = crate::m5_suppress::class_covered(crate::m5_suppress::CoverClass::PartwisePlainFold);
    if covered && trace_armed() {
        let _ = run;
        eprintln!(
            "m5-suppress-partwise: engine=runtime children={nchildren} rows={rows:.0} \
             cb={is_cb} => gather suppressed"
        );
    }
    Ok(covered)
}

#[cfg(test)]
mod tests {
    #[test]
    fn knob_default_on_flipped_kill() {
        // GL-PARTWISE-1 flip: env unset ⇒ the probe is ARMED (default ON);
        // the kill spellings are exactly "0"/"off" (t35 flipped-kill law).
        if std::env::var("PGRUST_LANE_V2_PARTWISE").is_err() {
            assert!(super::partwise_probe_enabled());
        }
    }

    #[test]
    fn matrix_class_is_covered() {
        // The class the classifier consults must be a covered BOOTSTRAP row
        // (the tsv side is tied by bootstrap_matrix_matches_tsv).
        assert!(crate::m5_suppress::class_covered(
            crate::m5_suppress::CoverClass::PartwisePlainFold
        ));
    }

    #[test]
    fn whitelist_is_count_sum_subset() {
        // The v1 whitelist must stay a strict subset of the plain-fold
        // vocabulary (every keyed agg is walk-admissible), and must NOT
        // contain min/max/avg (the increment's byte-parity envelope).
        for oid in super::PARTWISE_FOLD_AGGS {
            assert!(
                crate::m5_suppress::PLAIN_FOLD_AGGS.contains(oid),
                "partwise whitelist OID {oid} outside PLAIN_FOLD_AGGS"
            );
        }
        assert_eq!(super::PARTWISE_FOLD_AGGS.len(), 5);
    }
}
