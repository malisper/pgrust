//! M5-3 — coverage-keyed Gather suppression (docs/design/m5-planner.md
//! §2.3, branch m5-design-v2 @ bc18ae12c): THE one planner touch of M5
//! phase 1.
//!
//! Under `pgrust.parallel_engine = runtime`, a plan shape whose coverage-
//! matrix row is COVERED must not be handed to Gather (the runtime's
//! admission walks require "not already in parallel mode"): the planner
//! suppresses Gather/GatherMerge path generation for it, the serial-shaped
//! plan reaches the executor, and the M5-1 router engages the runtime.
//! Uncovered rows keep their Gather paths exactly as today (legacy engine
//! executes them). Under the default `legacy` engine this module is inert
//! — one cached-bool load behind call sites that already early-return when
//! no partial paths exist — and plans are byte-identical to today.
//!
//! Probe law (§2.3, risk P1): the probe is a conservative CLASS check
//! (relation AM × shape class × composition qualifiers), deliberately
//! COARSER and STRICTLY NARROWER than the executor admission walks.
//! False negatives (probe says uncovered, walk would have admitted) cost
//! only "legacy instead of runtime" — safe. False positives (probe
//! suppresses, walk then refuses) cost "serial instead of legacy-parallel"
//! — so every class below whitelists only shapes the §1.1 walk censuses
//! admit, and anything unrecognized is uncovered.
//!
//! MATRIX OF RECORD (reconciled at m5-integration): the class table below
//! is pinned against the `probe_key` column of the LIVING coverage matrix,
//! crates/backend/executor/execmain/src/lanev2/m5-coverage.tsv (the M5-1 router artifact — one file, one
//! (class × spill/topn/bytes) row-key vocabulary, asserted by the unit
//! test below; the separate bootstrap TSV is deleted). ROW-FLIP TRANCHE 1
//! (m5-integration-r2): the original seven bootstrap classes plus TWO
//! flipped rows — CbTopnBoundedIntKeys (bounded top-N, sort arm) and
//! CbHashJoinPlainAgg (plain agg over one two-cbstore-rel join, hashjoin
//! arm) — route runtime; living-matrix rows the probe cannot key at plan
//! time carry probe_key "-" and keep Gather regardless of their route_to
//! flag (the bootstrap-narrowing law — safe false negatives, upgraded per
//! class in future M5-3 row-flip increments with review + measurements).
//! Per-class measured comparisons: scripts/m5-rowflip-measure-e2e.sh (the
//! §4.4 vehicle; ledger rows in the lane notes).
//!
//! Kill switches / gates (outermost first):
//!   * `pgrust.parallel_engine` unset/`legacy` (the default) — inert.
//!   * `PGRUST_M5_SUPPRESS=0|off` — suppression's own kill, engine GUC
//!     untouched (guc_tables::parallel_engine).
//!   * `PGRUST_RUNTIME=1` + `pgrust.lane_executor` required, else the
//!     engine degrades to legacy loud-once (§2.2 — never suppress a
//!     Gather the runtime cannot pick up).
//!   * `PGRUST_M5_GROUPBY_HIGH_FLOOR=<ngroups>` — the groupby_high
//!     legacy-hold boundary (§10 default taken: groupby_high stays legacy
//!     until parity), default 1e6 estimated groups.
//!   * `PGRUST_M5_SUPPRESS_TRACE=1` — one stderr line per suppressed
//!     query (class, rel OID, group estimate) for the refusal-rate
//!     reports.

use types_error::PgResult;
use types_nodes::parsenodes::{Query, RTEKind};
use types_nodes::primnodes::{Aggref, Var, AGGKIND_NORMAL};
use types_nodes::{CmdType, LimitOption, Node};
use crate::run::PlannerRun;
use types_pathnodes::AMFLAG_CBSTORE;

// ---------------------------------------------------------------------------
// The bootstrap coverage classes (matrix rows the probe can key).
// ---------------------------------------------------------------------------

/// Shape classes the probe matrix knows. Every variant corresponds to one
/// or more probe_key rows of crates/backend/executor/execmain/src/lanev2/m5-coverage.tsv (asserted by
/// `tests` below);
/// rows carry §2.4 composition qualifiers as documentation — the executor
/// walk owns their enforcement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoverClass {
    /// cbstore seq-scan folds / plain agg (scan arm + plain-agg sink):
    /// whitelisted order-insensitive-exact aggregates, no GROUP BY.
    CbPlainAggFold,
    /// hashed GROUP BY over cbstore, int-family NOT-NULL-agnostic Var keys
    /// (walk enforces nullable-image refusal); spill-ELIGIBLE row.
    /// groupby_high stays legacy via the group-estimate floor (§10).
    CbGroupedAggIntKeys,
    /// hashed GROUP BY over cbstore with exactly one text/varchar key
    /// (default collation) among the Var keys; spill-DISABLED row
    /// (§2.4 law 2c: canonical-bytes engagements refuse under memory
    /// pressure — expected, serial-correct).
    CbGroupedAggTextKey,
    /// GROUP BY + ORDER BY <whitelisted agg> LIMIT n over cbstore (the
    /// m3-sort-b combine-phase top-N composition, q17/q18/q31–33 family);
    /// §2.4 law 2b degrade rules are arm-internal.
    CbGroupedAggTopN,
    /// SELECT DISTINCT over cbstore, int-family Var keys (distinct sink);
    /// ORDER BY + LIMIT above are walk-admitted.
    CbDistinctIntKeys,
    /// Bare `count(*)` over a plain heap rel, no quals (rowdrive car 1,
    /// StorelessCount direct morsel drive; block floor is arm-internal).
    HeapPlainCountStar,
    /// Heap CMP fold prefix (M1-b): count(col)/min(int)/max(int) over a
    /// plain heap rel, no quals, int-family args (text-first prefix and
    /// min(text) are walk refusals, so the probe never keys them).
    HeapCmpFoldPrefix,
    /// M5-3 row flip 1 (m5-integration-r2): bounded top-N over cbstore
    /// (sort arm shape a) — ORDER BY int-family Var keys + LIMIT without
    /// OFFSET/WITH TIES, all-Var tlist. Full sort (no LIMIT) stays the
    /// uncovered fullsort-shape-b row.
    CbTopnBoundedIntKeys,
    /// M5-3 row flip 2 (m5-integration-r2): plain (ungrouped) whitelisted
    /// aggregation over ONE explicit two-cbstore-relation join (the
    /// hashjoin arm's agg-over-HashJoin shape): single JoinExpr of a
    /// phase-1/right family, >=1 hashjoinable int-family equi clause,
    /// NEITHER rel indexed (index paths could cost a serial merge/NL plan
    /// the walk refuses — the strictly-narrower guard against the
    /// serial-instead-of-legacy false positive), both sides estimated
    /// nbatch==1 (the flipped row is hashjoin-nbatch1; the m35 spill row
    /// keeps its own future flip). Multi-build-side joins (2+ JoinExprs)
    /// classify uncovered — the m5p1-flagged SQL admission gap.
    CbHashJoinPlainAgg,
}

/// One bootstrap matrix row: class key, covered verdict, §2.4 qualifiers.
pub struct MatrixRow {
    pub class: CoverClass,
    pub covered: bool,
    /// Composition qualifiers of record (documentation; asserted against
    /// the TSV so coverage claims and routing flags cannot drift apart).
    pub qualifiers: &'static str,
}

/// The STATIC bootstrap matrix (design §4.1 route-to column, narrowed to
/// the classes this probe can key at plan time). Uncovered §4.1 rows
/// (hash-join family, bounded top-N scan, full sort, parallel
/// index/index-only/bitmap, Parallel Append/partitionwise, parallel
/// writes, FDW, merge join, groupby_high, DISTINCT text/date, avg/numeric
/// agg states, heap LIKE quals) are represented by the probe returning
/// None — they keep Gather exactly as today and appear as covered=false
/// rows in the TSV artifact.
pub const BOOTSTRAP_MATRIX: &[MatrixRow] = &[
    MatrixRow {
        class: CoverClass::CbPlainAggFold,
        covered: true,
        qualifiers: "whitelist=count/sum/avg/min/max-int; order-insensitive-exact partials",
    },
    MatrixRow {
        class: CoverClass::CbGroupedAggIntKeys,
        covered: true,
        qualifiers: "spill-eligible; ngroups<groupby_high_floor; byval-transition aggs only",
    },
    MatrixRow {
        class: CoverClass::CbGroupedAggTextKey,
        covered: true,
        qualifiers: "spill-disabled (canonical key bytes, §2.4 law 2c); <=1 text key, deterministic default collation; ngroups<groupby_high_floor",
    },
    MatrixRow {
        class: CoverClass::CbGroupedAggTopN,
        covered: true,
        qualifiers: "top-N spec armed => pass-through/adopt degrade (§2.4 law 2b); single agg sort key + LIMIT",
    },
    MatrixRow {
        class: CoverClass::CbDistinctIntKeys,
        covered: true,
        qualifiers: "int-family keys only; ORDER BY+LIMIT above admitted; spill-eligible",
    },
    MatrixRow {
        class: CoverClass::HeapPlainCountStar,
        covered: true,
        qualifiers: "rowdrive StorelessCount; no quals; block floor arm-internal",
    },
    MatrixRow {
        class: CoverClass::HeapCmpFoldPrefix,
        covered: true,
        qualifiers: "no quals; int-family args; excludes bare count(*) (own row), text prefixes",
    },
    MatrixRow {
        class: CoverClass::CbTopnBoundedIntKeys,
        covered: true,
        qualifiers: "int-family keys, single+multi (inc-5); LIMIT no OFFSET; relaxed tie-order default (probe-budget guard); full sort NOT keyed",
    },
    MatrixRow {
        class: CoverClass::CbHashJoinPlainAgg,
        covered: true,
        qualifiers: "one JoinExpr, phase-1+right families; hashable int equi key; unindexed rels only; both sides nbatch==1 estimate (spill row unflipped); multi-build-side = uncovered (m5p1 gap)",
    },
];

fn class_covered(class: CoverClass) -> bool {
    BOOTSTRAP_MATRIX.iter().any(|r| r.class == class && r.covered)
}

// ---------------------------------------------------------------------------
// Whitelists (pg_proc OIDs of record, verified against the vendored
// REL 18.3 pg_proc.dat) and type keys.
// ---------------------------------------------------------------------------

const F_COUNT_STAR: u32 = 2803; // count()
const F_COUNT_ANY: u32 = 2147; // count(any)
const F_SUM_INT8: u32 = 2107;
const F_SUM_INT4: u32 = 2108;
const F_SUM_INT2: u32 = 2109;
const F_AVG_INT8: u32 = 2100;
const F_AVG_INT4: u32 = 2101;
const F_AVG_INT2: u32 = 2102;
const F_MAX_INT8: u32 = 2115;
const F_MAX_INT4: u32 = 2116;
const F_MAX_INT2: u32 = 2117;
const F_MIN_INT8: u32 = 2131;
const F_MIN_INT4: u32 = 2132;
const F_MIN_INT2: u32 = 2133;

/// Plain-fold (scan-arm) aggregate whitelist: the order-insensitive-exact
/// partial kinds of §1.1 (CountStar/Any, Sum ring, AvgAccum/Int128Avg,
/// strict byval Min/Max) keyed by builtin OID over int-family args.
const PLAIN_FOLD_AGGS: &[u32] = &[
    F_COUNT_STAR,
    F_COUNT_ANY,
    F_SUM_INT8,
    F_SUM_INT4,
    F_SUM_INT2,
    F_AVG_INT8,
    F_AVG_INT4,
    F_AVG_INT2,
    F_MAX_INT8,
    F_MAX_INT4,
    F_MAX_INT2,
    F_MIN_INT8,
    F_MIN_INT4,
    F_MIN_INT2,
];

/// Grouped-sink aggregate whitelist: COMBINE_WHITELIST byval transitions
/// only — PolyInt128/NumericAgg states (avg(int*), sum(int8)) are walk
/// refusals on the grouped path (relocation car), so the probe excludes
/// them here even though the plain fold admits them.
const GROUPED_SINK_AGGS: &[u32] = &[
    F_COUNT_STAR,
    F_COUNT_ANY,
    F_SUM_INT4,
    F_SUM_INT2,
    F_MAX_INT8,
    F_MAX_INT4,
    F_MAX_INT2,
    F_MIN_INT8,
    F_MIN_INT4,
    F_MIN_INT2,
];

/// Heap CMP fold prefix whitelist (M1-b): count(col)/min(int)/max(int).
const HEAP_CMP_AGGS: &[u32] = &[
    F_COUNT_ANY,
    F_MAX_INT8,
    F_MAX_INT4,
    F_MAX_INT2,
    F_MIN_INT8,
    F_MIN_INT4,
    F_MIN_INT2,
];

const INT2OID: u32 = 21;
const INT4OID: u32 = 23;
const INT8OID: u32 = 20;
const TEXTOID: u32 = 25;
const VARCHAROID: u32 = 1043;
const DEFAULT_COLLATION_OID: u32 = 100;

fn is_int_family(typ: u32) -> bool {
    matches!(typ, INT2OID | INT4OID | INT8OID)
}

fn is_text_family(typ: u32) -> bool {
    matches!(typ, TEXTOID | VARCHAROID)
}

/// The §10 groupby_high legacy-hold boundary: estimated groups at or above
/// this stay legacy (the radix-exchange arm still wins there, 2.73× vs
/// 2.23×). Env-overridable for calibration sweeps.
fn groupby_high_floor() -> f64 {
    static FLOOR: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *FLOOR.get_or_init(|| {
        std::env::var("PGRUST_M5_GROUPBY_HIGH_FLOOR")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| *v > 0.0)
            .unwrap_or(1_000_000.0)
    })
}

fn trace_armed() -> bool {
    static ARMED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ARMED.get_or_init(|| {
        matches!(std::env::var("PGRUST_M5_SUPPRESS_TRACE").as_deref(), Ok("1"))
    })
}

// ---------------------------------------------------------------------------
// The probe.
// ---------------------------------------------------------------------------

/// The §2.3 `runtime_covered` probe, memoized per planner run. True ⇒ the
/// calling choke point must generate NO Gather/GatherMerge paths for this
/// query. Only ever true when `pgrust.parallel_engine = runtime` (plus the
/// module-doc gates) AND the top-level query classifies into a covered
/// bootstrap-matrix row.
pub(crate) fn m5_suppress_gather(run: &mut PlannerRun<'_>) -> PgResult<bool> {
    // Subquery levels never suppress in the bootstrap probe (nested
    // engagements are walk-refusal territory — params/SubPlan contexts);
    // the memo is top-level state, so non-top levels return unmemoized.
    if run.root.query_level != 1 {
        return Ok(false);
    }
    if let Some(v) = run.m5_suppress_gather {
        return Ok(v);
    }
    // The engine GUC is per-session and cannot change inside one planner
    // invocation, so memoizing the whole verdict (gate included) is sound.
    let verdict = if !guc_tables::parallel_engine::m5_gather_suppression_active() {
        false
    } else {
        classify_covered(run)?
    };
    run.m5_suppress_gather = Some(verdict);
    Ok(verdict)
}

/// Classify the top-level query into a bootstrap class and consult the
/// matrix. Every early `false` is "uncovered ⇒ keep Gather exactly as
/// today" (the safe direction, risk P1).
fn classify_covered(run: &mut PlannerRun<'_>) -> PgResult<bool> {
    let parse = run.parse();

    // Structural prefilter: the walks admit single-relation SELECT
    // pipelines only; anything else is uncovered wholesale.
    if parse.commandType != CmdType::CMD_SELECT
        || parse.resultRelation != 0
        || parse.utilityStmt.is_some()
        || parse.hasWindowFuncs
        || parse.hasTargetSRFs
        || parse.hasSubLinks
        || parse.hasDistinctOn
        || parse.hasRecursive
        || parse.hasModifyingCTE
        || parse.hasForUpdate
        || parse.hasRowSecurity
        || !parse.cteList.is_nil()
        || !parse.groupingSets.is_nil()
        || parse.havingQual.is_some()
        || !parse.windowClause.is_nil()
        || parse.setOperations.is_some()
        || !parse.rowMarks.is_nil()
        || !parse.mergeActionList.is_nil()
        || !parse.returningList.is_nil()
        || parse.limitOption == LimitOption::LIMIT_OPTION_WITH_TIES
    {
        return Ok(false);
    }

    // Exactly one FROM item: a plain relation (the single-rel classes) or
    // ONE explicit JoinExpr (row flip 2, CbHashJoinPlainAgg). A flat
    // comma-join (`FROM a, b WHERE ...`, fromlist len 2) and nested join
    // trees (multi-build-side — the m5p1 SQL admission gap) classify
    // uncovered by construction.
    let Some(top) = parse.jointree else { return Ok(false) };
    if top.fromlist.len() != 1 {
        return Ok(false);
    }
    if let Some(je) = top.fromlist.nth(0).as_join_expr() {
        return classify_join_covered(run, parse, je);
    }
    let Some(rtr) = top.fromlist.nth(0).as_range_tbl_ref() else {
        return Ok(false);
    };
    let rti = rtr.rtindex as usize;
    let Some(rte) = parse.rtable.nth(rti - 1).as_range_tbl_entry() else {
        return Ok(false);
    };
    if rte.rtekind != RTEKind::RTE_RELATION
        || rte.relkind != types_rel::RELKIND_RELATION
        || rte.inh
        || rte.tablesample.is_some()
    {
        return Ok(false);
    }
    let Some(rel_id) = run.root.simple_rel_array.get(rti).copied().flatten() else {
        return Ok(false);
    };
    let is_cb = run.root.rel(rel_id).amflags & AMFLAG_CBSTORE != 0;
    let has_quals = top.quals.is_some();

    // --- DISTINCT over cbstore, int keys -----------------------------------
    if !parse.distinctClause.is_nil() {
        if !is_cb || parse.hasAggs || !parse.groupClause.is_nil() {
            return Ok(false);
        }
        // Plain SELECT DISTINCT: every tlist entry a non-junk int-family
        // Var on the scanned rel (junk entries would be DISTINCT-invalid
        // sort keys anyway; refuse them).
        for tle_node in &parse.targetList {
            let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
            if tle.resjunk || !is_covered_key_var(tle.expr, rti, |t| is_int_family(t)) {
                return Ok(false);
            }
        }
        return finish(run, CoverClass::CbDistinctIntKeys, rte.relid, 0.0);
    }

    // --- Aggregate shapes ----------------------------------------------------
    if !parse.hasAggs {
        // Bounded top-N over cbstore (row flip 1, CbTopnBoundedIntKeys):
        // ORDER BY int-family Var keys + LIMIT, no OFFSET (WITH TIES is
        // prefiltered above), every tlist entry a plain Var on the rel
        // (the sort arm's emit face; junk sort-key entries are Vars too).
        // Full sort (no LIMIT) stays the uncovered fullsort-shape-b row;
        // heap rels stay uncovered (the arm is cbstore-fusible only).
        if is_cb
            && !parse.sortClause.is_nil()
            && parse.limitCount.is_some()
            && parse.limitOffset.is_none()
        {
            for sc_node in &parse.sortClause {
                let Some(sc) = sc_node.as_sort_group_clause() else { return Ok(false) };
                let Some(tle) = tle_by_sortgroupref(parse, sc.tleSortGroupRef) else {
                    return Ok(false);
                };
                if !is_covered_key_var(tle.expr, rti, is_int_family) {
                    return Ok(false);
                }
            }
            for tle_node in &parse.targetList {
                let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
                if key_var(tle.expr, rti).is_none() {
                    return Ok(false);
                }
            }
            return finish(run, CoverClass::CbTopnBoundedIntKeys, rte.relid, 0.0);
        }
        return Ok(false);
    }

    if parse.groupClause.is_nil() {
        // Plain aggregation, one output row.
        if is_cb {
            if tlist_all_whitelisted_aggs(parse, rti, PLAIN_FOLD_AGGS) {
                return finish(run, CoverClass::CbPlainAggFold, rte.relid, 1.0);
            }
            return Ok(false);
        }
        // Heap rows are no-qual only (LIKE-qual folds are walk refusals;
        // the qualed LIKE census is deliberately not keyed in bootstrap).
        if has_quals || !parse.sortClause.is_nil() {
            return Ok(false);
        }
        if is_bare_count_star(parse) {
            return finish(run, CoverClass::HeapPlainCountStar, rte.relid, 1.0);
        }
        if tlist_all_whitelisted_aggs(parse, rti, HEAP_CMP_AGGS) {
            return finish(run, CoverClass::HeapCmpFoldPrefix, rte.relid, 1.0);
        }
        return Ok(false);
    }

    // --- Grouped aggregation over cbstore ------------------------------------
    if !is_cb {
        return Ok(false);
    }
    // Key discipline: all keys plain Vars on the scanned rel; int-family
    // plus at most one text/varchar key under the deterministic default
    // collation (the c3 canonical-key-bytes classes).
    let mut n_text = 0usize;
    let mut key_refs: Vec<u32> = Vec::new();
    for gc_node in &parse.groupClause {
        let Some(gc) = gc_node.as_sort_group_clause() else { return Ok(false) };
        let Some(tle) = tle_by_sortgroupref(parse, gc.tleSortGroupRef) else {
            return Ok(false);
        };
        let Some(v) = key_var(tle.expr, rti) else { return Ok(false) };
        if is_int_family(v.vartype) {
            // covered
        } else if is_text_family(v.vartype) && v.varcollid == DEFAULT_COLLATION_OID {
            n_text += 1;
            if n_text > 1 {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }
        key_refs.push(gc.tleSortGroupRef);
    }
    // Emit discipline: every tlist entry is a bare group-key Var or a
    // whitelisted sink aggregate (const tlist entries — the q35 refusal —
    // and non-identity emits classify uncovered here).
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
        if tle.ressortgroupref != 0 && key_refs.contains(&tle.ressortgroupref) {
            if key_var(tle.expr, rti).is_none() {
                return Ok(false);
            }
            continue;
        }
        if !is_whitelisted_agg(tle.expr, rti, GROUPED_SINK_AGGS) {
            return Ok(false);
        }
    }

    // Sort/limit composition: none at all (plain grouped emit), or the
    // top-N winner-selection shape — a single whitelisted-aggregate sort
    // key plus LIMIT without OFFSET (q17/q18/q31–33). A sort on the group
    // keys themselves is an ordered-stream consumer (GatherMerge class,
    // uncovered in bootstrap).
    let topn = if parse.sortClause.is_nil() && parse.limitCount.is_none() {
        false
    } else if parse.sortClause.len() == 1
        && parse.limitCount.is_some()
        && parse.limitOffset.is_none()
    {
        let Some(sc) = parse.sortClause.nth(0).as_sort_group_clause() else {
            return Ok(false);
        };
        let Some(tle) = tle_by_sortgroupref(parse, sc.tleSortGroupRef) else {
            return Ok(false);
        };
        if !is_whitelisted_agg(tle.expr, rti, GROUPED_SINK_AGGS) {
            return Ok(false);
        }
        true
    } else {
        return Ok(false);
    };

    // groupby_high hold (§10): estimate the group cardinality off the
    // processed group clause; at or above the floor the class routes
    // legacy (the radix-exchange arm still wins).
    let ngroups = if run.root.processed_groupClause.is_empty() {
        1.0
    } else {
        let clauses =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.processed_groupClause);
        let group_exprs =
            types_pathnodes::run::sortgrouplist_exprs(run, &clauses, &parse.targetList);
        let input_rows = run.root.rel(rel_id).rows.max(1.0);
        crate::selfuncs::estimate_num_groups(run, &group_exprs, input_rows)?
    };
    if ngroups >= groupby_high_floor() {
        return Ok(false);
    }

    let class = if topn {
        CoverClass::CbGroupedAggTopN
    } else if n_text > 0 {
        CoverClass::CbGroupedAggTextKey
    } else {
        CoverClass::CbGroupedAggIntKeys
    };
    finish(run, class, rte.relid, ngroups)
}

/// Row flip 2 (CbHashJoinPlainAgg): plain whitelisted aggregation over one
/// explicit two-cbstore-relation join. Strictly narrower than the
/// runtime_hashjoin walk (probe ⊂ walk, risk P1) PLUS two planner-choice
/// guards the walk cannot express — the probe must also be confident the
/// SERIAL plan will BE an agg-over-HashJoin-over-two-SeqScans:
///   * neither rel carries an index (no serial merge/NL-with-inner-index
///     plan for the costing to prefer; unindexed equi-joins cost to hash);
///   * >=1 hashjoinable int-family equi clause in the JOIN quals.
/// Every early `false` keeps Gather exactly as today.
fn classify_join_covered(
    run: &mut PlannerRun<'_>,
    parse: &Query<'_>,
    je: &types_nodes::primnodes::JoinExpr<'_>,
) -> PgResult<bool> {
    use types_nodes::JoinType;
    // Plain one-row aggregation only (the arm drives a plain agg sink):
    // no grouping, no DISTINCT, no ORDER BY/LIMIT decoration.
    if !parse.hasAggs
        || !parse.groupClause.is_nil()
        || !parse.distinctClause.is_nil()
        || !parse.sortClause.is_nil()
        || parse.limitCount.is_some()
        || parse.limitOffset.is_some()
    {
        return Ok(false);
    }
    // Phase-1 + right join families the walk admits (semi/anti arrive via
    // sublinks, prefiltered upstream).
    if !matches!(
        je.jointype,
        JoinType::JOIN_INNER | JoinType::JOIN_LEFT | JoinType::JOIN_RIGHT | JoinType::JOIN_FULL
    ) {
        return Ok(false);
    }
    // Both arms plain cbstore relations (no nested joins: the
    // multi-build-side SQL shapes are the m5p1 admission gap, uncovered).
    let mut sides = [0usize; 2];
    for (i, arg) in [je.larg, je.rarg].into_iter().enumerate() {
        let Some(rtr) = arg.as_range_tbl_ref() else { return Ok(false) };
        sides[i] = rtr.rtindex as usize;
    }
    let [rti_l, rti_r] = sides;
    let mut relids = [0u32; 2];
    for (i, &rti) in [rti_l, rti_r].iter().enumerate() {
        let Some(rte) = parse.rtable.nth(rti - 1).as_range_tbl_entry() else {
            return Ok(false);
        };
        if rte.rtekind != RTEKind::RTE_RELATION
            || rte.relkind != types_rel::RELKIND_RELATION
            || rte.inh
            || rte.tablesample.is_some()
        {
            return Ok(false);
        }
        relids[i] = rte.relid;
        let Some(rel_id) = run.root.simple_rel_array.get(rti).copied().flatten() else {
            return Ok(false);
        };
        let rel = run.root.rel(rel_id);
        if rel.amflags & AMFLAG_CBSTORE == 0 {
            return Ok(false);
        }
        // Unindexed-only guard (see the fn doc): an index on either side
        // lets the costing pick serial merge/NL shapes the walk refuses.
        if !rel.indexlist.is_empty() {
            return Ok(false);
        }
        // nbatch==1 on this side's estimate (whichever side the planner
        // hashes must fit): the flipped row is hashjoin-nbatch1; larger
        // builds keep Gather until the spill row's own flip.
        let Some(pt_id) = rel.pathtarget_id else { return Ok(false) };
        let width = run.root.pathtarget(pt_id).width;
        let dop = guc_tables::runtime_pool::runtime_dop();
        let (_, nbatch, _, _) = ::nodehash::exec_choose_hash_table_size_full(
            rel.rows.max(1.0),
            width,
            false, // useskew: C PHJ parity
            true,  // try_combined_hash_mem: pooled participant budget
            dop.max(1),
        );
        if nbatch > 1 {
            return Ok(false);
        }
    }
    // >=1 hashjoinable int-family equi clause between the two sides in the
    // JOIN quals (top-level AND terms only).
    let mut has_equi = false;
    let quals: Vec<Node<'_>> = match je.quals {
        None => return Ok(false),
        Some(q) => match q.as_bool_expr() {
            Some(be)
                if matches!(be.boolop, types_nodes::primnodes::BoolExprType::AND_EXPR) =>
            {
                be.args.iter().collect()
            }
            _ => vec![q],
        },
    };
    for qual in quals {
        let Some(op) = qual.as_op_expr() else { continue };
        if op.args.len() != 2 {
            continue;
        }
        let (a, b) = (op.args.nth(0), op.args.nth(1));
        let pair = key_var(a, rti_l)
            .zip(key_var(b, rti_r))
            .or_else(|| key_var(a, rti_r).zip(key_var(b, rti_l)));
        let Some((va, vb)) = pair else { continue };
        if is_int_family(va.vartype)
            && is_int_family(vb.vartype)
            && lsyscache::op_hashjoinable(op.opno, va.vartype)?
        {
            has_equi = true;
            break;
        }
    }
    if !has_equi {
        return Ok(false);
    }
    // Emit discipline: every non-junk tlist entry is a whitelisted plain
    // aggregate whose args live on either joined rel (count(*) included).
    let mut n = 0usize;
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
        if !is_whitelisted_agg_2rti(tle.expr, rti_l, rti_r, PLAIN_FOLD_AGGS) {
            return Ok(false);
        }
        n += 1;
    }
    if n == 0 {
        return Ok(false);
    }
    finish(run, CoverClass::CbHashJoinPlainAgg, relids[0], 0.0)
}

/// Matrix consult + optional trace, shared tail.
fn finish(
    run: &mut PlannerRun<'_>,
    class: CoverClass,
    relid: u32,
    ngroups: f64,
) -> PgResult<bool> {
    let covered = class_covered(class);
    if covered && trace_armed() {
        let _ = run; // (run reserved for a future lane_trace surface)
        eprintln!(
            "m5-suppress: engine=runtime class={class:?} relid={relid} \
             ngroups={ngroups:.0} => gather suppressed"
        );
    }
    Ok(covered)
}

// ---------------------------------------------------------------------------
// Expression helpers.
// ---------------------------------------------------------------------------

/// A bare Var on the scanned rel, user column, current level.
fn key_var<'mcx>(expr: Node<'mcx>, rti: usize) -> Option<&'mcx Var<'mcx>> {
    let v = expr.as_var()?;
    (v.varno as usize == rti && v.varattno > 0 && v.varlevelsup == 0).then_some(v)
}

fn is_covered_key_var(expr: Node<'_>, rti: usize, type_ok: impl Fn(u32) -> bool) -> bool {
    key_var(expr, rti).is_some_and(|v| type_ok(v.vartype))
}

/// A structurally plain, whitelisted Aggref: builtin OID in `whitelist`,
/// no ORDER BY/DISTINCT/FILTER/variadic/ordered-set decoration, args
/// either empty (count(*)) or a single int-family Var on the scanned rel.
fn is_whitelisted_agg(expr: Node<'_>, rti: usize, whitelist: &[u32]) -> bool {
    let Some(agg) = expr.as_aggref() else { return false };
    aggref_plain(agg, rti) && whitelist.contains(&agg.aggfnoid)
}

/// `is_whitelisted_agg` over TWO candidate range-table indexes (the join
/// row flip): the aggregate's single Var arg may live on either joined rel.
fn is_whitelisted_agg_2rti(expr: Node<'_>, rti_l: usize, rti_r: usize, whitelist: &[u32]) -> bool {
    let Some(agg) = expr.as_aggref() else { return false };
    if !whitelist.contains(&agg.aggfnoid) {
        return false;
    }
    aggref_plain(agg, rti_l) || aggref_plain(agg, rti_r)
}

fn aggref_plain(agg: &Aggref<'_>, rti: usize) -> bool {
    if agg.agglevelsup != 0
        || agg.aggkind != AGGKIND_NORMAL
        || agg.aggvariadic
        || !agg.aggorder.is_nil()
        || !agg.aggdistinct.is_nil()
        || agg.aggfilter.is_some()
        || !agg.aggdirectargs.is_nil()
    {
        return false;
    }
    match agg.args.len() {
        0 => agg.aggstar || agg.aggfnoid == F_COUNT_STAR,
        1 => {
            let Some(arg_tle) = agg.args.nth(0).as_target_entry() else { return false };
            is_covered_key_var(arg_tle.expr, rti, is_int_family)
        }
        _ => false,
    }
}

/// Every non-junk tlist entry is a whitelisted Aggref (plain-agg tlists);
/// junk entries (ORDER BY keys not selected) must be whitelisted too.
fn tlist_all_whitelisted_aggs(parse: &Query<'_>, rti: usize, whitelist: &[u32]) -> bool {
    let mut n = 0usize;
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return false };
        if !is_whitelisted_agg(tle.expr, rti, whitelist) {
            return false;
        }
        n += 1;
    }
    n > 0
}

/// Exactly `SELECT count(*)` — one tlist entry, the star count.
fn is_bare_count_star(parse: &Query<'_>) -> bool {
    if parse.targetList.len() != 1 {
        return false;
    }
    let Some(tle) = parse.targetList.nth(0).as_target_entry() else { return false };
    let Some(agg) = tle.expr.as_aggref() else { return false };
    agg.aggfnoid == F_COUNT_STAR
        && agg.args.is_nil()
        && agg.aggfilter.is_none()
        && agg.agglevelsup == 0
        && agg.aggkind == AGGKIND_NORMAL
}

fn tle_by_sortgroupref<'mcx>(
    parse: &Query<'mcx>,
    sgref: u32,
) -> Option<&'mcx types_nodes::primnodes::TargetEntry<'mcx>> {
    if sgref == 0 {
        return None;
    }
    parse
        .targetList
        .iter()
        .filter_map(|n| n.as_target_entry())
        .find(|tle| tle.ressortgroupref == sgref)
}

// ---------------------------------------------------------------------------
// Bootstrap-matrix / TSV drift guard.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// The living-matrix discipline (§4.1, reconciled at m5-integration):
    /// the routing table the probe consults and the ONE checked-in living
    /// artifact (crates/backend/executor/execmain/src/lanev2/m5-coverage.tsv, the M5-1 router file) must
    /// not drift apart. The TSV is the reviewable/reportable surface; this
    /// table is the executable one; this test is the tie. A probe key may
    /// span several matrix rows (CbPlainAggFold keys both cbstore fold
    /// rows); all rows sharing a key must agree on route_to.
    #[test]
    fn bootstrap_matrix_matches_tsv() {
        let tsv = include_str!("../../../../../../crates/backend/executor/execmain/src/lanev2/m5-coverage.tsv");
        let mut keyed: std::collections::BTreeMap<String, bool> = std::collections::BTreeMap::new();
        for line in tsv.lines() {
            if line.starts_with('#') || line.trim().is_empty() {
                continue;
            }
            let cols: Vec<&str> = line.split('\t').collect();
            if cols[0] == "class" {
                continue; // header (schema pinned by the router's test)
            }
            assert_eq!(cols.len(), 9, "malformed TSV row: {line}");
            let (probe_key, route_to) = (cols[7], cols[6]);
            if probe_key == "-" {
                continue; // not plan-time keyable: probe returns None, Gather stands
            }
            let runtime = route_to == "runtime";
            if let Some(prev) = keyed.insert(probe_key.to_string(), runtime) {
                assert_eq!(
                    prev, runtime,
                    "rows sharing probe_key {probe_key} disagree on route_to"
                );
            }
        }
        // Every probe key in the TSV maps onto exactly one code row with the
        // same verdict, and vice versa.
        assert_eq!(
            keyed.len(),
            BOOTSTRAP_MATRIX.len(),
            "distinct TSV probe keys != BOOTSTRAP_MATRIX rows"
        );
        for row in BOOTSTRAP_MATRIX {
            let key = format!("{:?}", row.class);
            let runtime = *keyed
                .get(&key)
                .unwrap_or_else(|| panic!("class {key} missing from TSV probe_key column"));
            assert_eq!(
                runtime, row.covered,
                "route-to drift for {key}: TSV vs BOOTSTRAP_MATRIX"
            );
        }
    }
}
