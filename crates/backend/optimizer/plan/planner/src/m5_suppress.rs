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
//! CbHashJoinPlainAgg (plain agg over one two-pgrcolumnar-rel join, hashjoin
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
//!     until parity), default 4e6 estimated groups (raised from 1e6,
//!     night/routing-floor-fixes; setting 1000000 restores the old bound).
//!   * `PGRUST_M5_SUPPRESS_TRACE=1` — one stderr line per suppressed
//!     query (class, rel OID, group estimate) for the refusal-rate
//!     reports.

use types_error::PgResult;
use types_nodes::parsenodes::{Query, RTEKind};
use types_nodes::primnodes::{Aggref, Var, AGGKIND_NORMAL};
use types_nodes::{CmdType, LimitOption, Node};
use crate::run::PlannerRun;
use types_pathnodes::{AMFLAG_PGRCOLUMNAR, AMFLAG_PGRCOLUMNAR_ZEROCNT};

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
    /// pgrcolumnar seq-scan folds / plain agg (scan arm + plain-agg sink):
    /// whitelisted order-insensitive-exact aggregates, no GROUP BY. WS-COVER
    /// (phase3-close §3.2) widened the keyed shape to min/max(date) — the
    /// fold arm's classify_trans admits it at the I32 lane (date is int4-width
    /// byval), so the same fold economics + floor apply (CB q7 flip).
    CbPlainAggFold,
    /// hashed GROUP BY over pgrcolumnar, int-family NOT-NULL-agnostic Var keys
    /// (walk enforces nullable-image refusal); spill-ELIGIBLE row.
    /// groupby_high stays legacy via the group-estimate floor (§10).
    CbGroupedAggIntKeys,
    /// hashed GROUP BY over pgrcolumnar with exactly one text/varchar key
    /// (default collation) among the Var keys; spill-DISABLED row
    /// (§2.4 law 2c: canonical-bytes engagements refuse under memory
    /// pressure — expected, serial-correct).
    CbGroupedAggTextKey,
    /// GROUP BY + ORDER BY <whitelisted agg> LIMIT n over pgrcolumnar (the
    /// m3-sort-b combine-phase top-N composition, q17/q18/q31–33 family);
    /// §2.4 law 2b degrade rules are arm-internal.
    CbGroupedAggTopN,
    /// Grouped COUNT(DISTINCT <int Var>) over pgrcolumnar, int-family GROUP
    /// keys (the runtime distinct sink's sorted-distinct feed — CB q9/q10
    /// class); plain whitelisted aggs may ride alongside; single-agg-key
    /// ORDER BY + LIMIT composition is walk-admitted. RE-KEYED at
    /// m5-integration-r2: the bootstrap probe keyed plain `SELECT
    /// DISTINCT`, whose HashAggregate shape the sink never admits — a
    /// measured suppress-then-refuse false positive (2.66x vs legacy at
    /// dop4); plain SELECT DISTINCT is now UNKEYED (named matrix gap).
    CbDistinctIntKeys,
    /// Bare `count(*)` over a plain heap rel, no quals (rowdrive car 1,
    /// StorelessCount direct morsel drive; block floor is arm-internal).
    HeapPlainCountStar,
    /// Heap CMP fold prefix (M1-b): count(col)/min(int)/max(int) over a
    /// plain heap rel, no quals, int-family args (text-first prefix and
    /// min(text) are walk refusals, so the probe never keys them).
    HeapCmpFoldPrefix,
    /// M5-3 row flip 1 (m5-integration-r2): bounded top-N over pgrcolumnar
    /// (sort arm shape a) — ORDER BY int-family Var keys + LIMIT without
    /// OFFSET/WITH TIES, all-Var tlist. Full sort (no LIMIT) stays the
    /// uncovered fullsort-shape-b row.
    CbTopnBoundedIntKeys,
    /// M5-3 row flip 2 (m5-integration-r2): plain (ungrouped) whitelisted
    /// aggregation over ONE explicit two-pgrcolumnar-relation join (the
    /// hashjoin arm's agg-over-HashJoin shape): single JoinExpr of a
    /// phase-1/right family, >=1 hashjoinable int-family equi clause,
    /// NEITHER rel indexed (index paths could cost a serial merge/NL plan
    /// the walk refuses — the strictly-narrower guard against the
    /// serial-instead-of-legacy false positive), both sides estimated
    /// nbatch==1 (the flipped row is hashjoin-nbatch1; the m35 spill row
    /// keeps its own future flip). Multi-build-side joins (2+ JoinExprs)
    /// classify uncovered — the m5p1-flagged SQL admission gap.
    CbHashJoinPlainAgg,
    /// m5p1 row flip (band 88001): plain (ungrouped) whitelisted aggregation
    /// over THREE-TO-SIX pgrcolumnar relations joined by a CONNECTED graph of
    /// hashjoinable int-family equi clauses (the multibuild walk's 2+ build
    /// sides in one engagement). FROM forms keyed: the flat N-RangeTblRef
    /// FromExpr (comma/INNER form, quals in top.quals) and the left-deep
    /// nested INNER JoinExpr chain (every rarg a plain rel). Planner-choice
    /// guards, per rel: unindexed (no serial merge/NL-with-inner-index
    /// shapes for the costing to prefer), nbatch==1 estimate (EVERY rel —
    /// any of them may be a build side; the multibuild walk is unbatched
    /// only), cbstore AM (heap sides: TPCH-JHEAP keys them knob-gated
    /// behind PGRUST_LANE_V2_JHEAP — the K2 executor feed has been
    /// DEFAULT-ON since the SE9/SE15 flips, the coherence mirror keys its
    /// kills; the earlier "K2 DEFAULT-OFF" claim here was stale).
    /// Everything else —
    /// grouped/distinct/sorted shapes, outer types in nested trees,
    /// disconnected graphs — classifies uncovered by construction.
    CbHashJoinMultiBuild,
    /// SE-AGGJOIN row flip (band 87001): GROUPED (hashed) aggregation over
    /// 2..=6 cbstore relations joined by a CONNECTED int-family equi graph —
    /// the grouped-agg-over-join sink (per-worker hashed builds, grouped
    /// partial export/combine, leader-table absorb + canonical retrieve).
    /// FROM forms keyed: the flat N-RangeTblRef INNER forms and left-deep
    /// nested INNER chains (INNER-ONLY: outer families can plan side-swapped
    /// RIGHT shapes outside the walk's probe-local envelope). Group keys:
    /// bare int2/4/8 Vars (the walk's byval word-equality whitelist is
    /// wider — probe narrower). Aggregates: the PLAIN_FOLD_AGGS whitelist —
    /// numeric-family int states (avg/sum int2/4/8, AvgAccum/Int128 inline)
    /// INCLUDED, unlike the scan-grouped GROUPED_SINK_AGGS row (the grouped
    /// sink exports them via the runtime-partial states). Planner-choice
    /// guards: multibuild rel guards verbatim (distinct unindexed cbstore
    /// rels, every rel nbatch==1 — the B1 discipline inherited),
    /// enable_hashagg + enable_hashjoin required ON (either off costs a
    /// sort/merge/NL serial shape the walk refuses — the suppress-then-
    /// refuse direction), BARE-EQUI-ONLY quals (every top-level AND term an
    /// int-family hashjoinable equi clause between distinct rels — residual
    /// filter quals shifted the costing to a top-level Merge Join with full
    /// statistics present, the e2e leg-X5 live finding), statistics on
    /// every join/group key var (statistics-free keys default the join
    /// selectivities into the same merge landing — leg X6), no ORDER BY/
    /// LIMIT/OFFSET/DISTINCT (the Agg must be the plan ROOT), ngroups
    /// floored under BOTH the groupby_high boundary and the export-cap
    /// headroom.
    CbHashJoinGroupedAgg,
    /// SE-AGGPOLY row flip (band 101001, knob-gated `PGRUST_LANE_V2_AGG_POLY`
    /// — the probe keys this class ONLY when the executor arm is armed, the
    /// GROUPSINK coherence law): PLAIN (ungrouped) aggregation over ONE
    /// UNINDEXED plain heap relation, quals allowed, where every tlist entry
    /// is a whitelisted bare-int-Var aggregate (PLAIN_FOLD_AGGS) or a plain
    /// sum/avg(NUMERIC) over ANY parallel-safe single-argument expression
    /// (the poly export manifest's NumericAvg class — the runtime scan
    /// arm's per-row drive runs C's checked transition program, so the arg
    /// shape is free; helper-side evaluation safety is the planner's own
    /// `is_parallel_safe`, applied to the quals too), with at least one
    /// numeric aggregate (all-int shapes keep their existing rows). No
    /// sort/limit/offset (the Agg must be the plan ROOT — a Limit/Sort
    /// above it is an agg-not-plan-root walk refusal, the
    /// suppress-then-refuse direction); unindexed keeps the suppressed
    /// serial plan shape certain (Agg over SeqScan). tpch q06 class.
    AggPolyHeapPlain,
    /// M5-5 Meta-over-Gather (the band-2a q30 handoff): plain (ungrouped)
    /// FOOTER-ANSWERABLE aggregation over one plain pgrcolumnar rel with NO
    /// quals — count(*)/count(col), min/max over bare int-family Vars,
    /// and sum/avg over int2/int4 AFFINE transforms (`v±k`, `v*k`; the
    /// lanefold classify_arg admission, divk==1 only — classify_meta
    /// refuses division) or bare int8 Vars. The serial lane's Meta arm
    /// answers these from part footers in milliseconds; the
    /// planner-parallel FinalizeAgg→Gather→PartialAgg shape escapes the
    /// Meta arm's Agg-over-SeqScan scope entirely (band-2a measured
    /// q30@100M: 3.9–7.2s parallel vs ~5ms footer — a ~700x hole
    /// neutralized only by forced vectors). Suppressing Gather keeps the
    /// serial plan; if the Meta arm's runtime footer checks refuse (guard
    /// interval / non-MVCC), the runtime scan-arm fold is the engagement
    /// fallback (lanefold admits the same affine forms), so no
    /// suppress-then-refuse serial cliff class opens.
    CbMetaFooterAgg,
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
        qualifiers: "whitelist=count/sum/avg/min/max-int + min/max(date) (I32 fold, WS-COVER §3.2); order-insensitive-exact partials",
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
        qualifiers: "grouped count(DISTINCT int); int-family group keys; plain-agg passengers; agg-key ORDER BY+LIMIT admitted; spill-eligible; plain SELECT DISTINCT unkeyed (hash-shape gap)",
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
        qualifiers: "one JoinExpr, phase-1+right families; hashable int equi key; unindexed rels only; both sides nbatch==1 estimate (spill row unflipped); multi-build-side = the m5p1 CbHashJoinMultiBuild row",
    },
    MatrixRow {
        class: CoverClass::CbHashJoinMultiBuild,
        covered: true,
        qualifiers: "m5p1: 3-6 cbstore rels, flat/left-deep-INNER forms; connected int equi graph; unindexed; EVERY rel nbatch==1 (walk is unbatched-only); plain whitelisted aggs; floor reused from hashjoin-nbatch1 (provisional — GL-M5P1-1 letter owed)",
    },
    MatrixRow {
        class: CoverClass::CbHashJoinGroupedAgg,
        covered: true,
        qualifiers: "se-aggjoin: 2-6 cbstore rels, flat/left-deep INNER-only forms; connected int equi graph; unindexed distinct rels; EVERY rel nbatch==1; int2/4/8 bare-Var group keys; PLAIN_FOLD_AGGS incl. avg/sum numeric-family int states; enable_hashagg+enable_hashjoin required; Agg-root only (no sort/limit/distinct); ngroups < min(groupby_high, 64k export headroom); floor reused from hashjoin-nbatch1 (provisional — GL-AGGJOIN-1 letter owed)",
    },
    MatrixRow {
        class: CoverClass::CbMetaFooterAgg,
        covered: true,
        qualifiers: "no quals; footer-answerable aggs incl. affine int2/int4 sum/avg (divk==1, lanefold classify_arg forms); Meta lane answers, runtime scan fold is the engagement fallback",
    },
    MatrixRow {
        class: CoverClass::AggPolyHeapPlain,
        covered: true,
        qualifiers: "se-aggpoly (band 101001): keyed ONLY under PGRUST_LANE_V2_AGG_POLY (knob coherence); one unindexed heap rel; quals allowed, is_parallel_safe; tlist = PLAIN_FOLD_AGGS bare-int OR plain sum/avg(numeric) w/ parallel-safe arg exprs, >=1 numeric; Agg-root only (no sort/limit/offset); floor reused from HeapCmpFoldPrefix (provisional — GL-AGGPOLY-1 letter owed)",
    },
];

fn class_covered(class: CoverClass) -> bool {
    BOOTSTRAP_MATRIX.iter().any(|r| r.class == class && r.covered)
}

// ---------------------------------------------------------------------------
// M5-5 engagement-floor guards (the living matrix's floor values; measured
// on the crossover ladder + DOP sweep, notes/m5-5-floors.md — jobs @
// 2159563ff (rows ∈ 100k..5M, dop4) and 37decba75 (5M×dop8/16, 2.5M×dop16),
// fast-profile, medians of 5). Admission ECONOMICS the probe applies before
// suppressing Gather: outside a class's guard the plan keeps Gather, so
// engine=runtime routes the shape to legacy (or the planner's natural
// serial choice) — every guarded-off point was measured at parity, every
// guarded-on point within 5% of best(legacy, serial) or winning.
// min_dop is 12, not 16: the winning point was MEASURED at dop16 and dop8
// loses only mildly (1.06–1.13x); 12–15 is interpolated — and the auto-DOP
// clamp on 15-CPU fleet pods must clear the floor (a 16 floor would flap
// on cores-1 boxes).
// ---------------------------------------------------------------------------

struct FloorGuard {
    /// Below this estimated row count the arm cannot pay back engagement
    /// (or its own executor floor refuses and the serial fallback loses to
    /// legacy Gather) — keep Gather.
    min_rows: f64,
    /// Above this the LEGACY parallel machinery (PHJ / partial agg) beats
    /// the arm at every measured DOP — keep Gather.
    max_rows: f64,
    /// Heap block floor (mirrors the rowdrive arm's
    /// PGRUST_RUNTIME_ROWDRIVE_MIN_BLOCKS=8192 default, runtime_scan.rs:
    /// suppressing below it measured 1.08–1.41x serial-fallback losses).
    min_pages: f64,
    /// DOP-shaped classes: suppress below `min_dop` only when rows ≤
    /// `low_dop_max_rows` (the measured low-DOP win region, if any).
    min_dop: i32,
    low_dop_max_rows: f64,
}

const NO_GUARD: FloorGuard = FloorGuard {
    min_rows: 0.0,
    max_rows: f64::INFINITY,
    min_pages: 0.0,
    min_dop: 0,
    low_dop_max_rows: f64::INFINITY,
};

fn class_guard(class: CoverClass) -> FloorGuard {
    match class {
        // dop4: 1.21–1.26x ≥2.5M (WIN 0.34 at 1M); dop8 1.10; dop16
        // 0.89–1.04.
        CoverClass::CbPlainAggFold => {
            FloorGuard { min_dop: 12, low_dop_max_rows: 1_500_000.0, ..NO_GUARD }
        }
        // Wins everywhere engaged (0.49–0.76 at every measured point).
        CoverClass::CbGroupedAggIntKeys => NO_GUARD,
        // dop4@5M 1.60 / dop8@5M 1.24 (legacy partial-agg dedup wins at
        // this text NDV); dop16 0.95–1.05; dop4 wins ≤2.5M (0.60–0.78).
        CoverClass::CbGroupedAggTextKey => {
            FloorGuard { min_dop: 12, low_dop_max_rows: 3_000_000.0, ..NO_GUARD }
        }
        // Wins everywhere engaged (0.34–0.85).
        CoverClass::CbGroupedAggTopN => NO_GUARD,
        // dop4 1.21–1.24 at every engaged size; dop8 1.06; dop16 0.79–0.90.
        CoverClass::CbDistinctIntKeys => {
            FloorGuard { min_dop: 12, low_dop_max_rows: 0.0, ..NO_GUARD }
        }
        // Suppressing below the rowdrive 64MB block floor measured
        // 1.08–1.41x (arm refuses, serial fallback loses to Gather);
        // above it the arm WINS 0.27–0.37 at every DOP.
        CoverClass::HeapPlainCountStar => FloorGuard { min_pages: 8192.0, ..NO_GUARD },
        // 1.13–1.39x at dop4/8 at EVERY size (the arm engages even at
        // 100k); dop16 wins 0.73–0.76 (≥2.5M measured; 1M floor is the
        // unmeasured-corner conservatism).
        CoverClass::HeapCmpFoldPrefix => {
            FloorGuard { min_rows: 1_000_000.0, min_dop: 12, low_dop_max_rows: 0.0, ..NO_GUARD }
        }
        // RE-FLIPPED at the GL-SORTECON-3 flip increment (night/
        // sort-merge-redesign; measured 2026-07-21 @ 0296033fd, fleet jobs
        // 61f2/3efc/5509 — notes/sort-merge-redesign-lane.md): the COLSTAGE
        // staged accept + GCUT shared cutoff/zone-skip retired the 4.30-8.00x
        // loss (that was the per-row emit ceremony, not ceremony size); the
        // arm now measures rt/serial 0.49-0.09 on the zone-hostile band and
        // <=1.08 worst point semi-hostile at dop4 (damped geomean 1.005),
        // dop 4/8/16, 1M-20M rows, both profiles. min_dop=4: dop1/2 measured
        // LOSING (rand 1.96/1.80, dup 2.18/1.46 rt/serial, 5M local ladder
        // 2026-07-21) — below it keep Gather. Band routing WITHIN the
        // admitted region is the arm's own engage-time zone predicate
        // (runtime_sort.rs ZONE_FRIENDLY_MIN_SKIP_FRAC): zone-friendly
        // shapes refuse to the serial zone-adaptive walk, which beats both
        // engines there.
        CoverClass::CbTopnBoundedIntKeys => {
            FloorGuard { min_dop: 4, low_dop_max_rows: 0.0, ..NO_GUARD }
        }
        // Wins ≤1M (0.41 vs serial-shaped legacy); loses 1.39–1.50x once
        // legacy PHJ engages ≥2.5M at dop≤8, and 1.14 even at dop16@5M
        // (dop16@2.5M's 0.92 marginal win is deliberately forgone for the
        // clean single bound).
        CoverClass::CbHashJoinPlainAgg => FloorGuard { max_rows: 2_000_000.0, ..NO_GUARD },
        // m5p1: PROVISIONAL reuse of the hashjoin-nbatch1 floor (the walk's
        // build/probe economics are the same shared-build machinery per
        // level; the largest side is the ladder's per-table N). The fleet
        // letter (GL-M5P1-1) owns un-provisionalizing or re-measuring it.
        CoverClass::CbHashJoinMultiBuild => FloorGuard { max_rows: 2_000_000.0, ..NO_GUARD },
        // SE-AGGJOIN: PROVISIONAL reuse of the hashjoin-nbatch1 floor (same
        // shared-build machinery per level; the grouped tail adds the
        // export/absorb walk, floored separately by the ngroups guard in
        // classify_aggjoin_grouped). GL-AGGJOIN-1 owns re-measuring.
        CoverClass::CbHashJoinGroupedAgg => FloorGuard { max_rows: 2_000_000.0, ..NO_GUARD },
        // Footer answers are O(1) — never floored.
        CoverClass::CbMetaFooterAgg => NO_GUARD,
        // SE-AGGPOLY: PROVISIONAL reuse of the HeapCmpFoldPrefix guard (the
        // same heap per-row parallel drive; the numeric transition is
        // STRICTLY more per-row work than the int fold it was measured on,
        // which only widens the parallel win region — the reuse errs
        // conservative on the min side). GL-AGGPOLY-1 owns re-measuring.
        CoverClass::AggPolyHeapPlain => {
            FloorGuard { min_rows: 1_000_000.0, min_dop: 12, low_dop_max_rows: 0.0, ..NO_GUARD }
        }
    }
}

/// M5-5 floors kill switch: PGRUST_M5_SIZE_FLOORS=0 disables every guard.
/// The rowflip measure vehicle runs floors-off so engagement economics
/// stay measurable at any (size, dop); production default ON.
fn size_floors_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_M5_SIZE_FLOORS").map_or(true, |v| v.trim() != "0"))
}

/// SE-TEXTDISTINCT (C1 text-distinct + q36 exprkey coverage car, band
/// 86001): the row-executor-removal WS-COVER census's `distinct-text-date-
/// args` (7.58s/7q) + `gap:agg-expr-keys` (q36, 2.42s/1q) rows plan Gather
/// at default because the probe cannot key text-keyed / expression-keyed
/// DISTINCT + grouped-agg shapes — even though the runtime arms ALREADY
/// admit them (the runtime distinct SINK keys canonical-bytes text group
/// keys under a deterministic collation, runtime_distinct.rs module doc; the
/// plain-distinct SINK admits int+text distinct values, runtime_plaindistinct
/// .rs; the exprkey Reduced arm keys reduced-expr-key grouped agg,
/// exprkey.rs decide_reduced). This knob keys those admission gaps.
///
/// DEFAULT ON since night/planner-fix-forced (t34); `PGRUST_LANE_V2_TEXTDISTINCT
/// =0|off` is the kill switch, restoring the pre-flip keep-Gather posture
/// byte-for-byte. The arm suppresses via the knob-path finish
/// (finish_textdistinct) — NOT a BOOTSTRAP_MATRIX class, so the drift guards
/// (`bootstrap_matrix_matches_tsv`, `coverage_matrix_is_consistent`) are
/// untouched; the tsv rows record the flipped default with letter citations
/// (GL-TEXTDIST, knob letter 2026-07-21: the knob is code-inert at t34 ==
/// the measured noise floor 0.9889; grouped engagements q11/q12 0.010/0.011
/// hot vs cpg 0.44, ~40x).
fn textdistinct_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        // night/planner-fix-forced: DEFAULT FLIP OFF->ON (unset = on), the
        // GL-TEXTDIST default-flip win. Eliminates part of the CB_FORCE_PLANS=mt16
        // vector: unforced release selection now suppresses Gather and engages the
        // runtime distinct / plain-distinct / exprkey sinks for the text/int
        // count(DISTINCT) (q5/q6) + reduced-expr-key grouped-agg (q36) shapes the
        // mt16 vector forced (rt/rt16). The arm is proven byte-identical vs C and
        // vs knob-OFF (doc above); this flip is validated on the fleet
        // unforced-vs-mt16 A/B. PGRUST_LANE_V2_TEXTDISTINCT=0/off restores the
        // pre-flip keep-Gather posture for A/B.
        !matches!(
            std::env::var("PGRUST_LANE_V2_TEXTDISTINCT").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// SE-TEXTDISTINCT PLAIN (ungrouped) sub-arm gate — DEFAULT ON (t35
/// routing-flips); `PGRUST_LANE_V2_TEXTDISTINCT_PLAIN=0|off` is the kill
/// switch. HISTORY: night/planner-fix-forced held this OFF because the fleet
/// A/B measured the suppress-Gather arm as a 10M REGRESSION (q5 0.046->0.151,
/// q6 0.081->0.175) — but that was the suppress-then-UNARMED hole: the plain
/// exact-DISTINCT sink armed off the bench GUC alone and never consulted
/// router::arm_dop, so the suppressed plan landed SERIAL with no pool. Fixed
/// at 98a012ba2 (fix(runtime_plaindistinct): arm via router::arm_dop
/// (Distinct)); GL-TEXTDIST-2 re-measure post-fix is GREEN — q5/q6 at forced
/// parity (~0.020/0.045s, floor-fix verification job 7e66, 2026-07-21) — so
/// the sub-arm joins the flipped textdistinct default.
fn textdistinct_plain_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_LANE_V2_TEXTDISTINCT_PLAIN").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// PROVISIONAL floor for the SE-TEXTDISTINCT knob-gated shapes (shared;
/// GL-TEXTDIST fleet letters own re-measuring each shape's real economics).
/// Mirrors the CbGroupedAggTextKey economics (text-keyed grouped, min_dop
/// 12, low-dop win region ≤3M): the census fixture (3M rows, resolved
/// dop≥12) suppresses; the at-scale channels (dop16) suppress; small/low-dop
/// tables keep Gather. Text-key grouped count(DISTINCT) rides the distinct
/// sink whose own `agg_hashgroup_economical_sink` term is the real gate — a
/// probe refusal here only costs "legacy instead of runtime".
fn textdistinct_guard() -> FloorGuard {
    FloorGuard { min_dop: 12, low_dop_max_rows: 3_000_000.0, ..NO_GUARD }
}

/// SE-MKTEXT (Lane-3 probe widening, two-key text car): the ClickBench
/// q17/q18-class `GROUP BY UserID, SearchPhrase` shapes — TWO-key grouped
/// aggregation with one or two default-collation text keys — run 8-39x
/// slower unforced than on the hand-armed runtime agg pool (harvest3arm
/// t32 A/B @ 10M dist-control: q17 0.900s unforced vs 0.061s forced; q18
/// 0.122 vs 0.015) because the probe refuses them at plan time while the
/// runtime agg SINK already owns the shapes end to end: the Mk composite
/// feed packs int+text keys (C2/Mk cars, canonical-bytes merge), the
/// canonical multi-tail encoding carries TWO Intern components
/// (canon-sink car 1, `PGRUST_RUNTIME_AGG_TEXT2`), and the bare-LIMIT
/// group-admission FREEZE owns the q18 composition (band-2a,
/// `PGRUST_RUNTIME_AGG_FREEZE`). This knob keys the admission gaps.
///
/// DEFAULT ON (t35 routing-flips, GL-MKTEXT-1 FLIP-RECOMMENDED);
/// `PGRUST_LANE_V2_MULTIKEY_TEXT=0|off` is the kill switch — every other
/// spelling stays ON (the flipped-kill idiom: only the exact kill spellings
/// disarm). MEASURED (knob letter 2026-07-21, jobs -54df/-46fa @ 4479aae8d,
/// unforced 10M ClickBench): q17 0.861 -> 0.061 hot (14.1x, == the forced
/// ref exactly) via the family's own 16M ceiling; zero regressions across
/// 43q; no new byte-parity diff class. Same spelling in planner and execmain
/// (the AGG_POLY / GROUPSINK knob-coherence law: a keyed shape whose arm is
/// disarmed would suppress Gather and land on serial — BOTH sites flip
/// together). Still owed per the letter (flip mechanics, not blockers): the
/// 16M ceiling measured bound + the min_dop-12 floor reuse re-measure.
fn multikey_text_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        multikey_text_spelling_on(std::env::var("PGRUST_LANE_V2_MULTIKEY_TEXT").as_deref().ok())
    })
}

/// The default-ON kill spelling rule, factored pure for exhaustive unit
/// tests: OFF iff the value is exactly `0` or `off` (the flipped-kill
/// idiom); unset and every other spelling stay ON.
fn multikey_text_spelling_on(v: Option<&str>) -> bool {
    !matches!(v, Some("0") | Some("off"))
}

/// SE-MKTEXT pure shape law (unit-tested): a grouped key census of `nkeys`
/// bare group-key Vars with `n_text` deterministic-default-collation text
/// keys enters the knob-widened family iff it is EXACTLY the two-key
/// int+text or text+text shape. Everything wider fails closed: 3+ keys
/// (with any second text), all-int two-key (existing bootstrap rows), and
/// the single-key shapes (existing rows / sibling cars). Expression keys
/// and non-default collations never reach this law — the surrounding
/// census refuses them first (bare-Var + DEFAULT_COLLATION_OID discipline).
fn mk_text_family_shape_ok(nkeys: usize, n_text: usize) -> bool {
    nkeys == 2 && (1..=2).contains(&n_text)
}

/// SE-MKTEXT engine-kill coherence (the m5p1 `multibuild_enabled`
/// precedent): the runtime agg sink's text cars must be live for the keyed
/// shape — `PGRUST_RUNTIME_AGG_TEXT` (Intern components at all) and, for
/// the two-text census, `PGRUST_RUNTIME_AGG_TEXT2` (the canonical
/// multi-tail encoding). A keyed shape whose car is killed would suppress
/// a Gather the walk then refuses (risk P1's suppress-then-refuse
/// direction). Same spellings as the executor (runtime_agg.rs), own cached
/// reads; both default ON there, so this gate is inert unless someone
/// throws an attribution kill.
fn mk_text_agg_cars_live(n_text: usize) -> bool {
    static T1: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    static T2: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let t1 = *T1.get_or_init(|| {
        !matches!(std::env::var("PGRUST_RUNTIME_AGG_TEXT").as_deref(), Ok("0") | Ok("off"))
    });
    let t2 = *T2.get_or_init(|| {
        !matches!(std::env::var("PGRUST_RUNTIME_AGG_TEXT2").as_deref(), Ok("0") | Ok("off"))
    });
    t1 && (n_text < 2 || t2)
}

/// Freeze-car coherence (SE-MKTEXT + SE-BARELIMIT): the bare-LIMIT
/// composition engages the sink's group-admission freeze — keyed only
/// while `PGRUST_RUNTIME_AGG_FREEZE` (default ON) is live, same spelling
/// as the executor.
fn agg_freeze_car_live() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_RUNTIME_AGG_FREEZE").as_deref(), Ok("0") | Ok("off"))
    })
}

/// The shared default-ON kill spelling (t35 routing-flips): OFF iff exactly
/// `0` or `off`; unset and every other spelling stay ON. Factored pure for
/// the sibling lanes' unit tests (scanpass keeps its own historical
/// default-OFF twin).
fn knob_spelling_on(v: Option<&str>) -> bool {
    !matches!(v, Some("0") | Some("off"))
}

/// SE-EXTRACTKEY (Lane-3 sibling, cb q19 class — the routing map's biggest
/// single probe win, 1.44s @ 10M): `GROUP BY UserID, extract(minute FROM
/// EventTime), SearchPhrase` — the probe's bare-Var key discipline refuses
/// the extract() expr key, yet the SERIAL-lane exprkey Multi arm ALREADY
/// OWNS execution (exprkey.rs `decide_exprkey_mk`: one computed
/// NUMERIC-returning chain key + bare int/text Vars, `int8 + numeric4 +
/// intern4 = 16` — THE q19 shape; the forced arm ran mpwpg=0 with NO pools
/// at 0.088s vs 1.529s legacy-parallel). Suppression-only widening: the
/// knob keys the shape via `classify_extract_exprkey`, the suppressed
/// serial `[Limit<-Sort<-]HashAgg<-SeqScan` plan engages the Multi feed.
/// DEFAULT ON (t35 routing-flips); `PGRUST_LANE_V2_EXPRKEY_EXTRACT=0|off`
/// is the kill switch. GL-EXTRACTKEY-1 (2026-07-21, jobs -54df/-46fa/-75c3
/// @ 4479aae8d) measured the knob safe everywhere (zero deltas across 43q)
/// but held by the then-1e6 groupby_high floor: q19's estimate is 1,516,181
/// (identical to q17's — the extract() key adds nothing), and with the hold
/// bypassed the arm runs q19 at 0.093 hot (16x, forced ref 0.088). The
/// floor's raise to 4e6 LANDED (b12c3fc74, bench letter in-commit), so the
/// re-letter-together-with-the-floor condition is met and the flip engages
/// q19 unforced.
fn extract_exprkey_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        knob_spelling_on(std::env::var("PGRUST_LANE_V2_EXPRKEY_EXTRACT").as_deref().ok())
    })
}

/// SE-CONSTKEY (Lane-3 sibling, cb q35 class, 2.07s @ 10M): `SELECT 1, URL,
/// count(*) … GROUP BY 1, URL` — the const group key fails `key_var` and
/// the const tlist entry fails the emit discipline (the named q35 refusal;
/// matrix row agg-const-tlist). The forced arm (serial + pools) wins 10x,
/// so an engagement exists to key; the const contributes nothing to the
/// partition. The knob admits NON-NULL INT-FAMILY Const group keys (and
/// their tlist entries) alongside the existing bare-Var census — the REAL
/// keys still drive classification and floors. DEFAULT ON (t35
/// routing-flips); `PGRUST_LANE_V2_AGG_CONSTKEY=0|off` is the kill switch.
/// GL-CONSTKEY-1 (2026-07-21, jobs -54df/-46fa/-75c3 @ 4479aae8d) measured
/// the knob safe everywhere (zero deltas across 43q) but held by the
/// then-1e6 groupby_high floor: q35's estimate is 2,625,920 (all URL — the
/// const key contributes nothing), and with the hold bypassed the arm runs
/// q35 at 0.227 hot (9.4x — BEATS the forced ref 0.237). The floor's raise
/// to 4e6 LANDED (b12c3fc74), so the flip engages q35 unforced.
fn agg_constkey_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        knob_spelling_on(std::env::var("PGRUST_LANE_V2_AGG_CONSTKEY").as_deref().ok())
    })
}

/// SE-BARELIMIT (Lane-3 sibling, cb q18-composition class, 0.11s @ 10M):
/// bare `LIMIT k` with NO ORDER BY over a grouped agg falls into the topn
/// else-branch refusal today. The suppressed serial plan is
/// `Limit <- HashAgg <- SeqScan`; the runtime agg sink's group-admission
/// FREEZE (band-2a, `PGRUST_RUNTIME_AGG_FREEZE`) owns the bound and any k
/// groups are a correct answer for an unordered LIMIT. The knob admits the
/// composition for shapes the census otherwise covers (bare-Var keys,
/// GROUPED_SINK_AGGS passengers, no count(DISTINCT), no OFFSET); the
/// groupby_high hold still applies (the floor recalibration lane owns it).
/// The two-key-text family's own freeze branch (SE-MKTEXT) is the
/// more-specific sibling and carries the family ceiling. DEFAULT ON (t35
/// routing-flips, GL-BARELIMIT-1 FLIP-RECOMMENDED); `PGRUST_LANE_V2_
/// AGG_BARELIMIT=0|off` is the kill switch. MEASURED (2026-07-21, jobs
/// -54df/-46fa @ 4479aae8d): q18 0.124 -> 0.016 hot (7.8x, forced ref
/// 0.015) via the freeze composition with MKTEXT; zero regressions.
/// TIE-CLASS NOTE (per the letter): bare-LIMIT-no-ORDER-BY answers change
/// to a different VALID group subset (Q18 PASS-TIE) — callers snapshotting
/// raw bytes will see a change; the tie law accepts it.
fn agg_barelimit_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        knob_spelling_on(std::env::var("PGRUST_LANE_V2_AGG_BARELIMIT").as_deref().ok())
    })
}

/// PROVISIONAL floor for the SE-EXTRACTKEY knob path: the shared
/// text-keyed-grouped economics (the q19 shape carries a text key too).
/// GL-EXTRACTKEY-1 owns re-measuring.
fn extract_exprkey_guard() -> FloorGuard {
    FloorGuard { min_dop: 12, low_dop_max_rows: 3_000_000.0, ..NO_GUARD }
}

/// The shared default-OFF arming rule (the SE-SCANPASS / K1-latemat idiom,
/// factored pure for the tpch-cars lanes): ON iff the value is exactly `1`
/// or `on`; every other spelling — unset, `0`, `off`, typos — fails safe to
/// OFF (today's behaviour, byte-identical plan time).
fn knob_spelling_armed(v: Option<&str>) -> bool {
    matches!(v, Some("1") | Some("on"))
}

/// TPCH-DECOROOT (night/tpch-cars-1, CAR 1 — the R-root blocker,
/// scratchpad/night/tpch-conversion-scope.md §3 car 1): decorated-root
/// composition. Every grouped probe class is Agg-root-only today, which
/// gates 17/20 Gather-carrying TPC-H queries (ORDER BY / LIMIT / OFFSET
/// tops above the grouped agg). The runtime grouped arms produce the FULL
/// grouped output and stream subsequent pulls through the serial emit paths
/// off the filled table (se-aggjoin §3.1), so a serial Sort/Limit ABOVE the
/// engaged arm consumes it correctly — the exprkey Reduced arm
/// (`[Limit<-Sort<-]HashAgg<-SeqScan`), the CbGroupedAggTopN row, and the
/// t35 AGG_BARELIMIT flip already validate the pattern. This knob teaches
/// the probe to see THROUGH whitelisted root decoration (sortClause /
/// limitCount / limitOffset in the parse — the serial planner turns those
/// into Sort/Limit nodes above the Agg), keying the UNDERLYING agg class;
/// only when the child shape keys a covered grouped class and every sort
/// key is a group-key ref or a class-vocabulary aggregate (fail-closed).
/// DEFAULT OFF; `PGRUST_LANE_V2_DECOROOT=1|on` arms (GL-DECOROOT-1 fleet
/// letter owns the default flip).
fn decoroot_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        knob_spelling_armed(std::env::var("PGRUST_LANE_V2_DECOROOT").as_deref().ok())
    })
}

/// TPCH-DECOROOT hash-election margin (PROVISIONAL, GL-DECOROOT-1 owns the
/// measured bound): a decorated root changes the suppressed SERIAL plan's
/// economics — with ORDER BY over group keys the costing compares
/// `HashAgg + Sort(ngroups)` against `Sort(input) + GroupAggregate`, and
/// near ngroups≈input the sorted-agg shape can win, landing a plan the
/// runtime grouped arms refuse (the B1/X5/X6 suppress-then-refuse class,
/// costing flavor). Below this input/ngroups ratio the hash election is
/// safely dominant (HashAgg reads N rows once; the residual sort is over
/// ngroups ≤ N/16 rows); at or above it the decorated shape keeps Gather.
/// Also bounds the serial decoration cost: the Sort above the arm is over
/// at most rows/16 rows.
const DECOROOT_NGROUPS_MARGIN: f64 = 16.0;

/// TPCH-NUMJOIN (night/tpch-cars-1, CAR 2 — the N-join blocker, scoping §3
/// car 3, join half): numeric agg-expr probe vocabulary. The
/// runtime-partial NumericAgg/Int128 state relocation LANDED (SE-AGGPOLY:
/// exact digit snapshots, C numeric_avg_combine field law) and the agg-poly
/// matrix row records "the aggjoin seam's export is ready via the shared
/// runtime-partial vocabulary once its probe admits numeric args" — the
/// blocker for the sum(l_extendedprice*(1-l_discount)) family (13 TPC-H
/// queries carry it) is the probe whitelist, not the kernel. This knob
/// admits structurally plain sum/avg(NUMERIC) aggregates over ONE
/// parallel-safe argument expression (the heap-poly precedent: the join
/// arms run C's checked evaltrans transition program per emitted row, so
/// the arg SHAPE is free; helper-side safety is the planner's own
/// is_parallel_safe) into the JOIN-side classifiers. The grouped-over-SCAN
/// half stays REFUSED — the agg-poly row names its real gap (the lanetable
/// sink combine topology perf car), so GROUPED_SINK_AGGS is untouched.
/// DEFAULT OFF; `PGRUST_LANE_V2_AGGJOIN_NUMERIC=1|on` arms (GL-NUMJOIN-1
/// fleet letter owns the default flip).
fn aggjoin_numeric_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        knob_spelling_armed(std::env::var("PGRUST_LANE_V2_AGGJOIN_NUMERIC").as_deref().ok())
    })
}

/// TPCH-JHEAP (night/tpch-jheap — the scoping's car 2, the J-heap blocker):
/// heap-side join admission. Every join classifier admitted cbstore rels
/// ONLY ('side not cbstore' — the q14/q19 census refusal), while the
/// executor's K2 heap feed (BatchGranuleSource seam) has been DEFAULT ON
/// since the SE9/SE15 flips: the single-join arm and the multibuild
/// build/probe walk both admit heap SeqScans (`k2_heap` in
/// runtime_hashjoin's shape gates + `mb_state_walk`), INNER included in
/// both jointype envelopes — the m5_suppress class-doc claim "heap sides
/// ride the K2 knobs, DEFAULT-OFF" is STALE. This knob admits heap rels
/// into the plain-join / multibuild / grouped-join censuses, fail-closed
/// behind the executor coherence mirror below and the heap-specific guards
/// (`jheap_shape_guards`: stats on heap equi keys — the X6 class,
/// heap-flavored; enable_hashjoin required; unused-index tolerance with
/// the NL-margin law). DEFAULT OFF; `PGRUST_LANE_V2_JHEAP=1|on` arms
/// (GL-JHEAP-1 fleet letter owns the default flip).
fn jheap_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        knob_spelling_armed(std::env::var("PGRUST_LANE_V2_JHEAP").as_deref().ok())
    })
}

/// TPCH-JHEAP executor coherence (the m5p1 `multibuild_enabled` precedent):
/// the K2 heap feed's own kills must also un-key heap shapes — a heap-side
/// suppression whose feed is killed would land on the serial join build
/// (risk P1's suppress-then-refuse direction). Same spellings as the
/// executor (`PGRUST_LANE_V2_K2_PROBE` / `PGRUST_LANE_V2_HEAPFEED`, both
/// default ON, `=0|off` kills — runtime_hashjoin::k2_probe_resolve /
/// batch_source::heapfeed_v2_enabled), own cached reads.
fn k2_heapfeed_live() -> bool {
    static P: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    static H: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let p = *P.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_K2_PROBE").as_deref(), Ok("0") | Ok("off"))
    });
    let h = *H.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_HEAPFEED").as_deref(), Ok("0") | Ok("off"))
    });
    p && h
}

/// TPCH-JHEAP NL/merge-election margin (PROVISIONAL, GL-JHEAP-1 owns the
/// measured bound): an index on a heap rel's JOIN-KEY column makes the
/// post-suppression serial planner's NL-with-inner-index (and index-sorted
/// merge) shapes electable — plans the join walk refuses (the B1/X5/X6
/// suppress-then-refuse class the scoping named). NL(outer=X,
/// inner=IndexScan(Y)) beats hash only when X is comparable to or smaller
/// than Y (per-probe index cost vs the one-pass hash build); requiring
/// EVERY equi-partner of a join-key-indexed heap rel to carry at least
/// this many times its rows keeps the hash election safely dominant.
const JHEAP_NL_MARGIN: f64 = 4.0;

/// PROVISIONAL floor for heap-fed join shapes: the heap fold arms'
/// economics (rows>=1M & dop>=12 — the HeapCmpFoldPrefix/AggPolyHeapPlain
/// reuse; the scoping's "heap fold floor" note), with the hashjoin-nbatch1
/// 2M ceiling kept from the cbstore classes. GL-JHEAP-1 owns re-measuring.
fn jheap_guard() -> FloorGuard {
    FloorGuard {
        min_rows: 1_000_000.0,
        max_rows: 2_000_000.0,
        min_dop: 12,
        low_dop_max_rows: 0.0,
        ..NO_GUARD
    }
}

/// TPCH-CBKEYS (night/tpch-cbkeys): canonical-bytes join-key admission —
/// the grouped-JOIN sink's key vocabulary was word-only (byval int-family)
/// while the SCAN sinks already run canonical-bytes text keys (the C3
/// machinery, agg-text-canonical-bytes row). This knob admits bare
/// text/varchar group keys under the deterministic DEFAULT collation into
/// the grouped-join census (the sink's export/combine/absorb carry the
/// detoasted content bytes — byte equality IS texteq's verdict, the
/// `group_eq_representational` law). BPCHAR is the NAMED REFUSAL of
/// record: its space-stripping bpchareq and trailing-blank representative
/// ties sit outside the byte-equality envelope — exactly why the scan
/// sinks exclude it — so TPC-H's char(n) keys (q04/q05/q07/q08/q12/q21)
/// stay refused until a bpchar tie-law car rules on canonicalization.
/// DEFAULT OFF; `PGRUST_LANE_V2_CBKEYS=1|on` arms (GL-CBKEYS-1 fleet
/// letter owns the default flip).
fn cbkeys_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        knob_spelling_armed(std::env::var("PGRUST_LANE_V2_CBKEYS").as_deref().ok())
    })
}

/// PROVISIONAL floor for bytes-keyed grouped-join shapes: the
/// CbHashJoinGroupedAgg 2M ceiling verbatim — the scan text-key row's
/// min_dop-12 discipline is SUBSUMED here because its low-dop win region
/// (<=3M) covers the whole admitted range (every engaged size <= 2M).
/// GL-CBKEYS-1 owns re-measuring. (The grouped-join row is spill-disabled
/// by construction — the export refuses spill-mode tables — so matrix law
/// 2c, bytes keys disable the word-mode spill arm, holds inherently.)
fn cbkeys_guard() -> FloorGuard {
    FloorGuard { max_rows: 2_000_000.0, ..NO_GUARD }
}

/// SE-MKTEXT group-estimate ceiling, env-overridable
/// (`PGRUST_LANE_V2_MULTIKEY_TEXT_MAX_GROUPS`). The family's whole point is
/// shapes the §10 groupby_high hold (raised to 4e6 at b12c3fc74; the
/// family predates the raise and keeps its own headroom) floors out — the 10M
/// dist-control fixture estimates 3-5M groups for `UserID, SearchPhrase`
/// and the forced runtime arm WINS there (0.061s vs 0.900s legacy-parallel)
/// — so the knob path carries its OWN provisional ceiling instead:
/// default 16M, above the fixture's estimates, below untested 1e7+
/// radix-exchange territory (the groupby-high-1e7 covered-losing row).
/// The runtime backstop is the sink's own cap/budget/spill machinery
/// (canonical shapes spill through the C2 bytes record, canon-sink car 3).
/// GL-MKTEXT-1 owns the measured bound.
fn multikey_text_max_groups() -> f64 {
    static CEIL: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *CEIL.get_or_init(|| {
        std::env::var("PGRUST_LANE_V2_MULTIKEY_TEXT_MAX_GROUPS")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| *v > 0.0)
            .unwrap_or(16_000_000.0)
    })
}

/// PROVISIONAL floor for the SE-MKTEXT knob path: the CbGroupedAggTextKey
/// economics verbatim (min_dop 12, low-dop win region ≤3M — the same
/// text-keyed grouped engagement, one more key word/tail). GL-MKTEXT-1
/// owns re-measuring.
fn multikey_text_guard() -> FloorGuard {
    FloorGuard { min_dop: 12, low_dop_max_rows: 3_000_000.0, ..NO_GUARD }
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
// WS-COVER (phase3-close §3.2): min/max(date) aggregate OIDs. The scan-fold
// arm's classify_trans admits F_DATE_LARGER(1138)/F_DATE_SMALLER(1139) at the
// I32 lane width (lanefold::classify_trans) — date is int4-width byval, so the
// fold kernel and the CbPlainAggFold engagement floor are byte-identical to
// int4 min/max. Keyed apart from PLAIN_FOLD_AGGS because their arg type is
// DATE, not int-family (see is_plain_fold_agg).
const F_MAX_DATE: u32 = 2122;
const F_MIN_DATE: u32 = 2138;

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

// SE-AGGPOLY (band 101001): sum/avg over NUMERIC — aggregate OIDs of record
// (vendored REL 18.3 pg_proc/pg_aggregate, verified): both ride transfn
// numeric_avg_accum (2858, NOT strict) over an INTERNAL NumericAggState
// without sum_x2. The stddev/variance family (numeric_accum 1834, sum_x2)
// stays a named refusal.
const F_AVG_NUMERIC: u32 = 2103;
const F_SUM_NUMERIC: u32 = 2114;
// avg(int2)/avg(int4) — the runtime distinct sink's AvgInt vocab entries
// (pardistinct::vocab_kind); admitted as CbDistinctIntKeys passengers under
// the AGG_POLY knob below.
// (F_AVG_INT2/F_AVG_INT4 already defined above with the fold whitelist.)

/// SE-AGGPOLY knob coherence (the GROUPSINK precedent): the executor arm's
/// `PGRUST_LANE_V2_AGG_POLY` (execmain lanev2) must also gate the probe
/// keyings this lane adds — a keyed shape whose arm is disarmed would
/// suppress Gather and land on the serial path (risk P1's
/// suppress-then-refuse direction). Same env spelling in both crates, and
/// BOTH read sites flip together (the letter's knob-coherence duty).
///
/// DEFAULT ON (t35 routing-flips, GL letter 2026-07-21 FLIP-RECOMMENDED,
/// jobs -558e/-135a/-3773 @ 67a99589d, unforced 10M ClickBench): official
/// score 0.9278 (−7.2%; inert-arm noise floor 0.9889) — essentially all of
/// it CB q10 1.861 -> 0.066 hot (28.2x, == the forced rt16 ref 0.067,
/// confirming GL-AGGPOLY-2's avg-passenger claim unforced); 42/42 remaining
/// queries in the noise band, byte-parity class set unchanged; composes
/// with GL-AGGPOLY-1's SE16 −12.4% q06 heap-shape WIN. Probe cost is
/// plan-time only (the §6 OLTP same-pod Ir pair rides this train).
/// `PGRUST_LANE_V2_AGG_POLY=0|off` is the kill switch.
fn agg_poly_probe_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_LANE_V2_AGG_POLY").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// CbDistinctIntKeys PASSENGER whitelist = the runtime distinct sink's
/// EXACT vocabulary (`pardistinct::vocab_kind`: count(*)/count(any)/
/// sum(int2/int4), plus avg(int2/int4) — the (acc,count) transarray pair —
/// keyed only under the AGG_POLY knob until its fleet letter lands).
/// HISTORY (se-aggpoly fix): this branch previously consulted
/// GROUPED_SINK_AGGS, which also lists min/max(int2/4/8) — aggregates the
/// distinct sink's spec derivation REFUSES ("vocab transfn outside the
/// exact-integer whitelist", nodeagg lib.rs pd_derive), so a
/// count(DISTINCT)+min/max shape keyed, suppressed its Gather, and landed
/// on the serial arm — the latent suppress-then-refuse channel. The
/// min/max removal is UNCONDITIONAL (fail-closed regardless of the knob);
/// the e2e pins the shape NOT-KEYED.
const DISTINCT_PASSENGER_AGGS: &[u32] = &[F_COUNT_STAR, F_COUNT_ANY, F_SUM_INT4, F_SUM_INT2];
const DISTINCT_PASSENGER_AGGS_POLY: &[u32] =
    &[F_COUNT_STAR, F_COUNT_ANY, F_SUM_INT4, F_SUM_INT2, F_AVG_INT4, F_AVG_INT2];

fn distinct_passenger_aggs() -> &'static [u32] {
    if agg_poly_probe_enabled() { DISTINCT_PASSENGER_AGGS_POLY } else { DISTINCT_PASSENGER_AGGS }
}

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
const DATEOID: u32 = 1082;
const TEXTOID: u32 = 25;
/// TPCH-CBKEYS: bpchar — recognized ONLY to NAME its refusal (the
/// space-insensitive-equality exclusion; never admitted as a key).
const BPCHAROID: u32 = 1042;
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
/// 2.23× — measured at est_groups≈1e7, the matrix row's workload).
/// Env-overridable for calibration sweeps, and the override doubles as the
/// kill switch: `PGRUST_M5_GROUPBY_HIGH_FLOOR=1000000` restores the
/// pre-2026-07-21 boundary.
///
/// Default RAISED 1e6 → 4e6 (night/routing-floor-fixes, fleet letter in the
/// branch commit): the original 1e6 was derived from the OLD groupby-high
/// fixture at est_groups≈1e7 and silently held the whole 1e6..1e7 band
/// legacy. The forced-mt16 routing-gap harvest + the floor=4e6 env A/B
/// (unforced ClickBench 10M, cbstore9-v8-sorted-v2, c8gd NVMe) showed the
/// CURRENT runtime agg arm crushes the 1e6..3e6 band (cb q16 0.864s→~0.02s,
/// q17 0.900s→~0.06s, q34/q35 2.3s→~0.24s) while est_groups≈1e7 (cb q33
/// class) still loses in the runtime combine until the exchange program
/// lands — so the boundary moves to 4e6 (above the measured-winning ≈3e6
/// band, below the known-losing 1e7), NOT to unbounded.
fn groupby_high_floor() -> f64 {
    static FLOOR: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *FLOOR.get_or_init(|| {
        std::env::var("PGRUST_M5_GROUPBY_HIGH_FLOOR")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| *v > 0.0)
            .unwrap_or(4_000_000.0)
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

    // FROM shapes the probe keys (everything else classifies uncovered by
    // construction — notably nested join trees, the m5p1 multi-build-side
    // SQL admission gap):
    //   * ONE plain relation (the single-rel classes);
    //   * ONE explicit JoinExpr (row flip 2, CbHashJoinPlainAgg — the
    //     outer-join families survive to the planner in this form);
    //   * TWO RangeTblRefs (row flip 2, flat form): the INNER-join shape as
    //     the planner sees it — `a JOIN b ON q` and `a, b WHERE q` are the
    //     same FromExpr by probe time, with the equi quals in top.quals.
    let Some(top) = parse.jointree else { return Ok(false) };
    if top.fromlist.len() == 2 {
        let (Some(ra), Some(rb)) = (
            top.fromlist.nth(0).as_range_tbl_ref(),
            top.fromlist.nth(1).as_range_tbl_ref(),
        ) else {
            return Ok(false);
        };
        // SE-AGGJOIN (band 87001): grouped 2-rel flat INNER forms key the
        // grouped-sink row (the explicit outer-family JoinExpr forms stay
        // unkeyed — side-swapped RIGHT plans sit outside the walk's
        // probe-local envelope).
        if parse.hasAggs && !parse.groupClause.is_nil() {
            let rtis = [ra.rtindex as usize, rb.rtindex as usize];
            let mut quals = Vec::new();
            push_and_terms(top.quals, &mut quals);
            return classify_aggjoin_grouped(run, parse, &rtis, &quals);
        }
        return classify_join_sides(
            run,
            parse,
            ra.rtindex as usize,
            rb.rtindex as usize,
            top.quals,
        );
    }
    // m5p1 (band 88001): the flat N-relation INNER form (`a, b, c WHERE q`
    // == `a JOIN b JOIN c` by probe time — quals in top.quals). 3..=6 rels;
    // the 2-rel form stays the CbHashJoinPlainAgg branch above.
    if (3..=6).contains(&top.fromlist.len()) {
        let mut rtis = Vec::with_capacity(top.fromlist.len());
        for f in &top.fromlist {
            let Some(rtr) = f.as_range_tbl_ref() else { return Ok(false) };
            rtis.push(rtr.rtindex as usize);
        }
        let mut quals = Vec::new();
        push_and_terms(top.quals, &mut quals);
        return classify_multibuild(run, parse, &rtis, &quals);
    }
    if top.fromlist.len() != 1 {
        return Ok(false);
    }
    if let Some(je) = top.fromlist.nth(0).as_join_expr() {
        // m5p1: a nested left-deep INNER chain (`a JOIN b ON .. JOIN c ON ..`)
        // keys CbHashJoinMultiBuild; every other nested tree stays uncovered
        // by construction (classify_join_covered's refusal).
        if je.larg.as_join_expr().is_some() {
            let mut rtis = Vec::new();
            let mut quals = Vec::new();
            if !collect_inner_chain(je, &mut rtis, &mut quals) {
                return refuse_join("nested join tree (not a left-deep INNER chain)");
            }
            if !(3..=6).contains(&rtis.len()) {
                return refuse_join("multibuild chain size");
            }
            push_and_terms(top.quals, &mut quals);
            return classify_multibuild(run, parse, &rtis, &quals);
        }
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
    let is_cb = run.root.rel(rel_id).amflags & AMFLAG_PGRCOLUMNAR != 0;
    let rel_rows = run.root.rel(rel_id).rows.max(0.0);
    let rel_pages = f64::from(run.root.rel(rel_id).pages);
    let has_quals = top.quals.is_some();

    // --- plain SELECT DISTINCT: UNKEYED (m5-integration-r2 re-key) ---------
    // The runtime distinct sink admits the SORTED-distinct feed (grouped
    // count(DISTINCT), below); the plain shape plans HashAggregate, which
    // the sink refuses — suppressing it was a measured serial-instead-of-
    // legacy false positive (rowflip measure, 2.66x at dop4). Keep Gather.
    if !parse.distinctClause.is_nil() {
        // SE-T2AGG CAR A (knob-gated, default OFF — block doc below): the
        // plain single-column shape keys the runtime plain-distinct sink's
        // SELECT-DISTINCT sub-arm; every miss keeps the refusal verbatim.
        if let Some(verdict) = classify_distinct_plain(
            run, parse, rti, rte.relid, rel_id, is_cb, has_quals, rel_rows, rel_pages,
        )? {
            return Ok(verdict);
        }
        return Ok(false);
    }

    // --- Aggregate shapes ----------------------------------------------------
    if !parse.hasAggs {
        // Bounded top-N over pgrcolumnar (row flip 1, CbTopnBoundedIntKeys):
        // ORDER BY int-family Var keys + LIMIT, no OFFSET (WITH TIES is
        // prefiltered above), every tlist entry a plain Var on the rel
        // (the sort arm's emit face; junk sort-key entries are Vars too).
        // Full sort (no LIMIT) stays the uncovered fullsort-shape-b row;
        // heap rels stay uncovered (the arm is pgrcolumnar-fusible only).
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
            return finish(run, CoverClass::CbTopnBoundedIntKeys, rte.relid, 0.0, rel_rows, rel_pages);
        }
        // SE-SCANPASS (band 72001, se/scan-passthrough): the row-returning
        // passthrough shape (bare filtered SELECT, no agg / group / top-N /
        // DISTINCT) keeps its Gather — no `parallel_engine=runtime` arm
        // emits rows (they all fold). Behind PGRUST_LANE_V2_SCANPASS
        // (default OFF) this NAMES the refusal (§3.3 "no class routed by
        // accident") instead of the silent generic fall-through, and is the
        // seam a future row-emit arm engages from. INERT at default: OFF
        // takes the identical `Ok(false)` below (byte-identical plan-time).
        if scanpass_enabled() {
            return classify_scanpass(parse, rti, is_cb, has_quals);
        }
        return Ok(false);
    }

    if parse.groupClause.is_nil() {
        // Plain aggregation, one output row.
        if is_cb {
            if tlist_all_plain_fold_aggs(parse, rti) {
                // Qualed COUNT-ONLY census (q2box lane, 2026-07-15): the
                // transition program reads no scan column, so the runtime
                // scan arm never takes it (no fold plan; the serial lane's
                // per-row PREWHERE drive owns it) and the footer META
                // answer serves only the zero-count qual shape on parts
                // whose EVERY RG carries v7 zerocnts. Suppressing the
                // Gather without that answerability is a measured 5x
                // serial-instead-of-legacy false positive (q2 on the v6
                // 100M bank: Gather-16 0.011s -> suppressed-serial 0.055s;
                // notes/q2box-lane.md). Probe subset-of walk: keep the
                // legacy Gather when the META answer provably cannot
                // engage. Column-reading agg sets (count(v)/sum/min/max)
                // keep the keying — the fold walk owns them, quals and
                // all, through the kernel-qual PREWHERE feed.
                if has_quals
                    && tlist_all_count_star(parse)
                    && run.root.rel(rel_id).amflags & AMFLAG_PGRCOLUMNAR_ZEROCNT == 0
                {
                    return Ok(false);
                }
                return finish(run, CoverClass::CbPlainAggFold, rte.relid, 1.0, rel_rows, rel_pages);
            }
            // Meta-over-Gather (M5-5, the band-2a q30 handoff): the residual
            // plain-agg shapes the Meta footer arm answers — affine int
            // sum/avg args (`sum(v+k)` batteries) the bare-Var whitelist
            // above does not key. No quals (footer answers are whole-table;
            // the zero-count qual sub-arm stays unkeyed — narrower probe).
            if !has_quals
                && parse.sortClause.is_nil()
                && tlist_all_meta_footer_aggs(parse, rti)
            {
                return finish(run, CoverClass::CbMetaFooterAgg, rte.relid, 1.0, rel_rows, rel_pages);
            }
            // SE-TEXTDISTINCT (C1, band 86001): ungrouped count(DISTINCT
            // <int|default-collation text Var>) — the census's "plain q5/q6
            // shape unwired" gap (cb q5 count(DISTINCT UserID), q6
            // count(DISTINCT SearchPhrase)). The runtime PLAIN-distinct SINK
            // (runtime_plaindistinct.rs) admits int AND canonical-bytes text
            // distinct VALUES; suppressing Gather yields the serial
            // `Aggregate(AGG_PLAIN) <- Sort <- SeqScan(cbstore)` shape its
            // skip-sort dispatch owns. Gated on the SEPARATE plain sub-knob;
            // NARROW: no quals, no sort/limit, EXACTLY the single
            // count(DISTINCT) tlist entry (the sink stages the distinct arg
            // as scan column 0 — a WHERE or extra projected column could move
            // it off col 0 and the arm would land on serial). The sink arms
            // via router::arm_dop(Distinct) (98a012ba2). NOT a
            // BOOTSTRAP_MATRIX class. HISTORY: night/planner-fix-forced held
            // the plain sub-knob OFF off a measured 10M REGRESSION (q5
            // 0.046->0.151, q6 0.081->0.175) — later diagnosed as the
            // suppress-then-UNARMED hole (the sink armed off the bench GUC
            // alone, so the suppressed plan ran serial with no pool). With
            // the arm_dop fix landed, GL-TEXTDIST-2 re-measured GREEN (q5/q6
            // at forced parity ~0.020/0.045s, job 7e66) and t35
            // routing-flips flipped the sub-knob DEFAULT ON
            // (PGRUST_LANE_V2_TEXTDISTINCT_PLAIN=0|off is the kill).
            if textdistinct_plain_enabled()
                && !has_quals
                && parse.sortClause.is_nil()
                && parse.limitCount.is_none()
                && parse.targetList.len() == 1
            {
                if let Some(tle) = parse.targetList.nth(0).as_target_entry() {
                    if is_count_distinct_any(tle.expr, rti) {
                        return finish_textdistinct(
                            run,
                            "plain-count-distinct",
                            textdistinct_guard(),
                            rte.relid,
                            1.0,
                            rel_rows,
                            rel_pages,
                        );
                    }
                }
            }
            return Ok(false);
        }
        // SE-AGGPOLY (band 101001, knob-gated): plain heap aggregation with
        // sum/avg(numeric) states, quals ALLOWED (the per-row drive runs
        // them verbatim; helper-side safety = the planner's own
        // is_parallel_safe over quals + numeric agg args). Unindexed keeps
        // the suppressed serial plan an Agg-over-SeqScan; no sort/limit
        // keeps the Agg the plan root (both are walk refusals — the
        // suppress-then-refuse direction). tpch q06 class.
        if agg_poly_probe_enabled()
            && parse.sortClause.is_nil()
            && parse.limitCount.is_none()
            && parse.limitOffset.is_none()
            && heap_poly_indexes_admit(run, parse, top.quals, rti, rel_id)?
            && crate::is_parallel_safe_opt(run, top.quals)?
            && heap_poly_tlist_admits(run, parse, rti)?
        {
            // Floor denominator: the RAW tuple estimate, not the post-qual
            // rows — the per-row drive scans the WHOLE relation and runs the
            // qual per row, so the engagement's work (and the parallel win)
            // is scan-shaped. Using rel_rows here floored a 1.5M-row scan
            // out at 23% selectivity (live finding, worklog §3).
            let scan_tuples = run.root.rel(rel_id).tuples.max(rel_rows);
            return finish(run, CoverClass::AggPolyHeapPlain, rte.relid, 1.0, scan_tuples, rel_pages);
        }
        // Heap rows are no-qual only (LIKE-qual folds are walk refusals;
        // the qualed LIKE census is deliberately not keyed in bootstrap).
        if has_quals || !parse.sortClause.is_nil() {
            return Ok(false);
        }
        if is_bare_count_star(parse) {
            return finish(run, CoverClass::HeapPlainCountStar, rte.relid, 1.0, rel_rows, rel_pages);
        }
        if tlist_all_whitelisted_aggs(parse, rti, HEAP_CMP_AGGS) {
            return finish(run, CoverClass::HeapCmpFoldPrefix, rte.relid, 1.0, rel_rows, rel_pages);
        }
        return Ok(false);
    }

    // --- Grouped aggregation over pgrcolumnar ------------------------------------
    if !is_cb {
        return Ok(false);
    }
    // SE-TEXTDISTINCT (C1, band 86001): q36 reduced-expr-key grouped agg —
    // keyed only knob-ON and BEFORE the bare-Var key discipline (which
    // refuses expr keys). A shape MISS returns None and falls through
    // unchanged.
    if textdistinct_enabled() {
        if let Some(verdict) =
            classify_reduced_exprkey(run, parse, rti, rte.relid, rel_id, rel_rows, rel_pages)?
        {
            return Ok(verdict);
        }
    }
    // SE-EXTRACTKEY (cb q19 class): extract()-keyed grouped agg — keyed
    // only knob-ON and BEFORE the bare-Var key discipline (which refuses
    // expr keys). A shape MISS returns None and falls through unchanged.
    if extract_exprkey_enabled() {
        if let Some(verdict) =
            classify_extract_exprkey(run, parse, rti, rte.relid, rel_id, rel_rows, rel_pages)?
        {
            return Ok(verdict);
        }
    }
    // Key discipline: all keys plain Vars on the scanned rel; int-family
    // plus at most one text/varchar key under the deterministic default
    // collation (the c3 canonical-key-bytes classes). SE-CONSTKEY: non-null
    // int-family Const keys admitted knob-ON (the q35 `GROUP BY 1, URL`
    // census) — they contribute nothing to the partition, so the REAL keys
    // keep driving classification and floors.
    let mut n_text = 0usize;
    let mut n_const = 0usize;
    let mut key_refs: Vec<u32> = Vec::new();
    let mut const_key_refs: Vec<u32> = Vec::new();
    // SE-T2AGG CAR B: the key Vars' attnos (the stale-cell refusal input —
    // a min/max(text) over a GROUP KEY column keeps the refusal).
    let mut key_attnos: Vec<i16> = Vec::new();
    for gc_node in &parse.groupClause {
        let Some(gc) = gc_node.as_sort_group_clause() else { return Ok(false) };
        let Some(tle) = tle_by_sortgroupref(parse, gc.tleSortGroupRef) else {
            return Ok(false);
        };
        if agg_constkey_enabled() && is_admissible_const_key(tle.expr) {
            n_const += 1;
            const_key_refs.push(gc.tleSortGroupRef);
            key_refs.push(gc.tleSortGroupRef);
            continue;
        }
        let Some(v) = key_var(tle.expr, rti) else { return Ok(false) };
        key_attnos.push(v.varattno);
        if is_int_family(v.vartype) {
            // covered
        } else if is_text_family(v.vartype) && v.varcollid == DEFAULT_COLLATION_OID {
            n_text += 1;
            // SE-MKTEXT: a SECOND text key is keyable ONLY as the exact
            // two-key text+text shape under PGRUST_LANE_V2_MULTIKEY_TEXT
            // (the widened scan feed's envelope — two Intern components,
            // dict/raw-stageable). Anything wider (3+ keys carrying two
            // texts) stays uncovered — fail-closed, probe ⊂ walk. Knob OFF
            // takes the identical refusal as before.
            if n_text > 1
                && !(multikey_text_enabled()
                    && mk_text_family_shape_ok(parse.groupClause.len(), n_text))
            {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }
        key_refs.push(gc.tleSortGroupRef);
    }
    // SE-MKTEXT: the two-key-with-text family (int+text / text+text, bare
    // default-collation Vars — the knob-widened envelope), with the engine
    // text-car kills mirrored (suppress-then-refuse guard). DEFAULT ON
    // (t35 routing-flips); with the kill thrown this is false — every
    // branch it gates below then takes the pre-flip path byte-for-byte.
    let mk_text_family = multikey_text_enabled()
        && n_const == 0
        && mk_text_family_shape_ok(parse.groupClause.len(), n_text)
        && mk_text_agg_cars_live(n_text);
    // Emit discipline: every tlist entry is a bare group-key Var, a
    // whitelisted sink aggregate (const tlist entries — the q35 refusal,
    // now keyed under SE-CONSTKEY — and non-identity emits classify
    // uncovered here), or a
    // count(DISTINCT <int Var>) — the runtime distinct sink's class
    // (CbDistinctIntKeys; int GROUP keys only, checked below).
    let mut n_count_distinct = 0usize;
    let mut passengers: Vec<Node<'_>> = Vec::new();
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
        if tle.ressortgroupref != 0 && const_key_refs.contains(&tle.ressortgroupref) {
            // SE-CONSTKEY: the const key's own tlist entry (same shape law).
            if !is_admissible_const_key(tle.expr) {
                return Ok(false);
            }
            continue;
        }
        if tle.ressortgroupref != 0 && key_refs.contains(&tle.ressortgroupref) {
            if key_var(tle.expr, rti).is_none() {
                return Ok(false);
            }
            continue;
        }
        if is_count_distinct_int(tle.expr, rti) {
            n_count_distinct += 1;
            continue;
        }
        // Deferred: the passenger discipline depends on the CLASS (the
        // distinct sink's vocabulary vs the grouped sink's whitelist),
        // known only once the whole tlist was scanned.
        passengers.push(tle.expr);
    }
    // Text group key + grouped count(DISTINCT): the runtime distinct SINK
    // ADMITS canonical-bytes text group keys under the deterministic default
    // collation (pd_derive_spec(agg, desc, /*admit_text_keys=*/true) in
    // runtime_distinct.rs try_own_sorted_distinct_runtime) — so this is an
    // ADMISSION gap, NOT a missing kernel (SE-TEXTDISTINCT C1, band 86001;
    // cb q11/q12/q14, the census distinct-text-date-args mass). Knob-gated
    // (PGRUST_LANE_V2_TEXTDISTINCT, DEFAULT ON since t34; =0|off kills):
    // killed keeps Gather (the pre-flip posture); ON falls through to the
    // class selection, where the n_count_distinct && n_text branch routes
    // it through finish_textdistinct. The count(DISTINCT) arg stays
    // int-family (is_count_distinct_int — the SINK's exact-set vocabulary);
    // the TEXT is the GROUP key. GL-TEXTDIST letter (2026-07-21): the
    // grouped arm earns at default — q11/q12 0.010/0.011 hot vs cpg 0.44.
    // SE-MKTEXT fail-closed: grouped count(DISTINCT) rides the runtime
    // DISTINCT sink, whose canonical text-key admission is proven for ONE
    // text group key (pd_derive admit_text_keys) — the two-text distinct
    // feed is unproven, refuse outright (reachable only knob-ON; the
    // default census refused n_text > 1 in the key loop).
    if n_count_distinct > 0 && n_text > 1 {
        return Ok(false);
    }
    // SE-CONSTKEY fail-closed: const keys through the runtime DISTINCT
    // sink's key derivation are untested — refuse the mix.
    if n_count_distinct > 0 && n_const > 0 {
        return Ok(false);
    }
    if n_count_distinct > 0 && n_text > 0 && !textdistinct_enabled() {
        return Ok(false);
    }
    // Passenger discipline per class (se-aggpoly): the DISTINCT class
    // consults the distinct sink's exact vocabulary (min/max REMOVED — the
    // latent suppress-then-refuse channel; avg(int2/4) ADDED under the
    // AGG_POLY knob); everything else keeps GROUPED_SINK_AGGS verbatim.
    let passenger_list =
        if n_count_distinct > 0 { distinct_passenger_aggs() } else { GROUPED_SINK_AGGS };
    let mut n_strminmax = 0usize;
    for e in &passengers {
        if is_whitelisted_agg(*e, rti, passenger_list) {
            continue;
        }
        // SE-T2AGG CAR B (knob-gated, default OFF — block doc below):
        // min/max(text) passengers over default-collation bare Vars, the
        // grouped sink's new VarlenaMinMax vocabulary. Fail-closed
        // exclusions: NO QUALS (fleet containment, GL letter
        // fleet-ab-parallelism.md: the qualed target shape suppressed but the arm
        // never engaged on the real 10M bank — a data-dependent staging
        // refusal the probe cannot see, so the qualed shape landed
        // suppress-then-SERIAL at 7.6-8.5x; the local 1M fixture engages,
        // proving the refusal is bank-dependent — refuse outright until
        // the qualed-topn-through-the-runtime-sort-arm follow-up earns a
        // re-letter), SINGLE-key shapes only (the sink hosts the K2
        // single-int and C2 single-text drains; the packed multi-key feed
        // refuses vguard plans), never beside count(DISTINCT) (the distinct
        // sink's vocab stays exact — the se-aggpoly suppress-then-refuse
        // lesson), and never inside the SE-MKTEXT two-key-text family (the
        // mk finish above would key a combination the text cars never
        // proved).
        if n_count_distinct == 0
            && !has_quals
            && !mk_text_family
            && parse.groupClause.len() == 1
            && agg_strminmax_enabled()
        {
            if let Some(arg) = grouped_str_minmax_arg(*e, rti) {
                if !key_attnos.contains(&arg) {
                    n_strminmax += 1;
                    continue;
                }
            }
        }
        return Ok(false);
    }

    // Sort/limit composition: none at all (plain grouped emit), or the
    // top-N winner-selection shape — a single whitelisted-aggregate sort
    // key plus LIMIT without OFFSET (q17/q18/q31–33). A sort on the group
    // keys themselves is an ordered-stream consumer (GatherMerge class,
    // uncovered in bootstrap). TPCH-DECOROOT (CAR 1, knob-gated): the
    // residual decorated-root shapes — ORDER BY over group keys and/or
    // class-vocabulary aggregates, multi-key sorts, sorts without LIMIT,
    // and LIMIT+OFFSET forms — key the UNDERLYING grouped class; the arm
    // emits the full grouped output and the serial Sort/Limit above
    // consumes it (the exprkey-Reduced / CbGroupedAggTopN / AGG_BARELIMIT
    // precedent). Fail-closed: no count(DISTINCT) (distinct-sink
    // decoration owns its own topn composition only), no const/mk-family
    // keys (their knob paths keep their own proven compositions), at most
    // one text key, enable_hashagg required ON (with it off the suppressed
    // serial plan is a sorted-agg shape the walk refuses).
    let mut mk_freeze = false;
    let mut bare_limit = false;
    let mut full_sort = false;
    let mut decorated = false;
    let topn = if parse.sortClause.is_nil() && parse.limitCount.is_none() {
        false
    } else if parse.sortClause.is_nil()
        && parse.limitCount.is_some()
        && parse.limitOffset.is_none()
        && mk_text_family
        && n_count_distinct == 0
        && agg_freeze_car_live()
    {
        // SE-MKTEXT: bare `LIMIT k` with NO ORDER BY (the cb q18 class,
        // `GROUP BY UserID, SearchPhrase LIMIT 10`) — the runtime agg
        // sink's group-admission FREEZE composition (band-2a): the
        // suppressed serial plan is `Limit <- HashAgg <- SeqScan`, the sink
        // freezes admission at the bound and the serial Limit consumes the
        // drain (any-k-groups is a correct answer for an unordered LIMIT).
        // Knob-ON family shapes only; every other bare-LIMIT grouped shape
        // keeps the refusal below byte-for-byte.
        mk_freeze = true;
        false
    } else if parse.sortClause.is_nil()
        && parse.limitCount.is_some()
        && parse.limitOffset.is_none()
        && n_count_distinct == 0
        && agg_barelimit_enabled()
        && agg_freeze_car_live()
    {
        // SE-BARELIMIT: the GENERAL bare-LIMIT composition (its own knob,
        // the mk-text family branch above being the more-specific sibling):
        // the same freeze-owned `Limit <- HashAgg <- SeqScan` suppression
        // for the shapes the census otherwise covers. The groupby_high hold
        // below still applies (the floor recalibration lane owns raising
        // it), so this admits the COMPOSITION only.
        bare_limit = true;
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
        // The sort key rides the same class-dependent vocabulary as the
        // passengers (se-aggpoly): a distinct-class sort key outside the
        // sink vocab would key a shape the sink refuses.
        if !is_whitelisted_agg(tle.expr, rti, passenger_list)
            && !is_count_distinct_int(tle.expr, rti)
        {
            // TPCH-DECOROOT: the single-sort-key+LIMIT shape whose key is a
            // GROUP key (not an agg) is a decorated-root form too.
            if decoroot_enabled()
                && n_count_distinct == 0
                && n_const == 0
                && n_text <= 1
                && !mk_text_family
                && crate::gucs::enable_hashagg()
                && scan_sort_keys_covered(parse, &key_refs, rti, passenger_list)
            {
                decorated = true;
                false
            } else {
                return Ok(false);
            }
        } else {
            true
        }
    } else if parse.sortClause.len() == 1
        && parse.limitCount.is_none()
        && parse.limitOffset.is_none()
        && agg_sort_nolimit_enabled()
    {
        // SE-T2AGG CAR C (knob-gated, default OFF — block doc below): the
        // topn shape WITHOUT the bound (cb q8 class, ORDER BY count(*) no
        // LIMIT). Same single-agg sort-key vocabulary law as the topn arm;
        // the suppressed serial plan keeps its REAL Sort above the Agg (the
        // unbounded sink_topn_arm declines into the plain full drain and
        // the Sort consumes it), so this admits the COMPOSITION only.
        // (The TPCH-DECOROOT residual arm below owns this shape only when
        // this proven arm's knob is killed — same full-drain semantics.)
        let Some(sc) = parse.sortClause.nth(0).as_sort_group_clause() else {
            return Ok(false);
        };
        let Some(tle) = tle_by_sortgroupref(parse, sc.tleSortGroupRef) else {
            return Ok(false);
        };
        if !is_whitelisted_agg(tle.expr, rti, passenger_list)
            && !is_count_distinct_int(tle.expr, rti)
        {
            return Ok(false);
        }
        full_sort = true;
        false
    } else if decoroot_enabled()
        && !parse.sortClause.is_nil()
        && n_count_distinct == 0
        && n_const == 0
        && n_text <= 1
        && !mk_text_family
        && crate::gucs::enable_hashagg()
        && scan_sort_keys_covered(parse, &key_refs, rti, passenger_list)
    {
        // TPCH-DECOROOT (CAR 1): the residual whitelisted decorations —
        // multi-key sorts, group-key sorts, sorts without LIMIT, and
        // LIMIT+OFFSET above a sort. Bare LIMIT/OFFSET with NO sort stays
        // refused here (the SE-BARELIMIT / freeze rows own the no-sort
        // LIMIT composition; OFFSET without ORDER BY has no covered arm).
        decorated = true;
        false
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
    // SE-MKTEXT: the family's whole population sits ABOVE the groupby_high
    // hold at ClickBench scale (10M dist-control estimates 3-5M groups for
    // `UserID, SearchPhrase` — the forced runtime arm wins 8-15x there), so
    // the knob path carries its OWN provisional ceiling instead of the §10
    // hold; everything else keeps the hold byte-for-byte.
    let over_groupby_high = ngroups >= groupby_high_floor();
    if over_groupby_high
        && !(mk_text_family
            && n_count_distinct == 0
            && ngroups < multikey_text_max_groups())
    {
        return Ok(false);
    }

    // SE-T2AGG knob-path finishes (BEFORE the sibling knob finishes: shapes
    // only these knobs admit must route through their own trace tags — the
    // textdistinct/mktext lanes keep their proven admission domains).
    if n_strminmax > 0 {
        // Fail-closed: min/max(text) passengers ride the plain grouped /
        // topn compositions only (the freeze, bare-LIMIT, const-key,
        // no-limit-sort, and TPCH-DECOROOT decorated combinations are
        // unproven with byref text states; count(DISTINCT) +
        // mk-text-family were excluded at admission).
        if full_sort || decorated || bare_limit || mk_freeze || n_const > 0 {
            return Ok(false);
        }
        let class = if topn {
            CoverClass::CbGroupedAggTopN
        } else if n_text > 0 {
            CoverClass::CbGroupedAggTextKey
        } else {
            CoverClass::CbGroupedAggIntKeys
        };
        return finish_knob_path(
            run,
            "strminmax",
            if topn { "strminmax-grouped-topn" } else { "strminmax-grouped-agg" },
            class_guard(class),
            rte.relid,
            ngroups,
            rel_rows,
            rel_pages,
        );
    }
    if full_sort {
        // Fail-closed: the const-key emit and the mk-text ceiling path stay
        // outside the no-limit sort composition (unproven combinations; the
        // groupby_high hold above already bounds the serial Sort's input).
        if n_const > 0 || mk_text_family {
            return Ok(false);
        }
        let class = if n_count_distinct > 0 {
            CoverClass::CbDistinctIntKeys
        } else if n_text > 0 {
            CoverClass::CbGroupedAggTextKey
        } else {
            CoverClass::CbGroupedAggIntKeys
        };
        return finish_knob_path(
            run,
            "aggsortnl",
            if n_count_distinct > 0 { "sortnl-grouped-distinct" } else { "sortnl-grouped-agg" },
            class_guard(class),
            rte.relid,
            ngroups,
            rel_rows,
            rel_pages,
        );
    }
    // SE-TEXTDISTINCT (band 86001): text-keyed grouped count(DISTINCT) is
    // reachable here ONLY knob-ON (the n_count_distinct && n_text gate above
    // returns Ok(false) at defaults). It rides the SAME runtime distinct
    // sink as the int-key class, with text group keys admitted (module doc);
    // route it through the dedicated knob-path finish (own trace + provisional
    // floor; NOT a BOOTSTRAP_MATRIX class, so the default census + drift
    // guards are untouched). The top-N composition (cb q11/q12/q14 are all
    // ORDER BY count DESC LIMIT) rides the sink's paremit selection — the
    // walk composes it (named-kernels-distinct Kernel 2), so no extra probe
    // condition is needed beyond the topn shape already validated above.
    if n_count_distinct > 0 && n_text > 0 {
        return finish_textdistinct(
            run,
            "text-grouped-count-distinct",
            textdistinct_guard(),
            rte.relid,
            ngroups,
            rel_rows,
            rel_pages,
        );
    }
    // SE-MKTEXT knob-path finish: shapes admitted ONLY by the knob — a
    // second text key, the bare-LIMIT freeze composition, or a group
    // estimate past the groupby_high hold under the family ceiling — route
    // through the dedicated finish (own trace prefix + provisional floor;
    // NOT a BOOTSTRAP_MATRIX class, so the tsv/route_to columns, the drift
    // guards, and the DEFAULT census are untouched). Family shapes the
    // DEFAULT probe already covers (int+text under the groupby_high hold)
    // fall through to their bootstrap classes unchanged — knob-ON only
    // ADDS suppressions, never re-classes an existing one.
    if mk_text_family && n_count_distinct == 0 && (n_text > 1 || mk_freeze || over_groupby_high) {
        return finish_multikey_text(
            run,
            if mk_freeze { "twokey-text-freeze" } else { "twokey-text-grouped-agg" },
            multikey_text_guard(),
            rte.relid,
            ngroups,
            rel_rows,
            rel_pages,
        );
    }
    let class = if n_count_distinct > 0 {
        // The sorted-distinct feed owns grouped count(DISTINCT) — with or
        // without the top-N composition (walk-admitted, e2e leg 177 class).
        CoverClass::CbDistinctIntKeys
    } else if topn {
        CoverClass::CbGroupedAggTopN
    } else if n_text > 0 {
        CoverClass::CbGroupedAggTextKey
    } else {
        CoverClass::CbGroupedAggIntKeys
    };
    // TPCH-DECOROOT (CAR 1) knob-path finish: decorated-root shapes route
    // through the dedicated finish (own trace tag; NOT a BOOTSTRAP_MATRIX
    // class — tsv/route_to, drift guards, and the DEFAULT census untouched),
    // carrying the UNDERLYING class's floor economics. The hash-election
    // margin guards the sorted-agg serial landing (with ORDER BY over group
    // keys the costing compares HashAgg+Sort(ngroups) against
    // Sort(input)+GroupAggregate — near ngroups≈input the sorted shape can
    // win, and the walk refuses it: the suppress-then-refuse direction).
    if decorated {
        let input_rows = run.root.rel(rel_id).rows.max(1.0);
        if ngroups * DECOROOT_NGROUPS_MARGIN > input_rows {
            if trace_armed() {
                eprintln!(
                    "m5-suppress-refuse: decoroot scan-grouped (no hash-election margin: \
                     ngroups={ngroups:.0} rows={input_rows:.0})"
                );
            }
            return Ok(false);
        }
        return finish_knob_path(
            run,
            "decoroot",
            if n_text > 0 { "scan-grouped-text-decorated" } else { "scan-grouped-int-decorated" },
            class_guard(class),
            rte.relid,
            ngroups,
            rel_rows,
            rel_pages,
        );
    }
    // SE-CONSTKEY / SE-BARELIMIT knob-path finishes: shapes admitted only
    // by their knobs route through the dedicated finish (own trace prefix;
    // NOT BOOTSTRAP_MATRIX classes — tsv/route_to and the DEFAULT census
    // untouched), carrying the guard of the class the REAL keys classify
    // as (const keys add nothing to the partition; the bare-LIMIT freeze
    // rides its plain grouped class's economics).
    if n_const > 0 {
        return finish_knob_path(
            run,
            "constkey",
            if topn { "constkey-grouped-topn" } else { "constkey-grouped-agg" },
            class_guard(class),
            rte.relid,
            ngroups,
            rel_rows,
            rel_pages,
        );
    }
    if bare_limit {
        return finish_knob_path(
            run,
            "barelimit",
            "barelimit-grouped-agg",
            class_guard(class),
            rte.relid,
            ngroups,
            rel_rows,
            rel_pages,
        );
    }
    finish(run, class, rte.relid, ngroups, rel_rows, rel_pages)
}

/// Row flip 2 (CbHashJoinPlainAgg): plain whitelisted aggregation over one
/// explicit two-pgrcolumnar-relation join. Strictly narrower than the
/// runtime_hashjoin walk (probe ⊂ walk, risk P1) PLUS two planner-choice
/// guards the walk cannot express — the probe must also be confident the
/// SERIAL plan will BE an agg-over-HashJoin-over-two-SeqScans:
///   * neither rel carries an index (no serial merge/NL-with-inner-index
///     plan for the costing to prefer; unindexed equi-joins cost to hash);
///   * >=1 hashjoinable int-family equi clause in the JOIN quals.
/// Every early `false` keeps Gather exactly as today.
fn classify_join_covered<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &Query<'mcx>,
    je: &types_nodes::primnodes::JoinExpr<'mcx>,
) -> PgResult<bool> {
    use types_nodes::JoinType;
    // Phase-1 + right join families the walk admits (semi/anti arrive via
    // sublinks, prefiltered upstream).
    if !matches!(
        je.jointype,
        JoinType::JOIN_INNER | JoinType::JOIN_LEFT | JoinType::JOIN_RIGHT | JoinType::JOIN_FULL
    ) {
        return refuse_join("join family");
    }
    // Both arms plain relations (no nested joins: the multi-build-side SQL
    // shapes are the m5p1 admission gap, uncovered).
    let mut sides = [0usize; 2];
    for (i, arg) in [je.larg, je.rarg].into_iter().enumerate() {
        let Some(rtr) = arg.as_range_tbl_ref() else {
            return refuse_join("nested join tree (multi-build-side gap)");
        };
        sides[i] = rtr.rtindex as usize;
    }
    classify_join_sides(run, parse, sides[0], sides[1], je.quals)
}

/// Refusal diagnostics (PGRUST_M5_SUPPRESS_TRACE=1): the join probe's
/// guards are planner-choice-shaped and worth naming when they refuse.
/// The prefix is deliberately NOT `m5-suppress:` — the conformance leg's
/// M5CENSUS counts that exact prefix as SUPPRESSIONS, and the regress
/// corpus is full of join queries whose refusals would flood it.
fn refuse_join(why: &str) -> PgResult<bool> {
    if trace_armed() {
        eprintln!("m5-suppress-refuse: join probe ({why})");
    }
    Ok(false)
}

/// SE-SCANPASS named-refusal diagnostics. Same discipline as `refuse_join`:
/// the `m5-suppress-refuse:` prefix (NOT `m5-suppress:`, which the
/// conformance leg's M5CENSUS counts as a SUPPRESSION), so naming a
/// passthrough refusal never inflates the suppression count. Always returns
/// None (keeps Gather). Reached only when `scanpass_enabled()` — knob-OFF
/// never recognizes the shape, so the diagnostic is inert at default.
fn refuse_scanpass(why: &str) -> PgResult<bool> {
    if trace_armed() {
        eprintln!("m5-suppress-refuse: scan-passthrough ({why})");
    }
    Ok(false)
}

/// The passthrough-shape recognizer (SE-SCANPASS, band 72001). Called ONLY
/// under `scanpass_enabled()` for a single-relation `!hasAggs` SELECT that
/// is neither the bounded-top-N shape (keyed above) nor DISTINCT. It NAMES
/// the specific reason the shape is uncovered — one refusal per uncovered
/// expr/shape class — and always returns None (Gather stands). Naming, not
/// flipping: there is no `parallel_engine=runtime` row-emit arm, so every
/// arm of this recognizer keeps Gather; the reasons are the endgame §3.3
/// "no class routed by accident" surface and the future arm's admission
/// gates in embryo.
fn classify_scanpass(parse: &Query<'_>, rti: usize, is_cb: bool, has_quals: bool) -> PgResult<bool> {
    // Heap rels: the incumbent per-row drive owns them
    // (STANDALONE_SCAN_NO_UPSIDE — the row loop carries the identical
    // kernels; lanev2.rs:867). Not this arm's estate even if it existed.
    if !is_cb {
        return refuse_scanpass("heap rel — incumbent row drive owns it (STANDALONE_SCAN_NO_UPSIDE)");
    }
    // Full sort with no LIMIT (the bounded shape was keyed above): the
    // uncovered fullsort-shape-b row, owned by the sort-arm program.
    if !parse.sortClause.is_nil() {
        return refuse_scanpass("ordered passthrough (fullsort-shape-b) — sort-arm program owns it");
    }
    // Projection that is not a bare column reference: no vectorized
    // projection kernel is wired on a row-returning passthrough (the future
    // arm's covered-expr gate). Bare-Var tlists are the covered projection
    // class (the cb-q20 `SELECT UserID ...` shape).
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else {
            return refuse_scanpass("non-TargetEntry tlist");
        };
        if tle.resjunk {
            continue;
        }
        if key_var(tle.expr, rti).is_none() {
            return refuse_scanpass("projection expr not covered (bare-Var tlist only)");
        }
    }
    // A bare filtered (or unfiltered) row-returning pgrcolumnar passthrough
    // — the covered SHAPE (the cb-q20 class). Still refused: there is no
    // `parallel_engine=runtime` row-emit boundary to hand the suppressed
    // Gather to (every runtime arm folds). Owning enabler: the parallel
    // row-emit-boundary subsystem (notes/se-scanpass.md §4). The serial
    // lane executor (`pgrust.lane_executor`) already row-emits this exact
    // shape through `try_own_seq_scan`'s admitted standalone-cbstore path —
    // that is the World-A reuse, not a World-B Gather suppression.
    if has_quals {
        refuse_scanpass("bare filtered pgrcolumnar passthrough — no parallel row-emit arm (owning car: parallel-row-emit-boundary)")
    } else {
        refuse_scanpass("bare unfiltered pgrcolumnar passthrough — no parallel row-emit arm (owning car: parallel-row-emit-boundary)")
    }
}

/// The join classifier's shared body (both FROM forms of row flip 2: one
/// explicit JoinExpr, or the flat two-RangeTblRef FromExpr the planner
/// carries for INNER joins — `a JOIN b ON q` == `a, b WHERE q` by probe
/// time, quals in the FromExpr).
fn classify_join_sides<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &Query<'mcx>,
    rti_l: usize,
    rti_r: usize,
    join_quals: Option<Node<'mcx>>,
) -> PgResult<bool> {
    // Plain one-row aggregation only (the arm drives a plain agg sink):
    // no grouping, no DISTINCT, no ORDER BY/LIMIT decoration.
    if !parse.hasAggs
        || !parse.groupClause.is_nil()
        || !parse.distinctClause.is_nil()
        || !parse.sortClause.is_nil()
        || parse.limitCount.is_some()
        || parse.limitOffset.is_some()
    {
        return refuse_join("not a plain one-row aggregation");
    }
    let mut relids = [0u32; 2];
    let mut max_rows = 0.0f64;
    let mut heap: Vec<(usize, types_pathnodes::RelId)> = Vec::new();
    for (i, &rti) in [rti_l, rti_r].iter().enumerate() {
        let Some(rte) = parse.rtable.nth(rti - 1).as_range_tbl_entry() else {
            return refuse_join("side not a plain RTE");
        };
        if rte.rtekind != RTEKind::RTE_RELATION
            || rte.relkind != types_rel::RELKIND_RELATION
            || rte.inh
            || rte.tablesample.is_some()
        {
            return refuse_join("side not a plain relation");
        }
        relids[i] = rte.relid;
        let Some(rel_id) = run.root.simple_rel_array.get(rti).copied().flatten() else {
            return refuse_join("side has no RelOptInfo yet");
        };
        let rel = run.root.rel(rel_id);
        let is_cb = rel.amflags & AMFLAG_PGRCOLUMNAR != 0;
        if !is_cb {
            // TPCH-JHEAP: heap sides admit knob-gated (+ the executor K2
            // feed coherence mirror); OFF takes the pre-existing refusal
            // byte-for-byte. Index tolerance/stats ride jheap_shape_guards
            // below (they need the qual set).
            if !(jheap_enabled() && k2_heapfeed_live()) {
                return refuse_join("side not cbstore");
            }
            heap.push((i, rel_id));
        }
        max_rows = max_rows.max(rel.rows.max(0.0));
        // Unindexed-only guard (see the fn doc): an index on either side
        // lets the costing pick serial merge/NL shapes the walk refuses.
        // cbstore keeps it verbatim; heap rides the jheap tolerance.
        if is_cb && !rel.indexlist.is_empty() {
            return refuse_join("side has indexes");
        }
        // nbatch==1 on this side's estimate (whichever side the planner
        // hashes must fit): the flipped row is hashjoin-nbatch1; larger
        // builds keep Gather until the spill row's own flip.
        let Some(pt_id) = rel.pathtarget_id else {
            return refuse_join("side has no pathtarget yet");
        };
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
            return refuse_join("nbatch estimate > 1 (hashjoin-multibatch-spill row unflipped)");
        }
    }
    // >=1 hashjoinable int-family equi clause between the two sides in the
    // join quals (top-level AND terms only). By probe time the quals may be
    // an explicit BoolExpr AND, the planner's implicit-AND List (the
    // canonicalized form the FromExpr carries at path generation), or one
    // bare clause.
    let mut n_equi = 0usize;
    let mut int4_pair_only = true;
    let quals: Vec<Node<'_>> = match join_quals {
        None => return refuse_join("no join quals"),
        Some(q) => {
            if let Some(l) = q.as_list() {
                l.iter().collect()
            } else {
                match q.as_bool_expr() {
                    Some(be)
                        if matches!(
                            be.boolop,
                            types_nodes::primnodes::BoolExprType::AND_EXPR
                        ) =>
                    {
                        be.args.iter().collect()
                    }
                    _ => vec![q],
                }
            }
        }
    };
    for &qual in &quals {
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
            // Count EVERY hashjoinable clause (the GL-HJSEAT-2 seat-lift
            // predicate needs "exactly one, int4=int4" — the plan-time
            // image of the executor's dense_cols gate).
            n_equi += 1;
            if va.vartype != INT4OID || vb.vartype != INT4OID {
                int4_pair_only = false;
            }
        }
    }
    if n_equi == 0 {
        return refuse_join("no hashjoinable int-family equi clause");
    }
    // TPCH-JHEAP: the heap-side guards (stats on heap equi keys,
    // enable_hashjoin, index tolerance + NL margin). The 2-rel plain form
    // additionally refuses heap SELF-joins outright (the B1 alias-EC
    // hazard is newly reachable on this row's heap surface — fail-closed;
    // the cbstore census is byte-untouched).
    if !heap.is_empty() {
        if relids[0] == relids[1] {
            return refuse_join("relation appears more than once (EC self-join clause)");
        }
        if !jheap_shape_guards(run, parse, &[rti_l, rti_r], &quals, &heap)? {
            return Ok(false);
        }
    }
    // Emit discipline: every non-junk tlist entry is a whitelisted plain
    // aggregate whose args live on either joined rel (count(*) included).
    // TPCH-NUMJOIN (CAR 2, knob-gated): plain sum/avg(NUMERIC) over
    // parallel-safe joined-rel arg exprs additionally admit — the q14/q19
    // sum(price*(1-disc)) family (the plain-join arm's export speaks the
    // same relocated runtime-partial vocabulary via the poly manifest).
    let mut n = 0usize;
    let mut n_numeric = 0usize;
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
        if is_whitelisted_agg_2rti(tle.expr, rti_l, rti_r, PLAIN_FOLD_AGGS) {
            n += 1;
            continue;
        }
        if aggjoin_numeric_enabled()
            && is_numeric_expr_agg_nrti(run, tle.expr, &[rti_l, rti_r])?
        {
            n += 1;
            n_numeric += 1;
            continue;
        }
        return refuse_join("tlist entry not a whitelisted plain agg");
    }
    if n == 0 {
        return refuse_join("empty tlist");
    }
    // GL-HJSEAT-2 SEAT-SCOPED FLOOR LIFT (letter: scratchpad/night/
    // hj-seat-gate-and-floor-rederivation.md; witnessed band seat/legacy
    // 0.636-0.764 at 2.5M/5M/10M dop4 + 5M dop16, jobs 4aae/3fa8/1877/3862
    // @ f7022d98e, 2026-07-21): when the join is SEAT-SHAPED — exactly one
    // hashjoinable equi clause and it is bare int4 Var = int4 Var (the
    // plan-time image of the executor's dense_cols gate) — and the HJPROBE-V2
    // knob is live (flipped-kill; same spelling as the executor, the
    // GROUPSINK knob-coherence law), the 2M ceiling lifts: runtime+seat beat
    // legacy PHJ at every witnessed band point. The seat's remaining laws
    // (probe/build ratio >= 1 via seat_ok, range <= 4x at build) stay
    // executor-side; a build-time refusal degrades that query to the v1
    // runtime probe at the witnessed 1.15-1.66x vs PHJ — the letter's
    // bounded residual. Non-seat-shaped joins keep the 2M ceiling unchanged.
    let seat_shaped = n_equi == 1 && int4_pair_only;
    if seat_shaped && hjprobe_v2_live() {
        return finish_seat_lifted(run, relids[0], max_rows);
    }
    // Floor guard input: the larger side's estimated rows (the ladder's
    // per-table N; the probe fixture's dim side is negligible).
    // Knob-admitted shapes route through the knob-path finishes (own trace
    // tags; class row / tsv / drift guards untouched). Heap-fed shapes
    // carry the jheap floor (min 1M — the heap fold economics); the pure
    // numeric widening keeps the CbHashJoinPlainAgg floor.
    if !heap.is_empty() {
        return finish_knob_path(
            run,
            "jheap",
            if n_numeric > 0 { "plainjoin-heap+numeric" } else { "plainjoin-heap" },
            jheap_guard(),
            relids[0],
            0.0,
            max_rows,
            0.0,
        );
    }
    if n_numeric > 0 {
        return finish_knob_path(
            run,
            "aggjoinnum",
            "plainjoin-numeric",
            class_guard(CoverClass::CbHashJoinPlainAgg),
            relids[0],
            0.0,
            max_rows,
            0.0,
        );
    }
    finish(run, CoverClass::CbHashJoinPlainAgg, relids[0], 0.0, max_rows, 0.0)
}

/// GL-HJSEAT-2 knob coherence (the GROUPSINK/AGG_POLY precedent): the
/// executor's HJPROBE-V2 kill (`PGRUST_LANE_V2_HJPROBE_V2=0|off`,
/// FLIPPED-KILL: default ON) must also void the planner's seat-scoped floor
/// lift — a killed seat above 2M would ship the witnessed 1.15-1.66x
/// un-seated loss. Same spelling, same default, read once per process.
fn hjprobe_v2_live() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_LANE_V2_HJPROBE_V2").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// [`finish`] for the seat-lifted CbHashJoinPlainAgg path: the class floor's
/// 2M ceiling does not apply (the witnessed band has no structure — the
/// gated seat wins every measured point at every size/dop); every other
/// finish duty (coverage answer, trace) is identical. Separate fn so the
/// unlifted `finish` path stays byte-identical for every other caller.
fn finish_seat_lifted(run: &mut PlannerRun<'_>, relid: u32, rows: f64) -> PgResult<bool> {
    let class = CoverClass::CbHashJoinPlainAgg;
    let covered = class_covered(class);
    if covered && trace_armed() {
        let _ = run;
        eprintln!(
            "m5-suppress: engine=runtime class={class:?} relid={relid} rows={rows:.0} \
             seat-lift => gather suppressed (GL-HJSEAT-2)"
        );
    }
    Ok(covered)
}

/// Top-level AND terms of an optional qual tree into `out` (explicit
/// BoolExpr AND, the planner's implicit-AND List — the canonicalized form
/// the FromExpr carries at path generation — or one bare clause).
fn push_and_terms<'mcx>(quals: Option<Node<'mcx>>, out: &mut Vec<Node<'mcx>>) {
    let Some(q) = quals else { return };
    if let Some(l) = q.as_list() {
        out.extend(l.iter());
        return;
    }
    match q.as_bool_expr() {
        Some(be) if matches!(be.boolop, types_nodes::primnodes::BoolExprType::AND_EXPR) => {
            out.extend(be.args.iter());
        }
        _ => out.push(q),
    }
}

/// m5p1 (band 88001): left-deep INNER JoinExpr chain collector — every
/// level INNER with a plain rarg RangeTblRef, the deepest larg a
/// RangeTblRef; each level's ON-qual AND terms accumulate. Any other nested
/// shape (outer types, right-deep/bushy args) returns false — uncovered by
/// construction (probe narrower than the walk, which admits general trees).
fn collect_inner_chain<'mcx>(
    je: &types_nodes::primnodes::JoinExpr<'mcx>,
    rtis: &mut Vec<usize>,
    out_quals: &mut Vec<Node<'mcx>>,
) -> bool {
    if je.jointype != types_nodes::JoinType::JOIN_INNER {
        return false;
    }
    let Some(rarg) = je.rarg.as_range_tbl_ref() else { return false };
    push_and_terms(je.quals, out_quals);
    let deep_ok = if let Some(inner) = je.larg.as_join_expr() {
        collect_inner_chain(inner, rtis, out_quals)
    } else if let Some(l) = je.larg.as_range_tbl_ref() {
        rtis.push(l.rtindex as usize);
        true
    } else {
        false
    };
    rtis.push(rarg.rtindex as usize);
    deep_ok
}

/// `is_whitelisted_agg` over N candidate range-table indexes (the
/// multibuild row): the aggregate's single Var arg may live on any joined
/// rel.
fn is_whitelisted_agg_nrti(expr: Node<'_>, rtis: &[usize], whitelist: &[u32]) -> bool {
    let Some(agg) = expr.as_aggref() else { return false };
    if !whitelist.contains(&agg.aggfnoid) {
        return false;
    }
    rtis.iter().any(|&rti| aggref_plain(agg, rti))
}

/// TPCH-NUMJOIN (CAR 2): a structurally plain `sum(NUMERIC)` /
/// `avg(NUMERIC)` aggregate (no ORDER BY/DISTINCT/FILTER/variadic/
/// ordered-set/levelsup decoration) over ONE argument expression that
///   (a) the planner's own `is_parallel_safe` admits (it runs on helpers
///       through the join arm's per-row evaltrans transition program — C's
///       checked program, so the arg SHAPE is otherwise free: the
///       sum(price*(1-disc)) family), and
///   (b) references ONLY the joined relations (every level-0 varno in the
///       arg sits in `rtis` — fail-closed against alias/rowmark RTEs the
///       FROM census did not enumerate).
/// The stddev/variance family (numeric_accum, sum_x2 states) is NOT here:
/// only the NumericAgg-state pair the relocated runtime-partial vocabulary
/// carries (F_AVG_NUMERIC 2103 / F_SUM_NUMERIC 2114, transfn
/// numeric_avg_accum without sum_x2 — the SE-AGGPOLY OIDs of record).
fn is_numeric_expr_agg_nrti<'mcx>(
    run: &PlannerRun<'mcx>,
    expr: Node<'mcx>,
    rtis: &[usize],
) -> PgResult<bool> {
    let Some(agg) = expr.as_aggref() else { return Ok(false) };
    if !matches!(agg.aggfnoid, F_AVG_NUMERIC | F_SUM_NUMERIC)
        || agg.agglevelsup != 0
        || agg.aggkind != AGGKIND_NORMAL
        || agg.aggvariadic
        || !agg.aggorder.is_nil()
        || !agg.aggdistinct.is_nil()
        || agg.aggfilter.is_some()
        || !agg.aggdirectargs.is_nil()
        || agg.args.len() != 1
    {
        return Ok(false);
    }
    let Some(arg_tle) = agg.args.nth(0).as_target_entry() else { return Ok(false) };
    if !crate::is_parallel_safe_opt(run, Some(arg_tle.expr))? {
        return Ok(false);
    }
    let varnos = vars::pull_varnos(run.mcx, arg_tle.expr)?;
    for vn in varnos.iter() {
        if !rtis.contains(&(vn as usize)) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// m5p1 (band 88001): the N-relation multibuild classifier — the shared
/// body of both keyed FROM forms (flat N-RangeTblRef; left-deep INNER
/// chain). Strictly narrower than the multibuild walk (probe ⊂ walk, risk
/// P1) PLUS the planner-choice guards the walk cannot express — unindexed
/// rels (no serial NL-with-inner-index plan for the costing to prefer),
/// DISTINCT relids (a repeated relation lets the EC machinery derive a
/// dim-dim equality clause between the two aliases, and the costing then
/// prefers a serial Merge Join + Materialize on it WITHOUT any index —
/// the B1 suppress-then-refuse false positive; refused outright), EVERY
/// rel's build estimate nbatch==1 (any rel may be hashed; the walk is
/// unbatched-only), and a CONNECTED int-family hashjoinable equi graph
/// (a disconnected component would cost a cartesian shape the walk
/// refuses). Residual risk — the costing electing merge/NL among DISTINCT
/// unindexed rels via an EC-derived clause — rides GL-M5P1-1's engagement
/// counters. Every early `false` keeps Gather exactly as today.
/// m5p1 knob coherence: the executor walk's multibuild kill switch
/// (`PGRUST_RUNTIME_HASHJOIN_MULTIBUILD=0`) must also un-key the probe —
/// a suppression the walk then refuses would land on serial (risk P1's
/// false-positive direction). Same spelling, own cached read.
fn multibuild_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_HASHJOIN_MULTIBUILD").map_or(true, |v| v.trim() != "0")
    })
}

/// SE-SCANPASS (band 72001, branch se/scan-passthrough): the passthrough
/// lane knob, `PGRUST_LANE_V2_SCANPASS`, **default OFF** (the K1-latemat
/// idiom, batch_source.rs:152 — any spelling but `1`/`on` fails safe to
/// today's behaviour). Default OFF because there is no covered arm to hand a
/// suppressed passthrough Gather to: every `parallel_engine=runtime` arm
/// FOLDS to a small result, so a row-RETURNING parallel scan has no emit
/// boundary today (the census `gap:scan-passthrough` row; notes/se-scanpass.md
/// §2). When OFF the probe never even recognizes the shape — it hits the
/// generic `return Ok(false)` exactly as before, so the plan-time bytes,
/// the census, and every regress leg are byte-identical. When ON the probe
/// RECOGNIZES the passthrough shape and emits a NAMED refusal
/// (`classify_scanpass`) instead of the silent fall-through — the §3.3
/// endgame "no class routed by accident" surface and the seam a future
/// row-emit arm engages from. It still returns None (keeps Gather): naming
/// a refusal is not the same as flipping route_to (that needs the arm + a
/// measured win — see notes/se-scanpass.md §4).
fn scanpass_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| scanpass_spelling_on(std::env::var("PGRUST_LANE_V2_SCANPASS").as_deref().ok()))
}

/// The default-OFF spelling rule, factored pure for exhaustive unit tests:
/// ON iff the value is exactly `1` or `on`; every other spelling (incl.
/// unset, `0`, `off`, typos) fails safe to OFF.
fn scanpass_spelling_on(v: Option<&str>) -> bool {
    matches!(v, Some("1") | Some("on"))
}

fn classify_multibuild<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &Query<'mcx>,
    rtis: &[usize],
    quals: &[Node<'mcx>],
) -> PgResult<bool> {
    // SE-AGGJOIN (band 87001): grouped shapes divert to the grouped-sink
    // classifier (its own knobs + guards); everything below is the plain
    // one-row multibuild row verbatim.
    if parse.hasAggs && !parse.groupClause.is_nil() {
        return classify_aggjoin_grouped(run, parse, rtis, quals);
    }
    if !multibuild_enabled() {
        return refuse_join("multibuild disabled");
    }
    // Plain one-row aggregation only (the walk drives the plain-agg sink).
    if !parse.hasAggs
        || !parse.groupClause.is_nil()
        || !parse.distinctClause.is_nil()
        || !parse.sortClause.is_nil()
        || parse.limitCount.is_some()
        || parse.limitOffset.is_some()
    {
        return refuse_join("not a plain one-row aggregation");
    }
    let Some((relids, max_rows, heap)) = multibuild_rel_guards(run, parse, rtis)? else {
        return Ok(false);
    };
    if !equi_graph_connected(rtis, quals)? {
        return refuse_join("equi graph does not connect all relations");
    }
    // TPCH-JHEAP: the heap-side guards (stats on heap equi keys,
    // enable_hashjoin, index tolerance + NL margin) over the full qual set.
    if !jheap_shape_guards(run, parse, rtis, quals, &heap)? {
        return Ok(false);
    }
    // EC discipline (SE-AGGJOIN fixer — the grouped row's hostile review
    // proved the channel PRE-EXISTS here: at base 11fe9c48b the plain
    // variant of H2 keys CbHashJoinMultiBuild and lands the identical
    // serial merge plan). Distinct-relid dims off one shared fact key merge
    // into one EC exactly like B1's aliases do; refuse shared-endpoint
    // shapes. The plain row keeps its wider qual admission otherwise
    // (filter-term X5-class discipline = GL-M5P1-1's handoff).
    if ec_disjoint_equi_edges(rtis, quals)?.is_none() {
        return refuse_join("equi terms share a join key (EC-derived clause hazard)");
    }
    // Emit discipline: every tlist entry a whitelisted plain aggregate
    // whose args live on one of the joined rels (count(*) included).
    // TPCH-NUMJOIN (CAR 2, knob-gated): plain sum/avg(NUMERIC) over
    // parallel-safe joined-rel arg exprs additionally admit (see
    // classify_join_sides' twin note).
    let mut n = 0usize;
    let mut n_numeric = 0usize;
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
        if is_whitelisted_agg_nrti(tle.expr, rtis, PLAIN_FOLD_AGGS) {
            n += 1;
            continue;
        }
        if aggjoin_numeric_enabled() && is_numeric_expr_agg_nrti(run, tle.expr, rtis)? {
            n += 1;
            n_numeric += 1;
            continue;
        }
        return refuse_join("tlist entry not a whitelisted plain agg");
    }
    if n == 0 {
        return refuse_join("empty tlist");
    }
    // Floor guard input: the largest rel's estimated rows (the nbatch1
    // ladder's per-table N — provisional reuse, see class_guard). Heap-fed
    // shapes carry the jheap floor (min 1M — the heap fold economics).
    if !heap.is_empty() {
        return finish_knob_path(
            run,
            "jheap",
            if n_numeric > 0 { "multibuild-heap+numeric" } else { "multibuild-heap" },
            jheap_guard(),
            relids[0],
            0.0,
            max_rows,
            0.0,
        );
    }
    if n_numeric > 0 {
        return finish_knob_path(
            run,
            "aggjoinnum",
            "multibuild-numeric",
            class_guard(CoverClass::CbHashJoinMultiBuild),
            relids[0],
            0.0,
            max_rows,
            0.0,
        );
    }
    finish(run, CoverClass::CbHashJoinMultiBuild, relids[0], 0.0, max_rows, 0.0)
}

/// The multibuild per-relation guards, shared by the plain and grouped rows
/// (extracted verbatim at SE-AGGJOIN): plain DISTINCT rels (the B1
/// self-join discipline), EVERY rel's build estimate nbatch==1; cbstore
/// rels stay unindexed-only verbatim. TPCH-JHEAP: HEAP rels admit
/// knob-gated (the executor's K2 feed is default-ON; the coherence mirror
/// keys both kills) — their index tolerance and stats discipline are the
/// caller's `jheap_shape_guards` (they need the qual set). Returns the
/// relids, the largest side's rows, and the heap sides as
/// (index-into-rtis, RelId). `None` = refused (traced).
fn multibuild_rel_guards(
    run: &mut PlannerRun<'_>,
    parse: &Query<'_>,
    rtis: &[usize],
) -> PgResult<Option<(Vec<u32>, f64, Vec<(usize, types_pathnodes::RelId)>)>> {
    let mut relids = Vec::with_capacity(rtis.len());
    let mut max_rows = 0.0f64;
    let mut heap: Vec<(usize, types_pathnodes::RelId)> = Vec::new();
    for (i, &rti) in rtis.iter().enumerate() {
        let Some(rte) = parse.rtable.nth(rti - 1).as_range_tbl_entry() else {
            return refuse_join_none("side not a plain RTE");
        };
        if rte.rtekind != RTEKind::RTE_RELATION
            || rte.relkind != types_rel::RELKIND_RELATION
            || rte.inh
            || rte.tablesample.is_some()
        {
            return refuse_join_none("side not a plain relation");
        }
        if relids.contains(&rte.relid) {
            // B1 guard: a relation joined twice (self-join via aliases)
            // seeds an EquivalenceClass spanning both aliases; the planner
            // derives the alias-alias equality clause and can cost a serial
            // Merge Join + Materialize on it with NO indexes present — a
            // shape the multibuild walk refuses, which would land the
            // suppression on serial (probe-outruns-walk, risk P1).
            return refuse_join_none("relation appears more than once (EC self-join clause)");
        }
        relids.push(rte.relid);
        let Some(rel_id) = run.root.simple_rel_array.get(rti).copied().flatten() else {
            return refuse_join_none("side has no RelOptInfo yet");
        };
        let rel = run.root.rel(rel_id);
        let is_cb = rel.amflags & AMFLAG_PGRCOLUMNAR != 0;
        if !is_cb {
            // TPCH-JHEAP: a non-cbstore plain relation is the heap AM (the
            // TableAm vocabulary is {Heap, Pgrcolumnar}; the executor walk
            // double-checks via seq_scan_is_heap). Knob OFF (or either
            // executor feed kill thrown) takes the pre-existing refusal
            // byte-for-byte, trace included.
            if !(jheap_enabled() && k2_heapfeed_live()) {
                return refuse_join_none("side not cbstore");
            }
            heap.push((i, rel_id));
        }
        max_rows = max_rows.max(rel.rows.max(0.0));
        // cbstore keeps the blanket unindexed-only rule verbatim; heap
        // index tolerance is the caller's jheap_shape_guards (needs quals).
        if is_cb && !rel.indexlist.is_empty() {
            return refuse_join_none("side has indexes");
        }
        let Some(pt_id) = rel.pathtarget_id else {
            return refuse_join_none("side has no pathtarget yet");
        };
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
            return refuse_join_none("nbatch estimate > 1 (multibuild walk is unbatched-only)");
        }
    }
    Ok(Some((relids, max_rows, heap)))
}

/// Traced refusal in `Option` position (the rel-guards helper's shape).
fn refuse_join_none<T>(why: &str) -> PgResult<Option<T>> {
    let _ = refuse_join(why)?;
    Ok(None)
}

/// TPCH-JHEAP: the heap-side shape guards over the WHOLE qual set — run by
/// every join classifier when `multibuild_rel_guards` admitted heap sides.
/// `false` = refuse (traced). The guards, in order:
///   * `enable_hashjoin` required ON (with it off, the post-suppression
///     serial election on heap rels is NL/merge by construction — the
///     suppress-then-refuse direction; the grouped classifier requires it
///     anyway, this extends the law to the plain rows' heap shapes);
///   * X6, heap-flavored: every int-family hashjoinable equi term with a
///     HEAP endpoint needs statistics on BOTH key vars (stats-free heap
///     rels default the join selectivities into merge landings — the
///     SE-AGGJOIN live finding, now enforced for the plain rows' heap
///     shapes too);
///   * index tolerance (the q06/AggPolyHeapPlain precedent, join-widened;
///     TPC-H rels carry their PK indexes, so a blanket unindexed rule
///     would never key them): per heap-rel index — expression/partial
///     indexes refuse; an index whose KEY columns are referenced by any
///     RESTRICTION term refuses (an index path becomes electable); an
///     index COVERING every referenced column refuses (index-only scan
///     electable qual-free); an index on a JOIN-KEY column applies the
///     NL-margin law — every equi-PARTNER rel must carry >=
///     JHEAP_NL_MARGIN x this rel's rows (blocks the small-outer
///     NL-with-inner-index and index-sorted merge elections);
///   * whole-row/system-column references on a heap rel refuse (nothing
///     the tolerance can reason about).
fn jheap_shape_guards<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &Query<'mcx>,
    rtis: &[usize],
    quals: &[Node<'mcx>],
    heap: &[(usize, types_pathnodes::RelId)],
) -> PgResult<bool> {
    if heap.is_empty() {
        return Ok(true);
    }
    if !crate::gucs::enable_hashjoin() {
        return refuse_join("heap side with the hashjoin planner path disabled");
    }
    let heap_idx = |i: usize| heap.iter().any(|&(h, _)| h == i);
    // Term census: recognized equi edges (endpoint indexes + attnos) vs
    // residual terms (restriction filters and anything unrecognized).
    let mut edges: Vec<(usize, i16, usize, i16)> = Vec::new();
    let mut resid: Vec<Node<'mcx>> = Vec::new();
    for &q in quals {
        let mut edge = None;
        if let Some(op) = q.as_op_expr() {
            if op.args.len() == 2 {
                let (a, b) = (op.args.nth(0), op.args.nth(1));
                let hit = |e: Node<'mcx>| rtis.iter().position(|&rti| key_var(e, rti).is_some());
                if let (Some(ia), Some(ib)) = (hit(a), hit(b)) {
                    if ia != ib {
                        if let (Some(va), Some(vb)) =
                            (key_var(a, rtis[ia]), key_var(b, rtis[ib]))
                        {
                            if is_int_family(va.vartype)
                                && is_int_family(vb.vartype)
                                && lsyscache::op_hashjoinable(op.opno, va.vartype)?
                            {
                                if (heap_idx(ia) || heap_idx(ib))
                                    && (!key_var_estimable(run, a)?
                                        || !key_var_estimable(run, b)?)
                                {
                                    return refuse_join(
                                        "heap join key without statistics (X6, heap-flavored)",
                                    );
                                }
                                edge = Some((ia, va.varattno, ib, vb.varattno));
                            }
                        }
                    }
                }
            }
        }
        match edge {
            Some(e) => edges.push(e),
            None => resid.push(q),
        }
    }
    use types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
    let raw = |m: i32| m + FirstLowInvalidHeapAttributeNumber;
    for &(i, rel_id) in heap {
        let rti = rtis[i];
        // Column censuses for THIS rel: restriction references (residual
        // terms only) and all references (residuals + tlist; equi-edge join
        // keys are tracked by attno separately).
        let mut resid_bm = types_nodes::Bitmapset::empty();
        for &q in &resid {
            vars::pull_varattnos(run.mcx, q, rti as i32, &mut resid_bm)?;
        }
        let mut all_bm = types_nodes::Bitmapset::empty();
        for &q in &resid {
            vars::pull_varattnos(run.mcx, q, rti as i32, &mut all_bm)?;
        }
        for tle_node in &parse.targetList {
            let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
            vars::pull_varattnos(run.mcx, tle.expr, rti as i32, &mut all_bm)?;
        }
        for m in all_bm.iter() {
            if raw(m) <= 0 {
                return refuse_join("heap side with whole-row/system-column references");
            }
        }
        let join_attnos: Vec<i16> = edges
            .iter()
            .flat_map(|&(ia, aa, ib, ab)| {
                [(ia, aa), (ib, ab)]
                    .into_iter()
                    .filter(|&(x, _)| x == i)
                    .map(|(_, a)| a)
            })
            .collect();
        let partners: Vec<usize> = edges
            .iter()
            .filter_map(|&(ia, _, ib, _)| {
                if ia == i {
                    Some(ib)
                } else if ib == i {
                    Some(ia)
                } else {
                    None
                }
            })
            .collect();
        let rel_rows = run.root.rel(rel_id).rows.max(0.0);
        for index in run.root.rel(rel_id).indexlist.iter() {
            if !index.indexprs.is_empty() || !index.indpred.is_empty() {
                return refuse_join("heap expression/partial index");
            }
            let keys = &index.indexkeys;
            let nkey = (index.nkeycolumns as usize).min(keys.len());
            for m in resid_bm.iter() {
                let a = raw(m);
                if keys[..nkey].iter().any(|&k| k == a) {
                    return refuse_join("heap index key referenced by a filter qual");
                }
            }
            // Referenced set = residuals + tlist (all_bm) + the join keys.
            let covers_all = all_bm.iter().all(|m| keys.iter().any(|&k| k == raw(m)))
                && join_attnos.iter().all(|&j| keys.iter().any(|&k| k == i32::from(j)));
            if covers_all {
                return refuse_join("heap covering index (index-only scan electable)");
            }
            if keys[..nkey].iter().any(|&k| join_attnos.iter().any(|&j| i32::from(j) == k)) {
                // Join-key index: the NL/merge hazard — every equi partner
                // must dominate this rel by the margin.
                for &p in &partners {
                    let Some(p_rel) = run.root.simple_rel_array.get(rtis[p]).copied().flatten()
                    else {
                        return Ok(false);
                    };
                    let p_rows = run.root.rel(p_rel).rows.max(0.0);
                    if p_rows < JHEAP_NL_MARGIN * rel_rows {
                        return refuse_join(
                            "heap join-key index without the NL-election margin",
                        );
                    }
                }
            }
        }
    }
    Ok(true)
}

/// The multibuild connected-equi-graph check, shared by the plain and
/// grouped rows (extracted verbatim at SE-AGGJOIN): union-find over the
/// int-family hashjoinable equi terms; `false` = disconnected (a cartesian
/// shape the walk refuses; the caller traces the refusal).
fn equi_graph_connected(rtis: &[usize], quals: &[Node<'_>]) -> PgResult<bool> {
    // Connectivity (union-find with path halving).
    fn uf_find(uf: &mut [usize], mut x: usize) -> usize {
        while uf[x] != x {
            uf[x] = uf[uf[x]];
            x = uf[x];
        }
        x
    }
    let mut uf: Vec<usize> = (0..rtis.len()).collect();
    for &qual in quals {
        let Some(op) = qual.as_op_expr() else { continue };
        if op.args.len() != 2 {
            continue;
        }
        let (a, b) = (op.args.nth(0), op.args.nth(1));
        let hit = |e: Node<'_>| rtis.iter().position(|&rti| key_var(e, rti).is_some());
        let (Some(ia), Some(ib)) = (hit(a), hit(b)) else { continue };
        if ia == ib {
            continue;
        }
        let (Some(va), Some(vb)) = (key_var(a, rtis[ia]), key_var(b, rtis[ib])) else {
            continue;
        };
        if is_int_family(va.vartype)
            && is_int_family(vb.vartype)
            && lsyscache::op_hashjoinable(op.opno, va.vartype)?
        {
            let (ra, rb) = (uf_find(&mut uf, ia), uf_find(&mut uf, ib));
            if ra != rb {
                uf[ra] = rb;
            }
        }
    }
    let root0 = uf_find(&mut uf, 0);
    for i in 1..rtis.len() {
        if uf_find(&mut uf, i) != root0 {
            return Ok(false);
        }
    }
    Ok(true)
}

/// SE-AGGJOIN fixer guard (hostile-review BLOCKING find, legs
/// h1_ecdim/h2_trans): EquivalenceClass derivation evades PER-QUAL
/// discipline. Two equi terms sharing a key var — H2 `f.k1 = d1.k AND
/// f.k1 = db.k` (EC-derived dim-dim clause), H1 `f.k1 = d1.k AND
/// d1.k = d2.k` (written dim-dim term through a shared var) — merge into
/// ONE EquivalenceClass; the planner derives the dim-dim equality and can
/// cost a serial Merge Join + Materialize on it with no indexes present —
/// a shape the multibuild walk refuses (suppress-then-refuse, the B1
/// defect class, EC flavor; B1's repeated-relid guard is the SAME
/// mechanism through alias ECs). Guard: over the DISTINCT equi edges
/// (exact-duplicate terms collapse first — the planner dedups them into
/// one two-var EC with nothing left to derive, so `f.k1 = d1.k AND
/// f.k1 = d1.k` stays owned), no (rel, attno) endpoint may appear in more
/// than one edge — pairwise-DISJOINT two-var ECs leave the planner nothing
/// to derive, so the join graph it costs is exactly the written tree.
/// `None` = a shared endpoint (caller refuses); `Some(n)` = n distinct
/// edges (the grouped row additionally requires n == rels-1: a TREE, no
/// parallel edges — multi-clause hash joins are outside the proven
/// envelope, fail closed).
fn ec_disjoint_equi_edges(rtis: &[usize], quals: &[Node<'_>]) -> PgResult<Option<usize>> {
    let mut edges: Vec<((usize, i32), (usize, i32))> = Vec::new();
    for &qual in quals {
        let Some(op) = qual.as_op_expr() else { continue };
        if op.args.len() != 2 {
            continue;
        }
        let (a, b) = (op.args.nth(0), op.args.nth(1));
        let hit = |e: Node<'_>| rtis.iter().position(|&rti| key_var(e, rti).is_some());
        let (Some(ia), Some(ib)) = (hit(a), hit(b)) else { continue };
        if ia == ib {
            continue;
        }
        let (Some(va), Some(vb)) = (key_var(a, rtis[ia]), key_var(b, rtis[ib])) else {
            continue;
        };
        if !is_int_family(va.vartype)
            || !is_int_family(vb.vartype)
            || !lsyscache::op_hashjoinable(op.opno, va.vartype)?
        {
            continue;
        }
        let (ea, eb) = ((ia, va.varattno as i32), (ib, vb.varattno as i32));
        let edge = if ea <= eb { (ea, eb) } else { (eb, ea) };
        if !edges.contains(&edge) {
            edges.push(edge);
        }
    }
    for (i, e1) in edges.iter().enumerate() {
        for e2 in edges.iter().skip(i + 1) {
            if e1.0 == e2.0 || e1.0 == e2.1 || e1.1 == e2.0 || e1.1 == e2.1 {
                return Ok(None);
            }
        }
    }
    Ok(Some(edges.len()))
}

/// SE-AGGJOIN (band 87001): the grouped-agg-over-join classifier — the
/// CbHashJoinGroupedAgg row (see the CoverClass doc for the full guard
/// list). Shared by every keyed FROM form (flat 2..=6-RangeTblRef INNER;
/// left-deep INNER chains via classify_multibuild's divert). Strictly
/// narrower than `agg_grouped_runtime_admissible` + the multibuild state
/// walk, PLUS the planner-choice guards the walk cannot express. Every
/// early `false` keeps Gather exactly as today.
/// Knob coherence: `PGRUST_RUNTIME_HASHJOIN_GROUPSINK=0` (the grouped
/// arm's kill) un-keys the class outright; `PGRUST_RUNTIME_HASHJOIN_
/// MULTIBUILD=0` un-keys the 2+-join tree forms (the walk refuses them
/// then) while the single-join form stays keyed (the walk still owns it).
fn groupsink_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_HASHJOIN_GROUPSINK").map_or(true, |v| v.trim() != "0")
    })
}

/// Probe-side headroom under the executor's grouped export cap
/// (PGRUST_RUNTIME_HASHJOIN_GROUPSINK_MAX_GROUPS default 131072): estimates
/// above HALF the cap keep Gather — an estimate that near-misses the cap
/// would engage, cross it at runtime, and land the R5 serial rerun.
const GROUPSINK_NGROUPS_FLOOR: f64 = 65_536.0;

/// SE-AGGJOIN stats guard (the e2e leg-X6 LIVE finding): on STATISTICS-FREE
/// relations the costing's default join selectivities explode the join-row
/// estimates and elect serial MERGE shapes the walk refuses — the B1
/// suppress-then-refuse defect class, costing flavor (reproduced: unanalyzed
/// 3-rel fixture planned `HashAggregate -> Merge Join` post-suppression). A
/// key var is ESTIMABLE when it carries pg_statistic rows or is provably
/// unique — the signals `eqjoinsel` actually consults (the pgrcolumnar
/// FOOTER NDV feeds only the GROUP estimation path, not join selectivity —
/// footer-only rels reproduced the merge landing with a perfect ngroups
/// estimate, so footers deliberately do NOT admit; a footer-backed ANALYZE
/// harvests stadistinct and is the class's admission ticket — GL-AGGJOIN-1
/// leg (c) verifies the fleet fixtures key). Any key without one keeps
/// Gather.
fn key_var_estimable<'mcx>(
    run: &mut PlannerRun<'mcx>,
    v_node: Node<'mcx>,
) -> PgResult<bool> {
    let id = run.intern_expr(v_node);
    let vd = crate::selfuncs::examine_variable(run, id, v_node, 0)?;
    Ok(vd.stats.is_some() || vd.isunique)
}

fn classify_aggjoin_grouped<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &Query<'mcx>,
    rtis: &[usize],
    quals: &[Node<'mcx>],
) -> PgResult<bool> {
    if !groupsink_enabled() {
        return refuse_join("groupsink disabled");
    }
    if rtis.len() >= 3 && !multibuild_enabled() {
        return refuse_join("multibuild disabled (grouped tree)");
    }
    // Bare grouped aggregation, or (TPCH-DECOROOT, CAR 1) a WHITELISTED
    // decorated root: ORDER BY [+ LIMIT/OFFSET] above the grouped agg. The
    // arm fills the full grouped table and streams the serial emit paths
    // off it (se-aggjoin §3.1), so the serial Sort/Limit above consumes it;
    // sort keys are policed by the emit walk below (every tlist entry —
    // junk sort keys included — must be a group-key ref or an admitted
    // aggregate). DISTINCT stays refused (distinct-sink composition over
    // the join sink unproven); bare LIMIT/OFFSET without ORDER BY stays
    // refused (the freeze composition is unproven on the join sink — the
    // scan classes' SE-BARELIMIT row owns that pattern). Knob OFF takes the
    // pre-existing refusal byte-for-byte.
    if !parse.hasAggs || parse.groupClause.is_nil() || !parse.distinctClause.is_nil() {
        return refuse_join("not a bare grouped aggregation");
    }
    let decorated = !parse.sortClause.is_nil()
        || parse.limitCount.is_some()
        || parse.limitOffset.is_some();
    if decorated && !decoroot_enabled() {
        return refuse_join("not a bare grouped aggregation");
    }
    if decorated && parse.sortClause.is_nil() {
        return refuse_join(
            "LIMIT/OFFSET without ORDER BY over a grouped join (freeze composition unproven on the join sink)",
        );
    }
    // With either planner path off, the serial plan is a sort-grouped /
    // merge / NL shape the walk refuses (the suppress-then-refuse
    // direction, risk P1's false-positive arm) — keep Gather.
    if !crate::gucs::enable_hashagg() || !crate::gucs::enable_hashjoin() {
        return refuse_join("hashagg/hashjoin planner paths disabled");
    }
    let Some((relids, max_rows, heap)) = multibuild_rel_guards(run, parse, rtis)? else {
        return Ok(false);
    };
    if !equi_graph_connected(rtis, quals)? {
        return refuse_join("equi graph does not connect all relations");
    }
    // TPCH-JHEAP: the heap-side guards (index tolerance + NL margin;
    // stats/enable_hashjoin overlap this classifier's own X5/X6 discipline
    // — idempotent). The grouped row's bare-equi law below still applies
    // to heap shapes verbatim.
    if !jheap_shape_guards(run, parse, rtis, quals, &heap)? {
        return Ok(false);
    }
    // Qual discipline (legs X5+X6, both reproduced LIVE by this lane's e2e):
    // EVERY top-level AND term must be an int-family hashjoinable equi
    // clause between two DISTINCT joined rels, with statistics on BOTH key
    // vars. Residual filter quals shift the costing toward sort/merge
    // shapes the walk refuses (a fact-side filter elected a top-level Merge
    // Join with FULL statistics present — X5), and statistics-free keys give
    // the costing default join selectivities with the same merge landing
    // (X6). Bare equi-join grouped shapes over analyzed rels ONLY.
    for &qual in quals {
        let Some(op) = qual.as_op_expr() else {
            return refuse_join("non-equi qual (costing can elect merge/sort shapes)");
        };
        if op.args.len() != 2 {
            return refuse_join("non-equi qual (costing can elect merge/sort shapes)");
        }
        let (a, b) = (op.args.nth(0), op.args.nth(1));
        let hit = |e: Node<'_>| rtis.iter().position(|&rti| key_var(e, rti).is_some());
        let (Some(ia), Some(ib)) = (hit(a), hit(b)) else {
            return refuse_join("non-equi qual (costing can elect merge/sort shapes)");
        };
        if ia == ib {
            return refuse_join("non-equi qual (costing can elect merge/sort shapes)");
        }
        let (Some(va), Some(vb)) = (key_var(a, rtis[ia]), key_var(b, rtis[ib])) else {
            return refuse_join("non-equi qual (costing can elect merge/sort shapes)");
        };
        if !is_int_family(va.vartype)
            || !is_int_family(vb.vartype)
            || !lsyscache::op_hashjoinable(op.opno, va.vartype)?
        {
            return refuse_join("non-hashjoinable qual term");
        }
        if !key_var_estimable(run, a)? || !key_var_estimable(run, b)? {
            return refuse_join("join key without statistics (statistics-free rel)");
        }
    }
    // EC discipline (hostile-review BLOCKING find — see ec_disjoint_equi_edges):
    // pairwise-disjoint two-var ECs only, and exactly rels-1 distinct edges
    // (a TREE — parallel edges plan multi-clause hash joins outside the
    // proven envelope). Either violation keeps Gather.
    let Some(nedges) = ec_disjoint_equi_edges(rtis, quals)? else {
        return refuse_join("equi terms share a join key (EC-derived clause hazard)");
    };
    if nedges != rtis.len().saturating_sub(1) {
        return refuse_join("equi terms exceed a join tree (parallel edges)");
    }
    // Key discipline: every group key a bare int2/4/8 Var on one joined rel
    // (the walk's byval word-equality whitelist is wider — probe narrower).
    // TPCH-CBKEYS (knob-gated): bare text/varchar Vars under the
    // deterministic DEFAULT collation additionally admit (the canonical-
    // bytes key export). BPCHAR refuses BY NAME knob-on (space-insensitive
    // bpchareq — outside the byte-equality envelope, the scan sinks'
    // standing exclusion; TPC-H char(n) keys wait on the tie-law car).
    let mut key_refs: Vec<u32> = Vec::new();
    let mut n_bytes_keys = 0usize;
    for gc_node in &parse.groupClause {
        let Some(gc) = gc_node.as_sort_group_clause() else { return Ok(false) };
        let Some(tle) = tle_by_sortgroupref(parse, gc.tleSortGroupRef) else {
            return Ok(false);
        };
        let Some(v) = rtis.iter().find_map(|&rti| key_var(tle.expr, rti)) else {
            return refuse_join("group key not a bare joined-rel Var");
        };
        if is_int_family(v.vartype) {
            // The bootstrap word-key vocabulary.
        } else if cbkeys_enabled()
            && is_text_family(v.vartype)
            && v.varcollid == DEFAULT_COLLATION_OID
        {
            n_bytes_keys += 1;
        } else {
            if cbkeys_enabled() && v.vartype == BPCHAROID {
                return refuse_join(
                    "bpchar group key (space-insensitive bpchareq outside the canonical-bytes envelope — tie-law car owed)",
                );
            }
            return refuse_join("group key not int-family");
        }
        if !key_var_estimable(run, tle.expr)? {
            return refuse_join("group key without estimable ndistinct (statistics-free rel)");
        }
        key_refs.push(gc.tleSortGroupRef);
    }
    // Emit discipline: bare group-key Vars or whitelisted plain aggregates
    // (PLAIN_FOLD_AGGS — the grouped sink exports the numeric-family int
    // states the scan-grouped GROUPED_SINK_AGGS row refuses). TPCH-NUMJOIN
    // (CAR 2): knob-ON additionally admits plain sum/avg(NUMERIC) over
    // parallel-safe joined-rel arg exprs — the relocated runtime-partial
    // NumericAgg vocabulary the grouped export already carries (the agg-poly
    // matrix row's "export is ready once its probe admits numeric args").
    // Because sort keys are tlist entries, this loop polices the decorated
    // root's ORDER BY keys too (junk entries included).
    let mut n_aggs = 0usize;
    let mut n_numeric = 0usize;
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
        if tle.ressortgroupref != 0 && key_refs.contains(&tle.ressortgroupref) {
            if rtis.iter().all(|&rti| key_var(tle.expr, rti).is_none()) {
                return Ok(false);
            }
            continue;
        }
        if is_whitelisted_agg_nrti(tle.expr, rtis, PLAIN_FOLD_AGGS) {
            n_aggs += 1;
            continue;
        }
        if aggjoin_numeric_enabled() && is_numeric_expr_agg_nrti(run, tle.expr, rtis)? {
            n_aggs += 1;
            n_numeric += 1;
            continue;
        }
        return refuse_join("tlist entry not a whitelisted plain agg");
    }
    if n_aggs == 0 {
        // Zero aggregates = a DISTINCT-shaped emit (numtrans==0 tables have
        // no pergroup space to export) — walk refusal, keep Gather.
        return refuse_join("no aggregates");
    }
    // Group estimate under BOTH the groupby_high boundary and the export
    // cap headroom (input rows ≈ the largest rel — conservative for the
    // fixture shapes the class targets, and the runtime cap is the
    // fail-closed backstop either way).
    let ngroups = if run.root.processed_groupClause.is_empty() {
        1.0
    } else {
        let clauses =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.processed_groupClause);
        let group_exprs =
            types_pathnodes::run::sortgrouplist_exprs(run, &clauses, &parse.targetList);
        crate::selfuncs::estimate_num_groups(run, &group_exprs, max_rows.max(1.0))?
    };
    if ngroups >= groupby_high_floor() || ngroups >= GROUPSINK_NGROUPS_FLOOR {
        return refuse_join("group estimate above the grouped-sink floor");
    }
    // TPCH-DECOROOT hash-election margin: a decorated root makes the
    // sorted-agg serial shape competitive near ngroups≈input (the costing
    // can elect Sort+GroupAggregate the walk refuses — suppress-then-refuse,
    // costing flavor); require the hash election safely dominant.
    if decorated && ngroups * DECOROOT_NGROUPS_MARGIN > max_rows {
        return refuse_join("decorated root without hash-election margin (ngroups too close to input)");
    }
    // Knob-admitted shapes route through the dedicated knob-path finishes
    // (own trace tags, greppable apart from the bootstrap `m5-suppress:`
    // census line; the class row / tsv / drift guards untouched). Heap-fed
    // shapes carry the jheap floor (min 1M) — TPCH-JHEAP owns the tag; the
    // pure decorated/numeric widenings keep the CbHashJoinGroupedAgg floor
    // — the arm underneath is the same grouped sink either way.
    // TPCH-CBKEYS: bytes-keyed shapes route under the cbkeys tag (their
    // own kill's greppable line), composing with the heap/decorated/
    // numeric riders in the label; the binding floor is the strictest of
    // the composed cars (heap's 1M min when heap sides ride along).
    if n_bytes_keys > 0 {
        let mut label = String::from("aggjoin-grouped-cbkeys");
        if !heap.is_empty() {
            label.push_str("-heap");
        }
        if decorated {
            label.push_str("-decorated");
        }
        if n_numeric > 0 {
            label.push_str("+numeric");
        }
        let guard = if heap.is_empty() { cbkeys_guard() } else { jheap_guard() };
        return finish_knob_path(run, "cbkeys", &label, guard, relids[0], ngroups, max_rows, 0.0);
    }
    if !heap.is_empty() {
        let label = match (decorated, n_numeric > 0) {
            (true, true) => "aggjoin-grouped-heap-decorated+numeric",
            (true, false) => "aggjoin-grouped-heap-decorated",
            (false, true) => "aggjoin-grouped-heap+numeric",
            (false, false) => "aggjoin-grouped-heap",
        };
        return finish_knob_path(
            run,
            "jheap",
            label,
            jheap_guard(),
            relids[0],
            ngroups,
            max_rows,
            0.0,
        );
    }
    if decorated || n_numeric > 0 {
        let (tag, label) = match (decorated, n_numeric > 0) {
            (true, true) => ("decoroot", "aggjoin-grouped-decorated+numeric"),
            (true, false) => ("decoroot", "aggjoin-grouped-decorated"),
            _ => ("aggjoinnum", "aggjoin-grouped-numeric"),
        };
        return finish_knob_path(
            run,
            tag,
            label,
            class_guard(CoverClass::CbHashJoinGroupedAgg),
            relids[0],
            ngroups,
            max_rows,
            0.0,
        );
    }
    finish(run, CoverClass::CbHashJoinGroupedAgg, relids[0], ngroups, max_rows, 0.0)
}

/// Step-1 cost-route map: which fitted crossover curve
/// (costsize::runtime_model) prices a CoverClass's economics. `None` =
/// no curve — the FloorGuard rectangle stays the only economics gate
/// (rectangle-retained / never-floored classes; provenance in
/// crates/backend/optimizer/path/costsize/src/runtime-cost-constants.tsv).
fn cover_class_curve(class: CoverClass) -> Option<costsize::runtime_model::RuntimeClass> {
    use costsize::runtime_model::RuntimeClass as Rc;
    match class {
        CoverClass::CbPlainAggFold => Some(Rc::CbPlainAggFold),
        CoverClass::CbGroupedAggIntKeys => Some(Rc::CbGroupedAggIntKeys),
        CoverClass::CbGroupedAggTopN => Some(Rc::CbGroupedAggTopN),
        CoverClass::CbDistinctIntKeys => Some(Rc::CbDistinctIntKeys),
        CoverClass::CbTopnBoundedIntKeys => Some(Rc::CbTopnBoundedIntKeys),
        CoverClass::HeapPlainCountStar => Some(Rc::HeapPlainCountStar),
        CoverClass::HeapCmpFoldPrefix => Some(Rc::HeapCmpFoldPrefix),
        // Shipped guard reuse (same 2M rectangle) -> same curve; own
        // ladder cells owed (TSV curve_reuse rows, GL-COST-2).
        CoverClass::CbHashJoinPlainAgg
        | CoverClass::CbHashJoinMultiBuild
        | CoverClass::CbHashJoinGroupedAgg => Some(Rc::CbHashJoinPlainAgg),
        // PROVISIONAL reuse matching the shipped guard reuse (GL-AGGPOLY-1).
        CoverClass::AggPolyHeapPlain => Some(Rc::HeapCmpFoldPrefix),
        // Curve-fit since the witnessed v2 grid (the v1 record's
        // non-monotonic N profile was contamination — GL-COST-3).
        CoverClass::CbGroupedAggTextKey => Some(Rc::CbGroupedAggTextKey),
        // Footer answers are O(1): never floored, no curve.
        CoverClass::CbMetaFooterAgg => None,
    }
}

/// Matrix consult + optional trace, shared tail.
fn finish(
    run: &mut PlannerRun<'_>,
    class: CoverClass,
    relid: u32,
    ngroups: f64,
    rows: f64,
    pages: f64,
) -> PgResult<bool> {
    use costsize::runtime_model as rtm;
    let covered = class_covered(class);
    if covered {
        let dop = guc_tables::runtime_pool::runtime_dop();
        // M5-5 engagement-floor guard: a covered class outside its measured
        // economics keeps Gather (routes legacy). Traced under its OWN
        // prefix — floor refusals are neither suppressions (M5CENSUS greps
        // `m5-suppress:`) nor arm refusals (`m5-suppress-refuse:`).
        let floor_ok = if size_floors_enabled() {
            let g = class_guard(class);
            rows >= g.min_rows
                && rows <= g.max_rows
                && pages >= g.min_pages
                && (dop >= g.min_dop || rows <= g.low_dop_max_rows)
        } else {
            true
        };
        // Step-1 cost route (runtime-cost-model design §5 step 1): the
        // fitted crossover curve evaluated NEXT TO the rectangle. Default
        // mode is SHADOW — both verdicts traced, floors decide, zero
        // behavior change. PGRUST_M5_COST_ROUTE flips classes to
        // curve-decides after their flip gate. PGRUST_M5_SIZE_FLOORS=0
        // (the rowflip economics-measurement vehicle) disables BOTH
        // economics gates — measurement mode measures raw arm economics.
        let mut suppress = floor_ok;
        let mut decided_by = "floor";
        if !matches!(rtm::cost_route_mode(), rtm::CostRouteMode::Off) {
            if let Some(curve) = cover_class_curve(class) {
                let v = rtm::cost_route_verdict(curve, rows, dop);
                if rtm::cost_route_decides(curve) && size_floors_enabled() {
                    // The rowdrive block-floor ADMISSION MIRROR rides every
                    // mode (m5-5 reading #3; TSV admission_min_pages row).
                    suppress = v.suppress
                        && (class != CoverClass::HeapPlainCountStar
                            || pages >= rtm::HEAP_COUNT_ADMISSION_MIN_PAGES);
                    decided_by = "cost";
                }
                if trace_armed() {
                    eprintln!(
                        "m5-cost-route: class={class:?} curve={curve:?} relid={relid} \
                         rows={rows:.0} pages={pages:.0} ngroups={ngroups:.0} dop={dop} \
                         r_pred={:.3} cost_verdict={} floor_verdict={floor_ok} \
                         decided_by={decided_by}",
                        v.ratio, v.suppress
                    );
                }
            }
        }
        if !suppress {
            if trace_armed() {
                eprintln!(
                    "m5-suppress-floor: class={class:?} relid={relid} rows={rows:.0} \
                     pages={pages:.0} dop={dop} => gather stands"
                );
            }
            return Ok(false);
        }
    }
    if covered && trace_armed() {
        let _ = run; // (run reserved for a future lane_trace surface)
        eprintln!(
            "m5-suppress: engine=runtime class={class:?} relid={relid} \
             ngroups={ngroups:.0} => gather suppressed"
        );
    }
    Ok(covered)
}

/// SE-TEXTDISTINCT knob-path finish (band 86001). Reached ONLY from the
/// `textdistinct_enabled()`-gated admission branches (text-keyed grouped
/// count(DISTINCT), ungrouped count(DISTINCT), reduced-expr-key grouped
/// agg), so the caller has already proven the shape rides an existing
/// runtime arm. Unlike `finish`, this does NOT consult BOOTSTRAP_MATRIX /
/// the tsv — the shape is deliberately NOT a bootstrap class (the tsv rows
/// stay route_to=legacy / probe_key="-", so the drift guards and the DEFAULT
/// census are untouched). Applies the shared provisional floor + its own
/// trace prefix, then suppresses. `label` names the shape in the trace
/// (`m5-suppress-textdistinct:` — greppable apart from the bootstrap
/// `m5-suppress:` line).
fn finish_textdistinct(
    run: &mut PlannerRun<'_>,
    label: &str,
    guard: FloorGuard,
    relid: u32,
    ngroups: f64,
    rows: f64,
    pages: f64,
) -> PgResult<bool> {
    finish_knob_path(run, "textdistinct", label, guard, relid, ngroups, rows, pages)
}

/// The shared knob-path finish body (SE-TEXTDISTINCT precedent, extracted
/// at SE-MKTEXT): floor guard + per-lane trace prefixes derived from `tag`
/// — the floor line `m5-suppress-floor: {tag} label=…` and the suppression
/// line `m5-suppress-{tag}: …` (each lane greppable apart from the
/// bootstrap `m5-suppress:` census line). Every caller is a
/// knob-gated admission branch whose shape rides a proven runtime/serial
/// arm; none are BOOTSTRAP_MATRIX classes.
fn finish_knob_path(
    run: &mut PlannerRun<'_>,
    tag: &str,
    label: &str,
    guard: FloorGuard,
    relid: u32,
    ngroups: f64,
    rows: f64,
    pages: f64,
) -> PgResult<bool> {
    if size_floors_enabled() {
        let dop = guc_tables::runtime_pool::runtime_dop();
        let ok = rows >= guard.min_rows
            && rows <= guard.max_rows
            && pages >= guard.min_pages
            && (dop >= guard.min_dop || rows <= guard.low_dop_max_rows);
        if !ok {
            if trace_armed() {
                eprintln!(
                    "m5-suppress-floor: {tag} label={label} relid={relid} \
                     rows={rows:.0} pages={pages:.0} dop={dop} => gather stands"
                );
            }
            return Ok(false);
        }
    }
    if trace_armed() {
        let _ = run;
        eprintln!(
            "m5-suppress-{tag}: engine=runtime label={label} relid={relid} \
             ngroups={ngroups:.0} => gather suppressed"
        );
    }
    Ok(true)
}

/// SE-MKTEXT knob-path finish (Lane-3 two-key text car). Reached ONLY from
/// the `multikey_text_enabled()`-gated admission branches (two-key
/// int+text / text+text grouped agg: the text+text census, the bare-LIMIT
/// freeze composition, the family group-estimate ceiling), so the caller
/// has already proven the shape rides the runtime agg sink's existing Mk /
/// canonical-bytes / freeze machinery. Like `finish_textdistinct`, this
/// does NOT consult BOOTSTRAP_MATRIX / the tsv — deliberately not a
/// bootstrap class (route_to/probe_key stay legacy/"-"; drift guards and
/// the DEFAULT census untouched). Applies the provisional floor + its own
/// trace prefix (`m5-suppress-mktext:` — greppable apart from the
/// bootstrap `m5-suppress:` and the textdistinct lines), then suppresses.
fn finish_multikey_text(
    run: &mut PlannerRun<'_>,
    label: &str,
    guard: FloorGuard,
    relid: u32,
    ngroups: f64,
    rows: f64,
    pages: f64,
) -> PgResult<bool> {
    finish_knob_path(run, "mktext", label, guard, relid, ngroups, rows, pages)
}

// ===========================================================================
// SE-T2AGG (night/tier2-agg-cars): three tier-2 coverage cars, ONE fenced
// block (sibling probe lanes add their own blocks — keep this region
// contiguous; the classify_covered call sites are one-liner delegations).
//
//   CAR A  distinct-plain-shape (`classify_distinct_plain`): plain
//          `SELECT DISTINCT col` plans HashAggregate (AGG_HASHED, zero
//          aggregates), which no runtime sink admitted — the m5-integration
//          r2 suppress-then-refuse false positive re-keyed the bootstrap
//          class away and left the shape UNKEYED (matrix row
//          distinct-plain-shape). The runtime PLAIN-distinct sink's kernels
//          already collect int + canonical-bytes text distinct VALUES
//          (plainpd.rs — the distinct-text-date-args admission note); the
//          new executor sub-arm (runtime_plaindistinct.rs
//          `try_own_plain_selectdistinct_runtime`) reuses that pipeline and
//          adopts the merged set as emit rows. Knob:
//          `PGRUST_LANE_V2_DISTINCT_PLAINSHAPE` (default OFF; ON iff `1|on`),
//          same spelling read by the executor sub-arm (knob-coherence law),
//          plus the engine-car kill `PGRUST_RUNTIME_PLAINDISTINCT` mirrored
//          here (a keyed shape whose arm is disarmed would land on serial).
//          COMPOSITION NOTE (assembly): night/subquery-admission lands the
//          SERIAL half of the same gap (zero-transition grouping in the
//          lane, `PGRUST_LANE_V2_GROUPONLY`) — the halves compose: this
//          probe only suppresses the Gather; a runtime sub-arm refusal
//          falls to whatever serial arm owns the shape (theirs once landed
//          — strictly better than the per-row breaker), never a
//          conflicting route.
//
//   CAR B  gap:agg-min-text (`grouped_str_minmax_arg`): cb q22-class text-key
//          grouped agg with MIN(URL)/MIN(Title) — GROUPED_SINK_AGGS is
//          int-only and the runtime agg sink's spec derivation
//          (sink_resolve_combines) refused text min/max. The sink gains a
//          knob-gated VarlenaMinMax vocabulary entry (nodeagg sink.rs;
//          canonical-bytes survivor, memcmp-tier collations only, the
//          merge.rs VarlenaMinMax kernel mirrored); this probe admits
//          min/max(text) PASSENGERS under the SAME spelling
//          (`PGRUST_LANE_V2_AGG_STRMINMAX`, default OFF; ON iff `1|on`).
//          Fail-closed: default-collation (OID 100) bare text Vars only —
//          the only collation the probe recognizes as deterministic; the
//          engine's `str_collation_safe` is the stricter runtime twin.
//
//   CAR C  gap:agg-orderby-nolimit (`full_sort` composition): cb q8-class
//          grouped agg + `ORDER BY count(*)` with NO LIMIT — the topn arm's
//          `limitCount.is_some()` binding left the shape on the final
//          `Ok(false)`. The suppressed serial plan is `Sort <- HashAgg <-
//          SeqScan` (or `Sort <- Agg(SORTED) <- Sort <- SeqScan` for the
//          count(DISTINCT) class): the runtime sinks already engage with the
//          Agg below a Sort root (the q36 decorated-root precedent), the
//          unbounded `sink_topn_arm` declines into the plain full drain, and
//          the REAL serial Sort above orders the finalized groups — the
//          decorated-root pattern WITHOUT the bound; no executor change.
//          Knob: `PGRUST_LANE_V2_AGG_SORT_NOLIMIT` (DEFAULT ON since t36
//          flips2, GL-T2B; `=0|off` kills). NOTE for assembly: the
//          decorated-root generalization lane's CAR 1
//          generalizes root decoration — this is the agg-specific narrow
//          case behind its own switch; unify at merge if theirs subsumes.
//
// All three are knob-path finishes (finish_knob_path) — NOT BOOTSTRAP_MATRIX
// classes, so the drift guards are untouched; a thrown kill (or the two
// still-gated cars' default OFF) takes the identical pre-car refusal
// byte-for-byte. t36 flips2 dispositions per the GL-T2 letters: CARs A
// (GL-T2C) + C (GL-T2B) FLIPPED ON; CAR B KEEP-GATED (GL-T2A: the
// suppress-then-serial 7.6x containment violation).
// ===========================================================================

/// The STILL-GATED tier-2 cars' shared default-OFF spelling rule, factored
/// pure for exhaustive unit tests (the K1-latemat / scanpass idiom): ON iff
/// the value is exactly `1` or `on`; every other spelling (incl. unset,
/// `0`, `off`, typos) fails safe to OFF. Since t36 flips2 this covers ONLY
/// CAR B (STRMINMAX, KEEP-GATED per its letter); the flipped CARs A + C
/// ride `tier2_car_kill_spelling_on`.
fn tier2_car_spelling_on(v: Option<&str>) -> bool {
    matches!(v, Some("1") | Some("on"))
}

/// The FLIPPED tier-2 cars' default-ON kill spelling (t36 flips2, the
/// flipped-kill idiom): OFF iff exactly `0` or `off`; unset and every other
/// spelling stay ON.
fn tier2_car_kill_spelling_on(v: Option<&str>) -> bool {
    !matches!(v, Some("0") | Some("off"))
}

/// CAR A probe knob (`PGRUST_LANE_V2_DISTINCT_PLAINSHAPE`): DEFAULT ON
/// since t36 flips2 (`=0|off` kills). FLIP EVIDENCE (GL-T2C
/// FLIP-RECOMMENDED, 2026-07-21, tier2 campaign @ 7d8aa9a2b): bare SELECT
/// DISTINCT int 0.164s (6.1x win) / text 0.195s (5.1x win) at 10M/2M, md5
/// parity every leg, OFF arm inert (0 engagements), GROUP BY control flat,
/// wrapped/hasAggs forms correctly shape-refused; measured at dop 12 with
/// floors disabled symmetrically — the production floor guard (min_dop 12
/// / low-dop<=3M) bounds exposure. SAME spelling as the executor sub-arm
/// (runtime_plaindistinct `selectdistinct_enabled`) — both sites flip
/// together (knob-coherence law).
fn distinct_plainshape_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        tier2_car_kill_spelling_on(
            std::env::var("PGRUST_LANE_V2_DISTINCT_PLAINSHAPE").as_deref().ok(),
        )
    })
}

/// CAR A engine-kill coherence (the mk_text_agg_cars_live precedent): the
/// runtime plain-distinct sink family's own kill
/// (`PGRUST_RUNTIME_PLAINDISTINCT=0`, default ON — runtime_plaindistinct.rs
/// spelling verbatim) must be live for the keyed shape, or the suppression
/// would land on the serial hash-agg breaker (risk P1's suppress-then-
/// unarmed direction).
fn plaindistinct_engine_live() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_RUNTIME_PLAINDISTINCT").as_deref() != Ok("0"))
}

/// PROVISIONAL floor for the CAR A knob path: the shared distinct-family
/// economics (the textdistinct guard verbatim — same sink family, same
/// engagement shape). The fleet letter owns re-measuring.
fn distinct_plainshape_guard() -> FloorGuard {
    FloorGuard { min_dop: 12, low_dop_max_rows: 3_000_000.0, ..NO_GUARD }
}

/// CAR B knob (`PGRUST_LANE_V2_AGG_STRMINMAX`, default OFF — **KEEP-GATED
/// per its letter; do NOT flip**). LETTER OF RECORD (GL-T2A KEEP GATED,
/// 2026-07-21, tier2 campaign @ 7d8aa9a2b): the target qualed text-minmax
/// top-n shape is a
/// suppress-then-serial containment violation — `m5-suppress-strminmax ...
/// gather suppressed` then `runtime-sort: refused (full-sort shape spec)`,
/// zero runtime engagements, target hot 0.022 -> 0.234s (7.6x damped; 8.5x
/// mt16), official 43q geomean dragged +3.3%/+5.9%. Parity clean.
/// RE-LETTER PRECONDITIONS: narrow the probe (mirror the executor shape
/// gates into classify) or thread qualed topn through the runtime sort
/// arm; add a QUALED text-minmax-class e2e row (the CAR-B e2e shapes are unqualed
/// — the coverage hole). SAME spelling
/// as the executor half (nodeagg sink.rs `sink_strminmax_enabled` — the
/// resolve-combines / emit-plan vocabulary widening): both read sites flip
/// together, the AGG_POLY knob-coherence law.
fn agg_strminmax_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        tier2_car_spelling_on(std::env::var("PGRUST_LANE_V2_AGG_STRMINMAX").as_deref().ok())
    })
}

/// CAR C knob (`PGRUST_LANE_V2_AGG_SORT_NOLIMIT`): DEFAULT ON since t36
/// flips2 (`=0|off` kills). Planner-only (suppression-widening; the
/// executor composition already exists). FLIP EVIDENCE (GL-T2B
/// FLIP-RECOMMENDED, 2026-07-21, tier2 campaign @ 7d8aa9a2b, CB 10M
/// unforced + mt16): the grouped-agg-sort-no-limit target engages 3/3 in
/// BOTH postures (planner suppress +
/// runtime-agg engaged dop=16, groups=8), byte-identical output, wall flat
/// (the win is retiring the uncovered gap:agg-orderby-nolimit row), all 41
/// guard queries flat; attribution clean via the per-car suppress labels.
fn agg_sort_nolimit_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        tier2_car_kill_spelling_on(
            std::env::var("PGRUST_LANE_V2_AGG_SORT_NOLIMIT").as_deref().ok(),
        )
    })
}

/// min(text) 2145 / max(text) 2129 — pg_proc OIDs of record (vendored REL
/// 18.3 pg_proc.dat; transfns text_smaller 459 / text_larger 458).
const F_MIN_TEXT: u32 = 2145;
const F_MAX_TEXT: u32 = 2129;

/// CAR B shape law: a bare min/max(text) Aggref over a default-collation
/// text/varchar Var on the scanned rel — `Some(arg attno)` when admitted
/// (the caller additionally refuses args that ARE group-key columns: the
/// sink's stale-cell rule keeps the dict/intern key column out of the
/// fold's lane reads). Fail-closed on collation weirdness: BOTH the Var's
/// collation and the Aggref's inputcollid must be the deterministic default
/// (OID 100) — the only collation the probe recognizes (the
/// is_count_distinct_any contract); the runtime sink's `str_collation_safe`
/// gate is the stricter twin (memcmp tier), so probe ⊂ walk holds. bpchar
/// never reaches here (arg type discipline).
fn grouped_str_minmax_arg(expr: Node<'_>, rti: usize) -> Option<i16> {
    let agg = expr.as_aggref()?;
    if !matches!(agg.aggfnoid, F_MIN_TEXT | F_MAX_TEXT) {
        return None;
    }
    if agg.inputcollid != DEFAULT_COLLATION_OID {
        return None;
    }
    if !aggref_plain_typed(agg, rti, is_text_family) {
        return None;
    }
    // The bare-Var arg (proven by aggref_plain_typed) must itself carry the
    // deterministic default collation.
    let arg_tle = agg.args.nth(0).as_target_entry()?;
    let v = key_var(arg_tle.expr, rti)?;
    (v.varcollid == DEFAULT_COLLATION_OID).then_some(v.varattno)
}

/// CAR A classifier: plain `SELECT DISTINCT <col>` over one pgrcolumnar rel
/// — the AGG_HASHED zero-aggregate HashAggregate shape. `None` = shape miss
/// or knob off: the caller takes the historical keep-Gather refusal
/// byte-for-byte. NARROW (v1, fail-closed): no quals (the sink stages the
/// distinct column as scan col 0 — the plain count(DISTINCT) discipline),
/// no sort/limit/offset, EXACTLY one distinct column = the single tlist
/// entry, a bare int-family Var or default-collation text/varchar Var.
#[allow(clippy::too_many_arguments)]
fn classify_distinct_plain<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &Query<'mcx>,
    rti: usize,
    relid: u32,
    rel_id: types_pathnodes::RelId,
    is_cb: bool,
    has_quals: bool,
    rel_rows: f64,
    rel_pages: f64,
) -> PgResult<Option<bool>> {
    if !distinct_plainshape_enabled() || !plaindistinct_engine_live() {
        return Ok(None);
    }
    if !is_cb
        || has_quals
        || parse.hasAggs
        || !parse.groupClause.is_nil()
        || !parse.sortClause.is_nil()
        || parse.limitCount.is_some()
        || parse.limitOffset.is_some()
    {
        return Ok(None);
    }
    if parse.distinctClause.len() != 1 || parse.targetList.len() != 1 {
        return Ok(None);
    }
    let Some(dc) = parse.distinctClause.nth(0).as_sort_group_clause() else {
        return Ok(None);
    };
    let Some(tle) = parse.targetList.nth(0).as_target_entry() else {
        return Ok(None);
    };
    // The one distinct clause must name the one tlist entry.
    if tle.ressortgroupref == 0 || tle.ressortgroupref != dc.tleSortGroupRef {
        return Ok(None);
    }
    let Some(v) = key_var(tle.expr, rti) else { return Ok(None) };
    let type_ok = is_int_family(v.vartype)
        || (is_text_family(v.vartype) && v.varcollid == DEFAULT_COLLATION_OID);
    if !type_ok {
        return Ok(None);
    }
    // NDV estimate for the floor + the groupby_high hold (§10): the leader
    // emit materializes every distinct value, so the radix-exchange hold's
    // boundary applies unchanged.
    let input_rows = run.root.rel(rel_id).rows.max(1.0);
    let expr_id = run.intern_expr(tle.expr);
    let ngroups = crate::selfuncs::estimate_num_groups(run, &[(expr_id, tle.expr)], input_rows)?;
    if ngroups >= groupby_high_floor() {
        return Ok(None);
    }
    Ok(Some(finish_knob_path(
        run,
        "distinctplain",
        "plain-select-distinct",
        distinct_plainshape_guard(),
        relid,
        ngroups,
        rel_rows,
        rel_pages,
    )?))
}

// --------------------------- end SE-T2AGG block ----------------------------

// ---------------------------------------------------------------------------
// Expression helpers.
// ---------------------------------------------------------------------------

/// A bare Var on the scanned rel, user column, current level.
fn key_var<'mcx>(expr: Node<'mcx>, rti: usize) -> Option<&'mcx Var<'mcx>> {
    let v = expr.as_var()?;
    (v.varno as usize == rti && v.varattno > 0 && v.varlevelsup == 0).then_some(v)
}

/// SE-CONSTKEY: a group-key Const the knob admits — NON-NULL, INT-FAMILY
/// (byval word keys; the q35 shape's `1`). Null consts (NULL group-key
/// semantics), text/varlena consts (canonical-bytes derivation untested for
/// consts), and every other type fail closed.
fn is_admissible_const_key(expr: Node<'_>) -> bool {
    let Some(c) = expr.as_const() else { return false };
    !c.constisnull && is_int_family(c.consttype)
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

// ---------------------------------------------------------------------------
// Meta-over-Gather (CbMetaFooterAgg) admission — mirrors the lanefold
// classify_meta/classify_arg structural walk at parse-tree altitude.
// ---------------------------------------------------------------------------

// Affine int op funcids (pg_proc; lanefold classify_arg's table). Division
// forms are deliberately absent: classify_meta refuses divk != 1.
const F_INT4MUL_FN: u32 = 141;
const F_INT24MUL_FN: u32 = 170;
const F_INT42MUL_FN: u32 = 171;
const F_INT4PL_FN: u32 = 177;
const F_INT24PL_FN: u32 = 178;
const F_INT42PL_FN: u32 = 179;
const F_INT4MI_FN: u32 = 181;
const F_INT24MI_FN: u32 = 182;
const F_INT42MI_FN: u32 = 183;

/// An int4-result affine transform of one scanned-rel Var — `v ± k`,
/// `v * k`, `k ± v` — exactly the lanefold classify_arg OpExpr admission
/// with divk == 1. Refuses when the walk would (empty safe interval), via
/// the SAME lanefold guard math, so probe ⊂ walk holds coefficient-exactly.
fn meta_affine_int4_arg(expr: Node<'_>, rti: usize) -> bool {
    let Some(op) = expr.as_op_expr() else { return false };
    if op.opretset || op.args.len() != 2 {
        return false;
    }
    let (a, b) = (op.args.nth(0), op.args.nth(1));
    type Mk = fn(i64) -> (i64, i64);
    let (var, konst, vartype, mk): (Node<'_>, Node<'_>, u32, Mk) = match op.opfuncid {
        F_INT24PL_FN => (a, b, INT2OID, |k| (k, 1)),
        F_INT42PL_FN => (b, a, INT2OID, |k| (k, 1)),
        F_INT24MI_FN => (a, b, INT2OID, |k| (-k, 1)),
        F_INT42MI_FN => (b, a, INT2OID, |k| (k, -1)),
        F_INT24MUL_FN => (a, b, INT2OID, |k| (0, k)),
        F_INT42MUL_FN => (b, a, INT2OID, |k| (0, k)),
        F_INT4PL_FN => (a, b, INT4OID, |k| (k, 1)),
        F_INT4MI_FN => (a, b, INT4OID, |k| (-k, 1)),
        F_INT4MUL_FN => (a, b, INT4OID, |k| (0, k)),
        _ => return false,
    };
    if !is_covered_key_var(var, rti, |t| t == vartype) {
        return false;
    }
    let Some(c) = konst.as_const() else { return false };
    if c.constisnull || c.consttype != INT4OID {
        return false;
    }
    let (addend, mulk) = mk(c.constvalue.as_i32() as i64);
    let width =
        if vartype == INT2OID { ::lanefold::LaneWidth::I16 } else { ::lanefold::LaneWidth::I32 };
    if ::lanefold::type_proof(width, addend, mulk, 1) {
        return true;
    }
    let (lo, hi) = ::lanefold::safe_interval(addend, mulk, 1);
    lo <= hi
}

/// One footer-answerable Aggref (the classify_meta admission at parse
/// altitude): count(*) / count(bare Var); min/max over bare int-family
/// Vars (transforms are monotone but not identity — walk refusal,
/// mirrored); sum/avg(int4) over a bare int4 Var or an affine int4-result
/// transform; sum/avg(int2) and sum/avg(int8) over bare Vars of their type
/// (classify_arg admits OpExprs for INT4-expected args only).
fn is_meta_footer_agg(expr: Node<'_>, rti: usize) -> bool {
    let Some(agg) = expr.as_aggref() else { return false };
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
    let one_arg = |ok: &dyn Fn(Node<'_>) -> bool| -> bool {
        agg.args.len() == 1
            && agg.args.nth(0).as_target_entry().is_some_and(|tle| ok(tle.expr))
    };
    match agg.aggfnoid {
        F_COUNT_STAR => agg.args.is_nil(),
        // count(col): any bare scanned-rel Var (CountAny reads the isnull
        // lane only; footers carry per-column null counts).
        F_COUNT_ANY => one_arg(&|e| key_var(e, rti).is_some()),
        F_MAX_INT8 | F_MAX_INT4 | F_MAX_INT2 | F_MIN_INT8 | F_MIN_INT4 | F_MIN_INT2 => {
            one_arg(&|e| is_covered_key_var(e, rti, is_int_family))
        }
        F_SUM_INT2 | F_AVG_INT2 => one_arg(&|e| is_covered_key_var(e, rti, |t| t == INT2OID)),
        F_SUM_INT4 | F_AVG_INT4 => one_arg(&|e| {
            is_covered_key_var(e, rti, |t| t == INT4OID) || meta_affine_int4_arg(e, rti)
        }),
        F_SUM_INT8 | F_AVG_INT8 => one_arg(&|e| is_covered_key_var(e, rti, |t| t == INT8OID)),
        _ => false,
    }
}

/// Every tlist entry is a footer-answerable Aggref (all-or-nothing, the
/// classify_meta contract), and at least one entry exists.
fn tlist_all_meta_footer_aggs(parse: &Query<'_>, rti: usize) -> bool {
    let mut n = 0usize;
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return false };
        if !is_meta_footer_agg(tle.expr, rti) {
            return false;
        }
        n += 1;
    }
    n > 0
}

/// A `count(DISTINCT <int-family Var>)` on the scanned rel: normal kind,
/// one arg, one-entry aggdistinct, no order/filter/variadic decoration —
/// the runtime distinct sink's aggregate (CbDistinctIntKeys).
fn is_count_distinct_int(expr: Node<'_>, rti: usize) -> bool {
    let Some(agg) = expr.as_aggref() else { return false };
    if agg.aggfnoid != F_COUNT_ANY
        || agg.agglevelsup != 0
        || agg.aggkind != AGGKIND_NORMAL
        || agg.aggvariadic
        || !agg.aggorder.is_nil()
        || agg.aggdistinct.len() != 1
        || agg.aggfilter.is_some()
        || !agg.aggdirectargs.is_nil()
        || agg.args.len() != 1
    {
        return false;
    }
    let Some(arg_tle) = agg.args.nth(0).as_target_entry() else { return false };
    is_covered_key_var(arg_tle.expr, rti, is_int_family)
}

/// SE-TEXTDISTINCT (band 86001): `count(DISTINCT <bare Var>)` whose arg is
/// int-family OR text/varchar under the deterministic DEFAULT collation —
/// the plain-distinct SINK's exact-set vocabulary (runtime_plaindistinct.rs:
/// int lanes + canonical-bytes text keys, `distinct_set_kind` gated on a
/// deterministic collation). Same structural decoration gates as
/// `is_count_distinct_int`; only the arg-type predicate widens. Text keys
/// require the default collation (100) — the ONLY deterministic collation the
/// probe recognizes at parse altitude without a catalog lookup (the sink's
/// `get_collation_isdeterministic` gate is the walk's stricter twin; probe ⊂
/// walk holds because default-collation IS deterministic).
fn is_count_distinct_any(expr: Node<'_>, rti: usize) -> bool {
    let Some(agg) = expr.as_aggref() else { return false };
    if agg.aggfnoid != F_COUNT_ANY
        || agg.agglevelsup != 0
        || agg.aggkind != AGGKIND_NORMAL
        || agg.aggvariadic
        || !agg.aggorder.is_nil()
        || agg.aggdistinct.len() != 1
        || agg.aggfilter.is_some()
        || !agg.aggdirectargs.is_nil()
        || agg.args.len() != 1
    {
        return false;
    }
    let Some(arg_tle) = agg.args.nth(0).as_target_entry() else { return false };
    let Some(v) = key_var(arg_tle.expr, rti) else { return false };
    is_int_family(v.vartype)
        || (is_text_family(v.vartype) && v.varcollid == DEFAULT_COLLATION_OID)
}

/// SE-TEXTDISTINCT (band 86001) q36 reduced-expr-key affine check: `expr`
/// is `base ± <int4 Const>` (or `<int4 Const> ± base`) where `base` is the
/// representative int4 Var at `base_attno` on the scanned rel. Mirrors
/// `decide_reduced`'s per-key admission (exprkey.rs: Add2/Sub2/… over the
/// ONE representative Var, non-null Const, same width) STRICTLY NARROWER —
/// int4-only (decide_reduced admits any uniform int width; the probe keeps
/// to int4, the q36 shape). Mul/Div refuse (decide_reduced refuses them too).
fn reduced_affine_of_var(expr: Node<'_>, rti: usize, base_attno: i16) -> bool {
    let Some(op) = expr.as_op_expr() else { return false };
    if op.opretset || op.args.len() != 2 {
        return false;
    }
    // int4 ± int4 only (F_INT4PL_FN / F_INT4MI_FN); the base Var may sit on
    // either side for '+', but only the left for '-' (k - v is v negated,
    // which decide_reduced does not key).
    let (var_node, konst_node) = match op.opfuncid {
        F_INT4PL_FN => {
            // base + k  OR  k + base
            let (a, b) = (op.args.nth(0), op.args.nth(1));
            if key_var(a, rti).is_some_and(|v| v.varattno == base_attno && v.vartype == INT4OID) {
                (a, b)
            } else {
                (b, a)
            }
        }
        F_INT4MI_FN => (op.args.nth(0), op.args.nth(1)),
        _ => return false,
    };
    let Some(v) = key_var(var_node, rti) else { return false };
    if v.varattno != base_attno || v.vartype != INT4OID {
        return false;
    }
    let Some(c) = konst_node.as_const() else { return false };
    !c.constisnull && c.consttype == INT4OID
}

/// SE-TEXTDISTINCT (band 86001) q36 reduced-expr-key recognizer. Keys the
/// census `gap:agg-expr-keys` shape (cb q36):
///   SELECT ClientIP, ClientIP-1, ClientIP-2, ClientIP-3, count(*), ...
///   FROM hits GROUP BY 1,2,3,4 ORDER BY count DESC LIMIT n
/// — a single-rel grouped agg whose keys are ONE bare int4 Var plus affine
/// ±Const transforms of THAT Var (2..N keys, exactly one bare Var), with a
/// fold-admissible agg tlist and an optional `ORDER BY <agg> LIMIT` top-N.
/// The exprkey Reduced arm (exprkey.rs `decide_reduced`, default-ON
/// PGRUST_LANE_V2_REDKEY) owns the suppressed serial `[Limit<-Sort<-]
/// HashAgg<-SeqScan` plan and emits full grouped output (the serial
/// Sort+Limit consumes it) — engagement confirmed, no per-row breaker
/// fallback for the count/sum/avg-int fold set.
///
/// Returns `Some(verdict)` when the shape MATCHES (`verdict` = suppress, or
/// false when floored by groupby_high), `None` to fall through to the
/// bare-Var key discipline. Probe ⊂ walk (STRICTLY NARROWER than
/// decide_reduced): int4-only keys (decide_reduced admits any uniform int
/// width), affine ±Const only (Mul/Div refuse). CAVEATS of record (fleet
/// win owed, GL-TEXTDIST-3): decide_reduced refuses to the per-row breaker
/// if a fold column classifies as a residual transition — avg(int) SHOULD
/// fold via lanefold (the CbPlainAggFold avg path), but the at-scale
/// confirmation is fleet work; the arm's admission-time canonical-domain
/// check (empty => refuse) is non-empty for int4 ±int4 by construction.
fn classify_reduced_exprkey<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &Query<'mcx>,
    rti: usize,
    relid: u32,
    rel_id: types_pathnodes::RelId,
    rel_rows: f64,
    rel_pages: f64,
) -> PgResult<Option<bool>> {
    if parse.groupClause.len() < 2 {
        return Ok(None);
    }
    // Group keys: find the single bare int4 Var representative; every other
    // key must be an affine ±Const of it.
    let mut key_refs: Vec<u32> = Vec::new();
    let mut key_exprs: Vec<Node<'mcx>> = Vec::new();
    let mut base_attno: Option<i16> = None;
    let mut n_bare = 0usize;
    for gc_node in &parse.groupClause {
        let Some(gc) = gc_node.as_sort_group_clause() else { return Ok(None) };
        let Some(tle) = tle_by_sortgroupref(parse, gc.tleSortGroupRef) else {
            return Ok(None);
        };
        if let Some(v) = key_var(tle.expr, rti) {
            if v.vartype != INT4OID {
                return Ok(None); // a bare Var of another type is not this shape
            }
            n_bare += 1;
            base_attno = Some(v.varattno);
        }
        key_refs.push(gc.tleSortGroupRef);
        key_exprs.push(tle.expr);
    }
    if n_bare != 1 {
        return Ok(None);
    }
    let base_attno = base_attno.unwrap();
    for e in &key_exprs {
        if key_var(*e, rti).is_some() {
            continue; // the one representative
        }
        if !reduced_affine_of_var(*e, rti, base_attno) {
            return Ok(None);
        }
    }
    // Emit discipline: each tlist entry is a group-key expr (matched by
    // ressortgroupref) or a fold-admissible agg. PLAIN_FOLD_AGGS (WIDER than
    // GROUPED_SINK_AGGS: includes avg/sum poly-state int aggs) because the
    // exprkey fold hosts them via lanefold — q36 has avg(ResolutionWidth).
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(None) };
        if tle.ressortgroupref != 0 && key_refs.contains(&tle.ressortgroupref) {
            continue;
        }
        if !is_whitelisted_agg(tle.expr, rti, PLAIN_FOLD_AGGS) {
            return Ok(None);
        }
    }
    // Optional top-N: ORDER BY <fold-whitelisted agg> LIMIT, no OFFSET — or a
    // plain grouped emit (no sort/limit). Anything else is not this shape.
    if !parse.sortClause.is_nil() || parse.limitCount.is_some() {
        if parse.sortClause.len() != 1
            || parse.limitCount.is_none()
            || parse.limitOffset.is_some()
        {
            return Ok(None);
        }
        let Some(sc) = parse.sortClause.nth(0).as_sort_group_clause() else {
            return Ok(None);
        };
        let Some(tle) = tle_by_sortgroupref(parse, sc.tleSortGroupRef) else {
            return Ok(None);
        };
        if !is_whitelisted_agg(tle.expr, rti, PLAIN_FOLD_AGGS) {
            return Ok(None);
        }
    }
    // groupby_high hold (shared floor): matched-shape-but-floored keeps
    // Gather (Ok(Some(false))), same as the bare-Var grouped path.
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
        return Ok(Some(false));
    }
    Ok(Some(finish_textdistinct(
        run,
        "reduced-exprkey-grouped-agg",
        textdistinct_guard(),
        relid,
        ngroups,
        rel_rows,
        rel_pages,
    )?))
}

/// `extract(text, timestamp)` — pg_proc OID of record (vendored REL 18.3,
/// adt_timestamp builtins), NUMERIC result — the exprkey Multi decide's
/// computed-key class ("the extract()-class census result type",
/// exprkey.rs decide_exprkey_mk). `date_part` (float8) and
/// `date_trunc` (timestamp result) deliberately NOT keyed: the Multi walk
/// requires a NUMERIC computed key, so keying them would be
/// suppress-then-refuse; single-key date_trunc is the TsTrunc class whose
/// census composition (cb q43: ORDER BY the key + OFFSET) is the
/// topn-offset row's territory.
const F_EXTRACT_TIMESTAMP: u32 = 6202;
const TIMESTAMPOID: u32 = 1114;

/// SE-EXTRACTKEY: the computed group key `extract(<non-null Const field>
/// FROM <bare TIMESTAMP Var on the scanned rel>)` — cb q19's
/// `extract(minute FROM EventTime)`. Strictly narrower than the walk
/// (`compile_value_chain` admits any IMMUTABLE strict builtin chain): one
/// call, the exact extract-over-timestamp OID. The field spelling is NOT
/// whitelisted — the engine runs the real builtin, and an invalid field
/// errors identically on the suppressed serial plan (byte-identical
/// behavior either way).
fn is_extract_ts_key(expr: Node<'_>, rti: usize) -> bool {
    let Some(f) = expr.as_func_expr() else { return false };
    if f.funcid != F_EXTRACT_TIMESTAMP || f.funcretset || f.args.len() != 2 {
        return false;
    }
    let Some(c) = f.args.nth(0).as_const() else { return false };
    if c.constisnull {
        return false;
    }
    is_covered_key_var(f.args.nth(1), rti, |t| t == TIMESTAMPOID)
}

/// SE-EXTRACTKEY packed-image width preview (pure, unit-tested): mirrors
/// the exprkey Multi walk's 16-byte negotiation (decide_exprkey_mk /
/// mk_admit_n): Σ int-key widths + 4 per text key + the computed NUMERIC
/// key at 8 bytes, shrinking the numeric to 4 when the image exceeds 16.
/// A shape that fits neither way must NOT be keyed (walk refusal —
/// suppress-then-refuse). The q19 image: int8 + text4 + numeric8 = 20 →
/// shrink → 16 (fits exactly).
fn extract_key_image_fits(int_widths_sum: usize, n_text: usize) -> bool {
    let fixed = int_widths_sum + n_text * 4;
    fixed + 8 <= 16 || fixed + 4 <= 16
}

/// SE-EXTRACTKEY (cb q19 class) recognizer: a single-cbstore-rel grouped
/// agg whose keys are bare int-family Vars, at most ONE bare
/// default-collation text Var (the Multi walk caps TextRaw components at
/// one — dict/intern lane), and EXACTLY ONE `extract(field FROM ts)`
/// computed key (`is_extract_ts_key`); fold-admissible aggs
/// (PLAIN_FOLD_AGGS — the exprkey fold hosts them via lanefold, the
/// classify_reduced_exprkey precedent); optional `ORDER BY <agg> LIMIT`
/// top-N, no OFFSET. The SERIAL-lane exprkey Multi arm owns the suppressed
/// plan (decide_exprkey_mk — projected scan, packed int/numeric/intern
/// image, ts-extract fast kernel); suppression-only, no engine work.
///
/// Returns `Some(verdict)` when the shape MATCHES (suppress, or false when
/// floored), `None` to fall through to the bare-Var key discipline.
/// Fail-closed refusals: two computed keys, two text keys, non-extract
/// exprs (date_part/date_trunc — see the OID note), images past the
/// 16-byte negotiation, count(DISTINCT), OFFSET, groupby_high (the floor
/// lane owns raising it).
fn classify_extract_exprkey<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &Query<'mcx>,
    rti: usize,
    relid: u32,
    rel_id: types_pathnodes::RelId,
    rel_rows: f64,
    rel_pages: f64,
) -> PgResult<Option<bool>> {
    if parse.groupClause.len() < 2 {
        return Ok(None);
    }
    let mut key_refs: Vec<u32> = Vec::new();
    let mut n_extract = 0usize;
    let mut n_text = 0usize;
    let mut int_widths = 0usize;
    for gc_node in &parse.groupClause {
        let Some(gc) = gc_node.as_sort_group_clause() else { return Ok(None) };
        let Some(tle) = tle_by_sortgroupref(parse, gc.tleSortGroupRef) else {
            return Ok(None);
        };
        if let Some(v) = key_var(tle.expr, rti) {
            if is_int_family(v.vartype) {
                int_widths += match v.vartype {
                    INT2OID => 2,
                    INT4OID => 4,
                    _ => 8,
                };
            } else if is_text_family(v.vartype) && v.varcollid == DEFAULT_COLLATION_OID {
                n_text += 1;
                if n_text > 1 {
                    return Ok(None); // the Multi walk caps TextRaw at one
                }
            } else {
                return Ok(None);
            }
        } else if is_extract_ts_key(tle.expr, rti) {
            n_extract += 1;
            if n_extract > 1 {
                return Ok(None); // one computed chain key (walk shape)
            }
        } else {
            return Ok(None);
        }
        key_refs.push(gc.tleSortGroupRef);
    }
    if n_extract != 1 || !extract_key_image_fits(int_widths, n_text) {
        return Ok(None);
    }
    // Emit discipline: key exprs by sortgroupref, or fold-admissible aggs
    // (count(DISTINCT) is not in the fold vocabulary — is_whitelisted_agg
    // refuses aggdistinct decoration, fail-closed).
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(None) };
        if tle.ressortgroupref != 0 && key_refs.contains(&tle.ressortgroupref) {
            continue;
        }
        if !is_whitelisted_agg(tle.expr, rti, PLAIN_FOLD_AGGS) {
            return Ok(None);
        }
    }
    // Optional top-N: ORDER BY <fold-whitelisted agg> LIMIT, no OFFSET — or
    // a plain grouped emit (classify_reduced_exprkey's block verbatim).
    if !parse.sortClause.is_nil() || parse.limitCount.is_some() {
        if parse.sortClause.len() != 1
            || parse.limitCount.is_none()
            || parse.limitOffset.is_some()
        {
            return Ok(None);
        }
        let Some(sc) = parse.sortClause.nth(0).as_sort_group_clause() else {
            return Ok(None);
        };
        let Some(tle) = tle_by_sortgroupref(parse, sc.tleSortGroupRef) else {
            return Ok(None);
        };
        if !is_whitelisted_agg(tle.expr, rti, PLAIN_FOLD_AGGS) {
            return Ok(None);
        }
    }
    // groupby_high hold (shared floor; the floor recalibration lane owns
    // raising it): matched-shape-but-floored keeps Gather.
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
        return Ok(Some(false));
    }
    Ok(Some(finish_knob_path(
        run,
        "extractkey",
        "extract-exprkey-grouped-agg",
        extract_exprkey_guard(),
        relid,
        ngroups,
        rel_rows,
        rel_pages,
    )?))
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
    aggref_plain_typed(agg, rti, is_int_family)
}

/// SE-AGGPOLY (band 101001): the single-rel plain-agg INDEX guard —
/// strictly narrower than the join classes' blanket "unindexed" rule
/// (which refused tpch q06 outright: lineitem carries its PRIMARY KEY, a
/// live census finding). With ONE baserel and no join, an index can steer
/// the suppressed serial plan away from Agg-over-SeqScan only when:
///   (a) a QUAL references the index's KEY columns (an index path becomes
///       electable — the walk would refuse the IndexScan outer, the
///       suppress-then-refuse direction), or
///   (b) the index COVERS every column the query references (an
///       index-only scan can cost below the seqscan even qual-free).
/// Expression or partial indexes refuse outright (their matching is the
/// planner's own — not re-derived here), as do whole-row references.
/// q06's lineitem_pkey (l_orderkey, l_linenumber) triggers neither arm.
fn heap_poly_indexes_admit(
    run: &PlannerRun<'_>,
    parse: &Query<'_>,
    quals: Option<Node<'_>>,
    rti: usize,
    rel_id: types_pathnodes::RelId,
) -> PgResult<bool> {
    let rel = run.root.rel(rel_id);
    if rel.indexlist.is_empty() {
        return Ok(true);
    }
    use types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
    let mut qual_bm = types_nodes::Bitmapset::empty();
    if let Some(q) = quals {
        vars::pull_varattnos(run.mcx, q, rti as i32, &mut qual_bm)?;
    }
    let mut all_bm = types_nodes::Bitmapset::empty();
    if let Some(q) = quals {
        vars::pull_varattnos(run.mcx, q, rti as i32, &mut all_bm)?;
    }
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
        vars::pull_varattnos(run.mcx, tle.expr, rti as i32, &mut all_bm)?;
    }
    let raw = |m: i32| m + FirstLowInvalidHeapAttributeNumber;
    // Whole-row or system-column references: refuse (nothing the coverage
    // arm can reason about).
    for m in all_bm.iter() {
        if raw(m) <= 0 {
            return Ok(false);
        }
    }
    for index in rel.indexlist.iter() {
        if !index.indexprs.is_empty() || !index.indpred.is_empty() {
            return Ok(false);
        }
        let keys = &index.indexkeys;
        let nkey = (index.nkeycolumns as usize).min(keys.len());
        // (a) qual vars on the index's key columns.
        for m in qual_bm.iter() {
            let a = raw(m);
            if keys[..nkey].iter().any(|&k| k == a) {
                return Ok(false);
            }
        }
        // (b) every referenced column inside the index (key + INCLUDE) —
        // index-only-scan coverable.
        let covers_all = all_bm
            .iter()
            .all(|m| keys.iter().any(|&k| k == raw(m)));
        if covers_all {
            return Ok(false);
        }
    }
    Ok(true)
}

/// SE-AGGPOLY (band 101001): the plain-heap-poly tlist discipline — every
/// entry is a whitelisted bare-int-Var aggregate (PLAIN_FOLD_AGGS) or a
/// structurally plain sum/avg(NUMERIC) (no ORDER BY/DISTINCT/FILTER/
/// variadic/ordered-set/levelsup) whose single argument expression the
/// planner's own `is_parallel_safe` admits (it runs on helpers through the
/// per-row transition program; the arg SHAPE is otherwise free — the poly
/// manifest classifies by state, not argument). At least one numeric
/// aggregate required (all-int shapes keep their existing rows), and
/// nothing else in the tlist (consts and bare Vars refuse — narrow probe).
fn heap_poly_tlist_admits(
    run: &PlannerRun<'_>,
    parse: &Query<'_>,
    rti: usize,
) -> PgResult<bool> {
    let mut n_numeric = 0usize;
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
        if is_whitelisted_agg(tle.expr, rti, PLAIN_FOLD_AGGS) {
            continue;
        }
        let Some(agg) = tle.expr.as_aggref() else { return Ok(false) };
        if !matches!(agg.aggfnoid, F_AVG_NUMERIC | F_SUM_NUMERIC)
            || agg.agglevelsup != 0
            || agg.aggkind != AGGKIND_NORMAL
            || agg.aggvariadic
            || !agg.aggorder.is_nil()
            || !agg.aggdistinct.is_nil()
            || agg.aggfilter.is_some()
            || !agg.aggdirectargs.is_nil()
            || agg.args.len() != 1
        {
            return Ok(false);
        }
        let Some(arg_tle) = agg.args.nth(0).as_target_entry() else { return Ok(false) };
        if !crate::is_parallel_safe_opt(run, Some(arg_tle.expr))? {
            return Ok(false);
        }
        n_numeric += 1;
    }
    Ok(n_numeric > 0)
}

/// `aggref_plain` with a caller-supplied single-arg type predicate: a
/// structurally plain Aggref (no ORDER BY/DISTINCT/FILTER/variadic/
/// ordered-set/levelsup) whose arg is empty (count(*)) or a single Var of an
/// `arg_type_ok` type on the scanned rel. `aggref_plain` = int-family arg;
/// the date scan-fold recognizer (is_plain_fold_agg) passes t==DATEOID.
fn aggref_plain_typed(agg: &Aggref<'_>, rti: usize, arg_type_ok: impl Fn(u32) -> bool) -> bool {
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
            is_covered_key_var(arg_tle.expr, rti, arg_type_ok)
        }
        _ => false,
    }
}

/// A plain scan-fold aggregate (CbPlainAggFold arm): the int-family
/// PLAIN_FOLD_AGGS over int-family Vars (count(*) included), OR min/max(date)
/// over a bare DATE Var. WS-COVER (phase3-close §3.2) widens the probe onto
/// the date min/max shape the fold arm's classify_trans already admits at the
/// I32 lane width — strictly narrower than the walk (probe ⊂ walk, risk P1),
/// and reusing the CbPlainAggFold floor because date is int4-width byval so
/// the fold economics are byte-identical to int4 min/max.
fn is_plain_fold_agg(expr: Node<'_>, rti: usize) -> bool {
    if is_whitelisted_agg(expr, rti, PLAIN_FOLD_AGGS) {
        return true;
    }
    let Some(agg) = expr.as_aggref() else { return false };
    matches!(agg.aggfnoid, F_MAX_DATE | F_MIN_DATE)
        && aggref_plain_typed(agg, rti, |t| t == DATEOID)
}

/// Every tlist entry is a plain scan-fold aggregate (int-family or date
/// min/max), and at least one entry exists — the CbPlainAggFold admission.
fn tlist_all_plain_fold_aggs(parse: &Query<'_>, rti: usize) -> bool {
    let mut n = 0usize;
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return false };
        if !is_plain_fold_agg(tle.expr, rti) {
            return false;
        }
        n += 1;
    }
    n > 0
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

/// Every tlist entry is a zero-arg `count(*)` Aggref (n > 0) — the
/// count-only census shape: no transition reads a scan column, so no fold
/// plan exists for the runtime scan arm to own (q2box keying guard).
fn tlist_all_count_star(parse: &Query<'_>) -> bool {
    let mut n = 0usize;
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return false };
        let Some(agg) = tle.expr.as_aggref() else { return false };
        if agg.aggfnoid != F_COUNT_STAR || !agg.args.is_nil() {
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

/// TPCH-DECOROOT (CAR 1): every ORDER BY key resolves to a covered tlist
/// entry — a GROUP-key ref (any type and sort direction: the serial Sort
/// above the engaged arm owns the ordering semantics over the full grouped
/// output) or a class-vocabulary aggregate. Junk tlist entries the parser
/// adds for uncovered ORDER BY exprs fail here (and the emit walk refuses
/// them independently — defense in depth). Empty sort clauses are NOT this
/// shape (the bare LIMIT/OFFSET compositions have their own rows).
fn scan_sort_keys_covered(
    parse: &Query<'_>,
    key_refs: &[u32],
    rti: usize,
    passenger_list: &[u32],
) -> bool {
    if parse.sortClause.is_nil() {
        return false;
    }
    for sc_node in &parse.sortClause {
        let Some(sc) = sc_node.as_sort_group_clause() else { return false };
        if key_refs.contains(&sc.tleSortGroupRef) {
            continue;
        }
        let Some(tle) = tle_by_sortgroupref(parse, sc.tleSortGroupRef) else { return false };
        if !is_whitelisted_agg(tle.expr, rti, passenger_list) {
            return false;
        }
    }
    true
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

    /// SE-SCANPASS knob (band 72001): `PGRUST_LANE_V2_SCANPASS` is default
    /// OFF and only `1`/`on` arm it — every other spelling fails safe to
    /// today's behaviour (the K1-latemat idiom). This pins the default-OFF
    /// guarantee that makes the change inert at default.
    #[test]
    fn scanpass_knob_is_default_off() {
        assert!(!scanpass_spelling_on(None), "unset must be OFF (default)");
        assert!(!scanpass_spelling_on(Some("0")));
        assert!(!scanpass_spelling_on(Some("off")));
        assert!(!scanpass_spelling_on(Some("")));
        assert!(!scanpass_spelling_on(Some("true")), "typos fail safe to OFF");
        assert!(!scanpass_spelling_on(Some("ON")), "case-sensitive, like the arm knobs");
        assert!(scanpass_spelling_on(Some("1")));
        assert!(scanpass_spelling_on(Some("on")));
        // The live getter memoizes the process env; in the test binary the
        // var is unset, so it resolves OFF — the default-OFF invariant.
        assert!(!scanpass_enabled(), "test process has no knob set => OFF");
    }

    /// Naming a passthrough refusal is NEVER a suppression: every arm of the
    /// recognizer keeps Gather (returns None). Pins the "naming != flipping
    /// route_to" contract — a suppression without a covered arm would land
    /// on serial (risk P1's false-positive direction).
    #[test]
    fn scanpass_refusals_keep_gather() {
        assert_eq!(refuse_scanpass("x").unwrap(), false);
        // Both filtered and unfiltered pgrcolumnar passthrough refuse; heap
        // and non-Var-projection refuse. Every reason path returns None.
        for why in [
            "heap rel",
            "ordered passthrough",
            "projection expr not covered",
            "bare filtered pgrcolumnar passthrough",
        ] {
            assert_eq!(refuse_scanpass(why).unwrap(), false);
        }
    }

    /// SE-MKTEXT knob (Lane-3 two-key text car): `PGRUST_LANE_V2_
    /// MULTIKEY_TEXT` is DEFAULT ON since t35 routing-flips
    /// (GL-MKTEXT-1 FLIP-RECOMMENDED) and only the exact kill spellings
    /// `0`/`off` disarm it — the flipped-kill idiom (a typo'd kill leaves
    /// the measured-winning default in place; matches textdistinct/
    /// runtime-arm kill conventions). Pins the default-ON posture and the
    /// kill switch's exact spellings.
    #[test]
    fn multikey_text_knob_is_default_on_with_kill() {
        assert!(multikey_text_spelling_on(None), "unset must be ON (t35 flipped default)");
        assert!(!multikey_text_spelling_on(Some("0")), "kill spelling");
        assert!(!multikey_text_spelling_on(Some("off")), "kill spelling");
        assert!(multikey_text_spelling_on(Some("")), "non-kill spellings stay ON");
        assert!(multikey_text_spelling_on(Some("true")), "non-kill spellings stay ON");
        assert!(
            multikey_text_spelling_on(Some("OFF")),
            "kill is case-sensitive, like the arm kills"
        );
        assert!(multikey_text_spelling_on(Some("1")));
        assert!(multikey_text_spelling_on(Some("on")));
        // The live getter memoizes the process env; in the test binary the
        // var is unset, so it resolves ON — the flipped-default invariant.
        assert!(multikey_text_enabled(), "test process has no kill set => ON");
    }

    /// SE-MKTEXT shape law: the knob-widened family is EXACTLY the two-key
    /// int+text / text+text census — everything beyond fails closed. The
    /// admitted set and the still-refused set of record; the surrounding
    /// census refuses expr keys and non-default collations before this law
    /// (bare-Var + DEFAULT_COLLATION_OID discipline, unchanged).
    #[test]
    fn mk_text_family_admits_two_key_text_shapes_only() {
        // ADMITTED: the q17/q18 class (int+text) and text+text.
        assert!(mk_text_family_shape_ok(2, 1), "two keys, int+text");
        assert!(mk_text_family_shape_ok(2, 2), "two keys, text+text");
        // REFUSED: all-int two-key (existing bootstrap rows own it).
        assert!(!mk_text_family_shape_ok(2, 0), "int+int is not this family");
        // REFUSED: single-key shapes (existing rows / sibling cars).
        assert!(!mk_text_family_shape_ok(1, 1), "single text key is the C2/bootstrap row");
        assert!(!mk_text_family_shape_ok(1, 0));
        // REFUSED: 3+ keys — with or without a second text (fail-closed;
        // the q19 class additionally carries an expr key, refused upstream).
        assert!(!mk_text_family_shape_ok(3, 1), "3-key with one text stays bootstrap-only");
        assert!(!mk_text_family_shape_ok(3, 2), "3-key with two texts fails closed");
        assert!(!mk_text_family_shape_ok(4, 2));
        assert!(!mk_text_family_shape_ok(6, 1));
        // Degenerate censuses can never arise (n_text <= nkeys), but the
        // law still refuses them.
        assert!(!mk_text_family_shape_ok(0, 0));
        assert!(!mk_text_family_shape_ok(2, 3));
    }

    /// SE-MKTEXT engine-kill coherence: with the executor text cars at
    /// their defaults (no kill env set in the test process), the coherence
    /// gate admits both the one-text and two-text censuses — and the gate
    /// is the ONLY extra condition between the shape law and family
    /// membership, so a thrown kill un-keys the family (asserted live by
    /// the e2e, not reproducible in-process once the OnceLock caches).
    #[test]
    fn mk_text_agg_car_coherence_defaults_on() {
        assert!(mk_text_agg_cars_live(1));
        assert!(mk_text_agg_cars_live(2));
        assert!(agg_freeze_car_live());
    }

    /// Sibling-lane knobs (SE-EXTRACTKEY / SE-CONSTKEY / SE-BARELIMIT):
    /// the shared spelling rule is DEFAULT ON since t35 routing-flips with
    /// exact-spelling kills `0`/`off` (the flipped-kill idiom) — and the
    /// live getters resolve ON in the test process (no kill set), pinning
    /// each lane's flipped-default posture.
    #[test]
    fn sibling_lane_knobs_are_default_on_with_kill() {
        assert!(knob_spelling_on(None), "unset must be ON (t35 flipped default)");
        assert!(!knob_spelling_on(Some("0")), "kill spelling");
        assert!(!knob_spelling_on(Some("off")), "kill spelling");
        assert!(knob_spelling_on(Some("")), "non-kill spellings stay ON");
        assert!(knob_spelling_on(Some("true")), "non-kill spellings stay ON");
        assert!(knob_spelling_on(Some("OFF")), "kill is case-sensitive, like the arm kills");
        assert!(knob_spelling_on(Some("1")));
        assert!(knob_spelling_on(Some("on")));
        assert!(extract_exprkey_enabled(), "test process has no kill set => ON");
        assert!(agg_constkey_enabled(), "test process has no kill set => ON");
        assert!(agg_barelimit_enabled(), "test process has no kill set => ON");
    }

    /// SE-EXTRACTKEY packed-image width law: mirrors the exprkey Multi
    /// walk's 16-byte negotiation exactly — a shape that fits neither the
    /// 8-byte nor the shrunk 4-byte numeric image must NOT be keyed
    /// (suppress-then-refuse). Admitted/refused sets of record.
    #[test]
    fn extract_key_image_width_law() {
        // The q19 image: int8 + text4 + numeric8 = 20 → shrink → 16. Fits.
        assert!(extract_key_image_fits(8, 1));
        // int4 + text + extract: 4+4+8 = 16 exactly.
        assert!(extract_key_image_fits(4, 1));
        // extract alone / with one small int.
        assert!(extract_key_image_fits(0, 0));
        assert!(extract_key_image_fits(2, 0));
        assert!(extract_key_image_fits(8, 0));
        // int8 + int4 + extract: 12+8=20 → shrink 12+4=16. Fits.
        assert!(extract_key_image_fits(12, 0));
        // int8 + int8 + extract: 16+4=20 even shrunk. REFUSED.
        assert!(!extract_key_image_fits(16, 0));
        // int8 + int8 + text + extract: wider still. REFUSED.
        assert!(!extract_key_image_fits(16, 1));
        // int8 + int4 + text + extract: 12+4+4=20 even shrunk. REFUSED.
        assert!(!extract_key_image_fits(12, 1));
    }

    /// Step-1 cost-route wiring pins (runtime-cost-model design §5 step 1).
    /// The curve map is total by construction (match); this pins WHICH
    /// classes deliberately have no curve — a new CoverClass must either
    /// get a fitted curve (ladder cells + TSV rows) or join this list with
    /// a TSV note, never fall through silently.
    #[test]
    fn cost_route_map_names_its_curveless_classes() {
        for row in BOOTSTRAP_MATRIX {
            let curveless = cover_class_curve(row.class).is_none();
            let expect_curveless = matches!(row.class, CoverClass::CbMetaFooterAgg);
            assert_eq!(
                curveless, expect_curveless,
                "cost-route curve map drift for {:?}",
                row.class
            );
        }
    }

    /// The rectangle/admission/hold values the cost-route does NOT retire
    /// must match their rows in the constants table of record
    /// (crates/backend/optimizer/path/costsize/src/runtime-cost-constants.tsv) — same tie as
    /// bootstrap_matrix_matches_tsv, for the step-1 residue.
    #[test]
    fn retained_rectangles_match_constants_tsv() {
        let tsv = include_str!("../../../../../../crates/backend/optimizer/path/costsize/src/runtime-cost-constants.tsv");
        let mut vals: std::collections::BTreeMap<(String, String), String> =
            std::collections::BTreeMap::new();
        for line in tsv.lines() {
            if line.starts_with('#') || line.trim().is_empty() || line.starts_with("class\t") {
                continue;
            }
            let cols: Vec<&str> = line.split('\t').collect();
            assert_eq!(cols.len(), 10, "malformed TSV row: {line}");
            vals.insert((cols[0].to_string(), cols[1].to_string()), cols[2].to_string());
        }
        let get = |c: &str, t: &str| {
            vals.get(&(c.to_string(), t.to_string()))
                .unwrap_or_else(|| panic!("TSV missing {c}.{t}"))
                .clone()
        };
        assert_eq!(
            get("HeapPlainCountStar", "admission_min_pages").parse::<f64>().unwrap(),
            class_guard(CoverClass::HeapPlainCountStar).min_pages
        );
        assert_eq!(
            get("_grouped_classes", "hold_groups_min").parse::<f64>().unwrap(),
            groupby_high_floor(),
            "groupby-high HOLD drifted from its TSV row (env override in a test run?)"
        );
        // Reuse rows point at real curve classes.
        assert_eq!(get("CbHashJoinMultiBuild", "curve_reuse"), "CbHashJoinPlainAgg");
        assert_eq!(get("CbHashJoinGroupedAgg", "curve_reuse"), "CbHashJoinPlainAgg");
        assert_eq!(get("AggPolyHeapPlain", "curve_reuse"), "HeapCmpFoldPrefix");

    /// TPCH-CARS knobs (night/tpch-cars-1): both cars are DEFAULT OFF and
    /// arm only on the exact spellings `1`/`on` (the SE-SCANPASS /
    /// K1-latemat default-OFF idiom — typos fail safe to today's behaviour,
    /// byte-identical plan time). Pins the default-OFF posture that makes
    /// the branch inert at default, and the live getters' resolution in a
    /// knob-free process.
    #[test]
    fn tpch_cars_knobs_are_default_off() {
        assert!(!knob_spelling_armed(None), "unset must be OFF (default)");
        assert!(!knob_spelling_armed(Some("0")));
        assert!(!knob_spelling_armed(Some("off")));
        assert!(!knob_spelling_armed(Some("")));
        assert!(!knob_spelling_armed(Some("true")), "typos fail safe to OFF");
        assert!(!knob_spelling_armed(Some("ON")), "case-sensitive, like the arm knobs");
        assert!(knob_spelling_armed(Some("1")));
        assert!(knob_spelling_armed(Some("on")));
        // The live getters memoize the process env; in the test binary the
        // vars are unset, so both resolve OFF — the default-OFF invariant.
        assert!(!decoroot_enabled(), "test process has no knob set => OFF");
        assert!(!aggjoin_numeric_enabled(), "test process has no knob set => OFF");
    }

    /// TPCH-DECOROOT hash-election margin: the provisional bound must stay
    /// a real margin (>1 — ngroups strictly below input) so the decorated
    /// suppression never keys a shape whose serial costing could plausibly
    /// prefer the sorted-agg landing the walk refuses; and it must bound
    /// the serial decoration Sort at a small fraction of the input.
    #[test]
    fn decoroot_margin_is_conservative() {
        assert!(DECOROOT_NGROUPS_MARGIN >= 8.0);
        // The margin composes with the aggjoin export headroom: at the 64k
        // group floor, engaged inputs are >= 1M rows.
        assert!(GROUPSINK_NGROUPS_FLOOR * DECOROOT_NGROUPS_MARGIN >= 1_000_000.0);
    }

    /// TPCH-JHEAP knob (night/tpch-jheap): DEFAULT OFF, `1`/`on` arms
    /// (the shared tpch-cars idiom); the executor coherence mirror
    /// resolves LIVE in a knob-free process (K2_PROBE/HEAPFEED are
    /// default-ON with `=0|off` kills — the SE9/SE15 flipped posture), so
    /// the probe's heap gate is exactly the jheap knob at defaults. Pins
    /// the default-OFF inertness and the mirror's default-ON reading.
    #[test]
    fn jheap_knob_default_off_mirror_live() {
        assert!(!jheap_enabled(), "test process has no knob set => OFF");
        assert!(
            k2_heapfeed_live(),
            "K2_PROBE/HEAPFEED default ON (SE9/SE15 flips) => mirror live"
        );
    }

    /// TPCH-CBKEYS knob (night/tpch-cbkeys): DEFAULT OFF, `1`/`on` arms
    /// (the shared tpch-cars idiom) — bytes-key join shapes are unkeyable
    /// at default, byte-identical plan time; and the bytes floor keeps the
    /// grouped-join 2M ceiling (the scan text-key min_dop discipline is
    /// subsumed — its low-dop win region covers the whole admitted range).
    #[test]
    fn cbkeys_knob_default_off_and_floor() {
        assert!(!cbkeys_enabled(), "test process has no knob set => OFF");
        let g = cbkeys_guard();
        assert_eq!(g.max_rows, 2_000_000.0);
        assert_eq!(g.min_rows, 0.0);
    }

    /// TPCH-JHEAP NL-election margin + floor: the margin must be a real
    /// multiple (the NL-with-inner-index election needs the outer side
    /// comparable to the indexed side — 4x dominance keeps hash safely
    /// preferred), and the heap floor must sit at the heap fold arms'
    /// measured 1M/dop12 economics under the 2M nbatch1 ceiling.
    #[test]
    fn jheap_margin_and_floor_are_conservative() {
        assert!(JHEAP_NL_MARGIN >= 4.0);
        let g = jheap_guard();
        assert_eq!(g.min_rows, 1_000_000.0);
        assert_eq!(g.max_rows, 2_000_000.0);
        assert_eq!(g.min_dop, 12);
        assert_eq!(g.low_dop_max_rows, 0.0);
    }

    /// The living-matrix discipline (§4.1, reconciled at m5-integration):
    /// the routing table the probe consults and the ONE checked-in living
    /// artifact (crates/backend/executor/execmain/src/lanev2/m5-coverage.tsv, the M5-1 router file) must
    /// not drift apart. The TSV is the reviewable/reportable surface; this
    /// table is the executable one; this test is the tie. A probe key may
    /// span several matrix rows (CbPlainAggFold keys both pgrcolumnar fold
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

    /// SE-T2AGG (night/tier2-agg-cars) knob posture of record at t36
    /// flips2, per the GL-T2 letters: CAR B (gap:agg-min-text) stays
    /// DEFAULT OFF — `1`/`on` only (KEEP-GATED by GL-T2A: the
    /// suppress-then-serial containment violation). CARs A
    /// (distinct-plain-shape, GL-T2C) and C (gap:agg-orderby-nolimit,
    /// GL-T2B) are DEFAULT ON with the exact-spelling kill `0`/`off` —
    /// the flipped-kill idiom.
    #[test]
    fn tier2_agg_car_knob_postures() {
        // Still-gated spelling rule (CARs A + B).
        assert!(!tier2_car_spelling_on(None), "unset must be OFF (default)");
        for v in ["0", "off", "", "true", "ON", "yes"] {
            assert!(!tier2_car_spelling_on(Some(v)), "spelling {v:?} must fail safe to OFF");
        }
        assert!(tier2_car_spelling_on(Some("1")));
        assert!(tier2_car_spelling_on(Some("on")));
        // Flipped kill rule (CAR C).
        assert!(tier2_car_kill_spelling_on(None), "unset must be ON (t36 flipped default)");
        assert!(!tier2_car_kill_spelling_on(Some("0")), "kill spelling");
        assert!(!tier2_car_kill_spelling_on(Some("off")), "kill spelling");
        for v in ["", "true", "OFF", "1", "on"] {
            assert!(tier2_car_kill_spelling_on(Some(v)), "non-kill spelling {v:?} stays ON");
        }
        // The live getters memoize the process env; in the test binary no
        // vars are set, so the postures resolve to the shipped defaults.
        assert!(distinct_plainshape_enabled(), "CAR A must be ON at default (GL-T2C flip)");
        assert!(!agg_strminmax_enabled(), "CAR B must be OFF at default (KEEP-GATED)");
        assert!(agg_sort_nolimit_enabled(), "CAR C must be ON at default (GL-T2B flip)");
    }

    /// SE-T2AGG CAR A engine-kill coherence: the runtime plain-distinct sink
    /// family's kill (`PGRUST_RUNTIME_PLAINDISTINCT`, default ON) resolves
    /// LIVE in a kill-free process — the probe's coherence gate is inert
    /// unless an attribution kill is thrown (the mk_text_agg_cars_live
    /// pattern).
    #[test]
    fn tier2_plaindistinct_engine_coherence_defaults_live() {
        assert!(plaindistinct_engine_live());
    }

    /// SE-T2AGG CAR B: the min/max(text) OIDs of record (vendored REL 18.3
    /// pg_proc.dat) — a silent renumber would move the car onto the wrong
    /// aggregates.
    #[test]
    fn tier2_strminmax_oids_of_record() {
        assert_eq!(F_MIN_TEXT, 2145);
        assert_eq!(F_MAX_TEXT, 2129);
        assert!(!GROUPED_SINK_AGGS.contains(&F_MIN_TEXT), "text min/max stays knob-gated");
        assert!(!GROUPED_SINK_AGGS.contains(&F_MAX_TEXT), "text min/max stays knob-gated");
        assert!(
            !DISTINCT_PASSENGER_AGGS.contains(&F_MIN_TEXT)
                && !DISTINCT_PASSENGER_AGGS_POLY.contains(&F_MIN_TEXT),
            "the distinct sink's vocabulary never admits text min/max"
        );
    }
}
