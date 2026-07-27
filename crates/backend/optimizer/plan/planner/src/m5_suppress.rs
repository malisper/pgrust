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
use types_core::BTREE_AM_OID;
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
    /// byval), so the same fold economics + floor apply (byval min/max flip).
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
    /// m3-sort-b combine-phase top-N composition, two-key grouped-top-n family);
    /// §2.4 law 2b degrade rules are arm-internal.
    CbGroupedAggTopN,
    /// Grouped COUNT(DISTINCT <int Var>) over pgrcolumnar, int-family GROUP
    /// keys (the runtime distinct sink's sorted-distinct feed — the narrow-sort
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
    /// only), cbstore AM (heap sides: SE-JHEAP keys them knob-gated
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
    /// serial plan shape certain (Agg over SeqScan). Filtered fact-rel plain-agg class.
    ///
    /// AGG_INTCASE widening (int-CASE fold-args car, knob-gated
    /// `PGRUST_LANE_V2_AGG_INTCASE`, DEFAULT ON since GL-INTCASE-1 —
    /// `=0|off` kills; requires the AGG_POLY knob
    /// too): the tlist vocabulary additionally admits int-family
    /// plain aggregates (INTCASE_POLY_AGGS) over ANY parallel-safe
    /// single-argument expression — the conditional-aggregation idiom
    /// (sum(CASE...), count-if) — and non-Aggref emit expressions over
    /// admitted aggregates (ratio emits; leader-side finalize only). The
    /// keying gate becomes n_poly > 0: >=1 numeric anchor OR >=1
    /// conditional-bearing int arg (guaranteed manifest engagement — see
    /// heap_poly_tlist_admits).
    AggPolyHeapPlain,
    /// M5-5 Meta-over-Gather (the band-2a arithmetic-agg handoff): plain (ungrouped)
    /// FOOTER-ANSWERABLE aggregation over one plain pgrcolumnar rel with NO
    /// quals — count(*)/count(col), min/max over bare int-family Vars,
    /// and sum/avg over int2/int4 AFFINE transforms (`v±k`, `v*k`; the
    /// lanefold classify_arg admission, divk==1 only — classify_meta
    /// refuses division) or bare int8 Vars. The serial lane's Meta arm
    /// answers these from part footers in milliseconds; the
    /// planner-parallel FinalizeAgg→Gather→PartialAgg shape escapes the
    /// Meta arm's Agg-over-SeqScan scope entirely (band-2a measured
    /// the SUM(x op k) family @100M: 3.9–7.2s parallel vs ~5ms footer — a ~700x hole
    /// neutralized only by forced vectors). Suppressing Gather keeps the
    /// serial plan; if the Meta arm's runtime footer checks refuse (guard
    /// interval / non-MVCC), the runtime scan-arm fold is the engagement
    /// fallback (lanefold admits the same affine forms), so no
    /// suppress-then-refuse serial cliff class opens.
    CbMetaFooterAgg,
    /// PARTWISE-MORSELS increment 1 (night/partitionwise-morsels): plain
    /// count/sum fold over ONE declaratively partitioned parent — >=2 leaf
    /// partitions, uniform unindexed heap XOR pgrcolumnar children, no
    /// quals, Agg-root only. Classifier lives in m5_partwise.rs (its own
    /// module); the executor half is the partition-as-morsel arm
    /// (lanev2/runtime_partwise.rs — child edges as hard GranuleMap
    /// boundaries on ONE concatenated claim space). DEFAULT ON since
    /// GL-PARTWISE-1 (2026-07-21); PGRUST_LANE_V2_PARTWISE=0|off kills.
    /// Rectangle-retained: the PROVISIONAL per-AM floors live in the
    /// classifier (per-AM shape is inexpressible in one FloorGuard).
    PartwisePlainFold,
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
        qualifiers: "se-aggpoly (band 101001): keyed ONLY under PGRUST_LANE_V2_AGG_POLY (knob coherence); one unindexed heap rel; quals allowed, is_parallel_safe; tlist = PLAIN_FOLD_AGGS bare-int OR plain sum/avg(numeric) w/ parallel-safe arg exprs, >=1 numeric; AGG_INTCASE widening (PGRUST_LANE_V2_AGG_INTCASE, DEFAULT ON since GL-INTCASE-1 2026-07-21, =0|off kills; dop4 losses are the floor's region): + int-family aggs over parallel-safe conditional args (sum(CASE)/count-if) + ratio emits over admitted aggs, gate n_poly>0; Agg-root only (no sort/limit/offset); floor reused from HeapCmpFoldPrefix (provisional — GL-AGGPOLY-1 letter owed; GL-INTCASE-1 ladder validates the direction at 10M)",
    },
    MatrixRow {
        class: CoverClass::PartwisePlainFold,
        covered: true,
        qualifiers: "partwise-morsels inc-1: plain count(*)/count(any)/sum(int2/4/8) over ONE partitioned parent; >=2 leaf partitions, uniform unindexed heap XOR pgrcolumnar children, no quals, Agg-root only; keyed under PGRUST_LANE_V2_PARTWISE (knob coherence, BOTH read sites; DEFAULT ON since GL-PARTWISE-1, =0|off kills); per-AM PROVISIONAL floors in the classifier (CbPlainAggFold/HeapCmpFoldPrefix reuse — floor-calibration mini-ladder is the named follow-up)",
    },
];

pub(crate) fn class_covered(class: CoverClass) -> bool {
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

/// The runtime hash-join arm's tiny-input admission floor in rows: 64
/// granules (runtime_hashjoin `min_granules`, env-overridable there) x
/// 8192 rows/granule. Join-class suppression below this lands on the
/// arm's tiny-input refusal — a serial fallback measured 2.22-2.34x worse
/// than the forgone PHJ (GL-HJMB-3, 500k rung, dop {4,8,16} x 2 takes) —
/// so both the class FloorGuard's min and the seat-lift path bound on it.
const HJ_ARM_MIN_ROWS: f64 = 524_288.0;

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
        // Wins everywhere engaged (0.34–0.85) — but only where the arm can
        // OWN the suppressed plan. Qualed-selective shapes whose post-qual
        // estimate is tiny elect the sorted serial grouping plan
        // (Sort→GroupAggregate: near ngroups≈rows the sort election wins
        // the serial tournament), which the arm's HashAgg-over-scan shape
        // gate refuses — the suppress-then-refuse class, costing flavor
        // (the agg-arm analog of the GL-HJMB-3 join finding). Witnessed
        // (soak adjudication round 1, 2026-07-21 @ 307329686bda+rig, jobs
        // pgrust-fast-tests-f52d0d5a33-{1784664239-7dc7,1784664244-2e55} +
        // -2c2fa48f66-1784664675-7c72): ~250-row post-qual cells suppressed
        // to SERIAL at 3.3–5.3x the forgone single-worker frame plan;
        // ≥598k post-qual cells engaged and won 4–6x over both serial and
        // the forced frame. min_rows sits just below the smallest witnessed
        // engaged win (598k, estimate-wobble headroom); the fail-closed
        // region keeps Gather. Cost asymmetry is one-directional: where the
        // serial election IS arm-ownable, the arm still engages at exec
        // time with Gather standing (witnessed: identical wall both ways),
        // so the floor only stops deleting viable parallel plans on
        // tiny-selective shapes. Knob paths carrying this guard verbatim
        // (strminmax-topn / decoroot / constkey) inherit the floor; their
        // letters own re-measuring their own tiny cells.
        CoverClass::CbGroupedAggTopN => {
            FloorGuard { min_rows: 500_000.0, ..NO_GUARD }
        }
        // GL-LOWDIST-1 re-derivation (2026-07-21, letter scratchpad/night/
        // GL-LOWDIST-1-letter.md; witnessed fix A/B @ a3d09b8ff, dop
        // {2,4,8} x 1M-10M): with the low-width combine + leader-parity
        // bump DEFAULT ON, the sink beats the forced-legacy GM+pardistinct
        // hybrid at every measured low-dop cell (0.67-0.96; sole residual
        // 5M-class dop8 = 1.008 parity) — min_dop drops 12 -> 2. dop1
        // stays keep-Gather (below the measured band; the executor bump
        // excludes it too). Kill-coherent: LOWWIDTH=0|off restores the
        // pre-flip min_dop-12 floor (whose own basis was dop4 1.21-1.24 /
        // dop8 1.06 / dop16 0.79-0.90) so the kill reverts routing AND
        // combine together.
        CoverClass::CbDistinctIntKeys => distinct_lowwidth_guard(FloorGuard {
            min_dop: 12,
            low_dop_max_rows: 0.0,
            ..NO_GUARD
        }),
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
        // GL-COST-TOPN-1 GUARD-OFF (this letter; the GL-SORTECON-3 min_dop=4
        // re-flip retired): the re-flip was ratified on rt/SERIAL economics
        // only — the four-posture ladder (scripts/sortecon-topn-ladder.sh @
        // 27db94812, 2026-07-21, jobs -376a/-3d75/-7436/-7d0c/-4b35) added
        // the forced GATHER MERGE leg the re-flip never measured (soak
        // escalation E1) and the arm wins ZERO best-of-four cells on the
        // witnessed grid (250k-10M x dop{1,2,4,8,16} x LIMIT {10,1000},
        // uniform int keys): at LIMIT 1000 forced GM beats the engaged arm
        // 2.89-9.77x at EVERY cell (and serial by 2-8.7x); at LIMIT 10 the
        // SERIAL zone walk beats the arm 1.33-1.75x (the arm's zone
        // predicate failed to refuse a shape whose zone-min cutoff skips
        // ~99% of granules). So the class is guarded OFF at every size (the
        // hjrider max_rows=0 precedent): keep Gather, let the legacy cost
        // model route (it elects GM where GM wins and serial where serial
        // wins — both measured better than the arm everywhere on this
        // band). NAMED CAVEAT: the dup/zone-hostile band's rt/serial wins
        // (0.09-0.49, GL-SORTECON-3) were never measured against a GM leg;
        // its GM-legged ladder is the re-open trigger for a band-scoped
        // re-guard-on. PGRUST_M5_TOPN_RECT=1 restores the retired min_dop=4
        // rectangle for A/B vehicles and one-train rollback.
        CoverClass::CbTopnBoundedIntKeys => {
            if topn_rect_enabled() {
                FloorGuard { min_dop: 4, low_dop_max_rows: 0.0, ..NO_GUARD }
            } else {
                FloorGuard { max_rows: 0.0, ..NO_GUARD }
            }
        }
        // Wins ≤1M (0.41 vs serial-shaped legacy); loses 1.39–1.50x once
        // legacy PHJ engages ≥2.5M at dop≤8, and 1.14 even at dop16@5M
        // (dop16@2.5M's 0.92 marginal win is deliberately forgone for the
        // clean single bound).
        //
        // RE-DERIVATION NOTE (GL-HJMB-1, 2026-07-21): this bound's original
        // basis was the retired v1 ladder; the witnessed record (hj-ladder-v2
        // + the GL-HJMB-1 control rung at 8M-probe/4M-build: arm/PHJ
        // 0.59-0.86 at dop4/8) supports LIFTING it. The lift's PREREQUISITE
        // GATE is the demote-unsafe boundary guard (the
        // estimate_runtime_hj_build_peak_bytes refusal in the join rel
        // guards + the arm's batched-band admission): without it, admitting
        // larger builds exposes the estimate-boundary seal-crossing refusal
        // — a 5-11x serial-rerun cliff vs the PHJ the suppression forgoes.
        // Any floor change must land WITH (or after) that guard and re-run
        // the GL-HJMB-1 boundary cells.
        //
        // MIN floor (GL-HJMB-3, 2026-07-21): suppression must not outrun
        // the ARM's own tiny-input admission floor (64 granules x 8192 rows
        // — runtime_hashjoin min_granules; a suppressed join the arm then
        // tiny-refuses lands SERIAL, witnessed 2.22-2.34x worse than the
        // forgone PHJ at 500k across dop {4,8,16}, both takes). 524,288 is
        // that floor expressed in rows; the seat-lift path honors the same
        // bound (its witnessed win band starts at 1M).
        //
        // BAND COLLAPSE (S1, soak adjudication round 1, 2026-07-21): the
        // clean-2M ceiling's forgone win at the (2.5M, dop16) cell was
        // re-confirmed IN VIVO — suppression 2.06x wall over the kept
        // Gather at the census shape (jobs pgrust-fast-tests-f52d0d5a33-
        // {1784664239-7dc7,1784664244-2e55} + -2c2fa48f66-1784664675-7c72
        // @ 307329686bda+rig; v2 grid 0.923; flip A/B -0325 47→45ms) — so
        // the rectangle now mirrors the witnessed curve verdicts instead
        // of sacrificing it: low dop keeps the 2M ceiling (dop4 losses
        // 1.39–1.50 witnessed above it), dop≥12 extends to the fitted
        // curve's own dop16 crossover N*≈4.18M floored to 4M (witnessed
        // win at 2.5M/dop16; the witnessed 1.024 parity-loss at 5M/dop16
        // stays OUT, so no kill-posture cell regresses). min_dop=12 per
        // the house auto-DOP convention (12–15 interpolated). DEFAULT
        // behavior is unchanged — this class decides by curve since t36
        // flips2; the collapse keeps the KILL/floor band from
        // resurrecting the forgone-win rot. Ceiling-lift prerequisite (the
        // GL-HJMB-1/-2 demote-unsafe boundary guard) is landed and sits
        // upstream; the guarded 8M-probe/4M-build control rung measured
        // GREEN above this ceiling. Reuse rider: the aggjoinnum knob path
        // borrows this guard verbatim (its letter owns re-measuring).
        CoverClass::CbHashJoinPlainAgg => FloorGuard {
            min_rows: HJ_ARM_MIN_ROWS,
            max_rows: 4_000_000.0,
            min_dop: 12,
            low_dop_max_rows: 2_000_000.0,
            ..NO_GUARD
        },
        // GL-COST-2 UNWIRE (this letter): the m5p1/SE-AGGJOIN PROVISIONAL
        // reuse of the hashjoin-nbatch1 floor is REFUTED by the riders' own
        // witnessed grids (L1/L2 @ d10db8ef5e: rt/legacy 3.04-6.03x
        // multibuild, 3.14-6.40x grouped, at EVERY 1M/2.5M x dop4/16 cell;
        // TSV rectangle_max_rows rows). No witnessed win region exists for
        // either rider (the t35 floor was itself a reuse, never measured),
        // so both are GUARDED OFF at every size — the topn max_rows=0
        // precedent: a measured-losing arm expressed as an economics
        // rectangle, un-guarding itself only through an OWN witnessed curve
        // (ladder spec L1/L2). PGRUST_M5_SIZE_FLOORS=0 re-enables for the
        // measurement vehicle; PGRUST_M5_HJRIDER_CURVE=1 restores the
        // refuted pre-letter wiring (2M rectangle + PlainAgg curve) for
        // A/B vehicles and one-train rollback.
        CoverClass::CbHashJoinMultiBuild | CoverClass::CbHashJoinGroupedAgg => {
            if hjrider_curve_enabled() {
                FloorGuard { max_rows: 2_000_000.0, ..NO_GUARD }
            } else {
                FloorGuard { max_rows: 0.0, ..NO_GUARD }
            }
        }
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
        // PARTWISE: unused by construction — the classifier never calls
        // finish(); its PROVISIONAL per-AM floors (cb vs heap differ, one
        // FloorGuard cannot express both) live in m5_partwise.rs with
        // GL-PARTWISE-1 provenance.
        CoverClass::PartwisePlainFold => NO_GUARD,
    }
}

/// GL-COST-2 unwire kill (one-train retention, the FloorGuard =0-fallback
/// precedent): `PGRUST_M5_HJRIDER_CURVE=1|on` restores the REFUTED
/// pre-letter wiring — the multibuild / grouped-agg-join riders back on the
/// CbHashJoinPlainAgg curve with the 2M rectangle — for A/B vehicles and
/// emergency rollback. DEFAULT OFF = unwired (exact-spelling arm, the
/// scanpass fail-safe idiom): the riders are guarded off at every size
/// until an own witnessed curve shows a win region (witnessed grids in
/// crates/backend/optimizer/path/costsize/src/runtime-cost-constants.tsv; ladder specs L1/L2).
/// GL-MBSEAT-1 planner mirrors of the executor seat-world kills (the
/// knob-coherence law — same spellings, flipped-kill parses; both
/// default ON since the MBSHARED/MBSEAT flips). The grouped rider's OWN
/// curve is valid ONLY on the seated arm, so `cover_class_curve`
/// un-curves the class when either mirror reads dark and the class falls
/// back to its guarded-off rectangle (the GL-COST-2 kill posture).
fn mbshared_live() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_MBSHARED").as_deref(), Ok("0") | Ok("off"))
    })
}

fn mbseat_live() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_MBSEAT").as_deref(), Ok("0") | Ok("off"))
    })
}

fn hjrider_curve_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(
            std::env::var("PGRUST_M5_HJRIDER_CURVE").as_deref(),
            Ok("1") | Ok("on")
        )
    })
}

/// GL-COST-TOPN-1 rollback lever: PGRUST_M5_TOPN_RECT=1 restores the
/// retired GL-SORTECON-3 min_dop=4 rectangle for the bounded top-N int-key
/// class — for A/B vehicles and one-train emergency rollback. DEFAULT OFF
/// = the guard-off posture (exact-spelling arm, the hjrider idiom): the
/// class keeps Gather at every size until a witnessed grid shows a
/// best-of-four win region (four-posture record @ 27db94812 shows none).
fn topn_rect_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(
            std::env::var("PGRUST_M5_TOPN_RECT").as_deref(),
            Ok("1") | Ok("on")
        )
    })
}

/// GL-TOPNHEAP-1 routing twin of the executor's PGRUST_RUNTIME_TOPN_HEAP
/// (runtime_sort.rs `runtime_topn_heap_enabled` — the SAME env string, the
/// intcase knob-coherence pattern): DEFAULT ON since the flip, kill
/// spellings exactly `0|off`. Killed = the class's curve decide entry goes
/// dark and the GL-COST-TOPN-1 guard-off keep-Gather posture stands
/// byte-exactly (routing-coherent kill: the executor's direct feed reverts
/// on the same spelling, so no posture can suppress onto a car that will
/// not run).
fn topn_heap_route_live() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| topn_heap_spelling_on(std::env::var("PGRUST_RUNTIME_TOPN_HEAP").ok().as_deref()))
}

/// Pure spelling law for PGRUST_RUNTIME_TOPN_HEAP (unit-pinned): unset or
/// anything but the kill spellings = ON.
fn topn_heap_spelling_on(v: Option<&str>) -> bool {
    !matches!(v, Some("0") | Some("off"))
}

/// GL-TOPNHEAP-1 car-mirror payload vocabulary (plan-time UNDER-
/// approximation of the executor's `attbyval && attlen 1..=8` census —
/// the direct feed captures output cells as raw datum words): the
/// int family + the i32/i64-backed datetime pair the sort keys already
/// ride, bool, floats, oid. A miss keeps Gather (never suppresses onto
/// the incumbent arm).
fn topn_car_payload_type(typ: u32) -> bool {
    use ::types_core::catalog::{BOOLOID, FLOAT4OID, FLOAT8OID, OIDOID};
    is_int_family(typ)
        || matches!(typ, DATEOID | TIMESTAMPOID)
        || matches!(typ, x if x == BOOLOID || x == FLOAT4OID || x == FLOAT8OID || x == OIDOID)
}

/// Executor capture envelope mirror (runtime_sort.rs TOPN_PAY_MAX).
const TOPN_CAR_PAY_MAX: usize = 6;

/// M5-5 floors kill switch: PGRUST_M5_SIZE_FLOORS=0 disables every guard.
/// The rowflip measure vehicle runs floors-off so engagement economics
/// stay measurable at any (size, dop); production default ON.
pub(crate) fn size_floors_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_M5_SIZE_FLOORS").map_or(true, |v| v.trim() != "0"))
}

/// SE-TEXTDISTINCT (C1 text-distinct + reduced-exprkey coverage car, band
/// 86001): the row-executor-removal WS-COVER census's `distinct-text-date-
/// args` (7.58s/7q) + `gap:agg-expr-keys` (2.42s/1q) rows plan Gather
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
/// the measured noise floor 0.9889; grouped distinct-arg engagements 0.010/0.011
/// hot vs cpg 0.44, ~40x).
fn textdistinct_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        // night/planner-fix-forced: DEFAULT FLIP OFF->ON (unset = on), the
        // GL-TEXTDIST default-flip win. Eliminates part of the CB_FORCE_PLANS=mt16
        // vector: unforced release selection now suppresses Gather and engages the
        // runtime distinct / plain-distinct / exprkey sinks for the text/int
        // count(DISTINCT) + reduced-expr-key grouped-agg shapes the
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
/// A/B measured the suppress-Gather arm as a 10M REGRESSION (int-key distinct
/// 0.046->0.151, text-key 0.081->0.175) — but that was the suppress-then-UNARMED hole: the plain
/// exact-DISTINCT sink armed off the bench GUC alone and never consulted
/// router::arm_dop, so the suppressed plan landed SERIAL with no pool. Fixed
/// at 98a012ba2 (fix(runtime_plaindistinct): arm via router::arm_dop
/// (Distinct)); GL-TEXTDIST-2 re-measure post-fix is GREEN — both at forced
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

/// GL-LOWDIST-4 B1 heap-distinct knob — the EXECUTOR spelling verbatim
/// (runtime_distinct::distinct_heap_enabled; GROUPSINK coherence: probe
/// routing and sink admission flip together). t35 law: DEFAULT OFF for the
/// letter; ON iff exactly `1`/`on`.
fn distinct_heap_probe_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    // DEFAULT ON since the GL-LOWDIST-4 flip (Michael's B1 GO; kill 0|off).
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_RUNTIME_DISTINCT_HEAP").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// GL-LOWDIST-4 B1 provisional floor for the HEAP distinct faces: GUARDED
/// OFF (max_rows 0 — the pre-flip CbTopnBoundedIntKeys posture): at
/// floors-ON defaults the Gather stands even knob-ON; the witnessed heap
/// ladder (floors OFF) measures the sink's heap economics, and the flip
/// re-derives this guard from that verdict. Never a suppress-then-lose
/// channel by construction.
/// GL-LOWDIST-4 flip re-derivation (letter §2/§4 + Michael's B3
/// suppress-to-sink ruling): the heap sink beats the hybrid at every
/// measured dop-4/16 cell (0.44-0.97) and beats the post-deletion
/// per-tuple fall 2.7-6.4x at dop1 — suppress everywhere the probe
/// classifies (the arm's own granule floor handles tiny rels). Kill
/// coherence rides the B1 knob itself (kill = no classify = keep Gather).
fn heap_distinct_guard() -> FloorGuard {
    NO_GUARD
}

/// GL-LOWDIST-4 B1: the narrow HEAP grouped-distinct census face — every
/// group key a bare int-family Var on the scanned rel, every non-key tlist
/// entry an admitted `count(DISTINCT <int-kind Var>)`, at least one of
/// them, NOTHING else (no vocab passengers in v1 — a miss keeps Gather,
/// unchanged from today). `Ok(None)` = shape miss (fall through).
fn classify_heap_grouped_distinct<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &Query<'mcx>,
    rti: usize,
    relid: u32,
    rel_id: types_pathnodes::RelId,
    rel_rows: f64,
    rel_pages: f64,
) -> PgResult<Option<bool>> {
    let mut key_refs: Vec<u32> = Vec::new();
    for gc_node in &parse.groupClause {
        let Some(gc) = gc_node.as_sort_group_clause() else { return Ok(None) };
        let Some(tle) = tle_by_sortgroupref(parse, gc.tleSortGroupRef) else {
            return Ok(None);
        };
        if !is_covered_key_var(tle.expr, rti, is_int_family) {
            return Ok(None);
        }
        key_refs.push(gc.tleSortGroupRef);
    }
    if key_refs.is_empty() {
        return Ok(None);
    }
    let mut n_count_distinct = 0usize;
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(None) };
        if tle.ressortgroupref != 0 && key_refs.contains(&tle.ressortgroupref) {
            continue;
        }
        if is_count_distinct_int(tle.expr, rti) {
            n_count_distinct += 1;
            continue;
        }
        return Ok(None);
    }
    if n_count_distinct == 0 {
        return Ok(None);
    }
    let ngroups = {
        let clauses =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.processed_groupClause);
        let group_exprs =
            types_pathnodes::run::sortgrouplist_exprs(run, &clauses, &parse.targetList);
        let input_rows = run.root.rel(rel_id).rows.max(1.0);
        crate::selfuncs::estimate_num_groups(run, &group_exprs, input_rows)?
    };
    Ok(Some(finish_knob_path(
        run,
        "distinctheap",
        "grouped-count-distinct-heap",
        heap_distinct_guard(),
        relid,
        ngroups,
        rel_rows,
        rel_pages,
    )?))
}

/// GL-LOWDIST-1: the runtime distinct sinks' low-width combine +
/// leader-parity bump is live (executor knob
/// `PGRUST_RUNTIME_DISTINCT_LOWWIDTH`, DEFAULT ON since the GL-LOWDIST-1
/// flip; kill spellings exactly `0|off`). Same spelling both crates — the
/// GROUPSINK coherence rule: the planner's re-derived low-dop routing and
/// the executor's combine strategy flip TOGETHER, so the kill restores the
/// pre-flip world byte-for-byte on both sides.
fn distinct_lowwidth_live() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_RUNTIME_DISTINCT_LOWWIDTH").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// GL-LOWDIST-1 re-derived floor for the INT-face distinct sinks' low-DOP
/// band (letter of record: scratchpad/night/GL-LOWDIST-1-letter.md; fleet
/// fix A/B @ a3d09b8ff, dop {2,4,8} x 1M-10M — the sink with the low-width
/// combine beats the forced-legacy GM+pardistinct hybrid at 23/24 cells:
/// grouped 0.67-0.96, plain-int 0.33-0.44, sole residual 5M-class dop8
/// grouped = 1.008 parity; it also beats SERIAL at every grouped cell).
/// min_dop 2: dop-1 stays keep-Gather (below the measured band; the
/// executor's leader-parity bump excludes dop 1 too). `base` = the
/// pre-flip provisional floor, restored verbatim when the LOWWIDTH kill is
/// set (kill-coherent routing).
fn distinct_lowwidth_guard(base: FloorGuard) -> FloorGuard {
    if distinct_lowwidth_live() {
        // min_dop 1 since the GL-LOWDIST-4 flips: Michael's B3 ruling =
        // SUPPRESS-TO-SINK at dop1 (the sink is 2.7-6.4x better than the
        // post-deletion per-tuple fall at every measured dop1 cell; the
        // remaining sink-vs-hybrid dop1 gap on grouped-int is the
        // GL-LOWDIST-5 car: "continue optimizing dop1 so it always wins").
        FloorGuard { min_dop: 1, low_dop_max_rows: 0.0, ..NO_GUARD }
    } else {
        base
    }
}

/// SE-TOPNNI (gap:topn-nonint-keys car, tier 2): bounded top-N whose ORDER
/// BY keys are NON-integer — date/timestamp Vars (the sink's
/// I4/I8 CmpOp aliases: plain int compares per date.c/timestamp.c) and/or
/// stitched deterministic-default-collation text/varchar Vars (the DictCode
/// key class, docs/design/dict-code-flow.md) — the census's bounded-top-N
/// over datetime/text sort keys with wide (star-tlist) payloads, qualed
/// and unqualed. The runtime sort SINK already owns every piece (KeyWidth
/// I4/I8 widening for the datetime family, v7 part-global byte-rank codes
/// for text, multi-key wide heaps, winner-only late materialization for
/// the star tlist, COLSTAGE staged accept + GCUT band predicate); only
/// this probe refuses the shapes. Smoke (2M rows, dop4, 2026-07-21):
/// qualed star-tlist / narrow-tlist / text-key / mixed-key analogs of the
/// census shapes engage and win 2.3–6.7x vs forced legacy, byte parity OK.
///
/// DEFAULT ON since the GL-TOPNNI-1 flip (scratchpad/night/
/// GL-TOPNNI-1-letter.md, dist witnessed ladder 2026-07-21 @ 8cf38a8c7:
/// 10M dop{4,8,16} all six keyed shapes suppressed-and-winning vs
/// best(serial, forced legacy), worst cell exact parity; 100M dop{4,16}
/// census-family wins everywhere). `PGRUST_LANE_V2_TOPN_NONINT=0|off` is
/// the kill, restoring the keep-Gather posture byte-for-byte (the
/// flipped-kill idiom — only the exact kill spellings disarm). Named
/// residual carried from the letter: the UNQUALED star-tlist shape at
/// 100M-class scale forgoes a <=1.23x serial-walk win while beating the
/// legacy Gather it displaces 10-30x — cost-route step-2 arbitration /
/// band-predicate early-exit term owns it. Suppresses via the knob-path
/// finish — NOT a BOOTSTRAP_MATRIX class (probe_key stays "-"; drift
/// guards untouched).
pub(crate) fn topn_nonint_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        tier2_car_kill_spelling_on(std::env::var("PGRUST_LANE_V2_TOPN_NONINT").as_deref().ok())
    })
}

/// Floor for the SE-TOPNNI knob path: the min_dop=4 rectangle, OWN COPY.
/// This was a verbatim reuse of the CbTopnBoundedIntKeys class_guard until
/// GL-COST-TOPN-1 guarded THAT class off (its four-posture grid @ 27db94812
/// refuted the int-key arm against forced Gather Merge at every cell) —
/// the reuse was SEVERED rather than inherited because the non-int car's
/// floor is backed by its OWN witnessed record (GL-TOPNNI-1: dist 10M
/// dop{4,8,16} all keyed shapes winning vs best engine, floor verdict
/// KEEP min_dop=4 — datetime/text keys, wide tlists, qualed shapes: a
/// different economics regime from the narrow int-key fixture that lost).
/// NAMED DEBT: GL-TOPNNI-1's ladder predates the four-posture vehicle —
/// its "best engine" reference should be re-witnessed with a forced-GM leg
/// (the E1 lesson) before this floor is next widened.
fn topn_nonint_guard() -> FloorGuard {
    FloorGuard { min_dop: 4, low_dop_max_rows: 0.0, ..NO_GUARD }
}

/// SE-TOPNNI selective-qual carve threshold (GL-TOPNNI-1 selective-qual x
/// datetime-lead diagnosis):
/// estimated qual-survivor fraction below which a QUALED shape with a
/// DATETIME (band-eligible) leading key enters the carve. Minting-era
/// anchors on the real 10M sorted bank (jobs 1784633628/-632/-634272 @
/// 34b23fdf2): the suppressed serial walk LOST at survival ~0.001 (2.9x)
/// and WON at ~0.75 — the carve kept Gather unconditionally below 0.10.
/// GL-RESIDUAL-2 re-adjudication: the losing cell is INVERTED at the
/// current tree at BOTH scales (the serial lane's scan+topn improved past
/// the starved zone walk while the kept plan's width-blind wide
/// projection got worse), so below the threshold the verdict is now
/// PRICED (`topnni_selqual_priced_enabled`) instead of unconditional.
const TOPN_NONINT_MIN_QUAL_SURVIVAL: f64 = 0.10;

/// GL-RESIDUAL-2 carve flip (DEFAULT ON): below the survival threshold
/// the star-wide class takes the fitted two-way verdict
/// (costsize::serial_model::topn_selqual_starwide_two_way — witnessed at
/// both scales: the serial walk wins 1.18x at the mid-scale bank,
/// 6-9x-and-growing at the census bank where the kept plan's cold cell is
/// the pathology); every OTHER shape in the carve region, and everything
/// below the model's support, keeps Gather exactly as before (abstain =
/// incumbent). `PGRUST_LANE_V2_TOPNNI_SELQUAL_PRICED=0|off` is the kill,
/// restoring the unconditional keep-Gather carve byte-for-byte (the
/// flipped-kill idiom).
fn topnni_selqual_priced_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        tier2_car_kill_spelling_on(
            std::env::var("PGRUST_LANE_V2_TOPNNI_SELQUAL_PRICED").as_deref().ok(),
        )
    })
}

/// SE-TOPNNI text sort-key answerability (the zerocnt-answerability
/// precedent, per column): the DictCode key class serves order ONLY via
/// the v7 part-global byte-rank stitch — a text key column without one
/// would engage and then CONTRACT-BREAK at accept (RG abort, R5 serial
/// rerun: the suppress-then-refuse trap). plancat stores the footer's
/// per-column stitch NDV when the knob is armed
/// (`RelOptInfo::pgrcolumnar_stitch_gndv`, 1-based attno = index + 1;
/// empty on footer-less/pre-v7 parts and at knob-off); 0/absent = keep
/// Gather.
fn topn_nonint_text_key_stitched(
    run: &PlannerRun<'_>,
    rel_id: types_pathnodes::RelId,
    varattno: i32,
) -> bool {
    varattno >= 1
        && run
            .root
            .rel(rel_id)
            .pgrcolumnar_stitch_gndv
            .get(varattno as usize - 1)
            .is_some_and(|&g| g > 0)
}

/// SE-MKTEXT (Lane-3 probe widening, two-key text car): the analytics-bank
/// int+text-class `GROUP BY UserID, SearchPhrase` shapes — TWO-key grouped
/// aggregation with one or two default-collation text keys — run 8-39x
/// slower unforced than on the hand-armed runtime agg pool (harvest3arm
/// t32 A/B @ 10M dist-control: 0.900s unforced vs 0.061s forced; the LIMIT
/// sibling 0.122 vs 0.015) because the probe refuses them at plan time while the
/// runtime agg SINK already owns the shapes end to end: the Mk composite
/// feed packs int+text keys (C2/Mk cars, canonical-bytes merge), the
/// canonical multi-tail encoding carries TWO Intern components
/// (canon-sink car 1, `PGRUST_RUNTIME_AGG_TEXT2`), and the bare-LIMIT
/// group-admission FREEZE owns the bare-LIMIT composition (band-2a,
/// `PGRUST_RUNTIME_AGG_FREEZE`). This knob keys the admission gaps.
///
/// DEFAULT ON (t35 routing-flips, GL-MKTEXT-1 FLIP-RECOMMENDED);
/// `PGRUST_LANE_V2_MULTIKEY_TEXT=0|off` is the kill switch — every other
/// spelling stays ON (the flipped-kill idiom: only the exact kill spellings
/// disarm). MEASURED (knob letter 2026-07-21, jobs -54df/-46fa @ 4479aae8d,
/// unforced 10M bank): two-key shape 0.861 -> 0.061 hot (14.1x, == the forced
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

/// SE-EXTRACTKEY (Lane-3 sibling, ts-extract exprkey class — the routing map's biggest
/// single probe win, 1.44s @ 10M): `GROUP BY UserID, extract(minute FROM
/// EventTime), SearchPhrase` — the probe's bare-Var key discipline refuses
/// the extract() expr key, yet the SERIAL-lane exprkey Multi arm ALREADY
/// OWNS execution (exprkey.rs `decide_exprkey_mk`: one computed
/// NUMERIC-returning chain key + bare int/text Vars, `int8 + numeric4 +
/// intern4 = 16` — THE ts-extract shape; the forced arm ran mpwpg=0 with NO pools
/// at 0.088s vs 1.529s legacy-parallel). Suppression-only widening: the
/// knob keys the shape via `classify_extract_exprkey`, the suppressed
/// serial `[Limit<-Sort<-]HashAgg<-SeqScan` plan engages the Multi feed.
/// DEFAULT ON (t35 routing-flips); `PGRUST_LANE_V2_EXPRKEY_EXTRACT=0|off`
/// is the kill switch. GL-EXTRACTKEY-1 (2026-07-21, jobs -54df/-46fa/-75c3
/// @ 4479aae8d) measured the knob safe everywhere (zero deltas across 43q)
/// but held by the then-1e6 groupby_high floor: the shape's estimate is 1,516,181
/// (== the two-key base shape's — the extract() key adds nothing), and with the hold
/// bypassed the arm runs the shape at 0.093 hot (16x, forced ref 0.088). The
/// floor's raise to 4e6 LANDED (b12c3fc74, bench letter in-commit), so the
/// re-letter-together-with-the-floor condition is met and the flip engages
/// the shape unforced.
fn extract_exprkey_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        knob_spelling_on(std::env::var("PGRUST_LANE_V2_EXPRKEY_EXTRACT").as_deref().ok())
    })
}

/// SE-CONSTKEY (Lane-3 sibling, const-tlist-key class, 2.07s @ 10M): `SELECT 1, URL,
/// count(*) … GROUP BY 1, URL` — the const group key fails `key_var` and
/// the const tlist entry fails the emit discipline (the named const-tlist refusal;
/// matrix row agg-const-tlist). The forced arm (serial + pools) wins 10x,
/// so an engagement exists to key; the const contributes nothing to the
/// partition. The knob admits NON-NULL INT-FAMILY Const group keys (and
/// their tlist entries) alongside the existing bare-Var census — the REAL
/// keys still drive classification and floors. DEFAULT ON (t35
/// routing-flips); `PGRUST_LANE_V2_AGG_CONSTKEY=0|off` is the kill switch.
/// GL-CONSTKEY-1 (2026-07-21, jobs -54df/-46fa/-75c3 @ 4479aae8d) measured
/// the knob safe everywhere (zero deltas across 43q) but held by the
/// then-1e6 groupby_high floor: the shape's estimate is 2,625,920 (all URL — the
/// const key contributes nothing), and with the hold bypassed the arm runs
/// the shape at 0.227 hot (9.4x — BEATS the forced ref 0.237). The floor's raise
/// to 4e6 LANDED (b12c3fc74), so the flip engages the shape unforced.
fn agg_constkey_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        knob_spelling_on(std::env::var("PGRUST_LANE_V2_AGG_CONSTKEY").as_deref().ok())
    })
}

/// SE-BARELIMIT (Lane-3 sibling, bare-LIMIT-composition class, 0.11s @ 10M):
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
/// -54df/-46fa @ 4479aae8d): the composition 0.124 -> 0.016 hot (7.8x, forced ref
/// 0.015) via the freeze composition with MKTEXT; zero regressions.
/// TIE-CLASS NOTE (per the letter): bare-LIMIT-no-ORDER-BY answers change
/// to a different VALID group subset (a PASS-TIE class) — callers snapshotting
/// raw bytes will see a change; the tie law accepts it.
fn agg_barelimit_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        knob_spelling_on(std::env::var("PGRUST_LANE_V2_AGG_BARELIMIT").as_deref().ok())
    })
}

/// PROVISIONAL floor for the SE-EXTRACTKEY knob path: the shared
/// text-keyed-grouped economics (the ts-extract shape carries a text key too).
/// GL-EXTRACTKEY-1 owns re-measuring.
fn extract_exprkey_guard() -> FloorGuard {
    FloorGuard { min_dop: 12, low_dop_max_rows: 3_000_000.0, ..NO_GUARD }
}

/// GL-ELECT22-1 fix 4b — extract-exprkey winner-selection hold exemption
/// (`PGRUST_M5_EXTRACTKEY_TOPN_HIGHGROUPS`).
/// `classify_extract_exprkey` applies the §10 hold with NO
/// top-N exemption (fail-closed by design when the path landed): at the
/// pinned 100M census the ts-extract family estimates 17,614,259 groups
/// (== the two-key base shape's — the extract key adds nothing to the
/// estimate) and refuses, while the forced series proves the engaged arm
/// wins the exact shape (1.076/1.025 hot, 26x; the GL-EXTRACTKEY-1
/// mid-scale record banked 16x with the hold bypassed). Knob-ON
/// transplants the TOPN-HIGHGROUPS conditions the arm's composition
/// supports — bounded agg-sort top-N, sort key in the finalfn-free
/// int8-transvalue set (count(*) at the census shape), Const bound
/// within the shared sink cap — under its OWN fail-closed ceiling: the
/// suppressed serial plan drains every group into the bounded sort
/// (full-drain economics, NOT the winners-only grouped-sink bypass), so
/// the exemption stays witnessed-band only.
///
/// DEFAULT ON (GL-ELECT22-1 flip; `=0|off` kills): 100M witnessed pair
/// @ c9eb09e803/240b738c9 (jobs -4b6f/-5f1a vs OFF baseline -5ca6) —
/// knob-ON suppresses at ngroups=17,614,259 (under the 24M ceiling;
/// label extract-exprkey-grouped-topn-highgroups), hot 1.052-1.139 vs
/// the 1.076 forced recovery bound, byte parity across arms.
fn extractkey_topn_highgroups_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        tier2_car_kill_spelling_on(
            std::env::var("PGRUST_M5_EXTRACTKEY_TOPN_HIGHGROUPS").as_deref().ok(),
        )
    })
}

/// Fix-4b group-estimate ceiling (env-overridable
/// `PGRUST_M5_EXTRACTKEY_TOPN_MAX_GROUPS`, the ladder's sweep vehicle):
/// PROVISIONAL 24M — the census family estimate (17.61M) +
/// estimate-wobble headroom, aligned with the mk-text ceiling-refit band
/// (both families share the same base-key estimate); below the
/// unladdered 32M-class rung. GL-ELECT22-1's ladder owns the bound.
fn extractkey_topn_max_groups() -> f64 {
    static CEIL: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *CEIL.get_or_init(|| {
        std::env::var("PGRUST_M5_EXTRACTKEY_TOPN_MAX_GROUPS")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| *v > 0.0)
            .unwrap_or(24_000_000.0)
    })
}

/// The shared default-OFF arming rule (the SE-SCANPASS / K1-latemat idiom,
/// factored pure for the conversion-car lanes): ON iff the value is exactly `1`
/// or `on`; every other spelling — unset, `0`, `off`, typos — fails safe to
/// OFF (today's behaviour, byte-identical plan time).
fn knob_spelling_armed(v: Option<&str>) -> bool {
    matches!(v, Some("1") | Some("on"))
}

/// SE-DECOROOT (the GL-DECOROOT-1 lane, conversion-scope car 1 — the decorated-root blocker): decorated-root
/// composition. Every grouped probe class is Agg-root-only today, which
/// gates 17 of the 20 Gather-carrying census queries (ORDER BY / LIMIT / OFFSET
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
///
/// DEFAULT ON (conversion-flips train; GL-DECOROOT-1 FLIP-RECOMMENDED, campaign
/// of record scratchpad/night/fleet-ab-parallelism.md 2026-07-21, campaign
/// rig snapshots (see the letter file)): decorated grouped SCANS
/// win 1.4-5.2x vs legacy Gather at dop {4,8,16} (d3/l300/l24/l16 ladder);
/// decorated JOIN tops inherit the underlying default-ON aggjoin class's
/// economics exactly (the rider adds ~nothing — that class's own dop-8/16
/// 0.89x-vs-legacy trait pre-exists this car); the 16x hash-election
/// margin VALIDATED (the 16-rows/group boundary still wins 1.37-3.48x; 12
/// refuses by name); parity everywhere, default arm inert, the full
/// census byte-stable, the row-suite census flat (single scan-grouped-decorated
/// keying ~parity; the grouped topn-offset upside is 100m-scale
/// territory — the 10m bank margin correctly refuses). `PGRUST_LANE_V2_DECOROOT=0|off` is the kill switch
/// (t35 exact-spelling law).
fn decoroot_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        knob_spelling_on(std::env::var("PGRUST_LANE_V2_DECOROOT").as_deref().ok())
    })
}

/// SE-DECOROOT hash-election margin (PROVISIONAL, GL-DECOROOT-1 owns the
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

/// SE-NUMJOIN (the GL-NUMJOIN-1 lane, conversion-scope car 3 join half — the numeric agg-expr blocker): numeric agg-expr probe vocabulary. The
/// runtime-partial NumericAgg/Int128 state relocation LANDED (SE-AGGPOLY:
/// exact digit snapshots, C numeric_avg_combine field law) and the agg-poly
/// matrix row records "the aggjoin seam's export is ready via the shared
/// runtime-partial vocabulary once its probe admits numeric args" — the
/// blocker for the sum(price*(1-disc)) money-expression family (13
/// census queries carry it) is the probe whitelist, not the kernel. This knob
/// admits structurally plain sum/avg(NUMERIC) aggregates over ONE
/// parallel-safe argument expression (the heap-poly precedent: the join
/// arms run C's checked evaltrans transition program per emitted row, so
/// the arg SHAPE is free; helper-side safety is the planner's own
/// is_parallel_safe) into the JOIN-side classifiers. The grouped-over-SCAN
/// half stays REFUSED — the agg-poly row names its real gap (the lanetable
/// sink combine topology perf car), so GROUPED_SINK_AGGS is untouched.
///
/// DEFAULT ON (conversion-flips train; GL-NUMJOIN-1 FLIP-RECOMMENDED,
/// fleet-ab-parallelism.md 2026-07-21): the numeric evaltrans win is
/// THICK, not thin — grouped join 1.96-3.17x, the plain money-join
/// core 1.78-3.07x, the composed grouped core 1.81-3.03x vs legacy PHJ, thicker than
/// the int-fold analog because legacy numeric transitions cost more per
/// row (the scoping's "thinner than int" caveat is REFUTED on the class
/// fixture). One thin spot, NAMED: multibuild-numeric at 6M rows is
/// ~parity (engaged, byte-correct, no win — not a regression). Byte-exact
/// numeric results on every leg; zero refusal-guard trips; default arm
/// inert. `PGRUST_LANE_V2_AGGJOIN_NUMERIC=0|off` is the kill switch (t35
/// exact-spelling law).
fn aggjoin_numeric_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        knob_spelling_on(std::env::var("PGRUST_LANE_V2_AGGJOIN_NUMERIC").as_deref().ok())
    })
}

/// SE-JHEAP (the GL-JHEAP-1 lane, conversion-scope car 2 — the heap-side join blocker):
/// heap-side join admission. Every join classifier admitted cbstore rels
/// ONLY ('side not cbstore' — the pure-shape census refusal), while the
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
/// the NL-margin law).
///
/// DEFAULT ON (conversion-flips train; GL-JHEAP-1 FLIP-RECOMMENDED,
/// fleet-ab-parallelism.md 2026-07-21): heap-side joins engage 2.1-3.1x
/// vs legacy (grouped-int 2.3-2.8x, multibuild star 2.4-2.9x, the pure
/// plain-join money shape 2.6-3.1x, the composed grouped flagship 2.5-2.9x); JHEAP_NL_MARGIN=4
/// VALIDATED per-edge (8x and 4.0x-boundary partners engage full-thick,
/// 2x refuses by name — headroom, no loss at 4); the [1M,2M] row floor
/// enforced at scale (800k/3M refuse, 1.5M engages). NAMED caveats: at
/// bank census scale the car is INERT — the pure plain-join census query
/// plans NL+index under legacy (66ms hot), NOT a Gather hash join, so its
/// conversion belongs to the NL-inner-index follow-on family; the
/// agg-ratio emit shape stays a named gap. `PGRUST_LANE_V2_JHEAP=0|off` is the kill
/// switch (t35 exact-spelling law).
fn jheap_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        knob_spelling_on(std::env::var("PGRUST_LANE_V2_JHEAP").as_deref().ok())
    })
}

/// SE-JHEAP executor coherence (the m5p1 `multibuild_enabled` precedent):
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

/// SE-JHEAP NL/merge-election margin (PROVISIONAL, GL-JHEAP-1 owns the
/// measured bound): an index on a heap rel's JOIN-KEY column makes the
/// post-suppression serial planner's NL-with-inner-index (and index-sorted
/// merge) shapes electable — plans the join walk refuses (the B1/X5/X6
/// suppress-then-refuse class the scoping named). NL(outer=X,
/// inner=IndexScan(Y)) beats hash only when X is comparable to or smaller
/// than Y (per-probe index cost vs the one-pass hash build); requiring
/// EVERY equi-partner of a join-key-indexed heap rel to carry at least
/// this many times its rows keeps the hash election safely dominant.
const JHEAP_NL_MARGIN: f64 = 4.0;

/// GL-COST-2 x conversion-flips-train merge carve: the flipped knob paths
/// (aggjoinnum / decoroot-grouped / joinfilters cbstore-int) borrowed the
/// RIDER guards as their provisional floor; the GL-COST-2 unwire zeroed
/// those rider rectangles on the riders' OWN witnessed grids (pure-
/// bootstrap int-key shapes, rt/legacy 3.0-6.4x), but the knob paths'
/// shapes were measured SEPARATELY by their GL letters (NUMJOIN-1 /
/// DECOROOT-1 / FILTERQUALS-1, all FLIP-RECOMMENDED with wins) — so they
/// RETAIN the pre-unwire hashjoin-nbatch1 2M rectangle under their own
/// name. Those letters own re-measuring it; the riders' zeroed guard
/// governs only the finish(rider-class) bootstrap path.
fn hj_knobpath_2m_guard() -> FloorGuard {
    FloorGuard { max_rows: 2_000_000.0, ..NO_GUARD }
}

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

/// SE-CBKEYS (the GL-CBKEYS-1 lane): canonical-bytes join-key admission —
/// the grouped-JOIN sink's key vocabulary was word-only (byval int-family)
/// while the SCAN sinks already run canonical-bytes text keys (the C3
/// machinery, agg-text-canonical-bytes row). This knob admits bare
/// text/varchar group keys under the deterministic DEFAULT collation into
/// the grouped-join census (the sink's export/combine/absorb carry the
/// detoasted content bytes — byte equality IS texteq's verdict, the
/// `group_eq_representational` law). BPCHAR is the NAMED REFUSAL of
/// record: its space-stripping bpchareq and trailing-blank representative
/// ties sit outside the byte-equality envelope — exactly why the scan
/// sinks exclude it — so the census' char(n) keys stay refused until a
/// bpchar tie-law car rules on canonicalization.
///
/// DEFAULT ON (conversion-flips train; GL-CBKEYS-1 FLIP-RECOMMENDED,
/// fleet-ab-parallelism.md 2026-07-21): varchar/text canonical-bytes join
/// keys engage 2.55-3.07x vs legacy on the varchar-keyed composed core,
/// identically in the 4- and 5-car arms (word shapes never re-route; the
/// bytes lane is stable); bpchar refuses BY NAME without the sub-knob (52
/// pinned traces); COLLATE/stats laws held; parity 15/15; the full and
/// row-suite censuses byte-stable/flat. `PGRUST_LANE_V2_CBKEYS=0|off` is the kill switch
/// (t35 exact-spelling law).
fn cbkeys_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        knob_spelling_on(std::env::var("PGRUST_LANE_V2_CBKEYS").as_deref().ok())
    })
}

/// SE-BPCHAR (the GL-BPCHAR-1 lane) — the bpchar TIE-LAW sub-gate of the
/// cbkeys car (both knobs must be armed; same pair read in the executor —
/// knob coherence). The ruling of record, proven against the vendored
/// varchar functions (tie-law corpus in that crate's tests): stored
/// `char(n)` values carry EXACTLY n characters (bpchar_input/recv and the
/// length-coercion cast pad, or truncate trailing spaces only), and
/// bpchareq is texteq over the trailing-0x20-byte trim (multibyte-safe:
/// server encodings keep non-first bytes high-bit-set), so for BARE-VAR
/// keys of one column (same typmod by construction):
/// equal-under-bpchareq <=> byte-identical stored images. The canonical
/// bytes ARE the stored bytes; no trailing-blank representative tie
/// exists between equal keys. Guards at the admission: bare Vars only
/// (exprs — substr() etc. — break the padding invariant and refuse via
/// the bare-Var census), vartypmod >= 5 (`char(n)`, n >= 1 — typmod-less
/// bpchar stores unpadded and stays a named refusal), deterministic
/// DEFAULT collation. The absorb-side `!isnew` backstop remains defense
/// in depth (a non-canonical image refuses to the serial rerun), not the
/// argument.
///
/// DEFAULT ON (conversion-flips train; GL-BPCHAR-1 FLIP-RECOMMENDED,
/// fleet-ab-parallelism.md 2026-07-21): the K-text unlock on skeletons —
/// the char(25)-keyed five-car skeleton 2.77-3.32x, the char(10)-keyed
/// skeleton (the trailing-blank tie adversary in the fixture) 2.56-3.09x,
/// multibyte char(8) 2.21-2.65x vs legacy; the tie-law PRODUCTION canary
/// at SF10 (3 char(n) GROUP BY shapes x both postures x both arms) is ALL
/// byte-identical; typmod-less bpchar refuses by name (13 traces);
/// sub-knob OFF restores the cbkeys refusal byte-for-byte. `PGRUST_LANE_
/// V2_CBKEYS_BPCHAR=0|off` is the kill switch (t35 exact-spelling law).
fn bpchar_keys_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        knob_spelling_on(std::env::var("PGRUST_LANE_V2_CBKEYS_BPCHAR").as_deref().ok())
    })
}

/// SE-FILTERQUALS (the GL-FILTERQUALS-1 lane) — the X5 relaxation, the top
/// remaining conversion blocker after the five-car stack: the grouped-join
/// classifier's BARE-EQUI-ONLY law refuses every filtered census text
/// (the filtered census cores’ region/date and shipmode/date restriction quals). The EXECUTOR was
/// never the gap — the join walks parallel-safety-check scan quals
/// (mb_plan_walk + the single-join gates) and the worker RowFeed re-checks
/// them per row; X5 was probe conservatism against the merge-election
/// hazard its own e2e reproduced LIVE (a filter with a stats-defaulting
/// EXPR selectivity shifted the costing to a top-level Merge Join). This
/// knob admits per-rel PUSHED filter terms under a STATS-GROUNDED
/// discipline (classify_filter_term): single-rel simple restrictions only
/// — (possibly Relabel'd) stats'd Var op non-null Const, or a
/// ScalarArrayOp (IN) of the same shape — parallel-safe, so the planner's
/// selectivity is grounded in pg_statistic rather than defaults; the
/// stats-defaulting expr class that drove X5 (`f.v % 3 = 0`) stays a
/// named refusal, as do var-var terms (same-rel column-column date
/// compares — no grounding). Post-filter estimates flow into the
/// floors and the per-edge NL margins automatically (RelOptInfo.rows is
/// post-restriction at the probe's choke point), so filtered builds
/// cannot out-run the election evidence.
///
/// DEFAULT ON (conversion-flips train; GL-FILTERQUALS-1 FLIP-RECOMMENDED,
/// fleet-ab-parallelism.md 2026-07-21): the first FILTERED grouped-join
/// engagements — the six-car filtered composition 2.36-2.85x, the
/// SAOP-filtered core 2.50-3.01x, the stats-grounded restriction halves
/// 1.39-1.88x vs legacy; the stats-defaulting expr term NEVER keys
/// (suppress-then-refuse channel closed live at scale); the selectivity
/// ladder (90..1% x indexed/unindexed dims) found NO keyed point losing
/// to the serial costing's election — below the knob's own suppression
/// region the legacy costing itself elects SERIAL plans where the
/// default-ON serial-shaped runtime path already engages in both arms and
/// beats that election 3.7-8.2x — so NO selectivity floor is carried (the
/// ladder's explicit verdict). `PGRUST_LANE_V2_JOINFILTERS=0|off` is the
/// kill switch (t35 exact-spelling law). NOT touched: the EC-tree law (the six-rel census core’s shared
/// nation-key endpoint is the hostile-proven H1/H2 hazard — a correctness guard,
/// not conservatism) and the plain rows' pre-existing wider admission.
fn joinfilters_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        knob_spelling_on(std::env::var("PGRUST_LANE_V2_JOINFILTERS").as_deref().ok())
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
///
/// GL-ELECT22-1 fix 1 — CEILING REFIT (`PGRUST_M5_MKTEXT_CEIL_V2`): the 16M
/// provisional was derived at the mid-scale fixture and never witnessed at
/// the full-scale bank, where the two-key int+text census family's own
/// plan-time estimate is 17,614,259 (census pgrust-cb-standard-1784678138
/// -1176 @ 307329686) — a 10% miss that refuses BOTH family compositions
/// (agg-sort top-N and the bare-LIMIT freeze) on exactly the population the
/// family exists for. The forced series at the same sha proves the arm wins
/// there (top-N 0.632/0.594 hot, freeze 0.134/0.132 — vs legacy-parallel
/// tens of seconds). v2 lifts the DEFAULT ceiling to the refit band bound
/// (24M: the census family estimate + estimate-wobble headroom, below the
/// unladdered 32M-class rung); the explicit env override still wins over
/// both defaults (the ladder's sweep vehicle).
///
/// DEFAULT ON (GL-ELECT22-1 flip; `=0|off` kills — the flipped-kill
/// idiom): 100M witnessed pair @ c9eb09e803/240b738c9 (jobs -4b6f/-5f1a
/// vs OFF baseline -5ca6): the bank reproduces the census family
/// estimate EXACTLY (17,614,259); knob-ON suppresses BOTH compositions
/// with hot 0.638-0.676 (topn; forced bound 0.632) and 0.144-0.147
/// (freeze; bound 0.134), byte parity across arms, engine pinned. The
/// >24M band stays unladdered — the ceiling holds there (fail closed).
fn mktext_ceil_v2_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        tier2_car_kill_spelling_on(std::env::var("PGRUST_M5_MKTEXT_CEIL_V2").as_deref().ok())
    })
}

/// The mk-text family ceiling DEFAULT, pure for unit tests: the 16M
/// provisional, or the GL-ELECT22-1 refit bound with the v2 knob armed.
fn mktext_family_ceiling_default(ceil_v2: bool) -> f64 {
    if ceil_v2 { 24_000_000.0 } else { 16_000_000.0 }
}

fn multikey_text_max_groups() -> f64 {
    static CEIL: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *CEIL.get_or_init(|| {
        std::env::var("PGRUST_LANE_V2_MULTIKEY_TEXT_MAX_GROUPS")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| *v > 0.0)
            .unwrap_or_else(|| mktext_family_ceiling_default(mktext_ceil_v2_enabled()))
    })
}

/// SE-T2AGG CAR B group-estimate ceiling (GL-STRMM-2 flip calibration): at
/// or above this many estimated groups the string-min/max suppression
/// REFUSES — the witnessed A-B-A ladder banked a 1.25x LOSS for the engaged
/// sink at the ~1e5-group band (the serial hash lane wins there) against
/// 1.5-2x wins at <= 1e3 groups and int keys; the ceiling sits at the
/// conservative end of the letter's named band so the losing band routes to
/// the planner's own choice. Env-overridable for floor-recalibration ladders
/// (`PGRUST_LANE_V2_AGG_STRMINMAX_MAX_GROUPS`, > 0).
fn strminmax_max_groups() -> f64 {
    static CEIL: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *CEIL.get_or_init(|| {
        std::env::var("PGRUST_LANE_V2_AGG_STRMINMAX_MAX_GROUPS")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| *v > 0.0)
            .unwrap_or(30_000.0)
    })
}

/// Dict-key-class group-estimate ceiling (GL-HEAVYTIER-1 hold disposition
/// D1, coordinator-approved): the dict-key expr-key classifier carries its
/// OWN ceiling instead of the shared groupby_high floor — the class's
/// engaged sink is the witnessed winner far above 4e6 (same-pod cell on the
/// production-scale sorted bank: engaged 0.44-0.50 s vs 6.7-6.8 s, byte
/// parity; the class letter carries the ladder).
/// PROVISIONAL default 30e6: the ceiling compares against THIS
/// classifier's `estimate_num_groups` over the RAW computed-key clause —
/// 19,897,461 at the banked production-scale cell (suppress-trace figure
/// of record; the raw text key keeps passthrough NDV, so this is ~3x the
/// plan-displayed grouped-path estimate — the two estimators do NOT
/// agree, calibrate against the trace, not the plan). 30e6 = the trace
/// figure + wobble headroom, below unladdered territory (the fix-4a
/// ceiling precedent). Every OTHER classifier keeps the shared floor
/// byte-for-byte. Env-overridable for recalibration ladders
/// (`PGRUST_M5_DICTKEY_MAX_GROUPS`, > 0).
fn dictkey_max_groups() -> f64 {
    static CEIL: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *CEIL.get_or_init(|| {
        std::env::var("PGRUST_M5_DICTKEY_MAX_GROUPS")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| *v > 0.0)
            .unwrap_or(30_000_000.0)
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
pub(crate) const PLAIN_FOLD_AGGS: &[u32] = &[
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

/// AGG_INTCASE (int-CASE fold-args car): the int-family plain aggregates
/// whose transition STATE is an exportable runtime-partial kind regardless
/// of how the argument was evaluated — PLAIN_FOLD_AGGS minus the zero-arg
/// count(*) (bare-whitelist territory). MIRROR of nodeagg
/// runtime_partial.rs `intcase_perrow_kind` (fail-closed both sides; a
/// probe admission the manifest refuses would land on serial — the e2e's
/// engagement legs pin the pair).
const INTCASE_POLY_AGGS: &[u32] = &[
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

/// GROUPED-AVG widening (probe-side): `GROUPED_SINK_AGGS` plus
/// avg(int2)/avg(int4). The historical exclusion note above ("PolyInt128/
/// NumericAgg states are walk refusals on the grouped path") is STALE for
/// these two OIDs: the runtime grouped sink's combine resolution
/// (`sink_resolve_combines`, nodeagg sink.rs) admits the `_int8[2]`
/// {count,sum} transarray through `int4_avg_combine` UNCONDITIONALLY (the
/// AvgInt8/AvgInt8Packed classes; both finalize at emit, nothing
/// pointer-shaped reaches the leader), and the serial-shaped router path
/// engages the avg-carrying grouped top-N shapes end-to-end today. The
/// INTERNAL-transtype family (avg(int8)/sum(int8), PolyInt128) stays
/// excluded here — the sink admits it but the probe widening is unproven
/// for it; a named follow-up owns that pair.
const GROUPED_SINK_AGGS_AVG: &[u32] = &[
    F_COUNT_STAR,
    F_COUNT_ANY,
    F_SUM_INT4,
    F_SUM_INT2,
    F_AVG_INT4,
    F_AVG_INT2,
    F_MAX_INT8,
    F_MAX_INT4,
    F_MAX_INT2,
    F_MIN_INT8,
    F_MIN_INT4,
    F_MIN_INT2,
];

/// GROUPED-AVG knob (`PGRUST_M5_GROUPED_AVG`): DEFAULT ON (open-rows
/// flip train, GL-OPENROWS-AVG FLIP-RECOMMENDED as the car1+car2 PAIR —
/// fleet letter 2026-07-21, 10M unforced pair jobs -6edd/-533a: the
/// qualed/unqualed two-int-key winner-selection census rows land 5.4x/
/// 5.3x AT the forced ceiling with per-leg suppression witnesses; 100M
/// alone is FLAT within noise, zero knob-tax, because the shapes' group
/// estimates cross the §10 hold there — the PAIR flip with the
/// winner-selection hold exemption below is the scale recipe, composition
/// leg -2250: 8.0x/73x at the ceilings). Probe-side only: no executor
/// change rides this knob (the sink vocabulary above is live at default),
/// so the suppress-then-refuse direction is guarded by the sink's own
/// fail-closed combine resolution. `PGRUST_M5_GROUPED_AVG=0|off` is the
/// kill (the flipped-kill idiom: unset and every other spelling stay ON).
fn grouped_avg_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        tier2_car_kill_spelling_on(std::env::var("PGRUST_M5_GROUPED_AVG").as_deref().ok())
    })
}

/// The grouped-sink passenger vocabulary of record: base list, or the
/// avg-of-int widening knob-ON.
fn grouped_sink_aggs() -> &'static [u32] {
    if grouped_avg_enabled() { GROUPED_SINK_AGGS_AVG } else { GROUPED_SINK_AGGS }
}

// ---------------------------------------------------------------------------
// stragg-coverage inc-1 (GL-STRAGG-2): the LENARG + HAVING cars.
// ---------------------------------------------------------------------------

/// LENARG car knob (`PGRUST_LANE_V2_AGG_LENARG`): **DEFAULT ON** since the
/// GL-STRAGG-2 flip (t43; letter FLIP-RECOMMENDED both cars together —
/// zero structural tax, byte parity everywhere measured; the q28
/// forced-serial residual belongs to the presorted serial-agg program,
/// not these cars). Kill spellings exactly `0|off` (the t35 flipped-kill
/// idiom). Probe-side only — the executor already evaluates the
/// textlen-family agg arguments through the staged per-column length
/// lanes (lanefold `classify_len_arg`).
fn agg_lenarg_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        tier2_car_kill_spelling_on(std::env::var("PGRUST_LANE_V2_AGG_LENARG").as_deref().ok())
    })
}

/// HAVING car knob (`PGRUST_LANE_V2_AGG_HAVING`): **DEFAULT ON** since the
/// GL-STRAGG-2 flip (t43; both cars flip together per the letter). Kill
/// spellings exactly `0|off`. SAME spelling as the runtime grouped sink's
/// emit filter (`nodeagg::sink::sink_having_enabled`) — both seams flip
/// together (knob-coherence law: a probe that suppressed a HAVING shape
/// the sink's filter compile refuses would land it on the serial rerun).
fn agg_having_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        tier2_car_kill_spelling_on(std::env::var("PGRUST_LANE_V2_AGG_HAVING").as_deref().ok())
    })
}

/// DICTKEY car knob (`PGRUST_LANE_V2_AGG_DICTKEY`, GL-DICTDRAIN-1):
/// DEFAULT OFF, armed iff exactly `1`/`on`. SAME spelling as the executor
/// half (`exprkey::dictkey_sink_enabled` in execmain — the sink admission
/// of the Dict expr-key kind through the 1-Intern compact spec): both
/// seams flip together (knob-coherence law — a probe that suppressed a
/// dict-key shape the sink refuses would land it on the serial rerun;
/// note the serial rerun for THIS class is the engaged serial expr-key
/// fold feed, not the per-row world — the containment price is the
/// serial-fold wall, measured in the GL-DICTDRAIN-1 ladder).
/// DEFAULT ON since the A-on-top-of-B ruling (Michael, 2026-07-22:
/// "unless A is good on top of B — then let's do both a and b"; the
/// flipped-kill idiom — `=0|off` kills). The census-scale holds
/// (groupby_high floor, strminmax ceiling) are UNCHANGED — at defaults
/// the car keys only the sub-hold estimate band.
fn agg_dictkey_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        tier2_car_kill_spelling_on(std::env::var("PGRUST_LANE_V2_AGG_DICTKEY").as_deref().ok())
    })
}

/// DICTKEY engine-kill coherence (suppress-then-refuse guard): the drain
/// rides the serial-lane expr-key feed (`PGRUST_LANE_V2_EXPRKEY`) and the
/// canonical text car (`PGRUST_RUNTIME_AGG_TEXT` — `mk_shape_sink_ok`'s
/// 1-Intern gate). Either executor kill must gate the probe keying too.
fn dictkey_engine_live() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        exprkey_engine_live()
            && !matches!(
                std::env::var("PGRUST_RUNTIME_AGG_TEXT").as_deref(),
                Ok("0") | Ok("off")
            )
    })
}

/// The aggregates whose ONE argument slot is int4-typed — the only place
/// the lanefold fold-arg vocabulary consults the textlen family
/// (`classify_arg` admits `classify_len_arg` for INT4-expected args only;
/// count's arg reads the isnull lane, and the strict len funcs make the
/// result's NULL-ness the Var's). Function resolution guarantees the arg
/// type matches the fnoid, so fnoid membership IS the arg-slot type check.
const LEN_ARG_HOST_AGGS: &[u32] =
    &[F_COUNT_ANY, F_SUM_INT4, F_AVG_INT4, F_MIN_INT4, F_MAX_INT4];

/// A textlen-family agg ARGUMENT the lane fold vocabulary proves
/// (`length(v)`/`char_length(v)`/`octet_length(v)` over a bare
/// text/varchar Var on the scanned rel, varchar riding the binary-coercion
/// relabel — `classify_str_var`'s shape at parse altitude). The funcid +
/// encoding half lives in lanefold (`len_arg_funcid_admits`) so the two
/// seams share one table; collation is irrelevant to length semantics.
fn is_lanefold_len_arg(expr: Node<'_>, rti: usize) -> bool {
    let Some(f) = expr.as_func_expr() else { return false };
    if f.funcretset || f.args.len() != 1 || !::lanefold::len_arg_funcid_admits(f.funcid) {
        return false;
    }
    let arg = f.args.nth(0);
    let arg = match arg.as_relabel_type() {
        Some(r) if r.resulttype == TEXTOID => r.arg,
        Some(_) => return false,
        None => arg,
    };
    is_covered_key_var(arg, rti, |t| t == TEXTOID || t == VARCHAROID)
}

/// [`is_whitelisted_agg`] with the LENARG widening: the single argument may
/// be a lanefold-proven textlen-family expression instead of a bare Var,
/// for whitelist members whose arg slot is int4-typed. Decoration law is
/// `aggref_plain_typed`'s verbatim; zero-arg forms stay bare-whitelist
/// territory (this fn admits ONLY the widened 1-arg shape — callers try
/// the bare-Var whitelist first).
fn is_whitelisted_agg_lenarg(expr: Node<'_>, rti: usize, whitelist: &[u32]) -> bool {
    let Some(agg) = expr.as_aggref() else { return false };
    if !whitelist.contains(&agg.aggfnoid) || !LEN_ARG_HOST_AGGS.contains(&agg.aggfnoid) {
        return false;
    }
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
    if agg.args.len() != 1 {
        return false;
    }
    let Some(arg_tle) = agg.args.nth(0).as_target_entry() else { return false };
    is_lanefold_len_arg(arg_tle.expr, rti)
}

/// The int8-family comparison operator FUNCTIONS the HAVING car admits —
/// MIRROR of the runtime sink's `having_cmp_of` (nodeagg sink.rs; the
/// canonical fmgr rows: 467-472 int8×int8, 474-479 int8×int4, 852-857
/// int4×int8). Every fn compares exact signed values, which is what the
/// emit filter's widened-i64 comparison computes.
const HAVING_CMP_FNS: &[u32] = &[
    467, 468, 469, 470, 471, 472, // int8 eq/ne/lt/gt/le/ge
    474, 475, 476, 477, 478, 479, // int84
    852, 853, 854, 855, 856, 857, // int48
];

/// The ONE havingQual form the HAVING car admits — MIRROR of the runtime
/// sink's emit-filter compile (`nodeagg::sink::having_emit_filter`,
/// fail-closed both sides): a single OpExpr comparison between a bare
/// undecorated `count(*)` and a non-null int-family Const. count(*) is
/// rel-independent (no Var), so no rti is consulted; a count trans
/// initializes non-null 0 and carries no finalfn, so the filter's
/// transvalue read IS the finalized value.
///
/// By probe time subquery preprocessing has rewritten havingQual into the
/// implicit-AND List form (grouping.rs reads it as a list wholesale), so
/// the term is unwrapped from a ONE-element list; the pre-preprocess
/// expression form is accepted too (belt — the probe runs post-preprocess).
fn having_term_admissible(hq: Node<'_>) -> bool {
    let hq = match hq.as_list() {
        Some(l) => {
            if l.len() != 1 {
                return false;
            }
            l.nth(0)
        }
        None => hq,
    };
    let Some(op) = hq.as_op_expr() else { return false };
    if op.opretset || op.args.len() != 2 || !HAVING_CMP_FNS.contains(&op.opfuncid) {
        return false;
    }
    let (a, b) = (op.args.nth(0), op.args.nth(1));
    let (aggside, constside) = if a.as_aggref().is_some() { (a, b) } else { (b, a) };
    let Some(agg) = aggside.as_aggref() else { return false };
    if agg.aggfnoid != F_COUNT_STAR
        || !agg.args.is_nil()
        || agg.agglevelsup != 0
        || agg.aggkind != AGGKIND_NORMAL
        || agg.aggvariadic
        || !agg.aggorder.is_nil()
        || !agg.aggdistinct.is_nil()
        || agg.aggfilter.is_some()
        || !agg.aggdirectargs.is_nil()
    {
        return false;
    }
    let Some(c) = constside.as_const() else { return false };
    !c.constisnull && matches!(c.consttype, INT2OID | INT4OID | INT8OID)
}

/// TOPN-HIGHGROUPS sort-key vocabulary: the finalfn-free int8-transvalue
/// aggregates — exactly the order columns the runtime sink's combine-phase
/// top-N can resolve (the sink's order-column resolve wants a bare
/// finalfn-none int8 Aggref: count and the int2/int4 sum ring). An order
/// column outside this set declines the sink's bound and the drain
/// materializes every group — the exemption below must never admit that at
/// high group estimates.
const TOPN_INT8_RAW_SORT_AGGS: &[u32] = &[F_COUNT_STAR, F_COUNT_ANY, F_SUM_INT4, F_SUM_INT2];

/// Mirror of the runtime sink's combine-phase top-N bound cap
/// (`nodeagg::sink::SINK_TOPN_MAX_BOUND`): a bound past the cap declines
/// the winner selection, so the exemption fails closed past it too. The
/// DISTINCT sink's kernel-2 admission (runtime_distinct.rs
/// `distinct_topn_arm`) enforces the SAME cap, so the distinct-flavored
/// exemption below mirrors this constant too.
const SINK_TOPN_MAX_BOUND_MIRROR: i64 = 1 << 16;

/// GL-ELECT22-1 fix 4a — DISTINCT-sink winner-selection sort-key
/// vocabulary (the kernel-2 admission mirror, runtime_distinct.rs
/// `distinct_topn_arm`): beside the count(DISTINCT) column itself
/// (`is_count_distinct_int` — the merged set's value count, SetCount),
/// the paremit selection resolves ONLY count(*)/count(x) (the never-NULL
/// vocab sidecar word). sum(int2/4) is NULLABLE — the sink degrades it to
/// the FULL drain (no decline face, but exactly the every-group emit the
/// §10 hold prices), so the exemption never admits it as the order
/// column.
const DISTINCT_TOPN_SORT_AGGS: &[u32] = &[F_COUNT_STAR, F_COUNT_ANY];

/// TOPN-HIGHGROUPS knob (`PGRUST_M5_TOPN_HIGHGROUPS`): DEFAULT OFF, only
/// `1`/`on` arm (the K1-latemat idiom). Exempts the bounded
/// winner-selection composition from the §10 groupby_high legacy hold: the
/// hold's economics (unbounded emit of every group through the exchange
/// merge) predate the sink's combine-phase top-N, which materializes
/// winners only — at winner-selection shapes the group estimate no longer
/// prices the emit, and the held legacy plan is a serial leader hashagg
/// over raw exchanged rows (the spill cliff at scale).
///
/// DEFAULT ON (open-rows flip train, GL-OPENROWS-GBHIGH-TOPN
/// FLIP-RECOMMENDED — fleet letter 2026-07-21: the no-qual three-int-agg
/// winner-selection census row 100M 54.0s -> 1.04s, 51.9x, BEATING the
/// forced-vector ceiling; 10M 8.2x; jobs -695b/-10b3, composition leg
/// -2250 lands the qualed siblings 8.0x/73x at their ceilings. The
/// REQUIRED spill-pressure ladder — work_mem 128MB..1GB at full scale,
/// jobs -6012/-1bba, memsample banked — is FLAT 1.04-1.24s at every
/// rung, suppressed plans throughout, zero session deaths: the
/// winners-only sink has no work_mem cliff in-range, unlike the leader
/// hashagg it replaces). `PGRUST_M5_TOPN_HIGHGROUPS=0|off` is the kill
/// (flipped-kill idiom).
fn topn_highgroups_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        tier2_car_kill_spelling_on(std::env::var("PGRUST_M5_TOPN_HIGHGROUPS").as_deref().ok())
    })
}

/// GL-ELECT22-1 fix 2 — const keys into the TOPN-HIGHGROUPS exemption
/// (`PGRUST_M5_TOPN_HIGHGROUPS_CONSTKEY`, DEFAULT OFF; ON iff exactly
/// `1|on`). The bypass fails closed on `n_const == 0`, which splits the
/// const+text-key winner-selection census shape (held: est 18,436,094
/// groups, forced 1.900/1.712 hot at the pinned census) from its
/// const-less sibling that ROUTES through the bypass today. A const group
/// key partitions NOTHING — every row carries the identical value, so the
/// composite (const, k…) has exactly the group count of (k…): the
/// SE-CONSTKEY law already classifies, estimates, and floors on the REAL
/// keys, and the sink's winner selection tolerates the const tlist entry
/// (the constkey-grouped-topn engagement of record, GL-CONSTKEY-1). This
/// knob only extends the SAME argument past the §10 hold; every other
/// bypass condition (int8-raw sort key, bound cap, no count(DISTINCT)/
/// strminmax/mk-family riders) holds unchanged. Composes with
/// SE-CONSTKEY: with EITHER knob off the shape keeps today's refusal
/// byte-for-byte.
///
/// DEFAULT ON (GL-ELECT22-1 flip; `=0|off` kills): 100M witnessed pair
/// @ c9eb09e803/240b738c9 (jobs -4b6f/-5f1a vs OFF baseline -5ca6) —
/// knob-ON suppresses at ngroups=18,436,094 (label
/// constkey-grouped-topn-highgroups), hot 1.700-1.847 BEATING the
/// forced recovery bound (1.900); the const-less control cell keeps
/// routing through the base bypass unchanged; byte parity across arms.
fn topn_highgroups_constkey_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        tier2_car_kill_spelling_on(
            std::env::var("PGRUST_M5_TOPN_HIGHGROUPS_CONSTKEY").as_deref().ok(),
        )
    })
}

/// GL-ELECT22-1 fix 4a — DISTINCT-sink winner-selection hold exemption
/// (`PGRUST_M5_TOPN_HIGHGROUPS_DISTINCT`). The TOPN-HIGHGROUPS
/// exemption structurally excludes
/// `n_count_distinct > 0`, so the text-key grouped count(DISTINCT int)
/// top-N census shape refuses at the §10 hold (est 5,441,263 groups at
/// the pinned 100M census; forced recovery bound 0.800/0.752 hot) even
/// though the DISTINCT sink's OWN combine-phase top-N (pardistinct
/// kernel 2, `distinct_topn_arm`) materializes winners only — the same
/// economics argument the grouped-sink bypass banked. This knob admits
/// the composition into the textdistinct finish when the sink's
/// selection provably arms: sort key = the count(DISTINCT) itself or a
/// never-NULL count (DISTINCT_TOPN_SORT_AGGS mirror), Const bound within
/// the shared sink cap, exactly the witnessed one-text-key /
/// one-count-distinct shape, no sibling knob riders. Fail-closed ceiling
/// below (own headroom past the hold — the merged distinct SETS still
/// price memory before the selection truncates; the GL-DISTALPHA
/// flush-epoch finding is the named hazard, so the ladder cell carries a
/// spill-pressure witness). Executor-kill coherence: the kernel-2 /
/// paremit kills are mirrored (`distinct_topn_arm_live`) — with either
/// kill thrown the paremit is a FULL drain at census group counts,
/// exactly what the hold prices, so the exemption disarms with them.
///
/// DEFAULT ON (GL-ELECT22-1 flip; `=0|off` kills): 100M witnessed pair
/// @ c9eb09e803/240b738c9 (jobs -4b6f/-5f1a vs OFF baseline -5ca6) —
/// knob-ON suppresses at ngroups=5,428,026 (under the 8M ceiling; label
/// text-grouped-count-distinct-topn-highgroups), hot 0.910-1.034 vs the
/// 0.800 forced recovery bound, byte parity across arms, memsample
/// envelope clean (no OOM, no session deaths at the 27Gi pod class).
fn topn_highgroups_distinct_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        tier2_car_kill_spelling_on(
            std::env::var("PGRUST_M5_TOPN_HIGHGROUPS_DISTINCT").as_deref().ok(),
        )
    })
}

/// Fix-4a group-estimate ceiling (env-overridable
/// `PGRUST_M5_TOPN_HIGHGROUPS_DISTINCT_MAX_GROUPS`, the ladder's sweep
/// vehicle): PROVISIONAL 8M — the census cell (5.44M text groups) +
/// estimate-wobble headroom, below unladdered territory. Unlike the
/// grouped-sink bypass (winners-only all the way down), the distinct
/// sink holds every group's merged SET until the combine truncates —
/// the ceiling bounds that exposure until GL-ELECT22-1's ladder (with
/// the spill-pressure witness) derives the real bound.
fn topn_highgroups_distinct_max_groups() -> f64 {
    static CEIL: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *CEIL.get_or_init(|| {
        std::env::var("PGRUST_M5_TOPN_HIGHGROUPS_DISTINCT_MAX_GROUPS")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| *v > 0.0)
            .unwrap_or(8_000_000.0)
    })
}

/// Fix-4a executor-kill coherence (the suppress-then-full-drain guard):
/// the distinct sink's bounded selection needs BOTH the paremit drive
/// (`PGRUST_RUNTIME_DISTINCT_PAREMIT`, default ON, `0` kills) and the
/// kernel-2 selection (`PGRUST_RUNTIME_DISTINCT_TOPN`, default ON,
/// `0|off` kills) live — same spellings as runtime_distinct.rs; with
/// either thrown the suppressed plan drains EVERY group.
fn distinct_topn_arm_live() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_DISTINCT_PAREMIT").as_deref() != Ok("0")
            && !matches!(
                std::env::var("PGRUST_RUNTIME_DISTINCT_TOPN").as_deref(),
                Ok("0") | Ok("off")
            )
    })
}

/// A plan-time-constant LIMIT/OFFSET count: `Some(v)` for a non-null
/// int8/int4 Const with `v >= 0`; `None` (fail closed) for params,
/// expressions, nulls, and every other type.
fn const_count(node: Option<Node<'_>>) -> Option<i64> {
    let c = node?.as_const()?;
    if c.constisnull {
        return None;
    }
    let v = match c.consttype {
        INT8OID => c.constvalue.as_i64(),
        INT4OID => i64::from(c.constvalue.as_i32()),
        _ => return None,
    };
    (v >= 0).then_some(v)
}

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
/// jobs -558e/-135a/-3773 @ 67a99589d, unforced 10M bank): official suite
/// score 0.9278 (−7.2%; inert-arm noise floor 0.9889) — essentially all of
/// it the narrow-sort distinct shape 1.861 -> 0.066 hot (28.2x, == the forced rt16 ref 0.067,
/// confirming GL-AGGPOLY-2's avg-passenger claim unforced); 42/42 remaining
/// queries in the noise band, byte-parity class set unchanged; composes
/// with GL-AGGPOLY-1's SE16 −12.4% fact-rel heap-shape WIN. Probe cost is
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

/// AGG_INTCASE (int-CASE fold-args car): widen the AggPolyHeapPlain tlist
/// vocabulary onto int-family plain aggregates over parallel-safe
/// CONDITIONAL argument expressions (sum(CASE...), count-if — the
/// conditional-aggregation idiom) and onto emit expressions over admitted
/// aggregates (the ratio-emit shapes). The per-row drive already evaluates
/// arbitrary arg exprs (C's checked transition program); this knob only
/// widens ADMISSION. Knob coherence (the AGG_POLY law above): nodeagg's
/// poly manifest reads the SAME env spelling — a keyed shape whose
/// manifest arm is disarmed would suppress Gather and land on serial. The
/// widening also requires the AGG_POLY knob itself (it rides that class's
/// arm, floor, and keying site).
///
/// DEFAULT ON (GL-INTCASE-1, fleet-ab-parallelism.md 2026-07-21, @
/// 39219b094e1f: DOP ladder on the 10M heap fixture — dop16 wins 1.9-2.2x
/// on all six owned shapes, dop8 0.83-0.97; dop4 measured 1.03-1.20x
/// LOSSES, which the AggPolyHeapPlain floor OWNS — suppress iff rows>=1M
/// and dop>=12 keeps Gather in that region; kill-arm dead-flat
/// 1.000-1.003 with zero engagements; 43q hot-quotient 0.9819 = inert
/// noise band; byte-identity + error-identity witnessed local and
/// in-pod). `PGRUST_LANE_V2_AGG_INTCASE=0|off` is the kill switch.
fn agg_intcase_probe_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        intcase_spelling_on(std::env::var("PGRUST_LANE_V2_AGG_INTCASE").as_deref().ok())
    })
}

/// The default-ON kill spelling rule, factored pure for exhaustive unit
/// tests: OFF iff the value is exactly `0` or `off` (the flipped-kill
/// idiom); unset and every other spelling stay ON.
fn intcase_spelling_on(v: Option<&str>) -> bool {
    !matches!(v, Some("0") | Some("off"))
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
/// GL-LOWDIST-3 datetime family sibling (timestamptz; TIMESTAMPOID lives
/// next to F_EXTRACT_TIMESTAMP below).
const TIMESTAMPTZOID: u32 = 1184;
const TEXTOID: u32 = 25;
/// SE-CBKEYS: bpchar — recognized ONLY to NAME its refusal (the
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

/// GL-LOWDIST-3: the datetime family whose same-type equality is
/// representational word equality on the stored key (date = i32 days,
/// timestamp/timestamptz = i64 microseconds; the distinct_set_kind
/// argument) — admitted as DISTINCT ARGS only, under the widening knob.
fn is_datetime_family(typ: u32) -> bool {
    matches!(typ, DATEOID | TIMESTAMPOID | TIMESTAMPTZOID)
}

/// GL-LOWDIST-3 datetime-distinct widening — **DEFAULT ON** since the
/// GL-LOWDIST-3 flip; kill spellings exactly `0|off`, the EXECUTOR
/// spelling verbatim (nodeagg::distinct_datetime_enabled; GROUPSINK
/// coherence: probe routing and sink/serial admission kill together).
/// Letter of record: scratchpad/night/GL-LOWDIST-3-letter.md.
fn distinct_datetime_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_LANE_V2_DISTINCT_DATETIME").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// A DISTINCT-ARG type the exact-set machinery admits: int family always;
/// datetime under the GL-LOWDIST-3 knob.
fn is_distinct_arg_int_kind(typ: u32) -> bool {
    is_int_family(typ) || (distinct_datetime_enabled() && is_datetime_family(typ))
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
/// (unforced 10M analytics bank, cbstore9-v8-sorted-v2, c8gd NVMe) showed the
/// CURRENT runtime agg arm crushes the 1e6..3e6 band (dict-int-key 0.864s→~0.02s,
/// two-key 0.900s→~0.06s, URL-key shapes 2.3s→~0.24s) while est_groups≈1e7 (the
/// 10M-group class) still loses in the runtime combine until the exchange program
/// lands — so the boundary moves to 4e6 (above the measured-winning ≈3e6
/// band, below the known-losing 1e7), NOT to unbounded.
/// GL-RADIX-2 dop-lift knob — see the hold site. DEFAULT OFF; armed iff
/// exactly `1`/`on`. The flip rides the GL-RADIX-2 letter with the
/// cap-band-v2 executor knob (knob-coherence: the lift only holds its bar
/// with v2's band curve engaged on the executor side).
fn groupby_high_doplift_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(
            std::env::var("PGRUST_M5_GROUPBY_HIGH_DOPLIFT").as_deref(),
            Ok("1") | Ok("on")
        )
    })
}

/// Dop-lift α_est floor (`PGRUST_M5_GROUPBY_HIGH_DOPLIFT_ALPHA`, default
/// 4.0) — see the hold-site comment: α ≈ 1 shapes keep Gather.
fn groupby_high_doplift_alpha() -> f64 {
    static N: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_M5_GROUPBY_HIGH_DOPLIFT_ALPHA")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|a| a.is_finite() && *a >= 1.0)
            .unwrap_or(4.0)
    })
}

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

pub(crate) fn trace_armed() -> bool {
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
        || !parse.windowClause.is_nil()
        || parse.setOperations.is_some()
        || !parse.rowMarks.is_nil()
        || !parse.mergeActionList.is_nil()
        || !parse.returningList.is_nil()
        || parse.limitOption == LimitOption::LIMIT_OPTION_WITH_TIES
    {
        return Ok(false);
    }

    // HAVING (stragg-coverage inc-1, GL-STRAGG-2): historically a hard
    // structural refusal in the prefilter above. The ONE admitted
    // composition is the knob-gated post-aggregate filtered grouped shape —
    // a single `count(*) <cmp> Const` term over a single-plain-rel grouped
    // aggregation, exactly the emit-row filter the runtime grouped sink
    // compiles (nodeagg::sink::having_emit_filter — the probe mirror
    // `having_term_admissible`; suppress-then-refuse excluded by
    // construction). Multi-rel forms are excluded structurally RIGHT HERE;
    // the surviving single-rel branches that never proved the composition
    // (DISTINCT decoration, partwise, the expr-key classifiers, the
    // sibling knob cars) re-refuse at their entries below. Knob OFF takes
    // the identical refusal as before, byte-for-byte.
    if parse.havingQual.is_some() {
        let single_plain_rel = parse.jointree.is_some_and(|jt| {
            jt.fromlist.len() == 1 && jt.fromlist.nth(0).as_range_tbl_ref().is_some()
        });
        if !(agg_having_enabled()
            && parse.hasAggs
            && !parse.groupClause.is_nil()
            && single_plain_rel
            && parse.havingQual.is_some_and(having_term_admissible))
        {
            return Ok(false);
        }
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
    // PARTWISE-MORSELS (night/partitionwise-morsels; CoverClass::
    // PartwisePlainFold, tsv row partwise-plain-fold — covered/runtime;
    // DEFAULT ON since GL-PARTWISE-1, PGRUST_LANE_V2_PARTWISE=0|off kills):
    // partitioned-PARENT single-rel plain-fold shapes. The whole classifier
    // lives in m5_partwise.rs (its own module per the night-run
    // coordination note); this hook is the one touch in this file.
    // Killed-knob takes the identical refusal below (keep-Gather posture,
    // byte-for-byte the pre-flip world).
    if rte.rtekind == RTEKind::RTE_RELATION
        && rte.relkind == types_rel::RELKIND_PARTITIONED_TABLE
        && rte.inh
        && rte.tablesample.is_none()
        // stragg HAVING carve: the partwise classifier never proved the
        // post-aggregate filter composition — re-refuse (fail-closed).
        && parse.havingQual.is_none()
        && crate::m5_partwise::partwise_probe_enabled()
    {
        return crate::m5_partwise::classify_partitionwise(run, parse, rti);
    }
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
        // stragg HAVING carve: never composes with DISTINCT decoration
        // (fail-closed re-refusal — the prefilter carve above admits the
        // grouped-agg family wholesale).
        if parse.havingQual.is_some() {
            return Ok(false);
        }
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
            // One key-vocabulary walk serves both rows: all-int keys keep
            // the bootstrap class (verdicts byte-identical to the pre-
            // SE-TOPNNI code); any admissible non-int key routes to the
            // knob path below (kill =0|off => keep Gather exactly as
            // before the car).
            let mut n_nonint = 0usize;
            let mut n_text = 0usize;
            // The LEADING key's class decides the sink's band-predicate
            // eligibility (runtime_sort keys the zone machinery off
            // keys[0]; a dict-text lead skips it) — tracked for the
            // selective-qual carve below.
            let mut lead_is_text = false;
            for (ki, sc_node) in parse.sortClause.iter().enumerate() {
                let Some(sc) = sc_node.as_sort_group_clause() else { return Ok(false) };
                let Some(tle) = tle_by_sortgroupref(parse, sc.tleSortGroupRef) else {
                    return Ok(false);
                };
                let Some(v) = key_var(tle.expr, rti) else { return Ok(false) };
                if ki == 0 {
                    lead_is_text = is_text_family(v.vartype);
                }
                if is_int_family(v.vartype) {
                    continue;
                }
                // SE-TOPNNI non-int vocabulary — exactly what the sink
                // admits: the datetime family rides the I4/I8 CmpOp
                // aliases; text rides the DictCode class (deterministic
                // default collation + a v7 stitch on the column, checked
                // at plan time — the answerability discipline).
                if !topn_nonint_enabled() {
                    return Ok(false);
                }
                n_nonint += 1;
                // (timestamptz would ride the same I8 alias, but the cb AM
                // refuses timestamptz COLUMNS — a bare-Var tstz key on a cb
                // rel is unreachable, so it is deliberately not keyed.)
                match v.vartype {
                    DATEOID | TIMESTAMPOID => {}
                    TEXTOID | VARCHAROID
                        if v.varcollid == DEFAULT_COLLATION_OID
                            && topn_nonint_text_key_stitched(run, rel_id, v.varattno as i32) =>
                    {
                        n_text += 1;
                    }
                    _ => return Ok(false),
                }
            }
            let mut tlist_natts = 0usize;
            let mut tlist_all_car_payload = true;
            for tle_node in &parse.targetList {
                let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
                let Some(v) = key_var(tle.expr, rti) else {
                    return Ok(false);
                };
                tlist_natts += 1;
                if !topn_car_payload_type(v.vartype) {
                    tlist_all_car_payload = false;
                }
            }
            if n_nonint == 0 {
                // GL-TOPNHEAP-1 car mirror (plan-computable UNDER-
                // approximation of the direct feed's admission — a mirror
                // miss keeps Gather; over-approximating would suppress
                // onto the INCUMBENT arm, the measured 2.9-9.8x k=1000
                // loss the guard-off exists to prevent): single int key,
                // qual-free, all-byval tlist within the capture envelope,
                // and a CONST LIMIT bound (the k axis — the curve's
                // routed-admission band, rtm::topn_car_k_band, checked at
                // the verdict). Out-of-mirror shapes pass k=0 = out of
                // band: the guard-off keep-Gather posture stands and the
                // legacy planner elects GM/serial (both measured better
                // than the incumbent arm on this class).
                let car_k = if topn_heap_route_live()
                    && parse.sortClause.iter().count() == 1
                    && !has_quals
                    && tlist_all_car_payload
                    && tlist_natts <= TOPN_CAR_PAY_MAX
                {
                    const_count(parse.limitCount).map_or(0.0, |k| k as f64)
                } else {
                    0.0
                };
                return finish(
                    run,
                    CoverClass::CbTopnBoundedIntKeys,
                    rte.relid,
                    car_k,
                    rel_rows,
                    rel_pages,
                    true,
                );
            }
            // Knob-path guards mirroring the SINK's own admission (a keyed
            // shape the sink refuses lands on serial — the suppress-then-
            // refuse direction, excluded structurally):
            //   * <=4 keys (nodesort::sink::TOPN_MAX_KEYS — the wide-heap
            //     encode arity; the bootstrap int row predates the cap and
            //     keeps its historical behavior);
            //   * bound <= 65536 when the LIMIT is a plain Const
            //     (TOPN_MAX_BOUND; non-Const bounds refuse fail-closed);
            //   * single-entry tlists only with a text key (the sink's
            //     datum-shape gate: `is_datum` refuses unless a DictCode
            //     key admits — smoke-verified on the bare
            //     `SELECT ts ORDER BY ts LIMIT n` shape).
            if parse.sortClause.len() > 4 {
                return Ok(false);
            }
            let bound = match parse.limitCount.and_then(|n| n.as_const()) {
                Some(c) if !c.constisnull && c.consttype == INT8OID => {
                    let b = c.constvalue.as_i64();
                    if !(1..=65536).contains(&b) {
                        return Ok(false);
                    }
                    b
                }
                _ => return Ok(false),
            };
            if parse.targetList.len() == 1 && n_text == 0 {
                return Ok(false);
            }
            // SERIAL-SIDE three-way shadow (costsize::serial_model,
            // observation only): price the serial zone walk against both
            // parallel engines next to the carve below. Inputs are the
            // probe's own vocabulary: raw tuples (the walk's domain),
            // estimated qual survival, the Const bound, and the carve's
            // zone-posture proxy (band-eligible datetime lead).
            let survival = if has_quals {
                let tuples = run.root.rel(rel_id).tuples.max(rel_rows).max(1.0);
                (rel_rows / tuples).clamp(0.0, 1.0)
            } else {
                1.0
            };
            let topn_shadow = {
                use costsize::serial_model as sm;
                let class = if lead_is_text {
                    if parse.targetList.len() == 1 {
                        sm::TopnKeyClass::TextDatum
                    } else {
                        sm::TopnKeyClass::TextLead
                    }
                } else if parse.sortClause.len() > 1 {
                    sm::TopnKeyClass::MultiKey
                } else if parse.targetList.len() >= 3 {
                    sm::TopnKeyClass::StarWide
                } else {
                    sm::TopnKeyClass::NarrowTs
                };
                sm::topn_nonint_three_way(&sm::TopnShape {
                    class,
                    rows: run.root.rel(rel_id).tuples.max(rel_rows).max(1.0),
                    dop: guc_tables::runtime_pool::runtime_dop(),
                    limit: bound as f64,
                    survival,
                    zone_friendly: !lead_is_text,
                })
            };
            // SELECTIVE-QUAL x BAND-ELIGIBLE-LEAD carve (GL-TOPNNI-1 flip
            // sanity diagnosis 2026-07-21 @ 34b23fdf2): on the real
            // sorted bank a datetime LEADING key classifies ZONE-FRIENDLY
            // and the arm band-refuses to the serial zone walk — the
            // minting-era cell had a SELECTIVE qual starving the walk's
            // early exit, so the carve kept Gather unconditionally below
            // the threshold. GL-RESIDUAL-2 re-adjudication (doc at
            // `topnni_selqual_priced_enabled`): that cell is INVERTED at
            // the current tree at both scales, so the star-wide class now
            // takes the fitted two-way verdict; other shapes and
            // below-support keep Gather (abstain = incumbent), and the
            // kill spelling restores the unconditional carve. Under the
            // same economics gate as the floors so the rowflip measure
            // vehicle (PGRUST_M5_SIZE_FLOORS=0) can see through.
            if has_quals
                && !lead_is_text
                && size_floors_enabled()
                && survival < TOPN_NONINT_MIN_QUAL_SURVIVAL
            {
                let star_wide = parse.sortClause.len() == 1 && parse.targetList.len() >= 3;
                let priced_suppress = topnni_selqual_priced_enabled()
                    && star_wide
                    && costsize::serial_model::topn_selqual_starwide_two_way(
                        run.root.rel(rel_id).tuples.max(rel_rows).max(1.0),
                    )
                    .is_some_and(|v| {
                        v.pick == costsize::serial_model::EnginePick::Serial && !v.parity
                    });
                if !priced_suppress {
                    if trace_armed() {
                        eprintln!(
                            "m5-suppress-floor: topnnonint label=selective-qual-datetime-lead \
                             relid={} survival={survival:.4} => gather stands",
                            rte.relid
                        );
                    }
                    serial_shadow_tail(
                        serial_shadow::TOPN_NONINT,
                        "selective-qual-datetime-lead",
                        topn_shadow,
                        false,
                    );
                    return Ok(false);
                }
                if trace_armed() {
                    eprintln!(
                        "m5-suppress-topnnonint: label=selqual-priced relid={} \
                         survival={survival:.4} => gather suppressed (priced serial walk)",
                        rte.relid
                    );
                }
                // Fall through to the knob-path finish: the suppressed
                // plan band-refuses to the serial zone walk at exec (the
                // priced engine).
            }
            let label = if n_text > 0 { "topn-text-keys" } else { "topn-datetime-keys" };
            let suppressed = finish_knob_path(
                run,
                "topnnonint",
                label,
                topn_nonint_guard(),
                rte.relid,
                0.0,
                rel_rows,
                rel_pages,
            )?;
            serial_shadow_tail(serial_shadow::TOPN_NONINT, label, topn_shadow, suppressed);
            return Ok(suppressed);
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
                // serial-instead-of-legacy false positive (measured on the v6
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
                // Plan-time META-band mirror (the executor hands provable
                // folds to the serial footer answer; the serial curve was
                // fit on exactly that posture): unqualed, or estimated
                // survival ~1 (zone-provable-true class). Mixed-selective
                // quals must not ride the META-priced serial curve.
                let survival = if has_quals {
                    let tuples = run.root.rel(rel_id).tuples.max(rel_rows).max(1.0);
                    (rel_rows / tuples).clamp(0.0, 1.0)
                } else {
                    1.0
                };
                let meta_posture = !has_quals || survival >= 0.999;
                let suppressed = finish(
                    run,
                    CoverClass::CbPlainAggFold,
                    rte.relid,
                    1.0,
                    rel_rows,
                    rel_pages,
                    meta_posture,
                )?;
                // SERIAL-SIDE shadow, qualed-fold family: when the qual's
                // estimated survival is ~1 the serial lane answers per
                // granule from footer META (the zone-provable posture the
                // ladder-spec L4 named cell witnessed: the serial walk
                // beats BOTH parallel engines ~7x). The term abstains on
                // any other posture — single-anchor support.
                if has_quals {
                    let tuples = run.root.rel(rel_id).tuples.max(rel_rows).max(1.0);
                    let shadow = costsize::serial_model::scanfold_meta_three_way(
                        tuples,
                        guc_tables::runtime_pool::runtime_dop(),
                        (rel_rows / tuples).clamp(0.0, 1.0),
                    );
                    serial_shadow_tail(
                        serial_shadow::SCANFOLD_META,
                        "qualed-plain-fold",
                        shadow,
                        suppressed,
                    );
                }
                return Ok(suppressed);
            }
            // Meta-over-Gather (M5-5, the band-2a arithmetic-agg handoff): the residual
            // plain-agg shapes the Meta footer arm answers — affine int
            // sum/avg args (`sum(v+k)` batteries) the bare-Var whitelist
            // above does not key. No quals (footer answers are whole-table;
            // the zero-count qual sub-arm stays unkeyed — narrower probe).
            if !has_quals
                && parse.sortClause.is_nil()
                && tlist_all_meta_footer_aggs(parse, rti)
            {
                return finish(run, CoverClass::CbMetaFooterAgg, rte.relid, 1.0, rel_rows, rel_pages, true);
            }
            // SE-TEXTDISTINCT (C1, band 86001): ungrouped count(DISTINCT
            // <int|default-collation text Var>) — the census's "plain distinct
            // shape unwired" gap (int-key count(DISTINCT UserID), text-key
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
            // the plain sub-knob OFF off a measured 10M REGRESSION (int-key
            // 0.046->0.151, text-key 0.081->0.175) — later diagnosed as the
            // suppress-then-UNARMED hole (the sink armed off the bench GUC
            // alone, so the suppressed plan ran serial with no pool). With
            // the arm_dop fix landed, GL-TEXTDIST-2 re-measured GREEN (both shapes
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
                        // GL-LOWDIST-1 re-derivation (INT face — the
                        // baseline alone showed the provisional guard
                        // preserving a 1.7-2.7x-losing GM+hybrid across the
                        // low-dop band) + GL-LOWDIST-2 (TEXT face — its own
                        // witnessed ladder, dop {1,2,4,8,16} x 1M-10M
                        // spanning the 3M edge, fast + dist profiles: the
                        // sink wins EVERY dop>=2 cell against both the
                        // forced-legacy GM and serial, 0.32-0.57 rt/legacy;
                        // letter scratchpad/night/GL-LOWDIST-2-letter.md).
                        // Both arg faces ride the re-derived low-width
                        // curve; the kill restores the provisional floor.
                        return finish_textdistinct(
                            run,
                            "plain-count-distinct",
                            distinct_lowwidth_guard(textdistinct_guard()),
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
        // suppress-then-refuse direction). The qualed numeric-expr
        // plain-agg-over-one-heap-rel class. AGG_INTCASE (default ON,
        // =0|off kills — GL-INTCASE-1) widens the tlist vocabulary onto
        // int-family aggs over conditional args + ratio emits
        // (heap_poly_tlist_admits).
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
            return finish(run, CoverClass::AggPolyHeapPlain, rte.relid, 1.0, scan_tuples, rel_pages, true);
        }
        // GL-LOWDIST-4 B1 (knob-gated): plain count(DISTINCT) over one HEAP
        // rel — the cb plain-count-distinct gates verbatim (the plain sink's
        // col-0 direct-key discipline: no quals, no sort/limit, exactly the
        // one count(DISTINCT) tlist entry). The sink's heap feed is the B1
        // widening (runtime_distinct::distinct_task_source); heap text
        // rides the collected emit_key batch (no dict lane).
        if distinct_heap_probe_enabled()
            && textdistinct_plain_enabled()
            && !has_quals
            && parse.sortClause.is_nil()
            && parse.limitCount.is_none()
            && parse.targetList.len() == 1
        {
            if let Some(tle) = parse.targetList.nth(0).as_target_entry() {
                if is_count_distinct_any(tle.expr, rti) {
                    return finish_knob_path(
                        run,
                        "distinctheap",
                        "plain-count-distinct-heap",
                        heap_distinct_guard(),
                        rte.relid,
                        1.0,
                        rel_rows,
                        rel_pages,
                    );
                }
            }
        }
        // Heap rows are no-qual only (LIKE-qual folds are walk refusals;
        // the qualed LIKE census is deliberately not keyed in bootstrap).
        if has_quals || !parse.sortClause.is_nil() {
            return Ok(false);
        }
        if is_bare_count_star(parse) {
            return finish(run, CoverClass::HeapPlainCountStar, rte.relid, 1.0, rel_rows, rel_pages, true);
        }
        if tlist_all_whitelisted_aggs(parse, rti, HEAP_CMP_AGGS) {
            return finish(run, CoverClass::HeapCmpFoldPrefix, rte.relid, 1.0, rel_rows, rel_pages, true);
        }
        return Ok(false);
    }

    // --- Grouped aggregation over pgrcolumnar ------------------------------------
    if !is_cb {
        // GL-LOWDIST-4 B1 (knob-gated): grouped count(DISTINCT int-family)
        // over one HEAP rel — the narrow census face only (bare int keys,
        // key + count-distinct tlist entries, nothing else; passengers
        // refuse and keep Gather, unchanged). A miss falls through to the
        // unchanged refusal.
        if distinct_heap_probe_enabled() && !has_quals && parse.sortClause.is_nil() {
            if let Some(verdict) = classify_heap_grouped_distinct(
                run, parse, rti, rte.relid, rel_id, rel_rows, rel_pages,
            )? {
                return Ok(verdict);
            }
        }
        return Ok(false);
    }
    // stragg HAVING carve: the expr-key classifiers below never proved the
    // post-aggregate filter composition — re-refuse it here (fail-closed;
    // the bare-Var grouped path further down owns the admitted class).
    // SE-TEXTDISTINCT (C1, band 86001): reduced-expr-key grouped agg —
    // keyed only knob-ON and BEFORE the bare-Var key discipline (which
    // refuses expr keys). A shape MISS returns None and falls through
    // unchanged.
    if textdistinct_enabled() && parse.havingQual.is_none() {
        if let Some(verdict) =
            classify_reduced_exprkey(run, parse, rti, rte.relid, rel_id, rel_rows, rel_pages)?
        {
            return Ok(verdict);
        }
    }
    // SE-EXTRACTKEY (ts-extract class): extract()-keyed grouped agg — keyed
    // only knob-ON and BEFORE the bare-Var key discipline (which refuses
    // expr keys). A shape MISS returns None and falls through unchanged.
    if extract_exprkey_enabled() && parse.havingQual.is_none() {
        if let Some(verdict) =
            classify_extract_exprkey(run, parse, rti, rte.relid, rel_id, rel_rows, rel_pages)?
        {
            return Ok(verdict);
        }
    }
    // OPEN-ROWS car 3: conditional-text-select (CASE) / timestamp-trunc
    // computed keys + OFFSET-into-bound composition — keyed only knob-ON
    // and BEFORE the bare-Var key discipline (which refuses expr keys). A
    // shape MISS returns None and falls through unchanged.
    if exprkey_topn_enabled() && parse.havingQual.is_none() {
        if let Some(verdict) =
            classify_exprkey_topn(run, parse, rti, rte.relid, rel_id, rel_rows, rel_pages)?
        {
            return Ok(verdict);
        }
    }
    // GL-DICTDRAIN-1 (DICTKEY car, default OFF): the regexp-extracted
    // dict-key grouped class — keyed only knob-ON and BEFORE the bare-Var
    // key discipline (which refuses expr keys). HAVING composes: only the
    // prefilter-admitted single count(*) term ever reaches this line (the
    // HAVING car's own carve above), and the sink's emit filter is
    // key-kind-agnostic. A shape MISS returns None and falls through.
    if agg_dictkey_enabled() {
        if let Some(verdict) =
            classify_dictkey_exprkey(run, parse, rti, rte.relid, rel_id, rel_rows, rel_pages)?
        {
            return Ok(verdict);
        }
    }
    // Key discipline: all keys plain Vars on the scanned rel; int-family
    // plus at most one text/varchar key under the deterministic default
    // collation (the c3 canonical-key-bytes classes). SE-CONSTKEY: non-null
    // int-family Const keys admitted knob-ON (the `GROUP BY 1, URL`
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
    // whitelisted sink aggregate (const tlist entries — the const-tlist refusal,
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
    // the grouped distinct-arg top-n mass, census row distinct-text-date-args). Knob-gated
    // (PGRUST_LANE_V2_TEXTDISTINCT, DEFAULT ON since t34; =0|off kills):
    // killed keeps Gather (the pre-flip posture); ON falls through to the
    // class selection, where the n_count_distinct && n_text branch routes
    // it through finish_textdistinct. The count(DISTINCT) arg stays
    // int-family (is_count_distinct_int — the SINK's exact-set vocabulary);
    // the TEXT is the GROUP key. GL-TEXTDIST letter (2026-07-21): the
    // grouped arm earns at default — distinct-arg shapes 0.010/0.011 hot vs cpg 0.44.
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
    // AGG_POLY knob); everything else keeps the grouped-sink vocabulary
    // (base list, or the GROUPED-AVG widening knob-ON).
    let passenger_list =
        if n_count_distinct > 0 { distinct_passenger_aggs() } else { grouped_sink_aggs() };
    let mut n_strminmax = 0usize;
    let mut n_avg_widened = 0usize;
    let mut n_lenarg = 0usize;
    for e in &passengers {
        if is_whitelisted_agg(*e, rti, passenger_list) {
            // GROUPED-AVG bookkeeping: a passenger only the widened list
            // admits routes the verdict through the knob-path finish below
            // (own trace tag; the default census stays byte-identical).
            if n_count_distinct == 0 && !is_whitelisted_agg(*e, rti, GROUPED_SINK_AGGS) {
                n_avg_widened += 1;
            }
            continue;
        }
        // stragg LENARG car (knob-gated, default OFF): agg arguments the
        // lanefold vocabulary proves — textlen-family expressions over a
        // bare text/varchar Var, served by the staged per-column length
        // lanes the engaged plans already run (H2 of the attribution
        // letter: zero per-row fmgr at defaults). Never beside the
        // distinct sink (its vocabulary stays exact — the se-aggpoly
        // suppress-then-refuse lesson); the sibling knob compositions are
        // excluded below with the HAVING car's list.
        if n_count_distinct == 0
            && agg_lenarg_enabled()
            && is_whitelisted_agg_lenarg(*e, rti, passenger_list)
        {
            n_lenarg += 1;
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

    // Sort-key vocabulary: the BASE (un-widened) lists. An avg-of-int SORT
    // key would arm a bound the sink's combine-phase top-N declines (the
    // order-column resolve wants a finalfn-free int8 transvalue), degrading
    // the suppressed plan to the full drain — fail closed on that
    // composition until a letter proves the degrade economics.
    let sortkey_list =
        if n_count_distinct > 0 { distinct_passenger_aggs() } else { GROUPED_SINK_AGGS };
    // Sort/limit composition: none at all (plain grouped emit), or the
    // top-N winner-selection shape — a single whitelisted-aggregate sort
    // key plus LIMIT without OFFSET (the grouped winner-selection census
    // family). A sort on the group keys themselves is an ordered-stream
    // consumer (GatherMerge class, uncovered in bootstrap). SE-DECOROOT
    // (CAR 1, knob-gated): the residual decorated-root shapes — ORDER BY
    // over group keys and/or class-vocabulary aggregates, multi-key
    // sorts, sorts without LIMIT, and LIMIT+OFFSET forms — key the
    // UNDERLYING grouped class; the arm emits the full grouped output and
    // the serial Sort/Limit above consumes it (the exprkey-Reduced /
    // CbGroupedAggTopN / AGG_BARELIMIT precedent). Fail-closed: no
    // count(DISTINCT) (distinct-sink decoration owns its own topn
    // composition only), no const/mk-family keys (their knob paths keep
    // their own proven compositions), at most one text key,
    // enable_hashagg required ON (with it off the suppressed serial plan
    // is a sorted-agg shape the walk refuses).
    let mut mk_freeze = false;
    let mut bare_limit = false;
    let mut full_sort = false;
    let mut decorated = false;
    // TOPN-HIGHGROUPS: does the top-N sort key sit in the finalfn-free
    // int8-transvalue set the sink's winner selection can resolve?
    let mut topn_int8_raw_sort = false;
    // GL-ELECT22-1 fix 4a: does the top-N sort key resolve in the
    // DISTINCT sink's paremit selection (the count(DISTINCT) column
    // itself, or a never-NULL count — DISTINCT_TOPN_SORT_AGGS doc)?
    let mut topn_distinct_sort = false;
    let topn = if parse.sortClause.is_nil() && parse.limitCount.is_none() {
        false
    } else if parse.sortClause.is_nil()
        && parse.limitCount.is_some()
        && parse.limitOffset.is_none()
        && mk_text_family
        && n_count_distinct == 0
        && agg_freeze_car_live()
    {
        // SE-MKTEXT: bare `LIMIT k` with NO ORDER BY (the bare-LIMIT class,
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
        //
        // stragg LENARG car: a lanefold-proven len-arg aggregate sort key
        // (avg/sum over textlen-family args) is admitted knob-ON. The base
        // vocabulary's fail-closed note above (an avg sort key degrades
        // the sink's winner selection to the full drain) is priced by the
        // GL-STRAGG-2 witnessed ladder — the degrade lands on the full
        // drain + serial Sort/Limit, and `topn_int8_raw_sort` stays false
        // for these keys so the §10 hold exemption never fires on them.
        let lenarg_sortkey = n_count_distinct == 0
            && agg_lenarg_enabled()
            && is_whitelisted_agg_lenarg(tle.expr, rti, grouped_sink_aggs());
        if !is_whitelisted_agg(tle.expr, rti, sortkey_list)
            && !is_count_distinct_int(tle.expr, rti)
            && !lenarg_sortkey
        {
            // SE-DECOROOT: the single-sort-key+LIMIT shape whose key is a
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
            // TOPN-HIGHGROUPS capture: only the admitted-agg sort key can
            // be a winner-selection order column (decorated group-key
            // sorts are full-drain forms — the hold exemption never fires
            // for them, `topn` stays false there).
            topn_int8_raw_sort = is_whitelisted_agg(tle.expr, rti, TOPN_INT8_RAW_SORT_AGGS);
            // GL-ELECT22-1 fix 4a capture (same site, distinct flavor).
            topn_distinct_sort = is_count_distinct_int(tle.expr, rti)
                || is_whitelisted_agg(tle.expr, rti, DISTINCT_TOPN_SORT_AGGS);
            true
        }
    } else if parse.sortClause.len() == 1
        && parse.limitCount.is_none()
        && parse.limitOffset.is_none()
        && agg_sort_nolimit_enabled()
    {
        // SE-T2AGG CAR C (knob-gated, default OFF — block doc below): the
        // topn shape WITHOUT the bound (full-sort class, ORDER BY count(*) no
        // LIMIT). Same single-agg sort-key vocabulary law as the topn arm;
        // the suppressed serial plan keeps its REAL Sort above the Agg (the
        // unbounded sink_topn_arm declines into the plain full drain and
        // the Sort consumes it), so this admits the COMPOSITION only.
        // (The SE-DECOROOT residual arm below owns this shape only when
        // this proven arm's knob is killed — same full-drain semantics.)
        let Some(sc) = parse.sortClause.nth(0).as_sort_group_clause() else {
            return Ok(false);
        };
        let Some(tle) = tle_by_sortgroupref(parse, sc.tleSortGroupRef) else {
            return Ok(false);
        };
        if !is_whitelisted_agg(tle.expr, rti, sortkey_list)
            && !is_count_distinct_int(tle.expr, rti)
        {
            // conversion-flips composition: this arm owns AGG sort keys
            // only — a single GROUP-key sort without LIMIT is the
            // SE-DECOROOT residual's shape, so fall through to its census
            // (the topn arm's identical fallback) instead of refusing;
            // with both knobs killed the refusal is byte-identical.
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
            full_sort = true;
            false
        }
    } else if decoroot_enabled()
        && !parse.sortClause.is_nil()
        && n_count_distinct == 0
        && n_const == 0
        && n_text <= 1
        && !mk_text_family
        && crate::gucs::enable_hashagg()
        && scan_sort_keys_covered(parse, &key_refs, rti, passenger_list)
    {
        // SE-DECOROOT (CAR 1): the residual whitelisted decorations —
        // multi-key sorts, group-key sorts, sorts without LIMIT, and
        // LIMIT+OFFSET above a sort. Bare LIMIT/OFFSET with NO sort stays
        // refused here (the SE-BARELIMIT / freeze rows own the no-sort
        // LIMIT composition; OFFSET without ORDER BY has no covered arm).
        decorated = true;
        false
    } else {
        return Ok(false);
    };

    // GROUPED-AVG fail-closed compositions: the widened avg-of-int
    // passengers are proven for the plain grouped emit and the agg-sort
    // top-N winner selection only (the engagement witnesses of record).
    // The freeze/bare-LIMIT drains, the no-limit sort, const keys, text
    // min/max passengers, the two-key-text family, and the SE-DECOROOT
    // decorated-root forms never carried the widened vocabulary — keep
    // today's refusal there.
    if n_avg_widened > 0
        && (mk_freeze
            || bare_limit
            || full_sort
            || decorated
            || n_strminmax > 0
            || n_const > 0
            || mk_text_family)
    {
        return Ok(false);
    }

    // stragg-coverage inc-1 fail-closed compositions: the LENARG and
    // HAVING cars are proven for the plain grouped emit and the agg-sort
    // top-N shapes only (full drain + serial Sort/Limit; the engagement
    // vacates the sink's winner selection under a filter). Every sibling
    // knob composition and the distinct sink keep today's refusal — none
    // of them carried the widened vocabulary or the emit filter.
    let having = parse.havingQual.is_some();
    if (n_lenarg > 0 || having)
        && (mk_freeze
            || bare_limit
            || full_sort
            || decorated
            || n_strminmax > 0
            || n_const > 0
            || mk_text_family
            || n_count_distinct > 0)
    {
        return Ok(false);
    }

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
    // hold at analytics-bank scale (10M dist-control estimates 3-5M groups for
    // `UserID, SearchPhrase` — the forced runtime arm wins 8-15x there), so
    // the knob path carries its OWN provisional ceiling instead of the §10
    // hold; everything else keeps the hold byte-for-byte.
    let over_groupby_high = ngroups >= groupby_high_floor();
    // GL-RADIX-2 DOP-LIFT (default OFF, armed iff
    // PGRUST_M5_GROUPBY_HIGH_DOPLIFT=1|on): lift the §10 hold for the BARE
    // int-key grouped-agg class only, at dop >= 12 — the band where the
    // runtime sink's seal-flush + cap-band-v2 combine measured same-pod
    // parity with the radix-exchange arm (GL-RADIX-1 decision job; the
    // GL-RADIX-2 letter owns the flip). Every other composition (text /
    // mk-text / count-distinct / strminmax / const-key / topn / sort /
    // limit / freeze / decorated) and every dop < 12 session keeps the
    // hold byte-for-byte — the radix-exchange arm still wins those cells
    // (its participating leader is a structural +1 scan thread the pool
    // arm does not field at low width).
    let over_groupby_high = over_groupby_high
        && !(groupby_high_doplift_enabled()
            && guc_tables::runtime_pool::runtime_dop() >= 12
            // α_est floor (GL-RADIX-2 10M leg): at α ≈ 1 (near row-per-
            // group output) the radix-exchange arm keeps a ~20% lead the
            // pool arm's extra-seat diagnostic did not explain — those
            // shapes keep the hold. Parity was measured at α = 10; 4 is
            // the fail-closed midpoint (env override for calibration).
            && run.root.rel(rel_id).rows.max(1.0) / ngroups.max(1.0)
                >= groupby_high_doplift_alpha()
            && !topn
            && !full_sort
            && !bare_limit
            && !decorated
            && !mk_freeze
            && !mk_text_family
            && n_text == 0
            && n_count_distinct == 0
            && n_strminmax == 0
            && n_lenarg == 0
            && !having
            && n_const == 0);
    // TOPN-HIGHGROUPS exemption: the bounded winner-selection composition
    // only, admitted iff the sink's combine-phase top-N provably arms —
    // sort key in the finalfn-free int8-transvalue set, a small Const
    // bound within the sink's cap, and no sibling knob composition riding
    // along (fail closed). The avg-passenger widening (its own knob)
    // COMPOSES: the three-int-agg winner-selection census shape carries
    // avg and needs both knobs armed. GL-ELECT22-1 fix 2: const keys also
    // COMPOSE knob-ON (fn doc — a const key partitions nothing, the REAL
    // keys already drive ngroups and the floors; the composed shape routes
    // through the constkey finish under its own trace label below).
    let topn_highgroups_bypass = over_groupby_high
        && topn_highgroups_enabled()
        && topn
        && topn_int8_raw_sort
        && n_count_distinct == 0
        && n_strminmax == 0
        && (n_const == 0 || topn_highgroups_constkey_enabled())
        && !mk_text_family
        && const_count(parse.limitCount)
            .is_some_and(|b| b > 0 && b <= SINK_TOPN_MAX_BOUND_MIRROR);
    // GL-ELECT22-1 fix 4a — the DISTINCT-sink flavored exemption (fn doc
    // on the knob): exactly the witnessed one-text-key one-count-distinct
    // winner-selection shape, the sink's kernel-2 admission mirrored
    // (order column + bound cap + executor kills), under its own
    // fail-closed ceiling. n_const/n_strminmax/mk-riders are refused
    // beside count(DISTINCT) at admission above, and re-required here so
    // a future widening cannot silently ride in.
    let topn_highgroups_distinct_bypass = over_groupby_high
        && topn_highgroups_distinct_enabled()
        && distinct_topn_arm_live()
        && topn
        && topn_distinct_sort
        && n_count_distinct == 1
        && n_text == 1
        && n_strminmax == 0
        && n_const == 0
        // stragg-coverage fail-closed: a filtered emit vacates the sink's
        // winner selection (full drain), so the exemption's economics
        // never apply; lenarg shapes never carry an int8-raw sort key but
        // the belt costs nothing.
        && n_lenarg == 0
        && !having
        && !mk_text_family
        && ngroups < topn_highgroups_distinct_max_groups()
        && const_count(parse.limitCount)
            .is_some_and(|b| b > 0 && b <= SINK_TOPN_MAX_BOUND_MIRROR);
    if over_groupby_high
        && !topn_highgroups_bypass
        && !topn_highgroups_distinct_bypass
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
        // no-limit-sort, and SE-DECOROOT decorated combinations are
        // unproven with byref text states; count(DISTINCT) +
        // mk-text-family were excluded at admission).
        if full_sort || decorated || bare_limit || mk_freeze || n_const > 0 {
            return Ok(false);
        }
        // GL-STRMM-2 flip calibration: refuse the group-estimate band where
        // the engaged sink measurably LOSES to the serial hash lane (fn doc
        // on the ceiling) — the planner keeps its own plan there.
        if ngroups >= strminmax_max_groups() {
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
    // guards are untouched). The top-N composition (the distinct-arg shapes are all
    // ORDER BY count DESC LIMIT) rides the sink's paremit selection — the
    // walk composes it (named-kernels-distinct Kernel 2), so no extra probe
    // condition is needed beyond the topn shape already validated above.
    if n_count_distinct > 0 && n_text > 0 {
        // GL-LOWDIST-2 re-derivation: this face's witnessed ladder (dop
        // {1,2,4,8,16} x 1M-10M, fast + dist profiles) measured the sink
        // winning EVERY cell against the forced-legacy arm 3-24x — the
        // pardistinct hybrids never engage on text group keys (hashgroup
        // admission is int-key), so the provisional floor was preserving
        // per-tuple GM, the worst arm at every point. dop>=2 also beats
        // serial everywhere; dop1 keeps Gather under the re-derived guard
        // (the named forgone win — suppression measured 3-4x better than
        // GM at dop1 but 2.2-2.4x worse than serial; the clean single
        // bound matches the INT face). Letter:
        // scratchpad/night/GL-LOWDIST-2-letter.md; kill restores the
        // provisional floor.
        return finish_textdistinct(
            run,
            // GL-ELECT22-1 fix 4a: name the hold-exempt composition apart
            // (the ladder's grep vocabulary) — reachable only knob-ON.
            if topn_highgroups_distinct_bypass {
                "text-grouped-count-distinct-topn-highgroups"
            } else {
                "text-grouped-count-distinct"
            },
            distinct_lowwidth_guard(textdistinct_guard()),
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
    // stragg-coverage inc-1 knob-path finish: shapes admitted ONLY by the
    // LENARG/HAVING cars route through the dedicated finish (own trace
    // tag; NOT a BOOTSTRAP_MATRIX re-class — tsv/route_to, the drift
    // guards, and the DEFAULT census untouched), carrying the guard of
    // the class the shape otherwise classifies as (the cars widen the
    // vocabulary and compose a post-aggregate emit filter; the shape's
    // scan/group economics are unchanged). Reached only on the plain
    // grouped / agg-sort top-N compositions — every sibling combination
    // refused above.
    if n_lenarg > 0 || having {
        let label = match (having, n_lenarg > 0, topn) {
            (true, true, true) => "having-lenarg-grouped-topn",
            (true, true, false) => "having-lenarg-grouped-agg",
            (true, false, true) => "having-grouped-topn",
            (true, false, false) => "having-grouped-agg",
            (false, true, true) => "lenarg-grouped-topn",
            (false, true, false) => "lenarg-grouped-agg",
            (false, false, _) => unreachable!("stragg finish without a car"),
        };
        return finish_knob_path(
            run,
            "stragg",
            label,
            class_guard(class),
            rte.relid,
            ngroups,
            rel_rows,
            rel_pages,
        );
    }
    // SE-DECOROOT (CAR 1) knob-path finish: decorated-root shapes route
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
            // GL-ELECT22-1 fix 2: name the hold-exempt composition apart
            // (the letters' grep vocabulary) — reachable only with BOTH
            // the constkey knob and the highgroups-constkey knob armed.
            if topn_highgroups_bypass {
                "constkey-grouped-topn-highgroups"
            } else if topn {
                "constkey-grouped-topn"
            } else {
                "constkey-grouped-agg"
            },
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
    // TOPN-HIGHGROUPS knob-path finish: shapes past the §10 hold ONLY via
    // the exemption route through the dedicated finish (own trace tag; the
    // label names the avg-passenger composition when it rides along —
    // the letters' grep vocabulary). class is CbGroupedAggTopN by
    // construction here (topn true, no count(DISTINCT)).
    if topn_highgroups_bypass {
        return finish_knob_path(
            run,
            "topnhigh",
            if n_avg_widened > 0 {
                "avgint-grouped-topn-highgroups"
            } else {
                "grouped-topn-highgroups"
            },
            class_guard(class),
            rte.relid,
            ngroups,
            rel_rows,
            rel_pages,
        );
    }
    // GROUPED-AVG knob-path finish: shapes admitted ONLY by the widened
    // avg-of-int passenger vocabulary route through the dedicated finish
    // (own trace prefix; NOT a BOOTSTRAP_MATRIX re-class — tsv/route_to
    // and the DEFAULT census untouched), carrying the guard of the class
    // the shape otherwise classifies as (the widening changes the
    // passenger vocabulary, not the shape's economics).
    if n_avg_widened > 0 {
        return finish_knob_path(
            run,
            "groupedavg",
            if topn { "avgint-grouped-topn" } else { "avgint-grouped-agg" },
            class_guard(class),
            rte.relid,
            ngroups,
            rel_rows,
            rel_pages,
        );
    }
    finish(run, class, rte.relid, ngroups, rel_rows, rel_pages, true)
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

// ===========================================================================
// NLIDX (night/nlidx-arm, GL-NLIDX-2 — the routing half of the
// NL-INNER-INDEX runtime arm). The executor half (lanev2/runtime_nlindex.rs,
// GL-NLIDX-1) is the best NL-inner-index executor we have on the engagement
// region — 7-11x over serial NL, 1.4-1.9x over classic Gather-NL at matched
// width from dop 8 up (the banked ladder, scratchpad/night/
// nlx-ab-fleet-full.tsv) — but a census-shape query never reaches it: the
// planner elects GATHER parallel-NL (small filtered driver rel, inner index
// probes into a big fact), which the arm fail-closed refuses. This probe
// suppresses Gather for exactly that family so the serial NL plan survives
// to the executor, where the arm re-classifies and engages at the routed
// dop (router::arm_dop(ArmClass::NlIndex) → pgrust.runtime_dop).
//
// SUPPRESS-THEN-SERIAL LAW, ELECTION-EXACT: suppression is admitted only
// when the joinrel's cheapest SERIAL path already IS the arm's shape —
// NestPath(INNER, outer = heap SeqScan, inner = parameterized btree
// IndexPath) — checked at the rel-aware choke points where that path
// exists (generate_useful_gather_paths + create_partial_grouping_paths).
// No stats-shaped election prediction: the planner's own serial election
// is the guard (a smoke round proved margin/pages heuristics suppress
// into serial hash joins). If serial-NL won the serial election, the
// morsel arm at dop>=8 dominates every Gather form the suppression
// removes (parallel-X >= serial-X/dop >= serial-NL/dop ~ the arm; the
// banked ladder gives the >=8 crossover vs classic Gather-NL). Every
// refusal keeps Gather standing byte-for-byte.
// ===========================================================================

/// NLIDX planner knob. FLIPPED-KILL (DEFAULT ON since the GL-NLIDX-2
/// letter, 2026-07-21 — scratchpad/night/fleet-ab-parallelism.md; the t35
/// exact spelling): `PGRUST_LANE_V2_NLIDX=0|off` kills.
fn nlidx_enabled() -> bool {
    static KILLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    !*KILLED.get_or_init(|| {
        matches!(std::env::var("PGRUST_LANE_V2_NLIDX").as_deref(), Ok("0") | Ok("off"))
    })
}

/// NLIDX executor coherence (the jheap `k2_heapfeed_live` precedent): a
/// thrown executor kill must also un-key the shape — a suppression whose
/// executor arm is killed would land on the bare serial NL (the
/// suppress-then-serial direction). Same FLIPPED-KILL spelling as the
/// executor (`PGRUST_RUNTIME_NLINDEX=0|off` kills; DEFAULT ON since the
/// GL-NLIDX-2 letter — runtime_pool::runtime_nlindex_env_ok).
fn nlidx_exec_live() -> bool {
    static KILLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    !*KILLED.get_or_init(|| {
        matches!(std::env::var("PGRUST_RUNTIME_NLINDEX").as_deref(), Ok("0") | Ok("off"))
    })
}

/// NLIDX small-driver polarity margins (PROVISIONAL — GL-NLIDX-2 owns the
/// measured bounds). The serial NL(driver seqscan, probe index) election
/// dominates the serial hash join when the probe side's FULL SCAN (the
/// hash build/probe pass) is expensive relative to `driver.rows` index
/// descents: driver post-qual rows bounded and far under the probe side's
/// tuples, probe side big enough that scanning it wholesale loses. The
/// flagship census point sits deep inside the region (driver ~4.7k
/// post-qual rows of a 2M-row rel; probe 60M tuples / ~1M pages).
/// Driver-side pages floor (env-overridable — the e2e floors force it, the
/// runtime_scan MIN_GRANULES precedent; default PROVISIONAL, GL-NLIDX-2
/// owns the bound): the driver's own scan is the morselized work unit —
/// require enough of it to be worth a gang (mirrors the executor's block
/// floor direction). The rest of the old stats-shaped polarity guards are
/// GONE: the admission is ELECTION-EXACT (the joinrel's cheapest serial
/// path must itself be the NL-inner-index shape — see
/// m5_suppress_gather_nlidx), which subsumes them.
fn nlidx_min_driver_pages() -> f64 {
    static N: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_LANE_V2_NLIDX_MIN_DRIVER_PAGES")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .unwrap_or(64.0)
    })
}

/// NLIDX floor: dop>=8 is the banked ladder's crossover vs classic
/// Gather-NL (below it Gather-NL wins and stands — the band). Size
/// economics are the inline polarity guards above (driver/probe shaped,
/// two-sided — FloorGuard's single rows/pages slots cannot carry them).
fn nlidx_guard() -> FloorGuard {
    FloorGuard { min_dop: 8, ..NO_GUARD }
}

/// NLIDX named-refusal diagnostics (the `refuse_scanpass` discipline:
/// `m5-suppress-refuse:` prefix, never `m5-suppress:` — M5CENSUS counts
/// that prefix as suppressions). Reached only when both NLIDX knobs are
/// armed — knob-OFF never recognizes the shape, the diagnostic is inert
/// at default.
fn refuse_nlidx(why: &str) -> PgResult<bool> {
    if trace_armed() {
        eprintln!("m5-suppress-refuse: nlidx ({why})");
    }
    Ok(false)
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
    // class (the bare single-int-Var emit `SELECT UserID ...` shape).
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
    // — the covered SHAPE (the bare-Var-emit class). Still refused: there is no
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
    let mut side_rows = [0.0f64; 2];
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
            // SE-JHEAP: heap sides admit knob-gated (+ the executor K2
            // feed coherence mirror); OFF takes the pre-existing refusal
            // byte-for-byte. Index tolerance/stats ride jheap_shape_guards
            // below (they need the qual set).
            if !(jheap_enabled() && k2_heapfeed_live()) {
                return refuse_join("side not cbstore");
            }
            heap.push((i, rel_id));
        }
        max_rows = max_rows.max(rel.rows.max(0.0));
        side_rows[i] = rel.rows.max(0.0);
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
        // BOUNDARY GUARD (GL-HJMB-1 escalation A): exec_choose prices C's
        // representation; the runtime arm's own build footprint runs
        // ~40% heavier per tuple, so a band of builds estimates nbatch==1
        // here yet truly crosses the combined envelope at execution — and
        // an unbatched engagement that crosses has no demote path (it
        // refuses at seal into an R5 serial rerun measured 5-11x worse
        // than the legacy Parallel Hash this suppression forgoes). Keep
        // Gather for the band: PHJ is the measured best route for it.
        // This guard is a PREREQUISITE for any lift of the join rows'
        // 2M-row FloorGuard (see join_floor_guard) — without it the lift
        // converts the masked cliff into a live regression.
        if ::nodehash::estimate_runtime_hj_build_peak_bytes(rel.rows.max(1.0), width)
            > ::nodehash::get_hash_memory_limit().saturating_mul(dop.max(1) as usize + 1) as u64
        {
            return refuse_join("build estimate within the demote-unsafe envelope band (boundary guard)");
        }
    }
    // >=1 hashjoinable int-family equi clause between the two sides in the
    // join quals (top-level AND terms only). By probe time the quals may be
    // an explicit BoolExpr AND, the planner's implicit-AND List (the
    // canonicalized form the FromExpr carries at path generation), or one
    // bare clause.
    let mut n_equi = 0usize;
    let mut int4_pair_only = true;
    // DUP-FLIP ELECTION GUARD input: the admitted equi keys that live on
    // the SMALLER rel (the planner's natural build side).
    let small = if side_rows[0] <= side_rows[1] { 0 } else { 1 };
    let small_rti = [rti_l, rti_r][small];
    let mut small_keys: Vec<Node<'mcx>> = Vec::new();
    // R2 output-cardinality input (soak-adj §R2.4): the admitted equi
    // clauses' (left var, right var) pairs — the eqjoinsel-style product
    // below estimates the JOIN's output rows for the emit term.
    let mut equi_pairs: Vec<(Node<'mcx>, Node<'mcx>)> = Vec::new();
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
            // Dup-flip guard input: this clause's key on the smaller rel.
            if key_var(a, small_rti).is_some() {
                small_keys.push(a);
            } else if key_var(b, small_rti).is_some() {
                small_keys.push(b);
            }
            equi_pairs.push((a, b));
        }
    }
    if n_equi == 0 {
        return refuse_join("no hashjoinable int-family equi clause");
    }
    // DUP-FLIP ELECTION GUARD (GL-MBSEAT-1 named hazard, 2026-07-21;
    // notes/se-mbseat.md §3): a dup-dense key on the SMALLER rel carries
    // the bucket-stats penalty that makes the SERIAL election flip the
    // build side onto the BIG rel — the elected probe side (the small rel)
    // then sits under the arm's 64-granule tiny-input floor and the
    // suppression lands on a silent refusal -> serial rerun, measured
    // **10.1x** rt/legacy at the reproducer (1M fact x 100k build, 8
    // dups/key, dop4, floors-off vehicle; witness runtime:absent).
    // PROVENANCE (bracket at that geometry, EXPLAIN election): 2/3/4/6
    // dups keep the small rel as the build; 8 flips — the flip point sits
    // in (6, 8]. The guard trips ABOVE 4 (headroom below the witnessed
    // band, the boundary-guard 5/4-headroom idiom: the flip point moves
    // with stats/geometry and a missed flip costs 10x, while the widest
    // witnessed-engaged dup class — the vehicle's 2-dups cell, 0.46-1.25
    // across the whole GL-MBSEAT-1 grid — stays keyed with margin).
    // Evidence-only: a stats-free key never trips it (get_variable_
    // numdistinct's DEFAULT answer is not evidence; the stats-free
    // election hazard stays with the X5/X6 family). Election-risk guard,
    // the B1/EC-disjointness family: unconditional, NOT floor-gated.
    const DUP_FLIP_MAX: f64 = 4.0;
    let small_rows = side_rows[small].max(1.0);
    for &v_node in &small_keys {
        let id = run.intern_expr(v_node);
        let vd = crate::selfuncs::examine_variable(run, id, v_node, 0)?;
        let (nd, isdefault) = crate::selfuncs::get_variable_numdistinct(run, &vd);
        if !isdefault && nd > 0.0 && small_rows / nd > DUP_FLIP_MAX {
            return refuse_join("build-side key dup density above the election-flip band");
        }
    }
    // SE-JHEAP: the heap-side guards (stats on heap equi keys,
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
    // SE-NUMJOIN (CAR 2, knob-gated): plain sum/avg(NUMERIC) over
    // parallel-safe joined-rel arg exprs additionally admit — the
    // sum(price*(1-disc)) money-expression family (the plain-join arm's
    // export speaks the same relocated runtime-partial vocabulary via the
    // poly manifest).
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
    // SEAT-LIFT ORDERING (conversion-flips train): the lift's witnessed
    // band is the BOOTSTRAP census (cbstore rels, bare-int emits) — the
    // heap-fed and numeric-emit widenings below were not in its dataset
    // and carry their OWN floors (the jheap 1M min must not be bypassed by
    // the ceiling lift), so only pure-bootstrap shapes reach it.
    let seat_shaped = n_equi == 1 && int4_pair_only;
    // GL-HJMB-3 min bound on the lift: below the arm's tiny-input floor
    // (HJ_ARM_MIN_ROWS) the suppressed query lands on the arm's refusal —
    // serial, 2.2-2.3x worse than the forgone PHJ (witnessed 500k rung).
    // Fall through to the class guard, whose min_rows keeps Gather.
    // PGRUST_M5_SIZE_FLOORS=0 disables the bound with every other floor
    // (calibration boots).
    if seat_shaped
        && hjprobe_v2_live()
        && heap.is_empty()
        && n_numeric == 0
        && (max_rows >= HJ_ARM_MIN_ROWS || !size_floors_enabled())
    {
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
    // R2 OUTPUT-CARDINALITY estimate (the emit-term input; the
    // both-geometry grid witnessed the split: the one-clause member
    // OUT~N loses at dop4 while the two-clause selective member wins).
    // eqjoinsel form: out = N_l * N_r / prod_k max(ndv_l, ndv_r) —
    // per-var NDV from the same estimate_num_groups the grouped
    // classifiers consult; estimate errors move the emit term, never a
    // support gate (the letter's named quality caveat).
    let out_est = {
        let mut denom = 1.0f64;
        for &(a, b) in &equi_pairs {
            let ia = run.intern_expr(a);
            let nd_a =
                crate::selfuncs::estimate_num_groups(run, &[(ia, a)], side_rows[0].max(1.0))?;
            let ib = run.intern_expr(b);
            let nd_b =
                crate::selfuncs::estimate_num_groups(run, &[(ib, b)], side_rows[1].max(1.0))?;
            denom *= nd_a.max(nd_b).max(1.0);
        }
        (side_rows[0].max(1.0) * side_rows[1].max(1.0) / denom).max(1.0)
    };
    finish_out(
        run,
        CoverClass::CbHashJoinPlainAgg,
        relids[0],
        0.0,
        max_rows,
        0.0,
        true,
        Some(out_est),
    )
}

/// NLIDX rel-aware suppression entry (GL-NLIDX-2), called from the Gather
/// choke points that carry the rel (generate_useful_gather_paths and
/// create_partial_grouping_paths): suppress iff (a) the query SHAPE is the
/// arm's (memoized structural half below) and (b) THIS rel is the final
/// joinrel and its cheapest SERIAL path already is
/// NestPath(INNER, heap-SeqScan outer, parameterized btree IndexPath
/// inner) — the election itself is the guard, so a suppression can only
/// ever land on the exact serial plan the executor arm engages. `false` =
/// Gather stands exactly as today.
pub(crate) fn m5_suppress_gather_nlidx(
    run: &mut PlannerRun<'_>,
    rel_id: types_pathnodes::RelId,
) -> PgResult<bool> {
    // Same session gates as the bootstrap entry (engine=runtime + pool +
    // lane), then the nlidx knob pair — silent at default (knob-OFF never
    // recognizes the shape; no trace, Gather untouched).
    if run.root.query_level != 1
        || !(nlidx_enabled() && nlidx_exec_live())
        || !guc_tables::parallel_engine::m5_gather_suppression_active()
    {
        return Ok(false);
    }
    if !classify_nlidx_shape(run)? {
        return Ok(false);
    }
    // The banked-ladder dop band applies to EVERY suppression this probe
    // makes (below dop 8 classic Gather wins and stands, base rels
    // included).
    if size_floors_enabled() && guc_tables::runtime_pool::runtime_dop() < 8 {
        if trace_armed() {
            eprintln!("m5-suppress-floor: nlidx dop band (runtime_dop < 8)");
        }
        return Ok(false);
    }
    // SCAN/JOIN rels only: upper rels (notably the partially-grouped rel,
    // whose MAIN pathlist is populated exclusively by its gathers — the
    // gather_grouping_paths "could not devise" trap documented at that
    // site) must never be suppressed by this probe.
    let kind = run.root.rel(rel_id).reloptkind;
    if kind != types_pathnodes::RELOPT_BASEREL && kind != types_pathnodes::RELOPT_JOINREL {
        return Ok(false);
    }
    // BASE rels of the keyed 2-rel shape: suppress their own Gather forms
    // too — otherwise `NL(Gather(base), IndexProbe)` (the join ABOVE a
    // gathered driver) both pollutes the serial-subset election below and
    // survives the final-rel suppression as a residual Gather plan the
    // executor arm refuses. Regression-free: partial paths (which feed the
    // Finalize/Gather/Partial and Gather-over-rows forms at the final rel)
    // are untouched, so a query whose final election is NOT the arm's
    // shape keeps its parallel plans through the final-rel refusal.
    if !crate::relnode::relids_equal(&run.root.rel(rel_id).relids, &run.root.all_query_rels) {
        if kind != types_pathnodes::RELOPT_BASEREL {
            return Ok(false);
        }
        if trace_armed() {
            eprintln!("m5-suppress-nlidx: base-rel gather suppressed (rti set of the keyed shape)");
        }
        return Ok(true);
    }
    // THE ELECTION CHECK: the cheapest SERIAL path — the min-total-cost
    // non-Gather top of the pathlist (cheapest_total_path may already be a
    // Gather at the later choke points; the serial-subset election is what
    // survives the suppression) — must be the arm's shape, through a
    // Projection wrapper if the scanjoin target already applied. A
    // Memoize/Material-wrapped inner refuses (the executor arm refuses
    // those inners by shape — suppress-then-serial otherwise).
    let mut best: Option<(f64, types_pathnodes::PathId)> = None;
    for &pid in run.root.rel(rel_id).pathlist.iter() {
        let node = run.root.path(pid);
        if matches!(
            node,
            types_pathnodes::PathNode::GatherPath(_)
                | types_pathnodes::PathNode::GatherMergePath(_)
        ) {
            continue;
        }
        let c = node.base().total_cost;
        if best.is_none_or(|(bc, _)| c < bc) {
            best = Some((c, pid));
        }
    }
    let Some((_, mut top_id)) = best else { return Ok(false) };
    if let types_pathnodes::PathNode::ProjectionPath(pp) = run.root.path(top_id) {
        let Some(sub) = pp.subpath else { return Ok(false) };
        top_id = sub;
    }
    let types_pathnodes::PathNode::NestPath(np) = run.root.path(top_id) else {
        return refuse_nlidx("serial election is not a nested loop");
    };
    if np.jpath.jointype != types_nodes::JoinType::JOIN_INNER as u32 {
        return refuse_nlidx("serial NL is not INNER");
    }
    let (Some(outer_id), Some(inner_id)) = (np.jpath.outerjoinpath, np.jpath.innerjoinpath)
    else {
        return Ok(false);
    };
    // The scanjoin-target application may wrap the outer in a Projection
    // (a plan-time artifact — createplan's physical-tlist optimization
    // emits the bare SeqScan node, as the serial EXPLAIN shows).
    let mut outer_id = outer_id;
    if let types_pathnodes::PathNode::ProjectionPath(pp) = run.root.path(outer_id) {
        let Some(sub) = pp.subpath else { return Ok(false) };
        outer_id = sub;
    }
    let outer_parent = match run.root.path(outer_id) {
        types_pathnodes::PathNode::Path(pp)
            if pp.pathtype == crate::pathnode::tag16(types_nodes::NodeTag::T_SeqScan)
                && !pp.parallel_aware =>
        {
            pp.parent
        }
        _ => return refuse_nlidx("serial NL outer is not a plain seqscan"),
    };
    if run.root.rel(outer_parent).amflags & AMFLAG_PGRCOLUMNAR != 0 {
        return refuse_nlidx("driver side is cbstore");
    }
    match run.root.path(inner_id) {
        types_pathnodes::PathNode::IndexPath(ip) => {
            let btree = ip
                .indexinfo
                .is_some_and(|ix| ix.relam == BTREE_AM_OID && ix.indexprs.is_empty() && ix.indpred.is_empty());
            if !btree {
                return refuse_nlidx("serial NL inner index is not a plain btree");
            }
            // Parameterized probe (the inner index cond carries the join
            // key): an unparameterized inner would rescan the whole index
            // per outer row — never the census family, and the executor
            // arm's economics were never measured on it.
            if ip.indexclauses.is_empty() || ip.path.param_info.is_none() {
                return refuse_nlidx("serial NL inner is not a parameterized index probe");
            }
        }
        _ => return refuse_nlidx("serial NL inner is not an index probe"),
    }
    // Floors: the banked-ladder dop band (dop>=8 — below it classic
    // Gather-NL wins and stands) + the driver-scan pages floor, through
    // the shared knob-path finish (trace vocabulary: m5-suppress-nlidx /
    // m5-suppress-floor: nlidx).
    let driver = run.root.rel(outer_parent);
    let (rows, pages) = (driver.rows.max(0.0), f64::from(driver.pages));
    let relid = {
        let rti = driver.relid as usize;
        run.parse()
            .rtable
            .nth(rti - 1)
            .as_range_tbl_entry()
            .map(|rte| rte.relid)
            .unwrap_or(0)
    };
    finish_knob_path(
        run,
        "nlidx",
        "nlidx-plain",
        FloorGuard { min_dop: 8, min_pages: nlidx_min_driver_pages(), ..NO_GUARD },
        relid,
        0.0,
        rows,
        pages,
    )
}

/// NLIDX query-shape half (memoized on the run): plain one-row
/// aggregation over a 2-rel INNER join form of plain HEAP rels with
/// parallel-safe quals and an arm-admissible tlist (plain int-family
/// folds or — poly-knob-coherent — plain sum/avg(numeric) with
/// parallel-safe args). Rel-independent; the election check above is the
/// other half. `false` memoizes (one trace per query, not per rel-offer).
fn classify_nlidx_shape(run: &mut PlannerRun<'_>) -> PgResult<bool> {
    if let Some(v) = run.m5_nlidx_shape {
        return Ok(v);
    }
    let v = classify_nlidx_shape_uncached(run)?;
    run.m5_nlidx_shape = Some(v);
    Ok(v)
}

fn classify_nlidx_shape_uncached(run: &mut PlannerRun<'_>) -> PgResult<bool> {
    let parse = run.parse();
    // The classify_covered structural prefilter, replicated (this entry is
    // reached from the rel-aware choke points, not through
    // classify_covered).
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
    // Plain one-row aggregation only.
    if !parse.hasAggs
        || !parse.groupClause.is_nil()
        || !parse.distinctClause.is_nil()
        || !parse.sortClause.is_nil()
        || parse.limitCount.is_some()
        || parse.limitOffset.is_some()
    {
        return refuse_nlidx("not a plain one-row aggregation");
    }
    // The serial election must be able to pick NL-with-inner-index at all.
    if !costsize::gucs::enable_nestloop() || !costsize::gucs::enable_indexscan() {
        return refuse_nlidx("nestloop/indexscan election disabled");
    }
    // 2-rel INNER form: two flat RangeTblRefs, or one INNER JoinExpr over
    // two RangeTblRefs (the two forms the planner leaves at probe time).
    let Some(top) = parse.jointree else { return Ok(false) };
    let (rti_l, rti_r, quals_node) = if top.fromlist.len() == 2 {
        let (Some(ra), Some(rb)) = (
            top.fromlist.nth(0).as_range_tbl_ref(),
            top.fromlist.nth(1).as_range_tbl_ref(),
        ) else {
            return Ok(false);
        };
        (ra.rtindex as usize, rb.rtindex as usize, top.quals)
    } else if top.fromlist.len() == 1 {
        let Some(je) = top.fromlist.nth(0).as_join_expr() else { return Ok(false) };
        if je.jointype != types_nodes::JoinType::JOIN_INNER {
            return refuse_nlidx("join family");
        }
        let (Some(ra), Some(rb)) = (je.larg.as_range_tbl_ref(), je.rarg.as_range_tbl_ref())
        else {
            return Ok(false);
        };
        (ra.rtindex as usize, rb.rtindex as usize, je.quals)
    } else {
        return Ok(false);
    };
    // Both sides plain HEAP relations; self-joins refuse.
    let mut relids = [0u32; 2];
    for (i, &rti) in [rti_l, rti_r].iter().enumerate() {
        let Some(rte) = parse.rtable.nth(rti - 1).as_range_tbl_entry() else {
            return refuse_nlidx("side not a plain RTE");
        };
        if rte.rtekind != RTEKind::RTE_RELATION
            || rte.relkind != types_rel::RELKIND_RELATION
            || rte.inh
            || rte.tablesample.is_some()
        {
            return refuse_nlidx("side not a plain relation");
        }
        relids[i] = rte.relid;
        let Some(rel_id) = run.root.simple_rel_array.get(rti).copied().flatten() else {
            return refuse_nlidx("side has no RelOptInfo yet");
        };
        if run.root.rel(rel_id).amflags & AMFLAG_PGRCOLUMNAR != 0 {
            return refuse_nlidx("side is cbstore");
        }
    }
    if relids[0] == relids[1] {
        return refuse_nlidx("self-join");
    }
    // Arm-admissible tlist: whitelisted plain folds, or (poly-coherent)
    // plain sum/avg(NUMERIC) with parallel-safe args.
    let mut n = 0usize;
    let mut n_numeric = 0usize;
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
        if is_whitelisted_agg_2rti(tle.expr, rti_l, rti_r, PLAIN_FOLD_AGGS) {
            n += 1;
            continue;
        }
        let Some(agg) = tle.expr.as_aggref() else {
            return refuse_nlidx("tlist entry not a whitelisted plain agg");
        };
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
            return refuse_nlidx("tlist entry not a whitelisted plain agg");
        }
        let Some(arg_tle) = agg.args.nth(0).as_target_entry() else { return Ok(false) };
        if !crate::is_parallel_safe_opt(run, Some(arg_tle.expr))? {
            return refuse_nlidx("numeric agg arg not parallel-safe");
        }
        n += 1;
        n_numeric += 1;
    }
    if n == 0 {
        return refuse_nlidx("empty tlist");
    }
    if n_numeric > 0 && !agg_poly_probe_enabled() {
        return refuse_nlidx("numeric fold with the poly export knob killed");
    }
    // Worker-side expression safety (the executor's walk, mirrored — an
    // unsafe qual would refuse at exec, the suppress-then-serial
    // direction).
    if !crate::is_parallel_safe_opt(run, quals_node)? {
        return refuse_nlidx("quals not parallel-safe");
    }
    Ok(true)
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
    use costsize::runtime_model as rtm;
    let class = CoverClass::CbHashJoinPlainAgg;
    let covered = class_covered(class);
    // Step-2 shadow census on the seat-lifted path (the t36 pinned
    // seat-vs-curve OVERLAP DEBT, TSV seat_overlap_cell row): the seat
    // suppresses OUTSIDE `finish`, so without this branch the debt cell —
    // seat band witnessed only >= 2.5M while the curve witnessed a 1.319x
    // loss at 1M@dop4 — would be invisible to the disagreement census. The
    // whitelist verdict here IS the seat's suppress (true); the curve prices
    // the identical shape. OBSERVATION ONLY — the seat's verdict is already
    // returned below and nothing here is read back into it.
    if covered && size_floors_enabled()
        && !matches!(rtm::cost_route_mode(), rtm::CostRouteMode::Off)
    {
        let dop = guc_tables::runtime_pool::runtime_dop();
        // R1 regime split mirrored here too (the seat path never sees
        // pages; the hashjoin admission mirror is rows-only). Three-way
        // since the GL-ELECTION-22 gating fix (serial competes even when
        // the arm admits; the hashjoin serial curve never wins in its
        // measured range, so the seat census is verdict-identical —
        // coherence, not behavior).
        let v = if rtm::threeway_enabled() {
            rtm::cost_route_verdict_threeway(
                rtm::RuntimeClass::CbHashJoinPlainAgg,
                rows,
                dop,
                rows >= HJ_ARM_MIN_ROWS,
                true,
            )
        } else {
            rtm::cost_route_verdict_regime(
                rtm::RuntimeClass::CbHashJoinPlainAgg,
                rows,
                dop,
                rows >= HJ_ARM_MIN_ROWS,
            )
        };
        let (n_ws_mg, n_wg_ms) = cost_shadow::note(class, true, v.suppress);
        if trace_armed() && !v.suppress {
            eprintln!(
                "m5-cost-census: class={class:?} wl=suppress model=gather \
                 n_wl_suppress_model_gather={n_ws_mg} \
                 n_wl_gather_model_suppress={n_wg_ms}"
            );
        }
        cost_shadow::record_sample(cost_shadow::ExplainSample {
            class: cost_shadow::CLASS_NAMES[cost_shadow::class_idx(class)],
            ratio: v.ratio,
            model_suppress: v.suppress,
            whitelist_suppress: true,
            decided_by: "seat",
            rows,
            dop,
        });
    }
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

/// SE-NUMJOIN (CAR 2): a structurally plain `sum(NUMERIC)` /
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
    // SE-JHEAP: the heap-side guards (stats on heap equi keys,
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
    // SE-NUMJOIN (CAR 2, knob-gated): plain sum/avg(NUMERIC) over
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
            hj_knobpath_2m_guard(),
            relids[0],
            0.0,
            max_rows,
            0.0,
        );
    }
    finish(run, CoverClass::CbHashJoinMultiBuild, relids[0], 0.0, max_rows, 0.0, true)
}

/// The multibuild per-relation guards, shared by the plain and grouped rows
/// (extracted verbatim at SE-AGGJOIN): plain DISTINCT rels (the B1
/// self-join discipline), EVERY rel's build estimate nbatch==1; cbstore
/// rels stay unindexed-only verbatim. SE-JHEAP: HEAP rels admit
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
            // SE-JHEAP: a non-cbstore plain relation is the heap AM (the
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
        // BOUNDARY GUARD (GL-HJMB-1 escalation A), multibuild mirror: the
        // walk is unbatched-only AND demote-less, so a build in the
        // estimate-unbatched-but-truly-crossing band refuses at seal into
        // the serial-rerun cliff. Keep Gather for the band (the arm keeps
        // its behavior — no batch option exists for trees; the probe is
        // where PHJ can still be chosen).
        if ::nodehash::estimate_runtime_hj_build_peak_bytes(rel.rows.max(1.0), width)
            > ::nodehash::get_hash_memory_limit().saturating_mul(dop.max(1) as usize + 1) as u64
        {
            return refuse_join_none(
                "build estimate within the demote-unsafe envelope band (boundary guard)",
            );
        }
    }
    Ok(Some((relids, max_rows, heap)))
}

/// Traced refusal in `Option` position (the rel-guards helper's shape).
fn refuse_join_none<T>(why: &str) -> PgResult<Option<T>> {
    let _ = refuse_join(why)?;
    Ok(None)
}

/// SE-JHEAP: the heap-side shape guards over the WHOLE qual set — run by
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
///   * index tolerance (the AggPolyHeapPlain precedent, join-widened;
///     the conversion-target rels carry their PK indexes, so a blanket unindexed rule
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
        // (this rel's edge attno, partner rel index) per incident edge —
        // the NL-margin law is PER EDGE: a parameterized inner-index path
        // on THIS rel exists only for edges whose join column the index
        // covers (an index on a different column parameterizes nothing).
        let edge_partners: Vec<(i16, usize)> = edges
            .iter()
            .filter_map(|&(ia, aa, ib, ab)| {
                if ia == i {
                    Some((aa, ib))
                } else if ib == i {
                    Some((ab, ia))
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
            // Join-key index: the NL/merge hazard, PER EDGE — only the
            // partners of edges whose join column this index covers can
            // elect an inner-index path on this rel; each such partner
            // must dominate this rel by the margin.
            for &(attno, p) in &edge_partners {
                if !keys[..nkey].iter().any(|&k| k == i32::from(attno)) {
                    continue;
                }
                let Some(p_rel) = run.root.simple_rel_array.get(rtis[p]).copied().flatten()
                else {
                    return Ok(false);
                };
                let p_rows = run.root.rel(p_rel).rows.max(0.0);
                if p_rows < JHEAP_NL_MARGIN * rel_rows {
                    return refuse_join("heap join-key index without the NL-election margin");
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

/// SE-FILTERQUALS: one pushed filter term's admission census. `Some(i)` =
/// an admitted single-rel restriction on joined rel index `i`; `None` = not
/// this shape (the caller refuses, keeping Gather). Admitted, fail-closed:
///   * `OpExpr((Relabel'd) Var, non-null Const)` either side, or
///     `ScalarArrayOpExpr((Relabel'd) Var, non-null Const array)` (the IN /
///     ALL forms) — SIMPLE restrictions whose selectivity the planner
///     grounds in pg_statistic;
///   * the Var carries statistics (`key_var_estimable` — the X5 lesson:
///     the merge election was driven by a stats-DEFAULTING expr term);
///   * the whole term `is_parallel_safe` (it runs on helpers through the
///     scan feeds' per-row qual re-check).
/// Refused by shape (named at the caller): var-var terms (same-rel
/// column-column compares — no grounding), expr restrictions (X5’s
/// `f.v % 3 = 0` class), OR trees, NULL consts, volatile/unsafe exprs.
fn classify_filter_term<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rtis: &[usize],
    qual: Node<'mcx>,
) -> PgResult<Option<usize>> {
    let strip = |e: Node<'mcx>| e.as_relabel_type().map_or(e, |r| r.arg);
    let var_side = |e: Node<'mcx>| {
        let e = strip(e);
        rtis.iter().position(|&rti| key_var(e, rti).is_some()).map(|i| (i, e))
    };
    let const_ok = |e: Node<'mcx>| e.as_const().is_some_and(|c| !c.constisnull);
    let (i, v_node) = if let Some(op) = qual.as_op_expr() {
        if op.opretset || op.args.len() != 2 {
            return Ok(None);
        }
        let (a, b) = (op.args.nth(0), op.args.nth(1));
        match (var_side(a), const_ok(b), var_side(b), const_ok(a)) {
            (Some(x), true, _, _) => x,
            (_, _, Some(x), true) => x,
            _ => return Ok(None),
        }
    } else if let Some(saop) = qual.as_scalar_array_op_expr() {
        if saop.args.len() != 2 {
            return Ok(None);
        }
        match (var_side(saop.args.nth(0)), const_ok(saop.args.nth(1))) {
            (Some(x), true) => x,
            _ => return Ok(None),
        }
    } else {
        return Ok(None);
    };
    if !key_var_estimable(run, v_node)? {
        return Ok(None);
    }
    if !crate::is_parallel_safe_opt(run, Some(qual))? {
        return Ok(None);
    }
    Ok(Some(i))
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
    // Bare grouped aggregation, or (SE-DECOROOT, CAR 1) a WHITELISTED
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
    // SE-JHEAP: the heap-side guards (index tolerance + NL margin;
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
    let mut n_filters = 0usize;
    for &qual in quals {
        // Two-DISTINCT-rel bare-var terms are JOIN terms: they keep the
        // X5/X6 equi discipline verbatim (never filter candidates).
        let two_rel = qual.as_op_expr().and_then(|op| {
            if op.args.len() != 2 {
                return None;
            }
            let (a, b) = (op.args.nth(0), op.args.nth(1));
            let hit = |e: Node<'_>| rtis.iter().position(|&rti| key_var(e, rti).is_some());
            match (hit(a), hit(b)) {
                (Some(ia), Some(ib)) if ia != ib => Some((op, a, b, ia, ib)),
                _ => None,
            }
        });
        if let Some((op, a, b, ia, ib)) = two_rel {
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
            continue;
        }
        // SE-FILTERQUALS (knob-gated): single-rel stats-grounded simple
        // restrictions admit; everything else keeps the X5 refusal
        // byte-for-byte (knob OFF never reaches the classifier).
        if joinfilters_enabled() && classify_filter_term(run, rtis, qual)?.is_some() {
            n_filters += 1;
            continue;
        }
        return refuse_join("non-equi qual (costing can elect merge/sort shapes)");
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
    // SE-CBKEYS (knob-gated): bare text/varchar Vars under the
    // deterministic DEFAULT collation additionally admit (the canonical-
    // bytes key export). BPCHAR refuses BY NAME knob-on (space-insensitive
    // bpchareq — outside the byte-equality envelope, the scan sinks'
    // standing exclusion; census char(n) keys wait on the tie-law car).
    let mut key_refs: Vec<u32> = Vec::new();
    let mut n_bytes_keys = 0usize;
    let mut n_bpchar_keys = 0usize;
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
        } else if cbkeys_enabled()
            && bpchar_keys_enabled()
            && v.vartype == BPCHAROID
            && v.varcollid == DEFAULT_COLLATION_OID
            && v.vartypmod >= 5
        {
            // SE-BPCHAR: the tie law — a bare-Var char(n) key's stored
            // images are canonical (see bpchar_keys_enabled's doc), so it
            // rides the canonical-bytes export as-is.
            n_bytes_keys += 1;
            n_bpchar_keys += 1;
        } else {
            if cbkeys_enabled() && v.vartype == BPCHAROID {
                if bpchar_keys_enabled() && v.vartypmod < 5 {
                    return refuse_join(
                        "bpchar group key without a typmod (unpadded storage outside the tie law)",
                    );
                }
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
    // states the scan-grouped GROUPED_SINK_AGGS row refuses). SE-NUMJOIN
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
    // SE-DECOROOT hash-election margin: a decorated root makes the
    // sorted-agg serial shape competitive near ngroups≈input (the costing
    // can elect Sort+GroupAggregate the walk refuses — suppress-then-refuse,
    // costing flavor); require the hash election safely dominant.
    if decorated && ngroups * DECOROOT_NGROUPS_MARGIN > max_rows {
        return refuse_join("decorated root without hash-election margin (ngroups too close to input)");
    }
    // Knob-admitted shapes route through the dedicated knob-path finishes
    // (own trace tags, greppable apart from the bootstrap `m5-suppress:`
    // census line; the class row / tsv / drift guards untouched). Heap-fed
    // shapes carry the jheap floor (min 1M) — SE-JHEAP owns the tag; the
    // pure decorated/numeric widenings keep the CbHashJoinGroupedAgg floor
    // — the arm underneath is the same grouped sink either way.
    // SE-FILTERQUALS: filtered shapes route under the joinfilters tag
    // (every such shape is unkeyable without the knob), composing the
    // riders in the label; the binding floor is the strictest of the
    // composed cars. Post-filter estimates already flowed through
    // max_rows/ngroups/margins above (RelOptInfo.rows is post-restriction).
    if n_filters > 0 {
        let mut label = String::from("aggjoin-grouped-filtered");
        if n_bytes_keys > 0 {
            label.push_str("-cbkeys");
        }
        if n_bpchar_keys > 0 {
            label.push_str("-bpchar");
        }
        if !heap.is_empty() {
            label.push_str("-heap");
        }
        if decorated {
            label.push_str("-decorated");
        }
        if n_numeric > 0 {
            label.push_str("+numeric");
        }
        let guard = if !heap.is_empty() {
            jheap_guard()
        } else if n_bytes_keys > 0 {
            cbkeys_guard()
        } else {
            // GL-COST-2 carve: the knob path keeps its letters' measured
            // rectangle; the rider's zeroed guard governs only finish().
            hj_knobpath_2m_guard()
        };
        return finish_knob_path(
            run,
            "joinfilters",
            &label,
            guard,
            relids[0],
            ngroups,
            max_rows,
            0.0,
        );
    }
    // SE-CBKEYS: bytes-keyed shapes route under the cbkeys tag (their
    // own kill's greppable line), composing with the heap/decorated/
    // numeric riders in the label; the binding floor is the strictest of
    // the composed cars (heap's 1M min when heap sides ride along).
    if n_bytes_keys > 0 {
        let mut label = String::from("aggjoin-grouped-cbkeys");
        if n_bpchar_keys > 0 {
            label.push_str("-bpchar");
        }
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
            hj_knobpath_2m_guard(),
            relids[0],
            ngroups,
            max_rows,
            0.0,
        );
    }
    // GL-MBSEAT-1 GUARD LIFT ngroups bound: the seated curve's win region
    // was witnessed at ngroups 10 and 1k (the 1k twin tracks within
    // 0.03-0.11 at every cell); 32k LOSES 1.86-2.48 even seated — the
    // export/combine/absorb grouped tail is not seat-addressable, and the
    // fitted model carries no ngroups axis. Bound the CURVE path at the
    // last witnessed win axis point (the 1k..32k boundary is unmeasured;
    // TSV row ngroups_lift_max). Applies ONLY when the own-curve wiring is
    // live — every kill posture already keeps Gather via the rectangle,
    // and the HJRIDER A/B vehicles keep their pre-letter behavior.
    const NGROUPS_LIFT_MAX: f64 = 1024.0;
    if !hjrider_curve_enabled()
        && mbshared_live()
        && mbseat_live()
        && ngroups > NGROUPS_LIFT_MAX
    {
        return refuse_join("grouped ngroups above the seated win region (curve path)");
    }
    finish(run, CoverClass::CbHashJoinGroupedAgg, relids[0], ngroups, max_rows, 0.0, true)
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
        CoverClass::CbHashJoinPlainAgg => Some(Rc::CbHashJoinPlainAgg),
        // GL-COST-2 UNWIRE: the multibuild rider's witnessed grid refuted
        // the PlainAgg curve reuse (see class_guard) — NO curve at default;
        // the class routes by its guarded-off rectangle. The one-train
        // kill restores the refuted wiring for A/B vehicles only.
        CoverClass::CbHashJoinMultiBuild => {
            if hjrider_curve_enabled() { Some(Rc::CbHashJoinPlainAgg) } else { None }
        }
        // GL-MBSEAT-1 GUARD LIFT: the grouped rider decides by its OWN
        // fitted curve — valid ONLY on the seated arm (both seat-world
        // mirrors live), fitted from the 9-cell seated grid @ 39d74f143
        // (win region dop>=8 / 1M-2.5M; the classifier bounds the path at
        // ngroups <= NGROUPS_LIFT_MAX). Either kill un-curves the class
        // back to the guarded-off rectangle; the HJRIDER A/B knob keeps
        // its pre-letter meaning for vehicles.
        CoverClass::CbHashJoinGroupedAgg => {
            if hjrider_curve_enabled() {
                Some(Rc::CbHashJoinPlainAgg)
            } else if mbshared_live() && mbseat_live() {
                Some(Rc::CbHashJoinGroupedAgg)
            } else {
                None
            }
        }
        // PROVISIONAL reuse matching the shipped guard reuse (GL-AGGPOLY-1).
        CoverClass::AggPolyHeapPlain => Some(Rc::HeapCmpFoldPrefix),
        // Curve-fit since the witnessed v2 grid (the v1 record's
        // non-monotonic N profile was contamination — GL-COST-3).
        CoverClass::CbGroupedAggTextKey => Some(Rc::CbGroupedAggTextKey),
        // Footer answers are O(1): never floored, no curve.
        CoverClass::CbMetaFooterAgg => None,
        // Rectangle-retained: per-AM PROVISIONAL floors in m5_partwise.rs
        // (GL-PARTWISE-1); own curve cells ride the named floor-calibration
        // follow-up (runtime-cost-constants.tsv row).
        CoverClass::PartwisePlainFold => None,
    }
}

// ---------------------------------------------------------------------------
// Step-2 shadow routing observability (runtime-cost-model design §5 step 2):
// disagreement census + knob-gated EXPLAIN sample. PURE OBSERVATION — nothing
// in this module feeds back into a suppression verdict, so routing is
// byte-identical with and without it (the off-path Ir bar: an uncovered query
// never reaches `finish`, and a covered one takes only counter increments).
// ---------------------------------------------------------------------------

pub mod cost_shadow {
    //! Per-CoverClass whitelist-vs-model disagreement counters plus the
    //! last-planned-sample slot the knob-gated EXPLAIN line reads.
    //!
    //! CENSUS: `note` is called from `finish` wherever BOTH verdicts exist
    //! (covered class, fitted curve, cost mode not Off, floors enabled — the
    //! floors-off measurement vehicle deliberately does not count: its
    //! "whitelist verdict" is forced true and would poison the census).
    //! Four cells per class:
    //!   agree_suppress / agree_gather — both mechanisms concur;
    //!   wl_suppress_model_gather      — whitelist says engage the runtime
    //!                                   (suppress Gather), model says legacy;
    //!   wl_gather_model_suppress      — whitelist says legacy, model says
    //!                                   the runtime pays (the forgone-win
    //!                                   direction).
    //! Counters are process-cumulative atomics; under PGRUST_M5_SUPPRESS_TRACE
    //! every disagreement also emits one `m5-cost-census:` stderr line with
    //! the cumulative per-class cells (the e2e census vehicle).
    //!
    //! EXPLAIN SAMPLE: behind `PGRUST_M5_COST_EXPLAIN` (default OFF — any
    //! spelling but `1`/`on` is OFF, the scanpass fail-safe idiom). When
    //! armed, `finish` records the query's shadow sample in a thread-local
    //! slot; `standard_planner` clears the slot at entry (stale-sample
    //! hygiene — a query that never classifies covered must print nothing);
    //! EXPLAIN takes it right after planning. When the knob is OFF the slot
    //! is never touched: clear/record/take all no-op behind one cached-bool
    //! load (the probe's own inertness idiom), and EXPLAIN output is
    //! byte-identical to today.

    use super::CoverClass;
    use std::cell::Cell;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// One census-indexed slot per CoverClass variant (see `class_idx`).
    const N_CLASSES: usize = 14;

    pub(super) fn class_idx(class: CoverClass) -> usize {
        match class {
            CoverClass::CbPlainAggFold => 0,
            CoverClass::CbGroupedAggIntKeys => 1,
            CoverClass::CbGroupedAggTextKey => 2,
            CoverClass::CbGroupedAggTopN => 3,
            CoverClass::CbDistinctIntKeys => 4,
            CoverClass::HeapPlainCountStar => 5,
            CoverClass::HeapCmpFoldPrefix => 6,
            CoverClass::CbTopnBoundedIntKeys => 7,
            CoverClass::CbHashJoinPlainAgg => 8,
            CoverClass::CbHashJoinMultiBuild => 9,
            CoverClass::CbHashJoinGroupedAgg => 10,
            CoverClass::AggPolyHeapPlain => 11,
            CoverClass::CbMetaFooterAgg => 12,
            CoverClass::PartwisePlainFold => 13,
        }
    }

    pub const CLASS_NAMES: [&str; N_CLASSES] = [
        "CbPlainAggFold",
        "CbGroupedAggIntKeys",
        "CbGroupedAggTextKey",
        "CbGroupedAggTopN",
        "CbDistinctIntKeys",
        "HeapPlainCountStar",
        "HeapCmpFoldPrefix",
        "CbTopnBoundedIntKeys",
        "CbHashJoinPlainAgg",
        "CbHashJoinMultiBuild",
        "CbHashJoinGroupedAgg",
        "AggPolyHeapPlain",
        "CbMetaFooterAgg",
        "PartwisePlainFold",
    ];

    const ZERO: AtomicU64 = AtomicU64::new(0);
    static AGREE_SUPPRESS: [AtomicU64; N_CLASSES] = [ZERO; N_CLASSES];
    static AGREE_GATHER: [AtomicU64; N_CLASSES] = [ZERO; N_CLASSES];
    static WL_SUPPRESS_MODEL_GATHER: [AtomicU64; N_CLASSES] = [ZERO; N_CLASSES];
    static WL_GATHER_MODEL_SUPPRESS: [AtomicU64; N_CLASSES] = [ZERO; N_CLASSES];

    /// Count one (whitelist verdict, model verdict) pair. Returns the
    /// (wl_suppress_model_gather, wl_gather_model_suppress) cumulative pair
    /// for the class so the caller's trace line can print it without a
    /// second load.
    pub(super) fn note(class: CoverClass, wl_suppress: bool, model_suppress: bool) -> (u64, u64) {
        let i = class_idx(class);
        match (wl_suppress, model_suppress) {
            (true, true) => {
                AGREE_SUPPRESS[i].fetch_add(1, Ordering::Relaxed);
            }
            (false, false) => {
                AGREE_GATHER[i].fetch_add(1, Ordering::Relaxed);
            }
            (true, false) => {
                WL_SUPPRESS_MODEL_GATHER[i].fetch_add(1, Ordering::Relaxed);
            }
            (false, true) => {
                WL_GATHER_MODEL_SUPPRESS[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        (
            WL_SUPPRESS_MODEL_GATHER[i].load(Ordering::Relaxed),
            WL_GATHER_MODEL_SUPPRESS[i].load(Ordering::Relaxed),
        )
    }

    /// One census row (cumulative, process-wide).
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct CensusRow {
        pub class: &'static str,
        pub agree_suppress: u64,
        pub agree_gather: u64,
        pub wl_suppress_model_gather: u64,
        pub wl_gather_model_suppress: u64,
    }

    /// The full census snapshot (all classes, including all-zero rows).
    pub fn snapshot() -> [CensusRow; N_CLASSES] {
        core::array::from_fn(|i| CensusRow {
            class: CLASS_NAMES[i],
            agree_suppress: AGREE_SUPPRESS[i].load(Ordering::Relaxed),
            agree_gather: AGREE_GATHER[i].load(Ordering::Relaxed),
            wl_suppress_model_gather: WL_SUPPRESS_MODEL_GATHER[i].load(Ordering::Relaxed),
            wl_gather_model_suppress: WL_GATHER_MODEL_SUPPRESS[i].load(Ordering::Relaxed),
        })
    }

    /// The shadow sample of the last covered classification this thread
    /// planned — everything the EXPLAIN line prints.
    #[derive(Clone, Copy, Debug)]
    pub struct ExplainSample {
        pub class: &'static str,
        pub ratio: f64,
        pub model_suppress: bool,
        pub whitelist_suppress: bool,
        pub decided_by: &'static str,
        pub rows: f64,
        pub dop: i32,
    }

    /// The default-OFF spelling rule, factored pure for exhaustive unit
    /// tests (the scanpass idiom): ON iff exactly `1` or `on`.
    pub(super) fn explain_spelling_on(v: Option<&str>) -> bool {
        matches!(v, Some("1") | Some("on"))
    }

    /// `PGRUST_M5_COST_EXPLAIN` (default OFF): arm the EXPLAIN sample slot
    /// + the "M5 Cost Route" line in the explain crate.
    pub fn explain_armed() -> bool {
        static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *ON.get_or_init(|| {
            explain_spelling_on(std::env::var("PGRUST_M5_COST_EXPLAIN").as_deref().ok())
        })
    }

    thread_local! {
        /// Non-session TLS (census-classified in access/session tests):
        /// derived plan-time observability, cleared at every planner entry
        /// while armed; never read across a session boundary.
        static LAST_SAMPLE: Cell<Option<ExplainSample>> = const { Cell::new(None) };
    }

    /// Stale-sample hygiene at `standard_planner` entry. One cached-bool
    /// load when the knob is off.
    pub fn clear_last_sample() {
        if explain_armed() {
            LAST_SAMPLE.set(None);
        }
    }

    pub(super) fn record_sample(sample: ExplainSample) {
        if explain_armed() {
            LAST_SAMPLE.set(Some(sample));
        }
    }

    /// Take (and clear) the last sample. `None` whenever the knob is off.
    pub fn take_last_sample() -> Option<ExplainSample> {
        if explain_armed() { LAST_SAMPLE.take() } else { None }
    }
}

pub mod serial_shadow {
    //! Step-2 SERIAL-SIDE three-way shadow census (costsize::serial_model
    //! — the term for the serial lane's early-exit/zone-skip advantages
    //! the two-way rt/legacy curves cannot see). PURE OBSERVATION, the
    //! cost_shadow contract verbatim: nothing here feeds a suppression
    //! verdict; the three hand-carves this term prices (the selective-qual
    //! datetime-lead top-N carve, the truncated-timestamp fold page fence,
    //! and the plain-fold classify tail) keep deciding exactly as shipped.
    //!
    //! CENSUS: one row per serial family, cells keyed by (what the shipped
    //! mechanism ENFORCED, what the three-way model PICKS):
    //!   enforced ∈ {gather (carve/fence/floor kept the exchange),
    //!               suppress (the shape was handed to the serial-shaped
    //!               plan — the router/band decides the engine at exec)}
    //!   pick     ∈ {serial, runtime, gather}
    //! Agreement = enforced=gather & pick=gather, or enforced=suppress &
    //! pick∈{serial, runtime} (suppression's delivered engine is one of
    //! the two). Parity picks (top two engines within the band) are NOT
    //! counted — a tie is not routing evidence. Counted only when BOTH
    //! economics gates are live (cost mode not Off, floors on) so the
    //! floors-off measurement vehicle cannot poison the census, and only
    //! when the term is inside its witnessed support (it abstains below).
    //!
    //! Under PGRUST_M5_SUPPRESS_TRACE every counted sample also emits one
    //! `m5-serial-term:` stderr line with the three predicted walls, the
    //! pick, and the enforced verdict (the e2e/fleet census vehicle).

    use std::sync::atomic::{AtomicU64, Ordering};

    pub const FAMILY_NAMES: [&str; 3] = ["topn-nonint", "tstrunc-fold", "scanfold-meta"];
    pub const N_FAMILIES: usize = 3;
    pub const TOPN_NONINT: usize = 0;
    pub const TSTRUNC_FOLD: usize = 1;
    pub const SCANFOLD_META: usize = 2;

    /// [family][enforced(0=gather,1=suppress)][pick(0=serial,1=runtime,2=gather)]
    static CELLS: [[[AtomicU64; 3]; 2]; N_FAMILIES] = {
        #[allow(clippy::declare_interior_mutable_const)]
        const Z: AtomicU64 = AtomicU64::new(0);
        #[allow(clippy::declare_interior_mutable_const)]
        const ROW: [AtomicU64; 3] = [Z; 3];
        #[allow(clippy::declare_interior_mutable_const)]
        const ENF: [[AtomicU64; 3]; 2] = [ROW; 2];
        [ENF; N_FAMILIES]
    };

    pub(super) fn pick_idx(pick: costsize::serial_model::EnginePick) -> usize {
        use costsize::serial_model::EnginePick as P;
        match pick {
            P::Serial => 0,
            P::Runtime => 1,
            P::Gather => 2,
        }
    }

    /// Count one (enforced, pick) sample; returns the cell's cumulative
    /// count for the caller's trace line.
    pub(super) fn note(
        family: usize,
        enforced_suppress: bool,
        pick: costsize::serial_model::EnginePick,
    ) -> u64 {
        let cell = &CELLS[family][usize::from(enforced_suppress)][pick_idx(pick)];
        cell.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// One family's census row (cumulative, process-wide).
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct CensusRow {
        pub family: &'static str,
        /// [enforced(gather,suppress)][pick(serial,runtime,gather)]
        pub cells: [[u64; 3]; 2],
    }

    pub fn snapshot() -> [CensusRow; N_FAMILIES] {
        core::array::from_fn(|f| CensusRow {
            family: FAMILY_NAMES[f],
            cells: core::array::from_fn(|e| {
                core::array::from_fn(|p| CELLS[f][e][p].load(Ordering::Relaxed))
            }),
        })
    }

    /// Does the (enforced, pick) pair agree? (Suppression's delivered
    /// engine is serial OR runtime — the band/router decides at exec.)
    pub fn agrees(enforced_suppress: bool, pick: costsize::serial_model::EnginePick) -> bool {
        use costsize::serial_model::EnginePick as P;
        match (enforced_suppress, pick) {
            (false, P::Gather) => true,
            (true, P::Serial) | (true, P::Runtime) => true,
            _ => false,
        }
    }
}

/// Shared serial-shadow tail: census + trace for one classified shape.
/// Observation only (the cost_shadow contract); gated exactly like the
/// step-2 census — both economics gates live — and skipping parity picks
/// (a tie is not evidence). `verdict` is `None` when the term abstained
/// (below witnessed support / posture): nothing is counted, matching the
/// fail-toward-the-incumbent posture.
fn serial_shadow_tail(
    family: usize,
    label: &str,
    verdict: Option<costsize::serial_model::ThreeWay>,
    enforced_suppress: bool,
) {
    use costsize::runtime_model as rtm;
    if matches!(rtm::cost_route_mode(), rtm::CostRouteMode::Off) || !size_floors_enabled() {
        return;
    }
    let Some(v) = verdict else { return };
    if v.parity {
        return;
    }
    let n = serial_shadow::note(family, enforced_suppress, v.pick);
    if trace_armed() {
        eprintln!(
            "m5-serial-term: family={} label={label} t_ser={:.1} t_rt={} t_leg={:.1} \
             pick={} enforced={} agree={} n_cell={n}",
            serial_shadow::FAMILY_NAMES[family],
            v.t_serial,
            v.t_runtime.map_or("na".to_string(), |t| format!("{t:.1}")),
            v.t_gather,
            v.pick.name(),
            if enforced_suppress { "suppress" } else { "gather" },
            serial_shadow::agrees(enforced_suppress, v.pick),
        );
    }
}

/// R1 ARM-ADMISSION MIRROR (soak-adj round-2 §R2.4, built by the
/// serial-cost-term lane): the plan-computable predicate for "will the
/// arm OWN the suppressed plan?" — the regime input of
/// `cost_route_verdict_regime`. When it answers false, suppression
/// delivers the SERIAL lane and the verdict prices t_ser/t_leg on the
/// serial support floor instead of the engaged curve. Instances are
/// MIRRORS of witnessed engage floors, never new economics:
///   * cbstore classes — the scan/sort/agg arms' 64-granule geometry
///     floor (64 x 8192 = 524,288 rows; HJ_ARM_MIN_ROWS is the same
///     constant, S1's re-derivation): the nine-job grid witnessed
///     runtime:absent at <= 500k rows and engagement at >= 1M on every
///     cbstore class.
///   * CbGroupedAggTopN — F1's 500k post-qual floor (the sorted serial
///     election the arm refuses).
///   * HeapPlainCountStar — the rowdrive 64MB block floor
///     (admission_min_pages mirror, applied in every mode).
///   * HeapCmpFoldPrefix — no witnessed floor instance (the heap
///     cmp-fold arm engaged at the 100k cells): always admits.
fn arm_admission_mirror(class: CoverClass, rows: f64, pages: f64) -> bool {
    use costsize::runtime_model as rtm;
    match class {
        CoverClass::CbGroupedAggTopN => rows >= 500_000.0,
        CoverClass::HeapPlainCountStar => pages >= rtm::HEAP_COUNT_ADMISSION_MIN_PAGES,
        CoverClass::HeapCmpFoldPrefix => true,
        CoverClass::CbPlainAggFold
        | CoverClass::CbGroupedAggIntKeys
        | CoverClass::CbGroupedAggTextKey
        | CoverClass::CbDistinctIntKeys
        | CoverClass::CbTopnBoundedIntKeys
        | CoverClass::CbHashJoinPlainAgg
        | CoverClass::CbHashJoinMultiBuild
        | CoverClass::CbHashJoinGroupedAgg => rows >= HJ_ARM_MIN_ROWS,
        // Curveless / knob-path classes never reach the regime verdict.
        _ => true,
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
    // Three-way SERIAL-SIDE posture gate (GL-ELECTION-22 finding 2): does
    // the class's fitted serial curve describe THIS shape's serial
    // delivery? True for every class except the plain-fold family, whose
    // serial fit is the footer-META wall — its classify site passes the
    // plan-time META-band mirror (unqualed, or estimated survival ~1).
    serial_applies: bool,
) -> PgResult<bool> {
    finish_out(run, class, relid, ngroups, rows, pages, serial_applies, None)
}

/// [`finish`] with the R2 output-cardinality input (None = OUT := rows,
/// the one-clause posture — only the hashjoin classify passes Some).
#[allow(clippy::too_many_arguments)]
fn finish_out(
    run: &mut PlannerRun<'_>,
    class: CoverClass,
    relid: u32,
    ngroups: f64,
    rows: f64,
    pages: f64,
    serial_applies: bool,
    out_rows: Option<f64>,
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
                // R1 regime split (soak-adj round-2 §R2.4): when the
                // arm-admission mirror says the arm will NOT own the
                // suppressed plan, suppression delivers the SERIAL lane —
                // the verdict prices the witnessed serial curve
                // (t_ser/t_leg on the serial support floor) instead of
                // the engaged curve. arm_admits=true is byte-identical
                // to the pre-R1 verdict (pinned in runtime_model).
                let arm_admits = arm_admission_mirror(class, rows, pages);
                // GL-ELECTION-22 finding-2 fix: the THREE-WAY argmin —
                // suppression priced as the better of the engines the
                // suppressed plan can land on (arm where admitted, serial
                // where its curve applies) — so the serial term is
                // consulted even when the arm admits. PGRUST_M5_THREEWAY=
                // 0|off restores the regime-gated two-way for one train.
                let v = if rtm::threeway_enabled() {
                    rtm::cost_route_verdict_threeway_out(
                        curve, rows, dop, arm_admits, serial_applies, out_rows,
                    )
                } else {
                    rtm::cost_route_verdict_regime(curve, rows, dop, arm_admits)
                };
                // What the model WOULD do: the regime verdict composed
                // with the rowdrive block-floor ADMISSION MIRROR, which
                // rides every mode (m5-5 reading #3; TSV
                // admission_min_pages row).
                // GL-TOPNHEAP-1: the bounded top-N car's curve is valid
                // only inside its routed-admission k band (the classifier
                // threads the CONST LIMIT bound through the ngroups slot
                // when the plan-time car mirror admits, 0.0 otherwise —
                // killed knob / out-of-mirror / non-const LIMIT all land
                // 0.0 = out of band). Out of band the curve KEEPS Gather:
                // the GL-COST-TOPN-1 guard-off posture, byte-exactly.
                let cost_suppress = v.suppress
                    && (class != CoverClass::HeapPlainCountStar
                        || pages >= rtm::HEAP_COUNT_ADMISSION_MIN_PAGES)
                    && (class != CoverClass::CbTopnBoundedIntKeys
                        || rtm::topn_car_k_band(ngroups));
                if rtm::cost_route_decides(curve) && size_floors_enabled() {
                    suppress = cost_suppress;
                    decided_by = "cost";
                }
                // Step-2 shadow census + EXPLAIN sample (cost_shadow module
                // doc): observation only — `suppress` is already decided
                // above and is never read back out of this block. Floors-off
                // measurement runs do not count (forced-true whitelist
                // verdicts would poison the census).
                if size_floors_enabled() {
                    let (n_ws_mg, n_wg_ms) = cost_shadow::note(class, floor_ok, cost_suppress);
                    if trace_armed() && floor_ok != cost_suppress {
                        eprintln!(
                            "m5-cost-census: class={class:?} wl={} model={} \
                             n_wl_suppress_model_gather={n_ws_mg} \
                             n_wl_gather_model_suppress={n_wg_ms}",
                            if floor_ok { "suppress" } else { "gather" },
                            if cost_suppress { "suppress" } else { "gather" },
                        );
                    }
                    cost_shadow::record_sample(cost_shadow::ExplainSample {
                        class: cost_shadow::CLASS_NAMES[cost_shadow::class_idx(class)],
                        ratio: v.ratio,
                        model_suppress: cost_suppress,
                        whitelist_suppress: floor_ok,
                        decided_by,
                        rows,
                        dop,
                    });
                }
                if trace_armed() {
                    eprintln!(
                        "m5-cost-route: class={class:?} curve={curve:?} relid={relid} \
                         rows={rows:.0} pages={pages:.0} ngroups={ngroups:.0} dop={dop} \
                         arm_admits={arm_admits} r_pred={:.3} cost_verdict={} \
                         floor_verdict={floor_ok} decided_by={decided_by}",
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
//   CAR B  gap:agg-min-text (`grouped_str_minmax_arg`): the single-text-key
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
//   CAR C  gap:agg-orderby-nolimit (`full_sort` composition): the unbounded
//          grouped agg + `ORDER BY count(*)` with NO LIMIT — the topn arm's
//          `limitCount.is_some()` binding left the shape on the final
//          `Ok(false)`. The suppressed serial plan is `Sort <- HashAgg <-
//          SeqScan` (or `Sort <- Agg(SORTED) <- Sort <- SeqScan` for the
//          count(DISTINCT) class): the runtime sinks already engage with the
//          Agg below a Sort root (the reduced-exprkey decorated-root precedent), the
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

/// CAR B knob (`PGRUST_LANE_V2_AGG_STRMINMAX`, DEFAULT ON since the
/// GL-STRMM-2 flip — kill spellings exactly `0`/`off`, the t35/t36
/// flipped-kill idiom). LETTER OF RECORD (GL-STRMM-2, 2026-07-21,
/// night/strmm-qualed-topn): the GL-T2A suppress-then-serial had two
/// halves, both closed — (1) the QUAL-FREE varlena-remap staging refusal
/// (fixed: `seq_scan_cb_varlane_shed`); (2) the QUALED shapes\' serial
/// winner is the SORTED agg strategy the runtime hash sink structurally
/// never engages — CONTAINED at admission (`!has_quals`, kept). Flip
/// evidence: witnessed A-B-A ladder wins 1.5-2x at low group counts /
/// int keys where the planner otherwise runs serial; the ~1e5-group loss
/// band is refused by `strminmax_max_groups`; scored-bank inertness pair
/// flat under the containment. SAME spelling
/// as the executor half (nodeagg sink.rs `sink_strminmax_enabled` — the
/// resolve-combines / emit-plan vocabulary widening): both read sites flip
/// together, the AGG_POLY knob-coherence law.
fn agg_strminmax_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        tier2_car_kill_spelling_on(
            std::env::var("PGRUST_LANE_V2_AGG_STRMINMAX").as_deref().ok(),
        )
    })
}

/// CAR C knob (`PGRUST_LANE_V2_AGG_SORT_NOLIMIT`): DEFAULT ON since t36
/// flips2 (`=0|off` kills). Planner-only (suppression-widening; the
/// executor composition already exists). FLIP EVIDENCE (GL-T2B
/// FLIP-RECOMMENDED, 2026-07-21, tier2 campaign @ 7d8aa9a2b, 10M bank
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
/// (byval word keys; the const-tlist shape's `1`). Null consts (NULL group-key
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
pub(crate) fn is_whitelisted_agg(expr: Node<'_>, rti: usize, whitelist: &[u32]) -> bool {
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
    // GL-LOWDIST-3: datetime args ride the int lanes under the widening
    // knob (is_distinct_arg_int_kind); int-family unchanged at knob-off.
    is_covered_key_var(arg_tle.expr, rti, is_distinct_arg_int_kind)
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
    // GL-LOWDIST-3: datetime args under the widening knob (int lanes);
    // GL-LOWDIST-4 B2: text collation admission via the catalog helper.
    is_distinct_arg_int_kind(v.vartype)
        || (is_text_family(v.vartype) && distinct_arg_collation_ok(v.varcollid))
}

/// GL-LOWDIST-4 B2: a text DISTINCT arg's collation is admissible when it
/// is the default collation (the historical parse-altitude answer) OR —
/// under the widening knob — any DETERMINISTIC collation per the catalog
/// (`get_collation_isdeterministic`, exactly the walk's own
/// `distinct_set_kind` gate: byte equality of detoasted content IS the
/// equality verdict for every deterministic collation, COLLATE "C"
/// included). Probe ⊂ walk holds by construction — the widened probe
/// admits precisely what the serial set-mode init already admits. Lookup
/// errors refuse (fail-closed). Knob `PGRUST_LANE_V2_DISTINCT_COLLATION`
/// (t35 law: DEFAULT OFF for the letter; ON iff exactly `1`/`on`) —
/// probe-only: the executor side has admitted deterministic non-default
/// collations since distinct-bytes landed.
fn distinct_arg_collation_ok(collid: u32) -> bool {
    if collid == DEFAULT_COLLATION_OID {
        return true;
    }
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    // DEFAULT ON since the GL-LOWDIST-4 flip (Michael's B2 GO; kill 0|off).
    let widened = *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_LANE_V2_DISTINCT_COLLATION").as_deref(),
            Ok("0") | Ok("off")
        )
    });
    widened
        && collid != 0
        && lsyscache::get_collation_isdeterministic(collid).unwrap_or(false)
}

/// SE-TEXTDISTINCT (band 86001) reduced-expr-key affine check: `expr`
/// is `base ± <int4 Const>` (or `<int4 Const> ± base`) where `base` is the
/// representative int4 Var at `base_attno` on the scanned rel. Mirrors
/// `decide_reduced`'s per-key admission (exprkey.rs: Add2/Sub2/… over the
/// ONE representative Var, non-null Const, same width) STRICTLY NARROWER —
/// int4-only (decide_reduced admits any uniform int width; the probe keeps
/// to int4, the measured shape). Mul/Div refuse (decide_reduced refuses them too).
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

/// SE-TEXTDISTINCT (band 86001) reduced-expr-key recognizer. Keys the
/// census `gap:agg-expr-keys` shape:
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
/// GL-ELECT22-1 fix 3 — affine-derived-key DEDUP for the group estimate
/// (`PGRUST_M5_REDKEY_AFFINE_DEDUP`).
/// Every non-representative key this recognizer admits is `base ± Const`
/// — a pure function of the ONE representative Var — so the composite key
/// set partitions the input EXACTLY as the base Var alone does: the true
/// group count IS ndv(base), by functional dependence (not a heuristic).
/// `estimate_num_groups` over the full expr list cannot see the
/// dependence and multiplies per-expr NDVs: at the full-scale census the
/// affine-rider shape estimated 9,692,856 groups (census -1176 @
/// 307329686) and crossed the §10 hold, while the same shape's mid-scale
/// estimate (1,018,628, job -75c3) sat under it and ENGAGED at 0.022 hot
/// (28x, tsv row gap:agg-expr-keys) — the refusal is an estimation
/// artifact of the riders, not a measured hold economics. Knob-ON
/// estimates on the base key alone (the pgset form, riders skipped); the
/// §10 hold then judges the HONEST cardinality.
///
/// LADDER FINDING (GL-ELECT22-1 100M pair, job -4b6f): the dedup ALONE is
/// insufficient at full scale — the single-key estimate still crosses the
/// 4e6 hold on the 100M bank (the shape stayed refused, 6.7s unforced,
/// fail-closed as designed), while the forced series proves the engaged
/// arm wins the exact shape at 0.197/0.193 hot (31x). So the SAME knob
/// also transplants the TOPN-HIGHGROUPS exemption to this path (the
/// charter letter's named expectation — "the topnhigh bypass would clear
/// it once keyed"): the bounded winner-selection composition (sort key in
/// the finalfn-free int8-transvalue set, Const bound within the shared
/// sink cap) clears the hold under its OWN fail-closed ceiling below.
/// Like the extractkey twin (fix 4b), the suppressed plan is
/// full-drain-into-bounded-sort economics — witnessed-band only, never
/// unbounded. Kill-off keeps today's full-list estimate AND the plain
/// hold byte-for-byte.
///
/// DEFAULT ON (GL-ELECT22-1 flip; `=0|off` kills): 100M take-2 @
/// 240b738c9 (job -5f1a vs OFF baseline -5ca6) — the exemption
/// suppresses at ngroups=9,692,856 (the single-key estimate EQUALS the
/// full-list one on this bank — the multi-key estimator already clamps
/// here, so the exemption, not the dedup, is the binding fix; the dedup
/// stays for banks where the riders genuinely multiply), label
/// reduced-exprkey-grouped-topn-highgroups, hot 0.277 vs the 0.197
/// forced recovery bound (take-1 refused posture: 6.7s), byte parity
/// across arms AND vs the legacy plan's output.
fn redkey_affine_dedup_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        tier2_car_kill_spelling_on(
            std::env::var("PGRUST_M5_REDKEY_AFFINE_DEDUP").as_deref().ok(),
        )
    })
}

/// Fix-3 exemption ceiling (env-overridable
/// `PGRUST_M5_REDKEY_TOPN_MAX_GROUPS`, the ladder's sweep vehicle):
/// PROVISIONAL 16M — covers both estimate bases of the census cell (the
/// full-list 9.69M and the deduped single-key band) with headroom, below
/// the untested radix-exchange territory the §10 hold protects.
/// GL-ELECT22-1's ladder owns the bound.
fn redkey_topn_max_groups() -> f64 {
    static CEIL: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *CEIL.get_or_init(|| {
        std::env::var("PGRUST_M5_REDKEY_TOPN_MAX_GROUPS")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| *v > 0.0)
            .unwrap_or(16_000_000.0)
    })
}

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
    // exprkey fold hosts them via lanefold — the census shape has avg(ResolutionWidth).
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
    // GL-ELECT22-1 fix 3 (fn doc on the knob): the winner-selection
    // composition (bounded, int8-raw sort key, bound within the sink cap)
    // is captured here for the hold exemption below.
    let mut topn_hold_exempt_shape = false;
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
        topn_hold_exempt_shape = is_whitelisted_agg(tle.expr, rti, TOPN_INT8_RAW_SORT_AGGS)
            && const_count(parse.limitCount)
                .is_some_and(|b| b > 0 && b <= SINK_TOPN_MAX_BOUND_MIRROR);
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
        // GL-ELECT22-1 fix 3 (fn doc on the knob): the affine riders are
        // pure functions of the representative Var — estimate on the base
        // key ALONE (pgset form). If the processed clause no longer
        // carries a bare base-Var entry (never observed; the processed
        // list preserves the parse keys here), fail closed to the
        // full-list estimate — today's behavior.
        let base_idx = redkey_affine_dedup_enabled()
            .then(|| {
                group_exprs.iter().position(|(_, e)| {
                    key_var(*e, rti).is_some_and(|v| v.varattno == base_attno)
                })
            })
            .flatten();
        match base_idx {
            Some(i) => crate::selfuncs::estimate_num_groups_pgset(
                run,
                &group_exprs,
                input_rows,
                Some(&[i as i32]),
            )?,
            None => crate::selfuncs::estimate_num_groups(run, &group_exprs, input_rows)?,
        }
    };
    if ngroups >= groupby_high_floor() {
        // GL-ELECT22-1 fix 3 (fn doc on the knob): the bounded
        // winner-selection composition clears the hold knob-ON, inside
        // the witnessed band only; everything else keeps the fail-closed
        // refusal byte-for-byte. Own trace label for the ladder.
        if redkey_affine_dedup_enabled()
            && topn_hold_exempt_shape
            && ngroups < redkey_topn_max_groups()
        {
            return Ok(Some(finish_textdistinct(
                run,
                "reduced-exprkey-grouped-topn-highgroups",
                textdistinct_guard(),
                relid,
                ngroups,
                rel_rows,
                rel_pages,
            )?));
        }
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
/// census composition (ORDER BY the key + OFFSET) is the
/// topn-offset row's territory.
const F_EXTRACT_TIMESTAMP: u32 = 6202;
const TIMESTAMPOID: u32 = 1114;

/// SE-EXTRACTKEY: the computed group key `extract(<non-null Const field>
/// FROM <bare TIMESTAMP Var on the scanned rel>)` — the ts-extract class's
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
/// suppress-then-refuse). The measured image: int8 + text4 + numeric8 = 20 →
/// shrink → 16 (fits exactly).
fn extract_key_image_fits(int_widths_sum: usize, n_text: usize) -> bool {
    let fixed = int_widths_sum + n_text * 4;
    fixed + 8 <= 16 || fixed + 4 <= 16
}

/// SE-EXTRACTKEY (ts-extract class) recognizer: a single-cbstore-rel grouped
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
    // GL-ELECT22-1 fix 4b: the winner-selection composition (bounded, sort
    // key in the int8-raw set, bound within the sink cap) is captured
    // here for the hold exemption below.
    let mut topn_hold_exempt_shape = false;
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
        topn_hold_exempt_shape = is_whitelisted_agg(tle.expr, rti, TOPN_INT8_RAW_SORT_AGGS)
            && const_count(parse.limitCount)
                .is_some_and(|b| b > 0 && b <= SINK_TOPN_MAX_BOUND_MIRROR);
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
        // GL-ELECT22-1 fix 4b (fn doc on the knob): the bounded
        // winner-selection composition clears the hold knob-ON, inside
        // the witnessed band only; everything else keeps the fail-closed
        // refusal byte-for-byte. Own trace label for the ladder.
        if extractkey_topn_highgroups_enabled()
            && topn_hold_exempt_shape
            && ngroups < extractkey_topn_max_groups()
        {
            return Ok(Some(finish_knob_path(
                run,
                "extractkey",
                "extract-exprkey-grouped-topn-highgroups",
                extract_exprkey_guard(),
                relid,
                ngroups,
                rel_rows,
                rel_pages,
            )?));
        }
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

// ---------------------------------------------------------------------------
// OPEN-ROWS car 3 (EXPRKEY-TOPN): conditional-text-select (CASE) and
// timestamp-truncation computed group keys + OFFSET-into-bound composition.
// ---------------------------------------------------------------------------

/// EXPRKEY-TOPN knob (`PGRUST_LANE_V2_EXPRKEY_TOPN`): DEFAULT ON
/// (open-rows flip train, GL-OPENROWS-EXPRKEY-TOPN — fleet letter
/// 2026-07-21). Suppression-only widening for two census families whose
/// SERIAL-lane expr-key feed arms exist and engage today, refused only by
/// the probe's bare-Var key discipline and the top-N composition's OFFSET
/// refusal. Letter evidence per class:
/// * conditional-text-select (case-dict): wins BOTH scales — 5.0-5.1x at
///   the mid-scale bank, 4.6x at the full-scale bank, beating the
///   forced-vector ceiling there (jobs -0c51/-2e14, take-2 -66f5/-70ed);
/// * ts-trunc: 9.5-12x at the mid-scale bank but a measured 2.3x
///   REGRESSION at the full-scale bank — flips ONLY behind the
///   provisional page fence below (`tstrunc_max_pages`), which take-2
///   verified on the real bank (fenced shape = exact baseline wall, no
///   suppression trace; the case class and the mid-scale wins untouched).
///   NAMED FOLLOW-UP (the letter owns it): a THIRD SCALE POINT between
///   the two measured banks to turn the single provisional bound into a
///   curve — until then the fence errs toward keeping the exchange plan.
/// `PGRUST_LANE_V2_EXPRKEY_TOPN=0|off` is the kill (flipped-kill idiom).
fn exprkey_topn_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        tier2_car_kill_spelling_on(std::env::var("PGRUST_LANE_V2_EXPRKEY_TOPN").as_deref().ok())
    })
}

/// Engine-kill coherence (suppress-then-refuse guard): the serial-lane
/// expr-key feed owns the suppressed plans — its kill must gate the probe
/// keyings too (a keyed shape whose arm is disarmed would land
/// suppress-then-serial). Same env spellings as the executor (`0`/`off`
/// kill; default ON inside the lane).
fn exprkey_engine_live() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| knob_spelling_on(std::env::var("PGRUST_LANE_V2_EXPRKEY").as_deref().ok()))
}

/// The CASE class additionally rides the packed multi-key walk and its
/// conditional-text-select recognizer — both executor kills mirrored.
fn casedict_engine_live() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        exprkey_engine_live()
            && knob_spelling_on(std::env::var("PGRUST_LANE_V2_MULTIKEY").as_deref().ok())
            && knob_spelling_on(std::env::var("PGRUST_LANE_V2_CASEDICT").as_deref().ok())
    })
}

/// pg_proc oid of the truncation over the tz-less timestamp. The tz-aware
/// variants are timezone-dependent — the feed refuses them, so the probe
/// never sees them match (different funcid).
const F_TIMESTAMP_TRUNC_FN: u32 = 2020;

/// TS-TRUNC scale fence (pages, env-overridable
/// `PGRUST_LANE_V2_EXPRKEY_TSTRUNC_MAX_PAGES`): the ts-trunc class's
/// suppressed plan is a SERIAL qualed fold whose cost is the scan, while
/// the competing legacy plan for the qualed ordered-grouped shape is a
/// genuinely parallel ordered partial-agg exchange — the arm wins where
/// the scan is small and loses to the exchange once the relation's page
/// count is large (fleet letter, two measured points: ~9x win at the
/// mid-scale bank ~216k pages, ~2.3x REGRESSION at the full-scale bank
/// ~2.16M pages). PROVISIONAL single bound between the two points; the
/// GL letter owns the curve. The conditional-text-select class is NOT
/// fenced (its legacy competitor is the raw-row exchange hashagg, which
/// it beats at both measured scales).
fn tstrunc_max_pages() -> f64 {
    static V: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("PGRUST_LANE_V2_EXPRKEY_TSTRUNC_MAX_PAGES")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| *v > 0.0)
            .unwrap_or(1_000_000.0)
    })
}

/// The ts-trunc computed key: truncation with a non-null text-Const unit
/// in the UNIFORM-MICROSECOND whitelist over a bare tz-less-timestamp Var
/// — the expr-key feed's own recognizer mirrored (canonical lowercase
/// names + plurals only; aliases/abbreviations refuse — strictly narrower
/// than the arm, which itself degrades per-row on an unrecognized unit).
fn is_ts_trunc_key(expr: Node<'_>, rti: usize, mcx: mcx::Mcx<'_>) -> bool {
    let Some(f) = expr.as_func_expr() else { return false };
    if f.funcid != F_TIMESTAMP_TRUNC_FN || f.funcretset || f.args.len() != 2 {
        return false;
    }
    let Some(c) = f.args.nth(0).as_const() else { return false };
    if c.constisnull || c.consttype != TEXTOID {
        return false;
    }
    if !is_covered_key_var(f.args.nth(1), rti, |t| t == TIMESTAMPOID) {
        return false;
    }
    // SAFETY: a compile-time non-null text Const datum is a live varlena
    // for the statement's lifetime (the executor recognizer's contract).
    let Ok(packed) = (unsafe { types_fmgr::datum_varlena_packed(c.constvalue, mcx) }) else {
        return false;
    };
    let data = packed.data();
    if data.is_empty() || data.len() > 16 {
        return false;
    }
    let low: Vec<u8> = data.iter().map(|b| b.to_ascii_lowercase()).collect();
    matches!(
        low.as_slice(),
        b"second" | b"seconds" | b"minute" | b"minutes" | b"hour" | b"hours" | b"day" | b"days"
    )
}

/// The expr-key feed's int-equality funcids (mirror of its predicate
/// recognizer's table — any int2/4/8 cross-width builtin equality).
const INT_EQ_FNS_MIRROR: [u32; 9] = [63, 65, 467, 158, 159, 852, 474, 1850, 1856];

/// One conditional-select predicate: `<int Var on the rel> = <non-null
/// int Const>` through a builtin int equality (either argument order).
fn case_pred_ok(op: &types_nodes::primnodes::OpExpr<'_>, rti: usize) -> bool {
    if !INT_EQ_FNS_MIRROR.contains(&op.opfuncid) || op.args.len() != 2 {
        return false;
    }
    let (a, b) = (op.args.nth(0), op.args.nth(1));
    let vc = match (a.as_var(), b.as_const()) {
        (Some(v), Some(c)) => Some((v, c)),
        _ => match (b.as_var(), a.as_const()) {
            (Some(v), Some(c)) => Some((v, c)),
            _ => None,
        },
    };
    let Some((v, c)) = vc else { return false };
    v.varno as usize == rti
        && v.varlevelsup == 0
        && v.varattno > 0
        && is_int_family(v.vartype)
        && !c.constisnull
        && is_int_family(c.consttype)
}

/// The conditional-text-select computed key (the expr-key feed's CASE
/// class, mirrored): no CASE arg, TEXT result, exactly one WHEN whose
/// condition is one int-eq predicate or an AND of them, THEN a bare TEXT
/// Var on the rel (the probe adds the deterministic-default-collation
/// gate — strictly narrower), ELSE a non-null TEXT Const (a NULL default
/// derives NULL keys the packed image cannot carry — the feed refuses).
fn is_case_dict_key(expr: Node<'_>, rti: usize) -> bool {
    let Some(ce) = expr.as_case_expr() else { return false };
    if ce.arg.is_some() || ce.casetype != TEXTOID || ce.args.len() != 1 {
        return false;
    }
    let Some(when) = ce.args.nth(0).as_variant::<types_nodes::primnodes::CaseWhen>() else {
        return false;
    };
    let Some(cond) = when.expr else { return false };
    if let Some(op) = cond.as_op_expr() {
        if !case_pred_ok(op, rti) {
            return false;
        }
    } else if let Some(be) = cond.as_bool_expr() {
        if !matches!(be.boolop, types_nodes::primnodes::BoolExprType::AND_EXPR)
            || be.args.len() == 0
        {
            return false;
        }
        for a in be.args.iter() {
            let Some(op) = a.as_op_expr() else { return false };
            if !case_pred_ok(op, rti) {
                return false;
            }
        }
    } else {
        return false;
    }
    let Some(tres) = when.result else { return false };
    let Some(tv) = key_var(tres, rti) else { return false };
    if tv.vartype != TEXTOID || tv.varcollid != DEFAULT_COLLATION_OID {
        return false;
    }
    let Some(dres) = ce.defresult else { return false };
    let Some(dc) = dres.as_const() else { return false };
    !dc.constisnull && dc.consttype == TEXTOID
}

/// OPEN-ROWS car 3 recognizer: a single-cbstore-rel grouped agg whose
/// keys are bare int-family Vars, at most ONE bare default-collation text
/// Var (the Multi walk caps TextRaw components at one), and EXACTLY ONE
/// computed key — the conditional-text-select CASE (packed as an Intern
/// component beside the Var keys) or, ALONE, the uniform-unit timestamp
/// truncation (the feed's single-computed-key arm). Fold-admissible aggs
/// only (int-family args — the fold never reads the text/THEN lanes).
///
/// Compositions: plain grouped emit; a single fold-agg sort key with
/// Const LIMIT and optional Const OFFSET where limit+offset stays within
/// the sink's winner-selection bound cap (the bounded sort above the
/// suppressed plan carries limit+offset as its bound and the sink
/// composes it — the OFFSET refusal at the base top-N block is pure
/// admission debt for this family); ts-trunc only — ORDER BY the computed
/// key itself with the same Const bound (the suppressed serial plan keeps
/// its real Sort above the Agg and the fold's drain feeds it).
///
/// groupby_high hold applies UNCHANGED (no interplay with the
/// bounded-topn exemption — fail closed here). Returns `Some(verdict)` on
/// a family match, `None` to fall through to the bare-Var discipline.
fn classify_exprkey_topn<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &Query<'mcx>,
    rti: usize,
    relid: u32,
    rel_id: types_pathnodes::RelId,
    rel_rows: f64,
    rel_pages: f64,
) -> PgResult<Option<bool>> {
    #[derive(PartialEq, Clone, Copy)]
    enum Computed {
        Case,
        TsTrunc,
    }
    let mut computed: Option<Computed> = None;
    let mut computed_ref = 0u32;
    let mut key_refs: Vec<u32> = Vec::new();
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
        } else if computed.is_none() && is_case_dict_key(tle.expr, rti) {
            computed = Some(Computed::Case);
            computed_ref = gc.tleSortGroupRef;
        } else if computed.is_none() && is_ts_trunc_key(tle.expr, rti, run.mcx) {
            computed = Some(Computed::TsTrunc);
            computed_ref = gc.tleSortGroupRef;
        } else {
            return Ok(None); // second computed key / unadmitted expr key
        }
        key_refs.push(gc.tleSortGroupRef);
    }
    let Some(kind) = computed else { return Ok(None) };
    match kind {
        // The ts-trunc class is the feed's single-computed-key arm.
        Computed::TsTrunc if parse.groupClause.len() != 1 => return Ok(None),
        // Packed image negotiation: int widths + 4 per text + 4 for the
        // Intern'd computed component, within the 16-byte image.
        Computed::Case if int_widths + 4 * n_text + 4 > 16 => return Ok(None),
        _ => {}
    }
    match kind {
        Computed::Case if !casedict_engine_live() => return Ok(None),
        Computed::TsTrunc if !exprkey_engine_live() => return Ok(None),
        _ => {}
    }
    // TS-TRUNC scale fence (doc at `tstrunc_max_pages`): past the page
    // bound the serial fold loses to the ordered partial-agg exchange —
    // matched-shape-but-fenced keeps Gather. The serial-side term prices
    // the same two-way (fold-vs-exchange) in shadow next to the fence;
    // the groupby-high hold below is outside the term's witnessed cells
    // (it abstains there by not being consulted).
    let tstrunc_shadow = if kind == Computed::TsTrunc {
        costsize::serial_model::tstrunc_two_way(rel_rows)
    } else {
        None
    };
    if kind == Computed::TsTrunc && rel_pages >= tstrunc_max_pages() {
        serial_shadow_tail(
            serial_shadow::TSTRUNC_FOLD,
            "tstrunc-grouped-agg",
            tstrunc_shadow,
            false,
        );
        return Ok(Some(false));
    }
    // Emit discipline: keys by sortgroupref, everything else a
    // fold-admissible aggregate.
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(None) };
        if tle.ressortgroupref != 0 && key_refs.contains(&tle.ressortgroupref) {
            continue;
        }
        if !is_whitelisted_agg(tle.expr, rti, PLAIN_FOLD_AGGS) {
            return Ok(None);
        }
    }
    // Sort/limit composition (block doc above).
    if !parse.sortClause.is_nil() || parse.limitCount.is_some() || parse.limitOffset.is_some()
    {
        if parse.sortClause.len() != 1 || parse.limitCount.is_none() {
            return Ok(None);
        }
        let Some(sc) = parse.sortClause.nth(0).as_sort_group_clause() else {
            return Ok(None);
        };
        let Some(tle) = tle_by_sortgroupref(parse, sc.tleSortGroupRef) else {
            return Ok(None);
        };
        let key_sorted = kind == Computed::TsTrunc && sc.tleSortGroupRef == computed_ref;
        if !key_sorted && !is_whitelisted_agg(tle.expr, rti, PLAIN_FOLD_AGGS) {
            return Ok(None);
        }
        let Some(limit) = const_count(parse.limitCount) else { return Ok(None) };
        let offset = if parse.limitOffset.is_some() {
            match const_count(parse.limitOffset) {
                Some(v) => v,
                None => return Ok(None),
            }
        } else {
            0
        };
        let Some(bound) = limit.checked_add(offset) else { return Ok(None) };
        if bound < 1 || bound > SINK_TOPN_MAX_BOUND_MIRROR {
            return Ok(None);
        }
    }
    // groupby_high hold (shared floor; matched-shape-but-floored keeps
    // Gather).
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
    let suppressed = finish_knob_path(
        run,
        "exprkeytopn",
        match kind {
            Computed::Case => "casedict-grouped-agg",
            Computed::TsTrunc => "tstrunc-grouped-agg",
        },
        extract_exprkey_guard(),
        relid,
        ngroups,
        rel_rows,
        rel_pages,
    )?;
    if kind == Computed::TsTrunc {
        serial_shadow_tail(
            serial_shadow::TSTRUNC_FOLD,
            "tstrunc-grouped-agg",
            tstrunc_shadow,
            suppressed,
        );
    }
    Ok(Some(suppressed))
}

// ---------------------------------------------------------------------------
// GL-DICTDRAIN-1 (DICTKEY car): the regexp-extracted dict-key grouped class —
// the single-computed-dictionary-key shape (`gap:agg-regexp-dict-key`).
// ---------------------------------------------------------------------------

/// pg_proc oid of 3-arg `regexp_replace(text, text, text)` —
/// textregexreplace_noopt (the fmgr row of record, canonical.rs 2284). The
/// flags variant (2285) and the extended family are NOT keyed in v1 — the
/// executor hosts them (dicteval's catalog compile admits any strict
/// IMMUTABLE internal builtin), but the probe vocabulary widens only with
/// its own measured cell (fail-closed, probe ⊂ walk).
const F_TEXTREGEXREPLACE_NOOPT: u32 = 2284;

/// The DICTKEY car's key half: `regexp_replace(<bare text/varchar Var on
/// the rel>, <non-null text Const>, <non-null text Const>)` under the
/// deterministic default collation — the executor mirror: the expr-key
/// decide requires a pgrcolumnar TEXT|VARCHAR input column and a text-typed
/// key; dicteval's compile admits the catalog row (internal-language,
/// IMMUTABLE, strict, concrete rettype, usable collation — the probe's
/// default-collation gate is strictly narrower). A varchar input rides the
/// binary-coercion relabel exactly as the walker sees it. Returns the
/// input Var on a match.
fn regexp_dict_key_var<'mcx>(expr: Node<'mcx>, rti: usize) -> Option<&'mcx Var<'mcx>> {
    let f = expr.as_func_expr()?;
    if f.funcid != F_TEXTREGEXREPLACE_NOOPT || f.funcretset || f.args.len() != 3 {
        return None;
    }
    if f.inputcollid != DEFAULT_COLLATION_OID {
        return None;
    }
    let arg0 = f.args.nth(0);
    let arg0 = match arg0.as_relabel_type() {
        Some(r) if r.resulttype == TEXTOID => r.arg,
        Some(_) => return None,
        None => arg0,
    };
    let v = key_var(arg0, rti)?;
    if !is_text_family(v.vartype) || v.varcollid != DEFAULT_COLLATION_OID {
        return None;
    }
    for i in 1..3 {
        let c = f.args.nth(i).as_const()?;
        if c.constisnull || c.consttype != TEXTOID {
            return None;
        }
    }
    Some(v)
}

/// PROVISIONAL floor for the DICTKEY car: the charter bar is sink-beats-
/// serial at dop >= 4 (GL-DICTDRAIN-1); below it there is NO admitted
/// low-dop win region (`low_dop_max_rows = 0` — fail-closed, unlike
/// NO_GUARD's infinity which would make `min_dop` toothless). The
/// witnessed ladder owns the re-derivation.
fn dictkey_guard() -> FloorGuard {
    FloorGuard { min_dop: 4, low_dop_max_rows: 0.0, ..NO_GUARD }
}

/// GL-DICTDRAIN-1 recognizer: a single-cbstore-rel grouped agg whose ONE
/// group key is the regexp-extracted computed text key
/// (`regexp_dict_key_var`) over a PLAN-TIME DICT-ANSWERABLE column (the v7
/// stitch discipline — `topn_nonint_text_key_stitched`; a no-stitch column
/// keeps Gather: the executor's dict memo would run per-row-ish raw
/// windows and the drain's economics are unproven there). Passengers: the
/// grouped-sink vocabulary, the LENARG widening (its knob), and
/// min/max(text) under the strminmax car (its knob + the GL-STRMM-2
/// group-estimate ceiling MIRRORED — the executor refuses VarlenaMinMax
/// engagements past it, fn doc `strminmax_max_groups`). HAVING composes
/// (only the prefilter-admitted `count(*) <cmp> Const` term ever reaches
/// here — the HAVING car's own carve). Decorations: plain grouped emit, or
/// ONE agg sort key (base vocabulary, or a LENARG key — the winner-
/// selection declines to the full drain, the GL-STRAGG-2 priced degrade)
/// with a Const LIMIT within the sink bound cap, no OFFSET.
///
/// groupby_high hold applies UNCHANGED (charter: the 100M census shape's
/// ~6.6M estimate crosses it — matched-shape-but-floored keeps Gather; the
/// hold's env override is the ladder's measurement lever, and any floor
/// change is the flip letter's own witnessed justification). Returns
/// `Some(verdict)` on a family match, `None` to fall through.
#[allow(clippy::too_many_arguments)]
fn classify_dictkey_exprkey<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &Query<'mcx>,
    rti: usize,
    relid: u32,
    rel_id: types_pathnodes::RelId,
    rel_rows: f64,
    rel_pages: f64,
) -> PgResult<Option<bool>> {
    if !dictkey_engine_live() {
        return Ok(None);
    }
    // ONE group key: the computed dict key (the executor's single-key Dict
    // arm — `agg_hash_staged_probe_col` must name exactly this column).
    if parse.groupClause.len() != 1 {
        return Ok(None);
    }
    let Some(gc) = parse.groupClause.nth(0).as_sort_group_clause() else { return Ok(None) };
    let Some(key_tle) = tle_by_sortgroupref(parse, gc.tleSortGroupRef) else {
        return Ok(None);
    };
    let Some(kvar) = regexp_dict_key_var(key_tle.expr, rti) else { return Ok(None) };
    let key_ref = gc.tleSortGroupRef;
    // Plan-time dict answerability (the v7 stitch discipline).
    if !topn_nonint_text_key_stitched(run, rel_id, kvar.varattno as i32) {
        return Ok(None);
    }
    // Emit discipline: the key by sortgroupref; every other entry a
    // passenger from the class vocabulary.
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(None) };
        if tle.ressortgroupref != 0 && tle.ressortgroupref == key_ref {
            continue;
        }
        if is_whitelisted_agg(tle.expr, rti, grouped_sink_aggs()) {
            continue;
        }
        if agg_lenarg_enabled() && is_whitelisted_agg_lenarg(tle.expr, rti, grouped_sink_aggs())
        {
            continue;
        }
        // min/max(text) passengers: RE-ADMITTED (GL-DICTDRAIN-3 — the t45
        // revert's follow-up row discharged). The t45-reverted lift was
        // wrong not in the ADMISSION but in the state's memory home: the
        // drain's Local-owned table migrates across pool threads while the
        // per-thread FREEING byref-state child made the replace-free
        // allocator-INEXACT — a live pergroup could be left holding a
        // freed pointer, tripping the sink shape guard at combine/emit.
        // The transvalue store now travels WITH the table
        // (`lanefold::StrStateArena`, armed by `agg_sink_arm_str_state`),
        // restoring allocator-exactness across migration; the sink shape
        // check stays armed as the permanent detector. Admission mirrors
        // the sibling grouped classifiers (strminmax car knob + default
        // collation + bare text Var). The GL-STRMM-2 group-estimate
        // ceiling no longer mirrors HERE: hold disposition D2 exempts the
        // DictCoded kind on the EXECUTOR half (same commit, lockstep), so
        // a suppressed shape past the old ceiling engages instead of
        // landing suppress-then-serial.
        if agg_strminmax_enabled() && grouped_str_minmax_arg(tle.expr, rti).is_some() {
            continue;
        }
        return Ok(None);
    }
    // Sort/limit composition: none (plain grouped emit), or ONE agg sort
    // key + Const LIMIT, no OFFSET (the sibling classifiers' block).
    if !parse.sortClause.is_nil() || parse.limitCount.is_some() || parse.limitOffset.is_some()
    {
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
        let lenarg_sortkey =
            agg_lenarg_enabled() && is_whitelisted_agg_lenarg(tle.expr, rti, grouped_sink_aggs());
        if !is_whitelisted_agg(tle.expr, rti, GROUPED_SINK_AGGS) && !lenarg_sortkey {
            return Ok(None);
        }
        match const_count(parse.limitCount) {
            Some(b) if b >= 1 && b <= SINK_TOPN_MAX_BOUND_MIRROR => {}
            _ => return Ok(None),
        }
    }
    // groupby_high hold (shared floor, UNCHANGED — fn doc): matched-shape-
    // but-floored keeps Gather.
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
    // GL-HEAVYTIER-1 hold disposition D1 (coordinator-approved): this
    // classifier carries its own witnessed ceiling instead of the shared
    // groupby_high floor — the engaged sink is the measured winner for the
    // class far above the shared floor (fn doc on `dictkey_max_groups`).
    // The shared floor stays byte-for-byte for every other classifier;
    // the class kill (`PGRUST_LANE_V2_AGG_DICTKEY=0|off`) restores the
    // whole classifier off, holds included.
    if ngroups >= dictkey_max_groups() {
        return Ok(Some(false));
    }
    // GL-HEAVYTIER-1 hold disposition D2 (coordinator-approved): the
    // GL-STRMM-2 ceiling mirror is DROPPED here in LOCKSTEP with the
    // executor's leader-gate exemption for the DictCoded kind
    // (runtime_agg's strminmax gate skips DictCoded engagements — same
    // commit; knob-coherence law). The byref MIN/MAX(text) combine/emit
    // for THIS kind rides the cross-thread-allocator-exact substrate the
    // dict-class letter banked with parity at production scale; every
    // other kind keeps the ceiling on both halves byte-for-byte.
    Ok(Some(finish_knob_path(
        run,
        "dictkey",
        "dictkey-grouped-agg",
        dictkey_guard(),
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
/// (which refused the fact-rel shape outright: lineitem carries its PRIMARY KEY, a
/// live census finding). With ONE baserel and no join, an index can steer
/// the suppressed serial plan away from Agg-over-SeqScan only when:
///   (a) a QUAL references the index's KEY columns (an index path becomes
///       electable — the walk would refuse the IndexScan outer, the
///       suppress-then-refuse direction), or
///   (b) the index COVERS every column the query references (an
///       index-only scan can cost below the seqscan even qual-free).
/// Expression or partial indexes refuse outright (their matching is the
/// planner's own — not re-derived here), as do whole-row references.
/// The shape's lineitem_pkey (l_orderkey, l_linenumber) triggers neither arm.
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
/// entry is a whitelisted bare-int-Var aggregate (PLAIN_FOLD_AGGS), a
/// structurally plain sum/avg(NUMERIC) (no ORDER BY/DISTINCT/FILTER/
/// variadic/ordered-set/levelsup) whose single argument expression the
/// planner's own `is_parallel_safe` admits (it runs on helpers through the
/// per-row transition program; the arg SHAPE is otherwise free — the poly
/// manifest classifies by state, not argument), or — AGG_INTCASE,
/// knob-gated — an int-family plain aggregate over a parallel-safe arg
/// expression / an emit expression over admitted aggregates (ratio emits).
///
/// The keying gate is `n_poly > 0`: at least one entry the executor
/// manifest is GUARANTEED to classify as a poly entry — numeric anchors,
/// plus int-family aggs whose arg carries a CONDITIONAL node
/// (CASE/COALESCE/NULLIF/GREATEST/LEAST), which the fold plan's
/// classify_arg can never lane-classify. Int-family expr args WITHOUT a
/// conditional (affine forms, textlen, var-op-var) are admitted as
/// riders but never counted: a lane-classifiable rider that turned out to
/// be the only "poly" entry would leave the manifest empty — the
/// suppress-then-refuse channel this gate closes. All-rider/all-bare
/// shapes keep their existing rows (narrow probe).
fn heap_poly_tlist_admits(
    run: &PlannerRun<'_>,
    parse: &Query<'_>,
    rti: usize,
) -> PgResult<bool> {
    let intcase = agg_intcase_probe_enabled();
    let mut n_poly = 0usize;
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
        if is_whitelisted_agg(tle.expr, rti, PLAIN_FOLD_AGGS) {
            continue;
        }
        if let Some(agg) = tle.expr.as_aggref() {
            if !heap_poly_aggref_admits(run, agg, intcase, &mut n_poly)? {
                return Ok(false);
            }
            continue;
        }
        // AGG_INTCASE ratio emits: a non-Aggref entry admits iff it is a
        // const/operator/function composition over admitted aggregates
        // (>=1). The composition runs ONCE, leader-side, at the serial
        // plan's own finalize/projection — identical to the suppressed
        // serial execution by construction; only the per-row transition
        // work moves to helpers.
        if !intcase || !intcase_emit_expr_admits(run, tle.expr, rti, &mut n_poly)? {
            return Ok(false);
        }
    }
    Ok(n_poly > 0)
}

/// One non-bare-whitelist Aggref of the plain-heap-poly tlist: the plain
/// sum/avg(NUMERIC) arm (always counted — the manifest's NumericAvg class),
/// or — `intcase` — an int-family INTCASE_POLY_AGGS entry over any
/// parallel-safe single arg (counted toward the keying gate only when the
/// arg carries a conditional node; see `heap_poly_tlist_admits`).
fn heap_poly_aggref_admits(
    run: &PlannerRun<'_>,
    agg: &Aggref<'_>,
    intcase: bool,
    n_poly: &mut usize,
) -> PgResult<bool> {
    if agg.agglevelsup != 0
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
    let numeric = matches!(agg.aggfnoid, F_AVG_NUMERIC | F_SUM_NUMERIC);
    if !numeric && !(intcase && INTCASE_POLY_AGGS.contains(&agg.aggfnoid)) {
        return Ok(false);
    }
    let Some(arg_tle) = agg.args.nth(0).as_target_entry() else { return Ok(false) };
    if !crate::is_parallel_safe_opt(run, Some(arg_tle.expr))? {
        return Ok(false);
    }
    if numeric || contains_conditional(arg_tle.expr)? {
        *n_poly += 1;
    }
    Ok(true)
}

/// AGG_INTCASE: does the expression carry a conditional node anywhere?
/// (CASE / COALESCE / NULLIF / GREATEST / LEAST — the conditional-
/// aggregation idiom.) These forms NEVER lane-classify (lanefold
/// classify_arg admits bare Vars, textlen, and affine Var-op-Const only),
/// so a conditional-bearing arg is guaranteed to reach the manifest's
/// per-row classification — the keying gate's engagement proof.
fn contains_conditional(expr: Node<'_>) -> PgResult<bool> {
    use nodes_core::NodeWalker;
    use types_nodes::NodeTag;
    struct W {
        found: bool,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            match node.node_tag() {
                NodeTag::T_CaseExpr
                | NodeTag::T_CoalesceExpr
                | NodeTag::T_NullIfExpr
                | NodeTag::T_MinMaxExpr => {
                    self.found = true;
                    Ok(true)
                }
                _ => nodes_core::expression_tree_walker(node, self),
            }
        }
    }
    let mut w = W { found: false };
    let _ = w.visit(expr)?;
    Ok(w.found)
}

/// AGG_INTCASE ratio emits: admit `expr` iff it is a composition of
/// {OpExpr, FuncExpr (non-set-returning), RelabelType, CoerceViaIO, Const}
/// over >=1 admitted Aggref leaves (bare-whitelist or the poly arms above).
/// Anything else — Params, SRFs, CASE above the aggregates, sublinks —
/// fails closed. The composition itself is leader-side-only (see the
/// caller); the AGGREGATES' args carry the helper-side safety obligations.
fn intcase_emit_expr_admits(
    run: &PlannerRun<'_>,
    expr: Node<'_>,
    rti: usize,
    n_poly: &mut usize,
) -> PgResult<bool> {
    let mut n_aggs = 0usize;
    if !intcase_emit_walk(run, expr, rti, n_poly, &mut n_aggs)? {
        return Ok(false);
    }
    Ok(n_aggs > 0)
}

fn intcase_emit_walk(
    run: &PlannerRun<'_>,
    expr: Node<'_>,
    rti: usize,
    n_poly: &mut usize,
    n_aggs: &mut usize,
) -> PgResult<bool> {
    if let Some(agg) = expr.as_aggref() {
        *n_aggs += 1;
        if is_whitelisted_agg(expr, rti, PLAIN_FOLD_AGGS) {
            return Ok(true);
        }
        return heap_poly_aggref_admits(run, agg, true, n_poly);
    }
    if expr.as_const().is_some() {
        return Ok(true);
    }
    if let Some(op) = expr.as_op_expr() {
        if op.opretset {
            return Ok(false);
        }
        for a in op.args.iter() {
            if !intcase_emit_walk(run, a, rti, n_poly, n_aggs)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    if let Some(f) = expr.as_func_expr() {
        if f.funcretset {
            return Ok(false);
        }
        for a in f.args.iter() {
            if !intcase_emit_walk(run, a, rti, n_poly, n_aggs)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    if let Some(r) = expr.as_relabel_type() {
        return intcase_emit_walk(run, r.arg, rti, n_poly, n_aggs);
    }
    if let Some(c) = expr.as_coerce_via_io() {
        return intcase_emit_walk(run, c.arg, rti, n_poly, n_aggs);
    }
    Ok(false)
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

/// SE-DECOROOT (CAR 1): every ORDER BY key resolves to a covered tlist
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

    #[test]
    fn intcase_knob_is_default_on_with_kill() {
        assert!(intcase_spelling_on(None), "unset must be ON (GL-INTCASE-1 default flip)");
        assert!(!intcase_spelling_on(Some("0")), "kill spelling");
        assert!(!intcase_spelling_on(Some("off")), "kill spelling");
        assert!(intcase_spelling_on(Some("1")));
        assert!(intcase_spelling_on(Some("on")));
        assert!(
            intcase_spelling_on(Some("")) && intcase_spelling_on(Some("OFF")),
            "flipped-kill idiom: only exact 0/off kill (case-sensitive, like the sibling arm knobs)"
        );
        assert!(agg_intcase_probe_enabled(), "test process has no knob set => ON");
    }

    /// AGG_INTCASE vocabulary discipline: the per-row int whitelist is
    /// EXACTLY the plain-fold whitelist minus the zero-arg count(*) — a
    /// drift here would either key a shape the manifest refuses
    /// (suppress-then-refuse) or silently narrow the car.
    #[test]
    fn intcase_whitelist_is_plain_fold_minus_count_star() {
        let expect: Vec<u32> = PLAIN_FOLD_AGGS
            .iter()
            .copied()
            .filter(|&o| o != F_COUNT_STAR)
            .collect();
        assert_eq!(INTCASE_POLY_AGGS, expect.as_slice());
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
        // ADMITTED: the two-key grouped class (int+text) and text+text.
        assert!(mk_text_family_shape_ok(2, 1), "two keys, int+text");
        assert!(mk_text_family_shape_ok(2, 2), "two keys, text+text");
        // REFUSED: all-int two-key (existing bootstrap rows own it).
        assert!(!mk_text_family_shape_ok(2, 0), "int+int is not this family");
        // REFUSED: single-key shapes (existing rows / sibling cars).
        assert!(!mk_text_family_shape_ok(1, 1), "single text key is the C2/bootstrap row");
        assert!(!mk_text_family_shape_ok(1, 0));
        // REFUSED: 3+ keys — with or without a second text (fail-closed;
        // the ts-extract class additionally carries an expr key, refused upstream).
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
        // The ts-extract image: int8 + text4 + numeric8 = 20 → shrink → 16. Fits.
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
        // Test binary runs with PGRUST_M5_HJRIDER_CURVE unset and the
        // seat-world knobs at their flipped defaults (both LIVE): the
        // multibuild rider is curveless (GL-COST-2 unwire); the GROUPED
        // rider decides by its OWN seated curve (GL-MBSEAT-1 guard lift);
        // MetaFooter has no curve by design.
        for row in BOOTSTRAP_MATRIX {
            let curveless = cover_class_curve(row.class).is_none();
            // PartwisePlainFold: rectangle-retained — per-AM PROVISIONAL
            // floors in m5_partwise.rs (GL-PARTWISE-1); curve cells ride the
            // named floor-calibration follow-up.
            let expect_curveless = matches!(
                row.class,
                CoverClass::CbMetaFooterAgg
                    | CoverClass::CbHashJoinMultiBuild
                    | CoverClass::PartwisePlainFold
            );
            assert_eq!(
                curveless, expect_curveless,
                "cost-route curve map drift for {:?}",
                row.class
            );
        }
        // The grouped rider's own curve is the SEATED class (never the
        // refuted PlainAgg reuse).
        assert_eq!(
            cover_class_curve(CoverClass::CbHashJoinGroupedAgg),
            Some(costsize::runtime_model::RuntimeClass::CbHashJoinGroupedAgg)
        );
    }

    /// GL-COST-2 unwire posture of record, amended by the GL-MBSEAT-1
    /// guard lift: BOTH riders keep the max_rows=0 rectangle (the honest
    /// keep — floors alone never suppress them; every kill posture lands
    /// here), the MULTIBUILD rider stays curveless, and the GROUPED rider
    /// decides by its OWN seated curve at the flipped defaults (the lift
    /// lives entirely in the decide-listed curve; PGRUST_M5_COST_ROUTE=
    /// shadow is the standing routing kill, MBSEAT/MBSHARED=0 un-curve
    /// the class — OnceLock env, so only the default posture is
    /// assertable in-process; the kill postures are e2e restart legs).
    #[test]
    fn hjrider_unwire_posture() {
        assert!(!hjrider_curve_enabled(), "default must be UNWIRED");
        for class in [CoverClass::CbHashJoinMultiBuild, CoverClass::CbHashJoinGroupedAgg] {
            let g = class_guard(class);
            assert_eq!(g.max_rows, 0.0, "{class:?} rectangle must stay guarded off");
        }
        assert!(cover_class_curve(CoverClass::CbHashJoinMultiBuild).is_none());
        assert_eq!(
            cover_class_curve(CoverClass::CbHashJoinGroupedAgg),
            Some(costsize::runtime_model::RuntimeClass::CbHashJoinGroupedAgg),
            "the grouped rider lifts through its own seated curve"
        );
        // The non-rider hashjoin class keeps its curve and rectangle
        // (post-S1 band collapse: dop-conditioned, ceiling at the fitted
        // dop16 crossover — see the class_guard provenance comment).
        let g = class_guard(CoverClass::CbHashJoinPlainAgg);
        assert_eq!(g.min_rows, HJ_ARM_MIN_ROWS);
        assert_eq!(g.max_rows, 4_000_000.0);
        assert_eq!(g.min_dop, 12);
        assert_eq!(g.low_dop_max_rows, 2_000_000.0);
        assert!(cover_class_curve(CoverClass::CbHashJoinPlainAgg).is_some());
    }

    /// F1 (soak adjudication round 1): the grouped top-n class carries a
    /// post-qual min_rows floor — tiny-selective shapes elect the sorted
    /// serial grouping plan the arm refuses (suppress-then-refuse), so
    /// suppression below the witnessed engaged-win region must keep the
    /// frame's parallel plan. The floor sits below the smallest witnessed
    /// engaged win (598k post-qual) and above the witnessed 3.3-5.3x
    /// serial-landing losses (~250-row post-qual cells).
    #[test]
    fn grouped_topn_tiny_selective_floor() {
        let g = class_guard(CoverClass::CbGroupedAggTopN);
        assert_eq!(g.min_rows, 500_000.0);
        assert_eq!(g.max_rows, f64::INFINITY);
        assert_eq!(g.min_dop, 0, "the class's win band is not dop-shaped");
    }

    /// Step-2 census plumbing: every direction lands in its own cell, and
    /// the note() return carries the cumulative disagreement pair. Uses
    /// CbMetaFooterAgg — the one curveless class, which production code can
    /// never note() (finish only notes inside `if let Some(curve)`), so the
    /// deltas are interference-free even if other tests plan queries.
    #[test]
    fn cost_shadow_census_counts_directions() {
        use cost_shadow::{note, snapshot, class_idx};
        let c = CoverClass::CbMetaFooterAgg;
        let i = class_idx(c);
        let before = snapshot()[i];
        assert_eq!(before.class, "CbMetaFooterAgg");
        note(c, true, true);
        note(c, false, false);
        note(c, false, false);
        let (ws_mg, wg_ms) = note(c, true, false);
        assert_eq!((ws_mg, wg_ms), (
            before.wl_suppress_model_gather + 1,
            before.wl_gather_model_suppress,
        ));
        let (ws_mg, wg_ms) = note(c, false, true);
        assert_eq!((ws_mg, wg_ms), (
            before.wl_suppress_model_gather + 1,
            before.wl_gather_model_suppress + 1,
        ));
        let after = snapshot()[i];
        assert_eq!(after.agree_suppress, before.agree_suppress + 1);
        assert_eq!(after.agree_gather, before.agree_gather + 2);
        assert_eq!(after.wl_suppress_model_gather, before.wl_suppress_model_gather + 1);
        assert_eq!(after.wl_gather_model_suppress, before.wl_gather_model_suppress + 1);
    }

    /// Serial-side shadow census plumbing: every (enforced, pick) pair
    /// lands in its own cell; the agreement predicate is exactly
    /// "gather-gather or suppress-{serial,runtime}" (suppression's
    /// delivered engine is decided at exec by the router/band). Uses the
    /// scanfold-meta family row: its production note() path requires a
    /// qualed covered plain fold at 10M-scale, which no unit test plans —
    /// deltas are interference-free.
    #[test]
    fn serial_shadow_census_counts_cells() {
        use costsize::serial_model::EnginePick as P;
        use serial_shadow::{agrees, note, snapshot, SCANFOLD_META};
        let before = snapshot()[SCANFOLD_META];
        assert_eq!(before.family, "scanfold-meta");
        let n1 = note(SCANFOLD_META, false, P::Gather);
        note(SCANFOLD_META, true, P::Serial);
        note(SCANFOLD_META, true, P::Serial);
        note(SCANFOLD_META, true, P::Runtime);
        note(SCANFOLD_META, false, P::Serial);
        let after = snapshot()[SCANFOLD_META];
        assert_eq!(n1, before.cells[0][2] + 1);
        assert_eq!(after.cells[0][2], before.cells[0][2] + 1); // gather/gather
        assert_eq!(after.cells[1][0], before.cells[1][0] + 2); // suppress/serial
        assert_eq!(after.cells[1][1], before.cells[1][1] + 1); // suppress/runtime
        assert_eq!(after.cells[0][0], before.cells[0][0] + 1); // gather/serial
        assert_eq!(after.cells[0][1], before.cells[0][1]);
        assert_eq!(after.cells[1][2], before.cells[1][2]);
        // Agreement semantics.
        assert!(agrees(false, P::Gather));
        assert!(agrees(true, P::Serial));
        assert!(agrees(true, P::Runtime));
        assert!(!agrees(false, P::Serial));
        assert!(!agrees(false, P::Runtime));
        assert!(!agrees(true, P::Gather));
    }

    /// The serial shadow is OBSERVATION ONLY, pinned structurally: the
    /// tail helper takes the ALREADY-DECIDED verdict by value and returns
    /// unit — it cannot feed back. This test pins the carve-agreement
    /// surface the planner wires: at the carves' own witnessed anchors
    /// the model pick agrees with what each carve enforces (the letter's
    /// table lives in costsize::serial_model tests; this is the
    /// planner-side smoke that the wired inputs reproduce it).
    #[test]
    fn serial_shadow_agrees_with_the_carves_at_their_anchors() {
        use costsize::serial_model as sm;
        use serial_shadow::agrees;
        // Selective-qual datetime-lead carve, GL-RESIDUAL-2 posture: below
        // the survival bound the STAR-WIDE class enforces the fitted
        // two-way — serial at both witnessed scales, outside parity.
        for rows in [1e7, 1e8] {
            let v = sm::topn_selqual_starwide_two_way(rows).unwrap();
            assert_eq!(v.pick, sm::EnginePick::Serial, "priced carve at N={rows}: {v:?}");
            assert!(!v.parity, "the enforcing pick must be outside the parity band");
        }
        // Below the model's support the carve keeps Gather (abstain =
        // incumbent).
        assert!(sm::topn_selqual_starwide_two_way(9e6).is_none());
        // Every OTHER shape in the carve region still keeps Gather
        // unconditionally (enforced=false) — the observation shadow agrees
        // at the minting-era anchor.
        let losing = sm::TopnShape {
            class: sm::TopnKeyClass::NarrowTs,
            rows: 1e7,
            dop: 16,
            limit: 10.0,
            survival: 0.0101,
            zone_friendly: true,
        };
        assert!(losing.survival < TOPN_NONINT_MIN_QUAL_SURVIVAL);
        let v = sm::topn_nonint_three_way(&losing).unwrap();
        assert!(agrees(false, v.pick), "carve keeps Gather, model {v:?}");
        // Above the bound the carve admits (suppress) — model agrees.
        let winning = sm::TopnShape { survival: 0.75, ..losing };
        assert!(winning.survival >= TOPN_NONINT_MIN_QUAL_SURVIVAL);
        let v = sm::topn_nonint_three_way(&winning).unwrap();
        assert!(agrees(true, v.pick), "carve admits, model {v:?}");
        // Page fence: the fenced scale keeps Gather, the admitted scale
        // suppresses — model agrees on both sides of the shipped bound
        // (fixture geometry: ~46 rows/page on the banked fixture).
        let rows_per_page = 1e7 / 216_000.0;
        let fenced_rows = 2_160_000.0 * rows_per_page;
        let v = sm::tstrunc_two_way(fenced_rows).unwrap();
        assert!(2_160_000.0 >= tstrunc_max_pages());
        assert!(agrees(false, v.pick), "fence keeps Gather, model {v:?}");
        let admitted_rows = 216_000.0 * rows_per_page;
        let v = sm::tstrunc_two_way(admitted_rows).unwrap();
        assert!(216_000.0 < tstrunc_max_pages());
        assert!(agrees(true, v.pick), "fence admits, model {v:?}");
    }

    /// R1 arm-admission mirror pins: every instance mirrors a WITNESSED
    /// engage floor (never new economics) — the cbstore 64-granule
    /// geometry floor (== HJ_ARM_MIN_ROWS == S1's constant), F1's 500k
    /// grouped-topn post-qual floor, the heap-count block floor; the
    /// heap cmp-fold arm has no witnessed floor and always admits.
    #[test]
    fn arm_admission_mirror_matches_the_witnessed_floors() {
        use costsize::runtime_model as rtm;
        // cbstore classes: the nine-job grid witnessed absent <= 500k /
        // engaged >= 1M — the 64-granule mirror splits inside that band.
        for class in [
            CoverClass::CbPlainAggFold,
            CoverClass::CbGroupedAggIntKeys,
            CoverClass::CbGroupedAggTextKey,
            CoverClass::CbDistinctIntKeys,
            CoverClass::CbTopnBoundedIntKeys,
            CoverClass::CbHashJoinPlainAgg,
        ] {
            assert!(!arm_admission_mirror(class, 500_000.0, 1e5), "{class:?} @500k");
            assert!(arm_admission_mirror(class, 1_000_000.0, 1e5), "{class:?} @1M");
            assert!(!arm_admission_mirror(class, HJ_ARM_MIN_ROWS - 1.0, 1e5));
            assert!(arm_admission_mirror(class, HJ_ARM_MIN_ROWS, 1e5));
        }
        assert_eq!(HJ_ARM_MIN_ROWS, 64.0 * 8192.0, "the 64-granule geometry mirror");
        // F1's post-qual floor.
        assert!(!arm_admission_mirror(CoverClass::CbGroupedAggTopN, 499_999.0, 1e5));
        assert!(arm_admission_mirror(CoverClass::CbGroupedAggTopN, 500_000.0, 1e5));
        // Heap count: the block floor, rows-independent.
        assert!(!arm_admission_mirror(CoverClass::HeapPlainCountStar, 1e7, 8191.0));
        assert!(arm_admission_mirror(
            CoverClass::HeapPlainCountStar,
            1e7,
            rtm::HEAP_COUNT_ADMISSION_MIN_PAGES
        ));
        // Heap cmp-fold engaged at the 100k cells: always admits.
        assert!(arm_admission_mirror(CoverClass::HeapCmpFoldPrefix, 1_000.0, 10.0));
    }

    /// The census index and the printable class-name table cannot drift
    /// from the CoverClass vocabulary (Debug names ARE the census names).
    #[test]
    fn cost_shadow_class_names_match_cover_classes() {
        let mut seen = std::collections::BTreeSet::new();
        for row in BOOTSTRAP_MATRIX {
            let i = cost_shadow::class_idx(row.class);
            assert_eq!(
                cost_shadow::CLASS_NAMES[i],
                format!("{:?}", row.class),
                "census name drift at index {i}"
            );
            assert!(seen.insert(i), "duplicate census index {i}");
        }
        assert_eq!(seen.len(), BOOTSTRAP_MATRIX.len());
    }

    /// The EXPLAIN knob is default OFF (exact-spelling arm, the scanpass
    /// idiom), and while unarmed the sample slot is never readable — the
    /// EXPLAIN surface stays byte-identical at default even if a sample
    /// were recorded.
    #[test]
    fn cost_shadow_explain_knob_default_off() {
        assert!(!cost_shadow::explain_spelling_on(None));
        for v in ["0", "off", "", "true", "ON", "yes"] {
            assert!(!cost_shadow::explain_spelling_on(Some(v)), "spelling {v:?}");
        }
        assert!(cost_shadow::explain_spelling_on(Some("1")));
        assert!(cost_shadow::explain_spelling_on(Some("on")));
        // The test binary runs with the env unset: armed getters resolve OFF,
        // so clear/record/take are all inert no-ops.
        assert!(!cost_shadow::explain_armed());
        cost_shadow::clear_last_sample();
        assert!(cost_shadow::take_last_sample().is_none());
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
            assert_eq!(cols.len(), 11, "malformed TSV row: {line}");
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
        // GL-COST-2 unwire rows: the riders' guarded-off rectangles are of
        // record in the TSV (witnessed grids in the note columns).
        assert_eq!(
            get("CbHashJoinMultiBuild", "rectangle_max_rows").parse::<f64>().unwrap(),
            class_guard(CoverClass::CbHashJoinMultiBuild).max_rows
        );
        assert_eq!(
            get("CbHashJoinGroupedAgg", "rectangle_max_rows").parse::<f64>().unwrap(),
            class_guard(CoverClass::CbHashJoinGroupedAgg).max_rows
        );
        // The remaining reuse row points at a real curve class.
        assert_eq!(get("AggPolyHeapPlain", "curve_reuse"), "HeapCmpFoldPrefix");
    }

    /// SE-CARS knobs (the GL-DECOROOT-1/GL-NUMJOIN-1 lane): the shared
    /// default-OFF arming spelling (`1`/`on` only — typos fail safe), and
    /// arm only on the exact spellings `1`/`on` (the SE-SCANPASS /
    /// K1-latemat default-OFF idiom — typos fail safe to today's behaviour,
    /// byte-identical plan time). Pins the default-OFF posture that makes
    /// the branch inert at default, and the live getters' resolution in a
    /// knob-free process.
    #[test]
    fn conversion_car_knob_spellings() {
        assert!(!knob_spelling_armed(None), "unset must be OFF (default)");
        assert!(!knob_spelling_armed(Some("0")));
        assert!(!knob_spelling_armed(Some("off")));
        assert!(!knob_spelling_armed(Some("")));
        assert!(!knob_spelling_armed(Some("true")), "typos fail safe to OFF");
        assert!(!knob_spelling_armed(Some("ON")), "case-sensitive, like the arm knobs");
        assert!(knob_spelling_armed(Some("1")));
        assert!(knob_spelling_armed(Some("on")));
        // The live getters memoize the process env; unset in the test
        // binary. DECOROOT is DEFAULT ON since the conversion-flips train
        // (GL-DECOROOT-1; =0|off kills — the flipped-kill idiom).
        assert!(decoroot_enabled(), "conversion-flips: unset => ON (GL-DECOROOT-1)");
        // NUMJOIN is DEFAULT ON since the conversion-flips train (GL-NUMJOIN-1).
        assert!(aggjoin_numeric_enabled(), "conversion-flips: unset => ON (GL-NUMJOIN-1)");
    }

    /// SE-DECOROOT hash-election margin: the provisional bound must stay
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

    /// SE-JHEAP knob (the GL-JHEAP-1 lane): DEFAULT OFF, `1`/`on` arms
    /// (the shared conversion-car idiom); the executor coherence mirror
    /// resolves LIVE in a knob-free process (K2_PROBE/HEAPFEED are
    /// default-ON with `=0|off` kills — the SE9/SE15 flipped posture), so
    /// the probe's heap gate is exactly the jheap knob at defaults. Pins
    /// the default-OFF inertness and the mirror's default-ON reading.
    #[test]
    fn jheap_knob_default_off_mirror_live() {
        // conversion-flips: JHEAP is DEFAULT ON (GL-JHEAP-1; =0|off kills).
        assert!(jheap_enabled(), "conversion-flips: unset => ON (GL-JHEAP-1)");
        assert!(
            k2_heapfeed_live(),
            "K2_PROBE/HEAPFEED default ON (SE9/SE15 flips) => mirror live"
        );
    }

    /// SE-CBKEYS knob (the GL-CBKEYS-1 lane): DEFAULT OFF, `1`/`on` arms
    /// (the shared conversion-car idiom) — bytes-key join shapes are unkeyable
    /// at default, byte-identical plan time; and the bytes floor keeps the
    /// grouped-join 2M ceiling (the scan text-key min_dop discipline is
    /// subsumed — its low-dop win region covers the whole admitted range).
    #[test]
    fn cbkeys_knob_default_off_and_floor() {
        // conversion-flips: CBKEYS is DEFAULT ON (GL-CBKEYS-1; =0|off kills).
        assert!(cbkeys_enabled(), "conversion-flips: unset => ON (GL-CBKEYS-1)");
        let g = cbkeys_guard();
        assert_eq!(g.max_rows, 2_000_000.0);
        assert_eq!(g.min_rows, 0.0);
    }

    /// SE-BPCHAR sub-knob (the GL-BPCHAR-1 lane): DEFAULT OFF, `1`/`on`
    /// arms — bpchar keys are unkeyable at default even with CBKEYS armed
    /// (the sub-gate composes: BOTH knobs must be on). The tie law itself
    /// is proven in the adt_varchar crate's corpus against the real
    /// bpchar_input/bpchareq (bpchar_tie_law_* tests).
    #[test]
    fn bpchar_subknob_default_off() {
        // conversion-flips: DEFAULT ON (GL-BPCHAR-1; =0|off kills).
        assert!(bpchar_keys_enabled(), "conversion-flips: unset => ON (GL-BPCHAR-1)");
    }

    /// SE-FILTERQUALS knob (the GL-FILTERQUALS-1 lane): DEFAULT OFF, `1`/
    /// `on` arms — filtered grouped-join shapes keep the X5 bare-equi
    /// refusal byte-for-byte at default.
    #[test]
    fn joinfilters_knob_default_off() {
        // conversion-flips: DEFAULT ON (GL-FILTERQUALS-1; =0|off kills; the
        // ladder carried NO selectivity floor — its explicit verdict).
        assert!(joinfilters_enabled(), "conversion-flips: unset => ON (GL-FILTERQUALS-1)");
    }

    /// SE-JHEAP NL-election margin + floor: the margin must be a real
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
        assert!(agg_strminmax_enabled(), "CAR B must be ON at default (GL-STRMM-2 flip)");
        assert!(agg_sort_nolimit_enabled(), "CAR C must be ON at default (GL-T2B flip)");
        // SE-TOPNNI (gap:topn-nonint-keys car): DEFAULT ON since the
        // GL-TOPNNI-1 flip — the flipped-kill rule (kill =0|off).
        assert!(topn_nonint_enabled(), "SE-TOPNNI must be ON at default (GL-TOPNNI-1 flip)");
    }

    /// SE-TOPNNI floor: its OWN min_dop=4 rectangle (GL-TOPNNI-1 verdict
    /// KEEP), no longer the int-key class_guard reuse — GL-COST-TOPN-1
    /// guarded the int class off (four-posture grid: zero best-of-four
    /// wins) and severed the reuse so the non-int car keeps riding its own
    /// witnessed record. Pin both postures so neither can drift silently.
    #[test]
    fn topn_nonint_guard_is_its_own_min_dop4_rectangle() {
        let g = topn_nonint_guard();
        assert_eq!(g.min_rows, 0.0);
        assert_eq!(g.max_rows, f64::INFINITY);
        assert_eq!(g.min_pages, 0.0);
        assert_eq!(g.min_dop, 4);
        assert_eq!(g.low_dop_max_rows, 0.0);
        // The int-key class itself: guarded OFF at every size at default
        // (GL-COST-TOPN-1; PGRUST_M5_TOPN_RECT=1 is the A/B restore).
        // Since GL-TOPNHEAP-1 the class's WIN region rides the CURVE
        // decide entry on top (k-band-gated) — the rectangle stays the
        // keep-Gather floor posture.
        let i = class_guard(CoverClass::CbTopnBoundedIntKeys);
        assert_eq!(i.max_rows, 0.0, "int-key topn must be guarded off");
        assert_eq!(i.min_dop, 0);
    }

    /// GL-TOPNHEAP-1 knob coherence: the planner routing twin reads the
    /// SAME PGRUST_RUNTIME_TOPN_HEAP spelling as the executor's direct
    /// feed (runtime_sort.rs) — DEFAULT ON, kill spellings exactly
    /// `0|off`. Killing restores the guard-off keep-Gather routing AND
    /// the incumbent accept path together (no posture can suppress onto
    /// a car that will not run).
    #[test]
    fn topn_heap_spelling_law() {
        assert!(topn_heap_spelling_on(None), "unset = ON (flipped-kill)");
        assert!(topn_heap_spelling_on(Some("1")));
        assert!(topn_heap_spelling_on(Some("on")));
        assert!(!topn_heap_spelling_on(Some("0")));
        assert!(!topn_heap_spelling_on(Some("off")));
        // Anything else is not a kill (the exact-spelling arm).
        assert!(topn_heap_spelling_on(Some("false")));
    }

    /// GL-TOPNHEAP-1 car-mirror payload vocabulary: an UNDER-approximation
    /// of the executor's byval census — text/varlena must miss (a mirror
    /// over-approximation would suppress onto the INCUMBENT arm, the
    /// measured k=1000 loss the guard-off exists to prevent).
    #[test]
    fn topn_car_payload_vocabulary_fails_closed() {
        for t in [INT2OID, INT4OID, INT8OID, DATEOID, TIMESTAMPOID] {
            assert!(topn_car_payload_type(t));
        }
        for t in [TEXTOID, VARCHAROID, 1700 /* numeric */, 2950 /* uuid */] {
            assert!(!topn_car_payload_type(t), "type {t} must miss the mirror");
        }
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

    /// GROUPED-AVG widening: the widened list is EXACTLY the base list plus
    /// avg(int2)/avg(int4) — the two OIDs whose grouped-sink combine class
    /// (the {count,sum} transarray pair) is admitted unconditionally by the
    /// sink's combine resolution. The INTERNAL-transtype avg/sum family
    /// stays out (named follow-up), and the BASE list stays avg-free (the
    /// default census is byte-identical knob-OFF).
    #[test]
    fn grouped_avg_widening_is_exactly_two_oids() {
        assert!(!GROUPED_SINK_AGGS.contains(&F_AVG_INT4));
        assert!(!GROUPED_SINK_AGGS.contains(&F_AVG_INT2));
        assert!(GROUPED_SINK_AGGS_AVG.contains(&F_AVG_INT4));
        assert!(GROUPED_SINK_AGGS_AVG.contains(&F_AVG_INT2));
        assert!(!GROUPED_SINK_AGGS_AVG.contains(&F_AVG_INT8), "INTERNAL transtype stays out");
        assert!(!GROUPED_SINK_AGGS_AVG.contains(&F_SUM_INT8), "INTERNAL transtype stays out");
        assert_eq!(GROUPED_SINK_AGGS_AVG.len(), GROUPED_SINK_AGGS.len() + 2);
        for oid in GROUPED_SINK_AGGS {
            assert!(GROUPED_SINK_AGGS_AVG.contains(oid), "widening is a superset");
        }
        // The knob rides the flipped-kill spelling helper (pinned above):
        // only 0|off restore the base list; unset stays widened.
    }

    /// TOPN-HIGHGROUPS: the exemption's sort-key set is exactly the
    /// finalfn-free int8-transvalue aggregates the sink's winner selection
    /// resolves — a strict subset of the grouped-sink passenger list.
    /// min/max carry finalfn-free int8 transvalues too but were never
    /// witnessed as winner-selection order columns at high group counts;
    /// avg carries a finalfn (declines the resolve). Both stay out.
    #[test]
    fn topn_highgroups_sort_vocabulary_is_finalfn_free_int8() {
        assert_eq!(TOPN_INT8_RAW_SORT_AGGS, &[F_COUNT_STAR, F_COUNT_ANY, F_SUM_INT4, F_SUM_INT2]);
        for oid in TOPN_INT8_RAW_SORT_AGGS {
            assert!(GROUPED_SINK_AGGS.contains(oid), "subset of the base passenger list");
        }
        assert!(!TOPN_INT8_RAW_SORT_AGGS.contains(&F_AVG_INT4), "finalfn-bearing stays out");
        assert!(!TOPN_INT8_RAW_SORT_AGGS.contains(&F_MAX_INT8), "unwitnessed order column");
        assert_eq!(SINK_TOPN_MAX_BOUND_MIRROR, 1 << 16, "mirror of the sink bound cap");
    }

    /// GL-ELECT22-1 fix 1: the mk-text family ceiling defaults — the 16M
    /// provisional (knob off: byte-identical pre-fix posture) vs the 24M
    /// refit-band bound v2-armed. The pinned census family estimate
    /// (17,614,259) must sit exactly in the gap: refused by the
    /// provisional, admitted by the refit — the fix's whole point.
    #[test]
    fn mktext_family_ceiling_defaults() {
        assert_eq!(mktext_family_ceiling_default(false), 16_000_000.0);
        assert_eq!(mktext_family_ceiling_default(true), 24_000_000.0);
        let census_family_est = 17_614_259.0;
        assert!(census_family_est >= mktext_family_ceiling_default(false));
        assert!(census_family_est < mktext_family_ceiling_default(true));
    }

    /// GL-ELECT22-1 fix 4a: the distinct-flavored exemption's sort-key
    /// vocabulary is exactly the never-NULL order columns the distinct
    /// sink's kernel-2 admission resolves (runtime_distinct.rs
    /// `distinct_topn_arm`): the count(DISTINCT) column itself (checked
    /// via `is_count_distinct_int` at the capture site) plus bare
    /// count(*)/count(x). sum(int2/4) is NULLABLE — the sink silently
    /// degrades it to the full drain (exactly the emit the §10 hold
    /// prices), so it must NEVER enter this set.
    #[test]
    fn distinct_topn_sort_vocabulary_is_never_null_counts() {
        assert_eq!(DISTINCT_TOPN_SORT_AGGS, &[F_COUNT_STAR, F_COUNT_ANY]);
        for oid in DISTINCT_TOPN_SORT_AGGS {
            assert!(
                TOPN_INT8_RAW_SORT_AGGS.contains(oid),
                "subset of the grouped-sink exemption vocabulary"
            );
        }
        assert!(!DISTINCT_TOPN_SORT_AGGS.contains(&F_SUM_INT4), "nullable sum stays out");
        assert!(!DISTINCT_TOPN_SORT_AGGS.contains(&F_SUM_INT2), "nullable sum stays out");
    }

    /// EXPRKEY-TOPN mirrors of record: the truncation funcid (the tz-less
    /// timestamp variant — the tz-aware pair must NEVER match) and the
    /// expr-key feed's int-equality table (exprkey.rs `INT_EQ_FNS`,
    /// vendored REL 18.3 pg_proc.dat). A drift here silently widens or
    /// narrows the probe against the arm it mirrors.
    #[test]
    fn exprkey_topn_mirrors_of_record() {
        assert_eq!(F_TIMESTAMP_TRUNC_FN, 2020);
        assert_eq!(INT_EQ_FNS_MIRROR, [63, 65, 467, 158, 159, 852, 474, 1850, 1856]);
    }

    /// GL-DICTDRAIN-1 mirrors of record: the 3-arg regexp_replace fmgr row
    /// (textregexreplace_noopt, vendored REL 18.3 pg_proc.dat — fmgr_core
    /// canonical.rs `(2284, "textregexreplace_noopt", 3, strict, retset
    /// false)`; the flags variant 2285 is deliberately NOT keyed in v1)
    /// and the provisional floor (the charter bar: sink beats serial at
    /// dop >= 4 — the witnessed ladder owns the re-derivation). A drift
    /// here silently moves the probe off the arm it mirrors.
    #[test]
    fn dictkey_mirrors_of_record() {
        assert_eq!(F_TEXTREGEXREPLACE_NOOPT, 2284);
        let g = dictkey_guard();
        assert_eq!(g.min_dop, 4, "the GL-DICTDRAIN-1 provisional floor");
        // The knob rides the still-gated tier-2 spelling (armed iff
        // exactly 1|on — pinned by tier2_car_spelling_* above): DEFAULT
        // OFF until the flip letter.
        assert!(!tier2_car_spelling_on(None));
        assert!(tier2_car_spelling_on(Some("1")) && tier2_car_spelling_on(Some("on")));
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
