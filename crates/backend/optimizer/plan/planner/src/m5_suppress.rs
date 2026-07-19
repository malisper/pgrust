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
//!     until parity), default 1e6 estimated groups.
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
    /// only), cbstore AM (heap sides ride the K2 knobs, DEFAULT-OFF — a
    /// heap-keyed suppression would land on serial). Everything else —
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
        // GUARDED OFF (max_rows=0): 4.30–8.00x vs legacy at EVERY measured
        // (size, dop) — rt grows with size (11→44ms vs serial 4.3→10.1),
        // refuting the fixed-ceremony theory; this is sort-arm parallel
        // economics, the named follow-up. PGRUST_M5_SIZE_FLOORS=0
        // re-enables suppression for the measure vehicle.
        CoverClass::CbTopnBoundedIntKeys => FloorGuard { max_rows: 0.0, ..NO_GUARD },
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
    }
}

/// M5-5 floors kill switch: PGRUST_M5_SIZE_FLOORS=0 disables every guard.
/// The rowflip measure vehicle runs floors-off so engagement economics
/// stay measurable at any (size, dop); production default ON.
fn size_floors_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_M5_SIZE_FLOORS").map_or(true, |v| v.trim() != "0"))
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
            return Ok(false);
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
    // Emit discipline: every tlist entry is a bare group-key Var, a
    // whitelisted sink aggregate (const tlist entries — the q35 refusal —
    // and non-identity emits classify uncovered here), or a
    // count(DISTINCT <int Var>) — the runtime distinct sink's class
    // (CbDistinctIntKeys; int GROUP keys only, checked below).
    let mut n_count_distinct = 0usize;
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
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
        if !is_whitelisted_agg(tle.expr, rti, GROUPED_SINK_AGGS) {
            return Ok(false);
        }
    }
    // The distinct sink's class: int-family GROUP keys only (text grouped
    // distinct stays the refused distinct-text row).
    if n_count_distinct > 0 && n_text > 0 {
        return Ok(false);
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
        if !is_whitelisted_agg(tle.expr, rti, GROUPED_SINK_AGGS)
            && !is_count_distinct_int(tle.expr, rti)
        {
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
fn classify_join_covered(
    run: &mut PlannerRun<'_>,
    parse: &Query<'_>,
    je: &types_nodes::primnodes::JoinExpr<'_>,
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
fn classify_join_sides(
    run: &mut PlannerRun<'_>,
    parse: &Query<'_>,
    rti_l: usize,
    rti_r: usize,
    join_quals: Option<Node<'_>>,
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
        if rel.amflags & AMFLAG_PGRCOLUMNAR == 0 {
            return refuse_join("side not cbstore");
        }
        max_rows = max_rows.max(rel.rows.max(0.0));
        // Unindexed-only guard (see the fn doc): an index on either side
        // lets the costing pick serial merge/NL shapes the walk refuses.
        if !rel.indexlist.is_empty() {
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
    let mut has_equi = false;
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
        return refuse_join("no hashjoinable int-family equi clause");
    }
    // Emit discipline: every non-junk tlist entry is a whitelisted plain
    // aggregate whose args live on either joined rel (count(*) included).
    let mut n = 0usize;
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
        if !is_whitelisted_agg_2rti(tle.expr, rti_l, rti_r, PLAIN_FOLD_AGGS) {
            return refuse_join("tlist entry not a whitelisted plain agg");
        }
        n += 1;
    }
    if n == 0 {
        return refuse_join("empty tlist");
    }
    // Floor guard input: the larger side's estimated rows (the ladder's
    // per-table N; the probe fixture's dim side is negligible).
    finish(run, CoverClass::CbHashJoinPlainAgg, relids[0], 0.0, max_rows, 0.0)
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
    let Some((relids, max_rows)) = multibuild_rel_guards(run, parse, rtis)? else {
        return Ok(false);
    };
    if !equi_graph_connected(rtis, quals)? {
        return refuse_join("equi graph does not connect all relations");
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
    let mut n = 0usize;
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
        if !is_whitelisted_agg_nrti(tle.expr, rtis, PLAIN_FOLD_AGGS) {
            return refuse_join("tlist entry not a whitelisted plain agg");
        }
        n += 1;
    }
    if n == 0 {
        return refuse_join("empty tlist");
    }
    // Floor guard input: the largest rel's estimated rows (the nbatch1
    // ladder's per-table N — provisional reuse, see class_guard).
    finish(run, CoverClass::CbHashJoinMultiBuild, relids[0], 0.0, max_rows, 0.0)
}

/// The multibuild per-relation guards, shared by the plain and grouped rows
/// (extracted verbatim at SE-AGGJOIN): plain DISTINCT unindexed cbstore
/// rels (the B1 self-join discipline), EVERY rel's build estimate
/// nbatch==1. `None` = refused (traced).
fn multibuild_rel_guards(
    run: &mut PlannerRun<'_>,
    parse: &Query<'_>,
    rtis: &[usize],
) -> PgResult<Option<(Vec<u32>, f64)>> {
    let mut relids = Vec::with_capacity(rtis.len());
    let mut max_rows = 0.0f64;
    for &rti in rtis {
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
        if rel.amflags & AMFLAG_PGRCOLUMNAR == 0 {
            return refuse_join_none("side not cbstore");
        }
        max_rows = max_rows.max(rel.rows.max(0.0));
        if !rel.indexlist.is_empty() {
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
    Ok(Some((relids, max_rows)))
}

/// Traced refusal in `Option` position (the rel-guards helper's shape).
fn refuse_join_none<T>(why: &str) -> PgResult<Option<T>> {
    let _ = refuse_join(why)?;
    Ok(None)
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
    // Bare grouped aggregation only: the Agg must be the plan ROOT (any
    // sort/limit/distinct decoration plans a node above it — walk refusal).
    if !parse.hasAggs
        || parse.groupClause.is_nil()
        || !parse.distinctClause.is_nil()
        || !parse.sortClause.is_nil()
        || parse.limitCount.is_some()
        || parse.limitOffset.is_some()
    {
        return refuse_join("not a bare grouped aggregation");
    }
    // With either planner path off, the serial plan is a sort-grouped /
    // merge / NL shape the walk refuses (the suppress-then-refuse
    // direction, risk P1's false-positive arm) — keep Gather.
    if !crate::gucs::enable_hashagg() || !crate::gucs::enable_hashjoin() {
        return refuse_join("hashagg/hashjoin planner paths disabled");
    }
    let Some((relids, max_rows)) = multibuild_rel_guards(run, parse, rtis)? else {
        return Ok(false);
    };
    if !equi_graph_connected(rtis, quals)? {
        return refuse_join("equi graph does not connect all relations");
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
    let mut key_refs: Vec<u32> = Vec::new();
    for gc_node in &parse.groupClause {
        let Some(gc) = gc_node.as_sort_group_clause() else { return Ok(false) };
        let Some(tle) = tle_by_sortgroupref(parse, gc.tleSortGroupRef) else {
            return Ok(false);
        };
        let Some(v) = rtis.iter().find_map(|&rti| key_var(tle.expr, rti)) else {
            return refuse_join("group key not a bare joined-rel Var");
        };
        if !is_int_family(v.vartype) {
            return refuse_join("group key not int-family");
        }
        if !key_var_estimable(run, tle.expr)? {
            return refuse_join("group key without estimable ndistinct (statistics-free rel)");
        }
        key_refs.push(gc.tleSortGroupRef);
    }
    // Emit discipline: bare group-key Vars or whitelisted plain aggregates
    // (PLAIN_FOLD_AGGS — the grouped sink exports the numeric-family int
    // states the scan-grouped GROUPED_SINK_AGGS row refuses).
    let mut n_aggs = 0usize;
    for tle_node in &parse.targetList {
        let Some(tle) = tle_node.as_target_entry() else { return Ok(false) };
        if tle.ressortgroupref != 0 && key_refs.contains(&tle.ressortgroupref) {
            if rtis.iter().all(|&rti| key_var(tle.expr, rti).is_none()) {
                return Ok(false);
            }
            continue;
        }
        if !is_whitelisted_agg_nrti(tle.expr, rtis, PLAIN_FOLD_AGGS) {
            return refuse_join("tlist entry not a whitelisted plain agg");
        }
        n_aggs += 1;
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
    finish(run, CoverClass::CbHashJoinGroupedAgg, relids[0], ngroups, max_rows, 0.0)
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
    let covered = class_covered(class);
    // M5-5 engagement-floor guard: a covered class outside its measured
    // economics keeps Gather (routes legacy). Traced under its OWN prefix —
    // floor refusals are neither suppressions (M5CENSUS greps
    // `m5-suppress:`) nor arm refusals (`m5-suppress-refuse:`).
    if covered && size_floors_enabled() {
        let g = class_guard(class);
        let dop = guc_tables::runtime_pool::runtime_dop();
        let ok = rows >= g.min_rows
            && rows <= g.max_rows
            && pages >= g.min_pages
            && (dop >= g.min_dop || rows <= g.low_dop_max_rows);
        if !ok {
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
}
