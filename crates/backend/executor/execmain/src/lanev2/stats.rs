//! Lane-v2 engagement/refusal accounting — the substrate of the lane honesty
//! gates (design doc "Definition of complete": engagement floor +
//! assert-refuse allowlist; `scripts/lane-gates.sh`).
//!
//! Structure: one process-global relaxed-atomic counter per (shape class) for
//! OWNED decisions, and per (shape class × refusal reason) for REFUSED
//! decisions. Backends are threads of one process here, so the totals
//! aggregate across all backends for free.
//!
//! Tick semantics (documented per class; the gate's floor file restates them):
//!   * `SeqScan` — one tick per memoized static verdict (≈ one per scan node
//!     per (re)init), plus per-call ticks for the dynamic EPQ/backward gates
//!     (which run before the memo and are rare).
//!   * `IndexScan` / `IndexOnlyScan` / `BitmapHeapScan` — the fusibility check
//!     is per `exec_proc_node` call (not node-memoized), so owned/refused
//!     ticks are per-pull decisions. Counts are large but deterministic for a
//!     fixed corpus.
//!   * `AggBuild` — one OWNED tick per lane-owned agg feed event: a hash-agg
//!     build (the drain-the-scan-pipeline event), a plain-agg fold drive, or
//!     a sorted-agg stream start over a sort feed (index-order-fed sorted
//!     streams have no build event; their engagement ticks under the
//!     per-pull index classes as feed decisions). Refusals per offered call.
//!   * `SortFeed` — one OWNED tick per lane-owned sort feed; structural
//!     refusals once per memoized verdict; dynamic EPQ/backward per call.
//!   * `Join` — one OWNED tick per lane-owned join build event; structural
//!     refusals once per memoized verdict; dynamic EPQ/backward,
//!     fused-probe-drive economics, and multi-batch spill refusals per call.
//!   * `NestLoop` — one OWNED tick per accepted outer row (the unit the lane
//!     owns: bind params → rescan the inner → drain the expansion);
//!     structural refusals once per memoized verdict; dynamic EPQ/backward
//!     per call.
//!   * `Group` — one OWNED tick per lane-owned group-over-sort drive start
//!     (the underlying sort-feed event); refusals per offered call (the
//!     child-Sort verdict itself is memoized on the Sort node, so the
//!     per-call cascade is one flag load).
//!   * `ResultNode` — one OWNED tick per lane-owned Result execution (the
//!     no-FROM row / one-time-gate consumption, or the child feed event);
//!     refusals per offered call.
//!   * `SubqueryScan` — one OWNED tick per lane-owned feed event (the
//!     child sort feed for the bare hook; the agg build event for the
//!     agg-over-subquery composition); refusals per offered call.
//!   * `Append` — one OWNED tick per memoized structural verdict (per Append
//!     node per (re)init, like the seqscan class); structural child refusals
//!     once per memoized verdict, dynamic EPQ/backward/parallel gates per
//!     offered call.
//!   * `ProjectSet` — never owned (a documented wholesale refuse, design §4);
//!     one REFUSED tick per offered call.
//!
//! Overhead: with the lane OFF nothing here ever runs (the dispatch hooks gate
//! on `lanev2::enabled()` before any lane code). With the lane ON but
//! accounting disarmed (no `PGRUST_LANE_V2_STATS`), every tick is one cached
//! pointer load + branch. With accounting armed it is additionally one relaxed
//! `fetch_add` per lane-path decision — decisions, not rows (except the
//! per-pull index/IOS/bitmap classes noted above, which are exactly as
//! frequent as the fusibility checks the lane already runs there).
//!
//! Reporting: `PGRUST_LANE_V2_STATS=<dir>` arms the accounting; each backend
//! thread that ticked at least once dumps the *cumulative process-wide*
//! totals to `<dir>/lane-v2-stats.<pid>.tsv` when the thread exits (a TLS
//! drop guard — no exit-hook wiring, no new GUC, no change to `pg_settings`
//! byte-identity). Dumps overwrite atomically (tmp + rename) under a mutex,
//! so the last backend to exit leaves the final totals; the harness sums
//! across files (one per server *process*, i.e. per resume segment). NOT a
//! GUC by design — see the module doc of `lanev2` on `pg_settings`
//! byte-identity.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock};

/// Lane-ownable plan-shape classes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ShapeClass {
    SeqScan = 0,
    IndexScan = 1,
    IndexOnlyScan = 2,
    BitmapHeapScan = 3,
    AggBuild = 4,
    SortFeed = 5,
    Join = 6,
    NestLoop = 7,
    Group = 8,
    ResultNode = 9,
    SubqueryScan = 10,
    Append = 11,
    ProjectSet = 12,
    /// cbstore-backed SeqScan (the CbstoreSource tranche): counted apart
    /// from heap seqscans because the admission economics differ (standalone
    /// cbstore scans ARE lane-owned — the per-row drive lacks the batch
    /// kernels) and the regress corpus has no cbstore tables (its floor
    /// seeds from the dedicated cbstore e2e corpus).
    CbScan = 13,
    /// Metadata-answered plain agg over a bare cbstore scan (the metaagg
    /// footer-answer arm). OWNED ticks once per metadata-answered execution
    /// event; refusals tick ONLY for cbstore-backed plain-agg offers (heap
    /// scans are out of the arm's scope and tick nothing here) — structural
    /// reasons once per memoized per-node choice, runtime reasons per
    /// offered call. The regress corpus has no cbstore tables; the floor
    /// seeds from the cbstore e2e corpus.
    MetaAgg = 14,
    /// Streaming top-k cutoff pre-filter on the sort breaker feed: OWNED =
    /// one tick per sort feed the pre-filter ARMED on (engagement evidence
    /// for the internal fast path; never a refusal class — non-admission
    /// just feeds the sort unfiltered). Row-level effect is reported by the
    /// `counter` dump lines (`topkcut-rows-seen` / `topkcut-rows-cut`).
    TopkCut = 15,
}

const N_CLASSES: usize = 16;

impl ShapeClass {
    pub(super) const ALL: [ShapeClass; N_CLASSES] = [
        ShapeClass::SeqScan,
        ShapeClass::IndexScan,
        ShapeClass::IndexOnlyScan,
        ShapeClass::BitmapHeapScan,
        ShapeClass::AggBuild,
        ShapeClass::SortFeed,
        ShapeClass::Join,
        ShapeClass::NestLoop,
        ShapeClass::Group,
        ShapeClass::ResultNode,
        ShapeClass::SubqueryScan,
        ShapeClass::Append,
        ShapeClass::ProjectSet,
        ShapeClass::CbScan,
        ShapeClass::MetaAgg,
        ShapeClass::TopkCut,
    ];

    pub(super) fn name(self) -> &'static str {
        match self {
            ShapeClass::SeqScan => "seqscan",
            ShapeClass::IndexScan => "indexscan",
            ShapeClass::IndexOnlyScan => "indexonlyscan",
            ShapeClass::BitmapHeapScan => "bitmapheapscan",
            ShapeClass::AggBuild => "aggbuild",
            ShapeClass::SortFeed => "sortfeed",
            ShapeClass::Join => "join",
            ShapeClass::NestLoop => "nestloop",
            ShapeClass::Group => "group",
            ShapeClass::ResultNode => "result",
            ShapeClass::SubqueryScan => "subqueryscan",
            ShapeClass::Append => "append",
            ShapeClass::ProjectSet => "projectset",
            ShapeClass::CbScan => "cbscan",
            ShapeClass::MetaAgg => "metaagg",
            ShapeClass::TopkCut => "topkcut",
        }
    }
}

/// Why the lane refused a shape it was offered. Every variant an actual
/// refusal site can tick MUST appear in `scripts/lane-gates.allowlist` —
/// adding a variant here without an allowlist entry makes the gate fail the
/// first time it is observed, which is the point: a new deliberate refusal is
/// a reviewed, documented act.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RefuseReason {
    /// An in-lane kill switch is off. The master `PGRUST_LANE_V2` switch is
    /// checked by the dispatch hooks *before* any lane code runs and never
    /// ticks this; the metaagg arm's `PGRUST_LANE_V2_METAAGG=0` disarm ticks
    /// it (once per memoized per-node choice on a cbstore-backed plain agg).
    EnvOff = 0,
    /// EvalPlanQual re-check active (model-incompatible, §4).
    Epq = 1,
    /// Non-forward scan direction / backward index order.
    Backward = 2,
    /// Scrollable/backward/mark cursor eflags (`!batch_allowed`).
    ScrollMark = 3,
    /// EXPLAIN ANALYZE instrumentation (§4: refused by policy).
    Instrumented = 4,
    /// Parallel-aware node / worker shared state (Phase-2 worker-safety
    /// pending for the index/IOS/bitmap lanes).
    ParallelGate = 5,
    /// Qual/projection/recheck carries a SubPlan or exec-param dependency.
    SubplanParam = 6,
    /// Index runtime keys (exec-param-driven rescan keys).
    RuntimeKeys = 7,
    /// Non-btree index AM.
    NonBtree = 8,
    /// Non-MVCC snapshot (tidrun batching unsound).
    NonMvccSnapshot = 9,
    /// SeqScan Bloom variant.
    BloomVariant = 10,
    /// Table AM lacks the page-batch primitives.
    NoPageBatch = 11,
    /// amcanorderbyop reorder (`iss_OrderBy` / `ioss_OrderByKeys`).
    OrderByReorder = 12,
    /// Index/IOS/bitmap lanes admit only the bare-tuple shape today: a scan
    /// qual or projection refuses (Phase-2 breadth hosts them).
    ShapeQualProj = 13,
    /// Sort breaker: tuplesort random access required (REWIND/BACKWARD/MARK).
    RandomAccess = 14,
    /// Sort breaker: child is not a lane-fusible scan node type.
    NonScanChild = 15,
    /// Sort breaker: the child scan's own refuse-set refused (the specific
    /// reason is ticked under the child's class).
    ChildScanRefused = 16,
    /// Agg-side shape refusal, per offered call: the hashed breaker's
    /// batch-drainable gate (grouping sets / DISTINCT-or-ordered input /
    /// merge phase / subplan transitions / non-AGG_HASHED / initplan params)
    /// and the sorted streaming operator's admission
    /// (`agg_sorted_lane_admissible`: grouping sets / merge /
    /// within-aggregate internal sorts / subplan transitions / initplan
    /// params / non-AGG_SORTED strategies at its arms).
    AggNotDrainable = 17,
    /// Admission economics (design §4): the legacy fused drive already owns
    /// this shape better than the v2 pipeline (fused agg batch drive; fused
    /// hash-join probe drive). Ticked once per memoized agg-lane choice, and
    /// per pull for the join dispatch arm.
    AdmissionEconomicsFusedDrive = 18,
    /// Admission economics (design §4): no batch-consuming parent, so v2
    /// ownership is pure adapter overhead (`STANDALONE_SCAN_NO_UPSIDE`).
    /// Ticked per pull at the standalone `try_own_*` scan hooks.
    AdmissionEconomicsNoConsumer = 19,
    /// Dynamic tiny-input row-floor (§4 endgame refuse-set): the relation is
    /// too small for lane ownership to recover its own admission-probe cost
    /// (armed 2026-07-12 at the cbstore standalone hook — the memoized tiny
    /// verdict is taken BEFORE the qual-translate/arm cascade runs).
    TinyInputFloor = 20,
    /// Join-side shape refuse (hash-join breaker and NestLoop TupleOp) —
    /// non-INNER faces (hash join), joinqual/otherqual residuals (hash
    /// join), instrumented, subplan/param-bearing join exprs / quals /
    /// projection, or a node the row path already drove (whole-life
    /// ownership).
    JoinShape = 21,
    /// Hash-join breaker: the completed build's final nbatch > 1 (spill);
    /// the probe is refused before any lane tuple is emitted.
    MultiBatch = 22,
    /// Wave-4 streaming glue (Group / Result / SubqueryScan): the node's
    /// child is not a lane-owned pipeline this hook can chain onto — wrong
    /// node type, or a lane-ownable child whose own refuse-set refused (the
    /// specific reason ticks under the child's class).
    ChildNotLaneOwned = 23,
    /// ProjectSet: refused wholesale (documented refuse, design §4). The SRF
    /// ValuePerCall/Materialize multi-call protocol is per-tuple stateful
    /// (`pending_srf_tuples` resume, `args_valid` arg pinning, Materialize
    /// tuplestore read-back); an expanding-`TupleOp` hosting is model-
    /// compatible in principle but has no lane-owned child shape to chain
    /// onto in practice (ProjectSet children are scans, which refuse
    /// standalone ownership) — zero upside today. Re-evaluated when the
    /// design's "SRFs = expanding operator" phase item lands.
    SrfSetExpansion = 24,
    /// Stage-2.2 compact agg table: the K2-admitted shape's grouping key is
    /// not an admitted compact key kind (text/expr kernels keep the C-ported
    /// tuplehash — a mode choice inside a still-lane-owned build, ticked so
    /// the compact rollout is observable; see nodeagg::compact).
    CompactKeyKind = 25,
    /// Stage-2.2 compact agg table: spill-eligible by planner estimate — v1
    /// REFUSES the compact table (the C table spills; distinct-spill is v2,
    /// per the plan's 2.2 item). Ticked per build decision.
    CompactSpillRisk = 26,
    /// Stage-2.1 dict-code grouping (cbstore dict-group feed): the offered
    /// cbstore K2 fold shape cannot host dict-code grouping — varlena-guard
    /// (str MIN/MAX) fold plans, an unarmable columnar prefix, or a staging
    /// consumer conflict. Mode-choice observability inside a still-lane-owned
    /// build (like the compact rows); ticked once per build decision.
    DictGroupShape = 27,
    /// Metaagg arm structural shape refuse (once per memoized per-node
    /// choice on a cbstore-backed plain agg): a transition set that is not
    /// all-footer-answerable (classify_meta None — floats/bools/bitwise/
    /// text, FILTER, DISTINCT/ORDER BY, non-affine transforms), or a scan
    /// that is not bare (qual/projection/zone quals).
    MetaShape = 28,
    /// Metaagg arm runtime refuse (per offered call): the AM declined
    /// (parallel scan desc / uncovered column type), or a guarded sum's
    /// interval is unproven against the visible rows' footer min/max — the
    /// per-row drive owns the call and raises C's overflow error at C's row.
    MetaRuntime = 29,
    /// cbstore standalone scan: the scan HAS a qual, but no staged kernel
    /// armed for it — the PREWHERE walker/translate refused the shape
    /// (anchored/underscore/escape LIKE classes, non-whitelisted comparators,
    /// hybrid prefixes below the engagement gate). Split out of
    /// `AdmissionEconomicsNoConsumer` at the likeband landing so the gates
    /// can count the residual after the fixed-width-prefix refusal died
    /// (refusal-audit rider, 2026-07-14). Per-PULL cadence on the memoized
    /// verdict, like the cbscan admission row.
    QualNotVectorizable = 30,
    /// Plain-agg (ungrouped) plans whose transitions read NO input columns —
    /// pure count(*)-style census shapes. Deliberately refused by the cbstore
    /// per-row breaker-feed admission (lane-v2-noqualfeed): the census answer
    /// needs no data decode at all (heap: the incumbent fused drive's
    /// storeless advance; cbstore: the MetaAggScan footer path / the empty
    /// needed-set per-row walk), so a batch-decoded feed has nothing to win.
    CountOnlyCensus = 31,
    /// Multi-key packed grouping (multikey spike §2.4): the 2..N-key shape
    /// is not packable — a non-Int/non-dict-text component, widths past the
    /// 16-byte image, a missing dict-lane registration for the text
    /// component, or an unstageable key column. Mode-choice observability
    /// inside a still-lane-owned build; ticked once per build decision.
    MultiKeyShape = 32,
    /// Expression-group-key feed (expr-key tranche): the offered projected
    /// hash-agg-over-scan shape cannot host a computed grouping key — census
    /// outside the vocabulary (multi-key / multiple computed columns /
    /// non-admitted fns), an unmappable transition/needed column, a
    /// non-hostable probe kernel, or an unarmable staging prefix. Ticked once
    /// per memoized agg-lane choice; the build stays on the per-row breaker
    /// feed (byte-identical).
    ExprKeyShape = 33,
    /// Reduced grouping (redundant-key elimination): the offered 2..N-key
    /// projected shape is outside the Var-±-Const-over-one-bare-Var-key
    /// vocabulary (multiple bare-Var keys, expression-on-expression,
    /// mul/div, cross-Var arithmetic, mixed key widths, residual
    /// transitions, an unmappable needed column, an empty overflow-free
    /// domain, or an unarmable staging prefix). Ticked once per memoized
    /// agg-lane choice; the build keeps the per-row feed byte-identically.
    RedKeyShape = 34,
}

const N_REASONS: usize = 35;

impl RefuseReason {
    pub(super) fn name(self) -> &'static str {
        match self {
            RefuseReason::EnvOff => "env-off",
            RefuseReason::Epq => "epq",
            RefuseReason::Backward => "backward",
            RefuseReason::ScrollMark => "scroll-mark",
            RefuseReason::Instrumented => "instrumented",
            RefuseReason::ParallelGate => "parallel-gate",
            RefuseReason::SubplanParam => "subplan-param",
            RefuseReason::RuntimeKeys => "runtime-keys",
            RefuseReason::NonBtree => "non-btree",
            RefuseReason::NonMvccSnapshot => "non-mvcc-snapshot",
            RefuseReason::BloomVariant => "bloom-variant",
            RefuseReason::NoPageBatch => "no-pagebatch",
            RefuseReason::OrderByReorder => "order-by-reorder",
            RefuseReason::ShapeQualProj => "shape-qual-proj",
            RefuseReason::RandomAccess => "random-access",
            RefuseReason::NonScanChild => "non-scan-child",
            RefuseReason::ChildScanRefused => "child-scan-refused",
            RefuseReason::AggNotDrainable => "agg-not-drainable",
            RefuseReason::AdmissionEconomicsFusedDrive => "admission-economics-fused-drive",
            RefuseReason::AdmissionEconomicsNoConsumer => "admission-economics-no-consumer",
            RefuseReason::TinyInputFloor => "tiny-input-floor",
            RefuseReason::JoinShape => "join-shape",
            RefuseReason::MultiBatch => "multi-batch",
            RefuseReason::ChildNotLaneOwned => "child-not-lane-owned",
            RefuseReason::SrfSetExpansion => "srf-set-expansion",
            RefuseReason::CompactKeyKind => "compact-key-kind",
            RefuseReason::CompactSpillRisk => "compact-spill-risk",
            RefuseReason::DictGroupShape => "dictgroup-shape",
            RefuseReason::MetaShape => "meta-shape",
            RefuseReason::MetaRuntime => "meta-runtime",
            RefuseReason::QualNotVectorizable => "qual-not-vectorizable",
            RefuseReason::CountOnlyCensus => "count-only-census",
            RefuseReason::MultiKeyShape => "multikey-shape",
            RefuseReason::ExprKeyShape => "exprkey-shape",
            RefuseReason::RedKeyShape => "redkey-shape",
        }
    }

    fn from_index(i: usize) -> RefuseReason {
        use RefuseReason::*;
        [
            EnvOff,
            Epq,
            Backward,
            ScrollMark,
            Instrumented,
            ParallelGate,
            SubplanParam,
            RuntimeKeys,
            NonBtree,
            NonMvccSnapshot,
            BloomVariant,
            NoPageBatch,
            OrderByReorder,
            ShapeQualProj,
            RandomAccess,
            NonScanChild,
            ChildScanRefused,
            AggNotDrainable,
            AdmissionEconomicsFusedDrive,
            AdmissionEconomicsNoConsumer,
            TinyInputFloor,
            JoinShape,
            MultiBatch,
            ChildNotLaneOwned,
            SrfSetExpansion,
            CompactKeyKind,
            CompactSpillRisk,
            DictGroupShape,
            MetaShape,
            MetaRuntime,
            QualNotVectorizable,
            CountOnlyCensus,
            MultiKeyShape,
            ExprKeyShape,
            RedKeyShape,
        ][i]
    }
}

static OWNED: [AtomicU64; N_CLASSES] = [const { AtomicU64::new(0) }; N_CLASSES];

/// Row-level top-k cutoff effect counters (informational `counter` dump
/// lines; the gate's floor/allowlist machinery reads only owned/refused).
/// `SEEN` counts rows offered to an armed, engaged pre-filter (bounded heap
/// full + key lane staged); `CUT` counts the rows it discarded without a
/// tuplesort put.
static TOPKCUT_ROWS_SEEN: AtomicU64 = AtomicU64::new(0);
static TOPKCUT_ROWS_CUT: AtomicU64 = AtomicU64::new(0);
#[allow(clippy::declare_interior_mutable_const)]
static REFUSED: [[AtomicU64; N_REASONS]; N_CLASSES] =
    [const { [const { AtomicU64::new(0) }; N_REASONS] }; N_CLASSES];

/// The accounting arm switch: `PGRUST_LANE_V2_STATS=<dir>`. Resolved once per
/// process, like `lanev2::enabled()`.
fn stats_dir() -> Option<&'static PathBuf> {
    static DIR: OnceLock<Option<PathBuf>> = OnceLock::new();
    DIR.get_or_init(|| std::env::var_os("PGRUST_LANE_V2_STATS").map(PathBuf::from))
        .as_ref()
}

/// Record an OWNED decision for `class`. One cached load + branch when
/// accounting is disarmed.
#[inline]
pub(super) fn tick_owned(class: ShapeClass) {
    if stats_dir().is_none() {
        return;
    }
    OWNED[class as usize].fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
}

/// Record a REFUSED decision for `class` with its reason.
#[inline]
pub(super) fn tick_refused(class: ShapeClass, reason: RefuseReason) {
    if stats_dir().is_none() {
        return;
    }
    REFUSED[class as usize][reason as usize].fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
}

/// Accounting armed? Callers may gate row-counting work (a per-batch
/// popcount) on this before calling `tick_topkcut_rows`.
#[inline]
pub(super) fn armed() -> bool {
    stats_dir().is_some()
}

/// Record one engaged top-k pre-filter batch: `seen` rows offered, `cut`
/// rows discarded ahead of the tuplesort.
#[inline]
pub(super) fn tick_topkcut_rows(seen: u64, cut: u64) {
    if stats_dir().is_none() {
        return;
    }
    TOPKCUT_ROWS_SEEN.fetch_add(seen, Relaxed);
    TOPKCUT_ROWS_CUT.fetch_add(cut, Relaxed);
    arm_dump_on_thread_exit();
}

/// TLS drop guard: any backend thread that ticked dumps the cumulative totals
/// on its way out (backend exit = thread exit in this server). Dump-on-exit
/// keeps the hot path free of I/O and needs no exit-callback registration.
struct DumpOnThreadExit;

impl Drop for DumpOnThreadExit {
    fn drop(&mut self) {
        dump();
    }
}

#[inline]
fn arm_dump_on_thread_exit() {
    thread_local! {
        static GUARD: DumpOnThreadExit = const { DumpOnThreadExit };
    }
    // Touching the TLS key initializes it (arming the drop) on first use.
    GUARD.with(|_| {});
}

/// Write the cumulative process-wide totals to
/// `<dir>/lane-v2-stats.<pid>.tsv` (atomic tmp+rename; serialized so the last
/// writer's snapshot is also the latest one). Lines:
///   `owned\t<class>\t<count>`            (every class, zeros included)
///   `refused\t<class>\t<reason>\t<count>` (nonzero only)
///   `counter\t<name>\t<count>`            (informational row-level totals;
///                                          the gate aggregation ignores them)
fn dump() {
    let Some(dir) = stats_dir() else { return };
    static DUMP_LOCK: Mutex<()> = Mutex::new(());
    let _guard = match DUMP_LOCK.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let mut out = String::new();
    for class in ShapeClass::ALL {
        out.push_str(&format!(
            "owned\t{}\t{}\n",
            class.name(),
            OWNED[class as usize].load(Relaxed)
        ));
    }
    for class in ShapeClass::ALL {
        for (i, cell) in REFUSED[class as usize].iter().enumerate() {
            let n = cell.load(Relaxed);
            if n > 0 {
                out.push_str(&format!(
                    "refused\t{}\t{}\t{}\n",
                    class.name(),
                    RefuseReason::from_index(i).name(),
                    n
                ));
            }
        }
    }
    out.push_str(&format!(
        "counter\ttopkcut-rows-seen\t{}\n",
        TOPKCUT_ROWS_SEEN.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\ttopkcut-rows-cut\t{}\n",
        TOPKCUT_ROWS_CUT.load(Relaxed)
    ));
    let pid = std::process::id();
    let final_path = dir.join(format!("lane-v2-stats.{pid}.tsv"));
    let tmp_path = dir.join(format!(
        ".lane-v2-stats.{pid}.{:?}.tmp",
        std::thread::current().id()
    ));
    // Best-effort by design: accounting must never turn into a query error.
    let _ = std::fs::create_dir_all(dir);
    if std::fs::write(&tmp_path, out).is_ok() {
        let _ = std::fs::rename(&tmp_path, &final_path);
    }
}
