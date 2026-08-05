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
//!   * `ProjectSet` — at default config never owned (the documented
//!     wholesale refuse, design §4): one REFUSED tick per offered call,
//!     unchanged. With the default-OFF `PGRUST_LANE_V2_ROWMODE` knob ON
//!     (Phase-0 row-mode facility, rowmode.rs), the admitted
//!     `ProjectSet ← childless Result` shape ticks OWNED once per offered
//!     pull the row-mode drive owns (the per-pull decision cadence of the
//!     index classes); refusals stay per offered call. NOTE for a future
//!     default flip: ProjectSet compositions then bypass `result_arm`, so
//!     the `result` class's owned floor must be reseeded alongside the
//!     `projectset` one (see notes/ws-e-rowmode-ledger.md).
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
    /// pgrcolumnar-backed SeqScan (the PgrcolumnarSource tranche): counted apart
    /// from heap seqscans because the admission economics differ (standalone
    /// pgrcolumnar scans ARE lane-owned — the per-row drive lacks the batch
    /// kernels) and the regress corpus has no pgrcolumnar tables (its floor
    /// seeds from the dedicated pgrcolumnar e2e corpus).
    CbScan = 13,
    /// Metadata-answered plain agg over a bare pgrcolumnar scan (the metaagg
    /// footer-answer arm). OWNED ticks once per metadata-answered execution
    /// event; refusals tick ONLY for pgrcolumnar-backed plain-agg offers (heap
    /// scans are out of the arm's scope and tick nothing here) — structural
    /// reasons once per memoized per-node choice, runtime reasons per
    /// offered call. The regress corpus has no pgrcolumnar tables; the floor
    /// seeds from the pgrcolumnar e2e corpus.
    MetaAgg = 14,
    /// Streaming top-k cutoff pre-filter on the sort breaker feed: OWNED =
    /// one tick per sort feed the pre-filter ARMED on (engagement evidence
    /// for the internal fast path; never a refusal class — non-admission
    /// just feeds the sort unfiltered). Row-level effect is reported by the
    /// `counter` dump lines (`topkcut-rows-seen` / `topkcut-rows-cut`).
    TopkCut = 15,
    /// Agg-over-Gather composition (the leader-side hash-agg breaker fed by
    /// the gather machinery as a source): OWNED ticks once per lane-owned
    /// build feed (alongside the aggbuild tick, the join/subquery cadence);
    /// refusals tick the dynamic per-call gates (EPQ/backward) here and the
    /// agg-side shape refusal under aggbuild.
    Gather = 16,
    /// Zone-ordered adaptive top-N traversal on a pgrcolumnar-fed bounded sort
    /// (docs/design/pgrcolumnar-zone-adaptive.md): OWNED = one tick per sort
    /// feed the adaptive granule order ARMED on (never a refusal class —
    /// non-admission keeps the physical-order feed). Demotions (ambiguous
    /// boundary tie observed → exact physical re-feed) are the
    /// `adaptivetopk-demoted` counter dump line.
    AdaptiveTopk = 17,
    /// Emit-side top-N boundary cut on the hash-agg-fed sort breaker
    /// (lane-v2 topnemit): OWNED = one tick per agg sort feed the boundary
    /// ARMED on (never a refusal class — non-admission just feeds the sort
    /// unfiltered). Group-level effect is reported by the `counter` dump
    /// lines (`topnemit-groups-seen` / `topnemit-groups-cut`).
    TopnEmit = 18,
    /// MergeJoin hosted as a row-mode LEAF (Phase-1 WS-G, rowmode.rs
    /// `try_own_merge_join` behind the default-OFF `PGRUST_LANE_V2_ROWMODE`
    /// knob; the ported FSM drives both children Volcano inside the leaf).
    /// OWNED once per drive start (the Group cadence; each owned PG pull
    /// starts one `pull_step_rows` drive here). Ticks fire ONLY knob-ON:
    /// knob-OFF ticks NOTHING — there is no pre-existing MergeJoin wholesale
    /// refuse (unlike ProjectSet), so default-config accounting is
    /// byte-identical by construction and the class stays silent until a
    /// flip seeds its floors (integration contract §2d).
    MergeJoin = 19,
    /// WindowAgg lane hosting (Phase-1 WS-H, lanev2/windows.rs behind the
    /// default-OFF `PGRUST_LANE_V2_WINDOWS` knob). OWNED once per drive
    /// start (the Group cadence). Ticks fire ONLY knob-ON: knob-OFF ticks
    /// NOTHING (silent at default config, zero floor drift — integration
    /// contract §2d; floor seeding is flip-time work).
    WindowAgg = 20,
    // -----------------------------------------------------------------------
    // Wave-2 vocabulary (THE one wave-2 vocab commit, integration contract
    // §1 — WS-L authors, everyone consumes; nobody edits discriminants after
    // this commit; later needs go through a reconciler amendment).
    //
    // Classes 21-36 are the WS-L row-mode read-side tail (rowmode_tail.rs
    // behind the default-OFF `PGRUST_LANE_V2_ROWMODE` knob): pure delegation
    // leaves through the row-mode host template. Tick cadence for ALL of
    // them = the MergeJoin/WindowAgg cadence: OWNED once per drive start
    // (each owned PG pull starts one `pull_step_rows` drive); dynamic
    // per-call EPQ/backward/instrumented refusals only. Ticks fire ONLY
    // knob-ON — none of these shapes has a pre-existing wholesale refuse, so
    // knob-OFF ticks NOTHING and default-config accounting stays
    // byte-identical by construction (§2d); floors seed at flip time.
    //
    // SubqueryScan REUSES class 10 (§1: mechanism attribution goes in the
    // EngineEvent detail string, never a second class). LockRows class 36 is
    // SHARED by WS-L's delegation hosting and WS-N's later TupleOp hosting
    // (same rule). WindowAgg T2 REUSES class 20.
    // -----------------------------------------------------------------------
    FunctionScan = 21,
    TableFuncScan = 22,
    ValuesScan = 23,
    SampleScan = 24,
    TidScan = 25,
    TidRangeScan = 26,
    NamedTuplestoreScan = 27,
    Material = 28,
    CteScan = 29,
    /// RecursiveUnion + its WorkTableScan leaves (classes 30/31) follow the
    /// same delegation cadence; the iteration protocol stays inside the
    /// ported `exec_recursive_union` body (the shared-slot law binds the
    /// RowSource: es_worktable_shared take-use-put-back per call, no cached
    /// handles or read positions across `next_row` calls — contract §3.8).
    RecursiveUnion = 30,
    WorkTableScan = 31,
    Memoize = 32,
    SetOp = 33,
    MergeAppend = 34,
    Unique = 35,
    /// SHARED class (contract §1): WS-L's LockRows-without-EPQ delegation
    /// hosting now, WS-N inc-2b's TupleOp hosting later — mechanism
    /// attribution via the EngineEvent detail string, never a second class.
    LockRows = 36,
    /// RESERVED for WS-N (lanev2/dml.rs behind `PGRUST_LANE_V2_DML`):
    /// authored here per contract §1/§6-WS-L(1) so the wave-2 vocabulary is
    /// one commit. Ticks NOTHING until dml.rs lands.
    ModifyTable = 37,
    // -----------------------------------------------------------------------
    // Wave-9.5 cursor-admission vocabulary (WS-AI inc-1b,
    // `se/wave95-cursors-1b`, band 92001+; chartered append — the registry
    // is APPEND-ONLY, nobody edits discriminants above this line; the
    // wave-9 §5 zero-mint expectation is superseded by the inc-1b charter,
    // recorded in notes/se-wave9-ai.md).
    // -----------------------------------------------------------------------
    /// Forward-pull cursor admission (lane-cursors.md §1-§3, contract §3):
    /// the WHOLE-RUN refusal classes of the budgeted (FETCH-cadence) emit
    /// sink. Cadence = once per budgeted `execute_plan` run (never per
    /// tuple); ticks fire only with `PGRUST_LANE_V2_CURSORS` ON (knob-OFF
    /// never reaches the classifier), so default-config accounting is
    /// byte-identical by construction. A cursor refusal NEVER changes
    /// output bytes: the run rides Volcano exactly as today (fail-open law).
    Cursor = 38,
    // -----------------------------------------------------------------------
    // Wave-9.5 SPI-admission class (WS-AJ Stage-A seam, `se/spi-stage-a`;
    // APPEND-ONLY chartered mint, recorded in notes/se-spi-stage-a.md; the
    // wave-9 §5 placeholder in notes/se-phase0-integration.md names this
    // landing).
    // -----------------------------------------------------------------------
    /// Count-limited SPI-statement admission (docs/design/lane-spi.md §1/§3
    /// Stage A): the WHOLE-RUN refusal classes of a budgeted tcount-limited
    /// `CommandDest::Spi` run — `_SPI_pquery`'s STOP-then-END shape and the
    /// portal-fetch (SPI cursor / plpgsql FOR loop) resumable shape alike
    /// (notes/se-spi-stage-a.md §8). Cadence = once per budget-eligible
    /// `execute_plan` run (never per tuple); ticks fire only with
    /// `PGRUST_LANE_V2_SPI` ON (knob-OFF never reaches the classifier), so
    /// default-config accounting is byte-identical by construction. An SPI
    /// refusal NEVER changes output bytes: the statement rides Volcano
    /// exactly as today (fail-open law; refusal-not-error). Seam-visibility
    /// honesty (the `CursorWithHold` precedent): an SPI-entered run with a
    /// caller-supplied non-SPI receiver (`SPI_execute_extended` dest
    /// option) is NOT seam-visible below `executor_run` — it installs no
    /// budget and ticks nothing; no RESERVED variant is minted for it.
    Spi = 39,
    // -----------------------------------------------------------------------
    // Phase-5 D0 engagement-evidence class (Track-5 deletion program,
    // scratchpad/night/phase5-deletion-plan.md §2 D0; APPEND-ONLY chartered
    // mint — discriminants above this line stay frozen).
    // -----------------------------------------------------------------------
    // Discriminant 40 (ParDistinct — the lane-v2 pardistinct GM-hybrid
    // engine) was RETIRED at Phase-5 D1 with the engine's deletion: the
    // ticking sites are gone, so the row can never be nonzero again. The
    // discriminant stays reserved (frozen-mint law; never re-issue 40).
}

// 40, not 41: discriminant 40 (ParDistinct) retired at Phase-5 D1 — it was
// the top of the mint, so the dense counter arrays shrink with it.
const N_CLASSES: usize = 40;

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
        ShapeClass::Gather,
        ShapeClass::AdaptiveTopk,
        ShapeClass::TopnEmit,
        ShapeClass::MergeJoin,
        ShapeClass::WindowAgg,
        ShapeClass::FunctionScan,
        ShapeClass::TableFuncScan,
        ShapeClass::ValuesScan,
        ShapeClass::SampleScan,
        ShapeClass::TidScan,
        ShapeClass::TidRangeScan,
        ShapeClass::NamedTuplestoreScan,
        ShapeClass::Material,
        ShapeClass::CteScan,
        ShapeClass::RecursiveUnion,
        ShapeClass::WorkTableScan,
        ShapeClass::Memoize,
        ShapeClass::SetOp,
        ShapeClass::MergeAppend,
        ShapeClass::Unique,
        ShapeClass::LockRows,
        ShapeClass::ModifyTable,
        ShapeClass::Cursor,
        ShapeClass::Spi,
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
            ShapeClass::Gather => "gather",
            ShapeClass::AdaptiveTopk => "adaptivetopk",
            ShapeClass::TopnEmit => "topnemit",
            ShapeClass::MergeJoin => "mergejoin",
            ShapeClass::WindowAgg => "windowagg",
            ShapeClass::FunctionScan => "functionscan",
            ShapeClass::TableFuncScan => "tablefuncscan",
            ShapeClass::ValuesScan => "valuesscan",
            ShapeClass::SampleScan => "samplescan",
            ShapeClass::TidScan => "tidscan",
            ShapeClass::TidRangeScan => "tidrangescan",
            ShapeClass::NamedTuplestoreScan => "namedtuplestorescan",
            ShapeClass::Material => "material",
            ShapeClass::CteScan => "ctescan",
            ShapeClass::RecursiveUnion => "recursiveunion",
            ShapeClass::WorkTableScan => "worktablescan",
            ShapeClass::Memoize => "memoize",
            ShapeClass::SetOp => "setop",
            ShapeClass::MergeAppend => "mergeappend",
            ShapeClass::Unique => "unique",
            ShapeClass::LockRows => "lockrows",
            ShapeClass::ModifyTable => "modifytable",
            ShapeClass::Cursor => "cursor",
            ShapeClass::Spi => "spi",
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
    /// it (once per memoized per-node choice on a pgrcolumnar-backed plain agg).
    EnvOff = 0,
    /// EvalPlanQual re-check active (model-incompatible, §4).
    Epq = 1,
    /// TOMBSTONE (backward-execution wave B11, cursors inc-2 §6 rider row
    /// 11): the runtime-direction refusal. Every per-pull
    /// `es_direction`-driven tick site was DELETED together with its
    /// allowlist rows — the run seam refuses backward entry outright
    /// (deletion-prep B1), so no pull below it can be backward. The
    /// backward-INDEX-ORDER meaning this row used to share moved to its
    /// own `DescOrder` row (the B11 re-vocab; DESC plans stay). NEVER-
    /// TICKING; the variant stays because the registry is append-only and
    /// discriminants never move (the CursorScroll precedent).
    Backward = 2,
    /// Random-access/mark eflags on the scan (`!batch_allowed`). NARROWED
    /// TO MARK (backward-execution wave, rider row 11): B2 deleted the
    /// PortalStart scroll arm — the only REWIND|BACKWARD eflags producer —
    /// so the eflags a node can still see at init are the merge-join
    /// EXEC_FLAG_MARK family (and the name stays for the append-only
    /// serialized vocabulary).
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
    /// (armed 2026-07-12 at the pgrcolumnar standalone hook — the memoized tiny
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
    /// Stage-2.1 dict-code grouping (pgrcolumnar dict-group feed): the offered
    /// pgrcolumnar K2 fold shape cannot host dict-code grouping — varlena-guard
    /// (str MIN/MAX) fold plans, an unarmable columnar prefix, or a staging
    /// consumer conflict. Mode-choice observability inside a still-lane-owned
    /// build (like the compact rows); ticked once per build decision.
    DictGroupShape = 27,
    /// Metaagg arm structural shape refuse (once per memoized per-node
    /// choice on a pgrcolumnar-backed plain agg): a transition set that is not
    /// all-footer-answerable (classify_meta None — floats/bools/bitwise/
    /// text, FILTER, DISTINCT/ORDER BY, non-affine transforms), or a scan
    /// that is not bare (qual/projection/zone quals).
    MetaShape = 28,
    /// Metaagg arm runtime refuse (per offered call): the AM declined
    /// (parallel scan desc / uncovered column type), or a guarded sum's
    /// interval is unproven against the visible rows' footer min/max — the
    /// per-row drive owns the call and raises C's overflow error at C's row.
    MetaRuntime = 29,
    /// pgrcolumnar standalone scan: the scan HAS a qual, but no staged kernel
    /// armed for it — the PREWHERE walker/translate refused the shape
    /// (anchored/underscore/escape LIKE classes, non-whitelisted comparators,
    /// hybrid prefixes below the engagement gate). Split out of
    /// `AdmissionEconomicsNoConsumer` at the likeband landing so the gates
    /// can count the residual after the fixed-width-prefix refusal died
    /// (refusal-audit rider, 2026-07-14). Per-PULL cadence on the memoized
    /// verdict, like the cbscan admission row.
    QualNotVectorizable = 30,
    /// Plain-agg (ungrouped) plans whose transitions read NO input columns —
    /// pure count(*)-style census shapes. Deliberately refused by the pgrcolumnar
    /// per-row breaker-feed admission (lane-v2-noqualfeed): the census answer
    /// needs no data decode at all (heap: the incumbent fused drive's
    /// storeless advance; pgrcolumnar: the MetaAggScan footer path / the empty
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
    /// The ONE new wave-2 refusal variant (integration contract §1; part of
    /// the one wave-2 vocab commit, authored by WS-L for WS-N): DML shapes
    /// the lane-v2 dml hosting (lanev2/dml.rs, `PGRUST_LANE_V2_DML`)
    /// declines — triggers, cross-partition specifics, MERGE arms outside
    /// the admitted set. RESERVED: ticks NOTHING until WS-N's dml.rs lands
    /// (WS-L, WS-M, WS-P add ZERO reasons; the Layer-A assert reads reasons,
    /// never adds them).
    DmlShape = 35,
    // -----------------------------------------------------------------------
    // Wave-9.5 cursor-admission refusal taxonomy (WS-AI inc-1b,
    // `se/wave95-cursors-1b`; APPEND-ONLY — chartered mint, recorded in
    // notes/se-wave9-ai.md; the wave-9 §5 zero-mint expectation is
    // superseded by the inc-1b charter). All five tick ONLY under the
    // `ShapeClass::Cursor` class, once per budgeted run, knob-ON only; a
    // cursor refusal lands the WHOLE portal on Volcano byte-identically
    // (the fail-open law) — these rows are observability, never semantics.
    // -----------------------------------------------------------------------
    /// TOMBSTONE (SUNSET EXECUTED — se/seam-wiring, SE10-GATES item 1;
    /// notes/se-seam-wiring.md §5): the inc-1b scrollable-portal refusal.
    /// The tick site (the classifier's eflags arm,
    /// `cursor_admission_refusal`) and the `cursor cursor-scroll`
    /// allowlist row were REMOVED together once lane fill owned scroll
    /// stores — the audited shrink the wave-9.5 SUNSET note scheduled.
    /// Store-served SCROLL/HOLD portals are lane-ADMITTED; the eflags a
    /// run still carries (CURRENT-OF-eligible row-chain fills, D-CA-2's
    /// fence) refuse per-scan via `batch_allowed` and roll up as
    /// `cursor-plan-refused`. NEVER-TICKING; the variant stays because the
    /// registry is append-only and discriminants never move.
    CursorScroll = 36,
    /// Non-forward demand on a budgeted run (backward FETCH reaching the
    /// run seam). Reachable only through scroll-capable portals (NO SCROLL
    /// backward errors above the seam, pquery.rs no_scroll arm), but named
    /// apart from `CursorScroll` because the corpus's explicit-backward
    /// cells assert the direction demand, not the portal capability.
    CursorBackward = 37,
    /// SUNSET (wave-10 REMOVES this class, same ratification as
    /// `CursorScroll` — the two are named consistently so the wave-10
    /// removal is one audited shrink): WITH HOLD portal. RESERVED tick
    /// site: holdability is portal-level state (`CURSOR_OPT_HOLD`,
    /// portalcmds) and is NOT visible below the `executor_run` seam —
    /// today's structural posture already satisfies §5 (the persist drive
    /// is a count-0 run, which never installs a budget; pre-COMMIT FETCHes
    /// are ordinary forward budgeted runs and resume-forward is the
    /// DECIDED §5 shape). Ticks NOTHING until a seam-visible holdability
    /// signal exists or wave-10 removes it, whichever lands first
    /// (`DmlShape` reserved-row precedent).
    CursorWithHold = 38,
    /// `PersistHoldablePortal`'s COMMIT-time persist drive over an engaged
    /// lane pipeline. RESERVED tick site, same seam-visibility argument as
    /// `CursorWithHold` (the persist drive arrives as a plain count-0
    /// forward/NoMovement run); its wave-10 disposition rides the WITH-HOLD
    /// lazy-materialized-store contract (not marked SUNSET here — wave-10
    /// decides whether the persist path keeps a named class).
    CursorPersistHoldable = 39,
    /// The budgeted run's top plan carried no lane engagement — the whole
    /// portal rides Volcano (exactly as today). The plan's SPECIFIC refusal
    /// reasons tick under their own classes at the per-pull gates; this is
    /// the cursor-level roll-up. inc-1b detection breadth: scan-class
    /// engagement (seqscan/cbscan memoized verdicts + settled parks) —
    /// breaker-lane engagement (agg/sort/join builds) widens the detector
    /// in inc-1c (recorded in notes/se-wave9-ai.md; over-ticking on
    /// breaker-engaged plans is armed-accounting-only, never semantics).
    CursorPlanRefused = 40,
    // -----------------------------------------------------------------------
    // Wave-10 cursors inc-2 (WS-CB, `se/wave10-ws-cb`; APPEND-ONLY —
    // contract §3.3, the increment's SINGLE vocabulary mint; allowlist row
    // in the same commit; worklog notes/se-wave10-cb.md).
    // -----------------------------------------------------------------------
    /// TOMBSTONE (SUNSET EXECUTED — R1a, night/r1a-impl, the §2a reason-41
    /// completion; the `cursor-scroll` SUNSET precedent above). This
    /// accounted for a store fill over a CURRENT-OF-eligible plan taking
    /// the ROW-CHAIN fill so a POST-run `ss_ScanTupleSlot` read could
    /// capture the §4.2 identity (fill_portal_store_to's arm B). R1a
    /// UNIVERSALISED in-run capture: every eligible fill now runs ONE
    /// budgeted forward drive with the identity sidecar armed on the
    /// receiver, captured IN-RUN — the batch sink for the lane-owned
    /// capture-batch cell, the run seam's capture row loop for every shape
    /// the D-CA-2 fence keeps on the row chain. Arm B, its post-run read,
    /// the tick face (`push::cursor_fill_tid_capture_refused`), its seam,
    /// and the `cursor cursor-currentof-tidcapture` allowlist row were all
    /// REMOVED together — the reason no longer fires (R-VOCAB "reason no
    /// longer fires" criterion). NEVER-TICKING; the variant stays because
    /// the registry is append-only and discriminants never move.
    CursorCurrentOfTidCapture = 41,
    // Wave-9.5 SPI-admission refusal (WS-AJ Stage-A seam, `se/spi-stage-a`;
    // APPEND-ONLY chartered mint, recorded in notes/se-spi-stage-a.md). ONE
    // new variant: the classifier's shape arms REUSE the generic vocabulary
    // (`Backward` / `ScrollMark` / `ParallelGate` — the WS-AI ParallelGate
    // precedent). REACHABILITY, corrected by the review re-baseline
    // (notes/se-spi-stage-a.md §8; the original "all three structurally
    // unreachable from `_SPI_pquery`" record was falsified live):
    // `ScrollMark` ticks per fetch for every auto-SCROLL SPI portal (plain
    // plpgsql FOR loops whose plan supports backward scan —
    // SPI_cursor_open picks CURSOR_OPT_SCROLL, PortalStart passes
    // REWIND|BACKWARD eflags); `Backward` ticks via SPI_scroll_cursor_fetch
    // (plpgsql FETCH BACKWARD); both carry allowlist rows. `ParallelGate`
    // stays the fail-closed serial-law pin (count-limited runs serial by
    // the ported execmain.rs use_parallel_mode gate; no row). Ticks ONLY
    // under `ShapeClass::Spi`, once per budgeted run, knob-ON only; a
    // refusal lands the WHOLE statement on Volcano byte-identically
    // (fail-open law) — observability, never semantics.
    // -----------------------------------------------------------------------
    /// The budgeted SPI run's plan carried no lane engagement — the whole
    /// statement rides Volcano (exactly as today). The plan's SPECIFIC
    /// refusal reasons tick under their own classes at the per-pull gates;
    /// this is the SPI-level roll-up, ticked at the settle walk.
    /// Detection breadth = the WS-AI inc-1b scan-class detector (seqscan /
    /// cbscan memoized verdicts + settled claims); the same inc-1c breaker
    /// widening rides for both classes.
    /// Board-act drift record (the WS-CB "authored as 36" precedent
    /// above): authored as "next free discriminant, 41" on the branch
    /// before wave-10's `CursorCurrentOfTidCapture` landed 41; renumbered
    /// at the SE-BOARD-SPI merge — next free is 43.
    SpiPlanRefused = 42,
    // -----------------------------------------------------------------------
    // Backward-execution wave B11 (cursors inc-2 §6 rider row 11; APPEND-
    // ONLY chartered mint — the `Backward` row-retirement re-vocab).
    // -----------------------------------------------------------------------
    /// DESC-ordered index/IOS PLAN (indexorderdir backward — the planner's
    /// descending-scan shape, e.g. ORDER BY x DESC over a btree). Split out
    /// of the retired `Backward` row, which used to carry BOTH the runtime
    /// direction (now impossible below the forward-only run seam, B1) and
    /// this plan-shape refusal (still live: the lane's sequential tidrun
    /// drive is forward-only over the index). Ticks under the index/IOS
    /// classes at their admission gates; allowlist rows in the same commit.
    DescOrder = 43,
}

const N_REASONS: usize = 44;

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
            RefuseReason::DmlShape => "dml-shape",
            RefuseReason::CursorScroll => "cursor-scroll",
            RefuseReason::CursorBackward => "cursor-backward",
            RefuseReason::CursorWithHold => "cursor-with-hold",
            RefuseReason::CursorPersistHoldable => "cursor-persist-holdable",
            RefuseReason::CursorPlanRefused => "cursor-plan-refused",
            RefuseReason::CursorCurrentOfTidCapture => "cursor-currentof-tidcapture",
            RefuseReason::SpiPlanRefused => "spi-plan-refused",
            RefuseReason::DescOrder => "desc-order",
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
            DmlShape,
            CursorScroll,
            CursorBackward,
            CursorWithHold,
            CursorPersistHoldable,
            CursorPlanRefused,
            CursorCurrentOfTidCapture,
            SpiPlanRefused,
            DescOrder,
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
/// Zone-adaptive top-N demotions: the tracked feed observed an
/// arrival-order-sensitive tie at the LIMIT cut and re-fed physical-order.
static ADAPTIVE_TOPK_DEMOTED: AtomicU64 = AtomicU64::new(0);
static ADAPTIVE_TOPK_TIE_RELAXED: AtomicU64 = AtomicU64::new(0);
/// Rule-2 rowref-selection adaptive feeds that finished exact (no demote).
static ADAPTIVE_TOPK_ROWREF_EXACT: AtomicU64 = AtomicU64::new(0);

/// Group-level emit-side top-N boundary effect counters (lane-v2 topnemit;
/// informational `counter` dump lines). `SEEN` counts groups walked by an
/// armed retrieve (emitted + cut); `CUT` counts groups skipped ahead of
/// finalize/projection/sort-put.
static TOPNEMIT_GROUPS_SEEN: AtomicU64 = AtomicU64::new(0);
static TOPNEMIT_GROUPS_CUT: AtomicU64 = AtomicU64::new(0);
/// Batched compact finalize+emit effect counters (lane-v2 batchemit;
/// informational `counter` dump lines). `GROUPS` counts groups emitted
/// through the batched kernels (no per-group fmgr finalize / projection /
/// first_slot scatter); `FEEDS` counts armed feeds.
static BATCHEMIT_GROUPS: AtomicU64 = AtomicU64::new(0);
static BATCHEMIT_FEEDS: AtomicU64 = AtomicU64::new(0);
/// Topkfin (top-k group selection before finalize/emit; hot-c1) effect
/// counters (informational `counter` dump lines). `GROUPS` counts groups the
/// selection scan walked; `SELECTED` counts survivors finalized + emitted
/// (≤ the sort bound); `DEMOTED` counts armed passes that declined (no
/// compact table or a NULL order-key transvalue) back to the batched feed.
static TOPKFIN_GROUPS: AtomicU64 = AtomicU64::new(0);
static TOPKFIN_SELECTED: AtomicU64 = AtomicU64::new(0);
static TOPKFIN_DEMOTED: AtomicU64 = AtomicU64::new(0);
/// Refsort (late-materialization top-N) engagement counters (informational
/// `counter` dump lines): `OWNED` = feeds the refsort arm completed (narrow
/// puts + winner gather); `DEMOTED` = armed feeds that demoted back to the
/// legacy wide feed (mid-feed ref loss or a gather failure -- always before
/// any output escaped).
static REFSORT_OWNED: AtomicU64 = AtomicU64::new(0);
static REFSORT_DEMOTED: AtomicU64 = AtomicU64::new(0);
/// SE-HASHOFF census counters (informational `counter` dump lines; lane
/// deletion-prep arms #6/#7, notes/se-hashoff-letters.md): one tick per
/// `HashBuildInput::multi_exec` call (a hash-build event, rescans included)
/// classified at the fused-arm chokepoint in procnode.rs.
/// `ENGAGED_*` = the fused page-batch drive ran (bare / projected build
/// source); `PERTUPLE_SEQ` = a SeqScan build child fell to the per-tuple
/// feed (arm knob off or `hash_build_fusible`/batch-support refused);
/// `PERTUPLE_OTHER` = non-SeqScan build child (never arm surface).
static FUSED_HASH_BUILD_ENGAGED_BARE: AtomicU64 = AtomicU64::new(0);
static FUSED_HASH_BUILD_ENGAGED_PROJ: AtomicU64 = AtomicU64::new(0);
static FUSED_HASH_BUILD_PERTUPLE_SEQ: AtomicU64 = AtomicU64::new(0);
static FUSED_HASH_BUILD_PERTUPLE_OTHER: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// M2 inc-3 rung-2 — fallback-floor counters (m2-inc3-scope.md §5 rung 2):
// one counter per (runtime arm × engagement channel), so the rung-3/4
// demotion/deletion decisions read evidence, not vibes. Channels:
//   * `pooldb`   — the RG reached its outcome under the POOL-DB channel
//                  (bound-descriptor board; StandingWait::Done, pool phase);
//   * `gang`     — outcome under the STANDING GANG channel (Done, gang
//                  phase) — i.e. the pool phase refused/was off and the
//                  gang absorbed it: THE gang-fallback rate per arm;
//   * `launched` — RETIRED at M2 inc-3 rung 4 (the launched-bgworker
//                  ceremony is deleted from the runtime arms): never
//                  constructed anymore, ALWAYS 0. The row stays dumped
//                  until Phase-5 D5 — zeros-included is the witness
//                  contract (scripts/nolaunch-floors-witness.sh reads it
//                  as the "nothing launches" proof; absent≠zero would
//                  ambiguate a dropped row against the deletion).
//   * `serial`   — the whole parallel engagement fell back to the serial
//                  arm (EngageOutcome::Fallback at the arm's dispatch);
//   * `nolaunch-serial` — the board channels declined and the arm went
//                  STRAIGHT to serial (the rung-4 deletion's permanent
//                  ladder; formerly the PGRUST_RUNTIME_NOLAUNCH posture
//                  knob's row — the knob is inert since the deletion).
//                  Each tick is one engagement the deletion serialized
//                  (the R2-cliff watch, measured not vibed). Doubles with
//                  the arm-tail `serial` tick by design (serial stays the
//                  outcome census; this row is the cause attribution).
// Tick cadence: one per engagement OUTCOME (completions for the two board
// channels; path-taken for the serial rows) — engagement-grain, never
// per-row. Error exits tick nothing (they surface, they don't fall back).
// Informational `counter` dump lines (`engage-<arm>-<channel>`); the gate's
// floor/allowlist machinery ignores them until rung 3 pins gang≈0 floors.
// ---------------------------------------------------------------------------

/// The engagement-channel vocabulary for the fallback floors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum EngageChannel {
    PoolDb = 0,
    Gang = 1,
    /// Never constructed since the rung-4 launched-path deletion; the
    /// dump row stays (zeros-included witness contract) until Phase-5 D5.
    #[allow(dead_code)]
    Launched = 2,
    Serial = 3,
    NolaunchSerial = 4,
}

const N_ENGAGE_CHANNELS: usize = 5;
const ENGAGE_CHANNEL_NAMES: [&str; N_ENGAGE_CHANNELS] =
    ["pooldb", "gang", "launched", "serial", "nolaunch-serial"];

/// The arm vocabulary keys off the StandingArm label (already the stable
/// per-arm trace identity) so the channel code needs no new per-arm enum.
const ENGAGE_ARMS: [&str; 7] = [
    "runtime-scan",
    "runtime-agg",
    "runtime-agg-sorted",
    "runtime-sort",
    "runtime-hashjoin",
    "runtime-distinct",
    "runtime-plaindistinct",
];

#[allow(clippy::declare_interior_mutable_const)]
static ENGAGE: [[AtomicU64; N_ENGAGE_CHANNELS]; ENGAGE_ARMS.len()] =
    [const { [const { AtomicU64::new(0) }; N_ENGAGE_CHANNELS] }; ENGAGE_ARMS.len()];

/// Record one engagement on `channel` for the arm labeled `label` (the
/// StandingArm label / the arm's lane_trace prefix). Engagement-grain: the
/// linear label scan (7 entries) runs at most once per query engagement,
/// and only with accounting armed.
#[inline]
pub(super) fn tick_engaged(label: &str, channel: EngageChannel) {
    if !armed() {
        return;
    }
    if let Some(i) = ENGAGE_ARMS.iter().position(|a| *a == label) {
        ENGAGE[i][channel as usize].fetch_add(1, Relaxed);
        arm_dump_on_thread_exit();
    }
}
/// Serial-lease accounting witness (GL-SLEASE-1 origin, GL-SLEASE-2
/// semantics): top-level serial ExecutorRuns the lease machinery TRACKED —
/// v2 ticks at enter (slot published to the sweeper), not at permit
/// acquisition, which the admission floor makes workload-dependent (a
/// sub-floor run never touches the semaphore by design). Still the "the
/// scheduler sees serial load" witness row; the unarmed default
/// (GL-RO-BISECT-2 re-flip), `PGRUST_RUNTIME_SERIAL_LEASE=0`, or no runtime
/// must dump 0.
static SERIAL_LEASES: AtomicU64 = AtomicU64::new(0);

/// Record one tracked serial lease (armed-gated like every counter here).
/// pub(crate): re-exported at the lanev2 root for the slease engine.
#[inline]
pub(crate) fn tick_serial_lease() {
    if !armed() {
        return;
    }
    SERIAL_LEASES.fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
}

/// GL-SLEASE-2 mechanism witnesses: safe-point ADMISSIONS (a floor-crossed
/// run actually took a permit at the ProcessInterrupts tap) and DONATIONS
/// (a held permit released across a C-parity wait span). Zero-dumped like
/// the tracked row — an armed protective cell asserting admitted>0 is the
/// positive witness that the sweeper->interrupt->tap chain is alive, not
/// just the enter bookkeeping.
static SERIAL_LEASE_ADMITTED: AtomicU64 = AtomicU64::new(0);
static SERIAL_LEASE_DONATIONS: AtomicU64 = AtomicU64::new(0);
/// GL-SLEASE-3 admission census: runs the sweeper NEWLY flagged as
/// floor-crossed (Pending, first flag raise — Deficit re-flags don't
/// re-count; ticked by the sweeper thread). crossings vs admitted vs
/// donations is the attribution triple for the residual-tax ladder.
static SERIAL_LEASE_FLOOR_CROSSED: AtomicU64 = AtomicU64::new(0);

#[inline]
pub(crate) fn tick_serial_lease_admitted() {
    if !armed() {
        return;
    }
    SERIAL_LEASE_ADMITTED.fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
}

#[inline]
pub(crate) fn tick_serial_lease_donation() {
    if !armed() {
        return;
    }
    SERIAL_LEASE_DONATIONS.fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
}

#[inline]
pub(crate) fn tick_serial_lease_floor_crossing() {
    if !armed() {
        return;
    }
    SERIAL_LEASE_FLOOR_CROSSED.fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
}

/// GL-STMTTASK-2 accounting witnesses: armed statements engaged INLINE (a
/// borrowed pool seat, the session thread executing) vs ENQUEUED (a pool
/// worker served the dop-1 task). The protection scenario's stmt arm reads
/// these off the stats dir instead of trace lines.
static STMT_TASK_INLINE_N: AtomicU64 = AtomicU64::new(0);
static STMT_TASK_ENQUEUED_N: AtomicU64 = AtomicU64::new(0);
/// GL-STMTTASK-2 quantum-yield experiment: governor yields performed.
static STMT_TASK_YIELDS_N: AtomicU64 = AtomicU64::new(0);

#[inline]
pub(crate) fn tick_stmt_task_yield() {
    if !armed() {
        return;
    }
    STMT_TASK_YIELDS_N.fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
}

#[inline]
pub(crate) fn tick_stmt_task(inline: bool) {
    if !armed() {
        return;
    }
    if inline {
        STMT_TASK_INLINE_N.fetch_add(1, Relaxed);
    } else {
        STMT_TASK_ENQUEUED_N.fetch_add(1, Relaxed);
    }
    arm_dump_on_thread_exit();
}

#[allow(clippy::declare_interior_mutable_const)]
static REFUSED: [[AtomicU64; N_REASONS]; N_CLASSES] =
    [const { [const { AtomicU64::new(0) }; N_REASONS] }; N_CLASSES];

/// The accounting arm switch: `PGRUST_LANE_V2_STATS=<dir>`. Resolved once per
/// process, like `lanev2::enabled()`. Shared with the M5 router's counters
/// (router.rs) so one harness switch arms both surfaces.
pub(super) fn stats_dir() -> Option<&'static PathBuf> {
    static DIR: OnceLock<Option<PathBuf>> = OnceLock::new();
    DIR.get_or_init(|| std::env::var_os("PGRUST_LANE_V2_STATS").map(PathBuf::from))
        .as_ref()
}

/// The no-dump-dir arming env for the pgrust_lane_coverage view (WS-C,
/// single-executor Phase 0.2): `PGRUST_LANE_V2_COVERAGE=1` arms the ticks
/// without arming the TSV dump-on-exit (which still requires the dir).
/// Counters stay opt-in because the index/IOS/bitmap classes tick per pull;
/// always-on is a ledgered measurement (WS-C inc-2).
pub(super) fn coverage_armed() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        matches!(std::env::var("PGRUST_LANE_V2_COVERAGE").as_deref(), Ok("1") | Ok("on"))
    })
}

/// Record an OWNED decision for `class`. One cached load + branch when
/// accounting is disarmed (`armed()` memoizes the arm-switch disjunction).
#[inline]
pub(super) fn tick_owned(class: ShapeClass) {
    if !armed() {
        return;
    }
    OWNED[class as usize].fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
}

/// Record a REFUSED decision for `class` with its reason.
#[inline]
pub(super) fn tick_refused(class: ShapeClass, reason: RefuseReason) {
    if !armed() {
        return;
    }
    REFUSED[class as usize][reason as usize].fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
    // Layer-A assert-mode cold tail (WS-P inc-2; wave-2 contract §4: WS-P's
    // ONLY stats.rs edit). Reads the (class, reason) vocabulary, never adds
    // to it. Default config never reaches this line (the `armed()` early
    // return above fires first — refusal-path cost byte-identical); with
    // accounting armed it is one memoized-bool load + branch unless
    // PGRUST_LANE_V2_ASSERT_COVERED is set (diagnostics channels only), in
    // which case a manifest-covered class refusing an unallowed reason
    // raises `volcano-unreachable:` (census.rs module doc, OQ10).
    super::census::assert_covered_tail(class, reason);
}

/// Accounting armed (either the TSV dump dir or the coverage-view env)?
/// Callers may gate row-counting work (a per-batch popcount) on this before
/// calling `tick_topkcut_rows`.
///
/// MEMOIZED disjunction (se-entrycost): both arming sources are
/// process-static envs, and `tick_owned` sits on the select1/prepared hot
/// path (`try_own_result` ticks once per execution). Testing the two
/// OnceLocks separately here cost the entry-cost pair measurable
/// instructions per query; one cached bool restores the pre-coverage cost
/// (exactly one OnceLock fast path + branch).
#[inline]
pub(super) fn armed() -> bool {
    static ARMED: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ARMED, || stats_dir().is_some() || coverage_armed())
}

// ---------------------------------------------------------------------------
// Coverage-view snapshot accessors (lanev2/coverage.rs): relaxed loads of the
// process-cumulative counters, enumerated from the vocabulary tables above —
// the classifier source of truth, never a hand-written list. Counts derive
// from N_CLASSES/N_REASONS (integration contract R-VOCAB: no literals).
// ---------------------------------------------------------------------------

pub(super) const fn n_classes() -> usize {
    N_CLASSES
}

#[cfg(test)]
pub(super) const fn n_reasons() -> usize {
    N_REASONS
}

/// Every refusal-reason display name, by index (the full frozen vocabulary;
/// the derived-count tests' enumeration source).
#[cfg(test)]
pub(super) fn reason_names() -> Vec<&'static str> {
    (0..N_REASONS).map(|i| RefuseReason::from_index(i).name()).collect()
}

/// Every (class, owned-count) cell, zeros included.
pub(super) fn owned_snapshot() -> Vec<(ShapeClass, u64)> {
    ShapeClass::ALL
        .iter()
        .map(|&c| (c, OWNED[c as usize].load(Relaxed)))
        .collect()
}

/// Every nonzero (class, reason, count) refusal cell.
pub(super) fn refused_snapshot() -> Vec<(ShapeClass, RefuseReason, u64)> {
    let mut v = Vec::new();
    for class in ShapeClass::ALL {
        for (i, cell) in REFUSED[class as usize].iter().enumerate() {
            let n = cell.load(Relaxed);
            if n > 0 {
                v.push((class, RefuseReason::from_index(i), n));
            }
        }
    }
    v
}

/// Record one zone-adaptive top-N demotion (ambiguous boundary tie →
/// physical re-feed).
#[inline]
pub(super) fn tick_adaptive_topk_demoted() {
    if stats_dir().is_none() {
        return;
    }
    ADAPTIVE_TOPK_DEMOTED.fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
}

/// Record one accepted retained-tie-order relaxation (relaxed adaptive
/// top-N feed finished with an order-only boundary-tie verdict; ratified
/// tie-ordering rule 3 — no demotion, selection exact).
#[inline]
pub(super) fn tick_adaptive_topk_tie_relaxed() {
    if stats_dir().is_none() {
        return;
    }
    ADAPTIVE_TOPK_TIE_RELAXED.fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
}

/// Record one completed refsort (late-materialization top-N) feed.
#[inline]
pub(super) fn tick_refsort_owned() {
    if stats_dir().is_none() {
        return;
    }
    REFSORT_OWNED.fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
}

/// Record one refsort demotion (armed feed fell back to the legacy wide
/// feed before any output escaped).
#[inline]
pub(super) fn tick_refsort_demoted() {
    if stats_dir().is_none() {
        return;
    }
    REFSORT_DEMOTED.fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
}

/// SE-HASHOFF census tick (deletion-prep arms #6/#7): one hash-build event
/// classified at the fused-arm chokepoint. `engaged` = the fused page-batch
/// drive runs this build; `proj` = projected build source (arm
/// HASH_BUILD_PROJ) vs bare (arm HASH_BUILD). `engaged=false` = a SeqScan
/// build child taking the per-tuple feed.
#[inline]
pub(super) fn tick_fused_hash_build_seq(engaged: bool, proj: bool) {
    if stats_dir().is_none() {
        return;
    }
    match (engaged, proj) {
        (true, false) => FUSED_HASH_BUILD_ENGAGED_BARE.fetch_add(1, Relaxed),
        (true, true) => FUSED_HASH_BUILD_ENGAGED_PROJ.fetch_add(1, Relaxed),
        (false, _) => FUSED_HASH_BUILD_PERTUPLE_SEQ.fetch_add(1, Relaxed),
    };
    arm_dump_on_thread_exit();
}

/// SE-HASHOFF census tick: a hash-build event over a non-SeqScan build
/// child (outside both fused hash-build arms' surface by construction).
#[inline]
pub(super) fn tick_fused_hash_build_other() {
    if stats_dir().is_none() {
        return;
    }
    FUSED_HASH_BUILD_PERTUPLE_OTHER.fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
}

/// Record one rule-2 rowref-selection adaptive top-N feed that finished
/// exact (no demotion; survivor selection pinned to the physical feed's by
/// the (key, rowref) bounded-heap total order).
#[inline]
pub(super) fn tick_adaptive_topk_rowref_exact() {
    if stats_dir().is_none() {
        return;
    }
    ADAPTIVE_TOPK_ROWREF_EXACT.fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
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

/// Record one armed emit-side top-N feed: `seen` groups walked (emitted +
/// cut), `cut` groups skipped ahead of finalize/projection/sort-put.
#[inline]
pub(super) fn tick_topnemit_groups(seen: u64, cut: u64) {
    if stats_dir().is_none() {
        return;
    }
    TOPNEMIT_GROUPS_SEEN.fetch_add(seen, Relaxed);
    TOPNEMIT_GROUPS_CUT.fetch_add(cut, Relaxed);
    arm_dump_on_thread_exit();
}

/// Record one armed batched compact finalize+emit feed: `groups` emitted
/// through the batched kernels, `feeds` armed feed events (always 1).
#[inline]
pub(super) fn tick_batchemit_groups(groups: u64, feeds: u64) {
    if stats_dir().is_none() {
        return;
    }
    BATCHEMIT_GROUPS.fetch_add(groups, Relaxed);
    BATCHEMIT_FEEDS.fetch_add(feeds, Relaxed);
    arm_dump_on_thread_exit();
}

/// Record one owned topkfin feed: `groups` walked by the selection scan,
/// `selected` survivors finalized + emitted.
#[inline]
pub(super) fn tick_topkfin_groups(groups: u64, selected: u64) {
    if stats_dir().is_none() {
        return;
    }
    TOPKFIN_GROUPS.fetch_add(groups, Relaxed);
    TOPKFIN_SELECTED.fetch_add(selected, Relaxed);
    arm_dump_on_thread_exit();
}

/// Record one topkfin decline (armed pass fell back to the batched feed
/// before any side effect).
#[inline]
pub(super) fn tick_topkfin_demoted() {
    if stats_dir().is_none() {
        return;
    }
    TOPKFIN_DEMOTED.fetch_add(1, Relaxed);
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
    out.push_str(&format!(
        "counter\tadaptivetopk-demoted\t{}\n",
        ADAPTIVE_TOPK_DEMOTED.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\tadaptivetopk-tie-relaxed\t{}\n",
        ADAPTIVE_TOPK_TIE_RELAXED.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\tadaptivetopk-rowref-exact\t{}\n",
        ADAPTIVE_TOPK_ROWREF_EXACT.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\ttopnemit-groups-seen\t{}\n",
        TOPNEMIT_GROUPS_SEEN.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\ttopnemit-groups-cut\t{}\n",
        TOPNEMIT_GROUPS_CUT.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\tbatchemit-groups\t{}\n",
        BATCHEMIT_GROUPS.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\tbatchemit-feeds\t{}\n",
        BATCHEMIT_FEEDS.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\ttopkfin-groups\t{}\n",
        TOPKFIN_GROUPS.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\ttopkfin-selected\t{}\n",
        TOPKFIN_SELECTED.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\ttopkfin-demoted\t{}\n",
        TOPKFIN_DEMOTED.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\trefsort-owned\t{}\n",
        REFSORT_OWNED.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\trefsort-demoted\t{}\n",
        REFSORT_DEMOTED.load(Relaxed)
    ));
    // SE-HASHOFF census rows (deletion-prep arms #6/#7).
    out.push_str(&format!(
        "counter\tfused-hash-build-engaged-bare\t{}\n",
        FUSED_HASH_BUILD_ENGAGED_BARE.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\tfused-hash-build-engaged-proj\t{}\n",
        FUSED_HASH_BUILD_ENGAGED_PROJ.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\tfused-hash-build-pertuple-seq\t{}\n",
        FUSED_HASH_BUILD_PERTUPLE_SEQ.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\tfused-hash-build-pertuple-other\t{}\n",
        FUSED_HASH_BUILD_PERTUPLE_OTHER.load(Relaxed)
    ));
    // Serial-lease witness row (zero included — the OFF arms assert 0, so
    // absent≠zero would ambiguate). Renamed acquires->tracked at GL-SLEASE-2
    // with the tick's move to enter (see tick_serial_lease).
    out.push_str(&format!(
        "counter\tserial-lease-tracked\t{}\n",
        SERIAL_LEASES.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\tserial-lease-admitted\t{}\n",
        SERIAL_LEASE_ADMITTED.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\tserial-lease-donations\t{}\n",
        SERIAL_LEASE_DONATIONS.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\tserial-lease-floor-crossings\t{}\n",
        SERIAL_LEASE_FLOOR_CROSSED.load(Relaxed)
    ));
    // GL-STMTTASK-2 witness rows (zeros included, same absent!=zero law).
    out.push_str(&format!(
        "counter\tstmt-task-inline\t{}\n",
        STMT_TASK_INLINE_N.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\tstmt-task-enqueued\t{}\n",
        STMT_TASK_ENQUEUED_N.load(Relaxed)
    ));
    out.push_str(&format!(
        "counter\tstmt-task-yields\t{}\n",
        STMT_TASK_YIELDS_N.load(Relaxed)
    ));
    // M2 inc-3 rung-2 fallback-floor rows (engagement channels per arm;
    // zeros included — the floor reader diffs runs, absent≠zero would
    // ambiguate a never-engaged arm against a dropped row).
    for (i, arm) in ENGAGE_ARMS.iter().enumerate() {
        for (c, ch) in ENGAGE_CHANNEL_NAMES.iter().enumerate() {
            let short = arm.strip_prefix("runtime-").unwrap_or(arm);
            out.push_str(&format!(
                "counter\tengage-{short}-{ch}\t{}\n",
                ENGAGE[i][c].load(Relaxed)
            ));
        }
    }
    // --- WS-CB wave-10 (cursors inc-2 §6 staging; worklog EX-CB-2): the
    // run-seam backward-drive evidence counter — the post-flip deletion
    // bake reads this at zero across all corpora. Static lives in push.rs
    // (the WS-CB grant surface); this is the dump row only.
    out.push_str(&format!(
        "counter\trun-seam-backward\t{}\n",
        super::run_seam_backward_evidence_count()
    ));
    // --- end WS-CB wave-10 ---
    let pid = init_small::globals::process_id();
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
