//! WS-P flip machinery (single-executor wave 2): the per-statement plan-node
//! census (inc-1) and the Layer-A assert-covered tail (inc-2).
//!
//! THE INSTRUMENT (wave-2 contract §5 stage-0a): "no wave-2 flip talk without
//! its dashboard". The census answers, per corpus statement, *what fraction
//! of executed plan nodes the lane dispatched* — the number every default-flip
//! rung in docs/design/flip-ladder.md gates on. It is a diagnostics channel:
//! `PGRUST_LANE_V2_NODE_CENSUS=<dir>` arms it (default OFF — disarmed cost is
//! one memoized-bool branch at executor start/end, nothing else); official
//! benchmark channels NEVER carry the env (contract OQ8, perf-channel purity).
//!
//! Mechanism: when armed, ExecutorStart ORs `EXEC_FLAG_ENGINE_REPORT` into
//! eflags, so WS-C's per-node EngineEvent capture (the EXPLAIN (ENGINE)
//! machinery, emission-gate law in executils) records verdicts during the
//! run. At ExecutorEnd the census walks the PLAN tree (the same edges
//! EXPLAIN's walker uses: lefttree/righttree + the Append/MergeAppend/
//! BitmapAnd/BitmapOr member lists + the SubqueryScan subplan +
//! PlannedStmt.subplans), joins each node to its engine attribution by
//! plan_node_id, and appends one TSV row per plan node:
//!
//!   schema_version \t epoch \t query_hash \t plan_node_id \t kind \t engine \t class \t detail
//!
//!   * schema_version — "1" (contract §6 WS-P amendment 1: the column exists
//!     from row one so the aggregator can evolve the schema).
//!   * epoch — process-global execution counter (one per censused execution;
//!     rows of one execution share it). Uniqueness across server processes /
//!     resume segments rides the per-pid filename (OQ2: per-process epoch +
//!     pid file suffix is the operative uniqueness story; revisit if a
//!     harness needs cross-process ordering).
//!   * query_hash — FNV-1a 64 of the statement source text (es_sourceText,
//!     this port's debug_query_string carrier; OQ7).
//!   * kind — the census node vocabulary (plan-node tag name, lowercase);
//!     raw TSV keeps ALL nodes — the denominator-exclusion list
//!     (Gather/GatherMerge, BitmapIndexScan/BitmapAnd/BitmapOr, ForeignScan)
//!     lives ONLY in the aggregator (scripts/coverage/corpus-coverage.py).
//!   * engine — lane | runtime | fused-arm | spine | none (none = no verdict
//!     event recorded for the node: the spine ran it without ever offering
//!     it to a lane hook, or the chokepoint lacks a capture call — see
//!     KNOWN LIMITS).
//!   * class / detail — the EngineEvent's ShapeClass name and refusal reason
//!     ("" for owned / none).
//!
//! KNOWN LIMITS (honesty ledger, notes/se-ws-p-flip.md): census fidelity is
//! bounded by WS-C's capture breadth (D3 — chokepoints without engine_record
//! calls report "none" even when a lane owned the node), so every flip rung
//! carries a capture-breadth precondition; initially-pruned Append children
//! are counted from the plan tree (they never execute — they surface as
//! "none" and deflate partitioned-corpus coverage; ledgered refinement);
//! parallel-worker executions are skipped (leader-only rows — worker
//! subtree re-executions must not double-count the statement's nodes).
//!
//! OQ1 (compile-break safety): `kind_of_planstate` is an EXHAUSTIVE match
//! over `procnode::PlanStateNode` — adding an executor node variant breaks
//! THIS file's compilation until the census vocabulary admits it. The
//! plan-tree classifier (`kind_of_tag`) cannot be exhaustive (NodeTag spans
//! every parse/plan/executor tag), so the walk cross-checks the root against
//! `kind_of_planstate` on every censused execution and unknown tags emit a
//! LOUD "unknown" kind instead of erring (census must never turn into a
//! query error). The unit corpus cross-checks walker counts against the
//! init_plan/EXPLAIN traversal universe (es_instrumentation slots, keyed by
//! plan_node_id — the same ids EXPLAIN's walker visits).
//!
//! Layer-A (inc-2): `PGRUST_LANE_V2_ASSERT_COVERED=1` arms the assert-mode
//! tail stats.rs's `tick_refused` calls (the ONE stats.rs edit the contract
//! grants WS-P). A refusal of a class the flip manifest declares covered —
//! with a reason the manifest row does not allow — panics with the
//! `volcano-unreachable:` prefix; the backend's statement-boundary
//! catch_unwind converts that to a PgError ERROR (default sqlstate XX000),
//! which dualexec classifies by prefix (OQ10). The compiled-in manifest
//! below is EMPTY by design: rows land only in flip commits (same commit as
//! the floor reseed, both gate profiles — the WS-P law), so assert mode is
//! inert everywhere today. ARMING COMPOSITION: the tail sits behind
//! `stats::armed()` (the tick is the trigger), so assert runs require the
//! accounting armed too (`PGRUST_LANE_V2_COVERAGE=1` or a stats dir) — a
//! deliberate composition that keeps the default-config refusal path
//! byte-identical in cost; the e2e sets both.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock};

use ::executils::{EStateData, EngineKind};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::{Plan, PlannedStmt};
use ::types_nodes::NodeTag;

use super::stats::{RefuseReason, ShapeClass};
use crate::procnode::PlanStateNode;

// ---------------------------------------------------------------------------
// Knobs (R-KNOBS wave-2 registry, both default OFF)
// ---------------------------------------------------------------------------

/// `PGRUST_LANE_V2_NODE_CENSUS=<dir>` — arms the census and names the TSV
/// output directory. Resolved once per process (the stats.rs `stats_dir`
/// idiom); env-var, not GUC, per the standing `pg_settings` byte-identity
/// discipline (lanev2 module doc).
fn census_dir() -> Option<&'static PathBuf> {
    static DIR: OnceLock<Option<PathBuf>> = OnceLock::new();
    DIR.get_or_init(|| std::env::var_os("PGRUST_LANE_V2_NODE_CENSUS").map(PathBuf::from))
        .as_ref()
}

/// Census armed? One relaxed byte load + compare — the executor entry/exit
/// hooks gate on this (se-entrycost discipline; flagged for the select1
/// instruction-pair fleet gate in notes/se-ws-p-flip.md).
///
/// se2-cost-fix: the rowmode.rs AtomicU8 tri-state idiom instead of
/// `OnceLock<bool>` — the OnceLock fast path is an acquire state load PLUS
/// the value load; this is one relaxed load (the env is process-static, so
/// no ordering is needed), and the resolve path is `#[cold]`-outlined so
/// the two per-query hook sites carry no inline slow-path code. Gate: the
/// knob-OFF select1/prepared pair letter (se-wave2-integration.md leg 4a).
#[inline]
pub(crate) fn census_armed() -> bool {
    // 0 = unresolved, 1 = disarmed, 2 = armed.
    static ON: AtomicU8 = AtomicU8::new(0);
    match ON.load(Relaxed) {
        1 => false,
        2 => true,
        _ => {
            #[cold]
            #[inline(never)]
            fn resolve(cell: &AtomicU8) -> bool {
                let on = census_dir().is_some();
                cell.store(if on { 2 } else { 1 }, Relaxed);
                on
            }
            resolve(&ON)
        }
    }
}

/// `PGRUST_LANE_V2_ASSERT_COVERED=1` — Layer-A assert mode (inc-2). Never a
/// default; dualexec/e2e channels only.
fn assert_covered_armed() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        matches!(
            std::env::var("PGRUST_LANE_V2_ASSERT_COVERED").as_deref(),
            Ok("1") | Ok("on")
        )
    })
}

// ---------------------------------------------------------------------------
// Census node vocabulary
// ---------------------------------------------------------------------------

/// The census's own node-kind vocabulary — one name per PLAN node type the
/// executor can host. Deliberately NOT `stats::ShapeClass` (that is the
/// lane-engagement vocabulary, WS-L-owned in wave 2); the aggregator joins
/// the two through the per-row `class` column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum NodeKind {
    Result,
    ProjectSet,
    ModifyTable,
    Append,
    MergeAppend,
    RecursiveUnion,
    BitmapAnd,
    BitmapOr,
    SeqScan,
    SampleScan,
    IndexScan,
    IndexOnlyScan,
    BitmapIndexScan,
    BitmapHeapScan,
    TidScan,
    TidRangeScan,
    SubqueryScan,
    FunctionScan,
    ValuesScan,
    TableFuncScan,
    CteScan,
    NamedTuplestoreScan,
    WorkTableScan,
    ForeignScan,
    CustomScan,
    NestLoop,
    MergeJoin,
    HashJoin,
    Material,
    Memoize,
    Sort,
    IncrementalSort,
    Group,
    Agg,
    WindowAgg,
    Unique,
    Gather,
    GatherMerge,
    Hash,
    SetOp,
    LockRows,
    Limit,
}

impl NodeKind {
    pub(super) fn name(self) -> &'static str {
        match self {
            NodeKind::Result => "result",
            NodeKind::ProjectSet => "projectset",
            NodeKind::ModifyTable => "modifytable",
            NodeKind::Append => "append",
            NodeKind::MergeAppend => "mergeappend",
            NodeKind::RecursiveUnion => "recursiveunion",
            NodeKind::BitmapAnd => "bitmapand",
            NodeKind::BitmapOr => "bitmapor",
            NodeKind::SeqScan => "seqscan",
            NodeKind::SampleScan => "samplescan",
            NodeKind::IndexScan => "indexscan",
            NodeKind::IndexOnlyScan => "indexonlyscan",
            NodeKind::BitmapIndexScan => "bitmapindexscan",
            NodeKind::BitmapHeapScan => "bitmapheapscan",
            NodeKind::TidScan => "tidscan",
            NodeKind::TidRangeScan => "tidrangescan",
            NodeKind::SubqueryScan => "subqueryscan",
            NodeKind::FunctionScan => "functionscan",
            NodeKind::ValuesScan => "valuesscan",
            NodeKind::TableFuncScan => "tablefuncscan",
            NodeKind::CteScan => "ctescan",
            NodeKind::NamedTuplestoreScan => "namedtuplestorescan",
            NodeKind::WorkTableScan => "worktablescan",
            NodeKind::ForeignScan => "foreignscan",
            NodeKind::CustomScan => "customscan",
            NodeKind::NestLoop => "nestloop",
            NodeKind::MergeJoin => "mergejoin",
            NodeKind::HashJoin => "hashjoin",
            NodeKind::Material => "material",
            NodeKind::Memoize => "memoize",
            NodeKind::Sort => "sort",
            NodeKind::IncrementalSort => "incrementalsort",
            NodeKind::Group => "group",
            NodeKind::Agg => "agg",
            NodeKind::WindowAgg => "windowagg",
            NodeKind::Unique => "unique",
            NodeKind::Gather => "gather",
            NodeKind::GatherMerge => "gathermerge",
            NodeKind::Hash => "hash",
            NodeKind::SetOp => "setop",
            NodeKind::LockRows => "lockrows",
            NodeKind::Limit => "limit",
        }
    }
}

/// Classify a plan-tree node tag. `None` = a tag outside the plan-node band
/// (walked trees should never produce one; the walker emits a loud
/// "unknown" row rather than erring — census is best-effort by law).
pub(super) fn kind_of_tag(tag: NodeTag) -> Option<NodeKind> {
    Some(match tag {
        NodeTag::T_Result => NodeKind::Result,
        NodeTag::T_ProjectSet => NodeKind::ProjectSet,
        NodeTag::T_ModifyTable => NodeKind::ModifyTable,
        NodeTag::T_Append => NodeKind::Append,
        NodeTag::T_MergeAppend => NodeKind::MergeAppend,
        NodeTag::T_RecursiveUnion => NodeKind::RecursiveUnion,
        NodeTag::T_BitmapAnd => NodeKind::BitmapAnd,
        NodeTag::T_BitmapOr => NodeKind::BitmapOr,
        NodeTag::T_SeqScan => NodeKind::SeqScan,
        NodeTag::T_SampleScan => NodeKind::SampleScan,
        NodeTag::T_IndexScan => NodeKind::IndexScan,
        NodeTag::T_IndexOnlyScan => NodeKind::IndexOnlyScan,
        NodeTag::T_BitmapIndexScan => NodeKind::BitmapIndexScan,
        NodeTag::T_BitmapHeapScan => NodeKind::BitmapHeapScan,
        NodeTag::T_TidScan => NodeKind::TidScan,
        NodeTag::T_TidRangeScan => NodeKind::TidRangeScan,
        NodeTag::T_SubqueryScan => NodeKind::SubqueryScan,
        NodeTag::T_FunctionScan => NodeKind::FunctionScan,
        NodeTag::T_ValuesScan => NodeKind::ValuesScan,
        NodeTag::T_TableFuncScan => NodeKind::TableFuncScan,
        NodeTag::T_CteScan => NodeKind::CteScan,
        NodeTag::T_NamedTuplestoreScan => NodeKind::NamedTuplestoreScan,
        NodeTag::T_WorkTableScan => NodeKind::WorkTableScan,
        NodeTag::T_ForeignScan => NodeKind::ForeignScan,
        NodeTag::T_CustomScan => NodeKind::CustomScan,
        NodeTag::T_NestLoop => NodeKind::NestLoop,
        NodeTag::T_MergeJoin => NodeKind::MergeJoin,
        NodeTag::T_HashJoin => NodeKind::HashJoin,
        NodeTag::T_Material => NodeKind::Material,
        NodeTag::T_Memoize => NodeKind::Memoize,
        NodeTag::T_Sort => NodeKind::Sort,
        NodeTag::T_IncrementalSort => NodeKind::IncrementalSort,
        NodeTag::T_Group => NodeKind::Group,
        NodeTag::T_Agg => NodeKind::Agg,
        NodeTag::T_WindowAgg => NodeKind::WindowAgg,
        NodeTag::T_Unique => NodeKind::Unique,
        NodeTag::T_Gather => NodeKind::Gather,
        NodeTag::T_GatherMerge => NodeKind::GatherMerge,
        NodeTag::T_Hash => NodeKind::Hash,
        NodeTag::T_SetOp => NodeKind::SetOp,
        NodeTag::T_LockRows => NodeKind::LockRows,
        NodeTag::T_Limit => NodeKind::Limit,
        _ => return None,
    })
}

/// OQ1 compile-break safety: EXHAUSTIVE over the executor's node-variant
/// set — a new `PlanStateNode` variant fails to compile HERE until the
/// census vocabulary admits it. Called on the root of every censused
/// execution (the plan-vs-planstate root cross-check in
/// `record_execution`), so the mapping is live product code, not a
/// test-only artifact. `Instrumented` is transparent (a dispatch wrapper,
/// not a plan node).
pub(super) fn kind_of_planstate(node: &PlanStateNode<'_>) -> NodeKind {
    match node {
        PlanStateNode::Result(_) => NodeKind::Result,
        PlanStateNode::ProjectSet(_) => NodeKind::ProjectSet,
        PlanStateNode::SeqScan(_) => NodeKind::SeqScan,
        PlanStateNode::SampleScan(_) => NodeKind::SampleScan,
        PlanStateNode::FunctionScan(_) => NodeKind::FunctionScan,
        PlanStateNode::ValuesScan(_) => NodeKind::ValuesScan,
        PlanStateNode::TableFuncScan(_) => NodeKind::TableFuncScan,
        PlanStateNode::CteScan(_) => NodeKind::CteScan,
        PlanStateNode::IndexScan(_) => NodeKind::IndexScan,
        PlanStateNode::TidScan(_) => NodeKind::TidScan,
        PlanStateNode::TidRangeScan(_) => NodeKind::TidRangeScan,
        PlanStateNode::IndexOnlyScan(_) => NodeKind::IndexOnlyScan,
        PlanStateNode::Agg(_) => NodeKind::Agg,
        PlanStateNode::Sort(_) => NodeKind::Sort,
        PlanStateNode::IncrementalSort(_) => NodeKind::IncrementalSort,
        PlanStateNode::Material(_) => NodeKind::Material,
        PlanStateNode::Unique(_) => NodeKind::Unique,
        PlanStateNode::Group(_) => NodeKind::Group,
        PlanStateNode::Limit(_) => NodeKind::Limit,
        PlanStateNode::LockRows(_) => NodeKind::LockRows,
        PlanStateNode::BitmapHeapScan(_) => NodeKind::BitmapHeapScan,
        PlanStateNode::BitmapIndexScan(_) => NodeKind::BitmapIndexScan,
        PlanStateNode::BitmapAnd(_) => NodeKind::BitmapAnd,
        PlanStateNode::BitmapOr(_) => NodeKind::BitmapOr,
        PlanStateNode::ModifyTable(_) => NodeKind::ModifyTable,
        PlanStateNode::NestLoop(_) => NodeKind::NestLoop,
        PlanStateNode::HashJoin(_) => NodeKind::HashJoin,
        PlanStateNode::MergeJoin(_) => NodeKind::MergeJoin,
        PlanStateNode::WindowAgg(_) => NodeKind::WindowAgg,
        PlanStateNode::Append(_) => NodeKind::Append,
        PlanStateNode::MergeAppend(_) => NodeKind::MergeAppend,
        PlanStateNode::SubqueryScan(_) => NodeKind::SubqueryScan,
        PlanStateNode::SetOp(_) => NodeKind::SetOp,
        PlanStateNode::Memoize(_) => NodeKind::Memoize,
        PlanStateNode::RecursiveUnion(_) => NodeKind::RecursiveUnion,
        PlanStateNode::WorkTableScan(_) => NodeKind::WorkTableScan,
        PlanStateNode::NamedTuplestoreScan(_) => NodeKind::NamedTuplestoreScan,
        PlanStateNode::Gather(_) => NodeKind::Gather,
        PlanStateNode::GatherMerge(_) => NodeKind::GatherMerge,
        PlanStateNode::ForeignScan(_) => NodeKind::ForeignScan,
        PlanStateNode::Instrumented(w) => kind_of_planstate(&w.inner),
    }
}

// ---------------------------------------------------------------------------
// Plan-tree walk (EXPLAIN's edges: ExplainPreScanNode, explain/src/node.rs)
// ---------------------------------------------------------------------------

/// Visit every plan node reachable from `node`, EXPLAIN-walker edges:
/// per-tag member lists + the generic lefttree/righttree. SubPlan trees are
/// NOT reached from here (the caller enumerates `PlannedStmt.subplans`
/// directly — complete and duplicate-free, unlike chasing per-expression
/// SubPlan references). Unknown vocabulary emits `None` kind, no recursion.
pub(super) fn walk_plan<'mcx>(
    node: Node<'mcx>,
    f: &mut impl FnMut(Option<NodeKind>, Option<&'mcx Plan<'mcx>>),
) {
    let tag = node.node_tag();
    let kind = kind_of_tag(tag);
    let Some(plan) = node.as_plan() else {
        // Not plan vocabulary at all: one loud row, nothing to recurse.
        f(kind, None);
        return;
    };
    f(kind, Some(plan));
    match tag {
        NodeTag::T_Append => {
            if let Some(a) = node.as_append() {
                for child in &a.appendplans {
                    walk_plan(child, f);
                }
            }
        }
        NodeTag::T_MergeAppend => {
            if let Some(m) = node.as_merge_append() {
                for child in &m.mergeplans {
                    walk_plan(child, f);
                }
            }
        }
        NodeTag::T_BitmapAnd => {
            if let Some(b) = node.as_bitmap_and() {
                for child in &b.bitmapplans {
                    walk_plan(child, f);
                }
            }
        }
        NodeTag::T_BitmapOr => {
            if let Some(b) = node.as_bitmap_or() {
                for child in &b.bitmapplans {
                    walk_plan(child, f);
                }
            }
        }
        NodeTag::T_SubqueryScan => {
            if let Some(sq) = node.as_subquery_scan() {
                if let Some(sub) = sq.subplan {
                    walk_plan(sub, f);
                }
            }
        }
        _ => {}
    }
    if let Some(l) = plan.lefttree {
        walk_plan(l, f);
    }
    if let Some(r) = plan.righttree {
        walk_plan(r, f);
    }
}

// ---------------------------------------------------------------------------
// Engine attribution (join with WS-C's EngineEvents by plan_node_id)
// ---------------------------------------------------------------------------

/// The per-node engine verdict: scan the execution's EngineEvents for this
/// plan_node_id and pick the strongest claim (a node can carry one event
/// per ShapeClass — e.g. a composed agg-over-scan records under both
/// classes). Priority: an OWNED lane claim wins over a runtime claim wins
/// over fused-arm wins over a bare spine refusal; no event = "none" (never
/// offered, or the chokepoint lacks capture — module-doc KNOWN LIMITS).
pub(super) fn attribution(
    estate: &EStateData<'_>,
    plan_node_id: i32,
) -> (&'static str, &'static str, &'static str) {
    let mut best: Option<(u8, &'static str, &'static str, &'static str)> = None;
    for e in estate.es_engine_events.iter() {
        if e.plan_node_id != plan_node_id {
            continue;
        }
        let (rank, name) = match e.engine {
            EngineKind::Lane => (3u8, "lane"),
            EngineKind::Runtime => (2, "runtime"),
            EngineKind::FusedArm => (1, "fused-arm"),
            EngineKind::Spine => (0, "spine"),
        };
        if best.map(|(r, ..)| rank > r).unwrap_or(true) {
            best = Some((rank, name, e.class, e.detail));
        }
    }
    match best {
        Some((_, name, class, detail)) => (name, class, detail),
        None => ("none", "", ""),
    }
}

// ---------------------------------------------------------------------------
// Recording (the ExecutorEnd hook body)
// ---------------------------------------------------------------------------

/// Process-global execution epoch: one per censused execution; all of an
/// execution's rows share it.
static EPOCH: AtomicU64 = AtomicU64::new(0);

fn fnv64(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Record one execution's census rows (the standard_executor_end hook;
/// caller gates on `census_armed()`). Best-effort by law: never a query
/// error, never a panic. Skips EXPLAIN-only executions (nothing dispatched)
/// and parallel-worker executions (the leader's rows are the statement).
#[cold]
pub(crate) fn record_execution(
    pstmt: &PlannedStmt<'_>,
    estate: &EStateData<'_>,
    planstate: Option<&PlanStateNode<'_>>,
) {
    let Some(dir) = census_dir() else { return };
    if estate.es_top_eflags & ::types_slot::EXEC_FLAG_EXPLAIN_ONLY != 0 {
        return;
    }
    if ::parallel::IsParallelWorker() {
        return;
    }
    let Some(root) = pstmt.planTree else { return };
    // OQ1 live cross-check: the executed tree's root and the plan tree's
    // root must classify identically (the exhaustive planstate match is the
    // compile-break net; this pins it to the walked vocabulary at runtime).
    let ps_kind = planstate.map(kind_of_planstate);
    if let (Some(psk), Some(ptk)) = (ps_kind, kind_of_tag(root.node_tag())) {
        debug_assert_eq!(
            psk.name(),
            ptk.name(),
            "census root classification diverged (planstate vs plan tree)"
        );
    }
    let epoch = EPOCH.fetch_add(1, Relaxed);
    let qhash = fnv64(estate.es_sourceText.unwrap_or(""));
    let mut out = String::new();
    {
        let mut emit = |kind: Option<NodeKind>, plan: Option<&Plan<'_>>| {
            let id = plan.map(|p| p.plan_node_id).unwrap_or(-1);
            let kind_name = kind.map(NodeKind::name).unwrap_or("unknown");
            let (engine, class, detail) = attribution(estate, id);
            out.push_str(&format!(
                "1\t{epoch}\t{qhash:016x}\t{id}\t{kind_name}\t{engine}\t{class}\t{detail}\n"
            ));
        };
        walk_plan(root, &mut emit);
        for cell in pstmt.subplans.iter() {
            if let Some(sub) = cell {
                walk_plan(sub, &mut emit);
            }
        }
    }
    append_rows(dir, &out);
}

/// Append rows to `<dir>/lane-v2-census.<pid>.tsv` (one file per server
/// process; append-only so an execution never clobbers another's rows;
/// serialized so interleaved executions never split a line). Best-effort:
/// I/O failure loses census rows, never fails the query.
fn append_rows(dir: &PathBuf, rows: &str) {
    use std::io::Write;
    static WRITE_LOCK: Mutex<()> = Mutex::new(());
    let _guard = match WRITE_LOCK.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let _ = std::fs::create_dir_all(dir);
    let path = dir.join(format!("lane-v2-census.{}.tsv", std::process::id()));
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
        let _ = f.write_all(rows.as_bytes());
    }
}

// ---------------------------------------------------------------------------
// Layer A (inc-2): the flip manifest + the assert-covered tail
// ---------------------------------------------------------------------------

/// The compiled-in flip manifest: one row per DEFAULT-FLIPPED ShapeClass,
/// with the refusal reasons that stay LEGAL after the flip (the dynamic
/// gates a covered class may still refuse on — EPQ, backward, instrumented,
/// …; "reads reasons, never adds them", contract §1). Each flip commit adds
/// its row(s) HERE and mirrors them in crates/backend/executor/execmain/src/lanev2/flip-manifest.tsv
/// (same commit as the floor reseed — the WS-P law; the
/// `manifest_mirror_in_sync` unit pins the two together).
///
/// Wave-4 rows (docs/design/flip-ladder.md §3 rungs 1-3; the binding wave-4
/// flip manifest):
///   * FLIP-1 (rung 1, `PGRUST_LANE_V2_ROWMODE` default-on): projectset +
///     the 16 row-mode tail delegation classes. Allowed reasons = the
///     dynamic host-template gates (epq/backward/instrumented + the frozen
///     scroll-mark spelling per the manifest row) plus, per class, the
///     EXISTING class-specific reasons only: projectset keeps its
///     structural non-childless-Result refuse (child-not-lane-owned) and
///     its explicit-OFF wholesale refuse (srf-set-expansion — the permanent
///     `=0` arm must stay assert-clean on diagnostics channels); the six
///     T3-hostable shapes keep env-off (the per-shape `_SCANS_T3_<SHAPE>=0`
///     force-off spelling, observed on the allowlist).
///   * FLIP-2 (rung 2, `PGRUST_LANE_V2_MERGEJOIN` default-on): mergejoin —
///     pure delegation, dynamic gates only.
///   * FLIP-3 (rung 3, `PGRUST_LANE_V2_WINDOWS` default-on): windowagg (W1
///     only; T2-A/T2-B defaults unchanged) — dynamic gates + the W1
///     structural refuses (child-not-lane-owned for non-Sort/agg-fed
///     children, shape-qual-proj for non-W1 window shapes; both stay legal
///     post-flip — the sticky-batch lane owns only its admitted family).
///
/// ONE ROW PER CLASS (rebase law; tierB review finding 2, boards with B1):
/// `assert_covered_check` is first-match-wins, so a duplicate class row
/// silently shadows every row below it. When two flips claim the same
/// class (the six T3 tail classes are shared between FLIP-1 delegation and
/// the wave-7 B1 source-form per-shape flips), the lists MUST be merged
/// into a single row per class — union of allowed reasons, same merge
/// mirrored in flip-manifest.tsv — never stacked. The
/// `manifest_has_no_duplicate_class_rows` unit turns a stacked duplicate
/// into a loud failure.
pub(super) const ASSERT_MANIFEST: &[(ShapeClass, &[RefuseReason])] = &[
    // --- FLIP-1: rowmode (rung 1) -----------------------------------------
    (
        ShapeClass::ProjectSet,
        &[
            RefuseReason::Epq,
            RefuseReason::Instrumented,
            RefuseReason::ScrollMark,
            RefuseReason::ChildNotLaneOwned,
            RefuseReason::SrfSetExpansion,
        ],
    ),
    // + wave-7 B1 per-shape flip 1/6: functionscan SOURCE form default-ON
    // (tail_source.rs flip ledger). ONE-ROW-PER-CLASS law: B1 rides this
    // merged row — its allowed list (epq,backward,instrumented,env-off) is
    // already a subset of the FLIP-1 union here.
    (
        ShapeClass::FunctionScan,
        &[
            RefuseReason::Epq,
            RefuseReason::Instrumented,
            RefuseReason::ScrollMark,
            RefuseReason::EnvOff,
        ],
    ),
    // + wave-7 B1 per-shape flip 2/6: tablefuncscan SOURCE form default-ON
    // (tail_source.rs flip ledger; ONE-ROW-PER-CLASS — B1 rides this
    // merged row).
    (
        ShapeClass::TableFuncScan,
        &[
            RefuseReason::Epq,
            RefuseReason::Instrumented,
            RefuseReason::ScrollMark,
            RefuseReason::EnvOff,
        ],
    ),
    (
        ShapeClass::ValuesScan,
        &[
            RefuseReason::Epq,
            RefuseReason::Instrumented,
            RefuseReason::ScrollMark,
        ],
    ),
    // + wave-7 B1 per-shape flip 3/6: samplescan SOURCE form default-ON
    // (tail_source.rs flip ledger; ONE-ROW-PER-CLASS — B1 rides this
    // merged row).
    (
        ShapeClass::SampleScan,
        &[
            RefuseReason::Epq,
            RefuseReason::Instrumented,
            RefuseReason::ScrollMark,
            RefuseReason::EnvOff,
        ],
    ),
    // + wave-7 B1 per-shape flip 4/6: tidscan SOURCE form default-ON
    // (tail_source.rs flip ledger; ONE-ROW-PER-CLASS — B1 rides this
    // merged row).
    (
        ShapeClass::TidScan,
        &[
            RefuseReason::Epq,
            RefuseReason::Instrumented,
            RefuseReason::ScrollMark,
            RefuseReason::EnvOff,
        ],
    ),
    // + wave-7 B1 per-shape flip 6/6: tidrangescan SOURCE form default-ON
    // (tail_source.rs flip ledger; ONE-ROW-PER-CLASS — B1 rides this
    // merged row). The ladder is COMPLETE: all six T3 classes source-form
    // default-hosted; delegation stays the rollback form.
    (
        ShapeClass::TidRangeScan,
        &[
            RefuseReason::Epq,
            RefuseReason::Instrumented,
            RefuseReason::ScrollMark,
            RefuseReason::EnvOff,
        ],
    ),
    // + wave-7 B1 per-shape flip 5/6: namedtuplestorescan SOURCE form
    // default-ON (tail_source.rs flip ledger; ONE-ROW-PER-CLASS — B1 rides
    // this merged row).
    (
        ShapeClass::NamedTuplestoreScan,
        &[
            RefuseReason::Epq,
            RefuseReason::Instrumented,
            RefuseReason::ScrollMark,
            RefuseReason::EnvOff,
        ],
    ),
    (
        ShapeClass::Material,
        &[
            RefuseReason::Epq,
            RefuseReason::Instrumented,
            RefuseReason::ScrollMark,
        ],
    ),
    (
        ShapeClass::CteScan,
        &[
            RefuseReason::Epq,
            RefuseReason::Instrumented,
            RefuseReason::ScrollMark,
        ],
    ),
    (
        ShapeClass::RecursiveUnion,
        &[
            RefuseReason::Epq,
            RefuseReason::Instrumented,
            RefuseReason::ScrollMark,
        ],
    ),
    (
        ShapeClass::WorkTableScan,
        &[
            RefuseReason::Epq,
            RefuseReason::Instrumented,
            RefuseReason::ScrollMark,
        ],
    ),
    (
        ShapeClass::Memoize,
        &[
            RefuseReason::Epq,
            RefuseReason::Instrumented,
            RefuseReason::ScrollMark,
        ],
    ),
    (
        ShapeClass::SetOp,
        &[
            RefuseReason::Epq,
            RefuseReason::Instrumented,
            RefuseReason::ScrollMark,
        ],
    ),
    (
        ShapeClass::MergeAppend,
        &[
            RefuseReason::Epq,
            RefuseReason::Instrumented,
            RefuseReason::ScrollMark,
        ],
    ),
    (
        ShapeClass::Unique,
        &[
            RefuseReason::Epq,
            RefuseReason::Instrumented,
            RefuseReason::ScrollMark,
        ],
    ),
    (
        ShapeClass::LockRows,
        &[
            RefuseReason::Epq,
            RefuseReason::Instrumented,
            RefuseReason::ScrollMark,
        ],
    ),
    // --- FLIP-2: mergejoin (rung 2) ----------------------------------------
    (
        ShapeClass::MergeJoin,
        &[RefuseReason::Epq, RefuseReason::Instrumented],
    ),
    // --- FLIP-3: windows-w1 (rung 3) ---------------------------------------
    (
        ShapeClass::WindowAgg,
        &[
            RefuseReason::Epq,
            RefuseReason::Instrumented,
            RefuseReason::ChildNotLaneOwned,
            RefuseReason::ShapeQualProj,
        ],
    ),
];

/// The Layer-A tail `stats::tick_refused` calls (the one WS-P stats.rs
/// edit). Disarmed = one memoized-bool load + branch, only on paths where
/// accounting is already armed (default config never reaches it — the tick
/// early-returns first).
#[inline]
pub(super) fn assert_covered_tail(class: ShapeClass, reason: RefuseReason) {
    if !assert_covered_armed() {
        return;
    }
    assert_covered_check(class, reason);
}

/// Manifest check + the volcano-unreachable raise (OQ10). Cold: assert-mode
/// diagnostics channels only. The panic unwinds to the backend's statement
/// boundary (tcop main_loop catch_unwind), which converts it to a PgError
/// ERROR — default sqlstate XX000 — whose message keeps the
/// `volcano-unreachable:` prefix dualexec classifies on.
#[cold]
fn assert_covered_check(class: ShapeClass, reason: RefuseReason) {
    for (covered, allowed) in ASSERT_MANIFEST {
        if *covered == class {
            if allowed.contains(&reason) {
                return;
            }
            panic!(
                "volcano-unreachable: lane refused flipped class {} (reason {}) under \
                 PGRUST_LANE_V2_ASSERT_COVERED",
                class.name(),
                reason.name()
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Test faces
// ---------------------------------------------------------------------------

/// Walk a PlannedStmt the way `record_execution` does and return the
/// (plan_node_id, kind-name) rows — the unit corpus's counting face.
#[cfg(test)]
pub(crate) fn census_rows_for_tests(pstmt: &PlannedStmt<'_>) -> Vec<(i32, &'static str)> {
    let mut rows = Vec::new();
    let mut emit = |kind: Option<NodeKind>, plan: Option<&Plan<'_>>| {
        rows.push((
            plan.map(|p| p.plan_node_id).unwrap_or(-1),
            kind.map(NodeKind::name).unwrap_or("unknown"),
        ));
    };
    if let Some(root) = pstmt.planTree {
        walk_plan(root, &mut emit);
    }
    for cell in pstmt.subplans.iter() {
        if let Some(sub) = cell {
            walk_plan(sub, &mut emit);
        }
    }
    rows
}

/// The root-classification cross-check face for the unit corpus.
#[cfg(test)]
pub(crate) fn planstate_kind_name_for_tests(node: &PlanStateNode<'_>) -> &'static str {
    kind_of_planstate(node).name()
}

/// Engine-attribution face for the unit corpus.
#[cfg(test)]
pub(crate) fn attribution_for_tests(
    estate: &EStateData<'_>,
    plan_node_id: i32,
) -> (&'static str, &'static str, &'static str) {
    attribution(estate, plan_node_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The compiled-in manifest and its scripts/coverage mirror never drift:
    /// same row count, same class names, same allowed-reason lists. Both are
    /// EMPTY until the first flip commit (which must edit them together).
    #[test]
    fn manifest_mirror_in_sync() {
        const MIRROR: &str =
            include_str!("../../../../../../crates/backend/executor/execmain/src/lanev2/flip-manifest.tsv");
        let data_rows: Vec<&str> = MIRROR
            .lines()
            .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
            .filter(|l| !l.starts_with("class\t"))
            .collect();
        assert_eq!(
            data_rows.len(),
            ASSERT_MANIFEST.len(),
            "flip-manifest.tsv rows != compiled ASSERT_MANIFEST rows — flip commits must \
             edit both in the same commit"
        );
        for (row, (class, allowed)) in data_rows.iter().zip(ASSERT_MANIFEST) {
            let mut cols = row.split('\t');
            let class_col = cols.next().unwrap_or("");
            let reasons_col = cols.next().unwrap_or("");
            assert_eq!(class_col, class.name());
            let mirror_reasons: Vec<&str> = if reasons_col == "-" {
                Vec::new()
            } else {
                reasons_col.split(',').filter(|s| !s.is_empty()).collect()
            };
            let compiled: Vec<&str> = allowed.iter().map(|r| r.name()).collect();
            assert_eq!(mirror_reasons, compiled);
        }
    }

    /// `assert_covered_check` is FIRST-MATCH-WINS over ASSERT_MANIFEST, so a
    /// duplicate class row would silently shadow every row below it — the §4
    /// rebase hazard (tierB review finding 2, 4b22d75c4; that unit boards
    /// with B1 per the wave-4 integrator, notes/se-wave4-flips.md): the six
    /// T3 tail classes are shared between the FLIP-1 rows and the wave-7 B1
    /// per-shape flips, so stacked duplicates would silently drop an
    /// allowed-reason. Duplicates must be reconciled into ONE merged row per
    /// class (union of allowed reasons, mirrored in flip-manifest.tsv); this
    /// pin turns the silent shadow into a loud unit failure at rebase time.
    #[test]
    fn manifest_has_no_duplicate_class_rows() {
        for (i, (class, _)) in ASSERT_MANIFEST.iter().enumerate() {
            assert!(
                !ASSERT_MANIFEST[..i].iter().any(|(c, _)| c == class),
                "duplicate ASSERT_MANIFEST row for class {} — first-match-wins \
                 shadowing; merge the allowed-reason lists into one row (same \
                 merge in flip-manifest.tsv)",
                class.name()
            );
        }
    }

    /// Every census kind name is distinct and lowercase (the TSV/aggregator
    /// vocabulary contract), and the tag classifier covers the full plan-tag
    /// band exactly once each.
    #[test]
    fn kind_vocabulary_is_distinct() {
        let tags = [
            NodeTag::T_Result,
            NodeTag::T_ProjectSet,
            NodeTag::T_ModifyTable,
            NodeTag::T_Append,
            NodeTag::T_MergeAppend,
            NodeTag::T_RecursiveUnion,
            NodeTag::T_BitmapAnd,
            NodeTag::T_BitmapOr,
            NodeTag::T_SeqScan,
            NodeTag::T_SampleScan,
            NodeTag::T_IndexScan,
            NodeTag::T_IndexOnlyScan,
            NodeTag::T_BitmapIndexScan,
            NodeTag::T_BitmapHeapScan,
            NodeTag::T_TidScan,
            NodeTag::T_TidRangeScan,
            NodeTag::T_SubqueryScan,
            NodeTag::T_FunctionScan,
            NodeTag::T_ValuesScan,
            NodeTag::T_TableFuncScan,
            NodeTag::T_CteScan,
            NodeTag::T_NamedTuplestoreScan,
            NodeTag::T_WorkTableScan,
            NodeTag::T_ForeignScan,
            NodeTag::T_CustomScan,
            NodeTag::T_NestLoop,
            NodeTag::T_MergeJoin,
            NodeTag::T_HashJoin,
            NodeTag::T_Material,
            NodeTag::T_Memoize,
            NodeTag::T_Sort,
            NodeTag::T_IncrementalSort,
            NodeTag::T_Group,
            NodeTag::T_Agg,
            NodeTag::T_WindowAgg,
            NodeTag::T_Unique,
            NodeTag::T_Gather,
            NodeTag::T_GatherMerge,
            NodeTag::T_Hash,
            NodeTag::T_SetOp,
            NodeTag::T_LockRows,
            NodeTag::T_Limit,
        ];
        let mut names: Vec<&str> = tags
            .iter()
            .map(|&t| kind_of_tag(t).expect("plan tag must classify").name())
            .collect();
        names.sort();
        let n = names.len();
        names.dedup();
        assert_eq!(names.len(), n, "kind names must be distinct");
        for name in names {
            assert_eq!(name, name.to_lowercase(), "kind names are lowercase");
        }
        // Non-plan vocabulary stays out.
        assert!(kind_of_tag(NodeTag::T_Var).is_none());
        assert!(kind_of_tag(NodeTag::T_PlannedStmt).is_none());
    }

    #[test]
    fn fnv64_is_stable() {
        // Pinned reference values: the aggregator may join across runs on
        // the hash, so it must never silently change.
        assert_eq!(fnv64(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv64("SELECT 1"), fnv64("SELECT 1"));
        assert_ne!(fnv64("SELECT 1"), fnv64("SELECT 2"));
    }

    /// Manifest semantics (wave-4 FLIP rows): a class OUTSIDE the manifest
    /// never raises for any reason (assert mode is inert for unflipped
    /// classes — the pre-flip "empty manifest never raises" property,
    /// preserved per class); a manifest row returns for its allowed reasons
    /// and raises `volcano-unreachable:` for a reason outside its list.
    #[test]
    fn manifest_raise_semantics() {
        let covered = |class: ShapeClass| ASSERT_MANIFEST.iter().any(|(c, _)| *c == class);
        for class in ShapeClass::ALL {
            if !covered(class) {
                assert_covered_check(class, RefuseReason::Epq);
                assert_covered_check(class, RefuseReason::EnvOff);
            }
        }
        for (class, allowed) in ASSERT_MANIFEST {
            for r in *allowed {
                assert_covered_check(*class, *r);
            }
            // Pick a reason outside the row's list (dml-shape is in no
            // wave-4 row) and prove the raise + its dualexec-classifiable
            // prefix (OQ10).
            assert!(
                !allowed.contains(&RefuseReason::DmlShape),
                "test invariant: dml-shape must stay outside every wave-4 row"
            );
            let raised = std::panic::catch_unwind(|| {
                assert_covered_check(*class, RefuseReason::DmlShape)
            });
            let err = raised.expect_err("covered class + unallowed reason must raise");
            let msg = err
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| err.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            assert!(
                msg.starts_with("volcano-unreachable:"),
                "raise must keep the volcano-unreachable: prefix (got: {msg})"
            );
        }
    }
}
