use std::rc::Rc;

use ::execexpr::{EvalSlots, ExprState};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{Mcx, PgBox, PgVec};
use ::types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::Plan;
use ::types_nodes::NodeTag;
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::TupleDescData;

use crate::noderesult::{exec_end_result, exec_init_result, exec_result, ResultState};
use crate::nodeprojectset::{
    exec_end_project_set, exec_init_project_set, exec_project_set, ProjectSetState,
};

pub struct PlanStateBase<'mcx> {
    pub plan: &'mcx Plan<'mcx>,
    pub ps_ExprContext: Option<EcxtId>,
    // 'static, not 'mcx: result descriptors are Rc-shared with the QueryDesc
    // (C aliases the same pointer as queryDesc->tupDesc).
    pub ps_ResultTupleDesc: Option<Rc<TupleDescData<'static>>>,
    pub ps_ResultTupleSlot: Option<ExecSlotId>,
    pub ps_ProjInfo: Option<PgBox<'mcx, ExprState<'mcx>>>,
    pub qual: Option<PgBox<'mcx, ExprState<'mcx>>>,
}

// C's ExecProcNodeInstr pointer swap: instrumented trees wrap every node at
// init, so uninstrumented dispatch carries no per-tuple flag test.
pub struct InstrumentedNode<'mcx> {
    pub inner: PlanStateNode<'mcx>,
    pub instr_idx: u32,
}

pub enum PlanStateNode<'mcx> {
    Result(ResultState<'mcx>),
    ProjectSet(PgBox<'mcx, ProjectSetState<'mcx>>),
    SeqScan(::nodeseqscan::SeqScanState<'mcx>),
    SampleScan(PgBox<'mcx, ::nodesamplescan::SampleScanState<'mcx>>),
    FunctionScan(PgBox<'mcx, ::nodefunctionscan::FunctionScanState<'mcx>>),
    ValuesScan(PgBox<'mcx, ::nodevaluesscan::ValuesScanState<'mcx>>),
    TableFuncScan(PgBox<'mcx, ::nodetablefuncscan::TableFuncScanState<'mcx>>),
    CteScan(PgBox<'mcx, ::nodectescan::CteScanState<'mcx>>),
    IndexScan(::nodeindexscan::IndexScanState<'mcx>),
    TidScan(::nodetidscan::TidScanState<'mcx>),
    TidRangeScan(::nodetidrangescan::TidRangeScanState<'mcx>),
    IndexOnlyScan(PgBox<'mcx, ::nodeindexonlyscan::IndexOnlyScanState<'mcx>>),
    Agg(PgBox<'mcx, AggPlanState<'mcx>>),
    Sort(SortNode<'mcx>),
    IncrementalSort(PgBox<'mcx, IncrementalSortNode<'mcx>>),
    Material(PgBox<'mcx, MaterialNode<'mcx>>),
    Unique(PgBox<'mcx, UniqueNode<'mcx>>),
    Group(PgBox<'mcx, GroupNode<'mcx>>),
    Limit(LimitNode<'mcx>),
    LockRows(PgBox<'mcx, LockRowsNode<'mcx>>),
    BitmapHeapScan(PgBox<'mcx, BitmapHeapPlanState<'mcx>>),
    BitmapIndexScan(::nodebitmapindexscan::BitmapIndexScanState<'mcx>),
    BitmapAnd(PgBox<'mcx, BitmapCombineState<'mcx>>),
    BitmapOr(PgBox<'mcx, BitmapCombineState<'mcx>>),
    ModifyTable(PgBox<'mcx, ModifyTablePlanState<'mcx>>),
    NestLoop(NestLoopNode<'mcx>),
    HashJoin(PgBox<'mcx, HashJoinNode<'mcx>>),
    MergeJoin(PgBox<'mcx, MergeJoinNode<'mcx>>),
    WindowAgg(PgBox<'mcx, WindowAggNode<'mcx>>),
    Append(PgBox<'mcx, AppendNode<'mcx>>),
    MergeAppend(PgBox<'mcx, MergeAppendNode<'mcx>>),
    SubqueryScan(PgBox<'mcx, SubqueryScanNode<'mcx>>),
    SetOp(PgBox<'mcx, SetOpNode<'mcx>>),
    Memoize(PgBox<'mcx, MemoizeNode<'mcx>>),
    RecursiveUnion(PgBox<'mcx, RecursiveUnionNode<'mcx>>),
    WorkTableScan(PgBox<'mcx, ::nodeworktablescan::WorkTableScanState<'mcx>>),
    NamedTuplestoreScan(PgBox<'mcx, ::nodenamedtuplestorescan::NamedTuplestoreScanState<'mcx>>),
    Gather(PgBox<'mcx, GatherNode<'mcx>>),
    GatherMerge(PgBox<'mcx, GatherMergeNode<'mcx>>),
    ForeignScan(PgBox<'mcx, ::nodeforeignscan::ForeignScanState<'mcx>>),
    // Last variant: existing discriminants keep their values, so the
    // uninstrumented jump-table dispatch compiles unchanged.
    Instrumented(PgBox<'mcx, InstrumentedNode<'mcx>>),
}

// The outer child lives here (nodesort/nodeagg precedent).
pub struct GatherNode<'mcx> {
    pub state: crate::nodegather::GatherState<'mcx>,
    pub outer: PgBox<'mcx, PlanStateNode<'mcx>>,
}

pub struct GatherMergeNode<'mcx> {
    pub state: crate::nodegathermerge::GatherMergeState<'mcx>,
    pub outer: PgBox<'mcx, PlanStateNode<'mcx>>,
}

// The subplans live here (BitmapCombineState precedent; indexed fetch).
pub struct AppendNode<'mcx> {
    pub state: ::nodeappend::AppendState<'mcx>,
    pub substates: ::mcx::PgVec<'mcx, PlanStateNode<'mcx>>,
    /// Original appendplans index per substate (initial pruning skips some).
    pub subplan_origin: ::mcx::PgVec<'mcx, i32>,
    /// Lane-executor-v2 append verdict, memoized at first offer (verdict
    /// stability: a lane-driven child carries a staged-batch cursor across
    /// the Volcano boundary); the dynamic gates (EPQ, direction, parallel
    /// mode) stay per-call in `lanev2`.
    pub lane_fusible: Option<bool>,
}

pub struct MergeAppendNode<'mcx> {
    pub state: ::nodemergeappend::MergeAppendState<'mcx>,
    pub substates: ::mcx::PgVec<'mcx, PlanStateNode<'mcx>>,
    /// Original mergeplans index per substate (initial pruning skips some).
    pub subplan_origin: ::mcx::PgVec<'mcx, i32>,
}

// nodeSubqueryscan.c lives here whole (crate cycle with the node-enum owner).
pub struct SubqueryScanNode<'mcx> {
    pub ss: ::execscan::ScanState<'mcx>,
    pub subplan: PgBox<'mcx, PlanStateNode<'mcx>>,
}

impl<'mcx> ::execscan::ScanNode<'mcx> for SubqueryScanNode<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ::execscan::ScanState<'mcx> {
        &mut self.ss
    }

    /// `SubqueryRecheck`: nothing to check.
    fn epq_recheck(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        _slot: ExecSlotId,
    ) -> PgResult<bool> {
        Ok(true)
    }

    // SubqueryNext: the subplan's slot goes to the driver uncopied, as C.
    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let Some(id) = exec_proc_node(&mut self.subplan, estate)? else {
            return Ok(false);
        };
        self.ss.ss_ScanTupleSlot = id;
        Ok(true)
    }
}

// Both children live here; noderecursiveunion drives them via RuChild.
pub struct RecursiveUnionNode<'mcx> {
    pub state: ::noderecursiveunion::RecursiveUnionState<'mcx>,
    pub outer: PlanStateNode<'mcx>,
    pub inner: PlanStateNode<'mcx>,
}

// Both children live here (nodesort/nodeagg precedent; fetch closures).
pub struct SetOpNode<'mcx> {
    pub state: ::nodesetop::SetOpState<'mcx>,
    pub outer: PlanStateNode<'mcx>,
    pub inner: PlanStateNode<'mcx>,
}

// The subplan lives here (nodesort/nodeagg precedent; fetch closure).
pub struct ModifyTablePlanState<'mcx> {
    pub mt: ::nodemodifytable::ModifyTableState<'mcx>,
    pub subplan: PlanStateNode<'mcx>,
    pub epq: crate::epq::EpqState<'mcx>,
}

// The bitmapqual subtree lives here (crate cycle with the node-enum owner).
pub struct BitmapHeapPlanState<'mcx> {
    pub scan: ::nodebitmapheapscan::BitmapHeapScanState<'mcx>,
    pub bitmapqual: PlanStateNode<'mcx>,
}

// nodeBitmapAnd.c/nodeBitmapOr.c state: only the subplan list (the MultiExec
// bodies live in multi_exec_bitmap_node, next to the recursion they need).
pub struct BitmapCombineState<'mcx> {
    pub substates: ::mcx::PgVec<'mcx, PlanStateNode<'mcx>>,
}

// The outer child lives here (crate cycle with the node-enum owner).
pub struct AggPlanState<'mcx> {
    pub agg: ::nodeagg::AggStateData<'mcx>,
    pub outer: PlanStateNode<'mcx>,
    /// Lane-v2 memoized structural choice for the hash-agg breaker (None =
    /// undecided); all lane logic lives in `lanev2`.
    pub lane_choice: Option<crate::lanev2::AggLaneChoice>,
    /// Lane-v2 staged join-feed replay slot, memoized across rescan rebuilds
    /// (a fresh extra slot per rebuild would grow es_tupleTable per rescan).
    pub lane_stage_slot: Option<::executils::ExecSlotId>,
    /// Lane-v2 expression-group-key state (projected-scan builds), memoized
    /// with the choice; all logic lives in `lanev2::exprkey`.
    pub lane_exprkey: Option<Box<crate::lanev2::ExprKeyState>>,
}

// The WindowAgg node's outer child lives here (nodesort/nodeagg precedent).
pub struct WindowAggNode<'mcx> {
    pub state: ::nodewindowagg::WindowAggStateData<'mcx>,
    pub outer: PlanStateNode<'mcx>,
    /// Lane-executor-v2 structural admission verdict, memoized at the first
    /// offered pull (the SortNode::lane_fusible precedent); the dynamic
    /// EPQ/direction gates stay per-call in `lanev2::windows`.
    pub lane_admit: Option<bool>,
    /// Lane-v2 window drive (lanev2/windows.rs behind PGRUST_LANE_V2_WINDOWS).
    /// `Some` = STICKY lane ownership for the node's whole (re)scan life —
    /// the buffered partition machine cannot hand back mid-stream.
    pub lane: Option<::nodewindowagg::lane::LaneWindowDrive>,
    // --- WS-R T2-B (wave-3) ---
    /// T2-B framed-drive structural admission verdict, memoized like
    /// `lane_admit` (an independent census: W1 and T2-B admit different
    /// shape sets over the same child gates).
    pub lane_framed_admit: Option<bool>,
    /// T2-B sealed framed drive (lanev2/windows.rs behind
    /// PGRUST_LANE_V2_WINDOWS_T2B). `Some` = STICKY, exactly `lane`'s law.
    pub lane_framed: Option<::nodewindowagg::lane::LaneFramedDrive>,
    // --- end WS-R T2-B ---
}

pub struct MaterialNode<'mcx> {
    pub state: ::nodematerial::MaterialState<'mcx>,
    pub outer: PgBox<'mcx, PlanStateNode<'mcx>>,
}

pub struct MemoizeNode<'mcx> {
    pub state: ::nodememoize::MemoizeState<'mcx>,
    pub outer: PgBox<'mcx, PlanStateNode<'mcx>>,
    // C outerPlan->chgParam: accumulated changed params of the child; the
    // child rescan is deferred to the next pull (executor.h ExecProcNode) so
    // cache hits never touch the child subtree.
    pub outer_chg: ::types_nodes::bitmapset::Bitmapset<'mcx>,
}

pub struct SortNode<'mcx> {
    pub state: ::nodesort::SortState<'mcx>,
    pub outer: PgBox<'mcx, PlanStateNode<'mcx>>,
    // None only after exec_end_node (released for the forget path).
    pub outer_desc: Option<Rc<TupleDescData<'static>>>,
    /// Lane-executor-v2 sort-breaker verdict, memoized at first call (the
    /// structural fusibility cascade must not run per pulled tuple); the
    /// dynamic gates (EPQ, direction) stay per-call in `lanev2`.
    pub lane_fusible: Option<bool>,
    /// M2 runtime DISTINCT-sink STATIC shape refusal memo (lanev2
    /// `runtime_distinct`): once the plan-shape gates (order spec / spec
    /// derivation / subtree shape / expr safety) refuse, later pulls skip
    /// the whole probe. Dynamic gates (arming, EPQ, instrumentation,
    /// economics, granule floor) stay per-call.
    pub rd_shape_refused: bool,
}

// The IncrementalSort node's outer child lives here (nodesort precedent).
pub struct IncrementalSortNode<'mcx> {
    pub state: ::nodeincrementalsort::IncrementalSortState<'mcx>,
    pub outer: PlanStateNode<'mcx>,
}

pub struct LimitNode<'mcx> {
    pub state: ::nodelimit::LimitState<'mcx>,
    pub outer: PgBox<'mcx, PlanStateNode<'mcx>>,
}

pub struct LockRowsNode<'mcx> {
    pub state: ::nodelockrows::LockRowsState<'mcx>,
    pub outer: PgBox<'mcx, PlanStateNode<'mcx>>,
    pub epq: crate::epq::EpqState<'mcx>,
}

// The Unique node's outer child lives here (nodesort/nodeagg precedent).
pub struct UniqueNode<'mcx> {
    pub state: ::nodeunique::UniqueState<'mcx>,
    pub outer: PlanStateNode<'mcx>,
}

// The Group node's outer child lives here (nodesort/nodeagg precedent).
pub struct GroupNode<'mcx> {
    pub state: ::nodegroup::GroupState<'mcx>,
    pub outer: PlanStateNode<'mcx>,
}

// Both children live here; nodenestloop drives them via NestLoopChild.
pub struct NestLoopNode<'mcx> {
    pub state: ::nodenestloop::NestLoopState<'mcx>,
    pub outer: PgBox<'mcx, PlanStateNode<'mcx>>,
    pub inner: PgBox<'mcx, PlanStateNode<'mcx>>,
    /// Lane-executor-v2 NestLoop verdict, memoized at first call (the
    /// structural fusibility cascade must not run per pulled tuple, and the
    /// verdict must be stable — a lane-owned join stays lane-owned); the
    /// dynamic gates (EPQ, direction) stay per-call in `lanev2`.
    pub lane_fusible: Option<bool>,
}

// The inner Hash sub-node: its own HashState + the real inner scan child.
pub struct HashSubNode<'mcx> {
    pub state: ::nodehash::HashState<'mcx>,
    pub child: PgBox<'mcx, PlanStateNode<'mcx>>,
}

pub struct HashJoinNode<'mcx> {
    pub state: ::nodehashjoin::HashJoinState<'mcx>,
    pub outer: PgBox<'mcx, PlanStateNode<'mcx>>,
    pub hash: PgBox<'mcx, HashSubNode<'mcx>>,
    pub probe_batch: ProbeBatch<'mcx>,
    /// Lane-executor-v2 join-breaker verdict, memoized at first call (the
    /// structural fusibility cascade must not run per pulled tuple, and the
    /// verdict must be stable — a lane-owned join stays lane-owned); the
    /// dynamic gates (EPQ, direction) stay per-call in `lanev2`.
    pub lane_fusible: Option<bool>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbeBatchMode {
    Unknown,
    Off,
    On,
    // Parallel Hash: shared-table machine; probe fusion stays off (the outer
    // drive must interleave with barrier phases tuple-at-a-time). Folded into
    // this once-decided mode so the serial per-row dispatch is unchanged.
    Parallel,
}

// Probe-side fused-drive cursor: exec_hash_join returns per joined row, so
// the staged-page position must outlive each hash_join_arm call.
pub struct ProbeBatch<'mcx> {
    mode: ProbeBatchMode,
    n: u32,
    i: u32,
    // Columnar probe hashes for the staged page (hash32var_low32 cover);
    // hash_col == u16::MAX = disarmed; rows past hashes.len() (fallback
    // tail) re-eval per row.
    hash_col: u16,
    hashes: Option<PgVec<'mcx, u32>>,
    // Bloom pushed from the hash build; a miss on the staged hash skips the
    // batch fetch (false positives only — survivors run the hashclause
    // recheck). Exempt Rc: released on disarm/rebuild and in release_owned.
    filter: Option<std::rc::Rc<::nodehash::ProbeBloom<'mcx>>>,
    flt_seen: u32,
    flt_drop: u32,
}

impl<'mcx> ProbeBatch<'mcx> {
    pub const fn new() -> Self {
        ProbeBatch {
            mode: ProbeBatchMode::Unknown,
            n: 0,
            i: 0,
            hash_col: u16::MAX,
            hashes: None,
            filter: None,
            flt_seen: 0,
            flt_drop: 0,
        }
    }

    /// The fused probe drive's once-decided mode — read by the lane-v2 join
    /// hook's admission-economics gate (never preempt the fused drive).
    pub(crate) fn mode(&self) -> ProbeBatchMode {
        self.mode
    }

    // Rescan invalidates the staged page; the fusibility verdict survives.
    pub fn reset_staged(&mut self) {
        self.n = 0;
        self.i = 0;
        if let Some(h) = self.hashes.as_mut() {
            h.clear();
        }
    }
}

// Both children live here; nodemergejoin drives them via the MergeJoin traits.
pub struct MergeJoinNode<'mcx> {
    pub state: ::nodemergejoin::MergeJoinState<'mcx>,
    pub outer: PgBox<'mcx, PlanStateNode<'mcx>>,
    pub inner: PgBox<'mcx, PlanStateNode<'mcx>>,
    /// MJSORT adopted result (lanev2/runtime_mergejoin — the "merge join
    /// after sort" car, PGRUST_RUNTIME_MJSORT): both sides' published
    /// runs + the joined pair lists; the emit face serves them per pull.
    /// Dropped on rescan/end (the adopted-sort lifecycle).
    pub mjsort: Option<Box<crate::lanev2::MjSortAdopted>>,
    /// MJSORT probe-once law: only a node's FIRST pull may engage (a
    /// refused probe is sticky), so the FSM's stream can never be
    /// double-fed by a mid-stream engagement. Reset with `mjsort`.
    pub mjsort_probed: bool,
}

// Init-time tree node touched by &mut per tuple; rule-9 budget covers the per-row carriers inside.
const _: () = assert!(core::mem::size_of::<PlanStateNode<'static>>() <= 1024);

impl<'mcx> PlanStateNode<'mcx> {
    #[inline]
    pub fn ps_expr_context(&self) -> Option<EcxtId> {
        match self {
            // None: the wrapper defers to inner's rescan/reset (execami arm).
            PlanStateNode::Instrumented(_) => None,
            PlanStateNode::Result(rs) => rs.ps.ps_ExprContext,
            PlanStateNode::ProjectSet(ps) => ps.ps.ps_ExprContext,
            PlanStateNode::SeqScan(ss) => Some(ss.ss.ps_ExprContext),
            PlanStateNode::SampleScan(ss) => Some(ss.ss.ps_ExprContext),
            PlanStateNode::TidScan(ts) => Some(ts.ss.ps_ExprContext),
            PlanStateNode::TidRangeScan(ts) => Some(ts.ss.ps_ExprContext),
            PlanStateNode::FunctionScan(fs) => Some(fs.ss.ps_ExprContext),
            PlanStateNode::ValuesScan(vs) => Some(vs.ss.ps_ExprContext),
            PlanStateNode::ForeignScan(fs) => Some(fs.ss.ps_ExprContext),
            PlanStateNode::TableFuncScan(ts) => Some(ts.ss.ps_ExprContext),
            PlanStateNode::CteScan(cs) => Some(cs.ss.ps_ExprContext),
            PlanStateNode::IndexScan(is) => Some(is.ss.ps_ExprContext),
            PlanStateNode::IndexOnlyScan(ios) => Some(ios.ss.ps_ExprContext),
            PlanStateNode::Agg(aps) => Some(aps.agg.ps_ExprContext),
            // C sorts have no ExprContext.
            PlanStateNode::Sort(_) => None,
            // Divergence: this port's presorted-key equality runs in an
            // ExprState, which needs a resettable per-tuple context.
            PlanStateNode::IncrementalSort(s) => Some(s.state.ps_ExprContext),
            PlanStateNode::Material(_) => None,
            PlanStateNode::Memoize(m) => Some(m.state.ps_ExprContext),
            PlanStateNode::Unique(u) => Some(u.state.ps_ExprContext),
            PlanStateNode::Group(g) => Some(g.state.ps_ExprContext),
            PlanStateNode::Limit(l) => Some(l.state.ps_ExprContext),
            PlanStateNode::LockRows(_) => None,
            PlanStateNode::NestLoop(nl) => Some(nl.state.ps_ExprContext),
            PlanStateNode::HashJoin(hj) => Some(hj.state.ps_ExprContext),
            PlanStateNode::MergeJoin(mj) => Some(mj.state.ps_ExprContext),
            PlanStateNode::WindowAgg(w) => Some(w.state.ps_ExprContext),
            PlanStateNode::BitmapHeapScan(b) => Some(b.scan.ss.ps_ExprContext),
            // C's ExecInitAppend/ExecInitMergeAppend assign no ExprContext.
            PlanStateNode::Append(_) => None,
            PlanStateNode::MergeAppend(_) => None,
            PlanStateNode::SubqueryScan(s) => Some(s.ss.ps_ExprContext),
            PlanStateNode::SetOp(s) => Some(s.state.ps_ExprContext),
            // Divergence: C's RU has no ExprContext, only the tempContext here.
            PlanStateNode::RecursiveUnion(ru) => ru.state.ps_ExprContext,
            PlanStateNode::WorkTableScan(wts) => Some(wts.ss.ps_ExprContext),
            PlanStateNode::NamedTuplestoreScan(nts) => Some(nts.ss.ps_ExprContext),
            PlanStateNode::Gather(g) => g.state.ps.ps_ExprContext,
            PlanStateNode::GatherMerge(gm) => gm.state.ps.ps_ExprContext,
            PlanStateNode::BitmapIndexScan(_)
            | PlanStateNode::BitmapAnd(_)
            | PlanStateNode::BitmapOr(_)
            | PlanStateNode::ModifyTable(_) => None,
        }
    }

    /// `ExecGetResultType` (execUtils.c). Scan nodes don't retain a desc when
    /// projection is elided, so the root type is rebuilt from the targetlist
    /// (C's ExecInitResultTypeTL desc, same content).
    pub fn exec_get_result_type(
        &self,
        plan: &Plan<'mcx>,
    ) -> PgResult<Rc<TupleDescData<'static>>> {
        match self {
            PlanStateNode::Instrumented(w) => w.inner.exec_get_result_type(plan),
            PlanStateNode::Result(rs) => Ok(rs
                .ps
                .ps_ResultTupleDesc
                .clone()
                .expect("ResultState without a result type")),
            PlanStateNode::ProjectSet(ps) => Ok(ps
                .ps
                .ps_ResultTupleDesc
                .clone()
                .expect("ProjectSetState without a result type")),
            PlanStateNode::SeqScan(_)
            | PlanStateNode::SampleScan(_)
            | PlanStateNode::FunctionScan(_)
            | PlanStateNode::TableFuncScan(_)
            | PlanStateNode::ValuesScan(_)
            | PlanStateNode::ForeignScan(_)
            | PlanStateNode::CteScan(_)
            | PlanStateNode::IndexScan(_)
            | PlanStateNode::IndexOnlyScan(_)
            | PlanStateNode::TidScan(_)
            | PlanStateNode::TidRangeScan(_)
            | PlanStateNode::Limit(_)
            | PlanStateNode::LockRows(_)
            | PlanStateNode::BitmapHeapScan(_)
            | PlanStateNode::Append(_)
            | PlanStateNode::MergeAppend(_)
            | PlanStateNode::SubqueryScan(_)
            | PlanStateNode::RecursiveUnion(_)
            | PlanStateNode::WorkTableScan(_)
            | PlanStateNode::NamedTuplestoreScan(_) => crate::exec_type_from_tl(&plan.targetlist),
            // The tlist is NIL (empty type) without RETURNING, else the first
            // RETURNING list setrefs installed.
            PlanStateNode::ModifyTable(_) => crate::exec_type_from_tl(&plan.targetlist),
            PlanStateNode::Agg(aps) => Ok(aps.agg.ps_ResultTupleDesc.clone().expect("agg already ended")),
            PlanStateNode::Sort(s) => Ok(::nodesort::sort_result_type(&s.state)),
            PlanStateNode::IncrementalSort(s) => {
                Ok(s.state.ps_ResultTupleDesc.clone().expect("incremental sort already ended"))
            }
            PlanStateNode::Material(m) => Ok(m.state.ps_ResultTupleDesc.clone().expect("material already ended")),
            PlanStateNode::Unique(u) => Ok(u.state.ps_ResultTupleDesc.clone().expect("unique already ended")),
            PlanStateNode::Group(g) => Ok(g.state.ps_ResultTupleDesc.clone().expect("group already ended")),
            PlanStateNode::NestLoop(nl) => Ok(nl.state.ps_ResultTupleDesc.clone().expect("nest loop already ended")),
            PlanStateNode::HashJoin(hj) => Ok(hj.state.ps_ResultTupleDesc.clone().expect("hash join already ended")),
            PlanStateNode::MergeJoin(mj) => Ok(mj.state.ps_ResultTupleDesc.clone().expect("merge join already ended")),
            PlanStateNode::WindowAgg(w) => Ok(w.state.ps_ResultTupleDesc.clone().expect("window agg already ended")),
            PlanStateNode::SetOp(s) => Ok(s.state.ps_ResultTupleDesc.clone().expect("set op already ended")),
            PlanStateNode::Gather(g) => {
                Ok(g.state.ps.ps_ResultTupleDesc.clone().expect("gather already ended"))
            }
            PlanStateNode::GatherMerge(gm) => {
                Ok(gm.state.ps.ps_ResultTupleDesc.clone().expect("gather merge already ended"))
            }
            PlanStateNode::Memoize(m) => Ok(m.state.ps_ResultTupleDesc.clone().expect("memoize already ended")),
            PlanStateNode::BitmapIndexScan(_)
            | PlanStateNode::BitmapAnd(_)
            | PlanStateNode::BitmapOr(_) => {
                panic!("ExecGetResultType on a bitmap-producing node")
            }
        }
    }
}

// unported: planner-reachable unported plan nodes raise a clean
// ERRCODE_FEATURE_NOT_SUPPORTED error at init time (safe unwind); an
// unrecognized tag stays a loud invariant panic.
macro_rules! unported_nodes {
    ($tag:expr, { $($t:ident => $file:literal),+ $(,)? }) => {
        match $tag {
            $(NodeTag::$t => {
                return Err(Box::new(
                    PgError::error(concat!(
                        "plan node ", stringify!($t), " (", $file, ") is not yet implemented"
                    ))
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                ))
            })+
            other => panic!("ExecInitNode: unrecognized node type: {other:?}"),
        }
    };
}

/// `ExecInitNode` (execProcnode.c).
pub fn exec_init_node<'mcx>(
    node: Option<Node<'mcx>>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<Option<PlanStateNode<'mcx>>> {
    let Some(node) = node else {
        return Ok(None);
    };
    let result = match node.node_tag() {
        NodeTag::T_Result => PlanStateNode::Result(exec_init_result(
            node.as_result().unwrap(),
            estate,
            eflags,
        )?),
        NodeTag::T_ProjectSet => {
            let state =
                exec_init_project_set(node.as_project_set().unwrap(), estate, eflags)?;
            PlanStateNode::ProjectSet(::mcx::alloc_in(estate.es_query_cxt, state)?)
        }
        NodeTag::T_SeqScan => {
            let mcx = estate.es_query_cxt;
            PlanStateNode::SeqScan(::nodeseqscan::exec_init_seq_scan(
                mcx,
                node.as_seq_scan().unwrap(),
                estate,
                eflags,
            )?)
        }
        NodeTag::T_SampleScan => {
            let mcx = estate.es_query_cxt;
            let state = ::nodesamplescan::exec_init_sample_scan(
                mcx,
                node.as_sample_scan().unwrap(),
                estate,
                eflags,
            )?;
            PlanStateNode::SampleScan(::mcx::alloc_in(mcx, state)?)
        }
        NodeTag::T_FunctionScan => {
            let mcx = estate.es_query_cxt;
            let state = ::nodefunctionscan::exec_init_function_scan(
                mcx,
                node.as_function_scan().unwrap(),
                estate,
                eflags,
            )?;
            PlanStateNode::FunctionScan(::mcx::alloc_in(mcx, state)?)
        }
        NodeTag::T_TableFuncScan => {
            let mcx = estate.es_query_cxt;
            let state = ::nodetablefuncscan::exec_init_table_func_scan(
                mcx,
                node.as_table_func_scan().unwrap(),
                estate,
            )?;
            PlanStateNode::TableFuncScan(::mcx::alloc_in(mcx, state)?)
        }
        NodeTag::T_ValuesScan => {
            let mcx = estate.es_query_cxt;
            let state = ::nodevaluesscan::exec_init_values_scan(
                mcx,
                node.as_values_scan().unwrap(),
                estate,
            )?;
            PlanStateNode::ValuesScan(::mcx::alloc_in(mcx, state)?)
        }
        NodeTag::T_ForeignScan => {
            let mcx = estate.es_query_cxt;
            let state = ::nodeforeignscan::exec_init_foreign_scan(
                mcx,
                node.as_foreign_scan().unwrap(),
                estate,
                eflags,
            )?;
            PlanStateNode::ForeignScan(::mcx::alloc_in(mcx, state)?)
        }
        NodeTag::T_CteScan => {
            let mcx = estate.es_query_cxt;
            let cte_plan = node.as_cte_scan().unwrap();
            let idx = (cte_plan.ctePlanId - 1) as usize;
            let (scan_desc, sub_tlist) = {
                let cell = estate.es_subplanstates.get(idx).unwrap_or_else(|| {
                    panic!(
                        "ExecInitCteScan (nodeCtescan.c): could not find plan for \
                         ctePlanId {}",
                        cte_plan.ctePlanId
                    )
                });
                // SAFETY: es_subplanstates cells are arena-live
                // *mut Option<PlanStateNode> installed by InitPlan.
                let sub = unsafe { &*cell.0.cast::<Option<PlanStateNode>>().as_ptr() }
                    .as_ref()
                    .expect("CTE subplan state present at CteScan init");
                let sub_plan = estate
                    .es_plannedstmt
                    .expect("es_plannedstmt set before plan init")
                    .subplans
                    .nth(idx)
                    // CTE subplans are parallel-restricted; never a NULL hole.
                    .expect("CteScan subplan cell present")
                    .as_plan()
                    .expect("subplans cell is a plan tree");
                (sub.exec_get_result_type(sub_plan)?, &sub_plan.targetlist)
            };
            let state = ::nodectescan::exec_init_cte_scan(
                mcx, cte_plan, estate, eflags, scan_desc, sub_tlist,
            )?;
            PlanStateNode::CteScan(::mcx::alloc_in(mcx, state)?)
        }
        NodeTag::T_IndexScan => {
            let mcx = estate.es_query_cxt;
            PlanStateNode::IndexScan(::nodeindexscan::exec_init_index_scan(
                mcx,
                node.as_index_scan().unwrap(),
                estate,
                eflags,
            )?)
        }
        NodeTag::T_TidScan => {
            let mcx = estate.es_query_cxt;
            PlanStateNode::TidScan(::nodetidscan::exec_init_tid_scan(
                mcx,
                node.as_tid_scan().unwrap(),
                estate,
                eflags,
            )?)
        }
        NodeTag::T_TidRangeScan => {
            let mcx = estate.es_query_cxt;
            PlanStateNode::TidRangeScan(::nodetidrangescan::exec_init_tid_range_scan(
                mcx,
                node.as_tid_range_scan().unwrap(),
                estate,
                eflags,
            )?)
        }
        NodeTag::T_IndexOnlyScan => {
            let mcx = estate.es_query_cxt;
            let ios = ::nodeindexonlyscan::exec_init_index_only_scan(
                mcx,
                node.as_index_only_scan().unwrap(),
                estate,
                eflags,
            )?;
            PlanStateNode::IndexOnlyScan(::mcx::alloc_in(mcx, ios)?)
        }
        NodeTag::T_BitmapHeapScan => {
            let mcx = estate.es_query_cxt;
            let bhs_plan = node.as_bitmap_heap_scan().unwrap();
            let scan = ::nodebitmapheapscan::exec_init_bitmap_heap_scan(
                mcx, bhs_plan, estate, eflags,
            )?;
            let bitmapqual = exec_init_node(bhs_plan.scan.plan.lefttree, estate, eflags)?
                .unwrap_or_else(|| {
                    panic!("ExecInitBitmapHeapScan: BitmapHeapScan without a bitmapqual subplan")
                });
            PlanStateNode::BitmapHeapScan(::mcx::alloc_in(
                mcx,
                BitmapHeapPlanState { scan, bitmapqual },
            )?)
        }
        NodeTag::T_BitmapIndexScan => {
            let mcx = estate.es_query_cxt;
            PlanStateNode::BitmapIndexScan(::nodebitmapindexscan::exec_init_bitmap_index_scan(
                mcx,
                node.as_bitmap_index_scan().unwrap(),
                estate,
                eflags,
            )?)
        }
        NodeTag::T_BitmapAnd => {
            let mcx = estate.es_query_cxt;
            let plan = node.as_bitmap_and().unwrap();
            PlanStateNode::BitmapAnd(::mcx::alloc_in(
                mcx,
                init_bitmap_combine(&plan.bitmapplans, estate, eflags)?,
            )?)
        }
        NodeTag::T_BitmapOr => {
            let mcx = estate.es_query_cxt;
            let plan = node.as_bitmap_or().unwrap();
            // isshared only picks C's dsa allocator for the shared result
            // bitmap; thread-native needs no arm (see nodebitmapindexscan).
            PlanStateNode::BitmapOr(::mcx::alloc_in(
                mcx,
                init_bitmap_combine(&plan.bitmapplans, estate, eflags)?,
            )?)
        }
        NodeTag::T_Material => {
            let mcx = estate.es_query_cxt;
            let mat_plan = node.as_material().unwrap();
            let outer = exec_init_node(
                mat_plan.plan.lefttree,
                estate,
                ::nodematerial::child_eflags(eflags),
            )?
            .unwrap_or_else(|| {
                panic!("ExecInitMaterial (nodeMaterial.c): Material without an outer plan")
            });
            let result_desc = crate::exec_type_from_tl(&mat_plan.plan.targetlist)?;
            let state = ::nodematerial::exec_init_material(mat_plan, estate, eflags, result_desc)?;
            PlanStateNode::Material(::mcx::alloc_in(
                mcx,
                MaterialNode { state, outer: ::mcx::alloc_in(mcx, outer)? },
            )?)
        }
        NodeTag::T_Memoize => {
            let mcx = estate.es_query_cxt;
            let memo_plan = node.as_memoize().unwrap();
            let outer = exec_init_node(
                memo_plan.plan.lefttree,
                estate,
                ::nodememoize::child_eflags(eflags),
            )?
            .unwrap_or_else(|| {
                panic!("ExecInitMemoize (nodeMemoize.c): Memoize without an outer plan")
            });
            let result_desc = crate::exec_type_from_tl(&memo_plan.plan.targetlist)?;
            let hashkeydesc = crate::typefromtl::exec_type_from_expr_list(&memo_plan.param_exprs)?;
            let state = ::nodememoize::exec_init_memoize(
                memo_plan,
                estate,
                eflags,
                result_desc,
                hashkeydesc,
            )?;
            PlanStateNode::Memoize(::mcx::alloc_in(
                mcx,
                MemoizeNode {
                    state,
                    outer: ::mcx::alloc_in(mcx, outer)?,
                    outer_chg: ::types_nodes::bitmapset::Bitmapset::empty(),
                },
            )?)
        }
        NodeTag::T_Sort => {
            let sort_plan = node.as_sort().unwrap();
            let outer = exec_init_node(
                sort_plan.plan.lefttree,
                estate,
                ::nodesort::sort_child_eflags(eflags),
            )?
            .unwrap_or_else(|| panic!("ExecInitSort (nodeSort.c): Sort without an outer plan"));
            let outer_desc = outer
                .exec_get_result_type(sort_plan.plan.lefttree.unwrap().as_plan().unwrap())?;
            let result_desc = crate::exec_type_from_tl(&sort_plan.plan.targetlist)?;
            let state =
                ::nodesort::exec_init_sort(sort_plan, estate, eflags, &outer_desc, result_desc)?;
            PlanStateNode::Sort(SortNode {
                state,
                outer: ::mcx::alloc_in(estate.es_query_cxt, outer)?,
                outer_desc: Some(outer_desc),
                lane_fusible: None,
                rd_shape_refused: false,
            })
        }
        NodeTag::T_IncrementalSort => {
            let mcx = estate.es_query_cxt;
            let is_plan = node.as_incremental_sort().unwrap();
            // C keeps REWIND for the child; BACKWARD/MARK never reach here.
            let outer = exec_init_node(is_plan.sort.plan.lefttree, estate, eflags)?
                .unwrap_or_else(|| {
                    panic!(
                        "ExecInitIncrementalSort (nodeIncrementalSort.c): \
                         IncrementalSort without an outer plan"
                    )
                });
            let outer_desc = outer
                .exec_get_result_type(is_plan.sort.plan.lefttree.unwrap().as_plan().unwrap())?;
            let result_desc = crate::exec_type_from_tl(&is_plan.sort.plan.targetlist)?;
            let state = ::nodeincrementalsort::exec_init_incremental_sort(
                is_plan,
                estate,
                eflags,
                &outer_desc,
                result_desc,
            );
            PlanStateNode::IncrementalSort(::mcx::alloc_in(
                mcx,
                IncrementalSortNode { state, outer },
            )?)
        }
        NodeTag::T_Unique => {
            let mcx = estate.es_query_cxt;
            let uq_plan = node.as_unique().unwrap();
            let outer = exec_init_node(uq_plan.plan.lefttree, estate, eflags)?
                .unwrap_or_else(|| {
                    panic!("ExecInitUnique (nodeUnique.c): Unique without an outer plan")
                });
            let outer_desc =
                outer.exec_get_result_type(uq_plan.plan.lefttree.unwrap().as_plan().unwrap())?;
            let result_desc = crate::exec_type_from_tl(&uq_plan.plan.targetlist)?;
            let state =
                ::nodeunique::exec_init_unique(uq_plan, estate, eflags, &outer_desc, result_desc)?;
            PlanStateNode::Unique(::mcx::alloc_in(mcx, UniqueNode { state, outer })?)
        }
        NodeTag::T_Group => {
            let mcx = estate.es_query_cxt;
            let g_plan = node.as_group().unwrap();
            let outer = exec_init_node(g_plan.plan.lefttree, estate, eflags)?
                .unwrap_or_else(|| {
                    panic!("ExecInitGroup (nodeGroup.c): Group without an outer plan")
                });
            let outer_desc =
                outer.exec_get_result_type(g_plan.plan.lefttree.unwrap().as_plan().unwrap())?;
            let result_desc = crate::exec_type_from_tl(&g_plan.plan.targetlist)?;
            let params = estate.param_bind();
            let (qual, proj) = ::executils::with_subplan_compile_env(estate, |env| -> PgResult<_> {
                let qual =
                    ::execexpr::exec_init_qual_subplans(mcx, &g_plan.plan.qual, params, env)?;
                let proj = ::execexpr::exec_build_projection_info_subplans(
                    mcx,
                    &g_plan.plan.targetlist,
                    None,
                    params,
                    env,
                )?;
                Ok((qual, proj))
            })?;
            let state = ::nodegroup::exec_init_group(
                g_plan,
                estate,
                eflags,
                &outer_desc,
                result_desc,
                qual,
                proj,
            )?;
            PlanStateNode::Group(::mcx::alloc_in(mcx, GroupNode { state, outer })?)
        }
        NodeTag::T_Limit => {
            let limit_plan = node.as_limit().unwrap();
            let outer = exec_init_node(limit_plan.plan.lefttree, estate, eflags)?
                .unwrap_or_else(|| {
                    panic!("ExecInitLimit (nodeLimit.c): Limit without an outer plan")
                });
            // WITH TIES needs the outer result type for its tie-equality
            // program (C: ExecGetResultType(outerPlanState)).
            let outer_desc = if limit_plan.limitOption
                == ::types_nodes::LimitOption::LIMIT_OPTION_WITH_TIES
            {
                Some(outer.exec_get_result_type(
                    limit_plan.plan.lefttree.unwrap().as_plan().unwrap(),
                )?)
            } else {
                None
            };
            let state =
                ::nodelimit::exec_init_limit(limit_plan, estate, eflags, outer_desc.as_ref())?;
            PlanStateNode::Limit(LimitNode {
                state,
                outer: ::mcx::alloc_in(estate.es_query_cxt, outer)?,
            })
        }
        NodeTag::T_LockRows => {
            let lr_plan = node.as_lock_rows().unwrap();
            let outer_plan_node = lr_plan.plan.lefttree.unwrap_or_else(|| {
                panic!("ExecInitLockRows (nodeLockRows.c): LockRows without an outer plan")
            });
            let outer = exec_init_node(Some(outer_plan_node), estate, eflags)?
                .expect("ExecInitNode of a non-NULL outer plan");
            let outer_tlist = &outer_plan_node.as_plan().expect("plan node").targetlist;
            let state = ::nodelockrows::exec_init_lock_rows(lr_plan, estate, eflags, outer_tlist)?;
            // EvalPlanQualInit(epqstate, outerPlan, epq_arowmarks); the test
            // slots double as the mark slots (EvalPlanQualSlot).
            let epq = crate::epq::EpqState {
                plan: lr_plan.plan.lefttree,
                recheck: None,
                result_rti: state.lr_arowMarks.first().map_or(0, |a| a.rti),
                lane_verdicts: None,
            };
            PlanStateNode::LockRows(::mcx::alloc_in(
                estate.es_query_cxt,
                LockRowsNode {
                    state,
                    outer: ::mcx::alloc_in(estate.es_query_cxt, outer)?,
                    epq,
                },
            )?)
        }
        NodeTag::T_Agg => {
            let mcx = estate.es_query_cxt;
            let agg_plan = node.as_agg().unwrap();
            let outer = exec_init_node(agg_plan.plan.lefttree, estate, eflags)?
                .unwrap_or_else(|| panic!("ExecInitAgg (nodeAgg.c): Agg without an outer plan"));
            let desc = crate::exec_type_from_tl(&agg_plan.plan.targetlist)?;
            let outer_desc =
                outer.exec_get_result_type(agg_plan.plan.lefttree.unwrap().as_plan().unwrap())?;
            let agg = ::nodeagg::exec_init_agg(agg_plan, estate, eflags, desc, Some(outer_desc))?;
            PlanStateNode::Agg(::mcx::alloc_in(
                mcx,
                AggPlanState { agg, outer, lane_choice: None, lane_stage_slot: None, lane_exprkey: None },
            )?)
        }
        NodeTag::T_WindowAgg => {
            let mcx = estate.es_query_cxt;
            let wa_plan = node.as_window_agg().unwrap();
            let outer = exec_init_node(wa_plan.plan.lefttree, estate, eflags)?
                .unwrap_or_else(|| {
                    panic!("ExecInitWindowAgg (nodeWindowAgg.c): WindowAgg without an outer plan")
                });
            let outer_desc =
                outer.exec_get_result_type(wa_plan.plan.lefttree.unwrap().as_plan().unwrap())?;
            let result_desc = crate::exec_type_from_tl(&wa_plan.plan.targetlist)?;
            let state = ::nodewindowagg::exec_init_window_agg(
                wa_plan,
                estate,
                eflags,
                &outer_desc,
                result_desc,
            )?;
            PlanStateNode::WindowAgg(::mcx::alloc_in(
                mcx,
                WindowAggNode {
                    state,
                    outer,
                    lane_admit: None,
                    lane: None,
                    lane_framed_admit: None,
                    lane_framed: None,
                },
            )?)
        }
        NodeTag::T_NestLoop => {
            let mcx = estate.es_query_cxt;
            let nl_plan = node.as_nest_loop().unwrap();
            let outer = exec_init_node(nl_plan.join.plan.lefttree, estate, eflags)?
                .unwrap_or_else(|| {
                    panic!("ExecInitNestLoop (nodeNestloop.c): NestLoop without an outer plan")
                });
            // With no nestParams the inner rescans with unchanged params, so
            // request cheap rescans (C's EXEC_FLAG_REWIND arm).
            let inner_eflags = if nl_plan.nestParams.is_nil() {
                eflags | ::types_slot::EXEC_FLAG_REWIND
            } else {
                eflags
            };
            let inner = exec_init_node(
                nl_plan.join.plan.righttree,
                estate,
                inner_eflags,
            )?
            .unwrap_or_else(|| {
                panic!("ExecInitNestLoop (nodeNestloop.c): NestLoop without an inner plan")
            });
            let desc = crate::exec_type_from_tl(&nl_plan.join.plan.targetlist)?;
            let inner_desc = inner
                .exec_get_result_type(nl_plan.join.plan.righttree.unwrap().as_plan().unwrap())?;
            let state =
                ::nodenestloop::exec_init_nest_loop(nl_plan, estate, eflags, desc, &inner_desc)?;
            PlanStateNode::NestLoop(NestLoopNode {
                state,
                outer: ::mcx::alloc_in(mcx, outer)?,
                inner: ::mcx::alloc_in(mcx, inner)?,
                lane_fusible: None,
            })
        }
        NodeTag::T_HashJoin => {
            let mcx = estate.es_query_cxt;
            let hj_plan = node.as_hash_join().unwrap();
            let outer_p = hj_plan
                .join
                .plan
                .lefttree
                .unwrap_or_else(|| panic!("ExecInitHashJoin (nodeHashjoin.c): HashJoin without an outer plan"));
            let outer = exec_init_node(Some(outer_p), estate, eflags)?
                .expect("HashJoin outer plan initialized");
            let outer_desc = outer.exec_get_result_type(outer_p.as_plan().unwrap())?;

            // The inner is a Hash node; init its own child (the real inner scan).
            let hash_plan_node = hj_plan
                .join
                .plan
                .righttree
                .unwrap_or_else(|| panic!("ExecInitHashJoin (nodeHashjoin.c): HashJoin without a Hash inner plan"))
                .as_hash()
                .unwrap_or_else(|| panic!("ExecInitHashJoin (nodeHashjoin.c): HashJoin inner is not a Hash node"));
            let hash_child_p = hash_plan_node
                .plan
                .lefttree
                .unwrap_or_else(|| panic!("ExecInitHash (nodeHash.c): Hash without an outer plan"));
            let hash_child = exec_init_node(Some(hash_child_p), estate, eflags)?
                .expect("Hash child plan initialized");
            let inner_desc = hash_child.exec_get_result_type(hash_child_p.as_plan().unwrap())?;

            let result_desc = crate::exec_type_from_tl(&hj_plan.join.plan.targetlist)?;
            let (state, hash_state) = ::nodehashjoin::exec_init_hash_join(
                hj_plan,
                estate,
                eflags,
                result_desc,
                &outer_desc,
                inner_desc,
                |es, idesc, ihashfns, colls| {
                    ::nodehash::exec_init_hash(hash_plan_node, es, idesc, ihashfns, colls)
                },
            )?;
            PlanStateNode::HashJoin(::mcx::alloc_in(
                mcx,
                HashJoinNode {
                    state,
                    outer: ::mcx::alloc_in(mcx, outer)?,
                    hash: ::mcx::alloc_in(
                        mcx,
                        HashSubNode { state: hash_state, child: ::mcx::alloc_in(mcx, hash_child)? },
                    )?,
                    probe_batch: ProbeBatch::new(),
                    lane_fusible: None,
                },
            )?)
        }
        NodeTag::T_MergeJoin => {
            let mcx = estate.es_query_cxt;
            let mj_plan = node.as_merge_join().unwrap();
            let outer_p = mj_plan.join.plan.lefttree.unwrap_or_else(|| {
                panic!("ExecInitMergeJoin (nodeMergejoin.c): MergeJoin without an outer plan")
            });
            let outer = exec_init_node(Some(outer_p), estate, eflags)?
                .expect("MergeJoin outer plan initialized");
            let inner_p = mj_plan.join.plan.righttree.unwrap_or_else(|| {
                panic!("ExecInitMergeJoin (nodeMergejoin.c): MergeJoin without an inner plan")
            });
            let inner_eflags =
                ::nodemergejoin::inner_child_eflags(eflags, mj_plan.skip_mark_restore);
            let inner = exec_init_node(Some(inner_p), estate, inner_eflags)?
                .expect("MergeJoin inner plan initialized");
            let outer_desc = outer.exec_get_result_type(outer_p.as_plan().unwrap())?;
            let inner_desc = inner.exec_get_result_type(inner_p.as_plan().unwrap())?;
            let result_desc = crate::exec_type_from_tl(&mj_plan.join.plan.targetlist)?;
            let inner_is_material = inner_p.node_tag() == NodeTag::T_Material;
            let state = ::nodemergejoin::exec_init_merge_join(
                mj_plan,
                estate,
                eflags,
                &outer_desc,
                &inner_desc,
                result_desc,
                inner_is_material,
            )?;
            PlanStateNode::MergeJoin(::mcx::alloc_in(
                mcx,
                MergeJoinNode {
                    state,
                    outer: ::mcx::alloc_in(mcx, outer)?,
                    inner: ::mcx::alloc_in(mcx, inner)?,
                    mjsort: None,
                    mjsort_probed: false,
                },
            )?)
        }
        NodeTag::T_Append => {
            let mcx = estate.es_query_cxt;
            let ap_plan = node.as_append().unwrap();
            let n_total = ap_plan.appendplans.len();
            let (prune_state, valid) = if ap_plan.part_prune_index >= 0 {
                let (ps, valid) = ::execpartition::pruning::exec_init_partition_exec_pruning(
                    estate,
                    n_total as i32,
                    ap_plan.part_prune_index,
                    &ap_plan.apprelids,
                )?;
                (ps.map(Box::new), valid)
            } else {
                let mut all = ::types_nodes::bitmapset::Bitmapset::empty();
                if n_total > 0 {
                    ::partprune::bms_add_range(mcx, &mut all, 0, n_total as i32 - 1)?;
                }
                (None, all)
            };
            let mut substates: ::mcx::PgVec<'mcx, PlanStateNode<'mcx>> =
                ::mcx::PgVec::new_in(mcx);
            let mut subplan_origin: ::mcx::PgVec<'mcx, i32> = ::mcx::PgVec::new_in(mcx);
            let nvalid = valid.num_members() as usize;
            substates.try_reserve_exact(nvalid).map_err(|_| mcx.oom(nvalid))?;
            subplan_origin.try_reserve_exact(nvalid).map_err(|_| mcx.oom(nvalid))?;
            // C ExecInitAppend: as_first_partial_plan is the lowest surviving
            // (post-pruning, compacted-space) subplan index that is partial.
            let mut first_partial = nvalid as i32;
            let mut i = valid.next_member(-1);
            while i >= 0 {
                let subplan = ap_plan.appendplans.nth(i as usize);
                if i >= ap_plan.first_partial_plan && (substates.len() as i32) < first_partial {
                    first_partial = substates.len() as i32;
                }
                let state = exec_init_node(Some(subplan), estate, eflags)?
                    .expect("Append subplan list holds plan nodes");
                substates.push(state);
                subplan_origin.push(i);
                i = valid.next_member(i);
            }
            let state = ::nodeappend::exec_init_append(
                ap_plan,
                estate,
                eflags,
                substates.len(),
                first_partial,
                prune_state,
            )?;
            PlanStateNode::Append(::mcx::alloc_in(
                mcx,
                AppendNode { state, substates, subplan_origin, lane_fusible: None },
            )?)
        }
        NodeTag::T_MergeAppend => {
            let mcx = estate.es_query_cxt;
            let ma_plan = node.as_merge_append().unwrap();
            let n_total = ma_plan.mergeplans.len();
            let (prune_state, valid) = if ma_plan.part_prune_index >= 0 {
                let (ps, valid) = ::execpartition::pruning::exec_init_partition_exec_pruning(
                    estate,
                    n_total as i32,
                    ma_plan.part_prune_index,
                    &ma_plan.apprelids,
                )?;
                (ps.map(Box::new), valid)
            } else {
                let mut all = ::types_nodes::bitmapset::Bitmapset::empty();
                if n_total > 0 {
                    ::partprune::bms_add_range(mcx, &mut all, 0, n_total as i32 - 1)?;
                }
                (None, all)
            };
            let mut substates: ::mcx::PgVec<'mcx, PlanStateNode<'mcx>> =
                ::mcx::PgVec::new_in(mcx);
            let mut subplan_origin: ::mcx::PgVec<'mcx, i32> = ::mcx::PgVec::new_in(mcx);
            let nvalid = valid.num_members() as usize;
            substates.try_reserve_exact(nvalid).map_err(|_| mcx.oom(nvalid))?;
            subplan_origin.try_reserve_exact(nvalid).map_err(|_| mcx.oom(nvalid))?;
            let mut i = valid.next_member(-1);
            while i >= 0 {
                let subplan = ma_plan.mergeplans.nth(i as usize);
                let state = exec_init_node(Some(subplan), estate, eflags)?
                    .expect("MergeAppend subplan list holds plan nodes");
                substates.push(state);
                subplan_origin.push(i);
                i = valid.next_member(i);
            }
            let state = ::nodemergeappend::exec_init_merge_append(
                ma_plan,
                estate,
                eflags,
                substates.len(),
                prune_state,
            )?;
            PlanStateNode::MergeAppend(::mcx::alloc_in(
                mcx,
                MergeAppendNode { state, substates, subplan_origin },
            )?)
        }
        NodeTag::T_SubqueryScan => {
            let mcx = estate.es_query_cxt;
            let sq_plan = node.as_subquery_scan().unwrap();
            debug_assert!(
                sq_plan.scan.plan.lefttree.is_none() && sq_plan.scan.plan.righttree.is_none()
            );
            let sub_node = sq_plan.subplan.unwrap_or_else(|| {
                panic!("ExecInitSubqueryScan (nodeSubqueryscan.c): SubqueryScan without a subplan")
            });
            let subplan = exec_init_node(Some(sub_node), estate, eflags)?
                .expect("SubqueryScan subplan initialized");
            let scan_desc = subplan.exec_get_result_type(sub_node.as_plan().unwrap())?;
            let ps_ExprContext = estate.exec_assign_expr_context();
            // Desc carrier only: scan_next repoints it at the subplan's slot.
            let ss_ScanTupleSlot =
                estate.exec_init_extra_tuple_slot(Some(scan_desc), TupleSlotKind::Virtual);
            let mut ss = ::execscan::ScanState {
                qual: None,
                ps_ProjInfo: None,
                ps_ExprContext,
                scanrelid: sq_plan.scan.scanrelid,
                ss_currentRelation: None,
                ss_currentScanDesc: None,
                ss_ScanTupleSlot,
                instr_idx: None,
            };
            // C ExecInitWholeRowVar reaches the subplan tlist through
            // state->parent; here it rides the compile env.
            let sub_tlist = &sub_node.as_plan().unwrap().targetlist;
            ::execscan::exec_assign_scan_projection_info_parent(
                mcx,
                estate,
                &mut ss,
                &sq_plan.scan.plan.targetlist,
                Some(sub_tlist),
            )?;
            ss.qual =
                {
                let pb = estate.param_bind();
                ::executils::with_subplan_compile_env_parent(estate, Some(sub_tlist), |env| {
                    ::execexpr::exec_init_qual_subplans(mcx, &sq_plan.scan.plan.qual, pb, env)
                })?
            };
            PlanStateNode::SubqueryScan(::mcx::alloc_in(
                mcx,
                SubqueryScanNode { ss, subplan: ::mcx::alloc_in(mcx, subplan)? },
            )?)
        }
        NodeTag::T_SetOp => {
            let mcx = estate.es_query_cxt;
            let so_plan = node.as_set_op().unwrap();
            let child_eflags = ::nodesetop::child_eflags(so_plan.strategy, eflags);
            let outer = exec_init_node(so_plan.plan.lefttree, estate, child_eflags)?
                .unwrap_or_else(|| {
                    panic!("ExecInitSetOp (nodeSetOp.c): SetOp without an outer plan")
                });
            let inner = exec_init_node(so_plan.plan.righttree, estate, child_eflags)?
                .unwrap_or_else(|| {
                    panic!("ExecInitSetOp (nodeSetOp.c): SetOp without an inner plan")
                });
            let outer_desc =
                outer.exec_get_result_type(so_plan.plan.lefttree.unwrap().as_plan().unwrap())?;
            let result_desc = crate::exec_type_from_tl(&so_plan.plan.targetlist)?;
            let state =
                ::nodesetop::exec_init_set_op(so_plan, estate, eflags, &outer_desc, result_desc)?;
            PlanStateNode::SetOp(::mcx::alloc_in(mcx, SetOpNode { state, outer, inner })?)
        }
        NodeTag::T_RecursiveUnion => {
            let mcx = estate.es_query_cxt;
            let ru_plan = node.as_recursive_union().unwrap();
            let result_desc = crate::exec_type_from_tl(&ru_plan.plan.targetlist)?;
            // C order: the wtParam entry is published before child init.
            ::noderecursiveunion::exec_init_recursive_union_shared(ru_plan, estate, result_desc);
            let outer = exec_init_node(ru_plan.plan.lefttree, estate, eflags)?
                .unwrap_or_else(|| {
                    panic!(
                        "ExecInitRecursiveUnion (nodeRecursiveunion.c): RecursiveUnion \
                         without an outer plan"
                    )
                });
            let inner = exec_init_node(ru_plan.plan.righttree, estate, eflags)?
                .unwrap_or_else(|| {
                    panic!(
                        "ExecInitRecursiveUnion (nodeRecursiveunion.c): RecursiveUnion \
                         without an inner plan"
                    )
                });
            let outer_desc = outer
                .exec_get_result_type(ru_plan.plan.lefttree.unwrap().as_plan().unwrap())?;
            let state = ::noderecursiveunion::exec_init_recursive_union(
                ru_plan,
                estate,
                eflags,
                &outer_desc,
            )?;
            PlanStateNode::RecursiveUnion(::mcx::alloc_in(
                mcx,
                RecursiveUnionNode { state, outer, inner },
            )?)
        }
        NodeTag::T_WorkTableScan => {
            let mcx = estate.es_query_cxt;
            let state = ::nodeworktablescan::exec_init_work_table_scan(
                mcx,
                node.as_work_table_scan().unwrap(),
                estate,
                eflags,
            )?;
            PlanStateNode::WorkTableScan(::mcx::alloc_in(mcx, state)?)
        }
        NodeTag::T_NamedTuplestoreScan => {
            let mcx = estate.es_query_cxt;
            let state = ::nodenamedtuplestorescan::exec_init_named_tuplestore_scan(
                mcx,
                node.as_named_tuplestore_scan().unwrap(),
                estate,
                eflags,
            )?;
            PlanStateNode::NamedTuplestoreScan(::mcx::alloc_in(mcx, state)?)
        }
        NodeTag::T_ModifyTable => {
            let mcx = estate.es_query_cxt;
            let mt_plan = node.as_modify_table().unwrap();
            // With RETURNING, setrefs set the visible targetlist to the first
            // RETURNING list; its descriptor shapes the node's result slot.
            let returning_desc = if mt_plan.returningLists.is_nil() {
                None
            } else {
                Some(crate::exec_type_from_tl(&mt_plan.plan.targetlist)?)
            };
            let mt = ::nodemodifytable::exec_init_modify_table(
                mt_plan,
                estate,
                eflags,
                returning_desc,
            )?;
            let subplan = exec_init_node(mt_plan.plan.lefttree, estate, eflags)?
                .expect("ModifyTable has a subplan");
            // EvalPlanQualInit + EvalPlanQualSetPlan; relsubs alloc deferred
            // to first EPQ use (EStateData::epq_ensure).
            let epq = crate::epq::EpqState {
                plan: mt_plan.plan.lefttree,
                recheck: None,
                // Set per-row by the dispatch closure (multi-resultrel).
                result_rti: 0,
                lane_verdicts: None,
            };
            PlanStateNode::ModifyTable(::mcx::alloc_in(
                mcx,
                ModifyTablePlanState { mt, subplan, epq },
            )?)
        }
        NodeTag::T_Gather => {
            let mcx = estate.es_query_cxt;
            let g_plan = node.as_gather().unwrap();
            let outer = exec_init_node(g_plan.plan.lefttree, estate, eflags)?
                .unwrap_or_else(|| {
                    panic!("ExecInitGather (nodeGather.c): Gather without an outer plan")
                });
            let state = crate::nodegather::exec_init_gather(g_plan, estate, &outer)?;
            PlanStateNode::Gather(::mcx::alloc_in(
                mcx,
                GatherNode { state, outer: ::mcx::alloc_in(mcx, outer)? },
            )?)
        }
        NodeTag::T_GatherMerge => {
            let mcx = estate.es_query_cxt;
            let gm_plan = node.as_gather_merge().unwrap();
            let outer = exec_init_node(gm_plan.plan.lefttree, estate, eflags)?
                .unwrap_or_else(|| {
                    panic!(
                        "ExecInitGatherMerge (nodeGatherMerge.c): GatherMerge without \
                         an outer plan"
                    )
                });
            let state =
                crate::nodegathermerge::exec_init_gather_merge(gm_plan, estate, &outer)?;
            PlanStateNode::GatherMerge(::mcx::alloc_in(
                mcx,
                GatherMergeNode { state, outer: ::mcx::alloc_in(mcx, outer)? },
            )?)
        }
        tag => unported_nodes!(tag, {
            T_ValuesScan => "nodeValuesscan.c",
            T_NamedTuplestoreScan => "nodeNamedtuplestorescan.c",
            T_CustomScan => "nodeCustom.c",
            T_Material => "nodeMaterial.c",
            T_WindowAgg => "nodeWindowAgg.c",
            T_Hash => "nodeHash.c",
            T_LockRows => "nodeLockRows.c",
        }),
    };
    for sp_node in &node.as_plan().expect("plan-tree node").initPlan {
        let sp = sp_node.as_sub_plan().expect("initPlan cell is a SubPlan");
        crate::nodesubplan::exec_init_sub_plan(sp, estate)?;
    }
    if estate.es_instrument != 0 {
        return Ok(Some(instrument_node(result, node, estate)?));
    }
    Ok(Some(result))
}

// C: `result->instrument = InstrAlloc(1, estate->es_instrument, ...)`.
fn instrument_node<'mcx>(
    mut inner: PlanStateNode<'mcx>,
    node: Node<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PlanStateNode<'mcx>> {
    let id = node.as_plan().expect("plan-tree node").plan_node_id;
    let idx = usize::try_from(id).expect("plan_node_id is non-negative");
    if estate.es_instrumentation.len() <= idx {
        let grow = idx + 1 - estate.es_instrumentation.len();
        estate
            .es_instrumentation
            .try_reserve(grow)
            .map_err(|_| estate.es_query_cxt.oom(grow))?;
        estate
            .es_instrumentation
            .resize(idx + 1, ::types_core::instrument::Instrumentation::default());
    }
    ::instrument::instr_init(&mut estate.es_instrumentation[idx], estate.es_instrument);
    // InstrCountFiltered1/2 target for the scan driver.
    if let Some(ss) = scan_state_of(&mut inner) {
        ss.instr_idx = Some(idx as u32);
    }
    // InstrCountFiltered1 target for the Agg HAVING qual (nodeAgg.c).
    if let PlanStateNode::Agg(aps) = &mut inner {
        aps.agg.instr_idx = Some(idx as u32);
    }
    Ok(PlanStateNode::Instrumented(::mcx::alloc_in(
        estate.es_query_cxt,
        InstrumentedNode { inner, instr_idx: idx as u32 },
    )?))
}

fn scan_state_of<'a, 'mcx>(
    node: &'a mut PlanStateNode<'mcx>,
) -> Option<&'a mut ::execscan::ScanState<'mcx>> {
    match node {
        PlanStateNode::SeqScan(ss) => Some(&mut ss.ss),
        PlanStateNode::SampleScan(ss) => Some(&mut ss.ss),
        PlanStateNode::FunctionScan(fs) => Some(&mut fs.ss),
        PlanStateNode::ValuesScan(vs) => Some(&mut vs.ss),
        PlanStateNode::ForeignScan(fs) => Some(&mut fs.ss),
        PlanStateNode::TableFuncScan(ts) => Some(&mut ts.ss),
        PlanStateNode::CteScan(cs) => Some(&mut cs.ss),
        PlanStateNode::WorkTableScan(wts) => Some(&mut wts.ss),
        PlanStateNode::NamedTuplestoreScan(nts) => Some(&mut nts.ss),
        PlanStateNode::SubqueryScan(s) => Some(&mut s.ss),
        PlanStateNode::IndexScan(is) => Some(&mut is.ss),
        PlanStateNode::TidScan(ts) => Some(&mut ts.ss),
        PlanStateNode::TidRangeScan(ts) => Some(&mut ts.ss),
        PlanStateNode::IndexOnlyScan(ios) => Some(&mut ios.ss),
        PlanStateNode::BitmapHeapScan(b) => Some(&mut b.scan.ss),
        _ => None,
    }
}

/// `ExecProcNode`: one match over the closed node set. Every arm is an
/// `#[inline(never)]` helper: inner nodes recurse here per row, and one
/// inlined arm grows every recursion's frame to the union of all arms
/// (fullscan gate profile: 11 saved pairs + 1.4KB frame per fetched tuple).
pub fn exec_proc_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    match node {
        PlanStateNode::Instrumented(w) => exec_proc_node_instr(w, estate),
        PlanStateNode::Result(rs) => result_arm(rs, estate),
        PlanStateNode::ProjectSet(ps) => project_set_arm(ps, estate),
        PlanStateNode::SeqScan(ss) => seq_scan_arm(ss, estate),
        PlanStateNode::SampleScan(ss) => sample_scan_arm(ss, estate),
        PlanStateNode::FunctionScan(fs) => function_scan_arm(fs, estate),
        PlanStateNode::ValuesScan(vs) => values_scan_arm(vs, estate),
        PlanStateNode::ForeignScan(fs) => foreign_scan_arm(fs, estate),
        PlanStateNode::TableFuncScan(ts) => table_func_scan_arm(ts, estate),
        PlanStateNode::CteScan(cs) => cte_scan_arm(cs, estate),
        PlanStateNode::IndexScan(is) => index_scan_arm(is, estate),
        PlanStateNode::TidScan(ts) => tid_scan_arm(ts, estate),
        PlanStateNode::TidRangeScan(ts) => tid_range_scan_arm(ts, estate),
        PlanStateNode::IndexOnlyScan(ios) => index_only_scan_arm(ios, estate),
        PlanStateNode::Agg(aps) => agg_arm(aps, estate),
        PlanStateNode::WindowAgg(w) => window_agg_arm(w, estate),
        PlanStateNode::Sort(s) => sort_arm(s, estate),
        PlanStateNode::IncrementalSort(s) => incremental_sort_arm(s, estate),
        PlanStateNode::Material(m) => material_arm(m, estate),
        PlanStateNode::Memoize(m) => memoize_arm(m, estate),
        PlanStateNode::Unique(u) => unique_arm(u, estate),
        PlanStateNode::Group(g) => group_arm(g, estate),
        PlanStateNode::Limit(l) => limit_arm(l, estate),
        PlanStateNode::LockRows(l) => lockrows_arm(l, estate),
        PlanStateNode::BitmapHeapScan(b) => bitmap_heap_scan_arm(b, estate),
        PlanStateNode::BitmapIndexScan(_)
        | PlanStateNode::BitmapAnd(_)
        | PlanStateNode::BitmapOr(_) => {
            panic!("bitmap-producing node does not support ExecProcNode call convention")
        }
        PlanStateNode::ModifyTable(mps) => modify_table_arm(mps, estate),
        PlanStateNode::Append(a) => append_arm(a, estate),
        PlanStateNode::MergeAppend(m) => merge_append_arm(m, estate),
        PlanStateNode::SubqueryScan(s) => subquery_scan_arm(s, estate),
        PlanStateNode::SetOp(s) => set_op_arm(s, estate),
        PlanStateNode::RecursiveUnion(ru) => recursive_union_arm(ru, estate),
        PlanStateNode::WorkTableScan(wts) => work_table_scan_arm(wts, estate),
        PlanStateNode::NamedTuplestoreScan(nts) => named_tuplestore_scan_arm(nts, estate),
        PlanStateNode::NestLoop(nl) => nest_loop_arm(nl, estate),
        PlanStateNode::HashJoin(hj) => hash_join_arm(hj, estate),
        PlanStateNode::MergeJoin(mj) => merge_join_arm(mj, estate),
        PlanStateNode::Gather(g) => gather_arm(g, estate),
        PlanStateNode::GatherMerge(gm) => gather_merge_arm(gm, estate),
    }
}

#[inline(never)]
fn gather_arm<'mcx>(
    g: &mut PgBox<'mcx, GatherNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let g = &mut **g;
    crate::nodegather::exec_gather(&mut g.state, &mut g.outer, estate)
}

#[inline(never)]
fn gather_merge_arm<'mcx>(
    gm: &mut PgBox<'mcx, GatherMergeNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let gm = &mut **gm;
    crate::nodegathermerge::exec_gather_merge(&mut gm.state, &mut gm.outer, estate)
}

type ProcResult = PgResult<Option<ExecSlotId>>;

// ===========================================================================
// Fused-arm retirement P2 force knobs (docs/design/flip-ladder.md §5; wave-4
// tierA A4 commit 1 — the contractual FIRST commit of the retirement track).
//
// Each surviving M-era fused batched drive gets a default-ON
// `PGRUST_FUSED_ARM_<NAME>` env gate: `=0`/`off` forces THAT arm off (the P1
// A/B measurement lever AND the post-retirement revert lever; OQ3 one
// purpose per knob). Default (env absent or any other value) = ON —
// behavior-identical to today at default config by construction. These
// knobs die only at P3, each with its arm's body (flip-ladder §5) — arm #5
// SORT_FEED passed P3 (se/deletion-prep C1: knob + body deleted; the
// `PGRUST_FUSED_ARM_SORT_FEED` spelling is now inert env), and arms #2
// AGG_INDEX + #3 AGG_IOS passed P3 (se/deletion-prep SE-AGG arm-deletions:
// knob + body deleted, the lane agg-over-index_source / index_only_source
// seams re-drive them; the `PGRUST_FUSED_ARM_AGG_INDEX` and
// `PGRUST_FUSED_ARM_AGG_IOS` spellings are now inert env). Registry:
// notes/se-phase0-integration.md R-KNOBS
// (`PGRUST_FUSED_ARM_<NAME>` family, WS-P).
//
// Cost discipline (se2-cost law): one relaxed byte load + compare on the
// gate line; the env resolve is `#[cold]`-outlined and runs once per arm per
// process.
// ===========================================================================

/// The surviving fused arms, flip-ladder §5 table order (arm #5 SORT_FEED
/// deleted at P3 — se/deletion-prep C1; arms #2 AGG_INDEX + #3 AGG_IOS
/// deleted at P3 — se/deletion-prep SE-AGG arm-deletions; each knob died
/// with the body). Discriminant = the per-arm cell index.
#[derive(Clone, Copy)]
enum FusedArm {
    AggSeq = 0,
    AggBitmap = 1,
    HashBuildProj = 2,
    HashBuild = 3,
}

impl FusedArm {
    /// The `PGRUST_FUSED_ARM_<NAME>` env suffix (flip-ladder §5 spelling).
    fn env_suffix(self) -> &'static str {
        match self {
            FusedArm::AggSeq => "AGG_SEQ",
            FusedArm::AggBitmap => "AGG_BITMAP",
            FusedArm::HashBuildProj => "HASH_BUILD_PROJ",
            FusedArm::HashBuild => "HASH_BUILD",
        }
    }
}

/// Per-arm tri-state cells: 0 = unresolved (read env on first use), 1 =
/// forced OFF (`=0`/`off`), 2 = ON (the default). The rowmode.rs AtomicU8
/// idiom, one cell per arm so a forced-off arm never perturbs another's
/// resolve.
static FUSED_ARMS: [core::sync::atomic::AtomicU8; 4] =
    [const { core::sync::atomic::AtomicU8::new(0) }; 4];

/// The P2 gate read: `true` = the fused arm may engage (today's behavior).
#[inline]
fn fused_arm_enabled(arm: FusedArm) -> bool {
    use core::sync::atomic::Ordering::Relaxed;
    match FUSED_ARMS[arm as usize].load(Relaxed) {
        1 => false,
        2 => true,
        _ => fused_arm_resolve(arm),
    }
}

#[cold]
#[inline(never)]
fn fused_arm_resolve(arm: FusedArm) -> bool {
    use core::sync::atomic::Ordering::Relaxed;
    let forced_off = matches!(
        std::env::var(format!("PGRUST_FUSED_ARM_{}", arm.env_suffix())).as_deref(),
        Ok("0") | Ok("off")
    );
    FUSED_ARMS[arm as usize].store(if forced_off { 1 } else { 2 }, Relaxed);
    !forced_off
}

/// Same-process A/B lever for the unit corpus.
#[cfg(test)]
pub(crate) fn fused_arm_set_for_tests(env_suffix: &str, on: bool) {
    use core::sync::atomic::Ordering::Relaxed;
    let arm = [
        FusedArm::AggSeq,
        FusedArm::AggBitmap,
        FusedArm::HashBuildProj,
        FusedArm::HashBuild,
    ]
    .into_iter()
    .find(|a| a.env_suffix() == env_suffix)
    .unwrap_or_else(|| panic!("unknown fused arm: {env_suffix}"));
    FUSED_ARMS[arm as usize].store(if on { 2 } else { 1 }, Relaxed);
}

#[inline(never)]
fn result_arm<'mcx>(rs: &mut ResultState<'mcx>, estate: &mut EStateData<'mcx>) -> ProcResult {
    // Lane-executor-v2 dispatch hook (wave-4 glue: the no-FROM row / the
    // projection stream over the sort breaker): falls through to the
    // UNCHANGED exec_result on refuse. Lane logic + refuse-set in `lanev2`.
    if crate::lanev2::enabled() {
        if let Some(r) = crate::lanev2::try_own_result(rs, estate)? {
            return Ok(r);
        }
    }
    exec_result(rs, estate)
}

#[inline(never)]
fn project_set_arm<'mcx>(
    ps: &mut PgBox<'mcx, ProjectSetState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook (row-mode facility, Phase 0): the
    // default-OFF `PGRUST_LANE_V2_ROWMODE` shape (`ProjectSet ← childless
    // Result`, the no-FROM SRF tlist) — knob OFF this ticks the documented
    // wholesale refuse exactly as before and falls through to the UNCHANGED
    // exec_project_set. Lane logic + refuse-set in `lanev2` (rowmode.rs).
    if crate::lanev2::enabled() {
        if let Some(r) = crate::lanev2::try_own_project_set(ps, estate)? {
            return Ok(r);
        }
    }
    exec_project_set(ps, estate)
}

#[inline(never)]
fn seq_scan_arm<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 (Phase 1) dispatch hook: when enabled, the lane may
    // *own* this SeqScan; all lane logic + the refuse-set live in `lanev2`.
    // On refuse this falls through to the UNCHANGED per-tuple path.
    if crate::lanev2::enabled() {
        if let Some(r) = crate::lanev2::try_own_seq_scan(ss, estate)? {
            return Ok(r);
        }
    }
    ::nodeseqscan::exec_seq_scan(ss, estate)
}

#[inline(never)]
fn sample_scan_arm<'mcx>(
    ss: &mut PgBox<'mcx, ::nodesamplescan::SampleScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook: wave-2 row-mode tail delegation behind
    // PGRUST_LANE_V2_ROWMODE, and (wave-3 WS-Q) the T3 SOURCE form behind
    // PGRUST_LANE_V2_SCANS_T3 — source form probes first inside try_own_*.
    // Falls through to the UNCHANGED per-tuple path on refuse. Lane logic +
    // refuse-set live in `lanev2` (rowmode_tail.rs / tail_source.rs).
    if crate::lanev2::rowmode_tail_active() || crate::lanev2::scans_t3_active() {
        if let Some(r) = crate::lanev2::try_own_sample_scan(ss, estate)? {
            return Ok(r);
        }
    }
    ::nodesamplescan::exec_sample_scan(ss, estate)
}

#[inline(never)]
fn function_scan_arm<'mcx>(
    fs: &mut PgBox<'mcx, ::nodefunctionscan::FunctionScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook: wave-2 row-mode tail delegation behind
    // PGRUST_LANE_V2_ROWMODE, and (wave-3 WS-Q) the T3 SOURCE form behind
    // PGRUST_LANE_V2_SCANS_T3 — source form probes first inside try_own_*.
    // Falls through to the UNCHANGED per-tuple path on refuse. Lane logic +
    // refuse-set live in `lanev2` (rowmode_tail.rs / tail_source.rs).
    if crate::lanev2::rowmode_tail_active() || crate::lanev2::scans_t3_active() {
        if let Some(r) = crate::lanev2::try_own_function_scan(fs, estate)? {
            return Ok(r);
        }
    }
    ::nodefunctionscan::exec_function_scan(fs, estate)
}

#[inline(never)]
fn table_func_scan_arm<'mcx>(
    ts: &mut PgBox<'mcx, ::nodetablefuncscan::TableFuncScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook: wave-2 row-mode tail delegation behind
    // PGRUST_LANE_V2_ROWMODE, and (wave-3 WS-Q) the T3 SOURCE form behind
    // PGRUST_LANE_V2_SCANS_T3 — source form probes first inside try_own_*.
    // Falls through to the UNCHANGED per-tuple path on refuse. Lane logic +
    // refuse-set live in `lanev2` (rowmode_tail.rs / tail_source.rs).
    if crate::lanev2::rowmode_tail_active() || crate::lanev2::scans_t3_active() {
        if let Some(r) = crate::lanev2::try_own_table_func_scan(ts, estate)? {
            return Ok(r);
        }
    }
    ::nodetablefuncscan::exec_table_func_scan(ts, estate)
}

#[inline(never)]
fn values_scan_arm<'mcx>(
    vs: &mut PgBox<'mcx, ::nodevaluesscan::ValuesScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook (wave-2 row-mode tail delegation behind
    // PGRUST_LANE_V2_ROWMODE): falls through to the UNCHANGED per-tuple path
    // on refuse. Lane logic + refuse-set live in `lanev2` (rowmode_tail.rs).
    if crate::lanev2::rowmode_tail_active() {
        // SH-E ownership verdict: accounting only — the call below IS the
        // delegated body (tail-call shape preserved on both knob arms).
        crate::lanev2::values_scan_pull_verdict(estate);
    }
    ::nodevaluesscan::exec_values_scan(vs, estate)
}

#[inline(never)]
fn foreign_scan_arm<'mcx>(
    fs: &mut PgBox<'mcx, ::nodeforeignscan::ForeignScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    ::nodeforeignscan::exec_foreign_scan(fs, estate)
}

#[inline(never)]
fn cte_scan_arm<'mcx>(
    cs: &mut PgBox<'mcx, ::nodectescan::CteScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook (wave-2 row-mode tail delegation behind
    // PGRUST_LANE_V2_ROWMODE): falls through to the UNCHANGED per-tuple path
    // on refuse. Lane logic + refuse-set live in `lanev2` (rowmode_tail.rs).
    if crate::lanev2::rowmode_tail_active() {
        // SH-E ownership verdict (accounting only; single body below).
        crate::lanev2::cte_scan_pull_verdict(estate);
    }
    ::nodectescan::exec_cte_scan(cs, estate)
}

#[inline(never)]
fn index_scan_arm<'mcx>(
    is: &mut ::nodeindexscan::IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook: on refuse this falls through to the
    // UNCHANGED per-tuple path. All lane logic + refuse-set live in `lanev2`.
    if crate::lanev2::enabled() {
        if let Some(r) = crate::lanev2::try_own_index_scan(is, estate)? {
            return Ok(r);
        }
    }
    ::nodeindexscan::exec_index_scan(is, estate)
}

#[inline(never)]
fn tid_scan_arm<'mcx>(
    ts: &mut ::nodetidscan::TidScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook: wave-2 row-mode tail delegation behind
    // PGRUST_LANE_V2_ROWMODE, and (wave-3 WS-Q) the T3 SOURCE form behind
    // PGRUST_LANE_V2_SCANS_T3 — source form probes first inside try_own_*.
    // Falls through to the UNCHANGED per-tuple path on refuse. Lane logic +
    // refuse-set live in `lanev2` (rowmode_tail.rs / tail_source.rs).
    if crate::lanev2::rowmode_tail_active() || crate::lanev2::scans_t3_active() {
        if let Some(r) = crate::lanev2::try_own_tid_scan(ts, estate)? {
            return Ok(r);
        }
    }
    ::nodetidscan::exec_tid_scan(ts, estate)
}

#[inline(never)]
fn tid_range_scan_arm<'mcx>(
    ts: &mut ::nodetidrangescan::TidRangeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook: wave-2 row-mode tail delegation behind
    // PGRUST_LANE_V2_ROWMODE, and (wave-3 WS-Q) the T3 SOURCE form behind
    // PGRUST_LANE_V2_SCANS_T3 — source form probes first inside try_own_*.
    // Falls through to the UNCHANGED per-tuple path on refuse. Lane logic +
    // refuse-set live in `lanev2` (rowmode_tail.rs / tail_source.rs).
    if crate::lanev2::rowmode_tail_active() || crate::lanev2::scans_t3_active() {
        if let Some(r) = crate::lanev2::try_own_tid_range_scan(ts, estate)? {
            return Ok(r);
        }
    }
    ::nodetidrangescan::exec_tid_range_scan(ts, estate)
}

#[inline(never)]
fn index_only_scan_arm<'mcx>(
    ios: &mut ::nodeindexonlyscan::IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook: falls through to the UNCHANGED per-tuple
    // path on refuse. Lane logic + refuse-set live in `lanev2`.
    if crate::lanev2::enabled() {
        if let Some(r) = crate::lanev2::try_own_index_only_scan(ios, estate)? {
            return Ok(r);
        }
    }
    ::nodeindexonlyscan::exec_index_only_scan(ios, estate)
}

#[inline(never)]
fn agg_arm<'mcx>(
    aps: &mut PgBox<'mcx, AggPlanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    let aps = &mut **aps;
    let AggPlanState { agg, outer, lane_choice, lane_stage_slot, lane_exprkey } = aps;
    // EA-on-morsels dispatch (docs/design/ea-morsels.md §5): under EXPLAIN
    // ANALYZE every child is an `Instrumented` wrapper, so the concrete-
    // variant arms below cannot match — which is CORRECT for the serial
    // fused drives (they rely on the mismatch to keep EA C-exact), but the
    // runtime arms' EA admission (whose workers run uninstrumented) must
    // still get its walk. Peel ONE wrapper layer for the LANE HOOK ONLY:
    // the serial lane arms re-refuse instrumented shapes themselves
    // (instr_idx gates inside seq_scan_fusible / decide paths), so only the
    // runtime EA walks can own the node; a refusal falls through to the
    // unchanged per-tuple instrumented exec below, byte-identically.
    if estate.es_instrument != 0 && crate::lanev2::enabled() {
        if let PlanStateNode::Instrumented(w) = &mut *outer {
            match &mut w.inner {
                PlanStateNode::SeqScan(ss) => {
                    if let Some(r) = crate::lanev2::try_own_agg_over_seq_scan(
                        agg,
                        ss,
                        lane_choice,
                        lane_stage_slot,
                        lane_exprkey,
                        estate,
                    )? {
                        return Ok(r);
                    }
                }
                PlanStateNode::Sort(s) => {
                    // The DISTINCT sink's dedicated EA walk (its serial
                    // dispatch sits behind the sort fusibility memo, which
                    // rightly refuses instrumented trees).
                    if let Some(r) =
                        crate::lanev2::try_own_sorted_distinct_runtime_ea(agg, s, estate)?
                    {
                        return Ok(r);
                    }
                }
                _ => {}
            }
        }
    }
    match outer {
        PlanStateNode::SeqScan(ss) => {
            // Lane-executor-v2 dispatch hook (Phase-2 hash-agg breaker):
            // falls through to the UNCHANGED fused/per-tuple agg paths on
            // refuse. Lane logic + refuse-set live in `lanev2`.
            if crate::lanev2::enabled() {
                if let Some(r) =
                    crate::lanev2::try_own_agg_over_seq_scan(
                        agg,
                        ss,
                        lane_choice,
                        lane_stage_slot,
                        lane_exprkey,
                        estate,
                    )?
                {
                    return Ok(r);
                }
            }
            // P2 gate (flip-ladder §5 arm #1): PGRUST_FUSED_ARM_AGG_SEQ.
            if fused_arm_enabled(FusedArm::AggSeq)
                && seq_agg_fusible(agg, ss, estate)
                && ::nodeseqscan::seq_scan_batch_supported(ss, estate)?
            {
                // Outer-read-free drains (count(*)) stage the qual column
                // alone (scan-drive precedent): the census reads bits, not
                // the prefix; fallback rows re-check off the stored tuple.
                let qual_only = ::nodeagg::agg_batch_outer_prefix(agg) == Some(0);
                ::nodeseqscan::seq_scan_batch_soa_prepare(
                    ss,
                    estate,
                    fused_soa_prefix(agg, ss).unwrap_or(0),
                    qual_only,
                    false,
                    false,
                );
                let outer_slot = ss.ss.ss_ScanTupleSlot;
                let src = SeqScanBatchSource { ss, outer_slot };
                return ::nodeagg::exec_agg_batched(agg, estate, src);
            }
        }
        PlanStateNode::IndexScan(is) => {
            // Lane-executor-v2 dispatch hook (sorted-agg streaming operator
            // over an index-ordered feed): falls through to the UNCHANGED
            // fused/per-tuple paths on refuse. Lane logic + refuse-set live
            // in `lanev2`.
            if crate::lanev2::enabled() {
                if let Some(r) =
                    crate::lanev2::try_own_sorted_agg_over_index_scan(agg, is, estate)?
                {
                    return Ok(r);
                }
                // --- WS-AE (wave-8): AGG_INDEX arm re-earn ---
                // The fused drive below, routed through the
                // BatchGranuleSource storage seam — behind
                // PGRUST_LANE_V2_AGG_INDEXFEED (default ON since the
                // SE8-GATES AE2 flip; `=0`/`off` = permanent kill switch;
                // knob-OFF cost = one cached-bool test). Refuses fall
                // through to the UNCHANGED fused/per-tuple paths,
                // byte-identically (the WS-F IndexOnlyScan hook's posture,
                // one arm up).
                if let Some(r) =
                    crate::lanev2::try_own_agg_over_index_source(agg, is, estate)?
                {
                    return Ok(r);
                }
                // --- end WS-AE (wave-8) ---
            }
            // Fused arm #2 (AGG_INDEX, flip-ladder §5 / PGRUST_FUSED_ARM_AGG_INDEX)
            // DELETED — se/deletion-prep SE-AGG arm-deletions. The WS-AE
            // agg-over-index_source lane seam above re-drives the SAME
            // `exec_agg_batched` kernel over the SAME node primitives,
            // byte-identically (default ON, PGRUST_LANE_V2_AGG_INDEXFEED).
            // RB-C1 superset re-trace (docs/design/se-deletion-runbook.md §5
            // item 1): `index_scan_refuse_reason_ex`'s admission is a SUPERSET
            // of the old fused gate at defaults — Epq / MVCC / qual-proj /
            // runtime-keys / order-by-reorder / desc-order / non-btree map
            // 1:1; the fused arm's uninstrumented-only surface is preserved
            // (Instrumented children never reach this concrete IndexScan
            // match); ScrollMark closed by B2 (SCROLL eflags fence down =>
            // batch_allowed true where the arm was reachable); ParallelGate +
            // zero-block-geometry closed by the AGGIDX-PAR increment
            // (default ON). Refused shapes fall to the UNCHANGED per-tuple
            // `exec_agg` tail below — a correct fallback either way.
        }
        PlanStateNode::IndexOnlyScan(ios) => {
            // Lane-executor-v2 dispatch hook (sorted-agg streaming operator
            // over an index-ordered feed); see the IndexScan arm.
            if crate::lanev2::enabled() {
                if let Some(r) =
                    crate::lanev2::try_own_sorted_agg_over_index_only_scan(agg, &mut **ios, estate)?
                {
                    return Ok(r);
                }
                // WS-F (single-executor Phase 1): the fused drive below,
                // routed through the BatchGranuleSource storage seam —
                // behind PGRUST_LANE_V2_INDEXSOURCE (default OFF; knob-OFF
                // cost = one cached-bool test). Refuses fall through to the
                // UNCHANGED fused/per-tuple paths, byte-identically.
                if let Some(r) =
                    crate::lanev2::try_own_agg_over_index_only_source(agg, &mut **ios, estate)?
                {
                    return Ok(r);
                }
            }
            // Fused arm #3 (AGG_IOS, flip-ladder §5 / PGRUST_FUSED_ARM_AGG_IOS)
            // DELETED — se/deletion-prep SE-AGG arm-deletions. The WS-F
            // agg-over-index_only_source lane seam above re-drives the SAME
            // `exec_agg_batched` kernel over the SAME node primitives,
            // byte-identically (default ON, PGRUST_LANE_V2_INDEXSOURCE since
            // the SE17-GATES flip). RB-C1 superset re-trace
            // (docs/design/se-deletion-runbook.md §5 item 3):
            // `index_only_scan_refuse_reason_ex`'s admission is a SUPERSET of
            // the old fused gate at defaults — Epq / MVCC / qual-proj /
            // runtime-keys / order-by-reorder / desc-order / non-btree map
            // 1:1; the fused arm's uninstrumented-only surface is preserved
            // (Instrumented children never reach this concrete IndexOnlyScan
            // match, and the agg_arm peel covers only SeqScan/Sort);
            // ScrollMark closed by B2 (SCROLL eflags fence down); ParallelGate
            // + zero-block-geometry closed by the SE-AGGIOS increment
            // (INDEXSOURCE_PAR, default ON). Refused shapes fall to the
            // UNCHANGED per-tuple `exec_agg` tail below — a correct fallback
            // either way.
        }
        PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            // bitmap-morsels: the runtime bitmap-heap arm (morselized claims
            // over the frozen shared bitmap at DOP N). Falls through to the
            // UNCHANGED fused/per-tuple paths on refuse; when the bitmap was
            // built and the geometry floor refused, the classic setup
            // already ran and the fused drive below skips its own. Lane
            // logic + refuse-set live in `lanev2::runtime_bitmap`.
            if crate::lanev2::enabled() {
                if let Some(r) =
                    crate::lanev2::try_own_agg_over_bitmap_heap_scan(agg, b, estate)?
                {
                    return Ok(r);
                }
                // --- SE-AGGBITMAP: AGG_BITMAP arm re-host (deletion-prep
                // arm #4). The fused drive below, hosted at the lane
                // chokepoint — behind PGRUST_LANE_V2_AGG_BITMAP (default
                // OFF; knob-OFF cost = one cached-bool test). Serial AND
                // parallel-aware (the shared-iterator setup runs below the
                // seam). Refuses fall through to the UNCHANGED
                // fused/per-tuple paths, byte-identically (the WS-AE
                // agg-over-IndexScan hook's posture, two arms up).
                if let Some(r) =
                    crate::lanev2::try_own_agg_over_bitmap_feed(agg, b, estate)?
                {
                    return Ok(r);
                }
                // --- end SE-AGGBITMAP ---
            }
            // P2 gate (flip-ladder §5 arm #4): PGRUST_FUSED_ARM_AGG_BITMAP.
            if fused_arm_enabled(FusedArm::AggBitmap)
                && agg_fusible_common(agg, estate)
                && b.scan.ss.qual.is_none()
                && b.scan.ss.ps_ProjInfo.is_none()
            {
                if !b.scan.initialized {
                    bitmap_table_scan_setup_dispatch(b, estate)?;
                }
                let outer_slot = b.scan.ss.ss_ScanTupleSlot;
                let src = BitmapScanBatchSource { bhs: &mut b.scan, outer_slot };
                return ::nodeagg::exec_agg_batched(agg, estate, src);
            }
        }
        PlanStateNode::Sort(s) => {
            // Lane-executor-v2 dispatch hooks: the skip-sort exact-DISTINCT
            // drive (AGG_PLAIN whose every transition replays from a set —
            // the Sort's only observable effect is the dedup, so it is
            // skipped), then the sorted-agg streaming operator over the sort
            // breaker. Both fall through to the UNCHANGED per-tuple exec_agg
            // over exec_sort on refuse. Lane logic + refuse-sets in `lanev2`.
            if crate::lanev2::enabled() {
                if let Some(r) =
                    crate::lanev2::try_own_plain_distinct_agg_over_sort(agg, s, estate)?
                {
                    return Ok(r);
                }
                if let Some(r) = crate::lanev2::try_own_sorted_agg_over_sort(agg, s, estate)? {
                    return Ok(r);
                }
            }
        }
        // GatherMerge outers take the catch-all: the lane-v2-pardistinct
        // GM-hybrid leader drives were DELETED at Phase-5 D1 — agg over
        // GatherMerge always runs the per-tuple exec_agg over
        // exec_gather_merge path below (which stays until Phase-5 D5).
        PlanStateNode::MergeJoin(mj) => {
            // GL-MJSORT-FOLD dispatch hook (the merge-join duplicate-band
            // fold lever, PGRUST_RUNTIME_MJSORT_FOLD, default OFF, layered
            // under the car's kill): AGG-level ownership exactly like the
            // hashjoin arm — the car's phases 1-3 run verbatim, then the
            // joined pairs fold into partial-agg states on the pool. Falls
            // through to the UNCHANGED per-tuple agg over exec_merge_join
            // on refuse, and a fold-gate refusal leaves the MJ node's own
            // dispatch hook (the plain car) fully armed. Lane logic +
            // refuse-set in `lanev2::runtime_mergejoin`.
            if crate::lanev2::enabled() {
                if let Some(r) =
                    crate::lanev2::try_own_agg_over_merge_join(agg, &mut **mj, estate)?
                {
                    return Ok(r);
                }
            }
        }
        PlanStateNode::HashJoin(hj) => {
            // Lane-executor-v2 dispatch hook (Phase-2 breaker-to-breaker
            // composition: hash-agg breaker over the hash-join breaker over
            // lane scans). Falls through to the UNCHANGED per-tuple agg over
            // exec_hash_join on refuse. Lane logic + refuse-set in `lanev2`.
            if crate::lanev2::enabled() {
                if let Some(r) =
                    crate::lanev2::try_own_agg_over_hash_join(agg, hj, lane_stage_slot, estate)?
                {
                    return Ok(r);
                }
            }
        }
        PlanStateNode::NestLoop(nl) => {
            // Runtime NL-inner-index arm (lanev2/runtime_nlindex.rs): a
            // plain-Agg root over NestLoop(heap SeqScan, btree IndexScan)
            // executed with the OUTER side morselized across the runtime
            // gang, each helper driving private inner index probes.
            // FORCED/explicit and DEFAULT OFF (PGRUST_RUNTIME_NLINDEX=1 +
            // pgrust.runtime_nlindex_pool); dispatched BEFORE the serial
            // lane arm (the runtime_bitmap precedent). Falls through on
            // refuse — byte-identically, nothing consumed.
            if crate::lanev2::enabled() {
                if let Some(r) =
                    crate::lanev2::runtime_nlindex::try_own_plain_agg_runtime_nl_index(
                        agg, nl, estate,
                    )?
                {
                    return Ok(r);
                }
            }
            // Lane-executor-v2 dispatch hook (§4: hash-agg breaker over the
            // NestLoop TupleOp over a lane outer scan; the inner stays
            // Volcano). Falls through to the UNCHANGED per-tuple agg over
            // exec_nest_loop on refuse. Lane logic + refuse-set in `lanev2`.
            if crate::lanev2::enabled() {
                if let Some(r) = crate::lanev2::try_own_agg_over_nest_loop(agg, nl, estate)? {
                    return Ok(r);
                }
            }
        }
        PlanStateNode::Gather(g) => {
            // Lane-executor-v2 dispatch hook (agg-over-gather: the leader-
            // side hash-agg breaker fed by the gather machinery as a source;
            // the workers stay row-path). Falls through to the UNCHANGED
            // per-tuple agg over exec_gather on refuse. Lane logic +
            // refuse-set in `lanev2`.
            if crate::lanev2::enabled() {
                if let Some(r) =
                    crate::lanev2::try_own_agg_over_gather(agg, g, lane_stage_slot, estate)?
                {
                    return Ok(r);
                }
            }
        }
        PlanStateNode::SubqueryScan(sqs) => {
            // Lane-executor-v2 dispatch hook (wave-4 glue: hash-agg breaker
            // over a SubqueryScan over lane scans — pipelines chaining
            // through the subquery boundary). Falls through to the UNCHANGED
            // per-tuple agg over exec_scan on refuse. Lane logic + refuse-set
            // in `lanev2`.
            if crate::lanev2::enabled() {
                if let Some(r) =
                    crate::lanev2::try_own_agg_over_subquery_scan(agg, sqs, estate)?
                {
                    return Ok(r);
                }
            }
        }
        PlanStateNode::Append(apn) => {
            // PARTWISE-MORSELS dispatch hook (night/partitionwise-morsels,
            // knob-gated default OFF): plain fold agg over a partitioned
            // table's serial Append — the runtime scan arm's partition-as-
            // morsel engagement (lanev2/runtime_partwise.rs). Falls through
            // to the UNCHANGED per-tuple agg over exec_append on refuse.
            if crate::lanev2::enabled() {
                if let Some(r) =
                    crate::lanev2::try_own_agg_over_append(agg, &mut **apn, estate)?
                {
                    return Ok(r);
                }
            }
        }
        PlanStateNode::Agg(child) => {
            // GL-ALPHA1-EMIT-1 dispatch hook (PGRUST_LANE_AGG_EMIT_BATCH,
            // default OFF): plain Agg over a hashed-Agg child — drain the
            // child's runtime-sink ADOPTED EMIT in per-bucket blocks instead
            // of the per-emitted-row pull chain. Falls through to the
            // UNCHANGED per-tuple agg over the child's own dispatch on
            // refuse, byte-identically.
            if crate::lanev2::enabled() {
                if let Some(r) = crate::lanev2::try_own_plain_agg_over_agg_emit(
                    agg,
                    &mut **child,
                    estate,
                )? {
                    return Ok(r);
                }
            }
        }
        _ => {}
    }
    ::nodeagg::exec_agg(agg, estate, |e| exec_proc_node(outer, e))
}

// Shared gate for the fused agg-over-index/bitmap arms: uninstrumented
// (Instrumented children are a different variant), forward, no EPQ, MVCC
// snapshot, batch-drainable agg shape.
fn agg_fusible_common<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    estate: &EStateData<'mcx>,
) -> bool {
    !estate.es_epq_active
        && ::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
        && estate
            .es_snapshot
            .as_deref()
            .is_some_and(::types_snapshot::IsMVCCSnapshot)
        && ::nodeagg::agg_batch_drainable(agg)
}

// Fused agg-over-seqscan page-batch drive (upstream batch executor, CF 6176):
// same tuples, same transition order; per-tuple node recursion elided.
// Instrumented children never match the SeqScan arm, so EXPLAIN ANALYZE
// keeps the per-tuple drive and its filter counters.
pub(crate) fn seq_agg_fusible<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> bool {
    use ::execexpr::{Kernel, SlotSrc};
    if estate.es_epq_active
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
        || !::nodeagg::agg_batch_drainable(agg)
    {
        return false;
    }
    // Only allocation-free kernel quals run under the fused drive.
    let kernel_qual = || match ss.ss.qual.as_deref().map(|q| q.kernel()) {
        Some(Kernel::QualScanVarCmpConst { .. }) => true,
        Some(Kernel::QualVarCmpVar { a_src, b_src, .. }) => {
            a_src == SlotSrc::Scan && b_src == SlotSrc::Scan
        }
        _ => false,
    };
    // Projected scans (CP_SMALL_TLIST — the qual'd count(*) plan shape) fuse
    // only for outer-read-free drains: the drain skips the projection, which
    // is unobservable exactly when the agg reads no outer column and the
    // tlist carries no subplan/param (Var-only projections evaluate nothing).
    let outer_read_free = || {
        ::nodeagg::agg_batch_outer_prefix(agg) == Some(0)
            && ss.ss.ps_ProjInfo.as_ref().is_some_and(|p| {
                !p.pi_state.has_subplan() && p.pi_state.param_exec_deps().is_empty()
            })
    };
    match ss.variant() {
        ::nodeseqscan::SeqScanVariant::Plain => true,
        ::nodeseqscan::SeqScanVariant::WithQual => kernel_qual(),
        ::nodeseqscan::SeqScanVariant::WithProject => outer_read_free(),
        ::nodeseqscan::SeqScanVariant::WithQualProject => outer_read_free() && kernel_qual(),
        _ => false,
    }
}

// Deform prefix for the fused drive's SoA page-batch deform: everything the
// per-row consumers read from the scan's output slot.
fn fused_soa_prefix<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
) -> Option<i32> {
    let mut p = ::nodeagg::agg_batch_outer_prefix(agg)?;
    if let Some(q) = ss.ss.qual.as_deref() {
        p = p.max(q.max_fetch(::execexpr::SlotSrc::Scan)?);
    }
    Some(p)
}

struct SeqScanBatchSource<'a, 'mcx> {
    ss: &'a mut ::nodeseqscan::SeqScanState<'mcx>,
    outer_slot: ExecSlotId,
}

impl<'mcx> ::nodeagg::AggBatchSource<'mcx> for SeqScanBatchSource<'_, 'mcx> {
    #[inline]
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32> {
        ::nodeseqscan::seq_scan_next_pagebatch(self.ss, estate)
    }

    #[inline]
    fn fetch_tuple(&mut self, i: u32, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        ::nodeseqscan::seq_scan_batch_fetch(self.ss, estate, i)
    }

    #[inline]
    fn outer_slot(&self) -> ExecSlotId {
        self.outer_slot
    }

    #[inline]
    fn has_qual(&self) -> bool {
        self.ss.ss.qual.is_some()
    }

    #[inline]
    fn qualifying_count(
        &mut self,
        estate: &mut EStateData<'mcx>,
        n: u32,
    ) -> PgResult<Option<u32>> {
        ::nodeseqscan::seq_scan_batch_qual_count(self.ss, estate, n)
    }

    #[inline]
    fn skip_words(&self) -> Option<[u64; ::exectuples::SOA_BM_WORDS]> {
        let sel = ::nodeseqscan::seq_scan_batch_skip_sel(self.ss)?;
        let mut out = [0u64; ::exectuples::SOA_BM_WORDS];
        out[..sel.len()].copy_from_slice(sel);
        Some(out)
    }
}

impl<'mcx> ::nodehash::HashBuildBatchSource<'mcx> for SeqScanBatchSource<'_, 'mcx> {
    #[inline]
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32> {
        ::nodeseqscan::seq_scan_next_pagebatch(self.ss, estate)
    }

    #[inline]
    fn fetch_tuple(&mut self, i: u32, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        ::nodeseqscan::seq_scan_batch_fetch(self.ss, estate, i)
    }

    #[inline]
    fn slot(&self) -> ExecSlotId {
        self.outer_slot
    }

    #[inline]
    fn skip_words(&self) -> Option<[u64; ::exectuples::SOA_BM_WORDS]> {
        let sel = ::nodeseqscan::seq_scan_batch_skip_sel(self.ss)?;
        let mut out = [0u64; ::exectuples::SOA_BM_WORDS];
        out[..sel.len()].copy_from_slice(sel);
        Some(out)
    }
}

// Fused hash-build gate: uninstrumented bare SeqScan (Instrumented is a
// different variant), forward, no EPQ, allocation-free kernel qual only,
// subplan/param-free projection (C shrinks the hash inner to the key
// columns, so the projected shape IS the join-lane shape).
fn hash_build_fusible<'mcx>(
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> bool {
    use ::execexpr::{Kernel, SlotSrc};
    if estate.es_epq_active
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
    {
        return false;
    }
    if let Some(p) = ss.ss.ps_ProjInfo.as_ref() {
        if p.pi_state.has_subplan() || !p.pi_state.param_exec_deps().is_empty() {
            return false;
        }
    }
    match ss.variant() {
        ::nodeseqscan::SeqScanVariant::Plain
        | ::nodeseqscan::SeqScanVariant::WithProject => true,
        ::nodeseqscan::SeqScanVariant::WithQual
        | ::nodeseqscan::SeqScanVariant::WithQualProject => {
            match ss.ss.qual.as_deref().map(|q| q.kernel()) {
                Some(Kernel::QualScanVarCmpConst { .. }) => true,
                Some(Kernel::QualVarCmpVar { a_src, b_src, .. }) => {
                    a_src == SlotSrc::Scan && b_src == SlotSrc::Scan
                }
                _ => false,
            }
        }
        ::nodeseqscan::SeqScanVariant::PlainBloom
        | ::nodeseqscan::SeqScanVariant::Epq => false,
    }
}

// Deform prefix the fused hash-build drive reads from the scan slot: the
// projection's FETCHSOME bound (projected shape) or the build-side hash keys
// (bare shape; the minimal-tuple copy reads the buffer tuple image, not the
// deformed prefix), plus the kernel qual.
fn hash_build_soa_prefix<'mcx>(
    hs: &::nodehash::HashState<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
) -> Option<i32> {
    let mut p = match ss.ss.ps_ProjInfo.as_ref() {
        Some(pr) => pr.pi_state.max_fetch(::execexpr::SlotSrc::Scan)?,
        None => hs.build_prefix()?,
    };
    if let Some(q) = ss.ss.qual.as_deref() {
        p = p.max(q.max_fetch(::execexpr::SlotSrc::Scan)?);
    }
    Some(p)
}

struct SeqScanProjBatchSource<'a, 'mcx> {
    ss: &'a mut ::nodeseqscan::SeqScanState<'mcx>,
    result_slot: ExecSlotId,
}

impl<'mcx> ::nodehash::HashBuildBatchSource<'mcx> for SeqScanProjBatchSource<'_, 'mcx> {
    #[inline]
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32> {
        ::nodeseqscan::seq_scan_next_pagebatch(self.ss, estate)
    }

    #[inline]
    fn fetch_tuple(&mut self, i: u32, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        estate.ecxt_mut(self.ss.ss.ps_ExprContext).reset();
        if !::nodeseqscan::seq_scan_batch_fetch(self.ss, estate, i)? {
            return Ok(false);
        }
        let scan_id = self.ss.ss.ss_ScanTupleSlot;
        estate.ecxt_mut(self.ss.ss.ps_ExprContext).ecxt_scantuple = Some(scan_id);
        let mcx = estate.es_query_cxt;
        let proj = self.ss.ss.ps_ProjInfo.as_mut().expect("projected batch source");
        let (scan_slot, result_slot) = ::execscan::slot_pair(estate, scan_id, self.result_slot);
        let mut slots = EvalSlots { scan: Some(scan_slot), inner: None, outer: None };
        ::execexpr::exec_project(&mut proj.pi_state, &mut slots, result_slot, mcx)?;
        Ok(true)
    }

    #[inline]
    fn slot(&self) -> ExecSlotId {
        self.result_slot
    }

    // Skipping a fetch-dead row elides only its ExprContext reset — the
    // interleaved reset frees nothing (nothing allocated since the previous
    // reset), so every surviving fetch sees identical context state.
    #[inline]
    fn skip_words(&self) -> Option<[u64; ::exectuples::SOA_BM_WORDS]> {
        let sel = ::nodeseqscan::seq_scan_batch_skip_sel(self.ss)?;
        let mut out = [0u64; ::exectuples::SOA_BM_WORDS];
        out[..sel.len()].copy_from_slice(sel);
        Some(out)
    }
}

// IndexScanBatchSource DELETED with fused arm #2 (AGG_INDEX): its sole
// consumer was that arm; the lane's SeamIndexAggSource (indexsource.rs)
// re-drives the same primitives now.

// IndexOnlyScanBatchSource DELETED with fused arm #3 (AGG_IOS): its sole
// consumer was that arm; the lane's SeamAggSource over IndexOnlyScanSource
// (indexsource.rs) re-drives the same primitives now.

struct BitmapScanBatchSource<'a, 'mcx> {
    bhs: &'a mut ::nodebitmapheapscan::BitmapHeapScanState<'mcx>,
    outer_slot: ExecSlotId,
}

impl<'mcx> ::nodeagg::AggBatchSource<'mcx> for BitmapScanBatchSource<'_, 'mcx> {
    #[inline]
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32> {
        ::nodebitmapheapscan::bitmap_scan_next_pagebatch(self.bhs, estate)
    }

    #[inline]
    fn fetch_tuple(&mut self, i: u32, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        ::nodebitmapheapscan::bitmap_scan_batch_fetch(self.bhs, estate, i)
    }

    #[inline]
    fn outer_slot(&self) -> ExecSlotId {
        self.outer_slot
    }

    #[inline]
    fn has_qual(&self) -> bool {
        false
    }

    // Lossy/recheck pages apply bitmapqualorig in fetch_tuple.
    #[inline]
    fn storeless_ok(&self) -> bool {
        false
    }
}

#[inline(never)]
fn window_agg_arm<'mcx>(
    w: &mut PgBox<'mcx, WindowAggNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hooks (all default-OFF; on refuse each
    // falls through — ultimately to the UNCHANGED path below). First the
    // Phase-1 W1 batch lane (PGRUST_LANE_V2_WINDOWS, sticky owner of its
    // admitted shapes), then the wave-3 WS-R T2-B framed batch drive
    // (PGRUST_LANE_V2_WINDOWS_T2B, sticky, hosts the framed remainder over
    // admitted sort feeds), then the wave-2 WS-M T2-A row-mode delegation
    // (PGRUST_LANE_V2_WINDOWS_T2, per-pull, hosts everything both batch
    // lanes refused). Lane logic + refuse-sets live in `lanev2::windows`.
    if crate::lanev2::enabled() {
        if let Some(r) = crate::lanev2::try_own_window_agg(w, estate)? {
            return Ok(r);
        }
        // --- WS-R T2-B (wave-3) ---
        if let Some(r) = crate::lanev2::try_own_window_agg_t2b(w, estate)? {
            return Ok(r);
        }
        // --- end WS-R T2-B ---
        if let Some(r) = crate::lanev2::try_own_window_agg_t2(w, estate)? {
            return Ok(r);
        }
    }
    let w = &mut **w;
    let outer = &mut w.outer;
    ::nodewindowagg::exec_window_agg(&mut w.state, estate, |e| exec_proc_node(outer, e))
}

#[inline(never)]
fn sort_arm<'mcx>(s: &mut SortNode<'mcx>, estate: &mut EStateData<'mcx>) -> ProcResult {
    // Lane-executor-v2 dispatch hook: the Phase-2 sort pipeline-breaker.
    // (The lane-v2-pardistinct worker-partial hook that ran first was
    // DELETED at Phase-5 D1 with the GM-hybrid leader drives.) On refuse
    // this falls through to the UNCHANGED paths below. Lane logic +
    // refuse-sets live in `lanev2`.
    if crate::lanev2::enabled() {
        if let Some(r) = crate::lanev2::try_own_sort(s, estate)? {
            return Ok(r);
        }
    }
    // Arm #5 SORT_FEED is DELETED (se/deletion-prep C1): with AD2
    // (`PGRUST_LANE_V2_SORT_RANDOMACCESS`) default ON and the FEED-ONLY fix
    // (0d4bf241c) the lane sort surface is an admission SUPERSET of the old
    // fused gate (child gate admits all qual/projection variants over the
    // parallel-admitting page-batch probe; randomAccess sorts feed through
    // the breaker sink and drain on the row-path tuplesort below). Shapes
    // the lane refuses fall through to the per-tuple drive here —
    // byte-identical by the sortfeed-ra letter (arm C == arm B, −0.04% vs
    // the fused world; notes/se-deletion-prep.md §1 arm #5).
    let SortNode { state, outer, outer_desc, .. } = s;
    let outer_desc = outer_desc.as_ref().expect("Sort already ended").clone();
    ::nodesort::exec_sort(state, estate, outer_desc, |es| exec_proc_node(outer, es))
}

#[inline(never)]
fn incremental_sort_arm<'mcx>(
    s: &mut PgBox<'mcx, IncrementalSortNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    let s = &mut **s;
    let outer = &mut s.outer;
    ::nodeincrementalsort::exec_incremental_sort(&mut s.state, estate, |es| {
        exec_proc_node(outer, es)
    })
}

#[inline(never)]
fn material_arm<'mcx>(
    m: &mut PgBox<'mcx, MaterialNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook (wave-2 row-mode tail delegation behind
    // PGRUST_LANE_V2_ROWMODE): falls through to the UNCHANGED per-tuple path
    // on refuse. Mark/restore enters through execami directly, never through
    // this hosting. Lane logic + refuse-set live in `lanev2` (rowmode_tail.rs).
    if crate::lanev2::rowmode_tail_active() {
        // SH-E ownership verdict: accounting only — the call below IS the
        // delegated body (tail-call shape preserved on both knob arms).
        crate::lanev2::material_pull_verdict(m, estate);
    }
    let m = &mut **m;
    ::nodematerial::exec_material(&mut m.state, &mut *m.outer, estate)
}

#[inline(never)]
fn memoize_arm<'mcx>(
    m: &mut PgBox<'mcx, MemoizeNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook (wave-2 row-mode tail delegation behind
    // PGRUST_LANE_V2_ROWMODE; delegation leaf per the WS-L OQ ruling —
    // lane-owned-child composition is a ledgered later increment): falls
    // through to the UNCHANGED per-tuple path on refuse.
    if crate::lanev2::rowmode_tail_active() {
        // SH-E ownership verdict (accounting only; single body below).
        crate::lanev2::memoize_pull_verdict(m, estate);
    }
    let m = &mut **m;
    let plan = m.state.plan.plan.lefttree.expect("Memoize outer plan");
    let mut outer = MemoizeOuter { node: &mut m.outer, plan, chg: &mut m.outer_chg };
    ::nodememoize::exec_memoize(&mut m.state, &mut outer, estate)
}

// pub(crate) fields: the lanev2 rowmode_tail Memoize delegation leaf
// rebuilds this exact view per pull (memoize-arm-scoped infrastructure).
pub(crate) struct MemoizeOuter<'a, 'mcx> {
    pub(crate) node: &'a mut PlanStateNode<'mcx>,
    pub(crate) plan: Node<'mcx>,
    pub(crate) chg: &'a mut ::types_nodes::bitmapset::Bitmapset<'mcx>,
}

impl<'a, 'mcx> ::nodememoize::MemoizeChild<'mcx> for MemoizeOuter<'a, 'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        if !self.chg.is_empty() {
            let chg = core::mem::replace(
                self.chg,
                ::types_nodes::bitmapset::Bitmapset::empty(),
            );
            crate::execami::exec_re_scan_with_chg(self.node, self.plan, estate, &chg)?;
        }
        exec_proc_node(self.node, estate)
    }
}

#[inline(never)]
fn unique_arm<'mcx>(
    u: &mut PgBox<'mcx, UniqueNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    let u = &mut **u;
    // Lane-executor-v2 dispatch hook (Phase-2 streaming unique over the sort
    // breaker): falls through to the UNCHANGED exec_unique on refuse. Lane
    // logic + refuse-set live in `lanev2`.
    if crate::lanev2::enabled() {
        if let Some(r) = crate::lanev2::try_own_unique(u, estate)? {
            return Ok(r);
        }
    }
    let outer = &mut u.outer;
    ::nodeunique::exec_unique(&mut u.state, estate, |e| exec_proc_node(outer, e))
}

#[inline(never)]
fn group_arm<'mcx>(
    g: &mut PgBox<'mcx, GroupNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook (wave-4 glue: streaming sorted grouping
    // over the sort breaker): falls through to the UNCHANGED exec_group on
    // refuse. Lane logic + refuse-set live in `lanev2`.
    if crate::lanev2::enabled() {
        if let Some(r) = crate::lanev2::try_own_group(g, estate)? {
            return Ok(r);
        }
    }
    let g = &mut **g;
    let outer = &mut g.outer;
    ::nodegroup::exec_group(&mut g.state, estate, |e| exec_proc_node(outer, e))
}

#[inline(never)]
fn limit_arm<'mcx>(l: &mut LimitNode<'mcx>, estate: &mut EStateData<'mcx>) -> ProcResult {
    // Lane-executor-v2 dispatch hook (Phase-2 streaming limit over lane-owned
    // chains): falls through to the UNCHANGED exec_limit on refuse. Lane
    // logic + refuse-set live in `lanev2`.
    if crate::lanev2::enabled() {
        if let Some(r) = crate::lanev2::try_own_limit(l, estate)? {
            return Ok(r);
        }
    }
    let LimitNode { state, outer } = l;
    ::nodelimit::exec_limit(state, &mut **outer, estate)
}

#[inline(never)]
fn lockrows_arm<'mcx>(
    l: &mut PgBox<'mcx, LockRowsNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook (wave-2 row-mode tail delegation behind
    // PGRUST_LANE_V2_ROWMODE; LockRows-without-EPQ — es_epq_active refuses
    // inside; the RowSource closure boundary is the pinned WS-N inc-2b seam,
    // docs/design/rowmode-tail.md §4): falls through to the UNCHANGED
    // per-tuple path on refuse.
    let rowmode_admitted = crate::lanev2::rowmode_tail_active()
        && crate::lanev2::lock_rows_pull_verdict(l, estate);
    // --- WS-T wave-3 inc-2b (LockRows TupleOp behind PGRUST_LANE_V2_DML;
    // lanev2/dml.rs). Offered only when the rowmode-tail verdict did NOT
    // admit — exactly the pulls the retired delegation hook fell through
    // on, so hook priority and the ROWMODE knob's behavior are unchanged
    // at both of its arms; knob-OFF cost is the same one-byte dml_active()
    // gate the modify_table arm carries. Falls through to the UNCHANGED
    // exec_lock_rows on refuse. ---
    if !rowmode_admitted && crate::lanev2::dml_active() {
        if let Some(r) = crate::lanev2::try_own_lock_rows_dml(l, estate)? {
            return Ok(r);
        }
    }
    // --- end WS-T wave-3 inc-2b ---
    let LockRowsNode { state, outer, epq } = &mut **l;
    ::nodelockrows::exec_lock_rows(state, &mut **outer, estate, |subs, e, inputslot| {
        crate::epq::eval_plan_qual(epq, subs, e, inputslot)
    })
}

#[inline(never)]
fn bitmap_heap_scan_arm<'mcx>(
    b: &mut PgBox<'mcx, BitmapHeapPlanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    let b = &mut **b;
    if !b.scan.initialized {
        bitmap_table_scan_setup_dispatch(b, estate)?;
    }
    // Lane-executor-v2 dispatch hook: the bitmap is now built, so the lane may
    // own the heap-scan drive. Falls through to the UNCHANGED per-tuple path on
    // refuse. Lane logic + refuse-set live in `lanev2`.
    if crate::lanev2::enabled() {
        if let Some(r) = crate::lanev2::try_own_bitmap_heap_scan(&mut b.scan, estate)? {
            return Ok(r);
        }
    }
    ::nodebitmapheapscan::exec_bitmap_heap_scan(&mut b.scan, estate)
}

// BitmapTableScanSetup's MultiExec leg (nodeBitmapHeapscan.c): serial always
// builds; parallel builds only in the participant that wins BM_INITIAL.
// pub(crate): the lanev2 sort-breaker feed drives a BitmapHeapScan child
// directly and must run the same setup the arm would.
pub(crate) fn bitmap_table_scan_setup_dispatch<'mcx>(
    b: &mut BitmapHeapPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let build = match b.scan.pstate.as_deref() {
        None => true,
        Some(ps) => ::nodebitmapheapscan::bitmap_should_initialize_shared_state(ps)?,
    };
    let tbm = if build {
        Some(multi_exec_bitmap_node(&mut b.bitmapqual, estate)?)
    } else {
        None
    };
    ::nodebitmapheapscan::bitmap_table_scan_setup(&mut b.scan, estate, tbm)
}

#[inline(never)]
fn modify_table_arm<'mcx>(
    mps: &mut PgBox<'mcx, ModifyTablePlanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook (wave-2 WS-N inc-1: INSERT-no-triggers
    // hosting behind PGRUST_LANE_V2_DML — a delegation over the SAME mt_*
    // seams the fallback below drives): falls through to the UNCHANGED
    // exec_modify_table on refuse. Lane logic + refuse-set live in
    // `lanev2::dml` (the merge_join_arm pattern).
    if crate::lanev2::dml_active() {
        if let Some(r) = crate::lanev2::try_own_modify_table(mps, estate)? {
            return Ok(r);
        }
    }
    let mps = &mut **mps;
    let subplan = &mut mps.subplan;
    let epq = &mut mps.epq;
    // outerPlanState(mtstate)->instrument: MERGE's EPQ list switch adjusts
    // the outer plan's tuple count (InstrUpdateTupleCount leg).
    let outer_instr_idx = match subplan {
        PlanStateNode::Instrumented(w) => Some(w.instr_idx),
        _ => None,
    };
    ::nodemodifytable::exec_modify_table(
        &mut mps.mt,
        estate,
        outer_instr_idx,
        |e| exec_proc_node(subplan, e),
        |subs, e, inputslot, rti| {
            // EvalPlanQualSlot keys by the dispatch-current result relation.
            epq.result_rti = rti;
            crate::epq::eval_plan_qual(epq, subs, e, inputslot)
        },
    )
}

#[inline(never)]
fn append_arm<'mcx>(
    a: &mut PgBox<'mcx, AppendNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook (wave 5: the serial Append over
    // lane-fusible scan children — the node's own exec_append body over lane
    // child pipelines): falls through to the UNCHANGED exec_append on
    // refuse. Lane logic + refuse-set live in `lanev2`.
    if crate::lanev2::enabled() {
        if let Some(r) = crate::lanev2::try_own_append(a, estate)? {
            return Ok(r);
        }
    }
    let AppendNode { state, substates, .. } = &mut **a;
    ::nodeappend::exec_append(state, estate, |e, i| exec_proc_node(&mut substates[i], e))
}

#[inline(never)]
fn merge_append_arm<'mcx>(
    m: &mut PgBox<'mcx, MergeAppendNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook (wave-2 row-mode tail delegation behind
    // PGRUST_LANE_V2_ROWMODE): falls through to the UNCHANGED per-tuple path
    // on refuse. Lane logic + refuse-set live in `lanev2` (rowmode_tail.rs).
    if crate::lanev2::rowmode_tail_active() {
        // SH-E ownership verdict (accounting only; single body below).
        crate::lanev2::merge_append_pull_verdict(m, estate);
    }
    let MergeAppendNode { state, substates, subplan_origin: _ } = &mut **m;
    ::nodemergeappend::exec_merge_append(state, estate, |e, i| {
        exec_proc_node(&mut substates[i], e)
    })
}

#[inline(never)]
fn subquery_scan_arm<'mcx>(
    s: &mut PgBox<'mcx, SubqueryScanNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook (wave-4 glue: pass-through
    // filter/project over the sort breaker): falls through to the UNCHANGED
    // exec_scan on refuse. Lane logic + refuse-set live in `lanev2`.
    if crate::lanev2::enabled() {
        if let Some(r) = crate::lanev2::try_own_subquery_scan(s, estate)? {
            return Ok(r);
        }
    }
    ::execscan::exec_scan(&mut **s, estate)
}

#[inline(never)]
fn set_op_arm<'mcx>(
    s: &mut PgBox<'mcx, SetOpNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook (wave-2 row-mode tail delegation behind
    // PGRUST_LANE_V2_ROWMODE): falls through to the UNCHANGED per-tuple path
    // on refuse. Lane logic + refuse-set live in `lanev2` (rowmode_tail.rs).
    if crate::lanev2::rowmode_tail_active() {
        // SH-E ownership verdict (accounting only; single body below).
        crate::lanev2::set_op_pull_verdict(s, estate);
    }
    let SetOpNode { state, outer, inner } = &mut **s;
    ::nodesetop::exec_set_op(
        state,
        estate,
        |e| exec_proc_node(outer, e),
        |e| exec_proc_node(inner, e),
    )
}

#[inline(never)]
fn recursive_union_arm<'mcx>(
    ru: &mut PgBox<'mcx, RecursiveUnionNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook (wave-2 row-mode tail delegation behind
    // PGRUST_LANE_V2_ROWMODE; the iteration protocol stays inside the ported
    // body — docs/design/rowmode-tail.md §3): falls through to the UNCHANGED
    // per-tuple path on refuse.
    if crate::lanev2::rowmode_tail_active() {
        // SH-E ownership verdict (accounting only; single body below).
        crate::lanev2::recursive_union_pull_verdict(ru, estate);
    }
    let RecursiveUnionNode { state, outer, inner } = &mut **ru;
    ::noderecursiveunion::exec_recursive_union(state, outer, inner, estate)
}

#[inline(never)]
fn work_table_scan_arm<'mcx>(
    wts: &mut PgBox<'mcx, ::nodeworktablescan::WorkTableScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook (wave-2 row-mode tail delegation behind
    // PGRUST_LANE_V2_ROWMODE; shared-slot law — the body resolves rustate
    // from the estate per call): falls through to the UNCHANGED per-tuple
    // path on refuse.
    if crate::lanev2::rowmode_tail_active() {
        // SH-E ownership verdict (accounting only; single body below).
        crate::lanev2::work_table_scan_pull_verdict(estate);
    }
    ::nodeworktablescan::exec_work_table_scan(wts, estate)
}

#[inline(never)]
fn named_tuplestore_scan_arm<'mcx>(
    nts: &mut PgBox<'mcx, ::nodenamedtuplestorescan::NamedTuplestoreScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    // Lane-executor-v2 dispatch hook: wave-2 row-mode tail delegation behind
    // PGRUST_LANE_V2_ROWMODE, and (wave-3 WS-Q) the T3 SOURCE form behind
    // PGRUST_LANE_V2_SCANS_T3 — source form probes first inside try_own_*.
    // Falls through to the UNCHANGED per-tuple path on refuse. Lane logic +
    // refuse-set live in `lanev2` (rowmode_tail.rs / tail_source.rs).
    if crate::lanev2::rowmode_tail_active() || crate::lanev2::scans_t3_active() {
        if let Some(r) = crate::lanev2::try_own_named_tuplestore_scan(nts, estate)? {
            return Ok(r);
        }
    }
    ::nodenamedtuplestorescan::exec_named_tuplestore_scan(nts, estate)
}

#[inline(never)]
fn nest_loop_arm<'mcx>(nl: &mut NestLoopNode<'mcx>, estate: &mut EStateData<'mcx>) -> ProcResult {
    // Lane-executor-v2 dispatch hook (§4 NestLoop TupleOp over a lane outer
    // scan; the inner stays Volcano): falls through to the UNCHANGED
    // exec_nest_loop on refuse. Lane logic + refuse-set live in `lanev2`.
    if crate::lanev2::enabled() {
        if let Some(r) = crate::lanev2::try_own_nest_loop(nl, estate)? {
            return Ok(r);
        }
    }
    let NestLoopNode { state, outer, inner, .. } = nl;
    ::nodenestloop::exec_nest_loop(state, &mut **outer, &mut **inner, estate)
}

#[inline(never)]
fn hash_join_arm<'mcx>(
    hj: &mut PgBox<'mcx, HashJoinNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    let hj = &mut **hj;
    if hj.probe_batch.mode == ProbeBatchMode::Unknown {
        let HashJoinNode { state, outer, hash, probe_batch, .. } = hj;
        let HashSubNode { state: hstate, .. } = &mut **hash;
        probe_batch.mode = if hstate.parallel_state().is_some() {
            ProbeBatchMode::Parallel
        } else {
            probe_batch_probe(state, &mut **outer, estate, probe_batch)?
        };
    }
    // Lane-executor-v2 dispatch hook (Phase-2 join breaker, bare). The
    // admission-economics gate — engage only where the legacy fused probe
    // drive does NOT (never preempt the faster existing path) — lives inside
    // `try_own_hash_join` (via `ProbeBatch::mode()`) so its refusals are
    // ticked in the lane accounting. Falls through to the UNCHANGED
    // exec_hash_join on refuse. Lane logic + refuse-set live in `lanev2`.
    if crate::lanev2::enabled() {
        if let Some(r) = crate::lanev2::try_own_hash_join(hj, estate)? {
            return Ok(r);
        }
    }
    let HashJoinNode { state, outer, hash, probe_batch, .. } = hj;
    let HashSubNode { state: hstate, child } = &mut **hash;
    if probe_batch.mode == ProbeBatchMode::On {
        let PlanStateNode::SeqScan(ss) = &mut **outer else {
            unreachable!("probe fusion armed on a non-SeqScan outer")
        };
        let mut src = SeqScanProbeSource { ss, cur: probe_batch };
        return ::nodehashjoin::exec_hash_join(state, &mut src, hstate, &mut **child, estate);
    }
    if probe_batch.mode == ProbeBatchMode::Parallel {
        return ::nodehashjoin::exec_parallel_hash_join(
            state, &mut **outer, hstate, &mut **child, estate,
        );
    }
    ::nodehashjoin::exec_hash_join(state, &mut **outer, hstate, &mut **child, estate)
}

// Probe-drive gate, decided once — EXEC_FLAG_BACKWARD is asserted off at HJ
// init and EPQ trees re-init with es_epq_active set, so the verdict cannot
// flip mid-drive. Instrumented outers never fuse (EXPLAIN ANALYZE keeps the
// per-tuple drive and its counters).
#[inline(never)]
fn probe_batch_probe<'mcx>(
    hjs: &::nodehashjoin::HashJoinState<'mcx>,
    outer: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    pb: &mut ProbeBatch<'mcx>,
) -> PgResult<ProbeBatchMode> {
    use ::execexpr::{Kernel, SlotSrc};
    let PlanStateNode::SeqScan(ss) = outer else { return Ok(ProbeBatchMode::Off) };
    if estate.es_epq_active
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
    {
        return Ok(ProbeBatchMode::Off);
    }
    let variant_ok = match ss.variant() {
        ::nodeseqscan::SeqScanVariant::Plain => true,
        ::nodeseqscan::SeqScanVariant::WithQual => {
            match ss.ss.qual.as_deref().map(|q| q.kernel()) {
                Some(Kernel::QualScanVarCmpConst { .. }) => true,
                Some(Kernel::QualVarCmpVar { a_src, b_src, .. }) => {
                    a_src == SlotSrc::Scan && b_src == SlotSrc::Scan
                }
                _ => false,
            }
        }
        _ => false,
    };
    if !variant_ok || !::nodeseqscan::seq_scan_batch_supported(ss, estate)? {
        return Ok(ProbeBatchMode::Off);
    }
    let mut prefix = hjs.probe_outer_prefix().unwrap_or(0);
    if let Some(q) = ss.ss.qual.as_deref() {
        prefix = match q.max_fetch(SlotSrc::Scan) {
            Some(qp) => prefix.max(qp),
            None => 0,
        };
    }
    let hash_col = hjs.probe_hash_col();
    let force = hash_col.is_some_and(|c| (c as i32) < prefix);
    ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, prefix, false, force, false);
    if let Some(c) = hash_col {
        if ::nodeseqscan::seq_scan_batch_soa(ss).is_some_and(|soa| (c as i32) < soa.ncols() as i32)
        {
            pb.hash_col = c;
            let mcx = estate.es_query_cxt;
            let mut v: PgVec<'mcx, u32> = PgVec::new_in(mcx);
            v.try_reserve_exact(::exectuples::SOA_MAX_ROWS)
                .map_err(|_| mcx.oom(::exectuples::SOA_MAX_ROWS * 4))?;
            pb.hashes = Some(v);
        }
    }
    Ok(ProbeBatchMode::On)
}

struct SeqScanProbeSource<'a, 'mcx> {
    ss: &'a mut ::nodeseqscan::SeqScanState<'mcx>,
    cur: &'a mut ProbeBatch<'mcx>,
}

impl<'mcx> ::nodehashjoin::HashJoinOuter<'mcx> for SeqScanProbeSource<'_, 'mcx> {
    #[inline]
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        loop {
            while self.cur.i < self.cur.n {
                let i = self.cur.i;
                self.cur.i += 1;
                if let Some(f) = self.cur.filter.as_deref() {
                    // Bloom on the staged hash: a miss proves the bucket walk
                    // finds nothing (rows past hashes.len() — the fallback
                    // tail — pass conservatively and re-eval per row).
                    if let Some(&h) =
                        self.cur.hashes.as_deref().and_then(|h| h.get(i as usize))
                    {
                        self.cur.flt_seen += 1;
                        if !f.test(h) {
                            self.cur.flt_drop += 1;
                            continue;
                        }
                    }
                }
                if ::nodeseqscan::seq_scan_batch_fetch(self.ss, estate, i)? {
                    return Ok(Some(self.ss.ss.ss_ScanTupleSlot));
                }
            }
            // Adaptive page-boundary disarm: a near-passthrough filter costs
            // more than it saves on non-selective joins.
            if self.cur.filter.is_some()
                && self.cur.flt_seen >= 1024
                && self.cur.flt_drop < self.cur.flt_seen / 8
            {
                self.cur.filter = None;
            }
            let n = ::nodeseqscan::seq_scan_next_pagebatch(self.ss, estate)?;
            if n == 0 {
                return Ok(None);
            }
            self.cur.n = n;
            self.cur.i = 0;
            if let Some(h) = self.cur.hashes.as_mut() {
                h.clear();
                if let Some(soa) = ::nodeseqscan::seq_scan_batch_soa(self.ss) {
                    let col = self.cur.hash_col as usize;
                    let vals = &soa.col_values(col)[..n as usize];
                    let nulls = &soa.col_isnull(col)[..n as usize];
                    for r in 0..n as usize {
                        // NULL hashes to 0 and fallback rows re-eval per row
                        // — both the Hash32Var kernel's exact behavior.
                        if soa.is_fallback(r as u32) {
                            break;
                        }
                        let hv = if nulls[r] {
                            0
                        } else {
                            ::hashfn::hash_bytes_uint32(vals[r].as_u32())
                        };
                        h.push(hv);
                    }
                }
            }
        }
    }

    fn rescan(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        self.cur.reset_staged();
        ::nodeseqscan::exec_rescan_seq_scan(self.ss, estate)
    }

    // cur.i already advanced past the row exec_proc returned.
    #[inline(always)]
    fn staged_hash(&self) -> Option<u32> {
        let h = self.cur.hashes.as_deref()?;
        h.get((self.cur.i as usize).wrapping_sub(1)).copied()
    }

    // Composed bloom seat: the filter rides the staged columnar hashes, so it
    // arms only when that cover exists (same key column both sides).
    fn set_hash_filter(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        push: Option<::nodehashjoin::ProbeFilterPush<'mcx>>,
    ) -> PgResult<()> {
        self.cur.flt_seen = 0;
        self.cur.flt_drop = 0;
        self.cur.filter = match push {
            Some(p) if self.cur.hashes.is_some() && p.key_attnum == self.cur.hash_col => {
                Some(p.filter)
            }
            _ => None,
        };
        Ok(())
    }

    fn dense_armed(&mut self) {
        self.cur.hashes = None;
    }
}

#[inline(never)]
fn merge_join_arm<'mcx>(
    mj: &mut PgBox<'mcx, MergeJoinNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    let mj = &mut **mj;
    // MJSORT dispatch hook (the "merge join after sort" runtime car,
    // PGRUST_RUNTIME_MJSORT, DEFAULT ON since the GL-MJSORT-1 flip, kill
    // iff exactly 0|off — lanev2/runtime_mergejoin.rs):
    // whole-node ownership (both sorts + the merge run on the morsel
    // runtime; the adopted face serves the joined pairs). One relaxed
    // cached-bool load + compare on the default path; on refuse this
    // falls through byte-identically to the arms below.
    if let Some(r) = crate::lanev2::try_own_merge_join_mjsort(mj, estate)? {
        return Ok(r);
    }
    // WS-MJ1 lane-NATIVE dispatch hook (LANE-MERGEJOIN inc-1, contract §4.1:
    // "one relaxed cached-bool load + compare at the head of the mergejoin
    // dispatch arm" — PGRUST_LANE_V2_MERGEJOIN_NATIVE, default OFF): on
    // refuse this falls through byte-identically to the verdict + Volcano
    // body below (worklog notes/mergejoin-ws-mj1.md §1.5).
    if let Some(r) = crate::lanev2::try_own_merge_join(mj, estate)? {
        return Ok(r);
    }
    // Lane-executor-v2 dispatch hook (Phase-1 row-mode LEAF hosting behind
    // PGRUST_LANE_V2_ROWMODE; both children stay Volcano inside the ported
    // FSM): falls through to the UNCHANGED exec_merge_join on refuse. Lane
    // logic + refuse-set live in `lanev2` (the nest_loop_arm pattern).
    // SH-E ownership verdict: accounting only — the call below IS the
    // delegated body (tail-call shape preserved on both knob arms). SH-F:
    // no arm-level enabled() gate — the MERGEJOIN knob heads the verdict,
    // the GUC rides the fast-admit byte / slow-path head.
    crate::lanev2::merge_join_pull_verdict(mj, estate);
    let MergeJoinNode { state, outer, inner, .. } = mj;
    ::nodemergejoin::exec_merge_join(state, &mut **outer, &mut **inner, estate)
}

/// `ExecProcNodeInstr` (execProcnode.c). Cold-outlined so the uninstrumented
/// dispatch keeps its codegen (zc slope: +18.7 instr/iter as an inline arm,
/// parity like this).
#[cold]
#[inline(never)]
fn exec_proc_node_instr<'mcx>(
    w: &mut PgBox<'mcx, InstrumentedNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let w = &mut **w;
    let idx = w.instr_idx as usize;
    ::instrument::instr_start_node(&mut estate.es_instrumentation[idx]);
    let result = exec_proc_node(&mut w.inner, estate)?;
    let n_tuples = if result.is_some() { 1.0 } else { 0.0 };
    ::instrument::instr_stop_node(&mut estate.es_instrumentation[idx], n_tuples);
    Ok(result)
}

fn init_bitmap_combine<'mcx>(
    bitmapplans: &::types_nodes::list::NodeList<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<BitmapCombineState<'mcx>> {
    let mut substates: ::mcx::PgVec<'mcx, PlanStateNode<'mcx>> =
        ::mcx::PgVec::new_in(estate.es_query_cxt);
    substates
        .try_reserve_exact(bitmapplans.len())
        .map_err(|_| estate.es_query_cxt.oom(bitmapplans.len()))?;
    for subplan in bitmapplans.iter() {
        let state = exec_init_node(Some(subplan), estate, eflags)?
            .expect("BitmapAnd/BitmapOr subplan list holds plan nodes");
        substates.push(state);
    }
    Ok(BitmapCombineState { substates })
}

/// `MultiExecProcNode` (execProcnode.c), bitmap arms only: every consumer in
/// core is a bitmap combiner or BitmapHeapScan.
pub fn multi_exec_bitmap_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<::tidbitmap::TIDBitmap<'mcx>> {
    match node {
        // C MultiExec* nodes self-instrument (nTuples = bitmap insertions).
        PlanStateNode::Instrumented(w) => {
            let w = &mut **w;
            let idx = w.instr_idx as usize;
            ::instrument::instr_start_node(&mut estate.es_instrumentation[idx]);
            let (tbm, n_tuples) = match &mut w.inner {
                PlanStateNode::BitmapIndexScan(biss) => {
                    let mut tbm = ::tidbitmap::TIDBitmap::new(
                        estate.es_query_cxt,
                        init_small::globals::work_mem() as usize * 1024,
                    );
                    let n = ::nodebitmapindexscan::multi_exec_bitmap_index_scan_into(
                        biss, estate, &mut tbm,
                    )?;
                    (tbm, n)
                }
                inner => (multi_exec_bitmap_node(inner, estate)?, 0.0),
            };
            ::instrument::instr_stop_node(&mut estate.es_instrumentation[idx], n_tuples);
            Ok(tbm)
        }
        PlanStateNode::BitmapIndexScan(biss) => {
            ::nodebitmapindexscan::multi_exec_bitmap_index_scan(biss, estate)
        }
        // MultiExecBitmapAnd: intersect, stopping early on an empty result.
        PlanStateNode::BitmapAnd(bc) => {
            let mut result: Option<::tidbitmap::TIDBitmap<'mcx>> = None;
            for sub in bc.substates.iter_mut() {
                let subresult = multi_exec_bitmap_node(sub, estate)?;
                match result.as_mut() {
                    None => result = Some(subresult),
                    Some(r) => r.intersect(&subresult),
                }
                if result.as_ref().is_some_and(|r| r.is_empty()) {
                    break;
                }
            }
            Ok(result.expect("BitmapAnd with no subplans"))
        }
        // MultiExecBitmapOr: BitmapIndexScan children add into the shared
        // result (C's biss_result hand-off); other children get unioned.
        PlanStateNode::BitmapOr(bc) => {
            let mut result: Option<::tidbitmap::TIDBitmap<'mcx>> = None;
            for sub in bc.substates.iter_mut() {
                // C's nodeTag check is on the plan: the hand-off survives the wrapper.
                let (biss_child, instr_idx) = match sub {
                    PlanStateNode::BitmapIndexScan(biss) => (Some(biss), None),
                    PlanStateNode::Instrumented(w) => {
                        let w = &mut **w;
                        let idx = w.instr_idx;
                        match &mut w.inner {
                            PlanStateNode::BitmapIndexScan(biss) => (Some(biss), Some(idx)),
                            _ => (None, None),
                        }
                    }
                    _ => (None, None),
                };
                if let Some(biss) = biss_child {
                    let tbm = result.get_or_insert_with(|| {
                        ::tidbitmap::TIDBitmap::new(
                            estate.es_query_cxt,
                            init_small::globals::work_mem() as usize * 1024,
                        )
                    });
                    if let Some(idx) = instr_idx {
                        ::instrument::instr_start_node(
                            &mut estate.es_instrumentation[idx as usize],
                        );
                    }
                    let n = ::nodebitmapindexscan::multi_exec_bitmap_index_scan_into(
                        biss, estate, tbm,
                    )?;
                    if let Some(idx) = instr_idx {
                        ::instrument::instr_stop_node(
                            &mut estate.es_instrumentation[idx as usize],
                            n,
                        );
                    }
                } else {
                    let subresult = multi_exec_bitmap_node(sub, estate)?;
                    match result.as_mut() {
                        None => result = Some(subresult),
                        Some(r) => r.union(&subresult)?,
                    }
                }
            }
            Ok(result.expect("BitmapOr with no subplans"))
        }
        _ => panic!("MultiExecProcNode: node type does not produce a bitmap"),
    }
}

fn end_base(ps: &mut PlanStateBase<'_>) {
    ps.ps_ResultTupleDesc = None;
    ps.ps_ProjInfo = None;
    ps.qual = None;
}

fn end_scan(ss: &mut ::execscan::ScanState<'_>) {
    ss.qual = None;
    ss.ps_ProjInfo = None;
    ss.ss_currentScanDesc = None;
    ss.ss_currentRelation = None;
}

// Census-exempt owners the per-node end fns don't reach; releasing them here
// is the free_forget precondition (Drop stays the abort path).
fn release_owned(node: &mut PlanStateNode<'_>) {
    match node {
        PlanStateNode::Instrumented(_) => {}
        PlanStateNode::Result(rs) => {
            end_base(&mut rs.ps);
            rs.resconstantqual = None;
        }
        PlanStateNode::ProjectSet(ps) => {
            end_base(&mut ps.ps);
            crate::nodeprojectset::release_project_set(ps);
        }
        PlanStateNode::SeqScan(ss) => {
            end_scan(&mut ss.ss);
            ss.release_parallel();
        }
        PlanStateNode::SampleScan(ss) => end_scan(&mut ss.ss),
        PlanStateNode::FunctionScan(fs) => end_scan(&mut fs.ss),
        PlanStateNode::ValuesScan(vs) => {
            ::nodevaluesscan::exec_end_values_scan(vs);
            end_scan(&mut vs.ss)
        }
        PlanStateNode::ForeignScan(fs) => {
            fs.fdw_state = None;
            end_scan(&mut fs.ss)
        }
        PlanStateNode::TableFuncScan(ts) => end_scan(&mut ts.ss),
        PlanStateNode::CteScan(cs) => end_scan(&mut cs.ss),
        PlanStateNode::WorkTableScan(wts) => end_scan(&mut wts.ss),
        PlanStateNode::NamedTuplestoreScan(nts) => end_scan(&mut nts.ss),
        PlanStateNode::IndexScan(is) => end_scan(&mut is.ss),
        PlanStateNode::TidScan(ts) => end_scan(&mut ts.ss),
        PlanStateNode::TidRangeScan(ts) => end_scan(&mut ts.ss),
        PlanStateNode::IndexOnlyScan(ios) => end_scan(&mut ios.ss),
        PlanStateNode::BitmapHeapScan(b) => end_scan(&mut b.scan.ss),
        PlanStateNode::Sort(s) => s.outer_desc = None,
        PlanStateNode::SubqueryScan(s) => end_scan(&mut s.ss),
        PlanStateNode::Gather(g) => {
            end_base(&mut g.state.ps);
            g.state.pei = None;
            g.state.reader = Vec::new();
        }
        PlanStateNode::GatherMerge(gm) => {
            end_base(&mut gm.state.ps);
            gm.state.pei = None;
            gm.state.reader = Vec::new();
            gm.state.tuple_buffers_release();
        }
        PlanStateNode::HashJoin(hj) => hj.probe_batch.filter = None,
        // Exempt lane_exprkey: heap-owned census/scratch (+ the dicteval
        // memo arena) released here; the arena reset must not forget it.
        PlanStateNode::Agg(a) => a.lane_exprkey = None,
        PlanStateNode::LockRows(_)
        | PlanStateNode::Append(_)
        | PlanStateNode::MergeAppend(_)
        | PlanStateNode::SetOp(_)
        | PlanStateNode::RecursiveUnion(_)
        | PlanStateNode::IncrementalSort(_)
        | PlanStateNode::BitmapIndexScan(_)
        | PlanStateNode::BitmapAnd(_)
        | PlanStateNode::BitmapOr(_)
        | PlanStateNode::ModifyTable(_)
        | PlanStateNode::NestLoop(_)
        | PlanStateNode::MergeJoin(_)
        | PlanStateNode::WindowAgg(_)
        | PlanStateNode::Material(_)
        | PlanStateNode::Memoize(_)
        | PlanStateNode::Unique(_)
        | PlanStateNode::Group(_)
        | PlanStateNode::Limit(_) => {}
    }
}

// Per-node state EXPLAIN reads off the live PlanState tree, as C does.
pub enum InstrExtra {
    Storage(::types_core::instrument::TuplestoreInstrumentation),
    Bitmap(::types_core::instrument::BitmapHeapScanInstrumentation),
    Memoize(::types_core::instrument::MemoizeInstrumentation),
    IndexSearches(u64),
    /// Gather/GatherMerge nworkers_launched (EXPLAIN's Workers Launched).
    WorkersLaunched(i32),
    /// MERGE mt_merge_inserted/updated/deleted (EXPLAIN's Tuples: line).
    MergeCounts([f64; 3]),
}

/// ANALYZE wraps every node, so only Instrumented arms can match the id.
pub fn planstate_instr_extra<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_node_id: u32,
) -> Option<InstrExtra> {
    macro_rules! walk {
        ($($child:expr),+) => {{
            $(if let Some(x) = planstate_instr_extra($child, estate, plan_node_id) {
                return Some(x);
            })+
            None
        }};
    }
    match node {
        PlanStateNode::Instrumented(w) => {
            let w = &mut **w;
            if w.instr_idx == plan_node_id {
                instr_extra_of(&mut w.inner, estate)
            } else {
                planstate_instr_extra(&mut w.inner, estate, plan_node_id)
            }
        }
        PlanStateNode::Agg(aps) => walk!(&mut aps.outer),
        PlanStateNode::ProjectSet(ps) => walk!(&mut ps.outer),
        PlanStateNode::WindowAgg(w) => walk!(&mut w.outer),
        PlanStateNode::Sort(s) => walk!(&mut *s.outer),
        PlanStateNode::IncrementalSort(s) => walk!(&mut s.outer),
        PlanStateNode::Material(m) => walk!(&mut *m.outer),
        PlanStateNode::Memoize(m) => walk!(&mut *m.outer),
        PlanStateNode::Unique(u) => walk!(&mut u.outer),
        PlanStateNode::Group(g) => walk!(&mut g.outer),
        PlanStateNode::Limit(l) => walk!(&mut *l.outer),
        PlanStateNode::NestLoop(nl) => walk!(&mut *nl.outer, &mut *nl.inner),
        PlanStateNode::MergeJoin(mj) => walk!(&mut *mj.outer, &mut *mj.inner),
        PlanStateNode::HashJoin(hj) => {
            let hj = &mut **hj;
            walk!(&mut *hj.outer, &mut *hj.hash.child)
        }
        PlanStateNode::BitmapHeapScan(b) => walk!(&mut b.bitmapqual),
        PlanStateNode::BitmapAnd(bc) | PlanStateNode::BitmapOr(bc) => {
            for sub in bc.substates.iter_mut() {
                if let Some(x) = planstate_instr_extra(sub, estate, plan_node_id) {
                    return Some(x);
                }
            }
            None
        }
        PlanStateNode::Append(a) => {
            for sub in a.substates.iter_mut() {
                if let Some(x) = planstate_instr_extra(sub, estate, plan_node_id) {
                    return Some(x);
                }
            }
            None
        }
        PlanStateNode::MergeAppend(m) => {
            for sub in m.substates.iter_mut() {
                if let Some(x) = planstate_instr_extra(sub, estate, plan_node_id) {
                    return Some(x);
                }
            }
            None
        }
        PlanStateNode::SubqueryScan(s) => walk!(&mut *s.subplan),
        PlanStateNode::Gather(g) => walk!(&mut *g.outer),
        PlanStateNode::GatherMerge(gm) => walk!(&mut *gm.outer),
        PlanStateNode::SetOp(s) => walk!(&mut s.outer, &mut s.inner),
        PlanStateNode::RecursiveUnion(ru) => {
            let ru = &mut **ru;
            walk!(&mut ru.outer, &mut ru.inner)
        }
        PlanStateNode::LockRows(l) => walk!(&mut *l.outer),
        PlanStateNode::ModifyTable(mps) => walk!(&mut mps.subplan),
        // Result's outer child is optional (gating Result has one).
        PlanStateNode::Result(r) => match r.outer.as_mut() {
            Some(o) => walk!(&mut **o),
            None => None,
        },
        PlanStateNode::WorkTableScan(_)
        | PlanStateNode::NamedTuplestoreScan(_)
        | PlanStateNode::SeqScan(_)
        | PlanStateNode::SampleScan(_)
        | PlanStateNode::FunctionScan(_)
        | PlanStateNode::TableFuncScan(_)
        | PlanStateNode::ValuesScan(_)
        | PlanStateNode::ForeignScan(_)
        | PlanStateNode::CteScan(_)
        | PlanStateNode::IndexScan(_)
        | PlanStateNode::TidScan(_)
        | PlanStateNode::TidRangeScan(_)
        | PlanStateNode::IndexOnlyScan(_)
        | PlanStateNode::BitmapIndexScan(_) => None,
    }
}

/// show_foreignscan_info's drive: find the ForeignScanState by plan_node_id
/// (plain EXPLAIN has no Instrumented wrappers, so the match reads the
/// state's plan) and run the provider's explain callback on it.
/// None = no such node in this tree.
pub fn planstate_foreign_explain<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_node_id: i32,
    flags: ::types_nodes::FdwExplainFlags,
    emit: &mut dyn FnMut(&str, ::types_nodes::FdwExplainProp<'_>) -> PgResult<()>,
) -> Option<PgResult<()>> {
    macro_rules! walk {
        ($($child:expr),+) => {{
            $(if let Some(x) = planstate_foreign_explain($child, estate, plan_node_id, flags, emit) {
                return Some(x);
            })+
            None
        }};
    }
    match node {
        PlanStateNode::ForeignScan(fs) => (fs.plan.scan.plan.plan_node_id == plan_node_id)
            .then(|| ::nodeforeignscan::explain_foreign_scan(fs, estate, flags, emit)),
        PlanStateNode::Instrumented(w) => {
            planstate_foreign_explain(&mut w.inner, estate, plan_node_id, flags, emit)
        }
        PlanStateNode::Agg(aps) => walk!(&mut aps.outer),
        PlanStateNode::ProjectSet(ps) => walk!(&mut ps.outer),
        PlanStateNode::WindowAgg(w) => walk!(&mut w.outer),
        PlanStateNode::Sort(s) => walk!(&mut *s.outer),
        PlanStateNode::IncrementalSort(s) => walk!(&mut s.outer),
        PlanStateNode::Material(m) => walk!(&mut *m.outer),
        PlanStateNode::Memoize(m) => walk!(&mut *m.outer),
        PlanStateNode::Unique(u) => walk!(&mut u.outer),
        PlanStateNode::Group(g) => walk!(&mut g.outer),
        PlanStateNode::Limit(l) => walk!(&mut *l.outer),
        PlanStateNode::NestLoop(nl) => walk!(&mut *nl.outer, &mut *nl.inner),
        PlanStateNode::MergeJoin(mj) => walk!(&mut *mj.outer, &mut *mj.inner),
        PlanStateNode::HashJoin(hj) => {
            let hj = &mut **hj;
            walk!(&mut *hj.outer, &mut *hj.hash.child)
        }
        PlanStateNode::BitmapHeapScan(b) => walk!(&mut b.bitmapqual),
        PlanStateNode::BitmapAnd(bc) | PlanStateNode::BitmapOr(bc) => {
            for sub in bc.substates.iter_mut() {
                if let Some(x) = planstate_foreign_explain(sub, estate, plan_node_id, flags, emit) {
                    return Some(x);
                }
            }
            None
        }
        PlanStateNode::Append(a) => {
            for sub in a.substates.iter_mut() {
                if let Some(x) = planstate_foreign_explain(sub, estate, plan_node_id, flags, emit) {
                    return Some(x);
                }
            }
            None
        }
        PlanStateNode::MergeAppend(m) => {
            for sub in m.substates.iter_mut() {
                if let Some(x) = planstate_foreign_explain(sub, estate, plan_node_id, flags, emit) {
                    return Some(x);
                }
            }
            None
        }
        PlanStateNode::SubqueryScan(s) => walk!(&mut *s.subplan),
        PlanStateNode::Gather(g) => walk!(&mut *g.outer),
        PlanStateNode::GatherMerge(gm) => walk!(&mut *gm.outer),
        PlanStateNode::SetOp(s) => walk!(&mut s.outer, &mut s.inner),
        PlanStateNode::RecursiveUnion(ru) => {
            let ru = &mut **ru;
            walk!(&mut ru.outer, &mut ru.inner)
        }
        PlanStateNode::LockRows(l) => walk!(&mut *l.outer),
        PlanStateNode::ModifyTable(mps) => walk!(&mut mps.subplan),
        // Result's outer child is optional (gating Result has one), and a
        // pseudoconstant qual on a foreign table plans Result->ForeignScan.
        PlanStateNode::Result(r) => match r.outer.as_mut() {
            Some(o) => walk!(&mut **o),
            None => None,
        },
        PlanStateNode::WorkTableScan(_)
        | PlanStateNode::NamedTuplestoreScan(_)
        | PlanStateNode::SeqScan(_)
        | PlanStateNode::SampleScan(_)
        | PlanStateNode::FunctionScan(_)
        | PlanStateNode::TableFuncScan(_)
        | PlanStateNode::ValuesScan(_)
        | PlanStateNode::CteScan(_)
        | PlanStateNode::IndexScan(_)
        | PlanStateNode::TidScan(_)
        | PlanStateNode::TidRangeScan(_)
        | PlanStateNode::IndexOnlyScan(_)
        | PlanStateNode::BitmapIndexScan(_) => None,
    }
}

fn instr_extra_of<'mcx>(
    inner: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> Option<InstrExtra> {
    match inner {
        PlanStateNode::Material(m) => {
            ::nodematerial::storage_stats(&mut m.state).map(InstrExtra::Storage)
        }
        PlanStateNode::WindowAgg(w) => {
            ::nodewindowagg::storage_stats(&mut w.state).map(InstrExtra::Storage)
        }
        PlanStateNode::CteScan(cs) => {
            ::nodectescan::storage_stats(cs, estate).map(InstrExtra::Storage)
        }
        PlanStateNode::Memoize(m) => {
            Some(InstrExtra::Memoize(::nodememoize::memoize_stats(&m.state)))
        }
        PlanStateNode::BitmapHeapScan(b) => Some(InstrExtra::Bitmap(
            ::types_core::instrument::BitmapHeapScanInstrumentation {
                exact_pages: b.scan.stats_exact_pages,
                lossy_pages: b.scan.stats_lossy_pages,
            },
        )),
        PlanStateNode::IndexScan(is) => Some(InstrExtra::IndexSearches(
            is.iss_ScanDesc.as_deref().map_or(0, |sd| sd.xs_nsearches),
        )),
        PlanStateNode::IndexOnlyScan(ios) => Some(InstrExtra::IndexSearches(
            ios.ioss_ScanDesc.as_deref().map_or(0, |sd| sd.xs_nsearches),
        )),
        PlanStateNode::BitmapIndexScan(biss) => Some(InstrExtra::IndexSearches(
            biss.biss_ScanDesc.as_deref().map_or(0, |sd| sd.xs_nsearches),
        )),
        PlanStateNode::Gather(g) => {
            Some(InstrExtra::WorkersLaunched(g.state.nworkers_launched))
        }
        PlanStateNode::GatherMerge(gm) => {
            Some(InstrExtra::WorkersLaunched(gm.state.nworkers_launched))
        }
        PlanStateNode::ModifyTable(mps) => Some(InstrExtra::MergeCounts([
            mps.mt.mt_merge_inserted,
            mps.mt.mt_merge_updated,
            mps.mt.mt_merge_deleted,
        ])),
        _ => None,
    }
}

/// `ExecEndNode` (execProcnode.c).
pub fn exec_end_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    exec_end_node_inner(node, estate)?;
    release_owned(node);
    Ok(())
}

fn exec_end_node_inner<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    match node {
        PlanStateNode::Instrumented(w) => exec_end_node(&mut w.inner, estate),
        PlanStateNode::Result(rs) => exec_end_result(rs, estate),
        PlanStateNode::ProjectSet(ps) => exec_end_project_set(ps, estate),
        PlanStateNode::SeqScan(ss) => ::nodeseqscan::exec_end_seq_scan(ss),
        PlanStateNode::SampleScan(ss) => ::nodesamplescan::exec_end_sample_scan(ss),
        PlanStateNode::FunctionScan(fs) => {
            ::nodefunctionscan::exec_end_function_scan(fs);
            Ok(())
        }
        PlanStateNode::ValuesScan(vs) => {
            ::nodevaluesscan::exec_end_values_scan(vs);
            Ok(())
        }
        PlanStateNode::ForeignScan(fs) => ::nodeforeignscan::exec_end_foreign_scan(fs, estate),
        PlanStateNode::TableFuncScan(ts) => {
            ::nodetablefuncscan::exec_end_table_func_scan(ts);
            Ok(())
        }
        PlanStateNode::CteScan(cs) => {
            ::nodectescan::exec_end_cte_scan(cs, estate);
            Ok(())
        }
        PlanStateNode::IndexScan(is) => ::nodeindexscan::exec_end_index_scan(is),
        PlanStateNode::TidScan(ts) => ::nodetidscan::exec_end_tid_scan(ts),
        PlanStateNode::TidRangeScan(ts) => ::nodetidrangescan::exec_end_tid_range_scan(ts),
        PlanStateNode::IndexOnlyScan(ios) => {
            ::nodeindexonlyscan::exec_end_index_only_scan(ios)
        }
        PlanStateNode::Agg(aps) => {
            ::nodeagg::exec_end_agg(&mut aps.agg);
            exec_end_node(&mut aps.outer, estate)
        }
        PlanStateNode::WindowAgg(w) => {
            // Release the lane drive's Tuplestore (fd guard on the spill
            // arm) before the forget path reclaims the bundle.
            w.lane = None;
            ::nodewindowagg::exec_end_window_agg(&mut w.state);
            exec_end_node(&mut w.outer, estate)
        }
        PlanStateNode::Sort(s) => {
            ::nodesort::exec_end_sort(&mut s.state);
            exec_end_node(&mut s.outer, estate)
        }
        PlanStateNode::IncrementalSort(s) => {
            ::nodeincrementalsort::exec_end_incremental_sort(&mut s.state);
            exec_end_node(&mut s.outer, estate)
        }
        PlanStateNode::Material(m) => {
            ::nodematerial::exec_end_material(&mut m.state);
            exec_end_node(&mut m.outer, estate)
        }
        PlanStateNode::Memoize(m) => {
            ::nodememoize::exec_end_memoize(&mut m.state);
            exec_end_node(&mut m.outer, estate)
        }
        PlanStateNode::Unique(u) => {
            ::nodeunique::exec_end_unique(&mut u.state);
            exec_end_node(&mut u.outer, estate)
        }
        PlanStateNode::Group(g) => {
            ::nodegroup::exec_end_group(&mut g.state);
            exec_end_node(&mut g.outer, estate)
        }
        PlanStateNode::Limit(l) => {
            ::nodelimit::exec_end_limit(&mut l.state);
            exec_end_node(&mut l.outer, estate)
        }
        // C ExecEndLockRows: EvalPlanQualEnd + child.
        PlanStateNode::LockRows(l) => {
            let l = &mut **l;
            crate::epq::eval_plan_qual_end(&mut l.epq, &mut l.state.epq_subs, estate)?;
            exec_end_node(&mut l.outer, estate)
        }
        PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            exec_end_node(&mut b.bitmapqual, estate)?;
            ::nodebitmapheapscan::exec_end_bitmap_heap_scan(&mut b.scan)
        }
        PlanStateNode::BitmapIndexScan(biss) => {
            ::nodebitmapindexscan::exec_end_bitmap_index_scan(biss)
        }
        PlanStateNode::BitmapAnd(bc) | PlanStateNode::BitmapOr(bc) => {
            for sub in bc.substates.iter_mut() {
                exec_end_node(sub, estate)?;
            }
            Ok(())
        }
        PlanStateNode::ModifyTable(mps) => {
            let mps = &mut **mps;
            crate::epq::eval_plan_qual_end(&mut mps.epq, &mut mps.mt.epq_subs, estate)?;
            ::nodemodifytable::exec_end_modify_table(&mut mps.mt);
            exec_end_node(&mut mps.subplan, estate)
        }
        PlanStateNode::Append(a) => {
            let a = &mut **a;
            ::nodeappend::exec_end_append(&mut a.state);
            for sub in a.substates.iter_mut() {
                exec_end_node(sub, estate)?;
            }
            Ok(())
        }
        PlanStateNode::MergeAppend(m) => {
            let m = &mut **m;
            ::nodemergeappend::exec_end_merge_append(&mut m.state);
            for sub in m.substates.iter_mut() {
                exec_end_node(sub, estate)?;
            }
            Ok(())
        }
        PlanStateNode::SubqueryScan(s) => exec_end_node(&mut s.subplan, estate),
        // C ExecEndGather: end child first, then shutdown (workers + context).
        PlanStateNode::Gather(g) => {
            let g = &mut **g;
            exec_end_node(&mut g.outer, estate)?;
            crate::nodegather::exec_shutdown_gather(&mut g.state, estate)
        }
        PlanStateNode::GatherMerge(gm) => {
            let gm = &mut **gm;
            exec_end_node(&mut gm.outer, estate)?;
            crate::nodegathermerge::exec_shutdown_gather_merge(&mut gm.state, estate)
        }
        PlanStateNode::WorkTableScan(_) => Ok(()),
        // C frees the exec state only; the tuplestore stays with its
        // registrant (the trigger side owns reldata).
        PlanStateNode::NamedTuplestoreScan(_) => Ok(()),
        PlanStateNode::RecursiveUnion(ru) => {
            let ru = &mut **ru;
            ::noderecursiveunion::exec_end_recursive_union(&mut ru.state, estate);
            exec_end_node(&mut ru.outer, estate)?;
            exec_end_node(&mut ru.inner, estate)
        }
        PlanStateNode::SetOp(s) => {
            let s = &mut **s;
            ::nodesetop::exec_end_set_op(&mut s.state);
            exec_end_node(&mut s.outer, estate)?;
            exec_end_node(&mut s.inner, estate)
        }
        PlanStateNode::NestLoop(nl) => {
            ::nodenestloop::exec_end_nest_loop(&mut nl.state);
            exec_end_node(&mut nl.outer, estate)?;
            exec_end_node(&mut nl.inner, estate)
        }
        PlanStateNode::HashJoin(hj) => {
            let hj = &mut **hj;
            ::nodehashjoin::exec_end_hash_join(&mut hj.state, &mut hj.hash.state, estate)?;
            exec_end_node(&mut hj.outer, estate)?;
            exec_end_node(&mut hj.hash.child, estate)
        }
        PlanStateNode::MergeJoin(mj) => {
            let mj = &mut **mj;
            // MJSORT adopted result: released HERE (the exec_end_sort
            // exemption pattern — the field is forget-exempt in the
            // census below, so the normal end path must drop it).
            mj.mjsort = None;
            ::nodemergejoin::exec_end_merge_join(&mut mj.state);
            exec_end_node(&mut mj.outer, estate)?;
            exec_end_node(&mut mj.inner, estate)
        }
    }
}

/// `ExecShutdownNode` (execProcnode.c). PgResult because the Gather arms wait
/// on workers, whose errors rethrow here (C ereports out of ExecParallelFinish);
/// only the Gather arms can Err, so the caller's ? arm stays predicted-cold.
#[inline]
pub fn exec_shutdown_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    match node {
        PlanStateNode::Instrumented(w) => exec_shutdown_node(&mut w.inner, estate),
        PlanStateNode::Result(rs) => {
            if let Some(outer) = rs.outer.as_deref_mut() {
                exec_shutdown_node(outer, estate)?;
            }
            Ok(())
        }
        PlanStateNode::ProjectSet(ps) => exec_shutdown_node(&mut ps.outer, estate),
        PlanStateNode::SeqScan(_)
        | PlanStateNode::SampleScan(_)
        | PlanStateNode::FunctionScan(_)
        | PlanStateNode::TableFuncScan(_)
        | PlanStateNode::ValuesScan(_)
        | PlanStateNode::ForeignScan(_)
        | PlanStateNode::CteScan(_)
        | PlanStateNode::WorkTableScan(_)
        | PlanStateNode::NamedTuplestoreScan(_)
        | PlanStateNode::IndexScan(_)
        | PlanStateNode::TidScan(_)
        | PlanStateNode::TidRangeScan(_)
        | PlanStateNode::IndexOnlyScan(_)
        | PlanStateNode::BitmapIndexScan(_) => Ok(()),
        PlanStateNode::RecursiveUnion(ru) => {
            let ru = &mut **ru;
            exec_shutdown_node(&mut ru.outer, estate)?;
            exec_shutdown_node(&mut ru.inner, estate)
        }
        PlanStateNode::Agg(aps) => exec_shutdown_node(&mut aps.outer, estate),
        PlanStateNode::WindowAgg(w) => exec_shutdown_node(&mut w.outer, estate),
        PlanStateNode::Sort(s) => exec_shutdown_node(&mut s.outer, estate),
        PlanStateNode::IncrementalSort(s) => exec_shutdown_node(&mut s.outer, estate),
        PlanStateNode::Material(m) => exec_shutdown_node(&mut m.outer, estate),
        PlanStateNode::Memoize(m) => exec_shutdown_node(&mut m.outer, estate),
        PlanStateNode::Unique(u) => exec_shutdown_node(&mut u.outer, estate),
        PlanStateNode::Group(g) => exec_shutdown_node(&mut g.outer, estate),
        PlanStateNode::Limit(l) => exec_shutdown_node(&mut l.outer, estate),
        PlanStateNode::LockRows(l) => exec_shutdown_node(&mut l.outer, estate),
        PlanStateNode::BitmapHeapScan(b) => exec_shutdown_node(&mut b.bitmapqual, estate),
        PlanStateNode::BitmapAnd(bc) | PlanStateNode::BitmapOr(bc) => {
            for sub in bc.substates.iter_mut() {
                exec_shutdown_node(sub, estate)?;
            }
            Ok(())
        }
        PlanStateNode::ModifyTable(mps) => exec_shutdown_node(&mut mps.subplan, estate),
        PlanStateNode::Append(a) => {
            for sub in a.substates.iter_mut() {
                exec_shutdown_node(sub, estate)?;
            }
            Ok(())
        }
        PlanStateNode::MergeAppend(m) => {
            for sub in m.substates.iter_mut() {
                exec_shutdown_node(sub, estate)?;
            }
            Ok(())
        }
        PlanStateNode::SubqueryScan(s) => exec_shutdown_node(&mut s.subplan, estate),
        PlanStateNode::SetOp(s) => {
            let s = &mut **s;
            exec_shutdown_node(&mut s.outer, estate)?;
            exec_shutdown_node(&mut s.inner, estate)
        }
        PlanStateNode::NestLoop(nl) => {
            exec_shutdown_node(&mut nl.outer, estate)?;
            exec_shutdown_node(&mut nl.inner, estate)
        }
        // ExecShutdownHash: hand the table's instrumentation to the estate
        // (C: HashState.hinstrument) before EXPLAIN reads it.
        PlanStateNode::HashJoin(hj) => {
            let hj = &mut **hj;
            ::nodehashjoin::exec_shutdown_hash_join(&hj.state, &mut hj.hash.state, estate)?;
            exec_shutdown_node(&mut hj.outer, estate)?;
            exec_shutdown_node(&mut hj.hash.child, estate)
        }
        PlanStateNode::MergeJoin(mj) => {
            exec_shutdown_node(&mut mj.outer, estate)?;
            exec_shutdown_node(&mut mj.inner, estate)
        }
        // ExecShutdownGather/GatherMerge: reap workers so instrumentation is
        // final before EXPLAIN reads it; the context survives for rescan.
        PlanStateNode::Gather(g) => {
            let g = &mut **g;
            crate::nodegather::exec_shutdown_gather(&mut g.state, estate)?;
            exec_shutdown_node(&mut g.outer, estate)
        }
        PlanStateNode::GatherMerge(gm) => {
            let gm = &mut **gm;
            crate::nodegathermerge::exec_shutdown_gather_merge(&mut gm.state, estate)?;
            exec_shutdown_node(&mut gm.outer, estate)
        }
    }
}

/// `ExecSetTupleBound` (execProcnode.c): Sort gets the bound; Result, Append
/// members, and qual-less SubqueryScan pass it through; every other ported
/// variant is C's silent no-op fall-through (Agg included).
pub fn exec_set_tuple_bound<'mcx>(tuples_needed: i64, node: &mut PlanStateNode<'mcx>) {
    match node {
        PlanStateNode::Instrumented(w) => exec_set_tuple_bound(tuples_needed, &mut w.inner),
        PlanStateNode::Sort(s) => ::nodesort::sort_set_tuple_bound(&mut s.state, tuples_needed),
        PlanStateNode::IncrementalSort(s) => {
            ::nodeincrementalsort::incremental_sort_set_tuple_bound(&mut s.state, tuples_needed)
        }
        PlanStateNode::Result(rs) => {
            if let Some(outer) = rs.outer.as_deref_mut() {
                exec_set_tuple_bound(tuples_needed, outer);
            }
        }
        PlanStateNode::Append(a) => {
            for sub in a.substates.iter_mut() {
                exec_set_tuple_bound(tuples_needed, sub);
            }
        }
        PlanStateNode::MergeAppend(m) => {
            for sub in m.substates.iter_mut() {
                exec_set_tuple_bound(tuples_needed, sub);
            }
        }
        PlanStateNode::SubqueryScan(s) => {
            let s = &mut **s;
            if s.ss.qual.is_none() {
                exec_set_tuple_bound(tuples_needed, &mut s.subplan);
            }
        }
        // C remembers the bound for the workers AND bounds the leader's copy.
        PlanStateNode::Gather(g) => {
            let g = &mut **g;
            g.state.tuples_needed = tuples_needed;
            exec_set_tuple_bound(tuples_needed, &mut g.outer);
        }
        PlanStateNode::GatherMerge(gm) => {
            let gm = &mut **gm;
            gm.state.tuples_needed = tuples_needed;
            exec_set_tuple_bound(tuples_needed, &mut gm.outer);
        }
        _ => {}
    }
}

impl<'mcx> ::nodelimit::LimitChild<'mcx> for PlanStateNode<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        exec_proc_node(self, estate)
    }

    fn set_tuple_bound(&mut self, tuples_needed: i64) {
        exec_set_tuple_bound(tuples_needed, self);
    }
}

impl<'mcx> ::nodelockrows::LockRowsChild<'mcx> for PlanStateNode<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        exec_proc_node(self, estate)
    }
}

impl<'mcx> ::nodematerial::MaterialChild<'mcx> for PlanStateNode<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        exec_proc_node(self, estate)
    }

    fn rescan(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        crate::execami::exec_re_scan(self, estate)
    }
}

impl<'mcx> ::nodenestloop::NestLoopChild<'mcx> for PlanStateNode<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        exec_proc_node(self, estate)
    }

    fn rescan(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        crate::execami::exec_re_scan(self, estate)
    }

    fn rescan_with_chg(
        &mut self,
        plan: ::types_nodes::Node<'mcx>,
        estate: &mut EStateData<'mcx>,
        chg: &::types_nodes::bitmapset::Bitmapset<'mcx>,
    ) -> PgResult<()> {
        crate::execami::exec_re_scan_with_chg(self, plan, estate, chg)
    }
}

impl<'mcx> ::noderecursiveunion::RuChild<'mcx> for PlanStateNode<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        exec_proc_node(self, estate)
    }

    fn rescan_with_chg(
        &mut self,
        plan: ::types_nodes::Node<'mcx>,
        estate: &mut EStateData<'mcx>,
        chg: &::types_nodes::bitmapset::Bitmapset<'mcx>,
    ) -> PgResult<()> {
        crate::execami::exec_re_scan_with_chg(self, plan, estate, chg)
    }
}

impl<'mcx> ::nodememoize::MemoizeChild<'mcx> for PlanStateNode<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        exec_proc_node(self, estate)
    }
}

impl<'mcx> ::nodehashjoin::HashJoinOuter<'mcx> for PlanStateNode<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        exec_proc_node(self, estate)
    }

    fn rescan(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        crate::execami::exec_re_scan(self, estate)
    }

    // Bloom pushdown seat: bare uninstrumented SeqScan outers only
    // (Instrumented is a different variant, so EXPLAIN ANALYZE keeps the
    // per-tuple drive and its row counters). Arm errors surface as PgResult
    // in the scan gate; a failed gate simply leaves the per-tuple drive.
    fn set_hash_filter(
        &mut self,
        estate: &mut EStateData<'mcx>,
        push: Option<::nodehashjoin::ProbeFilterPush<'mcx>>,
    ) -> PgResult<()> {
        if let PlanStateNode::SeqScan(ss) = self {
            ::nodeseqscan::seq_scan_set_bloom(ss, estate, push.map(|p| (p.filter, p.key_attnum)))?;
        }
        Ok(())
    }
}

impl<'mcx> ::nodehash::HashBuildInput<'mcx> for PlanStateNode<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        exec_proc_node(self, estate)
    }

    // Fused hash-build page-batch drive: Instrumented children never match,
    // so EXPLAIN ANALYZE keeps the per-tuple drive and its counters.
    fn multi_exec(
        &mut self,
        hs: &mut ::nodehash::HashState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        if let PlanStateNode::SeqScan(ss) = self {
            // P2 gates (flip-ladder §5 arms #6/#7):
            // PGRUST_FUSED_ARM_HASH_BUILD_PROJ (projected build source) /
            // PGRUST_FUSED_ARM_HASH_BUILD (bare build source). The gate is
            // read BEFORE any drive-side effect (the SoA prepare below runs
            // only for an armed drive), so a forced-off arm takes the
            // per-tuple multi_exec_hash exactly as an unfused shape would.
            let proj_slot = ss.ss.ps_ProjInfo.as_ref().map(|p| p.pi_result_slot);
            let arm_on = match proj_slot {
                Some(_) => fused_arm_enabled(FusedArm::HashBuildProj),
                None => fused_arm_enabled(FusedArm::HashBuild),
            };
            let engaged = arm_on
                && hash_build_fusible(ss, estate)
                && ::nodeseqscan::seq_scan_batch_supported(ss, estate)?;
            // SE-HASHOFF census tick (stats-armed runs only): classify this
            // build event at the arm chokepoint before any drive-side
            // effect. Accounting only — the engage decision above is
            // untouched.
            crate::lanev2::fused_hash_build_census_seq(engaged, proj_slot.is_some());
            if engaged {
                ::nodeseqscan::seq_scan_batch_soa_prepare(
                    ss,
                    estate,
                    hash_build_soa_prefix(hs, ss).unwrap_or(0),
                    false,
                    false,
                    false,
                );
                match proj_slot {
                    Some(result_slot) => {
                        let src = SeqScanProjBatchSource { ss, result_slot };
                        return ::nodehash::multi_exec_hash_batched(hs, src, estate);
                    }
                    None => {
                        let outer_slot = ss.ss.ss_ScanTupleSlot;
                        let src = SeqScanBatchSource { ss, outer_slot };
                        return ::nodehash::multi_exec_hash_batched(hs, src, estate);
                    }
                }
            }
        } else {
            // SE-HASHOFF census: non-SeqScan build child — outside both
            // fused hash-build arms' surface by construction.
            crate::lanev2::fused_hash_build_census_other();
        }
        ::nodehash::multi_exec_hash(hs, self, estate)
    }
}

impl<'mcx> ::nodemergejoin::MergeJoinOuter<'mcx> for PlanStateNode<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        exec_proc_node(self, estate)
    }

    fn rescan(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        crate::execami::exec_re_scan(self, estate)
    }
}

impl<'mcx> ::nodemergejoin::MergeJoinInner<'mcx> for PlanStateNode<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        exec_proc_node(self, estate)
    }

    fn rescan(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        crate::execami::exec_re_scan(self, estate)
    }

    fn mark_pos(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        crate::execami::exec_mark_pos(self, estate)
    }

    fn restr_pos(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        crate::execami::exec_restr_pos(self, estate)
    }
}

// ExprContext slot triple + result slot as disjoint &mut borrows of es_tupleTable.
pub(crate) fn with_eval_slots<'mcx, R>(
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    result: Option<ExecSlotId>,
    f: impl FnOnce(&mut EvalSlots<'_, 'mcx>, Option<&mut SlotData<'mcx>>, Mcx<'mcx>) -> PgResult<R>,
) -> PgResult<R> {
    with_eval_slots_outer(estate, ecxt, result, None, f)
}

/// [`with_eval_slots`] with the Outer slot overridden by the owning node's
/// explicit row (SubplanEvalHook contract: the override lives OUTSIDE
/// es_tupleTable, so it aliases none of the table-derived borrows).
pub(crate) fn with_eval_slots_outer<'mcx, R>(
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    result: Option<ExecSlotId>,
    outer_override: Option<&mut SlotData<'mcx>>,
    f: impl FnOnce(&mut EvalSlots<'_, 'mcx>, Option<&mut SlotData<'mcx>>, Mcx<'mcx>) -> PgResult<R>,
) -> PgResult<R> {
    let mcx = estate.es_query_cxt;
    let (scan, inner, mut outer) = {
        let e = estate.ecxt(ecxt);
        (e.ecxt_scantuple, e.ecxt_innertuple, e.ecxt_outertuple)
    };
    if outer_override.is_some() {
        outer = None;
    }
    let table: &mut [SlotData<'mcx>] = &mut estate.es_tupleTable;
    let ids = [scan, inner, outer, result];
    for (i, id) in ids.iter().enumerate() {
        if let Some(a) = id {
            assert!((a.0 as usize) < table.len(), "slot id out of range");
            for later in &ids[i + 1..] {
                assert!(Some(*a) != *later, "aliased slot ids in expression eval");
            }
        }
    }
    let base = table.as_mut_ptr();
    // SAFETY: indices bounds-checked and pairwise-distinct above, so the four
    // derived &mut are disjoint elements of one live slice.
    let get = |id: Option<ExecSlotId>| id.map(|i| unsafe { &mut *base.add(i.0 as usize) });
    let mut slots = EvalSlots {
        scan: get(scan),
        inner: get(inner),
        outer: match outer_override {
            Some(o) => Some(o),
            None => get(outer),
        },
    };
    f(&mut slots, get(result), mcx)
}

// Exempt fields: released by release_owned/the per-node end fns before
// standard_executor_end forgets the bundle (Drop stays the abort path).
::mcx::forget_safe_nodrop!(ProbeBatchMode);
::mcx::forget_safe_struct!(
    ProbeBatch<'_> { mode, n, i, hash_col, hashes, flt_seen, flt_drop; filter }
);
::mcx::forget_safe_struct!(
    PlanStateBase<'_> { plan, ps_ExprContext, ps_ResultTupleSlot;
        ps_ResultTupleDesc, ps_ProjInfo, qual },
    InstrumentedNode<'_> { inner, instr_idx },
    ModifyTablePlanState<'_> { mt, subplan, epq },
    BitmapHeapPlanState<'_> { scan, bitmapqual },
    BitmapCombineState<'_> { substates },
    AggPlanState<'_> { agg, outer, lane_choice, lane_stage_slot; lane_exprkey },
    WindowAggNode<'_> { state, outer, lane_admit, lane_framed_admit, lane_framed; lane },
    MaterialNode<'_> { state, outer },
    MemoizeNode<'_> { state, outer, outer_chg },
    SortNode<'_> { state, outer, lane_fusible, rd_shape_refused; outer_desc },
    IncrementalSortNode<'_> { state, outer },
    AppendNode<'_> { state, substates, subplan_origin, lane_fusible },
    MergeAppendNode<'_> { state, substates, subplan_origin },
    SubqueryScanNode<'_> { ss, subplan },
    SetOpNode<'_> { state, outer, inner },
    RecursiveUnionNode<'_> { state, outer, inner },
    LockRowsNode<'_> { state, outer, epq },
    LimitNode<'_> { state, outer },
    UniqueNode<'_> { state, outer },
    GroupNode<'_> { state, outer },
    NestLoopNode<'_> { state, outer, inner, lane_fusible },
    HashSubNode<'_> { state, child },
    HashJoinNode<'_> { state, outer, hash, probe_batch, lane_fusible },
    // MergeJoinNode.mjsort exempt: released in exec_end_node's MergeJoin
    // arm and on every rescan (the SortState runtime_full precedent).
    MergeJoinNode<'_> { state, outer, inner, mjsort_probed; mjsort },
    GatherNode<'_> { state, outer },
    GatherMergeNode<'_> { state, outer },
);
::mcx::forget_safe_enum!(
    PlanStateNode<'_> {
        Result(x), SeqScan(x), SampleScan(x), FunctionScan(x), TableFuncScan(x), ValuesScan(x),
        ForeignScan(x), CteScan(x),
        IndexScan(x), TidScan(x), TidRangeScan(x), IndexOnlyScan(x), Agg(x), Sort(x), Material(x),
        IncrementalSort(x), Unique(x), Group(x), Limit(x), BitmapHeapScan(x),
        BitmapIndexScan(x), Append(x), MergeAppend(x), SubqueryScan(x), SetOp(x), LockRows(x),
        BitmapAnd(x), BitmapOr(x), ModifyTable(x), NestLoop(x), HashJoin(x),
        MergeJoin(x), WindowAgg(x), ProjectSet(x), Memoize(x),
        RecursiveUnion(x), WorkTableScan(x), NamedTuplestoreScan(x),
        Gather(x), GatherMerge(x), Instrumented(x),
    },
);

#[cfg(test)]
mod fused_arm_tests {
    use super::*;

    /// P2 knob semantics (flip-ladder §5): every SURVIVING arm resolves ON
    /// by default (env absent in the test process), the test lever flips a
    /// single arm without perturbing the others, and the spellings are the
    /// surviving flip-ladder names exactly (SORT_FEED deleted at P3 —
    /// se/deletion-prep C1; AGG_INDEX + AGG_IOS deleted at P3 —
    /// se/deletion-prep SE-AGG arm-deletions).
    #[test]
    fn fused_arm_knobs_default_on_and_isolate() {
        const ARMS: [FusedArm; 4] = [
            FusedArm::AggSeq,
            FusedArm::AggBitmap,
            FusedArm::HashBuildProj,
            FusedArm::HashBuild,
        ];
        let names: Vec<&str> = ARMS.iter().map(|a| a.env_suffix()).collect();
        assert_eq!(
            names,
            ["AGG_SEQ", "AGG_BITMAP", "HASH_BUILD_PROJ", "HASH_BUILD"]
        );
        for arm in ARMS {
            assert!(
                fused_arm_enabled(arm),
                "PGRUST_FUSED_ARM_{} must default ON (behavior-identical at default)",
                arm.env_suffix()
            );
        }
        fused_arm_set_for_tests("AGG_BITMAP", false);
        assert!(!fused_arm_enabled(FusedArm::AggBitmap));
        for arm in [FusedArm::AggSeq, FusedArm::HashBuild, FusedArm::HashBuildProj] {
            assert!(fused_arm_enabled(arm), "force-off must not leak across arms");
        }
        fused_arm_set_for_tests("AGG_BITMAP", true);
        assert!(fused_arm_enabled(FusedArm::AggBitmap));
    }
}
