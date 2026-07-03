use std::rc::Rc;

use ::execexpr::{EvalSlots, ExprState};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{Mcx, PgBox};
use ::types_error::PgResult;
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::Plan;
use ::types_nodes::NodeTag;
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::TupleDescData;

use crate::noderesult::{exec_end_result, exec_init_result, exec_result, ResultState};

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
    SeqScan(::nodeseqscan::SeqScanState<'mcx>),
    FunctionScan(PgBox<'mcx, ::nodefunctionscan::FunctionScanState<'mcx>>),
    ValuesScan(PgBox<'mcx, ::nodevaluesscan::ValuesScanState<'mcx>>),
    CteScan(PgBox<'mcx, ::nodectescan::CteScanState<'mcx>>),
    IndexScan(::nodeindexscan::IndexScanState<'mcx>),
    IndexOnlyScan(::nodeindexonlyscan::IndexOnlyScanState<'mcx>),
    Agg(PgBox<'mcx, AggPlanState<'mcx>>),
    Sort(SortNode<'mcx>),
    IncrementalSort(PgBox<'mcx, IncrementalSortNode<'mcx>>),
    Material(PgBox<'mcx, MaterialNode<'mcx>>),
    Unique(PgBox<'mcx, UniqueNode<'mcx>>),
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
    SubqueryScan(PgBox<'mcx, SubqueryScanNode<'mcx>>),
    SetOp(PgBox<'mcx, SetOpNode<'mcx>>),
    // Last variant: existing discriminants keep their values, so the
    // uninstrumented jump-table dispatch compiles unchanged.
    Instrumented(PgBox<'mcx, InstrumentedNode<'mcx>>),
}

// The subplans live here (BitmapCombineState precedent; indexed fetch).
pub struct AppendNode<'mcx> {
    pub state: ::nodeappend::AppendState<'mcx>,
    pub substates: ::mcx::PgVec<'mcx, PlanStateNode<'mcx>>,
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

    // SubqueryNext: the subplan's slot goes to the driver uncopied, as C.
    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let Some(id) = exec_proc_node(&mut self.subplan, estate)? else {
            return Ok(false);
        };
        self.ss.ss_ScanTupleSlot = id;
        Ok(true)
    }
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
}

// The WindowAgg node's outer child lives here (nodesort/nodeagg precedent).
pub struct WindowAggNode<'mcx> {
    pub state: ::nodewindowagg::WindowAggStateData<'mcx>,
    pub outer: PlanStateNode<'mcx>,
}

pub struct MaterialNode<'mcx> {
    pub state: ::nodematerial::MaterialState<'mcx>,
    pub outer: PgBox<'mcx, PlanStateNode<'mcx>>,
}

pub struct SortNode<'mcx> {
    pub state: ::nodesort::SortState<'mcx>,
    pub outer: PgBox<'mcx, PlanStateNode<'mcx>>,
    // None only after exec_end_node (released for the forget path).
    pub outer_desc: Option<Rc<TupleDescData<'static>>>,
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

// Both children live here; nodenestloop drives them via NestLoopChild.
pub struct NestLoopNode<'mcx> {
    pub state: ::nodenestloop::NestLoopState<'mcx>,
    pub outer: PgBox<'mcx, PlanStateNode<'mcx>>,
    pub inner: PgBox<'mcx, PlanStateNode<'mcx>>,
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
}

// Both children live here; nodemergejoin drives them via the MergeJoin traits.
pub struct MergeJoinNode<'mcx> {
    pub state: ::nodemergejoin::MergeJoinState<'mcx>,
    pub outer: PgBox<'mcx, PlanStateNode<'mcx>>,
    pub inner: PgBox<'mcx, PlanStateNode<'mcx>>,
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
            PlanStateNode::SeqScan(ss) => Some(ss.ss.ps_ExprContext),
            PlanStateNode::FunctionScan(fs) => Some(fs.ss.ps_ExprContext),
            PlanStateNode::ValuesScan(vs) => Some(vs.ss.ps_ExprContext),
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
            PlanStateNode::Unique(u) => Some(u.state.ps_ExprContext),
            PlanStateNode::Limit(l) => Some(l.state.ps_ExprContext),
            PlanStateNode::LockRows(_) => None,
            PlanStateNode::NestLoop(nl) => Some(nl.state.ps_ExprContext),
            PlanStateNode::HashJoin(hj) => Some(hj.state.ps_ExprContext),
            PlanStateNode::MergeJoin(mj) => Some(mj.state.ps_ExprContext),
            PlanStateNode::WindowAgg(w) => Some(w.state.ps_ExprContext),
            PlanStateNode::BitmapHeapScan(b) => Some(b.scan.ss.ps_ExprContext),
            // C's ExecInitAppend assigns no ExprContext.
            PlanStateNode::Append(_) => None,
            PlanStateNode::SubqueryScan(s) => Some(s.ss.ps_ExprContext),
            PlanStateNode::SetOp(s) => Some(s.state.ps_ExprContext),
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
            PlanStateNode::SeqScan(_)
            | PlanStateNode::FunctionScan(_)
            | PlanStateNode::ValuesScan(_)
            | PlanStateNode::CteScan(_)
            | PlanStateNode::IndexScan(_)
            | PlanStateNode::IndexOnlyScan(_)
            | PlanStateNode::Limit(_)
            | PlanStateNode::LockRows(_)
            | PlanStateNode::BitmapHeapScan(_)
            | PlanStateNode::Append(_)
            | PlanStateNode::SubqueryScan(_) => crate::exec_type_from_tl(&plan.targetlist),
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
            PlanStateNode::NestLoop(nl) => Ok(nl.state.ps_ResultTupleDesc.clone().expect("nest loop already ended")),
            PlanStateNode::HashJoin(hj) => Ok(hj.state.ps_ResultTupleDesc.clone().expect("hash join already ended")),
            PlanStateNode::MergeJoin(mj) => Ok(mj.state.ps_ResultTupleDesc.clone().expect("merge join already ended")),
            PlanStateNode::WindowAgg(w) => Ok(w.state.ps_ResultTupleDesc.clone().expect("window agg already ended")),
            PlanStateNode::SetOp(s) => Ok(s.state.ps_ResultTupleDesc.clone().expect("set op already ended")),
            PlanStateNode::BitmapIndexScan(_)
            | PlanStateNode::BitmapAnd(_)
            | PlanStateNode::BitmapOr(_) => {
                panic!("ExecGetResultType on a bitmap-producing node")
            }
        }
    }
}

macro_rules! unported_nodes {
    ($tag:expr, { $($t:ident => $file:literal),+ $(,)? }) => {
        match $tag {
            $(NodeTag::$t => panic!(concat!(
                "ExecInitNode (execProcnode.c): ", stringify!($t), " (", $file, ") not ported"
            )),)+
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
        NodeTag::T_SeqScan => {
            let mcx = estate.es_query_cxt;
            PlanStateNode::SeqScan(::nodeseqscan::exec_init_seq_scan(
                mcx,
                node.as_seq_scan().unwrap(),
                estate,
                eflags,
            )?)
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
        NodeTag::T_ValuesScan => {
            let mcx = estate.es_query_cxt;
            let state = ::nodevaluesscan::exec_init_values_scan(
                mcx,
                node.as_values_scan().unwrap(),
                estate,
            )?;
            PlanStateNode::ValuesScan(::mcx::alloc_in(mcx, state)?)
        }
        NodeTag::T_CteScan => {
            let mcx = estate.es_query_cxt;
            let cte_plan = node.as_cte_scan().unwrap();
            let idx = (cte_plan.ctePlanId - 1) as usize;
            let scan_desc = {
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
                    .as_plan()
                    .expect("subplans cell is a plan tree");
                sub.exec_get_result_type(sub_plan)?
            };
            let state = ::nodectescan::exec_init_cte_scan(
                mcx, cte_plan, estate, eflags, scan_desc,
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
        NodeTag::T_IndexOnlyScan => {
            let mcx = estate.es_query_cxt;
            PlanStateNode::IndexOnlyScan(::nodeindexonlyscan::exec_init_index_only_scan(
                mcx,
                node.as_index_only_scan().unwrap(),
                estate,
                eflags,
            )?)
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
            if plan.isshared {
                panic!("ExecInitBitmapOr: isshared (parallel bitmap scan lane) not ported");
            }
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
        NodeTag::T_Limit => {
            let limit_plan = node.as_limit().unwrap();
            let outer = exec_init_node(limit_plan.plan.lefttree, estate, eflags)?
                .unwrap_or_else(|| {
                    panic!("ExecInitLimit (nodeLimit.c): Limit without an outer plan")
                });
            let state = ::nodelimit::exec_init_limit(limit_plan, estate, eflags)?;
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
            PlanStateNode::Agg(::mcx::alloc_in(mcx, AggPlanState { agg, outer })?)
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
            PlanStateNode::WindowAgg(::mcx::alloc_in(mcx, WindowAggNode { state, outer })?)
        }
        NodeTag::T_NestLoop => {
            let mcx = estate.es_query_cxt;
            let nl_plan = node.as_nest_loop().unwrap();
            let outer = exec_init_node(nl_plan.join.plan.lefttree, estate, eflags)?
                .unwrap_or_else(|| {
                    panic!("ExecInitNestLoop (nodeNestloop.c): NestLoop without an outer plan")
                });
            // nestParams are loud in exec_init_nest_loop, so the inner child
            // always gets EXEC_FLAG_REWIND (cheap rescans wanted).
            let inner = exec_init_node(
                nl_plan.join.plan.righttree,
                estate,
                eflags | ::types_slot::EXEC_FLAG_REWIND,
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
                |es, idesc, iattnums, ihashfns, colls| {
                    ::nodehash::exec_init_hash(hash_plan_node, es, idesc, iattnums, ihashfns, colls)
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
                },
            )?)
        }
        NodeTag::T_Append => {
            let mcx = estate.es_query_cxt;
            let ap_plan = node.as_append().unwrap();
            let mut substates: ::mcx::PgVec<'mcx, PlanStateNode<'mcx>> =
                ::mcx::PgVec::new_in(mcx);
            substates
                .try_reserve_exact(ap_plan.appendplans.len())
                .map_err(|_| mcx.oom(ap_plan.appendplans.len()))?;
            for subplan in ap_plan.appendplans.iter() {
                let state = exec_init_node(Some(subplan), estate, eflags)?
                    .expect("Append subplan list holds plan nodes");
                substates.push(state);
            }
            let state =
                ::nodeappend::exec_init_append(ap_plan, estate, eflags, substates.len())?;
            PlanStateNode::Append(::mcx::alloc_in(mcx, AppendNode { state, substates })?)
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
            ::execscan::exec_assign_scan_projection_info(
                mcx,
                estate,
                &mut ss,
                &sq_plan.scan.plan.targetlist,
            )?;
            ss.qual =
                ::execexpr::exec_init_qual(mcx, &sq_plan.scan.plan.qual, estate.param_bind())?;
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
                result_rti: mt.result_rti,
            };
            PlanStateNode::ModifyTable(::mcx::alloc_in(
                mcx,
                ModifyTablePlanState { mt, subplan, epq },
            )?)
        }
        tag => unported_nodes!(tag, {
            T_ProjectSet => "nodeProjectSet.c",
            T_MergeAppend => "nodeMergeAppend.c",
            T_RecursiveUnion => "nodeRecursiveunion.c",
            T_SampleScan => "nodeSamplescan.c",
            T_TidScan => "nodeTidscan.c",
            T_TidRangeScan => "nodeTidrangescan.c",
            T_TableFuncScan => "nodeTableFuncscan.c",
            T_ValuesScan => "nodeValuesscan.c",
            T_NamedTuplestoreScan => "nodeNamedtuplestorescan.c",
            T_WorkTableScan => "nodeWorktablescan.c",
            T_ForeignScan => "nodeForeignscan.c",
            T_CustomScan => "nodeCustom.c",
            T_Material => "nodeMaterial.c",
            T_Memoize => "nodeMemoize.c",
            T_Group => "nodeGroup.c",
            T_WindowAgg => "nodeWindowAgg.c",
            T_Gather => "nodeGather.c",
            T_GatherMerge => "nodeGatherMerge.c",
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
        PlanStateNode::FunctionScan(fs) => Some(&mut fs.ss),
        PlanStateNode::ValuesScan(vs) => Some(&mut vs.ss),
        PlanStateNode::CteScan(cs) => Some(&mut cs.ss),
        PlanStateNode::IndexScan(is) => Some(&mut is.ss),
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
        PlanStateNode::SeqScan(ss) => seq_scan_arm(ss, estate),
        PlanStateNode::FunctionScan(fs) => function_scan_arm(fs, estate),
        PlanStateNode::ValuesScan(vs) => values_scan_arm(vs, estate),
        PlanStateNode::CteScan(cs) => cte_scan_arm(cs, estate),
        PlanStateNode::IndexScan(is) => index_scan_arm(is, estate),
        PlanStateNode::IndexOnlyScan(ios) => index_only_scan_arm(ios, estate),
        PlanStateNode::Agg(aps) => agg_arm(aps, estate),
        PlanStateNode::WindowAgg(w) => window_agg_arm(w, estate),
        PlanStateNode::Sort(s) => sort_arm(s, estate),
        PlanStateNode::IncrementalSort(s) => incremental_sort_arm(s, estate),
        PlanStateNode::Material(m) => material_arm(m, estate),
        PlanStateNode::Unique(u) => unique_arm(u, estate),
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
        PlanStateNode::SubqueryScan(s) => subquery_scan_arm(s, estate),
        PlanStateNode::SetOp(s) => set_op_arm(s, estate),
        PlanStateNode::NestLoop(nl) => nest_loop_arm(nl, estate),
        PlanStateNode::HashJoin(hj) => hash_join_arm(hj, estate),
        PlanStateNode::MergeJoin(mj) => merge_join_arm(mj, estate),
    }
}

type ProcResult = PgResult<Option<ExecSlotId>>;

#[inline(never)]
fn result_arm<'mcx>(rs: &mut ResultState<'mcx>, estate: &mut EStateData<'mcx>) -> ProcResult {
    exec_result(rs, estate)
}

#[inline(never)]
fn seq_scan_arm<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    ::nodeseqscan::exec_seq_scan(ss, estate)
}

#[inline(never)]
fn function_scan_arm<'mcx>(
    fs: &mut PgBox<'mcx, ::nodefunctionscan::FunctionScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    ::nodefunctionscan::exec_function_scan(fs, estate)
}

#[inline(never)]
fn values_scan_arm<'mcx>(
    vs: &mut PgBox<'mcx, ::nodevaluesscan::ValuesScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    ::nodevaluesscan::exec_values_scan(vs, estate)
}

#[inline(never)]
fn cte_scan_arm<'mcx>(
    cs: &mut PgBox<'mcx, ::nodectescan::CteScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    ::nodectescan::exec_cte_scan(cs, estate)
}

#[inline(never)]
fn index_scan_arm<'mcx>(
    is: &mut ::nodeindexscan::IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    ::nodeindexscan::exec_index_scan(is, estate)
}

#[inline(never)]
fn index_only_scan_arm<'mcx>(
    ios: &mut ::nodeindexonlyscan::IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    ::nodeindexonlyscan::exec_index_only_scan(ios, estate)
}

#[inline(never)]
fn agg_arm<'mcx>(
    aps: &mut PgBox<'mcx, AggPlanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    let aps = &mut **aps;
    let outer = &mut aps.outer;
    ::nodeagg::exec_agg(&mut aps.agg, estate, |e| exec_proc_node(outer, e))
}

#[inline(never)]
fn window_agg_arm<'mcx>(
    w: &mut PgBox<'mcx, WindowAggNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    let w = &mut **w;
    let outer = &mut w.outer;
    ::nodewindowagg::exec_window_agg(&mut w.state, estate, |e| exec_proc_node(outer, e))
}

#[inline(never)]
fn sort_arm<'mcx>(s: &mut SortNode<'mcx>, estate: &mut EStateData<'mcx>) -> ProcResult {
    let SortNode { state, outer, outer_desc } = s;
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
    let m = &mut **m;
    ::nodematerial::exec_material(&mut m.state, &mut *m.outer, estate)
}

#[inline(never)]
fn unique_arm<'mcx>(
    u: &mut PgBox<'mcx, UniqueNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    let u = &mut **u;
    let outer = &mut u.outer;
    ::nodeunique::exec_unique(&mut u.state, estate, |e| exec_proc_node(outer, e))
}

#[inline(never)]
fn limit_arm<'mcx>(l: &mut LimitNode<'mcx>, estate: &mut EStateData<'mcx>) -> ProcResult {
    let LimitNode { state, outer } = l;
    ::nodelimit::exec_limit(state, &mut **outer, estate)
}

#[inline(never)]
fn lockrows_arm<'mcx>(
    l: &mut PgBox<'mcx, LockRowsNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    let LockRowsNode { state, outer, epq } = &mut **l;
    ::nodelockrows::exec_lock_rows(state, &mut **outer, estate, |e, inputslot| {
        crate::epq::eval_plan_qual(epq, e, inputslot)
    })
}

#[inline(never)]
fn bitmap_heap_scan_arm<'mcx>(
    b: &mut PgBox<'mcx, BitmapHeapPlanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    let b = &mut **b;
    if !b.scan.initialized {
        let tbm = multi_exec_bitmap_node(&mut b.bitmapqual, estate)?;
        ::nodebitmapheapscan::bitmap_table_scan_setup(&mut b.scan, estate, tbm)?;
    }
    ::nodebitmapheapscan::exec_bitmap_heap_scan(&mut b.scan, estate)
}

#[inline(never)]
fn modify_table_arm<'mcx>(
    mps: &mut PgBox<'mcx, ModifyTablePlanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    let mps = &mut **mps;
    let subplan = &mut mps.subplan;
    let epq = &mut mps.epq;
    ::nodemodifytable::exec_modify_table(
        &mut mps.mt,
        estate,
        |e| exec_proc_node(subplan, e),
        |e, inputslot| crate::epq::eval_plan_qual(epq, e, inputslot),
    )
}

#[inline(never)]
fn append_arm<'mcx>(
    a: &mut PgBox<'mcx, AppendNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    let AppendNode { state, substates } = &mut **a;
    ::nodeappend::exec_append(state, estate, |e, i| exec_proc_node(&mut substates[i], e))
}

#[inline(never)]
fn subquery_scan_arm<'mcx>(
    s: &mut PgBox<'mcx, SubqueryScanNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    ::execscan::exec_scan(&mut **s, estate)
}

#[inline(never)]
fn set_op_arm<'mcx>(
    s: &mut PgBox<'mcx, SetOpNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    let SetOpNode { state, outer, inner } = &mut **s;
    ::nodesetop::exec_set_op(
        state,
        estate,
        |e| exec_proc_node(outer, e),
        |e| exec_proc_node(inner, e),
    )
}

#[inline(never)]
fn nest_loop_arm<'mcx>(nl: &mut NestLoopNode<'mcx>, estate: &mut EStateData<'mcx>) -> ProcResult {
    let NestLoopNode { state, outer, inner } = nl;
    ::nodenestloop::exec_nest_loop(state, &mut **outer, &mut **inner, estate)
}

#[inline(never)]
fn hash_join_arm<'mcx>(
    hj: &mut PgBox<'mcx, HashJoinNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    let hj = &mut **hj;
    let HashSubNode { state: hstate, child } = &mut *hj.hash;
    ::nodehashjoin::exec_hash_join(&mut hj.state, &mut *hj.outer, hstate, &mut **child, estate)
}

#[inline(never)]
fn merge_join_arm<'mcx>(
    mj: &mut PgBox<'mcx, MergeJoinNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> ProcResult {
    let MergeJoinNode { state, outer, inner } = &mut **mj;
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
        PlanStateNode::SeqScan(ss) => end_scan(&mut ss.ss),
        PlanStateNode::FunctionScan(fs) => end_scan(&mut fs.ss),
        PlanStateNode::ValuesScan(vs) => end_scan(&mut vs.ss),
        PlanStateNode::CteScan(cs) => end_scan(&mut cs.ss),
        PlanStateNode::IndexScan(is) => end_scan(&mut is.ss),
        PlanStateNode::IndexOnlyScan(ios) => end_scan(&mut ios.ss),
        PlanStateNode::BitmapHeapScan(b) => end_scan(&mut b.scan.ss),
        PlanStateNode::Sort(s) => s.outer_desc = None,
        PlanStateNode::SubqueryScan(s) => end_scan(&mut s.ss),
        PlanStateNode::LockRows(_)
        | PlanStateNode::Append(_)
        | PlanStateNode::SetOp(_)
        | PlanStateNode::IncrementalSort(_)
        | PlanStateNode::Agg(_)
        | PlanStateNode::BitmapIndexScan(_)
        | PlanStateNode::BitmapAnd(_)
        | PlanStateNode::BitmapOr(_)
        | PlanStateNode::ModifyTable(_)
        | PlanStateNode::NestLoop(_)
        | PlanStateNode::HashJoin(_)
        | PlanStateNode::MergeJoin(_)
        | PlanStateNode::WindowAgg(_)
        | PlanStateNode::Material(_)
        | PlanStateNode::Unique(_)
        | PlanStateNode::Limit(_) => {}
    }
}

// Per-node state EXPLAIN reads off the live PlanState tree, as C does.
pub enum InstrExtra {
    Storage(::types_core::instrument::TuplestoreInstrumentation),
    Bitmap(::types_core::instrument::BitmapHeapScanInstrumentation),
    IndexSearches(u64),
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
        PlanStateNode::WindowAgg(w) => walk!(&mut w.outer),
        PlanStateNode::Sort(s) => walk!(&mut *s.outer),
        PlanStateNode::IncrementalSort(s) => walk!(&mut s.outer),
        PlanStateNode::Material(m) => walk!(&mut *m.outer),
        PlanStateNode::Unique(u) => walk!(&mut u.outer),
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
        PlanStateNode::SubqueryScan(s) => walk!(&mut *s.subplan),
        PlanStateNode::SetOp(s) => walk!(&mut s.outer, &mut s.inner),
        PlanStateNode::LockRows(l) => walk!(&mut *l.outer),
        PlanStateNode::ModifyTable(mps) => walk!(&mut mps.subplan),
        PlanStateNode::Result(_)
        | PlanStateNode::SeqScan(_)
        | PlanStateNode::FunctionScan(_)
        | PlanStateNode::ValuesScan(_)
        | PlanStateNode::CteScan(_)
        | PlanStateNode::IndexScan(_)
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
        PlanStateNode::SeqScan(ss) => ::nodeseqscan::exec_end_seq_scan(ss),
        PlanStateNode::FunctionScan(fs) => {
            ::nodefunctionscan::exec_end_function_scan(fs);
            Ok(())
        }
        PlanStateNode::ValuesScan(vs) => {
            ::nodevaluesscan::exec_end_values_scan(vs);
            Ok(())
        }
        PlanStateNode::CteScan(cs) => {
            ::nodectescan::exec_end_cte_scan(cs, estate);
            Ok(())
        }
        PlanStateNode::IndexScan(is) => ::nodeindexscan::exec_end_index_scan(is),
        PlanStateNode::IndexOnlyScan(ios) => {
            ::nodeindexonlyscan::exec_end_index_only_scan(ios)
        }
        PlanStateNode::Agg(aps) => {
            ::nodeagg::exec_end_agg(&mut aps.agg);
            exec_end_node(&mut aps.outer, estate)
        }
        PlanStateNode::WindowAgg(w) => {
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
        PlanStateNode::Unique(u) => {
            ::nodeunique::exec_end_unique(&mut u.state);
            exec_end_node(&mut u.outer, estate)
        }
        PlanStateNode::Limit(l) => {
            ::nodelimit::exec_end_limit(&mut l.state);
            exec_end_node(&mut l.outer, estate)
        }
        // C ExecEndLockRows: EvalPlanQualEnd + child.
        PlanStateNode::LockRows(l) => {
            crate::epq::eval_plan_qual_end(&mut l.epq, estate)?;
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
            crate::epq::eval_plan_qual_end(&mut mps.epq, estate)?;
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
        PlanStateNode::SubqueryScan(s) => exec_end_node(&mut s.subplan, estate),
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
            ::nodemergejoin::exec_end_merge_join(&mut mj.state);
            exec_end_node(&mut mj.outer, estate)?;
            exec_end_node(&mut mj.inner, estate)
        }
    }
}

/// `ExecShutdownNode` (execProcnode.c): per-node arms are all no-ops for the
/// ported set (Gather/ForeignScan/CustomScan/Hash own real shutdowns).
pub fn exec_shutdown_node<'mcx>(node: &mut PlanStateNode<'mcx>, estate: &mut EStateData<'mcx>) {
    match node {
        PlanStateNode::Instrumented(w) => exec_shutdown_node(&mut w.inner, estate),
        PlanStateNode::Result(rs) => {
            if let Some(outer) = rs.outer.as_deref_mut() {
                exec_shutdown_node(outer, estate);
            }
        }
        PlanStateNode::SeqScan(_)
        | PlanStateNode::FunctionScan(_)
        | PlanStateNode::ValuesScan(_)
        | PlanStateNode::CteScan(_)
        | PlanStateNode::IndexScan(_)
        | PlanStateNode::IndexOnlyScan(_)
        | PlanStateNode::BitmapIndexScan(_) => {}
        PlanStateNode::Agg(aps) => exec_shutdown_node(&mut aps.outer, estate),
        PlanStateNode::WindowAgg(w) => exec_shutdown_node(&mut w.outer, estate),
        PlanStateNode::Sort(s) => exec_shutdown_node(&mut s.outer, estate),
        PlanStateNode::IncrementalSort(s) => exec_shutdown_node(&mut s.outer, estate),
        PlanStateNode::Material(m) => exec_shutdown_node(&mut m.outer, estate),
        PlanStateNode::Unique(u) => exec_shutdown_node(&mut u.outer, estate),
        PlanStateNode::Limit(l) => exec_shutdown_node(&mut l.outer, estate),
        PlanStateNode::LockRows(l) => exec_shutdown_node(&mut l.outer, estate),
        PlanStateNode::BitmapHeapScan(b) => exec_shutdown_node(&mut b.bitmapqual, estate),
        PlanStateNode::BitmapAnd(bc) | PlanStateNode::BitmapOr(bc) => {
            for sub in bc.substates.iter_mut() {
                exec_shutdown_node(sub, estate);
            }
        }
        PlanStateNode::ModifyTable(mps) => exec_shutdown_node(&mut mps.subplan, estate),
        PlanStateNode::Append(a) => {
            for sub in a.substates.iter_mut() {
                exec_shutdown_node(sub, estate);
            }
        }
        PlanStateNode::SubqueryScan(s) => exec_shutdown_node(&mut s.subplan, estate),
        PlanStateNode::SetOp(s) => {
            let s = &mut **s;
            exec_shutdown_node(&mut s.outer, estate);
            exec_shutdown_node(&mut s.inner, estate);
        }
        PlanStateNode::NestLoop(nl) => {
            exec_shutdown_node(&mut nl.outer, estate);
            exec_shutdown_node(&mut nl.inner, estate);
        }
        // ExecShutdownHash: hand the table's instrumentation to the estate
        // (C: HashState.hinstrument) before EXPLAIN reads it.
        PlanStateNode::HashJoin(hj) => {
            let hj = &mut **hj;
            ::nodehashjoin::shutdown_accum_instrumentation(&hj.state, &hj.hash.state, estate);
            exec_shutdown_node(&mut hj.outer, estate);
            exec_shutdown_node(&mut hj.hash.child, estate);
        }
        PlanStateNode::MergeJoin(mj) => {
            exec_shutdown_node(&mut mj.outer, estate);
            exec_shutdown_node(&mut mj.inner, estate);
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
        PlanStateNode::SubqueryScan(s) => {
            let s = &mut **s;
            if s.ss.qual.is_none() {
                exec_set_tuple_bound(tuples_needed, &mut s.subplan);
            }
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
}

impl<'mcx> ::nodehashjoin::HashJoinOuter<'mcx> for PlanStateNode<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        exec_proc_node(self, estate)
    }

    fn rescan(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        crate::execami::exec_re_scan(self, estate)
    }
}

impl<'mcx> ::nodehash::HashBuildInput<'mcx> for PlanStateNode<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        exec_proc_node(self, estate)
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
    let mcx = estate.es_query_cxt;
    let (scan, inner, outer) = {
        let e = estate.ecxt(ecxt);
        (e.ecxt_scantuple, e.ecxt_innertuple, e.ecxt_outertuple)
    };
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
        outer: get(outer),
    };
    f(&mut slots, get(result), mcx)
}

// Exempt fields: released by release_owned/the per-node end fns before
// standard_executor_end forgets the bundle (Drop stays the abort path).
::mcx::forget_safe_struct!(
    PlanStateBase<'_> { plan, ps_ExprContext, ps_ResultTupleSlot;
        ps_ResultTupleDesc, ps_ProjInfo, qual },
    InstrumentedNode<'_> { inner, instr_idx },
    ModifyTablePlanState<'_> { mt, subplan, epq },
    BitmapHeapPlanState<'_> { scan, bitmapqual },
    BitmapCombineState<'_> { substates },
    AggPlanState<'_> { agg, outer },
    WindowAggNode<'_> { state, outer },
    MaterialNode<'_> { state, outer },
    SortNode<'_> { state, outer; outer_desc },
    IncrementalSortNode<'_> { state, outer },
    AppendNode<'_> { state, substates },
    SubqueryScanNode<'_> { ss, subplan },
    SetOpNode<'_> { state, outer, inner },
    LockRowsNode<'_> { state, outer, epq },
    LimitNode<'_> { state, outer },
    UniqueNode<'_> { state, outer },
    NestLoopNode<'_> { state, outer, inner },
    HashSubNode<'_> { state, child },
    HashJoinNode<'_> { state, outer, hash },
    MergeJoinNode<'_> { state, outer, inner },
);
::mcx::forget_safe_enum!(
    PlanStateNode<'_> {
        Result(x), SeqScan(x), FunctionScan(x), ValuesScan(x), CteScan(x),
        IndexScan(x), IndexOnlyScan(x), Agg(x), Sort(x), Material(x),
        IncrementalSort(x), Unique(x), Limit(x), BitmapHeapScan(x),
        BitmapIndexScan(x), Append(x), SubqueryScan(x), SetOp(x), LockRows(x),
        BitmapAnd(x), BitmapOr(x), ModifyTable(x), NestLoop(x), HashJoin(x),
        MergeJoin(x), WindowAgg(x), Instrumented(x),
    },
);
