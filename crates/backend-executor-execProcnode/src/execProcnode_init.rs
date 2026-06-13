//! `execProcnode-init` family — node-tree initialization dispatch.
//!
//! Owns `ExecInitNode` (the 35-way `Plan`-tag switch that recursively builds
//! the plan-state tree by routing each `Plan` tag to the owning node unit's
//! `ExecInit*` routine, then runs the `initPlan` and instrumentation tail) and
//! `ExecSetExecProcNode` (installs the `ExecProcNode` callback wrapper).

use backend_utils_misc_stack_depth_seams as stack_depth;
use mcx::{alloc_in, Mcx, PgBox};
use types_error::{PgError, PgResult};
use types_nodes::nodes::Node;
use types_nodes::{EStateData, ExecProcNodeMtd, PlanStateNode};

use crate::execProcnode_run_end::exec_proc_node_first;

/// `ExecInitNode(node, estate, eflags)` (execProcnode.c).
///
/// Recursively initialize the plan subtree rooted at `node`, returning its
/// plan-state tree. A `None` plan yields `None` (C `if (node == NULL) return
/// NULL;`). After building the concrete state node via the owning node unit's
/// `ExecInit*` seam, the C code:
///   * `ExecSetExecProcNode(result, result->ExecProcNode)` — install the
///     first-call wrapper,
///   * walk `node->initPlan` building `SubPlanState`s via `ExecInitSubPlan`,
///   * if `estate->es_instrument`, attach `InstrAlloc` instrumentation.
///
/// The 35-way switch dispatches over `nodeTag(node)`; in the owned model the
/// `nodeTag` switch becomes a `match` over the [`Node`] tagged enum. Each arm
/// routes to the owning node unit's `ExecInit*` routine. None of the per-node
/// `ExecInit*` routines have a seam declared in this scaffold yet, so every
/// arm panics loudly with the unported-owner message (the "mirror PG and
/// panic" rule); the arms swap to real seam calls as each node owner lands.
/// The wildcard is the C `default:` (`elog(ERROR, "unrecognized node type")`)
/// for `Plan` tags that have no [`Node`] enum variant yet.
pub fn exec_init_node<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<&'mcx Node<'mcx>>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<Option<PgBox<'mcx, PlanStateNode<'mcx>>>> {
    // do nothing when we get to the end of a leaf on tree.
    //
    // if (node == NULL) return NULL;
    let Some(node) = node else {
        return Ok(None);
    };

    // Make sure there's enough stack available. Need to check here, in
    // addition to ExecProcNode() (via ExecProcNodeFirst()), to ensure the
    // stack isn't overrun while initializing the node tree.
    //
    // check_stack_depth();
    stack_depth::check_stack_depth::call()?;

    // switch (nodeTag(node))
    let mut result: PgBox<'mcx, PlanStateNode<'mcx>> = match node {
        // ------------------------------------------------------------------
        // control nodes
        // ------------------------------------------------------------------
        // case T_Result: ExecInitResult((Result *) node, estate, eflags)
        // (nodeResult.c — no Result variant / seam yet)

        // case T_ProjectSet: ExecInitProjectSet(...) (nodeProjectSet.c)

        // case T_ModifyTable: ExecInitModifyTable(...) (nodeModifyTable.c)

        // case T_Append: ExecInitAppend((Append *) node, estate, eflags)
        Node::Append(_) => panic!(
            "backend-executor-nodeAppend::ExecInitAppend: ExecInitNode T_Append arm; \
             not ported / no seam declared"
        ),

        // case T_MergeAppend: ExecInitMergeAppend(...) (nodeMergeAppend.c)
        Node::MergeAppend(_) => panic!(
            "backend-executor-nodeMergeAppend::ExecInitMergeAppend: ExecInitNode \
             T_MergeAppend arm; not ported / no seam declared"
        ),

        // case T_RecursiveUnion: ExecInitRecursiveUnion(...) (nodeRecursiveunion.c)

        // case T_BitmapAnd: ExecInitBitmapAnd(...) (nodeBitmapAnd.c)

        // case T_BitmapOr: ExecInitBitmapOr(...) (nodeBitmapOr.c)

        // ------------------------------------------------------------------
        // scan nodes
        // ------------------------------------------------------------------
        // case T_SeqScan: ExecInitSeqScan((SeqScan *) node, estate, eflags)
        Node::SeqScan(_) => panic!(
            "backend-executor-nodeSeqscan::ExecInitSeqScan: ExecInitNode T_SeqScan arm; \
             not ported / no seam declared"
        ),

        // case T_SampleScan: ExecInitSampleScan(...) (nodeSamplescan.c)

        // case T_IndexScan: ExecInitIndexScan(...) (nodeIndexscan.c)

        // case T_IndexOnlyScan: ExecInitIndexOnlyScan(...) (nodeIndexonlyscan.c)
        Node::IndexOnlyScan(_) => panic!(
            "backend-executor-nodeIndexonlyscan::ExecInitIndexOnlyScan: ExecInitNode \
             T_IndexOnlyScan arm; not ported / no seam declared"
        ),

        // case T_BitmapIndexScan: ExecInitBitmapIndexScan(...) (nodeBitmapIndexscan.c)

        // case T_BitmapHeapScan: ExecInitBitmapHeapScan(...) (nodeBitmapHeapscan.c)

        // case T_TidScan: ExecInitTidScan(...) (nodeTidscan.c)

        // case T_TidRangeScan: ExecInitTidRangeScan((TidRangeScan *) node, estate, eflags)
        Node::TidRangeScan(_) => panic!(
            "backend-executor-nodeTidrangescan::ExecInitTidRangeScan: ExecInitNode \
             T_TidRangeScan arm; not ported / no seam declared"
        ),

        // case T_SubqueryScan: ExecInitSubqueryScan(...) (nodeSubqueryscan.c)

        // case T_FunctionScan: ExecInitFunctionScan(...) (nodeFunctionscan.c)

        // case T_TableFuncScan: ExecInitTableFuncScan((TableFuncScan *) node, estate, eflags)
        Node::TableFuncScan(_) => panic!(
            "backend-executor-nodeTableFuncscan::ExecInitTableFuncScan: ExecInitNode \
             T_TableFuncScan arm; not ported / no seam declared"
        ),

        // case T_ValuesScan: ExecInitValuesScan(...) (nodeValuesscan.c)

        // case T_CteScan: ExecInitCteScan(...) (nodeCtescan.c)

        // case T_NamedTuplestoreScan: ExecInitNamedTuplestoreScan(...)
        //   (nodeNamedtuplestorescan.c)

        // case T_WorkTableScan: ExecInitWorkTableScan(...) (nodeWorktablescan.c)

        // case T_ForeignScan: ExecInitForeignScan((ForeignScan *) node, estate, eflags)
        Node::ForeignScan(_) => panic!(
            "backend-executor-nodeForeignscan::ExecInitForeignScan: ExecInitNode \
             T_ForeignScan arm; not ported / no seam declared"
        ),

        // case T_CustomScan: ExecInitCustomScan(...) (nodeCustom.c)

        // ------------------------------------------------------------------
        // join nodes
        // ------------------------------------------------------------------
        // case T_NestLoop: ExecInitNestLoop((NestLoop *) node, estate, eflags)
        Node::NestLoop(_) => panic!(
            "backend-executor-nodeNestloop::ExecInitNestLoop: ExecInitNode T_NestLoop arm; \
             not ported / no seam declared"
        ),

        // case T_MergeJoin: ExecInitMergeJoin((MergeJoin *) node, estate, eflags)
        Node::MergeJoin(_) => panic!(
            "backend-executor-nodeMergejoin::ExecInitMergeJoin: ExecInitNode T_MergeJoin arm; \
             not ported / no seam declared"
        ),

        // case T_HashJoin: ExecInitHashJoin((HashJoin *) node, estate, eflags)
        Node::HashJoin(_) => panic!(
            "backend-executor-nodeHashjoin::ExecInitHashJoin: ExecInitNode T_HashJoin arm; \
             not ported / no seam declared"
        ),

        // ------------------------------------------------------------------
        // materialization nodes
        // ------------------------------------------------------------------
        // case T_Material: ExecInitMaterial((Material *) node, estate, eflags)
        Node::Material(_) => panic!(
            "backend-executor-nodeMaterial::ExecInitMaterial: ExecInitNode T_Material arm; \
             not ported / no seam declared"
        ),

        // case T_Sort: ExecInitSort((Sort *) node, estate, eflags)
        Node::Sort(_) => panic!(
            "backend-executor-nodeSort::ExecInitSort: ExecInitNode T_Sort arm; \
             not ported / no seam declared"
        ),

        // case T_IncrementalSort: ExecInitIncrementalSort(...) (nodeIncrementalsort.c)

        // case T_Memoize: ExecInitMemoize((Memoize *) node, estate, eflags)
        Node::Memoize(_) => panic!(
            "backend-executor-nodeMemoize::ExecInitMemoize: ExecInitNode T_Memoize arm; \
             not ported / no seam declared"
        ),

        // case T_Group: ExecInitGroup(...) (nodeGroup.c)

        // case T_Agg: ExecInitAgg(...) (nodeAgg.c)

        // case T_WindowAgg: ExecInitWindowAgg(...) (nodeWindowAgg.c)

        // case T_Unique: ExecInitUnique(...) (nodeUnique.c)

        // case T_Gather: ExecInitGather(...) (nodeGather.c)

        // case T_GatherMerge: ExecInitGatherMerge(...) (nodeGatherMerge.c)

        // case T_Hash: ExecInitHash((Hash *) node, estate, eflags)
        Node::Hash(_) => panic!(
            "backend-executor-nodeHash::ExecInitHash: ExecInitNode T_Hash arm; \
             not ported / no seam declared"
        ),

        // case T_SetOp: ExecInitSetOp(...) (nodeSetop.c)

        // case T_LockRows: ExecInitLockRows(...) (nodeLockRows.c)

        // case T_Limit: ExecInitLimit((Limit *) node, estate, eflags)
        Node::Limit(_) => {
            let limitstate = backend_executor_nodeLimit::ExecInitLimit(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Limit(limitstate))?
        }

        // default:
        //   elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
        //   result = NULL;  /* keep compiler quiet */
        //
        // Reached for `Plan` tags that have no `Node` enum variant yet; each
        // node port that adds a variant adds its arm above.
        other => return Err(unrecognized_node_type(other)),
    };

    // ExecSetExecProcNode(result, result->ExecProcNode);
    //
    // The owning `ExecInit*` routine has already stored the node's real
    // next-tuple callback in `result->ExecProcNode`; pass it through, exactly as
    // C does.
    let function = result.ps_head().ExecProcNode;
    ExecSetExecProcNode(&mut result, function);

    // Initialize any initPlans present in this node.  The planner put them in
    // a separate list for us.
    //
    // The defining characteristic of initplans is that they don't have
    // arguments, so we don't need to evaluate them (in contrast to
    // ExecInitSubPlanExpr()).
    //
    //   subps = NIL;
    //   foreach(l, node->initPlan)
    //   {
    //       SubPlan    *subplan = (SubPlan *) lfirst(l);
    //       SubPlanState *sstate;
    //       Assert(IsA(subplan, SubPlan));
    //       Assert(subplan->args == NIL);
    //       sstate = ExecInitSubPlan(subplan, result);
    //       subps = lappend(subps, sstate);
    //   }
    //   result->initPlan = subps;
    //
    // The source `Plan.initPlan` list is not modeled on the trimmed `Plan`
    // struct, and `ExecInitSubPlan` (nodeSubplan.c) is unported with no seam
    // declared in this scaffold. Building this node's `SubPlanState`s routes
    // through that owner — a loud panic when a node actually carries
    // initplans. (A leaf node with no initplans is the C `NIL` walk, a no-op;
    // the result node already defaults `initPlan = None`.)
    if node_has_init_plan(node) {
        panic!(
            "backend-executor-nodeSubplan::ExecInitSubPlan: ExecInitNode initPlan walk \
             (Plan.initPlan not modeled on the trimmed Plan struct); not ported / no seam declared"
        );
    }

    // Set up instrumentation for this node if requested
    //
    //   if (estate->es_instrument)
    //       result->instrument = InstrAlloc(1, estate->es_instrument,
    //                                       result->async_capable);
    //
    // `InstrAlloc` (instrument.c) is unported and has no seam declared in this
    // scaffold (the `backend-executor-instrument-seams` crate declares only
    // InstrEndLoop / InstrUpdateTupleCount). Attaching instrumentation routes
    // through that owner — a loud panic when instrumentation is requested.
    if estate.es_instrument != 0 {
        panic!(
            "backend-executor-instrument::InstrAlloc: ExecInitNode instrumentation attach \
             (estate->es_instrument set); not ported / no seam declared"
        );
    }

    // return result;
    Ok(Some(result))
}

/// `elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node))` — the C
/// `ExecInitNode` `default:` arm. Carries `ERRCODE_INTERNAL_ERROR`, as every
/// bare `elog(ERROR)` does.
fn unrecognized_node_type(node: &Node<'_>) -> PgError {
    PgError::error(format!("unrecognized node type: {}", node.tag()))
}

/// `node->initPlan != NIL` — does this `Plan` node carry any initplans?
///
/// The trimmed `Plan` struct does not model the `List *initPlan` field, so
/// there is no per-node init-plan list to walk yet. Until that field lands a
/// `Plan` node is treated as carrying no initplans (the common C `NIL` case);
/// the `ExecInitNode` initPlan walk only fires its unported-owner panic once
/// the field exists and is non-empty.
fn node_has_init_plan(_node: &Node<'_>) -> bool {
    false
}

/// `ExecSetExecProcNode(node, function)` (execProcnode.c).
///
/// Install a node's `ExecProcNode` callback behind the first-call wrapper:
/// C records the per-node "real" routine in `node->ExecProcNodeReal` and arms
/// `node->ExecProcNode` with the `ExecProcNodeFirst` wrapper, so the first
/// `ExecProcNode` call runs the one-time stack-depth check and (if the node is
/// instrumented) swaps in the `ExecProcNodeInstr` bracket.
pub fn ExecSetExecProcNode<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    function: ExecProcNodeMtd<'mcx>,
) {
    // node->ExecProcNodeReal = function;
    node.ps_head_mut().ExecProcNodeReal = function;
    // node->ExecProcNode = ExecProcNodeFirst;
    node.ps_head_mut().ExecProcNode = Some(exec_proc_node_first);
}
