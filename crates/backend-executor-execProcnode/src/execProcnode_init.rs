//! `execProcnode-init` family — node-tree initialization dispatch.
//!
//! Owns `ExecInitNode` (the 35-way `Plan`-tag switch that recursively builds
//! the plan-state tree by routing each `Plan` tag to the owning node unit's
//! `ExecInit*` routine, then runs the `initPlan` and instrumentation tail) and
//! `ExecSetExecProcNode` (installs the `ExecProcNode` callback wrapper).

use backend_utils_misc_stack_depth_seams as stack_depth;
use mcx::{alloc_in, Mcx, PgBox, PgVec};
use types_error::{PgError, PgResult};
use types_nodes::nodes::Node;
use types_nodes::{EStateData, ExecProcNodeMtd, PlanStateNode, SubPlanState};

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
        Node::Result(_) => {
            let s = backend_executor_nodeResult::ExecInitResult(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Result(s))?
        }

        // case T_ProjectSet: ExecInitProjectSet(...) (nodeProjectSet.c)
        Node::ProjectSet(_) => {
            let s = backend_executor_nodeProjectSet::ExecInitProjectSet(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::ProjectSet(s))?
        }

        // case T_ModifyTable: ExecInitModifyTable(...) (nodeModifyTable.c)
        Node::ModifyTable(m) => {
            let s =
                backend_executor_nodeModifyTable::init::ExecInitModifyTable(mcx, m, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::ModifyTable(s))?
        }

        // case T_Append: ExecInitAppend((Append *) node, estate, eflags)
        Node::Append(append) => {
            let s = backend_executor_nodeAppend::ExecInitAppend(mcx, node, append, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Append(s))?
        }

        // case T_MergeAppend: ExecInitMergeAppend((MergeAppend *) node, estate, eflags)
        Node::MergeAppend(_) => {
            let s = backend_executor_nodeMergeAppend::ExecInitMergeAppend(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::MergeAppend(s))?
        }

        // case T_RecursiveUnion: ExecInitRecursiveUnion(...) (nodeRecursiveunion.c)
        Node::RecursiveUnion(_) => {
            let s = backend_executor_nodeRecursiveunion::ExecInitRecursiveUnion(
                node, estate, eflags,
            )?;
            alloc_in(mcx, PlanStateNode::RecursiveUnion(s))?
        }

        // case T_BitmapAnd: ExecInitBitmapAnd(...) (nodeBitmapAnd.c)
        //
        // `ExecInitBitmapAnd` already returns a `PgBox<PlanStateNode>` (its
        // makeNode wraps the result in the enum), so this arm passes it through
        // directly rather than re-wrapping a concrete state struct.
        Node::BitmapAnd(bitmap_and) => {
            backend_executor_nodeBitmapAnd::ExecInitBitmapAnd(
                mcx, node, bitmap_and, estate, eflags,
            )?
        }

        // case T_BitmapOr: ExecInitBitmapOr(...) (nodeBitmapOr.c)
        //
        // The trimmed central `Plan` (`Node`) enum has no `BitmapOr` variant
        // yet, so no `Node::BitmapOr` arm exists to route here. Adding the
        // `BitmapOr` Plan variant is the central-node keystone (K1 follow-on),
        // out of scope for this executor-driver dispatch; the owner
        // `ExecInitBitmapOr` is ported and ready. A plain `DestNone` SELECT
        // never reaches this (BitmapOr only appears under a BitmapHeapScan).

        // ------------------------------------------------------------------
        // scan nodes
        // ------------------------------------------------------------------
        // case T_SeqScan: ExecInitSeqScan((SeqScan *) node, estate, eflags)
        Node::SeqScan(seqscan) => {
            let s = backend_executor_nodeSeqscan::ExecInitSeqScan(seqscan, node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::SeqScan(s))?
        }

        // case T_SampleScan: ExecInitSampleScan(...) (nodeSamplescan.c)
        //
        // No `SampleScan` Plan variant on the trimmed central `Node` enum (and
        // `SampleScanState` lives in `types-samplescan`, which depends on
        // `types-nodes`, so it cannot become a `PlanStateNode` variant without
        // first relocating it — the central-node keystone). The owner
        // `ExecInitSampleScan` is ported; a plain SELECT does not use TABLESAMPLE.

        // case T_IndexScan: ExecInitIndexScan(...) (nodeIndexscan.c)
        Node::IndexScan(_) => {
            let s = backend_executor_nodeIndexscan::ExecInitIndexScan(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::IndexScan(s))?
        }

        // case T_IndexOnlyScan: ExecInitIndexOnlyScan((IndexOnlyScan *) node, estate, eflags)
        Node::IndexOnlyScan(_) => {
            let s = backend_executor_nodeIndexonlyscan::ExecInitIndexOnlyScan(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::IndexOnlyScan(s))?
        }

        // case T_BitmapIndexScan: ExecInitBitmapIndexScan(...) (nodeBitmapIndexscan.c)
        Node::BitmapIndexScan(_) => {
            let s = backend_executor_nodeBitmapIndexscan::ExecInitBitmapIndexScan(
                node, estate, eflags,
            )?;
            alloc_in(mcx, PlanStateNode::BitmapIndexScan(s))?
        }

        // case T_BitmapHeapScan: ExecInitBitmapHeapScan(...) (nodeBitmapHeapscan.c)
        //
        // No `BitmapHeapScan` Plan variant on the trimmed central `Node` enum
        // (central-node keystone, K1 follow-on). The owner
        // `ExecInitBitmapHeapScan` is ported; a plain seqscan/indexscan SELECT
        // does not produce a bitmap heap scan.

        // case T_TidScan: ExecInitTidScan(...) (nodeTidscan.c)
        //
        // No `TidScan` Plan variant on the trimmed central `Node` enum
        // (central-node keystone, K1 follow-on). The owner `ExecInitTidScan`
        // is ported.

        // case T_TidRangeScan: ExecInitTidRangeScan((TidRangeScan *) node, estate, eflags)
        Node::TidRangeScan(tidrangescan) => {
            let s = backend_executor_nodeTidrangescan::ExecInitTidRangeScan(
                tidrangescan,
                estate,
                eflags,
            )?;
            alloc_in(mcx, PlanStateNode::TidRangeScan(alloc_in(mcx, s)?))?
        }

        // case T_SubqueryScan: ExecInitSubqueryScan(...) (nodeSubqueryscan.c)
        Node::SubqueryScan(subqueryscan) => {
            let s = backend_executor_nodeSubqueryscan::ExecInitSubqueryScan(
                subqueryscan,
                node,
                estate,
                eflags,
            )?;
            alloc_in(mcx, PlanStateNode::SubqueryScan(s))?
        }

        // case T_FunctionScan: ExecInitFunctionScan(...) (nodeFunctionscan.c)

        // case T_TableFuncScan: ExecInitTableFuncScan((TableFuncScan *) node, estate, eflags)
        Node::TableFuncScan(_) => {
            let s = backend_executor_nodeTableFuncscan::ExecInitTableFuncScan(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::TableFuncScan(s))?
        }

        // case T_ValuesScan: ExecInitValuesScan(...) (nodeValuesscan.c)
        Node::ValuesScan(_) => {
            let s = backend_executor_nodeValuesscan::ExecInitValuesScan(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::ValuesScan(s))?
        }

        // case T_CteScan: ExecInitCteScan(...) (nodeCtescan.c)
        Node::CteScan(_) => {
            let s = backend_executor_nodeCtescan::ExecInitCteScan(node, eflags, estate)?;
            alloc_in(mcx, PlanStateNode::CteScan(s))?
        }

        // case T_NamedTuplestoreScan: ExecInitNamedTuplestoreScan(...)
        //   (nodeNamedtuplestorescan.c)
        //
        // The owner `ExecInitNamedTuplestoreScan` takes a real
        // `&mut QueryEnvironment` (the C `estate->es_queryEnv`), but the
        // `EState.es_queryEnv` field is still modeled as `Opaque` — the
        // QueryEnvironment / ENR value model is not built. There is no real
        // `&mut QueryEnvironment` to pass, so this arm cannot be wired
        // faithfully yet. A plain SELECT never scans an ephemeral named
        // relation (only trigger transition tables / WITH-tuplestores do).

        // case T_WorkTableScan: ExecInitWorkTableScan(...) (nodeWorktablescan.c)
        //
        // The `WorkTableScan` Plan variant and the `PlanStateNode::WorkTableScan`
        // variant both exist (the state struct lives in `types-nodes`, no crate
        // cycle), so this arm is fully wired. WorkTableScan only appears inside a
        // RecursiveUnion (WITH RECURSIVE).
        Node::WorkTableScan(wts) => {
            let s = backend_executor_nodeWorktablescan::ExecInitWorkTableScan(
                wts, estate, eflags,
            )?;
            alloc_in(mcx, PlanStateNode::WorkTableScan(alloc_in(mcx, s)?))?
        }

        // case T_ForeignScan: ExecInitForeignScan((ForeignScan *) node, estate, eflags)
        Node::ForeignScan(_) => {
            let s = backend_executor_nodeForeignscan::ExecInitForeignScan(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::ForeignScan(s))?
        }

        // case T_CustomScan: ExecInitCustomScan(...) (nodeCustom.c)
        Node::CustomScan(_) => {
            let s = backend_executor_nodeCustom::ExecInitCustomScan(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::CustomScan(s))?
        }

        // ------------------------------------------------------------------
        // join nodes
        // ------------------------------------------------------------------
        // case T_NestLoop: ExecInitNestLoop((NestLoop *) node, estate, eflags)
        Node::NestLoop(_) => {
            let s = backend_executor_nodeNestloop::ExecInitNestLoop(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::NestLoop(s))?
        }

        // case T_MergeJoin: ExecInitMergeJoin((MergeJoin *) node, estate, eflags)
        Node::MergeJoin(_) => {
            let s = backend_executor_nodeMergejoin::ExecInitMergeJoin(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::MergeJoin(s))?
        }

        // case T_HashJoin: ExecInitHashJoin((HashJoin *) node, estate, eflags)
        Node::HashJoin(_) => {
            let s = backend_executor_nodeHashjoin::ExecInitHashJoin(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::HashJoin(s))?
        }

        // ------------------------------------------------------------------
        // materialization nodes
        // ------------------------------------------------------------------
        // case T_Material: ExecInitMaterial((Material *) node, estate, eflags)
        Node::Material(_) => {
            let s = backend_executor_nodeMaterial::ExecInitMaterial(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Material(s))?
        }

        // case T_Sort: ExecInitSort((Sort *) node, estate, eflags)
        Node::Sort(_) => {
            let s = backend_executor_nodeSort::ExecInitSort(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Sort(s))?
        }

        // case T_IncrementalSort:
        //   ExecInitIncrementalSort((IncrementalSort *) node, estate, eflags)
        Node::IncrementalSort(_) => {
            let s = backend_executor_nodeIncrementalSort::ExecInitIncrementalSort(
                node, estate, eflags,
            )?;
            alloc_in(mcx, PlanStateNode::IncrementalSort(s))?
        }

        // case T_Memoize: ExecInitMemoize((Memoize *) node, estate, eflags)
        //
        // `ExecInitMemoize` returns the node state in a global-allocator
        // `alloc::boxed::Box` (the nodeMemoize port's local makeNode allocation
        // model), whereas the `PlanStateNode::Memoize` variant — like every
        // other plan-state arm — carries the executor-context `PgBox`. Re-home
        // the owned `MemoizeScanState` into the query context via `alloc_in`,
        // matching the C `makeNode(MemoizeState)` allocation in the executor
        // memory context (move-only; the state struct owns its contents).
        Node::Memoize(_) => {
            let s = backend_executor_nodeMemoize::ExecInitMemoize(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Memoize(alloc_in(mcx, *s)?))?
        }

        // case T_Group: ExecInitGroup(...) (nodeGroup.c)
        Node::Group(_) => {
            let s = backend_executor_nodeGroup::ExecInitGroup(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Group(s))?
        }

        // case T_Agg: ExecInitAgg((Agg *) node, estate, eflags) (nodeAgg.c)
        //
        // `AggStateData` lives in `backend-executor-nodeAgg` (ABOVE `types-nodes`),
        // so the `PlanStateNode::Agg` variant carries it behind the owned,
        // tag-checked erased `AggStateLive` carrier (#324/#165 keystone). The
        // `ExecInitAgg` result is unsized into that trait object here.
        Node::Agg(agg) => {
            let s = backend_executor_nodeAgg::ExecInitAgg(agg, estate, eflags, mcx)?;
            let live = backend_executor_nodeAgg::erase_agg_state(s);
            alloc_in(mcx, PlanStateNode::Agg(live))?
        }

        // case T_WindowAgg: ExecInitWindowAgg((WindowAgg *) node, estate, eflags)
        Node::WindowAgg(_) => {
            let s = backend_executor_nodeWindowAgg::ExecInitWindowAgg(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::WindowAgg(s))?
        }

        // case T_Unique: ExecInitUnique(...) (nodeUnique.c)
        Node::Unique(_) => {
            let s = backend_executor_nodeUnique::ExecInitUnique(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Unique(s))?
        }

        // case T_Gather: ExecInitGather((Gather *) node, estate, eflags)
        Node::Gather(_) => {
            let s = backend_executor_nodeGather::ExecInitGather(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Gather(s))?
        }

        // case T_GatherMerge: ExecInitGatherMerge(...) (nodeGatherMerge.c)
        Node::GatherMerge(_) => {
            let s = backend_executor_nodeGatherMerge::ExecInitGatherMerge(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::GatherMerge(s))?
        }

        // case T_Hash: ExecInitHash((Hash *) node, estate, eflags)
        Node::Hash(hash) => {
            let s = backend_executor_nodeHash::exec_hash::ExecInitHash(mcx, hash, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Hash(s))?
        }

        // case T_SetOp: ExecInitSetOp(...) (nodeSetop.c)
        Node::SetOp(_) => {
            let s = backend_executor_nodeSetOp::ExecInitSetOp(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::SetOp(s))?
        }

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

    // Set the `ExprState.parent` back-link on every expression this node owns.
    //
    // In C, `ExecInit*` builds its quals/projection with `ExecInitExpr(node,
    // (PlanState *) state)` — `parent` is the address-stable `makeNode`'d state,
    // available *during* the node's init. In the owned tree the concrete `*State`
    // struct and its enclosing `PlanStateNode` enum are two separate allocations:
    // the per-node `ExecInit*` (and the execExpr `ExecInitQual`/`ExecInitExpr`
    // seams it drives) only sees the embedded head and leaves `parent` unset; the
    // enum wrapper — whose address the `EEOP_GROUPING_FUNC` /
    // `EEOP_MERGE_SUPPORT_FUNC` / SubPlan consumers need (those read
    // `parent.as_agg_state()` / `as_modify_table_state()`, which are enum methods)
    // — only becomes address-stable here, once `result` is boxed. So the back-link
    // is stamped now, mirroring C's `(PlanState *) state` identity.
    result.stamp_expr_parents();

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
    // `Plan.initPlan` is now modeled on the central `Plan` struct, and
    // `ExecInitSubPlan` (nodeSubplan.c) is ported. Each `SubPlan` is cloned into
    // the per-query context (the C `node->initPlan` list lives in the plan tree;
    // `ExecInitSubPlan` takes ownership of an owned `SubPlan`) and built into a
    // `SubPlanState`, gathered into `result->initPlan`.
    if let Some(init) = node.plan_head().initPlan.as_ref() {
        if !init.is_empty() {
            let mut subps: PgVec<'mcx, SubPlanState<'mcx>> =
                mcx::vec_with_capacity_in(mcx, init.len())?;
            for subplan in init.iter() {
                // Assert(IsA(subplan, SubPlan)); Assert(subplan->args == NIL);
                debug_assert!(subplan.args.is_empty());
                let owned: PgBox<'mcx, types_nodes::primnodes::SubPlan<'mcx>> =
                    alloc_in(mcx, subplan.clone_in(mcx)?)?;
                let sstate = backend_executor_nodeSubplan::ExecInitSubPlan(owned, estate)?;
                subps.push(sstate);
            }
            result.ps_head_mut().initPlan = Some(subps);
        }
    }

    // Set up instrumentation for this node if requested
    //
    //   if (estate->es_instrument)
    //       result->instrument = InstrAlloc(1, estate->es_instrument,
    //                                       result->async_capable);
    //
    // `InstrAlloc(1, ...)` allocates a one-element array of `Instrumentation`;
    // C stores the array pointer in `result->instrument`, the single [0]
    // element being this node's stats block. Here we take the one allocated
    // `Instrumentation` and box it into the node's `instrument` slot.
    if estate.es_instrument != 0 {
        let async_capable = result.ps_head().async_capable;
        let mut instr = backend_executor_instrument_seams::instr_alloc::call(
            mcx,
            1,
            estate.es_instrument,
            async_capable,
        )?;
        let one = instr.swap_remove(0);
        result.ps_head_mut().instrument = Some(alloc_in(mcx, one)?);
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
