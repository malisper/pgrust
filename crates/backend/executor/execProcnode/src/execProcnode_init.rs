//! `execProcnode-init` family — node-tree initialization dispatch.
//!
//! Owns `ExecInitNode` (the 35-way `Plan`-tag switch that recursively builds
//! the plan-state tree by routing each `Plan` tag to the owning node unit's
//! `ExecInit*` routine, then runs the `initPlan` and instrumentation tail) and
//! `ExecSetExecProcNode` (installs the `ExecProcNode` callback wrapper).

use stack_depth_seams as stack_depth;
use mcx::{alloc_in, Mcx, PgBox, PgVec};
use types_error::{PgError, PgResult};
use ::nodes::nodes::{ntag, Node};
use nodes::{EStateData, ExecProcNodeMtd, PlanStateNode, SubPlanState};

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
    let mut result: PgBox<'mcx, PlanStateNode<'mcx>> = match node.node_tag() {
        // ------------------------------------------------------------------
        // control nodes
        // ------------------------------------------------------------------
        // case T_Result: ExecInitResult((Result *) node, estate, eflags)
        ntag::T_Result => {
            let s = nodeResult::ExecInitResult(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Result(s))?
        }

        // case T_ProjectSet: ExecInitProjectSet(...) (nodeProjectSet.c)
        ntag::T_ProjectSet => {
            let s = nodeProjectSet::ExecInitProjectSet(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::ProjectSet(s))?
        }

        // case T_ModifyTable: ExecInitModifyTable(...) (nodeModifyTable.c)
        ntag::T_ModifyTable => {
            let m = node.expect_modifytable();
            let s = nodeModifyTable::init::ExecInitModifyTable(
                mcx, node, m, estate, eflags,
            )?;
            alloc_in(mcx, PlanStateNode::ModifyTable(s))?
        }

        // case T_Append: ExecInitAppend((Append *) node, estate, eflags)
        ntag::T_Append => {
            let append = node.expect_append();
            let s = nodeAppend::ExecInitAppend(mcx, node, append, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Append(s))?
        }

        // case T_MergeAppend: ExecInitMergeAppend((MergeAppend *) node, estate, eflags)
        ntag::T_MergeAppend => {
            let s = nodeMergeAppend::ExecInitMergeAppend(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::MergeAppend(s))?
        }

        // case T_RecursiveUnion: ExecInitRecursiveUnion(...) (nodeRecursiveunion.c)
        ntag::T_RecursiveUnion => {
            let s = nodeRecursiveunion::ExecInitRecursiveUnion(
                node, estate, eflags,
            )?;
            alloc_in(mcx, PlanStateNode::RecursiveUnion(s))?
        }

        // case T_BitmapAnd: ExecInitBitmapAnd(...) (nodeBitmapAnd.c)
        //
        // `ExecInitBitmapAnd` already returns a `PgBox<PlanStateNode>` (its
        // makeNode wraps the result in the enum), so this arm passes it through
        // directly rather than re-wrapping a concrete state struct.
        ntag::T_BitmapAnd => {
            let bitmap_and = node.expect_bitmapand();
            nodeBitmapAnd::ExecInitBitmapAnd(
                mcx, node, bitmap_and, estate, eflags,
            )?
        }

        // case T_BitmapOr: ExecInitBitmapOr(...) (nodeBitmapOr.c)
        //
        // `ExecInitBitmapOr` returns a `PgBox<BitmapOrState>`; wrap it in the
        // central `PlanStateNode`.
        ntag::T_BitmapOr => {
            let bitmap_or = node.expect_bitmapor();
            let s = nodeBitmapOr::ExecInitBitmapOr(
                mcx, node, bitmap_or, estate, eflags,
            )?;
            alloc_in(mcx, PlanStateNode::BitmapOr(s))?
        }

        // ------------------------------------------------------------------
        // scan nodes
        // ------------------------------------------------------------------
        // case T_SeqScan: ExecInitSeqScan((SeqScan *) node, estate, eflags)
        ntag::T_SeqScan => {
            let seqscan = node.expect_seqscan();
            let s = nodeSeqscan::ExecInitSeqScan(seqscan, node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::SeqScan(s))?
        }

        // case T_SampleScan: ExecInitSampleScan(...) (nodeSamplescan.c)
        //
        // `SampleScanState` lives in `types-samplescan` (ABOVE `types-nodes`),
        // so the `PlanStateNode::SampleScan` variant carries it behind the
        // owned, tag-checked erased `SampleScanStateLive` carrier (same pattern
        // as `PlanStateNode::Agg`). The `ExecInitSampleScan` result is boxed
        // into the per-query context and unsized into that trait object here.
        ntag::T_SampleScan => {
            let samplescan = node.expect_samplescan();
            let s = nodeSamplescan::ExecInitSampleScan(
                samplescan, node, estate, eflags,
            )?;
            let boxed = alloc_in(mcx, s)?;
            let live = nodeSamplescan::erase_sample_scan_state(boxed);
            alloc_in(mcx, PlanStateNode::SampleScan(live))?
        }

        // case T_IndexScan: ExecInitIndexScan(...) (nodeIndexscan.c)
        ntag::T_IndexScan => {
            let s = nodeIndexscan::ExecInitIndexScan(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::IndexScan(s))?
        }

        // case T_IndexOnlyScan: ExecInitIndexOnlyScan((IndexOnlyScan *) node, estate, eflags)
        ntag::T_IndexOnlyScan => {
            let s = nodeIndexonlyscan::ExecInitIndexOnlyScan(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::IndexOnlyScan(s))?
        }

        // case T_BitmapIndexScan: ExecInitBitmapIndexScan(...) (nodeBitmapIndexscan.c)
        ntag::T_BitmapIndexScan => {
            let s = nodeBitmapIndexscan::ExecInitBitmapIndexScan(
                node, estate, eflags,
            )?;
            alloc_in(mcx, PlanStateNode::BitmapIndexScan(s))?
        }

        // case T_BitmapHeapScan: ExecInitBitmapHeapScan(...) (nodeBitmapHeapscan.c)
        ntag::T_BitmapHeapScan => {
            let bitmap_heap = node.expect_bitmapheapscan();
            let s = nodeBitmapHeapscan::ExecInitBitmapHeapScan(
                node, bitmap_heap, estate, eflags,
            )?;
            alloc_in(mcx, PlanStateNode::BitmapHeapScan(s))?
        }

        // case T_TidScan: ExecInitTidScan((TidScan *) node, estate, eflags) (nodeTidscan.c)
        ntag::T_TidScan => {
            let tidscan = node.expect_tidscan();
            let s = nodeTidscan::ExecInitTidScan(tidscan, node, eflags, estate)?;
            alloc_in(mcx, PlanStateNode::TidScan(s))?
        }

        // case T_TidRangeScan: ExecInitTidRangeScan((TidRangeScan *) node, estate, eflags)
        ntag::T_TidRangeScan => {
            let tidrangescan = node.expect_tidrangescan();
            let s = nodeTidrangescan::ExecInitTidRangeScan(
                tidrangescan,
                node,
                estate,
                eflags,
            )?;
            alloc_in(mcx, PlanStateNode::TidRangeScan(alloc_in(mcx, s)?))?
        }

        // case T_SubqueryScan: ExecInitSubqueryScan(...) (nodeSubqueryscan.c)
        ntag::T_SubqueryScan => {
            let subqueryscan = node.expect_subqueryscan();
            let s = nodeSubqueryscan::ExecInitSubqueryScan(
                subqueryscan,
                node,
                estate,
                eflags,
            )?;
            alloc_in(mcx, PlanStateNode::SubqueryScan(s))?
        }

        // case T_FunctionScan: ExecInitFunctionScan(...) (nodeFunctionscan.c)
        ntag::T_FunctionScan => {
            let s = nodeFunctionscan::ExecInitFunctionScan(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::FunctionScan(s))?
        }

        // case T_TableFuncScan: ExecInitTableFuncScan((TableFuncScan *) node, estate, eflags)
        ntag::T_TableFuncScan => {
            let s = nodeTableFuncscan::ExecInitTableFuncScan(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::TableFuncScan(s))?
        }

        // case T_ValuesScan: ExecInitValuesScan(...) (nodeValuesscan.c)
        ntag::T_ValuesScan => {
            let s = nodeValuesscan::ExecInitValuesScan(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::ValuesScan(s))?
        }

        // case T_CteScan: ExecInitCteScan(...) (nodeCtescan.c)
        ntag::T_CteScan => {
            let s = nodeCtescan::ExecInitCteScan(node, eflags, estate)?;
            alloc_in(mcx, PlanStateNode::CteScan(s))?
        }

        // case T_NamedTuplestoreScan: ExecInitNamedTuplestoreScan(...)
        //   (nodeNamedtuplestorescan.c)
        //
        // The owner reads the ENR through the C `estate->es_queryEnv`. In the
        // owned model that environment lives in the per-backend query-environment
        // *home* (a thread-local owned by the AFTER-trigger firing code, which
        // strictly wraps this whole SPI execution — ExecInit/ExecRun/ExecEnd run
        // inside the trigger call). Borrow the top (innermost) env from the home
        // and hand it to the node. A NamedTuplestoreScan only appears inside a
        // trigger function body reading a transition table (OLD/NEW TABLE), so a
        // live home env is guaranteed present here.
        ntag::T_NamedTuplestoreScan => {
            queryenvironment_home::with_top_query_env(|env| {
                let env = env.ok_or_else(|| {
                    PgError::error(
                        "NamedTuplestoreScan with no live query environment (no \
                         transition-table trigger on the SPI call stack)"
                            .to_string(),
                    )
                })?;
                // SAFETY: the home owns the env for 'static; the firing code that
                // pushed it strictly outlives this per-query EState (the trigger
                // call wraps the entire SPI execution). Reborrow its lifetime down
                // to 'mcx for this single init. The node only takes a non-owning
                // raw `reldata` alias from it, which the home keeps live for the
                // scan's duration.
                let env: &mut ::nodes::queryenvironment::QueryEnvironment<'mcx> =
                    unsafe { core::mem::transmute(env) };
                let s = nodeNamedtuplestorescan::ExecInitNamedTuplestoreScan(
                    node, estate, eflags, env,
                )?;
                Ok::<_, PgError>(alloc_in(mcx, PlanStateNode::NamedTuplestoreScan(s))?)
            })?
        }

        // case T_WorkTableScan: ExecInitWorkTableScan(...) (nodeWorktablescan.c)
        //
        // The `WorkTableScan` Plan variant and the `PlanStateNode::WorkTableScan`
        // variant both exist (the state struct lives in `types-nodes`, no crate
        // cycle), so this arm is fully wired. WorkTableScan only appears inside a
        // RecursiveUnion (WITH RECURSIVE).
        ntag::T_WorkTableScan => {
            let s = nodeWorktablescan::ExecInitWorkTableScan(
                node, estate, eflags,
            )?;
            alloc_in(mcx, PlanStateNode::WorkTableScan(s))?
        }

        // case T_ForeignScan: ExecInitForeignScan((ForeignScan *) node, estate, eflags)
        ntag::T_ForeignScan => {
            let s = nodeForeignscan::ExecInitForeignScan(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::ForeignScan(s))?
        }

        // case T_CustomScan: ExecInitCustomScan(...) (nodeCustom.c)
        ntag::T_CustomScan => {
            let s = nodeCustom::ExecInitCustomScan(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::CustomScan(s))?
        }

        // ------------------------------------------------------------------
        // join nodes
        // ------------------------------------------------------------------
        // case T_NestLoop: ExecInitNestLoop((NestLoop *) node, estate, eflags)
        ntag::T_NestLoop => {
            let s = nodeNestloop::ExecInitNestLoop(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::NestLoop(s))?
        }

        // case T_MergeJoin: ExecInitMergeJoin((MergeJoin *) node, estate, eflags)
        ntag::T_MergeJoin => {
            let s = nodeMergejoin::ExecInitMergeJoin(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::MergeJoin(s))?
        }

        // case T_HashJoin: ExecInitHashJoin((HashJoin *) node, estate, eflags)
        ntag::T_HashJoin => {
            let s = nodeHashjoin::ExecInitHashJoin(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::HashJoin(s))?
        }

        // ------------------------------------------------------------------
        // materialization nodes
        // ------------------------------------------------------------------
        // case T_Material: ExecInitMaterial((Material *) node, estate, eflags)
        ntag::T_Material => {
            let s = nodeMaterial::ExecInitMaterial(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Material(s))?
        }

        // case T_Sort: ExecInitSort((Sort *) node, estate, eflags)
        ntag::T_Sort => {
            let s = nodeSort::ExecInitSort(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Sort(s))?
        }

        // case T_IncrementalSort:
        //   ExecInitIncrementalSort((IncrementalSort *) node, estate, eflags)
        ntag::T_IncrementalSort => {
            let s = nodeIncrementalSort::ExecInitIncrementalSort(
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
        ntag::T_Memoize => {
            let s = nodeMemoize::ExecInitMemoize(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Memoize(alloc_in(mcx, *s)?))?
        }

        // case T_Group: ExecInitGroup(...) (nodeGroup.c)
        ntag::T_Group => {
            let s = nodeGroup::ExecInitGroup(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Group(s))?
        }

        // case T_Agg: ExecInitAgg((Agg *) node, estate, eflags) (nodeAgg.c)
        //
        // `AggStateData` lives in `backend-executor-nodeAgg` (ABOVE `types-nodes`),
        // so the `PlanStateNode::Agg` variant carries it behind the owned,
        // tag-checked erased `AggStateLive` carrier (#324/#165 keystone). The
        // `ExecInitAgg` result is unsized into that trait object here.
        ntag::T_Agg => {
            let agg = node.expect_agg();
            // Pass the wrapping `&Node` too so ExecInitAgg can set
            // `aggstate->ss.ps.plan = (Plan *) node` (the plan back-link the
            // result projection reads its targetlist from).
            let s = nodeAgg::ExecInitAgg(agg, node, estate, eflags, mcx)?;
            let live = nodeAgg::erase_agg_state(s);
            alloc_in(mcx, PlanStateNode::Agg(live))?
        }

        // case T_WindowAgg: ExecInitWindowAgg((WindowAgg *) node, estate, eflags)
        ntag::T_WindowAgg => {
            let s = nodeWindowAgg::ExecInitWindowAgg(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::WindowAgg(s))?
        }

        // case T_Unique: ExecInitUnique(...) (nodeUnique.c)
        ntag::T_Unique => {
            let s = nodeUnique::ExecInitUnique(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Unique(s))?
        }

        // case T_Gather: ExecInitGather((Gather *) node, estate, eflags)
        ntag::T_Gather => {
            let s = nodeGather::ExecInitGather(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Gather(s))?
        }

        // case T_GatherMerge: ExecInitGatherMerge(...) (nodeGatherMerge.c)
        ntag::T_GatherMerge => {
            let s = nodeGatherMerge::ExecInitGatherMerge(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::GatherMerge(s))?
        }

        // case T_Hash: ExecInitHash((Hash *) node, estate, eflags)
        ntag::T_Hash => {
            let s = nodeHash::exec_hash::ExecInitHash(mcx, node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Hash(s))?
        }

        // case T_SetOp: ExecInitSetOp(...) (nodeSetop.c)
        ntag::T_SetOp => {
            let s = nodeSetOp::ExecInitSetOp(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::SetOp(s))?
        }

        // case T_LockRows: ExecInitLockRows((LockRows *) node, estate, eflags)
        ntag::T_LockRows => {
            let lr = node.expect_lockrows();
            let mut lrstate = nodeLockRows::ExecInitLockRows(lr, estate, eflags)?;
            // lrstate->ps.plan = (Plan *) node — the plan back-link (read by
            // EXPLAIN's ExplainNode). The seam-installed init_plan_state_links
            // only carries the &LockRows, not the enclosing &'mcx Node, so the
            // plan-tree alias is wired here where the dispatch holds it.
            lrstate.ps.plan = Some(node);
            alloc_in(mcx, PlanStateNode::LockRows(lrstate))?
        }

        // case T_Limit: ExecInitLimit((Limit *) node, estate, eflags)
        ntag::T_Limit => {
            let limitstate = nodeLimit::ExecInitLimit(node, estate, eflags)?;
            alloc_in(mcx, PlanStateNode::Limit(limitstate))?
        }

        // default:
        //   elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
        //   result = NULL;  /* keep compiler quiet */
        //
        // Reached for `Plan` tags that have no `Node` enum variant yet; each
        // node port that adds a variant adds its arm above.
        _ => return Err(unrecognized_node_type(node)),
    };

    // The post-dispatch init tail (parent back-link stamping, initPlan build,
    // instrumentation) is hoisted into a separate `#[inline(never)]` function so
    // its locals (the `SubPlan` clone, the `Instrumentation` box, the loop
    // bookkeeping) are NOT reserved in `exec_init_node`'s own frame. Because
    // `exec_init_node` recurses (each interior plan node inits its children),
    // keeping that tail out of the recursive frame removes its cost from every
    // level of the plan-state tree, mirroring C's pointer-passing frame size.
    exec_init_node_finish(mcx, node, estate, result)
}

/// The init tail of [`exec_init_node`], factored out (`#[inline(never)]`) so its
/// locals live in their own frame rather than inflating the recursive
/// `exec_init_node` frame. Behavior is identical to the original inline tail.
#[inline(never)]
fn exec_init_node_finish<'mcx>(
    mcx: Mcx<'mcx>,
    node: &'mcx Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut result: PgBox<'mcx, PlanStateNode<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, PlanStateNode<'mcx>>>> {
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

    // ModifyTable nodes own additional `ExprState`s that live on the per-result
    // relation `ResultRelInfo` in the `EState`, not on the node's `PlanState`
    // head: the RETURNING projection (`ri_projectReturning` — where a
    // `merge_action()` in RETURNING compiles to `EEOP_MERGE_SUPPORT_FUNC`), the
    // per-MERGE-action projections/quals (`mas_proj`/`mas_whenqual`), and the
    // MERGE join-condition qual (`ri_MergeJoinCondition`). C sets their
    // `state->parent = &mtstate->ps` at build time; the owned model stamps them
    // here, once the `PlanStateNode::ModifyTable` enum is address-stable, so
    // `EEOP_MERGE_SUPPORT_FUNC` can recover the `ModifyTableState` through
    // `parent.as_modify_table_state()`.
    if let ::nodes::PlanStateNode::ModifyTable(m) = &*result {
        let mut result_rel_ids: Vec<::nodes::execnodes::RriId> =
            m.resultRelInfo.iter().copied().collect();
        // An inherited-table MERGE INSERT uses rootResultRelInfo (not in
        // resultRelInfo[]); its ri_projectReturning (built by
        // ExecInitMergeInheritedRoot, which may hold merge_action()) must also be
        // stamped. C passes &mtstate->ps there too.
        if let Some(root) = m.rootResultRelInfo {
            if !result_rel_ids.contains(&root) {
                result_rel_ids.push(root);
            }
        }
        // Record the enclosing-enum back-link on the ModifyTableState so the
        // per-leaf-partition ExprStates built lazily by ExecInitPartitionInfo
        // (after this up-front stamp pass) can be stamped with the same
        // ModifyTableState identity. C passes `&mtstate->ps` as the expression
        // parent at every build site, including the lazy partition init.
        let link = ::nodes::planstate::PlanStateLink::from_ref(&*result);
        if let ::nodes::PlanStateNode::ModifyTable(m) = &mut *result {
            m.mt_self_link = Some(link);
        }
        result.stamp_modifytable_expr_parents(estate, &result_rel_ids);
    }

    // Agg nodes additionally own the compiled `evaltrans` ExprState on every
    // phase (and its cached recompiled variants). C sets `state->parent =
    // &aggstate->ss.ps` inside `ExecBuildAggTrans`; in the owned tree the
    // enclosing `PlanStateNode::Agg` only becomes address-stable here, so the
    // back-link onto each phase's `evaltrans` is stamped now (the
    // `EEOP_AGG_PLAIN_TRANS_*` / `EEOP_AGG_PLAIN_PERGROUP_NULLCHECK` interpreter
    // steps recover the AggState through it, exactly as `EEOP_GROUPING_FUNC`
    // recovers it from the qual/proj `parent`).
    stamp_agg_evaltrans_parents(&mut result);

    // ExecInitWholeRowVar (execExpr.c) builds the EEOP_WHOLEROW junk filter from
    // the SubqueryScan/CteScan subplan targetlist when that subplan emits resjunk
    // (ORDER BY/GROUP BY) columns, so the whole-row result keeps only the real
    // output columns. C does this during the per-expression compile because it
    // has the address-stable `state->parent` there; in the owned tree the
    // enclosing `PlanStateNode` enum is only stable here, so the deferred build
    // is done now (mirrors `stamp_expr_parents`'s parent back-fill rationale).
    exec_init_subqueryscan_wholerow_junk(mcx, &mut result, estate)?;

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
        for i in 0..init.len() {
            // Assert(IsA(subplan, SubPlan)); Assert(subplan->args == NIL);
            let subplan = &init[i];
            debug_assert!(subplan.args.is_empty());
            let plan_id = subplan.plan_id;
            let owned: PgBox<'mcx, ::nodes::primnodes::SubPlan<'mcx>> =
                alloc_in(mcx, subplan.clone_in(mcx)?)?;
            let sstate = nodeSubplan::ExecInitSubPlan(owned, estate)?;
            // The InitPlan SubPlanState is reached lazily by `plan_id` from the
            // param-eval path (ExecEvalParamExec -> ExecSetParamPlan), so it
            // lives in the estate registry keyed by the 1-based plan_id, not on
            // the parent PlanState's owning `initPlan` list (which the owned
            // model can't co-own with the lazy-eval reachability).
            let idx = (plan_id as usize).saturating_sub(1);
            while estate.es_initplan.len() <= idx {
                estate.es_initplan.push(None);
            }
            estate.es_initplan[idx] = Some(sstate);

            // Owned-model bookkeeping for the rescan re-arm: C keeps the
            // SubPlanState on `result->initPlan` and walks it in ExecReScan to
            // re-arm correlated InitPlans (ExecReScanSetParamPlan). Here the
            // SubPlanState lives single-owned in `es_initplan`; record this
            // node's InitPlan `plan_id` so ExecReScan can reach it.
            let head = result.ps_head_mut();
            if head.init_plan_ids.is_none() {
                head.init_plan_ids = Some(::mcx::PgVec::new_in(mcx));
            }
            let v = head.init_plan_ids.as_mut().expect("just set");
            v.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<i32>()))?;
            v.push(plan_id);
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
        let mut instr = instrument_seams::instr_alloc::call(
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
/// Stamp the `ExprState.parent` back-link onto every `evaltrans` ExprState an
/// Agg node owns (each phase's `evaltrans` + the cached recompiled variants),
/// pointing at the enclosing, now-address-stable `PlanStateNode::Agg`. The
/// owned-model rendering of C's `state->parent = &aggstate->ss.ps` in
/// `ExecBuildAggTrans` — done here because the enum wrapper's address only
/// becomes stable after `ExecInitAgg` returns and the node is boxed. No-op for
/// non-Agg nodes.
fn stamp_agg_evaltrans_parents<'mcx>(result: &mut PlanStateNode<'mcx>) {
    // Compute the back-link to the boxed node first (Copy; releases the &self
    // borrow) before taking the &mut downcast, mirroring stamp_expr_parents.
    let link = ::nodes::planstate::PlanStateLink::from_ref(&*result);
    let Some(agg) =
        result.as_agg_state_mut_typed::<nodeAgg::AggStateData<'mcx>>()
    else {
        return;
    };
    if let Some(phases) = agg.phases.as_mut() {
        for phase in phases.iter_mut() {
            if let Some(evaltrans) = phase.evaltrans.as_mut() {
                evaltrans.parent = Some(link);
            }
            for row in phase.evaltrans_cache.iter_mut() {
                for cached in row.iter_mut() {
                    if let Some(es) = cached.as_mut() {
                        es.parent = Some(link);
                    }
                }
            }
        }
    }
}

/// Deferred half of `ExecInitWholeRowVar` (execExpr.c): for a `SubqueryScan`
/// (or `CteScan`) node whose subplan emits resjunk columns, build a `JunkFilter`
/// from the subplan's targetlist and attach it to every `EEOP_WHOLEROW` step in
/// the node's own projection and qual ExprStates. The C builds this inside the
/// expression compile (it has `state->parent` there); the owned tree defers it
/// to here, where the enclosing `PlanStateNode` enum is address-stable, so the
/// subplan child and the node's compiled steps are both reachable.
fn exec_init_subqueryscan_wholerow_junk<'mcx>(
    mcx: Mcx<'mcx>,
    result: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    use ::nodes::execexpr::{ExprEvalOp, ExprEvalStepData};

    // C: switch (nodeTag(parent)) { case T_SubqueryScanState: subplan = ...->subplan;
    //                               case T_CteScanState: subplan = ...->cteplanstate; }
    // A CteScan's subplan lives only as the `ctePlanId` identity into
    // es_subplanstates (no owned child node), so `subquery_subplan_state`
    // yields the subplan PlanState only for a SubqueryScan — matching the C
    // `default:` NULL for a CteScan parent here.
    let Some(subplan) = result.subquery_subplan_state() else {
        return Ok(());
    };

    // C: foreach(tlist, subplan->plan->targetlist) if (tle->resjunk) needed=true;
    let Some(src_tlist) = subplan
        .ps_head()
        .plan
        .and_then(|n| n.plan_head().targetlist.as_ref())
    else {
        return Ok(());
    };
    if !src_tlist.iter().any(|tle| tle.resjunk) {
        return Ok(());
    }

    // The node's own ExprStates only carry an EEOP_WHOLEROW step if a whole-row
    // Var over this subquery RTE was compiled into them. Detect that before
    // building the (slot-allocating) junk filter, so we don't allocate for a
    // SubqueryScan that has no whole-row reference.
    let has_wholerow = {
        let head = result.ps_head();
        let in_state = |st: Option<&::nodes::execexpr::ExprState<'mcx>>| {
            st.and_then(|s| s.steps.as_ref())
                .map(|steps| {
                    steps
                        .iter()
                        .any(|s| s.opcode == ExprEvalOp::EEOP_WHOLEROW)
                })
                .unwrap_or(false)
        };
        in_state(head.ps_ProjInfo.as_ref().map(|p| &p.pi_state))
            || in_state(head.qual.as_deref())
    };
    if !has_wholerow {
        return Ok(());
    }

    // Snapshot the subplan targetlist (clone_in) so the &mut estate borrow used
    // to build the filter below does not alias the borrowed subplan PlanState.
    let mut src_tlist_snap = ::mcx::vec_with_capacity_in(mcx, src_tlist.len())?;
    for tle in src_tlist.iter() {
        src_tlist_snap.push(tle.clone_in(mcx)?);
    }

    // For every EEOP_WHOLEROW step that has no filter yet, build a fresh
    // JunkFilter (one fresh virtual result slot apiece, exactly as a per-step C
    // ExecInitWholeRowVar would) and attach it. C:
    //   scratch->d.wholerow.junkFilter =
    //       ExecInitJunkFilter(subplan->plan->targetlist,
    //                          ExecInitExtraTupleSlot(..., &TTSOpsVirtual));
    // ExecInitJunkFilter with slot=None allocates the equivalent virtual slot.
    //
    // Build the filters up front (needs &mut estate) into a queue, then drain the
    // queue while installing (needs &mut result), avoiding overlapping borrows.
    let n_wholerow = {
        let head = result.ps_head();
        let count = |st: Option<&::nodes::execexpr::ExprState<'mcx>>| -> usize {
            st.and_then(|s| s.steps.as_ref())
                .map(|steps| {
                    steps
                        .iter()
                        .filter(|s| {
                            s.opcode == ExprEvalOp::EEOP_WHOLEROW
                                && matches!(
                                    &s.d,
                                    ExprEvalStepData::WholeRow { junk_filter, .. }
                                        if junk_filter.is_none()
                                )
                        })
                        .count()
                })
                .unwrap_or(0)
        };
        count(head.ps_ProjInfo.as_ref().map(|p| &p.pi_state))
            + count(head.qual.as_deref())
    };

    let mut filters: Vec<PgBox<'mcx, ::nodes::execnodes::JunkFilter<'mcx>>> =
        Vec::with_capacity(n_wholerow);
    for _ in 0..n_wholerow {
        let mut tlist = ::mcx::vec_with_capacity_in(mcx, src_tlist_snap.len())?;
        for tle in src_tlist_snap.iter() {
            tlist.push(tle.clone_in(mcx)?);
        }
        let jf = execJunk::ExecInitJunkFilter(estate, tlist, None)?;
        filters.push(alloc_in(mcx, jf)?);
    }

    let head = result.ps_head_mut();
    let mut filters = filters.into_iter();
    let mut install = |st: Option<&mut ::nodes::execexpr::ExprState<'mcx>>| {
        if let Some(steps) = st.and_then(|s| s.steps.as_mut()) {
            for step in steps.iter_mut() {
                if step.opcode == ExprEvalOp::EEOP_WHOLEROW {
                    if let ExprEvalStepData::WholeRow { junk_filter, .. } = &mut step.d {
                        if junk_filter.is_none() {
                            *junk_filter = filters.next();
                        }
                    }
                }
            }
        }
    };
    install(head.ps_ProjInfo.as_mut().map(|p| &mut p.pi_state));
    install(head.qual.as_deref_mut());
    Ok(())
}

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
