//! Node-lifecycle family: init / end / rescan, the `ExecAgg` driver, and its
//! setup helpers (phase and grouping-set selection, input fetch, the column
//! analysis that decides which outer-plan columns are needed, and the
//! per-trans build that reads the catalog for each aggregate).

use mcx::{alloc_in, Mcx, PgBox};
use types_core::primitive::Oid;
use types_datum::Datum;
use types_error::PgResult;
use types_nodes::nodeagg::{
    Aggref, AggStateData, AggStatePerTransData, AGG_HASHED, AGG_MIXED, AGG_PLAIN, AGG_SORTED,
};
use types_nodes::nodes::Node;
use types_nodes::{Bitmapset, EStateData, SlotId};

use crate::FindColsContext;

/// `curaggcontext` sentinel selecting `hashcontext` rather than an entry of
/// `aggcontexts`. C aliases either a per-grouping-set `ExprContext *` or the
/// single `hashcontext`; in the index-based model a non-negative value indexes
/// `aggcontexts` and this sentinel names `hashcontext`.
pub const CURAGGCONTEXT_HASH: i32 = -1;

/// `select_current_set(aggstate, setno, is_hash)` — select the current
/// grouping set; affects `current_set` and `curaggcontext`.
pub fn select_current_set(aggstate: &mut AggStateData<'_>, setno: i32, is_hash: bool) {
    // When changing this, also adapt ExecAggPlainTransByVal() and
    // ExecAggPlainTransByRef().
    if is_hash {
        aggstate.curaggcontext = CURAGGCONTEXT_HASH;
    } else {
        aggstate.curaggcontext = setno;
    }

    aggstate.current_set = setno;
}

/// `initialize_phase(aggstate, newphase)` — switch the Agg to a new phase,
/// resetting sort state as needed.
pub fn initialize_phase<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    newphase: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(newphase <= 1 || newphase == aggstate.current_phase + 1);

    // Whatever the previous state, we're now done with whatever input
    // tuplesort was in use.
    if let Some(sort_in) = aggstate.sort_in.take() {
        backend_utils_sort_tuplesort_seams::tuplesort_end::call(sort_in)?;
    }

    if newphase <= 1 {
        // Discard any existing output tuplesort.
        if let Some(sort_out) = aggstate.sort_out.take() {
            backend_utils_sort_tuplesort_seams::tuplesort_end::call(sort_out)?;
        }
    } else {
        // The old output tuplesort becomes the new input one, and this is the
        // right time to actually sort it.
        aggstate.sort_in = aggstate.sort_out.take();
        debug_assert!(aggstate.sort_in.is_some());
        let sort_in = aggstate.sort_in.as_mut().expect("sort_in set above");
        backend_utils_sort_tuplesort_seams::tuplesort_performsort::call(sort_in)?;
    }

    // If this isn't the last phase, we need to sort appropriately for the
    // next phase in sequence.
    if newphase > 0 && newphase < aggstate.numphases - 1 {
        let phases = aggstate
            .phases
            .as_ref()
            .expect("phases array set by ExecInitAgg");
        let next = &phases[(newphase + 1) as usize];
        let sortnode = next
            .sortnode
            .as_ref()
            .expect("next phase has a Sort node");
        let num_cols = sortnode.numCols;
        let sort_col_idx = &sortnode.sortColIdx;
        let sort_operators = &sortnode.sortOperators;
        let collations = &sortnode.collations;
        let nulls_first = &sortnode.nullsFirst;

        // TupleDesc tupDesc = ExecGetResultType(outerPlanState(aggstate));
        let outer = aggstate
            .ss
            .ps
            .lefttree
            .as_deref()
            .expect("Agg has an outer plan");
        let tup_desc =
            backend_executor_execTuples_seams::exec_get_result_type::call(outer.ps_head())
                .expect("outer plan result type set at init");

        let work_mem = work_mem();
        let sort_out = backend_utils_sort_tuplesort_seams::tuplesort_begin_heap::call(
            estate.es_query_cxt,
            tup_desc,
            num_cols,
            sort_col_idx,
            sort_operators,
            collations,
            nulls_first,
            work_mem,
            TUPLESORT_NONE,
        )?;
        // The Tuplesortstate * lives in the EState per-query context.
        aggstate.sort_out = Some(alloc_in(estate.es_query_cxt, sort_out)?);
    }

    aggstate.current_phase = newphase;
    aggstate.phase = newphase;
    Ok(())
}

/// `work_mem` (guc.c) — the planner/executor working-memory limit in
/// kilobytes. Owned by the GUC subsystem; the executor reads the per-backend
/// value. Until the GUC owner lands the default (4 MB) stands in.
fn work_mem() -> i32 {
    4096
}

/// `TUPLESORT_NONE` (tuplesort.h) — no sort options.
const TUPLESORT_NONE: i32 = 0;

/// `fetch_input_tuple(aggstate)` — read the next input tuple (from the sort
/// for phases > 0, else from the outer plan); returns `None` at end of input.
pub fn fetch_input_tuple<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let slot: Option<SlotId>;

    if let Some(sort_in) = aggstate.sort_in.as_mut() {
        // make sure we check for interrupts in either path through here
        backend_tcop_postgres_seams::check_for_interrupts::call()?;
        let sort_slot = aggstate.sort_slot.expect("sort_slot set when sorting");
        let found = backend_utils_sort_tuplesort_seams::tuplesort_gettupleslot::call(
            sort_in,
            true,
            false,
            estate.slot_mut(sort_slot),
        )?;
        if !found {
            return Ok(None);
        }
        slot = Some(sort_slot);
    } else {
        let outer = aggstate
            .ss
            .ps
            .lefttree
            .as_deref_mut()
            .expect("Agg has an outer plan");
        slot = backend_executor_execProcnode_seams::exec_proc_node::call(outer, estate)?;
    }

    // if (!TupIsNull(slot) && aggstate->sort_out)
    if let (Some(s), Some(sort_out)) = (slot, aggstate.sort_out.as_mut()) {
        backend_utils_sort_tuplesort_seams::tuplesort_puttupleslot::call(
            sort_out,
            estate.slot(s),
        )?;
    }

    Ok(slot)
}

/// `find_cols(aggstate, &aggregated, &unaggregated)` — find the columns the
/// outer plan must supply, split into those referenced under an Aggref and
/// those referenced elsewhere.
pub fn find_cols<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<(
    Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    Option<PgBox<'mcx, Bitmapset<'mcx>>>,
)> {
    // Agg *agg = (Agg *) aggstate->ss.ps.plan;
    let mut context = FindColsContext {
        is_aggref: false,
        aggregated: None,
        unaggregated: None,
    };

    // Examine tlist and quals. The targetlist and qual are expression trees
    // owned by the not-yet-ported nodes/nodeFuncs.c walker; the walk runs
    // through that owner's seam (find_cols_walker is the callback). The Agg
    // plan node's targetlist/qual are reached off the shared plan tree.
    let agg_node = agg_plan(aggstate)?;
    find_cols_walker(agg_node.targetlist.as_deref(), &mut context)?;
    find_cols_walker(agg_node.qual.as_deref(), &mut context)?;

    // In some cases, grouping columns will not appear in the tlist.
    let num_cols = agg_node.num_cols;
    let grp_col_idx = agg_node.grp_col_idx.clone_for_read();
    for i in 0..num_cols {
        let attno = grp_col_idx[i as usize] as i32;
        context.unaggregated = Some(backend_nodes_core_seams::bms_add_member::call(
            mcx,
            context.unaggregated.take(),
            attno,
        )?);
    }

    Ok((context.aggregated, context.unaggregated))
}

/// `(Agg *) aggstate->ss.ps.plan` — read the Agg plan node off the shared,
/// read-only plan tree the node-state aliases. The plan tree's `Node` enum
/// does not yet carry the `T_Agg` variant (it lands with the planner's Agg
/// node port), so the projection through it goes through the plan owner.
fn agg_plan<'a, 'mcx>(
    _aggstate: &'a AggStateData<'mcx>,
) -> PgResult<&'a AggPlanView<'mcx>> {
    // The Agg plan node is reached through the shared plan tree (`ss.ps.plan`,
    // a `&Node`). The `Node` vocabulary on main does not yet define the
    // `T_Agg` variant, so this projection is owned by the planner-plan-node
    // unit; until it lands, the access panics loudly rather than fabricate a
    // stand-in node shape.
    panic!(
        "backend-nodes-plannodes: Agg plan-node variant (T_Agg) not yet defined in the \
         shared Node vocabulary; find_cols/find_hash_columns cannot read ss.ps.plan"
    )
}

/// A read-only view of the `Agg` plan-node fields the column analysis consumes
/// (`plan.targetlist`, `plan.qual`, `numCols`, `grpColIdx`). Defined by the
/// planner-plannodes owner when the `Node::Agg` variant lands.
pub struct AggPlanView<'mcx> {
    /// `plan.targetlist`.
    pub targetlist: Option<PgBox<'mcx, Node<'mcx>>>,
    /// `plan.qual`.
    pub qual: Option<PgBox<'mcx, Node<'mcx>>>,
    /// `numCols`.
    pub num_cols: i32,
    /// `grpColIdx`.
    pub grp_col_idx: GrpColIdxView,
}

/// Helper carrying `grpColIdx`; abstracts the slice read so the planner-owned
/// view can supply it when the `Node::Agg` variant lands.
pub struct GrpColIdxView;

impl GrpColIdxView {
    fn clone_for_read(&self) -> &[i16] {
        // Reached only after agg_plan() panics for the missing Node variant;
        // present for signature parity.
        &[]
    }
}

/// `find_cols_walker(node, context)` — expression walker collecting referenced
/// `Var` colnos into the context, marking aggregated vs unaggregated.
pub fn find_cols_walker<'mcx>(
    node: Option<&Node<'mcx>>,
    context: &mut FindColsContext<'mcx>,
) -> PgResult<bool> {
    // if (node == NULL) return false;
    let Some(_node) = node else {
        return Ok(false);
    };

    // if (IsA(node, Var)) { ... } / if (IsA(node, Aggref)) { ... } /
    // return expression_tree_walker(node, find_cols_walker, context);
    //
    // The `Var`/`Aggref`/general expression node tags are not yet present in
    // the shared `Node` enum, and `expression_tree_walker` is owned by the
    // unported `nodes/nodeFuncs.c`. The walk therefore belongs to that owner;
    // dispatching it requires the expression-node vocabulary it defines. Until
    // it lands the walk panics loudly rather than silently dropping columns.
    let _ = context;
    panic!(
        "backend-nodes-nodeFuncs: expression_tree_walker / Var / Aggref node vocabulary \
         not yet ported; find_cols_walker cannot classify column references"
    )
}

/// `find_hash_columns(aggstate)` — set up the per-hash column descriptors
/// (input/hash slot column indices) for every grouping set.
pub fn find_hash_columns<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // Find Vars that will be needed in tlist and qual. find_cols itself
    // depends on the unported expression-walker vocabulary (see find_cols),
    // so the whole hash-column analysis lands with it.
    let _ = (aggstate, mcx);
    panic!(
        "backend-nodes-nodeFuncs: find_hash_columns depends on find_cols' expression walk, \
         which needs the unported nodeFuncs expression-node vocabulary"
    )
}

/// `build_pertrans_for_aggref(...)` — set up one `AggStatePerTransData` from
/// the aggregate's catalog rows (transfn/serialfn/deserialfn lookups, input
/// type metadata, sort/distinct comparators, per-set sort objects).
#[allow(clippy::too_many_arguments)]
pub fn build_pertrans_for_aggref<'mcx>(
    pertrans: &mut AggStatePerTransData<'mcx>,
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    aggref: &Aggref<'mcx>,
    transfn_oid: Oid,
    aggtranstype: Oid,
    aggserialfn: Oid,
    aggdeserialfn: Oid,
    init_value: Datum,
    init_value_is_null: bool,
    input_types: &[Oid],
    num_arguments: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let _ = (
        pertrans,
        aggstate,
        estate,
        aggref,
        transfn_oid,
        aggtranstype,
        aggserialfn,
        aggdeserialfn,
        init_value,
        init_value_is_null,
        input_types,
        num_arguments,
        mcx,
    );
    // build_pertrans_for_aggref drives fmgr_info / build_aggregate_*_expr /
    // InitFunctionCallInfoData / get_typlenbyval / get_sortgroupclause_tle /
    // exprCollation / get_opcode / execTuplesMatchPrepare, plus ExecTypeFromTL
    // and ExecInitExtraTupleSlot. Several of these (build_aggregate_*_expr in
    // parse_agg.c, fmgr_info in fmgr.c's still-unported surface,
    // get_sortgroupclause_tle/exprCollation in nodeFuncs.c) are not exposed by
    // any seam crate this unit depends on, and the SortGroupClause→TargetEntry
    // resolution needs the expression-node vocabulary that has not landed.
    panic!(
        "backend-parser-parse-agg / backend-nodes-nodeFuncs: build_pertrans_for_aggref needs \
         build_aggregate_*_expr, fmgr_info, get_sortgroupclause_tle and exprCollation, none of \
         which are exposed by this unit's seam dependencies yet"
    )
}

/// `GetAggInitVal(textInitVal, transtype)` — convert the `agginitval` text
/// Datum into the transition type's internal Datum via its input function.
pub fn GetAggInitVal(text_init_val: Datum, transtype: Oid) -> PgResult<Datum> {
    // getTypeInputInfo(transtype, &typinput, &typioparam);
    // strInitVal = TextDatumGetCString(textInitVal);
    // initVal = OidInputFunctionCall(typinput, strInitVal, typioparam, -1);
    //
    // getTypeInputInfo (lsyscache.c) and OidInputFunctionCall (fmgr.c) are not
    // exposed by this unit's seam dependencies; the conversion lands with
    // them.
    let _ = (text_init_val, transtype);
    panic!(
        "backend-utils-cache-lsyscache / backend-utils-fmgr: GetAggInitVal needs \
         getTypeInputInfo + OidInputFunctionCall, not exposed by this unit's seams yet"
    )
}

/// `ExecAgg(pstate)` — the node's `ExecProcNode` callback: produce the next
/// aggregated output tuple, or `None` at end. Dispatches on strategy/phase to
/// the sorted-grouping and hash-grouping retrieve paths.
pub fn ExecAgg<'mcx>(
    pstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let mut result: Option<SlotId> = None;

    backend_tcop_postgres_seams::check_for_interrupts::call()?;

    if !pstate.agg_done {
        // Dispatch based on strategy of the current phase.
        let strategy = current_phase_strategy(pstate);
        match strategy {
            AGG_HASHED => {
                if !pstate.table_filled {
                    crate::hash_grouping::agg_fill_hash_table(pstate, estate)?;
                }
                // FALLTHROUGH
                result = crate::hash_grouping::agg_retrieve_hash_table(pstate, estate)?;
            }
            AGG_MIXED => {
                result = crate::hash_grouping::agg_retrieve_hash_table(pstate, estate)?;
            }
            AGG_PLAIN | AGG_SORTED => {
                result = crate::sorted_grouping::agg_retrieve_direct(pstate, estate)?;
            }
        }

        // if (!TupIsNull(result)) return result;
        if result.is_some() {
            return Ok(result);
        }
    }

    Ok(None)
}

/// `aggstate->phase->aggstrategy` — strategy of the current phase. `phase` is
/// the index of the active `AggStatePerPhaseData` in `phases`.
fn current_phase_strategy(aggstate: &AggStateData<'_>) -> types_nodes::nodeagg::AggStrategy {
    let phases = aggstate
        .phases
        .as_ref()
        .expect("phases array set by ExecInitAgg");
    phases[aggstate.phase as usize].aggstrategy
}

/// `ExecEndAgg(node)` — shut down the Agg node: end sorts, close hash tapes,
/// release contexts.
pub fn ExecEndAgg<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let num_grouping_sets = i32::max(node.maxsets, 1);

    // When ending a parallel worker, copy the statistics gathered by the
    // worker back into shared memory so that it can be picked up by the main
    // process to report in EXPLAIN ANALYZE.
    if node.shared_info.is_some() && is_parallel_worker() {
        let worker = parallel_worker_number();
        let si = node
            .shared_info
            .as_mut()
            .expect("shared_info checked is_some")
            .sinstrument
            .as_mut()
            .expect("sinstrument allocated in InitializeDSM");
        debug_assert!(worker <= node.numaggs); // ParallelWorkerNumber <= num_workers
        let slot = &mut si[worker as usize];
        slot.hash_batches_used = node.hash_batches_used;
        slot.hash_disk_used = node.hash_disk_used;
        slot.hash_mem_peak = node.hash_mem_peak;
    }

    // Make sure we have closed any open tuplesorts.
    if let Some(sort_in) = node.sort_in.take() {
        backend_utils_sort_tuplesort_seams::tuplesort_end::call(sort_in)?;
    }
    if let Some(sort_out) = node.sort_out.take() {
        backend_utils_sort_tuplesort_seams::tuplesort_end::call(sort_out)?;
    }

    crate::spill::hashagg_reset_spill_state(node)?;

    // The hash meta/table contexts are owned child MemoryContexts; dropping
    // their handles is the C MemoryContextDelete.
    if let Some(metacxt) = node.hash_metacxt.take() {
        drop(metacxt);
    }
    if let Some(tablecxt) = node.hash_tablecxt.take() {
        drop(tablecxt);
    }

    // End any per-set open sorts of every transition state.
    let numtrans = node.numtrans;
    if let Some(pertrans) = node.pertrans.as_mut() {
        for transno in 0..numtrans as usize {
            if let Some(sortstates) = pertrans[transno].sortstates.as_mut() {
                for setno in 0..num_grouping_sets as usize {
                    if let Some(ts) = sortstates[setno].take() {
                        backend_utils_sort_tuplesort_seams::tuplesort_end::call(ts)?;
                    }
                }
            }
        }
    }

    // And ensure any agg shutdown callbacks have been called. ReScanExprContext
    // fires the registered reset callbacks; the owned ExprContext model runs
    // those on reset of its per-tuple context.
    for setno in 0..num_grouping_sets as usize {
        rescan_expr_context_aggcontext(node, setno);
    }
    if node.hashcontext.is_some() {
        rescan_expr_context_hashcontext(node);
    }

    // outerPlan = outerPlanState(node); ExecEndNode(outerPlan);
    if let Some(outer) = node.ss.ps.lefttree.as_deref_mut() {
        backend_executor_execProcnode_seams::exec_end_node::call(outer, estate)?;
    }

    Ok(())
}

/// `ReScanExprContext(aggstate->aggcontexts[setno])` — fire the per-set
/// aggregate context's reset callbacks. The owned `ExprContext` carries its
/// per-tuple context, whose `reset` runs the LIFO callbacks.
fn rescan_expr_context_aggcontext<'mcx>(node: &mut AggStateData<'mcx>, setno: usize) {
    if let Some(aggcontexts) = node.aggcontexts.as_mut() {
        if let Some(cxt) = aggcontexts.get_mut(setno) {
            cxt.ecxt_per_tuple_memory.reset();
        }
    }
}

/// `ReScanExprContext(aggstate->hashcontext)`.
fn rescan_expr_context_hashcontext<'mcx>(node: &mut AggStateData<'mcx>) {
    if let Some(cxt) = node.hashcontext.as_mut() {
        cxt.ecxt_per_tuple_memory.reset();
    }
}

/// `IsParallelWorker()` (parallel.h) — am I a parallel worker backend? Owned by
/// the parallel-infra; per-backend identity. Defaults to `false` (the leader)
/// until that owner lands.
fn is_parallel_worker() -> bool {
    false
}

/// `ParallelWorkerNumber` (parallel.c) — this worker's index, or -1 in the
/// leader. Per-backend identity owned by the parallel infra.
fn parallel_worker_number() -> i32 {
    -1
}

/// `ExecReScanAgg(node)` — rescan the Agg node, re-using the hash table where
/// the rescan parameters allow it.
pub fn ExecReScanAgg<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let num_grouping_sets = i32::max(node.maxsets, 1);

    node.agg_done = false;

    if node.aggstrategy == AGG_HASHED {
        // In the hashed case, if we haven't yet built the hash table then we
        // can just return; nothing done yet, so nothing to undo.
        if !node.table_filled {
            return Ok(());
        }

        // If we do have the hash table, and it never spilled, and the subplan
        // does not have any parameter changes, and none of our own parameter
        // changes affect input expressions of the aggregated functions, then
        // we can just rescan the existing hash table; no need to build it
        // again.
        let outer_chg_param_null = node
            .ss
            .ps
            .lefttree
            .as_deref()
            .map(|o| o.ps_head().chgParam.is_none())
            .unwrap_or(true);
        let agg_params = agg_plan_agg_params(node)?;
        let overlap = backend_nodes_core_seams::bms_overlap::call(
            node.ss.ps.chgParam.as_deref(),
            agg_params,
        );
        if outer_chg_param_null && !node.hash_ever_spilled && !overlap {
            // ResetTupleHashIterator(perhash[0].hashtable, &perhash[0].hashiter)
            let perhash = node
                .perhash
                .as_mut()
                .expect("perhash set in hashed strategy");
            let ht = perhash[0].hashtable.expect("hashtable built (table_filled)");
            perhash[0].hashiter =
                backend_executor_execGrouping_seams::init_tuple_hash_iterator::call(ht);
            select_current_set(node, 0, true);
            return Ok(());
        }
    }

    // Make sure we have closed any open tuplesorts.
    let numtrans = node.numtrans;
    if let Some(pertrans) = node.pertrans.as_mut() {
        for transno in 0..numtrans as usize {
            if let Some(sortstates) = pertrans[transno].sortstates.as_mut() {
                for setno in 0..num_grouping_sets as usize {
                    if let Some(ts) = sortstates[setno].take() {
                        backend_utils_sort_tuplesort_seams::tuplesort_end::call(ts)?;
                    }
                }
            }
        }
    }

    // Reset our per-grouping-set contexts, which may have transvalues stored
    // in them. (Rescan rather than reset because transfns may have registered
    // callbacks that need to be run now.)
    for setno in 0..num_grouping_sets as usize {
        rescan_expr_context_aggcontext(node, setno);
    }

    // Release first tuple of group, if we have made a copy.
    if let Some(_first) = node.grp_first_tuple.take() {
        // heap_freetuple: the owned HeapTuple box drops here.
    }
    // ExecClearTuple(node->ss.ss_ScanTupleSlot);
    if let Some(slot_id) = node.ss.ss_ScanTupleSlot {
        clear_slot(estate, slot_id)?;
    }

    // Forget current agg values: MemSet(ecxt_aggvalues/ecxt_aggnulls, 0).
    clear_agg_values(node);

    // With AGG_HASHED/MIXED, the hash table is allocated in a sub-context of
    // the hashcontext. Resetting a context automatically deletes sub-contexts.
    if node.aggstrategy == AGG_HASHED || node.aggstrategy == AGG_MIXED {
        crate::spill::hashagg_reset_spill_state(node)?;

        node.hash_ever_spilled = false;
        node.hash_spill_mode = false;
        node.hash_ngroups_current = 0;

        rescan_expr_context_hashcontext(node);
        if let Some(tablecxt) = node.hash_tablecxt.as_mut() {
            tablecxt.reset();
        }
        // Rebuild an empty hash table.
        crate::hash_grouping::build_hash_tables(node, estate)?;
        node.table_filled = false;
        // iterator will be reset when the table is filled

        crate::hash_grouping::hashagg_recompile_expressions(node, false, false, estate)?;
    }

    if node.aggstrategy != AGG_HASHED {
        // Reset the per-group state (in particular, mark transvalues null).
        let numaggs = node.numaggs;
        if let Some(pergroups) = node.pergroups.as_mut() {
            for setno in 0..num_grouping_sets as usize {
                if let Some(set) = pergroups.get_mut(setno).and_then(|s| s.as_mut()) {
                    for i in 0..numaggs as usize {
                        if let Some(pg) = set.get_mut(i) {
                            *pg = Default::default();
                        }
                    }
                }
            }
        }

        // reset to phase 1
        initialize_phase(node, 1, estate)?;

        node.input_done = false;
        node.projected_set = -1;
    }

    // if (outerPlan->chgParam == NULL) ExecReScan(outerPlan);
    let outer_chg_param_null = node
        .ss
        .ps
        .lefttree
        .as_deref()
        .map(|o| o.ps_head().chgParam.is_none())
        .unwrap_or(true);
    if outer_chg_param_null {
        exec_rescan_outer(node, estate)?;
    }

    Ok(())
}

/// `(Agg *) node->ss.ps.plan ; aggnode->aggParams` — the Agg plan node's
/// `aggParams` bitmapset, read off the shared plan tree. Owned by the planner
/// plan-node unit (the `Node::Agg` variant has not landed).
fn agg_plan_agg_params<'a, 'mcx>(
    _node: &'a AggStateData<'mcx>,
) -> PgResult<Option<&'a Bitmapset<'mcx>>> {
    panic!(
        "backend-nodes-plannodes: Agg plan-node variant (T_Agg) not yet defined; \
         ExecReScanAgg cannot read aggnode->aggParams off ss.ps.plan"
    )
}

/// `ExecClearTuple(slot)` (execTuples.c).
fn clear_slot<'mcx>(estate: &mut EStateData<'mcx>, slot_id: SlotId) -> PgResult<()> {
    let slot = &mut estate.es_tupleTable[slot_id.0 as usize];
    backend_executor_execTuples_seams::exec_clear_tuple::call(slot)
}

/// `MemSet(econtext->ecxt_aggvalues/ecxt_aggnulls, 0, ...)` — forget the
/// current aggregate values held in the node's per-output-tuple ExprContext.
/// The aggvalues/aggnulls arrays live on the EState-owned ExprContext the node
/// projects through; clearing them is owned by execUtils alongside the
/// EcxtId-addressed context pool. The owned AggState model does not carry the
/// EcxtId for ps_ExprContext on its facet here, so the clear lands with the
/// execUtils ExprContext model.
fn clear_agg_values<'mcx>(_node: &mut AggStateData<'mcx>) {
    // No-op placeholder: ecxt_aggvalues/ecxt_aggnulls are owned by the
    // EState-side ExprContext (EcxtId pool); the rescan owner clears them.
    // The C MemSet is exactly this and must run when that facet is threaded.
}

/// `ExecReScan(outerPlanState(node))` (execAmi.c) — rescan the child plan
/// subtree. Owned by the execAmi unit; routed through the node-dispatch seam
/// when it lands. The outer plan state head is lent for the rescan.
fn exec_rescan_outer<'mcx>(
    _node: &mut AggStateData<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    panic!(
        "backend-executor-execAmi: ExecReScan(outerPlanState) not exposed by this unit's \
         seam dependencies yet"
    )
}
