//! Node-lifecycle family: init / end / rescan, the `ExecAgg` driver, and its
//! setup helpers (phase and grouping-set selection, input fetch, the column
//! analysis that decides which outer-plan columns are needed, and the
//! per-trans build that reads the catalog for each aggregate).

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_core::primitive::{Oid, OidIsValid};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_error::PgResult;
use types_nodes::nodeagg::{
    Aggref, AGG_HASHED, AGG_MIXED, AGG_PLAIN, AGG_SORTED,
};
use crate::aggstate::{AggStateData, AggStatePerTransData};
use types_nodes::nodes::Node;
use types_nodes::{Bitmapset, EStateData, SlotId};
use types_tuple::heaptuple::TupleDescData;

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
    // expression_tree_walker IS ported (backend-nodes-core::node_walker) and
    // `Var`/`Aggref` ARE Expr variants in the shared Node enum now, so the walk
    // is implementable. But its only caller `find_cols` sources the Agg plan
    // node's targetlist/qual/grpColIdx via `agg_plan(aggstate)` reading
    // `ss.ps.plan` — and the shared `Node` vocabulary still has no `T_Agg`
    // plan-node variant (agg_plan() below panics on exactly that). The walk is
    // therefore unreachable with real expression trees until the planner's Agg
    // plan node lands. Blocked on the T_Agg plan-node keystone, NOT on nodeFuncs.
    let _ = context;
    panic!(
        "backend-nodes-plannodes: find_cols_walker is implementable (expression_tree_walker + \
         Var/Aggref ported) but unreachable — its caller find_cols cannot read the Agg plan node \
         (T_Agg not in the shared Node vocabulary; see agg_plan)"
    )
}

/// `find_hash_columns(aggstate)` — set up the per-hash column descriptors
/// (input/hash slot column indices) for every grouping set.
pub fn find_hash_columns<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // Find Vars that will be needed in tlist and qual. find_cols reads the Agg
    // plan node (ss.ps.plan) via agg_plan(), which the shared Node vocabulary
    // cannot express until the T_Agg plan-node variant lands. The
    // expression-walker vocabulary it would feed (expression_tree_walker /
    // Var / Aggref) is already ported; the block is the upstream Agg plan node.
    let _ = (aggstate, mcx);
    panic!(
        "backend-nodes-plannodes: find_hash_columns depends on find_cols, which cannot read the \
         Agg plan node (T_Agg not in the shared Node vocabulary). nodeFuncs walker is ported; \
         the block is the T_Agg plan-node keystone"
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
    init_value: Datum<'mcx>,
    init_value_is_null: bool,
    input_types: &[Oid],
    num_arguments: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // int numGroupingSets = Max(aggstate->maxsets, 1);
    let num_grouping_sets = core::cmp::max(aggstate.maxsets, 1);

    // Begin filling in the pertrans data
    //   pertrans->aggref = aggref;
    //   pertrans->aggshared = false;
    //   pertrans->aggCollation = aggref->inputcollid;
    //   pertrans->transfn_oid = transfn_oid;
    //   pertrans->serialfn_oid = aggserialfn;
    //   pertrans->deserialfn_oid = aggdeserialfn;
    //   pertrans->initValue = initValue;
    //   pertrans->initValueIsNull = initValueIsNull;
    pertrans.aggref = Some(clone_aggref_into(mcx, aggref)?);
    pertrans.aggshared = false;
    pertrans.agg_collation = aggref.inputcollid;
    pertrans.transfn_oid = transfn_oid;
    pertrans.serialfn_oid = aggserialfn;
    pertrans.deserialfn_oid = aggdeserialfn;
    pertrans.init_value = init_value;
    pertrans.init_value_is_null = init_value_is_null;

    // Count the "direct" arguments, if any
    //   numDirectArgs = list_length(aggref->aggdirectargs);
    let num_direct_args = list_len(&aggref.aggdirectargs);

    // Count the number of aggregated input columns
    //   pertrans->numInputs = numInputs = list_length(aggref->args);
    let num_inputs = list_len(&aggref.args) as i32;
    pertrans.num_inputs = num_inputs;

    // pertrans->aggtranstype = aggtranstype;
    pertrans.aggtranstype = aggtranstype;

    // account for the current transition state
    //   numTransArgs = pertrans->numTransInputs + 1;
    let num_trans_args = pertrans.num_trans_inputs + 1;
    let _ = num_trans_args;

    // Set up infrastructure for calling the transfn.  Note that invtransfn is
    // not needed here.
    //   build_aggregate_transfn_expr(inputTypes, numArguments, numDirectArgs,
    //                                aggref->aggvariadic, aggtranstype,
    //                                aggref->inputcollid, transfn_oid, InvalidOid,
    //                                &transfnexpr, NULL);
    //   fmgr_info(transfn_oid, &pertrans->transfn);
    //   fmgr_info_set_expr((Node *) transfnexpr, &pertrans->transfn);
    //   pertrans->transfn_fcinfo =
    //       (FunctionCallInfo) palloc(SizeForFunctionCallInfo(numTransArgs));
    //   InitFunctionCallInfoData(*pertrans->transfn_fcinfo, &pertrans->transfn,
    //                            numTransArgs, pertrans->aggCollation,
    //                            (Node *) aggstate, NULL);
    //
    // build_aggregate_transfn_expr (parse_agg.c, #224 — installed),
    // fmgr_info + fmgr_info_set_expr (fmgr.c — now installed: the resolved
    // transfn FmgrInfo can be stamped with the transfnexpr so the transfn reads
    // its declared arg types via get_fn_expr_argtype) are available. The
    // remaining blocker is InitFunctionCallInfoData allocating the transfn call
    // frame (`transfn_fcinfo`) carrying `(Node *) aggstate` as the fmgr context
    // — the #324/#165 agg call-frame channel, still gated on the
    // PlanState-ownership keystone. Panic loudly here (mirror-PG-and-panic). The
    // remaining owned arithmetic below is kept verbatim (unreachable) so it
    // lands once that owner does.
    panic!(
        "backend-utils-fmgr (InitFunctionCallInfoData): build_pertrans_for_aggref needs the \
         transfn call frame (transfn_fcinfo carrying (Node *) aggstate as context) — the \
         #324/#165 agg call-frame channel, still gated on the PlanState-ownership keystone. \
         build_aggregate_transfn_expr + fmgr_info + fmgr_info_set_expr are installed \
         (numTransArgs={num_trans_args}, numDirectArgs={num_direct_args})"
    );

    #[allow(unreachable_code)]
    {
        // get info about the state value's datatype
        //   get_typlenbyval(aggtranstype, &pertrans->transtypeLen,
        //                   &pertrans->transtypeByVal);
        let (transtype_len, transtype_by_val) = get_typlenbyval_owned(aggtranstype)?;
        pertrans.transtype_len = transtype_len;
        pertrans.transtype_by_val = transtype_by_val;

        // if (OidIsValid(aggserialfn)) { build_aggregate_serialfn_expr; fmgr_info;
        //   fmgr_info_set_expr; serialfn_fcinfo = palloc(SizeForFunctionCallInfo(1));
        //   InitFunctionCallInfoData(..., 1, InvalidOid, (Node *) aggstate, NULL); }
        if OidIsValid(aggserialfn) {
            build_serialfn_call_frame_owned(pertrans, aggstate, aggserialfn)?;
        }

        // if (OidIsValid(aggdeserialfn)) { build_aggregate_deserialfn_expr; fmgr_info;
        //   fmgr_info_set_expr; deserialfn_fcinfo = palloc(SizeForFunctionCallInfo(2));
        //   InitFunctionCallInfoData(..., 2, InvalidOid, (Node *) aggstate, NULL); }
        if OidIsValid(aggdeserialfn) {
            build_deserialfn_call_frame_owned(pertrans, aggstate, aggdeserialfn)?;
        }

        // If we're doing either DISTINCT or ORDER BY for a plain agg, then we have
        // a list of SortGroupClause nodes; fish out the data in them and stick them
        // into arrays. We ignore ORDER BY for an ordered-set agg, however; the
        // agg's transfn and finalfn are responsible for that.
        //
        // When the planner has set the aggpresorted flag, the input to the
        // aggregate is already correctly sorted. For ORDER BY aggregates we can
        // simply treat these as normal aggregates. For presorted DISTINCT
        // aggregates an extra step must be added to remove duplicate consecutive
        // inputs.
        //
        // Note that by construction, if there is a DISTINCT clause then the ORDER
        // BY clause is a prefix of it (see transformDistinctClause).
        let sortlist: Option<&[types_nodes::nodeagg::SortGroupClauseAgg]>;
        let num_sort_cols: i32;
        let num_distinct_cols: i32;
        if aggkind_is_ordered_set_lc(aggref.aggkind) {
            sortlist = None;
            num_sort_cols = 0;
            num_distinct_cols = 0;
            pertrans.aggsortrequired = false;
        } else if aggref.aggpresorted && aggref.aggdistinct.is_none() {
            sortlist = None;
            num_sort_cols = 0;
            num_distinct_cols = 0;
            pertrans.aggsortrequired = false;
        } else if aggref.aggdistinct.is_some() {
            let dl = aggref.aggdistinct.as_deref();
            sortlist = dl;
            num_sort_cols = dl.map_or(0, |s| s.len()) as i32;
            num_distinct_cols = num_sort_cols;
            // Assert(numSortCols >= list_length(aggref->aggorder));
            debug_assert!(num_sort_cols >= list_len(&aggref.aggorder) as i32);
            pertrans.aggsortrequired = !aggref.aggpresorted;
        } else {
            let ol = aggref.aggorder.as_deref();
            sortlist = ol;
            num_sort_cols = ol.map_or(0, |s| s.len()) as i32;
            num_distinct_cols = 0;
            pertrans.aggsortrequired = num_sort_cols > 0;
        }

        // pertrans->numSortCols = numSortCols;
        // pertrans->numDistinctCols = numDistinctCols;
        pertrans.num_sort_cols = num_sort_cols;
        pertrans.num_distinct_cols = num_distinct_cols;

        // If we have either sorting or filtering to do, create a tupledesc and
        // slot corresponding to the aggregated inputs (including sort
        // expressions) of the agg.
        //   if (numSortCols > 0 || aggref->aggfilter) {
        //       pertrans->sortdesc = ExecTypeFromTL(aggref->args);
        //       pertrans->sortslot = ExecInitExtraTupleSlot(estate, pertrans->sortdesc,
        //                                                   &TTSOpsMinimalTuple);
        //   }
        if num_sort_cols > 0 || aggref.aggfilter.is_some() {
            let sortdesc = exec_type_from_tl_owned(aggref)?;
            let sortslot = exec_init_extra_tuple_slot_minimal(estate, &sortdesc)?;
            pertrans.sortdesc = Some(sortdesc);
            pertrans.sortslot = Some(sortslot);
        }

        if num_sort_cols > 0 {
            // We don't implement DISTINCT or ORDER BY aggs in the HASHED case (yet)
            debug_assert!(
                aggstate.aggstrategy != AGG_HASHED && aggstate.aggstrategy != AGG_MIXED
            );

            // ORDER BY aggregates are not supported with partial aggregation
            debug_assert!(!types_nodes::nodeagg::do_aggsplit_combine(aggstate.aggsplit));

            // If we have only one input, we need its len/byval info.
            //   if (numInputs == 1)
            //       get_typlenbyval(inputTypes[numDirectArgs], &inputtypeLen, &inputtypeByVal);
            //   else if (numDistinctCols > 0)
            //       pertrans->uniqslot = ExecInitExtraTupleSlot(estate, sortdesc, MinimalTuple);
            if num_inputs == 1 {
                let (in_len, in_byval) =
                    get_typlenbyval_owned(input_types[num_direct_args as usize])?;
                pertrans.inputtype_len = in_len;
                pertrans.inputtype_by_val = in_byval;
            } else if num_distinct_cols > 0 {
                // we will need an extra slot to store prior values
                let sortdesc = pertrans
                    .sortdesc
                    .as_ref()
                    .expect("build_pertrans_for_aggref: sortdesc built above");
                let uniqslot = exec_init_extra_tuple_slot_minimal(estate, sortdesc)?;
                pertrans.uniqslot = Some(uniqslot);
            }

            // Extract the sort information for use later
            //   pertrans->sortColIdx     = palloc(numSortCols * sizeof(AttrNumber));
            //   pertrans->sortOperators  = palloc(numSortCols * sizeof(Oid));
            //   pertrans->sortCollations = palloc(numSortCols * sizeof(Oid));
            //   pertrans->sortNullsFirst = palloc(numSortCols * sizeof(bool));
            let mut sort_col_idx =
                vec_with_capacity_in(mcx, num_sort_cols as usize)?;
            let mut sort_operators =
                vec_with_capacity_in(mcx, num_sort_cols as usize)?;
            let mut sort_collations =
                vec_with_capacity_in(mcx, num_sort_cols as usize)?;
            let mut sort_nulls_first =
                vec_with_capacity_in(mcx, num_sort_cols as usize)?;

            // i = 0;
            // foreach(lc, sortlist) {
            //     SortGroupClause *sortcl = lfirst(lc);
            //     TargetEntry *tle = get_sortgroupclause_tle(sortcl, aggref->args);
            //     Assert(OidIsValid(sortcl->sortop));
            //     pertrans->sortColIdx[i]     = tle->resno;
            //     pertrans->sortOperators[i]  = sortcl->sortop;
            //     pertrans->sortCollations[i] = exprCollation((Node *) tle->expr);
            //     pertrans->sortNullsFirst[i] = sortcl->nulls_first;
            //     i++;
            // }
            // Assert(i == numSortCols);
            let mut i = 0i32;
            for sortcl in sortlist.unwrap_or(&[]).iter() {
                // TargetEntry *tle = get_sortgroupclause_tle(sortcl, aggref->args);
                // pertrans->sortColIdx[i]     = tle->resno;
                // pertrans->sortCollations[i] = exprCollation((Node *) tle->expr);
                //
                // tle->resno and exprCollation read fields of the planner-owned
                // TargetEntry / expression node that the trimmed shared
                // vocabulary does not yet carry; resolve both through the
                // nodeFuncs owner.
                let (resno, collation) = sortgroupclause_tle_resno_and_collation(sortcl, aggref)?;
                // the parser should have made sure of this
                debug_assert!(OidIsValid(sortcl.sortop));
                sort_col_idx.push(resno);
                sort_operators.push(sortcl.sortop);
                sort_collations.push(collation);
                sort_nulls_first.push(sortcl.nulls_first);
                i += 1;
            }
            debug_assert!(i == num_sort_cols);

            pertrans.sort_col_idx = Some(sort_col_idx);
            pertrans.sort_operators = Some(sort_operators);
            pertrans.sort_collations = Some(sort_collations);
            pertrans.sort_nulls_first = Some(sort_nulls_first);
        }

        if aggref.aggdistinct.is_some() {
            // Assert(numArguments > 0);
            // Assert(list_length(aggref->aggdistinct) == numDistinctCols);
            debug_assert!(num_arguments > 0);
            debug_assert!(list_len(&aggref.aggdistinct) as i32 == num_distinct_cols);

            // ops = palloc(numDistinctCols * sizeof(Oid));
            // i = 0; foreach(lc, aggref->aggdistinct) ops[i++] = ((SortGroupClause *) lfirst(lc))->eqop;
            let mut ops: PgVec<'mcx, Oid> =
                vec_with_capacity_in(mcx, num_distinct_cols as usize)?;
            for sortcl in aggref.aggdistinct.as_deref().unwrap_or(&[]).iter() {
                ops.push(sortcl.eqop);
            }

            // lookup / build the necessary comparators
            //   if (numDistinctCols == 1)
            //       fmgr_info(get_opcode(ops[0]), &pertrans->equalfnOne);
            //   else
            //       pertrans->equalfnMulti = execTuplesMatchPrepare(sortdesc, numDistinctCols,
            //                                   sortColIdx, ops, sortCollations, &aggstate->ss.ps);
            if num_distinct_cols == 1 {
                fmgr_info_get_opcode_into_equalfn_one(pertrans, ops[0])?;
            } else {
                exec_tuples_match_prepare_owned(pertrans, aggstate, &ops, num_distinct_cols)?;
            }
            // pfree(ops);  (PgVec drops with the context)
            let _ = ops;
        }

        // pertrans->sortstates = palloc0(sizeof(Tuplesortstate *) * numGroupingSets);
        let mut sortstates = vec_with_capacity_in(mcx, num_grouping_sets as usize)?;
        for _ in 0..num_grouping_sets {
            sortstates.push(None);
        }
        pertrans.sortstates = Some(sortstates);

        Ok(())
    }
}

/// `AGGKIND_IS_ORDERED_SET(kind)` (catalog/pg_aggregate.h): true unless the
/// aggregate is a normal (`'n'`) aggregate.
fn aggkind_is_ordered_set_lc(aggkind: i8) -> bool {
    aggkind != b'n' as i8
}

/// `list_length(list)` — element count of an optional `PgVec`-backed List.
fn list_len<T>(list: &Option<PgVec<'_, T>>) -> usize {
    list.as_ref().map_or(0, |l| l.len())
}

/// Deep-copy an `Aggref` into `mcx` for `pertrans->aggref` (C aliases the
/// planner-owned node directly; the owned model stores a copy).
fn clone_aggref_into<'mcx>(
    mcx: Mcx<'mcx>,
    aggref: &Aggref<'mcx>,
) -> PgResult<PgBox<'mcx, Aggref<'mcx>>> {
    // The Aggref carries only POD/Oid scalars plus expression-node Lists owned
    // by the unported nodes vocabulary; storing the back-reference faithfully
    // needs that owner's copyObject. Until it lands the copy panics loudly
    // rather than fabricate a partial node.
    let _ = (mcx, aggref);
    panic!(
        "backend-nodes-copyfuncs: pertrans->aggref back-reference needs copyObject over the \
         Aggref expression-node lists (unported nodes vocabulary)"
    )
}

/// `get_typlenbyval(typid, &typlen, &typbyval)` (lsyscache.c) — typcache read.
/// Owned by `backend-utils-cache-lsyscache`; read through its installed seam.
fn get_typlenbyval_owned(typid: Oid) -> PgResult<(i16, bool)> {
    backend_utils_cache_lsyscache_seams::get_typlenbyval::call(typid)
}

/// `build_aggregate_serialfn_expr` + `fmgr_info`/`fmgr_info_set_expr` +
/// `InitFunctionCallInfoData` for the 1-arg serialfn call frame.
fn build_serialfn_call_frame_owned<'mcx>(
    pertrans: &mut AggStatePerTransData<'mcx>,
    aggstate: &mut AggStateData<'mcx>,
    aggserialfn: Oid,
) -> PgResult<()> {
    let _ = (pertrans, aggstate, aggserialfn);
    panic!(
        "backend-parser-parse-agg / backend-utils-fmgr: build_aggregate_serialfn_expr + \
         fmgr_info + InitFunctionCallInfoData (serialfn call frame) not exposed by this unit's \
         seams yet"
    )
}

/// `build_aggregate_deserialfn_expr` + `fmgr_info`/`fmgr_info_set_expr` +
/// `InitFunctionCallInfoData` for the 2-arg deserialfn call frame.
fn build_deserialfn_call_frame_owned<'mcx>(
    pertrans: &mut AggStatePerTransData<'mcx>,
    aggstate: &mut AggStateData<'mcx>,
    aggdeserialfn: Oid,
) -> PgResult<()> {
    let _ = (pertrans, aggstate, aggdeserialfn);
    panic!(
        "backend-parser-parse-agg / backend-utils-fmgr: build_aggregate_deserialfn_expr + \
         fmgr_info + InitFunctionCallInfoData (deserialfn call frame) not exposed by this \
         unit's seams yet"
    )
}

/// `ExecTypeFromTL(aggref->args)` (execTuples.c) — build the sort tupledesc.
fn exec_type_from_tl_owned<'mcx>(
    aggref: &Aggref<'mcx>,
) -> PgResult<PgBox<'mcx, TupleDescData<'mcx>>> {
    let _ = aggref;
    panic!(
        "backend-executor-execTuples: ExecTypeFromTL over aggref->args (expression-node \
         TargetEntry list) not exposed by this unit's seams yet"
    )
}

/// `ExecInitExtraTupleSlot(estate, desc, &TTSOpsMinimalTuple)` — owned by
/// execTuples; the unit already declares `exec_init_extra_tuple_slot`.
fn exec_init_extra_tuple_slot_minimal<'mcx>(
    estate: &mut EStateData<'mcx>,
    desc: &PgBox<'mcx, TupleDescData<'mcx>>,
) -> PgResult<SlotId> {
    let _ = (estate, desc);
    panic!(
        "backend-executor-execTuples: ExecInitExtraTupleSlot(TTSOpsMinimalTuple) reached \
         through the execTuples seam once build_pertrans_for_aggref's transfn frame lands"
    )
}

/// `tle = get_sortgroupclause_tle(sortcl, aggref->args)` then `tle->resno` and
/// `exprCollation((Node *) tle->expr)` (nodes/nodeFuncs.c). Both reads need the
/// planner-owned TargetEntry/expression-node vocabulary (the trimmed shared
/// `TargetEntry` carries neither `resno` nor the typed expr collation), so the
/// resolution belongs to the unported nodeFuncs owner.
fn sortgroupclause_tle_resno_and_collation<'mcx>(
    sortcl: &types_nodes::nodeagg::SortGroupClauseAgg,
    aggref: &Aggref<'mcx>,
) -> PgResult<(types_core::primitive::AttrNumber, Oid)> {
    let _ = (sortcl, aggref);
    panic!(
        "backend-nodes-nodeFuncs: get_sortgroupclause_tle + tle->resno + exprCollation over \
         aggref->args (expression-node TargetEntry list) not exposed by this unit's seams yet"
    )
}

/// `fmgr_info(get_opcode(eqop), &pertrans->equalfnOne)` — single-column
/// distinct comparator (lsyscache get_opcode + fmgr_info).
fn fmgr_info_get_opcode_into_equalfn_one<'mcx>(
    pertrans: &mut AggStatePerTransData<'mcx>,
    eqop: Oid,
) -> PgResult<()> {
    let _ = (pertrans, eqop);
    panic!(
        "backend-utils-cache-lsyscache / backend-utils-fmgr: get_opcode + fmgr_info for the \
         single-column DISTINCT comparator not exposed by this unit's seams yet"
    )
}

/// `execTuplesMatchPrepare(...)` (execGrouping.c) — multi-column distinct
/// comparator ExprState.
fn exec_tuples_match_prepare_owned<'mcx>(
    pertrans: &mut AggStatePerTransData<'mcx>,
    aggstate: &mut AggStateData<'mcx>,
    ops: &PgVec<'mcx, Oid>,
    num_distinct_cols: i32,
) -> PgResult<()> {
    let _ = (pertrans, aggstate, ops, num_distinct_cols);
    panic!(
        "backend-executor-execGrouping: execTuplesMatchPrepare (multi-column DISTINCT \
         comparator) not exposed by this unit's seams yet"
    )
}

/// `GetAggInitVal(textInitVal, transtype)` — convert the `agginitval` text
/// Datum into the transition type's internal Datum via its input function.
pub fn GetAggInitVal<'mcx>(text_init_val: Datum<'mcx>, transtype: Oid) -> PgResult<Datum<'mcx>> {
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
        rescan_expr_context_aggcontext(node, setno, estate)?;
    }
    if node.hashcontext.is_some() {
        rescan_expr_context_hashcontext(node, estate)?;
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
fn rescan_expr_context_aggcontext<'mcx>(
    node: &mut AggStateData<'mcx>,
    setno: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // `aggcontexts[setno]` is an EcxtId into the EState pool; ReScanExprContext
    // fires its shutdown callbacks and resets its per-tuple memory.
    if let Some(ecxt) = node
        .aggcontexts
        .as_ref()
        .and_then(|a| a.get(setno).copied())
    {
        backend_executor_execUtils_seams::re_scan_expr_context::call(estate, ecxt)?;
    }
    Ok(())
}

/// `ReScanExprContext(aggstate->hashcontext)`.
fn rescan_expr_context_hashcontext<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(ecxt) = node.hashcontext {
        backend_executor_execUtils_seams::re_scan_expr_context::call(estate, ecxt)?;
    }
    Ok(())
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
            let ht = perhash[0]
                .hashtable
                .as_mut()
                .expect("hashtable built (table_filled)");
            let iter = backend_executor_execGrouping_seams::init_tuple_hash_iterator::call(
                &mut **ht,
            );
            perhash[0].hashiter = iter;
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
        rescan_expr_context_aggcontext(node, setno, estate)?;
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

        rescan_expr_context_hashcontext(node, estate)?;
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
    backend_executor_execTuples_seams::exec_clear_tuple::call(estate, slot_id)
}

/// `MemSet(econtext->ecxt_aggvalues/ecxt_aggnulls, 0, ...)` — forget the
/// current aggregate values held in the node's per-output-tuple ExprContext.
/// The aggvalues/aggnulls arrays live on the EState-owned ExprContext the node
/// projects through; clearing them is owned by execUtils alongside the
/// EcxtId-addressed context pool. The owned AggState model does not carry the
/// EcxtId for ps_ExprContext on its facet here, so the clear lands with the
/// execUtils ExprContext model.
fn clear_agg_values<'mcx>(_node: &mut AggStateData<'mcx>) {
    // C: `MemSet(econtext->ecxt_aggvalues, 0, sizeof(Datum) * node->numaggs);
    //     MemSet(econtext->ecxt_aggnulls, 0, sizeof(bool) * node->numaggs);`
    // The aggvalues/aggnulls arrays live on the node's per-output-tuple
    // ExprContext (`ss.ps.ps_ExprContext`), which the owned AggState carries as
    // an EcxtId into the EState pool — but that EcxtId is NOT on this facet (the
    // same ExprContext storage-model carrier gap ExecInitAgg/hash_create_memory
    // hit), and execUtils exposes no "clear ecxt_aggvalues/aggnulls by EcxtId"
    // seam to marshal the MemSet through. A silent no-op would drop the C clear;
    // per "loud panic beats a silent stub", panic until the per-tuple ExprContext
    // reaches this facet (and a clear seam exists).
    panic!(
        "backend-executor-nodeAgg::ExecReScanAgg: clearing ecxt_aggvalues/ecxt_aggnulls needs the \
         node's per-tuple ExprContext (EcxtId) on the AggState facet — same ExprContext carrier \
         gap as ExecInitAgg; no execUtils clear-agg-values seam yet"
    );
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
