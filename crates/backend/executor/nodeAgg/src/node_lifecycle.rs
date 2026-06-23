//! Node-lifecycle family: init / end / rescan, the `ExecAgg` driver, and its
//! setup helpers (phase and grouping-set selection, input fetch, the column
//! analysis that decides which outer-plan columns are needed, and the
//! per-trans build that reads the catalog for each aggregate).

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use ::types_core::primitive::{Oid, OidIsValid, INVALID_OID};
use ::types_tuple::heaptuple::Datum;
use ::types_error::PgResult;
use ::nodes::nodeagg::{
    Aggref, AGG_HASHED, AGG_MIXED, AGG_PLAIN, AGG_SORTED,
};
use crate::aggstate::{AggStateData, AggStatePerTransData};
use ::nodes::nodes::Node;
use nodes::{Bitmapset, EStateData, SlotId};
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::types_core::fmgr::FmgrInfo;
use ::types_tuple::heaptuple::TupleDescData;

extern crate alloc;

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
        tuplesort_seams::tuplesort_end::call(sort_in)?;
    }

    if newphase <= 1 {
        // Discard any existing output tuplesort.
        if let Some(sort_out) = aggstate.sort_out.take() {
            tuplesort_seams::tuplesort_end::call(sort_out)?;
        }
    } else {
        // The old output tuplesort becomes the new input one, and this is the
        // right time to actually sort it.
        aggstate.sort_in = aggstate.sort_out.take();
        debug_assert!(aggstate.sort_in.is_some());
        let sort_in = aggstate.sort_in.as_mut().expect("sort_in set above");
        tuplesort_seams::tuplesort_performsort::call(sort_in)?;
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
            execTuples_seams::exec_get_result_type::call(outer.ps_head())
                .expect("outer plan result type set at init");

        let work_mem = work_mem();
        let sort_out = tuplesort_seams::tuplesort_begin_heap::call(
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
        postgres_seams::check_for_interrupts::call()?;
        let sort_slot = aggstate.sort_slot.expect("sort_slot set when sorting");
        let found = tuplesort_seams::tuplesort_gettupleslot::call(
            sort_in,
            true,
            false,
            estate.slot_data_mut(sort_slot),
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
        slot = execProcnode_seams::exec_proc_node::call(outer, estate)?;
    }

    // if (!TupIsNull(slot) && aggstate->sort_out)
    //     tuplesort_puttupleslot(aggstate->sort_out, slot);
    //
    // C's tuplesort_puttupleslot does ExecCopySlotMinimalTuple(slot) =
    // slot->tts_ops->copy_minimal_tuple(slot). For lazily-deformed physical
    // slots (heap/buffer/minimal — e.g. a SeqScan that only touched the
    // grouping cols, tts_nvalid < natts) the per-kind copy materializes from
    // the stored tuple. The owned tuplesort instead forms the minimal tuple
    // from slot->tts_values/tts_isnull, so a physical source slot must be fully
    // deformed first — otherwise the unread columns (the aggregated input vars)
    // would feed stale values, silently corrupting the re-sorted phase's
    // aggregates. A VIRTUAL slot, however, is always fully materialized
    // (Assert(!TTS_EMPTY) only; tts_virtual_copy_minimal_tuple reads
    // tts_values/tts_isnull directly) and slot_getsomeattrs on it is an error
    // (tts_virtual_getsomeattrs elogs) — so skip the deform for virtual slots,
    // mirroring C's per-kind copy_minimal_tuple dispatch.
    if let Some(s) = slot {
        if aggstate.sort_out.is_some()
            && estate.slot_data_mut(s).kind() != ::nodes::TupleSlotKind::Virtual
        {
            execTuples_seams::slot_getallattrs_by_id::call(estate, s)?;
        }
    }
    if let (Some(s), Some(sort_out)) = (slot, aggstate.sort_out.as_mut()) {
        tuplesort_seams::tuplesort_puttupleslot::call(
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
    let agg = agg_plan(aggstate)?;

    let mut context = FindColsContext {
        is_aggref: false,
        aggregated: None,
        unaggregated: None,
    };

    // Examine tlist and quals.
    //   (void) find_cols_walker((Node *) agg->plan.targetlist, &context);
    //   (void) find_cols_walker((Node *) agg->plan.qual, &context);
    //
    // The C passes the `List *` targetlist/qual to the walker, which iterates
    // and walks each element. In the owned model the targetlist is a
    // `Vec<TargetEntry>` (each entry's expr is the walked child) and the qual a
    // `Vec<Expr>`; iterate and walk each as a wrapped `Node`. The wrappers are
    // built in a scratch context that is freed when the walk returns (the
    // walker only read-borrows them).
    let scratch = ::mcx::MemoryContext::new("find_cols scratch");
    let swx = scratch.mcx();
    if let Some(tlist) = agg.plan.targetlist.as_ref() {
        for te in tlist.iter() {
            if let Some(expr) = te.expr.as_deref() {
                let node = Node::mk_expr(swx, expr.clone_in(swx)?)?;
                find_cols_walker(Some(&node), &mut context, mcx)?;
            }
        }
    }
    if let Some(qual) = agg.plan.qual.as_ref() {
        for expr in qual.iter() {
            let node = Node::mk_expr(swx, expr.clone_in(swx)?)?;
            find_cols_walker(Some(&node), &mut context, mcx)?;
        }
    }

    // In some cases, grouping columns will not appear in the tlist.
    //   for (int i = 0; i < agg->numCols; i++)
    //       context.unaggregated = bms_add_member(context.unaggregated,
    //                                             agg->grpColIdx[i]);
    let num_cols = agg.num_cols;
    if num_cols > 0 {
        let grp = agg
            .grp_col_idx
            .as_ref()
            .expect("find_cols: grpColIdx with numCols > 0");
        for i in 0..num_cols as usize {
            let attno = grp[i] as i32;
            context.unaggregated = Some(nodes_core_seams::bms_add_member::call(
                mcx,
                context.unaggregated.take(),
                attno,
            )?);
        }
    }

    Ok((context.aggregated, context.unaggregated))
}

/// `(Agg *) aggstate->ss.ps.plan` — read the `Agg` plan node off the shared,
/// read-only plan tree the node-state aliases.
fn agg_plan<'a, 'mcx>(
    aggstate: &'a AggStateData<'mcx>,
) -> PgResult<&'a ::nodes::nodeagg::Agg<'mcx>> {
    let plan = aggstate
        .ss
        .ps
        .plan
        .expect("find_cols: ss.ps.plan is NULL");
    match plan.node_tag() {
        ::nodes::nodes::ntag::T_Agg => Ok(plan.expect_agg()),
        other => panic!("castNode(Agg, ss.ps.plan) failed: {other:?}"),
    }
}

/// `find_cols_walker(node, context)` — expression walker collecting referenced
/// `Var` colnos into the context, marking aggregated vs unaggregated. Returns
/// `false` (the C walker never aborts: it collects over the whole tree).
pub fn find_cols_walker<'mcx, 'n>(
    node: Option<&Node<'n>>,
    context: &mut FindColsContext<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<bool> {
    // if (node == NULL) return false;
    let Some(node) = node else {
        return Ok(false);
    };

    // if (IsA(node, Var)) { ... }
    if let Some(var) = node.as_var() {
        // setrefs.c should have set the varno to OUTER_VAR; varlevelsup == 0.
        debug_assert_eq!(var.varlevelsup, 0);
        let attno = var.varattno as i32;
        if context.is_aggref {
            context.aggregated = Some(nodes_core_seams::bms_add_member::call(
                mcx,
                context.aggregated.take(),
                attno,
            )?);
        } else {
            context.unaggregated = Some(nodes_core_seams::bms_add_member::call(
                mcx,
                context.unaggregated.take(),
                attno,
            )?);
        }
        return Ok(false);
    }

    // if (IsA(node, Aggref)) { is_aggref = true; walk; is_aggref = false; }
    if matches!(node.as_expr(), Some(::nodes::primnodes::Expr::Aggref(_))) {
        debug_assert!(!context.is_aggref);
        context.is_aggref = true;
        walk_children(node, context, mcx)?;
        context.is_aggref = false;
        return Ok(false);
    }

    // return expression_tree_walker(node, find_cols_walker, context);
    walk_children(node, context, mcx)?;
    Ok(false)
}

/// Drive `expression_tree_walker` over `node`'s children with `find_cols_walker`
/// as the per-child callback. The seam'd walker may surface an allocation
/// failure raised inside the callback; capture it and re-raise.
fn walk_children<'mcx, 'n>(
    node: &Node<'n>,
    context: &mut FindColsContext<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let mut err: Option<::types_error::PgError> = None;
    nodes_core_seams::expression_tree_walker::call(node, &mut |child| {
        if err.is_some() {
            return true;
        }
        match find_cols_walker(Some(child), context, mcx) {
            Ok(abort) => abort,
            Err(e) => {
                err = Some(e);
                true
            }
        }
    });
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// `find_hash_columns(aggstate)` — set up the per-hash column descriptors
/// (input/hash slot column indices) for every grouping set.
pub fn find_hash_columns<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    use ::types_core::primitive::AttrNumber;

    // scanDesc = aggstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
    let scan_desc = crate::exec_init_agg::scan_tuple_desc(aggstate, estate)?;
    let scan_natts = scan_desc
        .as_deref()
        .map(|d| d.natts)
        .expect("find_hash_columns: scanDesc is NULL");

    // List *outerTlist = outerPlanState(aggstate)->plan->targetlist;
    // Cloned into mcx so the hashTlist (list_nth of it) outlives this borrow.
    let outer_tlist: PgVec<'mcx, ::nodes::primnodes::TargetEntry<'mcx>> = {
        let outer = aggstate
            .ss
            .ps
            .lefttree
            .as_deref()
            .expect("find_hash_columns: outerPlanState is NULL");
        let outer_plan = outer
            .ps_head()
            .plan
            .expect("find_hash_columns: outer plan is NULL");
        match outer_plan.plan_head().targetlist.as_ref() {
            Some(tl) => {
                let mut v = vec_with_capacity_in(mcx, tl.len())?;
                for te in tl.iter() {
                    v.push(te.clone_in(mcx)?);
                }
                v
            }
            None => PgVec::new_in(mcx),
        }
    };

    let num_hashes = aggstate.num_hashes;

    // find_cols(aggstate, &aggregated_colnos, &base_colnos);
    let (aggregated_colnos, base_colnos) = find_cols(aggstate, mcx)?;

    // aggstate->colnos_needed = bms_union(base_colnos, aggregated_colnos);
    aggstate.colnos_needed = nodes_core_seams::bms_union::call(
        mcx,
        base_colnos.as_deref(),
        aggregated_colnos.as_deref(),
    )?;
    aggstate.max_colno_needed = 0;
    aggstate.all_cols_needed = true;

    for i in 0..scan_natts {
        let colno = i + 1;
        if nodes_core_seams::bms_is_member::call(colno, aggstate.colnos_needed.as_deref()) {
            aggstate.max_colno_needed = colno;
        } else {
            aggstate.all_cols_needed = false;
        }
    }

    // Snapshot all_grouped_cols (read for every grouping set below).
    let all_grouped_cols: Option<PgVec<'mcx, i32>> = match aggstate.all_grouped_cols.as_ref() {
        Some(v) => {
            let mut out = vec_with_capacity_in(mcx, v.len())?;
            for &c in v.iter() {
                out.push(c);
            }
            Some(out)
        }
        None => None,
    };

    for j in 0..num_hashes as usize {
        // Bitmapset *colnos = bms_copy(base_colnos);
        let mut colnos = nodes_core_seams::bms_copy::call(mcx, base_colnos.as_deref())?;

        // AttrNumber *grpColIdx = perhash->aggnode->grpColIdx;
        let perhash_num_cols = aggstate.perhash.as_ref().expect("perhash")[j].num_cols;
        let grp_col_idx: PgVec<'mcx, AttrNumber> = {
            let aggnode = aggstate.perhash.as_ref().expect("perhash")[j]
                .aggnode
                .as_ref()
                .expect("perhash->aggnode");
            let src = aggnode
                .grp_col_idx
                .as_ref()
                .expect("perhash->aggnode->grpColIdx");
            let mut v = vec_with_capacity_in(mcx, src.len())?;
            for &c in src.iter() {
                v.push(c);
            }
            v
        };

        // perhash->largestGrpColIdx = 0;
        aggstate.perhash.as_mut().expect("perhash")[j].largest_grp_col_idx = 0;

        // Grouping-sets pruning via prepare_projection_slot's logic: drop Vars
        // referenced in tlist/qual only for other grouping sets.
        //   if (aggstate->phases[0].grouped_cols) { ... }
        let has_grouped_cols = aggstate
            .phases
            .as_ref()
            .and_then(|p| p.first())
            .map(|ph| ph.grouped_cols.is_some())
            .unwrap_or(false);
        if has_grouped_cols {
            if let Some(all_gc) = all_grouped_cols.as_ref() {
                for &attnum in all_gc.iter() {
                    let is_member = {
                        let grouped_cols = aggstate.phases.as_ref().expect("phases")[0]
                            .grouped_cols
                            .as_ref()
                            .expect("grouped_cols")
                            .get(j)
                            .map(|b| &**b);
                        nodes_core_seams::bms_is_member::call(attnum, grouped_cols)
                    };
                    if !is_member {
                        colnos = nodes_core_seams::bms_del_member::call(
                            colnos.take(),
                            attnum,
                        );
                    }
                }
            }
        }

        // maxCols = bms_num_members(colnos) + perhash->numCols;
        let max_cols = nodes_core_seams::bms_num_members::call(colnos.as_deref())
            + perhash_num_cols;

        // perhash->hashGrpColIdxInput = palloc(maxCols * sizeof(AttrNumber));
        // perhash->hashGrpColIdxHash  = palloc(perhash->numCols * sizeof(AttrNumber));
        let mut hash_grp_col_idx_input: PgVec<'mcx, AttrNumber> =
            vec_with_capacity_in(mcx, max_cols.max(0) as usize)?;
        let mut hash_grp_col_idx_hash: PgVec<'mcx, AttrNumber> =
            vec_with_capacity_in(mcx, perhash_num_cols.max(0) as usize)?;
        // Size the input/hash vectors to their max so positional writes land.
        for _ in 0..max_cols.max(0) {
            hash_grp_col_idx_input.push(0);
        }
        for _ in 0..perhash_num_cols.max(0) {
            hash_grp_col_idx_hash.push(0);
        }

        // Add all the grouping columns to colnos.
        for i in 0..perhash_num_cols as usize {
            colnos = Some(nodes_core_seams::bms_add_member::call(
                mcx,
                colnos.take(),
                grp_col_idx[i] as i32,
            )?);
        }

        // First build mapping for columns directly hashed.
        let mut numhash_grp_cols: i32 = 0;
        for i in 0..perhash_num_cols as usize {
            hash_grp_col_idx_input[i] = grp_col_idx[i];
            hash_grp_col_idx_hash[i] = (i + 1) as AttrNumber;
            numhash_grp_cols += 1;
            // delete already mapped columns
            colnos =
                nodes_core_seams::bms_del_member::call(colnos.take(), grp_col_idx[i] as i32);
        }

        // and add the remaining columns
        let mut i: i32 = -1;
        loop {
            i = nodes_core_seams::bms_next_member::call(colnos.as_deref(), i);
            if i < 0 {
                break;
            }
            hash_grp_col_idx_input[numhash_grp_cols as usize] = i as AttrNumber;
            numhash_grp_cols += 1;
        }

        // and build a tuple descriptor for the hashtable
        let mut hash_tlist: PgVec<'mcx, ::nodes::primnodes::TargetEntry<'mcx>> =
            vec_with_capacity_in(mcx, numhash_grp_cols.max(0) as usize)?;
        let mut largest: i32 = 0;
        for i in 0..numhash_grp_cols as usize {
            let var_number = hash_grp_col_idx_input[i] as i32 - 1;
            // hashTlist = lappend(hashTlist, list_nth(outerTlist, varNumber));
            let te = outer_tlist
                .get(var_number as usize)
                .expect("find_hash_columns: list_nth(outerTlist, varNumber) out of range");
            hash_tlist.push(te.clone_in(mcx)?);
            if var_number + 1 > largest {
                largest = var_number + 1;
            }
        }

        // hashDesc = ExecTypeFromTL(hashTlist);
        let hash_desc = execTuples_seams::exec_type_from_tl::call(
            mcx,
            hash_tlist.as_slice(),
        )?;

        // execTuplesHashPrepare(perhash->numCols, perhash->aggnode->grpOperators,
        //                       &perhash->eqfuncoids, &perhash->hashfunctions);
        let grp_operators: PgVec<'mcx, Oid> = {
            let aggnode = aggstate.perhash.as_ref().expect("perhash")[j]
                .aggnode
                .as_ref()
                .expect("perhash->aggnode");
            let src = aggnode
                .grp_operators
                .as_ref()
                .expect("perhash->aggnode->grpOperators");
            let mut v = vec_with_capacity_in(mcx, src.len())?;
            for &o in src.iter() {
                v.push(o);
            }
            v
        };
        let (eqfuncoids, hashfunctions) =
            execGrouping_seams::exec_tuples_hash_prepare::call(
                mcx,
                perhash_num_cols,
                grp_operators.as_slice(),
            )?;

        // perhash->hashslot = ExecAllocTableSlot(&estate->es_tupleTable, hashDesc,
        //                                        &TTSOpsMinimalTuple);
        let hashslot = execTuples_seams::exec_alloc_table_slot::call(
            estate,
            hash_desc,
            ::nodes::TupleSlotKind::MinimalTuple,
        )?;

        // Commit all per-hash outputs.
        let perhash = &mut aggstate.perhash.as_mut().expect("perhash")[j];
        perhash.largest_grp_col_idx = largest;
        perhash.numhash_grp_cols = numhash_grp_cols;
        perhash.hash_grp_col_idx_input = Some(hash_grp_col_idx_input);
        perhash.hash_grp_col_idx_hash = Some(hash_grp_col_idx_hash);
        perhash.eqfuncoids = Some(eqfuncoids);
        perhash.hashfunctions = Some(hashfunctions);
        perhash.hashslot = Some(hashslot);

        // list_free(hashTlist); bms_free(colnos); — owned values drop here.
    }

    // bms_free(base_colnos); — owned value drops here.
    Ok(())
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
    // build_aggregate_transfn_expr (parse_agg.c) + fmgr_info + fmgr_info_set_expr
    // (fmgr.c) + InitFunctionCallInfoData for the transfn call frame.
    let (transfnexpr, _invtransfnexpr) =
        parse_agg_seams::build_aggregate_transfn_expr::call(
            input_types,
            num_arguments,
            num_direct_args as i32,
            aggref.aggvariadic,
            aggtranstype,
            aggref.inputcollid,
            transfn_oid,
            INVALID_OID,
            false,
        )?;
    let mut transfn = fmgr_seams::fmgr_info::call(mcx, transfn_oid)?;
    fmgr_seams::fmgr_info_set_expr::call(mcx, &mut transfn, &transfnexpr)?;
    pertrans.transfn = transfn.clone();
    // pertrans->transfn_fcinfo = palloc(SizeForFunctionCallInfo(numTransArgs));
    // InitFunctionCallInfoData(*fcinfo, &pertrans->transfn, numTransArgs,
    //                          pertrans->aggCollation, (Node *) aggstate, NULL);
    //
    // K1 (#324/#335 agg call-frame channel): the C `(Node *) aggstate` context is
    // installed — capture the address-stable AggState (PgBox-allocated in
    // ExecInitAgg, the C `makeNode`-stable node) as a tag-checked
    // `AggStateContextLink` and store it as the frame's `FmgrCallContext::Agg`.
    // The aggregate support functions recover the calling `AggState` through it.
    let agg_link = agg_state_context_link(aggstate);
    let transfn_fcinfo =
        new_agg_fcinfo(mcx, transfn, num_trans_args, pertrans.agg_collation, agg_link)?;
    pertrans.transfn_fcinfo = Some(transfn_fcinfo);

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
            build_serialfn_call_frame_owned(pertrans, aggstate, aggserialfn, mcx)?;
        }

        // if (OidIsValid(aggdeserialfn)) { build_aggregate_deserialfn_expr; fmgr_info;
        //   fmgr_info_set_expr; deserialfn_fcinfo = palloc(SizeForFunctionCallInfo(2));
        //   InitFunctionCallInfoData(..., 2, InvalidOid, (Node *) aggstate, NULL); }
        if OidIsValid(aggdeserialfn) {
            build_deserialfn_call_frame_owned(pertrans, aggstate, aggdeserialfn, mcx)?;
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
        let sortlist: Option<&[::nodes::nodeagg::SortGroupClauseAgg]>;
        let num_sort_cols: i32;
        let num_distinct_cols: i32;
        if aggkind_is_ordered_set_lc(aggref.aggkind) {
            sortlist = None;
            num_sort_cols = 0;
            num_distinct_cols = 0;
            pertrans.aggsortrequired = false;
        } else if aggref.aggpresorted && !aggdistinct_is_present(aggref) {
            sortlist = None;
            num_sort_cols = 0;
            num_distinct_cols = 0;
            pertrans.aggsortrequired = false;
        } else if aggdistinct_is_present(aggref) {
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
            let sortdesc = exec_type_from_tl_owned(aggref, mcx)?;
            let sortslot = exec_init_extra_tuple_slot_minimal(estate, &sortdesc)?;
            // `exec_type_from_tl_owned` returns the canonical `TupleDesc`
            // (= Option<PgBox<..>>); store it directly.
            pertrans.sortdesc = sortdesc;
            pertrans.sortslot = Some(sortslot);
        }

        if num_sort_cols > 0 {
            // We don't implement DISTINCT or ORDER BY aggs in the HASHED case (yet)
            debug_assert!(
                aggstate.aggstrategy != AGG_HASHED && aggstate.aggstrategy != AGG_MIXED
            );

            // ORDER BY aggregates are not supported with partial aggregation
            debug_assert!(!::nodes::nodeagg::do_aggsplit_combine(aggstate.aggsplit));

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
                let desc = clone_tuple_desc(&pertrans.sortdesc, mcx)?;
                let uniqslot = exec_init_extra_tuple_slot_minimal(estate, &desc)?;
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

        if aggdistinct_is_present(aggref) {
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
                fmgr_info_get_opcode_into_equalfn_one(pertrans, ops[0], mcx)?;
            } else {
                exec_tuples_match_prepare_owned(pertrans, aggstate, &ops, num_distinct_cols, estate)?;
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

/// C's `if (aggref->aggdistinct)` truthiness: a `List *` is `NIL` (falsy) when
/// empty. The owned model carries `aggdistinct` as `Option<PgVec<..>>` and a
/// node converted from the planner wraps even the empty DISTINCT list as
/// `Some([])`, so `is_some()` is NOT the right test — a plain `count(*)` (no
/// DISTINCT) has an empty list. Present ⇔ `Some` and non-empty.
fn aggdistinct_is_present(aggref: &Aggref<'_>) -> bool {
    aggref
        .aggdistinct
        .as_deref()
        .is_some_and(|d| !d.is_empty())
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
    // pertrans->aggref = aggref; — the owned model stores a deep copy
    // (copyObject shape) of the executor-side Aggref.
    alloc_in(mcx, aggref.clone_in(mcx)?)
}

/// `get_typlenbyval(typid, &typlen, &typbyval)` (lsyscache.c) — typcache read.
/// Owned by `backend-utils-cache-lsyscache`; read through its installed seam.
fn get_typlenbyval_owned(typid: Oid) -> PgResult<(i16, bool)> {
    lsyscache_seams::get_typlenbyval::call(typid)
}

/// `palloc(SizeForFunctionCallInfo(nargs)); InitFunctionCallInfoData(*fcinfo,
/// flinfo, nargs, fncollation, (Node *) aggstate, NULL)` — allocate and
/// zero-init a transition/serial/deserial call frame carrying the resolved
/// `FmgrInfo`.
///
/// `(Node *) aggstate` — capture the address-stable live `AggState` as a
/// tag-checked back-link for an aggregate call frame's `fcinfo->context`. The
/// `AggStateData` is PgBox-allocated by `ExecInitAgg` (the C `makeNode`-stable
/// node), so its address is fixed for the AggState's lifetime; the link points
/// from the call frames (which the AggState transitively owns) back to it,
/// discharging the `PlanStateLink` parent-outlives-child invariant.
fn agg_state_context_link<'mcx>(
    aggstate: &AggStateData<'mcx>,
) -> ::nodes::aggstate_carrier::AggStateContextLink {
    ::nodes::aggstate_carrier::AggStateContextLink::from_ref(
        aggstate as &(dyn ::nodes::aggstate_carrier::AggStateLive<'mcx> + 'mcx),
    )
}

/// K1 (#324/#335): the C `(Node *) aggstate` context IS stored — `context` is a
/// [`FmgrCallContext::Agg`] carrying the tag-checked
/// [`AggStateContextLink`](::nodes::aggstate_carrier::AggStateContextLink)
/// back-reference to the live `AggState` (the `PlanStateLink` discipline). The
/// caller passes the link captured from the PgBox-allocated (address-stable, C
/// `makeNode`-equivalent) `AggStateData`, so the aggregate support functions
/// (`AggCheckCallContext` etc.) recover the calling `AggState` through the frame.
fn new_agg_fcinfo<'mcx>(
    mcx: Mcx<'mcx>,
    flinfo: FmgrInfo,
    nargs: i32,
    fncollation: Oid,
    context: ::nodes::aggstate_carrier::AggStateContextLink,
) -> PgResult<PgBox<'mcx, FunctionCallInfoBaseData<'mcx>>> {
    let mut args = vec_with_capacity_in_std(nargs as usize);
    for _ in 0..nargs {
        args.push(datum::NullableDatum::default());
    }
    let fcinfo = FunctionCallInfoBaseData {
        flinfo: Some(flinfo),
        // C: InitFunctionCallInfoData(..., (Node *) aggstate, NULL)
        context: Some(::nodes::fmgr::FmgrCallContext::Agg(context)),
        resultinfo: None,
        fncollation,
        isnull: false,
        nargs: nargs as i16,
        args,
        // Value-per-call SRF channel (#349): unused for an aggregate transition
        // frame (no in-flight FuncCallContext / per-query SRF context).
        ..Default::default()
    };
    alloc_in(mcx, fcinfo)
}

/// `Vec::with_capacity` for the std `Vec` the shared `FunctionCallInfoBaseData`
/// uses for `args` (not arena-allocated — the C frame's flexible array is a
/// plain palloc, and the shared vocab carries a std `Vec`).
fn vec_with_capacity_in_std<T>(cap: usize) -> alloc::vec::Vec<T> {
    alloc::vec::Vec::with_capacity(cap)
}

/// `build_aggregate_serialfn_expr` + `fmgr_info`/`fmgr_info_set_expr` +
/// `InitFunctionCallInfoData` for the 1-arg serialfn call frame.
fn build_serialfn_call_frame_owned<'mcx>(
    pertrans: &mut AggStatePerTransData<'mcx>,
    aggstate: &mut AggStateData<'mcx>,
    aggserialfn: Oid,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // build_aggregate_serialfn_expr(aggserialfn, &serialfnexpr);
    // fmgr_info(aggserialfn, &pertrans->serialfn);
    // fmgr_info_set_expr((Node *) serialfnexpr, &pertrans->serialfn);
    // serialfn_fcinfo = palloc(SizeForFunctionCallInfo(1));
    // InitFunctionCallInfoData(*serialfn_fcinfo, &pertrans->serialfn, 1,
    //                          InvalidOid, (Node *) aggstate, NULL);
    let serialfnexpr =
        parse_agg_seams::build_aggregate_serialfn_expr::call(aggserialfn)?;
    let mut serialfn = fmgr_seams::fmgr_info::call(mcx, aggserialfn)?;
    fmgr_seams::fmgr_info_set_expr::call(mcx, &mut serialfn, &serialfnexpr)?;
    pertrans.serialfn = serialfn.clone();
    // InitFunctionCallInfoData(..., 1, InvalidOid, (Node *) aggstate, NULL)
    let agg_link = agg_state_context_link(aggstate);
    pertrans.serialfn_fcinfo = Some(new_agg_fcinfo(mcx, serialfn, 1, INVALID_OID, agg_link)?);
    Ok(())
}

/// `build_aggregate_deserialfn_expr` + `fmgr_info`/`fmgr_info_set_expr` +
/// `InitFunctionCallInfoData` for the 2-arg deserialfn call frame.
fn build_deserialfn_call_frame_owned<'mcx>(
    pertrans: &mut AggStatePerTransData<'mcx>,
    aggstate: &mut AggStateData<'mcx>,
    aggdeserialfn: Oid,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let deserialfnexpr =
        parse_agg_seams::build_aggregate_deserialfn_expr::call(aggdeserialfn)?;
    let mut deserialfn = fmgr_seams::fmgr_info::call(mcx, aggdeserialfn)?;
    fmgr_seams::fmgr_info_set_expr::call(mcx, &mut deserialfn, &deserialfnexpr)?;
    pertrans.deserialfn = deserialfn.clone();
    // InitFunctionCallInfoData(..., 2, InvalidOid, (Node *) aggstate, NULL)
    let agg_link = agg_state_context_link(aggstate);
    pertrans.deserialfn_fcinfo = Some(new_agg_fcinfo(mcx, deserialfn, 2, INVALID_OID, agg_link)?);
    Ok(())
}

/// `ExecTypeFromTL(aggref->args)` (execTuples.c) — build the sort tupledesc.
fn exec_type_from_tl_owned<'mcx>(
    aggref: &Aggref<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<::types_tuple::heaptuple::TupleDesc<'mcx>> {
    // ExecTypeFromTL takes a &[TargetEntry]; materialize a contiguous copy of
    // the aggref's args (which are stored as PgBox<TargetEntry>).
    let mut tl: PgVec<'mcx, ::nodes::primnodes::TargetEntry<'mcx>> =
        vec_with_capacity_in(mcx, list_len(&aggref.args))?;
    if let Some(args) = aggref.args.as_ref() {
        for tle in args.iter() {
            tl.push(tle.clone_in(mcx)?);
        }
    }
    execTuples_seams::exec_type_from_tl::call(mcx, &tl)
}

/// `ExecInitExtraTupleSlot(estate, desc, &TTSOpsMinimalTuple)` — owned by
/// execTuples; the unit already declares `exec_init_extra_tuple_slot`.
fn exec_init_extra_tuple_slot_minimal<'mcx>(
    estate: &mut EStateData<'mcx>,
    desc: &::types_tuple::heaptuple::TupleDesc<'mcx>,
) -> PgResult<SlotId> {
    let desc_clone = clone_tuple_desc(desc, estate.es_query_cxt)?;
    execTuples_seams::exec_init_extra_tuple_slot::call(
        estate,
        desc_clone,
        ::nodes::TupleSlotKind::MinimalTuple,
    )
}

/// `tle = get_sortgroupclause_tle(sortcl, aggref->args)` then `tle->resno` and
/// `exprCollation((Node *) tle->expr)` (nodes/nodeFuncs.c).
fn sortgroupclause_tle_resno_and_collation<'mcx>(
    sortcl: &::nodes::nodeagg::SortGroupClauseAgg,
    aggref: &Aggref<'mcx>,
) -> PgResult<(::types_core::primitive::AttrNumber, Oid)> {
    // get_sortgroupclause_tle: the TargetEntry whose ressortgroupref matches.
    let args = aggref
        .args
        .as_ref()
        .expect("sortgroupclause_tle: aggref->args is NULL");
    let tle = args
        .iter()
        .find(|tle| tle.ressortgroupref == sortcl.tle_sort_group_ref)
        .expect("get_sortgroupclause_tle: no matching TargetEntry");
    let collation = match tle.expr.as_ref() {
        Some(e) => nodeFuncs_seams::exprCollation::call(e),
        None => INVALID_OID,
    };
    Ok((tle.resno, collation))
}

/// `fmgr_info(get_opcode(eqop), &pertrans->equalfnOne)` — single-column
/// distinct comparator (lsyscache get_opcode + fmgr_info).
fn fmgr_info_get_opcode_into_equalfn_one<'mcx>(
    pertrans: &mut AggStatePerTransData<'mcx>,
    eqop: Oid,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let opcode = lsyscache_seams::get_opcode::call(eqop)?;
    pertrans.equalfn_one = fmgr_seams::fmgr_info::call(mcx, opcode)?;
    Ok(())
}

/// `execTuplesMatchPrepare(...)` (execGrouping.c) — multi-column distinct
/// comparator ExprState.
fn exec_tuples_match_prepare_owned<'mcx>(
    pertrans: &mut AggStatePerTransData<'mcx>,
    aggstate: &mut AggStateData<'mcx>,
    ops: &PgVec<'mcx, Oid>,
    num_distinct_cols: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    // execTuplesMatchPrepare(sortdesc, numDistinctCols, sortColIdx, ops,
    //                        sortCollations, &aggstate->ss.ps)
    debug_assert!(
        pertrans.sortdesc.is_some(),
        "exec_tuples_match_prepare: sortdesc is NULL"
    );
    let desc = clone_tuple_desc(&pertrans.sortdesc, mcx)?;
    let sort_col_idx = pertrans
        .sort_col_idx
        .as_ref()
        .expect("exec_tuples_match_prepare: sortColIdx is NULL")
        .clone();
    let sort_collations = pertrans
        .sort_collations
        .as_ref()
        .expect("exec_tuples_match_prepare: sortCollations is NULL")
        .clone();
    pertrans.equalfn_multi = execGrouping_seams::exec_tuples_match_prepare::call(
        desc,
        num_distinct_cols,
        &sort_col_idx,
        ops,
        &sort_collations,
        &mut aggstate.ss.ps,
        estate,
    )?;
    Ok(())
}

/// Deep-copy a `TupleDesc` (the C reads a shared `TupleDesc` pointer; the owned
/// seams take an owned `TupleDesc`, so clone it for each consumer).
fn clone_tuple_desc<'mcx>(
    desc: &::types_tuple::heaptuple::TupleDesc<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<::types_tuple::heaptuple::TupleDesc<'mcx>> {
    match desc {
        Some(d) => Ok(Some(alloc_in(mcx, d.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

/// `GetAggInitVal(textInitVal, transtype)` — convert the `agginitval` text
/// Datum into the transition type's internal Datum via its input function.
pub fn GetAggInitVal<'mcx>(
    mcx: Mcx<'mcx>,
    str_init_val: &str,
    transtype: Oid,
) -> PgResult<Datum<'mcx>> {
    // getTypeInputInfo(transtype, &typinput, &typioparam);
    // strInitVal = TextDatumGetCString(textInitVal);
    // initVal = OidInputFunctionCall(typinput, strInitVal, typioparam, -1);
    //
    // The repo carries the agginitval as the already-detoasted `Option<String>`
    // column of `AggFormData` (the syscache projection did the `SysCacheGetAttr`
    // + text read), so the C `TextDatumGetCString` is folded into the caller;
    // here we run getTypeInputInfo + OidInputFunctionCall.
    let (typinput, typioparam) =
        lsyscache_seams::get_type_input_info::call(transtype)?;
    fmgr_seams::oid_input_function_call::call(
        mcx,
        typinput,
        str_init_val,
        typioparam,
        -1,
    )
}

/// `ExecProcNode` callback dispatcher for an Agg node: recover the concrete
/// `AggStateData` from the `PlanStateNode::Agg` carrier (the C
/// `castNode(AggState, pstate)`) and run [`ExecAgg`].
pub fn exec_agg_node<'mcx>(
    pstate: &mut ::nodes::planstate::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let agg = pstate
        .as_agg_state_mut_typed::<AggStateData<'mcx>>()
        .expect("castNode(AggState, pstate) failed");
    ExecAgg(agg, estate)
}

/// `ExecAgg(pstate)` — the node's `ExecProcNode` callback: produce the next
/// aggregated output tuple, or `None` at end. Dispatches on strategy/phase to
/// the sorted-grouping and hash-grouping retrieve paths.
pub fn ExecAgg<'mcx>(
    pstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let mut result: Option<SlotId> = None;

    postgres_seams::check_for_interrupts::call()?;

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
fn current_phase_strategy(aggstate: &AggStateData<'_>) -> ::nodes::nodeagg::AggStrategy {
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
    //
    //   si = &node->shared_info->sinstrument[ParallelWorkerNumber];
    //   si->hash_batches_used = node->hash_batches_used;
    //   si->hash_disk_used = node->hash_disk_used;
    //   si->hash_mem_peak = node->hash_mem_peak;
    //
    // The worker writes ONLY its own slot in the DSM `SharedAggInfo` flex array;
    // the worker is the sole writer of that element, satisfying `with_mut`'s
    // sole-accessor obligation.
    if is_parallel_worker() {
        if let Some(crate::aggstate::SharedAggInfo::Dsm {
            chunk,
            seg,
            num_workers,
        }) = node.shared_info.as_ref()
        {
            let worker = parallel_worker_number();
            debug_assert!(worker >= 0 && worker < *num_workers);
            if worker >= 0 && worker < *num_workers {
                let stats = crate::aggstate::AggregateInstrumentation {
                    hash_batches_used: node.hash_batches_used,
                    hash_disk_used: node.hash_disk_used,
                    hash_mem_peak: node.hash_mem_peak,
                };
                let elem = crate::aggapi::sinstrument_slot_cursor(*chunk, worker);
                transam_parallel::shared_dsm_object::with_mut::<
                    crate::aggstate::AggregateInstrumentation,
                    (),
                >(*seg, elem, |si| {
                    *si = stats;
                });
            }
        }
    }

    // Make sure we have closed any open tuplesorts.
    if let Some(sort_in) = node.sort_in.take() {
        tuplesort_seams::tuplesort_end::call(sort_in)?;
    }
    if let Some(sort_out) = node.sort_out.take() {
        tuplesort_seams::tuplesort_end::call(sort_out)?;
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
                        tuplesort_seams::tuplesort_end::call(ts)?;
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
        execProcnode_seams::exec_end_node::call(outer, estate)?;
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
        execUtils_seams::re_scan_expr_context::call(estate, ecxt)?;
    }
    Ok(())
}

/// `ReScanExprContext(aggstate->hashcontext)`.
fn rescan_expr_context_hashcontext<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(ecxt) = node.hashcontext {
        execUtils_seams::re_scan_expr_context::call(estate, ecxt)?;
    }
    Ok(())
}

/// `IsParallelWorker()` (parallel.h) — am I a parallel worker backend? Owned by
/// the parallel-infra; per-backend identity.
fn is_parallel_worker() -> bool {
    transam_parallel::is_parallel_worker()
}

/// `ParallelWorkerNumber` (parallel.c) — this worker's index, or -1 in the
/// leader. Per-backend identity owned by the parallel infra.
fn parallel_worker_number() -> i32 {
    transam_parallel::parallel_worker_number()
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
        let overlap = nodes_core_seams::bms_overlap::call(
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
            let iter = execGrouping_seams::init_tuple_hash_iterator::call(
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
                        tuplesort_seams::tuplesort_end::call(ts)?;
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
    clear_agg_values(node, estate)?;

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

        let mcx = estate.es_query_cxt;
        crate::hash_grouping::hashagg_recompile_expressions(node, false, false, estate, mcx)?;
    }

    if node.aggstrategy != AGG_HASHED {
        // Reset the per-group state (in particular, mark transvalues null).
        // The sorted-path pergroups ALIAS the head of all_pergroups (the single
        // source of truth the transition interpreter mutates), so reset there.
        let numaggs = node.numaggs;
        if let Some(pergroups) = node.all_pergroups.as_mut() {
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
    node: &'a AggStateData<'mcx>,
) -> PgResult<Option<&'a Bitmapset<'mcx>>> {
    // C: `Agg *aggnode = (Agg *) node->ss.ps.plan; ... aggnode->aggParams`.
    // The plan back-link aliases the shared read-only plan tree (the wrapping
    // `Node::Agg`); downcast it with `castNode(Agg, ...)` (`as_agg`) and read
    // `aggParams`. `NULL`/no-plan → `None` (`bms_overlap` treats it as empty).
    let agg_params = node
        .ss
        .ps
        .plan
        .and_then(|p| p.as_agg())
        .and_then(|agg| agg.agg_params.as_deref());
    Ok(agg_params)
}

/// `ExecClearTuple(slot)` (execTuples.c).
fn clear_slot<'mcx>(estate: &mut EStateData<'mcx>, slot_id: SlotId) -> PgResult<()> {
    execTuples_seams::exec_clear_tuple::call(estate, slot_id)
}

/// `MemSet(econtext->ecxt_aggvalues/ecxt_aggnulls, 0, ...)` — forget the
/// current aggregate values held in the node's per-output-tuple ExprContext.
/// The aggvalues/aggnulls arrays live on the EState-owned ExprContext the node
/// projects through; clearing them is owned by execUtils alongside the
/// EcxtId-addressed context pool. The owned AggState model does not carry the
/// EcxtId for ps_ExprContext on its facet here, so the clear lands with the
/// execUtils ExprContext model.
fn clear_agg_values<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // C: `MemSet(econtext->ecxt_aggvalues, 0, sizeof(Datum) * node->numaggs);
    //     MemSet(econtext->ecxt_aggnulls, 0, sizeof(bool) * node->numaggs);`
    // The aggvalues/aggnulls arrays live on the node's per-output-tuple
    // ExprContext (`ss.ps.ps_ExprContext`), carried by the owned AggState as an
    // EcxtId into the EState pool (the SAME id `alloc_agg_result_storage`
    // populated at init). Clear the first `numaggs` slots through the execUtils
    // `clear_agg_values` seam. If no ExprContext is assigned (numaggs==0, no agg
    // storage was allocated), there is nothing to clear.
    if let Some(ecxt_id) = node.ss.ps.ps_ExprContext {
        execUtils_seams::clear_agg_values::call(estate, ecxt_id, node.numaggs)?;
    }
    Ok(())
}

/// `ExecReScan(outerPlanState(node))` (execAmi.c) — rescan the child plan
/// subtree. Owned by the execAmi unit; routed through the node-dispatch seam
/// when it lands. The outer plan state head is lent for the rescan.
fn exec_rescan_outer<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // C: `ExecReScan(outerPlan)` where `outerPlan = outerPlanState(node)`
    // (== `node->ss.ps.lefttree`). Dispatch through the execAmi node-rescan
    // seam. The owned child PlanState box is lent (taken out, rescanned, and
    // put back) so the borrow checker can hand `&mut PlanStateNode` plus the
    // disjoint `&mut EStateData` to the seam — matching nodeSubplan's
    // take/put pattern. The box is restored even when the rescan errors so the
    // owned plan-state tree is never left with a hole.
    if let Some(mut outer) = node.ss.ps.lefttree.take() {
        let r = execAmi_seams::exec_re_scan::call(&mut outer, estate);
        node.ss.ps.lefttree = Some(outer);
        r
    } else {
        Ok(())
    }
}
