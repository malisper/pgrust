//! `ExecInitAgg` sub-module: building the `AggState` from the Agg plan node.
//!
//! Split out of [`crate::node_lifecycle`] because the C `ExecInitAgg`
//! (`nodeAgg.c` ~854 lines: catalog reads for every aggregate, per-trans /
//! per-agg setup, phase and grouping-set layout, hash-table and context
//! creation) is far larger than the rest of the lifecycle family combined.

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::execnodes::PlanStateData;
use types_nodes::nodeagg::{
    do_aggsplit_combine, do_aggsplit_deserialize, do_aggsplit_serialize, do_aggsplit_skipfinal,
    Agg, AGG_HASHED, AGG_MIXED, AGG_PLAIN, AGG_SORTED,
};
use crate::aggstate::{
    AggStateData, AggStatePerAggData, AggStatePerGroupData, AggStatePerHashData,
    AggStatePerPhaseData, AggStatePerTransData,
};
use types_nodes::{AggStrategy, EStateData, Sort, TupleSlotKind};

use crate::hash_grouping::{hash_agg_entry_size, hash_create_memory};
use crate::node_lifecycle::{
    build_pertrans_for_aggref, find_hash_columns, initialize_phase, select_current_set,
    GetAggInitVal,
};
use crate::spill::hash_agg_set_limits;

// ---------------------------------------------------------------------------
// Constants verified against C headers (executor/executor.h, access/htup.h,
// catalog/pg_type_d.h, access/transam.h, postgres_ext.h).
// ---------------------------------------------------------------------------

/// `EXEC_FLAG_REWIND` (executor/executor.h).
const EXEC_FLAG_REWIND: i32 = 0x0001;
/// `EXEC_FLAG_BACKWARD` (executor/executor.h).
const EXEC_FLAG_BACKWARD: i32 = 0x0002;
/// `EXEC_FLAG_MARK` (executor/executor.h).
const EXEC_FLAG_MARK: i32 = 0x0004;
/// `EXEC_FLAG_EXPLAIN_ONLY` (executor/executor.h).
const EXEC_FLAG_EXPLAIN_ONLY: i32 = 0x0020;

/// `INTERNALOID` (catalog/pg_type_d.h).
const INTERNALOID: Oid = 2281;

/// `InvalidOid` (postgres_ext.h).
const INVALID_OID: Oid = types_core::primitive::INVALID_OID;

/// `AGGKIND_IS_ORDERED_SET(kind)` (catalog/pg_aggregate.h): true unless
/// `kind == AGGKIND_NORMAL` (`'n'`).
#[inline]
fn aggkind_is_ordered_set(aggkind: i8) -> bool {
    aggkind != b'n' as i8
}

/// `ExecInitAgg(node, estate, eflags)` — build the `AggState` from the Agg
/// plan node: catalog reads for every aggregate, per-trans/per-agg setup,
/// phase and grouping-set layout, hash-table and context creation.
pub fn ExecInitAgg<'mcx>(
    node: &'mcx Agg<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut eflags: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<PgBox<'mcx, AggStateData<'mcx>>> {
    // check for unsupported flags
    debug_assert!(
        eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0,
        "Agg does not support EXEC_FLAG_BACKWARD/EXEC_FLAG_MARK"
    );

    let use_hashing = node.aggstrategy == AGG_HASHED || node.aggstrategy == AGG_MIXED;

    // create state structure
    let mut aggstate = alloc_in(mcx, AggStateData::new_in(mcx)?)?;
    aggstate.ss.ps.plan = None; // C: aggstate->ss.ps.plan = (Plan *) node — the
                                // owned model has no Agg/AggState Node variant
                                // yet; the back-link lands with the Node enum's
                                // Agg arm.
    aggstate.ss.ps.ExecProcNode = None; // C: ExecAgg — installed once the
                                        // PlanStateNode gains an AggState arm so
                                        // ExecProcNode can dispatch to it.

    aggstate.aggs = None;
    aggstate.numaggs = 0;
    aggstate.numtrans = 0;
    aggstate.aggstrategy = node.aggstrategy;
    aggstate.aggsplit = node.aggsplit;
    aggstate.maxsets = 0;
    aggstate.projected_set = -1;
    aggstate.current_set = 0;
    aggstate.peragg = None;
    aggstate.pertrans = None;
    aggstate.curperagg = -1;
    aggstate.curpertrans = -1;
    aggstate.input_done = false;
    aggstate.agg_done = false;
    aggstate.pergroups = None;
    aggstate.grp_first_tuple = None;
    aggstate.sort_in = None;
    aggstate.sort_out = None;

    // phases[0] always exists, but is dummy in sorted/plain mode
    let mut num_phases = if use_hashing { 1 } else { 2 };
    let mut num_hashes = if use_hashing { 1 } else { 0 };

    // Calculate the maximum number of grouping sets in any phase; this
    // determines the size of some allocations. Also calculate the number of
    // phases, since all hashed/mixed nodes contribute to only a single phase.
    let mut num_grouping_sets: i32 = 1;
    if node.grouping_sets.is_some() {
        num_grouping_sets = list_length(&node.grouping_sets) as i32;

        if let Some(chain) = node.chain.as_ref() {
            for agg in chain.iter() {
                num_grouping_sets =
                    num_grouping_sets.max(list_length(&agg.grouping_sets) as i32);

                // additional AGG_HASHED aggs become part of phase 0, but all
                // others add an extra phase.
                if agg.aggstrategy != AGG_HASHED {
                    num_phases += 1;
                } else {
                    num_hashes += 1;
                }
            }
        }
    }

    aggstate.maxsets = num_grouping_sets;
    aggstate.numphases = num_phases;

    // aggcontexts = palloc0(sizeof(ExprContext *) * numGroupingSets);
    let mut aggcontexts: PgVec<'mcx, PgBox<'mcx, types_nodes::execnodes::ExprContext<'mcx>>> =
        vec_with_capacity_in(mcx, num_grouping_sets as usize)?;
    // The per-grouping-set ExprContexts are created by ExecAssignExprContext
    // below; the AggStateData carries them as owned PgBox<ExprContext>, but the
    // execUtils owner builds them in the EState pool (EcxtId). Bridging the two
    // representations is execUtils-owned work: panic loudly until execUtils lands.
    let _ = &mut aggcontexts;

    // Create expression contexts. We need three or more: per-input-tuple
    // processing, per-output-tuple processing, one for all the hashtables, and
    // one for each grouping set.
    //
    // ExecAssignExprContext(estate, &aggstate->ss.ps);
    // aggstate->tmpcontext = aggstate->ss.ps.ps_ExprContext;
    backend_executor_execUtils_seams::exec_assign_expr_context::call(
        estate,
        &mut aggstate.ss.ps,
    )?;
    // C: aggstate->tmpcontext = aggstate->ss.ps.ps_ExprContext (an ExprContext*).
    // The owned model's ps_ExprContext is an EcxtId into the EState pool, while
    // AggStateData.tmpcontext is Option<PgBox<ExprContext>>; reconciling the two
    // shapes is execUtils/types-nodes-owned. Until the ExprContext storage model
    // is unified, the assignment cannot be expressed faithfully.
    panic!(
        "backend-executor-nodeAgg::ExecInitAgg: ExprContext storage model unresolved \
         (AggState carries PgBox<ExprContext> but execUtils owns EcxtId-pooled \
         ExprContexts); tmpcontext/aggcontexts/hashcontext assignment, the \
         per-grouping-set ExecAssignExprContext loop, hash_create_memory, the \
         outer-plan init (ExecInitNode), source-slot setup \
         (ExecGetResultSlotOps/ExecCreateScanSlotFromOuterPlan), the resort slot \
         (ExecInitExtraTupleSlot), result slot+projection \
         (ExecInitResultTupleSlotTL/ExecAssignProjectionInfo), qual init \
         (ExecInitQual), the per-phase grouping-set/eqfunction build \
         (execTuplesMatchPrepare), the per-Aggref catalog reads \
         (SearchSysCache1 AGGFNOID/PROCOID, object_aclcheck, GetUserId, \
         InvokeFunctionExecuteHook, get_aggregate_argtypes, ExecInitExprList, \
         build_aggregate_finalfn_expr, fmgr_info, get_typlenbyval, SysCacheGetAttr, \
         build_pertrans_for_aggref, IsBinaryCoercible, format_type_be), and the \
         per-phase ExecBuildAggTrans build all depend on owners not yet ported"
    );

    // The remainder of ExecInitAgg follows the C structure below; it is kept
    // (unreachable past the panic above) so the owned arithmetic lands verbatim
    // once the ExprContext storage model and the missing executor/catalog seams
    // are available. Each cross-subsystem step is annotated with its C call.
    #[allow(unreachable_code)]
    {
        for _i in 0..num_grouping_sets {
            // ExecAssignExprContext(estate, &aggstate->ss.ps);
            // aggstate->aggcontexts[i] = aggstate->ss.ps.ps_ExprContext;
            backend_executor_execUtils_seams::exec_assign_expr_context::call(
                estate,
                &mut aggstate.ss.ps,
            )?;
        }

        if use_hashing {
            // hash_create_memory(aggstate);
            hash_create_memory(&mut aggstate)?;
        }

        // The per-output-tuple context.
        backend_executor_execUtils_seams::exec_assign_expr_context::call(
            estate,
            &mut aggstate.ss.ps,
        )?;

        // Initialize child nodes. If hashed, the child needn't handle REWIND.
        if node.aggstrategy == AGG_HASHED {
            eflags &= !EXEC_FLAG_REWIND;
        }
        // outerPlan = outerPlan(node);
        // outerPlanState(aggstate) = ExecInitNode(outerPlan, estate, eflags);
        let outer_plan = node.plan.lefttree.as_deref();
        let child = backend_executor_execProcnode_seams::exec_init_node::call(
            mcx, outer_plan, estate, eflags,
        )?;
        aggstate.ss.ps.lefttree = child;

        // Initialize source tuple type.
        // aggstate->ss.ps.outerops = ExecGetResultSlotOps(outerPlanState(...),
        //                                                  &outeropsfixed);
        // aggstate->ss.ps.outeropsset = true;
        set_outer_slot_ops_from_outer_plan(&mut aggstate);

        // ExecCreateScanSlotFromOuterPlan(estate, &aggstate->ss, outerops);
        let outerops = outer_slot_ops(&aggstate);
        backend_executor_execUtils_seams::exec_create_scan_slot_from_outer_plan::call(
            estate,
            &mut aggstate.ss,
            outerops,
        )?;
        // scanDesc = aggstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
        // (read where needed below, through the execTuples owner)

        // If there are more than two phases (incl. a dummy phase 0), input is
        // resorted using tuplesort; need a slot.
        if num_phases > 2 {
            // aggstate->sort_slot = ExecInitExtraTupleSlot(estate, scanDesc,
            //                                              &TTSOpsMinimalTuple);
            let scan_desc = scan_tuple_desc(&aggstate, estate)?;
            aggstate.sort_slot =
                Some(backend_executor_execTuples_seams::exec_init_extra_tuple_slot::call(
                    estate,
                    scan_desc,
                    TupleSlotKind::MinimalTuple,
                )?);

            // if (outeropsfixed && outerops != &TTSOpsMinimalTuple)
            //     outeropsfixed = false;
            if aggstate.ss.ps.resultopsfixed
                && aggstate.ss.ps.scanops != Some(TupleSlotKind::MinimalTuple)
            {
                aggstate.ss.ps.scanopsfixed = false;
            }
        }

        // Initialize result type, slot and projection.
        // ExecInitResultTupleSlotTL(&aggstate->ss.ps, &TTSOpsVirtual);
        backend_executor_execTuples_seams::exec_init_result_tuple_slot_tl::call(
            &mut aggstate.ss.ps,
            estate,
            TupleSlotKind::Virtual,
        )?;
        // ExecAssignProjectionInfo(&aggstate->ss.ps, NULL);
        backend_executor_execUtils_seams::exec_assign_projection_info::call(
            &mut aggstate.ss.ps,
            estate,
            None,
        )?;

        // initialize child expressions. execExpr.c finds Aggrefs for us and
        // adds them to aggstate->aggs. Aggrefs in the qual are found here.
        // aggstate->ss.ps.qual = ExecInitQual(node->plan.qual, (PlanState *)aggstate);
        aggstate.ss.ps.qual = exec_init_qual_for_agg(&mut aggstate, node, estate)?;

        // We should now have found all Aggrefs in the targetlist and quals.
        let numaggrefs = list_length(&aggstate.aggs) as i32;
        let mut max_aggno: i32 = -1;
        let mut max_transno: i32 = -1;
        if let Some(aggs) = aggstate.aggs.as_ref() {
            for aggref in aggs.iter() {
                max_aggno = max_aggno.max(aggref.aggno);
                max_transno = max_transno.max(aggref.aggtransno);
            }
        }
        let numaggs = max_aggno + 1;
        let numtrans = max_transno + 1;
        aggstate.numaggs = numaggs;
        aggstate.numtrans = numtrans;

        // For each phase, prepare grouping set data and fmgr lookup data.
        // aggstate->phases = palloc0(numPhases * sizeof(AggStatePerPhaseData));
        let mut phases: PgVec<'mcx, AggStatePerPhaseData<'mcx>> =
            vec_with_capacity_in(mcx, num_phases as usize)?;
        for _ in 0..num_phases {
            phases.push(AggStatePerPhaseData::default());
        }
        aggstate.phases = Some(phases);

        aggstate.num_hashes = num_hashes;
        if num_hashes > 0 {
            // aggstate->perhash = palloc0(sizeof(AggStatePerHashData) * numHashes);
            let mut perhash: PgVec<'mcx, AggStatePerHashData<'mcx>> =
                vec_with_capacity_in(mcx, num_hashes as usize)?;
            for _ in 0..num_hashes {
                perhash.push(AggStatePerHashData::default());
            }
            aggstate.perhash = Some(perhash);

            let phases = aggstate.phases.as_mut().unwrap();
            phases[0].numsets = 0;
            // gset_lengths = palloc(numHashes * sizeof(int));
            let mut gl: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, num_hashes as usize)?;
            for _ in 0..num_hashes {
                gl.push(0);
            }
            phases[0].gset_lengths = Some(gl);
            // grouped_cols = palloc(numHashes * sizeof(Bitmapset *));
            let gc: PgVec<'mcx, PgBox<'mcx, types_nodes::Bitmapset<'mcx>>> =
                vec_with_capacity_in(mcx, num_hashes as usize)?;
            phases[0].grouped_cols = Some(gc);
        }

        // The per-phase loop (grouping-set arithmetic in-crate; the bms
        // accumulation into all_grouped_cols and the execTuplesMatchPrepare
        // eqfunction builds are bitmapset/execGrouping owned).
        build_phases(&mut aggstate, node, mcx, estate)?;

        // Convert all_grouped_cols to a descending-order list — owned by the
        // bitmapset unit (bms_next_member + lcons_int); lands with bitmapset.
        convert_all_grouped_cols(&mut aggstate, mcx)?;

        // Set up aggregate-result storage in the output expr context, and
        // allocate per-agg working storage.
        // econtext->ecxt_aggvalues = palloc0(sizeof(Datum) * numaggs);
        // econtext->ecxt_aggnulls = palloc0(sizeof(bool) * numaggs);
        alloc_agg_result_storage(&mut aggstate, estate, numaggs, mcx)?;

        // peraggs = palloc0(sizeof(AggStatePerAggData) * numaggs);
        let mut peraggs: PgVec<'mcx, AggStatePerAggData<'mcx>> =
            vec_with_capacity_in(mcx, numaggs as usize)?;
        for _ in 0..numaggs {
            peraggs.push(AggStatePerAggData::default());
        }
        // pertransstates = palloc0(sizeof(AggStatePerTransData) * numtrans);
        let mut pertransstates: PgVec<'mcx, AggStatePerTransData<'mcx>> =
            vec_with_capacity_in(mcx, numtrans as usize)?;
        for _ in 0..numtrans {
            pertransstates.push(AggStatePerTransData::default());
        }
        aggstate.peragg = Some(peraggs);
        aggstate.pertrans = Some(pertransstates);

        // all_pergroups = palloc0(sizeof(AggStatePerGroup) *
        //                         (numGroupingSets + numHashes));
        let mut all_pergroups: PgVec<'mcx, Option<PgVec<'mcx, AggStatePerGroupData<'mcx>>>> =
            vec_with_capacity_in(mcx, (num_grouping_sets + num_hashes) as usize)?;
        for _ in 0..(num_grouping_sets + num_hashes) {
            all_pergroups.push(None);
        }

        let mut pergroup_offset = 0usize;
        if node.aggstrategy != AGG_HASHED {
            for i in 0..num_grouping_sets as usize {
                // pergroups[i] = palloc0(sizeof(AggStatePerGroupData) * numaggs);
                let mut pg: PgVec<'mcx, AggStatePerGroupData<'mcx>> =
                    vec_with_capacity_in(mcx, numaggs as usize)?;
                for _ in 0..numaggs {
                    pg.push(AggStatePerGroupData::default());
                }
                all_pergroups[i] = Some(pg);
            }
            // aggstate->pergroups = pergroups; pergroups += numGroupingSets;
            pergroup_offset = num_grouping_sets as usize;
        }
        // The C aliases aggstate->pergroups / aggstate->hash_pergroup into the
        // single all_pergroups buffer; the owned model carries separate owned
        // PgVecs, so split the buffer's regions accordingly.
        assign_pergroup_regions(
            &mut aggstate,
            all_pergroups,
            node,
            num_grouping_sets,
            pergroup_offset,
        )?;

        // Hashing can only appear in the initial phase.
        if use_hashing {
            // hash_spill_rslot = ExecInitExtraTupleSlot(estate, scanDesc, MinimalTuple);
            // hash_spill_wslot = ExecInitExtraTupleSlot(estate, scanDesc, Virtual);
            let scan_desc_r = scan_tuple_desc(&aggstate, estate)?;
            aggstate.hash_spill_rslot =
                Some(backend_executor_execTuples_seams::exec_init_extra_tuple_slot::call(
                    estate,
                    scan_desc_r,
                    TupleSlotKind::MinimalTuple,
                )?);
            let scan_desc_w = scan_tuple_desc(&aggstate, estate)?;
            aggstate.hash_spill_wslot =
                Some(backend_executor_execTuples_seams::exec_init_extra_tuple_slot::call(
                    estate,
                    scan_desc_w,
                    TupleSlotKind::Virtual,
                )?);

            // hashentrysize = hash_agg_entry_size(numtrans, outerplan->plan_width,
            //                                     node->transitionSpace);
            // The Plan type is trimmed of plan_width (lands with its first
            // consumer); use 0 until it is carried, matching the C 0 width for
            // a plan whose width has not been costed.
            let outer_plan_width = 0usize;
            aggstate.hashentrysize = hash_agg_entry_size(
                aggstate.numtrans,
                outer_plan_width,
                node.transition_space as usize,
            ) as f64;

            // totalGroups across all hashes.
            let mut total_groups: u64 = 0;
            if let Some(perhash) = aggstate.perhash.as_ref() {
                for k in 0..aggstate.num_hashes as usize {
                    total_groups += perhash[k]
                        .aggnode
                        .as_ref()
                        .map(|a| a.num_groups)
                        .unwrap_or(0) as u64;
                }
            }

            // hash_agg_set_limits(hashentrysize, totalGroups, 0, &mem_limit,
            //                     &ngroups_limit, &planned_partitions);
            let (mem_limit, ngroups_limit, planned_partitions) =
                hash_agg_set_limits(aggstate.hashentrysize, total_groups as f64, 0);
            aggstate.hash_mem_limit = mem_limit;
            aggstate.hash_ngroups_limit = ngroups_limit;
            aggstate.hash_planned_partitions = planned_partitions;

            // find_hash_columns(aggstate);
            find_hash_columns(&mut aggstate, mcx)?;

            // Skip massive memory allocation if just doing EXPLAIN.
            if eflags & EXEC_FLAG_EXPLAIN_ONLY == 0 {
                crate::hash_grouping::build_hash_tables(&mut aggstate, estate)?;
            }

            aggstate.table_filled = false;
            // Initialize to 1, meaning nothing spilled yet.
            aggstate.hash_batches_used = 1;
        }

        // Initialize current phase-dependent values to the initial phase.
        if node.aggstrategy == AGG_HASHED {
            aggstate.current_phase = 0;
            initialize_phase(&mut aggstate, 0, estate)?;
            select_current_set(&mut aggstate, 0, true);
        } else {
            aggstate.current_phase = 1;
            initialize_phase(&mut aggstate, 1, estate)?;
            select_current_set(&mut aggstate, 0, false);
        }

        // Perform aggregate-function info lookups + per-agg/per-trans init.
        init_per_aggref(&mut aggstate, estate, numaggrefs, mcx)?;

        // Build per-phase transition expressions (ExecBuildAggTrans).
        build_phase_eval_trans(&mut aggstate, estate, mcx)?;

        Ok(aggstate)
    }
}

// ---------------------------------------------------------------------------
// nodeAgg.c-local helpers (the ExecInitAgg sub-steps).
// ---------------------------------------------------------------------------

/// `list_length(list)` for an `Option<PgVec<_>>` field (the C `NIL` is `None`).
#[inline]
fn list_length<T>(list: &Option<PgVec<'_, T>>) -> usize {
    list.as_ref().map(|v| v.len()).unwrap_or(0)
}

/// C: `aggstate->ss.ps.outerops = ExecGetResultSlotOps(outerPlanState(...),
/// &outeropsfixed); aggstate->ss.ps.outeropsset = true;`
///
/// The owned `PlanStateData` carries the outer-slot ops in the scan-ops fields
/// (no separate `outerops` field), so read the child's result slot ops and
/// record them as the scan ops. Reaches the execTuples owner for the ops class.
fn set_outer_slot_ops_from_outer_plan(aggstate: &mut AggStateData<'_>) {
    let ops = match aggstate.ss.ps.lefttree.as_ref() {
        Some(child) => {
            let child_ps: &PlanStateData<'_> = child.ps_head();
            backend_executor_execTuples_seams::exec_get_result_slot_ops::call(child_ps)
        }
        None => TupleSlotKind::Virtual,
    };
    aggstate.ss.ps.scanops = Some(ops);
    aggstate.ss.ps.scanopsfixed = true;
    aggstate.ss.ps.scanopsset = true;
}

/// The outer-plan slot ops recorded by [`set_outer_slot_ops_from_outer_plan`].
#[inline]
fn outer_slot_ops(aggstate: &AggStateData<'_>) -> TupleSlotKind {
    aggstate.ss.ps.scanops.unwrap_or(TupleSlotKind::Virtual)
}

/// C: `scanDesc = aggstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;` — the
/// descriptor of the scan slot, owned by execTuples (the slot pool). The
/// trimmed `TupleTableSlot` carries no descriptor payload yet, so the read is
/// execTuples-owned: panic until the slot payload model lands.
fn scan_tuple_desc<'mcx>(
    _aggstate: &AggStateData<'mcx>,
    _estate: &EStateData<'mcx>,
) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>> {
    panic!(
        "backend-executor-execTuples (slot payload): reading \
         ss_ScanTupleSlot->tts_tupleDescriptor needs the descriptor the slot \
         pool owns; not ported"
    )
}

/// C: `aggstate->ss.ps.qual = ExecInitQual(node->plan.qual, (PlanState *)aggstate);`
/// — execExpr also pushes any Aggrefs found in the qual into `aggstate->aggs`.
/// The Aggref collection into `aggstate->aggs` and the qual compile are execExpr
/// owned (the `parent` is the AggState, which the owned model has no
/// PlanStateNode arm for yet).
fn exec_init_qual_for_agg<'mcx>(
    _aggstate: &mut AggStateData<'mcx>,
    _node: &Agg<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<Option<PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>> {
    panic!(
        "backend-executor-execExpr::ExecInitQual: compiling the Agg qual and \
         collecting its Aggrefs into aggstate->aggs needs the AggState parent \
         (no PlanStateNode::AggState arm yet) and execExpr's Aggref discovery; \
         not ported"
    )
}

/// The per-phase grouping-set / eqfunction build of `ExecInitAgg`
/// (the `for (phaseidx = 0; phaseidx <= list_length(node->chain); ...)` loop).
/// The grouping-set arithmetic is in-crate; the `bms_add_member` /
/// `bms_add_members` accumulation and the `execTuplesMatchPrepare` eqfunction
/// builds are reached through their owners.
fn build_phases<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    node: &Agg<'mcx>,
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let chain_len = list_length(&node.chain) as i32;

    let mut phase: i32 = 0;
    for phaseidx in 0..=chain_len {
        // aggnode = (phaseidx > 0) ? list_nth_node(Agg, node->chain, phaseidx-1)
        //                          : node;
        // sortnode = (phaseidx > 0) ? castNode(Sort, outerPlan(aggnode)) : NULL;
        let is_chained = phaseidx > 0;
        let (aggstrategy, num_cols, agg_num_groups, agg_grouping_sets_len): (AggStrategy, i32, i64, usize) =
            if is_chained {
                let agg = &node.chain.as_ref().unwrap()[(phaseidx - 1) as usize];
                (
                    agg.aggstrategy,
                    agg.num_cols,
                    agg.num_groups,
                    list_length(&agg.grouping_sets),
                )
            } else {
                (
                    node.aggstrategy,
                    node.num_cols,
                    node.num_groups,
                    list_length(&node.grouping_sets),
                )
            };

        debug_assert!(phase <= 1 || is_chained);

        if aggstrategy == AGG_HASHED || aggstrategy == AGG_MIXED {
            // phases[0] is the hash phase; record this hash's grouping set.
            debug_assert!(phase == 0);
            let phases = aggstate.phases.as_mut().unwrap();
            let i = phases[0].numsets;
            phases[0].numsets += 1;

            // phase 0 always points to the "real" Agg.
            phases[0].aggnode = Some(clone_agg_shallow(node, mcx)?);
            phases[0].aggstrategy = node.aggstrategy;

            // perhash[i].aggnode = aggnode; perhash[i].numCols = aggnode->numCols;
            let perhash = aggstate.perhash.as_mut().unwrap();
            perhash[i as usize].num_cols = num_cols;
            perhash[i as usize].aggnode = Some(clone_phase_agg(node, phaseidx, mcx)?);

            // phasedata->gset_lengths[i] = perhash->numCols = aggnode->numCols;
            let phases = aggstate.phases.as_mut().unwrap();
            if let Some(gl) = phases[0].gset_lengths.as_mut() {
                if let Some(slot) = gl.get_mut(i as usize) {
                    *slot = num_cols;
                }
            }

            // grouped_cols[i] = bms of aggnode->grpColIdx[0..numCols];
            // all_grouped_cols = bms_add_members(all_grouped_cols, cols);
            set_phase_grouped_cols_for_hash(aggstate, i, node, phaseidx, mcx)?;
            continue;
        } else {
            phase += 1;
            let p = phase as usize;
            let num_sets = agg_grouping_sets_len as i32;
            {
                let phases = aggstate.phases.as_mut().unwrap();
                phases[p].numsets = num_sets;
            }

            if num_sets > 0 {
                {
                    let phases = aggstate.phases.as_mut().unwrap();
                    let mut gl: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, num_sets as usize)?;
                    for _ in 0..num_sets {
                        gl.push(0);
                    }
                    phases[p].gset_lengths = Some(gl);
                    let gc: PgVec<'mcx, PgBox<'mcx, types_nodes::Bitmapset<'mcx>>> =
                        vec_with_capacity_in(mcx, num_sets as usize)?;
                    phases[p].grouped_cols = Some(gc);
                }
                set_phase_grouped_cols_for_sets(aggstate, phase, node, phaseidx, mcx)?;
            } else {
                debug_assert!(phaseidx == 0);
                let phases = aggstate.phases.as_mut().unwrap();
                phases[p].gset_lengths = None;
                phases[p].grouped_cols = None;
            }

            // Precompute fmgr lookup data for the inner loop if grouping.
            if aggstrategy == AGG_SORTED {
                // eqfunctions = palloc0(numCols * sizeof(ExprState *));
                {
                    let phases = aggstate.phases.as_mut().unwrap();
                    let mut eqf: PgVec<
                        'mcx,
                        Option<PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>,
                    > = vec_with_capacity_in(mcx, num_cols as usize)?;
                    for _ in 0..num_cols {
                        eqf.push(None);
                    }
                    phases[p].eqfunctions = Some(eqf);
                }

                let numsets = aggstate.phases.as_ref().unwrap()[p].numsets;
                for k in 0..numsets {
                    let length = aggstate.phases.as_ref().unwrap()[p]
                        .gset_lengths
                        .as_ref()
                        .map(|gl| gl[k as usize])
                        .unwrap_or(0);
                    // nothing to do for empty grouping set
                    if length == 0 {
                        continue;
                    }
                    // if we already had one of this length, it'll do
                    let already = aggstate.phases.as_ref().unwrap()[p]
                        .eqfunctions
                        .as_ref()
                        .and_then(|e| e.get((length - 1) as usize))
                        .map(|e| e.is_some())
                        .unwrap_or(false);
                    if already {
                        continue;
                    }
                    build_phase_eqfunction(aggstate, phase, length, node, phaseidx, estate, mcx)?;
                }

                // and for all grouped columns, unless already computed
                if num_cols > 0 {
                    let need = aggstate.phases.as_ref().unwrap()[p]
                        .eqfunctions
                        .as_ref()
                        .and_then(|e| e.get((num_cols - 1) as usize))
                        .map(|e| e.is_none())
                        .unwrap_or(true);
                    if need {
                        build_phase_eqfunction(
                            aggstate, phase, num_cols, node, phaseidx, estate, mcx,
                        )?;
                    }
                }
            }

            let phases = aggstate.phases.as_mut().unwrap();
            phases[p].aggnode = Some(clone_phase_agg(node, phaseidx, mcx)?);
            phases[p].aggstrategy = aggstrategy;
            // sortnode = castNode(Sort, outerPlan(aggnode)) for chained aggs.
            if is_chained {
                phases[p].sortnode = Some(clone_phase_sortnode(node, phaseidx, mcx)?);
            }
            let _ = agg_num_groups;
        }
    }
    Ok(())
}

/// A shallow `Agg` reference clone into `mcx` for `phases[..].aggnode`. The C
/// stores the same `Agg *` pointer; the owned `AggStatePerPhaseData.aggnode`
/// owns a `PgBox<Agg>`. A faithful copy is `Agg::clone_in`, which the types
/// crate must provide — not yet available, so this is types-nodes-owned.
fn clone_agg_shallow<'mcx>(_node: &Agg<'mcx>, _mcx: Mcx<'mcx>) -> PgResult<PgBox<'mcx, Agg<'mcx>>> {
    panic!(
        "types-nodes::Agg::clone_in: AggStatePerPhaseData.aggnode owns PgBox<Agg> \
         where C aliases the shared Agg*; a deep copy needs Agg::clone_in (not yet \
         provided by types-nodes)"
    )
}

/// The chained-Agg lookup `list_nth_node(Agg, node->chain, phaseidx-1)` (or
/// `node` for phaseidx 0), cloned for the owned `aggnode` field.
fn clone_phase_agg<'mcx>(
    _node: &Agg<'mcx>,
    _phaseidx: i32,
    _mcx: Mcx<'mcx>,
) -> PgResult<PgBox<'mcx, Agg<'mcx>>> {
    panic!(
        "types-nodes::Agg::clone_in: phases[..].aggnode / perhash[..].aggnode own \
         PgBox<Agg> where C aliases the chain's Agg*; needs Agg::clone_in"
    )
}

/// `castNode(Sort, outerPlan(aggnode))` for a chained Agg, cloned into the
/// owned `sortnode` field. The `Sort` node lives under the Agg's outer plan;
/// extracting it requires the Node enum's `Sort` arm (not present), so this is
/// types-nodes-owned.
fn clone_phase_sortnode<'mcx>(
    _node: &Agg<'mcx>,
    _phaseidx: i32,
    _mcx: Mcx<'mcx>,
) -> PgResult<PgBox<'mcx, Sort<'mcx>>> {
    panic!(
        "types-nodes::Node::Sort: castNode(Sort, outerPlan(aggnode)) needs the \
         Node enum's Sort arm to extract the chained Sort; not present"
    )
}

/// `grouped_cols[i] = bms of grpColIdx[0..numCols]; all_grouped_cols =
/// bms_add_members(all_grouped_cols, cols)` for the hash phase. The Bitmapset
/// construction and union are bitmapset-unit owned.
fn set_phase_grouped_cols_for_hash<'mcx>(
    _aggstate: &mut AggStateData<'mcx>,
    _i: i32,
    _node: &Agg<'mcx>,
    _phaseidx: i32,
    _mcx: Mcx<'mcx>,
) -> PgResult<()> {
    panic!(
        "backend-nodes-bitmapset (bms_add_member/bms_add_members): building \
         grouped_cols for the hash phase and accumulating all_grouped_cols needs \
         the Bitmapset add/union operations; not ported"
    )
}

/// The per-grouping-set `grouped_cols` build for a sorted/plain phase
/// (`for (j) cols = bms_add_member(cols, grpColIdx[j])`) plus the
/// `all_grouped_cols = bms_add_members(.., grouped_cols[0])` accumulation.
fn set_phase_grouped_cols_for_sets<'mcx>(
    _aggstate: &mut AggStateData<'mcx>,
    _phase: i32,
    _node: &Agg<'mcx>,
    _phaseidx: i32,
    _mcx: Mcx<'mcx>,
) -> PgResult<()> {
    panic!(
        "backend-nodes-bitmapset (bms_add_member/bms_add_members): building \
         per-grouping-set grouped_cols and accumulating all_grouped_cols needs \
         the Bitmapset add/union operations; not ported"
    )
}

/// One AGG_SORTED phase's comparator build:
/// `eqfunctions[length-1] = execTuplesMatchPrepare(scanDesc, length, grpColIdx,
/// grpOperators, grpCollations, (PlanState *)aggstate)`. execGrouping owned;
/// the scan descriptor and the AggState parent are not available yet.
fn build_phase_eqfunction<'mcx>(
    _aggstate: &mut AggStateData<'mcx>,
    _phase: i32,
    _length: i32,
    _node: &Agg<'mcx>,
    _phaseidx: i32,
    _estate: &mut EStateData<'mcx>,
    _mcx: Mcx<'mcx>,
) -> PgResult<()> {
    panic!(
        "backend-executor-execGrouping::execTuplesMatchPrepare: building a \
         grouping-set comparator needs the scan descriptor (execTuples slot \
         payload) and the AggState parent (no PlanStateNode arm); not ported"
    )
}

/// `i = -1; while ((i = bms_next_member(all_grouped_cols, i)) >= 0)
/// aggstate->all_grouped_cols = lcons_int(i, aggstate->all_grouped_cols);` —
/// build the descending-order grouped-column list. bitmapset-unit owned
/// (`bms_next_member`); the `lcons_int` prepend is in-crate but cannot run
/// without the iteration.
fn convert_all_grouped_cols<'mcx>(
    _aggstate: &mut AggStateData<'mcx>,
    _mcx: Mcx<'mcx>,
) -> PgResult<()> {
    panic!(
        "backend-nodes-bitmapset::bms_next_member: converting all_grouped_cols to \
         a descending-order list needs the Bitmapset iteration; not ported"
    )
}

/// `econtext->ecxt_aggvalues = palloc0(sizeof(Datum) * numaggs);
/// econtext->ecxt_aggnulls = palloc0(sizeof(bool) * numaggs);` on the
/// per-output-tuple ExprContext (`aggstate->ss.ps.ps_ExprContext`). The
/// ExprContext lives in the EState pool (EcxtId); sizing its agg arrays is
/// execUtils-owned because the AggState/EState ExprContext storage models are
/// not yet reconciled.
fn alloc_agg_result_storage<'mcx>(
    _aggstate: &mut AggStateData<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _numaggs: i32,
    _mcx: Mcx<'mcx>,
) -> PgResult<()> {
    panic!(
        "backend-executor-execUtils (ExprContext storage): sizing \
         ecxt_aggvalues/ecxt_aggnulls on the output ExprContext needs the \
         EcxtId-pooled ExprContext the EState owns; ExprContext storage model \
         not yet reconciled"
    )
}

/// Split the single `all_pergroups` buffer into the owned `pergroups` (the
/// first `numGroupingSets` entries, when not AGG_HASHED) and `hash_pergroup`
/// (the tail) regions, mirroring the C pointer aliasing
/// (`aggstate->pergroups = pergroups; pergroups += numGroupingSets;
/// aggstate->hash_pergroup = pergroups;`). The C keeps both as aliases into one
/// array; the owned model stores `all_pergroups` and slices its views. Carrying
/// three independent owned PgVecs that alias the same backing store is a
/// types-nodes ownership decision; defer it to the owner.
fn assign_pergroup_regions<'mcx>(
    _aggstate: &mut AggStateData<'mcx>,
    _all_pergroups: PgVec<'mcx, Option<PgVec<'mcx, AggStatePerGroupData<'mcx>>>>,
    _node: &Agg<'mcx>,
    _num_grouping_sets: i32,
    _pergroup_offset: usize,
) -> PgResult<()> {
    panic!(
        "types-nodes (AggStateData pergroup aliasing): aggstate->pergroups and \
         aggstate->hash_pergroup alias regions of all_pergroups in C; the owned \
         model needs an aliasing-or-split decision for the three owned PgVecs"
    )
}

/// The per-`Aggref` info-lookup loop of `ExecInitAgg`. The pg_aggregate /
/// pg_proc reads + ACL checks, `get_aggregate_argtypes`,
/// `build_aggregate_finalfn_expr` + `fmgr_info`, `GetAggInitVal`, and
/// `build_pertrans_for_aggref` reach their owners; the split-mode branch logic,
/// the INTERNAL-state serialization checks, and the strict-initval
/// binary-coercibility check are in-crate.
fn init_per_aggref<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    numaggrefs: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let aggsplit = aggstate.aggsplit;

    // foreach(l, aggstate->aggs)
    let agg_count = list_length(&aggstate.aggs);
    for idx in 0..agg_count {
        // Planner should have assigned aggregate to the correct level / split.
        let (aggno, agglevelsup, aggsplit_ref, aggtransno, aggtranstype, aggtype, aggfnoid) = {
            let aggs = aggstate.aggs.as_ref().unwrap();
            let a = &aggs[idx];
            (
                a.aggno,
                a.agglevelsup,
                a.aggsplit,
                a.aggtransno,
                a.aggtranstype,
                a.aggtype,
                a.aggfnoid,
            )
        };
        debug_assert!(agglevelsup == 0);
        debug_assert!(aggsplit_ref == aggsplit);

        // peragg = &peraggs[aggref->aggno]; if (peragg->aggref != NULL) continue;
        let already = aggstate
            .peragg
            .as_ref()
            .map(|p| p[aggno as usize].aggref.is_some())
            .unwrap_or(false);
        if already {
            continue;
        }

        // peragg->aggref = aggref; peragg->transno = aggref->aggtransno;
        set_peragg_aggref(aggstate, aggno, idx, mcx)?;
        if let Some(p) = aggstate.peragg.as_mut() {
            p[aggno as usize].transno = aggtransno;
        }

        // aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggref->aggfnoid));
        // aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);
        let aggform = fetch_agg_form(aggfnoid)?;

        // object_aclcheck(ProcedureRelationId, aggfnoid, GetUserId(), ACL_EXECUTE);
        // InvokeFunctionExecuteHook(aggfnoid);
        check_aggregate_acl(aggfnoid)?;

        debug_assert!(aggtranstype != INVALID_OID);

        // finalfn only required if finalizing.
        let finalfn_oid = if do_aggsplit_skipfinal(aggsplit) {
            INVALID_OID
        } else {
            aggform.aggfinalfn
        };
        if let Some(p) = aggstate.peragg.as_mut() {
            p[aggno as usize].finalfn_oid = finalfn_oid;
        }

        let mut serialfn_oid = INVALID_OID;
        let mut deserialfn_oid = INVALID_OID;

        // serialization/deserialization only for INTERNAL transtype.
        if aggtranstype == INTERNALOID {
            if do_aggsplit_serialize(aggsplit) {
                debug_assert!(do_aggsplit_skipfinal(aggsplit));
                if aggform.aggserialfn == INVALID_OID {
                    return Err(types_error::PgError::error(
                        "serialfunc not provided for serialization aggregation",
                    ));
                }
                serialfn_oid = aggform.aggserialfn;
            }
            if do_aggsplit_deserialize(aggsplit) {
                debug_assert!(do_aggsplit_combine(aggsplit));
                if aggform.aggdeserialfn == INVALID_OID {
                    return Err(types_error::PgError::error(
                        "deserialfunc not provided for deserialization aggregation",
                    ));
                }
                deserialfn_oid = aggform.aggdeserialfn;
            }
        }

        // Check that aggregate owner has permission to call component fns.
        // procTuple = SearchSysCache1(PROCOID, ...); aggOwner = proowner.
        let agg_owner = fetch_proc_owner(aggfnoid)?;
        if finalfn_oid != INVALID_OID {
            check_component_fn_acl(finalfn_oid, agg_owner)?;
        }
        if serialfn_oid != INVALID_OID {
            check_component_fn_acl(serialfn_oid, agg_owner)?;
        }
        if deserialfn_oid != INVALID_OID {
            check_component_fn_acl(deserialfn_oid, agg_owner)?;
        }

        // get_aggregate_argtypes(aggref, aggTransFnInputTypes)
        let (input_types, num_agg_trans_fn_args) = get_aggregate_argtypes(aggstate, idx, mcx)?;

        // numDirectArgs = list_length(aggref->aggdirectargs);
        let num_direct_args = {
            let aggs = aggstate.aggs.as_ref().unwrap();
            list_length(&aggs[idx].aggdirectargs) as i32
        };

        // numFinalArgs = aggfinalextra ? numAggTransFnArgs+1 : numDirectArgs+1;
        let num_final_args = if aggform.aggfinalextra {
            num_agg_trans_fn_args + 1
        } else {
            num_direct_args + 1
        };
        if let Some(p) = aggstate.peragg.as_mut() {
            p[aggno as usize].num_final_args = num_final_args;
        }

        // peragg->aggdirectargs = ExecInitExprList(aggref->aggdirectargs, parent);
        exec_init_direct_args(aggstate, aggno, idx, estate, mcx)?;

        // build finalfn expr + fmgr lookup if present.
        if finalfn_oid != INVALID_OID {
            setup_peragg_finalfn(
                aggstate,
                aggno,
                idx,
                finalfn_oid,
                num_final_args,
                aggtranstype,
                &input_types,
                mcx,
            )?;
        }

        // get info about the output value's datatype.
        // get_typlenbyval(aggref->aggtype, &resulttypeLen, &resulttypeByVal);
        set_peragg_result_typlenbyval(aggstate, aggno, aggtype)?;

        // Build the transition working state if not already done.
        let transno = aggtransno;
        let pertrans_done = aggstate
            .pertrans
            .as_ref()
            .map(|p| p[transno as usize].aggref.is_some())
            .unwrap_or(false);
        if !pertrans_done {
            // textInitVal = SysCacheGetAttr(AGGFNOID, aggTuple,
            //   Anum_pg_aggregate_agginitval, &initValueIsNull);
            let init_value_is_null = aggform.init_value_is_null;
            let init_value = if init_value_is_null {
                types_tuple::backend_access_common_heaptuple::Datum::null()
            } else {
                // GetAggInitVal(textInitVal, aggtranstype)
                GetAggInitVal(aggform.text_init_val, aggtranstype)?
            };

            if do_aggsplit_combine(aggsplit) {
                let transfn_oid = aggform.aggcombinefn;
                if transfn_oid == INVALID_OID {
                    return Err(types_error::PgError::error(
                        "combinefn not set for aggregate function",
                    ));
                }
                check_component_fn_acl(transfn_oid, agg_owner)?;

                // combinefn: numTransInputs = 1; two args of aggtranstype.
                let num_trans_inputs = 1;
                if let Some(p) = aggstate.pertrans.as_mut() {
                    p[transno as usize].num_trans_inputs = num_trans_inputs;
                }
                let combine_input_types = {
                    let mut v: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, 2)?;
                    v.push(aggtranstype);
                    v.push(aggtranstype);
                    v
                };
                build_pertrans_call(
                    aggstate,
                    estate,
                    transno,
                    idx,
                    transfn_oid,
                    aggtranstype,
                    serialfn_oid,
                    deserialfn_oid,
                    init_value,
                    init_value_is_null,
                    &combine_input_types,
                    2,
                    mcx,
                )?;

                // combine fn over INTERNAL states must not be strict.
                let strict = pertrans_transfn_strict(aggstate, transno);
                if strict && aggtranstype == INTERNALOID {
                    let typ = format_type_be(aggtranstype)?;
                    return Err(types_error::PgError::error(alloc::format!(
                        "combine function with transition type {typ} must not be declared STRICT"
                    )));
                }
            } else {
                let transfn_oid = aggform.aggtransfn;
                check_component_fn_acl(transfn_oid, agg_owner)?;

                let num_trans_inputs = if aggkind_is_ordered_set(aggform.aggkind) {
                    let aggs = aggstate.aggs.as_ref().unwrap();
                    list_length(&aggs[idx].args) as i32
                } else {
                    num_agg_trans_fn_args
                };
                if let Some(p) = aggstate.pertrans.as_mut() {
                    p[transno as usize].num_trans_inputs = num_trans_inputs;
                }

                build_pertrans_call(
                    aggstate,
                    estate,
                    transno,
                    idx,
                    transfn_oid,
                    aggtranstype,
                    serialfn_oid,
                    deserialfn_oid,
                    init_value,
                    init_value_is_null,
                    &input_types,
                    num_agg_trans_fn_args,
                    mcx,
                )?;

                // strict transfn + NULL initval ⇒ input type must be coercible.
                let strict = pertrans_transfn_strict(aggstate, transno);
                let initnull = aggstate
                    .pertrans
                    .as_ref()
                    .map(|p| p[transno as usize].init_value_is_null)
                    .unwrap_or(false);
                if strict && initnull {
                    let coercible = if num_agg_trans_fn_args <= num_direct_args {
                        false
                    } else {
                        is_binary_coercible(input_types[num_direct_args as usize], aggtranstype)?
                    };
                    if !coercible {
                        return Err(types_error::PgError::error(alloc::format!(
                            "aggregate {} needs to have compatible input type and transition type",
                            aggfnoid
                        )));
                    }
                }
            }
        } else {
            // pertrans->aggshared = true
            if let Some(p) = aggstate.pertrans.as_mut() {
                p[transno as usize].aggshared = true;
            }
        }
        // ReleaseSysCache(aggTuple) — handled inside fetch_agg_form's owner.
    }

    // Detect nested aggregates added during expression init.
    if numaggrefs != list_length(&aggstate.aggs) as i32 {
        return Err(types_error::PgError::error(
            "aggregate function calls cannot be nested",
        ));
    }

    Ok(())
}

/// The `for (phaseidx = 0; phaseidx < numphases; ...)` `ExecBuildAggTrans` loop.
/// The dohash/dosort selection per phase is in-crate; the `ExecBuildAggTrans`
/// compile and the `evaltrans_cache[0][0]` cache are execExpr owned.
fn build_phase_eval_trans<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let numphases = aggstate.numphases;
    let aggstrategy = aggstate.aggstrategy;

    for phaseidx in 0..numphases {
        // phase 0 doesn't necessarily exist.
        let exists = aggstate
            .phases
            .as_ref()
            .map(|p| p[phaseidx as usize].aggnode.is_some())
            .unwrap_or(false);
        if !exists {
            continue;
        }

        let phase_strategy = aggstate.phases.as_ref().unwrap()[phaseidx as usize].aggstrategy;
        let dohash;
        let dosort;

        if aggstrategy == AGG_MIXED && phaseidx == 1 {
            // Phase one (and only phase one) in a mixed agg does both.
            dohash = true;
            dosort = true;
        } else if aggstrategy == AGG_MIXED && phaseidx == 0 {
            // No transition function for an AGG_MIXED phase 0.
            continue;
        } else if phase_strategy == AGG_PLAIN || phase_strategy == AGG_SORTED {
            dohash = false;
            dosort = true;
        } else if phase_strategy == AGG_HASHED {
            dohash = true;
            dosort = false;
        } else {
            debug_assert!(false);
            dohash = false;
            dosort = false;
        }

        // phase->evaltrans = ExecBuildAggTrans(aggstate, phase, dosort, dohash,
        //                                      false);
        // phase->evaltrans_cache[0][0] = phase->evaltrans;
        exec_build_agg_trans(aggstate, phaseidx, dosort, dohash, false, estate, mcx)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Cross-subsystem step shims — owners not yet ported. Loud panics name the C
// callee and the owner; they collapse onto seam/sibling calls when the owners
// land. (No silent stubs: AGENTS mirror-PG-and-panic.)
// ---------------------------------------------------------------------------

/// pg_aggregate row, projected to the fields ExecInitAgg reads. C:
/// `aggTuple = SearchSysCache1(AGGFNOID, ...); aggform = GETSTRUCT(aggTuple)`
/// plus `SysCacheGetAttr(.., Anum_pg_aggregate_agginitval, ..)`.
struct AggForm<'mcx> {
    aggfinalfn: Oid,
    aggcombinefn: Oid,
    aggtransfn: Oid,
    aggserialfn: Oid,
    aggdeserialfn: Oid,
    aggfinalextra: bool,
    aggkind: i8,
    init_value_is_null: bool,
    text_init_val: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
}

/// C: `SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggfnoid))` +
/// `GETSTRUCT` + the agginitval `SysCacheGetAttr`. syscache (AGGFNOID, a
/// pg_aggregate projection) is not declared in this scaffold's syscache seam.
fn fetch_agg_form<'mcx>(_aggfnoid: Oid) -> PgResult<AggForm<'mcx>> {
    panic!(
        "backend-utils-cache-syscache (AGGFNOID): reading the pg_aggregate row \
         (aggfinalfn/aggcombinefn/aggtransfn/aggserialfn/aggdeserialfn/\
         aggfinalextra/aggkind + agginitval) needs an AGGFNOID syscache \
         projection seam; not declared"
    )
}

/// C: `aclresult = object_aclcheck(ProcedureRelationId, aggfnoid, GetUserId(),
/// ACL_EXECUTE); if (aclresult != OK) aclcheck_error(...);
/// InvokeFunctionExecuteHook(aggfnoid);` — `GetUserId()` has no seam in this
/// scaffold, so the current-user capability cannot be supplied yet.
fn check_aggregate_acl(_aggfnoid: Oid) -> PgResult<()> {
    panic!(
        "backend-utils-init-miscinit::GetUserId: object_aclcheck on the aggregate \
         needs the current user id (no GetUserId seam declared) and \
         InvokeFunctionExecuteHook; not ported"
    )
}

/// C: `procTuple = SearchSysCache1(PROCOID, ...); aggOwner = proowner;
/// ReleaseSysCache(procTuple);`.
fn fetch_proc_owner(_aggfnoid: Oid) -> PgResult<Oid> {
    panic!(
        "backend-utils-cache-syscache (PROCOID proowner): reading the aggregate \
         function's owner needs a PROCOID->proowner syscache projection seam; not \
         declared"
    )
}

/// C: `object_aclcheck(ProcedureRelationId, fnoid, aggOwner, ACL_EXECUTE);
/// if (!OK) aclcheck_error(.., get_func_name(fnoid));
/// InvokeFunctionExecuteHook(fnoid);` for a component function. `object_aclcheck`
/// / `aclcheck_error` take `types_acl` types (`AclMode`/`AclResult`) and an
/// `ObjectType`; `types_acl` is not a direct dependency of this crate (and the
/// aclchk seam does not re-export it), so the call cannot be marshaled here.
fn check_component_fn_acl(_fnoid: Oid, _agg_owner: Oid) -> PgResult<()> {
    panic!(
        "backend-catalog-aclchk::object_aclcheck/aclcheck_error: checking a \
         component function's ACL needs the types_acl AclMode/AclResult vocabulary \
         (not a dependency here) and get_func_name; not reachable"
    )
}

/// C: `get_aggregate_argtypes(aggref, aggTransFnInputTypes)` (parse_agg.c) —
/// the actual (nominal) input datatypes, returning the count. parse_agg is not
/// ported and has no seam in this scaffold.
fn get_aggregate_argtypes<'mcx>(
    _aggstate: &AggStateData<'mcx>,
    _idx: usize,
    _mcx: Mcx<'mcx>,
) -> PgResult<(PgVec<'mcx, Oid>, i32)> {
    panic!(
        "backend-parser-parse-agg::get_aggregate_argtypes: resolving the nominal \
         aggregate input datatypes needs parse_agg; not ported / no seam declared"
    )
}

/// C: `peragg->aggdirectargs = ExecInitExprList(aggref->aggdirectargs, parent)`
/// (execExpr.c). No ExecInitExprList seam is declared in this scaffold.
fn exec_init_direct_args<'mcx>(
    _aggstate: &mut AggStateData<'mcx>,
    _aggno: i32,
    _idx: usize,
    _estate: &mut EStateData<'mcx>,
    _mcx: Mcx<'mcx>,
) -> PgResult<()> {
    panic!(
        "backend-executor-execExpr::ExecInitExprList: compiling the aggregate's \
         direct-argument expressions needs the AggState parent and an \
         ExecInitExprList seam; not declared"
    )
}

/// C: `build_aggregate_finalfn_expr(...); fmgr_info(finalfn_oid, &finalfn);
/// fmgr_info_set_expr((Node *)finalfnexpr, &finalfn);` — parse_agg (expr build)
/// + fmgr (info, with expr). The expr build has no seam; fmgr_info_set_expr is
/// not declared either.
fn setup_peragg_finalfn<'mcx>(
    _aggstate: &mut AggStateData<'mcx>,
    _aggno: i32,
    _idx: usize,
    _finalfn_oid: Oid,
    _num_final_args: i32,
    _aggtranstype: Oid,
    _input_types: &[Oid],
    _mcx: Mcx<'mcx>,
) -> PgResult<()> {
    panic!(
        "backend-parser-parse-agg::build_aggregate_finalfn_expr + \
         backend-utils-fmgr-fmgr::fmgr_info/fmgr_info_set_expr: building the \
         finalfn expression and its FmgrInfo needs parse_agg and the fmgr \
         info-with-expr path; not ported / no seam declared"
    )
}

/// C: `get_typlenbyval(aggref->aggtype, &resulttypeLen, &resulttypeByVal)`
/// (lsyscache.c). Read through the installed `backend-utils-cache-lsyscache`
/// seam and store the result into the peragg slot.
fn set_peragg_result_typlenbyval(aggstate: &mut AggStateData<'_>, aggno: i32, aggtype: Oid) -> PgResult<()> {
    let (resulttype_len, resulttype_by_val) =
        backend_utils_cache_lsyscache_seams::get_typlenbyval::call(aggtype)?;
    if let Some(p) = aggstate.peragg.as_mut() {
        let peragg = &mut p[aggno as usize];
        peragg.resulttype_len = resulttype_len;
        peragg.resulttype_by_val = resulttype_by_val;
    }
    Ok(())
}

/// C: `peragg->aggref = aggref;` — clone the Aggref from `aggstate->aggs[idx]`
/// into the peragg slot. The owned `peragg.aggref` is `Option<PgBox<Aggref>>`
/// where C aliases the shared Aggref*; a faithful copy needs `Aggref::clone_in`,
/// not yet provided by types-nodes.
fn set_peragg_aggref<'mcx>(
    _aggstate: &mut AggStateData<'mcx>,
    _aggno: i32,
    _idx: usize,
    _mcx: Mcx<'mcx>,
) -> PgResult<()> {
    panic!(
        "types-nodes::Aggref::clone_in: peragg->aggref owns PgBox<Aggref> where C \
         aliases the shared Aggref*; needs Aggref::clone_in"
    )
}

/// C: `build_pertrans_for_aggref(pertrans, aggstate, estate, aggref, ...)` —
/// the in-crate per-trans build (node_lifecycle). It needs the Aggref by
/// reference, which the borrow checker cannot supply alongside `&mut aggstate`
/// without cloning the Aggref (types-nodes Aggref::clone_in, not yet provided).
#[allow(clippy::too_many_arguments)]
fn build_pertrans_call<'mcx>(
    _aggstate: &mut AggStateData<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _transno: i32,
    _idx: usize,
    _transfn_oid: Oid,
    _aggtranstype: Oid,
    _serialfn_oid: Oid,
    _deserialfn_oid: Oid,
    _init_value: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
    _init_value_is_null: bool,
    _input_types: &[Oid],
    _num_arguments: i32,
    _mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // The in-crate build_pertrans_for_aggref takes &mut pertrans, &mut aggstate,
    // and &aggref simultaneously; the owned model needs an Aggref copy
    // (Aggref::clone_in) to break the aliasing borrow. Defer to types-nodes.
    let _ = build_pertrans_for_aggref;
    panic!(
        "types-nodes::Aggref::clone_in: build_pertrans_for_aggref needs an Aggref \
         copy to break the &mut aggstate / &aggref aliasing borrow; needs \
         Aggref::clone_in"
    )
}

/// C: `pertrans->transfn.fn_strict` — the resolved transfn's strict flag, read
/// from the FmgrInfo build_pertrans_for_aggref filled in. Unavailable until the
/// per-trans build runs (which depends on the unported owners above).
fn pertrans_transfn_strict(_aggstate: &AggStateData<'_>, _transno: i32) -> bool {
    panic!(
        "backend-utils-fmgr-fmgr (FmgrInfo.fn_strict): pertrans->transfn.fn_strict \
         is set by build_pertrans_for_aggref's fmgr_info; unavailable until the \
         per-trans build (and its fmgr owner) land"
    )
}

/// C: `format_type_be(aggtranstype)` (format_type.c) — the type's display name
/// for the STRICT-combine error message.
fn format_type_be(_typeoid: Oid) -> PgResult<alloc::string::String> {
    panic!(
        "backend-utils-adt-format-type::format_type_be: the STRICT-combine error \
         needs the transition type's display name; no format_type_be seam reachable"
    )
}

/// C: `IsBinaryCoercible(input_type, aggtranstype)` (parse_coerce.c) — used by
/// the strict-initval compatibility check.
fn is_binary_coercible(_src: Oid, _target: Oid) -> PgResult<bool> {
    panic!(
        "backend-parser-parse-coerce::IsBinaryCoercible: the strict-initval \
         compatibility check needs binary-coercibility; not ported / no seam"
    )
}

/// C: `phase->evaltrans = ExecBuildAggTrans(aggstate, phase, dosort, dohash,
/// false); phase->evaltrans_cache[0][0] = phase->evaltrans;` (execExpr.c).
#[allow(clippy::too_many_arguments)]
fn exec_build_agg_trans<'mcx>(
    _aggstate: &mut AggStateData<'mcx>,
    _phaseidx: i32,
    _dosort: bool,
    _dohash: bool,
    _nullcheck: bool,
    _estate: &mut EStateData<'mcx>,
    _mcx: Mcx<'mcx>,
) -> PgResult<()> {
    panic!(
        "backend-executor-execExpr::ExecBuildAggTrans: compiling a phase's \
         transition-expression program needs execExpr's agg-trans builder and the \
         AggState parent; not ported / no seam declared"
    )
}
