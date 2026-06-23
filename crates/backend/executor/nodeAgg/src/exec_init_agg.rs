//! `ExecInitAgg` sub-module: building the `AggState` from the Agg plan node.
//!
//! Split out of [`crate::node_lifecycle`] because the C `ExecInitAgg`
//! (`nodeAgg.c` ~854 lines: catalog reads for every aggregate, per-trans /
//! per-agg setup, phase and grouping-set layout, hash-table and context
//! creation) is far larger than the rest of the lifecycle family combined.

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_core::primitive::Oid;
use types_error::PgResult;
use nodes::execnodes::PlanStateData;
use nodes::nodeagg::{
    do_aggsplit_combine, do_aggsplit_deserialize, do_aggsplit_serialize, do_aggsplit_skipfinal,
    Agg, AGG_HASHED, AGG_MIXED, AGG_PLAIN, AGG_SORTED,
};
use crate::aggstate::{
    AggStateData, AggStatePerAggData, AggStatePerGroupData, AggStatePerHashData,
    AggStatePerPhaseData, AggStatePerTransData,
};
use nodes::{AggStrategy, EStateData, Sort, TupleSlotKind};

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

/// `EXEC_FLAG_EXPLAIN_ONLY` (executor/executor.h).
const EXEC_FLAG_EXPLAIN_ONLY: i32 = 0x0001;
/// `EXEC_FLAG_REWIND` (executor/executor.h).
const EXEC_FLAG_REWIND: i32 = 0x0004;
/// `EXEC_FLAG_BACKWARD` (executor/executor.h).
const EXEC_FLAG_BACKWARD: i32 = 0x0008;
/// `EXEC_FLAG_MARK` (executor/executor.h).
const EXEC_FLAG_MARK: i32 = 0x0010;

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

/// Erase an owned `AggStateData` into the central `PlanStateNode::Agg` carrier
/// (`PgBox<dyn AggStateLive>`). `AggStateData` lives ABOVE `types-nodes`, so the
/// enum carries it type-erased; this is the same `into_raw`/`from_raw` unsize
/// the AM-opaque carriers use (the `allocator_api2` `PgBox` does not auto-coerce
/// unsized types on stable). The concrete type is recovered via the tag-checked
/// `downcast_agg_state_*` helpers.
pub fn erase_agg_state<'mcx>(
    boxed: PgBox<'mcx, AggStateData<'mcx>>,
) -> PgBox<'mcx, dyn nodes::aggstate_carrier::AggStateLive<'mcx> + 'mcx> {
    let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
    // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast only
    // attaches the `dyn AggStateLive` vtable (the established erase pattern).
    unsafe {
        PgBox::from_raw_in(
            ptr as *mut (dyn nodes::aggstate_carrier::AggStateLive<'mcx> + 'mcx),
            alloc,
        )
    }
}

/// `ExecInitAgg(node, estate, eflags)` — build the `AggState` from the Agg
/// plan node: catalog reads for every aggregate, per-trans/per-agg setup,
/// phase and grouping-set layout, hash-table and context creation.
pub fn ExecInitAgg<'mcx>(
    node: &'mcx Agg<'mcx>,
    plan_node: &'mcx nodes::nodes::Node<'mcx>,
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
    // C: aggstate->ss.ps.plan = (Plan *) node — the plan back-link aliases the
    // shared, read-only plan tree (the wrapping `Node::Agg`). The result
    // projection (ExecAssignProjectionInfo -> ExecBuildProjectionInfo) reads its
    // targetlist off this back-link via `Node::plan_head().targetlist`.
    debug_assert!(plan_node.is_agg());
    aggstate.ss.ps.plan = Some(plan_node);
    // C: aggstate->ss.ps.ExecProcNode = ExecAgg;
    aggstate.ss.ps.ExecProcNode = Some(crate::node_lifecycle::exec_agg_node);
    aggstate.ss.ps.ExecProcNodeReal = Some(crate::node_lifecycle::exec_agg_node);

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
    // The per-grouping-set ExprContexts are created by ExecAssignExprContext /
    // CreateExprContext below; in the owned model each is an EcxtId into the
    // EState ExprContext pool (matching ps_ExprContext / AggStateData.aggcontexts).
    let mut aggcontexts: PgVec<'mcx, nodes::EcxtId> =
        vec_with_capacity_in(mcx, num_grouping_sets as usize)?;
    let _ = &mut aggcontexts;

    // Create expression contexts. We need three or more: per-input-tuple
    // processing, per-output-tuple processing, one for all the hashtables, and
    // one for each grouping set.
    //
    // ExecAssignExprContext(estate, &aggstate->ss.ps);
    // aggstate->tmpcontext = aggstate->ss.ps.ps_ExprContext;
    execUtils_seams::exec_assign_expr_context::call(
        estate,
        &mut aggstate.ss.ps,
    )?;
    // aggstate->tmpcontext = aggstate->ss.ps.ps_ExprContext;
    // Both are now EcxtId into the EState pool (P0 storage-model reconcile), so
    // the alias is a plain id copy, faithful to the C `ExprContext *` alias.
    aggstate.tmpcontext = aggstate.ss.ps.ps_ExprContext;
    // The remainder of ExecInitAgg now runs against landed owners (the
    // #324/#165 keystone): the PlanStateNode::Agg carrier + AggStateLive bridge,
    // the AGGFNOID full projection (agg_form_by_oid), the parse_agg / fmgr /
    // execExpr / execTuples / execGrouping / aclchk seams, and the
    // exec_build_agg_trans seam (the AggState rides erased). The remaining
    // runtime-only residual is the live-AggState back-reference through
    // fcinfo->context (F4: AggCheckCallContext et al.) — built tag-only here and
    // seam-panicked at invocation time.
    {
        for _i in 0..num_grouping_sets {
            // ExecAssignExprContext(estate, &aggstate->ss.ps);
            // aggstate->aggcontexts[i] = aggstate->ss.ps.ps_ExprContext;
            execUtils_seams::exec_assign_expr_context::call(
                estate,
                &mut aggstate.ss.ps,
            )?;
            aggcontexts.push(
                aggstate
                    .ss
                    .ps
                    .ps_ExprContext
                    .expect("ExecAssignExprContext set ps_ExprContext"),
            );
        }
        aggstate.aggcontexts = Some(aggcontexts);

        if use_hashing {
            // hash_create_memory(aggstate); — assigns aggstate->hashcontext from
            // CreateWorkExprContext(estate) (an EcxtId).
            hash_create_memory(&mut aggstate, estate)?;
        }

        // The per-output-tuple context.
        execUtils_seams::exec_assign_expr_context::call(
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
        let child = execProcnode_seams::exec_init_node::call(
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
        execUtils_seams::exec_create_scan_slot_from_outer_plan::call(
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
                Some(execTuples_seams::exec_init_extra_tuple_slot::call(
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
        execTuples_seams::exec_init_result_tuple_slot_tl::call(
            &mut aggstate.ss.ps,
            estate,
            TupleSlotKind::Virtual,
        )?;
        // ExecAssignProjectionInfo(&aggstate->ss.ps, NULL);
        // C builds the result projection with parent = (PlanState *) aggstate,
        // so its targetlist Aggrefs are discovered into aggstate->aggs. In the
        // owned model the projection's ExprState collects them on its
        // `found_aggs` channel; drain it here (matching the C discovery point).
        execUtils_seams::exec_assign_projection_info::call(
            &mut aggstate.ss.ps,
            estate,
            None,
        )?;
        if let Some(proj) = aggstate.ss.ps.ps_ProjInfo.as_mut() {
            // Reborrow-free drain: pull the found list out of the projection
            // state, then push into aggstate (disjoint fields, but the borrow
            // checker needs the list moved out first).
            let found = proj.pi_state.found_aggs.take();
            if let Some(found) = found {
                let mut tmp = nodes::execexpr::ExprState::default();
                tmp.found_aggs = Some(found);
                drain_found_aggs(&mut aggstate, &mut tmp, mcx)?;
            }
        }

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
            let gc: PgVec<'mcx, PgBox<'mcx, nodes::Bitmapset<'mcx>>> =
                vec_with_capacity_in(mcx, num_hashes as usize)?;
            phases[0].grouped_cols = Some(gc);
        }

        // The per-phase loop (grouping-set arithmetic in-crate; the bms
        // accumulation into all_grouped_cols + the execTuplesMatchPrepare
        // eqfunction builds reach the bitmapset / execGrouping seams). The C
        // local `Bitmapset *all_grouped_cols` is threaded explicitly here.
        let mut all_grouped_cols: Option<PgBox<'mcx, nodes::Bitmapset<'mcx>>> = None;
        build_phases(&mut aggstate, node, mcx, estate, &mut all_grouped_cols)?;

        // Convert all_grouped_cols to a descending-order list (bms_next_member +
        // lcons_int).
        convert_all_grouped_cols(&mut aggstate, all_grouped_cols.as_deref(), mcx)?;

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
            mcx,
        )?;

        // Hashing can only appear in the initial phase.
        if use_hashing {
            // hash_spill_rslot = ExecInitExtraTupleSlot(estate, scanDesc, MinimalTuple);
            // hash_spill_wslot = ExecInitExtraTupleSlot(estate, scanDesc, Virtual);
            let scan_desc_r = scan_tuple_desc(&aggstate, estate)?;
            aggstate.hash_spill_rslot =
                Some(execTuples_seams::exec_init_extra_tuple_slot::call(
                    estate,
                    scan_desc_r,
                    TupleSlotKind::MinimalTuple,
                )?);
            let scan_desc_w = scan_tuple_desc(&aggstate, estate)?;
            aggstate.hash_spill_wslot =
                Some(execTuples_seams::exec_init_extra_tuple_slot::call(
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
            find_hash_columns(&mut aggstate, estate, mcx)?;

            // Skip massive memory allocation if just doing EXPLAIN.
            if eflags & EXEC_FLAG_EXPLAIN_ONLY == 0 {
                crate::hash_grouping::build_hash_tables(&mut aggstate, estate)?;
            }

            aggstate.table_filled = false;
            // Initialize to 1, meaning nothing spilled yet.
            aggstate.hash_batches_used = 1;

            // Owned-model transient (no C analogue): one entry-index slot per
            // grouping set, filled by lookup_hash_entries and drained by
            // store_hash_pergroups_back around each tuple's advance_aggregates.
            aggstate.hash_cur_entry_index =
                alloc::vec![None; aggstate.num_hashes.max(0) as usize];
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
            execTuples_seams::exec_get_result_slot_ops::call(child_ps)
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
pub(crate) fn scan_tuple_desc<'mcx>(
    aggstate: &AggStateData<'mcx>,
    estate: &EStateData<'mcx>,
) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>> {
    // scanDesc = aggstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor; — read
    // through the installed execTuples seam (the slot pool owns the descriptor).
    let mcx = estate.es_query_cxt;
    execTuples_seams::exec_scan_slot_descriptor::call(
        mcx,
        &aggstate.ss,
        estate,
    )
}

/// Drain the `found_aggs` channel an `ExprState` accumulated during compilation
/// into `aggstate->aggs` (the executor satellite) and `aggstate->aggs_prim`
/// (the expression-tree originals kept for the parse_agg helpers). C does
/// `aggstate->aggs = lappend(aggstate->aggs, astate)` inside `ExecInitExprRec`;
/// the owned model collects on the `ExprState` and drains here (the
/// planner-set `aggno`/`aggtransno` make the order divergence inert). See
/// [`nodes::execexpr::ExprState::found_aggs`].
fn drain_found_aggs<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    state: &mut nodes::execexpr::ExprState<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let found = match state.found_aggs.take() {
        Some(f) if !f.is_empty() => f,
        _ => return Ok(()),
    };
    if aggstate.aggs.is_none() {
        aggstate.aggs = Some(vec_with_capacity_in(mcx, found.len())?);
        aggstate.aggs_prim = Some(vec_with_capacity_in(mcx, found.len())?);
    }
    for prim in found.into_iter() {
        let exec = nodes::nodeagg::Aggref::from_primnode(&prim, mcx)?;
        aggstate
            .aggs
            .as_mut()
            .expect("aggs initialized")
            .push(alloc_in(mcx, exec)?);
        aggstate
            .aggs_prim
            .as_mut()
            .expect("aggs_prim initialized")
            .push(prim);
    }
    Ok(())
}

/// C: `aggstate->ss.ps.qual = ExecInitQual(node->plan.qual, (PlanState *)aggstate);`
/// — execExpr also pushes any Aggrefs found in the qual into `aggstate->aggs`.
/// The qual compile runs through the execExpr seam with the head-only parent;
/// the discovered Aggrefs ride the ExprState's `found_aggs` channel and are
/// drained into `aggstate->aggs` here.
fn exec_init_qual_for_agg<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    node: &Agg<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<PgBox<'mcx, nodes::execexpr::ExprState<'mcx>>>> {
    let mcx = estate.es_query_cxt;
    // node->plan.qual is the implicitly-ANDed qual list.
    let qual_slice: Option<PgVec<'mcx, nodes::primnodes::Expr>> =
        match node.plan.qual.as_ref() {
            Some(q) => {
                let mut v = vec_with_capacity_in(mcx, q.len())?;
                for e in q.iter() {
                    v.push(e.clone_in(mcx)?);
                }
                Some(v)
            }
            None => None,
        };
    let mut qual = execExpr_seams::exec_init_qual::call(
        qual_slice.as_deref(),
        &mut aggstate.ss.ps,
        estate,
    )?;
    if let Some(state) = qual.as_mut() {
        drain_found_aggs(aggstate, state, mcx)?;
    }
    Ok(qual)
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
    all_grouped_cols: &mut Option<PgBox<'mcx, nodes::Bitmapset<'mcx>>>,
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
            set_phase_grouped_cols_for_hash(aggstate, i, node, phaseidx, mcx, all_grouped_cols)?;
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
                    let gc: PgVec<'mcx, PgBox<'mcx, nodes::Bitmapset<'mcx>>> =
                        vec_with_capacity_in(mcx, num_sets as usize)?;
                    phases[p].grouped_cols = Some(gc);
                }
                set_phase_grouped_cols_for_sets(
                    aggstate, phase, node, phaseidx, mcx, all_grouped_cols,
                )?;
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
                        Option<PgBox<'mcx, nodes::execexpr::ExprState<'mcx>>>,
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
            // C's castNode is null-safe: a chained Agg built for a hashed or
            // first-sort rollup has a NULL outerPlan (no Sort), so sortnode
            // stays NULL. Mirror that — only AGG_SORTED chained phases carry a
            // Sort. (Assert(phase <= 1 || sortnode) still holds: phases beyond
            // the first sorted phase always have a Sort.)
            if is_chained {
                phases[p].sortnode = clone_phase_sortnode(node, phaseidx, mcx)?;
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
fn clone_agg_shallow<'mcx>(node: &Agg<'mcx>, mcx: Mcx<'mcx>) -> PgResult<PgBox<'mcx, Agg<'mcx>>> {
    // C aliases the shared `Agg *`; the owned `aggnode` field owns a deep copy.
    alloc_in(mcx, node.clone_in(mcx)?)
}

/// The chained-Agg lookup `list_nth_node(Agg, node->chain, phaseidx-1)` (or
/// `node` for phaseidx 0), cloned for the owned `aggnode` field.
fn clone_phase_agg<'mcx>(
    node: &Agg<'mcx>,
    phaseidx: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<PgBox<'mcx, Agg<'mcx>>> {
    let aggnode: &Agg<'mcx> = if phaseidx > 0 {
        &node.chain.as_ref().expect("chain present for phaseidx>0")[(phaseidx - 1) as usize]
    } else {
        node
    };
    alloc_in(mcx, aggnode.clone_in(mcx)?)
}

/// `castNode(Sort, outerPlan(aggnode))` for a chained Agg, cloned into the
/// owned `sortnode` field. The chained `aggnode`'s outer plan is a `Sort`.
fn clone_phase_sortnode<'mcx>(
    node: &Agg<'mcx>,
    phaseidx: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgBox<'mcx, Sort<'mcx>>>> {
    debug_assert!(phaseidx > 0, "sortnode only for chained phases");
    let aggnode = &node.chain.as_ref().expect("chain present")[(phaseidx - 1) as usize];
    // outerPlan(aggnode) is the chained Sort node — but is genuinely NULL for a
    // chained Agg built over a hashed or first-sort rollup (create_groupingsets_plan
    // passes sort_plan = NULL there). castNode(Sort, NULL) == NULL in C.
    let outer = match aggnode.plan.lefttree.as_ref() {
        Some(o) => o,
        None => return Ok(None),
    };
    let sort: &Sort<'mcx> = match outer.node_tag() {
        nodes::nodes::ntag::T_Sort => outer.expect_sort(),
        other => panic!("castNode(Sort, outerPlan(aggnode)) failed: {other:?}"),
    };
    Ok(Some(alloc_in(mcx, sort.clone_in(mcx)?)?))
}

/// Build a `Bitmapset` of `grpColIdx[0..numCols]` for an Agg node.
fn grouped_cols_bms<'mcx>(
    grp_col_idx: Option<&PgVec<'mcx, types_core::primitive::AttrNumber>>,
    num_cols: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgBox<'mcx, nodes::Bitmapset<'mcx>>>> {
    let mut cols: Option<PgBox<'mcx, nodes::Bitmapset<'mcx>>> = None;
    if let Some(idx) = grp_col_idx {
        for j in 0..num_cols as usize {
            cols = Some(nodes_core_seams::bms_add_member::call(
                mcx,
                cols,
                idx[j] as i32,
            )?);
        }
    }
    Ok(cols)
}

/// `grouped_cols[i] = bms of aggnode->grpColIdx[0..numCols]; all_grouped_cols =
/// bms_add_members(all_grouped_cols, cols)` for the hash phase.
fn set_phase_grouped_cols_for_hash<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    i: i32,
    node: &Agg<'mcx>,
    phaseidx: i32,
    mcx: Mcx<'mcx>,
    all_grouped_cols: &mut Option<PgBox<'mcx, nodes::Bitmapset<'mcx>>>,
) -> PgResult<()> {
    let aggnode: &Agg<'mcx> = if phaseidx > 0 {
        &node.chain.as_ref().expect("chain present")[(phaseidx - 1) as usize]
    } else {
        node
    };
    let cols = grouped_cols_bms(aggnode.grp_col_idx.as_ref(), aggnode.num_cols, mcx)?;
    // all_grouped_cols = bms_add_members(all_grouped_cols, cols);
    let merged = nodes_core_seams::bms_add_members::call(
        mcx,
        all_grouped_cols.take(),
        cols.as_deref(),
    )?;
    *all_grouped_cols = merged;
    // phasedata->grouped_cols[i] = cols;  (only when non-empty)
    if let Some(cols) = cols {
        let phases = aggstate.phases.as_mut().unwrap();
        if let Some(gc) = phases[0].grouped_cols.as_mut() {
            // grouped_cols is parallel to the hash index `i`; extend then set.
            while (gc.len() as i32) <= i {
                gc.push(alloc_in(mcx, nodes::Bitmapset::empty(mcx)?)?);
            }
            gc[i as usize] = cols;
        }
    }
    Ok(())
}

/// The per-grouping-set `grouped_cols` build for a sorted/plain phase
/// (`for (j) cols = bms_add_member(cols, grpColIdx[j])`) plus the
/// `all_grouped_cols = bms_add_members(.., grouped_cols[0])` accumulation.
fn set_phase_grouped_cols_for_sets<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    phase: i32,
    node: &Agg<'mcx>,
    phaseidx: i32,
    mcx: Mcx<'mcx>,
    all_grouped_cols: &mut Option<PgBox<'mcx, nodes::Bitmapset<'mcx>>>,
) -> PgResult<()> {
    let aggnode: &Agg<'mcx> = if phaseidx > 0 {
        &node.chain.as_ref().expect("chain present")[(phaseidx - 1) as usize]
    } else {
        node
    };
    let p = phase as usize;
    // The C builds one grouped_cols bms per grouping set. The set's length is
    // `current_length = list_length(lfirst(l))` over `aggnode->groupingSets`
    // (NOT a pre-filled gset_lengths, which the C populates from this loop);
    // the grpColIdx prefix of that length is the set's columns. We also write
    // the computed length back into gset_lengths[k] (C: gset_lengths[i] =
    // current_length), since the retrieve path reads gset_lengths directly.
    let numsets = aggstate.phases.as_ref().unwrap()[p].numsets;
    let mut first_cols: Option<PgBox<'mcx, nodes::Bitmapset<'mcx>>> = None;
    for k in 0..numsets as usize {
        // current_length = list_length(lfirst(l)) over aggnode->groupingSets[k].
        let length = aggnode
            .grouping_sets
            .as_ref()
            .and_then(|gs| gs.get(k))
            .map(|set| set.len() as i32)
            .unwrap_or(0);
        // gset_lengths[i] = current_length;
        {
            let phases = aggstate.phases.as_mut().unwrap();
            if let Some(gl) = phases[p].gset_lengths.as_mut() {
                if let Some(slot) = gl.get_mut(k) {
                    *slot = length;
                }
            }
        }
        let cols = grouped_cols_bms(aggnode.grp_col_idx.as_ref(), length, mcx)?;
        let cols = match cols { Some(c) => c, None => alloc_in(mcx, nodes::Bitmapset::empty(mcx)?)? };
        if k == 0 {
            // Snapshot grouped_cols[0] for the all_grouped_cols union below.
            first_cols = Some(alloc_in(mcx, cols.clone_in(mcx)?)?);
        }
        let phases = aggstate.phases.as_mut().unwrap();
        if let Some(gc) = phases[p].grouped_cols.as_mut() {
            while gc.len() <= k {
                gc.push(alloc_in(mcx, nodes::Bitmapset::empty(mcx)?)?);
            }
            gc[k] = cols;
        }
    }
    // all_grouped_cols = bms_add_members(all_grouped_cols, grouped_cols[0]);
    let merged = nodes_core_seams::bms_add_members::call(
        mcx,
        all_grouped_cols.take(),
        first_cols.as_deref(),
    )?;
    *all_grouped_cols = merged;
    Ok(())
}

/// One AGG_SORTED phase's comparator build:
/// `eqfunctions[length-1] = execTuplesMatchPrepare(scanDesc, length, grpColIdx,
/// grpOperators, grpCollations, (PlanState *)aggstate)`.
fn build_phase_eqfunction<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    phase: i32,
    length: i32,
    node: &Agg<'mcx>,
    phaseidx: i32,
    estate: &mut EStateData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let aggnode: &Agg<'mcx> = if phaseidx > 0 {
        &node.chain.as_ref().expect("chain present")[(phaseidx - 1) as usize]
    } else {
        node
    };
    // scanDesc = aggstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor
    let scan_desc = scan_tuple_desc(aggstate, estate)?;
    let key_col_idx: PgVec<'mcx, types_core::primitive::AttrNumber> = {
        let mut v = vec_with_capacity_in(mcx, length as usize)?;
        if let Some(idx) = aggnode.grp_col_idx.as_ref() {
            for j in 0..length as usize {
                v.push(idx[j]);
            }
        }
        v
    };
    let ops: PgVec<'mcx, Oid> = {
        let mut v = vec_with_capacity_in(mcx, length as usize)?;
        if let Some(o) = aggnode.grp_operators.as_ref() {
            for j in 0..length as usize {
                v.push(o[j]);
            }
        }
        v
    };
    let colls: PgVec<'mcx, Oid> = {
        let mut v = vec_with_capacity_in(mcx, length as usize)?;
        if let Some(c) = aggnode.grp_collations.as_ref() {
            for j in 0..length as usize {
                v.push(c[j]);
            }
        }
        v
    };
    let eqfn = execGrouping_seams::exec_tuples_match_prepare::call(
        scan_desc,
        length,
        &key_col_idx,
        &ops,
        &colls,
        &mut aggstate.ss.ps,
        estate,
    )?;
    let phases = aggstate.phases.as_mut().unwrap();
    if let Some(eqf) = phases[phase as usize].eqfunctions.as_mut() {
        eqf[(length - 1) as usize] = eqfn;
    }
    Ok(())
}

/// `i = -1; while ((i = bms_next_member(all_grouped_cols, i)) >= 0)
/// aggstate->all_grouped_cols = lcons_int(i, aggstate->all_grouped_cols);` —
/// build the descending-order grouped-column list.
fn convert_all_grouped_cols<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    all_grouped_cols: Option<&nodes::Bitmapset<'mcx>>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // lcons_int prepends, so iterating ascending yields a descending-order list.
    let mut list: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, 0)?;
    let mut i = -1i32;
    loop {
        i = nodes_core_seams::bms_next_member::call(all_grouped_cols, i);
        if i < 0 {
            break;
        }
        list.insert(0, i);
    }
    aggstate.all_grouped_cols = Some(list);
    Ok(())
}

/// `econtext->ecxt_aggvalues = palloc0(sizeof(Datum) * numaggs);
/// econtext->ecxt_aggnulls = palloc0(sizeof(bool) * numaggs);` on the
/// per-output-tuple ExprContext (`aggstate->ss.ps.ps_ExprContext`).
fn alloc_agg_result_storage<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    numaggs: i32,
    _mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let ecxt_id = aggstate
        .ss
        .ps
        .ps_ExprContext
        .expect("alloc_agg_result_storage: ps_ExprContext not set");
    let ecxt = estate.ecxt_mut(ecxt_id);
    ecxt.ecxt_aggvalues.clear();
    ecxt.ecxt_aggnulls.clear();
    for _ in 0..numaggs {
        ecxt.ecxt_aggvalues
            .push(types_tuple::heaptuple::Datum::null());
        ecxt.ecxt_aggnulls.push(false);
    }
    Ok(())
}

/// Split the single `all_pergroups` buffer into the owned `pergroups` (the
/// first `numGroupingSets` entries, when not AGG_HASHED) and `hash_pergroup`
/// (the tail) regions, mirroring the C pointer aliasing. The owned model stores
/// the whole buffer in `all_pergroups` and splits owned copies of the two
/// regions into `pergroups` / `hash_pergroup`.
fn assign_pergroup_regions<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    mut all_pergroups: PgVec<'mcx, Option<PgVec<'mcx, AggStatePerGroupData<'mcx>>>>,
    _node: &Agg<'mcx>,
    num_grouping_sets: i32,
    pergroup_offset: usize,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // The C aliases:  pergroups = all_pergroups (head of numGroupingSets, when
    // not AGG_HASHED), hash_pergroup = all_pergroups + pergroup_offset — the SAME
    // per-group arrays under different pointers. The transition interpreter
    // (`EEOP_AGG_PLAIN_TRANS_*`) mutates `all_pergroups[setoff][transno]`, and
    // initialize/finalize on the sorted path read the SAME head entries; so
    // `all_pergroups` is kept as the single source of truth and the sorted-path
    // init/finalize/rescan operate on it directly (see sorted_grouping.rs).
    // `aggstate.pergroups` is therefore left NULL (the head alias is reached
    // through `all_pergroups`); only the hash region is copied out into
    // `hash_pergroup` for the (separate, hashagg-gated) hash path.
    let _ = num_grouping_sets;
    let total = all_pergroups.len();
    // Build hash_pergroup from the tail [pergroup_offset..]. For the non-hashed
    // sorted path pergroup_offset == total, so this is empty (no tail).
    let mut hash_pergroup: PgVec<'mcx, Option<PgVec<'mcx, AggStatePerGroupData<'mcx>>>> =
        vec_with_capacity_in(mcx, total.saturating_sub(pergroup_offset))?;
    for k in pergroup_offset..total {
        // Clone the hash tail (the hash path mutates its own copy). The head
        // stays in all_pergroups untouched.
        hash_pergroup.push(all_pergroups[k].clone());
    }
    aggstate.pergroups = None;
    aggstate.hash_pergroup = Some(hash_pergroup);
    aggstate.all_pergroups = Some(all_pergroups);
    Ok(())
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
        let aggform = fetch_agg_form(mcx, aggfnoid)?;

        // object_aclcheck(ProcedureRelationId, aggfnoid, GetUserId(), ACL_EXECUTE);
        // InvokeFunctionExecuteHook(aggfnoid);
        check_aggregate_acl(mcx, aggfnoid)?;

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
        let agg_owner = fetch_proc_owner(mcx, aggfnoid)?;
        if finalfn_oid != INVALID_OID {
            check_component_fn_acl(mcx, finalfn_oid, agg_owner)?;
        }
        if serialfn_oid != INVALID_OID {
            check_component_fn_acl(mcx, serialfn_oid, agg_owner)?;
        }
        if deserialfn_oid != INVALID_OID {
            check_component_fn_acl(mcx, deserialfn_oid, agg_owner)?;
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
            let init_value_is_null = aggform.agginitval.is_none();
            let init_value = match aggform.agginitval.as_deref() {
                None => types_tuple::heaptuple::Datum::null(),
                // GetAggInitVal(textInitVal, aggtranstype)
                Some(s) => GetAggInitVal(mcx, s, aggtranstype)?,
            };

            if do_aggsplit_combine(aggsplit) {
                let transfn_oid = aggform.aggcombinefn;
                if transfn_oid == INVALID_OID {
                    return Err(types_error::PgError::error(
                        "combinefn not set for aggregate function",
                    ));
                }
                check_component_fn_acl(mcx, transfn_oid, agg_owner)?;

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
                check_component_fn_acl(mcx, transfn_oid, agg_owner)?;

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

/// C: `aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggfnoid));
/// aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple)` plus the agginitval
/// `SysCacheGetAttr`. The full pg_aggregate projection lands as the installed
/// `agg_form_by_oid` syscache seam returning [`AggFormData`] (every support-fn
/// Oid + the transition columns + `aggfinalextra`/`aggkind` + the already-read
/// `agginitval`/`aggminitval` texts).
fn fetch_agg_form<'mcx>(
    mcx: Mcx<'mcx>,
    aggfnoid: Oid,
) -> PgResult<types_catalog::pg_aggregate::AggFormData> {
    syscache_seams::agg_form_by_oid::call(mcx, aggfnoid)?.ok_or_else(|| {
        // C: elog(ERROR, "cache lookup failed for aggregate %u", aggref->aggfnoid)
        types_error::PgError::error(alloc::format!(
            "cache lookup failed for aggregate {aggfnoid}"
        ))
    })
}

/// C: `aclresult = object_aclcheck(ProcedureRelationId, aggref->aggfnoid,
/// GetUserId(), ACL_EXECUTE); if (aclresult != ACLCHECK_OK)
/// aclcheck_error(aclresult, OBJECT_AGGREGATE, get_func_name(aggref->aggfnoid));
/// InvokeFunctionExecuteHook(aggref->aggfnoid);`
fn check_aggregate_acl<'mcx>(mcx: Mcx<'mcx>, aggfnoid: Oid) -> PgResult<()> {
    let user = miscinit_seams::get_user_id::call();
    let aclresult = aclchk_seams::object_aclcheck::call(
        types_core::catalog::PROCEDURE_RELATION_ID,
        aggfnoid,
        user,
        types_acl::acl::ACL_EXECUTE,
    )?;
    if aclresult != types_acl::acl::AclResult::AclcheckOk {
        let name = lsyscache_seams::get_func_name::call(mcx, aggfnoid)?
            .map(|s| s.as_str().into());
        aclchk_seams::aclcheck_error::call(
            aclresult,
            nodes::parsenodes::ObjectType::Aggregate,
            name,
        )?;
    }
    objectaccess_seams::invoke_function_execute_hook::call(aggfnoid)?;
    Ok(())
}

/// C: `procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(aggref->aggfnoid));
/// aggOwner = ((Form_pg_proc) GETSTRUCT(procTuple))->proowner;
/// ReleaseSysCache(procTuple);` — read via the installed `lookup_proc` PROCOID
/// projection (which carries `proowner`).
fn fetch_proc_owner<'mcx>(mcx: Mcx<'mcx>, aggfnoid: Oid) -> PgResult<Oid> {
    let proc = syscache_seams::lookup_proc::call(mcx, aggfnoid)?
        .ok_or_else(|| {
            types_error::PgError::error(alloc::format!(
                "cache lookup failed for function {aggfnoid}"
            ))
        })?;
    Ok(proc.proowner)
}

/// C: `aclresult = object_aclcheck(ProcedureRelationId, fnoid, aggOwner,
/// ACL_EXECUTE); if (aclresult != ACLCHECK_OK) aclcheck_error(aclresult,
/// OBJECT_FUNCTION, get_func_name(fnoid)); InvokeFunctionExecuteHook(fnoid);`
/// for a transition/serial/deserial/final component function.
fn check_component_fn_acl<'mcx>(mcx: Mcx<'mcx>, fnoid: Oid, agg_owner: Oid) -> PgResult<()> {
    let aclresult = aclchk_seams::object_aclcheck::call(
        types_core::catalog::PROCEDURE_RELATION_ID,
        fnoid,
        agg_owner,
        types_acl::acl::ACL_EXECUTE,
    )?;
    if aclresult != types_acl::acl::AclResult::AclcheckOk {
        let name = lsyscache_seams::get_func_name::call(mcx, fnoid)?
            .map(|s| s.as_str().into());
        aclchk_seams::aclcheck_error::call(
            aclresult,
            nodes::parsenodes::ObjectType::Function,
            name,
        )?;
    }
    objectaccess_seams::invoke_function_execute_hook::call(fnoid)?;
    Ok(())
}

/// C: `get_aggregate_argtypes(aggref, aggTransFnInputTypes)` (parse_agg.c) —
/// the actual (nominal) input datatypes, returning the count. Reached through
/// the installed parse_agg seam; reads the expression-tree `primnodes::Aggref`
/// kept in `aggstate.aggs_prim`.
fn get_aggregate_argtypes<'mcx>(
    aggstate: &AggStateData<'mcx>,
    idx: usize,
    mcx: Mcx<'mcx>,
) -> PgResult<(PgVec<'mcx, Oid>, i32)> {
    let prim = &aggstate
        .aggs_prim
        .as_ref()
        .expect("get_aggregate_argtypes: aggs_prim is NULL")[idx];
    let input_types = parse_agg_seams::get_aggregate_argtypes::call(mcx, prim)?;
    let n = input_types.len() as i32;
    Ok((input_types, n))
}

/// C: `peragg->aggdirectargs = ExecInitExprList(aggref->aggdirectargs, parent)`
/// (execExpr.c). The direct-argument exprs are compiled through the execExpr
/// seam with the head-only parent; any Aggrefs they contain ride the
/// `found_aggs` channel and are drained into `aggstate->aggs`.
fn exec_init_direct_args<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    aggno: i32,
    idx: usize,
    estate: &mut EStateData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // ExecInitExprList(aggref->aggdirectargs, parent): clone the direct-arg
    // exprs into owned Exprs first (so the `aggs_prim` borrow ends before the
    // mutable `aggstate` calls below), then build the Option<&Expr> list.
    let owned_args: PgVec<'mcx, nodes::primnodes::Expr<'mcx>> = {
        let prim = &aggstate
            .aggs_prim
            .as_ref()
            .expect("exec_init_direct_args: aggs_prim is NULL")[idx];
        let mut v = vec_with_capacity_in(mcx, prim.aggdirectargs.len())?;
        for e in prim.aggdirectargs.iter() {
            v.push(e.clone_in(mcx)?);
        }
        v
    };
    let nodes: PgVec<'mcx, Option<&nodes::primnodes::Expr<'mcx>>> = {
        let mut v = vec_with_capacity_in(mcx, owned_args.len())?;
        for e in owned_args.iter() {
            v.push(Some(e));
        }
        v
    };
    let mut states = execExpr_seams::exec_init_expr_list::call(
        &nodes,
        &mut aggstate.ss.ps,
        estate,
    )?;
    drop(nodes);
    drop(owned_args);
    // Drain any Aggrefs discovered in the direct-arg expressions, then store the
    // compiled states on the peragg.
    for st in states.iter_mut() {
        if let Some(st) = st.as_mut() {
            drain_found_aggs(aggstate, st, mcx)?;
        }
    }
    // peragg->aggdirectargs = states;  (Option<&Expr> Nones can't occur here)
    let mut boxed: PgVec<'mcx, PgBox<'mcx, nodes::execexpr::ExprState<'mcx>>> =
        vec_with_capacity_in(mcx, states.len())?;
    for st in states.into_iter().flatten() {
        boxed.push(alloc_in(mcx, st)?);
    }
    if let Some(p) = aggstate.peragg.as_mut() {
        p[aggno as usize].aggdirectargs = Some(boxed);
    }
    Ok(())
}

/// C: `build_aggregate_finalfn_expr(...); fmgr_info(finalfn_oid, &finalfn);
/// fmgr_info_set_expr((Node *)finalfnexpr, &finalfn);` — parse_agg (expr build)
/// + fmgr (info, with expr), all installed seams.
fn setup_peragg_finalfn<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    aggno: i32,
    idx: usize,
    finalfn_oid: Oid,
    num_final_args: i32,
    aggtranstype: Oid,
    input_types: &[Oid],
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let (agg_result_type, agg_input_collation) = {
        let prim = &aggstate
            .aggs_prim
            .as_ref()
            .expect("setup_peragg_finalfn: aggs_prim is NULL")[idx];
        (prim.aggtype, prim.inputcollid)
    };
    // build_aggregate_finalfn_expr(input_types, num_final_args, aggtranstype,
    //                              aggref->aggtype, aggref->inputcollid, finalfn_oid)
    let finalfnexpr = parse_agg_seams::build_aggregate_finalfn_expr::call(
        input_types,
        num_final_args,
        aggtranstype,
        agg_result_type,
        agg_input_collation,
        finalfn_oid,
    )?;
    // fmgr_info(finalfn_oid, &peragg->finalfn);
    // fmgr_info_set_expr((Node *) finalfnexpr, &peragg->finalfn);
    let mut finfo = fmgr_seams::fmgr_info::call(mcx, finalfn_oid)?;
    fmgr_seams::fmgr_info_set_expr::call(mcx, &mut finfo, &finalfnexpr)?;
    if let Some(p) = aggstate.peragg.as_mut() {
        p[aggno as usize].finalfn = finfo;
    }
    Ok(())
}

/// C: `get_typlenbyval(aggref->aggtype, &resulttypeLen, &resulttypeByVal)`
/// (lsyscache.c). Read through the installed `backend-utils-cache-lsyscache`
/// seam and store the result into the peragg slot.
fn set_peragg_result_typlenbyval(aggstate: &mut AggStateData<'_>, aggno: i32, aggtype: Oid) -> PgResult<()> {
    let (resulttype_len, resulttype_by_val) =
        lsyscache_seams::get_typlenbyval::call(aggtype)?;
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
    aggstate: &mut AggStateData<'mcx>,
    aggno: i32,
    idx: usize,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // peragg->aggref = aggref; — the owned model stores its own copy of the
    // executor-side Aggref (built from the expression-tree original).
    let copy = {
        let prim = &aggstate
            .aggs_prim
            .as_ref()
            .expect("set_peragg_aggref: aggs_prim is NULL")[idx];
        nodes::nodeagg::Aggref::from_primnode(prim, mcx)?
    };
    if let Some(p) = aggstate.peragg.as_mut() {
        p[aggno as usize].aggref = Some(alloc_in(mcx, copy)?);
    }
    Ok(())
}

/// C: `build_pertrans_for_aggref(pertrans, aggstate, estate, aggref, ...)` —
/// the in-crate per-trans build (node_lifecycle). It needs the Aggref by
/// reference, which the borrow checker cannot supply alongside `&mut aggstate`
/// without cloning the Aggref (types-nodes Aggref::clone_in, not yet provided).
#[allow(clippy::too_many_arguments)]
fn build_pertrans_call<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    transno: i32,
    idx: usize,
    transfn_oid: Oid,
    aggtranstype: Oid,
    serialfn_oid: Oid,
    deserialfn_oid: Oid,
    init_value: types_tuple::heaptuple::Datum<'mcx>,
    init_value_is_null: bool,
    input_types: &[Oid],
    num_arguments: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // build_pertrans_for_aggref takes &mut pertrans, &mut aggstate and &aggref
    // simultaneously. Break the aliasing borrow by (1) taking the pertrans slot
    // out of aggstate, (2) building an owned executor-side Aggref copy from the
    // expression-tree original, then (3) calling and putting the pertrans back.
    let mut pertrans = {
        let p = aggstate
            .pertrans
            .as_mut()
            .expect("build_pertrans_call: pertrans is NULL");
        core::mem::take(&mut p[transno as usize])
    };
    let aggref = {
        let prim = &aggstate
            .aggs_prim
            .as_ref()
            .expect("build_pertrans_call: aggs_prim is NULL")[idx];
        nodes::nodeagg::Aggref::from_primnode(prim, mcx)?
    };
    let result = build_pertrans_for_aggref(
        &mut pertrans,
        aggstate,
        estate,
        &aggref,
        transfn_oid,
        aggtranstype,
        serialfn_oid,
        deserialfn_oid,
        init_value,
        init_value_is_null,
        input_types,
        num_arguments,
        mcx,
    );
    // Put the (now-filled) pertrans back regardless of result.
    if let Some(p) = aggstate.pertrans.as_mut() {
        p[transno as usize] = pertrans;
    }
    result
}

/// C: `pertrans->transfn.fn_strict` — the resolved transfn's strict flag, read
/// from the FmgrInfo build_pertrans_for_aggref filled in. Unavailable until the
/// per-trans build runs (which depends on the unported owners above).
fn pertrans_transfn_strict(aggstate: &AggStateData<'_>, transno: i32) -> bool {
    aggstate
        .pertrans
        .as_ref()
        .map(|p| p[transno as usize].transfn.fn_strict)
        .unwrap_or(false)
}

/// C: `format_type_be(aggtranstype)` (format_type.c) — the type's display name
/// for the STRICT-combine error message.
fn format_type_be(typeoid: Oid) -> PgResult<alloc::string::String> {
    // The STRICT-combine error message needs an owned String; format_type_be
    // palloc's into a context. Use the owned-String convenience seam so the
    // result outlives the call frame's transient context.
    format_type_seams::format_type_be_owned::call(typeoid)
}

/// C: `IsBinaryCoercible(input_type, aggtranstype)` (parse_coerce.c) — used by
/// the strict-initval compatibility check.
fn is_binary_coercible(src: Oid, target: Oid) -> PgResult<bool> {
    coerce_seams::is_binary_coercible::call(src, target)
}

/// C: `phase->evaltrans = ExecBuildAggTrans(aggstate, phase, dosort, dohash,
/// false); phase->evaltrans_cache[0][0] = phase->evaltrans;` (execExpr.c).
/// Reaches the execExpr owner through the `exec_build_agg_trans` seam, passing
/// the AggState as the erased `AggStateLive` carrier (the owner downcasts it).
#[allow(clippy::too_many_arguments)]
fn exec_build_agg_trans<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    phaseidx: i32,
    dosort: bool,
    dohash: bool,
    nullcheck: bool,
    estate: &mut EStateData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let evaltrans = execExpr_seams::exec_build_agg_trans::call(
        mcx, aggstate, phaseidx, dosort, dohash, nullcheck, estate,
    )?;
    // phase->evaltrans = evaltrans; phase->evaltrans_cache[0][0] = evaltrans;
    if let Some(phases) = aggstate.phases.as_mut() {
        phases[phaseidx as usize].evaltrans = Some(evaltrans);
    }
    Ok(())
}
