// execPartition.c, runtime pruning lane: ExecDoInitialPruning at executor
// startup + per-scan exec pruning over the partprune kernel. The prune state
// is rebuilt at Append init from the plan's PartitionPruneInfo (C passes the
// startup-built state through es_part_prune_states; initial results transfer
// through es_part_prune_results either way).
use std::rc::Rc;

use datum::Datum;
use types_core::{InvalidOid, Oid};
use types_error::PgResult;
use types_fmgr::{FmgrInfo, LocalFcinfo};
use types_nodes::bitmapset::Bitmapset;
use types_nodes::list::NodeList;
use types_nodes::plannodes::{PartitionPruneStepOp, PartitionedRelPruneInfo};
use types_nodes::{Node, NodeTag};
use types_slot::EXEC_FLAG_EXPLAIN_GENERIC;

use executils::{EStateData, EcxtId};
use partcache::PartitionKeyData;
use partdesc::PartitionDescData;
use partprune::{PruneStepResult, PARTITION_MAX_KEYS};

pub struct PartitionPruneState<'mcx> {
    hierarchies: Vec<Vec<PartitionedRelPruningData<'mcx>>>,
    pub do_initial_prune: bool,
    pub do_exec_prune: bool,
    other_subplans: Bitmapset<'mcx>,
    pub execparamids: Bitmapset<'mcx>,
    econtext: EcxtId,
}

struct PartitionedRelPruningData<'mcx> {
    pinfo: &'mcx PartitionedRelPruneInfo<'mcx>,
    partkey: Rc<PartitionKeyData>,
    partdesc: Rc<PartitionDescData>,
    // Sized by the executor-time partdesc; positions remapped from pinfo when
    // the partition set changed since planning (plan-time-pruned partitions
    // leave InvalidOid holes in relid_map; concurrent attach/detach shifts
    // positions). subplan_map is further re-sequenced by
    // init_exec_partition_prune_contexts after initial pruning.
    subplan_map: Vec<i32>,
    subpart_map: Vec<i32>,
    leafpart_rti_map: Vec<i32>,
    present_parts: Bitmapset<'mcx>,
    initial_ctx: Option<PruneContext<'mcx>>,
    exec_ctx: Option<PruneContext<'mcx>>,
}

// C PartitionPruneContext: per-(step,key) resolved cmpfns + ExprStates,
// stateidx = step_id * partnatts + keyno.
struct PruneContext<'mcx> {
    exprstates: Vec<Option<mcx::PgBox<'mcx, execexpr::ExprState<'mcx>>>>,
    cmpfuncs: Vec<Option<FmgrInfo>>,
}

// ExecDoInitialPruning (execPartition.c): fills es_unpruned_relids and
// es_part_prune_results; the states themselves are rebuilt at node init.
pub fn exec_do_initial_pruning<'mcx>(estate: &mut EStateData<'mcx>) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let pstmt = estate.es_plannedstmt.expect("es_plannedstmt set before pruning");
    for pruneinfo in pstmt.partPruneInfos.iter() {
        let mut all_leafpart_rtis = Bitmapset::empty();
        let mut prunestate =
            create_partition_prune_state(estate, pruneinfo, true, &mut all_leafpart_rtis)?;
        let result;
        let validsubplan_rtis;
        if prunestate.do_initial_prune {
            let mut rtis = Bitmapset::empty();
            let valid =
                exec_find_matching_subplans(&mut prunestate, estate, true, Some(&mut rtis))?;
            result = Some(valid);
            validsubplan_rtis = rtis;
        } else {
            result = None;
            validsubplan_rtis = all_leafpart_rtis;
        }
        estate.es_unpruned_relids.add_members(mcx, &validsubplan_rtis)?;
        estate.es_part_prune_results.push(result);
        let econtext = prunestate.econtext;
        drop(prunestate);
        estate.free_expr_context(econtext, true);
    }
    Ok(())
}

/// ExecInitPartitionExecPruning (execPartition.c). Returns the initially
/// valid original-subplan indexes plus the exec-pruning state (None when no
/// exec pruning steps exist).
pub fn exec_init_partition_exec_pruning<'mcx>(
    estate: &mut EStateData<'mcx>,
    n_total_subplans: i32,
    part_prune_index: i32,
    relids: &Bitmapset<'mcx>,
) -> PgResult<(Option<PartitionPruneState<'mcx>>, Bitmapset<'mcx>)> {
    let mcx = estate.es_query_cxt;
    let pstmt = estate.es_plannedstmt.expect("es_plannedstmt set");
    let pruneinfo_node = pstmt.partPruneInfos.nth(part_prune_index as usize);
    let pruneinfo = pruneinfo_node
        .as_partition_prune_info()
        .expect("partPruneInfos cell is a PartitionPruneInfo");
    assert!(
        relids.equal(&pruneinfo.relids),
        "wrong pruneinfo found at part_prune_index={part_prune_index}"
    );

    let mut unused = Bitmapset::empty();
    let mut prunestate = create_partition_prune_state(estate, pruneinfo_node, false, &mut unused)?;

    let initially_valid = if prunestate.do_initial_prune {
        estate.es_part_prune_results[part_prune_index as usize]
            .as_ref()
            .expect("initial pruning result recorded by ExecDoInitialPruning")
            .clone_in(mcx)?
    } else {
        debug_assert!(n_total_subplans > 0);
        let mut all = Bitmapset::empty();
        partprune::bms_add_range(mcx, &mut all, 0, n_total_subplans - 1)?;
        all
    };

    if prunestate.do_exec_prune {
        init_exec_partition_prune_contexts(
            &mut prunestate,
            estate,
            &initially_valid,
            n_total_subplans,
        )?;
        Ok((Some(prunestate), initially_valid))
    } else {
        let econtext = prunestate.econtext;
        drop(prunestate);
        estate.free_expr_context(econtext, true);
        Ok((None, initially_valid))
    }
}

// CreatePartitionPruneState (execPartition.c); `for_initial` selects whether
// initial-step ExprStates get built (the node-init rebuild only needs exec).
fn create_partition_prune_state<'mcx>(
    estate: &mut EStateData<'mcx>,
    pruneinfo_node: Node<'mcx>,
    for_initial: bool,
    all_leafpart_rtis: &mut Bitmapset<'mcx>,
) -> PgResult<PartitionPruneState<'mcx>> {
    let mcx = estate.es_query_cxt;
    let pruneinfo = pruneinfo_node
        .as_partition_prune_info()
        .expect("PartitionPruneInfo node");
    let econtext = estate.create_expr_context();
    let explain_generic = estate.es_top_eflags & EXEC_FLAG_EXPLAIN_GENERIC != 0;

    let mut state = PartitionPruneState {
        hierarchies: Vec::with_capacity(pruneinfo.prune_infos.len()),
        do_initial_prune: false,
        do_exec_prune: false,
        other_subplans: pruneinfo.other_subplans.clone_in(mcx)?,
        execparamids: Bitmapset::empty(),
        econtext,
    };

    for hierarchy in pruneinfo.prune_infos.iter() {
        let partrelpruneinfos = hierarchy.as_list().expect("prune_infos cell is a List");
        let mut prunedata = Vec::with_capacity(partrelpruneinfos.len());
        for pinfo_node in partrelpruneinfos.iter() {
            let pinfo = pinfo_node
                .as_partitioned_rel_prune_info()
                .expect("PartitionedRelPruneInfo node");
            let (partdesc, partkey) = {
                let partrel = estate.exec_get_range_table_relation(pinfo.rtindex, false)?;
                (
                    partdesc::RelationGetPartitionDesc(partrel, false)?,
                    partcache::RelationGetPartitionKey(partrel)?,
                )
            };

            // C's identical-partdesc fast path, else the positional remap:
            // walk both arrays in bound order, skipping relid_map's
            // InvalidOid holes (plan-time-pruned partitions) and planner-seen
            // partitions that vanished; partitions the planner never saw get
            // -1/-1/0 (as if pruned).
            let n_new = partdesc.nparts;
            let identical = n_new as i32 == pinfo.nparts
                && partdesc.oids.iter().zip(pinfo.relid_map.iter()).all(|(a, b)| a == b);
            let (subplan_map, subpart_map, leafpart_rti_map) = if identical {
                (
                    pinfo.subplan_map.to_vec(),
                    pinfo.subpart_map.to_vec(),
                    pinfo.leafpart_rti_map.to_vec(),
                )
            } else {
                let mut subplan_map = vec![-1i32; n_new];
                let mut subpart_map = vec![-1i32; n_new];
                let mut rti_map = vec![0i32; n_new];
                let n_old = pinfo.nparts as usize;
                let mut pd_idx = 0usize;
                for pp_idx in 0..n_new {
                    while pd_idx < n_old && pinfo.relid_map[pd_idx] == InvalidOid {
                        pd_idx += 1;
                    }
                    loop {
                        if pd_idx < n_old && pinfo.relid_map[pd_idx] == partdesc.oids[pp_idx] {
                            subplan_map[pp_idx] = pinfo.subplan_map[pd_idx];
                            subpart_map[pp_idx] = pinfo.subpart_map[pd_idx];
                            rti_map[pp_idx] = pinfo.leafpart_rti_map[pd_idx];
                            pd_idx += 1;
                            break;
                        }
                        // C's recheck goto: peek ahead for a later match
                        // (planner saw a since-detached partition in between).
                        match ((pd_idx + 1)..n_old)
                            .find(|&i| pinfo.relid_map[i] == partdesc.oids[pp_idx])
                        {
                            Some(i) => pd_idx = i,
                            None => break,
                        }
                    }
                }
                (subplan_map, subpart_map, rti_map)
            };

            let mut pprune = PartitionedRelPruningData {
                pinfo,
                partkey,
                partdesc,
                subplan_map,
                subpart_map,
                leafpart_rti_map,
                present_parts: pinfo.present_parts.clone_in(mcx)?,
                initial_ctx: None,
                exec_ctx: None,
            };

            if !pinfo.initial_pruning_steps.is_nil() && !explain_generic {
                if for_initial {
                    let partnatts = pprune.partkey.partnatts as usize;
                    pprune.initial_ctx =
                        Some(init_prune_context(estate, &pinfo.initial_pruning_steps, partnatts)?);
                }
                state.do_initial_prune = true;
            }
            if !pinfo.exec_pruning_steps.is_nil() && !explain_generic {
                state.do_exec_prune = true;
            }
            state.execparamids.add_members(mcx, &pinfo.execparamids)?;

            if !pinfo.initial_pruning_steps.is_nil() && !state.do_initial_prune {
                let mut part_index = pprune.present_parts.next_member(-1);
                while part_index >= 0 {
                    let rtindex = pprune.leafpart_rti_map[part_index as usize];
                    if rtindex != 0 {
                        all_leafpart_rtis.add_member(mcx, rtindex)?;
                    }
                    part_index = pprune.present_parts.next_member(part_index);
                }
            }

            prunedata.push(pprune);
        }
        state.hierarchies.push(prunedata);
    }

    Ok(state)
}

// InitPartitionPruneContext (execPartition.c): resolve cmpfns and compile
// non-Const step exprs once per (step, key).
fn init_prune_context<'mcx>(
    estate: &mut EStateData<'mcx>,
    steps: &NodeList<'mcx>,
    partnatts: usize,
) -> PgResult<PruneContext<'mcx>> {
    let mcx = estate.es_query_cxt;
    let n_steps = steps.len();
    let mut ctx = PruneContext { exprstates: Vec::new(), cmpfuncs: Vec::new() };
    ctx.exprstates.resize_with(n_steps * partnatts, || None);
    ctx.cmpfuncs.resize_with(n_steps * partnatts, || None);
    let params = estate.param_bind();
    // C compiles exec-pruning step exprs against the parent planstate
    // (execPartition.c:2311); SubPlans in step exprs need the env.
    ::executils::with_subplan_compile_env(estate, |env| -> PgResult<()> {
        for step in steps.iter() {
            let Some(op) = step.as_partition_prune_step_op() else { continue };
            debug_assert!(op.exprs.len() <= partnatts);
            let mut it = op.exprs.iter().zip(op.cmpfns.iter());
            for keyno in 0..partnatts {
                if op.nullkeys.is_member(keyno as i32) {
                    continue;
                }
                if let Some((expr, cmpfn)) = it.next() {
                    let stateidx = op.step_id as usize * partnatts + keyno;
                    debug_assert!(cmpfn != InvalidOid);
                    ctx.cmpfuncs[stateidx] = Some(
                        fmgr_core::fmgr_info(cmpfn)
                            .unwrap_or_else(|e| panic!("fmgr_info({cmpfn}) failed: {e:?}")),
                    );
                    if expr.node_tag() != NodeTag::T_Const {
                        let mut state =
                            execexpr::exec_init_expr_subplans(mcx, Some(expr), params, env)?;
                        if let Some(st) = state.as_mut() {
                            // By-ref step-expr results land in the query mcx (C:
                            // node econtext per-tuple; pruning runs per rescan,
                            // not per row — bounded growth).
                            st.arm_result_mcx(mcx);
                        }
                        ctx.exprstates[stateidx] = state;
                    }
                }
            }
        }
        Ok(())
    })?;
    Ok(ctx)
}

// InitExecPartitionPruneContexts (execPartition.c): build exec contexts and
// re-sequence subplan indexes to the post-initial-pruning positions.
fn init_exec_partition_prune_contexts<'mcx>(
    prunestate: &mut PartitionPruneState<'mcx>,
    estate: &mut EStateData<'mcx>,
    initially_valid_subplans: &Bitmapset<'mcx>,
    n_total_subplans: i32,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    debug_assert!(prunestate.do_exec_prune);

    let mut new_subplan_indexes: Option<Vec<i32>> = None;
    if initially_valid_subplans.num_members() < n_total_subplans {
        let mut map = vec![0i32; n_total_subplans as usize];
        let mut newidx = 1;
        let mut i = initially_valid_subplans.next_member(-1);
        while i >= 0 {
            map[i as usize] = newidx;
            newidx += 1;
            i = initially_valid_subplans.next_member(i);
        }
        new_subplan_indexes = Some(map);
    }

    for hi in 0..prunestate.hierarchies.len() {
        for j in (0..prunestate.hierarchies[hi].len()).rev() {
            if !prunestate.hierarchies[hi][j].pinfo.exec_pruning_steps.is_nil() {
                let partnatts = prunestate.hierarchies[hi][j].partkey.partnatts as usize;
                let steps = &prunestate.hierarchies[hi][j].pinfo.exec_pruning_steps;
                let ctx = init_prune_context(estate, steps, partnatts)?;
                prunestate.hierarchies[hi][j].exec_ctx = Some(ctx);
            }

            let Some(map) = new_subplan_indexes.as_ref() else { continue };
            // C pprune->nparts: the executor-time partdesc size.
            let nparts = prunestate.hierarchies[hi][j].subplan_map.len();
            let mut present = Bitmapset::empty();
            for k in 0..nparts {
                let oldidx = prunestate.hierarchies[hi][j].subplan_map[k];
                if oldidx >= 0 {
                    debug_assert!(oldidx < n_total_subplans);
                    prunestate.hierarchies[hi][j].subplan_map[k] = map[oldidx as usize] - 1;
                    if map[oldidx as usize] > 0 {
                        present.add_member(mcx, k as i32)?;
                    }
                } else {
                    let subidx = prunestate.hierarchies[hi][j].subpart_map[k];
                    if subidx >= 0
                        && !prunestate.hierarchies[hi][subidx as usize].present_parts.is_empty()
                    {
                        present.add_member(mcx, k as i32)?;
                    }
                }
            }
            prunestate.hierarchies[hi][j].present_parts = present;
        }
    }

    if let Some(map) = new_subplan_indexes {
        let mut new_other = Bitmapset::empty();
        let mut i = prunestate.other_subplans.next_member(-1);
        while i >= 0 {
            new_other.add_member(mcx, map[i as usize] - 1)?;
            i = prunestate.other_subplans.next_member(i);
        }
        prunestate.other_subplans = new_other;
    }
    Ok(())
}

/// ExecFindMatchingSubPlans (execPartition.c).
pub fn exec_find_matching_subplans<'mcx>(
    prunestate: &mut PartitionPruneState<'mcx>,
    estate: &mut EStateData<'mcx>,
    initial_prune: bool,
    mut validsubplan_rtis: Option<&mut Bitmapset<'mcx>>,
) -> PgResult<Bitmapset<'mcx>> {
    debug_assert!(initial_prune || prunestate.do_exec_prune);
    debug_assert!(validsubplan_rtis.is_some() || !initial_prune);
    let mut result = Bitmapset::empty();
    for hi in 0..prunestate.hierarchies.len() {
        find_matching_subplans_recurse(
            &mut prunestate.hierarchies[hi],
            0,
            estate,
            initial_prune,
            &mut result,
            &mut validsubplan_rtis,
        )?;
    }
    let mcx = estate.es_query_cxt;
    result.add_members(mcx, &prunestate.other_subplans)?;
    estate.reset_expr_context(prunestate.econtext);
    Ok(result)
}

fn find_matching_subplans_recurse<'mcx>(
    prunedata: &mut Vec<PartitionedRelPruningData<'mcx>>,
    idx: usize,
    estate: &mut EStateData<'mcx>,
    initial_prune: bool,
    validsubplans: &mut Bitmapset<'mcx>,
    validsubplan_rtis: &mut Option<&mut Bitmapset<'mcx>>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let partset = if initial_prune && !prunedata[idx].pinfo.initial_pruning_steps.is_nil() {
        get_matching_partitions(&mut prunedata[idx], estate, true)?
    } else if !initial_prune && !prunedata[idx].pinfo.exec_pruning_steps.is_nil() {
        get_matching_partitions(&mut prunedata[idx], estate, false)?
    } else {
        prunedata[idx].present_parts.clone_in(mcx)?
    };

    let mut i = partset.next_member(-1);
    while i >= 0 {
        let subplan = prunedata[idx].subplan_map[i as usize];
        if subplan >= 0 {
            validsubplans.add_member(mcx, subplan)?;
            let rti = prunedata[idx].leafpart_rti_map[i as usize];
            if rti != 0 {
                if let Some(rtis) = validsubplan_rtis.as_mut() {
                    rtis.add_member(mcx, rti)?;
                }
            }
        } else {
            let partidx = prunedata[idx].subpart_map[i as usize];
            if partidx >= 0 {
                find_matching_subplans_recurse(
                    prunedata,
                    partidx as usize,
                    estate,
                    initial_prune,
                    validsubplans,
                    validsubplan_rtis,
                )?;
            }
        }
        i = partset.next_member(i);
    }
    Ok(())
}

// get_matching_partitions (partprune.c), executor arm over the compiled
// per-step contexts.
fn get_matching_partitions<'mcx>(
    pprune: &mut PartitionedRelPruningData<'mcx>,
    estate: &mut EStateData<'mcx>,
    initial: bool,
) -> PgResult<Bitmapset<'mcx>> {
    let mcx = estate.es_query_cxt;
    let steps = if initial {
        &pprune.pinfo.initial_pruning_steps
    } else {
        &pprune.pinfo.exec_pruning_steps
    };
    let num_steps = steps.len();
    debug_assert!(num_steps > 0);
    let partdesc = Rc::clone(&pprune.partdesc);
    let partkey = Rc::clone(&pprune.partkey);
    let strategy = partkey.strategy as u8;
    let boundinfo = partdesc.boundinfo.as_ref().expect("partitioned rel has bounds");
    let ctx = if initial { pprune.initial_ctx.as_mut() } else { pprune.exec_ctx.as_mut() }
        .expect("prune context initialized for this pass");

    // ExecEvalParamExec pending-initplan arm, hoisted (execScan precedent).
    let mut deps: Vec<u32> = Vec::new();
    for st in ctx.exprstates.iter().flatten() {
        deps.extend_from_slice(st.param_exec_deps());
    }
    if !deps.is_empty() {
        executils::exec_eval_param_exec_params(estate, &deps)?;
    }

    let mut results: Vec<Option<PruneStepResult<'mcx>>> = Vec::new();
    results.resize_with(num_steps, || None);
    for step in steps.iter() {
        match step.node_tag() {
            NodeTag::T_PartitionPruneStepOp => {
                let op = step.as_partition_prune_step_op().unwrap();
                let res = perform_pruning_base_step_exec(mcx, ctx, &partkey, boundinfo, op)?;
                results[op.step_id as usize] = Some(res);
            }
            NodeTag::T_PartitionPruneStepCombine => {
                let c = step.as_partition_prune_step_combine().unwrap();
                let res = partprune::perform_pruning_combine_step(
                    mcx,
                    boundinfo,
                    c.combineOp,
                    c.step_id,
                    c.source_stepids.iter(),
                    &results,
                )?;
                results[c.step_id as usize] = Some(res);
            }
            other => panic!("invalid pruning step type: {other:?}"),
        }
    }
    let final_result = results[num_steps - 1].as_ref().expect("final step evaluated");
    partprune::matching_bounds_to_partitions(mcx, boundinfo, final_result, strategy)
}

fn sup_call(f: &mut FmgrInfo, coll: Oid, a: Datum, b: Datum) -> Datum {
    let mut fcinfo = LocalFcinfo::<2>::new(coll);
    // range_cmp / SQL-function support procs detoast and build by-ref
    // intermediates through the result mcx; call-lifetime scratch (sup_cmp
    // precedent in execpartition).
    let scratch = ::mcx::MemoryContext::new("partprune sup_call");
    // SAFETY: scratch outlives this call.
    unsafe { fcinfo.set_result_mcx(scratch.mcx()) };
    fcinfo.set_arg(0, a);
    fcinfo.set_arg(1, b);
    let r = f
        .invoke(&mut fcinfo)
        .unwrap_or_else(|e| panic!("partition comparison function failed: {e:?}"));
    assert!(!fcinfo.isnull, "partition comparison function returned NULL");
    r
}

// perform_pruning_base_step (partprune.c), executor arm: values come from
// Consts or the compiled ExprStates (empty EvalSlots — pruning exprs carry no
// Vars); a NULL comparison value prunes everything (strict operators only).
fn perform_pruning_base_step_exec<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    ctx: &mut PruneContext<'mcx>,
    partkey: &PartitionKeyData,
    boundinfo: &partbounds::PartitionBoundInfoData<'static>,
    opstep: &PartitionPruneStepOp<'mcx>,
) -> PgResult<PruneStepResult<'mcx>> {
    debug_assert_eq!(opstep.exprs.len(), opstep.cmpfns.len());
    let partnatts = partkey.partnatts as i32;
    let strategy = partkey.strategy as u8;
    let mut values = [Datum::null(); PARTITION_MAX_KEYS];
    let mut nvalues = 0i32;
    let mut it = opstep.exprs.iter();
    for keyno in 0..partnatts {
        if opstep.nullkeys.is_member(keyno) {
            continue;
        }
        if keyno > nvalues && strategy == b'r' {
            break;
        }
        if let Some(expr) = it.next() {
            let stateidx = opstep.step_id as usize * partnatts as usize + keyno as usize;
            let (datum, isnull) = if let Some(c) = expr.as_const() {
                (c.constvalue, c.constisnull)
            } else {
                let state = ctx.exprstates[stateidx]
                    .as_mut()
                    .expect("non-Const step expr has an ExprState");
                let mut slots = execexpr::EvalSlots { scan: None, inner: None, outer: None };
                let nd = execexpr::exec_eval_expr(state, &mut slots)?;
                (nd.value, nd.isnull)
            };
            if isnull {
                return Ok(PruneStepResult::empty());
            }
            values[keyno as usize] = datum;
            nvalues += 1;
        }
    }

    let base = opstep.step_id as usize * partnatts as usize;
    let partcollation = &partkey.partcollation;

    match strategy {
        b'h' => partprune::get_matching_hash_bounds(
            mcx,
            boundinfo,
            partnatts,
            opstep.opstrategy,
            nvalues,
            &opstep.nullkeys,
            || {
                let mut row_hash = 0u64;
                for keyno in 0..partnatts {
                    if opstep.nullkeys.is_member(keyno) {
                        continue;
                    }
                    let f = ctx.cmpfuncs[base + keyno as usize].as_mut().expect("cmpfn resolved");
                    let h = sup_call(
                        f,
                        partcollation[keyno as usize],
                        values[keyno as usize],
                        Datum::from_u64(partprune::HASH_PARTITION_SEED),
                    );
                    row_hash = partprune::hash_combine64(row_hash, h.as_u64());
                }
                row_hash
            },
        ),
        b'l' => {
            let coll = partcollation[0];
            let cmpfuncs = &mut ctx.cmpfuncs;
            partprune::get_matching_list_bounds(
                mcx,
                boundinfo,
                opstep.opstrategy,
                nvalues,
                &opstep.nullkeys,
                |bound| {
                    let f = cmpfuncs[base].as_mut().expect("cmpfn resolved");
                    sup_call(f, coll, bound, values[0]).as_i32()
                },
            )
        }
        b'r' => {
            let cmpfuncs = &mut ctx.cmpfuncs;
            partprune::get_matching_range_bounds(
                mcx,
                boundinfo,
                partnatts,
                opstep.opstrategy,
                nvalues,
                &opstep.nullkeys,
                &mut |j: i32, bound: Datum| {
                    let f = cmpfuncs[base + j as usize].as_mut().expect("cmpfn resolved");
                    sup_call(f, partcollation[j as usize], bound, values[j as usize]).as_i32()
                },
            )
        }
        other => panic!("unexpected partition strategy: {}", other as char),
    }
}
