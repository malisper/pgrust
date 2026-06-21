//! Run-time partition pruning family: `ExecDoInitialPruning`,
//! `ExecInitPartitionExecPruning`, `CreatePartitionPruneState`,
//! `InitPartitionPruneContext`, `InitExecPartitionPruneContexts`,
//! `ExecFindMatchingSubPlans`, `find_matching_subplans_recurse`.

use mcx::{alloc_in, slice_in, vec_with_capacity_in, Mcx, MemoryContext, PgBox, PgVec};
use types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use types_nodes::executor::EXEC_FLAG_EXPLAIN_GENERIC;
use types_nodes::partition::{
    PartitionDescData, PartitionKeyData, PartitionPruneContext, PartitionPruneState,
    PartitionPruningData, PartitionedRelPruningData,
};
use types_nodes::primnodes::Expr;
use types_nodes::{Bitmapset, EStateData, EcxtId, Opaque, PlanStateData};

use backend_executor_execUtils::{CreateExprContext, ExecGetRangeTableRelation};
use backend_nodes_core_seams as bms;
use backend_partitioning_core_seams as partdesc_seams;
use backend_partitioning_partprune_seams as partprune_seams;
use backend_utils_cache_partcache_seams as partcache_seams;
use backend_utils_misc_stack_depth_seams as stack_depth_seams;

/* ---------------------------------------------------------------------------
 * Planner-owned plan-node vocabulary the executor only reads.
 *
 * `PartitionPruneInfo` / `PartitionedRelPruneInfo` (nodes/plannodes.h) and the
 * `PartitionPruneStep` family (also nodes/plannodes.h) are produced by the
 * planner (`partprune.c` `make_partition_pruneinfo`) and stored by the executor
 * as the type-erased payload of the `Opaque` handles in
 * `EState.es_part_prune_infos` and the
 * `PartitionedRelPruningData.{initial,exec}_pruning_steps` fields. The real
 * types live in `types_nodes::partprune_carrier` (shared with the producing
 * crate); we re-export them and downcast the `Opaque` payload with a loud panic
 * on mismatch — exactly the C `lfirst_node()` cast.
 * ------------------------------------------------------------------------- */

pub use types_nodes::partprune_carrier::{
    PartitionPruneInfo, PartitionPruneStep, PartitionPruneStepCombine, PartitionPruneStepOp,
    PartitionedRelPruneInfo, RawBms,
};

/// `PruneCxtStateIdx(partnatts, step_id, keyno)` (partprune.h) — index into the
/// `stepcmpfuncs[]` / `exprstates[]` arrays.
fn prune_cxt_state_idx(partnatts: i32, step_id: i32, keyno: i32) -> usize {
    (partnatts * step_id + keyno) as usize
}

/// Downcast an `&Opaque` payload to a borrowed `PartitionPruneInfo`, mirroring
/// the C `lfirst_node(PartitionPruneInfo, lc)` cast (loud panic on mismatch).
fn pruneinfo_ref(o: &Opaque) -> &PartitionPruneInfo<'static> {
    // The `Opaque` payload is a type-erased `Any`, which is `'static`-bound; the
    // stored `PartitionPruneInfo` (a plan-tree node living in a context that
    // outlives the executor run) is the `'static`-erased form.
    o.0.as_ref()
        .expect("es_part_prune_infos element is NULL")
        .downcast_ref::<PartitionPruneInfo>()
        .expect("es_part_prune_infos element is not a PartitionPruneInfo")
}

/// Wrap a raw `bitmapword[]` plan bitmap into a transient `Bitmapset` allocated
/// in `mcx` so it can be passed to the `bms_*` owner seams (`None` stays the C
/// NULL set).
fn raw_to_bms<'mcx>(mcx: Mcx<'mcx>, raw: &RawBms) -> PgResult<Option<Bitmapset<'mcx>>> {
    match raw {
        Some(words) => Ok(Some(Bitmapset {
            words: slice_in(mcx, words.as_slice())?,
        })),
        None => Ok(None),
    }
}

/// `ExecDoInitialPruning(estate)` — perform runtime "initial" pruning for every
/// `PartitionPruneInfo` in `estate->es_part_prune_infos`, building each
/// `PartitionPruneState` (appended to `es_part_prune_states`) and storing the
/// surviving-subplan bitmapset (or `None`) in `es_part_prune_results`; also
/// accumulates the surviving leaf RT indexes into `es_unpruned_relids`.
/// Fallible (pruning evaluation, OOM).
pub fn ExecDoInitialPruning<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // foreach(lc, estate->es_part_prune_infos)
    let n = estate.es_part_prune_infos.len();
    for idx in 0..n {
        // PartitionPruneState *prunestate;
        // Bitmapset *validsubplans = NULL;
        // Bitmapset *all_leafpart_rtis = NULL;
        // Bitmapset *validsubplan_rtis = NULL;
        let mut validsubplans: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
        let mut all_leafpart_rtis: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
        let validsubplan_rtis: Option<PgBox<'mcx, Bitmapset<'mcx>>>;

        // prunestate = CreatePartitionPruneState(estate, pruneinfo,
        //                                        &all_leafpart_rtis);
        let mut prunestate = CreatePartitionPruneState(mcx, estate, idx, &mut all_leafpart_rtis)?;

        // if (prunestate->do_initial_prune)
        if prunestate.do_initial_prune {
            //     validsubplans = ExecFindMatchingSubPlans(prunestate, true,
            //                                              &validsubplan_rtis);
            // (the internal worker carries the `validsubplan_rtis` out-param;
            // the trimmed `exec_find_matching_subplans` seam wraps it with
            // `validsubplan_rtis = NULL`.)
            let mut rtis: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
            validsubplans =
                find_matching_subplans(mcx, estate, &mut prunestate, true, Some(&mut rtis))?;
            validsubplan_rtis = rtis;
        } else {
            //     validsubplan_rtis = all_leafpart_rtis;
            validsubplan_rtis = all_leafpart_rtis;
        }

        // estate->es_part_prune_states = lappend(..., prunestate);
        // (C lappends right after CreatePartitionPruneState; here the find
        // worker above operated on the local `&mut prunestate`, so the box is
        // installed into the pool now — index stays parallel to
        // es_part_prune_infos / es_part_prune_results.)
        let boxed = alloc_in(mcx, prunestate)?;
        estate
            .es_part_prune_states
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<Option<PgBox<'mcx, PartitionPruneState<'mcx>>>>()))?;
        estate.es_part_prune_states.push(Some(boxed));

        // estate->es_unpruned_relids =
        //     bms_add_members(estate->es_unpruned_relids, validsubplan_rtis);
        let cur = estate.es_unpruned_relids.take();
        estate.es_unpruned_relids =
            bms::bms_add_members::call(mcx, cur, validsubplan_rtis.as_deref())?;

        // estate->es_part_prune_results =
        //     lappend(estate->es_part_prune_results, validsubplans);
        estate.es_part_prune_results.try_reserve(1).map_err(|_| {
            mcx.oom(core::mem::size_of::<Option<PgBox<'mcx, Bitmapset<'mcx>>>>())
        })?;
        estate.es_part_prune_results.push(validsubplans);
    }
    Ok(())
}

/// `ExecInitPartitionExecPruning(planstate, n_total_subplans, part_prune_index,
/// relids, &initially_valid_subplans)` — initialize the data needed for "exec"
/// pruning and return the matching `PartitionPruneState` (the one
/// `ExecDoInitialPruning` already built into `es_part_prune_states`) along with
/// the initial-pruning result. Validates the pruneinfo relids against the plan
/// node's (`elog(ERROR)` on mismatch). Fallible (context init, OOM).
///
/// In C `estate = planstate->state` and the prunestate is fetched with
/// `list_nth(es_part_prune_states, part_prune_index)`, leaving the object in the
/// list while returning an aliasing pointer the node stores. The owned model
/// threads `estate` explicitly (no `state` back-pointer) and *moves* the owned
/// `PgBox<PartitionPruneState>` out of the pool into the caller (the consuming
/// node owns it as `ms_prune_state`), leaving a `None` tombstone so the parallel
/// list indexing is preserved.
pub fn ExecInitPartitionExecPruning<'mcx>(
    mcx: Mcx<'mcx>,
    planstate: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    n_total_subplans: i32,
    part_prune_index: i32,
    relids: Option<&Bitmapset<'_>>,
) -> PgResult<(
    PgBox<'mcx, PartitionPruneState<'mcx>>,
    Option<PgBox<'mcx, Bitmapset<'mcx>>>,
)> {
    let part_prune_index = part_prune_index as usize;

    // pruneinfo = list_nth_node(PartitionPruneInfo, estate->es_part_prune_infos,
    //                           part_prune_index);
    // if (!bms_equal(relids, pruneinfo->relids))
    //     elog(ERROR, "wrong pruneinfo with relids=... ...");
    {
        let pruneinfo_relids =
            raw_to_bms(mcx, &pruneinfo_ref(&estate.es_part_prune_infos[part_prune_index]).relids)?;
        if !bms::bms_equal::call(relids, pruneinfo_relids.as_ref()) {
            return Err(PgError::error(
                "wrong pruneinfo with relids found at part_prune_index contained in plan node",
            ));
        }
    }

    // prunestate = list_nth(estate->es_part_prune_states, part_prune_index);
    // Assert(prunestate != NULL);
    // (The owned model moves the prebuilt box out of the pool; a missing entry
    // is the C `Assert(prunestate != NULL)` — ExecDoInitialPruning must have
    // run first.)
    let mut prunestate = estate.es_part_prune_states[part_prune_index]
        .take()
        .expect("es_part_prune_states[part_prune_index] is NULL (ExecDoInitialPruning not run)");

    // Use the result of initial pruning done by ExecDoInitialPruning().
    let do_initial_prune = prunestate.do_initial_prune;
    let do_exec_prune = prunestate.do_exec_prune;

    let initially_valid_subplans: Option<PgBox<'mcx, Bitmapset<'mcx>>>;
    if do_initial_prune {
        // *initially_valid_subplans = list_nth_node(Bitmapset,
        //         estate->es_part_prune_results, part_prune_index);
        let res = estate.es_part_prune_results[part_prune_index].as_deref();
        initially_valid_subplans = bms::bms_copy::call(mcx, res)?;
    } else {
        // No pruning, so we'll need to initialize all subplans
        // Assert(n_total_subplans > 0);
        debug_assert!(n_total_subplans > 0);
        // *initially_valid_subplans = bms_add_range(NULL, 0, n_total_subplans - 1);
        initially_valid_subplans =
            bms::bms_add_range::call(mcx, None, 0, n_total_subplans - 1)?;
    }

    // if (prunestate->do_exec_prune)
    //     InitExecPartitionPruneContexts(prunestate, planstate,
    //                                    *initially_valid_subplans,
    //                                    n_total_subplans);
    if do_exec_prune {
        InitExecPartitionPruneContexts(
            mcx,
            estate,
            &mut prunestate,
            planstate,
            initially_valid_subplans.as_deref(),
            n_total_subplans,
        )?;
    }

    // return prunestate;
    Ok((prunestate, initially_valid_subplans))
}

/// `CreatePartitionPruneState(estate, pruneinfo, &all_leafpart_rtis)` — build
/// the `PartitionPruneState` for one `PartitionPruneInfo` (the per-hierarchy
/// `PartitionPruningData` / `PartitionedRelPruningData` tree, subplan/subpart
/// maps, and the initial pruning contexts). `pruneinfo` is addressed by its
/// index into `estate->es_part_prune_infos`. On the EXPLAIN-GENERIC skip path,
/// `all_leafpart_rtis` collects every leaf RT index. Fallible.
pub(crate) fn CreatePartitionPruneState<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    pruneinfo_index: usize,
    all_leafpart_rtis: &mut Option<PgBox<'mcx, Bitmapset<'mcx>>>,
) -> PgResult<PartitionPruneState<'mcx>> {
    // Expression context that will be used by partkey_datum_from_expr().
    // ExprContext *econtext = CreateExprContext(estate);
    let econtext = CreateExprContext(estate)?;

    // For data reading, executor always includes detached partitions.
    // if (estate->es_partition_directory == NULL)
    //     estate->es_partition_directory =
    //         CreatePartitionDirectory(estate->es_query_cxt, false);
    if estate.es_partition_directory.0.is_none() {
        let pdir = partdesc_seams::create_partition_directory::call(estate.es_query_cxt, false)?;
        estate.es_partition_directory = pdir;
    }

    // n_part_hierarchies = list_length(pruneinfo->prune_infos);
    // Assert(n_part_hierarchies > 0);
    let n_part_hierarchies = pruneinfo_ref(&estate.es_part_prune_infos[pruneinfo_index])
        .prune_infos
        .len();
    debug_assert!(n_part_hierarchies > 0);

    // prunestate = palloc(...);
    // prunestate->econtext = econtext;
    // prunestate->execparamids = NULL;
    // prunestate->other_subplans = bms_copy(pruneinfo->other_subplans);
    // prunestate->do_initial_prune = false;
    // prunestate->do_exec_prune = false;
    // prunestate->num_partprunedata = n_part_hierarchies;
    let other_subplans = {
        let raw = raw_to_bms(
            mcx,
            &pruneinfo_ref(&estate.es_part_prune_infos[pruneinfo_index]).other_subplans,
        )?;
        bms::bms_copy::call(mcx, raw.as_ref())?
    };
    let mut execparamids: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
    let mut do_initial_prune = false;
    let mut do_exec_prune = false;

    // prunestate->prune_context =
    //     AllocSetContextCreate(CurrentMemoryContext, "Partition Prune", ...);
    let prune_context = estate.es_query_cxt.context().new_child("Partition Prune");

    let mut partprunedata: PgVec<'mcx, Option<PgBox<'mcx, PartitionPruningData<'mcx>>>> =
        vec_with_capacity_in(mcx, n_part_hierarchies)?;

    // i = 0; foreach(lc, pruneinfo->prune_infos)
    for i in 0..n_part_hierarchies {
        // List *partrelpruneinfos = lfirst_node(List, lc);
        // int npartrelpruneinfos = list_length(partrelpruneinfos);
        let npartrelpruneinfos = pruneinfo_ref(&estate.es_part_prune_infos[pruneinfo_index])
            .prune_infos[i]
            .len();

        // prunedata->num_partrelprunedata = npartrelpruneinfos;
        let mut partrelprunedata: PgVec<'mcx, PartitionedRelPruningData<'mcx>> =
            vec_with_capacity_in(mcx, npartrelpruneinfos)?;

        // j = 0; foreach(lc2, partrelpruneinfos)
        for j in 0..npartrelpruneinfos {
            // partrel = ExecGetRangeTableRelation(estate, pinfo->rtindex, false);
            let rtindex = pruneinfo_ref(&estate.es_part_prune_infos[pruneinfo_index]).prune_infos
                [i][j]
                .rtindex;
            let partrel = ExecGetRangeTableRelation(estate, rtindex, false, false)?;

            // partkey = RelationGetPartitionKey(partrel);
            let partkey = partcache_seams::relation_get_partition_key::call(mcx, partrel.alias())?;
            // partdesc = PartitionDirectoryLookup(estate->es_partition_directory, partrel);
            let partdesc = partdesc_seams::partition_directory_lookup::call(
                mcx,
                &mut estate.es_partition_directory,
                partrel.alias(),
            )?;

            // pprune->nparts = partdesc->nparts;
            let nparts = partdesc.nparts;
            // pprune->subplan_map = palloc(sizeof(int) * partdesc->nparts);
            let mut subplan_map: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, nparts as usize)?;
            let subpart_map: PgVec<'mcx, i32>;
            let leafpart_rti_map: PgVec<'mcx, i32>;

            // Initialize the subplan_map and subpart_map; the quick-compare /
            // re-map logic mirrors CreatePartitionPruneState() exactly.
            let pinfo_nparts;
            let pinfo_oids_match;
            {
                let pinfo = &pruneinfo_ref(&estate.es_part_prune_infos[pruneinfo_index])
                    .prune_infos[i][j];
                pinfo_nparts = pinfo.nparts;
                pinfo_oids_match = partdesc.nparts == pinfo.nparts
                    && partdesc.oids.as_slice() == pinfo.relid_map.as_slice();
            }

            if pinfo_oids_match {
                // pprune->subpart_map = pinfo->subpart_map;
                // pprune->leafpart_rti_map = pinfo->leafpart_rti_map;
                // memcpy(pprune->subplan_map, pinfo->subplan_map, ...);
                let pinfo = &pruneinfo_ref(&estate.es_part_prune_infos[pruneinfo_index])
                    .prune_infos[i][j];
                subpart_map = slice_in(mcx, pinfo.subpart_map.as_slice())?;
                leafpart_rti_map = slice_in(mcx, pinfo.leafpart_rti_map.as_slice())?;
                for k in 0..pinfo_nparts as usize {
                    subplan_map.push(pinfo.subplan_map[k]);
                }
            } else {
                // pprune->subpart_map = palloc(sizeof(int) * partdesc->nparts);
                // pprune->leafpart_rti_map = palloc(sizeof(int) * partdesc->nparts);
                let mut sub_map: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, nparts as usize)?;
                let mut rti_map: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, nparts as usize)?;
                // Pre-size subplan_map; we'll index it directly.
                subplan_map.resize(nparts as usize, 0);
                sub_map.resize(nparts as usize, 0);
                rti_map.resize(nparts as usize, 0);

                let pinfo = &pruneinfo_ref(&estate.es_part_prune_infos[pruneinfo_index])
                    .prune_infos[i][j];

                let mut pd_idx: i32 = 0;
                let mut pp_idx: i32 = 0;
                while pp_idx < partdesc.nparts {
                    // Skip any InvalidOid relid_map entries
                    while pd_idx < pinfo.nparts
                        && !oid_is_valid(pinfo.relid_map[pd_idx as usize])
                    {
                        pd_idx += 1;
                    }

                    // recheck:
                    loop {
                        if pd_idx < pinfo.nparts
                            && pinfo.relid_map[pd_idx as usize]
                                == partdesc.oids[pp_idx as usize]
                        {
                            // match...
                            subplan_map[pp_idx as usize] = pinfo.subplan_map[pd_idx as usize];
                            sub_map[pp_idx as usize] = pinfo.subpart_map[pd_idx as usize];
                            rti_map[pp_idx as usize] = pinfo.leafpart_rti_map[pd_idx as usize];
                            pd_idx += 1;
                            break; // continue outer
                        }

                        // Peek ahead for a match further along relid_map.
                        let mut found = false;
                        let mut pd_idx2 = pd_idx + 1;
                        while pd_idx2 < pinfo.nparts {
                            if pinfo.relid_map[pd_idx2 as usize]
                                == partdesc.oids[pp_idx as usize]
                            {
                                pd_idx = pd_idx2;
                                found = true;
                                break;
                            }
                            pd_idx2 += 1;
                        }
                        if found {
                            continue; // goto recheck
                        }

                        // No match anywhere: mark this partition pruned.
                        sub_map[pp_idx as usize] = -1;
                        subplan_map[pp_idx as usize] = -1;
                        rti_map[pp_idx as usize] = 0;
                        break;
                    }
                    pp_idx += 1;
                }
                subpart_map = sub_map;
                leafpart_rti_map = rti_map;
            }

            // pprune->present_parts = bms_copy(pinfo->present_parts);
            let present_parts = {
                let raw = raw_to_bms(
                    mcx,
                    &pruneinfo_ref(&estate.es_part_prune_infos[pruneinfo_index]).prune_infos[i][j]
                        .present_parts,
                )?;
                bms::bms_copy::call(mcx, raw.as_ref())?
            };

            // Build the PartitionedRelPruningData with empty contexts; we fill
            // initial_context below if needed. exec_context is filled later.
            let mut pprune = PartitionedRelPruningData {
                partrel: Some(partrel),
                nparts,
                subplan_map,
                subpart_map,
                leafpart_rti_map,
                present_parts,
                initial_pruning_steps: Opaque(None),
                exec_pruning_steps: Opaque(None),
                initial_context: empty_prune_context(mcx),
                exec_context: empty_prune_context(mcx),
            };

            // pprune->initial_pruning_steps = pinfo->initial_pruning_steps;
            // (carried as the type-erased payload of the Opaque field)
            let (has_initial, has_exec) = {
                let pinfo = &pruneinfo_ref(&estate.es_part_prune_infos[pruneinfo_index])
                    .prune_infos[i][j];
                (
                    !pinfo.initial_pruning_steps.is_empty(),
                    !pinfo.exec_pruning_steps.is_empty(),
                )
            };
            // EXPLAIN (GENERIC_PLAN) skips execution-time pruning.
            let explain_generic =
                (estate.es_top_eflags & EXEC_FLAG_EXPLAIN_GENERIC) != 0;

            if has_initial && !explain_generic {
                // Clone the steps into the Opaque payload (List *). The payload is
                // the `'static`-erased plan-tree form (`Opaque` is `Any`/`'static`).
                let steps: alloc::vec::Vec<PartitionPruneStep<'static>> = clone_steps(
                    &pruneinfo_ref(&estate.es_part_prune_infos[pruneinfo_index]).prune_infos[i][j]
                        .initial_pruning_steps,
                );
                pprune.initial_pruning_steps = Opaque(Some(alloc::boxed::Box::new(steps.clone())));
                // InitPartitionPruneContext(&pprune->initial_context,
                //     pprune->initial_pruning_steps, partdesc, partkey, NULL,
                //     econtext);
                // The init step compiles each step's `Expr` against the `'mcx`
                // EState (`ExecInitExpr`), so re-clone the `'static` plan-tree
                // steps into the executor arena (`mcx`) to match — `PartitionPruneStep`
                // is invariant, so the `'static` form cannot be reused as `'mcx`.
                let steps_mcx: alloc::vec::Vec<PartitionPruneStep<'mcx>> = steps
                    .iter()
                    .map(|s| s.clone_in(mcx))
                    .collect::<PgResult<_>>()?;
                let partkey_ref = partkey
                    .as_deref()
                    .expect("RelationGetPartitionKey returned NULL for a partitioned table");
                InitPartitionPruneContext(
                    mcx,
                    estate,
                    &mut pprune.initial_context,
                    &steps_mcx,
                    partdesc,
                    partkey_ref,
                    None,
                    econtext,
                )?;
                do_initial_prune = true;
            } else if has_initial {
                // Still record the steps so present_parts walk below is gated
                // on the same condition as C (pinfo->initial_pruning_steps).
                let steps = clone_steps(
                    &pruneinfo_ref(&estate.es_part_prune_infos[pruneinfo_index]).prune_infos[i][j]
                        .initial_pruning_steps,
                );
                pprune.initial_pruning_steps = Opaque(Some(alloc::boxed::Box::new(steps)));
            }

            if has_exec && !explain_generic {
                let steps = clone_steps(
                    &pruneinfo_ref(&estate.es_part_prune_infos[pruneinfo_index]).prune_infos[i][j]
                        .exec_pruning_steps,
                );
                pprune.exec_pruning_steps = Opaque(Some(alloc::boxed::Box::new(steps)));
                do_exec_prune = true;
            } else if has_exec {
                let steps = clone_steps(
                    &pruneinfo_ref(&estate.es_part_prune_infos[pruneinfo_index]).prune_infos[i][j]
                        .exec_pruning_steps,
                );
                pprune.exec_pruning_steps = Opaque(Some(alloc::boxed::Box::new(steps)));
            }

            // prunestate->execparamids =
            //     bms_add_members(prunestate->execparamids, pinfo->execparamids);
            {
                let pinfo_execparamids = raw_to_bms(
                    mcx,
                    &pruneinfo_ref(&estate.es_part_prune_infos[pruneinfo_index]).prune_infos[i][j]
                        .execparamids,
                )?;
                execparamids = bms::bms_add_members::call(
                    mcx,
                    execparamids,
                    pinfo_execparamids.as_ref(),
                )?;
            }

            // Return all leaf partition indexes when skipping initial pruning
            // in the EXPLAIN (GENERIC_PLAN) case.
            if has_initial && !do_initial_prune {
                // int part_index = -1;
                // while ((part_index = bms_next_member(pprune->present_parts,
                //                                      part_index)) >= 0)
                let mut part_index: i32 = -1;
                loop {
                    part_index =
                        bms::bms_next_member::call(pprune.present_parts.as_deref(), part_index);
                    if part_index < 0 {
                        break;
                    }
                    // Index rtindex = pprune->leafpart_rti_map[part_index];
                    let rtindex = pprune.leafpart_rti_map[part_index as usize];
                    // if (rtindex)
                    //     *all_leafpart_rtis = bms_add_member(*all_leafpart_rtis, rtindex);
                    if rtindex != 0 {
                        let cur = all_leafpart_rtis.take();
                        *all_leafpart_rtis =
                            Some(bms::bms_add_member::call(mcx, cur, rtindex)?);
                    }
                }
            }

            partrelprunedata.push(pprune);
        }

        let prunedata = PartitionPruningData {
            num_partrelprunedata: npartrelpruneinfos as i32,
            partrelprunedata,
        };
        partprunedata.push(Some(alloc_in(mcx, prunedata)?));
    }

    Ok(PartitionPruneState {
        econtext: Some(econtext),
        execparamids,
        other_subplans,
        prune_context,
        do_initial_prune,
        do_exec_prune,
        num_partprunedata: n_part_hierarchies as i32,
        partprunedata,
    })
}

/// `InitPartitionPruneContext(context, pruning_steps, partdesc, partkey,
/// planstate, econtext)` — initialize a `PartitionPruneContext` for a list of
/// pruning steps: copy the strategy/bounds, allocate the per-step comparison
/// and `ExprState` arrays, and compile the non-Const lookup expressions
/// (through the planstate when available, else with the econtext's params).
/// `econtext` is an id into the EState pool. `partdesc` is taken by value (the
/// owned `PartitionDirectoryLookup` result) so its `boundinfo` can be moved into
/// the context, matching C's `context->boundinfo = partdesc->boundinfo`.
/// Fallible (expression compile, OOM).
#[allow(clippy::too_many_arguments)]
pub(crate) fn InitPartitionPruneContext<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    context: &mut PartitionPruneContext<'mcx>,
    pruning_steps: &[PartitionPruneStep<'mcx>],
    mut partdesc: PgBox<'mcx, PartitionDescData<'mcx>>,
    partkey: &PartitionKeyData<'mcx>,
    planstate: Option<&mut PlanStateData<'mcx>>,
    econtext: EcxtId,
) -> PgResult<()> {
    // n_steps = list_length(pruning_steps);
    let n_steps = pruning_steps.len() as i32;
    // partnatts = partkey->partnatts;
    let partnatts = partkey.partnatts as i32;

    // context->strategy = partkey->strategy;
    context.strategy = partkey.strategy;
    // context->partnatts = partnatts;
    context.partnatts = partnatts;
    // context->nparts = partdesc->nparts;
    context.nparts = partdesc.nparts;
    // context->boundinfo = partdesc->boundinfo;
    // C aliases the relcache PartitionDesc's boundinfo; the owned model moves it
    // out of the freshly looked-up (owned) partdesc into the context.
    context.boundinfo = partdesc.boundinfo.take();
    // context->partcollation = partkey->partcollation;
    context.partcollation = slice_in(mcx, partkey.partcollation.as_slice())?;
    // context->partsupfunc = partkey->partsupfunc; (aliased relcache data)
    context.partsupfunc = Opaque(Some(alloc::boxed::Box::new(
        partkey.partsupfunc.as_slice().to_vec(),
    )));

    // context->stepcmpfuncs = palloc0(sizeof(FmgrInfo) * n_steps * partnatts);
    let cmp_len = (n_steps * partnatts) as usize;
    let mut stepcmpfuncs = vec_with_capacity_in(mcx, cmp_len)?;
    stepcmpfuncs.resize(cmp_len, types_core::fmgr::FmgrInfo::default());
    context.stepcmpfuncs = stepcmpfuncs;

    // context->ppccontext = CurrentMemoryContext;
    context.ppccontext = MemoryContext::new("PartitionPruneContext");
    // context->planstate = planstate;  (executor-owned handle)
    context.planstate = Opaque(None);
    // context->exprcontext = econtext;
    context.exprcontext = Some(econtext);

    // context->exprstates = palloc0(sizeof(ExprState *) * n_steps * partnatts);
    let mut exprstates: PgVec<'mcx, Option<PgBox<'mcx, types_nodes::execexpr::ExprState>>> =
        vec_with_capacity_in(mcx, cmp_len)?;
    for _ in 0..cmp_len {
        exprstates.push(None);
    }

    // Whether the parent plan's PlanState is available decides which compiler
    // to use (ExecInitExpr vs ExecInitExprWithParams).
    let have_planstate = planstate.is_some();
    let mut planstate = planstate;

    // foreach(lc, pruning_steps)
    for step in pruning_steps.iter() {
        // PartitionPruneStepOp *step = (PartitionPruneStepOp *) lfirst(lc);
        // if (!IsA(step, PartitionPruneStepOp)) continue;
        let op = match step {
            PartitionPruneStep::Op(op) => op,
            PartitionPruneStep::Combine(_) => continue,
        };
        let step_id = step.step_id();

        // Assert(list_length(step->exprs) <= partnatts);
        debug_assert!(op.exprs.len() <= partnatts as usize);

        // ListCell *lc2 = list_head(step->exprs);
        let mut lc2: usize = 0;

        // for (keyno = 0; keyno < partnatts; keyno++)
        for keyno in 0..partnatts {
            // if (bms_is_member(keyno, step->nullkeys)) continue;
            let nullkeys_bms = match &op.nullkeys {
                Some(words) => Some(Bitmapset {
                    words: slice_in(mcx, words.as_slice())?,
                }),
                None => None,
            };
            if bms::bms_is_member::call(keyno, nullkeys_bms.as_ref()) {
                continue;
            }

            // if (lc2 != NULL)
            if lc2 < op.exprs.len() {
                // Expr *expr = lfirst(lc2);
                let expr = &op.exprs[lc2];
                // if (!IsA(expr, Const))
                if !matches!(expr, Expr::Const(_)) {
                    // stateidx = PruneCxtStateIdx(partnatts, step->step.step_id, keyno);
                    let stateidx = prune_cxt_state_idx(partnatts, step_id, keyno);
                    if have_planstate {
                        // context->exprstates[stateidx] =
                        //     ExecInitExpr(expr, context->planstate);
                        let ps = planstate
                            .as_deref_mut()
                            .expect("planstate present");
                        exprstates[stateidx] = Some(
                            backend_executor_execExpr_seams::exec_init_expr::call(
                                expr, ps, estate,
                            )?,
                        );
                    } else {
                        // context->exprstates[stateidx] =
                        //     ExecInitExprWithParams(expr, econtext->ecxt_param_list_info);
                        exprstates[stateidx] = Some(
                            backend_executor_execExpr_seams::exec_init_expr_with_params::call(
                                expr, econtext, estate,
                            )?,
                        );
                    }
                }
                // lc2 = lnext(step->exprs, lc2);
                lc2 += 1;
            }
        }
    }

    context.exprstates = exprstates;
    Ok(())
}

/// `InitExecPartitionPruneContexts(prunestate, parent_plan,
/// initially_valid_subplans, n_total_subplans)` — initialize the deferred
/// exec-pruning contexts of a `PartitionPruneState` (those needing the parent
/// plan's `PlanState`) and re-sequence the subplan/present-part maps to account
/// for subplans removed during initial pruning. Fallible (context init, OOM).
pub(crate) fn InitExecPartitionPruneContexts<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    prunestate: &mut PartitionPruneState<'mcx>,
    parent_plan: &mut PlanStateData<'mcx>,
    initially_valid_subplans: Option<&Bitmapset<'_>>,
    n_total_subplans: i32,
) -> PgResult<()> {
    // Assert(prunestate->do_exec_prune);
    debug_assert!(prunestate.do_exec_prune);

    // int *new_subplan_indexes = NULL;
    // bool fix_subplan_map = false;
    let mut new_subplan_indexes: Option<alloc::vec::Vec<i32>> = None;
    let mut fix_subplan_map = false;

    // if (bms_num_members(initially_valid_subplans) < n_total_subplans)
    if bms::bms_num_members::call(initially_valid_subplans) < n_total_subplans {
        fix_subplan_map = true;
        // new_subplan_indexes = palloc0(sizeof(int) * n_total_subplans);
        let mut nsi = alloc::vec![0i32; n_total_subplans as usize];
        // newidx = 1; i = -1;
        let mut newidx = 1;
        let mut i = -1;
        // while ((i = bms_next_member(initially_valid_subplans, i)) >= 0)
        loop {
            i = bms::bms_next_member::call(initially_valid_subplans, i);
            if i < 0 {
                break;
            }
            debug_assert!(i < n_total_subplans);
            nsi[i as usize] = newidx;
            newidx += 1;
        }
        new_subplan_indexes = Some(nsi);
    }

    init_exec_contexts_inner(
        mcx,
        estate,
        prunestate,
        parent_plan,
        new_subplan_indexes.as_deref(),
        fix_subplan_map,
        n_total_subplans,
    )
}

#[allow(clippy::too_many_arguments)]
fn init_exec_contexts_inner<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    prunestate: &mut PartitionPruneState<'mcx>,
    parent_plan: &mut PlanStateData<'mcx>,
    new_subplan_indexes: Option<&[i32]>,
    fix_subplan_map: bool,
    n_total_subplans: i32,
) -> PgResult<()> {
    let econtext = prunestate.econtext.expect("prunestate->econtext");

    // for (i = 0; i < prunestate->num_partprunedata; i++)
    for i in 0..prunestate.num_partprunedata as usize {
        let num_rel = prunestate.partprunedata[i]
            .as_ref()
            .expect("partprunedata[i]")
            .num_partrelprunedata;

        // for (j = num_partrelprunedata - 1; j >= 0; j--)  (back to front)
        for j in (0..num_rel as usize).rev() {
            // Initialize PartitionPruneContext for exec pruning, if needed.
            let has_exec_steps = {
                let prunedata = prunestate.partprunedata[i].as_ref().unwrap();
                prunedata.partrelprunedata[j].exec_pruning_steps.0.is_some()
            };
            if has_exec_steps {
                // partkey = RelationGetPartitionKey(pprune->partrel);
                // partdesc = PartitionDirectoryLookup(es_partition_directory, partrel);
                let partrel = {
                    let prunedata = prunestate.partprunedata[i].as_ref().unwrap();
                    prunedata.partrelprunedata[j]
                        .partrel
                        .as_ref()
                        .expect("pprune->partrel")
                        .alias()
                };
                let partkey =
                    partcache_seams::relation_get_partition_key::call(mcx, partrel.alias())?;
                let partdesc = partdesc_seams::partition_directory_lookup::call(
                    mcx,
                    &mut estate.es_partition_directory,
                    partrel.alias(),
                )?;
                let steps: alloc::vec::Vec<PartitionPruneStep<'static>> = downcast_steps(
                    &prunestate.partprunedata[i].as_ref().unwrap().partrelprunedata[j]
                        .exec_pruning_steps,
                );
                // `InitPartitionPruneContext` compiles each step's `Expr` against
                // the `'mcx` EState; re-clone the `'static`-erased steps into the
                // executor arena (`PartitionPruneStep` is invariant in `'mcx`).
                let steps: alloc::vec::Vec<PartitionPruneStep<'mcx>> = steps
                    .iter()
                    .map(|s| s.clone_in(mcx))
                    .collect::<PgResult<_>>()?;

                // InitPartitionPruneContext(&pprune->exec_context, ...,
                //     partdesc, partkey, parent_plan, prunestate->econtext);
                // Take the context out to avoid aliasing prunestate while we
                // pass estate too.
                let mut exec_context = core::mem::replace(
                    &mut prunestate.partprunedata[i].as_mut().unwrap().partrelprunedata[j]
                        .exec_context,
                    empty_prune_context(mcx),
                );
                let partkey_ref = partkey
                    .as_deref()
                    .expect("RelationGetPartitionKey returned NULL");
                let r = InitPartitionPruneContext(
                    mcx,
                    estate,
                    &mut exec_context,
                    &steps,
                    partdesc,
                    partkey_ref,
                    Some(parent_plan),
                    econtext,
                );
                prunestate.partprunedata[i].as_mut().unwrap().partrelprunedata[j].exec_context =
                    exec_context;
                r?;
            }

            // if (!fix_subplan_map) continue;
            if !fix_subplan_map {
                continue;
            }
            let nsi = new_subplan_indexes.expect("new_subplan_indexes present");

            // bms_free(pprune->present_parts); pprune->present_parts = NULL;
            {
                let old = prunestate.partprunedata[i].as_mut().unwrap().partrelprunedata[j]
                    .present_parts
                    .take();
                bms::bms_free::call(old);
            }

            // for (k = 0; k < nparts; k++)
            let nparts = prunestate.partprunedata[i].as_ref().unwrap().partrelprunedata[j].nparts;
            for k in 0..nparts as usize {
                let oldidx = prunestate.partprunedata[i].as_ref().unwrap().partrelprunedata[j]
                    .subplan_map[k];
                if oldidx >= 0 {
                    // Assert(oldidx < n_total_subplans);
                    debug_assert!(oldidx < n_total_subplans);
                    // pprune->subplan_map[k] = new_subplan_indexes[oldidx] - 1;
                    let newval = nsi[oldidx as usize] - 1;
                    prunestate.partprunedata[i].as_mut().unwrap().partrelprunedata[j].subplan_map
                        [k] = newval;
                    // if (new_subplan_indexes[oldidx] > 0) present_parts += k;
                    if nsi[oldidx as usize] > 0 {
                        let cur = prunestate.partprunedata[i].as_mut().unwrap()
                            .partrelprunedata[j]
                            .present_parts
                            .take();
                        prunestate.partprunedata[i].as_mut().unwrap().partrelprunedata[j]
                            .present_parts = Some(bms::bms_add_member::call(mcx, cur, k as i32)?);
                    }
                } else {
                    // else if ((subidx = pprune->subpart_map[k]) >= 0)
                    let subidx = prunestate.partprunedata[i].as_ref().unwrap()
                        .partrelprunedata[j]
                        .subpart_map[k];
                    if subidx >= 0 {
                        // subprune = &prunedata->partrelprunedata[subidx];
                        // if (!bms_is_empty(subprune->present_parts)) present_parts += k;
                        let subprune_empty = bms::bms_is_empty::call(
                            prunestate.partprunedata[i].as_ref().unwrap().partrelprunedata
                                [subidx as usize]
                                .present_parts
                                .as_deref(),
                        );
                        if !subprune_empty {
                            let cur = prunestate.partprunedata[i].as_mut().unwrap()
                                .partrelprunedata[j]
                                .present_parts
                                .take();
                            prunestate.partprunedata[i].as_mut().unwrap().partrelprunedata[j]
                                .present_parts =
                                Some(bms::bms_add_member::call(mcx, cur, k as i32)?);
                        }
                    }
                }
            }
        }
    }

    // If we fixed subplan maps, recompute other_subplans too.
    if fix_subplan_map {
        let nsi = new_subplan_indexes.expect("new_subplan_indexes present");
        // new_other_subplans = NULL; i = -1;
        let mut new_other_subplans: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
        let mut i = -1;
        // while ((i = bms_next_member(prunestate->other_subplans, i)) >= 0)
        loop {
            i = bms::bms_next_member::call(prunestate.other_subplans.as_deref(), i);
            if i < 0 {
                break;
            }
            // new_other_subplans = bms_add_member(new_other_subplans,
            //                                     new_subplan_indexes[i] - 1);
            new_other_subplans = Some(bms::bms_add_member::call(
                mcx,
                new_other_subplans,
                nsi[i as usize] - 1,
            )?);
        }
        // bms_free(prunestate->other_subplans);
        let old = prunestate.other_subplans.take();
        bms::bms_free::call(old);
        prunestate.other_subplans = new_other_subplans;
        // pfree(new_subplan_indexes); — owned by the caller's Vec; dropped there.
    }

    Ok(())
}

/// `ExecFindMatchingSubPlans(prunestate, initial_prune, NULL)` (the trimmed
/// seam) — determine which subplans match the pruning steps for the current
/// comparison values. This is the public seam the consuming node calls on its
/// owned `PartitionPruneState`; the MergeAppend caller always passes `NULL` for
/// the C `validsubplan_rtis` out-parameter, so this wrapper drops it. The
/// `ExecDoInitialPruning` internal call (which *does* want the RT indexes) goes
/// through [`find_matching_subplans`]. Fallible (pruning evaluation, OOM).
pub fn ExecFindMatchingSubPlans<'mcx>(
    mcx: Mcx<'mcx>,
    prunestate: &mut PartitionPruneState<'mcx>,
    estate: &mut EStateData<'mcx>,
    initial_prune: bool,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    find_matching_subplans(mcx, estate, prunestate, initial_prune, None)
}

/// The full `ExecFindMatchingSubPlans(prunestate, initial_prune,
/// &validsubplan_rtis)` worker (execPartition.c). `validsubplan_rtis` must be
/// `Some` during initial pruning (collects the leaf RT indexes whose subnodes
/// will run); the trimmed seam passes `None`. Operates directly on the
/// caller-owned `&mut PartitionPruneState` (C reaches everything through the
/// prunestate pointer; estate is threaded for the `get_matching_partitions`
/// seam, which the owned model takes `&mut EStateData` for). Fallible.
pub(crate) fn find_matching_subplans<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    prunestate: &mut PartitionPruneState<'mcx>,
    initial_prune: bool,
    validsubplan_rtis: Option<&mut Option<PgBox<'mcx, Bitmapset<'mcx>>>>,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    // Assert(initial_prune || prunestate->do_exec_prune);
    debug_assert!(initial_prune || prunestate.do_exec_prune);
    // Assert(validsubplan_rtis != NULL || !initial_prune);
    debug_assert!(validsubplan_rtis.is_some() || !initial_prune);

    // Switch to a temp context to avoid leaking memory (prune_context). In the
    // owned model the temp allocations go through `mcx`; the surviving result
    // is what C copies out before MemoryContextReset.
    let mut result: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;

    let mut rtis_local: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
    let want_rtis = validsubplan_rtis.is_some();

    let inner = (|| -> PgResult<()> {
        // for (i = 0; i < prunestate->num_partprunedata; i++)
        for i in 0..prunestate.num_partprunedata as usize {
            // pprune = &prunedata->partrelprunedata[0];
            // find_matching_subplans_recurse(prunedata, pprune, initial_prune,
            //                                &result, validsubplan_rtis);
            let mut prunedata = prunestate.partprunedata[i].take().expect("partprunedata[i]");
            let r = find_matching_subplans_recurse(
                mcx,
                estate,
                &mut prunedata,
                0,
                initial_prune,
                &mut result,
                if want_rtis {
                    Some(&mut rtis_local)
                } else {
                    None
                },
            );
            prunestate.partprunedata[i] = Some(prunedata);
            r?;

            // if (!initial_prune && pprune->exec_pruning_steps)
            //     ResetExprContext(pprune->exec_context.exprcontext);
            // (the exec ExprContext reset is owned by execUtils; in the owned
            // model the per-tuple context resets on its own reset cycle, and
            // the seam does not expose ResetExprContext for a foreign id —
            // this is a memory-hygiene no-op here, not a logic step.)
        }
        Ok(())
    })();

    // Add in any subplans that partition pruning didn't account for.
    // result = bms_add_members(result, prunestate->other_subplans);
    let add_result = inner.and_then(|_| {
        let cur = result.take();
        result = bms::bms_add_members::call(mcx, cur, prunestate.other_subplans.as_deref())?;
        Ok(())
    });

    // Copy result out of the temp context before we reset it.
    // result = bms_copy(result);
    // if (validsubplan_rtis) *validsubplan_rtis = bms_copy(*validsubplan_rtis);
    let final_result = add_result.and_then(|_| {
        let out = bms::bms_copy::call(mcx, result.as_deref())?;
        if let Some(slot) = validsubplan_rtis {
            *slot = bms::bms_copy::call(mcx, rtis_local.as_deref())?;
        }
        Ok(out)
    });

    // MemoryContextReset(prunestate->prune_context);
    prunestate.prune_context.reset();

    final_result
}

/// `find_matching_subplans_recurse(prunedata, pprune, initial_prune,
/// &validsubplans, &validsubplan_rtis)` — recursive worker for
/// `ExecFindMatchingSubPlans`: add valid (non-prunable) subplan IDs (and, when
/// requested, leaf RT indexes) to the accumulators, recursing into
/// sub-partitioned levels. Guards against overly deep hierarchies via
/// `check_stack_depth` (the partprune seam owner / stack-depth seam), so
/// fallible.
#[allow(clippy::too_many_arguments)]
pub(crate) fn find_matching_subplans_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    prunedata: &mut PartitionPruningData<'mcx>,
    pprune_index: usize,
    initial_prune: bool,
    validsubplans: &mut Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    mut validsubplan_rtis: Option<&mut Option<PgBox<'mcx, Bitmapset<'mcx>>>>,
) -> PgResult<()> {
    // Guard against stack overflow due to overly deep partition hierarchy.
    // check_stack_depth();
    stack_depth_seams::check_stack_depth::call()?;

    // Decide the partition set for this level.
    //   if (initial_prune && pprune->initial_pruning_steps)
    //       partset = get_matching_partitions(&pprune->initial_context, ...);
    //   else if (!initial_prune && pprune->exec_pruning_steps)
    //       partset = get_matching_partitions(&pprune->exec_context, ...);
    //   else
    //       partset = pprune->present_parts;
    let has_initial = prunedata.partrelprunedata[pprune_index]
        .initial_pruning_steps
        .0
        .is_some();
    let has_exec = prunedata.partrelprunedata[pprune_index]
        .exec_pruning_steps
        .0
        .is_some();

    // partset is owned (a copy) in the present_parts branch and owned in the
    // get_matching_partitions branches, so no borrow of prunedata is held
    // across the recursion / mutation in the loop below.
    let partset: Option<PgBox<'mcx, Bitmapset<'mcx>>>;
    if initial_prune && has_initial {
        // Extract the step list (owned) before borrowing initial_context mut.
        let steps =
            downcast_steps(&prunedata.partrelprunedata[pprune_index].initial_pruning_steps);
        partset = partprune_seams::get_matching_partitions::call(
            mcx,
            &mut prunedata.partrelprunedata[pprune_index].initial_context,
            &steps,
            estate,
        )?;
    } else if !initial_prune && has_exec {
        let steps =
            downcast_steps(&prunedata.partrelprunedata[pprune_index].exec_pruning_steps);
        partset = partprune_seams::get_matching_partitions::call(
            mcx,
            &mut prunedata.partrelprunedata[pprune_index].exec_context,
            &steps,
            estate,
        )?;
    } else {
        partset = bms::bms_copy::call(
            mcx,
            prunedata.partrelprunedata[pprune_index].present_parts.as_deref(),
        )?;
    }

    // Translate partset into subplan indexes.
    // i = -1; while ((i = bms_next_member(partset, i)) >= 0)
    let mut idx: i32 = -1;
    loop {
        idx = bms::bms_next_member::call(partset.as_deref(), idx);
        if idx < 0 {
            break;
        }
        let i = idx as usize;
        let subplan = prunedata.partrelprunedata[pprune_index].subplan_map[i];
        // if (pprune->subplan_map[i] >= 0)
        if subplan >= 0 {
            // *validsubplans = bms_add_member(*validsubplans, pprune->subplan_map[i]);
            let cur = validsubplans.take();
            *validsubplans = Some(bms::bms_add_member::call(mcx, cur, subplan)?);

            // Only report leaf partitions.
            // if (validsubplan_rtis && pprune->leafpart_rti_map[i])
            //     *validsubplan_rtis = bms_add_member(*validsubplan_rtis,
            //                                         pprune->leafpart_rti_map[i]);
            let rti = prunedata.partrelprunedata[pprune_index].leafpart_rti_map[i];
            if rti != 0 {
                if let Some(slot) = validsubplan_rtis.as_deref_mut() {
                    let cur = slot.take();
                    *slot = Some(bms::bms_add_member::call(mcx, cur, rti)?);
                }
            }
        } else {
            // int partidx = pprune->subpart_map[i];
            let partidx = prunedata.partrelprunedata[pprune_index].subpart_map[i];
            // if (partidx >= 0)
            if partidx >= 0 {
                //     find_matching_subplans_recurse(prunedata,
                //         &prunedata->partrelprunedata[partidx], ...);
                find_matching_subplans_recurse(
                    mcx,
                    estate,
                    prunedata,
                    partidx as usize,
                    initial_prune,
                    validsubplans,
                    validsubplan_rtis.as_deref_mut(),
                )?;
            }
            // else: planner already pruned all sub-partitions; ignore.
        }
    }

    // The owned `partset` (when from get_matching_partitions) drops here; in C
    // it lives in prune_context and is reclaimed by the later reset.
    drop(partset);
    Ok(())
}

/* ---------------------------------------------------------------------------
 * Local helpers.
 * ------------------------------------------------------------------------- */

/// `OidIsValid(oid)`.
fn oid_is_valid(oid: Oid) -> bool {
    types_core::primitive::OidIsValid(oid)
}

/// Deep-clone a pruning-step list (C: the steps are aliased from the plan; the
/// owned-model `Opaque` payload carries an independent copy).
fn clone_steps<'a>(steps: &'_ [PartitionPruneStep<'a>]) -> alloc::vec::Vec<PartitionPruneStep<'a>> {
    steps.to_vec()
}

/// Downcast an `&Opaque` pruning-steps payload to the step list.
fn downcast_steps(o: &Opaque) -> alloc::vec::Vec<PartitionPruneStep<'static>> {
    // The `Opaque` payload is a type-erased `Any` (`'static`-bound); the stored
    // step list is the `'static`-erased plan-tree form.
    o.0.as_ref()
        .expect("pruning_steps Opaque is NULL")
        .downcast_ref::<alloc::vec::Vec<PartitionPruneStep<'static>>>()
        .expect("pruning_steps Opaque is not a step list")
        .clone()
}

/// An empty `PartitionPruneContext` placeholder (the C struct is embedded and
/// zero-initialized by the enclosing palloc; fields are filled by
/// `InitPartitionPruneContext`).
fn empty_prune_context<'mcx>(mcx: Mcx<'mcx>) -> PartitionPruneContext<'mcx> {
    PartitionPruneContext {
        strategy: types_nodes::partition::PartitionStrategy::List,
        partnatts: 0,
        nparts: 0,
        boundinfo: None,
        partcollation: PgVec::new_in(mcx),
        partsupfunc: Opaque(None),
        stepcmpfuncs: PgVec::new_in(mcx),
        ppccontext: MemoryContext::new("PartitionPruneContext"),
        planstate: Opaque(None),
        exprcontext: None,
        exprstates: PgVec::new_in(mcx),
    }
}

extern crate alloc;
