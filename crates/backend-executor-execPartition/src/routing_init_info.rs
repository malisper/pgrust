//! `ExecInitPartitionInfo` — split out of the `routing_setup` family because
//! the C function (execPartition.c L501-985, ~479 lines) is large enough to own
//! its own module: it locks the partition, builds its `ResultRelInfo`, validates
//! it as an INSERT/UPDATE target, opens partition indices, and builds the WITH
//! CHECK OPTION / RETURNING / ON CONFLICT / MERGE state before storing it in the
//! `proute->partitions` array and the EState's tuple-routing list.

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodes::CmdType;
use types_nodes::{EStateData, ModifyTableState, ResultRelInfo, RriId};
use types_storage::lock::RowExclusiveLock;

use crate::{PartitionDispatchId, PartitionTupleRouting};

/// `ExecInitPartitionInfo(mtstate, estate, proute, dispatch, rootResultRelInfo,
/// partidx)` — lock the partition, build its `ResultRelInfo`, and store it in
/// the next free slot of `proute->partitions`. Returns the new `ResultRelInfo`
/// id. Fallible (table open, index open, expression compilation, OOM).
pub(crate) fn ExecInitPartitionInfo<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    proute: &mut PartitionTupleRouting<'mcx>,
    dispatch: PartitionDispatchId,
    root_result_rel_info: RriId,
    partidx: i32,
) -> PgResult<RriId> {
    // ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
    //
    // The `ModifyTable` *plan* node (nodeModifyTable.h) — distinct from the
    // ported `ModifyTableState` — is the unported neighbor's type. Each field of
    // `node` this function reads is exposed by a nodeModifyTable seam: a plain
    // read (`exec_get_on_conflict_action` for the `CheckValidResultRel` argument
    // below) for the scalar fields, and one cohesive per-command leg seam for
    // each list-driven block of the tail (WITH CHECK OPTION / RETURNING / ON
    // CONFLICT / MERGE — those read the plan node's lists and the
    // `OnConflictSetState` / `WithCheckOption` / `MergeAction` node types and
    // write the leaf `ResultRelInfo`'s per-command fields, all nodeModifyTable's).
    // execPartition keeps its own prologue + block ordering here; the seams
    // panic until nodeModifyTable lands.
    let on_conflict_action =
        backend_executor_nodeModifyTable_seams::exec_get_on_conflict_action::call(mtstate);

    // Oid partOid = dispatch->partdesc->oids[partidx];
    let part_oid = {
        let pd = &proute.partition_dispatch_info[dispatch];
        let partdesc = pd
            .partdesc
            .as_ref()
            .expect("PartitionDispatch.partdesc set");
        partdesc.oids[partidx as usize]
    };

    // int       firstVarno = mtstate->resultRelInfo[0].ri_RangeTableIndex;
    // Relation  firstResultRel = mtstate->resultRelInfo[0].ri_RelationDesc;
    //
    // These reference the "first" result relation, used as the attno-mapping
    // reference for the WCO / RETURNING / ON CONFLICT / MERGE Var translation.
    // Read them up front exactly as the C prologue does (before any branch).
    let first_rri_id = mtstate.resultRelInfo[0];
    let first_varno = estate.result_rel(first_rri_id).ri_RangeTableIndex;
    // `firstResultRel` is `resultRelInfo[0].ri_RelationDesc`; the per-command
    // legs that need it (build_attrmap_by_name against the first rel's tupdesc)
    // take the first result rel by its EState-pool id so they can read the live
    // relation themselves.
    let first_result_rel = first_rri_id;

    // oldcxt = MemoryContextSwitchTo(proute->memcxt);
    //
    // The owned model threads `mcx` (proute->memcxt) into each allocating call
    // rather than switching an ambient current context, so there is no switch
    // to perform here; the C `oldcxt`/restore pair collapses away.

    // partrel = table_open(partOid, RowExclusiveLock);
    let partrel =
        backend_access_common_relation_seams::relation_open::call(mcx, part_oid, RowExclusiveLock)?;

    // leaf_part_rri = makeNode(ResultRelInfo);
    let mut leaf_part_rri = ResultRelInfo::default();

    // InitResultRelInfo(leaf_part_rri, partrel, 0, rootResultRelInfo,
    //                   estate->es_instrument);
    let instrument = estate.es_instrument;
    backend_executor_execMain_seams::init_result_rel_info::call(
        mcx,
        &mut leaf_part_rri,
        partrel.alias(),
        0,
        Some(root_result_rel_info),
        instrument,
    )?;

    // Now that the ResultRelInfo is filled, give it its EState-pool identity so
    // the owner seams that operate on it (CheckValidResultRel, ExecInitRoutingInfo)
    // can address it by id, and so it can be found later off
    // es_tuple_routing_result_relations.
    let leaf_part_rri_id = estate.add_result_rel(leaf_part_rri)?;

    // Verify result relation is a valid target for an INSERT.  An UPDATE of a
    // partition-key becomes a DELETE+INSERT operation, so this check is still
    // required when the operation is CMD_UPDATE.
    //
    // CheckValidResultRel(leaf_part_rri, CMD_INSERT,
    //                     node ? node->onConflictAction : ONCONFLICT_NONE, NIL);
    backend_executor_execMain_seams::check_valid_result_rel::call(
        estate,
        leaf_part_rri_id,
        CmdType::CMD_INSERT,
        on_conflict_action,
        &[], /* mergeActions: tuple routing is INSERT-only */
    )?;

    // Open partition indices.  The user may have asked to check for conflicts
    // within this leaf partition and do "nothing" instead of throwing an error.
    // Be prepared in that case by initializing the index information needed by
    // ExecInsert() to perform speculative insertions.
    //
    // if (partrel->rd_rel->relhasindex &&
    //     leaf_part_rri->ri_IndexRelationDescs == NULL)
    //     ExecOpenIndices(leaf_part_rri,
    //                     (node != NULL &&
    //                      node->onConflictAction != ONCONFLICT_NONE));
    //
    // The `relhasindex` read off the partition's pg_class form and the
    // `ExecOpenIndices` callee (execIndexing.c) are the unported owner's; the
    // gating and the `speculative` argument computation live with nodeModifyTable's
    // view of its plan node, so route the whole leg through its seam (panics until
    // nodeModifyTable + execIndexing land).
    backend_executor_nodeModifyTable_seams::exec_open_partition_indices::call(
        mcx,
        mtstate,
        estate,
        leaf_part_rri_id,
    )?;

    // Build WITH CHECK OPTION constraints for the partition.  Note that we
    // didn't build the withCheckOptionList for partitions within the planner,
    // but simple translation of varattnos will suffice.  This only occurs for
    // the INSERT case or in the case of UPDATE/MERGE tuple routing where we
    // didn't find a result rel to reuse.
    //
    // if (node && node->withCheckOptionLists != NIL) { ... }
    //
    // The block reads the `ModifyTable` plan node's `withCheckOptionLists` and
    // builds `WithCheckOption` exprs into `leaf_part_rri->ri_WithCheckOptions` /
    // `ri_WithCheckOptionExprs` — both the plan-node list and the per-command
    // ResultRelInfo fields are nodeModifyTable's, so the leg is its seam (a no-op
    // when the plan has no WCO lists).
    backend_executor_nodeModifyTable_seams::exec_init_partition_with_check_options::call(
        mcx,
        mtstate,
        estate,
        leaf_part_rri_id,
        first_varno,
        first_result_rel,
    )?;

    // Build the RETURNING projection for the partition.  Note that we didn't
    // build the returningList for partitions within the planner, but simple
    // translation of varattnos will suffice.  This only occurs for the INSERT
    // case or in the case of UPDATE/MERGE tuple routing where we didn't find a
    // result rel to reuse.
    //
    // if (node && node->returningLists != NIL) { ... }
    //
    // Reads the plan node's `returningLists` and builds `ri_returningList` /
    // `ri_projectReturning` (using mtstate->ps's result slot + expr context) —
    // nodeModifyTable's plan node and per-command fields, so its seam (a no-op
    // when the plan has no RETURNING lists).
    backend_executor_nodeModifyTable_seams::exec_init_partition_returning::call(
        mcx,
        mtstate,
        estate,
        leaf_part_rri_id,
        first_varno,
        first_result_rel,
    )?;

    // Set up information needed for routing tuples to the partition.
    //
    // ExecInitRoutingInfo(mtstate, estate, proute, dispatch,
    //                     leaf_part_rri, partidx, false);
    crate::routing_setup::ExecInitRoutingInfo(
        mcx,
        mtstate,
        estate,
        proute,
        dispatch,
        leaf_part_rri_id,
        partidx,
        false,
    )?;

    // If there is an ON CONFLICT clause, initialize state for it.
    //
    // if (node && node->onConflictAction != ONCONFLICT_NONE) { ... }
    //
    // Maps the root's arbiter index list to the partition's, checks the
    // `elog(ERROR, "invalid arbiter index list")` invariant, stores
    // `ri_onConflictArbiterIndexes`, and (for ONCONFLICT_UPDATE) builds the
    // `OnConflictSetState` — all driven off the plan node and ResultRelInfo
    // per-command fields, so nodeModifyTable's seam (a no-op when no ON CONFLICT
    // clause).
    backend_executor_nodeModifyTable_seams::exec_init_partition_on_conflict::call(
        mcx,
        mtstate,
        estate,
        leaf_part_rri_id,
        root_result_rel_info,
        first_varno,
        first_result_rel,
    )?;

    // Since we've just initialized this ResultRelInfo, it's not in any list
    // attached to the estate as yet.  Add it, so that it can be found later.
    //
    // Note that the entries in this list appear in no predetermined order,
    // because partition result rels are initialized as and when they're needed.
    //
    // MemoryContextSwitchTo(estate->es_query_cxt);
    // estate->es_tuple_routing_result_relations =
    //     lappend(estate->es_tuple_routing_result_relations, leaf_part_rri);
    let q = estate.es_query_cxt;
    estate
        .es_tuple_routing_result_relations
        .try_reserve(1)
        .map_err(|_| q.oom(core::mem::size_of::<RriId>()))?;
    estate
        .es_tuple_routing_result_relations
        .push(leaf_part_rri_id);

    // Initialize information about this partition that's needed to handle MERGE.
    // We take the "first" result relation's mergeActionList as reference and make
    // a copy for this relation, converting stuff that references attribute
    // numbers to match this relation's.
    //
    // if (node && node->operation == CMD_MERGE) { ... }
    //
    // Reads the plan node's `mergeActionLists` / `mergeJoinConditions` and builds
    // the partition's `MergeActionState`s, `ri_MergeJoinCondition`, and (when
    // `!ri_projectNewInfoValid`) the merge tuple slots — all nodeModifyTable's
    // plan node and per-command fields, so its seam (a no-op when not MERGE).
    backend_executor_nodeModifyTable_seams::exec_init_partition_merge::call(
        mcx,
        mtstate,
        estate,
        leaf_part_rri_id,
        first_varno,
        first_result_rel,
    )?;

    // MemoryContextSwitchTo(oldcxt);  -- no ambient context to restore.

    // return leaf_part_rri;
    let _ = &partrel;
    Ok(leaf_part_rri_id)
}
