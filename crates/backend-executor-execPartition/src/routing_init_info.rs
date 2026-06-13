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
    // ported `ModifyTableState` — is not in `types-nodes` yet, so the only field
    // of `node` reachable here is `onConflictAction`, exposed by the owner's
    // `exec_get_on_conflict_action` seam (which returns `ONCONFLICT_NONE` when
    // the plan pointer is the C `NULL`). Every other field the conditional tail
    // reads (operation, withCheckOptionLists, returningLists, onConflictSet/
    // Cols/Where, mergeActionLists, mergeJoinConditions) has no representation
    // until the plan node lands; see the gated block below.
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
    let _first_varno = estate.result_rel(first_rri_id).ri_RangeTableIndex;
    let _first_result_rel = estate
        .result_rel(first_rri_id)
        .ri_RelationDesc
        .as_ref()
        .map(|r| r.alias());

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
    // `relhasindex` is not carried on the trimmed FormData_pg_class and no
    // `exec_open_indices` seam is authored, so this conditional cannot run yet;
    // the conditional per-command tail below (which the missing ModifyTable plan
    // node blocks outright) covers the rest of the body.

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

    // Since we've just initialized this ResultRelInfo, it's not in any list
    // attached to the estate as yet.  Add it, so that it can be found later.
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

    // The remaining per-command setup — building the WITH CHECK OPTION exprs,
    // the RETURNING projection, the ON CONFLICT (arbiter index mapping + DO
    // UPDATE SET projection / WHERE), and the MERGE action states — all branch
    // on the `ModifyTable` *plan* node's lists (`node->withCheckOptionLists`,
    // `node->returningLists`, `node->onConflictAction`, `node->operation ==
    // CMD_MERGE`) and write them into `ResultRelInfo` per-command fields
    // (ri_WithCheckOptions, ri_returningList, ri_projectReturning, ri_onConflict,
    // ri_MergeActions, ri_MergeJoinCondition, ri_newTupleSlot,
    // ri_projectNewInfoValid) that the trimmed executor type does not carry,
    // plus owner functions with no seam authored (map_variable_attnos,
    // build_attrmap_by_name, ExecBuildProjectionInfo on a returning list,
    // ExecBuildUpdateProjection, ExecInitMergeTupleSlots, RelationGetIndexList).
    //
    // The ModifyTable plan node is the unported neighbor's type; not one of its
    // gating lists can be read here, so the port cannot even decide whether each
    // conditional block fires — silently treating them as NIL would diverge from
    // C on any partitioned INSERT/UPDATE/MERGE carrying WCO / RETURNING / ON
    // CONFLICT / MERGE clauses. Per the mirror-PG-and-panic discipline, route the
    // whole tail through a loud panic until nodeModifyTable lands its plan-node
    // type and the per-command seams, rather than restructure around it or stub
    // it out.
    let _ = &partrel;
    panic!(
        "ExecInitPartitionInfo: per-command tail (WITH CHECK OPTION / RETURNING / \
         ON CONFLICT / MERGE) needs the ModifyTable plan node, the trimmed \
         ResultRelInfo's per-command fields, and owners (map_variable_attnos, \
         build_attrmap_by_name, ExecBuildUpdateProjection, ExecInitMergeTupleSlots, \
         RelationGetIndexList) that have not landed"
    );

    // MemoryContextSwitchTo(oldcxt);  -- no ambient context to restore.

    // return leaf_part_rri;
    // (unreachable until the per-command tail above is portable)
    #[allow(unreachable_code)]
    Ok(leaf_part_rri_id)
}
