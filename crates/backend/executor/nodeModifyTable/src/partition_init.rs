//! The per-leg `ResultRelInfo` initialization blocks of
//! `ExecInitPartitionInfo` (executor/execPartition.c L501-985) that the CATALOG
//! standing-adjudicated as owned by nodeModifyTable: they read the
//! `ModifyTable` plan node (`withCheckOptionLists` / `returningLists` /
//! `onConflictAction` / `arbiterIndexes` / `onConflictSet` / `mergeActionLists`
//! / ...) and write ModifyTable-meaning `ResultRelInfo` fields
//! (`ri_WithCheckOptions`, `ri_returningList`, `ri_onConflictArbiterIndexes`,
//! `ri_onConflict`, `ri_MergeActions`, ...) for a leaf partition reached by
//! tuple routing.
//!
//! Each function owns the gating (the `if node && node->X != NIL` guard), the
//! reference-list selection (`linitial(node->...Lists)`), and — for ON CONFLICT
//! — the arbiter-index mapping loop and its length invariant. The genuinely
//! foreign callees (index open, attmap build + `map_variable_attnos` rewrite,
//! `ExecInitQual` / `ExecBuildProjectionInfo` / `ExecBuildUpdateProjection`,
//! `table_slot_create`, `get_partition_ancestors`, `RelationGetIndexList`) are
//! reached through their owner `-seams` crates.
//!
//! These install into the `backend-executor-nodeModifyTable-seams` declarations
//! (`exec_open_partition_indices` / `exec_init_partition_*`), which
//! execPartition's `ExecInitPartitionInfo` calls per leaf partition.

use ::mcx::Mcx;
use ::types_error::PgResult;
use ::types_core::primitive::Index;
use ::nodes::modifytable::{ONCONFLICT_NONE, ONCONFLICT_UPDATE};
use ::nodes::nodes::{CmdType, OnConflictAction};
use nodes::{EStateData, ModifyTableState, RriId};

extern crate alloc;

/// `((ModifyTable *) mtstate->ps.plan)->onConflictAction` — the ON CONFLICT
/// action of the `ModifyTableState`'s plan node, or `ONCONFLICT_NONE` when the
/// plan pointer is the C `NULL` (the `node ? ... : ONCONFLICT_NONE` guard).
/// Infallible — a plain field read.
pub fn ExecGetOnConflictAction(mtstate: &ModifyTableState<'_>) -> OnConflictAction {
    // ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
    // node ? node->onConflictAction : ONCONFLICT_NONE
    mtstate
        .plan_node
        .map(|n| n.onConflictAction)
        .unwrap_or(ONCONFLICT_NONE)
}

/// The `relhasindex`-gated `ExecOpenIndices` leg of `ExecInitPartitionInfo`
/// (execPartition.c L543-547).
pub fn ExecOpenPartitionIndices<'mcx>(
    _mcx: Mcx<'mcx>,
    mtstate: &ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    leaf_part_rri: RriId,
) -> PgResult<()> {
    // ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
    //
    // if (partrel->rd_rel->relhasindex &&
    //     leaf_part_rri->ri_IndexRelationDescs == NULL)
    //     ExecOpenIndices(leaf_part_rri,
    //                     (node != NULL &&
    //                      node->onConflictAction != ONCONFLICT_NONE));
    let rri = estate.result_rel(leaf_part_rri);
    let relhasindex = rri
        .ri_RelationDesc
        .as_ref()
        .map(|r| r.rd_rel.relhasindex)
        .unwrap_or(false);
    if relhasindex && rri.ri_IndexRelationDescs.is_none() {
        // speculative = (node != NULL && node->onConflictAction != ONCONFLICT_NONE)
        let speculative = mtstate
            .plan_node
            .map(|n| n.onConflictAction != ONCONFLICT_NONE)
            .unwrap_or(false);
        execIndexing_seams::exec_open_indices::call(
            estate,
            leaf_part_rri,
            speculative,
        )?;
    }
    Ok(())
}

/// The WITH CHECK OPTION leg of `ExecInitPartitionInfo` (execPartition.c
/// L549-614).
pub fn ExecInitPartitionWithCheckOptions<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    leaf_part_rri: RriId,
    first_varno: Index,
    first_result_rel: RriId,
) -> PgResult<()> {
    // if (node && node->withCheckOptionLists != NIL) { ... }
    //
    // Both the reference relation and this partition have the same columns, so a
    // simple varattno translation suffices.
    let node = match mtstate.plan_node {
        Some(n) => n,
        None => return Ok(()),
    };
    //   wcoList = linitial(node->withCheckOptionLists);  — the first plan's WCO
    // list is the reference. Clone each WithCheckOption out of the plan node into
    // the per-query context so its `qual` can be remapped to the partition's
    // attnos without mutating the shared plan.
    let ref_wco_list = match node.withCheckOptionLists.as_ref() {
        Some(lists) if !lists.is_empty() => lists[0].as_slice(),
        _ => return Ok(()),
    };
    let mut cloned_wcos: alloc::vec::Vec<::nodes::rawnodes::WithCheckOption<'mcx>> =
        alloc::vec::Vec::with_capacity(ref_wco_list.len());
    for wco_node in ref_wco_list {
        let wco = wco_node.as_withcheckoption().ok_or_else(|| {
            ::types_error::PgError::error(
                "ExecInitPartitionWithCheckOptions: withCheckOptionLists element is not a \
                 WithCheckOption node",
            )
        })?;
        cloned_wcos.push(wco.clone_in(mcx)?);
    }

    // Convert Vars in each WCO qual to contain this partition's attribute numbers.
    //   part_attmap =
    //       build_attrmap_by_name(RelationGetDescr(partrel),
    //                             RelationGetDescr(firstResultRel),
    //                             false);
    let part_desc = estate
        .result_rel(leaf_part_rri)
        .ri_RelationDesc
        .as_ref()
        .expect(
            "ExecInitPartitionWithCheckOptions: leaf partition ResultRelInfo has no open relation",
        )
        .rd_att_clone_in(mcx)?;
    let part_reltype = estate
        .result_rel(leaf_part_rri)
        .ri_RelationDesc
        .as_ref()
        .expect(
            "ExecInitPartitionWithCheckOptions: leaf partition ResultRelInfo has no open relation",
        )
        .rd_rel
        .reltype;
    let first_desc = estate
        .result_rel(first_result_rel)
        .ri_RelationDesc
        .as_ref()
        .expect(
            "ExecInitPartitionWithCheckOptions: first result ResultRelInfo has no open relation",
        )
        .rd_att_clone_in(mcx)?;
    let part_attmap = next_seams::build_attrmap_by_name::call(
        mcx,
        &part_desc,
        &first_desc,
        false,
    )?;

    //   wcoList = (List *)
    //       map_variable_attnos((Node *) wcoList,
    //                           firstVarno, 0,
    //                           part_attmap,
    //                           RelationGetForm(partrel)->reltype,
    //                           &found_whole_row);
    //   /* We ignore the value of found_whole_row. */
    //
    // C maps the whole `List *` of WithCheckOption nodes in one call; the T_List
    // mutator arm recurses into each WithCheckOption's `qual`. Over the owned
    // model, map each WCO's `qual` node individually, then rewrap it as a
    // `WithCheckOption` Node so the (working) per-rel `exec_init_with_check_options`
    // seam — the same one ExecInitModifyTable uses — can ExecInitQual each qual and
    // store ri_WithCheckOptions / ri_WithCheckOptionExprs.
    let mut remapped_wco_nodes: ::mcx::PgVec<'mcx, ::nodes::nodes::Node<'mcx>> =
        ::mcx::vec_with_capacity_in(mcx, cloned_wcos.len())?;
    for mut wco in cloned_wcos {
        if let Some(qual) = wco.qual.take() {
            let (mapped_qual, _found_whole_row) =
                rewritemanip_seams::map_variable_attnos_node::call(
                    mcx,
                    qual,
                    first_varno as i32,
                    0,
                    &part_attmap.attnums,
                    part_reltype,
                )?;
            wco.qual = Some(mapped_qual);
        }
        remapped_wco_nodes.push(::nodes::nodes::Node::mk_with_check_option(mcx, wco)?);
    }

    // ExecInitQual each wco->qual and store ri_WithCheckOptions /
    // ri_WithCheckOptionExprs — the per-rel WCO compile is execExpr-owned and is
    // identical to the non-partition ExecInitModifyTable path.
    execExpr_seams::exec_init_with_check_options::call(
        mtstate,
        estate,
        leaf_part_rri,
        remapped_wco_nodes.as_slice(),
    )
}

/// The RETURNING leg of `ExecInitPartitionInfo` (execPartition.c L616-679).
pub fn ExecInitPartitionReturning<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    leaf_part_rri: RriId,
    first_varno: Index,
    first_result_rel: RriId,
) -> PgResult<()> {
    // if (node && node->returningLists != NIL) { ... }
    //
    // returningList = linitial(node->returningLists);  — the first plan's
    // RETURNING list is the reference.
    let node = match mtstate.plan_node {
        Some(n) => n,
        None => return Ok(()),
    };
    // returningList = linitial(node->returningLists); — the first plan's
    // RETURNING list is the reference. Clone it out of the plan node into the
    // per-query context so it can be remapped to the partition's attnos.
    let ref_returning_list = match node.returningLists.as_ref() {
        Some(lists) if !lists.is_empty() => lists[0].as_slice(),
        _ => return Ok(()),
    };
    let mut returning_list: ::mcx::PgVec<'mcx, ::nodes::primnodes::TargetEntry<'mcx>> =
        ::mcx::vec_with_capacity_in(mcx, ref_returning_list.len())?;
    for tle in ref_returning_list {
        returning_list.push(tle.clone_in(mcx)?);
    }

    // Convert Vars in it to contain this partition's attribute numbers.
    //   if (part_attmap == NULL)
    //       part_attmap = build_attrmap_by_name(RelationGetDescr(partrel),
    //                                           RelationGetDescr(firstResultRel),
    //                                           false);
    let part_desc = estate
        .result_rel(leaf_part_rri)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecInitPartitionReturning: leaf partition ResultRelInfo has no open relation")
        .rd_att_clone_in(mcx)?;
    let part_reltype = estate
        .result_rel(leaf_part_rri)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecInitPartitionReturning: leaf partition ResultRelInfo has no open relation")
        .rd_rel
        .reltype;
    let first_desc = estate
        .result_rel(first_result_rel)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecInitPartitionReturning: first result ResultRelInfo has no open relation")
        .rd_att_clone_in(mcx)?;
    let part_attmap = next_seams::build_attrmap_by_name::call(
        mcx,
        &part_desc,
        &first_desc,
        false,
    )?;

    //   returningList = (List *)
    //       map_variable_attnos((Node *) returningList,
    //                           firstVarno, 0,
    //                           part_attmap,
    //                           RelationGetForm(partrel)->reltype,
    //                           &found_whole_row);
    //   /* We ignore the value of found_whole_row. */
    let (returning_list, _found_whole_row) =
        rewritemanip_seams::map_variable_attnos_targetentry_list::call(
            mcx,
            returning_list,
            first_varno as i32,
            &part_attmap.attnums,
            part_reltype,
        )?;

    // Assert(mtstate->ps.ps_ResultTupleSlot != NULL);
    // Assert(mtstate->ps.ps_ExprContext != NULL);
    //
    //   leaf_part_rri->ri_returningList = returningList;
    //   leaf_part_rri->ri_projectReturning =
    //       ExecBuildProjectionInfo(returningList, econtext, slot,
    //                               &mtstate->ps, RelationGetDescr(partrel));
    //
    // Build ri_projectReturning via ExecBuildProjectionInfo using the slot
    // (mtstate->ps.ps_ResultTupleSlot) / econtext (ps_ExprContext) set up in
    // ExecInitModifyTable and the partition's tupdesc (leaf_part_rri's rd_att),
    // and store ri_returningList — execExpr-owned (the leaf's own rd_att is the
    // input descriptor, matching RelationGetDescr(partrel)).
    execExpr_seams::exec_build_returning_projection::call(
        mtstate,
        estate,
        leaf_part_rri,
        returning_list.as_slice(),
    )?;

    // C builds this projection with `&mtstate->ps` as the expression parent, so
    // a `merge_action()` in the RETURNING list (EEOP_MERGE_SUPPORT_FUNC) can
    // recover the ModifyTableState. This leaf projection is built lazily, after
    // the up-front stamp_modifytable_expr_parents pass, so stamp it here with the
    // recorded ModifyTableState back-link.
    if let Some(link) = mtstate.mt_self_link {
        ::nodes::planstate::stamp_result_rel_expr_parents(estate, leaf_part_rri, link);
    }
    Ok(())
}

/// The ON CONFLICT leg of `ExecInitPartitionInfo` (execPartition.c L685-862).
pub fn ExecInitPartitionOnConflict<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    leaf_part_rri: RriId,
    root_result_rel_info: RriId,
    first_varno: Index,
    first_result_rel: RriId,
) -> PgResult<()> {
    // if (node && node->onConflictAction != ONCONFLICT_NONE) { ... }
    let node = match mtstate.plan_node {
        Some(n) => n,
        None => return Ok(()),
    };
    if node.onConflictAction == ONCONFLICT_NONE {
        return Ok(());
    }

    // List *arbiterIndexes = NIL;
    //
    // If there is a list of arbiter indexes, map it to a list of indexes in the
    // partition by scanning the partition's index list and searching for
    // ancestry relationships to each index in the ancestor table.
    //   if (rootResultRelInfo->ri_onConflictArbiterIndexes != NIL) { ... }
    let mut arbiter_indexes: alloc::vec::Vec<::types_core::Oid> = alloc::vec::Vec::new();

    let root_arbiters: alloc::vec::Vec<::types_core::Oid> = estate
        .result_rel(root_result_rel_info)
        .ri_onConflictArbiterIndexes
        .as_ref()
        .map(|l| l.iter().copied().collect())
        .unwrap_or_default();

    if !root_arbiters.is_empty() {
        // childIdxs = RelationGetIndexList(leaf_part_rri->ri_RelationDesc);
        let part_rel = estate
            .result_rel(leaf_part_rri)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecInitPartitionInfo: leaf partition ResultRelInfo has an open relation")
            .alias();
        let child_idxs = relcache_seams::relation_get_index_list::call(
            mcx, &part_rel,
        )?;

        // foreach(lc, childIdxs) { ancestors = get_partition_ancestors(childIdx);
        //   foreach(lc2, root arbiters) if (list_member_oid(ancestors, lc2))
        //     arbiterIndexes = lappend_oid(arbiterIndexes, childIdx);
        //   list_free(ancestors); }
        for child_idx in child_idxs.iter().copied() {
            let ancestors =
                partition_seams::get_partition_ancestors::call(mcx, child_idx)?;
            for root_arb in root_arbiters.iter().copied() {
                if ancestors.iter().any(|a| *a == root_arb) {
                    arbiter_indexes.push(child_idx);
                    break;
                }
            }
        }
    }

    // If the resulting lists are of inequal length, something is wrong.
    //   if (list_length(root arbiters) != list_length(arbiterIndexes))
    //       elog(ERROR, "invalid arbiter index list");
    if root_arbiters.len() != arbiter_indexes.len() {
        return Err(::types_error::PgError::error("invalid arbiter index list"));
    }

    // leaf_part_rri->ri_onConflictArbiterIndexes = arbiterIndexes;
    {
        let dst = if arbiter_indexes.is_empty() {
            None
        } else {
            let mut v = ::mcx::vec_with_capacity_in(mcx, arbiter_indexes.len())?;
            for oid in arbiter_indexes.iter().copied() {
                v.push(oid);
            }
            Some(v)
        };
        estate.result_rel_mut(leaf_part_rri).ri_onConflictArbiterIndexes = dst;
    }

    // In the DO UPDATE case, we have some more state to initialize.
    //   if (node->onConflictAction == ONCONFLICT_UPDATE) { ... }
    if node.onConflictAction == ONCONFLICT_UPDATE {
        ExecInitPartitionOnConflictUpdate(
            mcx,
            mtstate,
            estate,
            leaf_part_rri,
            root_result_rel_info,
            first_varno,
            first_result_rel,
        )?;
    }

    Ok(())
}

/// `INNER_VAR` (primnodes.h) — the EXCLUDED pseudo-relation's varno in an ON
/// CONFLICT DO UPDATE SET / WHERE clause.
const INNER_VAR: i32 = -1;

/// The ON CONFLICT DO UPDATE `OnConflictSetState` build of
/// `ExecInitPartitionInfo` (execPartition.c L730-861).
///
/// `makeNode(OnConflictSetState)`, create the per-partition `oc_Existing` slot
/// (`table_slot_create(partrel)`), then `ExecGetRootToChildMap(leaf_part_rri)` —
/// when the map is `NULL` (rowtype matches root) reuse the root `ri_onConflict`'s
/// `oc_ProjSlot` / `oc_ProjInfo` / `oc_WhereClause`; otherwise translate
/// `node->onConflictSet` twice (`map_variable_attnos` over `INNER_VAR` then
/// `firstVarno`, with `build_attrmap_by_name(partrel, firstResultRel)`),
/// `adjust_partition_colnos(node->onConflictCols)` to the partition,
/// `table_slot_create(partrel)` the projection slot, build the UPDATE SET
/// projection via `ExecBuildUpdateProjection`, and (when `node->onConflictWhere`
/// is non-NULL) map+`ExecInitQual` the WHERE clause. Stores the built
/// `OnConflictSetState` on `leaf_part_rri.ri_onConflict`.
#[allow(clippy::too_many_arguments)]
fn ExecInitPartitionOnConflictUpdate<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    leaf_part_rri: RriId,
    _root_result_rel_info: RriId,
    first_varno: Index,
    first_result_rel: RriId,
) -> PgResult<()> {
    let node = mtstate
        .plan_node
        .expect("ExecInitPartitionOnConflictUpdate: ModifyTable plan node is present");

    // ExprContext *econtext = mtstate->ps.ps_ExprContext;
    let econtext = mtstate
        .ps
        .ps_ExprContext
        .expect("ExecInitPartitionInfo: ON CONFLICT node has an expression context");

    // Assert(node->onConflictSet != NIL);
    // Assert(rootResultRelInfo->ri_onConflict != NULL);

    // Need a separate existing slot for each partition, as the partition could
    // be of a different AM, even if the tuple descriptors match.
    //   onconfl->oc_Existing =
    //       table_slot_create(leaf_part_rri->ri_RelationDesc,
    //                         &mtstate->ps.state->es_tupleTable);
    let part_rel = estate
        .result_rel(leaf_part_rri)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecInitPartitionInfo: leaf partition ResultRelInfo has an open relation")
        .alias();
    let existing_slot = table_tableam::table_slot_create(mcx, &part_rel)?;
    let oc_existing = estate.push_slot_data(existing_slot)?;

    //   map = ExecGetRootToChildMap(leaf_part_rri, estate);
    //
    // If the partition's tuple descriptor matches exactly the root parent (the
    // common case, map == NULL), the C re-uses the parent's ON CONFLICT SET
    // projection/where pointers. Over the owned model — where `oc_ProjInfo`
    // (a `ProjectionInfo` value) and `oc_WhereClause` (an `ExprState` value) are
    // held by value, not by shared pointer — we instead build them fresh against
    // the leaf partition's descriptor (which, when map == NULL, is identical to
    // the root's), using `node->onConflictSet` / `onConflictCols` /
    // `onConflictWhere` directly with NO attribute remapping. When map != NULL we
    // remap (twice over INNER_VAR then firstVarno) and adjust the target colnos
    // to the partition, exactly as the C does.
    let map = execUtils_seams::exec_get_root_to_child_map::call(
        mcx,
        estate,
        leaf_part_rri,
    )?;
    let need_remap = map.is_some();

    // partrelDesc reltype for map_variable_attnos's to_rowtype.
    let part_reltype = estate
        .result_rel(leaf_part_rri)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecInitPartitionInfo: leaf partition has an open relation")
        .rd_rel
        .reltype;

    // onconflset = copyObject(node->onConflictSet);
    let ref_set = node
        .onConflictSet
        .as_ref()
        .map(|l| l.as_slice())
        .unwrap_or(&[]);
    let mut onconflset_init: ::mcx::PgVec<'mcx, ::nodes::primnodes::TargetEntry<'mcx>> =
        ::mcx::vec_with_capacity_in(mcx, ref_set.len())?;
    for tle in ref_set {
        onconflset_init.push(tle.clone_in(mcx)?);
    }
    let onconflset = onconflset_init;

    let ref_cols = node
        .onConflictCols
        .as_ref()
        .map(|l| l.as_slice())
        .unwrap_or(&[]);

    // When the rowtype differs, build part_attmap once and remap the SET list /
    // adjust the colnos. build_attrmap_by_name is fallible, so compute it lazily.
    // part_attmap is also needed to remap the WHERE clause below.
    let (onconflset, onconflcols_owned, part_attmap) = if need_remap {
        // part_attmap = build_attrmap_by_name(RelationGetDescr(partrel),
        //                                     RelationGetDescr(firstResultRel),
        //                                     false);
        let part_desc = estate
            .result_rel(leaf_part_rri)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecInitPartitionInfo: leaf partition has an open relation")
            .rd_att_clone_in(mcx)?;
        let first_desc = estate
            .result_rel(first_result_rel)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecInitPartitionInfo: first result ResultRelInfo has an open relation")
            .rd_att_clone_in(mcx)?;
        let attmap = next_seams::build_attrmap_by_name::call(
            mcx,
            &part_desc,
            &first_desc,
            false,
        )?;

        // onconflset = map_variable_attnos(onconflset, INNER_VAR, 0, part_attmap,
        //                                  partrel reltype);
        // onconflset = map_variable_attnos(onconflset, firstVarno, 0, part_attmap,
        //                                  partrel reltype);
        let (set1, _fwr1) =
            rewritemanip_seams::map_variable_attnos_targetentry_list::call(
                mcx,
                onconflset,
                INNER_VAR,
                &attmap.attnums,
                part_reltype,
            )?;
        let (set2, _fwr2) =
            rewritemanip_seams::map_variable_attnos_targetentry_list::call(
                mcx,
                set1,
                first_varno as i32,
                &attmap.attnums,
                part_reltype,
            )?;

        // onconflcols = adjust_partition_colnos(node->onConflictCols, leaf_part_rri);
        let cols = execPartition_seams::adjust_partition_colnos::call(
            mcx,
            estate,
            ref_cols,
            leaf_part_rri,
        )?;
        (set2, Some(cols), Some(attmap))
    } else {
        (onconflset, None, None)
    };

    let onconflcols: &[i32] = match &onconflcols_owned {
        Some(v) => v.as_slice(),
        None => ref_cols,
    };

    // Create the tuple slot for the UPDATE SET projection.
    //   onconfl->oc_ProjSlot =
    //       table_slot_create(partrel, &mtstate->ps.state->es_tupleTable);
    let part_rel2 = estate
        .result_rel(leaf_part_rri)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecInitPartitionInfo: leaf partition has an open relation")
        .alias();
    let proj_slot_raw = table_tableam::table_slot_create(mcx, &part_rel2)?;
    let oc_proj_slot = estate.push_slot_data(proj_slot_raw)?;

    // build UPDATE SET projection state
    //   onconfl->oc_ProjInfo =
    //       ExecBuildUpdateProjection(onconflset, true, onconflcols, partrelDesc,
    //                                 econtext, onconfl->oc_ProjSlot, &mtstate->ps);
    let oc_proj_info =
        execExpr_seams::exec_build_on_conflict_set_projection::call(
            mtstate,
            estate,
            leaf_part_rri,
            onconflset.as_slice(),
            onconflcols,
            econtext,
            oc_proj_slot,
        )?;

    // If there is a WHERE clause, initialize state where it will be evaluated,
    // mapping the attribute numbers (INNER_VAR then firstVarno) when remapping.
    //   if (node->onConflictWhere) { ... onconfl->oc_WhereClause =
    //       ExecInitQual((List *) clause, &mtstate->ps); }
    let oc_where_clause = match node.onConflictWhere.as_deref() {
        Some(ref_where) if !ref_where.is_empty() => {
            let mut clause: ::mcx::PgVec<'mcx, ::nodes::primnodes::Expr<'mcx>> =
                ::mcx::vec_with_capacity_in(mcx, ref_where.len())?;
            for e in ref_where {
                clause.push(e.clone_in(mcx)?);
            }
            let clause = if let Some(attmap) = part_attmap.as_ref() {
                let (c1, _fwr3) =
                    rewritemanip_seams::map_variable_attnos_expr_list_varno::call(
                        mcx,
                        clause,
                        INNER_VAR,
                        &attmap.attnums,
                        part_reltype,
                    )?;
                let (c2, _fwr4) =
                    rewritemanip_seams::map_variable_attnos_expr_list_varno::call(
                        mcx,
                        c1,
                        first_varno as i32,
                        &attmap.attnums,
                        part_reltype,
                    )?;
                c2
            } else {
                clause
            };
            execExpr_seams::exec_init_on_conflict_where::call(
                mtstate,
                estate,
                Some(clause.as_slice()),
            )?
        }
        _ => None,
    };

    let oc_proj_slot = Some(oc_proj_slot);
    let oc_proj_info = Some(oc_proj_info);

    // OnConflictSetState *onconfl = makeNode(OnConflictSetState);
    // leaf_part_rri->ri_onConflict = onconfl;
    let onconfl = ::mcx::alloc_in(
        mcx,
        ::nodes::modifytable::OnConflictSetState {
            type_: ::nodes::nodes::T_OnConflictSetState,
            oc_Existing: Some(oc_existing),
            oc_ProjSlot: oc_proj_slot,
            oc_ProjInfo: oc_proj_info,
            oc_WhereClause: oc_where_clause,
        },
    )?;
    estate.result_rel_mut(leaf_part_rri).ri_onConflict = Some(onconfl);

    Ok(())
}

/// The MERGE leg of `ExecInitPartitionInfo` (execPartition.c L877-981).
pub fn ExecInitPartitionMerge<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    leaf_part_rri: RriId,
    first_varno: Index,
    first_result_rel: RriId,
) -> PgResult<()> {
    // if (node && node->operation == CMD_MERGE) { ... }
    let node = match mtstate.plan_node {
        Some(n) => n,
        None => return Ok(()),
    };
    if node.operation != CmdType::CMD_MERGE {
        return Ok(());
    }

    // List *firstMergeActionList = linitial(node->mergeActionLists);
    let ref_merge_action_list: &'mcx ::mcx::PgVec<'mcx, ::nodes::modifytable::MergeAction<'mcx>> =
        match node.mergeActionLists.as_ref() {
            Some(lists) if !lists.is_empty() => &lists[0],
            _ => return Ok(()),
        };

    // ExprContext *econtext = mtstate->ps.ps_ExprContext;
    let econtext = mtstate
        .ps
        .ps_ExprContext
        .expect("ExecInitPartitionInfo: MERGE node has an expression context");

    // part_attmap =
    //     build_attrmap_by_name(RelationGetDescr(partrel),
    //                           RelationGetDescr(firstResultRel), false);
    let part_desc = estate
        .result_rel(leaf_part_rri)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecInitPartitionMerge: leaf partition ResultRelInfo has no open relation")
        .rd_att_clone_in(mcx)?;
    let part_reltype = estate
        .result_rel(leaf_part_rri)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecInitPartitionMerge: leaf partition ResultRelInfo has no open relation")
        .rd_rel
        .reltype;
    let first_desc = estate
        .result_rel(first_result_rel)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecInitPartitionMerge: first result ResultRelInfo has no open relation")
        .rd_att_clone_in(mcx)?;
    let part_attmap = next_seams::build_attrmap_by_name::call(
        mcx,
        &part_desc,
        &first_desc,
        false,
    )?;
    let part_attnums: alloc::vec::Vec<i16> = part_attmap.attnums.iter().copied().collect();

    // if (unlikely(!leaf_part_rri->ri_projectNewInfoValid))
    //     ExecInitMergeTupleSlots(mtstate, leaf_part_rri);
    if !estate.result_rel(leaf_part_rri).ri_projectNewInfoValid {
        crate::merge::ExecInitMergeTupleSlots(mcx, mtstate, estate, leaf_part_rri)?;
    }

    // Initialize state for join condition checking.
    //   joinCondition =
    //       map_variable_attnos(linitial(node->mergeJoinConditions),
    //                           firstVarno, 0, part_attmap,
    //                           RelationGetForm(partrel)->reltype,
    //                           &found_whole_row);
    //   leaf_part_rri->ri_MergeJoinCondition =
    //       ExecInitQual((List *) joinCondition, &mtstate->ps);
    let mapped_join_condition: Option<::mcx::PgVec<'mcx, ::nodes::primnodes::Expr<'mcx>>> = {
        let ref_join_condition: Option<&'mcx [::nodes::primnodes::Expr]> = node
            .mergeJoinConditions
            .as_ref()
            .and_then(|jc| jc.first())
            .and_then(|c| c.as_ref())
            .map(|v| v.as_slice());
        match ref_join_condition {
            None => None,
            Some(jc) => {
                let mut cloned: ::mcx::PgVec<'mcx, ::nodes::primnodes::Expr<'mcx>> =
                    ::mcx::vec_with_capacity_in(mcx, jc.len())?;
                for e in jc {
                    cloned.push(e.clone_in(mcx)?);
                }
                let (mapped, _fwr) =
                    rewritemanip_seams::map_variable_attnos_expr_list_varno::call(
                        mcx,
                        cloned,
                        first_varno as i32,
                        &part_attnums,
                        part_reltype,
                    )?;
                Some(mapped)
            }
        }
    };
    execExpr_seams::exec_init_merge_join_condition::call(
        mtstate,
        estate,
        leaf_part_rri,
        mapped_join_condition.as_ref().map(|v| v.as_slice()),
    )?;

    // foreach(lc, firstMergeActionList) — build each action's MergeActionState
    // for this leaf partition. This mirrors merge.rs::ExecInitMerge, but each
    // action's targetList / updateColnos / qual is first remapped to this
    // partition's attribute numbers (map_variable_attnos /
    // adjust_partition_colnos_using_map), since the reference action came from
    // the first result relation. Note the per-leaf re-init does NOT touch
    // mtstate->mt_merge_subcommands (that was accumulated by ExecInitMerge over
    // the first result rel).
    for action in ref_merge_action_list.iter() {
        // MergeAction *action = copyObject(lfirst(lc));
        let match_kind = action.matchKind;
        let command_type = action.commandType;

        // INSERT/UPDATE: clone + remap the action's targetList; UPDATE also
        // remaps updateColnos via the part_attmap.
        let target_list: ::mcx::PgVec<'mcx, ::nodes::primnodes::TargetEntry<'mcx>> = {
            let mut tl: ::mcx::PgVec<'mcx, ::nodes::primnodes::TargetEntry<'mcx>> =
                ::mcx::vec_with_capacity_in(
                    mcx,
                    action.targetList.as_ref().map(|t| t.len()).unwrap_or(0),
                )?;
            if let Some(src) = action.targetList.as_ref() {
                for tle in src.iter() {
                    tl.push(tle.clone_in(mcx)?);
                }
            }
            tl
        };

        let mas_proj: Option<::mcx::PgBox<'mcx, ::nodes::execexpr::ProjectionInfo<'mcx>>>;

        match command_type {
            CmdType::CMD_INSERT => {
                // ExecCheckPlanOutput() already done on the targetlist when the
                // "first" result relation was initialized (it is the same for
                // all result relations), so it is not repeated here.
                //
                // action_state->mas_proj =
                //     ExecBuildProjectionInfo(action->targetList, econtext,
                //                             leaf_part_rri->ri_newTupleSlot,
                //                             &mtstate->ps,
                //                             RelationGetDescr(partrel));
                let tgt_slot = estate
                    .result_rel(leaf_part_rri)
                    .ri_newTupleSlot
                    .expect("ExecInitPartitionMerge: leaf partition has a new tuple slot");
                mas_proj = Some(
                    execExpr_seams::exec_build_merge_insert_projection::call(
                        mtstate,
                        estate,
                        target_list.as_slice(),
                        econtext,
                        tgt_slot,
                        leaf_part_rri,
                    )?,
                );
            }
            CmdType::CMD_UPDATE => {
                // if (part_attmap)
                //     action->updateColnos =
                //         adjust_partition_colnos_using_map(action->updateColnos,
                //                                           part_attmap);
                let mapped_colnos = match action.updateColnos.as_ref() {
                    Some(cols) if !cols.is_empty() => {
                        execPartition_seams::adjust_partition_colnos_using_map::call(
                            mcx,
                            cols.as_slice(),
                            &part_attnums,
                        )?
                    }
                    _ => ::mcx::PgVec::new_in(mcx),
                };
                // action_state->mas_proj =
                //     ExecBuildUpdateProjection(action->targetList, true,
                //         action->updateColnos,
                //         RelationGetDescr(leaf_part_rri->ri_RelationDesc),
                //         econtext, leaf_part_rri->ri_newTupleSlot, NULL);
                mas_proj = Some(
                    execExpr_seams::exec_build_merge_update_projection::call(
                        mtstate,
                        estate,
                        leaf_part_rri,
                        target_list.as_slice(),
                        mapped_colnos.as_slice(),
                        econtext,
                    )?,
                );
            }
            CmdType::CMD_DELETE | CmdType::CMD_NOTHING => {
                // Nothing to do.
                mas_proj = None;
            }
            _ => {
                // default: elog(ERROR, "unknown action in MERGE WHEN clause");
                return Err(::types_error::PgError::error(
                    "unknown action in MERGE WHEN clause",
                ));
            }
        }

        // found_whole_row intentionally ignored.
        //   action->qual =
        //       map_variable_attnos(action->qual, firstVarno, 0, part_attmap,
        //                           RelationGetForm(partrel)->reltype,
        //                           &found_whole_row);
        //   action_state->mas_whenqual =
        //       ExecInitQual((List *) action->qual, &mtstate->ps);
        let mapped_qual: Option<::mcx::PgVec<'mcx, ::nodes::primnodes::Expr<'mcx>>> =
            match action.qual.as_ref() {
                None => None,
                Some(q) => {
                    let mut cloned: ::mcx::PgVec<'mcx, ::nodes::primnodes::Expr<'mcx>> =
                        ::mcx::vec_with_capacity_in(mcx, q.len())?;
                    for e in q.iter() {
                        cloned.push(e.clone_in(mcx)?);
                    }
                    let (mapped, _fwr) =
                        rewritemanip_seams::map_variable_attnos_expr_list_varno::call(
                            mcx,
                            cloned,
                            first_varno as i32,
                            &part_attnums,
                            part_reltype,
                        )?;
                    Some(mapped)
                }
            };
        let mas_whenqual = execExpr_seams::exec_init_merge_when_qual::call(
            mtstate,
            estate,
            mapped_qual.as_ref().map(|v| v.as_slice()),
        )?;

        // action_state = makeNode(MergeActionState);
        // action_state->mas_action = action;
        //
        // The owned-by-value tree cannot share the `&'mcx` plan borrow into the
        // pooled state, so `mas_action` carries the command/match-kind/overriding
        // fields the executor reads (ExecMergeMatched reads only commandType /
        // matchKind), matching merge.rs::ExecInitMerge.
        let mas_action = ::mcx::alloc_in(
            mcx,
            ::nodes::modifytable::MergeAction {
                matchKind: match_kind,
                commandType: command_type,
                overriding: action.overriding,
                qual: None,
                targetList: None,
                updateColnos: None,
            },
        )?;
        let action_state = ::mcx::alloc_in(
            mcx,
            ::nodes::modifytable::MergeActionState {
                type_: ::nodes::nodes::T_MergeActionState,
                mas_action: Some(mas_action),
                mas_proj,
                mas_whenqual,
            },
        )?;

        // leaf_part_rri->ri_MergeActions[action->matchKind] =
        //     lappend(leaf_part_rri->ri_MergeActions[action->matchKind],
        //             action_state);
        let slot = &mut estate.result_rel_mut(leaf_part_rri).ri_MergeActions[match_kind as usize];
        match slot {
            Some(list) => list.push(action_state),
            None => {
                let mut v = ::mcx::PgVec::new_in(mcx);
                v.push(action_state);
                *slot = Some(v);
            }
        }
    }

    // C builds the join-condition qual and each action's mas_proj/mas_whenqual
    // with `&mtstate->ps` as the expression parent. These leaf ExprStates are
    // built lazily, after the up-front stamp_modifytable_expr_parents pass, so
    // stamp the leaf's ri_MergeJoinCondition / ri_MergeActions here with the
    // recorded ModifyTableState back-link (mirrors ExecInitPartitionReturning).
    if let Some(link) = mtstate.mt_self_link {
        ::nodes::planstate::stamp_result_rel_expr_parents(estate, leaf_part_rri, link);
    }

    Ok(())
}
