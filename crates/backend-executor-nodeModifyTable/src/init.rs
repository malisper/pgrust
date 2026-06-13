//! The ModifyTable node initializer (`ExecInitModifyTable`), split out of the
//! `lifecycle` family because the C function (~579 lines) is large enough to
//! body-port independently of the rest of the node lifecycle.

use mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_core::primitive::{Index, InvalidOid};
use types_error::{PgError, PgResult};
use types_nodes::modifytable::{
    MergeAction, OnConflictAction, ResultRelHash, ONCONFLICT_NONE,
};
use types_nodes::nodes::CmdType;
use types_nodes::parsenodes::RTEKind;
use types_nodes::primnodes::TargetEntry;
use types_nodes::{
    EPQState, EStateData, ModifyTable, ModifyTableState, PlanStateData, ResultRelInfo, RriId,
};
use types_tuple::access::{
    RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
};

use crate::{lifecycle, merge};

/// In a debugging build C uses a small `MT_NRELS_HASH` threshold to exercise
/// both lookup paths; the release build uses 64. We mirror the release value
/// (this crate is not compiled with `USE_ASSERT_CHECKING`).
const MT_NRELS_HASH: usize = 64;

/// `ExecInitModifyTable(node, estate, eflags)` — initialize the ModifyTable
/// plan node: open result relations, set up projections/junk filters,
/// generated columns, ON CONFLICT / MERGE / tuple-routing state, and the
/// subplan. Returns the new `ModifyTableState`.
pub fn ExecInitModifyTable<'mcx>(
    mcx: Mcx<'mcx>,
    node: &'mcx ModifyTable<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, ModifyTableState<'mcx>>> {
    // Plan *subplan = outerPlan(node);
    let subplan = node.plan.lefttree.as_deref();
    // CmdType operation = node->operation;
    let operation = node.operation;
    // int total_nrels = list_length(node->resultRelations);
    let total_nrels: usize = node
        .resultRelations
        .as_ref()
        .map(|l| l.len())
        .unwrap_or(0);

    // check for unsupported flags
    //   Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    // Only consider unpruned relations for initializing their ResultRelInfo
    // struct and other fields such as withCheckOptions, etc.  See the long C
    // comment: at least one result relation must be kept (for MERGE NOT
    // MATCHED rows and partitioned-INSERT routing), so if every other rel is
    // pruned we keep the first one.
    //
    //   List *resultRelations = NIL; ... foreach(l, node->resultRelations)
    let mut result_relations: PgVec<'mcx, Index> = vec_with_capacity_in(mcx, total_nrels)?;
    let mut with_check_option_lists: PgVec<'mcx, &PgVec<'mcx, _>> =
        vec_with_capacity_in(mcx, total_nrels)?;
    let mut returning_lists: PgVec<'mcx, &PgVec<'mcx, TargetEntry<'mcx>>> =
        vec_with_capacity_in(mcx, total_nrels)?;
    let mut update_colnos_lists: PgVec<'mcx, PgVec<'mcx, i32>> =
        vec_with_capacity_in(mcx, total_nrels)?;
    // mergeActionLists / mergeJoinConditions: the owned model has no shared
    // alias for the planner's MergeAction lists, so these stay empty (the
    // per-target append errors as unported below); kept as the C `NIL` lists.
    let merge_action_lists: PgVec<'mcx, PgVec<'mcx, MergeAction<'mcx>>> =
        vec_with_capacity_in(mcx, total_nrels)?;
    let merge_join_conditions: PgVec<'mcx, Option<PgBox<'mcx, types_nodes::nodes::Node<'mcx>>>> =
        vec_with_capacity_in(mcx, total_nrels)?;

    {
        // foreach(l, node->resultRelations) with the C's `i` index.
        let rrs = node.resultRelations.as_ref();
        let mut i: usize = 0;
        if let Some(rrs) = rrs {
            for idx in 0..rrs.len() {
                // Index rti = lfirst_int(l);
                let mut rti = rrs[idx];
                // bool keep_rel = bms_is_member(rti, estate->es_unpruned_relids);
                let mut keep_rel = backend_nodes_core_seams::bms_is_member::call(
                    rti as i32,
                    estate.es_unpruned_relids.as_deref(),
                );
                if !keep_rel && i == total_nrels - 1 && result_relations.is_empty() {
                    // all result relations pruned; keep the first one
                    keep_rel = true;
                    // rti = linitial_int(node->resultRelations);
                    rti = rrs[0];
                    i = 0;
                }

                if keep_rel {
                    // resultRelations = lappend_int(resultRelations, rti);
                    result_relations.try_reserve(1).map_err(|_| mcx.oom(4))?;
                    result_relations.push(rti);

                    if let Some(wcol) = node.withCheckOptionLists.as_ref() {
                        // withCheckOptions = list_nth_node(List, ..., i);
                        let wco = list_nth_ref(wcol, i)?;
                        with_check_option_lists
                            .try_reserve(1)
                            .map_err(|_| mcx.oom(8))?;
                        with_check_option_lists.push(wco);
                    }
                    if let Some(rl) = node.returningLists.as_ref() {
                        let returning_list = list_nth_ref(rl, i)?;
                        returning_lists.try_reserve(1).map_err(|_| mcx.oom(8))?;
                        returning_lists.push(returning_list);
                    }
                    if let Some(ucl) = node.updateColnosLists.as_ref() {
                        // updateColnosList = list_nth(node->updateColnosLists, i);
                        let src = list_nth_ref(ucl, i)?;
                        let mut copy = vec_with_capacity_in(mcx, src.len())?;
                        for v in src.iter() {
                            copy.push(*v);
                        }
                        update_colnos_lists.try_reserve(1).map_err(|_| mcx.oom(8))?;
                        update_colnos_lists.push(copy);
                    }
                    if let Some(mal) = node.mergeActionLists.as_ref() {
                        let _ = list_nth_ref(mal, i)?;
                        // The MERGE action lists carry `MergeAction` nodes that
                        // are not `Clone`; the C aliases the planner-owned list
                        // (`lappend`), but the owned model has no shared alias
                        // for the plan tree's MergeActions here. MERGE init
                        // (ExecInitMerge) is the owner of this list's runtime
                        // form and is reached separately below; carrying the
                        // raw alias here needs the plan-tree sharing model that
                        // lands with the parsenodes/plan owner.
                        return Err(unported(
                            "ExecInitModifyTable: per-target MERGE action list aliasing \
                             (lappend of planner-owned MergeAction lists) needs the \
                             shared plan-tree model not yet ported",
                        ));
                    }
                    if let Some(mjc) = node.mergeJoinConditions.as_ref() {
                        let _ = list_nth_ref(mjc, i)?;
                        return Err(unported(
                            "ExecInitModifyTable: per-target MERGE join-condition aliasing \
                             needs the shared plan-tree model not yet ported",
                        ));
                    }
                }
                i += 1;
            }
        }
    }
    // nrels = list_length(resultRelations);  Assert(nrels > 0);
    let nrels = result_relations.len();
    debug_assert!(nrels > 0);

    // create state structure
    //   mtstate = makeNode(ModifyTableState);
    //   mtstate->ps.plan = (Plan *) node;
    //   mtstate->ps.state = estate;
    //   mtstate->ps.ExecProcNode = ExecModifyTable;
    //
    // The owned model threads `&mut EStateData` explicitly rather than the C
    // `PlanState.state` back-pointer, and `ModifyTableState` is not a
    // `PlanStateNode` variant in the trimmed node model, so the `ps.plan`
    // alias and the `ExecProcNode` callback install are not expressible here;
    // the node's `ExecProcNode` callback (`crate::exec::ExecModifyTable`) is
    // dispatched directly by the family driver, not through the trimmed
    // `ExecProcNodeMtd` table. `ps` is otherwise the zeroed PlanState head.
    let ps = PlanStateData::default();

    // mtstate->resultRelInfo = palloc(nrels * sizeof(ResultRelInfo));
    //
    // In the owned model every ResultRelInfo lives in the EState's pool and is
    // addressed by an RriId; the `mtstate->resultRelInfo[]` array is the vec of
    // ids. Allocate the nrels zeroed ResultRelInfos now (palloc, not palloc0 in
    // C — but C immediately fills each via ExecInitResultRelation), recording
    // their ids in order.
    let mut result_rel_info: PgVec<'mcx, RriId> = vec_with_capacity_in(mcx, nrels)?;
    for _ in 0..nrels {
        let id = estate.add_result_rel(ResultRelInfo::default())?;
        result_rel_info.try_reserve(1).map_err(|_| mcx.oom(4))?;
        result_rel_info.push(id);
    }

    let mut mtstate = mcx::alloc_in(
        mcx,
        ModifyTableState {
            ps,
            operation,
            // cached from the plan node so per-tuple paths need not re-downcast
            onConflictAction: node.onConflictAction,
            canSetTag: node.canSetTag,
            mt_done: false,
            resultRelInfo: result_rel_info,
            rootResultRelInfo: None,
            mt_epqstate: EPQState::default(),
            fireBSTriggers: true,
            // mt_merge_pending_not_matched = NULL; the merge counters = 0;
            mt_merge_pending_not_matched: None,
            mt_merge_inserted: 0.0,
            mt_merge_updated: 0.0,
            mt_merge_deleted: 0.0,
            mt_updateColnosLists: if update_colnos_lists.is_empty() {
                None
            } else {
                Some(update_colnos_lists)
            },
            mt_mergeActionLists: if merge_action_lists.is_empty() {
                None
            } else {
                Some(merge_action_lists)
            },
            mt_mergeJoinConditions: if merge_join_conditions.is_empty() {
                None
            } else {
                Some(merge_join_conditions)
            },
            mt_resultOidAttno: 0,
            mt_lastResultOid: InvalidOid,
            mt_lastResultIndex: 0,
            mt_resultOidHash: None,
            mt_root_tuple_slot: None,
            mt_partition_tuple_routing: None,
            mt_transition_capture: None,
            mt_oc_transition_capture: None,
            mt_merge_subcommands: 0,
            mt_merge_action: None,
        },
    )?;

    // Resolve the target relation (FOR STATEMENT trigger rel, transition-tuple
    // target rowtype, and root partitioned table for tuple routing).
    //
    //   if (node->rootRelation > 0) {
    //       Assert(bms_is_member(node->rootRelation, estate->es_unpruned_relids));
    //       mtstate->rootResultRelInfo = makeNode(ResultRelInfo);
    //       ExecInitResultRelation(estate, mtstate->rootResultRelInfo, node->rootRelation);
    //   } else {
    //       Assert(list_length(node->resultRelations) == 1);
    //       Assert(list_length(resultRelations) == 1);
    //       mtstate->rootResultRelInfo = mtstate->resultRelInfo;
    //       ExecInitResultRelation(estate, mtstate->resultRelInfo, linitial_int(resultRelations));
    //   }
    if node.rootRelation > 0 {
        debug_assert!(backend_nodes_core_seams::bms_is_member::call(
            node.rootRelation as i32,
            estate.es_unpruned_relids.as_deref(),
        ));
        let root_id = estate.add_result_rel(ResultRelInfo::default())?;
        mtstate.rootResultRelInfo = Some(root_id);
        exec_init_result_relation(estate, root_id, node.rootRelation)?;
    } else {
        debug_assert_eq!(total_nrels, 1);
        debug_assert_eq!(nrels, 1);
        let first = mtstate.resultRelInfo[0];
        mtstate.rootResultRelInfo = Some(first);
        exec_init_result_relation(estate, first, result_relations[0])?;
    }

    // set up epqstate with dummy subplan data for the moment
    //   EvalPlanQualInit(&mtstate->mt_epqstate, estate, NULL, NIL,
    //                    node->epqParam, resultRelations);
    eval_plan_qual_init(
        &mut mtstate.mt_epqstate,
        estate,
        node.epqParam,
        &result_relations,
        mcx,
    )?;
    mtstate.fireBSTriggers = true;

    // Build state for collecting transition tuples.  Skip in explain-only mode.
    //   if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
    //       ExecSetupTransitionCaptureState(mtstate, estate);
    if eflags & EXEC_FLAG_EXPLAIN_ONLY == 0 {
        lifecycle::ExecSetupTransitionCaptureState(mcx, &mut mtstate, estate)?;
    }

    // Open all the result relations and initialize the ResultRelInfo structs.
    // (Root relation was initialized above, if it's part of the array.)  Must
    // happen before initializing the subplan (direct-modify FDWs expect their
    // ResultRelInfos to be available).
    //
    //   resultRelInfo = mtstate->resultRelInfo; i = 0;
    //   foreach(l, resultRelations) { ... }
    {
        let root = mtstate.rootResultRelInfo;
        for i in 0..nrels {
            let result_relation = result_relations[i];
            let cur = mtstate.resultRelInfo[i];

            // if (mergeActionLists) mergeActions = list_nth(mergeActionLists, i);
            // (mergeActionLists is always empty in the owned model — see the
            // unported guard above — so mergeActions stays NIL here.)

            if Some(cur) != root {
                exec_init_result_relation(estate, cur, result_relation)?;
                // For child result relations, store the root result relation
                // pointer.
                //   resultRelInfo->ri_RootResultRelInfo = mtstate->rootResultRelInfo;
                estate.result_rel_mut(cur).ri_RootResultRelInfo = root;
            }

            // Initialize the usesFdwDirectModify flag.
            //   resultRelInfo->ri_usesFdwDirectModify =
            //       bms_is_member(i, node->fdwDirectModifyPlans);
            //
            // `ri_usesFdwDirectModify` is not carried on the trimmed
            // ResultRelInfo (FDW direct modify lands with the fdwapi owner). A
            // non-empty fdwDirectModifyPlans set therefore cannot be honored.
            if backend_nodes_core_seams::bms_is_member::call(
                i as i32,
                node.fdwDirectModifyPlans.as_deref(),
            ) {
                return Err(unported(
                    "ExecInitModifyTable: ri_usesFdwDirectModify (FDW direct modify) \
                     is not carried on the trimmed ResultRelInfo",
                ));
            }

            // Verify result relation is a valid target for the current operation.
            //   CheckValidResultRel(resultRelInfo, operation, node->onConflictAction,
            //                       mergeActions);
            check_valid_result_rel(
                estate,
                cur,
                operation,
                node.onConflictAction,
                /* mergeActions = */ None,
            )?;
        }
    }

    // Now we may initialize the subplan.
    //   outerPlanState(mtstate) = ExecInitNode(subplan, estate, eflags);
    let subplan_node: Option<&'mcx types_nodes::nodes::Node<'mcx>> = subplan;
    let child = backend_executor_execProcnode_seams::exec_init_node::call(
        mcx,
        subplan_node,
        estate,
        eflags,
    )?;
    mtstate.ps.lefttree = child;

    // Do additional per-result-relation initialization.
    //   for (i = 0; i < nrels; i++) { ... }
    for i in 0..nrels {
        let result_rel_info = mtstate.resultRelInfo[i];

        // Let FDWs init themselves for foreign-table result rels.
        //   if (!ri_usesFdwDirectModify && ri_FdwRoutine != NULL &&
        //       ri_FdwRoutine->BeginForeignModify != NULL) { ... }
        //
        // `ri_FdwRoutine` is modeled on the trimmed ResultRelInfo only as the
        // `ri_has_fdw_routine` presence flag; the routine vtable
        // (BeginForeignModify) lands with the fdwapi owner.
        if estate.result_rel(result_rel_info).ri_has_fdw_routine {
            return Err(unported(
                "ExecInitModifyTable: ri_FdwRoutine->BeginForeignModify (FDW per-rel \
                 init) — the FdwRoutine vtable is not yet ported",
            ));
        }

        // For UPDATE/DELETE/MERGE, find the appropriate junk attr now, either a
        // 'ctid' or 'wholerow' attribute depending on relkind.
        if operation == CmdType::CMD_UPDATE
            || operation == CmdType::CMD_DELETE
            || operation == CmdType::CMD_MERGE
        {
            // relkind = resultRelInfo->ri_RelationDesc->rd_rel->relkind;
            let relkind = relation_relkind(estate, result_rel_info)?;
            let subplan_tlist = subplan_targetlist(subplan)?;

            if relkind == RELKIND_RELATION
                || relkind == RELKIND_MATVIEW
                || relkind == RELKIND_PARTITIONED_TABLE
            {
                // ri_RowIdAttNo = ExecFindJunkAttributeInTlist(subplan->targetlist, "ctid");
                let row_id_attno =
                    exec_find_junk_attribute_in_tlist(subplan_tlist, "ctid");
                set_ri_row_id_attno(estate, result_rel_info, row_id_attno)?;

                // For heap relations, a ctid junk attribute must be present.
                // A partitioned table only reaches here with every leaf pruned
                // (nrels == 1, no rows produced, ctid not needed).
                if relkind == RELKIND_PARTITIONED_TABLE {
                    debug_assert_eq!(nrels, 1);
                } else if !attribute_number_is_valid(row_id_attno) {
                    return Err(PgError::error("could not find junk ctid column"));
                }
            } else if relkind == RELKIND_FOREIGN_TABLE {
                // We don't support MERGE with foreign tables for now.
                debug_assert!(operation != CmdType::CMD_MERGE);

                // When there is a row-level trigger, there should be a wholerow
                // attribute.  We also require it for UPDATE and MERGE.
                let row_id_attno =
                    exec_find_junk_attribute_in_tlist(subplan_tlist, "wholerow");
                set_ri_row_id_attno(estate, result_rel_info, row_id_attno)?;
                if (operation == CmdType::CMD_UPDATE || operation == CmdType::CMD_MERGE)
                    && !attribute_number_is_valid(row_id_attno)
                {
                    return Err(PgError::error("could not find junk wholerow column"));
                }
            } else {
                // Other valid target relkinds must provide wholerow.
                let row_id_attno =
                    exec_find_junk_attribute_in_tlist(subplan_tlist, "wholerow");
                set_ri_row_id_attno(estate, result_rel_info, row_id_attno)?;
                if !attribute_number_is_valid(row_id_attno) {
                    return Err(PgError::error("could not find junk wholerow column"));
                }
            }
        }
    }

    // If this is an inherited update/delete/merge, there will be a "tableoid"
    // junk attribute in the subplan's targetlist.
    //   mtstate->mt_resultOidAttno =
    //       ExecFindJunkAttributeInTlist(subplan->targetlist, "tableoid");
    //   Assert(AttributeNumberIsValid(mt_resultOidAttno) || total_nrels == 1);
    mtstate.mt_resultOidAttno =
        exec_find_junk_attribute_in_tlist(subplan_targetlist(subplan)?, "tableoid") as i32;
    debug_assert!(
        attribute_number_is_valid(mtstate.mt_resultOidAttno as i16) || total_nrels == 1
    );
    mtstate.mt_lastResultOid = InvalidOid; // force lookup at first tuple
    mtstate.mt_lastResultIndex = 0; // must be zero if no such attr

    // Get the root target relation.
    //   rel = mtstate->rootResultRelInfo->ri_RelationDesc;
    let root_rri = mtstate
        .rootResultRelInfo
        .expect("rootResultRelInfo set above");
    let root_relkind = relation_relkind(estate, root_rri)?;

    // Build state for tuple routing if it's a partitioned INSERT.
    //   if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE && operation == CMD_INSERT)
    //       mtstate->mt_partition_tuple_routing =
    //           ExecSetupPartitionTupleRouting(estate, rel);
    if root_relkind == RELKIND_PARTITIONED_TABLE && operation == CmdType::CMD_INSERT {
        let rel = estate
            .result_rel(root_rri)
            .ri_RelationDesc
            .as_ref()
            .expect("ri_RelationDesc of root result rel")
            .alias();
        let proute = backend_executor_execPartition_seams::exec_setup_partition_tuple_routing::call(
            mcx, estate, rel,
        )?;
        mtstate.mt_partition_tuple_routing = Some(proute);
    }

    // Initialize any WITH CHECK OPTION constraints if needed.
    //   resultRelInfo = mtstate->resultRelInfo;
    //   foreach(l, withCheckOptionLists) {
    //       ... ExecInitQual(wco->qual, &mtstate->ps) ...
    //       resultRelInfo->ri_WithCheckOptions = wcoList;
    //       resultRelInfo->ri_WithCheckOptionExprs = wcoExprs;
    //       resultRelInfo++;
    //   }
    //
    // The own logic is the per-rel loop; compiling each WithCheckOption's qual
    // with ExecInitQual and storing the result on the ResultRelInfo is routed
    // through the execExpr owner seam (the `WithCheckOption` parse node's `qual`
    // is not modeled in the trimmed `Node` enum).
    for i in 0..with_check_option_lists.len() {
        let wco_list = with_check_option_lists[i];
        let rri = mtstate.resultRelInfo[i];
        backend_executor_execExpr_seams::exec_init_with_check_options::call(
            &mut mtstate,
            estate,
            rri,
            wco_list.as_slice(),
        )?;
    }

    // Initialize RETURNING projections if needed.
    //   if (returningLists) { ... } else { ExecInitResultTypeTL(&mtstate->ps); ps_ExprContext = NULL; }
    //
    // The RETURNING branch sets up the shared result slot + econtext on
    // mtstate->ps, then builds each rel's `ri_projectReturning` projection and
    // stores `ri_returningList`. The slot/econtext setup
    // (ExecInitResultTupleSlotTL / ExecAssignExprContext) and the projection
    // build (ExecBuildProjectionInfo) are routed through their owner seams; the
    // per-rel loop is this function's own logic. The empty-list branch
    // constructs the dummy result tuple type and clears `ps_ExprContext`.
    if !returning_lists.is_empty() {
        // Initialize result tuple slot and assign its rowtype using the plan
        // node's declared targetlist (TTSOpsVirtual).
        //   ExecInitResultTupleSlotTL(&mtstate->ps, &TTSOpsVirtual);
        backend_executor_execTuples_seams::exec_init_result_tuple_slot_tl::call(
            &mut mtstate.ps,
            estate,
            types_nodes::TupleSlotKind::Virtual,
        )?;

        // Need an econtext too.
        //   if (mtstate->ps.ps_ExprContext == NULL)
        //       ExecAssignExprContext(estate, &mtstate->ps);
        if mtstate.ps.ps_ExprContext.is_none() {
            backend_executor_execUtils_seams::exec_assign_expr_context::call(
                estate,
                &mut mtstate.ps,
            )?;
        }

        // Build a projection for each result rel.
        for i in 0..returning_lists.len() {
            let rlist = returning_lists[i];
            let rri = mtstate.resultRelInfo[i];
            backend_executor_execExpr_seams::exec_build_returning_projection::call(
                &mut mtstate,
                estate,
                rri,
                rlist.as_slice(),
            )?;
        }
    } else {
        // We still must construct a dummy result tuple type, because InitPlan
        // expects one.
        //   ExecInitResultTypeTL(&mtstate->ps);
        //   mtstate->ps.ps_ExprContext = NULL;
        exec_init_result_type_tl(&mut mtstate.ps)?;
        mtstate.ps.ps_ExprContext = None;
    }

    // Set the list of arbiter indexes if needed for ON CONFLICT.
    //   resultRelInfo = mtstate->resultRelInfo;
    //   if (node->onConflictAction != ONCONFLICT_NONE) {
    //       Assert(total_nrels == 1);
    //       resultRelInfo->ri_onConflictArbiterIndexes = node->arbiterIndexes;
    //   }
    if node.onConflictAction != ONCONFLICT_NONE {
        debug_assert_eq!(total_nrels, 1);
        let first = mtstate.resultRelInfo[0];
        let arbiter = match node.arbiterIndexes.as_ref() {
            Some(idxs) => {
                let mut v = vec_with_capacity_in(mcx, idxs.len())?;
                for oid in idxs.iter() {
                    v.push(*oid);
                }
                Some(v)
            }
            None => None,
        };
        estate.result_rel_mut(first).ri_onConflictArbiterIndexes = arbiter;
    }

    // If needed, initialize target list, projection and qual for ON CONFLICT
    // DO UPDATE.
    //   if (node->onConflictAction == ONCONFLICT_UPDATE) { ... }
    //
    // The DO UPDATE state builds an OnConflictSetState whose
    // oc_Existing/oc_ProjSlot come from table_slot_create and oc_ProjInfo from
    // ExecBuildUpdateProjection, then stores it on `ri_onConflict`. The
    // `ri_onConflict` field is not carried on the trimmed ResultRelInfo, so
    // this cannot be honored; ON CONFLICT DO UPDATE state lands with that field.
    if node.onConflictAction == OnConflictAction::ONCONFLICT_UPDATE {
        return Err(unported(
            "ExecInitModifyTable: ON CONFLICT DO UPDATE state (ri_onConflict / \
             OnConflictSetState) is not carried on the trimmed ResultRelInfo",
        ));
    }

    // If we have any secondary relations in an UPDATE or DELETE, they need to
    // be treated like non-locked relations in SELECT FOR UPDATE (and likewise
    // the source relations in a MERGE).  Locate the relevant ExecRowMarks.
    //   arowmarks = NIL;
    //   foreach(l, node->rowMarks) { ... }
    //
    // Each iteration calls ExecFindRowMark (execMain.c) + ExecBuildAuxRowMark
    // (execMain.c) to build an ExecAuxRowMark, neither of which has a declared
    // seam in this unit's dependency set; the EPQ aux-rowmark machinery lands
    // with the execMain owner. A non-empty (non-parent, unpruned) rowMarks list
    // therefore cannot be honored. The empty/parent-only case is a genuine
    // no-op (arowmarks stays NIL).
    if let Some(row_marks) = node.rowMarks.as_ref() {
        for rc in row_marks.iter() {
            let (is_parent, rti, rtekind) = plan_row_mark_fields(rc)?;
            // ignore "parent" rowmarks; they are irrelevant at runtime
            if is_parent {
                continue;
            }
            // Also ignore rowmarks for child tables pruned in ExecDoInitialPruning.
            if rtekind == RTEKind::RTE_RELATION
                && !backend_nodes_core_seams::bms_is_member::call(
                    rti as i32,
                    estate.es_unpruned_relids.as_deref(),
                )
            {
                continue;
            }
            return Err(unported(
                "ExecInitModifyTable: ExecFindRowMark / ExecBuildAuxRowMark (EPQ \
                 aux-rowmark setup) have no declared seam in this unit",
            ));
        }
    }

    // For a MERGE command, initialize its state.
    //   if (mtstate->operation == CMD_MERGE) ExecInitMerge(mtstate, estate);
    if mtstate.operation == CmdType::CMD_MERGE {
        merge::ExecInitMerge(mcx, &mut mtstate, estate)?;
    }

    // EvalPlanQualSetPlan(&mtstate->mt_epqstate, subplan, arowmarks);
    //
    // arowmarks is necessarily NIL here (a non-empty rowMarks list errored
    // above), so this records the subplan and an empty aux-rowmark list on the
    // EPQ state. EvalPlanQualSetPlan is owned by execMain.c and has no declared
    // seam in this unit; setting the plan/rowmarks is reached through that owner.
    eval_plan_qual_set_plan(&mut mtstate.mt_epqstate, subplan, mcx)?;

    // If there are a lot of result relations, use a hash table to speed the
    // lookups; otherwise a simple linear search is faster (threshold 64).
    //   if (nrels >= MT_NRELS_HASH) { hash_create(...); for (...) hash_search(HASH_ENTER); }
    //   else mtstate->mt_resultOidHash = NULL;
    if nrels >= MT_NRELS_HASH {
        let mut hash = ResultRelHash::default();
        for i in 0..nrels {
            let result_rel_info = mtstate.resultRelInfo[i];
            // hashkey = RelationGetRelid(resultRelInfo->ri_RelationDesc);
            let hashkey = relation_get_relid(estate, result_rel_info)?;
            // mtlookup = hash_search(..., HASH_ENTER, &found); Assert(!found);
            let prev = hash.entries.insert(hashkey, i as i32);
            debug_assert!(prev.is_none());
        }
        mtstate.mt_resultOidHash = Some(mcx::alloc_in(mcx, hash)?);
    } else {
        mtstate.mt_resultOidHash = None;
    }

    // Determine if the FDW supports batch insert and the batch size.  Only for
    // INSERT (UPDATE/DELETE keep batch size 0).
    //   if (operation == CMD_INSERT) {
    //       Assert(total_nrels == 1);
    //       resultRelInfo = mtstate->resultRelInfo;
    //       if (!ri_usesFdwDirectModify && ri_FdwRoutine && ri_FdwRoutine->GetForeignModifyBatchSize
    //           && ri_FdwRoutine->ExecForeignBatchInsert)
    //           resultRelInfo->ri_BatchSize = ri_FdwRoutine->GetForeignModifyBatchSize(resultRelInfo);
    //       else resultRelInfo->ri_BatchSize = 1;
    //   }
    //
    // `ri_BatchSize` / `ri_FdwRoutine` are not carried on the trimmed
    // ResultRelInfo. A non-FDW INSERT's batch size is the constant 1, which the
    // trimmed model has no field to record; the value lands with that field.
    // An FDW result rel (presence flag) reaches the GetForeignModifyBatchSize
    // path, which has no seam.
    if operation == CmdType::CMD_INSERT {
        debug_assert_eq!(total_nrels, 1);
        let first = mtstate.resultRelInfo[0];
        if estate.result_rel(first).ri_has_fdw_routine {
            return Err(unported(
                "ExecInitModifyTable: ri_FdwRoutine->GetForeignModifyBatchSize / \
                 ri_BatchSize (FDW batch insert) are not yet ported",
            ));
        }
        // resultRelInfo->ri_BatchSize = 1; — no `ri_BatchSize` field to set on
        // the trimmed ResultRelInfo; the value is the constant 1 the plain-heap
        // insert path assumes.
    }

    // Lastly, if this is not the primary (canSetTag) ModifyTable node, add it
    // to estate->es_auxmodifytables so ExecPostprocessPlan runs it to
    // completion.  Note `lcons` not `lappend`.
    //   if (!mtstate->canSetTag)
    //       estate->es_auxmodifytables = lcons(mtstate, estate->es_auxmodifytables);
    //
    // `es_auxmodifytables` carries opaque `ModifyTableState *` aliases
    // (`Opaque`) in the trimmed EState; the owned model has no shared alias to
    // the `mtstate` we are about to return by value, so a non-canSetTag node
    // cannot be registered. Auxiliary (CTE) ModifyTable nodes land with that
    // aliasing model.
    if !mtstate.canSetTag {
        return Err(unported(
            "ExecInitModifyTable: es_auxmodifytables registration of a non-canSetTag \
             ModifyTableState needs the shared ModifyTableState alias model",
        ));
    }

    // return mtstate;
    Ok(mtstate)
}

// ===========================================================================
// EXEC_FLAG_* (executor/executor.h) — eflags bits this routine tests.
// ===========================================================================

/// `EXEC_FLAG_EXPLAIN_ONLY` (executor/executor.h).
const EXEC_FLAG_EXPLAIN_ONLY: i32 = 1 << 0;
/// `EXEC_FLAG_BACKWARD` (executor/executor.h).
const EXEC_FLAG_BACKWARD: i32 = 1 << 3;
/// `EXEC_FLAG_MARK` (executor/executor.h).
const EXEC_FLAG_MARK: i32 = 1 << 4;

// ===========================================================================
// Small inline helpers (access/attnum.h, nodes/pg_list.h shapes).
// ===========================================================================

/// `AttributeNumberIsValid(attno)` (access/attnum.h) — `InvalidAttrNumber == 0`.
#[inline]
fn attribute_number_is_valid(attno: i16) -> bool {
    attno != 0
}

/// `list_nth_node(List, list, n)` / `list_nth(list, n)` — the n-th element, by
/// reference. The C macros `elog(ERROR)` past the end (a planner invariant
/// violation); the owned model returns an `Err`.
fn list_nth_ref<'a, T>(list: &'a PgVec<'_, T>, n: usize) -> PgResult<&'a T> {
    list.get(n).ok_or_else(|| {
        PgError::error(alloc::format!(
            "ExecInitModifyTable: list_nth index {n} out of range (len {})",
            list.len()
        ))
    })
}

/// The subplan (outer plan) targetlist, mirroring `subplan->targetlist`. A
/// missing subplan or NIL targetlist is the C `NIL` (empty slice).
fn subplan_targetlist<'a, 'mcx>(
    subplan: Option<&'a types_nodes::nodes::Node<'mcx>>,
) -> PgResult<&'a [TargetEntry<'mcx>]> {
    match subplan {
        Some(node) => Ok(node
            .plan_head()
            .targetlist
            .as_deref()
            .map(|v| &v[..])
            .unwrap_or(&[])),
        None => Ok(&[]),
    }
}

/// A loud error for an operation owned by a neighbor unit that is not yet
/// ported, or for a `ResultRelInfo`/`EState` field that the trimmed node model
/// does not yet carry. Surfaces as an `Err` so the executor's error path runs,
/// mirroring the C `ereport(ERROR)`.
fn unported(what: &str) -> PgError {
    PgError::error(alloc::format!(
        "backend-executor-nodeModifyTable::init: unported neighbor/field: {what}"
    ))
}

// ===========================================================================
// Unported-owner shims. Each is a callee owned by a neighbor unit (execUtils,
// execMain, nodeFuncs, dynahash) for which no seam is declared in this unit's
// dependency set, and whose inputs/outputs touch fields not yet carried on the
// trimmed node model. Per mirror-PG-and-panic these panic loudly when reached.
// ===========================================================================

/// `ExecInitResultRelation(estate, resultRelInfo, rti)` (execUtils.c): open the
/// rti'th range-table relation (via `ExecGetRangeTableRelation`) and fill the
/// `ResultRelInfo` for it (`InitResultRelInfo`), recording it in
/// `es_result_relations[rti-1]` and prepending it to
/// `es_opened_result_relations`.
///
/// execUtils.c is not in this unit's dependency set (no `ExecInitResultRelation`
/// seam is declared), and the open reads `es_relations`/`es_range_table` and the
/// relcache through that owner; reached through the execUtils owner when it
/// lands.
fn exec_init_result_relation<'mcx>(
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    rti: Index,
) -> PgResult<()> {
    let _ = (estate, result_rel_info, rti);
    panic!(
        "ExecInitResultRelation (execUtils.c): no seam declared in this unit — \
         relation open + InitResultRelInfo are owned by execUtils"
    );
}

/// `CheckValidResultRel(resultRelInfo, operation, onConflictAction,
/// mergeActions)` (execMain.c): verify the relation is a valid target for the
/// operation (relkind/triggers/view-rules checks), `ereport(ERROR)` otherwise.
///
/// execMain.c declares no `CheckValidResultRel` seam in this unit's dependency
/// set, and the checks read the relation's relkind/trigger/rule state through
/// the relcache; reached through the execMain owner when it lands.
fn check_valid_result_rel<'mcx>(
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    operation: CmdType,
    on_conflict_action: OnConflictAction,
    merge_actions: Option<&[MergeAction<'mcx>]>,
) -> PgResult<()> {
    let _ = (estate, result_rel_info, operation, on_conflict_action, merge_actions);
    panic!(
        "CheckValidResultRel (execMain.c): no seam declared in this unit — \
         valid-target validation is owned by execMain"
    );
}

/// `EvalPlanQualInit(epqstate, parentestate, subplan, auxrowmarks, epqParam,
/// resultRelations)` (execMain.c): initialize an `EPQState` with dummy subplan
/// data, recording `epqParam` and `resultRelations`.
///
/// execMain.c declares no `EvalPlanQualInit` seam in this unit's dependency
/// set; the EPQ machinery is owned by execMain. Reached through that owner when
/// it lands.
fn eval_plan_qual_init<'mcx>(
    epqstate: &mut EPQState<'mcx>,
    estate: &mut EStateData<'mcx>,
    epq_param: i32,
    result_relations: &PgVec<'mcx, Index>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let _ = (epqstate, estate, epq_param, result_relations, mcx);
    panic!(
        "EvalPlanQualInit (execMain.c): no seam declared in this unit — \
         EvalPlanQual state init is owned by execMain"
    );
}

/// `EvalPlanQualSetPlan(epqstate, subplan, auxrowmarks)` (execMain.c): record
/// the recheck plan tree and the aux-rowmark list on the EPQ state.
///
/// execMain.c declares no `EvalPlanQualSetPlan` seam in this unit's dependency
/// set. Reached through the execMain owner when it lands.
fn eval_plan_qual_set_plan<'mcx>(
    epqstate: &mut EPQState<'mcx>,
    subplan: Option<&'mcx types_nodes::nodes::Node<'mcx>>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let _ = (epqstate, subplan, mcx);
    panic!(
        "EvalPlanQualSetPlan (execMain.c): no seam declared in this unit — \
         owned by execMain"
    );
}

/// `ExecFindJunkAttributeInTlist(targetlist, attrName)` (execJunk.c): find the
/// resno of the junk `TargetEntry` whose `resname` is `attr_name`, or
/// `InvalidAttrNumber` (0) if none.
///
/// execJunk.c is not in this unit's dependency set (no seam declared), and the
/// `TargetEntry.resname`/`resjunk` lookup it performs is owned by that unit.
fn exec_find_junk_attribute_in_tlist(target_list: &[TargetEntry<'_>], attr_name: &str) -> i16 {
    let _ = (target_list, attr_name);
    panic!(
        "ExecFindJunkAttributeInTlist (execJunk.c): no seam declared in this unit — \
         junk-attribute lookup is owned by execJunk"
    );
}

/// `resultRelInfo->ri_RowIdAttNo = attno` — store the row-identity junk
/// attribute number. `ri_RowIdAttNo` is not carried on the trimmed
/// ResultRelInfo (the UPDATE/DELETE/MERGE row-identity machinery lands with
/// that field), so the store is not expressible.
fn set_ri_row_id_attno<'mcx>(
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    attno: i16,
) -> PgResult<()> {
    let _ = (estate, result_rel_info, attno);
    panic!(
        "ResultRelInfo.ri_RowIdAttNo store: the row-identity junk-attr field is \
         not carried on the trimmed ResultRelInfo"
    );
}

/// `resultRelInfo->ri_RelationDesc->rd_rel->relkind` — the target relation's
/// relkind byte.
fn relation_relkind<'mcx>(estate: &EStateData<'mcx>, rri: RriId) -> PgResult<u8> {
    let rel = estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .ok_or_else(|| unported("ResultRelInfo.ri_RelationDesc is NULL"))?;
    Ok(rel.rd_rel.relkind)
}

/// `RelationGetRelid(resultRelInfo->ri_RelationDesc)` — the target relation's
/// OID (`rd_id`).
fn relation_get_relid<'mcx>(
    estate: &EStateData<'mcx>,
    rri: RriId,
) -> PgResult<types_core::Oid> {
    let rel = estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .ok_or_else(|| unported("ResultRelInfo.ri_RelationDesc is NULL"))?;
    Ok(rel.rd_id)
}

/// `ExecInitResultTypeTL(planstate)` (execTuples.c): set the node's result
/// tuple descriptor from the plan's targetlist, without creating a result slot.
///
/// execTuples.c declares no `ExecInitResultTypeTL` seam in this unit's
/// dependency set (only the slot-creating `ExecInitResultTupleSlotTL`); the
/// type-only setup is owned by that unit. Reached through it when it lands.
fn exec_init_result_type_tl<'mcx>(planstate: &mut PlanStateData<'mcx>) -> PgResult<()> {
    let _ = planstate;
    panic!(
        "ExecInitResultTypeTL (execTuples.c): no seam declared in this unit — \
         owned by execTuples"
    );
}

/// Read the `isParent` / `rti` / RTE-kind fields the rowmarks loop consumes off
/// a `PlanRowMark` node. The `PlanRowMark` node is not represented in the
/// trimmed `Node` enum (only `Material`/`MergeJoin`), so its fields cannot be
/// read; `PlanRowMark` lands with the plannodes owner.
fn plan_row_mark_fields(
    rc: &PgBox<'_, types_nodes::nodes::Node<'_>>,
) -> PgResult<(bool, Index, RTEKind)> {
    let _ = rc;
    panic!(
        "PlanRowMark field access: PlanRowMark is not represented in the trimmed \
         Node enum — owned by the plannodes vocabulary"
    );
}

extern crate alloc;
