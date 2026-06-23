//! MERGE family of `executor/nodeModifyTable.c`: the MERGE driver, the
//! not-matched action dispatch, and the per-action exec-state and tuple-slot
//! initialization. The matched-action dispatch (`ExecMergeMatched`) lives in
//! the [`crate::merge_matched`] sub-module.

use ::mcx::Mcx;
use ::types_error::PgResult;
use ::nodes::nodes::CmdType;
use nodes::{EStateData, ModifyTableState, RriId, SlotId};
use ::types_tuple::heaptuple::FormedTuple;
use ::types_tuple::heaptuple::ItemPointerData;

use crate::{insert_exec, ModifyTableContext};

extern crate alloc;

/// `MERGE_INSERT` (execnodes.h) — MERGE subcommand mask bit.
const MERGE_INSERT: i32 = 0x01;
/// `MERGE_UPDATE` (execnodes.h) — MERGE subcommand mask bit.
const MERGE_UPDATE: i32 = 0x02;
/// `MERGE_DELETE` (execnodes.h) — MERGE subcommand mask bit.
const MERGE_DELETE: i32 = 0x04;

/// `ExecMerge(context, resultRelInfo, tupleid, oldtuple, canSetTag)` — execute
/// the MERGE actions for one source row: dispatch to the matched or
/// not-matched action list depending on whether a target tuple
/// (`tupleid`/`oldtuple`) was found. Returns the RETURNING slot or `None`.
pub fn ExecMerge<'mcx>(
    mcx: Mcx<'mcx>,
    context: &mut ModifyTableContext,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    tupleid: Option<&ItemPointerData>,
    oldtuple: Option<FormedTuple<'mcx>>,
    can_set_tag: bool,
) -> PgResult<Option<SlotId>> {
    let mut rslot: Option<SlotId> = None;

    // matched = tupleid != NULL || oldtuple != NULL;
    let mut matched = tupleid.is_some() || oldtuple.is_some();
    if matched {
        rslot = crate::merge_matched::ExecMergeMatched(
            mcx,
            context,
            mtstate,
            estate,
            result_rel_info,
            tupleid,
            oldtuple,
            can_set_tag,
            &mut matched,
        )?;
    }

    // Deal with the NOT MATCHED case (either a NOT MATCHED tuple from the join,
    // or a previously MATCHED tuple for which ExecMergeMatched() set "matched"
    // to false, indicating that it no longer matches).
    if !matched {
        // If a concurrent update turned a MATCHED case into a NOT MATCHED case,
        // and we have both WHEN NOT MATCHED BY SOURCE and WHEN NOT MATCHED [BY
        // TARGET] actions, and there is a RETURNING clause, ExecMergeMatched()
        // may have already executed a WHEN NOT MATCHED BY SOURCE action, and
        // computed the row to return.  If so, we cannot execute a WHEN NOT
        // MATCHED [BY TARGET] action now, so mark it as pending (to be processed
        // on the next call to ExecModifyTable()).  Otherwise, just process the
        // action now.
        if rslot.is_none() {
            rslot = ExecMergeNotMatched(mcx, context, mtstate, estate, result_rel_info, can_set_tag)?;
        } else {
            mtstate.mt_merge_pending_not_matched = context.planSlot;
        }
    }

    Ok(rslot)
}

/// `ExecMergeNotMatched(context, resultRelInfo, canSetTag)` — run the WHEN NOT
/// MATCHED [BY TARGET] actions, performing the chosen INSERT/DO NOTHING.
/// Returns the RETURNING slot or `None`.
pub fn ExecMergeNotMatched<'mcx>(
    mcx: Mcx<'mcx>,
    context: &mut ModifyTableContext,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    can_set_tag: bool,
) -> PgResult<Option<SlotId>> {
    use ::nodes::modifytable::MERGE_WHEN_NOT_MATCHED_BY_TARGET;

    // ExprContext *econtext = mtstate->ps.ps_ExprContext;
    let econtext = mtstate
        .ps
        .ps_ExprContext
        .expect("MERGE node has an expression context");

    let mut rslot: Option<SlotId> = None;

    // For INSERT actions, the root relation's merge action is OK since the
    // INSERT's targetlist and the WHEN conditions can only refer to the source
    // relation and hence it does not matter which result relation we work with.
    //
    //   actionStates = resultRelInfo->ri_MergeActions[MERGE_WHEN_NOT_MATCHED_BY_TARGET];
    //
    // Count the not-matched-by-target actions (the action states live on the
    // pooled ResultRelInfo; we index them so the per-action ExecInsert can
    // re-borrow estate between iterations).
    let n_actions = estate.result_rel(result_rel_info).ri_MergeActions
        [MERGE_WHEN_NOT_MATCHED_BY_TARGET as usize]
        .as_ref()
        .map(|l| l.len())
        .unwrap_or(0);

    // Make source tuple available to ExecQual and ExecProject. We don't need the
    // target tuple, since the WHEN quals and targetlist can't refer to the
    // target columns.
    //
    //   econtext->ecxt_scantuple = NULL;
    //   econtext->ecxt_innertuple = context->planSlot;
    //   econtext->ecxt_outertuple = NULL;
    let plan_slot = context.planSlot;
    let ecxt = estate.ecxt_mut(econtext);
    ecxt.ecxt_scantuple = None;
    ecxt.ecxt_innertuple = plan_slot;
    ecxt.ecxt_outertuple = None;

    // foreach(l, actionStates) { ... }
    let root_result_rel_info = mtstate
        .rootResultRelInfo
        .expect("MERGE node has a root result relation");
    for idx in 0..n_actions {
        // MergeActionState *action = lfirst(l);
        // CmdType commandType = action->mas_action->commandType;
        //
        // ExprState pi_state / ProjectionInfo are small trimmed structs; clone
        // the action's whenqual + projection out of the pool so the estate
        // borrow is free for ExecQual / ExecProject / ExecInsert.
        // A compiled `ExprState` / `ProjectionInfo` cannot be `.clone()`d (the
        // step program is context-allocated; the derived clone is a loud guard)
        // and cannot be borrowed `&mut` while `estate` is also borrowed `&mut`
        // because it lives inside `estate`. So we MOVE it out of the pooled
        // `MergeActionState` (leaving `None`), use it, and restore it before the
        // loop continues. This faithfully mirrors C aliasing the same ExprState.
        let (command_type, action_match_kind, action_overriding, mut whenqual, mut proj) = {
            let action = &mut estate.result_rel_mut(result_rel_info).ri_MergeActions
                [MERGE_WHEN_NOT_MATCHED_BY_TARGET as usize]
                .as_mut()
                .expect("not-matched-by-target action list present")[idx];
            let mas_action = action
                .mas_action
                .as_ref()
                .expect("MergeActionState has a MergeAction");
            let command_type = mas_action.commandType;
            let action_match_kind = mas_action.matchKind;
            let action_overriding = mas_action.overriding;
            let whenqual = action.mas_whenqual.take();
            let proj = action.mas_proj.take();
            (command_type, action_match_kind, action_overriding, whenqual, proj)
        };

        // Restore the moved-out compiled states into the pooled action (called
        // before every loop exit so a later iteration / re-entry finds them).
        macro_rules! restore_action_states {
            () => {{
                let action = &mut estate.result_rel_mut(result_rel_info).ri_MergeActions
                    [MERGE_WHEN_NOT_MATCHED_BY_TARGET as usize]
                    .as_mut()
                    .expect("not-matched-by-target action list present")[idx];
                action.mas_whenqual = whenqual.take();
                action.mas_proj = proj.take();
            }};
        }

        // Test condition, if any.  In the absence of any condition, we perform
        // the action unconditionally (ExecQual returns true with no conditions).
        //   if (!ExecQual(action->mas_whenqual, econtext)) continue;
        let passed = match whenqual.as_mut() {
            Some(state) => {
                execExpr_seams::exec_qual::call(state, econtext, estate)?
            }
            None => true,
        };
        if !passed {
            restore_action_states!();
            continue;
        }

        // Perform stated action.
        match command_type {
            CmdType::CMD_INSERT => {
                // Project the tuple.  In case of a partitioned table, the
                // projection was already built to use the root's descriptor, so
                // we don't need to map the tuple here.
                //   newslot = ExecProject(action->mas_proj);
                let proj_box = proj
                    .as_mut()
                    .expect("CMD_INSERT MERGE action has a projection");
                let newslot =
                    execExpr_seams::exec_project_info::call(&mut **proj_box, estate)?;

                // mtstate->mt_merge_action = action;
                //
                // ExecInsert reads `mtstate->mt_merge_action->mas_action->commandType`
                // to pick the RLS WCO kind (UPDATE vs INSERT) under MERGE, so the
                // active action must be tracked. The owned-by-value tree can't
                // alias the pooled `MergeActionState`, so we materialize an owned
                // one carrying the active action's identity, mirroring how
                // ExecInitMerge builds the pooled state.
                mtstate.mt_merge_action = Some(::mcx::alloc_in(
                    mcx,
                    ::nodes::modifytable::MergeActionState {
                        type_: ::nodes::nodes::T_MergeActionState,
                        mas_action: Some(::mcx::alloc_in(
                            mcx,
                            ::nodes::modifytable::MergeAction {
                                matchKind: action_match_kind,
                                commandType: command_type,
                                overriding: action_overriding,
                                qual: None,
                                targetList: None,
                                updateColnos: None,
                            },
                        )?),
                        mas_proj: None,
                        mas_whenqual: None,
                    },
                )?);

                // rslot = ExecInsert(context, mtstate->rootResultRelInfo,
                //                    newslot, canSetTag, NULL, NULL);
                rslot = insert_exec::ExecInsert(
                    mcx,
                    context,
                    mtstate,
                    estate,
                    root_result_rel_info,
                    newslot,
                    can_set_tag,
                    None,
                    None,
                )?;

                // mtstate->mt_merge_inserted += 1;
                mtstate.mt_merge_inserted += 1.0;
            }
            CmdType::CMD_NOTHING => {
                // Do nothing.
            }
            _ => {
                // default: elog(ERROR, "unknown action in MERGE WHEN NOT MATCHED clause");
                return Err(::types_error::PgError::error(
                    "unknown action in MERGE WHEN NOT MATCHED clause",
                ));
            }
        }

        // We've activated one of the WHEN clauses, so we don't search further.
        // This is required behaviour, not an optimization.
        restore_action_states!();
        break;
    }

    Ok(rslot)
}

/// `ExecInitMerge(mtstate, estate)` — build the per-action `MergeActionState`
/// list (projection + WHEN qual + per-rel slots) for every result relation's
/// merge action list.
pub fn ExecInitMerge<'mcx>(
    mcx: Mcx<'mcx>,
    node: &'mcx ::nodes::ModifyTable<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = node;

    // List *mergeActionLists = mtstate->mt_mergeActionLists;
    // if (mergeActionLists == NIL) return;
    //
    // Snapshot the per-rel `&'mcx` action-list borrows so the loop can re-borrow
    // mtstate mutably between iterations (the referents live in the plan tree).
    let action_lists: alloc::vec::Vec<&'mcx ::mcx::PgVec<'mcx, ::nodes::modifytable::MergeAction<'mcx>>> =
        match mtstate.mt_mergeActionLists.as_ref() {
            None => return Ok(()),
            Some(lists) => lists.iter().copied().collect(),
        };

    let root_rel_info = mtstate
        .rootResultRelInfo
        .expect("MERGE node has a root result relation");

    // mtstate->mt_merge_subcommands = 0;
    mtstate.mt_merge_subcommands = 0;

    // if (mtstate->ps.ps_ExprContext == NULL)
    //     ExecAssignExprContext(estate, &mtstate->ps);
    // econtext = mtstate->ps.ps_ExprContext;
    if mtstate.ps.ps_ExprContext.is_none() {
        execUtils_seams::exec_assign_expr_context::call(estate, &mut mtstate.ps)?;
    }
    let econtext = mtstate
        .ps
        .ps_ExprContext
        .expect("MERGE node has an expression context");

    // Create a MergeActionState for each action on the mergeActionList and add
    // it to either a list of matched actions or not-matched actions.
    //
    //   i = 0; foreach(lc, mergeActionLists) { ... }
    for (i, merge_action_list) in action_lists.iter().enumerate() {
        // joinCondition = (Node *) list_nth(mergeJoinConditions, i);
        // Modeled as the implicit-AND `List` of `Expr` ExecInitQual consumes.
        let join_condition: Option<&'mcx [::nodes::primnodes::Expr]> = mtstate
            .mt_mergeJoinConditions
            .as_ref()
            .and_then(|jc| jc.get(i).copied().flatten())
            .map(|v| v.as_slice());

        // resultRelInfo = mtstate->resultRelInfo + i;  i++;
        let result_rel_info = mtstate.resultRelInfo[i];

        // initialize slots for MERGE fetches from this rel
        //   if (unlikely(!resultRelInfo->ri_projectNewInfoValid))
        //       ExecInitMergeTupleSlots(mtstate, resultRelInfo);
        if !estate.result_rel(result_rel_info).ri_projectNewInfoValid {
            ExecInitMergeTupleSlots(mcx, mtstate, estate, result_rel_info)?;
        }

        // initialize state for join condition checking
        //   resultRelInfo->ri_MergeJoinCondition =
        //       ExecInitQual((List *) joinCondition, &mtstate->ps);
        execExpr_seams::exec_init_merge_join_condition::call(
            mtstate,
            estate,
            result_rel_info,
            join_condition,
        )?;

        // foreach(l, mergeActionList) — build each action's MergeActionState
        // (WHEN qual + per-command projection), append it into
        // resultRelInfo->ri_MergeActions[action->matchKind], and accumulate the
        // MERGE_INSERT/UPDATE/DELETE bit into mtstate->mt_merge_subcommands. The
        // per-action loop, the commandType switch, the mt_merge_subcommands
        // accumulation, and the CMD_INSERT partitioned-vs-inherited decision are
        // this unit's own control flow and state and stay in-crate; only the
        // ExecInitQual / ExecBuildProjectionInfo / ExecBuildUpdateProjection /
        // ExecSetupPartitionTupleRouting leaf primitives go through their owner
        // seams.
        let n_actions = merge_action_list.len();
        for ai in 0..n_actions {
            // MergeAction *action = (MergeAction *) lfirst(l);
            //
            // Snapshot the action's match kind / command type / qual / target
            // list / update colnos out of the `&'mcx` plan-tree borrow so the
            // mtstate/estate borrows are free for the leaf seams.
            let action = &merge_action_list[ai];
            let match_kind = action.matchKind;
            let command_type = action.commandType;
            let qual: Option<&'mcx [::nodes::primnodes::Expr]> =
                action.qual.as_ref().map(|q| q.as_slice());
            let target_list: &'mcx [::nodes::TargetEntry<'mcx>] = action
                .targetList
                .as_ref()
                .map(|t| t.as_slice())
                .unwrap_or(&[]);
            let update_colnos: &'mcx [i32] = action
                .updateColnos
                .as_ref()
                .map(|u| u.as_slice())
                .unwrap_or(&[]);

            // action_state = makeNode(MergeActionState);
            // action_state->mas_action = action;
            // action_state->mas_whenqual = ExecInitQual((List *) action->qual,
            //                                            &mtstate->ps);
            let mas_whenqual = execExpr_seams::exec_init_merge_when_qual::call(
                mtstate, estate, qual,
            )?;

            // We create three lists - one for each MergeMatchKind - and stick
            // the MergeActionState into the appropriate list. The projection is
            // filled in by the switch below before the state is appended.
            let mas_proj: Option<::mcx::PgBox<'mcx, ::nodes::execexpr::ProjectionInfo>>;

            match command_type {
                CmdType::CMD_INSERT => {
                    // INSERT actions always use rootRelInfo.
                    //   ExecCheckPlanOutput(rootRelInfo->ri_RelationDesc,
                    //                       action->targetList);
                    let root_rel = estate
                        .result_rel(root_rel_info)
                        .ri_RelationDesc
                        .as_ref()
                        .expect("MERGE root ResultRelInfo has an open relation")
                        .alias();
                    crate::lifecycle::ExecCheckPlanOutput(root_rel, target_list)?;

                    // If the MERGE targets a partitioned table, any INSERT
                    // actions must be routed through it. Initialize the routing
                    // struct and the root table's "new" tuple slot, if not done.
                    // The projection (for all relations) uses the root relation
                    // descriptor and targets the plan's root slot.
                    let root_relkind = estate
                        .result_rel(root_rel_info)
                        .ri_RelationDesc
                        .as_ref()
                        .map(|r| r.rd_rel.relkind)
                        .unwrap_or(0);
                    let tgt_slot: ::nodes::SlotId;
                    if root_relkind == ::types_tuple::access::RELKIND_PARTITIONED_TABLE {
                        if mtstate.mt_partition_tuple_routing.is_none() {
                            // mtstate->mt_root_tuple_slot =
                            //     table_slot_create(rootRelInfo->ri_RelationDesc, NULL);
                            // (managed as a standalone ModifyTableState slot — the
                            // C passes NULL for the tupleTable list.)
                            let rel = estate
                                .result_rel(root_rel_info)
                                .ri_RelationDesc
                                .as_ref()
                                .expect("MERGE root ResultRelInfo has an open relation")
                                .alias();
                            let root_slot =
                                table_tableam::table_slot_create(mcx, &rel)?;
                            let root_slot_id = estate.push_slot_data(root_slot)?;
                            mtstate.mt_root_tuple_slot = Some(root_slot_id);

                            // mtstate->mt_partition_tuple_routing =
                            //     ExecSetupPartitionTupleRouting(estate,
                            //         rootRelInfo->ri_RelationDesc);
                            let rel2 = estate
                                .result_rel(root_rel_info)
                                .ri_RelationDesc
                                .as_ref()
                                .expect("MERGE root ResultRelInfo has an open relation")
                                .alias();
                            mtstate.mt_partition_tuple_routing = Some(
                                execPartition_seams::exec_setup_partition_tuple_routing::call(
                                    mcx, estate, rel2,
                                )?,
                            );
                        }
                        // tgtslot = mtstate->mt_root_tuple_slot;
                        // tgtdesc = RelationGetDescr(rootRelInfo->ri_RelationDesc);
                        tgt_slot = mtstate
                            .mt_root_tuple_slot
                            .expect("partitioned-root MERGE has a root tuple slot");
                    } else {
                        // If the MERGE targets an inherited table, we insert into
                        // the root table, so initialize its "new" tuple slot if
                        // not already done and use its relation descriptor for the
                        // projection. (For non-inherited tables rootRelInfo and
                        // resultRelInfo are the same and the slot is set already.)
                        //   if (rootRelInfo->ri_newTupleSlot == NULL)
                        //       rootRelInfo->ri_newTupleSlot =
                        //           table_slot_create(rootRelInfo->ri_RelationDesc,
                        //                              &estate->es_tupleTable);
                        if estate.result_rel(root_rel_info).ri_newTupleSlot.is_none() {
                            let rel = estate
                                .result_rel(root_rel_info)
                                .ri_RelationDesc
                                .as_ref()
                                .expect("MERGE root ResultRelInfo has an open relation")
                                .alias();
                            let new_slot =
                                table_tableam::table_slot_create(mcx, &rel)?;
                            let new_slot_id = estate.push_slot_data(new_slot)?;
                            estate.result_rel_mut(root_rel_info).ri_newTupleSlot =
                                Some(new_slot_id);
                        }
                        // tgtslot = rootRelInfo->ri_newTupleSlot;
                        // tgtdesc = RelationGetDescr(rootRelInfo->ri_RelationDesc);
                        tgt_slot = estate
                            .result_rel(root_rel_info)
                            .ri_newTupleSlot
                            .expect("inherited-root MERGE has a root new tuple slot");
                    }

                    // action_state->mas_proj =
                    //     ExecBuildProjectionInfo(action->targetList, econtext,
                    //                             tgtslot, &mtstate->ps, tgtdesc);
                    mas_proj = Some(
                        execExpr_seams::exec_build_merge_insert_projection::call(
                            mtstate,
                            estate,
                            target_list,
                            econtext,
                            tgt_slot,
                            root_rel_info,
                        )?,
                    );

                    // mtstate->mt_merge_subcommands |= MERGE_INSERT;
                    mtstate.mt_merge_subcommands |= MERGE_INSERT;
                }
                CmdType::CMD_UPDATE => {
                    // action_state->mas_proj = ExecBuildUpdateProjection(
                    //     action->targetList, true, action->updateColnos,
                    //     relationDesc, econtext,
                    //     resultRelInfo->ri_newTupleSlot, &mtstate->ps);
                    mas_proj = Some(
                        execExpr_seams::exec_build_merge_update_projection::call(
                            mtstate,
                            estate,
                            result_rel_info,
                            target_list,
                            update_colnos,
                            econtext,
                        )?,
                    );
                    // mtstate->mt_merge_subcommands |= MERGE_UPDATE;
                    mtstate.mt_merge_subcommands |= MERGE_UPDATE;
                }
                CmdType::CMD_DELETE => {
                    // mtstate->mt_merge_subcommands |= MERGE_DELETE;
                    mas_proj = None;
                    mtstate.mt_merge_subcommands |= MERGE_DELETE;
                }
                CmdType::CMD_NOTHING => {
                    mas_proj = None;
                }
                _ => {
                    // default: elog(ERROR, "unknown action in MERGE WHEN clause");
                    return Err(::types_error::PgError::error(
                        "unknown action in MERGE WHEN clause",
                    ));
                }
            }

            // resultRelInfo->ri_MergeActions[action->matchKind] =
            //     lappend(resultRelInfo->ri_MergeActions[action->matchKind],
            //             action_state);
            //
            // Build the owned MergeActionState. The C `mas_action = action`
            // aliases the plan `MergeAction`; the owned-by-value tree cannot
            // share the `&'mcx` plan borrow into the pooled state, so `mas_action`
            // carries the command/match-kind/overriding fields the executor reads
            // (ExecMergeMatched reads only `commandType`/`matchKind`).
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

            // resultRelInfo->ri_MergeActions[action->matchKind] =
            //     lappend(.., action_state);
            let slot = &mut estate.result_rel_mut(result_rel_info).ri_MergeActions
                [match_kind as usize];
            match slot {
                Some(list) => list.push(action_state),
                None => {
                    let mut v = ::mcx::PgVec::new_in(mcx);
                    v.push(action_state);
                    *slot = Some(v);
                }
            }
        }
    }

    // If the MERGE targets an inherited table, any INSERT actions will use
    // rootRelInfo, which is not in the resultRelInfo array; initialize its WITH
    // CHECK OPTION constraints and RETURNING projection (as ExecInitModifyTable
    // did for the resultRelInfo entries).
    //
    //   if (rootRelInfo != mtstate->resultRelInfo &&
    //       rootRelInfo->ri_RelationDesc->rd_rel->relkind != RELKIND_PARTITIONED_TABLE &&
    //       (mtstate->mt_merge_subcommands & MERGE_INSERT) != 0) { ... }
    let first_result_rel = mtstate.resultRelInfo[0];
    let root_relkind = estate
        .result_rel(root_rel_info)
        .ri_RelationDesc
        .as_ref()
        .map(|r| r.rd_rel.relkind)
        .unwrap_or(0);
    if root_rel_info != first_result_rel
        && root_relkind != ::types_tuple::access::RELKIND_PARTITIONED_TABLE
        && (mtstate.mt_merge_subcommands & MERGE_INSERT) != 0
    {
        ExecInitMergeInheritedRoot(mcx, mtstate, estate, root_rel_info, first_result_rel)?;
    }

    Ok(())
}

/// The inherited-root WITH CHECK OPTION / RETURNING setup of `ExecInitMerge`
/// (nodeModifyTable.c L3856-3947): when a MERGE targets an inherited
/// (non-partitioned) table whose root `ResultRelInfo` is not in the
/// `resultRelInfo[]` array, initialize the root rel's WCO constraints and
/// RETURNING projection — taking the first plan WCO/RETURNING list as the
/// reference and `build_attrmap_by_name` + `map_variable_attnos`-remapping it to
/// the root's attnos when the root and first result relation differ. Mirrors the
/// `ExecInitPartition{WithCheckOptions,Returning}` legs (the per-rel WCO/RETURNING
/// compile is the same execExpr-owned path).
fn ExecInitMergeInheritedRoot<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    root_rel_info: RriId,
    first_result_rel: RriId,
) -> PgResult<()> {
    // ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
    let node = match mtstate.plan_node {
        Some(n) => n,
        None => return Ok(()),
    };

    // Relation rootRelation = rootRelInfo->ri_RelationDesc;
    // Relation firstResultRel = mtstate->resultRelInfo[0].ri_RelationDesc;
    // int firstVarno = mtstate->resultRelInfo[0].ri_RangeTableIndex;
    let first_varno = estate.result_rel(first_result_rel).ri_RangeTableIndex;

    // The C decides whether a remap is needed by comparing the Relation pointers
    // (rootRelation != firstResultRel). The owned model compares by relation OID.
    let root_oid = estate
        .result_rel(root_rel_info)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecInitMergeInheritedRoot: root ResultRelInfo has no open relation")
        .rd_id;
    let first_oid = estate
        .result_rel(first_result_rel)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecInitMergeInheritedRoot: first result ResultRelInfo has no open relation")
        .rd_id;
    let needs_remap = root_oid != first_oid;

    // part_attmap = build_attrmap_by_name(RelationGetDescr(rootRelation),
    //                                     RelationGetDescr(firstResultRel), false);
    // (built lazily only when a remap is required; shared by the WCO and RETURNING
    // legs as in the C.)
    let (root_attnums, root_reltype) = if needs_remap {
        let root_desc = estate
            .result_rel(root_rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecInitMergeInheritedRoot: root ResultRelInfo has no open relation")
            .rd_att_clone_in(mcx)?;
        let root_reltype = estate
            .result_rel(root_rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecInitMergeInheritedRoot: root ResultRelInfo has no open relation")
            .rd_rel
            .reltype;
        let first_desc = estate
            .result_rel(first_result_rel)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecInitMergeInheritedRoot: first result ResultRelInfo has no open relation")
            .rd_att_clone_in(mcx)?;
        let attmap = next_seams::build_attrmap_by_name::call(
            mcx,
            &root_desc,
            &first_desc,
            false,
        )?;
        let attnums: alloc::vec::Vec<i16> = attmap.attnums.iter().copied().collect();
        (Some(attnums), root_reltype)
    } else {
        (None, 0)
    };

    // if (node->withCheckOptionLists != NIL) { ... }
    if let Some(lists) = node.withCheckOptionLists.as_ref() {
        if !lists.is_empty() {
            // wcoList = linitial(node->withCheckOptionLists);
            let ref_wco_list = lists[0].as_slice();
            // Clone each WithCheckOption out of the plan node so its qual can be
            // remapped without mutating the shared plan (mirrors
            // ExecInitPartitionWithCheckOptions).
            let mut remapped_wco_nodes: ::mcx::PgVec<'mcx, ::nodes::nodes::Node<'mcx>> =
                ::mcx::vec_with_capacity_in(mcx, ref_wco_list.len())?;
            for wco_node in ref_wco_list {
                let wco = wco_node.as_withcheckoption().ok_or_else(|| {
                    ::types_error::PgError::error(
                        "ExecInitMergeInheritedRoot: withCheckOptionLists element is not a \
                         WithCheckOption node",
                    )
                })?;
                let mut wco = wco.clone_in(mcx)?;
                // map_variable_attnos((Node *) wcoList, firstVarno, 0, part_attmap,
                //                     rootRelation reltype, &found_whole_row);
                if let Some(attnums) = root_attnums.as_ref() {
                    if let Some(qual) = wco.qual.take() {
                        let (mapped_qual, _found_whole_row) =
                            rewritemanip_seams::map_variable_attnos_node::call(
                                mcx,
                                qual,
                                first_varno as i32,
                                0,
                                attnums,
                                root_reltype,
                            )?;
                        wco.qual = Some(mapped_qual);
                    }
                }
                remapped_wco_nodes
                    .push(::nodes::nodes::Node::mk_with_check_option(mcx, wco)?);
            }
            // foreach(lc, wcoList) ExecInitQual ... ; rootRelInfo->ri_WithCheckOptions
            // = wcoList; ri_WithCheckOptionExprs = wcoExprs;
            execExpr_seams::exec_init_with_check_options::call(
                mtstate,
                estate,
                root_rel_info,
                remapped_wco_nodes.as_slice(),
            )?;
        }
    }

    // if (node->returningLists != NIL) { ... }
    if let Some(lists) = node.returningLists.as_ref() {
        if !lists.is_empty() {
            // returningList = linitial(node->returningLists);
            let ref_returning_list = lists[0].as_slice();
            let mut returning_list: ::mcx::PgVec<'mcx, ::nodes::primnodes::TargetEntry<'mcx>> =
                ::mcx::vec_with_capacity_in(mcx, ref_returning_list.len())?;
            for tle in ref_returning_list {
                returning_list.push(tle.clone_in(mcx)?);
            }
            // map_variable_attnos((Node *) returningList, firstVarno, 0, part_attmap,
            //                     rootRelation reltype, &found_whole_row);
            let returning_list = if let Some(attnums) = root_attnums.as_ref() {
                let (mapped, _fwr) =
                    rewritemanip_seams::map_variable_attnos_targetentry_list::call(
                        mcx,
                        returning_list,
                        first_varno as i32,
                        attnums,
                        root_reltype,
                    )?;
                mapped
            } else {
                returning_list
            };
            // rootRelInfo->ri_returningList = returningList;
            // rootRelInfo->ri_projectReturning =
            //     ExecBuildProjectionInfo(returningList, econtext,
            //         mtstate->ps.ps_ResultTupleSlot, &mtstate->ps,
            //         RelationGetDescr(rootRelation));
            execExpr_seams::exec_build_returning_projection::call(
                mtstate,
                estate,
                root_rel_info,
                returning_list.as_slice(),
            )?;
        }
    }

    Ok(())
}

/// `ExecInitMergeTupleSlots(mtstate, resultRelInfo)` — create the
/// existing/output tuple slots a relation's MERGE actions project into.
pub fn ExecInitMergeTupleSlots<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
) -> PgResult<()> {
    let _ = mtstate;

    // EState *estate = mtstate->ps.state;  — supplied as a parameter here.
    //
    // Assert(!resultRelInfo->ri_projectNewInfoValid);
    debug_assert!(!estate.result_rel(result_rel_info).ri_projectNewInfoValid);

    // resultRelInfo->ri_oldTupleSlot =
    //     table_slot_create(resultRelInfo->ri_RelationDesc, &estate->es_tupleTable);
    // resultRelInfo->ri_newTupleSlot =
    //     table_slot_create(resultRelInfo->ri_RelationDesc, &estate->es_tupleTable);
    // resultRelInfo->ri_projectNewInfoValid = true;
    let rel = estate
        .result_rel(result_rel_info)
        .ri_RelationDesc
        .as_ref()
        .expect("MERGE target ResultRelInfo has an open relation")
        .alias();
    let old_slot = table_tableam::table_slot_create(mcx, &rel)?;
    let old_id = estate.push_slot_data(old_slot)?;
    let new_slot = table_tableam::table_slot_create(mcx, &rel)?;
    let new_id = estate.push_slot_data(new_slot)?;
    let rri = estate.result_rel_mut(result_rel_info);
    rri.ri_oldTupleSlot = Some(old_id);
    rri.ri_newTupleSlot = Some(new_id);
    rri.ri_projectNewInfoValid = true;

    Ok(())
}
