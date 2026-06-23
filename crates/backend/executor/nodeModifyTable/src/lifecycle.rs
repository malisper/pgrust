//! Node-lifecycle family of `executor/nodeModifyTable.c`: end / rescan, plus
//! RETURNING projection, stored-generated-column computation, tuple-routing
//! preparation, transition-capture setup, statement-trigger firing, the
//! plan-output sanity check, and the target-OID lookup helper. The two large
//! drivers — node init (`ExecInitModifyTable`) and the `ExecProcNode` callback
//! (`ExecModifyTable`) — live in the [`crate::init`] and [`crate::exec`]
//! sub-modules.

extern crate alloc;

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_DATATYPE_MISMATCH};
use ::nodes::execexpr::ExprState;
use ::nodes::nodes::CmdType;
use ::nodes::primnodes::Expr;
use ::nodes::{
    EStateData, ModifyTableState, PartitionTupleRouting, RriId, SlotId, TargetEntry,
};
use ::rel::Relation;

/// `MERGE_INSERT` (execnodes.h) — MERGE subcommand mask bit.
const MERGE_INSERT: i32 = 0x01;
/// `MERGE_UPDATE` (execnodes.h).
const MERGE_UPDATE: i32 = 0x02;
/// `MERGE_DELETE` (execnodes.h).
const MERGE_DELETE: i32 = 0x04;

/// `ExecCheckPlanOutput(resultRel, targetList)` — verify that the ModifyTable
/// subplan's targetlist matches the result relation's tuple descriptor
/// (`ereport(ERROR)` on a mismatch — a planner/executor invariant violation).
///
/// Mirrors `ExecCheckPlanOutput` (nodeModifyTable.c). The C reads the
/// result-relation descriptor with `RelationGetDescr`, walks the tlist in
/// lock-step, and demands an exact type match in the "normal" (non-dropped,
/// non-generated) case. The dropped- and generated-column cases each insist
/// on *some* NULL `Const`, which the trimmed [`Expr`] vocabulary expresses
/// directly. The normal-case type check compares `exprType((Node *) tle->expr)`
/// against `attr->atttypid`; computing the type of an arbitrary expression tree
/// is owned by nodeFuncs (`exprType`), reached through the `expr_type_info`
/// seam. `format_type_be` (format_type.c) would prettify the type names in the
/// mismatch detail, but it is a genuinely foreign callee used only for the
/// message text, so the raw type OIDs are reported instead.
pub fn ExecCheckPlanOutput(resultRel: Relation<'_>, target_list: &[TargetEntry<'_>]) -> PgResult<()> {
    let result_desc = &*resultRel.rd_att;
    let mut attno: i32 = 0;

    for tle in target_list.iter() {
        // Assert(!tle->resjunk); -- caller removed junk items already.
        debug_assert!(!tle.resjunk);

        if attno >= result_desc.natts {
            return Err(PgError::error(
                "table row type and query-specified row type do not match",
            )
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
            .with_detail("Query has too many columns."));
        }
        let attr = result_desc.attr(attno as usize);
        attno += 1;

        // Special cases here should match planner's expand_insert_targetlist.
        if attr.attisdropped {
            // For a dropped column, we can't check atttypid; insist on *some*
            // NULL constant.
            let is_null_const = matches!(
                tle.expr.as_deref(),
                Some(Expr::Const(c)) if c.constisnull
            );
            if !is_null_const {
                return Err(PgError::error(
                    "table row type and query-specified row type do not match",
                )
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
                .with_detail(alloc::format!(
                    "Query provides a value for a dropped column at ordinal position {}.",
                    attno
                )));
            }
        } else if attr.attgenerated != 0 {
            // For a generated column, insist on *some* NULL constant, as above.
            let is_null_const = matches!(
                tle.expr.as_deref(),
                Some(Expr::Const(c)) if c.constisnull
            );
            if !is_null_const {
                return Err(PgError::error(
                    "table row type and query-specified row type do not match",
                )
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
                .with_detail(alloc::format!(
                    "Query provides a value for a generated column at ordinal position {}.",
                    attno
                )));
            }
        } else {
            // Normal case: demand type match.
            //
            // The C: `if (exprType((Node *) tle->expr) != attr->atttypid)
            // ereport(ERROR, ...)`. Computing the type of an arbitrary
            // expression tree is owned by nodeFuncs (`exprType`), reached
            // through the `expr_type_info` seam (which also bundles
            // typmod/collation). The comparison and the `ereport` itself are
            // this function's own logic. `format_type_be` (format_type.c)
            // would prettify both type names in the detail; it is a genuinely
            // foreign callee only used for the message text, so the OIDs are
            // reported raw here instead of being routed through a seam.
            let expr = tle.expr.as_deref().expect("non-junk TargetEntry has an expr");
            let expr_type =
                nodeFuncs_seams::expr_type_info::call(expr)?.typid;
            if expr_type != attr.atttypid {
                return Err(PgError::error(
                    "table row type and query-specified row type do not match",
                )
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
                .with_detail(alloc::format!(
                    "Table has type {} at ordinal position {}, but query expects {}.",
                    attr.atttypid,
                    attno,
                    expr_type
                )));
            }
        }
    }

    if attno != result_desc.natts {
        return Err(PgError::error(
            "table row type and query-specified row type do not match",
        )
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
        .with_detail("Query has too few columns."));
    }

    Ok(())
}

/// `ExecProcessReturning(context, resultRelInfo, cmdType, oldSlot, newSlot,
/// planSlot)` — evaluate the RETURNING projection for the just-modified row,
/// wiring OLD/NEW/subplan slots into the projection's econtext. Returns the
/// projected slot.
///
/// Mirrors `ExecProcessReturning` (nodeModifyTable.c): it reads
/// `resultRelInfo->ri_projectReturning` (the compiled RETURNING projection),
/// sets the projection econtext's scan/outer/old/new tuples and the
/// `EEO_FLAG_*` bits on `pi_state` — this function's own logic — then calls
/// `ExecGetAllNullSlot` (execUtils.c) for absent OLD/NEW rows and `ExecProject`
/// (execExpr.c) over `ri_projectReturning` through their owner seams (the
/// projection-step machinery and the all-NULL slot lazy-init are foreign).
pub fn ExecProcessReturning<'mcx>(
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    cmd_type: CmdType,
    old_slot: Option<SlotId>,
    new_slot: Option<SlotId>,
    plan_slot: Option<SlotId>,
) -> PgResult<SlotId> {
    use ::nodes::execexpr::{EEO_FLAG_HAS_NEW, EEO_FLAG_HAS_OLD, EEO_FLAG_NEW_IS_NULL, EEO_FLAG_OLD_IS_NULL};

    // ProjectionInfo *projectReturning = resultRelInfo->ri_projectReturning;
    // ExprContext *econtext = projectReturning->pi_exprContext;
    let econtext_id = estate
        .result_rel(result_rel_info)
        .ri_projectReturning
        .as_ref()
        .and_then(|p| p.pi_exprContext)
        .ok_or_else(|| {
            PgError::error(
                "ExecProcessReturning: ri_projectReturning has no expression context",
            )
        })?;

    // The projection's EEO_FLAG_HAS_OLD / HAS_NEW bits drive whether absent
    // OLD/NEW rows are filled with the all-NULL slot. Read them before mutably
    // borrowing the econtext.
    let flags = estate
        .result_rel(result_rel_info)
        .ri_projectReturning
        .as_ref()
        .map(|p| p.pi_state.flags)
        .unwrap_or(0);

    // For absent OLD/NEW rows referenced by the projection, materialize the
    // result relation's all-NULL slot (ExecGetAllNullSlot).
    let old_tuple = if old_slot.is_some() {
        old_slot
    } else if flags & EEO_FLAG_HAS_OLD != 0 {
        Some(execUtils_seams::exec_get_all_null_slot::call(
            estate,
            result_rel_info,
        )?)
    } else {
        None // No references to OLD columns.
    };

    let new_tuple = if new_slot.is_some() {
        new_slot
    } else if flags & EEO_FLAG_HAS_NEW != 0 {
        Some(execUtils_seams::exec_get_all_null_slot::call(
            estate,
            result_rel_info,
        )?)
    } else {
        None // No references to NEW columns.
    };

    // Make tuple and any needed join variables available to ExecProject by
    // wiring the projection econtext's slots.
    let econtext = estate.es_exprcontexts[econtext_id.0 as usize]
        .as_mut()
        .ok_or_else(|| PgError::error("ExecProcessReturning: RETURNING econtext freed"))?;

    match cmd_type {
        CmdType::CMD_INSERT | CmdType::CMD_UPDATE => {
            // Return new tuple by default.
            if new_slot.is_some() {
                econtext.ecxt_scantuple = new_slot;
            }
        }
        CmdType::CMD_DELETE => {
            // Return old tuple by default.
            if old_slot.is_some() {
                econtext.ecxt_scantuple = old_slot;
            }
        }
        _ => {
            return Err(PgError::error(alloc::format!(
                "unrecognized commandType: {}",
                cmd_type as i32
            )));
        }
    }
    econtext.ecxt_outertuple = plan_slot;
    econtext.ecxt_oldtuple = old_tuple;
    econtext.ecxt_newtuple = new_tuple;

    // Tell ExecProject whether or not the OLD/NEW rows actually exist.
    let projection = estate
        .result_rel_mut(result_rel_info)
        .ri_projectReturning
        .as_mut()
        .expect("ri_projectReturning present (checked above)");
    if old_slot.is_none() {
        projection.pi_state.flags |= EEO_FLAG_OLD_IS_NULL;
    } else {
        projection.pi_state.flags &= !EEO_FLAG_OLD_IS_NULL;
    }
    if new_slot.is_none() {
        projection.pi_state.flags |= EEO_FLAG_NEW_IS_NULL;
    } else {
        projection.pi_state.flags &= !EEO_FLAG_NEW_IS_NULL;
    }

    // Compute the RETURNING expressions: return ExecProject(projectReturning).
    execExpr_seams::exec_project_returning::call(estate, result_rel_info)
}

/// `ExecInitGenerated(resultRelInfo, estate, cmdtype)` — initialize the result
/// relation's stored-generated-column bookkeeping (`ri_GeneratedExprs*` and,
/// for UPDATE, `ri_extraUpdatedCols`). Installed into the unit's seam crate so
/// execUtils can drive it.
///
/// Mirrors `ExecInitGenerated` (nodeModifyTable.c). The early-return guard
/// (no generated columns), the per-attribute loop, and the field bookkeeping
/// (`ri_GeneratedExprsI`/`ri_GeneratedExprsU` / `ri_NumGeneratedNeeded*` /
/// `ri_extraUpdatedCols`) are this function's own logic. The expression
/// builders are genuinely foreign and routed through their owner seams:
/// `build_column_default` (rewriteHandler.c), `ExecPrepareExpr` (execExpr.c),
/// `pull_varattnos` (var.c), `ExecGetUpdatedCols` (execUtils.c); `bms_overlap`
/// / `bms_add_member` go through `backend-nodes-core-seams`.
pub fn ExecInitGenerated<'mcx>(
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    cmdtype: CmdType,
) -> PgResult<()> {
    use ::types_tuple::access::ATTRIBUTE_GENERATED_STORED;
    use ::types_tuple::heaptuple::FirstLowInvalidHeapAttributeNumber;

    let mcx = estate.es_query_cxt;
    let rel = estate
        .result_rel(result_rel_info)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecInitGenerated: result rel has an open relation")
        .alias();
    let natts = rel.rd_att.natts;

    // Nothing to do if no generated columns.
    let has_generated = rel
        .rd_att
        .constr
        .as_ref()
        .map(|c| c.has_generated_stored || c.has_generated_virtual)
        .unwrap_or(false);
    if !has_generated {
        return Ok(());
    }

    // In an UPDATE, we can skip computing any generated columns that do not
    // depend on any UPDATE target column.  But if there is a BEFORE ROW UPDATE
    // trigger, we cannot skip because the trigger might change more columns.
    let trig_update_before_row = estate
        .result_rel(result_rel_info)
        .ri_TrigDesc
        .as_ref()
        .map(|t| t.trig_update_before_row)
        .unwrap_or(false);
    let updated_cols = if cmdtype == CmdType::CMD_UPDATE && !trig_update_before_row {
        execUtils_seams::exec_get_updated_cols::call(mcx, estate, result_rel_info)?
    } else {
        None
    };

    // ri_GeneratedExprs = palloc0(natts * sizeof(ExprState *));
    let mut ri_generated_exprs: ::mcx::PgVec<'mcx, Option<::mcx::PgBox<'mcx, ExprState<'mcx>>>> =
        ::mcx::PgVec::new_in(mcx);
    for _ in 0..natts {
        ri_generated_exprs.push(None);
    }
    let mut ri_num_generated_needed: i32 = 0;
    // Columns to add to ri_extraUpdatedCols, gathered as we go (the relation /
    // result-rel borrows preclude mutating it inside the loop).
    let mut extra_updated_members: alloc::vec::Vec<i32> = alloc::vec::Vec::new();

    for i in 0..natts as usize {
        let attgenerated = rel.rd_att.attr(i).attgenerated;
        if attgenerated != 0 {
            // Fetch the GENERATED AS expression tree.
            let expr = rewritehandler_seams::build_column_default::call(
                mcx,
                rel.alias(),
                i as i32 + 1,
            )?;
            let expr = match expr {
                Some(e) => e,
                None => {
                    return Err(PgError::error(alloc::format!(
                        "no generation expression found for column number {} of table \"{}\"",
                        i + 1,
                        rel.name()
                    )));
                }
            };

            // If it's an update with a known set of update target columns, see
            // if we can skip the computation.
            if let Some(updated) = updated_cols.as_deref() {
                let attrs_used =
                    var_seams::pull_varattnos::call(mcx, &expr, 1)?;
                if !nodes_core_seams::bms_overlap::call(
                    Some(updated),
                    attrs_used.as_deref(),
                ) {
                    continue; // need not update this column
                }
            }

            // No luck, so prepare the expression for execution.
            if attgenerated == ATTRIBUTE_GENERATED_STORED {
                let state =
                    execExpr_seams::exec_prepare_expr::call(&expr, estate)?;
                ri_generated_exprs[i] = Some(state);
                ri_num_generated_needed += 1;
            }

            // If UPDATE, mark column in resultRelInfo->ri_extraUpdatedCols.
            if cmdtype == CmdType::CMD_UPDATE {
                extra_updated_members.push(i as i32 + 1 - FirstLowInvalidHeapAttributeNumber as i32);
            }
        }
    }

    // ri_NumGeneratedNeeded == 0: didn't need it after all (ri_GeneratedExprs
    // becomes NULL).
    let ri_generated_exprs = if ri_num_generated_needed == 0 {
        None
    } else {
        Some(ri_generated_exprs)
    };

    // Apply the gathered ri_extraUpdatedCols members (bms_add_member).
    for member in extra_updated_members {
        let cur = estate
            .result_rel_mut(result_rel_info)
            .ri_extraUpdatedCols
            .take();
        let updated =
            nodes_core_seams::bms_add_member::call(mcx, cur, member)?;
        estate.result_rel_mut(result_rel_info).ri_extraUpdatedCols = Some(updated);
    }

    // Save in appropriate set of fields.
    let rri = estate.result_rel_mut(result_rel_info);
    if cmdtype == CmdType::CMD_UPDATE {
        debug_assert!(rri.ri_GeneratedExprsU.is_none()); // Don't call twice.
        rri.ri_GeneratedExprsU = ri_generated_exprs;
        rri.ri_NumGeneratedNeededU = ri_num_generated_needed;
        rri.ri_extraUpdatedCols_valid = true;
    } else {
        debug_assert!(rri.ri_GeneratedExprsI.is_none()); // Don't call twice.
        rri.ri_GeneratedExprsI = ri_generated_exprs;
        rri.ri_NumGeneratedNeededI = ri_num_generated_needed;
    }

    Ok(())
}

/// `ExecComputeStoredGenerated(resultRelInfo, estate, slot, cmdtype)` — compute
/// the values of stored generated columns and store them into `slot`.
///
/// Mirrors `ExecComputeStoredGenerated` (nodeModifyTable.c): it reads the
/// per-column `ri_GeneratedExprs*` (lazily initializing via
/// [`ExecInitGenerated`]), evaluates each with `ExecEvalExpr` in the per-tuple
/// context, `datumCopy`s pass-by-reference results, and rebuilds the slot via
/// `ExecStoreVirtualTuple` / `ExecMaterializeSlot`. The generated-expression
/// state lives on the `ResultRelInfo`'s `ri_GeneratedExprs*` fields;
/// `ExecEvalExpr` over that stored state and the slot-store helpers are reached
/// through the execExpr / execTuples / datum seams.
pub fn ExecComputeStoredGenerated<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    slot: SlotId,
    cmdtype: CmdType,
) -> PgResult<()> {
    // Relation rel = resultRelInfo->ri_RelationDesc;
    // TupleDesc tupdesc = RelationGetDescr(rel); int natts = tupdesc->natts;
    //
    // We should not be called unless this is true:
    //   Assert(tupdesc->constr && tupdesc->constr->has_generated_stored);
    debug_assert!(estate
        .result_rel(result_rel_info)
        .ri_RelationDesc
        .as_ref()
        .and_then(|r| r.rd_att.constr.as_ref())
        .map(|c| c.has_generated_stored)
        .unwrap_or(false));

    // ExprContext *econtext = GetPerTupleExprContext(estate);
    let econtext = execUtils_seams::get_per_tuple_expr_context::call(estate)?;

    // Initialize the expressions if we didn't already, and check whether we can
    // exit early because nothing needs to be computed.
    if cmdtype == CmdType::CMD_UPDATE {
        // if (ri_GeneratedExprsU == NULL) ExecInitGenerated(...);
        if estate.result_rel(result_rel_info).ri_GeneratedExprsU.is_none() {
            ExecInitGenerated(estate, result_rel_info, cmdtype)?;
        }
        // if (ri_NumGeneratedNeededU == 0) return;
        if estate.result_rel(result_rel_info).ri_NumGeneratedNeededU == 0 {
            return Ok(());
        }
    } else {
        // if (ri_GeneratedExprsI == NULL) ExecInitGenerated(...);
        if estate.result_rel(result_rel_info).ri_GeneratedExprsI.is_none() {
            ExecInitGenerated(estate, result_rel_info, cmdtype)?;
        }
        // Early exit is impossible given the prior Assert.
        //   Assert(ri_NumGeneratedNeededI > 0);
        debug_assert!(estate.result_rel(result_rel_info).ri_NumGeneratedNeededI > 0);
    }

    // The per-attribute compute loop (C: the tail of ExecComputeStoredGenerated,
    // nodeModifyTable.c). It runs in the per-tuple memory context: deform the
    // slot, then for every column with a non-NULL generated ExprState evaluate it
    // (datumCopy a non-null pass-by-reference result), datumCopy the remaining
    // existing slot values, and rebuild the slot via store-virtual + materialize.
    //
    //   oldContext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
    // The per-tuple context is the allocation domain the seam shims allocate in
    // (slot_getallattrs_by_id / datum_copy_v / store_virtual_values allocate in
    // the EState's per-tuple/per-query context); we drive that explicitly rather
    // than via a current-context switch.

    // Snapshot the per-attribute (attbyval, attlen) the datumCopy calls need, plus
    // natts, off the result relation's descriptor (TupleDescCompactAttr in C).
    let (natts, attmeta): (usize, ::mcx::PgVec<'mcx, (bool, i16)>) = {
        let rel = estate
            .result_rel(result_rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("result relation must be open");
        let tupdesc = &rel.rd_att;
        let n = tupdesc.natts as usize;
        let mut meta = ::mcx::PgVec::new_in(mcx);
        for i in 0..n {
            let att = tupdesc.attr(i);
            meta.push((att.attbyval, att.attlen));
        }
        (n, meta)
    };

    // values = palloc(...); nulls = palloc(...);
    // slot_getallattrs(slot); memcpy(nulls, slot->tts_isnull, ...);
    let deformed = execTuples_seams::slot_getallattrs_by_id::call(estate, slot)?;
    debug_assert_eq!(deformed.len(), natts);
    let mut values: ::mcx::PgVec<'mcx, ::types_tuple::Datum<'mcx>> = ::mcx::PgVec::new_in(mcx);
    let mut nulls: ::mcx::PgVec<'mcx, bool> = ::mcx::PgVec::new_in(mcx);
    for (v, n) in deformed.iter() {
        values.push(v.clone_in(mcx)?);
        nulls.push(*n);
    }

    for i in 0..natts {
        let (attbyval, attlen) = attmeta[i];

        // if (ri_GeneratedExprs[i]) { ... } — select the array by cmdtype and take
        // out element i (an owned ExprState), leaving the slot for write-back.
        let gen_state: Option<::mcx::PgBox<'mcx, ExprState<'mcx>>> = {
            let rri = estate.result_rel_mut(result_rel_info);
            let arr = if cmdtype == CmdType::CMD_UPDATE {
                rri.ri_GeneratedExprsU.as_mut()
            } else {
                rri.ri_GeneratedExprsI.as_mut()
            }
            .expect("ri_GeneratedExprs* initialized above");
            arr[i].take()
        };

        if let Some(mut state) = gen_state {
            // econtext->ecxt_scantuple = slot;
            estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);
            // val = ExecEvalExpr(ri_GeneratedExprs[i], econtext, &isnull);
            let (mut val, isnull) =
                execExpr_seams::exec_eval_expr_switch_context::call(
                    &mut state, econtext, estate,
                )?;
            // We must make a copy of val as we have no guarantees about where
            // memory for a pass-by-reference Datum is located.
            //   if (!isnull) val = datumCopy(val, attr->attbyval, attr->attlen);
            if !isnull {
                val = datum_seams::datum_copy_v::call(
                    mcx,
                    &val,
                    attbyval,
                    attlen as i32,
                )?;
            }
            values[i] = val;
            nulls[i] = isnull;

            // Write the (untouched) ExprState back into its pool slot.
            let rri = estate.result_rel_mut(result_rel_info);
            let arr = if cmdtype == CmdType::CMD_UPDATE {
                rri.ri_GeneratedExprsU.as_mut()
            } else {
                rri.ri_GeneratedExprsI.as_mut()
            }
            .expect("ri_GeneratedExprs* initialized above");
            arr[i] = Some(state);
        } else {
            // else { if (!nulls[i]) values[i] = datumCopy(slot->tts_values[i], ...); }
            if !nulls[i] {
                values[i] = datum_seams::datum_copy_v::call(
                    mcx,
                    &values[i],
                    attbyval,
                    attlen as i32,
                )?;
            }
        }
    }

    // ExecClearTuple(slot); memcpy values/nulls into the slot;
    // ExecStoreVirtualTuple(slot);  (store_virtual_values does all three)
    execTuples_seams::store_virtual_values::call(estate, slot, &values, &nulls)?;
    // ExecMaterializeSlot(slot);
    execTuples_seams::exec_materialize_slot::call(estate, slot)?;

    Ok(())
}

/// `ExecSetupTransitionCaptureState(mtstate, estate)` — set up the
/// `TransitionCaptureState`(s) when the target's triggers want transition
/// tables, including the ON CONFLICT UPDATE variant.
///
/// Mirrors `ExecSetupTransitionCaptureState` (nodeModifyTable.c): it builds
/// `mt_transition_capture` (and, for INSERT ON CONFLICT UPDATE,
/// `mt_oc_transition_capture`) via `MakeTransitionCaptureState`, reading the
/// root target's `ri_TrigDesc`. `MakeTransitionCaptureState` is owned by
/// trigger.c (genuinely unported) and is reached through the trigger seam
/// crate; the assignments into `mt_transition_capture` /
/// `mt_oc_transition_capture` and the CMD_INSERT+ONCONFLICT_UPDATE decision are
/// this unit's own control flow.
pub fn ExecSetupTransitionCaptureState<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ResultRelInfo *targetRelInfo = mtstate->rootResultRelInfo;
    let target_rel_info = mtstate
        .rootResultRelInfo
        .expect("ExecSetupTransitionCaptureState: rootResultRelInfo is NULL");

    // Check for transition tables on the directly targeted relation.
    //   mtstate->mt_transition_capture =
    //       MakeTransitionCaptureState(targetRelInfo->ri_TrigDesc,
    //                                  RelationGetRelid(targetRelInfo->ri_RelationDesc),
    //                                  mtstate->operation);
    mtstate.mt_transition_capture = trigger_seams::make_transition_capture_state::call(
        mcx,
        estate,
        target_rel_info,
        mtstate.operation,
    )?;

    // if (plan->operation == CMD_INSERT && plan->onConflictAction == ONCONFLICT_UPDATE)
    //     mtstate->mt_oc_transition_capture =
    //         MakeTransitionCaptureState(targetRelInfo->ri_TrigDesc,
    //                                    RelationGetRelid(targetRelInfo->ri_RelationDesc),
    //                                    CMD_UPDATE);
    if mtstate.operation == CmdType::CMD_INSERT
        && mtstate.onConflictAction
            == ::nodes::modifytable::OnConflictAction::ONCONFLICT_UPDATE
    {
        mtstate.mt_oc_transition_capture =
            trigger_seams::make_transition_capture_state::call(
                mcx,
                estate,
                target_rel_info,
                CmdType::CMD_UPDATE,
            )?;
    }

    Ok(())
}

/// `ExecPrepareTupleRouting(mtstate, estate, proute, targetRelInfo, slot,
/// partRelInfo)` — route `slot` to its leaf partition, converting the tuple to
/// the partition rowtype and reporting the partition's `ResultRelInfo` id
/// through `partRelInfo`. Returns the (possibly converted) slot to insert.
///
/// Mirrors `ExecPrepareTupleRouting` (nodeModifyTable.c): it finds the leaf
/// partition via `ExecFindPartition`, then (when transition capture is active)
/// remembers the unconverted insert tuple unless the partition has a
/// before-insert-row trigger, and converts the slot through the partition's
/// root→child map. `ExecFindPartition`, the transition-capture
/// before-insert-row check (`ri_TrigDesc->trig_insert_before_row`), and writing
/// `*partRelInfo` are all available here. The root→child conversion calls
/// `ExecGetRootToChildMap` (execPartition.c — no seam declared here) into
/// `ri_PartitionTupleSlot` (not on the trimmed `ResultRelInfo`); that
/// conversion is an unported callee, surfaced only when reached.
pub fn ExecPrepareTupleRouting<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    proute: &mut PartitionTupleRouting<'mcx>,
    target_rel_info: RriId,
    slot: SlotId,
    part_rel_info: &mut Option<RriId>,
) -> PgResult<SlotId> {
    // Lookup the target partition's ResultRelInfo. ExecFindPartition raises an
    // error if it does not find a valid partition for the tuple, or if the
    // found partition is not a valid INSERT target.
    //   partrel = ExecFindPartition(mtstate, targetRelInfo, proute, slot, estate);
    let partrel = execPartition_seams::exec_find_partition::call(
        mcx,
        mtstate,
        target_rel_info,
        proute,
        slot,
        estate,
    )?;

    // If we're capturing transition tuples, we might need to convert from the
    // partition rowtype to root partitioned table's rowtype.  But if there are
    // no BEFORE triggers on the partition that could change the tuple, we can
    // just remember the original unconverted tuple to avoid a needless round
    // trip conversion.
    if mtstate.mt_transition_capture.is_some() {
        let has_before_insert_row_trig = estate
            .result_rel(partrel)
            .ri_TrigDesc
            .as_ref()
            .map(|td| td.trig_insert_before_row)
            .unwrap_or(false);

        let tcs_original = if !has_before_insert_row_trig {
            Some(slot)
        } else {
            None
        };
        if let Some(tc) = mtstate.mt_transition_capture.as_mut() {
            tc.tcs_original_insert_tuple = tcs_original;
        }
    }

    // Convert the tuple, if necessary.
    //   map = ExecGetRootToChildMap(partrel, estate);
    //   if (map != NULL) { new_slot = partrel->ri_PartitionTupleSlot;
    //       slot = execute_attr_map_slot(map->attrMap, slot, new_slot); }
    //
    // ExecGetRootToChildMap lazily builds (and caches) the root→child conversion
    // map; when the rowtypes already match it returns the C `NULL` map and no
    // conversion is needed (the fast path — keep `slot` unchanged). That NULL/
    // non-NULL decision is this function's own control flow. The map build is
    // owned by execUtils/execPartition and the attribute-remapping store is
    // owned by tupconvert (execute_attr_map_slot), reached through their seams.
    let map = execUtils_seams::exec_get_root_to_child_map::call(
        mcx, estate, partrel,
    )?;
    let slot = match map {
        // map == NULL: rowtypes match, no conversion needed.
        None => slot,
        // map != NULL: new_slot = partrel->ri_PartitionTupleSlot;
        //              slot = execute_attr_map_slot(map->attrMap, slot, new_slot);
        Some(attr_map) => {
            let new_slot = estate
                .result_rel(partrel)
                .ri_PartitionTupleSlot
                .expect("ExecPrepareTupleRouting: partition with a conversion map has a tuple slot");
            execTuples_seams::execute_attr_map_slot_explicit::call(
                estate, &attr_map, slot, new_slot,
            )?
        }
    };

    // *partRelInfo = partrel;
    *part_rel_info = Some(partrel);
    // return slot;
    Ok(slot)
}

/// `ExecLookupResultRelByOid(node, resultoid, missing_ok, update_cache)` — map
/// a target relation OID to its `resultRelInfo[]` id (via the linear scan or
/// the `mt_resultOidHash`), optionally caching the last-seen mapping.
///
/// Mirrors `ExecLookupResultRelByOid` (nodeModifyTable.c) exactly. The hash
/// branch uses the pre-built `mt_resultOidHash` (`HASH_FIND`); the array branch
/// linearly scans `node->resultRelInfo[0..mt_nrels]`, comparing each rel's
/// `RelationGetRelid` against `resultoid`. On a hit it optionally caches
/// `mt_lastResultOid` / `mt_lastResultIndex` and returns the rel id; on a miss
/// it `elog(ERROR)`s unless `missing_ok`, else returns the C `NULL`
/// ([`None`]).
pub fn ExecLookupResultRelByOid<'mcx>(
    node: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    resultoid: Oid,
    missing_ok: bool,
    update_cache: bool,
) -> PgResult<Option<RriId>> {
    if let Some(hash) = node.mt_resultOidHash.as_ref() {
        // Use the pre-built hash table to locate the rel.
        if let Some(&relation_index) = hash.entries.get(&resultoid) {
            if update_cache {
                node.mt_lastResultOid = resultoid;
                node.mt_lastResultIndex = relation_index;
            }
            return Ok(Some(node.resultRelInfo[relation_index as usize]));
        }
    } else {
        // With few target rels, just search the ResultRelInfo array.
        for ndx in 0..node.resultRelInfo.len() {
            let r_info = node.resultRelInfo[ndx];
            let relid = estate
                .result_rel(r_info)
                .ri_RelationDesc
                .as_ref()
                .expect("ri_RelationDesc")
                .rd_id;
            if relid == resultoid {
                if update_cache {
                    node.mt_lastResultOid = resultoid;
                    node.mt_lastResultIndex = ndx as i32;
                }
                return Ok(Some(r_info));
            }
        }
    }

    if !missing_ok {
        return Err(PgError::error(alloc::format!(
            "incorrect result relation OID {}",
            resultoid
        )));
    }
    Ok(None)
}

/// `ExecEndModifyTable(node)` — shut the node down: clean up tuple routing,
/// close result relations and indexes, end the subplan, and free per-node
/// resources.
///
/// Mirrors `ExecEndModifyTable` (nodeModifyTable.c). The C first lets each FDW
/// shut down (`ri_FdwRoutine->EndForeignModify`) and drops the FDW-batching
/// slots (`ri_Slots` / `ri_PlanSlots`, only non-empty for batching FDWs), then
/// cleans up tuple routing (`ExecCleanupTupleRouting` + drop of
/// `mt_root_tuple_slot`), terminates EPQ (`EvalPlanQualEnd`), and shuts down the
/// subplan (`ExecEndNode`). The FDW routine / batch-slot fields are not carried
/// on the trimmed [`::nodes::ResultRelInfo`] (`ri_NumSlotsInitialized == 0`
/// and `ri_FdwRoutine == NULL` for every modeled rel, so that loop body is a
/// no-op); the tuple-routing cleanup, slot drops, and subplan shutdown go
/// through their owners' seam crates.
pub fn ExecEndModifyTable<'mcx>(
    node: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Allow any FDWs to shut down, and clean up the FDW batch slots.
    //
    //   for (i = 0; i < node->mt_nrels; i++) { ... ri_FdwRoutine ... ri_Slots ... }
    //
    // The trimmed ResultRelInfo carries neither an FDW routine vtable
    // (ri_FdwRoutine == NULL for every modeled rel) nor initialized batch slots
    // (ri_NumSlotsInitialized == 0), so this loop body executes for no rel. The
    // FDW-batching teardown lands with the fdwapi type; until then there is
    // nothing to shut down here.

    // Close all the partitioned tables, leaf partitions, and their indices and
    // release the slot used for tuple routing, if set.
    if let Some(mut proute) = node.mt_partition_tuple_routing.take() {
        execPartition_seams::exec_cleanup_tuple_routing::call(
            node, estate, &mut proute,
        )?;

        // if (node->mt_root_tuple_slot) ExecDropSingleTupleTableSlot(...);
        //
        // In C mt_root_tuple_slot is a standalone slot (MakeSingleTupleTableSlot)
        // freed here individually. In the owned model it is a `SlotId` into the
        // EState slot pool (`es_tupleTable`), which has no individual-remove API
        // — the whole pool is reclaimed at executor teardown
        // (ExecResetTupleTable). So the slot's storage is released with the
        // pool, not here; there is nothing to drop individually.
        let _ = node.mt_root_tuple_slot;

        // C keeps the PartitionTupleRouting pointer in place (it is freed with
        // the executor context); restore the handle we borrowed for cleanup.
        node.mt_partition_tuple_routing = Some(proute);
    }

    // Terminate EPQ execution if active.
    //   EvalPlanQualEnd(&node->mt_epqstate);
    execMain_seams::eval_plan_qual_end::call(estate, &mut node.mt_epqstate)?;

    // Shut down subplan.
    //   ExecEndNode(outerPlanState(node));
    let subplanstate = node
        .ps
        .lefttree
        .as_mut()
        .expect("outerPlanState(ModifyTable) is NULL");
    execProcnode_seams::exec_end_node::call(subplanstate, estate)?;

    Ok(())
}

/// `ExecReScanModifyTable(node)` — rescan the ModifyTable node.
///
/// Mirrors `ExecReScanModifyTable` (nodeModifyTable.c) for PostgreSQL 18.3:
/// rescan of a ModifyTable node is not supported, so the function
/// `elog(ERROR, "ExecReScanModifyTable is not implemented")`.
pub fn ExecReScanModifyTable<'mcx>(node: &mut ModifyTableState<'mcx>) -> PgResult<()> {
    let _ = node;
    Err(PgError::error("ExecReScanModifyTable is not implemented"))
}

/// `fireBSTriggers(node)` — fire the BEFORE STATEMENT triggers for the
/// operation (INSERT/UPDATE/DELETE, and the ON CONFLICT UPDATE variant).
///
/// Mirrors `fireBSTriggers` (nodeModifyTable.c): a switch on `node->operation`
/// firing `ExecBSInsertTriggers` / `ExecBSUpdateTriggers` /
/// `ExecBSDeleteTriggers` on `node->rootResultRelInfo` (and both INSERT+UPDATE
/// for ON CONFLICT UPDATE; the MERGE subcommand mask for CMD_MERGE). The
/// before-statement trigger entry points are owned by trigger.c (genuinely
/// unported); each fire goes through the trigger seam crate. The operation
/// dispatch (the unit's own control flow) is mirrored exactly.
pub fn fireBSTriggers<'mcx>(
    node: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ResultRelInfo *resultRelInfo = node->rootResultRelInfo;
    let result_rel_info = node
        .rootResultRelInfo
        .expect("fireBSTriggers: rootResultRelInfo is NULL");

    match node.operation {
        CmdType::CMD_INSERT => {
            trigger_seams::exec_bs_insert_triggers::call(estate, result_rel_info)?;
            if node.onConflictAction
                == ::nodes::modifytable::OnConflictAction::ONCONFLICT_UPDATE
            {
                trigger_seams::exec_bs_update_triggers::call(
                    estate,
                    result_rel_info,
                )?;
            }
            Ok(())
        }
        CmdType::CMD_UPDATE => {
            trigger_seams::exec_bs_update_triggers::call(estate, result_rel_info)
        }
        CmdType::CMD_DELETE => {
            trigger_seams::exec_bs_delete_triggers::call(estate, result_rel_info)
        }
        CmdType::CMD_MERGE => {
            if node.mt_merge_subcommands & MERGE_INSERT != 0 {
                trigger_seams::exec_bs_insert_triggers::call(
                    estate,
                    result_rel_info,
                )?;
            }
            if node.mt_merge_subcommands & MERGE_UPDATE != 0 {
                trigger_seams::exec_bs_update_triggers::call(
                    estate,
                    result_rel_info,
                )?;
            }
            if node.mt_merge_subcommands & MERGE_DELETE != 0 {
                trigger_seams::exec_bs_delete_triggers::call(
                    estate,
                    result_rel_info,
                )?;
            }
            Ok(())
        }
        _ => Err(PgError::error("unknown operation")),
    }
}

/// `fireASTriggers(node)` — fire the AFTER STATEMENT triggers for the
/// operation (INSERT/UPDATE/DELETE, and the ON CONFLICT UPDATE variant).
///
/// Mirrors `fireASTriggers` (nodeModifyTable.c): a switch on `node->operation`
/// firing `ExecASInsertTriggers` / `ExecASUpdateTriggers` /
/// `ExecASDeleteTriggers` on `node->rootResultRelInfo`, threading
/// `mt_transition_capture` / `mt_oc_transition_capture` (and the MERGE
/// subcommand mask for CMD_MERGE). The after-statement trigger entry points are
/// owned by trigger.c (genuinely unported); each fire goes through the trigger
/// seam crate. The operation dispatch (the unit's own control flow) is mirrored
/// exactly.
pub fn fireASTriggers<'mcx>(
    node: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ResultRelInfo *resultRelInfo = node->rootResultRelInfo;
    let result_rel_info = node
        .rootResultRelInfo
        .expect("fireASTriggers: rootResultRelInfo is NULL");

    match node.operation {
        CmdType::CMD_INSERT => {
            if node.onConflictAction
                == ::nodes::modifytable::OnConflictAction::ONCONFLICT_UPDATE
            {
                // ExecASUpdateTriggers(..., node->mt_oc_transition_capture)
                let tc = node.mt_oc_transition_capture.as_deref_mut();
                trigger_seams::exec_as_update_triggers::call(
                    estate,
                    result_rel_info,
                    tc,
                )?;
            }
            // ExecASInsertTriggers(..., node->mt_transition_capture)
            let tc = node.mt_transition_capture.as_deref_mut();
            trigger_seams::exec_as_insert_triggers::call(
                estate,
                result_rel_info,
                tc,
            )
        }
        CmdType::CMD_UPDATE => {
            // ExecASUpdateTriggers(..., node->mt_transition_capture)
            let tc = node.mt_transition_capture.as_deref_mut();
            trigger_seams::exec_as_update_triggers::call(
                estate,
                result_rel_info,
                tc,
            )
        }
        CmdType::CMD_DELETE => {
            // ExecASDeleteTriggers(..., node->mt_transition_capture)
            let tc = node.mt_transition_capture.as_deref_mut();
            trigger_seams::exec_as_delete_triggers::call(
                estate,
                result_rel_info,
                tc,
            )
        }
        CmdType::CMD_MERGE => {
            if node.mt_merge_subcommands & MERGE_DELETE != 0 {
                let tc = node.mt_transition_capture.as_deref_mut();
                trigger_seams::exec_as_delete_triggers::call(
                    estate,
                    result_rel_info,
                    tc,
                )?;
            }
            if node.mt_merge_subcommands & MERGE_UPDATE != 0 {
                let tc = node.mt_transition_capture.as_deref_mut();
                trigger_seams::exec_as_update_triggers::call(
                    estate,
                    result_rel_info,
                    tc,
                )?;
            }
            if node.mt_merge_subcommands & MERGE_INSERT != 0 {
                let tc = node.mt_transition_capture.as_deref_mut();
                trigger_seams::exec_as_insert_triggers::call(
                    estate,
                    result_rel_info,
                    tc,
                )?;
            }
            Ok(())
        }
        _ => Err(PgError::error("unknown operation")),
    }
}

// The BEFORE/AFTER STATEMENT trigger entry points (ExecBSInsertTriggers,
// ExecASInsertTriggers, …) are owned by trigger.c (genuinely unported) and are
// reached through `trigger_seams` directly from `fireBSTriggers`
// / `fireASTriggers` above — no in-crate stand-ins.
