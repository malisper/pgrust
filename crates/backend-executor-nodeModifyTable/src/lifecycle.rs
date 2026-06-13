//! Node-lifecycle family of `executor/nodeModifyTable.c`: end / rescan, plus
//! RETURNING projection, stored-generated-column computation, tuple-routing
//! preparation, transition-capture setup, statement-trigger firing, the
//! plan-output sanity check, and the target-OID lookup helper. The two large
//! drivers — node init (`ExecInitModifyTable`) and the `ExecProcNode` callback
//! (`ExecModifyTable`) — live in the [`crate::init`] and [`crate::exec`]
//! sub-modules.

extern crate alloc;

use mcx::Mcx;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_DATATYPE_MISMATCH};
use types_nodes::execexpr::ExprState;
use types_nodes::nodes::CmdType;
use types_nodes::primnodes::Expr;
use types_nodes::{
    EStateData, ModifyTableState, PartitionTupleRouting, RriId, SlotId, TargetEntry,
};
use types_rel::Relation;

/// `MERGE_INSERT` (execnodes.h) — MERGE subcommand mask bit.
const MERGE_INSERT: i32 = 0x01;
/// `MERGE_UPDATE` (execnodes.h).
const MERGE_UPDATE: i32 = 0x02;
/// `MERGE_DELETE` (execnodes.h).
const MERGE_DELETE: i32 = 0x04;

/// A loud failure for an operation owned by a neighbor unit that is not yet
/// ported and whose seam/type surface is not modeled here (the trimmed
/// `ResultRelInfo`/`EPQState` drops the relevant fields, and no `-seams`
/// declaration for the callee is wired into this crate). It surfaces as an
/// `Err` so the executor's error path runs, mirroring the C `ereport(ERROR)`.
fn unported(what: &str) -> PgError {
    PgError::error(alloc::format!(
        "backend-executor-nodeModifyTable::lifecycle: unported neighbor operation: {what}"
    ))
}

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
                backend_nodes_nodeFuncs_seams::expr_type_info::call(expr)?.typid;
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
    use types_nodes::execexpr::{EEO_FLAG_HAS_NEW, EEO_FLAG_HAS_OLD, EEO_FLAG_NEW_IS_NULL, EEO_FLAG_OLD_IS_NULL};

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
        Some(backend_executor_execUtils_seams::exec_get_all_null_slot::call(
            estate,
            result_rel_info,
        )?)
    } else {
        None // No references to OLD columns.
    };

    let new_tuple = if new_slot.is_some() {
        new_slot
    } else if flags & EEO_FLAG_HAS_NEW != 0 {
        Some(backend_executor_execUtils_seams::exec_get_all_null_slot::call(
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
    backend_executor_execExpr_seams::exec_project_returning::call(estate, result_rel_info)
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
    use types_tuple::access::ATTRIBUTE_GENERATED_STORED;
    use types_tuple::heaptuple::FirstLowInvalidHeapAttributeNumber;

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
        backend_executor_execUtils_seams::exec_get_updated_cols::call(mcx, estate, result_rel_info)?
    } else {
        None
    };

    // ri_GeneratedExprs = palloc0(natts * sizeof(ExprState *));
    let mut ri_generated_exprs: mcx::PgVec<'mcx, Option<mcx::PgBox<'mcx, ExprState<'mcx>>>> =
        mcx::PgVec::new_in(mcx);
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
            let expr = backend_rewrite_rewritehandler_seams::build_column_default::call(
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
                    backend_optimizer_util_var_seams::pull_varattnos::call(mcx, &expr, 1)?;
                if !backend_nodes_core_seams::bms_overlap::call(
                    Some(updated),
                    attrs_used.as_deref(),
                ) {
                    continue; // need not update this column
                }
            }

            // No luck, so prepare the expression for execution.
            if attgenerated == ATTRIBUTE_GENERATED_STORED {
                let state =
                    backend_executor_execExpr_seams::exec_prepare_expr::call(&expr, estate)?;
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
            backend_nodes_core_seams::bms_add_member::call(mcx, cur, member)?;
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
/// state lives on fields not carried by the trimmed
/// [`types_nodes::ResultRelInfo`], and `ExecEvalExpr` over that stored state /
/// the slot-store helpers are unported owners with no seam for this form; this
/// is an unported callee.
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
    let econtext = backend_executor_execUtils_seams::get_per_tuple_expr_context::call(estate)?;

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

    // The per-attribute compute loop touches the slot's tts_values/tts_isnull
    // payload (slot_getallattrs, datumCopy, ExecClearTuple/memcpy/
    // ExecStoreVirtualTuple/ExecMaterializeSlot) and runs ExecEvalExpr over the
    // generated ExprStates in the per-tuple memory context — all slot-payload /
    // expression-interpreter work owned by execTuples/execExpr. It reads the
    // generated ExprStates off the ResultRelInfo (ri_GeneratedExprs*) selected
    // by cmdtype.
    backend_executor_execTuples_seams::exec_store_generated_columns::call(
        mcx,
        estate,
        result_rel_info,
        slot,
        econtext,
        cmdtype,
    )
}

/// `ExecSetupTransitionCaptureState(mtstate, estate)` — set up the
/// `TransitionCaptureState`(s) when the target's triggers want transition
/// tables, including the ON CONFLICT UPDATE variant.
///
/// Mirrors `ExecSetupTransitionCaptureState` (nodeModifyTable.c): it builds
/// `mt_transition_capture` (and, for INSERT ON CONFLICT UPDATE,
/// `mt_oc_transition_capture`) via `MakeTransitionCaptureState`, reading the
/// root target's `ri_TrigDesc`. `MakeTransitionCaptureState` is owned by
/// trigger.c — the trigger seam crate declares only the AfterTrigger* xact
/// entry points and the BR/AR row-trigger calls, not this constructor — so this
/// is an unported callee.
pub fn ExecSetupTransitionCaptureState<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = (mcx, mtstate, estate);
    Err(unported(
        "ExecSetupTransitionCaptureState: MakeTransitionCaptureState (trigger.c) not declared",
    ))
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
    proute: &mut PartitionTupleRouting,
    target_rel_info: RriId,
    slot: SlotId,
    part_rel_info: &mut Option<RriId>,
) -> PgResult<SlotId> {
    // Lookup the target partition's ResultRelInfo. ExecFindPartition raises an
    // error if it does not find a valid partition for the tuple, or if the
    // found partition is not a valid INSERT target.
    //   partrel = ExecFindPartition(mtstate, targetRelInfo, proute, slot, estate);
    let partrel = backend_executor_execPartition_seams::exec_find_partition::call(
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
    let map = backend_executor_execUtils_seams::exec_get_root_to_child_map::call(
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
            backend_executor_execTuples_seams::execute_attr_map_slot_explicit::call(
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

/// `EvalPlanQualEnd(epqstate)` (execMain.c) — terminate EPQ execution if it was
/// active: reset the EPQ tuple table, shut down the recheck plan and subplans,
/// close the recheck EState's result relations, and free that EState; finally
/// mark the EPQState idle (`origslot = NULL`).
///
/// The trimmed [`types_nodes::EPQState`] carries no `recheckestate` /
/// `tuple_table` / `recheckplanstate` (those land with the execMain EvalPlanQual
/// machinery), so the trimmed model is always in the "EPQ wasn't started"
/// state: the early `estate == NULL` return path applies and there is no recheck
/// EState to tear down. The faithful residue is clearing `origslot`.
fn eval_plan_qual_end(epqstate: &mut types_nodes::EPQState<'_>) {
    // C: if (epqstate->tuple_table != NIL) { ... } — not modeled (no tuple
    // table on the trimmed EPQState).
    // C: if (estate == NULL) return; — recheckestate is unmodeled, i.e. NULL,
    // so the teardown body (ExecEndNode/ExecCloseResultRelations/
    // FreeExecutorState) does not run.
    // C: epqstate->origslot = NULL; — origslot is trimmed from the canonical
    // EPQState (owned by execMain's EvalPlanQual machinery); no-op residue.
    let _ = epqstate;
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
/// on the trimmed [`types_nodes::ResultRelInfo`] (`ri_NumSlotsInitialized == 0`
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
        backend_executor_execPartition_seams::exec_cleanup_tuple_routing::call(node, &mut proute)?;

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
    eval_plan_qual_end(&mut node.mt_epqstate);

    // Shut down subplan.
    //   ExecEndNode(outerPlanState(node));
    let subplanstate = node
        .ps
        .lefttree
        .as_mut()
        .expect("outerPlanState(ModifyTable) is NULL");
    backend_executor_execProcnode_seams::exec_end_node::call(subplanstate, estate)?;

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
/// for ON CONFLICT UPDATE; the MERGE subcommand mask for CMD_MERGE). Those
/// before-statement trigger entry points are owned by trigger.c and are not
/// declared in the trigger seam crate (which carries only the AfterTrigger*
/// xact hooks and BR/AR row-trigger calls); they are unported callees. The
/// operation dispatch (the unit's own control flow) is mirrored exactly, with
/// each fire reached as an unported callee.
pub fn fireBSTriggers<'mcx>(
    node: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = estate;
    let _root_result_rel_info = node.rootResultRelInfo;

    match node.operation {
        CmdType::CMD_INSERT => {
            exec_bs_insert_triggers()?;
            if node.onConflictAction
                == types_nodes::modifytable::OnConflictAction::ONCONFLICT_UPDATE
            {
                exec_bs_update_triggers()?;
            }
            Ok(())
        }
        CmdType::CMD_UPDATE => exec_bs_update_triggers(),
        CmdType::CMD_DELETE => exec_bs_delete_triggers(),
        CmdType::CMD_MERGE => {
            if node.mt_merge_subcommands & MERGE_INSERT != 0 {
                exec_bs_insert_triggers()?;
            }
            if node.mt_merge_subcommands & MERGE_UPDATE != 0 {
                exec_bs_update_triggers()?;
            }
            if node.mt_merge_subcommands & MERGE_DELETE != 0 {
                exec_bs_delete_triggers()?;
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
/// subcommand mask for CMD_MERGE). Those after-statement trigger entry points
/// are owned by trigger.c and are not declared in the trigger seam crate; they
/// are unported callees. The operation dispatch (the unit's own control flow)
/// is mirrored exactly, with each fire reached as an unported callee.
pub fn fireASTriggers<'mcx>(
    node: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = estate;
    let _root_result_rel_info = node.rootResultRelInfo;

    match node.operation {
        CmdType::CMD_INSERT => {
            if node.onConflictAction
                == types_nodes::modifytable::OnConflictAction::ONCONFLICT_UPDATE
            {
                // ExecASUpdateTriggers(..., node->mt_oc_transition_capture)
                exec_as_update_triggers()?;
            }
            // ExecASInsertTriggers(..., node->mt_transition_capture)
            exec_as_insert_triggers()
        }
        CmdType::CMD_UPDATE => exec_as_update_triggers(),
        CmdType::CMD_DELETE => exec_as_delete_triggers(),
        CmdType::CMD_MERGE => {
            if node.mt_merge_subcommands & MERGE_DELETE != 0 {
                exec_as_delete_triggers()?;
            }
            if node.mt_merge_subcommands & MERGE_UPDATE != 0 {
                exec_as_update_triggers()?;
            }
            if node.mt_merge_subcommands & MERGE_INSERT != 0 {
                exec_as_insert_triggers()?;
            }
            Ok(())
        }
        _ => Err(PgError::error("unknown operation")),
    }
}

/// `ExecBSInsertTriggers(estate, relinfo)` (trigger.c) — fire BEFORE STATEMENT
/// INSERT triggers. Owned by trigger.c, not declared in the trigger seam crate;
/// unported callee.
fn exec_bs_insert_triggers() -> PgResult<()> {
    Err(unported("ExecBSInsertTriggers (trigger.c) not declared"))
}

/// `ExecBSUpdateTriggers(estate, relinfo)` (trigger.c) — fire BEFORE STATEMENT
/// UPDATE triggers. Owned by trigger.c, not declared in the trigger seam crate;
/// unported callee.
fn exec_bs_update_triggers() -> PgResult<()> {
    Err(unported("ExecBSUpdateTriggers (trigger.c) not declared"))
}

/// `ExecBSDeleteTriggers(estate, relinfo)` (trigger.c) — fire BEFORE STATEMENT
/// DELETE triggers. Owned by trigger.c, not declared in the trigger seam crate;
/// unported callee.
fn exec_bs_delete_triggers() -> PgResult<()> {
    Err(unported("ExecBSDeleteTriggers (trigger.c) not declared"))
}

/// `ExecASInsertTriggers(estate, relinfo, transition_capture)` (trigger.c) —
/// fire AFTER STATEMENT INSERT triggers. Owned by trigger.c, not declared in
/// the trigger seam crate; unported callee.
fn exec_as_insert_triggers() -> PgResult<()> {
    Err(unported("ExecASInsertTriggers (trigger.c) not declared"))
}

/// `ExecASUpdateTriggers(estate, relinfo, transition_capture)` (trigger.c) —
/// fire AFTER STATEMENT UPDATE triggers. Owned by trigger.c, not declared in
/// the trigger seam crate; unported callee.
fn exec_as_update_triggers() -> PgResult<()> {
    Err(unported("ExecASUpdateTriggers (trigger.c) not declared"))
}

/// `ExecASDeleteTriggers(estate, relinfo, transition_capture)` (trigger.c) —
/// fire AFTER STATEMENT DELETE triggers. Owned by trigger.c, not declared in
/// the trigger seam crate; unported callee.
fn exec_as_delete_triggers() -> PgResult<()> {
    Err(unported("ExecASDeleteTriggers (trigger.c) not declared"))
}
