//! Miscellaneous opcode evaluators (`execExprInterp.c`): SubPlan invocation,
//! GROUPING() and MERGE support functions.

use types_datum::Datum;
// The canonical unified value type (Datum-unification keystone) — what the
// keystone-owned `ResultCell.value` carries. The opcode results here are scalar
// words, so they cross into its by-value arm.
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;
use types_error::{PgError, PgResult};
use types_nodes::execexpr::{ExprEvalStepData, ExprState, ResultCell};
use types_nodes::execnodes::EcxtId;
use types_nodes::nodes::CmdType;
use types_nodes::EStateData;

use backend_executor_nodeSubplan_seams as nodesubplan;
use backend_nodes_core_seams as bms;
use backend_utils_adt_varlena_seams as varlena;
use backend_utils_misc_stack_depth_seams as stack_depth;

/// `ExecEvalSubPlan(ExprState *state, ExprEvalStep *op, ExprContext *econtext)`
/// — evaluate a SubPlan/AlternativeSubPlan (delegates to nodeSubplan's
/// `ExecSubPlan` through that owner's seam).
pub fn ExecEvalSubPlan<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // C:
    //   SubPlanState *sstate = op->d.subplan.sstate;
    //   /* could potentially be nested, so make sure there's enough stack */
    //   check_stack_depth();
    //   *op->resvalue = ExecSubPlan(sstate, econtext, op->resnull);
    //
    // `op->d.subplan.sstate` is the nodeSubplan-built `SubPlanState` the
    // compiler attached to the EEOP_SUBPLAN step; ExecSubPlan needs it `&mut`,
    // so it is moved out of the step for the call and replaced afterward (no
    // observable change to the step — same handle returned).
    let resvalue_id = {
        let steps = state.steps.as_ref().expect("ExecEvalSubPlan: steps not ready");
        steps[op].resvalue
    };

    // could potentially be nested, so make sure there's enough stack
    stack_depth::check_stack_depth::call()?;

    let mut sstate = {
        let steps = state.steps.as_mut().expect("ExecEvalSubPlan: steps not ready");
        match &mut steps[op].d {
            ExprEvalStepData::SubPlan { sstate } => sstate
                .take()
                .expect("ExecEvalSubPlan: EEOP_SUBPLAN step has no SubPlanState"),
            other => panic!("ExecEvalSubPlan: EEOP_SUBPLAN step has wrong payload: {other:?}"),
        }
    };

    // *op->resvalue = ExecSubPlan(sstate, econtext, op->resnull);
    let (value, isnull) = nodesubplan::exec_sub_plan::call(&mut sstate, econtext, estate)?;

    // restore the SubPlanState handle onto the step.
    {
        let steps = state.steps.as_mut().expect("ExecEvalSubPlan: steps not ready");
        if let ExprEvalStepData::SubPlan { sstate: slot } = &mut steps[op].d {
            *slot = Some(sstate);
        }
    }

    state
        .result_cells
        .set(resvalue_id, ResultCell { value: DatumV::ByVal(value), isnull });
    Ok(())
}

/// `ExecEvalGroupingFunc(ExprState *state, ExprEvalStep *op)` — evaluate a
/// GROUPING() expression against the current grouping set.
///
/// Computes a bitmask with a bit for each (unevaluated) argument expression
/// (rightmost arg is least significant bit). A bit is set if the corresponding
/// expression is NOT part of the set of grouping expressions in the current
/// grouping set.
pub fn ExecEvalGroupingFunc<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // C:
    //   AggState *aggstate = castNode(AggState, state->parent);
    //   int result = 0;
    //   Bitmapset *grouped_cols = aggstate->grouped_cols;
    //   foreach(lc, op->d.grouping_func.clauses) { ... }
    //   *op->resvalue = Int32GetDatum(result);
    //   *op->resnull = false;
    let resvalue_id = {
        let steps = state.steps.as_ref().expect("ExecEvalGroupingFunc: steps not ready");
        steps[op].resvalue
    };

    // AggState *aggstate = castNode(AggState, state->parent);
    //   Bitmapset *grouped_cols = aggstate->grouped_cols;
    // The parent AggState is owned by nodeAgg, which has not yet threaded its
    // `T_AggState` into the `PlanStateNode` enum; `as_agg_state()` yields the
    // cast (currently `None`). The `grouped_cols` it reads is the single
    // current-grouping-set Bitmapset (AggState->grouped_cols, not the rollup
    // **array**).
    let parent = state
        .parent
        .as_deref()
        .expect("ExecEvalGroupingFunc: EEOP_GROUPING_FUNC step has no parent PlanState");
    let aggstate = parent.as_agg_state().expect(
        "ExecEvalGroupingFunc: castNode(AggState, state->parent) — the parent AggState is owned \
         by nodeAgg, which has not yet threaded its T_AggState into PlanStateNode",
    );
    let grouped_cols = aggstate.grouped_cols.as_deref();

    // foreach(lc, op->d.grouping_func.clauses)
    // {
    //     int attnum = lfirst_int(lc);
    //     result <<= 1;
    //     if (!bms_is_member(attnum, grouped_cols))
    //         result |= 1;
    // }
    let mut result: i32 = 0;
    {
        let steps = state.steps.as_ref().expect("ExecEvalGroupingFunc: steps not ready");
        let clauses = match &steps[op].d {
            ExprEvalStepData::GroupingFunc { clauses } => clauses
                .as_ref()
                .expect("ExecEvalGroupingFunc: EEOP_GROUPING_FUNC step has no clauses list"),
            other => panic!(
                "ExecEvalGroupingFunc: EEOP_GROUPING_FUNC step has wrong payload: {other:?}"
            ),
        };
        for &attnum in clauses.iter() {
            result <<= 1;
            if !bms::bms_is_member::call(attnum, grouped_cols) {
                result |= 1;
            }
        }
    }

    // *op->resvalue = Int32GetDatum(result);
    // *op->resnull = false;
    state.result_cells.set(
        resvalue_id,
        ResultCell {
            value: DatumV::from_i32(result),
            isnull: false,
        },
    );
    Ok(())
}

/// `ExecEvalMergeSupportFunc(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — evaluate MERGE_ACTION() within a MERGE command.
///
/// Returns information about the current MERGE action for its RETURNING list.
pub fn ExecEvalMergeSupportFunc<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    _econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // C:
    //   ModifyTableState *mtstate = castNode(ModifyTableState, state->parent);
    //   MergeActionState *relaction = mtstate->mt_merge_action;
    //   if (!relaction)
    //       elog(ERROR, "no merge action in progress");
    //   switch (relaction->mas_action->commandType) { ... }
    let resvalue_id = {
        let steps = state.steps.as_ref().expect("ExecEvalMergeSupportFunc: steps not ready");
        steps[op].resvalue
    };
    let mcx = estate.es_query_cxt;

    // ModifyTableState *mtstate = castNode(ModifyTableState, state->parent);
    // The parent ModifyTableState is owned by nodeModifyTable, which has not yet
    // threaded its `T_ModifyTableState` into `PlanStateNode`; `as_modify_table_state()`
    // yields the cast.
    let parent = state.parent.as_deref().expect(
        "ExecEvalMergeSupportFunc: EEOP_MERGE_SUPPORT_FUNC step has no parent PlanState",
    );
    let mtstate = parent.as_modify_table_state().expect(
        "ExecEvalMergeSupportFunc: castNode(ModifyTableState, state->parent) — the parent \
         ModifyTableState is owned by nodeModifyTable, which has not yet threaded its \
         T_ModifyTableState into PlanStateNode",
    );

    // MergeActionState *relaction = mtstate->mt_merge_action;
    // if (!relaction) elog(ERROR, "no merge action in progress");
    let relaction = mtstate
        .mt_merge_action
        .as_deref()
        .ok_or_else(|| PgError::error("no merge action in progress"))?;

    let command_type = relaction
        .mas_action
        .as_deref()
        .expect("ExecEvalMergeSupportFunc: MergeActionState has no mas_action")
        .commandType;

    // Return the MERGE action ("INSERT", "UPDATE", or "DELETE")
    let value = match command_type {
        // *op->resvalue = PointerGetDatum(cstring_to_text_with_len("INSERT", 6));
        CmdType::CMD_INSERT => varlena::cstring_to_text::call(mcx, "INSERT")?,
        CmdType::CMD_UPDATE => varlena::cstring_to_text::call(mcx, "UPDATE")?,
        CmdType::CMD_DELETE => varlena::cstring_to_text::call(mcx, "DELETE")?,
        CmdType::CMD_NOTHING => {
            return Err(PgError::error("unexpected merge action: DO NOTHING"));
        }
        other => {
            return Err(PgError::error(format!(
                "unrecognized commandType: {}",
                other as u32
            )));
        }
    };

    // *op->resvalue = ...; *op->resnull = false;
    state.result_cells.set(
        resvalue_id,
        ResultCell {
            value: DatumV::ByVal(value),
            isnull: false,
        },
    );
    Ok(())
}
