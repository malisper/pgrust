//! DELETE family of `executor/nodeModifyTable.c`: the prologue (BEFORE-ROW
//! trigger firing / FDW delete), the act (`table_tuple_delete`), and the
//! epilogue (AFTER-ROW triggers + transition capture). The `ExecDelete` driver
//! lives in the [`crate::delete_exec`] sub-module.

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodes::CmdType;
use types_nodes::{EStateData, ModifyTableState, RriId, SlotId};
use types_tableam::tableam::TM_Result;
use types_tuple::heaptuple::{HeapTuple, ItemPointerData};

use crate::ModifyTableContext;

/// `ExecDeletePrologue(context, resultRelInfo, tupleid, oldtuple,
/// epqreturnslot, result)` — fire BEFORE ROW DELETE triggers (or dispatch the
/// FDW), returning `false` to skip the delete. `result` carries the trigger's
/// `TM_Result` to the caller.
pub fn ExecDeletePrologue<'mcx>(
    mcx: Mcx<'mcx>,
    context: &mut ModifyTableContext,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    tupleid: Option<&ItemPointerData>,
    oldtuple: HeapTuple<'mcx>,
    epqreturnslot: Option<&mut Option<SlotId>>,
    result: Option<&mut TM_Result>,
) -> PgResult<bool> {
    let mut result = result;
    if let Some(r) = result.as_deref_mut() {
        *r = TM_Result::TM_Ok;
    }

    // BEFORE ROW DELETE triggers
    let has_before_row = {
        let rri = &estate.es_result_rel_pool[result_rel_info.0 as usize];
        rri.ri_TrigDesc
            .as_ref()
            .map(|td| td.trig_delete_before_row)
            .unwrap_or(false)
    };
    if has_before_row {
        // Flush any pending inserts, so rows are visible to the triggers
        if !estate.es_insert_pending_result_relations.is_empty() {
            crate::insert::ExecPendingInserts(mcx, estate)?;
        }

        return backend_commands_trigger_seams::exec_br_delete_triggers::call(
            estate,
            &mut mtstate.mt_epqstate,
            result_rel_info,
            tupleid,
            oldtuple.as_deref(),
            epqreturnslot,
            result,
            &mut context.tmfd,
            mtstate.operation == CmdType::CMD_MERGE,
        );
    }

    Ok(true)
}

/// `ExecDeleteAct(context, resultRelInfo, tupleid, changingPart)` — perform the
/// actual `table_tuple_delete`, returning its `TM_Result`.
pub fn ExecDeleteAct<'mcx>(
    context: &mut ModifyTableContext,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    tupleid: &ItemPointerData,
    changing_part: bool,
) -> PgResult<TM_Result> {
    let snapshot = estate.es_snapshot.as_deref().cloned();
    let crosscheck = estate.es_crosscheck_snapshot.as_deref().cloned();
    let output_cid = estate.es_output_cid;
    let rri = &estate.es_result_rel_pool[result_rel_info.0 as usize];
    let rel = rri
        .ri_RelationDesc
        .as_ref()
        .expect("ExecDeleteAct: ri_RelationDesc must be open");

    backend_access_table_tableam::table_tuple_delete(
        rel,
        tupleid,
        output_cid,
        &snapshot,
        &crosscheck,
        true, // wait for commit
        &mut context.tmfd,
        changing_part,
    )
}

/// `ExecDeleteEpilogue(context, resultRelInfo, tupleid, oldtuple,
/// changingPart)` — fire AFTER ROW DELETE triggers and capture the OLD tuple
/// for transition tables.
pub fn ExecDeleteEpilogue<'mcx>(
    mcx: Mcx<'mcx>,
    context: &mut ModifyTableContext,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    tupleid: Option<&ItemPointerData>,
    oldtuple: HeapTuple<'mcx>,
    changing_part: bool,
) -> PgResult<()> {
    let _ = (mcx, context);

    // If this delete is the result of a partition key update that moved the
    // tuple to a new partition, put this row into the transition OLD TABLE,
    // if there is one. We need to do this separately for DELETE and INSERT
    // because they happen on different tables.
    //
    // `ar_delete_trig_tcs` starts as mtstate->mt_transition_capture; the
    // matched-CMD_UPDATE branch fires the OLD-table capture itself and then
    // nulls it so the AR DELETE trigger below doesn't capture the row again.
    let mut ar_delete_trig_tcs = mtstate.mt_transition_capture.is_some();
    let update_old_table = mtstate
        .mt_transition_capture
        .as_ref()
        .map(|tc| tc.tcs_update_old_table)
        .unwrap_or(false);

    if mtstate.operation == CmdType::CMD_UPDATE
        && mtstate.mt_transition_capture.is_some()
        && update_old_table
    {
        backend_commands_trigger_seams::exec_ar_update_triggers::call(
            estate,
            result_rel_info,
            None,
            None,
            tupleid,
            oldtuple.as_deref(),
            None,
            &[],
            mtstate.mt_transition_capture.as_deref_mut(),
            false,
        )?;

        // We've already captured the OLD TABLE row, so make sure any AR
        // DELETE trigger fired below doesn't capture it again.
        ar_delete_trig_tcs = false;
    }

    // AFTER ROW DELETE Triggers
    let tcs = if ar_delete_trig_tcs {
        mtstate.mt_transition_capture.as_deref()
    } else {
        None
    };
    backend_commands_trigger_seams::exec_ar_delete_triggers::call(
        estate,
        result_rel_info,
        tupleid,
        oldtuple.as_deref(),
        tcs,
        changing_part,
    )
}
