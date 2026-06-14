//! Family: slot creation / tuple table — `MakeTupleTableSlot`,
//! `ExecAllocTableSlot`, `ExecInitScanTupleSlot`, `ExecInitExtraTupleSlot`,
//! `ExecInitResultSlot`/`ExecInitResultTupleSlotTL`, `ExecInitNullTupleSlot`,
//! `ExecSetSlotDescriptor`, `ExecResetTupleTable`, `MakeSingleTupleTableSlot`,
//! `ExecDropSingleTupleTableSlot` (execTuples.c) and the tableam slot create.
//!
//! **This family owns and installs `backend-executor-execTuples-seams`** — the
//! 16 declarations the executor node units call back into.
//!
//! # Slot model: trimmed header vs. payload model
//!
//! `EState::es_tupleTable` is the `SlotId`-addressed pool of the trimmed
//! [`TupleTableSlot`] header (`tts_flags`/`tts_ops`/`tts_tid`/`tts_tableOid`).
//! The per-attribute `tts_values`/`tts_isnull` arrays, `tts_tupleDescriptor`,
//! `tts_nvalid` and the slot's own context — the payload model carried by
//! [`types_nodes::tuptable::SlotData`] — are not yet woven into the pool (the
//! scaffold's deferred header/`SlotBase` convergence). The slot-*creation*
//! routines below build and install the header (flags + ops identity) exactly
//! as `MakeTupleTableSlot` does for those fields; the routines that *operate on
//! a slot's stored tuple* (clear/copy/getsysattr/getallattrs, the all-null
//! store, and the descriptor-install array allocation) genuinely require the
//! payload arrays and stay seam-and-panic until the payload model lands.

use mcx::Mcx;
use types_core::primitive::AttrNumber;
use types_error::PgResult;
use types_nodes::execnodes::{EStateData, PlanStateData, ScanStateData};
use types_nodes::executor::{TTS_FLAG_EMPTY, TTS_FLAG_FIXED};
use types_nodes::tuptable::SlotData;
use types_nodes::{SlotId, TupleSlotKind, TupleTableSlot};
use types_tuple::backend_access_common_heaptuple::DeformedColumn;
use types_tuple::heaptuple::{TupleDesc, TupleDescData};

// ===========================================================================
//  MakeTupleTableSlot header construction
// ===========================================================================

/// The header-field portion of `MakeTupleTableSlot(tupleDesc, tts_ops)`
/// (execTuples.c): build a fresh, empty slot of the given kind. When a fixed
/// descriptor is supplied the slot is marked `TTS_FLAG_FIXED`. The payload
/// arrays / descriptor pin (`tts_values`/`tts_isnull`/`tts_tupleDescriptor`/
/// `PinTupleDesc`) and the kind's `init` callback belong to the payload model
/// and land at convergence; the pool only carries the header today.
fn make_tuple_table_slot_header(has_descriptor: bool, tts_ops: TupleSlotKind) -> TupleTableSlot {
    let mut slot = TupleTableSlot::default();
    // slot->tts_ops = tts_ops; slot->type = T_TupleTableSlot;
    slot.tts_ops = tts_ops;
    // slot->tts_flags |= TTS_FLAG_EMPTY;
    slot.tts_flags |= TTS_FLAG_EMPTY;
    // if (tupleDesc != NULL) slot->tts_flags |= TTS_FLAG_FIXED;
    if has_descriptor {
        slot.tts_flags |= TTS_FLAG_FIXED;
    }
    slot
}

/// `ExecAllocTableSlot(&estate->es_tupleTable, desc, tts_ops)` (execTuples.c):
/// build a slot (`MakeTupleTableSlot`) and append it to the per-query tuple
/// table, returning its pool id (the C pointer).
fn exec_alloc_table_slot(
    estate: &mut EStateData<'_>,
    has_descriptor: bool,
    tts_ops: TupleSlotKind,
) -> PgResult<SlotId> {
    let slot = make_tuple_table_slot_header(has_descriptor, tts_ops);
    // *tupleTable = lappend(*tupleTable, slot);
    estate.make_slot(slot)
}

// ===========================================================================
//  Standalone-slot creation/teardown over the owned [`SlotData`] payload model.
//  These mirror `MakeSingleTupleTableSlot` / `ExecDropSingleTupleTableSlot`
//  (execTuples.c) for in-crate callers (e.g. the `begin/do/end_tup_output`
//  family) that hold the live `SlotData` directly rather than a pool id.
// ===========================================================================

/// `MakeSingleTupleTableSlot(tupdesc, tts_ops)` (execTuples.c): create a
/// standalone slot of the given class, fixed to `tupdesc`, allocated in `mcx`.
pub fn MakeSingleTupleTableSlot<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: TupleDesc<'mcx>,
    tts_ops: TupleSlotKind,
) -> PgResult<SlotData<'mcx>> {
    // TupleTableSlot *slot = MakeTupleTableSlotWithOps(tupdesc, tts_ops); return slot;
    crate::slot_payload_model::MakeTupleTableSlot(mcx, tupdesc, tts_ops)
}

/// `ExecDropSingleTupleTableSlot(slot)` (execTuples.c): release a slot made
/// with [`MakeSingleTupleTableSlot`] (clears it first, releasing any pin).
pub fn ExecDropSingleTupleTableSlot(slot: SlotData<'_>) -> PgResult<()> {
    // ExecClearTuple(slot); ... pfree(slot);
    // The owned SlotData carries the value arrays and (optional) stored tuple;
    // clearing + freeing reduces to dropping the owned value here.
    drop(slot);
    Ok(())
}

// ===========================================================================
//  Seam install targets — signatures mirror
//  `backend-executor-execTuples-seams` exactly.
// ===========================================================================

/// Seam `slot_getallattrs` (provisional contract; see seam doc).
///
/// PAYLOAD MODEL: fully deconstructing the slot needs its `tts_values`/
/// `tts_isnull` payload arrays, which the trimmed pool header does not carry.
/// Stays seam-and-panic until the slot payload model lands.
fn seam_slot_getallattrs<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &TupleTableSlot,
) -> PgResult<mcx::PgVec<'mcx, DeformedColumn<'mcx>>> {
    panic!("execTuples.c slot_getallattrs — needs the slot payload model (tts_values/tts_isnull)")
}

/// Seam `exec_init_result_tuple_slot_tl` — `ExecInitResultTupleSlotTL`.
fn seam_exec_init_result_tuple_slot_tl<'mcx>(
    planstate: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    tts_ops: TupleSlotKind,
) -> PgResult<()> {
    // ExecInitResultTypeTL(planstate);
    backend_executor_execTuples_seams::exec_init_result_type_tl::call(planstate, estate)?;
    // ExecInitResultSlot(planstate, tts_ops);
    seam_exec_init_result_slot(planstate, estate, tts_ops)
}

/// Seam `exec_clear_tuple` — `ExecClearTuple`.
///
/// PAYLOAD MODEL: the `clear` callback resets the slot's stored-tuple payload,
/// which the trimmed pool header does not carry. Stays seam-and-panic until the
/// slot payload model lands.
fn seam_exec_clear_tuple(_slot: &mut TupleTableSlot) -> PgResult<()> {
    panic!("execTuples.c ExecClearTuple — needs the slot payload model (tts_ops->clear)")
}

/// Seam `exec_copy_slot` — `ExecCopySlot` (provisional contract; see seam doc).
///
/// PAYLOAD MODEL: copying a tuple between slots needs both slots' stored-tuple
/// payloads. Stays seam-and-panic until the slot payload model lands.
fn seam_exec_copy_slot<'mcx>(
    _mcx: Mcx<'mcx>,
    _dstslot: &mut TupleTableSlot,
    _srcslot: &TupleTableSlot,
) -> PgResult<()> {
    panic!("execTuples.c ExecCopySlot — needs the slot payload model (tts_ops->copyslot)")
}

/// Seam `exec_init_result_slot` — `ExecInitResultSlot`.
fn seam_exec_init_result_slot<'mcx>(
    planstate: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    tts_ops: TupleSlotKind,
) -> PgResult<()> {
    // slot = ExecAllocTableSlot(&planstate->state->es_tupleTable,
    //                           planstate->ps_ResultTupleDesc, tts_ops);
    let has_descriptor = planstate.ps_ResultTupleDesc.is_some();
    let slot = exec_alloc_table_slot(estate, has_descriptor, tts_ops)?;
    // planstate->ps_ResultTupleSlot = slot;
    planstate.ps_ResultTupleSlot = Some(slot);

    // planstate->resultopsfixed = planstate->ps_ResultTupleDesc != NULL;
    planstate.resultopsfixed = has_descriptor;
    // planstate->resultops = tts_ops;
    planstate.resultops = Some(tts_ops);
    // planstate->resultopsset = true;
    planstate.resultopsset = true;
    Ok(())
}

/// Seam `exec_init_scan_tuple_slot` — `ExecInitScanTupleSlot`.
fn seam_exec_init_scan_tuple_slot<'mcx>(
    estate: &mut EStateData<'mcx>,
    scanstate: &mut ScanStateData<'mcx>,
    tupledesc: TupleDesc<'mcx>,
    tts_ops: TupleSlotKind,
) -> PgResult<()> {
    // scanstate->ss_ScanTupleSlot = ExecAllocTableSlot(&estate->es_tupleTable,
    //                                                  tupledesc, tts_ops);
    let has_descriptor = tupledesc.is_some();
    let slot = exec_alloc_table_slot(estate, has_descriptor, tts_ops)?;
    scanstate.ss_ScanTupleSlot = Some(slot);
    // scanstate->ps.scandesc = tupledesc; (payload-model: the descriptor moves
    // into the slot at convergence; the trimmed scanstate carries no scandesc)
    let _ = tupledesc;
    // scanstate->ps.scanopsfixed = tupledesc != NULL;
    scanstate.ps.scanopsfixed = has_descriptor;
    // scanstate->ps.scanops = tts_ops;
    scanstate.ps.scanops = Some(tts_ops);
    // scanstate->ps.scanopsset = true;
    scanstate.ps.scanopsset = true;
    Ok(())
}

/// Seam `exec_init_extra_tuple_slot` — `ExecInitExtraTupleSlot`.
fn seam_exec_init_extra_tuple_slot<'mcx>(
    estate: &mut EStateData<'mcx>,
    tupledesc: TupleDesc<'mcx>,
    tts_ops: TupleSlotKind,
) -> PgResult<SlotId> {
    // return ExecAllocTableSlot(&estate->es_tupleTable, tupledesc, tts_ops);
    exec_alloc_table_slot(estate, tupledesc.is_some(), tts_ops)
}

/// Seam `exec_set_slot_descriptor` — `ExecSetSlotDescriptor`.
///
/// PAYLOAD MODEL: installing a descriptor allocates the slot's per-column
/// `tts_values`/`tts_isnull` arrays and pins the descriptor, both payload-model
/// state the trimmed pool header lacks. Stays seam-and-panic until the slot
/// payload model lands.
fn seam_exec_set_slot_descriptor<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _slot: SlotId,
    _tupdesc: TupleDesc<'mcx>,
) -> PgResult<()> {
    panic!("execTuples.c ExecSetSlotDescriptor — needs the slot payload model (tts_values/tts_isnull)")
}

/// Seam `exec_store_all_null_tuple` — `ExecStoreAllNullTuple`.
///
/// PAYLOAD MODEL: filling every column with NULL writes the slot's
/// `tts_values`/`tts_isnull` arrays then `ExecStoreVirtualTuple`. Stays
/// seam-and-panic until the slot payload model lands.
fn seam_exec_store_all_null_tuple<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _slot: SlotId,
) -> PgResult<()> {
    panic!("execTuples.c ExecStoreAllNullTuple — needs the slot payload model (tts_values/tts_isnull)")
}

/// Seam `make_single_tuple_table_slot` — `MakeSingleTupleTableSlot`.
fn seam_make_single_tuple_table_slot<'mcx>(
    _mcx: Mcx<'mcx>,
    tupdesc: TupleDesc<'mcx>,
    tts_ops: TupleSlotKind,
) -> PgResult<TupleTableSlot> {
    // TupleTableSlot *slot = MakeTupleTableSlot(tupdesc, tts_ops); return slot;
    Ok(make_tuple_table_slot_header(tupdesc.is_some(), tts_ops))
}

/// Seam `exec_drop_single_tuple_table_slot` — `ExecDropSingleTupleTableSlot`.
///
/// C: `ExecClearTuple(slot); slot->tts_ops->release(slot);
/// ReleaseTupleDesc(...); pfree(tts_values/tts_isnull); pfree(slot);`. The
/// trimmed pool header carries no stored-tuple payload, no descriptor pin and
/// no separately-`palloc`'d value arrays, so clear/release/free reduce to
/// dropping the owned header value, which Rust does when `slot` falls out of
/// scope here.
fn seam_exec_drop_single_tuple_table_slot(slot: TupleTableSlot) -> PgResult<()> {
    drop(slot);
    Ok(())
}

/// Seam `slot_getsysattr` — `slot_getsysattr`.
///
/// PAYLOAD MODEL: fetching a system attribute dispatches `tts_ops->getsysattr`
/// against the slot's stored tuple. Stays seam-and-panic until the slot payload
/// model lands.
fn seam_slot_getsysattr<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &TupleTableSlot,
    _attnum: AttrNumber,
) -> PgResult<(types_tuple::backend_access_common_heaptuple::Datum<'mcx>, bool)> {
    panic!("execTuples.c slot_getsysattr — needs the slot payload model (tts_ops->getsysattr)")
}

/// Seam `exec_init_null_tuple_slot` — `ExecInitNullTupleSlot`.
fn seam_exec_init_null_tuple_slot<'mcx>(
    estate: &mut EStateData<'mcx>,
    tupledesc: TupleDesc<'mcx>,
    tts_ops: TupleSlotKind,
) -> PgResult<SlotId> {
    // TupleTableSlot *slot = ExecInitExtraTupleSlot(estate, tupType, tts_ops);
    let slot = seam_exec_init_extra_tuple_slot(estate, tupledesc, tts_ops)?;
    // return ExecStoreAllNullTuple(slot);
    seam_exec_store_all_null_tuple(estate, slot)?;
    Ok(slot)
}

/// Seam `exec_get_result_type` — `ExecGetResultType` (execUtils.c surface,
/// owned here for the result-slot family).
fn seam_exec_get_result_type<'a, 'mcx>(
    planstate: &'a PlanStateData<'mcx>,
) -> Option<&'a TupleDescData<'mcx>> {
    // return planstate->ps_ResultTupleDesc;
    planstate.ps_ResultTupleDesc.as_deref()
}

/// Seam `exec_get_result_slot_ops` — `ExecGetResultSlotOps`.
///
/// C falls through to `planstate->ps_ResultTupleSlot->tts_ops` when `resultops`
/// is unset; that branch needs the EState slot pool to resolve the slot, which
/// this seam's signature (planstate only) does not carry. The seam contract is
/// scoped to callers that have already run `ExecInitResultTupleSlotTL` (so
/// `resultopsset && resultops`), where this returns `resultops` directly; the
/// `&TTSOpsVirtual` default covers the no-result-slot tail.
fn seam_exec_get_result_slot_ops<'mcx>(planstate: &PlanStateData<'mcx>) -> TupleSlotKind {
    // if (planstate->resultopsset && planstate->resultops)
    //     return planstate->resultops;
    if planstate.resultopsset {
        if let Some(ops) = planstate.resultops {
            return ops;
        }
    }
    // (fallthrough) return &TTSOpsVirtual;
    TupleSlotKind::Virtual
}

/// Seam `exec_init_result_type_tl` — `ExecInitResultTypeTL` (execTuples.c):
/// build the node's result tuple descriptor from its plan's targetlist and
/// store it in `planstate.ps_ResultTupleDesc`.
///
/// ```c
/// void
/// ExecInitResultTypeTL(PlanState *planstate)
/// {
///     TupleDesc   tupDesc = ExecTypeFromTL(planstate->plan->targetlist);
///     planstate->ps_ResultTupleDesc = tupDesc;
/// }
/// ```
fn seam_exec_init_result_type_tl<'mcx>(
    planstate: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    // planstate->plan->targetlist
    let tlist = planstate
        .plan
        .as_deref()
        .map(|node| node.plan_head().targetlist.as_deref().unwrap_or(&[]))
        .unwrap_or(&[]);
    // TupleDesc tupDesc = ExecTypeFromTL(planstate->plan->targetlist);
    let tup_desc = crate::exectype_tupoutput::ExecTypeFromTL(mcx, tlist)?;
    // planstate->ps_ResultTupleDesc = tupDesc;
    planstate.ps_ResultTupleDesc = tup_desc;
    Ok(())
}

/// Seam `exec_alloc_table_slot` — `ExecAllocTableSlot` (execTuples.c):
///
/// ```c
/// TupleTableSlot *
/// ExecAllocTableSlot(List **tupleTable, TupleDesc desc, const TupleTableSlotOps *tts_ops)
/// {
///     TupleTableSlot *slot = MakeTupleTableSlot(desc, tts_ops);
///     *tupleTable = lappend(*tupleTable, slot);
///     return slot;
/// }
/// ```
///
/// Adapter onto the in-crate [`exec_alloc_table_slot`] header builder. The seam
/// carries the descriptor (C shares the pointer into the slot); the trimmed
/// `es_tupleTable` pool header records only whether a fixed descriptor was
/// supplied (`TTS_FLAG_FIXED`), exactly as the already-installed
/// `exec_init_scan_tuple_slot` / `exec_init_extra_tuple_slot` seams do — the
/// descriptor itself moves into the slot's payload at the payload-model
/// convergence. So the shim forwards `desc.is_some()` and drops the body.
fn seam_exec_alloc_table_slot<'mcx>(
    estate: &mut EStateData<'mcx>,
    desc: TupleDesc<'mcx>,
    tts_ops: TupleSlotKind,
) -> PgResult<SlotId> {
    // TupleTableSlot *slot = MakeTupleTableSlot(desc, tts_ops);
    // *tupleTable = lappend(*tupleTable, slot); return slot;
    exec_alloc_table_slot(estate, desc.is_some(), tts_ops)
}

/// Install every seam this unit owns.
pub fn init_seams() {
    use backend_executor_execTuples_seams as seams;
    seams::exec_alloc_table_slot::set(seam_exec_alloc_table_slot);
    seams::slot_getallattrs::set(seam_slot_getallattrs);
    seams::exec_init_result_tuple_slot_tl::set(seam_exec_init_result_tuple_slot_tl);
    seams::exec_init_result_type_tl::set(seam_exec_init_result_type_tl);
    seams::exec_clear_tuple::set(seam_exec_clear_tuple);
    seams::exec_copy_slot::set(seam_exec_copy_slot);
    seams::exec_init_result_slot::set(seam_exec_init_result_slot);
    seams::exec_init_scan_tuple_slot::set(seam_exec_init_scan_tuple_slot);
    seams::exec_init_extra_tuple_slot::set(seam_exec_init_extra_tuple_slot);
    seams::exec_set_slot_descriptor::set(seam_exec_set_slot_descriptor);
    seams::exec_store_all_null_tuple::set(seam_exec_store_all_null_tuple);
    seams::make_single_tuple_table_slot::set(seam_make_single_tuple_table_slot);
    seams::exec_drop_single_tuple_table_slot::set(seam_exec_drop_single_tuple_table_slot);
    seams::slot_getsysattr::set(seam_slot_getsysattr);
    seams::exec_init_null_tuple_slot::set(seam_exec_init_null_tuple_slot);
    seams::exec_get_result_type::set(seam_exec_get_result_type);
    seams::exec_get_result_slot_ops::set(seam_exec_get_result_slot_ops);
    // ExecTypeFromTL is fully owned + implemented in this crate's
    // exectype_tupoutput family (no pool-payload / Plan-model dependency), so
    // its seam is installed here.
    seams::exec_type_from_tl::set(crate::exectype_tupoutput::ExecTypeFromTL);
    // ExecTypeFromExprList is likewise fully owned + implemented in this crate's
    // exectype_tupoutput family, so its seam is installed here.
    seams::exec_type_from_expr_list::set(crate::exectype_tupoutput::ExecTypeFromExprList);
    // ExecInitResultTypeTL lives in execTuples.c (sets ps_ResultTupleDesc from
    // the plan's targetlist via ExecTypeFromTL); home its seam here.
    seams::exec_init_result_type_tl::set(crate::exectype_tupoutput::ExecInitResultTypeTL);
}
