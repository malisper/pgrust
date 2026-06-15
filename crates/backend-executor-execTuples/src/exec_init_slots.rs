//! Family: slot creation / tuple table — `MakeTupleTableSlot`,
//! `ExecAllocTableSlot`, `ExecInitScanTupleSlot`, `ExecInitExtraTupleSlot`,
//! `ExecInitResultSlot`/`ExecInitResultTupleSlotTL`, `ExecInitNullTupleSlot`,
//! `ExecSetSlotDescriptor`, `ExecResetTupleTable`, `MakeSingleTupleTableSlot`,
//! `ExecDropSingleTupleTableSlot` (execTuples.c) and the tableam slot create.
//!
//! **This family owns and installs `backend-executor-execTuples-seams`** — the
//! 16 declarations the executor node units call back into.
//!
//! # Slot model
//!
//! `EState::es_tupleTable` is the `SlotId`-addressed pool of the unified
//! payload-bearing [`types_nodes::tuptable::SlotData`] (one `TupleTableSlot`
//! carrying `tts_flags`/`tts_ops`/`tts_tid`/`tts_tableOid` AND the per-attribute
//! `tts_values`/`tts_isnull`/`tts_nvalid`/`tts_tupleDescriptor` payload). The
//! standalone (non-pool) creation routines (`MakeSingleTupleTableSlot`) build
//! the same payload-bearing slot; the operating routines (clear/copy/
//! getsysattr/getallattrs/all-null store/set-descriptor) run the real slot ops.

use mcx::Mcx;
use types_core::primitive::AttrNumber;
use types_error::PgResult;
use types_nodes::execnodes::{EStateData, PlanStateData, ScanStateData};
use types_nodes::tuptable::SlotData;
use types_nodes::{SlotId, TupleSlotKind};
use types_tuple::backend_access_common_heaptuple::DeformedColumn;
use types_tuple::heaptuple::{TupleDesc, TupleDescData};

// ===========================================================================
//  ExecAllocTableSlot
// ===========================================================================

/// `ExecAllocTableSlot(&estate->es_tupleTable, desc, tts_ops)` (execTuples.c):
/// build a slot (`MakeTupleTableSlot`) and append it to the per-query tuple
/// table, returning its pool id (the C pointer).
///
/// The slot is the proper payload-bearing [`SlotData`] superstructure of the
/// requested kind (`Virtual/Heap/Minimal/BufferHeap`), fixed to `desc` with its
/// `tts_values`/`tts_isnull` arrays sized — exactly `MakeTupleTableSlot` — so
/// the store/fetch/copy callbacks (e.g. the minimal-tuple ops) can downcast to
/// the right variant. It allocates in the per-query context (`tts_mcxt`).
fn exec_alloc_table_slot<'mcx>(
    estate: &mut EStateData<'mcx>,
    desc: TupleDesc<'mcx>,
    tts_ops: TupleSlotKind,
) -> PgResult<SlotId> {
    let mcx = estate.es_query_cxt;
    // TupleTableSlot *slot = MakeTupleTableSlot(desc, tts_ops);
    let slot = crate::slot_payload_model::MakeTupleTableSlot(mcx, desc, tts_ops)?;
    // *tupleTable = lappend(*tupleTable, slot);
    estate.push_slot_data(slot)
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

/// Seam `slot_getallattrs` (STANDALONE-SLOT form; see seam doc).
///
/// The callers (`CopyOneRowTo`'s `table_slot_create` scan slot and logical
/// replication's `logicalrep_write_tuple` slot) hold a standalone (non-pool)
/// payload-bearing [`SlotData`]; this runs the real `slot_getallattrs` deform
/// and copies the per-attribute `(value, isnull)` array out into `mcx`.
fn seam_slot_getallattrs<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<mcx::PgVec<'mcx, DeformedColumn<'mcx>>> {
    // slot_getallattrs(slot): deform every column into tts_values/tts_isnull.
    crate::slot_deform::slot_getallattrs(mcx, slot)?;
    // Copy out (value, isnull) per attribute (C reads slot->tts_values[i]).
    let base = slot.base();
    let nvalid = base.tts_nvalid as usize;
    let mut cols: mcx::PgVec<'mcx, DeformedColumn<'mcx>> = mcx::vec_with_capacity_in(mcx, nvalid)?;
    for i in 0..nvalid {
        let value = base.tts_values[i].clone_in(mcx)?;
        cols.push((value, base.tts_isnull[i]));
    }
    Ok(cols)
}

/// Seam `slot_getallattrs_by_id` — `slot_getallattrs` resolving the pool
/// `SlotId` to its live payload-bearing `&mut SlotData`, fully deconstructing it
/// and returning its per-attribute `(value, isnull)` array copied into `mcx`.
///
/// ```c
/// slot_getallattrs(slot);
/// for (i = 0; i < natts; i++)
///     cols[i] = (slot->tts_values[i], slot->tts_isnull[i]);
/// ```
fn seam_slot_getallattrs_by_id<'mcx>(
    estate: &mut EStateData<'mcx>,
    slot: SlotId,
) -> PgResult<mcx::PgVec<'mcx, DeformedColumn<'mcx>>> {
    let mcx = estate.es_query_cxt;
    let slot_data = estate.slot_data_mut(slot);
    // slot_getallattrs(slot): deform every column into tts_values/tts_isnull.
    crate::slot_deform::slot_getallattrs(mcx, slot_data)?;
    // Copy out (value, isnull) per attribute (C reads slot->tts_values[i]).
    let base = slot_data.base();
    let nvalid = base.tts_nvalid as usize;
    let mut cols: mcx::PgVec<'mcx, DeformedColumn<'mcx>> = mcx::vec_with_capacity_in(mcx, nvalid)?;
    for i in 0..nvalid {
        let value = base.tts_values[i].clone_in(mcx)?;
        cols.push((value, base.tts_isnull[i]));
    }
    Ok(cols)
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

/// Seam `exec_clear_tuple` — `ExecClearTuple`, resolving the pool `SlotId` to
/// its live payload-bearing `&mut SlotData` and running the real `clear` op.
fn seam_exec_clear_tuple<'mcx>(estate: &mut EStateData<'mcx>, slot: SlotId) -> PgResult<()> {
    crate::slot_store_fetch::ExecClearTuple(estate.slot_data_mut(slot))
}

/// Seam `exec_clear_tuple_by_id` — `ExecClearTuple` resolving the pool `SlotId`
/// to its live payload-bearing `&mut SlotData` first.
fn seam_exec_clear_tuple_by_id<'mcx>(estate: &mut EStateData<'mcx>, slot: SlotId) -> PgResult<()> {
    crate::slot_store_fetch::ExecClearTuple(estate.slot_data_mut(slot))
}

/// Seam `exec_reset_one_slot` — `ExecResetTupleTable`'s per-slot processing.
/// The full payload-bearing [`SlotData`] is in scope, so this delegates to the
/// implemented [`crate::slot_store_fetch::ExecResetOneSlot`].
fn seam_exec_reset_one_slot(slot: &mut SlotData<'_>) -> PgResult<()> {
    crate::slot_store_fetch::ExecResetOneSlot(slot)
}

/// Seam `exec_copy_slot` — `ExecCopySlot`, resolving the two pool `SlotId`s to
/// their live payload-bearing `&mut SlotData` pair and running the real
/// `copyslot` op (copy the source slot's tuple into the destination).
fn seam_exec_copy_slot<'mcx>(
    estate: &mut EStateData<'mcx>,
    dstslot: SlotId,
    srcslot: SlotId,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let (dst, src) = estate.slot_data_pair_mut(dstslot, srcslot);
    crate::slot_store_fetch::ExecCopySlot(mcx, dst, src)
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
    // C shares the descriptor pointer into the slot; the owned slot needs its
    // own `'mcx` copy (the slot pins/holds the descriptor), so clone it.
    let desc = match planstate.ps_ResultTupleDesc.as_deref() {
        Some(d) => Some(mcx::alloc_in(estate.es_query_cxt, d.clone_in(estate.es_query_cxt)?)?),
        None => None,
    };
    let slot = exec_alloc_table_slot(estate, desc, tts_ops)?;
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
    let slot = exec_alloc_table_slot(estate, tupledesc, tts_ops)?;
    scanstate.ss_ScanTupleSlot = Some(slot);
    // scanstate->ps.scandesc = tupledesc; (the descriptor now lives in the slot's
    // payload; the trimmed scanstate carries no scandesc)
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
    exec_alloc_table_slot(estate, tupledesc, tts_ops)
}

/// Seam `exec_set_slot_descriptor` — `ExecSetSlotDescriptor`. Resolves the pool
/// `SlotId` to its live `SlotData` and installs the descriptor.
fn seam_exec_set_slot_descriptor<'mcx>(
    estate: &mut EStateData<'mcx>,
    slot: SlotId,
    tupdesc: TupleDesc<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    crate::slot_store_fetch::ExecSetSlotDescriptor(mcx, estate.slot_data_mut(slot), tupdesc)
}

/// Seam `exec_store_all_null_tuple` — `ExecStoreAllNullTuple`. Resolves the pool
/// `SlotId` to its live `SlotData` and fills every column NULL.
fn seam_exec_store_all_null_tuple<'mcx>(
    estate: &mut EStateData<'mcx>,
    slot: SlotId,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    crate::slot_store_fetch::ExecStoreAllNullTuple(mcx, estate.slot_data_mut(slot))
}

/// Seam `make_single_tuple_table_slot` — `MakeSingleTupleTableSlot`.
fn seam_make_single_tuple_table_slot<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: TupleDesc<'mcx>,
    tts_ops: TupleSlotKind,
) -> PgResult<SlotData<'mcx>> {
    // TupleTableSlot *slot = MakeTupleTableSlot(tupdesc, tts_ops); return slot;
    MakeSingleTupleTableSlot(mcx, tupdesc, tts_ops)
}

/// Seam `exec_drop_single_tuple_table_slot` — `ExecDropSingleTupleTableSlot`.
///
/// C: `ExecClearTuple(slot); slot->tts_ops->release(slot);
/// ReleaseTupleDesc(...); pfree(tts_values/tts_isnull); pfree(slot);`. The
/// trimmed pool header carries no stored-tuple payload, no descriptor pin and
/// no separately-`palloc`'d value arrays, so clear/release/free reduce to
/// dropping the owned header value, which Rust does when `slot` falls out of
/// scope here.
fn seam_exec_drop_single_tuple_table_slot(slot: SlotData<'_>) -> PgResult<()> {
    // ExecClearTuple(slot); slot->tts_ops->release(slot); ReleaseTupleDesc(...);
    // pfree(slot). The owned SlotData carries the value arrays, descriptor pin
    // and any stored tuple; clear/release/free reduce to dropping the value.
    ExecDropSingleTupleTableSlot(slot)
}

/// Seam `slot_getsysattr` — `slot_getsysattr` (STANDALONE-SLOT form; see seam
/// doc).
///
/// The only caller (`GetTupleTransactionInfo`, logical-replication conflict
/// detection) holds a standalone (non-pool) payload-bearing [`SlotData`]; this
/// dispatches the real `tts_ops->getsysattr` against the slot's stored tuple.
fn seam_slot_getsysattr<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
    attnum: AttrNumber,
) -> PgResult<(types_tuple::backend_access_common_heaptuple::Datum<'mcx>, bool)> {
    crate::slot_ops_vtables::slot_getsysattr(mcx, &*slot, attnum)
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
    exec_alloc_table_slot(estate, desc, tts_ops)
}

// ===========================================================================
//  MinimalTuple slot store/fetch/copy seams — the payload-bearing carrier
//  (`FormedMinimalTuple`) over the pool's `SlotData`. The bodies live in
//  `crate::slot_store_fetch`; these adapters resolve the `SlotId` to the live
//  `&mut SlotData` and thread `mcx`.
// ===========================================================================

/// Seam `exec_store_minimal_tuple` — `ExecStoreMinimalTuple`.
fn seam_exec_store_minimal_tuple<'mcx>(
    estate: &mut EStateData<'mcx>,
    mtup: types_tuple::backend_access_common_heaptuple::FormedMinimalTuple<'mcx>,
    slot: SlotId,
    should_free: bool,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    crate::slot_store_fetch::ExecStoreMinimalTuple(mcx, mtup, estate.slot_data_mut(slot), should_free)
}

/// Seam `exec_force_store_minimal_tuple` — `ExecForceStoreMinimalTuple`.
fn seam_exec_force_store_minimal_tuple<'mcx>(
    slot: SlotId,
    mtup: types_tuple::backend_access_common_heaptuple::FormedMinimalTuple<'mcx>,
    should_free: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    crate::slot_store_fetch::ExecForceStoreMinimalTuple(
        mcx,
        mtup,
        estate.slot_data_mut(slot),
        should_free,
    )
}

/// Seam `exec_copy_slot_minimal_tuple` — `ExecCopySlotMinimalTuple`.
fn seam_exec_copy_slot_minimal_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &EStateData<'mcx>,
    slot: SlotId,
) -> PgResult<types_tuple::backend_access_common_heaptuple::FormedMinimalTuple<'mcx>> {
    // ExecCopySlotMinimalTuple over the (immutable) source slot.
    crate::slot_ops_vtables::exec_copy_slot_minimal_tuple_ref(mcx, estate.slot_data(slot), 0)
}

/// Seam `exec_fetch_slot_minimal_tuple` — `ExecFetchSlotMinimalTuple`.
fn seam_exec_fetch_slot_minimal_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot: SlotId,
) -> PgResult<(
    types_tuple::backend_access_common_heaptuple::FormedMinimalTuple<'mcx>,
    bool,
)> {
    crate::slot_store_fetch::ExecFetchSlotMinimalTuple(mcx, estate.slot_data_mut(slot))
}

/// Seam `exec_fetch_slot_minimal_tuple_copy` — `ExecFetchSlotMinimalTuple` as
/// the boundary flat byte image (the shm_mq transport form). Materializes the
/// slot's `MinimalTuple` and serializes it to its contiguous C image.
fn seam_exec_fetch_slot_minimal_tuple_copy<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot: SlotId,
) -> PgResult<mcx::PgVec<'mcx, u8>> {
    // tuple = ExecFetchSlotMinimalTuple(slot, &shouldFree);  /* C copies tuple->t_len bytes */
    let (mtup, _should_free) =
        crate::slot_store_fetch::ExecFetchSlotMinimalTuple(mcx, estate.slot_data_mut(slot))?;
    // The flat blob is exactly the `tuple->t_len` bytes shm_mq ships. A fresh
    // materialized minimal tuple is structurally well-formed, so the only
    // possible failure is the allocation `ereport(ERROR)` (OOM).
    use backend_access_common_heaptuple::flat::MinimalTupleFlatError;
    match backend_access_common_heaptuple::flat::minimal_tuple_to_flat(mcx, &mtup) {
        Ok(blob) => Ok(blob),
        Err(MinimalTupleFlatError::Pg(err)) => Err(err),
        Err(other) => panic!("minimal_tuple_to_flat on a slot tuple failed: {other:?}"),
    }
}

/// Install every seam this unit owns.
pub fn init_seams() {
    use backend_executor_execTuples_seams as seams;
    seams::exec_alloc_table_slot::set(seam_exec_alloc_table_slot);
    seams::slot_getallattrs::set(seam_slot_getallattrs);
    seams::slot_getallattrs_by_id::set(seam_slot_getallattrs_by_id);
    seams::exec_init_result_tuple_slot_tl::set(seam_exec_init_result_tuple_slot_tl);
    seams::exec_init_result_type_tl::set(seam_exec_init_result_type_tl);
    seams::exec_clear_tuple::set(seam_exec_clear_tuple);
    seams::exec_clear_tuple_by_id::set(seam_exec_clear_tuple_by_id);
    seams::exec_reset_one_slot::set(seam_exec_reset_one_slot);
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
    // MinimalTuple payload-bearing-carrier store/fetch/copy seams (the SlotData
    // pool now carries the proper per-kind superstructure, so these resolve a
    // SlotId to its live `&mut SlotData`).
    seams::exec_store_minimal_tuple::set(seam_exec_store_minimal_tuple);
    seams::exec_force_store_minimal_tuple::set(seam_exec_force_store_minimal_tuple);
    seams::exec_copy_slot_minimal_tuple::set(seam_exec_copy_slot_minimal_tuple);
    seams::exec_fetch_slot_minimal_tuple::set(seam_exec_fetch_slot_minimal_tuple);
    seams::exec_fetch_slot_minimal_tuple_copy::set(seam_exec_fetch_slot_minimal_tuple_copy);
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
