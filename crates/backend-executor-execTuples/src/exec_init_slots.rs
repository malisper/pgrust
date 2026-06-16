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

/// Seam `exec_store_buffer_heap_tuple` — `ExecStoreBufferHeapTuple`. The slot
/// crosses as the payload-bearing `&mut SlotData` (the heap-scan vtable callback
/// holds it directly, not as a pool `SlotId`), so this adapter forwards straight
/// to the owner body, which performs the buffer-pin management.
fn seam_exec_store_buffer_heap_tuple<'mcx>(
    tuple: types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
    slot: &mut types_nodes::tuptable::SlotData<'mcx>,
    buffer: types_storage::buf::Buffer,
) -> PgResult<()> {
    crate::slot_store_fetch::ExecStoreBufferHeapTuple(tuple, slot, buffer)
}

/// Seam `exec_store_pinned_buffer_heap_tuple` — `ExecStorePinnedBufferHeapTuple`.
/// Like the buffer store but transfers an existing pin (the `heap_fetch`
/// `userbuf` in `heapam_fetch_row_version`). Forwards to the owner body.
fn seam_exec_store_pinned_buffer_heap_tuple<'mcx>(
    tuple: types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
    slot: &mut types_nodes::tuptable::SlotData<'mcx>,
    buffer: types_storage::buf::Buffer,
) -> PgResult<()> {
    crate::slot_store_fetch::ExecStorePinnedBufferHeapTuple(tuple, slot, buffer)
}

/// Seam `exec_clear_tuple_payload` — `ExecClearTuple(slot)` over the
/// payload-bearing `&mut SlotData` the heap-scan vtable holds directly.
fn seam_exec_clear_tuple_payload<'mcx>(
    slot: &mut types_nodes::tuptable::SlotData<'mcx>,
) -> PgResult<()> {
    crate::slot_store_fetch::ExecClearTuple(slot)
}

/// Seam `exec_fetch_slot_heap_tuple` — `ExecFetchSlotHeapTuple(slot,
/// materialize, &shouldFree)` over the payload-bearing `&mut SlotData` the
/// table-AM DML vtable callbacks hold directly. Forwards to the owner body,
/// which dispatches on the slot kind.
fn seam_exec_fetch_slot_heap_tuple<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    slot: &mut types_nodes::tuptable::SlotData<'mcx>,
    materialize: bool,
) -> PgResult<(
    types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
    bool,
)> {
    crate::slot_store_fetch::ExecFetchSlotHeapTuple(mcx, slot, materialize)
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

/// Seam `exec_copy_slot_minimal_tuple_extra` — `ExecCopySlotMinimalTupleExtra`.
fn seam_exec_copy_slot_minimal_tuple_extra<'mcx>(
    estate: &mut EStateData<'mcx>,
    slot: SlotId,
    extra: usize,
) -> PgResult<types_tuple::backend_access_common_heaptuple::FormedMinimalTuple<'mcx>> {
    let mcx = estate.es_query_cxt;
    crate::slot_ops_vtables::exec_copy_slot_minimal_tuple_ref(mcx, estate.slot_data(slot), extra)
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

// ===========================================================================
//  Slot-payload op seams over the pool's `SlotData` (the body already exists in
//  `slot_store_fetch` / `slot_deform`; these adapters resolve the `SlotId` to
//  the live `&mut SlotData` and thread `mcx`).
// ===========================================================================

use types_tuple::backend_access_common_heaptuple::{Datum, FormedTuple};

/// Seam `exec_materialize_slot` — `ExecMaterializeSlot`.
fn seam_exec_materialize_slot<'mcx>(estate: &mut EStateData<'mcx>, slot: SlotId) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    crate::slot_store_fetch::ExecMaterializeSlot(mcx, estate.slot_data_mut(slot))
}

/// Seam `exec_store_virtual_tuple` — `ExecStoreVirtualTuple`.
fn seam_exec_store_virtual_tuple<'mcx>(estate: &mut EStateData<'mcx>, slot: SlotId) -> PgResult<()> {
    crate::slot_store_fetch::ExecStoreVirtualTuple(estate.slot_data_mut(slot))
}

/// Seam `exec_store_first_datum` — `ExecClearTuple(slot); slot->tts_values[0] =
/// val; slot->tts_isnull[0] = isnull; ExecStoreVirtualTuple(slot)`
/// (nodeSort's Datum-sort output path).
fn seam_exec_store_first_datum<'mcx>(
    estate: &mut EStateData<'mcx>,
    slot: SlotId,
    val: Datum<'mcx>,
    is_null: bool,
) -> PgResult<()> {
    let slot_data = estate.slot_data_mut(slot);
    // ExecClearTuple(slot);
    crate::slot_store_fetch::ExecClearTuple(slot_data)?;
    // slot->tts_values[0] = val; slot->tts_isnull[0] = isnull;
    let base = slot_data.base_mut();
    base.tts_values[0] = val;
    base.tts_isnull[0] = is_null;
    // ExecStoreVirtualTuple(slot);
    crate::slot_store_fetch::ExecStoreVirtualTuple(slot_data)
}

/// Seam `store_virtual_values` — fill a virtual slot's per-column payload with
/// `values`/`isnull` (the N-column analogue of `exec_store_first_datum`):
/// `ExecClearTuple(slot); memcpy(tts_values, values); memcpy(tts_isnull,
/// isnull); ExecStoreVirtualTuple(slot)`.
fn seam_store_virtual_values<'mcx>(
    estate: &mut EStateData<'mcx>,
    slot: SlotId,
    values: &[Datum<'mcx>],
    isnull: &[bool],
) -> PgResult<()> {
    let slot_data = estate.slot_data_mut(slot);
    // ExecClearTuple(slot);
    crate::slot_store_fetch::ExecClearTuple(slot_data)?;
    // memcpy(slot->tts_values, values, natts * sizeof(Datum));
    // memcpy(slot->tts_isnull, isnull, natts * sizeof(bool));
    let base = slot_data.base_mut();
    for (i, v) in values.iter().enumerate() {
        base.tts_values[i] = v.clone();
    }
    for (i, n) in isnull.iter().enumerate() {
        base.tts_isnull[i] = *n;
    }
    // ExecStoreVirtualTuple(slot);
    crate::slot_store_fetch::ExecStoreVirtualTuple(slot_data)
}

/// Seam `exec_copy_slot_heap_tuple` — `ExecCopySlotHeapTuple` (returns the
/// owned [`FormedTuple`] carrier copied in the per-query context).
fn seam_exec_copy_slot_heap_tuple<'mcx>(
    estate: &mut EStateData<'mcx>,
    slot: SlotId,
) -> PgResult<FormedTuple<'mcx>> {
    let mcx = estate.es_query_cxt;
    crate::slot_store_fetch::ExecCopySlotHeapTuple(mcx, estate.slot_data_mut(slot))
}

/// Seam `slot_getattr_by_id` — `slot_getattr` resolving the pool `SlotId` to its
/// live payload-bearing `&mut SlotData`.
fn seam_slot_getattr_by_id<'mcx>(
    estate: &mut EStateData<'mcx>,
    slot: SlotId,
    attnum: AttrNumber,
) -> PgResult<backend_executor_execTuples_seams::SlotAttr<'mcx>> {
    let mcx = estate.es_query_cxt;
    let (value, isnull) = crate::slot_deform::slot_getattr(mcx, estate.slot_data_mut(slot), attnum)?;
    Ok(backend_executor_execTuples_seams::SlotAttr { value, isnull })
}

/// Seam `slot_getattr` (SlotId form) — `slot_getattr` resolving the pool
/// `SlotId` to its live payload-bearing `&mut SlotData`, returning the bare
/// `(value, isnull)` pair.
fn seam_slot_getattr<'mcx>(
    estate: &mut EStateData<'mcx>,
    slot: SlotId,
    attnum: AttrNumber,
) -> PgResult<(Datum<'mcx>, bool)> {
    let mcx = estate.es_query_cxt;
    crate::slot_deform::slot_getattr(mcx, estate.slot_data_mut(slot), attnum)
}

/// Seam `slot_getsomeattr` — `slot_getsomeattrs(slot, attnum)` then
/// `(slot->tts_values[attnum-1], slot->tts_isnull[attnum-1])`, resolving the
/// pool `SlotId` to its live `&mut SlotData`.
fn seam_slot_getsomeattr<'mcx>(
    estate: &mut EStateData<'mcx>,
    slot: SlotId,
    attnum: i32,
) -> PgResult<(Datum<'mcx>, bool)> {
    let mcx = estate.es_query_cxt;
    let slot_data = estate.slot_data_mut(slot);
    // slot_getsomeattrs(slot, attnum);
    crate::slot_deform::slot_getsomeattrs(mcx, slot_data, attnum)?;
    // return (slot->tts_values[attnum - 1], slot->tts_isnull[attnum - 1]);
    let base = slot_data.base();
    let value = base.tts_values[(attnum - 1) as usize].clone_in(mcx)?;
    let isnull = base.tts_isnull[(attnum - 1) as usize];
    Ok((value, isnull))
}

/// Seam `slot_natts` — `slot->tts_tupleDescriptor->natts` of the pool slot.
fn seam_slot_natts(estate: &EStateData<'_>, slot: SlotId) -> i32 {
    estate
        .slot_data(slot)
        .base()
        .tts_tupleDescriptor
        .as_ref()
        .map(|d| d.natts)
        .unwrap_or(0)
}

/// Seam `exec_scan_slot_descriptor` — `scanstate->ss_ScanTupleSlot->
/// tts_tupleDescriptor`: the scan slot's tuple descriptor, copied into `mcx`
/// (C reads the shared pointer).
fn seam_exec_scan_slot_descriptor<'mcx>(
    mcx: Mcx<'mcx>,
    scanstate: &ScanStateData<'mcx>,
    estate: &EStateData<'mcx>,
) -> PgResult<TupleDesc<'mcx>> {
    // scanstate->ss_ScanTupleSlot->tts_tupleDescriptor
    let slot = scanstate
        .ss_ScanTupleSlot
        .expect("exec_scan_slot_descriptor: scan slot not initialized");
    match estate.slot_data(slot).base().tts_tupleDescriptor.as_deref() {
        Some(d) => Ok(Some(mcx::alloc_in(mcx, d.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

/// Seam `execute_attr_map_slot_explicit` — `execute_attr_map_slot(attrMap,
/// in_slot, out_slot)` (tupconvert.c) with an explicitly-supplied `attr_map`.
///
/// ```c
/// outnatts = out_slot->tts_tupleDescriptor->natts;
/// slot_getallattrs(in_slot);
/// ExecClearTuple(out_slot);
/// for (i = 0; i < outnatts; i++) {
///     int j = attrMap->attnums[i] - 1;
///     if (j == -1) { outvalues[i] = 0; outisnull[i] = true; }
///     else { outvalues[i] = invalues[j]; outisnull[i] = inisnull[j]; }
/// }
/// ExecStoreVirtualTuple(out_slot);
/// ```
fn seam_execute_attr_map_slot_explicit<'mcx>(
    estate: &mut EStateData<'mcx>,
    attr_map: &types_tuple::attmap::AttrMap<'mcx>,
    in_slot: SlotId,
    out_slot: SlotId,
) -> PgResult<SlotId> {
    let mcx = estate.es_query_cxt;
    // slot_getallattrs(in_slot): deform every input column.
    {
        let in_data = estate.slot_data_mut(in_slot);
        crate::slot_deform::slot_getallattrs(mcx, in_data)?;
    }
    // outnatts = out_slot->tts_tupleDescriptor->natts;
    let outnatts = estate
        .slot_data(out_slot)
        .base()
        .tts_tupleDescriptor
        .as_ref()
        .map(|d| d.natts)
        .unwrap_or(0);
    // Snapshot the input (value, isnull) columns before borrowing the out slot
    // (the two pool entries are distinct; copy out the source values C reads via
    // `invalues = in_slot->tts_values`).
    let in_cols: Vec<(Datum<'mcx>, bool)> = {
        let base = estate.slot_data(in_slot).base();
        (0..base.tts_values.len())
            .map(|j| (base.tts_values[j].clone(), base.tts_isnull[j]))
            .collect()
    };
    // ExecClearTuple(out_slot);
    {
        let out_data = estate.slot_data_mut(out_slot);
        crate::slot_store_fetch::ExecClearTuple(out_data)?;
        // Transpose into proper fields of the out slot.
        let base = out_data.base_mut();
        for i in 0..outnatts as usize {
            // int j = attrMap->attnums[i] - 1;
            let j = attr_map.attnums[i] as i32 - 1;
            if j == -1 {
                // attrMap->attnums[i] == 0 means it's a NULL datum.
                base.tts_values[i] = Datum::null();
                base.tts_isnull[i] = true;
            } else {
                base.tts_values[i] = in_cols[j as usize].0.clone();
                base.tts_isnull[i] = in_cols[j as usize].1;
            }
        }
        // ExecStoreVirtualTuple(out_slot);
        crate::slot_store_fetch::ExecStoreVirtualTuple(out_data)?;
    }
    // return out_slot;
    Ok(out_slot)
}

/// `execute_attr_map_slot(attrMap, in_slot, out_slot)` (tupconvert.c) with the
/// conversion map read off the source `ResultRelInfo`'s `ri_ChildToRootMap`
/// (the `RriId` form `ExecCrossPartitionUpdate` uses after
/// `ExecGetChildToRootMap` has computed it). The transpose itself is identical
/// to [`seam_execute_attr_map_slot_explicit`]; this wrapper just resolves the
/// map from the pooled `ResultRelInfo` before applying it.
fn seam_execute_attr_map_slot<'mcx>(
    estate: &mut EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    in_slot: SlotId,
    out_slot: SlotId,
) -> PgResult<SlotId> {
    // The map lives on the pooled ResultRelInfo (ri_ChildToRootMap); take an
    // owned copy of its AttrMap so the estate can be re-borrowed mutably by the
    // transpose (mirrors execPartition's clone_attrmap dance).
    let mcx = estate.es_query_cxt;
    let attr_map = {
        let map = estate
            .result_rel(result_rel_info)
            .ri_ChildToRootMap
            .as_ref()
            .expect("execute_attr_map_slot: ri_ChildToRootMap is NULL (caller must run ExecGetChildToRootMap first)");
        types_tuple::attmap::AttrMap {
            attnums: mcx::slice_in(mcx, &map.attrMap.attnums)?,
        }
    };
    seam_execute_attr_map_slot_explicit(estate, &attr_map, in_slot, out_slot)
}

/// Slot-payload op for `StoreIndexTuple`'s name-cstring fix-up
/// (nodeIndexonlyscan.c): for each attribute index in `attnums` whose slot
/// value is non-null, read the cstring `Datum` and copy it into a
/// NAMEDATALEN-byte zero-padded `Name` (`namestrcpy`), storing the resulting
/// `Name` datum back.
///
/// C allocates the `Name` in `ps_ExprContext->ecxt_per_tuple_memory`; the owned
/// value model carries the by-reference bytes inline in the slot's `Datum`
/// (allocated in the slot pool's `'mcx`), so `per_tuple_ecxt` is not used as an
/// allocation target here — the lifetime is governed by the carrier, not a raw
/// context pointer.
fn seam_pad_name_cstring_columns<'mcx>(
    estate: &mut EStateData<'mcx>,
    slot: SlotId,
    _per_tuple_ecxt: types_nodes::EcxtId,
    attnums: &[AttrNumber],
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let base = estate.slot_data_mut(slot).base_mut();
    for &attnum in attnums {
        let idx = attnum as usize;
        // skip null Datums
        if base.tts_isnull[idx] {
            continue;
        }
        // namestrcpy(name, DatumGetCString(slot->tts_values[attnum])):
        // the cstring Datum is the by-reference image (NUL-terminated bytes);
        // copy its NameStr into a NAMEDATALEN-byte zero-padded buffer.
        let cstr_bytes = base.tts_values[idx].as_ref_bytes();
        // bytes up to (but not including) the first NUL — DatumGetCString.
        let end = cstr_bytes
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(cstr_bytes.len());
        let s = String::from_utf8_lossy(&cstr_bytes[..end]);
        let mut name = types_tuple::heaptuple::NameData::default();
        name.namestrcpy(&s);
        // slot->tts_values[attnum] = NameGetDatum(name): a fixed-length
        // pass-by-reference Name is the NAMEDATALEN-byte image carried as a
        // by-reference Datum.
        let name_datum = types_tuple::backend_access_common_heaptuple::Datum::ByRef(
            mcx::slice_in(mcx, &name.data)?,
        );
        base.tts_values[idx] = name_datum;
    }
    Ok(())
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
    seams::exec_store_buffer_heap_tuple::set(seam_exec_store_buffer_heap_tuple);
    seams::exec_store_pinned_buffer_heap_tuple::set(seam_exec_store_pinned_buffer_heap_tuple);
    seams::exec_clear_tuple_payload::set(seam_exec_clear_tuple_payload);
    seams::exec_fetch_slot_heap_tuple::set(seam_exec_fetch_slot_heap_tuple);
    seams::exec_force_store_minimal_tuple::set(seam_exec_force_store_minimal_tuple);
    seams::exec_copy_slot_minimal_tuple::set(seam_exec_copy_slot_minimal_tuple);
    seams::exec_copy_slot_minimal_tuple_extra::set(seam_exec_copy_slot_minimal_tuple_extra);
    seams::exec_fetch_slot_minimal_tuple::set(seam_exec_fetch_slot_minimal_tuple);
    seams::exec_fetch_slot_minimal_tuple_copy::set(seam_exec_fetch_slot_minimal_tuple_copy);
    // ExecTypeFromTL is fully owned + implemented in this crate's
    // exectype_tupoutput family (no pool-payload / Plan-model dependency), so
    // its seam is installed here.
    seams::exec_type_from_tl::set(crate::exectype_tupoutput::ExecTypeFromTL);
    // ExecTypeFromExprList is likewise fully owned + implemented in this crate's
    // exectype_tupoutput family, so its seam is installed here.
    seams::exec_type_from_expr_list::set(crate::exectype_tupoutput::ExecTypeFromExprList);
    // Slot-payload op seams over the pool's `SlotData` (bodies in
    // `slot_store_fetch` / `slot_deform`; the adapters resolve the `SlotId`).
    seams::exec_materialize_slot::set(seam_exec_materialize_slot);
    seams::exec_store_virtual_tuple::set(seam_exec_store_virtual_tuple);
    seams::exec_store_first_datum::set(seam_exec_store_first_datum);
    seams::store_virtual_values::set(seam_store_virtual_values);
    seams::exec_copy_slot_heap_tuple::set(seam_exec_copy_slot_heap_tuple);
    seams::slot_getattr_by_id::set(seam_slot_getattr_by_id);
    seams::slot_getattr::set(seam_slot_getattr);
    seams::slot_getsomeattr::set(seam_slot_getsomeattr);
    seams::slot_natts::set(seam_slot_natts);
    seams::exec_scan_slot_descriptor::set(seam_exec_scan_slot_descriptor);
    seams::execute_attr_map_slot_explicit::set(seam_execute_attr_map_slot_explicit);
    // RriId form: reads the conversion map off the ResultRelInfo's
    // ri_ChildToRootMap (ExecCrossPartitionUpdate path).
    seams::execute_attr_map_slot::set(seam_execute_attr_map_slot);
    // Slot-payload name-cstring fix-up (index-only scan over a name column).
    seams::pad_name_cstring_columns::set(seam_pad_name_cstring_columns);
}
