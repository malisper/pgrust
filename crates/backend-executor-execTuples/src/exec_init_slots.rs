//! Family: slot creation / tuple table — `MakeTupleTableSlot`,
//! `ExecAllocTableSlot`, `ExecInitScanTupleSlot`, `ExecInitExtraTupleSlot`,
//! `ExecInitResultSlot`/`ExecInitResultTupleSlotTL`, `ExecInitNullTupleSlot`,
//! `ExecSetSlotDescriptor`, `ExecResetTupleTable`, `MakeSingleTupleTableSlot`,
//! `ExecDropSingleTupleTableSlot` (execTuples.c) and the tableam slot create.
//!
//! **This family owns and installs `backend-executor-execTuples-seams`** — the
//! 16 declarations the executor node units call back into. Each install target
//! mirrors the seam's exact signature and `todo!()`s its body for the scaffold.

use mcx::Mcx;
use types_core::primitive::AttrNumber;
use types_datum::Datum;
use types_error::PgResult;
use types_nodes::execnodes::{EStateData, PlanStateData, ScanStateData};
use types_nodes::tuptable::SlotData;
use types_nodes::{SlotId, TupleSlotKind, TupleTableSlot};
use types_tuple::backend_access_common_heaptuple::DeformedColumn;
use types_tuple::heaptuple::{TupleDesc, TupleDescData};

// ===========================================================================
//  Standalone-slot creation/teardown over the owned [`SlotData`] payload model.
//  These mirror `MakeSingleTupleTableSlot` / `ExecDropSingleTupleTableSlot`
//  (execTuples.c) for in-crate callers (e.g. the `begin/do/end_tup_output`
//  family) that hold the live `SlotData` directly rather than a pool id. Their
//  bodies land with this slot-creation family.
// ===========================================================================

/// `MakeSingleTupleTableSlot(tupdesc, tts_ops)` (execTuples.c): create a
/// standalone slot of the given class, fixed to `tupdesc`, allocated in `mcx`.
pub fn MakeSingleTupleTableSlot<'mcx>(
    _mcx: Mcx<'mcx>,
    _tupdesc: TupleDesc<'mcx>,
    _tts_ops: TupleSlotKind,
) -> PgResult<SlotData<'mcx>> {
    todo!("execTuples.c MakeSingleTupleTableSlot")
}

/// `ExecDropSingleTupleTableSlot(slot)` (execTuples.c): release a slot made
/// with [`MakeSingleTupleTableSlot`] (clears it first, releasing any pin).
pub fn ExecDropSingleTupleTableSlot(_slot: SlotData<'_>) -> PgResult<()> {
    todo!("execTuples.c ExecDropSingleTupleTableSlot")
}

// ===========================================================================
//  Seam install targets — signatures mirror
//  `backend-executor-execTuples-seams` exactly.
// ===========================================================================

/// Seam `slot_getallattrs` (provisional contract; see seam doc).
fn seam_slot_getallattrs<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &TupleTableSlot,
) -> PgResult<mcx::PgVec<'mcx, DeformedColumn<'mcx>>> {
    todo!("execTuples.c slot_getallattrs (seam)")
}

/// Seam `exec_init_result_tuple_slot_tl` — `ExecInitResultTupleSlotTL`.
fn seam_exec_init_result_tuple_slot_tl<'mcx>(
    _planstate: &mut PlanStateData<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _tts_ops: TupleSlotKind,
) -> PgResult<()> {
    todo!("execTuples.c ExecInitResultTupleSlotTL (seam)")
}

/// Seam `exec_clear_tuple` — `ExecClearTuple`.
fn seam_exec_clear_tuple(_slot: &mut TupleTableSlot) -> PgResult<()> {
    todo!("execTuples.c ExecClearTuple (seam)")
}

/// Seam `exec_copy_slot` — `ExecCopySlot` (provisional contract; see seam doc).
fn seam_exec_copy_slot<'mcx>(
    _mcx: Mcx<'mcx>,
    _dstslot: &mut TupleTableSlot,
    _srcslot: &TupleTableSlot,
) -> PgResult<()> {
    todo!("execTuples.c ExecCopySlot (seam)")
}

/// Seam `exec_init_result_slot` — `ExecInitResultSlot`.
fn seam_exec_init_result_slot<'mcx>(
    _planstate: &mut PlanStateData<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _tts_ops: TupleSlotKind,
) -> PgResult<()> {
    todo!("execTuples.c ExecInitResultSlot (seam)")
}

/// Seam `exec_init_scan_tuple_slot` — `ExecInitScanTupleSlot`.
fn seam_exec_init_scan_tuple_slot<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _scanstate: &mut ScanStateData<'mcx>,
    _tupledesc: TupleDesc<'mcx>,
    _tts_ops: TupleSlotKind,
) -> PgResult<()> {
    todo!("execTuples.c ExecInitScanTupleSlot (seam)")
}

/// Seam `exec_init_extra_tuple_slot` — `ExecInitExtraTupleSlot`.
fn seam_exec_init_extra_tuple_slot<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _tupledesc: TupleDesc<'mcx>,
    _tts_ops: TupleSlotKind,
) -> PgResult<SlotId> {
    todo!("execTuples.c ExecInitExtraTupleSlot (seam)")
}

/// Seam `exec_set_slot_descriptor` — `ExecSetSlotDescriptor`.
fn seam_exec_set_slot_descriptor<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _slot: SlotId,
    _tupdesc: TupleDesc<'mcx>,
) -> PgResult<()> {
    todo!("execTuples.c ExecSetSlotDescriptor (seam)")
}

/// Seam `exec_store_all_null_tuple` — `ExecStoreAllNullTuple`.
fn seam_exec_store_all_null_tuple<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _slot: SlotId,
) -> PgResult<()> {
    todo!("execTuples.c ExecStoreAllNullTuple (seam)")
}

/// Seam `make_single_tuple_table_slot` — `MakeSingleTupleTableSlot`.
fn seam_make_single_tuple_table_slot<'mcx>(
    _mcx: Mcx<'mcx>,
    _tupdesc: TupleDesc<'mcx>,
    _tts_ops: TupleSlotKind,
) -> PgResult<TupleTableSlot> {
    todo!("execTuples.c MakeSingleTupleTableSlot (seam)")
}

/// Seam `exec_drop_single_tuple_table_slot` — `ExecDropSingleTupleTableSlot`.
fn seam_exec_drop_single_tuple_table_slot(_slot: TupleTableSlot) -> PgResult<()> {
    todo!("execTuples.c ExecDropSingleTupleTableSlot (seam)")
}

/// Seam `slot_getsysattr` — `slot_getsysattr`.
fn seam_slot_getsysattr(
    _slot: &TupleTableSlot,
    _attnum: AttrNumber,
) -> PgResult<(Datum, bool)> {
    todo!("execTuples.c slot_getsysattr (seam)")
}

/// Seam `exec_init_null_tuple_slot` — `ExecInitNullTupleSlot`.
fn seam_exec_init_null_tuple_slot<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _tupledesc: TupleDesc<'mcx>,
    _tts_ops: TupleSlotKind,
) -> PgResult<SlotId> {
    todo!("execTuples.c ExecInitNullTupleSlot (seam)")
}

/// Seam `exec_get_result_type` — `ExecGetResultType` (execUtils.c surface,
/// owned here for the result-slot family).
fn seam_exec_get_result_type<'a, 'mcx>(
    _planstate: &'a PlanStateData<'mcx>,
) -> Option<&'a TupleDescData<'mcx>> {
    todo!("ExecGetResultType (seam)")
}

/// Seam `exec_get_result_slot_ops` — `ExecGetResultSlotOps`.
fn seam_exec_get_result_slot_ops<'mcx>(_planstate: &PlanStateData<'mcx>) -> TupleSlotKind {
    todo!("ExecGetResultSlotOps (seam)")
}

/// Install every seam this unit owns.
pub fn init_seams() {
    use backend_executor_execTuples_seams as seams;
    seams::slot_getallattrs::set(seam_slot_getallattrs);
    seams::exec_init_result_tuple_slot_tl::set(seam_exec_init_result_tuple_slot_tl);
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
}
