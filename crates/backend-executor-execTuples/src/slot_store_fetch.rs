//! Family: store / fetch — the `ExecStore*Tuple` storers, the `ExecFetchSlot*`
//! accessors, `ExecClearTuple`, and `ExecCopySlot*` (execTuples.c).

use mcx::Mcx;
use types_core::primitive::Size;
use types_datum::Datum;
use types_error::PgResult;
use types_nodes::tuptable::SlotData;
use types_storage::buf::Buffer;
use types_tuple::heaptuple::{HeapTuple, MinimalTuple};

/// `ExecStoreHeapTuple(tuple, slot, shouldFree)` (execTuples.c).
pub fn ExecStoreHeapTuple<'mcx>(
    _tuple: HeapTuple<'mcx>,
    _slot: &mut SlotData<'mcx>,
    _should_free: bool,
) -> PgResult<()> {
    todo!("execTuples.c ExecStoreHeapTuple")
}

/// `ExecStoreBufferHeapTuple(tuple, slot, buffer)` (execTuples.c).
pub fn ExecStoreBufferHeapTuple<'mcx>(
    _tuple: HeapTuple<'mcx>,
    _slot: &mut SlotData<'mcx>,
    _buffer: Buffer,
) -> PgResult<()> {
    todo!("execTuples.c ExecStoreBufferHeapTuple")
}

/// `ExecStorePinnedBufferHeapTuple(tuple, slot, buffer)` (execTuples.c).
pub fn ExecStorePinnedBufferHeapTuple<'mcx>(
    _tuple: HeapTuple<'mcx>,
    _slot: &mut SlotData<'mcx>,
    _buffer: Buffer,
) -> PgResult<()> {
    todo!("execTuples.c ExecStorePinnedBufferHeapTuple")
}

/// `ExecStoreMinimalTuple(mtup, slot, shouldFree)` (execTuples.c).
pub fn ExecStoreMinimalTuple<'mcx>(
    _mtup: MinimalTuple<'mcx>,
    _slot: &mut SlotData<'mcx>,
    _should_free: bool,
) -> PgResult<()> {
    todo!("execTuples.c ExecStoreMinimalTuple")
}

/// `ExecForceStoreHeapTuple(tuple, slot, shouldFree)` (execTuples.c).
pub fn ExecForceStoreHeapTuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _tuple: HeapTuple<'mcx>,
    _slot: &mut SlotData<'mcx>,
    _should_free: bool,
) -> PgResult<()> {
    todo!("execTuples.c ExecForceStoreHeapTuple")
}

/// `ExecForceStoreMinimalTuple(mtup, slot, shouldFree)` (execTuples.c).
pub fn ExecForceStoreMinimalTuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _mtup: MinimalTuple<'mcx>,
    _slot: &mut SlotData<'mcx>,
    _should_free: bool,
) -> PgResult<()> {
    todo!("execTuples.c ExecForceStoreMinimalTuple")
}

/// `ExecStoreVirtualTuple(slot)` (execTuples.c): mark a slot holding
/// already-filled `tts_values`/`tts_isnull` as a valid virtual tuple.
pub fn ExecStoreVirtualTuple(_slot: &mut SlotData) -> PgResult<()> {
    todo!("execTuples.c ExecStoreVirtualTuple")
}

/// `ExecStoreAllNullTuple(slot)` (execTuples.c): set all attributes NULL and
/// mark the slot a valid virtual tuple.
pub fn ExecStoreAllNullTuple<'mcx>(_mcx: Mcx<'mcx>, _slot: &mut SlotData<'mcx>) -> PgResult<()> {
    todo!("execTuples.c ExecStoreAllNullTuple")
}

/// `ExecStoreHeapTupleDatum(data, slot)` (execTuples.c): deform a composite
/// `Datum` into the slot as a virtual tuple.
pub fn ExecStoreHeapTupleDatum<'mcx>(
    _mcx: Mcx<'mcx>,
    _data: Datum,
    _slot: &mut SlotData<'mcx>,
) -> PgResult<()> {
    todo!("execTuples.c ExecStoreHeapTupleDatum")
}

/// `ExecFetchSlotHeapTuple(slot, materialize, &shouldFree)` (execTuples.c):
/// the slot's heap tuple, materializing if requested; returns `(tuple,
/// shouldFree)`.
pub fn ExecFetchSlotHeapTuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut SlotData<'mcx>,
    _materialize: bool,
) -> PgResult<(HeapTuple<'mcx>, bool)> {
    todo!("execTuples.c ExecFetchSlotHeapTuple")
}

/// `ExecFetchSlotMinimalTuple(slot, &shouldFree)` (execTuples.c).
pub fn ExecFetchSlotMinimalTuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut SlotData<'mcx>,
) -> PgResult<(MinimalTuple<'mcx>, bool)> {
    todo!("execTuples.c ExecFetchSlotMinimalTuple")
}

/// `ExecFetchSlotHeapTupleDatum(slot)` (execTuples.c): the slot's contents as
/// a composite `Datum`.
pub fn ExecFetchSlotHeapTupleDatum<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut SlotData<'mcx>,
) -> PgResult<Datum> {
    todo!("execTuples.c ExecFetchSlotHeapTupleDatum")
}

/// `ExecMaterializeSlot(slot)` (tuptable.h inline): force the slot's contents
/// to depend solely on the slot (`slot->tts_ops->materialize`).
pub fn ExecMaterializeSlot<'mcx>(_mcx: Mcx<'mcx>, _slot: &mut SlotData<'mcx>) -> PgResult<()> {
    todo!("execTuples.c ExecMaterializeSlot")
}

/// `ExecClearTuple(slot)` (tuptable.h inline): clear the slot's contents
/// (`slot->tts_ops->clear`).
pub fn ExecClearTuple(_slot: &mut SlotData) -> PgResult<()> {
    todo!("execTuples.c ExecClearTuple")
}

/// `ExecCopySlot(dstslot, srcslot)` (tuptable.h inline): copy the source
/// slot's tuple into the destination (`dstslot->tts_ops->copyslot`).
pub fn ExecCopySlot<'mcx>(
    _mcx: Mcx<'mcx>,
    _dstslot: &mut SlotData<'mcx>,
    _srcslot: &SlotData<'mcx>,
) -> PgResult<()> {
    todo!("execTuples.c ExecCopySlot")
}

/// `ExecCopySlotHeapTuple(slot)` (tuptable.h inline): a heap tuple copy owned
/// by the caller (`slot->tts_ops->copy_heap_tuple`).
pub fn ExecCopySlotHeapTuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut SlotData<'mcx>,
) -> PgResult<HeapTuple<'mcx>> {
    todo!("execTuples.c ExecCopySlotHeapTuple")
}

/// `ExecCopySlotMinimalTupleExtra(slot, extra)` (tuptable.h inline): a minimal
/// tuple copy with `extra` leading bytes reserved.
pub fn ExecCopySlotMinimalTupleExtra<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut SlotData<'mcx>,
    _extra: Size,
) -> PgResult<MinimalTuple<'mcx>> {
    todo!("execTuples.c ExecCopySlotMinimalTupleExtra")
}
