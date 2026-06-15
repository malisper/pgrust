//! Family: store / fetch — the `ExecStore*Tuple` storers, the `ExecFetchSlot*`
//! accessors, `ExecClearTuple`, and `ExecCopySlot*` (execTuples.c).
//!
//! These are thin wrappers over the per-kind `TupleTableSlotOps` callbacks
//! owned by [`crate::slot_ops_vtables`] (the C `slot->tts_ops->op(slot)`
//! dispatch) plus the `slot_deform` engine. The `ExecStore*Tuple` storers first
//! verify the slot's kind (the C `TTS_IS_*` `elog(ERROR)` guards) then route to
//! the matching `tts_*_store_tuple`; the `ExecFetch*`/`ExecCopy*` accessors are
//! the `slot->tts_ops->{get,copy}_{heap,minimal}_tuple` / `materialize` /
//! `clear` / `copyslot` dispatch.

use mcx::Mcx;
use types_core::primitive::Size;
use types_error::{PgError, PgResult};
use types_nodes::tuptable::{SlotData, TTS_FLAG_SHOULDFREE};
use types_nodes::TupleSlotKind;
use types_storage::buf::{Buffer, BufferIsValid};
// The canonical value enum.
use types_tuple::backend_access_common_heaptuple::{Datum, FormedMinimalTuple, FormedTuple};
use types_tuple::heaptuple::{TupleDesc, MINIMAL_TUPLE_OFFSET};

use crate::slot_ops_vtables::{
    slot_clear, slot_copyslot, slot_materialize, slot_release, tts_buffer_heap_copy_heap_tuple,
    tts_buffer_heap_copy_minimal_tuple, tts_buffer_heap_get_heap_tuple, tts_heap_copy_heap_tuple,
    tts_heap_copy_minimal_tuple, tts_heap_get_heap_tuple, tts_heap_store_tuple,
    tts_minimal_copy_heap_tuple, tts_minimal_copy_minimal_tuple, tts_minimal_get_minimal_tuple,
    tts_minimal_store_tuple, tts_virtual_copy_heap_tuple, tts_virtual_copy_minimal_tuple,
};

/// `ExecStoreHeapTuple(tuple, slot, shouldFree)` (execTuples.c).
pub fn ExecStoreHeapTuple<'mcx>(
    tuple: FormedTuple<'mcx>,
    slot: &mut SlotData<'mcx>,
    should_free: bool,
) -> PgResult<()> {
    // sanity checks: Assert(tuple != NULL); Assert(slot != NULL);
    // Assert(slot->tts_tupleDescriptor != NULL); -- unrepresentable here.

    // if (unlikely(!TTS_IS_HEAPTUPLE(slot)))
    //     elog(ERROR, "trying to store a heap tuple into wrong type of slot");
    let SlotData::Heap(hslot) = slot else {
        return Err(PgError::error(
            "trying to store a heap tuple into wrong type of slot",
        ));
    };

    // slot->tts_tableOid = tuple->t_tableOid; (read before the move).
    let t_table_oid = tuple.tuple.t_tableOid;

    // tts_heap_store_tuple(slot, tuple, shouldFree);
    tts_heap_store_tuple(hslot, tuple, should_free);

    slot.base_mut().header.tts_tableOid = t_table_oid;

    Ok(())
}

/// `ExecStoreBufferHeapTuple(tuple, slot, buffer)` (execTuples.c). The on-disk
/// heap tuple stored into a buffer slot is the body-bearing [`FormedTuple`]
/// (its `t_data` lives in the pinned page), matching the slot's carrier.
pub fn ExecStoreBufferHeapTuple<'mcx>(
    tuple: FormedTuple<'mcx>,
    slot: &mut SlotData<'mcx>,
    buffer: Buffer,
) -> PgResult<()> {
    // Assert(BufferIsValid(buffer));
    debug_assert!(BufferIsValid(buffer));

    // if (unlikely(!TTS_IS_BUFFERTUPLE(slot)))
    //     elog(ERROR, "trying to store an on-disk heap tuple into wrong type of slot");
    let SlotData::BufferHeap(bslot) = slot else {
        return Err(PgError::error(
            "trying to store an on-disk heap tuple into wrong type of slot",
        ));
    };

    // slot->tts_tableOid = tuple->t_tableOid; (read before the move).
    let t_table_oid = tuple.tuple.t_tableOid;

    // tts_buffer_heap_store_tuple(slot, tuple, buffer, false);
    tts_buffer_heap_store_tuple(bslot, tuple, buffer, false);

    slot.base_mut().header.tts_tableOid = t_table_oid;

    Ok(())
}

/// `ExecStorePinnedBufferHeapTuple(tuple, slot, buffer)` (execTuples.c).
pub fn ExecStorePinnedBufferHeapTuple<'mcx>(
    tuple: FormedTuple<'mcx>,
    slot: &mut SlotData<'mcx>,
    buffer: Buffer,
) -> PgResult<()> {
    // Assert(BufferIsValid(buffer));
    debug_assert!(BufferIsValid(buffer));

    // if (unlikely(!TTS_IS_BUFFERTUPLE(slot)))
    //     elog(ERROR, "trying to store an on-disk heap tuple into wrong type of slot");
    let SlotData::BufferHeap(bslot) = slot else {
        return Err(PgError::error(
            "trying to store an on-disk heap tuple into wrong type of slot",
        ));
    };

    // slot->tts_tableOid = tuple->t_tableOid; (read before the move).
    let t_table_oid = tuple.tuple.t_tableOid;

    // tts_buffer_heap_store_tuple(slot, tuple, buffer, true);
    tts_buffer_heap_store_tuple(bslot, tuple, buffer, true);

    slot.base_mut().header.tts_tableOid = t_table_oid;

    Ok(())
}

/// `ExecStoreMinimalTuple(mtup, slot, shouldFree)` (execTuples.c).
pub fn ExecStoreMinimalTuple<'mcx>(
    mcx: Mcx<'mcx>,
    mtup: FormedMinimalTuple<'mcx>,
    slot: &mut SlotData<'mcx>,
    should_free: bool,
) -> PgResult<()> {
    // if (unlikely(!TTS_IS_MINIMALTUPLE(slot)))
    //     elog(ERROR, "trying to store a minimal tuple into wrong type of slot");
    let SlotData::Minimal(mslot) = slot else {
        return Err(PgError::error(
            "trying to store a minimal tuple into wrong type of slot",
        ));
    };

    // tts_minimal_store_tuple(slot, mtup, shouldFree);
    tts_minimal_store_tuple(mcx, mslot, mtup, should_free)
}

/// `ExecForceStoreHeapTuple(tuple, slot, shouldFree)` (execTuples.c): store a
/// `HeapTuple` into any kind of slot, performing conversion if necessary.
pub fn ExecForceStoreHeapTuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: FormedTuple<'mcx>,
    slot: &mut SlotData<'mcx>,
    should_free: bool,
) -> PgResult<()> {
    match slot {
        // if (TTS_IS_HEAPTUPLE(slot)) ExecStoreHeapTuple(tuple, slot, shouldFree);
        SlotData::Heap(_) => {
            ExecStoreHeapTuple(tuple, slot, should_free)?;
        }
        // else if (TTS_IS_BUFFERTUPLE(slot)) { ... heap_copytuple ... }
        SlotData::BufferHeap(_) => {
            // ExecClearTuple(slot);
            slot_clear(slot);
            // slot->tts_flags &= ~TTS_FLAG_EMPTY;
            slot.base_mut().mark_not_empty();
            // oldContext = MemoryContextSwitchTo(slot->tts_mcxt);
            // bslot->base.tuple = heap_copytuple(tuple);
            let copied = backend_access_common_heaptuple::heap_copytuple(mcx, Some(&tuple))?;
            if let SlotData::BufferHeap(bslot) = slot {
                bslot.base.tuple = copied;
            }
            // slot->tts_flags |= TTS_FLAG_SHOULDFREE;
            slot.base_mut().header.tts_flags |= TTS_FLAG_SHOULDFREE;
            // MemoryContextSwitchTo(oldContext);
            // if (shouldFree) pfree(tuple); -- the owned model drops `tuple`.
            let _ = should_free;
        }
        // else { ExecClearTuple; heap_deform_tuple into slot; ExecStoreVirtualTuple; ... }
        SlotData::Virtual(_) | SlotData::Minimal(_) => {
            // ExecClearTuple(slot);
            slot_clear(slot);
            // heap_deform_tuple(tuple, slot->tts_tupleDescriptor,
            //                   slot->tts_values, slot->tts_isnull);
            deform_into_slot(mcx, slot, &tuple)?;
            // ExecStoreVirtualTuple(slot);
            ExecStoreVirtualTuple(slot)?;
            // if (shouldFree) { ExecMaterializeSlot(slot); pfree(tuple); }
            if should_free {
                ExecMaterializeSlot(mcx, slot)?;
            }
        }
    }
    Ok(())
}

/// `ExecForceStoreMinimalTuple(mtup, slot, shouldFree)` (execTuples.c): store a
/// `MinimalTuple` into any kind of slot, performing conversion if necessary.
pub fn ExecForceStoreMinimalTuple<'mcx>(
    mcx: Mcx<'mcx>,
    mtup: FormedMinimalTuple<'mcx>,
    slot: &mut SlotData<'mcx>,
    should_free: bool,
) -> PgResult<()> {
    // if (TTS_IS_MINIMALTUPLE(slot)) tts_minimal_store_tuple(slot, mtup, shouldFree);
    if let SlotData::Minimal(mslot) = slot {
        return tts_minimal_store_tuple(mcx, mslot, mtup, should_free);
    }

    // else: build the transient HeapTupleData over the minimal tuple and deform.
    // ExecClearTuple(slot);
    slot_clear(slot);
    // htup.t_len = mtup->t_len + MINIMAL_TUPLE_OFFSET;
    // htup.t_data = (HeapTupleHeader) ((char *) mtup - MINIMAL_TUPLE_OFFSET);
    // heap_deform_tuple(&htup, slot->tts_tupleDescriptor,
    //                   slot->tts_values, slot->tts_isnull);
    //
    // heap_tuple_from_minimal_tuple builds exactly that transient heap-tuple view
    // over the minimal body (t_len + MINIMAL_TUPLE_OFFSET, shared infomask/natts/
    // t_bits/t_hoff tail, system columns zeroed) as a FormedTuple; deform it into
    // the slot's value arrays.
    let _ = MINIMAL_TUPLE_OFFSET;
    let htup = backend_access_common_heaptuple::heap_tuple_from_minimal_tuple(mcx, &mtup)?;
    deform_into_slot(mcx, slot, &htup)?;
    // ExecStoreVirtualTuple(slot);
    ExecStoreVirtualTuple(slot)?;
    // if (shouldFree) { ExecMaterializeSlot(slot); pfree(mtup); }
    if should_free {
        ExecMaterializeSlot(mcx, slot)?;
    }

    Ok(())
}

/// `ExecStoreVirtualTuple(slot)` (execTuples.c): mark a slot holding
/// already-filled `tts_values`/`tts_isnull` as a valid virtual tuple.
pub fn ExecStoreVirtualTuple(slot: &mut SlotData) -> PgResult<()> {
    // Assert(TTS_EMPTY(slot));
    debug_assert!(slot.base().is_empty());

    let natts = tupdesc_natts(slot);

    let base = slot.base_mut();
    // slot->tts_flags &= ~TTS_FLAG_EMPTY;
    base.mark_not_empty();
    // slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
    base.tts_nvalid = natts as i16;

    Ok(())
}

/// `ExecStoreAllNullTuple(slot)` (execTuples.c): set all attributes NULL and
/// mark the slot a valid virtual tuple.
pub fn ExecStoreAllNullTuple<'mcx>(mcx: Mcx<'mcx>, slot: &mut SlotData<'mcx>) -> PgResult<()> {
    // Clear any old contents: ExecClearTuple(slot);
    slot_clear(slot);

    let natts = tupdesc_natts(slot) as usize;

    let base = slot.base_mut();
    // MemSet(slot->tts_values, 0, natts * sizeof(Datum));
    base.tts_values.clear();
    base.tts_values
        .try_reserve(natts)
        .map_err(|_| mcx.oom(natts * core::mem::size_of::<Datum>()))?;
    for _ in 0..natts {
        base.tts_values.push(Datum::null());
    }
    // memset(slot->tts_isnull, true, natts * sizeof(bool));
    base.tts_isnull.clear();
    base.tts_isnull.try_reserve(natts).map_err(|_| mcx.oom(natts))?;
    for _ in 0..natts {
        base.tts_isnull.push(true);
    }

    // return ExecStoreVirtualTuple(slot);
    ExecStoreVirtualTuple(slot)
}

/// `ExecStoreHeapTupleDatum(data, slot)` (execTuples.c): deform a composite
/// `Datum` into the slot as a virtual tuple.
pub fn ExecStoreHeapTupleDatum<'mcx>(
    mcx: Mcx<'mcx>,
    data: Datum<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<()> {
    // HeapTupleData tuple = {0};
    // td = DatumGetHeapTupleHeader(data);
    // tuple.t_len = HeapTupleHeaderGetDatumLength(td);
    // tuple.t_self = td->t_ctid;
    // tuple.t_data = td;
    //
    // ExecClearTuple(slot);
    slot_clear(slot);
    // heap_deform_tuple(&tuple, slot->tts_tupleDescriptor,
    //                   slot->tts_values, slot->tts_isnull);
    deform_composite_datum_into_slot(mcx, slot, data)?;
    // ExecStoreVirtualTuple(slot);
    ExecStoreVirtualTuple(slot)
}

/// `ExecFetchSlotHeapTuple(slot, materialize, &shouldFree)` (execTuples.c):
/// the slot's heap tuple, materializing if requested; returns `(tuple,
/// shouldFree)`.
pub fn ExecFetchSlotHeapTuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
    materialize: bool,
) -> PgResult<(FormedTuple<'mcx>, bool)> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base().is_empty());

    // if (materialize) slot->tts_ops->materialize(slot);
    if materialize {
        slot_materialize(mcx, slot)?;
    }

    // if (slot->tts_ops->get_heap_tuple == NULL) { *shouldFree=true;
    //     return slot->tts_ops->copy_heap_tuple(slot); }
    // else { *shouldFree=false; return slot->tts_ops->get_heap_tuple(slot); }
    //
    // Only the heap and buffer-heap kinds implement get_heap_tuple; virtual and
    // minimal do not (their copy_heap_tuple builds a fresh tuple).
    match slot {
        SlotData::Virtual(vslot) => {
            let tup = tts_virtual_copy_heap_tuple(mcx, vslot)?;
            Ok((tup, true))
        }
        SlotData::Minimal(mslot) => {
            let tup = tts_minimal_copy_heap_tuple(mcx, mslot)?;
            Ok((tup, true))
        }
        SlotData::Heap(hslot) => {
            let tup = tts_heap_get_heap_tuple(mcx, hslot)?;
            Ok((tup, false))
        }
        SlotData::BufferHeap(bslot) => {
            let tup = tts_buffer_heap_get_heap_tuple(mcx, bslot)?;
            Ok((tup, false))
        }
    }
}

/// `ExecFetchSlotMinimalTuple(slot, &shouldFree)` (execTuples.c).
pub fn ExecFetchSlotMinimalTuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<(FormedMinimalTuple<'mcx>, bool)> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base().is_empty());

    // if (slot->tts_ops->get_minimal_tuple) { *shouldFree=false;
    //     return slot->tts_ops->get_minimal_tuple(slot); }
    // else { *shouldFree=true; return slot->tts_ops->copy_minimal_tuple(slot, 0); }
    //
    // Only the minimal kind implements get_minimal_tuple; the others copy.
    match slot {
        SlotData::Minimal(mslot) => {
            let mtup = tts_minimal_get_minimal_tuple(mcx, mslot)?;
            Ok((mtup, false))
        }
        SlotData::Virtual(vslot) => {
            let mtup = tts_virtual_copy_minimal_tuple(mcx, vslot, 0)?;
            Ok((mtup, true))
        }
        SlotData::Heap(hslot) => {
            let mtup = tts_heap_copy_minimal_tuple(mcx, hslot, 0)?;
            Ok((mtup, true))
        }
        SlotData::BufferHeap(bslot) => {
            let mtup = tts_buffer_heap_copy_minimal_tuple(mcx, bslot, 0)?;
            Ok((mtup, true))
        }
    }
}

/// `ExecFetchSlotHeapTupleDatum(slot)` (execTuples.c): the slot's contents as
/// a composite `Datum`.
pub fn ExecFetchSlotHeapTupleDatum<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    // tup = ExecFetchSlotHeapTuple(slot, false, &shouldFree);
    let (tup, should_free) = ExecFetchSlotHeapTuple(mcx, slot, false)?;
    // tupdesc = slot->tts_tupleDescriptor;
    // ret = heap_copy_tuple_as_datum(tup, tupdesc);
    let ret = heap_copy_tuple_as_datum_carrier(mcx, slot, tup)?;
    // if (shouldFree) pfree(tup); -- the owned model drops `tup`.
    let _ = should_free;
    Ok(ret)
}

/// `ExecMaterializeSlot(slot)` (tuptable.h inline): force the slot's contents
/// to depend solely on the slot (`slot->tts_ops->materialize`).
pub fn ExecMaterializeSlot<'mcx>(mcx: Mcx<'mcx>, slot: &mut SlotData<'mcx>) -> PgResult<()> {
    slot_materialize(mcx, slot)
}

/// `ExecClearTuple(slot)` (tuptable.h inline): clear the slot's contents
/// (`slot->tts_ops->clear`).
pub fn ExecClearTuple(slot: &mut SlotData) -> PgResult<()> {
    slot_clear(slot);
    Ok(())
}

/// `ExecResetTupleTable`'s per-slot processing (execTuples.c): release the
/// resources held by one tuple-table slot.
///
/// ```c
/// ExecClearTuple(slot);
/// slot->tts_ops->release(slot);
/// if (slot->tts_tupleDescriptor)
/// {
///     ReleaseTupleDesc(slot->tts_tupleDescriptor);
///     slot->tts_tupleDescriptor = NULL;
/// }
/// ```
///
/// The C `shouldFree` branch (`pfree` of `tts_values`/`tts_isnull` and the slot
/// itself) is the owning pool's drop in `ExecResetTupleTable`, so it is not
/// performed here. `ReleaseTupleDesc(tupdesc)` is `if (tupdesc->tdrefcount >= 0)
/// DecrTupleDescRefCount(tupdesc)`; a non-refcounted descriptor is left as-is
/// (and dropped with the slot).
pub fn ExecResetOneSlot(slot: &mut SlotData<'_>) -> PgResult<()> {
    // Always release resources and reset the slot to empty.
    slot_clear(slot);
    slot_release(slot);
    // if (slot->tts_tupleDescriptor) { ReleaseTupleDesc(...); = NULL; }
    if let Some(desc) = slot.base_mut().tts_tupleDescriptor.take() {
        // ReleaseTupleDesc(tupdesc): only refcounted descriptors are released;
        // a non-refcounted one is simply dropped here (C leaves it for context
        // teardown).
        if desc.tdrefcount >= 0 {
            // The owned descriptor lives in a PgBox; DecrTupleDescRefCount takes
            // the value (and frees it when the count reaches zero).
            backend_access_common_tupdesc::DecrTupleDescRefCount(
                mcx::PgBox::into_inner(desc),
            )?;
        }
    }
    Ok(())
}

/// `ExecSetSlotDescriptor(slot, tupdesc)` (execTuples.c): swap the tuple
/// descriptor of a non-fixed slot, reallocating the `tts_values`/`tts_isnull`
/// arrays and re-pinning the descriptor.
///
/// ```c
/// Assert(!TTS_FIXED(slot));
/// ExecClearTuple(slot);
/// if (slot->tts_tupleDescriptor) ReleaseTupleDesc(slot->tts_tupleDescriptor);
/// if (slot->tts_values) pfree(slot->tts_values);
/// if (slot->tts_isnull) pfree(slot->tts_isnull);
/// slot->tts_tupleDescriptor = tupdesc;
/// PinTupleDesc(tupdesc);
/// slot->tts_values = MemoryContextAlloc(slot->tts_mcxt, natts * sizeof(Datum));
/// slot->tts_isnull = MemoryContextAlloc(slot->tts_mcxt, natts * sizeof(bool));
/// ```
///
/// The C `pfree` of the old Datum/isnull arrays is the owned `PgVec`s being
/// reallocated below; old refcounted descriptors are released, non-refcounted
/// ones are dropped (matching `ReleaseTupleDesc`'s `tdrefcount >= 0` guard).
pub fn ExecSetSlotDescriptor<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
    tupdesc: TupleDesc<'mcx>,
) -> PgResult<()> {
    // Assert(!TTS_FIXED(slot));
    debug_assert!(!slot.base().is_fixed());

    // For safety, make sure slot is empty before changing it.
    slot_clear(slot);

    let base = slot.base_mut();

    // Release any old descriptor (refcounted ones get DecrTupleDescRefCount;
    // non-refcounted ones are simply dropped). Also drops the old Datum/isnull
    // arrays (C: pfree(tts_values)/pfree(tts_isnull)).
    if let Some(old) = base.tts_tupleDescriptor.take() {
        if old.tdrefcount >= 0 {
            backend_access_common_tupdesc::DecrTupleDescRefCount(mcx::PgBox::into_inner(old))?;
        }
    }

    // Install the new descriptor; if it's refcounted, bump its refcount.
    let mut tupdesc = tupdesc.expect("ExecSetSlotDescriptor: tupdesc must be non-NULL");
    let natts = tupdesc.natts as usize;
    if tupdesc.tdrefcount >= 0 {
        backend_access_common_tupdesc::IncrTupleDescRefCount(&mut tupdesc)?;
    }
    base.tts_tupleDescriptor = Some(tupdesc);

    // Allocate Datum/isnull arrays of the appropriate size.
    let mut values: mcx::PgVec<'mcx, Datum<'mcx>> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut isnull: mcx::PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    values.resize(natts, Datum::null());
    isnull.resize(natts, false);
    base.tts_values = values;
    base.tts_isnull = isnull;

    Ok(())
}

/// `ExecCopySlot(dstslot, srcslot)` (tuptable.h inline): copy the source
/// slot's tuple into the destination (`dstslot->tts_ops->copyslot`).
pub fn ExecCopySlot<'mcx>(
    mcx: Mcx<'mcx>,
    dstslot: &mut SlotData<'mcx>,
    srcslot: &SlotData<'mcx>,
) -> PgResult<()> {
    // Assert(!TTS_EMPTY(srcslot)); Assert(srcslot != dstslot);
    debug_assert!(!srcslot.base().is_empty());
    // return dstslot->tts_ops->copyslot(dstslot, srcslot);
    slot_copyslot(mcx, dstslot, srcslot)
}

/// `ExecCopySlotHeapTuple(slot)` (tuptable.h inline): a heap tuple copy owned
/// by the caller (`slot->tts_ops->copy_heap_tuple`).
pub fn ExecCopySlotHeapTuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<FormedTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base().is_empty());
    // return slot->tts_ops->copy_heap_tuple(slot);
    match slot {
        SlotData::Virtual(vslot) => tts_virtual_copy_heap_tuple(mcx, vslot),
        SlotData::Heap(hslot) => tts_heap_copy_heap_tuple(mcx, hslot),
        SlotData::Minimal(mslot) => tts_minimal_copy_heap_tuple(mcx, mslot),
        SlotData::BufferHeap(bslot) => tts_buffer_heap_copy_heap_tuple(mcx, bslot),
    }
}

/// `ExecCopySlotMinimalTupleExtra(slot, extra)` (tuptable.h inline): a minimal
/// tuple copy with `extra` leading bytes reserved.
pub fn ExecCopySlotMinimalTupleExtra<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
    extra: Size,
) -> PgResult<FormedMinimalTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base().is_empty());
    // return slot->tts_ops->copy_minimal_tuple(slot, extra);
    //
    // The per-kind `copy_minimal_tuple` callbacks reserve `extra` leading bytes.
    match slot {
        SlotData::Virtual(vslot) => tts_virtual_copy_minimal_tuple(mcx, vslot, extra),
        SlotData::Heap(hslot) => tts_heap_copy_minimal_tuple(mcx, hslot, extra),
        SlotData::Minimal(mslot) => tts_minimal_copy_minimal_tuple(mcx, mslot, extra),
        SlotData::BufferHeap(bslot) => tts_buffer_heap_copy_minimal_tuple(mcx, bslot, extra),
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// `tts_buffer_heap_store_tuple(slot, tuple, buffer, transfer_pin)`
/// (execTuples.c static): store an on-disk heap tuple into a buffer-heap slot,
/// (re)pinning / releasing the underlying buffer as the same-page optimization
/// dictates (`ReleaseBuffer`/`IncrBufferRefCount`).
///
/// This is a `TupleTableSlotOps`-family callback (it lives next to the other
/// `tts_buffer_heap_*` ops, and its only callers are these `ExecStore*`
/// storers). The expanded slot payload model carries the stored on-disk tuple as
/// the body-bearing [`FormedTuple`] (`bslot->base.tuple`), so the stored tuple is
/// taken as a `FormedTuple` (the on-disk image, whose data still lives in the
/// pinned page). The buffer pin management is the load-bearing logic and routes
/// through the real `ReleaseBuffer`/`IncrBufferRefCount` bufmgr seams.
pub(crate) fn tts_buffer_heap_store_tuple<'mcx>(
    slot: &mut types_nodes::tuptable::BufferHeapTupleTableSlot<'mcx>,
    tuple: FormedTuple<'mcx>,
    buffer: Buffer,
    transfer_pin: bool,
) {
    use types_nodes::tuptable::TTS_FLAG_SHOULDFREE;

    // if (TTS_SHOULDFREE(slot)) {
    //     /* materialized slot shouldn't have a buffer to release */
    //     Assert(!BufferIsValid(bslot->buffer));
    //     heap_freetuple(bslot->base.tuple);
    //     slot->tts_flags &= ~TTS_FLAG_SHOULDFREE; }
    if slot.base.base.should_free() {
        debug_assert!(!BufferIsValid(slot.buffer));
        // heap_freetuple(bslot->base.tuple): dropping the owned tuple frees it.
        slot.base.tuple = None;
        slot.base.base.header.tts_flags &= !TTS_FLAG_SHOULDFREE;
    }

    // slot->tts_flags &= ~TTS_FLAG_EMPTY;
    slot.base.base.mark_not_empty();
    // slot->tts_nvalid = 0;
    slot.base.base.tts_nvalid = 0;
    // slot->tts_tid = tuple->t_self; (read before the tuple is moved in).
    slot.base.base.header.tts_tid = tuple.tuple.t_self;
    // bslot->base.tuple = tuple;
    slot.base.tuple = Some(tuple);
    // bslot->base.off = 0;
    slot.base.off = 0;

    // /*
    //  * If tuple is on a disk page, keep the page pinned as long as we hold a
    //  * pointer into it. ... This is coded to optimize the case where the slot
    //  * previously held a tuple on the same disk page ...
    //  */
    // if (bslot->buffer != buffer) {
    if slot.buffer != buffer {
        //     if (BufferIsValid(bslot->buffer)) ReleaseBuffer(bslot->buffer);
        if BufferIsValid(slot.buffer) {
            backend_storage_buffer_bufmgr_seams::release_buffer::call(slot.buffer);
        }
        //     bslot->buffer = buffer;
        slot.buffer = buffer;
        //     if (!transfer_pin && BufferIsValid(buffer)) IncrBufferRefCount(buffer);
        if !transfer_pin && BufferIsValid(buffer) {
            backend_storage_buffer_bufmgr_seams::incr_buffer_ref_count::call(buffer);
        }
    } else if transfer_pin && BufferIsValid(buffer) {
        // } else if (transfer_pin && BufferIsValid(buffer)) {
        //     /* In transfer_pin mode the caller won't know about the same-page
        //      * optimization, so we gotta release its pin. */
        //     ReleaseBuffer(buffer); }
        backend_storage_buffer_bufmgr_seams::release_buffer::call(buffer);
    }
}

/// `slot->tts_tupleDescriptor->natts` — the slot descriptor's attribute count.
fn tupdesc_natts(slot: &SlotData) -> i32 {
    slot.base()
        .tts_tupleDescriptor
        .as_ref()
        .map(|d| d.natts)
        .unwrap_or(0)
}

/// `(void) kind` keeps the `TupleSlotKind` import meaningful for callers that
/// branch on the slot class without a separate vtable lookup.
#[allow(dead_code)]
fn _slot_kind(slot: &SlotData) -> TupleSlotKind {
    slot.kind()
}

// ---------------------------------------------------------------------------
// deform-into-slot (the `heap_deform_tuple(tuple, tupleDesc, slot->tts_values,
// slot->tts_isnull)` calls of the `ExecForceStore*` virtual/minimal-store paths).
//
// In C these are a `heap_deform_tuple` straight into the slot's `tts_values`/
// `tts_isnull` arrays. heaptuple.c's `heap_deform_tuple` is a direct dependency;
// its idiomatic form deforms a `(header, tupleDesc, data-area bytes)` triple
// into `(Datum, bool)` columns. The body-bearing `FormedTuple` carrier
// supplies both the header and the data area, so the deform is now real
// own-logic over the slot's `Datum` lanes.
// ---------------------------------------------------------------------------

/// `heap_deform_tuple(tuple, slot->tts_tupleDescriptor, slot->tts_values,
/// slot->tts_isnull)` over a `FormedTuple` (the `ExecForceStore*`
/// virtual/minimal-target path): deconstruct the source tuple's columns directly
/// into the slot's `tts_values`/`tts_isnull` arrays.
fn deform_into_slot<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
    tuple: &FormedTuple<'mcx>,
) -> PgResult<()> {
    // Snapshot the descriptor (read-only) before borrowing the tts arrays.
    let base = slot.base();
    let desc = base
        .tts_tupleDescriptor
        .as_ref()
        .ok_or_else(|| PgError::error("deform_into_slot: slot has no tuple descriptor"))?;
    // heap_deform_tuple(tuple, tupleDesc, values, isnull): a column at a time
    // into (Datum, bool) pairs (heaptuple.c is a direct dependency).
    let columns =
        backend_access_common_heaptuple::heap_deform_tuple(mcx, &tuple.tuple, desc, &tuple.data)?;

    // Write the deformed (value, isnull) pairs into the slot's value arrays.
    let base = slot.base_mut();
    debug_assert!(base.tts_values.len() >= columns.len());
    for (i, (value, isnull)) in columns.into_iter().enumerate() {
        base.tts_values[i] = value;
        base.tts_isnull[i] = isnull;
    }
    Ok(())
}

/// `tuple` (built from `DatumGetHeapTupleHeader(data)`) deformed into the slot
/// (`ExecStoreHeapTupleDatum`).
///
/// `DatumGetHeapTupleHeader(data)` decodes a composite (`record`) `Datum` back
/// into a `HeapTupleHeader` + its data area. The canonical `Datum` enum carries
/// a composite value as `ByRef(bytes)`; the composite/record-Datum carrier
/// bridge (task #161) decodes those `ByRef` bytes back into a `FormedTuple`
/// (`backend_access_common_heaptuple::DatumGetHeapTupleHeader`), which is then
/// deformed into the slot's `tts_values`/`tts_isnull` arrays exactly as
/// `deform_into_slot` does for a directly-stored tuple.
fn deform_composite_datum_into_slot<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
    data: Datum<'mcx>,
) -> PgResult<()> {
    // tuple = DatumGetHeapTupleHeader(data) — decode the composite Datum's
    // ByRef bytes back into a FormedTuple (owned header + data area).
    let tuple = backend_access_common_heaptuple::DatumGetHeapTupleHeader(mcx, &data)?;
    // heap_deform_tuple(&tuple, slot->tts_tupleDescriptor, values, isnull).
    deform_into_slot(mcx, slot, &tuple)
}

/// `heap_copy_tuple_as_datum(tup, slot->tts_tupleDescriptor)`
/// (`ExecFetchSlotHeapTupleDatum`): the fetched heap tuple flattened into a
/// composite `Datum`.
///
/// The composite/record-Datum carrier bridge (task #161): mint the formed tuple
/// into a composite `Datum` via
/// `backend_access_common_heaptuple::HeapTupleGetDatum`, which sets the
/// `datum_len_`/`datum_typeid`/`datum_typmod` header fields (and flattens any
/// external TOAST pointers) and serialises the contiguous `HeapTupleHeader` image
/// into the canonical `Datum::ByRef` byte layout.
fn heap_copy_tuple_as_datum_carrier<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &SlotData<'mcx>,
    tup: FormedTuple<'mcx>,
) -> PgResult<Datum<'mcx>> {
    // tupdesc = slot->tts_tupleDescriptor;
    let desc = slot
        .base()
        .tts_tupleDescriptor
        .as_ref()
        .ok_or_else(|| {
            PgError::error("ExecFetchSlotHeapTupleDatum: slot has no tuple descriptor")
        })?;
    // ret = heap_copy_tuple_as_datum(tup, tupdesc) -> composite Datum.
    backend_access_common_heaptuple::HeapTupleGetDatum(mcx, &tup, desc)
}
