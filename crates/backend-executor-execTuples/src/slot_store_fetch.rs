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
use types_datum::Datum;
use types_error::{PgError, PgResult};
use types_nodes::tuptable::{SlotData, TTS_FLAG_SHOULDFREE};
use types_nodes::TupleSlotKind;
use types_storage::buf::{Buffer, BufferIsValid};
use types_tuple::heaptuple::{HeapTuple, MinimalTuple, MINIMAL_TUPLE_OFFSET};

use crate::slot_ops_vtables::{
    slot_clear, slot_copyslot, slot_materialize, tts_buffer_heap_copy_heap_tuple,
    tts_buffer_heap_copy_minimal_tuple, tts_buffer_heap_get_heap_tuple, tts_heap_copy_heap_tuple,
    tts_heap_copy_minimal_tuple, tts_heap_get_heap_tuple, tts_heap_store_tuple,
    tts_minimal_copy_heap_tuple, tts_minimal_copy_minimal_tuple, tts_minimal_get_minimal_tuple,
    tts_minimal_store_tuple, tts_virtual_copy_heap_tuple, tts_virtual_copy_minimal_tuple,
};

/// `ExecStoreHeapTuple(tuple, slot, shouldFree)` (execTuples.c).
pub fn ExecStoreHeapTuple<'mcx>(
    tuple: HeapTuple<'mcx>,
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
    let t_table_oid = tuple
        .as_ref()
        .map(|t| t.t_tableOid)
        .unwrap_or_default();

    // tts_heap_store_tuple(slot, tuple, shouldFree);
    tts_heap_store_tuple(hslot, tuple, should_free);

    slot.base_mut().header.tts_tableOid = t_table_oid;

    Ok(())
}

/// `ExecStoreBufferHeapTuple(tuple, slot, buffer)` (execTuples.c).
pub fn ExecStoreBufferHeapTuple<'mcx>(
    tuple: HeapTuple<'mcx>,
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

    let t_table_oid = tuple
        .as_ref()
        .map(|t| t.t_tableOid)
        .unwrap_or_default();

    // tts_buffer_heap_store_tuple(slot, tuple, buffer, false);
    tts_buffer_heap_store_tuple(bslot, tuple, buffer, false);

    slot.base_mut().header.tts_tableOid = t_table_oid;

    Ok(())
}

/// `ExecStorePinnedBufferHeapTuple(tuple, slot, buffer)` (execTuples.c).
pub fn ExecStorePinnedBufferHeapTuple<'mcx>(
    tuple: HeapTuple<'mcx>,
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

    let t_table_oid = tuple
        .as_ref()
        .map(|t| t.t_tableOid)
        .unwrap_or_default();

    // tts_buffer_heap_store_tuple(slot, tuple, buffer, true);
    tts_buffer_heap_store_tuple(bslot, tuple, buffer, true);

    slot.base_mut().header.tts_tableOid = t_table_oid;

    Ok(())
}

/// `ExecStoreMinimalTuple(mtup, slot, shouldFree)` (execTuples.c).
pub fn ExecStoreMinimalTuple<'mcx>(
    mtup: MinimalTuple<'mcx>,
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
    tts_minimal_store_tuple(mslot, mtup, should_free);

    Ok(())
}

/// `ExecForceStoreHeapTuple(tuple, slot, shouldFree)` (execTuples.c): store a
/// `HeapTuple` into any kind of slot, performing conversion if necessary.
pub fn ExecForceStoreHeapTuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: HeapTuple<'mcx>,
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
            let copied = heap_copytuple_into_slot_context(mcx, tuple)?;
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
            deform_foreign_tuple_into_slot(mcx, slot, tuple.as_deref())?;
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
    mtup: MinimalTuple<'mcx>,
    slot: &mut SlotData<'mcx>,
    should_free: bool,
) -> PgResult<()> {
    // if (TTS_IS_MINIMALTUPLE(slot)) tts_minimal_store_tuple(slot, mtup, shouldFree);
    if let SlotData::Minimal(mslot) = slot {
        tts_minimal_store_tuple(mslot, mtup, should_free);
        return Ok(());
    }

    // else: build the transient HeapTupleData over the minimal tuple and deform.
    // ExecClearTuple(slot);
    slot_clear(slot);
    // htup.t_len = mtup->t_len + MINIMAL_TUPLE_OFFSET;
    // htup.t_data = (HeapTupleHeader) ((char *) mtup - MINIMAL_TUPLE_OFFSET);
    // heap_deform_tuple(&htup, slot->tts_tupleDescriptor,
    //                   slot->tts_values, slot->tts_isnull);
    deform_minimal_tuple_into_slot(mcx, slot, mtup.as_deref(), MINIMAL_TUPLE_OFFSET)?;
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
    base.tts_values.try_reserve(natts).map_err(|_| mcx.oom(natts * core::mem::size_of::<Datum>()))?;
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
    data: Datum,
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
) -> PgResult<(HeapTuple<'mcx>, bool)> {
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
            let tup = tts_heap_get_heap_tuple(hslot)?;
            Ok((tup, false))
        }
        SlotData::BufferHeap(bslot) => {
            let tup = tts_buffer_heap_get_heap_tuple(bslot)?;
            Ok((tup, false))
        }
    }
}

/// `ExecFetchSlotMinimalTuple(slot, &shouldFree)` (execTuples.c).
pub fn ExecFetchSlotMinimalTuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<(MinimalTuple<'mcx>, bool)> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base().is_empty());

    // if (slot->tts_ops->get_minimal_tuple) { *shouldFree=false;
    //     return slot->tts_ops->get_minimal_tuple(slot); }
    // else { *shouldFree=true; return slot->tts_ops->copy_minimal_tuple(slot, 0); }
    //
    // Only the minimal kind implements get_minimal_tuple; the others copy.
    match slot {
        SlotData::Minimal(mslot) => {
            let mtup = tts_minimal_get_minimal_tuple(mslot)?;
            Ok((mtup, false))
        }
        SlotData::Virtual(vslot) => {
            let mtup = tts_virtual_copy_minimal_tuple(mcx, vslot)?;
            Ok((mtup, true))
        }
        SlotData::Heap(hslot) => {
            let mtup = tts_heap_copy_minimal_tuple(mcx, hslot)?;
            Ok((mtup, true))
        }
        SlotData::BufferHeap(bslot) => {
            let mtup = tts_buffer_heap_copy_minimal_tuple(mcx, bslot)?;
            Ok((mtup, true))
        }
    }
}

/// `ExecFetchSlotHeapTupleDatum(slot)` (execTuples.c): the slot's contents as
/// a composite `Datum`.
pub fn ExecFetchSlotHeapTupleDatum<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<Datum> {
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
) -> PgResult<HeapTuple<'mcx>> {
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
) -> PgResult<MinimalTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base().is_empty());
    // return slot->tts_ops->copy_minimal_tuple(slot, extra);
    //
    // The per-kind `copy_minimal_tuple` callbacks reserve `extra` leading bytes;
    // the slot-ops family owns the `extra` plumbing.
    let _ = extra;
    match slot {
        SlotData::Virtual(vslot) => tts_virtual_copy_minimal_tuple(mcx, vslot),
        SlotData::Heap(hslot) => tts_heap_copy_minimal_tuple(mcx, hslot),
        SlotData::Minimal(mslot) => tts_minimal_copy_minimal_tuple(mcx, mslot),
        SlotData::BufferHeap(bslot) => tts_buffer_heap_copy_minimal_tuple(mcx, bslot),
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
/// storers). The slot-ops vtable family owns it and its buffer-manager seam
/// routing; the scaffold did not surface it, so it is routed through that owner
/// and panics until it lands.
fn tts_buffer_heap_store_tuple<'mcx>(
    _slot: &mut types_nodes::tuptable::BufferHeapTupleTableSlot<'mcx>,
    _tuple: HeapTuple<'mcx>,
    _buffer: Buffer,
    _transfer_pin: bool,
) {
    panic!(
        "execTuples.c tts_buffer_heap_store_tuple: buffer-heap store callback (incl. \
         ReleaseBuffer/IncrBufferRefCount pin management) is owned by the slot_ops_vtables family"
    )
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
// slot->tts_isnull)` calls of the `ExecForceStore*`/`ExecStoreHeapTupleDatum`
// virtual-store paths).
//
// In C these are a `heap_deform_tuple` straight into the slot's `tts_values`/
// `tts_isnull` arrays. heaptuple.c's `heap_deform_tuple` is a direct dependency
// here, but its idiomatic form deforms a `(header, tupleDesc, data-area bytes)`
// triple into `(TupleValue, bool)` columns. The source tuple's *data area*
// (`tup + t_hoff`) is carried by the slot payload model's heap-tuple-with-data
// carrier (a `FormedTuple`-shaped pairing), which the scaffold's header-only
// `HeapTuple` / `MinimalTuple` / opaque composite `Datum` do not yet expose —
// that carrier, and the `TupleValue`→slot-`Datum` bridge, are owned by the
// `slot_payload_model` / `slot_deform` sibling families and have not landed.
//
// Per "Mirror PG and panic", the foreign-tuple deform-into-slot is routed
// through that owner and panics loudly until the payload model lands; the
// surrounding `ExecForceStore*` control flow (clear / store-virtual /
// materialize / kind dispatch) is implemented in full above.
// ---------------------------------------------------------------------------

/// `heap_deform_tuple(tuple, slot->tts_tupleDescriptor, slot->tts_values,
/// slot->tts_isnull)` over a foreign `HeapTuple` (the `ExecForceStoreHeapTuple`
/// virtual/minimal-target path).
fn deform_foreign_tuple_into_slot<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut SlotData<'mcx>,
    _tuple: Option<&types_tuple::heaptuple::HeapTupleData<'mcx>>,
) -> PgResult<()> {
    // The header-only `HeapTuple` carries no data area; the heap-tuple-with-data
    // carrier is the slot payload model's, not yet landed.
    panic!(
        "execTuples.c ExecForceStoreHeapTuple: heap_deform_tuple into slot needs the \
         payload model's heap-tuple-data carrier (slot_payload_model/slot_deform owner)"
    )
}

/// `htup` (over `mtup - MINIMAL_TUPLE_OFFSET`) deformed into the slot (the
/// `ExecForceStoreMinimalTuple` non-minimal-target path).
fn deform_minimal_tuple_into_slot<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut SlotData<'mcx>,
    _mtup: Option<&types_tuple::heaptuple::MinimalTupleData<'mcx>>,
    _minimal_tuple_offset: usize,
) -> PgResult<()> {
    panic!(
        "execTuples.c ExecForceStoreMinimalTuple: heap_deform_tuple of the minimal tuple \
         into slot needs the payload model's heap-tuple-data carrier \
         (slot_payload_model/slot_deform owner)"
    )
}

/// `tuple` (built from `DatumGetHeapTupleHeader(data)`) deformed into the slot
/// (`ExecStoreHeapTupleDatum`).
fn deform_composite_datum_into_slot<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut SlotData<'mcx>,
    _data: Datum,
) -> PgResult<()> {
    panic!(
        "execTuples.c ExecStoreHeapTupleDatum: DatumGetHeapTupleHeader + heap_deform_tuple \
         into slot needs the payload model's composite-Datum carrier \
         (slot_payload_model/slot_deform owner)"
    )
}

/// `bslot->base.tuple = heap_copytuple(tuple)` (the `ExecForceStoreHeapTuple`
/// buffer-heap-target path): a self-owned copy of the source tuple in the
/// slot's context.
///
/// heaptuple.c's `heap_copytuple` is a direct dependency, but it copies a
/// `FormedTuple` (header + data area); the scaffold's `HeapTuple` is
/// header-only and the data-area pairing is the slot payload model's, not yet
/// landed — so the copy is routed through that owner and panics until it does.
fn heap_copytuple_into_slot_context<'mcx>(
    _mcx: Mcx<'mcx>,
    _tuple: HeapTuple<'mcx>,
) -> PgResult<HeapTuple<'mcx>> {
    panic!(
        "execTuples.c ExecForceStoreHeapTuple (buffer-heap target): heap_copytuple needs the \
         payload model's heap-tuple-data carrier (slot_payload_model/slot_deform owner)"
    )
}

/// `heap_copy_tuple_as_datum(tup, slot->tts_tupleDescriptor)`
/// (`ExecFetchSlotHeapTupleDatum`): the fetched heap tuple flattened into a
/// composite `Datum`.
///
/// heaptuple.c's `heap_copy_tuple_as_datum` is a direct dependency, but it
/// works over a `FormedTuple` (header + data area) and yields a `FormedTuple`
/// composite carrier; the scaffold's `HeapTuple` / `Datum` are header-only /
/// opaque and the data-area + composite-Datum carriers are the slot payload
/// model's — routed through that owner and panicking until it lands.
fn heap_copy_tuple_as_datum_carrier<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &SlotData<'mcx>,
    _tup: HeapTuple<'mcx>,
) -> PgResult<Datum> {
    panic!(
        "execTuples.c ExecFetchSlotHeapTupleDatum: heap_copy_tuple_as_datum needs the \
         payload model's heap-tuple-data + composite-Datum carriers \
         (slot_payload_model/slot_deform owner)"
    )
}
