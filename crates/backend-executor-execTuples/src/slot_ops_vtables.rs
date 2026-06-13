//! Family: slot-ops vtables — the per-kind `TupleTableSlotOps` callbacks for
//! the virtual/heap/minimal/buffer slot classes (execTuples.c
//! `tts_virtual_*` / `tts_heap_*` / `tts_minimal_*` / `tts_buffer_heap_*`),
//! plus the `Slot`-level dispatch that routes `slot->tts_ops->op(slot)` to the
//! right kind.
//!
//! Each callback takes a `&mut` to the concrete payload subtype (the analog of
//! the C downcast of `TupleTableSlot *`); allocating callbacks take `Mcx`.

use mcx::Mcx;
use types_core::primitive::AttrNumber;
use types_datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_nodes::tuptable::{
    BufferHeapTupleTableSlot, HeapTupleTableSlot, MinimalTupleTableSlot, SlotData,
    VirtualTupleTableSlot, TTS_FLAG_SHOULDFREE,
};
use types_storage::buf::{BufferIsValid, InvalidBuffer};
use types_tuple::backend_access_common_heaptuple::TupleValue;
use types_tuple::heaptuple::{HeapTuple, MinimalTuple};

use crate::slot_deform::slot_deform_heap_tuple;

// ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("...")))
fn feature_not_supported(msg: &'static str) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

// elog(ERROR, "...") — internal error.
fn elog_error(msg: &'static str) -> PgError {
    PgError::error(msg).with_sqlstate(types_error::ERRCODE_INTERNAL_ERROR)
}

// --- VirtualTupleTableSlot ops --------------------------------------------

/// `tts_virtual_init` (execTuples.c): empty body.
pub fn tts_virtual_init(_slot: &mut VirtualTupleTableSlot) {}

/// `tts_virtual_release` (execTuples.c): empty body.
pub fn tts_virtual_release(_slot: &mut VirtualTupleTableSlot) {}

/// `tts_virtual_clear` (execTuples.c).
pub fn tts_virtual_clear(slot: &mut VirtualTupleTableSlot) {
    // if (unlikely(TTS_SHOULDFREE(slot))) { pfree(vslot->data); vslot->data =
    // NULL; slot->tts_flags &= ~TTS_FLAG_SHOULDFREE; }
    if slot.base.should_free() {
        // pfree(vslot->data); vslot->data = NULL: release the materialized data
        // buffer (the owned PgVec models C's `char *data`; emptying it is the
        // free + NULL).
        slot.data.clear();
        slot.base.header.tts_flags &= !TTS_FLAG_SHOULDFREE;
    }

    // slot->tts_nvalid = 0;
    slot.base.tts_nvalid = 0;
    // slot->tts_flags |= TTS_FLAG_EMPTY;
    slot.base
        .header
        .tts_flags |= types_nodes::executor::TTS_FLAG_EMPTY;
    // ItemPointerSetInvalid(&slot->tts_tid);
    slot.base.header.tts_tid = types_tuple::heaptuple::ItemPointerData::default();
}

/// `tts_virtual_getsomeattrs` (execTuples.c):
/// `elog(ERROR, "getsomeattrs is not required to be called on a virtual tuple
/// table slot")`.
pub fn tts_virtual_getsomeattrs(_slot: &mut VirtualTupleTableSlot, _natts: i32) -> PgResult<()> {
    Err(elog_error(
        "getsomeattrs is not required to be called on a virtual tuple table slot",
    ))
}

/// `tts_virtual_getsysattr` (execTuples.c): `ereport(ERROR,
/// FEATURE_NOT_SUPPORTED, "cannot retrieve a system column in this context")`.
pub fn tts_virtual_getsysattr(
    slot: &VirtualTupleTableSlot,
    _attnum: i32,
) -> PgResult<(Datum, bool)> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());
    Err(feature_not_supported(
        "cannot retrieve a system column in this context",
    ))
}

/// `tts_virtual_is_current_xact_tuple` (execTuples.c): `ereport(ERROR,
/// FEATURE_NOT_SUPPORTED, "don't have transaction information for this type of
/// tuple")`.
pub fn tts_virtual_is_current_xact_tuple(slot: &VirtualTupleTableSlot) -> PgResult<bool> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());
    Err(feature_not_supported(
        "don't have transaction information for this type of tuple",
    ))
}

/// `tts_virtual_materialize` (execTuples.c): flatten the slot's
/// `tts_values`/`tts_isnull` so they no longer point at external memory.
pub fn tts_virtual_materialize<'mcx>(
    _mcx: Mcx<'mcx>,
    slot: &mut VirtualTupleTableSlot<'mcx>,
) -> PgResult<()> {
    // /* already materialized */
    // if (TTS_SHOULDFREE(slot)) return;
    if slot.base.should_free() {
        return Ok(());
    }

    // The C body computes the byte size of every non-NULL by-reference datum
    // (att_addlength_datum / EOH_get_flat_size), allocates `vslot->data` in
    // slot->tts_mcxt, copies each datum into it, and repoints
    // slot->tts_values[natt] = PointerGetDatum(data). That copy is raw-pointer
    // datum manipulation keyed off the unported `utils/adt/datum.c`
    // primitives (att_addlength_datum, datumGetSize) and the expanded-object
    // flatten path (EOH_get_flat_size / EOH_flatten_into) owned by
    // utils/adt/expandeddatum.c. The slot's `tts_values: PgVec<Datum>` carry
    // bare machine words, so repointing them into `vslot->data` requires those
    // owners' substrate. Mirror PG and panic until the slot payload model's
    // datum-flatten bridge lands (slot_payload_model + datum.c owners).
    let _ = slot;
    panic!("execTuples.c tts_virtual_materialize: datum flatten into vslot->data needs unported utils/adt/datum.c (att_addlength_datum/datumGetSize) + expandeddatum.c (EOH_get_flat_size/EOH_flatten_into) over the slot's raw tts_values words")
}

/// `tts_virtual_copyslot` (execTuples.c): copy `src`'s attributes into `dst`,
/// then materialize `dst` so it doesn't depend on external memory.
pub fn tts_virtual_copyslot<'mcx>(
    mcx: Mcx<'mcx>,
    dst: &mut SlotData<'mcx>,
    src: &SlotData<'mcx>,
) -> PgResult<()> {
    // TupleDesc srcdesc = srcslot->tts_tupleDescriptor;
    let srcnatts = src
        .base()
        .tts_tupleDescriptor
        .as_ref()
        .map(|d| d.natts)
        .unwrap_or(0);

    // tts_virtual_clear(dstslot);
    match dst {
        SlotData::Virtual(d) => tts_virtual_clear(d),
        // The copyslot callback is only ever installed for the virtual ops, so
        // dstslot is a virtual slot (C: dstslot->tts_ops->copyslot ==
        // tts_virtual_copyslot). A non-virtual dst is the C type confusion.
        _ => return Err(elog_error("tts_virtual_copyslot: destination is not a virtual slot")),
    }

    // slot_getallattrs(srcslot): deconstruct all of the source's attributes
    // into its tts_values/tts_isnull. The source slot is borrowed immutably
    // here (C mutates it); deforming a non-virtual source needs &mut. Since
    // the only safe-to-deform-in-place source is a virtual slot (already fully
    // valid: tts_nvalid == natts), and a non-virtual source would require a
    // &mut borrow the callback signature doesn't grant, route the deform for
    // non-virtual sources to the owner once the slot payload model's
    // shared-deform path lands. For a virtual source nothing to deform.
    if !matches!(src, SlotData::Virtual(_)) {
        panic!("execTuples.c tts_virtual_copyslot: slot_getallattrs(srcslot) on a non-virtual source needs the slot payload model's deform-through-shared-ref path")
    }

    // for (natt = 0; natt < srcdesc->natts; natt++) {
    //     dstslot->tts_values[natt] = srcslot->tts_values[natt];
    //     dstslot->tts_isnull[natt] = srcslot->tts_isnull[natt]; }
    {
        let (sv, si) = {
            let sb = src.base();
            (sb.tts_values.clone(), sb.tts_isnull.clone())
        };
        let db = dst.base_mut();
        for natt in 0..srcnatts as usize {
            db.tts_values[natt] = sv[natt].clone();
            db.tts_isnull[natt] = si[natt];
        }
        // dstslot->tts_nvalid = srcdesc->natts;
        db.tts_nvalid = srcnatts as AttrNumber;
        // dstslot->tts_flags &= ~TTS_FLAG_EMPTY;
        db.mark_not_empty();
    }

    // /* make sure storage doesn't depend on external memory */
    // tts_virtual_materialize(dstslot);
    match dst {
        SlotData::Virtual(d) => tts_virtual_materialize(mcx, d),
        _ => unreachable!(),
    }
}

/// `tts_virtual_copy_heap_tuple` (execTuples.c):
/// `heap_form_tuple(slot->tts_tupleDescriptor, slot->tts_values,
/// slot->tts_isnull)`.
pub fn tts_virtual_copy_heap_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    slot: &VirtualTupleTableSlot<'mcx>,
) -> PgResult<HeapTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());

    // return heap_form_tuple(slot->tts_tupleDescriptor, slot->tts_values,
    //                        slot->tts_isnull);
    //
    // heap_form_tuple returns a `FormedTuple` (the owned header + a separate
    // user-data byte buffer), which cannot be carried by the slot's
    // `HeapTuple = Option<PgBox<HeapTupleData>>` field (header-only, no data
    // bytes). The slot's `tts_values` are also raw `Datum` words rather than
    // `TupleValue`s heap_form_tuple consumes. Both gaps are the sibling
    // `slot_payload_model` family's tuple-carrier bridge. Mirror PG and panic
    // until it lands.
    let _ = slot;
    panic!("execTuples.c tts_virtual_copy_heap_tuple: heap_form_tuple over the slot's raw tts_values + the FormedTuple->HeapTuple carrier bridge are owned by the slot payload model")
}

/// `tts_virtual_copy_minimal_tuple` (execTuples.c):
/// `heap_form_minimal_tuple(slot->tts_tupleDescriptor, slot->tts_values,
/// slot->tts_isnull, extra)`.
pub fn tts_virtual_copy_minimal_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    slot: &VirtualTupleTableSlot<'mcx>,
) -> PgResult<MinimalTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());

    // return heap_form_minimal_tuple(slot->tts_tupleDescriptor,
    //                                slot->tts_values, slot->tts_isnull, extra);
    // Same carrier gap as tts_virtual_copy_heap_tuple (FormedMinimalTuple ->
    // the slot's MinimalTuple header-only carrier + raw tts_values words).
    let _ = slot;
    panic!("execTuples.c tts_virtual_copy_minimal_tuple: heap_form_minimal_tuple over raw tts_values + FormedMinimalTuple->MinimalTuple carrier bridge are owned by the slot payload model")
}

// --- HeapTupleTableSlot ops -----------------------------------------------

/// `tts_heap_init` (execTuples.c): empty body.
pub fn tts_heap_init(_slot: &mut HeapTupleTableSlot) {}

/// `tts_heap_release` (execTuples.c): empty body.
pub fn tts_heap_release(_slot: &mut HeapTupleTableSlot) {}

/// `tts_heap_clear` (execTuples.c).
pub fn tts_heap_clear(slot: &mut HeapTupleTableSlot) {
    // /* Free the memory for the heap tuple if it's allowed. */
    // if (TTS_SHOULDFREE(slot)) { heap_freetuple(hslot->tuple);
    //     slot->tts_flags &= ~TTS_FLAG_SHOULDFREE; }
    if slot.base.should_free() {
        // heap_freetuple(hslot->tuple): dropping the owned tuple frees it.
        slot.tuple = None;
        slot.base.header.tts_flags &= !TTS_FLAG_SHOULDFREE;
    }

    // slot->tts_nvalid = 0;
    slot.base.tts_nvalid = 0;
    // slot->tts_flags |= TTS_FLAG_EMPTY;
    slot.base.header.tts_flags |= types_nodes::executor::TTS_FLAG_EMPTY;
    // ItemPointerSetInvalid(&slot->tts_tid);
    slot.base.header.tts_tid = types_tuple::heaptuple::ItemPointerData::default();
    // hslot->off = 0;
    slot.off = 0;
    // hslot->tuple = NULL;
    slot.tuple = None;
}

/// `tts_heap_getsomeattrs` (execTuples.c):
/// `slot_deform_heap_tuple(slot, hslot->tuple, &hslot->off, natts)`.
pub fn tts_heap_getsomeattrs<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut HeapTupleTableSlot<'mcx>,
    natts: i32,
) -> PgResult<()> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());

    // slot_deform_heap_tuple(slot, hslot->tuple, &hslot->off, natts);
    slot_deform_heap_tuple(mcx, slot, natts)
}

/// `tts_heap_getsysattr` (execTuples.c).
pub fn tts_heap_getsysattr<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &HeapTupleTableSlot<'mcx>,
    attnum: i32,
) -> PgResult<(Datum, bool)> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());

    // if (!hslot->tuple) ereport(ERROR, FEATURE_NOT_SUPPORTED,
    //     "cannot retrieve a system column in this context");
    let Some(tuple) = slot.tuple.as_ref() else {
        return Err(feature_not_supported(
            "cannot retrieve a system column in this context",
        ));
    };

    // return heap_getsysattr(hslot->tuple, attnum, slot->tts_tupleDescriptor,
    //                        isnull);
    let (value, isnull) =
        backend_access_common_heaptuple::heap_getsysattr(mcx, &tuple.tuple, attnum)?;
    Ok((tuple_value_to_datum(&value)?, isnull))
}

/// `tts_heap_is_current_xact_tuple` (execTuples.c).
pub fn tts_heap_is_current_xact_tuple(slot: &HeapTupleTableSlot) -> PgResult<bool> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());

    // if (!hslot->tuple) ereport(ERROR, FEATURE_NOT_SUPPORTED,
    //     "don't have a storage tuple in this context");
    let Some(tuple) = slot.tuple.as_ref() else {
        return Err(feature_not_supported(
            "don't have a storage tuple in this context",
        ));
    };

    // xmin = HeapTupleHeaderGetRawXmin(hslot->tuple->t_data);
    let header = tuple
        .tuple
        .t_data
        .as_ref()
        .ok_or_else(|| elog_error("tts_heap_is_current_xact_tuple: tuple has no t_data"))?;
    let xmin = types_tuple::heaptuple::HeapTupleHeaderGetRawXmin(header);

    // return TransactionIdIsCurrentTransactionId(xmin);
    Ok(backend_access_transam_xact_seams::transaction_id_is_current_transaction_id::call(xmin))
}

/// `tts_heap_materialize` (execTuples.c).
pub fn tts_heap_materialize<'mcx>(
    _mcx: Mcx<'mcx>,
    slot: &mut HeapTupleTableSlot<'mcx>,
) -> PgResult<()> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());

    // /* If slot has its tuple already materialized, nothing to do. */
    // if (TTS_SHOULDFREE(slot)) return;
    if slot.base.should_free() {
        return Ok(());
    }

    // oldContext = MemoryContextSwitchTo(slot->tts_mcxt);
    // /* Have to deform from scratch ... */
    // slot->tts_nvalid = 0; hslot->off = 0;
    slot.base.tts_nvalid = 0;
    slot.off = 0;

    // if (!hslot->tuple)
    //     hslot->tuple = heap_form_tuple(slot->tts_tupleDescriptor,
    //         slot->tts_values, slot->tts_isnull);
    // else
    //     hslot->tuple = heap_copytuple(hslot->tuple);
    // slot->tts_flags |= TTS_FLAG_SHOULDFREE;
    //
    // Both heap_form_tuple (raw tts_values words) and heap_copytuple
    // (FormedTuple) produce the heaptuple crate's `FormedTuple`, which the
    // slot's header-only `HeapTuple` carrier cannot hold. Carrier bridge is
    // the sibling slot_payload_model family's. Mirror PG and panic.
    panic!("execTuples.c tts_heap_materialize: heap_form_tuple/heap_copytuple produce FormedTuple; the slot's HeapTuple carrier (header-only) + raw tts_values are the slot payload model's bridge")
}

/// `tts_heap_get_heap_tuple` (execTuples.c).
pub fn tts_heap_get_heap_tuple<'mcx>(
    _slot: &mut HeapTupleTableSlot<'mcx>,
) -> PgResult<HeapTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    // if (!hslot->tuple) tts_heap_materialize(slot);
    // return hslot->tuple;
    //
    // Returning hslot->tuple depends on tts_heap_materialize being able to
    // store a FormedTuple in the slot's HeapTuple carrier (blocked above).
    panic!("execTuples.c tts_heap_get_heap_tuple: depends on tts_heap_materialize's FormedTuple->HeapTuple carrier (slot payload model)")
}

/// `tts_heap_copy_heap_tuple` (execTuples.c).
pub fn tts_heap_copy_heap_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut HeapTupleTableSlot<'mcx>,
) -> PgResult<HeapTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    // if (!hslot->tuple) tts_heap_materialize(slot);
    // return heap_copytuple(hslot->tuple);
    //
    // heap_copytuple(hslot->tuple) returns a FormedTuple the slot's HeapTuple
    // carrier cannot hold; also gated on tts_heap_materialize. Mirror+panic.
    panic!("execTuples.c tts_heap_copy_heap_tuple: heap_copytuple yields FormedTuple; slot HeapTuple carrier + materialize are the slot payload model's bridge")
}

/// `tts_heap_copy_minimal_tuple` (execTuples.c).
pub fn tts_heap_copy_minimal_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut HeapTupleTableSlot<'mcx>,
) -> PgResult<MinimalTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    // if (!hslot->tuple) tts_heap_materialize(slot);
    // return minimal_tuple_from_heap_tuple(hslot->tuple, extra);
    //
    // minimal_tuple_from_heap_tuple needs a FormedTuple input (the slot's
    // HeapTuple carrier can't supply the data bytes) and returns a
    // FormedMinimalTuple the slot's MinimalTuple carrier can't hold.
    panic!("execTuples.c tts_heap_copy_minimal_tuple: minimal_tuple_from_heap_tuple over FormedTuple; carrier bridge owned by the slot payload model")
}

/// `tts_heap_store_tuple` (execTuples.c).
pub fn tts_heap_store_tuple<'mcx>(
    slot: &mut HeapTupleTableSlot<'mcx>,
    tuple: HeapTuple<'mcx>,
    should_free: bool,
) {
    // tts_heap_clear(slot);
    tts_heap_clear(slot);

    // slot->tts_nvalid = 0;
    slot.base.tts_nvalid = 0;

    // hslot->tuple = tuple;
    // hslot->off = 0;
    // slot->tts_flags &= ~(TTS_FLAG_EMPTY | TTS_FLAG_SHOULDFREE);
    // slot->tts_tid = tuple->t_self;
    // if (shouldFree) slot->tts_flags |= TTS_FLAG_SHOULDFREE;
    //
    // The expanded slot payload model carries the stored heap tuple as the
    // body-bearing `FormedTuple` (header + data-area bytes). The public
    // `HeapTuple` parameter is header-only (`Option<PgBox<HeapTupleData>>`), so
    // wrapping it into the slot's `FormedTuple` field needs the carrier bridge
    // (recovering / referencing the tuple's user-data bytes), which is the
    // sibling store/fetch fill family's. Mirror PG and panic until it lands.
    let _ = (slot, tuple, should_free);
    panic!("execTuples.c tts_heap_store_tuple: storing the HeapTuple param into the slot's FormedTuple carrier needs the slot payload model's HeapTuple->FormedTuple bridge")
}

/// `tts_heap_copyslot` (execTuples.c:438): copy `srcslot` into a heap slot by
/// forming a heap tuple in the destination slot's context and storing it.
pub fn tts_heap_copyslot<'mcx>(
    _mcx: Mcx<'mcx>,
    dst: &mut SlotData<'mcx>,
    src: &SlotData<'mcx>,
) -> PgResult<()> {
    // oldcontext = MemoryContextSwitchTo(dstslot->tts_mcxt);
    // tuple = ExecCopySlotHeapTuple(srcslot);
    // MemoryContextSwitchTo(oldcontext);
    // ExecStoreHeapTuple(tuple, dstslot, true);
    //
    // ExecCopySlotHeapTuple yields a FormedTuple the slot's HeapTuple carrier
    // cannot hold, and forming it in dstslot->tts_mcxt is the slot payload
    // model's per-slot context. Mirror PG and panic.
    let _ = (dst, src);
    panic!("execTuples.c tts_heap_copyslot: ExecCopySlotHeapTuple (FormedTuple -> slot HeapTuple carrier) into dstslot->tts_mcxt depends on the slot payload model's tuple-carrier bridge")
}

// --- MinimalTupleTableSlot ops --------------------------------------------

/// `tts_minimal_init` (execTuples.c): point `tuple` at `minhdr` so the minimal
/// tuple's attributes can be accessed as if it were a heap tuple.
pub fn tts_minimal_init(_slot: &mut MinimalTupleTableSlot) {
    // mslot->tuple = &mslot->minhdr;
    //
    // In C the slot's `tuple` HeapTuple pointer is aimed at the in-struct
    // `minhdr` workspace, so later code reads attributes through `mslot->tuple`
    // (== &minhdr). In the owned model `tuple` and `minhdr` are separate owned
    // fields of MinimalTupleTableSlot; the deform/materialize paths read the
    // `minhdr` workspace field directly rather than through an aliased pointer,
    // so there is no pointer to set here. (The aliasing is a representation
    // detail of the C struct, not observable logic.)
}

/// `tts_minimal_release` (execTuples.c): empty body.
pub fn tts_minimal_release(_slot: &mut MinimalTupleTableSlot) {}

/// `tts_minimal_clear` (execTuples.c).
pub fn tts_minimal_clear(slot: &mut MinimalTupleTableSlot) {
    // if (TTS_SHOULDFREE(slot)) { heap_free_minimal_tuple(mslot->mintuple);
    //     slot->tts_flags &= ~TTS_FLAG_SHOULDFREE; }
    if slot.base.should_free() {
        // heap_free_minimal_tuple(mslot->mintuple): dropping frees it.
        slot.mintuple = None;
        slot.base.header.tts_flags &= !TTS_FLAG_SHOULDFREE;
    }

    // slot->tts_nvalid = 0;
    slot.base.tts_nvalid = 0;
    // slot->tts_flags |= TTS_FLAG_EMPTY;
    slot.base.header.tts_flags |= types_nodes::executor::TTS_FLAG_EMPTY;
    // ItemPointerSetInvalid(&slot->tts_tid);
    slot.base.header.tts_tid = types_tuple::heaptuple::ItemPointerData::default();
    // mslot->off = 0;
    slot.off = 0;
    // mslot->mintuple = NULL;
    slot.mintuple = None;
}

/// `tts_minimal_getsomeattrs` (execTuples.c):
/// `slot_deform_heap_tuple(slot, mslot->tuple, &mslot->off, natts)`.
pub fn tts_minimal_getsomeattrs<'mcx>(
    _mcx: Mcx<'mcx>,
    slot: &mut MinimalTupleTableSlot<'mcx>,
    _natts: i32,
) -> PgResult<()> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());

    // slot_deform_heap_tuple(slot, mslot->tuple, &mslot->off, natts);
    //
    // The minimal-tuple deform reads through `mslot->tuple` (aliased to
    // `minhdr`, whose t_data points MINIMAL_TUPLE_OFFSET before the minimal
    // tuple body). The sibling `slot_deform` family's `slot_deform_heap_tuple`
    // takes a `HeapTupleTableSlot`; the minimal slot's heap-tuple view
    // (minhdr/mintuple) is the slot payload model's workspace bridge. Route to
    // it once the minimal-slot heap-tuple view lands. Mirror PG and panic.
    panic!("execTuples.c tts_minimal_getsomeattrs: slot_deform_heap_tuple over the minimal slot's minhdr heap-tuple view (slot payload model workspace)")
}

/// `tts_minimal_getsysattr` (execTuples.c): `ereport(ERROR,
/// FEATURE_NOT_SUPPORTED, "cannot retrieve a system column in this context")`.
pub fn tts_minimal_getsysattr(
    slot: &MinimalTupleTableSlot,
    _attnum: i32,
) -> PgResult<(Datum, bool)> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());
    Err(feature_not_supported(
        "cannot retrieve a system column in this context",
    ))
}

/// `tts_minimal_is_current_xact_tuple` (execTuples.c): `ereport(ERROR,
/// FEATURE_NOT_SUPPORTED, "don't have transaction information for this type of
/// tuple")`.
pub fn tts_minimal_is_current_xact_tuple(slot: &MinimalTupleTableSlot) -> PgResult<bool> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());
    Err(feature_not_supported(
        "don't have transaction information for this type of tuple",
    ))
}

/// `tts_minimal_materialize` (execTuples.c).
pub fn tts_minimal_materialize<'mcx>(
    _mcx: Mcx<'mcx>,
    slot: &mut MinimalTupleTableSlot<'mcx>,
) -> PgResult<()> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());

    // /* If slot has its tuple already materialized, nothing to do. */
    // if (TTS_SHOULDFREE(slot)) return;
    if slot.base.should_free() {
        return Ok(());
    }

    // slot->tts_nvalid = 0; mslot->off = 0;
    slot.base.tts_nvalid = 0;
    slot.off = 0;

    // if (!mslot->mintuple) mslot->mintuple =
    //     heap_form_minimal_tuple(..., 0);
    // else mslot->mintuple = heap_copy_minimal_tuple(mslot->mintuple, 0);
    // slot->tts_flags |= TTS_FLAG_SHOULDFREE;
    // Assert(mslot->tuple == &mslot->minhdr);
    // mslot->minhdr.t_len = mslot->mintuple->t_len + MINIMAL_TUPLE_OFFSET;
    // mslot->minhdr.t_data = (HeapTupleHeader)((char*)mslot->mintuple -
    //     MINIMAL_TUPLE_OFFSET);
    //
    // heap_form_minimal_tuple/heap_copy_minimal_tuple produce a
    // FormedMinimalTuple the slot's MinimalTuple carrier can't hold, and the
    // minhdr.t_data fix-up is raw-pointer aliasing of the minimal tuple body
    // — both the slot payload model's. Mirror PG and panic.
    panic!("execTuples.c tts_minimal_materialize: heap_form/copy_minimal_tuple -> FormedMinimalTuple + minhdr.t_data alias fix-up are the slot payload model's carrier bridge")
}

/// `tts_minimal_get_minimal_tuple` (execTuples.c).
pub fn tts_minimal_get_minimal_tuple<'mcx>(
    _slot: &mut MinimalTupleTableSlot<'mcx>,
) -> PgResult<MinimalTuple<'mcx>> {
    // if (!mslot->mintuple) tts_minimal_materialize(slot);
    // return mslot->mintuple;
    //
    // Returning mslot->mintuple depends on tts_minimal_materialize being able
    // to store a FormedMinimalTuple in the slot's MinimalTuple carrier.
    panic!("execTuples.c tts_minimal_get_minimal_tuple: depends on tts_minimal_materialize's FormedMinimalTuple->MinimalTuple carrier (slot payload model)")
}

/// `tts_minimal_copy_minimal_tuple` (execTuples.c).
pub fn tts_minimal_copy_minimal_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut MinimalTupleTableSlot<'mcx>,
) -> PgResult<MinimalTuple<'mcx>> {
    // if (!mslot->mintuple) tts_minimal_materialize(slot);
    // return heap_copy_minimal_tuple(mslot->mintuple, extra);
    //
    // heap_copy_minimal_tuple yields a FormedMinimalTuple the slot's
    // MinimalTuple carrier can't hold; gated on tts_minimal_materialize.
    panic!("execTuples.c tts_minimal_copy_minimal_tuple: heap_copy_minimal_tuple -> FormedMinimalTuple carrier bridge + materialize are the slot payload model's")
}

/// `tts_minimal_copy_heap_tuple` (execTuples.c:659).
pub fn tts_minimal_copy_heap_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut MinimalTupleTableSlot<'mcx>,
) -> PgResult<HeapTuple<'mcx>> {
    // if (!mslot->mintuple) tts_minimal_materialize(slot);
    // return heap_tuple_from_minimal_tuple(mslot->mintuple);
    //
    // Gated on tts_minimal_materialize being able to store a
    // FormedMinimalTuple in the slot's MinimalTuple carrier;
    // heap_tuple_from_minimal_tuple then yields a FormedTuple the slot's
    // HeapTuple carrier cannot hold either — both the slot payload model's
    // carrier bridge. Mirror PG and panic.
    panic!("execTuples.c tts_minimal_copy_heap_tuple: heap_tuple_from_minimal_tuple -> FormedTuple carrier bridge + materialize are the slot payload model's")
}

/// `tts_minimal_store_tuple` (execTuples.c).
pub fn tts_minimal_store_tuple<'mcx>(
    slot: &mut MinimalTupleTableSlot<'mcx>,
    mtup: MinimalTuple<'mcx>,
    should_free: bool,
) {
    // tts_minimal_clear(slot);
    tts_minimal_clear(slot);

    // Assert(!TTS_SHOULDFREE(slot)); Assert(TTS_EMPTY(slot));
    debug_assert!(!slot.base.should_free());
    debug_assert!(slot.base.is_empty());

    // slot->tts_flags &= ~TTS_FLAG_EMPTY;
    slot.base.mark_not_empty();
    // slot->tts_nvalid = 0;
    slot.base.tts_nvalid = 0;
    // mslot->off = 0;
    slot.off = 0;

    // mslot->mintuple = mtup;
    // Assert(mslot->tuple == &mslot->minhdr);
    // mslot->minhdr.t_len = mtup->t_len + MINIMAL_TUPLE_OFFSET;
    // mslot->minhdr.t_data = (HeapTupleHeader)((char*)mtup - MINIMAL_TUPLE_OFFSET);
    // if (shouldFree) slot->tts_flags |= TTS_FLAG_SHOULDFREE;
    //
    // The expanded slot payload model carries the stored minimal tuple as the
    // body-bearing `FormedMinimalTuple` (header + data-area bytes), and
    // `mslot->minhdr`/`mslot->tuple` form the `FormedTuple`-shaped view whose
    // `t_data` aliases `MINIMAL_TUPLE_OFFSET` before the body. The public
    // `MinimalTuple` parameter is header-only, so wrapping it into the slot's
    // `FormedMinimalTuple` carrier + wiring the minhdr alias needs the carrier
    // bridge owned by the sibling store/fetch fill family. Mirror PG and panic.
    let _ = (slot, mtup, should_free);
    panic!("execTuples.c tts_minimal_store_tuple: storing the MinimalTuple param into the slot's FormedMinimalTuple carrier + minhdr alias needs the slot payload model's MinimalTuple->FormedMinimalTuple bridge")
}

/// `tts_minimal_copyslot` (execTuples.c:635): copy `srcslot` into a minimal
/// slot by forming a minimal tuple in the destination slot's context and
/// storing it.
pub fn tts_minimal_copyslot<'mcx>(
    _mcx: Mcx<'mcx>,
    dst: &mut SlotData<'mcx>,
    src: &SlotData<'mcx>,
) -> PgResult<()> {
    // oldcontext = MemoryContextSwitchTo(dstslot->tts_mcxt);
    // mintuple = ExecCopySlotMinimalTuple(srcslot);
    // MemoryContextSwitchTo(oldcontext);
    // ExecStoreMinimalTuple(mintuple, dstslot, true);
    //
    // ExecCopySlotMinimalTuple yields a FormedMinimalTuple the slot's
    // MinimalTuple carrier cannot hold, and forming it in dstslot->tts_mcxt is
    // the slot payload model's per-slot context. Mirror PG and panic.
    let _ = (dst, src);
    panic!("execTuples.c tts_minimal_copyslot: ExecCopySlotMinimalTuple (FormedMinimalTuple -> slot MinimalTuple carrier) into dstslot->tts_mcxt depends on the slot payload model's tuple-carrier bridge")
}

// --- BufferHeapTupleTableSlot ops -----------------------------------------

/// `tts_buffer_heap_init` (execTuples.c): empty body.
pub fn tts_buffer_heap_init(_slot: &mut BufferHeapTupleTableSlot) {}

/// `tts_buffer_heap_release` (execTuples.c): empty body.
pub fn tts_buffer_heap_release(_slot: &mut BufferHeapTupleTableSlot) {}

/// `tts_buffer_heap_clear` (execTuples.c).
pub fn tts_buffer_heap_clear(slot: &mut BufferHeapTupleTableSlot) {
    // if (TTS_SHOULDFREE(slot)) {
    //     Assert(!BufferIsValid(bslot->buffer));
    //     heap_freetuple(bslot->base.tuple);
    //     slot->tts_flags &= ~TTS_FLAG_SHOULDFREE; }
    if slot.base.base.should_free() {
        debug_assert!(!BufferIsValid(slot.buffer));
        // heap_freetuple(bslot->base.tuple): dropping frees it.
        slot.base.tuple = None;
        slot.base.base.header.tts_flags &= !TTS_FLAG_SHOULDFREE;
    }

    // if (BufferIsValid(bslot->buffer)) ReleaseBuffer(bslot->buffer);
    if BufferIsValid(slot.buffer) {
        backend_storage_buffer_bufmgr_seams::release_buffer::call(slot.buffer);
    }

    // slot->tts_nvalid = 0;
    slot.base.base.tts_nvalid = 0;
    // slot->tts_flags |= TTS_FLAG_EMPTY;
    slot.base.base.header.tts_flags |= types_nodes::executor::TTS_FLAG_EMPTY;
    // ItemPointerSetInvalid(&slot->tts_tid);
    slot.base.base.header.tts_tid = types_tuple::heaptuple::ItemPointerData::default();
    // bslot->base.tuple = NULL;
    slot.base.tuple = None;
    // bslot->base.off = 0;
    slot.base.off = 0;
    // bslot->buffer = InvalidBuffer;
    slot.buffer = InvalidBuffer;
}

/// `tts_buffer_heap_getsomeattrs` (execTuples.c):
/// `slot_deform_heap_tuple(slot, bslot->base.tuple, &bslot->base.off, natts)`.
pub fn tts_buffer_heap_getsomeattrs<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut BufferHeapTupleTableSlot<'mcx>,
    natts: i32,
) -> PgResult<()> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.base.is_empty());

    // slot_deform_heap_tuple(slot, bslot->base.tuple, &bslot->base.off, natts);
    // The buffer slot's heap-tuple view is its embedded HeapTupleTableSlot
    // (`bslot->base`); deform over that.
    slot_deform_heap_tuple(mcx, &mut slot.base, natts)
}

/// `tts_buffer_heap_getsysattr` (execTuples.c).
pub fn tts_buffer_heap_getsysattr<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &BufferHeapTupleTableSlot<'mcx>,
    attnum: i32,
) -> PgResult<(Datum, bool)> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.base.is_empty());

    // if (!bslot->base.tuple) ereport(ERROR, FEATURE_NOT_SUPPORTED,
    //     "cannot retrieve a system column in this context");
    let Some(tuple) = slot.base.tuple.as_ref() else {
        return Err(feature_not_supported(
            "cannot retrieve a system column in this context",
        ));
    };

    // return heap_getsysattr(bslot->base.tuple, attnum,
    //                        slot->tts_tupleDescriptor, isnull);
    let (value, isnull) =
        backend_access_common_heaptuple::heap_getsysattr(mcx, &tuple.tuple, attnum)?;
    Ok((tuple_value_to_datum(&value)?, isnull))
}

/// `tts_buffer_is_current_xact_tuple` (execTuples.c).
pub fn tts_buffer_is_current_xact_tuple(slot: &BufferHeapTupleTableSlot) -> PgResult<bool> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.base.is_empty());

    // if (!bslot->base.tuple) ereport(ERROR, FEATURE_NOT_SUPPORTED,
    //     "don't have a storage tuple in this context");
    let Some(tuple) = slot.base.tuple.as_ref() else {
        return Err(feature_not_supported(
            "don't have a storage tuple in this context",
        ));
    };

    // xmin = HeapTupleHeaderGetRawXmin(bslot->base.tuple->t_data);
    let header = tuple
        .tuple
        .t_data
        .as_ref()
        .ok_or_else(|| elog_error("tts_buffer_is_current_xact_tuple: tuple has no t_data"))?;
    let xmin = types_tuple::heaptuple::HeapTupleHeaderGetRawXmin(header);

    // return TransactionIdIsCurrentTransactionId(xmin);
    Ok(backend_access_transam_xact_seams::transaction_id_is_current_transaction_id::call(xmin))
}

/// `tts_buffer_heap_materialize` (execTuples.c).
pub fn tts_buffer_heap_materialize<'mcx>(
    _mcx: Mcx<'mcx>,
    slot: &mut BufferHeapTupleTableSlot<'mcx>,
) -> PgResult<()> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.base.is_empty());

    // /* If slot has its tuple already materialized, nothing to do. */
    // if (TTS_SHOULDFREE(slot)) return;
    if slot.base.base.should_free() {
        return Ok(());
    }

    // bslot->base.off = 0; slot->tts_nvalid = 0;
    slot.base.off = 0;
    slot.base.base.tts_nvalid = 0;

    // if (!bslot->base.tuple)
    //     bslot->base.tuple = heap_form_tuple(...);
    // else { bslot->base.tuple = heap_copytuple(bslot->base.tuple);
    //     if (likely(BufferIsValid(bslot->buffer))) ReleaseBuffer(bslot->buffer);
    //     bslot->buffer = InvalidBuffer; }
    // slot->tts_flags |= TTS_FLAG_SHOULDFREE;
    //
    // heap_form_tuple/heap_copytuple produce FormedTuple the slot's HeapTuple
    // carrier can't hold (slot payload model bridge). The buffer-release
    // bookkeeping is owned logic but is downstream of the tuple copy, which is
    // blocked. Mirror PG and panic.
    panic!("execTuples.c tts_buffer_heap_materialize: heap_form_tuple/heap_copytuple -> FormedTuple; the slot's HeapTuple carrier bridge is the slot payload model's")
}

/// `tts_buffer_heap_copyslot` (execTuples.c).
pub fn tts_buffer_heap_copyslot<'mcx>(
    _mcx: Mcx<'mcx>,
    dst: &mut SlotData<'mcx>,
    src: &SlotData<'mcx>,
) -> PgResult<()> {
    // if (dstslot->tts_ops != srcslot->tts_ops || TTS_SHOULDFREE(srcslot) ||
    //     !bsrcslot->base.tuple) {
    //     ExecClearTuple(dstslot); dstslot->tts_flags &= ~TTS_FLAG_EMPTY;
    //     bdstslot->base.tuple = ExecCopySlotHeapTuple(srcslot);
    //     dstslot->tts_flags |= TTS_FLAG_SHOULDFREE; }
    // else { tts_buffer_heap_store_tuple(dstslot, bsrcslot->base.tuple,
    //     bsrcslot->buffer, false);
    //     memcpy(&bdstslot->base.tupdata, bdstslot->base.tuple, sizeof(HeapTupleData));
    //     bdstslot->base.tuple = &bdstslot->base.tupdata; }
    //
    // Both arms require ExecCopySlotHeapTuple (FormedTuple -> slot HeapTuple
    // carrier) or the in-buffer-tuple sharing through the tupdata workspace
    // alias — both the slot payload model's tuple-carrier bridge. Mirror+panic.
    let _ = (dst, src);
    panic!("execTuples.c tts_buffer_heap_copyslot: ExecCopySlotHeapTuple / in-buffer tuple sharing through tupdata depend on the slot payload model's HeapTuple carrier bridge")
}

/// `tts_buffer_heap_get_heap_tuple` (execTuples.c).
pub fn tts_buffer_heap_get_heap_tuple<'mcx>(
    _slot: &mut BufferHeapTupleTableSlot<'mcx>,
) -> PgResult<HeapTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    // if (!bslot->base.tuple) tts_buffer_heap_materialize(slot);
    // return bslot->base.tuple;
    panic!("execTuples.c tts_buffer_heap_get_heap_tuple: depends on tts_buffer_heap_materialize's FormedTuple->HeapTuple carrier (slot payload model)")
}

/// `tts_buffer_heap_copy_heap_tuple` (execTuples.c).
pub fn tts_buffer_heap_copy_heap_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut BufferHeapTupleTableSlot<'mcx>,
) -> PgResult<HeapTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    // if (!bslot->base.tuple) tts_buffer_heap_materialize(slot);
    // return heap_copytuple(bslot->base.tuple);
    panic!("execTuples.c tts_buffer_heap_copy_heap_tuple: heap_copytuple -> FormedTuple carrier bridge + materialize are the slot payload model's")
}

/// `tts_buffer_heap_copy_minimal_tuple` (execTuples.c).
pub fn tts_buffer_heap_copy_minimal_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut BufferHeapTupleTableSlot<'mcx>,
) -> PgResult<MinimalTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    // if (!bslot->base.tuple) tts_buffer_heap_materialize(slot);
    // return minimal_tuple_from_heap_tuple(bslot->base.tuple, extra);
    panic!("execTuples.c tts_buffer_heap_copy_minimal_tuple: minimal_tuple_from_heap_tuple over FormedTuple + carrier bridge are the slot payload model's")
}

// --- helpers --------------------------------------------------------------

/// Project a `DeformedColumn`'s value back to a bare `Datum` word. A by-value
/// sys-attr (xmin/xmax/cmin/cmax/tableoid) is the word itself; a by-reference
/// sys-attr (the `ctid`/`tableoid`-pointer cases C returns as a
/// `PointerGetDatum`) cannot be represented as a `Datum(usize)` word in the
/// owned model — that pointer-Datum boundary is the slot payload model's
/// (the owned values cross as `TupleValue`, not raw words). Mirror PG (which
/// hands a raw pointer) and panic.
fn tuple_value_to_datum(value: &TupleValue<'_>) -> PgResult<Datum> {
    match value {
        TupleValue::ByVal(d) => Ok(*d),
        TupleValue::ByRef(_) => Err(elog_error(
            "slot getsysattr: by-reference system column cannot cross as a bare Datum word (slot payload model)",
        )),
    }
}

// --- Slot-level dispatch (slot->tts_ops->op(slot)) ------------------------

/// `slot->tts_ops->clear(slot)` dispatch.
pub fn slot_clear(slot: &mut SlotData) {
    match slot {
        SlotData::Virtual(s) => tts_virtual_clear(s),
        SlotData::Heap(s) => tts_heap_clear(s),
        SlotData::Minimal(s) => tts_minimal_clear(s),
        SlotData::BufferHeap(s) => tts_buffer_heap_clear(s),
    }
}

/// `slot->tts_ops->release(slot)` dispatch.
pub fn slot_release(slot: &mut SlotData) {
    match slot {
        SlotData::Virtual(s) => tts_virtual_release(s),
        SlotData::Heap(s) => tts_heap_release(s),
        SlotData::Minimal(s) => tts_minimal_release(s),
        SlotData::BufferHeap(s) => tts_buffer_heap_release(s),
    }
}

/// `slot->tts_ops->materialize(slot)` dispatch.
pub fn slot_materialize<'mcx>(mcx: Mcx<'mcx>, slot: &mut SlotData<'mcx>) -> PgResult<()> {
    match slot {
        SlotData::Virtual(s) => tts_virtual_materialize(mcx, s),
        SlotData::Heap(s) => tts_heap_materialize(mcx, s),
        SlotData::Minimal(s) => tts_minimal_materialize(mcx, s),
        SlotData::BufferHeap(s) => tts_buffer_heap_materialize(mcx, s),
    }
}

/// `slot->tts_ops->getsomeattrs(slot, natts)` dispatch.
pub fn slot_ops_getsomeattrs<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
    natts: i32,
) -> PgResult<()> {
    match slot {
        SlotData::Virtual(s) => tts_virtual_getsomeattrs(s, natts),
        SlotData::Heap(s) => tts_heap_getsomeattrs(mcx, s, natts),
        SlotData::Minimal(s) => tts_minimal_getsomeattrs(mcx, s, natts),
        SlotData::BufferHeap(s) => tts_buffer_heap_getsomeattrs(mcx, s, natts),
    }
}

/// `slot->tts_ops->getsysattr(slot, attnum, &isnull)` dispatch.
pub fn slot_getsysattr<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &SlotData<'mcx>,
    attnum: AttrNumber,
) -> PgResult<(Datum, bool)> {
    match slot {
        SlotData::Virtual(s) => tts_virtual_getsysattr(s, attnum as i32),
        SlotData::Heap(s) => tts_heap_getsysattr(mcx, s, attnum as i32),
        SlotData::Minimal(s) => tts_minimal_getsysattr(s, attnum as i32),
        SlotData::BufferHeap(s) => tts_buffer_heap_getsysattr(mcx, s, attnum as i32),
    }
}

/// `slot->tts_ops->is_current_xact_tuple(slot)` dispatch.
pub fn slot_is_current_xact_tuple(slot: &SlotData) -> PgResult<bool> {
    match slot {
        SlotData::Virtual(s) => tts_virtual_is_current_xact_tuple(s),
        SlotData::Heap(s) => tts_heap_is_current_xact_tuple(s),
        SlotData::Minimal(s) => tts_minimal_is_current_xact_tuple(s),
        SlotData::BufferHeap(s) => tts_buffer_is_current_xact_tuple(s),
    }
}

/// `slot->tts_ops->copyslot(dst, src)` dispatch (invoked on the destination).
pub fn slot_copyslot<'mcx>(
    mcx: Mcx<'mcx>,
    dst: &mut SlotData<'mcx>,
    src: &SlotData<'mcx>,
) -> PgResult<()> {
    // The copyslot callback is dispatched on the *destination* slot's ops
    // (C: dstslot->tts_ops->copyslot). Each of the four ops installs its own:
    //   TTSOpsVirtual.copyslot      = tts_virtual_copyslot
    //   TTSOpsHeapTuple.copyslot    = tts_heap_copyslot
    //   TTSOpsMinimalTuple.copyslot = tts_minimal_copyslot
    //   TTSOpsBufferHeap.copyslot   = tts_buffer_heap_copyslot
    match dst.kind() {
        types_nodes::TupleSlotKind::Virtual => tts_virtual_copyslot(mcx, dst, src),
        types_nodes::TupleSlotKind::HeapTuple => tts_heap_copyslot(mcx, dst, src),
        types_nodes::TupleSlotKind::MinimalTuple => tts_minimal_copyslot(mcx, dst, src),
        types_nodes::TupleSlotKind::BufferHeapTuple => tts_buffer_heap_copyslot(mcx, dst, src),
    }
}
