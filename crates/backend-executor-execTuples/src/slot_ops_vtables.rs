//! Family: slot-ops vtables — the per-kind `TupleTableSlotOps` callbacks for
//! the virtual/heap/minimal/buffer slot classes (execTuples.c
//! `tts_virtual_*` / `tts_heap_*` / `tts_minimal_*` / `tts_buffer_heap_*`),
//! plus the `Slot`-level dispatch that routes `slot->tts_ops->op(slot)` to the
//! right kind.
//!
//! Each callback takes a `&mut` to the concrete payload subtype (the analog of
//! the C downcast of `TupleTableSlot *`); allocating callbacks take `Mcx`.

extern crate alloc;

use mcx::{slice_in, vec_with_capacity_in, Mcx};
use types_core::primitive::AttrNumber;
use types_datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_nodes::tuptable::{
    BufferHeapTupleTableSlot, HeapTupleTableSlot, MinimalTupleTableSlot, SlotData,
    VirtualTupleTableSlot, TTS_FLAG_SHOULDFREE,
};
use types_storage::buf::{BufferIsValid, InvalidBuffer};
use types_tuple::backend_access_common_heaptuple::TupleValue;
use types_tuple::heaptuple::{CompactAttribute, HeapTuple, MinimalTuple};

use crate::slot_deform::slot_deform_heap_tuple;

// --- alignment / varlena-length helpers (tupmacs.h / varatt.h) ------------
//
// Mirror the same primitives used by `slot_deform` and
// `backend-access-common-heaptuple`'s form path; `tts_virtual_materialize`
// sizes/copies a by-reference column from its owned on-disk bytes the same way.

/// `TYPEALIGN(ALIGNVAL, LEN)` (c.h).
#[inline]
fn type_align(alignval: usize, len: usize) -> usize {
    (len + (alignval - 1)) & !(alignval - 1)
}

/// `att_nominal_alignby(cur_offset, attalignby)` (tupmacs.h):
/// `TYPEALIGN(attalignby, cur_offset)`.
#[inline]
fn att_nominal_alignby(cur_offset: usize, attalignby: u8) -> usize {
    type_align(attalignby as usize, cur_offset)
}

#[inline]
fn varatt_is_1b_e(b: &[u8]) -> bool {
    b[0] == 0x01
}
#[inline]
fn varatt_is_1b(b: &[u8]) -> bool {
    (b[0] & 0x01) == 0x01
}
#[inline]
fn varsize_1b(b: &[u8]) -> usize {
    ((b[0] >> 1) & 0x7F) as usize
}
#[inline]
fn varsize_4b(b: &[u8]) -> usize {
    let hdr = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
    ((hdr >> 2) & 0x3FFF_FFFF) as usize
}
#[inline]
fn vartag_size(tag: u8) -> usize {
    const VARTAG_INDIRECT: u8 = 1;
    const VARTAG_EXPANDED_RO: u8 = 2;
    const VARTAG_ONDISK: u8 = 18;
    if tag == VARTAG_INDIRECT {
        8
    } else if (tag & !1) == VARTAG_EXPANDED_RO {
        8
    } else if tag == VARTAG_ONDISK {
        16
    } else {
        0
    }
}
#[inline]
fn varsize_external(b: &[u8]) -> usize {
    2 + vartag_size(b[1])
}
/// `VARSIZE_ANY(ptr)` (varatt.h) for an in-line varlena starting at `b[0]`.
#[inline]
fn varsize_any(b: &[u8]) -> usize {
    if varatt_is_1b_e(b) {
        varsize_external(b)
    } else if varatt_is_1b(b) {
        varsize_1b(b)
    } else {
        varsize_4b(b)
    }
}

/// `VARATT_IS_EXTERNAL_EXPANDED(PTR)` (varatt.h): a 1-byte external varlena
/// (`0x01`) whose tag is an expanded-object tag (`VARTAG_EXPANDED_RO`/`_RW`).
#[inline]
fn varatt_is_external_expanded(b: &[u8]) -> bool {
    const VARTAG_EXPANDED_RO: u8 = 2;
    b.len() >= 2 && b[0] == 0x01 && ((b[1] & !1) == VARTAG_EXPANDED_RO)
}

/// `att_addlength_datum(cur_offset, attlen, attdatum)` (tupmacs.h) over the
/// by-reference column's owned bytes (C's `DatumGetPointer(attdatum)`).
#[inline]
fn att_addlength_datum(cur_offset: usize, attlen: i16, val: &[u8]) -> usize {
    if attlen > 0 {
        cur_offset + attlen as usize
    } else if attlen == -1 {
        cur_offset + varsize_any(val)
    } else {
        debug_assert_eq!(attlen, -2);
        // strlen + 1
        let mut len = 0usize;
        while val[len] != 0 {
            len += 1;
        }
        cur_offset + len + 1
    }
}

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
    mcx: Mcx<'mcx>,
    slot: &mut VirtualTupleTableSlot<'mcx>,
) -> PgResult<()> {
    // VirtualTupleTableSlot *vslot = (VirtualTupleTableSlot *) slot;
    // TupleDesc desc = slot->tts_tupleDescriptor;
    // Size sz = 0;
    // char *data;

    // /* already materialized */
    // if (TTS_SHOULDFREE(slot)) return;
    if slot.base.should_free() {
        return Ok(());
    }

    // Snapshot the descriptor's compact attrs (read-only) so we don't hold a
    // borrow of the descriptor while mutating tts_values/vslot->data below.
    let compact_attrs: alloc::vec::Vec<CompactAttribute> = slot
        .base
        .tts_tupleDescriptor
        .as_ref()
        .expect("tts_virtual_materialize: slot has no tuple descriptor")
        .compact_attrs
        .iter()
        .copied()
        .collect();
    let natts = compact_attrs.len();

    // /* compute size of memory required */
    // for (int natt = 0; natt < desc->natts; natt++) { ... }
    //
    // The slot's tts_values now carry a `TupleValue`: a by-value column is
    // `ByVal` (skipped here, exactly as C skips `att->attbyval`), a
    // by-reference column is `ByRef` over the verbatim on-disk bytes (C's
    // `DatumGetPointer(val)`). Sizing/copying reads from those owned ByRef
    // bytes via the engine's own att_nominal_alignby / att_addlength_datum.
    let mut sz: usize = 0;
    for natt in 0..natts {
        let att = &compact_attrs[natt];

        // if (att->attbyval || slot->tts_isnull[natt]) continue;
        if att.attbyval || slot.base.tts_isnull[natt] {
            continue;
        }

        // val = slot->tts_values[natt]; (by-reference => owned bytes)
        let val = slot.base.tts_values[natt].as_ref_bytes();

        if att.attlen == -1 && varatt_is_external_expanded(val) {
            // /* flatten the expanded value so the materialized slot doesn't
            //  * depend on it. */
            // sz = att_nominal_alignby(sz, att->attalignby);
            // sz += EOH_get_flat_size(DatumGetEOHP(val));
            sz = att_nominal_alignby(sz, att.attalignby);
            sz += backend_utils_adt_misc2_seams::eoh_get_flat_size::call(
                types_datum::ExpandedObjectRef::from_expanded_datum_bytes(val),
            )?;
        } else {
            // sz = att_nominal_alignby(sz, att->attalignby);
            // sz = att_addlength_datum(sz, att->attlen, val);
            sz = att_nominal_alignby(sz, att.attalignby);
            sz = att_addlength_datum(sz, att.attlen, val);
        }
    }

    // /* all data is byval */
    // if (sz == 0) return;
    if sz == 0 {
        return Ok(());
    }

    // /* allocate memory */
    // vslot->data = data = MemoryContextAlloc(slot->tts_mcxt, sz);
    // slot->tts_flags |= TTS_FLAG_SHOULDFREE;
    //
    // tts_mcxt == the slot's context, modeled by `mcx`. C leaves the bytes
    // uninitialized (MemoryContextAlloc); we zero-fill so the alignment pad
    // bytes between fields are deterministic (behaviour-preserving — only the
    // copied field spans are read back).
    let mut data: mcx::PgVec<'mcx, u8> = vec_with_capacity_in(mcx, sz)?;
    data.resize(sz, 0u8);
    slot.base.header.tts_flags |= TTS_FLAG_SHOULDFREE;

    // /* and copy all attributes into the pre-allocated space */
    // The C cursor `data` walks the buffer; here `cur` is the byte offset into
    // `data`, and after copying a field we repoint
    // `slot->tts_values[natt] = PointerGetDatum(data)` — in the owned model the
    // re-pointed value is a fresh `ByRef` over the materialized field bytes
    // (the slot now owns them in `vslot->data`, no external dependency).
    let mut cur: usize = 0;
    for natt in 0..natts {
        let att = &compact_attrs[natt];

        // if (att->attbyval || slot->tts_isnull[natt]) continue;
        if att.attbyval || slot.base.tts_isnull[natt] {
            continue;
        }

        // val = slot->tts_values[natt];
        // (clone the source bytes so we can borrow `data` mutably below)
        let val: alloc::vec::Vec<u8> = slot.base.tts_values[natt].as_ref_bytes().to_vec();

        let data_length: usize;
        if att.attlen == -1 && varatt_is_external_expanded(&val) {
            // ExpandedObjectHeader *eoh = DatumGetEOHP(val);
            // data = (char *) att_nominal_alignby(data, att->attalignby);
            // data_length = EOH_get_flat_size(eoh);
            // EOH_flatten_into(eoh, data, data_length);
            let eoh = types_datum::ExpandedObjectRef::from_expanded_datum_bytes(&val);
            cur = att_nominal_alignby(cur, att.attalignby);
            data_length = backend_utils_adt_misc2_seams::eoh_get_flat_size::call(eoh)?;
            backend_utils_adt_misc2_seams::eoh_flatten_into::call(
                eoh,
                &mut data[cur..cur + data_length],
            )?;
        } else {
            // data = (char *) att_nominal_alignby(data, att->attalignby);
            // data_length = att_addlength_datum(0, att->attlen, val);
            // memcpy(data, DatumGetPointer(val), data_length);
            cur = att_nominal_alignby(cur, att.attalignby);
            data_length = att_addlength_datum(0, att.attlen, &val);
            data[cur..cur + data_length].copy_from_slice(&val[..data_length]);
        }

        // slot->tts_values[natt] = PointerGetDatum(data);
        // (re-point at the materialized field bytes — now owned by the slot)
        slot.base.tts_values[natt] =
            TupleValue::ByRef(slice_in(mcx, &data[cur..cur + data_length])?);

        // data += data_length;
        cur += data_length;
    }

    // vslot->data = data (the materialized buffer is now the slot's own).
    slot.data = data;

    Ok(())
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
    // into its tts_values/tts_isnull (C mutates srcslot here, caching the
    // deformed values). The shared deform path (`slot_deform::slot_getallattrs`)
    // is fully ported, but it needs `&mut SlotData` to write the cache, whereas
    // this callback receives `src: &SlotData<'mcx>` — the `copyslot(dst, src)`
    // contract fixed at the keystone hands the source immutably (matching the
    // dispatch `slot_copyslot` / `ExecCopySlot`). A virtual source is already
    // fully valid (`tts_nvalid == natts`, nothing to deform), so the copy below
    // is correct for it; a non-virtual source would require deforming through
    // the immutable borrow, which the callback signature forbids. Migrating the
    // copyslot src to `&mut` is a cross-family contract change (the dispatch +
    // store/fetch consumers), out of this family's scope — so the non-virtual
    // source arm stays mirror-PG-and-panic on that signature contract.
    if !matches!(src, SlotData::Virtual(_)) {
        panic!("execTuples.c tts_virtual_copyslot: slot_getallattrs(srcslot) on a non-virtual source needs a &mut source borrow the copyslot(dst, &src) callback contract (fixed at the keystone) does not grant")
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
    // The form-tuple body is itself ready: the slot's `tts_values` now carry
    // `TupleValue`s (the by-ref lane heap_form_tuple consumes), and
    // `backend_access_common_heaptuple::heap_form_tuple` returns the body-bearing
    // `FormedTuple`. The block is purely the RETURN-TYPE contract: this callback
    // (and its consumers `ExecCopySlotHeapTuple` / `ExecFetchSlotHeapTuple` in
    // the store/fetch family) still return the header-only
    // `HeapTuple = Option<PgBox<HeapTupleData>>`, which cannot hold a
    // `FormedTuple`'s data-area bytes. Migrating this callback's return to
    // `Option<FormedTuple>` requires the same migration across the store/fetch
    // family's dispatch (and the sibling heap/minimal/buffer `copy_heap_tuple`
    // callbacks, all still on the header-only type) — a cross-family contract
    // change the keystone did not perform and this virtual-ops family cannot
    // make alone. Mirror PG and panic until that return contract is migrated.
    let _ = slot;
    panic!("execTuples.c tts_virtual_copy_heap_tuple: heap_form_tuple body is ready, but the callback's header-only HeapTuple return type cannot carry the FormedTuple's data bytes; migrating it to Option<FormedTuple> is a cross-family (store/fetch dispatch) contract change the keystone left unmade")
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
    //
    // Same return-type contract block as tts_virtual_copy_heap_tuple: the
    // `heap_form_minimal_tuple` body over the slot's `TupleValue` tts_values is
    // ready and yields a body-bearing `FormedMinimalTuple`, but this callback
    // (and `ExecCopySlotMinimalTupleExtra` / `ExecFetchSlotMinimalTuple` in the
    // store/fetch family) still return the header-only `MinimalTuple`, which
    // can't carry the body bytes. Migrating to `Option<FormedMinimalTuple>` is a
    // cross-family contract change the keystone did not perform. Mirror+panic.
    let _ = slot;
    panic!("execTuples.c tts_virtual_copy_minimal_tuple: heap_form_minimal_tuple body is ready, but the callback's header-only MinimalTuple return type cannot carry the FormedMinimalTuple's body bytes; migrating it to Option<FormedMinimalTuple> is a cross-family (store/fetch dispatch) contract change the keystone left unmade")
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
    mcx: Mcx<'mcx>,
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
    // /* Have to deform from scratch, otherwise tts_values[] entries could
    //  * point into the non-materialized tuple ... */
    // slot->tts_nvalid = 0; hslot->off = 0;
    slot.base.tts_nvalid = 0;
    slot.off = 0;

    // if (!hslot->tuple)
    //     hslot->tuple = heap_form_tuple(slot->tts_tupleDescriptor,
    //         slot->tts_values, slot->tts_isnull);
    // else
    //     hslot->tuple = heap_copytuple(hslot->tuple);
    //
    // The expanded slot payload model carries the materialized tuple as the
    // body-bearing `FormedTuple` (header + data-area bytes), so both arms store
    // their `FormedTuple` result directly into `hslot->tuple`. `tts_values` now
    // carries `TupleValue`s (the by-ref lane owns the bytes), exactly what
    // heap_form_tuple consumes; the `'mcx` allocation context is the slot's
    // memory context (C's MemoryContextSwitchTo(slot->tts_mcxt)).
    if slot.tuple.is_none() {
        // hslot->tuple = heap_form_tuple(...)
        let desc = slot
            .base
            .tts_tupleDescriptor
            .as_ref()
            .ok_or_else(|| elog_error("tts_heap_materialize: slot has no tuple descriptor"))?;
        let formed = backend_access_common_heaptuple::heap_form_tuple(
            mcx,
            desc,
            slot.base.tts_values.as_slice(),
            slot.base.tts_isnull.as_slice(),
        )?;
        slot.tuple = Some(formed);
    } else {
        // hslot->tuple = heap_copytuple(hslot->tuple): copy the (foreign-context)
        // tuple into the slot's context.
        let copied = backend_access_common_heaptuple::heap_copytuple(mcx, slot.tuple.as_ref())?;
        slot.tuple = copied;
    }

    // slot->tts_flags |= TTS_FLAG_SHOULDFREE;
    slot.base.header.tts_flags |= TTS_FLAG_SHOULDFREE;

    // MemoryContextSwitchTo(oldContext);
    Ok(())
}

/// `tts_heap_get_heap_tuple` (execTuples.c).
pub fn tts_heap_get_heap_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut HeapTupleTableSlot<'mcx>,
) -> PgResult<HeapTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());

    // if (!hslot->tuple) tts_heap_materialize(slot);
    if slot.tuple.is_none() {
        tts_heap_materialize(mcx, slot)?;
    }

    // return hslot->tuple;
    //
    // C returns `hslot->tuple` (a HeapTuple whose t_data points at the
    // materialized contiguous image). In the split payload model the carrier is
    // a body-bearing `FormedTuple`; the public `get_heap_tuple` contract is the
    // header-only `HeapTuple` view, so we hand back a clone of the carrier's
    // header (`FormedTuple.tuple`). The data-area bytes (`FormedTuple.data`)
    // are not representable in the `HeapTuple` return; widening that fetch-path
    // return to carry the body is the sibling store/fetch family's contract
    // reconcile (its consumers — heap_copy_tuple_as_datum etc. — are still the
    // panicking carrier-bridge stubs). The header itself is faithful.
    match slot.tuple.as_ref() {
        Some(formed) => Ok(Some(mcx::alloc_in(mcx, formed.tuple.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

/// `tts_heap_copy_heap_tuple` (execTuples.c).
pub fn tts_heap_copy_heap_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut HeapTupleTableSlot<'mcx>,
) -> PgResult<HeapTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());

    // if (!hslot->tuple) tts_heap_materialize(slot);
    if slot.tuple.is_none() {
        tts_heap_materialize(mcx, slot)?;
    }

    // return heap_copytuple(hslot->tuple);
    //
    // heap_copytuple deep-copies the carried `FormedTuple` (header + data area)
    // into `mcx` (C: the caller's current context). The public
    // `copy_heap_tuple` contract is the header-only `HeapTuple` view, so the
    // copy's data-area bytes (`FormedTuple.data`) are dropped at this return
    // boundary — that fetch-path body widening is the sibling store/fetch
    // family's contract reconcile. The header copy itself is faithful.
    let copied = backend_access_common_heaptuple::heap_copytuple(mcx, slot.tuple.as_ref())?;
    Ok(copied.map(|formed| formed.tuple))
}

/// `tts_heap_copy_minimal_tuple` (execTuples.c).
pub fn tts_heap_copy_minimal_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut HeapTupleTableSlot<'mcx>,
) -> PgResult<MinimalTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());

    // if (!hslot->tuple) tts_heap_materialize(slot);
    if slot.tuple.is_none() {
        tts_heap_materialize(mcx, slot)?;
    }

    // return minimal_tuple_from_heap_tuple(hslot->tuple, extra);
    //
    // minimal_tuple_from_heap_tuple builds a FormedMinimalTuple from the carried
    // FormedTuple (header + data area). `extra` is reserved leading bytes; the
    // family's copy_minimal_tuple callback contract carries no `extra` param
    // (ExecCopySlotMinimalTupleExtra drops it at dispatch — the `extra` plumbing
    // is a separate, pre-existing family gap), so we mirror the common
    // ExecCopySlotMinimalTuple(slot) path with `extra == 0`. The public
    // `copy_minimal_tuple` contract is the header-only `MinimalTuple` view, so
    // the result's data-area bytes are dropped at this return boundary — the
    // fetch-path body widening is the sibling store/fetch family's reconcile.
    let formed = slot
        .tuple
        .as_ref()
        .ok_or_else(|| elog_error("tts_heap_copy_minimal_tuple: tuple not materialized"))?;
    let mtup = backend_access_common_heaptuple::minimal_tuple_from_heap_tuple(mcx, formed, 0)?;
    Ok(Some(mtup.tuple))
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
    // The copy leg (ExecCopySlotHeapTuple -> the src's copy_heap_tuple) is now
    // real, but the store leg `ExecStoreHeapTuple(tuple, dstslot, true)` routes
    // into `tts_heap_store_tuple`, whose job is to wrap the header-only
    // `HeapTuple` parameter back into the slot's body-bearing `FormedTuple`
    // carrier. That HeapTuple->FormedTuple store bridge is owned by the sibling
    // store/fetch fill family and is still a mirror-PG-and-panic stub on this
    // branch; until it lands, the chained store cannot complete. (The copyslot
    // dispatch also hands `srcslot` as `&SlotData`, while ExecCopySlotHeapTuple
    // needs `&mut` to materialize-on-demand — that, too, is the store/fetch
    // family's signature reconcile.) Mirror PG and panic on the unported store
    // dependency rather than restructure around it.
    let _ = (dst, src);
    panic!("execTuples.c tts_heap_copyslot: ExecStoreHeapTuple -> tts_heap_store_tuple (HeapTuple->FormedTuple store carrier bridge) is the sibling store/fetch family's still-unported stub on this branch")
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
    mcx: Mcx<'mcx>,
    slot: &mut MinimalTupleTableSlot<'mcx>,
    natts: i32,
) -> PgResult<()> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());

    // slot_deform_heap_tuple(slot, mslot->tuple, &mslot->off, natts);
    //
    // C passes `mslot->tuple` (the `&mslot->minhdr` heap-tuple view, whose
    // `t_data` points MINIMAL_TUPLE_OFFSET before the minimal body) and
    // `&mslot->off` explicitly. The sibling `slot_deform` family's
    // `slot_deform_heap_tuple` reads those three pieces — `base`, `tuple`,
    // `off` — off a `HeapTupleTableSlot`. The minimal slot carries the
    // structurally identical `base`/`tuple`/`off`, so we briefly host them in a
    // stack `HeapTupleTableSlot` view (moving `minhdr` in as the unused
    // `tupdata` workspace), deform through it, then move the (now-mutated)
    // state back. This is exactly C's "deform through mslot->tuple" with the
    // heap-slot fields named differently on the two structs.
    deform_minimal_through_heap_view(mcx, slot, natts)
}

/// Drive `slot_deform_heap_tuple` over a minimal slot's heap-tuple view by
/// temporarily hosting its `base`/`tuple`/`off` in a `HeapTupleTableSlot`
/// (C: `slot_deform_heap_tuple(slot, mslot->tuple, &mslot->off, natts)`). The
/// `minhdr` workspace doubles as the borrowed slot's unused `tupdata`.
///
/// The keystone gave `slot_deform_heap_tuple` a `&mut HeapTupleTableSlot`
/// receiver (it reads `slot.base`/`slot.tuple`/`slot.off`), while the minimal
/// slot keeps the structurally identical fields under different names. We move
/// those fields into a stack `HeapTupleTableSlot` for the duration of the
/// deform and move them back afterwards. A `Drop` guard performs the move-back
/// so the transfer is sound even when the deform unwinds (the current
/// `slot_deform` body unwinds via its still-pending `heap_slot_body` carrier):
/// the guard restores `slot` exactly once, and the consumed `borrowed` never
/// drops the moved-out fields.
fn deform_minimal_through_heap_view<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut MinimalTupleTableSlot<'mcx>,
    natts: i32,
) -> PgResult<()> {
    // Guard that owns the borrowed HeapTupleTableSlot view and, on drop (normal
    // or unwinding), moves base/tuple/off/minhdr back into the minimal slot.
    struct ViewGuard<'a, 'mcx> {
        slot: &'a mut MinimalTupleTableSlot<'mcx>,
        borrowed: core::mem::ManuallyDrop<HeapTupleTableSlot<'mcx>>,
    }
    impl<'a, 'mcx> Drop for ViewGuard<'a, 'mcx> {
        fn drop(&mut self) {
            // Move the (possibly-mutated) view fields out of `borrowed` exactly
            // once and back into `slot`. `borrowed` is ManuallyDrop, so the
            // taken fields are not double-dropped; `tupdata` returns to `minhdr`.
            let taken = unsafe { core::mem::ManuallyDrop::take(&mut self.borrowed) };
            let HeapTupleTableSlot {
                base,
                tuple,
                off,
                tupdata,
            } = taken;
            // The `slot` fields are still the originals we ptr::read out of
            // below; overwrite them without dropping the (logically moved-out)
            // stale copies.
            unsafe {
                core::ptr::write(&mut self.slot.base, base);
                core::ptr::write(&mut self.slot.tuple, tuple);
                core::ptr::write(&mut self.slot.minhdr, tupdata);
            }
            self.slot.off = off;
        }
    }

    // Move the minimal slot's heap-tuple-view fields into the borrowed slot.
    // SlotBase/FormedTuple have no Default to `mem::replace` with, so we
    // bit-copy them out; the guard's Drop is what makes the borrow sound by
    // writing them back before the stale `slot` copies can be observed/dropped.
    let base = unsafe { core::ptr::read(&slot.base) };
    let tuple = unsafe { core::ptr::read(&slot.tuple) };
    let off = slot.off;
    let minhdr = unsafe { core::ptr::read(&slot.minhdr) };

    let mut guard = ViewGuard {
        slot,
        borrowed: core::mem::ManuallyDrop::new(HeapTupleTableSlot {
            base,
            tuple,
            off,
            tupdata: minhdr,
        }),
    };

    // slot_deform_heap_tuple(slot, mslot->tuple, &mslot->off, natts).
    slot_deform_heap_tuple(mcx, &mut guard.borrowed, natts)
    // `guard` drops here (normal return or `?`-propagated/unwinding error),
    // moving the mutated view back into the minimal slot.
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
    mcx: Mcx<'mcx>,
    slot: &mut MinimalTupleTableSlot<'mcx>,
) -> PgResult<()> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());

    // /* If slot has its tuple already materialized, nothing to do. */
    // if (TTS_SHOULDFREE(slot)) return;
    if slot.base.should_free() {
        return Ok(());
    }

    // oldContext = MemoryContextSwitchTo(slot->tts_mcxt); — every allocation
    // below is in `mcx`, the slot's context (the owned model's MemoryContext).

    // /* Have to deform from scratch ... */
    // slot->tts_nvalid = 0; mslot->off = 0;
    slot.base.tts_nvalid = 0;
    slot.off = 0;

    // if (!mslot->mintuple)
    //     mslot->mintuple = heap_form_minimal_tuple(slot->tts_tupleDescriptor,
    //                           slot->tts_values, slot->tts_isnull, 0);
    // else
    //     mslot->mintuple = heap_copy_minimal_tuple(mslot->mintuple, 0);
    let mintuple = match slot.mintuple.as_ref() {
        None => {
            let desc = slot
                .base
                .tts_tupleDescriptor
                .as_ref()
                .ok_or_else(|| elog_error("tts_minimal_materialize: slot has no tuple descriptor"))?;
            backend_access_common_heaptuple::heap_form_minimal_tuple(
                mcx,
                desc,
                slot.base.tts_values.as_slice(),
                slot.base.tts_isnull.as_slice(),
                0,
            )?
        }
        Some(mintuple) => {
            // The minimal tuple is not in the slot's context (else SHOULDFREE
            // would be set); copy it in.
            backend_access_common_heaptuple::heap_copy_minimal_tuple(mcx, mintuple, 0)?
        }
    };

    // slot->tts_flags |= TTS_FLAG_SHOULDFREE;
    slot.base.header.tts_flags |= TTS_FLAG_SHOULDFREE;

    // Assert(mslot->tuple == &mslot->minhdr);
    // mslot->minhdr.t_len = mslot->mintuple->t_len + MINIMAL_TUPLE_OFFSET;
    // mslot->minhdr.t_data = (HeapTupleHeader)((char*)mslot->mintuple -
    //     MINIMAL_TUPLE_OFFSET);
    //
    // Wire the minhdr / tuple FormedTuple-shaped view over the freshly-owned
    // minimal body and store the carrier.
    slot.mintuple = Some(mintuple);
    set_minimal_minhdr_view(mcx, slot)?;

    // MemoryContextSwitchTo(oldContext);
    Ok(())
}

/// Wire `mslot->minhdr` / `mslot->tuple` as the heap-tuple view over the slot's
/// owned `mslot->mintuple` (execTuples.c's
/// `minhdr.t_len = mintuple->t_len + MINIMAL_TUPLE_OFFSET; minhdr.t_data =
/// (char*)mintuple - MINIMAL_TUPLE_OFFSET`).
///
/// C aliases `minhdr.t_data` MINIMAL_TUPLE_OFFSET bytes before the minimal body
/// so `(char*)minhdr.t_data + minhdr.t_hoff` lands on the body. In the owned
/// model the body bytes travel as `FormedTuple::data`; the equivalent
/// heap-tuple view is `heap_tuple_from_minimal_tuple(mintuple)` — a
/// HeapTupleData header (t_len = mintuple.t_len + MINIMAL_TUPLE_OFFSET, sharing
/// the minimal tuple's infomask/natts/t_bits/t_hoff tail, system columns zeroed)
/// over the minimal body. We mirror its `minhdr` into the workspace field and
/// hand the same header+body to `slot.tuple`.
fn set_minimal_minhdr_view<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut MinimalTupleTableSlot<'mcx>,
) -> PgResult<()> {
    let mintuple = slot
        .mintuple
        .as_ref()
        .ok_or_else(|| elog_error("set_minimal_minhdr_view: slot has no minimal tuple"))?;

    let view = backend_access_common_heaptuple::heap_tuple_from_minimal_tuple(mcx, mintuple)?;

    // mslot->minhdr (the workspace HeapTupleData header `tuple` aliases).
    slot.minhdr = view.tuple.as_ref().clone();
    // mslot->tuple = &mslot->minhdr — the heap-tuple-shaped view over the body.
    slot.tuple = Some(view);
    Ok(())
}

/// `tts_minimal_get_minimal_tuple` (execTuples.c).
pub fn tts_minimal_get_minimal_tuple<'mcx>(
    _slot: &mut MinimalTupleTableSlot<'mcx>,
) -> PgResult<MinimalTuple<'mcx>> {
    // if (!mslot->mintuple) tts_minimal_materialize(slot);
    // return mslot->mintuple;
    //
    // STOP / CONTRACT-DIVERGENCE: the keystone left this op's public return type
    // header-only (`MinimalTuple = Option<PgBox<MinimalTupleData>>`, no body
    // bytes). The slot now carries the body-bearing `FormedMinimalTuple`, so the
    // faithful return must be `FormedMinimalTuple` — but widening this return
    // forces widening the sibling `slot_store_fetch` entry point
    // `ExecFetchSlotMinimalTuple` (and `ExecCopySlotMinimalTupleExtra`), which
    // are public and consumed by 8 other executor crates (nodeHash, nodeAgg,
    // nodeHashjoin, nodeSetOp, nodeMemoize). That cross-crate contract change is
    // out of this family's scope and is the keystone's reserved carrier bridge
    // ("Genuine ... MinimalTuple->FormedMinimalTuple carrier-bridge bodies ...
    // stay mirror-PG-and-panic for the fill agents"). Mirror PG and panic.
    panic!("execTuples.c tts_minimal_get_minimal_tuple: keystone left the op return header-only (MinimalTuple); the body-bearing FormedMinimalTuple return widens the public ExecFetchSlotMinimalTuple consumed by 8 crates — keystone-reserved carrier bridge")
}

/// `tts_minimal_copy_minimal_tuple` (execTuples.c).
pub fn tts_minimal_copy_minimal_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut MinimalTupleTableSlot<'mcx>,
) -> PgResult<MinimalTuple<'mcx>> {
    // if (!mslot->mintuple) tts_minimal_materialize(slot);
    // return heap_copy_minimal_tuple(mslot->mintuple, extra);
    //
    // STOP / CONTRACT-DIVERGENCE: same as tts_minimal_get_minimal_tuple —
    // heap_copy_minimal_tuple yields a body-bearing `FormedMinimalTuple`, but the
    // keystone left this op's public return header-only, and widening it widens
    // the public `ExecCopySlotMinimalTupleExtra` consumed across crates. Keystone-
    // reserved carrier bridge; mirror PG and panic.
    panic!("execTuples.c tts_minimal_copy_minimal_tuple: keystone left the op return header-only (MinimalTuple); the FormedMinimalTuple result widens the public ExecCopySlotMinimalTupleExtra — keystone-reserved carrier bridge")
}

/// `tts_minimal_copy_heap_tuple` (execTuples.c:659).
pub fn tts_minimal_copy_heap_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut MinimalTupleTableSlot<'mcx>,
) -> PgResult<HeapTuple<'mcx>> {
    // if (!mslot->mintuple) tts_minimal_materialize(slot);
    // return heap_tuple_from_minimal_tuple(mslot->mintuple);
    //
    // STOP / CONTRACT-DIVERGENCE: heap_tuple_from_minimal_tuple yields a
    // body-bearing `FormedTuple`, but the keystone left this op's public return
    // header-only (`HeapTuple`); widening it widens the public
    // `ExecFetchSlotHeapTuple`/`ExecCopySlotHeapTuple` consumed across crates.
    // Keystone-reserved carrier bridge; mirror PG and panic.
    panic!("execTuples.c tts_minimal_copy_heap_tuple: keystone left the op return header-only (HeapTuple); the FormedTuple result widens the public ExecFetchSlotHeapTuple/ExecCopySlotHeapTuple — keystone-reserved carrier bridge")
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
    // STOP / CONTRACT-DIVERGENCE: the faithful body is
    // `ExecStoreMinimalTuple(ExecCopySlotMinimalTupleExtra(src, 0), dst, true)`,
    // but (1) `ExecCopySlotMinimalTuple` needs `&mut srcslot` (it may
    // materialize the source) while the keystone copyslot callback hands `src`
    // as `&SlotData` (immutable), and (2) the store leg routes through
    // `tts_minimal_store_tuple`, whose `MinimalTuple` (header-only) param is the
    // keystone-reserved MinimalTuple->FormedMinimalTuple carrier bridge that
    // still panics. Both are keystone-contract blockers outside this family.
    // Mirror PG and panic.
    let _ = (dst, src);
    panic!("execTuples.c tts_minimal_copyslot: keystone copyslot gives src as &SlotData but ExecCopySlotMinimalTuple needs &mut to materialize; the store leg's tts_minimal_store_tuple is the keystone-reserved MinimalTuple->FormedMinimalTuple carrier bridge")
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
