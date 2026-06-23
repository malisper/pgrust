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
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use ::nodes::tuptable::{
    BufferHeapTupleTableSlot, HeapTupleTableSlot, MinimalTupleTableSlot, SlotData,
    VirtualTupleTableSlot, TTS_FLAG_SHOULDFREE,
};
use types_storage::buf::{BufferIsValid, InvalidBuffer};
// The canonical value enum; `Datum` is its transitional alias.
use types_tuple::heaptuple::{Datum, FormedMinimalTuple, FormedTuple};
use types_tuple::heaptuple::CompactAttribute;

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
        slot.base.tts_flags &= !TTS_FLAG_SHOULDFREE;
    }

    // slot->tts_nvalid = 0;
    slot.base.tts_nvalid = 0;
    // slot->tts_flags |= TTS_FLAG_EMPTY;
    slot.base.tts_flags |= ::nodes::executor::TTS_FLAG_EMPTY;
    // ItemPointerSetInvalid(&slot->tts_tid);
    slot.base.tts_tid = types_tuple::heaptuple::ItemPointerData::invalid();
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
pub fn tts_virtual_getsysattr<'mcx>(
    slot: &VirtualTupleTableSlot,
    _attnum: i32,
) -> PgResult<(Datum<'mcx>, bool)> {
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
    // The slot's tts_values now carry a `Datum`: a by-value column is
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
        // A composite-typed column (attlen == -1) may carry an owned `Composite`
        // Datum (e.g. a wholerow Var / ROW() result projected into a virtual
        // slot, as in DELETE ... RETURNING). C always has a flat varlena pointer
        // here; serialize the composite into its flat datum image first
        // (as_varlena_bytes), matching heap_form_tuple's fill_val path.
        let val_cow = slot.base.tts_values[natt].as_varlena_bytes();
        let val = val_cow.as_ref();

        if att.attlen == -1 && varatt_is_external_expanded(val) {
            // /* flatten the expanded value so the materialized slot doesn't
            //  * depend on it. */
            // sz = att_nominal_alignby(sz, att->attalignby);
            // sz += EOH_get_flat_size(DatumGetEOHP(val));
            sz = att_nominal_alignby(sz, att.attalignby);
            sz += misc2_seams::eoh_get_flat_size::call(
                datum::ExpandedObjectRef::from_expanded_datum_bytes(val),
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
    slot.base.tts_flags |= TTS_FLAG_SHOULDFREE;

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
        // (clone the source bytes so we can borrow `data` mutably below; a
        // `Composite` Datum is serialized to its flat image by as_varlena_bytes)
        let val: alloc::vec::Vec<u8> =
            slot.base.tts_values[natt].as_varlena_bytes().into_owned();

        let data_length: usize;
        if att.attlen == -1 && varatt_is_external_expanded(&val) {
            // ExpandedObjectHeader *eoh = DatumGetEOHP(val);
            // data = (char *) att_nominal_alignby(data, att->attalignby);
            // data_length = EOH_get_flat_size(eoh);
            // EOH_flatten_into(eoh, data, data_length);
            let eoh = datum::ExpandedObjectRef::from_expanded_datum_bytes(&val);
            cur = att_nominal_alignby(cur, att.attalignby);
            data_length = misc2_seams::eoh_get_flat_size::call(eoh)?;
            misc2_seams::eoh_flatten_into::call(
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
            Datum::ByRef(slice_in(mcx, &data[cur..cur + data_length])?);

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
    // deformed values back into the source). The copyslot callback receives
    // `src: &SlotData` (the dispatch contract is `copyslot(dst, &src)`), so
    // rather than mutate the source's cache, we deform its physical tuple into a
    // local `(values, isnull)` snapshot. For a virtual source the values are
    // already valid in `tts_values`; for a heap/buffer/minimal source we deform
    // its stored body-bearing carrier. The deformed columns are byte-identical
    // to C's cached `srcslot->tts_values`, and a freshly-copied virtual dst (we
    // materialize it below) cannot observe whether the source's cache was
    // populated, so the result is behaviour-preserving.
    let (sv, si) = source_all_attrs(mcx, src, srcnatts)?;

    // for (natt = 0; natt < srcdesc->natts; natt++) {
    //     dstslot->tts_values[natt] = srcslot->tts_values[natt];
    //     dstslot->tts_isnull[natt] = srcslot->tts_isnull[natt]; }
    {
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

/// `slot_getallattrs(srcslot)` over an immutable source slot: the source's
/// per-attribute `(value, isnull)` arrays for the first `natts` columns.
///
/// A virtual source's values are already valid in `tts_values`/`tts_isnull`; a
/// heap/buffer source deforms its stored `FormedTuple` body; a minimal source
/// deforms the heap-tuple view over its `FormedMinimalTuple` body. The result is
/// byte-identical to C's `slot_getallattrs(srcslot)` cache (the engine here is
/// the same `heap_deform_tuple` the slot's own `getsomeattrs` callback uses),
/// without writing the immutable source's cache.
fn source_all_attrs<'mcx>(
    mcx: Mcx<'mcx>,
    src: &SlotData<'mcx>,
    natts: i32,
) -> PgResult<(
    alloc::vec::Vec<Datum<'mcx>>,
    alloc::vec::Vec<bool>,
)> {
    let natts = natts as usize;

    // Deform a stored heap/minimal body into (value, isnull) columns.
    let deform = |mcx: Mcx<'mcx>,
                  tuple: &types_tuple::heaptuple::HeapTupleData<'mcx>,
                  data: &[u8]|
     -> PgResult<(alloc::vec::Vec<Datum<'mcx>>, alloc::vec::Vec<bool>)> {
        let desc = src
            .base()
            .tts_tupleDescriptor
            .as_ref()
            .ok_or_else(|| elog_error("tts_virtual_copyslot: source slot has no tuple descriptor"))?;
        let columns =
            heaptuple::heap_deform_tuple(mcx, tuple, desc, data)?;
        let mut values: alloc::vec::Vec<Datum<'mcx>> = alloc::vec::Vec::with_capacity(natts);
        let mut isnull: alloc::vec::Vec<bool> = alloc::vec::Vec::with_capacity(natts);
        for (v, n) in columns.into_iter() {
            values.push(v);
            isnull.push(n);
        }
        // The tuple may carry fewer attributes than the descriptor; pad with the
        // missing-value defaults (NULL when there is no missing array). Mirrors
        // slot_getmissingattrs over the descriptor — but for a copyslot the dst
        // descriptor matches the src, and a too-short source tuple pads NULL.
        while values.len() < natts {
            values.push(Datum::null());
            isnull.push(true);
        }
        Ok((values, isnull))
    };

    match src {
        // A virtual source is already fully valid; copy its values out.
        SlotData::Virtual(_) => {
            let sb = src.base();
            let mut values: alloc::vec::Vec<Datum<'mcx>> = alloc::vec::Vec::with_capacity(natts);
            let mut isnull: alloc::vec::Vec<bool> = alloc::vec::Vec::with_capacity(natts);
            for natt in 0..natts {
                values.push(sb.tts_values[natt].clone());
                isnull.push(sb.tts_isnull[natt]);
            }
            Ok((values, isnull))
        }
        SlotData::Heap(h) => match h.tuple.as_ref() {
            Some(t) => deform(mcx, &t.tuple, &t.data),
            // A heap slot with no stored tuple holds its values virtually.
            None => {
                let sb = src.base();
                let mut values = alloc::vec::Vec::with_capacity(natts);
                let mut isnull = alloc::vec::Vec::with_capacity(natts);
                for natt in 0..natts {
                    values.push(sb.tts_values[natt].clone());
                    isnull.push(sb.tts_isnull[natt]);
                }
                Ok((values, isnull))
            }
        },
        SlotData::BufferHeap(b) => match b.base.tuple.as_ref() {
            Some(t) => deform(mcx, &t.tuple, &t.data),
            None => {
                let sb = src.base();
                let mut values = alloc::vec::Vec::with_capacity(natts);
                let mut isnull = alloc::vec::Vec::with_capacity(natts);
                for natt in 0..natts {
                    values.push(sb.tts_values[natt].clone());
                    isnull.push(sb.tts_isnull[natt]);
                }
                Ok((values, isnull))
            }
        },
        SlotData::Minimal(m) => match m.mintuple.as_ref() {
            Some(mt) => {
                // Deform through the heap-tuple view over the minimal body.
                let view = heaptuple::heap_tuple_from_minimal_tuple(mcx, mt)?;
                deform(mcx, &view.tuple, &view.data)
            }
            None => {
                let sb = src.base();
                let mut values = alloc::vec::Vec::with_capacity(natts);
                let mut isnull = alloc::vec::Vec::with_capacity(natts);
                for natt in 0..natts {
                    values.push(sb.tts_values[natt].clone());
                    isnull.push(sb.tts_isnull[natt]);
                }
                Ok((values, isnull))
            }
        },
    }
}

/// `tts_virtual_copy_heap_tuple` (execTuples.c):
/// `heap_form_tuple(slot->tts_tupleDescriptor, slot->tts_values,
/// slot->tts_isnull)`.
pub fn tts_virtual_copy_heap_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &VirtualTupleTableSlot<'mcx>,
) -> PgResult<FormedTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());

    // return heap_form_tuple(slot->tts_tupleDescriptor, slot->tts_values,
    //                        slot->tts_isnull);
    //
    // The slot's `tts_values` carry `Datum`s (the by-ref lane heap_form_tuple
    // consumes); `heap_form_tuple` yields the body-bearing `FormedTuple` (header +
    // data area), which the widened `copy_heap_tuple` op return carries verbatim.
    let desc = slot
        .base
        .tts_tupleDescriptor
        .as_ref()
        .ok_or_else(|| elog_error("tts_virtual_copy_heap_tuple: slot has no tuple descriptor"))?;
    heaptuple::heap_form_tuple(
        mcx,
        desc,
        slot.base.tts_values.as_slice(),
        slot.base.tts_isnull.as_slice(),
    )
    .map_err(PgError::from)
}

/// `tts_virtual_copy_minimal_tuple` (execTuples.c):
/// `heap_form_minimal_tuple(slot->tts_tupleDescriptor, slot->tts_values,
/// slot->tts_isnull, extra)`.
pub fn tts_virtual_copy_minimal_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &VirtualTupleTableSlot<'mcx>,
    extra: usize,
) -> PgResult<FormedMinimalTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());

    // return heap_form_minimal_tuple(slot->tts_tupleDescriptor,
    //                                slot->tts_values, slot->tts_isnull, extra);
    //
    // `heap_form_minimal_tuple` over the slot's `Datum` tts_values yields the
    // body-bearing `FormedMinimalTuple` (header + data area), carried verbatim by
    // the widened `copy_minimal_tuple` op return.
    let desc = slot
        .base
        .tts_tupleDescriptor
        .as_ref()
        .ok_or_else(|| elog_error("tts_virtual_copy_minimal_tuple: slot has no tuple descriptor"))?;
    heaptuple::heap_form_minimal_tuple(
        mcx,
        desc,
        slot.base.tts_values.as_slice(),
        slot.base.tts_isnull.as_slice(),
        extra,
    )
    .map_err(PgError::from)
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
        slot.base.tts_flags &= !TTS_FLAG_SHOULDFREE;
    }

    // slot->tts_nvalid = 0;
    slot.base.tts_nvalid = 0;
    // slot->tts_flags |= TTS_FLAG_EMPTY;
    slot.base.tts_flags |= ::nodes::executor::TTS_FLAG_EMPTY;
    // ItemPointerSetInvalid(&slot->tts_tid);
    slot.base.tts_tid = types_tuple::heaptuple::ItemPointerData::invalid();
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
) -> PgResult<(Datum<'mcx>, bool)> {
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
    //
    // `heap_getsysattr` already yields the canonical unified value (a `ByVal`
    // word for the scalar system columns, a `ByRef` image for ctid/oid); pass
    // it through verbatim — no lossy projection to a bare word.
    let (value, isnull) =
        heaptuple::heap_getsysattr(mcx, &tuple.tuple, attnum)?;
    Ok((value, isnull))
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
    Ok(transam_xact_seams::transaction_id_is_current_transaction_id::call(xmin))
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
    // carries `Datum`s (the by-ref lane owns the bytes), exactly what
    // heap_form_tuple consumes; the `'mcx` allocation context is the slot's
    // memory context (C's MemoryContextSwitchTo(slot->tts_mcxt)).
    if slot.tuple.is_none() {
        // hslot->tuple = heap_form_tuple(...)
        let desc = slot
            .base
            .tts_tupleDescriptor
            .as_ref()
            .ok_or_else(|| elog_error("tts_heap_materialize: slot has no tuple descriptor"))?;
        let formed = heaptuple::heap_form_tuple(
            mcx,
            desc,
            slot.base.tts_values.as_slice(),
            slot.base.tts_isnull.as_slice(),
        )?;
        slot.tuple = Some(formed);
    } else {
        // hslot->tuple = heap_copytuple(hslot->tuple): copy the (foreign-context)
        // tuple into the slot's context.
        let copied = heaptuple::heap_copytuple(mcx, slot.tuple.as_ref())?;
        slot.tuple = copied;
    }

    // slot->tts_flags |= TTS_FLAG_SHOULDFREE;
    slot.base.tts_flags |= TTS_FLAG_SHOULDFREE;

    // MemoryContextSwitchTo(oldContext);
    Ok(())
}

/// `tts_heap_get_heap_tuple` (execTuples.c).
pub fn tts_heap_get_heap_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut HeapTupleTableSlot<'mcx>,
) -> PgResult<FormedTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());

    // if (!hslot->tuple) tts_heap_materialize(slot);
    if slot.tuple.is_none() {
        tts_heap_materialize(mcx, slot)?;
    }

    // return hslot->tuple;
    //
    // C returns `hslot->tuple` (the materialized contiguous image). The carrier
    // is the body-bearing `FormedTuple`; the widened `get_heap_tuple` op return
    // hands back a clone of it (header + data area) so the caller owns a
    // self-contained tuple, matching the C pointer-to-materialized-image return.
    slot.tuple
        .as_ref()
        .ok_or_else(|| elog_error("tts_heap_get_heap_tuple: tuple not materialized"))?
        .clone_in(mcx)
}

/// `tts_heap_copy_heap_tuple` (execTuples.c).
pub fn tts_heap_copy_heap_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut HeapTupleTableSlot<'mcx>,
) -> PgResult<FormedTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());

    // if (!hslot->tuple) tts_heap_materialize(slot);
    if slot.tuple.is_none() {
        tts_heap_materialize(mcx, slot)?;
    }

    // return heap_copytuple(hslot->tuple);
    //
    // heap_copytuple deep-copies the carried `FormedTuple` (header + data area)
    // into `mcx` (C: the caller's current context). The widened `copy_heap_tuple`
    // op return carries the full FormedTuple, body bytes included.
    heaptuple::heap_copytuple(mcx, slot.tuple.as_ref())?
        .ok_or_else(|| elog_error("tts_heap_copy_heap_tuple: tuple not materialized"))
}

/// `tts_heap_copy_minimal_tuple` (execTuples.c).
pub fn tts_heap_copy_minimal_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut HeapTupleTableSlot<'mcx>,
    extra: usize,
) -> PgResult<FormedMinimalTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.is_empty());

    // if (!hslot->tuple) tts_heap_materialize(slot);
    if slot.tuple.is_none() {
        tts_heap_materialize(mcx, slot)?;
    }

    // return minimal_tuple_from_heap_tuple(hslot->tuple, extra);
    //
    // minimal_tuple_from_heap_tuple builds a FormedMinimalTuple from the carried
    // FormedTuple (header + data area), reserving `extra` leading bytes; the
    // widened `copy_minimal_tuple` op return carries it body bytes included.
    let formed = slot
        .tuple
        .as_ref()
        .ok_or_else(|| elog_error("tts_heap_copy_minimal_tuple: tuple not materialized"))?;
    heaptuple::minimal_tuple_from_heap_tuple(mcx, formed, extra)
}

/// `tts_heap_store_tuple` (execTuples.c).
pub fn tts_heap_store_tuple<'mcx>(
    slot: &mut HeapTupleTableSlot<'mcx>,
    tuple: FormedTuple<'mcx>,
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
    // The stored heap tuple crosses as the body-bearing `FormedTuple` (header +
    // data-area bytes), exactly the slot's carrier field.

    // slot->tts_tid = tuple->t_self; (read before the move).
    slot.base.tts_tid = tuple.tuple.t_self;
    // hslot->tuple = tuple;
    slot.tuple = Some(tuple);
    // hslot->off = 0;
    slot.off = 0;
    // slot->tts_flags &= ~(TTS_FLAG_EMPTY | TTS_FLAG_SHOULDFREE);
    slot.base.mark_not_empty();
    slot.base.tts_flags &= !TTS_FLAG_SHOULDFREE;
    // if (shouldFree) slot->tts_flags |= TTS_FLAG_SHOULDFREE;
    if should_free {
        slot.base.tts_flags |= TTS_FLAG_SHOULDFREE;
    }
}

/// `tts_heap_copyslot` (execTuples.c:438): copy `srcslot` into a heap slot by
/// forming a heap tuple in the destination slot's context and storing it.
pub fn tts_heap_copyslot<'mcx>(
    mcx: Mcx<'mcx>,
    dst: &mut SlotData<'mcx>,
    src: &SlotData<'mcx>,
) -> PgResult<()> {
    // The copyslot callback is installed only for the heap ops, so dst is a heap
    // slot (C: dstslot->tts_ops->copyslot == tts_heap_copyslot).
    let SlotData::Heap(_) = dst else {
        return Err(elog_error(
            "tts_heap_copyslot: destination is not a heap slot",
        ));
    };

    // oldcontext = MemoryContextSwitchTo(dstslot->tts_mcxt);
    // tuple = ExecCopySlotHeapTuple(srcslot);
    // MemoryContextSwitchTo(oldcontext);
    // ExecStoreHeapTuple(tuple, dstslot, true);
    //
    // ExecCopySlotHeapTuple over the (immutable) source forms / clones a fresh
    // self-owned `FormedTuple` (the in-place materialize C may do is a
    // behaviour-preserving cache of that same result; skipped so the source can
    // be borrowed `&`). The tuple is allocated in `mcx` (the destination slot's
    // context). Store it into the heap dst with shouldFree = true.
    let tuple = exec_copy_slot_heap_tuple_ref(mcx, src)?
        .ok_or_else(|| elog_error("tts_heap_copyslot: source produced an empty tuple"))?;
    let t_table_oid = tuple.tuple.t_tableOid;
    if let SlotData::Heap(hdst) = dst {
        tts_heap_store_tuple(hdst, tuple, true);
    }
    dst.base_mut().tts_tableOid = t_table_oid;
    Ok(())
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
        slot.base.tts_flags &= !TTS_FLAG_SHOULDFREE;
    }

    // slot->tts_nvalid = 0;
    slot.base.tts_nvalid = 0;
    // slot->tts_flags |= TTS_FLAG_EMPTY;
    slot.base.tts_flags |= ::nodes::executor::TTS_FLAG_EMPTY;
    // ItemPointerSetInvalid(&slot->tts_tid);
    slot.base.tts_tid = types_tuple::heaptuple::ItemPointerData::invalid();
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
pub fn tts_minimal_getsysattr<'mcx>(
    slot: &MinimalTupleTableSlot,
    _attnum: i32,
) -> PgResult<(Datum<'mcx>, bool)> {
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
            heaptuple::heap_form_minimal_tuple(
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
            heaptuple::heap_copy_minimal_tuple(mcx, mintuple, 0)?
        }
    };

    // slot->tts_flags |= TTS_FLAG_SHOULDFREE;
    slot.base.tts_flags |= TTS_FLAG_SHOULDFREE;

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

    let view = heaptuple::heap_tuple_from_minimal_tuple(mcx, mintuple)?;

    // mslot->minhdr (the workspace HeapTupleData header `tuple` aliases).
    slot.minhdr = view.tuple.as_ref().clone();
    // mslot->tuple = &mslot->minhdr — the heap-tuple-shaped view over the body.
    slot.tuple = Some(view);
    Ok(())
}

/// `tts_minimal_get_minimal_tuple` (execTuples.c).
pub fn tts_minimal_get_minimal_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut MinimalTupleTableSlot<'mcx>,
) -> PgResult<FormedMinimalTuple<'mcx>> {
    // if (!mslot->mintuple) tts_minimal_materialize(slot);
    if slot.mintuple.is_none() {
        tts_minimal_materialize(mcx, slot)?;
    }
    // return mslot->mintuple;
    //
    // The slot carries the body-bearing `FormedMinimalTuple`; the widened
    // `get_minimal_tuple` op return hands back a clone of it (header + data area)
    // so the caller owns a self-contained minimal tuple (C returns the pointer
    // with shouldFree = false; the owned-model equivalent is an owned clone).
    slot.mintuple
        .as_ref()
        .ok_or_else(|| elog_error("tts_minimal_get_minimal_tuple: tuple not materialized"))?
        .clone_in(mcx)
}

/// `tts_minimal_copy_minimal_tuple` (execTuples.c).
pub fn tts_minimal_copy_minimal_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut MinimalTupleTableSlot<'mcx>,
    extra: usize,
) -> PgResult<FormedMinimalTuple<'mcx>> {
    // if (!mslot->mintuple) tts_minimal_materialize(slot);
    if slot.mintuple.is_none() {
        tts_minimal_materialize(mcx, slot)?;
    }
    // return heap_copy_minimal_tuple(mslot->mintuple, extra);
    //
    // heap_copy_minimal_tuple deep-copies the carried `FormedMinimalTuple`
    // (header + data area), reserving `extra` leading bytes; the widened
    // `copy_minimal_tuple` op return carries it body bytes included.
    let mintuple = slot
        .mintuple
        .as_ref()
        .ok_or_else(|| elog_error("tts_minimal_copy_minimal_tuple: tuple not materialized"))?;
    heaptuple::heap_copy_minimal_tuple(mcx, mintuple, extra)
}

/// `tts_minimal_copy_heap_tuple` (execTuples.c:659).
pub fn tts_minimal_copy_heap_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut MinimalTupleTableSlot<'mcx>,
) -> PgResult<FormedTuple<'mcx>> {
    // if (!mslot->mintuple) tts_minimal_materialize(slot);
    if slot.mintuple.is_none() {
        tts_minimal_materialize(mcx, slot)?;
    }
    // return heap_tuple_from_minimal_tuple(mslot->mintuple);
    //
    // heap_tuple_from_minimal_tuple builds a body-bearing `FormedTuple` (header +
    // data area, system columns zeroed) from the carried `FormedMinimalTuple`;
    // the widened `copy_heap_tuple` op return carries it body bytes included.
    let mintuple = slot
        .mintuple
        .as_ref()
        .ok_or_else(|| elog_error("tts_minimal_copy_heap_tuple: tuple not materialized"))?;
    heaptuple::heap_tuple_from_minimal_tuple(mcx, mintuple)
}

/// `tts_minimal_store_tuple` (execTuples.c).
pub fn tts_minimal_store_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut MinimalTupleTableSlot<'mcx>,
    mtup: FormedMinimalTuple<'mcx>,
    should_free: bool,
) -> PgResult<()> {
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
    slot.mintuple = Some(mtup);
    // Assert(mslot->tuple == &mslot->minhdr);
    // mslot->minhdr.t_len = mtup->t_len + MINIMAL_TUPLE_OFFSET;
    // mslot->minhdr.t_data = (HeapTupleHeader)((char*)mtup - MINIMAL_TUPLE_OFFSET);
    //
    // The stored minimal tuple crosses as the body-bearing `FormedMinimalTuple`
    // (header + data-area bytes), the slot's carrier; `mslot->minhdr` /
    // `mslot->tuple` are the `FormedTuple`-shaped heap-tuple view over that body
    // (the C `minhdr.t_data = (char*)mtup - MINIMAL_TUPLE_OFFSET` alias), wired by
    // the same helper tts_minimal_materialize uses.
    set_minimal_minhdr_view(mcx, slot)?;

    // if (shouldFree) slot->tts_flags |= TTS_FLAG_SHOULDFREE;
    if should_free {
        slot.base.tts_flags |= TTS_FLAG_SHOULDFREE;
    }
    Ok(())
}

/// `tts_minimal_copyslot` (execTuples.c:635): copy `srcslot` into a minimal
/// slot by forming a minimal tuple in the destination slot's context and
/// storing it.
pub fn tts_minimal_copyslot<'mcx>(
    mcx: Mcx<'mcx>,
    dst: &mut SlotData<'mcx>,
    src: &SlotData<'mcx>,
) -> PgResult<()> {
    // The copyslot callback is installed only for the minimal ops, so dst is a
    // minimal slot (C: dstslot->tts_ops->copyslot == tts_minimal_copyslot).
    let SlotData::Minimal(_) = dst else {
        return Err(elog_error(
            "tts_minimal_copyslot: destination is not a minimal slot",
        ));
    };

    // oldcontext = MemoryContextSwitchTo(dstslot->tts_mcxt);
    // mintuple = ExecCopySlotMinimalTuple(srcslot);
    // MemoryContextSwitchTo(oldcontext);
    // ExecStoreMinimalTuple(mintuple, dstslot, true);
    //
    // ExecCopySlotMinimalTuple over the (immutable) source forms / clones a fresh
    // self-owned `FormedMinimalTuple` (the in-place materialize C may do is a
    // behaviour-preserving cache of that same result; skipped so the source can
    // be borrowed `&`). The tuple is allocated in `mcx` (the destination slot's
    // context). Store it into the minimal dst with shouldFree = true.
    let mtup = exec_copy_slot_minimal_tuple_ref(mcx, src, 0)?;
    if let SlotData::Minimal(mdst) = dst {
        tts_minimal_store_tuple(mcx, mdst, mtup, true)?;
    }
    Ok(())
}

/// `ExecCopySlotMinimalTuple(srcslot)` over an immutable source slot (the
/// `tts_minimal_copyslot` path): a fresh, self-owned minimal-tuple copy of the
/// source's contents with `extra` reserved leading bytes.
///
/// C calls `slot->tts_ops->copy_minimal_tuple(slot, extra)`, which for an
/// already-formed source clones / converts its stored tuple and for a
/// values-only source forms a fresh one (the in-place materialize C may perform
/// is a behaviour-preserving cache of that same result, skipped here so the
/// source can be borrowed `&`). The produced copy is byte-identical to C's.
pub(crate) fn exec_copy_slot_minimal_tuple_ref<'mcx>(
    mcx: Mcx<'mcx>,
    src: &SlotData<'mcx>,
    extra: usize,
) -> PgResult<FormedMinimalTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!src.base().is_empty());

    let form_from_values = |mcx: Mcx<'mcx>| -> PgResult<FormedMinimalTuple<'mcx>> {
        let base = src.base();
        let desc = base.tts_tupleDescriptor.as_ref().ok_or_else(|| {
            elog_error("ExecCopySlotMinimalTuple: source slot has no tuple descriptor")
        })?;
        heaptuple::heap_form_minimal_tuple(
            mcx,
            desc,
            &base.tts_values,
            &base.tts_isnull,
            extra,
        )
        .map_err(PgError::from)
    };

    match src {
        // tts_virtual_copy_minimal_tuple: heap_form_minimal_tuple(...).
        SlotData::Virtual(_) => form_from_values(mcx),
        // tts_minimal_copy_minimal_tuple: if (!mintuple) materialize;
        // heap_copy_minimal_tuple(mintuple, extra).
        SlotData::Minimal(m) => match m.mintuple.as_ref() {
            Some(mt) => heaptuple::heap_copy_minimal_tuple(mcx, mt, extra),
            None => form_from_values(mcx),
        },
        // tts_heap_copy_minimal_tuple: if (!tuple) materialize;
        // minimal_tuple_from_heap_tuple(tuple, extra).
        SlotData::Heap(h) => match h.tuple.as_ref() {
            Some(t) => heaptuple::minimal_tuple_from_heap_tuple(mcx, t, extra),
            None => form_from_values(mcx),
        },
        // tts_buffer_heap_copy_minimal_tuple: if (!tuple) materialize;
        // minimal_tuple_from_heap_tuple(tuple, extra).
        SlotData::BufferHeap(b) => match b.base.tuple.as_ref() {
            Some(t) => heaptuple::minimal_tuple_from_heap_tuple(mcx, t, extra),
            None => form_from_values(mcx),
        },
    }
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
        slot.base.base.tts_flags &= !TTS_FLAG_SHOULDFREE;
    }

    // if (BufferIsValid(bslot->buffer)) ReleaseBuffer(bslot->buffer);
    if BufferIsValid(slot.buffer) {
        bufmgr_seams::release_buffer::call(slot.buffer);
    }

    // slot->tts_nvalid = 0;
    slot.base.base.tts_nvalid = 0;
    // slot->tts_flags |= TTS_FLAG_EMPTY;
    slot.base.base.tts_flags |= ::nodes::executor::TTS_FLAG_EMPTY;
    // ItemPointerSetInvalid(&slot->tts_tid);
    slot.base.base.tts_tid = types_tuple::heaptuple::ItemPointerData::invalid();
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
) -> PgResult<(Datum<'mcx>, bool)> {
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
    //
    // `heap_getsysattr` already yields the canonical unified value; pass it
    // through verbatim (no lossy projection to a bare word).
    let (value, isnull) =
        heaptuple::heap_getsysattr(mcx, &tuple.tuple, attnum)?;
    Ok((value, isnull))
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
    Ok(transam_xact_seams::transaction_id_is_current_transaction_id::call(xmin))
}

/// `tts_buffer_heap_materialize` (execTuples.c).
pub fn tts_buffer_heap_materialize<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut BufferHeapTupleTableSlot<'mcx>,
) -> PgResult<()> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.base.is_empty());

    // /* If slot has its tuple already materialized, nothing to do. */
    // if (TTS_SHOULDFREE(slot)) return;
    if slot.base.base.should_free() {
        return Ok(());
    }

    // oldContext = MemoryContextSwitchTo(slot->tts_mcxt); — the owned model
    // allocates the formed/copied FormedTuple in `mcx` (the slot's context).

    // /*
    //  * Have to deform from scratch, otherwise tts_values[] entries could point
    //  * into the non-materialized tuple (which might be gone when accessed).
    //  */
    // bslot->base.off = 0; slot->tts_nvalid = 0;
    slot.base.off = 0;
    slot.base.base.tts_nvalid = 0;

    if slot.base.tuple.is_none() {
        // if (!bslot->base.tuple)
        //     bslot->base.tuple = heap_form_tuple(slot->tts_tupleDescriptor,
        //                                         slot->tts_values, slot->tts_isnull);
        // (Normally a buffer slot has a tuple+buffer; this arm only fires for a
        //  virtual tuple stored in a buffer slot that must be materializable.)
        let desc = slot
            .base
            .base
            .tts_tupleDescriptor
            .as_ref()
            .ok_or_else(|| elog_error("tts_buffer_heap_materialize: slot has no tuple descriptor"))?;
        let formed = heaptuple::heap_form_tuple(
            mcx,
            desc,
            &slot.base.base.tts_values,
            &slot.base.base.tts_isnull,
        )
        .map_err(PgError::from)?;
        slot.base.tuple = Some(formed);
    } else {
        // else {
        //     bslot->base.tuple = heap_copytuple(bslot->base.tuple);
        //     if (likely(BufferIsValid(bslot->buffer))) ReleaseBuffer(bslot->buffer);
        //     bslot->buffer = InvalidBuffer;
        // }
        let copied =
            heaptuple::heap_copytuple(mcx, slot.base.tuple.as_ref())?;
        slot.base.tuple = copied;

        if BufferIsValid(slot.buffer) {
            bufmgr_seams::release_buffer::call(slot.buffer);
        }
        slot.buffer = InvalidBuffer;
    }

    // /*
    //  * We don't set TTS_FLAG_SHOULDFREE until after releasing the buffer, if
    //  * any. ...
    //  */
    // slot->tts_flags |= TTS_FLAG_SHOULDFREE;
    slot.base.base.tts_flags |= TTS_FLAG_SHOULDFREE;

    Ok(())
}

/// `tts_buffer_heap_copyslot` (execTuples.c).
pub fn tts_buffer_heap_copyslot<'mcx>(
    mcx: Mcx<'mcx>,
    dst: &mut SlotData<'mcx>,
    src: &SlotData<'mcx>,
) -> PgResult<()> {
    // The copyslot callback is installed only for the buffer-heap ops, so dst is
    // a buffer-heap slot (C: dstslot->tts_ops->copyslot == tts_buffer_heap_copyslot).
    let SlotData::BufferHeap(_) = dst else {
        return Err(elog_error(
            "tts_buffer_heap_copyslot: destination is not a buffer-heap slot",
        ));
    };

    // /*
    //  * If the source slot is of a different kind, or is a buffer slot that has
    //  * been materialized / is virtual, make a new copy of the tuple. Otherwise
    //  * make a new reference to the in-buffer tuple.
    //  */
    // if (dstslot->tts_ops != srcslot->tts_ops || TTS_SHOULDFREE(srcslot) ||
    //     !bsrcslot->base.tuple)
    let src_is_buffer = matches!(src, SlotData::BufferHeap(_));
    let src_has_tuple = matches!(src, SlotData::BufferHeap(b) if b.base.tuple.is_some());
    if !src_is_buffer || src.base().should_free() || !src_has_tuple {
        // {
        //     ExecClearTuple(dstslot);
        crate::slot_store_fetch::ExecClearTuple(dst)?;
        //     dstslot->tts_flags &= ~TTS_FLAG_EMPTY;
        dst.base_mut().mark_not_empty();
        //     oldContext = MemoryContextSwitchTo(dstslot->tts_mcxt);
        //     bdstslot->base.tuple = ExecCopySlotHeapTuple(srcslot);
        // (allocated in `mcx`, the destination slot's context).
        let copied = exec_copy_slot_heap_tuple_ref(mcx, src)?;
        if let SlotData::BufferHeap(bdst) = dst {
            bdst.base.tuple = copied;
        }
        //     dstslot->tts_flags |= TTS_FLAG_SHOULDFREE;
        dst.base_mut().tts_flags |= TTS_FLAG_SHOULDFREE;
        //     MemoryContextSwitchTo(oldContext);
        // }
    } else {
        // else {
        //     Assert(BufferIsValid(bsrcslot->buffer));
        let (src_tuple, src_buffer) = match src {
            SlotData::BufferHeap(b) => (b.base.tuple.as_ref().unwrap(), b.buffer),
            _ => unreachable!(),
        };
        debug_assert!(BufferIsValid(src_buffer));

        //     tts_buffer_heap_store_tuple(dstslot, bsrcslot->base.tuple,
        //                                 bsrcslot->buffer, false);
        // The store callback takes ownership of the tuple it stores; the C in-
        // buffer sharing references the same on-disk image while holding a pin,
        // so the owned model stores a clone of the source's FormedTuple (header +
        // the bytes that still live in the pinned page) and pins the same buffer.
        let tuple_copy = src_tuple.clone_in(mcx)?;
        if let SlotData::BufferHeap(bdst) = dst {
            crate::slot_store_fetch::tts_buffer_heap_store_tuple(
                bdst, tuple_copy, src_buffer, false,
            );

            //     /*
            //      * The HeapTupleData portion of the source tuple might be shorter
            //      * lived than the destination slot. Therefore copy the HeapTuple
            //      * into our slot's tupdata, which is guaranteed to live long enough
            //      * (but will still point into the buffer).
            //      */
            //     memcpy(&bdstslot->base.tupdata, bdstslot->base.tuple, sizeof(HeapTupleData));
            //     bdstslot->base.tuple = &bdstslot->base.tupdata;
            //
            // In the owned model `bdstslot->base.tuple` already owns a self-contained
            // FormedTuple (its bytes outlive the source HeapTupleData), so the C
            // tupdata workspace alias is a representation detail with no separate
            // lifetime to extend. Mirror C's bookkeeping by copying the stored
            // tuple's HeapTupleData header into the `tupdata` workspace field; the
            // body-bearing `base.tuple` remains the live carrier.
            if let Some(stored) = bdst.base.tuple.as_ref() {
                bdst.base.tupdata = (*stored.tuple).clone_in(mcx)?;
            }
        }
        // }
    }

    Ok(())
}

/// `ExecCopySlotHeapTuple(srcslot)` over an immutable source slot (the
/// `tts_buffer_heap_copyslot` cross-ops arm): a fresh, self-owned heap-tuple
/// copy of the source's contents.
///
/// C calls `slot->tts_ops->copy_heap_tuple(slot)`, which for an already-formed
/// source clones its stored tuple and for a values-only source forms a fresh
/// one (the in-place materialize C may perform is a behaviour-preserving cache
/// of that same result, skipped here so the source can be borrowed `&`). The
/// produced copy is byte-identical to C's.
fn exec_copy_slot_heap_tuple_ref<'mcx>(
    mcx: Mcx<'mcx>,
    src: &SlotData<'mcx>,
) -> PgResult<Option<types_tuple::heaptuple::FormedTuple<'mcx>>> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!src.base().is_empty());

    let form_from_values =
        |mcx: Mcx<'mcx>| -> PgResult<types_tuple::heaptuple::FormedTuple<'mcx>> {
            let base = src.base();
            let desc = base.tts_tupleDescriptor.as_ref().ok_or_else(|| {
                elog_error("ExecCopySlotHeapTuple: source slot has no tuple descriptor")
            })?;
            heaptuple::heap_form_tuple(
                mcx,
                desc,
                &base.tts_values,
                &base.tts_isnull,
            )
            .map_err(PgError::from)
        };

    match src {
        // tts_virtual_copy_heap_tuple: heap_form_tuple(tupdesc, tts_values, tts_isnull).
        SlotData::Virtual(_) => Ok(Some(form_from_values(mcx)?)),
        // tts_heap_copy_heap_tuple: if (!tuple) materialize; heap_copytuple(tuple).
        SlotData::Heap(h) => match h.tuple.as_ref() {
            Some(t) => heaptuple::heap_copytuple(mcx, Some(t)),
            None => Ok(Some(form_from_values(mcx)?)),
        },
        // tts_buffer_heap_copy_heap_tuple: if (!tuple) materialize; heap_copytuple(tuple).
        SlotData::BufferHeap(b) => match b.base.tuple.as_ref() {
            Some(t) => heaptuple::heap_copytuple(mcx, Some(t)),
            None => Ok(Some(form_from_values(mcx)?)),
        },
        // tts_minimal_copy_heap_tuple: if (!mintuple) materialize;
        // heap_tuple_from_minimal_tuple(mintuple).
        SlotData::Minimal(m) => match m.mintuple.as_ref() {
            Some(mt) => Ok(Some(
                heaptuple::heap_tuple_from_minimal_tuple(mcx, mt)?,
            )),
            None => {
                // materialize forms the minimal tuple from values, then
                // heap_tuple_from_minimal_tuple over it. Equivalent result:
                // form the heap tuple directly from the slot's values.
                Ok(Some(form_from_values(mcx)?))
            }
        },
    }
}

/// `tts_buffer_heap_get_heap_tuple` (execTuples.c). Mirrors the heap-slot
/// `tts_heap_get_heap_tuple` over the embedded `base` heap slot.
pub fn tts_buffer_heap_get_heap_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut BufferHeapTupleTableSlot<'mcx>,
) -> PgResult<FormedTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.base.is_empty());

    // if (!bslot->base.tuple) tts_buffer_heap_materialize(slot);
    if slot.base.tuple.is_none() {
        tts_buffer_heap_materialize(mcx, slot)?;
    }

    // return bslot->base.tuple;
    //
    // The slot carries the materialized tuple as a body-bearing `FormedTuple`;
    // the widened `get_heap_tuple` op return hands back a clone of it (header +
    // data area), a self-contained tuple matching the C return.
    slot.base
        .tuple
        .as_ref()
        .ok_or_else(|| elog_error("tts_buffer_heap_get_heap_tuple: tuple not materialized"))?
        .clone_in(mcx)
}

/// `tts_buffer_heap_copy_heap_tuple` (execTuples.c). Mirrors the heap-slot
/// `tts_heap_copy_heap_tuple` over the embedded `base` heap slot.
pub fn tts_buffer_heap_copy_heap_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut BufferHeapTupleTableSlot<'mcx>,
) -> PgResult<FormedTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.base.is_empty());

    // if (!bslot->base.tuple) tts_buffer_heap_materialize(slot);
    if slot.base.tuple.is_none() {
        tts_buffer_heap_materialize(mcx, slot)?;
    }

    // return heap_copytuple(bslot->base.tuple);
    //
    // heap_copytuple deep-copies the carried `FormedTuple` (header + data area)
    // into `mcx`; the widened `copy_heap_tuple` op return carries it body bytes
    // included.
    heaptuple::heap_copytuple(mcx, slot.base.tuple.as_ref())?
        .ok_or_else(|| elog_error("tts_buffer_heap_copy_heap_tuple: tuple not materialized"))
}

/// `tts_buffer_heap_copy_minimal_tuple` (execTuples.c). Mirrors the heap-slot
/// `tts_heap_copy_minimal_tuple` over the embedded `base` heap slot.
pub fn tts_buffer_heap_copy_minimal_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut BufferHeapTupleTableSlot<'mcx>,
    extra: usize,
) -> PgResult<FormedMinimalTuple<'mcx>> {
    // Assert(!TTS_EMPTY(slot));
    debug_assert!(!slot.base.base.is_empty());

    // if (!bslot->base.tuple) tts_buffer_heap_materialize(slot);
    if slot.base.tuple.is_none() {
        tts_buffer_heap_materialize(mcx, slot)?;
    }

    // return minimal_tuple_from_heap_tuple(bslot->base.tuple, extra);
    //
    // minimal_tuple_from_heap_tuple builds a FormedMinimalTuple from the carried
    // FormedTuple, reserving `extra` leading bytes; the widened copy_minimal_tuple
    // op return carries it body bytes included.
    let formed = slot.base.tuple.as_ref().ok_or_else(|| {
        elog_error("tts_buffer_heap_copy_minimal_tuple: tuple not materialized")
    })?;
    heaptuple::minimal_tuple_from_heap_tuple(mcx, formed, extra)
}

// --- helpers --------------------------------------------------------------

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

/// `slot_getsysattr(slot, attnum, &isnull)` (tuptable.h).
///
/// ```c
/// Assert(attnum < 0);          /* caller error */
/// if (attnum == TableOidAttributeNumber) {
///     *isnull = false; return ObjectIdGetDatum(slot->tts_tableOid);
/// } else if (attnum == SelfItemPointerAttributeNumber) {
///     *isnull = false; return PointerGetDatum(&slot->tts_tid);
/// }
/// /* Fetch the system attribute from the underlying tuple. */
/// return slot->tts_ops->getsysattr(slot, attnum, isnull);
/// ```
///
/// `tts_tableOid` and `tts_tid` are slot-header fields handled *before* the
/// per-kind callback dispatch; only the remaining system attributes route to
/// `tts_ops->getsysattr`.
pub fn slot_getsysattr<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &SlotData<'mcx>,
    attnum: AttrNumber,
) -> PgResult<(Datum<'mcx>, bool)> {
    // Assert(attnum < 0); /* caller error */
    debug_assert!(attnum < 0);

    // if (attnum == TableOidAttributeNumber) { *isnull = false;
    //     return ObjectIdGetDatum(slot->tts_tableOid); }
    if attnum == types_tuple::heaptuple::TableOidAttributeNumber {
        return Ok((Datum::from_oid(slot.base().tts_tableOid), false));
    }
    // else if (attnum == SelfItemPointerAttributeNumber) { *isnull = false;
    //     return PointerGetDatum(&slot->tts_tid); }
    if attnum == types_tuple::heaptuple::SelfItemPointerAttributeNumber {
        let bytes = heaptuple::item_pointer_bytes(
            mcx,
            &slot.base().tts_tid,
        )?;
        return Ok((Datum::ByRef(bytes), false));
    }

    // Fetch the system attribute from the underlying tuple
    // (slot->tts_ops->getsysattr).
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
        ::nodes::TupleSlotKind::Virtual => tts_virtual_copyslot(mcx, dst, src),
        ::nodes::TupleSlotKind::HeapTuple => tts_heap_copyslot(mcx, dst, src),
        ::nodes::TupleSlotKind::MinimalTuple => tts_minimal_copyslot(mcx, dst, src),
        ::nodes::TupleSlotKind::BufferHeapTuple => tts_buffer_heap_copyslot(mcx, dst, src),
    }
}
