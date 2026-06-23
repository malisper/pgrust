//! Carrier round-trip tests for the slot payload model.
//!
//! These exercise the keystone-2 widening: the slot ops carry the body-bearing
//! `FormedTuple` / `FormedMinimalTuple` (header + data-area bytes) end to end,
//! so a value stored into a virtual slot survives form → store → fetch → deform
//! through the heap and minimal slot kinds. A by-reference (varlena) column is
//! the load-bearing case — if any boundary dropped `FormedTuple::data` the
//! deformed bytes would not match.

extern crate alloc;
use alloc::vec;
use alloc::vec::Vec;

use mcx::{alloc_in, slice_in, Mcx, MemoryContext, PgVec};
use ::nodes::tuptable::SlotData;
use ::nodes::TupleSlotKind;
// The canonical value enum; `Datum` is its transitional alias.
use ::types_tuple::heaptuple::{Datum};
use ::types_tuple::heaptuple::{CompactAttribute, TupleDesc, TupleDescData};

use crate::slot_payload_model::MakeTupleTableSlot;
use crate::slot_store_fetch::{
    ExecCopySlot, ExecCopySlotHeapTuple, ExecCopySlotMinimalTupleExtra, ExecFetchSlotHeapTuple,
    ExecFetchSlotMinimalTuple, ExecForceStoreHeapTuple, ExecStoreHeapTuple, ExecStoreMinimalTuple,
    ExecStoreVirtualTuple,
};

fn byval(attlen: i16, attalignby: u8) -> CompactAttribute {
    CompactAttribute {
        attcacheoff: -1,
        attlen,
        attbyval: true,
        attispackable: false,
        atthasmissing: false,
        attisdropped: false,
        attgenerated: false,
        attnullability: 0,
        attalignby,
    }
}

fn varlena() -> CompactAttribute {
    CompactAttribute {
        attcacheoff: -1,
        attlen: -1,
        attbyval: false,
        attispackable: true,
        atthasmissing: false,
        attisdropped: false,
        attgenerated: false,
        attnullability: 0,
        attalignby: 4,
    }
}

fn tupdesc<'mcx>(mcx: Mcx<'mcx>, attrs: &[CompactAttribute]) -> TupleDescData<'mcx> {
    TupleDescData {
        natts: attrs.len() as i32,
        tdtypeid: 2249, // RECORDOID
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: slice_in(mcx, attrs).unwrap(),
        attrs: PgVec::new_in(mcx),
    }
}

/// Build a fresh boxed `TupleDesc` from the test descriptor (the
/// `MakeTupleTableSlot` argument shape, `Option<PgBox<TupleDescData>>`).
fn mk_desc<'mcx>(mcx: Mcx<'mcx>, td: &TupleDescData<'mcx>) -> TupleDesc<'mcx> {
    Some(alloc_in(mcx, td.clone_in(mcx).unwrap()).unwrap())
}

/// A 4-byte-header varlena datum carrying `payload`.
fn varlena_4b(payload: &[u8]) -> Vec<u8> {
    let total = 4 + payload.len();
    let mut v = vec![0u8; total];
    let word = (total as u32) << 2;
    v[0..4].copy_from_slice(&word.to_ne_bytes());
    v[4..].copy_from_slice(payload);
    v
}

/// Decode the payload of an on-disk varlena (`varlena_4b` or its packed
/// 1-byte-header short form that `heap_form_tuple` may emit for a packable
/// attribute), returning the bytes after the length header.
fn varlena_payload(b: &[u8]) -> &[u8] {
    if (b[0] & 0x01) == 0x01 {
        // 1-byte short header: VARSIZE_1B == (b[0] >> 1) & 0x7F (incl. the byte).
        let total = ((b[0] >> 1) & 0x7F) as usize;
        &b[1..total]
    } else {
        // 4-byte header: VARSIZE_4B == (word >> 2) & 0x3FFFFFFF (incl. 4 bytes).
        let word = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
        let total = ((word >> 2) & 0x3FFF_FFFF) as usize;
        &b[4..total]
    }
}

/// Fill a fresh virtual slot's value arrays with `(int4=0x01020304, text)` and
/// mark it a valid virtual tuple.
fn fill_virtual<'mcx>(mcx: Mcx<'mcx>, slot: &mut SlotData<'mcx>, text: &[u8]) {
    let base = slot.base_mut();
    base.tts_values.clear();
    base.tts_isnull.clear();
    let v: PgVec<'mcx, u8> = slice_in(mcx, &varlena_4b(text)).unwrap();
    base.tts_values
        .push(Datum::from_i32(0x01020304));
    base.tts_values.push(Datum::ByRef(v));
    base.tts_isnull.push(false);
    base.tts_isnull.push(false);
    ExecStoreVirtualTuple(slot).unwrap();
}

/// The deformed `(value, isnull)` of every attribute, via slot_getallattrs.
fn deform_all<'mcx>(mcx: Mcx<'mcx>, slot: &mut SlotData<'mcx>) -> Vec<(Datum<'mcx>, bool)> {
    crate::slot_deform::slot_getallattrs(mcx, slot).unwrap();
    let base = slot.base();
    base.tts_values
        .iter()
        .cloned()
        .zip(base.tts_isnull.iter().copied())
        .collect()
}

#[test]
fn virtual_to_heap_carrier_roundtrip() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let td = tupdesc(mcx, &[byval(4, 4), varlena()]);

    // Form the virtual slot's contents into a heap tuple (the body-bearing
    // FormedTuple), store it into a heap slot, then fetch + deform it back.
    let mut vslot = MakeTupleTableSlot(mcx, mk_desc(mcx, &td), TupleSlotKind::Virtual)
        .unwrap();
    fill_virtual(mcx, &mut vslot, b"hello world");

    // ExecCopySlotHeapTuple over the virtual slot forms a FormedTuple with body.
    let formed = ExecCopySlotHeapTuple(mcx, &mut vslot).unwrap();
    assert!(!formed.data.is_empty(), "formed tuple must carry body bytes");

    // Store it into a heap slot.
    let mut hslot =
        MakeTupleTableSlot(mcx, mk_desc(mcx, &td), TupleSlotKind::HeapTuple).unwrap();
    ExecStoreHeapTuple(formed, &mut hslot, true).unwrap();

    // Fetch the stored heap tuple — must still carry the full body.
    let (fetched, _should_free) = ExecFetchSlotHeapTuple(mcx, &mut hslot, false).unwrap();
    assert!(!fetched.data.is_empty(), "fetched tuple lost its body bytes");

    // Deform the heap slot and confirm the round-tripped values match.
    let cols = deform_all(mcx, &mut hslot);
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].0, Datum::from_i32(0x01020304));
    assert_eq!(cols[0].1, false);
    match &cols[1].0 {
        Datum::ByRef(b) => assert_eq!(varlena_payload(b), b"hello world"),
        other => panic!("expected by-reference text column, got {other:?}"),
    }
    assert_eq!(cols[1].1, false);
}

#[test]
fn virtual_to_minimal_carrier_roundtrip() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let td = tupdesc(mcx, &[byval(4, 4), varlena()]);

    let mut vslot = MakeTupleTableSlot(mcx, mk_desc(mcx, &td), TupleSlotKind::Virtual)
        .unwrap();
    fill_virtual(mcx, &mut vslot, b"minimal!");

    // Copy out a minimal tuple (body-bearing FormedMinimalTuple), store it into
    // a minimal slot, fetch it back, and deform.
    let mtup = ExecCopySlotMinimalTupleExtra(mcx, &mut vslot, 0).unwrap();
    assert!(!mtup.data.is_empty(), "minimal tuple must carry body bytes");

    let mut mslot = MakeTupleTableSlot(
        mcx,
        mk_desc(mcx, &td),
        TupleSlotKind::MinimalTuple,
    )
    .unwrap();
    ExecStoreMinimalTuple(mcx, mtup, &mut mslot, true).unwrap();

    let (fetched, _) = ExecFetchSlotMinimalTuple(mcx, &mut mslot).unwrap();
    assert!(!fetched.data.is_empty(), "fetched minimal tuple lost its body");

    let cols = deform_all(mcx, &mut mslot);
    assert_eq!(cols[0].0, Datum::from_i32(0x01020304));
    match &cols[1].0 {
        Datum::ByRef(b) => assert_eq!(varlena_payload(b), b"minimal!"),
        other => panic!("expected by-reference text column, got {other:?}"),
    }
}

#[test]
fn copyslot_virtual_to_heap_then_heap_to_minimal() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let td = tupdesc(mcx, &[byval(4, 4), varlena()]);

    // Build a heap slot holding a real tuple.
    let mut vslot = MakeTupleTableSlot(mcx, mk_desc(mcx, &td), TupleSlotKind::Virtual)
        .unwrap();
    fill_virtual(mcx, &mut vslot, b"copyslot");
    let formed = ExecCopySlotHeapTuple(mcx, &mut vslot).unwrap();
    let mut hslot =
        MakeTupleTableSlot(mcx, mk_desc(mcx, &td), TupleSlotKind::HeapTuple).unwrap();
    ExecStoreHeapTuple(formed, &mut hslot, true).unwrap();

    // ExecCopySlot heap -> minimal (the tts_minimal_copyslot path over an
    // immutable heap source).
    let mut mslot = MakeTupleTableSlot(
        mcx,
        mk_desc(mcx, &td),
        TupleSlotKind::MinimalTuple,
    )
    .unwrap();
    ExecCopySlot(mcx, &mut mslot, &hslot).unwrap();

    let cols = deform_all(mcx, &mut mslot);
    match &cols[1].0 {
        Datum::ByRef(b) => assert_eq!(varlena_payload(b), b"copyslot"),
        other => panic!("expected by-reference text column, got {other:?}"),
    }
}

#[test]
fn force_store_heap_tuple_into_virtual_deforms() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let td = tupdesc(mcx, &[byval(4, 4), varlena()]);

    // Form a heap tuple from a virtual source.
    let mut vslot = MakeTupleTableSlot(mcx, mk_desc(mcx, &td), TupleSlotKind::Virtual)
        .unwrap();
    fill_virtual(mcx, &mut vslot, b"force");
    let formed = ExecCopySlotHeapTuple(mcx, &mut vslot).unwrap();

    // ExecForceStoreHeapTuple into a (different-kind) virtual slot deforms the
    // tuple's body into the slot's value arrays.
    let mut dst = MakeTupleTableSlot(mcx, mk_desc(mcx, &td), TupleSlotKind::Virtual)
        .unwrap();
    ExecForceStoreHeapTuple(mcx, formed, &mut dst, true).unwrap();

    let base = dst.base();
    assert_eq!(
        base.tts_values[0],
        Datum::from_i32(0x01020304)
    );
    match &base.tts_values[1] {
        Datum::ByRef(b) => assert_eq!(varlena_payload(b), b"force"),
        other => panic!("expected by-reference text column, got {other:?}"),
    }
}
