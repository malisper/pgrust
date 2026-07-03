use alloc::rc::Rc;
use alloc::vec::Vec;

use ::datum::Datum;
use ::heaptuple::{heap_form_minimal_tuple, heap_form_tuple};
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::varatt::varsize_any;
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, HeapTupleData, TableOidAttributeNumber, TupleDescData,
    TYPALIGN_DOUBLE, TYPALIGN_INT, TYPSTORAGE_EXTENDED, TYPSTORAGE_PLAIN,
};

use crate::*;

fn col(
    attnum: i16,
    attlen: i16,
    attbyval: bool,
    attalign: i8,
    attstorage: i8,
) -> FormData_pg_attribute {
    FormData_pg_attribute {
        attnum,
        attlen,
        attbyval,
        attalign,
        attstorage,
        ..Default::default()
    }
}

fn make_desc<'mcx>(mcx: Mcx<'mcx>, cols: &[FormData_pg_attribute]) -> Rc<TupleDescData<'mcx>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for att in cols {
        compact.push(CompactAttribute::populate_from(att));
        attrs.push(*att);
    }
    Rc::new(TupleDescData {
        natts: cols.len() as i32,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

// int4, text, int8
fn desc3<'mcx>(mcx: Mcx<'mcx>) -> Rc<TupleDescData<'mcx>> {
    make_desc(
        mcx,
        &[
            col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN),
            col(2, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED),
            col(3, 8, true, TYPALIGN_DOUBLE, TYPSTORAGE_PLAIN),
        ],
    )
}

fn text_varlena(s: &str) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(
        &::types_tuple::varatt::set_varsize_4b_word((s.len() + 4) as u32).to_ne_bytes(),
    );
    v.extend_from_slice(s.as_bytes());
    v
}

fn text_datum(image: &[u8]) -> Datum {
    Datum::from_usize(image.as_ptr() as usize)
}

// Content bytes regardless of 1B (packed) vs 4B header form.
fn datum_text_bytes<'a>(d: Datum) -> &'a [u8] {
    unsafe {
        let p = d.as_usize() as *const u8;
        let total = varsize_any(p);
        if ::types_tuple::varatt::varatt_is_1b(p) {
            core::slice::from_raw_parts(p.add(1), total - 1)
        } else {
            core::slice::from_raw_parts(p.add(4), total - 4)
        }
    }
}

#[test]
fn heap_slot_store_deform_and_clear() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = desc3(mcx);
    let txt = text_varlena("hello");
    let values = [
        Datum::from_i32(7),
        text_datum(&txt),
        Datum::from_i64(1_234_567_890_123),
    ];
    let isnull = [false, false, false];
    let tuple = heap_form_tuple(mcx, &desc, &values, &isnull).unwrap();

    let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc));
    assert!(slot.base().is_fixed() && slot.base().is_empty());
    exec_store_heap_tuple_owned(&mut slot, mcx, tuple);
    assert!(!slot.base().is_empty() && slot.base().should_free());

    let mut isnull_out = true;
    assert_eq!(slot_getattr(&mut slot, 1, &mut isnull_out).as_i32(), 7);
    assert!(!isnull_out);
    assert_eq!(slot.base().tts_nvalid, 1);
    let d2 = slot_getattr(&mut slot, 2, &mut isnull_out);
    assert!(!isnull_out);
    assert_eq!(datum_text_bytes(d2), b"hello");
    assert_eq!(
        slot_getattr(&mut slot, 3, &mut isnull_out).as_i64(),
        1_234_567_890_123
    );
    assert_eq!(slot.base().tts_nvalid, 3);

    exec_clear_tuple(&mut slot, mcx);
    assert!(slot.base().is_empty() && !slot.base().should_free());
    assert_eq!(slot.base().tts_nvalid, 0);
}

#[test]
fn heap_slot_nulls_and_slow_mode() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = desc3(mcx);
    let txt = text_varlena("x");
    let values = [Datum::from_i32(1), text_datum(&txt), Datum::from_i64(-5)];
    let isnull = [false, true, false];
    let tuple = heap_form_tuple(mcx, &desc, &values, &isnull).unwrap();

    let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc));
    exec_store_heap_tuple_owned(&mut slot, mcx, tuple);

    let mut n = false;
    assert!(slot_attisnull(&mut slot, 2));
    assert_eq!(slot_getattr(&mut slot, 3, &mut n).as_i64(), -5);
    assert!(!n);
    assert_eq!(slot_getattr(&mut slot, 1, &mut n).as_i32(), 1);
}

#[test]
fn heap_slot_monomorphized_getattr_lane() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = desc3(mcx);
    let txt = text_varlena("mono");
    let values = [Datum::from_i32(42), text_datum(&txt), Datum::from_i64(9)];
    let tuple = heap_form_tuple(mcx, &desc, &values, &[false; 3]).unwrap();

    let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc));
    exec_store_heap_tuple_owned(&mut slot, mcx, tuple);
    let SlotData::Heap(h) = &mut slot else {
        unreachable!()
    };
    let mut n = true;
    assert_eq!(heap_slot_getattr(h, 1, &mut n).as_i32(), 42);
    assert!(!n);
    assert_eq!(heap_slot_getattr(h, 3, &mut n).as_i64(), 9);
    exec_clear_tuple(&mut slot, mcx);
}

#[test]
fn missing_attrs_pad_null_for_narrow_tuple() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let cols = [
        col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN),
        col(2, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN),
    ];
    let narrow = make_desc(mcx, &cols[..1]);
    let wide = make_desc(mcx, &cols);
    let tuple = heap_form_tuple(mcx, &narrow, &[Datum::from_i32(5)], &[false]).unwrap();

    let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(wide));
    exec_store_heap_tuple_owned(&mut slot, mcx, tuple);
    let mut n = false;
    assert_eq!(slot_getattr(&mut slot, 1, &mut n).as_i32(), 5);
    assert!(slot_attisnull(&mut slot, 2));
    assert_eq!(slot.base().tts_nvalid, 2);
    exec_clear_tuple(&mut slot, mcx);
}

#[test]
fn minimal_slot_store_deform_copy() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = desc3(mcx);
    let txt = text_varlena("mini");
    let values = [Datum::from_i32(11), text_datum(&txt), Datum::from_i64(22)];
    let mtup = heap_form_minimal_tuple(mcx, &desc, &values, &[false; 3], 0).unwrap();

    let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));
    exec_store_minimal_tuple_owned(&mut slot, mcx, mtup);
    assert!(slot.base().should_free());

    let mut n = false;
    assert_eq!(slot_getattr(&mut slot, 1, &mut n).as_i32(), 11);
    let d2 = slot_getattr(&mut slot, 2, &mut n);
    assert_eq!(datum_text_bytes(d2), b"mini");
    assert_eq!(slot_getattr(&mut slot, 3, &mut n).as_i64(), 22);

    let copy = exec_copy_slot_minimal_tuple(&mut slot, mcx, mcx, 0).unwrap();
    let mut slot2 = make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    exec_store_minimal_tuple_owned(&mut slot2, mcx, copy);
    let SlotData::Minimal(m2) = &mut slot2 else {
        unreachable!()
    };
    assert_eq!(minimal_slot_getattr(m2, 3, &mut n).as_i64(), 22);

    exec_clear_tuple(&mut slot, mcx);
    exec_clear_tuple(&mut slot2, mcx);
}

#[test]
fn virtual_slot_store_and_materialize() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = desc3(mcx);
    let txt = text_varlena("materialize me");
    let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc));

    {
        let base = slot.base_mut();
        base.tts_values[0] = Datum::from_i32(1);
        base.tts_values[1] = text_datum(&txt);
        base.tts_values[2] = Datum::from_i64(2);
        base.tts_isnull.fill(false);
    }
    exec_store_virtual_tuple(&mut slot);
    assert_eq!(slot.base().tts_nvalid, 3);

    exec_materialize_slot(&mut slot, mcx).unwrap();
    assert!(slot.base().should_free());
    let stored = slot.base().tts_values[1];
    assert_ne!(stored.as_usize(), txt.as_ptr() as usize);
    assert_eq!(datum_text_bytes(stored), b"materialize me");
    // idempotent while SHOULDFREE
    exec_materialize_slot(&mut slot, mcx).unwrap();
    assert_eq!(slot.base().tts_values[1].as_usize(), stored.as_usize());

    let mut n = true;
    assert_eq!(slot_getattr(&mut slot, 1, &mut n).as_i32(), 1);
    assert!(!n);
}

#[test]
fn all_byval_virtual_materialize_is_noop() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = make_desc(mcx, &[col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN)]);
    let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc));
    slot.base_mut().tts_values[0] = Datum::from_i32(9);
    slot.base_mut().tts_isnull[0] = false;
    exec_store_virtual_tuple(&mut slot);
    exec_materialize_slot(&mut slot, mcx).unwrap();
    assert!(!slot.base().should_free());
}

#[test]
fn store_all_null_tuple() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = desc3(mcx);
    let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc));
    exec_store_all_null_tuple(&mut slot, mcx);
    assert!(!slot.base().is_empty());
    assert!(slot_attisnull(&mut slot, 1) && slot_attisnull(&mut slot, 3));
}

#[test]
fn copy_slot_heap_to_virtual_and_back() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = desc3(mcx);
    let txt = text_varlena("copy me");
    let values = [Datum::from_i32(3), text_datum(&txt), Datum::from_i64(4)];
    let tuple = heap_form_tuple(mcx, &desc, &values, &[false; 3]).unwrap();

    let mut hslot = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc.clone()));
    exec_store_heap_tuple_owned(&mut hslot, mcx, tuple);

    let mut vslot = make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc.clone()));
    exec_copy_slot(&mut vslot, &mut hslot, mcx, mcx).unwrap();
    let mut n = false;
    assert_eq!(slot_getattr(&mut vslot, 1, &mut n).as_i32(), 3);
    let d = slot_getattr(&mut vslot, 2, &mut n);
    assert_eq!(datum_text_bytes(d), b"copy me");

    let mut hslot2 = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc.clone()));
    exec_copy_slot(&mut hslot2, &mut vslot, mcx, mcx).unwrap();
    assert!(hslot2.base().should_free());
    assert_eq!(slot_getattr(&mut hslot2, 3, &mut n).as_i64(), 4);

    let mut mslot = make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    exec_copy_slot(&mut mslot, &mut hslot, mcx, mcx).unwrap();
    assert_eq!(slot_getattr(&mut mslot, 1, &mut n).as_i32(), 3);

    exec_clear_tuple(&mut hslot, mcx);
    exec_clear_tuple(&mut hslot2, mcx);
    exec_clear_tuple(&mut mslot, mcx);
}

#[test]
fn force_store_heap_into_virtual_and_minimal() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = desc3(mcx);
    let txt = text_varlena("forced");
    let values = [Datum::from_i32(8), text_datum(&txt), Datum::from_i64(88)];
    let tuple = heap_form_tuple(mcx, &desc, &values, &[false; 3]).unwrap();

    let mut vslot = make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc.clone()));
    exec_force_store_heap_tuple_owned(tuple, &mut vslot, mcx).unwrap();
    assert!(vslot.base().should_free());
    let mut n = false;
    let d = slot_getattr(&mut vslot, 2, &mut n);
    assert_eq!(datum_text_bytes(d), b"forced");

    let mtup = heap_form_minimal_tuple(mcx, &desc, &values, &[false; 3], 0).unwrap();
    let mut vslot2 = make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc));
    exec_force_store_minimal_tuple_owned(mtup, &mut vslot2, mcx).unwrap();
    assert_eq!(slot_getattr(&mut vslot2, 1, &mut n).as_i32(), 8);
    assert_eq!(slot_getattr(&mut vslot2, 3, &mut n).as_i64(), 88);
}

#[test]
fn fetch_heap_tuple_and_sysattr() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = desc3(mcx);
    let txt = text_varlena("f");
    let values = [Datum::from_i32(1), text_datum(&txt), Datum::from_i64(2)];
    let tuple = heap_form_tuple(mcx, &desc, &values, &[false; 3]).unwrap();

    let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc));
    exec_store_heap_tuple_owned(&mut slot, mcx, tuple);
    slot.base_mut().tts_tableOid = 4242;

    match exec_fetch_slot_heap_tuple(&mut slot, false, mcx, mcx).unwrap() {
        FetchedHeapTuple::Slot(t) => assert_eq!(t.t_data().natts(), 3),
        FetchedHeapTuple::Copied(_) => panic!("heap slot must lend, not copy"),
    }

    let mut n = true;
    let d = slot_getsysattr(&slot, TableOidAttributeNumber, &mut n).unwrap();
    assert_eq!(d.as_oid(), 4242);
    assert!(!n);

    let ctx2 = MemoryContext::new("test2");
    let desc2 = desc3(ctx2.mcx());
    let mut vslot = make_tuple_table_slot(ctx2.mcx(), TupleSlotKind::Virtual, Some(desc2));
    vslot.base_mut().tts_values[0] = Datum::from_i32(1);
    vslot.base_mut().tts_values[1] = text_datum(&txt);
    vslot.base_mut().tts_values[2] = Datum::from_i64(2);
    vslot.base_mut().tts_isnull.fill(false);
    exec_store_virtual_tuple(&mut vslot);
    match exec_fetch_slot_heap_tuple(&mut vslot, false, ctx2.mcx(), ctx2.mcx()).unwrap() {
        FetchedHeapTuple::Copied(t) => assert_eq!(t.t_data().natts(), 3),
        FetchedHeapTuple::Slot(_) => panic!("virtual slot must copy"),
    }
    assert!(slot_getsysattr(&vslot, -2, &mut n).is_err());

    exec_clear_tuple(&mut slot, mcx);
}

#[test]
fn minimal_fetch_lends_from_slot() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = desc3(mcx);
    let txt = text_varlena("m");
    let values = [Datum::from_i32(1), text_datum(&txt), Datum::from_i64(2)];
    let mtup = heap_form_minimal_tuple(mcx, &desc, &values, &[false; 3], 0).unwrap();
    let expect_len = mtup.t_len();

    let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    exec_store_minimal_tuple_owned(&mut slot, mcx, mtup);
    match exec_fetch_slot_minimal_tuple(&mut slot, mcx, mcx).unwrap() {
        FetchedMinimalTuple::Slot(m) => assert_eq!(m.t_len, expect_len),
        FetchedMinimalTuple::Copied(_) => panic!("minimal slot must lend"),
    }
    exec_clear_tuple(&mut slot, mcx);
}

#[test]
fn heap_materialize_from_virtual_content() {
    // C: a heap slot can carry virtual content (no tuple); materialize forms one.
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = desc3(mcx);
    let txt = text_varlena("v2h");
    let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc));
    {
        let base = slot.base_mut();
        base.tts_values[0] = Datum::from_i32(6);
        base.tts_values[1] = text_datum(&txt);
        base.tts_values[2] = Datum::from_i64(7);
        base.tts_isnull.fill(false);
        base.mark_not_empty();
        base.tts_nvalid = 3;
    }
    exec_materialize_slot(&mut slot, mcx).unwrap();
    assert!(slot.base().should_free());
    assert_eq!(slot.base().tts_nvalid, 0);
    let mut n = false;
    assert_eq!(slot_getattr(&mut slot, 1, &mut n).as_i32(), 6);
    let d = slot_getattr(&mut slot, 2, &mut n);
    assert_eq!(datum_text_bytes(d), b"v2h");
    exec_clear_tuple(&mut slot, mcx);
}

#[test]
fn borrowed_heap_store_does_not_free() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = desc3(mcx);
    let txt = text_varlena("borrow");
    let values = [Datum::from_i32(1), text_datum(&txt), Datum::from_i64(2)];
    let tuple = heap_form_tuple(mcx, &desc, &values, &[false; 3]).unwrap();

    let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc));
    // SAFETY: test-scoped read alias; the owner outlives the slot content.
    let view = unsafe { crate::slots::dup_heap_view(&tuple) };
    exec_store_heap_tuple(&mut slot, mcx, view);
    assert!(!slot.base().should_free());
    let mut n = false;
    assert_eq!(slot_getattr(&mut slot, 1, &mut n).as_i32(), 1);
    exec_clear_tuple(&mut slot, mcx);
    assert_eq!(tuple.t_data().natts(), 3);
}

#[test]
fn slot_getattr_hit_path_needs_no_deform() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = desc3(mcx);
    let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc));
    slot.base_mut().tts_values[0] = Datum::from_i32(5);
    slot.base_mut().tts_isnull[0] = false;
    exec_store_virtual_tuple(&mut slot);
    let mut n = true;
    // virtual getsomeattrs panics, so a successful read proves the hit path
    assert_eq!(slot_getattr(&mut slot, 1, &mut n).as_i32(), 5);
    assert!(!n);
}

#[test]
fn is_current_xact_tuple_surface() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = desc3(mcx);
    let txt = text_varlena("x");
    let values = [Datum::from_i32(1), text_datum(&txt), Datum::from_i64(2)];
    let tuple = heap_form_tuple(mcx, &desc, &values, &[false; 3]).unwrap();
    let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc.clone()));
    exec_store_heap_tuple_owned(&mut slot, mcx, tuple);
    assert!(slot_is_current_xact_tuple(&slot, |_| true).unwrap());

    let mut vslot = make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc));
    exec_store_all_null_tuple(&mut vslot, mcx);
    assert!(slot_is_current_xact_tuple(&vslot, |_| true).is_err());
    exec_clear_tuple(&mut slot, mcx);
}

#[test]
fn set_slot_descriptor_on_unfixed_slot() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::Virtual, None);
    assert!(!slot.base().is_fixed());
    assert!(slot.base().tts_tupleDescriptor.is_none());
    exec_set_slot_descriptor(&mut slot, mcx, desc3(mcx));
    assert_eq!(slot.base().tts_values.len(), 3);
}

#[test]
fn copy_slot_buffer_to_buffer_shares_pin() {
    use core::sync::atomic::{AtomicU32, Ordering};
    static INCRS: AtomicU32 = AtomicU32::new(0);
    static RELEASES: AtomicU32 = AtomicU32::new(0);
    bufmgr_seams::incr_buffer_ref_count::set(|_| {
        INCRS.fetch_add(1, Ordering::Relaxed);
    });
    bufmgr_seams::release_buffer::set(|_| {
        RELEASES.fetch_add(1, Ordering::Relaxed);
        Ok(())
    });

    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = desc3(mcx);
    let txt = text_varlena("pinned");
    let values = [Datum::from_i32(9), text_datum(&txt), Datum::from_i64(11)];
    let tuple = heap_form_tuple(mcx, &desc, &values, &[false; 3]).unwrap();
    let tuple = {
        // Simulate a buffer-resident tuple: the image is not slot-owned.
        let t = &tuple;
        unsafe { HeapTupleData::from_raw_parts(t.header_ptr(), t.t_len, t.t_self, t.t_tableOid) }
    };

    let mut src = make_tuple_table_slot(mcx, TupleSlotKind::BufferHeapTuple, Some(desc.clone()));
    exec_store_buffer_heap_tuple(&mut src, mcx, tuple, 5);
    assert_eq!(INCRS.load(Ordering::Relaxed), 1);
    assert!(!src.base().should_free());

    let mut dst = make_tuple_table_slot(mcx, TupleSlotKind::BufferHeapTuple, Some(desc));
    exec_copy_slot(&mut dst, &mut src, mcx, mcx).unwrap();
    assert_eq!(INCRS.load(Ordering::Relaxed), 2);
    assert_eq!(RELEASES.load(Ordering::Relaxed), 0);
    assert!(!dst.base().should_free());
    let SlotData::BufferHeap(b) = &dst else { unreachable!() };
    assert_eq!(b.buffer, 5);
    let mut n = false;
    assert_eq!(slot_getattr(&mut dst, 1, &mut n).as_i32(), 9);

    exec_clear_tuple(&mut dst, mcx);
    exec_clear_tuple(&mut src, mcx);
}

#[test]
fn deform_resumes_past_cstring_attrs() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = make_desc(
        mcx,
        &[
            col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN),
            col(2, -2, false, ::types_tuple::TYPALIGN_CHAR, TYPSTORAGE_PLAIN),
            col(3, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED),
            col(4, -2, false, ::types_tuple::TYPALIGN_CHAR, TYPSTORAGE_PLAIN),
            col(5, 8, true, TYPALIGN_DOUBLE, TYPSTORAGE_PLAIN),
        ],
    );
    let cs1 = b"alpha\0";
    let cs2 = b"z\0";
    let txt = text_varlena("varlena");
    for null_mid in [false, true] {
        let values = [
            Datum::from_i32(41),
            Datum::from_usize(cs1.as_ptr() as usize),
            text_datum(&txt),
            Datum::from_usize(cs2.as_ptr() as usize),
            Datum::from_i64(-9),
        ];
        let isnull = [false, false, null_mid, false, false];
        let tuple = heap_form_tuple(mcx, &desc, &values, &isnull).unwrap();
        let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc.clone()));
        exec_store_heap_tuple_owned(&mut slot, mcx, tuple);
        slot_getallattrs(&mut slot);
        let base = slot.base();
        assert_eq!(base.tts_nvalid, 5);
        assert_eq!(base.tts_values[0].as_i32(), 41);
        let got1 = unsafe {
            core::ffi::CStr::from_ptr(base.tts_values[1].as_usize() as *const core::ffi::c_char)
        };
        assert_eq!(got1.to_bytes(), b"alpha");
        assert_eq!(base.tts_isnull[2], null_mid);
        if !null_mid {
            assert_eq!(datum_text_bytes(base.tts_values[2]), b"varlena");
        }
        let got3 = unsafe {
            core::ffi::CStr::from_ptr(base.tts_values[3].as_usize() as *const core::ffi::c_char)
        };
        assert_eq!(got3.to_bytes(), b"z");
        assert_eq!(base.tts_values[4].as_i64(), -9);
        exec_clear_tuple(&mut slot, mcx);
    }
}
