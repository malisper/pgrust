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
        FetchedMinimalTuple::Slot(m, _) => assert_eq!(unsafe { m.as_ref() }.t_len, expect_len),
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

// SoA batch deform parity: gather must leave the slot exactly as
// slot_getsomeattrs(ncols) would, resume state included.
#[test]
fn soa_batch_deform_matches_lazy_deform() {
    use ::types_tuple::TYPALIGN_SHORT;
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    // int4, int2, int8 fixed prefix; text tail exercises resume past ncols.
    let desc = make_desc(
        mcx,
        &[
            col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN),
            col(2, 2, true, TYPALIGN_SHORT, TYPSTORAGE_PLAIN),
            col(3, 8, true, TYPALIGN_DOUBLE, TYPSTORAGE_PLAIN),
            col(4, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED),
        ],
    );
    let ncols = 3usize;
    let plan = SoaDeformPlan::try_new(mcx, &desc.compact_attrs, ncols).unwrap();
    let mut soa = SoaBatch::new_in(mcx, plan.ncols());
    let txt = text_varlena("tail");

    let rows: [( [Datum; 4], [bool; 4] ); 4] = [
        (
            [Datum::from_i32(7), Datum::from_i16(-3), Datum::from_i64(1_234_567), text_datum(&txt)],
            [false, false, false, false],
        ),
        (
            [Datum::from_i32(0), Datum::null(), Datum::from_i64(-1), text_datum(&txt)],
            [false, true, false, false],
        ),
        (
            [Datum::null(), Datum::from_i16(9), Datum::null(), text_datum(&txt)],
            [true, false, true, false],
        ),
        (
            [Datum::from_i32(i32::MAX), Datum::from_i16(1), Datum::from_i64(i64::MIN), Datum::null()],
            [false, false, false, true],
        ),
    ];

    let mut tuples = Vec::new();
    for (values, isnull) in &rows {
        tuples.push(heap_form_tuple(mcx, &desc, values, isnull).unwrap());
    }
    soa.begin(tuples.len() as u32);
    for (i, t) in tuples.iter().enumerate() {
        soa_classify_row(&mut soa, &plan, &desc.compact_attrs, i as u32, t);
    }
    soa_deform_columns(&mut soa, &plan, &desc.compact_attrs, None);

    for (i, (values, isnull)) in rows.iter().enumerate() {
        let mut got = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc.clone()));
        let tg = heap_form_tuple(mcx, &desc, values, isnull).unwrap();
        exec_store_heap_tuple_owned(&mut got, mcx, tg);
        assert!(soa_store_prefix(&mut got, &soa, i as u32));

        let mut want = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc.clone()));
        let tw = heap_form_tuple(mcx, &desc, values, isnull).unwrap();
        exec_store_heap_tuple_owned(&mut want, mcx, tw);
        slot_getsomeattrs(&mut want, ncols as i32);

        {
            let (gb, wb) = (got.base(), want.base());
            assert_eq!(gb.tts_nvalid, wb.tts_nvalid, "row {i}");
            assert_eq!(gb.tts_flags, wb.tts_flags, "row {i}");
            for c in 0..ncols {
                assert_eq!(gb.tts_isnull[c], wb.tts_isnull[c], "row {i} col {c}");
                assert_eq!(gb.tts_values[c].as_i64(), wb.tts_values[c].as_i64(), "row {i} col {c}");
            }
            let (SlotData::Heap(gh), SlotData::Heap(wh)) = (&got, &want) else {
                unreachable!()
            };
            assert_eq!(gh.off, wh.off, "row {i}");
        }

        // Resume past the prefix from the published offset.
        let mut n4 = false;
        let g4 = slot_getattr(&mut got, 4, &mut n4);
        let mut w4n = false;
        let w4 = slot_getattr(&mut want, 4, &mut w4n);
        assert_eq!(n4, w4n, "row {i}");
        if !n4 {
            assert_eq!(datum_text_bytes(g4), datum_text_bytes(w4), "row {i}");
        }
        exec_clear_tuple(&mut got, mcx);
        exec_clear_tuple(&mut want, mcx);
    }

    // Qual-column-only deform matches the full deform for that column.
    let mut qsoa = SoaBatch::new_in(mcx, plan.ncols());
    qsoa.begin(tuples.len() as u32);
    for (i, t) in tuples.iter().enumerate() {
        soa_classify_row(&mut qsoa, &plan, &desc.compact_attrs, i as u32, t);
    }
    soa_deform_columns(&mut qsoa, &plan, &desc.compact_attrs, Some(2));
    for i in 0..tuples.len() {
        assert_eq!(qsoa.col_isnull(2)[i], soa.col_isnull(2)[i], "qrow {i}");
        if !qsoa.col_isnull(2)[i] {
            assert_eq!(qsoa.col_values(2)[i].as_i64(), soa.col_values(2)[i].as_i64(), "qrow {i}");
        }
    }

    // Narrow tuple (pre-ALTER ADD COLUMN image) falls back to the lazy path.
    let narrow = make_desc(mcx, &[col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN)]);
    let nt = heap_form_tuple(mcx, &narrow, &[Datum::from_i32(5)], &[false]).unwrap();
    soa.begin(1);
    soa_classify_row(&mut soa, &plan, &desc.compact_attrs, 0, &nt);
    soa_deform_columns(&mut soa, &plan, &desc.compact_attrs, None);
    assert!(soa.is_fallback(0));
    let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc));
    let nt2 = heap_form_tuple(mcx, &narrow, &[Datum::from_i32(5)], &[false]).unwrap();
    exec_store_heap_tuple_owned(&mut slot, mcx, nt2);
    assert!(!soa_store_prefix(&mut slot, &soa, 0));
    exec_clear_tuple(&mut slot, mcx);
}

// Varkey staging parity: staged pointer/null must equal slot_getattr of the
// key attribute for every lane (fixed-prefix probe, walk past varlenas,
// nulls, packed headers) and narrow tuples must take the fallback bit.
#[test]
fn soa_stage_varkey_matches_slot_getattr() {
    use ::types_tuple::TYPALIGN_SHORT;
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    // int4, int2, int8, text, text: key col 3 has a fixed prefix; key col 4
    // walks past the col-3 varlena.
    let desc = make_desc(
        mcx,
        &[
            col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN),
            col(2, 2, true, TYPALIGN_SHORT, TYPSTORAGE_PLAIN),
            col(3, 8, true, TYPALIGN_DOUBLE, TYPSTORAGE_PLAIN),
            col(4, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED),
            col(5, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED),
        ],
    );
    assert!(SoaVarKeyPlan::try_new(&desc.compact_attrs, 0).is_none(), "fixed key col");
    assert!(SoaVarKeyPlan::try_new(&desc.compact_attrs, 5).is_none(), "out of range");
    let short = text_varlena("k");
    let long = text_varlena(&"x".repeat(200));
    let rows: [([Datum; 5], [bool; 5]); 5] = [
        (
            [Datum::from_i32(7), Datum::from_i16(-3), Datum::from_i64(1), text_datum(&short), text_datum(&long)],
            [false, false, false, false, false],
        ),
        (
            [Datum::from_i32(1), Datum::from_i16(2), Datum::from_i64(3), text_datum(&long), text_datum(&short)],
            [false, false, false, false, false],
        ),
        (
            [Datum::from_i32(0), Datum::null(), Datum::from_i64(-1), text_datum(&short), text_datum(&short)],
            [false, true, false, false, false],
        ),
        (
            [Datum::from_i32(5), Datum::from_i16(6), Datum::from_i64(7), Datum::null(), text_datum(&long)],
            [false, false, false, true, false],
        ),
        (
            [Datum::null(), Datum::from_i16(8), Datum::null(), text_datum(&long), Datum::null()],
            [true, false, true, false, true],
        ),
    ];
    let mut tuples = Vec::new();
    for (values, isnull) in &rows {
        tuples.push(heap_form_tuple(mcx, &desc, values, isnull).unwrap());
    }
    for key in [3usize, 4usize] {
        let plan = SoaVarKeyPlan::try_new(&desc.compact_attrs, key).unwrap();
        let mut soa = SoaBatch::new_in(mcx, 1);
        soa.begin(tuples.len() as u32);
        for (i, t) in tuples.iter().enumerate() {
            soa_stage_varkey(&mut soa, &plan, &desc.compact_attrs, i as u32, t);
        }
        for (i, (values, isnull)) in rows.iter().enumerate() {
            assert!(!soa.is_fallback(i as u32), "key {key} row {i}");
            let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc.clone()));
            let t = heap_form_tuple(mcx, &desc, values, isnull).unwrap();
            exec_store_heap_tuple_owned(&mut slot, mcx, t);
            let mut wn = false;
            let w = slot_getattr(&mut slot, key as i32 + 1, &mut wn);
            assert_eq!(soa.col_isnull(0)[i], wn, "key {key} row {i}");
            if !wn {
                assert_eq!(
                    datum_text_bytes(soa.col_values(0)[i]),
                    datum_text_bytes(w),
                    "key {key} row {i}"
                );
            }
            exec_clear_tuple(&mut slot, mcx);
        }
        // Narrow tuple: key attribute absent, fallback bit set.
        let narrow = make_desc(mcx, &[col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN)]);
        let nt = heap_form_tuple(mcx, &narrow, &[Datum::from_i32(5)], &[false]).unwrap();
        soa.begin(1);
        soa_stage_varkey(&mut soa, &plan, &desc.compact_attrs, 0, &nt);
        assert!(soa.is_fallback(0), "key {key} narrow");
    }
}

// Deform-JIT integration parity (docs/optimizations/jit-deform.md): an armed
// plan/slot must produce bit-identical batch and slot state to the AOT/
// interpreted paths across dense, mixed (hasnulls), and narrow batches.
#[cfg(target_arch = "aarch64")]
#[test]
fn jit_deform_matches_aot_and_interpreter() {
    use ::types_tuple::TYPALIGN_SHORT;
    // Leaked context: jit_deform::install pins the descriptor as 'static
    // (the relcache-entry contract).
    let mcx = alloc::boxed::Box::leak(alloc::boxed::Box::new(MemoryContext::new("jit-int"))).mcx();
    // char, int2, int4, int8 fixed prefix (padding holes); text tail.
    let desc = make_desc(
        mcx,
        &[
            col(1, 1, true, ::types_tuple::TYPALIGN_CHAR, TYPSTORAGE_PLAIN),
            col(2, 2, true, TYPALIGN_SHORT, TYPSTORAGE_PLAIN),
            col(3, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN),
            col(4, 8, true, TYPALIGN_DOUBLE, TYPSTORAGE_PLAIN),
            col(5, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED),
        ],
    );
    let ncols = 4usize;
    let kernel = ::jit_deform::install(&desc, ncols).expect("install");
    assert!(kernel.matches(&desc));
    let txt = text_varlena("tail");

    let rows: [([Datum; 5], [bool; 5]); 4] = [
        (
            [Datum::from_char(-7), Datum::from_i16(-3), Datum::from_i32(9), Datum::from_i64(1_234_567), text_datum(&txt)],
            [false, false, false, false, false],
        ),
        (
            [Datum::from_char(1), Datum::from_i16(i16::MIN), Datum::from_i32(i32::MAX), Datum::from_i64(i64::MIN), Datum::null()],
            [false, false, false, false, true],
        ),
        (
            [Datum::from_char(0), Datum::null(), Datum::from_i32(-1), Datum::null(), text_datum(&txt)],
            [false, true, false, true, false],
        ),
        (
            [Datum::from_char(66), Datum::from_i16(2), Datum::from_i32(3), Datum::from_i64(4), text_datum(&txt)],
            [false, false, false, false, false],
        ),
    ];
    // Rows 0 and 3 are fully null-free (dense lane); adding rows 1/2 stages
    // a mixed batch where the armed kernel must stand down.
    let dense: [usize; 2] = [0, 3];

    // Batch pass, dense staging: armed plan vs AOT plan bit-identical.
    let plan_aot = SoaDeformPlan::try_new(mcx, &desc.compact_attrs, ncols).unwrap();
    let mut plan_jit = SoaDeformPlan::try_new(mcx, &desc.compact_attrs, ncols).unwrap();
    plan_jit.arm_jit(kernel.clone());
    let mut tuples = Vec::new();
    for (values, isnull) in &rows {
        tuples.push(heap_form_tuple(mcx, &desc, values, isnull).unwrap());
    }
    let all: [usize; 4] = [0, 1, 2, 3];
    for stage in [&dense[..], &all[..]] {
        let mut a = SoaBatch::new_in(mcx, plan_aot.ncols());
        let mut j = SoaBatch::new_in(mcx, plan_jit.ncols());
        a.begin(stage.len() as u32);
        j.begin(stage.len() as u32);
        for (i, &t) in stage.iter().enumerate() {
            soa_classify_row(&mut a, &plan_aot, &desc.compact_attrs, i as u32, &tuples[t]);
            soa_classify_row(&mut j, &plan_jit, &desc.compact_attrs, i as u32, &tuples[t]);
        }
        soa_deform_columns(&mut a, &plan_aot, &desc.compact_attrs, None);
        soa_deform_columns(&mut j, &plan_jit, &desc.compact_attrs, None);
        for c in 0..ncols {
            for i in 0..stage.len() {
                assert_eq!(a.col_isnull(c)[i], j.col_isnull(c)[i], "batch col {c} row {i}");
                if !a.col_isnull(c)[i] {
                    assert_eq!(
                        a.col_values(c)[i].as_i64(),
                        j.col_values(c)[i].as_i64(),
                        "batch col {c} row {i}"
                    );
                }
            }
        }
    }

    // Slot lazy path: armed slot vs interpreted slot, full state compare,
    // including resume past the prefix into the varlena tail.
    for (i, (values, isnull)) in rows.iter().enumerate() {
        let mut got = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc.clone()));
        if let SlotData::Heap(h) = &mut got {
            h.jit_deform = Some(kernel.clone());
        }
        let tg = heap_form_tuple(mcx, &desc, values, isnull).unwrap();
        exec_store_heap_tuple_owned(&mut got, mcx, tg);
        let mut want = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc.clone()));
        let tw = heap_form_tuple(mcx, &desc, values, isnull).unwrap();
        exec_store_heap_tuple_owned(&mut want, mcx, tw);
        for attnum in [ncols as i32, desc.natts] {
            slot_getsomeattrs(&mut got, attnum);
            slot_getsomeattrs(&mut want, attnum);
            let (gb, wb) = (got.base(), want.base());
            assert_eq!(gb.tts_nvalid, wb.tts_nvalid, "row {i} attnum {attnum}");
            assert_eq!(gb.tts_flags, wb.tts_flags, "row {i} attnum {attnum}");
            for c in 0..gb.tts_nvalid as usize {
                assert_eq!(gb.tts_isnull[c], wb.tts_isnull[c], "row {i} col {c}");
                if !gb.tts_isnull[c] {
                    if desc.compact_attrs[c].attbyval {
                        assert_eq!(gb.tts_values[c].as_i64(), wb.tts_values[c].as_i64(), "row {i} col {c}");
                    } else {
                        assert_eq!(
                            datum_text_bytes(gb.tts_values[c]),
                            datum_text_bytes(wb.tts_values[c]),
                            "row {i} col {c}"
                        );
                    }
                }
            }
            let (SlotData::Heap(gh), SlotData::Heap(wh)) = (&got, &want) else { unreachable!() };
            assert_eq!(gh.off, wh.off, "row {i} attnum {attnum}");
        }
        exec_clear_tuple(&mut got, mcx);
        exec_clear_tuple(&mut want, mcx);
    }

    // Narrow tuple (pre-ALTER ADD COLUMN image): kernel gated off by
    // ncols <= min(tuple natts, requested), missing-attr pad unchanged.
    let narrow = make_desc(mcx, &[col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN)]);
    let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc.clone()));
    if let SlotData::Heap(h) = &mut slot {
        h.jit_deform = Some(kernel.clone());
    }
    let nt = heap_form_tuple(mcx, &narrow, &[Datum::from_i32(5)], &[false]).unwrap();
    exec_store_heap_tuple_owned(&mut slot, mcx, nt);
    slot_getsomeattrs(&mut slot, desc.natts);
    let b = slot.base();
    assert_eq!(b.tts_values[0].as_i64(), 5);
    assert!(!b.tts_isnull[0]);
    for c in 1..desc.natts as usize {
        assert!(b.tts_isnull[c], "narrow pad col {c}");
    }
    exec_clear_tuple(&mut slot, mcx);
}

// Dict-lane surface (pgrcolumnar dict currency): arm/answer negotiation, the
// fill gates the AM keys its batch fill on, and window-boundary clearing.
#[test]
fn dict_lane_negotiation_round_trip() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let mut soa = SoaBatch::new_in(mcx, 3);

    // Unarmed scan: fail open — every column wants a fill, nothing is dict.
    for c in 0..3 {
        assert!(soa.lane_fill_wanted(c), "unarmed fill col {c}");
        assert!(!soa.dict_want(c), "unarmed dict col {c}");
        assert!(soa.dict_lane(c).is_none());
        assert!(soa.col_datum_ready(c));
    }
    assert!(!soa.lane_read_armed());
    assert!(!soa.col_datum_ready(3), "out-of-range col is never ready");

    // Arm: lane program reads col 1 as codes+dict and col 2 as Raw datums.
    soa.set_dict_want(1);
    soa.set_lane_read(1);
    soa.set_lane_read(2);
    assert!(soa.lane_read_armed());
    assert!(!soa.lane_fill_wanted(0), "unread col needs no Datum fill");
    assert!(soa.lane_fill_wanted(1) && soa.lane_fill_wanted(2));
    assert!(soa.dict_want(1) && !soa.dict_want(0) && !soa.dict_want(2));

    // AM answers col 1 with a dict lane for this window: its Datum cells are
    // stale by contract, every other column is unaffected.
    let codes: [u32; 4] = [1, 0, 2, 1];
    let dict: [Datum; 3] = [Datum::from_i64(10), Datum::from_i64(20), Datum::from_i64(30)];
    let table = SoaDictTable { dict: dict.as_ptr(), ndict: 3, epoch: 7, sorted: true, stitch: core::ptr::null(), gndv: 0, gepoch: 0, lazy: core::ptr::null(), lazy_ensure: None, lazy_ensure_all: None, contig: false };
    soa.begin(codes.len() as u32);
    soa.set_dict_lane(1, SoaDictLane { codes: codes.as_ptr(), table });
    let lane = soa.dict_lane(1).expect("answered");
    assert_eq!(lane.table.epoch, 7);
    assert!(lane.table.sorted);
    for (i, &code) in codes.iter().enumerate() {
        assert_eq!(lane.code(i), code);
        assert_eq!(lane.datum(i).as_i64(), dict[code as usize].as_i64());
    }
    assert!(!soa.col_datum_ready(1), "dict answer means stale Datum cells");
    assert!(soa.col_datum_ready(2));
    assert!(!soa.col_datum_ready(0), "fill-skipped col is not ready");

    // Window boundary (begin) drops the answer: epoch-change invalidation is
    // structural — a stale codes pointer can never leak into the next window.
    soa.begin(2);
    assert!(soa.dict_lane(1).is_none(), "begin clears dict answers");
    assert!(soa.col_datum_ready(1), "cleared lane re-enables the Raw fill");
    assert!(soa.dict_want(1), "the arm itself persists across windows");
}

// dict[codes] gather == full-decode Raw: materializing a dict lane into the
// Datum cells produces exactly what a Raw fill of the same column would,
// writes isnull explicitly (NULL-free is a per-chunk proof), and clears the
// lane so col_datum_ready flips back on.
#[test]
fn dict_gather_matches_full_decode_raw() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let mut soa = SoaBatch::new_in(mcx, 2);
    soa.set_dict_want(0);

    let dict: [Datum; 4] = [
        Datum::from_i64(-1),
        Datum::from_i64(0),
        Datum::from_i64(i64::MAX),
        Datum::from_i64(42),
    ];
    let codes: [u32; 6] = [3, 3, 0, 2, 1, 0];
    let table = SoaDictTable { dict: dict.as_ptr(), ndict: 4, epoch: 0, sorted: false, stitch: core::ptr::null(), gndv: 0, gepoch: 0, lazy: core::ptr::null(), lazy_ensure: None, lazy_ensure_all: None, contig: false };

    soa.begin(codes.len() as u32);
    // Poison the target cells: garbage values, isnull = true. The gather
    // must overwrite both (it may not assume pre-cleared nulls).
    soa.col_values_mut(0).fill(Datum::from_i64(-777));
    soa.col_isnull_mut(0).fill(true);
    soa.set_dict_lane(0, SoaDictLane { codes: codes.as_ptr(), table });

    soa.gather_dict_lane(0);
    assert!(soa.dict_lane(0).is_none(), "gather consumes the answer");
    assert!(soa.col_datum_ready(0));
    // Full decode reference: dict[codes[i]] per row.
    for (i, &code) in codes.iter().enumerate() {
        assert_eq!(soa.col_values(0)[i].as_i64(), dict[code as usize].as_i64(), "row {i}");
        assert!(!soa.col_isnull(0)[i], "row {i} NULL-free proof");
    }

    // Gather on a Raw (unanswered) column is a no-op.
    soa.col_values_mut(1).fill(Datum::from_i64(5));
    soa.gather_dict_lane(1);
    assert_eq!(soa.col_values(1)[0].as_i64(), 5);
}

// Epoch discipline: identity is the epoch (rg index per pinned scan), not
// the pointer — consumers key memos on it and clear at change.
#[test]
fn dict_table_epoch_identity() {
    let dict: [Datum; 2] = [Datum::from_i64(1), Datum::from_i64(2)];
    let rg0 = SoaDictTable { dict: dict.as_ptr(), ndict: 2, epoch: 0, sorted: false, stitch: core::ptr::null(), gndv: 0, gepoch: 0, lazy: core::ptr::null(), lazy_ensure: None, lazy_ensure_all: None, contig: false };
    let rg0b = SoaDictTable { dict: dict.as_ptr(), ndict: 2, epoch: 0, sorted: false, stitch: core::ptr::null(), gndv: 0, gepoch: 0, lazy: core::ptr::null(), lazy_ensure: None, lazy_ensure_all: None, contig: false };
    let rg1 = SoaDictTable { dict: dict.as_ptr(), ndict: 2, epoch: 1, sorted: false, stitch: core::ptr::null(), gndv: 0, gepoch: 0, lazy: core::ptr::null(), lazy_ensure: None, lazy_ensure_all: None, contig: false };
    assert!(rg0.same_identity(&rg0b));
    assert!(!rg0.same_identity(&rg1), "same arena address, new row group: memo must clear");
    assert_eq!(rg1.datum(0).as_i64(), 1);
    assert_eq!(rg1.datum(1).as_i64(), 2);
}

// ---------------------------------------------------------------------------
// for_each_live: the shared word-skip iterator (wordskip-general lane) must
// visit EXACTLY the positions the plain pos..n loop keeps under a per-row
// bit test — same positions, same order — for arbitrary masks, pos offsets,
// ragged tails, and EXACT-LENGTH word slices (nwords = ceil(n/64), the
// `seq_scan_batch_skip_sel` shape), and must stop at the first Err exactly
// where the plain loop's `?` would.
// ---------------------------------------------------------------------------
#[test]
fn for_each_live_matches_plain_loop_on_set_bits() {
    let mut seed = 0x2545F4914F6CDD1Du64;
    let mut rng = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for case in 0..500u32 {
        let n: u32 = match case % 6 {
            0 => 1,
            1 => 63,
            2 => 64,
            3 => 65,
            4 => 128,
            _ => (rng() % (64 * SOA_BM_WORDS as u64 - 1) + 1) as u32,
        };
        let pos: u32 = (rng() % (n as u64 + 1)) as u32;
        let nwords = (n as usize).div_ceil(64);
        let mut words: Vec<u64> = (0..nwords)
            .map(|_| match case % 4 {
                0 => 0,
                1 => !0,
                _ => rng(),
            })
            .collect();
        // Producer contract: bits at/past n are zero.
        if n % 64 != 0 {
            words[nwords - 1] &= (1u64 << (n % 64)) - 1;
        }
        let expected: Vec<u32> = (pos..n)
            .filter(|&i| words[(i / 64) as usize] & (1u64 << (i % 64)) != 0)
            .collect();
        let mut got = Vec::new();
        for_each_live::<()>(Some(&words), pos, n, |i| {
            got.push(i);
            Ok(())
        })
        .unwrap();
        assert_eq!(got, expected, "case {case} n={n} pos={pos}");
        // None mask = the plain dense loop.
        let mut all = Vec::new();
        for_each_live::<()>(None, pos, n, |i| {
            all.push(i);
            Ok(())
        })
        .unwrap();
        assert_eq!(all, (pos..n).collect::<Vec<_>>());
        // Err stops the walk at that position (the plain loop's `?`).
        if !expected.is_empty() {
            let stop_at = expected[(rng() % expected.len() as u64) as usize];
            let mut seen = Vec::new();
            let r = for_each_live(Some(&words), pos, n, |i| {
                if i == stop_at {
                    return Err(i);
                }
                seen.push(i);
                Ok(())
            });
            assert_eq!(r, Err(stop_at));
            assert_eq!(
                seen,
                expected.iter().copied().take_while(|&i| i != stop_at).collect::<Vec<_>>()
            );
        }
    }
}

// for_each_live_onebody: identical visited stream to for_each_live for both
// Some masks and the dense None case (the single-body word loop).
#[test]
fn for_each_live_onebody_matches_two_arm() {
    let mut seed = 0xA0761D6478BD642Fu64;
    let mut rng = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for case in 0..300u32 {
        let n: u32 = (rng() % (64 * SOA_BM_WORDS as u64 - 1) + 1) as u32;
        let pos: u32 = (rng() % (n as u64 + 1)) as u32;
        let nwords = (n as usize).div_ceil(64);
        let mut words: Vec<u64> = (0..nwords).map(|_| rng()).collect();
        if n % 64 != 0 {
            words[nwords - 1] &= (1u64 << (n % 64)) - 1;
        }
        for live in [None, Some(&words[..])] {
            let mut a = Vec::new();
            for_each_live::<()>(live, pos, n, |i| {
                a.push(i);
                Ok(())
            })
            .unwrap();
            let mut b = Vec::new();
            for_each_live_onebody::<()>(live, pos, n, |i| {
                b.push(i);
                Ok(())
            })
            .unwrap();
            assert_eq!(a, b, "case {case} n={n} pos={pos} some={}", live.is_some());
        }
    }
}

// --- WS-AH wave-9 sub-region (K1 late materialization, band 91001+) --------

// K1 inc-2 deform split pins (`soa_deform_columns_set`): narrowed staging
// (explicit column set, no sel) + survivor completion (sel words) compose to
// exactly the full deform's cells for every selected row; word-skip does
// ZERO work on all-zero 64-row selection words (proven off the fresh
// batch's null-Datum cells); kind-1 hasnulls rows are live from classify
// regardless; kind-2 narrow rows carry the fallback bit and never fill;
// re-completion is idempotent per (column, row).
#[test]
fn k1_latemat_deform_split_matches_full_deform() {
    use ::types_tuple::TYPALIGN_SHORT;
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    // int4, int2, int8 fixed prefix; text tail past the prefix.
    let desc = make_desc(
        mcx,
        &[
            col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN),
            col(2, 2, true, TYPALIGN_SHORT, TYPSTORAGE_PLAIN),
            col(3, 8, true, TYPALIGN_DOUBLE, TYPSTORAGE_PLAIN),
            col(4, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED),
        ],
    );
    let ncols = 3usize;
    let plan = SoaDeformPlan::try_new(mcx, &desc.compact_attrs, ncols).unwrap();
    let txt = text_varlena("tail");
    // 130 rows -> 3 selection words: word 0 partial, word 1 all-zero
    // (word-skip), word 2 the 2-row tail (tail-mask edge).
    let n = 130usize;
    let null_row = 5usize; // kind-1 (hasnulls): full deform at classify
    let narrow_row = 70usize; // kind-2: pre-ALTER image, fallback bit
    let narrow = make_desc(mcx, &[col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN)]);
    let mut tuples = Vec::new();
    for i in 0..n {
        if i == narrow_row {
            tuples.push(
                heap_form_tuple(mcx, &narrow, &[Datum::from_i32(91001)], &[false]).unwrap(),
            );
            continue;
        }
        let values = [
            Datum::from_i32(91001 + i as i32),
            Datum::from_i16((i % 7) as i16 - 3),
            Datum::from_i64(91001i64 * (i as i64 + 1)),
            text_datum(&txt),
        ];
        let isnull = [false, i == null_row, false, false];
        tuples.push(heap_form_tuple(mcx, &desc, &values, &isnull).unwrap());
    }
    // Reference: the full staging deform (today's bytes).
    let mut full = SoaBatch::new_in(mcx, plan.ncols());
    full.begin(n as u32);
    for (i, t) in tuples.iter().enumerate() {
        soa_classify_row(&mut full, &plan, &desc.compact_attrs, i as u32, t);
    }
    soa_deform_columns(&mut full, &plan, &desc.compact_attrs, None);
    // Late-mat: identical classification, staging narrowed to col 0 only.
    let mut lm = SoaBatch::new_in(mcx, plan.ncols());
    lm.begin(n as u32);
    for (i, t) in tuples.iter().enumerate() {
        soa_classify_row(&mut lm, &plan, &desc.compact_attrs, i as u32, t);
    }
    soa_deform_columns_set(&mut lm, &plan, &desc.compact_attrs, &[0], None);
    // Classification parity (kinds are staging-independent).
    assert_eq!(lm.fallback_words(), full.fallback_words());
    assert!(lm.is_fallback(narrow_row as u32));
    // Pass A: the staged column matches the full deform everywhere.
    for i in 0..n {
        if lm.is_fallback(i as u32) {
            continue;
        }
        assert_eq!(lm.col_isnull(0)[i], full.col_isnull(0)[i], "col0 isnull row {i}");
        assert_eq!(
            lm.col_values(0)[i].as_i64(),
            full.col_values(0)[i].as_i64(),
            "col0 row {i}"
        );
        // Kind-1 rows deformed fully at classify: live before completion.
        if i == null_row {
            assert!(lm.col_isnull(1)[i], "kind-1 null live at classify");
            assert_eq!(lm.col_values(2)[i].as_i64(), full.col_values(2)[i].as_i64());
        }
    }
    // Deferred kind-0 cells are untouched (the fresh batch's null Datum).
    for i in [0usize, 64, 129] {
        assert_eq!(lm.col_values(2)[i].as_i64(), 0, "deferred col2 stale row {i}");
    }
    // Pass B: complete cols {1,2} for a selection with a partial word, an
    // all-zero word (word-skip), and the tail-masked full word.
    let sel = [0xAAAA_AAAA_AAAA_AAAAu64, 0, u64::MAX];
    for round in 0..2 {
        soa_deform_columns_set(&mut lm, &plan, &desc.compact_attrs, &[1, 2], Some(&sel));
        for i in 0..n {
            if lm.is_fallback(i as u32) {
                continue;
            }
            let selected = sel[i / 64] & (1u64 << (i % 64)) != 0;
            if selected || i == null_row {
                for c in [1usize, 2] {
                    assert_eq!(
                        lm.col_isnull(c)[i],
                        full.col_isnull(c)[i],
                        "round {round} col{c} isnull row {i}"
                    );
                    if !full.col_isnull(c)[i] {
                        assert_eq!(
                            lm.col_values(c)[i].as_i64(),
                            full.col_values(c)[i].as_i64(),
                            "round {round} col{c} row {i}"
                        );
                    }
                }
            } else {
                // Word-skip / cleared-bit proof: unselected kind-0 rows'
                // deferred cells were never written (col2 real values are
                // all nonzero).
                assert_eq!(
                    lm.col_values(2)[i].as_i64(),
                    0,
                    "round {round} unselected col2 written row {i}"
                );
            }
        }
    }
    // Kind-2 discipline unchanged: fallback rows never publish.
    let mut slot = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc));
    let nt = heap_form_tuple(mcx, &narrow, &[Datum::from_i32(91001)], &[false]).unwrap();
    exec_store_heap_tuple_owned(&mut slot, mcx, nt);
    assert!(!soa_store_prefix(&mut slot, &lm, narrow_row as u32));
    exec_clear_tuple(&mut slot, mcx);
}

// K1 inc-3 density-cutover pin (`soa_deform_columns_set` selected pass):
// a partial word at or above DENSE_WORD_CUTOVER set bits takes the dense
// row loop — every SELECTED kind-0 cell must equal the full-deform oracle
// (the observable contract; unselected kind-0 cells MAY over-fill, which
// is idempotent value movement no reader consumes), kind-1 rows stay live
// from classify, and kind-2 fallback rows never fill even inside a dense
// word. A sparse control word on the same call proves the bit-walk arm
// still skips its unselected cells.
#[test]
fn k1_latemat_dense_cutover_matches_full_deform() {
    use ::types_tuple::TYPALIGN_SHORT;
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = make_desc(
        mcx,
        &[
            col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN),
            col(2, 2, true, TYPALIGN_SHORT, TYPSTORAGE_PLAIN),
            col(3, 8, true, TYPALIGN_DOUBLE, TYPSTORAGE_PLAIN),
        ],
    );
    let plan = SoaDeformPlan::try_new(mcx, &desc.compact_attrs, 3).unwrap();
    let n = 128usize; // 2 full words: dense-cutover word + sparse control
    let null_row = 9usize; // kind-1 inside the dense word
    let narrow_row = 17usize; // kind-2 inside the dense word
    let narrow = make_desc(mcx, &[col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN)]);
    let mut tuples = Vec::new();
    for i in 0..n {
        if i == narrow_row {
            tuples.push(
                heap_form_tuple(mcx, &narrow, &[Datum::from_i32(91001)], &[false]).unwrap(),
            );
            continue;
        }
        let values = [
            Datum::from_i32(91001 + i as i32),
            Datum::from_i16((i % 5) as i16 - 2),
            Datum::from_i64(91003i64 * (i as i64 + 1)),
        ];
        let isnull = [false, i == null_row, false];
        tuples.push(heap_form_tuple(mcx, &desc, &values, &isnull).unwrap());
    }
    let mut full = SoaBatch::new_in(mcx, plan.ncols());
    full.begin(n as u32);
    for (i, t) in tuples.iter().enumerate() {
        soa_classify_row(&mut full, &plan, &desc.compact_attrs, i as u32, t);
    }
    soa_deform_columns(&mut full, &plan, &desc.compact_attrs, None);
    let mut lm = SoaBatch::new_in(mcx, plan.ncols());
    lm.begin(n as u32);
    for (i, t) in tuples.iter().enumerate() {
        soa_classify_row(&mut lm, &plan, &desc.compact_attrs, i as u32, t);
    }
    soa_deform_columns_set(&mut lm, &plan, &desc.compact_attrs, &[0], None);
    // Word 0: 58 set bits (>= the 48 cutover — dense row loop; 6 holes).
    // Word 1: 3 set bits (bit-walk control).
    let dense_word: u64 = !0u64 & !(1 << 3) & !(1 << 21) & !(1 << 33) & !(1 << 40) & !(1 << 55) & !(1 << 63);
    assert!(dense_word.count_ones() >= 48 && dense_word != u64::MAX);
    let sel = [dense_word, (1u64 << 2) | (1u64 << 40) | (1u64 << 63)];
    soa_deform_columns_set(&mut lm, &plan, &desc.compact_attrs, &[1, 2], Some(&sel));
    for i in 0..n {
        if lm.is_fallback(i as u32) {
            // Kind-2 never fills, dense word or not (fresh null-Datum cell).
            assert_eq!(lm.col_values(2)[i].as_i64(), 0, "kind-2 filled row {i}");
            continue;
        }
        let selected = sel[i / 64] & (1u64 << (i % 64)) != 0;
        if selected || i == null_row {
            for c in [1usize, 2] {
                assert_eq!(lm.col_isnull(c)[i], full.col_isnull(c)[i], "col{c} isnull row {i}");
                if !full.col_isnull(c)[i] {
                    assert_eq!(
                        lm.col_values(c)[i].as_i64(),
                        full.col_values(c)[i].as_i64(),
                        "col{c} row {i}"
                    );
                }
            }
        } else if i >= 64 {
            // The sparse control word keeps the bit-walk's skip discipline.
            assert_eq!(lm.col_values(2)[i].as_i64(), 0, "sparse word over-fill row {i}");
        }
        // Dense-word holes (i < 64, unselected): over-fill is PERMITTED —
        // no assertion either way (the cells are unread by contract).
    }
}

// --- end WS-AH wave-9 sub-region --------------------------------------------

// --- AGGSEQ-STAGE sub-region (walk-tail plans: heap prefixes crossing a
// varlena column) --------------------------------------------------------

// Shape contract of `try_new_walk`: refuses what `try_new` hosts (all
// fixed-width — callers try that first), hosts what `try_new` refuses
// (varlena inside the prefix), refuses cstrings anywhere in the prefix
// (the varkey plan's own refusal), and a varlena at column 0 is a legal
// empty-head walk plan that is NOT virtual (the explicit `walk_from`
// disambiguates the empty offset chain).
#[test]
fn varwalk_plan_shape_contract() {
    use ::types_tuple::TYPALIGN_SHORT;
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let fixed = make_desc(
        mcx,
        &[
            col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN),
            col(2, 8, true, TYPALIGN_DOUBLE, TYPSTORAGE_PLAIN),
        ],
    );
    assert!(SoaDeformPlan::try_new(mcx, &fixed.compact_attrs, 2).is_some());
    assert!(SoaDeformPlan::try_new_walk(mcx, &fixed.compact_attrs, 2).is_none());

    let crossing = make_desc(
        mcx,
        &[
            col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN),
            col(2, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED),
            col(3, 2, true, TYPALIGN_SHORT, TYPSTORAGE_PLAIN),
        ],
    );
    assert!(SoaDeformPlan::try_new(mcx, &crossing.compact_attrs, 3).is_none());
    let plan = SoaDeformPlan::try_new_walk(mcx, &crossing.compact_attrs, 3).unwrap();
    assert_eq!(plan.walk_from(), Some(1));
    assert_eq!(plan.ncols(), 3);
    assert!(!plan.is_virtual());
    // A prefix that stops BEFORE the varlena needs no walk.
    assert!(SoaDeformPlan::try_new_walk(mcx, &crossing.compact_attrs, 1).is_none());

    // cstring (attlen == -2) anywhere in the prefix refuses.
    let cstr = make_desc(
        mcx,
        &[
            col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN),
            col(2, -2, false, ::types_tuple::TYPALIGN_CHAR, TYPSTORAGE_PLAIN),
            col(3, 2, true, TYPALIGN_SHORT, TYPSTORAGE_PLAIN),
        ],
    );
    assert!(SoaDeformPlan::try_new_walk(mcx, &cstr.compact_attrs, 3).is_none());

    // Varlena at column 0: empty head, walk_from == 0, NOT virtual.
    let v0 = make_desc(
        mcx,
        &[
            col(1, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED),
            col(2, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN),
        ],
    );
    let p0 = SoaDeformPlan::try_new_walk(mcx, &v0.compact_attrs, 2).unwrap();
    assert_eq!(p0.walk_from(), Some(0));
    assert!(!p0.is_virtual());

    // Degenerate asks refuse.
    assert!(SoaDeformPlan::try_new_walk(mcx, &crossing.compact_attrs, 0).is_none());
    assert!(SoaDeformPlan::try_new_walk(mcx, &crossing.compact_attrs, 4).is_none());
}

// THE walk-deform oracle pin: every staged cell of a walk-tail batch —
// head, varlena (in-page pointer Datums), and post-varlena tail whose
// offsets vary PER ROW — equals the per-row slot deform of the same tuple
// image, for kind-0 dense rows, kind-1 hasnulls rows (nulls ON and PAST
// the varlena), and short/long/empty varlena headers; kind-2 narrow rows
// keep the fallback discipline. The prefix publish (`soa_store_prefix`)
// leaves EXACTLY `slot_getsomeattrs(ncols)`'s resume state (values,
// isnull, nvalid, byte offset, SLOW flag) so lazy deform PAST the prefix
// — through the varlena — reads back identical cells.
#[test]
fn varwalk_deform_matches_per_row_oracle() {
    use ::types_slot::TTS_FLAG_SLOW;
    use ::types_tuple::TYPALIGN_SHORT;
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    // int4 head | text | int2, int8 (double-align re-derived per row), int4
    // = the 5-column staged prefix; int4 + text PAST the prefix exercise
    // the store_prefix resume.
    let desc = make_desc(
        mcx,
        &[
            col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN),
            col(2, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED),
            col(3, 2, true, TYPALIGN_SHORT, TYPSTORAGE_PLAIN),
            col(4, 8, true, TYPALIGN_DOUBLE, TYPSTORAGE_PLAIN),
            col(5, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN),
            col(6, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN),
            col(7, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED),
        ],
    );
    let ncols = 5usize;
    let plan = SoaDeformPlan::try_new_walk(mcx, &desc.compact_attrs, ncols).unwrap();
    assert_eq!(plan.walk_from(), Some(1));
    let n = 67usize; // 2 words, tail-masked second word
    let null_varlena_row = 3usize; // kind-1: NULL ON the varlena
    let null_tail_row = 9usize; // kind-1: NULL past the varlena (int8)
    let narrow_row = 41usize; // kind-2: pre-ALTER image, fallback bit
    let long_row = 17usize; // 4-byte-header varlena (aligned form)
    let narrow = make_desc(mcx, &[col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN)]);
    let long_text = "y".repeat(211);
    let mut texts = Vec::new();
    for i in 0..n {
        // Varying lengths (0..=10) so every row's post-varlena offsets
        // differ — the load-bearing property the static chain cannot host.
        let short = "x".repeat(i % 11);
        texts.push(text_varlena(if i == long_row { &long_text } else { &short }));
    }
    let tail_txt = text_varlena("past-prefix");
    let mut tuples = Vec::new();
    for i in 0..n {
        if i == narrow_row {
            tuples.push(
                heap_form_tuple(mcx, &narrow, &[Datum::from_i32(91001)], &[false]).unwrap(),
            );
            continue;
        }
        let values = [
            Datum::from_i32(91001 + i as i32),
            text_datum(&texts[i]),
            Datum::from_i16((i % 7) as i16 - 3),
            Datum::from_i64(91003i64 * (i as i64 + 1)),
            Datum::from_i32(-(i as i32)),
            Datum::from_i32(7 * i as i32),
            text_datum(&tail_txt),
        ];
        let isnull = [
            false,
            i == null_varlena_row,
            false,
            i == null_tail_row,
            false,
            false,
            false,
        ];
        tuples.push(heap_form_tuple(mcx, &desc, &values, &isnull).unwrap());
    }
    let mut soa = SoaBatch::new_in(mcx, plan.ncols());
    soa.begin(n as u32);
    for (i, t) in tuples.iter().enumerate() {
        soa_classify_row(&mut soa, &plan, &desc.compact_attrs, i as u32, t);
    }
    soa_deform_columns(&mut soa, &plan, &desc.compact_attrs, None);
    assert!(soa.is_fallback(narrow_row as u32));

    // Per-row oracle + resume-state compare, over the SAME tuple images.
    let mut oracle = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc.clone()));
    let mut published = make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc.clone()));
    for (i, t) in tuples.iter().enumerate() {
        if i == narrow_row {
            exec_clear_tuple(&mut published, mcx);
            // SAFETY: alias of the live formed image (mcx outlives the slot use).
            let alias = unsafe {
                HeapTupleData::from_raw_parts(t.header_ptr(), t.t_len, t.t_self, t.t_tableOid)
            };
            exec_store_heap_tuple(&mut published, mcx, alias);
            assert!(!soa_store_prefix(&mut published, &soa, i as u32), "kind-2 must not publish");
            continue;
        }
        // SAFETY: as above — both slots alias the same live image, so text
        // cells compare by POINTER (the pin-holding/aliasing contract).
        let (a1, a2) = unsafe {
            (
                HeapTupleData::from_raw_parts(t.header_ptr(), t.t_len, t.t_self, t.t_tableOid),
                HeapTupleData::from_raw_parts(t.header_ptr(), t.t_len, t.t_self, t.t_tableOid),
            )
        };
        exec_clear_tuple(&mut oracle, mcx);
        exec_store_heap_tuple(&mut oracle, mcx, a1);
        slot_getsomeattrs(&mut oracle, ncols as i32);
        {
            let ob = oracle.base();
            for c in 0..ncols {
                assert_eq!(
                    soa.col_isnull(c)[i],
                    ob.tts_isnull[c],
                    "isnull col {c} row {i}"
                );
                if !ob.tts_isnull[c] {
                    assert_eq!(
                        soa.col_values(c)[i].as_i64(),
                        ob.tts_values[c].as_i64(),
                        "value col {c} row {i} (varlena cells compare by pointer)"
                    );
                }
            }
        }
        // Staged varlena cells alias the tuple image (R3v pin-holding).
        if !soa.col_isnull(1)[i] {
            let p = soa.col_values(1)[i].as_usize();
            let base = t.getstruct() as usize;
            assert!(p >= base && p < base + t.t_len as usize, "text cell outside image row {i}");
        }
        // Publish parity: store_prefix == slot_getsomeattrs(ncols) state.
        exec_clear_tuple(&mut published, mcx);
        exec_store_heap_tuple(&mut published, mcx, a2);
        assert!(soa_store_prefix(&mut published, &soa, i as u32));
        {
            let (ob, pb) = (oracle.base(), published.base());
            assert_eq!(pb.tts_nvalid, ncols as i16, "nvalid row {i}");
            assert_eq!(
                pb.tts_flags & TTS_FLAG_SLOW,
                ob.tts_flags & TTS_FLAG_SLOW,
                "SLOW flag row {i}"
            );
        }
        let (SlotData::Heap(oh), SlotData::Heap(ph)) = (&oracle, &published) else {
            unreachable!("heap slots")
        };
        assert_eq!(ph.off, oh.off, "resume byte offset row {i}");
        // Lazy deform PAST the prefix, THROUGH the varlena, off the
        // published resume state: identical to the whole-row oracle.
        slot_getsomeattrs(&mut oracle, 7);
        slot_getsomeattrs(&mut published, 7);
        let (ob, pb) = (oracle.base(), published.base());
        for c in ncols..7 {
            assert_eq!(pb.tts_isnull[c], ob.tts_isnull[c], "resumed isnull col {c} row {i}");
            if !ob.tts_isnull[c] {
                assert_eq!(
                    pb.tts_values[c].as_i64(),
                    ob.tts_values[c].as_i64(),
                    "resumed value col {c} row {i}"
                );
            }
        }
    }
    exec_clear_tuple(&mut oracle, mcx);
    exec_clear_tuple(&mut published, mcx);
}

// The (first, last)/walk split under a single-column ask: a HEAD-resident
// ask keeps today's static single-column pass and skips the walk (tail
// cells stay the fresh batch's null Datums — bitmap-only consumers, and
// qual-only stagings never publish); a TAIL-resident ask runs the walk
// alone (whole tail filled, head cells stay stale).
#[test]
fn varwalk_qual_col_only_split() {
    use ::types_tuple::TYPALIGN_SHORT;
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = make_desc(
        mcx,
        &[
            col(1, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN),
            col(2, -1, false, TYPALIGN_INT, TYPSTORAGE_EXTENDED),
            col(3, 2, true, TYPALIGN_SHORT, TYPSTORAGE_PLAIN),
        ],
    );
    let plan = SoaDeformPlan::try_new_walk(mcx, &desc.compact_attrs, 3).unwrap();
    let txt = text_varlena("walk");
    let n = 5usize;
    let mut tuples = Vec::new();
    for i in 0..n {
        let values =
            [Datum::from_i32(91001 + i as i32), text_datum(&txt), Datum::from_i16(i as i16)];
        tuples.push(heap_form_tuple(mcx, &desc, &values, &[false, false, false]).unwrap());
    }
    // Head ask: col 0 filled, tail untouched.
    let mut head = SoaBatch::new_in(mcx, plan.ncols());
    head.begin(n as u32);
    for (i, t) in tuples.iter().enumerate() {
        soa_classify_row(&mut head, &plan, &desc.compact_attrs, i as u32, t);
    }
    soa_deform_columns(&mut head, &plan, &desc.compact_attrs, Some(0));
    for i in 0..n {
        assert_eq!(head.col_values(0)[i].as_i64(), 91001 + i as i64);
        assert_eq!(head.col_values(2)[i].as_i64(), 0, "tail filled under a head ask");
    }
    // Tail ask: the walk fills the whole tail; head col stays stale.
    let mut tail = SoaBatch::new_in(mcx, plan.ncols());
    tail.begin(n as u32);
    for (i, t) in tuples.iter().enumerate() {
        soa_classify_row(&mut tail, &plan, &desc.compact_attrs, i as u32, t);
    }
    soa_deform_columns(&mut tail, &plan, &desc.compact_attrs, Some(2));
    for i in 0..n {
        assert_eq!(tail.col_values(2)[i].as_i64(), i as i64, "tail ask row {i}");
        assert_eq!(tail.col_values(0)[i].as_i64(), 0, "head filled under a tail ask");
        assert_eq!(datum_text_bytes(tail.col_values(1)[i]), b"walk", "varlena in the walked tail");
    }
}

// --- end AGGSEQ-STAGE sub-region ------------------------------------------
