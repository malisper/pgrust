use std::rc::Rc;

use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, TupleDescData, TYPALIGN_INT, TYPSTORAGE_PLAIN,
};

use crate::*;

fn leaked_mcx() -> Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("tuplestore-test")));
    m.mcx()
}

fn int4_desc(mcx: Mcx<'static>, natts: i32) -> Rc<TupleDescData<'static>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for i in 0..natts {
        let att = FormData_pg_attribute {
            attnum: (i + 1) as i16,
            atttypid: 23,
            attlen: 4,
            attbyval: true,
            attalign: TYPALIGN_INT,
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        };
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    Rc::new(TupleDescData {
        natts,
        tdtypeid: 2249,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn read_i32(slot: &mut SlotData<'_>) -> i32 {
    exectuples::slot_getallattrs(slot);
    assert!(!slot.base().tts_isnull[0]);
    slot.base().tts_values[0].as_i32()
}

fn put_i32(ts: &mut Tuplestore, desc: &TupleDescData<'_>, v: i32) {
    ts.putvalues(desc, &[Datum::from_i32(v)], &[false]).unwrap();
}

#[test]
fn putvalues_gettupleslot_roundtrip() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(false, true, 64);
    for v in 0..100 {
        put_i32(&mut ts, &desc, v);
    }
    assert_eq!(ts.tuple_count(), 100);
    assert!(!ts.ateof());

    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    for v in 0..100 {
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), v);
    }
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert!(slot.base().is_empty());
    assert!(ts.ateof());
    ts.end();
}

#[test]
fn eof_reader_advances_with_writes() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(false, true, 64);
    put_i32(&mut ts, &desc, 1);
    let mut slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    // The active read pointer at EOF stays at EOF across puts (C API spec).
    put_i32(&mut ts, &desc, 2);
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
}

#[test]
fn puttupleslot_copies_out_of_virtual_slot() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 2);
    let mut ts = Tuplestore::begin_heap(false, true, 64);
    let mut vslot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc.clone()));
    {
        let base = vslot.base_mut();
        base.tts_values[0] = Datum::from_i32(7);
        base.tts_values[1] = Datum::from_i32(9);
        base.tts_isnull[0] = false;
        base.tts_isnull[1] = false;
    }
    exectuples::exec_store_virtual_tuple(&mut vslot);
    ts.puttupleslot(&mut vslot, mcx).unwrap();
    exectuples::exec_clear_tuple(&mut vslot, mcx);

    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    exectuples::slot_getallattrs(&mut slot);
    assert_eq!(slot.base().tts_values[0].as_i32(), 7);
    assert_eq!(slot.base().tts_values[1].as_i32(), 9);
}

#[test]
fn copy_true_survives_clear() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(false, true, 64);
    put_i32(&mut ts, &desc, 42);
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    assert!(ts.gettupleslot(true, true, &mut slot, mcx).unwrap());
    ts.clear();
    assert_eq!(read_i32(&mut slot), 42);
    assert_eq!(ts.tuple_count(), 0);
    exectuples::exec_clear_tuple(&mut slot, mcx);
}

#[test]
fn clear_then_reuse_and_rescan() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(false, true, 64);
    for v in 0..10 {
        put_i32(&mut ts, &desc, v);
    }
    ts.clear();
    put_i32(&mut ts, &desc, 99);
    assert_eq!(ts.tuple_count(), 1);
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 99);
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    ts.rescan();
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 99);
}

#[test]
fn grow_memtuples_past_initial_size() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(false, true, 4096);
    let n = (INITIAL_MEMTUPSIZE * 3) as i32;
    for v in 0..n {
        put_i32(&mut ts, &desc, v);
    }
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    for v in 0..n {
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), v);
    }
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
}

#[test]
fn backward_walks_to_start_then_none() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(true, true, 64);
    for v in [1, 2, 3] {
        put_i32(&mut ts, &desc, v);
    }
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    for v in [1, 2, 3] {
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), v);
    }
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert!(ts.ateof());

    // C: backward after EOF re-returns the last tuple, then walks back.
    for v in [3, 2, 1] {
        assert!(ts.gettupleslot(false, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), v);
    }
    assert!(!ts.gettupleslot(false, false, &mut slot, mcx).unwrap());
    assert!(!ts.ateof());

    // Forward again from the start.
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 1);
}

#[test]
fn backward_before_eof_returns_tuple_before_last() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(true, true, 64);
    for v in [1, 2, 3] {
        put_i32(&mut ts, &desc, v);
    }
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 2);
    // Last returned was 2; backward yields the tuple before it.
    assert!(ts.gettupleslot(false, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 1);
}

#[test]
fn backward_at_start_is_none_and_rescan_resets() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(true, true, 64);
    put_i32(&mut ts, &desc, 7);
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    assert!(!ts.gettupleslot(false, false, &mut slot, mcx).unwrap());

    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    ts.rescan();
    assert!(!ts.gettupleslot(false, false, &mut slot, mcx).unwrap());
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 7);
}

#[test]
#[should_panic(expected = "work_mem")]
fn spill_to_tape_is_loud() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(false, true, 1);
    for v in 0..INITIAL_MEMTUPSIZE as i32 {
        put_i32(&mut ts, &desc, v);
    }
}

#[test]
#[should_panic(expected = "multi-reader")]
fn extra_read_pointer_is_loud() {
    let mut ts = Tuplestore::begin_heap(false, true, 64);
    let _ = ts.alloc_read_pointer(0);
}

#[test]
fn hold_registry_roundtrip_and_staleness() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let h = hold::register(Tuplestore::begin_heap(false, true, 64));
    hold::with_store(h, |ts| put_i32(ts, &desc, 5));

    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    hold::with_store(h, |ts| {
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    });
    assert_eq!(read_i32(&mut slot), 5);
    exectuples::exec_clear_tuple(&mut slot, mcx);

    hold::end(h);
    hold::end(h); // double-end is a no-op, as C never double-frees a live ptr
    let stale = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        hold::with_store(h, |ts| ts.tuple_count())
    }));
    assert!(stale.is_err());
    hold::end(types_portal::TuplestoreHandle::NULL);
}
#[test]
fn putvalues_packs_varlena_short_form() {
    let mcx = leaked_mcx();
    let att = FormData_pg_attribute {
        attnum: 1,
        atttypid: 25,
        attlen: -1,
        attbyval: false,
        attalign: TYPALIGN_INT,
        attstorage: b'x' as i8,
        ..Default::default()
    };
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute::populate_from(&att));
    attrs.push(att);
    let desc = Rc::new(TupleDescData {
        natts: 1, tdtypeid: 2249, tdtypmod: -1, tdrefcount: -1,
        constr: None, compact_attrs: compact, attrs,
    });

    let mut image: Vec<u8> = vec![];
    let payload = b"4MB";
    let hdr = ((payload.len() + 4) as u32) << 2;
    image.extend_from_slice(&hdr.to_ne_bytes());
    image.extend_from_slice(payload);
    let image = Box::leak(image.into_boxed_slice());
    let d = Datum::from_usize(image.as_ptr() as usize);

    let mtup = heaptuple::heap_form_minimal_tuple(mcx, &desc, &[d], &[false], 0).unwrap();
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    exectuples::exec_store_minimal_tuple_owned(&mut slot, mcx, mtup);
    exectuples::slot_getallattrs(&mut slot);
    // heap_form packs the 4B-header input to the 1B short form (C fill_val).
    let out = slot.base().tts_values[0];
    let p = out.as_usize() as *const u8;
    let b0 = unsafe { *p };
    assert_eq!(b0 & 0x01, 1, "short-form varlena header");
    assert_eq!((b0 >> 1) as usize, 1 + payload.len());
    let data = unsafe { std::slice::from_raw_parts(p.add(1), payload.len()) };
    assert_eq!(data, payload);
}
