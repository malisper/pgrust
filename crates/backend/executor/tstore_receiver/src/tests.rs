use std::rc::Rc;

use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_slot::TupleSlotKind;
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, TupleDescData, TYPALIGN_INT, TYPSTORAGE_PLAIN,
};

use crate::*;

fn leaked_mcx() -> Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("tstore-test")));
    m.mcx()
}

fn desc(mcx: Mcx<'static>, attlen: i16, attbyval: bool) -> Rc<TupleDescData<'static>> {
    let att = FormData_pg_attribute {
        attnum: 1,
        atttypid: if attbyval { 23 } else { 25 },
        attlen,
        attbyval,
        attalign: TYPALIGN_INT,
        attstorage: TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute::populate_from(&att));
    attrs.push(att);
    Rc::new(TupleDescData {
        natts: 1,
        tdtypeid: 2249,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

#[test]
fn receive_slot_materializes_into_store() {
    let mcx = leaked_mcx();
    let d = desc(mcx, 4, true);
    let h = tuplestore::hold::register(tuplestore::Tuplestore::begin_heap(false, true, 64));
    let mut dr = tstore_create_DR();
    set_params(&mut dr, h, false, None, None);
    dr.startup(1 /* CMD_SELECT */, &d).unwrap();

    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(d.clone()));
    slot.base_mut().tts_values[0] = Datum::from_i32(31);
    slot.base_mut().tts_isnull[0] = false;
    exectuples::exec_store_virtual_tuple(&mut slot);
    assert!(dr.receive_slot(&mut slot).unwrap());
    dr.shutdown();

    let mut out = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(d));
    tuplestore::hold::with_store(h, |ts| {
        assert_eq!(ts.tuple_count(), 1);
        assert!(ts.gettupleslot(true, false, &mut out, mcx).unwrap());
    });
    exectuples::slot_getallattrs(&mut out);
    assert_eq!(out.base().tts_values[0].as_i32(), 31);
    exectuples::exec_clear_tuple(&mut out, mcx);
    tuplestore::hold::end(h);
}

#[test]
fn detoast_arm_stores_inline_varlena() {
    let mcx = leaked_mcx();
    let d = desc(mcx, -1, false);
    let h = tuplestore::hold::register(tuplestore::Tuplestore::begin_heap(false, true, 64));
    let mut dr = tstore_create_DR();
    set_params(&mut dr, h, true, None, None);
    dr.startup(1, &d).unwrap();

    let mut payload = PgVec::new_in(mcx);
    let body = b"held cursor row";
    let len = (4 + body.len()) as u32;
    payload.extend_from_slice(&types_tuple::varatt::set_varsize_4b_word(len).to_ne_bytes());
    payload.extend_from_slice(body);
    let val = Datum::from_usize(payload.leak().as_ptr() as usize);

    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(d.clone()));
    slot.base_mut().tts_values[0] = val;
    slot.base_mut().tts_isnull[0] = false;
    exectuples::exec_store_virtual_tuple(&mut slot);
    assert!(dr.receive_slot(&mut slot).unwrap());
    assert!(dr.receive_slot(&mut slot).unwrap());
    dr.shutdown();

    let mut out = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(d));
    tuplestore::hold::with_store(h, |ts| {
        assert_eq!(ts.tuple_count(), 2);
        assert!(ts.gettupleslot(true, false, &mut out, mcx).unwrap());
    });
    exectuples::slot_getallattrs(&mut out);
    let stored = out.base().tts_values[0].as_usize() as *const u8;
    // SAFETY: live 4B-header varlena written above and copied by the store.
    unsafe {
        assert_eq!(types_tuple::varatt::varsize_4b(stored), len as usize);
        assert_eq!(core::slice::from_raw_parts(stored.add(4), body.len()), body);
    }
    exectuples::exec_clear_tuple(&mut out, mcx);
    tuplestore::hold::end(h);
}

#[test]
fn detoast_without_varlena_columns_takes_notoast_arm() {
    let mcx = leaked_mcx();
    let d = desc(mcx, 4, true);
    let mut dr = tstore_create_DR();
    set_params(&mut dr, types_portal::TuplestoreHandle::NULL, true, None, None);
    dr.startup(1, &d).unwrap();
}

fn int4_desc(mcx: Mcx<'static>, ncols: usize) -> Rc<TupleDescData<'static>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for i in 0..ncols {
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
        natts: ncols as i32,
        tdtypeid: 2249,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

const PORTAL_MISMATCH_MSG: &str = "query result type does not match portal result type";

// upstream 37b8f3b0e05e (18.6): Cross-check the type of a portal running EXECUTE or FETCH.
#[test]
fn startup_rejects_result_type_not_matching_target_tupdesc() {
    let mcx = leaked_mcx();
    let target = int4_desc(mcx, 1);
    let typeinfo = int4_desc(mcx, 2);
    let h = tuplestore::hold::register(tuplestore::Tuplestore::begin_heap(false, true, 64));
    let mut dr = tstore_create_DR();
    set_params(&mut dr, h, false, Some(target), Some(PORTAL_MISMATCH_MSG));
    let err = dr.startup(1, &typeinfo).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_DATATYPE_MISMATCH);
    assert_eq!(err.message(), PORTAL_MISMATCH_MSG);
    assert_eq!(
        err.detail().unwrap(),
        "Number of returned columns (2) does not match expected column count (1)."
    );
    tuplestore::hold::end(h);
}

#[test]
fn startup_accepts_result_type_matching_target_tupdesc() {
    let mcx = leaked_mcx();
    let target = int4_desc(mcx, 1);
    let typeinfo = int4_desc(mcx, 1);
    let h = tuplestore::hold::register(tuplestore::Tuplestore::begin_heap(false, true, 64));
    let mut dr = tstore_create_DR();
    set_params(&mut dr, h, false, Some(target), Some(PORTAL_MISMATCH_MSG));
    dr.startup(1, &typeinfo).unwrap();
    assert!(dr.scratch.is_none(), "the cross-check must not create a context");
    tuplestore::hold::end(h);
}

// int4 columns, with `dropped[i]` marking attisdropped entries (atttypid 0 as
// RemoveAttributeById leaves them): the descriptors that make
// convert_tuples_by_position return a non-identity map.
fn int4_desc_dropped(mcx: Mcx<'static>, dropped: &[bool]) -> Rc<TupleDescData<'static>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for (i, &isdropped) in dropped.iter().enumerate() {
        let att = FormData_pg_attribute {
            attnum: (i + 1) as i16,
            atttypid: if isdropped { 0 } else { 23 },
            attlen: 4,
            attbyval: true,
            attalign: TYPALIGN_INT,
            attstorage: TYPSTORAGE_PLAIN,
            attisdropped: isdropped,
            ..Default::default()
        };
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    Rc::new(TupleDescData {
        natts: dropped.len() as i32,
        tdtypeid: 2249,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

// audit-18.6 b134 (a186-candidate-fp-executor-b5-6d4bdb067c3a3f365735-1):
// tstoreStartupReceiver builds the map and tstoreReceiveSlot_tupmap
// (tstoreReceiver.c:200) remaps every received row into a slot over
// target_tupdesc before storing it — a dropped target column becomes NULL.
#[test]
fn tupmap_arm_remaps_rows_into_the_target_descriptor() {
    let mcx = leaked_mcx();
    // target (portal) rowtype: (a int4, <dropped>, c int4); executor result: (int4, int4)
    let target = int4_desc_dropped(mcx, &[false, true, false]);
    let typeinfo = int4_desc_dropped(mcx, &[false, false]);
    let h = tuplestore::hold::register(tuplestore::Tuplestore::begin_heap(false, true, 64));
    let mut dr = tstore_create_DR();
    set_params(&mut dr, h, false, Some(target.clone()), Some(PORTAL_MISMATCH_MSG));
    dr.startup(1, &typeinfo).unwrap();

    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(typeinfo.clone()));
    for (i, v) in [7, 9].into_iter().enumerate() {
        slot.base_mut().tts_values[i] = Datum::from_i32(v);
        slot.base_mut().tts_isnull[i] = false;
    }
    exectuples::exec_store_virtual_tuple(&mut slot);
    assert!(dr.receive_slot(&mut slot).unwrap());
    exectuples::exec_clear_tuple(&mut slot, mcx);
    slot.base_mut().tts_values[0] = Datum::from_i32(8);
    slot.base_mut().tts_isnull[0] = false;
    slot.base_mut().tts_isnull[1] = true;
    exectuples::exec_store_virtual_tuple(&mut slot);
    assert!(dr.receive_slot(&mut slot).unwrap());
    dr.shutdown();

    let mut out = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(target));
    tuplestore::hold::with_store(h, |ts| {
        assert_eq!(ts.tuple_count(), 2);
        assert!(ts.gettupleslot(true, false, &mut out, mcx).unwrap());
    });
    exectuples::slot_getallattrs(&mut out);
    assert_eq!(out.base().tts_values[0].as_i32(), 7);
    assert!(out.base().tts_isnull[1], "dropped target column maps from attno 0 = NULL");
    assert!(!out.base().tts_isnull[2]);
    assert_eq!(out.base().tts_values[2].as_i32(), 9);
    exectuples::exec_clear_tuple(&mut out, mcx);
    tuplestore::hold::with_store(h, |ts| {
        assert!(ts.gettupleslot(true, false, &mut out, mcx).unwrap());
    });
    exectuples::slot_getallattrs(&mut out);
    assert_eq!(out.base().tts_values[0].as_i32(), 8);
    assert!(out.base().tts_isnull[1]);
    assert!(out.base().tts_isnull[2], "input NULL stays NULL through the map");
    exectuples::exec_clear_tuple(&mut out, mcx);
    tuplestore::hold::end(h);
}

// The mirror image: a dropped column in the executor's rowtype is skipped by
// build_attrmap_by_position (attmap.c:108), so the stored row is the compact
// target shape.
#[test]
fn tupmap_arm_skips_dropped_input_columns() {
    let mcx = leaked_mcx();
    let target = int4_desc_dropped(mcx, &[false, false]);
    let typeinfo = int4_desc_dropped(mcx, &[false, true, false]);
    let h = tuplestore::hold::register(tuplestore::Tuplestore::begin_heap(false, true, 64));
    let mut dr = tstore_create_DR();
    set_params(&mut dr, h, false, Some(target.clone()), Some(PORTAL_MISMATCH_MSG));
    dr.startup(1, &typeinfo).unwrap();

    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(typeinfo.clone()));
    for (i, v) in [1, 2, 3].into_iter().enumerate() {
        slot.base_mut().tts_values[i] = Datum::from_i32(v);
        slot.base_mut().tts_isnull[i] = false;
    }
    exectuples::exec_store_virtual_tuple(&mut slot);
    assert!(dr.receive_slot(&mut slot).unwrap());
    dr.shutdown();

    let mut out = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(target));
    tuplestore::hold::with_store(h, |ts| {
        assert_eq!(ts.tuple_count(), 1);
        assert!(ts.gettupleslot(true, false, &mut out, mcx).unwrap());
    });
    exectuples::slot_getallattrs(&mut out);
    assert_eq!(out.base().tts_values[0].as_i32(), 1);
    assert_eq!(out.base().tts_values[1].as_i32(), 3);
    exectuples::exec_clear_tuple(&mut out, mcx);
    tuplestore::hold::end(h);
}
