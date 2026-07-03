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
    set_params(&mut dr, h, false);
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
#[should_panic(expected = "detoast")]
fn detoast_arm_is_loud() {
    let mcx = leaked_mcx();
    let d = desc(mcx, -1, false);
    let mut dr = tstore_create_DR();
    set_params(&mut dr, types_portal::TuplestoreHandle::NULL, true);
    let _ = dr.startup(1, &d);
}

#[test]
fn detoast_without_varlena_columns_takes_notoast_arm() {
    let mcx = leaked_mcx();
    let d = desc(mcx, 4, true);
    let mut dr = tstore_create_DR();
    set_params(&mut dr, types_portal::TuplestoreHandle::NULL, true);
    dr.startup(1, &d).unwrap();
}
