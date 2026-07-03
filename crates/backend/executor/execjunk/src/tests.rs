use alloc::rc::Rc;

use ::datum::Datum;
use ::executils::EStateData;
use ::exectuples::exec_store_virtual_tuple;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_nodes::list::NodeList;
use ::types_nodes::Node;
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, TupleDescData, TYPALIGN_INT, TYPSTORAGE_PLAIN,
};

use crate::*;

fn int4_desc<'mcx>(mcx: Mcx<'mcx>, natts: i16) -> Rc<TupleDescData<'mcx>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for attnum in 1..=natts {
        let att = FormData_pg_attribute {
            attnum,
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
        natts: natts as i32,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn junk_tlist<'mcx>(mcx: Mcx<'mcx>) -> NodeList<'mcx> {
    let mk = |resno: i16, name, junk| {
        let c = Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(0), false, true).unwrap();
        Node::mk_target_entry(mcx, c, resno, Some(name), junk).unwrap()
    };
    let mut tl = NodeList::make2(mcx, mk(1, "a", false), mk(2, "b", true)).unwrap();
    tl.lappend(mcx, mk(3, "c", false)).unwrap();
    tl
}

#[test]
fn filter_drops_junk_columns_per_tuple() {
    let cx = MemoryContext::new_bump("execjunk-test");
    let mcx = cx.mcx();
    let mut estate = EStateData::new_in(mcx);

    let input =
        estate.exec_init_extra_tuple_slot(Some(int4_desc(mcx, 3)), types_slot::TupleSlotKind::Virtual);
    let result = estate.exec_init_extra_tuple_slot(None, types_slot::TupleSlotKind::Virtual);

    let tlist = junk_tlist(mcx);
    let clean = int4_desc(mcx, 2);
    let jf = exec_init_junk_filter(&mut estate, &tlist, clean, result).unwrap();
    assert_eq!(jf.jf_cleanMap, &[1, 3]);
    assert_eq!(jf.jf_cleanTupType.natts, 2);
    estate.es_junkFilter = Some(jf);

    for row in 0..3i32 {
        let slot = estate.slot_mut(input);
        exec_clear_tuple(slot, mcx);
        let b = slot.base_mut();
        b.tts_values[0] = Datum::from_i32(10 + row);
        b.tts_isnull[0] = false;
        b.tts_values[1] = Datum::from_i32(999);
        b.tts_isnull[1] = false;
        b.tts_values[2] = Datum::from_i32(20 + row);
        b.tts_isnull[2] = row == 2;
        exec_store_virtual_tuple(slot);

        let out = exec_filter_junk(&mut estate, input);
        assert_eq!(out, result);
        let ob = estate.slot(out).base();
        assert_eq!(ob.tts_nvalid, 2);
        assert_eq!(ob.tts_values[0].as_i32(), 10 + row);
        assert!(!ob.tts_isnull[0]);
        if row == 2 {
            assert!(ob.tts_isnull[1]);
        } else {
            assert_eq!(ob.tts_values[1].as_i32(), 20 + row);
            assert!(!ob.tts_isnull[1]);
        }
    }
    estate.teardown();
}
