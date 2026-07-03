use std::rc::Rc;

use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_nodes::plannodes::WindowAgg;
use ::types_nodes::rawnodes::{
    FRAMEOPTION_END_CURRENT_ROW, FRAMEOPTION_RANGE, FRAMEOPTION_ROWS,
    FRAMEOPTION_START_UNBOUNDED_PRECEDING,
};
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, TupleDescData, TYPALIGN_INT, TYPSTORAGE_PLAIN,
};

use crate::*;

fn leaked_mcx() -> Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("windowagg-test")));
    m.mcx()
}

fn int4_desc(mcx: Mcx<'static>) -> Rc<TupleDescData<'static>> {
    let att = FormData_pg_attribute {
        attnum: 1,
        atttypid: 23,
        attlen: 4,
        attbyval: true,
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

// parsenodes.h: RANGE | START_UNBOUNDED_PRECEDING | END_CURRENT_ROW == 0x422.
#[test]
fn frameoption_defaults_value_matches_c() {
    assert_eq!(
        FRAMEOPTION_DEFAULTS,
        FRAMEOPTION_RANGE | FRAMEOPTION_START_UNBOUNDED_PRECEDING | FRAMEOPTION_END_CURRENT_ROW
    );
    assert_eq!(FRAMEOPTION_DEFAULTS, 0x422);
}

#[test]
#[should_panic(expected = "frameOptions")]
fn explicit_frame_panics_at_init() {
    let mcx = leaked_mcx();
    let node = Node::mk(
        mcx,
        WindowAgg {
            frameOptions: (FRAMEOPTION_DEFAULTS & !FRAMEOPTION_RANGE) | FRAMEOPTION_ROWS,
            ..Default::default()
        },
    )
    .unwrap();
    let mut estate = ::executils::EStateData::new_in(mcx);
    let desc = int4_desc(mcx);
    let _ =
        exec_init_window_agg(node.as_window_agg().unwrap(), &mut estate, 0, &desc, desc.clone());
}

#[test]
#[should_panic(expected = "runCondition")]
fn run_condition_panics_at_init() {
    let mcx = leaked_mcx();
    let mut wa = Node::build::<WindowAgg>(mcx).unwrap();
    wa.runCondition = NodeList::make1(mcx, Node::mk_string(mcx, "x").unwrap()).unwrap();
    let node = wa.seal();
    let mut estate = ::executils::EStateData::new_in(mcx);
    let desc = int4_desc(mcx);
    let _ =
        exec_init_window_agg(node.as_window_agg().unwrap(), &mut estate, 0, &desc, desc.clone());
}
