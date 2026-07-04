extern crate std;

use mcx::MemoryContext;
use types_nodes::list::NodeList;
use types_nodes::parsenodes::Query;
use types_nodes::primnodes::{Const, TargetEntry};
use types_nodes::Node;

use crate::{CleanQuerytext, EnableQueryId, JumbleQuery};

#[test]
fn clean_querytext_trims_and_offsets() {
    let src = "  SELECT 1;   ";
    let mut loc = -1;
    let mut len = 0;
    let out = CleanQuerytext(src, &mut loc, &mut len);
    assert_eq!(out, "SELECT 1;");
    assert_eq!(loc, 2);
    assert_eq!(len, 9);

    let src = "SELECT 1; SELECT 2";
    let mut loc = 10;
    let mut len = 8;
    let out = CleanQuerytext(src, &mut loc, &mut len);
    assert_eq!(out, "SELECT 2");
    assert_eq!(loc, 10);
}

fn const_query<'mcx>(mcx: mcx::Mcx<'mcx>, consttype: u32, location: i32) -> Query<'mcx> {
    let c = Node::mk(
        mcx,
        Const {
            consttype,
            consttypmod: -1,
            constcollid: 0,
            constlen: 4,
            constvalue: datum::Datum::from_i32(1),
            constisnull: false,
            constbyval: true,
            location,
        },
    )
    .unwrap();
    let tle = Node::mk(
        mcx,
        TargetEntry {
            expr: c,
            resno: 1,
            resname: None,
            ressortgroupref: 0,
            resorigtbl: 0,
            resorigcol: 0,
            resjunk: false,
        },
    )
    .unwrap();
    Query {
        commandType: types_nodes::nodes_enums::CmdType::CMD_SELECT,
        targetList: NodeList::make1(mcx, tle).unwrap(),
        ..Default::default()
    }
}

#[test]
fn queryid_stable_normalized_and_locations_recorded() {
    EnableQueryId();
    let ctx = MemoryContext::new_bump("qjumble-test");
    let mcx = ctx.mcx();

    let mut q1 = const_query(mcx, 23, 7);
    let mut q2 = const_query(mcx, 23, 7);
    let js1 = JumbleQuery(mcx, &mut q1).unwrap();
    let js2 = JumbleQuery(mcx, &mut q2).unwrap();
    assert_ne!(q1.queryId, 0);
    assert_eq!(q1.queryId, q2.queryId);
    assert_eq!(js1.clocations.len(), 1);
    assert_eq!(js1.clocations[0].location, 7);
    assert_eq!(js1.clocations[0].length, -1);
    assert!(!js1.clocations[0].squashed);
    assert!(!js1.clocations[0].extern_param);
    assert_eq!(js2.clocations.len(), 1);

    // Different const TYPE changes the id; different const LOCATION does not
    // (values/locations are normalized away).
    let mut q3 = const_query(mcx, 25, 7);
    JumbleQuery(mcx, &mut q3).unwrap();
    assert_ne!(q3.queryId, q1.queryId);
    let mut q4 = const_query(mcx, 23, 99);
    JumbleQuery(mcx, &mut q4).unwrap();
    assert_eq!(q4.queryId, q1.queryId);
}

#[test]
fn jumble_buffer_overflow_folds() {
    EnableQueryId();
    let ctx = MemoryContext::new_bump("qjumble-test");
    let mcx = ctx.mcx();

    // >1024 bytes of jumbled material exercises the fold path.
    let mut list = NodeList::nil();
    for i in 0..400 {
        let c = Node::mk(
            mcx,
            Const {
                consttype: i,
                consttypmod: -1,
                constcollid: 0,
                constlen: 4,
                constvalue: datum::Datum::from_i32(0),
                constisnull: false,
                constbyval: true,
                location: -1,
            },
        )
        .unwrap();
        list.lappend(mcx, c).unwrap();
    }
    let mut q = Query {
        commandType: types_nodes::nodes_enums::CmdType::CMD_SELECT,
        targetList: list,
        ..Default::default()
    };
    JumbleQuery(mcx, &mut q).unwrap();
    assert_ne!(q.queryId, 0);
}
