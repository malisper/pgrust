use mcx::MemoryContext;
use types_nodes::parsenodes::Query;
use types_nodes::primnodes::{Aggref, SubLink, SubLinkType};
use types_nodes::{Node, NodeList};

use crate::{contain_aggs_of_level, locate_agg_of_level};

// A SubLink over `SELECT <aggref agglevelsup=levelsup>`.
fn sublink_over_agg(mcx: mcx::Mcx<'_>, levelsup: u32, location: i32) -> Node<'_> {
    let mut agg = Node::build::<Aggref>(mcx).unwrap();
    agg.aggfnoid = 2803;
    agg.agglevelsup = levelsup;
    agg.location = location;
    let tle = Node::mk_target_entry(mcx, agg.seal(), 1, None, false).unwrap();
    let mut q = Node::build::<Query>(mcx).unwrap();
    q.targetList = NodeList::make1(mcx, tle).unwrap();
    Node::mk(
        mcx,
        SubLink {
            subLinkType: SubLinkType::EXPR_SUBLINK,
            subLinkId: 0,
            testexpr: None,
            operName: NodeList::nil(),
            subselect: q.seal(),
            location: -1,
        },
    )
    .unwrap()
}

#[test]
fn bare_aggref_matches_its_level() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut agg = Node::build::<Aggref>(mcx).unwrap();
    agg.aggfnoid = 2803;
    agg.location = 11;
    let agg = agg.seal();
    assert!(contain_aggs_of_level(agg, 0).unwrap());
    assert!(!contain_aggs_of_level(agg, 1).unwrap());
    assert_eq!(locate_agg_of_level(agg, 0).unwrap(), 11);
    assert_eq!(locate_agg_of_level(agg, 1).unwrap(), -1);
}

#[test]
fn sublink_recursion_bumps_the_level() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    // Outer-reference agg (agglevelsup=1 inside the subselect) belongs to
    // the level above the SubLink: level 0 from outside.
    let outer_ref = sublink_over_agg(mcx, 1, 23);
    assert!(contain_aggs_of_level(outer_ref, 0).unwrap());
    assert_eq!(locate_agg_of_level(outer_ref, 0).unwrap(), 23);
    // An agg belonging to the subselect itself is NOT of the outer level.
    let local = sublink_over_agg(mcx, 0, 23);
    assert!(!contain_aggs_of_level(local, 0).unwrap());
    assert_eq!(locate_agg_of_level(local, 0).unwrap(), -1);
}
