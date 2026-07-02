use mcx::{Mcx, MemoryContext};
use parser_small1::{make_parsestate, ParseExprKind};
use types_nodes::nodes_enums::LimitOption;
use types_nodes::rawnodes::ValUnion;
use types_nodes::{Integer, Node, NodeList};

use crate::{
    transformFromClause, transformGroupClause, transformLimitClause, transformSortClause,
    transformWhereClause, transformWindowDefinitions,
};

fn int_const<'mcx>(mcx: Mcx<'mcx>, ival: i32) -> Node<'mcx> {
    Node::mk_a_const(mcx, Some(ValUnion::Integer(Integer { ival })), 7).unwrap()
}

#[test]
fn trivial_arms_are_noops() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    transformFromClause(mcx, &mut pstate, &NodeList::nil()).unwrap();
    assert!(pstate.p_joinlist.is_nil());
    assert!(pstate.p_rtable.is_nil());

    let qual = transformWhereClause(
        mcx,
        &mut pstate,
        None,
        ParseExprKind::EXPR_KIND_WHERE,
        "WHERE",
    )
    .unwrap();
    assert!(qual.is_none());

    let limit = transformLimitClause(
        mcx,
        &mut pstate,
        None,
        ParseExprKind::EXPR_KIND_LIMIT,
        "LIMIT",
        LimitOption::default(),
    )
    .unwrap();
    assert!(limit.is_none());

    let mut tlist = NodeList::nil();
    let sort = transformSortClause(
        &mut pstate,
        &NodeList::nil(),
        &mut tlist,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        false,
    )
    .unwrap();
    assert!(sort.is_nil());

    let mut gsets = NodeList::nil();
    let group = transformGroupClause(
        &mut pstate,
        &NodeList::nil(),
        &mut gsets,
        &mut tlist,
        &sort,
        ParseExprKind::EXPR_KIND_GROUP_BY,
        false,
    )
    .unwrap();
    assert!(group.is_nil() && gsets.is_nil());

    let windows =
        transformWindowDefinitions(&mut pstate, &NodeList::nil(), &mut tlist).unwrap();
    assert!(windows.is_nil());
}

#[test]
#[should_panic(expected = "transformFromClauseItem")]
fn nonempty_from_panics_loudly() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let rv = Node::mk(mcx, types_nodes::primnodes::RangeVar::default()).unwrap();
    let from = NodeList::make1(mcx, rv).unwrap();
    let _ = transformFromClause(mcx, &mut pstate, &from);
}

#[test]
#[should_panic(expected = "coerce_to_boolean")]
fn where_clause_panics_at_coercion() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let _ = transformWhereClause(
        mcx,
        &mut pstate,
        Some(int_const(mcx, 1)),
        ParseExprKind::EXPR_KIND_WHERE,
        "WHERE",
    );
}
