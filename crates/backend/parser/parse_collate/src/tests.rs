use mcx::{Mcx, MemoryContext};
use parser_small1::make_parsestate;
use types_core::catalog::{DEFAULT_COLLATION_OID, INT4OID, TEXTOID};
use types_core::{InvalidOid, Oid};
use types_nodes::parsenodes::Query;
use types_nodes::primnodes::FromExpr;
use types_nodes::{Node, NodeList};

use crate::{assign_expr_collations, assign_query_collations, select_common_collation};

fn int_const<'mcx>(mcx: Mcx<'mcx>) -> Node<'mcx> {
    Node::mk_const(mcx, INT4OID, -1, InvalidOid, 4, datum::Datum::from_i32(1), false, true)
        .unwrap()
}

fn collated_var<'mcx>(mcx: Mcx<'mcx>, collid: Oid) -> Node<'mcx> {
    Node::mk_var(mcx, 1, 1, TEXTOID, -1, collid, 0).unwrap()
}

#[test]
fn leaf_nodes_walk_without_assignment() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pstate = make_parsestate(mcx, None);
    assign_expr_collations(mcx, &pstate, int_const(mcx)).unwrap();
    assign_expr_collations(mcx, &pstate, collated_var(mcx, DEFAULT_COLLATION_OID)).unwrap();
}

#[test]
fn query_walk_covers_tlist_and_jointree() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pstate = make_parsestate(mcx, None);

    let te = Node::mk_target_entry(mcx, int_const(mcx), 1, None, false).unwrap();
    let mut query = Query::default();
    query.targetList = NodeList::make1(mcx, te).unwrap();
    query.jointree = Some(
        Node::mk_mut(mcx, FromExpr { fromlist: NodeList::nil(), quals: None })
            .unwrap()
            .seal_ref(),
    );

    assign_query_collations(mcx, &pstate, &query).unwrap();
}

#[test]
fn common_collation_merge_rules() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pstate = make_parsestate(mcx, None);

    let noncollatable =
        NodeList::make2(mcx, int_const(mcx), int_const(mcx)).unwrap();
    assert_eq!(select_common_collation(mcx, &pstate, &noncollatable, true).unwrap(), InvalidOid);

    // Non-default implicit collation beats default.
    let default_vs_specific = NodeList::make2(
        mcx,
        collated_var(mcx, DEFAULT_COLLATION_OID),
        collated_var(mcx, 150),
    )
    .unwrap();
    assert_eq!(select_common_collation(mcx, &pstate, &default_vs_specific, false).unwrap(), 150);

    let agreeing =
        NodeList::make2(mcx, collated_var(mcx, 150), collated_var(mcx, 150)).unwrap();
    assert_eq!(select_common_collation(mcx, &pstate, &agreeing, false).unwrap(), 150);

    // Conflicting implicit collations: none_ok swallows the conflict.
    let conflicting =
        NodeList::make2(mcx, collated_var(mcx, 150), collated_var(mcx, 151)).unwrap();
    assert_eq!(select_common_collation(mcx, &pstate, &conflicting, true).unwrap(), InvalidOid);
}
