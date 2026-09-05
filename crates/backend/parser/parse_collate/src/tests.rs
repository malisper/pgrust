use mcx::{Mcx, MemoryContext};
use parser_small1::make_parsestate;
use types_core::catalog::{
    C_COLLATION_OID, DEFAULT_COLLATION_OID, INT4OID, POSIX_COLLATION_OID, TEXTOID,
};
use types_core::{InvalidOid, Oid};
use types_error::ERRCODE_INTERNAL_ERROR;
use types_nodes::parsenodes::Query;
use types_nodes::primnodes::FromExpr;
use types_nodes::{Node, NodeList, OptNodeList};

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

#[test]
fn expr_set_collation_covers_c_arms_beyond_the_json_shapes() {
    // exprSetCollation now carries the full nodeFuncs.c switch: writable
    // arms outside the coerceJsonFuncExpr coercion shapes (Var, CaseExpr,
    // SubscriptingRef here) store the collation instead of panicking.
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    let var = collated_var(mcx, InvalidOid);
    // SAFETY: this test exclusively owns the just-built node.
    unsafe { crate::expr_set_collation(var, DEFAULT_COLLATION_OID) };
    assert_eq!(var.as_var().unwrap().varcollid, DEFAULT_COLLATION_OID);

    let case = Node::mk(
        mcx,
        types_nodes::primnodes::CaseExpr {
            casetype: TEXTOID,
            casecollid: InvalidOid,
            arg: None,
            args: NodeList::nil(),
            defresult: None,
            location: -1,
        },
    )
    .unwrap();
    // SAFETY: as above.
    unsafe { crate::expr_set_collation(case, DEFAULT_COLLATION_OID) };
    assert_eq!(case.as_case_expr().unwrap().casecollid, DEFAULT_COLLATION_OID);
}

// Type-collation lookups for the non-leaf witnesses below: text is collatable
// (default collation), everything else is not.
fn install_type_shape_fixture() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            let is_text = typid == TEXTOID;
            Ok(Some(types_tuple::PgTypeShape {
                typlen: if is_text { -1 } else { 4 },
                typbyval: !is_text,
                typalign: b'i' as i8,
                typstorage: b'p' as i8,
                typcollation: if is_text { DEFAULT_COLLATION_OID } else { InvalidOid },
            }))
        });
    });
}

#[test]
fn subscripts_do_not_contribute_to_subscripting_ref_collation() {
    // parse_collate.c:684-687: subscripts are walked as independent
    // expressions (assign_expr_collations) and never contribute to the
    // SubscriptingRef's collation; only the container (and the assignment
    // source) do.  Shape of `('a=>b'::hstore)['a' COLLATE "C"]`: a
    // non-collatable container, an explicitly collated subscript, a text
    // result -> the node gets the implicit default collation, not "C".
    install_type_shape_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pstate = make_parsestate(mcx, None);

    const HSTORE_LIKE: Oid = 99_999;
    let subscript = Node::mk(
        mcx,
        types_nodes::primnodes::CollateExpr {
            arg: collated_var(mcx, DEFAULT_COLLATION_OID),
            collOid: C_COLLATION_OID,
            location: 20,
        },
    )
    .unwrap();
    let container = Node::mk_var(mcx, 1, 2, HSTORE_LIKE, -1, InvalidOid, 0).unwrap();
    let sbsref = Node::mk(
        mcx,
        types_nodes::primnodes::SubscriptingRef {
            refcontainertype: HSTORE_LIKE,
            refelemtype: TEXTOID,
            refrestype: TEXTOID,
            reftypmod: -1,
            refcollid: InvalidOid,
            refupperindexpr: OptNodeList::make1(mcx, Some(subscript)).unwrap(),
            reflowerindexpr: OptNodeList::nil(),
            refexpr: Some(container),
            refassgnexpr: None,
        },
    )
    .unwrap();

    assign_expr_collations(mcx, &pstate, sbsref).unwrap();
    assert_eq!(sbsref.as_subscripting_ref().unwrap().refcollid, DEFAULT_COLLATION_OID);

    // ... || ('x' COLLATE "POSIX"): the implicit default yields to the one
    // explicit collation; C never reports a C-vs-POSIX explicit mismatch here.
    let posix = Node::mk(
        mcx,
        types_nodes::primnodes::CollateExpr {
            arg: collated_var(mcx, DEFAULT_COLLATION_OID),
            collOid: POSIX_COLLATION_OID,
            location: 40,
        },
    )
    .unwrap();
    let operands = NodeList::make2(mcx, sbsref, posix).unwrap();
    assert_eq!(
        select_common_collation(mcx, &pstate, &operands, false).unwrap(),
        POSIX_COLLATION_OID
    );
}

#[test]
fn unrecognized_aggkind_is_an_internal_error() {
    // parse_collate.c:615: the aggkind switch has no fall-through; anything
    // but n/o/h is elog(ERROR, "unrecognized aggkind: %d") (XX000).
    install_type_shape_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pstate = make_parsestate(mcx, None);

    let agg = Node::mk(
        mcx,
        types_nodes::primnodes::Aggref { aggkind: b'x' as i8, ..Default::default() },
    )
    .unwrap();
    let err = assign_expr_collations(mcx, &pstate, agg).unwrap_err();
    assert_eq!(err.message(), "unrecognized aggkind: 120");
    assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR);

    // The three known kinds still walk.
    for kind in [
        types_nodes::primnodes::AGGKIND_NORMAL,
        types_nodes::primnodes::AGGKIND_ORDERED_SET,
        types_nodes::primnodes::AGGKIND_HYPOTHETICAL,
    ] {
        let agg = Node::mk(
            mcx,
            types_nodes::primnodes::Aggref { aggkind: kind, ..Default::default() },
        )
        .unwrap();
        assign_expr_collations(mcx, &pstate, agg).unwrap();
    }
}
