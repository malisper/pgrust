use mcx::{Mcx, MemoryContext};
use parser_small1::{make_parsestate, ParseExprKind};
use types_core::catalog::{INT4OID, INT8OID};
use types_core::InvalidOid;
use types_error::ERRCODE_GROUPING_ERROR;
use types_nodes::parsenodes::{Query, RangeTblEntry};
use types_nodes::primnodes::{Aggref, Alias};
use types_nodes::{Node, NodeList, String as PgStr};

use crate::{parseCheckAggregates, transformAggregateCall};

fn count_aggref<'mcx>(mcx: Mcx<'mcx>) -> types_nodes::NodeMut<'mcx, Aggref<'mcx>> {
    let mut agg = Node::build::<Aggref>(mcx).unwrap();
    agg.aggfnoid = 2803;
    agg.aggtype = INT8OID;
    agg.aggstar = true;
    agg.location = 7;
    agg
}

#[test]
fn transform_count_star_sets_levels_and_has_aggs() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_SELECT_TARGET;

    let mut agg = count_aggref(mcx);
    transformAggregateCall(mcx, &mut pstate, &mut agg, &NodeList::nil(), &[], &NodeList::nil(), false)
        .unwrap();

    assert_eq!(agg.agglevelsup, 0);
    assert!(agg.args.is_nil());
    assert!(agg.aggargtypes.is_nil());
    assert!(pstate.p_hasAggs);
}

#[test]
fn sum_var_arg_becomes_targetlist() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_SELECT_TARGET;

    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let args = NodeList::make1(mcx, var).unwrap();
    let mut agg = Node::build::<Aggref>(mcx).unwrap();
    agg.aggfnoid = 2108;
    agg.aggtype = INT8OID;
    agg.location = 7;

    transformAggregateCall(mcx, &mut pstate, &mut agg, &args, &[INT4OID], &NodeList::nil(), false)
        .unwrap();

    assert_eq!(agg.args.len(), 1);
    let tle = agg.args.nth(0).as_target_entry().unwrap();
    assert_eq!(tle.resno, 1);
    assert!(tle.expr.as_var().is_some());
    assert_eq!(agg.aggargtypes.nth(0), INT4OID);
}

#[test]
fn aggregate_in_where_is_42803() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_WHERE;

    let mut agg = count_aggref(mcx);
    let err = transformAggregateCall(
        mcx,
        &mut pstate,
        &mut agg,
        &NodeList::nil(),
        &[],
        &NodeList::nil(),
        false,
    )
    .map(|_| ())
    .unwrap_err();

    assert_eq!(err.sqlstate(), ERRCODE_GROUPING_ERROR);
    assert!(
        err.message().contains("aggregate functions are not allowed in WHERE"),
        "{}",
        err.message()
    );
}

#[test]
fn nested_aggregate_is_42803() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_SELECT_TARGET;

    let inner = count_aggref(mcx).seal();
    let args = NodeList::make1(mcx, inner).unwrap();
    let mut outer = Node::build::<Aggref>(mcx).unwrap();
    outer.aggfnoid = 2108;
    outer.aggtype = INT8OID;
    outer.location = 7;

    let err =
        transformAggregateCall(mcx, &mut pstate, &mut outer, &args, &[INT8OID], &NodeList::nil(), false)
            .map(|_| ())
            .unwrap_err();

    assert_eq!(err.sqlstate(), ERRCODE_GROUPING_ERROR);
    assert!(
        err.message().contains("aggregate function calls cannot be nested"),
        "{}",
        err.message()
    );
}

fn query_with_rtable<'mcx>(mcx: Mcx<'mcx>, tlist: NodeList<'mcx>) -> Query<'mcx> {
    let colnames =
        NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: "x" }).unwrap()).unwrap();
    let eref = Node::mk_mut(mcx, Alias { aliasname: Some("t"), colnames }).unwrap().seal_ref();
    let mut rte = Node::build::<RangeTblEntry>(mcx).unwrap();
    rte.eref = Some(eref);
    let mut qry = Query::default();
    qry.rtable = NodeList::make1(mcx, rte.seal()).unwrap();
    qry.targetList = tlist;
    qry
}

#[test]
fn ungrouped_column_is_42803_with_column_name() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_hasAggs = true;

    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, Some("x"), false).unwrap();
    let qry = query_with_rtable(mcx, NodeList::make1(mcx, tle).unwrap());

    let err = parseCheckAggregates(&pstate, &qry).map(|_| ()).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_GROUPING_ERROR);
    assert!(
        err.message().contains(
            "column \"t.x\" must appear in the GROUP BY clause or be used in an aggregate function"
        ),
        "{}",
        err.message()
    );
}

#[test]
fn var_inside_aggregate_passes_check() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_SELECT_TARGET;

    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let args = NodeList::make1(mcx, var).unwrap();
    let mut agg = Node::build::<Aggref>(mcx).unwrap();
    agg.aggfnoid = 2108;
    agg.aggtype = INT8OID;
    agg.location = 7;
    transformAggregateCall(mcx, &mut pstate, &mut agg, &args, &[INT4OID], &NodeList::nil(), false)
        .unwrap();

    let tle = Node::mk_target_entry(mcx, agg.seal(), 1, Some("sum"), false).unwrap();
    let qry = query_with_rtable(mcx, NodeList::make1(mcx, tle).unwrap());

    parseCheckAggregates(&pstate, &qry).unwrap();
}

fn group_clause_ref1<'mcx>(mcx: Mcx<'mcx>) -> NodeList<'mcx> {
    NodeList::make1(
        mcx,
        Node::mk(
            mcx,
            types_nodes::parsenodes::SortGroupClause {
                tleSortGroupRef: 1,
                eqop: 96,
                sortop: 97,
                reverse_sort: false,
                nulls_first: false,
                hashable: true,
            },
        )
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn grouped_column_passes_check() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_hasAggs = true;

    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, Some("x"), false).unwrap();
    // SAFETY: freshly built tlist; no other reference is live.
    unsafe {
        tle.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.ressortgroupref = 1)
    }
    .unwrap();
    let mut qry = query_with_rtable(mcx, NodeList::make1(mcx, tle).unwrap());
    qry.groupClause = group_clause_ref1(mcx);
    parseCheckAggregates(&pstate, &qry).unwrap();
}

#[test]
fn ungrouped_column_next_to_group_by_is_42803() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_hasAggs = true;

    let colnames = NodeList::make2(
        mcx,
        Node::mk(mcx, PgStr { sval: "x" }).unwrap(),
        Node::mk(mcx, PgStr { sval: "y" }).unwrap(),
    )
    .unwrap();
    let eref = Node::mk_mut(mcx, Alias { aliasname: Some("t"), colnames }).unwrap().seal_ref();
    let mut rte = Node::build::<RangeTblEntry>(mcx).unwrap();
    rte.eref = Some(eref);

    let gvar = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let gtle = Node::mk_target_entry(mcx, gvar, 1, Some("x"), false).unwrap();
    // SAFETY: freshly built tlist; no other reference is live.
    unsafe {
        gtle.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.ressortgroupref = 1)
    }
    .unwrap();
    let uvar = Node::mk_var(mcx, 1, 2, INT4OID, -1, InvalidOid, 0).unwrap();
    let utle = Node::mk_target_entry(mcx, uvar, 2, Some("y"), false).unwrap();
    let mut tlist = NodeList::make1(mcx, gtle).unwrap();
    tlist.lappend(mcx, utle).unwrap();

    let mut qry = Query::default();
    qry.rtable = NodeList::make1(mcx, rte.seal()).unwrap();
    qry.targetList = tlist;
    qry.groupClause = group_clause_ref1(mcx);

    let err = parseCheckAggregates(&pstate, &qry).map(|_| ()).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_GROUPING_ERROR);
    assert!(
        err.message().contains(
            "column \"t.y\" must appear in the GROUP BY clause or be used in an aggregate function"
        ),
        "{}",
        err.message()
    );
}
