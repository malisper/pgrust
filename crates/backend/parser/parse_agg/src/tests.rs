use mcx::{Mcx, MemoryContext};
use parser_small1::{make_parsestate, ParseExprKind};
use types_core::catalog::{INT4OID, INT8OID};
use types_core::InvalidOid;
use types_error::ERRCODE_GROUPING_ERROR;
use types_nodes::parsenodes::{GroupingSetKind, Query, RangeTblEntry};
use types_nodes::primnodes::{Aggref, Alias, GroupingFunc};
use types_nodes::{Node, NodeList, String as PgStr};

use crate::{
    expand_grouping_sets, parseCheckAggregates, transformAggregateCall, transformGroupingFunc,
};

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
    assert!(pstate.p_hasAggs.get());
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
    pstate.p_hasAggs.set(true);

    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, Some("x"), false).unwrap();
    let mut qry = query_with_rtable(mcx, NodeList::make1(mcx, tle).unwrap());

    let err = parseCheckAggregates(mcx, &mut pstate, &mut qry).map(|_| ()).unwrap_err();
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
    let mut qry = query_with_rtable(mcx, NodeList::make1(mcx, tle).unwrap());

    parseCheckAggregates(mcx, &mut pstate, &mut qry).unwrap();
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
    pstate.p_hasAggs.set(true);

    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, Some("x"), false).unwrap();
    // SAFETY: freshly built tlist; no other reference is live.
    unsafe {
        tle.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.ressortgroupref = 1)
    }
    .unwrap();
    let mut qry = query_with_rtable(mcx, NodeList::make1(mcx, tle).unwrap());
    qry.groupClause = group_clause_ref1(mcx);
    parseCheckAggregates(mcx, &mut pstate, &mut qry).unwrap();
}

#[test]
fn ungrouped_column_next_to_group_by_is_42803() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_hasAggs.set(true);

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

    let err = parseCheckAggregates(mcx, &mut pstate, &mut qry).map(|_| ()).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_GROUPING_ERROR);
    assert!(
        err.message().contains(
            "column \"t.y\" must appear in the GROUP BY clause or be used in an aggregate function"
        ),
        "{}",
        err.message()
    );
}

fn simple_set<'mcx>(mcx: Mcx<'mcx>, refs: &[i32], loc: i32) -> Node<'mcx> {
    let mut content = NodeList::nil();
    for &r in refs {
        content.lappend(mcx, Node::mk_integer(mcx, r).unwrap()).unwrap();
    }
    Node::mk_grouping_set(mcx, GroupingSetKind::GROUPING_SET_SIMPLE, content, loc).unwrap()
}

fn set_of<'mcx>(mcx: Mcx<'mcx>, kind: GroupingSetKind, children: &[Node<'mcx>]) -> Node<'mcx> {
    let mut content = NodeList::nil();
    for &c in children {
        content.lappend(mcx, c).unwrap();
    }
    Node::mk_grouping_set(mcx, kind, content, -1).unwrap()
}

fn expanded(sets: &mcx::PgVec<'_, mcx::PgVec<'_, i32>>) -> Vec<Vec<i32>> {
    sets.iter().map(|s| s.to_vec()).collect()
}

#[test]
fn expand_rollup_node_shape() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let rollup = set_of(
        mcx,
        GroupingSetKind::GROUPING_SET_ROLLUP,
        &[simple_set(mcx, &[1], 1), simple_set(mcx, &[2], 2)],
    );
    let out = crate::expand_groupingset_node(mcx, rollup.as_grouping_set().unwrap()).unwrap();
    assert_eq!(expanded(&out), [vec![1, 2], vec![1], vec![]]);
}

#[test]
fn expand_cube_sorted_by_length() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let cube = set_of(
        mcx,
        GroupingSetKind::GROUPING_SET_CUBE,
        &[simple_set(mcx, &[1], 1), simple_set(mcx, &[2], 2)],
    );
    let gsets = NodeList::make1(mcx, cube).unwrap();
    let out = expand_grouping_sets(mcx, &gsets, false, 4096).unwrap().unwrap();
    assert_eq!(expanded(&out), [vec![], vec![1], vec![2], vec![1, 2]]);
}

#[test]
fn expand_sets_with_nested_rollup() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let rollup = set_of(
        mcx,
        GroupingSetKind::GROUPING_SET_ROLLUP,
        &[simple_set(mcx, &[2], 2), simple_set(mcx, &[3], 3)],
    );
    let sets = set_of(
        mcx,
        GroupingSetKind::GROUPING_SET_SETS,
        &[simple_set(mcx, &[1], 1), rollup],
    );
    let out =
        crate::expand_groupingset_node(mcx, sets.as_grouping_set().unwrap()).unwrap();
    assert_eq!(expanded(&out), [vec![1], vec![2, 3], vec![2], vec![]]);

    let gsets = NodeList::make1(mcx, sets).unwrap();
    let out = expand_grouping_sets(mcx, &gsets, false, 4096).unwrap().unwrap();
    assert_eq!(expanded(&out), [vec![], vec![1], vec![2], vec![2, 3]]);
}

#[test]
fn expand_group_distinct_dedups() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let sets = set_of(
        mcx,
        GroupingSetKind::GROUPING_SET_SETS,
        &[
            simple_set(mcx, &[2, 1], 1),
            simple_set(mcx, &[1, 2], 2),
            simple_set(mcx, &[1], 3),
        ],
    );
    let gsets = NodeList::make1(mcx, sets).unwrap();
    let out = expand_grouping_sets(mcx, &gsets, true, 4096).unwrap().unwrap();
    assert_eq!(expanded(&out), [vec![1], vec![1, 2]]);
}

#[test]
fn expand_over_limit_returns_none() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let cube4 = || {
        set_of(
            mcx,
            GroupingSetKind::GROUPING_SET_CUBE,
            &[simple_set(mcx, &[1], 1), simple_set(mcx, &[2], 2)],
        )
    };
    let mut gsets = NodeList::nil();
    for _ in 0..7 {
        gsets.lappend(mcx, cube4()).unwrap();
    }
    assert!(expand_grouping_sets(mcx, &gsets, false, 4096).unwrap().is_none());
    let mut six = NodeList::nil();
    for _ in 0..6 {
        six.lappend(mcx, cube4()).unwrap();
    }
    let out = expand_grouping_sets(mcx, &six, false, 4096).unwrap().unwrap();
    assert_eq!(out.len(), 4096);
}

fn var_arg<'mcx>(mcx: Mcx<'mcx>, attno: i16, loc: i32) -> Node<'mcx> {
    let v = Node::mk_var(mcx, 1, attno, INT4OID, -1, InvalidOid, 0).unwrap();
    // SAFETY: freshly built node; no other reference is live.
    unsafe { v.with_mut::<types_nodes::primnodes::Var, _>(|var| var.location = loc) }.unwrap();
    v
}

#[test]
fn grouping_func_32_args_is_54023() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_SELECT_TARGET;

    let mut args = NodeList::nil();
    for _ in 0..32 {
        args.lappend(mcx, var_arg(mcx, 1, 20)).unwrap();
    }
    let raw = GroupingFunc { args, location: 9, ..Default::default() };

    let err = transformGroupingFunc(mcx, &mut pstate, &raw, |_, _, n| Ok(n))
        .map(|_| ())
        .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_TOO_MANY_ARGUMENTS);
    assert_eq!(err.message(), "GROUPING must have fewer than 32 arguments");
}

#[test]
fn grouping_in_where_is_42803_with_grouping_wording() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_WHERE;

    let raw = GroupingFunc {
        args: NodeList::make1(mcx, var_arg(mcx, 1, 20)).unwrap(),
        location: 9,
        ..Default::default()
    };
    let err = transformGroupingFunc(mcx, &mut pstate, &raw, |_, _, n| Ok(n))
        .map(|_| ())
        .unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_GROUPING_ERROR);
    assert_eq!(err.message(), "grouping operations are not allowed in WHERE");
}

#[test]
fn grouping_func_sets_hasaggs_and_levelsup() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_SELECT_TARGET;

    let raw = GroupingFunc {
        args: NodeList::make1(mcx, var_arg(mcx, 1, 20)).unwrap(),
        location: 9,
        ..Default::default()
    };
    let node = transformGroupingFunc(mcx, &mut pstate, &raw, |_, _, n| Ok(n)).unwrap();
    assert!(pstate.p_hasAggs.get());
    let grp = node.as_grouping_func().unwrap();
    assert_eq!((grp.agglevelsup, grp.location, grp.args.len()), (0, 9, 1));
    assert!(grp.refs.is_nil() && grp.cols.is_nil());
}

fn expr_sublink<'mcx>(mcx: Mcx<'mcx>, sub_tlist: NodeList<'mcx>) -> Node<'mcx> {
    let mut subq = Query::default();
    subq.targetList = sub_tlist;
    Node::mk(
        mcx,
        types_nodes::SubLink {
            subLinkType: types_nodes::SubLinkType::EXPR_SUBLINK,
            subLinkId: 0,
            testexpr: None,
            operName: NodeList::nil(),
            subselect: Node::mk(mcx, subq).unwrap(),
            location: -1,
        },
    )
    .unwrap()
}

/// select_parallel.sql: `max((select pa1.b from part_pa_test pa1 where
/// pa1.a = pa2.a))` — the aggregate's sublink subquery has its own jointree
/// (fromlist -> RangeTblRef for pa1), which caa_query/query_tree_walker
/// feeds straight to check_agg_arguments_walker as it recurses into the
/// Query. Regression test for the T_RangeTblRef panic.
#[test]
fn sublink_query_with_jointree_rangetblref_does_not_panic() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_SELECT_TARGET;

    let mut subq = Query::default();
    let local_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, local_var, 1, None, false).unwrap();
    subq.targetList = NodeList::make1(mcx, tle).unwrap();
    let rtr = Node::mk(mcx, types_nodes::primnodes::RangeTblRef { rtindex: 1 }).unwrap();
    subq.jointree = Some(
        Node::mk_mut(
            mcx,
            types_nodes::primnodes::FromExpr { fromlist: NodeList::make1(mcx, rtr).unwrap(), quals: None },
        )
        .unwrap()
        .seal_ref(),
    );
    let sublink = Node::mk(
        mcx,
        types_nodes::SubLink {
            subLinkType: types_nodes::SubLinkType::EXPR_SUBLINK,
            subLinkId: 0,
            testexpr: None,
            operName: NodeList::nil(),
            subselect: Node::mk(mcx, subq).unwrap(),
            location: -1,
        },
    )
    .unwrap();
    let args = NodeList::make1(mcx, sublink).unwrap();
    let mut agg = Node::build::<Aggref>(mcx).unwrap();
    agg.aggfnoid = 2116; // max(int4)
    agg.aggtype = INT4OID;

    transformAggregateCall(mcx, &mut pstate, &mut agg, &args, &[INT4OID], &NodeList::nil(), false)
        .unwrap();
    assert_eq!(agg.agglevelsup, 0);
}

#[test]
fn outer_var_in_sublink_counts_at_agg_level() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_SELECT_TARGET;

    let outer_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 1).unwrap();
    let tle = Node::mk_target_entry(mcx, outer_var, 1, None, false).unwrap();
    let sublink = expr_sublink(mcx, NodeList::make1(mcx, tle).unwrap());
    let args = NodeList::make1(mcx, sublink).unwrap();
    let mut agg = Node::build::<Aggref>(mcx).unwrap();
    agg.aggfnoid = 2108;
    agg.aggtype = INT8OID;

    transformAggregateCall(mcx, &mut pstate, &mut agg, &args, &[INT4OID], &NodeList::nil(), false)
        .unwrap();
    assert_eq!(agg.agglevelsup, 0);
}

#[test]
fn subquery_local_var_in_sublink_is_ignored() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_SELECT_TARGET;

    let local_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, local_var, 1, None, false).unwrap();
    let sublink = expr_sublink(mcx, NodeList::make1(mcx, tle).unwrap());
    let args = NodeList::make1(mcx, sublink).unwrap();
    let mut agg = Node::build::<Aggref>(mcx).unwrap();
    agg.aggfnoid = 2108;
    agg.aggtype = INT8OID;

    transformAggregateCall(mcx, &mut pstate, &mut agg, &args, &[INT4OID], &NodeList::nil(), false)
        .unwrap();
    assert_eq!(agg.agglevelsup, 0);
}

#[test]
fn nested_agg_via_sublink_is_42803() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_SELECT_TARGET;

    let mut inner = count_aggref(mcx);
    inner.agglevelsup = 1;
    let tle = Node::mk_target_entry(mcx, inner.seal(), 1, None, false).unwrap();
    let sublink = expr_sublink(mcx, NodeList::make1(mcx, tle).unwrap());
    let args = NodeList::make1(mcx, sublink).unwrap();
    let mut agg = Node::build::<Aggref>(mcx).unwrap();
    agg.aggfnoid = 2108;
    agg.aggtype = INT8OID;

    let err = transformAggregateCall(
        mcx,
        &mut pstate,
        &mut agg,
        &args,
        &[INT4OID],
        &NodeList::nil(),
        false,
    )
    .map(|_| ())
    .unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_GROUPING_ERROR);
    assert!(
        err.message().contains("aggregate function calls cannot be nested"),
        "{}",
        err.message()
    );
}

#[test]
fn grouped_outer_var_in_sublink_passes_check() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_hasAggs.set(true);

    let gvar = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let gtle = Node::mk_target_entry(mcx, gvar, 1, Some("x"), false).unwrap();
    // SAFETY: freshly built tlist; no other reference is live.
    unsafe {
        gtle.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.ressortgroupref = 1)
    }
    .unwrap();

    let outer_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 1).unwrap();
    let stle = Node::mk_target_entry(mcx, outer_var, 1, None, false).unwrap();
    let sublink = expr_sublink(mcx, NodeList::make1(mcx, stle).unwrap());
    let tle2 = Node::mk_target_entry(mcx, sublink, 2, Some("s"), false).unwrap();

    let mut tlist = NodeList::make1(mcx, gtle).unwrap();
    tlist.lappend(mcx, tle2).unwrap();
    let mut qry = query_with_rtable(mcx, tlist);
    qry.groupClause = group_clause_ref1(mcx);
    parseCheckAggregates(mcx, &mut pstate, &mut qry).unwrap();
}

#[test]
fn ungrouped_outer_var_in_sublink_is_42803() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_hasAggs.set(true);

    let outer_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 1).unwrap();
    let stle = Node::mk_target_entry(mcx, outer_var, 1, None, false).unwrap();
    let sublink = expr_sublink(mcx, NodeList::make1(mcx, stle).unwrap());
    let tle = Node::mk_target_entry(mcx, sublink, 1, Some("s"), false).unwrap();
    let mut qry = query_with_rtable(mcx, NodeList::make1(mcx, tle).unwrap());

    let err = parseCheckAggregates(mcx, &mut pstate, &mut qry).map(|_| ()).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_GROUPING_ERROR);
    assert_eq!(
        err.message(),
        "subquery uses ungrouped column \"t.x\" from outer query"
    );
    assert!(
        err.message().contains("subquery uses ungrouped column \"t.x\" from outer query"),
        "{}",
        err.message()
    );
}

fn sublink_with_from<'mcx>(mcx: Mcx<'mcx>, sub_tlist: NodeList<'mcx>) -> Node<'mcx> {
    let colnames = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: "z" }).unwrap()).unwrap();
    let eref = Node::mk_mut(mcx, Alias { aliasname: Some("s"), colnames }).unwrap().seal_ref();
    let mut rte = Node::build::<RangeTblEntry>(mcx).unwrap();
    rte.eref = Some(eref);
    let rtr = Node::mk(mcx, types_nodes::primnodes::RangeTblRef { rtindex: 1 }).unwrap();
    let jointree = Node::mk_mut(
        mcx,
        types_nodes::primnodes::FromExpr {
            fromlist: NodeList::make1(mcx, rtr).unwrap(),
            quals: None,
        },
    )
    .unwrap()
    .seal_ref();
    let mut subq = Query::default();
    subq.rtable = NodeList::make1(mcx, rte.seal()).unwrap();
    subq.jointree = Some(jointree);
    subq.targetList = sub_tlist;
    Node::mk(
        mcx,
        types_nodes::SubLink {
            subLinkType: types_nodes::SubLinkType::EXPR_SUBLINK,
            subLinkId: 0,
            testexpr: None,
            operName: NodeList::nil(),
            subselect: Node::mk(mcx, subq).unwrap(),
            location: -1,
        },
    )
    .unwrap()
}

#[test]
fn sublink_with_from_clause_in_agg_arg_walks_jointree() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_SELECT_TARGET;

    let local_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, local_var, 1, None, false).unwrap();
    let sublink = sublink_with_from(mcx, NodeList::make1(mcx, tle).unwrap());
    let args = NodeList::make1(mcx, sublink).unwrap();
    let mut agg = Node::build::<Aggref>(mcx).unwrap();
    agg.aggfnoid = 2108;
    agg.aggtype = INT8OID;

    transformAggregateCall(mcx, &mut pstate, &mut agg, &args, &[INT4OID], &NodeList::nil(), false)
        .unwrap();
    assert_eq!(agg.agglevelsup, 0);
}

#[test]
fn outer_var_arg_hops_to_parent_level() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut parent = make_parsestate(mcx, None);
    parent.p_expr_kind = ParseExprKind::EXPR_KIND_SELECT_TARGET;
    let mut pstate = make_parsestate(mcx, Some(&parent));
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_SELECT_TARGET;

    let outer_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 1).unwrap();
    let args = NodeList::make1(mcx, outer_var).unwrap();
    let mut agg = Node::build::<Aggref>(mcx).unwrap();
    agg.aggfnoid = 2108;
    agg.aggtype = INT8OID;

    transformAggregateCall(mcx, &mut pstate, &mut agg, &args, &[INT4OID], &NodeList::nil(), false)
        .unwrap();
    assert_eq!(agg.agglevelsup, 1);
    assert!(parent.p_hasAggs.get());
    assert!(!pstate.p_hasAggs.get());
}

#[test]
fn sublink_with_from_clause_passes_ungrouped_check() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_hasAggs.set(true);

    let gvar = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let gtle = Node::mk_target_entry(mcx, gvar, 1, Some("x"), false).unwrap();
    // SAFETY: freshly built tlist; no other reference is live.
    unsafe {
        gtle.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.ressortgroupref = 1)
    }
    .unwrap();

    let local_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let stle = Node::mk_target_entry(mcx, local_var, 1, None, false).unwrap();
    let sublink = sublink_with_from(mcx, NodeList::make1(mcx, stle).unwrap());
    let tle2 = Node::mk_target_entry(mcx, sublink, 2, Some("s"), false).unwrap();

    let mut tlist = NodeList::make1(mcx, gtle).unwrap();
    tlist.lappend(mcx, tle2).unwrap();
    let mut qry = query_with_rtable(mcx, tlist);
    qry.groupClause = group_clause_ref1(mcx);
    parseCheckAggregates(mcx, &mut pstate, &mut qry).unwrap();
}

#[test]
fn subscripting_ref_over_grouped_var_passes_check() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_hasAggs.set(true);

    let gvar = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let gtle = Node::mk_target_entry(mcx, gvar, 1, Some("x"), false).unwrap();
    // SAFETY: freshly built tlist; no other reference is live.
    unsafe {
        gtle.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.ressortgroupref = 1)
    }
    .unwrap();

    let refexpr = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let sref = Node::mk(
        mcx,
        types_nodes::primnodes::SubscriptingRef {
            refexpr: Some(refexpr),
            ..Default::default()
        },
    )
    .unwrap();
    let tle2 = Node::mk_target_entry(mcx, sref, 2, Some("a"), false).unwrap();

    let mut tlist = NodeList::make1(mcx, gtle).unwrap();
    tlist.lappend(mcx, tle2).unwrap();
    let mut qry = query_with_rtable(mcx, tlist);
    qry.groupClause = group_clause_ref1(mcx);
    parseCheckAggregates(mcx, &mut pstate, &mut qry).unwrap();
}

#[test]
fn grouping_func_in_sublink_resolves_refs() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_hasAggs.set(true);

    let gvar = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let gtle = Node::mk_target_entry(mcx, gvar, 1, Some("x"), false).unwrap();
    // SAFETY: freshly built tlist; no other reference is live.
    unsafe {
        gtle.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.ressortgroupref = 1)
    }
    .unwrap();

    let outer_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 1).unwrap();
    let gf = Node::mk(
        mcx,
        GroupingFunc {
            args: NodeList::make1(mcx, outer_var).unwrap(),
            agglevelsup: 1,
            location: 5,
            ..Default::default()
        },
    )
    .unwrap();
    let stle = Node::mk_target_entry(mcx, gf, 1, None, false).unwrap();
    let sublink = sublink_with_from(mcx, NodeList::make1(mcx, stle).unwrap());
    let tle2 = Node::mk_target_entry(mcx, sublink, 2, Some("g"), false).unwrap();

    let mut tlist = NodeList::make1(mcx, gtle).unwrap();
    tlist.lappend(mcx, tle2).unwrap();
    let mut qry = query_with_rtable(mcx, tlist);
    qry.groupClause = group_clause_ref1(mcx);
    parseCheckAggregates(mcx, &mut pstate, &mut qry).unwrap();

    let grp = gf.as_grouping_func().unwrap();
    assert_eq!(grp.refs.len(), 1);
    assert_eq!(grp.refs.nth(0), 1);
}

#[test]
fn outer_agg_constraint_checked_against_parent_clause() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut parent = make_parsestate(mcx, None);
    parent.p_expr_kind = ParseExprKind::EXPR_KIND_WHERE;
    let mut pstate = make_parsestate(mcx, Some(&parent));
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_SELECT_TARGET;

    let outer_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 1).unwrap();
    let args = NodeList::make1(mcx, outer_var).unwrap();
    let mut agg = Node::build::<Aggref>(mcx).unwrap();
    agg.aggfnoid = 2108;
    agg.aggtype = INT8OID;

    let err =
        transformAggregateCall(mcx, &mut pstate, &mut agg, &args, &[INT4OID], &NodeList::nil(), false)
            .unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_GROUPING_ERROR);
    assert!(
        err.message().contains("aggregate functions are not allowed in WHERE"),
        "{}",
        err.message()
    );
}
