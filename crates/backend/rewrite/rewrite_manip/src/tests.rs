use mcx::MemoryContext;
use types_nodes::parsenodes::{Query, RangeTblEntry, RangeTblFunction, RTEKind, WindowClause};
use types_nodes::primnodes::{Aggref, SubLink, SubLinkType, Var};
use types_nodes::{Node, NodeList, NodeTag};

use crate::{
    contain_aggs_of_level, locate_agg_of_level, ReplaceVarsFromTargetList,
    ReplaceVarsNoMatchOption,
};

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

// A Var referencing target varno 1, attno 1 (int4).
fn target_var(mcx: mcx::Mcx<'_>) -> Node<'_> {
    Node::mk(
        mcx,
        Var { varno: 1, varattno: 1, vartype: 23, ..Default::default() },
    )
    .unwrap()
}

// targetlist [{resno 1, expr Const(int4 42)}] replacing varno-1 references.
fn replacement_tlist(mcx: mcx::Mcx<'_>) -> NodeList<'_> {
    let c = Node::mk_const(mcx, 23, -1, 0, 4, datum::Datum::from_i32(42), false, true).unwrap();
    let tle = Node::mk_target_entry(mcx, c, 1, None, false).unwrap();
    NodeList::make1(mcx, tle).unwrap()
}

// ReplaceVarsFromTargetList rewrites window frame offsets in place
// (query_tree_mutator's WindowClause lane, nodeFuncs.c).
#[test]
fn replace_vars_rewrites_window_frame_offsets() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut wc = Node::build::<WindowClause>(mcx).unwrap();
    wc.startOffset = Some(target_var(mcx));
    wc.endOffset = Some(target_var(mcx));
    let wc = wc.seal();
    let mut q = Node::build::<Query>(mcx).unwrap();
    q.windowClause = NodeList::make1(mcx, wc).unwrap();
    let qnode = q.seal();

    let target_rte = RangeTblEntry { rtekind: RTEKind::RTE_RELATION, ..Default::default() };
    let tlist = replacement_tlist(mcx);
    ReplaceVarsFromTargetList(
        mcx,
        qnode,
        1,
        0,
        &target_rte,
        &tlist,
        0,
        ReplaceVarsNoMatchOption::ReportError,
        None,
    )
    .unwrap();
    let wc = qnode
        .as_query()
        .unwrap()
        .windowClause
        .nth(0)
        .as_window_clause()
        .unwrap();
    assert_eq!(wc.startOffset.unwrap().node_tag(), NodeTag::T_Const);
    assert_eq!(wc.endOffset.unwrap().node_tag(), NodeTag::T_Const);
}

// ReplaceVarsFromTargetList rewrites RTE_FUNCTION functions and RTE_GROUP
// groupexprs (range_table_mutator arms, nodeFuncs.c).
#[test]
fn replace_vars_rewrites_function_and_group_rtes() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut rtf = Node::build::<RangeTblFunction>(mcx).unwrap();
    rtf.funcexpr = Some(target_var(mcx));
    let rtf = rtf.seal();
    let mut func_rte = Node::build::<RangeTblEntry>(mcx).unwrap();
    func_rte.rtekind = RTEKind::RTE_FUNCTION;
    func_rte.functions = NodeList::make1(mcx, rtf).unwrap();
    let mut group_rte = Node::build::<RangeTblEntry>(mcx).unwrap();
    group_rte.rtekind = RTEKind::RTE_GROUP;
    group_rte.groupexprs = NodeList::make1(mcx, target_var(mcx)).unwrap();
    let mut rtable = NodeList::make1(mcx, func_rte.seal()).unwrap();
    rtable.lappend(mcx, group_rte.seal()).unwrap();
    let mut q = Node::build::<Query>(mcx).unwrap();
    q.rtable = rtable;
    let qnode = q.seal();

    let target_rte = RangeTblEntry { rtekind: RTEKind::RTE_RELATION, ..Default::default() };
    let tlist = replacement_tlist(mcx);
    ReplaceVarsFromTargetList(
        mcx,
        qnode,
        1,
        0,
        &target_rte,
        &tlist,
        0,
        ReplaceVarsNoMatchOption::ReportError,
        None,
    )
    .unwrap();
    let q = qnode.as_query().unwrap();
    let func_rte = q.rtable.nth(0).as_range_tbl_entry().unwrap();
    let rtf = func_rte.functions.nth(0).as_variant::<RangeTblFunction>().unwrap();
    assert_eq!(rtf.funcexpr.unwrap().node_tag(), NodeTag::T_Const);
    let group_rte = q.rtable.nth(1).as_range_tbl_entry().unwrap();
    assert_eq!(group_rte.groupexprs.nth(0).node_tag(), NodeTag::T_Const);
}

// map_variable_attnos recurses into SubLink subselects with sublevels_up
// tracking (C map_variable_attnos_mutator's Query arm over
// query_tree_mutator).
#[test]
fn map_variable_attnos_recurses_into_sublinks() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    // Subselect targetlist holds an outer reference: Var(varno 1, attno 1,
    // varlevelsup 1). Mapping varno-1 attno 1 -> 3 at sublevels_up 0 must
    // rewrite it through the SubLink.
    let outer_var = Node::mk(
        mcx,
        Var { varno: 1, varattno: 1, vartype: 23, varlevelsup: 1, ..Default::default() },
    )
    .unwrap();
    let tle = Node::mk_target_entry(mcx, outer_var, 1, None, false).unwrap();
    let mut q = Node::build::<Query>(mcx).unwrap();
    q.targetList = NodeList::make1(mcx, tle).unwrap();
    let sl = Node::mk(
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
    .unwrap();

    let attnums: [i16; 1] = [3];
    let (mapped, found_whole_row) =
        crate::map_variable_attnos(mcx, sl, 1, 0, &attnums, 0).unwrap();
    assert!(!found_whole_row);
    let sub = mapped.as_sub_link().unwrap().subselect.as_query().unwrap();
    let v = sub
        .targetList
        .nth(0)
        .as_target_entry()
        .unwrap()
        .expr
        .as_var()
        .unwrap();
    assert_eq!((v.varno, v.varattno, v.varlevelsup), (1, 3, 1));

    // A Var of the subselect's own level (varlevelsup 0 inside) is NOT
    // remapped by an outer-level mapping.
    let local_var = Node::mk(
        mcx,
        Var { varno: 1, varattno: 1, vartype: 23, varlevelsup: 0, ..Default::default() },
    )
    .unwrap();
    let tle = Node::mk_target_entry(mcx, local_var, 1, None, false).unwrap();
    let mut q = Node::build::<Query>(mcx).unwrap();
    q.targetList = NodeList::make1(mcx, tle).unwrap();
    let sl = Node::mk(
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
    .unwrap();
    let (mapped, _) = crate::map_variable_attnos(mcx, sl, 1, 0, &attnums, 0).unwrap();
    let sub = mapped.as_sub_link().unwrap().subselect.as_query().unwrap();
    let v = sub
        .targetList
        .nth(0)
        .as_target_entry()
        .unwrap()
        .expr
        .as_var()
        .unwrap();
    assert_eq!((v.varno, v.varattno, v.varlevelsup), (1, 1, 0));
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
