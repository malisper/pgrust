extern crate std;

use std::sync::Once;

use mcx::{Mcx, MemoryContext};
use types_nodes::parsenodes::{Query, RTEKind, RangeTblEntry};
use types_nodes::primnodes::{FromExpr, Var};
use types_nodes::{Bitmapset, Node, NodeList};
use types_tuple::htup::FirstLowInvalidHeapAttributeNumber;

use crate::var::*;

fn cx() -> MemoryContext {
    static ONCE: Once = Once::new();
    ONCE.call_once(crate::init_seams);
    MemoryContext::new_bump("vars-test")
}

fn var(mcx: Mcx<'_>, varno: i32, attno: i16, levelsup: u32) -> Node<'_> {
    Node::mk_var(mcx, varno, attno, 23, -1, 0, levelsup).unwrap()
}

#[test]
fn pull_varnos_collects_varnos_and_nullingrels() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let v1 = var(mcx, 1, 1, 0);
    let mut nulling = Bitmapset::empty();
    nulling.add_member(mcx, 5).unwrap();
    let v2 = Node::mk(
        mcx,
        Var { varno: 2, varattno: 1, vartype: 23, varnullingrels: nulling, ..Default::default() },
    )
    .unwrap();
    let expr = Node::mk_list(mcx, NodeList::make2(mcx, v1, v2).unwrap()).unwrap();
    let varnos = pull_varnos(mcx, expr).unwrap();
    assert!(varnos.is_member(1) && varnos.is_member(2) && varnos.is_member(5));
    assert_eq!(varnos.num_members(), 3);
}

#[test]
fn pull_varnos_recurses_into_rte_subquery() {
    let ctx = cx();
    let mcx = ctx.mcx();

    // Inner query's tlist references an outer var (varlevelsup 1, varno 3).
    let inner_te =
        Node::mk_target_entry(mcx, var(mcx, 3, 1, 1), 1, None, false).unwrap();
    let mut inner = Query::default();
    inner.targetList = NodeList::make1(mcx, inner_te).unwrap();
    inner.jointree = Some(Node::mk_mut(mcx, FromExpr::default()).unwrap().seal_ref());
    let inner_ref = Node::mk_mut(mcx, inner).unwrap().seal_ref();

    let mut rte = RangeTblEntry::default();
    rte.rtekind = RTEKind::RTE_SUBQUERY;
    rte.subquery = Some(inner_ref);
    let rte_node = Node::mk(mcx, rte).unwrap();

    let outer_te = Node::mk_target_entry(mcx, var(mcx, 1, 1, 0), 1, None, false).unwrap();
    let mut outer = Query::default();
    outer.targetList = NodeList::make1(mcx, outer_te).unwrap();
    outer.rtable = NodeList::make1(mcx, rte_node).unwrap();
    outer.jointree = Some(Node::mk_mut(mcx, FromExpr::default()).unwrap().seal_ref());
    let outer_node = Node::mk_mut(mcx, outer).unwrap().seal();

    let varnos = pull_varnos(mcx, outer_node).unwrap();
    assert!(varnos.is_member(1), "level-0 var in outer tlist");
    assert!(varnos.is_member(3), "uplevel var inside subquery");
    assert_eq!(varnos.num_members(), 2);
}

#[test]
fn pull_varnos_recurses_into_sublink_subselect() {
    let ctx = cx();
    let mcx = ctx.mcx();

    // Subselect tlist holds a subselect-local var (level 0 inside, varno 7 —
    // must NOT be collected) and an outer reference (level 1, varno 2 — must
    // be collected once sublevels_up matches inside the subquery).
    let local_te = Node::mk_target_entry(mcx, var(mcx, 7, 1, 0), 1, None, false).unwrap();
    let outer_te = Node::mk_target_entry(mcx, var(mcx, 2, 1, 1), 2, None, false).unwrap();
    let mut sub = Query::default();
    sub.targetList = NodeList::make2(mcx, local_te, outer_te).unwrap();
    sub.jointree = Some(Node::mk_mut(mcx, FromExpr::default()).unwrap().seal_ref());
    let subselect = Node::mk_mut(mcx, sub).unwrap().seal();

    let sublink = Node::mk(
        mcx,
        types_nodes::primnodes::SubLink {
            subLinkType: types_nodes::primnodes::SubLinkType::EXPR_SUBLINK,
            subLinkId: 0,
            testexpr: None,
            operName: NodeList::nil(),
            subselect,
            location: -1,
        },
    )
    .unwrap();
    let expr = Node::mk_list(mcx, NodeList::make2(mcx, var(mcx, 1, 1, 0), sublink).unwrap())
        .unwrap();

    let varnos = pull_varnos(mcx, expr).unwrap();
    assert!(varnos.is_member(1), "top-level var");
    assert!(varnos.is_member(2), "outer reference inside sublink subselect");
    assert!(!varnos.is_member(7), "subselect-local var must be excluded");
    assert_eq!(varnos.num_members(), 2);
}

#[test]
fn pull_varattnos_offsets_and_filters_by_varno() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let expr = Node::mk_list(
        mcx,
        NodeList::make3(
            mcx,
            var(mcx, 1, 2, 0),
            var(mcx, 1, -1, 0), // system attribute
            var(mcx, 2, 4, 0),  // other rel: ignored
        )
        .unwrap(),
    )
    .unwrap();
    let mut attnos = Bitmapset::empty();
    pull_varattnos(mcx, expr, 1, &mut attnos).unwrap();
    assert!(attnos.is_member(2 - FirstLowInvalidHeapAttributeNumber));
    assert!(attnos.is_member(-1 - FirstLowInvalidHeapAttributeNumber));
    assert_eq!(attnos.num_members(), 2);
}

#[test]
fn contain_and_locate_vars() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let v0 = var(mcx, 1, 1, 0);
    let v1 = var(mcx, 1, 1, 1);
    assert!(contain_var_clause(v0).unwrap());
    assert!(!contain_var_clause(v1).unwrap());
    assert!(contain_vars_of_level(v1, 1).unwrap());
    assert!(!contain_vars_of_level(v1, 0).unwrap());
    assert!(!contain_vars_returning_old_or_new(v0).unwrap());

    let located = Node::mk(
        mcx,
        Var { varno: 1, varattno: 1, vartype: 23, location: 42, ..Default::default() },
    )
    .unwrap();
    let te = Node::mk_target_entry(mcx, located, 1, None, false).unwrap();
    assert_eq!(locate_var_of_level(te, 0).unwrap(), 42);
    assert_eq!(locate_var_of_level(v0, 0).unwrap(), -1);
}

#[test]
fn pull_var_clause_links_vars() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let expr = Node::mk_list(
        mcx,
        NodeList::make2(mcx, var(mcx, 1, 1, 0), var(mcx, 2, 3, 0)).unwrap(),
    )
    .unwrap();
    let vs = pull_var_clause(mcx, expr, 0).unwrap();
    assert_eq!(vs.len(), 2);
    assert_eq!(vs.nth(0).as_var().unwrap().varno, 1);
    assert_eq!(vs.nth(1).as_var().unwrap().varattno, 3);
}

#[test]
fn pull_var_clause_rejects_upper_level_var() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let err = pull_var_clause(mcx, var(mcx, 1, 1, 2), 0).unwrap_err();
    assert!(err.message().contains("Upper-level Var"));
}

#[test]
fn pull_vars_of_level_links_matching_vars() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let expr = Node::mk_list(
        mcx,
        NodeList::make2(mcx, var(mcx, 1, 1, 0), var(mcx, 2, 1, 1)).unwrap(),
    )
    .unwrap();
    let l0 = pull_vars_of_level(mcx, expr, 0).unwrap();
    assert_eq!(l0.len(), 1);
    assert_eq!(l0.nth(0).as_var().unwrap().varno, 1);
    let l1 = pull_vars_of_level(mcx, expr, 1).unwrap();
    assert_eq!(l1.len(), 1);
    assert_eq!(l1.nth(0).as_var().unwrap().varno, 2);
}

#[test]
fn seam_installed_and_consistent() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let v = var(mcx, 1, 1, 0);
    assert!(var_seams::contain_var_clause::is_installed());
    assert!(var_seams::contain_var_clause::call(v));
}
