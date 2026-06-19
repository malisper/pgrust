//! Unit tests for the rewriteManip.c port: the `ExprRelids` set algebra and a
//! couple of the in-place Var mutators over a bare expression tree.

use crate::relids;
use mcx::{MemoryContext, PgBox, PgVec};
use types_nodes::copy_query::Query;
use types_nodes::nodes::{CmdType, Node, NodePtr};
use types_nodes::parsenodes::{RTEPermissionInfo, RangeTblEntry};
use types_nodes::primnodes::{BoolTestType, Expr, ExprRelids, Var};
use types_nodes::rawnodes::{FromExpr, RangeTblRef};

fn rel(members: &[i32]) -> ExprRelids {
    let mut r = ExprRelids::default();
    for &m in members {
        r = relids::add_member(r, m);
    }
    r
}

fn members(r: &ExprRelids) -> alloc::vec::Vec<i32> {
    let mut out = alloc::vec::Vec::new();
    let mut bit = -1;
    while let Some(m) = relids::next_member(r, bit) {
        bit = m;
        out.push(m);
    }
    out
}

#[test]
fn relids_algebra() {
    let a = rel(&[1, 3, 70]);
    assert!(relids::is_member(3, &a));
    assert!(!relids::is_member(2, &a));
    assert_eq!(members(&a), alloc::vec![1, 3, 70]);

    let b = rel(&[3, 5]);
    assert_eq!(members(&relids::union(&a, &b)), alloc::vec![1, 3, 5, 70]);
    assert_eq!(members(&relids::difference(&a, &b)), alloc::vec![1, 70]);
    assert!(relids::overlap(&a, &b));
    assert!(!relids::overlap(&rel(&[2]), &rel(&[4])));

    let d = relids::del_member(rel(&[1, 3]), 3);
    assert_eq!(members(&d), alloc::vec![1]);
    assert!(relids::is_empty(&ExprRelids::default()));
}

fn var(varno: i32, levelsup: u32) -> Node<'static> {
    Node::Expr(Expr::Var(Var {
        varno,
        varlevelsup: levelsup,
        location: -1,
        ..Var::default()
    }))
}

#[test]
fn offset_var_nodes_bumps_varno() {
    let mut n = var(3, 0);
    crate::offset::OffsetVarNodes(&mut n, 10, 0);
    if let Some(v) = n.as_var() {
        assert_eq!(v.varno, 13);
    } else {
        panic!("not a var");
    }
}

#[test]
fn change_var_nodes_remaps_varno() {
    let mut n = var(2, 0);
    crate::change::ChangeVarNodes(&mut n, 2, 9, 0);
    if let Some(v) = n.as_var() {
        assert_eq!(v.varno, 9);
    } else {
        panic!("not a var");
    }
    // sublevels_up mismatch: no change.
    let mut n2 = var(2, 1);
    crate::change::ChangeVarNodes(&mut n2, 2, 9, 0);
    if let Some(v) = n2.as_var() {
        assert_eq!(v.varno, 2);
    } else {
        panic!("not a var");
    }
}

#[test]
fn increment_var_sublevels_up_bumps() {
    let mut n = var(1, 0);
    crate::increment::IncrementVarSublevelsUp(&mut n, 2, 0).unwrap();
    if let Some(v) = n.as_var() {
        assert_eq!(v.varlevelsup, 2);
    } else {
        panic!("not a var");
    }
}

#[test]
fn add_nulling_relids_adds_to_var() {
    let mut n = var(4, 0);
    let added = rel(&[7]);
    crate::nulling::add_nulling_relids(&mut n, None, &added);
    if let Some(v) = n.as_var() {
        assert!(relids::is_member(7, &v.varnullingrels));
    } else {
        panic!("not a var");
    }
}

// ---------------------------------------------------------------------------
// Rule-action manipulation primitives (manip_rule.rs)
// ---------------------------------------------------------------------------

/// A bare boolean-ish qual Var node (lifetime-free `Expr`), wrapped as a Node.
fn qual_var(varno: i32) -> Node<'static> {
    Node::Expr(Expr::Var(Var {
        varno,
        varlevelsup: 0,
        location: -1,
        ..Var::default()
    }))
}

#[test]
fn add_qual_into_empty_where() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut q = Query::new(mcx);
    q.commandType = CmdType::CMD_SELECT;
    q.jointree = Some(
        PgBox::try_new_in(
            FromExpr {
                fromlist: PgVec::new_in(mcx),
                quals: None,
            },
            mcx,
        )
        .unwrap(),
    );

    let qual = qual_var(5);
    crate::manip_rule::AddQual(&mut q, Some(&qual), mcx).unwrap();

    // The single qual becomes the WHERE clause directly (make_and_qual: q2 alone).
    let quals = q.jointree.as_deref().unwrap().quals.as_deref().unwrap();
    if let Some(v) = quals.as_var() {
        assert_eq!(v.varno, 5)
    } else {
        panic!("unexpected quals: {quals:?}")
    }
}

#[test]
fn add_qual_ands_with_existing() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut q = Query::new(mcx);
    q.commandType = CmdType::CMD_SELECT;
    let existing = PgBox::try_new_in(qual_var(1), mcx).unwrap();
    q.jointree = Some(
        PgBox::try_new_in(
            FromExpr {
                fromlist: PgVec::new_in(mcx),
                quals: Some(existing),
            },
            mcx,
        )
        .unwrap(),
    );

    let qual = qual_var(2);
    crate::manip_rule::AddQual(&mut q, Some(&qual), mcx).unwrap();

    // Now an AND of the two quals.
    let quals = q.jointree.as_deref().unwrap().quals.as_deref().unwrap();
    match quals.as_expr() {
        Some(Expr::BoolExpr(b)) => assert_eq!(b.args.len(), 2),
        _ => panic!("expected AND BoolExpr, got {quals:?}"),
    }
}

#[test]
fn add_qual_none_is_noop() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut q = Query::new(mcx);
    q.commandType = CmdType::CMD_SELECT;
    q.jointree = Some(
        PgBox::try_new_in(
            FromExpr {
                fromlist: PgVec::new_in(mcx),
                quals: None,
            },
            mcx,
        )
        .unwrap(),
    );
    crate::manip_rule::AddQual(&mut q, None, mcx).unwrap();
    assert!(q.jointree.as_deref().unwrap().quals.is_none());
}

#[test]
fn add_qual_on_setop_errors() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut q = Query::new(mcx);
    q.commandType = CmdType::CMD_SELECT;
    q.setOperations = Some(PgBox::try_new_in(qual_var(9), mcx).unwrap());
    let qual = qual_var(1);
    assert!(crate::manip_rule::AddQual(&mut q, Some(&qual), mcx).is_err());
}

#[test]
fn add_inverted_qual_wraps_is_not_true() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut q = Query::new(mcx);
    q.commandType = CmdType::CMD_SELECT;
    q.jointree = Some(
        PgBox::try_new_in(
            FromExpr {
                fromlist: PgVec::new_in(mcx),
                quals: None,
            },
            mcx,
        )
        .unwrap(),
    );

    let qual = qual_var(3);
    crate::manip_rule::AddInvertedQual(&mut q, Some(&qual), mcx).unwrap();

    let quals = q.jointree.as_deref().unwrap().quals.as_deref().unwrap();
    match quals.as_expr() {
        Some(Expr::BooleanTest(bt)) => {
            assert_eq!(bt.booltesttype, BoolTestType::IS_NOT_TRUE);
            match bt.arg.as_deref() {
                Some(Expr::Var(v)) => assert_eq!(v.varno, 3),
                other => panic!("unexpected BooleanTest arg: {other:?}"),
            }
        }
        _ => panic!("expected BooleanTest, got {quals:?}"),
    }
}

#[test]
fn adjust_join_tree_list_removes_matching_rtr() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut q = Query::new(mcx);
    let mut fromlist: PgVec<NodePtr> = PgVec::new_in(mcx);
    for idx in [1i32, 2, 3] {
        fromlist.push(PgBox::try_new_in(Node::mk_range_tbl_ref(mcx, RangeTblRef { rtindex: idx }), mcx).unwrap());
    }
    q.jointree = Some(
        PgBox::try_new_in(
            FromExpr {
                fromlist,
                quals: None,
            },
            mcx,
        )
        .unwrap(),
    );

    // removert=true drops the item with rtindex == 2.
    let out = crate::manip_rule::adjustJoinTreeList(&q, true, 2, mcx).unwrap();
    let remaining: alloc::vec::Vec<i32> = out
        .iter()
        .map(|n| match n.as_rangetblref() {
            Some(r) => r.rtindex,
            None => panic!("not an rtr"),
        })
        .collect();
    assert_eq!(remaining, alloc::vec![1, 3]);

    // removert=false copies all three unchanged.
    let out2 = crate::manip_rule::adjustJoinTreeList(&q, false, 2, mcx).unwrap();
    assert_eq!(out2.len(), 3);
}

#[test]
fn combine_range_tables_bumps_perminfoindex() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    // dst already has 2 perminfos.
    let mut dst_rtable: PgVec<RangeTblEntry> = PgVec::new_in(mcx);
    let mut dst_perminfos: PgVec<RTEPermissionInfo> = PgVec::new_in(mcx);
    dst_perminfos.push(RTEPermissionInfo::default());
    dst_perminfos.push(RTEPermissionInfo::default());

    // src has one RTE with perminfoindex=1 and one perminfo.
    let mut src_rtable: PgVec<RangeTblEntry> = PgVec::new_in(mcx);
    let mut src_rte = RangeTblEntry::new_in(mcx);
    src_rte.perminfoindex = 1;
    src_rtable.push(src_rte);
    let mut src_perminfos: PgVec<RTEPermissionInfo> = PgVec::new_in(mcx);
    src_perminfos.push(RTEPermissionInfo::default());

    crate::manip_rule::CombineRangeTables(
        &mut dst_rtable,
        &mut dst_perminfos,
        src_rtable,
        src_perminfos,
    );

    assert_eq!(dst_perminfos.len(), 3);
    assert_eq!(dst_rtable.len(), 1);
    // perminfoindex bumped by the prior offset (2).
    assert_eq!(dst_rtable[0].perminfoindex, 3);
}
