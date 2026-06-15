//! Unit tests for the rewriteManip.c port: the `ExprRelids` set algebra and a
//! couple of the in-place Var mutators over a bare expression tree.

use crate::relids;
use types_nodes::nodes::Node;
use types_nodes::primnodes::{Expr, ExprRelids, Var};

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
    if let Node::Expr(Expr::Var(v)) = &n {
        assert_eq!(v.varno, 13);
    } else {
        panic!("not a var");
    }
}

#[test]
fn change_var_nodes_remaps_varno() {
    let mut n = var(2, 0);
    crate::change::ChangeVarNodes(&mut n, 2, 9, 0);
    if let Node::Expr(Expr::Var(v)) = &n {
        assert_eq!(v.varno, 9);
    } else {
        panic!("not a var");
    }
    // sublevels_up mismatch: no change.
    let mut n2 = var(2, 1);
    crate::change::ChangeVarNodes(&mut n2, 2, 9, 0);
    if let Node::Expr(Expr::Var(v)) = &n2 {
        assert_eq!(v.varno, 2);
    } else {
        panic!("not a var");
    }
}

#[test]
fn increment_var_sublevels_up_bumps() {
    let mut n = var(1, 0);
    crate::increment::IncrementVarSublevelsUp(&mut n, 2, 0).unwrap();
    if let Node::Expr(Expr::Var(v)) = &n {
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
    if let Node::Expr(Expr::Var(v)) = &n {
        assert!(relids::is_member(7, &v.varnullingrels));
    } else {
        panic!("not a var");
    }
}
