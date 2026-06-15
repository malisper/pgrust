//! Unit tests for the var.c read-only walkers (no cross-crate seam needed).

use crate::var::*;
use types_nodes::nodes::Node;
use types_nodes::primnodes::{Expr, OpExpr, Var};

fn var_at(varno: i32, levelsup: u32) -> Var {
    Var {
        varno,
        varlevelsup: levelsup,
        ..Var::default()
    }
}

fn op_of(args: Vec<Expr>) -> Expr {
    Expr::OpExpr(OpExpr {
        args,
        ..OpExpr::default()
    })
}

#[test]
fn pull_varnos_collects_distinct_level0_varnos() {
    let node = Node::Expr(op_of(vec![
        Expr::Var(var_at(2, 0)),
        Expr::Var(var_at(5, 0)),
        Expr::Var(var_at(2, 0)),
        Expr::Var(var_at(9, 1)), // upper level — ignored
    ]));
    let relids = pull_varnos(None, &node);
    let bms = relids.expect("non-empty");
    // members 2 and 5 set: bit2|bit5 = 0b100100 = 36
    assert_eq!(bms.words[0], (1u64 << 2) | (1u64 << 5));
}

#[test]
fn pull_varnos_of_level_filters_by_level() {
    let node = Node::Expr(op_of(vec![
        Expr::Var(var_at(2, 0)),
        Expr::Var(var_at(7, 1)),
    ]));
    let r0 = pull_varnos_of_level(None, &node, 0).expect("lvl0");
    assert_eq!(r0.words[0], 1u64 << 2);
    let r1 = pull_varnos_of_level(None, &node, 1).expect("lvl1");
    assert_eq!(r1.words[0], 1u64 << 7);
}

#[test]
fn contain_var_clause_detects_level0_var() {
    let with = Node::Expr(Expr::Var(var_at(1, 0)));
    assert!(contain_var_clause(&with));
    let upper = Node::Expr(Expr::Var(var_at(1, 2)));
    assert!(!contain_var_clause(&upper));
    let constish = Node::Expr(Expr::Const(types_nodes::primnodes::Const::default()));
    assert!(!contain_var_clause(&constish));
}

#[test]
fn contain_vars_of_level_matches_specific_level() {
    let node = Node::Expr(op_of(vec![Expr::Var(var_at(1, 1))]));
    assert!(!contain_vars_of_level(&node, 0));
    assert!(contain_vars_of_level(&node, 1));
}

#[test]
fn pull_vars_of_level_clones_matching_vars() {
    let node = Node::Expr(op_of(vec![
        Expr::Var(var_at(3, 0)),
        Expr::Var(var_at(4, 1)),
    ]));
    let vars = pull_vars_of_level(&node, 0);
    assert_eq!(vars.len(), 1);
    match &vars[0] {
        Expr::Var(v) => assert_eq!(v.varno, 3),
        _ => panic!("expected Var"),
    }
}

#[test]
fn locate_var_of_level_reports_location() {
    let mut v = var_at(1, 0);
    v.location = 42;
    let node = Node::Expr(op_of(vec![Expr::Var(v)]));
    assert_eq!(locate_var_of_level(&node, 0), 42);
    // no level-1 var → -1
    assert_eq!(locate_var_of_level(&node, 1), -1);
}

#[test]
fn pull_varattnos_offsets_by_first_low_invalid() {
    // Var varno=1, varattno=1 → member = 1 - (-7) = 8.
    let mut v = var_at(1, 0);
    v.varattno = 1;
    let node = Node::Expr(Expr::Var(v));
    let r = pull_varattnos(&node, 1, None).expect("non-empty");
    assert_eq!(r.words[0], 1u64 << 8);
}

#[test]
fn pull_var_clause_collects_vars() {
    let node = Node::Expr(op_of(vec![
        Expr::Var(var_at(1, 0)),
        Expr::Var(var_at(2, 0)),
    ]));
    let vars = pull_var_clause(&node, 0);
    assert_eq!(vars.len(), 2);
}
