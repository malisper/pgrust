//! Unit tests for the #159 Node↔Expr keystone wiring in `subquery_planner`'s
//! expression-preprocessing block. These prove that `preprocess_expression`
//! (eval_const_expressions / canonicalize_qual) and `preprocess_qual_conditions`
//! run end-to-end over the now concretely-typed `Query` expression fields — the
//! step that previously panicked unconditionally for every query.

extern crate alloc;

use mcx::{MemoryContext, PgBox, PgVec};
use types_nodes::nodes::Node;
use types_nodes::primnodes::Expr;
use types_nodes::rawnodes::{FromExpr, RangeTblRef};
use types_pathnodes::PlannerInfo;

use crate::{preprocess_expression, preprocess_qual_conditions, EXPRKIND_QUAL, EXPRKIND_TARGET};

/// A non-null boolean `Const` (the canonical leaf the limit/qual paths see after
/// const-folding).
fn bool_const(value: bool) -> Expr {
    Expr::Const(backend_nodes_core::makefuncs::make_bool_const(value, false))
}

/// `preprocess_expression` over a bare `Const` returns the (folded) `Const`
/// rather than panicking — the EXPRKIND_TARGET path runs `eval_const_expressions`.
#[test]
fn preprocess_expression_target_const_roundtrips() {
    let cx = MemoryContext::new("planner-test");
    let mcx = cx.mcx();
    let root = PlannerInfo::default();
    let out = preprocess_expression(mcx, &root, Some(bool_const(true)), EXPRKIND_TARGET)
        .expect("preprocess_expression must not error");
    match out {
        Some(Expr::Const(_)) => {}
        other => panic!("expected folded Const, got {other:?}"),
    }
}

/// `preprocess_expression(None, _)` short-circuits to `None`.
#[test]
fn preprocess_expression_none_is_none() {
    let cx = MemoryContext::new("planner-test2");
    let mcx = cx.mcx();
    let root = PlannerInfo::default();
    let out = preprocess_expression(mcx, &root, None, EXPRKIND_QUAL)
        .expect("preprocess_expression must not error");
    assert!(out.is_none());
}

/// `preprocess_qual_conditions` over a `FromExpr` whose `quals` is a
/// `Node::Expr(Const)` (the analyzed-jointree shape for `WHERE <bool const>`)
/// preprocesses the qual in place and keeps it wrapped as `Node::Expr` — the
/// jointree-quals bridge the keystone unblocks.
#[test]
fn preprocess_qual_conditions_fromexpr_qual() {
    let cx = MemoryContext::new("planner-test3");
    let mcx = cx.mcx();
    let root = PlannerInfo::default();

    // FromExpr { fromlist: [], quals: Some(Node::Expr(Const true)) }.
    let qual = PgBox::new_in(Node::Expr(bool_const(true)), mcx);
    let from = FromExpr {
        fromlist: PgVec::new_in(mcx),
        quals: Some(qual),
    };
    let mut node = Node::FromExpr(from);

    preprocess_qual_conditions(mcx, &root, &mut node)
        .expect("preprocess_qual_conditions must not panic for a Const qual");

    match node {
        Node::FromExpr(f) => match f.quals.as_deref() {
            Some(Node::Expr(Expr::Const(_))) => {}
            other => panic!("expected re-wrapped Node::Expr(Const) qual, got {other:?}"),
        },
        _ => panic!("jointree top must stay a FromExpr"),
    }
}

/// A `RangeTblRef` leaf in the jointree (the single-table FROM shape) has no
/// quals to process; `preprocess_qual_conditions` is a no-op.
#[test]
fn preprocess_qual_conditions_rangetblref_noop() {
    let cx = MemoryContext::new("planner-test4");
    let mcx = cx.mcx();
    let root = PlannerInfo::default();
    let mut node = Node::RangeTblRef(RangeTblRef { rtindex: 1 });
    preprocess_qual_conditions(mcx, &root, &mut node).expect("RangeTblRef leaf must be a no-op");
}
