//! Smoke tests over the `Expr` model that do not require any unported seam.

extern crate alloc;
use alloc::boxed::Box;
use alloc::vec;

use mcx::MemoryContext;
use nodes::primnodes::{
    BoolExpr, BoolExprType, BoolTestType, BooleanTest, Const, Expr, NullTest, NullTestType,
};

use crate::fold::eval_const_expressions;
use crate::grounded::{contain_subplans_slice, contain_window_function};

/// A non-null boolean `Const` of the given value.
fn bool_const(value: bool) -> Expr {
    Expr::Const(nodes_core::makefuncs::make_bool_const(value, false))
}

#[test]
fn fold_and_of_true_consts_to_true() {
    let cx = MemoryContext::new("clauses-test");
    let mcx = cx.mcx();
    // AND(true, true) -> Const TRUE
    let and = Expr::BoolExpr(BoolExpr {
        boolop: BoolExprType::AND_EXPR,
        args: vec![bool_const(true), bool_const(true)],
        location: -1,
    });
    let folded = eval_const_expressions(mcx, and).expect("fold");
    let c = folded.as_const().expect("Const");
    assert!(!c.constisnull);
    assert!(c.constvalue.as_bool());
}

#[test]
fn fold_and_with_false_const_to_false() {
    let cx = MemoryContext::new("clauses-test");
    let mcx = cx.mcx();
    // AND(true, false) -> Const FALSE
    let and = Expr::BoolExpr(BoolExpr {
        boolop: BoolExprType::AND_EXPR,
        args: vec![bool_const(true), bool_const(false)],
        location: -1,
    });
    let folded = eval_const_expressions(mcx, and).expect("fold");
    let c = folded.as_const().expect("Const");
    assert!(!c.constisnull);
    assert!(!c.constvalue.as_bool());
}

#[test]
fn fold_or_with_true_const_to_true() {
    let cx = MemoryContext::new("clauses-test");
    let mcx = cx.mcx();
    // OR(false, true) -> Const TRUE
    let or = Expr::BoolExpr(BoolExpr {
        boolop: BoolExprType::OR_EXPR,
        args: vec![bool_const(false), bool_const(true)],
        location: -1,
    });
    let folded = eval_const_expressions(mcx, or).expect("fold");
    let c = folded.as_const().expect("Const");
    assert!(!c.constisnull);
    assert!(c.constvalue.as_bool());
}

#[test]
fn fold_nulltest_on_const() {
    let cx = MemoryContext::new("clauses-test");
    let mcx = cx.mcx();
    // (NULL::bool) IS NULL -> Const TRUE
    let mut null_bool = Const::default();
    null_bool.consttype = 16; // BOOLOID
    null_bool.constisnull = true;
    let ntest = Expr::NullTest(NullTest {
        arg: Some(Box::new(Expr::Const(null_bool))),
        nulltesttype: NullTestType::IS_NULL,
        argisrow: false,
        location: -1,
    });
    let folded = eval_const_expressions(mcx, ntest).expect("fold");
    let c = folded.as_const().expect("Const");
    assert!(!c.constisnull);
    assert!(c.constvalue.as_bool());
}

#[test]
fn fold_booleantest_on_const() {
    let cx = MemoryContext::new("clauses-test");
    let mcx = cx.mcx();
    // (true) IS TRUE -> Const TRUE
    let btest = Expr::BooleanTest(BooleanTest {
        arg: Some(Box::new(bool_const(true))),
        booltesttype: BoolTestType::IS_TRUE,
        location: -1,
    });
    let folded = eval_const_expressions(mcx, btest).expect("fold");
    let c = folded.as_const().expect("Const");
    assert!(!c.constisnull);
    assert!(c.constvalue.as_bool());
}

#[test]
fn contain_subplans_finds_sublink() {
    // A bare bool Const list contains no subplan.
    let exprs = [bool_const(true), bool_const(false)];
    assert!(!contain_subplans_slice(&exprs));

    // A SubLink does.
    let sublink = Expr::SubLink(nodes::primnodes::SubLink {
        subLinkType: nodes::primnodes::SubLinkType::Exists,
        subLinkId: 0,
        testexpr: None,
        subselect: None,
        location: -1,
    });
    let exprs2 = [bool_const(true), sublink];
    assert!(contain_subplans_slice(&exprs2));
}

#[test]
fn window_function_absent_in_bool_const() {
    assert!(!contain_window_function(Some(&bool_const(true))).expect("ok"));
}
