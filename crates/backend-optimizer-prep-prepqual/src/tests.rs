//! Unit tests for the `prepqual.c` port over the owned `Expr` model.
//!
//! These exercise the self-contained boolean canonicalization
//! (`canonicalize_qual` / `negate_clause`) over hand-built `Expr` trees. They
//! deliberately avoid the two unported externals (`get_negator`,
//! `equal_expr`): operator-negator and `process_duplicate_ors` dedup paths
//! need real catalog/equalfuncs owners and are covered once those land.

extern crate alloc;
use alloc::boxed::Box;
use alloc::vec;

use types_nodes::primnodes::{
    BoolExpr, BoolExprType, BoolTestType, BooleanTest, Const, Expr, NullTest, NullTestType, Var,
};

/// A distinct opaque (non-constant) leaf: a `Var` whose `varattno` is `v`.
/// `find_duplicate_ors` drops any `Const` reaching it, so leaves must be
/// non-`Const` to survive.
fn leaf(v: i16) -> Expr {
    Expr::Var(Var {
        varno: 1,
        varattno: v,
        vartype: 16, // BOOLOID
        vartypmod: -1,
        location: -1,
        ..Default::default()
    })
}

/// A non-null boolean `Const`.
fn bool_const(value: bool) -> Expr {
    Expr::Const(backend_nodes_core::makefuncs::make_bool_const(value, false))
}

fn and_of(args: alloc::vec::Vec<Expr>) -> Expr {
    Expr::BoolExpr(BoolExpr {
        boolop: BoolExprType::AND_EXPR,
        args,
        location: -1,
    })
}

fn or_of(args: alloc::vec::Vec<Expr>) -> Expr {
    Expr::BoolExpr(BoolExpr {
        boolop: BoolExprType::OR_EXPR,
        args,
        location: -1,
    })
}

#[test]
fn negate_const_true_to_false() {
    let r = super::negate_clause(bool_const(true)).expect("negate");
    let c = r.as_const().expect("Const");
    assert!(!c.constisnull);
    assert!(!c.constvalue.as_bool());
}

#[test]
fn negate_null_const_stays_null() {
    let null_bool = Const {
        consttype: 16,
        constisnull: true,
        ..Default::default()
    };
    let r = super::negate_clause(Expr::Const(null_bool)).expect("negate");
    let c = r.as_const().expect("Const");
    assert!(c.constisnull);
}

#[test]
fn negate_nulltest_flips_scalar() {
    // (x IS NULL) -> (x IS NOT NULL)
    let nt = Expr::NullTest(NullTest {
        arg: Some(Box::new(leaf(1))),
        nulltesttype: NullTestType::IS_NULL,
        argisrow: false,
        location: -1,
    });
    let r = super::negate_clause(nt).expect("negate");
    let out = r.as_nulltest().expect("NullTest");
    assert_eq!(out.nulltesttype, NullTestType::IS_NOT_NULL);
}

#[test]
fn negate_nulltest_rowtype_wraps_not() {
    // rowtype NullTest cannot be inverted -> wrapped in NOT BoolExpr.
    let nt = Expr::NullTest(NullTest {
        arg: Some(Box::new(leaf(1))),
        nulltesttype: NullTestType::IS_NULL,
        argisrow: true,
        location: -1,
    });
    let r = super::negate_clause(nt).expect("negate");
    let be = r.as_boolexpr().expect("BoolExpr");
    assert_eq!(be.boolop, BoolExprType::NOT_EXPR);
}

#[test]
fn negate_booleantest_flips() {
    let bt = Expr::BooleanTest(BooleanTest {
        arg: Some(Box::new(leaf(1))),
        booltesttype: BoolTestType::IS_TRUE,
        location: -1,
    });
    let r = super::negate_clause(bt).expect("negate");
    let out = r.as_booleantest().expect("BooleanTest");
    assert_eq!(out.booltesttype, BoolTestType::IS_NOT_TRUE);
}

#[test]
fn negate_and_applies_demorgan() {
    // NOT(AND(x IS NULL, y IS NULL)) -> OR((x IS NOT NULL), (y IS NOT NULL))
    let inner = and_of(vec![
        Expr::NullTest(NullTest {
            arg: Some(Box::new(leaf(1))),
            nulltesttype: NullTestType::IS_NULL,
            argisrow: false,
            location: -1,
        }),
        Expr::NullTest(NullTest {
            arg: Some(Box::new(leaf(2))),
            nulltesttype: NullTestType::IS_NULL,
            argisrow: false,
            location: -1,
        }),
    ]);
    let r = super::negate_clause(inner).expect("negate");
    let be = r.as_boolexpr().expect("BoolExpr");
    assert_eq!(be.boolop, BoolExprType::OR_EXPR);
    assert_eq!(be.args.len(), 2);
    for a in &be.args {
        assert_eq!(
            a.as_nulltest().expect("NullTest").nulltesttype,
            NullTestType::IS_NOT_NULL
        );
    }
}

#[test]
fn negate_not_cancels() {
    // NOT(NOT(x IS NULL)) -> (x IS NULL)
    let nt = Expr::NullTest(NullTest {
        arg: Some(Box::new(leaf(1))),
        nulltesttype: NullTestType::IS_NULL,
        argisrow: false,
        location: -1,
    });
    let not = Expr::BoolExpr(BoolExpr {
        boolop: BoolExprType::NOT_EXPR,
        args: vec![nt],
        location: -1,
    });
    let r = super::negate_clause(not).expect("negate");
    assert_eq!(
        r.as_nulltest().expect("NullTest").nulltesttype,
        NullTestType::IS_NULL
    );
}

#[test]
fn canonicalize_empty_qual_is_none() {
    assert!(super::canonicalize_qual(None, false).expect("canon").is_none());
}

#[test]
fn canonicalize_or_with_true_const_reduces_to_true() {
    // WHERE: OR(x, true) -> Const TRUE
    let q = or_of(vec![leaf(1), bool_const(true)]);
    let r = super::canonicalize_qual(Some(q), false)
        .expect("canon")
        .expect("some");
    let c = r.as_const().expect("Const");
    assert!(!c.constisnull);
    assert!(c.constvalue.as_bool());
}

#[test]
fn canonicalize_and_with_false_const_reduces_to_false() {
    // WHERE: AND(x, false) -> Const FALSE
    let q = and_of(vec![leaf(1), bool_const(false)]);
    let r = super::canonicalize_qual(Some(q), false)
        .expect("canon")
        .expect("some");
    let c = r.as_const().expect("Const");
    assert!(!c.constisnull);
    assert!(!c.constvalue.as_bool());
}

#[test]
fn canonicalize_and_drops_true_const() {
    // WHERE: AND(x, true) -> x  (single-element AND reduces to that expr)
    let q = and_of(vec![leaf(7), bool_const(true)]);
    let r = super::canonicalize_qual(Some(q), false)
        .expect("canon")
        .expect("some");
    let v = r.as_var().expect("Var");
    assert_eq!(v.varattno, 7);
}

#[test]
fn canonicalize_flattens_nested_and() {
    // AND(AND(x, y), z) with no consts -> single AND of [x, y, z]
    let q = and_of(vec![and_of(vec![leaf(1), leaf(2)]), leaf(3)]);
    let r = super::canonicalize_qual(Some(q), false)
        .expect("canon")
        .expect("some");
    let be = r.as_boolexpr().expect("BoolExpr");
    assert_eq!(be.boolop, BoolExprType::AND_EXPR);
    assert_eq!(be.args.len(), 3);
}
