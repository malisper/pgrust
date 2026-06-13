//! Tests for nodeMergejoin pure (non-seam) logic: the const-qual classifier
//! and the `INVERT_COMPARE_RESULT` arithmetic.

use types_datum::Datum;
use types_nodes::primnodes::{Const, Expr, Var};

use super::{check_constant_qual, invert_compare_result};

#[test]
fn check_constant_qual_nil_is_true() {
    // A NIL joinqual list is constant-true, and never sets is_const_false.
    let mut cf = false;
    assert!(check_constant_qual(None, &mut cf));
    assert!(!cf);
}

#[test]
fn check_constant_qual_const_true() {
    // A list of constant-true Const nodes is constant, not const-false.
    let quals = [Expr::Const(Const {
        consttype: 16, // BOOLOID
        constvalue: Datum::from_bool(true),
        constisnull: false,
        ..Default::default()
    })];
    let mut cf = false;
    assert!(check_constant_qual(Some(&quals), &mut cf));
    assert!(!cf);
}

#[test]
fn check_constant_qual_const_false_and_null() {
    // constisnull or DatumGetBool(false) => is_const_false set, still "constant".
    let quals = [
        Expr::Const(Const {
            consttype: 16,
            constvalue: Datum::from_bool(false),
            constisnull: false,
            ..Default::default()
        }),
        Expr::Const(Const {
            consttype: 16,
            constvalue: Datum::null(),
            constisnull: true,
            ..Default::default()
        }),
    ];
    let mut cf = false;
    assert!(check_constant_qual(Some(&quals), &mut cf));
    assert!(cf);
}

#[test]
fn check_constant_qual_nonconst_returns_false() {
    // A non-Const term (e.g. a Var) means the qual is not constant.
    let quals = [Expr::Var(Var::default())];
    let mut cf = false;
    assert!(!check_constant_qual(Some(&quals), &mut cf));
}

#[test]
fn invert_compare_result_matches_c_macro() {
    // INVERT_COMPARE_RESULT(var) = (var < 0) ? 1 : -var
    assert_eq!(invert_compare_result(-5), 1);
    assert_eq!(invert_compare_result(-1), 1);
    assert_eq!(invert_compare_result(0), 0);
    assert_eq!(invert_compare_result(1), -1);
    assert_eq!(invert_compare_result(7), -7);
}
