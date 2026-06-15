//! Tests for the predtest proof engine over the owned `Expr` tree.
//!
//! These exercise the parts that do NOT cross a seam (the AND/OR recursion
//! structure, `equal()`-based self-implication, NULL-test refutation, the
//! NOT-clause rules) using an mcx context.  The catalog/executor-backed proofs
//! (`operator_predicate_proof` past the `equal()` fast path) require installed
//! lsyscache/executor seams and are covered by integration tests once those
//! owners land.

use std::sync::Once;

use mcx::MemoryContext;
use types_nodes::primnodes::{
    BoolExpr, BoolExprType, Const, Expr, NullTest, NullTestType, Var,
};

use crate::predtest::{predicate_implied_by_impl, predicate_refuted_by_impl};

static INSTALL: Once = Once::new();

/// Install a minimal structural `equal_expr` for the node shapes these tests
/// build (Var / Const / BoolExpr / NullTest).  In production `equal()` is the
/// equalfuncs.c seam (still unported); these tests exercise predtest's own
/// recursion structure, so a faithful structural comparator over the test
/// shapes is sufficient and avoids the seam panic.
fn setup() {
    INSTALL.call_once(|| {
        backend_nodes_equalfuncs_seams::equal_expr::set(test_equal_expr);
        backend_optimizer_util_pathnode_seams::check_for_interrupts::set(|| {});
    });
}

fn test_equal_expr(a: &Expr, b: &Expr) -> bool {
    match (a, b) {
        (Expr::Var(x), Expr::Var(y)) => x.varno == y.varno && x.varattno == y.varattno,
        (Expr::Const(x), Expr::Const(y)) => {
            x.constisnull == y.constisnull
                && x.constvalue.as_usize() == y.constvalue.as_usize()
        }
        (Expr::BoolExpr(x), Expr::BoolExpr(y)) => {
            x.boolop == y.boolop
                && x.args.len() == y.args.len()
                && x.args.iter().zip(&y.args).all(|(p, q)| test_equal_expr(p, q))
        }
        (Expr::NullTest(x), Expr::NullTest(y)) => {
            x.nulltesttype == y.nulltesttype
                && x.argisrow == y.argisrow
                && match (x.arg.as_deref(), y.arg.as_deref()) {
                    (Some(p), Some(q)) => test_equal_expr(p, q),
                    (None, None) => true,
                    _ => false,
                }
        }
        _ => false,
    }
}

fn var(varno: i32, varattno: i16) -> Expr {
    Expr::Var(Var {
        varno,
        varattno,
        ..Default::default()
    })
}

fn and(args: Vec<Expr>) -> Expr {
    Expr::BoolExpr(BoolExpr {
        boolop: BoolExprType::AND_EXPR,
        args,
    })
}

fn or(args: Vec<Expr>) -> Expr {
    Expr::BoolExpr(BoolExpr {
        boolop: BoolExprType::OR_EXPR,
        args,
    })
}

fn not_null(arg: Expr) -> Expr {
    Expr::NullTest(NullTest {
        arg: Some(Box::new(arg)),
        nulltesttype: NullTestType::IS_NOT_NULL,
        argisrow: false,
    })
}

fn is_null(arg: Expr) -> Expr {
    Expr::NullTest(NullTest {
        arg: Some(Box::new(arg)),
        nulltesttype: NullTestType::IS_NULL,
        argisrow: false,
    })
}

fn bool_const(v: bool) -> Expr {
    Expr::Const(Const {
        consttype: 16,
        constvalue: types_tuple::backend_access_common_heaptuple::Datum::from_bool(v),
        constisnull: false,
        ..Default::default()
    })
}

#[test]
fn empty_predicate_is_vacuously_implied() {
    setup();
    let ctx = MemoryContext::new("predtest-test");
    let mcx = ctx.mcx();
    assert!(predicate_implied_by_impl(mcx, &[], &[bool_const(true)], false).unwrap());
}

#[test]
fn empty_clause_list_fails_implication() {
    setup();
    let ctx = MemoryContext::new("predtest-test");
    let mcx = ctx.mcx();
    assert!(!predicate_implied_by_impl(mcx, &[var(1, 1)], &[], false).unwrap());
}

#[test]
fn clause_implies_itself() {
    setup();
    let ctx = MemoryContext::new("predtest-test");
    let mcx = ctx.mcx();
    // atom A => atom A via equal()
    assert!(predicate_implied_by_impl(mcx, &[var(1, 1)], &[var(1, 1)], false).unwrap());
}

#[test]
fn and_clause_implies_each_member() {
    setup();
    let ctx = MemoryContext::new("predtest-test");
    let mcx = ctx.mcx();
    // (x AND y) => x   and   (x AND y) => (x AND y)
    let clause = and(vec![var(1, 1), var(1, 2)]);
    assert!(predicate_implied_by_impl(mcx, &[var(1, 1)], &[clause.clone()], false).unwrap());
    assert!(predicate_implied_by_impl(mcx, &[var(1, 2)], &[clause.clone()], false).unwrap());
    // (x AND y) does NOT imply z
    assert!(!predicate_implied_by_impl(mcx, &[var(1, 3)], &[clause], false).unwrap());
}

#[test]
fn or_clause_implication() {
    setup();
    let ctx = MemoryContext::new("predtest-test");
    let mcx = ctx.mcx();
    // (x OR y) => (x OR y OR z)
    let clause = or(vec![var(1, 1), var(1, 2)]);
    let pred = or(vec![var(1, 1), var(1, 2), var(1, 3)]);
    assert!(predicate_implied_by_impl(mcx, &[pred], &[clause], false).unwrap());
}

#[test]
fn is_not_null_refuted_by_is_null() {
    setup();
    let ctx = MemoryContext::new("predtest-test");
    let mcx = ctx.mcx();
    // foo IS NULL refutes foo IS NOT NULL
    let clause = is_null(var(1, 1));
    let pred = not_null(var(1, 1));
    assert!(predicate_refuted_by_impl(mcx, &[pred], &[clause], false).unwrap());
}

#[test]
fn is_null_refuted_by_is_not_null() {
    setup();
    let ctx = MemoryContext::new("predtest-test");
    let mcx = ctx.mcx();
    // foo IS NOT NULL refutes foo IS NULL
    let clause = not_null(var(1, 1));
    let pred = is_null(var(1, 1));
    assert!(predicate_refuted_by_impl(mcx, &[pred], &[clause], false).unwrap());
}

#[test]
fn empty_predicate_not_refutable() {
    setup();
    let ctx = MemoryContext::new("predtest-test");
    let mcx = ctx.mcx();
    assert!(!predicate_refuted_by_impl(mcx, &[], &[var(1, 1)], false).unwrap());
}
