//! Unit tests for the collation-assignment walker.
//!
//! These exercise the collation logic over the owned `Expr` tree using the
//! `lsyscache` seams installed for the test (so `get_typcollation` /
//! `get_collation_name` resolve to predictable values). The seam-install uses a
//! `Once` guard so the suite is order-independent.

use super::*;
use std::sync::Once;
use mcx::{Mcx, PgString};
use ::nodes::primnodes::{
    BoolExpr, BoolExprType, CollateExpr, Const, Expr, OpExpr, Var,
};

// --- Test catalog over the lsyscache + syscache seams --------------------

const TEXTOID: Oid = 25; // pg_type.dat
const FOO_COLL: Oid = 12345; // a made-up non-default collation
const BAR_COLL: Oid = 12346; // a second non-default collation

fn test_get_typcollation(typid: Oid) -> PgResult<Oid> {
    // text is collatable (DEFAULT_COLLATION_OID); everything else noncollatable.
    if typid == TEXTOID {
        Ok(DEFAULT_COLLATION_OID)
    } else {
        Ok(InvalidOid)
    }
}

// `get_collation_name` is the real ported lsyscache fn calling the syscache
// `collation_name` seam; install that seam so error-message paths resolve.
fn test_collation_name<'mcx>(mcx: Mcx<'mcx>, collid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    let name = match collid {
        DEFAULT_COLLATION_OID => "default",
        FOO_COLL => "foo",
        BAR_COLL => "bar",
        _ => "other",
    };
    Ok(Some(PgString::from_str_in(name, mcx)?))
}

fn test_get_func_variadictype(_funcid: Oid) -> PgResult<Oid> {
    Ok(InvalidOid)
}

static INIT: Once = Once::new();

fn install_seams() {
    INIT.call_once(|| {
        lsyscache::get_typcollation::set(test_get_typcollation);
        lsyscache::get_func_variadictype::set(test_get_func_variadictype);
        syscache_seams::collation_name::set(test_collation_name);
    });
}

// --- builders ------------------------------------------------------------

fn text_var(collid: Oid) -> Expr {
    Expr::Var(Var {
        vartype: TEXTOID,
        varcollid: collid,
        ..Var::default()
    })
}

fn text_const(collid: Oid) -> Expr {
    Expr::Const(Const {
        consttype: TEXTOID,
        constcollid: collid,
        ..Const::default()
    })
}

/// A boolean-result OpExpr over two args (e.g. `a = b`). The result type
/// (BOOLOID) is noncollatable, but its inputcollid bubbles from the args.
fn bool_op(args: Vec<Expr>) -> Expr {
    Expr::OpExpr(OpExpr {
        args,
        ..OpExpr::default()
    })
}

// --- tests ---------------------------------------------------------------

#[test]
fn implicit_collation_bubbles_to_op_input() {
    install_seams();
    // a = b where a has foo, b has default → non-default beats default → foo.
    let mut e = bool_op(vec![text_var(FOO_COLL), text_var(DEFAULT_COLLATION_OID)]);
    let cx = ::mcx::MemoryContext::new("test");
    assign_expr_collations_in(cx.mcx(), &mut e).unwrap();
    if let Expr::OpExpr(o) = &e {
        assert_eq!(o.inputcollid, FOO_COLL);
        // BOOLOID isn't collatable → output collation InvalidOid.
        assert_eq!(o.opcollid, InvalidOid);
    } else {
        panic!("expected OpExpr");
    }
}

#[test]
fn conflicting_implicit_collations_yield_invalid_input() {
    install_seams();
    // a = b with two different non-default implicit collations → CONFLICT →
    // inputcollid becomes InvalidOid (no eager error; might fail at runtime).
    let mut e = bool_op(vec![text_var(FOO_COLL), text_var(BAR_COLL)]);
    let cx = ::mcx::MemoryContext::new("test");
    assign_expr_collations_in(cx.mcx(), &mut e).unwrap();
    if let Expr::OpExpr(o) = &e {
        assert_eq!(o.inputcollid, InvalidOid);
    } else {
        panic!("expected OpExpr");
    }
}

#[test]
fn collate_expr_forces_explicit_collation() {
    install_seams();
    // (a COLLATE foo) = (b COLLATE bar) — explicit on each side. The OpExpr's
    // children are independent here; each CollateExpr is EXPLICIT. Two different
    // explicit collations under the same parent op → immediate error.
    let lhs = Expr::CollateExpr(CollateExpr {
        arg: Some(Box::new(text_var(DEFAULT_COLLATION_OID))),
        collOid: FOO_COLL,
        location: -1,
    });
    let rhs = Expr::CollateExpr(CollateExpr {
        arg: Some(Box::new(text_var(DEFAULT_COLLATION_OID))),
        collOid: BAR_COLL,
        location: -1,
    });
    let mut e = bool_op(vec![lhs, rhs]);
    let cx = ::mcx::MemoryContext::new("test");
    let res = assign_expr_collations_in(cx.mcx(), &mut e);
    assert!(res.is_err(), "expected explicit-collation conflict error");
}

#[test]
fn select_common_collation_picks_nondefault() {
    install_seams();
    let mut exprs = vec![text_const(DEFAULT_COLLATION_OID), text_const(FOO_COLL)];
    let coll = select_common_collation(None, &mut exprs, false).unwrap();
    assert_eq!(coll, FOO_COLL);
}

#[test]
fn select_common_collation_conflict_none_ok() {
    install_seams();
    // Two conflicting implicit collations, none_ok=true → InvalidOid (no error).
    let mut exprs = vec![text_const(FOO_COLL), text_const(BAR_COLL)];
    let coll = select_common_collation(None, &mut exprs, true).unwrap();
    assert_eq!(coll, InvalidOid);
    // none_ok=false → error.
    let mut exprs2 = vec![text_const(FOO_COLL), text_const(BAR_COLL)];
    assert!(select_common_collation(None, &mut exprs2, false).is_err());
}

#[test]
fn noncollatable_inputs_give_none() {
    install_seams();
    // AND over two booleans: nothing collatable anywhere.
    let mut e = Expr::BoolExpr(BoolExpr {
        boolop: BoolExprType::AND_EXPR,
        args: vec![
            bool_op(vec![text_var(FOO_COLL), text_var(FOO_COLL)]),
            bool_op(vec![text_var(FOO_COLL), text_var(FOO_COLL)]),
        ],
        location: -1,
    });
    // Should not error; inner ops get foo inputcollid, the AND stays noncollatable.
    let cx = ::mcx::MemoryContext::new("test");
    assign_expr_collations_in(cx.mcx(), &mut e).unwrap();
    if let Expr::BoolExpr(b) = &e {
        for a in &b.args {
            if let Expr::OpExpr(o) = a {
                assert_eq!(o.inputcollid, FOO_COLL);
            }
        }
    }
}
