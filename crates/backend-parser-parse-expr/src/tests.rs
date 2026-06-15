//! Unit tests for `parse_expr.c` arms that do not require any panic-until-landed
//! sibling seam.

extern crate std;

use super::*;

/// `ParseExprKindName` matches the C name table for every kind.
#[test]
fn parse_expr_kind_name_table() {
    assert_eq!(
        ParseExprKindName(ParseExprKind::EXPR_KIND_WHERE),
        "WHERE"
    );
    assert_eq!(
        ParseExprKindName(ParseExprKind::EXPR_KIND_NONE),
        "invalid expression context"
    );
    assert_eq!(
        ParseExprKindName(ParseExprKind::EXPR_KIND_MERGE_RETURNING),
        "RETURNING"
    );
    assert_eq!(
        ParseExprKindName(ParseExprKind::EXPR_KIND_GENERATED_COLUMN),
        "GENERATED AS"
    );
    assert_eq!(
        ParseExprKindName(ParseExprKind::EXPR_KIND_CYCLE_MARK),
        "CYCLE"
    );
}

/// `exprIsNullConstant` recognizes only an undecorated NULL `A_Const`.
#[test]
fn null_constant_detection() {
    let null_const = Node::A_Const(A_Const {
        val: None,
        isnull: true,
        location: -1,
    });
    assert!(exprIsNullConstant(Some(&null_const)));

    let not_null = Node::A_Const(A_Const {
        val: None,
        isnull: false,
        location: -1,
    });
    assert!(!exprIsNullConstant(Some(&not_null)));

    assert!(!exprIsNullConstant(None));
}

/// `parser_errposition_impl` clamps negative locations to 0 and passes others
/// through.
#[test]
fn parser_errposition_clamps() {
    assert_eq!(parser_errposition_impl("select 1", -1).unwrap(), 0);
    assert_eq!(parser_errposition_impl("select 1", 7).unwrap(), 7);
}

/// The `T_A_Const` arm wires directly to `parse_node.c` `make_const`
/// (`backend-parser-small1`): an integer literal decodes to an `INT4` `Const`.
#[test]
fn a_const_integer_dispatches_to_make_const() {
    use mcx::MemoryContext;
    use types_tuple::heaptuple::INT4OID;

    let ctx = MemoryContext::new("a_const_test");
    let mcx = ctx.mcx();
    let mut pstate = types_nodes::parsestmt::ParseState::new(mcx).unwrap();

    let ival = Node::Integer(types_nodes::value::Integer { ival: 42 });
    let aconst = Node::A_Const(A_Const {
        val: Some(mcx::alloc_in(mcx, ival).unwrap()),
        isnull: false,
        location: -1,
    });

    let out = transformExprRecurse(&mut pstate, Some(aconst)).unwrap();
    match out {
        Some(Expr::Const(c)) => {
            assert_eq!(c.consttype, INT4OID);
            assert!(!c.constisnull);
        }
        other => panic!("expected Const, got {other:?}"),
    }
}

/// A bare NULL `A_Const` decodes to an UNKNOWN null `Const`.
#[test]
fn a_const_null_dispatches_to_make_const() {
    use mcx::MemoryContext;
    use types_tuple::heaptuple::UNKNOWNOID;

    let ctx = MemoryContext::new("a_const_null_test");
    let mcx = ctx.mcx();
    let mut pstate = types_nodes::parsestmt::ParseState::new(mcx).unwrap();

    let aconst = Node::A_Const(A_Const { val: None, isnull: true, location: -1 });
    let out = transformExprRecurse(&mut pstate, Some(aconst)).unwrap();
    match out {
        Some(Expr::Const(c)) => {
            assert_eq!(c.consttype, UNKNOWNOID);
            assert!(c.constisnull);
        }
        other => panic!("expected null Const, got {other:?}"),
    }
}
