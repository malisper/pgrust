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
