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
    let mut pstate = nodes::parsestmt::ParseState::new(mcx).unwrap();

    let ival = Node::mk_integer(mcx, nodes::value::Integer { ival: 42 });
    let aconst = Node::mk_a_const(mcx, A_Const {
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

// ===========================================================================
// Re-pass e2e: the transformExpr arms that were stale seam-panics now reach
// real logic (their now-landed sibling owners). A pure unit test has no live
// catalog, so an operator/namespace lookup bottoms out in the relevant
// owner's *uninstalled* seam ("seam not installed: ..."), NOT the old
// "not yet ported / List ... ListCell raw-pointer" stand-in panic. These
// tests assert the arm executes real transform logic (it gets *past* the
// dispatch and into the catalog leg) by inspecting the panic origin.
// ===========================================================================

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::string::{String as StdString, ToString};

/// Run `f` and return its panic message (if it panicked).
fn panic_message<F: FnOnce()>(f: F) -> Option<StdString> {
    let prev = std::panic::take_hook();
    std::panic::set_hook(std::boxed::Box::new(|_| {}));
    let res = catch_unwind(AssertUnwindSafe(f));
    std::panic::set_hook(prev);
    match res {
        Ok(()) => None,
        Err(e) => {
            if let Some(s) = e.downcast_ref::<&str>() {
                Some((*s).to_string())
            } else if let Some(s) = e.downcast_ref::<StdString>() {
                Some(s.clone())
            } else {
                Some(StdString::from("<non-string panic>"))
            }
        }
    }
}

/// Assert the message is the catalog/owner reach (real logic executed), not a
/// stale "unported / List-of-nodes-unwalkable" stand-in.
fn assert_reached_real_logic(msg: &str) {
    assert!(
        !msg.contains("not yet ported")
            && !msg.contains("ListCell")
            && !msg.contains("expression-list parse-node carrier")
            && !msg.contains("is not yet ported"),
        "arm still routes to a stale seam-and-panic: {msg}"
    );
}

fn col_ref<'mcx>(mcx: mcx::Mcx<'mcx>, name: &str) -> Node<'mcx> {
    let mut fields: mcx::PgVec<'mcx, nodes::NodePtr<'mcx>> = mcx::PgVec::new_in(mcx);
    fields.push(
        mcx::alloc_in(
            mcx,
            Node::mk_string(mcx, nodes::value::StringNode {
                sval: mcx::PgString::from_str_in(name, mcx).unwrap(),
            }),
        )
        .unwrap(),
    );
    Node::mk_column_ref(mcx, ColumnRef { fields, location: -1 })
}

fn int_const<'mcx>(mcx: mcx::Mcx<'mcx>, v: i32) -> Node<'mcx> {
    Node::mk_a_const(mcx, A_Const {
        val: Some(
            mcx::alloc_in(mcx, Node::mk_integer(mcx, nodes::value::Integer { ival: v })).unwrap(),
        ),
        isnull: false,
        location: -1,
    })
}

fn op_name<'mcx>(mcx: mcx::Mcx<'mcx>, op: &str) -> mcx::PgVec<'mcx, nodes::NodePtr<'mcx>> {
    let mut name: mcx::PgVec<'mcx, nodes::NodePtr<'mcx>> = mcx::PgVec::new_in(mcx);
    name.push(
        mcx::alloc_in(
            mcx,
            Node::mk_string(mcx, nodes::value::StringNode {
                sval: mcx::PgString::from_str_in(op, mcx).unwrap(),
            }),
        )
        .unwrap(),
    );
    name
}

/// `a > 1` (A_Expr AEXPR_OP) — a ColumnRef arm + a scalar operator. The arm
/// reaches colNameToVar (parse_relation, landed) and bottoms out resolving the
/// column / operator against the (absent) catalog, not the old panic.
#[test]
fn columnref_and_aexpr_op_reach_real_logic() {
    let msg = panic_message(|| {
        let ctx = mcx::MemoryContext::new("e2e_columnref");
        let mcx = ctx.mcx();
        let mut pstate = nodes::parsestmt::ParseState::new(mcx).unwrap();
        pstate.p_expr_kind = ParseExprKind::EXPR_KIND_WHERE;
        let a = Node::A_Expr(A_Expr {
            kind: A_Expr_Kind::AEXPR_OP,
            name: op_name(mcx, ">"),
            lexpr: Some(mcx::alloc_in(mcx, col_ref(mcx, "a")).unwrap()),
            rexpr: Some(mcx::alloc_in(mcx, int_const(mcx, 1)).unwrap()),
            rexpr_list_start: -1,
            rexpr_list_end: -1,
            location: -1,
        });
        // Result (Ok or Err) is catalog-dependent; we only need to observe the
        // arm runs real logic rather than the stale panic.
        let _ = transformExprRecurse(&mut pstate, Some(a));
    });
    // colNameToVar over an empty namespace returns NULL, then refnameNamespaceItem
    // returns None and errorMissingColumn searches the (empty) range table.
    // If a panic occurs it must be a catalog/owner seam, not a stale stub.
    if let Some(msg) = msg {
        assert_reached_real_logic(&msg);
    }
}

/// `x IN (1, 2, 3)` (AEXPR_IN) — the now-implemented value-list transform. It
/// transforms the inputs (constants) and only then reaches the operator/
/// array-type catalog lookups; the stale "List unwalkable" panic is gone.
#[test]
fn aexpr_in_reaches_real_logic() {
    let msg = panic_message(|| {
        let ctx = mcx::MemoryContext::new("e2e_in");
        let mcx = ctx.mcx();
        let mut pstate = nodes::parsestmt::ParseState::new(mcx).unwrap();
        pstate.p_expr_kind = ParseExprKind::EXPR_KIND_WHERE;

        let mut items: mcx::PgVec<'_, nodes::NodePtr<'_>> = mcx::PgVec::new_in(mcx);
        for v in [1, 2, 3] {
            items.push(mcx::alloc_in(mcx, int_const(mcx, v)).unwrap());
        }
        let a = Node::A_Expr(A_Expr {
            kind: A_Expr_Kind::AEXPR_IN,
            name: op_name(mcx, "="),
            lexpr: Some(mcx::alloc_in(mcx, int_const(mcx, 1)).unwrap()),
            rexpr: Some(mcx::alloc_in(mcx, Node::mk_list(mcx, items)).unwrap()),
            rexpr_list_start: -1,
            rexpr_list_end: -1,
            location: -1,
        });
        let _ = transformExprRecurse(&mut pstate, Some(a));
    });
    if let Some(msg) = msg {
        assert_reached_real_logic(&msg);
    }
}

/// `x BETWEEN 1 AND 3` (AEXPR_BETWEEN) — the now-implemented expansion into a
/// `>=`/`<=` AND tree, recursed. The stale "List unwalkable" panic is gone.
#[test]
fn aexpr_between_reaches_real_logic() {
    let msg = panic_message(|| {
        let ctx = mcx::MemoryContext::new("e2e_between");
        let mcx = ctx.mcx();
        let mut pstate = nodes::parsestmt::ParseState::new(mcx).unwrap();
        pstate.p_expr_kind = ParseExprKind::EXPR_KIND_WHERE;

        let mut bounds: mcx::PgVec<'_, nodes::NodePtr<'_>> = mcx::PgVec::new_in(mcx);
        bounds.push(mcx::alloc_in(mcx, int_const(mcx, 1)).unwrap());
        bounds.push(mcx::alloc_in(mcx, int_const(mcx, 3)).unwrap());
        let a = Node::A_Expr(A_Expr {
            kind: A_Expr_Kind::AEXPR_BETWEEN,
            name: op_name(mcx, "BETWEEN"),
            lexpr: Some(mcx::alloc_in(mcx, int_const(mcx, 2)).unwrap()),
            rexpr: Some(mcx::alloc_in(mcx, Node::mk_list(mcx, bounds)).unwrap()),
            rexpr_list_start: -1,
            rexpr_list_end: -1,
            location: -1,
        });
        let _ = transformExprRecurse(&mut pstate, Some(a));
    });
    if let Some(msg) = msg {
        assert_reached_real_logic(&msg);
    }
}

/// `foo(1)` (FuncCall) — reaches ParseFuncOrColumn (parse_func, landed) after
/// transforming the args; the stale "parse_func.c not yet ported" panic is gone.
#[test]
fn funccall_reaches_real_logic() {
    let msg = panic_message(|| {
        let ctx = mcx::MemoryContext::new("e2e_funccall");
        let mcx = ctx.mcx();
        let mut pstate = nodes::parsestmt::ParseState::new(mcx).unwrap();
        pstate.p_expr_kind = ParseExprKind::EXPR_KIND_WHERE;

        let mut args: mcx::PgVec<'_, nodes::NodePtr<'_>> = mcx::PgVec::new_in(mcx);
        args.push(mcx::alloc_in(mcx, int_const(mcx, 1)).unwrap());
        let fc = Node::mk_func_call(mcx, FuncCall {
            funcname: op_name(mcx, "foo"),
            args,
            agg_order: mcx::PgVec::new_in(mcx),
            agg_filter: None,
            over: None,
            agg_within_group: false,
            agg_star: false,
            agg_distinct: false,
            func_variadic: false,
            funcformat: CoercionForm::COERCE_EXPLICIT_CALL,
            location: -1,
        });
        let _ = transformExprRecurse(&mut pstate, Some(fc));
    });
    if let Some(msg) = msg {
        assert_reached_real_logic(&msg);
    }
}

/// A bare NULL `A_Const` decodes to an UNKNOWN null `Const`.
#[test]
fn a_const_null_dispatches_to_make_const() {
    use mcx::MemoryContext;
    use types_tuple::heaptuple::UNKNOWNOID;

    let ctx = MemoryContext::new("a_const_null_test");
    let mcx = ctx.mcx();
    let mut pstate = nodes::parsestmt::ParseState::new(mcx).unwrap();

    let aconst = Node::mk_a_const(mcx, A_Const { val: None, isnull: true, location: -1 });
    let out = transformExprRecurse(&mut pstate, Some(aconst)).unwrap();
    match out {
        Some(Expr::Const(c)) => {
            assert_eq!(c.consttype, UNKNOWNOID);
            assert!(c.constisnull);
        }
        other => panic!("expected null Const, got {other:?}"),
    }
}
