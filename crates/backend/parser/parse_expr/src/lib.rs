#![allow(non_snake_case)]

mod node_funcs;
#[cfg(test)]
mod tests;

use std::cell::Cell;

use guc_tables::GucVarAccessors;
use mcx::Mcx;
use parser_small1::{make_const, ParseExprKind, ParseRefHookState, ParseState};
use types_error::PgResult;
use types_nodes::rawnodes::{A_Expr, A_Expr_Kind};
use types_nodes::{Node, NodeTag};

pub use node_funcs::{
    expr_collation, expr_is_null_constant, expr_location, expr_type, expr_typmod,
};

std::thread_local! {
    static TRANSFORM_NULL_EQUALS: Cell<bool> = const { Cell::new(false) };
}

pub fn transform_null_equals() -> bool {
    TRANSFORM_NULL_EQUALS.with(|c| c.get())
}

fn set_transform_null_equals(v: bool) {
    TRANSFORM_NULL_EQUALS.with(|c| c.set(v));
}

pub fn init_seams() {
    guc_tables::vars::Transform_null_equals.install(GucVarAccessors {
        get: transform_null_equals,
        set: set_transform_null_equals,
    });
}

pub fn transformExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
    exprKind: ParseExprKind,
) -> PgResult<Node<'mcx>> {
    debug_assert!(exprKind != ParseExprKind::EXPR_KIND_NONE);
    let sv_expr_kind = pstate.p_expr_kind;
    pstate.p_expr_kind = exprKind;

    let result = transformExprRecurse(mcx, pstate, expr);

    pstate.p_expr_kind = sv_expr_kind;
    result
}

pub fn transformExprRecurse<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    stack_depth::check_stack_depth()?;

    match expr.node_tag() {
        NodeTag::T_ParamRef => transformParamRef(mcx, pstate, expr),
        NodeTag::T_A_Const => make_const(mcx, pstate, expr.as_a_const().unwrap()),
        NodeTag::T_A_Expr => {
            let a = expr.as_a_expr().unwrap();
            match a.kind {
                A_Expr_Kind::AEXPR_OP
                | A_Expr_Kind::AEXPR_LIKE
                | A_Expr_Kind::AEXPR_ILIKE
                | A_Expr_Kind::AEXPR_SIMILAR => transformAExprOp(mcx, pstate, a),
                other => panic!(
                    "transformExprRecurse (parse_expr.c): A_Expr kind {other:?} arm \
                     (DISTINCT/NULLIF/IN/BETWEEN/ANY/ALL) unported — unit backend-parser-expr"
                ),
            }
        }
        NodeTag::T_CaseTestExpr | NodeTag::T_Var => Ok(expr),
        other => panic!(
            "transformExprRecurse (parse_expr.c): arm for {other:?} unported — \
             unit backend-parser-expr (ColumnRef/TypeCast/FuncCall/SubLink and friends \
             land with their parser units)"
        ),
    }
}

fn transformAExprOp<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    a: &A_Expr<'mcx>,
) -> PgResult<Node<'mcx>> {
    let lexpr = a.lexpr;
    let rexpr = a.rexpr;

    let is_case_test =
        |n: Option<Node<'mcx>>| n.is_some_and(|n| n.node_tag() == NodeTag::T_CaseTestExpr);
    if transform_null_equals()
        && a.name.len() == 1
        && a.name.first().and_then(|n| n.as_string()).is_some_and(|s| s.sval == "=")
        && (lexpr.is_some_and(expr_is_null_constant) || rexpr.is_some_and(expr_is_null_constant))
        && !is_case_test(lexpr)
        && !is_case_test(rexpr)
    {
        panic!(
            "transformAExprOp (parse_expr.c): transform_null_equals rewrite needs the \
             NullTest vocabulary — unit backend-parser-expr"
        );
    }

    if lexpr.is_some_and(|n| n.node_tag() == NodeTag::T_RowExpr) {
        panic!(
            "transformAExprOp (parse_expr.c): RowExpr operand arms \
             (make_row_comparison_op / ROWCOMPARE sublink) unported — unit backend-parser-expr"
        );
    }

    let lexpr = match lexpr {
        Some(l) => Some(transformExprRecurse(mcx, pstate, l)?),
        None => None,
    };
    let rexpr = match rexpr {
        Some(r) => Some(transformExprRecurse(mcx, pstate, r)?),
        None => None,
    };
    let _ = (lexpr, rexpr, pstate.p_last_srf);

    panic!(
        "transformAExprOp (parse_expr.c): make_op (parse_oper.c) operator lookup \
         (oper()/OprCacheHash/OpernameGetOprid via OPERNAMENSP) unported — \
         unit backend-parser-parse-oper"
    );
}

fn transformParamRef<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let pref = expr.as_param_ref().unwrap();
    let encoding = mbutils::GetDatabaseEncoding();
    match &pstate.p_ref_hook_state {
        ParseRefHookState::FixedParams(_) => {
            parser_small1::fixed_paramref_hook(mcx, pstate, pref, encoding)
        }
        ParseRefHookState::VarParams(_) => {
            parser_small1::variable_paramref_hook(mcx, pstate, pref, encoding)
        }
        ParseRefHookState::None => Err(no_parameter_error(pstate, pref, encoding)),
    }
}

#[cold]
fn no_parameter_error(
    pstate: &ParseState<'_, '_>,
    pref: &types_nodes::ParamRef,
    encoding: wchar::pg_enc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_UNDEFINED_PARAMETER, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_PARAMETER)
            .errmsg(format!("there is no parameter ${}", pref.number))
            .errposition(parser_small1::parser_errposition(pstate, pref.location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_expr.c", 0, "transformParamRef")),
    )
}
