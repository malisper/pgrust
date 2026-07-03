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
        NodeTag::T_ColumnRef => transformColumnRef(mcx, pstate, expr),
        NodeTag::T_FuncCall => transformFuncCall(mcx, pstate, expr),
        NodeTag::T_CaseTestExpr | NodeTag::T_Var => Ok(expr),
        other => panic!(
            "transformExprRecurse (parse_expr.c): arm for {other:?} unported — \
             unit backend-parser-expr (TypeCast/SubLink and friends land with their \
             parser units)"
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

    let last_srf = pstate.p_last_srf;
    let lexpr = match lexpr {
        Some(l) => Some(transformExprRecurse(mcx, pstate, l)?),
        None => None,
    };
    let rexpr = match rexpr {
        Some(r) => Some(transformExprRecurse(mcx, pstate, r)?),
        None => None,
    };

    let ltypeId = lexpr.map_or(types_core::InvalidOid, expr_type);
    let rtypeId = rexpr.map_or(types_core::InvalidOid, expr_type);
    parse_oper::make_op(mcx, pstate, &a.name, lexpr, rexpr, ltypeId, rtypeId, last_srf, a.location)
}

// C's PreParseColumnRefHook/PostParseColumnRefHook slots are absent: the
// closed ParseRefHookState set carries no columnref hooks yet (they arrive
// with their installer units).
fn transformColumnRef<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    use parser_small1::ParseExprKind::*;
    let cref = expr.as_column_ref().unwrap();

    debug_assert!(pstate.p_expr_kind != EXPR_KIND_NONE);
    if matches!(pstate.p_expr_kind, EXPR_KIND_COLUMN_DEFAULT | EXPR_KIND_PARTITION_BOUND) {
        return Err(column_ref_not_allowed(pstate, cref));
    }

    let field_str = |n: Node<'mcx>| n.as_string().map(|s| s.sval);
    let fields = cref.fields.as_slice();

    let mut nspname: Option<&str> = None;
    let mut relname: Option<&str> = None;
    let mut colname: Option<&str> = None;
    let mut levels_up = 0;

    let node: Option<Node<'mcx>> = match fields {
        [field1] => {
            let name = field_str(*field1).expect("single-field ColumnRef holds a String");
            colname = Some(name);
            match parse_relation::colNameToVar(mcx, pstate, name, false, cref.location)? {
                Some(node) => Some(node),
                None => {
                    let nsitem = parse_relation::refnameNamespaceItem(
                        pstate,
                        None,
                        name,
                        cref.location,
                        Some(&mut levels_up),
                    )?;
                    match nsitem {
                        Some(_) => panic!(
                            "transformColumnRef (parse_expr.c): transformWholeRowRef \
                             (PostQUEL bare-relation reference) unported — \
                             unit backend-parser-expr"
                        ),
                        None => None,
                    }
                }
            }
        }
        [field1, field2] | [_, field1, field2] | [_, _, field1, field2] => {
            if fields.len() == 3 {
                nspname = Some(field_str(fields[0]).expect("qualifier is a String"));
            } else if fields.len() == 4 {
                panic!(
                    "transformColumnRef (parse_expr.c): catalog-qualified column \
                     reference needs get_database_name — unit backend-parser-expr"
                );
            }
            let rel = field_str(*field1).expect("relation qualifier is a String");
            relname = Some(rel);
            let nsitem = parse_relation::refnameNamespaceItem(
                pstate,
                nspname,
                rel,
                cref.location,
                Some(&mut levels_up),
            )?;
            match nsitem {
                None => None,
                Some(nsitem) => {
                    if field2.node_tag() == NodeTag::T_A_Star {
                        panic!(
                            "transformColumnRef (parse_expr.c): transformWholeRowRef \
                             (rel.* outside a SELECT list) unported — unit backend-parser-expr"
                        );
                    }
                    let name = field_str(*field2).expect("column field is a String");
                    colname = Some(name);
                    match parse_relation::scanNSItemForColumn(
                        mcx,
                        pstate,
                        nsitem,
                        levels_up,
                        name,
                        cref.location,
                    )? {
                        Some(node) => Some(node),
                        None => panic!(
                            "transformColumnRef (parse_expr.c): ParseFuncOrColumn \
                             whole-row fallback unported — unit backend-parser-func"
                        ),
                    }
                }
            }
        }
        _ => panic!(
            "transformColumnRef (parse_expr.c): >4 dotted names — C raises 42601 here; \
             arm unported with the catalog-qualified lane — unit backend-parser-expr"
        ),
    };

    match node {
        Some(node) => Ok(node),
        None => {
            if relname.is_some() && colname.is_none() {
                let rv = Node::mk_mut(
                    mcx,
                    types_nodes::RangeVar {
                        schemaname: nspname.map(|s| str_in(mcx, s)).transpose()?,
                        relname: relname.map(|s| str_in(mcx, s)).transpose()?,
                        location: cref.location,
                        ..Default::default()
                    },
                )?
                .seal_ref();
                Err(parse_relation::errorMissingRTE(mcx, pstate, rv))
            } else {
                Err(parse_relation::errorMissingColumn(
                    pstate,
                    relname,
                    colname.expect("no-column arm always has a colname"),
                    cref.location,
                ))
            }
        }
    }
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_borrow_in(mcx, s.as_bytes())?;
    // SAFETY: byte-for-byte copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

#[cold]
fn column_ref_not_allowed(
    pstate: &ParseState<'_, '_>,
    cref: &types_nodes::rawnodes::ColumnRef<'_>,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
    let msg = match pstate.p_expr_kind {
        parser_small1::ParseExprKind::EXPR_KIND_COLUMN_DEFAULT => {
            "cannot use column reference in DEFAULT expression"
        }
        _ => "cannot use column reference in partition bound expression",
    };
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg_internal(msg)
            .errposition(parser_small1::parser_errposition(
                pstate,
                cref.location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_expr.c", 0, "transformColumnRef")),
    )
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

pub fn ParseExprKindName(exprKind: ParseExprKind) -> &'static str {
    use ParseExprKind::*;
    match exprKind {
        EXPR_KIND_NONE => "invalid expression context",
        EXPR_KIND_OTHER => "extension expression",
        EXPR_KIND_JOIN_ON => "JOIN/ON",
        EXPR_KIND_JOIN_USING => "JOIN/USING",
        EXPR_KIND_FROM_SUBSELECT => "sub-SELECT in FROM",
        EXPR_KIND_FROM_FUNCTION => "function in FROM",
        EXPR_KIND_WHERE | EXPR_KIND_COPY_WHERE => "WHERE",
        EXPR_KIND_POLICY => "POLICY",
        EXPR_KIND_HAVING => "HAVING",
        EXPR_KIND_FILTER => "FILTER",
        EXPR_KIND_WINDOW_PARTITION => "window PARTITION BY",
        EXPR_KIND_WINDOW_ORDER => "window ORDER BY",
        EXPR_KIND_WINDOW_FRAME_RANGE => "window RANGE",
        EXPR_KIND_WINDOW_FRAME_ROWS => "window ROWS",
        EXPR_KIND_WINDOW_FRAME_GROUPS => "window GROUPS",
        EXPR_KIND_SELECT_TARGET => "SELECT",
        EXPR_KIND_INSERT_TARGET => "INSERT",
        EXPR_KIND_UPDATE_SOURCE | EXPR_KIND_UPDATE_TARGET => "UPDATE",
        EXPR_KIND_MERGE_WHEN => "MERGE WHEN",
        EXPR_KIND_GROUP_BY => "GROUP BY",
        EXPR_KIND_ORDER_BY => "ORDER BY",
        EXPR_KIND_DISTINCT_ON => "DISTINCT ON",
        EXPR_KIND_LIMIT => "LIMIT",
        EXPR_KIND_OFFSET => "OFFSET",
        EXPR_KIND_RETURNING | EXPR_KIND_MERGE_RETURNING => "RETURNING",
        EXPR_KIND_VALUES | EXPR_KIND_VALUES_SINGLE => "VALUES",
        EXPR_KIND_CHECK_CONSTRAINT | EXPR_KIND_DOMAIN_CHECK => "CHECK",
        EXPR_KIND_COLUMN_DEFAULT | EXPR_KIND_FUNCTION_DEFAULT => "DEFAULT",
        EXPR_KIND_INDEX_EXPRESSION => "index expression",
        EXPR_KIND_INDEX_PREDICATE => "index predicate",
        EXPR_KIND_STATS_EXPRESSION => "statistics expression",
        EXPR_KIND_ALTER_COL_TRANSFORM => "USING",
        EXPR_KIND_EXECUTE_PARAMETER => "EXECUTE",
        EXPR_KIND_TRIGGER_WHEN => "WHEN",
        EXPR_KIND_PARTITION_BOUND => "partition bound",
        EXPR_KIND_PARTITION_EXPRESSION => "PARTITION BY",
        EXPR_KIND_CALL_ARGUMENT => "CALL",
        EXPR_KIND_GENERATED_COLUMN => "GENERATED AS",
        EXPR_KIND_CYCLE_MARK => "CYCLE",
    }
}

fn transformFuncCall<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let fc = expr.as_func_call().unwrap();
    let last_srf = pstate.p_last_srf;

    let mut fargs = types_nodes::NodeList::nil();
    for arg in &fc.args {
        fargs.lappend(mcx, transformExprRecurse(mcx, pstate, arg)?)?;
    }
    let mut arg_types: mcx::PgVec<'mcx, types_core::Oid> =
        mcx::vec_with_capacity_in(mcx, fargs.len())?;
    for arg in &fargs {
        arg_types.push(expr_type(arg));
    }

    parse_func::ParseFuncOrColumn(
        mcx,
        pstate,
        &fc.funcname,
        fargs,
        arg_types.as_slice(),
        fc,
        last_srf,
        fc.location,
    )
}
