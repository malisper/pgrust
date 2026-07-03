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
    expr_collation, expr_is_null_constant, expr_location, expr_location_list, expr_type,
    expr_typmod,
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
                A_Expr_Kind::AEXPR_BETWEEN
                | A_Expr_Kind::AEXPR_NOT_BETWEEN
                | A_Expr_Kind::AEXPR_BETWEEN_SYM
                | A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM => {
                    transformAExprBetween(mcx, pstate, a)
                }
                other => panic!(
                    "transformExprRecurse (parse_expr.c): A_Expr kind {other:?} arm \
                     (DISTINCT/NULLIF/IN/ANY/ALL) unported — unit backend-parser-expr"
                ),
            }
        }
        NodeTag::T_TypeCast => transformTypeCast(mcx, pstate, expr),
        NodeTag::T_BoolExpr => transformBoolExpr(mcx, pstate, expr),
        NodeTag::T_CaseExpr => transformCaseExpr(mcx, pstate, expr),
        NodeTag::T_CoalesceExpr => transformCoalesceExpr(mcx, pstate, expr),
        NodeTag::T_MinMaxExpr => transformMinMaxExpr(mcx, pstate, expr),
        NodeTag::T_SQLValueFunction => transformSQLValueFunction(mcx, expr),
        NodeTag::T_ColumnRef => transformColumnRef(mcx, pstate, expr),
        NodeTag::T_FuncCall => transformFuncCall(mcx, pstate, expr),
        NodeTag::T_SubLink => transformSubLink(mcx, pstate, expr),
        NodeTag::T_NullTest => transformNullTest(mcx, pstate, expr),
        NodeTag::T_GroupingFunc => parse_agg::transformGroupingFunc(
            mcx,
            pstate,
            expr.as_grouping_func().unwrap(),
            |mcx, pstate, arg| {
                let kind = pstate.p_expr_kind;
                transformExpr(mcx, pstate, arg, kind)
            },
        ),
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
        let arg = if lexpr.is_some_and(expr_is_null_constant) { rexpr } else { lexpr };
        let n = Node::mk(
            mcx,
            types_nodes::primnodes::NullTest {
                arg,
                nulltesttype: types_nodes::primnodes::NullTestType::IS_NULL,
                argisrow: false,
                location: a.location,
            },
        )?;
        return transformExprRecurse(mcx, pstate, n);
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

fn transformNullTest<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let n = expr.as_null_test().unwrap();
    let arg = transformExprRecurse(mcx, pstate, n.arg.expect("NullTest.arg"))?;
    // The argument can be any type, so don't coerce it.
    let argisrow = lsyscache::type_is_rowtype(expr_type(arg))?;
    if argisrow {
        panic!(
            "transformExprRecurse (parse_expr.c): row-type NullTest (argisrow) unported — \
             unit backend-parser-expr"
        );
    }
    Node::mk(
        mcx,
        types_nodes::primnodes::NullTest {
            arg: Some(arg),
            nulltesttype: n.nulltesttype,
            argisrow,
            location: n.location,
        },
    )
}

fn transformTypeCast<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let tc = expr.as_type_cast().unwrap();
    let tn = tc
        .typeName
        .expect("TypeCast.typeName")
        .as_variant::<types_nodes::TypeName>()
        .expect("TypeName");
    let (target_type, target_typmod) = parse_utilcmd::typenameTypeIdAndMod(mcx, tn)?;

    let arg = tc.arg.expect("TypeCast.arg");
    if arg.node_tag() == NodeTag::T_A_ArrayExpr {
        panic!(
            "transformTypeCast (parse_expr.c): A_ArrayExpr arm (transformArrayExpr with \
             pushed-down element type) unported — unit backend-parser-expr"
        );
    }
    let arg = transformExprRecurse(mcx, pstate, arg)?;

    let input_type = expr_type(arg);
    if input_type == types_core::InvalidOid {
        return Ok(arg);
    }

    let mut location = tc.location;
    if location < 0 {
        location = tn.location;
    }

    match coerce::coerce_to_target_type(
        mcx,
        pstate,
        arg,
        input_type,
        target_type,
        target_typmod,
        coerce::COERCION_EXPLICIT,
        types_nodes::CoercionForm::COERCE_EXPLICIT_CAST,
        location,
    )? {
        Some(result) => Ok(result),
        None => Err(cannot_cast_error(pstate, input_type, target_type, location, arg)),
    }
}

#[cold]
fn cannot_cast_error(
    pstate: &ParseState<'_, '_>,
    input_type: types_core::Oid,
    target_type: types_core::Oid,
    location: types_core::ParseLoc,
    arg: Node<'_>,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_CANNOT_COERCE, ERROR};
    // C parser_coercion_errposition: coerce location, else the arg's.
    let pos_loc = if location >= 0 { location } else { expr_location(arg) };
    let src = format_type::format_type_be(input_type).unwrap_or_else(|_| input_type.to_string());
    let dst = format_type::format_type_be(target_type).unwrap_or_else(|_| target_type.to_string());
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_CANNOT_COERCE)
            .errmsg(format!("cannot cast type {src} to {dst}"))
            .errposition(parser_small1::parser_errposition(
                pstate,
                pos_loc,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_expr.c", 0, "transformTypeCast")),
    )
}

fn between_a_expr<'mcx>(
    mcx: Mcx<'mcx>,
    op: &'mcx str,
    lexpr: Option<Node<'mcx>>,
    rexpr: Option<Node<'mcx>>,
    location: i32,
) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        types_nodes::A_Expr {
            kind: A_Expr_Kind::AEXPR_OP,
            name: types_nodes::list::NodeList::make1(mcx, Node::mk_string(mcx, op)?)?,
            lexpr,
            rexpr,
            rexpr_list_start: 0,
            rexpr_list_end: 0,
            location,
        },
    )
}

fn between_bool_expr<'mcx>(
    mcx: Mcx<'mcx>,
    boolop: types_nodes::primnodes::BoolExprType,
    arg1: Node<'mcx>,
    arg2: Node<'mcx>,
    location: i32,
) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        types_nodes::primnodes::BoolExpr {
            boolop,
            args: types_nodes::list::NodeList::make2(mcx, arg1, arg2)?,
            location,
        },
    )
}

// transformAExprBetween (parse_expr.c): hard-wired >= <= < > comparisons.
// C copyObject's the re-used raw subexprs; the raw tree is read-only under
// transform, so the arena share is that copy.
fn transformAExprBetween<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    a: &types_nodes::A_Expr<'mcx>,
) -> PgResult<Node<'mcx>> {
    use types_nodes::primnodes::BoolExprType::{AND_EXPR, OR_EXPR};
    let aexpr = a.lexpr;
    let args = a
        .rexpr
        .and_then(|r| r.as_list())
        .expect("BETWEEN rexpr is a two-item List");
    debug_assert_eq!(args.len(), 2);
    let bexpr = Some(args.nth(0));
    let cexpr = Some(args.nth(1));
    let loc = a.location;

    let result = match a.kind {
        A_Expr_Kind::AEXPR_BETWEEN => between_bool_expr(
            mcx,
            AND_EXPR,
            between_a_expr(mcx, ">=", aexpr, bexpr, loc)?,
            between_a_expr(mcx, "<=", aexpr, cexpr, loc)?,
            loc,
        )?,
        A_Expr_Kind::AEXPR_NOT_BETWEEN => between_bool_expr(
            mcx,
            OR_EXPR,
            between_a_expr(mcx, "<", aexpr, bexpr, loc)?,
            between_a_expr(mcx, ">", aexpr, cexpr, loc)?,
            loc,
        )?,
        A_Expr_Kind::AEXPR_BETWEEN_SYM => {
            let sub1 = between_bool_expr(
                mcx,
                AND_EXPR,
                between_a_expr(mcx, ">=", aexpr, bexpr, loc)?,
                between_a_expr(mcx, "<=", aexpr, cexpr, loc)?,
                loc,
            )?;
            let sub2 = between_bool_expr(
                mcx,
                AND_EXPR,
                between_a_expr(mcx, ">=", aexpr, cexpr, loc)?,
                between_a_expr(mcx, "<=", aexpr, bexpr, loc)?,
                loc,
            )?;
            between_bool_expr(mcx, OR_EXPR, sub1, sub2, loc)?
        }
        A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM => {
            let sub1 = between_bool_expr(
                mcx,
                OR_EXPR,
                between_a_expr(mcx, "<", aexpr, bexpr, loc)?,
                between_a_expr(mcx, ">", aexpr, cexpr, loc)?,
                loc,
            )?;
            let sub2 = between_bool_expr(
                mcx,
                OR_EXPR,
                between_a_expr(mcx, "<", aexpr, cexpr, loc)?,
                between_a_expr(mcx, ">", aexpr, bexpr, loc)?,
                loc,
            )?;
            between_bool_expr(mcx, AND_EXPR, sub1, sub2, loc)?
        }
        other => panic!("unrecognized A_Expr kind: {other:?}"),
    };
    transformExprRecurse(mcx, pstate, result)
}

fn transformBoolExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    use types_nodes::BoolExprType::*;
    let b = expr.as_bool_expr().unwrap();
    let opname = match b.boolop {
        AND_EXPR => "AND",
        OR_EXPR => "OR",
        NOT_EXPR => "NOT",
    };

    let mut args = types_nodes::NodeList::nil();
    for arg in &b.args {
        let arg = transformExprRecurse(mcx, pstate, arg)?;
        let arg = coerce::coerce_to_boolean(
            mcx,
            pstate,
            arg,
            expr_type(arg),
            expr_location(arg),
            opname,
        )?;
        args.lappend(mcx, arg)?;
    }

    Node::mk(
        mcx,
        types_nodes::BoolExpr { boolop: b.boolop, args, location: b.location },
    )
}

// C mutates each CaseWhen/arg node in place after select_common_type; sealed
// nodes force the two-phase shape (transform all, pick type, coerce, build) —
// the output tree is identical.
fn transformCaseExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let c = expr.as_case_expr().unwrap();
    let last_srf = pstate.p_last_srf;

    let (arg, placeholder) = match c.arg {
        Some(a) => {
            let mut arg = transformExprRecurse(mcx, pstate, a)?;
            // C: an untyped-literal test expression is forced to text now —
            // the placeholder can't be coerced later.
            if expr_type(arg) == types_core::catalog::UNKNOWNOID {
                arg = coerce::coerce_to_common_type(
                    mcx,
                    pstate,
                    arg,
                    expr_type(arg),
                    expr_location(arg),
                    types_core::catalog::TEXTOID,
                    "CASE",
                )?;
            }
            // C assigns collations mid-transform so the placeholder carries
            // the test expression's collation (seam: parse_collate depends on
            // this crate's expr accessors).
            parse_collate_seams::assign_expr_collations::call(mcx, pstate, arg)?;
            let placeholder = Node::mk(
                mcx,
                types_nodes::primnodes::CaseTestExpr {
                    typeId: expr_type(arg),
                    typeMod: expr_typmod(arg),
                    collation: expr_collation(arg),
                },
            )?;
            (Some(arg), Some(placeholder))
        }
        None => (None, None),
    };

    let mut whens: mcx::PgVec<'mcx, (Node<'mcx>, Node<'mcx>, types_core::ParseLoc)> =
        mcx::vec_with_capacity_in(mcx, c.args.len())?;
    for w in &c.args {
        let w = w.as_case_when().expect("CaseWhen");
        let mut warg = w.expr.expect("CaseWhen.expr");
        if let Some(placeholder) = placeholder {
            warg = Node::mk_a_expr(
                mcx,
                A_Expr_Kind::AEXPR_OP,
                types_nodes::NodeList::make1(mcx, Node::mk_string(mcx, "=")?)?,
                Some(placeholder),
                Some(warg),
                w.location,
            )?;
        }
        let cond = transformExprRecurse(mcx, pstate, warg)?;
        let cond = coerce::coerce_to_boolean(
            mcx,
            pstate,
            cond,
            expr_type(cond),
            expr_location(cond),
            "CASE/WHEN",
        )?;
        let result = transformExprRecurse(mcx, pstate, w.result.expect("CaseWhen.result"))?;
        whens.push((cond, result, w.location));
    }

    let defresult = match c.defresult {
        Some(d) => d,
        None => Node::mk_a_const(mcx, None, -1)?,
    };
    let defresult = transformExprRecurse(mcx, pstate, defresult)?;

    // C: resultexprs = lcons(defresult, ...) — the default result is the most
    // significant type for preferred-type resolution.
    let mut typelocs: mcx::PgVec<'mcx, (types_core::Oid, types_core::ParseLoc)> =
        mcx::vec_with_capacity_in(mcx, whens.len() + 1)?;
    typelocs.push((expr_type(defresult), expr_location(defresult)));
    for &(_, result, _) in whens.iter() {
        typelocs.push((expr_type(result), expr_location(result)));
    }
    let ptype = coerce::select_common_type(pstate, typelocs.as_slice(), Some("CASE"))?;
    debug_assert!(types_core::OidIsValid(ptype));

    let defresult = coerce::coerce_to_common_type(
        mcx,
        pstate,
        defresult,
        expr_type(defresult),
        expr_location(defresult),
        ptype,
        "CASE/ELSE",
    )?;
    let mut args = types_nodes::NodeList::nil();
    for &(cond, result, location) in whens.iter() {
        let result = coerce::coerce_to_common_type(
            mcx,
            pstate,
            result,
            expr_type(result),
            expr_location(result),
            ptype,
            "CASE/WHEN",
        )?;
        args.lappend(
            mcx,
            Node::mk(
                mcx,
                types_nodes::primnodes::CaseWhen {
                    expr: Some(cond),
                    result: Some(result),
                    location,
                },
            )?,
        )?;
    }

    check_srf_in_construct(pstate, last_srf, "CASE")?;

    Node::mk(
        mcx,
        types_nodes::primnodes::CaseExpr {
            casetype: ptype,
            casecollid: types_core::InvalidOid,
            arg,
            args,
            defresult: Some(defresult),
            location: c.location,
        },
    )
}

fn transformCoalesceExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let c = expr.as_coalesce_expr().unwrap();
    let last_srf = pstate.p_last_srf;

    let mut newargs: mcx::PgVec<'mcx, Node<'mcx>> =
        mcx::vec_with_capacity_in(mcx, c.args.len())?;
    let mut typelocs: mcx::PgVec<'mcx, (types_core::Oid, types_core::ParseLoc)> =
        mcx::vec_with_capacity_in(mcx, c.args.len())?;
    for e in &c.args {
        let newe = transformExprRecurse(mcx, pstate, e)?;
        typelocs.push((expr_type(newe), expr_location(newe)));
        newargs.push(newe);
    }

    let coalescetype = coerce::select_common_type(pstate, typelocs.as_slice(), Some("COALESCE"))?;

    let mut coerced = types_nodes::NodeList::nil();
    for (&e, &(typ, loc)) in newargs.iter().zip(typelocs.iter()) {
        coerced.lappend(
            mcx,
            coerce::coerce_to_common_type(mcx, pstate, e, typ, loc, coalescetype, "COALESCE")?,
        )?;
    }

    check_srf_in_construct(pstate, last_srf, "COALESCE")?;

    Node::mk(
        mcx,
        types_nodes::primnodes::CoalesceExpr {
            coalescetype,
            coalescecollid: types_core::InvalidOid,
            args: coerced,
            location: c.location,
        },
    )
}

fn transformMinMaxExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    use types_nodes::primnodes::MinMaxOp;
    let m = expr.as_min_max_expr().unwrap();
    let funcname = if m.op == MinMaxOp::IS_GREATEST { "GREATEST" } else { "LEAST" };

    let mut newargs: mcx::PgVec<'mcx, Node<'mcx>> =
        mcx::vec_with_capacity_in(mcx, m.args.len())?;
    let mut typelocs: mcx::PgVec<'mcx, (types_core::Oid, types_core::ParseLoc)> =
        mcx::vec_with_capacity_in(mcx, m.args.len())?;
    for e in &m.args {
        let newe = transformExprRecurse(mcx, pstate, e)?;
        typelocs.push((expr_type(newe), expr_location(newe)));
        newargs.push(newe);
    }

    let minmaxtype = coerce::select_common_type(pstate, typelocs.as_slice(), Some(funcname))?;

    let mut coerced = types_nodes::NodeList::nil();
    for (&e, &(typ, loc)) in newargs.iter().zip(typelocs.iter()) {
        coerced.lappend(
            mcx,
            coerce::coerce_to_common_type(mcx, pstate, e, typ, loc, minmaxtype, funcname)?,
        )?;
    }

    Node::mk(
        mcx,
        types_nodes::primnodes::MinMaxExpr {
            minmaxtype,
            minmaxcollid: types_core::InvalidOid,
            inputcollid: types_core::InvalidOid,
            op: m.op,
            args: coerced,
            location: m.location,
        },
    )
}

fn transformSQLValueFunction<'mcx>(mcx: Mcx<'mcx>, expr: Node<'mcx>) -> PgResult<Node<'mcx>> {
    use types_core::catalog::{DATEOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID};
    use types_nodes::primnodes::{SQLValueFunction, SQLValueFunctionOp as Op};

    let svf = expr.as_sql_value_function().unwrap();
    let (typ, typmod) = match svf.op {
        Op::SVFOP_CURRENT_DATE => (DATEOID, svf.typmod),
        Op::SVFOP_CURRENT_TIME => (TIMETZOID, svf.typmod),
        Op::SVFOP_CURRENT_TIME_N => (TIMETZOID, anytime_typmod_check(true, svf.typmod)?),
        Op::SVFOP_CURRENT_TIMESTAMP => (TIMESTAMPTZOID, svf.typmod),
        Op::SVFOP_CURRENT_TIMESTAMP_N => {
            (TIMESTAMPTZOID, anytimestamp_typmod_check(true, svf.typmod)?)
        }
        Op::SVFOP_LOCALTIME => (TIMEOID, svf.typmod),
        Op::SVFOP_LOCALTIME_N => (TIMEOID, anytime_typmod_check(false, svf.typmod)?),
        Op::SVFOP_LOCALTIMESTAMP => (TIMESTAMPOID, svf.typmod),
        Op::SVFOP_LOCALTIMESTAMP_N => {
            (TIMESTAMPOID, anytimestamp_typmod_check(false, svf.typmod)?)
        }
        other => panic!(
            "transformSQLValueFunction (parse_expr.c): name-returning op {other:?} unported \
             (grammar arms 2149-2155 are louds) — unit backend-parser-expr"
        ),
    };
    Node::mk(
        mcx,
        SQLValueFunction { op: svf.op, r#type: typ, typmod, location: svf.location },
    )
}

// DIVERGENCE: anytime/anytimestamp_typmod_check live in adt date.c/timestamp.c
// in C; duplicated here until the adt lane exports them (both MAX precisions
// are 6, see date.h/timestamp.h).
fn anytime_typmod_check(istz: bool, typmod: i32) -> PgResult<i32> {
    typmod_check("TIME", istz, typmod)
}

fn anytimestamp_typmod_check(istz: bool, typmod: i32) -> PgResult<i32> {
    typmod_check("TIMESTAMP", istz, typmod)
}

fn typmod_check(what: &str, istz: bool, typmod: i32) -> PgResult<i32> {
    use types_error::{ErrorLocation, ERRCODE_INVALID_PARAMETER_VALUE, ERROR, WARNING};
    const MAX_PRECISION: i32 = 6;
    let tz = if istz { " WITH TIME ZONE" } else { "" };
    if typmod < 0 {
        return Err(Box::new(
            elog::ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("{what}({typmod}){tz} precision must not be negative"))
                .into_error()
                .with_error_location(ErrorLocation::new("parse_expr.c", 0, "typmod_check")),
        ));
    }
    if typmod > MAX_PRECISION {
        elog::ereport(WARNING)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "{what}({typmod}){tz} precision reduced to maximum allowed, {MAX_PRECISION}"
            ))
            .finish(ErrorLocation::new("parse_expr.c", 0, "typmod_check"))?;
        return Ok(MAX_PRECISION);
    }
    Ok(typmod)
}

fn check_srf_in_construct(
    pstate: &ParseState<'_, '_>,
    last_srf: Option<Node<'_>>,
    construct: &str,
) -> PgResult<()> {
    let same = match (pstate.p_last_srf, last_srf) {
        (None, None) => true,
        (Some(a), Some(b)) => a.ptr_eq(b),
        _ => false,
    };
    if !same {
        return Err(srf_not_allowed_in(pstate, construct));
    }
    Ok(())
}

#[cold]
fn srf_not_allowed_in(
    pstate: &ParseState<'_, '_>,
    construct: &str,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
    let loc = pstate.p_last_srf.map_or(-1, expr_location);
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("set-returning functions are not allowed in {construct}"))
            .errhint(
                "You might be able to move the set-returning function into a LATERAL FROM item.",
            )
            .errposition(parser_small1::parser_errposition(
                pstate,
                loc,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_expr.c", 0, "transformCaseExpr")),
    )
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
                    mcx,
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

fn transformSubLink<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    use parser_small1::ParseExprKind::*;
    use types_nodes::SubLinkType;
    let sublink = expr.as_sub_link().unwrap();

    let err: Option<&str> = match pstate.p_expr_kind {
        EXPR_KIND_NONE => unreachable!("can't happen"),
        EXPR_KIND_OTHER
        | EXPR_KIND_JOIN_ON
        | EXPR_KIND_JOIN_USING
        | EXPR_KIND_FROM_SUBSELECT
        | EXPR_KIND_FROM_FUNCTION
        | EXPR_KIND_WHERE
        | EXPR_KIND_POLICY
        | EXPR_KIND_HAVING
        | EXPR_KIND_FILTER
        | EXPR_KIND_WINDOW_PARTITION
        | EXPR_KIND_WINDOW_ORDER
        | EXPR_KIND_WINDOW_FRAME_RANGE
        | EXPR_KIND_WINDOW_FRAME_ROWS
        | EXPR_KIND_WINDOW_FRAME_GROUPS
        | EXPR_KIND_SELECT_TARGET
        | EXPR_KIND_INSERT_TARGET
        | EXPR_KIND_UPDATE_SOURCE
        | EXPR_KIND_UPDATE_TARGET
        | EXPR_KIND_MERGE_WHEN
        | EXPR_KIND_GROUP_BY
        | EXPR_KIND_ORDER_BY
        | EXPR_KIND_DISTINCT_ON
        | EXPR_KIND_LIMIT
        | EXPR_KIND_OFFSET
        | EXPR_KIND_RETURNING
        | EXPR_KIND_MERGE_RETURNING
        | EXPR_KIND_VALUES
        | EXPR_KIND_VALUES_SINGLE
        | EXPR_KIND_CYCLE_MARK => None,
        EXPR_KIND_CHECK_CONSTRAINT | EXPR_KIND_DOMAIN_CHECK => {
            Some("cannot use subquery in check constraint")
        }
        EXPR_KIND_COLUMN_DEFAULT | EXPR_KIND_FUNCTION_DEFAULT => {
            Some("cannot use subquery in DEFAULT expression")
        }
        EXPR_KIND_INDEX_EXPRESSION => Some("cannot use subquery in index expression"),
        EXPR_KIND_INDEX_PREDICATE => Some("cannot use subquery in index predicate"),
        EXPR_KIND_STATS_EXPRESSION => Some("cannot use subquery in statistics expression"),
        EXPR_KIND_ALTER_COL_TRANSFORM => Some("cannot use subquery in transform expression"),
        EXPR_KIND_EXECUTE_PARAMETER => Some("cannot use subquery in EXECUTE parameter"),
        EXPR_KIND_TRIGGER_WHEN => Some("cannot use subquery in trigger WHEN condition"),
        EXPR_KIND_PARTITION_BOUND => Some("cannot use subquery in partition bound"),
        EXPR_KIND_PARTITION_EXPRESSION => {
            Some("cannot use subquery in partition key expression")
        }
        EXPR_KIND_CALL_ARGUMENT => Some("cannot use subquery in CALL argument"),
        EXPR_KIND_COPY_WHERE => Some("cannot use subquery in COPY FROM WHERE condition"),
        EXPR_KIND_GENERATED_COLUMN => {
            Some("cannot use subquery in column generation expression")
        }
    };
    if let Some(msg) = err {
        return Err(sublink_not_allowed(pstate, msg, sublink.location));
    }

    pstate.p_hasSubLinks = true;

    let qtree = analyze_seams::parse_sub_analyze::call(
        mcx,
        sublink.subselect,
        pstate,
        None,
        false,
        true,
    )?;

    if qtree.commandType != types_nodes::CmdType::CMD_SELECT {
        return Err(Box::new(types_error::PgError::error(
            "unexpected non-SELECT command in SubLink".to_string(),
        )));
    }

    let (testexpr, oper_name) = match sublink.subLinkType {
        SubLinkType::EXISTS_SUBLINK => (None, types_nodes::NodeList::nil()),
        SubLinkType::EXPR_SUBLINK => {
            let nonjunk = qtree
                .targetList
                .iter()
                .filter(|te| !te.as_target_entry().expect("tlist entry").resjunk)
                .count();
            if nonjunk != 1 {
                return Err(one_column_required(pstate, sublink.location));
            }
            (None, types_nodes::NodeList::nil())
        }
        SubLinkType::ANY_SUBLINK | SubLinkType::ALL_SUBLINK => {
            let oper_name = if sublink.operName.is_nil() {
                types_nodes::NodeList::make1(mcx, Node::mk_string(mcx, "=")?)?
            } else {
                sublink.operName.clone_in(mcx)?
            };
            let lefthand = transformExprRecurse(
                mcx,
                pstate,
                sublink.testexpr.expect("ANY/ALL sublink carries a testexpr"),
            )?;
            if lefthand.node_tag() == NodeTag::T_RowExpr {
                panic!(
                    "transformSubLink (parse_expr.c): RowExpr lefthand (multi-column \
                     row comparison) unported — unit backend-parser-expr"
                );
            }
            let mut right_param = None;
            let mut right_count = 0usize;
            for te_node in &qtree.targetList {
                let tent = te_node.as_target_entry().expect("tlist entry");
                if tent.resjunk {
                    continue;
                }
                right_count += 1;
                if right_param.is_none() {
                    right_param = Some(Node::mk(
                        mcx,
                        types_nodes::Param {
                            paramkind: types_nodes::ParamKind::PARAM_SUBLINK,
                            paramid: tent.resno as i32,
                            paramtype: expr_type(tent.expr),
                            paramtypmod: expr_typmod(tent.expr),
                            paramcollid: expr_collation(tent.expr),
                            location: -1,
                        },
                    )?);
                }
            }
            if right_count > 1 {
                return Err(column_count_mismatch(
                    pstate,
                    "subquery has too many columns",
                    sublink.location,
                ));
            }
            let Some(rarg) = right_param else {
                return Err(column_count_mismatch(
                    pstate,
                    "subquery has too few columns",
                    sublink.location,
                ));
            };
            let test =
                make_row_comparison_op(mcx, pstate, &oper_name, lefthand, rarg, sublink.location)?;
            (Some(test), oper_name)
        }
        other => panic!(
            "transformSubLink (parse_expr.c): {other:?} arm (ROWCOMPARE/MULTIEXPR/\
             ARRAY) unported — unit backend-parser-expr"
        ),
    };

    Node::mk(
        mcx,
        types_nodes::SubLink {
            subLinkType: sublink.subLinkType,
            subLinkId: sublink.subLinkId,
            testexpr,
            operName: oper_name,
            subselect: Node::mk(mcx, qtree)?,
            location: sublink.location,
        },
    )
}

// make_row_comparison_op (parse_expr.c), single-column reduction; the RowExpr
// (nopers > 1) legs are loud at the caller.
fn make_row_comparison_op<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    opname: &types_nodes::NodeList<'mcx>,
    larg: Node<'mcx>,
    rarg: Node<'mcx>,
    location: types_core::ParseLoc,
) -> PgResult<Node<'mcx>> {
    let last_srf = pstate.p_last_srf;
    let ltype = expr_type(larg);
    let rtype = expr_type(rarg);
    let cmp = parse_oper::make_op(
        mcx,
        pstate,
        opname,
        Some(larg),
        Some(rarg),
        ltype,
        rtype,
        last_srf,
        location,
    )?;
    let op = cmp.as_op_expr().expect("make_op returns an OpExpr");
    if op.opresulttype != types_core::catalog::BOOLOID {
        return Err(row_comparison_not_boolean(pstate, op.opresulttype, location));
    }
    if coerce::expression_returns_set(cmp) {
        return Err(row_comparison_returns_set(pstate, location));
    }
    Ok(cmp)
}

#[cold]
fn column_count_mismatch(
    pstate: &ParseState<'_, '_>,
    msg: &str,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(msg.to_string())
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_expr.c", 0, "transformSubLink")),
    )
}

#[cold]
fn row_comparison_not_boolean(
    pstate: &ParseState<'_, '_>,
    resulttype: types_core::Oid,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_DATATYPE_MISMATCH, ERROR};
    let tyname = format_type::format_type_be(resulttype).unwrap_or_else(|_| resulttype.to_string());
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!(
                "row comparison operator must yield type boolean, not type {tyname}"
            ))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_expr.c", 0, "make_row_comparison_op")),
    )
}

#[cold]
fn row_comparison_returns_set(
    pstate: &ParseState<'_, '_>,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_DATATYPE_MISMATCH, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg("row comparison operator must not return a set".to_string())
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_expr.c", 0, "make_row_comparison_op")),
    )
}

#[cold]
fn sublink_not_allowed(
    pstate: &ParseState<'_, '_>,
    msg: &str,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg_internal(msg)
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_expr.c", 0, "transformSubLink")),
    )
}

#[cold]
fn one_column_required(
    pstate: &ParseState<'_, '_>,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("subquery must return only one column".to_string())
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_expr.c", 0, "transformSubLink")),
    )
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
