#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use std::cell::Cell;

use guc_tables::GucVarAccessors;
use mcx::Mcx;
use parser_small1::{make_const, ParseExprKind, ParseRefHookState, ParseState};
use types_error::PgResult;
use types_nodes::rawnodes::{A_Expr, A_Expr_Kind};
use types_nodes::{Node, NodeTag};

pub use nodes_core::node_funcs::{
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
                A_Expr_Kind::AEXPR_DISTINCT | A_Expr_Kind::AEXPR_NOT_DISTINCT => {
                    transformAExprDistinct(mcx, pstate, a)
                }
                A_Expr_Kind::AEXPR_OP_ANY => transformAExprOpAny(mcx, pstate, a),
                A_Expr_Kind::AEXPR_OP_ALL => transformAExprOpAll(mcx, pstate, a),
                A_Expr_Kind::AEXPR_IN => transformAExprIn(mcx, pstate, a),
                other => panic!(
                    "transformExprRecurse (parse_expr.c): A_Expr kind {other:?} arm \
                     (NULLIF) unported — unit backend-parser-expr"
                ),
            }
        }
        NodeTag::T_BooleanTest => transformBooleanTest(mcx, pstate, expr),
        NodeTag::T_RowExpr => transformRowExpr(mcx, pstate, expr),
        NodeTag::T_TypeCast => transformTypeCast(mcx, pstate, expr),
        NodeTag::T_CollateClause => transformCollateClause(mcx, pstate, expr),
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
        // Everywhere DEFAULT is legal the caller strips it before transformExpr.
        NodeTag::T_SetToDefault => Err(default_not_allowed(
            pstate,
            expr.as_set_to_default().unwrap().location,
        )),
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

    if lexpr.is_some_and(|n| n.node_tag() == NodeTag::T_RowExpr)
        && rexpr.is_some_and(|n| n.node_tag() == NodeTag::T_SubLink)
    {
        panic!(
            "transformAExprOp (parse_expr.c): ROW() op (SELECT...) ROWCOMPARE sublink \
             unported — unit backend-parser-expr"
        );
    }
    if lexpr.is_some_and(|n| n.node_tag() == NodeTag::T_RowExpr)
        && rexpr.is_some_and(|n| n.node_tag() == NodeTag::T_RowExpr)
    {
        let lrow = transformExprRecurse(mcx, pstate, lexpr.expect("checked above"))?;
        let rrow = transformExprRecurse(mcx, pstate, rexpr.expect("checked above"))?;
        let largs = &lrow.as_row_expr().expect("transformed RowExpr").args;
        let rargs = &rrow.as_row_expr().expect("transformed RowExpr").args;
        return make_row_comparison_op_lists(mcx, pstate, &a.name, largs, rargs, a.location);
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

fn transformAExprOpAny<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    a: &A_Expr<'mcx>,
) -> PgResult<Node<'mcx>> {
    let lexpr = transformExprRecurse(mcx, pstate, a.lexpr.expect("AEXPR_OP_ANY lexpr"))?;
    let rexpr = transformExprRecurse(mcx, pstate, a.rexpr.expect("AEXPR_OP_ANY rexpr"))?;
    parse_oper::make_scalar_array_op(
        mcx,
        pstate,
        &a.name,
        true,
        lexpr,
        rexpr,
        expr_type(lexpr),
        expr_type(rexpr),
        a.location,
    )
}

fn transformAExprOpAll<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    a: &A_Expr<'mcx>,
) -> PgResult<Node<'mcx>> {
    let lexpr = transformExprRecurse(mcx, pstate, a.lexpr.expect("AEXPR_OP_ALL lexpr"))?;
    let rexpr = transformExprRecurse(mcx, pstate, a.rexpr.expect("AEXPR_OP_ALL rexpr"))?;
    parse_oper::make_scalar_array_op(
        mcx,
        pstate,
        &a.name,
        false,
        lexpr,
        rexpr,
        expr_type(lexpr),
        expr_type(rexpr),
        a.location,
    )
}

fn transformAExprIn<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    a: &A_Expr<'mcx>,
) -> PgResult<Node<'mcx>> {
    use types_core::{InvalidOid, OidIsValid};

    let useOr = a.name.first().and_then(|n| n.as_string()).map(|s| s.sval) != Some("<>");

    let lexpr = transformExprRecurse(mcx, pstate, a.lexpr.expect("IN lexpr"))?;
    let in_list = a.rexpr.expect("IN rexpr").as_list().expect("IN rexpr is a List");
    let mut rexprs = types_nodes::NodeList::nil();
    let mut rvars = types_nodes::NodeList::nil();
    let mut rnonvars = types_nodes::NodeList::nil();
    let mut has_rvars = false;
    for r in in_list {
        let r = transformExprRecurse(mcx, pstate, r)?;
        rexprs.lappend(mcx, r)?;
        if vars::contain_vars_of_level(r, 0)? {
            rvars.lappend(mcx, r)?;
            has_rvars = true;
        } else {
            rnonvars.lappend(mcx, r)?;
        }
    }

    let mut result: Option<Node<'mcx>> = None;
    if rnonvars.len() > 1 {
        let mut typelocs: mcx::PgVec<'mcx, (types_core::Oid, types_core::ParseLoc)> =
            mcx::vec_with_capacity_in(mcx, rnonvars.len() + 1)?;
        typelocs.push((expr_type(lexpr), expr_location(lexpr)));
        for r in &rnonvars {
            typelocs.push((expr_type(r), expr_location(r)));
        }
        let mut scalar_type = coerce::select_common_type(pstate, typelocs.as_slice(), None)?;

        if OidIsValid(scalar_type) {
            let mut alltypes: mcx::PgVec<'mcx, types_core::Oid> =
                mcx::vec_with_capacity_in(mcx, typelocs.len())?;
            for &(t, _) in typelocs.iter() {
                alltypes.push(t);
            }
            if !coerce::verify_common_type(scalar_type, alltypes.as_slice())? {
                scalar_type = InvalidOid;
            }
        }

        let array_type = if OidIsValid(scalar_type) && scalar_type != types_core::catalog::RECORDOID
        {
            lsyscache::get_array_type(scalar_type)?
        } else {
            InvalidOid
        };
        if array_type != InvalidOid {
            let mut aexprs = types_nodes::NodeList::nil();
            for r in &rnonvars {
                let r = coerce::coerce_to_common_type(
                    mcx,
                    pstate,
                    r,
                    expr_type(r),
                    expr_location(r),
                    scalar_type,
                    "IN",
                )?;
                aexprs.lappend(mcx, r)?;
            }
            let newa = Node::mk(
                mcx,
                types_nodes::ArrayExpr {
                    array_typeid: array_type,
                    array_collid: InvalidOid,
                    element_typeid: scalar_type,
                    elements: aexprs,
                    multidims: false,
                    // Vars cannot be safely query-jumbled; disable squashing.
                    list_start: if has_rvars { -1 } else { a.rexpr_list_start },
                    list_end: if has_rvars { -1 } else { a.rexpr_list_end },
                    location: -1,
                },
            )?;
            result = Some(parse_oper::make_scalar_array_op(
                mcx,
                pstate,
                &a.name,
                useOr,
                lexpr,
                newa,
                expr_type(lexpr),
                array_type,
                a.location,
            )?);
            rexprs = rvars;
        }
    }

    for r in &rexprs {
        // C copyObject's lexpr per comparison; the sealed lexpr subtree is
        // shared instead (parse-phase walks only re-write identical values).
        let cmp = if lexpr.node_tag() == NodeTag::T_RowExpr && r.node_tag() == NodeTag::T_RowExpr {
            let largs = &lexpr.as_row_expr().expect("transformed RowExpr").args;
            let rargs = &r.as_row_expr().expect("transformed RowExpr").args;
            make_row_comparison_op_lists(mcx, pstate, &a.name, largs, rargs, a.location)?
        } else {
            parse_oper::make_op(
                mcx,
                pstate,
                &a.name,
                Some(lexpr),
                Some(r),
                expr_type(lexpr),
                expr_type(r),
                pstate.p_last_srf,
                a.location,
            )?
        };
        let cmp =
            coerce::coerce_to_boolean(mcx, pstate, cmp, expr_type(cmp), expr_location(cmp), "IN")?;
        result = Some(match result {
            None => cmp,
            Some(prev) => Node::mk(
                mcx,
                types_nodes::BoolExpr {
                    boolop: if useOr {
                        types_nodes::BoolExprType::OR_EXPR
                    } else {
                        types_nodes::BoolExprType::AND_EXPR
                    },
                    args: types_nodes::NodeList::make2(mcx, prev, cmp)?,
                    location: a.location,
                },
            )?,
        });
    }

    Ok(result.expect("IN list is never empty"))
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
    let (target_type, target_typmod) = parse_utilcmd::typenameTypeIdAndMod(mcx, Some(pstate), tn)?;

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
// transformCollateClause (parse_expr.c) + LookupCollation (parse_type.c);
// the errposition callback collapses into direct positions on the errors.
fn transformCollateClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    use types_core::catalog::UNKNOWNOID;
    use types_nodes::primnodes::CollateExpr;

    let c = expr.as_collate_clause().unwrap();
    let arg = transformExprRecurse(mcx, pstate, c.arg.expect("CollateClause arg"))?;

    let argtype = expr_type(arg);
    if !lsyscache::type_is_collatable(argtype)? && argtype != UNKNOWNOID {
        return Err(collations_not_supported(pstate, argtype, c.location));
    }

    let coll_oid = catalog_namespace::get_collation_oid_list(&c.collname, false)
        .map_err(|e| collation_lookup_position(pstate, e, c.location))?;

    Node::mk(mcx, CollateExpr { arg, collOid: coll_oid, location: c.location })
}

// C: setup_parser_errposition_callback around get_collation_oid.
#[cold]
#[inline(never)]
fn collation_lookup_position(
    pstate: &ParseState<'_, '_>,
    e: Box<types_error::PgError>,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    if e.cursor_position().is_some() {
        return e;
    }
    Box::new((*e).with_cursor_position(parser_small1::parser_errposition(
        pstate,
        location,
        mbutils::GetDatabaseEncoding(),
    )))
}

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

fn transformAExprDistinct<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    a: &A_Expr<'mcx>,
) -> PgResult<Node<'mcx>> {
    if a.rexpr.is_some_and(expr_is_null_constant) {
        return make_nulltest_from_distinct(mcx, pstate, a, a.lexpr);
    }
    if a.lexpr.is_some_and(expr_is_null_constant) {
        return make_nulltest_from_distinct(mcx, pstate, a, a.rexpr);
    }

    let lexpr = match a.lexpr {
        Some(l) => Some(transformExprRecurse(mcx, pstate, l)?),
        None => None,
    };
    let rexpr = match a.rexpr {
        Some(r) => Some(transformExprRecurse(mcx, pstate, r)?),
        None => None,
    };

    if lexpr.is_some_and(|n| n.node_tag() == NodeTag::T_RowExpr)
        && rexpr.is_some_and(|n| n.node_tag() == NodeTag::T_RowExpr)
    {
        panic!(
            "transformAExprDistinct (parse_expr.c): make_row_distinct_op (ROW IS \
             DISTINCT FROM ROW) unported — unit backend-parser-expr"
        );
    }

    let result = make_distinct_op(mcx, pstate, &a.name, lexpr, rexpr, a.location)?;

    if a.kind == A_Expr_Kind::AEXPR_NOT_DISTINCT {
        return Node::mk(
            mcx,
            types_nodes::BoolExpr {
                boolop: types_nodes::BoolExprType::NOT_EXPR,
                args: types_nodes::NodeList::make1(mcx, result)?,
                location: a.location,
            },
        );
    }
    Ok(result)
}

fn make_distinct_op<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    opname: &types_nodes::NodeList<'mcx>,
    ltree: Option<Node<'mcx>>,
    rtree: Option<Node<'mcx>>,
    location: types_core::ParseLoc,
) -> PgResult<Node<'mcx>> {
    let last_srf = pstate.p_last_srf;
    let ltype = ltree.map_or(types_core::InvalidOid, expr_type);
    let rtype = rtree.map_or(types_core::InvalidOid, expr_type);
    let result =
        parse_oper::make_op(mcx, pstate, opname, ltree, rtree, ltype, rtype, last_srf, location)?;
    let op = result.as_op_expr().expect("make_op returns an OpExpr");
    if op.opresulttype != types_core::catalog::BOOLOID {
        return Err(distinct_requires_boolean_eq(pstate, location));
    }
    // C NodeSetTag(result, T_DistinctExpr): same struct, new tag. make_op's
    // retset panic covers C's opretset ereport leg.
    Node::mk(
        mcx,
        types_nodes::DistinctExpr {
            opno: op.opno,
            opfuncid: op.opfuncid,
            opresulttype: op.opresulttype,
            opretset: op.opretset,
            opcollid: op.opcollid,
            inputcollid: op.inputcollid,
            // Shallow list copy; C retags the same node in place.
            args: op.args.clone_in(mcx)?,
            location: op.location,
        },
    )
}

fn make_nulltest_from_distinct<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    distincta: &A_Expr<'mcx>,
    arg: Option<Node<'mcx>>,
) -> PgResult<Node<'mcx>> {
    let arg = match arg {
        Some(a) => Some(transformExprRecurse(mcx, pstate, a)?),
        None => None,
    };
    let t = if distincta.kind == A_Expr_Kind::AEXPR_NOT_DISTINCT {
        types_nodes::primnodes::NullTestType::IS_NULL
    } else {
        types_nodes::primnodes::NullTestType::IS_NOT_NULL
    };
    Node::mk(
        mcx,
        types_nodes::primnodes::NullTest {
            arg,
            nulltesttype: t,
            argisrow: false,
            location: distincta.location,
        },
    )
}

fn transformBooleanTest<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    use types_nodes::BoolTestType;
    let b = expr.as_boolean_test().unwrap();
    let clausename = match b.booltesttype {
        BoolTestType::IS_TRUE => "IS TRUE",
        BoolTestType::IS_NOT_TRUE => "IS NOT TRUE",
        BoolTestType::IS_FALSE => "IS FALSE",
        BoolTestType::IS_NOT_FALSE => "IS NOT FALSE",
        BoolTestType::IS_UNKNOWN => "IS UNKNOWN",
        BoolTestType::IS_NOT_UNKNOWN => "IS NOT UNKNOWN",
    };
    let arg = transformExprRecurse(mcx, pstate, b.arg.expect("BooleanTest.arg"))?;
    let arg =
        coerce::coerce_to_boolean(mcx, pstate, arg, expr_type(arg), expr_location(arg), clausename)?;
    Node::mk(
        mcx,
        types_nodes::BooleanTest {
            arg: Some(arg),
            booltesttype: b.booltesttype,
            location: b.location,
        },
    )
}

fn transformRowExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let r = expr.as_row_expr().unwrap();
    // C transformExpressionList(allowDefault=false): per-item transformExpr at
    // the current p_expr_kind; the multiassign/DEFAULT legs are unreachable
    // from the ported grammar arms.
    let mut args = types_nodes::NodeList::nil();
    for e in r.args.iter() {
        args.lappend(mcx, transformExprRecurse(mcx, pstate, e)?)?;
    }
    if args.len() > types_tuple::htup::MaxTupleAttributeNumber as usize {
        return Err(too_many_row_entries(pstate, r.location));
    }
    let mut colnames = types_nodes::NodeList::nil();
    for fnum in 1..=args.len() {
        let fname: &'mcx [u8] = mcx::slice_in(mcx, format!("f{fnum}").as_bytes())?.leak();
        // SAFETY: "f{N}" is ASCII.
        let fname = unsafe { core::str::from_utf8_unchecked(fname) };
        colnames.lappend(mcx, Node::mk_string(mcx, fname)?)?;
    }
    Node::mk(
        mcx,
        types_nodes::RowExpr {
            args,
            row_typeid: types_core::catalog::RECORDOID,
            row_format: types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
            colnames,
            location: r.location,
        },
    )
}

#[cold]
fn distinct_requires_boolean_eq(
    pstate: &ParseState<'_, '_>,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_DATATYPE_MISMATCH, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg("IS DISTINCT FROM requires = operator to yield boolean".to_string())
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_expr.c", 0, "make_distinct_op")),
    )
}

#[cold]
fn collations_not_supported(
    pstate: &ParseState<'_, '_>,
    argtype: types_core::Oid,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_DATATYPE_MISMATCH, ERROR};
    let tyname = format_type::format_type_be(argtype).unwrap_or_else(|_| argtype.to_string());
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!("collations are not supported by type {tyname}"))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_expr.c", 0, "transformCollateClause")),
    )
}

#[cold]
fn too_many_row_entries(
    pstate: &ParseState<'_, '_>,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_TOO_MANY_COLUMNS, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_COLUMNS)
            .errmsg(format!(
                "ROW expressions can have at most {} entries",
                types_tuple::htup::MaxTupleAttributeNumber
            ))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_expr.c", 0, "transformRowExpr")),
    )
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
        Op::SVFOP_CURRENT_ROLE
        | Op::SVFOP_CURRENT_USER
        | Op::SVFOP_USER
        | Op::SVFOP_SESSION_USER
        | Op::SVFOP_CURRENT_CATALOG
        | Op::SVFOP_CURRENT_SCHEMA => (types_core::catalog::NAMEOID, -1),
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

// C transformWholeRowRef + makeWholeRowVar (parse_expr.c, makefuncs.c):
// whole-row Var leg over RELATION/SUBQUERY RTEs; the JOIN USING alias
// RowExpr expansion and FUNCTION/VALUES/CTE rowtypes are loud.
fn transformWholeRowRef<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    nsitem: &parser_small1::ParseNamespaceItem<'mcx>,
    sublevels_up: i32,
    location: types_core::ParseLoc,
) -> PgResult<Node<'mcx>> {
    use types_core::{InvalidOid, OidIsValid};
    use types_nodes::parsenodes::RTEKind;
    use types_nodes::primnodes::VarReturningType;

    let rte = nsitem.p_rte;
    let is_eref = match rte.eref {
        Some(eref) => core::ptr::eq(nsitem.p_names, eref),
        None => false,
    };
    if !(is_eref || nsitem.p_returning_type != VarReturningType::VAR_RETURNING_DEFAULT) {
        panic!(
            "transformWholeRowRef (parse_expr.c): JOIN USING alias RowExpr expansion \
             unported — unit backend-parser-expr"
        );
    }
    let toid = match rte.rtekind {
        RTEKind::RTE_RELATION => {
            let toid = lsyscache::get_rel_type_id(rte.relid)?;
            if !OidIsValid(toid) {
                return Err(no_composite_type(mcx, rte.relid));
            }
            toid
        }
        RTEKind::RTE_SUBQUERY => {
            if OidIsValid(rte.relid) {
                let toid = lsyscache::get_rel_type_id(rte.relid)?;
                if !OidIsValid(toid) {
                    return Err(no_composite_type(mcx, rte.relid));
                }
                toid
            } else {
                debug_assert!(rte.functions.is_nil());
                types_core::catalog::RECORDOID
            }
        }
        other => panic!(
            "makeWholeRowVar (makefuncs.c): {other:?} whole-row rowtype unported — \
             unit backend-parser-expr"
        ),
    };
    let mut var = types_nodes::Var {
        varno: nsitem.p_rtindex,
        varattno: 0,
        vartype: toid,
        vartypmod: -1,
        varcollid: InvalidOid,
        varnullingrels: types_nodes::Bitmapset::empty(),
        varlevelsup: sublevels_up as types_core::Index,
        varreturningtype: nsitem.p_returning_type,
        varnosyn: nsitem.p_rtindex as types_core::Index,
        varattnosyn: 0,
        location,
    };
    parse_relation::markNullableIfNeeded(mcx, pstate, &mut var)?;
    parse_relation::markVarForSelectPriv(mcx, pstate, &var)?;
    Node::mk(mcx, var)
}

// C makeWholeRowVar's ereport carries no error position.
#[cold]
fn no_composite_type(mcx: Mcx<'_>, relid: types_core::Oid) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_WRONG_OBJECT_TYPE, ERROR};
    let name = lsyscache::get_rel_name(mcx, relid).ok().flatten();
    let name = name.as_ref().map(|s| s.as_str()).unwrap_or("");
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("relation \"{name}\" does not have a composite type"))
            .into_error()
            .with_error_location(ErrorLocation::new("makefuncs.c", 0, "makeWholeRowVar")),
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
    use types_error::{ErrorLocation, ERROR};
    let cref = expr.as_column_ref().unwrap();

    debug_assert!(pstate.p_expr_kind != EXPR_KIND_NONE);
    if matches!(pstate.p_expr_kind, EXPR_KIND_COLUMN_DEFAULT | EXPR_KIND_PARTITION_BOUND) {
        return Err(column_ref_not_allowed(pstate, cref));
    }

    if let parser_small1::PreColumnRefHook::DomainValue(dv) = pstate.p_pre_columnref_hook {
        if let [field1] = cref.fields.as_slice() {
            if field1.as_string().map(|s| s.sval) == Some("value") {
                let mut copy = dv;
                copy.location = cref.location;
                return Node::mk(mcx, copy);
            }
        }
    }

    let field_str = |n: Node<'mcx>| n.as_string().map(|s| s.sval);
    let fields = cref.fields.as_slice();

    // plpgsql_pre_column_ref (pl_exec.c): variable takes precedence.
    let plpgsql_hooks: Option<parser_small1::PlpgsqlHookState<'_>> =
        pstate.p_ref_hook_state.as_plpgsql_params().copied();
    if let Some(st) = &plpgsql_hooks {
        if st.resolve_option == parser_small1::PlpgsqlResolveOption::Variable {
            if let Some(p) = plpgsql_column_ref(mcx, pstate, st, fields, cref.location, false)? {
                return Ok(p);
            }
        }
    }

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
                        Some(nsitem) => Some(transformWholeRowRef(
                            mcx,
                            pstate,
                            nsitem,
                            levels_up,
                            cref.location,
                        )?),
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
                        return transformWholeRowRef(mcx, pstate, nsitem, levels_up, cref.location);
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
                        None => {
                            // C tries a function call on the whole row
                            // (attribute notation). Resolvable only when a
                            // function of that name exists; otherwise C falls
                            // through to errorMissingColumn.
                            if !catalog_namespace::FuncnameGetCandidates(
                                mcx, &[name], 1, false, false,
                            )?
                            .is_empty()
                            {
                                panic!(
                                    "transformColumnRef (parse_expr.c): ParseFuncOrColumn \
                                     whole-row attribute notation unported — \
                                     unit backend-parser-func"
                                );
                            }
                            None
                        }
                    }
                }
            }
        }
        _ => panic!(
            "transformColumnRef (parse_expr.c): >4 dotted names — C raises 42601 here; \
             arm unported with the catalog-qualified lane — unit backend-parser-expr"
        ),
    };

    // plpgsql_post_column_ref (pl_exec.c): runs whether or not the core
    // resolved, to raise the variable-vs-column ambiguity error.
    if let Some(st) = &plpgsql_hooks {
        let skip = st.resolve_option == parser_small1::PlpgsqlResolveOption::Variable
            || (node.is_some()
                && st.resolve_option == parser_small1::PlpgsqlResolveOption::Column);
        if !skip {
            if let Some(p) = plpgsql_column_ref(
                mcx,
                pstate,
                st,
                fields,
                cref.location,
                node.is_none(),
            )? {
                if node.is_some() {
                    let mut name = String::new();
                    for (i, f) in fields.iter().enumerate() {
                        if i > 0 {
                            name.push('.');
                        }
                        name.push_str(field_str(*f).unwrap_or("*"));
                    }
                    return Err(elog::ereport(ERROR)
                        .errcode(types_error::ERRCODE_AMBIGUOUS_COLUMN)
                        .errmsg(format!("column reference \"{name}\" is ambiguous"))
                        .errdetail(
                            "It could refer to either a PL/pgSQL variable or a table column.",
                        )
                        .errposition(parser_small1::parser_errposition(
                            pstate,
                            cref.location,
                            mbutils::GetDatabaseEncoding(),
                        ))
                        .into_error()
                        .with_error_location(ErrorLocation::new(
                            "pl_exec.c",
                            0,
                            "plpgsql_post_column_ref",
                        ))
                        .into());
                }
                return Ok(p);
            }
        }
    }

    match node {
        Some(node) => Ok(node),
        None => {
            if let Some(p) = sql_fn_post_column_ref(mcx, pstate, fields, cref.location)? {
                return Ok(p);
            }
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


// resolve_column_ref marshal: ColumnRef fields to &str names (an A_Star
// field means the reference cannot be a plpgsql name).
fn plpgsql_column_ref<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    st: &parser_small1::PlpgsqlHookState<'_>,
    fields: &[Node<'mcx>],
    location: types_core::ParseLoc,
    error_if_no_field: bool,
) -> PgResult<Option<Node<'mcx>>> {
    let mut names: [&str; 3] = [""; 3];
    if fields.is_empty() || fields.len() > 3 {
        return Ok(None);
    }
    for (i, f) in fields.iter().enumerate() {
        match f.as_string() {
            Some(s) => names[i] = s.sval,
            None => return Ok(None),
        }
    }
    parser_small1::plpgsql_resolve_column_ref(
        mcx,
        pstate,
        st,
        &names[..fields.len()],
        location,
        error_if_no_field,
        mbutils::GetDatabaseEncoding(),
    )
}

// C sql_fn_post_column_ref (executor/functions.c): resolve unmatched column
// references against SQL-function parameter names. Runs only after normal
// column resolution missed, matching C's hook precedence.
fn sql_fn_post_column_ref<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    fields: &[Node<'mcx>],
    location: types_core::ParseLoc,
) -> PgResult<Option<Node<'mcx>>> {
    let Some(state) = pstate.p_ref_hook_state.as_sql_fn_params() else {
        return Ok(None);
    };
    let mut nnames = fields.len();
    if nnames == 0 || nnames > 3 {
        return Ok(None);
    }
    let star = fields[nnames - 1].node_tag() == NodeTag::T_A_Star;
    if star {
        nnames -= 1;
        if nnames == 0 {
            return Ok(None);
        }
    }
    let name = |i: usize| fields[i].as_string().map(|s| s.sval);
    let resolve = |i: usize| {
        name(i).and_then(|n| parser_small1::sql_fn_resolve_param_name(state, n))
    };
    let (param, has_subfield) = match nnames {
        1 => (resolve(0), false),
        2 => {
            if name(0) == Some(state.fname) {
                match resolve(1) {
                    Some(p) => (Some(p), false),
                    None => (resolve(0), true),
                }
            } else {
                (resolve(0), true)
            }
        }
        _ => {
            if name(0) != Some(state.fname) {
                return Ok(None);
            }
            (resolve(1), true)
        }
    };
    let Some((paramno, ptype)) = param else { return Ok(None) };
    if star {
        panic!(
            "sql_fn_post_column_ref (functions.c): whole-row reference to a SQL \
             function parameter unported"
        );
    }
    if has_subfield {
        panic!(
            "sql_fn_post_column_ref (functions.c): composite-parameter field \
             selection (ParseFuncOrColumn on a Param) unported"
        );
    }
    Ok(Some(parser_small1::sql_fn_make_param(mcx, paramno, ptype, location)?))
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
        ParseRefHookState::SqlFnParams(_) => {
            parser_small1::sql_fn_paramref_hook(mcx, pstate, pref, encoding)
        }
        ParseRefHookState::PlpgsqlParams(_) => {
            parser_small1::plpgsql_paramref_hook(mcx, pstate, pref, encoding)
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

// make_row_comparison_op (parse_expr.c), list form: = composes to AND, <>
// to OR; ordered row comparisons (< <= > >=) need RowCompareExpr — loud.
fn make_row_comparison_op_lists<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    opname: &types_nodes::NodeList<'mcx>,
    largs: &types_nodes::NodeList<'mcx>,
    rargs: &types_nodes::NodeList<'mcx>,
    location: types_core::ParseLoc,
) -> PgResult<Node<'mcx>> {
    use lsyscache::{COMPARE_EQ, COMPARE_NE};
    let nopers = largs.len();
    if nopers != rargs.len() {
        return Err(row_length_error(
            pstate,
            types_error::ERRCODE_SYNTAX_ERROR,
            "unequal number of entries in row expressions",
            location,
        ));
    }
    if nopers == 0 {
        return Err(row_length_error(
            pstate,
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            "cannot compare rows of zero length",
            location,
        ));
    }
    let mut opexprs = types_nodes::NodeList::nil();
    for (larg, rarg) in largs.iter().zip(rargs.iter()) {
        let cmp = make_row_comparison_op(mcx, pstate, opname, larg, rarg, location)?;
        opexprs.lappend(mcx, cmp)?;
    }
    if nopers == 1 {
        return Ok(opexprs.nth(0));
    }
    // Intersect each operator's index interpretations; C picks the lowest
    // common CompareType.
    let mut common: Option<u64> = None;
    for cmp in &opexprs {
        let opno = cmp.as_op_expr().expect("make_op returns an OpExpr").opno;
        let interps = lsyscache::get_op_index_interpretation(mcx, opno)?;
        let mut mask: u64 = 0;
        for it in interps.iter() {
            mask |= 1u64 << (it.cmptype as u32);
        }
        common = Some(match common {
            None => mask,
            Some(c) => c & mask,
        });
    }
    let common = common.unwrap_or(0);
    if common == 0 {
        return Err(row_comparison_no_interpretation(pstate, opname, location));
    }
    let cmptype = common.trailing_zeros() as lsyscache::CompareType;
    if cmptype == COMPARE_EQ {
        return Node::mk(
            mcx,
            types_nodes::BoolExpr {
                boolop: types_nodes::BoolExprType::AND_EXPR,
                args: opexprs,
                location,
            },
        );
    }
    if cmptype == COMPARE_NE {
        return Node::mk(
            mcx,
            types_nodes::BoolExpr {
                boolop: types_nodes::BoolExprType::OR_EXPR,
                args: opexprs,
                location,
            },
        );
    }
    panic!(
        "make_row_comparison_op (parse_expr.c): ordered row comparison \
         (RowCompareExpr end-to-end) unported — unit backend-parser-expr"
    );
}

#[cold]
fn row_length_error(
    pstate: &ParseState<'_, '_>,
    code: types_error::SqlState,
    msg: &str,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(code)
            .errmsg(msg.to_string())
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
fn row_comparison_no_interpretation(
    pstate: &ParseState<'_, '_>,
    opname: &types_nodes::NodeList<'_>,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
    let op = opname.last().and_then(|n| n.as_string()).map(|s| s.sval).unwrap_or("");
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "could not determine interpretation of row comparison operator {op}"
            ))
            .errhint("Row comparison operators must be associated with btree operator families.")
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
fn default_not_allowed(
    pstate: &ParseState<'_, '_>,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("DEFAULT is not allowed in this context".to_string())
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_expr.c", 0, "transformExprRecurse")),
    )
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

    // C transforms fn->agg_filter first thing inside ParseFuncOrColumn
    // (transformWhereClause, parse_clause.c); hoisted here to keep the
    // parse_func -> parse_expr edge acyclic. Same evaluation order.
    let agg_filter = match fc.agg_filter {
        None => None,
        Some(f) => {
            let qual = transformExpr(mcx, pstate, f, ParseExprKind::EXPR_KIND_FILTER)?;
            Some(coerce::coerce_to_boolean(
                mcx,
                pstate,
                qual,
                expr_type(qual),
                expr_location(qual),
                "FILTER",
            )?)
        }
    };

    parse_func::ParseFuncOrColumn(
        mcx,
        pstate,
        &fc.funcname,
        fargs,
        arg_types.as_slice(),
        fc,
        agg_filter,
        last_srf,
        fc.location,
    )
}
