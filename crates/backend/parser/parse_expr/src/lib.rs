#![allow(non_snake_case)]

mod subscripts;
#[cfg(test)]
mod tests;

mod json;

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
pub use subscripts::{subscript_handler_for, transformContainerSubscripts, SubscriptHandler};

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
                A_Expr_Kind::AEXPR_NULLIF => transformAExprNullIf(mcx, pstate, a),
            }
        }
        NodeTag::T_BooleanTest => transformBooleanTest(mcx, pstate, expr),
        NodeTag::T_NamedArgExpr => {
            let raw = expr.as_named_arg_expr().unwrap().arg.expect("NamedArgExpr has an arg");
            let arg = transformExprRecurse(mcx, pstate, raw)?;
            // SAFETY: parse analysis exclusively owns the just-built raw tree.
            unsafe {
                expr.with_mut::<types_nodes::primnodes::NamedArgExpr, _>(|n| n.arg = Some(arg))
            }
            .expect("node checked as NamedArgExpr");
            Ok(expr)
        }
        NodeTag::T_RowExpr => transformRowExpr(mcx, pstate, expr, false),
        NodeTag::T_TypeCast => transformTypeCast(mcx, pstate, expr),
        NodeTag::T_CollateClause => transformCollateClause(mcx, pstate, expr),
        NodeTag::T_BoolExpr => transformBoolExpr(mcx, pstate, expr),
        NodeTag::T_CaseExpr => transformCaseExpr(mcx, pstate, expr),
        NodeTag::T_CoalesceExpr => transformCoalesceExpr(mcx, pstate, expr),
        NodeTag::T_MinMaxExpr => transformMinMaxExpr(mcx, pstate, expr),
        NodeTag::T_SQLValueFunction => transformSQLValueFunction(mcx, expr),
        NodeTag::T_ColumnRef => transformColumnRef(mcx, pstate, expr),
        NodeTag::T_A_Indirection => transformIndirection(mcx, pstate, expr),
        NodeTag::T_A_ArrayExpr => {
            transformArrayExpr(mcx, pstate, expr.as_a_array_expr().unwrap(), 0, 0, -1)
        }
        NodeTag::T_MultiAssignRef => transformMultiAssignRef(mcx, pstate, expr),
        NodeTag::T_FuncCall => transformFuncCall(mcx, pstate, expr),
        // C mutates na->arg in place; sealed nodes rebuild the wrapper.
        NodeTag::T_NamedArgExpr => {
            let na = expr.as_named_arg_expr().unwrap();
            let arg = transformExprRecurse(mcx, pstate, na.arg.expect("NamedArgExpr has an arg"))?;
            Node::mk(
                mcx,
                types_nodes::NamedArgExpr {
                    arg: Some(arg),
                    name: na.name,
                    argnumber: na.argnumber,
                    location: na.location,
                },
            )
        }
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
        NodeTag::T_JsonObjectConstructor => json::transformJsonObjectConstructor(mcx, pstate, expr),
        NodeTag::T_JsonArrayConstructor => json::transformJsonArrayConstructor(mcx, pstate, expr),
        NodeTag::T_JsonArrayQueryConstructor => {
            json::transformJsonArrayQueryConstructor(mcx, pstate, expr)
        }
        NodeTag::T_JsonObjectAgg => json::transformJsonObjectAgg(mcx, pstate, expr),
        NodeTag::T_JsonArrayAgg => json::transformJsonArrayAgg(mcx, pstate, expr),
        NodeTag::T_JsonIsPredicate => json::transformJsonIsPredicate(mcx, pstate, expr),
        NodeTag::T_JsonParseExpr => json::transformJsonParseExpr(mcx, pstate, expr),
        NodeTag::T_JsonScalarExpr => json::transformJsonScalarExpr(mcx, pstate, expr),
        NodeTag::T_JsonSerializeExpr => json::transformJsonSerializeExpr(mcx, pstate, expr),
        NodeTag::T_JsonFuncExpr => json::transformJsonFuncExpr(mcx, pstate, expr),
        NodeTag::T_XmlExpr => transformXmlExpr(mcx, pstate, expr),
        NodeTag::T_XmlSerialize => transformXmlSerialize(mcx, pstate, expr),
        NodeTag::T_MergeSupportFunc => transformMergeSupportFunc(pstate, expr),
        NodeTag::T_CaseTestExpr | NodeTag::T_Var => Ok(expr),
        NodeTag::T_CurrentOfExpr => transformCurrentOfExpr(mcx, pstate, expr),
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

// transformCurrentOfExpr (parse_expr.c).
fn transformCurrentOfExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    const REFCURSOROID: types_core::Oid = 1790;
    let rtindex = pstate
        .p_target_nsitem
        .expect("CURRENT OF only at top level of UPDATE/DELETE")
        .p_rtindex;
    let cursor_name = expr.as_current_of_expr().expect("T_CurrentOfExpr node").cursor_name;
    let mut cursor_param = 0;
    if let Some(name) = cursor_name {
        if let Some(st) = pstate.p_ref_hook_state.as_plpgsql_params().copied() {
            if let Some(node) = parser_small1::plpgsql_resolve_column_ref(
                mcx,
                pstate,
                &st,
                &[name],
                -1,
                false,
                mbutils::GetDatabaseEncoding(),
            )? {
                if let Some(p) = node.as_param() {
                    if p.paramkind == types_nodes::ParamKind::PARAM_EXTERN
                        && p.paramtype == REFCURSOROID
                    {
                        cursor_param = p.paramid;
                    }
                }
            }
        }
    }
    // SAFETY: freshly built raw-parse node; single mutator, tag matches.
    unsafe {
        expr.with_mut::<types_nodes::CurrentOfExpr, _>(|c| {
            c.cvarno = rtindex as types_core::Index;
            if cursor_param != 0 {
                c.cursor_name = None;
                c.cursor_param = cursor_param;
            }
        })
        .expect("T_CurrentOfExpr node");
    }
    Ok(expr)
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
        && rexpr.is_some_and(|n| {
            n.as_sub_link()
                .is_some_and(|s| s.subLinkType == types_nodes::SubLinkType::EXPR_SUBLINK)
        })
    {
        let s = rexpr.expect("checked above").as_sub_link().expect("checked above");
        let converted = Node::mk(
            mcx,
            types_nodes::SubLink {
                subLinkType: types_nodes::SubLinkType::ROWCOMPARE_SUBLINK,
                subLinkId: s.subLinkId,
                testexpr: lexpr,
                operName: a.name.clone_in(mcx)?,
                subselect: s.subselect,
                location: a.location,
            },
        )?;
        return transformExprRecurse(mcx, pstate, converted);
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

// transformAExprNullIf (parse_expr.c). C retags the OpExpr in place
// (typedef OpExpr NullIfExpr); sealed nodes rebuild under the new tag.
fn transformAExprNullIf<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    a: &A_Expr<'mcx>,
) -> PgResult<Node<'mcx>> {
    let last_srf = pstate.p_last_srf;
    let lexpr = transformExprRecurse(mcx, pstate, a.lexpr.expect("AEXPR_NULLIF lexpr"))?;
    let rexpr = transformExprRecurse(mcx, pstate, a.rexpr.expect("AEXPR_NULLIF rexpr"))?;
    let result = parse_oper::make_op(
        mcx,
        pstate,
        &a.name,
        Some(lexpr),
        Some(rexpr),
        expr_type(lexpr),
        expr_type(rexpr),
        last_srf,
        a.location,
    )?;
    let op = result.as_op_expr().expect("make_op yields an OpExpr");
    if op.opresulttype != types_core::catalog::BOOLOID {
        return Err(construct_requires_boolean_eq(pstate, "NULLIF", a.location));
    }
    if op.opretset {
        return Err(construct_must_not_return_set(pstate, "NULLIF", a.location));
    }
    Node::mk(
        mcx,
        types_nodes::primnodes::NullIfExpr {
            opno: op.opno,
            opfuncid: op.opfuncid,
            opresulttype: expr_type(op.args.nth(0)),
            opretset: op.opretset,
            opcollid: op.opcollid,
            inputcollid: op.inputcollid,
            args: op.args.clone_in(mcx)?,
            location: op.location,
        },
    )
}

#[cold]
fn construct_requires_boolean_eq(
    pstate: &ParseState<'_, '_>,
    construct: &str,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_DATATYPE_MISMATCH, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!("{construct} requires = operator to yield boolean"))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformAExprNullIf")),
    )
}

/// transformMergeSupportFunc (parse_expr.c): MERGE_ACTION() is only legal in
/// the RETURNING list of a MERGE command; otherwise error, else pass through.
#[allow(non_snake_case)]
fn transformMergeSupportFunc<'mcx>(
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    if pstate.p_expr_kind != ParseExprKind::EXPR_KIND_MERGE_RETURNING {
        let mut parent = pstate.parentParseState;
        while let Some(pp) = parent {
            if pp.p_expr_kind == ParseExprKind::EXPR_KIND_MERGE_RETURNING {
                break;
            }
            parent = pp.parentParseState;
        }
        if parent.is_none() {
            use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
            let f = expr.as_merge_support_func().expect("MergeSupportFunc");
            return Err(Box::new(
                elog::ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(
                        "MERGE_ACTION() can only be used in the RETURNING list of a MERGE command",
                    )
                    .errposition(parser_small1::parser_errposition(
                        pstate,
                        f.location,
                        mbutils::GetDatabaseEncoding(),
                    ))
                    .into_error()
                    .with_error_location(ErrorLocation::new(
                        "parse_expr.c",
                        0,
                        "transformMergeSupportFunc",
                    )),
            ));
        }
    }
    Ok(expr)
}

#[cold]
fn construct_must_not_return_set(
    pstate: &ParseState<'_, '_>,
    construct: &str,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_DATATYPE_MISMATCH, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!("{construct} must not return a set"))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformAExprNullIf")),
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

// transformIndirection (parse_expr.c): adjacent A_Indices merge into one
// SubscriptingRef; String members project via ParseComplexProjection (the
// attribute-notation function fallback is loud).
fn transformIndirection<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let ind = expr.as_a_indirection().unwrap();
    let last_srf = pstate.p_last_srf;
    let mut result = transformExprRecurse(mcx, pstate, ind.arg.expect("A_Indirection.arg"))?;
    let mut subscripts: types_nodes::NodeList<'mcx> = types_nodes::NodeList::nil();
    let location = expr_location(result);

    for n in ind.indirection.iter() {
        match n.node_tag() {
            NodeTag::T_A_Indices => subscripts.lappend(mcx, n)?,
            NodeTag::T_A_Star => {
                return Err(row_expansion_error(pstate, expr_location(result)));
            }
            _ => {
                let name = n.as_string().expect("indirection member is String").sval;
                if !subscripts.is_nil() {
                    result = transformContainerSubscripts(
                        mcx,
                        pstate,
                        result,
                        expr_type(result),
                        expr_typmod(result),
                        &subscripts,
                        false,
                    )?;
                    subscripts = types_nodes::NodeList::nil();
                }
                let argtype = expr_type(result);
                // C ISCOMPLEX (parse_type.h): typeOrDomainTypeRelid looks
                // through domains over composites.
                let could_be_projection = argtype == types_core::catalog::RECORDOID
                    || types_core::OidIsValid(lsyscache::get_typ_typrelid(
                        lsyscache::getBaseType(argtype)?,
                    )?);
                let projected = if could_be_projection {
                    parse_func::ParseComplexProjection(mcx, pstate, name, result, location)?
                } else {
                    None
                };
                let projected = match projected {
                    Some(newresult) => Some(newresult),
                    None => {
                        attribute_notation_func_call(mcx, pstate, name, result, last_srf, location)?
                    }
                };
                match projected {
                    Some(newresult) => result = newresult,
                    None => return Err(unknown_attribute(pstate, result, name, location)),
                }
            }
        }
    }
    if !subscripts.is_nil() {
        result = transformContainerSubscripts(
            mcx,
            pstate,
            result,
            expr_type(result),
            expr_typmod(result),
            &subscripts,
            false,
        )?;
    }
    Ok(result)
}

#[cold]
fn row_expansion_error(pstate: &ParseState<'_, '_>, location: i32) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("row expansion via \"*\" is not supported here".to_string())
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformIndirection")),
    )
}

#[cold]
fn unknown_attribute(
    pstate: &ParseState<'_, '_>,
    relref: Node<'_>,
    attname: &str,
    location: i32,
) -> Box<types_error::PgError> {
    use types_error::{
        ErrorLocation, ERRCODE_UNDEFINED_COLUMN, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
    };
    let errpos =
        parser_small1::parser_errposition(pstate, location, mbutils::GetDatabaseEncoding());
    let builder = if let Some(v) = relref.as_var().filter(|v| v.varattno == types_core::InvalidAttrNumber)
    {
        let rte = parse_relation::GetRTEByRangeTablePosn(pstate, v.varno, v.varlevelsup as i32);
        let alias = rte.eref.and_then(|a| a.aliasname).unwrap_or("");
        elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!("column {alias}.{attname} does not exist"))
    } else {
        let rel_type_id = expr_type(relref);
        // C ISCOMPLEX: domain-aware.
        let is_complex = types_core::OidIsValid(
            lsyscache::getBaseType(rel_type_id)
                .and_then(lsyscache::get_typ_typrelid)
                .unwrap_or(types_core::InvalidOid),
        );
        let tyname = format_type::format_type_be(rel_type_id)
            .unwrap_or_else(|_| std::string::String::from("???"));
        if is_complex {
            elog::ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!("column \"{attname}\" not found in data type {tyname}"))
        } else if rel_type_id == types_core::catalog::RECORDOID {
            elog::ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!(
                    "could not identify column \"{attname}\" in record data type"
                ))
        } else {
            elog::ereport(ERROR).errcode(ERRCODE_WRONG_OBJECT_TYPE).errmsg(format!(
                "column notation .{attname} applied to type {tyname}, which is not a composite type"
            ))
        }
    };
    Box::new(
        builder
            .errposition(errpos)
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "unknown_attribute")),
    )
}

// The ParseFuncOrColumn(fn=NULL) attribute-notation leg shared by
// transformIndirection and transformColumnRef. C returns NULL on
// FUNCDETAIL_NOTFOUND when fn == NULL; the ported entry raises 42883 instead,
// so that error maps back to None and the caller reports the attribute.
fn attribute_notation_func_call<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    name: &'mcx str,
    arg: Node<'mcx>,
    last_srf: Option<Node<'mcx>>,
    location: types_core::ParseLoc,
) -> PgResult<Option<Node<'mcx>>> {
    let funcname = types_nodes::NodeList::make1(mcx, Node::mk_string(mcx, name)?)?;
    let fargs = types_nodes::NodeList::make1(mcx, arg)?;
    let fn_call = types_nodes::rawnodes::FuncCall {
        funcname: types_nodes::NodeList::nil(),
        args: types_nodes::NodeList::nil(),
        agg_order: types_nodes::NodeList::nil(),
        agg_filter: None,
        over: None,
        agg_within_group: false,
        agg_star: false,
        agg_distinct: false,
        func_variadic: false,
        funcformat: types_nodes::CoercionForm::COERCE_EXPLICIT_CALL,
        location,
    };
    match parse_func::ParseFuncOrColumn(
        mcx,
        pstate,
        &funcname,
        fargs,
        &[expr_type(arg)],
        &fn_call,
        None,
        last_srf,
        false,
        true,
        location,
    ) {
        Ok(node) => Ok(Some(node)),
        Err(e) if e.sqlstate() == types_error::ERRCODE_UNDEFINED_FUNCTION => Ok(None),
        Err(e) => Err(e),
    }
}

// transformArrayExpr (parse_expr.c). array_type == InvalidOid means "infer a
// common element type"; a valid array_type (cast push-down) coerces hard.
fn transformArrayExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    a: &types_nodes::A_ArrayExpr<'mcx>,
    mut array_type: types_core::Oid,
    mut element_type: types_core::Oid,
    typmod: i32,
) -> PgResult<Node<'mcx>> {
    use types_core::{InvalidOid, OidIsValid, INT2VECTOROID, OIDVECTOROID};

    let mut newelems: types_nodes::NodeList<'mcx> = types_nodes::NodeList::nil();
    let mut multidims = false;

    for e in a.elements.iter() {
        let newe;
        if e.node_tag() == NodeTag::T_A_ArrayExpr {
            newe = transformArrayExpr(
                mcx,
                pstate,
                e.as_a_array_expr().unwrap(),
                array_type,
                element_type,
                typmod,
            )?;
            multidims = true;
        } else {
            newe = transformExprRecurse(mcx, pstate, e)?;
            if !multidims {
                let newetype = expr_type(newe);
                if newetype != INT2VECTOROID
                    && newetype != OIDVECTOROID
                    && OidIsValid(lsyscache::typ::get_element_type(newetype)?)
                {
                    multidims = true;
                }
            }
        }
        newelems.lappend(mcx, newe)?;
    }

    let coerce_type_id;
    let coerce_hard;
    if OidIsValid(array_type) {
        debug_assert!(OidIsValid(element_type));
        coerce_type_id = if multidims { array_type } else { element_type };
        coerce_hard = true;
    } else {
        if newelems.is_nil() {
            return Err(empty_array_error(pstate, a.location));
        }
        let mut pairs: Vec<(types_core::Oid, i32)> = Vec::with_capacity(newelems.len());
        for e in newelems.iter() {
            pairs.push((expr_type(e), expr_location(e)));
        }
        coerce_type_id = coerce::select_common_type(pstate, &pairs, Some("ARRAY"))?;
        if multidims {
            array_type = coerce_type_id;
            element_type = lsyscache::typ::get_element_type(array_type)?;
            if !OidIsValid(element_type) {
                return Err(array_elem_type_error(pstate, array_type, a.location, false));
            }
        } else {
            element_type = coerce_type_id;
            array_type = lsyscache::typ::get_array_type(element_type)?;
            if !OidIsValid(array_type) {
                return Err(array_elem_type_error(pstate, element_type, a.location, true));
            }
        }
        coerce_hard = false;
    }

    let mut newcoercedelems: types_nodes::NodeList<'mcx> = types_nodes::NodeList::nil();
    for e in newelems.iter() {
        let etype = expr_type(e);
        let newe = if coerce_hard {
            coerce::coerce_to_target_type(
                mcx,
                pstate,
                e,
                etype,
                coerce_type_id,
                typmod,
                coerce::COERCION_EXPLICIT,
                types_nodes::CoercionForm::COERCE_EXPLICIT_CAST,
                -1,
            )?
            .ok_or_else(|| cannot_cast_error(pstate, etype, coerce_type_id, -1, e))?
        } else {
            coerce::coerce_to_common_type(
                mcx,
                pstate,
                e,
                etype,
                expr_location(e),
                coerce_type_id,
                "ARRAY",
            )?
        };
        newcoercedelems.lappend(mcx, newe)?;
    }

    Node::mk(
        mcx,
        types_nodes::ArrayExpr {
            array_typeid: array_type,
            array_collid: types_core::InvalidOid,
            element_typeid: element_type,
            elements: newcoercedelems,
            multidims,
            list_start: a.list_start,
            list_end: a.list_end,
            location: a.location,
        },
    )
}

#[cold]
fn empty_array_error(pstate: &ParseState<'_, '_>, location: i32) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_INDETERMINATE_DATATYPE, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_INDETERMINATE_DATATYPE)
            .errmsg("cannot determine type of empty array".to_string())
            .errhint(
                "Explicitly cast to the desired type, for example ARRAY[]::integer[]."
                    .to_string(),
            )
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformArrayExpr")),
    )
}

#[cold]
fn array_elem_type_error(
    pstate: &ParseState<'_, '_>,
    typ: types_core::Oid,
    location: i32,
    missing_array: bool,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_UNDEFINED_OBJECT, ERROR};
    let t = format_type::format_type_be(typ).unwrap_or_else(|_| typ.to_string());
    let msg = if missing_array {
        format!("could not find array type for data type {t}")
    } else {
        format!("could not find element type for data type {t}")
    };
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(msg)
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformArrayExpr")),
    )
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
    // C typenameTypeIdAndMod has no typtype gate; casts accept composites.
    let (target_type, target_typmod) =
        parse_utilcmd::typenameTypeIdAndModAllowComposite(mcx, Some(pstate), tn)?;

    let arg = tc.arg.expect("TypeCast.arg");
    let arg = if arg.node_tag() == NodeTag::T_A_ArrayExpr {
        let mut target_base_typmod = target_typmod;
        let target_base_type =
            lsyscache::typ::getBaseTypeAndTypmod(target_type, &mut target_base_typmod)?;
        let element_type = lsyscache::typ::get_element_type(target_base_type)?;
        if types_core::OidIsValid(element_type) {
            transformArrayExpr(
                mcx,
                pstate,
                arg.as_a_array_expr().unwrap(),
                target_base_type,
                element_type,
                target_base_typmod,
            )?
        } else {
            transformExprRecurse(mcx, pstate, arg)?
        }
    } else {
        transformExprRecurse(mcx, pstate, arg)?
    };

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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformTypeCast")),
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

    let result = if lexpr.is_some_and(|n| n.node_tag() == NodeTag::T_RowExpr)
        && rexpr.is_some_and(|n| n.node_tag() == NodeTag::T_RowExpr)
    {
        make_row_distinct_op(
            mcx,
            pstate,
            &a.name,
            lexpr.unwrap().as_row_expr().unwrap(),
            rexpr.unwrap().as_row_expr().unwrap(),
            a.location,
        )?
    } else {
        make_distinct_op(mcx, pstate, &a.name, lexpr, rexpr, a.location)?
    };

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

// make_row_distinct_op (parse_expr.c): pairwise IS DISTINCT ORed together;
// zero-length rows fold to constant FALSE.
fn make_row_distinct_op<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    opname: &types_nodes::NodeList<'mcx>,
    lrow: &types_nodes::RowExpr<'mcx>,
    rrow: &types_nodes::RowExpr<'mcx>,
    location: types_core::ParseLoc,
) -> PgResult<Node<'mcx>> {
    if lrow.args.len() != rrow.args.len() {
        return Err(row_length_error(
            pstate,
            types_error::ERRCODE_SYNTAX_ERROR,
            "unequal number of entries in row expressions",
            location,
        ));
    }
    let mut result: Option<Node<'mcx>> = None;
    for (larg, rarg) in lrow.args.iter().zip(rrow.args.iter()) {
        let cmp = make_distinct_op(mcx, pstate, opname, Some(larg), Some(rarg), location)?;
        result = Some(match result {
            None => cmp,
            Some(prev) => Node::mk(
                mcx,
                types_nodes::BoolExpr {
                    boolop: types_nodes::BoolExprType::OR_EXPR,
                    args: types_nodes::NodeList::make2(mcx, prev, cmp)?,
                    location,
                },
            )?,
        });
    }
    match result {
        Some(r) => Ok(r),
        None => Node::mk_const(
            mcx,
            types_core::catalog::BOOLOID,
            -1,
            types_core::InvalidOid,
            1,
            datum::Datum::from_bool(false),
            false,
            true,
        ),
    }
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

fn xml_name_in<'mcx>(
    mcx: Mcx<'mcx>,
    ident: &str,
    fully_escaped: bool,
    escape_period: bool,
) -> PgResult<&'mcx str> {
    let v = adt_xml::map_sql_identifier_to_xml_name(ident.as_bytes(), fully_escaped, escape_period)?;
    let b = mcx::slice_borrow_in(mcx, &v)?;
    Ok(core::str::from_utf8(b).expect("xml name stays server-encoded UTF-8"))
}

fn transformXmlExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    use types_core::catalog::{INT4OID, TEXTOID, XMLOID};
    use types_nodes::primnodes::{XmlExpr, XmlExprOp};

    let x = expr.as_xml_expr().unwrap();
    let name = match x.name {
        Some(n) => Some(xml_name_in(mcx, n, false, false)?),
        None => None,
    };

    let mut named_args = types_nodes::NodeList::nil();
    let mut arg_names = types_nodes::NodeList::nil();
    for r_node in &x.named_args {
        let r = r_node.as_res_target().expect("XML named arg is ResTarget");
        let val = r.val.expect("grammar sets ResTarget.val");
        let e = transformExprRecurse(mcx, pstate, val)?;
        let argname = if let Some(n) = r.name {
            xml_name_in(mcx, n, false, false)?
        } else if let Some(cr) = val.as_column_ref() {
            // FigureColname's ColumnRef arm: last String field.
            let mut fname: Option<&str> = None;
            for f in &cr.fields {
                if let Some(s) = f.as_string() {
                    fname = Some(s.sval);
                }
            }
            xml_name_in(mcx, fname.unwrap_or("?column?"), true, false)?
        } else {
            return Err(xml_syntax_error(
                pstate,
                if x.op == XmlExprOp::IS_XMLELEMENT {
                    "unnamed XML attribute value must be a column reference".to_string()
                } else {
                    "unnamed XML element value must be a column reference".to_string()
                },
                r.location,
            ));
        };
        if x.op == XmlExprOp::IS_XMLELEMENT {
            for prior in &arg_names {
                if prior.as_string().map(|s| s.sval) == Some(argname) {
                    return Err(xml_syntax_error(
                        pstate,
                        format!("XML attribute name \"{argname}\" appears more than once"),
                        r.location,
                    ));
                }
            }
        }
        named_args.lappend(mcx, e)?;
        arg_names.lappend(mcx, Node::mk_string(mcx, argname)?)?;
    }

    let mut args = types_nodes::NodeList::nil();
    for (i, e) in x.args.iter().enumerate() {
        let newe = transformExprRecurse(mcx, pstate, e)?;
        let cst = |newe, target, cname| {
            coerce::coerce_to_specific_type(
                mcx,
                pstate,
                newe,
                expr_type(newe),
                expr_location(newe),
                target,
                cname,
            )
        };
        let newe = match x.op {
            XmlExprOp::IS_XMLCONCAT => cst(newe, XMLOID, "XMLCONCAT")?,
            XmlExprOp::IS_XMLELEMENT => newe,
            XmlExprOp::IS_XMLFOREST => cst(newe, XMLOID, "XMLFOREST")?,
            XmlExprOp::IS_XMLPARSE => {
                if i == 0 {
                    cst(newe, TEXTOID, "XMLPARSE")?
                } else {
                    coerce::coerce_to_boolean(
                        mcx,
                        pstate,
                        newe,
                        expr_type(newe),
                        expr_location(newe),
                        "XMLPARSE",
                    )?
                }
            }
            XmlExprOp::IS_XMLPI => cst(newe, TEXTOID, "XMLPI")?,
            XmlExprOp::IS_XMLROOT => match i {
                0 => cst(newe, XMLOID, "XMLROOT")?,
                1 => cst(newe, TEXTOID, "XMLROOT")?,
                _ => cst(newe, INT4OID, "XMLROOT")?,
            },
            XmlExprOp::IS_XMLSERIALIZE => {
                unreachable!("XMLSERIALIZE goes through transformXmlSerialize")
            }
            XmlExprOp::IS_DOCUMENT => cst(newe, XMLOID, "IS DOCUMENT")?,
        };
        args.lappend(mcx, newe)?;
    }

    Node::mk(
        mcx,
        XmlExpr {
            op: x.op,
            name,
            named_args,
            arg_names,
            args,
            xmloption: x.xmloption,
            indent: false,
            r#type: XMLOID,
            typmod: -1,
            location: x.location,
        },
    )
}

fn transformXmlSerialize<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    use types_core::catalog::{TEXTOID, XMLOID};
    use types_nodes::primnodes::{XmlExpr, XmlExprOp};

    let xs = expr
        .as_variant::<types_nodes::rawnodes::XmlSerialize>()
        .expect("T_XmlSerialize is XmlSerialize");
    let inner = transformExprRecurse(mcx, pstate, xs.expr.expect("grammar sets XmlSerialize.expr"))?;
    let arg = coerce::coerce_to_specific_type(
        mcx,
        pstate,
        inner,
        expr_type(inner),
        expr_location(inner),
        XMLOID,
        "XMLSERIALIZE",
    )?;

    let tn = xs.typeName.and_then(|n| n.as_type_name()).expect("grammar sets XmlSerialize.typeName");
    let (target_type, target_typmod) = parse_utilcmd::typenameTypeIdAndMod(mcx, Some(pstate), tn)?;

    let xexpr = Node::mk(
        mcx,
        XmlExpr {
            op: XmlExprOp::IS_XMLSERIALIZE,
            name: None,
            named_args: types_nodes::NodeList::nil(),
            arg_names: types_nodes::NodeList::nil(),
            args: types_nodes::NodeList::make1(mcx, arg)?,
            xmloption: xs.xmloption,
            indent: xs.indent,
            r#type: target_type,
            typmod: target_typmod,
            location: xs.location,
        },
    )?;

    match coerce::coerce_to_target_type(
        mcx,
        pstate,
        xexpr,
        TEXTOID,
        target_type,
        target_typmod,
        coerce::COERCION_IMPLICIT,
        types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )? {
        Some(r) => Ok(r),
        None => Err(cannot_cast_xmlserialize(pstate, target_type, xs.location)?),
    }
}

#[cold]
fn xml_syntax_error(
    pstate: &ParseState<'_, '_>,
    msg: std::string::String,
    location: i32,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(msg)
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformXmlExpr")),
    )
}

#[cold]
fn cannot_cast_xmlserialize(
    pstate: &ParseState<'_, '_>,
    target_type: types_core::Oid,
    location: i32,
) -> PgResult<Box<types_error::PgError>> {
    use types_error::{ErrorLocation, ERRCODE_CANNOT_COERCE, ERROR};
    Ok(Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_CANNOT_COERCE)
            .errmsg(format!(
                "cannot cast XMLSERIALIZE result to {}",
                format_type::format_type_be(target_type)?
            ))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformXmlSerialize")),
    ))
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
    allow_default: bool,
) -> PgResult<Node<'mcx>> {
    let r = expr.as_row_expr().unwrap();
    // C transformExpressionList (via parse_target's seam): per-item
    // transformExpr at the current p_expr_kind, `x.*` items expanded;
    // allowDefault keeps SetToDefault items untransformed (multiassign
    // UPDATE SET (a, b) = ROW(...)).
    let kind = pstate.p_expr_kind;
    let args = parse_func_seams::transformExpressionList::call(
        mcx,
        pstate,
        &r.args,
        kind,
        allow_default,
    )?;
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

// transformMultiAssignRef (parse_expr.c). C mutates the raw SubLink's
// subLinkType/subLinkId in place; sealed nodes rebuild the wrapper instead.
fn transformMultiAssignRef<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    use types_nodes::SubLinkType;

    let maref = expr.as_multi_assign_ref().unwrap();
    debug_assert!(pstate.p_expr_kind == parser_small1::ParseExprKind::EXPR_KIND_UPDATE_SOURCE);
    let source = maref.source.expect("MultiAssignRef.source");

    if maref.colno == 1 {
        let tle_expr = if let Some(s) = source
            .as_sub_link()
            .filter(|s| s.subLinkType == SubLinkType::EXPR_SUBLINK)
        {
            let relabeled = Node::mk(
                mcx,
                types_nodes::SubLink {
                    subLinkType: SubLinkType::MULTIEXPR_SUBLINK,
                    subLinkId: s.subLinkId,
                    testexpr: s.testexpr,
                    operName: s.operName.clone_in(mcx)?,
                    subselect: s.subselect,
                    location: s.location,
                },
            )?;
            let transformed = transformExprRecurse(mcx, pstate, relabeled)?;
            let sl = transformed.as_sub_link().expect("transformSubLink yields a SubLink");
            let qtree = sl.subselect.as_query().expect("SubLink.subselect is a Query");
            let nonjunk = qtree
                .targetList
                .iter()
                .filter(|te| !te.as_target_entry().expect("tlist entry").resjunk)
                .count();
            if nonjunk != maref.ncolumns as usize {
                return Err(column_value_count_mismatch(pstate, sl.location));
            }
            // subLinkId = the SubLink's position in p_multiassign_exprs.
            Node::mk(
                mcx,
                types_nodes::SubLink {
                    subLinkType: sl.subLinkType,
                    subLinkId: pstate.p_multiassign_exprs.len() as i32 + 1,
                    testexpr: sl.testexpr,
                    operName: sl.operName.clone_in(mcx)?,
                    subselect: sl.subselect,
                    location: sl.location,
                },
            )?
        } else if source.node_tag() == NodeTag::T_RowExpr {
            let rexpr = transformRowExpr(mcx, pstate, source, true)?;
            let nargs = rexpr.as_row_expr().expect("transformed RowExpr").args.len();
            if nargs != maref.ncolumns as usize {
                return Err(column_value_count_mismatch(pstate, expr_location(rexpr)));
            }
            rexpr
        } else {
            return Err(multiassign_source_not_supported(pstate, expr_location(source)));
        };
        let tle = Node::mk(
            mcx,
            types_nodes::TargetEntry {
                expr: tle_expr,
                resno: 0,
                resname: None,
                ressortgroupref: 0,
                resorigtbl: types_core::InvalidOid,
                resorigcol: 0,
                resjunk: true,
            },
        )?;
        pstate.p_multiassign_exprs.lappend(mcx, tle)?;
    }

    let tle = pstate
        .p_multiassign_exprs
        .last()
        .expect("transformMultiAssignRef with empty p_multiassign_exprs")
        .as_target_entry()
        .expect("tlist entry");

    if let Some(sl) = tle.expr.as_sub_link() {
        debug_assert!(sl.subLinkType == SubLinkType::MULTIEXPR_SUBLINK);
        let qtree = sl.subselect.as_query().expect("SubLink.subselect is a Query");
        let coltle = qtree
            .targetList
            .nth(maref.colno as usize - 1)
            .as_target_entry()
            .expect("tlist entry");
        debug_assert!(!coltle.resjunk);
        return Node::mk(
            mcx,
            types_nodes::Param {
                paramkind: types_nodes::ParamKind::PARAM_MULTIEXPR,
                paramid: (sl.subLinkId << 16) | maref.colno,
                paramtype: expr_type(coltle.expr),
                paramtypmod: expr_typmod(coltle.expr),
                paramcollid: expr_collation(coltle.expr),
                location: expr_location(coltle.expr),
            },
        );
    }
    let r = tle.expr.as_row_expr().expect("unexpected expr type in multiassign list");
    let result = r.args.nth(maref.colno as usize - 1);
    if maref.colno == maref.ncolumns {
        let n = pstate.p_multiassign_exprs.len();
        pstate.p_multiassign_exprs.truncate(n - 1);
    }
    Ok(result)
}

#[cold]
fn column_value_count_mismatch(
    pstate: &ParseState<'_, '_>,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("number of columns does not match number of values".to_string())
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformMultiAssignRef")),
    )
}

#[cold]
fn multiassign_source_not_supported(
    pstate: &ParseState<'_, '_>,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(
                "source for a multiple-column UPDATE item must be a sub-SELECT or ROW() \
                 expression"
                    .to_string(),
            )
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformMultiAssignRef")),
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "make_distinct_op")),
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformCollateClause")),
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformRowExpr")),
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
                .with_error_location(ErrorLocation::new(file!(), line!() as i32, "typmod_check")),
        ));
    }
    if typmod > MAX_PRECISION {
        elog::ereport(WARNING)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "{what}({typmod}){tz} precision reduced to maximum allowed, {MAX_PRECISION}"
            ))
            .finish(ErrorLocation::new(file!(), line!() as i32, "typmod_check"))?;
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformCaseExpr")),
    )
}

// C transformWholeRowRef (parse_expr.c); rowtype resolution lives in
// nodes_core::makefuncs::make_whole_row_var. The JOIN USING alias RowExpr
// expansion remains loud.
fn transformWholeRowRef<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    nsitem: &parser_small1::ParseNamespaceItem<'mcx>,
    sublevels_up: i32,
    location: types_core::ParseLoc,
) -> PgResult<Node<'mcx>> {
    use types_nodes::primnodes::VarReturningType;

    let rte = nsitem.rte();
    let is_eref = match rte.eref {
        Some(eref) => core::ptr::eq(nsitem.p_names, eref),
        None => false,
    };
    if !(is_eref || nsitem.p_returning_type != VarReturningType::VAR_RETURNING_DEFAULT) {
        // A JOIN USING alias exposes only the merged columns, so a whole-row
        // Var cannot represent it; expand to a RowExpr immediately.
        let (_, fields) = parse_relation::expandRTE(
            mcx,
            rte,
            nsitem.p_rtindex,
            sublevels_up,
            nsitem.p_returning_type,
            location,
            false,
        )?;
        let mut args = fields;
        args.truncate(nsitem.p_names.colnames.len());
        return Node::mk(
            mcx,
            types_nodes::RowExpr {
                args,
                row_typeid: types_core::catalog::RECORDOID,
                row_format: types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
                colnames: nsitem.p_names.colnames.clone_in(mcx)?,
                location,
            },
        );
    }
    let mut var = nodes_core::makefuncs::make_whole_row_var(
        mcx,
        rte,
        nsitem.p_rtindex as types_core::Index,
        sublevels_up as types_core::Index,
        true,
    )?;
    var.varreturningtype = nsitem.p_returning_type;
    var.location = location;
    parse_relation::markNullableIfNeeded(mcx, pstate, &mut var)?;
    parse_relation::markVarForSelectPriv(mcx, pstate, &var)?;
    Node::mk(mcx, var)
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
    let mut crerr = ColumnRefErr::NoColumn;
    let last_srf = pstate.p_last_srf;

    let node: Option<Node<'mcx>> = 'resolve: {
        match fields {
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
                // C checks the catalog name against the current database and
                // then ignores it.
                let catname = field_str(fields[0]).expect("catalog qualifier is a String");
                let dbname =
                    dbcommands_seams::get_database_name::call(init_small::globals::MyDatabaseId())?;
                if dbname.as_deref() != Some(catname) {
                    crerr = ColumnRefErr::WrongDb;
                    break 'resolve None;
                }
                nspname = Some(field_str(fields[1]).expect("qualifier is a String"));
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
                            // Not a column; C tries a function call on the
                            // whole row (attribute notation).
                            let wholerow = transformWholeRowRef(
                                mcx,
                                pstate,
                                nsitem,
                                levels_up,
                                cref.location,
                            )?;
                            attribute_notation_func_call(
                                mcx,
                                pstate,
                                name,
                                wholerow,
                                last_srf,
                                cref.location,
                            )?
                        }
                    }
                }
            }
        }
        _ => {
            crerr = ColumnRefErr::TooMany;
            None
        }
    }
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
            match crerr {
                ColumnRefErr::NoColumn => {}
                ColumnRefErr::WrongDb => {
                    return Err(improper_qualified_name(
                        pstate,
                        types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                        "cross-database references are not implemented",
                        fields,
                        cref.location,
                    ))
                }
                ColumnRefErr::TooMany => {
                    return Err(improper_qualified_name(
                        pstate,
                        types_error::ERRCODE_SYNTAX_ERROR,
                        "improper qualified name (too many dotted names)",
                        fields,
                        cref.location,
                    ))
                }
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


// C crerr (transformColumnRef): which flavor of not-found to report if no
// hook resolves the reference.
enum ColumnRefErr {
    NoColumn,
    WrongDb,
    TooMany,
}

#[cold]
fn improper_qualified_name(
    pstate: &ParseState<'_, '_>,
    code: types_error::SqlState,
    msg: &str,
    fields: &[Node<'_>],
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERROR};
    let mut name = String::new();
    for (i, f) in fields.iter().enumerate() {
        if i > 0 {
            name.push('.');
        }
        name.push_str(f.as_string().map_or("*", |s| s.sval));
    }
    Box::new(
        elog::ereport(ERROR)
            .errcode(code)
            .errmsg(format!("{msg}: {name}"))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformColumnRef")),
    )
}

// resolve_column_ref marshal: ColumnRef fields to &str names. A trailing
// A_Star is a whole-row record reference (pl_comp.c:1131-1163: A.* / A.B.*
// match only NSTYPE_REC, never scalars); a non-trailing A_Star cannot be a
// plpgsql name.
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
    let n = fields.len();
    if fields[n - 1].node_tag() == NodeTag::T_A_Star {
        // Whole-row arms (pl_comp.c:1131-1163): "*" blocks scalar matches
        // (keeps the valueless-rec 55000 arm), then a rec-gated prefix
        // lookup returns the whole-row Param (parse_target precedent).
        if n < 2 {
            return Ok(None);
        }
        for (i, f) in fields[..n - 1].iter().enumerate() {
            match f.as_string() {
                Some(s) => names[i] = s.sval,
                None => return Ok(None),
            }
        }
        names[n - 1] = "*";
        if let Some(node) = parser_small1::plpgsql_resolve_column_ref(
            mcx,
            pstate,
            st,
            &names[..n],
            location,
            false,
            mbutils::GetDatabaseEncoding(),
        )? {
            return Ok(Some(node));
        }
        let prefix = names[..n - 1].join(".").to_ascii_lowercase();
        if !st.recs.iter().any(|r| *r == prefix) {
            return Ok(None);
        }
        return parser_small1::plpgsql_resolve_column_ref(
            mcx,
            pstate,
            st,
            &names[..n - 1],
            location,
            false,
            mbutils::GetDatabaseEncoding(),
        );
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
pub fn sql_fn_post_column_ref<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    fields: &[Node<'mcx>],
    location: types_core::ParseLoc,
) -> PgResult<Option<Node<'mcx>>> {
    let Some(state) = pstate.p_ref_hook_state.as_sql_fn_params().cloned() else {
        return Ok(None);
    };
    let mut nnames = fields.len();
    if nnames == 0 || nnames > 3 {
        return Ok(None);
    }
    // A trailing star is ignored: the caller expands the whole-row reference.
    if fields[nnames - 1].node_tag() == NodeTag::T_A_Star {
        nnames -= 1;
        if nnames == 0 {
            return Ok(None);
        }
    }
    let name = |i: usize| fields[i].as_string().map(|s| s.sval);
    let resolve = |i: usize| {
        name(i).and_then(|n| parser_small1::sql_fn_resolve_param_name(&state, n))
    };
    let (param, subfield) = match nnames {
        1 => (resolve(0), None),
        2 => {
            if name(0) == Some(state.fname) {
                match resolve(1) {
                    Some(p) => (Some(p), None),
                    None => (resolve(0), name(1)),
                }
            } else {
                (resolve(0), name(1))
            }
        }
        _ => {
            if name(0) != Some(state.fname) {
                return Ok(None);
            }
            (resolve(1), name(2))
        }
    };
    let Some((paramno, ptype)) = param else { return Ok(None) };
    let param = parser_small1::sql_fn_make_param(mcx, &state, paramno, ptype, location)?;
    let Some(subfield) = subfield else { return Ok(Some(param)) };
    // C routes through ParseFuncOrColumn(fn = NULL): composite projection,
    // else attribute-notation function call (p.upper => upper(p)), else
    // NULL — a None falls back to the caller's column-not-found report.
    match parse_func::ParseComplexProjection(mcx, pstate, subfield, param, location)? {
        Some(node) => Ok(Some(node)),
        None => {
            let last_srf = pstate.p_last_srf;
            attribute_notation_func_call(mcx, pstate, subfield, param, last_srf, location)
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformColumnRef")),
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformParamRef")),
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
        // Same as EXPR, except no restriction on number of columns.
        SubLinkType::MULTIEXPR_SUBLINK => (None, types_nodes::NodeList::nil()),
        SubLinkType::EXPR_SUBLINK | SubLinkType::ARRAY_SUBLINK => {
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
        SubLinkType::ANY_SUBLINK | SubLinkType::ALL_SUBLINK | SubLinkType::ROWCOMPARE_SUBLINK => {
            let oper_name = if sublink.operName.is_nil() {
                types_nodes::NodeList::make1(mcx, Node::mk_string(mcx, "=")?)?
            } else {
                sublink.operName.clone_in(mcx)?
            };
            let lefthand = transformExprRecurse(
                mcx,
                pstate,
                sublink.testexpr.expect("ANY/ALL/ROWCOMPARE sublink carries a testexpr"),
            )?;
            let left_list = match lefthand.as_row_expr() {
                Some(r) => r.args.clone_in(mcx)?,
                None => types_nodes::NodeList::make1(mcx, lefthand)?,
            };
            let mut right_list = types_nodes::NodeList::nil();
            for te_node in &qtree.targetList {
                let tent = te_node.as_target_entry().expect("tlist entry");
                if tent.resjunk {
                    continue;
                }
                right_list.lappend(
                    mcx,
                    Node::mk(
                        mcx,
                        types_nodes::Param {
                            paramkind: types_nodes::ParamKind::PARAM_SUBLINK,
                            paramid: tent.resno as i32,
                            paramtype: expr_type(tent.expr),
                            paramtypmod: expr_typmod(tent.expr),
                            paramcollid: expr_collation(tent.expr),
                            location: -1,
                        },
                    )?,
                )?;
            }
            if left_list.len() < right_list.len() {
                return Err(column_count_mismatch(
                    pstate,
                    "subquery has too many columns",
                    sublink.location,
                ));
            }
            if left_list.len() > right_list.len() {
                return Err(column_count_mismatch(
                    pstate,
                    "subquery has too few columns",
                    sublink.location,
                ));
            }
            let test = make_row_comparison_op_lists(
                mcx,
                pstate,
                &oper_name,
                &left_list,
                &right_list,
                sublink.location,
            )?;
            (Some(test), oper_name)
        }
        SubLinkType::CTE_SUBLINK => {
            unreachable!("CTE_SUBLINK is built in parse_cte, never raw-parsed")
        }
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
// to OR, ordered comparisons to RowCompareExpr.
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
    let mut opinfo_lists = mcx::PgVec::new_in(mcx);
    let mut common: Option<u64> = None;
    for cmp in &opexprs {
        let opno = cmp.as_op_expr().expect("make_op returns an OpExpr").opno;
        let interps = lsyscache::get_op_index_interpretation(mcx, opno)?;
        let mut mask: u64 = 0;
        for it in interps.iter() {
            mask |= 1u64 << (it.cmptype as u32);
        }
        opinfo_lists.push(interps);
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

    let mut opfamilies = types_nodes::list::OidList::nil();
    for interps in &opinfo_lists {
        let opfamily = interps
            .iter()
            .find(|it| it.cmptype == cmptype)
            .map(|it| it.opfamily_id)
            .ok_or_else(|| row_comparison_ambiguous(pstate, opname, location))?;
        opfamilies.lappend(mcx, opfamily)?;
    }

    // C rebuilds largs/rargs from the OpExprs: make_op may have inserted
    // coercions.
    let mut opnos = types_nodes::list::OidList::nil();
    let mut new_largs = types_nodes::NodeList::nil();
    let mut new_rargs = types_nodes::NodeList::nil();
    for cmp in &opexprs {
        let op = cmp.as_op_expr().expect("make_op returns an OpExpr");
        opnos.lappend(mcx, op.opno)?;
        new_largs.lappend(mcx, op.args.nth(0))?;
        new_rargs.lappend(mcx, op.args.nth(1))?;
    }

    Node::mk(
        mcx,
        types_nodes::RowCompareExpr {
            cmptype,
            opnos,
            opfamilies,
            // assign_expr_collations fills inputcollids.
            inputcollids: types_nodes::list::OidList::nil(),
            largs: new_largs,
            rargs: new_rargs,
        },
    )
}

#[cold]
fn row_comparison_ambiguous(
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
            .errdetail("There are multiple equally-plausible candidates.".to_string())
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "make_row_comparison_op")),
    )
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "make_row_comparison_op")),
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "make_row_comparison_op")),
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformExprRecurse")),
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformSubLink")),
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "make_row_comparison_op")),
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "make_row_comparison_op")),
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformSubLink")),
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformSubLink")),
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
    // C: WITHIN GROUP ORDER BY expressions become trailing arguments for
    // function lookup and coercion (ParseFuncOrColumn splits them back out).
    if fc.agg_within_group {
        debug_assert!(!fc.agg_order.is_nil());
        for sb_node in &fc.agg_order {
            let sortby = sb_node.as_sort_by().expect("agg_order holds SortBy nodes");
            let arg = sortby.node.expect("SortBy.node is never NULL");
            let t = transformExpr(mcx, pstate, arg, ParseExprKind::EXPR_KIND_ORDER_BY)?;
            fargs.lappend(mcx, t)?;
        }
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
        false,
        false,
        fc.location,
    )
}
