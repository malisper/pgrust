use super::agg_output::bind_agg_output_expr;
use super::functions::{resolve_scalar_function, validate_scalar_function_arity};
use super::infer::{infer_array_literal_type, infer_sql_expr_type};
use super::*;

pub(super) fn bind_grouped_concat_expr(
    left: &SqlExpr,
    right: &SqlExpr,
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(AggFunc, Vec<SqlFunctionArg>, bool, bool)],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    let left_expr = bind_agg_output_expr(
        left,
        group_by_exprs,
        input_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        agg_list,
        n_keys,
    )?;
    let right_expr = bind_agg_output_expr(
        right,
        group_by_exprs,
        input_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        agg_list,
        n_keys,
    )?;
    let left_type = infer_sql_expr_type(left, input_scope, catalog, outer_scopes, grouped_outer);
    let right_type = infer_sql_expr_type(right, input_scope, catalog, outer_scopes, grouped_outer);
    bind_concat_operands(left, left_type, left_expr, right, right_type, right_expr)
}

pub(super) fn bind_grouped_scalar_subquery(
    select: &SelectStatement,
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
) -> Result<Expr, ParseError> {
    let plan =
        build_grouped_subquery_plan(select, group_by_exprs, input_scope, catalog, outer_scopes)?;
    ensure_single_column_subquery(&plan)?;
    Ok(Expr::ScalarSubquery(Box::new(plan)))
}

pub(super) fn bind_grouped_exists_subquery(
    select: &SelectStatement,
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
) -> Result<Expr, ParseError> {
    Ok(Expr::ExistsSubquery(Box::new(build_grouped_subquery_plan(
        select,
        group_by_exprs,
        input_scope,
        catalog,
        outer_scopes,
    )?)))
}

pub(super) fn bind_grouped_in_subquery(
    expr: &SqlExpr,
    subquery: &SelectStatement,
    negated: bool,
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(AggFunc, Vec<SqlFunctionArg>, bool, bool)],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    let subquery_plan =
        build_grouped_subquery_plan(subquery, group_by_exprs, input_scope, catalog, outer_scopes)?;
    ensure_single_column_subquery(&subquery_plan)?;
    let any = Expr::AnySubquery {
        left: Box::new(bind_agg_output_expr(
            expr,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?),
        op: SubqueryComparisonOp::Eq,
        subquery: Box::new(subquery_plan),
    };
    if negated {
        Ok(Expr::Not(Box::new(any)))
    } else {
        Ok(any)
    }
}

pub(super) fn bind_grouped_quantified_subquery(
    left: &SqlExpr,
    op: SubqueryComparisonOp,
    is_all: bool,
    subquery: &SelectStatement,
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(AggFunc, Vec<SqlFunctionArg>, bool, bool)],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    let subquery_plan =
        build_grouped_subquery_plan(subquery, group_by_exprs, input_scope, catalog, outer_scopes)?;
    ensure_single_column_subquery(&subquery_plan)?;
    let left = Box::new(bind_agg_output_expr(
        left,
        group_by_exprs,
        input_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        agg_list,
        n_keys,
    )?);
    if is_all {
        Ok(Expr::AllSubquery {
            left,
            op,
            subquery: Box::new(subquery_plan),
        })
    } else {
        Ok(Expr::AnySubquery {
            left,
            op,
            subquery: Box::new(subquery_plan),
        })
    }
}

pub(super) fn bind_grouped_quantified_array(
    left: &SqlExpr,
    op: SubqueryComparisonOp,
    is_all: bool,
    array: &SqlExpr,
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(AggFunc, Vec<SqlFunctionArg>, bool, bool)],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    let left = Box::new(bind_agg_output_expr(
        left,
        group_by_exprs,
        input_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        agg_list,
        n_keys,
    )?);
    let right = Box::new(bind_agg_output_expr(
        array,
        group_by_exprs,
        input_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        agg_list,
        n_keys,
    )?);
    if is_all {
        Ok(Expr::AllArray { left, op, right })
    } else {
        Ok(Expr::AnyArray { left, op, right })
    }
}

pub(super) fn bind_grouped_func_call(
    name: &str,
    args: &[SqlFunctionArg],
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(AggFunc, Vec<SqlFunctionArg>, bool, bool)],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    if name.eq_ignore_ascii_case("coalesce") {
        return bind_grouped_coalesce_call(
            args,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        );
    }
    let func = resolve_scalar_function(name).ok_or_else(|| ParseError::UnexpectedToken {
        expected: "supported builtin function",
        actual: name.to_string(),
    })?;
    let lowered_args = lower_named_scalar_function_args(func, args)?;
    validate_scalar_function_arity(func, &lowered_args)?;
    let bound_args = lowered_args
        .iter()
        .map(|arg| {
            bind_agg_output_expr(
                arg,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    match func {
        BuiltinScalarFunction::Left | BuiltinScalarFunction::Repeat => {
            let left_type = infer_sql_expr_type(
                &lowered_args[0],
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
            );
            let right_type = infer_sql_expr_type(
                &lowered_args[1],
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
            );
            if !should_use_text_concat(&lowered_args[0], left_type, &lowered_args[0], left_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "text argument",
                    actual: format!("{func:?}({})", sql_type_name(left_type)),
                });
            }
            if !is_numeric_family(right_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "integer argument",
                    actual: format!("{func:?}({})", sql_type_name(right_type)),
                });
            }
            Ok(Expr::FuncCall {
                func_oid: 0,
                func,
                args: vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        left_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        right_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ],
                func_variadic: false,
            })
        }
        BuiltinScalarFunction::Lower => {
            let arg_type = infer_sql_expr_type(
                &lowered_args[0],
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
            );
            Ok(Expr::FuncCall {
                func_oid: 0,
                func,
                args: vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Text),
                )],
                func_variadic: false,
            })
        }
        _ => Ok(Expr::FuncCall {
            func_oid: 0,
            func,
            args: bound_args,
            func_variadic: false,
        }),
    }
}

fn bind_grouped_coalesce_call(
    args: &[SqlFunctionArg],
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(AggFunc, Vec<SqlFunctionArg>, bool)],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    if args.iter().any(|arg| arg.name.is_some()) {
        return Err(ParseError::UnexpectedToken {
            expected: "positional COALESCE arguments",
            actual: "COALESCE with named arguments".into(),
        });
    }
    if args.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "at least one COALESCE argument",
            actual: format!("COALESCE({} args)", args.len()),
        });
    }
    let lowered_args = args.iter().map(|arg| arg.value.clone()).collect::<Vec<_>>();
    let common_type = infer_common_scalar_expr_type_with_ctes(
        &lowered_args,
        input_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        &[],
        "COALESCE arguments with a common type",
    )?;
    let mut bound_args = Vec::with_capacity(lowered_args.len());
    for arg in &lowered_args {
        let arg_type = infer_sql_expr_type(arg, input_scope, catalog, outer_scopes, grouped_outer);
        let bound = bind_agg_output_expr(
            arg,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?;
        bound_args.push(coerce_bound_expr(bound, arg_type, common_type));
    }
    let mut iter = bound_args.into_iter().rev();
    let mut expr = iter.next().expect("coalesce arity validated");
    for arg in iter {
        expr = Expr::Coalesce(Box::new(arg), Box::new(expr));
    }
    Ok(expr)
}

pub(super) fn bind_grouped_array_literal(
    elements: &[SqlExpr],
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(AggFunc, Vec<SqlFunctionArg>, bool, bool)],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    Ok(Expr::ArrayLiteral {
        elements: elements
            .iter()
            .map(|element| {
                bind_agg_output_expr(
                    element,
                    group_by_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )
            })
            .collect::<Result<_, _>>()?,
        array_type: infer_array_literal_type(
            elements,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
    })
}

fn build_grouped_subquery_plan(
    select: &SelectStatement,
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
) -> Result<Plan, ParseError> {
    let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
    child_outer.push(input_scope.clone());
    child_outer.extend_from_slice(outer_scopes);
    build_plan_with_outer(
        select,
        catalog,
        &child_outer,
        Some(GroupedOuterScope {
            scope: input_scope.clone(),
            group_by_exprs: group_by_exprs.to_vec(),
        }),
        &[],
    )
}
