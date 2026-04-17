use super::agg_output::bind_agg_output_expr;
use super::functions::{resolve_scalar_function, validate_scalar_function_arity};
use super::infer::{infer_array_literal_type, infer_sql_expr_type};
use super::*;
use crate::include::nodes::primnodes::{SubLink, SubLinkType};

pub(super) fn bind_grouped_concat_expr(
    left: &SqlExpr,
    right: &SqlExpr,
    group_by_exprs: &[SqlExpr],
    group_key_exprs: &[Expr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(AggFunc, Vec<SqlFunctionArg>, bool, bool, Option<SqlExpr>)],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    let left_expr = bind_agg_output_expr(
        left,
        group_by_exprs,
        group_key_exprs,
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
        group_key_exprs,
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
    let query =
        build_grouped_subquery_plan(select, group_by_exprs, input_scope, catalog, outer_scopes)?;
    ensure_single_column_subquery(query.columns().len())?;
    Ok(Expr::SubLink(Box::new(SubLink {
        sublink_type: SubLinkType::ExprSubLink,
        testexpr: None,
        subselect: Box::new(query),
    })))
}

pub(super) fn bind_grouped_exists_subquery(
    select: &SelectStatement,
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
) -> Result<Expr, ParseError> {
    Ok(Expr::SubLink(Box::new(SubLink {
        sublink_type: SubLinkType::ExistsSubLink,
        testexpr: None,
        subselect: Box::new(build_grouped_subquery_plan(
            select,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
        )?),
    })))
}

pub(super) fn bind_grouped_in_subquery(
    expr: &SqlExpr,
    subquery: &SelectStatement,
    negated: bool,
    group_by_exprs: &[SqlExpr],
    group_key_exprs: &[Expr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(AggFunc, Vec<SqlFunctionArg>, bool, bool, Option<SqlExpr>)],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    let subquery =
        build_grouped_subquery_plan(subquery, group_by_exprs, input_scope, catalog, outer_scopes)?;
    ensure_single_column_subquery(subquery.columns().len())?;
    let any = Expr::SubLink(Box::new(SubLink {
        sublink_type: SubLinkType::AnySubLink(SubqueryComparisonOp::Eq),
        testexpr: Some(Box::new(bind_agg_output_expr(
            expr,
            group_by_exprs,
            group_key_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?)),
        subselect: Box::new(subquery),
    }));
    if negated {
        Ok(Expr::bool_expr(
            crate::include::nodes::primnodes::BoolExprType::Not,
            vec![any],
        ))
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
    group_key_exprs: &[Expr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(AggFunc, Vec<SqlFunctionArg>, bool, bool, Option<SqlExpr>)],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    let subquery =
        build_grouped_subquery_plan(subquery, group_by_exprs, input_scope, catalog, outer_scopes)?;
    ensure_single_column_subquery(subquery.columns().len())?;
    let left = Box::new(bind_agg_output_expr(
        left,
        group_by_exprs,
        group_key_exprs,
        input_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        agg_list,
        n_keys,
    )?);
    Ok(Expr::SubLink(Box::new(SubLink {
        sublink_type: if is_all {
            SubLinkType::AllSubLink(op)
        } else {
            SubLinkType::AnySubLink(op)
        },
        testexpr: Some(left),
        subselect: Box::new(subquery),
    })))
}

pub(super) fn bind_grouped_quantified_array(
    left: &SqlExpr,
    op: SubqueryComparisonOp,
    is_all: bool,
    array: &SqlExpr,
    group_by_exprs: &[SqlExpr],
    group_key_exprs: &[Expr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(AggFunc, Vec<SqlFunctionArg>, bool, bool, Option<SqlExpr>)],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    let raw_left_type =
        infer_sql_expr_type_with_ctes(left, input_scope, catalog, outer_scopes, grouped_outer, &[]);
    let raw_array_type = infer_sql_expr_type_with_ctes(
        array,
        input_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        &[],
    );
    let left_type =
        coerce_unknown_string_literal_type(left, raw_left_type, raw_array_type.element_type());
    let array_type = if raw_array_type.is_array {
        coerce_unknown_string_literal_type(array, raw_array_type, raw_left_type)
    } else {
        SqlType::array_of(left_type.element_type())
    };
    let left = coerce_bound_expr(
        bind_agg_output_expr(
            left,
            group_by_exprs,
            group_key_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?,
        raw_left_type,
        left_type,
    );
    let right = coerce_bound_expr(
        bind_agg_output_expr(
            array,
            group_by_exprs,
            group_key_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?,
        raw_array_type,
        array_type,
    );
    Ok(Expr::scalar_array_op(op, !is_all, left, right))
}

pub(super) fn bind_grouped_func_call(
    name: &str,
    args: &[SqlFunctionArg],
    group_by_exprs: &[SqlExpr],
    group_key_exprs: &[Expr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(AggFunc, Vec<SqlFunctionArg>, bool, bool, Option<SqlExpr>)],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    if name.eq_ignore_ascii_case("coalesce") {
        return bind_grouped_coalesce_call(
            args,
            group_by_exprs,
            group_key_exprs,
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
                group_key_exprs,
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
            Ok(Expr::builtin_func(
                func,
                Some(SqlType::new(SqlTypeKind::Text)),
                false,
                vec![
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
            ))
        }
        BuiltinScalarFunction::Lower => {
            let arg_type = infer_sql_expr_type(
                &lowered_args[0],
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
            );
            Ok(Expr::builtin_func(
                func,
                Some(SqlType::new(SqlTypeKind::Text)),
                false,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Text),
                )],
            ))
        }
        _ => Ok(Expr::builtin_func(func, None, false, bound_args)),
    }
}

fn bind_grouped_coalesce_call(
    args: &[SqlFunctionArg],
    group_by_exprs: &[SqlExpr],
    group_key_exprs: &[Expr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(AggFunc, Vec<SqlFunctionArg>, bool, bool, Option<SqlExpr>)],
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
            group_key_exprs,
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
    group_key_exprs: &[Expr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(AggFunc, Vec<SqlFunctionArg>, bool, bool, Option<SqlExpr>)],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    Ok(Expr::ArrayLiteral {
        elements: elements
            .iter()
            .map(|element| {
                bind_agg_output_expr(
                    element,
                    group_by_exprs,
                    group_key_exprs,
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
) -> Result<Query, ParseError> {
    let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
    child_outer.push(input_scope.clone());
    child_outer.extend_from_slice(outer_scopes);
    let (query, _) = analyze_select_query_with_outer(
        select,
        catalog,
        &child_outer,
        Some(GroupedOuterScope {
            scope: input_scope.clone(),
            group_by_exprs: group_by_exprs.to_vec(),
        }),
        &[],
        &[],
    )?;
    Ok(query)
}
