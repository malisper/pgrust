use super::agg_output::{
    bind_agg_output_expr, current_grouped_agg_visible_ctes, grouped_infer_common_scalar_expr_type,
    grouped_infer_sql_expr_type,
};
use super::expr::{
    bind_user_defined_scalar_function_call_from_resolved_typed_args,
    catalog_backed_explicit_cast_allowed, exists_subquery_query,
};
use super::functions::{
    fixed_scalar_return_type, resolve_function_cast_type, resolve_scalar_function,
    validate_scalar_function_arity,
};
use super::infer::infer_array_literal_type_with_ctes;
use super::*;
use pgrust_nodes::primnodes::{SubLink, SubLinkType, expr_sql_type_hint};

pub(super) fn bind_grouped_concat_expr(
    left: &SqlExpr,
    right: &SqlExpr,
    group_by_exprs: &[SqlExpr],
    group_key_exprs: &[Expr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[CollectedAggregate],
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
    let left_type =
        grouped_infer_sql_expr_type(left, input_scope, catalog, outer_scopes, grouped_outer);
    let right_type =
        grouped_infer_sql_expr_type(right, input_scope, catalog, outer_scopes, grouped_outer);
    bind_concat_operands(left, left_type, left_expr, right, right_type, right_expr)
}

pub(super) fn bind_grouped_scalar_subquery(
    select: &SelectStatement,
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
) -> Result<Expr, ParseError> {
    bind_grouped_single_column_sublink(
        select,
        SubLinkType::ExprSubLink,
        group_by_exprs,
        input_scope,
        catalog,
        outer_scopes,
    )
}

pub(super) fn bind_grouped_array_subquery(
    select: &SelectStatement,
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
) -> Result<Expr, ParseError> {
    bind_grouped_single_column_sublink(
        select,
        SubLinkType::ArraySubLink,
        group_by_exprs,
        input_scope,
        catalog,
        outer_scopes,
    )
}

fn bind_grouped_single_column_sublink(
    select: &SelectStatement,
    sublink_type: SubLinkType,
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
) -> Result<Expr, ParseError> {
    let query =
        build_grouped_subquery_plan(select, group_by_exprs, input_scope, catalog, outer_scopes)?;
    ensure_single_column_subquery(query.columns().len())?;
    Ok(Expr::SubLink(Box::new(SubLink {
        sublink_type,
        testexpr: None,
        comparison: None,
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
        comparison: None,
        subselect: Box::new(exists_subquery_query(build_grouped_subquery_plan(
            select,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
        )?)),
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
    agg_list: &[CollectedAggregate],
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
        comparison: None,
        subselect: Box::new(subquery),
    }));
    if negated {
        Ok(Expr::bool_expr(
            pgrust_nodes::primnodes::BoolExprType::Not,
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
    agg_list: &[CollectedAggregate],
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
        comparison: None,
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
    agg_list: &[CollectedAggregate],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    let raw_left_type =
        grouped_infer_sql_expr_type(left, input_scope, catalog, outer_scopes, grouped_outer);
    let raw_array_type =
        grouped_infer_sql_expr_type(array, input_scope, catalog, outer_scopes, grouped_outer);
    let left_type =
        coerce_unknown_string_literal_type(left, raw_left_type, raw_array_type.element_type());
    let array_type = if raw_array_type.is_array {
        coerce_unknown_string_literal_type(array, raw_array_type, raw_left_type)
    } else {
        SqlType::array_of(left_type.element_type())
    };
    let bound_left = bind_agg_output_expr(
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
    let (bound_left, left_explicit_collation) = strip_explicit_collation(bound_left);
    let left = coerce_bound_expr(bound_left, raw_left_type, left_type);
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
    let collation_oid = consumer_for_subquery_comparison_op(op)
        .map(|consumer| {
            derive_consumer_collation(
                catalog,
                consumer,
                &[
                    (left_type, left_explicit_collation),
                    (array_type.element_type(), None),
                ],
            )
        })
        .transpose()?
        .flatten();
    Ok(Expr::scalar_array_op_with_collation(
        op,
        !is_all,
        left,
        right,
        collation_oid,
    ))
}

pub(super) fn bind_grouped_func_call(
    name: &str,
    args: &[SqlFunctionArg],
    func_variadic: bool,
    group_by_exprs: &[SqlExpr],
    group_key_exprs: &[Expr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[CollectedAggregate],
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
    if !func_variadic
        && !name.eq_ignore_ascii_case("pg_lsn")
        && let Some(target_type) = resolve_function_cast_type(catalog, name)
        && args.len() == 1
        && args.iter().all(|arg| arg.name.is_none())
    {
        let source_type = grouped_infer_sql_expr_type(
            &args[0].value,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
        );
        let bound_arg = bind_agg_output_expr(
            &args[0].value,
            group_by_exprs,
            group_key_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?;
        if catalog_backed_explicit_cast_allowed(source_type, target_type, catalog) {
            return Ok(Expr::Cast(
                Box::new(bound_arg),
                if source_type == target_type {
                    source_type
                } else {
                    target_type
                },
            ));
        }
    }
    let func = match resolve_scalar_function(name) {
        Some(func) => func,
        None => {
            let raw_args = args.iter().map(|arg| arg.value.clone()).collect::<Vec<_>>();
            let bound_args = raw_args
                .iter()
                .map(|arg| {
                    let expr = bind_agg_output_expr(
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
                    let sql_type = expr_sql_type_hint(&expr).unwrap_or_else(|| {
                        grouped_infer_sql_expr_type(
                            arg,
                            input_scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                        )
                    });
                    Ok(TypedExpr {
                        expr,
                        sql_type,
                        contains_srf: false,
                    })
                })
                .collect::<Result<Vec<_>, ParseError>>()?;
            let arg_types = bound_args
                .iter()
                .map(|arg| arg.sql_type)
                .collect::<Vec<_>>();
            let resolved = resolve_function_call(catalog, name, &arg_types, func_variadic)
                .map_err(|_| ParseError::UnexpectedToken {
                    expected: "supported builtin function",
                    actual: name.to_string(),
                })?;
            if resolved.prokind != 'f' {
                return Err(ParseError::UnexpectedToken {
                    expected: "scalar function",
                    actual: name.to_string(),
                });
            }
            return bind_user_defined_scalar_function_call_from_resolved_typed_args(
                &resolved, &raw_args, bound_args, catalog,
            );
        }
    };
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
    let arg_types = bound_args
        .iter()
        .zip(lowered_args.iter())
        .map(|(arg, raw_arg)| {
            expr_sql_type_hint(arg).unwrap_or_else(|| {
                grouped_infer_sql_expr_type(
                    raw_arg,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                )
            })
        })
        .collect::<Vec<_>>();
    match func {
        BuiltinScalarFunction::Left | BuiltinScalarFunction::Repeat => {
            let left_type = arg_types[0];
            let right_type = arg_types[1];
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
        BuiltinScalarFunction::Lower | BuiltinScalarFunction::Upper => {
            let arg_type = arg_types[0];
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
        BuiltinScalarFunction::Abs => {
            let arg_type = arg_types[0];
            Ok(Expr::builtin_func(
                func,
                Some(arg_type),
                func_variadic,
                bound_args,
            ))
        }
        BuiltinScalarFunction::Sin
        | BuiltinScalarFunction::Cos
        | BuiltinScalarFunction::Erf
        | BuiltinScalarFunction::Erfc => Ok(Expr::builtin_func(
            func,
            Some(SqlType::new(SqlTypeKind::Float8)),
            func_variadic,
            vec![coerce_bound_expr(
                bound_args[0].clone(),
                arg_types[0],
                SqlType::new(SqlTypeKind::Float8),
            )],
        )),
        BuiltinScalarFunction::Sqrt | BuiltinScalarFunction::Exp | BuiltinScalarFunction::Ln => {
            let result_type = if matches!(arg_types[0].kind, SqlTypeKind::Numeric) {
                SqlType::new(SqlTypeKind::Numeric)
            } else {
                SqlType::new(SqlTypeKind::Float8)
            };
            Ok(Expr::builtin_func(
                func,
                Some(result_type),
                func_variadic,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_types[0],
                    result_type,
                )],
            ))
        }
        _ => Ok(Expr::builtin_func(
            func,
            fixed_scalar_return_type(func),
            func_variadic,
            bound_args,
        )),
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
    agg_list: &[CollectedAggregate],
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
    let common_type = grouped_infer_common_scalar_expr_type(
        &lowered_args,
        input_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        "COALESCE arguments with a common type",
    )?;
    let mut bound_args = Vec::with_capacity(lowered_args.len());
    for arg in &lowered_args {
        let arg_type =
            grouped_infer_sql_expr_type(arg, input_scope, catalog, outer_scopes, grouped_outer);
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
    agg_list: &[CollectedAggregate],
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
        array_type: infer_array_literal_type_with_ctes(
            elements,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            &current_grouped_agg_visible_ctes(),
        )
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "array literal elements with a common type",
            actual: "ARRAY[...]".into(),
        })?,
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
    let visible_ctes = current_grouped_agg_visible_ctes();
    let child_visible_agg_scope = child_visible_aggregate_scope();
    let (query, _) = analyze_select_query_with_outer(
        select,
        catalog,
        &child_outer,
        Some(GroupedOuterScope {
            scope: input_scope.clone(),
            group_by_exprs: group_by_exprs.to_vec(),
        }),
        child_visible_agg_scope.as_ref(),
        &visible_ctes,
        &[],
    )?;
    Ok(query)
}
