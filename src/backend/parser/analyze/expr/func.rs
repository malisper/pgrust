use super::*;
use crate::backend::utils::record::assign_anonymous_record_descriptor;
use crate::include::catalog::{ANYOID, range_type_ref_for_sql_type};
use crate::include::nodes::primnodes::expr_sql_type_hint;

pub(super) fn bind_row_to_json_call(
    name: &str,
    args: &[SqlFunctionArg],
    func_variadic: bool,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if args.iter().any(|arg| arg.name.is_some()) {
        return Err(ParseError::FeatureNotSupported(
            "named arguments are not supported for row_to_json".into(),
        ));
    }

    let bound_args = args
        .iter()
        .map(|arg| {
            bind_row_to_json_arg_expr(
                &arg.value,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let actual_types = bound_args
        .iter()
        .map(|(_, sql_type)| *sql_type)
        .collect::<Vec<_>>();
    let resolved =
        resolve_function_call(catalog, name, &actual_types, func_variadic).or_else(|_| {
            let first = actual_types.first().copied();
            let second = actual_types.get(1).copied();
            match (first, second, actual_types.len()) {
                (Some(first), None, 1)
                    if matches!(first.kind, SqlTypeKind::Record | SqlTypeKind::Composite)
                        && !first.is_array =>
                {
                    let mut resolved = resolve_function_call(
                        catalog,
                        name,
                        &[SqlType::record(crate::include::catalog::RECORD_TYPE_OID)],
                        func_variadic,
                    )?;
                    resolved.declared_arg_types = vec![first];
                    Ok(resolved)
                }
                (Some(first), Some(second), 2)
                    if matches!(first.kind, SqlTypeKind::Record | SqlTypeKind::Composite)
                        && !first.is_array
                        && second == SqlType::new(SqlTypeKind::Bool) =>
                {
                    let mut resolved = resolve_function_call(
                        catalog,
                        name,
                        &[
                            SqlType::record(crate::include::catalog::RECORD_TYPE_OID),
                            SqlType::new(SqlTypeKind::Bool),
                        ],
                        func_variadic,
                    )?;
                    resolved.declared_arg_types = vec![first, second];
                    Ok(resolved)
                }
                _ => Err(ParseError::UnexpectedToken {
                    expected: "supported function",
                    actual: name.into(),
                }),
            }
        })?;
    let coerced_args = bound_args
        .into_iter()
        .zip(resolved.declared_arg_types.iter().copied())
        .map(|((arg, actual_type), declared_type)| {
            coerce_bound_expr(arg, actual_type, declared_type)
        })
        .collect::<Vec<_>>();
    Ok(Expr::resolved_builtin_func(
        BuiltinScalarFunction::RowToJson,
        resolved.proc_oid,
        Some(resolved.result_type),
        resolved.func_variadic,
        coerced_args,
    ))
}

fn bind_row_to_json_arg_expr(
    arg: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(Expr, SqlType), ParseError> {
    match arg {
        SqlExpr::Column(name) => {
            if let Some(fields) = resolve_relation_row_expr_with_outer(scope, outer_scopes, name) {
                let descriptor = assign_anonymous_record_descriptor(
                    fields
                        .iter()
                        .map(|(field_name, expr)| {
                            (
                                field_name.clone(),
                                expr_sql_type_hint(expr).unwrap_or(SqlType::new(SqlTypeKind::Text)),
                            )
                        })
                        .collect(),
                );
                Ok((
                    Expr::Row {
                        descriptor: descriptor.clone(),
                        fields,
                    },
                    descriptor.sql_type(),
                ))
            } else {
                let sql_type = infer_sql_expr_type_with_ctes(
                    arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                Ok((
                    bind_expr_with_outer_and_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?,
                    sql_type,
                ))
            }
        }
        _ => {
            let sql_type = infer_sql_expr_type_with_ctes(
                arg,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            Ok((
                bind_expr_with_outer_and_ctes(
                    arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
                sql_type,
            ))
        }
    }
}

fn bind_json_constructor_arg_expr(
    arg: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(Expr, SqlType), ParseError> {
    match arg {
        SqlExpr::Column(name) => {
            if let Some(fields) = resolve_relation_row_expr_with_outer(scope, outer_scopes, name) {
                let descriptor = assign_anonymous_record_descriptor(
                    fields
                        .iter()
                        .map(|(field_name, expr)| {
                            (
                                field_name.clone(),
                                expr_sql_type_hint(expr).unwrap_or(SqlType::new(SqlTypeKind::Text)),
                            )
                        })
                        .collect(),
                );
                Ok((
                    Expr::Row {
                        descriptor: descriptor.clone(),
                        fields,
                    },
                    descriptor.sql_type(),
                ))
            } else {
                let sql_type = infer_sql_expr_type_with_ctes(
                    arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                Ok((
                    bind_expr_with_outer_and_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?,
                    sql_type,
                ))
            }
        }
        _ => {
            let sql_type = infer_sql_expr_type_with_ctes(
                arg,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            Ok((
                bind_expr_with_outer_and_ctes(
                    arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
                sql_type,
            ))
        }
    }
}

pub(super) fn bind_user_defined_scalar_function_call(
    proc_oid: u32,
    result_type: SqlType,
    declared_arg_types: &[SqlType],
    args: &[SqlFunctionArg],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if args.iter().any(|arg| arg.name.is_some()) {
        return Err(ParseError::FeatureNotSupported(
            "named arguments are not supported for user-defined function calls".into(),
        ));
    }
    let arg_types = args
        .iter()
        .map(|arg| {
            infer_sql_expr_type_with_ctes(
                &arg.value,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        })
        .collect::<Vec<_>>();
    let bound_args = args
        .iter()
        .map(|arg| {
            bind_expr_with_outer_and_ctes(
                &arg.value,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let coerced_args = bound_args
        .into_iter()
        .zip(arg_types)
        .zip(declared_arg_types.iter().copied())
        .map(|((arg, actual_type), declared_type)| {
            coerce_bound_expr(arg, actual_type, declared_type)
        })
        .collect();
    Ok(Expr::user_defined_func(
        proc_oid,
        Some(result_type),
        false,
        coerced_args,
    ))
}

pub(super) fn bind_scalar_function_call(
    func: BuiltinScalarFunction,
    func_oid: u32,
    result_type: Option<SqlType>,
    func_variadic: bool,
    nvargs: usize,
    vatype_oid: u32,
    declared_arg_types: &[SqlType],
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let bound_args_with_types = if matches!(
        func,
        BuiltinScalarFunction::JsonBuildArray
            | BuiltinScalarFunction::JsonBuildObject
            | BuiltinScalarFunction::JsonbBuildArray
            | BuiltinScalarFunction::JsonbBuildObject
    ) {
        args.iter()
            .map(|arg| {
                bind_json_constructor_arg_expr(
                    arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        args.iter()
            .map(|arg| {
                let sql_type = infer_sql_expr_type_with_ctes(
                    arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                let bound = bind_expr_with_outer_and_ctes(
                    arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                Ok((bound, sql_type))
            })
            .collect::<Result<Vec<_>, ParseError>>()?
    };
    let arg_types = bound_args_with_types
        .iter()
        .map(|(_, sql_type)| *sql_type)
        .collect::<Vec<_>>();
    let bound_args = bound_args_with_types
        .into_iter()
        .map(|(bound, _)| bound)
        .collect::<Vec<_>>();
    let rewritten_bound_args = rewrite_variadic_bound_args(
        bound_args.clone(),
        &arg_types,
        func_variadic,
        nvargs,
        vatype_oid,
        catalog,
    )?;
    let build_func = |funcvariadic: bool, args: Vec<Expr>| {
        Expr::resolved_builtin_func(func, func_oid, result_type, funcvariadic, args)
    };
    match func {
        BuiltinScalarFunction::Random | BuiltinScalarFunction::RandomNormal => {
            if bound_args.is_empty() {
                return Ok(build_func(false, bound_args));
            }

            let target_types = if matches!(func, BuiltinScalarFunction::RandomNormal) {
                vec![SqlType::new(SqlTypeKind::Float8); bound_args.len()]
            } else if bound_args.len() == 2 {
                let left_type = arg_types[0];
                let right_type = arg_types[1];
                let target = if matches!(left_type.kind, SqlTypeKind::Numeric)
                    || matches!(right_type.kind, SqlTypeKind::Numeric)
                {
                    SqlType::new(SqlTypeKind::Numeric)
                } else if matches!(left_type.kind, SqlTypeKind::Int8)
                    || matches!(right_type.kind, SqlTypeKind::Int8)
                {
                    SqlType::new(SqlTypeKind::Int8)
                } else {
                    SqlType::new(SqlTypeKind::Int4)
                };
                vec![target; 2]
            } else if declared_arg_types.len() == bound_args.len() {
                declared_arg_types.to_vec()
            } else {
                arg_types.clone()
            };

            let coerced = bound_args
                .into_iter()
                .zip(arg_types)
                .zip(target_types)
                .map(|((arg, actual_type), declared_type)| {
                    coerce_bound_expr(arg, actual_type, declared_type)
                })
                .collect();
            Ok(build_func(false, coerced))
        }
        BuiltinScalarFunction::CashLarger | BuiltinScalarFunction::CashSmaller => {
            let money = SqlType::new(SqlTypeKind::Money);
            let coerced = bound_args
                .into_iter()
                .zip(arg_types)
                .map(|(arg, ty)| coerce_bound_expr(arg, ty, money))
                .collect();
            Ok(build_func(false, coerced))
        }
        BuiltinScalarFunction::CashWords => Ok(build_func(
            false,
            vec![coerce_bound_expr(
                bound_args[0].clone(),
                arg_types[0],
                SqlType::new(SqlTypeKind::Money),
            )],
        )),
        BuiltinScalarFunction::DatePart => Ok(build_func(
            false,
            vec![
                coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_types[0],
                    SqlType::new(SqlTypeKind::Text),
                ),
                bound_args[1].clone(),
            ],
        )),
        BuiltinScalarFunction::DateTrunc => {
            let target_type = match arg_types[1].kind {
                SqlTypeKind::Date => SqlType::new(SqlTypeKind::Date),
                SqlTypeKind::Timestamp => SqlType::new(SqlTypeKind::Timestamp),
                SqlTypeKind::TimestampTz => SqlType::new(SqlTypeKind::TimestampTz),
                _ => arg_types[1],
            };
            Ok(build_func(
                false,
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        arg_types[0],
                        SqlType::new(SqlTypeKind::Text),
                    ),
                    coerce_bound_expr(bound_args[1].clone(), arg_types[1], target_type),
                ],
            ))
        }
        BuiltinScalarFunction::IsFinite => Ok(build_func(false, bound_args)),
        BuiltinScalarFunction::MakeDate => Ok(build_func(
            false,
            arg_types
                .into_iter()
                .zip(bound_args)
                .map(|(ty, arg)| coerce_bound_expr(arg, ty, SqlType::new(SqlTypeKind::Int4)))
                .collect(),
        )),
        BuiltinScalarFunction::ToTsVector
        | BuiltinScalarFunction::ToTsQuery
        | BuiltinScalarFunction::PlainToTsQuery
        | BuiltinScalarFunction::PhraseToTsQuery
        | BuiltinScalarFunction::WebSearchToTsQuery
        | BuiltinScalarFunction::TsLexize => Ok(build_func(
            false,
            args.iter()
                .enumerate()
                .map(|(idx, arg)| {
                    let ty = infer_sql_expr_type_with_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                    coerce_bound_expr(bound_args[idx].clone(), ty, SqlType::new(SqlTypeKind::Text))
                })
                .collect(),
        )),
        BuiltinScalarFunction::Left
        | BuiltinScalarFunction::Right
        | BuiltinScalarFunction::Repeat => {
            let left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !should_use_text_concat(&args[0], left_type, &args[0], left_type) {
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
            Ok(build_func(
                func_variadic,
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
        BuiltinScalarFunction::Concat => Ok(build_func(func_variadic, bound_args)),
        BuiltinScalarFunction::ConcatWs => {
            let separator_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let mut lowered = Vec::with_capacity(bound_args.len());
            lowered.push(coerce_bound_expr(
                bound_args[0].clone(),
                separator_type,
                SqlType::new(SqlTypeKind::Text),
            ));
            lowered.extend(bound_args.iter().skip(1).cloned());
            Ok(build_func(func_variadic, lowered))
        }
        BuiltinScalarFunction::Format => {
            let format_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let mut lowered = Vec::with_capacity(bound_args.len());
            lowered.push(coerce_bound_expr(
                bound_args[0].clone(),
                format_type,
                SqlType::new(SqlTypeKind::Text),
            ));
            lowered.extend(bound_args.iter().skip(1).cloned());
            Ok(build_func(func_variadic, lowered))
        }
        BuiltinScalarFunction::Length => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !matches!(arg_type.kind, SqlTypeKind::TsVector)
                && !is_bit_string_type(arg_type)
                && !should_use_text_concat(&args[0], arg_type, &args[0], arg_type)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "text, bit, or tsvector argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            Ok(build_func(func_variadic, vec![bound_args[0].clone()]))
        }
        BuiltinScalarFunction::Position => {
            let left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if is_bit_string_type(left_type) && is_bit_string_type(right_type) {
                let common = resolve_common_scalar_type(left_type, right_type)
                    .unwrap_or(SqlType::new(SqlTypeKind::VarBit));
                return Ok(build_func(
                    func_variadic,
                    vec![
                        coerce_bound_expr(bound_args[0].clone(), left_type, common),
                        coerce_bound_expr(bound_args[1].clone(), right_type, common),
                    ],
                ));
            }
            if left_type.kind == SqlTypeKind::Bytea && right_type.kind == SqlTypeKind::Bytea {
                return Ok(build_func(
                    func_variadic,
                    vec![bound_args[0].clone(), bound_args[1].clone()],
                ));
            }
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        left_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        right_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::Strpos => {
            let left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        left_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        right_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::Substring => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let start_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if is_bit_string_type(value_type) {
                if !is_integer_family(start_type) {
                    return Err(ParseError::UnexpectedToken {
                        expected: "substring(bit, int4[, int4])",
                        actual: format!(
                            "{func:?}({}, {})",
                            sql_type_name(value_type),
                            sql_type_name(start_type)
                        ),
                    });
                }
                let mut coerced = vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        start_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ];
                if let Some(len_arg) = args.get(2) {
                    let len_type = infer_sql_expr_type_with_ctes(
                        len_arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                    if !is_integer_family(len_type) {
                        return Err(ParseError::UnexpectedToken {
                            expected: "integer length argument",
                            actual: sql_type_name(len_type),
                        });
                    }
                    coerced.push(coerce_bound_expr(
                        bound_args[2].clone(),
                        len_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ));
                }
                return Ok(build_func(func_variadic, coerced));
            }
            if value_type.kind == SqlTypeKind::Bytea {
                if !is_integer_family(start_type) {
                    return Err(ParseError::UnexpectedToken {
                        expected: "substring(bytea, int4[, int4])",
                        actual: format!(
                            "{func:?}({}, {})",
                            sql_type_name(value_type),
                            sql_type_name(start_type)
                        ),
                    });
                }
                let mut coerced = vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        start_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ];
                if let Some(len_arg) = args.get(2) {
                    let len_type = infer_sql_expr_type_with_ctes(
                        len_arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                    if !is_integer_family(len_type) {
                        return Err(ParseError::UnexpectedToken {
                            expected: "integer length argument",
                            actual: sql_type_name(len_type),
                        });
                    }
                    coerced.push(coerce_bound_expr(
                        bound_args[2].clone(),
                        len_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ));
                }
                return Ok(build_func(func_variadic, coerced));
            }
            if value_type.kind != SqlTypeKind::Text {
                return Err(ParseError::UnexpectedToken {
                    expected: "substring(text, int4[, int4]) or substring(text, text[, text])",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(value_type),
                        sql_type_name(start_type)
                    ),
                });
            }
            let text_target = coerce_bound_expr(
                bound_args[0].clone(),
                value_type,
                SqlType::new(SqlTypeKind::Text),
            );
            if start_type.kind == SqlTypeKind::Text {
                let mut coerced = vec![
                    text_target,
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        start_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ];
                if let Some(third_arg) = args.get(2) {
                    let third_type = infer_sql_expr_type_with_ctes(
                        third_arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                    if third_type.kind != SqlTypeKind::Text {
                        return Err(ParseError::UnexpectedToken {
                            expected: "text escape argument",
                            actual: sql_type_name(third_type),
                        });
                    }
                    coerced.push(coerce_bound_expr(
                        bound_args[2].clone(),
                        third_type,
                        SqlType::new(SqlTypeKind::Text),
                    ));
                    return Ok(Expr::builtin_func(
                        BuiltinScalarFunction::SimilarSubstring,
                        result_type,
                        func_variadic,
                        coerced,
                    ));
                }
                return Ok(build_func(func_variadic, coerced));
            }
            if !is_integer_family(start_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "substring(text, int4[, int4]) or substring(text, text[, text])",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(value_type),
                        sql_type_name(start_type)
                    ),
                });
            }
            let mut coerced = vec![
                text_target,
                coerce_bound_expr(
                    bound_args[1].clone(),
                    start_type,
                    SqlType::new(SqlTypeKind::Int4),
                ),
            ];
            if let Some(len_arg) = args.get(2) {
                let len_type = infer_sql_expr_type_with_ctes(
                    len_arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                if !is_integer_family(len_type) {
                    return Err(ParseError::UnexpectedToken {
                        expected: "integer length argument",
                        actual: sql_type_name(len_type),
                    });
                }
                coerced.push(coerce_bound_expr(
                    bound_args[2].clone(),
                    len_type,
                    SqlType::new(SqlTypeKind::Int4),
                ));
            }
            Ok(build_func(func_variadic, coerced))
        }
        BuiltinScalarFunction::SimilarSubstring => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let pattern_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if value_type.kind != SqlTypeKind::Text || pattern_type.kind != SqlTypeKind::Text {
                return Err(ParseError::UnexpectedToken {
                    expected: "substring(text similar text escape text)",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(value_type),
                        sql_type_name(pattern_type)
                    ),
                });
            }
            let mut coerced = vec![
                coerce_bound_expr(
                    bound_args[0].clone(),
                    value_type,
                    SqlType::new(SqlTypeKind::Text),
                ),
                coerce_bound_expr(
                    bound_args[1].clone(),
                    pattern_type,
                    SqlType::new(SqlTypeKind::Text),
                ),
            ];
            if let Some(escape_arg) = args.get(2) {
                let escape_type = infer_sql_expr_type_with_ctes(
                    escape_arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                if escape_type.kind != SqlTypeKind::Text {
                    return Err(ParseError::UnexpectedToken {
                        expected: "text escape argument",
                        actual: sql_type_name(escape_type),
                    });
                }
                coerced.push(coerce_bound_expr(
                    bound_args[2].clone(),
                    escape_type,
                    SqlType::new(SqlTypeKind::Text),
                ));
            }
            Ok(build_func(func_variadic, coerced))
        }
        BuiltinScalarFunction::Overlay => {
            let raw_value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let raw_place_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let value_type =
                coerce_unknown_string_literal_type(&args[0], raw_value_type, raw_place_type);
            let place_type =
                coerce_unknown_string_literal_type(&args[1], raw_place_type, value_type);
            let start_type = infer_sql_expr_type_with_ctes(
                &args[2],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let same_bit_kind = is_bit_string_type(value_type) && is_bit_string_type(place_type);
            let same_bytea_kind =
                value_type.kind == SqlTypeKind::Bytea && place_type.kind == SqlTypeKind::Bytea;
            let same_text_kind = is_text_like_type(value_type) && is_text_like_type(place_type);
            if (!same_bit_kind && !same_bytea_kind && !same_text_kind)
                || !is_integer_family(start_type)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "overlay(text, text, int4[, int4]), overlay(bit, bit, int4[, int4]) or overlay(bytea, bytea, int4[, int4])",
                    actual: format!(
                        "{func:?}({}, {}, {})",
                        sql_type_name(value_type),
                        sql_type_name(place_type),
                        sql_type_name(start_type)
                    ),
                });
            }
            let mut coerced = if same_bytea_kind {
                vec![
                    bound_args[0].clone(),
                    bound_args[1].clone(),
                    coerce_bound_expr(
                        bound_args[2].clone(),
                        start_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ]
            } else if same_text_kind {
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        raw_value_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        raw_place_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                    coerce_bound_expr(
                        bound_args[2].clone(),
                        start_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ]
            } else {
                let common = resolve_common_scalar_type(value_type, place_type)
                    .unwrap_or(SqlType::new(SqlTypeKind::VarBit));
                vec![
                    coerce_bound_expr(bound_args[0].clone(), raw_value_type, common),
                    coerce_bound_expr(bound_args[1].clone(), raw_place_type, common),
                    coerce_bound_expr(
                        bound_args[2].clone(),
                        start_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ]
            };
            if let Some(len_arg) = args.get(3) {
                let len_type = infer_sql_expr_type_with_ctes(
                    len_arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                if !is_integer_family(len_type) {
                    return Err(ParseError::UnexpectedToken {
                        expected: "integer length argument",
                        actual: sql_type_name(len_type),
                    });
                }
                coerced.push(coerce_bound_expr(
                    bound_args[3].clone(),
                    len_type,
                    SqlType::new(SqlTypeKind::Int4),
                ));
            }
            Ok(build_func(func_variadic, coerced))
        }
        BuiltinScalarFunction::GetBit => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let index_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !(is_bit_string_type(value_type) || value_type.kind == SqlTypeKind::Bytea)
                || !is_integer_family(index_type)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "get_bit(bit, int4) or get_bit(bytea, int4)",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(value_type),
                        sql_type_name(index_type)
                    ),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        index_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::SetBit => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let index_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let bit_type = infer_sql_expr_type_with_ctes(
                &args[2],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !(is_bit_string_type(value_type) || value_type.kind == SqlTypeKind::Bytea)
                || !is_integer_family(index_type)
                || !is_integer_family(bit_type)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "set_bit(bit, int4, int4) or set_bit(bytea, int4, int4)",
                    actual: format!(
                        "{func:?}({}, {}, {})",
                        sql_type_name(value_type),
                        sql_type_name(index_type),
                        sql_type_name(bit_type)
                    ),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        index_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                    coerce_bound_expr(
                        bound_args[2].clone(),
                        bit_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::BitCount => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !(is_bit_string_type(value_type) || value_type.kind == SqlTypeKind::Bytea) {
                return Err(ParseError::UnexpectedToken {
                    expected: "bit or bytea argument",
                    actual: format!("{func:?}({})", sql_type_name(value_type)),
                });
            }
            Ok(build_func(func_variadic, vec![bound_args[0].clone()]))
        }
        BuiltinScalarFunction::GetByte => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let index_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if value_type.kind != SqlTypeKind::Bytea || !is_integer_family(index_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "get_byte(bytea, int4)",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(value_type),
                        sql_type_name(index_type)
                    ),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        index_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::SetByte => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let index_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let new_type = infer_sql_expr_type_with_ctes(
                &args[2],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if value_type.kind != SqlTypeKind::Bytea
                || !is_integer_family(index_type)
                || !is_integer_family(new_type)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "set_byte(bytea, int4, int4)",
                    actual: format!(
                        "{func:?}({}, {}, {})",
                        sql_type_name(value_type),
                        sql_type_name(index_type),
                        sql_type_name(new_type)
                    ),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        index_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                    coerce_bound_expr(
                        bound_args[2].clone(),
                        new_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::ConvertFrom => {
            let left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        left_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        right_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::Lower | BuiltinScalarFunction::Unistr => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Text),
                )],
            ))
        }
        BuiltinScalarFunction::Initcap
        | BuiltinScalarFunction::Ascii
        | BuiltinScalarFunction::Replace
        | BuiltinScalarFunction::Translate => Ok(build_func(
            func_variadic,
            args.iter()
                .enumerate()
                .map(|(idx, arg)| {
                    let ty = infer_sql_expr_type_with_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                    coerce_bound_expr(bound_args[idx].clone(), ty, SqlType::new(SqlTypeKind::Text))
                })
                .collect(),
        )),
        BuiltinScalarFunction::Chr => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !is_integer_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "integer argument",
                    actual: sql_type_name(arg_type),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Int4),
                )],
            ))
        }
        BuiltinScalarFunction::SplitPart => Ok(build_func(
            func_variadic,
            vec![
                coerce_bound_expr(
                    bound_args[0].clone(),
                    infer_sql_expr_type_with_ctes(
                        &args[0],
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    ),
                    SqlType::new(SqlTypeKind::Text),
                ),
                coerce_bound_expr(
                    bound_args[1].clone(),
                    infer_sql_expr_type_with_ctes(
                        &args[1],
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    ),
                    SqlType::new(SqlTypeKind::Text),
                ),
                coerce_bound_expr(
                    bound_args[2].clone(),
                    infer_sql_expr_type_with_ctes(
                        &args[2],
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    ),
                    SqlType::new(SqlTypeKind::Int4),
                ),
            ],
        )),
        BuiltinScalarFunction::LPad | BuiltinScalarFunction::RPad => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let len_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let mut coerced = vec![
                coerce_bound_expr(
                    bound_args[0].clone(),
                    value_type,
                    SqlType::new(SqlTypeKind::Text),
                ),
                coerce_bound_expr(
                    bound_args[1].clone(),
                    len_type,
                    SqlType::new(SqlTypeKind::Int4),
                ),
            ];
            if let Some(fill_arg) = args.get(2) {
                let fill_type = infer_sql_expr_type_with_ctes(
                    fill_arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                coerced.push(coerce_bound_expr(
                    bound_args[2].clone(),
                    fill_type,
                    SqlType::new(SqlTypeKind::Text),
                ));
            }
            Ok(build_func(func_variadic, coerced))
        }
        BuiltinScalarFunction::BTrim
        | BuiltinScalarFunction::LTrim
        | BuiltinScalarFunction::RTrim => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let mut coerced = vec![bound_args[0].clone()];
            if let Some(chars_arg) = args.get(1) {
                let chars_type = infer_sql_expr_type_with_ctes(
                    chars_arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                let target = if value_type.kind == SqlTypeKind::Bytea {
                    SqlType::new(SqlTypeKind::Bytea)
                } else {
                    SqlType::new(SqlTypeKind::Text)
                };
                coerced[0] = coerce_bound_expr(bound_args[0].clone(), value_type, target);
                coerced.push(coerce_bound_expr(bound_args[1].clone(), chars_type, target));
            } else if value_type.kind != SqlTypeKind::Bytea {
                coerced[0] = coerce_bound_expr(
                    bound_args[0].clone(),
                    value_type,
                    SqlType::new(SqlTypeKind::Text),
                );
            }
            Ok(build_func(func_variadic, coerced))
        }
        BuiltinScalarFunction::RegexpMatch | BuiltinScalarFunction::RegexpLike => Ok(build_func(
            func_variadic,
            args.iter()
                .enumerate()
                .map(|(idx, arg)| {
                    let ty = infer_sql_expr_type_with_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                    let target = SqlType::new(SqlTypeKind::Text);
                    coerce_bound_expr(bound_args[idx].clone(), ty, target)
                })
                .collect(),
        )),
        BuiltinScalarFunction::RegexpCount => Ok(build_func(
            func_variadic,
            bind_regex_count_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        )),
        BuiltinScalarFunction::RegexpInstr => Ok(build_func(
            func_variadic,
            bind_regex_instr_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        )),
        BuiltinScalarFunction::RegexpSubstr => Ok(build_func(
            func_variadic,
            bind_regex_substr_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        )),
        BuiltinScalarFunction::RegexpReplace => Ok(build_func(
            func_variadic,
            bind_regex_replace_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        )),
        BuiltinScalarFunction::RegexpSplitToArray => Ok(build_func(
            func_variadic,
            bind_regex_split_to_array_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        )),
        BuiltinScalarFunction::Md5 => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !matches!(arg_type.kind, SqlTypeKind::Text | SqlTypeKind::Bytea) || arg_type.is_array
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "text or bytea argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            Ok(build_func(func_variadic, vec![bound_args[0].clone()]))
        }
        BuiltinScalarFunction::Reverse
        | BuiltinScalarFunction::Sha224
        | BuiltinScalarFunction::Sha256
        | BuiltinScalarFunction::Sha384
        | BuiltinScalarFunction::Sha512
        | BuiltinScalarFunction::Crc32
        | BuiltinScalarFunction::Crc32c => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !matches!(arg_type.kind, SqlTypeKind::Text | SqlTypeKind::Bytea) || arg_type.is_array
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "text or bytea argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            Ok(build_func(func_variadic, vec![bound_args[0].clone()]))
        }
        BuiltinScalarFunction::QuoteLiteral => Ok(build_func(func_variadic, bound_args)),
        BuiltinScalarFunction::Encode => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let format_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if value_type.kind != SqlTypeKind::Bytea {
                return Err(ParseError::UnexpectedToken {
                    expected: "bytea argument",
                    actual: sql_type_name(value_type),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        format_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::Decode => Ok(build_func(
            func_variadic,
            vec![
                coerce_bound_expr(
                    bound_args[0].clone(),
                    infer_sql_expr_type_with_ctes(
                        &args[0],
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    ),
                    SqlType::new(SqlTypeKind::Text),
                ),
                coerce_bound_expr(
                    bound_args[1].clone(),
                    infer_sql_expr_type_with_ctes(
                        &args[1],
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    ),
                    SqlType::new(SqlTypeKind::Text),
                ),
            ],
        )),
        BuiltinScalarFunction::ToChar => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let format_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !is_numeric_family(value_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric argument",
                    actual: format!("{func:?}({})", sql_type_name(value_type)),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        format_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::NumericInc
        | BuiltinScalarFunction::Factorial
        | BuiltinScalarFunction::PgLsn => {
            let raw_arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let arg_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_arg_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            if !is_numeric_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Numeric),
                )],
            ))
        }
        BuiltinScalarFunction::Log10 | BuiltinScalarFunction::Log if args.len() == 1 => {
            let raw_arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let arg_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_arg_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            if !is_numeric_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            let target = if matches!(
                arg_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) {
                SqlType::new(SqlTypeKind::Float8)
            } else {
                SqlType::new(SqlTypeKind::Numeric)
            };
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(bound_args[0].clone(), arg_type, target)],
            ))
        }
        BuiltinScalarFunction::Log => {
            let raw_left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let raw_right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let left_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_left_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            let right_type =
                coerce_unknown_string_literal_type(&args[1], raw_right_type, left_type);
            if !is_numeric_family(left_type) || !is_numeric_family(right_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric arguments",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(left_type),
                        sql_type_name(right_type)
                    ),
                });
            }
            let target = if matches!(
                left_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) || matches!(
                right_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) {
                SqlType::new(SqlTypeKind::Float8)
            } else {
                SqlType::new(SqlTypeKind::Numeric)
            };
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(bound_args[0].clone(), left_type, target),
                    coerce_bound_expr(bound_args[1].clone(), right_type, target),
                ],
            ))
        }
        BuiltinScalarFunction::Gcd | BuiltinScalarFunction::Lcm => {
            let left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !is_numeric_family(left_type) || !is_numeric_family(right_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric or integer arguments",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(left_type),
                        sql_type_name(right_type)
                    ),
                });
            }
            let common = resolve_numeric_binary_type("+", left_type, right_type)?;
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(bound_args[0].clone(), left_type, common),
                    coerce_bound_expr(bound_args[1].clone(), right_type, common),
                ],
            ))
        }
        BuiltinScalarFunction::Div | BuiltinScalarFunction::Mod => {
            let raw_left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let raw_right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let left_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_left_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            let right_type = coerce_unknown_string_literal_type(
                &args[1],
                raw_right_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            if !is_numeric_family(left_type) || !is_numeric_family(right_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric arguments",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(left_type),
                        sql_type_name(right_type)
                    ),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        left_type,
                        SqlType::new(SqlTypeKind::Numeric),
                    ),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        right_type,
                        SqlType::new(SqlTypeKind::Numeric),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::Scale
        | BuiltinScalarFunction::MinScale
        | BuiltinScalarFunction::TrimScale => {
            let raw_arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let arg_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_arg_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            if !is_numeric_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Numeric),
                )],
            ))
        }
        BuiltinScalarFunction::WidthBucket => {
            if args.len() == 2 {
                return Ok(build_func(func_variadic, bound_args));
            }
            let raw_operand_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let raw_low_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let raw_high_type = infer_sql_expr_type_with_ctes(
                &args[2],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let count_type = infer_sql_expr_type_with_ctes(
                &args[3],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let initial_operand_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_operand_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            let initial_low_type =
                coerce_unknown_string_literal_type(&args[1], raw_low_type, initial_operand_type);
            let initial_high_type =
                coerce_unknown_string_literal_type(&args[2], raw_high_type, initial_operand_type);
            if !is_numeric_family(initial_operand_type)
                || !is_numeric_family(initial_low_type)
                || !is_numeric_family(initial_high_type)
                || !is_integer_family(count_type)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "width_bucket(numeric, numeric, numeric, int4)",
                    actual: format!(
                        "{func:?}({}, {}, {}, {})",
                        sql_type_name(initial_operand_type),
                        sql_type_name(initial_low_type),
                        sql_type_name(initial_high_type),
                        sql_type_name(count_type)
                    ),
                });
            }
            let target = if matches!(
                initial_operand_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) || matches!(
                initial_low_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) || matches!(
                initial_high_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) {
                SqlType::new(SqlTypeKind::Float8)
            } else {
                SqlType::new(SqlTypeKind::Numeric)
            };
            let operand_type =
                coerce_unknown_string_literal_type(&args[0], raw_operand_type, target);
            let low_type = coerce_unknown_string_literal_type(&args[1], raw_low_type, target);
            let high_type = coerce_unknown_string_literal_type(&args[2], raw_high_type, target);
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(bound_args[0].clone(), operand_type, target),
                    coerce_bound_expr(bound_args[1].clone(), low_type, target),
                    coerce_bound_expr(bound_args[2].clone(), high_type, target),
                    coerce_bound_expr(
                        bound_args[3].clone(),
                        count_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::Trunc | BuiltinScalarFunction::Round if args.len() == 2 => {
            let raw_value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let value_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_value_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            let scale_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !matches!(value_type.element_type().kind, SqlTypeKind::Numeric)
                || !is_integer_family(scale_type)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric, integer arguments",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(value_type),
                        sql_type_name(scale_type)
                    ),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        value_type,
                        SqlType::new(SqlTypeKind::Numeric),
                    ),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        scale_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::Trunc | BuiltinScalarFunction::Round => {
            let raw_arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let arg_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_arg_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            if !is_numeric_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            let target = if matches!(
                arg_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) {
                SqlType::new(SqlTypeKind::Float8)
            } else {
                SqlType::new(SqlTypeKind::Numeric)
            };
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(bound_args[0].clone(), arg_type, target)],
            ))
        }
        BuiltinScalarFunction::Ceil
        | BuiltinScalarFunction::Ceiling
        | BuiltinScalarFunction::Floor
        | BuiltinScalarFunction::Sign
            if args.len() == 1 =>
        {
            let raw_arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let arg_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_arg_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            if !is_numeric_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            let target = if matches!(
                arg_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) {
                SqlType::new(SqlTypeKind::Float8)
            } else {
                SqlType::new(SqlTypeKind::Numeric)
            };
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(bound_args[0].clone(), arg_type, target)],
            ))
        }
        BuiltinScalarFunction::Ceil
        | BuiltinScalarFunction::Ceiling
        | BuiltinScalarFunction::Floor
        | BuiltinScalarFunction::Sign
        | BuiltinScalarFunction::Cbrt
        | BuiltinScalarFunction::Sinh
        | BuiltinScalarFunction::Cosh
        | BuiltinScalarFunction::Tanh
        | BuiltinScalarFunction::Asinh
        | BuiltinScalarFunction::Acosh
        | BuiltinScalarFunction::Atanh
        | BuiltinScalarFunction::Sind
        | BuiltinScalarFunction::Cosd
        | BuiltinScalarFunction::Tand
        | BuiltinScalarFunction::Cotd
        | BuiltinScalarFunction::Asind
        | BuiltinScalarFunction::Acosd
        | BuiltinScalarFunction::Atand
        | BuiltinScalarFunction::Erf
        | BuiltinScalarFunction::Erfc
        | BuiltinScalarFunction::Gamma
        | BuiltinScalarFunction::Lgamma => {
            let raw_arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let arg_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_arg_type,
                SqlType::new(SqlTypeKind::Float8),
            );
            if !is_numeric_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Float8),
                )],
            ))
        }
        BuiltinScalarFunction::Sqrt | BuiltinScalarFunction::Exp | BuiltinScalarFunction::Ln => {
            let raw_arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let arg_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_arg_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            if !is_numeric_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            let target = if matches!(
                arg_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) {
                SqlType::new(SqlTypeKind::Float8)
            } else {
                SqlType::new(SqlTypeKind::Numeric)
            };
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(bound_args[0].clone(), arg_type, target)],
            ))
        }
        BuiltinScalarFunction::BitcastIntegerToFloat4 => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if arg_type != SqlType::new(SqlTypeKind::Int4) {
                return Err(ParseError::UnexpectedToken {
                    expected: "integer argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            Ok(build_func(func_variadic, vec![bound_args[0].clone()]))
        }
        BuiltinScalarFunction::BitcastBigintToFloat8 => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if arg_type != SqlType::new(SqlTypeKind::Int8) {
                return Err(ParseError::UnexpectedToken {
                    expected: "bigint argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            Ok(build_func(func_variadic, vec![bound_args[0].clone()]))
        }
        BuiltinScalarFunction::Atan2d => {
            let raw_left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let raw_right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let left_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_left_type,
                SqlType::new(SqlTypeKind::Float8),
            );
            let right_type = coerce_unknown_string_literal_type(
                &args[1],
                raw_right_type,
                SqlType::new(SqlTypeKind::Float8),
            );
            if !is_numeric_family(left_type) || !is_numeric_family(right_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric arguments",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(left_type),
                        sql_type_name(right_type)
                    ),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        left_type,
                        SqlType::new(SqlTypeKind::Float8),
                    ),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        right_type,
                        SqlType::new(SqlTypeKind::Float8),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::Power => {
            let raw_left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let raw_right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let left_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_left_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            let right_type = coerce_unknown_string_literal_type(
                &args[1],
                raw_right_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            if !is_numeric_family(left_type) || !is_numeric_family(right_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric arguments",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(left_type),
                        sql_type_name(right_type)
                    ),
                });
            }
            let target = if matches!(
                left_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) || matches!(
                right_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) {
                SqlType::new(SqlTypeKind::Float8)
            } else {
                SqlType::new(SqlTypeKind::Numeric)
            };
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(bound_args[0].clone(), left_type, target),
                    coerce_bound_expr(bound_args[1].clone(), right_type, target),
                ],
            ))
        }
        BuiltinScalarFunction::Float4Send | BuiltinScalarFunction::Float8Send => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let target_type = if matches!(func, BuiltinScalarFunction::Float4Send) {
                SqlType::new(SqlTypeKind::Float4)
            } else {
                SqlType::new(SqlTypeKind::Float8)
            };
            if !is_numeric_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    target_type,
                )],
            ))
        }
        BuiltinScalarFunction::PgInputIsValid
        | BuiltinScalarFunction::PgInputErrorMessage
        | BuiltinScalarFunction::PgInputErrorDetail
        | BuiltinScalarFunction::PgInputErrorHint
        | BuiltinScalarFunction::PgInputErrorSqlState => {
            let text_type = SqlType::new(SqlTypeKind::Text);
            let left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(bound_args[0].clone(), left_type, text_type),
                    coerce_bound_expr(bound_args[1].clone(), right_type, text_type),
                ],
            ))
        }
        BuiltinScalarFunction::PgTypeof => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            Ok(Expr::Const(Value::Text(sql_type_name(arg_type).into())))
        }
        BuiltinScalarFunction::JsonbDeletePath
        | BuiltinScalarFunction::JsonbSet
        | BuiltinScalarFunction::JsonbSetLax
        | BuiltinScalarFunction::JsonbInsert => {
            let path_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let target_path_type = if matches!(
                &args[1],
                SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
            ) {
                SqlType::array_of(SqlType::new(SqlTypeKind::Text))
            } else {
                path_type
            };
            let mut rewritten = rewritten_bound_args;
            rewritten[1] = coerce_bound_expr(rewritten[1].clone(), path_type, target_path_type);
            Ok(build_func(func_variadic, rewritten))
        }
        BuiltinScalarFunction::ArrayNdims | BuiltinScalarFunction::ArrayDims => {
            Ok(build_func(func_variadic, vec![bound_args[0].clone()]))
        }
        BuiltinScalarFunction::ArrayLower => {
            let dim_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            Ok(build_func(
                func_variadic,
                vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        dim_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::JsonObject | BuiltinScalarFunction::JsonbObject => {
            let coerced = bound_args
                .into_iter()
                .zip(arg_types)
                .zip(declared_arg_types.iter().copied())
                .map(|((arg, actual_type), declared_type)| {
                    coerce_bound_expr(arg, actual_type, declared_type)
                })
                .collect();
            Ok(build_func(func_variadic, coerced))
        }
        BuiltinScalarFunction::JsonbArrayLength => {
            let target_type = declared_arg_types
                .first()
                .copied()
                .unwrap_or(SqlType::new(SqlTypeKind::Jsonb));
            let raw_arg_type = arg_types[0];
            let resolved_arg_type =
                coerce_unknown_string_literal_type(&args[0], raw_arg_type, target_type);
            Ok(build_func(
                func_variadic,
                vec![
                    if resolved_arg_type == target_type && raw_arg_type != target_type {
                        coerce_bound_expr(bound_args[0].clone(), raw_arg_type, target_type)
                    } else {
                        bound_args[0].clone()
                    },
                ],
            ))
        }
        BuiltinScalarFunction::RangeConstructor
        | BuiltinScalarFunction::RangeIsEmpty
        | BuiltinScalarFunction::RangeLower
        | BuiltinScalarFunction::RangeUpper
        | BuiltinScalarFunction::RangeLowerInc
        | BuiltinScalarFunction::RangeUpperInc
        | BuiltinScalarFunction::RangeLowerInf
        | BuiltinScalarFunction::RangeUpperInf
        | BuiltinScalarFunction::RangeContains
        | BuiltinScalarFunction::RangeContainedBy
        | BuiltinScalarFunction::RangeOverlap
        | BuiltinScalarFunction::RangeStrictLeft
        | BuiltinScalarFunction::RangeStrictRight
        | BuiltinScalarFunction::RangeOverLeft
        | BuiltinScalarFunction::RangeOverRight
        | BuiltinScalarFunction::RangeAdjacent
        | BuiltinScalarFunction::RangeUnion
        | BuiltinScalarFunction::RangeIntersect
        | BuiltinScalarFunction::RangeDifference
        | BuiltinScalarFunction::RangeMerge => {
            let fallback_declared = if !declared_arg_types.is_empty() {
                declared_arg_types.to_vec()
            } else if matches!(func, BuiltinScalarFunction::RangeConstructor) && args.is_empty() {
                Vec::new()
            } else if matches!(func, BuiltinScalarFunction::RangeConstructor) {
                let range_type = result_type
                    .and_then(range_type_ref_for_sql_type)
                    .ok_or_else(|| ParseError::UnexpectedToken {
                        expected: "range constructor with a concrete range return type",
                        actual: format!("{func:?}"),
                    })?;
                let mut types = vec![range_type.subtype, range_type.subtype];
                if args.len() == 3 {
                    types.push(SqlType::new(SqlTypeKind::Text));
                }
                types
            } else {
                arg_types.clone()
            };
            let coerced = bound_args
                .into_iter()
                .zip(arg_types)
                .zip(fallback_declared.into_iter())
                .map(|((arg, actual_type), declared_type)| {
                    coerce_bound_expr(arg, actual_type, declared_type)
                })
                .collect();
            Ok(build_func(func_variadic, coerced))
        }
        _ => Ok(build_func(func_variadic, rewritten_bound_args)),
    }
}

fn rewrite_variadic_bound_args(
    bound_args: Vec<Expr>,
    arg_types: &[SqlType],
    func_variadic: bool,
    nvargs: usize,
    vatype_oid: u32,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<Expr>, ParseError> {
    if !func_variadic {
        return Ok(bound_args);
    }
    if vatype_oid == ANYOID {
        return Ok(bound_args);
    }

    let element_type = catalog
        .type_by_oid(vatype_oid)
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "known variadic element type",
            actual: vatype_oid.to_string(),
        })?
        .sql_type;
    let array_type = SqlType::array_of(element_type);

    if nvargs > 0 {
        let fixed_prefix_len = bound_args.len().saturating_sub(nvargs);
        let mut rewritten = bound_args[..fixed_prefix_len].to_vec();
        let elements = bound_args[fixed_prefix_len..]
            .iter()
            .zip(arg_types[fixed_prefix_len..].iter())
            .map(|(expr, sql_type)| coerce_bound_expr(expr.clone(), *sql_type, element_type))
            .collect();
        rewritten.push(Expr::ArrayLiteral {
            elements,
            array_type,
        });
        return Ok(rewritten);
    }

    let mut rewritten = bound_args;
    if let (Some(last), Some(last_type)) = (rewritten.last_mut(), arg_types.last()) {
        *last = coerce_bound_expr(last.clone(), *last_type, array_type);
    }
    Ok(rewritten)
}

fn bind_regex_count_args(
    bound_args: &[Expr],
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Vec<Expr> {
    let mut out = Vec::with_capacity(args.len());
    for (idx, arg) in args.iter().enumerate() {
        let ty =
            infer_sql_expr_type_with_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes);
        out.push(coerce_bound_expr(
            bound_args[idx].clone(),
            ty,
            if idx == 2 {
                SqlType::new(SqlTypeKind::Int4)
            } else {
                SqlType::new(SqlTypeKind::Text)
            },
        ));
    }
    out
}

fn bind_regex_instr_args(
    bound_args: &[Expr],
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Vec<Expr> {
    let mut out = Vec::with_capacity(args.len());
    for (idx, arg) in args.iter().enumerate() {
        let ty =
            infer_sql_expr_type_with_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes);
        out.push(coerce_bound_expr(
            bound_args[idx].clone(),
            ty,
            match idx {
                2..=4 | 6 => SqlType::new(SqlTypeKind::Int4),
                _ => SqlType::new(SqlTypeKind::Text),
            },
        ));
    }
    out
}

fn bind_regex_substr_args(
    bound_args: &[Expr],
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Vec<Expr> {
    let mut out = Vec::with_capacity(args.len());
    for (idx, arg) in args.iter().enumerate() {
        let ty =
            infer_sql_expr_type_with_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes);
        out.push(coerce_bound_expr(
            bound_args[idx].clone(),
            ty,
            match idx {
                2 | 3 | 5 => SqlType::new(SqlTypeKind::Int4),
                _ => SqlType::new(SqlTypeKind::Text),
            },
        ));
    }
    out
}

fn bind_regex_replace_args(
    bound_args: &[Expr],
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Vec<Expr> {
    let mut out = Vec::with_capacity(args.len());
    let fourth_is_text = if args.len() == 4 {
        let arg_type = infer_sql_expr_type_with_ctes(
            &args[3],
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        );
        !is_integer_family(arg_type)
    } else {
        false
    };
    for (idx, arg) in args.iter().enumerate() {
        let ty =
            infer_sql_expr_type_with_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes);
        out.push(coerce_bound_expr(
            bound_args[idx].clone(),
            ty,
            match idx {
                0..=2 => SqlType::new(SqlTypeKind::Text),
                3 if fourth_is_text => SqlType::new(SqlTypeKind::Text),
                3 | 4 => SqlType::new(SqlTypeKind::Int4),
                _ => SqlType::new(SqlTypeKind::Text),
            },
        ));
    }
    out
}

fn bind_regex_split_to_array_args(
    bound_args: &[Expr],
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Vec<Expr> {
    let mut out = Vec::with_capacity(args.len());
    for (idx, arg) in args.iter().enumerate() {
        let ty =
            infer_sql_expr_type_with_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes);
        out.push(coerce_bound_expr(
            bound_args[idx].clone(),
            ty,
            SqlType::new(SqlTypeKind::Text),
        ));
    }
    out
}
