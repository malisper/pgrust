use super::*;

pub(super) fn resolve_scalar_function(name: &str) -> Option<BuiltinScalarFunction> {
    match name.to_ascii_lowercase().as_str() {
        "random" => Some(BuiltinScalarFunction::Random),
        "getdatabaseencoding" => Some(BuiltinScalarFunction::GetDatabaseEncoding),
        "to_json" => Some(BuiltinScalarFunction::ToJson),
        "to_jsonb" => Some(BuiltinScalarFunction::ToJsonb),
        "array_to_json" => Some(BuiltinScalarFunction::ArrayToJson),
        "json_build_array" => Some(BuiltinScalarFunction::JsonBuildArray),
        "json_build_object" => Some(BuiltinScalarFunction::JsonBuildObject),
        "json_object" => Some(BuiltinScalarFunction::JsonObject),
        "json_typeof" => Some(BuiltinScalarFunction::JsonTypeof),
        "json_array_length" => Some(BuiltinScalarFunction::JsonArrayLength),
        "json_extract_path" => Some(BuiltinScalarFunction::JsonExtractPath),
        "json_extract_path_text" => Some(BuiltinScalarFunction::JsonExtractPathText),
        "jsonb_typeof" => Some(BuiltinScalarFunction::JsonbTypeof),
        "jsonb_array_length" => Some(BuiltinScalarFunction::JsonbArrayLength),
        "jsonb_extract_path" => Some(BuiltinScalarFunction::JsonbExtractPath),
        "jsonb_extract_path_text" => Some(BuiltinScalarFunction::JsonbExtractPathText),
        "jsonb_build_array" => Some(BuiltinScalarFunction::JsonbBuildArray),
        "jsonb_build_object" => Some(BuiltinScalarFunction::JsonbBuildObject),
        "jsonb_path_exists" => Some(BuiltinScalarFunction::JsonbPathExists),
        "jsonb_path_match" => Some(BuiltinScalarFunction::JsonbPathMatch),
        "jsonb_path_query_array" => Some(BuiltinScalarFunction::JsonbPathQueryArray),
        "jsonb_path_query_first" => Some(BuiltinScalarFunction::JsonbPathQueryFirst),
        "left" => Some(BuiltinScalarFunction::Left),
        "repeat" => Some(BuiltinScalarFunction::Repeat),
        _ => None,
    }
}

pub(super) fn resolve_json_table_function(name: &str) -> Option<JsonTableFunction> {
    match name.to_ascii_lowercase().as_str() {
        "json_object_keys" => Some(JsonTableFunction::ObjectKeys),
        "json_each" => Some(JsonTableFunction::Each),
        "json_each_text" => Some(JsonTableFunction::EachText),
        "json_array_elements" => Some(JsonTableFunction::ArrayElements),
        "json_array_elements_text" => Some(JsonTableFunction::ArrayElementsText),
        "jsonb_object_keys" => Some(JsonTableFunction::JsonbObjectKeys),
        "jsonb_each" => Some(JsonTableFunction::JsonbEach),
        "jsonb_each_text" => Some(JsonTableFunction::JsonbEachText),
        "jsonb_array_elements" => Some(JsonTableFunction::JsonbArrayElements),
        "jsonb_array_elements_text" => Some(JsonTableFunction::JsonbArrayElementsText),
        _ => None,
    }
}

pub(super) fn validate_scalar_function_arity(
    func: BuiltinScalarFunction,
    args: &[SqlExpr],
) -> Result<(), ParseError> {
    let valid = match func {
        BuiltinScalarFunction::Random => args.is_empty(),
        BuiltinScalarFunction::GetDatabaseEncoding => args.is_empty(),
        BuiltinScalarFunction::ToJson | BuiltinScalarFunction::ToJsonb => args.len() == 1,
        BuiltinScalarFunction::ArrayToJson => matches!(args.len(), 1 | 2),
        BuiltinScalarFunction::JsonBuildArray | BuiltinScalarFunction::JsonBuildObject => true,
        BuiltinScalarFunction::JsonObject => matches!(args.len(), 1 | 2),
        BuiltinScalarFunction::JsonTypeof
        | BuiltinScalarFunction::JsonArrayLength
        | BuiltinScalarFunction::JsonbTypeof
        | BuiltinScalarFunction::JsonbArrayLength => args.len() == 1,
        BuiltinScalarFunction::JsonExtractPath
        | BuiltinScalarFunction::JsonExtractPathText
        | BuiltinScalarFunction::JsonbExtractPath
        | BuiltinScalarFunction::JsonbExtractPathText => !args.is_empty(),
        BuiltinScalarFunction::JsonbBuildArray | BuiltinScalarFunction::JsonbBuildObject => true,
        BuiltinScalarFunction::JsonbPathExists
        | BuiltinScalarFunction::JsonbPathMatch
        | BuiltinScalarFunction::JsonbPathQueryArray
        | BuiltinScalarFunction::JsonbPathQueryFirst => matches!(args.len(), 2..=4),
        BuiltinScalarFunction::Left | BuiltinScalarFunction::Repeat => args.len() == 2,
    };

    if valid {
        Ok(())
    } else {
        Err(ParseError::UnexpectedToken {
            expected: "valid builtin function arity",
            actual: format!("{func:?}({} args)", args.len()),
        })
    }
}

pub(super) fn validate_aggregate_arity(
    func: AggFunc,
    args: &[SqlExpr],
) -> Result<(), ParseError> {
    let valid = match func {
        AggFunc::Count => args.len() <= 1,
        AggFunc::Sum
        | AggFunc::Avg
        | AggFunc::Min
        | AggFunc::Max
        | AggFunc::JsonAgg
        | AggFunc::JsonbAgg => args.len() == 1,
        AggFunc::JsonObjectAgg | AggFunc::JsonbObjectAgg => args.len() == 2,
    };
    if valid {
        Ok(())
    } else {
        Err(ParseError::UnexpectedToken {
            expected: "valid aggregate arity",
            actual: format!("{}({} args)", func.name(), args.len()),
        })
    }
}

pub(super) fn bind_select_targets(
    targets: &[SelectItem],
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<Vec<TargetEntry>, ParseError> {
    if targets.len() == 1 && matches!(targets[0].expr, SqlExpr::Column(ref name) if name == "*") {
        return Ok(scope
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| TargetEntry {
                name: column.output_name.clone(),
                expr: Expr::Column(index),
                sql_type: scope.desc.columns[index].sql_type,
            })
            .collect());
    }

    targets
        .iter()
        .map(|item| {
            Ok(TargetEntry {
                name: item.output_name.clone(),
                expr: bind_expr_with_outer(
                    &item.expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                )?,
                sql_type: infer_sql_expr_type(
                    &item.expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                ),
            })
        })
        .collect()
}

#[allow(dead_code)]
pub(crate) fn bind_expr(expr: &SqlExpr, scope: &BoundScope) -> Result<Expr, ParseError> {
    bind_expr_with_outer(expr, scope, &Catalog::default(), &[], None)
}

pub(crate) fn bind_expr_with_outer(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<Expr, ParseError> {
    Ok(match expr {
        SqlExpr::Column(name) => {
            match resolve_column_with_outer(scope, outer_scopes, name, grouped_outer)? {
                ResolvedColumn::Local(index) => Expr::Column(index),
                ResolvedColumn::Outer { depth, index } => Expr::OuterColumn { depth, index },
            }
        }
        SqlExpr::Const(value) => Expr::Const(value.clone()),
        SqlExpr::IntegerLiteral(value) => Expr::Const(bind_integer_literal(value)?),
        SqlExpr::NumericLiteral(value) => Expr::Const(bind_numeric_literal(value)?),
        SqlExpr::Add(left, right) => bind_arithmetic_expr(
            "+",
            Expr::Add,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::Sub(left, right) => bind_arithmetic_expr(
            "-",
            Expr::Sub,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::Mul(left, right) => bind_arithmetic_expr(
            "*",
            Expr::Mul,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::Div(left, right) => bind_arithmetic_expr(
            "/",
            Expr::Div,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::Mod(left, right) => bind_arithmetic_expr(
            "%",
            Expr::Mod,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::Concat(left, right) => bind_concat_expr(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::UnaryPlus(inner) => Expr::UnaryPlus(Box::new(bind_expr_with_outer(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?)),
        SqlExpr::Negate(inner) => Expr::Negate(Box::new(bind_expr_with_outer(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?)),
        SqlExpr::Cast(inner, ty) => {
            let bound_inner = if let SqlExpr::ArrayLiteral(elements) = inner.as_ref() {
                Expr::ArrayLiteral {
                    elements: elements
                        .iter()
                        .map(|element| {
                            bind_expr_with_outer(
                                element,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                            )
                        })
                        .collect::<Result<_, _>>()?,
                    array_type: *ty,
                }
            } else {
                bind_expr_with_outer(inner, scope, catalog, outer_scopes, grouped_outer)?
            };
            Expr::Cast(Box::new(bound_inner), *ty)
        }
        SqlExpr::Eq(left, right) => bind_comparison_expr(
            Expr::Eq,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::NotEq(left, right) => bind_comparison_expr(
            Expr::NotEq,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::Lt(left, right) => bind_comparison_expr(
            Expr::Lt,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::LtEq(left, right) => bind_comparison_expr(
            Expr::LtEq,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::Gt(left, right) => bind_comparison_expr(
            Expr::Gt,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::GtEq(left, right) => bind_comparison_expr(
            Expr::GtEq,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::RegexMatch(left, right) => Expr::RegexMatch(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::And(left, right) => Expr::And(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::Or(left, right) => Expr::Or(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::Not(inner) => Expr::Not(Box::new(bind_expr_with_outer(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?)),
        SqlExpr::IsNull(inner) => Expr::IsNull(Box::new(bind_expr_with_outer(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?)),
        SqlExpr::IsNotNull(inner) => Expr::IsNotNull(Box::new(bind_expr_with_outer(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?)),
        SqlExpr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::ArrayLiteral(elements) => Expr::ArrayLiteral {
            elements: elements
                .iter()
                .map(|element| {
                    bind_expr_with_outer(element, scope, catalog, outer_scopes, grouped_outer)
                })
                .collect::<Result<_, _>>()?,
            array_type: infer_array_literal_type(
                elements,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?,
        },
        SqlExpr::ArrayOverlap(left, right) => Expr::ArrayOverlap(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::AggCall { .. } => {
            return Err(ParseError::UnexpectedToken {
                expected: "non-aggregate expression",
                actual: "aggregate function".into(),
            });
        }
        SqlExpr::ScalarSubquery(select) => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            let plan = build_plan_with_outer(select, catalog, &child_outer, None)?;
            ensure_single_column_subquery(&plan)?;
            Expr::ScalarSubquery(Box::new(plan))
        }
        SqlExpr::Exists(select) => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            Expr::ExistsSubquery(Box::new(build_plan_with_outer(
                select,
                catalog,
                &child_outer,
                None,
            )?))
        }
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            let subquery_plan = build_plan_with_outer(subquery, catalog, &child_outer, None)?;
            ensure_single_column_subquery(&subquery_plan)?;
            let any_expr = Expr::AnySubquery {
                left: Box::new(bind_expr_with_outer(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                )?),
                op: SubqueryComparisonOp::Eq,
                subquery: Box::new(subquery_plan),
            };
            if *negated {
                Expr::Not(Box::new(any_expr))
            } else {
                any_expr
            }
        }
        SqlExpr::QuantifiedSubquery {
            left,
            op,
            is_all,
            subquery,
        } => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            let subquery_plan = build_plan_with_outer(subquery, catalog, &child_outer, None)?;
            ensure_single_column_subquery(&subquery_plan)?;
            if *is_all {
                Expr::AllSubquery {
                    left: Box::new(bind_expr_with_outer(
                        left,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )?),
                    op: *op,
                    subquery: Box::new(subquery_plan),
                }
            } else {
                Expr::AnySubquery {
                    left: Box::new(bind_expr_with_outer(
                        left,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )?),
                    op: *op,
                    subquery: Box::new(subquery_plan),
                }
            }
        }
        SqlExpr::QuantifiedArray {
            left,
            op,
            is_all,
            array,
        } => {
            if *is_all {
                Expr::AllArray {
                    left: Box::new(bind_expr_with_outer(
                        left,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )?),
                    op: *op,
                    right: Box::new(bind_expr_with_outer(
                        array,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )?),
                }
            } else {
                Expr::AnyArray {
                    left: Box::new(bind_expr_with_outer(
                        left,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )?),
                    op: *op,
                    right: Box::new(bind_expr_with_outer(
                        array,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )?),
                }
            }
        }
        SqlExpr::Random => Expr::Random,
        SqlExpr::JsonGet(left, right) => Expr::JsonGet(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::JsonGetText(left, right) => Expr::JsonGetText(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::JsonPath(left, right) => Expr::JsonPath(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::JsonPathText(left, right) => Expr::JsonPathText(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::JsonbContains(left, right) => Expr::JsonbContains(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::JsonbContained(left, right) => Expr::JsonbContained(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::JsonbExists(left, right) => Expr::JsonbExists(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::JsonbExistsAny(left, right) => Expr::JsonbExistsAny(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::JsonbExistsAll(left, right) => Expr::JsonbExistsAll(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::JsonbPathExists(left, right) => Expr::JsonbPathExists(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::JsonbPathMatch(left, right) => Expr::JsonbPathMatch(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::FuncCall { name, args } => {
            let func =
                resolve_scalar_function(name).ok_or_else(|| ParseError::UnexpectedToken {
                    expected: "supported builtin function",
                    actual: name.clone(),
                })?;
            validate_scalar_function_arity(func, args)?;
            bind_scalar_function_call(func, args, scope, catalog, outer_scopes, grouped_outer)?
        }
        SqlExpr::CurrentTimestamp => Expr::CurrentTimestamp,
    })
}

fn bind_scalar_function_call(
    func: BuiltinScalarFunction,
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<Expr, ParseError> {
    let bound_args = args
        .iter()
        .map(|arg| bind_expr_with_outer(arg, scope, catalog, outer_scopes, grouped_outer))
        .collect::<Result<Vec<_>, _>>()?;
    match func {
        BuiltinScalarFunction::Left | BuiltinScalarFunction::Repeat => {
            let left_type = infer_sql_expr_type(&args[0], scope, catalog, outer_scopes, grouped_outer);
            let right_type = infer_sql_expr_type(&args[1], scope, catalog, outer_scopes, grouped_outer);
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
            Ok(Expr::FuncCall {
                func,
                args: vec![
                    coerce_bound_expr(bound_args[0].clone(), left_type, SqlType::new(SqlTypeKind::Text)),
                    coerce_bound_expr(bound_args[1].clone(), right_type, SqlType::new(SqlTypeKind::Int4)),
                ],
            })
        }
        _ => Ok(Expr::FuncCall {
            func,
            args: bound_args,
        }),
    }
}

fn bind_arithmetic_expr(
    op: &'static str,
    make: fn(Box<Expr>, Box<Expr>) -> Expr,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<Expr, ParseError> {
    let left_type = infer_sql_expr_type(left, scope, catalog, outer_scopes, grouped_outer);
    let right_type = infer_sql_expr_type(right, scope, catalog, outer_scopes, grouped_outer);
    let common = resolve_numeric_binary_type(op, left_type, right_type)?;
    let left = coerce_bound_expr(
        bind_expr_with_outer(left, scope, catalog, outer_scopes, grouped_outer)?,
        left_type,
        common,
    );
    let right = coerce_bound_expr(
        bind_expr_with_outer(right, scope, catalog, outer_scopes, grouped_outer)?,
        right_type,
        common,
    );
    Ok(make(Box::new(left), Box::new(right)))
}

fn bind_comparison_expr(
    make: fn(Box<Expr>, Box<Expr>) -> Expr,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<Expr, ParseError> {
    let left_type = infer_sql_expr_type(left, scope, catalog, outer_scopes, grouped_outer);
    let right_type = infer_sql_expr_type(right, scope, catalog, outer_scopes, grouped_outer);
    let left_bound = bind_expr_with_outer(left, scope, catalog, outer_scopes, grouped_outer)?;
    let right_bound = bind_expr_with_outer(right, scope, catalog, outer_scopes, grouped_outer)?;
    let (left, right) = if is_numeric_family(left_type) && is_numeric_family(right_type) {
        let common = resolve_numeric_binary_type("=", left_type, right_type)?;
        (
            coerce_bound_expr(left_bound, left_type, common),
            coerce_bound_expr(right_bound, right_type, common),
        )
    } else {
        (left_bound, right_bound)
    };
    Ok(make(Box::new(left), Box::new(right)))
}

fn bind_concat_expr(
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<Expr, ParseError> {
    let left_type = infer_sql_expr_type(left, scope, catalog, outer_scopes, grouped_outer);
    let right_type = infer_sql_expr_type(right, scope, catalog, outer_scopes, grouped_outer);
    let left_bound = bind_expr_with_outer(left, scope, catalog, outer_scopes, grouped_outer)?;
    let right_bound = bind_expr_with_outer(right, scope, catalog, outer_scopes, grouped_outer)?;
    bind_concat_operands(left, left_type, left_bound, right, right_type, right_bound)
}

fn bind_concat_operands(
    left_sql: &SqlExpr,
    left_type: SqlType,
    left_bound: Expr,
    right_sql: &SqlExpr,
    right_type: SqlType,
    right_bound: Expr,
) -> Result<Expr, ParseError> {
    if left_type.kind == SqlTypeKind::Jsonb
        && !left_type.is_array
        && right_type.kind == SqlTypeKind::Jsonb
        && !right_type.is_array
    {
        return Ok(Expr::Concat(Box::new(left_bound), Box::new(right_bound)));
    }

    if left_type.is_array || right_type.is_array {
        let element_type = resolve_array_concat_element_type(left_type, right_type)?;
        let left_expr = if left_type.is_array {
            coerce_bound_expr(left_bound, left_type, SqlType::array_of(element_type))
        } else {
            coerce_bound_expr(left_bound, left_type, element_type)
        };
        let right_expr = if right_type.is_array {
            coerce_bound_expr(right_bound, right_type, SqlType::array_of(element_type))
        } else {
            coerce_bound_expr(right_bound, right_type, element_type)
        };
        return Ok(Expr::Concat(Box::new(left_expr), Box::new(right_expr)));
    }

    if should_use_text_concat(left_sql, left_type, right_sql, right_type) {
        let text_type = SqlType::new(SqlTypeKind::Text);
        let left_expr = coerce_bound_expr(left_bound, left_type, text_type);
        let right_expr = coerce_bound_expr(right_bound, right_type, text_type);
        return Ok(Expr::Concat(Box::new(left_expr), Box::new(right_expr)));
    }

    Err(ParseError::UndefinedOperator {
        op: "||",
        left_type: sql_type_name(left_type),
        right_type: sql_type_name(right_type),
    })
}

pub(super) fn infer_sql_expr_type(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> SqlType {
    match expr {
        SqlExpr::Column(name) => {
            match resolve_column_with_outer(scope, outer_scopes, name, grouped_outer) {
                Ok(ResolvedColumn::Local(idx)) => scope.desc.columns.get(idx).map(|c| c.sql_type),
                Ok(ResolvedColumn::Outer { depth, index }) => outer_scopes
                    .get(depth)
                    .and_then(|s| s.desc.columns.get(index).map(|c| c.sql_type)),
                Err(_) => None,
            }
            .unwrap_or(SqlType::new(SqlTypeKind::Text))
        }
        SqlExpr::Const(Value::Int16(_)) => SqlType::new(SqlTypeKind::Int2),
        SqlExpr::Const(Value::Int32(_)) => SqlType::new(SqlTypeKind::Int4),
        SqlExpr::Const(Value::Int64(_)) => SqlType::new(SqlTypeKind::Int8),
        SqlExpr::Const(Value::Bool(_)) => SqlType::new(SqlTypeKind::Bool),
        SqlExpr::Const(Value::Numeric(_)) => SqlType::new(SqlTypeKind::Numeric),
        SqlExpr::Const(Value::Json(_)) => SqlType::new(SqlTypeKind::Json),
        SqlExpr::Const(Value::Jsonb(_)) => SqlType::new(SqlTypeKind::Jsonb),
        SqlExpr::Const(Value::JsonPath(_)) => SqlType::new(SqlTypeKind::JsonPath),
        SqlExpr::Const(Value::Text(_))
        | SqlExpr::Const(Value::TextRef(_, _))
        | SqlExpr::Const(Value::Null) => SqlType::new(SqlTypeKind::Text),
        SqlExpr::Const(Value::Array(_)) => SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
        SqlExpr::Const(Value::Float64(_)) => SqlType::new(SqlTypeKind::Float8),
        SqlExpr::IntegerLiteral(value) => infer_integer_literal_type(value),
        SqlExpr::NumericLiteral(_) => SqlType::new(SqlTypeKind::Numeric),
        SqlExpr::Add(left, right)
        | SqlExpr::Sub(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right)
        | SqlExpr::Mod(left, right) => infer_arithmetic_sql_type(
            expr,
            infer_sql_expr_type(left, scope, catalog, outer_scopes, grouped_outer),
            infer_sql_expr_type(right, scope, catalog, outer_scopes, grouped_outer),
        ),
        SqlExpr::Concat(left, right) => infer_concat_sql_type(
            expr,
            infer_sql_expr_type(left, scope, catalog, outer_scopes, grouped_outer),
            infer_sql_expr_type(right, scope, catalog, outer_scopes, grouped_outer),
        ),
        SqlExpr::UnaryPlus(inner) => {
            infer_sql_expr_type(inner, scope, catalog, outer_scopes, grouped_outer)
        }
        SqlExpr::Negate(inner) => {
            infer_sql_expr_type(inner, scope, catalog, outer_scopes, grouped_outer)
        }
        SqlExpr::Cast(_, ty) => *ty,
        SqlExpr::Eq(_, _)
        | SqlExpr::NotEq(_, _)
        | SqlExpr::Lt(_, _)
        | SqlExpr::LtEq(_, _)
        | SqlExpr::Gt(_, _)
        | SqlExpr::GtEq(_, _)
        | SqlExpr::RegexMatch(_, _)
        | SqlExpr::And(_, _)
        | SqlExpr::Or(_, _)
        | SqlExpr::Not(_)
        | SqlExpr::IsNull(_)
        | SqlExpr::IsNotNull(_)
        | SqlExpr::IsDistinctFrom(_, _)
        | SqlExpr::IsNotDistinctFrom(_, _)
        | SqlExpr::ArrayOverlap(_, _)
        | SqlExpr::JsonbContains(_, _)
        | SqlExpr::JsonbContained(_, _)
        | SqlExpr::JsonbExists(_, _)
        | SqlExpr::JsonbExistsAny(_, _)
        | SqlExpr::JsonbExistsAll(_, _)
        | SqlExpr::JsonbPathExists(_, _)
        | SqlExpr::JsonbPathMatch(_, _)
        | SqlExpr::QuantifiedArray { .. } => SqlType::new(SqlTypeKind::Bool),
        SqlExpr::JsonGet(left, _) | SqlExpr::JsonPath(left, _) => {
            let left_type = infer_sql_expr_type(left, scope, catalog, outer_scopes, grouped_outer);
            if matches!(left_type.element_type().kind, SqlTypeKind::Jsonb) {
                SqlType::new(SqlTypeKind::Jsonb)
            } else {
                SqlType::new(SqlTypeKind::Json)
            }
        }
        SqlExpr::JsonGetText(_, _) | SqlExpr::JsonPathText(_, _) => SqlType::new(SqlTypeKind::Text),
        SqlExpr::AggCall { func, args, .. } => aggregate_sql_type(
            *func,
            args.first()
                .map(|expr| infer_sql_expr_type(expr, scope, catalog, outer_scopes, grouped_outer)),
        ),
        SqlExpr::ArrayLiteral(elements) => {
            infer_array_literal_type(elements, scope, catalog, outer_scopes, grouped_outer)
                .unwrap_or(SqlType::array_of(SqlType::new(SqlTypeKind::Text)))
        }
        SqlExpr::ScalarSubquery(select) => {
            build_plan_with_outer(select, catalog, outer_scopes, grouped_outer.cloned())
                .ok()
                .and_then(|plan| {
                    let cols = plan.columns();
                    if cols.len() == 1 {
                        Some(cols[0].sql_type)
                    } else {
                        None
                    }
                })
                .unwrap_or(SqlType::new(SqlTypeKind::Text))
        }
        SqlExpr::Exists(_) | SqlExpr::InSubquery { .. } | SqlExpr::QuantifiedSubquery { .. } => {
            SqlType::new(SqlTypeKind::Bool)
        }
        SqlExpr::Random => SqlType::new(SqlTypeKind::Float8),
        SqlExpr::FuncCall { name, .. } => match resolve_scalar_function(name) {
            Some(BuiltinScalarFunction::Random) => SqlType::new(SqlTypeKind::Float8),
            Some(BuiltinScalarFunction::ToJson)
            | Some(BuiltinScalarFunction::ArrayToJson)
            | Some(BuiltinScalarFunction::JsonBuildArray)
            | Some(BuiltinScalarFunction::JsonBuildObject)
            | Some(BuiltinScalarFunction::JsonObject) => SqlType::new(SqlTypeKind::Json),
            Some(BuiltinScalarFunction::ToJsonb)
            | Some(BuiltinScalarFunction::JsonbExtractPath)
            | Some(BuiltinScalarFunction::JsonbBuildArray)
            | Some(BuiltinScalarFunction::JsonbBuildObject)
            | Some(BuiltinScalarFunction::JsonbPathQueryArray)
            | Some(BuiltinScalarFunction::JsonbPathQueryFirst) => SqlType::new(SqlTypeKind::Jsonb),
            Some(BuiltinScalarFunction::GetDatabaseEncoding)
            | Some(BuiltinScalarFunction::JsonTypeof)
            | Some(BuiltinScalarFunction::JsonExtractPathText)
            | Some(BuiltinScalarFunction::JsonbTypeof)
            | Some(BuiltinScalarFunction::JsonbExtractPathText)
            | Some(BuiltinScalarFunction::Left)
            | Some(BuiltinScalarFunction::Repeat) => SqlType::new(SqlTypeKind::Text),
            Some(BuiltinScalarFunction::JsonArrayLength)
            | Some(BuiltinScalarFunction::JsonbArrayLength) => SqlType::new(SqlTypeKind::Int4),
            Some(BuiltinScalarFunction::JsonbPathExists)
            | Some(BuiltinScalarFunction::JsonbPathMatch) => SqlType::new(SqlTypeKind::Bool),
            Some(BuiltinScalarFunction::JsonExtractPath) => SqlType::new(SqlTypeKind::Json),
            None => SqlType::new(SqlTypeKind::Text),
        },
        SqlExpr::CurrentTimestamp => SqlType::new(SqlTypeKind::Timestamp),
    }
}

pub(super) fn bind_agg_output_expr(
    expr: &SqlExpr,
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(AggFunc, Vec<SqlExpr>, bool)],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    for (i, gk) in group_by_exprs.iter().enumerate() {
        if gk == expr {
            return Ok(Expr::Column(i));
        }
    }

    match expr {
        SqlExpr::AggCall {
            func,
            args,
            distinct,
        } => {
            let entry = (*func, args.clone(), *distinct);
            for (i, agg) in agg_list.iter().enumerate() {
                if *agg == entry {
                    return Ok(Expr::Column(n_keys + i));
                }
            }
            Err(ParseError::UnexpectedToken {
                expected: "known aggregate",
                actual: format!("{}(...)", func.name()),
            })
        }
        SqlExpr::Column(name) => {
            let col_index =
                match resolve_column_with_outer(input_scope, outer_scopes, name, grouped_outer)? {
                    ResolvedColumn::Local(index) => index,
                    ResolvedColumn::Outer { depth, index } => {
                        return Ok(Expr::OuterColumn { depth, index });
                    }
                };
            for (i, gk) in group_by_exprs.iter().enumerate() {
                if let SqlExpr::Column(gk_name) = gk {
                    if let Ok(gk_index) = resolve_column(input_scope, gk_name) {
                        if gk_index == col_index {
                            return Ok(Expr::Column(i));
                        }
                    }
                }
            }
            Err(ParseError::UngroupedColumn(name.clone()))
        }
        SqlExpr::Const(v) => Ok(Expr::Const(v.clone())),
        SqlExpr::IntegerLiteral(value) => Ok(Expr::Const(bind_integer_literal(value)?)),
        SqlExpr::NumericLiteral(value) => Ok(Expr::Const(bind_numeric_literal(value)?)),
        SqlExpr::Add(l, r) => Ok(Expr::Add(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Sub(l, r) => Ok(Expr::Sub(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Mul(l, r) => Ok(Expr::Mul(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Div(l, r) => Ok(Expr::Div(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Mod(l, r) => Ok(Expr::Mod(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Concat(l, r) => {
            let left_expr = bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?;
            let right_expr = bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?;
            let left_type = infer_sql_expr_type(l, input_scope, catalog, outer_scopes, grouped_outer);
            let right_type =
                infer_sql_expr_type(r, input_scope, catalog, outer_scopes, grouped_outer);
            bind_concat_operands(l, left_type, left_expr, r, right_type, right_expr)
        }
        SqlExpr::UnaryPlus(inner) => Ok(Expr::UnaryPlus(Box::new(bind_agg_output_expr(
            inner,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?))),
        SqlExpr::Negate(inner) => Ok(Expr::Negate(Box::new(bind_agg_output_expr(
            inner,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?))),
        SqlExpr::Cast(inner, ty) => {
            let bound_inner = if let SqlExpr::ArrayLiteral(elements) = inner.as_ref() {
                Expr::ArrayLiteral {
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
                    array_type: *ty,
                }
            } else {
                bind_agg_output_expr(
                    inner,
                    group_by_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?
            };
            Ok(Expr::Cast(Box::new(bound_inner), *ty))
        }
        SqlExpr::Eq(l, r) => Ok(Expr::Eq(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::NotEq(l, r) => Ok(Expr::NotEq(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Lt(l, r) => Ok(Expr::Lt(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::LtEq(l, r) => Ok(Expr::LtEq(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Gt(l, r) => Ok(Expr::Gt(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::GtEq(l, r) => Ok(Expr::GtEq(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::RegexMatch(l, r) => Ok(Expr::RegexMatch(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::And(l, r) => Ok(Expr::And(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Or(l, r) => Ok(Expr::Or(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Not(inner) => Ok(Expr::Not(Box::new(bind_agg_output_expr(
            inner,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?))),
        SqlExpr::IsNull(inner) => Ok(Expr::IsNull(Box::new(bind_agg_output_expr(
            inner,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?))),
        SqlExpr::IsNotNull(inner) => Ok(Expr::IsNotNull(Box::new(bind_agg_output_expr(
            inner,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?))),
        SqlExpr::IsDistinctFrom(l, r) => Ok(Expr::IsDistinctFrom(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::IsNotDistinctFrom(l, r) => Ok(Expr::IsNotDistinctFrom(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::ArrayLiteral(elements) => Ok(Expr::ArrayLiteral {
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
        }),
        SqlExpr::ArrayOverlap(l, r) => Ok(Expr::ArrayOverlap(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonGet(l, r) => Ok(Expr::JsonGet(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonGetText(l, r) => Ok(Expr::JsonGetText(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonPath(l, r) => Ok(Expr::JsonPath(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonPathText(l, r) => Ok(Expr::JsonPathText(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbContains(l, r) => Ok(Expr::JsonbContains(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbContained(l, r) => Ok(Expr::JsonbContained(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbExists(l, r) => Ok(Expr::JsonbExists(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbExistsAny(l, r) => Ok(Expr::JsonbExistsAny(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbExistsAll(l, r) => Ok(Expr::JsonbExistsAll(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbPathExists(l, r) => Ok(Expr::JsonbPathExists(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbPathMatch(l, r) => Ok(Expr::JsonbPathMatch(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::ScalarSubquery(select) => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(input_scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            let plan = build_plan_with_outer(
                select,
                catalog,
                &child_outer,
                Some(GroupedOuterScope {
                    scope: input_scope.clone(),
                    group_by_exprs: group_by_exprs.to_vec(),
                }),
            )?;
            ensure_single_column_subquery(&plan)?;
            Ok(Expr::ScalarSubquery(Box::new(plan)))
        }
        SqlExpr::Exists(select) => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(input_scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            Ok(Expr::ExistsSubquery(Box::new(build_plan_with_outer(
                select,
                catalog,
                &child_outer,
                Some(GroupedOuterScope {
                    scope: input_scope.clone(),
                    group_by_exprs: group_by_exprs.to_vec(),
                }),
            )?)))
        }
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(input_scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            let subquery_plan = build_plan_with_outer(
                subquery,
                catalog,
                &child_outer,
                Some(GroupedOuterScope {
                    scope: input_scope.clone(),
                    group_by_exprs: group_by_exprs.to_vec(),
                }),
            )?;
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
            if *negated {
                Ok(Expr::Not(Box::new(any)))
            } else {
                Ok(any)
            }
        }
        SqlExpr::QuantifiedSubquery {
            left,
            op,
            is_all,
            subquery,
        } => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(input_scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            let subquery_plan = build_plan_with_outer(
                subquery,
                catalog,
                &child_outer,
                Some(GroupedOuterScope {
                    scope: input_scope.clone(),
                    group_by_exprs: group_by_exprs.to_vec(),
                }),
            )?;
            ensure_single_column_subquery(&subquery_plan)?;
            if *is_all {
                Ok(Expr::AllSubquery {
                    left: Box::new(bind_agg_output_expr(
                        left,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                    op: *op,
                    subquery: Box::new(subquery_plan),
                })
            } else {
                Ok(Expr::AnySubquery {
                    left: Box::new(bind_agg_output_expr(
                        left,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                    op: *op,
                    subquery: Box::new(subquery_plan),
                })
            }
        }
        SqlExpr::QuantifiedArray {
            left,
            op,
            is_all,
            array,
        } => {
            if *is_all {
                Ok(Expr::AllArray {
                    left: Box::new(bind_agg_output_expr(
                        left,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                    op: *op,
                    right: Box::new(bind_agg_output_expr(
                        array,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                })
            } else {
                Ok(Expr::AnyArray {
                    left: Box::new(bind_agg_output_expr(
                        left,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                    op: *op,
                    right: Box::new(bind_agg_output_expr(
                        array,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                })
            }
        }
        SqlExpr::Random => Ok(Expr::Random),
        SqlExpr::FuncCall { name, args } => {
            let func =
                resolve_scalar_function(name).ok_or_else(|| ParseError::UnexpectedToken {
                    expected: "supported builtin function",
                    actual: name.clone(),
                })?;
            validate_scalar_function_arity(func, args)?;
            let bound_args = args
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
                    let left_type =
                        infer_sql_expr_type(&args[0], input_scope, catalog, outer_scopes, grouped_outer);
                    let right_type =
                        infer_sql_expr_type(&args[1], input_scope, catalog, outer_scopes, grouped_outer);
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
                    Ok(Expr::FuncCall {
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
                    })
                }
                _ => Ok(Expr::FuncCall {
                    func,
                    args: bound_args,
                }),
            }
        }
        SqlExpr::CurrentTimestamp => Ok(Expr::CurrentTimestamp),
    }
}

fn infer_array_literal_type(
    elements: &[SqlExpr],
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<SqlType, ParseError> {
    for element in elements {
        if matches!(element, SqlExpr::Const(Value::Null)) {
            continue;
        }
        return Ok(SqlType::array_of(
            infer_sql_expr_type(element, scope, catalog, outer_scopes, grouped_outer)
                .element_type(),
        ));
    }
    Err(ParseError::UnexpectedToken {
        expected: "ARRAY[...] with a typed element or explicit cast",
        actual: "ARRAY[]".into(),
    })
}
