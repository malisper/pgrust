use super::*;
use super::functions::*;
use super::infer::*;

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

pub(super) fn bind_concat_operands(
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
