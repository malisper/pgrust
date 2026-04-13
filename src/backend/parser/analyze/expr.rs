use super::functions::*;
use super::infer::*;
use super::*;

mod ops;
mod targets;

pub(crate) use self::ops::bind_concat_operands;
use self::ops::{
    bind_arithmetic_expr, bind_bitwise_expr, bind_comparison_expr, bind_concat_expr,
    bind_shift_expr,
};
pub(crate) use self::targets::{
    BoundSelectTargets, bind_select_targets, select_targets_contain_set_returning_call,
};

#[allow(dead_code)]
pub(crate) fn bind_expr(expr: &SqlExpr, scope: &BoundScope) -> Result<Expr, ParseError> {
    bind_expr_with_outer(expr, scope, &Catalog::default(), &[], None)
}

pub(crate) fn bind_expr_with_outer(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<Expr, ParseError> {
    bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, &[])
}

pub(crate) fn bind_expr_with_outer_and_ctes(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    Ok(match expr {
        SqlExpr::Column(name) => {
            match resolve_column_with_outer(scope, outer_scopes, name, grouped_outer)? {
                ResolvedColumn::Local(index) => Expr::Column(index),
                ResolvedColumn::Outer { depth, index } => Expr::OuterColumn { depth, index },
            }
        }
        SqlExpr::Default => {
            return Err(ParseError::UnexpectedToken {
                expected: "expression",
                actual: "DEFAULT".into(),
            });
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
            ctes,
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
            ctes,
        )?,
        SqlExpr::BitAnd(left, right) => bind_bitwise_expr(
            "&",
            Expr::BitAnd,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::BitOr(left, right) => bind_bitwise_expr(
            "|",
            Expr::BitOr,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::BitXor(left, right) => bind_bitwise_expr(
            "#",
            Expr::BitXor,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::Shl(left, right) => bind_shift_expr(
            "<<",
            Expr::Shl,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::Shr(left, right) => bind_shift_expr(
            ">>",
            Expr::Shr,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
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
            ctes,
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
            ctes,
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
            ctes,
        )?,
        SqlExpr::Concat(left, right) => bind_concat_expr(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::UnaryPlus(inner) => Expr::UnaryPlus(Box::new(bind_expr_with_outer_and_ctes(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?)),
        SqlExpr::Negate(inner) => Expr::Negate(Box::new(bind_expr_with_outer_and_ctes(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?)),
        SqlExpr::BitNot(inner) => {
            let inner_type = infer_sql_expr_type_with_ctes(
                inner,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !is_integer_family(inner_type) && !is_bit_string_type(inner_type) {
                return Err(ParseError::UndefinedOperator {
                    op: "~",
                    left_type: sql_type_name(inner_type),
                    right_type: "unknown".to_string(),
                });
            }
            Expr::BitNot(Box::new(bind_expr_with_outer_and_ctes(
                inner,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?))
        }
        SqlExpr::Cast(inner, ty) => {
            let source_type = infer_sql_expr_type_with_ctes(
                inner,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let bound_inner = if let SqlExpr::ArrayLiteral(elements) = inner.as_ref() {
                Expr::ArrayLiteral {
                    elements: elements
                        .iter()
                        .map(|element| {
                            bind_expr_with_outer_and_ctes(
                                element,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            )
                        })
                        .collect::<Result<_, _>>()?,
                    array_type: *ty,
                }
            } else {
                bind_expr_with_outer_and_ctes(
                    inner,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            };
            validate_catalog_backed_explicit_cast(source_type, *ty, catalog)?;
            Expr::Cast(Box::new(bound_inner), *ty)
        }
        SqlExpr::Eq(left, right) => bind_comparison_expr(
            "=",
            Expr::Eq,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::NotEq(left, right) => bind_comparison_expr(
            "<>",
            Expr::NotEq,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::Lt(left, right) => bind_comparison_expr(
            "<",
            Expr::Lt,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::LtEq(left, right) => bind_comparison_expr(
            "<=",
            Expr::LtEq,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::Gt(left, right) => bind_comparison_expr(
            ">",
            Expr::Gt,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::GtEq(left, right) => bind_comparison_expr(
            ">=",
            Expr::GtEq,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::RegexMatch(left, right) => Expr::RegexMatch(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
        ),
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(bind_expr_with_outer_and_ctes(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            pattern: Box::new(bind_expr_with_outer_and_ctes(
                pattern,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            escape: match escape {
                Some(value) => Some(Box::new(bind_expr_with_outer_and_ctes(
                    value,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?)),
                None => None,
            },
            case_insensitive: *case_insensitive,
            negated: *negated,
        },
        SqlExpr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(bind_expr_with_outer_and_ctes(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            pattern: Box::new(bind_expr_with_outer_and_ctes(
                pattern,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            escape: match escape {
                Some(value) => Some(Box::new(bind_expr_with_outer_and_ctes(
                    value,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?)),
                None => None,
            },
            negated: *negated,
        },
        SqlExpr::And(left, right) => Expr::And(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
        ),
        SqlExpr::Or(left, right) => Expr::Or(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
        ),
        SqlExpr::Not(inner) => Expr::Not(Box::new(bind_expr_with_outer_and_ctes(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?)),
        SqlExpr::IsNull(inner) => Expr::IsNull(Box::new(bind_expr_with_outer_and_ctes(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?)),
        SqlExpr::IsNotNull(inner) => Expr::IsNotNull(Box::new(bind_expr_with_outer_and_ctes(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?)),
        SqlExpr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
        ),
        SqlExpr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
        ),
        SqlExpr::ArrayLiteral(elements) => Expr::ArrayLiteral {
            elements: elements
                .iter()
                .map(|element| {
                    bind_expr_with_outer_and_ctes(
                        element,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                })
                .collect::<Result<_, _>>()?,
            array_type: infer_array_literal_type_with_ctes(
                elements,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "ARRAY[...] with a typed element or explicit cast",
                actual: "ARRAY[]".into(),
            })?,
        },
        SqlExpr::ArrayOverlap(left, right) => Expr::ArrayOverlap(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
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
            let plan = build_plan_with_outer(select, catalog, &child_outer, None, ctes)?;
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
                ctes,
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
            let subquery_plan = build_plan_with_outer(subquery, catalog, &child_outer, None, ctes)?;
            ensure_single_column_subquery(&subquery_plan)?;
            let any_expr = Expr::AnySubquery {
                left: Box::new(bind_expr_with_outer_and_ctes(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
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
            let subquery_plan = build_plan_with_outer(subquery, catalog, &child_outer, None, ctes)?;
            ensure_single_column_subquery(&subquery_plan)?;
            if *is_all {
                Expr::AllSubquery {
                    left: Box::new(bind_expr_with_outer_and_ctes(
                        left,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?),
                    op: *op,
                    subquery: Box::new(subquery_plan),
                }
            } else {
                Expr::AnySubquery {
                    left: Box::new(bind_expr_with_outer_and_ctes(
                        left,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
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
                    left: Box::new(bind_expr_with_outer_and_ctes(
                        left,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?),
                    op: *op,
                    right: Box::new(bind_expr_with_outer_and_ctes(
                        array,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?),
                }
            } else {
                Expr::AnyArray {
                    left: Box::new(bind_expr_with_outer_and_ctes(
                        left,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?),
                    op: *op,
                    right: Box::new(bind_expr_with_outer_and_ctes(
                        array,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?),
                }
            }
        }
        SqlExpr::Random => Expr::Random,
        SqlExpr::JsonGet(left, right) => Expr::JsonGet(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
        ),
        SqlExpr::JsonGetText(left, right) => Expr::JsonGetText(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
        ),
        SqlExpr::JsonPath(left, right) => Expr::JsonPath(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
        ),
        SqlExpr::JsonPathText(left, right) => Expr::JsonPathText(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
        ),
        SqlExpr::JsonbContains(left, right) => Expr::JsonbContains(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
        ),
        SqlExpr::JsonbContained(left, right) => Expr::JsonbContained(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
        ),
        SqlExpr::JsonbExists(left, right) => Expr::JsonbExists(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
        ),
        SqlExpr::JsonbExistsAny(left, right) => Expr::JsonbExistsAny(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
        ),
        SqlExpr::JsonbExistsAll(left, right) => Expr::JsonbExistsAll(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
        ),
        SqlExpr::JsonbPathExists(left, right) => Expr::JsonbPathExists(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
        ),
        SqlExpr::JsonbPathMatch(left, right) => Expr::JsonbPathMatch(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
        ),
        SqlExpr::FuncCall { name, args } => {
            if let Some(target_type) = resolve_function_cast_type(catalog, name) {
                if args.iter().any(|arg| arg.name.is_some()) {
                    return Err(ParseError::UnexpectedToken {
                        expected: "positional cast argument",
                        actual: format!("{name} with named arguments"),
                    });
                }
                if args.len() != 1 {
                    return Err(ParseError::UnexpectedToken {
                        expected: "single-argument cast function",
                        actual: format!("{name}({} args)", args.len()),
                    });
                }
                let arg_type = infer_sql_expr_type_with_ctes(
                    &args[0].value,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                let bound_arg = bind_expr_with_outer_and_ctes(
                    &args[0].value,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                validate_catalog_backed_explicit_cast(arg_type, target_type, catalog)?;
                return Ok(Expr::Cast(
                    Box::new(bound_arg),
                    if arg_type == target_type {
                        arg_type
                    } else {
                        target_type
                    },
                ));
            }
            let func =
                resolve_scalar_function(name).ok_or_else(|| ParseError::UnexpectedToken {
                    expected: "supported builtin function",
                    actual: name.clone(),
                })?;
            let lowered_args = lower_named_scalar_function_args(func, args)?;
            validate_scalar_function_arity(func, &lowered_args)?;
            bind_scalar_function_call(
                func,
                &lowered_args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?
        }
        SqlExpr::CurrentTimestamp => Expr::CurrentTimestamp,
    })
}

fn validate_catalog_backed_explicit_cast(
    source_type: SqlType,
    target_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    if source_type.element_type() == target_type.element_type() {
        return Ok(());
    }
    if source_type.is_array || !is_text_like_type(source_type) {
        return Ok(());
    }
    if explicit_text_input_cast_exists(catalog, target_type) {
        return Ok(());
    }
    Err(ParseError::UnexpectedToken {
        expected: "supported explicit cast",
        actual: format!(
            "cannot cast type {} to {}",
            sql_type_name(source_type),
            sql_type_name(target_type)
        ),
    })
}

fn bind_scalar_function_call(
    func: BuiltinScalarFunction,
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let bound_args = args
        .iter()
        .map(|arg| {
            bind_expr_with_outer_and_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes)
        })
        .collect::<Result<Vec<_>, _>>()?;
    match func {
        BuiltinScalarFunction::Left | BuiltinScalarFunction::Repeat => {
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
        BuiltinScalarFunction::Length => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !is_bit_string_type(arg_type)
                && !should_use_text_concat(&args[0], arg_type, &args[0], arg_type)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "text or bit argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            Ok(Expr::FuncCall {
                func,
                args: vec![bound_args[0].clone()],
            })
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
                return Ok(Expr::FuncCall {
                    func,
                    args: vec![
                        coerce_bound_expr(bound_args[0].clone(), left_type, common),
                        coerce_bound_expr(bound_args[1].clone(), right_type, common),
                    ],
                });
            }
            if left_type.kind == SqlTypeKind::Bytea && right_type.kind == SqlTypeKind::Bytea {
                return Ok(Expr::FuncCall {
                    func,
                    args: vec![bound_args[0].clone(), bound_args[1].clone()],
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
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ],
            })
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
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ],
            })
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
                return Ok(Expr::FuncCall {
                    func,
                    args: coerced,
                });
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
                return Ok(Expr::FuncCall {
                    func,
                    args: coerced,
                });
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
                    return Ok(Expr::FuncCall {
                        func: BuiltinScalarFunction::SimilarSubstring,
                        args: coerced,
                    });
                }
                return Ok(Expr::FuncCall {
                    func,
                    args: coerced,
                });
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
            Ok(Expr::FuncCall {
                func,
                args: coerced,
            })
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
            Ok(Expr::FuncCall {
                func,
                args: coerced,
            })
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
            if (!same_bit_kind && !same_bytea_kind) || !is_integer_family(start_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "overlay(bit, bit, int4[, int4]) or overlay(bytea, bytea, int4[, int4])",
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
            Ok(Expr::FuncCall {
                func,
                args: coerced,
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        index_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ],
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![
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
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![bound_args[0].clone()],
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        index_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ],
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![
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
            })
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
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ],
            })
        }
        BuiltinScalarFunction::Lower => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            Ok(Expr::FuncCall {
                func,
                args: vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Text),
                )],
            })
        }
        BuiltinScalarFunction::Initcap
        | BuiltinScalarFunction::Ascii
        | BuiltinScalarFunction::Replace
        | BuiltinScalarFunction::Translate => Ok(Expr::FuncCall {
            func,
            args: args
                .iter()
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
                    coerce_bound_expr(
                        bound_args[idx].clone(),
                        ty,
                        SqlType::new(SqlTypeKind::Text),
                    )
                })
                .collect(),
        }),
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
            Ok(Expr::FuncCall {
                func,
                args: vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Int4),
                )],
            })
        }
        BuiltinScalarFunction::SplitPart => Ok(Expr::FuncCall {
            func,
            args: vec![
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
        }),
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
            Ok(Expr::FuncCall {
                func,
                args: coerced,
            })
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
            Ok(Expr::FuncCall {
                func,
                args: coerced,
            })
        }
        BuiltinScalarFunction::RegexpLike => Ok(Expr::FuncCall {
            func,
            args: args
                .iter()
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
        }),
        BuiltinScalarFunction::RegexpCount => Ok(Expr::FuncCall {
            func,
            args: bind_regex_count_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        }),
        BuiltinScalarFunction::RegexpInstr => Ok(Expr::FuncCall {
            func,
            args: bind_regex_instr_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        }),
        BuiltinScalarFunction::RegexpSubstr => Ok(Expr::FuncCall {
            func,
            args: bind_regex_substr_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        }),
        BuiltinScalarFunction::RegexpReplace => Ok(Expr::FuncCall {
            func,
            args: bind_regex_replace_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        }),
        BuiltinScalarFunction::RegexpSplitToArray => Ok(Expr::FuncCall {
            func,
            args: bind_regex_split_to_array_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        }),
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
            Ok(Expr::FuncCall {
                func,
                args: vec![bound_args[0].clone()],
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![bound_args[0].clone()],
            })
        }
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
            Ok(Expr::FuncCall {
                func,
                args: vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        format_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ],
            })
        }
        BuiltinScalarFunction::Decode => Ok(Expr::FuncCall {
            func,
            args: vec![
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
        }),
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
            Ok(Expr::FuncCall {
                func,
                args: vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        format_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ],
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Numeric),
                )],
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![coerce_bound_expr(bound_args[0].clone(), arg_type, target)],
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![
                    coerce_bound_expr(bound_args[0].clone(), left_type, target),
                    coerce_bound_expr(bound_args[1].clone(), right_type, target),
                ],
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![
                    coerce_bound_expr(bound_args[0].clone(), left_type, common),
                    coerce_bound_expr(bound_args[1].clone(), right_type, common),
                ],
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![
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
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Numeric),
                )],
            })
        }
        BuiltinScalarFunction::WidthBucket => {
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
            let operand_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_operand_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            let low_type = coerce_unknown_string_literal_type(&args[1], raw_low_type, operand_type);
            let high_type =
                coerce_unknown_string_literal_type(&args[2], raw_high_type, operand_type);
            if !is_numeric_family(operand_type)
                || !is_numeric_family(low_type)
                || !is_numeric_family(high_type)
                || !is_integer_family(count_type)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "width_bucket(numeric, numeric, numeric, int4)",
                    actual: format!(
                        "{func:?}({}, {}, {}, {})",
                        sql_type_name(operand_type),
                        sql_type_name(low_type),
                        sql_type_name(high_type),
                        sql_type_name(count_type)
                    ),
                });
            }
            let target = if matches!(
                operand_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) || matches!(
                low_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) || matches!(
                high_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) {
                SqlType::new(SqlTypeKind::Float8)
            } else {
                SqlType::new(SqlTypeKind::Numeric)
            };
            Ok(Expr::FuncCall {
                func,
                args: vec![
                    coerce_bound_expr(bound_args[0].clone(), operand_type, target),
                    coerce_bound_expr(bound_args[1].clone(), low_type, target),
                    coerce_bound_expr(bound_args[2].clone(), high_type, target),
                    coerce_bound_expr(
                        bound_args[3].clone(),
                        count_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ],
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![
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
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![coerce_bound_expr(bound_args[0].clone(), arg_type, target)],
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![coerce_bound_expr(bound_args[0].clone(), arg_type, target)],
            })
        }
        BuiltinScalarFunction::Ceil
        | BuiltinScalarFunction::Ceiling
        | BuiltinScalarFunction::Floor
        | BuiltinScalarFunction::Sign
        | BuiltinScalarFunction::Sqrt
        | BuiltinScalarFunction::Cbrt
        | BuiltinScalarFunction::Exp
        | BuiltinScalarFunction::Ln
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
            Ok(Expr::FuncCall {
                func,
                args: vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Float8),
                )],
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![bound_args[0].clone()],
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![bound_args[0].clone()],
            })
        }
        BuiltinScalarFunction::Power | BuiltinScalarFunction::Atan2d => {
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
            Ok(Expr::FuncCall {
                func,
                args: vec![
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
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    target_type,
                )],
            })
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
            Ok(Expr::FuncCall {
                func,
                args: vec![
                    coerce_bound_expr(bound_args[0].clone(), left_type, text_type),
                    coerce_bound_expr(bound_args[1].clone(), right_type, text_type),
                ],
            })
        }
        _ => Ok(Expr::FuncCall {
            func,
            args: bound_args,
        }),
    }
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
        let ty = infer_sql_expr_type_with_ctes(
            arg,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        );
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
        let ty = infer_sql_expr_type_with_ctes(
            arg,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        );
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
        let ty = infer_sql_expr_type_with_ctes(
            arg,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        );
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
        let ty = infer_sql_expr_type_with_ctes(
            arg,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        );
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
        let ty = infer_sql_expr_type_with_ctes(
            arg,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        );
        out.push(coerce_bound_expr(
            bound_args[idx].clone(),
            ty,
            SqlType::new(SqlTypeKind::Text),
        ));
    }
    out
}
