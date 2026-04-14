use super::functions::*;
use super::infer::*;
use super::*;
use crate::include::catalog::ANYOID;
use crate::include::nodes::primnodes::{BoolExprType, OpExprKind};

mod json;
mod ops;
mod subquery;
mod targets;

use self::json::{
    bind_json_binary_expr, bind_jsonb_contained_expr, bind_jsonb_contains_expr,
    bind_jsonb_exists_all_expr, bind_jsonb_exists_any_expr, bind_jsonb_exists_expr,
    bind_jsonb_path_binary_expr, bind_maybe_jsonb_delete,
};
pub(crate) use self::ops::bind_concat_operands;
use self::ops::{
    bind_arithmetic_expr, bind_bitwise_expr, bind_comparison_expr, bind_concat_expr,
    bind_overloaded_binary_expr, bind_prefix_operator_expr, bind_shift_expr,
};
use self::subquery::{
    bind_exists_subquery_expr, bind_in_subquery_expr, bind_quantified_array_expr,
    bind_quantified_subquery_expr, bind_scalar_subquery_expr,
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
        SqlExpr::BinaryOperator { op, left, right } => match op.as_str() {
            "@@" => bind_overloaded_binary_expr(
                "@@",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "&&" => bind_overloaded_binary_expr(
                "&&",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "bound builtin operator",
                    actual: format!("unsupported operator {op}"),
                });
            }
        },
        SqlExpr::Add(left, right) => {
            if let Some(result) = bind_maybe_geometry_arithmetic(
                "+",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_arithmetic_expr(
                    "+",
                    OpExprKind::Add,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Sub(left, right) => {
            if let Some(result) = bind_maybe_jsonb_delete(
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_arithmetic(
                "-",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_arithmetic_expr(
                    "-",
                    OpExprKind::Sub,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::BitAnd(left, right) => bind_bitwise_expr(
            "&",
            OpExprKind::BitAnd,
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
            OpExprKind::BitOr,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::BitXor(left, right) => {
            if let Some(result) = bind_maybe_geometry_arithmetic(
                "#",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_bitwise_expr(
                    "#",
                    OpExprKind::BitXor,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Shl(left, right) => {
            if let Some(result) = bind_maybe_geometry_shift(
                "<<",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_shift_expr(
                    "<<",
                    OpExprKind::Shl,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Shr(left, right) => {
            if let Some(result) = bind_maybe_geometry_shift(
                ">>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_shift_expr(
                    ">>",
                    OpExprKind::Shr,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Mul(left, right) => {
            if let Some(result) = bind_maybe_geometry_arithmetic(
                "*",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_arithmetic_expr(
                    "*",
                    OpExprKind::Mul,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Div(left, right) => {
            if let Some(result) = bind_maybe_geometry_arithmetic(
                "/",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_arithmetic_expr(
                    "/",
                    OpExprKind::Div,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Mod(left, right) => bind_arithmetic_expr(
            "%",
            OpExprKind::Mod,
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
        SqlExpr::UnaryPlus(inner) => Expr::op_auto(
            OpExprKind::UnaryPlus,
            vec![bind_expr_with_outer_and_ctes(
                inner,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?],
        ),
        SqlExpr::PrefixOperator { op, expr } => bind_prefix_operator_expr(
            op.as_str(),
            expr,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::Negate(inner) => Expr::op_auto(
            OpExprKind::Negate,
            vec![bind_expr_with_outer_and_ctes(
                inner,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?],
        ),
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
            Expr::op_auto(
                OpExprKind::BitNot,
                vec![bind_expr_with_outer_and_ctes(
                    inner,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?],
            )
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
        SqlExpr::Eq(left, right) => {
            if let Some(result) = bind_maybe_geometry_comparison(
                "=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_comparison_expr(
                    "=",
                    OpExprKind::Eq,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::NotEq(left, right) => {
            if let Some(result) = bind_maybe_geometry_comparison(
                "<>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_comparison_expr(
                    "<>",
                    OpExprKind::NotEq,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Lt(left, right) => {
            if let Some(result) = bind_maybe_geometry_comparison(
                "<",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_comparison_expr(
                    "<",
                    OpExprKind::Lt,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::LtEq(left, right) => {
            if let Some(result) = bind_maybe_geometry_comparison(
                "<=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_comparison_expr(
                    "<=",
                    OpExprKind::LtEq,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Gt(left, right) => {
            if let Some(result) = bind_maybe_geometry_comparison(
                ">",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_comparison_expr(
                    ">",
                    OpExprKind::Gt,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::GtEq(left, right) => {
            if let Some(result) = bind_maybe_geometry_comparison(
                ">=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_comparison_expr(
                    ">=",
                    OpExprKind::GtEq,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::RegexMatch(left, right) => Expr::op_auto(
            OpExprKind::RegexMatch,
            vec![
                bind_expr_with_outer_and_ctes(
                    left,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
                bind_expr_with_outer_and_ctes(
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
            ],
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
        SqlExpr::And(left, right) => Expr::bool_expr(
            BoolExprType::And,
            vec![
                bind_expr_with_outer_and_ctes(
                    left,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
                bind_expr_with_outer_and_ctes(
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
            ],
        ),
        SqlExpr::Or(left, right) => Expr::bool_expr(
            BoolExprType::Or,
            vec![
                bind_expr_with_outer_and_ctes(
                    left,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
                bind_expr_with_outer_and_ctes(
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
            ],
        ),
        SqlExpr::Not(inner) => Expr::bool_expr(
            BoolExprType::Not,
            vec![bind_expr_with_outer_and_ctes(
                inner,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?],
        ),
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
        SqlExpr::ArraySubscript { array, subscripts } => {
            let array_type = infer_sql_expr_type_with_ctes(
                array,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !array_type.is_array {
                return Err(ParseError::UnexpectedToken {
                    expected: "array expression",
                    actual: sql_type_name(array_type).into(),
                });
            }
            Expr::ArraySubscript {
                array: Box::new(bind_expr_with_outer_and_ctes(
                    array,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?),
                subscripts: subscripts
                    .iter()
                    .map(|subscript| {
                        Ok(crate::include::nodes::plannodes::ExprArraySubscript {
                            is_slice: subscript.is_slice,
                            lower: subscript
                                .lower
                                .as_deref()
                                .map(|expr| {
                                    bind_expr_with_outer_and_ctes(
                                        expr,
                                        scope,
                                        catalog,
                                        outer_scopes,
                                        grouped_outer,
                                        ctes,
                                    )
                                })
                                .transpose()?,
                            upper: subscript
                                .upper
                                .as_deref()
                                .map(|expr| {
                                    bind_expr_with_outer_and_ctes(
                                        expr,
                                        scope,
                                        catalog,
                                        outer_scopes,
                                        grouped_outer,
                                        ctes,
                                    )
                                })
                                .transpose()?,
                        })
                    })
                    .collect::<Result<_, ParseError>>()?,
            }
        }
        SqlExpr::ArrayOverlap(left, right) => {
            if let Some(result) = bind_maybe_geometry_comparison(
                "&&",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                let raw_left_type = infer_sql_expr_type_with_ctes(
                    left,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                let raw_right_type = infer_sql_expr_type_with_ctes(
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                let left_bound = bind_expr_with_outer_and_ctes(
                    left,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let right_bound = bind_expr_with_outer_and_ctes(
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let mut left_type =
                    coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
                let mut right_type =
                    coerce_unknown_string_literal_type(right, raw_right_type, left_type);
                let left_expr = if matches!(
                    &**left,
                    SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
                ) && !left_type.is_array
                {
                    if let Expr::ArrayLiteral { array_type, .. } = &right_bound {
                        left_type = *array_type;
                    }
                    coerce_bound_expr(left_bound, raw_left_type, left_type)
                } else {
                    coerce_bound_expr(left_bound, raw_left_type, left_type)
                };
                let right_expr = if matches!(
                    &**right,
                    SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
                ) && !right_type.is_array
                {
                    if let Expr::ArrayLiteral { array_type, .. } = &left_expr {
                        right_type = *array_type;
                    }
                    coerce_bound_expr(right_bound, raw_right_type, right_type)
                } else {
                    coerce_bound_expr(right_bound, raw_right_type, right_type)
                };
                Expr::op_auto(OpExprKind::ArrayOverlap, vec![left_expr, right_expr])
            }
        }
        SqlExpr::AggCall { .. } => {
            return Err(ParseError::UnexpectedToken {
                expected: "non-aggregate expression",
                actual: "aggregate function".into(),
            });
        }
        SqlExpr::ScalarSubquery(select) => {
            bind_scalar_subquery_expr(select, scope, catalog, outer_scopes, ctes)?
        }
        SqlExpr::Exists(select) => {
            bind_exists_subquery_expr(select, scope, catalog, outer_scopes, ctes)?
        }
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => bind_in_subquery_expr(
            expr,
            subquery,
            *negated,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::QuantifiedSubquery {
            left,
            op,
            is_all,
            subquery,
        } => bind_quantified_subquery_expr(
            left,
            *op,
            *is_all,
            subquery,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::QuantifiedArray {
            left,
            op,
            is_all,
            array,
        } => bind_quantified_array_expr(
            left,
            *op,
            *is_all,
            array,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::Random => Expr::Random,
        SqlExpr::JsonGet(left, right) => bind_json_binary_expr(
            OpExprKind::JsonGet,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonGetText(left, right) => bind_json_binary_expr(
            OpExprKind::JsonGetText,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonPath(left, right) => bind_json_binary_expr(
            OpExprKind::JsonPath,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonPathText(left, right) => bind_json_binary_expr(
            OpExprKind::JsonPathText,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonbContains(left, right) => bind_jsonb_contains_expr(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonbContained(left, right) => bind_jsonb_contained_expr(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonbExists(left, right) => bind_jsonb_exists_expr(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonbExistsAny(left, right) => bind_jsonb_exists_any_expr(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonbExistsAll(left, right) => bind_jsonb_exists_all_expr(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonbPathExists(left, right) => bind_jsonb_path_binary_expr(
            OpExprKind::JsonbPathExists,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonbPathMatch(left, right) => bind_jsonb_path_binary_expr(
            OpExprKind::JsonbPathMatch,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::FuncCall {
            name,
            args,
            func_variadic,
        } => {
            if name.eq_ignore_ascii_case("coalesce") {
                return bind_coalesce_call(args, scope, catalog, outer_scopes, grouped_outer, ctes);
            }
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
            let legacy_func =
                resolve_scalar_function(name).ok_or_else(|| ParseError::UnexpectedToken {
                    expected: "supported builtin function",
                    actual: name.clone(),
                })?;
            let lowered_args = lower_named_scalar_function_args(legacy_func, args)?;
            let actual_types = lowered_args
                .iter()
                .map(|arg| {
                    infer_sql_expr_type_with_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                })
                .collect::<Vec<_>>();
            if let Ok(resolved) =
                resolve_function_call(catalog, name, &actual_types, *func_variadic)
            {
                if resolved.prokind != 'f' || resolved.proretset || resolved.scalar_impl.is_none() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "supported builtin scalar function",
                        actual: name.clone(),
                    });
                }
                return bind_scalar_function_call(
                    resolved.scalar_impl.expect("scalar impl"),
                    resolved.proc_oid,
                    Some(resolved.result_type),
                    resolved.func_variadic,
                    resolved.nvargs,
                    resolved.vatype_oid,
                    &resolved.declared_arg_types,
                    &lowered_args,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            validate_scalar_function_arity(legacy_func, &lowered_args)?;
            bind_scalar_function_call(
                legacy_func,
                0,
                None,
                false,
                0,
                0,
                &actual_types,
                &lowered_args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?
        }
        SqlExpr::Subscript { expr, index } => bind_geometry_subscript(
            expr,
            *index,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::GeometryUnaryOp { op, expr } => {
            bind_geometry_unary_expr(*op, expr, scope, catalog, outer_scopes, grouped_outer, ctes)?
        }
        SqlExpr::GeometryBinaryOp { op, left, right } => bind_geometry_binary_expr(
            *op,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::FieldSelect { field, .. } => {
            return Err(ParseError::UnexpectedToken {
                expected: "scalar expression",
                actual: format!("field selection .{field} is not bound yet"),
            });
        }
        SqlExpr::CurrentDate => Expr::CurrentDate,
        SqlExpr::CurrentTime { precision } => Expr::CurrentTime {
            precision: *precision,
        },
        SqlExpr::CurrentTimestamp { precision } => Expr::CurrentTimestamp {
            precision: *precision,
        },
        SqlExpr::LocalTime { precision } => Expr::LocalTime {
            precision: *precision,
        },
        SqlExpr::LocalTimestamp { precision } => Expr::LocalTimestamp {
            precision: *precision,
        },
    })
}

fn bind_coalesce_call(
    args: &[SqlFunctionArg],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
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
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
        "COALESCE arguments with a common type",
    )?;
    let mut bound_args = Vec::with_capacity(lowered_args.len());
    for arg in &lowered_args {
        let arg_type =
            infer_sql_expr_type_with_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes);
        let bound =
            bind_expr_with_outer_and_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes)?;
        bound_args.push(coerce_bound_expr(bound, arg_type, common_type));
    }
    let mut iter = bound_args.into_iter().rev();
    let mut expr = iter.next().expect("coalesce arity validated");
    for arg in iter {
        expr = Expr::Coalesce(Box::new(arg), Box::new(expr));
    }
    Ok(expr)
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
    if target_type.is_array {
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
    func_oid: u32,
    result_type: Option<SqlType>,
    func_variadic: bool,
    nvargs: usize,
    vatype_oid: u32,
    _declared_arg_types: &[SqlType],
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let arg_types = args
        .iter()
        .map(|arg| {
            infer_sql_expr_type_with_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes)
        })
        .collect::<Vec<_>>();
    let bound_args = args
        .iter()
        .map(|arg| {
            bind_expr_with_outer_and_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes)
        })
        .collect::<Result<Vec<_>, _>>()?;
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
            Ok(build_func(func_variadic, vec![
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
                ]))
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
                return Ok(build_func(func_variadic, vec![
                        coerce_bound_expr(bound_args[0].clone(), left_type, common),
                        coerce_bound_expr(bound_args[1].clone(), right_type, common),
                    ]));
            }
            if left_type.kind == SqlTypeKind::Bytea && right_type.kind == SqlTypeKind::Bytea {
                return Ok(build_func(func_variadic, vec![bound_args[0].clone(), bound_args[1].clone()]));
            }
            Ok(build_func(func_variadic, vec![
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
                ]))
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
            Ok(build_func(func_variadic, vec![
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
                ]))
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
            Ok(build_func(func_variadic, vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        index_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ]))
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
            Ok(build_func(func_variadic, vec![
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
                ]))
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
            Ok(build_func(func_variadic, vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        index_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ]))
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
            Ok(build_func(func_variadic, vec![
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
                ]))
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
            Ok(build_func(func_variadic, vec![
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
                ]))
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
            Ok(build_func(func_variadic, vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Text),
                )]))
        }
        BuiltinScalarFunction::Initcap
        | BuiltinScalarFunction::Ascii
        | BuiltinScalarFunction::Replace
        | BuiltinScalarFunction::Translate => Ok(build_func(func_variadic, args
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
                    coerce_bound_expr(bound_args[idx].clone(), ty, SqlType::new(SqlTypeKind::Text))
                })
                .collect())),
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
            Ok(build_func(func_variadic, vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Int4),
                )]))
        }
        BuiltinScalarFunction::SplitPart => Ok(build_func(func_variadic, vec![
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
            ])),
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
        BuiltinScalarFunction::RegexpMatch | BuiltinScalarFunction::RegexpLike => {
            Ok(build_func(func_variadic, args
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
                    .collect()))
        }
        BuiltinScalarFunction::RegexpCount => Ok(build_func(func_variadic, bind_regex_count_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ))),
        BuiltinScalarFunction::RegexpInstr => Ok(build_func(func_variadic, bind_regex_instr_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ))),
        BuiltinScalarFunction::RegexpSubstr => Ok(build_func(func_variadic, bind_regex_substr_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ))),
        BuiltinScalarFunction::RegexpReplace => Ok(build_func(func_variadic, bind_regex_replace_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ))),
        BuiltinScalarFunction::RegexpSplitToArray => Ok(build_func(func_variadic, bind_regex_split_to_array_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ))),
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
            Ok(build_func(func_variadic, vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        format_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ]))
        }
        BuiltinScalarFunction::Decode => Ok(build_func(func_variadic, vec![
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
            ])),
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
            Ok(build_func(func_variadic, vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        format_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ]))
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
            Ok(build_func(func_variadic, vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Numeric),
                )]))
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
            Ok(build_func(func_variadic, vec![coerce_bound_expr(bound_args[0].clone(), arg_type, target)]))
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
            Ok(build_func(func_variadic, vec![
                    coerce_bound_expr(bound_args[0].clone(), left_type, target),
                    coerce_bound_expr(bound_args[1].clone(), right_type, target),
                ]))
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
            Ok(build_func(func_variadic, vec![
                    coerce_bound_expr(bound_args[0].clone(), left_type, common),
                    coerce_bound_expr(bound_args[1].clone(), right_type, common),
                ]))
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
            Ok(build_func(func_variadic, vec![
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
                ]))
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
            Ok(build_func(func_variadic, vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Numeric),
                )]))
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
            Ok(build_func(func_variadic, vec![
                    coerce_bound_expr(bound_args[0].clone(), operand_type, target),
                    coerce_bound_expr(bound_args[1].clone(), low_type, target),
                    coerce_bound_expr(bound_args[2].clone(), high_type, target),
                    coerce_bound_expr(
                        bound_args[3].clone(),
                        count_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ]))
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
            Ok(build_func(func_variadic, vec![
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
                ]))
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
            Ok(build_func(func_variadic, vec![coerce_bound_expr(bound_args[0].clone(), arg_type, target)]))
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
            Ok(build_func(func_variadic, vec![coerce_bound_expr(bound_args[0].clone(), arg_type, target)]))
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
            Ok(build_func(func_variadic, vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Float8),
                )]))
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
            Ok(build_func(func_variadic, vec![
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
                ]))
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
            Ok(build_func(func_variadic, vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    target_type,
                )]))
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
            Ok(build_func(func_variadic, vec![
                    coerce_bound_expr(bound_args[0].clone(), left_type, text_type),
                    coerce_bound_expr(bound_args[1].clone(), right_type, text_type),
                ]))
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
            Ok(build_func(func_variadic, vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        dim_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ]))
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
