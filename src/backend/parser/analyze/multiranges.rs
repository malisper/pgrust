use super::*;
use crate::include::catalog::{multirange_type_ref_for_sql_type, range_type_ref_for_sql_type};
use crate::include::nodes::datum::MultirangeTypeRef;
use crate::include::nodes::primnodes::OpExprKind;

fn infer_arg_type(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> SqlType {
    infer_sql_expr_type_with_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)
}

fn is_string_literal_expr(expr: &SqlExpr) -> bool {
    matches!(
        expr,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    )
}

fn literal_text(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Const(value) => value.as_text(),
        _ => None,
    }
}

fn looks_like_range_literal_expr(expr: &SqlExpr) -> bool {
    let Some(text) = literal_text(expr) else {
        return false;
    };
    let trimmed = text.trim();
    trimmed.eq_ignore_ascii_case("empty")
        || matches!(trimmed.as_bytes().first().copied(), Some(b'[' | b'('))
}

fn looks_like_multirange_literal_expr(expr: &SqlExpr) -> bool {
    literal_text(expr)
        .map(|text| text.trim().starts_with('{'))
        .unwrap_or(false)
}

fn bind_multirange_call(
    func: BuiltinScalarFunction,
    args: &[&SqlExpr],
    declared_arg_types: &[SqlType],
    result_type: SqlType,
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
    let coerced_args = bound_args
        .into_iter()
        .zip(arg_types)
        .zip(declared_arg_types.iter().copied())
        .map(|((arg, actual_type), declared_type)| {
            coerce_bound_expr(arg, actual_type, declared_type)
        })
        .collect();
    Ok(Expr::builtin_func(
        func,
        Some(result_type),
        false,
        coerced_args,
    ))
}

fn multirange_binary_target(
    expr: &SqlExpr,
    actual_type: SqlType,
    multirange_type: MultirangeTypeRef,
) -> Option<SqlType> {
    if let Some(actual_multirange) = multirange_type_ref_for_sql_type(actual_type) {
        return (actual_multirange.type_oid() == multirange_type.type_oid())
            .then_some(multirange_type.sql_type);
    }
    if let Some(actual_range) = range_type_ref_for_sql_type(actual_type) {
        return (actual_range.type_oid() == multirange_type.range_type.type_oid())
            .then_some(multirange_type.range_type.sql_type);
    }
    if is_string_literal_expr(expr) {
        if looks_like_multirange_literal_expr(expr) {
            return Some(multirange_type.sql_type);
        }
        if looks_like_range_literal_expr(expr) {
            return Some(multirange_type.range_type.sql_type);
        }
    }
    Some(multirange_type.range_type.subtype)
}

fn multirange_relation_target(
    expr: &SqlExpr,
    actual_type: SqlType,
    multirange_type: MultirangeTypeRef,
) -> Option<SqlType> {
    let target = multirange_binary_target(expr, actual_type, multirange_type)?;
    (target == multirange_type.sql_type || target == multirange_type.range_type.sql_type)
        .then_some(target)
}

fn bind_same_kind_multirange_binary(
    op: &'static str,
    func: BuiltinScalarFunction,
    result_type: SqlType,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let raw_left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let raw_right_type = infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let left_multirange = multirange_type_ref_for_sql_type(raw_left_type);
    let right_multirange = multirange_type_ref_for_sql_type(raw_right_type);
    let multirange_type = match (left_multirange, right_multirange) {
        (Some(left_multirange), Some(right_multirange))
            if left_multirange.type_oid() == right_multirange.type_oid() =>
        {
            left_multirange
        }
        (Some(left_multirange), None) if looks_like_multirange_literal_expr(right) => {
            left_multirange
        }
        (None, Some(right_multirange)) if looks_like_multirange_literal_expr(left) => {
            right_multirange
        }
        _ => {
            return Err(ParseError::UndefinedOperator {
                op: op.into(),
                left_type: sql_type_name(raw_left_type),
                right_type: sql_type_name(raw_right_type),
            });
        }
    };
    bind_multirange_call(
        func,
        &[left, right],
        &[multirange_type.sql_type, multirange_type.sql_type],
        result_type,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )
}

fn bind_multirange_relation_binary(
    op: &'static str,
    func: BuiltinScalarFunction,
    result_type: SqlType,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let raw_left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let raw_right_type = infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let left_multirange = multirange_type_ref_for_sql_type(raw_left_type);
    let right_multirange = multirange_type_ref_for_sql_type(raw_right_type);
    let (left_target, right_target) = if let Some(left_multirange) = left_multirange {
        let Some(right_target) = multirange_relation_target(right, raw_right_type, left_multirange)
        else {
            return Err(ParseError::UndefinedOperator {
                op: op.into(),
                left_type: sql_type_name(raw_left_type),
                right_type: sql_type_name(raw_right_type),
            });
        };
        (left_multirange.sql_type, right_target)
    } else if let Some(right_multirange) = right_multirange {
        let Some(left_target) = multirange_relation_target(left, raw_left_type, right_multirange)
        else {
            return Err(ParseError::UndefinedOperator {
                op: op.into(),
                left_type: sql_type_name(raw_left_type),
                right_type: sql_type_name(raw_right_type),
            });
        };
        (left_target, right_multirange.sql_type)
    } else {
        return Err(ParseError::UndefinedOperator {
            op: op.into(),
            left_type: sql_type_name(raw_left_type),
            right_type: sql_type_name(raw_right_type),
        });
    };
    bind_multirange_call(
        func,
        &[left, right],
        &[left_target, right_target],
        result_type,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )
}

pub(super) fn bind_maybe_multirange_arithmetic(
    op: &'static str,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<Result<Expr, ParseError>> {
    let left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let right_type = infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let left_multirange = multirange_type_ref_for_sql_type(left_type);
    let right_multirange = multirange_type_ref_for_sql_type(right_type);
    if left_multirange.is_none() && right_multirange.is_none() {
        return None;
    }
    let func = match op {
        "+" => BuiltinScalarFunction::RangeUnion,
        "-" => BuiltinScalarFunction::RangeDifference,
        "*" => BuiltinScalarFunction::RangeIntersect,
        _ => return None,
    };
    let result_type = left_multirange
        .or(right_multirange)
        .map(|multirange_type| multirange_type.sql_type)
        .unwrap_or(SqlType::new(SqlTypeKind::Text));
    Some(bind_same_kind_multirange_binary(
        op,
        func,
        result_type,
        left,
        right,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ))
}

pub(super) fn bind_maybe_multirange_comparison(
    op: &'static str,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<Result<Expr, ParseError>> {
    let raw_left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let raw_right_type = infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let left_multirange = multirange_type_ref_for_sql_type(raw_left_type);
    let right_multirange = multirange_type_ref_for_sql_type(raw_right_type);
    if left_multirange.is_none() && right_multirange.is_none() {
        return None;
    }
    Some(
        bind_same_kind_multirange_binary(
            op,
            BuiltinScalarFunction::RangeUnion,
            SqlType::new(SqlTypeKind::Bool),
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )
        .map(|expr| {
            let Expr::Func(func_expr) = expr else {
                unreachable!("multirange comparison lowering must build a builtin call")
            };
            let op_kind = match op {
                "=" => OpExprKind::Eq,
                "<>" => OpExprKind::NotEq,
                "<" => OpExprKind::Lt,
                "<=" => OpExprKind::LtEq,
                ">" => OpExprKind::Gt,
                ">=" => OpExprKind::GtEq,
                _ => unreachable!("unsupported multirange comparison operator"),
            };
            Expr::op_auto(op_kind, func_expr.args)
        }),
    )
}

pub(super) fn bind_maybe_multirange_overlap_or_adjacent(
    op: &'static str,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<Result<Expr, ParseError>> {
    let left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let right_type = infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    if multirange_type_ref_for_sql_type(left_type).is_none()
        && multirange_type_ref_for_sql_type(right_type).is_none()
    {
        return None;
    }
    let func = match op {
        "&&" => BuiltinScalarFunction::RangeOverlap,
        "-|-" => BuiltinScalarFunction::RangeAdjacent,
        _ => return None,
    };
    Some(bind_multirange_relation_binary(
        op,
        func,
        SqlType::new(SqlTypeKind::Bool),
        left,
        right,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ))
}

pub(super) fn bind_maybe_multirange_shift(
    op: &'static str,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<Result<Expr, ParseError>> {
    let left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let right_type = infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    if multirange_type_ref_for_sql_type(left_type).is_none()
        && multirange_type_ref_for_sql_type(right_type).is_none()
    {
        return None;
    }
    let func = match op {
        "<<" => BuiltinScalarFunction::RangeStrictLeft,
        ">>" => BuiltinScalarFunction::RangeStrictRight,
        _ => return None,
    };
    Some(bind_multirange_relation_binary(
        op,
        func,
        SqlType::new(SqlTypeKind::Bool),
        left,
        right,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ))
}

pub(super) fn bind_maybe_multirange_over_position(
    op: &'static str,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<Result<Expr, ParseError>> {
    let left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let right_type = infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    if multirange_type_ref_for_sql_type(left_type).is_none()
        && multirange_type_ref_for_sql_type(right_type).is_none()
    {
        return None;
    }
    let func = match op {
        "&<" => BuiltinScalarFunction::RangeOverLeft,
        "&>" => BuiltinScalarFunction::RangeOverRight,
        _ => return None,
    };
    Some(bind_multirange_relation_binary(
        op,
        func,
        SqlType::new(SqlTypeKind::Bool),
        left,
        right,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ))
}

pub(super) fn bind_maybe_multirange_contains(
    op: &'static str,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<Result<Expr, ParseError>> {
    let left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let right_type = infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    if multirange_type_ref_for_sql_type(left_type).is_none()
        && multirange_type_ref_for_sql_type(right_type).is_none()
    {
        return None;
    }
    let func = if op == "@>" {
        BuiltinScalarFunction::RangeContains
    } else {
        BuiltinScalarFunction::RangeContainedBy
    };
    Some(bind_multirange_call(
        func,
        &[left, right],
        &{
            let raw_left_type =
                infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
            let raw_right_type =
                infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
            if let Some(left_multirange) = multirange_type_ref_for_sql_type(raw_left_type) {
                let Some(right_target) =
                    multirange_binary_target(right, raw_right_type, left_multirange)
                else {
                    return Some(Err(ParseError::UndefinedOperator {
                        op: op.into(),
                        left_type: sql_type_name(raw_left_type),
                        right_type: sql_type_name(raw_right_type),
                    }));
                };
                vec![left_multirange.sql_type, right_target]
            } else if let Some(right_multirange) = multirange_type_ref_for_sql_type(raw_right_type)
            {
                let Some(left_target) =
                    multirange_binary_target(left, raw_left_type, right_multirange)
                else {
                    return Some(Err(ParseError::UndefinedOperator {
                        op: op.into(),
                        left_type: sql_type_name(raw_left_type),
                        right_type: sql_type_name(raw_right_type),
                    }));
                };
                vec![left_target, right_multirange.sql_type]
            } else {
                return Some(Err(ParseError::UndefinedOperator {
                    op: op.into(),
                    left_type: sql_type_name(raw_left_type),
                    right_type: sql_type_name(raw_right_type),
                }));
            }
        },
        SqlType::new(SqlTypeKind::Bool),
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ))
}

pub(super) fn infer_multirange_special_expr_type_with_ctes(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<SqlType> {
    match expr {
        SqlExpr::Add(left, right) | SqlExpr::Sub(left, right) | SqlExpr::Mul(left, right) => {
            let left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
            let right_type =
                infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
            multirange_type_ref_for_sql_type(left_type)
                .or_else(|| multirange_type_ref_for_sql_type(right_type))
                .map(|multirange_type| multirange_type.sql_type)
        }
        SqlExpr::Shl(left, right)
        | SqlExpr::Shr(left, right)
        | SqlExpr::Eq(left, right)
        | SqlExpr::NotEq(left, right)
        | SqlExpr::Lt(left, right)
        | SqlExpr::LtEq(left, right)
        | SqlExpr::Gt(left, right)
        | SqlExpr::GtEq(left, right)
        | SqlExpr::ArrayContains(left, right)
        | SqlExpr::ArrayContained(left, right)
        | SqlExpr::JsonbContains(left, right)
        | SqlExpr::JsonbContained(left, right) => {
            let left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
            let right_type =
                infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
            if multirange_type_ref_for_sql_type(left_type).is_some()
                || multirange_type_ref_for_sql_type(right_type).is_some()
            {
                Some(SqlType::new(SqlTypeKind::Bool))
            } else {
                None
            }
        }
        SqlExpr::BinaryOperator { op, left, right } if matches!(op.as_str(), "&&" | "-|-") => {
            let left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
            let right_type =
                infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
            if multirange_type_ref_for_sql_type(left_type).is_some()
                || multirange_type_ref_for_sql_type(right_type).is_some()
            {
                Some(SqlType::new(SqlTypeKind::Bool))
            } else {
                None
            }
        }
        SqlExpr::GeometryBinaryOp { op, left, right }
            if matches!(op, GeometryBinaryOp::OverLeft | GeometryBinaryOp::OverRight) =>
        {
            let left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
            let right_type =
                infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
            if multirange_type_ref_for_sql_type(left_type).is_some()
                || multirange_type_ref_for_sql_type(right_type).is_some()
            {
                Some(SqlType::new(SqlTypeKind::Bool))
            } else {
                None
            }
        }
        _ => None,
    }
}
