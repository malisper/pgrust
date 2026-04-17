use super::*;
use crate::include::catalog::{builtin_range_spec, range_kind_for_sql_type, sql_type_for_range_kind};
use crate::include::nodes::datum::RangeTypeId;

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

fn looks_like_range_literal_expr(expr: &SqlExpr) -> bool {
    let text = match expr {
        SqlExpr::Const(value) => match value.as_text() {
            Some(text) => text,
            None => return false,
        },
        _ => return false,
    };
    let trimmed = text.trim();
    trimmed.eq_ignore_ascii_case("empty")
        || matches!(trimmed.as_bytes().first().copied(), Some(b'[' | b'('))
}

fn range_declared_arg_types(kind: RangeTypeId, arity: usize) -> Vec<SqlType> {
    let spec = builtin_range_spec(kind);
    match arity {
        2 => vec![spec.sql_type, spec.sql_type],
        3 => vec![spec.subtype, spec.subtype, SqlType::new(SqlTypeKind::Text)],
        _ => unreachable!("unsupported range declared arity"),
    }
}

fn bind_range_call(
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
    Ok(Expr::builtin_func(func, Some(result_type), false, coerced_args))
}

fn bind_same_kind_range_binary(
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
    let left_type = coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
    let right_type = coerce_unknown_string_literal_type(right, raw_right_type, raw_left_type);
    let Some(left_kind) = range_kind_for_sql_type(left_type) else {
        return Err(ParseError::UndefinedOperator {
            op,
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        });
    };
    let Some(right_kind) = range_kind_for_sql_type(right_type) else {
        return Err(ParseError::UndefinedOperator {
            op,
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        });
    };
    if left_kind != right_kind {
        return Err(ParseError::UndefinedOperator {
            op,
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        });
    }
    bind_range_call(
        func,
        &[left, right],
        &range_declared_arg_types(left_kind, 2),
        result_type,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )
}

pub(super) fn bind_maybe_range_arithmetic(
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
    let left_kind = range_kind_for_sql_type(left_type);
    let right_kind = range_kind_for_sql_type(right_type);
    if left_kind.is_none() && right_kind.is_none() {
        return None;
    }
    let func = match op {
        "+" => BuiltinScalarFunction::RangeUnion,
        "-" => BuiltinScalarFunction::RangeDifference,
        "*" => BuiltinScalarFunction::RangeIntersect,
        _ => return None,
    };
    let result_type = left_kind
        .or(right_kind)
        .map(sql_type_for_range_kind)
        .unwrap_or(SqlType::new(SqlTypeKind::Text));
    Some(bind_same_kind_range_binary(
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

pub(super) fn bind_maybe_range_comparison(
    op: &'static str,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<Result<Expr, ParseError>> {
    let raw_left_type =
        infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let raw_right_type =
        infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let left_type = coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
    let right_type = coerce_unknown_string_literal_type(right, raw_right_type, raw_left_type);
    if range_kind_for_sql_type(left_type).is_none() && range_kind_for_sql_type(right_type).is_none() {
        return None;
    }
    let Some(left_kind) = range_kind_for_sql_type(left_type) else {
        return Some(Err(ParseError::UndefinedOperator {
            op,
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        }));
    };
    let Some(right_kind) = range_kind_for_sql_type(right_type) else {
        return Some(Err(ParseError::UndefinedOperator {
            op,
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        }));
    };
    if left_kind != right_kind {
        return Some(Err(ParseError::UndefinedOperator {
            op,
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        }));
    }
    let left_bound = match bind_expr_with_outer_and_ctes(
        left,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ) {
        Ok(expr) => expr,
        Err(err) => return Some(Err(err)),
    };
    let right_bound = match bind_expr_with_outer_and_ctes(
        right,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ) {
        Ok(expr) => expr,
        Err(err) => return Some(Err(err)),
    };
    let range_type = sql_type_for_range_kind(left_kind);
    Some(Ok(Expr::op_auto(
        match op {
            "=" => crate::include::nodes::primnodes::OpExprKind::Eq,
            "<>" => crate::include::nodes::primnodes::OpExprKind::NotEq,
            "<" => crate::include::nodes::primnodes::OpExprKind::Lt,
            "<=" => crate::include::nodes::primnodes::OpExprKind::LtEq,
            ">" => crate::include::nodes::primnodes::OpExprKind::Gt,
            ">=" => crate::include::nodes::primnodes::OpExprKind::GtEq,
            _ => return Some(Err(ParseError::UndefinedOperator {
                op,
                left_type: sql_type_name(left_type),
                right_type: sql_type_name(right_type),
            })),
        },
        vec![
            coerce_bound_expr(left_bound, raw_left_type, range_type),
            coerce_bound_expr(right_bound, raw_right_type, range_type),
        ],
    )))
}

pub(super) fn bind_maybe_range_shift(
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
    if range_kind_for_sql_type(left_type).is_none() && range_kind_for_sql_type(right_type).is_none()
    {
        return None;
    }
    let func = match op {
        "<<" => BuiltinScalarFunction::RangeStrictLeft,
        ">>" => BuiltinScalarFunction::RangeStrictRight,
        _ => return None,
    };
    Some(bind_same_kind_range_binary(
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

pub(super) fn bind_maybe_range_overlap_or_adjacent(
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
    if range_kind_for_sql_type(left_type).is_none() && range_kind_for_sql_type(right_type).is_none()
    {
        return None;
    }
    let func = match op {
        "&&" => BuiltinScalarFunction::RangeOverlap,
        "-|-" => BuiltinScalarFunction::RangeAdjacent,
        _ => return None,
    };
    Some(bind_same_kind_range_binary(
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

pub(super) fn bind_maybe_range_contains(
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
    let left_kind = range_kind_for_sql_type(left_type);
    let right_kind = range_kind_for_sql_type(right_type);
    if left_kind.is_none() && right_kind.is_none() {
        return None;
    }

    match (left_kind, right_kind) {
        (Some(left_kind), Some(right_kind)) => Some(if left_kind == right_kind {
            bind_range_call(
                if op == "@>" {
                    BuiltinScalarFunction::RangeContains
                } else {
                    BuiltinScalarFunction::RangeContainedBy
                },
                &[left, right],
                &range_declared_arg_types(left_kind, 2),
                SqlType::new(SqlTypeKind::Bool),
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        } else {
            Err(ParseError::UndefinedOperator {
                op,
                left_type: sql_type_name(left_type),
                right_type: sql_type_name(right_type),
            })
        }),
        (Some(kind), None) if op == "@>" => {
            let spec = builtin_range_spec(kind);
            let target_type = if is_string_literal_expr(right) && looks_like_range_literal_expr(right) {
                spec.sql_type
            } else {
                spec.subtype
            };
            Some(bind_range_call(
                BuiltinScalarFunction::RangeContains,
                &[left, right],
                &[spec.sql_type, target_type],
                SqlType::new(SqlTypeKind::Bool),
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ))
        }
        (None, Some(kind)) if op == "<@" => {
            let spec = builtin_range_spec(kind);
            let target_type = if is_string_literal_expr(left) && looks_like_range_literal_expr(left) {
                spec.sql_type
            } else {
                spec.subtype
            };
            Some(bind_range_call(
                BuiltinScalarFunction::RangeContainedBy,
                &[left, right],
                &[target_type, spec.sql_type],
                SqlType::new(SqlTypeKind::Bool),
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ))
        }
        _ => Some(Err(ParseError::UndefinedOperator {
            op,
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        })),
    }
}

pub(super) fn bind_maybe_range_over_position(
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
    if range_kind_for_sql_type(left_type).is_none() && range_kind_for_sql_type(right_type).is_none()
    {
        return None;
    }
    let func = match op {
        "&<" => BuiltinScalarFunction::RangeOverLeft,
        "&>" => BuiltinScalarFunction::RangeOverRight,
        _ => return None,
    };
    Some(bind_same_kind_range_binary(
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

pub(super) fn infer_range_special_expr_type_with_ctes(
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
            range_kind_for_sql_type(left_type)
                .or_else(|| range_kind_for_sql_type(right_type))
                .map(sql_type_for_range_kind)
        }
        SqlExpr::Shl(left, right)
        | SqlExpr::Shr(left, right)
        | SqlExpr::Eq(left, right)
        | SqlExpr::NotEq(left, right)
        | SqlExpr::Lt(left, right)
        | SqlExpr::LtEq(left, right)
        | SqlExpr::Gt(left, right)
        | SqlExpr::GtEq(left, right)
        | SqlExpr::JsonbContains(left, right)
        | SqlExpr::JsonbContained(left, right) => {
            let left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
            let right_type =
                infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
            if range_kind_for_sql_type(left_type).is_some() || range_kind_for_sql_type(right_type).is_some() {
                Some(SqlType::new(SqlTypeKind::Bool))
            } else {
                None
            }
        }
        SqlExpr::BinaryOperator { op, left, right } if matches!(op.as_str(), "&&" | "-|-") => {
            let left_type = infer_arg_type(left, scope, catalog, outer_scopes, grouped_outer, ctes);
            let right_type =
                infer_arg_type(right, scope, catalog, outer_scopes, grouped_outer, ctes);
            if range_kind_for_sql_type(left_type).is_some() || range_kind_for_sql_type(right_type).is_some() {
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
            if range_kind_for_sql_type(left_type).is_some() || range_kind_for_sql_type(right_type).is_some() {
                Some(SqlType::new(SqlTypeKind::Bool))
            } else {
                None
            }
        }
        _ => None,
    }
}
