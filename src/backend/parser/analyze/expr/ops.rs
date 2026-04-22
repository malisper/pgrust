use super::*;
use crate::backend::parser::analyze::multiranges::bind_maybe_multirange_overlap_or_adjacent;
use crate::backend::parser::analyze::ranges::bind_maybe_range_overlap_or_adjacent;

pub(super) fn bind_arithmetic_expr(
    op: &'static str,
    make: crate::include::nodes::primnodes::OpExprKind,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let raw_left_type =
        infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let raw_right_type =
        infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let left_type = coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
    let right_type = coerce_unknown_string_literal_type(right, raw_right_type, left_type);
    if !left_type.is_array
        && !right_type.is_array
        && (matches!(left_type.kind, SqlTypeKind::Money)
            || matches!(right_type.kind, SqlTypeKind::Money))
    {
        let left =
            bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?;
        let right = bind_expr_with_outer_and_ctes(
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?;
        return bind_money_arithmetic_expr(
            op,
            make,
            left,
            raw_left_type,
            left_type,
            right,
            raw_right_type,
            right_type,
        );
    }
    if !left_type.is_array
        && !right_type.is_array
        && op == "-"
        && matches!(left_type.kind, SqlTypeKind::Date)
        && matches!(right_type.kind, SqlTypeKind::Date)
    {
        return Ok(Expr::binary_op(
            make,
            SqlType::new(SqlTypeKind::Int4),
            coerce_bound_expr(
                bind_expr_with_outer_and_ctes(
                    left,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
                raw_left_type,
                left_type,
            ),
            coerce_bound_expr(
                bind_expr_with_outer_and_ctes(
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
                raw_right_type,
                right_type,
            ),
        ));
    }
    let common = resolve_numeric_binary_type(op, left_type, right_type)?;
    let left = coerce_bound_expr(
        bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?,
        raw_left_type,
        common,
    );
    let right = coerce_bound_expr(
        bind_expr_with_outer_and_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes)?,
        raw_right_type,
        common,
    );
    Ok(Expr::op_auto(make, vec![left, right]))
}

pub(super) fn bind_comparison_expr(
    op: &'static str,
    make: crate::include::nodes::primnodes::OpExprKind,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let raw_left_type =
        infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let raw_right_type =
        infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let left_type = coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
    let right_type = coerce_unknown_string_literal_type(right, raw_right_type, left_type);
    if !left_type.is_array
        && !right_type.is_array
        && matches!(left_type.kind, SqlTypeKind::Money)
        && matches!(right_type.kind, SqlTypeKind::Money)
    {
        let left_bound =
            bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?;
        let right_bound = bind_expr_with_outer_and_ctes(
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?;
        let (left_bound, left_explicit_collation) = strip_explicit_collation(left_bound);
        let (right_bound, right_explicit_collation) = strip_explicit_collation(right_bound);
        return bind_lowered_comparison_expr(
            op,
            make,
            left_bound,
            raw_left_type,
            left_type,
            right_bound,
            raw_right_type,
            right_type,
            left_explicit_collation,
            right_explicit_collation,
            catalog,
        );
    }
    let left_bound =
        bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let right_bound =
        bind_expr_with_outer_and_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let (left_bound, left_explicit_collation) = strip_explicit_collation(left_bound);
    let (right_bound, right_explicit_collation) = strip_explicit_collation(right_bound);
    bind_lowered_comparison_expr(
        op,
        make,
        left_bound,
        raw_left_type,
        left_type,
        right_bound,
        raw_right_type,
        right_type,
        left_explicit_collation,
        right_explicit_collation,
        catalog,
    )
}

pub(super) fn bind_bound_comparison_expr(
    op: &'static str,
    make: crate::include::nodes::primnodes::OpExprKind,
    left_bound: Expr,
    raw_left_type: SqlType,
    left_type: SqlType,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let raw_right_type =
        infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let right_type = coerce_unknown_string_literal_type(right, raw_right_type, left_type);
    if !left_type.is_array
        && !right_type.is_array
        && matches!(left_type.kind, SqlTypeKind::Money)
        && matches!(right_type.kind, SqlTypeKind::Money)
    {
        let right_bound = bind_expr_with_outer_and_ctes(
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?;
        let (left_bound, left_explicit_collation) = strip_explicit_collation(left_bound);
        let (right_bound, right_explicit_collation) = strip_explicit_collation(right_bound);
        return bind_lowered_comparison_expr(
            op,
            make,
            left_bound,
            raw_left_type,
            left_type,
            right_bound,
            raw_right_type,
            right_type,
            left_explicit_collation,
            right_explicit_collation,
            catalog,
        );
    }
    let right_bound =
        bind_expr_with_outer_and_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let (left_bound, left_explicit_collation) = strip_explicit_collation(left_bound);
    let (right_bound, right_explicit_collation) = strip_explicit_collation(right_bound);
    bind_lowered_comparison_expr(
        op,
        make,
        left_bound,
        raw_left_type,
        left_type,
        right_bound,
        raw_right_type,
        right_type,
        left_explicit_collation,
        right_explicit_collation,
        catalog,
    )
}

pub(crate) fn bind_lowered_comparison_expr(
    op: &'static str,
    make: crate::include::nodes::primnodes::OpExprKind,
    left_bound: Expr,
    raw_left_type: SqlType,
    left_type: SqlType,
    right_bound: Expr,
    raw_right_type: SqlType,
    right_type: SqlType,
    left_explicit_collation: Option<u32>,
    right_explicit_collation: Option<u32>,
    catalog: &dyn CatalogLookup,
) -> Result<Expr, ParseError> {
    let (left, right) = if is_numeric_family(left_type) && is_numeric_family(right_type) {
        let common = if is_oid_integer_comparison(left_type, right_type) {
            SqlType::new(SqlTypeKind::Oid)
        } else {
            resolve_numeric_binary_type(op, left_type, right_type)?
        };
        (
            coerce_bound_expr(left_bound, raw_left_type, common),
            coerce_bound_expr(right_bound, raw_right_type, common),
        )
    } else if left_type.is_array && right_type.is_array {
        if !supports_comparison_operator(catalog, op, left_type, right_type) {
            return Err(ParseError::UndefinedOperator {
                op,
                left_type: sql_type_name(left_type),
                right_type: sql_type_name(right_type),
            });
        }
        (
            coerce_bound_expr(left_bound, raw_left_type, left_type),
            coerce_bound_expr(right_bound, raw_right_type, right_type),
        )
    } else {
        let (left, right, resolved_left_type, resolved_right_type) =
            if !left_type.is_array && !right_type.is_array {
                if let Some(common) = resolve_common_scalar_type(left_type, right_type)
                    .filter(|ty| !is_numeric_family(*ty))
                {
                    (
                        coerce_bound_expr(left_bound, raw_left_type, common),
                        coerce_bound_expr(right_bound, raw_right_type, common),
                        common,
                        common,
                    )
                } else {
                    (left_bound, right_bound, left_type, right_type)
                }
            } else {
                (left_bound, right_bound, left_type, right_type)
            };
        if !supports_comparison_operator(catalog, op, resolved_left_type, resolved_right_type) {
            return Err(ParseError::UndefinedOperator {
                op,
                left_type: sql_type_name(resolved_left_type),
                right_type: sql_type_name(resolved_right_type),
            });
        }
        (left, right)
    };
    let collation_oid = derive_consumer_collation(
        catalog,
        CollationConsumer::StringComparison,
        &[
            (expr_sql_type_hint(&left).unwrap_or(left_type), left_explicit_collation),
            (
                expr_sql_type_hint(&right).unwrap_or(right_type),
                right_explicit_collation,
            ),
        ],
    )?;
    Ok(Expr::op_with_collation(
        make,
        SqlType::new(SqlTypeKind::Bool),
        vec![left, right],
        collation_oid,
    ))
}

fn bind_money_arithmetic_expr(
    op: &'static str,
    make: crate::include::nodes::primnodes::OpExprKind,
    left: Expr,
    raw_left_type: SqlType,
    left_type: SqlType,
    right: Expr,
    raw_right_type: SqlType,
    right_type: SqlType,
) -> Result<Expr, ParseError> {
    let money = SqlType::new(SqlTypeKind::Money);
    let left_is_money = matches!(left_type.kind, SqlTypeKind::Money);
    let right_is_money = matches!(right_type.kind, SqlTypeKind::Money);
    if left_is_money && right_is_money {
        let result = if op == "/" {
            SqlType::new(SqlTypeKind::Float8)
        } else {
            money
        };
        return Ok(Expr::binary_op(
            make,
            result,
            coerce_bound_expr(left, raw_left_type, money),
            coerce_bound_expr(right, raw_right_type, money),
        ));
    }
    let other = if left_is_money { right_type } else { left_type };
    let supported_other = matches!(
        other.kind,
        SqlTypeKind::Int2
            | SqlTypeKind::Int4
            | SqlTypeKind::Int8
            | SqlTypeKind::Float4
            | SqlTypeKind::Float8
    );
    if supported_other && matches!(op, "*" | "/") {
        return Ok(Expr::binary_op(
            make,
            money,
            if left_is_money {
                coerce_bound_expr(left, raw_left_type, money)
            } else {
                coerce_bound_expr(left, raw_left_type, other)
            },
            if right_is_money {
                coerce_bound_expr(right, raw_right_type, money)
            } else {
                coerce_bound_expr(right, raw_right_type, other)
            },
        ));
    }
    Err(ParseError::UndefinedOperator {
        op,
        left_type: sql_type_name(left_type),
        right_type: sql_type_name(right_type),
    })
}

fn supports_comparison_operator(
    catalog: &dyn CatalogLookup,
    op: &str,
    left: SqlType,
    right: SqlType,
) -> bool {
    if comparison_operator_exists(catalog, op, left, right) {
        return true;
    }
    if !left.is_array
        && !right.is_array
        && left == right
        && matches!(left.kind, SqlTypeKind::TsQuery | SqlTypeKind::TsVector)
        && matches!(op, "=" | "<>" | "<" | "<=" | ">" | ">=")
    {
        return true;
    }
    if supports_builtin_datetime_comparison(op, left, right) {
        return true;
    }
    if supports_builtin_text_like_comparison(op, left, right) {
        return true;
    }
    supports_array_comparison_operator(op, left, right)
}

// :HACK: PostgreSQL models array operators via polymorphic catalog operators.
// pgrust does not bootstrap that polymorphic operator surface yet, so allow the
// exact same-type array operators that the executor already supports.
fn supports_array_comparison_operator(op: &str, left: SqlType, right: SqlType) -> bool {
    left.is_array
        && right.is_array
        && left == right
        && matches!(op, "=" | "<>" | "<" | "<=" | ">" | ">=" | "@>" | "<@" | "&&")
}

fn supports_builtin_datetime_comparison(op: &str, left: SqlType, right: SqlType) -> bool {
    !left.is_array
        && !right.is_array
        && left.kind == right.kind
        && matches!(
            left.kind,
            SqlTypeKind::Date
                | SqlTypeKind::Time
                | SqlTypeKind::TimeTz
                | SqlTypeKind::Timestamp
                | SqlTypeKind::TimestampTz
        )
        && matches!(op, "=" | "<>" | "<" | "<=" | ">" | ">=")
}

// :HACK: PostgreSQL has catalog operators for these string-ish types. pgrust's
// bootstrap operator catalog is still sparse, but executor comparison already
// handles them through textual value semantics.
fn supports_builtin_text_like_comparison(op: &str, left: SqlType, right: SqlType) -> bool {
    !left.is_array
        && !right.is_array
        && left.kind == right.kind
        && matches!(
            left.kind,
            SqlTypeKind::Name
                | SqlTypeKind::Char
                | SqlTypeKind::Varchar
                | SqlTypeKind::InternalChar
        )
        && matches!(op, "=" | "<>" | "<" | "<=" | ">" | ">=")
}

fn is_oid_integer_comparison(left: SqlType, right: SqlType) -> bool {
    !left.is_array
        && !right.is_array
        && matches!(
            left.kind,
            SqlTypeKind::Oid | SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8
        )
        && matches!(
            right.kind,
            SqlTypeKind::Oid | SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8
        )
        && (matches!(left.kind, SqlTypeKind::Oid) || matches!(right.kind, SqlTypeKind::Oid))
}

pub(super) fn bind_shift_expr(
    op: &'static str,
    make: crate::include::nodes::primnodes::OpExprKind,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let left_type =
        infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let right_type =
        infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    if is_bit_string_type(left_type) {
        if !is_integer_family(right_type) {
            return Err(ParseError::UndefinedOperator {
                op,
                left_type: sql_type_name(left_type),
                right_type: sql_type_name(right_type),
            });
        }
        let left =
            bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?;
        let right = coerce_bound_expr(
            bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            right_type,
            SqlType::new(SqlTypeKind::Int4),
        );
        return Ok(Expr::op_auto(make, vec![left, right]));
    }
    if !is_integer_family(left_type) || !is_integer_family(right_type) {
        return Err(ParseError::UndefinedOperator {
            op,
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        });
    }

    let left =
        bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let right = coerce_bound_expr(
        bind_expr_with_outer_and_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes)?,
        right_type,
        SqlType::new(SqlTypeKind::Int4),
    );
    Ok(Expr::op_auto(make, vec![left, right]))
}

pub(super) fn bind_bitwise_expr(
    op: &'static str,
    make: crate::include::nodes::primnodes::OpExprKind,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let left_type =
        infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let right_type =
        infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    if is_bit_string_type(left_type) && is_bit_string_type(right_type) {
        let common = resolve_common_scalar_type(left_type, right_type)
            .unwrap_or(SqlType::new(SqlTypeKind::VarBit));
        let left = coerce_bound_expr(
            bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?,
            left_type,
            common,
        );
        let right = coerce_bound_expr(
            bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            right_type,
            common,
        );
        return Ok(Expr::op_auto(make, vec![left, right]));
    }
    if !is_integer_family(left_type) || !is_integer_family(right_type) {
        return Err(ParseError::UndefinedOperator {
            op,
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        });
    }
    let common = resolve_numeric_binary_type(op, left_type, right_type)?;
    let left = coerce_bound_expr(
        bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?,
        left_type,
        common,
    );
    let right = coerce_bound_expr(
        bind_expr_with_outer_and_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes)?,
        right_type,
        common,
    );
    Ok(Expr::op_auto(make, vec![left, right]))
}

pub(super) fn bind_concat_expr(
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let left_type =
        infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let right_type =
        infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let left_bound =
        bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let right_bound =
        bind_expr_with_outer_and_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    bind_concat_operands(left, left_type, left_bound, right, right_type, right_bound)
}

pub(super) fn bind_overloaded_binary_expr(
    op: &'static str,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if let Some(result) = bind_maybe_multirange_overlap_or_adjacent(
        op,
        left,
        right,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ) {
        return result;
    }
    if let Some(result) = bind_maybe_range_overlap_or_adjacent(
        op,
        left,
        right,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ) {
        return result;
    }
    let raw_left_type =
        infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let raw_right_type =
        infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let mut left_type = coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
    let mut right_type = coerce_unknown_string_literal_type(right, raw_right_type, raw_left_type);
    let right_is_string_literal = matches!(
        right,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    );
    let left_is_string_literal = matches!(
        left,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    );

    if op == "@@" {
        if matches!(left_type.kind, SqlTypeKind::Jsonb) && right_is_string_literal {
            right_type = SqlType::new(SqlTypeKind::JsonPath);
        } else if matches!(right_type.kind, SqlTypeKind::Jsonb) && left_is_string_literal {
            left_type = SqlType::new(SqlTypeKind::JsonPath);
        } else if matches!(left_type.kind, SqlTypeKind::TsVector) && right_is_string_literal {
            right_type = SqlType::new(SqlTypeKind::TsQuery);
        } else if matches!(right_type.kind, SqlTypeKind::TsVector) && left_is_string_literal {
            left_type = SqlType::new(SqlTypeKind::TsQuery);
        }
    } else if op == "&&" {
        if matches!(left_type.kind, SqlTypeKind::TsQuery) && right_is_string_literal {
            right_type = SqlType::new(SqlTypeKind::TsQuery);
        } else if matches!(right_type.kind, SqlTypeKind::TsQuery) && left_is_string_literal {
            left_type = SqlType::new(SqlTypeKind::TsQuery);
        }
    }

    let left_bound =
        bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let right_bound =
        bind_expr_with_outer_and_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes)?;

    match op {
        "@@" => {
            if left_type.kind == SqlTypeKind::Jsonb && right_type.kind == SqlTypeKind::JsonPath {
                return Ok(Expr::op_auto(
                    crate::include::nodes::primnodes::OpExprKind::JsonbPathMatch,
                    vec![
                        coerce_bound_expr(
                            left_bound,
                            raw_left_type,
                            SqlType::new(SqlTypeKind::Jsonb),
                        ),
                        coerce_bound_expr(
                            right_bound,
                            raw_right_type,
                            SqlType::new(SqlTypeKind::JsonPath),
                        ),
                    ],
                ));
            }
            if matches!(left_type.kind, SqlTypeKind::TsVector)
                && matches!(right_type.kind, SqlTypeKind::TsQuery)
            {
                return Ok(Expr::builtin_func(
                    BuiltinScalarFunction::TsMatch,
                    Some(SqlType::new(SqlTypeKind::Bool)),
                    false,
                    vec![
                        coerce_bound_expr(
                            left_bound,
                            raw_left_type,
                            SqlType::new(SqlTypeKind::TsVector),
                        ),
                        coerce_bound_expr(
                            right_bound,
                            raw_right_type,
                            SqlType::new(SqlTypeKind::TsQuery),
                        ),
                    ],
                ));
            }
            if matches!(left_type.kind, SqlTypeKind::TsQuery)
                && matches!(right_type.kind, SqlTypeKind::TsVector)
            {
                return Ok(Expr::builtin_func(
                    BuiltinScalarFunction::TsMatch,
                    Some(SqlType::new(SqlTypeKind::Bool)),
                    false,
                    vec![
                        coerce_bound_expr(
                            left_bound,
                            raw_left_type,
                            SqlType::new(SqlTypeKind::TsQuery),
                        ),
                        coerce_bound_expr(
                            right_bound,
                            raw_right_type,
                            SqlType::new(SqlTypeKind::TsVector),
                        ),
                    ],
                ));
            }
        }
        "&&" => {
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
                return result;
            }
            if left_type.is_array && right_type.is_array {
                let left_expr = coerce_bound_expr(left_bound, raw_left_type, left_type);
                let left_expr = if left_is_string_literal {
                    if let Expr::ArrayLiteral { array_type, .. } = &right_bound {
                        coerce_bound_expr(left_expr, left_type, *array_type)
                    } else {
                        left_expr
                    }
                } else {
                    left_expr
                };
                let right_expr = if right_is_string_literal {
                    if let Expr::ArrayLiteral { array_type, .. } = &left_expr {
                        coerce_bound_expr(right_bound, raw_right_type, *array_type)
                    } else {
                        coerce_bound_expr(right_bound, raw_right_type, right_type)
                    }
                } else {
                    coerce_bound_expr(right_bound, raw_right_type, right_type)
                };
                return Ok(Expr::op_auto(
                    crate::include::nodes::primnodes::OpExprKind::ArrayOverlap,
                    vec![left_expr, right_expr],
                ));
            }
            if matches!(left_type.kind, SqlTypeKind::TsQuery)
                && matches!(right_type.kind, SqlTypeKind::TsQuery)
            {
                return Ok(Expr::builtin_func(
                    BuiltinScalarFunction::TsQueryAnd,
                    Some(SqlType::new(SqlTypeKind::TsQuery)),
                    false,
                    vec![
                        coerce_bound_expr(
                            left_bound,
                            raw_left_type,
                            SqlType::new(SqlTypeKind::TsQuery),
                        ),
                        coerce_bound_expr(
                            right_bound,
                            raw_right_type,
                            SqlType::new(SqlTypeKind::TsQuery),
                        ),
                    ],
                ));
            }
        }
        _ => {}
    }

    Err(ParseError::UndefinedOperator {
        op,
        left_type: sql_type_name(left_type),
        right_type: sql_type_name(right_type),
    })
}

pub(super) fn bind_prefix_operator_expr(
    op: &str,
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let raw_type =
        infer_sql_expr_type_with_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes);
    let bound =
        bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    match op {
        "!!" if matches!(raw_type.kind, SqlTypeKind::TsQuery) => Ok(Expr::builtin_func(
            BuiltinScalarFunction::TsQueryNot,
            Some(SqlType::new(SqlTypeKind::TsQuery)),
            false,
            vec![coerce_bound_expr(
                bound,
                raw_type,
                SqlType::new(SqlTypeKind::TsQuery),
            )],
        )),
        _ => Err(ParseError::UnexpectedToken {
            expected: "supported prefix operator",
            actual: op.into(),
        }),
    }
}

pub(crate) fn bind_concat_operands(
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
        return Ok(Expr::op_auto(
            crate::include::nodes::primnodes::OpExprKind::Concat,
            vec![left_bound, right_bound],
        ));
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
        return Ok(Expr::op_auto(
            crate::include::nodes::primnodes::OpExprKind::Concat,
            vec![left_expr, right_expr],
        ));
    }

    if is_bit_string_type(left_type) && is_bit_string_type(right_type) {
        let common = resolve_common_scalar_type(left_type, right_type)
            .unwrap_or(SqlType::new(SqlTypeKind::VarBit));
        let left_expr = coerce_bound_expr(left_bound, left_type, common);
        let right_expr = coerce_bound_expr(right_bound, right_type, common);
        return Ok(Expr::op_auto(
            OpExprKind::Concat,
            vec![left_expr, right_expr],
        ));
    }

    if matches!(left_type.kind, SqlTypeKind::TsVector)
        && matches!(right_type.kind, SqlTypeKind::TsVector)
    {
        return Ok(Expr::builtin_func(
            BuiltinScalarFunction::TsVectorConcat,
            Some(SqlType::new(SqlTypeKind::TsVector)),
            false,
            vec![
                coerce_bound_expr(left_bound, left_type, SqlType::new(SqlTypeKind::TsVector)),
                coerce_bound_expr(right_bound, right_type, SqlType::new(SqlTypeKind::TsVector)),
            ],
        ));
    }

    if matches!(left_type.kind, SqlTypeKind::TsQuery)
        && matches!(right_type.kind, SqlTypeKind::TsQuery)
    {
        return Ok(Expr::builtin_func(
            BuiltinScalarFunction::TsQueryOr,
            Some(SqlType::new(SqlTypeKind::TsQuery)),
            false,
            vec![
                coerce_bound_expr(left_bound, left_type, SqlType::new(SqlTypeKind::TsQuery)),
                coerce_bound_expr(right_bound, right_type, SqlType::new(SqlTypeKind::TsQuery)),
            ],
        ));
    }

    if should_use_text_concat(left_sql, left_type, right_sql, right_type) {
        let text_type = SqlType::new(SqlTypeKind::Text);
        let left_expr = coerce_bound_expr(left_bound, left_type, text_type);
        let right_expr = coerce_bound_expr(right_bound, right_type, text_type);
        return Ok(Expr::op_auto(
            crate::include::nodes::primnodes::OpExprKind::Concat,
            vec![left_expr, right_expr],
        ));
    }

    Err(ParseError::UndefinedOperator {
        op: "||",
        left_type: sql_type_name(left_type),
        right_type: sql_type_name(right_type),
    })
}
