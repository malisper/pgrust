use super::*;

pub(super) fn bind_arithmetic_expr(
    op: &'static str,
    make: fn(Box<Expr>, Box<Expr>) -> Expr,
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
    Ok(make(Box::new(left), Box::new(right)))
}

pub(super) fn bind_comparison_expr(
    make: fn(Box<Expr>, Box<Expr>) -> Expr,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let op = comparison_operator_name(make);
    let raw_left_type =
        infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let raw_right_type =
        infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let left_type = coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
    let right_type = coerce_unknown_string_literal_type(right, raw_right_type, left_type);
    let left_bound =
        bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let right_bound =
        bind_expr_with_outer_and_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes)?;
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
    } else {
        let (left, right, resolved_left_type, resolved_right_type) =
            if !left_type.is_array && !right_type.is_array {
                if let Some(common) =
                    resolve_common_scalar_type(left_type, right_type).filter(|ty| !is_numeric_family(*ty))
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
        if !supports_comparison_operator(op, resolved_left_type, resolved_right_type) {
            return Err(ParseError::UndefinedOperator {
                op,
                left_type: sql_type_name(resolved_left_type),
                right_type: sql_type_name(resolved_right_type),
            });
        }
        (left, right)
    };
    Ok(make(Box::new(left), Box::new(right)))
}

fn supports_comparison_operator(op: &str, left: SqlType, right: SqlType) -> bool {
    if comparison_operator_exists(op, left, right) {
        return true;
    }
    supports_array_comparison_operator(op, left, right)
}

// :HACK: PostgreSQL models array comparison via polymorphic catalog operators.
// pgrust does not bootstrap that polymorphic operator surface yet, so allow the
// exact same-type array comparison operators that the executor already supports.
fn supports_array_comparison_operator(op: &str, left: SqlType, right: SqlType) -> bool {
    left.is_array
        && right.is_array
        && left == right
        && matches!(op, "=" | "<>" | "<" | "<=" | ">" | ">=")
}

fn comparison_operator_name(make: fn(Box<Expr>, Box<Expr>) -> Expr) -> &'static str {
    if std::ptr::fn_addr_eq(make, Expr::Eq as fn(Box<Expr>, Box<Expr>) -> Expr) {
        "="
    } else if std::ptr::fn_addr_eq(make, Expr::NotEq as fn(Box<Expr>, Box<Expr>) -> Expr) {
        "<>"
    } else if std::ptr::fn_addr_eq(make, Expr::Lt as fn(Box<Expr>, Box<Expr>) -> Expr) {
        "<"
    } else if std::ptr::fn_addr_eq(make, Expr::LtEq as fn(Box<Expr>, Box<Expr>) -> Expr) {
        "<="
    } else if std::ptr::fn_addr_eq(make, Expr::Gt as fn(Box<Expr>, Box<Expr>) -> Expr) {
        ">"
    } else if std::ptr::fn_addr_eq(make, Expr::GtEq as fn(Box<Expr>, Box<Expr>) -> Expr) {
        ">="
    } else {
        unreachable!("unsupported comparison expression constructor")
    }
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
    make: fn(Box<Expr>, Box<Expr>) -> Expr,
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
        return Ok(make(Box::new(left), Box::new(right)));
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
    Ok(make(Box::new(left), Box::new(right)))
}

pub(super) fn bind_bitwise_expr(
    op: &'static str,
    make: fn(Box<Expr>, Box<Expr>) -> Expr,
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
        return Ok(make(Box::new(left), Box::new(right)));
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
    Ok(make(Box::new(left), Box::new(right)))
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

    if is_bit_string_type(left_type) && is_bit_string_type(right_type) {
        let common = resolve_common_scalar_type(left_type, right_type)
            .unwrap_or(SqlType::new(SqlTypeKind::VarBit));
        let left_expr = coerce_bound_expr(left_bound, left_type, common);
        let right_expr = coerce_bound_expr(right_bound, right_type, common);
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
