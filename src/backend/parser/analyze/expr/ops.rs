use super::*;
use crate::backend::parser::analyze::multiranges::bind_maybe_multirange_overlap_or_adjacent;
use crate::backend::parser::analyze::ranges::bind_maybe_range_overlap_or_adjacent;
use crate::include::catalog::{
    BTREE_AM_OID, C_COLLATION_OID, HASH_AM_OID, PgOperatorRow, RECORD_TYPE_OID,
    TEXT_CMP_GE_PROC_OID, TEXT_CMP_GT_PROC_OID, TEXT_CMP_LE_PROC_OID, TEXT_CMP_LT_PROC_OID,
    TEXT_PATTERN_GE_OPERATOR_OID, TEXT_PATTERN_GT_OPERATOR_OID, TEXT_PATTERN_LE_OPERATOR_OID,
    TEXT_PATTERN_LT_OPERATOR_OID,
};
use crate::include::nodes::primnodes::{OpExpr, OpExprKind};

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
    reject_arithmetic_quantified_array_call(
        op,
        right,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    let raw_left_type =
        infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let raw_right_type =
        infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let left_type = coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
    let right_type = coerce_unknown_string_literal_type(right, raw_right_type, left_type);
    let right_is_string_literal = matches!(
        right,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    );
    let left_is_string_literal = matches!(
        left,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    );
    if !left_type.is_array
        && !right_type.is_array
        && matches!(left_type.kind, SqlTypeKind::PgLsn)
        && (matches!(right_type.kind, SqlTypeKind::PgLsn) || is_numeric_family(right_type))
        && matches!(op, "+" | "-")
    {
        let result_type = if matches!(right_type.kind, SqlTypeKind::PgLsn) {
            SqlType::new(SqlTypeKind::Numeric)
        } else {
            SqlType::new(SqlTypeKind::PgLsn)
        };
        return Ok(Expr::binary_op(
            make,
            result_type,
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
    if !left_type.is_array
        && !right_type.is_array
        && op == "+"
        && is_numeric_family(left_type)
        && matches!(right_type.kind, SqlTypeKind::PgLsn)
    {
        return Ok(Expr::binary_op(
            make,
            SqlType::new(SqlTypeKind::PgLsn),
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
    if !left_type.is_array
        && !right_type.is_array
        && op == "+"
        && matches!(left_type.kind, SqlTypeKind::Time)
        && matches!(right_type.kind, SqlTypeKind::Time)
    {
        return Err(ParseError::DetailedError {
            message: "operator is not unique: time without time zone + time without time zone"
                .into(),
            detail: None,
            hint: Some(
                "Could not choose a best candidate operator. You might need to add explicit type casts."
                    .into(),
            ),
            sqlstate: "42725",
        });
    }
    if !left_type.is_array
        && !right_type.is_array
        && op == "+"
        && matches!(left_type.kind, SqlTypeKind::TimeTz)
        && matches!(right_type.kind, SqlTypeKind::TimeTz)
    {
        return Err(ParseError::UndefinedOperator {
            op: op.into(),
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        });
    }
    if !left_type.is_array
        && !right_type.is_array
        && op == "-"
        && matches!(left_type.kind, SqlTypeKind::Date)
        && matches!(right_type.kind, SqlTypeKind::TimeTz)
    {
        return Err(ParseError::UndefinedOperator {
            op: op.into(),
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        });
    }
    if !left_type.is_array && !right_type.is_array {
        let result_type = match (op, left_type.kind, right_type.kind) {
            ("+", SqlTypeKind::Date, SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8)
            | ("+", SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8, SqlTypeKind::Date)
            | ("-", SqlTypeKind::Date, SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8) => {
                Some(SqlType::new(SqlTypeKind::Date))
            }
            ("+", SqlTypeKind::Date, SqlTypeKind::Time)
            | ("+", SqlTypeKind::Time, SqlTypeKind::Date)
            | ("-", SqlTypeKind::Date, SqlTypeKind::Time) => {
                Some(SqlType::new(SqlTypeKind::Timestamp))
            }
            ("+", SqlTypeKind::Date, SqlTypeKind::TimeTz)
            | ("+", SqlTypeKind::TimeTz, SqlTypeKind::Date) => {
                Some(SqlType::new(SqlTypeKind::TimestampTz))
            }
            _ => None,
        };
        if let Some(result_type) = result_type {
            return Ok(Expr::binary_op(
                make,
                result_type,
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
    }
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
        && matches!(
            (left_type.kind, right_type.kind),
            (SqlTypeKind::Timestamp, SqlTypeKind::Timestamp)
                | (SqlTypeKind::TimestampTz, SqlTypeKind::TimestampTz)
        )
    {
        return Ok(Expr::binary_op(
            make,
            SqlType::new(SqlTypeKind::Interval),
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
    if !left_type.is_array
        && !right_type.is_array
        && op == "*"
        && (matches!(left_type.kind, SqlTypeKind::Interval)
            || matches!(right_type.kind, SqlTypeKind::Interval))
        && matches!(
            if matches!(left_type.kind, SqlTypeKind::Interval) {
                right_type.kind
            } else {
                left_type.kind
            },
            SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8
        )
    {
        let interval = SqlType::new(SqlTypeKind::Interval);
        return Ok(Expr::binary_op(
            make,
            interval,
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
    if !left_type.is_array
        && !right_type.is_array
        && op == "-"
        && left_type.kind == right_type.kind
        && matches!(
            left_type.kind,
            SqlTypeKind::Timestamp | SqlTypeKind::TimestampTz
        )
    {
        return Ok(Expr::binary_op(
            make,
            SqlType::new(SqlTypeKind::Interval),
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
    if !left_type.is_array
        && !right_type.is_array
        && matches!(op, "+" | "-")
        && matches!(left_type.kind, SqlTypeKind::Interval)
        && matches!(right_type.kind, SqlTypeKind::Interval)
    {
        let interval = SqlType::new(SqlTypeKind::Interval);
        return Ok(Expr::binary_op(
            make,
            interval,
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
                interval,
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
                interval,
            ),
        ));
    }
    if !left_type.is_array
        && !right_type.is_array
        && matches!(op, "+" | "-")
        && matches!(
            left_type.kind,
            SqlTypeKind::Date | SqlTypeKind::Timestamp | SqlTypeKind::TimestampTz
        )
        && matches!(right_type.kind, SqlTypeKind::Interval)
    {
        let result_type = match left_type.kind {
            SqlTypeKind::Date => SqlType::new(SqlTypeKind::Timestamp),
            _ => left_type,
        };
        return Ok(Expr::binary_op(
            make,
            result_type,
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
                SqlType::new(SqlTypeKind::Interval),
            ),
        ));
    }
    if !left_type.is_array
        && !right_type.is_array
        && op == "+"
        && matches!(left_type.kind, SqlTypeKind::Interval)
        && matches!(
            right_type.kind,
            SqlTypeKind::Date | SqlTypeKind::Timestamp | SqlTypeKind::TimestampTz
        )
    {
        let result_type = match right_type.kind {
            SqlTypeKind::Date => SqlType::new(SqlTypeKind::Timestamp),
            _ => right_type,
        };
        return Ok(Expr::binary_op(
            make,
            result_type,
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
                SqlType::new(SqlTypeKind::Interval),
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
    if !left_type.is_array
        && !right_type.is_array
        && matches!(op, "*" | "/")
        && (matches!(left_type.kind, SqlTypeKind::Interval)
            || matches!(right_type.kind, SqlTypeKind::Interval))
    {
        let interval = SqlType::new(SqlTypeKind::Interval);
        let float8 = SqlType::new(SqlTypeKind::Float8);
        if op == "*"
            && matches!(left_type.kind, SqlTypeKind::Interval)
            && (is_numeric_family(right_type) || right_is_string_literal)
        {
            return Ok(Expr::binary_op(
                make,
                interval,
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
                    interval,
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
                    float8,
                ),
            ));
        }
        if op == "*"
            && (is_numeric_family(left_type) || left_is_string_literal)
            && matches!(right_type.kind, SqlTypeKind::Interval)
        {
            return Ok(Expr::binary_op(
                make,
                interval,
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
                    float8,
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
                    interval,
                ),
            ));
        }
        if op == "/"
            && matches!(left_type.kind, SqlTypeKind::Interval)
            && (is_numeric_family(right_type) || right_is_string_literal)
        {
            return Ok(Expr::binary_op(
                make,
                interval,
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
                    interval,
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
                    float8,
                ),
            ));
        }
    }
    if !left_type.is_array
        && !right_type.is_array
        && matches!(op, "+" | "-")
        && matches!(left_type.kind, SqlTypeKind::Time | SqlTypeKind::TimeTz)
        && matches!(right_type.kind, SqlTypeKind::Interval)
    {
        return Ok(Expr::binary_op(
            make,
            left_type,
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
                SqlType::new(SqlTypeKind::Interval),
            ),
        ));
    }
    if !left_type.is_array
        && !right_type.is_array
        && op == "+"
        && matches!(left_type.kind, SqlTypeKind::Interval)
        && matches!(right_type.kind, SqlTypeKind::Time | SqlTypeKind::TimeTz)
    {
        return Ok(Expr::binary_op(
            make,
            right_type,
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
                SqlType::new(SqlTypeKind::Interval),
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

fn reject_arithmetic_quantified_array_call(
    op: &'static str,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(), ParseError> {
    let SqlExpr::FuncCall { name, args, .. } = right else {
        return Ok(());
    };
    if !name.eq_ignore_ascii_case("any") && !name.eq_ignore_ascii_case("all") {
        return Ok(());
    }
    let [arg] = args.args() else {
        return Ok(());
    };
    if arg.name.is_some() {
        return Ok(());
    }
    let arg_type = infer_sql_expr_type_with_ctes(
        &arg.value,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    );
    let string_literal = matches!(
        arg.value,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    );
    if !arg_type.is_array && !string_literal {
        return Err(ParseError::DetailedError {
            message: "op ANY/ALL (array) requires array on right side".into(),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    // :HACK: The grammar only lowers comparison operators to QuantifiedArray today.
    // Arithmetic operators followed by ANY/ALL parse as a function call on the
    // right-hand side, so emit PostgreSQL's parse-analysis diagnostic here.
    if matches!(op, "+" | "-" | "*" | "/" | "%") {
        return Err(ParseError::DetailedError {
            message: "op ANY/ALL (array) requires operator to yield boolean".into(),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    Ok(())
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

fn text_pattern_operator_metadata(
    op: &'static str,
) -> Option<(crate::include::nodes::primnodes::OpExprKind, u32, u32)> {
    Some(match op {
        "~<~" => (
            crate::include::nodes::primnodes::OpExprKind::Lt,
            TEXT_PATTERN_LT_OPERATOR_OID,
            TEXT_CMP_LT_PROC_OID,
        ),
        "~<=~" => (
            crate::include::nodes::primnodes::OpExprKind::LtEq,
            TEXT_PATTERN_LE_OPERATOR_OID,
            TEXT_CMP_LE_PROC_OID,
        ),
        "~>=~" => (
            crate::include::nodes::primnodes::OpExprKind::GtEq,
            TEXT_PATTERN_GE_OPERATOR_OID,
            TEXT_CMP_GE_PROC_OID,
        ),
        "~>~" => (
            crate::include::nodes::primnodes::OpExprKind::Gt,
            TEXT_PATTERN_GT_OPERATOR_OID,
            TEXT_CMP_GT_PROC_OID,
        ),
        _ => return None,
    })
}

pub(super) fn bind_text_pattern_comparison_expr(
    op: &'static str,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let (kind, operator_oid, proc_oid) =
        text_pattern_operator_metadata(op).ok_or(ParseError::UnexpectedToken {
            expected: "text pattern comparison operator",
            actual: op.into(),
        })?;
    let raw_left_type =
        infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let raw_right_type =
        infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let left_type = coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
    let right_type = coerce_unknown_string_literal_type(right, raw_right_type, left_type);
    if !supports_pattern_ordering_operator(left_type)
        || !supports_pattern_ordering_operator(right_type)
    {
        return Err(ParseError::UndefinedOperator {
            op: op.into(),
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        });
    }
    let text_type = SqlType::new(SqlTypeKind::Text);
    let left = coerce_bound_expr(
        bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?,
        raw_left_type,
        text_type,
    );
    let right = coerce_bound_expr(
        bind_expr_with_outer_and_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes)?,
        raw_right_type,
        text_type,
    );
    Ok(Expr::Op(Box::new(OpExpr {
        opno: operator_oid,
        opfuncid: proc_oid,
        op: kind,
        opresulttype: SqlType::new(SqlTypeKind::Bool),
        args: vec![left, right],
        collation_oid: Some(C_COLLATION_OID),
    })))
}

pub(super) fn bind_text_starts_with_expr(
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
    if !supports_pattern_ordering_operator(left_type)
        || !supports_pattern_ordering_operator(right_type)
    {
        return Err(ParseError::UndefinedOperator {
            op: "^@".into(),
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        });
    }
    let text_type = SqlType::new(SqlTypeKind::Text);
    Ok(Expr::builtin_func(
        BuiltinScalarFunction::TextStartsWith,
        Some(SqlType::new(SqlTypeKind::Bool)),
        false,
        vec![
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
                text_type,
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
                text_type,
            ),
        ],
    ))
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
                op: op.into(),
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
                    .filter(|_| !is_mixed_date_timestamp_comparison(left_type, right_type))
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
                op: op.into(),
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
            (
                expr_sql_type_hint(&left).unwrap_or(left_type),
                left_explicit_collation,
            ),
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
        op: op.into(),
        left_type: sql_type_name(left_type),
        right_type: sql_type_name(right_type),
    })
}

pub(super) fn supports_comparison_operator(
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
        && matches!(left.kind, SqlTypeKind::Enum)
        && left.type_oid != 0
        && matches!(op, "=" | "<>" | "<" | "<=" | ">" | ">=")
    {
        return true;
    }
    if !left.is_array
        && !right.is_array
        && left == right
        && matches!(left.kind, SqlTypeKind::Record | SqlTypeKind::Composite)
        && matches!(op, "=" | "<>" | "<" | "<=" | ">" | ">=")
    {
        return true;
    }
    if !left.is_array
        && !right.is_array
        && left == right
        && matches!(
            left.kind,
            SqlTypeKind::TsQuery
                | SqlTypeKind::TsVector
                | SqlTypeKind::Inet
                | SqlTypeKind::Cidr
                | SqlTypeKind::PgLsn
                | SqlTypeKind::MacAddr
                | SqlTypeKind::MacAddr8
                | SqlTypeKind::RegClass
                | SqlTypeKind::RegType
                | SqlTypeKind::RegRole
                | SqlTypeKind::RegOperator
                | SqlTypeKind::RegProcedure
                | SqlTypeKind::RegConfig
                | SqlTypeKind::RegDictionary
                | SqlTypeKind::Int2Vector
                | SqlTypeKind::OidVector
        )
        && matches!(op, "=" | "<>" | "<" | "<=" | ">" | ">=")
    {
        return true;
    }
    if supports_builtin_datetime_comparison(op, left, right) {
        return true;
    }
    if supports_builtin_interval_comparison(op, left, right) {
        return true;
    }
    if supports_builtin_money_comparison(op, left, right) {
        return true;
    }
    if supports_builtin_text_like_comparison(op, left, right) {
        return true;
    }
    supports_array_comparison_operator(op, left, right)
}

pub(super) fn bind_maybe_network_operator(
    op: &'static str,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<Result<Expr, ParseError>> {
    let func = match op {
        "<<" => BuiltinScalarFunction::NetworkSubnet,
        "<<=" => BuiltinScalarFunction::NetworkSubnetEq,
        ">>" => BuiltinScalarFunction::NetworkSupernet,
        ">>=" => BuiltinScalarFunction::NetworkSupernetEq,
        "&&" => BuiltinScalarFunction::NetworkOverlap,
        _ => return None,
    };
    let raw_left_type =
        infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let raw_right_type =
        infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let mut left_type = coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
    let mut right_type = coerce_unknown_string_literal_type(right, raw_right_type, left_type);
    if !is_network_type(left_type) || !is_network_type(right_type) {
        return None;
    }
    if let Some(common) = resolve_common_scalar_type(left_type, right_type) {
        left_type = common;
        right_type = common;
    }

    Some((|| {
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
        Ok(Expr::builtin_func(
            func,
            Some(SqlType::new(SqlTypeKind::Bool)),
            false,
            vec![
                coerce_bound_expr(left_bound, raw_left_type, left_type),
                coerce_bound_expr(right_bound, raw_right_type, right_type),
            ],
        ))
    })())
}

pub(super) fn bind_maybe_network_arithmetic(
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
    let right_type = coerce_unknown_string_literal_type(right, raw_right_type, left_type);
    let left_is_network = is_network_type(left_type);
    let right_is_network = is_network_type(right_type);
    let left_is_integer = is_integer_family(left_type);
    let right_is_integer = is_integer_family(right_type);

    let result_type = match op {
        "+" if (left_is_network && right_is_integer) || (left_is_integer && right_is_network) => {
            SqlType::new(SqlTypeKind::Inet)
        }
        "-" if left_is_network && right_is_integer => SqlType::new(SqlTypeKind::Inet),
        "-" if left_is_network && right_is_network => SqlType::new(SqlTypeKind::Int8),
        _ => return None,
    };
    Some((|| {
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
        Ok(Expr::binary_op(
            if op == "+" {
                crate::include::nodes::primnodes::OpExprKind::Add
            } else {
                crate::include::nodes::primnodes::OpExprKind::Sub
            },
            result_type,
            coerce_bound_expr(left_bound, raw_left_type, left_type),
            coerce_bound_expr(right_bound, raw_right_type, right_type),
        ))
    })())
}

pub(super) fn bind_maybe_network_bitwise(
    _op: &'static str,
    make: crate::include::nodes::primnodes::OpExprKind,
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
    let right_type = coerce_unknown_string_literal_type(right, raw_right_type, left_type);
    if !is_network_type(left_type) || !is_network_type(right_type) {
        return None;
    }
    Some((|| {
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
        Ok(Expr::binary_op(
            make,
            SqlType::new(SqlTypeKind::Inet),
            coerce_bound_expr(left_bound, raw_left_type, left_type),
            coerce_bound_expr(right_bound, raw_right_type, right_type),
        ))
    })())
}

fn is_network_type(ty: SqlType) -> bool {
    !ty.is_array && matches!(ty.kind, SqlTypeKind::Inet | SqlTypeKind::Cidr)
}

// :HACK: PostgreSQL models array operators via polymorphic catalog operators.
// pgrust does not bootstrap that polymorphic operator surface yet, so allow the
// exact same-type array operators that the executor already supports.
fn supports_array_comparison_operator(op: &str, left: SqlType, right: SqlType) -> bool {
    left.is_array
        && right.is_array
        && left == right
        && matches!(
            op,
            "=" | "<>" | "<" | "<=" | ">" | ">=" | "@>" | "<@" | "&&"
        )
}

fn supports_builtin_datetime_comparison(op: &str, left: SqlType, right: SqlType) -> bool {
    !left.is_array
        && !right.is_array
        && (left.kind == right.kind
            && matches!(
                left.kind,
                SqlTypeKind::Date
                    | SqlTypeKind::Time
                    | SqlTypeKind::TimeTz
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::TimestampTz
            )
            || is_mixed_date_timestamp_comparison(left, right))
        && matches!(op, "=" | "<>" | "<" | "<=" | ">" | ">=")
}

fn is_mixed_date_timestamp_comparison(left: SqlType, right: SqlType) -> bool {
    !left.is_array
        && !right.is_array
        && matches!(
            (left.kind, right.kind),
            (SqlTypeKind::Date, SqlTypeKind::Timestamp)
                | (SqlTypeKind::Timestamp, SqlTypeKind::Date)
                | (SqlTypeKind::Date, SqlTypeKind::TimestampTz)
                | (SqlTypeKind::TimestampTz, SqlTypeKind::Date)
        )
}

// :HACK: PostgreSQL exposes interval comparison operators through pg_operator.
// pgrust's bootstrap operator catalog is still sparse, while the executor
// already compares interval values using the same normalized sort key.
fn supports_builtin_interval_comparison(op: &str, left: SqlType, right: SqlType) -> bool {
    !left.is_array
        && !right.is_array
        && left.kind == right.kind
        && matches!(left.kind, SqlTypeKind::Interval)
        && matches!(op, "=" | "<>" | "<" | "<=" | ">" | ">=")
}

// :HACK: PostgreSQL has catalog operators for money comparisons. pgrust's
// bootstrap operator catalog is still sparse, but executor comparison already
// handles money through its stored cent value.
fn supports_builtin_money_comparison(op: &str, left: SqlType, right: SqlType) -> bool {
    !left.is_array
        && !right.is_array
        && left.kind == right.kind
        && matches!(left.kind, SqlTypeKind::Money)
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

fn supports_pattern_ordering_operator(sql_type: SqlType) -> bool {
    !sql_type.is_array
        && matches!(
            sql_type.kind,
            SqlTypeKind::Text
                | SqlTypeKind::Name
                | SqlTypeKind::Char
                | SqlTypeKind::Varchar
                | SqlTypeKind::InternalChar
        )
}

pub(super) fn bind_order_by_using_direction(
    catalog: &dyn CatalogLookup,
    operator: &str,
    expr_type: SqlType,
) -> Result<bool, ParseError> {
    let (canonical_operator, descending, requires_pattern_type) = match operator {
        "<" => ("<", false, false),
        ">" => (">", true, false),
        "~<~" => ("<", false, true),
        "~>~" => (">", true, true),
        _ => {
            return Err(ParseError::DetailedError {
                message: format!("operator {operator} is not a valid ordering operator"),
                detail: None,
                hint: Some(
                    "Ordering operators must be \"<\" or \">\" members of supported btree operator families."
                        .into(),
                ),
                sqlstate: "42809",
            });
        }
    };
    if requires_pattern_type && !supports_pattern_ordering_operator(expr_type) {
        return Err(ParseError::DetailedError {
            message: format!("operator {operator} is not a valid ordering operator"),
            detail: None,
            hint: Some(
                "Ordering operators must be \"<\" or \">\" members of supported btree operator families."
                    .into(),
            ),
            sqlstate: "42809",
        });
    }
    if !supports_comparison_operator(catalog, canonical_operator, expr_type, expr_type) {
        return Err(ParseError::DetailedError {
            message: format!("operator {operator} is not a valid ordering operator"),
            detail: None,
            hint: Some(
                "Ordering operators must be \"<\" or \">\" members of supported btree operator families."
                    .into(),
            ),
            sqlstate: "42809",
        });
    }
    Ok(descending)
}

fn is_oid_integer_comparison(left: SqlType, right: SqlType) -> bool {
    !left.is_array
        && !right.is_array
        && matches!(
            left.kind,
            SqlTypeKind::Oid
                | SqlTypeKind::Int2
                | SqlTypeKind::Int4
                | SqlTypeKind::Int8
                | SqlTypeKind::RegProc
                | SqlTypeKind::RegClass
                | SqlTypeKind::RegType
                | SqlTypeKind::RegRole
                | SqlTypeKind::RegNamespace
                | SqlTypeKind::RegOper
                | SqlTypeKind::RegOperator
                | SqlTypeKind::RegProcedure
                | SqlTypeKind::RegCollation
                | SqlTypeKind::RegConfig
                | SqlTypeKind::RegDictionary
        )
        && matches!(
            right.kind,
            SqlTypeKind::Oid
                | SqlTypeKind::Int2
                | SqlTypeKind::Int4
                | SqlTypeKind::Int8
                | SqlTypeKind::RegProc
                | SqlTypeKind::RegClass
                | SqlTypeKind::RegType
                | SqlTypeKind::RegRole
                | SqlTypeKind::RegNamespace
                | SqlTypeKind::RegOper
                | SqlTypeKind::RegOperator
                | SqlTypeKind::RegProcedure
                | SqlTypeKind::RegCollation
                | SqlTypeKind::RegConfig
                | SqlTypeKind::RegDictionary
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
                op: op.into(),
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
            op: op.into(),
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
    let raw_left_type =
        infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let raw_right_type =
        infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let left_type = coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
    let right_type = coerce_unknown_string_literal_type(right, raw_right_type, left_type);
    if is_bit_string_type(left_type) && is_bit_string_type(right_type) {
        let common = resolve_common_scalar_type(left_type, right_type)
            .unwrap_or(SqlType::new(SqlTypeKind::VarBit));
        let left = coerce_bound_expr(
            bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?,
            raw_left_type,
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
            raw_right_type,
            common,
        );
        return Ok(Expr::op_auto(make, vec![left, right]));
    }
    if left_type == right_type && is_macaddr_type(left_type) {
        let left = coerce_bound_expr(
            bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?,
            raw_left_type,
            left_type,
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
            raw_right_type,
            right_type,
        );
        return Ok(Expr::op_auto(make, vec![left, right]));
    }
    if !is_integer_family(left_type) || !is_integer_family(right_type) {
        return Err(ParseError::UndefinedOperator {
            op: op.into(),
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        });
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
    if let Some(result) = bind_maybe_network_operator(
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
    } else if matches!(op, "@>" | "<@") {
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
            if is_text_like_type(left_type) && matches!(right_type.kind, SqlTypeKind::TsQuery) {
                return Ok(Expr::builtin_func(
                    BuiltinScalarFunction::TsMatch,
                    Some(SqlType::new(SqlTypeKind::Bool)),
                    false,
                    vec![
                        coerce_bound_expr(
                            left_bound,
                            raw_left_type,
                            SqlType::new(SqlTypeKind::Text),
                        ),
                        coerce_bound_expr(
                            right_bound,
                            raw_right_type,
                            SqlType::new(SqlTypeKind::TsQuery),
                        ),
                    ],
                ));
            }
            if matches!(left_type.kind, SqlTypeKind::TsQuery) && is_text_like_type(right_type) {
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
                            SqlType::new(SqlTypeKind::Text),
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
            if is_text_like_type(left_type) && matches!(right_type.kind, SqlTypeKind::TsQuery) {
                return Ok(Expr::builtin_func(
                    BuiltinScalarFunction::TsMatch,
                    Some(SqlType::new(SqlTypeKind::Bool)),
                    false,
                    vec![
                        coerce_bound_expr(
                            left_bound,
                            raw_left_type,
                            SqlType::new(SqlTypeKind::Text),
                        ),
                        coerce_bound_expr(
                            right_bound,
                            raw_right_type,
                            SqlType::new(SqlTypeKind::TsQuery),
                        ),
                    ],
                ));
            }
            if is_text_like_type(left_type) && is_text_like_type(right_type) {
                return Ok(Expr::builtin_func(
                    BuiltinScalarFunction::TsMatch,
                    Some(SqlType::new(SqlTypeKind::Bool)),
                    false,
                    vec![
                        coerce_bound_expr(
                            left_bound,
                            raw_left_type,
                            SqlType::new(SqlTypeKind::Text),
                        ),
                        coerce_bound_expr(
                            right_bound,
                            raw_right_type,
                            SqlType::new(SqlTypeKind::Text),
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
        "@>" | "<@" => {
            if matches!(left_type.kind, SqlTypeKind::TsQuery)
                && matches!(right_type.kind, SqlTypeKind::TsQuery)
            {
                return Ok(Expr::builtin_func(
                    BuiltinScalarFunction::TsQueryContains,
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
                            SqlType::new(SqlTypeKind::TsQuery),
                        ),
                        Expr::Const(Value::Bool(op == "@>")),
                    ],
                ));
            }
        }
        _ => {}
    }

    Err(ParseError::UndefinedOperator {
        op: op.into(),
        left_type: sql_type_name(left_type),
        right_type: sql_type_name(right_type),
    })
}

pub(super) fn bind_catalog_binary_operator_expr(
    op: &str,
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
    let left_oid = catalog
        .type_oid_for_sql_type(left_type)
        .ok_or_else(|| ParseError::UnsupportedType(sql_type_name(left_type)))?;
    let right_oid = catalog
        .type_oid_for_sql_type(right_type)
        .ok_or_else(|| ParseError::UnsupportedType(sql_type_name(right_type)))?;
    let operator = catalog
        .operator_by_name_left_right(op, left_oid, right_oid)
        .or_else(|| {
            let left_oid = catalog_operator_lookup_oid_for_row_type(left_type, left_oid);
            let right_oid = catalog_operator_lookup_oid_for_row_type(right_type, right_oid);
            catalog.operator_by_name_left_right(op, left_oid, right_oid)
        })
        .ok_or_else(|| ParseError::UndefinedOperator {
            op: op.into(),
            left_type: sql_type_name(left_type),
            right_type: sql_type_name(right_type),
        })?;
    let result_type = catalog
        .type_by_oid(operator.oprresult)
        .map(|row| row.sql_type.with_identity(row.oid, row.typrelid))
        .ok_or_else(|| ParseError::UnsupportedType(operator.oprresult.to_string()))?;
    let left_bound =
        bind_expr_with_outer_and_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let right_bound =
        bind_expr_with_outer_and_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let args = vec![
        coerce_bound_expr(left_bound, raw_left_type, left_type),
        coerce_bound_expr(right_bound, raw_right_type, right_type),
    ];
    if let Some(op_kind) = catalog_operator_expr_kind(catalog, &operator) {
        return Ok(Expr::Op(Box::new(OpExpr {
            opno: operator.oid,
            opfuncid: operator.oprcode,
            op: op_kind,
            opresulttype: result_type,
            args,
            collation_oid: None,
        })));
    }
    let implementation = catalog
        .proc_row_by_oid(operator.oprcode)
        .and_then(|row| builtin_impl_for_catalog_proc(&row))
        .map(ScalarFunctionImpl::Builtin)
        .unwrap_or(ScalarFunctionImpl::UserDefined {
            proc_oid: operator.oprcode,
        });
    Ok(Expr::func_with_impl(
        operator.oprcode,
        Some(result_type),
        false,
        implementation,
        args,
    ))
}

fn catalog_operator_lookup_oid_for_row_type(sql_type: SqlType, oid: u32) -> u32 {
    if matches!(sql_type.kind, SqlTypeKind::Composite | SqlTypeKind::Record) {
        RECORD_TYPE_OID
    } else {
        oid
    }
}

fn catalog_operator_expr_kind(
    catalog: &dyn CatalogLookup,
    operator: &PgOperatorRow,
) -> Option<OpExprKind> {
    catalog
        .amop_rows()
        .into_iter()
        .find(|row| row.amopopr == operator.oid)
        .and_then(|row| match (row.amopmethod, row.amopstrategy) {
            (BTREE_AM_OID, 1) => Some(OpExprKind::Lt),
            (BTREE_AM_OID, 2) => Some(OpExprKind::LtEq),
            (BTREE_AM_OID, 3) | (HASH_AM_OID, 1) => Some(OpExprKind::Eq),
            (BTREE_AM_OID, 4) => Some(OpExprKind::GtEq),
            (BTREE_AM_OID, 5) => Some(OpExprKind::Gt),
            _ => None,
        })
}

fn builtin_impl_for_catalog_proc(
    row: &crate::include::catalog::PgProcRow,
) -> Option<crate::include::nodes::primnodes::BuiltinScalarFunction> {
    crate::include::catalog::builtin_scalar_function_for_proc_row(row).or_else(|| {
        [row.prosrc.as_str(), row.proname.as_str()]
            .into_iter()
            .any(|name| {
                ["pt_in_widget", "pg_rust_test_pt_in_widget"]
                    .into_iter()
                    .any(|candidate| name.eq_ignore_ascii_case(candidate))
            })
            .then_some(
                crate::include::nodes::primnodes::BuiltinScalarFunction::PgRustTestPtInWidget,
            )
    })
}

pub(super) fn bind_maybe_tsquery_contains(
    op: &'static str,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<Result<Expr, ParseError>> {
    if !matches!(op, "@>" | "<@") {
        return None;
    }

    let raw_left_type =
        infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes);
    let raw_right_type =
        infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes);
    let mut left_type = coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
    let mut right_type = coerce_unknown_string_literal_type(right, raw_right_type, left_type);
    let left_is_string_literal = matches!(
        left,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    );
    let right_is_string_literal = matches!(
        right,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    );

    if matches!(left_type.kind, SqlTypeKind::TsQuery) && right_is_string_literal {
        right_type = SqlType::new(SqlTypeKind::TsQuery);
    } else if matches!(right_type.kind, SqlTypeKind::TsQuery) && left_is_string_literal {
        left_type = SqlType::new(SqlTypeKind::TsQuery);
    }

    if !matches!(left_type.kind, SqlTypeKind::TsQuery)
        || !matches!(right_type.kind, SqlTypeKind::TsQuery)
    {
        return None;
    }

    Some((|| {
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
        Ok(Expr::builtin_func(
            if op == "@>" {
                BuiltinScalarFunction::TsQueryContains
            } else {
                BuiltinScalarFunction::TsQueryContainedBy
            },
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
                    SqlType::new(SqlTypeKind::TsQuery),
                ),
            ],
        ))
    })())
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
    if let Some((operator, declared_type, result_type)) =
        resolve_prefix_operator(catalog, op, raw_type)
    {
        return Ok(Expr::func(
            operator.oprcode,
            Some(result_type),
            false,
            vec![coerce_bound_expr(bound, raw_type, declared_type)],
        ));
    }
    match op {
        "!!" => {
            let target_type = SqlType::new(SqlTypeKind::TsQuery);
            let operand_type = coerce_unknown_string_literal_type(expr, raw_type, target_type);
            if matches!(operand_type.kind, SqlTypeKind::TsQuery) {
                Ok(Expr::builtin_func(
                    BuiltinScalarFunction::TsQueryNot,
                    Some(target_type),
                    false,
                    vec![coerce_bound_expr(bound, raw_type, operand_type)],
                ))
            } else {
                Err(ParseError::UnexpectedToken {
                    expected: "supported prefix operator",
                    actual: op.into(),
                })
            }
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "supported prefix operator",
            actual: op.into(),
        }),
    }
}

fn resolve_prefix_operator(
    catalog: &dyn CatalogLookup,
    op: &str,
    actual_type: SqlType,
) -> Option<(crate::include::catalog::PgOperatorRow, SqlType, SqlType)> {
    let mut best: Option<(
        crate::include::catalog::PgOperatorRow,
        SqlType,
        SqlType,
        usize,
    )> = None;
    let mut ambiguous = false;

    for operator in catalog
        .operator_rows()
        .into_iter()
        .filter(|row| row.oprname.eq_ignore_ascii_case(op) && row.oprleft == 0 && row.oprright != 0)
    {
        let declared_type = catalog.type_by_oid(operator.oprright)?.sql_type;
        let result_type = catalog.type_by_oid(operator.oprresult)?.sql_type;
        let cost = prefix_operator_match_cost(actual_type, declared_type)?;
        match &best {
            None => {
                best = Some((operator, declared_type, result_type, cost));
                ambiguous = false;
            }
            Some((_, _, _, best_cost)) if cost < *best_cost => {
                best = Some((operator, declared_type, result_type, cost));
                ambiguous = false;
            }
            Some((_, _, _, best_cost)) if cost == *best_cost => ambiguous = true,
            _ => {}
        }
    }

    if ambiguous {
        None
    } else {
        best.map(|(operator, declared_type, result_type, _)| (operator, declared_type, result_type))
    }
}

fn prefix_operator_match_cost(actual_type: SqlType, target_type: SqlType) -> Option<usize> {
    if actual_type == target_type {
        return Some(0);
    }
    if actual_type.is_array != target_type.is_array {
        return None;
    }
    if is_numeric_family(actual_type) && is_numeric_family(target_type) {
        return Some(1);
    }
    if is_text_like_type(actual_type) && is_text_like_type(target_type) {
        return Some(1);
    }
    if is_bit_string_type(actual_type) && is_bit_string_type(target_type) {
        return Some(1);
    }
    None
}

pub(crate) fn bind_concat_operands(
    left_sql: &SqlExpr,
    left_type: SqlType,
    left_bound: Expr,
    right_sql: &SqlExpr,
    right_type: SqlType,
    right_bound: Expr,
) -> Result<Expr, ParseError> {
    let raw_left_type = left_type;
    let raw_right_type = right_type;
    let mut left_type = left_type;
    let mut right_type = right_type;
    let left_is_string_literal = matches!(
        left_sql,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    );
    let right_is_string_literal = matches!(
        right_sql,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    );
    if matches!(left_type.kind, SqlTypeKind::TsVector) && right_is_string_literal {
        right_type = SqlType::new(SqlTypeKind::TsVector);
    } else if matches!(right_type.kind, SqlTypeKind::TsVector) && left_is_string_literal {
        left_type = SqlType::new(SqlTypeKind::TsVector);
    } else if matches!(left_type.kind, SqlTypeKind::TsQuery) && right_is_string_literal {
        right_type = SqlType::new(SqlTypeKind::TsQuery);
    } else if matches!(right_type.kind, SqlTypeKind::TsQuery) && left_is_string_literal {
        left_type = SqlType::new(SqlTypeKind::TsQuery);
    }

    if left_type.kind == SqlTypeKind::Jsonb
        && !left_type.is_array
        && right_type.kind == SqlTypeKind::Jsonb
        && !right_type.is_array
    {
        return Ok(Expr::binary_op(
            crate::include::nodes::primnodes::OpExprKind::Concat,
            SqlType::new(SqlTypeKind::Jsonb),
            left_bound,
            right_bound,
        ));
    }

    if left_type.kind == SqlTypeKind::Bytea
        && !left_type.is_array
        && right_type.kind == SqlTypeKind::Bytea
        && !right_type.is_array
    {
        return Ok(Expr::binary_op(
            OpExprKind::Concat,
            SqlType::new(SqlTypeKind::Bytea),
            left_bound,
            right_bound,
        ));
    }

    if left_type.is_array || right_type.is_array {
        let element_type = resolve_array_concat_element_type(left_type, right_type)?;
        let result_type = SqlType::array_of(element_type);
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
        return Ok(Expr::binary_op(
            crate::include::nodes::primnodes::OpExprKind::Concat,
            result_type,
            left_expr,
            right_expr,
        ));
    }

    if is_bit_string_type(left_type) && is_bit_string_type(right_type) {
        let common = resolve_common_scalar_type(left_type, right_type)
            .unwrap_or(SqlType::new(SqlTypeKind::VarBit));
        let left_expr = coerce_bound_expr(left_bound, left_type, common);
        let right_expr = coerce_bound_expr(right_bound, right_type, common);
        return Ok(Expr::binary_op(
            OpExprKind::Concat,
            common,
            left_expr,
            right_expr,
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
                coerce_bound_expr(
                    left_bound,
                    raw_left_type,
                    SqlType::new(SqlTypeKind::TsVector),
                ),
                coerce_bound_expr(
                    right_bound,
                    raw_right_type,
                    SqlType::new(SqlTypeKind::TsVector),
                ),
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
        op: "||".into(),
        left_type: sql_type_name(left_type),
        right_type: sql_type_name(right_type),
    })
}
