use super::*;

pub(super) fn coerce_bound_expr(expr: Expr, from: SqlType, to: SqlType) -> Expr {
    if from.element_type() == to.element_type() {
        expr
    } else {
        Expr::Cast(Box::new(expr), to)
    }
}

pub(super) fn resolve_numeric_binary_type(
    op: &'static str,
    left: SqlType,
    right: SqlType,
) -> Result<SqlType, ParseError> {
    use SqlTypeKind::*;
    let left = left.element_type();
    let right = right.element_type();
    if op == "%" && (matches!(left.kind, Float4 | Float8) || matches!(right.kind, Float4 | Float8))
    {
        return Err(ParseError::UndefinedOperator {
            op,
            left_type: sql_type_name(left),
            right_type: sql_type_name(right),
        });
    }
    if matches!(left.kind, Float8) || matches!(right.kind, Float8) {
        return Ok(SqlType::new(Float8));
    }
    if matches!(left.kind, Float4) || matches!(right.kind, Float4) {
        return Ok(SqlType::new(Float4));
    }
    if matches!(left.kind, Numeric) || matches!(right.kind, Numeric) {
        return Ok(SqlType::new(Numeric));
    }
    if matches!(left.kind, Int8) || matches!(right.kind, Int8) {
        return Ok(SqlType::new(Int8));
    }
    if matches!(left.kind, Int4 | Oid) || matches!(right.kind, Int4 | Oid) {
        return Ok(SqlType::new(Int4));
    }
    Ok(SqlType::new(Int2))
}

pub(super) fn sql_type_name(ty: SqlType) -> String {
    match ty.kind {
        SqlTypeKind::Int2 => "smallint",
        SqlTypeKind::Int4 => "integer",
        SqlTypeKind::Int8 => "bigint",
        SqlTypeKind::Oid => "oid",
        SqlTypeKind::Float4 => "real",
        SqlTypeKind::Float8 => "double precision",
        SqlTypeKind::Numeric => "numeric",
        SqlTypeKind::Json => "json",
        SqlTypeKind::Jsonb => "jsonb",
        SqlTypeKind::JsonPath => "jsonpath",
        SqlTypeKind::Text => "text",
        SqlTypeKind::Bool => "boolean",
        SqlTypeKind::Timestamp => "timestamp",
        SqlTypeKind::Char => "character",
        SqlTypeKind::Varchar => "character varying",
    }
    .to_string()
}

pub(super) fn is_numeric_family(ty: SqlType) -> bool {
    matches!(
        ty.element_type().kind,
        SqlTypeKind::Int2
            | SqlTypeKind::Int4
            | SqlTypeKind::Int8
            | SqlTypeKind::Oid
            | SqlTypeKind::Float4
            | SqlTypeKind::Float8
            | SqlTypeKind::Numeric
    )
}

pub(super) fn is_integer_family(ty: SqlType) -> bool {
    matches!(
        ty.element_type().kind,
        SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8 | SqlTypeKind::Oid
    )
}

fn is_text_like_type(ty: SqlType) -> bool {
    matches!(
        ty.element_type().kind,
        SqlTypeKind::Text | SqlTypeKind::Char | SqlTypeKind::Varchar
    )
}

fn is_string_literal_expr(expr: &SqlExpr) -> bool {
    matches!(
        expr,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    )
}

pub(super) fn should_use_text_concat(
    left_expr: &SqlExpr,
    left_type: SqlType,
    right_expr: &SqlExpr,
    right_type: SqlType,
) -> bool {
    if left_type.is_array || right_type.is_array {
        return false;
    }
    is_text_like_type(left_type)
        || is_text_like_type(right_type)
        || is_string_literal_expr(left_expr)
        || is_string_literal_expr(right_expr)
}

pub(super) fn resolve_common_scalar_type(left: SqlType, right: SqlType) -> Option<SqlType> {
    let left = left.element_type();
    let right = right.element_type();
    if left == right {
        return Some(left);
    }
    if is_text_like_type(left) && is_text_like_type(right) {
        return Some(SqlType::new(SqlTypeKind::Text));
    }
    if is_numeric_family(left) && is_numeric_family(right) {
        return resolve_numeric_binary_type("+", left, right).ok();
    }
    None
}

pub(super) fn resolve_array_concat_element_type(
    left: SqlType,
    right: SqlType,
) -> Result<SqlType, ParseError> {
    let left_elem = left.element_type();
    let right_elem = right.element_type();
    if left.is_array && right.is_array {
        return resolve_common_scalar_type(left_elem, right_elem).ok_or(ParseError::UndefinedOperator {
            op: "||",
            left_type: sql_type_name(left),
            right_type: sql_type_name(right),
        });
    }
    if left.is_array {
        return resolve_common_scalar_type(left_elem, right_elem).ok_or(ParseError::UndefinedOperator {
            op: "||",
            left_type: sql_type_name(left),
            right_type: sql_type_name(right),
        });
    }
    if right.is_array {
        return resolve_common_scalar_type(left_elem, right_elem).ok_or(ParseError::UndefinedOperator {
            op: "||",
            left_type: sql_type_name(left),
            right_type: sql_type_name(right),
        });
    }
    Err(ParseError::UndefinedOperator {
        op: "||",
        left_type: sql_type_name(left),
        right_type: sql_type_name(right),
    })
}

pub(super) fn infer_integer_literal_type(value: &str) -> SqlType {
    if value.parse::<i32>().is_ok() {
        SqlType::new(SqlTypeKind::Int4)
    } else if value.parse::<i64>().is_ok() {
        SqlType::new(SqlTypeKind::Int8)
    } else {
        SqlType::new(SqlTypeKind::Numeric)
    }
}

pub(super) fn infer_arithmetic_sql_type(expr: &SqlExpr, left: SqlType, right: SqlType) -> SqlType {
    use SqlTypeKind::*;

    let left = left.element_type();
    let right = right.element_type();

    let has_float8 = matches!(left.kind, Float8) || matches!(right.kind, Float8);
    let has_float4 = matches!(left.kind, Float4) || matches!(right.kind, Float4);
    if has_float8 {
        return SqlType::new(Float8);
    }
    if has_float4 {
        return SqlType::new(Float4);
    }
    if matches!(left.kind, Numeric) || matches!(right.kind, Numeric) {
        return SqlType::new(Numeric);
    }

    let widest_int = if matches!(left.kind, Int8) || matches!(right.kind, Int8) {
        Int8
    } else if matches!(left.kind, Int4) || matches!(right.kind, Int4) {
        Int4
    } else {
        Int2
    };

    match expr {
        SqlExpr::Div(_, _) | SqlExpr::Mod(_, _) => SqlType::new(widest_int),
        SqlExpr::Add(_, _) | SqlExpr::Sub(_, _) | SqlExpr::Mul(_, _) => SqlType::new(widest_int),
        SqlExpr::BitAnd(_, _)
        | SqlExpr::BitOr(_, _)
        | SqlExpr::BitXor(_, _)
        | SqlExpr::Shl(_, _)
        | SqlExpr::Shr(_, _) => left,
        _ => SqlType::new(Int4),
    }
}

pub(super) fn infer_concat_sql_type(expr: &SqlExpr, left: SqlType, right: SqlType) -> SqlType {
    let _ = expr;
    if left.kind == SqlTypeKind::Jsonb && !left.is_array && right.kind == SqlTypeKind::Jsonb && !right.is_array {
        return SqlType::new(SqlTypeKind::Jsonb);
    }
    if left.is_array || right.is_array {
        if let Ok(element_type) = resolve_array_concat_element_type(left, right) {
            return SqlType::array_of(element_type);
        }
    }
    SqlType::new(SqlTypeKind::Text)
}

pub(super) fn bind_integer_literal(value: &str) -> Result<Value, ParseError> {
    if let Ok(parsed) = value.parse::<i32>() {
        Ok(Value::Int32(parsed))
    } else if let Ok(parsed) = value.parse::<i64>() {
        Ok(Value::Int64(parsed))
    } else if value.chars().all(|ch| ch.is_ascii_digit()) {
        Ok(Value::Numeric(value.into()))
    } else {
        Err(ParseError::InvalidInteger(value.to_string()))
    }
}

pub(super) fn bind_numeric_literal(value: &str) -> Result<Value, ParseError> {
    value
        .parse::<f64>()
        .map(|_| Value::Numeric(value.into()))
        .map_err(|_| ParseError::InvalidNumeric(value.to_string()))
}
