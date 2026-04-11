use super::*;
use super::functions::resolve_scalar_function;

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
        | SqlExpr::BitAnd(left, right)
        | SqlExpr::BitOr(left, right)
        | SqlExpr::BitXor(left, right)
        | SqlExpr::Shl(left, right)
        | SqlExpr::Shr(left, right)
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
        SqlExpr::BitNot(inner) => {
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
        SqlExpr::JsonGetText(_, _) | SqlExpr::JsonPathText(_, _) => {
            SqlType::new(SqlTypeKind::Text)
        }
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
        SqlExpr::FuncCall { name, args } => match resolve_scalar_function(name) {
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
            Some(BuiltinScalarFunction::Abs) => args.first().map_or(
                SqlType::new(SqlTypeKind::Text),
                |arg| infer_sql_expr_type(arg, scope, catalog, outer_scopes, grouped_outer),
            ),
            Some(
                BuiltinScalarFunction::Trunc
                | BuiltinScalarFunction::Round
                | BuiltinScalarFunction::Ceil
                | BuiltinScalarFunction::Ceiling
                | BuiltinScalarFunction::Floor
                | BuiltinScalarFunction::Sign
                | BuiltinScalarFunction::Sqrt
                | BuiltinScalarFunction::Cbrt
                | BuiltinScalarFunction::Power
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
                | BuiltinScalarFunction::Atan2d
                | BuiltinScalarFunction::Erf
                | BuiltinScalarFunction::Erfc
                | BuiltinScalarFunction::Gamma
                | BuiltinScalarFunction::Lgamma,
            ) => SqlType::new(SqlTypeKind::Float8),
            Some(BuiltinScalarFunction::Float4Send | BuiltinScalarFunction::Float8Send) => {
                SqlType::new(SqlTypeKind::Text)
            }
            Some(BuiltinScalarFunction::Gcd) | Some(BuiltinScalarFunction::Lcm) => args.first().zip(args.get(1)).map_or(
                SqlType::new(SqlTypeKind::Text),
                |(left, right)| {
                    resolve_numeric_binary_type(
                        "+",
                        infer_sql_expr_type(left, scope, catalog, outer_scopes, grouped_outer),
                        infer_sql_expr_type(right, scope, catalog, outer_scopes, grouped_outer),
                    )
                    .unwrap_or(SqlType::new(SqlTypeKind::Text))
                },
            ),
            Some(BuiltinScalarFunction::PgInputIsValid) => SqlType::new(SqlTypeKind::Bool),
            Some(BuiltinScalarFunction::ToChar)
            | Some(BuiltinScalarFunction::PgInputErrorMessage)
            | Some(BuiltinScalarFunction::PgInputErrorDetail)
            | Some(BuiltinScalarFunction::PgInputErrorHint)
            | Some(BuiltinScalarFunction::PgInputErrorSqlState) => SqlType::new(SqlTypeKind::Text),
            None => resolve_function_cast_type(name).unwrap_or(SqlType::new(SqlTypeKind::Text)),
        },
        SqlExpr::CurrentTimestamp => SqlType::new(SqlTypeKind::Timestamp),
    }
}


pub(super) fn infer_array_literal_type(
    elements: &[SqlExpr],
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<SqlType, ParseError> {
    let mut common: Option<SqlType> = None;
    for element in elements {
        if matches!(element, SqlExpr::Const(Value::Null)) {
            continue;
        }
        let ty = infer_sql_expr_type(element, scope, catalog, outer_scopes, grouped_outer)
            .element_type();
        common = Some(match common {
            None => ty,
            Some(current) => resolve_common_scalar_type(current, ty).ok_or_else(|| {
                ParseError::UnexpectedToken {
                    expected: "array literal elements with a common type",
                    actual: format!("{} and {}", sql_type_name(current), sql_type_name(ty)),
                }
            })?,
        });
    }
    let Some(common) = common else {
        return Err(ParseError::UnexpectedToken {
        expected: "ARRAY[...] with a typed element or explicit cast",
        actual: "ARRAY[]".into(),
    });
    };
    Ok(SqlType::array_of(common))
}
