use super::functions::{resolve_function_call, resolve_scalar_function};
use super::ranges::infer_range_special_expr_type_with_ctes;
use super::*;
use crate::backend::utils::record::assign_anonymous_record_descriptor;
use crate::include::catalog::builtin_range_spec_for_sql_type;
use crate::include::nodes::primnodes::expr_sql_type_hint;

pub(super) fn infer_sql_expr_type(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> SqlType {
    infer_sql_expr_type_with_ctes(expr, scope, catalog, outer_scopes, grouped_outer, &[])
}

pub(super) fn infer_sql_expr_type_with_ctes(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> SqlType {
    if let Some(sql_type) = infer_geometry_special_expr_type_with_ctes(
        expr,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ) {
        return sql_type;
    }

    if let Some(sql_type) = infer_range_special_expr_type_with_ctes(
        expr,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ) {
        return sql_type;
    }

    match expr {
        SqlExpr::Column(name) => {
            if name.ends_with(".*") {
                infer_sql_row_expr_type(
                    std::slice::from_ref(expr),
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            } else {
                resolve_system_column_with_outer(scope, outer_scopes, name)
                    .ok()
                    .flatten()
                    .map(|resolved| resolved.sql_type)
                    .or_else(|| {
                        match resolve_column_with_outer(scope, outer_scopes, name, grouped_outer) {
                            Ok(ResolvedColumn::Local(idx)) => {
                                scope.desc.columns.get(idx).map(|c| c.sql_type)
                            }
                            Ok(ResolvedColumn::Outer { depth, index }) => outer_scopes
                                .get(depth)
                                .and_then(|s| s.desc.columns.get(index).map(|c| c.sql_type)),
                            Err(_) => None,
                        }
                    })
                    .unwrap_or(SqlType::new(SqlTypeKind::Text))
            }
        }
        SqlExpr::Default => SqlType::new(SqlTypeKind::Text),
        SqlExpr::Const(Value::Int16(_)) => SqlType::new(SqlTypeKind::Int2),
        SqlExpr::Const(Value::Int32(_)) => SqlType::new(SqlTypeKind::Int4),
        SqlExpr::Const(Value::Int64(_)) => SqlType::new(SqlTypeKind::Int8),
        SqlExpr::Const(Value::Money(_)) => SqlType::new(SqlTypeKind::Money),
        SqlExpr::Const(Value::Date(_)) => SqlType::new(SqlTypeKind::Date),
        SqlExpr::Const(Value::Time(_)) => SqlType::new(SqlTypeKind::Time),
        SqlExpr::Const(Value::TimeTz(_)) => SqlType::new(SqlTypeKind::TimeTz),
        SqlExpr::Const(Value::Timestamp(_)) => SqlType::new(SqlTypeKind::Timestamp),
        SqlExpr::Const(Value::TimestampTz(_)) => SqlType::new(SqlTypeKind::TimestampTz),
        SqlExpr::Const(Value::Range(range)) => range.range_type.sql_type,
        SqlExpr::Const(Value::Bit(v)) => SqlType::with_bit_len(SqlTypeKind::VarBit, v.bit_len),
        SqlExpr::Const(Value::Bytea(_)) => SqlType::new(SqlTypeKind::Bytea),
        SqlExpr::Const(Value::Bool(_)) => SqlType::new(SqlTypeKind::Bool),
        SqlExpr::Row(items) => {
            infer_sql_row_expr_type(items, scope, catalog, outer_scopes, grouped_outer, ctes)
        }
        SqlExpr::Const(Value::Numeric(_)) => SqlType::new(SqlTypeKind::Numeric),
        SqlExpr::Const(Value::Json(_)) => SqlType::new(SqlTypeKind::Json),
        SqlExpr::Const(Value::Jsonb(_)) => SqlType::new(SqlTypeKind::Jsonb),
        SqlExpr::Const(Value::JsonPath(_)) => SqlType::new(SqlTypeKind::JsonPath),
        SqlExpr::Const(Value::Point(_)) => SqlType::new(SqlTypeKind::Point),
        SqlExpr::Const(Value::Lseg(_)) => SqlType::new(SqlTypeKind::Lseg),
        SqlExpr::Const(Value::Path(_)) => SqlType::new(SqlTypeKind::Path),
        SqlExpr::Const(Value::Line(_)) => SqlType::new(SqlTypeKind::Line),
        SqlExpr::Const(Value::Box(_)) => SqlType::new(SqlTypeKind::Box),
        SqlExpr::Const(Value::Polygon(_)) => SqlType::new(SqlTypeKind::Polygon),
        SqlExpr::Const(Value::Circle(_)) => SqlType::new(SqlTypeKind::Circle),
        SqlExpr::Const(Value::TsVector(_)) => SqlType::new(SqlTypeKind::TsVector),
        SqlExpr::Const(Value::TsQuery(_)) => SqlType::new(SqlTypeKind::TsQuery),
        SqlExpr::Const(Value::InternalChar(_)) => SqlType::new(SqlTypeKind::InternalChar),
        SqlExpr::Const(Value::Record(record)) => record.sql_type(),
        SqlExpr::Const(Value::Text(_))
        | SqlExpr::Const(Value::TextRef(_, _))
        | SqlExpr::Const(Value::Null) => SqlType::new(SqlTypeKind::Text),
        SqlExpr::Const(Value::Array(_)) => SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
        SqlExpr::Const(Value::PgArray(_)) => SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
        SqlExpr::Const(Value::Float64(_)) => SqlType::new(SqlTypeKind::Float8),
        SqlExpr::ArraySubscript { array, subscripts } => {
            let array_type = infer_sql_expr_type_with_ctes(
                array,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if subscripts.iter().any(|subscript| subscript.upper.is_some()) {
                SqlType::array_of(array_type.element_type())
            } else {
                array_type.element_type()
            }
        }
        SqlExpr::IntegerLiteral(value) => infer_integer_literal_type(value),
        SqlExpr::NumericLiteral(_) => SqlType::new(SqlTypeKind::Numeric),
        SqlExpr::Add(left, right)
        | SqlExpr::BitAnd(left, right)
        | SqlExpr::BitOr(left, right)
        | SqlExpr::BitXor(left, right)
        | SqlExpr::Shl(left, right)
        | SqlExpr::Shr(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right)
        | SqlExpr::Mod(left, right) => infer_arithmetic_sql_type(
            expr,
            infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes),
            infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes),
        ),
        SqlExpr::Sub(left, right) => {
            let left_type = infer_sql_expr_type_with_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let right_type = infer_sql_expr_type_with_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if left_type == SqlType::new(SqlTypeKind::Jsonb)
                && (!right_type.is_array && is_integer_family(right_type)
                    || (!right_type.is_array && matches!(right_type.kind, SqlTypeKind::Text))
                    || right_type == SqlType::array_of(SqlType::new(SqlTypeKind::Text)))
            {
                SqlType::new(SqlTypeKind::Jsonb)
            } else {
                infer_arithmetic_sql_type(expr, left_type, right_type)
            }
        }
        SqlExpr::Concat(left, right) => infer_concat_sql_type(
            expr,
            infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes),
            infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes),
        ),
        SqlExpr::BinaryOperator { op, left, right } => {
            let left_type = infer_sql_expr_type_with_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let right_type = infer_sql_expr_type_with_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            match op.as_str() {
                "@@" => SqlType::new(SqlTypeKind::Bool),
                "||" if matches!(left_type.element_type().kind, SqlTypeKind::TsVector)
                    && matches!(right_type.element_type().kind, SqlTypeKind::TsVector) =>
                {
                    SqlType::new(SqlTypeKind::TsVector)
                }
                "&&" | "||"
                    if matches!(left_type.element_type().kind, SqlTypeKind::TsQuery)
                        && matches!(right_type.element_type().kind, SqlTypeKind::TsQuery) =>
                {
                    SqlType::new(SqlTypeKind::TsQuery)
                }
                _ => SqlType::new(SqlTypeKind::Text),
            }
        }
        SqlExpr::UnaryPlus(inner) => {
            infer_sql_expr_type_with_ctes(inner, scope, catalog, outer_scopes, grouped_outer, ctes)
        }
        SqlExpr::Negate(inner) => {
            infer_sql_expr_type_with_ctes(inner, scope, catalog, outer_scopes, grouped_outer, ctes)
        }
        SqlExpr::BitNot(inner) => {
            infer_sql_expr_type_with_ctes(inner, scope, catalog, outer_scopes, grouped_outer, ctes)
        }
        SqlExpr::PrefixOperator { op, expr } => match op.as_str() {
            "!!" => SqlType::new(SqlTypeKind::TsQuery),
            _ => infer_sql_expr_type_with_ctes(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        },
        SqlExpr::Cast(_, ty) => {
            resolve_raw_type_name(ty, catalog).unwrap_or_else(|_| raw_type_name_hint(ty))
        }
        SqlExpr::FieldSelect { expr, field } => {
            if let SqlExpr::Column(name) = expr.as_ref()
                && let Some(fields) = resolve_relation_row_expr_with_outer(scope, outer_scopes, name)
                && let Some((_, field_expr)) = fields
                    .iter()
                    .find(|(candidate, _)| candidate.eq_ignore_ascii_case(field))
            {
                expr_sql_type_hint(field_expr).unwrap_or(SqlType::new(SqlTypeKind::Text))
            } else {
                SqlType::new(SqlTypeKind::Text)
            }
        }
        SqlExpr::Eq(_, _)
        | SqlExpr::NotEq(_, _)
        | SqlExpr::Lt(_, _)
        | SqlExpr::LtEq(_, _)
        | SqlExpr::Gt(_, _)
        | SqlExpr::GtEq(_, _)
        | SqlExpr::RegexMatch(_, _)
        | SqlExpr::Like { .. }
        | SqlExpr::Similar { .. }
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
        SqlExpr::Case {
            args, defresult, ..
        } => {
            let mut common = defresult.as_deref().map(|expr| {
                infer_sql_expr_type_with_ctes(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
                .element_type()
            });
            for arm in args {
                if matches!(arm.result, SqlExpr::Const(Value::Null)) {
                    continue;
                }
                let ty = infer_sql_expr_type_with_ctes(
                    &arm.result,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
                .element_type();
                common = Some(match common {
                    None => ty,
                    Some(current) => resolve_common_scalar_type(current, ty).unwrap_or(ty),
                });
            }
            common.unwrap_or(SqlType::new(SqlTypeKind::Text))
        }
        SqlExpr::JsonGet(left, _) | SqlExpr::JsonPath(left, _) => {
            let left_type = infer_sql_expr_type_with_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if matches!(left_type.element_type().kind, SqlTypeKind::Jsonb) {
                SqlType::new(SqlTypeKind::Jsonb)
            } else {
                SqlType::new(SqlTypeKind::Json)
            }
        }
        SqlExpr::JsonGetText(_, _) | SqlExpr::JsonPathText(_, _) => SqlType::new(SqlTypeKind::Text),
        SqlExpr::AggCall { func, args, .. } => aggregate_sql_type(
            *func,
            function_arg_values(args).next().map(|expr| {
                infer_sql_expr_type_with_ctes(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            }),
        ),
        SqlExpr::ArrayLiteral(elements) => infer_array_literal_type_with_ctes(
            elements,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )
        .unwrap_or(SqlType::array_of(SqlType::new(SqlTypeKind::Text))),
        SqlExpr::ScalarSubquery(select) => bind_select_query_with_outer(
            select,
            catalog,
            outer_scopes,
            grouped_outer.cloned(),
            ctes,
            &[],
        )
        .ok()
        .and_then(|(plan, _)| {
            let cols = plan.columns();
            if cols.len() == 1 {
                Some(cols[0].sql_type)
            } else {
                None
            }
        })
        .unwrap_or(SqlType::new(SqlTypeKind::Text)),
        SqlExpr::ArraySubquery(select) => SqlType::array_of(
            bind_select_query_with_outer(
                select,
                catalog,
                outer_scopes,
                grouped_outer.cloned(),
                ctes,
                &[],
            )
            .ok()
            .and_then(|(plan, _)| {
                let cols = plan.columns();
                if cols.len() == 1 {
                    Some(cols[0].sql_type)
                } else {
                    None
                }
            })
            .unwrap_or(SqlType::new(SqlTypeKind::Text)),
        ),
        SqlExpr::Exists(_) | SqlExpr::InSubquery { .. } | SqlExpr::QuantifiedSubquery { .. } => {
            SqlType::new(SqlTypeKind::Bool)
        }
        SqlExpr::Random => SqlType::new(SqlTypeKind::Float8),
        SqlExpr::FuncCall {
            name,
            args,
            func_variadic,
            ..
        } => {
            if name.eq_ignore_ascii_case("coalesce") {
                let values = args.iter().map(|arg| arg.value.clone()).collect::<Vec<_>>();
                return infer_common_scalar_expr_type_with_ctes(
                    &values,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                    "COALESCE arguments with a common type",
                )
                .unwrap_or(SqlType::new(SqlTypeKind::Text));
            }
            let actual_types = args
                .iter()
                .map(|arg| {
                    infer_sql_expr_type_with_ctes(
                        &arg.value,
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
                return resolved.result_type;
            }
            let resolved = resolve_scalar_function(name);
            if let Some(BuiltinScalarFunction::RangeConstructor) = resolved
                && let Some(target_type) = resolve_function_cast_type(catalog, name)
                && builtin_range_spec_for_sql_type(target_type).is_some()
            {
                return target_type;
            }
            if let Some(func) = resolved
                && let Some(sql_type) = fixed_scalar_return_type(func)
            {
                return sql_type;
            }
            if let Some(func) = resolved
                && let Some(sql_type) = infer_geometry_function_return_type_with_ctes(
                    func,
                    args,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            {
                return sql_type;
            }
            match resolved {
                Some(BuiltinScalarFunction::TsMatch) => SqlType::new(SqlTypeKind::Bool),
                Some(BuiltinScalarFunction::ToTsVector) => SqlType::new(SqlTypeKind::TsVector),
                Some(
                    BuiltinScalarFunction::ToTsQuery
                    | BuiltinScalarFunction::PlainToTsQuery
                    | BuiltinScalarFunction::PhraseToTsQuery
                    | BuiltinScalarFunction::WebSearchToTsQuery,
                ) => SqlType::new(SqlTypeKind::TsQuery),
                Some(BuiltinScalarFunction::TsLexize) => {
                    SqlType::array_of(SqlType::new(SqlTypeKind::Text))
                }
                Some(
                    BuiltinScalarFunction::TsQueryAnd
                    | BuiltinScalarFunction::TsQueryOr
                    | BuiltinScalarFunction::TsQueryNot,
                ) => SqlType::new(SqlTypeKind::TsQuery),
                Some(BuiltinScalarFunction::TsVectorConcat) => SqlType::new(SqlTypeKind::TsVector),
                Some(BuiltinScalarFunction::Random) => SqlType::new(SqlTypeKind::Float8),
                Some(BuiltinScalarFunction::CashLarger | BuiltinScalarFunction::CashSmaller) => {
                    SqlType::new(SqlTypeKind::Money)
                }
                Some(BuiltinScalarFunction::CashWords) => SqlType::new(SqlTypeKind::Text),
                Some(BuiltinScalarFunction::ArrayNdims)
                | Some(BuiltinScalarFunction::ArrayLower) => SqlType::new(SqlTypeKind::Int4),
                Some(BuiltinScalarFunction::ArrayDims) => SqlType::new(SqlTypeKind::Text),
                Some(BuiltinScalarFunction::Now)
                | Some(BuiltinScalarFunction::TransactionTimestamp)
                | Some(BuiltinScalarFunction::StatementTimestamp)
                | Some(BuiltinScalarFunction::ClockTimestamp) => {
                    SqlType::new(SqlTypeKind::TimestampTz)
                }
                Some(BuiltinScalarFunction::TimeOfDay) => SqlType::new(SqlTypeKind::Text),
                Some(BuiltinScalarFunction::DatePart) => SqlType::new(SqlTypeKind::Float8),
                Some(BuiltinScalarFunction::DateTrunc) => match args.get(1).map(|arg| {
                    infer_sql_expr_type_with_ctes(
                        &arg.value,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                }) {
                    Some(SqlType {
                        kind: SqlTypeKind::Date,
                        ..
                    })
                    | Some(SqlType {
                        kind: SqlTypeKind::TimestampTz,
                        ..
                    }) => SqlType::new(SqlTypeKind::TimestampTz),
                    Some(SqlType {
                        kind: SqlTypeKind::Timestamp,
                        ..
                    }) => SqlType::new(SqlTypeKind::Timestamp),
                    _ => SqlType::new(SqlTypeKind::Timestamp),
                },
                Some(BuiltinScalarFunction::IsFinite) => SqlType::new(SqlTypeKind::Bool),
                Some(BuiltinScalarFunction::MakeDate) => SqlType::new(SqlTypeKind::Date),
                Some(BuiltinScalarFunction::ToJson)
                | Some(BuiltinScalarFunction::ArrayToJson)
                | Some(BuiltinScalarFunction::JsonBuildArray)
                | Some(BuiltinScalarFunction::JsonBuildObject)
                | Some(BuiltinScalarFunction::JsonObject)
                | Some(BuiltinScalarFunction::JsonStripNulls) => SqlType::new(SqlTypeKind::Json),
                Some(BuiltinScalarFunction::ToJsonb)
                | Some(BuiltinScalarFunction::JsonbObject)
                | Some(BuiltinScalarFunction::JsonbExtractPath)
                | Some(BuiltinScalarFunction::JsonbStripNulls)
                | Some(BuiltinScalarFunction::JsonbBuildArray)
                | Some(BuiltinScalarFunction::JsonbBuildObject)
                | Some(BuiltinScalarFunction::JsonbDelete)
                | Some(BuiltinScalarFunction::JsonbDeletePath)
                | Some(BuiltinScalarFunction::JsonbSet)
                | Some(BuiltinScalarFunction::JsonbSetLax)
                | Some(BuiltinScalarFunction::JsonbInsert)
                | Some(BuiltinScalarFunction::JsonbPathQueryArray)
                | Some(BuiltinScalarFunction::JsonbPathQueryFirst) => {
                    SqlType::new(SqlTypeKind::Jsonb)
                }
                Some(BuiltinScalarFunction::GetDatabaseEncoding)
                | Some(BuiltinScalarFunction::Initcap)
                | Some(BuiltinScalarFunction::JsonTypeof)
                | Some(BuiltinScalarFunction::JsonExtractPathText)
                | Some(BuiltinScalarFunction::JsonbPretty)
                | Some(BuiltinScalarFunction::JsonbTypeof)
                | Some(BuiltinScalarFunction::JsonbExtractPathText)
                | Some(BuiltinScalarFunction::BpcharToText)
                | Some(BuiltinScalarFunction::Concat)
                | Some(BuiltinScalarFunction::ConcatWs)
                | Some(BuiltinScalarFunction::Format)
                | Some(BuiltinScalarFunction::Lower)
                | Some(BuiltinScalarFunction::Unistr)
                | Some(BuiltinScalarFunction::BTrim)
                | Some(BuiltinScalarFunction::LTrim)
                | Some(BuiltinScalarFunction::RTrim)
                | Some(BuiltinScalarFunction::Left)
                | Some(BuiltinScalarFunction::Right)
                | Some(BuiltinScalarFunction::LPad)
                | Some(BuiltinScalarFunction::RPad)
                | Some(BuiltinScalarFunction::Repeat)
                | Some(BuiltinScalarFunction::Md5)
                | Some(BuiltinScalarFunction::Chr)
                | Some(BuiltinScalarFunction::QuoteLiteral)
                | Some(BuiltinScalarFunction::Replace)
                | Some(BuiltinScalarFunction::SplitPart)
                | Some(BuiltinScalarFunction::Translate)
                | Some(BuiltinScalarFunction::ConvertFrom)
                | Some(BuiltinScalarFunction::Encode)
                | Some(BuiltinScalarFunction::RegexpSubstr)
                | Some(BuiltinScalarFunction::RegexpReplace)
                | Some(BuiltinScalarFunction::SimilarSubstring)
                | Some(BuiltinScalarFunction::PgLsn) => SqlType::new(SqlTypeKind::Text),
                Some(BuiltinScalarFunction::Decode)
                | Some(BuiltinScalarFunction::Sha224)
                | Some(BuiltinScalarFunction::Sha256)
                | Some(BuiltinScalarFunction::Sha384)
                | Some(BuiltinScalarFunction::Sha512) => SqlType::new(SqlTypeKind::Bytea),
                Some(BuiltinScalarFunction::Length)
                | Some(BuiltinScalarFunction::ArrayLength)
                | Some(BuiltinScalarFunction::Cardinality)
                | Some(BuiltinScalarFunction::ArrayPosition)
                | Some(BuiltinScalarFunction::Ascii)
                | Some(BuiltinScalarFunction::RegexpCount)
                | Some(BuiltinScalarFunction::RegexpInstr)
                | Some(BuiltinScalarFunction::JsonArrayLength)
                | Some(BuiltinScalarFunction::JsonbArrayLength)
                | Some(BuiltinScalarFunction::Scale)
                | Some(BuiltinScalarFunction::MinScale)
                | Some(BuiltinScalarFunction::WidthBucket)
                | Some(BuiltinScalarFunction::GetByte) => SqlType::new(SqlTypeKind::Int4),
                Some(BuiltinScalarFunction::Crc32)
                | Some(BuiltinScalarFunction::Crc32c)
                | Some(BuiltinScalarFunction::BitCount) => SqlType::new(SqlTypeKind::Int8),
                Some(
                    BuiltinScalarFunction::RegexpMatch | BuiltinScalarFunction::RegexpSplitToArray,
                ) => SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                Some(BuiltinScalarFunction::StringToArray) => {
                    SqlType::array_of(SqlType::new(SqlTypeKind::Text))
                }
                Some(BuiltinScalarFunction::ArrayPositions) => {
                    SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
                }
                Some(BuiltinScalarFunction::Position | BuiltinScalarFunction::Strpos) => {
                    SqlType::new(SqlTypeKind::Int4)
                }
                Some(BuiltinScalarFunction::ArrayToString) => SqlType::new(SqlTypeKind::Text),
                Some(BuiltinScalarFunction::ArrayFill) => function_arg_values(args).next().map_or(
                    SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                    |arg| {
                        SqlType::array_of(
                            infer_sql_expr_type_with_ctes(
                                arg,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            )
                            .element_type(),
                        )
                    },
                ),
                Some(
                    BuiltinScalarFunction::ArrayRemove
                    | BuiltinScalarFunction::ArrayReplace
                    | BuiltinScalarFunction::ArraySort,
                ) => function_arg_values(args).next().map_or(
                    SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                    |arg| {
                        infer_sql_expr_type_with_ctes(
                            arg,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        )
                    },
                ),
                Some(BuiltinScalarFunction::Substring | BuiltinScalarFunction::Overlay) => {
                    function_arg_values(args).next().map_or(
                        SqlType::new(SqlTypeKind::Text),
                        |arg| {
                            infer_sql_expr_type_with_ctes(
                                arg,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            )
                        },
                    )
                }
                Some(BuiltinScalarFunction::Reverse) => function_arg_values(args).next().map_or(
                    SqlType::new(SqlTypeKind::Text),
                    |arg| {
                        infer_sql_expr_type_with_ctes(
                            arg,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        )
                    },
                ),
                Some(BuiltinScalarFunction::GetBit) => SqlType::new(SqlTypeKind::Int4),
                Some(BuiltinScalarFunction::SetBit | BuiltinScalarFunction::SetByte) => {
                    function_arg_values(args).next().map_or(
                        SqlType::new(SqlTypeKind::Text),
                        |arg| {
                            infer_sql_expr_type_with_ctes(
                                arg,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            )
                        },
                    )
                }
                Some(BuiltinScalarFunction::JsonbPathExists)
                | Some(BuiltinScalarFunction::JsonbPathMatch)
                | Some(BuiltinScalarFunction::RegexpLike) => SqlType::new(SqlTypeKind::Bool),
                Some(BuiltinScalarFunction::JsonExtractPath) => SqlType::new(SqlTypeKind::Json),
                Some(BuiltinScalarFunction::Abs) => function_arg_values(args).next().map_or(
                    SqlType::new(SqlTypeKind::Text),
                    |arg| {
                        infer_sql_expr_type_with_ctes(
                            arg,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        )
                    },
                ),
                Some(BuiltinScalarFunction::Div)
                | Some(BuiltinScalarFunction::Mod)
                | Some(BuiltinScalarFunction::TrimScale)
                | Some(BuiltinScalarFunction::NumericInc)
                | Some(BuiltinScalarFunction::Factorial) => SqlType::new(SqlTypeKind::Numeric),
                Some(
                    BuiltinScalarFunction::Cbrt
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
                Some(
                    BuiltinScalarFunction::Sqrt
                    | BuiltinScalarFunction::Exp
                    | BuiltinScalarFunction::Ln,
                ) => args
                    .first()
                    .map_or(SqlType::new(SqlTypeKind::Float8), |arg| {
                        let ty = infer_sql_expr_type_with_ctes(
                            &arg.value,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        );
                        match ty.element_type().kind {
                            SqlTypeKind::Float4 | SqlTypeKind::Float8 => {
                                SqlType::new(SqlTypeKind::Float8)
                            }
                            _ if is_numeric_family(ty) => SqlType::new(SqlTypeKind::Numeric),
                            _ => SqlType::new(SqlTypeKind::Float8),
                        }
                    }),
                Some(BuiltinScalarFunction::Power) => {
                    let left = args.first().map(|arg| {
                        infer_sql_expr_type_with_ctes(
                            &arg.value,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        )
                    });
                    let right = args.get(1).map(|arg| {
                        infer_sql_expr_type_with_ctes(
                            &arg.value,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        )
                    });
                    match (left, right) {
                        (Some(left), Some(right))
                            if matches!(
                                left.element_type().kind,
                                SqlTypeKind::Float4 | SqlTypeKind::Float8
                            ) || matches!(
                                right.element_type().kind,
                                SqlTypeKind::Float4 | SqlTypeKind::Float8
                            ) =>
                        {
                            SqlType::new(SqlTypeKind::Float8)
                        }
                        (Some(left), Some(right))
                            if is_numeric_family(left) && is_numeric_family(right) =>
                        {
                            SqlType::new(SqlTypeKind::Numeric)
                        }
                        _ => SqlType::new(SqlTypeKind::Float8),
                    }
                }
                Some(BuiltinScalarFunction::Log | BuiltinScalarFunction::Log10) => {
                    if args.len() == 2 {
                        function_arg_values(args).next().map_or(
                            SqlType::new(SqlTypeKind::Float8),
                            |arg| {
                                let ty = infer_sql_expr_type_with_ctes(
                                    arg,
                                    scope,
                                    catalog,
                                    outer_scopes,
                                    grouped_outer,
                                    ctes,
                                );
                                match ty.element_type().kind {
                                    SqlTypeKind::Float4 | SqlTypeKind::Float8 => {
                                        SqlType::new(SqlTypeKind::Float8)
                                    }
                                    _ if is_numeric_family(ty) => {
                                        SqlType::new(SqlTypeKind::Numeric)
                                    }
                                    _ => SqlType::new(SqlTypeKind::Float8),
                                }
                            },
                        )
                    } else {
                        function_arg_values(args).next().map_or(
                            SqlType::new(SqlTypeKind::Float8),
                            |arg| {
                                let ty = infer_sql_expr_type_with_ctes(
                                    arg,
                                    scope,
                                    catalog,
                                    outer_scopes,
                                    grouped_outer,
                                    ctes,
                                );
                                match ty.element_type().kind {
                                    SqlTypeKind::Float4 | SqlTypeKind::Float8 => {
                                        SqlType::new(SqlTypeKind::Float8)
                                    }
                                    _ if is_numeric_family(ty) => {
                                        SqlType::new(SqlTypeKind::Numeric)
                                    }
                                    _ => SqlType::new(SqlTypeKind::Float8),
                                }
                            },
                        )
                    }
                }
                Some(
                    BuiltinScalarFunction::Ceil
                    | BuiltinScalarFunction::Ceiling
                    | BuiltinScalarFunction::Floor
                    | BuiltinScalarFunction::Sign,
                ) => function_arg_values(args).next().map_or(
                    SqlType::new(SqlTypeKind::Float8),
                    |arg| {
                        let ty = infer_sql_expr_type_with_ctes(
                            arg,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        );
                        match ty.element_type().kind {
                            SqlTypeKind::Float4 | SqlTypeKind::Float8 => {
                                SqlType::new(SqlTypeKind::Float8)
                            }
                            _ if is_numeric_family(ty) => SqlType::new(SqlTypeKind::Numeric),
                            _ => SqlType::new(SqlTypeKind::Float8),
                        }
                    },
                ),
                Some(BuiltinScalarFunction::Trunc | BuiltinScalarFunction::Round) => {
                    function_arg_values(args).next().map_or(
                        SqlType::new(SqlTypeKind::Float8),
                        |arg| {
                            let ty = infer_sql_expr_type_with_ctes(
                                arg,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            );
                            match ty.element_type().kind {
                                SqlTypeKind::Float4 | SqlTypeKind::Float8 => {
                                    SqlType::new(SqlTypeKind::Float8)
                                }
                                _ if is_numeric_family(ty) => SqlType::new(SqlTypeKind::Numeric),
                                _ => SqlType::new(SqlTypeKind::Float8),
                            }
                        },
                    )
                }
                Some(BuiltinScalarFunction::BitcastIntegerToFloat4) => {
                    SqlType::new(SqlTypeKind::Float4)
                }
                Some(BuiltinScalarFunction::BitcastBigintToFloat8) => {
                    SqlType::new(SqlTypeKind::Float8)
                }
                Some(BuiltinScalarFunction::Float4Send | BuiltinScalarFunction::Float8Send) => {
                    SqlType::new(SqlTypeKind::Text)
                }
                Some(BuiltinScalarFunction::BoolEq | BuiltinScalarFunction::BoolNe) => {
                    SqlType::new(SqlTypeKind::Bool)
                }
                Some(BuiltinScalarFunction::Gcd) | Some(BuiltinScalarFunction::Lcm) => {
                    function_arg_values(args)
                        .next()
                        .zip(function_arg_values(args).nth(1))
                        .map_or(SqlType::new(SqlTypeKind::Text), |(left, right)| {
                            resolve_numeric_binary_type(
                                "+",
                                infer_sql_expr_type_with_ctes(
                                    left,
                                    scope,
                                    catalog,
                                    outer_scopes,
                                    grouped_outer,
                                    ctes,
                                ),
                                infer_sql_expr_type_with_ctes(
                                    right,
                                    scope,
                                    catalog,
                                    outer_scopes,
                                    grouped_outer,
                                    ctes,
                                ),
                            )
                            .unwrap_or(SqlType::new(SqlTypeKind::Text))
                        })
                }
                Some(BuiltinScalarFunction::PgInputIsValid) => SqlType::new(SqlTypeKind::Bool),
                Some(BuiltinScalarFunction::ToNumber) => SqlType::new(SqlTypeKind::Numeric),
                Some(BuiltinScalarFunction::ToChar)
                | Some(BuiltinScalarFunction::PgInputErrorMessage)
                | Some(BuiltinScalarFunction::PgInputErrorDetail)
                | Some(BuiltinScalarFunction::PgInputErrorHint)
                | Some(BuiltinScalarFunction::PgInputErrorSqlState) => {
                    SqlType::new(SqlTypeKind::Text)
                }
                None => resolve_function_cast_type(catalog, name)
                    .unwrap_or(SqlType::new(SqlTypeKind::Text)),
                Some(_) => SqlType::new(SqlTypeKind::Text),
            }
        }
        SqlExpr::Subscript { .. }
        | SqlExpr::GeometryUnaryOp { .. }
        | SqlExpr::GeometryBinaryOp { .. } => unreachable!("handled before match"),
        SqlExpr::CurrentDate => SqlType::new(SqlTypeKind::Date),
        SqlExpr::CurrentTime { precision } => precision
            .map(|precision| SqlType::with_time_precision(SqlTypeKind::TimeTz, precision))
            .unwrap_or_else(|| SqlType::new(SqlTypeKind::TimeTz)),
        SqlExpr::CurrentTimestamp { precision } => precision
            .map(|precision| SqlType::with_time_precision(SqlTypeKind::TimestampTz, precision))
            .unwrap_or_else(|| SqlType::new(SqlTypeKind::TimestampTz)),
        SqlExpr::LocalTime { precision } => precision
            .map(|precision| SqlType::with_time_precision(SqlTypeKind::Time, precision))
            .unwrap_or_else(|| SqlType::new(SqlTypeKind::Time)),
        SqlExpr::LocalTimestamp { precision } => precision
            .map(|precision| SqlType::with_time_precision(SqlTypeKind::Timestamp, precision))
            .unwrap_or_else(|| SqlType::new(SqlTypeKind::Timestamp)),
    }
}

fn infer_sql_row_expr_type(
    items: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> SqlType {
    let mut fields = Vec::new();
    let mut next_index = 1usize;
    for item in items {
        if let SqlExpr::Column(name) = item
            && let Some(relation_name) = name.strip_suffix(".*")
            && let Some(relation_fields) =
                resolve_relation_row_expr_with_outer(scope, outer_scopes, relation_name)
        {
            for (field_name, expr) in relation_fields {
                fields.push((
                    field_name,
                    expr_sql_type_hint(&expr).unwrap_or(SqlType::new(SqlTypeKind::Text)),
                ));
            }
            next_index = fields.len() + 1;
            continue;
        }

        fields.push((
            format!("f{next_index}"),
            infer_sql_expr_type_with_ctes(item, scope, catalog, outer_scopes, grouped_outer, ctes),
        ));
        next_index += 1;
    }

    assign_anonymous_record_descriptor(fields).sql_type()
}

pub(super) fn infer_common_scalar_expr_type_with_ctes(
    exprs: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
    expected: &'static str,
) -> Result<SqlType, ParseError> {
    let mut common: Option<SqlType> = None;
    for expr in exprs {
        if matches!(expr, SqlExpr::Const(Value::Null)) {
            continue;
        }
        let ty =
            infer_sql_expr_type_with_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)
                .element_type();
        common = Some(match common {
            None => ty,
            Some(current) => resolve_common_scalar_type(current, ty).ok_or_else(|| {
                ParseError::UnexpectedToken {
                    expected,
                    actual: format!("{} and {}", sql_type_name(current), sql_type_name(ty)),
                }
            })?,
        });
    }
    Ok(common.unwrap_or(SqlType::new(SqlTypeKind::Text)))
}

pub(super) fn infer_array_literal_type_with_ctes(
    elements: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<SqlType> {
    let mut common = None;
    for element in elements {
        if matches!(element, SqlExpr::Const(Value::Null)) {
            continue;
        }
        let ty = infer_sql_expr_type_with_ctes(
            element,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        );
        common = Some(match common {
            None => ty.element_type(),
            Some(existing) => resolve_common_scalar_type(existing, ty)?,
        });
    }
    common.map(SqlType::array_of)
}

pub(super) fn infer_array_literal_type(
    elements: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
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
