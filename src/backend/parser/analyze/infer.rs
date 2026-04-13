use super::functions::resolve_scalar_function;
use super::*;

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
        SqlExpr::Default => SqlType::new(SqlTypeKind::Text),
        SqlExpr::Const(Value::Int16(_)) => SqlType::new(SqlTypeKind::Int2),
        SqlExpr::Const(Value::Int32(_)) => SqlType::new(SqlTypeKind::Int4),
        SqlExpr::Const(Value::Int64(_)) => SqlType::new(SqlTypeKind::Int8),
        SqlExpr::Const(Value::Bit(v)) => SqlType::with_bit_len(SqlTypeKind::VarBit, v.bit_len),
        SqlExpr::Const(Value::Bytea(_)) => SqlType::new(SqlTypeKind::Bytea),
        SqlExpr::Const(Value::Bool(_)) => SqlType::new(SqlTypeKind::Bool),
        SqlExpr::Const(Value::Numeric(_)) => SqlType::new(SqlTypeKind::Numeric),
        SqlExpr::Const(Value::Json(_)) => SqlType::new(SqlTypeKind::Json),
        SqlExpr::Const(Value::Jsonb(_)) => SqlType::new(SqlTypeKind::Jsonb),
        SqlExpr::Const(Value::JsonPath(_)) => SqlType::new(SqlTypeKind::JsonPath),
        SqlExpr::Const(Value::InternalChar(_)) => SqlType::new(SqlTypeKind::InternalChar),
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
            infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes),
            infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes),
        ),
        SqlExpr::Concat(left, right) => infer_concat_sql_type(
            expr,
            infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes),
            infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes),
        ),
        SqlExpr::UnaryPlus(inner) => {
            infer_sql_expr_type_with_ctes(inner, scope, catalog, outer_scopes, grouped_outer, ctes)
        }
        SqlExpr::Negate(inner) => {
            infer_sql_expr_type_with_ctes(inner, scope, catalog, outer_scopes, grouped_outer, ctes)
        }
        SqlExpr::BitNot(inner) => {
            infer_sql_expr_type_with_ctes(inner, scope, catalog, outer_scopes, grouped_outer, ctes)
        }
        SqlExpr::Cast(_, ty) => *ty,
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
        SqlExpr::ScalarSubquery(select) => {
            build_plan_with_outer(select, catalog, outer_scopes, grouped_outer.cloned(), ctes)
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
        SqlExpr::FuncCall { name, args } => {
            let resolved = resolve_scalar_function(name);
            if let Some(func) = resolved
                && let Some(sql_type) = fixed_scalar_return_type(func)
            {
                return sql_type;
            }
            match resolved {
                Some(BuiltinScalarFunction::Random) => SqlType::new(SqlTypeKind::Float8),
                Some(BuiltinScalarFunction::ArrayNdims)
                | Some(BuiltinScalarFunction::ArrayLower) => SqlType::new(SqlTypeKind::Int4),
                Some(BuiltinScalarFunction::ArrayDims) => SqlType::new(SqlTypeKind::Text),
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
                Some(BuiltinScalarFunction::Position | BuiltinScalarFunction::Strpos) => {
                    SqlType::new(SqlTypeKind::Int4)
                }
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
                Some(BuiltinScalarFunction::Reverse) => {
                    function_arg_values(args).next().map_or(SqlType::new(SqlTypeKind::Text), |arg| {
                        infer_sql_expr_type_with_ctes(
                            arg,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        )
                    })
                }
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
                    BuiltinScalarFunction::Sqrt
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
            }
        }
        SqlExpr::CurrentTimestamp => SqlType::new(SqlTypeKind::Timestamp),
    }
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
