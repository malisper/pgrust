use super::*;

pub(super) fn resolve_scalar_function(name: &str) -> Option<BuiltinScalarFunction> {
    match name.to_ascii_lowercase().as_str() {
        "random" => Some(BuiltinScalarFunction::Random),
        "getdatabaseencoding" => Some(BuiltinScalarFunction::GetDatabaseEncoding),
        "to_json" => Some(BuiltinScalarFunction::ToJson),
        "to_jsonb" => Some(BuiltinScalarFunction::ToJsonb),
        "array_to_json" => Some(BuiltinScalarFunction::ArrayToJson),
        "json_build_array" => Some(BuiltinScalarFunction::JsonBuildArray),
        "json_build_object" => Some(BuiltinScalarFunction::JsonBuildObject),
        "json_object" => Some(BuiltinScalarFunction::JsonObject),
        "json_typeof" => Some(BuiltinScalarFunction::JsonTypeof),
        "json_array_length" => Some(BuiltinScalarFunction::JsonArrayLength),
        "json_extract_path" => Some(BuiltinScalarFunction::JsonExtractPath),
        "json_extract_path_text" => Some(BuiltinScalarFunction::JsonExtractPathText),
        "jsonb_typeof" => Some(BuiltinScalarFunction::JsonbTypeof),
        "jsonb_array_length" => Some(BuiltinScalarFunction::JsonbArrayLength),
        "jsonb_extract_path" => Some(BuiltinScalarFunction::JsonbExtractPath),
        "jsonb_extract_path_text" => Some(BuiltinScalarFunction::JsonbExtractPathText),
        "jsonb_build_array" => Some(BuiltinScalarFunction::JsonbBuildArray),
        "jsonb_build_object" => Some(BuiltinScalarFunction::JsonbBuildObject),
        "jsonb_path_exists" => Some(BuiltinScalarFunction::JsonbPathExists),
        "jsonb_path_match" => Some(BuiltinScalarFunction::JsonbPathMatch),
        "jsonb_path_query_array" => Some(BuiltinScalarFunction::JsonbPathQueryArray),
        "jsonb_path_query_first" => Some(BuiltinScalarFunction::JsonbPathQueryFirst),
        "left" => Some(BuiltinScalarFunction::Left),
        "repeat" => Some(BuiltinScalarFunction::Repeat),
        "lower" => Some(BuiltinScalarFunction::Lower),
        "position" => Some(BuiltinScalarFunction::Position),
        "convert_from" => Some(BuiltinScalarFunction::ConvertFrom),
        "to_char" => Some(BuiltinScalarFunction::ToChar),
        "abs" => Some(BuiltinScalarFunction::Abs),
        "gcd" => Some(BuiltinScalarFunction::Gcd),
        "lcm" => Some(BuiltinScalarFunction::Lcm),
        "trunc" => Some(BuiltinScalarFunction::Trunc),
        "round" => Some(BuiltinScalarFunction::Round),
        "ceil" => Some(BuiltinScalarFunction::Ceil),
        "ceiling" => Some(BuiltinScalarFunction::Ceiling),
        "floor" => Some(BuiltinScalarFunction::Floor),
        "sign" => Some(BuiltinScalarFunction::Sign),
        "sqrt" => Some(BuiltinScalarFunction::Sqrt),
        "cbrt" => Some(BuiltinScalarFunction::Cbrt),
        "power" => Some(BuiltinScalarFunction::Power),
        "exp" => Some(BuiltinScalarFunction::Exp),
        "ln" => Some(BuiltinScalarFunction::Ln),
        "sinh" => Some(BuiltinScalarFunction::Sinh),
        "cosh" => Some(BuiltinScalarFunction::Cosh),
        "tanh" => Some(BuiltinScalarFunction::Tanh),
        "asinh" => Some(BuiltinScalarFunction::Asinh),
        "acosh" => Some(BuiltinScalarFunction::Acosh),
        "atanh" => Some(BuiltinScalarFunction::Atanh),
        "sind" => Some(BuiltinScalarFunction::Sind),
        "cosd" => Some(BuiltinScalarFunction::Cosd),
        "tand" => Some(BuiltinScalarFunction::Tand),
        "cotd" => Some(BuiltinScalarFunction::Cotd),
        "asind" => Some(BuiltinScalarFunction::Asind),
        "acosd" => Some(BuiltinScalarFunction::Acosd),
        "atand" => Some(BuiltinScalarFunction::Atand),
        "atan2d" => Some(BuiltinScalarFunction::Atan2d),
        "float4send" => Some(BuiltinScalarFunction::Float4Send),
        "float8send" => Some(BuiltinScalarFunction::Float8Send),
        "erf" => Some(BuiltinScalarFunction::Erf),
        "erfc" => Some(BuiltinScalarFunction::Erfc),
        "gamma" => Some(BuiltinScalarFunction::Gamma),
        "lgamma" => Some(BuiltinScalarFunction::Lgamma),
        "booleq" => Some(BuiltinScalarFunction::BoolEq),
        "boolne" => Some(BuiltinScalarFunction::BoolNe),
        "bitcast_integer_to_float4" => Some(BuiltinScalarFunction::BitcastIntegerToFloat4),
        "bitcast_bigint_to_float8" => Some(BuiltinScalarFunction::BitcastBigintToFloat8),
        "pg_input_is_valid" => Some(BuiltinScalarFunction::PgInputIsValid),
        _ => None,
    }
}

pub(super) fn resolve_function_cast_type(name: &str) -> Option<SqlType> {
    match name.to_ascii_lowercase().as_str() {
        "int2" | "smallint" => Some(SqlType::new(SqlTypeKind::Int2)),
        "int4" | "int" | "integer" => Some(SqlType::new(SqlTypeKind::Int4)),
        "int8" | "bigint" => Some(SqlType::new(SqlTypeKind::Int8)),
        "oid" => Some(SqlType::new(SqlTypeKind::Oid)),
        "float4" | "real" => Some(SqlType::new(SqlTypeKind::Float4)),
        "float8" => Some(SqlType::new(SqlTypeKind::Float8)),
        "numeric" | "decimal" => Some(SqlType::new(SqlTypeKind::Numeric)),
        "text" => Some(SqlType::new(SqlTypeKind::Text)),
        "bool" | "boolean" => Some(SqlType::new(SqlTypeKind::Bool)),
        _ => None,
    }
}

pub(super) fn resolve_json_table_function(name: &str) -> Option<JsonTableFunction> {
    match name.to_ascii_lowercase().as_str() {
        "json_object_keys" => Some(JsonTableFunction::ObjectKeys),
        "json_each" => Some(JsonTableFunction::Each),
        "json_each_text" => Some(JsonTableFunction::EachText),
        "json_array_elements" => Some(JsonTableFunction::ArrayElements),
        "json_array_elements_text" => Some(JsonTableFunction::ArrayElementsText),
        "jsonb_object_keys" => Some(JsonTableFunction::JsonbObjectKeys),
        "jsonb_each" => Some(JsonTableFunction::JsonbEach),
        "jsonb_each_text" => Some(JsonTableFunction::JsonbEachText),
        "jsonb_array_elements" => Some(JsonTableFunction::JsonbArrayElements),
        "jsonb_array_elements_text" => Some(JsonTableFunction::JsonbArrayElementsText),
        _ => None,
    }
}

pub(super) fn validate_scalar_function_arity(
    func: BuiltinScalarFunction,
    args: &[SqlExpr],
) -> Result<(), ParseError> {
    let valid = match func {
        BuiltinScalarFunction::Random => args.is_empty(),
        BuiltinScalarFunction::GetDatabaseEncoding => args.is_empty(),
        BuiltinScalarFunction::ToJson | BuiltinScalarFunction::ToJsonb => args.len() == 1,
        BuiltinScalarFunction::Abs
        | BuiltinScalarFunction::Lower
        | BuiltinScalarFunction::Trunc
        | BuiltinScalarFunction::Round
        | BuiltinScalarFunction::Ceil
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
        | BuiltinScalarFunction::Float4Send
        | BuiltinScalarFunction::Float8Send
        | BuiltinScalarFunction::Erf
        | BuiltinScalarFunction::Erfc
        | BuiltinScalarFunction::Gamma
        | BuiltinScalarFunction::Lgamma
        | BuiltinScalarFunction::BitcastIntegerToFloat4
        | BuiltinScalarFunction::BitcastBigintToFloat8
        | BuiltinScalarFunction::BpcharToText => args.len() == 1,
        BuiltinScalarFunction::Power
        | BuiltinScalarFunction::Atan2d
        | BuiltinScalarFunction::BoolEq
        | BuiltinScalarFunction::BoolNe => args.len() == 2,
        BuiltinScalarFunction::Gcd | BuiltinScalarFunction::Lcm => args.len() == 2,
        BuiltinScalarFunction::Position
        | BuiltinScalarFunction::ConvertFrom
        | BuiltinScalarFunction::Left
        | BuiltinScalarFunction::Repeat
        | BuiltinScalarFunction::ToChar
        | BuiltinScalarFunction::PgInputIsValid
        | BuiltinScalarFunction::PgInputErrorMessage
        | BuiltinScalarFunction::PgInputErrorDetail
        | BuiltinScalarFunction::PgInputErrorHint
        | BuiltinScalarFunction::PgInputErrorSqlState => args.len() == 2,
        BuiltinScalarFunction::ArrayToJson => matches!(args.len(), 1 | 2),
        BuiltinScalarFunction::JsonBuildArray | BuiltinScalarFunction::JsonBuildObject => true,
        BuiltinScalarFunction::JsonObject => matches!(args.len(), 1 | 2),
        BuiltinScalarFunction::JsonTypeof
        | BuiltinScalarFunction::JsonArrayLength
        | BuiltinScalarFunction::JsonbTypeof
        | BuiltinScalarFunction::JsonbArrayLength => args.len() == 1,
        BuiltinScalarFunction::JsonExtractPath
        | BuiltinScalarFunction::JsonExtractPathText
        | BuiltinScalarFunction::JsonbExtractPath
        | BuiltinScalarFunction::JsonbExtractPathText => !args.is_empty(),
        BuiltinScalarFunction::JsonbBuildArray | BuiltinScalarFunction::JsonbBuildObject => true,
        BuiltinScalarFunction::JsonbPathExists
        | BuiltinScalarFunction::JsonbPathMatch
        | BuiltinScalarFunction::JsonbPathQueryArray
        | BuiltinScalarFunction::JsonbPathQueryFirst => matches!(args.len(), 2..=4),
    };

    if valid {
        Ok(())
    } else {
        Err(ParseError::UnexpectedToken {
            expected: "valid builtin function arity",
            actual: format!("{func:?}({} args)", args.len()),
        })
    }
}

pub(super) fn validate_aggregate_arity(
    func: AggFunc,
    args: &[SqlExpr],
) -> Result<(), ParseError> {
    let valid = match func {
        AggFunc::Count => args.len() <= 1,
        AggFunc::Sum
        | AggFunc::Avg
        | AggFunc::Min
        | AggFunc::Max
        | AggFunc::JsonAgg
        | AggFunc::JsonbAgg => args.len() == 1,
        AggFunc::JsonObjectAgg | AggFunc::JsonbObjectAgg => args.len() == 2,
    };
    if valid {
        Ok(())
    } else {
        Err(ParseError::UnexpectedToken {
            expected: "valid aggregate arity",
            actual: format!("{}({} args)", func.name(), args.len()),
        })
    }
}
