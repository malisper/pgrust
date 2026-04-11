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
        "abs" => Some(BuiltinScalarFunction::Abs),
        "gcd" => Some(BuiltinScalarFunction::Gcd),
        "lcm" => Some(BuiltinScalarFunction::Lcm),
        "pg_input_is_valid" => Some(BuiltinScalarFunction::PgInputIsValid),
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
        BuiltinScalarFunction::Abs => args.len() == 1,
        BuiltinScalarFunction::Gcd | BuiltinScalarFunction::Lcm => args.len() == 2,
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
        BuiltinScalarFunction::Left
        | BuiltinScalarFunction::Repeat
        | BuiltinScalarFunction::PgInputIsValid
        | BuiltinScalarFunction::PgInputErrorMessage
        | BuiltinScalarFunction::PgInputErrorDetail
        | BuiltinScalarFunction::PgInputErrorHint
        | BuiltinScalarFunction::PgInputErrorSqlState => args.len() == 2,
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
