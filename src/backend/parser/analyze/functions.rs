use super::*;
use crate::include::catalog::{bootstrap_pg_operator_rows, bootstrap_pg_proc_rows, builtin_type_rows};
use std::collections::BTreeMap;
use std::sync::OnceLock;

pub(super) fn resolve_scalar_function(name: &str) -> Option<BuiltinScalarFunction> {
    scalar_functions_by_name()
        .get(&name.to_ascii_lowercase())
        .copied()
}

pub(super) fn resolve_function_cast_type(name: &str) -> Option<SqlType> {
    function_cast_types_by_name()
        .get(&name.to_ascii_lowercase())
        .copied()
}

pub(super) fn resolve_json_table_function(name: &str) -> Option<JsonTableFunction> {
    json_table_functions_by_name()
        .get(&name.to_ascii_lowercase())
        .copied()
}

pub(super) fn validate_scalar_function_arity(
    func: BuiltinScalarFunction,
    args: &[SqlExpr],
) -> Result<(), ParseError> {
    let valid = scalar_function_arity_overrides()
        .iter()
        .find_map(|(candidate, arity)| (*candidate == func).then_some(arity))
        .map(|arity| match arity {
            ScalarFunctionArity::Exact(count) => args.len() == *count,
        })
        .unwrap_or_else(|| match func {
        BuiltinScalarFunction::Random => args.is_empty(),
        BuiltinScalarFunction::GetDatabaseEncoding => args.is_empty(),
        BuiltinScalarFunction::ToJson | BuiltinScalarFunction::ToJsonb => args.len() == 1,
        BuiltinScalarFunction::Abs
        | BuiltinScalarFunction::Log10
        | BuiltinScalarFunction::Length
        | BuiltinScalarFunction::Lower
        | BuiltinScalarFunction::Scale
        | BuiltinScalarFunction::MinScale
        | BuiltinScalarFunction::TrimScale
        | BuiltinScalarFunction::NumericInc
        | BuiltinScalarFunction::Factorial
        | BuiltinScalarFunction::PgLsn
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
        | BuiltinScalarFunction::Md5
        | BuiltinScalarFunction::BitcastIntegerToFloat4
        | BuiltinScalarFunction::BitcastBigintToFloat8
        | BuiltinScalarFunction::BpcharToText
        | BuiltinScalarFunction::BitCount => args.len() == 1,
        BuiltinScalarFunction::Trunc | BuiltinScalarFunction::Round => matches!(args.len(), 1 | 2),
        BuiltinScalarFunction::Log => matches!(args.len(), 1 | 2),
        BuiltinScalarFunction::Power
        | BuiltinScalarFunction::Atan2d
        | BuiltinScalarFunction::BoolEq
        | BuiltinScalarFunction::BoolNe
        | BuiltinScalarFunction::Div
        | BuiltinScalarFunction::Mod => args.len() == 2,
        BuiltinScalarFunction::WidthBucket => args.len() == 4,
        BuiltinScalarFunction::GetBit => args.len() == 2,
        BuiltinScalarFunction::SetBit => args.len() == 3,
        BuiltinScalarFunction::Gcd | BuiltinScalarFunction::Lcm => args.len() == 2,
        BuiltinScalarFunction::Position
        | BuiltinScalarFunction::ConvertFrom
        | BuiltinScalarFunction::Left
        | BuiltinScalarFunction::Repeat
        | BuiltinScalarFunction::ToChar
        | BuiltinScalarFunction::ToNumber
        | BuiltinScalarFunction::PgInputIsValid
        | BuiltinScalarFunction::PgInputErrorMessage
        | BuiltinScalarFunction::PgInputErrorDetail
        | BuiltinScalarFunction::PgInputErrorHint
        | BuiltinScalarFunction::PgInputErrorSqlState => args.len() == 2,
        BuiltinScalarFunction::Substring => matches!(args.len(), 2 | 3),
        BuiltinScalarFunction::Overlay => matches!(args.len(), 3 | 4),
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
    });

    if valid {
        Ok(())
    } else {
        Err(ParseError::UnexpectedToken {
            expected: "valid builtin function arity",
            actual: format!("{func:?}({} args)", args.len()),
        })
    }
}

pub(super) fn validate_aggregate_arity(func: AggFunc, args: &[SqlExpr]) -> Result<(), ParseError> {
    let valid = aggregate_arity_overrides()
        .iter()
        .find_map(|(candidate, count)| (*candidate == func).then_some(*count))
        .map(|count| args.len() == count)
        .unwrap_or_else(|| match func {
        AggFunc::Count => args.len() <= 1,
        AggFunc::Sum
        | AggFunc::Avg
        | AggFunc::Variance
        | AggFunc::Stddev
        | AggFunc::Min
        | AggFunc::Max
        | AggFunc::JsonAgg
        | AggFunc::JsonbAgg => args.len() == 1,
        AggFunc::JsonObjectAgg | AggFunc::JsonbObjectAgg => args.len() == 2,
    });
    if valid {
        Ok(())
    } else {
        Err(ParseError::UnexpectedToken {
            expected: "valid aggregate arity",
            actual: format!("{}({} args)", func.name(), args.len()),
        })
    }
}

pub(super) fn comparison_operator_exists(op: &str, left: SqlType, right: SqlType) -> bool {
    let Some(left_oid) = builtin_type_oid(left) else {
        return false;
    };
    let Some(right_oid) = builtin_type_oid(right) else {
        return false;
    };
    bootstrap_pg_operator_rows().iter().any(|row| {
        row.oprname == op && row.oprleft == left_oid && row.oprright == right_oid
    })
}

pub(super) fn fixed_aggregate_return_type(func: AggFunc) -> Option<SqlType> {
    aggregate_fixed_return_types()
        .iter()
        .find_map(|(candidate, sql_type)| (*candidate == func).then_some(*sql_type))
}

fn scalar_functions_by_name() -> &'static BTreeMap<String, BuiltinScalarFunction> {
    static FUNCTIONS: OnceLock<BTreeMap<String, BuiltinScalarFunction>> = OnceLock::new();
    FUNCTIONS.get_or_init(|| {
        let mut by_name = BTreeMap::new();
        for row in bootstrap_pg_proc_rows() {
            if row.prokind != 'f' || row.proretset {
                continue;
            }
            if let Some(func) = builtin_scalar_function_for_proc_src(&row.prosrc) {
                by_name.insert(row.proname.to_ascii_lowercase(), func);
            }
        }
        for (name, func) in legacy_scalar_function_entries() {
            by_name.entry((*name).into()).or_insert(*func);
        }
        by_name
    })
}

fn builtin_scalar_function_for_proc_src(proc_src: &str) -> Option<BuiltinScalarFunction> {
    legacy_scalar_function_entries()
        .iter()
        .find_map(|(name, func)| proc_src.eq_ignore_ascii_case(name).then_some(*func))
}

fn legacy_scalar_function_entries() -> &'static [(&'static str, BuiltinScalarFunction)] {
    &[
        ("random", BuiltinScalarFunction::Random),
        (
            "getdatabaseencoding",
            BuiltinScalarFunction::GetDatabaseEncoding,
        ),
        ("to_json", BuiltinScalarFunction::ToJson),
        ("to_jsonb", BuiltinScalarFunction::ToJsonb),
        ("array_to_json", BuiltinScalarFunction::ArrayToJson),
        ("json_build_array", BuiltinScalarFunction::JsonBuildArray),
        ("json_build_object", BuiltinScalarFunction::JsonBuildObject),
        ("json_object", BuiltinScalarFunction::JsonObject),
        ("json_typeof", BuiltinScalarFunction::JsonTypeof),
        ("json_array_length", BuiltinScalarFunction::JsonArrayLength),
        ("json_extract_path", BuiltinScalarFunction::JsonExtractPath),
        (
            "json_extract_path_text",
            BuiltinScalarFunction::JsonExtractPathText,
        ),
        ("jsonb_typeof", BuiltinScalarFunction::JsonbTypeof),
        (
            "jsonb_array_length",
            BuiltinScalarFunction::JsonbArrayLength,
        ),
        ("jsonb_extract_path", BuiltinScalarFunction::JsonbExtractPath),
        (
            "jsonb_extract_path_text",
            BuiltinScalarFunction::JsonbExtractPathText,
        ),
        ("jsonb_build_array", BuiltinScalarFunction::JsonbBuildArray),
        ("jsonb_build_object", BuiltinScalarFunction::JsonbBuildObject),
        ("jsonb_path_exists", BuiltinScalarFunction::JsonbPathExists),
        ("jsonb_path_match", BuiltinScalarFunction::JsonbPathMatch),
        (
            "jsonb_path_query_array",
            BuiltinScalarFunction::JsonbPathQueryArray,
        ),
        (
            "jsonb_path_query_first",
            BuiltinScalarFunction::JsonbPathQueryFirst,
        ),
        ("left", BuiltinScalarFunction::Left),
        ("repeat", BuiltinScalarFunction::Repeat),
        ("length", BuiltinScalarFunction::Length),
        ("lower", BuiltinScalarFunction::Lower),
        ("position", BuiltinScalarFunction::Position),
        ("substring", BuiltinScalarFunction::Substring),
        ("overlay", BuiltinScalarFunction::Overlay),
        ("get_bit", BuiltinScalarFunction::GetBit),
        ("set_bit", BuiltinScalarFunction::SetBit),
        ("bit_count", BuiltinScalarFunction::BitCount),
        ("convert_from", BuiltinScalarFunction::ConvertFrom),
        ("md5", BuiltinScalarFunction::Md5),
        ("to_char", BuiltinScalarFunction::ToChar),
        ("to_number", BuiltinScalarFunction::ToNumber),
        ("abs", BuiltinScalarFunction::Abs),
        ("log", BuiltinScalarFunction::Log),
        ("log10", BuiltinScalarFunction::Log10),
        ("gcd", BuiltinScalarFunction::Gcd),
        ("lcm", BuiltinScalarFunction::Lcm),
        ("div", BuiltinScalarFunction::Div),
        ("mod", BuiltinScalarFunction::Mod),
        ("scale", BuiltinScalarFunction::Scale),
        ("min_scale", BuiltinScalarFunction::MinScale),
        ("trim_scale", BuiltinScalarFunction::TrimScale),
        ("numeric_inc", BuiltinScalarFunction::NumericInc),
        ("factorial", BuiltinScalarFunction::Factorial),
        ("pg_lsn", BuiltinScalarFunction::PgLsn),
        ("trunc", BuiltinScalarFunction::Trunc),
        ("round", BuiltinScalarFunction::Round),
        ("width_bucket", BuiltinScalarFunction::WidthBucket),
        ("ceil", BuiltinScalarFunction::Ceil),
        ("ceiling", BuiltinScalarFunction::Ceiling),
        ("floor", BuiltinScalarFunction::Floor),
        ("sign", BuiltinScalarFunction::Sign),
        ("sqrt", BuiltinScalarFunction::Sqrt),
        ("cbrt", BuiltinScalarFunction::Cbrt),
        ("power", BuiltinScalarFunction::Power),
        ("exp", BuiltinScalarFunction::Exp),
        ("ln", BuiltinScalarFunction::Ln),
        ("sinh", BuiltinScalarFunction::Sinh),
        ("cosh", BuiltinScalarFunction::Cosh),
        ("tanh", BuiltinScalarFunction::Tanh),
        ("asinh", BuiltinScalarFunction::Asinh),
        ("acosh", BuiltinScalarFunction::Acosh),
        ("atanh", BuiltinScalarFunction::Atanh),
        ("sind", BuiltinScalarFunction::Sind),
        ("cosd", BuiltinScalarFunction::Cosd),
        ("tand", BuiltinScalarFunction::Tand),
        ("cotd", BuiltinScalarFunction::Cotd),
        ("asind", BuiltinScalarFunction::Asind),
        ("acosd", BuiltinScalarFunction::Acosd),
        ("atand", BuiltinScalarFunction::Atand),
        ("atan2d", BuiltinScalarFunction::Atan2d),
        ("float4send", BuiltinScalarFunction::Float4Send),
        ("float8send", BuiltinScalarFunction::Float8Send),
        ("erf", BuiltinScalarFunction::Erf),
        ("erfc", BuiltinScalarFunction::Erfc),
        ("gamma", BuiltinScalarFunction::Gamma),
        ("lgamma", BuiltinScalarFunction::Lgamma),
        ("booleq", BuiltinScalarFunction::BoolEq),
        ("boolne", BuiltinScalarFunction::BoolNe),
        (
            "bitcast_integer_to_float4",
            BuiltinScalarFunction::BitcastIntegerToFloat4,
        ),
        (
            "bitcast_bigint_to_float8",
            BuiltinScalarFunction::BitcastBigintToFloat8,
        ),
        ("pg_input_is_valid", BuiltinScalarFunction::PgInputIsValid),
    ]
}

fn json_table_functions_by_name() -> &'static BTreeMap<String, JsonTableFunction> {
    static FUNCTIONS: OnceLock<BTreeMap<String, JsonTableFunction>> = OnceLock::new();
    FUNCTIONS.get_or_init(|| {
        let mut by_name = BTreeMap::new();
        for row in bootstrap_pg_proc_rows() {
            if row.prokind != 'f' || !row.proretset {
                continue;
            }
            if let Some(func) = legacy_json_table_function_entries()
                .iter()
                .find_map(|(name, func)| row.proname.eq_ignore_ascii_case(name).then_some(*func))
            {
                by_name.insert(row.proname.to_ascii_lowercase(), func);
            }
        }
        for (name, func) in legacy_json_table_function_entries() {
            by_name.entry((*name).into()).or_insert(*func);
        }
        by_name
    })
}

fn legacy_json_table_function_entries() -> &'static [(&'static str, JsonTableFunction)] {
    &[
        ("json_object_keys", JsonTableFunction::ObjectKeys),
        ("json_each", JsonTableFunction::Each),
        ("json_each_text", JsonTableFunction::EachText),
        ("json_array_elements", JsonTableFunction::ArrayElements),
        (
            "json_array_elements_text",
            JsonTableFunction::ArrayElementsText,
        ),
        ("jsonb_object_keys", JsonTableFunction::JsonbObjectKeys),
        ("jsonb_each", JsonTableFunction::JsonbEach),
        ("jsonb_each_text", JsonTableFunction::JsonbEachText),
        ("jsonb_array_elements", JsonTableFunction::JsonbArrayElements),
        (
            "jsonb_array_elements_text",
            JsonTableFunction::JsonbArrayElementsText,
        ),
    ]
}

fn function_cast_types_by_name() -> &'static BTreeMap<String, SqlType> {
    static TYPES: OnceLock<BTreeMap<String, SqlType>> = OnceLock::new();
    TYPES.get_or_init(|| {
        let mut by_name = BTreeMap::new();
        for row in builtin_type_rows() {
            let sql_type = match row.typname.as_str() {
                "int2" | "int4" | "int8" | "oid" | "bytea" | "float4" | "float8"
                | "numeric" | "text" | "bool" => Some(row.sql_type),
                "bit" => Some(SqlType::with_bit_len(SqlTypeKind::Bit, 1)),
                "varbit" => Some(SqlType::new(SqlTypeKind::VarBit)),
                _ => None,
            };
            if let Some(sql_type) = sql_type {
                by_name.insert(row.typname.to_ascii_lowercase(), sql_type);
            }
        }
        for (alias, canonical) in function_cast_type_aliases() {
            if let Some(sql_type) = by_name.get(*canonical).copied() {
                by_name.insert((*alias).into(), sql_type);
            }
        }
        by_name
    })
}

fn function_cast_type_aliases() -> &'static [(&'static str, &'static str)] {
    &[
        ("smallint", "int2"),
        ("int", "int4"),
        ("integer", "int4"),
        ("bigint", "int8"),
        ("bit varying", "varbit"),
        ("real", "float4"),
        ("decimal", "numeric"),
        ("boolean", "bool"),
    ]
}

fn scalar_function_arity_overrides() -> &'static Vec<(BuiltinScalarFunction, ScalarFunctionArity)> {
    static ARITIES: OnceLock<Vec<(BuiltinScalarFunction, ScalarFunctionArity)>> = OnceLock::new();
    ARITIES.get_or_init(|| {
        let mut by_func = Vec::new();
        for row in bootstrap_pg_proc_rows() {
            if row.prokind != 'f' || row.proretset || row.provariadic != 0 {
                continue;
            }
            if let Some(func) = builtin_scalar_function_for_proc_src(&row.prosrc) {
                if !supports_exact_proc_arity(func) {
                    continue;
                }
                if by_func.iter().all(|(candidate, _)| *candidate != func) {
                    by_func.push((func, ScalarFunctionArity::Exact(row.pronargs.max(0) as usize)));
                }
            }
        }
        by_func
    })
}

fn supports_exact_proc_arity(func: BuiltinScalarFunction) -> bool {
    !matches!(
        func,
        BuiltinScalarFunction::Log
            | BuiltinScalarFunction::Trunc
            | BuiltinScalarFunction::Round
            | BuiltinScalarFunction::Substring
            | BuiltinScalarFunction::Overlay
            | BuiltinScalarFunction::ArrayToJson
            | BuiltinScalarFunction::JsonBuildArray
            | BuiltinScalarFunction::JsonBuildObject
            | BuiltinScalarFunction::JsonObject
            | BuiltinScalarFunction::JsonExtractPath
            | BuiltinScalarFunction::JsonExtractPathText
            | BuiltinScalarFunction::JsonbExtractPath
            | BuiltinScalarFunction::JsonbExtractPathText
            | BuiltinScalarFunction::JsonbBuildArray
            | BuiltinScalarFunction::JsonbBuildObject
            | BuiltinScalarFunction::JsonbPathExists
            | BuiltinScalarFunction::JsonbPathMatch
            | BuiltinScalarFunction::JsonbPathQueryArray
            | BuiltinScalarFunction::JsonbPathQueryFirst
    )
}

fn aggregate_arity_overrides() -> &'static Vec<(AggFunc, usize)> {
    static ARITIES: OnceLock<Vec<(AggFunc, usize)>> = OnceLock::new();
    ARITIES.get_or_init(|| {
        let mut by_func = Vec::new();
        for row in bootstrap_pg_proc_rows() {
            if row.prokind != 'a' {
                continue;
            }
            let Some(func) = aggregate_func_for_proname(&row.proname) else {
                continue;
            };
            if func == AggFunc::Count || by_func.iter().any(|(candidate, _)| *candidate == func) {
                continue;
            }
            by_func.push((func, row.pronargs.max(0) as usize));
        }
        by_func
    })
}

fn aggregate_fixed_return_types() -> &'static Vec<(AggFunc, SqlType)> {
    static TYPES: OnceLock<Vec<(AggFunc, SqlType)>> = OnceLock::new();
    TYPES.get_or_init(|| {
        let mut by_func = Vec::new();
        for row in bootstrap_pg_proc_rows() {
            if row.prokind != 'a' {
                continue;
            }
            let Some(func) = aggregate_func_for_proname(&row.proname) else {
                continue;
            };
            if !supports_fixed_aggregate_return_type(func) {
                continue;
            }
            let Some(sql_type) = builtin_sql_type_for_oid(row.prorettype) else {
                continue;
            };
            if by_func.iter().all(|(candidate, _)| *candidate != func) {
                by_func.push((func, sql_type));
            }
        }
        by_func
    })
}

fn supports_fixed_aggregate_return_type(func: AggFunc) -> bool {
    matches!(
        func,
        AggFunc::Count
            | AggFunc::JsonAgg
            | AggFunc::JsonbAgg
            | AggFunc::JsonObjectAgg
            | AggFunc::JsonbObjectAgg
    )
}

fn builtin_sql_type_for_oid(oid: u32) -> Option<SqlType> {
    builtin_type_rows()
        .into_iter()
        .find_map(|row| (row.oid == oid).then_some(row.sql_type))
}

fn builtin_type_oid(sql_type: SqlType) -> Option<u32> {
    if sql_type.is_array {
        return None;
    }
    builtin_type_rows().into_iter().find_map(|row| {
        (!row.sql_type.is_array && row.sql_type.kind == sql_type.kind).then_some(row.oid)
    })
}

fn aggregate_func_for_proname(name: &str) -> Option<AggFunc> {
    match name.to_ascii_lowercase().as_str() {
        "count" => Some(AggFunc::Count),
        "sum" => Some(AggFunc::Sum),
        "avg" => Some(AggFunc::Avg),
        "variance" => Some(AggFunc::Variance),
        "stddev" => Some(AggFunc::Stddev),
        "min" => Some(AggFunc::Min),
        "max" => Some(AggFunc::Max),
        "json_agg" => Some(AggFunc::JsonAgg),
        "jsonb_agg" => Some(AggFunc::JsonbAgg),
        "json_object_agg" => Some(AggFunc::JsonObjectAgg),
        "jsonb_object_agg" => Some(AggFunc::JsonbObjectAgg),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ScalarFunctionArity {
    Exact(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_scalar_function_uses_pg_proc_and_filters_non_scalar_rows() {
        assert_eq!(
            resolve_scalar_function("random"),
            Some(BuiltinScalarFunction::Random)
        );
        assert_eq!(
            resolve_scalar_function("lower"),
            Some(BuiltinScalarFunction::Lower)
        );
        assert_eq!(
            resolve_scalar_function("ceiling"),
            Some(BuiltinScalarFunction::Ceiling)
        );
        assert_eq!(resolve_scalar_function("count"), None);
        assert_eq!(resolve_scalar_function("json_array_elements"), None);
        assert_eq!(resolve_scalar_function("int4"), None);
    }

    #[test]
    fn resolve_json_table_function_uses_pg_proc_and_legacy_fallback() {
        assert_eq!(
            resolve_json_table_function("json_array_elements"),
            Some(JsonTableFunction::ArrayElements)
        );
        assert_eq!(
            resolve_json_table_function("jsonb_array_elements"),
            Some(JsonTableFunction::JsonbArrayElements)
        );
        assert_eq!(
            resolve_json_table_function("json_each"),
            Some(JsonTableFunction::Each)
        );
        assert_eq!(resolve_json_table_function("random"), None);
    }

    #[test]
    fn resolve_function_cast_type_uses_pg_type_catalog_and_aliases() {
        assert_eq!(
            resolve_function_cast_type("int4"),
            Some(SqlType::new(SqlTypeKind::Int4))
        );
        assert_eq!(
            resolve_function_cast_type("smallint"),
            Some(SqlType::new(SqlTypeKind::Int2))
        );
        assert_eq!(
            resolve_function_cast_type("bit"),
            Some(SqlType::with_bit_len(SqlTypeKind::Bit, 1))
        );
        assert_eq!(
            resolve_function_cast_type("boolean"),
            Some(SqlType::new(SqlTypeKind::Bool))
        );
        assert_eq!(resolve_function_cast_type("varchar"), None);
    }

    #[test]
    fn validate_scalar_function_arity_uses_pg_proc_for_exact_arity_rows() {
        assert!(validate_scalar_function_arity(BuiltinScalarFunction::Lower, &[SqlExpr::Default]).is_ok());
        assert!(validate_scalar_function_arity(BuiltinScalarFunction::Lower, &[]).is_err());
        assert!(validate_scalar_function_arity(BuiltinScalarFunction::Random, &[]).is_ok());
        assert!(validate_scalar_function_arity(BuiltinScalarFunction::Random, &[SqlExpr::Default]).is_err());
        assert!(validate_scalar_function_arity(
            BuiltinScalarFunction::JsonBuildArray,
            &[SqlExpr::Default, SqlExpr::Default]
        )
        .is_ok());
    }

    #[test]
    fn validate_aggregate_arity_uses_pg_proc_for_exact_rows() {
        assert!(validate_aggregate_arity(AggFunc::Sum, &[SqlExpr::Default]).is_ok());
        assert!(validate_aggregate_arity(AggFunc::Sum, &[]).is_err());
        assert!(validate_aggregate_arity(
            AggFunc::JsonObjectAgg,
            &[SqlExpr::Default, SqlExpr::Default]
        )
        .is_ok());
        assert!(validate_aggregate_arity(AggFunc::JsonObjectAgg, &[SqlExpr::Default]).is_err());
        assert!(validate_aggregate_arity(AggFunc::Count, &[]).is_ok());
    }

    #[test]
    fn fixed_aggregate_return_type_uses_pg_proc_for_type_invariant_rows() {
        assert_eq!(
            fixed_aggregate_return_type(AggFunc::Count),
            Some(SqlType::new(SqlTypeKind::Int8))
        );
        assert_eq!(
            fixed_aggregate_return_type(AggFunc::JsonAgg),
            Some(SqlType::new(SqlTypeKind::Json))
        );
        assert_eq!(
            fixed_aggregate_return_type(AggFunc::JsonbObjectAgg),
            Some(SqlType::new(SqlTypeKind::Jsonb))
        );
        assert_eq!(fixed_aggregate_return_type(AggFunc::Sum), None);
        assert_eq!(fixed_aggregate_return_type(AggFunc::Max), None);
    }

    #[test]
    fn comparison_operator_exists_uses_pg_operator_catalog() {
        assert!(comparison_operator_exists(
            "<",
            SqlType::new(SqlTypeKind::Text),
            SqlType::new(SqlTypeKind::Text)
        ));
        assert!(comparison_operator_exists(
            ">=",
            SqlType::new(SqlTypeKind::Text),
            SqlType::new(SqlTypeKind::Text)
        ));
        assert!(comparison_operator_exists(
            "=",
            SqlType::new(SqlTypeKind::Bool),
            SqlType::new(SqlTypeKind::Bool)
        ));
        assert!(!comparison_operator_exists(
            "=",
            SqlType::new(SqlTypeKind::Jsonb),
            SqlType::new(SqlTypeKind::Jsonb)
        ));
        assert!(!comparison_operator_exists(
            "=",
            SqlType::array_of(SqlType::new(SqlTypeKind::Int4)),
            SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
        ));
    }
}
