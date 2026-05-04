use pgrust_catalog_data::{
    PG_LANGUAGE_C_OID, PG_LANGUAGE_INTERNAL_OID, PG_LANGUAGE_PLPGSQL_OID, PG_LANGUAGE_SQL_OID,
    PgProcRow, builtin_scalar_function_for_proc_row,
};
use pgrust_nodes::datum::ArrayValue;
use pgrust_nodes::primnodes::BuiltinScalarFunction;
use pgrust_nodes::{SqlType, SqlTypeKind, Value};

#[derive(Debug, Clone)]
pub enum ScalarFunctionCallInfo {
    Builtin(BuiltinScalarFunction),
    Sql(PgProcRow),
    PlPgSql { proc_oid: u32 },
    UnsupportedInternal(PgProcRow),
    PlHandler { proc_oid: u32 },
}

pub fn scalar_function_call_info_for_proc_row(row: PgProcRow) -> ScalarFunctionCallInfo {
    let proc_oid = row.oid;
    if let Some(builtin) = builtin_scalar_function_for_proc_row(&row) {
        ScalarFunctionCallInfo::Builtin(builtin)
    } else {
        match row.prolang {
            PG_LANGUAGE_SQL_OID => ScalarFunctionCallInfo::Sql(row),
            PG_LANGUAGE_PLPGSQL_OID => ScalarFunctionCallInfo::PlPgSql { proc_oid },
            PG_LANGUAGE_INTERNAL_OID | PG_LANGUAGE_C_OID => {
                ScalarFunctionCallInfo::UnsupportedInternal(row)
            }
            _ => ScalarFunctionCallInfo::PlHandler { proc_oid },
        }
    }
}

pub fn unsupported_internal_function_detail(row: &PgProcRow) -> UnsupportedInternalFunctionDetail {
    if is_unsupported_xml_mapping_function(row.prosrc.as_str())
        || is_unsupported_xml_mapping_function(row.proname.as_str())
    {
        return UnsupportedInternalFunctionDetail::UnsupportedXmlFeature;
    }

    UnsupportedInternalFunctionDetail::UnsupportedInternal {
        proname: row.proname.clone(),
        prosrc: row.prosrc.clone(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnsupportedInternalFunctionDetail {
    UnsupportedXmlFeature,
    UnsupportedInternal { proname: String, prosrc: String },
}

fn is_unsupported_xml_mapping_function(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "table_to_xml"
            | "table_to_xmlschema"
            | "table_to_xml_and_xmlschema"
            | "query_to_xml"
            | "query_to_xmlschema"
            | "query_to_xml_and_xmlschema"
            | "cursor_to_xml"
            | "cursor_to_xmlschema"
            | "schema_to_xml"
            | "schema_to_xmlschema"
            | "schema_to_xml_and_xmlschema"
    )
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct NormalizedVariadicArgs {
    pub values: Option<Vec<Value>>,
    pub arg_types: Option<Vec<SqlType>>,
}

pub fn normalize_variadic_scalar_function_args(
    row: &PgProcRow,
    arg_values: &[Value],
    arg_types: Option<&[SqlType]>,
    type_oid_for_sql_type: impl Fn(SqlType) -> Option<u32>,
) -> NormalizedVariadicArgs {
    if row.provariadic == 0 || row.pronargs <= 0 {
        return NormalizedVariadicArgs::default();
    }
    let variadic_index = row.pronargs as usize - 1;
    if arg_values.len() < row.pronargs as usize {
        return NormalizedVariadicArgs::default();
    }
    let last_is_explicit_array = arg_values.len() == row.pronargs as usize
        && (matches!(arg_values.get(variadic_index), Some(Value::PgArray(_)))
            || arg_types
                .and_then(|types| types.get(variadic_index))
                .is_some_and(|ty| ty.is_array));
    if last_is_explicit_array {
        return NormalizedVariadicArgs::default();
    }

    let variadic_values = arg_values[variadic_index..]
        .iter()
        .map(Value::to_owned_value)
        .collect::<Vec<_>>();
    let element_type = arg_types
        .and_then(|types| types.get(variadic_index).copied())
        .or_else(|| {
            arg_values[variadic_index..]
                .iter()
                .find_map(Value::sql_type_hint)
        });
    let mut variadic_array = ArrayValue::from_1d(variadic_values);
    if let Some(element_oid) = element_type.and_then(type_oid_for_sql_type) {
        variadic_array = variadic_array.with_element_type_oid(element_oid);
    }

    let mut values = Vec::with_capacity(row.pronargs as usize);
    values.extend(
        arg_values[..variadic_index]
            .iter()
            .map(Value::to_owned_value),
    );
    values.push(Value::PgArray(variadic_array));

    let arg_types = arg_types.map(|types| {
        let mut normalized = Vec::with_capacity(row.pronargs as usize);
        normalized.extend(types[..variadic_index].iter().copied());
        let array_type = element_type
            .map(SqlType::array_of)
            .unwrap_or_else(|| SqlType::new(SqlTypeKind::AnyArray));
        normalized.push(array_type);
        normalized
    });

    NormalizedVariadicArgs {
        values: Some(values),
        arg_types,
    }
}
