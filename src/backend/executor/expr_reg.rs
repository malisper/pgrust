use super::ExecError;
use crate::backend::parser::{
    CatalogLookup, ParseError, RawTypeName, SqlType, SqlTypeKind, parse_type_name,
    resolve_raw_type_name,
};
use crate::include::catalog::{
    ACLITEM_ARRAY_TYPE_OID, BIT_TYPE_OID, BOOL_TYPE_OID, BPCHAR_TYPE_OID, BYTEA_TYPE_OID,
    FLOAT4_TYPE_OID, FLOAT8_TYPE_OID, INT2_TYPE_OID, INT4_ARRAY_TYPE_OID, INT4_TYPE_OID,
    INT8_TYPE_OID, INTERNAL_CHAR_TYPE_OID, NUMERIC_TYPE_OID, OID_TYPE_OID, REGCOLLATION_TYPE_OID,
    REGOPER_TYPE_OID, REGOPERATOR_TYPE_OID, REGPROC_TYPE_OID, REGPROCEDURE_TYPE_OID, TEXT_TYPE_OID,
    TIME_TYPE_OID, TIMESTAMP_TYPE_OID, TIMESTAMPTZ_TYPE_OID, TIMETZ_TYPE_OID, VARBIT_TYPE_OID,
    VARCHAR_TYPE_OID,
};
use crate::include::nodes::datum::Value;

struct BootstrapLookup;

impl CatalogLookup for BootstrapLookup {
    fn lookup_any_relation(&self, _name: &str) -> Option<crate::backend::parser::BoundRelation> {
        None
    }
}

static BOOTSTRAP_LOOKUP: BootstrapLookup = BootstrapLookup;

fn effective_catalog(catalog: Option<&dyn CatalogLookup>) -> &dyn CatalogLookup {
    catalog.unwrap_or(&BOOTSTRAP_LOOKUP)
}

fn detailed_error(message: impl Into<String>, sqlstate: &'static str) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate,
    }
}

pub(crate) fn quote_identifier_if_needed(identifier: &str) -> String {
    if !identifier.is_empty()
        && identifier.chars().enumerate().all(|(idx, ch)| {
            if idx == 0 {
                ch == '_' || ch.is_ascii_lowercase()
            } else {
                ch == '_' || ch.is_ascii_lowercase() || ch.is_ascii_digit()
            }
        })
    {
        return identifier.into();
    }
    let escaped = identifier.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

pub(crate) fn parse_sql_name_parts(input: &str) -> Result<Vec<String>, ExecError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(detailed_error("invalid name syntax", "42601"));
    }
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut current_quoted = false;
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if quoted {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    current.push('"');
                } else {
                    quoted = false;
                }
            } else {
                current.push(ch);
            }
            continue;
        }
        match ch {
            '"' if current.is_empty() => {
                quoted = true;
                current_quoted = true;
            }
            '.' => {
                if current.is_empty() {
                    return Err(detailed_error("invalid name syntax", "42601"));
                }
                parts.push(if current_quoted {
                    current.clone()
                } else {
                    current.to_ascii_lowercase()
                });
                current.clear();
                current_quoted = false;
            }
            ch if ch.is_whitespace() => return Err(detailed_error("invalid name syntax", "42601")),
            _ => current.push(ch),
        }
    }
    if quoted || current.is_empty() {
        return Err(detailed_error("invalid name syntax", "42601"));
    }
    parts.push(if current_quoted {
        current
    } else {
        current.to_ascii_lowercase()
    });
    Ok(parts)
}

fn normalize_optional_pg_catalog_name(input: &str) -> Result<Option<String>, ExecError> {
    let parts = parse_sql_name_parts(input)?;
    match parts.as_slice() {
        [name] => Ok(Some(name.clone())),
        [schema, name] if schema == "pg_catalog" => Ok(Some(name.clone())),
        [_schema, _name] => Ok(None),
        _ => Ok(None),
    }
}

fn parse_optional_schema_name(input: &str) -> Result<(Option<String>, String), ExecError> {
    let parts = parse_sql_name_parts(input)?;
    match parts.as_slice() {
        [name] => Ok((None, name.clone())),
        [schema, name] => Ok((Some(schema.clone()), name.clone())),
        _ => Err(detailed_error("invalid name syntax", "42601")),
    }
}

fn proc_schema_matches(
    row: &crate::include::catalog::PgProcRow,
    schema: Option<&str>,
    catalog: &dyn CatalogLookup,
) -> bool {
    schema.is_none_or(|schema| {
        catalog
            .namespace_row_by_oid(row.pronamespace)
            .is_some_and(|namespace| namespace.nspname == schema)
    })
}

fn parse_operator_name(input: &str) -> Option<(Option<&str>, &str)> {
    let input = input.trim();
    if input.is_empty() {
        return None;
    }
    input
        .split_once('.')
        .map(|(schema, name)| (Some(schema.trim()), name.trim()))
        .or_else(|| Some((None, input)))
        .filter(|(_, name)| !name.is_empty())
}

fn operator_schema_matches(schema: Option<&str>) -> bool {
    schema.is_none_or(|schema| schema.eq_ignore_ascii_case("pg_catalog"))
}

pub(crate) fn resolve_regproc_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    if let Some(oid) = parse_numeric_oid(input)? {
        return Ok(oid);
    }
    let lookup = effective_catalog(catalog);
    let (schema, name) = parse_optional_schema_name(input)?;
    let matches = lookup
        .proc_rows_by_name(&name)
        .into_iter()
        .filter(|row| proc_schema_matches(row, schema.as_deref(), lookup))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [row] => Ok(row.oid),
        [] => Err(detailed_error(
            format!("function \"{}\" does not exist", input.trim()),
            "42883",
        )),
        _ => Err(detailed_error(
            format!("more than one function named {}", input.trim()),
            "42725",
        )),
    }
}

pub(crate) fn resolve_regprocedure_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    if let Some(oid) = parse_numeric_oid(input)? {
        return Ok(oid);
    }
    let (name_sql, arg_sql) = parse_signature(input, "function")?;
    let lookup = effective_catalog(catalog);
    let (schema, name) = parse_optional_schema_name(name_sql)?;
    let arg_oids = parse_signature_arg_oids(arg_sql, lookup)?;
    lookup
        .proc_rows_by_name(&name)
        .into_iter()
        .filter(|row| proc_schema_matches(row, schema.as_deref(), lookup))
        .find(|row| parse_oid_list(&row.proargtypes).as_deref() == Some(arg_oids.as_slice()))
        .map(|row| row.oid)
        .ok_or_else(|| {
            detailed_error(
                format!("function \"{}\" does not exist", input.trim()),
                "42883",
            )
        })
}

pub(crate) fn resolve_regoper_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    if let Some(oid) = parse_numeric_oid(input)? {
        return Ok(oid);
    }
    let Some((schema, name)) = parse_operator_name(input) else {
        return Err(detailed_error("invalid name syntax", "42601"));
    };
    let lookup = effective_catalog(catalog);
    let matches = if operator_schema_matches(schema) {
        lookup
            .operator_rows()
            .into_iter()
            .filter(|row| row.oprname == name)
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    match matches.as_slice() {
        [row] => Ok(row.oid),
        [] => Err(detailed_error(
            format!("operator does not exist: {}", input.trim()),
            "42883",
        )),
        _ => Err(detailed_error(
            format!("more than one operator named {}", name),
            "42725",
        )),
    }
}

pub(crate) fn resolve_regoperator_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    if let Some(oid) = parse_numeric_oid(input)? {
        return Ok(oid);
    }
    let (name_sql, arg_sql) = parse_signature(input, "operator")?;
    let args = split_signature_args(arg_sql)?;
    if args.len() != 2 {
        return Err(detailed_error("expected two type names", "22P02"));
    }
    let left_oid = parse_operator_arg_oid(&args[0], effective_catalog(catalog))?;
    let right_oid = parse_operator_arg_oid(&args[1], effective_catalog(catalog))?;
    let Some((schema, name)) = parse_operator_name(name_sql) else {
        return Err(detailed_error("invalid name syntax", "42601"));
    };
    let lookup = effective_catalog(catalog);
    if operator_schema_matches(schema)
        && let Some(row) = lookup.operator_by_name_left_right(name, left_oid, right_oid)
    {
        return Ok(row.oid);
    }
    Err(detailed_error(
        format!("operator does not exist: {}", input.trim()),
        "42883",
    ))
}

pub(crate) fn resolve_regclass_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    if let Some(oid) = parse_numeric_oid(input)? {
        return Ok(oid);
    }
    effective_catalog(catalog)
        .lookup_any_relation(input.trim())
        .map(|entry| entry.relation_oid)
        .ok_or_else(|| {
            detailed_error(
                format!("relation \"{}\" does not exist", input.trim()),
                "42P01",
            )
        })
}

pub(crate) fn resolve_regtype_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    if let Some(oid) = parse_numeric_oid(input)? {
        return Ok(oid);
    }
    let trimmed = input.trim();
    validate_regtype_input_syntax(trimmed)?;
    let raw_type = parse_type_name(trimmed).map_err(ExecError::Parse)?;
    reject_trailing_regtype_tokens(trimmed, &raw_type)?;
    let lookup = effective_catalog(catalog);
    let sql_type = match resolve_raw_type_name(&raw_type, lookup) {
        Ok(sql_type) => sql_type,
        Err(ParseError::UnsupportedType(_)) => {
            return Err(detailed_error(
                format!("type \"{trimmed}\" does not exist"),
                "42704",
            ));
        }
        Err(err) => return Err(ExecError::Parse(err)),
    };
    lookup
        .type_oid_for_sql_type(sql_type)
        .ok_or_else(|| detailed_error(format!("type \"{trimmed}\" does not exist"), "42704"))
}

pub(crate) fn resolve_regrole_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    if let Some(oid) = parse_numeric_oid(input)? {
        return Ok(oid);
    }
    let parts = parse_sql_name_parts(input)?;
    let [name] = parts.as_slice() else {
        return Err(detailed_error("invalid name syntax", "42601"));
    };
    effective_catalog(catalog)
        .authid_rows()
        .into_iter()
        .find(|row| row.rolname == *name)
        .map(|row| row.oid)
        .ok_or_else(|| detailed_error(format!("role \"{name}\" does not exist"), "42704"))
}

pub(crate) fn resolve_regnamespace_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    if let Some(oid) = parse_numeric_oid(input)? {
        return Ok(oid);
    }
    let parts = parse_sql_name_parts(input)?;
    let [name] = parts.as_slice() else {
        return Err(detailed_error("invalid name syntax", "42601"));
    };
    effective_catalog(catalog)
        .namespace_rows()
        .into_iter()
        .find(|row| row.nspname == *name)
        .map(|row| row.oid)
        .ok_or_else(|| detailed_error(format!("schema \"{name}\" does not exist"), "3F000"))
}

pub(crate) fn resolve_regcollation_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    if let Some(oid) = parse_numeric_oid(input)? {
        return Ok(oid);
    }
    let Some(name) = normalize_optional_pg_catalog_name(input)? else {
        return Err(detailed_error(
            format!(
                "collation \"{}\" for encoding \"UTF8\" does not exist",
                input.trim()
            ),
            "42704",
        ));
    };
    effective_catalog(catalog)
        .collation_rows()
        .into_iter()
        .find(|row| row.collname == name)
        .map(|row| row.oid)
        .ok_or_else(|| {
            detailed_error(
                format!(
                    "collation \"{}\" for encoding \"UTF8\" does not exist",
                    input.trim()
                ),
                "42704",
            )
        })
}

pub(crate) fn resolve_regconfig_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    if let Some(oid) = parse_numeric_oid(input)? {
        return Ok(oid);
    }
    let Some(name) = normalize_optional_pg_catalog_name(input)? else {
        return Err(detailed_error(
            format!(
                "text search configuration \"{}\" does not exist",
                input.trim()
            ),
            "42704",
        ));
    };
    effective_catalog(catalog)
        .ts_config_rows()
        .into_iter()
        .find(|row| row.cfgname == name)
        .map(|row| row.oid)
        .ok_or_else(|| {
            detailed_error(
                format!(
                    "text search configuration \"{}\" does not exist",
                    input.trim()
                ),
                "42704",
            )
        })
}

pub(crate) fn resolve_regdictionary_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    if let Some(oid) = parse_numeric_oid(input)? {
        return Ok(oid);
    }
    let Some(name) = normalize_optional_pg_catalog_name(input)? else {
        return Err(detailed_error(
            format!("text search dictionary \"{}\" does not exist", input.trim()),
            "42704",
        ));
    };
    effective_catalog(catalog)
        .ts_dict_rows()
        .into_iter()
        .find(|row| row.dictname == name)
        .map(|row| row.oid)
        .ok_or_else(|| {
            detailed_error(
                format!("text search dictionary \"{}\" does not exist", input.trim()),
                "42704",
            )
        })
}

pub(crate) fn resolve_reg_object_oid(
    input: &str,
    kind: SqlTypeKind,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    match kind {
        SqlTypeKind::RegProc => resolve_regproc_oid(input, catalog),
        SqlTypeKind::RegProcedure => resolve_regprocedure_oid(input, catalog),
        SqlTypeKind::RegOper => resolve_regoper_oid(input, catalog),
        SqlTypeKind::RegOperator => resolve_regoperator_oid(input, catalog),
        SqlTypeKind::RegClass => resolve_regclass_oid(input, catalog),
        SqlTypeKind::RegType => resolve_regtype_oid(input, catalog),
        SqlTypeKind::RegRole => resolve_regrole_oid(input, catalog),
        SqlTypeKind::RegNamespace => resolve_regnamespace_oid(input, catalog),
        SqlTypeKind::RegCollation => resolve_regcollation_oid(input, catalog),
        SqlTypeKind::RegConfig => resolve_regconfig_oid(input, catalog),
        SqlTypeKind::RegDictionary => resolve_regdictionary_oid(input, catalog),
        _ => Err(detailed_error("unsupported reg object type", "0A000")),
    }
}

pub(crate) fn is_hard_regtype_input_error(err: &ExecError) -> bool {
    match err {
        ExecError::WithContext { context, .. } => context.starts_with("invalid type name "),
        ExecError::DetailedError { message, .. } => {
            message == "invalid NUMERIC type modifier"
                || message.starts_with("improper qualified name (too many dotted names): ")
                || message.starts_with("cross-database references are not implemented: ")
        }
        _ => false,
    }
}

pub(crate) fn cast_text_to_reg_object(
    input: &str,
    kind: SqlTypeKind,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    resolve_reg_object_oid(input, kind, catalog).map(|oid| Value::Int64(i64::from(oid)))
}

pub(crate) fn to_reg_object(
    value: &Value,
    kind: SqlTypeKind,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let Some(text) = value.as_text() else {
        return Err(ExecError::TypeMismatch {
            op: "to_reg",
            left: value.clone(),
            right: Value::Text("".into()),
        });
    };
    Ok(cast_text_to_reg_object(text, kind, catalog).unwrap_or(Value::Null))
}

pub(crate) fn to_regtypemod(value: &Value, catalog: Option<&dyn CatalogLookup>) -> Value {
    if matches!(value, Value::Null) {
        return Value::Null;
    }
    let Some(text) = value.as_text() else {
        return Value::Null;
    };
    let trimmed = text.trim();
    let quoted_whole_name = trimmed.starts_with('"') && trimmed.ends_with('"');
    let lower = trimmed.to_ascii_lowercase();
    if lower == "text" || quoted_whole_name {
        return Value::Int32(-1);
    }
    if lower == "bit" {
        return Value::Int32(1);
    }
    if let Some(precision) = parse_single_i32_typmod(&lower, "bit") {
        return Value::Int32(precision);
    }
    if let Some(precision) = parse_single_i32_typmod(&lower, "timestamp") {
        return Value::Int32(precision);
    }
    if let Some(length) = parse_single_i32_typmod(&lower, "varchar")
        .or_else(|| parse_single_i32_typmod(&lower, "character varying"))
    {
        return Value::Int32(length + 4);
    }
    if resolve_regtype_oid(trimmed, catalog).is_ok() {
        Value::Int32(-1)
    } else {
        Value::Null
    }
}

pub(crate) fn format_type(
    oid: Option<u32>,
    typmod: Option<i32>,
    catalog: &dyn CatalogLookup,
) -> Value {
    let Some(oid) = oid else {
        return Value::Null;
    };
    Value::Text(format_type_text(oid, typmod, catalog).into())
}

pub(crate) fn format_type_optional(
    oid: Option<u32>,
    typmod: Option<i32>,
    catalog: Option<&dyn CatalogLookup>,
) -> Value {
    let Some(oid) = oid else {
        return Value::Null;
    };
    Value::Text(format_type_text(oid, typmod, effective_catalog(catalog)).into())
}

pub(crate) fn format_type_text(
    oid: u32,
    typmod: Option<i32>,
    catalog: &dyn CatalogLookup,
) -> String {
    match oid {
        INT2_TYPE_OID => "smallint".into(),
        INT4_TYPE_OID => "integer".into(),
        INT8_TYPE_OID => "bigint".into(),
        BOOL_TYPE_OID => "boolean".into(),
        BYTEA_TYPE_OID => "bytea".into(),
        FLOAT4_TYPE_OID => "real".into(),
        FLOAT8_TYPE_OID => "double precision".into(),
        OID_TYPE_OID => "oid".into(),
        TEXT_TYPE_OID => "text".into(),
        ACLITEM_ARRAY_TYPE_OID => "aclitem[]".into(),
        INTERNAL_CHAR_TYPE_OID => "\"char\"".into(),
        NUMERIC_TYPE_OID => "numeric".into(),
        VARCHAR_TYPE_OID => match typmod {
            Some(value) if value >= 4 => format!("character varying({})", value - 4),
            _ => "character varying".into(),
        },
        BPCHAR_TYPE_OID => match typmod {
            Some(value) if value >= 4 => format!("character({})", value - 4),
            Some(-1) => "bpchar".into(),
            _ => "character".into(),
        },
        BIT_TYPE_OID => match typmod {
            Some(-1) => "\"bit\"".into(),
            Some(value) if value >= 0 => format!("bit({value})"),
            _ => "bit".into(),
        },
        VARBIT_TYPE_OID => match typmod {
            Some(value) if value >= 0 => format!("bit varying({value})"),
            _ => "bit varying".into(),
        },
        TIME_TYPE_OID => match typmod {
            Some(value) if value >= 0 => format!("time({value}) without time zone"),
            _ => "time without time zone".into(),
        },
        TIMETZ_TYPE_OID => match typmod {
            Some(value) if value >= 0 => format!("time({value}) with time zone"),
            _ => "time with time zone".into(),
        },
        TIMESTAMP_TYPE_OID => match typmod {
            Some(value) if value >= 0 => format!("timestamp({value}) without time zone"),
            _ => "timestamp without time zone".into(),
        },
        TIMESTAMPTZ_TYPE_OID => match typmod {
            Some(value) if value >= 0 => format!("timestamp({value}) with time zone"),
            _ => "timestamp with time zone".into(),
        },
        REGPROC_TYPE_OID => "regproc".into(),
        REGPROCEDURE_TYPE_OID => "regprocedure".into(),
        REGOPER_TYPE_OID => "regoper".into(),
        REGOPERATOR_TYPE_OID => "regoperator".into(),
        REGCOLLATION_TYPE_OID => "regcollation".into(),
        crate::include::catalog::ANYOID => "\"any\"".into(),
        _ => catalog
            .type_by_oid(oid)
            .map(|row| {
                if row.typelem != 0 && row.typname.starts_with('_') {
                    return format!("{}[]", format_type_text(row.typelem, None, catalog));
                }
                let name = quote_identifier_if_needed(&row.typname);
                if let Some(typmod) = typmod
                    && typmod >= 0
                    && let Some(suffix) = format_user_type_typmod(row.typmodout, typmod, catalog)
                {
                    return format!("{name}{suffix}");
                }
                name
            })
            .unwrap_or_else(|| "???".into()),
    }
}

fn format_user_type_typmod(
    typmodout_oid: u32,
    typmod: i32,
    catalog: &dyn CatalogLookup,
) -> Option<String> {
    let proc_name = catalog.proc_row_by_oid(typmodout_oid)?.proname;
    match proc_name.to_ascii_lowercase().as_str() {
        "numerictypmodout" => {
            let packed = typmod.checked_sub(SqlType::VARHDRSZ)?;
            let precision = (packed >> 16) & 0xffff;
            let scale = packed & 0xffff;
            Some(format!("({precision},{scale})"))
        }
        "varchartypmodout" | "bpchartypmodout" => {
            let len = typmod.checked_sub(SqlType::VARHDRSZ)?;
            Some(format!("({len})"))
        }
        _ => None,
    }
}

pub(crate) fn format_regproc_oid(oid: u32, catalog: &dyn CatalogLookup) -> Option<String> {
    if oid == 0 {
        return Some("-".into());
    }
    catalog.proc_row_by_oid(oid).map(|row| row.proname)
}

pub(crate) fn format_regproc_oid_optional(
    oid: u32,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<String> {
    format_regproc_oid(oid, effective_catalog(catalog))
}

pub(crate) fn format_regprocedure_oid(oid: u32, catalog: &dyn CatalogLookup) -> Option<String> {
    if oid == 0 {
        return Some("-".into());
    }
    catalog
        .proc_row_by_oid(oid)
        .map(|row| function_signature_text(&row, catalog))
}

pub(crate) fn format_regprocedure_oid_optional(
    oid: u32,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<String> {
    format_regprocedure_oid(oid, effective_catalog(catalog))
}

pub(crate) fn format_regoper_oid(oid: u32, catalog: &dyn CatalogLookup) -> Option<String> {
    if oid == 0 {
        return Some("-".into());
    }
    catalog.operator_by_oid(oid).map(|row| row.oprname)
}

pub(crate) fn format_regoper_oid_optional(
    oid: u32,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<String> {
    format_regoper_oid(oid, effective_catalog(catalog))
}

pub(crate) fn format_regoperator_oid(oid: u32, catalog: &dyn CatalogLookup) -> Option<String> {
    if oid == 0 {
        return Some("-".into());
    }
    catalog
        .operator_by_oid(oid)
        .map(|row| operator_signature_text(&row, catalog))
}

pub(crate) fn format_regoperator_oid_optional(
    oid: u32,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<String> {
    format_regoperator_oid(oid, effective_catalog(catalog))
}

pub(crate) fn format_regcollation_oid(oid: u32, catalog: &dyn CatalogLookup) -> Option<String> {
    if oid == 0 {
        return Some("-".into());
    }
    catalog
        .collation_rows()
        .into_iter()
        .find(|row| row.oid == oid)
        .map(|row| quote_identifier_if_needed(&row.collname))
}

pub(crate) fn format_regcollation_oid_optional(
    oid: u32,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<String> {
    format_regcollation_oid(oid, effective_catalog(catalog))
}

pub(crate) fn function_signature_text(
    proc_row: &crate::include::catalog::PgProcRow,
    catalog: &dyn CatalogLookup,
) -> String {
    let arg_types = parse_oid_list(&proc_row.proargtypes)
        .unwrap_or_default()
        .into_iter()
        .map(|oid| format_type_text(oid, None, catalog))
        .collect::<Vec<_>>()
        .join(",");
    format!("{}({arg_types})", function_name_text(&proc_row.proname))
}

fn function_name_text(proname: &str) -> String {
    match proname {
        "interval" | "numeric" | "varchar" => {
            format!("\"{}\"", proname.replace('"', "\"\""))
        }
        _ => quote_identifier_if_needed(proname),
    }
}

pub(crate) fn operator_signature_text(
    operator_row: &crate::include::catalog::PgOperatorRow,
    catalog: &dyn CatalogLookup,
) -> String {
    let left = if operator_row.oprleft == 0 {
        "none".to_string()
    } else {
        format_type_text(operator_row.oprleft, None, catalog)
    };
    let right = if operator_row.oprright == 0 {
        "none".to_string()
    } else {
        format_type_text(operator_row.oprright, None, catalog)
    };
    format!("{}({left},{right})", operator_row.oprname)
}

pub(crate) fn type_oid_to_sql_type(oid: u32, catalog: &dyn CatalogLookup) -> Option<SqlType> {
    catalog.type_by_oid(oid).map(|row| row.sql_type)
}

fn parse_numeric_oid(input: &str) -> Result<Option<u32>, ExecError> {
    let text = input.trim();
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    if text.is_empty() || digits.is_empty() || !digits.chars().all(|ch| ch.is_ascii_digit()) {
        return Ok(None);
    }
    let value = text
        .parse::<i128>()
        .map_err(|_| ExecError::InvalidIntegerInput {
            ty: "oid",
            value: text.to_string(),
        })?;
    if (0..=u32::MAX as i128).contains(&value) {
        Ok(Some(value as u32))
    } else if (i32::MIN as i128..=-1).contains(&value) {
        Ok(Some((value as i32) as u32))
    } else {
        Err(ExecError::IntegerOutOfRange {
            ty: "oid",
            value: text.to_string(),
        })
    }
}

fn validate_regtype_input_syntax(input: &str) -> Result<(), ExecError> {
    if numeric_type_modifier_arity(input) > Some(2) {
        return Err(detailed_error("invalid NUMERIC type modifier", "42601"));
    }
    if let Some(parts) = regtype_qualified_name_parts(input)? {
        match parts.as_slice() {
            [_name] => {}
            [schema, _name] if schema == "pg_catalog" => {}
            [schema, _name] => {
                return Err(detailed_error(
                    format!("schema \"{schema}\" does not exist"),
                    "3F000",
                ));
            }
            [_catalog, _schema, _name] => {
                return Err(detailed_error(
                    format!("cross-database references are not implemented: {input}"),
                    "0A000",
                ));
            }
            _ => {
                return Err(detailed_error(
                    format!("improper qualified name (too many dotted names): {input}"),
                    "42601",
                ));
            }
        }
    }
    Ok(())
}

fn reject_trailing_regtype_tokens(input: &str, raw_type: &RawTypeName) -> Result<(), ExecError> {
    if !matches!(raw_type, RawTypeName::Named { .. }) {
        return Ok(());
    }
    let Some(token) = first_unquoted_whitespace_token(input) else {
        return Ok(());
    };
    Err(ExecError::WithContext {
        source: Box::new(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "type name",
            actual: format!("syntax error at or near \"{token}\""),
        })),
        context: format!("invalid type name \"{input}\""),
    })
}

fn regtype_qualified_name_parts(input: &str) -> Result<Option<Vec<String>>, ExecError> {
    let Some(prefix) = type_name_prefix(input) else {
        return Ok(None);
    };
    if prefix.contains(char::is_whitespace) {
        return Ok(None);
    }
    if !prefix.contains('.') {
        return Ok(None);
    }
    parse_sql_name_parts(prefix).map(Some)
}

fn type_name_prefix(input: &str) -> Option<&str> {
    let mut quoted = false;
    let mut chars = input.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        if quoted {
            if ch == '"' {
                if chars.peek().is_some_and(|(_, next)| *next == '"') {
                    chars.next();
                } else {
                    quoted = false;
                }
            }
            continue;
        }
        match ch {
            '"' => quoted = true,
            '(' => return Some(input[..idx].trim()),
            _ => {}
        }
    }
    (!quoted).then_some(input.trim())
}

fn first_unquoted_whitespace_token(input: &str) -> Option<String> {
    let mut quoted = false;
    let mut chars = input.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        if quoted {
            if ch == '"' {
                if chars.peek().is_some_and(|(_, next)| *next == '"') {
                    chars.next();
                } else {
                    quoted = false;
                }
            }
            continue;
        }
        match ch {
            '"' => quoted = true,
            ch if ch.is_whitespace() => {
                let rest = input[idx..].trim_start();
                let token = rest
                    .split(|ch: char| ch.is_whitespace() || ch == '(' || ch == ')' || ch == ',')
                    .next()
                    .unwrap_or_default();
                return (!token.is_empty()).then(|| token.to_string());
            }
            _ => {}
        }
    }
    None
}

fn numeric_type_modifier_arity(input: &str) -> Option<usize> {
    let trimmed = input.trim();
    let lower = trimmed.to_ascii_lowercase();
    let rest = lower.strip_prefix("numeric")?.trim_start();
    let inner = rest.strip_prefix('(')?.strip_suffix(')')?;
    Some(inner.split(',').count())
}

fn parse_signature<'a>(
    input: &'a str,
    _object_type: &'static str,
) -> Result<(&'a str, &'a str), ExecError> {
    let input = input.trim();
    let Some(open_paren) = input.rfind('(') else {
        return Err(detailed_error("expected a left parenthesis", "22P02"));
    };
    if !input.ends_with(')') {
        return Err(detailed_error("expected a right parenthesis", "22P02"));
    }
    let name = input[..open_paren].trim();
    if name.is_empty() {
        return Err(detailed_error("invalid name syntax", "42601"));
    }
    Ok((name, &input[open_paren + 1..input.len() - 1]))
}

fn split_signature_args(arg_sql: &str) -> Result<Vec<String>, ExecError> {
    if arg_sql.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut args = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut depth = 0usize;
    let mut chars = arg_sql.chars().peekable();
    while let Some(ch) = chars.next() {
        if quoted {
            current.push(ch);
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else {
                    quoted = false;
                }
            }
            continue;
        }
        match ch {
            '"' => {
                quoted = true;
                current.push(ch);
            }
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                current.push(ch);
            }
            ',' if depth == 0 => {
                args.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if quoted {
        return Err(detailed_error("invalid name syntax", "42601"));
    }
    args.push(current.trim().to_string());
    Ok(args)
}

fn parse_signature_arg_oids(
    arg_sql: &str,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<u32>, ExecError> {
    split_signature_args(arg_sql)?
        .into_iter()
        .map(|arg| parse_type_oid(&arg, catalog))
        .collect()
}

fn parse_operator_arg_oid(arg: &str, catalog: &dyn CatalogLookup) -> Result<u32, ExecError> {
    if arg.eq_ignore_ascii_case("none") {
        return Ok(0);
    }
    parse_type_oid(arg, catalog)
}

fn parse_type_oid(arg: &str, catalog: &dyn CatalogLookup) -> Result<u32, ExecError> {
    let raw_type = parse_type_name(arg).map_err(ExecError::Parse)?;
    let sql_type = resolve_raw_type_name(&raw_type, catalog).map_err(ExecError::Parse)?;
    catalog
        .type_oid_for_sql_type(sql_type)
        .ok_or_else(|| detailed_error(format!("type \"{arg}\" does not exist"), "42704"))
}

fn parse_oid_list(text: &str) -> Option<Vec<u32>> {
    if text.trim().is_empty() {
        return Some(Vec::new());
    }
    text.split_whitespace()
        .map(|oid| oid.parse::<u32>().ok())
        .collect()
}

fn parse_single_i32_typmod(input: &str, type_name: &str) -> Option<i32> {
    let rest = input.strip_prefix(type_name)?.trim_start();
    let inner = rest.strip_prefix('(')?.strip_suffix(')')?;
    if inner.contains(',') {
        return None;
    }
    inner.trim().parse::<i32>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::catalog::{
        ACLITEM_ARRAY_TYPE_OID, ACLITEM_TYPE_OID, BIT_TYPE_OID,
        INFORMATION_SCHEMA_INDEX_POSITION_PROC_OID, INT4_TYPE_OID, NUMERIC_TYPE_OID,
        POSIX_COLLATION_OID, REGDICTIONARY_ARRAY_TYPE_OID, REGDICTIONARY_TYPE_OID,
        TIMESTAMP_TYPE_OID, VARCHAR_TYPE_OID, VOID_TYPE_OID,
    };

    #[test]
    fn regproc_regoper_regcollation_helpers_resolve_and_format() {
        assert_eq!(resolve_regproc_oid("pg_catalog.now", None).unwrap(), 1299);
        assert_eq!(
            resolve_regproc_oid("information_schema._pg_index_position", None).unwrap(),
            INFORMATION_SCHEMA_INDEX_POSITION_PROC_OID
        );
        assert_eq!(resolve_regoper_oid("pg_catalog.||/", None).unwrap(), 597);
        assert_eq!(
            resolve_regcollation_oid("pg_catalog.\"POSIX\"", None).unwrap(),
            POSIX_COLLATION_OID
        );

        assert_eq!(format_regproc_oid_optional(1299, None).unwrap(), "now");
        assert_eq!(format_regoper_oid_optional(597, None).unwrap(), "||/");
        assert_eq!(
            format_regcollation_oid_optional(POSIX_COLLATION_OID, None).unwrap(),
            "\"POSIX\""
        );
    }

    #[test]
    fn format_type_handles_regression_cases() {
        assert_eq!(
            format_type_text(VARCHAR_TYPE_OID, Some(36), &BOOTSTRAP_LOOKUP),
            "character varying(32)"
        );
        assert_eq!(
            format_type_text(BIT_TYPE_OID, Some(1), &BOOTSTRAP_LOOKUP),
            "bit(1)"
        );
        assert_eq!(
            format_type_text(BIT_TYPE_OID, Some(-1), &BOOTSTRAP_LOOKUP),
            "\"bit\""
        );
        assert_eq!(
            format_type_text(TIMESTAMP_TYPE_OID, Some(3), &BOOTSTRAP_LOOKUP),
            "timestamp(3) without time zone"
        );
        assert_eq!(
            format_type_text(INT4_TYPE_OID, None, &BOOTSTRAP_LOOKUP),
            "integer"
        );
        assert_eq!(
            format_type_text(INT4_ARRAY_TYPE_OID, None, &BOOTSTRAP_LOOKUP),
            "integer[]"
        );
        assert_eq!(
            format_type_text(NUMERIC_TYPE_OID, None, &BOOTSTRAP_LOOKUP),
            "numeric"
        );
        assert_eq!(
            format_type_text(ACLITEM_TYPE_OID, None, &BOOTSTRAP_LOOKUP),
            "aclitem"
        );
        assert_eq!(
            format_type_text(ACLITEM_ARRAY_TYPE_OID, None, &BOOTSTRAP_LOOKUP),
            "aclitem[]"
        );
        assert_eq!(
            format_type_text(REGDICTIONARY_TYPE_OID, None, &BOOTSTRAP_LOOKUP),
            "regdictionary"
        );
        assert_eq!(
            format_type_text(REGDICTIONARY_ARRAY_TYPE_OID, None, &BOOTSTRAP_LOOKUP),
            "regdictionary[]"
        );
        assert_eq!(
            format_type_text(VOID_TYPE_OID, None, &BOOTSTRAP_LOOKUP),
            "void"
        );
        assert_eq!(format_type_text(9_999_999, None, &BOOTSTRAP_LOOKUP), "???");
    }

    #[test]
    fn regtype_input_distinguishes_soft_and_hard_errors() {
        let soft = resolve_regtype_oid("no_such_type", None).unwrap_err();
        assert!(!is_hard_regtype_input_error(&soft));
        assert!(matches!(
            soft,
            ExecError::DetailedError {
                message,
                sqlstate: "42704",
                ..
            } if message == "type \"no_such_type\" does not exist"
        ));

        let hard = resolve_regtype_oid("numeric(1,2,3)", None).unwrap_err();
        assert!(is_hard_regtype_input_error(&hard));

        let hard = resolve_regtype_oid("way.too.many.names", None).unwrap_err();
        assert!(is_hard_regtype_input_error(&hard));
    }
}
