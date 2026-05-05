use std::borrow::Cow;
use std::collections::HashMap;
use std::io::{self, Write};

use crate::backend::access::heap::heapam::HeapError;
use crate::backend::executor::value_io::builtin_type_oid_for_sql_type;
use crate::backend::executor::{
    ArrayValue, ExecError, QueryColumn, Value, render_datetime_value_text_with_config,
    render_internal_char_text, render_interval_text_with_config, render_macaddr_text,
    render_macaddr8_text, render_multirange_text_with_config, render_pg_lsn_text,
    render_range_text_with_config,
};
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::statistics::{
    render_pg_dependencies_text, render_pg_mcv_list_text, render_pg_ndistinct_text,
};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::include::access::htup::TupleError;
use crate::include::catalog::{
    PG_DEPENDENCIES_TYPE_OID, PG_MCV_LIST_TYPE_OID, PG_NDISTINCT_TYPE_OID,
};
use crate::include::nodes::datum::InetValue;
use crate::include::nodes::parsenodes::CopyFormat;
use crate::pgrust::session::ByteaOutputFormat;

// :HACK: Preserve the old root pqformat type path while scalar formatting
// options live in pgrust_expr.
pub(crate) type FloatFormatOptions = pgrust_expr::libpq::pqformat::FloatFormatOptions;

fn postgres_srf_placement_error(message: &str) -> Option<Cow<'_, str>> {
    match message {
        "set-returning functions are not allowed in aggregate arguments" => Some(Cow::Borrowed(
            "aggregate function calls cannot contain set-returning function calls",
        )),
        "set-returning functions are not allowed in window aggregate arguments" => Some(
            Cow::Borrowed("window function calls cannot contain set-returning function calls"),
        ),
        message if message.starts_with("set-returning functions are not allowed in ") => {
            Some(Cow::Borrowed(message))
        }
        _ => None,
    }
}

fn postgres_srf_placement_hint(message: &str) -> Option<&'static str> {
    match message {
        "set-returning functions are not allowed in CASE"
        | "set-returning functions are not allowed in COALESCE"
        | "set-returning functions are not allowed in aggregate arguments"
        | "set-returning functions are not allowed in window aggregate arguments" => {
            Some("You might be able to move the set-returning function into a LATERAL FROM item.")
        }
        _ => None,
    }
}

pub(crate) fn format_exec_error(e: &ExecError) -> String {
    match e {
        ExecError::WithContext { source, .. }
        | ExecError::WithInternalQueryContext { source, .. } => format_exec_error(source),
        ExecError::Parse(crate::backend::parser::ParseError::Positioned { source, .. }) => {
            format_exec_error(&ExecError::Parse((**source).clone()))
        }
        ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
            expected: "text or bit argument",
            actual,
        })
        | ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
            expected: "text, bytea, bit, or tsvector argument",
            actual,
        }) if actual.starts_with("Length(") => {
            let signature = actual.replace("Length", "length");
            format!("function {signature} does not exist")
        }
        ExecError::Parse(crate::backend::parser::ParseError::FeatureNotSupported(message)) => {
            postgres_srf_placement_error(message)
                .map(Cow::into_owned)
                .unwrap_or_else(|| {
                    crate::backend::parser::ParseError::FeatureNotSupported(message.clone())
                        .to_string()
                })
        }
        ExecError::Parse(p) => p.to_string(),
        ExecError::Regex(err) => err.message.clone(),
        ExecError::JsonInput { message, .. } => message.clone(),
        ExecError::XmlInput { message, .. } => message.clone(),
        ExecError::DetailedError { message, .. } | ExecError::DiagnosticError { message, .. } => {
            message.clone()
        }
        ExecError::RaiseException(message) => message.clone(),
        ExecError::InvalidRegex(message) => message.clone(),
        ExecError::CardinalityViolation { message, .. } => message.clone(),
        ExecError::UniqueViolation { constraint, .. } => {
            format!("duplicate key value violates unique constraint \"{constraint}\"")
        }
        ExecError::NotNullViolation {
            relation, column, ..
        } => format!(
            "null value in column \"{column}\" of relation \"{relation}\" violates not-null constraint"
        ),
        ExecError::CheckViolation {
            relation,
            constraint,
            ..
        } => format!(
            "new row for relation \"{relation}\" violates check constraint \"{constraint}\""
        ),
        ExecError::ForeignKeyViolation { message, .. } => message.clone(),
        ExecError::StringDataRightTruncation { ty } => format!("value too long for type {ty}"),
        ExecError::ArrayInput { message, .. } => message.clone(),
        ExecError::InvalidIntegerInput { ty, value } => {
            format!("invalid input syntax for type {ty}: \"{value}\"")
        }
        ExecError::IntegerOutOfRange { ty, value } => {
            format!("value \"{value}\" is out of range for type {ty}")
        }
        ExecError::InvalidNumericInput(value) => {
            format!("invalid input syntax for type numeric: \"{value}\"")
        }
        ExecError::InvalidByteaInput { .. } => "invalid input syntax for type bytea".to_string(),
        ExecError::InvalidUuidInput { value } => {
            format!("invalid input syntax for type uuid: \"{value}\"")
        }
        ExecError::InvalidByteaHexDigit { digit, .. } => {
            format!("invalid hexadecimal digit: \"{digit}\"")
        }
        ExecError::InvalidByteaHexOddDigits { .. } => {
            "invalid hexadecimal data: odd number of digits".to_string()
        }
        ExecError::InvalidGeometryInput { ty, value } => {
            pgrust_expr::geometry_input_error_message(ty, value)
                .unwrap_or_else(|| format!("invalid input syntax for type {ty}: \"{value}\""))
        }
        ExecError::InvalidBitInput { digit, is_hex } => {
            if *is_hex {
                format!("\"{digit}\" is not a valid hexadecimal digit")
            } else {
                format!("\"{digit}\" is not a valid binary digit")
            }
        }
        ExecError::BitStringLengthMismatch { actual, expected } => {
            format!("bit string length {actual} does not match type bit({expected})")
        }
        ExecError::BitStringTooLong { limit, .. } => {
            format!("bit string too long for type bit varying({limit})")
        }
        ExecError::BitStringSizeMismatch { op } => match *op {
            "&" => "cannot AND bit strings of different sizes".to_string(),
            "|" => "cannot OR bit strings of different sizes".to_string(),
            "#" => "cannot XOR bit strings of different sizes".to_string(),
            _ => format!("cannot apply {op} to bit strings of different sizes"),
        },
        ExecError::BitIndexOutOfRange { index, max_index } => {
            format!("bit index {index} out of valid range (0..{max_index})")
        }
        ExecError::NegativeSubstringLength => "negative substring length not allowed".to_string(),
        ExecError::InvalidBooleanInput { value } => {
            format!("invalid input syntax for type boolean: \"{value}\"")
        }
        ExecError::InvalidFloatInput { ty, value } => {
            format!("invalid input syntax for type {ty}: \"{value}\"")
        }
        ExecError::FloatOutOfRange { ty, value } => {
            format!("\"{value}\" is out of range for type {ty}")
        }
        ExecError::FloatOverflow => "value out of range: overflow".to_string(),
        ExecError::FloatUnderflow => "value out of range: underflow".to_string(),
        ExecError::NumericNaNToInt { ty } => format!("cannot convert NaN to {ty}"),
        ExecError::NumericInfinityToInt { ty } => format!("cannot convert infinity to {ty}"),
        ExecError::InvalidStorageValue { details, .. } => details.clone(),
        ExecError::Int2OutOfRange => "smallint out of range".to_string(),
        ExecError::Int4OutOfRange => "integer out of range".to_string(),
        ExecError::Int8OutOfRange => "bigint out of range".to_string(),
        ExecError::OidOutOfRange => "OID out of range".to_string(),
        ExecError::NumericFieldOverflow => "numeric field overflow".to_string(),
        ExecError::RequestedLengthTooLarge => "requested length too large".to_string(),
        ExecError::Interrupted(reason) => reason.message().to_string(),
        ExecError::DivisionByZero(_) => "division by zero".to_string(),
        ExecError::GenerateSeriesZeroStep => "step size cannot equal zero".to_string(),
        ExecError::GenerateSeriesInvalidArg(arg, issue) => {
            if *arg == "step size" {
                format!("{arg} cannot be {issue}")
            } else {
                format!("{arg} value cannot be {issue}")
            }
        }
        ExecError::TypeMismatch { op, left, right } if type_mismatch_op_is_operator(op) => {
            format!(
                "operator does not exist: {} {op} {}",
                value_type_name(left),
                value_type_name(right)
            )
        }
        ExecError::Heap(HeapError::Tuple(TupleError::Oversized { size, max_size })) => {
            format!("row is too big: size {size}, maximum size {max_size}")
        }
        ExecError::Heap(HeapError::NoEmptyLocalBuffer) => {
            "no empty local buffer available".to_string()
        }
        other => format!("{other:?}"),
    }
}

pub(crate) fn format_exec_error_hint(e: &ExecError) -> Option<String> {
    match e {
        ExecError::WithContext { source, .. }
        | ExecError::WithInternalQueryContext { source, .. } => format_exec_error_hint(source),
        ExecError::Parse(crate::backend::parser::ParseError::Positioned { source, .. }) => {
            format_exec_error_hint(&ExecError::Parse((**source).clone()))
        }
        ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
            expected: "text or bit argument",
            actual,
        })
        | ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
            expected: "text, bytea, bit, or tsvector argument",
            actual,
        }) if actual.starts_with("Length(") => Some(
            "No function matches the given name and argument types. You might need to add explicit type casts.".into(),
        ),
        ExecError::Parse(crate::backend::parser::ParseError::UndefinedOperator { .. }) => Some(
            "No operator matches the given name and argument types. You might need to add explicit type casts.".into(),
        ),
        ExecError::Parse(crate::backend::parser::ParseError::FeatureNotSupported(message))
            if postgres_srf_placement_hint(message).is_some() =>
        {
            postgres_srf_placement_hint(message).map(str::to_string)
        }
        ExecError::TypeMismatch { op, .. } if type_mismatch_op_is_operator(op) => Some(
            "No operator matches the given name and argument types. You might need to add explicit type casts.".into(),
        ),
        ExecError::RaiseException(message)
            if message.starts_with("unrecognized format() type specifier")
                || message == "unterminated format() type specifier" =>
        {
            Some("For a single \"%\" use \"%%\".".into())
        }
        ExecError::DetailedError { hint, .. } | ExecError::DiagnosticError { hint, .. } => {
            hint.clone()
        }
        ExecError::CardinalityViolation { hint, .. } => hint.clone(),
        _ => None,
    }
}

fn type_mismatch_op_is_operator(op: &str) -> bool {
    !op.is_empty() && op.chars().all(|ch| "!~+-*/<>=@#%^&|`?".contains(ch))
}

fn value_type_name(value: &Value) -> String {
    match value {
        Value::Int16(_) => "smallint",
        Value::Int32(_) => "integer",
        Value::Int64(_) => "bigint",
        Value::Xid8(_) => "xid8",
        Value::Money(_) => "money",
        Value::Date(_) => "date",
        Value::Time(_) => "time without time zone",
        Value::TimeTz(_) => "time with time zone",
        Value::Timestamp(_) => "timestamp without time zone",
        Value::TimestampTz(_) => "timestamp with time zone",
        Value::Interval(_) => "interval",
        Value::Bit(_) => "bit",
        Value::Bytea(_) => "bytea",
        Value::Uuid(_) => "uuid",
        Value::Inet(_) => "inet",
        Value::Cidr(_) => "cidr",
        Value::MacAddr(_) => "macaddr",
        Value::MacAddr8(_) => "macaddr8",
        Value::Point(_) => "point",
        Value::Lseg(_) => "lseg",
        Value::Path(_) => "path",
        Value::Line(_) => "line",
        Value::Box(_) => "box",
        Value::Polygon(_) => "polygon",
        Value::Circle(_) => "circle",
        Value::Range(_) => "anyrange",
        Value::Multirange(_) => "anymultirange",
        Value::Float64(_) => "double precision",
        Value::Numeric(_) => "numeric",
        Value::Json(_) => "json",
        Value::Jsonb(_) => "jsonb",
        Value::JsonPath(_) => "jsonpath",
        Value::Xml(_) => "xml",
        Value::TsVector(_) => "tsvector",
        Value::TsQuery(_) => "tsquery",
        Value::PgLsn(_) => "pg_lsn",
        Value::Tid(_) => "tid",
        Value::Text(_) | Value::TextRef(_, _) => "text",
        Value::EnumOid(_) => "anyenum",
        Value::InternalChar(_) => "\"char\"",
        Value::Bool(_) => "boolean",
        Value::Array(_) | Value::PgArray(_) => "anyarray",
        Value::Record(_) => "record",
        Value::IndirectVarlena(_) => "text",
        Value::DroppedColumn(_) | Value::WrongTypeColumn { .. } | Value::Null => "unknown",
    }
    .into()
}

fn enum_array_type_oid(
    sql_type: Option<SqlType>,
    array: &ArrayValue,
    enum_labels: Option<&HashMap<(u32, u32), String>>,
) -> Option<u32> {
    let labels = enum_labels?;
    if let Some(sql_type) = sql_type
        && sql_type.is_array
        && let Some(enum_type_oid) = enum_label_type_oid(sql_type.element_type())
    {
        return Some(enum_type_oid);
    }
    array.element_type_oid.filter(|type_oid| {
        labels
            .keys()
            .any(|(enum_type_oid, _)| enum_type_oid == type_oid)
    })
}

fn enum_label_type_oid(sql_type: SqlType) -> Option<u32> {
    if !matches!(sql_type.kind, SqlTypeKind::Enum) || sql_type.type_oid == 0 {
        return None;
    }
    Some(if sql_type.typrelid != 0 {
        sql_type.typrelid
    } else {
        sql_type.type_oid
    })
}

fn format_enum_array_value_text(
    array: &ArrayValue,
    enum_type_oid: u32,
    enum_labels: &HashMap<(u32, u32), String>,
    datetime_config: &DateTimeConfig,
) -> String {
    if array.dimensions.is_empty() {
        return "{}".into();
    }
    let mut out = String::new();
    if array.dimensions.iter().any(|dim| dim.lower_bound != 1) {
        for dim in &array.dimensions {
            let upper = dim.lower_bound + dim.length as i32 - 1;
            out.push('[');
            out.push_str(&dim.lower_bound.to_string());
            out.push(':');
            out.push_str(&upper.to_string());
            out.push(']');
        }
        out.push('=');
    }
    out.push_str(&format_enum_array_values_nested(
        array,
        0,
        &mut 0,
        enum_type_oid,
        enum_labels,
        datetime_config,
    ));
    out
}

fn format_enum_array_values_nested(
    array: &ArrayValue,
    depth: usize,
    offset: &mut usize,
    enum_type_oid: u32,
    enum_labels: &HashMap<(u32, u32), String>,
    datetime_config: &DateTimeConfig,
) -> String {
    let mut out = String::from("{");
    let len = array.dimensions[depth].length;
    for idx in 0..len {
        if idx > 0 {
            out.push(',');
        }
        if depth + 1 < array.dimensions.len() {
            out.push_str(&format_enum_array_values_nested(
                array,
                depth + 1,
                offset,
                enum_type_oid,
                enum_labels,
                datetime_config,
            ));
            continue;
        }
        let item = &array.elements[*offset];
        *offset += 1;
        match item {
            Value::Null => out.push_str("NULL"),
            Value::EnumOid(label_oid) => {
                let rendered = enum_labels
                    .get(&(enum_type_oid, *label_oid))
                    .cloned()
                    .unwrap_or_else(|| label_oid.to_string());
                push_array_text_element(&mut out, &rendered);
            }
            Value::PgArray(nested) => out.push_str(&format_enum_array_value_text(
                nested,
                enum_type_oid,
                enum_labels,
                datetime_config,
            )),
            other => {
                let rendered =
                    crate::backend::executor::value_io::format_array_value_text_with_config(
                        &ArrayValue::from_1d(vec![other.clone()]),
                        datetime_config,
                    );
                out.push_str(rendered.trim_start_matches('{').trim_end_matches('}'));
            }
        }
    }
    out.push('}');
    out
}

fn push_array_text_element(out: &mut String, text: &str) {
    if text.is_empty()
        || text.eq_ignore_ascii_case("NULL")
        || text
            .chars()
            .any(|ch| matches!(ch, '"' | '\\' | '{' | '}' | ',' | ' ' | '\t' | '\n' | '\r'))
    {
        out.push('"');
        for ch in text.chars() {
            if matches!(ch, '"' | '\\') {
                out.push('\\');
            }
            out.push(ch);
        }
        out.push('"');
    } else {
        out.push_str(text);
    }
}

pub(crate) fn infer_command_tag(sql: &str, affected: usize) -> String {
    pgrust_protocol::pqformat::infer_command_tag(sql, affected)
}

pub(crate) fn infer_dml_returning_command_tag(sql: &str, affected: usize) -> Option<String> {
    pgrust_protocol::pqformat::infer_dml_returning_command_tag(sql, affected)
}

pub(crate) fn send_query_result(
    stream: &mut impl Write,
    columns: &[QueryColumn],
    rows: &[Vec<Value>],
    tag: &str,
    float_format: FloatFormatOptions,
    role_names: Option<&HashMap<u32, String>>,
    relation_names: Option<&HashMap<u32, String>>,
    proc_names: Option<&HashMap<u32, String>>,
    namespace_names: Option<&HashMap<u32, String>>,
    type_names: Option<&HashMap<u32, String>>,
    type_catalog: Option<&dyn crate::backend::parser::CatalogLookup>,
    enum_labels: Option<&HashMap<(u32, u32), String>>,
    proc_signatures: Option<&HashMap<u32, String>>,
) -> io::Result<()> {
    pgrust_protocol::pqformat::send_query_result_with_rows(
        stream,
        columns,
        rows,
        tag,
        |stream, row, row_buf| {
            send_typed_data_row(
                stream,
                row,
                columns,
                &[],
                row_buf,
                float_format.clone(),
                role_names,
                relation_names,
                proc_names,
                namespace_names,
                type_names,
                type_catalog,
                enum_labels,
                proc_signatures,
            )
        },
    )
}

pub(crate) fn send_auth_ok(w: &mut impl Write) -> io::Result<()> {
    pgrust_protocol::pqformat::send_auth_ok(w)
}

pub(crate) fn send_parameter_status(w: &mut impl Write, name: &str, value: &str) -> io::Result<()> {
    pgrust_protocol::pqformat::send_parameter_status(w, name, value)
}

pub(crate) fn send_backend_key_data(w: &mut impl Write, pid: i32, key: i32) -> io::Result<()> {
    pgrust_protocol::pqformat::send_backend_key_data(w, pid, key)
}

pub(crate) fn send_ready_for_query(w: &mut impl Write, status: u8) -> io::Result<()> {
    pgrust_protocol::pqformat::send_ready_for_query(w, status)
}

pub(crate) fn send_row_description(w: &mut impl Write, columns: &[QueryColumn]) -> io::Result<()> {
    pgrust_protocol::pqformat::send_row_description(w, columns)
}

pub(crate) fn send_row_description_with_formats(
    w: &mut impl Write,
    columns: &[QueryColumn],
    result_formats: &[i16],
) -> io::Result<()> {
    pgrust_protocol::pqformat::send_row_description_with_formats(w, columns, result_formats)
}

pub(crate) fn validate_binary_result_formats(
    rows: &[Vec<Value>],
    columns: &[QueryColumn],
    result_formats: &[i16],
) -> Result<(), ExecError> {
    pgrust_protocol::pqformat::validate_binary_result_formats(
        rows,
        columns,
        result_formats,
        |column| validate_binary_output_type(column.sql_type),
        |value, column| encode_binary_data_row_value(value, column.sql_type),
    )
    .map_err(|err| match err {
        pgrust_protocol::pqformat::ResultFormatValidationError::UnsupportedResultFormatCode(
            code,
        ) => ExecError::Parse(crate::backend::parser::ParseError::FeatureNotSupported(
            format!("result format code {code}"),
        )),
        pgrust_protocol::pqformat::ResultFormatValidationError::BinaryType(err)
        | pgrust_protocol::pqformat::ResultFormatValidationError::BinaryEncode(err) => err,
    })
}

fn validate_binary_output_type(sql_type: SqlType) -> Result<(), ExecError> {
    if pgrust_protocol::pqformat::binary_output_type_supported(sql_type) {
        Ok(())
    } else {
        Err(ExecError::Parse(
            crate::backend::parser::ParseError::FeatureNotSupported(format!(
                "binary output for {:?}",
                sql_type
            )),
        ))
    }
}

fn format_catalog_oid_text(
    oid: u32,
    names: Option<&HashMap<u32, String>>,
    dash_on_zero: bool,
) -> String {
    if dash_on_zero && oid == 0 {
        return "-".to_string();
    }
    names
        .and_then(|names| names.get(&oid).cloned())
        .unwrap_or_else(|| oid.to_string())
}

fn format_typed_oid_text(
    kind: Option<SqlTypeKind>,
    oid: u32,
    role_names: Option<&HashMap<u32, String>>,
    relation_names: Option<&HashMap<u32, String>>,
    proc_names: Option<&HashMap<u32, String>>,
    proc_signatures: Option<&HashMap<u32, String>>,
    namespace_names: Option<&HashMap<u32, String>>,
    type_names: Option<&HashMap<u32, String>>,
    type_catalog: Option<&dyn crate::backend::parser::CatalogLookup>,
) -> Option<String> {
    match kind? {
        SqlTypeKind::RegRole => Some(format_catalog_oid_text(oid, role_names, true)),
        SqlTypeKind::RegClass => Some(format_catalog_oid_text(oid, relation_names, true)),
        SqlTypeKind::RegNamespace => Some(format_catalog_oid_text(oid, namespace_names, true)),
        SqlTypeKind::RegProc => Some(
            crate::backend::executor::expr_reg::format_regproc_oid_optional(oid, None)
                .unwrap_or_else(|| format_proc_oid_text(oid, proc_names, true)),
        ),
        SqlTypeKind::RegProcedure => Some(
            proc_signatures
                .and_then(|names| names.get(&oid).cloned())
                .or_else(|| {
                    crate::backend::executor::expr_reg::format_regprocedure_oid_optional(oid, None)
                })
                .or_else(|| format_proc_oid_text_optional(oid, proc_names))
                .unwrap_or_else(|| format_catalog_oid_text(oid, None, true)),
        ),
        SqlTypeKind::RegOper => Some(
            crate::backend::executor::expr_reg::format_regoper_oid_optional(oid, None)
                .unwrap_or_else(|| format_catalog_oid_text(oid, None, true)),
        ),
        SqlTypeKind::RegOperator => Some(
            crate::backend::executor::expr_reg::format_regoperator_oid_optional(oid, None)
                .unwrap_or_else(|| format_catalog_oid_text(oid, None, true)),
        ),
        SqlTypeKind::RegType => type_catalog
            .map(|catalog| crate::backend::executor::expr_reg::format_type_text(oid, None, catalog))
            .or_else(|| type_names.and_then(|names| names.get(&oid).cloned()))
            .or_else(|| {
                match crate::backend::executor::expr_reg::format_type_optional(
                    Some(oid),
                    None,
                    None,
                ) {
                    Value::Text(text) => Some(text.to_string()),
                    _ => Some(oid.to_string()),
                }
            }),
        SqlTypeKind::RegCollation => Some(
            crate::backend::executor::expr_reg::format_regcollation_oid_optional(oid, None)
                .unwrap_or_else(|| format_catalog_oid_text(oid, None, true)),
        ),
        _ => None,
    }
}

fn format_proc_oid_text(
    oid: u32,
    proc_names: Option<&HashMap<u32, String>>,
    dash_on_zero: bool,
) -> String {
    if dash_on_zero && oid == 0 {
        return "-".to_string();
    }
    format_proc_oid_text_optional(oid, proc_names).unwrap_or_else(|| oid.to_string())
}

fn format_proc_oid_text_optional(
    oid: u32,
    proc_names: Option<&HashMap<u32, String>>,
) -> Option<String> {
    proc_names
        .and_then(|names| names.get(&oid))
        .map(|name| crate::backend::executor::expr_reg::quote_identifier_if_needed(name))
}

pub(crate) fn send_typed_data_row(
    w: &mut impl Write,
    values: &[Value],
    columns: &[QueryColumn],
    result_formats: &[i16],
    buf: &mut Vec<u8>,
    float_format: FloatFormatOptions,
    role_names: Option<&HashMap<u32, String>>,
    relation_names: Option<&HashMap<u32, String>>,
    proc_names: Option<&HashMap<u32, String>>,
    namespace_names: Option<&HashMap<u32, String>>,
    type_names: Option<&HashMap<u32, String>>,
    type_catalog: Option<&dyn crate::backend::parser::CatalogLookup>,
    enum_labels: Option<&HashMap<(u32, u32), String>>,
    proc_signatures: Option<&HashMap<u32, String>>,
) -> io::Result<()> {
    pgrust_protocol::pqformat::begin_data_row_body(buf, values.len());
    for (idx, val) in values.iter().enumerate() {
        let decoded_indirect;
        let val = if let Value::IndirectVarlena(indirect) = val {
            decoded_indirect = Some(
                crate::backend::executor::value_io::indirect_varlena_to_value(indirect)
                    .map_err(|err| io::Error::other(format!("{err:?}")))?,
            );
            decoded_indirect.as_ref().expect("decoded indirect value")
        } else {
            val
        };
        let sql_type = columns.get(idx).map(|col| col.sql_type);
        let format_code = pgrust_protocol::pqformat::result_format_code(result_formats, idx);
        if format_code == 1 {
            if matches!(val, Value::Null) {
                pgrust_protocol::pqformat::append_data_row_null_field(buf);
                continue;
            }
            let sql_type = sql_type.ok_or_else(|| io::Error::other("missing column type"))?;
            let payload = encode_binary_data_row_value(val, sql_type)
                .map_err(|e| io::Error::other(format!("{e:?}")))?;
            pgrust_protocol::pqformat::append_data_row_field(buf, &payload);
            continue;
        }
        if format_code != 0 {
            return Err(io::Error::other(format!(
                "unsupported result format code {}",
                format_code
            )));
        }
        match val {
            Value::Null => pgrust_protocol::pqformat::append_data_row_null_field(buf),
            Value::Int16(v) => {
                let start = buf.len();
                buf.extend_from_slice(&0_i32.to_be_bytes());
                let mut itoa_buf = itoa::Buffer::new();
                let written = itoa_buf.format(*v);
                buf.extend_from_slice(written.as_bytes());
                let text_len = (buf.len() - start - 4) as i32;
                buf[start..start + 4].copy_from_slice(&text_len.to_be_bytes());
            }
            Value::Int32(v) => {
                let start = buf.len();
                buf.extend_from_slice(&0_i32.to_be_bytes());
                if let Ok(oid) = u32::try_from(*v)
                    && let Some(text) = format_typed_oid_text(
                        sql_type.map(|ty| ty.kind),
                        oid,
                        role_names,
                        relation_names,
                        proc_names,
                        proc_signatures,
                        namespace_names,
                        type_names,
                        type_catalog,
                    )
                {
                    buf.extend_from_slice(text.as_bytes());
                } else if matches!(sql_type.map(|ty| ty.kind), Some(SqlTypeKind::RegRole)) {
                    if let Ok(role_oid) = u32::try_from(*v) {
                        if let Some(role_name) = role_names.and_then(|names| names.get(&role_oid)) {
                            buf.extend_from_slice(role_name.as_bytes());
                        } else {
                            let mut itoa_buf = itoa::Buffer::new();
                            let written = itoa_buf.format(*v);
                            buf.extend_from_slice(written.as_bytes());
                        }
                    } else {
                        let mut itoa_buf = itoa::Buffer::new();
                        let written = itoa_buf.format(*v);
                        buf.extend_from_slice(written.as_bytes());
                    }
                } else if matches!(sql_type.map(|ty| ty.kind), Some(SqlTypeKind::RegClass)) {
                    if let Ok(relation_oid) = u32::try_from(*v) {
                        buf.extend_from_slice(
                            format_catalog_oid_text(relation_oid, relation_names, true).as_bytes(),
                        );
                    } else {
                        let mut itoa_buf = itoa::Buffer::new();
                        let written = itoa_buf.format(*v);
                        buf.extend_from_slice(written.as_bytes());
                    }
                } else if matches!(sql_type.map(|ty| ty.kind), Some(SqlTypeKind::RegNamespace)) {
                    if let Ok(namespace_oid) = u32::try_from(*v) {
                        buf.extend_from_slice(
                            format_catalog_oid_text(namespace_oid, namespace_names, true)
                                .as_bytes(),
                        );
                    } else {
                        let mut itoa_buf = itoa::Buffer::new();
                        let written = itoa_buf.format(*v);
                        buf.extend_from_slice(written.as_bytes());
                    }
                } else if matches!(sql_type.map(|ty| ty.kind), Some(SqlTypeKind::RegProcedure)) {
                    if let Ok(proc_oid) = u32::try_from(*v) {
                        if proc_oid == 0 {
                            buf.extend_from_slice(b"-");
                        } else if let Some(proc_name) =
                            proc_names.and_then(|names| names.get(&proc_oid))
                        {
                            buf.extend_from_slice(proc_name.as_bytes());
                        } else {
                            let mut itoa_buf = itoa::Buffer::new();
                            let written = itoa_buf.format(*v);
                            buf.extend_from_slice(written.as_bytes());
                        }
                    } else {
                        let mut itoa_buf = itoa::Buffer::new();
                        let written = itoa_buf.format(*v);
                        buf.extend_from_slice(written.as_bytes());
                    }
                } else {
                    let mut itoa_buf = itoa::Buffer::new();
                    let written = itoa_buf.format(*v);
                    buf.extend_from_slice(written.as_bytes());
                }
                let text_len = (buf.len() - start - 4) as i32;
                buf[start..start + 4].copy_from_slice(&text_len.to_be_bytes());
            }
            Value::Int64(v) => {
                let start = buf.len();
                buf.extend_from_slice(&0_i32.to_be_bytes());
                if let Ok(oid) = u32::try_from(*v)
                    && let Some(text) = format_typed_oid_text(
                        sql_type.map(|ty| ty.kind),
                        oid,
                        role_names,
                        relation_names,
                        proc_names,
                        proc_signatures,
                        namespace_names,
                        type_names,
                        type_catalog,
                    )
                {
                    buf.extend_from_slice(text.as_bytes());
                } else if matches!(sql_type.map(|ty| ty.kind), Some(SqlTypeKind::RegRole)) {
                    if let Ok(role_oid) = u32::try_from(*v) {
                        if let Some(role_name) = role_names.and_then(|names| names.get(&role_oid)) {
                            buf.extend_from_slice(role_name.as_bytes());
                        } else {
                            let mut itoa_buf = itoa::Buffer::new();
                            let written = itoa_buf.format(*v);
                            buf.extend_from_slice(written.as_bytes());
                        }
                    } else {
                        let mut itoa_buf = itoa::Buffer::new();
                        let written = itoa_buf.format(*v);
                        buf.extend_from_slice(written.as_bytes());
                    }
                } else if matches!(sql_type.map(|ty| ty.kind), Some(SqlTypeKind::RegClass)) {
                    if let Ok(relation_oid) = u32::try_from(*v) {
                        buf.extend_from_slice(
                            format_catalog_oid_text(relation_oid, relation_names, true).as_bytes(),
                        );
                    } else {
                        let mut itoa_buf = itoa::Buffer::new();
                        let written = itoa_buf.format(*v);
                        buf.extend_from_slice(written.as_bytes());
                    }
                } else if matches!(sql_type.map(|ty| ty.kind), Some(SqlTypeKind::RegNamespace)) {
                    if let Ok(namespace_oid) = u32::try_from(*v) {
                        buf.extend_from_slice(
                            format_catalog_oid_text(namespace_oid, namespace_names, true)
                                .as_bytes(),
                        );
                    } else {
                        let mut itoa_buf = itoa::Buffer::new();
                        let written = itoa_buf.format(*v);
                        buf.extend_from_slice(written.as_bytes());
                    }
                } else if matches!(sql_type.map(|ty| ty.kind), Some(SqlTypeKind::RegProcedure)) {
                    if let Ok(proc_oid) = u32::try_from(*v) {
                        if proc_oid == 0 {
                            buf.extend_from_slice(b"-");
                        } else if let Some(proc_name) =
                            proc_names.and_then(|names| names.get(&proc_oid))
                        {
                            buf.extend_from_slice(proc_name.as_bytes());
                        } else {
                            let mut itoa_buf = itoa::Buffer::new();
                            let written = itoa_buf.format(*v);
                            buf.extend_from_slice(written.as_bytes());
                        }
                    } else {
                        let mut itoa_buf = itoa::Buffer::new();
                        let written = itoa_buf.format(*v);
                        buf.extend_from_slice(written.as_bytes());
                    }
                } else {
                    let mut itoa_buf = itoa::Buffer::new();
                    let written = itoa_buf.format(*v);
                    buf.extend_from_slice(written.as_bytes());
                }
                let text_len = (buf.len() - start - 4) as i32;
                buf[start..start + 4].copy_from_slice(&text_len.to_be_bytes());
            }
            Value::Xid8(v) => {
                let rendered = v.to_string();
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::Money(v) => {
                let rendered = crate::backend::executor::money_format_text(*v);
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::Bytea(v) => {
                let rendered = match sql_type.and_then(|ty| (!ty.is_array).then_some(ty.type_oid)) {
                    Some(PG_NDISTINCT_TYPE_OID) => render_pg_ndistinct_text(v)
                        .unwrap_or_else(|_| format_bytea_text(v, float_format.bytea_output)),
                    Some(PG_DEPENDENCIES_TYPE_OID) => render_pg_dependencies_text(v)
                        .unwrap_or_else(|_| format_bytea_text(v, float_format.bytea_output)),
                    Some(PG_MCV_LIST_TYPE_OID) => render_pg_mcv_list_text(v),
                    _ => format_bytea_text(v, float_format.bytea_output),
                };
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::Uuid(v) => {
                let rendered = crate::backend::executor::value_io::render_uuid_text(v);
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::Inet(v) => {
                let rendered = v.render_inet();
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::Cidr(v) => {
                let rendered = v.render_cidr();
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::MacAddr(v) => {
                let rendered = render_macaddr_text(v);
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::MacAddr8(v) => {
                let rendered = render_macaddr8_text(v);
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::Date(_)
            | Value::Time(_)
            | Value::TimeTz(_)
            | Value::Timestamp(_)
            | Value::TimestampTz(_) => {
                let rendered =
                    render_datetime_value_text_with_config(val, &float_format.datetime_config)
                        .expect("datetime values render");
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::Range(_) => {
                let rendered = render_range_text_with_config(val, &float_format.datetime_config)
                    .unwrap_or_default();
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::Multirange(_) => {
                let rendered =
                    render_multirange_text_with_config(val, &float_format.datetime_config)
                        .unwrap_or_default();
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::Bit(v) => {
                let rendered = crate::backend::executor::render_bit_text(v);
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::Float64(v) => {
                let start = buf.len();
                buf.extend_from_slice(&0_i32.to_be_bytes());
                let rendered = match sql_type.map(|ty| ty.kind) {
                    Some(SqlTypeKind::Float4) => format_float4_text(*v, float_format.clone()),
                    _ => format_float8_text(*v, float_format.clone()),
                };
                buf.extend_from_slice(rendered.as_bytes());
                let text_len = (buf.len() - start - 4) as i32;
                buf[start..start + 4].copy_from_slice(&text_len.to_be_bytes());
            }
            Value::Numeric(v) => {
                let text = v.render();
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &text);
            }
            Value::PgLsn(v) => {
                let text = render_pg_lsn_text(*v);
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &text);
            }
            Value::Tid(v) => {
                let text = crate::backend::executor::value_io::render_tid_text(v);
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &text);
            }
            Value::Interval(v) => {
                let text = render_interval_text_with_config(*v, &float_format.datetime_config);
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &text);
            }
            Value::Json(v) => {
                pgrust_protocol::pqformat::append_data_row_text_field(buf, v);
            }
            Value::Xml(v) => {
                let text = crate::backend::executor::render_xml_output_text(v);
                pgrust_protocol::pqformat::append_data_row_text_field(buf, text);
            }
            Value::Jsonb(v) => {
                let text = crate::backend::executor::jsonb::render_jsonb_bytes(v).unwrap();
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &text);
            }
            Value::JsonPath(v) => {
                pgrust_protocol::pqformat::append_data_row_text_field(buf, v);
            }
            Value::TsVector(v) => {
                let rendered = crate::backend::executor::render_tsvector_text(v);
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::TsQuery(v) => {
                let rendered = crate::backend::executor::render_tsquery_text(v);
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::Text(v) => {
                pgrust_protocol::pqformat::append_data_row_text_field(buf, v);
            }
            Value::TextRef(_, _) => {
                let s = val.as_text().unwrap();
                pgrust_protocol::pqformat::append_data_row_text_field(buf, s);
            }
            Value::InternalChar(byte) => {
                let rendered = render_internal_char_text(*byte);
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::EnumOid(v) => {
                let rendered = sql_type
                    .and_then(enum_label_type_oid)
                    .and_then(|type_oid| enum_labels.and_then(|labels| labels.get(&(type_oid, *v))))
                    .cloned()
                    .unwrap_or_else(|| v.to_string());
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::Bool(true) => {
                pgrust_protocol::pqformat::append_data_row_field(buf, b"t");
            }
            Value::Bool(false) => {
                pgrust_protocol::pqformat::append_data_row_field(buf, b"f");
            }
            Value::Point(_)
            | Value::Lseg(_)
            | Value::Path(_)
            | Value::Line(_)
            | Value::Box(_)
            | Value::Polygon(_)
            | Value::Circle(_) => {
                let rendered = pgrust_expr::render_geometry_text(val, float_format.clone())
                    .unwrap_or_default();
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::Array(items) => {
                let rendered = if let Some(sql_type) = sql_type.filter(|ty| ty.is_array) {
                    if matches!(sql_type.element_type().kind, SqlTypeKind::Enum)
                        && sql_type.element_type().type_oid != 0
                        && let Some(enum_labels) = enum_labels
                    {
                        let array = ArrayValue::from_1d(items.clone())
                            .with_element_type_oid(sql_type.element_type().type_oid);
                        format_enum_array_value_text(
                            &array,
                            sql_type.element_type().type_oid,
                            enum_labels,
                            &float_format.datetime_config,
                        )
                    } else {
                        let array = builtin_type_oid_for_sql_type(sql_type.element_type()).map(
                            |element_type_oid| {
                                ArrayValue::from_1d(items.clone())
                                    .with_element_type_oid(element_type_oid)
                            },
                        );
                        array
                        .as_ref()
                        .map(|array| {
                            crate::backend::executor::value_io::format_array_value_text_with_config(
                                array,
                                &float_format.datetime_config,
                            )
                        })
                        .unwrap_or_else(|| {
                            crate::backend::executor::value_io::format_array_text_with_config(
                                items,
                                &float_format.datetime_config,
                            )
                        })
                    }
                } else {
                    crate::backend::executor::value_io::format_array_text_with_config(
                        items,
                        &float_format.datetime_config,
                    )
                };
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::PgArray(array) => {
                let rendered = enum_array_type_oid(sql_type, array, enum_labels)
                    .map(|enum_type_oid| {
                        format_enum_array_value_text(
                            array,
                            enum_type_oid,
                            enum_labels.expect("enum labels present"),
                            &float_format.datetime_config,
                        )
                    })
                    .unwrap_or_else(|| {
                        crate::backend::executor::value_io::format_array_value_text_with_config(
                            array,
                            &float_format.datetime_config,
                        )
                    });
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::Record(record) => {
                let rendered = crate::backend::executor::value_io::format_record_text_with_options(
                    record,
                    &float_format,
                );
                pgrust_protocol::pqformat::append_data_row_text_field(buf, &rendered);
            }
            Value::DroppedColumn(_) | Value::WrongTypeColumn { .. } => {
                pgrust_protocol::pqformat::append_data_row_null_field(buf);
            }
            Value::IndirectVarlena(_) => unreachable!("indirect datums are decoded before output"),
        }
    }

    pgrust_protocol::pqformat::send_data_row_body(w, buf)
}

pub(crate) fn format_text_data_value(
    value: &Value,
    column: &QueryColumn,
    float_format: FloatFormatOptions,
    role_names: Option<&HashMap<u32, String>>,
    relation_names: Option<&HashMap<u32, String>>,
    proc_names: Option<&HashMap<u32, String>>,
    namespace_names: Option<&HashMap<u32, String>>,
    type_names: Option<&HashMap<u32, String>>,
    type_catalog: Option<&dyn crate::backend::parser::CatalogLookup>,
) -> Result<Option<String>, ExecError> {
    let mut row = Vec::new();
    let mut buf = Vec::new();
    send_typed_data_row(
        &mut row,
        std::slice::from_ref(value),
        std::slice::from_ref(column),
        &[0],
        &mut buf,
        float_format,
        role_names,
        relation_names,
        proc_names,
        namespace_names,
        type_names,
        type_catalog,
        None,
        None,
    )
    .map_err(|err| ExecError::DetailedError {
        message: format!("could not format COPY data: {err}"),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    })?;
    if row.len() < 11 || row[0] != b'D' {
        return Err(ExecError::DetailedError {
            message: "could not format COPY data".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    }
    let field_count = i16::from_be_bytes([row[5], row[6]]);
    if field_count != 1 {
        return Err(ExecError::DetailedError {
            message: "could not format COPY data".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    }
    let len = i32::from_be_bytes([row[7], row[8], row[9], row[10]]);
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    let start = 11usize;
    let end = start.saturating_add(len);
    if end > row.len() {
        return Err(ExecError::DetailedError {
            message: "could not format COPY data".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    }
    String::from_utf8(row[start..end].to_vec())
        .map(Some)
        .map_err(|err| ExecError::DetailedError {
            message: format!("could not format COPY data: {err}"),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

pub(crate) fn encode_binary_data_row_value(
    value: &Value,
    sql_type: SqlType,
) -> Result<Vec<u8>, ExecError> {
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Int16(v) if matches!(sql_type.kind, SqlTypeKind::Int2) => {
            Ok(v.to_be_bytes().to_vec())
        }
        Value::Int32(v) if matches!(sql_type.kind, SqlTypeKind::Int4 | SqlTypeKind::Tid) => {
            Ok(v.to_be_bytes().to_vec())
        }
        Value::Int64(v) if matches!(sql_type.kind, SqlTypeKind::Int8 | SqlTypeKind::Money) => {
            Ok(v.to_be_bytes().to_vec())
        }
        Value::Xid8(v) if sql_type.type_oid == crate::include::catalog::XID8_TYPE_OID => {
            Ok(v.to_be_bytes().to_vec())
        }
        Value::Int64(v)
            if matches!(
                sql_type.kind,
                SqlTypeKind::Oid
                    | SqlTypeKind::RegProc
                    | SqlTypeKind::RegClass
                    | SqlTypeKind::RegType
                    | SqlTypeKind::RegRole
                    | SqlTypeKind::RegNamespace
                    | SqlTypeKind::RegOper
                    | SqlTypeKind::RegOperator
                    | SqlTypeKind::RegProcedure
                    | SqlTypeKind::RegCollation
                    | SqlTypeKind::Xid
                    | SqlTypeKind::RegConfig
                    | SqlTypeKind::RegDictionary
            ) =>
        {
            let oid = u32::try_from(*v).map_err(|_| ExecError::OidOutOfRange)?;
            Ok(oid.to_be_bytes().to_vec())
        }
        Value::Bool(v) => Ok(vec![u8::from(*v)]),
        Value::Bytea(bytes) => Ok(bytes.clone()),
        Value::Uuid(bytes) if matches!(sql_type.kind, SqlTypeKind::Uuid) => Ok(bytes.to_vec()),
        Value::Inet(value) if matches!(sql_type.kind, SqlTypeKind::Inet) => {
            Ok(encode_binary_network_value(value, false))
        }
        Value::Cidr(value) if matches!(sql_type.kind, SqlTypeKind::Cidr) => {
            Ok(encode_binary_network_value(value, true))
        }
        Value::MacAddr(value) if matches!(sql_type.kind, SqlTypeKind::MacAddr) => {
            Ok(value.to_vec())
        }
        Value::MacAddr8(value) if matches!(sql_type.kind, SqlTypeKind::MacAddr8) => {
            Ok(value.to_vec())
        }
        Value::Text(text)
            if matches!(
                sql_type.kind,
                SqlTypeKind::Text
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Char
                    | SqlTypeKind::Name
                    | SqlTypeKind::PgNodeTree
                    | SqlTypeKind::Json
                    | SqlTypeKind::JsonPath
                    | SqlTypeKind::Xml
            ) =>
        {
            Ok(text.as_bytes().to_vec())
        }
        Value::TextRef(_, _)
            if matches!(
                sql_type.kind,
                SqlTypeKind::Text
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Char
                    | SqlTypeKind::Name
                    | SqlTypeKind::PgNodeTree
                    | SqlTypeKind::Json
                    | SqlTypeKind::JsonPath
                    | SqlTypeKind::Xml
            ) =>
        {
            Ok(value.as_text().unwrap_or_default().as_bytes().to_vec())
        }
        Value::Xml(text) if matches!(sql_type.kind, SqlTypeKind::Xml) => {
            Ok(crate::backend::executor::render_xml_output_text(text)
                .as_bytes()
                .to_vec())
        }
        Value::InternalChar(byte) => Ok(vec![*byte]),
        Value::Float64(v) if matches!(sql_type.kind, SqlTypeKind::Float4) => {
            Ok((*v as f32).to_bits().to_be_bytes().to_vec())
        }
        Value::Float64(v) if matches!(sql_type.kind, SqlTypeKind::Float8) => {
            Ok(v.to_bits().to_be_bytes().to_vec())
        }
        Value::Date(v) => Ok(v.0.to_be_bytes().to_vec()),
        Value::Time(v) => Ok(v.0.to_be_bytes().to_vec()),
        Value::TimeTz(v) => {
            let mut out = Vec::with_capacity(12);
            out.extend_from_slice(&v.time.0.to_be_bytes());
            out.extend_from_slice(&v.offset_seconds.to_be_bytes());
            Ok(out)
        }
        Value::Timestamp(v) => Ok(v.0.to_be_bytes().to_vec()),
        Value::TimestampTz(v) => Ok(v.0.to_be_bytes().to_vec()),
        Value::Interval(v) => {
            let mut out = Vec::with_capacity(16);
            out.extend_from_slice(&v.time_micros.to_be_bytes());
            out.extend_from_slice(&v.days.to_be_bytes());
            out.extend_from_slice(&v.months.to_be_bytes());
            Ok(out)
        }
        Value::Record(record)
            if matches!(sql_type.kind, SqlTypeKind::Record | SqlTypeKind::Composite) =>
        {
            encode_binary_record(record)
        }
        Value::Array(items)
            if sql_type.is_array
                && matches!(sql_type.kind, SqlTypeKind::Record | SqlTypeKind::Composite) =>
        {
            encode_binary_record_array(
                &crate::include::nodes::datum::ArrayValue::from_1d(items.clone()),
                sql_type.element_type(),
            )
        }
        Value::PgArray(array)
            if sql_type.is_array
                && matches!(sql_type.kind, SqlTypeKind::Record | SqlTypeKind::Composite) =>
        {
            encode_binary_record_array(array, sql_type.element_type())
        }
        other => Err(ExecError::Parse(
            crate::backend::parser::ParseError::FeatureNotSupported(format!(
                "binary output for {:?}",
                other.sql_type_hint().unwrap_or(sql_type)
            )),
        )),
    }
}

fn encode_binary_network_value(value: &InetValue, cidr: bool) -> Vec<u8> {
    const PGSQL_AF_INET: u8 = 2;
    const PGSQL_AF_INET6: u8 = 3;

    let mut out = Vec::with_capacity(20);
    match value.addr {
        std::net::IpAddr::V4(addr) => {
            out.push(PGSQL_AF_INET);
            out.push(value.bits);
            out.push(u8::from(cidr));
            out.push(4);
            out.extend_from_slice(&addr.octets());
        }
        std::net::IpAddr::V6(addr) => {
            out.push(PGSQL_AF_INET6);
            out.push(value.bits);
            out.push(u8::from(cidr));
            out.push(16);
            out.extend_from_slice(&addr.octets());
        }
    }
    out
}

fn encode_binary_record(
    record: &crate::include::nodes::datum::RecordValue,
) -> Result<Vec<u8>, ExecError> {
    let mut out = Vec::new();
    pgrust_protocol::pqformat::begin_binary_record_body(&mut out, record.fields.len());
    for (field, value) in record.iter() {
        let field_oid = if !field.sql_type.is_array && field.sql_type.type_oid != 0 {
            field.sql_type.type_oid
        } else {
            pgrust_protocol::pqformat::wire_type_info(&QueryColumn {
                name: field.name.clone(),
                sql_type: field.sql_type,
                wire_type_oid: None,
            })
            .0 as u32
        };
        if matches!(value, Value::Null) {
            pgrust_protocol::pqformat::append_binary_record_field(&mut out, field_oid, None);
            continue;
        }
        let payload = encode_binary_data_row_value(value, field.sql_type)?;
        pgrust_protocol::pqformat::append_binary_record_field(&mut out, field_oid, Some(&payload));
    }
    Ok(out)
}

fn encode_binary_record_array(
    array: &crate::include::nodes::datum::ArrayValue,
    element_sql_type: SqlType,
) -> Result<Vec<u8>, ExecError> {
    let element_oid = if element_sql_type.type_oid != 0 {
        element_sql_type.type_oid
    } else {
        array
            .elements
            .iter()
            .find_map(|value| match value {
                Value::Record(record) => Some(record.type_oid()),
                _ => None,
            })
            .unwrap_or(crate::include::catalog::RECORD_TYPE_OID)
    };
    let mut out = Vec::new();
    pgrust_protocol::pqformat::begin_binary_array_body(
        &mut out,
        array.dimensions.len(),
        array.elements.iter().any(|v| matches!(v, Value::Null)),
        element_oid,
    );
    for dim in &array.dimensions {
        pgrust_protocol::pqformat::append_binary_array_dimension(
            &mut out,
            dim.length,
            dim.lower_bound,
        );
    }
    for value in &array.elements {
        match value {
            Value::Null => pgrust_protocol::pqformat::append_binary_array_element(&mut out, None),
            Value::Record(record) => {
                let payload = encode_binary_record(record)?;
                pgrust_protocol::pqformat::append_binary_array_element(&mut out, Some(&payload));
            }
            other => {
                return Err(ExecError::Parse(
                    crate::backend::parser::ParseError::FeatureNotSupported(format!(
                        "binary composite array element {:?}",
                        other.sql_type_hint()
                    )),
                ));
            }
        }
    }
    Ok(out)
}

pub fn format_bytea_text(bytes: &[u8], output: ByteaOutputFormat) -> String {
    pgrust_expr::libpq::pqformat::format_bytea_text(bytes, output)
}

pub(crate) fn send_command_complete(w: &mut impl Write, tag: &str) -> io::Result<()> {
    pgrust_protocol::pqformat::send_command_complete(w, tag)
}

pub(crate) fn send_parse_complete(w: &mut impl Write) -> io::Result<()> {
    pgrust_protocol::pqformat::send_parse_complete(w)
}

pub(crate) fn send_bind_complete(w: &mut impl Write) -> io::Result<()> {
    pgrust_protocol::pqformat::send_bind_complete(w)
}

pub(crate) fn send_portal_suspended(w: &mut impl Write) -> io::Result<()> {
    pgrust_protocol::pqformat::send_portal_suspended(w)
}

pub(crate) fn send_close_complete(w: &mut impl Write) -> io::Result<()> {
    pgrust_protocol::pqformat::send_close_complete(w)
}

pub(crate) fn send_no_data(w: &mut impl Write) -> io::Result<()> {
    pgrust_protocol::pqformat::send_no_data(w)
}

pub(crate) fn send_parameter_description(w: &mut impl Write, type_oids: &[i32]) -> io::Result<()> {
    pgrust_protocol::pqformat::send_parameter_description(w, type_oids)
}

pub(crate) fn send_copy_in_response(w: &mut impl Write) -> io::Result<()> {
    pgrust_protocol::pqformat::send_copy_in_response(w)
}

pub(crate) fn send_copy_out_response(
    w: &mut impl Write,
    format: CopyFormat,
    column_count: usize,
) -> io::Result<()> {
    pgrust_protocol::pqformat::send_copy_out_response(w, format, column_count)
}

pub(crate) fn send_copy_data(w: &mut impl Write, data: &[u8]) -> io::Result<()> {
    pgrust_protocol::pqformat::send_copy_data(w, data)
}

pub(crate) fn send_copy_done(w: &mut impl Write) -> io::Result<()> {
    pgrust_protocol::pqformat::send_copy_done(w)
}

pub(crate) fn send_notification_response(
    w: &mut impl Write,
    sender_pid: i32,
    channel: &str,
    payload: &str,
) -> io::Result<()> {
    pgrust_protocol::pqformat::send_notification_response(w, sender_pid, channel, payload)
}

pub(crate) fn send_empty_query(w: &mut impl Write) -> io::Result<()> {
    pgrust_protocol::pqformat::send_empty_query(w)
}

pub(crate) fn send_error(
    w: &mut impl Write,
    sqlstate: &str,
    message: &str,
    detail: Option<&str>,
    hint: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    pgrust_protocol::pqformat::send_error(w, sqlstate, message, detail, hint, position)
}

pub(crate) fn send_error_with_hint(
    w: &mut impl Write,
    sqlstate: &str,
    message: &str,
    hint: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    pgrust_protocol::pqformat::send_error_with_hint(w, sqlstate, message, hint, position)
}

pub(crate) fn send_error_with_fields(
    w: &mut impl Write,
    sqlstate: &str,
    message: &str,
    detail: Option<&str>,
    hint: Option<&str>,
    context: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    pgrust_protocol::pqformat::send_error_with_fields(
        w, sqlstate, message, detail, hint, context, position,
    )
}

pub(crate) fn send_error_with_internal_fields(
    w: &mut impl Write,
    sqlstate: &str,
    message: &str,
    detail: Option<&str>,
    hint: Option<&str>,
    context: Option<&str>,
    position: Option<usize>,
    internal_query: Option<&str>,
    internal_position: Option<usize>,
) -> io::Result<()> {
    pgrust_protocol::pqformat::send_error_with_internal_fields(
        w,
        sqlstate,
        message,
        detail,
        hint,
        context,
        position,
        internal_query,
        internal_position,
    )
}

pub(crate) fn send_notice(
    w: &mut impl Write,
    message: &str,
    detail: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    pgrust_protocol::pqformat::send_notice(w, message, detail, position)
}

pub(crate) fn send_notice_with_severity(
    w: &mut impl Write,
    severity: &str,
    sqlstate: &str,
    message: &str,
    detail: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    pgrust_protocol::pqformat::send_notice_with_severity(
        w, severity, sqlstate, message, detail, position,
    )
}

pub(crate) fn send_notice_with_fields(
    w: &mut impl Write,
    severity: &str,
    sqlstate: &str,
    message: &str,
    detail: Option<&str>,
    hint: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    pgrust_protocol::pqformat::send_notice_with_fields(
        w, severity, sqlstate, message, detail, hint, position,
    )
}

pub(crate) fn send_notice_with_hint(
    w: &mut impl Write,
    severity: &str,
    sqlstate: &str,
    message: &str,
    hint: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    pgrust_protocol::pqformat::send_notice_with_hint(w, severity, sqlstate, message, hint, position)
}

pub(crate) fn format_float8_text(value: f64, options: FloatFormatOptions) -> String {
    pgrust_expr::libpq::pqformat::format_float8_text(value, options)
}

pub(crate) fn format_float4_text(value: f64, options: FloatFormatOptions) -> String {
    pgrust_expr::libpq::pqformat::format_float4_text(value, options)
}

#[cfg(test)]
mod tests {
    use super::{
        FloatFormatOptions, format_bytea_text, format_exec_error, format_exec_error_hint,
        format_float4_text, format_float8_text, send_error_with_fields, send_typed_data_row,
    };
    use crate::backend::executor::{ExecError, QueryColumn, Value};
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::include::nodes::datum::GeoPoint;
    use crate::pgrust::session::ByteaOutputFormat;
    use std::collections::HashMap;

    #[test]
    fn large_float8_values_render_in_scientific_notation() {
        assert_eq!(
            format_float8_text(4_567_890_123_456_789.0, FloatFormatOptions::default()),
            "4.567890123456789e+15"
        );
        assert_eq!(
            format_float8_text(-4_567_890_123_456_789.0, FloatFormatOptions::default()),
            "-4.567890123456789e+15"
        );
        assert_eq!(
            format_float8_text(123.0, FloatFormatOptions::default()),
            "123"
        );
    }

    #[test]
    fn large_float4_values_render_in_scientific_notation() {
        assert_eq!(
            format_float4_text(4_567_890_123_456_789.0, FloatFormatOptions::default()),
            "4.56789e+15"
        );
        assert_eq!(
            format_float4_text(123.0, FloatFormatOptions::default()),
            "123"
        );
    }

    #[test]
    fn float_special_values_use_postgres_spelling() {
        assert_eq!(
            format_float8_text(f64::NAN, FloatFormatOptions::default()),
            "NaN"
        );
        assert_eq!(
            format_float8_text(f64::INFINITY, FloatFormatOptions::default()),
            "Infinity"
        );
        assert_eq!(
            format_float8_text(f64::NEG_INFINITY, FloatFormatOptions::default()),
            "-Infinity"
        );
        assert_eq!(
            format_float4_text(f64::NAN, FloatFormatOptions::default()),
            "NaN"
        );
        assert_eq!(
            format_float4_text(f64::INFINITY, FloatFormatOptions::default()),
            "Infinity"
        );
        assert_eq!(
            format_float4_text(f64::NEG_INFINITY, FloatFormatOptions::default()),
            "-Infinity"
        );
    }

    #[test]
    fn operator_type_mismatch_uses_postgres_message() {
        let err = ExecError::TypeMismatch {
            op: "+",
            left: Value::Point(GeoPoint { x: 0.0, y: 0.0 }),
            right: Value::Int32(1),
        };

        assert_eq!(
            format_exec_error(&err),
            "operator does not exist: point + integer"
        );
        assert_eq!(
            format_exec_error_hint(&err).as_deref(),
            Some(
                "No operator matches the given name and argument types. You might need to add explicit type casts."
            )
        );
    }

    #[test]
    fn extra_float_digits_zero_uses_rounded_general_format() {
        let options = FloatFormatOptions {
            extra_float_digits: 0,
            bytea_output: ByteaOutputFormat::Hex,
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        };
        assert_eq!(
            format_float8_text(31.690692639953454, options.clone()),
            "31.6906926399535"
        );
        assert_eq!(
            format_float8_text(1004.3000000000004, options.clone()),
            "1004.3"
        );
        assert_eq!(
            format_float4_text(1.2345679402097818e20, options),
            "1.23457e+20"
        );
    }

    #[test]
    fn float4_default_format_uses_postgres_scientific_thresholds() {
        assert_eq!(
            format_float4_text(1_000_000.0, FloatFormatOptions::default()),
            "1e+06"
        );
        assert_eq!(
            format_float4_text(0.0000001, FloatFormatOptions::default()),
            "1e-07"
        );
        assert_eq!(
            format_float4_text(0.0001, FloatFormatOptions::default()),
            "0.0001"
        );
    }

    #[test]
    fn shortest_format_preserves_negative_zero() {
        assert_eq!(
            format_float8_text(-0.0, FloatFormatOptions::default()),
            "-0"
        );
        assert_eq!(
            format_float4_text(-0.0, FloatFormatOptions::default()),
            "-0"
        );
    }

    #[test]
    fn bytea_text_output_supports_hex_and_escape() {
        assert_eq!(
            format_bytea_text(&[0xde, 0xad, 0xbe, 0xef], ByteaOutputFormat::Hex),
            "\\xdeadbeef"
        );
        assert_eq!(
            format_bytea_text(&[b'a', b'\\', 0, 0xff], ByteaOutputFormat::Escape),
            "a\\\\\\000\\377"
        );
    }

    #[test]
    fn shortest_format_avoids_midpoint_roundtrip_values() {
        let float8_cases = [
            (0x44b5_2d02_c7e1_4af6_u64, "9.999999999999999e+22"),
            (0x4350_0000_0000_0002_u64, "1.8014398509481992e+16"),
        ];
        for (bits, expected) in float8_cases {
            assert_eq!(
                format_float8_text(f64::from_bits(bits), FloatFormatOptions::default()),
                expected
            );
        }

        let float4_cases = [
            (0x4c00_0004_u32, "3.3554448e+07"),
            (0x5006_1c46_u32, "8.999999e+09"),
        ];
        for (bits, expected) in float4_cases {
            assert_eq!(
                format_float4_text(f32::from_bits(bits) as f64, FloatFormatOptions::default()),
                expected
            );
        }
    }

    #[test]
    fn error_response_can_include_context_field() {
        let mut out = Vec::new();
        send_error_with_fields(
            &mut out,
            "22P02",
            "invalid input syntax for type json",
            Some("The input string ended unexpectedly."),
            None,
            Some("JSON data, line 1: {\"a\":true"),
            Some(8),
        )
        .unwrap();

        assert!(
            out.windows("WJSON data, line 1: {\"a\":true\0".len())
                .any(|window| { window == b"WJSON data, line 1: {\"a\":true\0" })
        );
    }

    #[test]
    fn format_exec_error_renders_cardinality_violation_message() {
        let err = ExecError::CardinalityViolation {
            message: "ON CONFLICT DO UPDATE command cannot affect row a second time".into(),
            hint: Some(
                "Ensure that no rows proposed for insertion within the same command have duplicate constrained values.".into(),
            ),
        };
        assert_eq!(
            format_exec_error(&err),
            "ON CONFLICT DO UPDATE command cannot affect row a second time"
        );
        assert_eq!(
            format_exec_error_hint(&err).as_deref(),
            Some(
                "Ensure that no rows proposed for insertion within the same command have duplicate constrained values."
            )
        );
    }

    #[test]
    fn format_exec_error_renders_generate_series_step_infinity() {
        let err = ExecError::GenerateSeriesInvalidArg("step size", "infinity");

        assert_eq!(format_exec_error(&err), "step size cannot be infinity");
    }

    #[test]
    fn typed_data_row_renders_regrole_with_role_name() {
        let mut out = Vec::new();
        let mut row_buf = Vec::new();
        let mut role_names = HashMap::new();
        role_names.insert(42, "app_role".to_string());

        send_typed_data_row(
            &mut out,
            &[Value::Int64(42)],
            &[QueryColumn {
                name: "member".into(),
                sql_type: SqlType::new(SqlTypeKind::RegRole),
                wire_type_oid: None,
            }],
            &[],
            &mut row_buf,
            FloatFormatOptions::default(),
            Some(&role_names),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        assert!(
            out.windows("app_role".len())
                .any(|window| window == b"app_role")
        );
    }

    #[test]
    fn typed_data_row_renders_regprocedure_with_proc_name() {
        let mut out = Vec::new();
        let mut row_buf = Vec::new();
        let mut proc_names = HashMap::new();
        let mut proc_signatures = HashMap::new();
        proc_names.insert(6403, "pg_rust_test_fdw_handler".to_string());
        proc_signatures.insert(6403, "pg_rust_test_fdw_handler(internal)".to_string());

        send_typed_data_row(
            &mut out,
            &[Value::Int64(6403)],
            &[QueryColumn {
                name: "fdwhandler".into(),
                sql_type: SqlType::new(SqlTypeKind::RegProcedure),
                wire_type_oid: None,
            }],
            &[],
            &mut row_buf,
            FloatFormatOptions::default(),
            None,
            None,
            Some(&proc_names),
            None,
            None,
            None,
            None,
            Some(&proc_signatures),
        )
        .unwrap();

        assert!(
            out.windows("pg_rust_test_fdw_handler(internal)".len())
                .any(|window| window == b"pg_rust_test_fdw_handler(internal)")
        );
    }

    #[test]
    fn typed_data_row_renders_zero_regprocedure_as_dash() {
        let mut out = Vec::new();
        let mut row_buf = Vec::new();

        send_typed_data_row(
            &mut out,
            &[Value::Int64(0)],
            &[QueryColumn {
                name: "fdwhandler".into(),
                sql_type: SqlType::new(SqlTypeKind::RegProcedure),
                wire_type_oid: None,
            }],
            &[],
            &mut row_buf,
            FloatFormatOptions::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        assert!(out.windows(1).any(|window| window == b"-"));
    }

    #[test]
    fn typed_data_row_renders_regclass_with_relation_name() {
        let mut out = Vec::new();
        let mut row_buf = Vec::new();
        let mut relation_names = HashMap::new();
        relation_names.insert(1259, "pg_class".to_string());

        send_typed_data_row(
            &mut out,
            &[Value::Int64(1259)],
            &[QueryColumn {
                name: "relid".into(),
                sql_type: SqlType::new(SqlTypeKind::RegClass),
                wire_type_oid: None,
            }],
            &[],
            &mut row_buf,
            FloatFormatOptions::default(),
            None,
            Some(&relation_names),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        assert!(
            out.windows("pg_class".len())
                .any(|window| window == b"pg_class")
        );
    }

    #[test]
    fn typed_data_row_renders_regnamespace_with_schema_name() {
        let mut out = Vec::new();
        let mut row_buf = Vec::new();
        let mut namespace_names = HashMap::new();
        namespace_names.insert(2200, "public".to_string());

        send_typed_data_row(
            &mut out,
            &[Value::Int64(2200)],
            &[QueryColumn {
                name: "nsp".into(),
                sql_type: SqlType::new(SqlTypeKind::RegNamespace),
                wire_type_oid: None,
            }],
            &[],
            &mut row_buf,
            FloatFormatOptions::default(),
            None,
            None,
            None,
            Some(&namespace_names),
            None,
            None,
            None,
            None,
        )
        .unwrap();

        assert!(
            out.windows("public".len())
                .any(|window| window == b"public")
        );
    }

    #[test]
    fn typed_data_row_renders_regtype_with_type_name() {
        let mut out = Vec::new();
        let mut row_buf = Vec::new();
        let mut type_names = HashMap::new();
        type_names.insert(80_001, "domainint4".to_string());

        send_typed_data_row(
            &mut out,
            &[Value::Int64(80_001)],
            &[QueryColumn {
                name: "pg_typeof".into(),
                sql_type: SqlType::new(SqlTypeKind::RegType),
                wire_type_oid: None,
            }],
            &[],
            &mut row_buf,
            FloatFormatOptions::default(),
            None,
            None,
            None,
            None,
            Some(&type_names),
            None,
            None,
            None,
        )
        .unwrap();

        assert!(
            out.windows("domainint4".len())
                .any(|window| window == b"domainint4")
        );
    }

    #[test]
    fn typed_data_row_renders_interval_arrays_with_interval_text() {
        let mut out = Vec::new();
        let mut row_buf = Vec::new();

        send_typed_data_row(
            &mut out,
            &[Value::Array(vec![
                Value::Text("00:00:00".into()),
                Value::Text("01:42:20".into()),
            ])],
            &[QueryColumn {
                name: "intervals".into(),
                sql_type: SqlType::array_of(SqlType::new(SqlTypeKind::Interval)),
                wire_type_oid: None,
            }],
            &[],
            &mut row_buf,
            FloatFormatOptions::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        assert!(
            out.windows("{00:00:00,01:42:20}".len())
                .any(|window| window == b"{00:00:00,01:42:20}")
        );
    }

    #[test]
    fn typed_data_row_renders_enum_domain_with_base_label() {
        let mut out = Vec::new();
        let mut row_buf = Vec::new();
        let mut enum_labels = HashMap::new();
        enum_labels.insert((7001, 1313634107), "red".to_string());

        send_typed_data_row(
            &mut out,
            &[Value::EnumOid(1313634107)],
            &[QueryColumn {
                name: "rgb".into(),
                sql_type: SqlType::new(SqlTypeKind::Enum).with_identity(8001, 7001),
                wire_type_oid: None,
            }],
            &[],
            &mut row_buf,
            FloatFormatOptions::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(&enum_labels),
            None,
        )
        .unwrap();

        assert!(out.windows(3).any(|window| window == b"red"));
        assert!(
            !out.windows("1313634107".len())
                .any(|window| window == b"1313634107")
        );
    }

    #[test]
    fn macaddr_protocol_metadata_and_binary_output_use_postgres_oids() {
        assert_eq!(
            pgrust_protocol::pqformat::wire_type_info(&QueryColumn {
                name: "m".into(),
                sql_type: SqlType::new(SqlTypeKind::MacAddr),
                wire_type_oid: None,
            }),
            (crate::include::catalog::MACADDR_TYPE_OID as i32, 6, -1)
        );
        assert_eq!(
            pgrust_protocol::pqformat::wire_type_info(&QueryColumn {
                name: "m8".into(),
                sql_type: SqlType::array_of(SqlType::new(SqlTypeKind::MacAddr8)),
                wire_type_oid: None,
            }),
            (
                crate::include::catalog::MACADDR8_ARRAY_TYPE_OID as i32,
                -1,
                -1
            )
        );

        let mut out = Vec::new();
        let mut row_buf = Vec::new();
        send_typed_data_row(
            &mut out,
            &[
                Value::MacAddr([0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]),
                Value::MacAddr8([0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05]),
            ],
            &[
                QueryColumn {
                    name: "m".into(),
                    sql_type: SqlType::new(SqlTypeKind::MacAddr),
                    wire_type_oid: None,
                },
                QueryColumn {
                    name: "m8".into(),
                    sql_type: SqlType::new(SqlTypeKind::MacAddr8),
                    wire_type_oid: None,
                },
            ],
            &[1, 1],
            &mut row_buf,
            FloatFormatOptions::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        assert!(
            out.windows(10)
                .any(|window| { window == [0, 0, 0, 6, 0x08, 0x00, 0x2b, 0x01, 0x02, 0x03] })
        );
        assert!(out.windows(12).any(|window| {
            window == [0, 0, 0, 8, 0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05]
        }));
    }
}
