use std::collections::HashMap;
use std::io::{self, Write};
use std::str::FromStr;

use crate::backend::access::heap::heapam::HeapError;
use crate::backend::executor::value_io::builtin_type_oid_for_sql_type;
use crate::backend::executor::{
    ArrayValue, ExecError, QueryColumn, Value, geometry_input_error_message,
    render_datetime_value_text_with_config, render_geometry_text, render_internal_char_text,
    render_interval_text, render_macaddr_text, render_macaddr8_text,
    render_multirange_text_with_config, render_pg_lsn_text, render_range_text_with_config,
};
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::statistics::{
    render_pg_dependencies_text, render_pg_mcv_list_text, render_pg_ndistinct_text,
};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::include::access::htup::TupleError;
use crate::include::catalog::{
    PG_DEPENDENCIES_TYPE_OID, PG_MCV_LIST_TYPE_OID, PG_NDISTINCT_TYPE_OID, TRIGGER_TYPE_OID,
    builtin_type_rows, range_type_ref_for_sql_type,
};
use crate::include::nodes::datum::InetValue;
use crate::include::nodes::parsenodes::CopyFormat;
use crate::pgrust::session::ByteaOutputFormat;
use num_bigint::BigInt;
use num_traits::One;

#[derive(Debug, Clone)]
pub(crate) struct FloatFormatOptions {
    pub(crate) extra_float_digits: i32,
    pub(crate) bytea_output: ByteaOutputFormat,
    pub(crate) datetime_config: DateTimeConfig,
}

impl Default for FloatFormatOptions {
    fn default() -> Self {
        Self {
            extra_float_digits: 1,
            bytea_output: ByteaOutputFormat::Hex,
            datetime_config: DateTimeConfig::default(),
        }
    }
}

pub(crate) fn format_exec_error(e: &ExecError) -> String {
    match e {
        ExecError::WithContext { source, .. } => format_exec_error(source),
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
        ExecError::Parse(p) => p.to_string(),
        ExecError::Regex(err) => err.message.clone(),
        ExecError::JsonInput { message, .. } => message.clone(),
        ExecError::XmlInput { message, .. } => message.clone(),
        ExecError::DetailedError { message, .. } => message.clone(),
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
        ExecError::InvalidGeometryInput { ty, value } => geometry_input_error_message(ty, value)
            .unwrap_or_else(|| format!("invalid input syntax for type {ty}: \"{value}\"")),
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
        ExecError::Heap(HeapError::Tuple(TupleError::Oversized { size, max_size })) => {
            format!("row is too big: size {size}, maximum size {max_size}")
        }
        other => format!("{other:?}"),
    }
}

pub(crate) fn format_exec_error_hint(e: &ExecError) -> Option<String> {
    match e {
        ExecError::WithContext { source, .. } => format_exec_error_hint(source),
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
        ExecError::RaiseException(message)
            if message.starts_with("unrecognized format() type specifier")
                || message == "unterminated format() type specifier" =>
        {
            Some("For a single \"%\" use \"%%\".".into())
        }
        ExecError::DetailedError { hint, .. } => hint.clone(),
        ExecError::CardinalityViolation { hint, .. } => hint.clone(),
        _ => None,
    }
}

fn enum_array_type_oid(
    sql_type: Option<SqlType>,
    array: &ArrayValue,
    enum_labels: Option<&HashMap<(u32, u32), String>>,
) -> Option<u32> {
    let labels = enum_labels?;
    if let Some(sql_type) = sql_type
        && sql_type.is_array
        && matches!(sql_type.element_type().kind, SqlTypeKind::Enum)
        && sql_type.element_type().type_oid != 0
    {
        return Some(sql_type.element_type().type_oid);
    }
    array.element_type_oid.filter(|type_oid| {
        labels
            .keys()
            .any(|(enum_type_oid, _)| enum_type_oid == type_oid)
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
    let mut words = sql
        .split_ascii_whitespace()
        .map(|word| word.to_ascii_uppercase());
    let first_word = words.next().unwrap_or_default();
    let second_word = words.next().unwrap_or_default();
    match (first_word.as_str(), second_word.as_str()) {
        ("INSERT", _) => format!("INSERT 0 {affected}"),
        ("UPDATE", _) => format!("UPDATE {affected}"),
        ("DELETE", _) => format!("DELETE {affected}"),
        ("CREATE", "TRIGGER") => "CREATE TRIGGER".to_string(),
        ("CREATE", "TYPE") => "CREATE TYPE".to_string(),
        ("CREATE", _) => "CREATE TABLE".to_string(),
        ("DROP", "TRIGGER") => "DROP TRIGGER".to_string(),
        ("DROP", "TYPE") => "DROP TYPE".to_string(),
        ("DROP", _) => "DROP TABLE".to_string(),
        ("ANALYZE", _) => "ANALYZE".to_string(),
        ("COMMENT", _) => "COMMENT".to_string(),
        ("CHECKPOINT", _) => "CHECKPOINT".to_string(),
        ("COPY", _) => format!("COPY {affected}"),
        ("DO", _) => "DO".to_string(),
        ("LISTEN", _) => "LISTEN".to_string(),
        ("NOTIFY", _) => "NOTIFY".to_string(),
        ("UNLISTEN", _) => "UNLISTEN".to_string(),
        ("VACUUM", _) => "VACUUM".to_string(),
        ("SET", _) => "SET".to_string(),
        ("RESET", _) => "RESET".to_string(),
        ("BEGIN", _) | ("START", _) => "BEGIN".to_string(),
        ("COMMIT", _) | ("END", _) => "COMMIT".to_string(),
        ("ROLLBACK", _) => "ROLLBACK".to_string(),
        _ => format!("SELECT {affected}"),
    }
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
    enum_labels: Option<&HashMap<(u32, u32), String>>,
) -> io::Result<()> {
    send_row_description(stream, columns)?;
    let mut row_buf = Vec::new();
    for row in rows {
        send_typed_data_row(
            stream,
            row,
            columns,
            &[],
            &mut row_buf,
            float_format.clone(),
            role_names,
            relation_names,
            proc_names,
            namespace_names,
            enum_labels,
        )?;
    }
    send_command_complete(stream, tag)
}

pub(crate) fn send_auth_ok(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'R'])?;
    w.write_all(&8_i32.to_be_bytes())?;
    w.write_all(&0_i32.to_be_bytes())?;
    Ok(())
}

pub(crate) fn send_parameter_status(w: &mut impl Write, name: &str, value: &str) -> io::Result<()> {
    let len = 4 + name.len() + 1 + value.len() + 1;
    w.write_all(&[b'S'])?;
    w.write_all(&(len as i32).to_be_bytes())?;
    w.write_all(name.as_bytes())?;
    w.write_all(&[0])?;
    w.write_all(value.as_bytes())?;
    w.write_all(&[0])?;
    Ok(())
}

pub(crate) fn send_backend_key_data(w: &mut impl Write, pid: i32, key: i32) -> io::Result<()> {
    w.write_all(&[b'K'])?;
    w.write_all(&12_i32.to_be_bytes())?;
    w.write_all(&pid.to_be_bytes())?;
    w.write_all(&key.to_be_bytes())?;
    Ok(())
}

pub(crate) fn send_ready_for_query(w: &mut impl Write, status: u8) -> io::Result<()> {
    w.write_all(&[b'Z'])?;
    w.write_all(&5_i32.to_be_bytes())?;
    w.write_all(&[status])?;
    Ok(())
}

pub(crate) fn send_row_description(w: &mut impl Write, columns: &[QueryColumn]) -> io::Result<()> {
    send_row_description_with_formats(w, columns, &[])
}

pub(crate) fn send_row_description_with_formats(
    w: &mut impl Write,
    columns: &[QueryColumn],
    result_formats: &[i16],
) -> io::Result<()> {
    let mut body = Vec::new();
    body.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    for (index, col) in columns.iter().enumerate() {
        body.extend_from_slice(col.name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0_i32.to_be_bytes());
        body.extend_from_slice(&0_i16.to_be_bytes());
        let (oid, typlen, typmod) = wire_type_info(col);
        body.extend_from_slice(&oid.to_be_bytes());
        body.extend_from_slice(&typlen.to_be_bytes());
        body.extend_from_slice(&typmod.to_be_bytes());
        body.extend_from_slice(&result_format_code(result_formats, index).to_be_bytes());
    }

    w.write_all(&[b'T'])?;
    w.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
    w.write_all(&body)?;
    Ok(())
}

fn result_format_code(result_formats: &[i16], index: usize) -> i16 {
    match result_formats {
        [] => 0,
        [single] => *single,
        many => many.get(index).copied().unwrap_or(0),
    }
}

pub(crate) fn validate_binary_result_formats(
    rows: &[Vec<Value>],
    columns: &[QueryColumn],
    result_formats: &[i16],
) -> Result<(), ExecError> {
    for (index, column) in columns.iter().enumerate() {
        match result_format_code(result_formats, index) {
            0 => {}
            1 => {
                validate_binary_output_type(column.sql_type)?;
                for row in rows {
                    let Some(value) = row.get(index) else {
                        continue;
                    };
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    let _ = encode_binary_data_row_value(value, column.sql_type)?;
                }
            }
            code => {
                return Err(ExecError::Parse(
                    crate::backend::parser::ParseError::FeatureNotSupported(format!(
                        "result format code {code}"
                    )),
                ));
            }
        }
    }
    Ok(())
}

fn validate_binary_output_type(sql_type: SqlType) -> Result<(), ExecError> {
    let supported = if sql_type.is_array {
        matches!(sql_type.kind, SqlTypeKind::Record | SqlTypeKind::Composite)
    } else {
        matches!(
            sql_type.kind,
            SqlTypeKind::Int2
                | SqlTypeKind::Int4
                | SqlTypeKind::Int8
                | SqlTypeKind::Tid
                | SqlTypeKind::Oid
                | SqlTypeKind::Xid
                | SqlTypeKind::Money
                | SqlTypeKind::RegConfig
                | SqlTypeKind::RegDictionary
                | SqlTypeKind::Bool
                | SqlTypeKind::Bytea
                | SqlTypeKind::Inet
                | SqlTypeKind::Cidr
                | SqlTypeKind::MacAddr
                | SqlTypeKind::MacAddr8
                | SqlTypeKind::Text
                | SqlTypeKind::Varchar
                | SqlTypeKind::Char
                | SqlTypeKind::Name
                | SqlTypeKind::PgNodeTree
                | SqlTypeKind::Json
                | SqlTypeKind::JsonPath
                | SqlTypeKind::Xml
                | SqlTypeKind::InternalChar
                | SqlTypeKind::Float4
                | SqlTypeKind::Float8
                | SqlTypeKind::Date
                | SqlTypeKind::Time
                | SqlTypeKind::TimeTz
                | SqlTypeKind::Timestamp
                | SqlTypeKind::TimestampTz
                | SqlTypeKind::Record
                | SqlTypeKind::Composite
                | SqlTypeKind::Multirange
        )
    };
    if supported {
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

fn wire_type_info(col: &QueryColumn) -> (i32, i16, i32) {
    if col.sql_type.is_array
        && let Some(oid) = col.wire_type_oid
    {
        return (oid as i32, -1, -1);
    }
    if col.sql_type.is_array {
        if col.sql_type.type_oid != 0 && matches!(col.sql_type.kind, SqlTypeKind::Range) {
            return (col.sql_type.type_oid as i32, -1, -1);
        }
        if let Some(range_type) = range_type_ref_for_sql_type(col.sql_type)
            && let Some(array_row) = builtin_type_rows()
                .into_iter()
                .find(|row| row.typelem == range_type.type_oid())
        {
            return (array_row.oid as i32, -1, -1);
        }
        if col.sql_type.type_oid != 0 && matches!(col.sql_type.kind, SqlTypeKind::Multirange) {
            return (col.sql_type.type_oid as i32, -1, -1);
        }
        if let Some(multirange_type) =
            crate::include::catalog::multirange_type_ref_for_sql_type(col.sql_type)
            && let Some(array_row) = builtin_type_rows()
                .into_iter()
                .find(|row| row.typelem == multirange_type.type_oid())
        {
            return (array_row.oid as i32, -1, -1);
        }
    }
    if matches!(
        col.sql_type.kind,
        SqlTypeKind::Record | SqlTypeKind::Composite
    ) && let Some(oid) = col.wire_type_oid
    {
        return (oid as i32, -1, col.sql_type.typmod);
    }
    if let Some(range_type) = range_type_ref_for_sql_type(col.sql_type) {
        return (range_type.type_oid() as i32, -1, col.sql_type.typmod);
    }
    if let Some(multirange_type) =
        crate::include::catalog::multirange_type_ref_for_sql_type(col.sql_type)
    {
        return (multirange_type.type_oid() as i32, -1, col.sql_type.typmod);
    }
    if !col.sql_type.is_array && col.sql_type.type_oid != 0 {
        return (col.sql_type.type_oid as i32, -1, col.sql_type.typmod);
    }
    if col.sql_type.is_array {
        let oid = match col.sql_type.kind {
            SqlTypeKind::Int2 => 1005,
            SqlTypeKind::Int4 => 1007,
            SqlTypeKind::Int8 => 1016,
            SqlTypeKind::PgLsn => crate::include::catalog::PG_LSN_ARRAY_TYPE_OID as i32,
            SqlTypeKind::Range => col.sql_type.type_oid as i32,
            SqlTypeKind::Multirange => col.sql_type.type_oid as i32,
            SqlTypeKind::Enum => col.sql_type.type_oid as i32,
            SqlTypeKind::Internal => unreachable!("internal arrays are unsupported"),
            SqlTypeKind::Void => unreachable!("void arrays are unsupported"),
            SqlTypeKind::FdwHandler => unreachable!("fdw_handler arrays are unsupported"),
            SqlTypeKind::Oid => 1028,
            SqlTypeKind::RegProc => crate::include::catalog::REGPROC_ARRAY_TYPE_OID as i32,
            SqlTypeKind::RegClass => crate::include::catalog::REGCLASS_ARRAY_TYPE_OID as i32,
            SqlTypeKind::RegType => unreachable!("regtype arrays are unsupported"),
            SqlTypeKind::RegRole => unreachable!("regrole arrays are unsupported"),
            SqlTypeKind::RegNamespace => {
                crate::include::catalog::REGNAMESPACE_ARRAY_TYPE_OID as i32
            }
            SqlTypeKind::RegOper => crate::include::catalog::REGOPER_ARRAY_TYPE_OID as i32,
            SqlTypeKind::RegOperator => crate::include::catalog::REGOPERATOR_ARRAY_TYPE_OID as i32,
            SqlTypeKind::RegProcedure => {
                crate::include::catalog::REGPROCEDURE_ARRAY_TYPE_OID as i32
            }
            SqlTypeKind::RegCollation => {
                crate::include::catalog::REGCOLLATION_ARRAY_TYPE_OID as i32
            }
            SqlTypeKind::Tid => 1010,
            SqlTypeKind::Xid => 1011,
            SqlTypeKind::Bit => 1561,
            SqlTypeKind::VarBit => 1563,
            SqlTypeKind::Bytea => 1001,
            SqlTypeKind::Uuid => crate::include::catalog::UUID_ARRAY_TYPE_OID as i32,
            SqlTypeKind::Inet => crate::include::catalog::INET_ARRAY_TYPE_OID as i32,
            SqlTypeKind::Cidr => crate::include::catalog::CIDR_ARRAY_TYPE_OID as i32,
            SqlTypeKind::MacAddr => crate::include::catalog::MACADDR_ARRAY_TYPE_OID as i32,
            SqlTypeKind::MacAddr8 => crate::include::catalog::MACADDR8_ARRAY_TYPE_OID as i32,
            SqlTypeKind::Float4 => 1021,
            SqlTypeKind::Float8 => 1022,
            SqlTypeKind::Money => 791,
            SqlTypeKind::Numeric => 1231,
            SqlTypeKind::Json => 199,
            SqlTypeKind::Jsonb => 3807,
            SqlTypeKind::JsonPath => 4073,
            SqlTypeKind::Xml => 143,
            SqlTypeKind::Date => 1182,
            SqlTypeKind::Time => 1183,
            SqlTypeKind::TimeTz => 1270,
            SqlTypeKind::Interval => 1187,
            SqlTypeKind::Point
            | SqlTypeKind::Lseg
            | SqlTypeKind::Path
            | SqlTypeKind::Box
            | SqlTypeKind::Polygon
            | SqlTypeKind::Line
            | SqlTypeKind::Circle => unreachable!("geometry arrays are unsupported"),
            SqlTypeKind::TsVector => 3643,
            SqlTypeKind::TsQuery => 3645,
            SqlTypeKind::RegConfig => 3735,
            SqlTypeKind::RegDictionary => 3770,
            SqlTypeKind::InternalChar => 1002,
            SqlTypeKind::Name => 1003,
            SqlTypeKind::Text
            | SqlTypeKind::Int2Vector
            | SqlTypeKind::OidVector
            | SqlTypeKind::Timestamp
            | SqlTypeKind::TimestampTz
            | SqlTypeKind::Char
            | SqlTypeKind::PgNodeTree => 1009,
            SqlTypeKind::Bool => 1000,
            SqlTypeKind::Varchar => 1015,
            SqlTypeKind::AnyElement
            | SqlTypeKind::AnyRange
            | SqlTypeKind::AnyMultirange
            | SqlTypeKind::AnyCompatible
            | SqlTypeKind::AnyCompatibleArray
            | SqlTypeKind::AnyCompatibleRange
            | SqlTypeKind::AnyCompatibleMultirange
            | SqlTypeKind::AnyEnum => {
                unreachable!("polymorphic pseudo-types are not concrete SQL array types")
            }
            SqlTypeKind::AnyArray => unreachable!("anyarray is not a concrete SQL array type"),
            SqlTypeKind::Trigger => unreachable!("trigger arrays are unsupported"),
            SqlTypeKind::Record | SqlTypeKind::Composite => {
                crate::include::catalog::RECORD_ARRAY_TYPE_OID as i32
            }
            SqlTypeKind::Int4Range
            | SqlTypeKind::Int8Range
            | SqlTypeKind::NumericRange
            | SqlTypeKind::DateRange
            | SqlTypeKind::TimestampRange
            | SqlTypeKind::TimestampTzRange => unreachable!("range handled above"),
        };
        return (oid, -1, -1);
    }
    match col.sql_type.kind {
        SqlTypeKind::AnyElement => (crate::include::catalog::ANYELEMENTOID as i32, 4, -1),
        SqlTypeKind::AnyEnum => (crate::include::catalog::ANYENUMOID as i32, 4, -1),
        SqlTypeKind::AnyArray => (2277, -1, -1),
        SqlTypeKind::AnyRange => (crate::include::catalog::ANYRANGEOID as i32, -1, -1),
        SqlTypeKind::AnyMultirange => (crate::include::catalog::ANYMULTIRANGEOID as i32, -1, -1),
        SqlTypeKind::AnyCompatible => (crate::include::catalog::ANYCOMPATIBLEOID as i32, 4, -1),
        SqlTypeKind::AnyCompatibleArray => (
            crate::include::catalog::ANYCOMPATIBLEARRAYOID as i32,
            -1,
            -1,
        ),
        SqlTypeKind::AnyCompatibleRange => (
            crate::include::catalog::ANYCOMPATIBLERANGEOID as i32,
            -1,
            -1,
        ),
        SqlTypeKind::AnyCompatibleMultirange => (
            crate::include::catalog::ANYCOMPATIBLEMULTIRANGEOID as i32,
            -1,
            -1,
        ),
        SqlTypeKind::Trigger => (TRIGGER_TYPE_OID as i32, -1, -1),
        SqlTypeKind::Internal => (crate::include::catalog::INTERNAL_TYPE_OID as i32, -1, -1),
        SqlTypeKind::FdwHandler => (crate::include::catalog::FDW_HANDLER_TYPE_OID as i32, 4, -1),
        SqlTypeKind::Record | SqlTypeKind::Composite => {
            (col.sql_type.type_oid as i32, -1, col.sql_type.typmod)
        }
        SqlTypeKind::Enum => (col.sql_type.type_oid as i32, 4, col.sql_type.typmod),
        SqlTypeKind::Int2 => (21, 2, -1),
        SqlTypeKind::Int4 => (23, 4, -1),
        SqlTypeKind::Int8 => (20, 8, -1),
        SqlTypeKind::PgLsn => (crate::include::catalog::PG_LSN_TYPE_OID as i32, 8, -1),
        SqlTypeKind::Void => (crate::include::catalog::VOID_TYPE_OID as i32, 4, -1),
        SqlTypeKind::Oid => (26, 4, -1),
        SqlTypeKind::RegProc => (crate::include::catalog::REGPROC_TYPE_OID as i32, 4, -1),
        SqlTypeKind::RegClass => (crate::include::catalog::REGCLASS_TYPE_OID as i32, 4, -1),
        SqlTypeKind::RegType => (crate::include::catalog::REGTYPE_TYPE_OID as i32, 4, -1),
        SqlTypeKind::RegRole => (crate::include::catalog::REGROLE_TYPE_OID as i32, 4, -1),
        SqlTypeKind::RegNamespace => (crate::include::catalog::REGNAMESPACE_TYPE_OID as i32, 4, -1),
        SqlTypeKind::RegOper => (crate::include::catalog::REGOPER_TYPE_OID as i32, 4, -1),
        SqlTypeKind::RegOperator => (crate::include::catalog::REGOPERATOR_TYPE_OID as i32, 4, -1),
        SqlTypeKind::RegProcedure => (crate::include::catalog::REGPROCEDURE_TYPE_OID as i32, 4, -1),
        SqlTypeKind::RegCollation => (crate::include::catalog::REGCOLLATION_TYPE_OID as i32, 4, -1),
        SqlTypeKind::Tid => (27, 6, -1),
        SqlTypeKind::Xid => (28, 4, -1),
        SqlTypeKind::Bit => (1560, -1, col.sql_type.typmod),
        SqlTypeKind::VarBit => (1562, -1, col.sql_type.typmod),
        SqlTypeKind::Bytea => (17, -1, -1),
        SqlTypeKind::Uuid => (crate::include::catalog::UUID_TYPE_OID as i32, 16, -1),
        SqlTypeKind::Inet => (crate::include::catalog::INET_TYPE_OID as i32, -1, -1),
        SqlTypeKind::Cidr => (crate::include::catalog::CIDR_TYPE_OID as i32, -1, -1),
        SqlTypeKind::MacAddr => (crate::include::catalog::MACADDR_TYPE_OID as i32, 6, -1),
        SqlTypeKind::MacAddr8 => (crate::include::catalog::MACADDR8_TYPE_OID as i32, 8, -1),
        SqlTypeKind::Float4 => (700, 4, -1),
        SqlTypeKind::Float8 => (701, 8, -1),
        SqlTypeKind::Money => (790, 8, -1),
        SqlTypeKind::Numeric => (1700, -1, col.sql_type.typmod),
        SqlTypeKind::Json => (114, -1, -1),
        SqlTypeKind::Jsonb => (3802, -1, -1),
        SqlTypeKind::JsonPath => (4072, -1, -1),
        SqlTypeKind::Xml => (142, -1, -1),
        SqlTypeKind::Date => (1082, 4, -1),
        SqlTypeKind::Time => (1083, 8, col.sql_type.typmod),
        SqlTypeKind::TimeTz => (1266, 12, col.sql_type.typmod),
        SqlTypeKind::Interval => (1186, 16, col.sql_type.typmod),
        SqlTypeKind::Point => (600, 16, -1),
        SqlTypeKind::Lseg => (601, 32, -1),
        SqlTypeKind::Path => (602, -1, -1),
        SqlTypeKind::Box => (603, 32, -1),
        SqlTypeKind::Polygon => (604, -1, -1),
        SqlTypeKind::Line => (628, 24, -1),
        SqlTypeKind::Circle => (718, 24, -1),
        SqlTypeKind::TsVector => (3614, -1, -1),
        SqlTypeKind::TsQuery => (3615, -1, -1),
        SqlTypeKind::RegConfig => (3734, 4, -1),
        SqlTypeKind::RegDictionary => (3769, 4, -1),
        SqlTypeKind::InternalChar => (18, 1, -1),
        SqlTypeKind::Name => (19, 64, -1),
        SqlTypeKind::Bool => (16, 1, -1),
        SqlTypeKind::Varchar => (1043, -1, col.sql_type.typmod),
        SqlTypeKind::Text
        | SqlTypeKind::Int2Vector
        | SqlTypeKind::OidVector
        | SqlTypeKind::Char
        | SqlTypeKind::PgNodeTree => (25, -1, col.sql_type.typmod),
        SqlTypeKind::Timestamp => (1114, 8, col.sql_type.typmod),
        SqlTypeKind::TimestampTz => (1184, 8, col.sql_type.typmod),
        SqlTypeKind::Range
        | SqlTypeKind::Int4Range
        | SqlTypeKind::Int8Range
        | SqlTypeKind::NumericRange
        | SqlTypeKind::DateRange
        | SqlTypeKind::TimestampRange
        | SqlTypeKind::TimestampTzRange => unreachable!("range handled above"),
        SqlTypeKind::Multirange => unreachable!("multirange handled above"),
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
    namespace_names: Option<&HashMap<u32, String>>,
) -> Option<String> {
    match kind? {
        SqlTypeKind::RegRole => Some(format_catalog_oid_text(oid, role_names, true)),
        SqlTypeKind::RegClass => Some(format_catalog_oid_text(oid, relation_names, true)),
        SqlTypeKind::RegNamespace => Some(format_catalog_oid_text(oid, namespace_names, true)),
        SqlTypeKind::RegProc => Some(
            crate::backend::executor::expr_reg::format_regproc_oid_optional(oid, None)
                .unwrap_or_else(|| format_catalog_oid_text(oid, proc_names, true)),
        ),
        SqlTypeKind::RegProcedure => Some(
            crate::backend::executor::expr_reg::format_regprocedure_oid_optional(oid, None)
                .or_else(|| proc_names.and_then(|names| names.get(&oid).cloned()))
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
        SqlTypeKind::RegType => {
            match crate::backend::executor::expr_reg::format_type_optional(Some(oid), None, None) {
                Value::Text(text) => Some(text.to_string()),
                _ => Some(oid.to_string()),
            }
        }
        SqlTypeKind::RegCollation => Some(
            crate::backend::executor::expr_reg::format_regcollation_oid_optional(oid, None)
                .unwrap_or_else(|| format_catalog_oid_text(oid, None, true)),
        ),
        _ => None,
    }
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
    enum_labels: Option<&HashMap<(u32, u32), String>>,
) -> io::Result<()> {
    buf.clear();
    buf.extend_from_slice(&(values.len() as i16).to_be_bytes());
    for (idx, val) in values.iter().enumerate() {
        let sql_type = columns.get(idx).map(|col| col.sql_type);
        let format_code = result_format_code(result_formats, idx);
        if format_code == 1 {
            if matches!(val, Value::Null) {
                buf.extend_from_slice(&(-1_i32).to_be_bytes());
                continue;
            }
            let sql_type = sql_type.ok_or_else(|| io::Error::other("missing column type"))?;
            let payload = encode_binary_data_row_value(val, sql_type)
                .map_err(|e| io::Error::other(format!("{e:?}")))?;
            buf.extend_from_slice(&(payload.len() as i32).to_be_bytes());
            buf.extend_from_slice(&payload);
            continue;
        }
        if format_code != 0 {
            return Err(io::Error::other(format!(
                "unsupported result format code {}",
                format_code
            )));
        }
        match val {
            Value::Null => buf.extend_from_slice(&(-1_i32).to_be_bytes()),
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
                        namespace_names,
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
                        namespace_names,
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
            Value::Money(v) => {
                let rendered = crate::backend::executor::money_format_text(*v);
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
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
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
            }
            Value::Uuid(v) => {
                let rendered = crate::backend::executor::value_io::render_uuid_text(v);
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
            }
            Value::Inet(v) => {
                let rendered = v.render_inet();
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
            }
            Value::Cidr(v) => {
                let rendered = v.render_cidr();
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
            }
            Value::MacAddr(v) => {
                let rendered = render_macaddr_text(v);
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
            }
            Value::MacAddr8(v) => {
                let rendered = render_macaddr8_text(v);
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
            }
            Value::Date(_)
            | Value::Time(_)
            | Value::TimeTz(_)
            | Value::Timestamp(_)
            | Value::TimestampTz(_) => {
                let rendered =
                    render_datetime_value_text_with_config(val, &float_format.datetime_config)
                        .expect("datetime values render");
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
            }
            Value::Range(_) => {
                let rendered = render_range_text_with_config(val, &float_format.datetime_config)
                    .unwrap_or_default();
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
            }
            Value::Multirange(_) => {
                let rendered =
                    render_multirange_text_with_config(val, &float_format.datetime_config)
                        .unwrap_or_default();
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
            }
            Value::Bit(v) => {
                let rendered = crate::backend::executor::render_bit_text(v);
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
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
                buf.extend_from_slice(&(text.len() as i32).to_be_bytes());
                buf.extend_from_slice(text.as_bytes());
            }
            Value::PgLsn(v) => {
                let text = render_pg_lsn_text(*v);
                buf.extend_from_slice(&(text.len() as i32).to_be_bytes());
                buf.extend_from_slice(text.as_bytes());
            }
            Value::Interval(v) => {
                let text = render_interval_text(*v);
                buf.extend_from_slice(&(text.len() as i32).to_be_bytes());
                buf.extend_from_slice(text.as_bytes());
            }
            Value::Json(v) => {
                buf.extend_from_slice(&(v.len() as i32).to_be_bytes());
                buf.extend_from_slice(v.as_bytes());
            }
            Value::Xml(v) => {
                buf.extend_from_slice(&(v.len() as i32).to_be_bytes());
                buf.extend_from_slice(v.as_bytes());
            }
            Value::Jsonb(v) => {
                let text = crate::backend::executor::jsonb::render_jsonb_bytes(v).unwrap();
                buf.extend_from_slice(&(text.len() as i32).to_be_bytes());
                buf.extend_from_slice(text.as_bytes());
            }
            Value::JsonPath(v) => {
                buf.extend_from_slice(&(v.len() as i32).to_be_bytes());
                buf.extend_from_slice(v.as_bytes());
            }
            Value::TsVector(v) => {
                let rendered = crate::backend::executor::render_tsvector_text(v);
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
            }
            Value::TsQuery(v) => {
                let rendered = crate::backend::executor::render_tsquery_text(v);
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
            }
            Value::Text(v) => {
                buf.extend_from_slice(&(v.len() as i32).to_be_bytes());
                buf.extend_from_slice(v.as_bytes());
            }
            Value::TextRef(_, _) => {
                let s = val.as_text().unwrap();
                buf.extend_from_slice(&(s.len() as i32).to_be_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            Value::InternalChar(byte) => {
                let rendered = render_internal_char_text(*byte);
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
            }
            Value::EnumOid(v) => {
                let rendered = sql_type
                    .filter(|ty| matches!(ty.kind, SqlTypeKind::Enum))
                    .and_then(|ty| enum_labels.and_then(|labels| labels.get(&(ty.type_oid, *v))))
                    .cloned()
                    .unwrap_or_else(|| v.to_string());
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
            }
            Value::Bool(true) => {
                buf.extend_from_slice(&1_i32.to_be_bytes());
                buf.push(b't');
            }
            Value::Bool(false) => {
                buf.extend_from_slice(&1_i32.to_be_bytes());
                buf.push(b'f');
            }
            Value::Point(_)
            | Value::Lseg(_)
            | Value::Path(_)
            | Value::Line(_)
            | Value::Box(_)
            | Value::Polygon(_)
            | Value::Circle(_) => {
                let rendered = render_geometry_text(val, float_format.clone()).unwrap_or_default();
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
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
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
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
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
            }
            Value::Record(record) => {
                let rendered = crate::backend::executor::value_io::format_record_text_with_options(
                    record,
                    &float_format,
                );
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
            }
        }
    }

    w.write_all(&[b'D'])?;
    w.write_all(&((buf.len() + 4) as i32).to_be_bytes())?;
    w.write_all(buf)?;
    Ok(())
}

pub(crate) fn format_text_data_value(
    value: &Value,
    column: &QueryColumn,
    float_format: FloatFormatOptions,
    role_names: Option<&HashMap<u32, String>>,
    relation_names: Option<&HashMap<u32, String>>,
    proc_names: Option<&HashMap<u32, String>>,
    namespace_names: Option<&HashMap<u32, String>>,
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
            Ok(text.as_bytes().to_vec())
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
    out.extend_from_slice(&(record.fields.len() as i32).to_be_bytes());
    for (field, value) in record.iter() {
        let field_oid = if !field.sql_type.is_array && field.sql_type.type_oid != 0 {
            field.sql_type.type_oid
        } else {
            wire_type_info(&QueryColumn {
                name: field.name.clone(),
                sql_type: field.sql_type,
                wire_type_oid: None,
            })
            .0 as u32
        };
        out.extend_from_slice(&field_oid.to_be_bytes());
        if matches!(value, Value::Null) {
            out.extend_from_slice(&(-1_i32).to_be_bytes());
            continue;
        }
        let payload = encode_binary_data_row_value(value, field.sql_type)?;
        out.extend_from_slice(&(payload.len() as i32).to_be_bytes());
        out.extend_from_slice(&payload);
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
    out.extend_from_slice(&(array.dimensions.len() as i32).to_be_bytes());
    out.extend_from_slice(
        &(array.elements.iter().any(|v| matches!(v, Value::Null)) as i32).to_be_bytes(),
    );
    out.extend_from_slice(&element_oid.to_be_bytes());
    for dim in &array.dimensions {
        out.extend_from_slice(&(dim.length as i32).to_be_bytes());
        out.extend_from_slice(&dim.lower_bound.to_be_bytes());
    }
    for value in &array.elements {
        match value {
            Value::Null => out.extend_from_slice(&(-1_i32).to_be_bytes()),
            Value::Record(record) => {
                let payload = encode_binary_record(record)?;
                out.extend_from_slice(&(payload.len() as i32).to_be_bytes());
                out.extend_from_slice(&payload);
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
    match output {
        ByteaOutputFormat::Hex => {
            let mut out = String::with_capacity(2 + bytes.len() * 2);
            out.push('\\');
            out.push('x');
            for byte in bytes {
                use std::fmt::Write as _;
                let _ = write!(&mut out, "{:02x}", byte);
            }
            out
        }
        ByteaOutputFormat::Escape => {
            let mut out = String::new();
            for &byte in bytes {
                match byte {
                    b'\\' => out.push_str("\\\\"),
                    0x20..=0x7e => out.push(byte as char),
                    0x01..=0x1f | 0x7f => {
                        use std::fmt::Write as _;
                        let _ = write!(&mut out, "\\x{byte:02x}");
                    }
                    _ => {
                        use std::fmt::Write as _;
                        let _ = write!(&mut out, "\\{:03o}", byte);
                    }
                }
            }
            out
        }
    }
}

pub(crate) fn send_command_complete(w: &mut impl Write, tag: &str) -> io::Result<()> {
    let len = 4 + tag.len() + 1;
    w.write_all(&[b'C'])?;
    w.write_all(&(len as i32).to_be_bytes())?;
    w.write_all(tag.as_bytes())?;
    w.write_all(&[0])?;
    Ok(())
}

pub(crate) fn send_parse_complete(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'1'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

pub(crate) fn send_bind_complete(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'2'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

pub(crate) fn send_portal_suspended(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b's'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

pub(crate) fn send_close_complete(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'3'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

pub(crate) fn send_no_data(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'n'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

pub(crate) fn send_parameter_description(w: &mut impl Write, type_oids: &[i32]) -> io::Result<()> {
    let len = 4 + 2 + type_oids.len() * 4;
    w.write_all(&[b't'])?;
    w.write_all(&(len as i32).to_be_bytes())?;
    w.write_all(&(type_oids.len() as i16).to_be_bytes())?;
    for oid in type_oids {
        w.write_all(&oid.to_be_bytes())?;
    }
    Ok(())
}

pub(crate) fn send_copy_in_response(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'G'])?;
    w.write_all(&7_i32.to_be_bytes())?;
    w.write_all(&[0])?;
    w.write_all(&0_i16.to_be_bytes())?;
    Ok(())
}

pub(crate) fn send_copy_out_response(
    w: &mut impl Write,
    format: CopyFormat,
    column_count: usize,
) -> io::Result<()> {
    let format_code = if matches!(format, CopyFormat::Binary) {
        1_i16
    } else {
        0_i16
    };
    let len = 4 + 1 + 2 + column_count * 2;
    w.write_all(&[b'H'])?;
    w.write_all(&(len as i32).to_be_bytes())?;
    w.write_all(&[format_code as u8])?;
    w.write_all(&(column_count as i16).to_be_bytes())?;
    for _ in 0..column_count {
        w.write_all(&format_code.to_be_bytes())?;
    }
    Ok(())
}

pub(crate) fn send_copy_data(w: &mut impl Write, data: &[u8]) -> io::Result<()> {
    w.write_all(&[b'd'])?;
    w.write_all(&((4 + data.len()) as i32).to_be_bytes())?;
    w.write_all(data)?;
    Ok(())
}

pub(crate) fn send_copy_done(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'c'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

pub(crate) fn send_notification_response(
    w: &mut impl Write,
    sender_pid: i32,
    channel: &str,
    payload: &str,
) -> io::Result<()> {
    let len = 4 + 4 + channel.len() + 1 + payload.len() + 1;
    w.write_all(&[b'A'])?;
    w.write_all(&(len as i32).to_be_bytes())?;
    w.write_all(&sender_pid.to_be_bytes())?;
    w.write_all(channel.as_bytes())?;
    w.write_all(&[0])?;
    w.write_all(payload.as_bytes())?;
    w.write_all(&[0])?;
    Ok(())
}

pub(crate) fn send_empty_query(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'I'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

pub(crate) fn send_error(
    w: &mut impl Write,
    sqlstate: &str,
    message: &str,
    detail: Option<&str>,
    hint: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    send_error_with_fields(w, sqlstate, message, detail, hint, None, position)
}

pub(crate) fn send_error_with_hint(
    w: &mut impl Write,
    sqlstate: &str,
    message: &str,
    hint: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    send_error_with_fields(w, sqlstate, message, None, hint, None, position)
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
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'V');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'C');
    body.extend_from_slice(sqlstate.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    if let Some(detail) = detail {
        body.push(b'D');
        body.extend_from_slice(detail.as_bytes());
        body.push(0);
    }
    if let Some(hint) = hint {
        body.push(b'H');
        body.extend_from_slice(hint.as_bytes());
        body.push(0);
    }
    if let Some(context) = context {
        body.push(b'W');
        body.extend_from_slice(context.as_bytes());
        body.push(0);
    }
    if let Some(position) = position {
        body.push(b'P');
        body.extend_from_slice(position.to_string().as_bytes());
        body.push(0);
    }
    body.push(0);

    w.write_all(&[b'E'])?;
    w.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
    w.write_all(&body)?;
    Ok(())
}

pub(crate) fn send_notice(
    w: &mut impl Write,
    message: &str,
    detail: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    send_notice_with_severity(w, "NOTICE", "00000", message, detail, position)
}

pub(crate) fn send_notice_with_severity(
    w: &mut impl Write,
    severity: &str,
    sqlstate: &str,
    message: &str,
    detail: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(severity.as_bytes());
    body.push(0);
    body.push(b'V');
    body.extend_from_slice(severity.as_bytes());
    body.push(0);
    body.push(b'C');
    body.extend_from_slice(sqlstate.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    if let Some(detail) = detail {
        body.push(b'D');
        body.extend_from_slice(detail.as_bytes());
        body.push(0);
    }
    if let Some(position) = position {
        body.push(b'P');
        body.extend_from_slice(position.to_string().as_bytes());
        body.push(0);
    }
    body.push(0);

    w.write_all(&[b'N'])?;
    w.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
    w.write_all(&body)?;
    Ok(())
}

pub(crate) fn send_notice_with_hint(
    w: &mut impl Write,
    severity: &str,
    sqlstate: &str,
    message: &str,
    hint: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(severity.as_bytes());
    body.push(0);
    body.push(b'V');
    body.extend_from_slice(severity.as_bytes());
    body.push(0);
    body.push(b'C');
    body.extend_from_slice(sqlstate.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    if let Some(hint) = hint {
        body.push(b'H');
        body.extend_from_slice(hint.as_bytes());
        body.push(0);
    }
    if let Some(position) = position {
        body.push(b'P');
        body.extend_from_slice(position.to_string().as_bytes());
        body.push(0);
    }
    body.push(0);

    w.write_all(&[b'N'])?;
    w.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
    w.write_all(&body)?;
    Ok(())
}

pub(crate) fn format_float8_text(value: f64, options: FloatFormatOptions) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-Infinity".to_string()
        } else {
            "Infinity".to_string()
        };
    }

    if options.extra_float_digits <= 0 {
        return format_float_with_precision(value, 15 + options.extra_float_digits);
    }
    format_float_shortest(value, false)
}

pub(crate) fn format_float4_text(value: f64, options: FloatFormatOptions) -> String {
    let value = value as f32;
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-Infinity".to_string()
        } else {
            "Infinity".to_string()
        };
    }

    if options.extra_float_digits <= 0 {
        return format_float_with_precision(value as f64, 6 + options.extra_float_digits);
    }
    format_float_shortest(value as f64, true)
}

fn format_float_shortest(value: f64, is_float4: bool) -> String {
    let normalized = if is_float4 {
        let mut buffer = ryu::Buffer::new();
        normalize_float_rendering(buffer.format_finite(value as f32), true)
    } else {
        let mut buffer = ryu::Buffer::new();
        normalize_float_rendering(buffer.format_finite(value), false)
    };
    if let Some(repaired) = repair_midpoint_render(value, is_float4, &normalized) {
        repaired
    } else {
        normalized
    }
}

#[derive(Clone)]
struct ExactRational {
    num: BigInt,
    den: BigInt,
}

fn repair_midpoint_render(value: f64, is_float4: bool, shortest: &str) -> Option<String> {
    if !is_exact_midpoint_render(value, is_float4, shortest) {
        return None;
    }

    let start_digits = significand_digit_count(shortest);
    let max_digits = if is_float4 { 9 } else { 17 };
    for digits in (start_digits + 1)..=max_digits {
        let candidate = rounded_decimal_candidate(value, is_float4, digits);
        if !parses_same_float(&candidate, value, is_float4) {
            continue;
        }
        if !is_exact_midpoint_render(value, is_float4, &candidate) {
            return Some(candidate);
        }
    }

    None
}

fn rounded_decimal_candidate(value: f64, is_float4: bool, digits: usize) -> String {
    let precision = digits.saturating_sub(1);
    let raw = if is_float4 {
        format!("{:.*e}", precision, value as f32)
    } else {
        format!("{:.*e}", precision, value)
    };
    normalize_float_rendering(&raw, is_float4)
}

fn parses_same_float(candidate: &str, value: f64, is_float4: bool) -> bool {
    if is_float4 {
        candidate
            .parse::<f32>()
            .map(|parsed| parsed.to_bits() == (value as f32).to_bits())
            .unwrap_or(false)
    } else {
        candidate
            .parse::<f64>()
            .map(|parsed| parsed.to_bits() == value.to_bits())
            .unwrap_or(false)
    }
}

fn is_exact_midpoint_render(value: f64, is_float4: bool, rendered: &str) -> bool {
    let Some(candidate) = decimal_rational(rendered) else {
        return false;
    };

    if is_float4 {
        let target = value as f32;
        if !target.is_finite() {
            return false;
        }
        let exact = rational_from_f32(target);
        let lower = rational_from_f32(next_down_f32(target));
        let upper = rational_from_f32(next_up_f32(target));
        rational_is_midpoint(&candidate, &lower, &exact)
            || rational_is_midpoint(&candidate, &exact, &upper)
    } else {
        if !value.is_finite() {
            return false;
        }
        let exact = rational_from_f64(value);
        let lower = rational_from_f64(next_down_f64(value));
        let upper = rational_from_f64(next_up_f64(value));
        rational_is_midpoint(&candidate, &lower, &exact)
            || rational_is_midpoint(&candidate, &exact, &upper)
    }
}

fn significand_digit_count(text: &str) -> usize {
    let unsigned = text.trim_start_matches('-');
    let significand = unsigned
        .split_once(['e', 'E'])
        .map(|(mantissa, _)| mantissa)
        .unwrap_or(unsigned);
    let digits = significand.replace('.', "");
    let trimmed = digits.trim_start_matches('0');
    trimmed.len().max(1)
}

fn decimal_rational(text: &str) -> Option<ExactRational> {
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exp)) => (mantissa, exp.parse::<i32>().ok()?),
        None => (unsigned, 0),
    };
    let (whole, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let mut digits = String::with_capacity(whole.len() + frac.len());
    digits.push_str(whole);
    digits.push_str(frac);
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Some(ExactRational {
            num: BigInt::from(0u8),
            den: BigInt::one(),
        });
    }

    let mut num = BigInt::from_str(digits).ok()?;
    let scale = frac.len() as i32 - exponent;
    let den = if scale >= 0 {
        pow10(scale as u32)
    } else {
        num *= pow10((-scale) as u32);
        BigInt::one()
    };
    if negative {
        num = -num;
    }
    Some(ExactRational { num, den })
}

fn rational_from_f64(value: f64) -> ExactRational {
    let bits = value.to_bits();
    let negative = (bits >> 63) != 0;
    let ieee_mantissa = bits & ((1u64 << 52) - 1);
    let ieee_exponent = ((bits >> 52) & 0x7ff) as i32;
    let (mantissa, exp2) = if ieee_exponent == 0 {
        (ieee_mantissa, 1 - 1023 - 52)
    } else {
        ((1u64 << 52) | ieee_mantissa, ieee_exponent - 1023 - 52)
    };
    rational_from_binary_parts(negative, BigInt::from(mantissa), exp2)
}

fn rational_from_f32(value: f32) -> ExactRational {
    let bits = value.to_bits();
    let negative = (bits >> 31) != 0;
    let ieee_mantissa = bits & ((1u32 << 23) - 1);
    let ieee_exponent = ((bits >> 23) & 0xff) as i32;
    let (mantissa, exp2) = if ieee_exponent == 0 {
        (ieee_mantissa, 1 - 127 - 23)
    } else {
        ((1u32 << 23) | ieee_mantissa, ieee_exponent - 127 - 23)
    };
    rational_from_binary_parts(negative, BigInt::from(mantissa), exp2)
}

fn rational_from_binary_parts(negative: bool, mut num: BigInt, exp2: i32) -> ExactRational {
    if negative {
        num = -num;
    }
    if exp2 >= 0 {
        num <<= exp2 as usize;
        ExactRational {
            num,
            den: BigInt::one(),
        }
    } else {
        ExactRational {
            num,
            den: BigInt::one() << (-exp2 as usize),
        }
    }
}

fn rational_is_midpoint(
    candidate: &ExactRational,
    left: &ExactRational,
    right: &ExactRational,
) -> bool {
    let lhs = &candidate.num * BigInt::from(2u8) * &left.den * &right.den;
    let rhs = &candidate.den * (&left.num * &right.den + &right.num * &left.den);
    lhs == rhs
}

fn pow10(exp: u32) -> BigInt {
    BigInt::from(10u8).pow(exp)
}

fn next_up_f64(value: f64) -> f64 {
    if value.is_nan() || value == f64::INFINITY {
        return value;
    }
    if value == 0.0 {
        return f64::from_bits(1);
    }
    let bits = value.to_bits();
    if value.is_sign_negative() {
        f64::from_bits(bits - 1)
    } else {
        f64::from_bits(bits + 1)
    }
}

fn next_down_f64(value: f64) -> f64 {
    if value.is_nan() || value == f64::NEG_INFINITY {
        return value;
    }
    if value == 0.0 {
        return f64::from_bits((1u64 << 63) | 1);
    }
    let bits = value.to_bits();
    if value.is_sign_negative() {
        f64::from_bits(bits + 1)
    } else {
        f64::from_bits(bits - 1)
    }
}

fn next_up_f32(value: f32) -> f32 {
    if value.is_nan() || value == f32::INFINITY {
        return value;
    }
    if value == 0.0 {
        return f32::from_bits(1);
    }
    let bits = value.to_bits();
    if value.is_sign_negative() {
        f32::from_bits(bits - 1)
    } else {
        f32::from_bits(bits + 1)
    }
}

fn next_down_f32(value: f32) -> f32 {
    if value.is_nan() || value == f32::NEG_INFINITY {
        return value;
    }
    if value == 0.0 {
        return f32::from_bits((1u32 << 31) | 1);
    }
    let bits = value.to_bits();
    if value.is_sign_negative() {
        f32::from_bits(bits + 1)
    } else {
        f32::from_bits(bits - 1)
    }
}

fn format_float_with_precision(value: f64, precision: i32) -> String {
    let precision = precision.clamp(1, 32) as usize;
    let sign = if value.is_sign_negative() { "-" } else { "" };
    let abs = value.abs();
    if abs == 0.0 {
        return format!("{sign}0");
    }

    let rendered = format!("{:.*e}", precision - 1, abs);
    let (mantissa, exponent) = rendered.split_once('e').unwrap_or((&rendered, "0"));
    let exponent = exponent.parse::<i32>().unwrap_or(0);
    let digits = mantissa.replace('.', "");
    let body = if exponent < -4 || exponent >= precision as i32 {
        let mantissa = trim_fractional_zeros(mantissa);
        format_scientific_mantissa(mantissa, exponent, true)
    } else {
        let decimal_pos = exponent + 1;
        let rendered = if decimal_pos <= 0 {
            format!("0.{}{}", "0".repeat((-decimal_pos) as usize), digits)
        } else if decimal_pos as usize >= digits.len() {
            format!(
                "{digits}{}",
                "0".repeat(decimal_pos as usize - digits.len())
            )
        } else {
            format!(
                "{}.{}",
                &digits[..decimal_pos as usize],
                &digits[decimal_pos as usize..]
            )
        };
        trim_fractional_zeros(&rendered).to_string()
    };
    format!("{sign}{body}")
}

fn normalize_float_rendering(raw: &str, is_float4: bool) -> String {
    let (sign, unsigned) = match raw.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", raw),
    };
    let scientific_threshold = if is_float4 { 6 } else { 15 };

    let (mut digits, exponent) = if let Some((mantissa, exponent)) = unsigned.split_once(['e', 'E'])
    {
        let exponent = exponent.parse::<i32>().unwrap_or(0);
        let fractional_digits = mantissa
            .split_once('.')
            .map(|(_, frac)| frac.len())
            .unwrap_or(0);
        (
            mantissa.replace('.', ""),
            exponent - fractional_digits as i32,
        )
    } else if let Some((whole, frac)) = unsigned.split_once('.') {
        (format!("{whole}{frac}"), -(frac.len() as i32))
    } else {
        (unsigned.to_string(), 0)
    };

    digits = digits.trim_start_matches('0').to_string();
    if digits.is_empty() {
        return format!("{sign}0");
    }

    let display_exponent = exponent + digits.len() as i32 - 1;
    if display_exponent < -4 || display_exponent >= scientific_threshold {
        let significant_digits = digits.trim_end_matches('0');
        let mantissa = if significant_digits.len() == 1 {
            significant_digits.to_string()
        } else {
            format!("{}.{}", &significant_digits[..1], &significant_digits[1..])
        };
        return format!(
            "{sign}{}",
            format_scientific_mantissa(&mantissa, display_exponent, true)
        );
    }

    if exponent >= 0 {
        digits.push_str(&"0".repeat(exponent as usize));
        return format!("{sign}{digits}");
    }

    let decimal_pos = digits.len() as i32 + exponent;
    let rendered = if decimal_pos > 0 {
        format!(
            "{}.{}",
            &digits[..decimal_pos as usize],
            &digits[decimal_pos as usize..]
        )
    } else {
        format!("0.{}{}", "0".repeat((-decimal_pos) as usize), digits)
    };
    format!("{sign}{}", trim_fractional_zeros(&rendered))
}

fn format_scientific_mantissa(mantissa: &str, exponent: i32, pad_exponent: bool) -> String {
    let mantissa = trim_fractional_zeros(mantissa);
    if pad_exponent {
        let sign = if exponent < 0 { '-' } else { '+' };
        let digits = exponent.abs();
        if digits < 10 {
            return format!("{mantissa}e{sign}0{digits}");
        }
        return format!("{mantissa}e{sign}{digits}");
    } else {
        format!("{mantissa}e{exponent:+}")
    }
}

fn trim_fractional_zeros(text: &str) -> &str {
    let trimmed = text.trim_end_matches('0').trim_end_matches('.');
    if trimmed.is_empty() || trimmed == "-" {
        if text.starts_with('-') { "-0" } else { "0" }
    } else {
        trimmed
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FloatFormatOptions, format_bytea_text, format_exec_error, format_exec_error_hint,
        format_float4_text, format_float8_text, send_error_with_fields, send_typed_data_row,
    };
    use crate::backend::executor::{ExecError, QueryColumn, Value};
    use crate::backend::parser::{SqlType, SqlTypeKind};
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
        proc_names.insert(6403, "pg_rust_test_fdw_handler".to_string());

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
        )
        .unwrap();

        assert!(
            out.windows("pg_rust_test_fdw_handler".len())
                .any(|window| window == b"pg_rust_test_fdw_handler")
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
        )
        .unwrap();

        assert!(
            out.windows("public".len())
                .any(|window| window == b"public")
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
        )
        .unwrap();

        assert!(
            out.windows("{\"@ 0\",\"@ 1 hour 42 mins 20 secs\"}".len())
                .any(|window| window == b"{\"@ 0\",\"@ 1 hour 42 mins 20 secs\"}")
        );
    }

    #[test]
    fn macaddr_protocol_metadata_and_binary_output_use_postgres_oids() {
        assert_eq!(
            super::wire_type_info(&QueryColumn {
                name: "m".into(),
                sql_type: SqlType::new(SqlTypeKind::MacAddr),
                wire_type_oid: None,
            }),
            (crate::include::catalog::MACADDR_TYPE_OID as i32, 6, -1)
        );
        assert_eq!(
            super::wire_type_info(&QueryColumn {
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
