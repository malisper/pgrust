use std::io::{self, Write};
use std::str::FromStr;

use crate::backend::access::heap::heapam::HeapError;
use crate::backend::executor::exec_expr::format_array_text;
use crate::backend::executor::{
    ExecError, QueryColumn, Value, geometry_input_error_message,
    render_datetime_value_text_with_config, render_geometry_text, render_internal_char_text,
};
use crate::backend::parser::SqlTypeKind;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::include::access::htup::TupleError;
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
        ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
            expected: "text or bit argument",
            actual,
        }) if actual.starts_with("Length(") => {
            let signature = actual.replace("Length", "length");
            format!("function {signature} does not exist")
        }
        ExecError::Parse(p) => p.to_string(),
        ExecError::Regex(err) => err.message.clone(),
        ExecError::DetailedError { message, .. } => message.clone(),
        ExecError::RaiseException(message) => message.clone(),
        ExecError::InvalidRegex(message) => message.clone(),
        ExecError::UniqueViolation { constraint } => {
            format!("duplicate key value violates unique constraint \"{constraint}\"")
        }
        ExecError::NotNullViolation {
            relation,
            column,
            constraint,
        } => format!(
            "null value in column \"{column}\" of relation \"{relation}\" violates not-null constraint \"{constraint}\""
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
        ExecError::InvalidByteaInput { value } => {
            format!("invalid input syntax for type bytea: \"{value}\"")
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
            format!("{arg} value cannot be {issue}")
        }
        ExecError::Heap(HeapError::Tuple(TupleError::Oversized { size, max_size })) => {
            format!("row is too big: size {size}, maximum size {max_size}")
        }
        other => format!("{other:?}"),
    }
}

pub(crate) fn format_exec_error_hint(e: &ExecError) -> Option<String> {
    match e {
        ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
            expected: "text or bit argument",
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
        _ => None,
    }
}

pub(crate) fn infer_command_tag(sql: &str, affected: usize) -> String {
    let first_word = sql
        .split_ascii_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    match first_word.as_str() {
        "INSERT" => format!("INSERT 0 {affected}"),
        "UPDATE" => format!("UPDATE {affected}"),
        "DELETE" => format!("DELETE {affected}"),
        "CREATE" => "CREATE TABLE".to_string(),
        "DROP" => "DROP TABLE".to_string(),
        "ANALYZE" => "ANALYZE".to_string(),
        "COMMENT" => "COMMENT".to_string(),
        "DO" => "DO".to_string(),
        "VACUUM" => "VACUUM".to_string(),
        "SET" => "SET".to_string(),
        "RESET" => "RESET".to_string(),
        "BEGIN" | "START" => "BEGIN".to_string(),
        "COMMIT" | "END" => "COMMIT".to_string(),
        "ROLLBACK" => "ROLLBACK".to_string(),
        _ => format!("SELECT {affected}"),
    }
}

pub(crate) fn send_query_result(
    stream: &mut impl Write,
    columns: &[QueryColumn],
    rows: &[Vec<Value>],
    tag: &str,
    float_format: FloatFormatOptions,
) -> io::Result<()> {
    send_row_description(stream, columns)?;
    let mut row_buf = Vec::new();
    for row in rows {
        send_typed_data_row(stream, row, columns, &mut row_buf, float_format.clone())?;
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
    let mut body = Vec::new();
    body.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    for col in columns {
        body.extend_from_slice(col.name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0_i32.to_be_bytes());
        body.extend_from_slice(&0_i16.to_be_bytes());
        let (oid, typlen, typmod) = wire_type_info(col);
        body.extend_from_slice(&oid.to_be_bytes());
        body.extend_from_slice(&typlen.to_be_bytes());
        body.extend_from_slice(&typmod.to_be_bytes());
        body.extend_from_slice(&0_i16.to_be_bytes());
    }

    w.write_all(&[b'T'])?;
    w.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
    w.write_all(&body)?;
    Ok(())
}

fn wire_type_info(col: &QueryColumn) -> (i32, i16, i32) {
    if !col.sql_type.is_array && col.sql_type.type_oid != 0 {
        return (col.sql_type.type_oid as i32, -1, col.sql_type.typmod);
    }
    if col.sql_type.is_array {
        let oid = match col.sql_type.kind {
            SqlTypeKind::Int2 => 1005,
            SqlTypeKind::Int4 => 1007,
            SqlTypeKind::Int8 => 1016,
            SqlTypeKind::Oid => 1028,
            SqlTypeKind::Bit => 1561,
            SqlTypeKind::VarBit => 1563,
            SqlTypeKind::Bytea => 1001,
            SqlTypeKind::Float4 => 1021,
            SqlTypeKind::Float8 => 1022,
            SqlTypeKind::Money => 791,
            SqlTypeKind::Numeric => 1231,
            SqlTypeKind::Json => 199,
            SqlTypeKind::Jsonb => 3807,
            SqlTypeKind::JsonPath => 4073,
            SqlTypeKind::Date => 1182,
            SqlTypeKind::Time => 1183,
            SqlTypeKind::TimeTz => 1270,
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
            SqlTypeKind::AnyArray => unreachable!("anyarray is not a concrete SQL array type"),
            SqlTypeKind::Record | SqlTypeKind::Composite => {
                unreachable!("record arrays are unsupported")
            }
        };
        return (oid, -1, -1);
    }
    match col.sql_type.kind {
        SqlTypeKind::AnyArray => (2277, -1, -1),
        SqlTypeKind::Record | SqlTypeKind::Composite => {
            (col.sql_type.type_oid as i32, -1, col.sql_type.typmod)
        }
        SqlTypeKind::Int2 => (21, 2, -1),
        SqlTypeKind::Int4 => (23, 4, -1),
        SqlTypeKind::Int8 => (20, 8, -1),
        SqlTypeKind::Oid => (26, 4, -1),
        SqlTypeKind::Bit => (1560, -1, col.sql_type.typmod),
        SqlTypeKind::VarBit => (1562, -1, col.sql_type.typmod),
        SqlTypeKind::Bytea => (17, -1, -1),
        SqlTypeKind::Float4 => (700, 4, -1),
        SqlTypeKind::Float8 => (701, 8, -1),
        SqlTypeKind::Money => (790, 8, -1),
        SqlTypeKind::Numeric => (1700, -1, col.sql_type.typmod),
        SqlTypeKind::Json => (114, -1, -1),
        SqlTypeKind::Jsonb => (3802, -1, -1),
        SqlTypeKind::JsonPath => (4072, -1, -1),
        SqlTypeKind::Date => (1082, 4, -1),
        SqlTypeKind::Time => (1083, 8, col.sql_type.typmod),
        SqlTypeKind::TimeTz => (1266, 12, col.sql_type.typmod),
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
    }
}

pub(crate) fn send_typed_data_row(
    w: &mut impl Write,
    values: &[Value],
    columns: &[QueryColumn],
    buf: &mut Vec<u8>,
    float_format: FloatFormatOptions,
) -> io::Result<()> {
    buf.clear();
    buf.extend_from_slice(&(values.len() as i16).to_be_bytes());
    for (idx, val) in values.iter().enumerate() {
        let sql_type = columns.get(idx).map(|col| col.sql_type);
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
                let mut itoa_buf = itoa::Buffer::new();
                let written = itoa_buf.format(*v);
                buf.extend_from_slice(written.as_bytes());
                let text_len = (buf.len() - start - 4) as i32;
                buf[start..start + 4].copy_from_slice(&text_len.to_be_bytes());
            }
            Value::Int64(v) => {
                let start = buf.len();
                buf.extend_from_slice(&0_i32.to_be_bytes());
                let mut itoa_buf = itoa::Buffer::new();
                let written = itoa_buf.format(*v);
                buf.extend_from_slice(written.as_bytes());
                let text_len = (buf.len() - start - 4) as i32;
                buf[start..start + 4].copy_from_slice(&text_len.to_be_bytes());
            }
            Value::Money(v) => {
                let rendered = crate::backend::executor::money_format_text(*v);
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
            }
            Value::Bytea(v) => {
                let rendered = format_bytea_text(v, float_format.bytea_output);
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
            Value::Json(v) => {
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
                let rendered = format_array_text(items);
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
            }
            Value::PgArray(array) => {
                let rendered = crate::backend::executor::value_io::format_array_value_text(array);
                buf.extend_from_slice(&(rendered.len() as i32).to_be_bytes());
                buf.extend_from_slice(rendered.as_bytes());
            }
            Value::Record(record) => {
                let rendered = crate::backend::executor::jsonb::jsonb_from_value(&Value::Record(
                    record.clone(),
                ))
                .map(|value| value.to_serde().to_string())
                .unwrap_or_default();
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
    send_error_with_fields(w, sqlstate, message, detail, hint, position)
}

pub(crate) fn send_error_with_hint(
    w: &mut impl Write,
    sqlstate: &str,
    message: &str,
    hint: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    send_error_with_fields(w, sqlstate, message, None, hint, position)
}

pub(crate) fn send_error_with_fields(
    w: &mut impl Write,
    sqlstate: &str,
    message: &str,
    detail: Option<&str>,
    hint: Option<&str>,
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
    use super::{FloatFormatOptions, format_bytea_text, format_float4_text, format_float8_text};
    use crate::pgrust::session::ByteaOutputFormat;

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
}
