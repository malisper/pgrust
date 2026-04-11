use std::io::{self, Write};

use crate::backend::executor::exec_expr::format_array_text;
use crate::backend::access::heap::heapam::HeapError;
use crate::backend::executor::{ExecError, QueryColumn, Value};
use crate::include::access::htup::TupleError;
use crate::backend::parser::SqlTypeKind;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct FloatFormatOptions {
    pub(crate) extra_float_digits: i32,
}

pub(crate) fn format_exec_error(e: &ExecError) -> String {
    match e {
        ExecError::Parse(p) => p.to_string(),
        ExecError::StringDataRightTruncation { ty } => format!("value too long for type {ty}"),
        ExecError::InvalidIntegerInput { ty, value } => {
            format!("invalid input syntax for type {ty}: \"{value}\"")
        }
        ExecError::IntegerOutOfRange { ty, value } => {
            format!("value \"{value}\" is out of range for type {ty}")
        }
        ExecError::InvalidNumericInput(value) => {
            format!("invalid input syntax for type numeric: \"{value}\"")
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
        ExecError::DivisionByZero(_) => "division by zero".to_string(),
        ExecError::GenerateSeriesZeroStep => "step size cannot equal zero".to_string(),
        ExecError::Heap(HeapError::Tuple(TupleError::Oversized { size, max_size })) => {
            format!("row is too big: size {size}, maximum size {max_size}")
        }
        other => format!("{other:?}"),
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
        send_typed_data_row(stream, row, columns, &mut row_buf, float_format)?;
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
    if col.sql_type.is_array {
        let oid = match col.sql_type.kind {
            SqlTypeKind::Int2 => 1005,
            SqlTypeKind::Int4 => 1007,
            SqlTypeKind::Int8 => 1016,
            SqlTypeKind::Oid => 1028,
            SqlTypeKind::Float4 => 1021,
            SqlTypeKind::Float8 => 1022,
            SqlTypeKind::Numeric => 1231,
            SqlTypeKind::Json => 199,
            SqlTypeKind::Jsonb => 3807,
            SqlTypeKind::JsonPath => 4073,
            SqlTypeKind::Text | SqlTypeKind::Timestamp | SqlTypeKind::Char => 1009,
            SqlTypeKind::Bool => 1000,
            SqlTypeKind::Varchar => 1015,
        };
        return (oid, -1, -1);
    }
    match col.sql_type.kind {
        SqlTypeKind::Int2 => (21, 2, -1),
        SqlTypeKind::Int4 => (23, 4, -1),
        SqlTypeKind::Int8 => (20, 8, -1),
        SqlTypeKind::Oid => (26, 4, -1),
        SqlTypeKind::Float4 => (700, 4, -1),
        SqlTypeKind::Float8 => (701, 8, -1),
        SqlTypeKind::Numeric => (1700, -1, col.sql_type.typmod),
        SqlTypeKind::Json => (114, -1, -1),
        SqlTypeKind::Jsonb => (3802, -1, -1),
        SqlTypeKind::JsonPath => (4072, -1, -1),
        SqlTypeKind::Bool => (16, 1, -1),
        SqlTypeKind::Varchar => (1043, -1, col.sql_type.typmod),
        SqlTypeKind::Text | SqlTypeKind::Timestamp | SqlTypeKind::Char => {
            (25, -1, col.sql_type.typmod)
        }
    }
}

pub(crate) fn send_data_row(
    w: &mut impl Write,
    values: &[Value],
    buf: &mut Vec<u8>,
    float_format: FloatFormatOptions,
) -> io::Result<()> {
    send_typed_data_row(w, values, &[], buf, float_format)
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
            Value::Float64(v) => {
                let start = buf.len();
                buf.extend_from_slice(&0_i32.to_be_bytes());
                let rendered = match sql_type.map(|ty| ty.kind) {
                    Some(SqlTypeKind::Float4) => format_float4_text(*v, float_format),
                    _ => format_float8_text(*v, float_format),
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
            Value::Text(v) => {
                buf.extend_from_slice(&(v.len() as i32).to_be_bytes());
                buf.extend_from_slice(v.as_bytes());
            }
            Value::TextRef(_, _) => {
                let s = val.as_text().unwrap();
                buf.extend_from_slice(&(s.len() as i32).to_be_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            Value::Bool(true) => {
                buf.extend_from_slice(&1_i32.to_be_bytes());
                buf.push(b't');
            }
            Value::Bool(false) => {
                buf.extend_from_slice(&1_i32.to_be_bytes());
                buf.push(b'f');
            }
            Value::Array(items) => {
                let rendered = format_array_text(items);
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

fn format_float8_text(value: f64, options: FloatFormatOptions) -> String {
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

fn format_float4_text(value: f64, options: FloatFormatOptions) -> String {
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
    let raw = if is_float4 {
        let mut buffer = ryu::Buffer::new();
        buffer.format_finite(value as f32).to_string()
    } else {
        let mut buffer = ryu::Buffer::new();
        buffer.format_finite(value).to_string()
    };
    normalize_float_rendering(raw)
}

fn format_float_with_precision(value: f64, precision: i32) -> String {
    let precision = precision.max(1) as usize;
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
        format!("{mantissa}e{exponent:+}")
    } else {
        let decimal_pos = exponent + 1;
        let rendered = if decimal_pos <= 0 {
            format!("0.{}{}", "0".repeat((-decimal_pos) as usize), digits)
        } else if decimal_pos as usize >= digits.len() {
            format!("{digits}{}", "0".repeat(decimal_pos as usize - digits.len()))
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

fn normalize_float_rendering(raw: String) -> String {
    if let Some((mantissa, exponent)) = raw.split_once(['e', 'E']) {
        let mantissa = trim_fractional_zeros(mantissa);
        let exponent = exponent.parse::<i32>().unwrap_or(0);
        return format!("{mantissa}e{exponent:+}");
    }
    trim_fractional_zeros(&raw).to_string()
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
    use super::{FloatFormatOptions, format_float4_text, format_float8_text};

    #[test]
    fn large_float8_values_render_in_scientific_notation() {
        assert_eq!(
            format_float8_text(4_567_890_123_456_789.0, FloatFormatOptions::default()),
            "4.56789012345679e+15"
        );
        assert_eq!(
            format_float8_text(-4_567_890_123_456_789.0, FloatFormatOptions::default()),
            "-4.56789012345679e+15"
        );
        assert_eq!(format_float8_text(123.0, FloatFormatOptions::default()), "123");
    }

    #[test]
    fn large_float4_values_render_in_scientific_notation() {
        assert_eq!(
            format_float4_text(4_567_890_123_456_789.0, FloatFormatOptions::default()),
            "4.56789e+15"
        );
        assert_eq!(format_float4_text(123.0, FloatFormatOptions::default()), "123");
    }

    #[test]
    fn float_special_values_use_postgres_spelling() {
        assert_eq!(format_float8_text(f64::NAN, FloatFormatOptions::default()), "NaN");
        assert_eq!(
            format_float8_text(f64::INFINITY, FloatFormatOptions::default()),
            "Infinity"
        );
        assert_eq!(
            format_float8_text(f64::NEG_INFINITY, FloatFormatOptions::default()),
            "-Infinity"
        );
        assert_eq!(format_float4_text(f64::NAN, FloatFormatOptions::default()), "NaN");
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
        };
        assert_eq!(format_float8_text(31.690692639953454, options), "31.6906926399535");
        assert_eq!(format_float8_text(1004.3000000000004, options), "1004.3");
        assert_eq!(format_float4_text(1.2345679402097818e20, options), "1.23457e+20");
    }

    #[test]
    fn shortest_format_preserves_negative_zero() {
        assert_eq!(format_float8_text(-0.0, FloatFormatOptions::default()), "-0");
    }
}
