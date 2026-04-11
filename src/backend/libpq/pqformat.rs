use std::io::{self, Write};

use crate::backend::executor::exec_expr::format_array_text;
use crate::backend::access::heap::heapam::HeapError;
use crate::backend::executor::{ExecError, QueryColumn, Value};
use crate::include::access::htup::TupleError;
use crate::backend::parser::SqlTypeKind;

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
        ExecError::InvalidFloatInput(value) => {
            format!("invalid input syntax for type double precision: \"{value}\"")
        }
        ExecError::InvalidStorageValue { details, .. } => details.clone(),
        ExecError::Int2OutOfRange => "smallint out of range".to_string(),
        ExecError::Int4OutOfRange => "integer out of range".to_string(),
        ExecError::Int8OutOfRange => "bigint out of range".to_string(),
        ExecError::NumericFieldOverflow => "numeric field overflow".to_string(),
        ExecError::RequestedLengthTooLarge => "requested length too large".to_string(),
        ExecError::DivisionByZero(_) => "division by zero".to_string(),
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
) -> io::Result<()> {
    send_row_description(stream, columns)?;
    let mut row_buf = Vec::new();
    for row in rows {
        send_data_row(stream, row, &mut row_buf)?;
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
) -> io::Result<()> {
    buf.clear();
    buf.extend_from_slice(&(values.len() as i16).to_be_bytes());
    for val in values {
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
                use std::io::Write as _;
                write!(buf, "{v}").unwrap();
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

pub(crate) fn send_error(w: &mut impl Write, sqlstate: &str, message: &str) -> io::Result<()> {
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
    body.push(0);

    w.write_all(&[b'E'])?;
    w.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
    w.write_all(&body)?;
    Ok(())
}
