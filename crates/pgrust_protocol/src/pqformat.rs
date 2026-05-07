use std::io::{self, Write};

use pgrust_catalog_data::*;
use pgrust_nodes::Value;
use pgrust_nodes::parsenodes::{CopyFormat, SqlTypeKind};
use pgrust_nodes::primnodes::QueryColumn;

pub fn infer_command_tag(sql: &str, affected: usize) -> String {
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
        ("CREATE", "CAST") => "CREATE CAST".to_string(),
        ("CREATE", _) => "CREATE TABLE".to_string(),
        ("DROP", "TRIGGER") => "DROP TRIGGER".to_string(),
        ("DROP", "TYPE") => "DROP TYPE".to_string(),
        ("DROP", "CAST") => "DROP CAST".to_string(),
        ("DROP", _) => "DROP TABLE".to_string(),
        ("ANALYZE", _) => "ANALYZE".to_string(),
        ("COMMENT", _) => "COMMENT".to_string(),
        ("CHECKPOINT", _) => "CHECKPOINT".to_string(),
        ("COPY", _) => format!("COPY {affected}"),
        ("DO", _) => "DO".to_string(),
        ("LISTEN", _) => "LISTEN".to_string(),
        ("NOTIFY", _) => "NOTIFY".to_string(),
        ("UNLISTEN", _) => "UNLISTEN".to_string(),
        ("LOAD", _) => "LOAD".to_string(),
        ("DISCARD", _) => "DISCARD".to_string(),
        ("LOCK", _) => "LOCK TABLE".to_string(),
        ("VACUUM", _) => "VACUUM".to_string(),
        ("PREPARE", _) => "PREPARE".to_string(),
        ("SET", _) => "SET".to_string(),
        ("RESET", _) => "RESET".to_string(),
        ("BEGIN", _) | ("START", _) => "BEGIN".to_string(),
        ("COMMIT", _) | ("END", _) => "COMMIT".to_string(),
        ("RELEASE", _) => "RELEASE".to_string(),
        ("ROLLBACK", _) => "ROLLBACK".to_string(),
        _ => format!("SELECT {affected}"),
    }
}

pub fn infer_dml_returning_command_tag(sql: &str, affected: usize) -> Option<String> {
    let first_word = sql
        .split_ascii_whitespace()
        .next()
        .map(|word| word.to_ascii_uppercase())
        .unwrap_or_default();
    matches!(first_word.as_str(), "INSERT" | "UPDATE" | "DELETE")
        .then(|| infer_command_tag(sql, affected))
}

pub fn send_auth_ok(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'R'])?;
    w.write_all(&8_i32.to_be_bytes())?;
    w.write_all(&0_i32.to_be_bytes())?;
    Ok(())
}

pub fn send_parameter_status(w: &mut impl Write, name: &str, value: &str) -> io::Result<()> {
    let len = 4 + name.len() + 1 + value.len() + 1;
    w.write_all(&[b'S'])?;
    w.write_all(&(len as i32).to_be_bytes())?;
    w.write_all(name.as_bytes())?;
    w.write_all(&[0])?;
    w.write_all(value.as_bytes())?;
    w.write_all(&[0])?;
    Ok(())
}

pub fn send_backend_key_data(w: &mut impl Write, pid: i32, key: i32) -> io::Result<()> {
    w.write_all(&[b'K'])?;
    w.write_all(&12_i32.to_be_bytes())?;
    w.write_all(&pid.to_be_bytes())?;
    w.write_all(&key.to_be_bytes())?;
    Ok(())
}

pub fn send_ready_for_query(w: &mut impl Write, status: u8) -> io::Result<()> {
    w.write_all(&[b'Z'])?;
    w.write_all(&5_i32.to_be_bytes())?;
    w.write_all(&[status])?;
    Ok(())
}

pub fn send_row_description(w: &mut impl Write, columns: &[QueryColumn]) -> io::Result<()> {
    send_row_description_with_formats(w, columns, &[])
}

pub fn send_query_result_with_rows<W: Write>(
    w: &mut W,
    columns: &[QueryColumn],
    rows: &[Vec<Value>],
    tag: &str,
    mut send_row: impl FnMut(&mut W, &[Value], &mut Vec<u8>) -> io::Result<()>,
) -> io::Result<()> {
    send_row_description(w, columns)?;
    let mut row_buf = Vec::new();
    for row in rows {
        send_row(w, row, &mut row_buf)?;
    }
    send_command_complete(w, tag)
}

pub fn send_row_description_with_formats(
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

pub fn result_format_code(result_formats: &[i16], index: usize) -> i16 {
    match result_formats {
        [] => 0,
        [single] => *single,
        many => many.get(index).copied().unwrap_or(0),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResultFormatValidationError<E> {
    UnsupportedResultFormatCode(i16),
    BinaryType(E),
    BinaryEncode(E),
}

pub fn binary_output_type_supported(sql_type: pgrust_nodes::SqlType) -> bool {
    if sql_type.is_array {
        return matches!(sql_type.kind, SqlTypeKind::Record | SqlTypeKind::Composite);
    }
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
            | SqlTypeKind::Cstring
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
}

pub fn validate_binary_result_formats<E>(
    rows: &[Vec<Value>],
    columns: &[QueryColumn],
    result_formats: &[i16],
    mut validate_binary_output_type: impl FnMut(&QueryColumn) -> Result<(), E>,
    mut encode_binary_value: impl FnMut(&Value, &QueryColumn) -> Result<Vec<u8>, E>,
) -> Result<(), ResultFormatValidationError<E>> {
    for (index, column) in columns.iter().enumerate() {
        match result_format_code(result_formats, index) {
            0 => {}
            1 => {
                validate_binary_output_type(column)
                    .map_err(ResultFormatValidationError::BinaryType)?;
                for row in rows {
                    let Some(value) = row.get(index) else {
                        continue;
                    };
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    encode_binary_value(value, column)
                        .map_err(ResultFormatValidationError::BinaryEncode)?;
                }
            }
            code => {
                return Err(ResultFormatValidationError::UnsupportedResultFormatCode(
                    code,
                ));
            }
        }
    }
    Ok(())
}

pub fn wire_type_info(col: &QueryColumn) -> (i32, i16, i32) {
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
        if let Some(multirange_type) = multirange_type_ref_for_sql_type(col.sql_type)
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
    if let Some(multirange_type) = multirange_type_ref_for_sql_type(col.sql_type) {
        return (multirange_type.type_oid() as i32, -1, col.sql_type.typmod);
    }
    if let Some(oid) = col.wire_type_oid {
        return (oid as i32, -1, col.sql_type.typmod);
    }
    if !col.sql_type.is_array && col.sql_type.type_oid != 0 {
        return (col.sql_type.type_oid as i32, -1, col.sql_type.typmod);
    }
    if col.sql_type.is_array {
        let oid = match col.sql_type.kind {
            SqlTypeKind::Int2 => 1005,
            SqlTypeKind::Int4 => 1007,
            SqlTypeKind::Int8 => 1016,
            SqlTypeKind::PgLsn => PG_LSN_ARRAY_TYPE_OID as i32,
            SqlTypeKind::Range => col.sql_type.type_oid as i32,
            SqlTypeKind::Multirange => col.sql_type.type_oid as i32,
            SqlTypeKind::Enum => col.sql_type.type_oid as i32,
            SqlTypeKind::Internal => unreachable!("internal arrays are unsupported"),
            SqlTypeKind::Shell => unreachable!("shell type arrays are unsupported"),
            SqlTypeKind::Cstring => CSTRING_ARRAY_TYPE_OID as i32,
            SqlTypeKind::Void => unreachable!("void arrays are unsupported"),
            SqlTypeKind::FdwHandler => unreachable!("fdw_handler arrays are unsupported"),
            SqlTypeKind::Oid => 1028,
            SqlTypeKind::RegProc => REGPROC_ARRAY_TYPE_OID as i32,
            SqlTypeKind::RegClass => REGCLASS_ARRAY_TYPE_OID as i32,
            SqlTypeKind::RegType => REGTYPE_ARRAY_TYPE_OID as i32,
            SqlTypeKind::RegRole => REGROLE_ARRAY_TYPE_OID as i32,
            SqlTypeKind::RegNamespace => REGNAMESPACE_ARRAY_TYPE_OID as i32,
            SqlTypeKind::RegOper => REGOPER_ARRAY_TYPE_OID as i32,
            SqlTypeKind::RegOperator => REGOPERATOR_ARRAY_TYPE_OID as i32,
            SqlTypeKind::RegProcedure => REGPROCEDURE_ARRAY_TYPE_OID as i32,
            SqlTypeKind::RegCollation => REGCOLLATION_ARRAY_TYPE_OID as i32,
            SqlTypeKind::Tid => 1010,
            SqlTypeKind::Xid => 1011,
            SqlTypeKind::Bit => 1561,
            SqlTypeKind::VarBit => 1563,
            SqlTypeKind::Bytea => 1001,
            SqlTypeKind::Uuid => UUID_ARRAY_TYPE_OID as i32,
            SqlTypeKind::Inet => INET_ARRAY_TYPE_OID as i32,
            SqlTypeKind::Cidr => CIDR_ARRAY_TYPE_OID as i32,
            SqlTypeKind::MacAddr => MACADDR_ARRAY_TYPE_OID as i32,
            SqlTypeKind::MacAddr8 => MACADDR8_ARRAY_TYPE_OID as i32,
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
            SqlTypeKind::EventTrigger => unreachable!("event_trigger arrays are unsupported"),
            SqlTypeKind::Record | SqlTypeKind::Composite => RECORD_ARRAY_TYPE_OID as i32,
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
        SqlTypeKind::AnyElement => (ANYELEMENTOID as i32, 4, -1),
        SqlTypeKind::AnyEnum => (ANYENUMOID as i32, 4, -1),
        SqlTypeKind::AnyArray => (2277, -1, -1),
        SqlTypeKind::AnyRange => (ANYRANGEOID as i32, -1, -1),
        SqlTypeKind::AnyMultirange => (ANYMULTIRANGEOID as i32, -1, -1),
        SqlTypeKind::AnyCompatible => (ANYCOMPATIBLEOID as i32, 4, -1),
        SqlTypeKind::AnyCompatibleArray => (ANYCOMPATIBLEARRAYOID as i32, -1, -1),
        SqlTypeKind::AnyCompatibleRange => (ANYCOMPATIBLERANGEOID as i32, -1, -1),
        SqlTypeKind::AnyCompatibleMultirange => (ANYCOMPATIBLEMULTIRANGEOID as i32, -1, -1),
        SqlTypeKind::Trigger => (TRIGGER_TYPE_OID as i32, -1, -1),
        SqlTypeKind::EventTrigger => (EVENT_TRIGGER_TYPE_OID as i32, -1, -1),
        SqlTypeKind::Internal => (INTERNAL_TYPE_OID as i32, -1, -1),
        SqlTypeKind::Shell => (col.sql_type.type_oid as i32, -1, -1),
        SqlTypeKind::Cstring => (CSTRING_TYPE_OID as i32, -2, -1),
        SqlTypeKind::FdwHandler => (FDW_HANDLER_TYPE_OID as i32, 4, -1),
        SqlTypeKind::Record | SqlTypeKind::Composite => {
            (col.sql_type.type_oid as i32, -1, col.sql_type.typmod)
        }
        SqlTypeKind::Enum => (col.sql_type.type_oid as i32, 4, col.sql_type.typmod),
        SqlTypeKind::Int2 => (21, 2, -1),
        SqlTypeKind::Int4 => (23, 4, -1),
        SqlTypeKind::Int8 => (20, 8, -1),
        SqlTypeKind::PgLsn => (PG_LSN_TYPE_OID as i32, 8, -1),
        SqlTypeKind::Void => (VOID_TYPE_OID as i32, 4, -1),
        SqlTypeKind::Oid => (26, 4, -1),
        SqlTypeKind::RegProc => (REGPROC_TYPE_OID as i32, 4, -1),
        SqlTypeKind::RegClass => (REGCLASS_TYPE_OID as i32, 4, -1),
        SqlTypeKind::RegType => (REGTYPE_TYPE_OID as i32, 4, -1),
        SqlTypeKind::RegRole => (REGROLE_TYPE_OID as i32, 4, -1),
        SqlTypeKind::RegNamespace => (REGNAMESPACE_TYPE_OID as i32, 4, -1),
        SqlTypeKind::RegOper => (REGOPER_TYPE_OID as i32, 4, -1),
        SqlTypeKind::RegOperator => (REGOPERATOR_TYPE_OID as i32, 4, -1),
        SqlTypeKind::RegProcedure => (REGPROCEDURE_TYPE_OID as i32, 4, -1),
        SqlTypeKind::RegCollation => (REGCOLLATION_TYPE_OID as i32, 4, -1),
        SqlTypeKind::Tid => (27, 6, -1),
        SqlTypeKind::Xid => (28, 4, -1),
        SqlTypeKind::Bit => (1560, -1, col.sql_type.typmod),
        SqlTypeKind::VarBit => (1562, -1, col.sql_type.typmod),
        SqlTypeKind::Bytea => (17, -1, -1),
        SqlTypeKind::Uuid => (UUID_TYPE_OID as i32, 16, -1),
        SqlTypeKind::Inet => (INET_TYPE_OID as i32, -1, -1),
        SqlTypeKind::Cidr => (CIDR_TYPE_OID as i32, -1, -1),
        SqlTypeKind::MacAddr => (MACADDR_TYPE_OID as i32, 6, -1),
        SqlTypeKind::MacAddr8 => (MACADDR8_TYPE_OID as i32, 8, -1),
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

pub fn send_command_complete(w: &mut impl Write, tag: &str) -> io::Result<()> {
    let len = 4 + tag.len() + 1;
    w.write_all(&[b'C'])?;
    w.write_all(&(len as i32).to_be_bytes())?;
    w.write_all(tag.as_bytes())?;
    w.write_all(&[0])?;
    Ok(())
}

pub fn send_parse_complete(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'1'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

pub fn send_bind_complete(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'2'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

pub fn send_portal_suspended(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b's'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

pub fn send_close_complete(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'3'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

pub fn send_no_data(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'n'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

pub fn send_parameter_description(w: &mut impl Write, type_oids: &[i32]) -> io::Result<()> {
    let len = 4 + 2 + type_oids.len() * 4;
    w.write_all(&[b't'])?;
    w.write_all(&(len as i32).to_be_bytes())?;
    w.write_all(&(type_oids.len() as i16).to_be_bytes())?;
    for oid in type_oids {
        w.write_all(&oid.to_be_bytes())?;
    }
    Ok(())
}

pub fn send_copy_in_response(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'G'])?;
    w.write_all(&7_i32.to_be_bytes())?;
    w.write_all(&[0])?;
    w.write_all(&0_i16.to_be_bytes())?;
    Ok(())
}

pub fn send_copy_out_response(
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

pub fn send_copy_data(w: &mut impl Write, data: &[u8]) -> io::Result<()> {
    w.write_all(&[b'd'])?;
    w.write_all(&((4 + data.len()) as i32).to_be_bytes())?;
    w.write_all(data)?;
    Ok(())
}

pub fn send_copy_done(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'c'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

pub fn send_data_row_body(w: &mut impl Write, body: &[u8]) -> io::Result<()> {
    w.write_all(&[b'D'])?;
    w.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
    w.write_all(body)?;
    Ok(())
}

pub fn begin_data_row_body(buf: &mut Vec<u8>, column_count: usize) {
    buf.clear();
    buf.extend_from_slice(&(column_count as i16).to_be_bytes());
}

pub fn append_data_row_null_field(buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(-1_i32).to_be_bytes());
}

pub fn append_data_row_field(buf: &mut Vec<u8>, payload: &[u8]) {
    buf.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    buf.extend_from_slice(payload);
}

pub fn append_data_row_text_field(buf: &mut Vec<u8>, text: &str) {
    append_data_row_field(buf, text.as_bytes());
}

pub fn begin_binary_record_body(buf: &mut Vec<u8>, field_count: usize) {
    buf.clear();
    buf.extend_from_slice(&(field_count as i32).to_be_bytes());
}

pub fn append_binary_record_field(buf: &mut Vec<u8>, type_oid: u32, payload: Option<&[u8]>) {
    buf.extend_from_slice(&type_oid.to_be_bytes());
    append_binary_optional_payload(buf, payload);
}

pub fn begin_binary_array_body(
    buf: &mut Vec<u8>,
    dimension_count: usize,
    has_null: bool,
    element_type_oid: u32,
) {
    buf.clear();
    buf.extend_from_slice(&(dimension_count as i32).to_be_bytes());
    buf.extend_from_slice(&(i32::from(has_null)).to_be_bytes());
    buf.extend_from_slice(&element_type_oid.to_be_bytes());
}

pub fn append_binary_array_dimension(buf: &mut Vec<u8>, length: usize, lower_bound: i32) {
    buf.extend_from_slice(&(length as i32).to_be_bytes());
    buf.extend_from_slice(&lower_bound.to_be_bytes());
}

pub fn append_binary_array_element(buf: &mut Vec<u8>, payload: Option<&[u8]>) {
    append_binary_optional_payload(buf, payload);
}

fn append_binary_optional_payload(buf: &mut Vec<u8>, payload: Option<&[u8]>) {
    match payload {
        Some(payload) => append_data_row_field(buf, payload),
        None => append_data_row_null_field(buf),
    }
}

pub fn send_notification_response(
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

pub fn send_empty_query(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'I'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

pub fn send_error(
    w: &mut impl Write,
    sqlstate: &str,
    message: &str,
    detail: Option<&str>,
    hint: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    send_error_with_fields(w, sqlstate, message, detail, hint, None, position)
}

pub fn send_error_with_hint(
    w: &mut impl Write,
    sqlstate: &str,
    message: &str,
    hint: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    send_error_with_fields(w, sqlstate, message, None, hint, None, position)
}

pub fn send_error_with_fields(
    w: &mut impl Write,
    sqlstate: &str,
    message: &str,
    detail: Option<&str>,
    hint: Option<&str>,
    context: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    send_error_with_internal_fields(
        w, sqlstate, message, detail, hint, context, position, None, None,
    )
}

pub fn send_error_with_internal_fields(
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
    let mut body = Vec::new();
    push_diagnostic_header(&mut body, "ERROR", sqlstate, message);
    push_optional_diagnostic_fields(
        &mut body,
        detail,
        hint,
        context,
        position,
        internal_query,
        internal_position,
    );

    w.write_all(&[b'E'])?;
    w.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
    w.write_all(&body)?;
    Ok(())
}

pub fn send_notice(
    w: &mut impl Write,
    message: &str,
    detail: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    send_notice_with_fields(w, "NOTICE", "00000", message, detail, None, position)
}

pub fn send_notice_with_severity(
    w: &mut impl Write,
    severity: &str,
    sqlstate: &str,
    message: &str,
    detail: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    send_notice_with_fields(w, severity, sqlstate, message, detail, None, position)
}

pub fn send_notice_with_fields(
    w: &mut impl Write,
    severity: &str,
    sqlstate: &str,
    message: &str,
    detail: Option<&str>,
    hint: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    send_notice_with_context_fields(w, severity, sqlstate, message, detail, hint, None, position)
}

pub fn send_notice_with_context_fields(
    w: &mut impl Write,
    severity: &str,
    sqlstate: &str,
    message: &str,
    detail: Option<&str>,
    hint: Option<&str>,
    context: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    send_notice_with_internal_fields(
        w, severity, sqlstate, message, detail, hint, context, position, None, None,
    )
}

pub fn send_notice_with_internal_fields(
    w: &mut impl Write,
    severity: &str,
    sqlstate: &str,
    message: &str,
    detail: Option<&str>,
    hint: Option<&str>,
    context: Option<&str>,
    position: Option<usize>,
    internal_query: Option<&str>,
    internal_position: Option<usize>,
) -> io::Result<()> {
    let mut body = Vec::new();
    push_diagnostic_header(&mut body, severity, sqlstate, message);
    push_optional_diagnostic_fields(
        &mut body,
        detail,
        hint,
        context,
        position,
        internal_query,
        internal_position,
    );

    w.write_all(&[b'N'])?;
    w.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
    w.write_all(&body)?;
    Ok(())
}

pub fn send_notice_with_hint(
    w: &mut impl Write,
    severity: &str,
    sqlstate: &str,
    message: &str,
    hint: Option<&str>,
    position: Option<usize>,
) -> io::Result<()> {
    send_notice_with_fields(w, severity, sqlstate, message, None, hint, position)
}

fn push_diagnostic_header(body: &mut Vec<u8>, severity: &str, sqlstate: &str, message: &str) {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_row_body_is_framed_with_message_type_and_length() {
        let mut out = Vec::new();
        send_data_row_body(&mut out, &[0, 1, 0, 0, 0, 1, b'x']).unwrap();

        assert_eq!(out, vec![b'D', 0, 0, 0, 11, 0, 1, 0, 0, 0, 1, b'x']);
    }

    #[test]
    fn data_row_body_helpers_prefix_field_lengths() {
        let mut body = Vec::new();
        begin_data_row_body(&mut body, 3);
        append_data_row_text_field(&mut body, "ok");
        append_data_row_field(&mut body, &[1, 2, 3]);
        append_data_row_null_field(&mut body);

        assert_eq!(
            body,
            vec![
                0, 3, 0, 0, 0, 2, b'o', b'k', 0, 0, 0, 3, 1, 2, 3, 255, 255, 255, 255
            ]
        );
    }

    #[test]
    fn binary_record_helpers_write_field_count_oids_and_payloads() {
        let mut body = Vec::new();
        begin_binary_record_body(&mut body, 2);
        append_binary_record_field(&mut body, 23, Some(&[0, 0, 0, 1]));
        append_binary_record_field(&mut body, 25, None);

        assert_eq!(
            body,
            vec![
                0, 0, 0, 2, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 25, 255, 255, 255, 255
            ]
        );
    }

    #[test]
    fn binary_array_helpers_write_header_dimensions_and_elements() {
        let mut body = Vec::new();
        begin_binary_array_body(&mut body, 1, true, 2249);
        append_binary_array_dimension(&mut body, 2, 1);
        append_binary_array_element(&mut body, Some(&[0, 0, 0, 1]));
        append_binary_array_element(&mut body, None);

        assert_eq!(
            body,
            vec![
                0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 8, 201, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0,
                1, 255, 255, 255, 255
            ]
        );
    }

    #[test]
    fn binary_result_format_validation_checks_each_binary_cell() {
        let columns = vec![QueryColumn {
            name: "a".into(),
            sql_type: pgrust_nodes::SqlType::new(SqlTypeKind::Int4),
            wire_type_oid: None,
        }];
        let rows = vec![vec![Value::Int32(1)], vec![Value::Null]];
        let mut encoded = 0;

        validate_binary_result_formats::<()>(
            &rows,
            &columns,
            &[1],
            |_| Ok(()),
            |_, _| {
                encoded += 1;
                Ok(Vec::new())
            },
        )
        .unwrap();

        assert_eq!(encoded, 1);
    }

    #[test]
    fn binary_result_format_validation_rejects_unknown_format_code() {
        let columns = vec![QueryColumn {
            name: "a".into(),
            sql_type: pgrust_nodes::SqlType::new(SqlTypeKind::Int4),
            wire_type_oid: None,
        }];

        assert_eq!(
            validate_binary_result_formats::<()>(
                &[],
                &columns,
                &[2],
                |_| Ok(()),
                |_, _| Ok(Vec::new()),
            ),
            Err(ResultFormatValidationError::UnsupportedResultFormatCode(2))
        );
    }

    #[test]
    fn binary_output_type_policy_matches_supported_shapes() {
        assert!(binary_output_type_supported(pgrust_nodes::SqlType::new(
            SqlTypeKind::Int4
        )));
        assert!(binary_output_type_supported(
            pgrust_nodes::SqlType::array_of(pgrust_nodes::SqlType::new(SqlTypeKind::Record))
        ));
        assert!(!binary_output_type_supported(
            pgrust_nodes::SqlType::array_of(pgrust_nodes::SqlType::new(SqlTypeKind::Int4))
        ));
        assert!(!binary_output_type_supported(pgrust_nodes::SqlType::new(
            SqlTypeKind::Trigger
        )));
    }

    #[test]
    fn query_result_framing_sends_description_rows_and_complete() {
        let columns = vec![QueryColumn {
            name: "a".into(),
            sql_type: pgrust_nodes::SqlType::new(SqlTypeKind::Int4),
            wire_type_oid: None,
        }];
        let rows = vec![vec![Value::Int32(1)], vec![Value::Int32(2)]];
        let mut out = Vec::new();
        let mut sent_rows = 0;

        send_query_result_with_rows(&mut out, &columns, &rows, "SELECT 2", |w, row, buf| {
            sent_rows += 1;
            buf.clear();
            buf.extend_from_slice(&(row.len() as i16).to_be_bytes());
            buf.extend_from_slice(&(-1_i32).to_be_bytes());
            send_data_row_body(w, buf)
        })
        .unwrap();

        assert_eq!(sent_rows, 2);
        assert_eq!(out.first(), Some(&b'T'));
        assert!(
            out.windows(6)
                .any(|window| window == [b'D', 0, 0, 0, 10, 0])
        );
        assert!(
            out.windows(14)
                .any(|window| window == b"C\0\0\0\rSELECT 2\0")
        );
    }
}

fn push_optional_diagnostic_fields(
    body: &mut Vec<u8>,
    detail: Option<&str>,
    hint: Option<&str>,
    context: Option<&str>,
    position: Option<usize>,
    internal_query: Option<&str>,
    internal_position: Option<usize>,
) {
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
    if let Some(internal_position) = internal_position {
        body.push(b'p');
        body.extend_from_slice(internal_position.to_string().as_bytes());
        body.push(0);
    }
    if let Some(internal_query) = internal_query {
        body.push(b'q');
        body.extend_from_slice(internal_query.as_bytes());
        body.push(0);
    }
    body.push(0);
}
