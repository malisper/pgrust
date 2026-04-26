#![allow(dead_code)]

use super::ExecError;
use super::exec_expr::parse_numeric_text;
use super::expr_bit::{coerce_bit_string, render_bit_text};
use super::expr_casts::{
    cast_numeric_value, cast_text_value_with_config, cast_value, cast_value_with_config,
    parse_text_array_literal_with_options, render_internal_char_text,
    render_interval_text_with_config, render_pg_lsn_text,
};
use super::expr_datetime::{render_datetime_value_text, render_datetime_value_text_with_config};
use super::expr_geometry::{
    decode_path_bytes, decode_polygon_bytes, encode_path_bytes, encode_polygon_bytes,
    render_geometry_text,
};
use super::expr_mac::{
    parse_macaddr_bytes, parse_macaddr8_bytes, render_macaddr_text, render_macaddr8_text,
};
use super::expr_multirange::{render_multirange, render_multirange_text_with_config};
use super::expr_network::{encode_network_bytes, parse_cidr_bytes, parse_inet_bytes};
use super::expr_range::{
    decode_range_bytes, encode_range_bytes, render_range_text, render_range_text_with_config,
};
use super::node_types::*;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::expr_json::{canonicalize_jsonpath_text, validate_json_text};
use crate::backend::executor::jsonb::{decode_jsonb, render_jsonb_bytes};
use crate::backend::libpq::pqformat::{FloatFormatOptions, format_float4_text, format_float8_text};
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::record::register_anonymous_record_descriptor;
use crate::include::access::htup::{HeapTuple, TupleValue};
use crate::include::catalog::range_type_ref_for_sql_type;
use crate::include::nodes::execnodes::ToastFetchContext;
use crate::pgrust::compact_string::CompactString;

mod array;

pub(crate) use array::{
    builtin_type_oid_for_sql_type, decode_anyarray_bytes, decode_array_bytes,
    encode_anyarray_bytes, encode_array_bytes, format_array_text, format_array_text_with_config,
};
pub use array::{format_array_value_text, format_array_value_text_with_config};

const INTERNAL_VALUE_TAG_NULL: u8 = 0;
const INTERNAL_VALUE_TAG_INT16: u8 = 1;
const INTERNAL_VALUE_TAG_INT32: u8 = 2;
const INTERNAL_VALUE_TAG_INT64: u8 = 3;
const INTERNAL_VALUE_TAG_MONEY: u8 = 4;
const INTERNAL_VALUE_TAG_DATE: u8 = 5;
const INTERNAL_VALUE_TAG_TIME: u8 = 6;
const INTERNAL_VALUE_TAG_TIMETZ: u8 = 7;
const INTERNAL_VALUE_TAG_TIMESTAMP: u8 = 8;
const INTERNAL_VALUE_TAG_TIMESTAMPTZ: u8 = 9;
const INTERNAL_VALUE_TAG_BIT: u8 = 10;
const INTERNAL_VALUE_TAG_BYTEA: u8 = 11;
const INTERNAL_VALUE_TAG_POINT: u8 = 12;
const INTERNAL_VALUE_TAG_LSEG: u8 = 13;
const INTERNAL_VALUE_TAG_PATH: u8 = 14;
const INTERNAL_VALUE_TAG_LINE: u8 = 15;
const INTERNAL_VALUE_TAG_BOX: u8 = 16;
const INTERNAL_VALUE_TAG_POLYGON: u8 = 17;
const INTERNAL_VALUE_TAG_CIRCLE: u8 = 18;
const INTERNAL_VALUE_TAG_RANGE: u8 = 19;
const INTERNAL_VALUE_TAG_FLOAT64: u8 = 20;
const INTERNAL_VALUE_TAG_NUMERIC: u8 = 21;
const INTERNAL_VALUE_TAG_JSON: u8 = 22;
const INTERNAL_VALUE_TAG_JSONB: u8 = 23;
const INTERNAL_VALUE_TAG_JSONPATH: u8 = 24;
const INTERNAL_VALUE_TAG_XML: u8 = 33;
const INTERNAL_VALUE_TAG_TSVECTOR: u8 = 25;
const INTERNAL_VALUE_TAG_TSQUERY: u8 = 26;
const INTERNAL_VALUE_TAG_TEXT: u8 = 27;
const INTERNAL_VALUE_TAG_INTERNAL_CHAR: u8 = 28;
const INTERNAL_VALUE_TAG_BOOL: u8 = 29;
const INTERNAL_VALUE_TAG_ARRAY: u8 = 30;
const INTERNAL_VALUE_TAG_RECORD: u8 = 31;
const INTERNAL_VALUE_TAG_MULTIRANGE: u8 = 32;
const INTERNAL_VALUE_TAG_INET: u8 = 34;
const INTERNAL_VALUE_TAG_CIDR: u8 = 35;
const INTERNAL_VALUE_TAG_INTERVAL: u8 = 36;
const INTERNAL_VALUE_TAG_UUID: u8 = 37;
const INTERNAL_VALUE_TAG_PG_LSN: u8 = 38;
const INTERNAL_VALUE_TAG_MACADDR: u8 = 39;
const INTERNAL_VALUE_TAG_MACADDR8: u8 = 40;
const INTERNAL_VALUE_TAG_ENUM: u8 = 41;
const COMPOSITE_DATUM_VERSION: u8 = 1;

pub fn render_uuid_text(value: &[u8; 16]) -> String {
    let mut out = String::with_capacity(36);
    for (idx, byte) in value.iter().enumerate() {
        if matches!(idx, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

pub(crate) fn format_record_text(record: &crate::include::nodes::datum::RecordValue) -> String {
    format_record_text_with_options(record, &FloatFormatOptions::default())
}

pub(crate) fn format_record_text_with_config(
    record: &crate::include::nodes::datum::RecordValue,
    datetime_config: &DateTimeConfig,
) -> String {
    format_record_text_with_options(
        record,
        &FloatFormatOptions {
            datetime_config: datetime_config.clone(),
            ..FloatFormatOptions::default()
        },
    )
}

pub(crate) fn format_record_text_with_options(
    record: &crate::include::nodes::datum::RecordValue,
    float_format: &FloatFormatOptions,
) -> String {
    let mut out = String::from("(");
    for (index, (field, value)) in record.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        if matches!(value, Value::Null) {
            continue;
        }
        let rendered = match value {
            Value::Record(record) => format_record_text_with_options(record, float_format),
            Value::PgArray(array) => {
                format_array_value_text_with_config(array, &float_format.datetime_config)
            }
            Value::Array(values) => {
                format_array_text_with_config(values, &float_format.datetime_config)
            }
            Value::Range(_) => render_range_text_with_config(value, &float_format.datetime_config)
                .unwrap_or_default(),
            Value::InternalChar(byte) => render_internal_char_text(*byte),
            Value::Jsonb(bytes) => render_jsonb_bytes(bytes).unwrap_or_default(),
            other => {
                if let Some(text) = other.as_text() {
                    text.to_string()
                } else {
                    render_datetime_value_text_with_config(other, &float_format.datetime_config)
                        .or_else(|| render_geometry_text(other, float_format.clone()))
                        .unwrap_or_else(|| match other {
                            Value::Bool(true) => "t".to_string(),
                            Value::Bool(false) => "f".to_string(),
                            Value::Int16(v) => v.to_string(),
                            Value::Int32(v) => v.to_string(),
                            Value::Int64(v) => v.to_string(),
                            Value::Xid8(v) => v.to_string(),
                            Value::Money(v) => v.to_string(),
                            Value::Float64(v) => match field.sql_type.kind {
                                SqlTypeKind::Float4 => format_float4_text(*v, float_format.clone()),
                                SqlTypeKind::Float8 => format_float8_text(*v, float_format.clone()),
                                _ => v.to_string(),
                            },
                            Value::Numeric(v) => v.render(),
                            Value::Interval(v) => {
                                render_interval_text_with_config(*v, &float_format.datetime_config)
                            }
                            Value::Bytea(v) => {
                                let mut rendered = String::from("\\\\x");
                                for byte in v {
                                    rendered.push_str(&format!("{byte:02x}"));
                                }
                                rendered
                            }
                            Value::Uuid(v) => render_uuid_text(v),
                            Value::Inet(v) => v.render_inet(),
                            Value::Cidr(v) => v.render_cidr(),
                            Value::MacAddr(v) => render_macaddr_text(v),
                            Value::MacAddr8(v) => render_macaddr8_text(v),
                            Value::Bit(v) => v.render(),
                            Value::TsVector(v) => crate::backend::executor::render_tsvector_text(v),
                            Value::TsQuery(v) => crate::backend::executor::render_tsquery_text(v),
                            Value::Json(v) => v.to_string(),
                            Value::JsonPath(v) => v.to_string(),
                            Value::Xml(v) => v.to_string(),
                            Value::Null => String::new(),
                            _ => format!("{other:?}"),
                        })
                }
            }
        };
        let needs_quotes = rendered.is_empty()
            || rendered
                .chars()
                .any(|ch| matches!(ch, '"' | '\\' | '(' | ')' | ',') || ch.is_ascii_whitespace());
        if needs_quotes {
            out.push('"');
        }
        for ch in rendered.chars() {
            if matches!(ch, '"' | '\\') {
                out.push(ch);
            }
            out.push(ch);
        }
        if needs_quotes {
            out.push('"');
        }
    }
    out.push(')');
    out
}

pub(crate) fn format_failing_row_detail(
    values: &[Value],
    datetime_config: &DateTimeConfig,
) -> String {
    let body = values
        .iter()
        .map(|value| format_failing_row_value(value, datetime_config))
        .collect::<Vec<_>>()
        .join(", ");
    format!("Failing row contains ({body}).")
}

pub(crate) fn format_unique_key_detail(columns: &[ColumnDesc], values: &[Value]) -> String {
    let datetime_config = DateTimeConfig::default();
    let names = columns
        .iter()
        .take(values.len())
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let body = values
        .iter()
        .map(|value| format_failing_row_value(value, &datetime_config))
        .collect::<Vec<_>>()
        .join(", ");
    format!("Key ({names})=({body}) already exists.")
}

pub(crate) fn format_exclusion_key_detail(
    columns: &[ColumnDesc],
    proposed: &[Value],
    existing: &[Value],
) -> String {
    format_exclusion_key_detail_with_config(columns, proposed, existing, &DateTimeConfig::default())
}

pub(crate) fn format_exclusion_key_detail_with_config(
    columns: &[ColumnDesc],
    proposed: &[Value],
    existing: &[Value],
    datetime_config: &DateTimeConfig,
) -> String {
    format_exclusion_key_detail_with_existing_label(
        columns,
        proposed,
        existing,
        true,
        datetime_config,
    )
}

pub(crate) fn format_exclusion_create_key_detail(
    columns: &[ColumnDesc],
    proposed: &[Value],
    existing: &[Value],
) -> String {
    format_exclusion_create_key_detail_with_config(
        columns,
        proposed,
        existing,
        &DateTimeConfig::default(),
    )
}

pub(crate) fn format_exclusion_create_key_detail_with_config(
    columns: &[ColumnDesc],
    proposed: &[Value],
    existing: &[Value],
    datetime_config: &DateTimeConfig,
) -> String {
    format_exclusion_key_detail_with_existing_label(
        columns,
        proposed,
        existing,
        false,
        datetime_config,
    )
}

fn format_exclusion_key_detail_with_existing_label(
    columns: &[ColumnDesc],
    proposed: &[Value],
    existing: &[Value],
    existing_label: bool,
    datetime_config: &DateTimeConfig,
) -> String {
    let names = columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let proposed = proposed
        .iter()
        .take(columns.len())
        .map(|value| format_failing_row_value(value, datetime_config))
        .collect::<Vec<_>>()
        .join(", ");
    let existing = existing
        .iter()
        .take(columns.len())
        .map(|value| format_failing_row_value(value, datetime_config))
        .collect::<Vec<_>>()
        .join(", ");
    if existing_label {
        format!("Key ({names})=({proposed}) conflicts with existing key ({names})=({existing}).")
    } else {
        format!("Key ({names})=({proposed}) conflicts with key ({names})=({existing}).")
    }
}

fn format_failing_row_value(value: &Value, datetime_config: &DateTimeConfig) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::EnumOid(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Xid8(v) => v.to_string(),
        Value::Money(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Bool(true) => "t".to_string(),
        Value::Bool(false) => "f".to_string(),
        Value::Numeric(v) => v.render(),
        Value::Interval(v) => render_interval_text_with_config(*v, datetime_config),
        Value::Text(text) => text.to_string(),
        Value::TextRef(_, _) => value.as_text().unwrap_or_default().to_string(),
        Value::Json(text) => text.to_string(),
        Value::JsonPath(text) => text.to_string(),
        Value::Xml(text) => text.to_string(),
        Value::Bytea(bytes) => {
            let mut rendered = String::from("\\\\x");
            for byte in bytes {
                rendered.push_str(&format!("{byte:02x}"));
            }
            rendered
        }
        Value::Uuid(v) => render_uuid_text(v),
        Value::Inet(v) => v.render_inet(),
        Value::Cidr(v) => v.render_cidr(),
        Value::MacAddr(v) => render_macaddr_text(v),
        Value::MacAddr8(v) => render_macaddr8_text(v),
        Value::InternalChar(byte) => render_internal_char_text(*byte),
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => {
            render_datetime_value_text_with_config(value, datetime_config).unwrap_or_default()
        }
        Value::Range(_) => {
            render_range_text_with_config(value, datetime_config).unwrap_or_default()
        }
        Value::Multirange(_) => {
            render_multirange_text_with_config(value, datetime_config).unwrap_or_default()
        }
        Value::Bit(bits) => render_bit_text(bits),
        Value::Jsonb(bytes) => render_jsonb_bytes(bytes).unwrap_or_default(),
        Value::TsVector(vector) => crate::backend::executor::render_tsvector_text(vector),
        Value::TsQuery(query) => crate::backend::executor::render_tsquery_text(query),
        Value::PgLsn(value) => render_pg_lsn_text(*value),
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => {
            render_geometry_text(value, FloatFormatOptions::default()).unwrap_or_default()
        }
        Value::Array(values) => format_array_text_with_config(values, datetime_config),
        Value::PgArray(array) => format_array_value_text_with_config(array, datetime_config),
        Value::Record(record) => format_record_text_with_config(record, datetime_config),
    }
}

fn encode_internal_text(text: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(text.len() as u32).to_le_bytes());
    out.extend_from_slice(text);
}

fn decode_internal_text<'a>(bytes: &'a [u8], offset: &mut usize) -> Result<&'a [u8], ExecError> {
    if *offset + 4 > bytes.len() {
        return Err(ExecError::InvalidStorageValue {
            column: "<record>".into(),
            details: "truncated internal value length".into(),
        });
    }
    let len = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap()) as usize;
    *offset += 4;
    if *offset + len > bytes.len() {
        return Err(ExecError::InvalidStorageValue {
            column: "<record>".into(),
            details: "truncated internal value payload".into(),
        });
    }
    let slice = &bytes[*offset..*offset + len];
    *offset += len;
    Ok(slice)
}

fn sql_type_kind_tag(kind: SqlTypeKind) -> u8 {
    match kind {
        SqlTypeKind::AnyArray => 0,
        SqlTypeKind::AnyElement => 55,
        SqlTypeKind::AnyRange => 56,
        SqlTypeKind::AnyMultirange => 57,
        SqlTypeKind::AnyCompatible => 58,
        SqlTypeKind::AnyCompatibleArray => 59,
        SqlTypeKind::AnyCompatibleRange => 60,
        SqlTypeKind::AnyCompatibleMultirange => 61,
        SqlTypeKind::AnyEnum => 70,
        SqlTypeKind::Enum => 71,
        SqlTypeKind::Record => 1,
        SqlTypeKind::Composite => 2,
        SqlTypeKind::Internal => 64,
        SqlTypeKind::Shell => 78,
        SqlTypeKind::Cstring => 79,
        SqlTypeKind::Trigger => 54,
        SqlTypeKind::Void => 51,
        SqlTypeKind::FdwHandler => 69,
        SqlTypeKind::Int2 => 3,
        SqlTypeKind::Int2Vector => 4,
        SqlTypeKind::Int4 => 5,
        SqlTypeKind::Int8 => 6,
        SqlTypeKind::Name => 7,
        SqlTypeKind::Oid => 8,
        SqlTypeKind::RegProc => 73,
        SqlTypeKind::RegClass => 8,
        SqlTypeKind::RegType => 63,
        SqlTypeKind::RegRole => 55,
        SqlTypeKind::RegNamespace => 8,
        SqlTypeKind::RegOper => 74,
        SqlTypeKind::RegOperator => 66,
        SqlTypeKind::RegProcedure => 52,
        SqlTypeKind::RegCollation => 75,
        SqlTypeKind::Tid => 9,
        SqlTypeKind::Xid => 10,
        SqlTypeKind::OidVector => 11,
        SqlTypeKind::Bit => 12,
        SqlTypeKind::VarBit => 13,
        SqlTypeKind::Bytea => 14,
        SqlTypeKind::Inet => 67,
        SqlTypeKind::Cidr => 68,
        SqlTypeKind::MacAddr => 76,
        SqlTypeKind::MacAddr8 => 77,
        SqlTypeKind::Float4 => 15,
        SqlTypeKind::Float8 => 16,
        SqlTypeKind::Money => 17,
        SqlTypeKind::Numeric => 18,
        SqlTypeKind::Range => 53,
        SqlTypeKind::Int4Range => 19,
        SqlTypeKind::Int8Range => 20,
        SqlTypeKind::NumericRange => 21,
        SqlTypeKind::DateRange => 22,
        SqlTypeKind::TimestampRange => 23,
        SqlTypeKind::TimestampTzRange => 24,
        SqlTypeKind::Multirange => 62,
        SqlTypeKind::Json => 25,
        SqlTypeKind::Jsonb => 26,
        SqlTypeKind::JsonPath => 27,
        SqlTypeKind::Xml => 56,
        SqlTypeKind::Date => 28,
        SqlTypeKind::Time => 29,
        SqlTypeKind::TimeTz => 30,
        SqlTypeKind::Interval => 31,
        SqlTypeKind::Uuid => 70,
        SqlTypeKind::TsVector => 32,
        SqlTypeKind::TsQuery => 33,
        SqlTypeKind::PgLsn => 71,
        SqlTypeKind::RegConfig => 34,
        SqlTypeKind::RegDictionary => 35,
        SqlTypeKind::Text => 36,
        SqlTypeKind::Bool => 37,
        SqlTypeKind::Point => 38,
        SqlTypeKind::Lseg => 39,
        SqlTypeKind::Path => 40,
        SqlTypeKind::Box => 41,
        SqlTypeKind::Polygon => 42,
        SqlTypeKind::Line => 43,
        SqlTypeKind::Circle => 44,
        SqlTypeKind::Varchar => 45,
        SqlTypeKind::Char => 46,
        SqlTypeKind::Timestamp => 47,
        SqlTypeKind::TimestampTz => 48,
        SqlTypeKind::InternalChar => 49,
        SqlTypeKind::PgNodeTree => 50,
    }
}

fn canonical_sql_type_identity(sql_type: SqlType) -> SqlType {
    if let Some(range_type) = range_type_ref_for_sql_type(sql_type) {
        let mut canonical = range_type.sql_type.with_typmod(sql_type.typmod);
        canonical.is_array = sql_type.is_array;
        canonical.type_oid = if sql_type.is_array {
            sql_type.type_oid
        } else if sql_type.type_oid != 0 {
            sql_type.type_oid
        } else {
            canonical.type_oid
        };
        canonical.typrelid = sql_type.typrelid;
        return canonical;
    }
    if let Some(multirange_type) =
        crate::include::catalog::multirange_type_ref_for_sql_type(sql_type)
    {
        let mut canonical = multirange_type.sql_type.with_typmod(sql_type.typmod);
        canonical.is_array = sql_type.is_array;
        canonical.type_oid = if sql_type.is_array {
            sql_type.type_oid
        } else if sql_type.type_oid != 0 {
            sql_type.type_oid
        } else {
            canonical.type_oid
        };
        canonical.typrelid = sql_type.typrelid;
        return canonical;
    }
    sql_type
}

fn sql_type_kind_from_tag(tag: u8) -> Result<SqlTypeKind, ExecError> {
    Ok(match tag {
        0 => SqlTypeKind::AnyArray,
        55 => SqlTypeKind::AnyElement,
        56 => SqlTypeKind::AnyRange,
        57 => SqlTypeKind::AnyMultirange,
        58 => SqlTypeKind::AnyCompatible,
        59 => SqlTypeKind::AnyCompatibleArray,
        60 => SqlTypeKind::AnyCompatibleRange,
        61 => SqlTypeKind::AnyCompatibleMultirange,
        1 => SqlTypeKind::Record,
        2 => SqlTypeKind::Composite,
        64 => SqlTypeKind::Internal,
        78 => SqlTypeKind::Shell,
        79 => SqlTypeKind::Cstring,
        54 => SqlTypeKind::Trigger,
        51 => SqlTypeKind::Void,
        69 => SqlTypeKind::FdwHandler,
        3 => SqlTypeKind::Int2,
        4 => SqlTypeKind::Int2Vector,
        5 => SqlTypeKind::Int4,
        6 => SqlTypeKind::Int8,
        7 => SqlTypeKind::Name,
        8 => SqlTypeKind::Oid,
        73 => SqlTypeKind::RegProc,
        63 => SqlTypeKind::RegType,
        74 => SqlTypeKind::RegOper,
        66 => SqlTypeKind::RegOperator,
        52 => SqlTypeKind::RegProcedure,
        75 => SqlTypeKind::RegCollation,
        9 => SqlTypeKind::Tid,
        10 => SqlTypeKind::Xid,
        11 => SqlTypeKind::OidVector,
        12 => SqlTypeKind::Bit,
        13 => SqlTypeKind::VarBit,
        14 => SqlTypeKind::Bytea,
        67 => SqlTypeKind::Inet,
        68 => SqlTypeKind::Cidr,
        76 => SqlTypeKind::MacAddr,
        77 => SqlTypeKind::MacAddr8,
        15 => SqlTypeKind::Float4,
        16 => SqlTypeKind::Float8,
        17 => SqlTypeKind::Money,
        18 => SqlTypeKind::Numeric,
        53 => SqlTypeKind::Range,
        19 => SqlTypeKind::Int4Range,
        20 => SqlTypeKind::Int8Range,
        21 => SqlTypeKind::NumericRange,
        22 => SqlTypeKind::DateRange,
        23 => SqlTypeKind::TimestampRange,
        24 => SqlTypeKind::TimestampTzRange,
        62 => SqlTypeKind::Multirange,
        25 => SqlTypeKind::Json,
        26 => SqlTypeKind::Jsonb,
        27 => SqlTypeKind::JsonPath,
        65 => SqlTypeKind::Xml,
        28 => SqlTypeKind::Date,
        29 => SqlTypeKind::Time,
        30 => SqlTypeKind::TimeTz,
        31 => SqlTypeKind::Interval,
        70 => SqlTypeKind::Uuid,
        32 => SqlTypeKind::TsVector,
        33 => SqlTypeKind::TsQuery,
        71 => SqlTypeKind::PgLsn,
        34 => SqlTypeKind::RegConfig,
        35 => SqlTypeKind::RegDictionary,
        36 => SqlTypeKind::Text,
        37 => SqlTypeKind::Bool,
        38 => SqlTypeKind::Point,
        39 => SqlTypeKind::Lseg,
        40 => SqlTypeKind::Path,
        41 => SqlTypeKind::Box,
        42 => SqlTypeKind::Polygon,
        43 => SqlTypeKind::Line,
        44 => SqlTypeKind::Circle,
        45 => SqlTypeKind::Varchar,
        46 => SqlTypeKind::Char,
        47 => SqlTypeKind::Timestamp,
        48 => SqlTypeKind::TimestampTz,
        49 => SqlTypeKind::InternalChar,
        50 => SqlTypeKind::PgNodeTree,
        _ => {
            return Err(ExecError::InvalidStorageValue {
                column: "<record>".into(),
                details: format!("unknown composite sql type tag {tag}"),
            });
        }
    })
}

fn encode_sql_type_identity(sql_type: SqlType, out: &mut Vec<u8>) {
    let sql_type = canonical_sql_type_identity(sql_type);
    out.push(sql_type_kind_tag(sql_type.kind));
    out.extend_from_slice(&sql_type.typmod.to_le_bytes());
    out.push(u8::from(sql_type.is_array));
    out.extend_from_slice(&sql_type.type_oid.to_le_bytes());
    out.extend_from_slice(&sql_type.typrelid.to_le_bytes());
    out.extend_from_slice(&sql_type.range_subtype_oid.to_le_bytes());
    out.extend_from_slice(&sql_type.range_multitype_oid.to_le_bytes());
    out.push(u8::from(sql_type.range_discrete));
    out.extend_from_slice(&sql_type.multirange_range_oid.to_le_bytes());
}

fn decode_sql_type_identity(bytes: &[u8], offset: &mut usize) -> Result<SqlType, ExecError> {
    if *offset + 27 > bytes.len() {
        return Err(ExecError::InvalidStorageValue {
            column: "<record>".into(),
            details: "truncated composite field type".into(),
        });
    }
    let kind = sql_type_kind_from_tag(bytes[*offset])?;
    *offset += 1;
    let typmod = i32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    let is_array = bytes[*offset] != 0;
    *offset += 1;
    let type_oid = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    let typrelid = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    let range_subtype_oid = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    let range_multitype_oid = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    let range_discrete = bytes[*offset] != 0;
    *offset += 1;
    let multirange_range_oid = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    Ok(SqlType {
        kind,
        typmod,
        is_array,
        type_oid,
        typrelid,
        range_subtype_oid,
        range_multitype_oid,
        range_discrete,
        multirange_range_oid,
    })
}

fn record_relation_desc(
    descriptor: &crate::include::nodes::datum::RecordDescriptor,
) -> RelationDesc {
    RelationDesc {
        columns: descriptor
            .fields
            .iter()
            .map(|field| column_desc(field.name.clone(), field.sql_type, true))
            .collect(),
    }
}

fn encode_composite_datum(
    record: &crate::include::nodes::datum::RecordValue,
) -> Result<Vec<u8>, ExecError> {
    let desc = record_relation_desc(&record.descriptor);
    let tuple = tuple_from_values(&desc, &record.fields)?;
    let tuple_bytes = tuple.serialize();

    let mut out = Vec::new();
    out.push(COMPOSITE_DATUM_VERSION);
    out.extend_from_slice(&record.type_oid().to_le_bytes());
    out.extend_from_slice(&record.typrelid().to_le_bytes());
    out.extend_from_slice(&record.typmod().to_le_bytes());
    out.extend_from_slice(&(record.descriptor.fields.len() as u32).to_le_bytes());
    for field in &record.descriptor.fields {
        encode_internal_text(field.name.as_bytes(), &mut out);
        encode_sql_type_identity(field.sql_type, &mut out);
    }
    encode_internal_text(&tuple_bytes, &mut out);
    Ok(out)
}

fn decode_composite_datum(
    bytes: &[u8],
) -> Result<crate::include::nodes::datum::RecordValue, ExecError> {
    let mut offset = 0usize;
    let version = *bytes
        .get(offset)
        .ok_or_else(|| ExecError::InvalidStorageValue {
            column: "<record>".into(),
            details: "missing composite datum version".into(),
        })?;
    if version != COMPOSITE_DATUM_VERSION {
        return Err(ExecError::InvalidStorageValue {
            column: "<record>".into(),
            details: format!("unsupported composite datum version {version}"),
        });
    }
    offset += 1;
    if offset + 16 > bytes.len() {
        return Err(ExecError::InvalidStorageValue {
            column: "<record>".into(),
            details: "truncated composite datum header".into(),
        });
    }
    let type_oid = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let typrelid = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let typmod = i32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let field_count = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let mut fields = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        let name = std::str::from_utf8(decode_internal_text(bytes, &mut offset)?)
            .unwrap_or_default()
            .to_string();
        let sql_type = decode_sql_type_identity(bytes, &mut offset)?;
        fields.push((name, sql_type));
    }
    let tuple_payload = decode_internal_text(bytes, &mut offset)?;
    let descriptor = if typrelid != 0 {
        crate::include::nodes::datum::RecordDescriptor::named(type_oid, typrelid, typmod, fields)
    } else {
        crate::include::nodes::datum::RecordDescriptor::anonymous(fields, typmod)
    };
    if descriptor.typrelid == 0 {
        register_anonymous_record_descriptor(&descriptor);
    }

    let relation_desc = record_relation_desc(&descriptor);
    let tuple = HeapTuple::parse(tuple_payload).map_err(ExecError::from)?;
    let raw_values = tuple
        .deform(&relation_desc.attribute_descs())
        .map_err(ExecError::from)?;
    let values = relation_desc
        .columns
        .iter()
        .zip(raw_values.iter())
        .map(|(column, raw)| decode_value(column, *raw))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(crate::include::nodes::datum::RecordValue::from_descriptor(
        descriptor, values,
    ))
}

fn encode_internal_array(
    array: &crate::include::nodes::datum::ArrayValue,
) -> Result<Vec<u8>, ExecError> {
    let mut out = Vec::new();
    match array.element_type_oid {
        Some(oid) => {
            out.push(1);
            out.extend_from_slice(&oid.to_le_bytes());
        }
        None => out.push(0),
    }
    out.extend_from_slice(&(array.dimensions.len() as u32).to_le_bytes());
    for dim in &array.dimensions {
        out.extend_from_slice(&dim.lower_bound.to_le_bytes());
        out.extend_from_slice(&(dim.length as u32).to_le_bytes());
    }
    out.extend_from_slice(&(array.elements.len() as u32).to_le_bytes());
    for element in &array.elements {
        let payload = encode_internal_value(element)?;
        encode_internal_text(&payload, &mut out);
    }
    Ok(out)
}

fn decode_internal_array(
    bytes: &[u8],
) -> Result<crate::include::nodes::datum::ArrayValue, ExecError> {
    let mut offset = 0usize;
    if offset >= bytes.len() {
        return Err(ExecError::InvalidStorageValue {
            column: "<record>".into(),
            details: "missing internal array header".into(),
        });
    }
    let has_oid = bytes[offset];
    offset += 1;
    let element_type_oid = if has_oid == 1 {
        if offset + 4 > bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: "<record>".into(),
                details: "truncated internal array element oid".into(),
            });
        }
        let oid = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        Some(oid)
    } else {
        None
    };
    if offset + 4 > bytes.len() {
        return Err(ExecError::InvalidStorageValue {
            column: "<record>".into(),
            details: "truncated internal array dimension count".into(),
        });
    }
    let ndim = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let mut dimensions = Vec::with_capacity(ndim);
    for _ in 0..ndim {
        if offset + 8 > bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: "<record>".into(),
                details: "truncated internal array dimension".into(),
            });
        }
        let lower_bound = i32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let length = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
        offset += 8;
        dimensions.push(crate::include::nodes::datum::ArrayDimension {
            lower_bound,
            length,
        });
    }
    if offset + 4 > bytes.len() {
        return Err(ExecError::InvalidStorageValue {
            column: "<record>".into(),
            details: "truncated internal array element count".into(),
        });
    }
    let count = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let mut elements = Vec::with_capacity(count);
    for _ in 0..count {
        let payload = decode_internal_text(bytes, &mut offset)?;
        elements.push(decode_internal_value(payload)?);
    }
    Ok(crate::include::nodes::datum::ArrayValue {
        element_type_oid,
        dimensions,
        elements,
    })
}

fn encode_internal_record(
    record: &crate::include::nodes::datum::RecordValue,
) -> Result<Vec<u8>, ExecError> {
    encode_composite_datum(record)
}

fn decode_internal_record(
    bytes: &[u8],
) -> Result<crate::include::nodes::datum::RecordValue, ExecError> {
    decode_composite_datum(bytes)
}

fn encode_internal_value(value: &Value) -> Result<Vec<u8>, ExecError> {
    let mut out = Vec::new();
    match value.to_owned_value() {
        Value::Null => out.push(INTERNAL_VALUE_TAG_NULL),
        Value::Int16(v) => {
            out.push(INTERNAL_VALUE_TAG_INT16);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Int32(v) => {
            out.push(INTERNAL_VALUE_TAG_INT32);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::EnumOid(v) => {
            out.push(INTERNAL_VALUE_TAG_ENUM);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Int64(v) => {
            out.push(INTERNAL_VALUE_TAG_INT64);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Xid8(v) => {
            out.push(INTERNAL_VALUE_TAG_INT64);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Money(v) => {
            out.push(INTERNAL_VALUE_TAG_MONEY);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Date(v) => {
            out.push(INTERNAL_VALUE_TAG_DATE);
            out.extend_from_slice(&v.0.to_le_bytes());
        }
        Value::Time(v) => {
            out.push(INTERNAL_VALUE_TAG_TIME);
            out.extend_from_slice(&v.0.to_le_bytes());
        }
        Value::TimeTz(v) => {
            out.push(INTERNAL_VALUE_TAG_TIMETZ);
            out.extend_from_slice(&v.time.0.to_le_bytes());
            out.extend_from_slice(&v.offset_seconds.to_le_bytes());
        }
        Value::Timestamp(v) => {
            out.push(INTERNAL_VALUE_TAG_TIMESTAMP);
            out.extend_from_slice(&v.0.to_le_bytes());
        }
        Value::TimestampTz(v) => {
            out.push(INTERNAL_VALUE_TAG_TIMESTAMPTZ);
            out.extend_from_slice(&v.0.to_le_bytes());
        }
        Value::Interval(v) => {
            out.push(INTERNAL_VALUE_TAG_INTERVAL);
            out.extend_from_slice(&v.time_micros.to_le_bytes());
            out.extend_from_slice(&v.days.to_le_bytes());
            out.extend_from_slice(&v.months.to_le_bytes());
        }
        Value::Bit(v) => {
            out.push(INTERNAL_VALUE_TAG_BIT);
            out.extend_from_slice(&v.bit_len.to_le_bytes());
            encode_internal_text(&v.bytes, &mut out);
        }
        Value::Bytea(v) => {
            out.push(INTERNAL_VALUE_TAG_BYTEA);
            encode_internal_text(&v, &mut out);
        }
        Value::Uuid(v) => {
            out.push(INTERNAL_VALUE_TAG_UUID);
            out.extend_from_slice(&v);
        }
        Value::Inet(v) => {
            out.push(INTERNAL_VALUE_TAG_INET);
            encode_internal_text(v.render_inet().as_bytes(), &mut out);
        }
        Value::Cidr(v) => {
            out.push(INTERNAL_VALUE_TAG_CIDR);
            encode_internal_text(v.render_cidr().as_bytes(), &mut out);
        }
        Value::MacAddr(v) => {
            out.push(INTERNAL_VALUE_TAG_MACADDR);
            out.extend_from_slice(&v);
        }
        Value::MacAddr8(v) => {
            out.push(INTERNAL_VALUE_TAG_MACADDR8);
            out.extend_from_slice(&v);
        }
        Value::Point(v) => {
            out.push(INTERNAL_VALUE_TAG_POINT);
            out.extend_from_slice(&v.x.to_le_bytes());
            out.extend_from_slice(&v.y.to_le_bytes());
        }
        Value::Lseg(v) => {
            out.push(INTERNAL_VALUE_TAG_LSEG);
            for point in &v.p {
                out.extend_from_slice(&point.x.to_le_bytes());
                out.extend_from_slice(&point.y.to_le_bytes());
            }
        }
        Value::Path(v) => {
            out.push(INTERNAL_VALUE_TAG_PATH);
            out.push(u8::from(v.closed));
            out.extend_from_slice(&(v.points.len() as u32).to_le_bytes());
            for point in &v.points {
                out.extend_from_slice(&point.x.to_le_bytes());
                out.extend_from_slice(&point.y.to_le_bytes());
            }
        }
        Value::Line(v) => {
            out.push(INTERNAL_VALUE_TAG_LINE);
            out.extend_from_slice(&v.a.to_le_bytes());
            out.extend_from_slice(&v.b.to_le_bytes());
            out.extend_from_slice(&v.c.to_le_bytes());
        }
        Value::Box(v) => {
            out.push(INTERNAL_VALUE_TAG_BOX);
            out.extend_from_slice(&v.high.x.to_le_bytes());
            out.extend_from_slice(&v.high.y.to_le_bytes());
            out.extend_from_slice(&v.low.x.to_le_bytes());
            out.extend_from_slice(&v.low.y.to_le_bytes());
        }
        Value::Polygon(v) => {
            out.push(INTERNAL_VALUE_TAG_POLYGON);
            encode_internal_text(&encode_polygon_bytes(&v), &mut out);
        }
        Value::Circle(v) => {
            out.push(INTERNAL_VALUE_TAG_CIRCLE);
            out.extend_from_slice(&v.center.x.to_le_bytes());
            out.extend_from_slice(&v.center.y.to_le_bytes());
            out.extend_from_slice(&v.radius.to_le_bytes());
        }
        Value::Range(v) => {
            out.push(INTERNAL_VALUE_TAG_RANGE);
            encode_sql_type_identity(v.range_type.sql_type, &mut out);
            encode_internal_text(
                crate::backend::executor::render_range_text(&Value::Range(v))
                    .unwrap_or_default()
                    .as_bytes(),
                &mut out,
            );
        }
        Value::Multirange(v) => {
            out.push(INTERNAL_VALUE_TAG_MULTIRANGE);
            encode_sql_type_identity(v.multirange_type.sql_type, &mut out);
            encode_internal_text(render_multirange(&v).as_bytes(), &mut out);
        }
        Value::Float64(v) => {
            out.push(INTERNAL_VALUE_TAG_FLOAT64);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Numeric(v) => {
            out.push(INTERNAL_VALUE_TAG_NUMERIC);
            encode_internal_text(v.render().as_bytes(), &mut out);
        }
        Value::Json(v) => {
            out.push(INTERNAL_VALUE_TAG_JSON);
            encode_internal_text(v.as_bytes(), &mut out);
        }
        Value::Jsonb(v) => {
            out.push(INTERNAL_VALUE_TAG_JSONB);
            encode_internal_text(&v, &mut out);
        }
        Value::JsonPath(v) => {
            out.push(INTERNAL_VALUE_TAG_JSONPATH);
            encode_internal_text(v.as_bytes(), &mut out);
        }
        Value::Xml(v) => {
            out.push(INTERNAL_VALUE_TAG_XML);
            encode_internal_text(v.as_bytes(), &mut out);
        }
        Value::TsVector(v) => {
            out.push(INTERNAL_VALUE_TAG_TSVECTOR);
            encode_internal_text(
                &crate::backend::executor::encode_tsvector_bytes(&v),
                &mut out,
            );
        }
        Value::TsQuery(v) => {
            out.push(INTERNAL_VALUE_TAG_TSQUERY);
            encode_internal_text(
                &crate::backend::executor::encode_tsquery_bytes(&v),
                &mut out,
            );
        }
        Value::PgLsn(v) => {
            out.push(INTERNAL_VALUE_TAG_PG_LSN);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Text(v) => {
            out.push(INTERNAL_VALUE_TAG_TEXT);
            encode_internal_text(v.as_bytes(), &mut out);
        }
        Value::TextRef(_, _) => unreachable!(),
        Value::InternalChar(v) => {
            out.push(INTERNAL_VALUE_TAG_INTERNAL_CHAR);
            out.push(v);
        }
        Value::Bool(v) => {
            out.push(INTERNAL_VALUE_TAG_BOOL);
            out.push(u8::from(v));
        }
        Value::Array(v) => {
            out.push(INTERNAL_VALUE_TAG_ARRAY);
            encode_internal_text(
                &encode_internal_array(&crate::include::nodes::datum::ArrayValue::from_1d(v))?,
                &mut out,
            );
        }
        Value::PgArray(v) => {
            out.push(INTERNAL_VALUE_TAG_ARRAY);
            encode_internal_text(&encode_internal_array(&v)?, &mut out);
        }
        Value::Record(v) => {
            out.push(INTERNAL_VALUE_TAG_RECORD);
            encode_internal_text(&encode_internal_record(&v)?, &mut out);
        }
    }
    Ok(out)
}

fn decode_internal_value(bytes: &[u8]) -> Result<Value, ExecError> {
    if bytes.is_empty() {
        return Err(ExecError::InvalidStorageValue {
            column: "<record>".into(),
            details: "missing internal value tag".into(),
        });
    }
    let tag = bytes[0];
    let rest = &bytes[1..];
    Ok(match tag {
        INTERNAL_VALUE_TAG_NULL => Value::Null,
        INTERNAL_VALUE_TAG_INT16 => {
            Value::Int16(i16::from_le_bytes(rest.try_into().map_err(|_| {
                ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "invalid int16 record payload".into(),
                }
            })?))
        }
        INTERNAL_VALUE_TAG_INT32 => {
            Value::Int32(i32::from_le_bytes(rest.try_into().map_err(|_| {
                ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "invalid int32 record payload".into(),
                }
            })?))
        }
        INTERNAL_VALUE_TAG_ENUM => {
            Value::EnumOid(u32::from_le_bytes(rest.try_into().map_err(|_| {
                ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "invalid enum record payload".into(),
                }
            })?))
        }
        INTERNAL_VALUE_TAG_INT64 => {
            Value::Int64(i64::from_le_bytes(rest.try_into().map_err(|_| {
                ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "invalid int64 record payload".into(),
                }
            })?))
        }
        INTERNAL_VALUE_TAG_MONEY => {
            Value::Money(i64::from_le_bytes(rest.try_into().map_err(|_| {
                ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "invalid money record payload".into(),
                }
            })?))
        }
        INTERNAL_VALUE_TAG_DATE => Value::Date(crate::include::nodes::datetime::DateADT(
            i32::from_le_bytes(
                rest.try_into()
                    .map_err(|_| ExecError::InvalidStorageValue {
                        column: "<record>".into(),
                        details: "invalid date record payload".into(),
                    })?,
            ),
        )),
        INTERNAL_VALUE_TAG_TIME => Value::Time(crate::include::nodes::datetime::TimeADT(
            i64::from_le_bytes(
                rest.try_into()
                    .map_err(|_| ExecError::InvalidStorageValue {
                        column: "<record>".into(),
                        details: "invalid time record payload".into(),
                    })?,
            ),
        )),
        INTERNAL_VALUE_TAG_TIMETZ => {
            if rest.len() != 12 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "invalid timetz record payload".into(),
                });
            }
            Value::TimeTz(crate::include::nodes::datetime::TimeTzADT {
                time: crate::include::nodes::datetime::TimeADT(i64::from_le_bytes(
                    rest[0..8].try_into().unwrap(),
                )),
                offset_seconds: i32::from_le_bytes(rest[8..12].try_into().unwrap()),
            })
        }
        INTERNAL_VALUE_TAG_TIMESTAMP => Value::Timestamp(
            crate::include::nodes::datetime::TimestampADT(i64::from_le_bytes(
                rest.try_into()
                    .map_err(|_| ExecError::InvalidStorageValue {
                        column: "<record>".into(),
                        details: "invalid timestamp record payload".into(),
                    })?,
            )),
        ),
        INTERNAL_VALUE_TAG_TIMESTAMPTZ => Value::TimestampTz(
            crate::include::nodes::datetime::TimestampTzADT(i64::from_le_bytes(
                rest.try_into()
                    .map_err(|_| ExecError::InvalidStorageValue {
                        column: "<record>".into(),
                        details: "invalid timestamptz record payload".into(),
                    })?,
            )),
        ),
        INTERNAL_VALUE_TAG_INTERVAL => {
            if rest.len() != 16 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "invalid interval record payload".into(),
                });
            }
            Value::Interval(crate::include::nodes::datum::IntervalValue {
                time_micros: i64::from_le_bytes(rest[0..8].try_into().unwrap()),
                days: i32::from_le_bytes(rest[8..12].try_into().unwrap()),
                months: i32::from_le_bytes(rest[12..16].try_into().unwrap()),
            })
        }
        INTERNAL_VALUE_TAG_BIT => {
            if rest.len() < 8 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "invalid bit record payload".into(),
                });
            }
            let bit_len = i32::from_le_bytes(rest[0..4].try_into().unwrap());
            let mut offset = 4usize;
            let bit_bytes = decode_internal_text(rest, &mut offset)?.to_vec();
            Value::Bit(crate::include::nodes::datum::BitString::new(
                bit_len, bit_bytes,
            ))
        }
        INTERNAL_VALUE_TAG_BYTEA => {
            let mut offset = 0usize;
            Value::Bytea(decode_internal_text(rest, &mut offset)?.to_vec())
        }
        INTERNAL_VALUE_TAG_UUID => {
            if rest.len() != 16 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "uuid payload must be 16 bytes".into(),
                });
            }
            Value::Uuid(rest.try_into().unwrap())
        }
        INTERNAL_VALUE_TAG_INET => {
            let mut offset = 0usize;
            Value::Inet(parse_inet_bytes(decode_internal_text(rest, &mut offset)?)?)
        }
        INTERNAL_VALUE_TAG_CIDR => {
            let mut offset = 0usize;
            Value::Cidr(parse_cidr_bytes(decode_internal_text(rest, &mut offset)?)?)
        }
        INTERNAL_VALUE_TAG_MACADDR => Value::MacAddr(parse_macaddr_bytes(rest)?),
        INTERNAL_VALUE_TAG_MACADDR8 => Value::MacAddr8(parse_macaddr8_bytes(rest)?),
        INTERNAL_VALUE_TAG_POINT => {
            if rest.len() != 16 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "invalid point record payload".into(),
                });
            }
            Value::Point(crate::include::nodes::datum::GeoPoint {
                x: f64::from_le_bytes(rest[0..8].try_into().unwrap()),
                y: f64::from_le_bytes(rest[8..16].try_into().unwrap()),
            })
        }
        INTERNAL_VALUE_TAG_LSEG => {
            if rest.len() != 32 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "invalid lseg record payload".into(),
                });
            }
            Value::Lseg(crate::include::nodes::datum::GeoLseg {
                p: [
                    crate::include::nodes::datum::GeoPoint {
                        x: f64::from_le_bytes(rest[0..8].try_into().unwrap()),
                        y: f64::from_le_bytes(rest[8..16].try_into().unwrap()),
                    },
                    crate::include::nodes::datum::GeoPoint {
                        x: f64::from_le_bytes(rest[16..24].try_into().unwrap()),
                        y: f64::from_le_bytes(rest[24..32].try_into().unwrap()),
                    },
                ],
            })
        }
        INTERNAL_VALUE_TAG_PATH => {
            if rest.len() < 5 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "invalid path record payload".into(),
                });
            }
            let closed = rest[0] != 0;
            let count = u32::from_le_bytes(rest[1..5].try_into().unwrap()) as usize;
            if rest.len() != 5 + count * 16 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "invalid path point payload".into(),
                });
            }
            let mut points = Vec::with_capacity(count);
            let mut offset = 5usize;
            for _ in 0..count {
                points.push(crate::include::nodes::datum::GeoPoint {
                    x: f64::from_le_bytes(rest[offset..offset + 8].try_into().unwrap()),
                    y: f64::from_le_bytes(rest[offset + 8..offset + 16].try_into().unwrap()),
                });
                offset += 16;
            }
            Value::Path(crate::include::nodes::datum::GeoPath { closed, points })
        }
        INTERNAL_VALUE_TAG_LINE => {
            if rest.len() != 24 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "invalid line record payload".into(),
                });
            }
            Value::Line(crate::include::nodes::datum::GeoLine {
                a: f64::from_le_bytes(rest[0..8].try_into().unwrap()),
                b: f64::from_le_bytes(rest[8..16].try_into().unwrap()),
                c: f64::from_le_bytes(rest[16..24].try_into().unwrap()),
            })
        }
        INTERNAL_VALUE_TAG_BOX => {
            if rest.len() != 32 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "invalid box record payload".into(),
                });
            }
            Value::Box(crate::include::nodes::datum::GeoBox {
                high: crate::include::nodes::datum::GeoPoint {
                    x: f64::from_le_bytes(rest[0..8].try_into().unwrap()),
                    y: f64::from_le_bytes(rest[8..16].try_into().unwrap()),
                },
                low: crate::include::nodes::datum::GeoPoint {
                    x: f64::from_le_bytes(rest[16..24].try_into().unwrap()),
                    y: f64::from_le_bytes(rest[24..32].try_into().unwrap()),
                },
            })
        }
        INTERNAL_VALUE_TAG_POLYGON => {
            let mut offset = 0usize;
            Value::Polygon(decode_polygon_bytes(decode_internal_text(
                rest,
                &mut offset,
            )?)?)
        }
        INTERNAL_VALUE_TAG_CIRCLE => {
            if rest.len() != 24 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "invalid circle record payload".into(),
                });
            }
            Value::Circle(crate::include::nodes::datum::GeoCircle {
                center: crate::include::nodes::datum::GeoPoint {
                    x: f64::from_le_bytes(rest[0..8].try_into().unwrap()),
                    y: f64::from_le_bytes(rest[8..16].try_into().unwrap()),
                },
                radius: f64::from_le_bytes(rest[16..24].try_into().unwrap()),
            })
        }
        INTERNAL_VALUE_TAG_RANGE => {
            let mut offset = 0usize;
            let sql_type = decode_sql_type_identity(rest, &mut offset)?;
            let text =
                std::str::from_utf8(decode_internal_text(rest, &mut offset)?).unwrap_or_default();
            crate::backend::executor::expr_range::parse_range_text(text, sql_type)?
        }
        INTERNAL_VALUE_TAG_MULTIRANGE => {
            let mut offset = 0usize;
            let sql_type = decode_sql_type_identity(rest, &mut offset)?;
            let text =
                std::str::from_utf8(decode_internal_text(rest, &mut offset)?).unwrap_or_default();
            crate::backend::executor::parse_multirange_text(text, sql_type)?
        }
        INTERNAL_VALUE_TAG_FLOAT64 => {
            Value::Float64(f64::from_le_bytes(rest.try_into().map_err(|_| {
                ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "invalid float record payload".into(),
                }
            })?))
        }
        INTERNAL_VALUE_TAG_NUMERIC => {
            Value::Numeric(crate::include::nodes::datum::NumericValue::from(
                std::str::from_utf8({
                    let mut offset = 0usize;
                    decode_internal_text(rest, &mut offset)?
                })
                .unwrap_or_default(),
            ))
        }
        INTERNAL_VALUE_TAG_JSON => Value::Json(CompactString::new(
            std::str::from_utf8({
                let mut offset = 0usize;
                decode_internal_text(rest, &mut offset)?
            })
            .unwrap_or_default(),
        )),
        INTERNAL_VALUE_TAG_JSONB => Value::Jsonb({
            let mut offset = 0usize;
            decode_internal_text(rest, &mut offset)?.to_vec()
        }),
        INTERNAL_VALUE_TAG_JSONPATH => Value::JsonPath(CompactString::new(
            std::str::from_utf8({
                let mut offset = 0usize;
                decode_internal_text(rest, &mut offset)?
            })
            .unwrap_or_default(),
        )),
        INTERNAL_VALUE_TAG_XML => Value::Xml(CompactString::new(
            std::str::from_utf8({
                let mut offset = 0usize;
                decode_internal_text(rest, &mut offset)?
            })
            .unwrap_or_default(),
        )),
        INTERNAL_VALUE_TAG_TSVECTOR => {
            Value::TsVector(crate::backend::executor::decode_tsvector_bytes({
                let mut offset = 0usize;
                decode_internal_text(rest, &mut offset)?
            })?)
        }
        INTERNAL_VALUE_TAG_TSQUERY => {
            Value::TsQuery(crate::backend::executor::decode_tsquery_bytes({
                let mut offset = 0usize;
                decode_internal_text(rest, &mut offset)?
            })?)
        }
        INTERNAL_VALUE_TAG_PG_LSN => {
            if rest.len() != 8 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "pg_lsn must be exactly 8 bytes".into(),
                });
            }
            Value::PgLsn(u64::from_le_bytes(rest.try_into().unwrap()))
        }
        INTERNAL_VALUE_TAG_TEXT => Value::Text(CompactString::new(
            std::str::from_utf8({
                let mut offset = 0usize;
                decode_internal_text(rest, &mut offset)?
            })
            .unwrap_or_default(),
        )),
        INTERNAL_VALUE_TAG_INTERNAL_CHAR => {
            Value::InternalChar(*rest.first().ok_or_else(|| ExecError::InvalidStorageValue {
                column: "<record>".into(),
                details: "invalid internal char payload".into(),
            })?)
        }
        INTERNAL_VALUE_TAG_BOOL => Value::Bool(rest.first().copied().unwrap_or(0) != 0),
        INTERNAL_VALUE_TAG_ARRAY => {
            let mut offset = 0usize;
            Value::PgArray(decode_internal_array(decode_internal_text(
                rest,
                &mut offset,
            )?)?)
        }
        INTERNAL_VALUE_TAG_RECORD => {
            let mut offset = 0usize;
            Value::Record(decode_internal_record(decode_internal_text(
                rest,
                &mut offset,
            )?)?)
        }
        _ => {
            return Err(ExecError::InvalidStorageValue {
                column: "<record>".into(),
                details: format!("unknown internal value tag {tag}"),
            });
        }
    })
}

pub(crate) fn tuple_from_values(
    desc: &RelationDesc,
    values: &[Value],
) -> Result<HeapTuple, ExecError> {
    let tuple_values = encode_tuple_values(desc, values)?;
    HeapTuple::from_values(&desc.attribute_descs(), &tuple_values).map_err(ExecError::from)
}

pub(crate) fn encode_tuple_values(
    desc: &RelationDesc,
    values: &[Value],
) -> Result<Vec<TupleValue>, ExecError> {
    encode_tuple_values_with_config(desc, values, &DateTimeConfig::default())
}

pub(crate) fn encode_tuple_values_with_config(
    desc: &RelationDesc,
    values: &[Value],
    datetime_config: &DateTimeConfig,
) -> Result<Vec<TupleValue>, ExecError> {
    desc.columns
        .iter()
        .zip(values.iter())
        .map(|(column, value)| encode_value_with_config(column, value, datetime_config))
        .collect::<Result<Vec<_>, _>>()
}

pub(crate) fn encode_value(column: &ColumnDesc, value: &Value) -> Result<TupleValue, ExecError> {
    encode_value_with_config(column, value, &DateTimeConfig::default())
}

pub(crate) fn encode_value_with_config(
    column: &ColumnDesc,
    value: &Value,
    datetime_config: &DateTimeConfig,
) -> Result<TupleValue, ExecError> {
    if matches!(value, Value::Null) {
        return if !column.storage.nullable {
            Err(ExecError::MissingRequiredColumn(column.name.clone()))
        } else {
            Ok(TupleValue::Null)
        };
    }

    let coerced = coerce_assignment_value_with_config(value, column.sql_type, datetime_config)?;
    match (&column.ty, coerced) {
        (ScalarType::Int16, Value::Int16(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Int32, Value::Int32(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Enum, Value::EnumOid(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Int32, Value::Int64(v))
            if matches!(
                column.sql_type.kind,
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
            let oid = u32::try_from(v).map_err(|_| ExecError::OidOutOfRange)?;
            Ok(TupleValue::Bytes(oid.to_le_bytes().to_vec()))
        }
        (ScalarType::Int64, Value::Int64(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Int64, Value::Xid8(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Date, Value::Date(v)) => Ok(TupleValue::Bytes(v.0.to_le_bytes().to_vec())),
        (ScalarType::Time, Value::Time(v)) => Ok(TupleValue::Bytes(v.0.to_le_bytes().to_vec())),
        (ScalarType::TimeTz, Value::TimeTz(v)) => {
            let mut bytes = Vec::with_capacity(12);
            bytes.extend_from_slice(&v.time.0.to_le_bytes());
            bytes.extend_from_slice(&v.offset_seconds.to_le_bytes());
            Ok(TupleValue::Bytes(bytes))
        }
        (ScalarType::Timestamp, Value::Timestamp(v)) => {
            Ok(TupleValue::Bytes(v.0.to_le_bytes().to_vec()))
        }
        (ScalarType::TimestampTz, Value::TimestampTz(v)) => {
            Ok(TupleValue::Bytes(v.0.to_le_bytes().to_vec()))
        }
        (ScalarType::Interval, Value::Interval(v)) => {
            let mut bytes = Vec::with_capacity(16);
            bytes.extend_from_slice(&v.time_micros.to_le_bytes());
            bytes.extend_from_slice(&v.days.to_le_bytes());
            bytes.extend_from_slice(&v.months.to_le_bytes());
            Ok(TupleValue::Bytes(bytes))
        }
        (ScalarType::Range(_), Value::Range(range)) => {
            Ok(TupleValue::Bytes(encode_range_bytes(&range)?))
        }
        (ScalarType::Multirange(_), Value::Multirange(multirange)) => Ok(TupleValue::Bytes(
            crate::backend::executor::encode_multirange_bytes(&multirange)?,
        )),
        (ScalarType::BitString, Value::Bit(v)) => {
            let mut bytes = Vec::with_capacity(4 + v.bytes.len());
            bytes.extend_from_slice(&(v.bit_len as u32).to_le_bytes());
            bytes.extend_from_slice(&v.bytes);
            Ok(TupleValue::Bytes(bytes))
        }
        (ScalarType::Bytea, Value::Bytea(v)) => Ok(TupleValue::Bytes(v)),
        (ScalarType::Uuid, Value::Uuid(v)) => Ok(TupleValue::Bytes(v.to_vec())),
        (ScalarType::Inet, Value::Inet(v)) => {
            Ok(TupleValue::Bytes(encode_network_bytes(&v, false)))
        }
        (ScalarType::Cidr, Value::Cidr(v)) => Ok(TupleValue::Bytes(encode_network_bytes(&v, true))),
        (ScalarType::MacAddr, Value::MacAddr(v)) => Ok(TupleValue::Bytes(v.to_vec())),
        (ScalarType::MacAddr8, Value::MacAddr8(v)) => Ok(TupleValue::Bytes(v.to_vec())),
        (ScalarType::Float32, Value::Float64(v)) => {
            Ok(TupleValue::Bytes((v as f32).to_le_bytes().to_vec()))
        }
        (ScalarType::Float64, Value::Float64(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Point, Value::Point(point)) => {
            let mut bytes = Vec::with_capacity(16);
            bytes.extend_from_slice(&point.x.to_le_bytes());
            bytes.extend_from_slice(&point.y.to_le_bytes());
            Ok(TupleValue::Bytes(bytes))
        }
        (ScalarType::Line, Value::Line(line)) => {
            let mut bytes = Vec::with_capacity(24);
            bytes.extend_from_slice(&line.a.to_le_bytes());
            bytes.extend_from_slice(&line.b.to_le_bytes());
            bytes.extend_from_slice(&line.c.to_le_bytes());
            Ok(TupleValue::Bytes(bytes))
        }
        (ScalarType::Lseg, Value::Lseg(lseg)) => {
            let mut bytes = Vec::with_capacity(32);
            for point in &lseg.p {
                bytes.extend_from_slice(&point.x.to_le_bytes());
                bytes.extend_from_slice(&point.y.to_le_bytes());
            }
            Ok(TupleValue::Bytes(bytes))
        }
        (ScalarType::Box, Value::Box(geo_box)) => {
            let mut bytes = Vec::with_capacity(32);
            bytes.extend_from_slice(&geo_box.high.x.to_le_bytes());
            bytes.extend_from_slice(&geo_box.high.y.to_le_bytes());
            bytes.extend_from_slice(&geo_box.low.x.to_le_bytes());
            bytes.extend_from_slice(&geo_box.low.y.to_le_bytes());
            Ok(TupleValue::Bytes(bytes))
        }
        (ScalarType::Circle, Value::Circle(circle)) => {
            let mut bytes = Vec::with_capacity(24);
            bytes.extend_from_slice(&circle.center.x.to_le_bytes());
            bytes.extend_from_slice(&circle.center.y.to_le_bytes());
            bytes.extend_from_slice(&circle.radius.to_le_bytes());
            Ok(TupleValue::Bytes(bytes))
        }
        (ScalarType::Path, Value::Path(path)) => Ok(TupleValue::Bytes(encode_path_bytes(&path))),
        (ScalarType::Polygon, Value::Polygon(poly)) => {
            Ok(TupleValue::Bytes(encode_polygon_bytes(&poly)))
        }
        (ScalarType::Money, Value::Money(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Numeric, Value::Numeric(numeric)) => {
            Ok(TupleValue::Bytes(numeric.render().into_bytes()))
        }
        (ScalarType::Json, Value::Json(text)) => Ok(TupleValue::Bytes(text.as_bytes().to_vec())),
        (ScalarType::Jsonb, Value::Jsonb(bytes)) => Ok(TupleValue::Bytes(bytes)),
        (ScalarType::JsonPath, Value::JsonPath(text)) => {
            Ok(TupleValue::Bytes(text.as_bytes().to_vec()))
        }
        (ScalarType::Xml, Value::Xml(text)) => Ok(TupleValue::Bytes(text.as_bytes().to_vec())),
        (ScalarType::TsVector, Value::TsVector(vector)) => Ok(TupleValue::Bytes(
            crate::backend::executor::encode_tsvector_bytes(&vector),
        )),
        (ScalarType::TsQuery, Value::TsQuery(query)) => Ok(TupleValue::Bytes(
            crate::backend::executor::encode_tsquery_bytes(&query),
        )),
        (ScalarType::PgLsn, Value::PgLsn(value)) => {
            Ok(TupleValue::Bytes(value.to_le_bytes().to_vec()))
        }
        (ScalarType::Text, value) => {
            if let Some(array) = value.as_array_value()
                && matches!(
                    column.sql_type.kind,
                    SqlTypeKind::Int2Vector | SqlTypeKind::OidVector
                )
            {
                return Ok(TupleValue::Bytes(
                    format_vector_array_storage_text(column.sql_type, &array)?.into_bytes(),
                ));
            }
            let text = text_value_for_storage(&value)?;
            Ok(TupleValue::Bytes(text.into_bytes()))
        }
        (ScalarType::Record, Value::Record(record)) => {
            Ok(TupleValue::Bytes(encode_composite_datum(&record)?))
        }
        (ScalarType::Bool, Value::Bool(v)) => Ok(TupleValue::Bytes(vec![u8::from(v)])),
        (ScalarType::Array(_), Value::Array(items))
            if column.sql_type.kind == SqlTypeKind::AnyArray =>
        {
            Ok(TupleValue::Bytes(encode_anyarray_bytes(
                &ArrayValue::from_1d(items),
            )?))
        }
        (ScalarType::Array(_), Value::PgArray(array))
            if column.sql_type.kind == SqlTypeKind::AnyArray =>
        {
            Ok(TupleValue::Bytes(encode_anyarray_bytes(&array)?))
        }
        (ScalarType::Array(_), Value::Array(items)) => Ok(TupleValue::Bytes(encode_array_bytes(
            array_storage_element_type(column.sql_type),
            &ArrayValue::from_1d(items),
        )?)),
        (ScalarType::Array(_), Value::PgArray(array)) => Ok(TupleValue::Bytes(encode_array_bytes(
            array_storage_element_type(column.sql_type),
            &array,
        )?)),
        (_, other) => Err(ExecError::TypeMismatch {
            op: "assignment",
            left: Value::Null,
            right: other,
        }),
    }
}

fn array_storage_element_type(sql_type: SqlType) -> SqlType {
    let mut element_type = sql_type.element_type();
    if sql_type.type_oid != 0
        && !matches!(
            element_type.kind,
            SqlTypeKind::Composite | SqlTypeKind::Record | SqlTypeKind::Enum
        )
    {
        element_type.type_oid = 0;
    }
    element_type
}

fn text_value_for_storage(value: &Value) -> Result<String, ExecError> {
    if let Some(text) = value.as_text() {
        return Ok(text.to_string());
    }
    if let Value::InternalChar(v) = value {
        return Ok(render_internal_char_text(*v));
    }
    match cast_value(value.clone(), SqlType::new(SqlTypeKind::Text))? {
        Value::Text(text) => Ok(text.to_string()),
        Value::TextRef(ptr, len) => Ok(unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len as usize)).to_owned()
        }),
        Value::InternalChar(v) => Ok(render_internal_char_text(v)),
        other => Err(ExecError::TypeMismatch {
            op: "text storage coercion",
            left: other,
            right: Value::Text("".into()),
        }),
    }
}

pub(crate) fn format_vector_array_storage_text(
    sql_type: SqlType,
    array: &ArrayValue,
) -> Result<String, ExecError> {
    let mut parts = Vec::with_capacity(array.elements.len());
    for element in &array.elements {
        let text = match sql_type.kind {
            SqlTypeKind::Int2Vector => match element {
                Value::Int16(value) => value.to_string(),
                Value::Int32(value) => i16::try_from(*value)
                    .map_err(|_| ExecError::Int2OutOfRange)?
                    .to_string(),
                Value::Int64(value) => i16::try_from(*value)
                    .map_err(|_| ExecError::Int2OutOfRange)?
                    .to_string(),
                _ => {
                    return Err(ExecError::TypeMismatch {
                        op: "int2vector storage",
                        left: element.clone(),
                        right: Value::Null,
                    });
                }
            },
            SqlTypeKind::OidVector => match element {
                Value::Int32(value) if *value >= 0 => (*value as u32).to_string(),
                Value::Int64(value) => u32::try_from(*value)
                    .map_err(|_| ExecError::OidOutOfRange)?
                    .to_string(),
                _ => {
                    return Err(ExecError::TypeMismatch {
                        op: "oidvector storage",
                        left: element.clone(),
                        right: Value::Null,
                    });
                }
            },
            _ => {
                return Err(ExecError::TypeMismatch {
                    op: "vector storage",
                    left: Value::PgArray(array.clone()),
                    right: Value::Null,
                });
            }
        };
        parts.push(text);
    }
    Ok(parts.join(" "))
}

pub(crate) fn coerce_assignment_value(value: &Value, target: SqlType) -> Result<Value, ExecError> {
    coerce_assignment_value_with_config(value, target, &DateTimeConfig::default())
}

pub(crate) fn coerce_assignment_value_with_config(
    value: &Value,
    target: SqlType,
    datetime_config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    if target.kind == SqlTypeKind::AnyArray {
        return match value {
            Value::Null => Ok(Value::Null),
            Value::Array(items) => Ok(Value::PgArray(ArrayValue::from_1d(
                items.iter().map(Value::to_owned_value).collect(),
            ))),
            Value::PgArray(array) => Ok(Value::PgArray(array.to_owned_value())),
            other => Err(ExecError::TypeMismatch {
                op: "assignment",
                left: Value::Null,
                right: other.clone(),
            }),
        };
    }

    if target.is_array {
        return match value {
            Value::Null => Ok(Value::Null),
            Value::Array(items) => {
                let element_type = target.element_type();
                if items
                    .iter()
                    .any(|item| matches!(item, Value::Array(_) | Value::PgArray(_)))
                {
                    let array = ArrayValue::from_nested_values(items.clone(), vec![1]).map_err(
                        |details| ExecError::DetailedError {
                            message: "malformed array literal".into(),
                            detail: Some(details),
                            hint: None,
                            sqlstate: "22P02",
                        },
                    )?;
                    let mut coerced = Vec::with_capacity(array.elements.len());
                    for item in &array.elements {
                        coerced.push(coerce_assignment_value_with_config(
                            item,
                            element_type,
                            datetime_config,
                        )?);
                    }
                    Ok(Value::PgArray(ArrayValue::from_dimensions(
                        array.dimensions,
                        coerced,
                    )))
                } else {
                    let mut coerced = Vec::with_capacity(items.len());
                    for item in items {
                        coerced.push(coerce_assignment_value_with_config(
                            item,
                            element_type,
                            datetime_config,
                        )?);
                    }
                    Ok(Value::Array(coerced))
                }
            }
            Value::PgArray(array) => {
                let element_type = target.element_type();
                let mut coerced = Vec::with_capacity(array.elements.len());
                for item in &array.elements {
                    coerced.push(coerce_assignment_value_with_config(
                        item,
                        element_type,
                        datetime_config,
                    )?);
                }
                Ok(Value::PgArray(ArrayValue::from_dimensions(
                    array.dimensions.clone(),
                    coerced,
                )))
            }
            other => match other.as_text() {
                Some(text) => parse_text_array_literal_with_options(
                    text,
                    target.element_type(),
                    "copy assignment",
                    false,
                ),
                None => Err(ExecError::TypeMismatch {
                    op: "copy assignment",
                    left: Value::Null,
                    right: other.clone(),
                }),
            },
        };
    }

    match value {
        Value::Null => Ok(Value::Null),
        Value::Int16(v) => {
            cast_text_value_with_config(&v.to_string(), target, false, datetime_config)
        }
        Value::Int32(v) => {
            cast_text_value_with_config(&v.to_string(), target, false, datetime_config)
        }
        Value::EnumOid(v) if matches!(target.kind, SqlTypeKind::Enum) => Ok(Value::EnumOid(*v)),
        Value::EnumOid(v) => {
            cast_text_value_with_config(&v.to_string(), target, false, datetime_config)
        }
        Value::Int64(v) => {
            cast_text_value_with_config(&v.to_string(), target, false, datetime_config)
        }
        Value::Xid8(v) => {
            cast_text_value_with_config(&v.to_string(), target, false, datetime_config)
        }
        Value::PgLsn(v) => {
            cast_text_value_with_config(&render_pg_lsn_text(*v), target, false, datetime_config)
        }
        Value::Money(v) => cast_text_value_with_config(
            &crate::backend::executor::money_format_text(*v),
            target,
            false,
            datetime_config,
        ),
        Value::Date(v) => cast_value_with_config(Value::Date(*v), target, datetime_config),
        Value::Time(v) => cast_value_with_config(Value::Time(*v), target, datetime_config),
        Value::TimeTz(v) => cast_value_with_config(Value::TimeTz(*v), target, datetime_config),
        Value::Timestamp(v) => {
            cast_value_with_config(Value::Timestamp(*v), target, datetime_config)
        }
        Value::TimestampTz(v) => {
            cast_value_with_config(Value::TimestampTz(*v), target, datetime_config)
        }
        Value::Interval(v) => cast_value_with_config(Value::Interval(*v), target, datetime_config),
        Value::Bit(bits) => match target.kind {
            SqlTypeKind::Bit | SqlTypeKind::VarBit => {
                Ok(Value::Bit(coerce_bit_string(bits.clone(), target, false)?))
            }
            _ => cast_value_with_config(Value::Bit(bits.clone()), target, datetime_config),
        },
        Value::Bool(v) => cast_text_value_with_config(
            if *v { "true" } else { "false" },
            target,
            false,
            datetime_config,
        ),
        Value::Float64(v) => match target.kind {
            SqlTypeKind::Float4
            | SqlTypeKind::Float8
            | SqlTypeKind::Numeric
            | SqlTypeKind::Int2
            | SqlTypeKind::Int4
            | SqlTypeKind::Int8
            | SqlTypeKind::Oid => cast_value(Value::Float64(*v), target),
            _ => cast_text_value_with_config(&v.to_string(), target, false, datetime_config),
        },
        Value::Numeric(numeric) => cast_numeric_value(numeric.clone(), target, false),
        Value::JsonPath(text) => {
            cast_text_value_with_config(text.as_str(), target, false, datetime_config)
        }
        Value::Json(text) => {
            cast_text_value_with_config(text.as_str(), target, false, datetime_config)
        }
        Value::Xml(text) => {
            cast_text_value_with_config(text.as_str(), target, false, datetime_config)
        }
        Value::Jsonb(bytes) => {
            cast_text_value_with_config(&render_jsonb_bytes(bytes)?, target, false, datetime_config)
        }
        Value::Bytea(bytes) => {
            cast_value_with_config(Value::Bytea(bytes.clone()), target, datetime_config)
        }
        Value::Uuid(value) => cast_value_with_config(Value::Uuid(*value), target, datetime_config),
        Value::Inet(v) => {
            cast_text_value_with_config(&v.render_inet(), target, false, datetime_config)
        }
        Value::Cidr(v) => {
            cast_text_value_with_config(&v.render_cidr(), target, false, datetime_config)
        }
        Value::MacAddr(v) => {
            cast_text_value_with_config(&render_macaddr_text(v), target, false, datetime_config)
        }
        Value::MacAddr8(v) => {
            cast_text_value_with_config(&render_macaddr8_text(v), target, false, datetime_config)
        }
        Value::TsVector(vector) => cast_text_value_with_config(
            &crate::backend::executor::render_tsvector_text(vector),
            target,
            false,
            datetime_config,
        ),
        Value::TsQuery(query) => cast_text_value_with_config(
            &crate::backend::executor::render_tsquery_text(query),
            target,
            false,
            datetime_config,
        ),
        Value::Text(text) => {
            cast_text_value_with_config(text.as_str(), target, false, datetime_config)
        }
        Value::TextRef(_, _) => {
            cast_text_value_with_config(value.as_text().unwrap(), target, false, datetime_config)
        }
        Value::InternalChar(byte) => {
            cast_value_with_config(Value::InternalChar(*byte), target, datetime_config)
        }
        Value::Range(range) => Ok(Value::Range(range.clone())),
        Value::Multirange(multirange) => Ok(Value::Multirange(multirange.clone())),
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => cast_value_with_config(value.clone(), target, datetime_config),
        Value::Array(items) => Ok(Value::Array(items.clone())),
        Value::PgArray(array) => Ok(Value::PgArray(array.clone())),
        Value::Record(record) => Ok(Value::Record(record.clone())),
    }
}

pub(crate) fn decode_value(column: &ColumnDesc, bytes: Option<&[u8]>) -> Result<Value, ExecError> {
    decode_value_with_toast(column, bytes, None)
}

fn unsupported_storage_type(column: &ColumnDesc, bytes: &[u8]) -> ExecError {
    ExecError::UnsupportedStorageType {
        column: column.name.clone(),
        ty: column.ty.clone(),
        attlen: column.storage.attlen,
        actual_len: Some(bytes.len()),
    }
}

pub(crate) fn decode_value_with_toast(
    column: &ColumnDesc,
    bytes: Option<&[u8]>,
    toast: Option<&ToastFetchContext>,
) -> Result<Value, ExecError> {
    let Some(bytes) = bytes else {
        return Ok(Value::Null);
    };
    let owned = if column.storage.attlen == -1 {
        if bytes.len() == crate::include::varatt::TOAST_POINTER_SIZE
            && crate::include::access::detoast::is_ondisk_toast_pointer(bytes)
        {
            let toast = toast.ok_or_else(|| ExecError::InvalidStorageValue {
                column: column.name.clone(),
                details: "toast pointer found without toast relation context".into(),
            })?;
            Some(crate::backend::access::common::detoast::detoast_value_bytes(toast, bytes)?)
        } else if crate::include::varatt::compressed_inline_total_size(bytes) == Some(bytes.len())
            && crate::include::access::detoast::is_compressed_inline_datum(bytes)
        {
            Some(
                crate::backend::access::common::toast_compression::decompress_inline_datum(bytes)
                    .map_err(|err| match err {
                    ExecError::InvalidStorageValue { details, .. } => {
                        ExecError::InvalidStorageValue {
                            column: column.name.clone(),
                            details,
                        }
                    }
                    other => other,
                })?,
            )
        } else {
            None
        }
    } else {
        None
    };
    let bytes = owned.as_deref().unwrap_or(bytes);

    match column.ty {
        ScalarType::Int16 => {
            if column.storage.attlen != 2 || bytes.len() != 2 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Int16(i16::from_le_bytes(bytes.try_into().map_err(
                |_| ExecError::InvalidStorageValue {
                    column: column.name.clone(),
                    details: "int2 must be exactly 2 bytes".into(),
                },
            )?)))
        }
        ScalarType::Int32 => {
            if column.storage.attlen != 4 || bytes.len() != 4 {
                return Err(unsupported_storage_type(column, bytes));
            }
            let raw = i32::from_le_bytes(bytes.try_into().map_err(|_| {
                ExecError::InvalidStorageValue {
                    column: column.name.clone(),
                    details: "int4 must be exactly 4 bytes".into(),
                }
            })?);
            if matches!(
                column.sql_type.kind,
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
            ) {
                Ok(Value::Int64(raw as u32 as i64))
            } else {
                Ok(Value::Int32(raw))
            }
        }
        ScalarType::Enum => {
            if column.storage.attlen != 4 || bytes.len() != 4 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                    actual_len: Some(bytes.len()),
                });
            }
            Ok(Value::EnumOid(u32::from_le_bytes(
                bytes
                    .try_into()
                    .map_err(|_| ExecError::InvalidStorageValue {
                        column: column.name.clone(),
                        details: "enum must be exactly 4 bytes".into(),
                    })?,
            )))
        }
        ScalarType::Int64 => {
            if column.storage.attlen != 8 || bytes.len() != 8 {
                return Err(unsupported_storage_type(column, bytes));
            }
            if column.sql_type.type_oid == crate::include::catalog::XID8_TYPE_OID {
                return Ok(Value::Xid8(u64::from_le_bytes(bytes.try_into().map_err(
                    |_| ExecError::InvalidStorageValue {
                        column: column.name.clone(),
                        details: "xid8 must be exactly 8 bytes".into(),
                    },
                )?)));
            }
            Ok(Value::Int64(i64::from_le_bytes(bytes.try_into().map_err(
                |_| ExecError::InvalidStorageValue {
                    column: column.name.clone(),
                    details: "int8 must be exactly 8 bytes".into(),
                },
            )?)))
        }
        ScalarType::Money => {
            if column.storage.attlen != 8 || bytes.len() != 8 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Money(i64::from_le_bytes(bytes.try_into().map_err(
                |_| ExecError::InvalidStorageValue {
                    column: column.name.clone(),
                    details: "money must be exactly 8 bytes".into(),
                },
            )?)))
        }
        ScalarType::Date => {
            if column.storage.attlen != 4 || bytes.len() != 4 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Date(crate::include::nodes::datetime::DateADT(
                i32::from_le_bytes(bytes.try_into().unwrap()),
            )))
        }
        ScalarType::Time => {
            if column.storage.attlen != 8 || bytes.len() != 8 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Time(crate::include::nodes::datetime::TimeADT(
                i64::from_le_bytes(bytes.try_into().unwrap()),
            )))
        }
        ScalarType::TimeTz => {
            if column.storage.attlen != 12 || bytes.len() != 12 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::TimeTz(crate::include::nodes::datetime::TimeTzADT {
                time: crate::include::nodes::datetime::TimeADT(i64::from_le_bytes(
                    bytes[0..8].try_into().unwrap(),
                )),
                offset_seconds: i32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            }))
        }
        ScalarType::Timestamp => {
            if column.storage.attlen != 8 || bytes.len() != 8 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Timestamp(
                crate::include::nodes::datetime::TimestampADT(i64::from_le_bytes(
                    bytes.try_into().unwrap(),
                )),
            ))
        }
        ScalarType::TimestampTz => {
            if column.storage.attlen != 8 || bytes.len() != 8 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::TimestampTz(
                crate::include::nodes::datetime::TimestampTzADT(i64::from_le_bytes(
                    bytes.try_into().unwrap(),
                )),
            ))
        }
        ScalarType::Interval => {
            if column.storage.attlen != 16 || bytes.len() != 16 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Interval(
                crate::include::nodes::datum::IntervalValue {
                    time_micros: i64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                    days: i32::from_le_bytes(bytes[8..12].try_into().unwrap()),
                    months: i32::from_le_bytes(bytes[12..16].try_into().unwrap()),
                },
            ))
        }
        ScalarType::BitString => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(unsupported_storage_type(column, bytes));
            }
            if bytes.len() < 4 {
                return Err(ExecError::InvalidStorageValue {
                    column: column.name.clone(),
                    details: "bit payload too short".into(),
                });
            }
            let bit_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as i32;
            Ok(Value::Bit(crate::include::nodes::datum::BitString::new(
                bit_len,
                bytes[4..].to_vec(),
            )))
        }
        ScalarType::Bytea => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Bytea(bytes.to_vec()))
        }
        ScalarType::Uuid => {
            if column.storage.attlen != 16 || bytes.len() != 16 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Uuid(bytes.try_into().unwrap()))
        }
        ScalarType::Inet => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(unsupported_storage_type(column, bytes));
            }
            parse_inet_bytes(bytes).map(Value::Inet)
        }
        ScalarType::Cidr => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(unsupported_storage_type(column, bytes));
            }
            parse_cidr_bytes(bytes).map(Value::Cidr)
        }
        ScalarType::MacAddr => {
            if column.storage.attlen != 6 || bytes.len() != 6 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                    actual_len: Some(bytes.len()),
                });
            }
            parse_macaddr_bytes(bytes).map(Value::MacAddr)
        }
        ScalarType::MacAddr8 => {
            if column.storage.attlen != 8 || bytes.len() != 8 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                    actual_len: Some(bytes.len()),
                });
            }
            parse_macaddr8_bytes(bytes).map(Value::MacAddr8)
        }
        ScalarType::Float32 => {
            if column.storage.attlen != 4 || bytes.len() != 4 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Float64(
                f32::from_le_bytes(bytes.try_into().map_err(|_| {
                    ExecError::InvalidStorageValue {
                        column: column.name.clone(),
                        details: "float4 must be exactly 4 bytes".into(),
                    }
                })?) as f64,
            ))
        }
        ScalarType::Float64 => {
            if column.storage.attlen != 8 || bytes.len() != 8 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Float64(f64::from_le_bytes(
                bytes
                    .try_into()
                    .map_err(|_| ExecError::InvalidStorageValue {
                        column: column.name.clone(),
                        details: "float8 must be exactly 8 bytes".into(),
                    })?,
            )))
        }
        ScalarType::Point => {
            if column.storage.attlen != 16 || bytes.len() != 16 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Point(GeoPoint {
                x: f64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                y: f64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            }))
        }
        ScalarType::Line => {
            if column.storage.attlen != 24 || bytes.len() != 24 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Line(GeoLine {
                a: f64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                b: f64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                c: f64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            }))
        }
        ScalarType::Lseg => {
            if column.storage.attlen != 32 || bytes.len() != 32 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Lseg(GeoLseg {
                p: [
                    GeoPoint {
                        x: f64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                        y: f64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                    },
                    GeoPoint {
                        x: f64::from_le_bytes(bytes[16..24].try_into().unwrap()),
                        y: f64::from_le_bytes(bytes[24..32].try_into().unwrap()),
                    },
                ],
            }))
        }
        ScalarType::Box => {
            if column.storage.attlen != 32 || bytes.len() != 32 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Box(GeoBox {
                high: GeoPoint {
                    x: f64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                    y: f64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                },
                low: GeoPoint {
                    x: f64::from_le_bytes(bytes[16..24].try_into().unwrap()),
                    y: f64::from_le_bytes(bytes[24..32].try_into().unwrap()),
                },
            }))
        }
        ScalarType::Circle => {
            if column.storage.attlen != 24 || bytes.len() != 24 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Circle(GeoCircle {
                center: GeoPoint {
                    x: f64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                    y: f64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                },
                radius: f64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            }))
        }
        ScalarType::Numeric => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Numeric(
                parse_numeric_text(unsafe { std::str::from_utf8_unchecked(bytes) }).ok_or_else(
                    || ExecError::InvalidStorageValue {
                        column: column.name.clone(),
                        details: "invalid numeric text".into(),
                    },
                )?,
            ))
        }
        ScalarType::Json => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(unsupported_storage_type(column, bytes));
            }
            let text = unsafe { std::str::from_utf8_unchecked(bytes) };
            validate_json_text(text)?;
            Ok(Value::Json(CompactString::new(text)))
        }
        ScalarType::Jsonb => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(unsupported_storage_type(column, bytes));
            }
            decode_jsonb(bytes)?;
            Ok(Value::Jsonb(bytes.to_vec()))
        }
        ScalarType::JsonPath => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(unsupported_storage_type(column, bytes));
            }
            let text = unsafe { std::str::from_utf8_unchecked(bytes) };
            Ok(Value::JsonPath(canonicalize_jsonpath_text(text)?))
        }
        ScalarType::Xml => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(unsupported_storage_type(column, bytes));
            }
            let text = unsafe { std::str::from_utf8_unchecked(bytes) };
            Ok(Value::Xml(CompactString::new(text)))
        }
        ScalarType::TsVector => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::TsVector(
                crate::backend::executor::decode_tsvector_bytes(bytes)?,
            ))
        }
        ScalarType::TsQuery => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::TsQuery(
                crate::backend::executor::decode_tsquery_bytes(bytes)?,
            ))
        }
        ScalarType::PgLsn => {
            if column.storage.attlen != 8 || bytes.len() != 8 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::PgLsn(u64::from_le_bytes(bytes.try_into().map_err(
                |_| ExecError::InvalidStorageValue {
                    column: column.name.clone(),
                    details: "pg_lsn must be exactly 8 bytes".into(),
                },
            )?)))
        }
        ScalarType::Text => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Text(CompactString::new(unsafe {
                std::str::from_utf8_unchecked(bytes)
            })))
        }
        ScalarType::Record => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(unsupported_storage_type(column, bytes));
            }
            decode_composite_datum(bytes).map(Value::Record)
        }
        ScalarType::Path => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Path(decode_path_bytes(bytes)?))
        }
        ScalarType::Polygon => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Polygon(decode_polygon_bytes(bytes)?))
        }
        ScalarType::Bool => {
            if column.storage.attlen != 1 || bytes.len() != 1 {
                return Err(unsupported_storage_type(column, bytes));
            }
            match bytes[0] {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                other => Err(ExecError::InvalidStorageValue {
                    column: column.name.clone(),
                    details: format!("invalid bool byte {}", other),
                }),
            }
        }
        ScalarType::Array(_) => {
            if column.storage.attlen != -1 {
                return Err(unsupported_storage_type(column, bytes));
            }
            if column.sql_type.kind == SqlTypeKind::AnyArray {
                decode_anyarray_bytes(bytes)
            } else {
                decode_array_bytes(array_storage_element_type(column.sql_type), bytes)
            }
        }
        ScalarType::Multirange(multirange_type) => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Multirange(
                crate::backend::executor::decode_multirange_bytes(multirange_type, bytes)?,
            ))
        }
        ScalarType::Range(range_type) => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(unsupported_storage_type(column, bytes));
            }
            Ok(Value::Range(decode_range_bytes(range_type, bytes)?))
        }
    }
}

pub(crate) fn missing_column_value(column: &ColumnDesc) -> Value {
    if column.generated.is_some() {
        return Value::Null;
    }
    column
        .missing_default_value
        .clone()
        .or_else(|| {
            (column.default_sequence_oid.is_none())
                .then_some(column.default_expr.as_deref())
                .flatten()
                .and_then(|sql| {
                    crate::backend::parser::derive_literal_default_value(sql, column.sql_type).ok()
                })
        })
        .unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::executor::expr_range::parse_range_text;
    use crate::backend::utils::misc::guc_datetime::{DateOrder, DateStyleFormat, DateTimeConfig};
    use crate::backend::utils::time::timestamp::parse_timestamp_text;
    use crate::include::catalog::{INT4_TYPE_OID, INT4RANGE_TYPE_OID};
    use crate::include::nodes::datum::{ArrayDimension, RecordDescriptor, RecordValue};

    #[test]
    fn anyarray_value_roundtrips_through_tuple_storage() {
        let desc = RelationDesc {
            columns: vec![column_desc("v", SqlType::new(SqlTypeKind::AnyArray), true)],
        };
        let value = Value::PgArray(
            ArrayValue::from_1d(vec![Value::Int32(1), Value::Int32(2)])
                .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
        );

        let tuple = tuple_from_values(&desc, std::slice::from_ref(&value)).unwrap();
        let raw = tuple.deform(&desc.attribute_descs()).unwrap();
        let decoded = decode_value(&desc.columns[0], raw[0]).unwrap();

        assert_eq!(decoded, value);
    }

    #[test]
    fn anyarray_payload_roundtrips_directly() {
        let array = ArrayValue::from_1d(vec![Value::Text("a".into()), Value::Text("b".into())])
            .with_element_type_oid(crate::include::catalog::TEXT_TYPE_OID);
        let bytes = encode_anyarray_bytes(&array).unwrap();
        let decoded = decode_anyarray_bytes(&bytes).unwrap();

        assert_eq!(decoded, Value::PgArray(array));
    }

    #[test]
    fn encode_text_column_coerces_non_text_values() {
        let column = column_desc("v", SqlType::new(SqlTypeKind::Text), true);

        let encoded = encode_value(&column, &Value::Int32(42)).unwrap();

        assert_eq!(encoded, TupleValue::Bytes(b"42".to_vec()));
    }

    #[test]
    fn concrete_array_payload_preserves_element_oid() {
        let array = ArrayValue::from_1d(vec![Value::Int32(1), Value::Int32(2)])
            .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID);
        let bytes = encode_array_bytes(SqlType::new(SqlTypeKind::Int4), &array).unwrap();
        let decoded = decode_array_bytes(SqlType::new(SqlTypeKind::Int4), &bytes).unwrap();

        assert_eq!(decoded, Value::PgArray(array));
    }

    #[test]
    fn concrete_array_decoder_ignores_varchar_typmod_in_header_check() {
        let array = ArrayValue::from_1d(vec![Value::Text("ab".into())])
            .with_element_type_oid(crate::include::catalog::VARCHAR_TYPE_OID);
        let bytes =
            encode_array_bytes(SqlType::with_char_len(SqlTypeKind::Varchar, 4), &array).unwrap();
        let decoded =
            decode_array_bytes(SqlType::with_char_len(SqlTypeKind::Varchar, 4), &bytes).unwrap();

        assert_eq!(decoded, Value::PgArray(array));
    }

    #[test]
    fn record_storage_roundtrip_preserves_identity() {
        let desc = RelationDesc {
            columns: vec![column_desc(
                "v",
                SqlType::record(crate::include::catalog::RECORD_TYPE_OID),
                true,
            )],
        };
        let value = Value::Record(RecordValue::named(
            4242,
            3131,
            7,
            vec![
                ("a".into(), Value::Int32(1)),
                ("b".into(), Value::Text("x".into())),
            ],
        ));

        let tuple = tuple_from_values(&desc, std::slice::from_ref(&value)).unwrap();
        let raw = tuple.deform(&desc.attribute_descs()).unwrap();
        let decoded = decode_value(&desc.columns[0], raw[0]).unwrap();

        assert_eq!(decoded, value);
    }

    #[test]
    fn record_storage_roundtrip_preserves_generic_range_identity() {
        let desc = RelationDesc {
            columns: vec![column_desc(
                "v",
                SqlType::record(crate::include::catalog::RECORD_TYPE_OID),
                true,
            )],
        };
        let range_sql_type = SqlType::range(INT4RANGE_TYPE_OID, INT4_TYPE_OID).with_range_metadata(
            INT4_TYPE_OID,
            0,
            true,
        );
        let range = parse_range_text("[1,5)", range_sql_type)
            .expect("parse builtin range through generic identity");
        let expected_range = range.clone();
        let value = Value::Record(RecordValue::anonymous(vec![("span".into(), range)]));

        let tuple = tuple_from_values(&desc, std::slice::from_ref(&value)).unwrap();
        let raw = tuple.deform(&desc.attribute_descs()).unwrap();
        let decoded = decode_value(&desc.columns[0], raw[0]).unwrap();
        let Value::Record(decoded) = decoded else {
            panic!("expected record");
        };

        assert_eq!(
            decoded.descriptor.fields[0].sql_type.kind,
            SqlTypeKind::Range
        );
        assert_eq!(
            decoded.descriptor.fields[0].sql_type.type_oid,
            INT4RANGE_TYPE_OID
        );
        assert_eq!(
            decoded.descriptor.fields[0].sql_type.range_subtype_oid,
            INT4_TYPE_OID
        );
        assert_eq!(decoded.fields[0], expected_range);
    }

    #[test]
    fn record_storage_canonicalizes_legacy_range_alias_identity() {
        let desc = RelationDesc {
            columns: vec![column_desc(
                "v",
                SqlType::record(crate::include::catalog::RECORD_TYPE_OID),
                true,
            )],
        };
        let range = parse_range_text("[1,5)", SqlType::new(SqlTypeKind::Int4Range))
            .expect("parse legacy alias range");
        let descriptor = RecordDescriptor::anonymous(
            vec![("span".into(), SqlType::new(SqlTypeKind::Int4Range))],
            -1,
        );
        let expected_range = range.clone();
        let value = Value::Record(RecordValue::from_descriptor(descriptor, vec![range]));

        let tuple = tuple_from_values(&desc, std::slice::from_ref(&value)).unwrap();
        let raw = tuple.deform(&desc.attribute_descs()).unwrap();
        let decoded = decode_value(&desc.columns[0], raw[0]).unwrap();
        let Value::Record(decoded) = decoded else {
            panic!("expected record");
        };

        assert_eq!(
            decoded.descriptor.fields[0].sql_type.kind,
            SqlTypeKind::Range
        );
        assert_eq!(
            decoded.descriptor.fields[0].sql_type.type_oid,
            crate::include::catalog::INT4RANGE_TYPE_OID
        );
        assert_eq!(
            decoded.descriptor.fields[0].sql_type.range_subtype_oid,
            crate::include::catalog::INT4_TYPE_OID
        );
        assert_eq!(decoded.fields[0], expected_range);
    }

    #[test]
    fn flat_int4_array_payload_matches_postgres_style_layout() {
        let array = ArrayValue::from_1d(vec![Value::Int32(1), Value::Int32(2)]);
        let bytes = encode_array_bytes(SqlType::new(SqlTypeKind::Int4), &array).unwrap();

        assert_eq!(bytes.len(), 32);
        assert_eq!(i32::from_le_bytes(bytes[0..4].try_into().unwrap()), 1);
        assert_eq!(i32::from_le_bytes(bytes[4..8].try_into().unwrap()), 0);
        assert_eq!(
            u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            crate::include::catalog::INT4_TYPE_OID
        );
        assert_eq!(i32::from_le_bytes(bytes[12..16].try_into().unwrap()), 2);
        assert_eq!(i32::from_le_bytes(bytes[16..20].try_into().unwrap()), 1);
        assert_eq!(&bytes[20..24], &[0, 0, 0, 0]);
        assert_eq!(i32::from_le_bytes(bytes[24..28].try_into().unwrap()), 1);
        assert_eq!(i32::from_le_bytes(bytes[28..32].try_into().unwrap()), 2);
    }

    #[test]
    fn flat_text_array_payload_uses_bitmap_and_embedded_varlena() {
        let array = ArrayValue::from_1d(vec![
            Value::Text("a".into()),
            Value::Null,
            Value::Text("bee".into()),
        ]);
        let bytes = encode_array_bytes(SqlType::new(SqlTypeKind::Text), &array).unwrap();

        assert_eq!(bytes.len(), 32);
        assert_eq!(i32::from_le_bytes(bytes[0..4].try_into().unwrap()), 1);
        assert_eq!(i32::from_le_bytes(bytes[4..8].try_into().unwrap()), 24);
        assert_eq!(
            u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            crate::include::catalog::TEXT_TYPE_OID
        );
        assert_eq!(i32::from_le_bytes(bytes[12..16].try_into().unwrap()), 3);
        assert_eq!(i32::from_le_bytes(bytes[16..20].try_into().unwrap()), 1);
        assert_eq!(bytes[20], 0b0000_0101);
        assert_eq!(&bytes[21..24], &[0, 0, 0]);
        assert_eq!(&bytes[24..28], &[0x05, b'a', 0, 0]);
        assert_eq!(&bytes[28..32], &[0x09, b'b', b'e', b'e']);
    }

    #[test]
    fn concrete_arrays_use_declared_element_oid() {
        let array = ArrayValue::from_1d(vec![Value::Int32(1)])
            .with_element_type_oid(crate::include::catalog::TEXT_TYPE_OID);
        let bytes = encode_array_bytes(SqlType::new(SqlTypeKind::Int4), &array).unwrap();

        assert_eq!(
            u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            crate::include::catalog::INT4_TYPE_OID
        );
    }

    #[test]
    fn interval_arrays_render_postgres_interval_style() {
        let array = ArrayValue::from_1d(vec![
            Value::Text("00:00:00".into()),
            Value::Text("01:42:20".into()),
        ])
        .with_element_type_oid(crate::include::catalog::INTERVAL_TYPE_OID);

        assert_eq!(format_array_value_text(&array), "{00:00:00,01:42:20}");
    }

    #[test]
    fn explicit_zero_length_dimension_roundtrips() {
        let array = ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound: 5,
                length: 0,
            }],
            Vec::new(),
        )
        .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID);
        let bytes = encode_array_bytes(SqlType::new(SqlTypeKind::Int4), &array).unwrap();
        let decoded = decode_array_bytes(SqlType::new(SqlTypeKind::Int4), &bytes).unwrap();

        assert_eq!(decoded, Value::PgArray(array));
    }

    #[test]
    fn typed_array_decoder_rejects_mismatched_header_oid() {
        let array = ArrayValue::from_1d(vec![Value::Text("a".into())])
            .with_element_type_oid(crate::include::catalog::TEXT_TYPE_OID);
        let bytes = encode_anyarray_bytes(&array).unwrap();
        let error = decode_array_bytes(SqlType::new(SqlTypeKind::Int4), &bytes).unwrap_err();

        match error {
            ExecError::InvalidStorageValue { details, .. } => {
                assert!(details.contains("does not match expected element type"));
            }
            other => panic!("expected invalid storage value, got {other:?}"),
        }
    }

    #[test]
    fn format_record_text_uses_datetime_config() {
        let config = DateTimeConfig {
            date_style_format: DateStyleFormat::Postgres,
            date_order: DateOrder::Mdy,
            ..DateTimeConfig::default()
        };
        let timestamp = parse_timestamp_text("2012-12-31 15:30:56", &DateTimeConfig::default())
            .expect("parse timestamp");
        let record = RecordValue::anonymous(vec![("c".into(), Value::Timestamp(timestamp))]);

        assert_eq!(
            format_record_text_with_config(&record, &config),
            "(\"Mon Dec 31 15:30:56 2012\")"
        );
    }

    #[test]
    fn format_record_text_uses_float_field_type() {
        let value =
            crate::backend::executor::expr_casts::parse_pg_float("99.097", SqlTypeKind::Float4)
                .expect("parse float4");
        let record = RecordValue::from_descriptor(
            crate::include::nodes::datum::RecordDescriptor::anonymous(
                vec![
                    ("a".into(), SqlType::new(SqlTypeKind::Int2)),
                    ("b".into(), SqlType::new(SqlTypeKind::Float4)),
                ],
                -1,
            ),
            vec![Value::Int16(100), Value::Float64(value)],
        );

        assert_eq!(
            format_record_text_with_options(
                &record,
                &FloatFormatOptions {
                    extra_float_digits: 0,
                    ..FloatFormatOptions::default()
                }
            ),
            "(100,99.097)"
        );
    }

    #[test]
    fn format_array_text_uses_datetime_config_for_record_elements() {
        let config = DateTimeConfig {
            date_style_format: DateStyleFormat::Postgres,
            date_order: DateOrder::Mdy,
            ..DateTimeConfig::default()
        };
        let timestamp = parse_timestamp_text("2003-01-02 00:00:00", &DateTimeConfig::default())
            .expect("parse timestamp");
        let array = ArrayValue::from_1d(vec![Value::Record(RecordValue::anonymous(vec![(
            "c".into(),
            Value::Timestamp(timestamp),
        )]))]);

        assert_eq!(
            format_array_value_text_with_config(&array, &config),
            "{\"(\\\"Thu Jan 02 00:00:00 2003\\\")\"}"
        );
    }
}
