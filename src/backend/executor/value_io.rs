use super::ExecError;
use super::exec_expr::parse_numeric_text;
use super::expr_bit::{coerce_bit_string, render_bit_text};
use super::expr_casts::{
    cast_numeric_value, cast_text_value, cast_value, parse_text_array_literal_with_options,
    render_internal_char_text,
};
use super::expr_datetime::{render_datetime_value_text, render_datetime_value_text_with_config};
use super::expr_geometry::{
    decode_path_bytes, decode_polygon_bytes, encode_path_bytes, encode_polygon_bytes,
    render_geometry_text,
};
use super::expr_range::{decode_range_bytes, encode_range_bytes, render_range_text};
use super::node_types::*;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::expr_json::{canonicalize_jsonpath_text, validate_json_text};
use crate::backend::executor::jsonb::{decode_jsonb, render_jsonb_bytes};
use crate::backend::libpq::pqformat::FloatFormatOptions;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::record::register_anonymous_record_descriptor;
use crate::include::access::htup::{HeapTuple, TupleValue};
use crate::include::nodes::execnodes::ToastFetchContext;
use crate::pgrust::compact_string::CompactString;

mod array;

pub use array::format_array_value_text;
pub(crate) use array::{
    decode_anyarray_bytes, decode_array_bytes, encode_anyarray_bytes, encode_array_bytes,
    format_array_text,
};

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
const INTERNAL_VALUE_TAG_TSVECTOR: u8 = 25;
const INTERNAL_VALUE_TAG_TSQUERY: u8 = 26;
const INTERNAL_VALUE_TAG_TEXT: u8 = 27;
const INTERNAL_VALUE_TAG_INTERNAL_CHAR: u8 = 28;
const INTERNAL_VALUE_TAG_BOOL: u8 = 29;
const INTERNAL_VALUE_TAG_ARRAY: u8 = 30;
const INTERNAL_VALUE_TAG_RECORD: u8 = 31;
const COMPOSITE_DATUM_VERSION: u8 = 1;

pub(crate) fn format_record_text(record: &crate::include::nodes::datum::RecordValue) -> String {
    let mut out = String::from("(");
    for (index, value) in record.fields.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        if matches!(value, Value::Null) {
            continue;
        }
        let rendered = match value {
            Value::Record(record) => format_record_text(record),
            Value::PgArray(array) => format_array_value_text(array),
            Value::Array(values) => format_array_text(values),
            Value::Range(_) => render_range_text(value).unwrap_or_default(),
            Value::InternalChar(byte) => render_internal_char_text(*byte),
            Value::Jsonb(bytes) => render_jsonb_bytes(bytes).unwrap_or_default(),
            other => {
                if let Some(text) = other.as_text() {
                    text.to_string()
                } else {
                    render_datetime_value_text_with_config(other, &DateTimeConfig::default())
                        .or_else(|| render_geometry_text(other, FloatFormatOptions::default()))
                        .unwrap_or_else(|| match other {
                            Value::Bool(true) => "t".to_string(),
                            Value::Bool(false) => "f".to_string(),
                            Value::Int16(v) => v.to_string(),
                            Value::Int32(v) => v.to_string(),
                            Value::Int64(v) => v.to_string(),
                            Value::Money(v) => v.to_string(),
                            Value::Float64(v) => v.to_string(),
                            Value::Numeric(v) => v.render(),
                            Value::Bytea(v) => {
                                let mut rendered = String::from("\\\\x");
                                for byte in v {
                                    rendered.push_str(&format!("{byte:02x}"));
                                }
                                rendered
                            }
                            Value::Bit(v) => v.render(),
                            Value::TsVector(v) => crate::backend::executor::render_tsvector_text(v),
                            Value::TsQuery(v) => crate::backend::executor::render_tsquery_text(v),
                            Value::Json(v) => v.to_string(),
                            Value::JsonPath(v) => v.to_string(),
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
        SqlTypeKind::Record => 1,
        SqlTypeKind::Composite => 2,
        SqlTypeKind::Int2 => 3,
        SqlTypeKind::Int2Vector => 4,
        SqlTypeKind::Int4 => 5,
        SqlTypeKind::Int8 => 6,
        SqlTypeKind::Name => 7,
        SqlTypeKind::Oid => 8,
        SqlTypeKind::Tid => 9,
        SqlTypeKind::Xid => 10,
        SqlTypeKind::OidVector => 11,
        SqlTypeKind::Bit => 12,
        SqlTypeKind::VarBit => 13,
        SqlTypeKind::Bytea => 14,
        SqlTypeKind::Float4 => 15,
        SqlTypeKind::Float8 => 16,
        SqlTypeKind::Money => 17,
        SqlTypeKind::Numeric => 18,
        SqlTypeKind::Int4Range => 19,
        SqlTypeKind::Int8Range => 20,
        SqlTypeKind::NumericRange => 21,
        SqlTypeKind::DateRange => 22,
        SqlTypeKind::TimestampRange => 23,
        SqlTypeKind::TimestampTzRange => 24,
        SqlTypeKind::Json => 25,
        SqlTypeKind::Jsonb => 26,
        SqlTypeKind::JsonPath => 27,
        SqlTypeKind::Date => 28,
        SqlTypeKind::Time => 29,
        SqlTypeKind::TimeTz => 30,
        SqlTypeKind::Interval => 31,
        SqlTypeKind::TsVector => 32,
        SqlTypeKind::TsQuery => 33,
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

fn sql_type_kind_from_tag(tag: u8) -> Result<SqlTypeKind, ExecError> {
    Ok(match tag {
        0 => SqlTypeKind::AnyArray,
        1 => SqlTypeKind::Record,
        2 => SqlTypeKind::Composite,
        3 => SqlTypeKind::Int2,
        4 => SqlTypeKind::Int2Vector,
        5 => SqlTypeKind::Int4,
        6 => SqlTypeKind::Int8,
        7 => SqlTypeKind::Name,
        8 => SqlTypeKind::Oid,
        9 => SqlTypeKind::Tid,
        10 => SqlTypeKind::Xid,
        11 => SqlTypeKind::OidVector,
        12 => SqlTypeKind::Bit,
        13 => SqlTypeKind::VarBit,
        14 => SqlTypeKind::Bytea,
        15 => SqlTypeKind::Float4,
        16 => SqlTypeKind::Float8,
        17 => SqlTypeKind::Money,
        18 => SqlTypeKind::Numeric,
        19 => SqlTypeKind::Int4Range,
        20 => SqlTypeKind::Int8Range,
        21 => SqlTypeKind::NumericRange,
        22 => SqlTypeKind::DateRange,
        23 => SqlTypeKind::TimestampRange,
        24 => SqlTypeKind::TimestampTzRange,
        25 => SqlTypeKind::Json,
        26 => SqlTypeKind::Jsonb,
        27 => SqlTypeKind::JsonPath,
        28 => SqlTypeKind::Date,
        29 => SqlTypeKind::Time,
        30 => SqlTypeKind::TimeTz,
        31 => SqlTypeKind::Interval,
        32 => SqlTypeKind::TsVector,
        33 => SqlTypeKind::TsQuery,
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
    out.push(sql_type_kind_tag(sql_type.kind));
    out.extend_from_slice(&sql_type.typmod.to_le_bytes());
    out.push(u8::from(sql_type.is_array));
    out.extend_from_slice(&sql_type.type_oid.to_le_bytes());
    out.extend_from_slice(&sql_type.typrelid.to_le_bytes());
}

fn decode_sql_type_identity(bytes: &[u8], offset: &mut usize) -> Result<SqlType, ExecError> {
    if *offset + 14 > bytes.len() {
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
    Ok(SqlType {
        kind,
        typmod,
        is_array,
        type_oid,
        typrelid,
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
        Value::Int64(v) => {
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
        Value::Bit(v) => {
            out.push(INTERNAL_VALUE_TAG_BIT);
            out.extend_from_slice(&v.bit_len.to_le_bytes());
            encode_internal_text(&v.bytes, &mut out);
        }
        Value::Bytea(v) => {
            out.push(INTERNAL_VALUE_TAG_BYTEA);
            encode_internal_text(&v, &mut out);
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
            out.push(match v.kind {
                crate::include::nodes::datum::RangeTypeId::Int4Range => 0,
                crate::include::nodes::datum::RangeTypeId::Int8Range => 1,
                crate::include::nodes::datum::RangeTypeId::NumericRange => 2,
                crate::include::nodes::datum::RangeTypeId::DateRange => 3,
                crate::include::nodes::datum::RangeTypeId::TimestampRange => 4,
                crate::include::nodes::datum::RangeTypeId::TimestampTzRange => 5,
            });
            encode_internal_text(
                crate::backend::executor::render_range_text(&Value::Range(v))
                    .unwrap_or_default()
                    .as_bytes(),
                &mut out,
            );
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
            let kind = match *rest
                .get(offset)
                .ok_or_else(|| ExecError::InvalidStorageValue {
                    column: "<record>".into(),
                    details: "missing range kind".into(),
                })? {
                0 => SqlTypeKind::Int4Range,
                1 => SqlTypeKind::Int8Range,
                2 => SqlTypeKind::NumericRange,
                3 => SqlTypeKind::DateRange,
                4 => SqlTypeKind::TimestampRange,
                5 => SqlTypeKind::TimestampTzRange,
                _ => {
                    return Err(ExecError::InvalidStorageValue {
                        column: "<record>".into(),
                        details: "invalid range kind".into(),
                    });
                }
            };
            offset += 1;
            let text =
                std::str::from_utf8(decode_internal_text(rest, &mut offset)?).unwrap_or_default();
            crate::backend::executor::expr_range::parse_range_text(text, kind)?
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
    desc.columns
        .iter()
        .zip(values.iter())
        .map(|(column, value)| encode_value(column, value))
        .collect::<Result<Vec<_>, _>>()
}

pub(crate) fn encode_value(column: &ColumnDesc, value: &Value) -> Result<TupleValue, ExecError> {
    if matches!(value, Value::Null) {
        return if !column.storage.nullable {
            Err(ExecError::MissingRequiredColumn(column.name.clone()))
        } else {
            Ok(TupleValue::Null)
        };
    }

    let coerced = coerce_assignment_value(value, column.sql_type)?;
    match (&column.ty, coerced) {
        (ScalarType::Int16, Value::Int16(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Int32, Value::Int32(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Int32, Value::Int64(v))
            if matches!(
                column.sql_type.kind,
                SqlTypeKind::Oid
                    | SqlTypeKind::Xid
                    | SqlTypeKind::RegConfig
                    | SqlTypeKind::RegDictionary
            ) =>
        {
            let oid = u32::try_from(v).map_err(|_| ExecError::OidOutOfRange)?;
            Ok(TupleValue::Bytes(oid.to_le_bytes().to_vec()))
        }
        (ScalarType::Int64, Value::Int64(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
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
        (ScalarType::Range(_), Value::Range(range)) => {
            Ok(TupleValue::Bytes(encode_range_bytes(&range)?))
        }
        (ScalarType::BitString, Value::Bit(v)) => {
            let mut bytes = Vec::with_capacity(4 + v.bytes.len());
            bytes.extend_from_slice(&(v.bit_len as u32).to_le_bytes());
            bytes.extend_from_slice(&v.bytes);
            Ok(TupleValue::Bytes(bytes))
        }
        (ScalarType::Bytea, Value::Bytea(v)) => Ok(TupleValue::Bytes(v)),
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
        (ScalarType::TsVector, Value::TsVector(vector)) => Ok(TupleValue::Bytes(
            crate::backend::executor::encode_tsvector_bytes(&vector),
        )),
        (ScalarType::TsQuery, Value::TsQuery(query)) => Ok(TupleValue::Bytes(
            crate::backend::executor::encode_tsquery_bytes(&query),
        )),
        (ScalarType::Text, Value::InternalChar(v)) => {
            Ok(TupleValue::Bytes(render_internal_char_text(v).into_bytes()))
        }
        (ScalarType::Text, value) => Ok(TupleValue::Bytes(
            value.as_text().unwrap().as_bytes().to_vec(),
        )),
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
            column.sql_type.element_type(),
            &ArrayValue::from_1d(items),
        )?)),
        (ScalarType::Array(_), Value::PgArray(array)) => Ok(TupleValue::Bytes(encode_array_bytes(
            column.sql_type.element_type(),
            &array,
        )?)),
        (_, other) => Err(ExecError::TypeMismatch {
            op: "assignment",
            left: Value::Null,
            right: other,
        }),
    }
}

pub(crate) fn coerce_assignment_value(value: &Value, target: SqlType) -> Result<Value, ExecError> {
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
                let mut coerced = Vec::with_capacity(items.len());
                for item in items {
                    coerced.push(coerce_assignment_value(item, element_type)?);
                }
                Ok(Value::Array(coerced))
            }
            Value::PgArray(array) => {
                let element_type = target.element_type();
                let mut coerced = Vec::with_capacity(array.elements.len());
                for item in &array.elements {
                    coerced.push(coerce_assignment_value(item, element_type)?);
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
        Value::Int16(v) => cast_text_value(&v.to_string(), target, false),
        Value::Int32(v) => cast_text_value(&v.to_string(), target, false),
        Value::Int64(v) => cast_text_value(&v.to_string(), target, false),
        Value::Money(v) => cast_text_value(
            &crate::backend::executor::money_format_text(*v),
            target,
            false,
        ),
        Value::Date(v) => cast_value(Value::Date(*v), target),
        Value::Time(v) => cast_value(Value::Time(*v), target),
        Value::TimeTz(v) => cast_value(Value::TimeTz(*v), target),
        Value::Timestamp(v) => cast_value(Value::Timestamp(*v), target),
        Value::TimestampTz(v) => cast_value(Value::TimestampTz(*v), target),
        Value::Bit(bits) => match target.kind {
            SqlTypeKind::Bit | SqlTypeKind::VarBit => {
                Ok(Value::Bit(coerce_bit_string(bits.clone(), target, false)?))
            }
            _ => cast_value(Value::Bit(bits.clone()), target),
        },
        Value::Bool(v) => cast_text_value(if *v { "true" } else { "false" }, target, false),
        Value::Float64(v) => match target.kind {
            SqlTypeKind::Float4
            | SqlTypeKind::Float8
            | SqlTypeKind::Numeric
            | SqlTypeKind::Int2
            | SqlTypeKind::Int4
            | SqlTypeKind::Int8
            | SqlTypeKind::Oid => cast_value(Value::Float64(*v), target),
            _ => cast_text_value(&v.to_string(), target, false),
        },
        Value::Numeric(numeric) => cast_numeric_value(numeric.clone(), target, false),
        Value::JsonPath(text) => cast_text_value(text.as_str(), target, false),
        Value::Json(text) => cast_text_value(text.as_str(), target, false),
        Value::Jsonb(bytes) => cast_text_value(&render_jsonb_bytes(bytes)?, target, false),
        Value::Bytea(bytes) => cast_value(Value::Bytea(bytes.clone()), target),
        Value::TsVector(vector) => cast_text_value(
            &crate::backend::executor::render_tsvector_text(vector),
            target,
            false,
        ),
        Value::TsQuery(query) => cast_text_value(
            &crate::backend::executor::render_tsquery_text(query),
            target,
            false,
        ),
        Value::Text(text) => cast_text_value(text.as_str(), target, false),
        Value::TextRef(_, _) => cast_text_value(value.as_text().unwrap(), target, false),
        Value::InternalChar(byte) => cast_value(Value::InternalChar(*byte), target),
        Value::Range(range) => Ok(Value::Range(range.clone())),
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => cast_value(value.clone(), target),
        Value::Array(items) => Ok(Value::Array(items.clone())),
        Value::PgArray(array) => Ok(Value::PgArray(array.clone())),
        Value::Record(record) => Ok(Value::Record(record.clone())),
    }
}

pub(crate) fn decode_value(column: &ColumnDesc, bytes: Option<&[u8]>) -> Result<Value, ExecError> {
    decode_value_with_toast(column, bytes, None)
}

pub(crate) fn decode_value_with_toast(
    column: &ColumnDesc,
    bytes: Option<&[u8]>,
    toast: Option<&ToastFetchContext>,
) -> Result<Value, ExecError> {
    let Some(bytes) = bytes else {
        return Ok(Value::Null);
    };
    let owned;
    let bytes = if let Some(toast) = toast {
        if crate::include::access::detoast::is_ondisk_toast_pointer(bytes) {
            owned = crate::backend::access::common::detoast::detoast_value_bytes(toast, bytes)?;
            &owned[..]
        } else {
            bytes
        }
    } else {
        bytes
    };

    match column.ty {
        ScalarType::Int16 => {
            if column.storage.attlen != 2 || bytes.len() != 2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
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
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            let raw = i32::from_le_bytes(bytes.try_into().map_err(|_| {
                ExecError::InvalidStorageValue {
                    column: column.name.clone(),
                    details: "int4 must be exactly 4 bytes".into(),
                }
            })?);
            if matches!(
                column.sql_type.kind,
                SqlTypeKind::Oid | SqlTypeKind::RegConfig | SqlTypeKind::RegDictionary
            ) {
                Ok(Value::Int64(raw as u32 as i64))
            } else {
                Ok(Value::Int32(raw))
            }
        }
        ScalarType::Int64 => {
            if column.storage.attlen != 8 || bytes.len() != 8 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
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
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
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
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Date(crate::include::nodes::datetime::DateADT(
                i32::from_le_bytes(bytes.try_into().unwrap()),
            )))
        }
        ScalarType::Time => {
            if column.storage.attlen != 8 || bytes.len() != 8 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Time(crate::include::nodes::datetime::TimeADT(
                i64::from_le_bytes(bytes.try_into().unwrap()),
            )))
        }
        ScalarType::TimeTz => {
            if column.storage.attlen != 12 || bytes.len() != 12 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
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
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Timestamp(
                crate::include::nodes::datetime::TimestampADT(i64::from_le_bytes(
                    bytes.try_into().unwrap(),
                )),
            ))
        }
        ScalarType::TimestampTz => {
            if column.storage.attlen != 8 || bytes.len() != 8 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::TimestampTz(
                crate::include::nodes::datetime::TimestampTzADT(i64::from_le_bytes(
                    bytes.try_into().unwrap(),
                )),
            ))
        }
        ScalarType::BitString => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
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
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Bytea(bytes.to_vec()))
        }
        ScalarType::Float32 => {
            if column.storage.attlen != 4 || bytes.len() != 4 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
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
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
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
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Point(GeoPoint {
                x: f64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                y: f64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            }))
        }
        ScalarType::Line => {
            if column.storage.attlen != 24 || bytes.len() != 24 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Line(GeoLine {
                a: f64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                b: f64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                c: f64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            }))
        }
        ScalarType::Lseg => {
            if column.storage.attlen != 32 || bytes.len() != 32 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
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
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
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
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
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
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
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
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            let text = unsafe { std::str::from_utf8_unchecked(bytes) };
            validate_json_text(text)?;
            Ok(Value::Json(CompactString::new(text)))
        }
        ScalarType::Jsonb => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            decode_jsonb(bytes)?;
            Ok(Value::Jsonb(bytes.to_vec()))
        }
        ScalarType::JsonPath => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            let text = unsafe { std::str::from_utf8_unchecked(bytes) };
            Ok(Value::JsonPath(canonicalize_jsonpath_text(text)?))
        }
        ScalarType::TsVector => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::TsVector(
                crate::backend::executor::decode_tsvector_bytes(bytes)?,
            ))
        }
        ScalarType::TsQuery => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::TsQuery(
                crate::backend::executor::decode_tsquery_bytes(bytes)?,
            ))
        }
        ScalarType::Text => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Text(CompactString::new(unsafe {
                std::str::from_utf8_unchecked(bytes)
            })))
        }
        ScalarType::Record => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            decode_composite_datum(bytes).map(Value::Record)
        }
        ScalarType::Path => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Path(decode_path_bytes(bytes)?))
        }
        ScalarType::Polygon => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Polygon(decode_polygon_bytes(bytes)?))
        }
        ScalarType::Bool => {
            if column.storage.attlen != 1 || bytes.len() != 1 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
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
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            if column.sql_type.kind == SqlTypeKind::AnyArray {
                decode_anyarray_bytes(bytes)
            } else {
                decode_array_bytes(column.sql_type.element_type(), bytes)
            }
        }
        ScalarType::Range(kind) => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty.clone(),
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Range(decode_range_bytes(kind, bytes)?))
        }
    }
}

pub(crate) fn missing_column_value(column: &ColumnDesc) -> Value {
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
    use crate::include::nodes::datum::{ArrayDimension, RecordValue};

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
}
