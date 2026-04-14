use super::ExecError;
use super::exec_expr::parse_numeric_text;
use super::expr_bit::{coerce_bit_string, render_bit_text};
use super::expr_casts::{
    cast_numeric_value, cast_text_value, cast_value, parse_text_array_literal_with_options,
    render_internal_char_text,
};
use super::expr_datetime::render_datetime_value_text;
use super::expr_geometry::{
    decode_path_bytes, decode_polygon_bytes, encode_path_bytes, encode_polygon_bytes,
    render_geometry_text,
};
use super::node_types::*;
use crate::backend::executor::expr_json::{canonicalize_jsonpath_text, validate_json_text};
use crate::backend::executor::jsonb::{decode_jsonb, render_jsonb_bytes};
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::access::htup::{HeapTuple, TupleValue};
use crate::include::nodes::execnodes::ToastFetchContext;
use crate::pgrust::compact_string::CompactString;

mod array;

pub use array::format_array_value_text;
pub(crate) use array::{
    decode_anyarray_bytes, decode_array_bytes, encode_anyarray_bytes, encode_array_bytes,
    format_array_text,
};

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
                SqlTypeKind::Oid | SqlTypeKind::RegConfig | SqlTypeKind::RegDictionary
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
        (ScalarType::Text | ScalarType::Record, Value::InternalChar(v)) => {
            Ok(TupleValue::Bytes(render_internal_char_text(v).into_bytes()))
        }
        (ScalarType::Text | ScalarType::Record, value) => Ok(TupleValue::Bytes(
            value.as_text().unwrap().as_bytes().to_vec(),
        )),
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
        Value::Money(v) => cast_text_value(&crate::backend::executor::money_format_text(*v), target, false),
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
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => cast_value(value.clone(), target),
        Value::Array(items) => Ok(Value::Array(items.clone())),
        Value::PgArray(array) => Ok(Value::PgArray(array.clone())),
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
        ScalarType::Text | ScalarType::Record => {
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
    }
}

pub(crate) fn missing_column_value(column: &ColumnDesc) -> Value {
    column
        .missing_default_value
        .clone()
        .or_else(|| {
            column.default_expr.as_deref().and_then(|sql| {
                crate::backend::parser::derive_literal_default_value(sql, column.sql_type).ok()
            })
        })
        .unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::catalog::column_desc;
    use crate::include::nodes::datum::ArrayDimension;

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
