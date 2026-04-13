use super::ExecError;
use super::exec_expr::parse_numeric_text;
use super::expr_bit::{coerce_bit_string, render_bit_text};
use super::expr_casts::{
    cast_numeric_value, cast_text_value, cast_value, parse_text_array_literal_with_options,
    render_internal_char_text,
};
use super::expr_geometry::{
    decode_path_bytes, decode_polygon_bytes, encode_path_bytes, encode_polygon_bytes,
    render_geometry_text,
};
use super::expr_datetime::render_datetime_value_text;
use super::node_types::*;
use crate::backend::executor::expr_json::{canonicalize_jsonpath_text, validate_json_text};
use crate::backend::executor::jsonb::{decode_jsonb, render_jsonb_bytes};
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::access::htup::{HeapTuple, TupleValue};
use crate::include::nodes::execnodes::ToastFetchContext;
use crate::pgrust::compact_string::CompactString;

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
    desc
        .columns
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
        (ScalarType::Bool, Value::Bool(v)) => Ok(TupleValue::Bytes(vec![u8::from(v)])),
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
            decode_array_bytes(column.sql_type.element_type(), bytes)
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

fn encode_array_bytes(element_type: SqlType, array: &ArrayValue) -> Result<Vec<u8>, ExecError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(array.dimensions.len() as u32).to_le_bytes());
    for dim in &array.dimensions {
        bytes.extend_from_slice(&(dim.length as u32).to_le_bytes());
        bytes.extend_from_slice(&dim.lower_bound.to_le_bytes());
    }
    bytes.extend_from_slice(&(array.elements.len() as u32).to_le_bytes());
    for item in &array.elements {
        match item {
            Value::Null => bytes.extend_from_slice(&(-1_i32).to_le_bytes()),
            _ => {
                let payload = encode_array_element(element_type, item)?;
                bytes.extend_from_slice(&(payload.len() as i32).to_le_bytes());
                bytes.extend_from_slice(&payload);
            }
        };
    }
    Ok(bytes)
}

fn encode_array_element(element_type: SqlType, value: &Value) -> Result<Vec<u8>, ExecError> {
    let coerced = coerce_assignment_value(value, element_type)?;
    match coerced {
        Value::Null => Ok(Vec::new()),
        Value::Int16(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Int32(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Int64(v)
            if matches!(
                element_type.kind,
                SqlTypeKind::Oid | SqlTypeKind::RegConfig | SqlTypeKind::RegDictionary
            ) =>
        {
            let oid = u32::try_from(v).map_err(|_| ExecError::OidOutOfRange)?;
            Ok(oid.to_le_bytes().to_vec())
        }
        Value::Int64(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Date(v) => Ok(v.0.to_le_bytes().to_vec()),
        Value::Time(v) => Ok(v.0.to_le_bytes().to_vec()),
        Value::TimeTz(v) => {
            let mut bytes = Vec::with_capacity(12);
            bytes.extend_from_slice(&v.time.0.to_le_bytes());
            bytes.extend_from_slice(&v.offset_seconds.to_le_bytes());
            Ok(bytes)
        }
        Value::Timestamp(v) => Ok(v.0.to_le_bytes().to_vec()),
        Value::TimestampTz(v) => Ok(v.0.to_le_bytes().to_vec()),
        Value::Bit(v) => {
            let mut bytes = Vec::with_capacity(4 + v.bytes.len());
            bytes.extend_from_slice(&(v.bit_len as u32).to_le_bytes());
            bytes.extend_from_slice(&v.bytes);
            Ok(bytes)
        }
        Value::Bytea(v) => Ok(v),
        Value::Bool(v) => Ok(vec![u8::from(v)]),
        Value::Numeric(text) => Ok(text.render().into_bytes()),
        Value::Json(text) => Ok(text.as_bytes().to_vec()),
        Value::Text(text) => Ok(text.as_bytes().to_vec()),
        Value::TextRef(_, _) => Ok(coerced.as_text().unwrap().as_bytes().to_vec()),
        Value::TsVector(vector) => Ok(crate::backend::executor::encode_tsvector_bytes(&vector)),
        Value::TsQuery(query) => Ok(crate::backend::executor::encode_tsquery_bytes(&query)),
        Value::InternalChar(v) => Ok(vec![v]),
        Value::Float64(v) => {
            if matches!(element_type.kind, SqlTypeKind::Float4) {
                Ok((v as f32).to_le_bytes().to_vec())
            } else {
                Ok(v.to_le_bytes().to_vec())
            }
        }
        Value::JsonPath(text) => Ok(text.as_bytes().to_vec()),
        Value::Point(point) => {
            let mut bytes = Vec::with_capacity(16);
            bytes.extend_from_slice(&point.x.to_le_bytes());
            bytes.extend_from_slice(&point.y.to_le_bytes());
            Ok(bytes)
        }
        Value::Line(line) => {
            let mut bytes = Vec::with_capacity(24);
            bytes.extend_from_slice(&line.a.to_le_bytes());
            bytes.extend_from_slice(&line.b.to_le_bytes());
            bytes.extend_from_slice(&line.c.to_le_bytes());
            Ok(bytes)
        }
        Value::Lseg(lseg) => {
            let mut bytes = Vec::with_capacity(32);
            for point in &lseg.p {
                bytes.extend_from_slice(&point.x.to_le_bytes());
                bytes.extend_from_slice(&point.y.to_le_bytes());
            }
            Ok(bytes)
        }
        Value::Box(geo_box) => {
            let mut bytes = Vec::with_capacity(32);
            bytes.extend_from_slice(&geo_box.high.x.to_le_bytes());
            bytes.extend_from_slice(&geo_box.high.y.to_le_bytes());
            bytes.extend_from_slice(&geo_box.low.x.to_le_bytes());
            bytes.extend_from_slice(&geo_box.low.y.to_le_bytes());
            Ok(bytes)
        }
        Value::Circle(circle) => {
            let mut bytes = Vec::with_capacity(24);
            bytes.extend_from_slice(&circle.center.x.to_le_bytes());
            bytes.extend_from_slice(&circle.center.y.to_le_bytes());
            bytes.extend_from_slice(&circle.radius.to_le_bytes());
            Ok(bytes)
        }
        Value::Path(path) => Ok(encode_path_bytes(&path)),
        Value::Polygon(poly) => Ok(encode_polygon_bytes(&poly)),
        Value::Array(_) | Value::PgArray(_) => Err(ExecError::TypeMismatch {
            op: "array element",
            left: coerced,
            right: Value::Null,
        }),
        Value::Jsonb(bytes) => Ok(bytes),
    }
}

fn decode_array_bytes(element_type: SqlType, bytes: &[u8]) -> Result<Value, ExecError> {
    if bytes.len() < 4 {
        return Err(ExecError::InvalidStorageValue {
            column: "<array>".into(),
            details: "array payload too short".into(),
        });
    }
    let ndim = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let mut offset = 4usize;
    let mut dimensions = Vec::with_capacity(ndim);
    for _ in 0..ndim {
        if offset + 8 > bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: "<array>".into(),
                details: "array dimension header truncated".into(),
            });
        }
        let length = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let lower_bound = i32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        dimensions.push(ArrayDimension {
            lower_bound,
            length,
        });
    }
    if offset + 4 > bytes.len() {
        return Err(ExecError::InvalidStorageValue {
            column: "<array>".into(),
            details: "array element count header truncated".into(),
        });
    }
    let count = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 4 > bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: "<array>".into(),
                details: "array length header truncated".into(),
            });
        }
        let len = i32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if len == -1 {
            items.push(Value::Null);
            continue;
        }
        let len = len as usize;
        if offset + len > bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: "<array>".into(),
                details: "array element payload truncated".into(),
            });
        }
        items.push(decode_array_element(
            element_type,
            &bytes[offset..offset + len],
        )?);
        offset += len;
    }
    Ok(Value::PgArray(ArrayValue::from_dimensions(
        dimensions, items,
    )))
}

fn decode_array_element(element_type: SqlType, bytes: &[u8]) -> Result<Value, ExecError> {
    match element_type.kind {
        SqlTypeKind::Int2 => {
            if bytes.len() != 2 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "int2 array element must be 2 bytes".into(),
                });
            }
            Ok(Value::Int16(i16::from_le_bytes(bytes.try_into().unwrap())))
        }
        SqlTypeKind::Int4
        | SqlTypeKind::Oid
        | SqlTypeKind::RegConfig
        | SqlTypeKind::RegDictionary => {
            if bytes.len() != 4 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "int4 array element must be 4 bytes".into(),
                });
            }
            Ok(Value::Int32(i32::from_le_bytes(bytes.try_into().unwrap())))
        }
        SqlTypeKind::Int8 => {
            if bytes.len() != 8 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "int8 array element must be 8 bytes".into(),
                });
            }
            Ok(Value::Int64(i64::from_le_bytes(bytes.try_into().unwrap())))
        }
        SqlTypeKind::Date => {
            if bytes.len() != 4 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "date array element must be 4 bytes".into(),
                });
            }
            Ok(Value::Date(crate::include::nodes::datetime::DateADT(
                i32::from_le_bytes(bytes.try_into().unwrap()),
            )))
        }
        SqlTypeKind::Time => {
            if bytes.len() != 8 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "time array element must be 8 bytes".into(),
                });
            }
            Ok(Value::Time(crate::include::nodes::datetime::TimeADT(
                i64::from_le_bytes(bytes.try_into().unwrap()),
            )))
        }
        SqlTypeKind::TimeTz => {
            if bytes.len() != 12 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "timetz array element must be 12 bytes".into(),
                });
            }
            Ok(Value::TimeTz(crate::include::nodes::datetime::TimeTzADT {
                time: crate::include::nodes::datetime::TimeADT(i64::from_le_bytes(
                    bytes[0..8].try_into().unwrap(),
                )),
                offset_seconds: i32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            }))
        }
        SqlTypeKind::Timestamp => {
            if bytes.len() != 8 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "timestamp array element must be 8 bytes".into(),
                });
            }
            Ok(Value::Timestamp(
                crate::include::nodes::datetime::TimestampADT(i64::from_le_bytes(
                    bytes.try_into().unwrap(),
                )),
            ))
        }
        SqlTypeKind::TimestampTz => {
            if bytes.len() != 8 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "timestamptz array element must be 8 bytes".into(),
                });
            }
            Ok(Value::TimestampTz(
                crate::include::nodes::datetime::TimestampTzADT(i64::from_le_bytes(
                    bytes.try_into().unwrap(),
                )),
            ))
        }
        SqlTypeKind::Float4 | SqlTypeKind::Float8 => {
            let width = if matches!(element_type.kind, SqlTypeKind::Float4) {
                4
            } else {
                8
            };
            if bytes.len() != width {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "float array element has wrong width".into(),
                });
            }
            if matches!(element_type.kind, SqlTypeKind::Float4) {
                Ok(Value::Float64(
                    f32::from_le_bytes(bytes.try_into().unwrap()) as f64,
                ))
            } else {
                Ok(Value::Float64(f64::from_le_bytes(
                    bytes.try_into().unwrap(),
                )))
            }
        }
        SqlTypeKind::Numeric => Ok(Value::Numeric(
            parse_numeric_text(unsafe { std::str::from_utf8_unchecked(bytes) }).ok_or_else(
                || ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "invalid numeric array element".into(),
                },
            )?,
        )),
        SqlTypeKind::Bit | SqlTypeKind::VarBit => {
            if bytes.len() < 4 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "bit array element payload too short".into(),
                });
            }
            let bit_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as i32;
            Ok(Value::Bit(crate::include::nodes::datum::BitString::new(
                bit_len,
                bytes[4..].to_vec(),
            )))
        }
        SqlTypeKind::Bytea => Ok(Value::Bytea(bytes.to_vec())),
        SqlTypeKind::Point => {
            if bytes.len() != 16 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "point array element must be 16 bytes".into(),
                });
            }
            Ok(Value::Point(GeoPoint {
                x: f64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                y: f64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            }))
        }
        SqlTypeKind::Line => {
            if bytes.len() != 24 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "line array element must be 24 bytes".into(),
                });
            }
            Ok(Value::Line(GeoLine {
                a: f64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                b: f64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                c: f64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            }))
        }
        SqlTypeKind::Lseg => {
            if bytes.len() != 32 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "lseg array element must be 32 bytes".into(),
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
        SqlTypeKind::Box => {
            if bytes.len() != 32 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "box array element must be 32 bytes".into(),
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
        SqlTypeKind::Circle => {
            if bytes.len() != 24 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "circle array element must be 24 bytes".into(),
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
        SqlTypeKind::Path => Ok(Value::Path(decode_path_bytes(bytes)?)),
        SqlTypeKind::Polygon => Ok(Value::Polygon(decode_polygon_bytes(bytes)?)),
        SqlTypeKind::Json => {
            let text = unsafe { std::str::from_utf8_unchecked(bytes) };
            validate_json_text(text)?;
            Ok(Value::Json(CompactString::new(text)))
        }
        SqlTypeKind::Jsonb => {
            decode_jsonb(bytes)?;
            Ok(Value::Jsonb(bytes.to_vec()))
        }
        SqlTypeKind::JsonPath => {
            let text = unsafe { std::str::from_utf8_unchecked(bytes) };
            Ok(Value::JsonPath(canonicalize_jsonpath_text(text)?))
        }
        SqlTypeKind::TsVector => Ok(Value::TsVector(
            crate::backend::executor::decode_tsvector_bytes(bytes)?,
        )),
        SqlTypeKind::TsQuery => Ok(Value::TsQuery(
            crate::backend::executor::decode_tsquery_bytes(bytes)?,
        )),
        SqlTypeKind::Bool => {
            if bytes.len() != 1 {
                return Err(ExecError::InvalidStorageValue {
                    column: "<array>".into(),
                    details: "bool array element must be 1 byte".into(),
                });
            }
            Ok(Value::Bool(bytes[0] != 0))
        }
        SqlTypeKind::Text
        | SqlTypeKind::Name
        | SqlTypeKind::Int2Vector
        | SqlTypeKind::OidVector
        | SqlTypeKind::PgNodeTree
        | SqlTypeKind::InternalChar
        | SqlTypeKind::Char
        | SqlTypeKind::Varchar => Ok(Value::Text(CompactString::new(unsafe {
            std::str::from_utf8_unchecked(bytes)
        }))),
    }
}

pub(crate) fn format_array_text(items: &[Value]) -> String {
    match ArrayValue::from_nested_values(items.to_vec(), vec![1]) {
        Ok(array) => format_array_value_text(&array),
        Err(_) => format_array_value_text(&ArrayValue::from_1d(items.to_vec())),
    }
}

pub fn format_array_value_text(array: &ArrayValue) -> String {
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
    out.push_str(&format_array_values_nested(array, 0, &mut 0usize));
    out
}

fn format_array_values_nested(array: &ArrayValue, depth: usize, offset: &mut usize) -> String {
    let mut out = String::from("{");
    let len = array.dimensions[depth].length;
    for idx in 0..len {
        if idx > 0 {
            out.push(',');
        }
        if depth + 1 < array.dimensions.len() {
            out.push_str(&format_array_values_nested(array, depth + 1, offset));
            continue;
        }
        let item = &array.elements[*offset];
        *offset += 1;
        match item {
            Value::Null => out.push_str("NULL"),
            Value::Int16(v) => out.push_str(&v.to_string()),
            Value::Int32(v) => out.push_str(&v.to_string()),
            Value::Int64(v) => out.push_str(&v.to_string()),
            Value::Float64(v) => out.push_str(&v.to_string()),
            Value::Numeric(v) => out.push_str(&v.render()),
            Value::Date(_)
            | Value::Time(_)
            | Value::TimeTz(_)
            | Value::Timestamp(_)
            | Value::TimestampTz(_) => push_array_text_element(
                &mut out,
                &render_datetime_value_text(item).expect("datetime values render"),
            ),
            Value::Bit(v) => out.push_str(&render_bit_text(v)),
            Value::Bytea(v) => {
                let rendered = crate::backend::libpq::pqformat::format_bytea_text(
                    v,
                    crate::pgrust::session::ByteaOutputFormat::Hex,
                );
                out.push('"');
                out.push_str(&rendered);
                out.push('"');
            }
            Value::Json(v) => {
                out.push('"');
                for ch in v.chars() {
                    match ch {
                        '"' | '\\' => {
                            out.push('\\');
                            out.push(ch);
                        }
                        _ => out.push(ch),
                    }
                }
                out.push('"');
            }
            Value::JsonPath(v) => {
                out.push('"');
                for ch in v.chars() {
                    match ch {
                        '"' | '\\' => {
                            out.push('\\');
                            out.push(ch);
                        }
                        _ => out.push(ch),
                    }
                }
                out.push('"');
            }
            Value::TsVector(v) => {
                let rendered = crate::backend::executor::render_tsvector_text(v);
                out.push('"');
                for ch in rendered.chars() {
                    match ch {
                        '"' | '\\' => {
                            out.push('\\');
                            out.push(ch);
                        }
                        _ => out.push(ch),
                    }
                }
                out.push('"');
            }
            Value::TsQuery(v) => {
                let rendered = crate::backend::executor::render_tsquery_text(v);
                out.push('"');
                for ch in rendered.chars() {
                    match ch {
                        '"' | '\\' => {
                            out.push('\\');
                            out.push(ch);
                        }
                        _ => out.push(ch),
                    }
                }
                out.push('"');
            }
            Value::Jsonb(v) => {
                let rendered = render_jsonb_bytes(v).unwrap_or_else(|_| "null".into());
                out.push('"');
                for ch in rendered.chars() {
                    match ch {
                        '"' | '\\' => {
                            out.push('\\');
                            out.push(ch);
                        }
                        _ => out.push(ch),
                    }
                }
                out.push('"');
            }
            Value::Bool(v) => out.push_str(if *v { "true" } else { "false" }),
            Value::Point(_)
            | Value::Lseg(_)
            | Value::Path(_)
            | Value::Line(_)
            | Value::Box(_)
            | Value::Polygon(_)
            | Value::Circle(_) => {
                let rendered = render_geometry_text(item, Default::default()).unwrap_or_default();
                out.push('"');
                for ch in rendered.chars() {
                    match ch {
                        '"' | '\\' => {
                            out.push('\\');
                            out.push(ch);
                        }
                        _ => out.push(ch),
                    }
                }
                out.push('"');
            }
            Value::Text(_) | Value::TextRef(_, _) => {
                push_array_text_element(&mut out, item.as_text().unwrap());
            }
            Value::InternalChar(byte) => {
                let rendered = super::expr_casts::render_internal_char_text(*byte);
                push_array_text_element(&mut out, &rendered);
            }
            Value::Array(nested) => out.push_str(&format_array_text(nested)),
            Value::PgArray(nested) => out.push_str(&format_array_value_text(nested)),
        }
    }
    out.push('}');
    out
}

fn push_array_text_element(out: &mut String, text: &str) {
    if array_text_needs_quotes(text) {
        out.push('"');
        for ch in text.chars() {
            match ch {
                '"' | '\\' => {
                    out.push('\\');
                    out.push(ch);
                }
                _ => out.push(ch),
            }
        }
        out.push('"');
    } else {
        out.push_str(text);
    }
}

fn array_text_needs_quotes(text: &str) -> bool {
    text.is_empty()
        || text.eq_ignore_ascii_case("null")
        || text.chars().any(|ch| {
            ch.is_whitespace() || matches!(ch, '"' | '\\' | '{' | '}' | ',' | '\n' | '\r' | '\t')
        })
}
