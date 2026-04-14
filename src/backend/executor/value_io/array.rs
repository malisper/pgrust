use super::*;
use crate::backend::storage::page::bufpage::max_align;
use crate::include::access::htup::AttributeAlign;
use crate::include::catalog::builtin_type_rows;

pub(crate) fn encode_array_bytes(
    element_type: SqlType,
    array: &ArrayValue,
) -> Result<Vec<u8>, ExecError> {
    let element_oid = builtin_type_oid_for_sql_type(element_type).ok_or_else(|| {
        ExecError::InvalidStorageValue {
            column: "<array>".into(),
            details: format!("unsupported array element type {:?}", element_type),
        }
    })?;
    encode_flat_array_bytes(element_type, element_oid, array, "<array>")
}

pub(crate) fn encode_anyarray_bytes(array: &ArrayValue) -> Result<Vec<u8>, ExecError> {
    let element_type = anyarray_element_type(array)?;
    let element_oid = builtin_type_oid_for_sql_type(element_type).ok_or_else(|| {
        ExecError::InvalidStorageValue {
            column: "<anyarray>".into(),
            details: format!("unsupported anyarray element type {:?}", element_type),
        }
    })?;
    encode_flat_array_bytes(element_type, element_oid, array, "<anyarray>")
}

fn encode_flat_array_bytes(
    element_type: SqlType,
    element_oid: u32,
    array: &ArrayValue,
    column: &'static str,
) -> Result<Vec<u8>, ExecError> {
    let layout = array_element_layout(element_type, column)?;
    let item_count = validate_array_shape(array, column)?;
    let has_nulls = array
        .elements
        .iter()
        .any(|item| matches!(item, Value::Null));
    let data_start = flat_array_data_start(array.dimensions.len(), item_count, has_nulls);
    let mut bytes = Vec::with_capacity(data_start);
    bytes.extend_from_slice(&(array.dimensions.len() as i32).to_le_bytes());
    bytes.extend_from_slice(&(if has_nulls { data_start as i32 } else { 0_i32 }).to_le_bytes());
    bytes.extend_from_slice(&element_oid.to_le_bytes());
    for dim in &array.dimensions {
        let length = i32::try_from(dim.length).map_err(|_| ExecError::InvalidStorageValue {
            column: column.into(),
            details: "array dimension length exceeds i32".into(),
        })?;
        bytes.extend_from_slice(&length.to_le_bytes());
    }
    for dim in &array.dimensions {
        bytes.extend_from_slice(&dim.lower_bound.to_le_bytes());
    }
    if has_nulls {
        let bitmap_start = bytes.len();
        bytes.resize(bitmap_start + array_bitmap_len(item_count), 0);
        for (index, item) in array.elements.iter().enumerate() {
            if !matches!(item, Value::Null) {
                bytes[bitmap_start + index / 8] |= 1 << (index % 8);
            }
        }
    }
    bytes.resize(data_start, 0);

    let mut offset = data_start;
    for item in &array.elements {
        if matches!(item, Value::Null) {
            continue;
        }
        offset = layout.typalign.align_offset(offset);
        if offset > bytes.len() {
            bytes.resize(offset, 0);
        }
        let payload = encode_array_element_payload(element_type, item)?;
        let datum = encode_array_element_datum(layout, payload, column)?;
        let start = offset;
        bytes.extend_from_slice(&datum);
        offset = align_array_offset(start + datum.len(), layout.typalign);
        if offset > bytes.len() {
            bytes.resize(offset, 0);
        }
    }

    Ok(bytes)
}

fn encode_array_element_payload(
    element_type: SqlType,
    value: &Value,
) -> Result<Vec<u8>, ExecError> {
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

fn encode_array_element_datum(
    layout: ArrayElementLayout,
    payload: Vec<u8>,
    column: &'static str,
) -> Result<Vec<u8>, ExecError> {
    match layout.typlen {
        len if len > 0 => {
            let expected = len as usize;
            if payload.len() != expected {
                return Err(ExecError::InvalidStorageValue {
                    column: column.into(),
                    details: format!(
                        "fixed-width array element expected {} bytes, got {}",
                        expected,
                        payload.len()
                    ),
                });
            }
            Ok(payload)
        }
        -1 => Ok(encode_embedded_varlena(&payload)),
        other => Err(ExecError::InvalidStorageValue {
            column: column.into(),
            details: format!("unsupported array element storage length {}", other),
        }),
    }
}

fn decode_flat_array_header<'a>(
    bytes: &'a [u8],
    column: &'static str,
) -> Result<DecodedArrayHeader<'a>, ExecError> {
    if bytes.len() < FLAT_ARRAY_HEADER_LEN {
        return Err(ExecError::InvalidStorageValue {
            column: column.into(),
            details: "array payload too short".into(),
        });
    }
    let raw_ndim = i32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if raw_ndim < 0 {
        return Err(ExecError::InvalidStorageValue {
            column: column.into(),
            details: "array ndim cannot be negative".into(),
        });
    }
    let ndim = raw_ndim as usize;
    let raw_dataoffset = i32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if raw_dataoffset < 0 {
        return Err(ExecError::InvalidStorageValue {
            column: column.into(),
            details: "array dataoffset cannot be negative".into(),
        });
    }
    let element_oid = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let element_type =
        sql_type_for_builtin_oid(element_oid).ok_or_else(|| ExecError::InvalidStorageValue {
            column: column.into(),
            details: format!("unknown array element oid {}", element_oid),
        })?;
    let dims_offset = FLAT_ARRAY_HEADER_LEN;
    let dims_bytes = ndim
        .checked_mul(4)
        .and_then(|value| value.checked_mul(2))
        .ok_or_else(|| ExecError::InvalidStorageValue {
            column: column.into(),
            details: "array dimension header too large".into(),
        })?;
    let header_end = dims_offset + dims_bytes;
    if header_end > bytes.len() {
        return Err(ExecError::InvalidStorageValue {
            column: column.into(),
            details: "array dimension header truncated".into(),
        });
    }

    let mut lengths = Vec::with_capacity(ndim);
    for index in 0..ndim {
        let start = dims_offset + index * 4;
        let raw_len = i32::from_le_bytes(bytes[start..start + 4].try_into().unwrap());
        if raw_len < 0 {
            return Err(ExecError::InvalidStorageValue {
                column: column.into(),
                details: "array dimension length cannot be negative".into(),
            });
        }
        lengths.push(raw_len as usize);
    }

    let lbounds_offset = dims_offset + ndim * 4;
    let mut dimensions = Vec::with_capacity(ndim);
    for (index, length) in lengths.into_iter().enumerate() {
        let start = lbounds_offset + index * 4;
        let lower_bound = i32::from_le_bytes(bytes[start..start + 4].try_into().unwrap());
        dimensions.push(ArrayDimension {
            lower_bound,
            length,
        });
    }

    let item_count = array_item_count_from_dimensions(&dimensions, column)?;
    let base_without_bitmap = FLAT_ARRAY_HEADER_LEN + ndim * 8;
    let (bitmap, data_start) = if raw_dataoffset == 0 {
        (None, max_align(base_without_bitmap))
    } else {
        let bitmap_len = array_bitmap_len(item_count);
        let expected_data_start = max_align(base_without_bitmap + bitmap_len);
        let data_start = raw_dataoffset as usize;
        if data_start != expected_data_start {
            return Err(ExecError::InvalidStorageValue {
                column: column.into(),
                details: format!(
                    "array dataoffset {} does not match expected {}",
                    data_start, expected_data_start
                ),
            });
        }
        let bitmap_end = base_without_bitmap + bitmap_len;
        if bitmap_end > bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: column.into(),
                details: "array null bitmap truncated".into(),
            });
        }
        (Some(&bytes[base_without_bitmap..bitmap_end]), data_start)
    };

    if data_start > bytes.len() {
        return Err(ExecError::InvalidStorageValue {
            column: column.into(),
            details: "array data payload truncated".into(),
        });
    }

    Ok(DecodedArrayHeader {
        element_type,
        element_oid,
        dimensions,
        item_count,
        bitmap,
        data_start,
    })
}

pub(crate) fn decode_array_bytes(element_type: SqlType, bytes: &[u8]) -> Result<Value, ExecError> {
    decode_array_bytes_internal(Some(element_type), bytes, "<array>")
}

pub(crate) fn decode_anyarray_bytes(bytes: &[u8]) -> Result<Value, ExecError> {
    decode_array_bytes_internal(None, bytes, "<anyarray>")
}

fn decode_array_bytes_internal(
    expected_element_type: Option<SqlType>,
    bytes: &[u8],
    column: &'static str,
) -> Result<Value, ExecError> {
    let header = decode_flat_array_header(bytes, column)?;
    if let Some(expected) = expected_element_type
        && header.element_type != expected
    {
        return Err(ExecError::InvalidStorageValue {
            column: column.into(),
            details: format!(
                "array element oid {} does not match expected element type {:?}",
                header.element_oid, expected
            ),
        });
    }
    let layout = array_element_layout(header.element_type, column)?;
    let mut items = Vec::with_capacity(header.item_count);
    let mut offset = header.data_start;

    for index in 0..header.item_count {
        if array_bitmap_value(header.bitmap, index) == Some(false) {
            items.push(Value::Null);
            continue;
        }
        offset = layout.typalign.align_offset(offset);
        let (item, next) =
            decode_array_element_datum(header.element_type, layout, bytes, offset, column)?;
        items.push(item);
        offset = align_array_offset(next, layout.typalign);
    }

    Ok(Value::PgArray(
        ArrayValue::from_dimensions(header.dimensions, items)
            .with_element_type_oid(header.element_oid),
    ))
}

fn decode_array_element_datum(
    element_type: SqlType,
    layout: ArrayElementLayout,
    bytes: &[u8],
    offset: usize,
    column: &'static str,
) -> Result<(Value, usize), ExecError> {
    match layout.typlen {
        len if len > 0 => {
            let end = offset + len as usize;
            if end > bytes.len() {
                return Err(ExecError::InvalidStorageValue {
                    column: column.into(),
                    details: "array element payload truncated".into(),
                });
            }
            Ok((
                decode_array_element_value(element_type, &bytes[offset..end], column)?,
                end,
            ))
        }
        -1 => {
            let (payload, end) = decode_embedded_varlena(bytes, offset, column)?;
            Ok((
                decode_array_element_value(element_type, payload, column)?,
                end,
            ))
        }
        other => Err(ExecError::InvalidStorageValue {
            column: column.into(),
            details: format!("unsupported array element storage length {}", other),
        }),
    }
}

fn anyarray_element_type(array: &ArrayValue) -> Result<SqlType, ExecError> {
    if let Some(element_oid) = array.element_type_oid {
        return sql_type_for_builtin_oid(element_oid).ok_or_else(|| {
            ExecError::InvalidStorageValue {
                column: "<anyarray>".into(),
                details: format!("unknown anyarray element oid {}", element_oid),
            }
        });
    }
    array
        .elements
        .iter()
        .find(|value| !matches!(value, Value::Null))
        .and_then(infer_sql_type_from_value)
        .ok_or_else(|| ExecError::InvalidStorageValue {
            column: "<anyarray>".into(),
            details: "cannot infer element type for anyarray".into(),
        })
}

const FLAT_ARRAY_HEADER_LEN: usize = 12;

#[derive(Debug, Clone, Copy)]
struct ArrayElementLayout {
    typlen: i16,
    typalign: AttributeAlign,
}

struct DecodedArrayHeader<'a> {
    element_type: SqlType,
    element_oid: u32,
    dimensions: Vec<ArrayDimension>,
    item_count: usize,
    bitmap: Option<&'a [u8]>,
    data_start: usize,
}

fn array_element_layout(
    element_type: SqlType,
    column: &'static str,
) -> Result<ArrayElementLayout, ExecError> {
    let (typlen, typalign) = match element_type.kind {
        SqlTypeKind::AnyArray => {
            return Err(ExecError::InvalidStorageValue {
                column: column.into(),
                details: "anyarray cannot be used as a concrete array element type".into(),
            });
        }
        SqlTypeKind::Int2 => (2, AttributeAlign::Short),
        SqlTypeKind::Int4
        | SqlTypeKind::Oid
        | SqlTypeKind::RegConfig
        | SqlTypeKind::RegDictionary
        | SqlTypeKind::Date
        | SqlTypeKind::Float4 => (4, AttributeAlign::Int),
        SqlTypeKind::Int8
        | SqlTypeKind::Time
        | SqlTypeKind::Timestamp
        | SqlTypeKind::TimestampTz
        | SqlTypeKind::Float8 => (8, AttributeAlign::Double),
        SqlTypeKind::TimeTz => (12, AttributeAlign::Double),
        SqlTypeKind::Point => (16, AttributeAlign::Double),
        SqlTypeKind::Line | SqlTypeKind::Circle => (24, AttributeAlign::Double),
        SqlTypeKind::Lseg | SqlTypeKind::Box => (32, AttributeAlign::Double),
        SqlTypeKind::Bool => (1, AttributeAlign::Char),
        SqlTypeKind::Bit
        | SqlTypeKind::VarBit
        | SqlTypeKind::Bytea
        | SqlTypeKind::Numeric
        | SqlTypeKind::Json
        | SqlTypeKind::Jsonb
        | SqlTypeKind::JsonPath
        | SqlTypeKind::Text
        | SqlTypeKind::Name
        | SqlTypeKind::PgNodeTree
        | SqlTypeKind::InternalChar
        | SqlTypeKind::Char
        | SqlTypeKind::Varchar
        | SqlTypeKind::Path
        | SqlTypeKind::Polygon
        | SqlTypeKind::TsVector
        | SqlTypeKind::TsQuery => (-1, AttributeAlign::Int),
        SqlTypeKind::Int2Vector | SqlTypeKind::OidVector => {
            return Err(ExecError::InvalidStorageValue {
                column: column.into(),
                details: format!("unsupported array element type {:?}", element_type.kind),
            });
        }
    };
    Ok(ArrayElementLayout { typlen, typalign })
}

fn validate_array_shape(array: &ArrayValue, column: &'static str) -> Result<usize, ExecError> {
    let expected = array_item_count_from_dimensions(&array.dimensions, column)?;
    if expected != array.elements.len() {
        return Err(ExecError::InvalidStorageValue {
            column: column.into(),
            details: format!(
                "array shape expects {} elements but found {}",
                expected,
                array.elements.len()
            ),
        });
    }
    Ok(expected)
}

fn array_item_count_from_dimensions(
    dimensions: &[ArrayDimension],
    column: &'static str,
) -> Result<usize, ExecError> {
    if dimensions.is_empty() {
        return Ok(0);
    }
    dimensions.iter().try_fold(1usize, |count, dim| {
        count
            .checked_mul(dim.length)
            .ok_or_else(|| ExecError::InvalidStorageValue {
                column: column.into(),
                details: "array item count overflow".into(),
            })
    })
}

fn flat_array_data_start(ndim: usize, item_count: usize, has_nulls: bool) -> usize {
    let base = FLAT_ARRAY_HEADER_LEN
        + ndim * 8
        + if has_nulls {
            array_bitmap_len(item_count)
        } else {
            0
        };
    max_align(base)
}

fn array_bitmap_len(item_count: usize) -> usize {
    item_count.div_ceil(8)
}

fn array_bitmap_value(bitmap: Option<&[u8]>, index: usize) -> Option<bool> {
    bitmap.map(|bitmap| (bitmap[index / 8] & (1 << (index % 8))) != 0)
}

fn align_array_offset(offset: usize, align: AttributeAlign) -> usize {
    align.align_offset(offset)
}

fn encode_embedded_varlena(payload: &[u8]) -> Vec<u8> {
    let total_len_1b = 1 + payload.len();
    if total_len_1b <= 127 {
        let mut bytes = Vec::with_capacity(total_len_1b);
        bytes.push((total_len_1b as u8) << 1 | 0x01);
        bytes.extend_from_slice(payload);
        bytes
    } else {
        let mut bytes = Vec::with_capacity(4 + payload.len());
        let total_len = (4 + payload.len()) as u32;
        bytes.extend_from_slice(&(total_len << 2).to_le_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }
}

fn decode_embedded_varlena<'a>(
    bytes: &'a [u8],
    offset: usize,
    column: &'static str,
) -> Result<(&'a [u8], usize), ExecError> {
    if offset >= bytes.len() {
        return Err(ExecError::InvalidStorageValue {
            column: column.into(),
            details: "array element payload truncated".into(),
        });
    }
    if bytes[offset] & 0x01 != 0 {
        let total_len = (bytes[offset] >> 1) as usize;
        if total_len == 0 {
            return Err(ExecError::InvalidStorageValue {
                column: column.into(),
                details: "short varlena array element has zero length".into(),
            });
        }
        let start = offset + 1;
        let end = offset + total_len;
        if end > bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: column.into(),
                details: "array element payload truncated".into(),
            });
        }
        Ok((&bytes[start..end], end))
    } else {
        if offset + 4 > bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: column.into(),
                details: "array varlena header truncated".into(),
            });
        }
        let raw = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let total_len = (raw >> 2) as usize;
        if total_len < 4 {
            return Err(ExecError::InvalidStorageValue {
                column: column.into(),
                details: "array varlena element is too short".into(),
            });
        }
        let start = offset + 4;
        let end = offset + total_len;
        if end > bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: column.into(),
                details: "array element payload truncated".into(),
            });
        }
        Ok((&bytes[start..end], end))
    }
}

fn builtin_type_oid_for_sql_type(sql_type: SqlType) -> Option<u32> {
    builtin_type_rows().into_iter().find_map(|row| {
        (!row.sql_type.is_array
            && row.sql_type.kind == sql_type.kind
            && !matches!(row.sql_type.kind, SqlTypeKind::AnyArray))
        .then_some(row.oid)
    })
}

fn sql_type_for_builtin_oid(oid: u32) -> Option<SqlType> {
    builtin_type_rows()
        .into_iter()
        .find_map(|row| (row.oid == oid).then_some(row.sql_type))
}

fn infer_sql_type_from_value(value: &Value) -> Option<SqlType> {
    match value {
        Value::Null => None,
        Value::Int16(_) => Some(SqlType::new(SqlTypeKind::Int2)),
        Value::Int32(_) => Some(SqlType::new(SqlTypeKind::Int4)),
        Value::Int64(_) => Some(SqlType::new(SqlTypeKind::Int8)),
        Value::Float64(_) => Some(SqlType::new(SqlTypeKind::Float8)),
        Value::Bool(_) => Some(SqlType::new(SqlTypeKind::Bool)),
        Value::Text(_) | Value::TextRef(_, _) => Some(SqlType::new(SqlTypeKind::Text)),
        Value::Numeric(_) => Some(SqlType::new(SqlTypeKind::Numeric)),
        Value::Date(_) => Some(SqlType::new(SqlTypeKind::Date)),
        Value::Time(_) => Some(SqlType::new(SqlTypeKind::Time)),
        Value::TimeTz(_) => Some(SqlType::new(SqlTypeKind::TimeTz)),
        Value::Timestamp(_) => Some(SqlType::new(SqlTypeKind::Timestamp)),
        Value::TimestampTz(_) => Some(SqlType::new(SqlTypeKind::TimestampTz)),
        Value::Bytea(_) => Some(SqlType::new(SqlTypeKind::Bytea)),
        Value::Bit(_) => Some(SqlType::new(SqlTypeKind::VarBit)),
        Value::PgArray(array) => anyarray_element_type(array).ok().map(SqlType::array_of),
        Value::Array(_) => Some(SqlType::array_of(SqlType::new(SqlTypeKind::Text))),
        Value::TsVector(_) => Some(SqlType::new(SqlTypeKind::TsVector)),
        Value::TsQuery(_) => Some(SqlType::new(SqlTypeKind::TsQuery)),
        Value::InternalChar(_) => Some(SqlType::new(SqlTypeKind::InternalChar)),
        Value::Json(_) => Some(SqlType::new(SqlTypeKind::Json)),
        Value::Jsonb(_) => Some(SqlType::new(SqlTypeKind::Jsonb)),
        Value::JsonPath(_) => Some(SqlType::new(SqlTypeKind::JsonPath)),
        Value::Point(_) => Some(SqlType::new(SqlTypeKind::Point)),
        Value::Line(_) => Some(SqlType::new(SqlTypeKind::Line)),
        Value::Lseg(_) => Some(SqlType::new(SqlTypeKind::Lseg)),
        Value::Path(_) => Some(SqlType::new(SqlTypeKind::Path)),
        Value::Box(_) => Some(SqlType::new(SqlTypeKind::Box)),
        Value::Polygon(_) => Some(SqlType::new(SqlTypeKind::Polygon)),
        Value::Circle(_) => Some(SqlType::new(SqlTypeKind::Circle)),
    }
}

fn decode_array_element_value(
    element_type: SqlType,
    bytes: &[u8],
    column: &'static str,
) -> Result<Value, ExecError> {
    match element_type.kind {
        SqlTypeKind::AnyArray => Err(ExecError::InvalidStorageValue {
            column: column.into(),
            details: "anyarray cannot be used as a concrete array element type".into(),
        }),
        SqlTypeKind::Int2 => {
            if bytes.len() != 2 {
                return Err(ExecError::InvalidStorageValue {
                    column: column.into(),
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
                    column: column.into(),
                    details: "int4 array element must be 4 bytes".into(),
                });
            }
            Ok(Value::Int32(i32::from_le_bytes(bytes.try_into().unwrap())))
        }
        SqlTypeKind::Int8 => {
            if bytes.len() != 8 {
                return Err(ExecError::InvalidStorageValue {
                    column: column.into(),
                    details: "int8 array element must be 8 bytes".into(),
                });
            }
            Ok(Value::Int64(i64::from_le_bytes(bytes.try_into().unwrap())))
        }
        SqlTypeKind::Date => {
            if bytes.len() != 4 {
                return Err(ExecError::InvalidStorageValue {
                    column: column.into(),
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
                    column: column.into(),
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
                    column: column.into(),
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
                    column: column.into(),
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
                    column: column.into(),
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
                    column: column.into(),
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
                    column: column.into(),
                    details: "invalid numeric array element".into(),
                },
            )?,
        )),
        SqlTypeKind::Bit | SqlTypeKind::VarBit => {
            if bytes.len() < 4 {
                return Err(ExecError::InvalidStorageValue {
                    column: column.into(),
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
                    column: column.into(),
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
                    column: column.into(),
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
                    column: column.into(),
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
                    column: column.into(),
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
                    column: column.into(),
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
                    column: column.into(),
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
                let rendered = render_internal_char_text(*byte);
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
