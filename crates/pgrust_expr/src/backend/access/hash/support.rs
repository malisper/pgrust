use crate::compat::backend::executor::jsonb::{JsonbValue, decode_jsonb};
use pgrust_catalog_data::BPCHAR_HASH_OPCLASS_OID;
use pgrust_nodes::datum::{
    ArrayValue, InetValue, MultirangeValue, RangeBound, RangeValue, RecordValue, Value,
};
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use std::net::IpAddr;

pub const HASH_PARTITION_SEED: u64 = 0x7A5B_2236_7996_DCFD;
const HASH_INITIAL_VALUE: u32 = 0x9e37_79b9 + 3_923_095;

fn hash_mix(mut a: u32, mut b: u32, mut c: u32) -> (u32, u32, u32) {
    a = a.wrapping_sub(c);
    a ^= c.rotate_left(4);
    c = c.wrapping_add(b);
    b = b.wrapping_sub(a);
    b ^= a.rotate_left(6);
    a = a.wrapping_add(c);
    c = c.wrapping_sub(b);
    c ^= b.rotate_left(8);
    b = b.wrapping_add(a);
    a = a.wrapping_sub(c);
    a ^= c.rotate_left(16);
    c = c.wrapping_add(b);
    b = b.wrapping_sub(a);
    b ^= a.rotate_left(19);
    a = a.wrapping_add(c);
    c = c.wrapping_sub(b);
    c ^= b.rotate_left(4);
    b = b.wrapping_add(a);
    (a, b, c)
}

fn hash_final(mut a: u32, mut b: u32, mut c: u32) -> (u32, u32, u32) {
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(14));
    a ^= c;
    a = a.wrapping_sub(c.rotate_left(11));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(25));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(16));
    a ^= c;
    a = a.wrapping_sub(c.rotate_left(4));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(14));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(24));
    (a, b, c)
}

pub fn hash_bytes_extended(bytes: &[u8], seed: u64) -> u64 {
    // :HACK: pgrust preserves PostgreSQL SQL hash semantics, but this shared
    // digest wrapper is not a full port of PostgreSQL's per-type C hash
    // functions, so exact result bits and hash placement may differ.
    let mut a = HASH_INITIAL_VALUE.wrapping_add(bytes.len() as u32);
    let mut b = a;
    let mut c = a;

    if seed != 0 {
        a = a.wrapping_add((seed >> 32) as u32);
        b = b.wrapping_add(seed as u32);
        (a, b, c) = hash_mix(a, b, c);
    }

    let mut chunks = bytes;
    while chunks.len() >= 12 {
        a = a.wrapping_add(u32::from_le_bytes(chunks[0..4].try_into().unwrap()));
        b = b.wrapping_add(u32::from_le_bytes(chunks[4..8].try_into().unwrap()));
        c = c.wrapping_add(u32::from_le_bytes(chunks[8..12].try_into().unwrap()));
        (a, b, c) = hash_mix(a, b, c);
        chunks = &chunks[12..];
    }

    if chunks.len() >= 11 {
        c = c.wrapping_add((chunks[10] as u32) << 24);
    }
    if chunks.len() >= 10 {
        c = c.wrapping_add((chunks[9] as u32) << 16);
    }
    if chunks.len() >= 9 {
        c = c.wrapping_add((chunks[8] as u32) << 8);
    }
    if chunks.len() >= 8 {
        b = b.wrapping_add((chunks[7] as u32) << 24);
    }
    if chunks.len() >= 7 {
        b = b.wrapping_add((chunks[6] as u32) << 16);
    }
    if chunks.len() >= 6 {
        b = b.wrapping_add((chunks[5] as u32) << 8);
    }
    if chunks.len() >= 5 {
        b = b.wrapping_add(chunks[4] as u32);
    }
    if chunks.len() >= 4 {
        a = a.wrapping_add((chunks[3] as u32) << 24);
    }
    if chunks.len() >= 3 {
        a = a.wrapping_add((chunks[2] as u32) << 16);
    }
    if chunks.len() >= 2 {
        a = a.wrapping_add((chunks[1] as u32) << 8);
    }
    if !chunks.is_empty() {
        a = a.wrapping_add(chunks[0] as u32);
    }

    (_, b, c) = hash_final(a, b, c);
    ((b as u64) << 32) | c as u64
}

pub fn hash_uint32_extended(value: u32, seed: u64) -> u64 {
    let mut a = HASH_INITIAL_VALUE.wrapping_add(std::mem::size_of::<u32>() as u32);
    let mut b = a;
    let mut c = a;

    if seed != 0 {
        a = a.wrapping_add((seed >> 32) as u32);
        b = b.wrapping_add(seed as u32);
        (a, b, c) = hash_mix(a, b, c);
    }

    a = a.wrapping_add(value);
    (_, b, c) = hash_final(a, b, c);
    ((b as u64) << 32) | c as u64
}

pub fn hash_int8_extended(value: i64, seed: u64) -> u64 {
    let mut lohalf = value as u32;
    let hihalf = (value >> 32) as u32;
    lohalf ^= if value >= 0 { hihalf } else { !hihalf };
    hash_uint32_extended(lohalf, seed)
}

pub fn hash_combine64(mut left: u64, right: u64) -> u64 {
    left ^= right
        .wrapping_add(0x49a0_f4dd_15e5_a8e3)
        .wrapping_add(left << 54)
        .wrapping_add(left >> 7);
    left
}

pub fn hash_value_extended(
    value: &Value,
    opclass: Option<u32>,
    seed: u64,
) -> Result<Option<u64>, String> {
    let hash = match value {
        Value::Null => return Ok(None),
        Value::Bool(value) => hash_uint32_extended(u32::from(*value), seed),
        Value::InternalChar(value) => hash_uint32_extended(i32::from(*value) as u32, seed),
        Value::Int16(value) => hash_uint32_extended(i32::from(*value) as u32, seed),
        Value::Int32(value) => hash_uint32_extended(*value as u32, seed),
        Value::EnumOid(value) => hash_uint32_extended(*value, seed),
        Value::Int64(value) => hash_int8_extended(*value, seed),
        Value::Xid8(value) => hash_int8_extended(*value as i64, seed),
        Value::Date(value) => hash_uint32_extended(value.0 as u32, seed),
        Value::Time(value) => hash_int8_extended(value.0, seed),
        Value::Timestamp(value) => hash_int8_extended(value.0, seed),
        Value::TimestampTz(value) => hash_int8_extended(value.0, seed),
        Value::TimeTz(value) => {
            let mut bytes = Vec::with_capacity(12);
            bytes.extend_from_slice(&value.time.0.to_le_bytes());
            bytes.extend_from_slice(&value.offset_seconds.to_le_bytes());
            hash_bytes_extended(&bytes, seed)
        }
        Value::Interval(value) => {
            let key = value.cmp_key();
            let mut bytes = Vec::with_capacity(16);
            bytes.extend_from_slice(&key.to_le_bytes());
            hash_bytes_extended(&bytes, seed)
        }
        Value::Float64(value) if *value == 0.0 => seed,
        Value::Float64(value) if value.is_nan() => {
            hash_bytes_extended(&f64::NAN.to_le_bytes(), seed)
        }
        Value::Float64(value) => hash_bytes_extended(&value.to_le_bytes(), seed),
        Value::Numeric(value) => {
            hash_bytes_extended(value.normalize_display_scale().render().as_bytes(), seed)
        }
        Value::Bytea(value) => hash_bytes_extended(value, seed),
        Value::Uuid(value) => hash_bytes_extended(value, seed),
        Value::Inet(value) | Value::Cidr(value) => hash_inet_value(value, seed),
        Value::PgLsn(value) => hash_bytes_extended(&value.to_le_bytes(), seed),
        Value::Jsonb(value) => match decode_jsonb(value) {
            Ok(json) => hash_jsonb_value(&json, seed),
            Err(_) => hash_bytes_extended(value, seed),
        },
        Value::Array(values) => {
            let array = ArrayValue::from_nested_values(values.clone(), vec![1])?;
            hash_array_value(&array, seed)?
        }
        Value::PgArray(value) => hash_array_value(value, seed)?,
        Value::Record(value) => hash_record_value(value, seed)?,
        Value::Range(value) => hash_range_value(value, seed)?,
        Value::Multirange(value) => hash_multirange_value(value, seed)?,
        Value::MacAddr(value) => hash_bytes_extended(value, seed),
        Value::MacAddr8(value) => hash_bytes_extended(value, seed),
        Value::Bit(_) => return Err(non_hashable_type_error(value.sql_type_hint())),
        value if value.as_text().is_some() => {
            let mut text = value.as_text().unwrap();
            if opclass == Some(BPCHAR_HASH_OPCLASS_OID) {
                text = text.trim_end_matches(' ');
            }
            hash_bytes_extended(text.as_bytes(), seed)
        }
        other => return Err(non_hashable_type_error(other.sql_type_hint())),
    };
    Ok(Some(hash))
}

fn hash_inet_value(value: &InetValue, seed: u64) -> u64 {
    let mut bytes = Vec::with_capacity(18);
    match value.addr {
        IpAddr::V4(addr) => {
            bytes.push(4);
            bytes.push(value.bits);
            bytes.extend_from_slice(&addr.octets());
        }
        IpAddr::V6(addr) => {
            bytes.push(6);
            bytes.push(value.bits);
            bytes.extend_from_slice(&addr.octets());
        }
    }
    hash_bytes_extended(&bytes, seed)
}

fn hash_jsonb_value(value: &JsonbValue, seed: u64) -> u64 {
    match value {
        JsonbValue::Null => hash_bytes_extended(b"n", seed),
        JsonbValue::String(value) => hash_combine64(
            hash_bytes_extended(b"s", seed),
            hash_bytes_extended(value.as_bytes(), seed),
        ),
        JsonbValue::Numeric(value) => hash_combine64(
            hash_bytes_extended(b"m", seed),
            hash_bytes_extended(value.normalize_display_scale().render().as_bytes(), seed),
        ),
        JsonbValue::Bool(value) => hash_combine64(
            hash_bytes_extended(b"b", seed),
            hash_uint32_extended(u32::from(*value), seed),
        ),
        JsonbValue::Array(items) => {
            let mut hash = hash_bytes_extended(b"a", seed);
            for item in items {
                hash = hash_combine64(hash, hash_jsonb_value(item, seed));
            }
            hash
        }
        JsonbValue::Object(items) => {
            let mut hash = hash_bytes_extended(b"o", seed);
            for (key, value) in items {
                hash = hash_combine64(hash, hash_bytes_extended(key.as_bytes(), seed));
                hash = hash_combine64(hash, hash_jsonb_value(value, seed));
            }
            hash
        }
        other => hash_combine64(
            hash_bytes_extended(b"t", seed),
            hash_bytes_extended(format!("{other:?}").as_bytes(), seed),
        ),
    }
}

fn hash_array_value(value: &ArrayValue, seed: u64) -> Result<u64, String> {
    let mut hash = 1_u64;
    for element in &value.elements {
        let element_hash = hash_value_extended(element, None, seed)?.unwrap_or(0);
        hash = (hash << 5).wrapping_sub(hash).wrapping_add(element_hash);
    }
    Ok(hash)
}

fn hash_record_value(value: &RecordValue, seed: u64) -> Result<u64, String> {
    let mut hash = hash_uint32_extended(value.fields.len() as u32, seed);
    for (field, field_value) in value.iter() {
        let field_hash = hash_value_extended(field_value, None, seed)
            .map_err(|_| non_hashable_type_error(Some(field.sql_type)))?
            .unwrap_or_else(|| hash_null_sentinel(seed));
        hash = hash_combine64(hash, field_hash);
    }
    Ok(hash)
}

fn hash_null_sentinel(seed: u64) -> u64 {
    hash_uint32_extended(0xFFFF_FFFF, seed)
}

fn non_hashable_type_error(ty: Option<SqlType>) -> String {
    let type_name = ty
        .map(non_hashable_type_name)
        .unwrap_or_else(|| "unknown".to_string());
    format!("could not identify a hash function for type {type_name}")
}

fn non_hashable_type_name(ty: SqlType) -> String {
    match ty.element_type().kind {
        SqlTypeKind::Bit | SqlTypeKind::VarBit => "bit varying".to_string(),
        SqlTypeKind::Point => "point".to_string(),
        SqlTypeKind::Lseg => "lseg".to_string(),
        SqlTypeKind::Path => "path".to_string(),
        SqlTypeKind::Line => "line".to_string(),
        SqlTypeKind::Box => "box".to_string(),
        SqlTypeKind::Polygon => "polygon".to_string(),
        SqlTypeKind::Circle => "circle".to_string(),
        other => format!("{other:?}").to_ascii_lowercase(),
    }
}

fn hash_multirange_value(value: &MultirangeValue, seed: u64) -> Result<u64, String> {
    let mut hash = hash_uint32_extended(value.ranges.len() as u32, seed);
    for range in &value.ranges {
        hash = hash_combine64(hash, hash_range_value(range, seed)?);
    }
    Ok(hash)
}

fn hash_range_value(value: &RangeValue, seed: u64) -> Result<u64, String> {
    if value.empty {
        return Ok(hash_uint32_extended(1, seed));
    }
    let mut flags = 0_u32;
    if value.lower.is_some() {
        flags |= 1 << 0;
    }
    if value.upper.is_some() {
        flags |= 1 << 1;
    }
    if value.lower.as_ref().is_some_and(|bound| bound.inclusive) {
        flags |= 1 << 2;
    }
    if value.upper.as_ref().is_some_and(|bound| bound.inclusive) {
        flags |= 1 << 3;
    }

    let mut hash = hash_uint32_extended(flags, seed);
    hash = hash_combine64(hash, hash_range_bound(value.lower.as_ref(), seed)?);
    hash = hash_combine64(hash, hash_range_bound(value.upper.as_ref(), seed)?);
    Ok(hash)
}

fn hash_range_bound(bound: Option<&RangeBound>, seed: u64) -> Result<u64, String> {
    match bound {
        Some(bound) => hash_value_extended(bound.value.as_ref(), None, seed)?
            .ok_or_else(|| "unsupported null range bound".to_string()),
        None => Ok(hash_uint32_extended(0, seed)),
    }
}

pub fn hash_index_value(value: &Value, opclass: Option<u32>) -> Result<Option<u32>, String> {
    Ok(hash_value_extended(value, opclass, 0)?.map(|hash| hash as u32))
}

pub fn hash_values_combined(values: &[Value], opclasses: &[u32]) -> Result<u64, String> {
    let mut row_hash = 0_u64;
    for (index, value) in values.iter().enumerate() {
        if let Some(value_hash) =
            hash_value_extended(value, opclasses.get(index).copied(), HASH_PARTITION_SEED)?
        {
            row_hash = hash_combine64(row_hash, value_hash);
        }
    }
    Ok(row_hash)
}

pub fn hash_values_equal(left: &Value, right: &Value, opclass: Option<u32>) -> bool {
    if opclass == Some(BPCHAR_HASH_OPCLASS_OID)
        && let (Some(left), Some(right)) = (left.as_text(), right.as_text())
    {
        return left.trim_end_matches(' ') == right.trim_end_matches(' ');
    }
    crate::compat::backend::access::nbtree::nbtcompare::compare_bt_values(left, right)
        == std::cmp::Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_nodes::datum::NumericValue;

    #[test]
    fn hash_value_normalizes_zero_and_bpchar() {
        assert_eq!(
            hash_index_value(&Value::Float64(0.0), None).unwrap(),
            hash_index_value(&Value::Float64(-0.0), None).unwrap()
        );
        assert_eq!(
            hash_index_value(&Value::Text("x".into()), Some(BPCHAR_HASH_OPCLASS_OID)).unwrap(),
            hash_index_value(&Value::Text("x   ".into()), Some(BPCHAR_HASH_OPCLASS_OID)).unwrap()
        );
    }

    #[test]
    fn hash_value_is_stable_for_numeric_and_bytea() {
        assert_eq!(
            hash_index_value(&Value::Numeric(NumericValue::from_i64(42)), None).unwrap(),
            hash_index_value(&Value::Numeric(NumericValue::from_i64(42)), None).unwrap()
        );
        assert_ne!(
            hash_index_value(&Value::Bytea(vec![1, 2, 3]), None).unwrap(),
            hash_index_value(&Value::Bytea(vec![1, 2, 4]), None).unwrap()
        );
    }

    #[test]
    fn hash_value_matches_semantic_jsonb_numeric_equality() {
        let left = Value::Jsonb(
            crate::compat::backend::executor::jsonb::parse_jsonb_text("{\"age\":25}")
                .expect("jsonb parses"),
        );
        let right = Value::Jsonb(
            crate::compat::backend::executor::jsonb::parse_jsonb_text("{\"age\":25.0}")
                .expect("jsonb parses"),
        );
        assert_eq!(
            hash_index_value(&left, None).unwrap(),
            hash_index_value(&right, None).unwrap()
        );
        assert!(hash_values_equal(&left, &right, None));
    }

    #[test]
    fn null_hash_values_are_skipped() {
        assert_eq!(hash_index_value(&Value::Null, None).unwrap(), None);
    }
}
