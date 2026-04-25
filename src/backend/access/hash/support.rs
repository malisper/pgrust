use crate::include::catalog::BPCHAR_HASH_OPCLASS_OID;
use crate::include::nodes::datum::Value;

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

pub(crate) fn hash_bytes_extended(bytes: &[u8], seed: u64) -> u64 {
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

pub(crate) fn hash_uint32_extended(value: u32, seed: u64) -> u64 {
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

pub(crate) fn hash_int8_extended(value: i64, seed: u64) -> u64 {
    let mut lohalf = value as u32;
    let hihalf = (value >> 32) as u32;
    lohalf ^= if value >= 0 { hihalf } else { !hihalf };
    hash_uint32_extended(lohalf, seed)
}

pub(crate) fn hash_combine64(mut left: u64, right: u64) -> u64 {
    left ^= right
        .wrapping_add(0x49a0_f4dd_15e5_a8e3)
        .wrapping_add(left << 54)
        .wrapping_add(left >> 7);
    left
}

pub(crate) fn hash_value_extended(
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
        Value::Int64(value) => hash_int8_extended(*value, seed),
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
        Value::Float64(value) if *value == 0.0 => seed,
        Value::Float64(value) if value.is_nan() => {
            hash_bytes_extended(&f64::NAN.to_le_bytes(), seed)
        }
        Value::Float64(value) => hash_bytes_extended(&value.to_le_bytes(), seed),
        Value::Numeric(value) => hash_bytes_extended(value.render().as_bytes(), seed),
        Value::Bytea(value) => hash_bytes_extended(value, seed),
        Value::Uuid(value) => hash_bytes_extended(value, seed),
        Value::Range(value) => {
            let bytes = crate::backend::executor::encode_range_bytes(value)
                .map_err(|err| format!("{err:?}"))?;
            hash_bytes_extended(&bytes, seed)
        }
        Value::Multirange(value) => {
            let bytes = crate::backend::executor::encode_multirange_bytes(value)
                .map_err(|err| format!("{err:?}"))?;
            hash_bytes_extended(&bytes, seed)
        }
        value if value.as_text().is_some() => {
            let mut text = value.as_text().unwrap();
            if opclass == Some(BPCHAR_HASH_OPCLASS_OID) {
                text = text.trim_end_matches(' ');
            }
            hash_bytes_extended(text.as_bytes(), seed)
        }
        other => return Err(format!("unsupported hash key value {other:?}")),
    };
    Ok(Some(hash))
}

pub(crate) fn hash_index_value(value: &Value, opclass: Option<u32>) -> Result<Option<u32>, String> {
    Ok(hash_value_extended(value, opclass, 0)?.map(|hash| {
        let low = hash as u32;
        let high = (hash >> 32) as u32;
        low ^ high
    }))
}

pub(crate) fn hash_values_combined(values: &[Value], opclasses: &[u32]) -> Result<u64, String> {
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

pub(crate) fn hash_values_equal(left: &Value, right: &Value, opclass: Option<u32>) -> bool {
    if opclass == Some(BPCHAR_HASH_OPCLASS_OID)
        && let (Some(left), Some(right)) = (left.as_text(), right.as_text())
    {
        return left.trim_end_matches(' ') == right.trim_end_matches(' ');
    }
    crate::backend::access::nbtree::nbtcompare::compare_bt_values(left, right)
        == std::cmp::Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::nodes::datum::NumericValue;

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
    fn null_hash_values_are_skipped() {
        assert_eq!(hash_index_value(&Value::Null, None).unwrap(), None);
    }
}
