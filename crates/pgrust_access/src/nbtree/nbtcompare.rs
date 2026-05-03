use std::cmp::Ordering;

use pgrust_nodes::datum::{NumericValue, Value};

use crate::AccessResult;
use crate::access::itemptr::ItemPointerData;
use crate::services::AccessScalarServices;

pub const BT_DESC_FLAG: i16 = 0x0001;
pub const BT_NULLS_FIRST_FLAG: i16 = 0x0002;

pub fn compare_bt_values_with_services(
    left: &Value,
    right: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<Ordering> {
    match (left, right) {
        (Value::Null, Value::Null) => Ok(Ordering::Equal),
        (Value::Null, _) => Ok(Ordering::Greater),
        (_, Value::Null) => Ok(Ordering::Less),
        (Value::Int16(a), Value::Int16(b)) => Ok(a.cmp(b)),
        (Value::Int16(a), Value::Int32(b)) => Ok(i32::from(*a).cmp(b)),
        (Value::Int16(a), Value::Int64(b)) => Ok(i64::from(*a).cmp(b)),
        (Value::Int32(a), Value::Int16(b)) => Ok(a.cmp(&i32::from(*b))),
        (Value::Int32(a), Value::Int32(b)) => Ok(a.cmp(b)),
        (Value::EnumOid(a), Value::EnumOid(b)) => Ok(a.cmp(b)),
        (Value::Int32(a), Value::Int64(b)) => Ok(i64::from(*a).cmp(b)),
        (Value::Int64(a), Value::Int16(b)) => Ok(a.cmp(&i64::from(*b))),
        (Value::Int64(a), Value::Int32(b)) => Ok(a.cmp(&i64::from(*b))),
        (Value::Int64(a), Value::Int64(b)) => Ok(a.cmp(b)),
        (Value::Xid8(a), Value::Xid8(b)) => Ok(a.cmp(b)),
        (Value::Tid(a), Value::Tid(b)) => Ok(a.cmp(b)),
        (Value::Int16(a), Value::Float64(b)) => Ok(pg_float_cmp(f64::from(*a), *b)),
        (Value::Int32(a), Value::Float64(b)) => Ok(pg_float_cmp(f64::from(*a), *b)),
        (Value::Int64(a), Value::Float64(b)) => Ok(pg_float_cmp(*a as f64, *b)),
        (Value::Float64(a), Value::Int16(b)) => Ok(pg_float_cmp(*a, f64::from(*b))),
        (Value::Float64(a), Value::Int32(b)) => Ok(pg_float_cmp(*a, f64::from(*b))),
        (Value::Float64(a), Value::Int64(b)) => Ok(pg_float_cmp(*a, *b as f64)),
        (Value::Text(_) | Value::TextRef(_, _), Value::Text(_) | Value::TextRef(_, _)) => Ok(left
            .as_text()
            .expect("text-family btree value should expose text")
            .cmp(
                right
                    .as_text()
                    .expect("text-family btree value should expose text"),
            )),
        (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),
        (Value::Bytea(a), Value::Bytea(b)) => Ok(a.cmp(b)),
        (Value::Uuid(a), Value::Uuid(b)) => Ok(a.cmp(b)),
        (Value::Bit(a), Value::Bit(b)) => Ok(a
            .bytes
            .cmp(&b.bytes)
            .then_with(|| a.bit_len.cmp(&b.bit_len))),
        (Value::Array(_) | Value::PgArray(_), Value::Array(_) | Value::PgArray(_)) => {
            services.compare_order_values(left, right, None, None, false)
        }
        (Value::InternalChar(a), Value::InternalChar(b)) => Ok(a.cmp(b)),
        (Value::Range(a), Value::Range(b)) => Ok(services.compare_range_values(a, b)),
        (Value::Interval(a), Value::Interval(b)) => Ok(a.cmp_key().cmp(&b.cmp_key())),
        (Value::Multirange(a), Value::Multirange(b)) => {
            Ok(services.compare_multirange_values(a, b))
        }
        (Value::TsQuery(a), Value::TsQuery(b)) => Ok(services.compare_tsquery(a, b)),
        (Value::TsVector(a), Value::TsVector(b)) => Ok(services.compare_tsvector(a, b)),
        (Value::Inet(a) | Value::Cidr(a), Value::Inet(b) | Value::Cidr(b)) => {
            Ok(services.compare_network_values(a, b))
        }
        (Value::Jsonb(a), Value::Jsonb(b)) => Ok(services
            .compare_jsonb_bytes(a, b)
            .unwrap_or_else(|| a.cmp(b))),
        (Value::Record(_), Value::Record(_)) => {
            services.compare_order_values(left, right, None, None, false)
        }
        (a, b) if numeric_key_value(a).is_some() && numeric_key_value(b).is_some() => {
            Ok(numeric_key_value(a)
                .unwrap()
                .cmp(&numeric_key_value(b).unwrap()))
        }
        (Value::Float64(a), Value::Float64(b)) => Ok(pg_float_cmp(*a, *b)),
        _ => Ok(Ordering::Equal),
    }
}

fn pg_float_cmp(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
    }
}

pub fn compare_bt_values_with_options_and_services(
    left: &Value,
    right: &Value,
    option: i16,
    services: &dyn AccessScalarServices,
) -> AccessResult<Ordering> {
    let nulls_first = option & BT_NULLS_FIRST_FLAG != 0;
    let ord = match (left, right) {
        (Value::Null, Value::Null) => return Ok(Ordering::Equal),
        (Value::Null, _) => {
            return Ok(if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            });
        }
        (_, Value::Null) => {
            return Ok(if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            });
        }
        _ => compare_bt_values_with_services(left, right, services)?,
    };
    if option & BT_DESC_FLAG != 0 {
        Ok(ord.reverse())
    } else {
        Ok(ord)
    }
}

fn numeric_key_value(value: &Value) -> Option<NumericValue> {
    match value {
        Value::Int16(value) => Some(NumericValue::from_i64(i64::from(*value))),
        Value::Int32(value) => Some(NumericValue::from_i64(i64::from(*value))),
        Value::Int64(value) => Some(NumericValue::from_i64(*value)),
        Value::Xid8(value) => Some(NumericValue::finite((*value).into(), 0)),
        Value::Numeric(value) => Some(value.clone()),
        _ => None,
    }
}

pub fn compare_item_pointers(left: &ItemPointerData, right: &ItemPointerData) -> Ordering {
    left.block_number
        .cmp(&right.block_number)
        .then_with(|| left.offset_number.cmp(&right.offset_number))
}

pub fn compare_bt_keyspace_with_services(
    left_keys: &[Value],
    left_tid: &ItemPointerData,
    right_keys: &[Value],
    right_tid: &ItemPointerData,
    services: &dyn AccessScalarServices,
) -> AccessResult<Ordering> {
    compare_bt_keyspace_with_options_and_services(
        left_keys,
        left_tid,
        right_keys,
        right_tid,
        &[],
        services,
    )
}

pub fn compare_bt_keyspace_with_options_and_services(
    left_keys: &[Value],
    left_tid: &ItemPointerData,
    right_keys: &[Value],
    right_tid: &ItemPointerData,
    indoption: &[i16],
    services: &dyn AccessScalarServices,
) -> AccessResult<Ordering> {
    for (index, (left, right)) in left_keys.iter().zip(right_keys).enumerate() {
        let option = indoption.get(index).copied().unwrap_or_default();
        let ord = compare_bt_values_with_options_and_services(left, right, option, services)?;
        if ord != Ordering::Equal {
            return Ok(ord);
        }
    }
    Ok(compare_item_pointers(left_tid, right_tid))
}
