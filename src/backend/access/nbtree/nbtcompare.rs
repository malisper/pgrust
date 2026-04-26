use std::cmp::Ordering;

use crate::backend::executor::{
    compare_multirange_values, compare_network_values, compare_order_values,
};
use crate::include::access::itemptr::ItemPointerData;
use crate::include::nodes::datum::{NumericValue, Value};

pub fn compare_bt_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::Int16(a), Value::Int16(b)) => a.cmp(b),
        (Value::Int16(a), Value::Int32(b)) => i32::from(*a).cmp(b),
        (Value::Int16(a), Value::Int64(b)) => i64::from(*a).cmp(b),
        (Value::Int32(a), Value::Int16(b)) => a.cmp(&i32::from(*b)),
        (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
        (Value::EnumOid(a), Value::EnumOid(b)) => a.cmp(b),
        (Value::Int32(a), Value::Int64(b)) => i64::from(*a).cmp(b),
        (Value::Int64(a), Value::Int16(b)) => a.cmp(&i64::from(*b)),
        (Value::Int64(a), Value::Int32(b)) => a.cmp(&i64::from(*b)),
        (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
        (Value::Int16(a), Value::Float64(b)) => f64::from(*a).total_cmp(b),
        (Value::Int32(a), Value::Float64(b)) => f64::from(*a).total_cmp(b),
        (Value::Int64(a), Value::Float64(b)) => (*a as f64).total_cmp(b),
        (Value::Float64(a), Value::Int16(b)) => a.total_cmp(&f64::from(*b)),
        (Value::Float64(a), Value::Int32(b)) => a.total_cmp(&f64::from(*b)),
        (Value::Float64(a), Value::Int64(b)) => a.total_cmp(&(*b as f64)),
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        (Value::TextRef(_, _), Value::TextRef(_, _)) => Ordering::Equal,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Bytea(a), Value::Bytea(b)) => a.cmp(b),
        (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
        (Value::Bit(a), Value::Bit(b)) => a
            .bytes
            .cmp(&b.bytes)
            .then_with(|| a.bit_len.cmp(&b.bit_len)),
        (Value::Array(_) | Value::PgArray(_), Value::Array(_) | Value::PgArray(_)) => {
            compare_order_values(left, right, None, None, false)
                .expect("btree array comparisons use implicit default collation")
        }
        (Value::InternalChar(a), Value::InternalChar(b)) => a.cmp(b),
        (Value::Interval(a), Value::Interval(b)) => a.cmp_key().cmp(&b.cmp_key()),
        (Value::Multirange(a), Value::Multirange(b)) => compare_multirange_values(a, b),
        (Value::Inet(a) | Value::Cidr(a), Value::Inet(b) | Value::Cidr(b)) => {
            compare_network_values(a, b)
        }
        (a, b) if numeric_key_value(a).is_some() && numeric_key_value(b).is_some() => {
            numeric_key_value(a)
                .unwrap()
                .cmp(&numeric_key_value(b).unwrap())
        }
        (Value::Float64(a), Value::Float64(b)) => a.total_cmp(b),
        _ => Ordering::Equal,
    }
}

fn numeric_key_value(value: &Value) -> Option<NumericValue> {
    match value {
        Value::Int16(value) => Some(NumericValue::from_i64(i64::from(*value))),
        Value::Int32(value) => Some(NumericValue::from_i64(i64::from(*value))),
        Value::Int64(value) => Some(NumericValue::from_i64(*value)),
        Value::Numeric(value) => Some(value.clone()),
        _ => None,
    }
}

pub fn compare_item_pointers(left: &ItemPointerData, right: &ItemPointerData) -> Ordering {
    left.block_number
        .cmp(&right.block_number)
        .then_with(|| left.offset_number.cmp(&right.offset_number))
}

pub fn compare_bt_keyspace(
    left_keys: &[Value],
    left_tid: &ItemPointerData,
    right_keys: &[Value],
    right_tid: &ItemPointerData,
) -> Ordering {
    for (left, right) in left_keys.iter().zip(right_keys) {
        let ord = compare_bt_values(left, right);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    compare_item_pointers(left_tid, right_tid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::nodes::datum::{ArrayDimension, ArrayValue};

    #[test]
    fn bt_keyspace_uses_heap_tid_as_final_tiebreak() {
        let a = ItemPointerData {
            block_number: 1,
            offset_number: 2,
        };
        let b = ItemPointerData {
            block_number: 1,
            offset_number: 3,
        };
        assert_eq!(
            compare_bt_keyspace(&[Value::Int32(10)], &a, &[Value::Int32(10)], &b),
            Ordering::Less
        );
    }

    #[test]
    fn bt_array_comparison_uses_pg_array_shape_rules() {
        let lower_bounds_first = Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound: 0,
                length: 2,
            }],
            vec![Value::Int32(1), Value::Int32(2)],
        ));
        let lower_bounds_second = Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound: 1,
                length: 2,
            }],
            vec![Value::Int32(1), Value::Int32(2)],
        ));
        assert_eq!(
            compare_bt_values(&lower_bounds_first, &lower_bounds_second),
            Ordering::Less
        );

        let with_null = Value::PgArray(ArrayValue::from_1d(vec![Value::Int32(1), Value::Null]));
        let without_null =
            Value::PgArray(ArrayValue::from_1d(vec![Value::Int32(1), Value::Int32(2)]));
        assert_eq!(
            compare_bt_values(&with_null, &without_null),
            Ordering::Greater
        );
    }
}
